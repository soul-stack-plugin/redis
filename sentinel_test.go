package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	pluginv1 "github.com/souls-guild/soul-stack/proto/plugin/gen/go/v1"
)

// sentinelConn - fake redisConn for sentinel tests. Answers:
//   - SENTINEL MASTER <name> -> masterReply (flat "k v k v"; empty -> "No such
//     master" error, simulating unknown master for action=add);
//   - SENTINEL CONFIG GET <p> -> configReply[p] as "p value" (empty -> "p").
//
// Writes each call (assert MONITOR/REMOVE/SET/CONFIG SET).
const secretAuthPass = "vault-resolved-sentinel-pass-7c2f9a"

type sentinelConn struct {
	cfg          connConfig
	masterReply  string            // response SENTINEL MASTER (flat "k v..."); "" -> No such master
	masterExists bool              // true -> master is known (even if masterReply is set)
	configReply  map[string]string // SENTINEL CONFIG GET <p> -> value
	calls        [][]any
	closed       bool
}

func (c *sentinelConn) Do(_ context.Context, args ...any) (string, error) {
	c.calls = append(c.calls, args)
	switch {
	case isSub(args, "SENTINEL", "MASTER"):
		if c.masterReply == "" && !c.masterExists {
			return "", errors.New("ERR No such master with that name")
		}
		return c.masterReply, nil
	case isSub(args, "SENTINEL", "CONFIG") && len(args) >= 4 && strings.EqualFold(str(args[2]), "GET"):
		p := str(args[3])
		if v, ok := c.configReply[p]; ok {
			return p + " " + v, nil
		}
		return p + " ", nil
	}
	return "OK", nil
}

// ConfigGet - sentinel-state reads SENTINEL CONFIG GET (via Do) rather than plain
// CONFIG GET; stub for the redisConn interface.
func (c *sentinelConn) ConfigGet(_ context.Context, param string) (map[string]string, error) {
	return map[string]string{param: ""}, nil
}

// GetKeysInSlot - sentinel-state slots do not migrate, stub for redisConn interface.
func (c *sentinelConn) GetKeysInSlot(_ context.Context, _, _ int) ([]string, error) {
	return nil, nil
}

// AclList - sentinel-state ACL does not touch, stub for redisConn interface.
func (c *sentinelConn) AclList(_ context.Context) ([]string, error) { return nil, nil }

func (c *sentinelConn) Close() error { c.closed = true; return nil }

func str(v any) string { s, _ := v.(string); return s }

func isSub(args []any, want ...string) bool {
	if len(args) < len(want) {
		return false
	}
	for i, w := range want {
		if !strings.EqualFold(str(args[i]), w) {
			return false
		}
	}
	return true
}

func sentinelModule(conn *sentinelConn) *RedisModule {
	return &RedisModule{
		connect: func(_ context.Context, cfg connConfig) (redisConn, error) {
			conn.cfg = cfg
			return conn, nil
		},
	}
}

// --- classifyConfig (pure function, wrap classify_config) ---

func TestClassifyConfig_SplitsGlobalsAndPerMaster(t *testing.T) {
	config := map[string]string{
		"loglevel":             "notice",
		"sentinel announce-ip": "10.0.0.9",
		"sentinel down-after-milliseconds mymaster": "12000",
		"sentinel failover-timeout other":           "70000",          // other master -> ignore
		"dir":                                       "/var/lib/redis", // startup-only -> ignore
		"port":                                      "26379",          // startup-only -> ignore
	}
	globals, perMaster := classifyConfig(config, "mymaster")

	if globals["loglevel"] != "notice" {
		t.Errorf("loglevel should be global: %v", globals)
	}
	if globals["announce-ip"] != "10.0.0.9" {
		t.Errorf("announce-ip should be global: %v", globals)
	}
	if perMaster["down-after-milliseconds"] != "12000" {
		t.Errorf("down-after-milliseconds should be per-master: %v", perMaster)
	}
	if _, ok := perMaster["failover-timeout"]; ok {
		t.Error("failover-timeout for ANOTHER master should not get into per-master")
	}
	if _, ok := globals["dir"]; ok {
		t.Error("startup-only dir should not be included in globals")
	}
	if _, ok := globals["port"]; ok {
		t.Error("startup-only port should not be included in globals")
	}
}

// --- supportedGlobals: loglevel version-gate + secret-filter ---

func TestSupportedGlobals_LoglevelVersionGate(t *testing.T) {
	g := map[string]string{"loglevel": "notice", "announce-ip": "10.0.0.1"}

	on70 := supportedGlobals(g, "7.4.1")
	if on70["loglevel"] != "notice" {
		t.Errorf("loglevel should pass to 7.4.1: %v", on70)
	}
	on62 := supportedGlobals(g, "6.2.0")
	if _, ok := on62["loglevel"]; ok {
		t.Errorf("loglevel should NOT pass on 6.2.0: %v", on62)
	}
	if on62["announce-ip"] != "10.0.0.1" {
		t.Errorf("announce-ip should run on any version: %v", on62)
	}
	// The empty version -> loglevel is discarded fail-closed.
	if _, ok := supportedGlobals(g, "")["loglevel"]; ok {
		t.Error("loglevel should NOT pass on the empty version")
	}
}

func TestSupportedGlobals_DropsSecretGlobals(t *testing.T) {
	g := map[string]string{"sentinel-pass": secretAuthPass, "announce-ip": "10.0.0.1"}
	out := supportedGlobals(g, "7.4.1")
	if _, ok := out["sentinel-pass"]; ok {
		t.Error("sentinel-pass (secret) should not be included in applied globals (diff is not possible)")
	}
}

// --- versionTuple / versionGE: epoch, v-prefix, garbage suffix, 2-component ---

func TestVersionTuple_NormalizesDistroForms(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want [3]int
	}{
		{"8.0.3", [3]int{8, 0, 3}},
		{"v8.0.3", [3]int{8, 0, 3}},              // v-prefix
		{"6.2", [3]int{6, 2, 0}},                 // 2-component -> patch=0
		{"7", [3]int{7, 0, 0}},                   // major only
		{"5:7.0.15-1~deb12u7", [3]int{7, 0, 15}}, // deb epoch + revision: major=7, NOT 5
		{"7.0.15-1ubuntu0.1", [3]int{7, 0, 15}},  // revision - tail cut off
		{"  7.4.1  ", [3]int{7, 4, 1}},           // surrounding spaces
		{"7.0.15~rc1", [3]int{7, 0, 15}},         // garbage suffix in chunk (leading digits)
		{"", [3]int{0, 0, 0}},                    // empty -> (0,0,0)
		{"garbage", [3]int{0, 0, 0}},             // non-digits -> (0,0,0)
	} {
		if got := versionTuple(tc.in); got != tc.want {
			t.Errorf("versionTuple(%q)=%v, expected %v", tc.in, got, tc.want)
		}
	}
}

// TestVersionGE_EpochDoesNotMisgate - regression to m6: distro epoch should not drop
// major to epoch number. "5:7.0.15..." must be considered >= 7.0 (and not like 5.x).
func TestVersionGE_EpochDoesNotMisgate(t *testing.T) {
	target := [3]int{7, 0, 0}
	if !versionGE("5:7.0.15-1~deb12u7", target) {
		t.Error("epoch-form 5:7.0.15 must be >= 7.0 (epoch is not major)")
	}
	if versionGE("5:6.2.7-1", target) {
		t.Error("epoch-form 5:6.2.7 must NOT be >= 7.0 (epoch does not make it 7+)")
	}
	if versionGE("6.2.0", target) {
		t.Error("6.2.0 must NOT be >= 7.0")
	}
	if !versionGE("7.0.0", target) {
		t.Error("7.0.0 must be >= 7.0 (lower bound inclusive)")
	}
	if versionGE("", target) {
		t.Error("empty version -> (0,0,0) NOT >= 7.0 (fail-closed)")
	}
}

// TestSentinelGlobals_SkippedPre70 - m1: at <7.0 reconcileGlobals entirely NOT
// running (SENTINEL CONFIG GET/SET not available), non-empty globals -> no-op +
// warning action. On 6.2, announce-ip should NOT be included in CONFIG GET/SET.
func TestSentinelGlobals_SkippedPre70(t *testing.T) {
	conn := &sentinelConn{
		masterExists: true,
		masterReply:  "name mymaster ip 10.0.0.1 port 6379 quorum 2",
		configReply:  map[string]string{"announce-ip": "10.0.0.9"},
	}
	m := sentinelModule(conn)
	stream := &applyStream{}

	err := m.sentinel().Apply(&pluginv1.ApplyRequest{
		State: "monitored",
		Params: mustStruct(t, map[string]any{
			"addr":          "127.0.0.1:26379",
			"master_name":   "mymaster",
			"redis_version": "6.2.7", // <7.0 -> globals are completely skipped
			"monitor":       map[string]any{"ip": "10.0.0.1", "port": 6379, "quorum": 2},
			"config": map[string]any{
				"sentinel announce-ip": "10.0.0.1",
			},
		}),
	}, stream)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil || fin.Failed {
		t.Fatalf("expected success (not falling to 6.2), got %+v", fin)
	}
	if fin.Changed {
		t.Error("waited changed=false: globals skipped to <7.0, monitor is already in place")
	}
	// KEY: SENTINEL CONFIG GET/SET should NOT be called on 6.2.
	if hasCall(conn.calls, "SENTINEL", "CONFIG", "GET") {
		t.Errorf("SENTINEL CONFIG GET should not be called on 6.2: %v", conn.calls)
	}
	if hasCall(conn.calls, "SENTINEL", "CONFIG", "SET") {
		t.Errorf("SENTINEL CONFIG SET should not be called on 6.2: %v", conn.calls)
	}
	if got := fin.GetOutput().GetFields()["actions"].GetStringValue(); !strings.Contains(got, "globals_skipped_pre_7.0") {
		t.Errorf("expected the warning action globals_skipped_pre_7.0 in actions, got %q", got)
	}
}

// TestValidate_SentinelRejectsZeroQuorum - m2: quorum:0 if there is a monitor
// rejected (SENTINEL MONITOR... 0 does not accept Redis).
func TestValidate_SentinelRejectsZeroQuorum(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.sentinel().Validate(context.Background(), &pluginv1.ValidateRequest{
		State: "monitored",
		Params: mustStruct(t, map[string]any{
			"addr":    "127.0.0.1:26379",
			"monitor": map[string]any{"ip": "10.0.0.1", "port": 6379, "quorum": 0},
		}),
	})
	if reply.Ok {
		t.Fatal("waited Ok=false: quorum=0 invalid")
	}
}

func TestValidate_SentinelRejectsNegativeQuorum(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.sentinel().Validate(context.Background(), &pluginv1.ValidateRequest{
		State: "monitored",
		Params: mustStruct(t, map[string]any{
			"addr":    "127.0.0.1:26379",
			"monitor": map[string]any{"ip": "10.0.0.1", "port": 6379, "quorum": -1},
		}),
	})
	if reply.Ok {
		t.Fatal("expected Ok=false: negative quorum is not allowed")
	}
}

// TestValidate_SentinelQuorumOmittedOK - quorum is not set -> passes (default 1
// in reconcileMonitor); guard is triggered only if the value is EXPLICITLY incorrect.
func TestValidate_SentinelQuorumOmittedOK(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.sentinel().Validate(context.Background(), &pluginv1.ValidateRequest{
		State: "monitored",
		Params: mustStruct(t, map[string]any{
			"addr":    "127.0.0.1:26379",
			"monitor": map[string]any{"ip": "10.0.0.1", "port": 6379},
		}),
	})
	if !reply.Ok || len(reply.Errors) != 0 {
		t.Fatalf("waited Ok=true: quorum is optional, got %+v", reply)
	}
}

// --- computeMonitorAction ---

func TestComputeMonitorAction(t *testing.T) {
	if a := computeMonitorAction(nil, "10.0.0.1", "6379"); a != "add" {
		t.Errorf("unknown master -> add, got %q", a)
	}
	cur := map[string]string{"ip": "10.0.0.1", "port": "6379"}
	if a := computeMonitorAction(cur, "10.0.0.1", "6379"); a != "none" {
		t.Errorf("matched address -> none, got %q", a)
	}
	if a := computeMonitorAction(cur, "10.0.0.2", "6379"); a != "readd" {
		t.Errorf("changed IP -> readd, got %q", a)
	}
}

// --- computeSetUpdates: differences only, deterministic order ---

func TestComputeSetUpdates_OnlyDiffsSorted(t *testing.T) {
	desired := map[string]string{"quorum": "2", "down-after-milliseconds": "12000", "failover-timeout": "70000"}
	current := map[string]string{"quorum": "2", "down-after-milliseconds": "5000"} // quorum matches, dam is different, ft is missing
	keys, values := computeSetUpdates(desired, current)

	// quorum matched -> not in updates; the other two -> yes; order is sorted.
	want := []string{"down-after-milliseconds", "failover-timeout"}
	if strings.Join(keys, ",") != strings.Join(want, ",") {
		t.Errorf("keys=%v, expected %v (without quorum, sorted)", keys, want)
	}
	if values["down-after-milliseconds"] != "12000" || values["failover-timeout"] != "70000" {
		t.Errorf("incorrect update values: %v", values)
	}
}

// --- Validate: sentinel ---

func TestValidate_SentinelRejectsEmptyAddr(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.sentinel().Validate(context.Background(), &pluginv1.ValidateRequest{
		State:  "monitored",
		Params: mustStruct(t, map[string]any{"addr": ""}),
	})
	if reply.Ok {
		t.Fatal("waited Ok=false on empty addr")
	}
}

func TestValidate_SentinelRejectsMonitorWithoutIP(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.sentinel().Validate(context.Background(), &pluginv1.ValidateRequest{
		State: "monitored",
		Params: mustStruct(t, map[string]any{
			"addr":    "127.0.0.1:26379",
			"monitor": map[string]any{"port": 6379, "quorum": 2},
		}),
	})
	if reply.Ok {
		t.Fatal("waited Ok=false: monitor without ip")
	}
}

func TestValidate_SentinelHappy(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.sentinel().Validate(context.Background(), &pluginv1.ValidateRequest{
		State: "monitored",
		Params: mustStruct(t, map[string]any{
			"addr":    "127.0.0.1:26379",
			"monitor": map[string]any{"ip": "10.0.0.1", "port": 6379, "quorum": 2},
		}),
	})
	if !reply.Ok || len(reply.Errors) != 0 {
		t.Fatalf("waited Ok=true, got %+v", reply)
	}
}

// --- Apply sentinel: new monitor (MONITOR + auth-set) ---

func TestApplySentinel_MonitorAddWithAuth(t *testing.T) {
	// master is unknown -> action=add.
	conn := &sentinelConn{configReply: map[string]string{}}
	m := sentinelModule(conn)
	stream := &applyStream{}

	err := m.sentinel().Apply(&pluginv1.ApplyRequest{
		State: "monitored",
		Params: mustStruct(t, map[string]any{
			"addr":        "127.0.0.1:26379",
			"master_name": "mymaster",
			"monitor":     map[string]any{"ip": "10.0.0.1", "port": 6379, "quorum": 2},
			"auth_user":   "sentinel",
			"auth_pass":   secretAuthPass,
		}),
	}, stream)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil || fin.Failed || !fin.Changed {
		t.Fatalf("expected success changed=true, got %+v", fin)
	}
	if !hasCall(conn.calls, "SENTINEL", "MONITOR", "mymaster", "10.0.0.1", "6379", "2") {
		t.Errorf("no SENTINEL MONITOR mymaster 10.0.0.1 6379 2: %v", conn.calls)
	}
	if !hasCall(conn.calls, "SENTINEL", "SET", "mymaster", "auth-user", "sentinel") {
		t.Errorf("no SENTINEL SET auth-user: %v", conn.calls)
	}
	if !hasCall(conn.calls, "SENTINEL", "SET", "mymaster", "auth-pass") {
		t.Errorf("no SENTINEL SET auth-pass: %v", conn.calls)
	}
	// auth_pass should not leak into events.
	assertSentinelNoSecret(t, stream)
}

// --- Apply sentinel: monitor is already in place -> idempotent (no MONITOR) ---

func TestApplySentinel_MonitorIdempotent(t *testing.T) {
	// master is known at the desired address -> action=none; config is empty -> no SET/CONFIG SET.
	conn := &sentinelConn{
		masterExists: true,
		masterReply:  "name mymaster ip 10.0.0.1 port 6379 quorum 2",
		configReply:  map[string]string{},
	}
	m := sentinelModule(conn)
	stream := &applyStream{}

	err := m.sentinel().Apply(&pluginv1.ApplyRequest{
		State: "monitored",
		Params: mustStruct(t, map[string]any{
			"addr":        "127.0.0.1:26379",
			"master_name": "mymaster",
			"monitor":     map[string]any{"ip": "10.0.0.1", "port": 6379, "quorum": 2},
		}),
	}, stream)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil || fin.Failed {
		t.Fatalf("expected success, got %+v", fin)
	}
	if fin.Changed {
		t.Error("waited changed=false: the monitor is already at the desired address, quorum matched (no-op)")
	}
	if hasCall(conn.calls, "SENTINEL", "MONITOR") {
		t.Errorf("idempotent broken: MONITOR called: %v", conn.calls)
	}
	if hasCall(conn.calls, "SENTINEL", "SET") {
		t.Errorf("idempotent broken: SET called when quorum matched: %v", conn.calls)
	}
}

// --- Apply sentinel: change address master -> REMOVE + MONITOR (readd) ---

func TestApplySentinel_MonitorReadd(t *testing.T) {
	conn := &sentinelConn{
		masterExists: true,
		masterReply:  "name mymaster ip 10.0.0.5 port 6379 quorum 2", // old address
		configReply:  map[string]string{},
	}
	m := sentinelModule(conn)
	stream := &applyStream{}

	err := m.sentinel().Apply(&pluginv1.ApplyRequest{
		State: "monitored",
		Params: mustStruct(t, map[string]any{
			"addr":        "127.0.0.1:26379",
			"master_name": "mymaster",
			"monitor":     map[string]any{"ip": "10.0.0.1", "port": 6379, "quorum": 2}, // new address
		}),
	}, stream)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil || fin.Failed || !fin.Changed {
		t.Fatalf("expected success changed=true, got %+v", fin)
	}
	if !hasCall(conn.calls, "SENTINEL", "REMOVE", "mymaster") {
		t.Errorf("no SENTINEL REMOVE when changing address: %v", conn.calls)
	}
	if !hasCall(conn.calls, "SENTINEL", "MONITOR", "mymaster", "10.0.0.1", "6379", "2") {
		t.Errorf("no SENTINEL MONITOR with new address: %v", conn.calls)
	}
}

// --- Apply sentinel: per-master SET reconcile (down-after different) ---

func TestApplySentinel_PerMasterSetReconcile(t *testing.T) {
	conn := &sentinelConn{
		masterExists: true,
		masterReply:  "name mymaster ip 10.0.0.1 port 6379 quorum 2 down-after-milliseconds 5000",
		configReply:  map[string]string{},
	}
	m := sentinelModule(conn)
	stream := &applyStream{}

	err := m.sentinel().Apply(&pluginv1.ApplyRequest{
		State: "monitored",
		Params: mustStruct(t, map[string]any{
			"addr":        "127.0.0.1:26379",
			"master_name": "mymaster",
			"monitor":     map[string]any{"ip": "10.0.0.1", "port": 6379, "quorum": 2},
			"config": map[string]any{
				"sentinel down-after-milliseconds mymaster": "12000",
			},
		}),
	}, stream)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil || fin.Failed || !fin.Changed {
		t.Fatalf("expected success changed=true (down-after changed), got %+v", fin)
	}
	if !hasCall(conn.calls, "SENTINEL", "SET", "mymaster", "down-after-milliseconds", "12000") {
		t.Errorf("no SENTINEL SET down-after-milliseconds 12000: %v", conn.calls)
	}
	// quorum matched (2) -> it should not be in SET.
	if hasCall(conn.calls, "SENTINEL", "SET", "mymaster", "quorum") {
		t.Errorf("quorum matched - SET quorum should not be called: %v", conn.calls)
	}
}

// --- Apply sentinel: globals reconcile via SENTINEL CONFIG SET ---

func TestApplySentinel_GlobalsConfigSet(t *testing.T) {
	conn := &sentinelConn{
		masterExists: true,
		masterReply:  "name mymaster ip 10.0.0.1 port 6379 quorum 2",
		configReply:  map[string]string{"announce-ip": "10.0.0.9"}, // current value is different
	}
	m := sentinelModule(conn)
	stream := &applyStream{}

	err := m.sentinel().Apply(&pluginv1.ApplyRequest{
		State: "monitored",
		Params: mustStruct(t, map[string]any{
			"addr":          "127.0.0.1:26379",
			"master_name":   "mymaster",
			"redis_version": "7.4.1", // SENTINEL CONFIG GET/SET available since 7.0
			"monitor":       map[string]any{"ip": "10.0.0.1", "port": 6379, "quorum": 2},
			"config": map[string]any{
				"sentinel announce-ip": "10.0.0.1", // desired != current 10.0.0.9
			},
		}),
	}, stream)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil || fin.Failed || !fin.Changed {
		t.Fatalf("expected success changed=true (announce-ip has changed), got %+v", fin)
	}
	if !hasCall(conn.calls, "SENTINEL", "CONFIG", "SET", "announce-ip", "10.0.0.1") {
		t.Errorf("no SENTINEL CONFIG SET announce-ip: %v", conn.calls)
	}
}

// --- Apply sentinel: auth_pass does not flow (events + connection failure) ---

func TestApplySentinel_NoSecretLeak(t *testing.T) {
	conn := &sentinelConn{configReply: map[string]string{}}
	m := sentinelModule(conn)
	stream := &applyStream{}

	_ = m.sentinel().Apply(&pluginv1.ApplyRequest{
		State: "monitored",
		Params: mustStruct(t, map[string]any{
			"addr":        "127.0.0.1:26379",
			"master_name": "mymaster",
			"monitor":     map[string]any{"ip": "10.0.0.1", "port": 6379, "quorum": 2},
			"auth_pass":   secretAuthPass,
		}),
	}, stream)

	assertSentinelNoSecret(t, stream)
}

func TestApplySentinel_ConnectFailureNoLeak(t *testing.T) {
	m := &RedisModule{
		connect: func(_ context.Context, cfg connConfig) (redisConn, error) {
			return nil, errors.New("dial failed for AUTH " + cfg.password)
		},
	}
	stream := &applyStream{}
	_ = m.sentinel().Apply(&pluginv1.ApplyRequest{
		State: "monitored",
		Params: mustStruct(t, map[string]any{
			"addr":      "127.0.0.1:26379",
			"password":  secretPass,
			"auth_pass": secretAuthPass,
			"monitor":   map[string]any{"ip": "10.0.0.1", "port": 6379, "quorum": 2},
		}),
	}, stream)

	fin := stream.final()
	if fin == nil || !fin.Failed {
		t.Fatalf("waited failed=true, got %+v", fin)
	}
	assertSentinelNoSecret(t, stream)
}

// assertSentinelNoSecret - no event carries auth_pass/sentinel-pass.
func assertSentinelNoSecret(t *testing.T, stream *applyStream) {
	t.Helper()
	for i, ev := range stream.sent {
		if strings.Contains(ev.GetMessage(), secretAuthPass) {
			t.Errorf("event[%d].Message contains auth_pass: %q", i, ev.GetMessage())
		}
		if out := ev.GetOutput(); out != nil {
			for k, v := range out.GetFields() {
				if strings.Contains(v.GetStringValue(), secretAuthPass) {
					t.Errorf("event[%d].Output[%s] contains auth_pass", i, k)
				}
			}
		}
	}
}
