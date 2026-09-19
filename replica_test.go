package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	pluginv1 "github.com/souls-guild/soul-stack/proto/plugin/gen/go/v1"
)

// replConn - fake redisConn for replica tests: responds to INFO replication
// given section, writes each call (for assert REPLICAOF/CONFIG SET).
type replConn struct {
	cfg        connConfig
	infoReply  string // reply to INFO replication
	calls      [][]any
	closed     bool
	failOnVerb string // if non-empty, Do returns an error on this verb (args[0])
}

func (c *replConn) Do(_ context.Context, args ...any) (string, error) {
	c.calls = append(c.calls, args)
	verb, _ := args[0].(string)
	if c.failOnVerb != "" && strings.EqualFold(verb, c.failOnVerb) {
		return "", errors.New("redis error on " + verb)
	}
	if strings.EqualFold(verb, "INFO") {
		return c.infoReply, nil
	}
	return "OK", nil
}

// ConfigGet - replica-state does not call CONFIG GET (diagnostics via INFO),
// so stub to satisfy the redisConn interface.
func (c *replConn) ConfigGet(_ context.Context, param string) (map[string]string, error) {
	return map[string]string{param: ""}, nil
}

// GetKeysInSlot - replica-state slots do not migrate, stub under the redisConn interface.
func (c *replConn) GetKeysInSlot(_ context.Context, _, _ int) ([]string, error) {
	return nil, nil
}

// AclList - replica-state ACL does not touch, stub for redisConn interface.
func (c *replConn) AclList(_ context.Context) ([]string, error) { return nil, nil }

func (c *replConn) Close() error { c.closed = true; return nil }

func replModule(conn *replConn) *RedisModule {
	return &RedisModule{
		connect: func(_ context.Context, cfg connConfig) (redisConn, error) {
			conn.cfg = cfg
			return conn, nil
		},
	}
}

// hasCall - whether there was a call with the given first tokens (prefix match).
func hasCall(calls [][]any, want ...string) bool {
	for _, call := range calls {
		if len(call) < len(want) {
			continue
		}
		ok := true
		for i, w := range want {
			s, _ := call[i].(string)
			if !strings.EqualFold(s, w) {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}

// callIndex - index of the FIRST call with the given first tokens (prefix match)
// or -1 if there is no such thing.
func callIndex(calls [][]any, want ...string) int {
	for idx, call := range calls {
		if len(call) < len(want) {
			continue
		}
		ok := true
		for i, w := range want {
			s, _ := call[i].(string)
			if !strings.EqualFold(s, w) {
				ok = false
				break
			}
		}
		if ok {
			return idx
		}
	}
	return -1
}

// callBefore - whether prefix call a was encountered BEFORE call prefix b (both must
// be present). To check the command order (tls-replication BEFORE REPLICAOF).
func callBefore(calls [][]any, a, b []string) bool {
	ia := callIndex(calls, a...)
	ib := callIndex(calls, b...)
	return ia >= 0 && ib >= 0 && ia < ib
}

// --- Validate: replica ---

func TestValidate_ReplicaRequiresMasterAddr(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.replica().Validate(context.Background(), &pluginv1.ValidateRequest{
		State:  "present",
		Params: mustStruct(t, map[string]any{"addr": "127.0.0.1:6379"}),
	})
	if reply.Ok {
		t.Fatal("expected Ok=false without master_addr")
	}
}

func TestValidate_ReplicaHappy(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.replica().Validate(context.Background(), &pluginv1.ValidateRequest{
		State:  "present",
		Params: mustStruct(t, map[string]any{"addr": "127.0.0.1:6379", "master_addr": "10.0.0.1:6379"}),
	})
	if !reply.Ok || len(reply.Errors) != 0 {
		t.Fatalf("waited Ok=true, got %+v", reply)
	}
}

// --- Apply replica: REPLICAOF + masterauth ---

func TestApplyReplica_SetsReplicaofAndMasterauth(t *testing.T) {
	// INFO: the instance is still master (does not replicate) -> needs to be configured.
	conn := &replConn{infoReply: "# Replication\r\nrole:master\r\nconnected_slaves:0\r\n"}
	m := replModule(conn)
	stream := &applyStream{}

	err := m.replica().Apply(&pluginv1.ApplyRequest{
		State: "present",
		Params: mustStruct(t, map[string]any{
			"addr":        "127.0.0.1:6379",
			"master_addr": "10.0.0.1:6379",
			"password":    secretPass,
		}),
	}, stream)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil || fin.Failed || !fin.Changed {
		t.Fatalf("expected success changed=true, got %+v", fin)
	}
	// masterauth BEFORE REPLICAOF, REPLICAOF from host port.
	if !hasCall(conn.calls, "CONFIG", "SET", "masterauth") {
		t.Errorf("no CONFIG SET masterauth: %v", conn.calls)
	}
	if !hasCall(conn.calls, "REPLICAOF", "10.0.0.1", "6379") {
		t.Errorf("no REPLICAOF 10.0.0.1 6379: %v", conn.calls)
	}
	// The password was not leaked into events or command arguments (masterauth is a separate
	// invariant: the password value is included in the CONFIG SET argument, this is valid for
	// Redis, but should NOT be included in events).
	assertEventsNoSecret(t, stream)
}

// --- Apply replica: idempotency (already a replica of the desired master) ---

func TestApplyReplica_AlreadyReplicaNoOp(t *testing.T) {
	conn := &replConn{infoReply: "# Replication\r\n" +
		"role:slave\r\n" +
		"master_host:10.0.0.1\r\n" +
		"master_port:6379\r\n" +
		"master_link_status:up\r\n"}
	m := replModule(conn)
	stream := &applyStream{}

	err := m.replica().Apply(&pluginv1.ApplyRequest{
		State: "present",
		Params: mustStruct(t, map[string]any{
			"addr":        "127.0.0.1:6379",
			"master_addr": "10.0.0.1:6379",
			"password":    secretPass,
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
		t.Error("waited changed=false: already a replica of the desired master (no-op)")
	}
	// No-op: neither REPLICAOF nor CONFIG SET masterauth.
	if hasCall(conn.calls, "REPLICAOF") {
		t.Errorf("no-op violated: REPLICAOF called: %v", conn.calls)
	}
	if hasCall(conn.calls, "CONFIG", "SET", "masterauth") {
		t.Errorf("no-op broken: masterauth set: %v", conn.calls)
	}
}

// --- Apply replica: addr == master_addr -> master does not replicate itself ---

// TestApplyReplica_SelfIsMasterNoOp tests PLUGIN-GUARD (addr==master_addr ->
// no-op) as defense-in-depth. IMPORTANT: this addr==master_addr combination in prod is NOT
// appears - in sentinel.yml addr=127.0.0.1:6379, master_addr=primary_ip (for example.
// 10.0.0.1), they are never equal. Real protection against "master replicates itself"
// yourself" - scenario `where:` (master excluded by SID), proven by L0 case
// sentinel-create-1master-2replica (there is NO replica task on the master host).
func TestApplyReplica_SelfIsMasterNoOp(t *testing.T) {
	// The same endpoint as master_addr (but through a different form of host entry).
	conn := &replConn{}
	m := replModule(conn)
	stream := &applyStream{}

	err := m.replica().Apply(&pluginv1.ApplyRequest{
		State: "present",
		Params: mustStruct(t, map[string]any{
			"addr":        "10.0.0.1:6379",
			"master_addr": "10.0.0.1:6379",
			"password":    secretPass,
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
		t.Error("waited changed=false: addr==master_addr (master, no-op)")
	}
	// master-guard fires BEFORE INFO: there should be no command.
	if len(conn.calls) != 0 {
		t.Errorf("master-guard did not work, commands called: %v", conn.calls)
	}
	if got := fin.GetOutput().GetFields()["role"].GetStringValue(); got != "master" {
		t.Errorf("role=%q, waiting for master", got)
	}
}

// --- Apply replica: password does not leak (events + connection error) ---

func TestApplyReplica_NoSecretLeak(t *testing.T) {
	conn := &replConn{infoReply: "role:master\r\n"}
	m := replModule(conn)
	stream := &applyStream{}

	_ = m.replica().Apply(&pluginv1.ApplyRequest{
		State: "present",
		Params: mustStruct(t, map[string]any{
			"addr":        "127.0.0.1:6379",
			"master_addr": "10.0.0.1:6379",
			"password":    secretPass,
		}),
	}, stream)

	assertEventsNoSecret(t, stream)
	if conn.cfg.password != secretPass {
		t.Errorf("the password did not reach the connection: %q", conn.cfg.password)
	}
}

func TestApplyReplica_ConnectFailureNoLeak(t *testing.T) {
	m := &RedisModule{
		connect: func(_ context.Context, cfg connConfig) (redisConn, error) {
			return nil, errors.New("dial failed for AUTH " + cfg.password)
		},
	}
	stream := &applyStream{}
	_ = m.replica().Apply(&pluginv1.ApplyRequest{
		State: "present",
		Params: mustStruct(t, map[string]any{
			"addr":        "127.0.0.1:6379",
			"master_addr": "10.0.0.1:6379",
			"password":    secretPass,
		}),
	}, stream)

	fin := stream.final()
	if fin == nil || !fin.Failed {
		t.Fatalf("waited failed=true, got %+v", fin)
	}
	assertEventsNoSecret(t, stream)
}

// --- Apply replica: username -> CONFIG SET masteruser ---

func TestApplyReplica_SetsMasteruserWhenUsernameGiven(t *testing.T) {
	conn := &replConn{infoReply: "role:master\r\n"}
	m := replModule(conn)
	stream := &applyStream{}

	err := m.replica().Apply(&pluginv1.ApplyRequest{
		State: "present",
		Params: mustStruct(t, map[string]any{
			"addr":        "127.0.0.1:6379",
			"master_addr": "10.0.0.1:6379",
			"password":    secretPass,
			"username":    "replicator",
		}),
	}, stream)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil || fin.Failed || !fin.Changed {
		t.Fatalf("expected success changed=true, got %+v", fin)
	}
	if !hasCall(conn.calls, "CONFIG", "SET", "masteruser", "replicator") {
		t.Errorf("no CONFIG SET masteruser replicator: %v", conn.calls)
	}
	if !hasCall(conn.calls, "REPLICAOF", "10.0.0.1", "6379") {
		t.Errorf("no REPLICAOF after masteruser: %v", conn.calls)
	}
}

func TestApplyReplica_NoUsernameSkipsMasteruser(t *testing.T) {
	conn := &replConn{infoReply: "role:master\r\n"}
	m := replModule(conn)
	stream := &applyStream{}

	_ = m.replica().Apply(&pluginv1.ApplyRequest{
		State: "present",
		Params: mustStruct(t, map[string]any{
			"addr":        "127.0.0.1:6379",
			"master_addr": "10.0.0.1:6379",
			"password":    secretPass,
		}),
	}, stream)

	if hasCall(conn.calls, "CONFIG", "SET", "masteruser") {
		t.Errorf("without username masteruser should not be installed: %v", conn.calls)
	}
}

// --- Apply replica: re-point to ANOTHER master (was a slave of another) -> changed=true ---

func TestApplyReplica_RepointFromOtherMaster(t *testing.T) {
	// Already a replica, but ANOTHER master (10.0.0.9) -> must reconfigure to 10.0.0.1.
	conn := &replConn{infoReply: "# Replication\r\n" +
		"role:slave\r\n" +
		"master_host:10.0.0.9\r\n" +
		"master_port:6379\r\n" +
		"master_link_status:up\r\n"}
	m := replModule(conn)
	stream := &applyStream{}

	err := m.replica().Apply(&pluginv1.ApplyRequest{
		State: "present",
		Params: mustStruct(t, map[string]any{
			"addr":        "127.0.0.1:6379",
			"master_addr": "10.0.0.1:6379",
			"password":    secretPass,
		}),
	}, stream)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil || fin.Failed || !fin.Changed {
		t.Fatalf("waited changed=true: the replica of someone else's master must reconfigure, got %+v", fin)
	}
	if !hasCall(conn.calls, "REPLICAOF", "10.0.0.1", "6379") {
		t.Errorf("re-point: no REPLICAOF to the desired master: %v", conn.calls)
	}
}

// --- Apply replica: correct master, but link is NOT up -> changed=true (re-sync) ---

func TestApplyReplica_RightMasterLinkDownRepoints(t *testing.T) {
	// master is the same, but link down -> NOT idempotent (you need to reinstall REPLICAOF).
	conn := &replConn{infoReply: "# Replication\r\n" +
		"role:slave\r\n" +
		"master_host:10.0.0.1\r\n" +
		"master_port:6379\r\n" +
		"master_link_status:down\r\n"}
	m := replModule(conn)
	stream := &applyStream{}

	err := m.replica().Apply(&pluginv1.ApplyRequest{
		State: "present",
		Params: mustStruct(t, map[string]any{
			"addr":        "127.0.0.1:6379",
			"master_addr": "10.0.0.1:6379",
			"password":    secretPass,
		}),
	}, stream)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil || fin.Failed || !fin.Changed {
		t.Fatalf("waited changed=true: link down -> not no-op, got %+v", fin)
	}
	if !hasCall(conn.calls, "REPLICAOF", "10.0.0.1", "6379") {
		t.Errorf("link down: repeated expected REPLICAOF: %v", conn.calls)
	}
}

// --- Apply replica: empty password -> masterauth is NOT installed ---

func TestApplyReplica_NoPasswordSkipsMasterauth(t *testing.T) {
	conn := &replConn{infoReply: "role:master\r\n"}
	m := replModule(conn)
	stream := &applyStream{}

	_ = m.replica().Apply(&pluginv1.ApplyRequest{
		State: "present",
		Params: mustStruct(t, map[string]any{
			"addr":        "127.0.0.1:6379",
			"master_addr": "10.0.0.1:6379",
		}),
	}, stream)

	if hasCall(conn.calls, "CONFIG", "SET", "masterauth") {
		t.Errorf("empty password: masterauth should not be set: %v", conn.calls)
	}
	if !hasCall(conn.calls, "REPLICAOF", "10.0.0.1", "6379") {
		t.Errorf("REPLICAOF should run without a password: %v", conn.calls)
	}
}

// --- Apply replica: source_external (external master) ---

// masterSourcePass - password of the EXTERNAL source (master_password). Different from
// secretPass (your password) to prove: masterauth is taken from the source,
// and NOT from your password. It also shouldn't leak into events.
const masterSourcePass = "vault-resolved-external-source-7b2d9e4a1c"

// TestApplyReplica_SourceExternal_MasterauthFromMasterPassword - source_external:
// masterauth is set from master_password (NOT from password). Proves (3)th
// contract clause source_external.
func TestApplyReplica_SourceExternal_MasterauthFromMasterPassword(t *testing.T) {
	conn := &replConn{infoReply: "# Replication\r\nrole:master\r\n"}
	m := replModule(conn)
	stream := &applyStream{}

	err := m.replica().Apply(&pluginv1.ApplyRequest{
		State: "present",
		Params: mustStruct(t, map[string]any{
			"addr":            "127.0.0.1:6379",
			"master_addr":     "203.0.113.5:6379", // external source
			"password":        secretPass,         // YOUR password - should NOT go to masterauth
			"source_external": true,
			"master_password": masterSourcePass,
		}),
	}, stream)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil || fin.Failed || !fin.Changed {
		t.Fatalf("expected success changed=true, got %+v", fin)
	}
	// masterauth is from master_password.
	if !hasCall(conn.calls, "CONFIG", "SET", "masterauth", masterSourcePass) {
		t.Errorf("masterauth must be set from master_password: %v", conn.calls)
	}
	// YOUR password should NOT be included in the masterauth argument.
	if hasCall(conn.calls, "CONFIG", "SET", "masterauth", secretPass) {
		t.Errorf("your password leaked to masterauth (expected master_password): %v", conn.calls)
	}
	if !hasCall(conn.calls, "REPLICAOF", "203.0.113.5", "6379") {
		t.Errorf("no REPLICAOF to external source: %v", conn.calls)
	}
	// Both passwords should not be leaked into events.
	assertEventsNoSecret(t, stream)
	for i, ev := range stream.sent {
		if strings.Contains(ev.GetMessage(), masterSourcePass) {
			t.Errorf("event[%d].Message carries master_password: %q", i, ev.GetMessage())
		}
	}
}

// TestApplyReplica_SourceExternal_SelfGuardDisabled - when source_external self-
// guard addr==master_addr DISABLED: even if the addresses match, the real
// binding (INFO + REPLICAOF) rather than a silent no-op. Proves (1) clause of the contract.
func TestApplyReplica_SourceExternal_SelfGuardDisabled(t *testing.T) {
	conn := &replConn{infoReply: "# Replication\r\nrole:master\r\n"}
	m := replModule(conn)
	stream := &applyStream{}

	err := m.replica().Apply(&pluginv1.ApplyRequest{
		State: "present",
		Params: mustStruct(t, map[string]any{
			"addr":            "203.0.113.5:6379", // matches master_addr
			"master_addr":     "203.0.113.5:6379",
			"source_external": true,
			"master_password": masterSourcePass,
		}),
	}, stream)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil || fin.Failed {
		t.Fatalf("expected not-failed, got %+v", fin)
	}
	// self-guard disabled -> INFO replication CALLED (without guard early return to INFO).
	if !hasCall(conn.calls, "INFO", "replication") {
		t.Errorf("self-guard NOT disabled: INFO replication not called (early no-op): %v", conn.calls)
	}
	// And a real REPLICAOF (not a no-op master branch).
	if !hasCall(conn.calls, "REPLICAOF", "203.0.113.5", "6379") {
		t.Errorf("expected REPLICAOF (self-guard disabled): %v", conn.calls)
	}
}

// TestApplyReplica_SourceExternal_MasteruserFromMasterUsername - masteruser
// taken from master_username (NOT from its username) with source_external.
func TestApplyReplica_SourceExternal_MasteruserFromMasterUsername(t *testing.T) {
	conn := &replConn{infoReply: "# Replication\r\nrole:master\r\n"}
	m := replModule(conn)
	stream := &applyStream{}

	_ = m.replica().Apply(&pluginv1.ApplyRequest{
		State: "present",
		Params: mustStruct(t, map[string]any{
			"addr":            "127.0.0.1:6379",
			"master_addr":     "203.0.113.5:6379",
			"username":        "own-user", // yours - should NOT go to masteruser
			"source_external": true,
			"master_password": masterSourcePass,
			"master_username": "source-replicator",
		}),
	}, stream)

	if !hasCall(conn.calls, "CONFIG", "SET", "masteruser", "source-replicator") {
		t.Errorf("masteruser must be set from master_username: %v", conn.calls)
	}
	if hasCall(conn.calls, "CONFIG", "SET", "masteruser", "own-user") {
		t.Errorf("your username leaked to masteruser (expected master_username): %v", conn.calls)
	}
}

// TestApplyReplica_SourceExternal_MasterTLS_SetsReplicationDirective -
// source_external + master_tls=true: the plugin sets CONFIG SET tls-replication yes
// (the outgoing replication link of the replica goes to TLS) BEFORE REPLICAOF. Proves
// brought TLS-to-origin (TODO S-batch removed): without REPLICAOF directive to
// A TLS-only source would run into its TLS listener.
func TestApplyReplica_SourceExternal_MasterTLS_SetsReplicationDirective(t *testing.T) {
	conn := &replConn{infoReply: "# Replication\r\nrole:master\r\n"}
	m := replModule(conn)
	stream := &applyStream{}

	err := m.replica().Apply(&pluginv1.ApplyRequest{
		State: "present",
		Params: mustStruct(t, map[string]any{
			"addr":            "127.0.0.1:6379",
			"master_addr":     "203.0.113.5:6380", // TLS source listener
			"source_external": true,
			"master_password": masterSourcePass,
			"master_tls":      true,
			"master_tls_ca":   "-----BEGIN CERTIFICATE-----\nSOURCE-CA\n-----END CERTIFICATE-----",
		}),
	}, stream)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil || fin.Failed || !fin.Changed {
		t.Fatalf("expected success changed=true, got %+v", fin)
	}
	// tls-replication yes is set.
	if !hasCall(conn.calls, "CONFIG", "SET", "tls-replication", "yes") {
		t.Errorf("no CONFIG SET tls-replication yes with master_tls: %v", conn.calls)
	}
	// tls-replication BEFORE REPLICAOF (otherwise the link will go up to plaintext before switching to TLS).
	if !callBefore(conn.calls, []string{"CONFIG", "SET", "tls-replication"}, []string{"REPLICAOF"}) {
		t.Errorf("tls-replication must be placed BEFORE REPLICAOF: %v", conn.calls)
	}
	if !hasCall(conn.calls, "REPLICAOF", "203.0.113.5", "6380") {
		t.Errorf("no REPLICAOF on TLS source: %v", conn.calls)
	}
	// Source CA (master_tls_ca) - secret, should not leak into events (its plugin
	// and does not apply: the path to disk is rendered).
	assertEventsNoSecret(t, stream)
}

// TestApplyReplica_SourceExternal_NoMasterTLS_SkipsReplicationDirective -
// source_external WITHOUT master_tls (plaintext source): tls-replication is NOT installed
// (enabling a TLS link there would be harmful - the source listens to plaintext).
func TestApplyReplica_SourceExternal_NoMasterTLS_SkipsReplicationDirective(t *testing.T) {
	conn := &replConn{infoReply: "# Replication\r\nrole:master\r\n"}
	m := replModule(conn)
	stream := &applyStream{}

	_ = m.replica().Apply(&pluginv1.ApplyRequest{
		State: "present",
		Params: mustStruct(t, map[string]any{
			"addr":            "127.0.0.1:6379",
			"master_addr":     "203.0.113.5:6379",
			"source_external": true,
			"master_password": masterSourcePass,
			// master_tls is not set (default false)
		}),
	}, stream)

	if hasCall(conn.calls, "CONFIG", "SET", "tls-replication") {
		t.Errorf("without master_tls tls-replication should not be installed: %v", conn.calls)
	}
	if !hasCall(conn.calls, "REPLICAOF", "203.0.113.5", "6379") {
		t.Errorf("REPLICAOF should be executed without TLS: %v", conn.calls)
	}
}

// TestApplyReplica_MasterTLSWithoutSourceExternal_NoDirective - master_tls=true,
// but source_external is NOT set (its own master): tls-replication is NOT set by the plugin -
// The TLS mode of the link in its incarnation is set by the general redis.conf at startup, separately
// does not need to be enabled (the directive applies only to an external source).
func TestApplyReplica_MasterTLSWithoutSourceExternal_NoDirective(t *testing.T) {
	conn := &replConn{infoReply: "# Replication\r\nrole:master\r\n"}
	m := replModule(conn)
	stream := &applyStream{}

	_ = m.replica().Apply(&pluginv1.ApplyRequest{
		State: "present",
		Params: mustStruct(t, map[string]any{
			"addr":        "127.0.0.1:6379",
			"master_addr": "10.0.0.1:6379",
			"password":    secretPass,
			"master_tls":  true, // without source_external - should not work
		}),
	}, stream)

	if hasCall(conn.calls, "CONFIG", "SET", "tls-replication") {
		t.Errorf("master_tls without source_external should not include tls-replication: %v", conn.calls)
	}
}

// TestApplyReplica_NotSourceExternal_SelfGuardActive - WITHOUT source_external
// self-guard works as before (addr==master_addr -> no-op). Regression-guard:
// The new flag didn't break the old behavior.
func TestApplyReplica_NotSourceExternal_SelfGuardActive(t *testing.T) {
	conn := &replConn{}
	m := replModule(conn)
	stream := &applyStream{}

	_ = m.replica().Apply(&pluginv1.ApplyRequest{
		State: "present",
		Params: mustStruct(t, map[string]any{
			"addr":        "10.0.0.1:6379",
			"master_addr": "10.0.0.1:6379",
			"password":    secretPass,
			// source_external is not set (default false) -> guard is active
		}),
	}, stream)

	fin := stream.final()
	if fin == nil || fin.Failed || fin.Changed {
		t.Fatalf("waited no-op changed=false (self-guard is active by default), got %+v", fin)
	}
	if len(conn.calls) != 0 {
		t.Errorf("self-guard should fire BEFORE INFO (without source_external): %v", conn.calls)
	}
}
