package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	pluginv1 "github.com/souls-guild/soul-stack/proto/plugin/gen/go/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

// secretPass - a password that should NEVER be leaked to events/errors/stderr
// (IS-invariant ADR-010). Long/unique so that substring searches are reliable.
const secretPass = "vault-resolved-supersecret-9f3a7c1e2b"

// fakeConn - in-memory redisConn: writes every Do call, returns scripted
// answers. seenPassword is recorded to prove: the password goes into the connection, but NOT
// into command arguments.
//
// configLive simulates the current values CONFIG GET <param> (for fair diff
// config-state): empty/no key -> "param" pair (value "").
type fakeConn struct {
	cfg        connConfig
	calls      [][]any
	results    map[string]string // key - args[0]; "" -> echo "OK"
	configLive map[string]string // CONFIG GET <param> -> current value
	doErr      error
	closed     bool

	// aclSeq - sequential ACL LIST responses (one per call: before, after).
	// If a longer one is not needed, repeat the last one. nil -> empty list.
	aclSeq   [][]string
	aclCalls int
	aclErr   error // ACL LIST error (for error-path acl-state)
}

func (f *fakeConn) Do(_ context.Context, args ...any) (string, error) {
	f.calls = append(f.calls, args)
	if f.doErr != nil {
		return "", f.doErr
	}
	if len(args) == 0 {
		return "", nil
	}
	verb, _ := args[0].(string)
	if r, ok := f.results[verb]; ok {
		return r, nil
	}
	return "OK", nil
}

// ConfigGet - typed path: returns {param: value} from configLive WITHOUT
// space-join, so multiword values (save) are saved in their entirety (parity
// with real go-redis ConfigGet -> map[string]string). We record the call in calls,
// so that the CONFIG GET diff probe remains visible to assert.
func (f *fakeConn) ConfigGet(_ context.Context, param string) (map[string]string, error) {
	f.calls = append(f.calls, []any{"CONFIG", "GET", param})
	if f.doErr != nil {
		return nil, f.doErr
	}
	return map[string]string{param: f.configLive[param]}, nil
}

// GetKeysInSlot - command/config tests, slots do not migrate, stub under the interface.
func (f *fakeConn) GetKeysInSlot(_ context.Context, _, _ int) ([]string, error) {
	return nil, nil
}

// AclList - returns the next aclSeq element (before/after for diff acl-state),
// fixing the serial call. Records the call in calls (visible for assert order
// LIST -> LOAD -> LIST). Longer aclSeq - repeats the last one.
func (f *fakeConn) AclList(_ context.Context) ([]string, error) {
	f.calls = append(f.calls, []any{"ACL", "LIST"})
	if f.aclErr != nil {
		return nil, f.aclErr
	}
	i := f.aclCalls
	f.aclCalls++
	if i >= len(f.aclSeq) {
		if len(f.aclSeq) == 0 {
			return nil, nil
		}
		return f.aclSeq[len(f.aclSeq)-1], nil
	}
	return f.aclSeq[i], nil
}

func (f *fakeConn) Close() error { f.closed = true; return nil }

// newModule assembles a RedisModule with an injected fakeConn and returns both.
func newModule(conn *fakeConn) *RedisModule {
	return &RedisModule{
		connect: func(_ context.Context, cfg connConfig) (redisConn, error) {
			conn.cfg = cfg
			return conn, nil
		},
	}
}

// applyStream - local fake grpc-stream (parity with sdk fakeApplyStream).
type applyStream struct {
	grpc.ServerStreamingServer[pluginv1.ApplyEvent]
	sent []*pluginv1.ApplyEvent
}

func (s *applyStream) Send(e *pluginv1.ApplyEvent) error { s.sent = append(s.sent, e); return nil }
func (s *applyStream) Context() context.Context          { return context.Background() }

func (s *applyStream) final() *pluginv1.ApplyEvent {
	if len(s.sent) == 0 {
		return nil
	}
	return s.sent[len(s.sent)-1]
}

func mustStruct(t *testing.T, m map[string]any) *structpb.Struct {
	t.Helper()
	s, err := structpb.NewStruct(m)
	if err != nil {
		t.Fatalf("structpb.NewStruct: %v", err)
	}
	return s
}

// --- Validate ---

func TestValidate_CommandRejectsEmptyArgs(t *testing.T) {
	m := &RedisModule{}
	reply, err := m.command().Validate(context.Background(), &pluginv1.ValidateRequest{
		State:  "run",
		Params: mustStruct(t, map[string]any{"addr": "127.0.0.1:6379", "args": []any{}}),
	})
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if reply.Ok {
		t.Fatal("expected Ok=false on empty args")
	}
}

func TestValidate_ConfigRejectsEmptyMap(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.instance().Validate(context.Background(), &pluginv1.ValidateRequest{
		State:  "configured",
		Params: mustStruct(t, map[string]any{"addr": "127.0.0.1:6379", "config": map[string]any{}}),
	})
	if reply.Ok {
		t.Fatal("waited Ok=false on empty config")
	}
}

func TestValidate_RejectsEmptyAddr(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.command().Validate(context.Background(), &pluginv1.ValidateRequest{
		State:  "run",
		Params: mustStruct(t, map[string]any{"addr": "", "args": []any{"PING"}}),
	})
	if reply.Ok {
		t.Fatal("waited Ok=false on empty addr")
	}
}

// TestValidate_RejectsUnknownState - a state no object serves is refused, and so
// is one ANOTHER object serves. The second half is the object boundary itself
// (NIM-766): `instance` and `cluster` share one driver, and if the boundary were
// only a naming convention, `redis.instance.created` would build a cluster.
func TestValidate_RejectsUnknownState(t *testing.T) {
	cases := []struct {
		name  string
		obj   func(*RedisModule) *object
		state string
	}{
		{"nobody serves it", (*RedisModule).instance, "failover"},
		{"another object serves it", (*RedisModule).instance, "created"},
		{"another object serves it (reverse)", (*RedisModule).cluster, "pinged"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := &RedisModule{}
			reply, _ := tc.obj(m).Validate(context.Background(), &pluginv1.ValidateRequest{
				State:  tc.state,
				Params: mustStruct(t, map[string]any{"addr": "127.0.0.1:6379"}),
			})
			if reply.Ok {
				t.Fatalf("expected Ok=false for state %q", tc.state)
			}
		})
	}
}

// TestApply_RejectsForeignState - the same boundary on the Apply path, which is
// the one that would actually change a host. Validate is a separate RPC a runner
// need not call, so the refusal cannot live there alone.
func TestApply_RejectsForeignState(t *testing.T) {
	conn := &fakeConn{}
	m := newModule(conn)
	stream := &applyStream{}
	_ = m.instance().Apply(&pluginv1.ApplyRequest{
		State:  "created",
		Params: mustStruct(t, map[string]any{"addr": "127.0.0.1:6379"}),
	}, stream)

	fin := stream.final()
	if fin == nil || !fin.Failed {
		t.Fatalf("expected failed=true for a cluster action on instance, got %+v", fin)
	}
	if len(conn.calls) != 0 {
		t.Fatalf("a refused state still reached Redis: %v", conn.calls)
	}
}

func TestValidate_HappyPath(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.command().Validate(context.Background(), &pluginv1.ValidateRequest{
		State:  "run",
		Params: mustStruct(t, map[string]any{"addr": "127.0.0.1:6379", "args": []any{"PING"}}),
	})
	if !reply.Ok || len(reply.Errors) != 0 {
		t.Fatalf("waited Ok=true without errors, got %+v", reply)
	}
}

// --- Apply: command ---

func TestApplyCommand_HappyPath_ChangedFalseByDefault(t *testing.T) {
	conn := &fakeConn{results: map[string]string{"PING": "PONG"}}
	m := newModule(conn)
	stream := &applyStream{}

	err := m.command().Apply(&pluginv1.ApplyRequest{
		State: "run",
		Params: mustStruct(t, map[string]any{
			"addr":     "127.0.0.1:6379",
			"password": secretPass,
			"args":     []any{"PING"},
		}),
	}, stream)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil || fin.Failed {
		t.Fatalf("were waiting for a successful final event, got %+v", fin)
	}
	if fin.Changed {
		t.Error("changed should be false by default (probe semantics)")
	}
	if got := fin.GetOutput().GetFields()["result"].GetStringValue(); got != "PONG" {
		t.Errorf("result=%q, expected PONG", got)
	}
	// The password went into the connection, but NOT into the command arguments.
	if conn.cfg.password != secretPass {
		t.Errorf("the password did not reach the connection: %q", conn.cfg.password)
	}
	assertNoCommandCarriesSecret(t, conn)
	if !conn.closed {
		t.Error("connection not closed")
	}
}

func TestApplyCommand_ChangedTrueWhenRequested(t *testing.T) {
	conn := &fakeConn{}
	m := newModule(conn)
	stream := &applyStream{}

	_ = m.command().Apply(&pluginv1.ApplyRequest{
		State: "run",
		Params: mustStruct(t, map[string]any{
			"addr":    "127.0.0.1:6379",
			"args":    []any{"FLUSHDB"},
			"changed": true,
		}),
	}, stream)

	if fin := stream.final(); fin == nil || !fin.Changed {
		t.Fatalf("expected changed=true with changed:true in params, got %+v", fin)
	}
}

func TestApplyCommand_RedisErrorIsFailure(t *testing.T) {
	conn := &fakeConn{doErr: errors.New("WRONGPASS invalid username-password pair")}
	m := newModule(conn)
	stream := &applyStream{}

	_ = m.command().Apply(&pluginv1.ApplyRequest{
		State:  "run",
		Params: mustStruct(t, map[string]any{"addr": "127.0.0.1:6379", "args": []any{"PING"}}),
	}, stream)

	if fin := stream.final(); fin == nil || !fin.Failed {
		t.Fatalf("waited failed=true for error Redis, got %+v", fin)
	}
}

// --- Apply: config ---

func TestApplyConfig_HappyPath_ChangedTrue(t *testing.T) {
	conn := &fakeConn{}
	m := newModule(conn)
	stream := &applyStream{}

	err := m.instance().Apply(&pluginv1.ApplyRequest{
		State: "configured",
		Params: mustStruct(t, map[string]any{
			"addr":     "127.0.0.1:6379",
			"password": secretPass,
			"config": map[string]any{
				"maxmemory":        "256mb",
				"maxmemory-policy": "allkeys-lru",
			},
		}),
	}, stream)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil || fin.Failed || !fin.Changed {
		t.Fatalf("expected success changed=true, got %+v", fin)
	}
	// Honest diff: live is empty -> both directives are different -> GET before each SET,
	// then SET (deterministic order by key).
	wantCalls := [][]any{
		{"CONFIG", "GET", "maxmemory"},
		{"CONFIG", "SET", "maxmemory", "256mb"},
		{"CONFIG", "GET", "maxmemory-policy"},
		{"CONFIG", "SET", "maxmemory-policy", "allkeys-lru"},
	}
	assertCalls(t, conn, wantCalls)
	assertNoCommandCarriesSecret(t, conn)
}

// TestApplyConfig_NoOpWhenLiveMatches - fair diff: live is already on the desired one
// value -> CONFIG SET is NOT called, changed=false (idempotency, M3-fix).
func TestApplyConfig_NoOpWhenLiveMatches(t *testing.T) {
	conn := &fakeConn{configLive: map[string]string{
		"maxmemory":        "256mb",
		"maxmemory-policy": "allkeys-lru",
	}}
	m := newModule(conn)
	stream := &applyStream{}

	err := m.instance().Apply(&pluginv1.ApplyRequest{
		State: "configured",
		Params: mustStruct(t, map[string]any{
			"addr": "127.0.0.1:6379",
			"config": map[string]any{
				"maxmemory":        "256mb",
				"maxmemory-policy": "allkeys-lru",
			},
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
		t.Error("waited changed=false: live already on the desired one (no-op, M3 fair diff)")
	}
	if hasCall(conn.calls, "CONFIG", "SET") {
		t.Errorf("no-op broken: CONFIG SET called when live: %v matches", conn.calls)
	}
	// GET for both directives still passed (diff test).
	if !hasCall(conn.calls, "CONFIG", "GET", "maxmemory") {
		t.Errorf("CONFIG GET maxmemory should be executed for diff: %v", conn.calls)
	}
}

// TestApplyConfig_PartialDiff - some of the directives matched, some did not: SET only for
// really different, changed=true, count - according to those applied.
func TestApplyConfig_PartialDiff(t *testing.T) {
	conn := &fakeConn{configLive: map[string]string{
		"maxmemory":        "256mb",      // matches -> no-op
		"maxmemory-policy": "noeviction", // different -> SET
	}}
	m := newModule(conn)
	stream := &applyStream{}

	_ = m.instance().Apply(&pluginv1.ApplyRequest{
		State: "configured",
		Params: mustStruct(t, map[string]any{
			"addr": "127.0.0.1:6379",
			"config": map[string]any{
				"maxmemory":        "256mb",
				"maxmemory-policy": "allkeys-lru",
			},
		}),
	}, stream)

	fin := stream.final()
	if fin == nil || fin.Failed || !fin.Changed {
		t.Fatalf("expected success changed=true (one directive is different), got %+v", fin)
	}
	if hasCall(conn.calls, "CONFIG", "SET", "maxmemory", "256mb") {
		t.Errorf("the matching directive should not be SET: %v", conn.calls)
	}
	if !hasCall(conn.calls, "CONFIG", "SET", "maxmemory-policy", "allkeys-lru") {
		t.Errorf("a different directive must be SET: %v", conn.calls)
	}
	if got := fin.GetOutput().GetFields()["count"].GetNumberValue(); got != 1 {
		t.Errorf("count=%v, waited 1 (one applied directive)", got)
	}
}

// TestApplyConfig_MultiwordValueNoOp - P3 regression: multiword value
// (save "900 1 300 10 60 10000") with space-join+strings.Fields would crumble into
// mixed up pairs -> current != want -> false CONFIG SET (loss
// idempotency on day-2 update_config). Typed ConfigGet saves
// entire meaning -> live == want -> no-op.
func TestApplyConfig_MultiwordValueNoOp(t *testing.T) {
	const save = "900 1 300 10 60 10000"
	conn := &fakeConn{configLive: map[string]string{"save": save}}
	m := newModule(conn)
	stream := &applyStream{}

	err := m.instance().Apply(&pluginv1.ApplyRequest{
		State: "configured",
		Params: mustStruct(t, map[string]any{
			"addr":   "127.0.0.1:6379",
			"config": map[string]any{"save": save},
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
		t.Error("expected changed=false: live save matches the desired one (the verbose value is saved in its entirety)")
	}
	if hasCall(conn.calls, "CONFIG", "SET") {
		t.Errorf("false CONFIG SET on matched multiword value (P3): %v", conn.calls)
	}
}

func TestApplyConfig_NumericValueStringified(t *testing.T) {
	conn := &fakeConn{}
	m := newModule(conn)
	stream := &applyStream{}

	_ = m.instance().Apply(&pluginv1.ApplyRequest{
		State: "configured",
		Params: mustStruct(t, map[string]any{
			"addr":   "127.0.0.1:6379",
			"config": map[string]any{"maxclients": 20000},
		}),
	}, stream)

	if fin := stream.final(); fin == nil || fin.Failed {
		t.Fatalf("expected success, got %+v", fin)
	}
	// GET (live is empty) + SET with stringified value.
	if !hasCall(conn.calls, "CONFIG", "SET", "maxclients", "20000") {
		t.Errorf("waited CONFIG SET maxclients 20000 (stringified), got %v", conn.calls)
	}
}

func TestApplyConfig_RewriteCallsConfigRewrite(t *testing.T) {
	conn := &fakeConn{}
	m := newModule(conn)
	stream := &applyStream{}

	_ = m.instance().Apply(&pluginv1.ApplyRequest{
		State: "configured",
		Params: mustStruct(t, map[string]any{
			"addr":    "127.0.0.1:6379",
			"config":  map[string]any{"maxmemory": "512mb"},
			"rewrite": true,
		}),
	}, stream)

	last := conn.calls[len(conn.calls)-1]
	if len(last) != 2 || last[0] != "CONFIG" || last[1] != "REWRITE" {
		t.Errorf("expected the final CONFIG REWRITE, got %v", last)
	}
}

// TestApplyConfig_StartupOnlyDirectivesSkipped - denylist startup-only: CONFIG SET
// rejects them, day-2 renders the FULL redis.conf (with them), so the plugin them
// SKIPS (does not fall). Hot-settable directives of the same call are applied as
// usually. We check: neither CONFIG GET nor CONFIG SET by startup-only are called;
// hot-settable applied; skipped/skippedCount in Output are correct.
func TestApplyConfig_StartupOnlyDirectivesSkipped(t *testing.T) {
	conn := &fakeConn{}
	m := newModule(conn)
	stream := &applyStream{}

	err := m.instance().Apply(&pluginv1.ApplyRequest{
		State: "configured",
		Params: mustStruct(t, map[string]any{
			"addr": "127.0.0.1:6379",
			"config": map[string]any{
				// startup-only - must be skipped (CONFIG SET rejects them):
				"port":            "6379",
				"dir":             "/var/lib/redis",
				"aclfile":         "/etc/redis/users.acl",
				"cluster-enabled": "yes",
				"loadmodule":      "/x.so",
				// hot-settable - should apply:
				"maxmemory": "512mb",
			},
		}),
	}, stream)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil || fin.Failed {
		t.Fatalf("expected success (startup-only skipped, no crash), got %+v", fin)
	}
	if !fin.Changed {
		t.Error("waited changed=true: hot-settable maxmemory applied")
	}

	// No startup-only directive should reach either GET or SET.
	for _, k := range []string{"port", "dir", "aclfile", "cluster-enabled", "loadmodule"} {
		if hasCall(conn.calls, "CONFIG", "GET", k) || hasCall(conn.calls, "CONFIG", "SET", k) {
			t.Errorf("startup-only %q should not be included in CONFIG GET/SET: %v", k, conn.calls)
		}
	}
	// Hot-settable applied.
	if !hasCall(conn.calls, "CONFIG", "SET", "maxmemory", "512mb") {
		t.Errorf("hot-settable maxmemory should apply: %v", conn.calls)
	}
	// Output: applied/count by hot-settable, skipped/skippedCount by startup-only.
	out := fin.GetOutput().GetFields()
	if got := out["count"].GetNumberValue(); got != 1 {
		t.Errorf("count=%v, waited 1 (one hot-settable)", got)
	}
	if got := out["skippedCount"].GetNumberValue(); got != 5 {
		t.Errorf("skippedCount=%v, waited 5 (port/dir/aclfile/cluster-enabled/loadmodule)", got)
	}
	skipped := out["skipped"].GetStringValue()
	for _, k := range []string{"port", "dir", "aclfile", "cluster-enabled", "loadmodule"} {
		if !strings.Contains(skipped, k) {
			t.Errorf("skipped does not contain %q: %q", k, skipped)
		}
	}
}

// TestApplyConfig_AllStartupOnly_NoChange - if ALL directives are startup-only,
// CONFIG SET is not called even once, changed=false (there is nothing to use hot), the run is not
// falls. Borderline denilista case: rewrite does not twitch either (len(applied)==0).
func TestApplyConfig_AllStartupOnly_NoChange(t *testing.T) {
	conn := &fakeConn{}
	m := newModule(conn)
	stream := &applyStream{}

	_ = m.instance().Apply(&pluginv1.ApplyRequest{
		State: "configured",
		Params: mustStruct(t, map[string]any{
			"addr":    "127.0.0.1:6379",
			"config":  map[string]any{"port": "6379", "dir": "/var/lib/redis"},
			"rewrite": true,
		}),
	}, stream)

	fin := stream.final()
	if fin == nil || fin.Failed {
		t.Fatalf("expected success (all startup-only skipped), got %+v", fin)
	}
	if fin.Changed {
		t.Error("expected changed=false: there is nothing to apply hot (all directives are startup-only)")
	}
	if hasCall(conn.calls, "CONFIG", "SET") {
		t.Errorf("CONFIG SET should not be called for all startup-only: %v", conn.calls)
	}
	if hasCall(conn.calls, "CONFIG", "REWRITE") {
		t.Errorf("CONFIG REWRITE should not be called when applied: %v", conn.calls)
	}
}

// --- Validate: acl ---

func TestValidate_AclRequiresAddr(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.acl().Validate(context.Background(), &pluginv1.ValidateRequest{
		State:  "reloaded",
		Params: mustStruct(t, map[string]any{"addr": ""}),
	})
	if reply.Ok {
		t.Fatal("waited Ok=false on an empty addr for acl")
	}
}

func TestValidate_AclHappyPath(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.acl().Validate(context.Background(), &pluginv1.ValidateRequest{
		State:  "reloaded",
		Params: mustStruct(t, map[string]any{"addr": "127.0.0.1:6379"}),
	})
	if !reply.Ok || len(reply.Errors) != 0 {
		t.Fatalf("waited Ok=true without errors, got %+v", reply)
	}
}

// --- Apply: acl ---

// TestApplyACL_SendsAclLoad - base contract: acl-state sends ACL LOAD between
// two ACL LIST (diff test), the connection receives a password, the command arguments do not
// carried, the connection is closed.
func TestApplyACL_SendsAclLoad(t *testing.T) {
	conn := &fakeConn{aclSeq: [][]string{
		{"user default on nopass ~* +@all"},
		{"user default on nopass ~* +@all", "user alice on #abc ~app:* +@read"}, // after LOAD alice was added
	}}
	m := newModule(conn)
	stream := &applyStream{}

	err := m.acl().Apply(&pluginv1.ApplyRequest{
		State: "reloaded",
		Params: mustStruct(t, map[string]any{
			"addr":     "127.0.0.1:6379",
			"password": secretPass,
		}),
	}, stream)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil || fin.Failed {
		t.Fatalf("were waiting for a successful final event, got %+v", fin)
	}
	// Order: ACL LIST (before) -> ACL LOAD -> ACL LIST (after).
	wantCalls := [][]any{
		{"ACL", "LIST"},
		{"ACL", "LOAD"},
		{"ACL", "LIST"},
	}
	assertCalls(t, conn, wantCalls)
	if conn.cfg.password != secretPass {
		t.Errorf("the password did not reach the connection: %q", conn.cfg.password)
	}
	assertNoCommandCarriesSecret(t, conn)
	if !conn.closed {
		t.Error("connection not closed")
	}
}

// TestApplyACL_ChangedTrueWhenAclDiffers - ACL LIST before/after differs (LOAD
// I actually re-read the file with the new rules) -> changed=true, users - by after.
func TestApplyACL_ChangedTrueWhenAclDiffers(t *testing.T) {
	conn := &fakeConn{aclSeq: [][]string{
		{"user default on nopass ~* +@all"},
		{"user default on nopass ~* +@all", "user alice on #abc ~app:* +@read"},
	}}
	m := newModule(conn)
	stream := &applyStream{}

	_ = m.acl().Apply(&pluginv1.ApplyRequest{
		State:  "reloaded",
		Params: mustStruct(t, map[string]any{"addr": "127.0.0.1:6379"}),
	}, stream)

	fin := stream.final()
	if fin == nil || fin.Failed || !fin.Changed {
		t.Fatalf("expected success changed=true (ACL has changed), got %+v", fin)
	}
	if got := fin.GetOutput().GetFields()["users"].GetNumberValue(); got != 2 {
		t.Errorf("users=%v, waited 2 (after-list)", got)
	}
}

// TestApplyACL_ChangedFalseWhenAclUnchanged - the live instance has already matched
// aclfile: ACL LIST before/after is identical -> changed=false (no-op, idempotency
// like config/cluster/sentinel). ACL LOAD is still executed
// (reduction to the declared one - by construction).
func TestApplyACL_ChangedFalseWhenAclUnchanged(t *testing.T) {
	same := []string{"user default on nopass ~* +@all", "user alice on #abc ~app:* +@read"}
	conn := &fakeConn{aclSeq: [][]string{same, same}}
	m := newModule(conn)
	stream := &applyStream{}

	_ = m.acl().Apply(&pluginv1.ApplyRequest{
		State:  "reloaded",
		Params: mustStruct(t, map[string]any{"addr": "127.0.0.1:6379"}),
	}, stream)

	fin := stream.final()
	if fin == nil || fin.Failed {
		t.Fatalf("expected success, got %+v", fin)
	}
	if fin.Changed {
		t.Error("waited changed=false: ACL LIST before/after matched (no-op)")
	}
	if !hasCall(conn.calls, "ACL", "LOAD") {
		t.Errorf("ACL LOAD must be executed even on a no-op (cast by construction): %v", conn.calls)
	}
}

// TestApplyACL_LoadErrorIsFailure - ACL LOAD failed (broken/unconfigured
// aclfile) -> failed=true with a clear message.
func TestApplyACL_LoadErrorIsFailure(t *testing.T) {
	conn := &fakeConn{doErr: errors.New("ERR This Redis instance is not configured to use an ACL file.")}
	m := newModule(conn)
	stream := &applyStream{}

	_ = m.acl().Apply(&pluginv1.ApplyRequest{
		State:  "reloaded",
		Params: mustStruct(t, map[string]any{"addr": "127.0.0.1:6379"}),
	}, stream)

	fin := stream.final()
	if fin == nil || !fin.Failed {
		t.Fatalf("waited failed=true for ACL LOAD error, got %+v", fin)
	}
	if !strings.Contains(fin.GetMessage(), "ACL LOAD") {
		t.Errorf("Expected a clear prefix 'ACL LOAD' in the message, got %q", fin.GetMessage())
	}
}

// TestApplyACL_ListErrorIsFailure - ACL LIST (before) unavailable -> failed, ACL
// LOAD is NOT executed (there is no point of comparison - we do not touch the live instance blindly).
func TestApplyACL_ListErrorIsFailure(t *testing.T) {
	conn := &fakeConn{aclErr: errors.New("WRONGPASS invalid username-password pair")}
	m := newModule(conn)
	stream := &applyStream{}

	_ = m.acl().Apply(&pluginv1.ApplyRequest{
		State:  "reloaded",
		Params: mustStruct(t, map[string]any{"addr": "127.0.0.1:6379"}),
	}, stream)

	fin := stream.final()
	if fin == nil || !fin.Failed {
		t.Fatalf("waited failed=true for ACL LIST error, got %+v", fin)
	}
	if hasCall(conn.calls, "ACL", "LOAD") {
		t.Errorf("ACL LOAD should not be executed when ACL LIST is not available (before): %v", conn.calls)
	}
}

// TestApplyACL_ConnectFailure_DoesNotLeakPassword - connection error, whose text
// CONTAINS password, sanitized (general path Apply, redactError).
func TestApplyACL_ConnectFailure_DoesNotLeakPassword(t *testing.T) {
	m := &RedisModule{
		connect: func(_ context.Context, cfg connConfig) (redisConn, error) {
			return nil, errors.New("dial failed for AUTH " + cfg.password)
		},
	}
	stream := &applyStream{}
	_ = m.acl().Apply(&pluginv1.ApplyRequest{
		State: "reloaded",
		Params: mustStruct(t, map[string]any{
			"addr":     "127.0.0.1:6379",
			"password": secretPass,
		}),
	}, stream)

	fin := stream.final()
	if fin == nil || !fin.Failed {
		t.Fatalf("waited failed=true, got %+v", fin)
	}
	assertEventsNoSecret(t, stream)
}

// TestApplyACL_TLSParamsReachConnect - TLS parameters (tls/tls_ca) reach
// connection (acl reads the same set as config/command - the general path).
func TestApplyACL_TLSParamsReachConnect(t *testing.T) {
	var captured connConfig
	m := &RedisModule{
		connect: func(_ context.Context, cfg connConfig) (redisConn, error) {
			captured = cfg
			return &fakeConn{}, nil
		},
	}
	const caPEM = "-----BEGIN CERTIFICATE-----\nFAKE\n-----END CERTIFICATE-----"
	_ = m.acl().Apply(&pluginv1.ApplyRequest{
		State: "reloaded",
		Params: mustStruct(t, map[string]any{
			"addr":   "127.0.0.1:6379",
			"tls":    true,
			"tls_ca": caPEM,
		}),
	}, &applyStream{})

	if !captured.tls.enabled {
		t.Error("tls=true did not reach the acl-state connection")
	}
	if captured.tls.caPEM != caPEM {
		t.Errorf("tls_ca did not reach the connection: %q", captured.tls.caPEM)
	}
}

// --- Apply: unix-socket addr ---

func TestApply_UnixSocketAddrParsed(t *testing.T) {
	var captured connConfig
	m := &RedisModule{
		connect: func(_ context.Context, cfg connConfig) (redisConn, error) {
			captured = cfg
			return &fakeConn{}, nil
		},
	}
	_ = m.command().Apply(&pluginv1.ApplyRequest{
		State:  "run",
		Params: mustStruct(t, map[string]any{"addr": "unix:/var/run/redis/redis-server.sock", "args": []any{"PING"}}),
	}, &applyStream{})

	if captured.addr != "unix:/var/run/redis/redis-server.sock" {
		t.Errorf("addr did not reach the connection: %q", captured.addr)
	}
}

// --- Cybersecurity invariant: password does not leak ---

// TestApply_ConnectFailure_DoesNotLeakPassword - connection error, whose text
// CONTAINS a password, must be sanitized in the (redactError) event.
func TestApply_ConnectFailure_DoesNotLeakPassword(t *testing.T) {
	m := &RedisModule{
		connect: func(_ context.Context, cfg connConfig) (redisConn, error) {
			// Let's simulate the worst-case: the driver put the password in the error text.
			return nil, errors.New("dial failed for AUTH " + cfg.password)
		},
	}
	stream := &applyStream{}
	_ = m.command().Apply(&pluginv1.ApplyRequest{
		State: "run",
		Params: mustStruct(t, map[string]any{
			"addr":     "127.0.0.1:6379",
			"password": secretPass,
			"args":     []any{"PING"},
		}),
	}, stream)

	fin := stream.final()
	if fin == nil || !fin.Failed {
		t.Fatalf("waited failed=true, got %+v", fin)
	}
	assertEventsNoSecret(t, stream)
}

// TestApply_NoEventCarriesSecret - end-to-end verification: no events
// (command + config happy-path) does not contain the password in either the Message or Output.
func TestApply_NoEventCarriesSecret(t *testing.T) {
	for _, tc := range []struct {
		name   string
		obj    func(*RedisModule) *object
		state  string
		params map[string]any
	}{
		{"command.run", (*RedisModule).command, "run", map[string]any{"addr": "127.0.0.1:6379", "password": secretPass, "args": []any{"GET", "key"}}},
		{"instance.configured", (*RedisModule).instance, "configured", map[string]any{"addr": "127.0.0.1:6379", "password": secretPass, "config": map[string]any{"maxmemory": "256mb"}}},
		{"acl.reloaded", (*RedisModule).acl, "reloaded", map[string]any{"addr": "127.0.0.1:6379", "password": secretPass}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := &fakeConn{}
			m := newModule(conn)
			stream := &applyStream{}
			_ = tc.obj(m).Apply(&pluginv1.ApplyRequest{State: tc.state, Params: mustStruct(t, tc.params)}, stream)
			assertEventsNoSecret(t, stream)
			assertNoCommandCarriesSecret(t, conn)
		})
	}
}

// TestApplyConfig_SecretLeakInDriverError_Redacted - the driver returned an error,
// CONTAINING the value of the directive (requirepass): applyConfig must cut it
// via redactError. Proves that error-path config-state is symmetric
// replica/cluster/sentinel (M4, defense-in-depth).
func TestApplyConfig_SecretLeakInDriverError_Redacted(t *testing.T) {
	conn := &leakyConfigConn{secret: secretPass}
	m := &RedisModule{
		connect: func(_ context.Context, cfg connConfig) (redisConn, error) { return conn, nil },
	}
	stream := &applyStream{}

	_ = m.instance().Apply(&pluginv1.ApplyRequest{
		State: "configured",
		Params: mustStruct(t, map[string]any{
			"addr":   "127.0.0.1:6379",
			"config": map[string]any{"requirepass": secretPass},
		}),
	}, stream)

	fin := stream.final()
	if fin == nil || !fin.Failed {
		t.Fatalf("waited failed=true, got %+v", fin)
	}
	if strings.Contains(fin.GetMessage(), secretPass) {
		t.Errorf("the value of the directive (secret) was leaked in the error Message: %q", fin.GetMessage())
	}
	if !strings.Contains(fin.GetMessage(), "***") {
		t.Errorf("expected mask *** in edited error, got %q", fin.GetMessage())
	}
}

// leakyConfigConn simulates a driver echoing the value of a SET argument to text
// errors (worst-case for checking redactError on error-path config-state).
type leakyConfigConn struct {
	secret string
}

func (c *leakyConfigConn) Do(_ context.Context, args ...any) (string, error) {
	if len(args) >= 4 && args[0] == "CONFIG" && args[1] == "SET" {
		val, _ := args[3].(string)
		return "", errors.New("ERR could not set value to " + val) // secret echoes
	}
	return "OK", nil
}

// ConfigGet - live is empty -> diff will work, applyConfig will reach CONFIG SET.
func (c *leakyConfigConn) ConfigGet(_ context.Context, param string) (map[string]string, error) {
	return map[string]string{param: ""}, nil
}
func (c *leakyConfigConn) GetKeysInSlot(_ context.Context, _, _ int) ([]string, error) {
	return nil, nil
}
func (c *leakyConfigConn) AclList(_ context.Context) ([]string, error) { return nil, nil }
func (c *leakyConfigConn) Close() error                                { return nil }

// --- redactError: regression for short substring passwords ---

// TestRedactError_ShortPasswordSubstring - substring password that matches the part
// harmless text ("6379" in "127.0.0.1:6379"), ReplaceAll will overwrite AND the address.
// The test FIXES the current behavior (fragility of ReplaceAll on short secrets):
// ANY occurrence of the substring is masked. The information security invariant is met (the secret is not visible),
// at the cost of possible over-concealment of diagnosis. Regression catches change.
func TestRedactError_ShortPasswordSubstring(t *testing.T) {
	err := errors.New("dial 127.0.0.1:6379: connection refused")
	got := redactError(err, "6379")
	if strings.Contains(got, "6379") {
		t.Errorf("secret-substring must be stripped from everywhere, got %q", got)
	}
	// We document over-masking: the address is also affected - EXPECTED with a short secret.
	if !strings.Contains(got, "127.0.0.1:***") {
		t.Errorf("expected a worn out port (over-masking is fixed): %q", got)
	}
	// An empty password is no-op.
	if redactError(errors.New("plain error"), "") != "plain error" {
		t.Error("empty password should be no-op for redactError")
	}
}

// --- valueToString / args with value containing space ---

// TestStringList_PreservesValueWithSpace - guard: the value of the args element with
// space ("hello world") comes as ONE command argument and does not lose
// structure (each list element = separate arg, without gluing/Fields).
func TestStringList_PreservesValueWithSpace(t *testing.T) {
	conn := &fakeConn{}
	m := newModule(conn)
	stream := &applyStream{}

	_ = m.command().Apply(&pluginv1.ApplyRequest{
		State: "run",
		Params: mustStruct(t, map[string]any{
			"addr": "127.0.0.1:6379",
			"args": []any{"SET", "greeting", "hello world"},
		}),
	}, stream)

	if len(conn.calls) != 1 {
		t.Fatalf("expected one command call, got %v", conn.calls)
	}
	call := conn.calls[0]
	if len(call) != 3 {
		t.Fatalf("expected 3 arguments (SET greeting 'hello world'), got %d: %v", len(call), call)
	}
	if call[2] != "hello world" {
		t.Errorf("a value with a space has lost its structure: arg[2]=%v (expected 'hello world')", call[2])
	}
}

// TestStringMap_NumericAndStringValuesStringified - guard valueToString in composition
// config-map: number and bool are stringified predictably (256 not 256.000000;
// true is not empty) - Redis is waiting for a line.
func TestStringMap_NumericAndStringValuesStringified(t *testing.T) {
	conn := &fakeConn{}
	m := newModule(conn)
	stream := &applyStream{}

	_ = m.instance().Apply(&pluginv1.ApplyRequest{
		State: "configured",
		Params: mustStruct(t, map[string]any{
			"addr": "127.0.0.1:6379",
			"config": map[string]any{
				"maxclients":             20000,      // number -> "20000"
				"appendfsync":            "everysec", // line as is
				"lazyfree-lazy-eviction": true,       // bool -> "true" (the operator is warned: Redis expects yes/no - this is his choice)
			},
		}),
	}, stream)

	if !hasCall(conn.calls, "CONFIG", "SET", "maxclients", "20000") {
		t.Errorf("the number should become '20000': %v", conn.calls)
	}
	if !hasCall(conn.calls, "CONFIG", "SET", "appendfsync", "everysec") {
		t.Errorf("the line as is: %v", conn.calls)
	}
}

// --- assert helpers ---

func assertEventsNoSecret(t *testing.T, stream *applyStream) {
	t.Helper()
	for i, ev := range stream.sent {
		if strings.Contains(ev.GetMessage(), secretPass) {
			t.Errorf("event[%d].Message contains password: %q", i, ev.GetMessage())
		}
		if out := ev.GetOutput(); out != nil {
			for k, v := range out.GetFields() {
				if strings.Contains(v.GetStringValue(), secretPass) {
					t.Errorf("event[%d].Output[%s] contains the password", i, k)
				}
			}
		}
	}
}

func assertNoCommandCarriesSecret(t *testing.T, conn *fakeConn) {
	t.Helper()
	for i, call := range conn.calls {
		for _, a := range call {
			if s, ok := a.(string); ok && strings.Contains(s, secretPass) {
				t.Errorf("command[%d] carries the password in its arguments: %v", i, call)
			}
		}
	}
}

func assertCalls(t *testing.T, conn *fakeConn, want [][]any) {
	t.Helper()
	if len(conn.calls) != len(want) {
		t.Fatalf("calls %d, waiting for %d: %v", len(conn.calls), len(want), conn.calls)
	}
	for i := range want {
		if len(conn.calls[i]) != len(want[i]) {
			t.Fatalf("call[%d] arity %d, expected %d: %v", i, len(conn.calls[i]), len(want[i]), conn.calls[i])
		}
		for j := range want[i] {
			if conn.calls[i][j] != want[i][j] {
				t.Errorf("call[%d][%d]=%v, expected %v", i, j, conn.calls[i][j], want[i][j])
			}
		}
	}
}
