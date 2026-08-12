//go:build live

// L1: the module against a real Redis.
//
// Everything the L0 suite asserts about normalisation is modelled by a fake that
// normalises the way I told it to. These tests are what checks that the model
// matches the server: that "100mb" really does come back as 104857600, that
// "KEA" really is reordered, that 6.2 really does reject a multi-parameter
// CONFIG SET, and that a server with no config file really does refuse to
// rewrite.
//
//	REDIS_ADDR        a modern server (7.x/8) started WITH a config file
//	REDIS_ADDR_62     a Redis 6.2 server
//	REDIS_ADDR_NOFILE a server started WITHOUT a config file
//
// All three are required. An integration test that skips itself when the
// environment is missing looks green and proves nothing.
package config

import (
	"context"
	"os"
	"strings"
	"testing"

	pluginv1 "github.com/souls-guild/soul-stack/proto/plugin/gen/go/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

func liveAddr(t *testing.T, env string) (host string, port int) {
	t.Helper()
	addr := os.Getenv(env)
	if addr == "" {
		t.Fatalf("%s is not set — this suite talks to a real server and refuses to pass without one", env)
	}
	h, p, ok := strings.Cut(addr, ":")
	if !ok {
		t.Fatalf("%s=%q, want host:port", env, addr)
	}
	n := 0
	for _, r := range p {
		if r < '0' || r > '9' {
			t.Fatalf("%s=%q, want host:port", env, addr)
		}
		n = n*10 + int(r-'0')
	}
	return h, n
}

func liveParams(t *testing.T, env string, extra map[string]any) *structpb.Struct {
	t.Helper()
	host, port := liveAddr(t, env)
	m := map[string]any{"host": host, "port": port}
	for k, v := range extra {
		m[k] = v
	}
	s, err := structpb.NewStruct(m)
	if err != nil {
		t.Fatalf("build params: %v", err)
	}
	return s
}

func liveApply(t *testing.T, env, state string, extra map[string]any) *pluginv1.ApplyEvent {
	t.Helper()
	m := &Config{}
	st := &applyStream{}
	if err := m.Apply(&pluginv1.ApplyRequest{State: state, Params: liveParams(t, env, extra)}, st); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	return st.last(t)
}

func livePlan(t *testing.T, env, state string, extra map[string]any) *pluginv1.PlanEvent {
	t.Helper()
	m := &Config{}
	st := &planStream{}
	if err := m.Plan(&pluginv1.PlanRequest{State: state, Params: liveParams(t, env, extra)}, st); err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if len(st.events) != 1 {
		t.Fatalf("Plan sent %d events, want exactly 1 (ADR-031)", len(st.events))
	}
	return st.events[0]
}

// liveClient opens a direct connection for setting up and reading back around
// the module.
func liveClient(t *testing.T, env string) configClient {
	t.Helper()
	host, port := liveAddr(t, env)
	c, err := dial(context.Background(), connParams{host: host, port: port, timeout: liveTimeout})
	if err != nil {
		t.Fatalf("dial %s: %v", env, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

const liveTimeout = 10_000_000_000 // 10s

func liveSet(t *testing.T, env, name, value string) {
	t.Helper()
	if err := liveClient(t, env).Set(context.Background(), []setting{{name: name, value: value}}); err != nil {
		t.Fatalf("setup CONFIG SET %s %s: %v", name, value, err)
	}
}

func liveGet(t *testing.T, env, name string) string {
	t.Helper()
	got, err := liveClient(t, env).Get(context.Background(), name)
	if err != nil {
		t.Fatalf("CONFIG GET %s: %v", name, err)
	}
	return got[name]
}

// ---------------------------------------------------------------------------

// The whole reason this module re-reads instead of trusting its own comparison.
// Redis stores 104857600 for "100mb", and a second run must see no change.
func TestLiveMemoryUnitConverges(t *testing.T) {
	liveSet(t, "REDIS_ADDR", "maxmemory", "0")
	t.Cleanup(func() { liveSet(t, "REDIS_ADDR", "maxmemory", "0") })

	first := liveApply(t, "REDIS_ADDR", "present", map[string]any{
		"settings": map[string]any{"maxmemory": "100mb"},
	})
	if first.GetFailed() {
		t.Fatalf("first run: %s", first.GetMessage())
	}
	if !first.GetChanged() {
		t.Error("first run reported no change")
	}
	if got := liveGet(t, "REDIS_ADDR", "maxmemory"); got != "104857600" {
		t.Fatalf("server stored %q, want 104857600 — the multiplier assumption is wrong", got)
	}

	second := liveApply(t, "REDIS_ADDR", "present", map[string]any{
		"settings": map[string]any{"maxmemory": "100mb"},
	})
	if second.GetChanged() {
		t.Error("second run reported a change; the module churns on its own normalisation")
	}
}

// ★ Redis reorders the flags of notify-keyspace-events: "KEA" is stored as
// "AKE". The module cannot predict that, writes anyway, and must still report no
// change because the stored value never moved.
func TestLiveUnpredictableNormalisationReportsNoChange(t *testing.T) {
	liveSet(t, "REDIS_ADDR", "notify-keyspace-events", "KEA")
	t.Cleanup(func() { liveSet(t, "REDIS_ADDR", "notify-keyspace-events", "") })

	stored := liveGet(t, "REDIS_ADDR", "notify-keyspace-events")
	if stored == "KEA" {
		t.Skip("this server does not reorder the flags, so there is nothing to prove here")
	}
	t.Logf("server rendered KEA as %q", stored)

	e := liveApply(t, "REDIS_ADDR", "present", map[string]any{
		"settings": map[string]any{"notify-keyspace-events": "KEA"},
	})
	if e.GetFailed() {
		t.Fatalf("apply: %s", e.GetMessage())
	}
	if e.GetChanged() {
		t.Error("changed=true although the stored value is unchanged")
	}
}

func TestLiveCaseNormalisationReportsNoChange(t *testing.T) {
	liveSet(t, "REDIS_ADDR", "appendfsync", "everysec")
	e := liveApply(t, "REDIS_ADDR", "present", map[string]any{
		"settings": map[string]any{"appendfsync": "EVERYSEC"},
	})
	if e.GetFailed() {
		t.Fatalf("apply: %s", e.GetMessage())
	}
	if e.GetChanged() {
		t.Error("changed=true for EVERYSEC against a stored everysec")
	}
}

// multiSetSupported asks the server rather than the version string: it writes
// two parameters their own current values, which changes nothing, and looks at
// whether the multi-argument form was refused for arity.
//
// Asking matters because the CI matrix runs a leg where the main server IS 6.2,
// and a test that assumed 7.0+ there would fail for a reason that has nothing to
// do with the module.
func multiSetSupported(t *testing.T, env string) bool {
	t.Helper()
	c := liveClient(t, env)
	cur := []setting{
		{name: "maxmemory", value: liveGet(t, env, "maxmemory")},
		{name: "maxmemory-policy", value: liveGet(t, env, "maxmemory-policy")},
	}
	err := c.Set(context.Background(), cur)
	if err == nil {
		return true
	}
	if isArityError(err) {
		return false
	}
	t.Fatalf("probing for multi-parameter CONFIG SET: %v", err)
	return false
}

// Redis 7.0+ applies a multi-parameter CONFIG SET all-or-nothing: an immutable
// parameter in the list must leave the mutable one untouched. Where the form
// does not exist, the module writes one at a time and the property under test is
// the other one — that it says so instead of implying atomicity it did not have.
func TestLiveMultiSetAtomicityMatchesTheServer(t *testing.T) {
	liveSet(t, "REDIS_ADDR", "maxmemory", "0")
	t.Cleanup(func() { liveSet(t, "REDIS_ADDR", "maxmemory", "0") })
	atomicServer := multiSetSupported(t, "REDIS_ADDR")

	e := liveApply(t, "REDIS_ADDR", "present", map[string]any{
		"settings": map[string]any{"maxmemory": "50mb", "databases": "32"},
	})
	if !e.GetFailed() {
		t.Fatal("setting an immutable parameter was reported as success")
	}

	if atomicServer {
		if got := liveGet(t, "REDIS_ADDR", "maxmemory"); got != "0" {
			t.Errorf("maxmemory moved to %q despite the command being rejected as a unit", got)
		}
		return
	}
	// Pre-7.0: the writes went out one at a time, so some may have landed. The
	// module must not hide that.
	if !strings.Contains(e.GetMessage(), "ARE applied") {
		t.Errorf("on a server without an atomic multi-set the failure must say earlier writes landed: %s", e.GetMessage())
	}
}

func TestLiveAtomicFlagReportsWhatTheServerDid(t *testing.T) {
	liveSet(t, "REDIS_ADDR", "maxmemory", "0")
	liveSet(t, "REDIS_ADDR", "maxmemory-policy", "noeviction")
	t.Cleanup(func() {
		liveSet(t, "REDIS_ADDR", "maxmemory", "0")
		liveSet(t, "REDIS_ADDR", "maxmemory-policy", "noeviction")
	})
	want := multiSetSupported(t, "REDIS_ADDR")

	e := liveApply(t, "REDIS_ADDR", "present", map[string]any{
		"settings": map[string]any{"maxmemory": "50mb", "maxmemory-policy": "allkeys-lru"},
	})
	if e.GetFailed() {
		t.Fatalf("apply: %s", e.GetMessage())
	}
	if got := e.GetOutput().GetFields()["atomic"].GetBoolValue(); got != want {
		t.Errorf("atomic=%v, but this server %s a multi-parameter CONFIG SET",
			got, map[bool]string{true: "accepts", false: "rejects"}[want])
	}
}

// Redis 6.2 has no multi-parameter CONFIG SET. The module must fall back and say
// the write was not atomic.
func TestLiveOldServerFallsBack(t *testing.T) {
	liveSet(t, "REDIS_ADDR_62", "maxmemory", "0")
	liveSet(t, "REDIS_ADDR_62", "maxmemory-policy", "noeviction")
	t.Cleanup(func() {
		liveSet(t, "REDIS_ADDR_62", "maxmemory", "0")
		liveSet(t, "REDIS_ADDR_62", "maxmemory-policy", "noeviction")
	})

	e := liveApply(t, "REDIS_ADDR_62", "present", map[string]any{
		"settings": map[string]any{"maxmemory": "50mb", "maxmemory-policy": "allkeys-lru"},
		// persist is off because the subject here is the write path, not
		// durability. The 6.2 server is not required to have been started from a
		// config file, and a CONFIG REWRITE failure would fail this test for a
		// reason it is not testing.
		"persist": false,
	})
	if e.GetFailed() {
		t.Fatalf("apply: %s", e.GetMessage())
	}
	if !e.GetChanged() {
		t.Error("changed=false after two real changes")
	}
	if e.GetOutput().GetFields()["atomic"].GetBoolValue() {
		t.Error("atomic=true on 6.2, which cannot do a multi-parameter CONFIG SET")
	}
	if got := liveGet(t, "REDIS_ADDR_62", "maxmemory"); got != "52428800" {
		t.Errorf("maxmemory=%q want 52428800", got)
	}
}

func TestLiveRequireAtomicRefusesOnOldServer(t *testing.T) {
	liveSet(t, "REDIS_ADDR_62", "maxmemory", "0")
	liveSet(t, "REDIS_ADDR_62", "maxmemory-policy", "noeviction")

	e := liveApply(t, "REDIS_ADDR_62", "present", map[string]any{
		"settings":       map[string]any{"maxmemory": "50mb", "maxmemory-policy": "allkeys-lru"},
		"require_atomic": true,
	})
	if !e.GetFailed() {
		t.Fatal("require_atomic did not refuse on a server without atomic multi-set")
	}
	if got := liveGet(t, "REDIS_ADDR_62", "maxmemory"); got != "0" {
		t.Errorf("maxmemory moved to %q although require_atomic refused before writing", got)
	}
}

// The parameter set differs a lot between versions. Asking 6.2 for a parameter
// only 7.0 has must fail loudly — silence here would report success forever.
func TestLiveUnsupportedParameterOnOldServer(t *testing.T) {
	e := liveApply(t, "REDIS_ADDR_62", "present", map[string]any{
		"settings": map[string]any{"latency-tracking": "yes"},
	})
	if !e.GetFailed() {
		t.Fatal("a parameter this server does not have was accepted")
	}
	if !strings.Contains(e.GetMessage(), "latency-tracking") {
		t.Errorf("message does not name it: %s", e.GetMessage())
	}
}

// A server started without a config file cannot persist. The values ARE applied
// — the step must say exactly that instead of reporting a durable success.
func TestLivePersistFailsWithoutConfigFile(t *testing.T) {
	liveSet(t, "REDIS_ADDR_NOFILE", "maxmemory", "0")
	t.Cleanup(func() { liveSet(t, "REDIS_ADDR_NOFILE", "maxmemory", "0") })

	e := liveApply(t, "REDIS_ADDR_NOFILE", "present", map[string]any{
		"settings": map[string]any{"maxmemory": "50mb"},
		"persist":  true,
	})
	if !e.GetFailed() {
		t.Fatal("CONFIG REWRITE succeeded on a server with no config file?")
	}
	if !strings.Contains(e.GetMessage(), "NOT durable") {
		t.Errorf("message does not say the change is not durable: %s", e.GetMessage())
	}
	if got := liveGet(t, "REDIS_ADDR_NOFILE", "maxmemory"); got != "52428800" {
		t.Errorf("maxmemory=%q — the message claims the value IS applied in memory", got)
	}
}

func TestLivePersistSucceedsWithConfigFile(t *testing.T) {
	liveSet(t, "REDIS_ADDR", "maxmemory", "0")
	t.Cleanup(func() { liveSet(t, "REDIS_ADDR", "maxmemory", "0") })

	e := liveApply(t, "REDIS_ADDR", "present", map[string]any{
		"settings": map[string]any{"maxmemory": "50mb"},
		"persist":  true,
	})
	if e.GetFailed() {
		t.Fatalf("apply: %s", e.GetMessage())
	}
	if !e.GetOutput().GetFields()["persisted"].GetBoolValue() {
		t.Error("persisted=false although the server has a config file")
	}
}

// ---------------------------------------------------------------------------

func TestLivePlanDoesNotTouchTheServer(t *testing.T) {
	liveSet(t, "REDIS_ADDR", "maxmemory", "0")
	t.Cleanup(func() { liveSet(t, "REDIS_ADDR", "maxmemory", "0") })

	e := livePlan(t, "REDIS_ADDR", "present", map[string]any{
		"settings": map[string]any{"maxmemory": "50mb"},
	})
	if !e.GetChanged() {
		t.Error("Plan reported clean against a value that differs")
	}
	if got := liveGet(t, "REDIS_ADDR", "maxmemory"); got != "0" {
		t.Errorf("Plan changed maxmemory to %q; it must be a pure read", got)
	}

	clean := livePlan(t, "REDIS_ADDR", "present", map[string]any{
		"settings": map[string]any{"maxmemory": "0"},
	})
	if clean.GetChanged() {
		t.Error("Plan reported drift against the value already stored")
	}
}

func TestLiveReadRedactsSecret(t *testing.T) {
	e := liveApply(t, "REDIS_ADDR", "read", map[string]any{"patterns": []any{"requirepass", "maxmemory"}})
	if e.GetFailed() {
		t.Fatalf("read: %s", e.GetMessage())
	}
	cfg := e.GetOutput().GetFields()["config"].GetStructValue().GetFields()
	if got := cfg["requirepass"].GetStringValue(); got != redacted {
		t.Errorf("requirepass=%q, want it withheld — CONFIG GET hands it out in the clear", got)
	}
	if _, ok := cfg["maxmemory"]; !ok {
		t.Error("maxmemory missing from the read")
	}
}

// The parameter set grows with the version, which is why an unsupported
// parameter has to fail rather than pass silently.
//
// The assertion is `not more` rather than `strictly fewer` on purpose: in CI the
// matrix leg that runs 6.2 as the main server compares 6.2 against 6.2, and a
// strict comparison would fail there for a reason that has nothing to do with
// the module. What must never happen is the older server reporting MORE.
func TestLiveReadWildcardCoversTheWholeParameterSet(t *testing.T) {
	modern := liveApply(t, "REDIS_ADDR", "read", map[string]any{})
	old := liveApply(t, "REDIS_ADDR_62", "read", map[string]any{})
	for _, e := range []*pluginv1.ApplyEvent{modern, old} {
		if e.GetFailed() {
			t.Fatalf("read: %s", e.GetMessage())
		}
		if e.GetChanged() {
			t.Error("a read reported a change")
		}
	}
	nModern := int(modern.GetOutput().GetFields()["count"].GetNumberValue())
	nOld := int(old.GetOutput().GetFields()["count"].GetNumberValue())
	// A wildcard read that comes back nearly empty means the glob never reached
	// the server, not that the server is small.
	if nOld < 100 || nModern < 100 {
		t.Errorf("a `*` read returned 6.2=%d modern=%d; both should be well over a hundred", nOld, nModern)
	}
	if nOld > nModern {
		t.Errorf("6.2 reported %d parameters and the modern server %d; the older set cannot be the larger one", nOld, nModern)
	}
	t.Logf("parameters: 6.2=%d modern=%d", nOld, nModern)
}

func TestLiveConnectivityGateHoldsOnARealServer(t *testing.T) {
	// The port the server listens on is asked of the server, not derived from
	// the address used to reach it: behind a port mapping the two differ, and
	// the question here is whether the listener moved.
	before := liveGet(t, "REDIS_ADDR", "port")

	e := liveApply(t, "REDIS_ADDR", "present", map[string]any{
		"settings": map[string]any{"port": "6399"},
	})
	if !e.GetFailed() {
		t.Fatal("the port change was accepted without the gate")
	}
	if got := liveGet(t, "REDIS_ADDR", "port"); got != before {
		t.Fatalf("the listener moved from %q to %q — the gate did not hold", before, got)
	}
	if !strings.Contains(e.GetMessage(), "allow_connectivity_change") {
		t.Errorf("the refusal does not name the way out: %s", e.GetMessage())
	}
}
