package config

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"

	pluginv1 "github.com/souls-guild/soul-stack/proto/plugin/gen/go/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

// ---------------------------------------------------------------------------
// The fake server.
//
// It deliberately does NOT store what it is given. A fake that echoes its input
// back makes every idempotence test pass by construction and guards nothing —
// the whole difficulty with CONFIG is that Redis stores a normalised form and
// hands THAT back. So this fake normalises too, in its own way: memory literals
// resolve to bytes, notify-keyspace-events comes back with its flags reordered,
// and a few parameters are lower-cased. The reordering is not Redis' exact
// ordering on purpose (Redis answers "AKE" to "KEA", this answers "AEK"); the
// module must not depend on the particular rendering, only on the fact that
// there is one.
// ---------------------------------------------------------------------------

type fakeClient struct {
	values map[string]string

	setCalls [][]setting
	rewrites int
	closed   bool

	// multiUnsupported emulates Redis 6.2, which rejects a multi-parameter
	// CONFIG SET with an arity error, having applied nothing.
	multiUnsupported bool
	// setErr, when set, decides the outcome of each Set call.
	setErr func(pairs []setting) error
	// rewriteErr, when set, is what Rewrite returns.
	rewriteErr error
	// getErr, when set, is what Get returns for that pattern.
	getErr map[string]error
}

func newFake(values map[string]string) *fakeClient {
	v := make(map[string]string, len(values))
	for k, val := range values {
		v[k] = val
	}
	return &fakeClient{values: v}
}

func (f *fakeClient) Get(_ context.Context, pattern string) (map[string]string, error) {
	if err, ok := f.getErr[pattern]; ok {
		return nil, err
	}
	out := map[string]string{}
	for k, v := range f.values {
		if globMatch(pattern, k) {
			out[k] = v
		}
	}
	return out, nil
}

func (f *fakeClient) Set(_ context.Context, pairs []setting) error {
	if f.multiUnsupported && len(pairs) > 1 {
		return errors.New("ERR Unknown subcommand or wrong number of arguments for 'SET'. Try CONFIG HELP.")
	}
	if f.setErr != nil {
		if err := f.setErr(pairs); err != nil {
			return err
		}
	}
	f.setCalls = append(f.setCalls, append([]setting(nil), pairs...))
	for _, p := range pairs {
		f.values[p.name] = fakeNormalize(p.name, p.value)
	}
	return nil
}

func (f *fakeClient) Rewrite(context.Context) error {
	if f.rewriteErr != nil {
		return f.rewriteErr
	}
	f.rewrites++
	return nil
}

func (f *fakeClient) Close() error { f.closed = true; return nil }

// fakeNormalize is written independently of the module's own canonical(): if
// both shared an implementation, a bug in it would cancel itself out and the
// test would stay green.
func fakeNormalize(name, value string) string {
	switch name {
	case "notify-keyspace-events":
		b := []byte(value)
		sort.Slice(b, func(i, j int) bool { return b[i] < b[j] })
		return string(b)
	case "appendfsync", "maxmemory-policy":
		return strings.ToLower(value)
	}
	if n, ok := fakeBytes(value); ok {
		return n
	}
	return value
}

func fakeBytes(v string) (string, bool) {
	mult := map[string]int64{"kb": 1 << 10, "mb": 1 << 20, "gb": 1 << 30, "k": 1e3, "m": 1e6, "g": 1e9}
	low := strings.ToLower(v)
	for _, suffix := range []string{"kb", "mb", "gb", "k", "m", "g"} {
		if !strings.HasSuffix(low, suffix) {
			continue
		}
		n, err := strconv.ParseInt(strings.TrimSuffix(low, suffix), 10, 64)
		if err != nil {
			return "", false
		}
		return strconv.FormatInt(n*mult[suffix], 10), true
	}
	return "", false
}

// globMatch handles the only two shapes the tests use: an exact name and a
// trailing star.
func globMatch(pattern, name string) bool {
	if pattern == "*" {
		return true
	}
	if strings.HasSuffix(pattern, "*") {
		return strings.HasPrefix(name, strings.TrimSuffix(pattern, "*"))
	}
	return pattern == name
}

// ---------------------------------------------------------------------------
// Stream doubles.
// ---------------------------------------------------------------------------

type applyStream struct {
	grpc.ServerStream
	events []*pluginv1.ApplyEvent
}

func (s *applyStream) Send(e *pluginv1.ApplyEvent) error {
	s.events = append(s.events, e)
	return nil
}
func (s *applyStream) Context() context.Context { return context.Background() }

func (s *applyStream) last(t *testing.T) *pluginv1.ApplyEvent {
	t.Helper()
	if len(s.events) == 0 {
		t.Fatal("no ApplyEvent was sent")
	}
	return s.events[len(s.events)-1]
}

type planStream struct {
	grpc.ServerStream
	events []*pluginv1.PlanEvent
}

func (s *planStream) Send(e *pluginv1.PlanEvent) error {
	s.events = append(s.events, e)
	return nil
}
func (s *planStream) Context() context.Context { return context.Background() }

func (s *planStream) last(t *testing.T) *pluginv1.PlanEvent {
	t.Helper()
	if len(s.events) == 0 {
		t.Fatal("no PlanEvent was sent")
	}
	return s.events[len(s.events)-1]
}

// ---------------------------------------------------------------------------
// Harness.
// ---------------------------------------------------------------------------

func params(t *testing.T, extra map[string]any) *structpb.Struct {
	t.Helper()
	m := map[string]any{"host": "127.0.0.1"}
	for k, v := range extra {
		m[k] = v
	}
	s, err := structpb.NewStruct(m)
	if err != nil {
		t.Fatalf("build params: %v", err)
	}
	return s
}

func runApply(t *testing.T, f *fakeClient, state string, extra map[string]any) *applyStream {
	t.Helper()
	m := &Config{connect: func(context.Context, connParams) (configClient, error) { return f, nil }}
	st := &applyStream{}
	if err := m.Apply(&pluginv1.ApplyRequest{State: state, Params: params(t, extra)}, st); err != nil {
		t.Fatalf("Apply returned a transport error: %v", err)
	}
	return st
}

func runPlan(t *testing.T, f *fakeClient, state string, extra map[string]any) *planStream {
	t.Helper()
	m := &Config{connect: func(context.Context, connParams) (configClient, error) { return f, nil }}
	st := &planStream{}
	if err := m.Plan(&pluginv1.PlanRequest{State: state, Params: params(t, extra)}, st); err != nil {
		t.Fatalf("Plan returned a transport error: %v", err)
	}
	return st
}

func outString(t *testing.T, e *pluginv1.ApplyEvent, key string) string {
	t.Helper()
	return e.GetOutput().GetFields()[key].GetStringValue()
}

func outBool(t *testing.T, e *pluginv1.ApplyEvent, key string) bool {
	t.Helper()
	return e.GetOutput().GetFields()[key].GetBoolValue()
}

// ---------------------------------------------------------------------------
// present
// ---------------------------------------------------------------------------

func TestPresentNoChangeWritesNothing(t *testing.T) {
	f := newFake(map[string]string{"maxmemory-policy": "allkeys-lru"})
	st := runApply(t, f, "present", map[string]any{
		"settings": map[string]any{"maxmemory-policy": "allkeys-lru"},
	})
	e := st.last(t)
	if e.GetFailed() {
		t.Fatalf("unexpected failure: %s", e.GetMessage())
	}
	if e.GetChanged() {
		t.Error("changed=true for a value that already matched")
	}
	if len(f.setCalls) != 0 {
		t.Errorf("CONFIG SET was issued %d time(s) with nothing to change", len(f.setCalls))
	}
	if f.rewrites != 0 {
		t.Errorf("CONFIG REWRITE ran %d time(s) on a no-op", f.rewrites)
	}
	if got := outString(t, e, "action"); got != "noop" {
		t.Errorf("action=%q want noop", got)
	}
}

func TestPresentAppliesChange(t *testing.T) {
	f := newFake(map[string]string{"maxmemory-policy": "noeviction"})
	st := runApply(t, f, "present", map[string]any{
		"settings": map[string]any{"maxmemory-policy": "allkeys-lru"},
	})
	e := st.last(t)
	if e.GetFailed() {
		t.Fatalf("unexpected failure: %s", e.GetMessage())
	}
	if !e.GetChanged() {
		t.Error("changed=false after a real change")
	}
	if got := f.values["maxmemory-policy"]; got != "allkeys-lru" {
		t.Errorf("server value %q", got)
	}
	if got := outString(t, e, "changes"); !strings.Contains(got, "noeviction -> allkeys-lru") {
		t.Errorf("changes=%q, want the before and after in it", got)
	}
}

// The size the caller writes is not the size the server stores. Asking for
// "100mb" when 104857600 is already stored must not write and must not report a
// change — this is the case that makes a text-comparing module churn forever.
func TestPresentMemoryUnitIsNotAChange(t *testing.T) {
	f := newFake(map[string]string{"maxmemory": "104857600"})
	st := runApply(t, f, "present", map[string]any{
		"settings": map[string]any{"maxmemory": "100mb"},
	})
	e := st.last(t)
	if e.GetChanged() {
		t.Error("changed=true for 100mb against a stored 104857600")
	}
	if len(f.setCalls) != 0 {
		t.Errorf("CONFIG SET was issued for a value that only differed in spelling")
	}
}

// ★ The canonicalisation cannot know that the server reorders these flags, so it
// says "different" and a write goes out. The verdict must still be "no change",
// because it is taken from what the server reported before and after — not from
// what the module predicted.
func TestPresentUnpredictableNormalisationStillReportsNoChange(t *testing.T) {
	f := newFake(map[string]string{"notify-keyspace-events": fakeNormalize("notify-keyspace-events", "KEA")})
	st := runApply(t, f, "present", map[string]any{
		"settings": map[string]any{"notify-keyspace-events": "KEA"},
		"persist":  true,
	})
	e := st.last(t)
	if e.GetFailed() {
		t.Fatalf("unexpected failure: %s", e.GetMessage())
	}
	if len(f.setCalls) != 1 {
		t.Fatalf("expected exactly one write attempt, got %d", len(f.setCalls))
	}
	if e.GetChanged() {
		t.Error("changed=true although the stored value never moved")
	}
	if f.rewrites != 0 {
		t.Error("CONFIG REWRITE ran even though nothing moved")
	}
	if got := outString(t, e, "action"); got != "noop" {
		t.Errorf("action=%q want noop", got)
	}
}

func TestPresentRejectsUnsupportedParameter(t *testing.T) {
	f := newFake(map[string]string{"maxmemory": "0"})
	st := runApply(t, f, "present", map[string]any{
		"settings": map[string]any{"latency-tracking": "yes"},
	})
	e := st.last(t)
	if !e.GetFailed() {
		t.Fatal("an unsupported parameter was accepted; it would report success forever")
	}
	if !strings.Contains(e.GetMessage(), "latency-tracking") {
		t.Errorf("message does not name the parameter: %s", e.GetMessage())
	}
	if len(f.setCalls) != 0 {
		t.Error("a write went out despite the parameter not existing")
	}
}

func TestPresentMultiParameterIsOneAtomicWrite(t *testing.T) {
	f := newFake(map[string]string{"maxmemory": "0", "maxmemory-policy": "noeviction"})
	st := runApply(t, f, "present", map[string]any{
		"settings": map[string]any{"maxmemory": "100mb", "maxmemory-policy": "allkeys-lru"},
	})
	e := st.last(t)
	if e.GetFailed() {
		t.Fatalf("unexpected failure: %s", e.GetMessage())
	}
	if len(f.setCalls) != 1 || len(f.setCalls[0]) != 2 {
		t.Fatalf("expected one CONFIG SET with two pairs, got %v", f.setCalls)
	}
	if !outBool(t, e, "atomic") {
		t.Error("atomic=false although the multi-parameter form was accepted")
	}
}

// Redis 6.2 has no multi-parameter CONFIG SET. It rejects the command having
// applied nothing, so writing one at a time is safe — but it is not atomic and
// the output has to say so.
func TestPresentFallsBackToSingleWritesOnOldServer(t *testing.T) {
	f := newFake(map[string]string{"maxmemory": "0", "maxmemory-policy": "noeviction"})
	f.multiUnsupported = true
	st := runApply(t, f, "present", map[string]any{
		"settings": map[string]any{"maxmemory": "100mb", "maxmemory-policy": "allkeys-lru"},
	})
	e := st.last(t)
	if e.GetFailed() {
		t.Fatalf("unexpected failure: %s", e.GetMessage())
	}
	if len(f.setCalls) != 2 {
		t.Fatalf("expected two single-pair writes, got %v", f.setCalls)
	}
	if outBool(t, e, "atomic") {
		t.Error("atomic=true although the write was split")
	}
	// Sorted order, so a partial failure is reproducible.
	if f.setCalls[0][0].name != "maxmemory" || f.setCalls[1][0].name != "maxmemory-policy" {
		t.Errorf("writes were not issued in a fixed order: %v", f.setCalls)
	}
}

func TestPresentRequireAtomicRefusesFallback(t *testing.T) {
	f := newFake(map[string]string{"maxmemory": "0", "maxmemory-policy": "noeviction"})
	f.multiUnsupported = true
	st := runApply(t, f, "present", map[string]any{
		"settings":       map[string]any{"maxmemory": "100mb", "maxmemory-policy": "allkeys-lru"},
		"require_atomic": true,
	})
	e := st.last(t)
	if !e.GetFailed() {
		t.Fatal("require_atomic did not refuse a non-atomic write")
	}
	if len(f.setCalls) != 0 {
		t.Errorf("something was written despite require_atomic: %v", f.setCalls)
	}
}

// A rejection that is not about arity must NOT trigger the fallback: on 7.0+ the
// whole command was rolled back, and retrying one by one would apply the half
// the server refused as a unit.
func TestPresentRealRejectionIsNotRetriedOneByOne(t *testing.T) {
	f := newFake(map[string]string{"maxmemory": "0", "maxmemory-policy": "noeviction"})
	f.setErr = func(pairs []setting) error {
		if len(pairs) > 1 {
			return errors.New("ERR CONFIG SET failed (possibly related to argument 'maxmemory-policy') - argument(s) must be one of the following: ...")
		}
		return nil
	}
	st := runApply(t, f, "present", map[string]any{
		"settings": map[string]any{"maxmemory": "100mb", "maxmemory-policy": "bogus"},
	})
	e := st.last(t)
	if !e.GetFailed() {
		t.Fatal("a rejected write was reported as success")
	}
	if len(f.setCalls) != 0 {
		t.Errorf("the module retried a non-arity rejection: %v", f.setCalls)
	}
	if f.values["maxmemory"] != "0" {
		t.Errorf("maxmemory moved to %q despite the write being rejected", f.values["maxmemory"])
	}
}

func TestPresentFallbackFailureSaysEarlierWritesLanded(t *testing.T) {
	f := newFake(map[string]string{"maxmemory": "0", "maxmemory-policy": "noeviction"})
	f.multiUnsupported = true
	f.setErr = func(pairs []setting) error {
		if pairs[0].name == "maxmemory-policy" {
			return errors.New("ERR CONFIG SET failed - argument(s) must be one of the following: ...")
		}
		return nil
	}
	st := runApply(t, f, "present", map[string]any{
		"settings": map[string]any{"maxmemory": "100mb", "maxmemory-policy": "bogus"},
	})
	e := st.last(t)
	if !e.GetFailed() {
		t.Fatal("a failed write was reported as success")
	}
	if !strings.Contains(e.GetMessage(), "ARE applied") {
		t.Errorf("the message hides that earlier parameters landed: %s", e.GetMessage())
	}
}

func TestPresentPersistRunsRewrite(t *testing.T) {
	f := newFake(map[string]string{"maxmemory": "0"})
	st := runApply(t, f, "present", map[string]any{
		"settings": map[string]any{"maxmemory": "100mb"},
	})
	e := st.last(t)
	if f.rewrites != 1 {
		t.Errorf("CONFIG REWRITE ran %d time(s); persist defaults to true", f.rewrites)
	}
	if !outBool(t, e, "persisted") {
		t.Error("persisted=false although the rewrite succeeded")
	}
}

func TestPresentPersistFalseSkipsRewrite(t *testing.T) {
	f := newFake(map[string]string{"maxmemory": "0"})
	st := runApply(t, f, "present", map[string]any{
		"settings": map[string]any{"maxmemory": "100mb"},
		"persist":  false,
	})
	e := st.last(t)
	if f.rewrites != 0 {
		t.Errorf("CONFIG REWRITE ran %d time(s) with persist:false", f.rewrites)
	}
	if outBool(t, e, "persisted") {
		t.Error("persisted=true with persist:false")
	}
}

// A server with no config file cannot persist. The step must fail rather than
// report a durable change that is not durable.
func TestPresentRewriteFailureFailsTheStep(t *testing.T) {
	f := newFake(map[string]string{"maxmemory": "0"})
	f.rewriteErr = errors.New("ERR The server is running without a config file")
	st := runApply(t, f, "present", map[string]any{
		"settings": map[string]any{"maxmemory": "100mb"},
	})
	e := st.last(t)
	if !e.GetFailed() {
		t.Fatal("a failed CONFIG REWRITE was reported as success")
	}
	if !strings.Contains(e.GetMessage(), "NOT durable") {
		t.Errorf("the message does not say the change is not durable: %s", e.GetMessage())
	}
}

func TestPresentSecretValueIsNotEchoed(t *testing.T) {
	f := newFake(map[string]string{"requirepass": "old-secret"})
	st := runApply(t, f, "present", map[string]any{
		"settings":                  map[string]any{"requirepass": "new-secret"},
		"allow_connectivity_change": true,
	})
	e := st.last(t)
	if e.GetFailed() {
		t.Fatalf("unexpected failure: %s", e.GetMessage())
	}
	changes := outString(t, e, "changes")
	for _, leak := range []string{"old-secret", "new-secret"} {
		if strings.Contains(changes, leak) {
			t.Errorf("changes leaked the password: %s", changes)
		}
	}
	if !strings.Contains(changes, redacted) {
		t.Errorf("changes=%q, want the redaction marker", changes)
	}
	if !e.GetChanged() {
		t.Error("changed=false although the password moved")
	}
}

// ---------------------------------------------------------------------------
// gates
// ---------------------------------------------------------------------------

func TestConnectivityGateRefusesBeforeConnecting(t *testing.T) {
	for _, name := range []string{"port", "bind", "protected-mode", "requirepass", "maxclients"} {
		t.Run(name, func(t *testing.T) {
			opened := false
			m := &Config{connect: func(context.Context, connParams) (configClient, error) {
				opened = true
				return newFake(nil), nil
			}}
			st := &applyStream{}
			if err := m.Apply(&pluginv1.ApplyRequest{
				State:  "present",
				Params: params(t, map[string]any{"settings": map[string]any{name: "1"}}),
			}, st); err != nil {
				t.Fatalf("Apply: %v", err)
			}
			e := st.last(t)
			if !e.GetFailed() {
				t.Fatalf("%s was accepted without the gate", name)
			}
			if opened {
				t.Error("a connection was opened before the gate refused")
			}
		})
	}
}

func TestConnectivityGateOpens(t *testing.T) {
	f := newFake(map[string]string{"maxclients": "10000"})
	st := runApply(t, f, "present", map[string]any{
		"settings":                  map[string]any{"maxclients": "20000"},
		"allow_connectivity_change": true,
	})
	if e := st.last(t); e.GetFailed() {
		t.Fatalf("the gate refused with allow_connectivity_change: %s", e.GetMessage())
	}
}

// ---------------------------------------------------------------------------
// read
// ---------------------------------------------------------------------------

func TestReadRedactsSecretsByDefault(t *testing.T) {
	f := newFake(map[string]string{"requirepass": "hunter2", "maxmemory": "0"})
	st := runApply(t, f, "read", map[string]any{})
	e := st.last(t)
	if e.GetChanged() {
		t.Error("read reported a change")
	}
	cfg := e.GetOutput().GetFields()["config"].GetStructValue().GetFields()
	if got := cfg["requirepass"].GetStringValue(); got != redacted {
		t.Errorf("requirepass=%q, want it withheld", got)
	}
	if got := cfg["maxmemory"].GetStringValue(); got != "0" {
		t.Errorf("maxmemory=%q", got)
	}
	names := e.GetOutput().GetFields()["redacted"].GetListValue().GetValues()
	if len(names) != 1 || names[0].GetStringValue() != "requirepass" {
		t.Errorf("redacted=%v", names)
	}
}

func TestReadRevealSecrets(t *testing.T) {
	f := newFake(map[string]string{"requirepass": "hunter2"})
	st := runApply(t, f, "read", map[string]any{"reveal_secrets": true})
	cfg := st.last(t).GetOutput().GetFields()["config"].GetStructValue().GetFields()
	if got := cfg["requirepass"].GetStringValue(); got != "hunter2" {
		t.Errorf("requirepass=%q, want the value with reveal_secrets", got)
	}
}

func TestReadMergesPatterns(t *testing.T) {
	f := newFake(map[string]string{"maxmemory": "0", "maxmemory-policy": "noeviction", "appendonly": "no"})
	st := runApply(t, f, "read", map[string]any{
		"patterns": []any{"maxmemory*", "appendonly"},
	})
	e := st.last(t)
	cfg := e.GetOutput().GetFields()["config"].GetStructValue().GetFields()
	if len(cfg) != 3 {
		t.Errorf("got %d parameters, want 3: %v", len(cfg), cfg)
	}
	if n := int(e.GetOutput().GetFields()["count"].GetNumberValue()); n != 3 {
		t.Errorf("count=%d want 3", n)
	}
}

func TestReadPatternErrorFails(t *testing.T) {
	f := newFake(map[string]string{"maxmemory": "0"})
	f.getErr = map[string]error{"*": errors.New("NOPERM this user has no permissions to run the 'config|get' command")}
	st := runApply(t, f, "read", map[string]any{})
	if e := st.last(t); !e.GetFailed() {
		t.Fatal("a failed CONFIG GET was reported as success")
	}
}

// ---------------------------------------------------------------------------
// Plan (ADR-031)
// ---------------------------------------------------------------------------

func TestPlanReportsDriftWithoutWriting(t *testing.T) {
	f := newFake(map[string]string{"maxmemory-policy": "noeviction"})
	st := runPlan(t, f, "present", map[string]any{
		"settings": map[string]any{"maxmemory-policy": "allkeys-lru"},
	})
	e := st.last(t)
	if !e.GetChanged() {
		t.Error("changed=false although the value differs")
	}
	if len(f.setCalls) != 0 || f.rewrites != 0 {
		t.Error("Plan wrote to the server; it must be a pure read")
	}
}

func TestPlanCleanWhenAlreadyCorrect(t *testing.T) {
	f := newFake(map[string]string{"maxmemory": "104857600"})
	st := runPlan(t, f, "present", map[string]any{
		"settings": map[string]any{"maxmemory": "100mb"},
	})
	if st.last(t).GetChanged() {
		t.Error("changed=true for a value that only differed in spelling")
	}
}

func TestPlanReadIsAlwaysClean(t *testing.T) {
	m := &Config{connect: func(context.Context, connParams) (configClient, error) {
		t.Fatal("Plan opened a connection for the read state")
		return nil, nil
	}}
	st := &planStream{}
	if err := m.Plan(&pluginv1.PlanRequest{State: "read", Params: params(t, nil)}, st); err != nil {
		t.Fatalf("Plan: %v", err)
	}
	if st.last(t).GetChanged() {
		t.Error("read reported drift")
	}
}

func TestPlanSendsExactlyOneEvent(t *testing.T) {
	f := newFake(map[string]string{"maxmemory-policy": "noeviction"})
	st := runPlan(t, f, "present", map[string]any{
		"settings": map[string]any{"maxmemory-policy": "allkeys-lru"},
	})
	// ADR-031 invariant: a read-safe Plan sends EXACTLY ONE final event.
	if len(st.events) != 1 {
		t.Errorf("Plan sent %d events, want exactly 1", len(st.events))
	}
}

func TestPlanReadSafeIsDeclared(t *testing.T) {
	var m any = &Config{}
	if _, ok := m.(interface{ PlanReadSafe() }); !ok {
		t.Fatal("the module does not declare PlanReadSafe, so the host will refuse dry-run")
	}
}

// ---------------------------------------------------------------------------
// parameter parsing
// ---------------------------------------------------------------------------

func TestParameterNameIsValidated(t *testing.T) {
	for _, name := range []string{"MAXMEMORY", "max memory", "-maxmemory", "9lives", "maxmemory;FLUSHALL", ""} {
		t.Run(fmt.Sprintf("%q", name), func(t *testing.T) {
			f := newFake(nil)
			st := runApply(t, f, "present", map[string]any{"settings": map[string]any{name: "1"}})
			if e := st.last(t); !e.GetFailed() {
				t.Errorf("%q was accepted as a parameter name", name)
			}
		})
	}
}

func TestSettingsMustNameSomething(t *testing.T) {
	f := newFake(nil)
	for _, extra := range []map[string]any{
		{},
		{"settings": map[string]any{}},
		{"settings": "maxmemory"},
	} {
		st := runApply(t, f, "present", extra)
		if e := st.last(t); !e.GetFailed() {
			t.Errorf("%v was accepted", extra)
		}
	}
}

func TestScalarValuesAreRendered(t *testing.T) {
	f := newFake(map[string]string{"maxmemory": "5", "appendonly": "no", "maxclients": "1"})
	st := runApply(t, f, "present", map[string]any{
		"settings":                  map[string]any{"maxmemory": 0, "appendonly": true, "maxclients": 20000},
		"allow_connectivity_change": true,
	})
	if e := st.last(t); e.GetFailed() {
		t.Fatalf("unexpected failure: %s", e.GetMessage())
	}
	if f.values["maxmemory"] != "0" {
		t.Errorf("a numeric 0 arrived as %q", f.values["maxmemory"])
	}
	if f.values["appendonly"] != "yes" {
		t.Errorf("a boolean true arrived as %q, want yes", f.values["appendonly"])
	}
	if f.values["maxclients"] != "20000" {
		t.Errorf("a large number arrived as %q (scientific notation?)", f.values["maxclients"])
	}
}

func TestLoginUsernameWithoutPasswordRefused(t *testing.T) {
	f := newFake(nil)
	st := runApply(t, f, "present", map[string]any{
		"login_username": "admin",
		"settings":       map[string]any{"maxmemory": "0"},
	})
	if e := st.last(t); !e.GetFailed() {
		t.Fatal("a username with no password was accepted")
	}
}

func TestUnknownStateRefused(t *testing.T) {
	f := newFake(nil)
	st := runApply(t, f, "absent", map[string]any{"settings": map[string]any{"maxmemory": "0"}})
	if e := st.last(t); !e.GetFailed() {
		t.Fatal("an unknown state was accepted")
	}
}

// ---------------------------------------------------------------------------
// canonicalisation
// ---------------------------------------------------------------------------

// The multipliers are Redis' own and are not what anyone guesses: 1k is 1000
// while 1kb is 1024. Read off a live server, not from the documentation.
func TestParseMemoryMultipliers(t *testing.T) {
	cases := map[string]string{
		"1k": "1000", "1kb": "1024",
		"1m": "1000000", "1mb": "1048576",
		"1g": "1000000000", "1gb": "1073741824",
		"100mb": "104857600", "100MB": "104857600",
		"0": "", "100": "", "allkeys-lru": "", "": "",
		"everysec": "", "900 1 300 10": "", "-1": "", "1x": "",
	}
	for in, want := range cases {
		got, ok := parseMemory(in)
		if want == "" {
			if ok {
				t.Errorf("parseMemory(%q) = %q, want it left alone", in, got)
			}
			continue
		}
		if !ok || got != want {
			t.Errorf("parseMemory(%q) = %q,%v want %q,true", in, got, ok, want)
		}
	}
}

// Case is NOT folded: `notify-keyspace-events` is a flag set where `K` and `k`
// mean different things, so folding case to save a redundant write on
// `appendfsync` would silence a real difference here.
func TestCanonicalKeepsCase(t *testing.T) {
	if canonical("KEA") == canonical("kea") {
		t.Error("canonical folded case; that hides a real difference in notify-keyspace-events")
	}
	if canonical(" 900   1 ") != "900 1" {
		t.Errorf("canonical did not collapse whitespace: %q", canonical(" 900   1 "))
	}
}

func TestIsArityError(t *testing.T) {
	arity := []string{
		"ERR Unknown subcommand or wrong number of arguments for 'SET'. Try CONFIG HELP.",
		"ERR wrong number of arguments for 'config|set' command",
	}
	notArity := []string{
		"ERR CONFIG SET failed (possibly related to argument 'databases') - can't set immutable config",
		"ERR CONFIG SET failed (possibly related to argument 'maxmemory') - argument must be a memory value",
		"NOPERM this user has no permissions to run the 'config|set' command",
	}
	for _, s := range arity {
		if !isArityError(errors.New(s)) {
			t.Errorf("not recognised as arity: %s", s)
		}
	}
	for _, s := range notArity {
		if isArityError(errors.New(s)) {
			t.Errorf("wrongly recognised as arity: %s", s)
		}
	}
}
