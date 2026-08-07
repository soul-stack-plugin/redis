//go:build live

// L1 tests against a real Redis. They are the ones that matter: the rule diff
// is built on Redis' own rendering, and a fake cannot prove that the rendering
// behaves as assumed.
//
// Two servers are needed, because persistence is on by default and its two
// outcomes are both worth pinning:
//
//	REDIS_ADDR           — a server WITH an `aclfile`, where ACL SAVE works
//	REDIS_ADDR_NO_ACLFILE — a server WITHOUT one, where ACL SAVE must fail loudly
//
// Both are required rather than defaulted — a skipped integration test that
// looks green is worse than no test. See the README for the two docker lines.
package acl

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"

	pluginv1 "github.com/souls-guild/soul-stack/proto/plugin/gen/go/v1"
)

func liveParams(t *testing.T, extra map[string]any) map[string]any {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Fatal("REDIS_ADDR is not set: this test talks to a real Redis and will not silently skip")
	}
	host, portStr, ok := strings.Cut(addr, ":")
	if !ok {
		t.Fatalf("REDIS_ADDR=%q: want host:port", addr)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("REDIS_ADDR=%q: %v", addr, err)
	}
	m := map[string]any{"host": host, "port": float64(port), "username": "soulstack_live_test"}
	for k, v := range extra {
		m[k] = v
	}
	return m
}

// runLive drives the real module — real client, real server.
func runLive(t *testing.T, state string, p map[string]any) *pluginv1.ApplyEvent {
	t.Helper()
	m := &ACL{}
	st := &fakeApplyStream{ctx: context.Background()}
	if err := m.Apply(&pluginv1.ApplyRequest{State: state, Params: params(t, p)}, st); err != nil {
		t.Fatalf("Apply returned a transport error: %v", err)
	}
	ev := st.last()
	if ev == nil {
		t.Fatal("Apply sent no event")
	}
	return ev
}

func liveCleanup(t *testing.T) {
	t.Helper()
	ev := runLive(t, "absent", liveParams(t, nil))
	if ev.GetFailed() {
		t.Fatalf("cleanup: %s", ev.GetMessage())
	}
}

// TestLiveConvergence is the claim the whole module rests on: applying the same
// desired state twice writes once and then reports no change, against a server
// whose rendering of the rules does not match what was written.
func TestLiveConvergence(t *testing.T) {
	liveCleanup(t)
	t.Cleanup(func() { liveCleanup(t) })

	desired := liveParams(t, map[string]any{
		"password": "s3cret",
		// Deliberately mixed case and a mix of rule kinds: Redis lowercases the
		// commands, roots them at -@all, and stores keys and channels separately.
		//
		// `resetchannels` is explicit because rule portability is the operator's
		// business, not the module's: on 6.2 `acl-pubsub-default` is
		// `allchannels`, so `&events:*` is rejected without it, while on 7.x the
		// default is already `resetchannels`. The module passes rules through
		// verbatim and reports the server's own error — inserting the token
		// silently would change what the operator asked for.
		"rules": []any{"~cache:*", "resetchannels", "&events:*", "+@read", "+SET", "-DEBUG"},
	})

	first := runLive(t, "present", desired)
	if first.GetFailed() {
		t.Fatalf("first apply failed: %s", first.GetMessage())
	}
	if !first.GetChanged() {
		t.Fatal("first apply must create the user and report changed")
	}

	second := runLive(t, "present", desired)
	if second.GetFailed() {
		t.Fatalf("second apply failed: %s", second.GetMessage())
	}
	if second.GetChanged() {
		t.Fatalf("second apply must be a no-op, got %q (changes=%q)",
			second.GetMessage(), second.GetOutput().GetFields()["changes"].GetStringValue())
	}
}

// TestLivePasswordIsComparedNotRewritten covers the second design claim: an
// unchanged password is recognised through the stored SHA-256, and a rotation
// leaves exactly one valid password rather than two.
func TestLivePasswordIsComparedNotRewritten(t *testing.T) {
	liveCleanup(t)
	t.Cleanup(func() { liveCleanup(t) })

	p := liveParams(t, map[string]any{"password": "first-pass", "rules": []any{"~*", "+@read"}})
	if ev := runLive(t, "present", p); ev.GetFailed() {
		t.Fatalf("create: %s", ev.GetMessage())
	}
	if ev := runLive(t, "present", p); ev.GetChanged() {
		t.Fatal("an unchanged password must not report a change")
	}

	p["password"] = "second-pass"
	ev := runLive(t, "present", p)
	if !ev.GetChanged() {
		t.Fatal("a rotated password must report a change")
	}

	// The old password must be gone, not merely joined by the new one: `>pass`
	// adds to the set, and only `reset` clears it.
	client, err := dial(context.Background(), connParams{host: hostOf(t), port: portOf(t), timeout: liveTimeout})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = client.Close() }()
	u, exists, err := client.GetUser(context.Background(), "soulstack_live_test")
	if err != nil || !exists {
		t.Fatalf("read back: err=%v exists=%v", err, exists)
	}
	if len(u.passwords) != 1 {
		t.Fatalf("after rotation the user must hold exactly one password, got %d: %v", len(u.passwords), u.passwords)
	}
	if !strings.EqualFold(u.passwords[0], sha256Hex("second-pass")) {
		t.Fatalf("the surviving password is not the new one: %v", u.passwords)
	}
}

// TestLiveRuleOrderChangesPermissions pins the reason rules are never sorted:
// the same tokens in another order are a different grant, and the module must
// see that as drift.
func TestLiveRuleOrderChangesPermissions(t *testing.T) {
	liveCleanup(t)
	t.Cleanup(func() { liveCleanup(t) })

	p := liveParams(t, map[string]any{"password": "p", "rules": []any{"~*", "&*", "+@all", "-@admin"}})
	if ev := runLive(t, "present", p); ev.GetFailed() {
		t.Fatalf("create: %s", ev.GetMessage())
	}

	p["rules"] = []any{"~*", "&*", "-@admin", "+@all"}
	ev := runLive(t, "present", p)
	if !ev.GetChanged() {
		t.Fatal("reordering +@all and -@admin grants admin rights — the module must not treat it as equal")
	}
}

// TestLiveBadRuleLeavesUserUntouched checks that an unparseable rule fails
// against the scratch user rather than half-applying to the managed one.
func TestLiveBadRuleLeavesUserUntouched(t *testing.T) {
	liveCleanup(t)
	t.Cleanup(func() { liveCleanup(t) })

	good := liveParams(t, map[string]any{"password": "p", "rules": []any{"~ok:*", "+@read"}})
	if ev := runLive(t, "present", good); ev.GetFailed() {
		t.Fatalf("create: %s", ev.GetMessage())
	}

	bad := liveParams(t, map[string]any{"password": "p", "rules": []any{"~new:*", "+@write", "THIS-IS-GARBAGE"}})
	ev := runLive(t, "present", bad)
	if !ev.GetFailed() {
		t.Fatal("an unparseable rule must fail the step")
	}

	// The user must still be the one the good apply left behind.
	if ev := runLive(t, "present", good); ev.GetChanged() {
		t.Fatalf("the failed apply modified the managed user (changes=%q)",
			ev.GetOutput().GetFields()["changes"].GetStringValue())
	}
}

// TestLiveProbeUserDoesNotSurvive makes sure the scratch user is not left
// behind on the server.
func TestLiveProbeUserDoesNotSurvive(t *testing.T) {
	liveCleanup(t)
	t.Cleanup(func() { liveCleanup(t) })

	p := liveParams(t, map[string]any{"password": "p", "rules": []any{"~*", "+@read"}})
	if ev := runLive(t, "present", p); ev.GetFailed() {
		t.Fatalf("create: %s", ev.GetMessage())
	}

	client, err := dial(context.Background(), connParams{host: hostOf(t), port: portOf(t), timeout: liveTimeout})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = client.Close() }()
	if _, exists, err := client.GetUser(context.Background(), probePrefix+"soulstack_live_test"); err != nil {
		t.Fatalf("read back: %v", err)
	} else if exists {
		t.Fatal("the scratch user outlived the call")
	}
}

const liveTimeout = 10_000_000_000 // 10s

func hostOf(t *testing.T) string {
	t.Helper()
	host, _, _ := strings.Cut(os.Getenv("REDIS_ADDR"), ":")
	return host
}

func portOf(t *testing.T) int {
	t.Helper()
	_, p, _ := strings.Cut(os.Getenv("REDIS_ADDR"), ":")
	n, err := strconv.Atoi(p)
	if err != nil {
		t.Fatalf("REDIS_ADDR port: %v", err)
	}
	return n
}

// TestLivePersistFailsWithoutAclfile pins the default's sharp edge: with persist
// on and no `aclfile` on the server, Redis has nowhere to write, and the step
// must say so rather than report a durability it did not achieve.
func TestLivePersistFailsWithoutAclfile(t *testing.T) {
	addr := os.Getenv("REDIS_ADDR_NO_ACLFILE")
	if addr == "" {
		t.Fatal("REDIS_ADDR_NO_ACLFILE is not set: this test needs a server without an aclfile and will not silently skip")
	}
	host, portStr, ok := strings.Cut(addr, ":")
	if !ok {
		t.Fatalf("REDIS_ADDR_NO_ACLFILE=%q: want host:port", addr)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("REDIS_ADDR_NO_ACLFILE=%q: %v", addr, err)
	}

	p := map[string]any{
		"host": host, "port": float64(port),
		"username": "soulstack_live_test",
		"password": "p", "rules": []any{"~*", "+@read"},
	}
	ev := runLive(t, "present", p)
	if !ev.GetFailed() {
		t.Fatal("persist defaults to on; without an aclfile ACL SAVE cannot work and the step must fail")
	}
	if !strings.Contains(ev.GetMessage(), "persist: false") {
		t.Fatalf("the message must say how to proceed, got %q", ev.GetMessage())
	}

	// And with persistence turned off the same apply goes through.
	p["persist"] = false
	if ev := runLive(t, "present", p); ev.GetFailed() {
		t.Fatalf("persist: false must work on a server with no aclfile, got %s", ev.GetMessage())
	}
	// Clean up on that server too.
	delete(p, "password")
	delete(p, "rules")
	if ev := runLive(t, "absent", p); ev.GetFailed() {
		t.Fatalf("cleanup: %s", ev.GetMessage())
	}
}
