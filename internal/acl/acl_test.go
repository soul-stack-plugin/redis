// L0 unit tests for Apply. They use a fake client — no server is contacted.
//
// The fake deliberately renders rules the way Redis does and then some: it
// lowercases them and prepends `-@all`, so what comes back never equals what
// the operator wrote. That is the point. A module that compares the desired
// rules against the returned text reports a change on every run; this one
// compares two renderings produced by the same renderer, so it does not.
package acl

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	pluginv1 "github.com/souls-guild/soul-stack/proto/plugin/gen/go/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/structpb"
)

type fakeApplyStream struct {
	grpc.ServerStream
	ctx    context.Context
	events []*pluginv1.ApplyEvent
}

func (f *fakeApplyStream) Context() context.Context     { return f.ctx }
func (f *fakeApplyStream) SetHeader(metadata.MD) error  { return nil }
func (f *fakeApplyStream) SendHeader(metadata.MD) error { return nil }
func (f *fakeApplyStream) SetTrailer(metadata.MD)       {}
func (f *fakeApplyStream) Send(e *pluginv1.ApplyEvent) error {
	f.events = append(f.events, e)
	return nil
}
func (f *fakeApplyStream) last() *pluginv1.ApplyEvent {
	if len(f.events) == 0 {
		return nil
	}
	return f.events[len(f.events)-1]
}

// fakeClient is an in-memory stand-in for a Redis server's ACL table.
type fakeClient struct {
	users map[string]aclUser
	calls []string

	setErr  error
	saveErr error
	// rejectRule makes SetUser fail for any token equal to this value, standing
	// in for a rule Redis refuses to parse.
	rejectRule string
}

func newFake() *fakeClient { return &fakeClient{users: map[string]aclUser{}} }

func (f *fakeClient) Close() error { return nil }

func (f *fakeClient) GetUser(_ context.Context, username string) (aclUser, bool, error) {
	u, ok := f.users[username]
	return u, ok, nil
}

func (f *fakeClient) DelUser(_ context.Context, username string) error {
	f.calls = append(f.calls, "DELUSER "+username)
	delete(f.users, username)
	return nil
}

func (f *fakeClient) Save(_ context.Context) error {
	f.calls = append(f.calls, "SAVE")
	return f.saveErr
}

// SetUser mimics `ACL SETUSER`: tokens are applied left to right, and the stored
// form is the server's own rendering, not the caller's text.
func (f *fakeClient) SetUser(_ context.Context, username string, rules []string) error {
	if f.setErr != nil {
		return f.setErr
	}
	for _, r := range rules {
		if f.rejectRule != "" && r == f.rejectRule {
			return errors.New("Error in ACL SETUSER modifier '" + r + "': Syntax error")
		}
	}
	f.calls = append(f.calls, "SETUSER "+username+" "+strings.Join(rules, " "))

	u := f.users[username]
	var cmds, keys, chans []string
	for _, r := range rules {
		switch {
		case r == "reset":
			u = aclUser{}
			cmds, keys, chans = nil, nil, nil
		case r == "on":
			u.enabled = true
		case r == "off":
			u.enabled = false
		case r == "nopass":
			u.nopass = true
			u.passwords = nil
		case strings.HasPrefix(r, ">"):
			u.nopass = false
			u.passwords = append(u.passwords, hashOf(r[1:]))
		case strings.HasPrefix(r, "#"):
			u.nopass = false
			u.passwords = append(u.passwords, strings.ToLower(r[1:]))
		case strings.HasPrefix(r, "~"), strings.HasPrefix(r, "%"):
			keys = append(keys, r)
		case strings.HasPrefix(r, "&"):
			chans = append(chans, r)
		case strings.HasPrefix(r, "("):
			u.selectors = append(u.selectors, strings.ToLower(r))
		default:
			cmds = append(cmds, strings.ToLower(r))
		}
	}
	// The server's rendering: always rooted at -@all, always lowercase.
	u.commands = strings.TrimSpace("-@all " + strings.Join(cmds, " "))
	u.keys = strings.Join(keys, " ")
	u.channels = strings.Join(chans, " ")
	f.users[username] = u
	return nil
}

func hashOf(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// seed installs a user exactly as the fake server would have rendered it.
func (f *fakeClient) seed(username string, rules []string) {
	_ = f.SetUser(context.Background(), username, append([]string{"reset"}, rules...))
	f.calls = nil
}

func params(t *testing.T, m map[string]any) *structpb.Struct {
	t.Helper()
	s, err := structpb.NewStruct(m)
	if err != nil {
		t.Fatalf("build params: %v", err)
	}
	return s
}

func base(extra map[string]any) map[string]any {
	m := map[string]any{"host": "127.0.0.1", "username": "app"}
	for k, v := range extra {
		m[k] = v
	}
	return m
}

func run(t *testing.T, f *fakeClient, state string, p map[string]any) *pluginv1.ApplyEvent {
	t.Helper()
	m := &ACL{connect: func(context.Context, connParams) (aclClient, error) { return f, nil }}
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

func outStr(t *testing.T, ev *pluginv1.ApplyEvent, key string) string {
	t.Helper()
	return ev.GetOutput().GetFields()[key].GetStringValue()
}

func TestPresentCreatesWhenAbsent(t *testing.T) {
	f := newFake()
	ev := run(t, f, "present", base(map[string]any{
		"password": "s3cret",
		"rules":    []any{"~cache:*", "+@read"},
	}))
	if ev.GetFailed() {
		t.Fatalf("unexpected failure: %s", ev.GetMessage())
	}
	if !ev.GetChanged() {
		t.Fatal("creating a user must report changed")
	}
	if got := outStr(t, ev, "action"); got != "created" {
		t.Fatalf("action = %q, want created", got)
	}
	var wrote string
	for _, c := range f.calls {
		if strings.HasPrefix(c, "SETUSER app ") {
			wrote = c
		}
	}
	if !strings.Contains(wrote, "reset") || !strings.Contains(wrote, "on") || !strings.Contains(wrote, ">s3cret") {
		t.Fatalf("write did not carry the full definition: %q", wrote)
	}
}

// The regression this module exists for: the operator writes `+@read`, the
// server stores `-@all +@read`, and a second run must still be a no-op.
func TestPresentNoopWhenServerRenderingDiffers(t *testing.T) {
	f := newFake()
	f.seed("app", []string{"on", ">s3cret", "~cache:*", "+@read"})

	ev := run(t, f, "present", base(map[string]any{
		"password": "s3cret",
		"rules":    []any{"~cache:*", "+@read"},
	}))
	if ev.GetFailed() {
		t.Fatalf("unexpected failure: %s", ev.GetMessage())
	}
	if ev.GetChanged() {
		t.Fatalf("expected no change, got %q (changes=%q)", ev.GetMessage(), outStr(t, ev, "changes"))
	}
	if got := outStr(t, ev, "action"); got != "noop" {
		t.Fatalf("action = %q, want noop", got)
	}
	for _, c := range f.calls {
		if strings.HasPrefix(c, "SETUSER app ") {
			t.Fatalf("a no-op must not write the managed user, got %q", c)
		}
	}
}

// Rule order is part of the meaning, so a reordering that changes what Redis
// stores has to register as a change rather than be sorted away.
func TestPresentRuleOrderIsNotNormalisedAway(t *testing.T) {
	f := newFake()
	f.seed("app", []string{"on", ">p", "~*", "+@all", "-@admin"})

	ev := run(t, f, "present", base(map[string]any{
		"password": "p",
		"rules":    []any{"~*", "-@admin", "+@all"},
	}))
	if !ev.GetChanged() {
		t.Fatal("a different rule order stores different permissions and must report a change")
	}
	if !strings.Contains(outStr(t, ev, "changes"), "commands") {
		t.Fatalf("changes = %q, want it to mention commands", outStr(t, ev, "changes"))
	}
}

func TestPresentDriftOnRules(t *testing.T) {
	f := newFake()
	f.seed("app", []string{"on", ">s3cret", "~cache:*", "+@read"})

	ev := run(t, f, "present", base(map[string]any{
		"password": "s3cret",
		"rules":    []any{"~cache:*", "+@read", "+@write"},
	}))
	if !ev.GetChanged() {
		t.Fatal("added rule must report a change")
	}
	if got := outStr(t, ev, "action"); got != "altered" {
		t.Fatalf("action = %q, want altered", got)
	}
}

func TestPresentDriftOnEnabled(t *testing.T) {
	f := newFake()
	f.seed("app", []string{"off", ">s3cret", "+@read"})

	ev := run(t, f, "present", base(map[string]any{
		"password": "s3cret",
		"rules":    []any{"+@read"},
	}))
	if !ev.GetChanged() {
		t.Fatal("off → on must report a change")
	}
	if !strings.Contains(outStr(t, ev, "changes"), "enabled") {
		t.Fatalf("changes = %q, want it to mention enabled", outStr(t, ev, "changes"))
	}
}

func TestPresentUnchangedPasswordIsNotRewritten(t *testing.T) {
	f := newFake()
	f.seed("app", []string{"on", ">s3cret", "+@read"})

	ev := run(t, f, "present", base(map[string]any{
		"password": "s3cret",
		"rules":    []any{"+@read"},
	}))
	if ev.GetChanged() {
		t.Fatalf("an unchanged password must not report a change, got changes=%q", outStr(t, ev, "changes"))
	}
}

func TestPresentChangedPassword(t *testing.T) {
	f := newFake()
	f.seed("app", []string{"on", ">old", "+@read"})

	ev := run(t, f, "present", base(map[string]any{
		"password": "new",
		"rules":    []any{"+@read"},
	}))
	if !ev.GetChanged() {
		t.Fatal("a changed password must report a change")
	}
	if !strings.Contains(outStr(t, ev, "changes"), "password") {
		t.Fatalf("changes = %q, want it to mention password", outStr(t, ev, "changes"))
	}
	// `reset` is what stops the old password from staying valid: `>pass` alone
	// would add the new one beside it.
	if u := f.users["app"]; len(u.passwords) != 1 || u.passwords[0] != hashOf("new") {
		t.Fatalf("after rotation the user must hold exactly the new password, got %v", u.passwords)
	}
}

func TestPresentPasswordHashIsAppliedAsHashToken(t *testing.T) {
	f := newFake()
	h := hashOf("s3cret")
	ev := run(t, f, "present", base(map[string]any{
		"password_hash": h,
		"rules":         []any{"+@read"},
	}))
	if ev.GetFailed() {
		t.Fatalf("unexpected failure: %s", ev.GetMessage())
	}
	var wrote string
	for _, c := range f.calls {
		if strings.HasPrefix(c, "SETUSER app ") {
			wrote = c
		}
	}
	if !strings.Contains(wrote, "#"+h) {
		t.Fatalf("hash must be applied as Redis' own #<hash> token, got %q", wrote)
	}
	// And a second run with the same hash is a no-op.
	f.calls = nil
	ev = run(t, f, "present", base(map[string]any{"password_hash": h, "rules": []any{"+@read"}}))
	if ev.GetChanged() {
		t.Fatalf("same hash must be a no-op, got changes=%q", outStr(t, ev, "changes"))
	}
}

func TestPresentSelectorDriftIsSeen(t *testing.T) {
	f := newFake()
	f.seed("app", []string{"on", ">p", "~main:*", "+@read", "(%R~ro:* +get)"})

	ev := run(t, f, "present", base(map[string]any{
		"password": "p",
		"rules":    []any{"~main:*", "+@read"},
	}))
	if !ev.GetChanged() {
		t.Fatal("a selector present on the server but absent from the desired state grants access invisibly — it must register")
	}
	if !strings.Contains(outStr(t, ev, "changes"), "selectors") {
		t.Fatalf("changes = %q, want it to mention selectors", outStr(t, ev, "changes"))
	}
}

// The scratch user must be disabled and passwordless while it exists, and must
// not outlive the call.
func TestProbeUserIsDisabledAndCleanedUp(t *testing.T) {
	f := newFake()
	run(t, f, "present", base(map[string]any{"password": "p", "rules": []any{"+@read"}}))

	probe := probePrefix + "app"
	if _, still := f.users[probe]; still {
		t.Fatal("the scratch user must be deleted before the module returns")
	}
	var created string
	for _, c := range f.calls {
		if strings.HasPrefix(c, "SETUSER "+probe+" ") {
			created = c
		}
	}
	if created == "" {
		t.Fatal("expected the desired rules to be normalised through a scratch user")
	}
	if !strings.Contains(created, " off ") || !strings.Contains(created, " nopass ") {
		t.Fatalf("the scratch user must be off and nopass so it cannot be logged into, got %q", created)
	}
	if !containsCall(f.calls, "DELUSER "+probe) {
		t.Fatal("the scratch user must be deleted")
	}
}

// A rule Redis refuses must fail against the scratch user, leaving the managed
// user untouched.
func TestBadRuleFailsBeforeTouchingTheManagedUser(t *testing.T) {
	f := newFake()
	f.seed("app", []string{"on", ">p", "+@read"})
	f.rejectRule = "GARBAGE"

	ev := run(t, f, "present", base(map[string]any{
		"password": "p",
		"rules":    []any{"+@read", "GARBAGE"},
	}))
	if !ev.GetFailed() {
		t.Fatal("an unparseable rule must fail the step")
	}
	for _, c := range f.calls {
		if strings.HasPrefix(c, "SETUSER app ") {
			t.Fatalf("the managed user must not be written when the rules are invalid, got %q", c)
		}
	}
}

// Durability is the default: ACL SETUSER alone lives in memory, so `present`
// would stop being true at the next restart.
func TestPersistIsOnByDefault(t *testing.T) {
	f := newFake()
	ev := run(t, f, "present", base(map[string]any{"password": "p", "rules": []any{"+@read"}}))
	if !containsCall(f.calls, "SAVE") {
		t.Fatal("persist defaults to true — ACL SAVE must run")
	}
	if !ev.GetOutput().GetFields()["persisted"].GetBoolValue() {
		t.Fatal("persisted must be reported true when ACL SAVE ran")
	}

	f2 := newFake()
	ev = run(t, f2, "present", base(map[string]any{
		"password": "p", "rules": []any{"+@read"}, "persist": false,
	}))
	if containsCall(f2.calls, "SAVE") {
		t.Fatal("persist: false must not run ACL SAVE")
	}
	if ev.GetOutput().GetFields()["persisted"].GetBoolValue() {
		t.Fatal("persisted must be false when ACL SAVE did not run")
	}
}

// A server with no aclfile has nowhere to write. Reporting success there would
// claim a durability that is not present, so the step fails and says what to do.
func TestPersistFailureIsReportedNotSwallowed(t *testing.T) {
	f := newFake()
	f.saveErr = errors.New("ERR This Redis instance is not configured to use an ACL file")
	ev := run(t, f, "present", base(map[string]any{"password": "p", "rules": []any{"+@read"}}))
	if !ev.GetFailed() {
		t.Fatal("a failed ACL SAVE must fail the step")
	}
	if !strings.Contains(ev.GetMessage(), "persist: false") {
		t.Fatalf("the message must say how to proceed, got %q", ev.GetMessage())
	}
}

// Saying nothing about the credential means "leave it alone" — the reading
// anyone expects. The existing digest is carried across the `reset`.
func TestOmittedPasswordKeepsTheExistingOne(t *testing.T) {
	f := newFake()
	f.seed("app", []string{"on", ">s3cret", "~cache:*", "+@read"})

	ev := run(t, f, "present", base(map[string]any{"rules": []any{"~cache:*", "+@read", "+@write"}}))
	if ev.GetFailed() {
		t.Fatalf("unexpected failure: %s", ev.GetMessage())
	}
	if strings.Contains(ev.GetOutput().GetFields()["changes"].GetStringValue(), "password") {
		t.Fatalf("omitting the credential must not count as a password change, got %q",
			ev.GetOutput().GetFields()["changes"].GetStringValue())
	}
	if u := f.users["app"]; len(u.passwords) != 1 || u.passwords[0] != hashOf("s3cret") {
		t.Fatalf("the existing password must survive the reset, got %v", u.passwords)
	}
}

func TestOmittedPasswordIsANoopOnItsOwn(t *testing.T) {
	f := newFake()
	f.seed("app", []string{"on", ">s3cret", "~cache:*", "+@read"})

	ev := run(t, f, "present", base(map[string]any{"rules": []any{"~cache:*", "+@read"}}))
	if ev.GetChanged() {
		t.Fatalf("nothing was asked to change, got %q", ev.GetOutput().GetFields()["changes"].GetStringValue())
	}
}

// There is nothing to keep on a user that does not exist yet, and an enabled
// user without a credential can never authenticate.
func TestOmittedPasswordOnANewUserIsRefused(t *testing.T) {
	ev := run(t, newFake(), "present", base(map[string]any{"rules": []any{"+@read"}}))
	if !ev.GetFailed() {
		t.Fatal("a new enabled user with no credential must be refused")
	}
	if !strings.Contains(ev.GetMessage(), "no password to keep") {
		t.Fatalf("message = %q", ev.GetMessage())
	}
}

func TestOmittedPasswordKeepsNopass(t *testing.T) {
	f := newFake()
	f.seed("app", []string{"on", "nopass", "~*", "+@read"})

	run(t, f, "present", base(map[string]any{"rules": []any{"~*", "+@read", "+@write"}}))
	if u := f.users["app"]; !u.nopass {
		t.Fatal("nopass must survive the reset when the credential was not mentioned")
	}
}

func TestAbsentDeletesAndIsIdempotent(t *testing.T) {
	f := newFake()
	f.seed("app", []string{"on", ">p", "+@read"})

	ev := run(t, f, "absent", base(nil))
	if !ev.GetChanged() {
		t.Fatal("deleting an existing user must report a change")
	}
	if got := outStr(t, ev, "action"); got != "dropped" {
		t.Fatalf("action = %q, want dropped", got)
	}

	ev = run(t, f, "absent", base(nil))
	if ev.GetChanged() {
		t.Fatal("deleting an already absent user must be a no-op")
	}
}

func TestRefusesToDeleteTheUserItAuthenticatedAs(t *testing.T) {
	f := newFake()
	f.seed("app", []string{"on", ">p", "+@all"})

	ev := run(t, f, "absent", base(map[string]any{
		"login_username": "app", "login_password": "p",
	}))
	if !ev.GetFailed() {
		t.Fatal("Redis allows this and it locks the next run out — the module must refuse")
	}
	if _, gone := f.users["app"]; !gone {
		t.Fatal("the user must still exist after the refusal")
	}
}

func TestRefusesToResetTheUserItAuthenticatedAs(t *testing.T) {
	f := newFake()
	f.seed("app", []string{"on", ">p", "+@all"})

	p := base(map[string]any{
		"login_username": "app", "login_password": "p",
		"password": "p", "rules": []any{"+@read"},
	})
	if ev := run(t, f, "present", p); !ev.GetFailed() {
		t.Fatal("the write starts with reset, so narrowing your own user locks this connection out — must refuse")
	}

	p["allow_self_modification"] = true
	if ev := run(t, f, "present", p); ev.GetFailed() {
		t.Fatalf("allow_self_modification must let it through, got %s", ev.GetMessage())
	}
}

func TestDefaultUserIsGated(t *testing.T) {
	f := newFake()
	f.seed("default", []string{"on", "nopass", "~*", "+@all"})

	p := map[string]any{"host": "h", "username": "default", "enabled": false, "nopass": true}
	if ev := run(t, f, "present", p); !ev.GetFailed() {
		t.Fatal("`default off` locks out every unauthenticated client — must be gated")
	}
	p["allow_default_user"] = true
	if ev := run(t, f, "present", p); ev.GetFailed() {
		t.Fatalf("allow_default_user must let it through, got %s", ev.GetMessage())
	}

	if ev := run(t, f, "absent", map[string]any{"host": "h", "username": "default"}); !ev.GetFailed() {
		t.Fatal("Redis cannot delete `default` at all — refuse before trying")
	}
}

func TestParamValidation(t *testing.T) {
	cases := []struct {
		name  string
		state string
		p     map[string]any
		want  string
	}{
		{"username with a space", "present", map[string]any{"host": "h", "username": "bad name", "nopass": true}, "not a valid user name"},
		{"password and nopass together", "present", base(map[string]any{"password": "p", "nopass": true}), "mutually exclusive"},
		{"rule carrying whitespace", "present", base(map[string]any{"password": "p", "rules": []any{"+@read -DEBUG"}}), "contains whitespace"},
		{"malformed password hash", "present", base(map[string]any{"password_hash": "not-hex"}), "hex SHA-256 digest"},
		{"missing host", "present", map[string]any{"username": "app", "nopass": true}, "params.host"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ev := run(t, newFake(), tc.state, tc.p)
			if !ev.GetFailed() {
				t.Fatalf("expected a failure, got %q", ev.GetMessage())
			}
			if !strings.Contains(ev.GetMessage(), tc.want) {
				t.Fatalf("message = %q, want it to contain %q", ev.GetMessage(), tc.want)
			}
		})
	}
}

func TestUnknownState(t *testing.T) {
	ev := run(t, newFake(), "installed", base(map[string]any{"password": "p"}))
	if !ev.GetFailed() {
		t.Fatal("an unknown state must fail")
	}
	if !strings.Contains(ev.GetMessage(), "expected present|absent") {
		t.Fatalf("message = %q", ev.GetMessage())
	}
}

func containsCall(calls []string, want string) bool {
	for _, c := range calls {
		if c == want {
			return true
		}
	}
	return false
}
