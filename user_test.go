package main

// L0 for the `user` object (user.go, NIM-767). Fake redisConn + fake ApplyEvent
// stream, driven through the real object.Apply so the connect path and the action
// table are exercised too.
//
// Three of these are guards rather than coverage, and each one is here because the
// obvious implementation gets it wrong:
//
//   - TestApplyUserPresent_PasswordIsHashedNeverPlaintext — the plaintext must be
//     absent from the argument vector AND the sha256 present in it. Asserting only
//     the absence would pass just as well on a build that dropped the credential
//     entirely, which is a silent lockout, not a fix.
//   - TestApplyUserPresent_ResetIsDeclarative + the carry-over pair — `reset` is
//     what makes `present` converge downward, and carrying the live hashes past it
//     is what keeps that from revoking a working credential. Either one alone is a
//     defect.
//   - TestApplyUserPresent_FailedSetUserPersistsNothing — the NIM-624 shape. That
//     ticket's failure was a half-written users.acl left behind by a failed ACL
//     LOAD; the equivalent mistake here is saving after a command that failed.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"

	pluginv1 "github.com/souls-guild/soul-stack/proto/plugin/gen/go/v1"
)

// userPass — the MANAGED user's password (params.user_password), distinct from
// secretPass, which is the connection's. Both must stay out of events; only this
// one has a hash that is expected to appear on the wire.
const userPass = "vault-resolved-appuser-4c8b1d90ae"

// userConn — fake redisConn for the `user` object. Records every Do call, answers
// ACL LIST from a sequential script (before / after, in the order applyUserPresent
// reads it), and can fail ONE command chosen by its verb.
//
// The per-verb failure is what the persistence invariant needs: "SETUSER failed"
// and "ACL SAVE failed" are different claims about different files, and a fake that
// fails every Do at once cannot tell them apart.
type userConn struct {
	cfg      connConfig
	calls    [][]any
	aclSeq   [][]string
	aclCalls int
	aclErr   error

	failVerb string // matched as a prefix of the rendered call, e.g. "ACL SAVE"
	failErr  error

	// replies overrides the default "OK" for a call whose rendered line starts with
	// the key. DELUSER answers with a COUNT, and the difference between "1" and "0"
	// is a real branch (someone else removed the user first).
	replies map[string]string

	// noAclfile makes CONFIG GET aclfile answer empty — an instance that cannot
	// persist an ACL at all.
	noAclfile bool

	// configErr makes CONFIG GET fail — a connection permitted to manage the ACL but
	// not to read the configuration (NOPERM, or rename-command CONFIG "").
	configErr error
}

func (c *userConn) Do(_ context.Context, args ...any) (string, error) {
	line := argsLine(args)
	c.calls = append(c.calls, args)
	if c.failVerb != "" && strings.HasPrefix(line, c.failVerb) {
		return "", c.failErr
	}
	for prefix, reply := range c.replies {
		if strings.HasPrefix(line, prefix) {
			return reply, nil
		}
	}
	return "OK", nil
}

// ConfigGet answers the aclfile pre-flight. The default is an instance that HAS an
// aclfile, because that is the shape every other test is about; noAclfile is the one
// that does not, and it is a real branch — a persisting apply against it is refused
// before anything is mutated.
//
// configErr is the third case and the one a fake had to grow to express at all: a
// connection that may run ACL SETUSER but not CONFIG GET. That is not exotic —
// CONFIG is @admin/@dangerous and the ACL commands are not, so it is what a
// least-privilege operator looks like, and the first pre-flight refused every one of
// them.
func (c *userConn) ConfigGet(_ context.Context, param string) (map[string]string, error) {
	if c.configErr != nil {
		return nil, c.configErr
	}
	if param == "aclfile" && !c.noAclfile {
		return map[string]string{param: "/etc/redis/users.acl"}, nil
	}
	return map[string]string{param: ""}, nil
}

func (c *userConn) GetKeysInSlot(_ context.Context, _, _ int) ([]string, error) { return nil, nil }

// AclList returns the next scripted answer, recording the call so an assertion can
// see the LIST → SETUSER → LIST order. A script shorter than the number of calls
// repeats its last element.
func (c *userConn) AclList(_ context.Context) ([]string, error) {
	c.calls = append(c.calls, []any{"ACL", "LIST"})
	if c.aclErr != nil {
		return nil, c.aclErr
	}
	i := c.aclCalls
	c.aclCalls++
	if i >= len(c.aclSeq) {
		if len(c.aclSeq) == 0 {
			return nil, nil
		}
		return c.aclSeq[len(c.aclSeq)-1], nil
	}
	return c.aclSeq[i], nil
}

func (c *userConn) Close() error { return nil }

// argsLine renders a recorded call the way Redis would see it, so an assertion can
// name a whole vector instead of indexing into []any.
func argsLine(args []any) string {
	parts := make([]string, 0, len(args))
	for _, a := range args {
		parts = append(parts, fmt.Sprint(a))
	}
	return strings.Join(parts, " ")
}

// call returns the first recorded call whose rendered line starts with prefix.
func (c *userConn) call(prefix string) (string, bool) {
	for _, args := range c.calls {
		if line := argsLine(args); strings.HasPrefix(line, prefix) {
			return line, true
		}
	}
	return "", false
}

func (c *userConn) sent(prefix string) bool {
	_, ok := c.call(prefix)
	return ok
}

func newUserModule(conn *userConn) *RedisModule {
	return &RedisModule{
		connect: func(_ context.Context, cfg connConfig) (redisConn, error) {
			conn.cfg = cfg
			return conn, nil
		},
	}
}

// applyUser drives the object through its real dispatch.
func applyUser(t *testing.T, m *RedisModule, state string, params map[string]any) (*applyStream, *pluginv1.ApplyEvent) {
	t.Helper()
	stream := &applyStream{}
	_ = m.user().Apply(&pluginv1.ApplyRequest{State: state, Params: mustStruct(t, params)}, stream)
	return stream, stream.final()
}

func validateUser(t *testing.T, state string, params map[string]any) *pluginv1.ValidateReply {
	t.Helper()
	m := &RedisModule{}
	reply, _ := m.user().Validate(context.Background(), &pluginv1.ValidateRequest{
		State:  state,
		Params: mustStruct(t, params),
	})
	return reply
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// --- Validate: present ---

func TestValidate_UserPresentRejectsEmptyAddr(t *testing.T) {
	reply := validateUser(t, "present", map[string]any{"addr": "", "name": "app", "perms": "~* +@read"})
	if reply.Ok {
		t.Fatal("waited Ok=false on empty addr")
	}
}

func TestValidate_UserPresentRejectsEmptyName(t *testing.T) {
	reply := validateUser(t, "present", map[string]any{"addr": "127.0.0.1:6379", "name": "  ", "perms": "~* +@read"})
	if reply.Ok {
		t.Fatal("waited Ok=false on a blank name")
	}
}

// TestValidate_UserPresentRejectsWhitespaceName — a name with a space would arrive
// as one RESP argument and be rejected by Redis mid-apply; the author is told first.
func TestValidate_UserPresentRejectsWhitespaceName(t *testing.T) {
	reply := validateUser(t, "present", map[string]any{"addr": "127.0.0.1:6379", "name": "app user", "perms": "~* +@read"})
	if reply.Ok {
		t.Fatal("waited Ok=false on a name carrying whitespace")
	}
}

func TestValidate_UserPresentRejectsEmptyPerms(t *testing.T) {
	reply := validateUser(t, "present", map[string]any{"addr": "127.0.0.1:6379", "name": "app", "perms": ""})
	if reply.Ok {
		t.Fatal("waited Ok=false on empty perms")
	}
}

// TestValidate_UserPresentRejectsBooleanState — the YAML 1.1 trap, refused rather
// than coerced. `state: off` unquoted parses as false long before the plugin sees
// it, and coercing a bool would land the one value that disables an account as the
// default "on" — a silent inversion of the declaration.
func TestValidate_UserPresentRejectsBooleanState(t *testing.T) {
	for _, v := range []bool{true, false} {
		t.Run(fmt.Sprint(v), func(t *testing.T) {
			reply := validateUser(t, "present", map[string]any{
				"addr": "127.0.0.1:6379", "name": "app", "perms": "~* +@read", "state": v,
			})
			if reply.Ok {
				t.Fatalf("waited Ok=false on a boolean state (%v) — an unquoted YAML on/off must not be coerced", v)
			}
		})
	}
}

func TestValidate_UserPresentRejectsUnknownState(t *testing.T) {
	reply := validateUser(t, "present", map[string]any{
		"addr": "127.0.0.1:6379", "name": "app", "perms": "~* +@read", "state": "enabled",
	})
	if reply.Ok {
		t.Fatal(`waited Ok=false on a state other than "on"/"off"`)
	}
}

func TestValidate_UserPresentHappyPath(t *testing.T) {
	reply := validateUser(t, "present", map[string]any{
		"addr": "127.0.0.1:6379", "name": "app", "perms": "~app:* +@read +@write", "state": "off",
	})
	if !reply.Ok || len(reply.Errors) != 0 {
		t.Fatalf("waited Ok=true without errors, got %+v", reply)
	}
}

// --- Validate: absent ---

// TestValidate_UserAbsentRejectsDefault — Redis cannot remove the built-in user, so
// the refusal belongs at validation with the way out named, not in a Redis error.
func TestValidate_UserAbsentRejectsDefault(t *testing.T) {
	reply := validateUser(t, "absent", map[string]any{"addr": "127.0.0.1:6379", "name": "default"})
	if reply.Ok {
		t.Fatal(`waited Ok=false on name "default"`)
	}
}

func TestValidate_UserAbsentHappyPath(t *testing.T) {
	reply := validateUser(t, "absent", map[string]any{"addr": "127.0.0.1:6379", "name": "app"})
	if !reply.Ok || len(reply.Errors) != 0 {
		t.Fatalf("waited Ok=true without errors, got %+v", reply)
	}
}

func TestValidate_UserAbsentRejectsEmptyName(t *testing.T) {
	reply := validateUser(t, "absent", map[string]any{"addr": "127.0.0.1:6379", "name": ""})
	if reply.Ok {
		t.Fatal("waited Ok=false on an empty name")
	}
}

// --- Apply present ---

// TestApplyUserPresent_CreatesUserAndPersists — a user the instance does not have:
// the vector carries reset + state + hash + perms, changed=true, and ACL SAVE
// follows so the user survives a restart and the next ACL LOAD.
func TestApplyUserPresent_CreatesUserAndPersists(t *testing.T) {
	conn := &userConn{aclSeq: [][]string{
		{"user default on nopass ~* &* +@all"},
		{"user default on nopass ~* &* +@all", "user app on #" + sha256Hex(userPass) + " ~app:* +@read"},
	}}
	stream, fin := applyUser(t, newUserModule(conn), "present", map[string]any{
		"addr":          "127.0.0.1:6379",
		"name":          "app",
		"perms":         "~app:* +@read",
		"user_password": userPass,
	})

	if fin == nil || fin.Failed || !fin.Changed {
		t.Fatalf("waited changed=true without failure, got %+v", fin)
	}
	line, ok := conn.call("ACL SETUSER")
	if !ok {
		t.Fatalf("ACL SETUSER was not sent; calls: %v", conn.calls)
	}
	if want := "ACL SETUSER app reset on #" + sha256Hex(userPass) + " ~app:* +@read"; line != want {
		t.Errorf("rule vector\n got: %s\nwant: %s", line, want)
	}
	if !conn.sent("ACL SAVE") {
		t.Error("a changing SETUSER must be persisted (ACL SAVE), or the next restart / ACL LOAD reverts it")
	}
	if got := fin.GetOutput().GetFields()["name"].GetStringValue(); got != "app" {
		t.Errorf("Output.name = %q, want %q", got, "app")
	}
	assertEventsNoSecret(t, stream)
}

// TestApplyUserPresent_ResetIsDeclarative — `reset` opens the vector, which is what
// makes `present` total: a permission dropped from the declaration is dropped on
// the instance. Without it SETUSER merges and the state converges upward only.
func TestApplyUserPresent_ResetIsDeclarative(t *testing.T) {
	conn := &userConn{aclSeq: [][]string{
		{"user app on nopass ~* &* +@all"},
		{"user app on nopass ~app:* +@read"},
	}}
	_, fin := applyUser(t, newUserModule(conn), "present", map[string]any{
		"addr":  "127.0.0.1:6379",
		"name":  "app",
		"perms": "~app:* +@read",
	})
	if fin == nil || fin.Failed {
		t.Fatalf("waited a successful apply, got %+v", fin)
	}
	line, _ := conn.call("ACL SETUSER")
	if !strings.HasPrefix(line, "ACL SETUSER app reset ") {
		t.Fatalf("the rule vector does not reset the user first: %s", line)
	}
	if strings.Contains(line, "+@all") {
		t.Errorf("a rule the declaration does not carry reached the vector: %s", line)
	}
}

// TestApplyUserPresent_PasswordIsHashedNeverPlaintext — the credential invariant,
// both halves. The plaintext must not appear in any argument, event, output or
// error; the sha256 MUST appear in the vector, because a build that simply dropped
// the password would satisfy the first half and lock every client out.
func TestApplyUserPresent_PasswordIsHashedNeverPlaintext(t *testing.T) {
	conn := &userConn{aclSeq: [][]string{{}, {"user app on #" + sha256Hex(userPass) + " ~* +@read"}}}
	stream, fin := applyUser(t, newUserModule(conn), "present", map[string]any{
		"addr":          "127.0.0.1:6379",
		"password":      secretPass,
		"name":          "app",
		"perms":         "~* +@read",
		"user_password": userPass,
	})
	if fin == nil || fin.Failed {
		t.Fatalf("waited a successful apply, got %+v", fin)
	}

	for i, args := range conn.calls {
		line := argsLine(args)
		if strings.Contains(line, userPass) {
			t.Errorf("call[%d] carries the managed user's password in the clear: %s", i, line)
		}
		if strings.Contains(line, secretPass) {
			t.Errorf("call[%d] carries the connection password: %s", i, line)
		}
	}
	line, _ := conn.call("ACL SETUSER")
	if !strings.Contains(line, "#"+sha256Hex(userPass)) {
		t.Errorf("the credential did not reach the vector as a hash — a dropped password is not a fix: %s", line)
	}

	for i, ev := range stream.sent {
		if strings.Contains(ev.GetMessage(), userPass) || strings.Contains(ev.GetOutput().String(), userPass) {
			t.Errorf("event[%d] leaks the managed user's password", i)
		}
	}
	assertEventsNoSecret(t, stream)
	// The connection is where the connection password belongs, and nowhere else.
	if conn.cfg.password != secretPass {
		t.Errorf("params.password did not reach the connect: %q", conn.cfg.password)
	}
}

// TestApplyUserPresent_OmittedPasswordCarriesLiveHash — an omitted user_password
// means "keep the credential clients already hold". `reset` clears passwords, so
// the hashes are read off the live ACL LIST line and re-applied; without this a
// perms-only re-run would revoke a working credential.
func TestApplyUserPresent_OmittedPasswordCarriesLiveHash(t *testing.T) {
	const liveHash = "#3f786850e387550fdab836ed7e6dc881de23001b"
	conn := &userConn{aclSeq: [][]string{
		{"user app on " + liveHash + " ~old:* +@read"},
		{"user app on " + liveHash + " ~app:* +@read"},
	}}
	_, fin := applyUser(t, newUserModule(conn), "present", map[string]any{
		"addr":  "127.0.0.1:6379",
		"name":  "app",
		"perms": "~app:* +@read",
	})
	if fin == nil || fin.Failed {
		t.Fatalf("waited a successful apply, got %+v", fin)
	}
	line, _ := conn.call("ACL SETUSER")
	if want := "ACL SETUSER app reset on " + liveHash + " ~app:* +@read"; line != want {
		t.Errorf("the live credential was not carried past the reset\n got: %s\nwant: %s", line, want)
	}
}

// TestApplyUserPresent_OmittedPasswordCarriesNopass — same carry-over, for a user
// deliberately declared without a credential.
func TestApplyUserPresent_OmittedPasswordCarriesNopass(t *testing.T) {
	conn := &userConn{aclSeq: [][]string{
		{"user app on nopass ~* +@all"},
		{"user app on nopass ~app:* +@read"},
	}}
	_, fin := applyUser(t, newUserModule(conn), "present", map[string]any{
		"addr":  "127.0.0.1:6379",
		"name":  "app",
		"perms": "~app:* +@read",
	})
	if fin == nil || fin.Failed {
		t.Fatalf("waited a successful apply, got %+v", fin)
	}
	line, _ := conn.call("ACL SETUSER")
	if !strings.Contains(line, " nopass ") {
		t.Errorf("nopass was not carried past the reset: %s", line)
	}
}

// TestApplyUserPresent_DeclaredPasswordReplacesLiveHash — a declared credential is
// the declaration, so it replaces whatever the user held rather than adding to it.
func TestApplyUserPresent_DeclaredPasswordReplacesLiveHash(t *testing.T) {
	const liveHash = "#3f786850e387550fdab836ed7e6dc881de23001b"
	conn := &userConn{aclSeq: [][]string{
		{"user app on " + liveHash + " ~app:* +@read"},
		{"user app on #" + sha256Hex(userPass) + " ~app:* +@read"},
	}}
	_, fin := applyUser(t, newUserModule(conn), "present", map[string]any{
		"addr":          "127.0.0.1:6379",
		"name":          "app",
		"perms":         "~app:* +@read",
		"user_password": userPass,
	})
	if fin == nil || fin.Failed {
		t.Fatalf("waited a successful apply, got %+v", fin)
	}
	line, _ := conn.call("ACL SETUSER")
	if strings.Contains(line, liveHash) {
		t.Errorf("the superseded credential is still in the vector: %s", line)
	}
	if !strings.Contains(line, "#"+sha256Hex(userPass)) {
		t.Errorf("the declared credential did not reach the vector: %s", line)
	}
}

// TestApplyUserPresent_StateOffReachesVector — "off" is the value the YAML trap
// would swallow; this proves a quoted one arrives.
func TestApplyUserPresent_StateOffReachesVector(t *testing.T) {
	conn := &userConn{aclSeq: [][]string{
		{"user app on nopass ~* +@all"},
		{"user app off nopass ~* +@all"},
	}}
	_, fin := applyUser(t, newUserModule(conn), "present", map[string]any{
		"addr":  "127.0.0.1:6379",
		"name":  "app",
		"perms": "~* +@all",
		"state": "off",
	})
	if fin == nil || fin.Failed {
		t.Fatalf("waited a successful apply, got %+v", fin)
	}
	line, _ := conn.call("ACL SETUSER")
	if !strings.HasPrefix(line, "ACL SETUSER app reset off ") {
		t.Errorf(`state "off" did not reach the vector: %s`, line)
	}
	if got := fin.GetOutput().GetFields()["state"].GetStringValue(); got != "off" {
		t.Errorf("Output.state = %q, want %q", got, "off")
	}
}

// TestApplyUserPresent_BooleanStateRefusedByApply — Validate is a separate RPC a
// runner need not call, so Apply refuses the coercion on its own.
func TestApplyUserPresent_BooleanStateRefusedByApply(t *testing.T) {
	conn := &userConn{}
	_, fin := applyUser(t, newUserModule(conn), "present", map[string]any{
		"addr":  "127.0.0.1:6379",
		"name":  "app",
		"perms": "~* +@all",
		"state": false,
	})
	if fin == nil || !fin.Failed {
		t.Fatalf("waited failed=true on a boolean state, got %+v", fin)
	}
	if conn.sent("ACL SETUSER") {
		t.Error("a refused state must not reach the instance")
	}
}

// TestApplyUserPresent_AlreadyMatchesNoOp — idempotency, and it does NOT persist:
// ACL SAVE on a no-op would rewrite a rendered aclfile in Redis's own byte order,
// which the next render reads as a change and answers with a restart, every run.
func TestApplyUserPresent_AlreadyMatchesNoOp(t *testing.T) {
	live := []string{"user app on #" + sha256Hex(userPass) + " ~app:* +@read"}
	conn := &userConn{aclSeq: [][]string{live, live}}
	_, fin := applyUser(t, newUserModule(conn), "present", map[string]any{
		"addr":          "127.0.0.1:6379",
		"name":          "app",
		"perms":         "~app:* +@read",
		"user_password": userPass,
	})
	if fin == nil || fin.Failed {
		t.Fatalf("waited a successful apply, got %+v", fin)
	}
	if fin.Changed {
		t.Error("waited changed=false when the live line already matches")
	}
	if conn.sent("ACL SAVE") {
		t.Error("a no-op must not rewrite the aclfile")
	}
}

// TestApplyUserPresent_NormalizedLineIsNotAChange — the reason `changed` is a diff
// of Redis's OWN renderings and not of the declaration against the live line: Redis
// reorders the rules and adds the `resetchannels` it implies, so comparing what was
// asked to what came back would report a change forever.
//
// The declaration here deliberately shares no byte order with the line it is
// compared against, and the second half of the test is what makes the first half
// falsifiable: the vector we SEND carries the declared tokens in the declared order,
// so an implementation that had echoed the live line back (and thus diffed equal for
// the wrong reason) fails here rather than passing quietly.
func TestApplyUserPresent_NormalizedLineIsNotAChange(t *testing.T) {
	normalized := []string{"user app on nopass sanitize-payload ~app:* resetchannels +@read"}
	conn := &userConn{aclSeq: [][]string{normalized, normalized}}
	_, fin := applyUser(t, newUserModule(conn), "present", map[string]any{
		"addr":  "127.0.0.1:6379",
		"name":  "app",
		"perms": "+@read ~app:*",
	})
	if fin == nil || fin.Failed {
		t.Fatalf("waited a successful apply, got %+v", fin)
	}
	if fin.Changed {
		t.Error("Redis's normalization of the rules must not read as a change")
	}
	sent, ok := conn.call("ACL SETUSER")
	if !ok {
		t.Fatal("no ACL SETUSER reached the instance — `changed` must come from a diff, not from a skipped write")
	}
	if !strings.HasSuffix(sent, "+@read ~app:*") {
		t.Errorf("the vector must carry the DECLARED perms in the declared order, got %q", sent)
	}
	if strings.Contains(sent, "sanitize-payload") || strings.Contains(sent, "resetchannels") {
		t.Errorf("Redis's own rendering was echoed back into the vector: %q", sent)
	}
}

// TestApplyUserPresent_PersistFalseKeepsChangeInMemory — the opt-out for an
// instance with no aclfile, where ACL SAVE cannot succeed.
func TestApplyUserPresent_PersistFalseKeepsChangeInMemory(t *testing.T) {
	conn := &userConn{aclSeq: [][]string{{}, {"user app on nopass ~* +@read"}}}
	_, fin := applyUser(t, newUserModule(conn), "present", map[string]any{
		"addr":    "127.0.0.1:6379",
		"name":    "app",
		"perms":   "~* +@read",
		"persist": false,
	})
	if fin == nil || fin.Failed || !fin.Changed {
		t.Fatalf("waited changed=true without failure, got %+v", fin)
	}
	if conn.sent("ACL SAVE") {
		t.Error("persist=false must not send ACL SAVE")
	}
}

// TestApplyUserPresent_FailedSetUserPersistsNothing — the NIM-624 shape. That
// ticket's defect was a half-written users.acl left on disk by a failed ACL LOAD,
// after which Redis would not start. SETUSER is atomic, so a failure leaves the
// user untouched — provided nothing persists afterwards.
func TestApplyUserPresent_FailedSetUserPersistsNothing(t *testing.T) {
	conn := &userConn{
		aclSeq:   [][]string{{"user app on nopass ~old:* +@read"}},
		failVerb: "ACL SETUSER",
		failErr:  errors.New("ERR Error in ACL SETUSER modifier '+@nosuch': Unknown command or category"),
	}
	stream, fin := applyUser(t, newUserModule(conn), "present", map[string]any{
		"addr":          "127.0.0.1:6379",
		"password":      secretPass,
		"name":          "app",
		"perms":         "+@nosuch",
		"user_password": userPass,
	})
	if fin == nil || !fin.Failed {
		t.Fatalf("waited failed=true, got %+v", fin)
	}
	if conn.sent("ACL SAVE") {
		t.Error("a failed SETUSER must persist nothing — this is the NIM-624 trap, mirrored")
	}
	assertEventsNoSecret(t, stream)
	if strings.Contains(fin.GetMessage(), userPass) {
		t.Error("the managed user's password leaked into the failure message")
	}
}

// TestApplyUserPresent_SaveFailureIsAFailure — a change that could not be persisted
// is not a success: it would be silently reverted by the next restart or ACL LOAD.
// The message names the likely cause and the way out.
func TestApplyUserPresent_SaveFailureIsAFailure(t *testing.T) {
	conn := &userConn{
		aclSeq:   [][]string{{}, {"user app on nopass ~* +@read"}},
		failVerb: "ACL SAVE",
		failErr:  errors.New("ERR This Redis instance is not configured to use an ACL file"),
	}
	_, fin := applyUser(t, newUserModule(conn), "present", map[string]any{
		"addr":  "127.0.0.1:6379",
		"name":  "app",
		"perms": "~* +@read",
	})
	if fin == nil || !fin.Failed {
		t.Fatalf("waited failed=true when the change could not be persisted, got %+v", fin)
	}
	if !strings.Contains(fin.GetMessage(), "persist: false") {
		t.Errorf("the failure does not name the way out: %q", fin.GetMessage())
	}
}

func TestApplyUserPresent_AclListErrorIsFailure(t *testing.T) {
	conn := &userConn{aclErr: errors.New("NOPERM this user has no permissions to run the 'acl' command")}
	stream, fin := applyUser(t, newUserModule(conn), "present", map[string]any{
		"addr":     "127.0.0.1:6379",
		"password": secretPass,
		"name":     "app",
		"perms":    "~* +@read",
	})
	if fin == nil || !fin.Failed {
		t.Fatalf("waited failed=true, got %+v", fin)
	}
	if conn.sent("ACL SETUSER") {
		t.Error("a run that cannot read the live ACL must not write one")
	}
	assertEventsNoSecret(t, stream)
}

func TestApplyUserPresent_ConnectFailureDoesNotLeak(t *testing.T) {
	m := &RedisModule{
		connect: func(_ context.Context, cfg connConfig) (redisConn, error) {
			return nil, errors.New("dial failed for AUTH " + cfg.password)
		},
	}
	stream := &applyStream{}
	_ = m.user().Apply(&pluginv1.ApplyRequest{
		State: "present",
		Params: mustStruct(t, map[string]any{
			"addr":          "127.0.0.1:6379",
			"password":      secretPass,
			"name":          "app",
			"perms":         "~* +@read",
			"user_password": userPass,
		}),
	}, stream)

	fin := stream.final()
	if fin == nil || !fin.Failed {
		t.Fatalf("waited failed=true, got %+v", fin)
	}
	assertEventsNoSecret(t, stream)
	if strings.Contains(fin.GetMessage(), userPass) {
		t.Error("the managed user's password leaked into the connect failure")
	}
}

// TestApplyUser_TLSParamsReachConnect — the object reads the same connect set as
// every other one dispatched through the shared path.
func TestApplyUser_TLSParamsReachConnect(t *testing.T) {
	const caPEM = "-----BEGIN CERTIFICATE-----\nFAKE\n-----END CERTIFICATE-----"
	conn := &userConn{}
	_, _ = applyUser(t, newUserModule(conn), "present", map[string]any{
		"addr":   "127.0.0.1:6379",
		"name":   "app",
		"perms":  "~* +@read",
		"tls":    true,
		"tls_ca": caPEM,
	})
	if !conn.cfg.tls.enabled {
		t.Error("tls=true did not reach the connection")
	}
	if conn.cfg.tls.caPEM != caPEM {
		t.Errorf("tls_ca did not reach the connection: %q", conn.cfg.tls.caPEM)
	}
}

// --- Apply absent ---

func TestApplyUserAbsent_RemovesAndPersists(t *testing.T) {
	conn := &userConn{aclSeq: [][]string{{"user default on nopass ~* &* +@all", "user app on nopass ~* +@read"}}}
	stream, fin := applyUser(t, newUserModule(conn), "absent", map[string]any{
		"addr":     "127.0.0.1:6379",
		"password": secretPass,
		"name":     "app",
	})
	if fin == nil || fin.Failed || !fin.Changed {
		t.Fatalf("waited changed=true without failure, got %+v", fin)
	}
	if line, ok := conn.call("ACL DELUSER"); !ok || line != "ACL DELUSER app" {
		t.Errorf("ACL DELUSER app was not sent; calls: %v", conn.calls)
	}
	if !conn.sent("ACL SAVE") {
		t.Error("a removal that happened must be persisted, or ACL LOAD brings the user back")
	}
	assertEventsNoSecret(t, stream)
}

// TestApplyUserAbsent_UnknownUserNoOp — idempotent, and it sends no command at all:
// DELUSER would answer 0 harmlessly, but a probe-skip keeps `changed` truthful and
// leaves the aclfile alone.
func TestApplyUserAbsent_UnknownUserNoOp(t *testing.T) {
	conn := &userConn{aclSeq: [][]string{{"user default on nopass ~* &* +@all"}}}
	_, fin := applyUser(t, newUserModule(conn), "absent", map[string]any{
		"addr": "127.0.0.1:6379",
		"name": "app",
	})
	if fin == nil || fin.Failed {
		t.Fatalf("waited a successful apply, got %+v", fin)
	}
	if fin.Changed {
		t.Error("waited changed=false on a user the instance does not have")
	}
	if conn.sent("ACL DELUSER") {
		t.Error("a no-op must send no DELUSER")
	}
	if conn.sent("ACL SAVE") {
		t.Error("a no-op must not rewrite the aclfile")
	}
}

// TestApplyUserAbsent_SubstringNameIsNotAMatch — the line lookup matches the
// username field, not the line's text: removing `app` must not be triggered by an
// `application` the instance holds instead.
func TestApplyUserAbsent_SubstringNameIsNotAMatch(t *testing.T) {
	conn := &userConn{aclSeq: [][]string{{"user application on nopass ~* +@read"}}}
	_, fin := applyUser(t, newUserModule(conn), "absent", map[string]any{
		"addr": "127.0.0.1:6379",
		"name": "app",
	})
	if fin == nil || fin.Failed {
		t.Fatalf("waited a successful apply, got %+v", fin)
	}
	if fin.Changed || conn.sent("ACL DELUSER") {
		t.Error("a different user whose name contains this one is not this user")
	}
}

func TestApplyUserAbsent_DelUserErrorIsFailure(t *testing.T) {
	conn := &userConn{
		aclSeq:   [][]string{{"user app on nopass ~* +@read"}},
		failVerb: "ACL DELUSER",
		failErr:  errors.New("ERR unknown command"),
	}
	stream, fin := applyUser(t, newUserModule(conn), "absent", map[string]any{
		"addr":     "127.0.0.1:6379",
		"password": secretPass,
		"name":     "app",
	})
	if fin == nil || !fin.Failed {
		t.Fatalf("waited failed=true, got %+v", fin)
	}
	if conn.sent("ACL SAVE") {
		t.Error("a failed DELUSER must persist nothing")
	}
	assertEventsNoSecret(t, stream)
}

func TestApplyUserAbsent_ConnectFailureDoesNotLeak(t *testing.T) {
	m := &RedisModule{
		connect: func(_ context.Context, cfg connConfig) (redisConn, error) {
			return nil, errors.New("dial failed for AUTH " + cfg.password)
		},
	}
	stream := &applyStream{}
	_ = m.user().Apply(&pluginv1.ApplyRequest{
		State:  "absent",
		Params: mustStruct(t, map[string]any{"addr": "127.0.0.1:6379", "password": secretPass, "name": "app"}),
	}, stream)

	if fin := stream.final(); fin == nil || !fin.Failed {
		t.Fatalf("waited failed=true, got %+v", fin)
	}
	assertEventsNoSecret(t, stream)
}

// --- guards: Apply enforces the object's rules, not just Validate ---
//
// These exist because the first draft of this object re-checked `state` in Apply and
// nothing else, on the reasoning that Validate covers the rest. It does not cover it
// for anyone: soul's applyrunner calls Apply and never Validate, and keeper's static
// check is presence-only and returns early on a `${…}`-rendered cell. So every rule
// the docs present as a property of the object has to hold HERE or it holds nowhere,
// and each test below drives the real object.Apply against a fake that would happily
// accept the command the rule forbids.

// TestApplyUserPresent_EmptyPermsRefusedByApply — the severe one. `perms` that
// rendered empty (an unset var behind `${ … }`) passes lint, and the rule vector
// opens with `reset`, so reaching the instance would strip every permission the live
// user holds — and then, since that IS a change, ACL SAVE it to the aclfile. The
// account keeps authenticating and answers NOPERM to everything.
func TestApplyUserPresent_EmptyPermsRefusedByApply(t *testing.T) {
	conn := &userConn{aclSeq: [][]string{{"user app on #" + sha256Hex(userPass) + " ~app:* +@all"}}}
	_, fin := applyUser(t, newUserModule(conn), "present", map[string]any{
		"addr":  "127.0.0.1:6379",
		"name":  "app",
		"perms": "",
	})
	if fin == nil || !fin.Failed {
		t.Fatalf("waited failed=true on empty perms, got %+v", fin)
	}
	if conn.sent("ACL SETUSER") {
		t.Error("an empty perms must not reach the instance — it would reset the user to no permissions")
	}
	if conn.sent("ACL SAVE") {
		t.Error("nothing may be persisted after a refused apply")
	}
}

// TestApplyUserPresent_EmptyNameRefusedByApply — real Redis ACCEPTS an empty
// username: `ACL SETUSER "" …` creates a nameless entry that ACL SAVE then writes
// into the aclfile, which no operator can address afterwards and `user.absent`
// cannot remove without reproducing the same empty name.
func TestApplyUserPresent_EmptyNameRefusedByApply(t *testing.T) {
	conn := &userConn{}
	_, fin := applyUser(t, newUserModule(conn), "present", map[string]any{
		"addr":  "127.0.0.1:6379",
		"name":  "",
		"perms": "~app:* +@read",
	})
	if fin == nil || !fin.Failed {
		t.Fatalf("waited failed=true on an empty name, got %+v", fin)
	}
	if conn.sent("ACL SETUSER") {
		t.Error("an empty name must not reach the instance")
	}
}

// TestApplyUserPresent_CredentialInPermsRefused — `perms` is not a secret param, so
// a credential inside it is plaintext in the rendered task, the logs, the traces,
// the UI and git. The refusal is in Apply because that is the only place that runs.
func TestApplyUserPresent_CredentialInPermsRefused(t *testing.T) {
	for _, tok := range []string{">hunter2", "<hunter2", "#" + sha256Hex("hunter2"), "!" + sha256Hex("hunter2"), "reset"} {
		t.Run(tok, func(t *testing.T) {
			conn := &userConn{}
			_, fin := applyUser(t, newUserModule(conn), "present", map[string]any{
				"addr":  "127.0.0.1:6379",
				"name":  "app",
				"perms": "~app:* +@read " + tok,
			})
			if fin == nil || !fin.Failed {
				t.Fatalf("waited failed=true on %q in perms, got %+v", tok, fin)
			}
			if conn.sent("ACL SETUSER") {
				t.Errorf("%q must not reach the instance", tok)
			}
			// The refusal itself must not leak what it refused: ApplyEvent.Message is
			// the log/trace/UI surface this rule exists to keep the credential out of.
			if strings.Contains(fin.Message, "hunter2") {
				t.Errorf("the refusal echoed the credential back: %q", fin.Message)
			}
		})
	}
}

// TestApplyUserPresent_PermsKeywordsAreCaseInsensitive — Redis's ACL parser matches
// keywords with strcasecmp, so a refusal that compares exactly is a one-keystroke
// bypass. The first version of this guard did, and `RESET` walked through it: on a
// live instance the declared perms, state and password were all discarded and the
// step reported success.
func TestApplyUserPresent_PermsKeywordsAreCaseInsensitive(t *testing.T) {
	for _, tok := range []string{"RESET", "Reset", "ON", "Off"} {
		t.Run(tok, func(t *testing.T) {
			conn := &userConn{}
			_, fin := applyUser(t, newUserModule(conn), "present", map[string]any{
				"addr":  "127.0.0.1:6379",
				"name":  "app",
				"perms": "~app:* +@read " + tok,
			})
			if fin == nil || !fin.Failed {
				t.Fatalf("waited failed=true on %q in perms, got %+v", tok, fin)
			}
			if conn.sent("ACL SETUSER") {
				t.Errorf("%q must not reach the instance", tok)
			}
		})
	}
}

// TestApplyUserPresent_NulInPermsRefused — case folding was only half of matching
// Redis. `strcasecmp` stops at the first NUL and `strings.EqualFold` does not, so
// "reset\x00x" is `reset` to the server and not-`reset` to the guard — and the YAML
// parser this tree uses decodes an embedded NUL into a Go string, so it is reachable
// from a scenario. Live, each of these restored the exact harm the guard names, with
// the step reporting success: the declared perms, state and password discarded; an
// account left authenticating with any string; Output.state describing an instance
// that was off.
func TestApplyUserPresent_NulInPermsRefused(t *testing.T) {
	for _, name := range []string{"reset", "nopass", "resetpass", "off", "on"} {
		t.Run(name, func(t *testing.T) {
			conn := &userConn{}
			_, fin := applyUser(t, newUserModule(conn), "present", map[string]any{
				"addr":  "127.0.0.1:6379",
				"name":  "app",
				"perms": "~app:* +@read " + name + "\x00x",
			})
			if fin == nil || !fin.Failed {
				t.Fatalf("waited failed=true on %q with a NUL tail, got %+v", name, fin)
			}
			if conn.sent("ACL SETUSER") {
				t.Errorf("%q with a NUL tail must not reach the instance", name)
			}
		})
	}
}

// TestApplyUserPresent_NulInNameRefused — the same truncation on the subject of the
// step. Redis refuses a NUL in a username itself, but a safety borrowed from the
// server is one this module is not entitled to claim in its own docs.
func TestApplyUserPresent_NulInNameRefused(t *testing.T) {
	conn := &userConn{}
	_, fin := applyUser(t, newUserModule(conn), "present", map[string]any{
		"addr":  "127.0.0.1:6379",
		"name":  "app\x00admin",
		"perms": "~app:* +@read",
	})
	if fin == nil || !fin.Failed {
		t.Fatalf("waited failed=true on a NUL in the name, got %+v", fin)
	}
	if conn.sent("ACL SETUSER") {
		t.Error("a name carrying a NUL must not reach the instance")
	}
}

// TestApplyUserPresent_NoAclfileRefusedBeforeMutating — the half-apply. With
// `persist` (default true) against an instance that has no aclfile, ACL SAVE cannot
// succeed; letting the SETUSER go first left a live user behind a step that reported
// failure, and the re-run then said "no-op" because the INSTANCE matched — so no run
// ever reported the change and no handler keyed on it fired. ApplyEvent cannot carry
// changed+failed (applyrunner tests GetFailed first), so the only honest fix is to
// refuse before touching anything.
func TestApplyUserPresent_NoAclfileRefusedBeforeMutating(t *testing.T) {
	conn := &userConn{noAclfile: true}
	_, fin := applyUser(t, newUserModule(conn), "present", map[string]any{
		"addr":  "127.0.0.1:6379",
		"name":  "app",
		"perms": "~app:* +@read",
	})
	if fin == nil || !fin.Failed {
		t.Fatalf("waited failed=true on an instance with no aclfile, got %+v", fin)
	}
	if conn.sent("ACL SETUSER") {
		t.Error("the ACL must not be mutated when the persistence it was promised cannot happen")
	}
	if conn.sent("ACL SAVE") {
		t.Error("ACL SAVE must not even be attempted")
	}
}

// TestApplyUserAbsent_NoAclfileRefusedBeforeMutating — the same on the removal side,
// where the half-apply is worse: the user is gone and the retry says "already
// absent".
func TestApplyUserAbsent_NoAclfileRefusedBeforeMutating(t *testing.T) {
	conn := &userConn{noAclfile: true, aclSeq: [][]string{{"user app on nopass ~app:* +@read"}}}
	_, fin := applyUser(t, newUserModule(conn), "absent", map[string]any{
		"addr": "127.0.0.1:6379",
		"name": "app",
	})
	if fin == nil || !fin.Failed {
		t.Fatalf("waited failed=true on an instance with no aclfile, got %+v", fin)
	}
	if conn.sent("ACL DELUSER") {
		t.Error("the user must not be removed when the removal cannot be persisted")
	}
}

// TestApplyUserPresent_ConfigGetDeniedStillApplies — the pre-flight must never fail
// the apply on its own account. CONFIG is @admin/@dangerous while ACL SETUSER and ACL
// SAVE are not, so `+acl +ping +select` without `+config|get` is a connection that
// can do the whole job — and the first version of this refused all of it with NOPERM,
// work that very connection was able to perform. An unreadable config means "cannot
// tell", not "no".
func TestApplyUserPresent_ConfigGetDeniedStillApplies(t *testing.T) {
	conn := &userConn{
		configErr: errors.New("NOPERM User aclop has no permissions to run the 'config|get' command"),
		aclSeq:    [][]string{{}, {"user app on nopass ~app:* +@read"}},
	}
	_, fin := applyUser(t, newUserModule(conn), "present", map[string]any{
		"addr":  "127.0.0.1:6379",
		"name":  "app",
		"perms": "~app:* +@read",
	})
	if fin == nil || fin.Failed {
		t.Fatalf("an unreadable config must not fail the apply, got %+v", fin)
	}
	if !fin.Changed {
		t.Error("waited changed=true")
	}
	if !conn.sent("ACL SETUSER") || !conn.sent("ACL SAVE") {
		t.Error("the apply must proceed exactly as it did before the pre-flight existed")
	}
}

// TestApplyUserAbsent_ConfigGetDeniedStillRemoves — the same on the removal side.
func TestApplyUserAbsent_ConfigGetDeniedStillRemoves(t *testing.T) {
	conn := &userConn{
		configErr: errors.New("ERR unknown command 'CONFIG'"),
		aclSeq:    [][]string{{"user app on nopass ~app:* +@read"}},
	}
	_, fin := applyUser(t, newUserModule(conn), "absent", map[string]any{
		"addr": "127.0.0.1:6379",
		"name": "app",
	})
	if fin == nil || fin.Failed || !fin.Changed {
		t.Fatalf("waited changed=true without failure, got %+v", fin)
	}
	if !conn.sent("ACL DELUSER") {
		t.Error("the removal must proceed")
	}
}

// TestApplyUserAbsent_NoAclfileNoOpIsNotAFailure — the pre-flight runs AFTER the
// probe on this side. With no user to remove there is nothing to persist, so checking
// first turned an idempotent cleanup into a hard red on an instance with no aclfile —
// a failure for a run that had no work to do.
func TestApplyUserAbsent_NoAclfileNoOpIsNotAFailure(t *testing.T) {
	conn := &userConn{noAclfile: true, aclSeq: [][]string{{"user other on nopass ~* +@all"}}}
	_, fin := applyUser(t, newUserModule(conn), "absent", map[string]any{
		"addr": "127.0.0.1:6379",
		"name": "ghost",
	})
	if fin == nil || fin.Failed {
		t.Fatalf("a no-op removal must not fail for want of a place to persist, got %+v", fin)
	}
	if fin.Changed {
		t.Error("waited changed=false")
	}
	if conn.sent("ACL DELUSER") || conn.sent("ACL SAVE") {
		t.Error("a no-op must send no mutating command")
	}
}

// TestApplyUserPresent_CredentialInSelectorRefused — Redis merges a selector across
// arguments, so `(>secret +get)` splits into `(>secret` and `+get)` and the
// credential sits at byte 1. A test of byte 0 missed it, Redis rejected the rule and
// quoted the WHOLE modifier back — putting the plaintext into ApplyEvent.Message,
// the exact surface the refusal exists to protect, where redactError cannot reach it.
// The mirrored ordering `(+get >secret)` was caught all along: one rule, two verdicts.
func TestApplyUserPresent_CredentialInSelectorRefused(t *testing.T) {
	// `((>hunter2 +get)` is the row that pins TrimLeft rather than TrimPrefix: Redis
	// strips one paren, so this is still a real modifier whose syntax error quotes the
	// plaintext back. A one-paren strip passed the rest of this table.
	for _, perms := range []string{"(>hunter2 +get)", "(+get >hunter2)", "(#" + sha256Hex("hunter2") + " +get)", "((>hunter2 +get)"} {
		t.Run(perms, func(t *testing.T) {
			conn := &userConn{}
			_, fin := applyUser(t, newUserModule(conn), "present", map[string]any{
				"addr":  "127.0.0.1:6379",
				"name":  "app",
				"perms": "~app:* " + perms,
			})
			if fin == nil || !fin.Failed {
				t.Fatalf("waited failed=true on a credential in a selector, got %+v", fin)
			}
			if conn.sent("ACL SETUSER") {
				t.Error("the credential must not reach Redis, which would quote it back in its error")
			}
			if strings.Contains(fin.Message, "hunter2") {
				t.Errorf("the refusal echoed the credential: %q", fin.Message)
			}
			// The message must name the CREDENTIAL prefix, not the paren in front of
			// it — pointing the author at "(" tells them nothing about what is wrong.
			if strings.Contains(fin.Message, `opens with "("`) {
				t.Errorf("the refusal named the paren instead of the credential prefix: %q", fin.Message)
			}
		})
	}
}

// TestApplyUserPresent_NoAclfileAllowedWhenPersistFalse — the pre-flight must not
// break the instance-with-no-aclfile case the opt-out exists for.
func TestApplyUserPresent_NoAclfileAllowedWhenPersistFalse(t *testing.T) {
	conn := &userConn{noAclfile: true, aclSeq: [][]string{{}, {"user app on nopass ~app:* +@read"}}}
	_, fin := applyUser(t, newUserModule(conn), "present", map[string]any{
		"addr":    "127.0.0.1:6379",
		"name":    "app",
		"perms":   "~app:* +@read",
		"persist": false,
	})
	if fin == nil || fin.Failed || !fin.Changed {
		t.Fatalf("waited changed=true without failure, got %+v", fin)
	}
	if !conn.sent("ACL SETUSER") {
		t.Error("persist: false must still apply the change")
	}
}

// TestSaveACL_FailureNamesTheLandedChange — the message is the only channel left:
// ApplyEvent cannot report changed together with failed, so an operator reading this
// has to be told from the text that the instance was mutated and the file was not.
func TestSaveACL_FailureNamesTheLandedChange(t *testing.T) {
	conn := &userConn{failVerb: "ACL SAVE", failErr: errors.New("ERR write error")}
	err := saveACL(context.Background(), conn, true)
	if err == nil {
		t.Fatal("waited an error from a failing ACL SAVE")
	}
	for _, want := range []string{"IS live", "NOT in the aclfile", "no-op"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the failure must say the change landed; %q is missing from %q", want, err)
		}
	}
}

// TestApplyUserPresent_StateKeywordInPermsRefused — `on`/`off` in perms lands AFTER
// the state the vector already set and overrides it, and Output.state then reports
// the declaration rather than the instance: a scenario reading register.<x>.state
// would be told the account is disabled while it authenticates.
func TestApplyUserPresent_StateKeywordInPermsRefused(t *testing.T) {
	conn := &userConn{}
	_, fin := applyUser(t, newUserModule(conn), "present", map[string]any{
		"addr":  "127.0.0.1:6379",
		"name":  "app",
		"perms": "~app:* +@read on",
		"state": "off",
	})
	if fin == nil || !fin.Failed {
		t.Fatalf("waited failed=true on a state keyword in perms, got %+v", fin)
	}
	if conn.sent("ACL SETUSER") {
		t.Error("a state keyword in perms must not reach the instance")
	}
}

// TestApplyUserPresent_NopassContradictingDeclaredPasswordRefused — `nopass` alone is
// legal, but together with a declared user_password the two say opposite things and
// Redis takes the last directive, which is the perms one. Before this refusal the
// step reported success and left an account that authenticated with ANY string.
func TestApplyUserPresent_NopassContradictingDeclaredPasswordRefused(t *testing.T) {
	for _, tok := range []string{"nopass", "NOPASS", "resetpass"} {
		t.Run(tok, func(t *testing.T) {
			conn := &userConn{}
			_, fin := applyUser(t, newUserModule(conn), "present", map[string]any{
				"addr":          "127.0.0.1:6379",
				"name":          "app",
				"perms":         "~app:* +@read " + tok,
				"user_password": userPass,
			})
			if fin == nil || !fin.Failed {
				t.Fatalf("waited failed=true on %q beside a declared password, got %+v", tok, fin)
			}
			if conn.sent("ACL SETUSER") {
				t.Errorf("the contradiction must not reach the instance")
			}
			if strings.Contains(fin.Message, userPass) {
				t.Errorf("the refusal echoed the declared password: %q", fin.Message)
			}
		})
	}
}

// TestApplyUserAbsent_NonBooleanPersistRefused — the twin of the present-side check.
// It is here because a mutation that deleted the absent-side rule left the whole
// suite green: the rule was right and nothing would have caught its removal.
func TestApplyUserAbsent_NonBooleanPersistRefused(t *testing.T) {
	conn := &userConn{aclSeq: [][]string{{"user app on nopass ~app:* +@read"}}}
	_, fin := applyUser(t, newUserModule(conn), "absent", map[string]any{
		"addr":    "127.0.0.1:6379",
		"name":    "app",
		"persist": "yes",
	})
	if fin == nil || !fin.Failed {
		t.Fatalf("waited failed=true on a non-boolean persist, got %+v", fin)
	}
	if conn.sent("ACL DELUSER") {
		t.Error("a refused persist must not reach the instance")
	}
}

// TestApplyUserPresent_NopassInPermsIsAllowed — the boundary is a credential VALUE,
// not the topic of credentials. `nopass` carries nothing secret and is the honest
// way to declare a user that holds no password, so it passes — and it overrides the
// carry-over on purpose, which is the author saying so rather than an omitted
// user_password doing it by accident.
func TestApplyUserPresent_NopassInPermsIsAllowed(t *testing.T) {
	conn := &userConn{aclSeq: [][]string{
		{"user app on #" + sha256Hex(userPass) + " ~app:* +@read"},
		{"user app on nopass ~app:* +@read"},
	}}
	_, fin := applyUser(t, newUserModule(conn), "present", map[string]any{
		"addr":  "127.0.0.1:6379",
		"name":  "app",
		"perms": "~app:* +@read nopass",
	})
	if fin == nil || fin.Failed {
		t.Fatalf("waited a successful apply, got %+v", fin)
	}
	sent, ok := conn.call("ACL SETUSER")
	if !ok {
		t.Fatal("no ACL SETUSER reached the instance")
	}
	if !strings.HasSuffix(sent, "nopass") {
		t.Errorf("nopass must reach the instance last, so it overrides the carried credential: %q", sent)
	}
}

// TestApplyUserPresent_NonBooleanPersistRefused — `persist` decides whether this
// touches the aclfile, and boolOrDefault would have fallen back to true (write it)
// on a value it cannot read. Refused instead, the same call the `state` coercion got.
func TestApplyUserPresent_NonBooleanPersistRefused(t *testing.T) {
	conn := &userConn{}
	_, fin := applyUser(t, newUserModule(conn), "present", map[string]any{
		"addr":    "127.0.0.1:6379",
		"name":    "app",
		"perms":   "~app:* +@read",
		"persist": "yes",
	})
	if fin == nil || !fin.Failed {
		t.Fatalf("waited failed=true on a non-boolean persist, got %+v", fin)
	}
	if conn.sent("ACL SETUSER") {
		t.Error("a refused persist must not reach the instance")
	}
}

// TestApplyUserAbsent_DefaultUserRefusedByApply — Redis refuses ACL DELUSER default
// on its own, so the outcome was already right; what was wrong is that the docs
// attribute the refusal to this module and Apply did not make it. A safety borrowed
// from the server is one the next Redis release can change.
func TestApplyUserAbsent_DefaultUserRefusedByApply(t *testing.T) {
	conn := &userConn{aclSeq: [][]string{{"user default on nopass ~* &* +@all"}}}
	_, fin := applyUser(t, newUserModule(conn), "absent", map[string]any{
		"addr": "127.0.0.1:6379",
		"name": "default",
	})
	if fin == nil || !fin.Failed {
		t.Fatalf(`waited failed=true on name "default", got %+v`, fin)
	}
	if conn.sent("ACL DELUSER") {
		t.Error("the built-in default user must not be sent to DELUSER at all")
	}
}

// TestApplyUserAbsent_DelUserZeroIsNoOp — DELUSER answers with a count, and between
// the probe that found the user and the command that removes it another actor may
// have got there first. Reporting that as a change would run ACL SAVE, rewriting the
// aclfile in Redis's byte order for a removal this step did not perform — the exact
// churn the unknown-user probe-skip exists to avoid.
func TestApplyUserAbsent_DelUserZeroIsNoOp(t *testing.T) {
	conn := &userConn{
		aclSeq:  [][]string{{"user app on nopass ~app:* +@read"}},
		replies: map[string]string{"ACL DELUSER": "0"},
	}
	_, fin := applyUser(t, newUserModule(conn), "absent", map[string]any{
		"addr": "127.0.0.1:6379",
		"name": "app",
	})
	if fin == nil || fin.Failed {
		t.Fatalf("waited a successful apply, got %+v", fin)
	}
	if fin.Changed {
		t.Error("a DELUSER that removed nothing is not a change")
	}
	if conn.sent("ACL SAVE") {
		t.Error("a no-op must not rewrite the aclfile")
	}
}

// --- unit: the pieces the applies are built from ---

// TestAclPasswordHash_MatchesTheAclfileDigest — the hash this object sends and the
// hash users.acl.tmpl writes are the same function of the same input, which is what
// lets a rendered user and a SETUSER user be one user to Redis. The expected value
// is sha256("") and sha256("password") as any other implementation would compute
// them, not as this one does.
func TestAclPasswordHash_MatchesTheAclfileDigest(t *testing.T) {
	cases := map[string]string{
		"":         "#e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
		"password": "#5e884898da28047151d0e56f8dc6292773603d0d6aabbdd62a11ef721d1542d8",
	}
	for in, want := range cases {
		if got := aclPasswordHash(in); got != want {
			t.Errorf("aclPasswordHash(%q) = %s, want %s", in, got, want)
		}
	}
}

func TestAclUserLine_MatchesTheNameField(t *testing.T) {
	lines := []string{
		"user default on nopass ~* &* +@all",
		"user application on nopass ~* +@read",
		"user app off #abc ~app:* +@read",
	}
	if got := aclUserLine(lines, "app"); got != lines[2] {
		t.Errorf("aclUserLine picked %q", got)
	}
	if got := aclUserLine(lines, "nobody"); got != "" {
		t.Errorf("aclUserLine invented a line for an absent user: %q", got)
	}
	// "nopass" is a rule, not a username: a lookup must not match a rule token.
	if got := aclUserLine(lines, "nopass"); got != "" {
		t.Errorf("a rule token was read as a username: %q", got)
	}
}

func TestCarryPasswords_TakesCredentialsOnly(t *testing.T) {
	got := carryPasswords("user app on #aaa #bbb nopass ~app:* +@read")
	want := []string{"#aaa", "#bbb", "nopass"}
	if len(got) != len(want) {
		t.Fatalf("carryPasswords = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("carryPasswords = %v, want %v", got, want)
		}
	}
	if got := carryPasswords(""); got != nil {
		t.Errorf("carryPasswords(\"\") = %v, want nil", got)
	}
}

func TestUserState_DefaultsToOn(t *testing.T) {
	got, err := userState(nil)
	if err != nil || got != "on" {
		t.Fatalf(`userState(nil) = %q, %v; want "on", nil`, got, err)
	}
}
