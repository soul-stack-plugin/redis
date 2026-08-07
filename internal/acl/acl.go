// Package acl is the `acl` module of the Redis bundle: idempotent Redis ACL user
// management, SETUSER / DELUSER per the requested state. The current state is
// probed with `ACL GETUSER`.
//
// [Module] describes the contract, this file implements it. The address the
// operator writes — `redis.acl.present` — takes its first level from the alias
// the source was registered under, not from anything declared here.
//
// ★ The rule diff goes through Redis, not through string matching. What
// `ACL GETUSER` returns is Redis' own rendering of the rules, and that rendering
// depends on the order they were applied in: `+@read +SET -DEBUG` comes back as
// `-@all +@read +set -debug`, while the same three rules in another order come
// back as `-@all -debug +set +@read`. Redis 6.2 renders `keys` and `channels` as
// arrays without their prefixes where 7.x renders strings with them, and
// minimises the command set differently again. So instead of guessing at a
// canonical form, this module pushes the desired rules through a disabled
// scratch user and compares the two renderings — both produced by the same
// server, so they are comparable by construction.
//
// ★ The password is compared, not re-applied. Redis stores an unsalted SHA-256
// hex digest, so the desired password can be checked against it directly and an
// unchanged password reports no change. Note that `>pass` ADDS a password rather
// than replacing the set, which is why every write starts with `reset`: without
// it, a rotated password leaves the old one valid indefinitely.
//
// Backend is github.com/redis/go-redis/v9. Credentials arrive already resolved
// by keeper-render from a vault-ref; the plugin does not resolve Vault itself.
package acl

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"

	pluginv1 "github.com/souls-guild/soul-stack/proto/plugin/gen/go/v1"
	"github.com/souls-guild/soul-stack/sdk/module"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

// ACL is the running module — the value [Module] points its Impl at, and the
// value whose Apply the host calls.
type ACL struct {
	module.BaseModule

	// connect is the injection point for L0 tests. nil → a real redis client.
	connect func(ctx context.Context, c connParams) (aclClient, error)
}

// aclClient is the narrow surface of a Redis connection this module needs.
type aclClient interface {
	GetUser(ctx context.Context, username string) (aclUser, bool, error)
	SetUser(ctx context.Context, username string, rules []string) error
	DelUser(ctx context.Context, username string) error
	Save(ctx context.Context) error
	Close() error
}

// aclUser is the snapshot read from `ACL GETUSER`. commands/keys/channels are
// kept as Redis rendered them: they are only ever compared against another
// snapshot from the same server, never parsed for meaning.
type aclUser struct {
	enabled   bool
	nopass    bool
	passwords []string
	commands  string
	keys      string
	channels  string
	selectors []string
}

// userParams is the desired state.
type userParams struct {
	username     string
	enabled      bool
	password     string
	passwordHash string
	nopass       bool
	// keepPassword is set when the caller said nothing about the credential,
	// which means "leave whatever is there alone".
	keepPassword bool
	rules        []string
	persist      bool

	allowSelfMod bool
	allowDefault bool
}

const defaultUser = "default"

// probePrefix names the scratch user used to normalise the desired rules. It is
// created disabled and passwordless, so it cannot be authenticated as while it
// briefly exists.
const probePrefix = "__soulstack-probe-"

// Apply applies the requested state to the resource.
func (m *ACL) Apply(req *pluginv1.ApplyRequest, stream grpc.ServerStreamingServer[pluginv1.ApplyEvent]) error {
	ctx := stream.Context()

	conn, err := parseConnParams(req.GetParams())
	if err != nil {
		return sendFailure(stream, err.Error())
	}
	p, err := parseUserParams(req.GetParams(), req.GetState())
	if err != nil {
		return sendFailure(stream, err.Error())
	}
	if err := checkLockout(p, conn.username, req.GetState()); err != nil {
		return sendFailure(stream, err.Error())
	}

	client, err := m.open(ctx, conn)
	if err != nil {
		return sendFailure(stream, fmt.Sprintf("connect: %v", err))
	}
	defer func() { _ = client.Close() }()

	cur, exists, err := client.GetUser(ctx, p.username)
	if err != nil {
		return sendFailure(stream, fmt.Sprintf("probe: %v", err))
	}

	switch req.GetState() {
	case "present":
		return applyPresent(ctx, stream, client, p, cur, exists)
	case "absent":
		return applyAbsent(ctx, stream, client, p, exists)
	default:
		return sendFailure(stream, fmt.Sprintf("unknown state %q (expected present|absent)", req.GetState()))
	}
}

// checkLockout refuses the two writes that take away the credentials the next
// run depends on. Redis prevents neither: deleting the user you authenticated
// as succeeds, and so does stripping your own permissions.
func checkLockout(p userParams, connectedAs, state string) error {
	if p.username == defaultUser {
		if state == "absent" {
			return errors.New("refusing to delete the built-in `default` user: Redis does not allow it, and an attempt only fails half way through the run")
		}
		if !p.allowDefault {
			return errors.New("refusing to manage the `default` user: disabling it locks out every unauthenticated client at once, and with persist on that survives a restart — set allow_default_user: true to confirm")
		}
	}
	if connectedAs == "" || connectedAs != p.username {
		return nil
	}
	if state == "absent" {
		return fmt.Errorf("refusing to delete user %q: it is the user this connection authenticated as, and deleting it would lock the next run out", p.username)
	}
	if !p.allowSelfMod {
		return fmt.Errorf("refusing to manage user %q: it is the user this connection authenticated as, and the write starts with `reset` — a narrower rule set would take this connection's own permissions away — set allow_self_modification: true to confirm", p.username)
	}
	return nil
}

func applyPresent(ctx context.Context, stream grpc.ServerStreamingServer[pluginv1.ApplyEvent], client aclClient, p userParams, cur aclUser, exists bool) error {
	// Normalising the desired rules doubles as validating them: a malformed rule
	// fails against the scratch user, before the managed user is touched.
	want, err := normalizeRules(ctx, client, p)
	if err != nil {
		return sendFailure(stream, err.Error())
	}

	if !exists {
		// "Leave the credential alone" has nothing to leave alone on a user that
		// does not exist yet, and an enabled user without one can never
		// authenticate — it would look configured and not be.
		if p.keepPassword && p.enabled {
			return sendFailure(stream, fmt.Sprintf(
				"user %s does not exist yet, so there is no password to keep: set password, password_hash or nopass (or enabled: false)", p.username))
		}
		if err := client.SetUser(ctx, p.username, desiredTokens(p, cur)); err != nil {
			return sendFailure(stream, fmt.Sprintf("ACL SETUSER: %v", err))
		}
		persisted, err := persist(ctx, client, p)
		if err != nil {
			return sendFailure(stream, err.Error())
		}
		return sendOutcome(stream, true, fmt.Sprintf("ACL SETUSER %s (created)", p.username), map[string]any{
			"action":    "created",
			"username":  p.username,
			"changes":   "created",
			"persisted": persisted,
		})
	}

	changes := diff(p, cur, want)
	if len(changes) == 0 {
		return sendOutcome(stream, false, fmt.Sprintf("USER %s already in the requested state", p.username), map[string]any{
			"action":    "noop",
			"username":  p.username,
			"changes":   "",
			"persisted": false,
		})
	}

	if err := client.SetUser(ctx, p.username, desiredTokens(p, cur)); err != nil {
		return sendFailure(stream, fmt.Sprintf("ACL SETUSER: %v", err))
	}
	persisted, err := persist(ctx, client, p)
	if err != nil {
		return sendFailure(stream, err.Error())
	}
	summary := strings.Join(changes, ", ")
	return sendOutcome(stream, true, fmt.Sprintf("ACL SETUSER %s (%s)", p.username, summary), map[string]any{
		"action":    "altered",
		"username":  p.username,
		"changes":   summary,
		"persisted": persisted,
	})
}

func applyAbsent(ctx context.Context, stream grpc.ServerStreamingServer[pluginv1.ApplyEvent], client aclClient, p userParams, exists bool) error {
	if !exists {
		return sendOutcome(stream, false, fmt.Sprintf("USER %s already absent", p.username), map[string]any{
			"action":    "noop",
			"username":  p.username,
			"persisted": false,
		})
	}
	if err := client.DelUser(ctx, p.username); err != nil {
		return sendFailure(stream, fmt.Sprintf("ACL DELUSER: %v", err))
	}
	persisted, err := persist(ctx, client, p)
	if err != nil {
		return sendFailure(stream, err.Error())
	}
	return sendOutcome(stream, true, fmt.Sprintf("ACL DELUSER %s", p.username), map[string]any{
		"action":    "dropped",
		"username":  p.username,
		"persisted": persisted,
	})
}

// persist runs ACL SAVE when asked. Without it the change lives in memory only
// and is lost on restart; with it, the whole aclfile is rewritten — including
// users this module knows nothing about.
func persist(ctx context.Context, client aclClient, p userParams) (bool, error) {
	if !p.persist {
		return false, nil
	}
	if err := client.Save(ctx); err != nil {
		// The common cause is a server with no `aclfile`, where Redis has nowhere
		// to write. Failing is the honest answer: the change did land in memory,
		// but it will not survive a restart, and silently reporting success would
		// claim a durability that is not there.
		return false, fmt.Errorf("ACL SAVE: %w — the change is in memory and will be lost on restart. Give the server an `aclfile` directive, or set persist: false to accept that", err)
	}
	return true, nil
}

// normalizeRules renders the desired rules through Redis itself, by applying
// them to a disabled scratch user and reading back what the server stored. The
// result is comparable with the managed user's snapshot because both went
// through the same renderer.
func normalizeRules(ctx context.Context, client aclClient, p userParams) (aclUser, error) {
	probe := probePrefix + p.username
	tokens := append([]string{"reset", "off", "nopass"}, p.rules...)
	if err := client.SetUser(ctx, probe, tokens); err != nil {
		return aclUser{}, fmt.Errorf("rules rejected by Redis: %v", err)
	}
	defer func() { _ = client.DelUser(ctx, probe) }()

	got, exists, err := client.GetUser(ctx, probe)
	if err != nil {
		return aclUser{}, fmt.Errorf("normalise rules: %v", err)
	}
	if !exists {
		return aclUser{}, errors.New("normalise rules: the scratch user disappeared between write and read — another client is managing ACLs concurrently")
	}
	return got, nil
}

// diff reports what differs, in terms an operator reading a log can act on.
func diff(p userParams, cur, want aclUser) []string {
	var out []string
	if p.enabled != cur.enabled {
		out = append(out, fmt.Sprintf("enabled %t → %t", cur.enabled, p.enabled))
	}
	if changed, what := passwordDiff(p, cur); changed {
		out = append(out, what)
	}
	if cur.commands != want.commands {
		out = append(out, "commands")
	}
	if cur.keys != want.keys {
		out = append(out, "keys")
	}
	if cur.channels != want.channels {
		out = append(out, "channels")
	}
	if !equalStrings(cur.selectors, want.selectors) {
		out = append(out, "selectors")
	}
	return out
}

// passwordDiff decides whether the credential differs. An unchanged password
// has to be recognised here, not worked around with an "update password" flag.
func passwordDiff(p userParams, cur aclUser) (bool, string) {
	switch {
	case p.keepPassword:
		// Saying nothing about the credential means "leave it alone", so there is
		// nothing here that can differ. The existing hashes are carried over in
		// desiredTokens; see the note there for why that is possible at all.
		return false, ""
	case p.nopass:
		if cur.nopass {
			return false, ""
		}
		return true, "password → nopass"
	default:
		want := p.passwordHash
		if want == "" {
			want = sha256Hex(p.password)
		}
		// `reset` clears the whole password set, so the user ends up with exactly
		// one password. Anything else — an extra password left over from an
		// earlier run, or nopass — is a difference worth reporting.
		if cur.nopass || len(cur.passwords) != 1 || !strings.EqualFold(cur.passwords[0], want) {
			return true, "password"
		}
		return false, ""
	}
}

// desiredTokens builds the argument list for `ACL SETUSER <user> …`. It always
// starts with `reset` so the result is exactly the requested state: rules do not
// accumulate across runs, and — because `>pass` adds rather than replaces — an
// old password does not survive a rotation.
//
// When no credential was requested, the current one is carried across the
// `reset` by re-applying the stored digests as Redis' own `#<hash>` tokens. That
// is what lets "say nothing about the password" mean "leave it alone" without
// giving up `reset`, and it works because the digest Redis hands out in
// `ACL GETUSER` is exactly the digest it accepts back.
func desiredTokens(p userParams, cur aclUser) []string {
	tokens := []string{"reset"}
	if p.enabled {
		tokens = append(tokens, "on")
	} else {
		tokens = append(tokens, "off")
	}
	switch {
	case p.keepPassword:
		if cur.nopass {
			tokens = append(tokens, "nopass")
		}
		for _, h := range cur.passwords {
			tokens = append(tokens, "#"+strings.ToLower(h))
		}
	case p.nopass:
		tokens = append(tokens, "nopass")
	case p.passwordHash != "":
		tokens = append(tokens, "#"+strings.ToLower(p.passwordHash))
	default:
		tokens = append(tokens, ">"+p.password)
	}
	return append(tokens, p.rules...)
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	aa := append([]string{}, a...)
	bb := append([]string{}, b...)
	sort.Strings(aa)
	sort.Strings(bb)
	for i := range aa {
		if aa[i] != bb[i] {
			return false
		}
	}
	return true
}

func (m *ACL) open(ctx context.Context, c connParams) (aclClient, error) {
	if m.connect != nil {
		return m.connect(ctx, c)
	}
	return dial(ctx, c)
}

func parseUserParams(s *structpb.Struct, state string) (userParams, error) {
	if s == nil {
		return userParams{}, errors.New("params: missing")
	}
	fields := s.GetFields()

	username, err := requireString(s, "username")
	if err != nil {
		return userParams{}, err
	}
	if !validUsername(username) {
		return userParams{}, fmt.Errorf("params.username: %q is not a valid user name (letters, digits, underscore, hyphen, dot and colon, up to 256 characters)", username)
	}

	p := userParams{
		username: username,
		enabled:  boolValueOr(fields["enabled"], true),
		// Durable by default: an ACL that vanishes on the next restart is not the
		// state the caller asked for. Turning it off is an explicit choice.
		persist:      boolValueOr(fields["persist"], true),
		allowSelfMod: boolValue(fields["allow_self_modification"]),
		allowDefault: boolValue(fields["allow_default_user"]),
	}

	// Nothing below describes a user that is about to be deleted.
	if state == "absent" {
		return p, nil
	}

	p.nopass = boolValue(fields["nopass"])
	if v, ok := stringValue(fields["password"]); ok && v != "" {
		p.password = v
	}
	if v, ok := stringValue(fields["password_hash"]); ok && v != "" {
		if !validSHA256Hex(v) {
			return userParams{}, fmt.Errorf("params.password_hash: %q is not a 64-character hex SHA-256 digest", v)
		}
		p.passwordHash = v
	}
	set := 0
	for _, on := range []bool{p.nopass, p.password != "", p.passwordHash != ""} {
		if on {
			set++
		}
	}
	if set > 1 {
		return userParams{}, errors.New("params: password, password_hash and nopass are mutually exclusive")
	}
	// Saying nothing about the credential means "leave it alone" — the reading
	// anyone expects. It is only possible because the digest `ACL GETUSER` hands
	// out is the same digest `ACL SETUSER` accepts back, so the existing password
	// can be carried across the `reset`; see desiredTokens.
	p.keepPassword = set == 0

	rules, err := stringList(fields["rules"])
	if err != nil {
		return userParams{}, fmt.Errorf("params.rules: %w", err)
	}
	for i, r := range rules {
		if strings.ContainsAny(r, " \t\r\n") {
			return userParams{}, fmt.Errorf("params.rules[%d]: %q contains whitespace — every rule is one argument, write them as separate list entries", i, r)
		}
	}
	p.rules = rules
	return p, nil
}

// validUsername is a closed set. Redis accepts almost any binary-safe string,
// but a name with a space breaks the `ACL LIST` line format and a name with a
// `>` or `#` reads as a password token, so the name is validated rather than
// escaped — there is nothing to escape it with.
func validUsername(s string) bool {
	if s == "" || len(s) > 256 {
		return false
	}
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9',
			r == '_', r == '-', r == '.', r == ':':
		default:
			return false
		}
	}
	return true
}

func validSHA256Hex(s string) bool {
	if len(s) != 64 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

func requireString(s *structpb.Struct, key string) (string, error) {
	if s == nil {
		return "", fmt.Errorf("params.%s: missing (params is nil)", key)
	}
	v, ok := s.GetFields()[key]
	if !ok {
		return "", fmt.Errorf("params.%s: missing", key)
	}
	str, ok := stringValue(v)
	if !ok || str == "" {
		return "", fmt.Errorf("params.%s: must be a non-empty string", key)
	}
	return str, nil
}

func stringValue(v *structpb.Value) (string, bool) {
	if v == nil {
		return "", false
	}
	if sv, ok := v.GetKind().(*structpb.Value_StringValue); ok {
		return sv.StringValue, true
	}
	return "", false
}

func stringList(v *structpb.Value) ([]string, error) {
	if v == nil {
		return nil, nil
	}
	lv, ok := v.GetKind().(*structpb.Value_ListValue)
	if !ok {
		return nil, errors.New("must be a list of strings")
	}
	out := make([]string, 0, len(lv.ListValue.GetValues()))
	for _, item := range lv.ListValue.GetValues() {
		str, ok := stringValue(item)
		if !ok || str == "" {
			return nil, errors.New("every entry must be a non-empty string")
		}
		out = append(out, str)
	}
	return out, nil
}

func boolValue(v *structpb.Value) bool { return boolValueOr(v, false) }

func boolValueOr(v *structpb.Value, def bool) bool {
	if v == nil {
		return def
	}
	if bv, ok := v.GetKind().(*structpb.Value_BoolValue); ok {
		return bv.BoolValue
	}
	return def
}

func intValueOr(v *structpb.Value, def int) int {
	if v == nil {
		return def
	}
	if nv, ok := v.GetKind().(*structpb.Value_NumberValue); ok {
		return int(nv.NumberValue)
	}
	return def
}

// sendOutcome sends the final ApplyEvent with changed+message+output.
func sendOutcome(stream grpc.ServerStreamingServer[pluginv1.ApplyEvent], changed bool, message string, output map[string]any) error {
	out, err := structpb.NewStruct(output)
	if err != nil {
		return fmt.Errorf("build output struct: %w", err)
	}
	return stream.Send(&pluginv1.ApplyEvent{Message: message, Changed: changed, Output: out})
}

// sendFailure sends a final ApplyEvent with failed=true (failure travels in the
// event, not as a gRPC error).
func sendFailure(stream grpc.ServerStreamingServer[pluginv1.ApplyEvent], message string) error {
	return stream.Send(&pluginv1.ApplyEvent{Message: message, Failed: true})
}
