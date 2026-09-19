// The `user` object's driver — an ACL user reconciled as the SUBJECT, through ACL
// SETUSER / ACL DELUSER, entirely over go-redis. No redis-cli, no shell, and — the
// whole point of NIM-767 — no file.
//
// What it replaces. A user used to be managed in two halves: a destiny rendered the
// WHOLE users.acl to disk and `acl.reloaded` made the instance re-read it (ACL
// LOAD). The source of truth was the file, so adding one user meant re-rendering
// every user, and forgetting to merge the service accounts back into that render
// wiped replication: an `add_user` scenario needs a ★★ CRITICAL note telling its
// author to merge the system accounts back in, precisely because the render is
// total. Here the subject is the user, and the rest of the file is nobody's
// business.
//
// ★ THE PASSWORD NEVER REACHES THE WIRE IN THE CLEAR. ACL SETUSER takes a
// credential as either `>plaintext` or `#<sha256hex>`, and this object only ever
// sends the hash. It is the same digest users.acl has always carried
// (examples/destiny/redis/templates/users.acl.tmpl, sprig sha256sum), so a user
// written here and the same user written by the render are indistinguishable to
// Redis — which is what keeps the two paths comparable while both exist.
// params.user_password is `secret: true` with `pattern: ^vault:.*` like every other
// secret in this artifact, and the plaintext's absence from the argument vector is
// a guard test, not a promise (user_test.go). It is also the ONLY way to declare a
// credential here: `perms` is a plain string that reaches logs, traces, the UI and
// git unmasked, so a credential directive inside it is refused rather than passed
// through ([aclSecretToken]).
//
// ★ DECLARATIVE, hence `reset`. SETUSER MERGES into a user's existing rules, so a
// permission dropped from the declaration would stay live on the instance and a
// state called `present` would only ever converge upward. Every apply therefore
// opens its rule vector with `reset`. That clears the user's passwords too, which
// is why an omitted user_password re-applies the credentials CARRIED OVER from the
// live ACL LIST line rather than stripping the one clients already hold — the
// add_user scenario's "re-running KEEPS their password", kept mechanically.
//
// ★ NIM-624 HAS NO ANALOGUE HERE — precisely, and no wider. What that ticket
// recorded was a half-written users.acl left on disk by a failed ACL LOAD, after
// which Redis would not start. This path writes no file of its own: ACL SETUSER is
// atomic (Redis validates the entire rule vector and applies none of it on error),
// and `ACL SAVE` — the one thing here that touches the aclfile at all — runs only
// after a command that both succeeded and changed something. A failed apply leaves
// no file Redis will refuse to load.
//
// It does NOT say a failed apply changed nothing. SETUSER and SAVE are two commands
// and nothing makes them one, so a SAVE that fails leaves the instance mutated
// behind a step reporting failure — and ApplyEvent cannot say so, because soul's
// applyrunner tests GetFailed() before GetChanged(). [ensurePersistable] removes the
// common cause of that up front rather than reporting it afterwards, and what it
// cannot remove, saveACL's message names.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	pluginv1 "github.com/souls-guild/soul-stack/proto/plugin/gen/go/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

// validateUserPresent — static checks for `user.present`.
func validateUserPresent(f map[string]*structpb.Value) []string {
	errs := validateAddr(f)
	errs = append(errs, validateUserName(f)...)
	if strings.TrimSpace(stringOrEmpty(f["perms"])) == "" {
		errs = append(errs, `params.perms: must be a non-empty ACL rule string (e.g. "~app:* +@read +@write")`)
	}
	errs = append(errs, validateUserPerms(f)...)
	if _, err := userState(f["state"]); err != nil {
		errs = append(errs, "params.state: "+err.Error())
	}
	if _, err := userPersist(f["persist"]); err != nil {
		errs = append(errs, "params.persist: "+err.Error())
	}
	return errs
}

// validateUserAbsent — static checks for `user.absent`.
func validateUserAbsent(f map[string]*structpb.Value) []string {
	errs := validateAddr(f)
	errs = append(errs, validateUserName(f)...)
	if stringOrEmpty(f["name"]) == "default" {
		errs = append(errs, `params.name: the built-in "default" user cannot be removed (Redis refuses ACL DELUSER default) — disable it with redis.user.present and state: "off"`)
	}
	if _, err := userPersist(f["persist"]); err != nil {
		errs = append(errs, "params.persist: "+err.Error())
	}
	return errs
}

// validateUserName — the shared name check. An ACL username carries no whitespace,
// and a name that does would land in the rule vector as a separate token; the
// author is told here rather than by a Redis parse error mid-apply.
func validateUserName(f map[string]*structpb.Value) []string {
	name := stringOrEmpty(f["name"])
	if strings.TrimSpace(name) == "" {
		return []string{"params.name: must be a non-empty ACL username"}
	}
	// Not echoed: the tail past a NUL is not what the author sees in the YAML, so
	// quoting it in a log line prints something they never wrote.
	if strings.ContainsRune(name, 0) {
		return []string{"params.name: an ACL username carries no NUL byte — Redis would read the name as truncated there and this module would not"}
	}
	if strings.ContainsAny(name, " \t\r\n") {
		return []string{fmt.Sprintf("params.name: an ACL username carries no whitespace, got %q", name)}
	}
	return nil
}

// aclSecretToken reports whether an ACL rule token carries a credential VALUE.
//
// Redis takes permissions and credentials in one rule vector, which is exactly the
// problem: `perms` is a plain declared string — not `secret: true`, so unmasked in
// the rendered task, in keeper's logs and traces and UI, and in git — and
// `>hunter2` smuggled into it is a plaintext password in every one of those places,
// on a param whose contract says it carries permissions. `user_password` is the
// credential channel here, masked and sent as a hash; this is what keeps it the only
// one.
//
// The line is drawn at a VALUE, not at the topic. `nopass` and `resetpass` state
// something about credentials while carrying nothing secret, and they are the honest
// way to declare a user that holds no password — so they stay legal ALONE, and the
// fact that they override the carry-over is the author saying so explicitly rather
// than an omitted user_password doing it by accident. Together with a declared
// user_password they are a contradiction and [validateUserPerms] refuses them.
//
// Prefixes only, so this one needs no case folding: Redis's credential forms are all
// symbols. Every KEYWORD compared in validateUserPerms does need it.
//
// A LEADING `(` is stripped first, and that is not cosmetic. Redis merges a selector
// across arguments (ACLMergeSelectorArguments), so `(>secret +get)` splits on
// whitespace into `(>secret` and `+get)` — a token whose credential sits at byte 1,
// invisible to a test of byte 0, while Redis reads the same rule as one modifier and
// quotes the WHOLE thing back in its syntax error. That error becomes
// ApplyEvent.Message, so the prefix test missing it does not merely let a credential
// through: it puts the plaintext on the exact surface this function exists to keep it
// off, where redactError cannot reach it (the value is in perms, not in a secret
// param). `(+get >secret)` was already caught, so the two orderings of one rule got
// opposite verdicts.
func aclSecretToken(tok string) bool {
	tok = strings.TrimLeft(tok, "(")
	// >plaintext / <plaintext / #sha256hex / !sha256hex — Redis's four
	// credential-bearing prefixes, all single-character.
	return strings.HasPrefix(tok, ">") || strings.HasPrefix(tok, "<") ||
		strings.HasPrefix(tok, "#") || strings.HasPrefix(tok, "!")
}

// validateUserPerms refuses, inside params.perms, the tokens that would reach past
// this object and undo what it just declared.
//
// EVERY keyword comparison here is case-insensitive, because Redis's own ACL parser
// is (strcasecmp), and an exact match is a one-keystroke bypass of the whole rule:
// `RESET` walked straight through the first version of this and cost the user their
// declared perms, state and password on a live instance, reported as a success.
//
// The three refusals are three different harms:
//
//   - a credential VALUE ([aclSecretToken]) — plaintext on an unmasked param;
//   - `reset` — the vector this object builds OPENS with one and then sets the state
//     and the credentials, so a second one mid-perms discards both and leaves
//     whatever follows it: a user with no state and no password and no indication
//     why;
//   - `on` / `off` — that is params.state's job, and a token here lands AFTER it and
//     overrides it, which also makes Output.state describe something the instance is
//     not (a scenario reading register.<x>.state would get the declaration back);
//   - `nopass` / `resetpass` TOGETHER WITH a declared user_password — Redis takes the
//     last directive, so the two contradict and the perms token wins silently. Either
//     one alone is legitimate: `nopass` with no user_password is how a passwordless
//     user is declared, and that stays legal.
func validateUserPerms(f map[string]*structpb.Value) []string {
	var errs []string
	declaresPassword := stringOrEmpty(f["user_password"]) != ""
	for i, tok := range strings.Fields(stringOrEmpty(f["perms"])) {
		switch {
		case strings.ContainsRune(tok, 0):
			// FIRST, and it does not echo the token. strcasecmp stops at the first
			// NUL and EqualFold does not, so "reset\x00x" is `reset` to Redis and
			// not-`reset` to every comparison below — which restored all three
			// harms this function exists to prevent, live, reported as success.
			// Refusing the byte is the only comparison that cannot drift from
			// Redis's: truncating instead would leave the object silently
			// reinterpreting what the author wrote. Not echoed because the tail
			// past the NUL is attacker-chosen and this message reaches the logs.
			errs = append(errs, fmt.Sprintf(
				"params.perms: token %d contains a NUL byte — Redis compares ACL keywords with strcasecmp, which stops there, so the token would mean something other than it reads as", i+1))
		case aclSecretToken(tok):
			// The token is named by its PREFIX and its position, never quoted: the
			// value is the credential, and this message goes into ApplyEvent.Message,
			// which is the log/trace/UI surface the rule is protecting. Echoing it
			// would leak it on the one path that exists to refuse it. The keywords
			// below carry no value, so those messages do quote them.
			// The prefix is taken from the token with its leading `(` stripped, the
			// same reading aclSecretToken used — naming `(` here would point the
			// author at the wrong character.
			bare := strings.TrimLeft(tok, "(")
			errs = append(errs, fmt.Sprintf(
				"params.perms: token %d carries a credential (it opens with %q), and perms is not a secret param — its value reaches logs, traces, the UI and git in the clear; declare the password in user_password (masked, sent as a hash) instead", i+1, bare[:1]))
		case strings.EqualFold(tok, "reset"):
			errs = append(errs, fmt.Sprintf(
				"params.perms: %q would discard the state and credentials this step sets before it — the rule vector already opens with a reset; drop the token", tok))
		case strings.EqualFold(tok, "on"), strings.EqualFold(tok, "off"):
			errs = append(errs, fmt.Sprintf(
				"params.perms: %q belongs in params.state — a token here lands after it and overrides it, and Output.state would then describe something the instance is not", tok))
		case declaresPassword && (strings.EqualFold(tok, "nopass") || strings.EqualFold(tok, "resetpass")):
			errs = append(errs, fmt.Sprintf(
				"params.perms: %q clears the credential this step declares in user_password, and Redis takes the last directive — the two contradict; drop one", tok))
		}
	}
	return errs
}

// userPersist reads params.persist as a boolean, defaulting to true.
//
// Strict for the same reason [userState] is, and with more at stake: the fallback
// direction of a coercion here is "write it to the aclfile", so a value this cannot
// read would land on the side that touches disk. Refused instead.
func userPersist(v *structpb.Value) (bool, error) {
	if v == nil {
		return true, nil
	}
	if bv, ok := v.GetKind().(*structpb.Value_BoolValue); ok {
		return bv.BoolValue, nil
	}
	return false, fmt.Errorf("must be a boolean (true/false)")
}

// userState reads params.state as "on"/"off", defaulting to "on".
//
// It REFUSES a boolean rather than coercing one, and the reason is YAML 1.1:
// unquoted `state: on` parses as true and `state: off` as false long before this
// plugin sees either. Coercing would land `off` — the one value whose entire point
// is to disable an account — as the default `on`, silently inverting what the
// author wrote. Refused, so they quote it.
func userState(v *structpb.Value) (string, error) {
	if v == nil {
		return "on", nil
	}
	if _, ok := v.GetKind().(*structpb.Value_BoolValue); ok {
		return "", fmt.Errorf(`must be the string "on" or "off" — an unquoted YAML on/off parses as a boolean, so write it quoted`)
	}
	s, ok := stringValue(v)
	if !ok {
		return "", fmt.Errorf(`must be the string "on" or "off"`)
	}
	switch s = strings.TrimSpace(s); s {
	case "":
		return "on", nil
	case "on", "off":
		return s, nil
	default:
		return "", fmt.Errorf(`must be "on" or "off", got %q`, s)
	}
}

// aclPasswordHash renders a credential the way ACL SETUSER takes it WITHOUT the
// plaintext: `#<sha256hex>`, the same digest users.acl.tmpl writes.
func aclPasswordHash(password string) string {
	sum := sha256.Sum256([]byte(password))
	return "#" + hex.EncodeToString(sum[:])
}

// userRules builds the ACL SETUSER argument vector: reset, then the state, then the
// credential directives, then the declared perms split into tokens.
func userRules(name, state string, passwords []string, perms string) []any {
	args := []any{"ACL", "SETUSER", name, "reset", state}
	for _, p := range passwords {
		args = append(args, p)
	}
	for _, tok := range strings.Fields(perms) {
		args = append(args, tok)
	}
	return args
}

// aclUserLine returns the ACL LIST line describing `name`, or "" when the instance
// has no such user.
//
// The line carries password hashes, so everything derived from it stays inside this
// file — the rule acl.reloaded already follows for the same reason.
func aclUserLine(lines []string, name string) string {
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) >= 2 && fields[0] == "user" && fields[1] == name {
			return line
		}
	}
	return ""
}

// carryPasswords pulls the credential directives out of a live ACL LIST line: every
// `#<hash>` the user holds, plus `nopass` if it is set.
//
// This is what makes an omitted user_password mean "keep the credential clients
// already hold" instead of "strip it": `reset` clears passwords, so without
// carrying them over a perms-only re-run would lock every client out of the account
// it was tidying.
func carryPasswords(line string) []string {
	fields := strings.Fields(line)
	if len(fields) <= 2 {
		return nil
	}
	var out []string
	for _, tok := range fields[2:] {
		if tok == "nopass" || strings.HasPrefix(tok, "#") {
			out = append(out, tok)
		}
	}
	return out
}

// applyUserPresent reconciles ONE ACL user to the declared perms and state.
//
// Idempotent BY CONSTRUCTION, and `changed` is honest without predicting Redis's
// normalization: the ACL LIST line for this user is read before and after the
// SETUSER, and both sides are Redis's OWN rendering of its own rules. Comparing a
// declaration against a live line instead would report a permanent false change the
// first time Redis reorders `+@all ~*` or adds the `resetchannels` it now implies.
// Symmetry with acl.reloaded, which diffs the same way around ACL LOAD.
func (m *RedisModule) applyUserPresent(ctx context.Context, stream grpc.ServerStreamingServer[pluginv1.ApplyEvent], conn redisConn, params *structpb.Struct) error {
	f := params.GetFields()
	password := stringOrEmpty(f["password"])
	userPassword := stringOrEmpty(f["user_password"])
	name := stringOrEmpty(f["name"])
	perms := stringOrEmpty(f["perms"])

	// Apply re-checks EVERY static rule, not a chosen one, because nothing upstream
	// checks them for it. Validate is a separate RPC and the runner in this tree does
	// not call it (soul/internal/runtime/applyrunner.go calls Apply); keeper's static
	// check is presence-only and returns early on a `${…}`-rendered cell
	// (shared/config/module_params.go). So `perms: "${ vars.acl }"` with the var
	// unset lints clean, arrives as "", and — since the vector opens with `reset` —
	// would strip every permission the live user holds and then ACL SAVE it. The
	// first draft re-checked `state` alone and had exactly that hole.
	if errs := validateUserPresent(f); len(errs) > 0 {
		return sendFailure(stream, strings.Join(errs, "; "))
	}
	state, _ := userState(f["state"])       // accepted just above
	persist, _ := userPersist(f["persist"]) // likewise

	if err := ensurePersistable(ctx, conn, persist); err != nil {
		return sendFailure(stream, err.Error())
	}

	before, err := conn.AclList(ctx)
	if err != nil {
		return sendFailure(stream, "ACL LIST (before): "+redactError(err, password, userPassword))
	}
	beforeLine := aclUserLine(before, name)

	passwords := carryPasswords(beforeLine)
	if userPassword != "" {
		passwords = []string{aclPasswordHash(userPassword)}
	}

	if _, err := conn.Do(ctx, userRules(name, state, passwords, perms)...); err != nil {
		// Atomic: Redis applied none of the vector, so the user is still exactly
		// what `before` found — nothing to unwind and nothing to persist.
		return sendFailure(stream, fmt.Sprintf("ACL SETUSER %s: %s", name, redactError(err, password, userPassword)))
	}

	after, err := conn.AclList(ctx)
	if err != nil {
		return sendFailure(stream, "ACL LIST (after): "+redactError(err, password, userPassword))
	}
	afterLine := aclUserLine(after, name)

	changed := beforeLine != afterLine
	if changed {
		if err := saveACL(ctx, conn, persist, password, userPassword); err != nil {
			return sendFailure(stream, err.Error())
		}
	}

	message := fmt.Sprintf("ACL user %s already matches (no-op)", name)
	if changed {
		message = fmt.Sprintf("ACL user %s reconciled", name)
	}
	// Output carries the name and the declared state only. The lines this was
	// diffed against hold password hashes and go nowhere.
	return sendOutcome(stream, changed, message, map[string]any{
		"name":  name,
		"state": state,
	})
}

// applyUserAbsent removes ONE ACL user (ACL DELUSER).
//
// Idempotent: an unknown user is a no-op that sends no MUTATING command — it reads
// the ACL to find out, and nothing else. DELUSER
// would answer 0 and do no harm, but an honest probe-skip keeps `changed` truthful
// and leaves the aclfile alone — the same shape as detached's already-master no-op.
func (m *RedisModule) applyUserAbsent(ctx context.Context, stream grpc.ServerStreamingServer[pluginv1.ApplyEvent], conn redisConn, params *structpb.Struct) error {
	f := params.GetFields()
	password := stringOrEmpty(f["password"])
	name := stringOrEmpty(f["name"])

	// Same reason as applyUserPresent: nothing upstream enforces these. Here it is
	// what keeps `name: default` from reaching the instance and what keeps an empty
	// rendered name from creating the nameless ACL entry Redis will accept and no
	// operator can then address.
	if errs := validateUserAbsent(f); len(errs) > 0 {
		return sendFailure(stream, strings.Join(errs, "; "))
	}
	persist, _ := userPersist(f["persist"]) // accepted just above

	before, err := conn.AclList(ctx)
	if err != nil {
		return sendFailure(stream, "ACL LIST (before): "+redactError(err, password))
	}
	if aclUserLine(before, name) == "" {
		return sendOutcome(stream, false, fmt.Sprintf("ACL user %s is already absent (no-op)", name), map[string]any{
			"name": name,
		})
	}

	// AFTER the probe, unlike `present`. There the pre-flight has to come first
	// because nothing can say whether the SETUSER will change anything until it has
	// run. Here the probe already did: with no user to remove there is nothing to
	// persist either, so checking first turned an idempotent cleanup step into a
	// hard failure on an instance with no aclfile — a red for a run that had no work
	// to do.
	if err := ensurePersistable(ctx, conn, persist); err != nil {
		return sendFailure(stream, err.Error())
	}

	removed, err := conn.Do(ctx, "ACL", "DELUSER", name)
	if err != nil {
		return sendFailure(stream, fmt.Sprintf("ACL DELUSER %s: %s", name, redactError(err, password)))
	}
	// DELUSER answers with how many users it removed. The probe above found one, so
	// a 0 means something else removed it in between — a no-op, and persisting a
	// no-op is the aclfile churn the probe-skip exists to avoid.
	if strings.TrimSpace(removed) == "0" {
		return sendOutcome(stream, false, fmt.Sprintf("ACL user %s is already absent (no-op)", name), map[string]any{
			"name": name,
		})
	}
	if err := saveACL(ctx, conn, persist, password); err != nil {
		return sendFailure(stream, err.Error())
	}

	return sendOutcome(stream, true, fmt.Sprintf("ACL user %s removed", name), map[string]any{
		"name": name,
	})
}

// saveACL persists the in-memory ACL into the instance's aclfile (ACL SAVE), which
// is what keeps a SETUSER from being reverted by the next restart or by the next
// acl.reloaded — ACL LOAD re-reads the file, and a user only Redis's memory knows
// about is gone.
//
// Called ONLY after a command that succeeded AND changed something, deliberately on
// both counts. After a failure there is nothing to persist. On a no-op it would
// rewrite the aclfile with Redis's own rendering of rules a destiny template wrote
// — the same ACL in different bytes, which the next render reads as a change and
// answers with a restart, on every single run.
//
// A change DOES leave the file in Redis's rendering rather than the template's,
// which is a real seam between the two paths while both exist. It belongs to
// whoever wires this object into a service (NIM-768), not to the object.
func saveACL(ctx context.Context, conn redisConn, persist bool, secrets ...string) error {
	if !persist {
		return nil
	}
	if _, err := conn.Do(ctx, "ACL", "SAVE"); err != nil {
		// The message has to carry the half that already happened. SETUSER/DELUSER
		// have landed by the time this runs, and ApplyEvent cannot say so: soul's
		// applyrunner tests GetFailed() BEFORE GetChanged() (applyrunner.go:1123),
		// so `changed` on a failed event is unreachable and every handler keyed on
		// it stays silent. An operator reading "failed" would otherwise conclude
		// nothing was done, and a re-run reports a no-op because the INSTANCE now
		// matches — the drift is in the file, where nothing looks at it.
		return fmt.Errorf("ACL SAVE: %s — the change IS live on the instance and is NOT in the aclfile: it will be lost on the next restart or acl.reloaded, and a re-run will report a no-op because the instance already matches. Fix the aclfile directive, or declare persist: false if this instance is meant to hold its ACL in memory only",
			redactError(err, secrets...))
	}
	return nil
}

// ensurePersistable refuses, BEFORE anything is mutated, a `persist: true` run
// against an instance that has no aclfile to persist into.
//
// Without it the common shape of that mistake is the worst kind of half-apply: the
// SETUSER lands, ACL SAVE fails, the step reports failure, and the operator is left
// with a live user nothing recorded — then a re-run says "no-op" because the
// instance matches, so no run ever reports the change. Asking CONFIG GET first
// turns all of that into one refusal that mutates nothing, at the cost of one round
// trip on the applies that persist.
//
// It is a pre-flight, not a guarantee: a SAVE can still fail on a full disk or a
// read-only mount, which is what the message in [saveACL] is for.
//
// ★ AND IT NEVER FAILS THE APPLY ON ITS OWN ACCOUNT. A pre-flight that turns its own
// unavailability into a refusal is worse than no pre-flight: CONFIG is @admin
// /@dangerous while ACL SETUSER and ACL SAVE are not, so a least-privilege operator
// (`+acl +ping +select`, no `+config|get`) could do the entire job and the first
// version of this refused all of it with NOPERM — work the very same connection was
// able to perform. `rename-command CONFIG ""` is the same story. So only a CONFIG GET
// that SUCCEEDS and answers empty is evidence, and an error means "cannot tell",
// which puts us back to exactly the behaviour before the pre-flight existed, message
// and all.
func ensurePersistable(ctx context.Context, conn redisConn, persist bool) error {
	if !persist {
		return nil
	}
	aclfile, err := configGet(ctx, conn, "aclfile")
	if err != nil {
		return nil
	}
	if strings.TrimSpace(aclfile) == "" {
		return fmt.Errorf("params.persist is true but this instance has no aclfile directive, so ACL SAVE cannot succeed — refused BEFORE touching the ACL, because the change would otherwise be live and unrecorded. Declare persist: false to keep the change in memory only, or configure an aclfile")
	}
	return nil
}
