// Package config is the `config` module of the Redis bundle: idempotent
// management of Redis runtime parameters, `CONFIG SET` for the `present` state
// and `CONFIG GET` into the register for `read`.
//
// [Module] describes the contract, this file implements it. The address the
// operator writes — `redis.config.present` — takes its first level from the
// alias the source was registered under, not from anything declared here.
//
// ★ `changed` comes from the server, not from string matching. Redis normalises
// what it stores: "100mb" is handed back as "104857600", "KEA" as "AKE",
// "EVERYSEC" as "everysec", and the multipliers are not the ones anyone guesses
// (1k = 1000 but 1kb = 1024, verified live). Comparing the requested spelling
// against the stored one therefore reports a change on every single run. This
// module does the opposite: it uses its own canonicalisation ONLY to decide
// whether a write is worth attempting, then re-reads and reports `changed` from
// the before/after values the server itself produced. A canonicalisation miss
// costs one redundant CONFIG SET; it cannot cost a false "changed", which is the
// property that matters.
//
// ★ The write is one atomic CONFIG SET where the server supports it. Redis 7.0+
// applies a multi-parameter CONFIG SET all-or-nothing — verified live: a bad
// second argument leaves the first untouched. Redis 6.2 rejects the form with an
// arity error, having applied nothing, so falling back to one write per
// parameter is safe; but that fallback is no longer atomic, and it says so in
// the output rather than pretending otherwise.
//
// Backend is github.com/redis/go-redis/v9. Credentials arrive already resolved
// by keeper-render from a vault-ref; the plugin does not resolve Vault itself.
package config

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	pluginv1 "github.com/souls-guild/soul-stack/proto/plugin/gen/go/v1"
	"github.com/souls-guild/soul-stack/sdk/module"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

// Config implements the SoulModule redis.config.
type Config struct {
	module.BaseModule

	// connect is the injection point for L0 tests. nil → a real redis client.
	connect func(ctx context.Context, c connParams) (configClient, error)
}

// PlanReadSafe declares that Plan touches nothing (ADR-031 Scry). It holds here
// because determining drift needs `CONFIG GET` and nothing else — unlike the
// bundle's `acl` module, whose rule diff has to push the desired rules through a
// scratch user and therefore genuinely writes, which is why that one leaves the
// marker off and takes default-deny on dry-run.
func (m *Config) PlanReadSafe() {}

// configClient is the narrow surface of a Redis connection this module needs.
type configClient interface {
	Get(ctx context.Context, pattern string) (map[string]string, error)
	Set(ctx context.Context, pairs []setting) error
	Rewrite(ctx context.Context) error
	Close() error
}

// setting is one parameter/value pair.
type setting struct {
	name  string
	value string
}

// presentParams is the desired state of the `present` state.
type presentParams struct {
	settings      []setting
	persist       bool
	allowConnLoss bool
	requireAtomic bool
}

// readParams is the input of the `read` state.
type readParams struct {
	patterns      []string
	revealSecrets bool
}

// secretParams are handed out by `CONFIG GET` in the clear — verified live, a
// plain `CONFIG GET requirepass` prints the password. Their values never reach
// the output unless the caller asks for them by name.
var secretParams = map[string]bool{
	"requirepass":              true,
	"masterauth":               true,
	"tls-key-file-pass":        true,
	"tls-client-key-file-pass": true,
}

// connectivityParams can cut the path this module manages the server through.
// Verified live: `CONFIG SET port` moves the listener and the connection that
// issued it never gets a reply; `protected-mode yes` makes every later command
// from a non-loopback client fail with DENIED, including the one that would turn
// it back off. `bind` turned out to be defended by Redis itself (an
// unbindable address is rejected and the old one kept) — it stays on the list
// anyway, because a bindable-but-wrong address is not defended by anything.
var connectivityParams = map[string]bool{
	"port": true, "bind": true, "unixsocket": true, "unixsocketperm": true,
	"tls-port": true, "protected-mode": true, "requirepass": true,
	"masterauth": true, "maxclients": true, "tls-cluster": true,
	"tls-replication": true, "tls-auth-clients": true, "tls-cert-file": true,
	"tls-key-file": true, "tls-ca-cert-file": true, "tls-ca-cert-dir": true,
}

const redacted = "(redacted)"

// Apply applies the requested state to the resource.
func (m *Config) Apply(req *pluginv1.ApplyRequest, stream grpc.ServerStreamingServer[pluginv1.ApplyEvent]) error {
	ctx := stream.Context()

	conn, err := parseConnParams(req.GetParams())
	if err != nil {
		return sendFailure(stream, err.Error())
	}

	switch req.GetState() {
	case "present":
		p, err := parsePresentParams(req.GetParams())
		if err != nil {
			return sendFailure(stream, err.Error())
		}
		if err := checkConnectivityGate(p); err != nil {
			return sendFailure(stream, err.Error())
		}
		client, err := m.open(ctx, conn)
		if err != nil {
			return sendFailure(stream, fmt.Sprintf("connect: %v", err))
		}
		defer func() { _ = client.Close() }()
		return applyPresent(ctx, stream, client, p)

	case "read":
		p, err := parseReadParams(req.GetParams())
		if err != nil {
			return sendFailure(stream, err.Error())
		}
		client, err := m.open(ctx, conn)
		if err != nil {
			return sendFailure(stream, fmt.Sprintf("connect: %v", err))
		}
		defer func() { _ = client.Close() }()
		return applyRead(ctx, stream, client, p)

	default:
		return sendFailure(stream, fmt.Sprintf("unknown state %q (expected present|read)", req.GetState()))
	}
}

// Plan reports drift without touching the server (ADR-031 Scry): it runs the
// same `CONFIG GET` reads Apply starts with and stops there.
//
// Its answer is the one Apply would give only up to canonicalisation: a value
// Redis stores in a form this module cannot predict — `notify-keyspace-events`
// reordering "KEA" to "AKE" is the standing example — is reported as drift here
// and then turns out to be a no-op under Apply, which re-reads and sees the
// value never moved. The error is one-directional on purpose: Plan may
// over-report a change, never under-report one.
func (m *Config) Plan(req *pluginv1.PlanRequest, stream grpc.ServerStreamingServer[pluginv1.PlanEvent]) error {
	ctx := stream.Context()

	conn, err := parseConnParams(req.GetParams())
	if err != nil {
		return stream.Send(&pluginv1.PlanEvent{Message: err.Error()})
	}

	switch req.GetState() {
	case "present":
		p, err := parsePresentParams(req.GetParams())
		if err != nil {
			return stream.Send(&pluginv1.PlanEvent{Message: err.Error()})
		}
		if err := checkConnectivityGate(p); err != nil {
			return stream.Send(&pluginv1.PlanEvent{Message: err.Error()})
		}
		client, err := m.open(ctx, conn)
		if err != nil {
			return stream.Send(&pluginv1.PlanEvent{Message: fmt.Sprintf("connect: %v", err)})
		}
		defer func() { _ = client.Close() }()

		before, err := readCurrent(ctx, client, p.settings)
		if err != nil {
			return stream.Send(&pluginv1.PlanEvent{Message: err.Error()})
		}
		pending := candidates(p.settings, before)
		if len(pending) == 0 {
			return stream.Send(&pluginv1.PlanEvent{Message: "all parameters already hold the requested values", Changed: false})
		}
		names := make([]string, 0, len(pending))
		for _, s := range pending {
			names = append(names, s.name)
		}
		return stream.Send(&pluginv1.PlanEvent{
			Message: fmt.Sprintf("would set %d parameter(s): %s", len(pending), strings.Join(names, ", ")),
			Changed: true,
		})

	case "read":
		// Reading never changes anything, so drift is not a question that has a
		// meaning here. Answer clean without opening a connection.
		return stream.Send(&pluginv1.PlanEvent{Message: "read is a pure read", Changed: false})

	default:
		return stream.Send(&pluginv1.PlanEvent{Message: fmt.Sprintf("unknown state %q (expected present|read)", req.GetState())})
	}
}

func (m *Config) open(ctx context.Context, c connParams) (configClient, error) {
	if m.connect != nil {
		return m.connect(ctx, c)
	}
	return dial(ctx, c)
}

func applyPresent(ctx context.Context, stream grpc.ServerStreamingServer[pluginv1.ApplyEvent], client configClient, p presentParams) error {
	before, err := readCurrent(ctx, client, p.settings)
	if err != nil {
		return sendFailure(stream, err.Error())
	}

	pending := candidates(p.settings, before)
	if len(pending) == 0 {
		return sendOutcome(stream, false, "all parameters already hold the requested values", map[string]any{
			"action":             "noop",
			"changes":            "",
			"changed_parameters": []any{},
			"persisted":          false,
			"atomic":             true,
		})
	}

	atomic, err := write(ctx, client, pending, p.requireAtomic)
	if err != nil {
		return sendFailure(stream, err.Error())
	}

	// The verdict comes from the server, not from what was asked for: re-read the
	// parameters that were written and compare against the snapshot taken before.
	after, err := readCurrent(ctx, client, pending)
	if err != nil {
		return sendFailure(stream, fmt.Sprintf("re-read after write: %v", err))
	}

	moved := make([]string, 0, len(pending))
	lines := make([]string, 0, len(pending))
	for _, s := range pending {
		if before[s.name] == after[s.name] {
			continue
		}
		moved = append(moved, s.name)
		lines = append(lines, fmt.Sprintf("%s: %s -> %s", s.name, display(s.name, before[s.name]), display(s.name, after[s.name])))
	}

	if len(moved) == 0 {
		// Every write was accepted and nothing moved: the values were already
		// right and only spelled differently. This is the canonicalisation miss
		// the package comment describes, and the honest answer is "no change".
		return sendOutcome(stream, false, "all parameters already hold the requested values", map[string]any{
			"action":             "noop",
			"changes":            "",
			"changed_parameters": []any{},
			"persisted":          false,
			"atomic":             atomic,
		})
	}

	persisted := false
	if p.persist {
		if err := client.Rewrite(ctx); err != nil {
			return sendFailure(stream, fmt.Sprintf(
				"CONFIG REWRITE after setting %s: %v (the values ARE applied in memory but are NOT durable; "+
					"a server started without a config file has nowhere to write — set persist: false to accept that)",
				strings.Join(moved, ", "), err))
		}
		persisted = true
	}

	names := make([]any, 0, len(moved))
	for _, n := range moved {
		names = append(names, n)
	}
	return sendOutcome(stream, true, fmt.Sprintf("set %d parameter(s)", len(moved)), map[string]any{
		"action":             "altered",
		"changes":            strings.Join(lines, "; "),
		"changed_parameters": names,
		"persisted":          persisted,
		"atomic":             atomic,
	})
}

func applyRead(ctx context.Context, stream grpc.ServerStreamingServer[pluginv1.ApplyEvent], client configClient, p readParams) error {
	merged := map[string]string{}
	for _, pattern := range p.patterns {
		got, err := client.Get(ctx, pattern)
		if err != nil {
			return sendFailure(stream, fmt.Sprintf("CONFIG GET %s: %v", pattern, err))
		}
		for k, v := range got {
			merged[k] = v
		}
	}

	out := make(map[string]any, len(merged))
	withheld := make([]any, 0)
	names := make([]string, 0, len(merged))
	for k := range merged {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		if secretParams[k] && !p.revealSecrets {
			out[k] = redacted
			withheld = append(withheld, k)
			continue
		}
		out[k] = merged[k]
	}

	return sendOutcome(stream, false, fmt.Sprintf("read %d parameter(s)", len(out)), map[string]any{
		"config":   out,
		"count":    len(out),
		"redacted": withheld,
	})
}

// readCurrent reads the named parameters one at a time. One at a time because
// Redis 6.2 rejects a multi-pattern CONFIG GET outright, and because an empty
// reply then means exactly one thing: this server does not have this parameter.
func readCurrent(ctx context.Context, client configClient, want []setting) (map[string]string, error) {
	cur := make(map[string]string, len(want))
	for _, s := range want {
		got, err := client.Get(ctx, s.name)
		if err != nil {
			return nil, fmt.Errorf("CONFIG GET %s: %w", s.name, err)
		}
		v, ok := got[s.name]
		if !ok {
			// Silence here would be the worst outcome: an unsupported parameter
			// would look like "nothing to change" and the step would report
			// success forever. The parameter set differs a lot between versions —
			// 169 parameters on 6.2 against 302 on 8.
			return nil, fmt.Errorf("params.settings: this server does not have a parameter named %q "+
				"(the set differs between Redis versions; CONFIG GET returned nothing for it)", s.name)
		}
		cur[s.name] = v
	}
	return cur, nil
}

// candidates keeps the settings whose canonical desired value differs from the
// canonical current one. A miss here is not a correctness problem — see the
// package comment — but every hit saved is a write not made.
func candidates(want []setting, cur map[string]string) []setting {
	out := make([]setting, 0, len(want))
	for _, s := range want {
		if canonical(s.value) == canonical(cur[s.name]) {
			continue
		}
		out = append(out, s)
	}
	return out
}

// write applies the pending settings, atomically if the server accepts the
// multi-parameter form. Returns whether the write was atomic.
func write(ctx context.Context, client configClient, pending []setting, requireAtomic bool) (bool, error) {
	if len(pending) == 1 {
		// A single pair is atomic by construction on every version.
		if err := client.Set(ctx, pending); err != nil {
			return true, fmt.Errorf("CONFIG SET %s: %w", pending[0].name, err)
		}
		return true, nil
	}

	err := client.Set(ctx, pending)
	if err == nil {
		return true, nil
	}
	if !isArityError(err) {
		// A real rejection — a bad value, an immutable parameter. Nothing was
		// applied (Redis 7.0+ is all-or-nothing), so report it as it came.
		return false, fmt.Errorf("CONFIG SET: %w", err)
	}
	if requireAtomic {
		return false, fmt.Errorf("CONFIG SET: this server does not accept a multi-parameter CONFIG SET "+
			"(Redis 6.2 and older), so %d parameters cannot be applied atomically; "+
			"require_atomic is set, so nothing was written: %w", len(pending), err)
	}

	// Pre-7.0: one write per parameter, in a fixed order so a partial failure is
	// reproducible rather than depending on map iteration.
	for _, s := range pending {
		if err := client.Set(ctx, []setting{s}); err != nil {
			return false, fmt.Errorf("CONFIG SET %s (writing one parameter at a time, because this server "+
				"rejects the multi-parameter form; parameters before this one ARE applied): %w", s.name, err)
		}
	}
	return false, nil
}

// isArityError distinguishes "this server has no multi-parameter CONFIG SET"
// from "this server refused what you asked". Redis 6.2 answers the former with
// `ERR Unknown subcommand or wrong number of arguments for 'SET'`.
//
// The distinction does not have to be perfect. Every parameter name was already
// proven to exist by readCurrent, so an unknown-option error cannot arrive here;
// and a false positive only costs a fallback to one-at-a-time writes, which
// re-raises the very same rejection on the very same parameter.
func isArityError(err error) bool {
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "wrong number of arguments") || strings.Contains(s, "unknown subcommand")
}

// canonical folds the spellings that are safe to fold. Whitespace is collapsed,
// and a memory size is resolved to bytes using Redis' own multipliers (1k =
// 1000, 1kb = 1024, 1m = 1000000, 1mb = 1048576, 1g = 1000000000, 1gb =
// 1073741824 — verified live, not assumed).
//
// Case is deliberately NOT folded. Redis lowercases `appendfsync`, which would
// make case-folding look attractive, but `notify-keyspace-events` is a flag set
// where case carries meaning: `K` is keyspace events and `k` is not the same
// thing. Folding case would silence a real difference on one parameter to save a
// redundant write on another.
func canonical(v string) string {
	v = strings.Join(strings.Fields(v), " ")
	if b, ok := parseMemory(v); ok {
		return b
	}
	return v
}

// parseMemory resolves "100mb" to "104857600". Only a bare number with a known
// suffix qualifies; anything else is left alone.
func parseMemory(v string) (string, bool) {
	if v == "" || strings.ContainsAny(v, " \t") {
		return "", false
	}
	lower := strings.ToLower(v)
	units := []struct {
		suffix string
		mult   int64
	}{
		{"kb", 1024}, {"mb", 1024 * 1024}, {"gb", 1024 * 1024 * 1024},
		{"k", 1000}, {"m", 1000 * 1000}, {"g", 1000 * 1000 * 1000},
		{"b", 1},
	}
	for _, u := range units {
		if !strings.HasSuffix(lower, u.suffix) {
			continue
		}
		digits := strings.TrimSuffix(lower, u.suffix)
		n, err := strconv.ParseInt(digits, 10, 64)
		if err != nil || n < 0 {
			return "", false
		}
		return strconv.FormatInt(n*u.mult, 10), true
	}
	return "", false
}

// display hides the value of a secret parameter. The before/after of a password
// change is still visible as a change — only the two values are withheld.
func display(name, value string) string {
	if secretParams[name] {
		return redacted
	}
	return value
}

func checkConnectivityGate(p presentParams) error {
	if p.allowConnLoss {
		return nil
	}
	var blocked []string
	for _, s := range p.settings {
		if connectivityParams[s.name] {
			blocked = append(blocked, s.name)
		}
	}
	if len(blocked) == 0 {
		return nil
	}
	return fmt.Errorf("params.settings: %s can cut the path this module reaches the server through "+
		"(CONFIG SET port drops the connection that issues it; protected-mode yes makes every further "+
		"command from a non-loopback client fail, including the one that would undo it) — "+
		"set allow_connectivity_change: true to proceed anyway", strings.Join(blocked, ", "))
}

func parsePresentParams(s *structpb.Struct) (presentParams, error) {
	if s == nil {
		return presentParams{}, errors.New("params: missing")
	}
	fields := s.GetFields()

	raw, ok := fields["settings"]
	if !ok {
		return presentParams{}, errors.New("params.settings: missing")
	}
	sv, ok := raw.GetKind().(*structpb.Value_StructValue)
	if !ok {
		return presentParams{}, errors.New("params.settings: must be a map of parameter name to value")
	}
	entries := sv.StructValue.GetFields()
	if len(entries) == 0 {
		return presentParams{}, errors.New("params.settings: must name at least one parameter")
	}

	names := make([]string, 0, len(entries))
	for name := range entries {
		names = append(names, name)
	}
	// Fixed order: it decides the order of a non-atomic fallback write and of
	// everything reported back, neither of which should depend on map iteration.
	sort.Strings(names)

	p := presentParams{
		settings: make([]setting, 0, len(names)),
		// Durable by default: a parameter that reverts on the next restart is not
		// the state the caller asked for. Turning it off is an explicit choice.
		persist:       boolValueOr(fields["persist"], true),
		allowConnLoss: boolValue(fields["allow_connectivity_change"]),
		requireAtomic: boolValue(fields["require_atomic"]),
	}
	for _, name := range names {
		if !validParamName(name) {
			return presentParams{}, fmt.Errorf("params.settings: %q is not a valid parameter name "+
				"(lower-case letters, digits and hyphens). It reaches Redis as a bare argument, so it is "+
				"validated rather than escaped", name)
		}
		value, err := scalarString(entries[name])
		if err != nil {
			return presentParams{}, fmt.Errorf("params.settings.%s: %w", name, err)
		}
		p.settings = append(p.settings, setting{name: name, value: value})
	}
	return p, nil
}

func parseReadParams(s *structpb.Struct) (readParams, error) {
	if s == nil {
		return readParams{}, errors.New("params: missing")
	}
	fields := s.GetFields()

	patterns, err := stringList(fields["patterns"])
	if err != nil {
		return readParams{}, fmt.Errorf("params.patterns: %w", err)
	}
	if len(patterns) == 0 {
		patterns = []string{"*"}
	}
	for i, pat := range patterns {
		if strings.ContainsAny(pat, " \t\r\n") {
			return readParams{}, fmt.Errorf("params.patterns[%d]: %q contains whitespace — every pattern is one argument", i, pat)
		}
	}
	return readParams{
		patterns:      patterns,
		revealSecrets: boolValue(fields["reveal_secrets"]),
	}, nil
}

// validParamName is a closed set. Every Redis configuration parameter is
// lower-case letters, digits and hyphens; the name is passed to the server as a
// bare argument, so it is validated rather than escaped — there is nothing to
// escape it with.
func validParamName(s string) bool {
	if s == "" || len(s) > 128 {
		return false
	}
	for i, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= '0' && r <= '9', r == '-':
			if i == 0 {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// scalarString accepts a string, a number or a bool and renders it the way
// Redis spells it. Writing `maxmemory: 0` in YAML is natural and arrives as a
// number; `appendonly: true` arrives as a bool and Redis wants "yes".
func scalarString(v *structpb.Value) (string, error) {
	switch k := v.GetKind().(type) {
	case *structpb.Value_StringValue:
		return k.StringValue, nil
	case *structpb.Value_NumberValue:
		if k.NumberValue == float64(int64(k.NumberValue)) {
			return strconv.FormatInt(int64(k.NumberValue), 10), nil
		}
		return strconv.FormatFloat(k.NumberValue, 'f', -1, 64), nil
	case *structpb.Value_BoolValue:
		if k.BoolValue {
			return "yes", nil
		}
		return "no", nil
	default:
		return "", errors.New("must be a string, a number or a boolean")
	}
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
