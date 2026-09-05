// soul-mod-redis is a real SoulModule plugin for Soul Stack: the PRIMARY interface
// to live Redis in redis consolidation. A service scenario orchestrates
// order/targeting, the plugin executes ONE operation on one instance.
//
// The artifact serves SEVEN objects — address level 2 of `redis.<object>.<action>`,
// where level 1 is the alias an operator registers it under (ADR-020 amendment
// 2026-09-02):
//
//	command  — run (raw verb-action, changed=false by default)
//	instance — pinged / role-probed (read probe, changed=false by design, see
//	           probe.go), configured (CONFIG SET from a map)
//	acl      — reloaded (ACL LOAD — hot reload of the aclfile, changed by a diff
//	           of ACL LIST before/after)
//	user     — present / absent (ACL SETUSER / DELUSER on ONE user, see user.go)
//	replica  — present (REPLICAOF, see replica.go), detached (REPLICAOF NO ONE,
//	           see detach.go), synced / offset-synced (read probe, see probe.go)
//	cluster  — created / node-added / node-removed / resharded (cluster.go),
//	           external-joined / failed-over / external-forgotten (migrate.go)
//	sentinel — monitored (SENTINEL MONITOR/SET reconcile, see sentinel.go)
//
// The object tables and the dispatch live in object.go. failover — next batch.
//
// Intentionally without dry-run preview: plugin on BaseModule does NOT implement PlanReadSafe
// → host applies default-deny (on dry_run task gets honest "drift not
// supported", not false "no drift"). User decision 2026-06-22.
//
// Backend — github.com/redis/go-redis/v9. Address + password come from Keeper:
// password already resolved by render phase from vault-ref (ADR-012), plugin does NOT
// pull its own Vault client (capability — network_outbound only).
//
// CRITICAL SECURITY (ADR-010): params["password"] NEVER goes into ApplyEvent.Message,
// .Output, error text, or stderr. All connect-level errors are sanitized
// (redactError), command output is not (this is Redis response, not operator secret).
package main

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
	pluginv1 "github.com/souls-guild/soul-stack/proto/plugin/gen/go/v1"
	"github.com/souls-guild/soul-stack/sdk/module"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

// RedisModule is the Redis driver every object of this artifact delegates to — one
// body of code behind seven serving surfaces (see object.go). It is not itself a
// SoulModule: what the host dispatches to is an [object], which owns the actions it
// serves and calls the applyXxx methods below.
//
// BaseModule provides the no-op Plan (without PlanReadSafe → default-deny on
// dry_run) and intentionally does NOT implement ErrandReadSafe (default-deny on
// Errand): both behaviours are deliberate for this plugin, and [object] embeds
// BaseModule for the same reason.
type RedisModule struct {
	module.BaseModule

	// connect is an injection point for L0. nil → real redis.NewClient.
	connect func(ctx context.Context, cfg connConfig) (redisConn, error)
}

// redisConn is a narrow interface over *redis.Client (for L0 fake).
type redisConn interface {
	// Do executes an arbitrary command and returns string representation of
	// Redis response (or error). The string goes to ApplyEvent.Output — this is
	// server response, not operator secret.
	Do(ctx context.Context, args ...any) (string, error)
	// ConfigGet reads CONFIG GET <param> through the driver's TYPED path
	// (go-redis natively returns map[string]string). NOT through Do+strings.Fields:
	// multi-word values (for example save "900 1 300 10 60 10000") would be split
	// into mismatched pairs after space-join + Fields → false CONFIG SET and loss
	// of idempotency on day-2 update_config.
	ConfigGet(ctx context.Context, param string) (map[string]string, error)
	// GetKeysInSlot reads CLUSTER GETKEYSINSLOT <slot> <count> through
	// the driver's TYPED path ([]string natively). NOT through Do+strings.Fields:
	// Redis key is an arbitrary byte string and may contain space/\t/\n; after
	// space-join + Fields key "user 42" would split into two tokens → MIGRATE
	// nonexistent keys → key is NOT migrated, while SETSLOT NODE still gives away
	// the slot → DATA LOSS. Native path preserves separators (symmetry with ConfigGet).
	GetKeysInSlot(ctx context.Context, slot, count int) ([]string, error)
	// AclList reads ACL LIST through the driver's TYPED path ([]string — one
	// line per user). Used for diff before/after ACL LOAD (changed detection
	// for acl-state) and, per user, before/after ACL SETUSER (user.go).
	// NOT through Do+strings.Fields: each ACL line is a whole
	// rule ("user alice on >hash ~* +@all") with spaces; space-join + Fields
	// would split it into tokens → false diff. Native path preserves whole lines
	// (symmetry with ConfigGet/GetKeysInSlot).
	AclList(ctx context.Context) ([]string, error)
	Close() error
}

// connConfig is connection parameters. password and tls.*PEM are kept separate and
// NEVER logged or placed in events (security invariant ADR-010).
type connConfig struct {
	addr     string // host:port or unix:/path
	username string
	password string
	db       int
	tls      tlsParams // TLS parameters (enabled=false → plaintext connection)
}

// validateProbe — the check shared by every action whose only requirement is a
// reachable instance: PING, INFO replication, REPLICAOF NO ONE and ACL LOAD all
// take no arguments of their own.
func validateProbe(f map[string]*structpb.Value) []string {
	return validateAddr(f)
}

// validateCommand — static checks for `command.run`.
func validateCommand(f map[string]*structpb.Value) []string {
	errs := validateAddr(f)
	if len(stringList(f["args"])) == 0 {
		errs = append(errs, "params.args: must be a non-empty list (e.g. [\"PING\"])")
	}
	return errs
}

// validateConfig — static checks for `instance.configured`.
func validateConfig(f map[string]*structpb.Value) []string {
	errs := validateAddr(f)
	if len(stringMap(f["config"])) == 0 {
		errs = append(errs, "params.config: must be a non-empty map of directives")
	}
	return errs
}

// applyCommand executes raw command. changed is taken from params.changed (default false,
// probe semantics). Output carries result (Redis response). Password does not go into
// events (operator args + server result).
func (m *RedisModule) applyCommand(ctx context.Context, stream grpc.ServerStreamingServer[pluginv1.ApplyEvent], conn redisConn, params *structpb.Struct) error {
	f := params.GetFields()
	args := stringList(f["args"])
	if len(args) == 0 {
		return sendFailure(stream, "params.args: must be a non-empty list")
	}
	changed := boolOrDefault(f["changed"], false)

	cmdArgs := make([]any, len(args))
	for i, a := range args {
		cmdArgs[i] = a
	}
	res, err := conn.Do(ctx, cmdArgs...)
	if err != nil {
		// Redis error on command itself is its output, not a secret; but connection level
		// could carry password. We cannot redact by cfg.password here
		// (not available here), so redact only wrapper text. err from Do is server
		// response (WRONGPASS/ERR ...) and is safe.
		return sendFailure(stream, fmt.Sprintf("command %s: %v", args[0], err))
	}

	return sendOutcome(stream, changed, fmt.Sprintf("command %s ok", args[0]), map[string]any{
		"verb":   args[0],
		"result": res,
	})
}

// startupOnlyDirectives are redis.conf directives set ONLY at process startup:
// CONFIG SET rejects them ("Unknown option or number of arguments" /
// "can't set ... at runtime"). day-2 update_config renders FULL redis.conf
// (including these directives, needed on next process restart), but they cannot
// be applied to a LIVE instance. To avoid CONFIG SET failure, plugin
// SKIPS them (skip counter in Output) — hot-settable directives apply as usual. Changing
// startup-only directive takes effect on next process restart (triggered by
// hardening unit change, destiny redis server.yml). Set contains known
// startup-only Redis directives (port/listener/threading/cluster-bootstrap/file-layout/
// persistence names/modules/daemon/syslog/databases/logo).
var startupOnlyDirectives = map[string]bool{
	"port":                true,
	"tls-port":            true,
	"bind":                true,
	"unixsocket":          true,
	"unixsocketperm":      true,
	"io-threads":          true,
	"io-threads-do-reads": true,
	"cluster-enabled":     true,
	"cluster-config-file": true,
	"aclfile":             true,
	"logfile":             true,
	"pidfile":             true,
	"dir":                 true,
	"daemonize":           true,
	"supervised":          true,
	"dbfilename":          true,
	"loadmodule":          true,
	"syslog-enabled":      true,
	"syslog-ident":        true,
	"syslog-facility":     true,
	"databases":           true,
	"always-show-logo":    true,
	"set-proc-title":      true,
	"locale-collate":      true,
	"socket-mark-id":      true,
}

// applyConfig performs an honest diff: CONFIG GET current value of each directive,
// CONFIG SET only truly differing values (no-op → changed=false, idempotent like
// reconcileGlobals / cluster / replica / sentinel). Order is deterministic
// (sorted keys) for reproducible output. Optional CONFIG REWRITE
// runs only if at least one directive was applied (no drift between live and
// redis.conf that needs persistence). Directive values go into Output — this is
// redis config, not password; but error-path is sanitized with redactError by directive
// value (defense-in-depth: value could come from vault, e.g. requirepass).
//
// ★ Startup-only directives (startupOnlyDirectives) are SKIPPED: CONFIG SET rejects
// them, while day-2 renders FULL redis.conf (including them for next
// restart). Without skip, CONFIG SET dir/port/... would fail and break the run. Hot-settable
// directives from the same call apply normally; number skipped goes into Output
// (skipped), names go into a separate field for audit.
func (m *RedisModule) applyConfig(ctx context.Context, stream grpc.ServerStreamingServer[pluginv1.ApplyEvent], conn redisConn, params *structpb.Struct) error {
	f := params.GetFields()
	directives := stringMap(f["config"])
	if len(directives) == 0 {
		return sendFailure(stream, "params.config: must be a non-empty map of directives")
	}
	rewrite := boolOrDefault(f["rewrite"], false)

	applied := make([]string, 0, len(directives))
	skipped := make([]string, 0)
	for _, key := range sortedKeys(directives) {
		if startupOnlyDirectives[key] {
			// startup-only: CONFIG SET would reject → skip (takes effect on
			// process restart; see startupOnlyDirectives).
			skipped = append(skipped, key)
			continue
		}
		want := directives[key]
		current, err := configGet(ctx, conn, key)
		if err != nil {
			// redactError by directive value: defense-in-depth — value may have
			// come from vault (requirepass/masterauth), and driver error text may
			// echo it (symmetry with replica/cluster/sentinel error-path).
			return sendFailure(stream, fmt.Sprintf("CONFIG GET %s: %s", key, redactError(err, want)))
		}
		if current == want {
			continue // no-op: live already has the desired value
		}
		if _, err := conn.Do(ctx, "CONFIG", "SET", key, want); err != nil {
			return sendFailure(stream, fmt.Sprintf("CONFIG SET %s: %s", key, redactError(err, want)))
		}
		applied = append(applied, key)
	}

	if rewrite && len(applied) > 0 {
		if _, err := conn.Do(ctx, "CONFIG", "REWRITE"); err != nil {
			return sendFailure(stream, fmt.Sprintf("CONFIG REWRITE: %v", err))
		}
	}

	return sendOutcome(stream, len(applied) > 0, fmt.Sprintf("CONFIG SET applied: %s", strings.Join(applied, ",")), map[string]any{
		"applied":      strings.Join(applied, ","),
		"count":        int64(len(applied)),
		"rewrite":      rewrite && len(applied) > 0,
		"skipped":      strings.Join(skipped, ","),
		"skippedCount": int64(len(skipped)),
	})
}

// applyACL hot-reloads ACL: ACL LOAD forces Redis to reread aclfile
// in full (foundation of hot-reload wave; aclfile is already rendered by destiny before this
// step). Idempotent BY CONSTRUCTION: ACL LOAD reconciles live instance to
// declared file regardless of current state.
//
// changed semantics: ACL LOAD itself does not report "changed", so we do a cheap
// honest diff — ACL LIST before and after LOAD (typed path, []string with one
// rule per user). Equal → changed=false (live instance already matched
// file, no-op like config/cluster/sentinel); different → changed=true.
// Symmetry with config: compare live and reconcile to desired.
//
// Security: ACL rules (ACL LIST output) are NOT placed into Output — user line
// may carry password-hash (>hash / #sha256). Output carries only number of
// affected users and changed flag. error-path is sanitized
// (redactError) by cfg.* through the common Apply path; inside, no secrets.
func (m *RedisModule) applyACL(ctx context.Context, stream grpc.ServerStreamingServer[pluginv1.ApplyEvent], conn redisConn, _ *structpb.Struct) error {
	before, err := conn.AclList(ctx)
	if err != nil {
		return sendFailure(stream, fmt.Sprintf("ACL LIST (before): %v", err))
	}
	if _, err := conn.Do(ctx, "ACL", "LOAD"); err != nil {
		// ACL LOAD fails on broken aclfile / aclfile not configured — this is
		// Redis response (not operator secret), and connection-level secrets are already gone here.
		return sendFailure(stream, fmt.Sprintf("ACL LOAD: %v", err))
	}
	after, err := conn.AclList(ctx)
	if err != nil {
		return sendFailure(stream, fmt.Sprintf("ACL LIST (after): %v", err))
	}

	changed := !aclListEqual(before, after)
	return sendOutcome(stream, changed, "ACL LOAD ok", map[string]any{
		"users": int64(len(after)),
	})
}

// aclListEqual compares two ACL LIST outputs byte-for-byte (order matters: ACL LIST
// returns users deterministically in file load order, and after LOAD
// order reflects file). Any difference → ACL changed.
func aclListEqual(before, after []string) bool {
	if len(before) != len(after) {
		return false
	}
	for i := range before {
		if before[i] != after[i] {
			return false
		}
	}
	return true
}

// configGet reads CONFIG GET <param> → current value or "" (parameter empty/missing).
// Uses driver's TYPED ConfigGet (map[string]string), NOT
// Do+strings.Fields: multi-word values (save "900 1 300 10 60 10000") would
// split into mismatched pairs after space-join + Fields → false CONFIG SET.
func configGet(ctx context.Context, conn redisConn, param string) (string, error) {
	m, err := conn.ConfigGet(ctx, param)
	if err != nil {
		return "", err
	}
	return m[param], nil
}

func (m *RedisModule) openConn(ctx context.Context, cfg connConfig) (redisConn, error) {
	if m.connect != nil {
		return m.connect(ctx, cfg)
	}
	return defaultConnect(ctx, cfg)
}

func defaultConnect(ctx context.Context, cfg connConfig) (redisConn, error) {
	opts := &redis.Options{
		Username: cfg.username,
		Password: cfg.password,
		DB:       cfg.db,
	}
	// unix: prefix → unix socket; otherwise TCP host:port.
	if path, ok := strings.CutPrefix(cfg.addr, "unix:"); ok {
		opts.Network = "unix"
		opts.Addr = path
	} else {
		opts.Network = "tcp"
		opts.Addr = cfg.addr
	}
	// TLS: with tls=true go-redis connects over TLS (RootCAs/client-cert/
	// skip_verify from cfg.tls). This is REQUIRED for only-TLS (port 0): without
	// tls.Config go-redis sends plaintext and hits closed plain port.
	tlsCfg, err := buildTLSConfig(cfg.tls)
	if err != nil {
		return nil, err
	}
	opts.TLSConfig = tlsCfg
	c := redis.NewClient(opts)
	if err := c.Ping(ctx).Err(); err != nil {
		_ = c.Close()
		return nil, err
	}
	return &realConn{c: c}, nil
}

// realConn is a wrapper of *redis.Client for redisConn.
type realConn struct {
	c *redis.Client
}

func (r *realConn) Do(ctx context.Context, args ...any) (string, error) {
	res, err := r.c.Do(ctx, args...).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return "", nil
		}
		return "", err
	}
	return stringifyResult(res), nil
}

// ConfigGet is typed CONFIG GET through go-redis (map[string]string).
// Values are preserved whole, including multi-word ones (save). redis.Nil →
// empty map (parameter without value).
func (r *realConn) ConfigGet(ctx context.Context, param string) (map[string]string, error) {
	m, err := r.c.ConfigGet(ctx, param).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return map[string]string{}, nil
		}
		return nil, err
	}
	return m, nil
}

// GetKeysInSlot is typed CLUSTER GETKEYSINSLOT through go-redis ([]string).
// Keys are returned whole, including whitespace in name. redis.Nil →
// empty slice (slot emptied).
func (r *realConn) GetKeysInSlot(ctx context.Context, slot, count int) ([]string, error) {
	keys, err := r.c.ClusterGetKeysInSlot(ctx, slot, count).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, err
	}
	return keys, nil
}

// AclList is typed ACL LIST through go-redis ([]string, one line per
// user). Lines are preserved whole (ACL rule contains spaces).
// redis.Nil → empty slice (ACL not configured).
func (r *realConn) AclList(ctx context.Context) ([]string, error) {
	users, err := r.c.ACLList(ctx).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, err
	}
	return users, nil
}

func (r *realConn) Close() error { return r.c.Close() }

// stringifyResult converts Redis response to string for Output. Scalar → as-is,
// array → join with space (best-effort; command/config commands return simple
// responses: OK / PONG / value).
func stringifyResult(res any) string {
	switch v := res.(type) {
	case nil:
		return ""
	case string:
		return v
	case []any:
		parts := make([]string, 0, len(v))
		for _, e := range v {
			parts = append(parts, fmt.Sprintf("%v", e))
		}
		return strings.Join(parts, " ")
	default:
		return fmt.Sprintf("%v", v)
	}
}
