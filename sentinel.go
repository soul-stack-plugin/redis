// sentinel-state redis plugin - hot configuration reconciliation
// Redis Sentinel ENTIRELY via go-redis: SENTINEL MASTERS/MASTER (diff) ->
// SENTINEL MONITOR/REMOVE+MONITOR (monitor) -> SENTINEL SET (per-master) ->
// SENTINEL CONFIG GET/SET (globals). NO redis-cli/shell - capability
// The only plugin left is network_outbound.
//
// The algorithm reconciles Sentinel state in three phases
// (classify_config / compute_monitor_action / compute_set_updates): top-level
// CONFIG in Sentinel mode is NOT supported, so global options go
// via SENTINEL CONFIG SET, and the master parameters via SENTINEL SET.
//
// Idempotent: the monitor is not touched if master is already at the desired address; SET/
// CONFIG SET are applied only if there is a real difference from the current one.
//
// KRIT IB (ADR-010): auth_pass / sentinel-pass NEVER get into
// ApplyEvent.Message/.Output/errors. Output carries only the names of applied
// actions, not their secret meanings.
package main

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	pluginv1 "github.com/souls-guild/soul-stack/proto/plugin/gen/go/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

// defaultMasterName - the default name of the monitored master (as in role: mymaster).
const defaultMasterName = "mymaster"

// sentinelGlobalParams - global parameters Sentinel accepted by SENTINEL
// CONFIG SET (one word after "sentinel"). Transfer SENTINEL_GLOBAL_PARAMS.
var sentinelGlobalParams = map[string]bool{
	"announce-ip":        true,
	"announce-port":      true,
	"resolve-hostnames":  true,
	"announce-hostnames": true,
	"sentinel-user":      true,
	"sentinel-pass":      true,
}

// secretGlobalParams - global secret parameters: SENTINEL CONFIG GET
// returns them empty, diff is not possible. We don't do rotation in the create slice, so
// they are simply not applied as globals (sentinel-pass is set via the monitor
// when created, like auth-pass). Transfer SECRET_GLOBAL_PARAMS.
var secretGlobalParams = map[string]bool{"sentinel-pass": true}

// validateSentinel - static checks of sentinel-params (texts without secrets).
func validateSentinel(f map[string]*structpb.Value) []string {
	var errs []string
	errs = append(errs, validateAddr(f)...)
	// A Sentinel has no keyspace and refuses SELECT ([stateHasKeyspace]), so a
	// non-zero db is not a misconfiguration - it makes the connection
	// unopenable. Refused here rather than left to a connect error, which reads
	// as an unreachable Sentinel. The key stays declared in the manifest: since
	// NIM-204 an undeclared param fails the task, so removing it would break
	// every scenario passing the documented `db: 0`.
	if db := intOrDefault(f["db"], 0); db != 0 {
		errs = append(errs, fmt.Sprintf("params.db: must be 0 on sentinel (got %d) - a Sentinel serves no keyspace and refuses SELECT", db))
	}
	if mon := nodeSpec(f["monitor"]); len(mon) > 0 {
		if strings.TrimSpace(stringOrEmpty(mon["ip"])) == "" {
			errs = append(errs, "params.monitor.ip: must be a non-empty string")
		}
		// `monitor` is declared a map and nothing declares what is inside it, so
		// the object's type gate reaches the map and stops (params.go): these two
		// carry the rule the rest of the way themselves.
		if port, err := intField(mon, "port", "params.monitor.port", 0); err != nil {
			errs = append(errs, err.Error())
		} else if port <= 0 {
			errs = append(errs, "params.monitor.port: must be a positive integer")
		}
		// quorum is optional (default 1 in reconcileMonitor), but IF specified - must
		// be >=1: SENTINEL MONITOR... 0 is rejected by Redis (symmetrical port>0).
		if q, err := intField(mon, "quorum", "params.monitor.quorum", 1); err != nil {
			errs = append(errs, err.Error())
		} else if q < 1 {
			errs = append(errs, "params.monitor.quorum: must be a positive integer (>= 1)")
		}
	}
	return errs
}

// applySentinel reconciles one Sentinel instance to the desired state.
func (m *RedisModule) applySentinel(ctx context.Context, stream grpc.ServerStreamingServer[pluginv1.ApplyEvent], conn redisConn, params *structpb.Struct) error {
	f := params.GetFields()
	authPass := stringOrEmpty(f["auth_pass"])
	authUser := stringOrEmpty(f["auth_user"])
	masterName := strings.TrimSpace(stringOrEmpty(f["master_name"]))
	if masterName == "" {
		masterName = defaultMasterName
	}
	redisVersion := stringOrEmpty(f["redis_version"])

	globalsAll, perMaster := classifyConfig(stringMap(f["config"]), masterName)
	globals := supportedGlobals(globalsAll, redisVersion)

	monitor := nodeSpec(f["monitor"])

	changed := false
	var actions []string

	if len(monitor) > 0 {
		mChanged, err := reconcileMonitor(ctx, conn, masterName, monitor, authUser, authPass, &actions)
		if err != nil {
			return sendFailure(stream, redactError(err, authPass))
		}
		changed = changed || mChanged

		pChanged, err := reconcileMasterParams(ctx, conn, masterName, perMaster, monitor, &actions)
		if err != nil {
			return sendFailure(stream, redactError(err, authPass))
		}
		changed = changed || pChanged
	}

	// SENTINEL CONFIG GET/SET (entire reconcileGlobals mechanism) appeared in Redis
	// 7.0: on 6.2 ANY global will fail apply on the first CONFIG GET. At <7.0
	// skip all globals entirely (no-op) - NOT just loglevel; non-empty set
	// We mark it with a warning action so that the operator sees the empty desired one.
	if sentinelGlobalsSupported(redisVersion) {
		gChanged, err := reconcileGlobals(ctx, conn, globals, &actions)
		if err != nil {
			return sendFailure(stream, redactError(err, authPass))
		}
		changed = changed || gChanged
	} else if len(globals) > 0 {
		actions = append(actions, "globals_skipped_pre_7.0")
	}

	return sendOutcome(stream, changed, fmt.Sprintf("sentinel reconciled: %s", strings.Join(actions, ",")), map[string]any{
		"master_name": masterName,
		"actions":     strings.Join(actions, ","),
	})
}

// sentinelGlobalsSupported - whether the version of SENTINEL CONFIG GET/SET (>=7.0) can be used.
// This is the reconcileGlobals mechanism as a whole, not a separate parameter. Empty/Unknown
// version -> false (fail-closed: on an unknown version we do not touch globals).
func sentinelGlobalsSupported(redisVersion string) bool {
	return versionGE(redisVersion, [3]int{7, 0, 0})
}

// classifyConfig decomposes sentinel.conf directives into (globals, per_master).
// Pure function (transfer classify_config):
//   - plain "loglevel" -> global;
//   - "sentinel <param>" (one word) -> global;
//   - "sentinel <opt> <master>" -> per-master, only for our master_name;
//   - the rest (dir/port/tls-*/pidfile - startup-only) is ignored.
func classifyConfig(config map[string]string, masterName string) (globals, perMaster map[string]string) {
	globals = map[string]string{}
	perMaster = map[string]string{}
	for key, value := range config {
		if key == "loglevel" {
			globals["loglevel"] = value
			continue
		}
		if !strings.HasPrefix(key, "sentinel ") {
			continue
		}
		tokens := strings.Fields(strings.TrimSpace(key[len("sentinel "):]))
		switch {
		case len(tokens) == 1:
			globals[tokens[0]] = value
		case len(tokens) == 2 && tokens[1] == masterName:
			perMaster[tokens[0]] = value
		}
	}
	return globals, perMaster
}

// supportedGlobals leaves only global parameters that SENTINEL can handle
// CONFIG SET on this version (supported_globals port). loglevel in Sentinel -
// only with Redis 7.0; we drop it to 6.2. Secret globals (sentinel-pass)
// discard them right away - diff on them is impossible, there is no rotation in the create slice.
func supportedGlobals(globals map[string]string, redisVersion string) map[string]string {
	out := map[string]string{}
	for key, value := range globals {
		if key == "loglevel" {
			if versionGE(redisVersion, [3]int{7, 0, 0}) {
				out[key] = value
			}
			continue
		}
		if secretGlobalParams[key] {
			continue
		}
		if sentinelGlobalParams[key] {
			out[key] = value
		}
	}
	return out
}

// computeMonitorAction - what to do with the monitor: "add" (there is no such master),
// "readd" (address changed), "none" (transfer compute_monitor_action). current -
// map fields->value from SENTINEL MASTER (or nil if master is unknown).
func computeMonitorAction(current map[string]string, ip, port string) string {
	if current == nil {
		return "add"
	}
	if current["ip"] != ip || current["port"] != port {
		return "readd"
	}
	return "none"
}

// computeSetUpdates computes options different from the current one (transfer
// compute_set_updates): {option: new-value} for those where cur is missing or
// didn't match. Returns a sorted list of keys for SET determinism.
func computeSetUpdates(desired, current map[string]string) (keys []string, values map[string]string) {
	values = map[string]string{}
	for opt, value := range desired {
		if cur, ok := current[opt]; !ok || cur != value {
			values[opt] = value
		}
	}
	keys = make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys, values
}

// reconcileMonitor brings the monitor to the desired address (transfer
// reconcile_monitor). add/readd -> SENTINEL MONITOR; readd first REMOVE. auth
// on master is set when creating/recreating a monitor (the new monitor has
// no). Returns changed (monitor created/recreated).
func reconcileMonitor(ctx context.Context, conn redisConn, masterName string, monitor map[string]*structpb.Value, authUser, authPass string, actions *[]string) (bool, error) {
	ip := strings.TrimSpace(stringOrEmpty(monitor["ip"]))
	portNum, err := intField(monitor, "port", "params.monitor.port", 0)
	if err != nil {
		return false, err
	}
	port := strconv.Itoa(portNum)
	quorum, err := intField(monitor, "quorum", "params.monitor.quorum", 1)
	if err != nil {
		return false, err
	}

	current, err := sentinelMaster(ctx, conn, masterName)
	if err != nil {
		return false, fmt.Errorf("SENTINEL MASTER %s: %w", masterName, err)
	}
	action := computeMonitorAction(current, ip, port)

	if action == "readd" {
		if _, err := conn.Do(ctx, "SENTINEL", "REMOVE", masterName); err != nil {
			return false, fmt.Errorf("SENTINEL REMOVE %s: %w", masterName, err)
		}
		*actions = append(*actions, "sentinel_remove")
	}
	if action == "add" || action == "readd" {
		if _, err := conn.Do(ctx, "SENTINEL", "MONITOR", masterName, ip, port, strconv.Itoa(quorum)); err != nil {
			return false, fmt.Errorf("SENTINEL MONITOR %s: %w", masterName, err)
		}
		*actions = append(*actions, "sentinel_monitor")

		// auth on master - only for the new/recreated monitor (it does not have them).
		if authUser != "" {
			if _, err := conn.Do(ctx, "SENTINEL", "SET", masterName, "auth-user", authUser); err != nil {
				return false, fmt.Errorf("SENTINEL SET %s auth-user: %w", masterName, err)
			}
		}
		if authPass != "" {
			if _, err := conn.Do(ctx, "SENTINEL", "SET", masterName, "auth-pass", authPass); err != nil {
				return false, fmt.Errorf("SENTINEL SET %s auth-pass: %w", masterName, err)
			}
		}
		if authUser != "" || authPass != "" {
			*actions = append(*actions, "sentinel_set_auth")
		}
	}

	return action == "add" || action == "readd", nil
}

// reconcileMasterParams updates the master parameters via SENTINEL SET (transfer
// reconcile_master_params). quorum from monitor is added to the desired one. Apply
// only really different ones (compute_set_updates). Returns changed.
func reconcileMasterParams(ctx context.Context, conn redisConn, masterName string, perMaster map[string]string, monitor map[string]*structpb.Value, actions *[]string) (bool, error) {
	desired := make(map[string]string, len(perMaster)+1)
	for k, v := range perMaster {
		desired[k] = v
	}
	if monitor["quorum"] != nil {
		q, err := intField(monitor, "quorum", "params.monitor.quorum", 1)
		if err != nil {
			return false, err
		}
		desired["quorum"] = strconv.Itoa(q)
	}
	if len(desired) == 0 {
		return false, nil
	}

	current, err := sentinelMaster(ctx, conn, masterName)
	if err != nil {
		return false, fmt.Errorf("SENTINEL MASTER %s: %w", masterName, err)
	}
	keys, values := computeSetUpdates(desired, current)
	for _, opt := range keys {
		if _, err := conn.Do(ctx, "SENTINEL", "SET", masterName, opt, values[opt]); err != nil {
			return false, fmt.Errorf("SENTINEL SET %s %s: %w", masterName, opt, err)
		}
	}
	if len(keys) > 0 {
		*actions = append(*actions, "sentinel_set")
	}
	return len(keys) > 0, nil
}

// reconcileGlobals updates global parameters via SENTINEL CONFIG SET
// (transfer reconcile_globals). Applies only if different from the current one
// (SENTINEL CONFIG GET). Secret globals do not reach here (filtered
// supportedGlobals). Returns changed. Directive names are deterministic (sort).
func reconcileGlobals(ctx context.Context, conn redisConn, globals map[string]string, actions *[]string) (bool, error) {
	keys := make([]string, 0, len(globals))
	for k := range globals {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	changed := false
	for _, param := range keys {
		value := globals[param]
		current, err := sentinelConfigGet(ctx, conn, param)
		if err != nil {
			return changed, fmt.Errorf("SENTINEL CONFIG GET %s: %w", param, err)
		}
		if current != value {
			if _, err := conn.Do(ctx, "SENTINEL", "CONFIG", "SET", param, value); err != nil {
				return changed, fmt.Errorf("SENTINEL CONFIG SET %s: %w", param, err)
			}
			changed = true
		}
	}
	if changed {
		*actions = append(*actions, "sentinel_config_set")
	}
	return changed, nil
}

// sentinelMaster reads SENTINEL MASTER <name> into the map field->value. Answer Redis
// - flat array [field, value,...]; redisConn.Do stringifies it as
// join with a space, so we parse pairs of tokens. If master is unknown (Redis
// responds with the error "No such master") - return nil (for action=add).
func sentinelMaster(ctx context.Context, conn redisConn, name string) (map[string]string, error) {
	res, err := conn.Do(ctx, "SENTINEL", "MASTER", name)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "no such master") {
			return nil, nil
		}
		return nil, err
	}
	if strings.TrimSpace(res) == "" {
		return nil, nil
	}
	return pairsToMap(strings.Fields(res)), nil
}

// sentinelConfigGet reads SENTINEL CONFIG GET <param> -> value or "" (carry
// sentinel_config_get). The answer is the pair [param, value]; "" if the parameter is empty/no.
func sentinelConfigGet(ctx context.Context, conn redisConn, param string) (string, error) {
	res, err := conn.Do(ctx, "SENTINEL", "CONFIG", "GET", param)
	if err != nil {
		return "", err
	}
	return pairsToMap(strings.Fields(res))[param], nil
}

// pairsToMap collapses the flat slice [k0, v0, k1, v1,...] into a map. Odd
// the tail is ignored. Used to parse the SENTINEL response array.
func pairsToMap(tokens []string) map[string]string {
	out := make(map[string]string, len(tokens)/2)
	for i := 0; i+1 < len(tokens); i += 2 {
		out[tokens[i]] = tokens[i+1]
	}
	return out
}

// versionGE - version >= target by (major, minor, patch). Transfer parse_version/
// version_ge: from each dot chunk we take the leading digits (8.0.3, v=8.0.3, 6.2 ->
// (8,0,3)/(8,0,3)/(6,2,0)). Empty version -> (0,0,0): version-gated parameters
// on an unknown version, fail-closed are discarded.
func versionGE(version string, target [3]int) bool {
	v := versionTuple(version)
	rank := func(t [3]int) int { return t[0]*1_000_000 + t[1]*1_000 + t[2] }
	return rank(v) >= rank(target)
}

// versionTuple parses the version string into (major, minor, patch). Normalizes
// distro-native form before parsing:
//   - epoch "N:" is cut off (deb-form "5:7.0.15-1~deb12u7" -> major=7, NOT 5);
//   - revision tail "-..." is cut off ("7.0.15-1~deb12u7" -> "7.0.15");
//   - the prefix "v" is ignored (we take the leading digits from each chunk).
func versionTuple(version string) [3]int {
	version = strings.TrimSpace(version)
	if i := strings.IndexByte(version, ':'); i >= 0 {
		version = version[i+1:] // cut off epoch "N:"
	}
	if i := strings.IndexByte(version, '-'); i >= 0 {
		version = version[:i] // cut off revision "-..."
	}

	var out [3]int
	for i, chunk := range strings.Split(version, ".") {
		if i >= 3 {
			break
		}
		digits := ""
		for _, ch := range chunk {
			if ch >= '0' && ch <= '9' {
				digits += string(ch)
			} else if digits != "" {
				break
			}
		}
		if digits != "" {
			out[i], _ = strconv.Atoi(digits)
		}
	}
	return out
}
