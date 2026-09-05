// probe-states of the redis plugin - read-probe operations on live Redis
// ENTIRELY via go-redis (without redis-cli/shell, capability only
// network_outbound). All state is read-only, changed=false CONSTRUCTIVE
// (use case core.http.probe/core.exec.run): This is a test, not a change.
// Output carries the probe result for health-gate (retry/until/failed_when) and for
// volatile where targeting (host role).
//
//	pinged - health-probe: go-redis PING -> expects PONG. Replaces idiom
//	         redis.command args:[PING] (Output.result == 'PONG'
//	         saved as result field - compatible with register.self.result).
//	role - role-probe: INFO replication -> instance role (master/slave).
//	         Replaces shell-idiom `redis-cli role | head -1 | tr -d '\n'` -
//	         volatile role for where targeting rolling-restart (ADR-008:
//	         the actual role is volatile, a live probe is taken before targeting).
//	replica-synced - restart health-gate replicas: INFO replication ->
//	         master_link_status == "up" (replica HAS RESYNCED with master).
//	         Stricter pinged (PONG = the demon is alive, but might not have caught up with the master yet);
//	         Output.synced bool + master_link_status as a diagnostic string.
//	         ONLY the replica has the master_link_status field - the master has it
//	         no: synced=false with explicit reason (not silent success), state
//	         intended for the slave path (restart block.where slave).
//	offset-synced - safety-gate of migration from an EXTERNAL source: "link is alive !=
//	         The data has been caught up." Checks slave_repl_offset of HIS instance with
//	         master_repl_offset EXTERNAL master (SECOND connection to source_addr
//	         with source secrets). caught_up=true only when link up + NO
//	         running full-sync (master_sync_in_progress==0) + lag
//	         (master - slave) <= lag_threshold. Opt. checking DBSIZE of both when
//	         !skip_checksum. Read-only, changed=false CONSTRUCTIVE.
//
// KRIT IB (ADR-010): params.password / source_password NEVER get into
// ApplyEvent.Message/.Output/errors. Connection errors are sanitized by redactError
// (for both passwords); the PING (PONG) response, role and offset are the server response, not
// operator secret.
package main

import (
	"context"
	"strconv"
	"strings"

	pluginv1 "github.com/souls-guild/soul-stack/proto/plugin/gen/go/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

// applyPinged - health-probe via go-redis PING. changed=false CONSTRUCTIVE
// (probe, not change): interpretation of "healthy/not" - at the scenario level via
// retry/until/failed_when by register.self.result. Output.result carries the answer
// server (PONG) - compatible with the previous redis.command args:[PING],
// which also put the answer in Output.result.
func (m *RedisModule) applyPinged(ctx context.Context, stream grpc.ServerStreamingServer[pluginv1.ApplyEvent], conn redisConn, params *structpb.Struct) error {
	password := stringOrEmpty(params.GetFields()["password"])
	// The connection has already been made openConn -> defaultConnect, which itself sends PING when
	// opening. Explicit PING is needed here to (1) separate the health-probe from the fact
	// connection and (2) put the server response in Output.result for health-gate.
	res, err := conn.Do(ctx, "PING")
	if err != nil {
		// PING error - server response (LOADING / MASTERDOWN /...): its arguments
		// they don't carry the password. redactError by password - defense-in-depth/uniformity
		// with applyRole/applyConfig (the driver could theoretically fail the connect cradle).
		return sendFailure(stream, "PING: "+redactError(err, password))
	}
	return sendOutcome(stream, false, "PING ok", map[string]any{
		"result": res,
	})
}

// applyRole - role-probe via INFO replication. Returns the actual
// (volatile) instance role in Output.role: "master" / "slave" (Redis values
// in INFO replication; redis-cli role-shell gave the same master/slave). changed=
// false CONSTRUCTIVE (probe). where targeting compares register.self.role ==
// 'master'/'slave' (ADR-008: the role is volatile, measured by a live probe).
//
// INFO replication selected instead of ROLE command: reuses parseInfoSection
// (replica.go) - typed "key:value" parsing, no fragile parsing
// ROLE array (first element). master_link_status is also available (bonus for
// diagnostics), but for where role is enough.
func (m *RedisModule) applyRole(ctx context.Context, stream grpc.ServerStreamingServer[pluginv1.ApplyEvent], conn redisConn, params *structpb.Struct) error {
	password := stringOrEmpty(params.GetFields()["password"])
	info, err := conn.Do(ctx, "INFO", "replication")
	if err != nil {
		return sendFailure(stream, "INFO replication: "+redactError(err, password))
	}
	repl := parseInfoSection(info)
	role := repl["role"]
	if role == "" {
		// INFO replication without the role field is an abnormal response (truncated INFO/
		// broken instance). Explicit fail, not an empty role in where (quietly no one
		// does not target -> silently skipping the restart).
		return sendFailure(stream, "INFO replication: role field is missing in response")
	}
	return sendOutcome(stream, false, "role: "+role, map[string]any{
		"role": role,
	})
}

// applyReplicaSynced - restart health-gate replicas: INFO replication ->
// master_link_status == "up" (the replica has RESYNCED with the master, not just
// "the demon responds with PONG"). changed=false CONSTRUCTIVE (read-probe). Output.synced
// bool + Output.master_link_status string for health-gate diagnostics
// (until:/failed_when: by register.self.synced).
//
// master/slave boundary: the master_link_status field is present in INFO replication
// ONLY the replica (role:slave) - the master does not have it. State is for
// slave paths (restart block.where slave). If there is no field (this is master or non-standard
// INFO) -> synced=false with explicit reason in Message (NOT silent success): otherwise
// The health-gate of the replica would silently pass on an instance that is not yet a replica.
func (m *RedisModule) applyReplicaSynced(ctx context.Context, stream grpc.ServerStreamingServer[pluginv1.ApplyEvent], conn redisConn, params *structpb.Struct) error {
	password := stringOrEmpty(params.GetFields()["password"])
	info, err := conn.Do(ctx, "INFO", "replication")
	if err != nil {
		return sendFailure(stream, "INFO replication: "+redactError(err, password))
	}
	repl := parseInfoSection(info)
	status, present := repl["master_link_status"]
	if !present {
		// master_link_status is missing - this is master (the master role does not have this field) or
		// abnormal answer. synced=false with reason, NOT silent success: health-gate
		// replicas must not pass on a non-replica.
		return sendOutcome(stream, false, "master_link_status is missing (the instance is not a replica - it is master or non-standard INFO)", map[string]any{
			"synced":             false,
			"master_link_status": "",
		})
	}
	synced := status == "up"
	return sendOutcome(stream, false, "master_link_status: "+status, map[string]any{
		"synced":             synced,
		"master_link_status": status,
	})
}

// validateOffsetSynced - static checks offset-synced: addr (own) +
// source_addr (external master) are required; lag_threshold (if set) >= 0.
func validateOffsetSynced(f map[string]*structpb.Value) []string {
	var errs []string
	errs = append(errs, validateAddr(f)...)
	if strings.TrimSpace(stringOrEmpty(f["source_addr"])) == "" {
		errs = append(errs, "params.source_addr: must be a non-empty string (host:port of the external master)")
	}
	if v := f["lag_threshold"]; v != nil && intOrDefault(v, 0) < 0 {
		errs = append(errs, "params.lag_threshold: must be an integer >= 0 (bytes)")
	}
	return errs
}

// applyOffsetSynced - safety-gate migration from an external source. conn -
// OWN instance (addr); the method itself opens a SECOND connection to the external master
// (source_addr) with source secrets (like cluster.go opens per-node conn).
// Read-only, changed=false CONSTRUCTIVE (probe).
//
// caught_up=true master_link_status=="up" AND master_sync_in_progress==0 AND
// (master_repl_offset - slave_repl_offset) <= lag_threshold. Any of the conditions
// false -> caught_up=false (success-event, NOT failed: health-gate decides itself through
// until:/failed_when: by register.self.caught_up). master_link_status/fields
// offset on the master/replica is missing from the opposite role - this is abnormal
// input (your addr is not a replica, or source_addr is not master): caught_up=false.
func (m *RedisModule) applyOffsetSynced(ctx context.Context, stream grpc.ServerStreamingServer[pluginv1.ApplyEvent], conn redisConn, params *structpb.Struct) error {
	f := params.GetFields()
	password := stringOrEmpty(f["password"])
	sourcePassword := stringOrEmpty(f["source_password"])
	skipChecksum := boolOrDefault(f["skip_checksum"], false)
	lagThreshold := intOrDefault(f["lag_threshold"], 0)

	// Own instance: slave_repl_offset, state of the link and running full-sync.
	selfInfo, err := conn.Do(ctx, "INFO", "replication")
	if err != nil {
		return sendFailure(stream, "INFO replication (self): "+redactError(err, password, sourcePassword))
	}
	selfRepl := parseInfoSection(selfInfo)
	slaveOffset, slaveOffsetOK := parseOffset(selfRepl["slave_repl_offset"])
	linkStatus := selfRepl["master_link_status"]
	syncInProgress := selfRepl["master_sync_in_progress"] == "1"

	// External master: SECOND connection with source secrets (source_addr/
	// source_password/source_tls*). master_repl_offset - authoritative "head".
	sourceCfg := connConfig{
		addr:     strings.TrimSpace(stringOrEmpty(f["source_addr"])),
		password: sourcePassword,
		tls:      parseSourceTLS(f),
	}
	sourceConn, err := m.openConn(ctx, sourceCfg)
	if err != nil {
		return sendFailure(stream, "connect source: "+redactError(err, password, sourcePassword, sourceCfg.tls.keyPEM))
	}
	defer func() { _ = sourceConn.Close() }()

	sourceInfo, err := sourceConn.Do(ctx, "INFO", "replication")
	if err != nil {
		return sendFailure(stream, "INFO replication (source): "+redactError(err, password, sourcePassword))
	}
	sourceRepl := parseInfoSection(sourceInfo)
	masterOffset, masterOffsetOK := parseOffset(sourceRepl["master_repl_offset"])

	// lag and caught_up. Without both offsets, lag is undefined -> caught_up=false
	// (abnormal input: your addr is not a replica, or source is not master).
	lagBytes := 0
	offsetsKnown := slaveOffsetOK && masterOffsetOK
	if offsetsKnown {
		lagBytes = masterOffset - slaveOffset
		if lagBytes < 0 {
			lagBytes = 0 // replica "ahead" (read-after-write window) - non-negative lag
		}
	}
	caughtUp := linkStatus == "up" && !syncInProgress && offsetsKnown && lagBytes <= lagThreshold

	output := map[string]any{
		"caught_up":               caughtUp,
		"lag_bytes":               int64(lagBytes),
		"master_sync_in_progress": syncInProgress,
	}

	// Opt. checksum - reconciliation of the sizes of both sets (rough sanity check on top
	// offset; offset - authority, DBSIZE - auxiliary signal). Does not affect
	// caught_up: different DBSIZE on the go is normal (TTL/eviction), but visible in Output.
	if !skipChecksum {
		dbSource, derr := dbSize(ctx, sourceConn)
		if derr != nil {
			return sendFailure(stream, "DBSIZE (source): "+redactError(derr, password, sourcePassword))
		}
		dbReplica, derr := dbSize(ctx, conn)
		if derr != nil {
			return sendFailure(stream, "DBSIZE (replica): "+redactError(derr, password, sourcePassword))
		}
		output["dbsize_source"] = dbSource
		output["dbsize_replica"] = dbReplica
	}

	return sendOutcome(stream, false, "caught_up: "+strconv.FormatBool(caughtUp)+", lag_bytes: "+strconv.Itoa(lagBytes), output)
}

// parseOffset parses the INFO replication offset field into an int. Empty/non-number -> (0,
// false): the field is not available for the opposite role (slave_repl_offset is not available for
// master; master_repl_offset on a replica reflects its own thread, not
// head of the source - therefore we take head from a SEPARATE connection to source).
func parseOffset(s string) (int, bool) {
	if s == "" {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return 0, false
	}
	return n, true
}

// dbSize reads DBSIZE via redisConn.Do (the number of keys in the current database). Answer -
// number is no secret.
func dbSize(ctx context.Context, conn redisConn) (int64, error) {
	res, err := conn.Do(ctx, "DBSIZE")
	if err != nil {
		return 0, err
	}
	n, convErr := strconv.ParseInt(strings.TrimSpace(res), 10, 64)
	if convErr != nil {
		return 0, nil // non-numeric response - best-effort 0 (do not fail probe)
	}
	return n, nil
}
