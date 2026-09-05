// replica-state of the redis plugin binds a Redis instance to a master
// (REPLICAOF) entirely via go-redis: INFO replication (diagnostics and
// idempotency) → REPLICAOF host port + CONFIG SET masterauth. No
// redis-cli/shell — the plugin's capability remains network_outbound only.
//
// Idempotent: if the instance is already replicating the desired master (role:slave,
// master_host/master_port match, master_link_status:up) → changed=false, no-op.
//
// addr == master_addr → no-op (guard IN PLUGIN) — defense-in-depth. REAL
// protection from "master replicating itself" is in the scenario `where:` (sentinel.yml step
// 4): the task is rendered ONLY on replicas (master excluded by SID). In prod,
// addr=127.0.0.1:6379, master_addr=primary_ip (e.g. 10.0.0.1) — addr is NEVER equal
// to master_addr on any host, so this guard does not trigger in prod;
// it catches only the degenerate addr==master_addr combination (test
// TestApplyReplica_SelfIsMasterNoOp), which does not occur in the scenario.
//
// source_external — binding to an EXTERNAL master (migration from another incarnation):
// self-guard is disabled, masterauth/masteruser are taken from master_password/
// master_username (source credentials).
//
// TLS-to-source (master_tls=true): the replication link to the source master
// uses TLS. On the Redis side this is controlled by the directive `tls-replication yes`
// — it switches the replica's OUTGOING replication connection to TLS mode. The directive
// is hot-settable, so the plugin sets it via CONFIG SET BEFORE REPLICAOF (alongside
// masterauth/masteruser): then the subsequent REPLICAOF establishes a TLS link, not
// plaintext. Without `tls-replication yes`, REPLICAOF to the TLS master of the source
// would fail against the source's TLS-only listener (handshake failure) — TODO S-batch removed.
//
// ★ Dependency on render/scenario (NOT handled by this state):trust of the source server
// certificate. To verify the source's server-cert during handshake, the replica
// reads the source's CA (master_tls_ca) from DISK — Redis directives tls-ca-cert-file /
// tls-ca-cert-dir accept a PATH, not inline-PEM, and CONFIG SET tls-ca-cert-file <path>
// requires the file to already be on disk. The plugin does not write files (capability —
// network_outbound), so the render (core.file.rendered) puts the source's CA on disk
// BEFORE the replica step, and also tells Redis the path via config-state (CONFIG SET
// tls-ca-cert-file). Contract for scenario-dev — in the step header and in README S3.
// master_tls_cert/master_tls_key (mTLS cert of the replica on the link) are similarly read
// by Redis from a path (tls-cert-file/tls-key-file of its own instance) — they are placed there by the same
// render; this state does not operate with their values, only enables tls-replication.
package main

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"

	pluginv1 "github.com/souls-guild/soul-stack/proto/plugin/gen/go/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

// validateReplica performs static validation of replica params (text without passwords).
func validateReplica(f map[string]*structpb.Value) []string {
	var errs []string
	errs = append(errs, validateAddr(f)...)
	if strings.TrimSpace(stringOrEmpty(f["master_addr"])) == "" {
		errs = append(errs, "params.master_addr: must be a non-empty string (host:port of the master)")
	}
	return errs
}

// applyReplica sets an instance to be a replica of the specified master. addr is this
// instance (local on the redis host, 127.0.0.1:6379); master_addr is the host-
// invariant address of the cluster master (one per cluster). The master does NOT replicate
// itself: addr == master_addr → no-op.
//
// source_external=true (master_addr is an EXTERNAL source, not own incarnation):
// (1) self-guard addr==master_addr is DISABLED (external master never coincides
// with its own addr by design, and if it coincidentally matches — that's user error, not
// the intended no-op that would silently ignore the binding); (2) idempotency is checked
// against the external source's master_host/master_port (same as for own incarnation —
// INFO replication fields are identical); (3) masterauth is taken from master_password
// (SOURCE password), masteruser from master_username; own password/username do not apply
// to the external source.
func (m *RedisModule) applyReplica(ctx context.Context, stream grpc.ServerStreamingServer[pluginv1.ApplyEvent], conn redisConn, params *structpb.Struct) error {
	f := params.GetFields()
	password := stringOrEmpty(f["password"])
	sourceExternal := boolOrDefault(f["source_external"], false)
	masterTLS := boolOrDefault(f["master_tls"], false)
	masterAddr := strings.TrimSpace(stringOrEmpty(f["master_addr"]))

	masterHost, masterPort, err := net.SplitHostPort(masterAddr)
	if err != nil {
		return sendFailure(stream, fmt.Sprintf("params.master_addr %q: %v", masterAddr, err))
	}
	if _, convErr := strconv.Atoi(masterPort); convErr != nil {
		return sendFailure(stream, fmt.Sprintf("params.master_addr %q: invalid port", masterAddr))
	}

	// master does NOT replicate itself: scenario calls replica on ALL hosts
	// (including the selected master), guard here makes the master call a no-op. For
	// external sources (source_external), this guard is DISABLED: master_addr is
	// an external instance, not one of our hosts, a no-op based on address match would be
	// semantically incorrect.
	if !sourceExternal {
		selfAddr := strings.TrimSpace(stringOrEmpty(f["addr"]))
		if sameRedisEndpoint(selfAddr, masterAddr) {
			return sendOutcome(stream, false, "this instance is the master (no-op)", map[string]any{
				"role":   "master",
				"master": masterAddr,
			})
		}
	}

	// masterauth/masteruser: for external sources — SOURCE credentials
	// (master_password/master_username); for own incarnation — shared
	// password/username (master is the same service, same creds). redactError for
	// ALL passwords in context (own + master_password) — security invariant ADR-010.
	masterAuth := password
	masterUser := stringOrEmpty(f["username"])
	masterPass := stringOrEmpty(f["master_password"])
	if sourceExternal {
		masterAuth = masterPass
		masterUser = stringOrEmpty(f["master_username"])
	}

	// Idempotency: already replicating the desired master with healthy link → no-op.
	info, err := conn.Do(ctx, "INFO", "replication")
	if err != nil {
		return sendFailure(stream, "INFO replication: "+redactError(err, password, masterPass))
	}
	repl := parseInfoSection(info)
	if repl["role"] == "slave" &&
		repl["master_host"] == masterHost &&
		repl["master_port"] == masterPort &&
		repl["master_link_status"] == "up" {
		return sendOutcome(stream, false, "already replica of master (no-op)", map[string]any{
			"role":   "slave",
			"master": masterAddr,
		})
	}

	// tls-replication BEFORE REPLICAOF: switches the replica's OUTGOING replication link to
	// TLS. Otherwise REPLICAOF to a TLS-only source (master_tls=true) would fail against its
	// TLS listener (handshake failure). Only for source_external (own master lives in
	// same incarnation — TLS mode is already set by common redis.conf at startup,
	// no need to enable it separately). CONFIG SET is idempotent on Redis side;
	// directive is hot-settable. Source's server cert is verified via CA from DISK
	// (tls-ca-cert-file/-dir is set by render via config-state — see header).
	if sourceExternal && masterTLS {
		if _, err := conn.Do(ctx, "CONFIG", "SET", "tls-replication", "yes"); err != nil {
			return sendFailure(stream, "CONFIG SET tls-replication: "+redactError(err, password, masterPass))
		}
	}

	// masterauth BEFORE REPLICAOF: replica must know the master's password, otherwise
	// synchronization fails on AUTH. CONFIG SET is idempotent on Redis side.
	// Empty masterAuth — no AUTH requirement on master: do not set masterauth.
	if masterAuth != "" {
		if _, err := conn.Do(ctx, "CONFIG", "SET", "masterauth", masterAuth); err != nil {
			return sendFailure(stream, "CONFIG SET masterauth: "+redactError(err, password, masterPass))
		}
	}
	if masterUser != "" {
		if _, err := conn.Do(ctx, "CONFIG", "SET", "masteruser", masterUser); err != nil {
			return sendFailure(stream, "CONFIG SET masteruser: "+redactError(err, password, masterPass))
		}
	}

	if _, err := conn.Do(ctx, "REPLICAOF", masterHost, masterPort); err != nil {
		return sendFailure(stream, fmt.Sprintf("REPLICAOF %s: %s", masterAddr, redactError(err, password, masterPass)))
	}

	return sendOutcome(stream, true, "instance set as replica of master", map[string]any{
		"role":   "slave",
		"master": masterAddr,
	})
}

// sameRedisEndpoint checks if two host:port addresses point to the same Redis instance.
// Comparison is on the normalized pair (host, port); invalid addr → false (conservatively
// for the no-op guard: do not consider as master what did not parse correctly).
func sameRedisEndpoint(a, b string) bool {
	ah, ap, aerr := net.SplitHostPort(a)
	bh, bp, berr := net.SplitHostPort(b)
	if aerr != nil || berr != nil {
		return false
	}
	return ah == bh && ap == bp
}

// parseInfoSection parses INFO output (or a single section, e.g., INFO
// replication) into a "key:value" map line by line. Section headers (# Replication) and
// empty lines are ignored. CRLF line endings from Redis are trimmed.
func parseInfoSection(s string) map[string]string {
	out := map[string]string{}
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimRight(line, "\r")
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		out[strings.TrimSpace(k)] = strings.TrimSpace(v)
	}
	return out
}
