// detached-state of the redis plugin - detaching an instance from the master
// (REPLICAOF NO ONE) ENTIRELY via go-redis: INFO replication (diagnostics and
// idempotency) -> REPLICAOF NO ONE. NO redis-cli/shell - capability
// The only plugin left is network_outbound.
//
// Promotion of a former replica to an independent master is the final step of migration from
// external source (after offset-synced confirmed that the data has been caught up): tear
// replication, the instance becomes an autonomous master.
//
// Idempotent: INFO replication -> role==master already -> no-op (changed=false). This
// makes the step safe to repeat (rerunning the script on an already promoted instance does not
// will "promote" it again, just confirms the role).
package main

import (
	"context"

	pluginv1 "github.com/souls-guild/soul-stack/proto/plugin/gen/go/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

// applyDetached unbinds the instance from the master (REPLICAOF NO ONE), promoting it to
// independent master. Idempotent: already master (role==master in INFO
// replication) -> changed=false, no-op. Output.previous_master carries the previous one
// master_host:master_port (for auditing/diagnostics) is the server address, not a secret.
func (m *RedisModule) applyDetached(ctx context.Context, stream grpc.ServerStreamingServer[pluginv1.ApplyEvent], conn redisConn, params *structpb.Struct) error {
	password := stringOrEmpty(params.GetFields()["password"])

	info, err := conn.Do(ctx, "INFO", "replication")
	if err != nil {
		return sendFailure(stream, "INFO replication: "+redactError(err, password))
	}
	repl := parseInfoSection(info)

	// Already master -> there is nothing to untie (no-op, idempotent). REPLICAOF NO ONE on
	// master Redis accepts silently, but an honest probe-skip avoids a false one
	// changed=true and an extra command.
	if repl["role"] == "master" {
		return sendOutcome(stream, false, "instance is already master (no-op)", map[string]any{
			"changed":         false,
			"previous_master": "",
		})
	}

	// The previous master for the report (the replica has the fields; "" if for some reason
	// absent - does not block promotion).
	previousMaster := ""
	if host := repl["master_host"]; host != "" {
		previousMaster = host
		if port := repl["master_port"]; port != "" {
			previousMaster = host + ":" + port
		}
	}

	if _, err := conn.Do(ctx, "REPLICAOF", "NO", "ONE"); err != nil {
		return sendFailure(stream, "REPLICAOF NO ONE: "+redactError(err, password))
	}

	return sendOutcome(stream, true, "instance detached from master (promoted to master)", map[string]any{
		"changed":         true,
		"previous_master": previousMaster,
	})
}
