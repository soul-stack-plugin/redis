package main

import (
	"context"
	"errors"
	"testing"

	pluginv1 "github.com/souls-guild/soul-stack/proto/plugin/gen/go/v1"
)

// --- Validate: detached only requires addr ---

func TestValidate_DetachedRejectsEmptyAddr(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.replica().Validate(context.Background(), &pluginv1.ValidateRequest{
		State:  "detached",
		Params: mustStruct(t, map[string]any{"addr": ""}),
	})
	if reply.Ok {
		t.Fatal("waited Ok=false on empty addr (detached)")
	}
}

func TestValidate_DetachedHappyPath(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.replica().Validate(context.Background(), &pluginv1.ValidateRequest{
		State:  "detached",
		Params: mustStruct(t, map[string]any{"addr": "127.0.0.1:6379"}),
	})
	if !reply.Ok || len(reply.Errors) != 0 {
		t.Fatalf("waited Ok=true without errors, got %+v", reply)
	}
}

// --- Apply detached: slave -> master (REPLICAOF NO ONE, changed=true) ---

// TestApplyDetached_SlavePromotedToMaster - the instance was a replica -> REPLICAOF NO
// ONE, changed=true, previous_master carries the previous master host:port.
func TestApplyDetached_SlavePromotedToMaster(t *testing.T) {
	conn := &replConn{infoReply: "# Replication\r\n" +
		"role:slave\r\n" +
		"master_host:10.0.0.1\r\n" +
		"master_port:6379\r\n" +
		"master_link_status:up\r\n"}
	m := replModule(conn)
	stream := &applyStream{}

	err := m.replica().Apply(&pluginv1.ApplyRequest{
		State: "detached",
		Params: mustStruct(t, map[string]any{
			"addr":     "127.0.0.1:6379",
			"password": secretPass,
		}),
	}, stream)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil || fin.Failed || !fin.Changed {
		t.Fatalf("expected success changed=true (slave is promoted to master), got %+v", fin)
	}
	if !hasCall(conn.calls, "REPLICAOF", "NO", "ONE") {
		t.Errorf("no REPLICAOF NO ONE: %v", conn.calls)
	}
	if got := fin.GetOutput().GetFields()["previous_master"].GetStringValue(); got != "10.0.0.1:6379" {
		t.Errorf("previous_master=%q, waited 10.0.0.1:6379", got)
	}
	assertEventsNoSecret(t, stream)
}

// --- Apply detached: master -> no-op (idempotency) ---

// TestApplyDetached_AlreadyMasterNoOp - instance is already master -> changed=false,
// REPLICAOF is NOT called (idempotent, script replay safe).
func TestApplyDetached_AlreadyMasterNoOp(t *testing.T) {
	conn := &replConn{infoReply: "# Replication\r\nrole:master\r\nconnected_slaves:2\r\n"}
	m := replModule(conn)
	stream := &applyStream{}

	err := m.replica().Apply(&pluginv1.ApplyRequest{
		State: "detached",
		Params: mustStruct(t, map[string]any{
			"addr":     "127.0.0.1:6379",
			"password": secretPass,
		}),
	}, stream)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil || fin.Failed {
		t.Fatalf("expected success, got %+v", fin)
	}
	if fin.Changed {
		t.Error("waited changed=false: instance is already master (no-op idempotent)")
	}
	if hasCall(conn.calls, "REPLICAOF") {
		t.Errorf("no-op violated: REPLICAOF called on already-master: %v", conn.calls)
	}
	if got := fin.GetOutput().GetFields()["previous_master"].GetStringValue(); got != "" {
		t.Errorf("previous_master=%q, waited empty (already master)", got)
	}
}

// TestApplyDetached_InfoErrorIsFailure - INFO replication fell -> failed (not
// we touch the instance blindly without knowing the role).
func TestApplyDetached_InfoErrorIsFailure(t *testing.T) {
	conn := &replConn{failOnVerb: "INFO"}
	m := replModule(conn)
	stream := &applyStream{}

	_ = m.replica().Apply(&pluginv1.ApplyRequest{
		State:  "detached",
		Params: mustStruct(t, map[string]any{"addr": "127.0.0.1:6379"}),
	}, stream)

	fin := stream.final()
	if fin == nil || !fin.Failed {
		t.Fatalf("expected failed=true for INFO replication error, got %+v", fin)
	}
	if hasCall(conn.calls, "REPLICAOF") {
		t.Errorf("REPLICAOF should not be called when INFO: %v is not available", conn.calls)
	}
}

// TestApplyDetached_ConnectFailure_DoesNotLeakPassword - connection error with
// the password in the text is sanitized (IS-invariant ADR-010).
func TestApplyDetached_ConnectFailure_DoesNotLeakPassword(t *testing.T) {
	m := &RedisModule{
		connect: func(_ context.Context, cfg connConfig) (redisConn, error) {
			return nil, errors.New("dial failed for AUTH " + cfg.password)
		},
	}
	stream := &applyStream{}
	_ = m.replica().Apply(&pluginv1.ApplyRequest{
		State: "detached",
		Params: mustStruct(t, map[string]any{
			"addr":     "127.0.0.1:6379",
			"password": secretPass,
		}),
	}, stream)

	fin := stream.final()
	if fin == nil || !fin.Failed {
		t.Fatalf("waited failed=true, got %+v", fin)
	}
	assertEventsNoSecret(t, stream)
}
