package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	pluginv1 "github.com/souls-guild/soul-stack/proto/plugin/gen/go/v1"
)

// --- Validate: pinged / role only require addr ---

func TestValidate_PingedRejectsEmptyAddr(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.instance().Validate(context.Background(), &pluginv1.ValidateRequest{
		State:  "pinged",
		Params: mustStruct(t, map[string]any{"addr": ""}),
	})
	if reply.Ok {
		t.Fatal("waited Ok=false on empty addr (pinged)")
	}
}

func TestValidate_PingedHappyPath(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.instance().Validate(context.Background(), &pluginv1.ValidateRequest{
		State:  "pinged",
		Params: mustStruct(t, map[string]any{"addr": "127.0.0.1:6379"}),
	})
	if !reply.Ok || len(reply.Errors) != 0 {
		t.Fatalf("waited Ok=true without errors, got %+v", reply)
	}
}

func TestValidate_RoleRejectsEmptyAddr(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.instance().Validate(context.Background(), &pluginv1.ValidateRequest{
		State:  "role-probed",
		Params: mustStruct(t, map[string]any{"addr": ""}),
	})
	if reply.Ok {
		t.Fatal("waited Ok=false on empty addr (role)")
	}
}

func TestValidate_RoleHappyPath(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.instance().Validate(context.Background(), &pluginv1.ValidateRequest{
		State:  "role-probed",
		Params: mustStruct(t, map[string]any{"addr": "127.0.0.1:6379"}),
	})
	if !reply.Ok || len(reply.Errors) != 0 {
		t.Fatalf("waited Ok=true without errors, got %+v", reply)
	}
}

func TestValidate_ReplicaSyncedRejectsEmptyAddr(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.replica().Validate(context.Background(), &pluginv1.ValidateRequest{
		State:  "synced",
		Params: mustStruct(t, map[string]any{"addr": ""}),
	})
	if reply.Ok {
		t.Fatal("waited Ok=false on an empty addr (replica-synced)")
	}
}

func TestValidate_ReplicaSyncedHappyPath(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.replica().Validate(context.Background(), &pluginv1.ValidateRequest{
		State:  "synced",
		Params: mustStruct(t, map[string]any{"addr": "127.0.0.1:6379"}),
	})
	if !reply.Ok || len(reply.Errors) != 0 {
		t.Fatalf("waited Ok=true without errors, got %+v", reply)
	}
}

// --- Apply: pinged ---

// TestApplyPinged_HappyPath_PongChangedFalse - PING -> Output.result == PONG,
// changed=false CONSTRUCTIVE (probe semantics). The result field is compatible with the previous one
// redis.command args:[PING] (register.self.result in health-gate).
func TestApplyPinged_HappyPath_PongChangedFalse(t *testing.T) {
	conn := &fakeConn{results: map[string]string{"PING": "PONG"}}
	m := newModule(conn)
	stream := &applyStream{}

	err := m.instance().Apply(&pluginv1.ApplyRequest{
		State: "pinged",
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
		t.Fatalf("were waiting for a successful final event, got %+v", fin)
	}
	if fin.Changed {
		t.Error("pinged: changed must be false constructively (probe, not change)")
	}
	if got := fin.GetOutput().GetFields()["result"].GetStringValue(); got != "PONG" {
		t.Errorf("Output.result=%q, expected PONG (compatible with register.self.result)", got)
	}
	// The password went into the connection, but NOT into the PING arguments.
	if conn.cfg.password != secretPass {
		t.Errorf("the password did not reach the connection: %q", conn.cfg.password)
	}
	assertNoCommandCarriesSecret(t, conn)
	assertEventsNoSecret(t, stream)
	if !conn.closed {
		t.Error("connection not closed")
	}
}

// TestApplyPinged_RedisErrorIsFailure - PING with a server error (LOADING, etc.)
// -> failed=true (health-gate will see failure via failed_when/until).
func TestApplyPinged_RedisErrorIsFailure(t *testing.T) {
	conn := &fakeConn{doErr: errors.New("LOADING Redis is loading the dataset in memory")}
	m := newModule(conn)
	stream := &applyStream{}

	_ = m.instance().Apply(&pluginv1.ApplyRequest{
		State:  "pinged",
		Params: mustStruct(t, map[string]any{"addr": "127.0.0.1:6379"}),
	}, stream)

	if fin := stream.final(); fin == nil || !fin.Failed {
		t.Fatalf("waited failed=true for PING error, got %+v", fin)
	}
}

// TestApplyPinged_ConnectFailure_DoesNotLeakPassword - connection error, whose text
// contains a password, is sanitized (redactError) - IS invariant ADR-010.
func TestApplyPinged_ConnectFailure_DoesNotLeakPassword(t *testing.T) {
	m := &RedisModule{
		connect: func(_ context.Context, cfg connConfig) (redisConn, error) {
			return nil, errors.New("dial failed for AUTH " + cfg.password)
		},
	}
	stream := &applyStream{}
	_ = m.instance().Apply(&pluginv1.ApplyRequest{
		State: "pinged",
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

// --- Apply: role ---

// TestApplyRole_Master - INFO replication role:master -> Output.role == master,
// changed=false. Proves the where-targeting of the rolling-restart master branch.
func TestApplyRole_Master(t *testing.T) {
	conn := &fakeConn{results: map[string]string{
		"INFO": "# Replication\r\nrole:master\r\nconnected_slaves:2\r\n",
	}}
	m := newModule(conn)
	stream := &applyStream{}

	err := m.instance().Apply(&pluginv1.ApplyRequest{
		State: "role-probed",
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
		t.Error("role: changed must be false constructively (probe)")
	}
	if got := fin.GetOutput().GetFields()["role"].GetStringValue(); got != "master" {
		t.Errorf("Output.role=%q, waiting for master", got)
	}
	assertNoCommandCarriesSecret(t, conn)
	assertEventsNoSecret(t, stream)
	if !conn.closed {
		t.Error("connection not closed")
	}
}

// TestApplyRole_Slave - INFO replication role:slave -> Output.role == slave
// (value Redis; redis-cli role-shell gave the same). Proves the slave branch.
func TestApplyRole_Slave(t *testing.T) {
	conn := &fakeConn{results: map[string]string{
		"INFO": "# Replication\r\nrole:slave\r\nmaster_host:10.0.0.1\r\nmaster_link_status:up\r\n",
	}}
	m := newModule(conn)
	stream := &applyStream{}

	_ = m.instance().Apply(&pluginv1.ApplyRequest{
		State:  "role-probed",
		Params: mustStruct(t, map[string]any{"addr": "127.0.0.1:6379"}),
	}, stream)

	fin := stream.final()
	if fin == nil || fin.Failed {
		t.Fatalf("expected success, got %+v", fin)
	}
	if got := fin.GetOutput().GetFields()["role"].GetStringValue(); got != "slave" {
		t.Errorf("Output.role=%q, waiting for slave", got)
	}
	// INFO replication is actually called (and not something else).
	if !hasCall(conn.calls, "INFO", "replication") {
		t.Errorf("expected the INFO replication call, got %v", conn.calls)
	}
}

// TestApplyRole_MissingRoleField - abnormal INFO (without role field) -> failed, not
// empty role in where (otherwise silently skipping a restart on this host).
func TestApplyRole_MissingRoleField(t *testing.T) {
	conn := &fakeConn{results: map[string]string{
		"INFO": "# Replication\r\nconnected_slaves:0\r\n",
	}}
	m := newModule(conn)
	stream := &applyStream{}

	_ = m.instance().Apply(&pluginv1.ApplyRequest{
		State:  "role-probed",
		Params: mustStruct(t, map[string]any{"addr": "127.0.0.1:6379"}),
	}, stream)

	if fin := stream.final(); fin == nil || !fin.Failed {
		t.Fatalf("expected failed=true on INFO without the role field, got %+v", fin)
	}
}

// TestApplyRole_ConnectFailure_DoesNotLeakPassword - connection error with password in
// the text is sanitized (IS-invariant ADR-010).
func TestApplyRole_ConnectFailure_DoesNotLeakPassword(t *testing.T) {
	m := &RedisModule{
		connect: func(_ context.Context, cfg connConfig) (redisConn, error) {
			return nil, errors.New("dial failed for AUTH " + cfg.password)
		},
	}
	stream := &applyStream{}
	_ = m.instance().Apply(&pluginv1.ApplyRequest{
		State: "role-probed",
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

// TestApplyRole_InfoErrorRedactsPassword - error on INFO replication itself
// (the driver echoed the password from the connection) is edited via redactError by password.
func TestApplyRole_InfoErrorRedactsPassword(t *testing.T) {
	conn := &fakeConn{doErr: errors.New("READONLY context for AUTH " + secretPass)}
	m := newModule(conn)
	stream := &applyStream{}

	_ = m.instance().Apply(&pluginv1.ApplyRequest{
		State: "role-probed",
		Params: mustStruct(t, map[string]any{
			"addr":     "127.0.0.1:6379",
			"password": secretPass,
		}),
	}, stream)

	fin := stream.final()
	if fin == nil || !fin.Failed {
		t.Fatalf("waited failed=true, got %+v", fin)
	}
	if strings.Contains(fin.GetMessage(), secretPass) {
		t.Errorf("password leaked in INFO replication error message: %q", fin.GetMessage())
	}
}

// --- Apply: replica-synced ---

// TestApplyReplicaSynced_LinkUp - master_link_status:up -> synced=true,
// changed=false. Strict resink gate of the replica (the replica caught up with the master after
// restart); until: register.self.synced == true passes.
func TestApplyReplicaSynced_LinkUp(t *testing.T) {
	conn := &fakeConn{results: map[string]string{
		"INFO": "# Replication\r\nrole:slave\r\nmaster_host:10.0.0.1\r\nmaster_link_status:up\r\n",
	}}
	m := newModule(conn)
	stream := &applyStream{}

	err := m.replica().Apply(&pluginv1.ApplyRequest{
		State: "synced",
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
		t.Error("replica-synced: changed must be false by design (probe)")
	}
	if !fin.GetOutput().GetFields()["synced"].GetBoolValue() {
		t.Errorf("Output.synced=%v, expected true (master_link_status:up)", fin.GetOutput().GetFields()["synced"])
	}
	if got := fin.GetOutput().GetFields()["master_link_status"].GetStringValue(); got != "up" {
		t.Errorf("Output.master_link_status=%q, waited up", got)
	}
	assertNoCommandCarriesSecret(t, conn)
	assertEventsNoSecret(t, stream)
	if !conn.closed {
		t.Error("connection not closed")
	}
}

// TestApplyReplicaSynced_LinkDown - master_link_status:down -> synced=false (but
// success-event, not failed: health-gate decides itself via until/failed_when
// register.self.synced). The replica has not yet caught up with the master.
func TestApplyReplicaSynced_LinkDown(t *testing.T) {
	conn := &fakeConn{results: map[string]string{
		"INFO": "# Replication\r\nrole:slave\r\nmaster_host:10.0.0.1\r\nmaster_link_status:down\r\n",
	}}
	m := newModule(conn)
	stream := &applyStream{}

	_ = m.replica().Apply(&pluginv1.ApplyRequest{
		State:  "synced",
		Params: mustStruct(t, map[string]any{"addr": "127.0.0.1:6379"}),
	}, stream)

	fin := stream.final()
	if fin == nil || fin.Failed {
		t.Fatalf("expected success-event (synced=false, not failed), got %+v", fin)
	}
	if fin.GetOutput().GetFields()["synced"].GetBoolValue() {
		t.Error("Output.synced=true, expected false (master_link_status:down)")
	}
	if got := fin.GetOutput().GetFields()["master_link_status"].GetStringValue(); got != "down" {
		t.Errorf("Output.master_link_status=%q, waited down", got)
	}
}

// TestApplyReplicaSynced_MasterHasNoLinkField - the master has a master_link_status field
// NO (master/slave boundary): synced=false with a CLEAR reason in the Message (NOT silent
// success). State is intended for the slave path - the health-gate of the replica should not
// go to master.
func TestApplyReplicaSynced_MasterHasNoLinkField(t *testing.T) {
	conn := &fakeConn{results: map[string]string{
		"INFO": "# Replication\r\nrole:master\r\nconnected_slaves:2\r\n",
	}}
	m := newModule(conn)
	stream := &applyStream{}

	err := m.replica().Apply(&pluginv1.ApplyRequest{
		State:  "synced",
		Params: mustStruct(t, map[string]any{"addr": "127.0.0.1:6379"}),
	}, stream)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil {
		t.Fatal("were waiting for the final event")
	}
	if fin.GetOutput().GetFields()["synced"].GetBoolValue() {
		t.Error("Output.synced=true on master (no master_link_status) - should be false")
	}
	// The reason must be explicit (NOT silent success): Message mentions the absence of a field.
	if !strings.Contains(fin.GetMessage(), "master_link_status") {
		t.Errorf("expected a clear reason in the Message (no master_link_status), got %q", fin.GetMessage())
	}
}

// TestApplyReplicaSynced_ConnectFailure_DoesNotLeakPassword - connection error with
// the password in the text is sanitized (IS-invariant ADR-010).
func TestApplyReplicaSynced_ConnectFailure_DoesNotLeakPassword(t *testing.T) {
	m := &RedisModule{
		connect: func(_ context.Context, cfg connConfig) (redisConn, error) {
			return nil, errors.New("dial failed for AUTH " + cfg.password)
		},
	}
	stream := &applyStream{}
	_ = m.replica().Apply(&pluginv1.ApplyRequest{
		State: "synced",
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

// TestApplyReplicaSynced_InfoErrorRedactsPassword - error on the INFO itself
// replication (the driver echoed the password from the connection) is edited by redactError.
func TestApplyReplicaSynced_InfoErrorRedactsPassword(t *testing.T) {
	conn := &fakeConn{doErr: errors.New("READONLY context for AUTH " + secretPass)}
	m := newModule(conn)
	stream := &applyStream{}

	_ = m.replica().Apply(&pluginv1.ApplyRequest{
		State: "synced",
		Params: mustStruct(t, map[string]any{
			"addr":     "127.0.0.1:6379",
			"password": secretPass,
		}),
	}, stream)

	fin := stream.final()
	if fin == nil || !fin.Failed {
		t.Fatalf("waited failed=true, got %+v", fin)
	}
	if strings.Contains(fin.GetMessage(), secretPass) {
		t.Errorf("password leaked in INFO replication error message: %q", fin.GetMessage())
	}
}

// TestApplyPinged_RedisErrorRedactsPassword - nit #2: applyPinged on PING error
// edits the password via redactError (defense-in-depth/consistency with role/config).
func TestApplyPinged_RedisErrorRedactsPassword(t *testing.T) {
	conn := &fakeConn{doErr: errors.New("ERR context for AUTH " + secretPass)}
	m := newModule(conn)
	stream := &applyStream{}

	_ = m.instance().Apply(&pluginv1.ApplyRequest{
		State: "pinged",
		Params: mustStruct(t, map[string]any{
			"addr":     "127.0.0.1:6379",
			"password": secretPass,
		}),
	}, stream)

	fin := stream.final()
	if fin == nil || !fin.Failed {
		t.Fatalf("waited failed=true, got %+v", fin)
	}
	if strings.Contains(fin.GetMessage(), secretPass) {
		t.Errorf("password leaked in PING error message: %q", fin.GetMessage())
	}
}
