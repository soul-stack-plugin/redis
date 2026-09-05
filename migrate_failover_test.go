package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	pluginv1 "github.com/souls-guild/soul-stack/proto/plugin/gen/go/v1"
)

// --- INFO replication / CLUSTER NODES fixtures for failover-takeover ---

// replicaInfoSynced - INFO replication of a synchronous replica (link up): sync-gate
// passes, failover can be launched.
func replicaInfoSynced() string {
	return "role:slave\nmaster_link_status:up\nmaster_host:10.0.0.1\nmaster_port:6379\n"
}

// replicaInfoNotSynced - INFO replication of a replica with a NON-up link (still catching up):
// sync-gate must fail BEFORE the first failover.
func replicaInfoNotSynced() string {
	return "role:slave\nmaster_link_status:down\nmaster_host:10.0.0.1\nmaster_port:6379\n"
}

// masterInfo - INFO replication of an already promoted master (no master_link_status):
// idempotency failover-takeover -> no-op.
func masterInfo() string {
	return "role:master\nconnected_slaves:1\n"
}

// promotedNodeRow - CLUSTER NODES string of the node itself as master WITH slots (after
// graceful failover node took the slots). waitNodePromoted converges on it.
func promotedNodeRow(id, ipPort string, from, to int) string {
	return masterRowWithSlots(id, ipPort, from, to)
}

// hasClusterFailover - the node has executed at least one CLUSTER FAILOVER.
func hasClusterFailover(c *clusterConn) bool {
	for _, call := range c.calls {
		if isClusterSub(call, "FAILOVER") {
			return true
		}
	}
	return false
}

// assertGracefulFailoverOnly requires that all CLUSTER FAILOVERs on the node be
// GRACEFUL (exactly "CLUSTER FAILOVER", without the FORCE/TAKEOVER argument). This is the information security core
// fail-closed: escalation to FORCE/TAKEOVER - split-brain, prohibited.
func assertGracefulFailoverOnly(t *testing.T, c *clusterConn) {
	t.Helper()
	for _, call := range c.calls {
		if !isClusterSub(call, "FAILOVER") {
			continue
		}
		for _, a := range call[2:] {
			s, _ := a.(string)
			if strings.EqualFold(s, "FORCE") || strings.EqualFold(s, "TAKEOVER") {
				t.Errorf("CLUSTER FAILOVER escalated to %q (split-brain): %v", s, call)
			}
		}
	}
}

// =========================== Validate: failover-takeover =====================

func TestValidate_FailoverTakeoverRejectsEmptyNodes(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.cluster().Validate(context.Background(), &pluginv1.ValidateRequest{
		State: "failed-over",
		Params: mustStruct(t, map[string]any{
			"nodes": map[string]any{},
		}),
	})
	if reply.Ok {
		t.Fatal("waited Ok=false on empty nodes")
	}
}

func TestValidate_FailoverTakeoverHappy(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.cluster().Validate(context.Background(), &pluginv1.ValidateRequest{
		State: "failed-over",
		Params: mustStruct(t, map[string]any{
			"nodes": clusterNodesParam("10.1.0.1:6379", "10.1.0.2:6379"),
		}),
	})
	if !reply.Ok || len(reply.Errors) != 0 {
		t.Fatalf("waited Ok=true, got %+v", reply)
	}
}

// =========================== Apply: failover-takeover ========================

// TestApplyFailoverTakeover_HappyPath - two synchronous new replicas will be promoted
// to master: sync-gate passes (both master_link_status:up), on each GRACEFUL
// CLUSTER FAILOVER, poll sees it as a master with slots. changed=true.
func TestApplyFailoverTakeover_HappyPath(t *testing.T) {
	new0, new1 := "10.1.0.1:6379", "10.1.0.2:6379"
	fl := newFleet(new0, new1)

	// sync-gate: Both replicas are synchronous.
	fl.byAddr[new0].infoRepl = replicaInfoSynced()
	fl.byAddr[new1].infoRepl = replicaInfoSynced()
	// After CLUSTER FAILOVER, the first CLUSTER NODES of a node shows it as master
	// with slots (waitNodePromoted converges immediately).
	fl.byAddr[new0].nodes = promotedNodeRow("n0", new0, 0, 8191)
	fl.byAddr[new1].nodes = promotedNodeRow("n1", new1, 8192, 16383)

	m := fl.module()
	stream := &applyStream{}
	err := m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "failed-over",
		Params: mustStruct(t, map[string]any{
			"password": secretPass,
			"nodes":    clusterNodesParam(new0, new1),
		}),
	}, stream)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil || fin.Failed || !fin.Changed {
		t.Fatalf("expected success changed=true, got %+v", fin)
	}

	// Both replicas received GRACEFUL CLUSTER FAILOVER (without FORCE/TAKEOVER).
	if !hasClusterFailover(fl.byAddr[new0]) || !hasClusterFailover(fl.byAddr[new1]) {
		t.Error("expected CLUSTER FAILOVER on both new replicas")
	}
	assertGracefulFailoverOnly(t, fl.byAddr[new0])
	assertGracefulFailoverOnly(t, fl.byAddr[new1])

	if got := fin.GetOutput().GetFields()["promoted"].GetNumberValue(); got != 2 {
		t.Errorf("promoted=%v, waited 2", got)
	}
	if got := fin.GetOutput().GetFields()["per_node"].GetStringValue(); !strings.Contains(got, "promoted") {
		t.Errorf("per_node=%q, were waiting for promoted status", got)
	}

	assertEventsNoSecret(t, stream)
	for addr, c := range fl.byAddr {
		assertNoClusterSecret(t, addr, c)
	}
}

// TestApplyFailoverTakeover_SyncGateBlocksBeforeFailover - ONE of two replicas
// I haven't caught up yet (master_link_status:down). sync-gate is obliged to refuse BEFORE ANY
// CLUSTER FAILOVER (early failover on a failed replica loses its tail). None
// a node (including a synchronous one) should not receive FAILOVER.
func TestApplyFailoverTakeover_SyncGateBlocksBeforeFailover(t *testing.T) {
	new0, new1 := "10.1.0.1:6379", "10.1.0.2:6379"
	fl := newFleet(new0, new1)

	// new0 is synchronous, new1 is still catching up (link down) -> failure BEFORE failover.
	fl.byAddr[new0].infoRepl = replicaInfoSynced()
	fl.byAddr[new1].infoRepl = replicaInfoNotSynced()
	// The slots would appear, but it would not reach them (fail on sync-gate).
	fl.byAddr[new0].nodes = promotedNodeRow("n0", new0, 0, 8191)
	fl.byAddr[new1].nodes = promotedNodeRow("n1", new1, 8192, 16383)

	m := fl.module()
	stream := &applyStream{}
	_ = m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "failed-over",
		Params: mustStruct(t, map[string]any{
			"password": secretPass,
			"nodes":    clusterNodesParam(new0, new1),
		}),
	}, stream)

	fin := stream.final()
	if fin == nil || !fin.Failed {
		t.Fatalf("expected failed=true on a non-synchronous replica, got %+v", fin)
	}
	if !strings.Contains(fin.GetMessage(), "master_link_status") {
		t.Errorf("Expected a clear reason about master_link_status, got %q", fin.GetMessage())
	}
	// CRITIQUE: NOT ONE node received a FAILOVER (sync-gate works BEFORE the first failover).
	for _, addr := range []string{new0, new1} {
		if hasClusterFailover(fl.byAddr[addr]) {
			t.Errorf("node %s received CLUSTER FAILOVER despite the cluster being out of sync", addr)
		}
	}
	assertEventsNoSecret(t, stream)
}

// TestApplyFailoverTakeover_FailClosedNoEscalation - graceful CLUSTER FAILOVER NOT
// completed: poll CLUSTER NODES always sees the REPLICA node (has not become master).
// FAIL-CLOSED: error + NO CLUSTER FAILOVER FORCE/TAKEOVER (escalation
// prohibited - split-brain).
func TestApplyFailoverTakeover_FailClosedNoEscalation(t *testing.T) {
	new0 := "10.1.0.1:6379"
	fl := newFleet(new0)

	fl.byAddr[new0].infoRepl = replicaInfoSynced() // sync-gate passes
	// CLUSTER NODES of a node ALWAYS shows it as a replica (graceful did not match).
	fl.byAddr[new0].nodes = clusterNodesTable(
		masterRowSpecForFailover("oldm0", "10.0.0.1:6379"),
		nodeRowSpec{id: "n0", ipPort: new0, masterID: "oldm0"},
	)

	m := fl.module()
	stream := &applyStream{}
	_ = m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "failed-over",
		Params: mustStruct(t, map[string]any{
			"password": secretPass,
			"nodes":    clusterNodesParam(new0),
		}),
	}, stream)

	fin := stream.final()
	if fin == nil || !fin.Failed {
		t.Fatalf("expected failed=true on incomplete graceful failover, got %+v", fin)
	}
	// graceful CLUSTER FAILOVER was sent (attempted), but NO escalation.
	if !hasClusterFailover(fl.byAddr[new0]) {
		t.Error("were waiting for at least one (graceful) CLUSTER FAILOVER")
	}
	assertGracefulFailoverOnly(t, fl.byAddr[new0]) // neither FORCE nor TAKEOVER
	if !strings.Contains(fin.GetMessage(), "FORCE") && !strings.Contains(fin.GetMessage(), "manually") {
		t.Errorf("were waiting for a message about escalation failure / manual intervention, got %q", fin.GetMessage())
	}
	assertEventsNoSecret(t, stream)
}

// TestApplyFailoverTakeover_AlreadyMasterNoOp - both nodes are ALREADY masters (repeat
// apply, INFO replication role:master) -> changed=false, no CLUSTER FAILOVER.
func TestApplyFailoverTakeover_AlreadyMasterNoOp(t *testing.T) {
	new0, new1 := "10.1.0.1:6379", "10.1.0.2:6379"
	fl := newFleet(new0, new1)
	fl.byAddr[new0].infoRepl = masterInfo()
	fl.byAddr[new1].infoRepl = masterInfo()

	m := fl.module()
	stream := &applyStream{}
	if err := m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "failed-over",
		Params: mustStruct(t, map[string]any{
			"nodes": clusterNodesParam(new0, new1),
		}),
	}, stream); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil || fin.Failed {
		t.Fatalf("expected success, got %+v", fin)
	}
	if fin.Changed {
		t.Error("waited changed=false: both nodes are already masters (no-op)")
	}
	// No-op: not a single FAILOVER.
	for _, addr := range []string{new0, new1} {
		if hasClusterFailover(fl.byAddr[addr]) {
			t.Errorf("node %s (already master): FAILOVER should not be called", addr)
		}
	}
	if got := fin.GetOutput().GetFields()["per_node"].GetStringValue(); !strings.Contains(got, "already") {
		t.Errorf("per_node=%q, waiting for status already", got)
	}
}

// TestApplyFailoverTakeover_PartialIdempotent - one node is already master (no-op),
// the second synchronous replica (promoted): changed=true, FAILOVER only on the second.
func TestApplyFailoverTakeover_PartialIdempotent(t *testing.T) {
	new0, new1 := "10.1.0.1:6379", "10.1.0.2:6379"
	fl := newFleet(new0, new1)
	// new0 is already master (no-op), new1 is a synchronous replica (will be promoted).
	fl.byAddr[new0].infoRepl = masterInfo()
	fl.byAddr[new1].infoRepl = replicaInfoSynced()
	fl.byAddr[new1].nodes = promotedNodeRow("n1", new1, 8192, 16383)

	m := fl.module()
	stream := &applyStream{}
	if err := m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "failed-over",
		Params: mustStruct(t, map[string]any{
			"nodes": clusterNodesParam(new0, new1),
		}),
	}, stream); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil || fin.Failed || !fin.Changed {
		t.Fatalf("expected success changed=true (new1 promoted), got %+v", fin)
	}
	if hasClusterFailover(fl.byAddr[new0]) {
		t.Error("new0 (already master): FAILOVER should not be called")
	}
	if !hasClusterFailover(fl.byAddr[new1]) {
		t.Error("new1 (synchronous replica): expected CLUSTER FAILOVER")
	}
	if got := fin.GetOutput().GetFields()["promoted"].GetNumberValue(); got != 1 {
		t.Errorf("promoted=%v, waited 1", got)
	}
}

// TestApplyFailoverTakeover_ConnectFailNoLeak - the connection to the new node fails
// password in the text -> failed, the password does not leak.
func TestApplyFailoverTakeover_ConnectFailNoLeak(t *testing.T) {
	m := &RedisModule{
		connect: func(_ context.Context, cfg connConfig) (redisConn, error) {
			return nil, errors.New("dial failed for AUTH " + cfg.password)
		},
	}
	stream := &applyStream{}
	_ = m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "failed-over",
		Params: mustStruct(t, map[string]any{
			"password": secretPass,
			"nodes":    clusterNodesParam("10.1.0.1:6379"),
		}),
	}, stream)

	fin := stream.final()
	if fin == nil || !fin.Failed {
		t.Fatalf("waited failed=true, got %+v", fin)
	}
	assertEventsNoSecret(t, stream)
}

// =========================== Validate: forget-external =======================

func TestValidate_ForgetExternalRejectsEmptyNodes(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.cluster().Validate(context.Background(), &pluginv1.ValidateRequest{
		State: "external-forgotten",
		Params: mustStruct(t, map[string]any{
			"nodes":        map[string]any{},
			"source_nodes": []any{"10.0.0.1:6379"},
		}),
	})
	if reply.Ok {
		t.Fatal("waited Ok=false on empty nodes")
	}
}

func TestValidate_ForgetExternalRejectsEmptySourceNodes(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.cluster().Validate(context.Background(), &pluginv1.ValidateRequest{
		State: "external-forgotten",
		Params: mustStruct(t, map[string]any{
			"nodes":        clusterNodesParam("10.1.0.1:6379"),
			"source_nodes": []any{},
		}),
	})
	if reply.Ok {
		t.Fatal("waited Ok=false on empty source_nodes")
	}
}

func TestValidate_ForgetExternalHappy(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.cluster().Validate(context.Background(), &pluginv1.ValidateRequest{
		State: "external-forgotten",
		Params: mustStruct(t, map[string]any{
			"nodes":        clusterNodesParam("10.1.0.1:6379", "10.1.0.2:6379"),
			"source_nodes": []any{"10.0.0.1:6379"},
		}),
	})
	if !reply.Ok || len(reply.Errors) != 0 {
		t.Fatalf("waited Ok=true, got %+v", reply)
	}
}

// =========================== Apply: forget-external ==========================

// oldClusterTwoMastersOneReplica - OLD cluster topology: 2 masters + 1
// replica (forget-external throws out EVERYONE - both masters and replicas).
func oldClusterTwoMastersOneReplica() string {
	return strings.Join([]string{
		masterRowWithSlots("oldm0", "10.0.0.1:6379", 0, 8191),
		masterRowWithSlots("oldm1", "10.0.0.2:6379", 8192, 16383),
		replicaRow("oldr0", "10.0.0.3:6379", "oldm0"),
	}, "\n")
}

// TestApplyForgetExternal_ForgetsAllOldOnEachNode - two new nodes execute
// CLUSTER FORGET all three old node-ids (2 masters + 1 replica). changed=true.
func TestApplyForgetExternal_ForgetsAllOldOnEachNode(t *testing.T) {
	new0, new1 := "10.1.0.1:6379", "10.1.0.2:6379"
	seed := "10.0.0.1:6379"
	fl := newFleet(new0, new1, seed)
	fl.byAddr[seed].nodes = oldClusterTwoMastersOneReplica()

	m := fl.module()
	stream := &applyStream{}
	err := m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "external-forgotten",
		Params: mustStruct(t, map[string]any{
			"password":     secretPass,
			"nodes":        clusterNodesParam(new0, new1),
			"source_nodes": []any{seed},
		}),
	}, stream)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil || fin.Failed || !fin.Changed {
		t.Fatalf("expected success changed=true, got %+v", fin)
	}

	// EACH new node executed FORGET for ALL three old ids.
	for _, addr := range []string{new0, new1} {
		for _, oldID := range []string{"oldm0", "oldm1", "oldr0"} {
			if !hasClusterForget(fl.byAddr[addr], oldID) {
				t.Errorf("node %s: waiting for CLUSTER FORGET %s", addr, oldID)
			}
		}
	}
	// FORGET itself does not receive the source-seed (it only receives the source id).
	if len(clusterForgetTargets(fl.byAddr[seed])) != 0 {
		t.Error("source seed should not receive CLUSTER FORGET")
	}

	if got := fin.GetOutput().GetFields()["old_nodes"].GetNumberValue(); got != 3 {
		t.Errorf("old_nodes=%v, waited 3", got)
	}
	// 2 new x 3 old = 6 really forgotten pairs.
	if got := fin.GetOutput().GetFields()["forgotten"].GetNumberValue(); got != 6 {
		t.Errorf("forgotten=%v, waited 6 (2 nodes x 3 old)", got)
	}

	assertEventsNoSecret(t, stream)
	for addr, c := range fl.byAddr {
		assertNoClusterSecret(t, addr, c)
	}
}

// TestApplyForgetExternal_SharedTopologyNoSelfForget - post-join reality:
// after join-external + failover-takeover, new nodes are members of the SAME cluster, and
// CLUSTER NODES of the old seed lists BOTH old and new nodes (according to their REAL
// id). forget-external must forget ONLY old ids and NOT ONE new node should
// get CLUSTER FORGET your own id (Redis: "can't forget myself" -
// hard-fail). The isolated old-cluster fixture hides this bug; here is the topology
// general.
func TestApplyForgetExternal_SharedTopologyNoSelfForget(t *testing.T) {
	new0, new1 := "10.1.0.1:6379", "10.1.0.2:6379"
	seed := "10.0.0.1:6379"
	fl := newFleet(new0, new1, seed)

	// SHARED topology: old masters/replica gave slots to new ones (after failover),
	// old ones are empty masters; new nodes (n0/n1) - cluster members with REAL id.
	// clusterNodesParam gives the keys node-0/node-1, but the id in the topology is its own
	// each node (like a real CLUSTER NODES).
	const n0ID, n1ID = "newid0", "newid1"
	fl.byAddr[seed].nodes = strings.Join([]string{
		masterRowNoSlots("oldm0", "10.0.0.1:6379"), // old master, the slots are gone
		masterRowNoSlots("oldm1", "10.0.0.2:6379"),
		replicaRow("oldr0", "10.0.0.4:6379", "oldm0"),
		masterRowWithSlots(n0ID, new0, 0, 8191), // new node - now master WITH slots
		masterRowWithSlots(n1ID, new1, 8192, 16383),
	}, "\n")

	m := fl.module()
	stream := &applyStream{}
	err := m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "external-forgotten",
		Params: mustStruct(t, map[string]any{
			"password":     secretPass,
			"nodes":        clusterNodesParam(new0, new1),
			"source_nodes": []any{seed},
		}),
	}, stream)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil || fin.Failed {
		t.Fatalf("expected success (self-forget excluded), got %+v", fin)
	}
	if !fin.Changed {
		t.Error("waited changed=true (old ones are forgotten)")
	}

	// CRIT: not a single new node forgets ITS id (and the id of the neighboring new node).
	for _, addr := range []string{new0, new1} {
		for _, selfish := range []string{n0ID, n1ID} {
			if hasClusterForget(fl.byAddr[addr], selfish) {
				t.Errorf("node %s forgets NEW node %s (self-forget / forget-peer): %v",
					addr, selfish, clusterForgetTargets(fl.byAddr[addr]))
			}
		}
	}
	// Each new node forgot exactly the old ids (3 old ones).
	for _, addr := range []string{new0, new1} {
		for _, oldID := range []string{"oldm0", "oldm1", "oldr0"} {
			if !hasClusterForget(fl.byAddr[addr], oldID) {
				t.Errorf("node %s: expected CLUSTER FORGET of old %s", addr, oldID)
			}
		}
	}
	// oldIDs = 3 (new ones are filtered out from the seed topology), forgotten = 2 nodes x 3.
	if got := fin.GetOutput().GetFields()["old_nodes"].GetNumberValue(); got != 3 {
		t.Errorf("old_nodes=%v, waited 3 (new nodes are filtered out)", got)
	}
	if got := fin.GetOutput().GetFields()["forgotten"].GetNumberValue(); got != 6 {
		t.Errorf("forgotten=%v, waited 6 (2 nodes x 3 old)", got)
	}

	assertEventsNoSecret(t, stream)
	for addr, c := range fl.byAddr {
		assertNoClusterSecret(t, addr, c)
	}
}

// TestApplyForgetExternal_CantForgetSelfSwallowed - defense-in-depth: even if
// id of new node leaked into oldIDs (gossip race between filter and FORGET, ip form
// dispersed), Redis-answer "I can't forget myself" is swallowed as idempotency -
// the run does NOT crash.
func TestApplyForgetExternal_CantForgetSelfSwallowed(t *testing.T) {
	new0 := "10.1.0.1:6379"
	seed := "10.0.0.1:6379"
	fl := newFleet(new0, seed)
	fl.byAddr[seed].nodes = strings.Join([]string{
		masterRowWithSlots("oldm0", "10.0.0.2:6379", 0, 16383),
	}, "\n")
	// The FORGET node responds "can't forget myself" (as if the id were its own).
	fl.byAddr[new0].forgetErr = errors.New("ERR I tried hard but I can't forget myself...")

	m := fl.module()
	stream := &applyStream{}
	if err := m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "external-forgotten",
		Params: mustStruct(t, map[string]any{
			"nodes":        clusterNodesParam(new0),
			"source_nodes": []any{seed},
		}),
	}, stream); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	fin := stream.final()
	if fin == nil || fin.Failed {
		t.Fatalf("expected success (can't-forget-myself swallowed), got %+v", fin)
	}
	if got := fin.GetOutput().GetFields()["forgotten"].GetNumberValue(); got != 0 {
		t.Errorf("forgotten=%v, waited 0 (self-forget swallowed)", got)
	}
}

// TestApplyForgetExternal_OnlyNewNodesLeftNoOp - there are no old seeds in the topology
// left (all forgotten by the previous apply), seed lists ONLY new nodes:
// oldIDs is empty -> changed=false, no FORGET (steady-state no-op, not an error).
func TestApplyForgetExternal_OnlyNewNodesLeftNoOp(t *testing.T) {
	new0, new1 := "10.1.0.1:6379", "10.1.0.2:6379"
	seed := new0 // The seed is now the new node itself (the old ones are extinguished, the operator gave a new seed)
	fl := newFleet(new0, new1)
	fl.byAddr[new0].nodes = strings.Join([]string{
		masterRowWithSlots("newid0", new0, 0, 8191),
		masterRowWithSlots("newid1", new1, 8192, 16383),
	}, "\n")

	m := fl.module()
	stream := &applyStream{}
	if err := m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "external-forgotten",
		Params: mustStruct(t, map[string]any{
			"nodes":        clusterNodesParam(new0, new1),
			"source_nodes": []any{seed},
		}),
	}, stream); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	fin := stream.final()
	if fin == nil || fin.Failed {
		t.Fatalf("expected success (no-op), got %+v", fin)
	}
	if fin.Changed {
		t.Error("waited changed=false: there are no old ones left (steady-state no-op)")
	}
	for _, addr := range []string{new0, new1} {
		if len(clusterForgetTargets(fl.byAddr[addr])) != 0 {
			t.Errorf("node %s: no FORGET was expected (no old ones): %v", addr, clusterForgetTargets(fl.byAddr[addr]))
		}
	}
	if got := fin.GetOutput().GetFields()["old_nodes"].GetNumberValue(); got != 0 {
		t.Errorf("old_nodes=%v, waited 0", got)
	}
}

// TestApplyForgetExternal_NoSlotMigration - forget-external does NOT migrate slots
// (as opposed to remove-node): new masters already have slots after failover. Neither
// SETSLOT nor MIGRATE.
func TestApplyForgetExternal_NoSlotMigration(t *testing.T) {
	new0 := "10.1.0.1:6379"
	seed := "10.0.0.1:6379"
	fl := newFleet(new0, seed)
	fl.byAddr[seed].nodes = oldClusterTwoMastersOneReplica()

	m := fl.module()
	stream := &applyStream{}
	if err := m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "external-forgotten",
		Params: mustStruct(t, map[string]any{
			"nodes":        clusterNodesParam(new0),
			"source_nodes": []any{seed},
		}),
	}, stream); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	fin := stream.final()
	if fin == nil || fin.Failed {
		t.Fatalf("expected success, got %+v", fin)
	}
	for addr, c := range fl.byAddr {
		for _, call := range c.calls {
			if isClusterSub(call, "SETSLOT") {
				t.Errorf("node %s: SETSLOT in forget-external should not be called: %v", addr, call)
			}
			if v0, _ := call[0].(string); strings.EqualFold(v0, "MIGRATE") {
				t.Errorf("node %s: MIGRATE in forget-external should not be called: %v", addr, call)
			}
		}
	}
}

// TestApplyForgetExternal_UnknownNodeIdempotent - the old id is no longer known to the new one
// node (re-apply / gossip already forgotten): FORGET returned "Unknown node" ->
// swallowed as idempotency, run successful, changed=false (nothing forgotten).
func TestApplyForgetExternal_UnknownNodeIdempotent(t *testing.T) {
	new0 := "10.1.0.1:6379"
	seed := "10.0.0.1:6379"
	fl := newFleet(new0, seed)
	fl.byAddr[seed].nodes = strings.Join([]string{
		masterRowWithSlots("oldm0", "10.0.0.1:6379", 0, 16383),
	}, "\n")
	// The new node responds to EVERY FORGET with "Unknown node" (I've already forgotten the old one).
	fl.byAddr[new0].forgetErr = errors.New("ERR Unknown node oldm0")

	m := fl.module()
	stream := &applyStream{}
	if err := m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "external-forgotten",
		Params: mustStruct(t, map[string]any{
			"nodes":        clusterNodesParam(new0),
			"source_nodes": []any{seed},
		}),
	}, stream); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	fin := stream.final()
	if fin == nil || fin.Failed {
		t.Fatalf("expected success (Unknown node swallowed), got %+v", fin)
	}
	if fin.Changed {
		t.Error("waited changed=false: all old ones are already forgotten (no-op)")
	}
	if got := fin.GetOutput().GetFields()["forgotten"].GetNumberValue(); got != 0 {
		t.Errorf("forgotten=%v, waited 0 (all Unknown nodes)", got)
	}
}

// TestApplyForgetExternal_SourceSeedFailoverToNext - the first source-seed is not available,
// the second one answers: the old ids are taken from the second one.
func TestApplyForgetExternal_SourceSeedFailoverToNext(t *testing.T) {
	new0 := "10.1.0.1:6379"
	seedDown, seedUp := "10.0.0.9:6379", "10.0.0.1:6379"
	fl := newFleet(new0, seedUp) // seedDown is not established -> connection to it fails
	fl.byAddr[seedUp].nodes = strings.Join([]string{
		masterRowWithSlots("oldm0", "10.0.0.1:6379", 0, 16383),
	}, "\n")

	m := fl.module()
	stream := &applyStream{}
	if err := m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "external-forgotten",
		Params: mustStruct(t, map[string]any{
			"nodes":        clusterNodesParam(new0),
			"source_nodes": []any{seedDown, seedUp},
		}),
	}, stream); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	fin := stream.final()
	if fin == nil || fin.Failed || !fin.Changed {
		t.Fatalf("expected success through the second seed, got %+v", fin)
	}
	if !hasClusterForget(fl.byAddr[new0], "oldm0") {
		t.Error("were waiting for CLUSTER FORGET oldm0 (id from the second seed)")
	}
	if got := fin.GetOutput().GetFields()["source_via"].GetStringValue(); got != seedUp {
		t.Errorf("source_via=%q, expected %q (second seed)", got, seedUp)
	}
}

// TestApplyForgetExternal_AllSourceSeedsDown - all old seeds are not available (old
// the cluster has already been extinguished): there is nowhere to get the id -> failed (not an idempotent path - we don't
// we know what to forget). The password does not leak.
func TestApplyForgetExternal_AllSourceSeedsDown(t *testing.T) {
	m := &RedisModule{
		connect: func(_ context.Context, cfg connConfig) (redisConn, error) {
			return nil, errors.New("dial failed for AUTH " + cfg.password)
		},
	}
	stream := &applyStream{}
	_ = m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "external-forgotten",
		Params: mustStruct(t, map[string]any{
			"password":     secretPass,
			"nodes":        clusterNodesParam("10.1.0.1:6379"),
			"source_nodes": []any{"10.0.0.1:6379"},
		}),
	}, stream)

	fin := stream.final()
	if fin == nil || !fin.Failed {
		t.Fatalf("expected failed=true on all unavailable seeds, got %+v", fin)
	}
	assertEventsNoSecret(t, stream)
}

// masterRowSpecForFailover - nodeRowSpec master row for fail-closed test (node
// remains a replica of this master). Local readability helper.
func masterRowSpecForFailover(id, ipPort string) nodeRowSpec {
	return nodeRowSpec{id: id, ipPort: ipPort, master: true}
}
