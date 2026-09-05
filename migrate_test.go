package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	pluginv1 "github.com/souls-guild/soul-stack/proto/plugin/gen/go/v1"
)

// masterRowWithSlots builds a CLUSTER NODES row for master WITH slot range
// (clusterNodesTable does not carry slots, but join-external parses them for sorting
// mapping 1:1). Format: <id> <ip:port@cport> master - 0 0 0 connected <from-to>.
func masterRowWithSlots(id, ipPort string, from, to int) string {
	return fmt.Sprintf("%s %s@%s master - 0 0 0 connected %d-%d", id, ipPort, ipPort, from, to)
}

// sourceClusterTwoMasters - topology of the old cluster of two masters with
// slots (m0 owns the bottom half, m1 the top): happy-path base and checks
// sort the mapping in ascending order of the first slot.
func sourceClusterTwoMasters() string {
	return strings.Join([]string{
		masterRowWithSlots("oldm0", "10.0.0.1:6379", 0, 8191),
		masterRowWithSlots("oldm1", "10.0.0.2:6379", 8192, 16383),
	}, "\n")
}

// ownIsolated - CLUSTER NODES of the fresh isolated new node (one line:
// he himself is like a master without slots) -> alreadyReplicaOf=false, let's go pour in.
func ownIsolated(id, ipPort string) string {
	return clusterNodesTable(nodeRowSpec{id: id, ipPort: ipPort, master: true})
}

// --- Validate: join-external ---

func TestValidate_JoinExternalRejectsEmptyNodes(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.cluster().Validate(context.Background(), &pluginv1.ValidateRequest{
		State: "external-joined",
		Params: mustStruct(t, map[string]any{
			"nodes":        map[string]any{},
			"source_nodes": []any{"10.0.0.1:6379"},
			"shards_dest":  1,
		}),
	})
	if reply.Ok {
		t.Fatal("waited Ok=false on empty nodes")
	}
}

func TestValidate_JoinExternalRejectsEmptySourceNodes(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.cluster().Validate(context.Background(), &pluginv1.ValidateRequest{
		State: "external-joined",
		Params: mustStruct(t, map[string]any{
			"nodes":        clusterNodesParam("10.1.0.1:6379"),
			"source_nodes": []any{},
			"shards_dest":  1,
		}),
	})
	if reply.Ok {
		t.Fatal("waited Ok=false on empty source_nodes")
	}
}

func TestValidate_JoinExternalRejectsBadShardsDest(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.cluster().Validate(context.Background(), &pluginv1.ValidateRequest{
		State: "external-joined",
		Params: mustStruct(t, map[string]any{
			"nodes":        clusterNodesParam("10.1.0.1:6379"),
			"source_nodes": []any{"10.0.0.1:6379"},
			"shards_dest":  0,
		}),
	})
	if reply.Ok {
		t.Fatal("waited Ok=false on shards_dest < 1")
	}
}

func TestValidate_JoinExternalHappy(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.cluster().Validate(context.Background(), &pluginv1.ValidateRequest{
		State: "external-joined",
		Params: mustStruct(t, map[string]any{
			"nodes":        clusterNodesParam("10.1.0.1:6379", "10.1.0.2:6379"),
			"source_nodes": []any{"10.0.0.1:6379"},
			"shards_dest":  2,
		}),
	})
	if !reply.Ok || len(reply.Errors) != 0 {
		t.Fatalf("waited Ok=true, got %+v", reply)
	}
}

// --- Apply join-external: happy-path (NODES -> MEET -> REPLICATE, 1:1 by slots) ---

// TestApplyJoinExternal_HappyPath - two new nodes join the old cluster from
// two masters: each sends MEET to source-seed, waits for convergence, REPLICATE
// mapped master. 1:1 mapping BY SLOT: new node with smaller key
// old master with a smaller first slot.
func TestApplyJoinExternal_HappyPath(t *testing.T) {
	new0, new1 := "10.1.0.1:6379", "10.1.0.2:6379"
	seed := "10.0.0.1:6379"
	fl := newFleet(new0, new1, seed)

	// Source-seed gives the topology of the old cluster (2 masters with slots).
	fl.byAddr[seed].nodes = sourceClusterTwoMasters()

	// New node: first CLUSTER NODES (before MEET) - isolated (itself, master
	// without master-id) -> not a replica; after MEET - sees the target old master
	// (its id in the topology) -> waitNodeKnows converges.
	converged := sourceClusterTwoMasters()
	fl.byAddr[new0].nodesSeq = []string{ownIsolated("n0", new0), converged}
	fl.byAddr[new1].nodesSeq = []string{ownIsolated("n1", new1), converged}

	m := fl.module()
	stream := &applyStream{}

	err := m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "external-joined",
		Params: mustStruct(t, map[string]any{
			"password":     secretPass,
			"nodes":        clusterNodesParam(new0, new1),
			"source_nodes": []any{seed},
			"shards_dest":  2,
		}),
	}, stream)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil || fin.Failed || !fin.Changed {
		t.Fatalf("expected success changed=true, got %+v", fin)
	}

	// 1:1 mapping by slots: node-0 (clusterNodesParam key "node-0") oldm0
	// (slots 0-8191), node-1 oldm1 (8192-16383).
	assertReplicateTo(t, fl.byAddr[new0], "oldm0")
	assertReplicateTo(t, fl.byAddr[new1], "oldm1")

	// Each new node sent MEET to the source-seed ip:port.
	assertMeetTargets(t, fl.byAddr[new0], []string{seed})
	assertMeetTargets(t, fl.byAddr[new1], []string{seed})

	// Source-seed does NOT receive MEET/REPLICATE - it is only a topology source.
	for _, call := range fl.byAddr[seed].calls {
		if isClusterSub(call, "MEET") || isClusterSub(call, "REPLICATE") {
			t.Errorf("source seed should not receive MEET/REPLICATE: %v", call)
		}
	}

	if got := fin.GetOutput().GetFields()["mapping"].GetStringValue(); got != "node-0->oldm0,node-1->oldm1" {
		t.Errorf("mapping=%q, waited node-0->oldm0,node-1->oldm1", got)
	}
	if got := fin.GetOutput().GetFields()["shards"].GetNumberValue(); got != 2 {
		t.Errorf("shards=%v, waited 2", got)
	}

	assertEventsNoSecret(t, stream)
	for addr, c := range fl.byAddr {
		assertNoClusterSecret(t, addr, c)
	}
}

// TestApplyJoinExternal_MappingByFirstSlotNotID - order of CLUSTER NODES rows and
// node-id does NOT affect the mapping: it is strictly in ascending order of the first slot. Old
// a master with an alphabetically LARGER id, but a SMALLER slot must be mapped to the first one
// (by key) new node.
func TestApplyJoinExternal_MappingByFirstSlotNotID(t *testing.T) {
	new0, new1 := "10.1.0.1:6379", "10.1.0.2:6379"
	seed := "10.0.0.1:6379"
	fl := newFleet(new0, new1, seed)

	// zzz owns the lower slots 0-8191, aaa owns the upper slots 8192-16383. By id sorting
	// the first would be aaa; by slot sorting (correct) - zzz.
	src := strings.Join([]string{
		masterRowWithSlots("aaa", "10.0.0.2:6379", 8192, 16383),
		masterRowWithSlots("zzz", "10.0.0.1:6379", 0, 8191),
	}, "\n")
	fl.byAddr[seed].nodes = src
	fl.byAddr[new0].nodesSeq = []string{ownIsolated("n0", new0), src}
	fl.byAddr[new1].nodesSeq = []string{ownIsolated("n1", new1), src}

	m := fl.module()
	stream := &applyStream{}
	if err := m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "external-joined",
		Params: mustStruct(t, map[string]any{
			"nodes":        clusterNodesParam(new0, new1),
			"source_nodes": []any{seed},
			"shards_dest":  2,
		}),
	}, stream); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil || fin.Failed {
		t.Fatalf("expected success, got %+v", fin)
	}
	// node-0 zzz (first slot 0), node-1 aaa (first slot 8192).
	assertReplicateTo(t, fl.byAddr[new0], "zzz")
	assertReplicateTo(t, fl.byAddr[new1], "aaa")
}

// --- Apply join-external: FAIL-FAST on shards-mismatch ---

// TestApplyJoinExternal_FailFastShardsMismatch - source has 3 masters, dest
// waits for 2 shards (and feeds 2 new nodes) -> 1:1 impossible -> failed, REPLICATE/MEET
// NOT performed on new nodes. The password does not leak.
func TestApplyJoinExternal_FailFastShardsMismatch(t *testing.T) {
	new0, new1 := "10.1.0.1:6379", "10.1.0.2:6379"
	seed := "10.0.0.1:6379"
	fl := newFleet(new0, new1, seed)

	// Source: 3 masters with slots (3 shards).
	fl.byAddr[seed].nodes = strings.Join([]string{
		masterRowWithSlots("oldm0", "10.0.0.1:6379", 0, 5460),
		masterRowWithSlots("oldm1", "10.0.0.2:6379", 5461, 10922),
		masterRowWithSlots("oldm2", "10.0.0.3:6379", 10923, 16383),
	}, "\n")

	m := fl.module()
	stream := &applyStream{}
	_ = m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "external-joined",
		Params: mustStruct(t, map[string]any{
			"password":     secretPass,
			"nodes":        clusterNodesParam(new0, new1), // 2 new nodes
			"source_nodes": []any{seed},
			"shards_dest":  2, // waiting 2, at the source 3
		}),
	}, stream)

	fin := stream.final()
	if fin == nil || !fin.Failed {
		t.Fatalf("expected failed=true on shards-mismatch (3 source masters vs 2 dest), got %+v", fin)
	}
	if !strings.Contains(fin.GetMessage(), "3 masters") || !strings.Contains(fin.GetMessage(), "2 shards") {
		t.Errorf("We were expecting a clear error about 3 masters / 2 shards, got %q", fin.GetMessage())
	}
	// New nodes were NOT touched: neither MEET nor REPLICATE (fail before infusion).
	for _, addr := range []string{new0, new1} {
		for _, call := range fl.byAddr[addr].calls {
			if isClusterSub(call, "MEET") || isClusterSub(call, "REPLICATE") {
				t.Errorf("node %s touched during fail-fast: %v", addr, call)
			}
		}
	}
	assertEventsNoSecret(t, stream)
}

// TestApplyJoinExternal_FailFastNodesCountMismatch - number of new nodes != shards_dest
// (statically unverifiable against live source, but nodesshards_dest
// verified until connection) -> failed, the source is not even polled.
func TestApplyJoinExternal_FailFastNodesCountMismatch(t *testing.T) {
	new0 := "10.1.0.1:6379"
	seed := "10.0.0.1:6379"
	fl := newFleet(new0, seed)
	fl.byAddr[seed].nodes = sourceClusterTwoMasters()

	m := fl.module()
	stream := &applyStream{}
	_ = m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "external-joined",
		Params: mustStruct(t, map[string]any{
			"nodes":        clusterNodesParam(new0), // 1 node
			"source_nodes": []any{seed},
			"shards_dest":  2, // waiting 2
		}),
	}, stream)

	fin := stream.final()
	if fin == nil || !fin.Failed {
		t.Fatalf("waited failed=true on nodes!=shards_dest, got %+v", fin)
	}
	// The source was not polled - fail before connecting to the seed.
	if len(fl.byAddr[seed].calls) != 0 {
		t.Errorf("source seed should not be polled when nodes!=shards_dest: %v", fl.byAddr[seed].calls)
	}
}

// --- Apply join-external: idempotency (the node is already a replica of the desired master) ---

// TestApplyJoinExternal_AlreadyReplicaNoOp - both new nodes are ALREADY replicas of their own
// mapped old masters (re-apply) -> changed=false, neither MEET nor
// REPLICATE are not sent.
func TestApplyJoinExternal_AlreadyReplicaNoOp(t *testing.T) {
	new0, new1 := "10.1.0.1:6379", "10.1.0.2:6379"
	seed := "10.0.0.1:6379"
	fl := newFleet(new0, new1, seed)
	fl.byAddr[seed].nodes = sourceClusterTwoMasters()

	// CLUSTER NODES of each node ALREADY contains its row as a replica of the desired
	// masters (n0 -> oldm0, n1 -> oldm1) -> alreadyReplicaOf=true.
	n0Topo := strings.Join([]string{
		masterRowWithSlots("oldm0", "10.0.0.1:6379", 0, 8191),
		clusterNodesTable(nodeRowSpec{id: "n0", ipPort: new0, masterID: "oldm0"}),
	}, "\n")
	n1Topo := strings.Join([]string{
		masterRowWithSlots("oldm1", "10.0.0.2:6379", 8192, 16383),
		clusterNodesTable(nodeRowSpec{id: "n1", ipPort: new1, masterID: "oldm1"}),
	}, "\n")
	fl.byAddr[new0].nodes = n0Topo
	fl.byAddr[new1].nodes = n1Topo

	m := fl.module()
	stream := &applyStream{}
	if err := m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "external-joined",
		Params: mustStruct(t, map[string]any{
			"nodes":        clusterNodesParam(new0, new1),
			"source_nodes": []any{seed},
			"shards_dest":  2,
		}),
	}, stream); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil || fin.Failed {
		t.Fatalf("expected success, got %+v", fin)
	}
	if fin.Changed {
		t.Error("expected changed=false: both nodes are already replicas of the required masters (no-op)")
	}
	// No-op: neither MEET nor REPLICATE on new nodes.
	for _, addr := range []string{new0, new1} {
		for _, call := range fl.byAddr[addr].calls {
			if isClusterSub(call, "MEET") || isClusterSub(call, "REPLICATE") {
				t.Errorf("node %s: no-op broken, caused by %v", addr, call)
			}
		}
	}
	if got := fin.GetOutput().GetFields()["per_node"].GetStringValue(); !strings.Contains(got, "already") {
		t.Errorf("per_node=%q, waiting for status already", got)
	}
}

// TestApplyJoinExternal_PartialIdempotent - one node is already a replica, the second is not yet:
// changed=true (the second one is in), the first one is not touched.
func TestApplyJoinExternal_PartialIdempotent(t *testing.T) {
	new0, new1 := "10.1.0.1:6379", "10.1.0.2:6379"
	seed := "10.0.0.1:6379"
	fl := newFleet(new0, new1, seed)
	fl.byAddr[seed].nodes = sourceClusterTwoMasters()

	// n0 is already a replica of oldm0 (no-op), n1 is still isolated (joined in).
	fl.byAddr[new0].nodes = strings.Join([]string{
		masterRowWithSlots("oldm0", "10.0.0.1:6379", 0, 8191),
		clusterNodesTable(nodeRowSpec{id: "n0", ipPort: new0, masterID: "oldm0"}),
	}, "\n")
	fl.byAddr[new1].nodesSeq = []string{ownIsolated("n1", new1), sourceClusterTwoMasters()}

	m := fl.module()
	stream := &applyStream{}
	if err := m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "external-joined",
		Params: mustStruct(t, map[string]any{
			"nodes":        clusterNodesParam(new0, new1),
			"source_nodes": []any{seed},
			"shards_dest":  2,
		}),
	}, stream); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil || fin.Failed || !fin.Changed {
		t.Fatalf("expected success changed=true (n1 added), got %+v", fin)
	}
	// n0 was not touched.
	for _, call := range fl.byAddr[new0].calls {
		if isClusterSub(call, "MEET") || isClusterSub(call, "REPLICATE") {
			t.Errorf("n0 (already a replica) should not be touched: %v", call)
		}
	}
	// n1 infused: MEET seed + REPLICATE oldm1.
	assertMeetTargets(t, fl.byAddr[new1], []string{seed})
	assertReplicateTo(t, fl.byAddr[new1], "oldm1")
}

// --- Apply join-external: connection to source-seed fails without leaking password ---

func TestApplyJoinExternal_SourceConnectFailNoLeak(t *testing.T) {
	m := &RedisModule{
		connect: func(_ context.Context, cfg connConfig) (redisConn, error) {
			return nil, errors.New("dial failed for AUTH " + cfg.password)
		},
	}
	stream := &applyStream{}
	_ = m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "external-joined",
		Params: mustStruct(t, map[string]any{
			"password":     secretPass,
			"nodes":        clusterNodesParam("10.1.0.1:6379", "10.1.0.2:6379"),
			"source_nodes": []any{"10.0.0.1:6379"},
			"shards_dest":  2,
		}),
	}, stream)

	fin := stream.final()
	if fin == nil || !fin.Failed {
		t.Fatalf("waited failed=true, got %+v", fin)
	}
	assertEventsNoSecret(t, stream)
}

// TestApplyJoinExternal_SourceSeedFailoverToNext - the first source-seed is not available,
// the second one answers: the topology is taken from the second one (like redis-cli with the first one
// reachable node).
func TestApplyJoinExternal_SourceSeedFailoverToNext(t *testing.T) {
	new0, new1 := "10.1.0.1:6379", "10.1.0.2:6379"
	seedDown, seedUp := "10.0.0.9:6379", "10.0.0.1:6379"
	// We do NOT start seedDown in fleet -> connect to it will return an error, the search goes further.
	fl := newFleet(new0, new1, seedUp)
	fl.byAddr[seedUp].nodes = sourceClusterTwoMasters()
	conv := sourceClusterTwoMasters()
	fl.byAddr[new0].nodesSeq = []string{ownIsolated("n0", new0), conv}
	fl.byAddr[new1].nodesSeq = []string{ownIsolated("n1", new1), conv}

	m := fl.module()
	stream := &applyStream{}
	if err := m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "external-joined",
		Params: mustStruct(t, map[string]any{
			"nodes":        clusterNodesParam(new0, new1),
			"source_nodes": []any{seedDown, seedUp},
			"shards_dest":  2,
		}),
	}, stream); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil || fin.Failed || !fin.Changed {
		t.Fatalf("expected success through the second seed, got %+v", fin)
	}
	// MEET went to the SECOND (live) seed, not to the first (dead).
	assertMeetTargets(t, fl.byAddr[new0], []string{seedUp})
	if got := fin.GetOutput().GetFields()["source_via"].GetStringValue(); got != seedUp {
		t.Errorf("source_via=%q, expected %q (second seed)", got, seedUp)
	}
}
