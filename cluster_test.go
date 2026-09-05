package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"testing"

	pluginv1 "github.com/souls-guild/soul-stack/proto/plugin/gen/go/v1"
)

// clusterConn - fake redisConn for cluster tests: writes each call and responds
// to CLUSTER subcommands by args[1] (MYID/INFO/NODES). On MEET/ADDSLOTS/REPLICATE
// gives "OK". id is unique per node - REPLICATE-asserts check the correct master.
type clusterConn struct {
	cfg   connConfig
	id    string // reply to CLUSTER MYID
	info  string // response to CLUSTER INFO (empty -> form "not formed")
	nodes string // answer to CLUSTER NODES (empty -> exactly nodesCount rows)
	// nodesSeq - sequential responses to CLUSTER NODES (add-node: before/after MEET
	// topology is different). The i-th call returns nodesSeq[i]; outside - last
	// element (or nodes if seq is empty). nodesCalls - NODES call counter.
	nodesSeq   []string
	nodesCalls int
	calls      [][]any
	closed     bool

	// keysInSlot - slot keys for CLUSTER GETKEYSINSLOT (remove-node migration).
	// slot -> single-batch: the first GetKeysInSlot gives the entire slice at once, then it is empty.
	keysInSlot map[int][]string
	// keysInSlotBatches - multi-batch model: slot -> QUEUE of portions. Everyone
	// GetKeysInSlot removes the next portion from the head (simulating that the previous
	// MIGRATE removed its keys from the source), empty queue -> nil (cycle ends).
	// Covers a slot with >migrateBatch keys (several iterations of the migrateOneSlot loop).
	// keysInSlot and keysInSlotBatches on the SAME slot are not set at the same time.
	keysInSlotBatches map[int][][]string

	// infoRepl - response to INFO replication (failover-takeover sync-gate reads
	// master_link_status/role). Empty -> "" (parseInfoSection will return an empty map ->
	// role/master_link_status are missing, sync-gate is treated as non-standard INFO).
	infoRepl string

	// forgetErr - error on CLUSTER FORGET (forget-external idempotency: node
	// I've already forgotten the old one -> "Unknown node", swallowed). nil -> FORGET successful ("OK").
	forgetErr error
}

func (c *clusterConn) Do(_ context.Context, args ...any) (string, error) {
	c.calls = append(c.calls, args)
	if len(args) >= 2 {
		v0, _ := args[0].(string)
		v1, _ := args[1].(string)
		if strings.EqualFold(v0, "CLUSTER") {
			switch strings.ToUpper(v1) {
			case "MYID":
				return c.id, nil
			case "INFO":
				return c.info, nil
			case "NODES":
				return c.nodesResponse(), nil
			case "FORGET":
				if c.forgetErr != nil {
					return "", c.forgetErr
				}
				return "OK", nil
			}
		}
		// INFO replication - sync-gate failover-takeover (master_link_status/role).
		if strings.EqualFold(v0, "INFO") && strings.EqualFold(v1, "replication") {
			return c.infoRepl, nil
		}
	}
	return "OK", nil
}

// GetKeysInSlot simulates CLUSTER GETKEYSINSLOT go-redis: migration loop
// migrateOneSlot rotates as long as the slice is not empty. Keys are returned WITHOUT loss
// delimiters (a name with a space remains one element) - this checks
// whitespace-key lossless test. Two source forms per slot (mutually exclusive):
//
//   - keysInSlot[slot] - single-batch: the entire slice at once, then nil;
//   - keysInSlotBatches[slot] - multi-batch: QUEUE of batches, one per call
//     (simulating that the previous MIGRATE removed the chunk from the source), then nil.
//
// Multi-batch covers the slot with >migrateBatch keys: multiple iterations of the loop,
// where each portion is a separate MIGRATE. The pool of all portions is required to move.
func (c *clusterConn) GetKeysInSlot(_ context.Context, slot, _ int) ([]string, error) {
	if q := c.keysInSlotBatches[slot]; len(q) > 0 {
		batch := q[0]
		c.keysInSlotBatches[slot] = q[1:]
		return batch, nil
	}
	if c.keysInSlot == nil {
		return nil, nil
	}
	batch := c.keysInSlot[slot]
	if len(batch) == 0 {
		return nil, nil
	}
	// We give away the entire remaining batch at once (single-batch test holds <= migrateBatch
	// keys) and empty - the next call will return nil -> the cycle ends.
	c.keysInSlot[slot] = nil
	return batch, nil
}

// nodesResponse gives the current response to CLUSTER NODES taking into account nodesSeq.
func (c *clusterConn) nodesResponse() string {
	idx := c.nodesCalls
	c.nodesCalls++
	if len(c.nodesSeq) == 0 {
		return c.nodes
	}
	if idx >= len(c.nodesSeq) {
		return c.nodesSeq[len(c.nodesSeq)-1]
	}
	return c.nodesSeq[idx]
}

// ConfigGet - cluster-state does not call CONFIG GET, stub for the redisConn interface.
func (c *clusterConn) ConfigGet(_ context.Context, param string) (map[string]string, error) {
	return map[string]string{param: ""}, nil
}

// AclList - cluster-state ACL does not touch, stub for redisConn interface.
func (c *clusterConn) AclList(_ context.Context) ([]string, error) { return nil, nil }

func (c *clusterConn) Close() error { c.closed = true; return nil }

// clusterFleet - a set of fake nodes distributed via addr. registry records which
// Every connection has left the node (for per-node assert ADDSLOTS/REPLICATE).
type clusterFleet struct {
	byAddr map[string]*clusterConn
}

func newFleet(addrs ...string) *clusterFleet {
	fl := &clusterFleet{byAddr: make(map[string]*clusterConn, len(addrs))}
	for i, a := range addrs {
		fl.byAddr[a] = &clusterConn{id: fmt.Sprintf("nodeid-%d", i)}
	}
	return fl
}

// nodesView - output common to all nodes CLUSTER NODES (gossip converged): by line
// to the node. The line carries the REAL node-id of the node (the one that gives it CLUSTER MYID) -
// gossip-gate needs this before REPLICATE (the replica node must see the node-id
// your master in the local CLUSTER NODES). countClusterNodes counts rows.
func (fl *clusterFleet) setConvergedNodesView() {
	lines := make([]string, 0, len(fl.byAddr))
	for addr, c := range fl.byAddr {
		lines = append(lines, c.id+" "+addr+"@16379 master - 0 0 0 connected")
	}
	view := strings.Join(lines, "\n")
	for _, c := range fl.byAddr {
		c.nodes = view
	}
}

func (fl *clusterFleet) module() *RedisModule {
	return &RedisModule{
		connect: func(_ context.Context, cfg connConfig) (redisConn, error) {
			c, ok := fl.byAddr[cfg.addr]
			if !ok {
				return nil, fmt.Errorf("no fake node for addr %q", cfg.addr)
			}
			c.cfg = cfg
			return c, nil
		},
	}
}

// clusterNodesParam builds params.nodes-map from set addr(key="node-<i>").
func clusterNodesParam(addrs ...string) map[string]any {
	nodes := map[string]any{}
	for i, a := range addrs {
		nodes[fmt.Sprintf("node-%d", i)] = map[string]any{"addr": a}
	}
	return nodes
}

// --- Validate: cluster ---

func TestValidate_ClusterRejectsEmptyNodes(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.cluster().Validate(context.Background(), &pluginv1.ValidateRequest{
		State:  "created",
		Params: mustStruct(t, map[string]any{"nodes": map[string]any{}}),
	})
	if reply.Ok {
		t.Fatal("waited Ok=false on empty nodes")
	}
}

// TestValidate_ClusterActionsDoNotShareParams - each cluster action validates
// against ITS OWN params, not the object's union. Before NIM-766 all seven shared
// one state and one declared input, so `nodes` (a created param) satisfied nothing
// and refused nothing on a reshard; now it is simply not what `resharded` reads.
func TestValidate_ClusterActionsDoNotShareParams(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.cluster().Validate(context.Background(), &pluginv1.ValidateRequest{
		State: "resharded",
		Params: mustStruct(t, map[string]any{
			"nodes": clusterNodesParam("127.0.0.1:6379"),
		}),
	})
	if reply.Ok {
		t.Fatal("waited Ok=false: `nodes` is a created param, resharded needs from/to/slots")
	}
}

func TestValidate_ClusterRejectsIndivisibleNodes(t *testing.T) {
	m := &RedisModule{}
	// 5 nodes are not divisible by shardSize=2 (1 master + 1 replica).
	reply, _ := m.cluster().Validate(context.Background(), &pluginv1.ValidateRequest{
		State: "created",
		Params: mustStruct(t, map[string]any{
			"replicas_per_shard": 1,
			"nodes": clusterNodesParam(
				"10.0.0.1:6379", "10.0.0.2:6379", "10.0.0.3:6379",
				"10.0.0.4:6379", "10.0.0.5:6379"),
		}),
	})
	if reply.Ok {
		t.Fatal("waited Ok=false: 5 nodes is not divisible by shard size 2")
	}
}

func TestValidate_ClusterHappy(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.cluster().Validate(context.Background(), &pluginv1.ValidateRequest{
		State: "created",
		Params: mustStruct(t, map[string]any{
			"replicas_per_shard": 1,
			"nodes":              clusterNodesParam("10.0.0.1:6379", "10.0.0.2:6379"),
		}),
	})
	if !reply.Ok || len(reply.Errors) != 0 {
		t.Fatalf("waited Ok=true, got %+v", reply)
	}
}

// --- slot-allocation (16384 division determinism) ---

func TestAllocateSlots_FullCoverageAndRemainder(t *testing.T) {
	for _, shards := range []int{1, 2, 3, 6, 7} {
		ranges := allocateSlots(shards)
		if len(ranges) != shards {
			t.Fatalf("shards=%d: expected %d ranges, got %d", shards, shards, len(ranges))
		}
		// Continuous coverage 0..16383 without holes or intersections.
		expect := 0
		total := 0
		for i, r := range ranges {
			if r.from != expect {
				t.Fatalf("shards=%d range[%d].from=%d, expected %d", shards, i, r.from, expect)
			}
			if r.to < r.from {
				t.Fatalf("shards=%d range[%d] empty: [%d-%d]", shards, i, r.from, r.to)
			}
			expect = r.to + 1
			total += r.to - r.from + 1
		}
		if total != totalSlots {
			t.Fatalf("shards=%d: %d slots covered, waiting for %d", shards, total, totalSlots)
		}
		// The rest goes to the first masters: the sizes are monotonically non-increasing.
		for i := 1; i < len(ranges); i++ {
			prev := ranges[i-1].to - ranges[i-1].from + 1
			cur := ranges[i].to - ranges[i].from + 1
			if cur > prev {
				t.Fatalf("shards=%d: range size[%d]=%d > [%d]=%d (remainder not first)", shards, i, cur, i-1, prev)
			}
		}
	}
}

// --- Apply create: happy-path (MEET/ADDSLOTS/REPLICATE + full coverage) ---

func TestApplyClusterCreate_HappyPath(t *testing.T) {
	addrs := []string{"10.0.0.1:6379", "10.0.0.2:6379", "10.0.0.3:6379", "10.0.0.4:6379"}
	fl := newFleet(addrs...)
	fl.setConvergedNodesView() // gossip converges immediately
	m := fl.module()
	stream := &applyStream{}

	err := m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "created",
		Params: mustStruct(t, map[string]any{
			"password":           secretPass,
			"replicas_per_shard": 1,
			"nodes":              clusterNodesParam(addrs...),
		}),
	}, stream)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil || fin.Failed || !fin.Changed {
		t.Fatalf("expected success changed=true, got %+v", fin)
	}

	// node-0/node-1 - masters (first shards=2 by sort keys), node-2/node-3 - replicas.
	master0 := fl.byAddr[addrs[0]]
	master1 := fl.byAddr[addrs[1]]
	replica2 := fl.byAddr[addrs[2]]
	replica3 := fl.byAddr[addrs[3]]

	// Hub (first node = master0) sends MEET to all others via ip:port.
	assertMeetTargets(t, master0, []string{"10.0.0.2:6379", "10.0.0.3:6379", "10.0.0.4:6379"})

	// ADDSLOTS for masters only, ranges are deterministic and fully cover 16384.
	r0 := assertAddSlots(t, master0)
	r1 := assertAddSlots(t, master1)
	assertNoAddSlots(t, replica2)
	assertNoAddSlots(t, replica3)
	assertFullSlotCoverage(t, r0, r1)

	// REPLICATE: replicas are bound to their master (round-robin j%shards).
	// node-2 (j=0) -> master0, node-3 (j=1) -> master1.
	assertReplicateTo(t, replica2, master0.id)
	assertReplicateTo(t, replica3, master1.id)

	assertEventsNoSecret(t, stream)
	for addr, c := range fl.byAddr {
		assertNoClusterSecret(t, addr, c)
	}
}

// --- Apply create: idempotency (already generated -> changed=false) ---

func TestApplyClusterCreate_AlreadyFormedNoOp(t *testing.T) {
	addrs := []string{"10.0.0.1:6379", "10.0.0.2:6379"}
	fl := newFleet(addrs...)
	// The first master reports the formed cluster: state ok, all nodes, all slots.
	fl.byAddr[addrs[0]].info = "cluster_state:ok\n" +
		"cluster_slots_assigned:16384\n" +
		"cluster_known_nodes:2\n"
	m := fl.module()
	stream := &applyStream{}

	err := m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "created",
		Params: mustStruct(t, map[string]any{
			"replicas_per_shard": 0,
			"nodes":              clusterNodesParam(addrs...),
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
		t.Error("expected changed=false on an already formed cluster (no-op)")
	}
	// No-op: Neither MEET nor ADDSLOTS should be called.
	for addr, c := range fl.byAddr {
		for _, call := range c.calls {
			if isClusterSub(call, "MEET") || isClusterSub(call, "ADDSLOTS") {
				t.Errorf("node %s: no-op broken, caused by %v", addr, call)
			}
		}
	}
}

// convergedNodesViewMastersOnly builds CLUSTER NODES, where ALL fleet nodes are
// master (gossip agreed, slots can be assigned, but replicas are NOT configured).
// rows: addr -> range of slots (nil -> master without slots). node-id is taken from
// fake node (its CLUSTER MYID), which requires gossip-gate before REPLICATE.
func (fl *clusterFleet) convergedNodesViewMastersOnly(slotsByAddr map[string][2]int) string {
	lines := make([]string, 0, len(fl.byAddr))
	for addr, c := range fl.byAddr {
		line := c.id + " " + addr + "@16379 master - 0 0 0 connected"
		if r, ok := slotsByAddr[addr]; ok {
			line += fmt.Sprintf(" %d-%d", r[0], r[1])
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n")
}

// --- Apply create: PARTIAL topology (6 master, no replicas configured) -> complete REPLICATE ---
//
// LIVE BUG (proven at the stand): the first REPLICATE fell on gossip-timing ("Unknown
// node"), the cluster is frozen as N master (slots are full, cluster_state: ok), and idempotent is
// gate clusterAlreadyFormed looked ONLY cluster_state/known_nodes/slots_assigned
// -> reported "already formed" -> no-op -> replicas were NEVER completed. Here
// 4 nodes (plan: 2 master + 2 replica), live cluster converged as 4 master without replicas:
// the plugin MUST complete the REPLICATE of the missing replicas (changed=true), WITHOUT touching
// MEET/ADDSLOTS (slots are already in place).
func TestApplyClusterCreate_PartialTopologyCompletesReplicas(t *testing.T) {
	addrs := []string{"10.0.0.1:6379", "10.0.0.2:6379", "10.0.0.3:6379", "10.0.0.4:6379"}
	fl := newFleet(addrs...)
	// Plan (sort of keys node-0..node-3): master node-0/node-1, replica node-2/node-3.
	// Round-robin replicas: node-2 (j=0) -> master0, node-3 (j=1) -> master1.
	master0 := fl.byAddr[addrs[0]]
	master1 := fl.byAddr[addrs[1]]
	replica2 := fl.byAddr[addrs[2]]
	replica3 := fl.byAddr[addrs[3]]

	// Live topology: ALL 4 nodes are master (replicas are not configured). slots are scattered
	// between the first two (as if ADDSLOTS passed, but REPLICATE did not).
	view := fl.convergedNodesViewMastersOnly(map[string][2]int{
		addrs[0]: {0, 8191},
		addrs[1]: {8192, 16383},
	})
	for _, c := range fl.byAddr {
		c.nodes = view
	}
	// CLUSTER INFO of the first master: state ok, all nodes, all slots - exactly those three
	// the conditions under which the old gate erroneously said "formed".
	master0.info = "cluster_state:ok\ncluster_slots_assigned:16384\ncluster_known_nodes:4\n"

	m := fl.module()
	stream := &applyStream{}

	err := m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "created",
		Params: mustStruct(t, map[string]any{
			"password":           secretPass,
			"replicas_per_shard": 1,
			"nodes":              clusterNodesParam(addrs...),
		}),
	}, stream)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil || fin.Failed {
		t.Fatalf("expected success, got %+v", fin)
	}
	if !fin.Changed {
		t.Fatal("waited changed=true: partial-topology must complete REPLICATE")
	}

	// REPLICATE is completed on BOTH replica nodes to their masters (by live node-id).
	assertReplicateTo(t, replica2, master0.id)
	assertReplicateTo(t, replica3, master1.id)

	// ADDSLOTS are NOT reassigned (slots are already in place - otherwise "Slot is already busy").
	assertNoAddSlots(t, master0)
	assertNoAddSlots(t, master1)
	// REPLICATE is NOT called on masters.
	for _, c := range []*clusterConn{master0, master1} {
		for _, call := range c.calls {
			if isClusterSub(call, "REPLICATE") {
				t.Errorf("the master should not receive REPLICATE: %v", call)
			}
		}
	}

	assertEventsNoSecret(t, stream)
	for addr, c := range fl.byAddr {
		assertNoClusterSecret(t, addr, c)
	}
}

// --- Apply create: FULL topology (master+replica configured) -> no-op ---
//
// Guard versus over-fix: if the live cluster is already full (replicas are in place) - gate
// must remain idempotent (changed=false, no REPLICATE/ADDSLOTS).
func TestApplyClusterCreate_FullyFormedWithReplicasNoOp(t *testing.T) {
	addrs := []string{"10.0.0.1:6379", "10.0.0.2:6379", "10.0.0.3:6379", "10.0.0.4:6379"}
	fl := newFleet(addrs...)
	master0 := fl.byAddr[addrs[0]]
	master1 := fl.byAddr[addrs[1]]
	replica2 := fl.byAddr[addrs[2]]
	replica3 := fl.byAddr[addrs[3]]

	// FULL live topology: node-2 is a replica of master0, node-3 is a replica of master1.
	view := strings.Join([]string{
		master0.id + " " + addrs[0] + "@16379 master - 0 0 0 connected 0-8191",
		master1.id + " " + addrs[1] + "@16379 master - 0 0 0 connected 8192-16383",
		replica2.id + " " + addrs[2] + "@16379 slave " + master0.id + " 0 0 0 connected",
		replica3.id + " " + addrs[3] + "@16379 slave " + master1.id + " 0 0 0 connected",
	}, "\n")
	for _, c := range fl.byAddr {
		c.nodes = view
	}
	master0.info = "cluster_state:ok\ncluster_slots_assigned:16384\ncluster_known_nodes:4\n"

	m := fl.module()
	stream := &applyStream{}

	err := m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "created",
		Params: mustStruct(t, map[string]any{
			"replicas_per_shard": 1,
			"nodes":              clusterNodesParam(addrs...),
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
		t.Error("waited changed=false: the cluster is already fully formed (no-op)")
	}
	// No-op: no MEET, no ADDSLOTS, no REPLICATE.
	for addr, c := range fl.byAddr {
		for _, call := range c.calls {
			if isClusterSub(call, "MEET") || isClusterSub(call, "ADDSLOTS") || isClusterSub(call, "REPLICATE") {
				t.Errorf("node %s: no-op broken, caused by %v", addr, call)
			}
		}
	}
}

// --- Apply create: gossip-gate before REPLICATE (master-id is not yet visible to the replica) ---
//
// Root live-"Unknown node": forming from scratch sends REPLICATE on the replica node
// immediately after ADDSLOTS, but the node might not yet have received gossip about the master -> it
// local CLUSTER NODES does not contain master node-id -> REPLICATE crashes. Fix
// waits (bounded retry) until the replica node sees the master-id. Here is a replica node
// CLUSTER NODES makes the first calls WITHOUT a master line, then with the line:
// The plugin must wait and not fall.
func TestApplyClusterCreate_GossipGateBeforeReplicate(t *testing.T) {
	addrs := []string{"10.0.0.1:6379", "10.0.0.2:6379"}
	fl := newFleet(addrs...)
	fl.setConvergedNodesView() // hub sees both nodes (number) - MEET-gate passes

	master0 := fl.byAddr[addrs[0]]
	replica1 := fl.byAddr[addrs[1]]

	// The replica node (node-1) does NOT first see the master in the local topology (only
	// himself), then the gossip informs the master. master0.id is what CLUSTER MYID will return
	// master and what must appear in the NODES of the replica before REPLICATE.
	selfOnly := replica1.id + " " + addrs[1] + "@16379 myself,master - 0 0 0 connected"
	withMaster := selfOnly + "\n" + master0.id + " " + addrs[0] + "@16379 master - 0 0 0 connected 0-16383"
	// The first 2 NODES responses of the replica are without a master, the 3rd and further are with a master. hub
	// (master0) already has its own converged-view (setConvergedNodesView above).
	replica1.nodesSeq = []string{selfOnly, selfOnly, withMaster}

	m := fl.module()
	stream := &applyStream{}

	err := m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "created",
		Params: mustStruct(t, map[string]any{
			"password":           secretPass,
			"replicas_per_shard": 1,
			"nodes":              clusterNodesParam(addrs...),
		}),
	}, stream)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil || fin.Failed {
		t.Fatalf("expected success (gossip-gate expected the master), got %+v", fin)
	}
	if !fin.Changed {
		t.Fatal("waited changed=true")
	}
	// REPLICATE took place to master0 AFTER master-id became visible to the replica.
	assertReplicateTo(t, replica1, master0.id)
	// The replica node polled the local CLUSTER NODES at least three times (2 empty + 1 s
	// master) - gossip-gate really waited, and did not shoot blindly.
	if replica1.nodesCalls < 3 {
		t.Errorf("waited >=3 polls CLUSTER NODES replicas (gossip-gate), got %d", replica1.nodesCalls)
	}
}

// --- Determinism: one input -> one layout (multiple runs) ---

func TestApplyClusterCreate_LayoutDeterministic(t *testing.T) {
	addrs := []string{"10.0.0.4:6379", "10.0.0.1:6379", "10.0.0.3:6379", "10.0.0.2:6379", "10.0.0.6:6379", "10.0.0.5:6379"}

	var first string
	for run := 0; run < 5; run++ {
		fl := newFleet(addrs...)
		fl.setConvergedNodesView()
		m := fl.module()
		stream := &applyStream{}
		err := m.cluster().Apply(&pluginv1.ApplyRequest{
			State: "created",
			Params: mustStruct(t, map[string]any{
				"replicas_per_shard": 1,
				"nodes":              clusterNodesParam(addrs...),
			}),
		}, stream)
		if err != nil {
			t.Fatalf("run %d Apply: %v", run, err)
		}
		got := stream.final().GetOutput().GetFields()["layout"].GetStringValue()
		if got == "" {
			t.Fatalf("run %d: empty layout", run)
		}
		if run == 0 {
			first = got
			continue
		}
		if got != first {
			t.Fatalf("the layout is non-deterministic: run0=%q run%d=%q", first, run, got)
		}
	}
}

// --- Determinism by key, not by addr-line: same keys -> same role ---

func TestBuildClusterPlan_RoleByKeyOrder(t *testing.T) {
	// The keys are specified explicitly - the layout must follow the SORTING of the keys.
	nodes := []clusterNode{
		{key: "z", addr: "10.0.0.9:6379", ip: "10.0.0.9", port: 6379},
		{key: "a", addr: "10.0.0.1:6379", ip: "10.0.0.1", port: 6379},
	}
	sortNodesByKey(nodes)
	plan, err := buildClusterPlan(nodes, 1)
	if err != nil {
		t.Fatalf("buildClusterPlan: %v", err)
	}
	if len(plan.masters) != 1 || plan.masters[0].key != "a" {
		t.Fatalf("master must be key 'a' (first by sort), got %+v", plan.masters)
	}
	if len(plan.replicas) != 1 || plan.replicas[0].key != "z" {
		t.Fatalf("replica must be key 'z', got %+v", plan.replicas)
	}
}

// --- Negative Validate: duplicate the happy binding for an empty/invalid input ---

func TestValidate_ClusterRejectsNegativeReplicas(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.cluster().Validate(context.Background(), &pluginv1.ValidateRequest{
		State: "created",
		Params: mustStruct(t, map[string]any{
			"replicas_per_shard": -1,
			"nodes":              clusterNodesParam("10.0.0.1:6379"),
		}),
	})
	if reply.Ok {
		t.Fatal("waited Ok=false for negative replicas_per_shard")
	}
}

// --- Password does not leak (Apply create) ---

func TestApplyClusterCreate_NoSecretLeak(t *testing.T) {
	addrs := []string{"10.0.0.1:6379", "10.0.0.2:6379"}
	fl := newFleet(addrs...)
	fl.setConvergedNodesView()
	m := fl.module()
	stream := &applyStream{}

	_ = m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "created",
		Params: mustStruct(t, map[string]any{
			"password":           secretPass,
			"replicas_per_shard": 0,
			"nodes":              clusterNodesParam(addrs...),
		}),
	}, stream)

	assertEventsNoSecret(t, stream)
	for addr, c := range fl.byAddr {
		// The password has gone into the connection.
		if c.cfg.password != secretPass {
			t.Errorf("node %s: password did not reach the connection", addr)
		}
		assertNoClusterSecret(t, addr, c)
	}
}

func TestApplyClusterCreate_ConnectFailureNoLeak(t *testing.T) {
	m := &RedisModule{
		connect: func(_ context.Context, cfg connConfig) (redisConn, error) {
			return nil, errors.New("dial failed for AUTH " + cfg.password)
		},
	}
	stream := &applyStream{}
	_ = m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "created",
		Params: mustStruct(t, map[string]any{
			"password":           secretPass,
			"replicas_per_shard": 0,
			"nodes":              clusterNodesParam("10.0.0.1:6379", "10.0.0.2:6379"),
		}),
	}, stream)

	fin := stream.final()
	if fin == nil || !fin.Failed {
		t.Fatalf("waited failed=true, got %+v", fin)
	}
	assertEventsNoSecret(t, stream)
}

// =============================== add-node ====================================

// clusterNodesTable builds realistic CLUSTER NODES output from row specifications.
// Each line: "<id> <ipPort>@<cport> <flags> <masterID|-> 0 0 0 connected".
type nodeRowSpec struct {
	id       string
	ipPort   string
	master   bool
	masterID string // for a replica
}

func clusterNodesTable(rows ...nodeRowSpec) string {
	lines := make([]string, 0, len(rows))
	for _, r := range rows {
		flags := "slave"
		mid := r.masterID
		if r.master {
			flags = "master"
			mid = "-"
		}
		if mid == "" {
			mid = "-"
		}
		cport := r.ipPort // @cport doesn't parse - any suffix will do
		lines = append(lines, fmt.Sprintf("%s %s@%s %s %s 0 0 0 connected", r.id, r.ipPort, cport, flags, mid))
	}
	return strings.Join(lines, "\n")
}

// formedTwoMasterSeed - seed with two masters m0/m1 (for add-node scenarios).
// Before MEET: 2 lines; after MEET: + newbie line (gossip agreed).
func formedTwoMasterSeed(newIPPort string) (before, after string) {
	before = clusterNodesTable(
		nodeRowSpec{id: "m0id", ipPort: "10.0.0.1:6379", master: true},
		nodeRowSpec{id: "m1id", ipPort: "10.0.0.2:6379", master: true},
	)
	after = clusterNodesTable(
		nodeRowSpec{id: "m0id", ipPort: "10.0.0.1:6379", master: true},
		nodeRowSpec{id: "m1id", ipPort: "10.0.0.2:6379", master: true},
		nodeRowSpec{id: "newid", ipPort: newIPPort, master: true},
	)
	return before, after
}

func nodeMapParam(addr string) map[string]any { return map[string]any{"addr": addr} }

// --- Validate: add-node ---

func TestValidate_AddNodeRequiresNewNodeAndSeed(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.cluster().Validate(context.Background(), &pluginv1.ValidateRequest{
		State:  "node-added",
		Params: mustStruct(t, map[string]any{}),
	})
	if reply.Ok {
		t.Fatal("expected Ok=false without new_node/seed")
	}
}

func TestValidate_AddNodeRejectsBadRole(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.cluster().Validate(context.Background(), &pluginv1.ValidateRequest{
		State: "node-added",
		Params: mustStruct(t, map[string]any{
			"new_node": nodeMapParam("10.0.0.9:6379"),
			"seed":     nodeMapParam("10.0.0.1:6379"),
			"role":     "arbiter",
		}),
	})
	if reply.Ok {
		t.Fatal("expected Ok=false for unknown role")
	}
}

func TestValidate_AddNodeHappy(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.cluster().Validate(context.Background(), &pluginv1.ValidateRequest{
		State: "node-added",
		Params: mustStruct(t, map[string]any{
			"new_node": nodeMapParam("10.0.0.9:6379"),
			"seed":     nodeMapParam("10.0.0.1:6379"),
			"role":     "replica",
		}),
	})
	if !reply.Ok || len(reply.Errors) != 0 {
		t.Fatalf("waited Ok=true, got %+v", reply)
	}
}

// --- Apply add-node: replica, auto-select master (least loaded) ---

func TestApplyClusterAddNode_ReplicaAutoMaster(t *testing.T) {
	newAddr, seedAddr := "10.0.0.9:6379", "10.0.0.1:6379"
	fl := newFleet(newAddr, seedAddr)
	// m1 already carries one replica -> auto-selection must fall on m0 (0 replicas).
	before := clusterNodesTable(
		nodeRowSpec{id: "m0id", ipPort: "10.0.0.1:6379", master: true},
		nodeRowSpec{id: "m1id", ipPort: "10.0.0.2:6379", master: true},
		nodeRowSpec{id: "r1id", ipPort: "10.0.0.3:6379", masterID: "m1id"},
	)
	// after - gossip agreed: + newbie (len(before)+1 = 4 lines).
	after := clusterNodesTable(
		nodeRowSpec{id: "m0id", ipPort: "10.0.0.1:6379", master: true},
		nodeRowSpec{id: "m1id", ipPort: "10.0.0.2:6379", master: true},
		nodeRowSpec{id: "r1id", ipPort: "10.0.0.3:6379", masterID: "m1id"},
		nodeRowSpec{id: "newid", ipPort: "10.0.0.9:6379", masterID: "m0id"},
	)
	fl.byAddr[seedAddr].nodesSeq = []string{before, after}
	m := fl.module()
	stream := &applyStream{}

	err := m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "node-added",
		Params: mustStruct(t, map[string]any{
			"password": secretPass,
			"new_node": nodeMapParam(newAddr),
			"seed":     nodeMapParam(seedAddr),
			"role":     "replica",
		}),
	}, stream)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil || fin.Failed || !fin.Changed {
		t.Fatalf("expected success changed=true, got %+v", fin)
	}

	// seed sends MEET to the newcomer via ip:port.
	assertMeetTargets(t, fl.byAddr[seedAddr], []string{"10.0.0.9:6379"})
	// REPLICATE executed ON NOVICE to m0id (least loaded master).
	assertReplicateTo(t, fl.byAddr[newAddr], "m0id")
	if got := fin.GetOutput().GetFields()["master_id"].GetStringValue(); got != "m0id" {
		t.Errorf("master_id=%q, expected m0id (auto-select the least loaded one)", got)
	}

	assertEventsNoSecret(t, stream)
	for addr, c := range fl.byAddr {
		assertNoClusterSecret(t, addr, c)
	}
}

// --- Apply add-node: replica, explicit master ---

func TestApplyClusterAddNode_ReplicaExplicitMaster(t *testing.T) {
	newAddr, seedAddr := "10.0.0.9:6379", "10.0.0.1:6379"
	fl := newFleet(newAddr, seedAddr)
	before, after := formedTwoMasterSeed("10.0.0.9:6379")
	fl.byAddr[seedAddr].nodesSeq = []string{before, after}
	m := fl.module()
	stream := &applyStream{}

	err := m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "node-added",
		Params: mustStruct(t, map[string]any{
			"new_node": nodeMapParam(newAddr),
			"seed":     nodeMapParam(seedAddr),
			"role":     "replica",
			"master":   nodeMapParam("10.0.0.2:6379"), // m1
		}),
	}, stream)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	fin := stream.final()
	if fin == nil || fin.Failed || !fin.Changed {
		t.Fatalf("expected success changed=true, got %+v", fin)
	}
	// Explicit master 10.0.0.2 -> m1id, despite the fact that m0 is less loaded.
	assertReplicateTo(t, fl.byAddr[newAddr], "m1id")
}

// Explicit master is not from the cluster -> error (failed=true), password does not leak.
func TestApplyClusterAddNode_ReplicaUnknownMaster(t *testing.T) {
	newAddr, seedAddr := "10.0.0.9:6379", "10.0.0.1:6379"
	fl := newFleet(newAddr, seedAddr)
	before, _ := formedTwoMasterSeed("10.0.0.9:6379")
	fl.byAddr[seedAddr].nodesSeq = []string{before}
	m := fl.module()
	stream := &applyStream{}

	_ = m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "node-added",
		Params: mustStruct(t, map[string]any{
			"password": secretPass,
			"new_node": nodeMapParam(newAddr),
			"seed":     nodeMapParam(seedAddr),
			"role":     "replica",
			"master":   nodeMapParam("10.0.0.7:6379"), // not in the cluster
		}),
	}, stream)

	fin := stream.final()
	if fin == nil || !fin.Failed {
		t.Fatalf("expected failed=true on master outside the cluster, got %+v", fin)
	}
	// MEET should not have been executed (the wizard resolves BEFORE MEET).
	for _, call := range fl.byAddr[seedAddr].calls {
		if isClusterSub(call, "MEET") {
			t.Error("MEET should not be called when master is unresolvable")
		}
	}
	assertEventsNoSecret(t, stream)
}

// --- Apply add-node: master (empty, no slots) ---

func TestApplyClusterAddNode_EmptyMaster(t *testing.T) {
	newAddr, seedAddr := "10.0.0.9:6379", "10.0.0.1:6379"
	fl := newFleet(newAddr, seedAddr)
	before, after := formedTwoMasterSeed("10.0.0.9:6379")
	fl.byAddr[seedAddr].nodesSeq = []string{before, after}
	m := fl.module()
	stream := &applyStream{}

	err := m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "node-added",
		Params: mustStruct(t, map[string]any{
			"new_node": nodeMapParam(newAddr),
			"seed":     nodeMapParam(seedAddr),
			"role":     "master",
		}),
	}, stream)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	fin := stream.final()
	if fin == nil || fin.Failed || !fin.Changed {
		t.Fatalf("expected success changed=true, got %+v", fin)
	}
	if got := fin.GetOutput().GetFields()["role"].GetStringValue(); got != "master" {
		t.Errorf("role=%q, waiting for master", got)
	}
	// add-node master DOES NOT move slots: neither ADDSLOTS nor REPLICATE.
	assertNoAddSlots(t, fl.byAddr[newAddr])
	for _, call := range fl.byAddr[newAddr].calls {
		if isClusterSub(call, "REPLICATE") {
			t.Error("empty master should not REPLICATE")
		}
	}
}

// --- Apply add-node: idempotency (node is already in the cluster -> no-op) ---

func TestApplyClusterAddNode_AlreadyMemberNoOp(t *testing.T) {
	newAddr, seedAddr := "10.0.0.9:6379", "10.0.0.1:6379"
	fl := newFleet(newAddr, seedAddr)
	// The seed topology ALREADY contains newcomer 10.0.0.9.
	fl.byAddr[seedAddr].nodes = clusterNodesTable(
		nodeRowSpec{id: "m0id", ipPort: "10.0.0.1:6379", master: true},
		nodeRowSpec{id: "newid", ipPort: "10.0.0.9:6379", master: true},
	)
	m := fl.module()
	stream := &applyStream{}

	err := m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "node-added",
		Params: mustStruct(t, map[string]any{
			"new_node": nodeMapParam(newAddr),
			"seed":     nodeMapParam(seedAddr),
			"role":     "replica",
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
		t.Error("expected changed=false: node is already in the cluster (no-op)")
	}
	// No-op: neither MEET nor REPLICATE.
	for addr, c := range fl.byAddr {
		for _, call := range c.calls {
			if isClusterSub(call, "MEET") || isClusterSub(call, "REPLICATE") {
				t.Errorf("node %s: no-op broken, caused by %v", addr, call)
			}
		}
	}
}

// --- Apply add-node: connection file to seed does not contain password ---

func TestApplyClusterAddNode_SeedConnectFailNoLeak(t *testing.T) {
	m := &RedisModule{
		connect: func(_ context.Context, cfg connConfig) (redisConn, error) {
			return nil, errors.New("dial failed for AUTH " + cfg.password)
		},
	}
	stream := &applyStream{}
	_ = m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "node-added",
		Params: mustStruct(t, map[string]any{
			"password": secretPass,
			"new_node": nodeMapParam("10.0.0.9:6379"),
			"seed":     nodeMapParam("10.0.0.1:6379"),
			"role":     "replica",
		}),
	}, stream)

	fin := stream.final()
	if fin == nil || !fin.Failed {
		t.Fatalf("waited failed=true, got %+v", fin)
	}
	assertEventsNoSecret(t, stream)
}

// --- parseClusterNodesTable: parse topology rows ---

func TestParseClusterNodesTable(t *testing.T) {
	table := clusterNodesTable(
		nodeRowSpec{id: "m0id", ipPort: "10.0.0.1:6379", master: true},
		nodeRowSpec{id: "r0id", ipPort: "10.0.0.3:6379", masterID: "m0id"},
	)
	rows := parseClusterNodesTable(table)
	if len(rows) != 2 {
		t.Fatalf("waited 2 lines, got %d", len(rows))
	}
	if rows[0].id != "m0id" || rows[0].ipPort != "10.0.0.1:6379" || !rows[0].isMaster {
		t.Errorf("master line parsed incorrectly: %+v", rows[0])
	}
	if rows[1].isMaster || rows[1].masterID != "m0id" {
		t.Errorf("replica line parsed incorrectly: %+v", rows[1])
	}
	// @cport should be cut off.
	if strings.Contains(rows[0].ipPort, "@") {
		t.Errorf("ipPort carries @cport: %q", rows[0].ipPort)
	}
}

// --- assert helpers (cluster) ---

func sortNodesByKey(nodes []clusterNode) {
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].key < nodes[j].key })
}

func isClusterSub(call []any, sub string) bool {
	if len(call) < 2 {
		return false
	}
	v0, _ := call[0].(string)
	v1, _ := call[1].(string)
	return strings.EqualFold(v0, "CLUSTER") && strings.EqualFold(v1, sub)
}

// assertMeetTargets checks the set of ip:ports to which the node sent CLUSTER MEET.
func assertMeetTargets(t *testing.T, c *clusterConn, want []string) {
	t.Helper()
	var got []string
	for _, call := range c.calls {
		if isClusterSub(call, "MEET") {
			ip, _ := call[2].(string)
			port, _ := call[3].(string)
			got = append(got, ip+":"+port)
		}
	}
	sort.Strings(got)
	sort.Strings(want)
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("MEET targets=%v, expected %v", got, want)
	}
}

// assertAddSlots retrieves a range of slots from CLUSTER ADDSLOTS on the node; requires
// exactly one such challenge.
func assertAddSlots(t *testing.T, c *clusterConn) slotRange {
	t.Helper()
	var found *slotRange
	for _, call := range c.calls {
		if !isClusterSub(call, "ADDSLOTS") {
			continue
		}
		if found != nil {
			t.Fatal("waited exactly one ADDSLOTS on the master")
		}
		slots := make([]int, 0, len(call)-2)
		for _, a := range call[2:] {
			s, _ := a.(string)
			n, err := strconv.Atoi(s)
			if err != nil {
				t.Fatalf("ADDSLOTS argument is not a number: %v", a)
			}
			slots = append(slots, n)
		}
		if len(slots) == 0 {
			t.Fatal("ADDSLOTS without slots")
		}
		// The slots are continuous and increasing.
		for i := 1; i < len(slots); i++ {
			if slots[i] != slots[i-1]+1 {
				t.Fatalf("ADDSLOTS slots are not continuous: %v", slots)
			}
		}
		r := slotRange{from: slots[0], to: slots[len(slots)-1]}
		found = &r
	}
	if found == nil {
		t.Fatal("there were no ADDSLOTS on the master")
	}
	return *found
}

func assertNoAddSlots(t *testing.T, c *clusterConn) {
	t.Helper()
	for _, call := range c.calls {
		if isClusterSub(call, "ADDSLOTS") {
			t.Errorf("there should be no ADDSLOTS on the replica, got %v", call)
		}
	}
}

func assertFullSlotCoverage(t *testing.T, ranges ...slotRange) {
	t.Helper()
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].from < ranges[j].from })
	expect := 0
	for _, r := range ranges {
		if r.from != expect {
			t.Fatalf("hole/slot overlap: waited from=%d, got %d", expect, r.from)
		}
		expect = r.to + 1
	}
	if expect != totalSlots {
		t.Fatalf("covered %d slots, waiting for %d", expect, totalSlots)
	}
}

func assertReplicateTo(t *testing.T, c *clusterConn, masterID string) {
	t.Helper()
	for _, call := range c.calls {
		if isClusterSub(call, "REPLICATE") {
			got, _ := call[2].(string)
			if got != masterID {
				t.Errorf("REPLICATE -> %q, expected %q", got, masterID)
			}
			return
		}
	}
	t.Errorf("there was no CLUSTER REPLICATE on the replica")
}

func assertNoClusterSecret(t *testing.T, addr string, c *clusterConn) {
	t.Helper()
	for i, call := range c.calls {
		for _, a := range call {
			if s, ok := a.(string); ok && strings.Contains(s, secretPass) {
				t.Errorf("node %s command[%d] carries the password: %v", addr, i, call)
			}
		}
	}
}

// ============================= remove-node ===================================

// nodesTableWithSlots collects CLUSTER NODES output from ready rows (clusterNodesTable
// does not carry slots, but remove-node parses them - we build the lines explicitly).
func nodesTableWithSlots(rows ...string) string { return strings.Join(rows, "\n") }

// masterRowSlots builds the master's CLUSTER NODES row with a range of slots.
func masterRowSlots(id, ipPort string, from, to int) string {
	return fmt.Sprintf("%s %s@%s master - 0 0 0 connected %d-%d", id, ipPort, ipPort, from, to)
}

// masterRowNoSlots - master without slots (empty master).
func masterRowNoSlots(id, ipPort string) string {
	return fmt.Sprintf("%s %s@%s master - 0 0 0 connected", id, ipPort, ipPort)
}

// replicaRow - replica (slave) of the master masterID, without slots.
func replicaRow(id, ipPort, masterID string) string {
	return fmt.Sprintf("%s %s@%s slave %s 0 0 0 connected", id, ipPort, ipPort, masterID)
}

func clusterForgetTargets(c *clusterConn) []string {
	var got []string
	for _, call := range c.calls {
		if isClusterSub(call, "FORGET") {
			id, _ := call[2].(string)
			got = append(got, id)
		}
	}
	return got
}

func hasClusterForget(c *clusterConn, removeID string) bool {
	for _, id := range clusterForgetTargets(c) {
		if id == removeID {
			return true
		}
	}
	return false
}

// --- Validate: remove-node ---

func TestValidate_RemoveNodeRequiresNodeAndSeed(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.cluster().Validate(context.Background(), &pluginv1.ValidateRequest{
		State:  "node-removed",
		Params: mustStruct(t, map[string]any{}),
	})
	if reply.Ok {
		t.Fatal("expected Ok=false without node/seed")
	}
}

func TestValidate_RemoveNodeHappy(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.cluster().Validate(context.Background(), &pluginv1.ValidateRequest{
		State: "node-removed",
		Params: mustStruct(t, map[string]any{
			"node": nodeMapParam("10.0.0.3:6379"),
			"seed": nodeMapParam("10.0.0.1:6379"),
		}),
	})
	if !reply.Ok || len(reply.Errors) != 0 {
		t.Fatalf("waited Ok=true, got %+v", reply)
	}
}

// --- Apply remove-node: replica (simply FORGET on all remaining ones) ---

func TestApplyClusterRemoveNode_ReplicaForgetOnly(t *testing.T) {
	removeAddr, seedAddr := "10.0.0.3:6379", "10.0.0.1:6379"
	m0Addr := "10.0.0.1:6379"
	m1Addr := "10.0.0.2:6379"
	fl := newFleet(m0Addr, m1Addr, removeAddr)
	// Topology: m0/m1 - masters with slots, r1 (10.0.0.3) - replica m1.
	table := nodesTableWithSlots(
		masterRowSlots("m0id", "10.0.0.1:6379", 0, 8191),
		masterRowSlots("m1id", "10.0.0.2:6379", 8192, 16383),
		replicaRow("r1id", "10.0.0.3:6379", "m1id"),
	)
	for _, c := range fl.byAddr {
		c.nodes = table
	}
	m := fl.module()
	stream := &applyStream{}

	err := m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "node-removed",
		Params: mustStruct(t, map[string]any{
			"password": secretPass,
			"node":     nodeMapParam(removeAddr),
			"seed":     nodeMapParam(seedAddr),
		}),
	}, stream)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil || fin.Failed || !fin.Changed {
		t.Fatalf("expected success changed=true, got %+v", fin)
	}

	// The replica is not migrated: neither SETSLOT nor MIGRATE.
	for addr, c := range fl.byAddr {
		for _, call := range c.calls {
			if isClusterSub(call, "SETSLOT") {
				t.Errorf("node %s: SETSLOT should not be called when deleting a REPLICA: %v", addr, call)
			}
			if v0, _ := call[0].(string); strings.EqualFold(v0, "MIGRATE") {
				t.Errorf("node %s: MIGRATE should not be called when deleting a REPLICA: %v", addr, call)
			}
		}
	}
	// FORGET r1id on BOTH remaining masters, NOT on the one being deleted.
	if !hasClusterForget(fl.byAddr[m0Addr], "r1id") {
		t.Error("m0: waiting for CLUSTER FORGET r1id")
	}
	if !hasClusterForget(fl.byAddr[m1Addr], "r1id") {
		t.Error("m1: waiting for CLUSTER FORGET r1id")
	}
	if len(clusterForgetTargets(fl.byAddr[removeAddr])) != 0 {
		t.Error("The node being deleted should not receive FORGET")
	}
	if got := fin.GetOutput().GetFields()["forgotten_on"].GetNumberValue(); got != 2 {
		t.Errorf("forgotten_on=%v, waited 2", got)
	}
	if got := fin.GetOutput().GetFields()["slots_migrated"].GetNumberValue(); got != 0 {
		t.Errorf("slots_migrated=%v, waited 0 (replica)", got)
	}

	assertEventsNoSecret(t, stream)
	for addr, c := range fl.byAddr {
		assertNoClusterSecret(t, addr, c)
	}
}

// --- Apply remove-node: master WITH slots (slot migration + FORGET) ---

func TestApplyClusterRemoveNode_MasterWithSlotsMigrates(t *testing.T) {
	removeAddr := "10.0.0.3:6379" // m2 - removable master with slots 16380-16383 (4 slots)
	m0Addr := "10.0.0.1:6379"
	m1Addr := "10.0.0.2:6379"
	seedAddr := m0Addr
	fl := newFleet(m0Addr, m1Addr, removeAddr)
	table := nodesTableWithSlots(
		masterRowSlots("m0id", "10.0.0.1:6379", 0, 8189),
		masterRowSlots("m1id", "10.0.0.2:6379", 8190, 16379),
		masterRowSlots("m2id", "10.0.0.3:6379", 16380, 16383),
	)
	for _, c := range fl.byAddr {
		c.nodes = table
	}
	// There is one key on slot 16380 at the source -> migration actually sends MIGRATE.
	fl.byAddr[removeAddr].keysInSlot = map[int][]string{16380: {"key-a"}}
	m := fl.module()
	stream := &applyStream{}

	err := m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "node-removed",
		Params: mustStruct(t, map[string]any{
			"password": secretPass,
			"node":     nodeMapParam(removeAddr),
			"seed":     nodeMapParam(seedAddr),
		}),
	}, stream)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil || fin.Failed || !fin.Changed {
		t.Fatalf("expected success changed=true, got %+v", fin)
	}
	// 4 slots (16380..16383) have been moved.
	if got := fin.GetOutput().GetFields()["slots_migrated"].GetNumberValue(); got != 4 {
		t.Errorf("slots_migrated=%v, waited 4", got)
	}

	src := fl.byAddr[removeAddr]

	// Source: MIGRATING per slot + GETKEYSINSLOT + SETSLOT NODE.
	migratingSlots := setslotSlots(src, "MIGRATING")
	if len(migratingSlots) != 4 {
		t.Errorf("source: waited 4 SETSLOT MIGRATING, got %d (%v)", len(migratingSlots), migratingSlots)
	}
	// Round-robin slots between two destination masters (16380->m0, 16381->m1,...).
	assertSetslotImporting(t, fl.byAddr[m0Addr], "m2id") // m0 imports slots from m2
	assertSetslotImporting(t, fl.byAddr[m1Addr], "m2id")

	// On slot 16380 the switch -> MIGRATE was indeed executed (with AUTH ***).
	if !hasMigrate(src) {
		t.Error("source: expected MIGRATE for slot with key")
	}

	// After migration - FORGET m2id on both remaining masters.
	if !hasClusterForget(fl.byAddr[m0Addr], "m2id") {
		t.Error("m0: expected CLUSTER FORGET m2id after migration")
	}
	if !hasClusterForget(fl.byAddr[m1Addr], "m2id") {
		t.Error("m1: expected CLUSTER FORGET m2id after migration")
	}

	// Information security invariant: password is NOT in events/errors (logs/OTel/RunResult). ON THE WIRE
	// it is inevitable in exactly one place - MIGRATE... AUTH <pass> (AUTH to
	// password-protected destination; This is exactly what go-redis itself sends). Everywhere EXCEPT
	// this AUTH argument, there should not be a password.
	assertEventsNoSecret(t, stream)
	assertSecretOnlyInMigrateAuth(t, src)
	for addr, c := range fl.byAddr {
		if addr == removeAddr {
			continue // source MIGRATE - checked separately above
		}
		assertNoClusterSecret(t, addr, c)
	}
}

// --- Apply remove-node: keys with whitespace in the name migrate lossless ---

// TestApplyClusterRemoveNode_WhitespaceKeysLossless fixes the MAJOR defect:
// Redis-key is an arbitrary byte string and can contain a space/\t/\n. Previously
// GETKEYSINSLOT is stringed with a join separated by a space + strings.Fields ->
// the key "user 42" was torn into two tokens -> MIGRATE on non-existent keys -> key
// NOT transferred, but SETSLOT NODE still gave away the slot -> DATA LOSS.
//
// The typed GetKeysInSlot ([]string) stores the entire keys. The test requires:
// each slot key (including whitespace names) ended up in MIGRATE as ONE KEYS-
// argument, and the set of transferred keys is exactly equal to the original one (lossless).
func TestApplyClusterRemoveNode_WhitespaceKeysLossless(t *testing.T) {
	removeAddr := "10.0.0.3:6379" // m2 - removable master with slot 16380
	m0Addr := "10.0.0.1:6379"
	m1Addr := "10.0.0.2:6379"
	seedAddr := m0Addr
	fl := newFleet(m0Addr, m1Addr, removeAddr)
	table := nodesTableWithSlots(
		masterRowSlots("m0id", "10.0.0.1:6379", 0, 8189),
		masterRowSlots("m1id", "10.0.0.2:6379", 8190, 16379),
		masterRowSlots("m2id", "10.0.0.3:6379", 16380, 16380), // exactly one slot
	)
	for _, c := range fl.byAddr {
		c.nodes = table
	}
	// Slot 16380 contains keys With name delimiters: space, tab, translate
	// lines - and a regular key for contrast. Everyone should move as is.
	keys := []string{"user 42", "a\tb", "c\nd", "plain"}
	fl.byAddr[removeAddr].keysInSlot = map[int][]string{16380: append([]string(nil), keys...)}
	m := fl.module()
	stream := &applyStream{}

	err := m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "node-removed",
		Params: mustStruct(t, map[string]any{
			"password": secretPass,
			"node":     nodeMapParam(removeAddr),
			"seed":     nodeMapParam(seedAddr),
		}),
	}, stream)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil || fin.Failed || !fin.Changed {
		t.Fatalf("expected success changed=true, got %+v", fin)
	}

	// The set of keys that actually went into MIGRATE... KEYS... must coincide with
	// original WITHOUT loss and WITHOUT splitting (no "user"/"42" separately).
	got := migrateKeyArgs(t, fl.byAddr[removeAddr])
	if !equalStringSets(got, keys) {
		t.Fatalf("MIGRATE KEYS loss: got %q, expected %q (keys with whitespace are split/lost)", got, keys)
	}
	// Point to point: "user 42" must be ONE argument, not two tokens.
	if !containsString(got, "user 42") {
		t.Errorf(`key "user 42" not passed as one argument MIGRATE: %q`, got)
	}
}

// --- Apply remove-node: slot with >1 non-empty batch (multi-batch cycle) ---

// TestApplyClusterRemoveNode_MultiBatchSlotLossless fixes the DATA-RISK path:
// migrateOneSlot loops GETKEYSINSLOT+MIGRATE UNTIL the slot is empty. Slot with >migrateBatch
// gives several portions of keys -> several iterations of the loop. If the cycle is interrupted
// after the first portion (or the fake source will give everything away at once and hide the bug), the keys
// beyond the first batch WILL BE LOST at SETSLOT NODE. The test keeps on being deleted
// master slot with a queue of 3 portions (the whitespace key is in the SECOND, not the first):
// requires that the union of ALL portions go to MIGRATE, the loop ends, and
// the slot phase order was IMPORTING->MIGRATING->(N MIGRATE)->SETSLOT NODE.
func TestApplyClusterRemoveNode_MultiBatchSlotLossless(t *testing.T) {
	removeAddr := "10.0.0.3:6379" // m2 - removable master with slot 16380
	m0Addr := "10.0.0.1:6379"
	m1Addr := "10.0.0.2:6379"
	seedAddr := m0Addr
	fl := newFleet(m0Addr, m1Addr, removeAddr)
	table := nodesTableWithSlots(
		masterRowSlots("m0id", "10.0.0.1:6379", 0, 8189),
		masterRowSlots("m1id", "10.0.0.2:6379", 8190, 16379),
		masterRowSlots("m2id", "10.0.0.3:6379", 16380, 16380), // exactly one slot
	)
	for _, c := range fl.byAddr {
		c.nodes = table
	}
	// Slot 16380 is emptied in 3 chunks (>1 loop iteration each). whitespace-key
	// "user 42" - in the SECOND portion (not in the first): proves that lossless holds
	// and beyond the first batch. Round-robin: 16380 - first and only slot
	// of the deleted master -> goes to m0 (di=0).
	batch1 := []string{"k1", "k2"}
	batch2 := []string{"user 42", "k3"}
	batch3 := []string{"k4"}
	fl.byAddr[removeAddr].keysInSlotBatches = map[int][][]string{
		16380: {batch1, batch2, batch3},
	}
	m := fl.module()
	stream := &applyStream{}

	err := m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "node-removed",
		Params: mustStruct(t, map[string]any{
			"password": secretPass,
			"node":     nodeMapParam(removeAddr),
			"seed":     nodeMapParam(seedAddr),
		}),
	}, stream)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil || fin.Failed || !fin.Changed {
		t.Fatalf("expected success changed=true, got %+v", fin)
	}
	if got := fin.GetOutput().GetFields()["slots_migrated"].GetNumberValue(); got != 1 {
		t.Errorf("slots_migrated=%v, waited 1", got)
	}

	src := fl.byAddr[removeAddr]
	wantKeys := append(append(append([]string(nil), batch1...), batch2...), batch3...)

	// ALL keys of all portions went to MIGRATE (merger), nothing was lost.
	got := migrateKeyArgs(t, src)
	if !equalStringSets(got, wantKeys) {
		t.Fatalf("multi-batch loss: MIGRATE KEYS=%q, waiting for the merging of portions %q", got, wantKeys)
	}
	if !containsString(got, "user 42") {
		t.Errorf(`whitespace key "user 42" from the 2nd portion did not move/split: %q`, got)
	}
	// Three portions -> exactly three MIGRATE calls (one per non-empty batch).
	if n := countMigrate(src); n != 3 {
		t.Errorf("expected 3 MIGRATE (portions per batch), got %d", n)
	}
	// The loop has ended (not looped): the slot's queue is empty.
	if q := src.keysInSlotBatches[16380]; len(q) != 0 {
		t.Errorf("queue of slot 16380 is not exhausted (the cycle is not enough): there are %d portions left", len(q))
	}
	// Slot 16380 phase order: IMPORTING(target)->MIGRATING(source)->3xMIGRATE->
	// SETSLOT NODE. The source carries MIGRATING, 3 MIGRATE and SETSLOT NODE in that order.
	assertSlotPhaseOrder(t, src, 16380)
	// IMPORTING on target (m0, di=0) points to the source m2id.
	assertSetslotImporting(t, fl.byAddr[m0Addr], "m2id")

	assertEventsNoSecret(t, stream)
	assertSecretOnlyInMigrateAuth(t, src)
}

// migrateKeyArgs collects all the keys passed to MIGRATE... KEYS k1 k2... (all
// after the literal "KEYS"). Each key is a SEPARATE command argument.
func migrateKeyArgs(t *testing.T, c *clusterConn) []string {
	t.Helper()
	var out []string
	for _, call := range c.calls {
		if v0, _ := call[0].(string); !strings.EqualFold(v0, "MIGRATE") {
			continue
		}
		keysAt := -1
		for j, a := range call {
			if s, _ := a.(string); strings.EqualFold(s, "KEYS") {
				keysAt = j
				break
			}
		}
		if keysAt < 0 {
			t.Fatalf("MIGRATE without KEYS section: %v", call)
		}
		for _, a := range call[keysAt+1:] {
			s, _ := a.(string)
			out = append(out, s)
		}
	}
	return out
}

func containsString(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}

func equalStringSets(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	sa := append([]string(nil), a...)
	sb := append([]string(nil), b...)
	sort.Strings(sa)
	sort.Strings(sb)
	for i := range sa {
		if sa[i] != sb[i] {
			return false
		}
	}
	return true
}

// --- Apply remove-node: master WITHOUT slots (just FORGET) ---

func TestApplyClusterRemoveNode_EmptyMasterForgetOnly(t *testing.T) {
	removeAddr := "10.0.0.3:6379"
	m0Addr := "10.0.0.1:6379"
	m1Addr := "10.0.0.2:6379"
	fl := newFleet(m0Addr, m1Addr, removeAddr)
	table := nodesTableWithSlots(
		masterRowSlots("m0id", "10.0.0.1:6379", 0, 8191),
		masterRowSlots("m1id", "10.0.0.2:6379", 8192, 16383),
		masterRowNoSlots("m2id", "10.0.0.3:6379"), // empty master, no slots
	)
	for _, c := range fl.byAddr {
		c.nodes = table
	}
	m := fl.module()
	stream := &applyStream{}

	err := m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "node-removed",
		Params: mustStruct(t, map[string]any{
			"node": nodeMapParam(removeAddr),
			"seed": nodeMapParam(m0Addr),
		}),
	}, stream)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	fin := stream.final()
	if fin == nil || fin.Failed || !fin.Changed {
		t.Fatalf("expected success changed=true, got %+v", fin)
	}
	if got := fin.GetOutput().GetFields()["slots_migrated"].GetNumberValue(); got != 0 {
		t.Errorf("slots_migrated=%v, waited 0 (empty master)", got)
	}
	// No migration for an empty master.
	for addr, c := range fl.byAddr {
		for _, call := range c.calls {
			if isClusterSub(call, "SETSLOT") {
				t.Errorf("node %s: SETSLOT when deleting an EMPTY master should not be called: %v", addr, call)
			}
		}
	}
	if !hasClusterForget(fl.byAddr[m0Addr], "m2id") || !hasClusterForget(fl.byAddr[m1Addr], "m2id") {
		t.Error("expected CLUSTER FORGET m2id on both remaining masters")
	}
}

// --- Apply remove-node: idempotency (no node no longer exists -> no-op) ---

func TestApplyClusterRemoveNode_AbsentNoOp(t *testing.T) {
	removeAddr := "10.0.0.9:6379" // no in topology
	m0Addr := "10.0.0.1:6379"
	m1Addr := "10.0.0.2:6379"
	fl := newFleet(m0Addr, m1Addr)
	table := nodesTableWithSlots(
		masterRowSlots("m0id", "10.0.0.1:6379", 0, 8191),
		masterRowSlots("m1id", "10.0.0.2:6379", 8192, 16383),
	)
	for _, c := range fl.byAddr {
		c.nodes = table
	}
	m := fl.module()
	stream := &applyStream{}

	err := m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "node-removed",
		Params: mustStruct(t, map[string]any{
			"node": nodeMapParam(removeAddr),
			"seed": nodeMapParam(m0Addr),
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
		t.Error("waited changed=false: the node no longer exists (no-op)")
	}
	// No-op: no FORGET, no SETSLOT, no MIGRATE.
	for addr, c := range fl.byAddr {
		for _, call := range c.calls {
			if isClusterSub(call, "FORGET") || isClusterSub(call, "SETSLOT") {
				t.Errorf("node %s: no-op broken, caused by %v", addr, call)
			}
		}
	}
}

// --- Apply remove-node: connection file to seed does not contain password ---

func TestApplyClusterRemoveNode_SeedConnectFailNoLeak(t *testing.T) {
	m := &RedisModule{
		connect: func(_ context.Context, cfg connConfig) (redisConn, error) {
			return nil, errors.New("dial failed for AUTH " + cfg.password)
		},
	}
	stream := &applyStream{}
	_ = m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "node-removed",
		Params: mustStruct(t, map[string]any{
			"password": secretPass,
			"node":     nodeMapParam("10.0.0.3:6379"),
			"seed":     nodeMapParam("10.0.0.1:6379"),
		}),
	}, stream)

	fin := stream.final()
	if fin == nil || !fin.Failed {
		t.Fatalf("waited failed=true, got %+v", fin)
	}
	assertEventsNoSecret(t, stream)
}

// ================================ reshard ====================================

// --- Validate: reshard ---

func TestValidate_ReshardRequiresFromToSlots(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.cluster().Validate(context.Background(), &pluginv1.ValidateRequest{
		State:  "resharded",
		Params: mustStruct(t, map[string]any{}),
	})
	if reply.Ok {
		t.Fatal("expected Ok=false without from/to/slots")
	}
}

func TestValidate_ReshardRejectsSameFromTo(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.cluster().Validate(context.Background(), &pluginv1.ValidateRequest{
		State: "resharded",
		Params: mustStruct(t, map[string]any{
			"from":  nodeMapParam("10.0.0.1:6379"),
			"to":    nodeMapParam("10.0.0.1:6379"),
			"slots": 10,
		}),
	})
	if reply.Ok {
		t.Fatal("waited Ok=false on from == to")
	}
}

// The {addr} and {ip,port} forms of the SAME node must be recognized as a match.
func TestValidate_ReshardRejectsSameFromToMixedForm(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.cluster().Validate(context.Background(), &pluginv1.ValidateRequest{
		State: "resharded",
		Params: mustStruct(t, map[string]any{
			"from":  nodeMapParam("10.0.0.1:6379"),
			"to":    map[string]any{"ip": "10.0.0.1", "port": 6379},
			"slots": 10,
		}),
	})
	if reply.Ok {
		t.Fatal("waited Ok=false on from == to (addr vs ip+port of one node)")
	}
}

func TestValidate_ReshardRejectsZeroSlots(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.cluster().Validate(context.Background(), &pluginv1.ValidateRequest{
		State: "resharded",
		Params: mustStruct(t, map[string]any{
			"from":  nodeMapParam("10.0.0.1:6379"),
			"to":    nodeMapParam("10.0.0.2:6379"),
			"slots": 0,
		}),
	})
	if reply.Ok {
		t.Fatal("waited Ok=false on slots < 1")
	}
}

func TestValidate_ReshardHappy(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.cluster().Validate(context.Background(), &pluginv1.ValidateRequest{
		State: "resharded",
		Params: mustStruct(t, map[string]any{
			"from":  nodeMapParam("10.0.0.1:6379"),
			"to":    nodeMapParam("10.0.0.2:6379"),
			"slots": 100,
		}),
	})
	if !reply.Ok || len(reply.Errors) != 0 {
		t.Fatalf("waited Ok=true, got %+v", reply)
	}
}

// --- Apply reshard: happy-path (transfer N slots from->to, sequence) ---

func TestApplyClusterReshard_HappyPathSequence(t *testing.T) {
	fromAddr := "10.0.0.1:6379" // m0 - source, slots 0..8191
	toAddr := "10.0.0.2:6379"   // m1 - receiver, slots 8192..16383
	fl := newFleet(fromAddr, toAddr)
	table := nodesTableWithSlots(
		masterRowSlots("m0id", "10.0.0.1:6379", 0, 8191),
		masterRowSlots("m1id", "10.0.0.2:6379", 8192, 16383),
	)
	for _, c := range fl.byAddr {
		c.nodes = table
	}
	// The first of the transferred slots (0) contains the key -> MIGRATE is actually called.
	fl.byAddr[fromAddr].keysInSlot = map[int][]string{0: {"key-a"}}
	m := fl.module()
	stream := &applyStream{}

	const moveN = 3
	err := m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "resharded",
		Params: mustStruct(t, map[string]any{
			"password": secretPass,
			"from":     nodeMapParam(fromAddr),
			"to":       nodeMapParam(toAddr),
			"slots":    moveN,
		}),
	}, stream)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil || fin.Failed || !fin.Changed {
		t.Fatalf("expected success changed=true, got %+v", fin)
	}
	if got := fin.GetOutput().GetFields()["slots_moved"].GetNumberValue(); got != moveN {
		t.Errorf("slots_moved=%v, expected %d", got, moveN)
	}

	src := fl.byAddr[fromAddr]
	dst := fl.byAddr[toAddr]

	// The FIRST N source slots are transferred in ascending order: 0, 1, 2.
	wantSlots := []int{0, 1, 2}

	// Goal: SETSLOT <slot> IMPORTING <src-id> for each transferred slot.
	if got := setslotSlots(dst, "IMPORTING"); !equalIntSlices(got, wantSlots) {
		t.Errorf("goal: SETSLOT IMPORTING slots=%v, waiting for %v", got, wantSlots)
	}
	// Source: SETSLOT <slot> MIGRATING <dst-id> per slot.
	if got := setslotSlots(src, "MIGRATING"); !equalIntSlices(got, wantSlots) {
		t.Errorf("source: SETSLOT MIGRATING slots=%v, waiting for %v", got, wantSlots)
	}
	// SETSLOT NODE <dst-id> on BOTH nodes for each slot (fixing the owner).
	if got := setslotSlots(src, "NODE"); !equalIntSlices(got, wantSlots) {
		t.Errorf("source: SETSLOT NODE slots=%v, expected %v", got, wantSlots)
	}
	if got := setslotSlots(dst, "NODE"); !equalIntSlices(got, wantSlots) {
		t.Errorf("target: SETSLOT NODE slots=%v, waiting for %v", got, wantSlots)
	}
	// On slot 0 the switch -> MIGRATE was indeed executed.
	if !hasMigrate(src) {
		t.Error("source: expected MIGRATE for slot with key")
	}
	// IMPORTING/MIGRATING point to the correct node-id (m1id imports from m0id).
	assertSetslotImporting(t, dst, "m0id")

	// IB: password only in MIGRATE AUTH (inevitably on the wire), nowhere else.
	assertEventsNoSecret(t, stream)
	assertSecretOnlyInMigrateAuth(t, src)
	assertNoClusterSecret(t, toAddr, dst)
}

// --- Apply reshard: lossless keys with whitespace in the name ---

func TestApplyClusterReshard_WhitespaceKeysLossless(t *testing.T) {
	fromAddr := "10.0.0.1:6379"
	toAddr := "10.0.0.2:6379"
	fl := newFleet(fromAddr, toAddr)
	table := nodesTableWithSlots(
		masterRowSlots("m0id", "10.0.0.1:6379", 0, 0), // exactly one slot at the source
		masterRowSlots("m1id", "10.0.0.2:6379", 1, 16383),
	)
	for _, c := range fl.byAddr {
		c.nodes = table
	}
	// Slot 0 contains keys Delimited by: space, tab, newline + regular.
	keys := []string{"user 42", "a\tb", "c\nd", "plain"}
	fl.byAddr[fromAddr].keysInSlot = map[int][]string{0: append([]string(nil), keys...)}
	m := fl.module()
	stream := &applyStream{}

	err := m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "resharded",
		Params: mustStruct(t, map[string]any{
			"password": secretPass,
			"from":     nodeMapParam(fromAddr),
			"to":       nodeMapParam(toAddr),
			"slots":    1,
		}),
	}, stream)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil || fin.Failed || !fin.Changed {
		t.Fatalf("expected success changed=true, got %+v", fin)
	}

	// All keys (including whitespace names) went to MIGRATE... KEYS... as SEPARATE
	// arguments, the set is the same as the original one (typed GetKeysInSlot).
	got := migrateKeyArgs(t, fl.byAddr[fromAddr])
	if !equalStringSets(got, keys) {
		t.Fatalf("MIGRATE KEYS loss: got %q, expected %q (whitespace keys split/lost)", got, keys)
	}
	if !containsString(got, "user 42") {
		t.Errorf(`key "user 42" not passed as one argument MIGRATE: %q`, got)
	}
}

// --- Apply reshard: slot with >1 non-empty batch (multi-batch cycle) ---

// TestApplyClusterReshard_MultiBatchSlotLossless - multi-batch reshard mirror
// guard: the only portable slot (0) gives out keys in three portions, loop
// migrateOneSlot must empty the slot completely (3 MIGRATE) and commit
// owner only after. whitespace key - in the 3rd (last) portion.
func TestApplyClusterReshard_MultiBatchSlotLossless(t *testing.T) {
	fromAddr := "10.0.0.1:6379"
	toAddr := "10.0.0.2:6379"
	fl := newFleet(fromAddr, toAddr)
	table := nodesTableWithSlots(
		masterRowSlots("m0id", "10.0.0.1:6379", 0, 0), // exactly one slot at the source
		masterRowSlots("m1id", "10.0.0.2:6379", 1, 16383),
	)
	for _, c := range fl.byAddr {
		c.nodes = table
	}
	batch1 := []string{"k1", "k2"}
	batch2 := []string{"k3"}
	batch3 := []string{"c\nd", "user 42"} // whitespace keys in the last portion
	fl.byAddr[fromAddr].keysInSlotBatches = map[int][][]string{
		0: {batch1, batch2, batch3},
	}
	m := fl.module()
	stream := &applyStream{}

	err := m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "resharded",
		Params: mustStruct(t, map[string]any{
			"password": secretPass,
			"from":     nodeMapParam(fromAddr),
			"to":       nodeMapParam(toAddr),
			"slots":    1,
		}),
	}, stream)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil || fin.Failed || !fin.Changed {
		t.Fatalf("expected success changed=true, got %+v", fin)
	}

	src := fl.byAddr[fromAddr]
	dst := fl.byAddr[toAddr]
	wantKeys := append(append(append([]string(nil), batch1...), batch2...), batch3...)

	got := migrateKeyArgs(t, src)
	if !equalStringSets(got, wantKeys) {
		t.Fatalf("multi-batch loss: MIGRATE KEYS=%q, waiting for the merging of portions %q", got, wantKeys)
	}
	if !containsString(got, "user 42") {
		t.Errorf(`whitespace key "user 42" from the 3rd portion did not move/split: %q`, got)
	}
	if n := countMigrate(src); n != 3 {
		t.Errorf("expected 3 MIGRATE (portions per batch), got %d", n)
	}
	if q := src.keysInSlotBatches[0]; len(q) != 0 {
		t.Errorf("queue of slot 0 is not exhausted (the cycle is not enough): %d portions left", len(q))
	}
	// The phase order of slot 0 is: IMPORTING(target)->MIGRATING(source)->3xMIGRATE->SETSLOT NODE.
	assertSlotPhaseOrder(t, src, 0)
	assertSetslotImporting(t, dst, "m0id")

	assertEventsNoSecret(t, stream)
	assertSecretOnlyInMigrateAuth(t, src)
}

// --- Apply reshard: from not master in the cluster -> failed, password does not leak ---

func TestApplyClusterReshard_UnknownFromMaster(t *testing.T) {
	fromAddr := "10.0.0.9:6379" // no in topology
	toAddr := "10.0.0.2:6379"
	fl := newFleet(fromAddr, toAddr)
	table := nodesTableWithSlots(
		masterRowSlots("m0id", "10.0.0.1:6379", 0, 8191),
		masterRowSlots("m1id", "10.0.0.2:6379", 8192, 16383),
	)
	for _, c := range fl.byAddr {
		c.nodes = table
	}
	m := fl.module()
	stream := &applyStream{}

	_ = m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "resharded",
		Params: mustStruct(t, map[string]any{
			"password": secretPass,
			"from":     nodeMapParam(fromAddr),
			"to":       nodeMapParam(toAddr),
			"slots":    5,
		}),
	}, stream)

	fin := stream.final()
	if fin == nil || !fin.Failed {
		t.Fatalf("expected failed=true on from outside the cluster, got %+v", fin)
	}
	// No SETSLOT/MIGRATE until the topology is resolved.
	for _, c := range fl.byAddr {
		for _, call := range c.calls {
			if isClusterSub(call, "SETSLOT") {
				t.Error("SETSLOT should not be called when from is unresolvable")
			}
		}
	}
	assertEventsNoSecret(t, stream)
}

// --- Apply reshard: slots > available at the source -> failed ---

func TestApplyClusterReshard_SlotsExceedOwned(t *testing.T) {
	fromAddr := "10.0.0.1:6379" // m0 owns exactly 4 slots 0..3
	toAddr := "10.0.0.2:6379"
	fl := newFleet(fromAddr, toAddr)
	table := nodesTableWithSlots(
		masterRowSlots("m0id", "10.0.0.1:6379", 0, 3),
		masterRowSlots("m1id", "10.0.0.2:6379", 4, 16383),
	)
	for _, c := range fl.byAddr {
		c.nodes = table
	}
	m := fl.module()
	stream := &applyStream{}

	_ = m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "resharded",
		Params: mustStruct(t, map[string]any{
			"from":  nodeMapParam(fromAddr),
			"to":    nodeMapParam(toAddr),
			"slots": 5, // more than 4 available
		}),
	}, stream)

	fin := stream.final()
	if fin == nil || !fin.Failed {
		t.Fatalf("expected failed=true on slots > available, got %+v", fin)
	}
	// Transfer not started: no SETSLOT.
	for _, c := range fl.byAddr {
		for _, call := range c.calls {
			if isClusterSub(call, "SETSLOT") {
				t.Error("SETSLOT should not be called when slots > available")
			}
		}
	}
}

// --- Apply reshard: connection file to from does not leak password ---

func TestApplyClusterReshard_FromConnectFailNoLeak(t *testing.T) {
	m := &RedisModule{
		connect: func(_ context.Context, cfg connConfig) (redisConn, error) {
			return nil, errors.New("dial failed for AUTH " + cfg.password)
		},
	}
	stream := &applyStream{}
	_ = m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "resharded",
		Params: mustStruct(t, map[string]any{
			"password": secretPass,
			"from":     nodeMapParam("10.0.0.1:6379"),
			"to":       nodeMapParam("10.0.0.2:6379"),
			"slots":    5,
		}),
	}, stream)

	fin := stream.final()
	if fin == nil || !fin.Failed {
		t.Fatalf("waited failed=true, got %+v", fin)
	}
	assertEventsNoSecret(t, stream)
}

// equalIntSlices - exact element-wise comparison (order is important: slots are in order
// transfer). setslotSlots already sorts, so the comparison is deterministic.
func equalIntSlices(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// --- assert helpers (remove-node) ---

// setslotSlots - slots to which the node sent CLUSTER SETSLOT <slot> <sub>.
func setslotSlots(c *clusterConn, sub string) []int {
	var got []int
	for _, call := range c.calls {
		if !isClusterSub(call, "SETSLOT") || len(call) < 4 {
			continue
		}
		s, _ := call[3].(string)
		if !strings.EqualFold(s, sub) {
			continue
		}
		slot, _ := strconv.Atoi(call[2].(string))
		got = append(got, slot)
	}
	sort.Ints(got)
	return got
}

// assertSetslotImporting - the node has received at least one SETSLOT... IMPORTING <srcID>.
func assertSetslotImporting(t *testing.T, c *clusterConn, srcID string) {
	t.Helper()
	for _, call := range c.calls {
		if !isClusterSub(call, "SETSLOT") || len(call) < 5 {
			continue
		}
		if s, _ := call[3].(string); strings.EqualFold(s, "IMPORTING") {
			if got, _ := call[4].(string); got == srcID {
				return
			}
		}
	}
	t.Errorf("expected SETSLOT IMPORTING %s, not found", srcID)
}

// hasMigrate - the node has executed at least one MIGRATE.
func hasMigrate(c *clusterConn) bool {
	for _, call := range c.calls {
		if v0, _ := call[0].(string); strings.EqualFold(v0, "MIGRATE") {
			return true
		}
	}
	return false
}

// countMigrate - how many MIGRATE calls the node executed (in a multi-batch cycle -
// one per non-empty portion of slot keys).
func countMigrate(c *clusterConn) int {
	n := 0
	for _, call := range c.calls {
		if v0, _ := call[0].(string); strings.EqualFold(v0, "MIGRATE") {
			n++
		}
	}
	return n
}

// assertSlotPhaseOrder checks the correct order of migration phases on the SOURCE
// specific slot: SETSLOT <slot> MIGRATING (exactly once) -> one or more
// MIGRATE -> SETSLOT <slot> NODE (fix owner AFTER all MIGRATEs). This
// ensures that the slot owner is not fixed before all have been transferred
// portions of keys (otherwise, data loss in multi-batch). MIGRATE on source
// refer to the current slot (one slot is migrated at a time).
func assertSlotPhaseOrder(t *testing.T, c *clusterConn, slot int) {
	t.Helper()
	slotArg := strconv.Itoa(slot)
	migratingAt, nodeAt := -1, -1
	var migrateIdx []int
	for i, call := range c.calls {
		if v0, _ := call[0].(string); strings.EqualFold(v0, "MIGRATE") {
			migrateIdx = append(migrateIdx, i)
			continue
		}
		if !isClusterSub(call, "SETSLOT") || len(call) < 4 {
			continue
		}
		if s, _ := call[2].(string); s != slotArg {
			continue
		}
		switch sub, _ := call[3].(string); {
		case strings.EqualFold(sub, "MIGRATING"):
			if migratingAt != -1 {
				t.Errorf("slot %d: more than one SETSLOT MIGRATING", slot)
			}
			migratingAt = i
		case strings.EqualFold(sub, "NODE"):
			nodeAt = i
		}
	}
	if migratingAt < 0 {
		t.Fatalf("slot %d: no SETSLOT MIGRATING on source", slot)
	}
	if nodeAt < 0 {
		t.Fatalf("slot %d: no SETSLOT NODE on source", slot)
	}
	if len(migrateIdx) == 0 {
		t.Fatalf("slot %d: there is no MIGRATE between MIGRATING and NODE", slot)
	}
	if nodeAt < migratingAt {
		t.Errorf("slot %d: SETSLOT NODE (idx %d) before MIGRATING (idx %d)", slot, nodeAt, migratingAt)
	}
	for _, mi := range migrateIdx {
		if mi < migratingAt {
			t.Errorf("slot %d: MIGRATE (idx %d) before SETSLOT MIGRATING (idx %d)", slot, mi, migratingAt)
		}
		if mi > nodeAt {
			t.Errorf("slot %d: MIGRATE (idx %d) AFTER SETSLOT NODE (idx %d) - owner fixed before transfer", slot, mi, nodeAt)
		}
	}
}

// assertSecretOnlyInMigrateAuth allows password ONLY as an AUTH argument to MIGRATE
// (inevitably on the wire for a password-protected destination); any other
// the appearance of a password in commands is a leak.
func assertSecretOnlyInMigrateAuth(t *testing.T, c *clusterConn) {
	t.Helper()
	for i, call := range c.calls {
		isMigrate := false
		if v0, _ := call[0].(string); strings.EqualFold(v0, "MIGRATE") {
			isMigrate = true
		}
		for j, a := range call {
			s, ok := a.(string)
			if !ok || !strings.Contains(s, secretPass) {
				continue
			}
			// MIGRATE host port "" db timeout AUTH <pass> KEYS... -> password on position
			// right after "AUTH".
			if isMigrate && j > 0 {
				if prev, _ := call[j-1].(string); strings.EqualFold(prev, "AUTH") {
					continue
				}
			}
			t.Errorf("command[%d] carries the password outside MIGRATE AUTH: %v", i, call)
		}
	}
}

// ============================= explicit topology =============================
//
// The optional params.topology specifies an EXPLICIT layout of shards (the operator himself stuffed
// VM by zone / coded anti-affinity in the list). buildClusterPlanExplicit -
// mirror buildClusterPlan: masters[i]=nodes[topology[i][0]], replicas from tails
// (replicaOf=i), slots - the same allocateSlots(len(topology)). Assembly (MEET/ADDSLOTS/
// REPLICATE), idempotency and no-secret-leak are reused (general clusterPlan).

// topologyParam builds params.topology - a list of shards from SID lists.
func topologyParam(shards ...[]string) []any {
	out := make([]any, 0, len(shards))
	for _, shard := range shards {
		inner := make([]any, 0, len(shard))
		for _, sid := range shard {
			inner = append(inner, sid)
		}
		out = append(out, inner)
	}
	return out
}

// namedNodesParam builds params.nodes-map with EXPLICIT keys (SID) - topology
// refers to nodes by key, not by index (unlike clusterNodesParam).
func namedNodesParam(byKey map[string]string) map[string]any {
	nodes := map[string]any{}
	for key, addr := range byKey {
		nodes[key] = map[string]any{"addr": addr}
	}
	return nodes
}

// --- Validate: topology ---

func TestValidate_ClusterTopology(t *testing.T) {
	nodes := map[string]any{
		"node-a": map[string]any{"addr": "10.0.0.1:6379"},
		"node-b": map[string]any{"addr": "10.0.0.2:6379"},
		"node-c": map[string]any{"addr": "10.0.0.3:6379"},
		"node-d": map[string]any{"addr": "10.0.0.4:6379"},
	}
	cases := []struct {
		name     string
		params   map[string]any
		wantOk   bool
		errMatch string // substring in one of the errors (if wantOk=false)
	}{
		{
			name: "happy: 2 shards x 1 replica covers all nodes",
			params: map[string]any{
				"nodes":    nodes,
				"topology": topologyParam([]string{"node-a", "node-c"}, []string{"node-b", "node-d"}),
			},
			wantOk: true,
		},
		{
			name: "happy: master-only shard (no replicas) is allowed without replicas_per_shard",
			params: map[string]any{
				"nodes": map[string]any{
					"node-a": map[string]any{"addr": "10.0.0.1:6379"},
					"node-b": map[string]any{"addr": "10.0.0.2:6379"},
				},
				"topology": topologyParam([]string{"node-a"}, []string{"node-b"}),
			},
			wantOk: true,
		},
		{
			name: "duplicate SID across shards rejected",
			params: map[string]any{
				"nodes":    nodes,
				"topology": topologyParam([]string{"node-a", "node-c"}, []string{"node-a", "node-d"}),
			},
			wantOk:   false,
			errMatch: "appears 2 times",
		},
		{
			name: "non-existent SID rejected",
			params: map[string]any{
				"nodes": map[string]any{
					"node-a": map[string]any{"addr": "10.0.0.1:6379"},
					"node-b": map[string]any{"addr": "10.0.0.2:6379"},
				},
				"topology": topologyParam([]string{"node-a", "node-ZZZ"}, []string{"node-b"}),
			},
			wantOk:   false,
			errMatch: "not found in nodes",
		},
		{
			name: "empty shard rejected",
			params: map[string]any{
				"nodes": map[string]any{
					"node-a": map[string]any{"addr": "10.0.0.1:6379"},
				},
				"topology": topologyParam([]string{"node-a"}, []string{}),
			},
			wantOk:   false,
			errMatch: "at least a master SID",
		},
		{
			name: "unused node (not covered by topology) rejected",
			params: map[string]any{
				"nodes":    nodes, // 4 nodes, but topology only covers 3
				"topology": topologyParam([]string{"node-a", "node-c"}, []string{"node-b"}),
			},
			wantOk:   false,
			errMatch: "not assigned to any shard",
		},
		{
			name: "replicas_per_shard conflicting with shard size rejected (fail-fast)",
			params: map[string]any{
				"nodes":              nodes,
				"replicas_per_shard": 2, // waiting for shards with 3 nodes, but they have 2
				"topology":           topologyParam([]string{"node-a", "node-c"}, []string{"node-b", "node-d"}),
			},
			wantOk:   false,
			errMatch: "replicas_per_shard=2 requires 3",
		},
		{
			name: "replicas_per_shard matching shard size accepted",
			params: map[string]any{
				"nodes":              nodes,
				"replicas_per_shard": 1, // 2 shards = 1 master + 1 replica -> ok
				"topology":           topologyParam([]string{"node-a", "node-c"}, []string{"node-b", "node-d"}),
			},
			wantOk: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := &RedisModule{}
			reply, _ := m.cluster().Validate(context.Background(), &pluginv1.ValidateRequest{
				State:  "created",
				Params: mustStruct(t, tc.params),
			})
			if reply.Ok != tc.wantOk {
				t.Fatalf("Ok=%v, expected %v (errors=%v)", reply.Ok, tc.wantOk, reply.Errors)
			}
			if tc.wantOk {
				return
			}
			joined := strings.Join(reply.Errors, " | ")
			if !strings.Contains(joined, tc.errMatch) {
				t.Fatalf("error does not contain %q: %v", tc.errMatch, reply.Errors)
			}
		})
	}
}

// --- buildClusterPlanExplicit: layout from explicit topology ---

func TestBuildClusterPlanExplicit_RolesAndSlots(t *testing.T) {
	// The keys are arranged in such a way that the AUTO layout (sort) would give node-a/node-b masters;
	// topology makes node-b/node-d masters - it proves that topology covers
	// sort rather than randomly matching it.
	nodes := []clusterNode{
		{key: "node-a", addr: "10.0.0.1:6379", ip: "10.0.0.1", port: 6379},
		{key: "node-b", addr: "10.0.0.2:6379", ip: "10.0.0.2", port: 6379},
		{key: "node-c", addr: "10.0.0.3:6379", ip: "10.0.0.3", port: 6379},
		{key: "node-d", addr: "10.0.0.4:6379", ip: "10.0.0.4", port: 6379},
	}
	topology := [][]string{
		{"node-b", "node-a"}, // shard 0: master node-b, replica node-a
		{"node-d", "node-c"}, // shard 1: master node-d, replica node-c
	}

	plan, err := buildClusterPlanExplicit(nodes, topology)
	if err != nil {
		t.Fatalf("buildClusterPlanExplicit: %v", err)
	}

	// Masters are the FIRST SIDs of shards in topology order (not sort).
	if len(plan.masters) != 2 || plan.masters[0].key != "node-b" || plan.masters[1].key != "node-d" {
		t.Fatalf("masters=%v, waited [node-b node-d]", masterKeys(plan))
	}
	// Replicas are the tails of shards, replicaOf points to their shard.
	if len(plan.replicas) != 2 || len(plan.replicaOf) != 2 {
		t.Fatalf("replicas=%v replicaOf=%v, expected 2", replicaKeys(plan), plan.replicaOf)
	}
	if plan.replicas[0].key != "node-a" || plan.replicaOf[0] != 0 {
		t.Errorf("replica[0]=%q->shard%d, expected node-a->shard0", plan.replicas[0].key, plan.replicaOf[0])
	}
	if plan.replicas[1].key != "node-c" || plan.replicaOf[1] != 1 {
		t.Errorf("replica[1]=%q->shard%d, expected node-c->shard1", plan.replicas[1].key, plan.replicaOf[1])
	}
	// Slots - the same uniform allocateSlots(2): full coverage of 16384 without holes.
	if len(plan.slots) != 2 {
		t.Fatalf("slots=%v, waiting for 2 ranges", plan.slots)
	}
	assertFullSlotCoverage(t, plan.slots...)
}

func TestBuildClusterPlanExplicit_MasterOnlyShards(t *testing.T) {
	// Shards without replicas (master-only) - there are no replicas, replicaOf is empty, slots are full.
	nodes := []clusterNode{
		{key: "node-a", addr: "10.0.0.1:6379", ip: "10.0.0.1", port: 6379},
		{key: "node-b", addr: "10.0.0.2:6379", ip: "10.0.0.2", port: 6379},
		{key: "node-c", addr: "10.0.0.3:6379", ip: "10.0.0.3", port: 6379},
	}
	plan, err := buildClusterPlanExplicit(nodes, [][]string{{"node-a"}, {"node-b"}, {"node-c"}})
	if err != nil {
		t.Fatalf("buildClusterPlanExplicit: %v", err)
	}
	if len(plan.masters) != 3 {
		t.Fatalf("masters=%v, waited 3", masterKeys(plan))
	}
	if len(plan.replicas) != 0 || len(plan.replicaOf) != 0 {
		t.Fatalf("expected 0 replicas, got replicas=%v replicaOf=%v", replicaKeys(plan), plan.replicaOf)
	}
	assertFullSlotCoverage(t, plan.slots...)
}

func masterKeys(p clusterPlan) []string {
	out := make([]string, 0, len(p.masters))
	for _, m := range p.masters {
		out = append(out, m.key)
	}
	return out
}

func replicaKeys(p clusterPlan) []string {
	out := make([]string, 0, len(p.replicas))
	for _, r := range p.replicas {
		out = append(out, r.key)
	}
	return out
}

// --- Apply create with explicit topology: MEET/ADDSLOTS/REPLICATE by operator list ---

func TestApplyClusterCreate_ExplicitTopology(t *testing.T) {
	addrByKey := map[string]string{
		"node-a": "10.0.0.1:6379",
		"node-b": "10.0.0.2:6379",
		"node-c": "10.0.0.3:6379",
		"node-d": "10.0.0.4:6379",
	}
	addrs := []string{addrByKey["node-a"], addrByKey["node-b"], addrByKey["node-c"], addrByKey["node-d"]}
	fl := newFleet(addrs...)
	fl.setConvergedNodesView()
	m := fl.module()
	stream := &applyStream{}

	// topology: node-b/node-d masters (NOT first in sort), node-a/node-c replicas.
	err := m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "created",
		Params: mustStruct(t, map[string]any{
			"password": secretPass,
			"nodes":    namedNodesParam(addrByKey),
			"topology": topologyParam([]string{"node-b", "node-a"}, []string{"node-d", "node-c"}),
		}),
	}, stream)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	fin := stream.final()
	if fin == nil || fin.Failed || !fin.Changed {
		t.Fatalf("expected success changed=true, got %+v", fin)
	}

	masterB := fl.byAddr[addrByKey["node-b"]]
	masterD := fl.byAddr[addrByKey["node-d"]]
	replicaA := fl.byAddr[addrByKey["node-a"]]
	replicaC := fl.byAddr[addrByKey["node-c"]]

	// ADDSLOTS - only to masters from topology (node-b/node-d), full coverage 16384.
	rB := assertAddSlots(t, masterB)
	rD := assertAddSlots(t, masterD)
	assertNoAddSlots(t, replicaA)
	assertNoAddSlots(t, replicaC)
	assertFullSlotCoverage(t, rB, rD)

	// REPLICATE - replicas are tied to the master of YOUR shard (and not round-robin by sort).
	assertReplicateTo(t, replicaA, masterB.id)
	assertReplicateTo(t, replicaC, masterD.id)

	assertEventsNoSecret(t, stream)
	for addr, c := range fl.byAddr {
		assertNoClusterSecret(t, addr, c)
	}
}
