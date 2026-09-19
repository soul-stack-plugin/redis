// The `cluster` object - assembly of a hash-slot Redis cluster
// (16384 slots) ENTIRELY via go-redis: CLUSTER MEET / ADDSLOTS / REPLICATE +
// CLUSTER INFO / CLUSTER NODES (diagnostics and idempotency). NO
// shell/redis-cli/exec - the plugin's capability remains only network_outbound.
//
// `created` builds a cluster from scratch; `node-added` attaches ONE new
// node to an ALREADY formed cluster (day-2); `node-removed` evicts ONE
// node from the cluster (day-2, with migration of its slots to the remaining masters, if it is a
// master with slots). `resharded` transfers N slots from one master to
// another (day-2, mirror of redis-cli `--cluster reshard`). Migration "old cluster
// -> new" (see migrate.go) - three steps: `external-joined` merges NEW nodes into
// the ALIEN (old) cluster with replicas of its masters 1:1; `failed-over`
// promotes these replicas to masters via graceful CLUSTER FAILOVER (after a sync-gate,
// fail-closed without FORCE/TAKEOVER); `external-forgotten` throws out the old nodes
// (CLUSTER FORGET, without migration of slots - the slots are already with the new masters).
//
// These seven were ONE state `cluster` carrying `params.action` until NIM-766;
// they are seven actions at address level 3 now, and each declares only the params
// it reads (object.go). The operations themselves did not change.
//
// resharded is IMPERATIVE and NOT idempotent (consciously, like the old
// redis-cluster-live without unless): applying again will shift N more slots. This
// exec-style day-2 operation - the operator calls it explicitly, it is NOT part of converge.
// created/node-added/node-removed/external-joined/failed-over/external-forgotten,
// on the contrary, are idempotent (no-op on converged input: the node is already replica/master/forgotten).
//
// The layout of roles and slots is STRICTLY deterministic (sorting nodes keys):
// same input -> same master/replica topology and same
// slot ranges. This is critical for reproducibility and for idempotent form
// (re-create on the formed cluster -> changed=false, no-op).
package main

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	pluginv1 "github.com/souls-guild/soul-stack/proto/plugin/gen/go/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

// totalSlots - fixed number of hash slots Redis Cluster (0..16383).
const totalSlots = 16384

// gossip convergence: limited retry (NOT infinite loop) - wait for everything
// the nodes will see each other in CLUSTER NODES after MEET.
const (
	gossipPollAttempts = 30
	gossipPollInterval = 200 * time.Millisecond
)

// clusterNode - one cluster node after resolution from nodes-map.
//
//	key - stable key from nodes-map (SID/name); specifies a deterministic order.
//	addr - host:port for go-redis connection.
//	ip - IP for CLUSTER MEET (gossip operates on ip:port, not DNS name).
//	port - client port for CLUSTER MEET.
type clusterNode struct {
	key  string
	addr string
	ip   string
	port int
}

// clusterPlan - deterministic layout: which nodes are masters, which are replicas,
// what slot ranges each master has, which master the replica is attached to.
type clusterPlan struct {
	masters  []clusterNode
	replicas []clusterNode
	// slots[i] is a contiguous range of master slots masters[i].
	slots []slotRange
	// replicaOf[j] - master index in masters for replica replicas[j].
	replicaOf []int
}

// slotRange - continuous half-interval [from, to] slots (both ends inclusive).
type slotRange struct {
	from int
	to   int
}

// validateNodeSpec reports what [resolveNodeEndpoint] would refuse about ONE node
// spec, addressed exactly as [resolveSingleNode] addresses it in Apply. Validate
// exists to refuse before anything happens, so an endpoint it cannot resolve is an
// error here and not a surprise mid-run (NIM-786).
//
// A spec that is absent entirely is left to the caller: its message names what the
// parameter is for, which this one cannot.
func validateNodeSpec(addr string, spec map[string]*structpb.Value) []string {
	if len(spec) == 0 {
		return nil
	}
	if _, _, _, err := resolveNodeEndpoint(spec); err != nil {
		return []string{fmt.Sprintf("%s: %v", addr, err)}
	}
	return nil
}

// validateNodeSpecs is the same over a nodes-map, addressed as [parseClusterNodes]
// addresses it. Unlike the single-spec form it does NOT skip an empty entry: a key
// whose value is not a map reaches Apply as a node carrying no endpoint, and Apply
// refuses it, so skipping it here would re-open the gap this closes. Keys are
// sorted for a stable report.
func validateNodeSpecs(nodes map[string]map[string]*structpb.Value) []string {
	var errs []string
	for _, key := range sortedNodeKeys(nodes) {
		if _, _, _, err := resolveNodeEndpoint(nodes[key]); err != nil {
			errs = append(errs, fmt.Sprintf("params.nodes[%s]: %v", key, err))
		}
	}
	return errs
}

// validateClusterCreate - create checks: non-empty nodes-map, resolvable endpoint
// on each node, correct replicas_per_shard and divisibility of the composition by
// the size of the shard.
func validateClusterCreate(f map[string]*structpb.Value) []string {
	var errs []string

	nodes := nodeSpecs(f["nodes"])
	if len(nodes) == 0 {
		errs = append(errs, "params.nodes: must be a non-empty map (key -> {addr|ip+port})")
	}
	errs = append(errs, validateNodeSpecs(nodes)...)

	replicas := intOrDefault(f["replicas_per_shard"], 0)
	if replicas < 0 {
		errs = append(errs, "params.replicas_per_shard: must be >= 0")
	}

	// Explicit topology (params.topology) REPLACES auto-layout: with it the size
	// shards are specified by the operator, and the divisibility of nodes by 1+replicas is NOT checked
	// (replicas_per_shard is ignored - see validateTopology about conflict gate).
	if hasTopology(f["topology"]) {
		return append(errs, validateTopology(f["topology"], nodes, replicas)...)
	}

	// The composition of nodes must be evenly divided by the size of the shard (1 master + N replicas).
	if len(nodes) > 0 && replicas >= 0 {
		shardSize := 1 + replicas
		if len(nodes)%shardSize != 0 {
			errs = append(errs, fmt.Sprintf(
				"params.nodes: %d nodes not divisible by shard size %d (1 master + %d replicas)",
				len(nodes), shardSize, replicas))
		}
	}

	return errs
}

// hasTopology - whether a non-empty params.topology (list with at least one element) is specified.
// Empty list/no/non-list -> false (auto-layout, old behavior).
func hasTopology(v *structpb.Value) bool {
	if v == nil {
		return false
	}
	lv, ok := v.GetKind().(*structpb.Value_ListValue)
	return ok && len(lv.ListValue.GetValues()) > 0
}

// validateTopology checks the explicit topology against nodes-map: each shard is non-empty,
// each SID exists in nodes and occurs EXACTLY once (there are no duplicates and nodes in
// two shards), all nodes are covered (unused -> error). Additionally: if specified and
// topology, AND replicas_per_shard (>0) - the size of EACH shard must match
// 1+replicas (aka fail-fast: inconsistent input). Without replicas_per_shard (0)
// shard sizes are free - topology is the only source of layout.
func validateTopology(v *structpb.Value, nodes map[string]map[string]*structpb.Value, replicas int) []string {
	var errs []string

	lv, ok := v.GetKind().(*structpb.Value_ListValue)
	if !ok {
		return []string{"params.topology: must be a list of shards (each a list of SID strings)"}
	}

	seen := make(map[string]int, len(nodes)) // SID -> number of times met
	for i, shardVal := range lv.ListValue.GetValues() {
		shard, ok := shardVal.GetKind().(*structpb.Value_ListValue)
		if !ok {
			errs = append(errs, fmt.Sprintf("params.topology[%d]: must be a list of SID strings", i))
			continue
		}
		members := shard.ListValue.GetValues()
		if len(members) == 0 {
			errs = append(errs, fmt.Sprintf("params.topology[%d]: shard must have at least a master SID", i))
			continue
		}
		// replicas_per_shard conflict gate: shard size must be 1+replicas.
		if replicas > 0 && len(members) != 1+replicas {
			errs = append(errs, fmt.Sprintf(
				"params.topology[%d]: shard has %d nodes but replicas_per_shard=%d requires %d (1 master + %d replicas) - drop replicas_per_shard or fix the shard",
				i, len(members), replicas, 1+replicas, replicas))
		}
		for _, m := range members {
			sid, isStr := stringValue(m)
			if !isStr {
				errs = append(errs, fmt.Sprintf("params.topology[%d]: SID entries must be strings", i))
				continue
			}
			if _, exists := nodes[sid]; !exists {
				errs = append(errs, fmt.Sprintf("params.topology[%d]: SID %q not found in nodes", i, sid))
			}
			seen[sid]++
		}
	}

	// Doubles: SID in two shards / twice in one.
	for _, sid := range sortedSeenKeys(seen) {
		if seen[sid] > 1 {
			errs = append(errs, fmt.Sprintf("params.topology: SID %q appears %d times (must be exactly once)", sid, seen[sid]))
		}
	}

	// Coverage: each node must be included in the topology (unused -> error).
	for _, key := range sortedNodeKeys(nodes) {
		if seen[key] == 0 {
			errs = append(errs, fmt.Sprintf("params.topology: node %q is not assigned to any shard (all nodes must be covered)", key))
		}
	}

	return errs
}

// sortedSeenKeys / sortedNodeKeys - deterministic order of error messages
// (stable output for tests and operator reports).
func sortedSeenKeys(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func sortedNodeKeys(m map[string]map[string]*structpb.Value) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// validateClusterAddNode - checks add-node: non-empty new_node and seed with
// resolvable endpoints, valid role and (for replica) correct connection with master.
func validateClusterAddNode(f map[string]*structpb.Value) []string {
	var errs []string

	newNode := nodeSpec(f["new_node"])
	if len(newNode) == 0 {
		errs = append(errs, "params.new_node: must be a map {addr|ip+port} of the node to add")
	}
	errs = append(errs, validateNodeSpec("params.new_node", newNode)...)

	seed := nodeSpec(f["seed"])
	if len(seed) == 0 {
		errs = append(errs, "params.seed: must be a map {addr|ip+port} of an existing cluster node")
	}
	errs = append(errs, validateNodeSpec("params.seed", seed)...)

	role := roleOrDefault(f["role"])
	switch role {
	case "replica", "master":
	default:
		errs = append(errs, fmt.Sprintf("params.role: %q not supported (only \"replica\", \"master\")", role))
	}

	// Apply reads params.master only for a replica, and only when the spec carries
	// an endpoint at all - an empty one means auto-selection ([pickReplicationMaster]).
	// Both conditions are repeated here so Validate refuses where Apply refuses and
	// nowhere else: outside them the value is never read.
	if master := nodeSpec(f["master"]); role == "replica" && masterSpecGiven(master) {
		errs = append(errs, validateNodeSpec("params.master", master)...)
	}

	return errs
}

// validateClusterRemoveNode - remove-node checks: non-empty node (removable) and
// seed (pin for CLUSTER NODES + topology source for FORGET/slot migration), each
// with a resolvable endpoint.
func validateClusterRemoveNode(f map[string]*structpb.Value) []string {
	var errs []string

	node := nodeSpec(f["node"])
	if len(node) == 0 {
		errs = append(errs, "params.node: must be a map {addr|ip+port} of the node to remove")
	}
	errs = append(errs, validateNodeSpec("params.node", node)...)

	seed := nodeSpec(f["seed"])
	if len(seed) == 0 {
		errs = append(errs, "params.seed: must be a map {addr|ip+port} of an existing cluster node")
	}
	errs = append(errs, validateNodeSpec("params.seed", seed)...)

	return errs
}

// validateClusterReshard - static reshard checks: non-empty from/to
// (resolvable endpoints of masters), their difference and slots >= 1. "from/to - existing
// masters" and "slots <= number of slots in source" are checked in Apply live
// topology (CLUSTER NODES), they are not statically visible (texts without a password).
func validateClusterReshard(f map[string]*structpb.Value) []string {
	var errs []string

	from := nodeSpec(f["from"])
	to := nodeSpec(f["to"])
	if len(from) == 0 {
		errs = append(errs, "params.from: must be a map {addr|ip+port} of the source master")
	}
	if len(to) == 0 {
		errs = append(errs, "params.to: must be a map {addr|ip+port} of the target master")
	}
	errs = append(errs, validateNodeSpec("params.from", from)...)
	errs = append(errs, validateNodeSpec("params.to", to)...)

	// from != to: compare by resolved endpoint (ip:port) so that {addr} and
	// {ip,port} forms of the same node were also recognized as a match. An endpoint
	// that does not resolve is already reported above - the comparison then has
	// nothing to say about it, which is NOT the same as having nothing to say.
	fi, fp, _, ferr := resolveNodeEndpoint(from)
	ti, tp, _, terr := resolveNodeEndpoint(to)
	if ferr == nil && terr == nil &&
		net.JoinHostPort(fi, strconv.Itoa(fp)) == net.JoinHostPort(ti, strconv.Itoa(tp)) {
		errs = append(errs, "params.from and params.to must be different masters")
	}

	if intOrDefault(f["slots"], 0) < 1 {
		errs = append(errs, "params.slots: must be an integer >= 1")
	}

	return errs
}

// roleOrDefault - normalized add-node role (default replica, like
// redis-cli `--cluster add-node ... --cluster-slave`).
func roleOrDefault(v *structpb.Value) string {
	if role := strings.TrimSpace(stringOrEmpty(v)); role != "" {
		return role
	}
	return "replica"
}

// applyClusterCreate builds a cluster from nodes-map. The layout of roles is given by EITHER
// deterministic auto-form by sort keys (buildClusterPlan, replicas_per_shard),
// OR EXPLICIT operator topology (params.topology - list of shards [master, replica...],
// buildClusterPlanExplicit). Assembly further (MEET/ADDSLOTS/REPLICATE, idempotency)
// the same - both forms give the plan in one clusterPlan. Idempotent: if the cluster
// already formed (cluster_state:ok, all our nodes are in place, 16384 slots are covered) -
// changed=false, no-op.
func (m *RedisModule) applyClusterCreate(ctx context.Context, stream grpc.ServerStreamingServer[pluginv1.ApplyEvent], params *structpb.Struct) error {
	f := params.GetFields()
	password := stringOrEmpty(f["password"])
	username := stringOrEmpty(f["username"])

	nodes, err := parseClusterNodes(f["nodes"])
	if err != nil {
		return sendFailure(stream, redactError(err, password))
	}
	replicas := intOrDefault(f["replicas_per_shard"], 0)

	// Explicit topology (params.topology) -> layout FROM operator list; otherwise -
	// deterministic auto-layout by sort keys (backward-compat bit-for-bit).
	var plan clusterPlan
	if topology := parseTopology(f["topology"]); len(topology) > 0 {
		plan, err = buildClusterPlanExplicit(nodes, topology)
	} else {
		plan, err = buildClusterPlan(nodes, replicas)
	}
	if err != nil {
		return sendFailure(stream, redactError(err, password))
	}

	tlsP := parseTLS(f)
	connect := func(node clusterNode) (redisConn, error) {
		conn, err := m.openConn(ctx, connConfig{addr: node.addr, username: username, password: password, tls: tlsP})
		if err != nil {
			// A TLS handshake error could theoretically carry a PEM client-key -
			// we edit it DIRECTLY in connect, so that any caller (probe/
			// formCluster/migrate) received an already-sanitized error (key
			// password is already edited by their own redactError text).
			return nil, fmt.Errorf("%s", redactError(err, tlsP.keyPEM))
		}
		return conn, nil
	}

	// Idempotency: let's ask the first master about the current state of the cluster.
	state, liveTable := clusterFormStatus(ctx, connect, plan)
	switch state {
	case clusterFormed:
		return sendOutcome(stream, false, "cluster already formed (no-op)", map[string]any{
			"shards":   int64(len(plan.masters)),
			"replicas": int64(len(plan.replicas)),
			"slots":    int64(totalSlots),
		})
	case clusterPartial:
		// MEET+ADDSLOTS passed, but the replicas are not fully configured (the trace of the one that fell on
		// gossip-timing REPLICATE). We complete ONLY the missing replicas without touching
		// slots (repeated ADDSLOTS of an already assigned slot -> "Slot is already busy").
		done, err := completeReplication(ctx, connect, plan, liveTable, password)
		if err != nil {
			return sendFailure(stream, redactError(err, password))
		}
		return sendOutcome(stream, done > 0, fmt.Sprintf("cluster replication completed: %d replicas attached", done), map[string]any{
			"shards":         int64(len(plan.masters)),
			"replicas":       int64(len(plan.replicas)),
			"slots":          int64(totalSlots),
			"replicas_fixed": int64(done),
			"layout":         layoutSummary(plan),
		})
	}

	if err := formCluster(ctx, connect, plan, password); err != nil {
		return sendFailure(stream, redactError(err, password))
	}

	return sendOutcome(stream, true, fmt.Sprintf("cluster created: %d masters, %d replicas", len(plan.masters), len(plan.replicas)), map[string]any{
		"shards":   int64(len(plan.masters)),
		"replicas": int64(len(plan.replicas)),
		"slots":    int64(totalSlots),
		"layout":   layoutSummary(plan),
	})
}

// applyClusterAddNode attaches ONE new node to the formed cluster
// (day-2): CLUSTER MEET via seed -> waiting for convergence -> role assignment.
//
//	role=replica - CLUSTER REPLICATE: the newcomer becomes a replica of the specified
//	  master (params.master) or, if master is not specified, the master with the smallest
//	  number of replicas (balancing like redis-cli without --cluster-master-id).
//	role=master - empty master (MEET without slots). Slots for new master
//	  transfers a SEPARATE operation reshard (follow-up); add-node does NOT move them.
//
// Idempotent: if new_node is already in the cluster (CLUSTER NODES seed contains it
// ip:port) -> changed=false, no-op.
func (m *RedisModule) applyClusterAddNode(ctx context.Context, stream grpc.ServerStreamingServer[pluginv1.ApplyEvent], params *structpb.Struct) error {
	f := params.GetFields()
	password := stringOrEmpty(f["password"])
	username := stringOrEmpty(f["username"])
	role := roleOrDefault(f["role"])

	newNode, err := resolveSingleNode("new_node", f["new_node"])
	if err != nil {
		return sendFailure(stream, redactError(err, password))
	}
	seed, err := resolveSingleNode("seed", f["seed"])
	if err != nil {
		return sendFailure(stream, redactError(err, password))
	}

	tlsP := parseTLS(f)
	connect := func(node clusterNode) (redisConn, error) {
		conn, err := m.openConn(ctx, connConfig{addr: node.addr, username: username, password: password, tls: tlsP})
		if err != nil {
			// A TLS handshake error could theoretically carry a PEM client-key -
			// we edit it DIRECTLY in connect, so that any caller (probe/
			// formCluster/migrate) received an already-sanitized error (key
			// password is already edited by their own redactError text).
			return nil, fmt.Errorf("%s", redactError(err, tlsP.keyPEM))
		}
		return conn, nil
	}

	seedConn, err := connect(seed)
	if err != nil {
		return sendFailure(stream, "connect seed: "+redactError(err, password))
	}
	defer func() { _ = seedConn.Close() }()

	topology, err := seedConn.Do(ctx, "CLUSTER", "NODES")
	if err != nil {
		return sendFailure(stream, "CLUSTER NODES: "+redactError(err, password))
	}
	existing := parseClusterNodesTable(topology)

	// Idempotency: new to topology -> no-op.
	if nodeInTable(existing, newNode) {
		return sendOutcome(stream, false, "node already in cluster (no-op)", map[string]any{
			"node": newNode.ip + ":" + strconv.Itoa(newNode.port),
			"role": role,
		})
	}

	// Selecting a master for a replica BEFORE MEET - on an empty topology (the newbie is not yet
	// vlit) the choice is deterministic and clear in the error message.
	var masterID string
	if role == "replica" {
		masterID, err = pickReplicationMaster(f["master"], existing)
		if err != nil {
			return sendFailure(stream, redactError(err, password))
		}
	}

	// MEET: using the seed, we invite a newcomer to gossip using his ip:port.
	if _, err := seedConn.Do(ctx, "CLUSTER", "MEET", newNode.ip, strconv.Itoa(newNode.port)); err != nil {
		return sendFailure(stream, fmt.Sprintf("CLUSTER MEET %s: %s",
			net.JoinHostPort(newNode.ip, strconv.Itoa(newNode.port)), redactError(err, password)))
	}

	// Convergence: the seed must see the newcomer (in total there are +1 known nodes).
	if err := waitGossipConverged(ctx, seedConn, len(existing)+1); err != nil {
		return sendFailure(stream, redactError(err, password))
	}

	if role == "master" {
		return sendOutcome(stream, true, "node added as empty master (no slots; reshard to populate)", map[string]any{
			"node": newNode.ip + ":" + strconv.Itoa(newNode.port),
			"role": "master",
		})
	}

	// REPLICATE is executed ON THE NEWBOY (it becomes the master-id replica).
	newConn, err := connect(newNode)
	if err != nil {
		return sendFailure(stream, "connect new_node: "+redactError(err, password))
	}
	defer func() { _ = newConn.Close() }()
	if _, err := newConn.Do(ctx, "CLUSTER", "REPLICATE", masterID); err != nil {
		return sendFailure(stream, "CLUSTER REPLICATE: "+redactError(err, password))
	}

	return sendOutcome(stream, true, "node added as replica", map[string]any{
		"node":      newNode.ip + ":" + strconv.Itoa(newNode.port),
		"role":      "replica",
		"master_id": masterID,
	})
}

// migrateBatch - how many keys in one CLUSTER GETKEYSINSLOT + MIGRATE package
// (redis-cli uses the same scale; limits the size of a single MIGRATE).
const migrateBatch = 100

// applyClusterRemoveNode removes ONE node from the cluster (day-2). If deleted -
// master WITH slots, its slots are FIRST migrated to the remaining masters
// (CLUSTER SETSLOT IMPORTING/MIGRATING + GETKEYSINSLOT + MIGRATE keys + SETSLOT
// NODE), then CLUSTER FORGET on ALL remaining nodes. If the one being deleted is replica
// or master without slots - just FORGET for all.
//
// Idempotent: the node is no longer in CLUSTER NODES seed -> changed=false, no-op.
func (m *RedisModule) applyClusterRemoveNode(ctx context.Context, stream grpc.ServerStreamingServer[pluginv1.ApplyEvent], params *structpb.Struct) error {
	f := params.GetFields()
	password := stringOrEmpty(f["password"])
	username := stringOrEmpty(f["username"])

	removeNode, err := resolveSingleNode("node", f["node"])
	if err != nil {
		return sendFailure(stream, redactError(err, password))
	}
	seed, err := resolveSingleNode("seed", f["seed"])
	if err != nil {
		return sendFailure(stream, redactError(err, password))
	}

	tlsP := parseTLS(f)
	connect := func(node clusterNode) (redisConn, error) {
		conn, err := m.openConn(ctx, connConfig{addr: node.addr, username: username, password: password, tls: tlsP})
		if err != nil {
			// A TLS handshake error could theoretically carry a PEM client-key -
			// we edit it DIRECTLY in connect, so that any caller (probe/
			// formCluster/migrate) received an already-sanitized error (key
			// password is already edited by their own redactError text).
			return nil, fmt.Errorf("%s", redactError(err, tlsP.keyPEM))
		}
		return conn, nil
	}

	seedConn, err := connect(seed)
	if err != nil {
		return sendFailure(stream, "connect seed: "+redactError(err, password))
	}
	defer func() { _ = seedConn.Close() }()

	topology, err := seedConn.Do(ctx, "CLUSTER", "NODES")
	if err != nil {
		return sendFailure(stream, "CLUSTER NODES: "+redactError(err, password))
	}
	table := parseClusterNodesTable(topology)

	target := findNodeRow(table, removeNode)
	if target == nil {
		// Idempotency: the node is no longer in the cluster -> no-op.
		return sendOutcome(stream, false, "node already absent from cluster (no-op)", map[string]any{
			"node": removeNode.ip + ":" + strconv.Itoa(removeNode.port),
		})
	}

	// We transfer slots ONLY if the one being deleted is a master with assigned slots.
	migrated := 0
	if target.isMaster && len(target.slots) > 0 {
		moved, err := migrateSlotsAway(ctx, connect, table, *target, password)
		if err != nil {
			return sendFailure(stream, redactError(err, password))
		}
		migrated = moved
	}

	// FORGET the deleted node to ALL remaining nodes (each node forgets it
	// regardless - gossip-anti-entropy I would have forgotten, but an explicit FORGET on everyone
	// determines the result and closes the re-invitation window).
	forgotten, err := forgetOnRemaining(ctx, connect, table, *target, password)
	if err != nil {
		return sendFailure(stream, redactError(err, password))
	}

	return sendOutcome(stream, true, fmt.Sprintf("node removed (slots migrated: %d, forgotten on %d nodes)", migrated, forgotten), map[string]any{
		"node":           removeNode.ip + ":" + strconv.Itoa(removeNode.port),
		"slots_migrated": int64(migrated),
		"forgotten_on":   int64(forgotten),
	})
}

// applyClusterReshard transfers N slots from one master (from) to another (to)
// in an ALREADY formed cluster (day-2). Mirror redis-cli `--cluster reshard`:
// selects the first N source slots (ascending) and moves each through
// migrateOneSlot(SETSLOT IMPORTING on target -> MIGRATING on source ->
// GETKEYSINSLOT + MIGRATE lossless -> SETSLOT NODE on both nodes).
//
// NOT IDEMPOTENT (deliberately): applying again will shift N more slots from from to
// to. This is an imperative exec-style day-2 operation - the operator calls it explicitly, not
// part of converge. There is no unless/probe "already transferred".
//
// Topology (CLUSTER NODES with from) gives node-id of both masters and current slots
// source. The contact is from itself (it is master, always responds to CLUSTER NODES).
func (m *RedisModule) applyClusterReshard(ctx context.Context, stream grpc.ServerStreamingServer[pluginv1.ApplyEvent], params *structpb.Struct) error {
	f := params.GetFields()
	password := stringOrEmpty(f["password"])
	username := stringOrEmpty(f["username"])

	from, err := resolveSingleNode("from", f["from"])
	if err != nil {
		return sendFailure(stream, redactError(err, password))
	}
	to, err := resolveSingleNode("to", f["to"])
	if err != nil {
		return sendFailure(stream, redactError(err, password))
	}
	slots := intOrDefault(f["slots"], 0)
	if slots < 1 {
		return sendFailure(stream, "params.slots: must be an integer >= 1")
	}

	tlsP := parseTLS(f)
	connect := func(node clusterNode) (redisConn, error) {
		conn, err := m.openConn(ctx, connConfig{addr: node.addr, username: username, password: password, tls: tlsP})
		if err != nil {
			// A TLS handshake error could theoretically carry a PEM client-key -
			// we edit it DIRECTLY in connect, so that any caller (probe/
			// formCluster/migrate) received an already-sanitized error (key
			// password is already edited by their own redactError text).
			return nil, fmt.Errorf("%s", redactError(err, tlsP.keyPEM))
		}
		return conn, nil
	}

	fromConn, err := connect(from)
	if err != nil {
		return sendFailure(stream, "connect from: "+redactError(err, password))
	}
	defer func() { _ = fromConn.Close() }()

	topology, err := fromConn.Do(ctx, "CLUSTER", "NODES")
	if err != nil {
		return sendFailure(stream, "CLUSTER NODES: "+redactError(err, password))
	}
	table := parseClusterNodesTable(topology)

	srcRow := findNodeRow(table, from)
	if srcRow == nil || !srcRow.isMaster {
		return sendFailure(stream, fmt.Sprintf("params.from %s: not a master in this cluster", from.addr))
	}
	dstRow := findNodeRow(table, to)
	if dstRow == nil || !dstRow.isMaster {
		return sendFailure(stream, fmt.Sprintf("params.to %s: not a master in this cluster", to.addr))
	}

	// The first N source slots are in ascending order (deterministic). If source
	// Less than N slots is an input error (you cannot transfer more than you have).
	owned := flattenSlots(srcRow.slots)
	if slots > len(owned) {
		return sendFailure(stream, fmt.Sprintf(
			"params.slots: %d exceeds %d slots currently owned by source master %s", slots, len(owned), from.addr))
	}
	picked := owned[:slots]

	dstConn, err := connect(to)
	if err != nil {
		return sendFailure(stream, "connect to: "+redactError(err, password))
	}
	defer func() { _ = dstConn.Close() }()

	for _, slot := range picked {
		if err := migrateOneSlot(ctx, fromConn, dstConn, srcRow.id, *dstRow, slot, password); err != nil {
			return sendFailure(stream, redactError(err, password))
		}
	}

	return sendOutcome(stream, true, fmt.Sprintf("resharded %d slots: %s -> %s", slots, from.addr, to.addr), map[string]any{
		"slots_moved": int64(slots),
		"from":        from.ip + ":" + strconv.Itoa(from.port),
		"to":          to.ip + ":" + strconv.Itoa(to.port),
	})
}

// flattenSlots expands the master's ranges into a flat one sorted by
// ascending list of individual slots (deterministic selection of the first N for
// reshard). The CLUSTER NODES ranges are already in ascending order, but we sort for
// stability to token order.
func flattenSlots(ranges []slotRange) []int {
	var out []int
	for _, r := range ranges {
		for s := r.from; s <= r.to; s++ {
			out = append(out, s)
		}
	}
	sort.Ints(out)
	return out
}

// findNodeRow returns the topology string of the node being deleted (by ip:port) or nil.
func findNodeRow(table []clusterNodeRow, node clusterNode) *clusterNodeRow {
	want := net.JoinHostPort(node.ip, strconv.Itoa(node.port))
	for i := range table {
		if table[i].ipPort == want {
			return &table[i]
		}
	}
	return nil
}

// remainingMasters - masters of the cluster WITHOUT the node to be deleted, deterministic by id.
// Slot migration goals; FORGET also goes to all the remaining ones (masters+replicas).
func remainingMasters(table []clusterNodeRow, removeID string) []clusterNodeRow {
	var out []clusterNodeRow
	for _, row := range table {
		if row.isMaster && row.id != removeID {
			out = append(out, row)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].id < out[j].id })
	return out
}

// migrateSlotsAway transfers ALL slots of the removed master to the remaining masters
// (round-robin by their sorted order -> deterministic). Returns
// number of transferred slots. Mirror redis-cli reshard for one slot:
//
//	IMPORTING on target -> MIGRATING on source -> key transfer (GETKEYSINSLOT +
//	MIGRATE) -> SETSLOT NODE <to> on both nodes (fixing the owner).
func migrateSlotsAway(ctx context.Context, connect func(clusterNode) (redisConn, error), table []clusterNodeRow, src clusterNodeRow, password string) (int, error) {
	dests := remainingMasters(table, src.id)
	if len(dests) == 0 {
		return 0, fmt.Errorf("cannot migrate slots: no remaining master to receive them")
	}

	srcNode, err := nodeFromRow(src)
	if err != nil {
		return 0, err
	}
	srcConn, err := connect(srcNode)
	if err != nil {
		return 0, fmt.Errorf("connect source master %s: %w", srcNode.addr, err)
	}
	defer func() { _ = srcConn.Close() }()

	// One long-lived connection per target node (SETSLOT is executed on
	// each). We close everything with one defer, without a defer-in-the-loop.
	destConns := make([]redisConn, len(dests))
	defer func() {
		for _, c := range destConns {
			if c != nil {
				_ = c.Close()
			}
		}
	}()
	for i, d := range dests {
		dn, err := nodeFromRow(d)
		if err != nil {
			return 0, err
		}
		c, err := connect(dn)
		if err != nil {
			return 0, fmt.Errorf("connect destination master %s: %w", dn.addr, err)
		}
		destConns[i] = c
	}

	moved := 0
	for _, r := range src.slots {
		for slot := r.from; slot <= r.to; slot++ {
			di := moved % len(dests)
			if err := migrateOneSlot(ctx, srcConn, destConns[di], src.id, dests[di], slot, password); err != nil {
				return moved, err
			}
			moved++
		}
	}
	return moved, nil
}

// migrateOneSlot migrates one slot from source to target (redis-cli algorithm):
// IMPORTING(to) -> MIGRATING(from) -> transfer of all slot keys in batches via
// MIGRATE -> SETSLOT NODE <to-id> on BOTH nodes (new owner).
func migrateOneSlot(ctx context.Context, srcConn, destConn redisConn, srcID string, dest clusterNodeRow, slot int, password string) error {
	slotArg := strconv.Itoa(slot)
	if _, err := destConn.Do(ctx, "CLUSTER", "SETSLOT", slotArg, "IMPORTING", srcID); err != nil {
		return fmt.Errorf("SETSLOT %d IMPORTING: %w", slot, err)
	}
	if _, err := srcConn.Do(ctx, "CLUSTER", "SETSLOT", slotArg, "MIGRATING", dest.id); err != nil {
		return fmt.Errorf("SETSLOT %d MIGRATING: %w", slot, err)
	}

	destIP, destPort, err := splitIPPort(dest.ipPort)
	if err != nil {
		return fmt.Errorf("slot %d destination addr: %w", slot, err)
	}
	for {
		keys, err := srcConn.GetKeysInSlot(ctx, slot, migrateBatch)
		if err != nil {
			return fmt.Errorf("GETKEYSINSLOT %d: %w", slot, err)
		}
		if len(keys) == 0 {
			break // slot is empty
		}
		// MIGRATE <host> <port> "" <db> <timeout> [AUTH pass] KEYS k...
		args := []any{"MIGRATE", destIP, strconv.Itoa(destPort), "", "0", "5000"}
		if password != "" {
			args = append(args, "AUTH", password)
		}
		args = append(args, "KEYS")
		for _, k := range keys {
			args = append(args, k)
		}
		if _, err := srcConn.Do(ctx, args...); err != nil {
			return fmt.Errorf("MIGRATE slot %d: %w", slot, redactErr(err, password))
		}
	}

	// Commit the new slot owner to the source and target. Complete
	// spreading throughout the cluster will complete gossip.
	if _, err := srcConn.Do(ctx, "CLUSTER", "SETSLOT", slotArg, "NODE", dest.id); err != nil {
		return fmt.Errorf("SETSLOT %d NODE (source): %w", slot, err)
	}
	if _, err := destConn.Do(ctx, "CLUSTER", "SETSLOT", slotArg, "NODE", dest.id); err != nil {
		return fmt.Errorf("SETSLOT %d NODE (destination): %w", slot, err)
	}
	return nil
}

// forgetOnRemaining executes CLUSTER FORGET <remove-id> on each remaining
// node (masters + replicas, except the one being deleted). Returns the number of nodes on which
// FORGET completed. "Unknown node" on a separate node (I already forgot) is not an error.
func forgetOnRemaining(ctx context.Context, connect func(clusterNode) (redisConn, error), table []clusterNodeRow, remove clusterNodeRow, password string) (int, error) {
	done := 0
	for _, row := range table {
		if row.id == remove.id {
			continue
		}
		node, err := nodeFromRow(row)
		if err != nil {
			return done, err
		}
		conn, err := connect(node)
		if err != nil {
			return done, fmt.Errorf("connect %s: %w", node.addr, err)
		}
		_, err = conn.Do(ctx, "CLUSTER", "FORGET", remove.id)
		_ = conn.Close()
		if err != nil {
			// The node might have already forgotten the one being deleted (gossip-anti-entropy) -> "Unknown
			// node": not an error, move on.
			if isUnknownNodeErr(err) {
				continue
			}
			return done, fmt.Errorf("CLUSTER FORGET on %s: %w", node.addr, err)
		}
		done++
	}
	return done, nil
}

// nodeFromRow builds a clusterNode from a topology row (for an ip:port connection).
func nodeFromRow(row clusterNodeRow) (clusterNode, error) {
	ip, port, err := splitIPPort(row.ipPort)
	if err != nil {
		return clusterNode{}, fmt.Errorf("node %s: %w", row.id, err)
	}
	return clusterNode{key: row.id, addr: row.ipPort, ip: ip, port: port}, nil
}

// splitIPPort cuts "ip:port" CLUSTER NODES into (ip, port).
func splitIPPort(ipPort string) (string, int, error) {
	h, p, err := net.SplitHostPort(ipPort)
	if err != nil {
		return "", 0, err
	}
	n, err := strconv.Atoi(p)
	if err != nil || n <= 0 {
		return "", 0, fmt.Errorf("invalid port in %q", ipPort)
	}
	return h, n, nil
}

// isUnknownNodeErr - FORGET on an already forgotten node -> "ERR Unknown node...".
func isUnknownNodeErr(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "unknown node")
}

// redactErr strips the password from the error and returns the wrapped error (for
// error chains inside migration; redactError returns a string). Disguise -
// a single point of truth on top of redactError so that the mask edit does not move apart.
func redactErr(err error, password string) error {
	return fmt.Errorf("%s", redactError(err, password))
}

// masterSpecGiven - whether an explicit master-endpoint is specified. Blank spec or spec with
// empty addr/ip+port (scenario sends master.addr: "") if master_sid is not specified
// interpreted as "not specified" -> auto-selection of master.
func masterSpecGiven(spec map[string]*structpb.Value) bool {
	if len(spec) == 0 {
		return false
	}
	return strings.TrimSpace(stringOrEmpty(spec["addr"])) != "" ||
		strings.TrimSpace(stringOrEmpty(spec["ip"])) != ""
}

// resolveSingleNode retrieves a clusterNode from a single specification node
// ({addr|ip+port}). key is the same as the field name (for error messages).
func resolveSingleNode(field string, v *structpb.Value) (clusterNode, error) {
	spec := nodeSpec(v)
	if len(spec) == 0 {
		return clusterNode{}, fmt.Errorf("params.%s: must be a map {addr|ip+port}", field)
	}
	ip, port, addr, err := resolveNodeEndpoint(spec)
	if err != nil {
		return clusterNode{}, fmt.Errorf("params.%s: %w", field, err)
	}
	return clusterNode{key: field, addr: addr, ip: ip, port: port}, nil
}

// pickReplicationMaster determines the master node-id for the new replica. If set
// params.master ({addr|ip+port}) - resolves its id from the topology by ip:port; otherwise
// selects the master with the smallest number of replicas already attached (balancing), when
// equality - deterministic by node-id.
func pickReplicationMaster(masterSpec *structpb.Value, table []clusterNodeRow) (string, error) {
	masters := mastersFromTable(table)
	if len(masters) == 0 {
		return "", fmt.Errorf("cluster has no master to replicate")
	}

	// An explicit master is specified only if the spec carries a non-empty endpoint (empty
	// master.addr from scenario when master_sid is not specified -> auto-selection).
	if spec := nodeSpec(masterSpec); masterSpecGiven(spec) {
		ip, port, _, err := resolveNodeEndpoint(spec)
		if err != nil {
			return "", fmt.Errorf("params.master: %w", err)
		}
		want := net.JoinHostPort(ip, strconv.Itoa(port))
		for _, mr := range masters {
			if mr.ipPort == want {
				return mr.id, nil
			}
		}
		return "", fmt.Errorf("params.master %s: not a master in this cluster", want)
	}

	// Auto-selection: least number of cues; if equal, the smaller node-id.
	replicaCount := make(map[string]int, len(masters))
	for _, row := range table {
		if !row.isMaster && row.masterID != "" {
			replicaCount[row.masterID]++
		}
	}
	best := masters[0]
	for _, mr := range masters[1:] {
		switch {
		case replicaCount[mr.id] < replicaCount[best.id]:
			best = mr
		case replicaCount[mr.id] == replicaCount[best.id] && mr.id < best.id:
			best = mr
		}
	}
	return best.id, nil
}

// clusterNodeRow - one parsed row CLUSTER NODES (necessary fields).
type clusterNodeRow struct {
	id       string
	ipPort   string // "ip:port" client address (without @cport)
	isMaster bool
	masterID string      // master id for the replica ("-" -> "")
	slots    []slotRange // assigned slot ranges (only for masters with slots)
}

// parseClusterNodesTable parses CLUSTER NODES output into strings. String format:
//
//	<id> <ip:port@cport> <flags> <master-id> <ping> <pong> <epoch> <link> [slots...]
//
// Take id, ip:port (up to @), master/slave flag, master-id replicas and assigned
// slot ranges (fields from 8th). Slot tokens of the form "[N-<importing-id" / "[N->-
// migrating-id" (in-flight migration) are skipped - this is not a steady-state possession.
func parseClusterNodesTable(s string) []clusterNodeRow {
	var rows []clusterNodeRow
	for _, line := range strings.Split(s, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		ipPort := fields[1]
		if at := strings.IndexByte(ipPort, '@'); at >= 0 {
			ipPort = ipPort[:at]
		}
		masterID := fields[3]
		if masterID == "-" {
			masterID = ""
		}
		var slots []slotRange
		if len(fields) > 8 {
			slots = parseSlotTokens(fields[8:])
		}
		rows = append(rows, clusterNodeRow{
			id:       fields[0],
			ipPort:   ipPort,
			isMaster: strings.Contains(fields[2], "master"),
			masterID: masterID,
			slots:    slots,
		})
	}
	return rows
}

// parseSlotTokens parses the slot tokens of the CLUSTER NODES string into ranges. Token -
// either a single slot "N" or a range "N-M". Importing/migrating tokens into
// square brackets ("[...") - unstable in-flight migration, skipped.
func parseSlotTokens(tokens []string) []slotRange {
	var ranges []slotRange
	for _, tok := range tokens {
		if strings.HasPrefix(tok, "[") {
			continue
		}
		from, to, ok := strings.Cut(tok, "-")
		lo, err := strconv.Atoi(from)
		if err != nil {
			continue
		}
		hi := lo
		if ok {
			if hi, err = strconv.Atoi(to); err != nil {
				continue
			}
		}
		ranges = append(ranges, slotRange{from: lo, to: hi})
	}
	return ranges
}

// nodeInTable - whether the node (by ip:port) is present in the topology (idempotency).
func nodeInTable(table []clusterNodeRow, node clusterNode) bool {
	want := net.JoinHostPort(node.ip, strconv.Itoa(node.port))
	for _, row := range table {
		if row.ipPort == want {
			return true
		}
	}
	return false
}

// masterRow - master from the topology (id + ip:port) to select the REPLICATE target.
type masterRow struct {
	id     string
	ipPort string
}

// mastersFromTable is a deterministically sorted (by id) list of masters.
func mastersFromTable(table []clusterNodeRow) []masterRow {
	var out []masterRow
	for _, row := range table {
		if row.isMaster {
			out = append(out, masterRow{id: row.id, ipPort: row.ipPort})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].id < out[j].id })
	return out
}

// parseClusterNodes resolves nodes-map into deterministically sorted
// slice clusterNode. Each node is either {addr: "host:port"} or {ip, port}.
func parseClusterNodes(v *structpb.Value) ([]clusterNode, error) {
	raw := nodeSpecs(v)
	if len(raw) == 0 {
		return nil, fmt.Errorf("params.nodes: must be a non-empty map")
	}

	keys := make([]string, 0, len(raw))
	for k := range raw {
		keys = append(keys, k)
	}
	sort.Strings(keys) // deterministic layout base

	out := make([]clusterNode, 0, len(keys))
	for _, k := range keys {
		spec := raw[k]
		ip, port, addr, err := resolveNodeEndpoint(spec)
		if err != nil {
			return nil, fmt.Errorf("params.nodes[%s]: %w", k, err)
		}
		out = append(out, clusterNode{key: k, addr: addr, ip: ip, port: port})
	}
	return out, nil
}

// parseTopology parses params.topology - a list of shards, each shard - a list
// SID ([master_sid, replica_sid,...], first = master). Empty/missing
// topology -> nil (caller interprets as "auto-layout", old behavior). Non-strings
// inside the shard are skipped (validation in validateClusterCreate has already rejected such
// entrance; here fail-safe). structpb format: ListValue from ListValues StringValue.
func parseTopology(v *structpb.Value) [][]string {
	if v == nil {
		return nil
	}
	lv, ok := v.GetKind().(*structpb.Value_ListValue)
	if !ok {
		return nil
	}
	shards := lv.ListValue.GetValues()
	if len(shards) == 0 {
		return nil
	}
	out := make([][]string, 0, len(shards))
	for _, shard := range shards {
		out = append(out, stringList(shard))
	}
	return out
}

// resolveNodeEndpoint retrieves (ip, port, addr) from the specification of one node.
// Priority: explicit ip+port; otherwise split addr "host:port". addr for connection,
// ip+port - for CLUSTER MEET (gossip operates ip:port).
func resolveNodeEndpoint(spec map[string]*structpb.Value) (ip string, port int, addr string, err error) {
	ip = strings.TrimSpace(stringOrEmpty(spec["ip"]))
	// A node spec is nested inside a map-typed param, so the object's type gate
	// does not reach this key (params.go): refuse a non-integer here rather than
	// read it as 0 and fall through to the addr branch, which would drop the
	// ip+port the author actually wrote.
	port, err = intField(spec, "port", "port", 0)
	if err != nil {
		return "", 0, "", err
	}
	addr = strings.TrimSpace(stringOrEmpty(spec["addr"]))

	switch {
	case ip != "" && port > 0:
		if addr == "" {
			addr = net.JoinHostPort(ip, strconv.Itoa(port))
		}
		return ip, port, addr, nil
	case addr != "":
		h, p, splitErr := net.SplitHostPort(addr)
		if splitErr != nil {
			return "", 0, "", fmt.Errorf("addr %q: %v", addr, splitErr)
		}
		pn, convErr := strconv.Atoi(p)
		if convErr != nil || pn <= 0 {
			return "", 0, "", fmt.Errorf("addr %q: invalid port", addr)
		}
		return h, pn, addr, nil
	default:
		return "", 0, "", fmt.Errorf("must specify addr (host:port) or ip+port")
	}
}

// buildClusterPlan arranges nodes into roles and slots in a STRICTLY deterministic manner.
//
//	shards   = len(nodes) / (1 + replicas_per_shard)
//	masters = first shards of nodes (in sorted order)
//	replicas = rest, round-robin to masters
//	slots = 16384 equally divided between masters; the rest goes to the first masters
func buildClusterPlan(nodes []clusterNode, replicas int) (clusterPlan, error) {
	if replicas < 0 {
		return clusterPlan{}, fmt.Errorf("params.replicas_per_shard: must be >= 0")
	}
	shardSize := 1 + replicas
	if len(nodes)%shardSize != 0 {
		return clusterPlan{}, fmt.Errorf("params.nodes: %d nodes not divisible by shard size %d", len(nodes), shardSize)
	}
	shards := len(nodes) / shardSize
	if shards < 1 {
		return clusterPlan{}, fmt.Errorf("params.nodes: need at least %d nodes for one shard", shardSize)
	}

	plan := clusterPlan{
		masters:   nodes[:shards],
		replicas:  nodes[shards:],
		slots:     allocateSlots(shards),
		replicaOf: make([]int, len(nodes)-shards),
	}
	// Round-robin replicas to masters: replica j -> master j%shards.
	for j := range plan.replicas {
		plan.replicaOf[j] = j % shards
	}
	return plan, nil
}

// buildClusterPlanExplicit arranges nodes into roles FROM EXPLICIT TOPOLOGY operator
// (params.topology) - a mirror of buildClusterPlan, but without auto-layout: the operator itself
// specified a list of shards, each shard - [master_sid, replica_sid,...] (first = master).
//
//	masters[i] = nodes[topology[i][0]] (master of the i-th shard)
//	replicas = tails of shards (topology[i][1:]), replicaOf = i (their shard)
//	slots = allocateSlots(len(topology)) (same uniform as in auto)
//
// The order of masters is the order of shards topology (operator layout, NOT sort keys).
// Replicas are managed shard by shard (deterministically). Slots are divided EXACTLY the same way,
// as in auto-layout (allocateSlots), - the operator controls only who is master
// and WHO is whose replica, and not by the size of the ranges. All SIDs are assumed to be valid
// (exist in nodes, no duplicates, shards are non-empty) - this guarantees
// validateClusterCreate; here the error occurs only if there is a missing index (fail-safe).
func buildClusterPlanExplicit(nodes []clusterNode, topology [][]string) (clusterPlan, error) {
	if len(topology) == 0 {
		return clusterPlan{}, fmt.Errorf("params.topology: must be a non-empty list of shards")
	}
	byKey := make(map[string]clusterNode, len(nodes))
	for _, n := range nodes {
		byKey[n.key] = n
	}

	plan := clusterPlan{
		masters: make([]clusterNode, 0, len(topology)),
		slots:   allocateSlots(len(topology)),
	}
	for i, shard := range topology {
		if len(shard) == 0 {
			return clusterPlan{}, fmt.Errorf("params.topology[%d]: shard must have at least a master SID", i)
		}
		master, ok := byKey[shard[0]]
		if !ok {
			return clusterPlan{}, fmt.Errorf("params.topology[%d]: master SID %q not found in nodes", i, shard[0])
		}
		plan.masters = append(plan.masters, master)
		for _, sid := range shard[1:] {
			replica, ok := byKey[sid]
			if !ok {
				return clusterPlan{}, fmt.Errorf("params.topology[%d]: replica SID %q not found in nodes", i, sid)
			}
			plan.replicas = append(plan.replicas, replica)
			plan.replicaOf = append(plan.replicaOf, i)
		}
	}
	return plan, nil
}

// allocateSlots divides 16384 slots equally among shards masters; remainder
// (16384 % shards) is distributed over one slot to the first masters.
func allocateSlots(shards int) []slotRange {
	base := totalSlots / shards
	rem := totalSlots % shards
	ranges := make([]slotRange, shards)
	cursor := 0
	for i := 0; i < shards; i++ {
		size := base
		if i < rem {
			size++
		}
		ranges[i] = slotRange{from: cursor, to: cursor + size - 1}
		cursor += size
	}
	return ranges
}

// clusterFormState - the result of the create idempotency check against the LIVE cluster.
type clusterFormState int

const (
	// clusterNotFormed - there is no cluster yet (or has not converged): you need to assemble it from scratch
	// (MEET -> ADDSLOTS -> REPLICATE). The connection file to the first master is also here.
	clusterNotFormed clusterFormState = iota
	// clusterPartial - MEET+ADDSLOTS passed (cluster_state:ok, all nodes, 16384
	// slot), but REPLICAS are not fully configured: part of the nodes that are planned to be
	// replicas, was left by the masters without slots (a typical trace of something that fell on gossip-
	// timing of the first REPLICATE). You only need to COMPLETE the replication without touching the slots.
	clusterPartial
	// clusterFormed - the cluster is fully assembled (slots + all replicas in place) ->
	// no-op.
	clusterFormed
)

// clusterFormStatus determines the status of cluster formation according to the LIVE topology
// first master. Connection failure / abnormal INFO is interpreted as clusterNotFormed
// (nodes can only rise) - NOT an error.
//
// The completeness of replicas (clusterPartial vs clusterFormed) is checked by CLUSTER NODES:
// the number of slave nodes in a live topology must match len(plan.replicas). If
// INFO reports state:ok + all nodes + 16384 slots, but LESS replicas than expected -
// this is a partial topology (a failed REPLICATE froze the cluster as N master), and
// the old gate mistakenly considered it to be formed. We also return a live topology
// (table) - it is reused by completeReplication (node-id of masters by ip:port).
func clusterFormStatus(ctx context.Context, connect func(clusterNode) (redisConn, error), plan clusterPlan) (clusterFormState, []clusterNodeRow) {
	first := plan.masters[0]
	conn, err := connect(first)
	if err != nil {
		return clusterNotFormed, nil // nodes rise; not formed
	}
	defer func() { _ = conn.Close() }()

	info, err := conn.Do(ctx, "CLUSTER", "INFO")
	if err != nil {
		return clusterNotFormed, nil
	}
	fields := parseClusterInfo(info)
	if fields["cluster_state"] != "ok" {
		return clusterNotFormed, nil
	}
	if known, _ := strconv.Atoi(fields["cluster_known_nodes"]); known != len(plan.masters)+len(plan.replicas) {
		return clusterNotFormed, nil
	}
	if assigned, _ := strconv.Atoi(fields["cluster_slots_assigned"]); assigned != totalSlots {
		return clusterNotFormed, nil
	}

	// state:ok + all nodes + full slots. We distinguish partial/formed by the number of replicas
	// in living topology. Any reading file NODES -> consider NOT formed (we will rebuild
	// replication is safe: completeReplication is idempotent).
	nodesOut, err := conn.Do(ctx, "CLUSTER", "NODES")
	if err != nil {
		return clusterPartial, nil //nolint:nilerr
	}
	table := parseClusterNodesTable(nodesOut)
	if countReplicas(table) >= len(plan.replicas) {
		return clusterFormed, table
	}
	return clusterPartial, table
}

// countReplicas - the number of slave nodes (replicas) in the live topology.
func countReplicas(table []clusterNodeRow) int {
	n := 0
	for _, row := range table {
		if !row.isMaster {
			n++
		}
	}
	return n
}

// formCluster executes the assembly: MEET all nodes in gossip from the first master,
// waiting for convergence, ADDSLOTS to masters, REPLICATE to replicas.
func formCluster(ctx context.Context, connect func(clusterNode) (redisConn, error), plan clusterPlan, password string) error {
	all := append(append([]clusterNode{}, plan.masters...), plan.replicas...)

	// One long-lived connection per node (needed for MEET/ADDSLOTS/REPLICATE
	// exactly on this node). We close everything with one defer, without a defer-in-the-loop.
	conns := make([]redisConn, 0, len(all))
	defer func() {
		for _, c := range conns {
			_ = c.Close()
		}
	}()
	for _, n := range all {
		c, err := connect(n)
		if err != nil {
			return fmt.Errorf("connect %s: %w", n.addr, err)
		}
		conns = append(conns, c)
	}

	// CLUSTER MYID is needed ONLY by masters: REPLICATE requires the node-id of the master,
	// the replica's own id is not used. We ask the masters for id (conns[:M]).
	idByKey := make(map[string]string, len(plan.masters))
	for i, master := range plan.masters {
		id, err := conns[i].Do(ctx, "CLUSTER", "MYID")
		if err != nil {
			return fmt.Errorf("CLUSTER MYID %s: %w", master.addr, err)
		}
		idByKey[master.key] = strings.TrimSpace(id)
	}

	// Gossip: from the first MEET node to all the others via ip:port.
	hub := conns[0]
	for _, n := range all[1:] {
		if _, err := hub.Do(ctx, "CLUSTER", "MEET", n.ip, strconv.Itoa(n.port)); err != nil {
			return fmt.Errorf("CLUSTER MEET %s: %w", net.JoinHostPort(n.ip, strconv.Itoa(n.port)), err)
		}
	}

	if err := waitGossipConverged(ctx, hub, len(all)); err != nil {
		return err
	}

	// ADDSLOTS to masters - their deterministic ranges.
	for i, master := range plan.masters {
		r := plan.slots[i]
		args := addSlotsArgs(r)
		if _, err := conns[i].Do(ctx, args...); err != nil {
			return fmt.Errorf("CLUSTER ADDSLOTS %s [%d-%d]: %w", master.addr, r.from, r.to, err)
		}
	}

	// REPLICATE to replicas - the id of their master. BEFORE each REPLICATE we wait until the node
	// the replica will see the node-id of the master in ITS CLUSTER NODES (gossip-gate): otherwise on
	// unsettled gossip REPLICATE crashes with "ERR Unknown node" (live bug -
	// the first REPLICATE froze the cluster as N master without replicas).
	for j, replica := range plan.replicas {
		master := plan.masters[plan.replicaOf[j]]
		masterID := idByKey[master.key]
		if masterID == "" {
			return fmt.Errorf("replica %s: unknown master id for %s", replica.addr, master.key)
		}
		ci := len(plan.masters) + j
		if err := waitMasterVisible(ctx, conns[ci], masterID); err != nil {
			return fmt.Errorf("replica %s: %w", replica.addr, err)
		}
		if _, err := conns[ci].Do(ctx, "CLUSTER", "REPLICATE", masterID); err != nil {
			return fmt.Errorf("CLUSTER REPLICATE %s -> %s: %w", replica.addr, master.addr, err)
		}
	}

	return nil
}

// completeReplication completes REPLICATION on a partially assembled cluster
// (clusterPartial): MEET and ADDSLOTS have already passed, but some of the nodes that are scheduled to
// be replicas, remained masters without slots (fell on gossip-timing first
// REPLICATE). Sends CLUSTER REPLICATE only to those nodes that are NOT replicas yet
// your master; node-id of masters is taken from the LIVE topology (liveTable) by ip:port
// (nodes are already in the cluster - CLUSTER MYID is not needed). Idempotent: node, already
// bound to the desired master is skipped. Returns the number of completed replicas.
//
// BEFORE REPLICATE - the same gossip-gate as in formCluster (the replica node must
// see master node-id in your CLUSTER NODES). Slots are NOT touched (repeat
// ADDSLOTS of an already assigned slot -> "Slot is already busy").
func completeReplication(ctx context.Context, connect func(clusterNode) (redisConn, error), plan clusterPlan, liveTable []clusterNodeRow, password string) (int, error) {
	// node-id of scheduled masters - from the live topology according to their ip:port. If alive
	// there is no topology (read NODES file in probe), read CLUSTER MYID of each master.
	masterID, err := resolvePlanMasterIDs(ctx, connect, plan, liveTable)
	if err != nil {
		return 0, err
	}

	done := 0
	for j, replica := range plan.replicas {
		master := plan.masters[plan.replicaOf[j]]
		wantMasterID := masterID[master.key]
		if wantMasterID == "" {
			return done, fmt.Errorf("replica %s: unknown master id for %s", replica.addr, master.key)
		}
		// Idempotency: the node is already a replica of the desired master in the living topology -> skip.
		if replicaAlreadyAttached(liveTable, replica, wantMasterID) {
			continue
		}

		conn, err := connect(replica)
		if err != nil {
			return done, fmt.Errorf("connect replica %s: %w", replica.addr, err)
		}
		if err := waitMasterVisible(ctx, conn, wantMasterID); err != nil {
			_ = conn.Close()
			return done, fmt.Errorf("replica %s: %w", replica.addr, err)
		}
		_, err = conn.Do(ctx, "CLUSTER", "REPLICATE", wantMasterID)
		_ = conn.Close()
		if err != nil {
			return done, fmt.Errorf("CLUSTER REPLICATE %s -> %s: %w", replica.addr, master.addr, err)
		}
		done++
	}
	return done, nil
}

// resolvePlanMasterIDs matches each plan master (plan.masters) with its
// node-id. First it tries using the LIVE topology (liveTable) - the master is already in the cluster,
// its id is known by ip:port. For those not found (or if the liveTable is empty) - ask
// CLUSTER MYID directly (fallback, eg probe could not read NODES).
func resolvePlanMasterIDs(ctx context.Context, connect func(clusterNode) (redisConn, error), plan clusterPlan, liveTable []clusterNodeRow) (map[string]string, error) {
	out := make(map[string]string, len(plan.masters))
	for _, master := range plan.masters {
		if id := nodeIDFromTable(liveTable, master); id != "" {
			out[master.key] = id
			continue
		}
		conn, err := connect(master)
		if err != nil {
			return nil, fmt.Errorf("connect master %s: %w", master.addr, err)
		}
		id, err := conn.Do(ctx, "CLUSTER", "MYID")
		_ = conn.Close()
		if err != nil {
			return nil, fmt.Errorf("CLUSTER MYID %s: %w", master.addr, err)
		}
		out[master.key] = strings.TrimSpace(id)
	}
	return out, nil
}

// nodeIDFromTable - node-id of the node (by ip:port) from the live topology or "".
func nodeIDFromTable(table []clusterNodeRow, node clusterNode) string {
	want := net.JoinHostPort(node.ip, strconv.Itoa(node.port))
	for _, row := range table {
		if row.ipPort == want {
			return row.id
		}
	}
	return ""
}

// replicaAlreadyAttached - the node is already a replica of the master ID in the live topology.
func replicaAlreadyAttached(table []clusterNodeRow, node clusterNode, masterID string) bool {
	want := net.JoinHostPort(node.ip, strconv.Itoa(node.port))
	for _, row := range table {
		if row.ipPort == want {
			return !row.isMaster && row.masterID == masterID
		}
	}
	return false
}

// waitMasterVisible waits (bounded retry, reuses MEET gossip limits) until
// the replica node will see the node-id of its master in ITS CLUSTER NODES. Without this
// REPLICATE on an unsettled gossip drops "ERR Unknown node". Returns an error
// if the master-id did not appear beyond the limit (an obvious failure is better than an "Unknown node").
func waitMasterVisible(ctx context.Context, conn redisConn, masterID string) error {
	for attempt := 0; attempt < gossipPollAttempts; attempt++ {
		nodesOut, err := conn.Do(ctx, "CLUSTER", "NODES")
		if err != nil {
			return fmt.Errorf("CLUSTER NODES: %w", err)
		}
		if nodeIDInTable(parseClusterNodesTable(nodesOut), masterID) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(gossipPollInterval):
		}
	}
	return fmt.Errorf("master node %s not visible in local topology after %d attempts (gossip did not converge)", masterID, gossipPollAttempts)
}

// nodeIDInTable - whether a node with a given node-id is present in the topology.
func nodeIDInTable(table []clusterNodeRow, id string) bool {
	for _, row := range table {
		if row.id == id {
			return true
		}
	}
	return false
}

// waitGossipConverged waits (limited retry, NOT infinite) for hub to see
// in CLUSTER NODES all want nodes. Returns an error if the limit is not met.
func waitGossipConverged(ctx context.Context, hub redisConn, want int) error {
	for attempt := 0; attempt < gossipPollAttempts; attempt++ {
		nodes, err := hub.Do(ctx, "CLUSTER", "NODES")
		if err != nil {
			return fmt.Errorf("CLUSTER NODES: %w", err)
		}
		if countClusterNodes(nodes) >= want {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(gossipPollInterval):
		}
	}
	return fmt.Errorf("gossip did not converge: fewer than %d nodes visible after %d attempts", want, gossipPollAttempts)
}

// addSlotsArgs builds the CLUSTER ADDSLOTS arguments for a contiguous range.
func addSlotsArgs(r slotRange) []any {
	args := make([]any, 0, 2+(r.to-r.from+1))
	args = append(args, "CLUSTER", "ADDSLOTS")
	for s := r.from; s <= r.to; s++ {
		args = append(args, strconv.Itoa(s))
	}
	return args
}

// parseClusterInfo parses the output of CLUSTER INFO ("key:value" line by line).
func parseClusterInfo(s string) map[string]string {
	out := map[string]string{}
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(strings.TrimRight(line, "\r"))
		if line == "" {
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

// countClusterNodes counts non-empty lines of CLUSTER NODES output (one line =
// one node).
func countClusterNodes(s string) int {
	n := 0
	for _, line := range strings.Split(s, "\n") {
		if strings.TrimSpace(line) != "" {
			n++
		}
	}
	return n
}

// layoutSummary - deterministic human-readable summary of the layout for
// Output (no secrets: only key and slots).
func layoutSummary(plan clusterPlan) string {
	parts := make([]string, 0, len(plan.masters)+len(plan.replicas))
	for i, master := range plan.masters {
		r := plan.slots[i]
		parts = append(parts, fmt.Sprintf("%s=master[%d-%d]", master.key, r.from, r.to))
	}
	for j, replica := range plan.replicas {
		parts = append(parts, fmt.Sprintf("%s=replica->%s", replica.key, plan.masters[plan.replicaOf[j]].key))
	}
	return strings.Join(parts, ",")
}
