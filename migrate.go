// migrate-cluster redis plugin - migration "old cluster -> new"
// cluster of the same topology" in THREE day-2 steps (each is a separate action cluster):
//
//	join-external - entry of NEW cluster-mode nodes into an ALREADY existing one
//	                     (old) cluster with replicas of old masters 1:1 (CLUSTER
//	                     MEET + REPLICATE); new nodes are catching up with the data.
//	failover-takeover - promotion of these replicas to the master via GRACEFUL CLUSTER
//	                     FAILOVER (first sync-gate master_link_status==up on ALL;
//	                     fail-closed without escalation to FORCE/TAKEOVER - split-brain).
//	forget-external - throwing out old nodes from the cluster (CLUSTER FORGET all
//	                     old node-id on each new node; the slots are already with the new ones).
//
// ENTIRELY via go-redis (CLUSTER NODES / MEET / REPLICATE / FAILOVER / FORGET +
// INFO replication) like create/add-node: no redis-cli/shell, capability
// remains network_outbound. SAME network and SAME cluster password (operator
// aligns new password == old before launch) - single password/tls for old ones
// seed nodes and to new nodes.
//
// 1:1 mapping is DETERMINISTIC: new nodes are sorted by nodes-map key (as
// buildClusterPlan), old masters - in ascending order of the first slot range.
// The i-th new node replicates the i-th old master. It requires EXACTLY the same amount
// new nodes, how many old masters (shards_dest == shards_source) - otherwise
// fail-fast (1:1 is not possible). shards_source is not visible in the render phase (it is in the live
// topology of the old cluster), so the check is runtime-assert in Apply.
//
// Idempotent: the node is already a replica of the desired old master (CLUSTER NODES of the node) ->
// no-op for him; apply again on the converged input -> changed=false.
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

// validateClusterJoinExternal - static checks join-external: non-empty nodes-map
// with a resolvable endpoint on each node, non-empty source_nodes,
// shards_dest >= 1. Match shards_dest
// the number of new nodes AND the number of old masters is checked in Apply (the number of old
// masters are visible only in live topology). Texts without password.
func validateClusterJoinExternal(f map[string]*structpb.Value) []string {
	var errs []string

	nodes := nodeSpecs(f["nodes"])
	if len(nodes) == 0 {
		errs = append(errs, "params.nodes: must be a non-empty map (key -> {addr|ip+port}) of the NEW cluster nodes")
	}
	errs = append(errs, validateNodeSpecs(nodes)...)
	if len(stringList(f["source_nodes"])) == 0 {
		errs = append(errs, "params.source_nodes: must be a non-empty list of seed nodes (host:port) of the SOURCE cluster")
	}
	if intOrDefault(f["shards_dest"], 0) < 1 {
		errs = append(errs, "params.shards_dest: must be an integer >= 1")
	}

	return errs
}

// applyClusterJoinExternal merges new cluster-mode nodes into the old cluster and
// makes each a replica of the mapped old master (day-2 migration step 1):
//
//  1. connection to the first source-seed -> CLUSTER NODES -> old masters + their slots;
//  2. fail-fast: number of old masters != shards_dest -> 1:1 impossible;
//  3. 1:1 mapping - new node i (sorting nodes keys) old master i
//     (sorting by ascending slot range);
//  4. on each new node: MEET old-seed -> waitGossipConverged -> REPLICATE
//     old-master-id (idempotent: already a replica of the desired master -> no-op).
//
// The final Output carries a mapping (new-key -> old-master-id) and per-node
// join-status (joined|already). The password is NOT included in the events (IB ADR-010).
func (m *RedisModule) applyClusterJoinExternal(ctx context.Context, stream grpc.ServerStreamingServer[pluginv1.ApplyEvent], params *structpb.Struct) error {
	f := params.GetFields()
	password := stringOrEmpty(f["password"])
	username := stringOrEmpty(f["username"])
	shardsDest := intOrDefault(f["shards_dest"], 0)

	newNodes, err := parseClusterNodes(f["nodes"])
	if err != nil {
		return sendFailure(stream, redactError(err, password))
	}
	sources, err := parseSourceSeeds(f["source_nodes"])
	if err != nil {
		return sendFailure(stream, redactError(err, password))
	}

	// The number of NEW nodes must match shards_dest (1:1 for exactly one replica
	// to the old master - the pilot does not make >1 replica and does not leave nodes without a pair).
	if len(newNodes) != shardsDest {
		return sendFailure(stream, fmt.Sprintf(
			"params.nodes: %d new nodes != shards_dest %d (join-external maps exactly one new node per source master)",
			len(newNodes), shardsDest))
	}

	tlsP := parseTLS(f)
	connect := func(node clusterNode) (redisConn, error) {
		conn, err := m.openConn(ctx, connConfig{addr: node.addr, username: username, password: password, tls: tlsP})
		if err != nil {
			// TLS-handshake error theoretically carries PEM client-key - edit
			// it is RIGHT here (the password is edited by the caller separately in the text).
			return nil, fmt.Errorf("%s", redactError(err, tlsP.keyPEM))
		}
		return conn, nil
	}

	// Topology of the OLD cluster with the first available source-seed. We sort through the seeds
	// in order: the first CLUSTER NODES to respond sets the topology (like redis-cli,
	// which takes the first reachable node).
	srcMasters, seedEndpoint, err := sourceMasters(ctx, connect, sources, password)
	if err != nil {
		return sendFailure(stream, redactError(err, password))
	}

	// FAIL-FAST: 1:1 mapping is possible only with an equal number of old masters and
	// new nodes (= shards_dest). shards_source is not visible to the render phase (live
	// topology), so the assert is here.
	if len(srcMasters) != shardsDest {
		return sendFailure(stream, fmt.Sprintf(
			"source cluster has %d masters, dest expects %d shards - 1:1 mapping impossible (align shards_dest with source master count)",
			len(srcMasters), shardsDest))
	}

	// Mapping 1:1: new node i old master i. newNodes are already sorted by
	// key (parseClusterNodes), srcMasters - in ascending slot range.
	results := make([]joinResult, len(newNodes))
	mapping := make(map[string]any, len(newNodes))
	for i, node := range newNodes {
		master := srcMasters[i]
		res, err := joinNodeToMaster(ctx, connect, node, master, seedEndpoint, password)
		if err != nil {
			return sendFailure(stream, redactError(err, password))
		}
		results[i] = res
		mapping[node.key] = master.id
	}

	joined := 0
	statuses := make([]string, len(results))
	for i, r := range results {
		statuses[i] = r.node.key + "=" + r.status + "->" + r.masterID
		if r.status == "joined" {
			joined++
		}
	}

	return sendOutcome(stream, joined > 0, fmt.Sprintf(
		"join-external: %d/%d new nodes joined source cluster (%d masters replicated 1:1)",
		joined, len(newNodes), len(srcMasters)),
		map[string]any{
			"shards":     int64(len(srcMasters)),
			"joined":     int64(joined),
			"nodes":      int64(len(newNodes)),
			"mapping":    mappingSummary(results),
			"per_node":   strings.Join(statuses, ","),
			"source_via": seedEndpoint,
		})
}

// joinResult - the result of the entry of one new node: status (joined|already) and
// node-id of the old master whose replica the node became.
type joinResult struct {
	node     clusterNode
	masterID string
	status   string // "joined" (did REPLICATE) | "already" (already a replica)
}

// joinNodeToMaster merges one new node into the old cluster and makes it a replica
// given by the old master. Idempotent: the node is already a replica of this master
// (according to its CLUSTER NODES) -> status "already", REPLICATE is NOT sent.
//
//	MEET old-seed (by ip:port) -> waitGossipConverged (the node saw the old cluster)
//	-> REPLICATE old-master-id (the node becomes a replica of the mapped master).
func joinNodeToMaster(ctx context.Context, connect func(clusterNode) (redisConn, error), node clusterNode, master sourceMaster, seedEndpoint, password string) (joinResult, error) {
	conn, err := connect(node)
	if err != nil {
		return joinResult{}, fmt.Errorf("connect new node %s: %w", node.addr, err)
	}
	defer func() { _ = conn.Close() }()

	// Idempotency: the node is already a replica of the desired master -> no-op (its CLUSTER NODES
	// carries the string of the node itself with master-id == target). On isolated fresh
	// the node has its own master-id line is empty -> let's go pour it in.
	own, err := conn.Do(ctx, "CLUSTER", "NODES")
	if err != nil {
		return joinResult{}, fmt.Errorf("CLUSTER NODES on new node %s: %w", node.addr, err)
	}
	if alreadyReplicaOf(parseClusterNodesTable(own), node, master.id) {
		return joinResult{node: node, masterID: master.id, status: "already"}, nil
	}

	// MEET the old seed: the node gets acquainted with the gossip of the old cluster via ip:port.
	seedIP, seedPort, err := splitIPPort(seedEndpoint)
	if err != nil {
		return joinResult{}, fmt.Errorf("source seed %q: %w", seedEndpoint, err)
	}
	if _, err := conn.Do(ctx, "CLUSTER", "MEET", seedIP, strconv.Itoa(seedPort)); err != nil {
		return joinResult{}, fmt.Errorf("CLUSTER MEET %s from %s: %w",
			net.JoinHostPort(seedIP, strconv.Itoa(seedPort)), node.addr, err)
	}

	// Convergence: the node must see the entire old cluster + itself. We are waiting for him to
	// CLUSTER NODES at least the target master (its id) will appear - otherwise REPLICATE
	// will run into an unknown node-id.
	if err := waitNodeKnows(ctx, conn, master.id); err != nil {
		return joinResult{}, fmt.Errorf("new node %s: %w", node.addr, err)
	}

	if _, err := conn.Do(ctx, "CLUSTER", "REPLICATE", master.id); err != nil {
		return joinResult{}, fmt.Errorf("CLUSTER REPLICATE %s on %s: %w", master.id, node.addr, err)
	}
	return joinResult{node: node, masterID: master.id, status: "joined"}, nil
}

// sourceMaster - old master from the source topology: node-id + first slot
// (1:1 mapping deterministic sort key).
type sourceMaster struct {
	id        string
	ipPort    string
	firstSlot int
}

// sourceMasters connects to source-seeds in order, takes CLUSTER NODES from
// first responder and returns her masters WITH slots, sorted by
// increasing the first slot range (deterministic mapping base 1:1).
// It also returns the endpoint (ip:port) of the triggered seed for subsequent MEETs.
// Masters WITHOUT slots (fresh empty) do not go into mapping - we replicate owners
// data. All seeds are unavailable -> error.
func sourceMasters(ctx context.Context, connect func(clusterNode) (redisConn, error), seeds []clusterNode, password string) ([]sourceMaster, string, error) {
	var lastErr error
	for _, seed := range seeds {
		conn, err := connect(seed)
		if err != nil {
			lastErr = fmt.Errorf("connect source seed %s: %w", seed.addr, err)
			continue
		}
		topology, err := conn.Do(ctx, "CLUSTER", "NODES")
		_ = conn.Close()
		if err != nil {
			lastErr = fmt.Errorf("CLUSTER NODES on source seed %s: %w", seed.addr, err)
			continue
		}

		masters := mastersWithSlots(parseClusterNodesTable(topology))
		if len(masters) == 0 {
			lastErr = fmt.Errorf("source seed %s: no masters with assigned slots in CLUSTER NODES", seed.addr)
			continue
		}
		return masters, net.JoinHostPort(seed.ip, strconv.Itoa(seed.port)), nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("params.source_nodes: no reachable source seed")
	}
	return nil, "", lastErr
}

// mastersWithSlots retrieves masters WITH slots from the topology, sorted by
// increasing the first slot (deterministic 1:1 mapping). Master without slots
// does not own data -> is not included in migration mapping.
func mastersWithSlots(table []clusterNodeRow) []sourceMaster {
	var out []sourceMaster
	for _, row := range table {
		if !row.isMaster || len(row.slots) == 0 {
			continue
		}
		first := row.slots[0].from
		for _, r := range row.slots[1:] {
			if r.from < first {
				first = r.from
			}
		}
		out = append(out, sourceMaster{id: row.id, ipPort: row.ipPort, firstSlot: first})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].firstSlot != out[j].firstSlot {
			return out[i].firstSlot < out[j].firstSlot
		}
		return out[i].id < out[j].id // tie-break (ranges do not intersect - almost not needed)
	})
	return out
}

// alreadyReplicaOf - does the topology of the node itself carry a line with its ip:port, already
// tied as a replica to masterID (join idempotency). Fresh isolated
// node its string as master with empty master-id -> false.
func alreadyReplicaOf(table []clusterNodeRow, node clusterNode, masterID string) bool {
	row := findNodeRow(table, node)
	return row != nil && !row.isMaster && row.masterID == masterID
}

// parseSourceSeeds resolves the list of source_nodes (host:port lines of the old
// cluster) in clusterNode for connection + MEET. key = "source-<i>" (for messages).
func parseSourceSeeds(v *structpb.Value) ([]clusterNode, error) {
	raw := stringList(v)
	if len(raw) == 0 {
		return nil, fmt.Errorf("params.source_nodes: must be a non-empty list of host:port")
	}
	out := make([]clusterNode, 0, len(raw))
	for i, s := range raw {
		ip, port, err := splitIPPort(strings.TrimSpace(s))
		if err != nil {
			return nil, fmt.Errorf("params.source_nodes[%d] %q: %w", i, s, err)
		}
		out = append(out, clusterNode{
			key:  "source-" + strconv.Itoa(i),
			addr: net.JoinHostPort(ip, strconv.Itoa(port)),
			ip:   ip,
			port: port,
		})
	}
	return out, nil
}

// waitNodeKnows waits (limited retry, reuses gossip timeouts) until
// CLUSTER NODES of the node will begin to contain a line with masterID - the node has recognized the target
// old master and REPLICATE by his id will pass. Did not meet the limit -> error.
func waitNodeKnows(ctx context.Context, conn redisConn, masterID string) error {
	for attempt := 0; attempt < gossipPollAttempts; attempt++ {
		nodes, err := conn.Do(ctx, "CLUSTER", "NODES")
		if err != nil {
			return fmt.Errorf("CLUSTER NODES: %w", err)
		}
		if tableHasID(parseClusterNodesTable(nodes), masterID) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(gossipPollInterval):
		}
	}
	return fmt.Errorf("gossip did not converge: source master %s not visible after %d attempts", masterID, gossipPollAttempts)
}

// tableHasID - whether there is a row in the topology with this node-id (the node has recognized the master).
func tableHasID(table []clusterNodeRow, id string) bool {
	for _, row := range table {
		if row.id == id {
			return true
		}
	}
	return false
}

// mappingSummary - deterministic mapping summary (new-key -> old-master-id)
// for Output (no secrets: nodes and node-id keys, not addresses/passwords).
func mappingSummary(results []joinResult) string {
	parts := make([]string, 0, len(results))
	for _, r := range results {
		parts = append(parts, r.node.key+"->"+r.masterID)
	}
	return strings.Join(parts, ",")
}

// ============================ failover-takeover ==============================

// validateClusterFailoverTakeover - static failover-takeover checks: non-empty
// nodes-map (new nodes are replicas of old masters) with a resolvable endpoint on
// each node. Texts without password.
func validateClusterFailoverTakeover(f map[string]*structpb.Value) []string {
	nodes := nodeSpecs(f["nodes"])
	if len(nodes) == 0 {
		return []string{"params.nodes: must be a non-empty map (key -> {addr|ip+port}) of the NEW cluster nodes (replicas to promote)"}
	}
	return validateNodeSpecs(nodes)
}

// applyClusterFailoverTakeover will promote new nodes (replicas of old masters after
// join-external) to the master via GRACEFUL CLUSTER FAILOVER (day-2 migration step 2):
//
//  1. sync-gate BEFORE the first failover: on EVERY new node INFO replication
//     master_link_status == "up" (the replica has caught up with its old master). At least one
//     didn't catch up -> ERROR before any failover (early failover on not caught up
//     the replica loses an unwritten replication tail);
//  2. on each new node: idempotency (already master -> no-op), otherwise GRACEFUL
//     CLUSTER FAILOVER (no arguments: the master stops recording, waits for catch-up,
//     lossless) -> poll CLUSTER NODES of a node until it becomes master WITH slots.
//
// FAIL-CLOSED: graceful did not meet the limit -> ERROR. DO NOT escalate to
// FORCE/TAKEOVER - they will promote without the consent of the old master (split-brain + loss
// data, contradicts "safety first"). The operator clearly understands.
//
// The final Output carries the per-node status (promoted|already) + number
// promoted. The password is NOT included in the events (IB ADR-010).
func (m *RedisModule) applyClusterFailoverTakeover(ctx context.Context, stream grpc.ServerStreamingServer[pluginv1.ApplyEvent], params *structpb.Struct) error {
	f := params.GetFields()
	password := stringOrEmpty(f["password"])
	username := stringOrEmpty(f["username"])

	newNodes, err := parseClusterNodes(f["nodes"])
	if err != nil {
		return sendFailure(stream, redactError(err, password))
	}

	tlsP := parseTLS(f)
	connect := func(node clusterNode) (redisConn, error) {
		conn, err := m.openConn(ctx, connConfig{addr: node.addr, username: username, password: password, tls: tlsP})
		if err != nil {
			// TLS-handshake error theoretically carries PEM client-key - edit
			// it is RIGHT here (the password is edited by the caller separately in the text).
			return nil, fmt.Errorf("%s", redactError(err, tlsP.keyPEM))
		}
		return conn, nil
	}

	// SYNC-GATE: ALL new replicas must be synchronous (master_link_status==up)
	// BEFORE the first failover. If you promote the first shard on a catch-up replica, and
	// the second one is still catching up - his tail will be lost during his failover. Let's check everything at once
	// to any mutating CLUSTER FAILOVER. master nodes (already promoted,
	// re-apply) sync-gate is skipped: master does not have master_link_status.
	syncState := make([]bool, len(newNodes)) // true -> node is already master (failover is not needed)
	for i, node := range newNodes {
		isMaster, err := nodeSyncReady(ctx, connect, node)
		if err != nil {
			return sendFailure(stream, redactError(err, password))
		}
		syncState[i] = isMaster
	}

	// Everyone is synchronous (or already master) - let's promote. Idempotency: node is already master
	// -> no-op (we don't send failover).
	promoted := 0
	statuses := make([]string, len(newNodes))
	for i, node := range newNodes {
		if syncState[i] {
			statuses[i] = node.key + "=already"
			continue
		}
		if err := failoverNode(ctx, connect, node); err != nil {
			return sendFailure(stream, redactError(err, password))
		}
		statuses[i] = node.key + "=promoted"
		promoted++
	}

	return sendOutcome(stream, promoted > 0, fmt.Sprintf(
		"failover-takeover: %d/%d new nodes promoted to master (graceful)", promoted, len(newNodes)),
		map[string]any{
			"promoted": int64(promoted),
			"nodes":    int64(len(newNodes)),
			"per_node": strings.Join(statuses, ","),
		})
}

// nodeSyncReady checks that one new node is ready to failover BEFORE it starts.
// Returns (isMaster, error): isMaster=true - node ALREADY master (reapply,
// failover is not needed, sync-gate is not applicable). isMaster=false + nil - replica node,
// its link is healthy (master_link_status=="up"), failover can be launched. Replica with
// unhealthy link (or abnormal INFO) -> ERROR (fail before any failover).
func nodeSyncReady(ctx context.Context, connect func(clusterNode) (redisConn, error), node clusterNode) (bool, error) {
	conn, err := connect(node)
	if err != nil {
		return false, fmt.Errorf("connect new node %s: %w", node.addr, err)
	}
	defer func() { _ = conn.Close() }()

	info, err := conn.Do(ctx, "INFO", "replication")
	if err != nil {
		return false, fmt.Errorf("INFO replication on %s: %w", node.addr, err)
	}
	repl := parseInfoSection(info)
	if repl["role"] == "master" {
		return true, nil // already promoted (idempotency), sync-gate is not applicable
	}
	if repl["master_link_status"] != "up" {
		// The replica has not yet caught up with its old master. An early failover would have lost
		// replication tail - we refuse BEFORE the first failover (fail-closed).
		return false, fmt.Errorf(
			"new node %s not synced before failover: master_link_status=%q (want \"up\") - replica has not caught up, refusing to fail over",
			node.addr, repl["master_link_status"])
	}
	return false, nil
}

// failoverNode will promote one new node (synchronous replica) to master via
// GRACEFUL CLUSTER FAILOVER (WITHOUT arguments) and waits until the node actually becomes
// master WITH slots (CLUSTER NODES of the node itself). graceful: old master
// stops recording, gives the tail to the replica, which takes slots - lossless.
//
// FAIL-CLOSED: did not match gossipPollAttempts -> ERROR. We DO NOT send FORCE/TAKEOVER
// (promotion without master's consent = split-brain). The node is already master (suddenly
// moved between sync-gate and here) -> poll will converge immediately (no-op de-facto).
func failoverNode(ctx context.Context, connect func(clusterNode) (redisConn, error), node clusterNode) error {
	conn, err := connect(node)
	if err != nil {
		return fmt.Errorf("connect new node %s: %w", node.addr, err)
	}
	defer func() { _ = conn.Close() }()

	// GRACEFUL: without FORCE/TAKEOVER. Redis coordinates with the master (stop recording
	// + send the tail + change of era). On an already-master node, FAILOVER will return an error
	// "You should send CLUSTER FAILOVER to a replica" - but here we only get
	// for NON-master nodes (sync-gate above), so the path is standard.
	if _, err := conn.Do(ctx, "CLUSTER", "FAILOVER"); err != nil {
		return fmt.Errorf("CLUSTER FAILOVER on %s: %w", node.addr, err)
	}

	// We are waiting for graceful failover to complete: the node has become master and owns the slots
	// (only then did the promotion actually take place). Slots appear in line
	// the node itself is its own CLUSTER NODES after changing the role.
	if err := waitNodePromoted(ctx, conn, node); err != nil {
		return fmt.Errorf("new node %s: %w", node.addr, err)
	}
	return nil
}

// waitNodePromoted waits (limited retry, reuses gossip timeouts) until
// CLUSTER NODES of a node will show itself as a master WITH slots. Didn't meet the limit
// -> ERROR (FAIL-CLOSED: graceful failover did not complete, NO escalation to FORCE).
func waitNodePromoted(ctx context.Context, conn redisConn, node clusterNode) error {
	for attempt := 0; attempt < gossipPollAttempts; attempt++ {
		nodes, err := conn.Do(ctx, "CLUSTER", "NODES")
		if err != nil {
			return fmt.Errorf("CLUSTER NODES: %w", err)
		}
		if row := findNodeRow(parseClusterNodesTable(nodes), node); row != nil && row.isMaster && len(row.slots) > 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(gossipPollInterval):
		}
	}
	return fmt.Errorf("graceful CLUSTER FAILOVER did not complete after %d attempts (node not master with slots) - NOT escalating to FORCE/TAKEOVER (split-brain risk); resolve manually", gossipPollAttempts)
}

// ============================== forget-external ==============================

// validateClusterForgetExternal - static checks forget-external: non-empty
// nodes-map (new nodes executing FORGET) with a resolvable endpoint on each node,
// and non-empty source_nodes (old seeds,
// where do node-ids come from for forgetting). Texts without password.
func validateClusterForgetExternal(f map[string]*structpb.Value) []string {
	var errs []string
	nodes := nodeSpecs(f["nodes"])
	if len(nodes) == 0 {
		errs = append(errs, "params.nodes: must be a non-empty map (key -> {addr|ip+port}) of the NEW cluster nodes")
	}
	errs = append(errs, validateNodeSpecs(nodes)...)
	if len(stringList(f["source_nodes"])) == 0 {
		errs = append(errs, "params.source_nodes: must be a non-empty list of seed nodes (host:port) of the OLD cluster to forget")
	}
	return errs
}

// applyClusterForgetExternal throws old nodes out of the cluster via CLUSTER
// FORGET on each new node (day-2 migration step 3, after failover-takeover):
//
//  1. connection to source_nodes (old seeds, search in order) -> CLUSTER NODES ->
//     node-id of the OLD cluster (master AND replicas - throw them all out). New lines
//     nodes (by ip:port from nodes-map) FILTERED: post-join they are in the same topology,
//     their id -> self-forget;
//  2. on EACH new node: CLUSTER FORGET <old-id> for each old id.
//
// WITHOUT slot migration (unlike remove-node): new masters ALREADY have the slots
// after the failover-takeover, the old masters lost them. Idempotent: old id
// already unknown to the node -> FORGET will return "Unknown node", swallow (no-op); old in
// there is no seed topology left (everyone is forgotten) -> empty oldIDs, changed=false. All
// old seeds are not available (the cluster has already been extinguished, there is nowhere to get the id) -> ERROR with
// in clear text (we don't know that forgetting is not an idempotent way).
//
// The final Output carries the number of forgotten old nodes and the number of new nodes on which
// FORGET is executed. The password is NOT included in the events (IB ADR-010).
func (m *RedisModule) applyClusterForgetExternal(ctx context.Context, stream grpc.ServerStreamingServer[pluginv1.ApplyEvent], params *structpb.Struct) error {
	f := params.GetFields()
	password := stringOrEmpty(f["password"])
	username := stringOrEmpty(f["username"])

	newNodes, err := parseClusterNodes(f["nodes"])
	if err != nil {
		return sendFailure(stream, redactError(err, password))
	}
	sources, err := parseSourceSeeds(f["source_nodes"])
	if err != nil {
		return sendFailure(stream, redactError(err, password))
	}

	tlsP := parseTLS(f)
	connect := func(node clusterNode) (redisConn, error) {
		conn, err := m.openConn(ctx, connConfig{addr: node.addr, username: username, password: password, tls: tlsP})
		if err != nil {
			return nil, fmt.Errorf("%s", redactError(err, tlsP.keyPEM))
		}
		return conn, nil
	}

	// node-id of the old cluster with the first available source-seed (like join-external).
	// EXCLUDE new nodes: after join-external + failover-takeover new nodes -
	// members of the SAME cluster, and CLUSTER NODES of the old seed lists them too. Without
	// filter, their ids would end up in oldIDs -> the node would forget ITSELF (Redis: "I can't
	// forget myself" - this is NOT "unknown node", hard-fail). Filter - by ip:port from
	// nodes-map (more reliable than CLUSTER MYID on each node: one topology pass).
	oldIDs, seedEndpoint, err := sourceNodeIDs(ctx, connect, sources, newNodes)
	if err != nil {
		return sendFailure(stream, redactError(err, password))
	}

	// On EVERY new node FORGET all old ids. "Unknown node" (I have already forgotten the node
	// old) -> idempotency, swallow. We count the number of (node x old) pairs, where
	// FORGET really forgot something.
	forgotten := 0
	for _, node := range newNodes {
		n, err := forgetIDsOnNode(ctx, connect, node, oldIDs)
		if err != nil {
			return sendFailure(stream, redactError(err, password))
		}
		forgotten += n
	}

	return sendOutcome(stream, forgotten > 0, fmt.Sprintf(
		"forget-external: forgot %d old node(s) across %d new node(s)", len(oldIDs), len(newNodes)),
		map[string]any{
			"old_nodes":  int64(len(oldIDs)),
			"new_nodes":  int64(len(newNodes)),
			"forgotten":  int64(forgotten),
			"source_via": seedEndpoint,
		})
}

// sourceNodeIDs connects to source seeds in order, takes CLUSTER NODES from
// first to respond and returns the node-id of the OLD cluster (master AND replica -
// forget everyone), deterministically sorted. Returns also endpoint
// triggered seed (for Output). All seeds are unavailable -> error.
//
// Nodes whose ip:port matches newNodes ARE EXCLUDED: after join-external +
// failover-takeover new nodes are members of the same cluster and fall into CLUSTER NODES
// old seed; their id in oldIDs would result in self-forget ("can't forget myself").
//
// Difference from sourceMasters: it only takes masters with slots (for mapping
// replication), ALL old nodes are needed here (forget throws out the entire old cluster).
func sourceNodeIDs(ctx context.Context, connect func(clusterNode) (redisConn, error), seeds, newNodes []clusterNode) ([]string, string, error) {
	// Set ip:port of new nodes - discard their lines in the topology of the old seed.
	newEndpoints := make(map[string]struct{}, len(newNodes))
	for _, n := range newNodes {
		newEndpoints[net.JoinHostPort(n.ip, strconv.Itoa(n.port))] = struct{}{}
	}

	var lastErr error
	for _, seed := range seeds {
		conn, err := connect(seed)
		if err != nil {
			lastErr = fmt.Errorf("connect source seed %s: %w", seed.addr, err)
			continue
		}
		topology, err := conn.Do(ctx, "CLUSTER", "NODES")
		_ = conn.Close()
		if err != nil {
			lastErr = fmt.Errorf("CLUSTER NODES on source seed %s: %w", seed.addr, err)
			continue
		}

		rows := parseClusterNodesTable(topology)
		if len(rows) == 0 {
			// Broken/empty response seed (not a single line) - try the next seed.
			lastErr = fmt.Errorf("source seed %s: no nodes in CLUSTER NODES", seed.addr)
			continue
		}
		var ids []string
		for _, row := range rows {
			if row.id == "" {
				continue
			}
			if _, isNew := newEndpoints[row.ipPort]; isNew {
				continue // new node (already in the cluster) - don't forget (otherwise self-forget)
			}
			ids = append(ids, row.id)
		}
		// ids can be empty if all topology lines are new nodes (old ones are already
		// forgotten/disabled): there is nothing to forget - idempotent no-op (caller will return
		// changed=false), and NOT an error. We got here with a non-empty topology (broken
		// seed is filtered out above), so an empty ids is a steady-state, not a seed failure.
		sort.Strings(ids) // deterministic order FORGET (stable output/asserts)
		return ids, net.JoinHostPort(seed.ip, strconv.Itoa(seed.port)), nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("params.source_nodes: no reachable source seed")
	}
	return nil, "", lastErr
}

// forgetIDsOnNode executes CLUSTER FORGET <old-id> on one new node for each
// old id. Returns the number of actually forgotten ones. Two classes of errors are swallowed as
// idempotency: "Unknown node" (node has already forgotten this id - gossip-anti-entropy /
// apply again) and "can't forget myself" (the id turned out to be the node itself - defense-in-
// depth: oldIDs is already filtered by ip:port in sourceNodeIDs, but gossip could have added
// a new node in the seed topology AFTER the filter, the ip form could diverge - swallow it,
// so as not to fall on a safe no-op).
func forgetIDsOnNode(ctx context.Context, connect func(clusterNode) (redisConn, error), node clusterNode, oldIDs []string) (int, error) {
	conn, err := connect(node)
	if err != nil {
		return 0, fmt.Errorf("connect new node %s: %w", node.addr, err)
	}
	defer func() { _ = conn.Close() }()

	done := 0
	for _, id := range oldIDs {
		_, err := conn.Do(ctx, "CLUSTER", "FORGET", id)
		if err != nil {
			// Idempotency: the node has already forgotten the old one ("Unknown node") or the id is it
			// itself ("can't forget myself", post-join gossip race). No mistake, move on.
			if isUnknownNodeErr(err) || isCantForgetSelfErr(err) {
				continue
			}
			return done, fmt.Errorf("CLUSTER FORGET %s on %s: %w", id, node.addr, err)
		}
		done++
	}
	return done, nil
}

// isCantForgetSelfErr - CLUSTER FORGET by its own node-id -> Redis responds
// "ERR I tried hard but I can't forget myself...". Post-join new node is a member of that
// same cluster; sourceNodeIDs filters it out, but we swallow the gossip race here too.
func isCantForgetSelfErr(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "can't forget myself")
}
