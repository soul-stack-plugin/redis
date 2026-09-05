// The `cluster` object — a Redis Cluster: built, grown, shrunk, resharded, and
// migrated off an older cluster (cluster.go and migrate.go).
//
// Every action here manages SEVERAL nodes, so none of them takes a single `addr`:
// each opens one connection per node from its own nodes-map, which is why they are
// wired through `applyNodes` rather than `apply` (object.go).
//
// These seven were one state carrying `params.action` until NIM-766. Splitting
// them onto address level 3 is what lets each declare only the params it reads:
// before, `created` and `resharded` shared one union of fifteen, so param
// strictness had no contract to hold either of them to.
package main

import "github.com/souls-guild/soul-stack/sdk/module"

// cluster binds the object's actions to the shared driver.
func (m *RedisModule) cluster() *object {
	return &object{
		impl:     m,
		name:     "cluster",
		decl:     clusterStates(),
		keyspace: false,
		actions: map[string]action{
			"created":            {validate: validateClusterCreate, applyNodes: (*RedisModule).applyClusterCreate},
			"node-added":         {validate: validateClusterAddNode, applyNodes: (*RedisModule).applyClusterAddNode},
			"node-removed":       {validate: validateClusterRemoveNode, applyNodes: (*RedisModule).applyClusterRemoveNode},
			"resharded":          {validate: validateClusterReshard, applyNodes: (*RedisModule).applyClusterReshard},
			"external-joined":    {validate: validateClusterJoinExternal, applyNodes: (*RedisModule).applyClusterJoinExternal},
			"failed-over":        {validate: validateClusterFailoverTakeover, applyNodes: (*RedisModule).applyClusterFailoverTakeover},
			"external-forgotten": {validate: validateClusterForgetExternal, applyNodes: (*RedisModule).applyClusterForgetExternal},
		},
	}
}

// clusterDef is the object's entry in the artifact's bundle.
func clusterDef(m *RedisModule) module.Def {
	return module.Def{
		Name:         "cluster",
		Description:  "A Redis Cluster: built, grown, shrunk, resharded, and migrated off an older cluster.",
		Side:         module.SideSoul,
		Capabilities: []module.Capability{module.NetworkOutbound},
		SideEffects:  []module.SideEffect{{Service: "redis-server"}},
		Impl:         m.cluster(),
		States:       clusterStates(),
	}
}

// clusterStates declares the parameters of every action this object serves.
// It is lifted out of [clusterDef] because [object] reads it too: the declared
// type of a parameter is what Validate and Apply refuse a wrong-typed value
// against (NIM-778), and a second copy of it would be a second answer.
func clusterStates() map[string]module.State {
	return map[string]module.State{
		"created": {
			Description: "Build a Redis cluster from scratch via go-redis, without redis-cli/shell:\n" +
				"MEET -> ADDSLOTS -> REPLICATE. The role layout and the 16384 slots are\n" +
				"deterministic (nodes keys sorted), so the same input yields the same topology.\n" +
				"Idempotent: a repeat apply on the formed cluster is a no-op (changed=false).\n" +
				"No dry-run preview (the plugin does not implement PlanReadSafe).",
			Input: module.Input{
				"nodes": {Type: module.Map,
					Description: "Nodes of the cluster being BUILT: a map of stable-key (SID/name) -> { addr: \"host:port\" } or { ip: \"10.0.0.1\", port: 6379 }. addr - for connecting, ip+port - for CLUSTER MEET (gossip operates on ip:port). Keys are SORTED -> they determine the master/replica layout.",
				},
				"password": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "Redis password (vault-ref in operator-input; keeper resolves it before Apply). Applied when connecting to EACH node. Masked in logs/traces/UI.",
				},
				"replicas_per_shard": {Type: module.Int, Default: 0,
					Description: "Replicas per shard. shards = len(nodes) / (1 + replicas_per_shard); len(nodes) must be divisible by the shard size. The first shards nodes (by key sort) are masters, the rest are round-robin replicas.",
				},
				"tls": {Type: module.Bool, Default: false,
					Description: "Connect to every cluster node over TLS. Required in only-TLS (port 0). Default false (plaintext).",
				},
				"tls_ca": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "PEM CA certificate to verify the node certificates (RootCAs). ONE CA for all nodes incl. source_nodes (a shared cluster PKI). Masked (secret).",
				},
				"tls_cert": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "PEM client certificate for mTLS (optional, only together with tls_key). Masked (secret).",
				},
				"tls_key": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "PEM client key for mTLS (optional, only together with tls_cert). Masked (secret); does not end up in events/errors.",
				},
				"tls_skip_verify": {Type: module.Bool, Default: false,
					Description: "EXPLICIT opt-out of node certificate verification. Default false (verification enabled - default secure).",
				},
				"topology": {Type: module.List,
					Description: "EXPLICIT shard layout, a list of shards, each a list of keys from nodes: [[master-sid, replica-sid, ...], ...]. Replaces the deterministic key-sort layout when the operator needs specific pairings (anti-affinity across racks/AZ). Every key of nodes must appear exactly once; the first entry of a shard is its master, the rest its replicas. With replicas_per_shard > 0 every shard must be exactly that size (1 master + replicas_per_shard); without it, shard sizes are free.",
				},
				"username": {Type: module.String,
					Description: "ACL username for AUTH (if not the default user).",
				},
			},
		},
		"external-forgotten": {
			Description: "Step 3 of the migration old cluster -> new: evict the old nodes (CLUSTER FORGET\n" +
				"of all old node-ids on each new node, without migrating slots - the slots are\n" +
				"already with the new masters). Idempotent: an Unknown node is a no-op.\n" +
				"No dry-run preview (the plugin does not implement PlanReadSafe).",
			Input: module.Input{
				"nodes": {Type: module.Map,
					Description: "The NEW nodes on which CLUSTER FORGET runs: a map of stable-key (SID/name) -> { addr: \"host:port\" } or { ip: \"10.0.0.1\", port: 6379 }. addr - for connecting, ip+port - for CLUSTER MEET (gossip operates on ip:port).",
				},
				"password": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "Redis password (vault-ref in operator-input; keeper resolves it before Apply). Applied when connecting to EACH node. Masked in logs/traces/UI. ONE password for the source-seed AND the new nodes - the shared cluster password (the operator aligns new == old before migration).",
				},
				"source_nodes": {Type: module.List,
					Description: "Seed nodes of the OLD cluster: a list of host:port. Tried in order - the first one that answers CLUSTER NODES determines the result. Its CLUSTER NODES gives ALL old node-ids (masters AND replicas), which are forgotten on the new nodes. The same password/TLS as the new nodes (shared cluster; the operator aligns them).",
				},
				"tls": {Type: module.Bool, Default: false,
					Description: "Connect to every cluster node over TLS. Required in only-TLS (port 0). Default false (plaintext).",
				},
				"tls_ca": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "PEM CA certificate to verify the node certificates (RootCAs). ONE CA for all nodes incl. source_nodes (a shared cluster PKI). Masked (secret).",
				},
				"tls_cert": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "PEM client certificate for mTLS (optional, only together with tls_key). Masked (secret).",
				},
				"tls_key": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "PEM client key for mTLS (optional, only together with tls_cert). Masked (secret); does not end up in events/errors.",
				},
				"tls_skip_verify": {Type: module.Bool, Default: false,
					Description: "EXPLICIT opt-out of node certificate verification. Default false (verification enabled - default secure).",
				},
				"username": {Type: module.String,
					Description: "ACL username for AUTH (if not the default user).",
				},
			},
		},
		"external-joined": {
			Description: "Step 1 of the migration old cluster -> new: merge NEW cluster-mode nodes into\n" +
				"the OLD cluster as replicas of the old masters 1:1 (MEET old-seed + REPLICATE;\n" +
				"the nodes<->masters mapping is by key / first slot). Fail-fast when the number\n" +
				"of old masters != shards_dest. Idempotent: a node already a replica of the\n" +
				"right master is a no-op.\n" +
				"No dry-run preview (the plugin does not implement PlanReadSafe).",
			Input: module.Input{
				"nodes": {Type: module.Map,
					Description: "The NEW nodes of the joining cluster: a map of stable-key (SID/name) -> { addr: \"host:port\" } or { ip: \"10.0.0.1\", port: 6379 }. addr - for connecting, ip+port - for CLUSTER MEET (gossip operates on ip:port). The 1:1 mapping to the old masters is by key.",
				},
				"password": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "Redis password (vault-ref in operator-input; keeper resolves it before Apply). Applied when connecting to EACH node. Masked in logs/traces/UI. ONE password for the source-seed AND the new nodes - the shared cluster password (the operator aligns new == old before migration).",
				},
				"shards_dest": {Type: module.Int,
					Description: "The expected number of destination shards (>= 1). Must match BOTH the number of new nodes (params.nodes) AND the number of masters in the old cluster - otherwise the 1:1 mapping is impossible (fail-fast). shards_source is only visible in the live source topology, so the equality assert runs in Apply (not at the render phase).",
				},
				"source_nodes": {Type: module.List,
					Description: "Seed nodes of the OLD cluster: a list of host:port. Tried in order - the first one that answers CLUSTER NODES determines the result. Its topology (masters + slots) determines the 1:1 mapping and serves as the MEET point for the new nodes. The same password/TLS as the new nodes (shared cluster; the operator aligns them).",
				},
				"tls": {Type: module.Bool, Default: false,
					Description: "Connect to every cluster node over TLS. Required in only-TLS (port 0). Default false (plaintext).",
				},
				"tls_ca": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "PEM CA certificate to verify the node certificates (RootCAs). ONE CA for all nodes incl. source_nodes (a shared cluster PKI). Masked (secret).",
				},
				"tls_cert": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "PEM client certificate for mTLS (optional, only together with tls_key). Masked (secret).",
				},
				"tls_key": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "PEM client key for mTLS (optional, only together with tls_cert). Masked (secret); does not end up in events/errors.",
				},
				"tls_skip_verify": {Type: module.Bool, Default: false,
					Description: "EXPLICIT opt-out of node certificate verification. Default false (verification enabled - default secure).",
				},
				"username": {Type: module.String,
					Description: "ACL username for AUTH (if not the default user).",
				},
			},
		},
		"failed-over": {
			Description: "Step 2 of the migration old cluster -> new: promote the new replicas to masters\n" +
				"via GRACEFUL CLUSTER FAILOVER. A sync-gate runs first - master_link_status==up\n" +
				"on ALL of them - and the step is fail-closed without escalation to\n" +
				"FORCE/TAKEOVER, which would risk split-brain. Idempotent: a node already master\n" +
				"is a no-op.\n" +
				"No dry-run preview (the plugin does not implement PlanReadSafe).",
			Input: module.Input{
				"nodes": {Type: module.Map,
					Description: "The NEW replica nodes being promoted to masters: a map of stable-key (SID/name) -> { addr: \"host:port\" } or { ip: \"10.0.0.1\", port: 6379 }. addr - for connecting, ip+port - for CLUSTER MEET (gossip operates on ip:port).",
				},
				"password": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "Redis password (vault-ref in operator-input; keeper resolves it before Apply). Applied when connecting to EACH node. Masked in logs/traces/UI.",
				},
				"tls": {Type: module.Bool, Default: false,
					Description: "Connect to every cluster node over TLS. Required in only-TLS (port 0). Default false (plaintext).",
				},
				"tls_ca": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "PEM CA certificate to verify the node certificates (RootCAs). ONE CA for all nodes incl. source_nodes (a shared cluster PKI). Masked (secret).",
				},
				"tls_cert": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "PEM client certificate for mTLS (optional, only together with tls_key). Masked (secret).",
				},
				"tls_key": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "PEM client key for mTLS (optional, only together with tls_cert). Masked (secret); does not end up in events/errors.",
				},
				"tls_skip_verify": {Type: module.Bool, Default: false,
					Description: "EXPLICIT opt-out of node certificate verification. Default false (verification enabled - default secure).",
				},
				"username": {Type: module.String,
					Description: "ACL username for AUTH (if not the default user).",
				},
			},
		},
		"node-added": {
			Description: "Join ONE node to an already formed cluster (day-2): MEET + REPLICATE for a\n" +
				"replica, or an empty master with no slots for role=master (a separate\n" +
				"resharded moves the slots to it). Idempotent: a node already in the cluster is\n" +
				"a no-op. No dry-run preview (the plugin does not implement PlanReadSafe).",
			Input: module.Input{
				"master": {Type: module.Map,
					Description: "(role=replica) The master the newcomer becomes a replica of: { addr: \"host:port\" } or { ip, port }. If unset, the plugin picks the master with the fewest replicas (balancing, like redis-cli without --cluster-master-id).",
				},
				"new_node": {Type: module.Map,
					Description: "The node being joined: { addr: \"host:port\" } or { ip, port }. addr - for connecting (REPLICATE runs on it), ip+port - for CLUSTER MEET.",
				},
				"password": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "Redis password (vault-ref in operator-input; keeper resolves it before Apply). Applied when connecting to EACH node. Masked in logs/traces/UI.",
				},
				"role": {Type: module.String, Default: "replica",
					Description: "Role of the new node: \"replica\" (CLUSTER REPLICATE to the master from params.master or to the least loaded one) or \"master\" (an empty master with no slots; a separate resharded moves the slots).",
				},
				"seed": {Type: module.Map,
					Description: "Any existing cluster node (the contact for MEET and the source of CLUSTER NODES): { addr: \"host:port\" } or { ip, port }.",
				},
				"tls": {Type: module.Bool, Default: false,
					Description: "Connect to every cluster node over TLS. Required in only-TLS (port 0). Default false (plaintext).",
				},
				"tls_ca": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "PEM CA certificate to verify the node certificates (RootCAs). ONE CA for all nodes incl. source_nodes (a shared cluster PKI). Masked (secret).",
				},
				"tls_cert": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "PEM client certificate for mTLS (optional, only together with tls_key). Masked (secret).",
				},
				"tls_key": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "PEM client key for mTLS (optional, only together with tls_cert). Masked (secret); does not end up in events/errors.",
				},
				"tls_skip_verify": {Type: module.Bool, Default: false,
					Description: "EXPLICIT opt-out of node certificate verification. Default false (verification enabled - default secure).",
				},
				"username": {Type: module.String,
					Description: "ACL username for AUTH (if not the default user).",
				},
			},
		},
		"node-removed": {
			Description: "Evict ONE node from the cluster (day-2). A master WITH slots has them migrated\n" +
				"to the remaining masters first, then CLUSTER FORGET runs everywhere; a replica\n" +
				"or a master without slots is just FORGET. Idempotent: a node already gone is a\n" +
				"no-op. No dry-run preview (the plugin does not implement PlanReadSafe).",
			Input: module.Input{
				"node": {Type: module.Map,
					Description: "The node being evicted: { addr: \"host:port\" } or { ip, port }. If it is a master WITH slots, the slots are migrated first to the remaining masters, then CLUSTER FORGET runs everywhere; a replica / a master without slots - just FORGET. Idempotent (node already gone -> no-op).",
				},
				"password": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "Redis password (vault-ref in operator-input; keeper resolves it before Apply). Applied when connecting to EACH node. Masked in logs/traces/UI.",
				},
				"seed": {Type: module.Map,
					Description: "Any existing cluster node (the source of CLUSTER NODES): { addr: \"host:port\" } or { ip, port }.",
				},
				"tls": {Type: module.Bool, Default: false,
					Description: "Connect to every cluster node over TLS. Required in only-TLS (port 0). Default false (plaintext).",
				},
				"tls_ca": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "PEM CA certificate to verify the node certificates (RootCAs). ONE CA for all nodes incl. source_nodes (a shared cluster PKI). Masked (secret).",
				},
				"tls_cert": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "PEM client certificate for mTLS (optional, only together with tls_key). Masked (secret).",
				},
				"tls_key": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "PEM client key for mTLS (optional, only together with tls_cert). Masked (secret); does not end up in events/errors.",
				},
				"tls_skip_verify": {Type: module.Bool, Default: false,
					Description: "EXPLICIT opt-out of node certificate verification. Default false (verification enabled - default secure).",
				},
				"username": {Type: module.String,
					Description: "ACL username for AUTH (if not the default user).",
				},
			},
		},
		"resharded": {
			Description: "Move N slots from master `from` to master `to` (SETSLOT/MIGRATE of the first N\n" +
				"source slots, mirroring redis-cli --cluster reshard).\n" +
				"star NOT IDEMPOTENT, deliberately: a repeat apply moves N more slots. This is\n" +
				"an imperative exec-style day-2 operation, NOT part of converge - created /\n" +
				"node-added / node-removed, in contrast, are idempotent.\n" +
				"No dry-run preview (the plugin does not implement PlanReadSafe).",
			Input: module.Input{
				"from": {Type: module.Map,
					Description: "The SOURCE master slots are taken from: { addr: \"host:port\" } or { ip, port }. Must be a master in the cluster and own >= slots slots.",
				},
				"password": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "Redis password (vault-ref in operator-input; keeper resolves it before Apply). Applied when connecting to EACH node. Masked in logs/traces/UI.",
				},
				"slots": {Type: module.Int,
					Description: "How many slots to move from from to to (>= 1). The FIRST slots slots of the source, ascending, are taken. star NOT idempotent: a repeat apply moves slots more slots.",
				},
				"tls": {Type: module.Bool, Default: false,
					Description: "Connect to every cluster node over TLS. Required in only-TLS (port 0). Default false (plaintext).",
				},
				"tls_ca": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "PEM CA certificate to verify the node certificates (RootCAs). ONE CA for all nodes incl. source_nodes (a shared cluster PKI). Masked (secret).",
				},
				"tls_cert": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "PEM client certificate for mTLS (optional, only together with tls_key). Masked (secret).",
				},
				"tls_key": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "PEM client key for mTLS (optional, only together with tls_cert). Masked (secret); does not end up in events/errors.",
				},
				"tls_skip_verify": {Type: module.Bool, Default: false,
					Description: "EXPLICIT opt-out of node certificate verification. Default false (verification enabled - default secure).",
				},
				"to": {Type: module.Map,
					Description: "The RECIPIENT master of the slots: { addr: \"host:port\" } or { ip, port }. Must be a master in the cluster and differ from from.",
				},
				"username": {Type: module.String,
					Description: "ACL username for AUTH (if not the default user).",
				},
			},
		},
	}
}
