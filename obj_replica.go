// The `replica` object — the replication link of one instance: whether it exists,
// whether it is torn down, and whether the data behind it has caught up.
//
// `present` binds the instance to a master (REPLICAOF, replica.go); `detached`
// tears the link down and promotes the instance to an autonomous master (REPLICAOF
// NO ONE, detach.go); `synced` and `offset-synced` are read probes over the link,
// the second one against an EXTERNAL source master (probe.go).
//
// The object HAS a keyspace: SELECT succeeds on all four, even though REPLICAOF
// and INFO replication are indifferent to which database is current.
package main

import "github.com/souls-guild/soul-stack/sdk/module"

// replica binds the object's actions to the shared driver.
func (m *RedisModule) replica() *object {
	return &object{
		impl:     m,
		name:     "replica",
		decl:     replicaStates(),
		keyspace: true,
		actions: map[string]action{
			"present":       {validate: validateReplica, apply: (*RedisModule).applyReplica},
			"detached":      {validate: validateProbe, apply: (*RedisModule).applyDetached},
			"synced":        {validate: validateProbe, apply: (*RedisModule).applyReplicaSynced},
			"offset-synced": {validate: validateOffsetSynced, apply: (*RedisModule).applyOffsetSynced},
		},
	}
}

// replicaDef is the object's entry in the artifact's bundle.
func replicaDef(m *RedisModule) module.Def {
	return module.Def{
		Name:         "replica",
		Description:  "The replication link of one Redis instance: bound to a master, detached, or caught up.",
		Side:         module.SideSoul,
		Capabilities: []module.Capability{module.NetworkOutbound},
		SideEffects:  []module.SideEffect{{Service: "redis-server"}},
		Impl:         m.replica(),
		States:       replicaStates(),
	}
}

// replicaStates declares the parameters of every action this object serves.
// It is lifted out of [replicaDef] because [object] reads it too: the declared
// type of a parameter is what Validate and Apply refuse a wrong-typed value
// against (NIM-778), and a second copy of it would be a second answer.
func replicaStates() map[string]module.State {
	return map[string]module.State{
		"detached": {
			Description: "Detach Redis from its master via REPLICAOF NO ONE (go-redis), promoting it to a\n" +
				"standalone master. Final step of the migration (after offset-synced).\n" +
				"Idempotent: already master (INFO replication role==master) -> changed=false,\n" +
				"no-op. Output.changed + Output.previous_master (former master host:port,\n" +
				"for audit). No dry-run preview (the plugin does not implement PlanReadSafe).",
			Input: module.Input{
				"addr": {Type: module.String, Required: true,
					Description: "Address of THIS instance: host:port (TCP) or \"unix:/path\".",
				},
				"db": {Type: module.Int, Default: 0,
					Description: "DB number (SELECT) before REPLICAOF NO ONE.",
				},
				"password": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "Redis password (vault-ref; keeper resolves it before Apply). Masked - does not end up in events/logs. Not passed in the REPLICAOF arguments.",
				},
				"tls": {Type: module.Bool, Default: false,
					Description: "Connect to Redis over TLS. Required in only-TLS (port 0). Default false (plaintext).",
				},
				"tls_ca": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "PEM CA certificate to verify the server certificate (RootCAs). Masked (secret).",
				},
				"tls_cert": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "PEM client certificate for mTLS (optional, only together with tls_key). Masked (secret).",
				},
				"tls_key": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "PEM client key for mTLS (optional, only together with tls_cert). Masked (secret); does not end up in events/errors.",
				},
				"tls_skip_verify": {Type: module.Bool, Default: false,
					Description: "EXPLICIT opt-out of server certificate verification. Default false (verification enabled - default secure).",
				},
				"username": {Type: module.String,
					Description: "ACL username for connecting (if not the default user).",
				},
			},
		},
		"offset-synced": {
			Description: "Migration safety-gate for a replica from an external source via go-redis: \"link\n" +
				"alive != data caught up\". Compares its own instance's slave_repl_offset with the\n" +
				"master_repl_offset of the external master (a second connect to source_addr).\n" +
				"Read-only, changed=false by design (probe). Output.caught_up (bool) -\n" +
				"the health-gate condition (until: register.self.caught_up == true): true ONLY\n" +
				"with master_link_status=up + master_sync_in_progress=0 + lag<=lag_threshold.\n" +
				"Output.lag_bytes / master_sync_in_progress - for diagnostics; when\n" +
				"!skip_checksum, optionally dbsize_source/dbsize_replica. No dry-run preview.",
			Input: module.Input{
				"addr": {Type: module.String, Required: true,
					Description: "Address of ITS OWN instance (the replica): host:port (TCP) or \"unix:/path\".",
				},
				"db": {Type: module.Int, Default: 0,
					Description: "DB number (SELECT) on ITS OWN instance before INFO replication.",
				},
				"lag_threshold": {Type: module.Int, Default: 0,
					Description: "Allowed lag (master_repl_offset - slave_repl_offset) in BYTES for caught_up=true. Default 0 (strict full catch-up).",
				},
				"password": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "Password of ITS OWN instance (vault-ref; keeper resolves it before Apply). Masked - does not end up in events/logs.",
				},
				"skip_checksum": {Type: module.Bool, Default: false,
					Description: "Skip the optional DBSIZE comparison of both instances. Default false (source and replica DBSIZE are placed into Output as an auxiliary signal; they do NOT affect caught_up - the offset is authoritative).",
				},
				"source_addr": {Type: module.String, Required: true,
					Description: "Address of the EXTERNAL master source host:port. Second connect (the source's master_repl_offset is the authoritative head for the lag calculation).",
				},
				"source_password": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "Password of the EXTERNAL source for the second connect (vault-ref; keeper resolves it before Apply). Masked - does not end up in events/logs.",
				},
				"source_tls": {Type: module.Bool, Default: false,
					Description: "Connect to the EXTERNAL source over TLS. Default false.",
				},
				"source_tls_ca": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "PEM CA to verify the EXTERNAL source's certificate (RootCAs). Masked (secret).",
				},
				"source_tls_cert": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "PEM client certificate for mTLS to the EXTERNAL source (optional, only together with source_tls_key). Masked (secret). Usually unset - the migration pilot expects only source_tls + source_tls_ca.",
				},
				"source_tls_key": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "PEM client key for mTLS to the EXTERNAL source (optional, only together with source_tls_cert). Masked (secret); does not end up in events/errors.",
				},
				"source_tls_skip_verify": {Type: module.Bool, Default: false,
					Description: "EXPLICIT opt-out of the EXTERNAL source's certificate verification. Default false (verification enabled - default secure).",
				},
				"tls": {Type: module.Bool, Default: false,
					Description: "Connect to ITS OWN instance over TLS. Default false.",
				},
				"tls_ca": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "PEM CA to verify ITS OWN instance's certificate (RootCAs). Masked.",
				},
				"tls_cert": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "PEM client certificate for mTLS to ITS OWN instance (optional, only together with tls_key). Masked (secret).",
				},
				"tls_key": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "PEM client key for mTLS to ITS OWN instance (optional, only together with tls_cert). Masked (secret); does not end up in events/errors.",
				},
				"tls_skip_verify": {Type: module.Bool, Default: false,
					Description: "EXPLICIT opt-out of ITS OWN instance's certificate verification. Default false (verification enabled - default secure).",
				},
				"username": {Type: module.String,
					Description: "ACL username for AUTH on ITS OWN instance (if not the default user). The source connect has no username of its own - source_password authenticates as the source's default user.",
				},
			},
		},
		"present": {
			Description: "Attach Redis to a master via REPLICAOF (go-redis, no redis-cli).\n" +
				"masterauth is set via CONFIG SET before REPLICAOF (the replica knows the master's password).\n" +
				"Idempotent (INFO replication -> already a replica of the right master = no-op).\n" +
				"addr == master_addr -> no-op (a master does not replicate itself, guard in the plugin).\n" +
				"No dry-run preview (the plugin does not implement PlanReadSafe).",
			Input: module.Input{
				"addr": {Type: module.String, Required: true,
					Description: "Address of THIS instance: host:port (TCP) or a unix socket (\"unix:/path\"). Local on the redis host (127.0.0.1:6379) - the plugin runs there too.",
				},
				"db": {Type: module.Int, Default: 0,
					Description: "DB number (SELECT) before REPLICAOF.",
				},
				"master_addr": {Type: module.String, Required: true,
					Description: "Address of the master host:port. HOST-INVARIANT (one per cluster) - scenario resolves it run_once (soulprint.hosts[0]) and passes it via apply.input. addr == master_addr -> the instance is the master, no-op.",
				},
				"master_password": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "(source_external) Password of the EXTERNAL master source (CONFIG SET masterauth). vault-ref; keeper resolves it before Apply. Masked. Empty -> masterauth is not set (source without requirepass).",
				},
				"master_tls": {Type: module.Bool, Default: false,
					Description: "(source_external) The source accepts the replica over TLS. true -> the plugin sets CONFIG SET tls-replication yes BEFORE REPLICAOF (the replica's outgoing replication link switches to TLS). star DEPENDENCY on render: the source's CA (master_tls_ca) is placed onto the replica's DISK and pointed to via CONFIG SET tls-ca-cert-file/-dir (instance.configured) BEFORE the replica.present step - without this, verifying the source's server-cert at handshake fails. The master_tls_ca value itself is NOT converted into a path by the plugin (Redis reads the CA by path, not inline; the plugin does not write files). Contract for scenario - README S3.",
				},
				"master_tls_ca": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "(source_external, master_tls) PEM CA of the external source to verify its server-cert on the replication link. Masked (secret). star The plugin does NOT apply this value directly: Redis reads the CA from disk by path (tls-ca-cert-file/-dir), so render writes the PEM to a file and points the path via instance.configured BEFORE replica.present (the plugin only enables tls-replication).",
				},
				"master_tls_cert": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "(source_external, master_tls) PEM client-cert of the replica for mTLS on the replication link to the source (optional, together with master_tls_key). Masked. star Applied as tls-cert-file (a path on disk) by the same render that handles the instance's server-cert; the plugin does not operate on its value.",
				},
				"master_tls_key": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "(source_external, master_tls) PEM client-key of the replica for mTLS on the replication link to the source (optional, together with master_tls_cert). Masked; does not end up in events. star Applied as tls-key-file (a path on disk) by render; the plugin does not operate on its value.",
				},
				"master_username": {Type: module.String,
					Description: "(source_external) ACL username of the external source for replication (CONFIG SET masteruser). Optional.",
				},
				"password": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "Redis password (vault-ref; keeper resolves it before Apply). Set as masterauth (CONFIG SET) before REPLICAOF. Masked. Empty -> masterauth is not set (master without requirepass).",
				},
				"source_external": {Type: module.Bool, Default: false,
					Description: "master_addr points at an EXTERNAL master (a different incarnation / migration), not a host of its own incarnation. true -> (1) the self-guard addr==master_addr is disabled; (2) masterauth is taken from master_password (NOT password); (3) masteruser - from master_username. Default false (master is its own).",
				},
				"tls": {Type: module.Bool, Default: false,
					Description: "Connect to THIS instance over TLS. Required in only-TLS (port 0). Default false (plaintext).",
				},
				"tls_ca": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "PEM CA certificate to verify THIS instance's certificate (RootCAs). Masked (secret).",
				},
				"tls_cert": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "PEM client certificate for mTLS (optional, only together with tls_key). Masked (secret).",
				},
				"tls_key": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "PEM client key for mTLS (optional, only together with tls_cert). Masked (secret); does not end up in events/errors.",
				},
				"tls_skip_verify": {Type: module.Bool, Default: false,
					Description: "EXPLICIT opt-out of server certificate verification. Default false (verification enabled - default secure).",
				},
				"username": {Type: module.String,
					Description: "ACL username for replicating ITS OWN incarnation (CONFIG SET masteruser). Optional. When source_external=true, masteruser is taken NOT from here but from master_username (external source credentials).",
				},
			},
		},
		"synced": {
			Description: "Replica restart health-gate via go-redis INFO replication: checks\n" +
				"master_link_status == \"up\" (the replica has caught up with the master after a restart).\n" +
				"Read-only, changed=false by design (probe). Output.synced (bool) -\n" +
				"the condition for the health-gate (until: register.self.synced == true);\n" +
				"Output.master_link_status (string) - for diagnostics. master_link_status\n" +
				"is present ONLY on a replica: on a master (field absent) synced=false with the\n" +
				"reason in Message - the state is applied on the slave path of rolling-restart.\n" +
				"No dry-run preview.",
			Input: module.Input{
				"addr": {Type: module.String, Required: true,
					Description: "Redis address: host:port (TCP) or a unix socket with the \"unix:\" prefix.",
				},
				"db": {Type: module.Int, Default: 0,
					Description: "DB number (SELECT) before INFO.",
				},
				"password": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "Redis password (vault-ref; keeper resolves it before Apply). Masked - does not end up in events/logs (INFO replication does not carry a secret, but a connect error theoretically could - redactError).",
				},
				"tls": {Type: module.Bool, Default: false,
					Description: "Connect to Redis over TLS. Required in only-TLS (port 0). Default false (plaintext).",
				},
				"tls_ca": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "PEM CA certificate to verify the server certificate (RootCAs). Masked (secret). Resolved keeper-side from Vault (render phase).",
				},
				"tls_cert": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "PEM client certificate for mTLS (optional, only together with tls_key). Masked (secret).",
				},
				"tls_key": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "PEM client key for mTLS (optional, only together with tls_cert). Masked (secret); does not end up in events/errors.",
				},
				"tls_skip_verify": {Type: module.Bool, Default: false,
					Description: "EXPLICIT opt-out of server certificate verification. Default false (verification enabled - default secure).",
				},
				"username": {Type: module.String,
					Description: "ACL username for AUTH (if not the default user).",
				},
			},
		},
	}
}
