// The `sentinel` object — a live Redis Sentinel, reconciled to the masters it
// should monitor and the options it should hold (sentinel.go).
//
// It is the one object of this artifact WITHOUT a keyspace: a Sentinel is not a
// keyspace server and refuses SELECT, so go-redis — which issues SELECT for any DB
// > 0 on connect — could not open the connection at all (NIM-229). Its side effect
// is the `redis-sentinel` unit, not `redis-server`: with side effects declared per
// object, an operator approving this one sees what this one touches.
package main

import "github.com/souls-guild/soul-stack/sdk/module"

// sentinel binds the object's actions to the shared driver.
func (m *RedisModule) sentinel() *object {
	return &object{
		impl:     m,
		name:     "sentinel",
		decl:     sentinelStates(),
		keyspace: false,
		actions: map[string]action{
			"monitored": {validate: validateSentinel, apply: (*RedisModule).applySentinel},
		},
	}
}

// sentinelDef is the object's entry in the artifact's bundle.
func sentinelDef(m *RedisModule) module.Def {
	return module.Def{
		Name:         "sentinel",
		Description:  "A live Redis Sentinel: the masters it monitors and the options it holds.",
		Side:         module.SideSoul,
		Capabilities: []module.Capability{module.NetworkOutbound},
		SideEffects:  []module.SideEffect{{Service: "redis-sentinel"}},
		Impl:         m.sentinel(),
		States:       sentinelStates(),
	}
}

// sentinelStates declares the parameters of every action this object serves.
// It is lifted out of [sentinelDef] because [object] reads it too: the declared
// type of a parameter is what Validate and Apply refuse a wrong-typed value
// against (NIM-778), and a second copy of it would be a second answer.
func sentinelStates() map[string]module.State {
	return map[string]module.State{
		"monitored": {
			Description: "Reconcile Redis Sentinel via go-redis (no redis-cli/shell).\n" +
				"SENTINEL MONITOR/REMOVE (monitor) + SET (per-master) + CONFIG SET (globals).\n" +
				"The desired-state source is config (directives in the file form of sentinel.conf):\n" +
				"the plugin itself splits it into globals/per-master (Sentinel has no top-level CONFIG).\n" +
				"Idempotent (diff against SENTINEL MASTER/CONFIG GET). No dry-run preview.",
			Input: module.Input{
				"addr": {Type: module.String, Required: true,
					Description: "Address of the Sentinel instance host:port (usually 127.0.0.1:26379).",
				},
				"auth_pass": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "Sentinel's password for AUTH on the master (SENTINEL SET auth-pass). vault-ref; keeper resolves it before Apply. Masked - does NOT end up in events/logs.",
				},
				"auth_user": {Type: module.String,
					Description: "Sentinel's user for AUTH on the master (SENTINEL SET auth-user). Set when creating/recreating the monitor.",
				},
				"config": {Type: module.Map,
					Description: "Desired Sentinel directives in the FILE form (as in sentinel.conf): { \"sentinel down-after-milliseconds mymaster\": \"12000\", \"sentinel announce-ip\": \"10.0.0.1\", \"loglevel\": \"notice\" }. The plugin itself splits them into globals (SENTINEL CONFIG SET) and per-master (SENTINEL SET); startup-only (dir/port/tls-*) is ignored - those change via a restart.",
				},
				"db": {Type: module.Int, Default: 0,
					Description: "Must be 0 on this state, and refused otherwise (NIM-229). A Sentinel serves no keyspace and answers SELECT with an error, so a non-zero value does not misconfigure the connection - it makes it unopenable. Declared rather than dropped because the key is part of the shared connect path every scenario passes, and since NIM-204 an undeclared param fails the task.",
				},
				"master_name": {Type: module.String, Default: "mymaster",
					Description: "Logical name of the monitored master in Sentinel.",
				},
				"monitor": {Type: module.Map,
					Description: "Desired master address for monitor reconciliation: { ip, port, quorum }. ip - HOST-INVARIANT (one per cluster). Unset -> the monitor is not touched (only SET/CONFIG SET of parameters).",
				},
				"password": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "Password for connecting TO THE SENTINEL ITSELF (requirepass of the Sentinel instance), if set. vault-ref; keeper resolves it before Apply. Masked.",
				},
				"redis_version": {Type: module.String,
					Description: "Redis version for the version-gate of global parameters (loglevel available in Sentinel since 7.0). Unset -> version-gated parameters are dropped.",
				},
				"tls": {Type: module.Bool, Default: false,
					Description: "Connect to Sentinel over TLS. Required in only-TLS (port 0). Default false (plaintext).",
				},
				"tls_ca": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "PEM CA certificate to verify Sentinel's server certificate (RootCAs). Masked (secret).",
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
					Description: "ACL username for connecting to Sentinel (if not the default user).",
				},
			},
		},
	}
}
