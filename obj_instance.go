// The `instance` object — a live Redis server as a whole: is it answering, what
// role does it hold, and what is its running configuration.
//
// `pinged` and `role-probed` are read probes (changed=false by design, probe.go);
// `configured` reconciles directives through CONFIG SET (impl.go). The ACL of the
// same instance is a separate object (`acl`), because reloading it touches a file
// the render put there and not the server's own configuration.
package main

import "github.com/souls-guild/soul-stack/sdk/module"

// instance binds the object's actions to the shared driver. The table is the
// object's boundary: nothing else in this artifact is reachable through it.
func (m *RedisModule) instance() *object {
	return &object{
		impl:     m,
		name:     "instance",
		decl:     instanceStates(),
		keyspace: true,
		actions: map[string]action{
			"pinged":      {validate: validateProbe, apply: (*RedisModule).applyPinged},
			"role-probed": {validate: validateProbe, apply: (*RedisModule).applyRole},
			"configured":  {validate: validateConfig, apply: (*RedisModule).applyConfig},
		},
	}
}

// instanceDef is the object's entry in the artifact's bundle: what it declares to
// an operator, plus the implementation that serves it.
func instanceDef(m *RedisModule) module.Def {
	return module.Def{
		Name:         "instance",
		Description:  "A live Redis server: health and role probes, plus its running configuration.",
		Side:         module.SideSoul,
		Capabilities: []module.Capability{module.NetworkOutbound},
		SideEffects:  []module.SideEffect{{Service: "redis-server"}},
		Impl:         m.instance(),
		States:       instanceStates(),
	}
}

// instanceStates declares the parameters of every action this object serves.
// It is lifted out of [instanceDef] because [object] reads it too: the declared
// type of a parameter is what Validate and Apply refuse a wrong-typed value
// against (NIM-778), and a second copy of it would be a second answer.
func instanceStates() map[string]module.State {
	return map[string]module.State{
		"configured": {
			Description: "Apply a map of redis.conf directives via CONFIG SET to a live Redis.\n" +
				"Optionally CONFIG REWRITE (persist). Startup-only directives (port/dir/\n" +
				"aclfile/cluster-enabled/loadmodule/...) are skipped (CONFIG SET rejects them).\n" +
				"No dry-run preview.",
			Input: module.Input{
				"addr": {Type: module.String, Required: true,
					Description: "Redis address: host:port (TCP) or a unix socket with the \"unix:\" prefix.",
				},
				"config": {Type: module.Map, Required: true,
					Description: "redis.conf directives: { maxmemory: \"256mb\", maxmemory-policy: allkeys-lru, ... }. Each applied via CONFIG SET <key> <value>.",
				},
				"db": {Type: module.Int, Default: 0,
					Description: "DB number (SELECT) before CONFIG SET.",
				},
				"password": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "Redis password (vault-ref in operator-input; keeper resolves it before Apply). Masked in logs/traces/UI (secret).",
				},
				"rewrite": {Type: module.Bool, Default: false,
					Description: "After CONFIG SET run CONFIG REWRITE (persist into redis.conf). Default false (change stays runtime-only).",
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
		"pinged": {
			Description: "Health-probe Redis via go-redis PING (expects PONG). Read-only,\n" +
				"changed=false by design (probe, not a mutation). Replaces the idiom\n" +
				"command args:[PING]: Output.result == 'PONG' - the same field returned by\n" +
				"command, so register.self.result == 'PONG' in a health-gate\n" +
				"(retry/until/failed_when) works without changes. No dry-run preview.",
			Input: module.Input{
				"addr": {Type: module.String, Required: true,
					Description: "Redis address: host:port (TCP) or a unix socket with the \"unix:\" prefix.",
				},
				"db": {Type: module.Int, Default: 0,
					Description: "DB number (SELECT) before PING.",
				},
				"password": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "Redis password (vault-ref; keeper resolves it before Apply). Masked - does not end up in events/logs. Not passed in the PING arguments.",
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
		"role-probed": {
			Description: "Role-probe Redis via go-redis INFO replication. Read-only,\n" +
				"changed=false by design (probe). Output.role carries the actual\n" +
				"(volatile) instance role: \"master\" / \"slave\" - the same values returned\n" +
				"by shell `redis-cli role`. Used in where-targeting of\n" +
				"rolling-restart (register.self.role == 'master'/'slave'); the role is volatile\n" +
				"(ADR-008), taken by a live probe before targeting. No dry-run preview.",
			Input: module.Input{
				"addr": {Type: module.String, Required: true,
					Description: "Redis address: host:port (TCP) or a unix socket with the \"unix:\" prefix.",
				},
				"db": {Type: module.Int, Default: 0,
					Description: "DB number (SELECT) before INFO.",
				},
				"password": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "Redis password (vault-ref; keeper resolves it before Apply). Masked - does not end up in events/logs (INFO replication does not carry a role secret, but a connect error theoretically could - redactError).",
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
