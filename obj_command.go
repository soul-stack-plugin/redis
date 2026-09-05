// The `command` object — an arbitrary Redis command, run as given.
//
// Non-stateful, so level 3 takes the verb form (`redis.command.run`), exactly as
// `core.exec.run` and `core.cmd.shell` do. It takes ONE verb and will keep taking
// one: the discipline that removed `acl` / `role` / `offset-synced` from level 3
// says two operations are two objects, so a second thing to do to Redis becomes
// its own object rather than a second word here (ADR-020 amendment 2026-09-02).
//
// The name grants nothing. The Errand allow-list decides for a plugin by the
// `ErrandReadSafe` marker, which this artifact deliberately does not implement, so
// `command` is default-denied there like every other object of this artifact.
package main

import "github.com/souls-guild/soul-stack/sdk/module"

// command binds the object's single action to the shared driver.
func (m *RedisModule) command() *object {
	return &object{
		impl:     m,
		name:     "command",
		decl:     commandStates(),
		keyspace: true,
		actions: map[string]action{
			"run": {validate: validateCommand, apply: (*RedisModule).applyCommand},
		},
	}
}

// commandDef is the object's entry in the artifact's bundle.
func commandDef(m *RedisModule) module.Def {
	return module.Def{
		Name:         "command",
		Description:  "An arbitrary Redis command, run as given against one instance.",
		Side:         module.SideSoul,
		Capabilities: []module.Capability{module.NetworkOutbound},
		SideEffects:  []module.SideEffect{{Service: "redis-server"}},
		Impl:         m.command(),
		States:       commandStates(),
	}
}

// commandStates declares the parameters of every action this object serves.
// It is lifted out of [commandDef] because [object] reads it too: the declared
// type of a parameter is what Validate and Apply refuse a wrong-typed value
// against (NIM-778), and a second copy of it would be a second answer.
func commandStates() map[string]module.State {
	return map[string]module.State{
		"run": {
			Description: "Execute a raw command against Redis (imperative verb-action). By default\n" +
				"changed=false (like a probe); the operator is responsible for idempotency.\n" +
				"No dry-run preview (the plugin does not implement PlanReadSafe).\n" +
				"\n" +
				"WARNING (security): the command output is placed into Output.result IN PLAINTEXT - this\n" +
				"is Redis's response, not a plugin-managed secret, so it is NOT covered by the ADR-010\n" +
				"masks. DO NOT run read commands that return secrets through this action\n" +
				"(CONFIG GET requirepass / masterauth, ACL GETUSER <user>, ACL LIST):\n" +
				"their result will end up in RunResult/logs/OTel/UI in plaintext. For that -\n" +
				"use a specialized object, whose output declares which of its fields are\n" +
				"secret (instance.configured with a secret directive does not echo the value itself). params.password itself is masked\n" +
				"and does not end up in the command arguments (see the guard tests).",
			Input: module.Input{
				"addr": {Type: module.String, Required: true,
					Description: "Redis address: host:port (TCP) or a unix socket with the \"unix:\" prefix (e.g. unix:/var/run/redis/redis-server.sock).",
				},
				"args": {Type: module.List, Required: true,
					Description: "The command as an array of arguments (no shell), e.g. [\"CONFIG\", \"SET\", \"maxmemory\", \"256mb\"]. The first element is the verb.",
				},
				"changed": {Type: module.Bool, Default: false,
					Description: "Mark the result as changed=true (for commands that actually mutate state). Default false (probe semantics).",
				},
				"db": {Type: module.Int, Default: 0,
					Description: "DB number (SELECT) before the command.",
				},
				"password": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "Redis password (requirepass / ACL). vault-ref in operator-input; keeper resolves it before Apply, the plugin gets plaintext (ADR-012). Masked in logs/traces/UI (secret).",
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
