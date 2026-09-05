// The `acl` object — the access-control list of a live Redis instance.
//
// One action: `reloaded` makes the server re-read its aclfile in full (ACL LOAD),
// which the render has already written. Creating and removing individual ACL users
// through ACL SETUSER is the separate `user` object (obj_user.go, NIM-767) — this
// one deliberately declares no user-shaped params, and the two are not
// alternatives: the subject here is the file, there it is the user.
package main

import "github.com/souls-guild/soul-stack/sdk/module"

// acl binds the object's actions to the shared driver.
func (m *RedisModule) acl() *object {
	return &object{
		impl:     m,
		name:     "acl",
		decl:     aclStates(),
		keyspace: true,
		actions: map[string]action{
			"reloaded": {validate: validateProbe, apply: (*RedisModule).applyACL},
		},
	}
}

// aclDef is the object's entry in the artifact's bundle.
func aclDef(m *RedisModule) module.Def {
	return module.Def{
		Name:         "acl",
		Description:  "The access-control list of a live Redis instance, reconciled from its aclfile.",
		Side:         module.SideSoul,
		Capabilities: []module.Capability{module.NetworkOutbound},
		SideEffects:  []module.SideEffect{{Service: "redis-server"}},
		Impl:         m.acl(),
		States:       aclStates(),
	}
}

// aclStates declares the parameters of every action this object serves.
// It is lifted out of [aclDef] because [object] reads it too: the declared
// type of a parameter is what Validate and Apply refuse a wrong-typed value
// against (NIM-778), and a second copy of it would be a second answer.
func aclStates() map[string]module.State {
	return map[string]module.State{
		"reloaded": {
			Description: "Hot-reload the ACL of a live Redis via ACL LOAD (re-read the aclfile entirely).\n" +
				"users.acl is rendered by destiny BEFORE this step; acl only makes the instance\n" +
				"re-read the file. Idempotent by construction; changed is determined by a diff of\n" +
				"ACL LIST before/after LOAD (matched -> changed=false). ACL LIST output does not\n" +
				"end up in Output (may carry password hashes). No dry-run preview.",
			Input: module.Input{
				"addr": {Type: module.String, Required: true,
					Description: "Redis address: host:port (TCP) or a unix socket with the \"unix:\" prefix.",
				},
				"db": {Type: module.Int, Default: 0,
					Description: "DB number (SELECT) before ACL LOAD.",
				},
				"password": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "Redis password (vault-ref in operator-input; keeper resolves it before Apply). Masked in logs/traces/UI (secret). Not passed in ACL command arguments (goes only into the connect).",
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
