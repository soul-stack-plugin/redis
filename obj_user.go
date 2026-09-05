// The `user` object — one ACL user of a live Redis, declared and reconciled
// (user.go).
//
// It is the object `acl` is not. `acl.reloaded` makes an instance re-read the
// aclfile a destiny rendered, so the subject there is the FILE and every edit runs
// through both halves; here the subject is the user, and ACL SETUSER / DELUSER
// reach it directly. The two coexist on purpose — a service still holding its ACL
// in a rendered file keeps `acl.reloaded`, and wiring this object into one is
// NIM-768, not this ticket.
package main

import "github.com/souls-guild/soul-stack/sdk/module"

// user binds the object's actions to the shared driver.
func (m *RedisModule) user() *object {
	return &object{
		impl:     m,
		name:     "user",
		decl:     userStates(),
		keyspace: true,
		actions: map[string]action{
			"present": {validate: validateUserPresent, apply: (*RedisModule).applyUserPresent},
			"absent":  {validate: validateUserAbsent, apply: (*RedisModule).applyUserAbsent},
		},
	}
}

// userDef is the object's entry in the artifact's bundle.
func userDef(m *RedisModule) module.Def {
	return module.Def{
		Name:         "user",
		Description:  "One ACL user of a live Redis: declared with its permissions and state, reconciled through ACL SETUSER.",
		Side:         module.SideSoul,
		Capabilities: []module.Capability{module.NetworkOutbound},
		SideEffects:  []module.SideEffect{{Service: "redis-server"}},
		Impl:         m.user(),
		States:       userStates(),
	}
}

// userStates declares the parameters of every action this object serves.
// It is lifted out of [userDef] because [object] reads it too: the declared
// type of a parameter is what Validate and Apply refuse a wrong-typed value
// against (NIM-778), and a second copy of it would be a second answer.
func userStates() map[string]module.State {
	return map[string]module.State{
		"present": {
			Description: "Reconcile ONE ACL user to the declared perms/state via ACL SETUSER (no\n" +
				"redis-cli/shell). The user is the subject: unlike acl.reloaded, nothing here\n" +
				"renders or re-reads a file, so the rest of the ACL is untouched.\n" +
				"\n" +
				"DECLARATIVE: the rule vector opens with `reset`, so the live user ends up with\n" +
				"exactly the declared perms — a permission dropped from the declaration is\n" +
				"dropped on the instance too. SETUSER alone MERGES, which would only ever\n" +
				"converge upward.\n" +
				"\n" +
				"Idempotent by construction; changed is a diff of this user's ACL LIST line\n" +
				"before/after (both sides are Redis's own rendering, so its normalization of the\n" +
				"rules cannot produce a false change). ACL LIST output does not end up in Output\n" +
				"(it carries password hashes). No dry-run preview.\n" +
				"\n" +
				"user_password is sent as `#<sha256hex>`, NEVER as plaintext: the same digest\n" +
				"users.acl carries, so this user and a rendered one are identical to Redis. An\n" +
				"OMITTED user_password keeps the credential clients already hold (the hashes are\n" +
				"carried over from the live line past the `reset`); on a user that does not exist\n" +
				"yet it creates one with no password, which cannot authenticate until you declare\n" +
				"one or put `nopass` in perms.",
			Input: module.Input{
				"addr": {Type: module.String, Required: true,
					Description: "Redis address: host:port (TCP) or a unix socket with the \"unix:\" prefix.",
				},
				"db": {Type: module.Int, Default: 0,
					Description: "DB number (SELECT) before ACL SETUSER. Inert — the ACL is server-wide — but declared because it is part of the shared connect path every scenario passes.",
				},
				"name": {Type: module.String, Required: true,
					Description: "ACL username to reconcile. Carries no whitespace and no NUL byte (Redis would read the name as truncated at a NUL and this module would not). This is the SUBJECT of the step, not the user to authenticate as (that is `username`).",
				},
				"password": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "Redis password for the CONNECTION (vault-ref in operator-input; keeper resolves it before Apply). Masked in logs/traces/UI (secret). Not passed in ACL command arguments (goes only into the connect).",
				},
				"perms": {Type: module.String, Required: true,
					Description: "The FULL ACL rule string for this user, Redis ACL directives verbatim (e.g. \"~app:* +@read +@write\"). Total, not additive: whatever is not here is not granted, because the apply resets the user first. A token carrying a credential VALUE (>pass, <pass, #hash, !hash) is REFUSED: perms is not a secret param, so its value reaches logs, traces, the UI and git in the clear — declare the password in user_password, which is masked and sent as a hash. Also refused, in ANY case (Redis matches ACL keywords case-insensitively): \"reset\" (the vector already opens with one, and a second discards the state and credentials set before it); \"on\"/\"off\" (that is params.state, and a token here would override it and make Output.state describe something the instance is not); and \"nopass\"/\"resetpass\" WHEN user_password is also declared (Redis takes the last directive, so the two contradict). \"nopass\"/\"resetpass\" alone stay allowed — they carry nothing secret and are how you declare a user that holds no password.",
				},
				"persist": {Type: module.Bool, Default: true,
					Description: "After a CHANGING SETUSER run ACL SAVE, so the user survives a restart and the next acl.reloaded (ACL LOAD re-reads the aclfile and would drop a memory-only user). Not run on a no-op — it would rewrite a rendered aclfile in Redis's own byte order and read as a change on every run. Set false on an instance with no aclfile directive, where ACL SAVE fails — including when the user already matches, since whether this run changes anything is only knowable after SETUSER, so the refusal comes first and a no-op is refused too. A non-boolean value is refused rather than coerced: the coercion would fall back on the side that writes to disk.",
				},
				"state": {Type: module.String, Default: "on",
					Description: "\"on\" or \"off\" — whether the user may authenticate. QUOTE IT in YAML: unquoted on/off parse as booleans (YAML 1.1) and are refused rather than coerced, because coercing `off` would silently land as the default `on`.",
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
				"user_password": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "Password OF THE MANAGED USER (vault-ref; keeper resolves it before Apply). Sent as `#<sha256hex>` — the plaintext never reaches the wire, the command arguments, events or errors. Omit to keep the credential the user already holds; on a new user, omitting it leaves them without one.",
				},
				"username": {Type: module.String,
					Description: "ACL username for AUTH — who this step authenticates AS (if not the default user). Not the user being managed, which is `name`.",
				},
			},
		},
		"absent": {
			Description: "Remove ONE ACL user via ACL DELUSER (no redis-cli/shell). The rest of the ACL\n" +
				"is untouched — nothing is re-rendered or re-read.\n" +
				"\n" +
				"Idempotent: a user the instance does not have is a no-op that sends no MUTATING\n" +
				"command (changed=false), so the aclfile is not rewritten either. It does READ:\n" +
				"the connection needs ACL LIST to find out, so a grant of DELUSER and SAVE alone\n" +
				"fails on every run. The built-in\n" +
				"`default` user is refused — by this module, in Apply as well as Validate, since\n" +
				"a runner need not call Validate.\n" +
				"\n" +
				"To disable it instead, use user.present with state \"off\" — but only once another\n" +
				"user can authenticate. On an instance whose only account is `default`, turning it\n" +
				"off succeeds, is saved to the aclfile, and every new connection then gets NOAUTH:\n" +
				"the step survives only because Redis does not drop the connection it is already\n" +
				"holding. Recovering means editing the file out of band. No dry-run preview.",
			Input: module.Input{
				"addr": {Type: module.String, Required: true,
					Description: "Redis address: host:port (TCP) or a unix socket with the \"unix:\" prefix.",
				},
				"db": {Type: module.Int, Default: 0,
					Description: "DB number (SELECT) before ACL DELUSER. Inert — the ACL is server-wide — but declared because it is part of the shared connect path every scenario passes.",
				},
				"name": {Type: module.String, Required: true,
					Description: "ACL username to remove. Carries no whitespace and no NUL byte. Not \"default\" — Redis refuses to remove the built-in user.",
				},
				"password": {Type: module.String, Secret: true, Pattern: "^vault:.*",
					Description: "Redis password for the CONNECTION (vault-ref in operator-input; keeper resolves it before Apply). Masked in logs/traces/UI (secret). Not passed in ACL command arguments (goes only into the connect).",
				},
				"persist": {Type: module.Bool, Default: true,
					Description: "After a removal that actually happened run ACL SAVE, so the user does not come back on a restart or the next acl.reloaded. Not run on the no-op. Set false on an instance with no aclfile directive, where ACL SAVE fails. A non-boolean value is refused rather than coerced: the coercion would fall back on the side that writes to disk.",
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
					Description: "ACL username for AUTH — who this step authenticates AS (if not the default user). Not the user being removed, which is `name`.",
				},
			},
		},
	}
}
