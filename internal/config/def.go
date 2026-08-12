package config

import "github.com/souls-guild/soul-stack/sdk/module"

// Module is what this module offers, declared next to the code that offers it.
// `soul-mod stamp` turns it into the artifact's schema; there is no manifest to
// keep in step by hand.
//
// The name is address level 2. Level 1 is the alias the operator picks when the
// source is registered, so `config` becomes `redis.config.present` or
// `redis-community.config.present` from the same bytes — nothing here says which.
//
// `network_outbound` is the only capability: the module talks to a Redis server
// over the wire. Vault refs are resolved during keeper-render, before Apply, so
// the process never needs `vault_access`.
//
// The side effects are the running server's behaviour and — when `persist` is on
// — the server's own configuration file, which Redis rewrites under its own uid.
var Module = module.Def{
	Name:         "config",
	Description:  "Redis runtime configuration",
	Capabilities: []module.Capability{module.NetworkOutbound},
	SideEffects:  []module.SideEffect{{Service: "redis_server"}, {File: "redis_config_file"}},
	Impl:         &Config{},

	States: map[string]module.State{
		"present": {
			Description: "The named runtime parameters hold the requested values (CONFIG SET), optionally persisted with CONFIG REWRITE",
			Input: module.Input{
				"host": {
					Type: module.String, Required: true,
					Description: "Redis host to connect to",
				},
				"port": {Type: module.Int, Default: 6379},
				"db": {
					Type: module.Int, Default: 0,
					Description: "Database index for the admin connection. Configuration is server-wide; this only selects the connection's db",
				},
				"login_username": {
					Type: module.String,
					Description: "ACL user to authenticate as. Needs `config|get` and `config|set`; " +
						"`+@read` is NOT enough, and on a hardened server CONFIG is often " +
						"excluded from `+@admin` grants as well.",
				},
				"login_password": {
					Type: module.String, Secret: true, Pattern: `^vault:.*`,
					Description: "Password for login_username; MUST be a vault-ref (resolved keeper-side)",
				},
				"tls_enable":  {Type: module.Bool, Default: false},
				"tls_ca_path": {Type: module.String},
				"tls_skip_verify": {
					Type: module.Bool, Default: false,
					Description: "Do not verify the server certificate. For a broken lab, not for production",
				},
				"timeout_seconds": {Type: module.Int, Default: 10},
				"settings": {
					Type: module.Map, Required: true,
					Description: `Parameter name → desired value, e.g. {maxmemory: "100mb", ` +
						`maxmemory-policy: "allkeys-lru"}. Names are validated, not escaped. ` +
						"Values are compared against what the server reports, NOT against the " +
						"config file: this module converges the RUNTIME. Redis normalises what " +
						`it stores ("100mb" comes back as "104857600", "KEA" as "AKE", ` +
						`"EVERYSEC" as "everysec"), so the module re-reads after writing and ` +
						"decides `changed` from the before/after values the server itself " +
						"reported — a value that was already correct reports no change even " +
						"when it was spelled differently.",
				},
				"persist": {
					Type: module.Bool, Default: true,
					Description: "Run `CONFIG REWRITE` after a change, so it survives a restart. On " +
						"by default because `CONFIG SET` alone lives in memory only: without a " +
						"rewrite, `present` stops being true the moment Redis restarts. " +
						"THREE THINGS TO KNOW. A server started without a config file has " +
						"nowhere to write and the step FAILS rather than quietly reporting " +
						"success. The rewrite is only issued when this module changed " +
						"something — a value someone else set by hand without saving is not " +
						"noticed, because the module reads the runtime and never the file. " +
						"And Redis rewrites the file under its own uid, so the file's owner and " +
						"mode after the first rewrite are Redis' business, not this module's.",
				},
				"allow_connectivity_change": {
					Type: module.Bool, Default: false,
					Description: "Permit setting parameters that can cut the management path: port, " +
						"bind, unixsocket, unixsocketperm, tls-port, protected-mode, " +
						"requirepass, masterauth, maxclients, tls-cluster, tls-replication, " +
						"tls-auth-clients, tls-cert-file, tls-key-file, tls-ca-cert-file, " +
						"tls-ca-cert-dir. Refused by default. Verified live: `CONFIG SET port` " +
						"moves the listener and drops the connection that issued it, and " +
						"`protected-mode yes` makes every further command from a non-loopback " +
						"client fail with DENIED — including the one that would turn it back " +
						"off. `bind` is the exception Redis defends itself: an address it " +
						"cannot bind is rejected and the old one kept — but a bindable-and-wrong " +
						"address is not defended by anything, so it stays on the list.",
				},
				"require_atomic": {
					Type: module.Bool, Default: false,
					Description: "Fail instead of falling back to one-parameter-at-a-time writes. " +
						"Redis 7.0+ accepts several parameters in a single CONFIG SET and " +
						"applies them all or none; 6.2 rejects the multi-argument form " +
						"outright, so the module writes them one by one and a mid-way failure " +
						"leaves the earlier ones applied. Set this when a partial application " +
						"would be worse than no application at all.",
				},
			},
			Output: module.Output{
				"action": {
					Type:        module.String,
					Description: "altered | noop",
				},
				"changes": {
					Type:        module.String,
					Description: "Which parameters differed, as `name: before -> after`. Secret parameters show (redacted)",
				},
				"changed_parameters": {
					Type:        module.List,
					Description: "Names of the parameters whose value actually moved",
				},
				"persisted": {
					Type:        module.Bool,
					Description: "Whether CONFIG REWRITE ran. false means the change is in memory only and is lost on restart",
				},
				"atomic": {
					Type:        module.Bool,
					Description: "Whether the write went out as one all-or-nothing CONFIG SET",
				},
			},
		},

		"read": {
			Description: "Read runtime parameters into the register (a pure read; never reports a change)",
			Input: module.Input{
				"host":            {Type: module.String, Required: true},
				"port":            {Type: module.Int, Default: 6379},
				"db":              {Type: module.Int, Default: 0},
				"login_username":  {Type: module.String},
				"login_password":  {Type: module.String, Secret: true, Pattern: `^vault:.*`},
				"tls_enable":      {Type: module.Bool, Default: false},
				"tls_ca_path":     {Type: module.String},
				"tls_skip_verify": {Type: module.Bool, Default: false},
				"timeout_seconds": {Type: module.Int, Default: 10},
				"patterns": {
					Type: module.List,
					Description: `Glob patterns to read, e.g. ["maxmemory*", "appendonly"]. Defaults to ` +
						`["*"], which is 169 parameters on Redis 6.2 and 302 on Redis 8 — ask ` +
						"for what you need rather than for everything. Patterns are read one at " +
						"a time and merged, because Redis 6.2 rejects a multi-pattern CONFIG GET.",
				},
				"reveal_secrets": {
					Type: module.Bool, Default: false,
					Description: "Return the values of requirepass, masterauth, tls-key-file-pass and " +
						`tls-client-key-file-pass instead of "(redacted)". ` + "`CONFIG GET` hands " +
						"these out in the clear, and the register is not a secret store — the " +
						"value ends up in whatever reads the register afterwards. Off by default.",
				},
			},
			Output: module.Output{
				"config": {
					Type:        module.Map,
					Description: "Parameter name → value, as the server reports it",
				},
				"count": {
					Type:        module.Int,
					Description: "How many parameters were returned",
				},
				"redacted": {
					Type:        module.List,
					Description: "Names whose value was withheld (empty when reveal_secrets is true)",
				},
			},
		},
	},
}
