package acl

import "github.com/souls-guild/soul-stack/sdk/module"

// Module is what this module offers, declared next to the code that offers it.
// `soul-mod stamp` turns it into the artifact's schema; there is no manifest to
// keep in step by hand.
//
// The name is address level 2. Level 1 is the alias the operator picks when the
// source is registered, so `acl` becomes `redis.acl.present` or
// `redis-community.acl.present` from the same bytes — nothing here says which.
//
// `network_outbound` is the only capability: the module talks to a Redis server
// over the wire. Vault refs are resolved during keeper-render, before Apply, so
// the process never needs `vault_access`.
var Module = module.Def{
	Name:         "acl",
	Description:  "Redis ACL users",
	Capabilities: []module.Capability{module.NetworkOutbound},
	SideEffects:  []module.SideEffect{{User: "redis_acl_user"}},
	Impl:         &ACL{},

	States: map[string]module.State{
		"present": {
			Description: "The ACL user exists, enabled as requested, with the given password and rules",
			Input: module.Input{
				"host": {
					Type: module.String, Required: true,
					Description: "Redis host to connect to",
				},
				"port": {Type: module.Int, Default: 6379},
				"db": {
					Type: module.Int, Default: 0,
					Description: "Database index for the admin connection. ACLs are server-wide; this only selects the connection's db",
				},
				"login_username": {
					Type: module.String,
					Description: "ACL user to authenticate as (not the user being managed). Goes together " +
						"with login_password. Needs ACL access — `+@read` is NOT enough, the " +
						"connection must be allowed to run `acl|getuser` and `acl|setuser`.",
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
				"username": {
					Type: module.String, Required: true, Pattern: `^[A-Za-z0-9_.:-]{1,256}$`,
					Description: "ACL user to manage. Validated rather than escaped: it reaches Redis as a bare argument",
				},
				"enabled": {
					Type: module.Bool, Default: true,
					Description: "true → `on`, false → `off`. An `off` user cannot authenticate at all",
				},
				"password": {
					Type: module.String, Secret: true, Pattern: `^vault:.*`,
					Description: "The managed user's password; MUST be a vault-ref. It is COMPARED against " +
						"the SHA-256 hashes in `ACL GETUSER`, so an unchanged password reports no " +
						"change and is not rewritten. Mutually exclusive with password_hash and nopass. " +
						"Say nothing about the credential at all and the existing one is kept as it " +
						"is — on a user that does not exist yet there is nothing to keep, so a new " +
						"enabled user has to be given one of the three.",
				},
				"password_hash": {
					Type: module.String, Pattern: `^[0-9a-fA-F]{64}$`,
					Description: "The password as a SHA-256 hex digest, applied as Redis' own `#<hash>` " +
						"token. Use this when the plaintext should never reach the module at all. " +
						"Mutually exclusive with password and nopass.",
				},
				"nopass": {
					Type: module.Bool, Default: false,
					Description: "Accept any password for this user. Mutually exclusive with password and password_hash",
				},
				"rules": {
					Type: module.List,
					Description: `ACL rule tokens in the order Redis should apply them, exactly as they ` +
						`would be typed after ` + "`ACL SETUSER <user>`" + `: ["~cache:*", "&events:*", ` +
						`"+@read", "-DEBUG"]. ORDER IS PART OF THE MEANING — Redis applies rules ` +
						"left to right, so `+@all -@admin` and `-@admin +@all` grant different " +
						"things. The list is never reordered.",
				},
				"persist": {
					Type: module.Bool, Default: true,
					Description: "Run `ACL SAVE` after a change, so the user survives a restart. This is on " +
						"by default because `ACL SETUSER` alone lives in memory only: without a save, " +
						"`present` stops being true the moment Redis restarts. " +
						"TWO THINGS TO KNOW. It needs an `aclfile` directive on the server — without " +
						"one Redis has nowhere to write and the step FAILS rather than quietly " +
						"reporting success. And `ACL SAVE` rewrites that file IN FULL, including " +
						"users this module does not manage, so do not point it at a file something " +
						"else renders from a template. Set false to accept an in-memory-only change.",
				},
				"allow_self_modification": {
					Type: module.Bool, Default: false,
					Description: "Permit managing the same user this connection authenticated as. Refused " +
						"by default: the write starts with `reset`, so a narrower rule set takes " +
						"the connection's own permissions away mid-run.",
				},
				"allow_default_user": {
					Type: module.Bool, Default: false,
					Description: "Permit managing the built-in `default` user. Refused by default: " +
						"`default off` instantly locks out every unauthenticated client, and with " +
						"persist on that lockout survives a restart.",
				},
			},
			Output: module.Output{
				"action": {
					Type:        module.String,
					Description: "created | altered | noop",
				},
				"username": {Type: module.String},
				"changes": {
					Type:        module.String,
					Description: "What actually differed, for an operator reading the log",
				},
				"persisted": {
					Type:        module.Bool,
					Description: "Whether ACL SAVE ran. false means the change is in memory only and is lost on restart",
				},
			},
		},

		"absent": {
			Description: "The ACL user is absent (ACL DELUSER when present)",
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
				"username": {
					Type: module.String, Required: true, Pattern: `^[A-Za-z0-9_.:-]{1,256}$`,
					Description: "User to delete. Deleting the user named in login_username is refused — " +
						"Redis allows it, and it would take the credentials away from the next " +
						"run. `default` cannot be deleted at all (Redis refuses).",
				},
				"persist": {
					Type:        module.Bool,
					Default:     true,
					Description: "Run `ACL SAVE` after the deletion; see the note on the present state",
				},
			},
			Output: module.Output{
				"action": {
					Type:        module.String,
					Description: "dropped | noop",
				},
				"username":  {Type: module.String},
				"persisted": {Type: module.Bool},
			},
		},
	},
}
