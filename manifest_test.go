// Guard on the schema document ↔ implementation contract (NIM-206, NIM-525, NIM-766).
//
// `modules[<object>].states.<action>.input` is the ONLY thing param-level strictness
// reads (ADR-0076, NIM-163): a key a state omits has no declaration, so since
// NIM-204 enforced strictness for plugins too (ADR-0076(t)) a legitimate call
// carrying it FAILS with module.unknown_param. A prose promise in a comment is
// not a declaration — four states used to carry that promise and declare nothing,
// which is what this file exists to prevent from coming back.
//
// Since NIM-525 the document is GENERATED from the Go value (`redisBundle`), not
// written beside it: `soul-mod stamp` runs the artifact's `schema` subcommand and
// writes those bytes both into the binary and to `schema.json`, and
// TestPublishedSchemaMatchesTheBundle below is the local half of that guard.
// The document carries no `namespace:`/`name:` of its own: this artifact serves
// seven objects, and address level 1 (`redis` in `redis.instance.pinged`) comes
// from the alias an operator registers it under, which appears nowhere in these
// bytes.
//
// The three halves are checked together on purpose. TestConnectParams* proves the
// key lists below are the ones the Go parse path actually reads (a rename in
// tls.go/helpers.go breaks it); TestManifestStatesDeclareWhatTheyAccept proves
// every state declares exactly those plus its own params; and
// TestDeclaredStatesAreDispatched proves the object that SERVES a state is the one
// that DECLARES it. Split across two packages the set would only agree by convention.
package main

import (
	"bytes"
	"os"
	"sort"
	"testing"

	"github.com/souls-guild/soul-stack/sdk/schema"
	"google.golang.org/protobuf/types/known/structpb"
)

// connectParams — read by parseConnConfig (which calls parseTLS) for EVERY state
// dispatched through the shared connect path in [object.Apply], i.e. all but the
// cluster object's.
var connectParams = []string{
	"addr", "username", "password", "db",
	"tls", "tls_ca", "tls_cert", "tls_key", "tls_skip_verify",
}

// clusterConnectParams — a cluster action has no single instance: it builds a
// connConfig per node from its nodes-map, so it reads the same auth+TLS keys but
// neither addr nor db.
var clusterConnectParams = []string{
	"username", "password",
	"tls", "tls_ca", "tls_cert", "tls_key", "tls_skip_verify",
}

// sourceConnectParams — replica.offset-synced opens a SECOND connection to the
// external master with credentials and a PKI of its own (parseSourceTLS).
var sourceConnectParams = []string{
	"source_addr", "source_password",
	"source_tls", "source_tls_ca", "source_tls_cert", "source_tls_key", "source_tls_skip_verify",
}

// secretParams — params carrying a password or PEM. Declaring one without
// `secret: true` would leave it unmasked in logs/traces/UI (ADR-010).
var secretParams = map[string]bool{
	"password": true, "source_password": true, "master_password": true, "auth_pass": true,
	"user_password": true,
	"tls_ca":        true, "tls_cert": true, "tls_key": true,
	"source_tls_ca": true, "source_tls_cert": true, "source_tls_key": true,
	"master_tls_ca": true, "master_tls_cert": true, "master_tls_key": true,
}

// objects — the seven objects this artifact serves, paired with their dispatch
// tables. Address level 2 in `redis.<object>.<action>`.
func objects(m *RedisModule) map[string]*object {
	return map[string]*object{
		"acl":      m.acl(),
		"cluster":  m.cluster(),
		"command":  m.command(),
		"instance": m.instance(),
		"replica":  m.replica(),
		"sentinel": m.sentinel(),
		"user":     m.user(),
	}
}

// loadDocument reads the PUBLISHED schema document and runs the SDK validator: a
// document keeper would reject at `plugin.allow` must not pass here either.
func loadDocument(t *testing.T) schema.Document {
	t.Helper()
	raw, err := os.ReadFile(schema.SchemaFileName)
	if err != nil {
		t.Fatalf("read %s: %v", schema.SchemaFileName, err)
	}
	doc, err := schema.Unmarshal(raw)
	if err != nil {
		t.Fatalf("parse %s: %v", schema.SchemaFileName, err)
	}
	for _, i := range schema.Validate(doc) {
		if i.Level == schema.LevelError {
			t.Errorf("%s is invalid: %s at %s", schema.SchemaFileName, i, i.Path)
		}
	}
	// The bytes are canonical (sorted keys, no insignificant whitespace) because
	// they are hashed and signed — a hand edit that reformats them is a finding.
	canonical, err := schema.IsCanonical(raw)
	if err != nil || !canonical {
		t.Errorf("%s is not canonical (%v) — regenerate it, do not hand-edit", schema.SchemaFileName, err)
	}
	return doc
}

// loadModule returns one object's declaration from the published document.
func loadModule(t *testing.T, name string) schema.Module {
	t.Helper()
	doc := loadDocument(t)
	mod, ok := doc.Module(name)
	if !ok {
		t.Fatalf("%s declares no module %q, only %v", schema.SchemaFileName, name, doc.ModuleNames())
	}
	if len(mod.States) == 0 {
		t.Fatalf("module %q declares no states", name)
	}
	return mod
}

// TestPublishedSchemaMatchesTheBundle — `schema.json` is what `redisBundle`
// renders, byte for byte. It is GENERATED (`soul-mod stamp`, which runs the
// artifact's own `schema` subcommand), and the moment someone edits a Def without
// re-stamping, everything downstream — soul-lint, `plugin.allow`, the module form —
// is reading a contract the binary no longer implements (NIM-525).
func TestPublishedSchemaMatchesTheBundle(t *testing.T) {
	fromCode, err := redisBundle(&RedisModule{}).Schema()
	if err != nil {
		t.Fatalf("render the bundle: %v", err)
	}
	published, err := os.ReadFile(schema.SchemaFileName)
	if err != nil {
		t.Fatalf("read %s: %v", schema.SchemaFileName, err)
	}
	if !bytes.Equal(published, fromCode) {
		t.Fatalf("%s disagrees with the bundle — re-run `soul-mod stamp dist/redis`\n"+
			"  published: %d bytes\n  code:      %d bytes", schema.SchemaFileName, len(published), len(fromCode))
	}
}

// TestBundleIsValid — the same rules keeper applies at approval time, applied at
// build time. Impl included: a Def with no implementation would serve nothing.
func TestBundleIsValid(t *testing.T) {
	for _, i := range redisBundle(&RedisModule{}).Validate() {
		if i.Level == schema.LevelError {
			t.Errorf("bundle is invalid: %s at %s", i, i.Path)
		}
	}
}

// TestDeclaredStatesAreDispatched — the object that DECLARES a state is the one
// that SERVES it, in both directions. This is the guard the object split needs
// (NIM-766): seven objects share one driver, so a state declared on `instance` and
// dispatched only by `cluster` would lint clean, pass every param check, and fail
// at apply time with "unknown state" on a live host.
func TestDeclaredStatesAreDispatched(t *testing.T) {
	doc := loadDocument(t)
	served := objects(&RedisModule{})

	if len(doc.Modules) != len(served) {
		t.Fatalf("the document declares %d modules, the artifact serves %d (%v)",
			len(doc.Modules), len(served), doc.ModuleNames())
	}

	for _, mod := range doc.Modules {
		obj, ok := served[mod.Name]
		if !ok {
			t.Errorf("module %q is declared but no object serves it", mod.Name)
			continue
		}
		declared := make([]string, 0, len(mod.States))
		for state := range mod.States {
			declared = append(declared, state)
		}
		sort.Strings(declared)

		for _, missing := range diff(declared, obj.states()) {
			t.Errorf("%s.%s is declared but nothing dispatches it", mod.Name, missing)
		}
		for _, extra := range diff(obj.states(), declared) {
			t.Errorf("%s.%s is dispatched but not declared — strictness has no contract for it", mod.Name, extra)
		}
	}
}

// with returns base plus extra as a fresh slice — the shared *Params vars must
// never be appended into.
func with(base []string, extra ...string) []string {
	out := make([]string, 0, len(base)+len(extra))
	out = append(out, base...)
	return append(out, extra...)
}

// TestManifestStatesDeclareWhatTheyAccept — every state declares EXACTLY the keys
// its implementation reads: the shared connect set (or the cluster variant) plus
// its own. Both directions matter — a missing key is the NIM-206 hole, an extra
// one promises an operator a param nothing reads.
//
// The cluster rows are what the object split bought (NIM-766): the seven used to
// share ONE declared input of fifteen params, so `created` promised `slots` and
// `resharded` promised `topology` and neither promise was checkable.
func TestManifestStatesDeclareWhatTheyAccept(t *testing.T) {
	want := map[string]map[string][]string{
		"command": {
			"run": with(connectParams, "args", "changed"),
		},
		"instance": {
			"pinged":      with(connectParams),
			"role-probed": with(connectParams),
			"configured":  with(connectParams, "config", "rewrite"),
		},
		"acl": {
			// acl.reloaded reconciles a LIVE instance to the already rendered
			// aclfile with the ACL LOAD command — no params except the connection
			// (addr + optional auth/TLS).
			"reloaded": with(connectParams),
		},
		"user": {
			// user.present is the object acl.reloaded is not: the subject is ONE
			// user, so it declares the user-shaped params `acl` deliberately
			// refuses (NIM-767). `name` is who is managed, `username` (in
			// connectParams) is who the step authenticates as — two different
			// people, which is exactly why both are declared here.
			"present": with(connectParams, "name", "perms", "state", "user_password", "persist"),
			"absent":  with(connectParams, "name", "persist"),
		},
		"replica": {
			// master_tls_ca/cert/key are declared but NOT read by the plugin: Redis
			// reads the replication link's PEMs from DISK by path, so render places
			// them and the plugin only flips tls-replication.
			//
			// Recorded as a decision, not a leftover (NIM-229). Expressing
			// "consumed by render" in the manifest is not implementable: parsing is
			// yaml.Strict() and discovery skips a slot on a decode error, so a new
			// key would make an older Soul lose the whole module rather than read
			// the annotation - the same wall NIM-204 hit looking for an opt-in
			// flag. Moving them out of the module's params would fail every
			// scenario that passes them, since NIM-204 made a plugin manifest gate
			// its input. So they stay declared, masked, and documented as
			// render-consumed in docs/module/redis/README.md. This entry is the
			// roster that keeps the exception from spreading in silence.
			"present": with(connectParams,
				"master_addr", "source_external", "master_password", "master_username",
				"master_tls", "master_tls_ca", "master_tls_cert", "master_tls_key"),
			"detached": with(connectParams),
			"synced":   with(connectParams),
			"offset-synced": with(connectParams,
				with(sourceConnectParams, "lag_threshold", "skip_checksum")...),
		},
		"sentinel": {
			"monitored": with(connectParams,
				"master_name", "monitor", "config", "auth_user", "auth_pass", "redis_version"),
		},
		"cluster": {
			"created":            with(clusterConnectParams, "nodes", "replicas_per_shard", "topology"),
			"node-added":         with(clusterConnectParams, "new_node", "seed", "role", "master"),
			"node-removed":       with(clusterConnectParams, "node", "seed"),
			"resharded":          with(clusterConnectParams, "from", "to", "slots"),
			"external-joined":    with(clusterConnectParams, "nodes", "source_nodes", "shards_dest"),
			"failed-over":        with(clusterConnectParams, "nodes"),
			"external-forgotten": with(clusterConnectParams, "nodes", "source_nodes"),
		},
	}

	for objName, states := range want {
		t.Run(objName, func(t *testing.T) {
			mod := loadModule(t, objName)
			if len(mod.States) != len(states) {
				t.Fatalf("module %q declares %d states, table covers %d — a new state needs a row here",
					objName, len(mod.States), len(states))
			}

			for state, wantKeys := range states {
				t.Run(state, func(t *testing.T) {
					def, ok := mod.States[state]
					if !ok {
						t.Fatalf("module %q has no state %q", objName, state)
					}
					got := make([]string, 0, len(def.Input))
					for name := range def.Input {
						got = append(got, name)
					}
					sort.Strings(got)
					sorted := append([]string(nil), wantKeys...)
					sort.Strings(sorted)

					for _, missing := range diff(sorted, got) {
						t.Errorf("param %q is read but NOT declared — strictness has no contract for it (NIM-206)", missing)
					}
					for _, extra := range diff(got, sorted) {
						t.Errorf("param %q is declared but nothing reads it", extra)
					}
				})
			}
		})
	}
}

// TestManifestSecretParamsAreMasked — a password/PEM param must be declared
// secret with the vault-ref pattern, or it reaches logs/traces/UI in the clear.
func TestManifestSecretParamsAreMasked(t *testing.T) {
	for _, mod := range loadDocument(t).Modules {
		for state, def := range mod.States {
			for name, p := range def.Input {
				if !secretParams[name] {
					continue
				}
				if !p.Secret {
					t.Errorf("%s.%s.%s: carries a password/PEM but is not declared secret", mod.Name, state, name)
				}
				if p.Pattern != "^vault:.*" {
					t.Errorf("%s.%s.%s: secret param must pin pattern ^vault:.* , got %q", mod.Name, state, name, p.Pattern)
				}
			}
		}
	}
}

// TestConnectParamsAreRead — connectParams is what parseConnConfig actually
// reads, not a list that drifted from it. Each key is fed alone and must land in
// the resulting connConfig; a rename in helpers.go/tls.go fails here rather than
// silently making the manifest table above assert the wrong set.
func TestConnectParamsAreRead(t *testing.T) {
	cases := []struct {
		key   string
		value any
		got   func(connConfig) any
		want  any
	}{
		{"addr", "127.0.0.1:6379", func(c connConfig) any { return c.addr }, "127.0.0.1:6379"},
		{"username", "acl-user", func(c connConfig) any { return c.username }, "acl-user"},
		{"password", "s3cret", func(c connConfig) any { return c.password }, "s3cret"},
		{"db", 3, func(c connConfig) any { return c.db }, 3},
		{"tls", true, func(c connConfig) any { return c.tls.enabled }, true},
		{"tls_ca", "CA-PEM", func(c connConfig) any { return c.tls.caPEM }, "CA-PEM"},
		{"tls_cert", "CERT-PEM", func(c connConfig) any { return c.tls.certPEM }, "CERT-PEM"},
		{"tls_key", "KEY-PEM", func(c connConfig) any { return c.tls.keyPEM }, "KEY-PEM"},
		{"tls_skip_verify", true, func(c connConfig) any { return c.tls.skipVerify }, true},
	}
	if len(cases) != len(connectParams) {
		t.Fatalf("connectParams has %d keys, table covers %d", len(connectParams), len(cases))
	}

	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			// addr is required by parseConnConfig, so it is always present.
			fields := map[string]any{"addr": "127.0.0.1:6379"}
			fields[tc.key] = tc.value
			s, err := structpb.NewStruct(fields)
			if err != nil {
				t.Fatalf("build params: %v", err)
			}
			// keyspace=true — an object that addresses a keyspace, so every key of
			// connectParams including `db` is read (NIM-229: on `sentinel` the
			// keyspace is dropped on purpose, covered in db_scope_test.go).
			cfg, err := parseConnConfig(true, s)
			if err != nil {
				t.Fatalf("parseConnConfig: %v", err)
			}
			if got := tc.got(cfg); got != tc.want {
				t.Errorf("param %q not read by parseConnConfig: got %v, want %v", tc.key, got, tc.want)
			}
		})
	}
}

// TestSourceConnectTLSParamsAreRead — the source_tls* half of
// sourceConnectParams reaches parseSourceTLS. source_addr/source_password are
// read inline by applyOffsetSynced and covered by offset_synced_test.go.
func TestSourceConnectTLSParamsAreRead(t *testing.T) {
	cases := []struct {
		key   string
		value any
		got   func(tlsParams) any
		want  any
	}{
		{"source_tls", true, func(p tlsParams) any { return p.enabled }, true},
		{"source_tls_ca", "CA-PEM", func(p tlsParams) any { return p.caPEM }, "CA-PEM"},
		{"source_tls_cert", "CERT-PEM", func(p tlsParams) any { return p.certPEM }, "CERT-PEM"},
		{"source_tls_key", "KEY-PEM", func(p tlsParams) any { return p.keyPEM }, "KEY-PEM"},
		{"source_tls_skip_verify", true, func(p tlsParams) any { return p.skipVerify }, true},
	}
	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			s, err := structpb.NewStruct(map[string]any{tc.key: tc.value})
			if err != nil {
				t.Fatalf("build params: %v", err)
			}
			if got := tc.got(parseSourceTLS(s.GetFields())); got != tc.want {
				t.Errorf("param %q not read by parseSourceTLS: got %v, want %v", tc.key, got, tc.want)
			}
		})
	}
}

// diff returns the members of a missing from b; both must be sorted.
func diff(a, b []string) []string {
	in := make(map[string]bool, len(b))
	for _, s := range b {
		in[s] = true
	}
	var out []string
	for _, s := range a {
		if !in[s] {
			out = append(out, s)
		}
	}
	return out
}
