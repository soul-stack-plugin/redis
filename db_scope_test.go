package main

import (
	"strings"
	"testing"

	pluginv1 "github.com/souls-guild/soul-stack/proto/plugin/gen/go/v1"
)

// `db` selects a keyspace, and Sentinel has none — it answers SELECT with an
// error, so a non-zero `db` on this state does not misconfigure the connection,
// it breaks it (NIM-229). The manifest said so in prose ("leave 0"), which is the
// surface NIM-205 just established nobody reads; the connect path took the value
// anyway and handed it to go-redis, which issues SELECT for any DB > 0.
//
// The key stays DECLARED: since NIM-204 a plugin manifest gates its params, so
// removing it would fail every scenario that passes `db: 0` with
// module.unknown_param — a break in exchange for a cosmetic tidy. Declared and
// refused when it cannot work is the honest contract.

func TestValidateSentinel_NonZeroDBRefused(t *testing.T) {
	m := &RedisModule{}
	reply, err := m.sentinel().Validate(t.Context(), &pluginv1.ValidateRequest{
		State: "monitored",
		Params: mustStruct(t, map[string]any{
			"addr": "127.0.0.1:26379",
			"db":   1,
		}),
	})
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if reply.GetOk() {
		t.Fatal("Validate accepted db=1 on sentinel — SELECT is refused by Sentinel, so the connection cannot be opened at all")
	}
	if !hasErrorAbout(reply.GetErrors(), "params.db") {
		t.Errorf("errors do not name params.db, so the author cannot tell what to fix: %v", reply.GetErrors())
	}
}

// db: 0 is what every sentinel task in the corpus passes (the manifest's own
// advice), and it must keep validating — the point is to refuse the value that
// breaks, not the key.
func TestValidateSentinel_ZeroDBAccepted(t *testing.T) {
	m := &RedisModule{}
	reply, err := m.sentinel().Validate(t.Context(), &pluginv1.ValidateRequest{
		State: "monitored",
		Params: mustStruct(t, map[string]any{
			"addr": "127.0.0.1:26379",
			"db":   0,
		}),
	})
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if !reply.GetOk() {
		t.Errorf("Validate refused db=0 on sentinel: %v", reply.GetErrors())
	}
}

// Validate is a separate RPC and a runner is free not to call it, so Apply may
// not rely on it: the connect path itself must not carry a keyspace onto a state
// that has none.
func TestApplySentinel_ConnectCarriesNoKeyspace(t *testing.T) {
	conn := &sentinelConn{masterExists: true, masterReply: "name mymaster ip 10.0.0.1 port 6379 quorum 2", configReply: map[string]string{}}
	m := sentinelModule(conn)
	stream := &applyStream{}

	err := m.sentinel().Apply(&pluginv1.ApplyRequest{
		State: "monitored",
		Params: mustStruct(t, map[string]any{
			"addr":        "127.0.0.1:26379",
			"master_name": "mymaster",
			"monitor":     map[string]any{"ip": "10.0.0.1", "port": 6379, "quorum": 2},
			"db":          3,
		}),
	}, stream)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if conn.cfg.db != 0 {
		t.Errorf("sentinel connection carries db=%d — go-redis issues SELECT for any DB > 0 and Sentinel refuses it", conn.cfg.db)
	}
}

// The states that do address a keyspace keep doing so. This is the other half of
// the rule: `db` is dropped where it cannot work, not everywhere.
func TestApplyCommand_ConnectCarriesKeyspace(t *testing.T) {
	conn := &fakeConn{}
	m := newModule(conn)
	stream := &applyStream{}

	err := m.command().Apply(&pluginv1.ApplyRequest{
		State: "run",
		Params: mustStruct(t, map[string]any{
			"addr": "127.0.0.1:6379",
			"args": []any{"PING"},
			"db":   3,
		}),
	}, stream)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if conn.cfg.db != 3 {
		t.Errorf("command connection lost its keyspace: db=%d, want 3", conn.cfg.db)
	}
}

func hasErrorAbout(errs []string, needle string) bool {
	for _, e := range errs {
		if strings.Contains(e, needle) {
			return true
		}
	}
	return false
}
