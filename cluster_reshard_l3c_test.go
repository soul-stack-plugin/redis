//go:build e2e_live

// L3c-skeleton: e2e-live reshard vs REAL Redis cluster. Build-tag
// e2e_live (like other live-harness repo) + t.Skip: the framework is compiled into
// general gate, but really doesn't race without a raised cluster and explicit unlocking
// (a separate slice - propose-and-wait for the harness-entity "live redis cluster").
//
// L0 (cluster_test.go) proves the SEQUENCE of commands (SETSLOT IMPORTING/
// MIGRATING/MIGRATE/SETSLOT NODE) and lossless on fake-conn. L0 does NOT prove that
// the slots actually changed ownership, the keys (incl. whitespace+TTL) moved without
// losses, and DBSIZE converged - this is the L3c zone on a live cluster.
//
// reshard is IMPERATIVE and NOT idempotent: the test calls it ONCE and checks
// the result of this one transfer. A repeat run would shift another N
// slots - this is by design (exec-style day-2, not converge), not a bug.
package main

import (
	"context"
	"testing"
)

// TestL3cReshard_LiveLossless - e2e-live: reshard N slots from->to on the present
// cluster with check of lossless transfer and correct change of owner of slots.
//
// TODO(L3c-future, we need a harness-entity "live redis cluster", propose-and-wait):
//  1. Raise a REAL Redis cluster of at least 2 masters (testcontainers /
//     docker-compose redis:7 cluster, --cluster-enabled yes) -> endpoints
//     from/to + node-id of both masters.
//  2. Write a set of keys to the slots belonging to the source master (from),
//     MUST include:
//     - whitespace names ("user 42", "a\tb", "c\nd") - lossless invariant
//     typed GetKeysInSlot (see brief P3/remove-node-fix);
//     - keys with TTL (SET k v EX 3600) - check that TTL has moved (MIGRATE
//     carries TTL along with the value), TTL on target > 0 and close to 3600;
//     - regular keys for contrast.
//     Fix the set of source keys and the total DBSIZE to reshard.
//  3. Call m.cluster().Apply(state=resharded, from, to, slots=N) ONCE
//     (imperative). slots <= number of slots source.
//  4. Invariants AFTER:
//     - CLUSTER NODES: transferred N slots now belong to (node-id
//     target), from no longer has them; the remaining slots are not touched.
//     - cluster_state:ok (CLUSTER INFO), 16384 slots covered, no holes.
//     - all keys of transferred slots are read from TARGET (incl. whitespace+TTL),
//     and are NOT read from source -> lossless, not a single lost key.
//     - DBSIZE(from) + DBSIZE(to) == original amount (nothing missing or
//     doubled).
//  5. Check non-idempotency with a separate sub-case: repeated reshard
//     the same from->to slots=N shifts ANOTHER N slots (a NOT no-op) - this
//     fixed semantics, the test confirms it, and does not catch it as a regression.
func TestL3cReshard_LiveLossless(t *testing.T) {
	t.Skip("L3c-skeleton: we need a live Redis cluster (harness entity \"live redis cluster\"," +
		"propose-and-wait). L0 cluster_test.go proves command sequence + lossless" +
		"on fake-conn; here the actual change of slot ownership, whitespace+TTL transfer are checked" +
		"keys and DBSIZE convergence on a real cluster.")

	// Framework for future unlocking: context + call point Apply. Real
	// raising the cluster/writing keys/owner assertions will add L3c-future-slice.
	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	// from, to := <endpoint source master>, <endpoint target master>
	// keys := []string{"user 42", "a\tb", "c\nd", "plain", "withttl"}
	// ... SET keys into source slots (withttl with EX 3600)...
	// m:= &RedisModule{} // real connect (defaultConnect to live nodes)
	// stream := &applyStream{}
	// _ = m.cluster().Apply(&pluginv1.ApplyRequest{State: "resharded", Params: ...}, stream)
	// ... assert owner of slots / lossless keys / TTL / DBSIZE...
}
