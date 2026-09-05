// The objects this artifact serves — address level 2 of `redis.<object>.<action>`
// (ADR-020 amendment 2026-09-02, NIM-765/NIM-766).
//
// One artifact, seven objects, one body of Redis code. Every object is the same
// [object] value with a different action table; the tables ARE the boundary, so
// `instance` cannot reach a cluster action by accident — that state is simply
// unknown to it. The driver was not split: an action delegates to the very same
// method on [RedisModule] it was dispatched to before, which is why ~12k lines of
// Redis behaviour and its tests are untouched by the re-layout.
//
// The one thing that DID move is the dispatch key. `cluster` used to be a single
// state carrying `params.action` with seven verbs in it; those seven are now seven
// actions at level 3, and each declares only the params it reads. Keeping the verb
// in a param would have re-admitted at the parameter level exactly what NIM-765
// removed from the address.
package main

import (
	"context"
	"fmt"
	"sort"
	"strings"

	pluginv1 "github.com/souls-guild/soul-stack/proto/plugin/gen/go/v1"
	"github.com/souls-guild/soul-stack/sdk/module"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

// eventStream is the Apply server stream, named once so the action table below
// stays readable.
type eventStream = grpc.ServerStreamingServer[pluginv1.ApplyEvent]

// action is one state of an object — address level 3.
//
// Exactly one of apply / applyNodes is set. apply gets a connection already open
// to `params.addr`; applyNodes gets none, because it manages several nodes and
// opens one connection per node (every cluster action, which has no single addr).
type action struct {
	validate func(f map[string]*structpb.Value) []string
	apply    func(m *RedisModule, ctx context.Context, stream eventStream, conn redisConn, params *structpb.Struct) error

	applyNodes func(m *RedisModule, ctx context.Context, stream eventStream, params *structpb.Struct) error
}

// object is one addressable object of this artifact — the `instance` in
// `redis.instance.pinged`. It serves the actions in its table and nothing else.
//
// It implements SoulModule, so the value goes straight into [module.Def].Impl;
// BaseModule supplies the no-op Plan, which keeps the deliberate default-deny on
// dry_run (no PlanReadSafe) and on Errand (no ErrandReadSafe) that this plugin has
// had since the 2026-06-22 decision.
type object struct {
	module.BaseModule

	// impl is the shared Redis implementation. Seven objects, one driver.
	impl *RedisModule

	// name is address level 2 — used in diagnostics only; what an operator
	// actually addresses is the registration alias plus this name.
	name string

	// decl is what this object's Def declares about each of its actions — the
	// same map, from the same function, not a copy. Validate and Apply refuse a
	// param whose value is not of the declared type (params.go, NIM-778), so the
	// declaration is load-bearing at runtime and not only in the schema document.
	decl map[string]module.State

	// keyspace says whether `params.db` means anything on this object (NIM-229).
	// Only `sentinel` says false, and not as a matter of taste: a Sentinel is not
	// a keyspace server and refuses SELECT, so go-redis — which issues SELECT for
	// any DB > 0 on connect — cannot open the connection at all. The value does
	// not misconfigure the object, it breaks it.
	//
	// The rule lives here, once: [parseConnConfig] then stops carrying the
	// keyspace and [validateSentinel] refuses the value up front, so Apply is safe
	// on its own (Validate is a separate RPC a runner need not call) and the
	// author is told before the run rather than by a connect failure.
	keyspace bool

	actions map[string]action
}

// Validate performs runtime checks on top of the static ones from soul-lint.
// Returns a ValidateReply with errors (not an error) — that is the Validate
// contract. Error text does NOT contain the password.
func (o *object) Validate(_ context.Context, req *pluginv1.ValidateRequest) (*pluginv1.ValidateReply, error) {
	act, ok := o.actions[req.GetState()]
	if !ok {
		return &pluginv1.ValidateReply{Ok: false, Errors: []string{o.unknownState(req.GetState())}}, nil
	}
	// Types before content: an action's own checks read the values, and a value
	// of the wrong type makes whatever they report about it noise.
	if errs := checkParamTypes(o.decl[req.GetState()].Input, req.GetParams().GetFields()); len(errs) > 0 {
		return &pluginv1.ValidateReply{Ok: false, Errors: errs}, nil
	}
	if errs := act.validate(req.GetParams().GetFields()); len(errs) > 0 {
		return &pluginv1.ValidateReply{Ok: false, Errors: errs}, nil
	}
	return &pluginv1.ValidateReply{Ok: true}, nil
}

// Apply dispatches by state within this object. The final event carries
// changed/failed + output (ADR-012). Connection errors are sanitized
// (redactError) — the address is preserved for diagnostics, the password stripped.
func (o *object) Apply(req *pluginv1.ApplyRequest, stream eventStream) error {
	ctx := stream.Context()

	act, ok := o.actions[req.GetState()]
	if !ok {
		return sendFailure(stream, o.unknownState(req.GetState()))
	}

	// Before anything opens a socket: a param of the wrong type is refused, not
	// coerced (params.go, NIM-778). Here rather than only in Validate because a
	// runner need not call Validate at all — the runtime calls Apply — and the
	// value this protects decides whether the password goes out over TLS.
	if errs := checkParamTypes(o.decl[req.GetState()].Input, req.GetParams().GetFields()); len(errs) > 0 {
		return sendFailure(stream, strings.Join(errs, "; "))
	}

	// An action that manages MULTIPLE nodes (a connection to each from its
	// nodes-map) has no single addr and owns its connection lifecycle.
	if act.applyNodes != nil {
		return act.applyNodes(o.impl, ctx, stream, req.GetParams())
	}

	cfg, err := parseConnConfig(o.keyspace, req.GetParams())
	if err != nil {
		return sendFailure(stream, err.Error())
	}
	conn, err := o.impl.openConn(ctx, cfg)
	if err != nil {
		// Redact BOTH password and PEM client-key: a TLS handshake error could
		// theoretically carry the client-key (security invariant ADR-010, same as
		// the password).
		return sendFailure(stream, "connect: "+redactError(err, cfg.password, cfg.tls.keyPEM))
	}
	defer func() { _ = conn.Close() }()

	return act.apply(o.impl, ctx, stream, conn, req.GetParams())
}

// unknownState names the object as well as the state: with seven objects in one
// artifact, "unknown state" alone would leave an author guessing whether the word
// is wrong or the object is.
func (o *object) unknownState(state string) string {
	names := make([]string, 0, len(o.actions))
	for name := range o.actions {
		names = append(names, name)
	}
	sort.Strings(names)
	return fmt.Sprintf("unknown state %q for object %q (expected %s)", state, o.name, strings.Join(names, "|"))
}

// states returns the action names this object serves, for the guard that keeps the
// schema document and the dispatch table from drifting apart.
func (o *object) states() []string {
	names := make([]string, 0, len(o.actions))
	for name := range o.actions {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
