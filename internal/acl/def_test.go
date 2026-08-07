package acl

import (
	"context"
	"testing"

	pluginv1 "github.com/souls-guild/soul-stack/proto/plugin/gen/go/v1"
)

// The description and the implementation are two declarations of one contract,
// and nothing in the language ties them together. A state added to [Module] and
// not to Apply's switch passes every other test here and fails on a host, with
// the task already dispatched; a state added to Apply and not to [Module] is
// simply unreachable, because keeper checks the address against the schema
// before it sends anything.
//
// So: every declared state, applied with valid parameters to an empty server,
// must go through. The parameters have to be valid or the run never reaches the
// state dispatch at all — Apply parses first, and a parameter error would let an
// undeclared state pass this test while failing on a host.
func TestEveryDeclaredStateIsImplemented(t *testing.T) {
	if len(Module.States) == 0 {
		t.Fatal("the module declares no states")
	}
	for state := range Module.States {
		ev := run(t, newFake(), state, base(map[string]any{"nopass": true}))
		if ev.GetFailed() {
			t.Errorf("state %q is declared in Module.States but Apply does not serve it: %s",
				state, ev.GetMessage())
		}
	}
}

// Impl is what the host actually runs; a Def whose Impl is nil or points at
// something else describes one module and serves another.
func TestImplIsThisModule(t *testing.T) {
	if _, ok := Module.Impl.(*ACL); !ok {
		t.Fatalf("Module.Impl is %T, want *ACL", Module.Impl)
	}
}

// Every state the module serves reads `host` and `username` — they are how it
// finds the server and the user, in both directions. A state that declares
// neither would fail on every task with a parameter error nobody can act on.
func TestDeclaredStatesCarryTheParametersApplyReads(t *testing.T) {
	for state, def := range Module.States {
		for _, name := range []string{"host", "username"} {
			p, ok := def.Input[name]
			if !ok {
				t.Errorf("state %q does not declare %q, which Apply requires", state, name)
				continue
			}
			if !p.Required {
				t.Errorf("state %q declares %q as optional, but Apply fails without it", state, name)
			}
		}
	}
}

// Apply refuses a state it does not know rather than picking one — the same
// property the artifact has for module names, one level down.
func TestUndeclaredStateIsRefused(t *testing.T) {
	m := &ACL{connect: func(context.Context, connParams) (aclClient, error) { return newFake(), nil }}
	st := &fakeApplyStream{ctx: context.Background()}
	req := &pluginv1.ApplyRequest{State: "presnet", Params: params(t, base(nil))}
	if err := m.Apply(req, st); err != nil {
		t.Fatalf("Apply returned a transport error: %v", err)
	}
	if ev := st.last(); ev == nil || !ev.GetFailed() {
		t.Fatal("a state the module does not declare must fail the task")
	}
}
