package config

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
func TestEveryDeclaredStateIsImplemented(t *testing.T) {
	if len(Module.States) == 0 {
		t.Fatal("the module declares no states")
	}
	for state := range Module.States {
		f := newFake(map[string]string{"maxmemory": "0"})
		ev := runApply(t, f, state, map[string]any{"settings": map[string]any{"maxmemory": "0"}})
		if ev.last(t).GetFailed() {
			t.Errorf("state %q is declared in Module.States but Apply does not serve it: %s",
				state, ev.last(t).GetMessage())
		}
	}
}

// Impl is what the host actually runs; a Def whose Impl is nil or points at
// something else describes one module and serves another.
func TestImplIsThisModule(t *testing.T) {
	if _, ok := Module.Impl.(*Config); !ok {
		t.Fatalf("Module.Impl is %T, want *Config", Module.Impl)
	}
}

// Every state reads `host` — it is how the module finds the server. A state that
// declares it optional would fail on every task with a parameter error nobody
// can act on.
func TestDeclaredStatesCarryTheParametersApplyReads(t *testing.T) {
	for state, def := range Module.States {
		p, ok := def.Input["host"]
		if !ok {
			t.Errorf("state %q does not declare \"host\", which Apply requires", state)
			continue
		}
		if !p.Required {
			t.Errorf("state %q declares \"host\" as optional, but Apply fails without it", state)
		}
	}
	if p, ok := Module.States["present"].Input["settings"]; !ok || !p.Required {
		t.Error("present must declare settings as required — Apply refuses without it")
	}
}

// ★ The Output block is what an operator writes `register:` against, and nothing
// checks it at runtime: a key renamed in Apply and not here leaves every scenario
// reading an absent field, silently. So the two are compared on a real run.
//
// The comparison is one-directional on purpose. Every key Apply emits must be
// declared; a declared key that a particular run does not emit is fine, because
// `changes` is empty on a noop and the states legitimately differ in what they
// fill in.
func TestEmittedOutputKeysAreDeclared(t *testing.T) {
	runs := []struct {
		name     string
		state    string
		fake     map[string]string
		extra    map[string]any
		wantKeys []string
	}{
		{
			name:  "present/noop",
			state: "present",
			fake:  map[string]string{"maxmemory": "0"},
			extra: map[string]any{"settings": map[string]any{"maxmemory": "0"}},
		},
		{
			name:  "present/altered",
			state: "present",
			fake:  map[string]string{"maxmemory": "0"},
			extra: map[string]any{"settings": map[string]any{"maxmemory": "100mb"}},
			// A real change must name what moved; that is the whole point of the block.
			wantKeys: []string{"action", "changes", "changed_parameters", "persisted", "atomic"},
		},
		{
			name:     "read",
			state:    "read",
			fake:     map[string]string{"maxmemory": "0"},
			extra:    map[string]any{},
			wantKeys: []string{"config", "count", "redacted"},
		},
	}

	for _, r := range runs {
		t.Run(r.name, func(t *testing.T) {
			declared := Module.States[r.state].Output
			if len(declared) == 0 {
				t.Fatalf("state %q declares no output", r.state)
			}
			ev := runApply(t, newFake(r.fake), r.state, r.extra).last(t)
			if ev.GetFailed() {
				t.Fatalf("apply: %s", ev.GetMessage())
			}
			emitted := ev.GetOutput().GetFields()
			for key := range emitted {
				if _, ok := declared[key]; !ok {
					t.Errorf("Apply emits %q, which state %q does not declare — a scenario registering it reads an undocumented field", key, r.state)
				}
			}
			for _, key := range r.wantKeys {
				if _, ok := emitted[key]; !ok {
					t.Errorf("state %q did not emit %q on this run", r.state, key)
				}
			}
		})
	}
}

// Apply refuses a state it does not know rather than picking one — the same
// property the artifact has for module names, one level down.
func TestUndeclaredStateIsRefused(t *testing.T) {
	m := &Config{connect: func(context.Context, connParams) (configClient, error) { return newFake(nil), nil }}
	st := &applyStream{}
	req := &pluginv1.ApplyRequest{State: "presnet", Params: params(t, map[string]any{
		"settings": map[string]any{"maxmemory": "0"},
	})}
	if err := m.Apply(req, st); err != nil {
		t.Fatalf("Apply returned a transport error: %v", err)
	}
	if ev := st.last(t); !ev.GetFailed() {
		t.Fatal("a state the module does not declare must fail the task")
	}
}

// Secret parameters must be declared as such, or keeper-render has no reason to
// resolve the vault-ref and the literal string reaches Redis.
func TestCredentialsAreDeclaredSecret(t *testing.T) {
	for state, def := range Module.States {
		p, ok := def.Input["login_password"]
		if !ok {
			continue
		}
		if !p.Secret {
			t.Errorf("state %q declares login_password without secret: true", state)
		}
		if p.Pattern != `^vault:.*` {
			t.Errorf("state %q declares login_password with pattern %q, want the vault-ref pattern", state, p.Pattern)
		}
	}
}
