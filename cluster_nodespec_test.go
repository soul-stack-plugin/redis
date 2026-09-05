// NIM-786: Validate must refuse a node spec exactly where Apply refuses it.
//
// Every cluster action reads its endpoints out of a map-typed param, and a map is
// where the declared-type gate stops (params.go, NIM-778) - so whatever is inside
// one is checked by the action or by nothing. It was by nothing: `validateCluster*`
// either counted the keys and looked no further, or resolved the endpoints only to
// compare them and DISCARDED the parse error. A `port: "6379"` therefore passed
// Validate clean and failed on Apply, which is to say the operator learned of the
// typo in the middle of a run instead of before it.
//
// The guard is a table over every action and every node-spec param it reads. For
// each one it asserts three things: Validate refuses, the message carries the
// parameter's ADDRESS, and - where Apply refuses before it dials - the two
// messages are the SAME string. Identical text is the checkable form of "exactly
// where": a Validate that refuses for its own reason, at its own address, would
// still leave the phase pretending to a completeness it does not have.
package main

import (
	"context"
	"strings"
	"testing"

	pluginv1 "github.com/souls-guild/soul-stack/proto/plugin/gen/go/v1"
)

// stringPortSpec is one node spec with the port written as a string - the shape
// from the ticket. It is a plain typo (YAML quoting), not an exotic input.
func stringPortSpec() map[string]any {
	return map[string]any{"ip": "10.0.0.1", "port": "6379"}
}

// notAMapSpec is the other way a nodes-map entry goes wrong: the operator wrote the
// endpoint where the map goes. Apply reads it as a node with no endpoint at all.
func notAMapSpec() any { return "10.0.0.1:6379" }

type nodeSpecCase struct {
	name   string
	state  string
	params map[string]any

	// addr is the parameter address the refusal must name.
	addr string

	// applyDialsFirst marks the one param Apply reads only after connecting to the
	// cluster (`master`, read by pickReplicationMaster off a live CLUSTER NODES).
	// Its refusal cannot be compared against Apply's without a fleet, and the fact
	// that Apply gets there only mid-run is the reason Validate must carry it.
	applyDialsFirst bool
}

func nodeSpecCases() []nodeSpecCase {
	return []nodeSpecCase{
		{
			name:  "created/nodes",
			state: "created",
			params: map[string]any{
				"nodes": map[string]any{
					"node-0": map[string]any{"addr": "10.0.0.1:6379"},
					"node-1": stringPortSpec(),
				},
			},
			addr: "params.nodes[node-1]",
		},
		{
			name:  "created/nodes-not-a-map",
			state: "created",
			params: map[string]any{
				"nodes": map[string]any{
					"node-0": map[string]any{"addr": "10.0.0.1:6379"},
					"node-1": notAMapSpec(),
				},
			},
			addr: "params.nodes[node-1]",
		},
		{
			name:  "node-added/new_node",
			state: "node-added",
			params: map[string]any{
				"new_node": stringPortSpec(),
				"seed":     map[string]any{"addr": "10.0.0.2:6379"},
			},
			addr: "params.new_node",
		},
		{
			name:  "node-added/seed",
			state: "node-added",
			params: map[string]any{
				"new_node": map[string]any{"addr": "10.0.0.1:6379"},
				"seed":     stringPortSpec(),
			},
			addr: "params.seed",
		},
		{
			name:  "node-added/master",
			state: "node-added",
			params: map[string]any{
				"new_node": map[string]any{"addr": "10.0.0.1:6379"},
				"seed":     map[string]any{"addr": "10.0.0.2:6379"},
				"master":   stringPortSpec(),
				"role":     "replica",
			},
			addr:            "params.master",
			applyDialsFirst: true,
		},
		{
			name:  "node-removed/node",
			state: "node-removed",
			params: map[string]any{
				"node": stringPortSpec(),
				"seed": map[string]any{"addr": "10.0.0.2:6379"},
			},
			addr: "params.node",
		},
		{
			name:  "node-removed/seed",
			state: "node-removed",
			params: map[string]any{
				"node": map[string]any{"addr": "10.0.0.1:6379"},
				"seed": stringPortSpec(),
			},
			addr: "params.seed",
		},
		{
			name:  "resharded/from",
			state: "resharded",
			params: map[string]any{
				"from":  stringPortSpec(),
				"to":    map[string]any{"addr": "10.0.0.2:6379"},
				"slots": 100,
			},
			addr: "params.from",
		},
		{
			name:  "resharded/to",
			state: "resharded",
			params: map[string]any{
				"from":  map[string]any{"addr": "10.0.0.1:6379"},
				"to":    stringPortSpec(),
				"slots": 100,
			},
			addr: "params.to",
		},
		{
			name:  "external-joined/nodes",
			state: "external-joined",
			params: map[string]any{
				"nodes":        map[string]any{"node-0": stringPortSpec()},
				"source_nodes": []any{"10.0.0.9:6379"},
				"shards_dest":  1,
			},
			addr: "params.nodes[node-0]",
		},
		{
			name:  "failed-over/nodes",
			state: "failed-over",
			params: map[string]any{
				"nodes": map[string]any{"node-0": stringPortSpec()},
			},
			addr: "params.nodes[node-0]",
		},
		{
			name:  "external-forgotten/nodes",
			state: "external-forgotten",
			params: map[string]any{
				"nodes":        map[string]any{"node-0": stringPortSpec()},
				"source_nodes": []any{"10.0.0.9:6379"},
			},
			addr: "params.nodes[node-0]",
		},
	}
}

// TestValidate_ClusterRefusesUnresolvableNodeSpec is the ticket itself: the refusal
// must happen in Validate, and it must say WHICH parameter is wrong.
func TestValidate_ClusterRefusesUnresolvableNodeSpec(t *testing.T) {
	for _, tc := range nodeSpecCases() {
		t.Run(tc.name, func(t *testing.T) {
			o := bundleObjects(refusingModule(t))["cluster"]
			reply, err := o.Validate(context.Background(), &pluginv1.ValidateRequest{
				State:  tc.state,
				Params: mustStruct(t, tc.params),
			})
			if err != nil {
				t.Fatalf("Validate: %v", err)
			}
			if reply.GetOk() {
				t.Fatalf("waited ok=false on an unresolvable %s, got %+v", tc.addr, reply)
			}
			if !strings.Contains(strings.Join(reply.GetErrors(), "; "), tc.addr+":") {
				t.Errorf("the refusal must address %s, got %q", tc.addr, reply.GetErrors())
			}
		})
	}
}

// TestValidate_ClusterNodeSpecRefusalMatchesApply is the "exactly where" half. Apply
// already refuses every one of these; the two refusals must be the same sentence,
// or Validate is answering a different question than the one Apply will ask.
func TestValidate_ClusterNodeSpecRefusalMatchesApply(t *testing.T) {
	for _, tc := range nodeSpecCases() {
		if tc.applyDialsFirst {
			continue
		}
		t.Run(tc.name, func(t *testing.T) {
			// refusingModule fails the test on any dial: Apply must refuse this
			// input before it opens a socket, which is what makes the comparison
			// with Validate meaningful rather than a race with the network.
			o := bundleObjects(refusingModule(t))["cluster"]

			reply, err := o.Validate(context.Background(), &pluginv1.ValidateRequest{
				State:  tc.state,
				Params: mustStruct(t, tc.params),
			})
			if err != nil {
				t.Fatalf("Validate: %v", err)
			}

			_, fin := applyObject(o, tc.state, tc.params)
			if fin == nil || !fin.Failed {
				t.Fatalf("waited failed=true from Apply on an unresolvable %s, got %+v", tc.addr, fin)
			}
			if got := strings.Join(reply.GetErrors(), "; "); got != fin.GetMessage() {
				t.Errorf("Validate and Apply disagree on %s:\n  Validate: %q\n  Apply:    %q",
					tc.addr, got, fin.GetMessage())
			}
		})
	}
}

// TestValidate_ClusterAcceptsResolvableNodeSpec is the other half of the acceptance:
// the guard above refuses a wrong endpoint, it does not refuse an endpoint. Both
// spelled forms - {addr} and {ip, port} - still pass, so a scenario that was valid
// stays valid.
func TestValidate_ClusterAcceptsResolvableNodeSpec(t *testing.T) {
	for _, tc := range []struct {
		name   string
		state  string
		params map[string]any
	}{
		{
			name:  "created/both-forms",
			state: "created",
			params: map[string]any{
				"replicas_per_shard": 1,
				"nodes": map[string]any{
					"node-0": map[string]any{"addr": "10.0.0.1:6379"},
					"node-1": map[string]any{"ip": "10.0.0.2", "port": 6379},
				},
			},
		},
		{
			name:  "node-added/with-master",
			state: "node-added",
			params: map[string]any{
				"new_node": map[string]any{"ip": "10.0.0.1", "port": 6379},
				"seed":     map[string]any{"addr": "10.0.0.2:6379"},
				"master":   map[string]any{"addr": "10.0.0.3:6379"},
			},
		},
		{
			// An unset master means auto-selection, and the scenario spells "unset"
			// as an empty addr (masterSpecGiven). That is not an endpoint Validate
			// may refuse - Apply never reads it.
			name:  "node-added/empty-master-is-auto-select",
			state: "node-added",
			params: map[string]any{
				"new_node": map[string]any{"addr": "10.0.0.1:6379"},
				"seed":     map[string]any{"addr": "10.0.0.2:6379"},
				"master":   map[string]any{"addr": ""},
			},
		},
		{
			name:  "resharded/mixed-forms",
			state: "resharded",
			params: map[string]any{
				"from":  map[string]any{"addr": "10.0.0.1:6379"},
				"to":    map[string]any{"ip": "10.0.0.2", "port": 6379},
				"slots": 100,
			},
		},
		{
			name:  "external-joined/happy",
			state: "external-joined",
			params: map[string]any{
				"nodes":        map[string]any{"node-0": map[string]any{"ip": "10.0.0.1", "port": 6379}},
				"source_nodes": []any{"10.0.0.9:6379"},
				"shards_dest":  1,
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := bundleObjects(refusingModule(t))["cluster"]
			reply, err := o.Validate(context.Background(), &pluginv1.ValidateRequest{
				State:  tc.state,
				Params: mustStruct(t, tc.params),
			})
			if err != nil {
				t.Fatalf("Validate: %v", err)
			}
			if !reply.GetOk() || len(reply.GetErrors()) != 0 {
				t.Fatalf("waited ok=true, got %+v", reply)
			}
		})
	}
}
