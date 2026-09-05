// Parameter typing at the artifact's boundary — a value whose type is not the one
// the object declares is REFUSED, never coerced (NIM-778).
//
// The rule it replaces coerced: [boolOrDefault] returned its default on anything
// that was not a boolean, so `tls: "true"` written as a string read as `tls: false`
// and the connection — password included — went out in plaintext, reported as
// reconciled. An author's typo became a silent leak instead of a refusal, and the
// direction of the fallback was what made it one: `false` is the insecure side of
// that parameter. Nothing upstream catches it either. The runtime calls Apply, not
// Validate, and the Keeper's static `checkParamType` returns nil on a `${…}` cell,
// so `tls: "${ vars.redis_tls }"` over a string var lints clean. The plugin is the
// last place that can say no.
//
// It is checked against the DECLARATION rather than at each read site on purpose.
// `persist` was made strict by hand in NIM-767 and `tls` beside it was not, which
// is the asymmetry NIM-778 opens with; a rule derived from [object.decl] cannot
// drift that way, and a parameter added later inherits it without being remembered.
package main

import (
	"fmt"
	"sort"

	"github.com/souls-guild/soul-stack/sdk/module"
	"google.golang.org/protobuf/types/known/structpb"
)

// checkParamTypes reports every param of f whose value does not match the type
// decl gives it, addressed as `params.<name>`. Deterministic order.
//
// Three things it deliberately does NOT do. An UNDECLARED key is left alone — the
// engine refuses one as `unknown_param` (NIM-204) and duplicating that here would
// only give it a second wording. An ABSENT key is left alone — that is what a
// default is for. And a NULL is read as absent, because `tls:` written with nothing
// after it arrives as one and means "unset", not "a value of the wrong type".
//
// Nothing here is about a value's CONTENT: `db: -1` and an unparseable PEM are the
// action's own business. This answers one question, the one that was answered by
// guessing.
func checkParamTypes(decl module.Input, f map[string]*structpb.Value) []string {
	if len(decl) == 0 || len(f) == 0 {
		return nil
	}
	names := make([]string, 0, len(f))
	for name := range f {
		names = append(names, name)
	}
	sort.Strings(names)

	var errs []string
	for _, name := range names {
		p, declared := decl[name]
		if !declared || p.Type == "" {
			continue
		}
		v := f[name]
		if v == nil {
			continue
		}
		if _, isNull := v.GetKind().(*structpb.Value_NullValue); isNull {
			continue
		}
		if want, ok := paramTypeMismatch(p.Type, v); !ok {
			errs = append(errs, fmt.Sprintf("params.%s: must be %s, got %s", name, want, valueTypeName(v)))
		}
	}
	return errs
}

// paramTypeMismatch answers whether v carries the declared type, and names that
// type for the message when it does not.
//
// The synonyms are the ones `sdk/schema` admits (`docs/input.md` spellings), so a
// module declaring `module.Integer` is held to the same rule as one declaring
// `module.Int`. An INT is not merely a number: 7.5 where an int is declared is
// refused rather than truncated to 7, for the reason the whole file exists —
// truncation is a guess, and `db: 7.5` silently addressing database 7 is the same
// class of surprise as `tls: "true"` addressing plaintext.
func paramTypeMismatch(t module.ParamType, v *structpb.Value) (string, bool) {
	switch t {
	case module.Bool, module.Boolean:
		_, ok := v.GetKind().(*structpb.Value_BoolValue)
		return "a boolean (true/false)", ok
	case module.Int, module.Integer:
		n, ok := v.GetKind().(*structpb.Value_NumberValue)
		return "an integer", ok && n.NumberValue == float64(int64(n.NumberValue))
	case module.Number:
		_, ok := v.GetKind().(*structpb.Value_NumberValue)
		return "a number", ok
	case module.String:
		_, ok := v.GetKind().(*structpb.Value_StringValue)
		return "a string", ok
	case module.List, module.Array:
		_, ok := v.GetKind().(*structpb.Value_ListValue)
		return "a list", ok
	case module.Map, module.Object:
		_, ok := v.GetKind().(*structpb.Value_StructValue)
		return "a map", ok
	default:
		// A type this build does not know is not a licence to guess, but it is
		// also not this function's to refuse: it can only come from a newer
		// sdk/schema, and the schema validator is what reports that.
		return "", true
	}
}

// valueTypeName names what actually arrived, in the same vocabulary the message
// asks for. Without it "must be a boolean" leaves an author to work out which of
// their values is the string.
func valueTypeName(v *structpb.Value) string {
	switch v.GetKind().(type) {
	case *structpb.Value_BoolValue:
		return "a boolean"
	case *structpb.Value_NumberValue:
		return "a number"
	case *structpb.Value_StringValue:
		return "a string"
	case *structpb.Value_ListValue:
		return "a list"
	case *structpb.Value_StructValue:
		return "a map"
	default:
		return "nothing"
	}
}

// --- nested specs ---
//
// `monitor`, `nodes` and the node specs (`seed`/`master`/`new_node`/`from`/`to`)
// are declared as maps and nothing declares what is INSIDE them, so
// [checkParamTypes] reaches the map and stops. The readers below carry the rule the
// rest of the way. `monitor.quorum` is why they exist and not only for symmetry: a
// coerced `quorum: "2"` fell back to 1, and a Sentinel quorum of 1 is a single
// vote deciding a failover.

// intField reads one integer out of a nested spec, refusing a value of another type
// rather than defaulting. addr is the full parameter address for the message
// (`params.monitor.quorum`), since the caller is the only one that knows it.
func intField(spec map[string]*structpb.Value, key, addr string, def int) (int, error) {
	v := spec[key]
	if v == nil {
		return def, nil
	}
	if _, isNull := v.GetKind().(*structpb.Value_NullValue); isNull {
		return def, nil
	}
	n, ok := v.GetKind().(*structpb.Value_NumberValue)
	if !ok || n.NumberValue != float64(int64(n.NumberValue)) {
		return 0, fmt.Errorf("%s: must be an integer, got %s", addr, valueTypeName(v))
	}
	return int(n.NumberValue), nil
}
