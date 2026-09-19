package main

import (
	"fmt"
	"sort"
	"strings"

	pluginv1 "github.com/souls-guild/soul-stack/proto/plugin/gen/go/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"
)

// parseConnConfig pulls connection parameters from params. password holds
// separately from everything that falls into the events (IS-invariant ADR-010).
// `keyspace` decides whether `params.db` is carried — it is the object's own
// [object.keyspace], where the rule and its reason live (NIM-229).
//
// The `replica` object DOES have a keyspace, so SELECT succeeds on
// present/detached/offset-synced even though REPLICAOF and INFO replication are
// indifferent to which database is current. Inert is not the same as harmful, and
// dropping a value the server accepts would be a second, invisible rule to
// remember. It keeps it.
func parseConnConfig(keyspace bool, s *structpb.Struct) (connConfig, error) {
	f := s.GetFields()
	addr, _ := stringValue(f["addr"])
	if strings.TrimSpace(addr) == "" {
		return connConfig{}, fmt.Errorf("params.addr: must be a non-empty string")
	}
	db := 0
	if keyspace {
		db = intOrDefault(f["db"], 0)
	}
	return connConfig{
		addr:     addr,
		username: stringOrEmpty(f["username"]),
		password: stringOrEmpty(f["password"]),
		db:       db,
		tls:      parseTLS(f),
	}, nil
}

// validateAddr - general check for non-empty params.addr (command/config).
func validateAddr(f map[string]*structpb.Value) []string {
	if s, _ := stringValue(f["addr"]); strings.TrimSpace(s) == "" {
		return []string{"params.addr: must be a non-empty string"}
	}
	return nil
}

// redactError strips secrets from the error text. go-redis generates errors
// connection by fields *redis.Options / *tls.Config; on some paths (dial fail
// with authentication, TLS-handshake) the text can theoretically contain cradle -
// fail-safe replace the substring of each secret with "***". Variadic: called and
// with one password (redactError(err, password)), and with a password+PEM key pair
// (redactError(err, password, keyPEM) on TLS connection). Empty secrets are no-op.
func redactError(err error, secrets ...string) string {
	msg := err.Error()
	for _, s := range secrets {
		if s != "" {
			msg = strings.ReplaceAll(msg, s, "***")
		}
	}
	return msg
}

// sortedKeys - deterministic order of directives (reproducible
// output applied and stable L0-asserts).
func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// --- structpb helpers ---

func stringValue(v *structpb.Value) (string, bool) {
	if v == nil {
		return "", false
	}
	if sv, ok := v.GetKind().(*structpb.Value_StringValue); ok {
		return sv.StringValue, true
	}
	return "", false
}

// stringOrEmpty - string value or "" (for optional fields). does NOT take
// "first element" - returns the string as is or "" for a non-string/nil.
func stringOrEmpty(v *structpb.Value) string {
	s, _ := stringValue(v)
	return s
}

func stringList(v *structpb.Value) []string {
	if v == nil {
		return nil
	}
	lv, ok := v.GetKind().(*structpb.Value_ListValue)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(lv.ListValue.GetValues()))
	for _, x := range lv.ListValue.GetValues() {
		out = append(out, valueToString(x))
	}
	return out
}

// stringMap - map[string]string from structpb-map. Values are cast to string
// (CONFIG SET accepts string values; the number from YAML will become "256", etc.).
func stringMap(v *structpb.Value) map[string]string {
	if v == nil {
		return nil
	}
	sv, ok := v.GetKind().(*structpb.Value_StructValue)
	if !ok {
		return nil
	}
	fields := sv.StructValue.GetFields()
	out := make(map[string]string, len(fields))
	for k, val := range fields {
		out[k] = valueToString(val)
	}
	return out
}

// nodeSpecs - map[key -> nested struct] from structpb-map-of-maps (cluster
// nodes). The value of each key is the node specification ({addr|ip+port}); NOT
// is stringified (unlike stringMap), access to fields remains standard.
func nodeSpecs(v *structpb.Value) map[string]map[string]*structpb.Value {
	if v == nil {
		return nil
	}
	sv, ok := v.GetKind().(*structpb.Value_StructValue)
	if !ok {
		return nil
	}
	fields := sv.StructValue.GetFields()
	out := make(map[string]map[string]*structpb.Value, len(fields))
	for k, val := range fields {
		inner, ok := val.GetKind().(*structpb.Value_StructValue)
		if !ok {
			out[k] = nil
			continue
		}
		out[k] = inner.StructValue.GetFields()
	}
	return out
}

// nodeSpec - fields of one nested specification node ({addr|ip+port}) from
// structpb-map. Parallel nodeSpecs, but for ONE node (add-node: new_node/seed/
// master). nil -> empty map (caller interprets it as "not specified").
func nodeSpec(v *structpb.Value) map[string]*structpb.Value {
	if v == nil {
		return nil
	}
	sv, ok := v.GetKind().(*structpb.Value_StructValue)
	if !ok {
		return nil
	}
	return sv.StructValue.GetFields()
}

// boolOrDefault and intOrDefault read a DECLARED top-level param, and their last
// arm — a value of another type reading as the default — is unreachable through
// the plugin's own entry points: [object.Apply] and [object.Validate] refuse such a
// value against the declaration before either is called (params.go, NIM-778). That
// arm is what made `tls: "true"` a plaintext connection, so it is left here only
// because a total function still needs one, not because any caller may rely on it.
// A key these cannot reach — one nested inside a map-typed param — is read with
// [intField], which refuses instead.
func boolOrDefault(v *structpb.Value, def bool) bool {
	if v == nil {
		return def
	}
	if bv, ok := v.GetKind().(*structpb.Value_BoolValue); ok {
		return bv.BoolValue
	}
	return def
}

func intOrDefault(v *structpb.Value, def int) int {
	if v == nil {
		return def
	}
	if nv, ok := v.GetKind().(*structpb.Value_NumberValue); ok {
		return int(nv.NumberValue)
	}
	return def
}

// valueToString casts an arbitrary structpb.Value to a string. Numbers without
// fractional part - without ".000000" (256, not 256.000000): config directives and args
// often numeric in YAML, Redis expects a string.
func valueToString(v *structpb.Value) string {
	if v == nil {
		return ""
	}
	switch k := v.GetKind().(type) {
	case *structpb.Value_StringValue:
		return k.StringValue
	case *structpb.Value_NumberValue:
		if k.NumberValue == float64(int64(k.NumberValue)) {
			return fmt.Sprintf("%d", int64(k.NumberValue))
		}
		return fmt.Sprintf("%v", k.NumberValue)
	case *structpb.Value_BoolValue:
		return fmt.Sprintf("%t", k.BoolValue)
	default:
		return ""
	}
}

// --- ApplyEvent helpers ---

func sendOutcome(stream grpc.ServerStreamingServer[pluginv1.ApplyEvent], changed bool, message string, output map[string]any) error {
	out, err := structpb.NewStruct(output)
	if err != nil {
		return fmt.Errorf("build output struct: %w", err)
	}
	return stream.Send(&pluginv1.ApplyEvent{Message: message, Changed: changed, Output: out})
}

func sendFailure(stream grpc.ServerStreamingServer[pluginv1.ApplyEvent], message string) error {
	return stream.Send(&pluginv1.ApplyEvent{Message: message, Failed: true})
}
