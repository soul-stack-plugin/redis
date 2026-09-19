package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	pluginv1 "github.com/souls-guild/soul-stack/proto/plugin/gen/go/v1"
	"github.com/souls-guild/soul-stack/sdk/module"
	"google.golang.org/protobuf/types/known/structpb"
)

// --- harness ---

// refusingModule is a RedisModule whose connect FAILS THE TEST. Every guard below
// asserts a refusal, and a refusal that still opened the socket is the bug: the
// value being refused is the one deciding whether that socket carries TLS.
func refusingModule(t *testing.T) *RedisModule {
	t.Helper()
	return &RedisModule{
		connect: func(_ context.Context, cfg connConfig) (redisConn, error) {
			t.Errorf("a refused param must not reach a connection (addr %q, tls=%v)", cfg.addr, cfg.tls.enabled)
			return nil, errors.New("must not connect")
		},
	}
}

// bundleObjects returns the seven objects as the artifact serves them, built over
// m. Going through redisBundle rather than calling m.acl() and friends by hand is
// the point: it is the path production takes, so a declaration this test reads is
// the declaration Apply enforces.
func bundleObjects(m *RedisModule) map[string]*object {
	out := make(map[string]*object)
	for _, d := range redisBundle(m).Modules {
		out[d.Name] = d.Impl.(*object)
	}
	return out
}

func applyObject(o *object, state string, params map[string]any) (*applyStream, *pluginv1.ApplyEvent) {
	stream := &applyStream{}
	s, _ := structpb.NewStruct(params)
	_ = o.Apply(&pluginv1.ApplyRequest{State: state, Params: s}, stream)
	return stream, stream.final()
}

// --- the subject: tls as a string ---

// TestApply_StringTLSRefusedNotCoerced is NIM-778 itself. `tls: "true"` used to
// read as false and the connection — password and all — went out in plaintext with
// the step reporting success. The refusal must name the parameter, and nothing may
// dial.
func TestApply_StringTLSRefusedNotCoerced(t *testing.T) {
	objs := bundleObjects(refusingModule(t))
	for _, tc := range []struct{ object, state string }{
		{"acl", "reloaded"},
		{"command", "run"},
		{"instance", "pinged"},
		{"replica", "present"},
		{"sentinel", "monitored"},
		{"user", "present"},
		{"cluster", "created"},
	} {
		t.Run(tc.object+"."+tc.state, func(t *testing.T) {
			_, fin := applyObject(objs[tc.object], tc.state, map[string]any{
				"addr":     "127.0.0.1:6379",
				"password": "s3cr3t",
				"tls":      "true",
			})
			if fin == nil || !fin.Failed {
				t.Fatalf(`waited failed=true on tls: "true", got %+v`, fin)
			}
			if !strings.Contains(fin.GetMessage(), "params.tls:") {
				t.Errorf("the refusal must address the parameter, got %q", fin.GetMessage())
			}
			if strings.Contains(fin.GetMessage(), "s3cr3t") {
				t.Errorf("the refusal leaked the password: %q", fin.GetMessage())
			}
		})
	}
}

// TestValidate_StringTLSRefused — the same refusal on the other entry point. Apply
// is the one that matters (the runtime does not call Validate), but an author who
// runs soul-lint should be told there too.
func TestValidate_StringTLSRefused(t *testing.T) {
	o := bundleObjects(&RedisModule{})["acl"]
	reply, err := o.Validate(context.Background(), &pluginv1.ValidateRequest{
		State:  "reloaded",
		Params: mustStruct(t, map[string]any{"addr": "127.0.0.1:6379", "tls": "true"}),
	})
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if reply.GetOk() {
		t.Fatal(`waited ok=false on tls: "true"`)
	}
	if !strings.Contains(strings.Join(reply.GetErrors(), "; "), "params.tls:") {
		t.Errorf("the refusal must address the parameter, got %q", reply.GetErrors())
	}
}

// TestApply_BooleanTLSStillConnectsOverTLS — the other half of the acceptance: the
// fix refuses a wrong TYPE, it does not refuse TLS. A real boolean still reaches
// the connection as enabled.
func TestApply_BooleanTLSStillConnectsOverTLS(t *testing.T) {
	conn := &fakeConn{}
	o := bundleObjects(newModule(conn))["acl"]
	_, fin := applyObject(o, "reloaded", map[string]any{
		"addr": "127.0.0.1:6379",
		"tls":  true,
	})
	if fin == nil || fin.Failed {
		t.Fatalf("waited a successful apply on tls: true, got %+v", fin)
	}
	if !conn.cfg.tls.enabled {
		t.Error("tls: true must reach the connection as enabled")
	}
}

// TestApply_BooleanTLSFalseStaysPlaintext — and it does not refuse an installation
// that has no TLS: false is a boolean and means what it says.
func TestApply_BooleanTLSFalseStaysPlaintext(t *testing.T) {
	conn := &fakeConn{}
	o := bundleObjects(newModule(conn))["acl"]
	_, fin := applyObject(o, "reloaded", map[string]any{
		"addr": "127.0.0.1:6379",
		"tls":  false,
	})
	if fin == nil || fin.Failed {
		t.Fatalf("waited a successful apply on tls: false, got %+v", fin)
	}
	if conn.cfg.tls.enabled {
		t.Error("tls: false must stay plaintext")
	}
}

// --- the coverage guard, derived from the declarations ---

// TestApply_NoDeclaredBoolOrIntCoerces walks what the artifact PUBLISHES — every
// object, every action, every parameter declared bool or int — and feeds each one a
// string. This is the guard that outlives the ticket: NIM-778 exists because
// `persist` was made strict by hand and `tls` beside it was not, and a rule checked
// per parameter can always be forgotten on the next one. Here a parameter added
// later is covered the moment it is declared.
func TestApply_NoDeclaredBoolOrIntCoerces(t *testing.T) {
	objs := bundleObjects(refusingModule(t))
	names := make([]string, 0, len(objs))
	for name := range objs {
		names = append(names, name)
	}
	sort.Strings(names)

	covered := make(map[string]int, len(objs))
	for _, objName := range names {
		o := objs[objName]
		for _, state := range o.states() {
			decl := o.decl[state]
			params := make([]string, 0, len(decl.Input))
			for p := range decl.Input {
				params = append(params, p)
			}
			sort.Strings(params)

			for _, param := range params {
				switch decl.Input[param].Type {
				case module.Bool, module.Boolean, module.Int, module.Integer:
				default:
					continue
				}
				covered[objName]++
				t.Run(fmt.Sprintf("%s.%s/%s", objName, state, param), func(t *testing.T) {
					_, fin := applyObject(o, state, map[string]any{
						"addr": "127.0.0.1:6379",
						param:  "1",
					})
					if fin == nil || !fin.Failed {
						t.Fatalf("waited failed=true on a string %s, got %+v", param, fin)
					}
					if !strings.Contains(fin.GetMessage(), "params."+param+":") {
						t.Errorf("the refusal must address the parameter, got %q", fin.GetMessage())
					}
				})
			}
		}
	}
	// The walk is only a guard while it walks something: an object whose
	// declaration silently went missing contributes no subtests at all, and every
	// assertion above would then be vacuous FOR THAT OBJECT while the test still
	// passed on the other six.
	//
	// Per object rather than against a total, deliberately. A floor on the total
	// does not bind — dropping the four params of `command` from a count of 64
	// lands on 60, which passes any floor a reader would have written — and an
	// exact total would fail every time a parameter is legitimately added, which
	// teaches the next author to raise the number rather than read the failure.
	for _, objName := range names {
		if covered[objName] == 0 {
			t.Errorf("object %q contributed no bool/int parameter — its declaration is not reaching the gate", objName)
		}
	}
}

// TestObjectsDeclareEveryActionTheyServe is the wiring guard. object.decl is what
// Apply checks against, and an action missing from it is checked against an empty
// Input — a gate that passes everything, silently.
func TestObjectsDeclareEveryActionTheyServe(t *testing.T) {
	for name, o := range bundleObjects(&RedisModule{}) {
		for _, state := range o.states() {
			decl, ok := o.decl[state]
			if !ok {
				t.Errorf("%s.%s: served but not declared — Apply would check it against nothing", name, state)
				continue
			}
			if len(decl.Input) == 0 {
				t.Errorf("%s.%s: declares no parameters", name, state)
			}
		}
	}
}

// TestApply_FractionalIntRefused — an int is not merely a number. Truncating 7.5 to
// 7 is the same guess by another name, and `db: 7.5` would silently address a
// different keyspace than the one written.
func TestApply_FractionalIntRefused(t *testing.T) {
	o := bundleObjects(refusingModule(t))["acl"]
	_, fin := applyObject(o, "reloaded", map[string]any{
		"addr": "127.0.0.1:6379",
		"db":   7.5,
	})
	if fin == nil || !fin.Failed {
		t.Fatalf("waited failed=true on db: 7.5, got %+v", fin)
	}
	if !strings.Contains(fin.GetMessage(), "params.db:") {
		t.Errorf("the refusal must address the parameter, got %q", fin.GetMessage())
	}
}

// TestApply_UndeclaredAndAbsentParamsAreLeftAlone — the gate answers one question.
// An unknown key is the engine's refusal to make (unknown_param, NIM-204) and an
// absent one is what a default is for; neither is this check's business, and a gate
// that refused them would break every scenario passing a documented default.
func TestApply_UndeclaredAndAbsentParamsAreLeftAlone(t *testing.T) {
	conn := &fakeConn{}
	o := bundleObjects(newModule(conn))["acl"]
	_, fin := applyObject(o, "reloaded", map[string]any{
		"addr":             "127.0.0.1:6379",
		"not_a_real_param": "whatever",
	})
	if fin == nil || fin.Failed {
		t.Fatalf("waited a successful apply, got %+v", fin)
	}
}

// TestParamTypeMismatch_EveryDeclarableType covers all ten members of the closed
// set in sdk/schema, not the five this artifact happens to use.
//
// The five synonyms (`integer`/`number`/`boolean`/`array`/`object`) are reachable
// by any author writing `module.Integer` where the rest of this file writes
// `module.Int`, and an arm that answered the wrong question there would fail OPEN —
// a parameter silently exempt from the whole rule. Untested, that is invisible;
// this file's own doc invites it to be copied into another plugin.
func TestParamTypeMismatch_EveryDeclarableType(t *testing.T) {
	values := map[string]*structpb.Value{
		"bool":   structpb.NewBoolValue(true),
		"int":    structpb.NewNumberValue(7),
		"frac":   structpb.NewNumberValue(7.5),
		"string": structpb.NewStringValue("x"),
		"list":   structpb.NewListValue(&structpb.ListValue{}),
		"map":    structpb.NewStructValue(&structpb.Struct{}),
	}
	// For each declared type: the value kinds it must ACCEPT, and one it must not.
	for _, tc := range []struct {
		declared       module.ParamType
		accepts, wants string
	}{
		{module.Bool, "bool", "string"},
		{module.Boolean, "bool", "string"},
		{module.Int, "int", "string"},
		{module.Integer, "int", "frac"},
		{module.Number, "frac", "string"},
		{module.String, "string", "int"},
		{module.List, "list", "map"},
		{module.Array, "list", "string"},
		{module.Map, "map", "list"},
		{module.Object, "map", "string"},
	} {
		t.Run(string(tc.declared), func(t *testing.T) {
			if _, ok := paramTypeMismatch(tc.declared, values[tc.accepts]); !ok {
				t.Errorf("declared %q must accept a %s value", tc.declared, tc.accepts)
			}
			if _, ok := paramTypeMismatch(tc.declared, values[tc.wants]); ok {
				t.Errorf("declared %q must REFUSE a %s value — it fails open", tc.declared, tc.wants)
			}
		})
	}
}

// TestCheckParamTypes_NullReadsAsAbsent — `tls:` with nothing after it arrives as a
// null and means unset, not "a value of the wrong type".
func TestCheckParamTypes_NullReadsAsAbsent(t *testing.T) {
	decl := module.Input{"tls": {Type: module.Bool, Default: false}}
	errs := checkParamTypes(decl, map[string]*structpb.Value{"tls": structpb.NewNullValue()})
	if len(errs) != 0 {
		t.Errorf("a null must read as absent, got %q", errs)
	}
}

// --- nested specs, which the declaration cannot reach ---

// TestValidate_StringQuorumRefused — `monitor` is declared a map and nothing
// declares what is inside it, so the gate reaches the map and stops. A coerced
// `quorum: "2"` fell back to 1, and a Sentinel quorum of 1 is a single vote
// deciding a failover.
func TestValidate_StringQuorumRefused(t *testing.T) {
	o := bundleObjects(&RedisModule{})["sentinel"]
	reply, err := o.Validate(context.Background(), &pluginv1.ValidateRequest{
		State: "monitored",
		Params: mustStruct(t, map[string]any{
			"addr":        "127.0.0.1:26379",
			"master_name": "mymaster",
			"monitor":     map[string]any{"ip": "10.0.0.1", "port": 6379.0, "quorum": "2"},
		}),
	})
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if reply.GetOk() {
		t.Fatal(`waited ok=false on monitor.quorum: "2"`)
	}
	if !strings.Contains(strings.Join(reply.GetErrors(), "; "), "params.monitor.quorum:") {
		t.Errorf("the refusal must address the parameter, got %q", reply.GetErrors())
	}
}

// TestResolveNodeEndpoint_StringPortRefused — a node spec is nested the same way. A
// coerced port read as 0 and fell through to the addr branch, dropping the ip+port
// the author wrote.
//
// The assertion is on the ADDRESS in the message, not merely on there being an
// error: the coercion also ended in an error here, but one blaming a missing addr,
// which points an author at a key they did write and away from the one they got
// wrong. That difference is the whole fix on this path.
func TestResolveNodeEndpoint_StringPortRefused(t *testing.T) {
	spec := map[string]*structpb.Value{
		"ip":   structpb.NewStringValue("10.0.0.1"),
		"port": structpb.NewStringValue("6379"),
	}
	_, _, _, err := resolveNodeEndpoint(spec)
	if err == nil {
		t.Fatal(`waited an error on port: "6379"`)
	}
	if !strings.Contains(err.Error(), "port:") {
		t.Errorf("the refusal must address the port, got %q", err)
	}
}

// --- the second half: TLS that cannot come up must not degrade ---

// plaintextSniffer is a TCP listener that speaks no TLS and records every byte it
// is handed, so a test can ask the one question that matters: did the credential
// reach the wire in the clear.
//
// It accepts REPEATEDLY, and that is load-bearing rather than tidiness. A
// degradation is a second dial — a client that retries in the clear after the TLS
// attempt fails — and a sniffer holding one connection would miss exactly the
// behaviour it exists to catch, refusing the retry at the TCP level and reporting a
// clean wire.
type plaintextSniffer struct {
	addr string
	ln   net.Listener
	wg   sync.WaitGroup

	mu   sync.Mutex
	seen []byte
}

func newPlaintextSniffer(t *testing.T) *plaintextSniffer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &plaintextSniffer{addr: ln.Addr().String(), ln: ln}
	// Close on the way out too: wire() is not reached on a t.Fatal, and the
	// accept loop would outlive the test holding a port.
	t.Cleanup(func() { _ = ln.Close() })
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			s.wg.Add(1)
			go s.read(conn)
		}
	}()
	return s
}

// sniffIdle is how long a reader waits for a client that has stopped talking.
//
// It is an IDLE window, refreshed after every read, not a budget for the
// exchange: a client is free to take as long as it likes as long as it keeps
// sending. An absolute deadline here cost the whole of it on every run — nothing
// closes the client socket in the control case, so the reader sat until the
// deadline expired with the answer already in hand.
const sniffIdle = 500 * time.Millisecond

func (s *plaintextSniffer) read(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()
	buf := make([]byte, 4096)
	for {
		_ = conn.SetReadDeadline(time.Now().Add(sniffIdle))
		n, err := conn.Read(buf)
		if n > 0 {
			s.mu.Lock()
			s.seen = append(s.seen, buf[:n]...)
			s.mu.Unlock()
			// Refuse everything: this is not a Redis, and what the client does
			// after being refused is not what is under test.
			if _, werr := conn.Write([]byte("-ERR not a redis\r\n")); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// wire stops listening and returns every byte that reached the socket.
func (s *plaintextSniffer) wire() string {
	_ = s.ln.Close()
	s.wg.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	return string(s.seen)
}

// TestDefaultConnect_TLSDoesNotDegradeToPlaintext is the half of NIM-778 that
// parsing cannot cause. Even with an honest `tls: true`, a connection that answers
// no TLS must FAIL — a client that retried in the clear, or a config the driver
// dropped, would put the password on the wire exactly as the coercion did.
//
// The control case below is what makes this assertion mean something: against the
// same listener, a plaintext connect DOES put the password on the wire, so the
// sniffer is looking at the right bytes.
func TestDefaultConnect_TLSDoesNotDegradeToPlaintext(t *testing.T) {
	const password = "pa55word-on-the-wire"

	s := newPlaintextSniffer(t)
	conn, err := defaultConnect(context.Background(), connConfig{
		addr:     s.addr,
		password: password,
		tls:      tlsParams{enabled: true, skipVerify: true},
	})
	if err == nil {
		_ = conn.Close()
		t.Fatal("a TLS connect against a server that speaks no TLS must fail, not degrade")
	}
	if wire := s.wire(); strings.Contains(wire, password) {
		t.Fatalf("the password reached the wire in the clear after tls: true (%d bytes seen)", len(wire))
	}
}

func TestDefaultConnect_PlaintextDoesPutThePasswordOnTheWire(t *testing.T) {
	const password = "pa55word-on-the-wire"

	s := newPlaintextSniffer(t)
	conn, err := defaultConnect(context.Background(), connConfig{
		addr:     s.addr,
		password: password,
	})
	if err == nil {
		_ = conn.Close()
	}
	if wire := s.wire(); !strings.Contains(wire, password) {
		t.Fatalf("control case: a plaintext connect must show the password to the sniffer, saw %q", wire)
	}
}
