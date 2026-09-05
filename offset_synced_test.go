package main

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"

	pluginv1 "github.com/souls-guild/soul-stack/proto/plugin/gen/go/v1"
)

// offsetConn - fake redisConn for offset-synced: responds to INFO replication
// by a given section and on DBSIZE by a given number; other verb -> "OK". Writes calls.
type offsetConn struct {
	cfg       connConfig
	infoReply string // reply INFO replication
	dbsize    int64  // answer DBSIZE
	calls     [][]any
	closed    bool
	failVerb  string // non-empty -> error for this verb (args[0])
}

func (c *offsetConn) Do(_ context.Context, args ...any) (string, error) {
	c.calls = append(c.calls, args)
	verb, _ := args[0].(string)
	if c.failVerb != "" && strings.EqualFold(verb, c.failVerb) {
		return "", errors.New("redis error on " + verb)
	}
	switch {
	case strings.EqualFold(verb, "INFO"):
		return c.infoReply, nil
	case strings.EqualFold(verb, "DBSIZE"):
		return strconv.FormatInt(c.dbsize, 10), nil
	default:
		return "OK", nil
	}
}

func (c *offsetConn) ConfigGet(_ context.Context, p string) (map[string]string, error) {
	return map[string]string{p: ""}, nil
}
func (c *offsetConn) GetKeysInSlot(_ context.Context, _, _ int) ([]string, error) { return nil, nil }
func (c *offsetConn) AclList(_ context.Context) ([]string, error)                 { return nil, nil }
func (c *offsetConn) Close() error                                                { c.closed = true; return nil }

// offsetModule collects RedisModule, routing connection by cfg.addr: selfAddr ->
// self, otherwise -> source. This is how applyOffsetSynced gets DIFFERENT instances for its
// addr and for source_addr (second connection).
func offsetModule(selfAddr string, self, source *offsetConn) *RedisModule {
	return &RedisModule{
		connect: func(_ context.Context, cfg connConfig) (redisConn, error) {
			if cfg.addr == selfAddr {
				self.cfg = cfg
				return self, nil
			}
			source.cfg = cfg
			return source, nil
		},
	}
}

// --- Validate: offset-synced ---

func TestValidate_OffsetSyncedRequiresAddrAndSource(t *testing.T) {
	m := &RedisModule{}
	// no source_addr
	r1, _ := m.replica().Validate(context.Background(), &pluginv1.ValidateRequest{
		State:  "offset-synced",
		Params: mustStruct(t, map[string]any{"addr": "127.0.0.1:6379"}),
	})
	if r1.Ok {
		t.Error("expected Ok=false without source_addr")
	}
	// no addr
	r2, _ := m.replica().Validate(context.Background(), &pluginv1.ValidateRequest{
		State:  "offset-synced",
		Params: mustStruct(t, map[string]any{"source_addr": "203.0.113.5:6379"}),
	})
	if r2.Ok {
		t.Error("waited Ok=false without addr")
	}
}

func TestValidate_OffsetSyncedHappyPath(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.replica().Validate(context.Background(), &pluginv1.ValidateRequest{
		State: "offset-synced",
		Params: mustStruct(t, map[string]any{
			"addr":        "127.0.0.1:6379",
			"source_addr": "203.0.113.5:6379",
		}),
	})
	if !reply.Ok || len(reply.Errors) != 0 {
		t.Fatalf("waited Ok=true without errors, got %+v", reply)
	}
}

func TestValidate_OffsetSyncedRejectsNegativeLag(t *testing.T) {
	m := &RedisModule{}
	reply, _ := m.replica().Validate(context.Background(), &pluginv1.ValidateRequest{
		State: "offset-synced",
		Params: mustStruct(t, map[string]any{
			"addr":          "127.0.0.1:6379",
			"source_addr":   "203.0.113.5:6379",
			"lag_threshold": -1,
		}),
	})
	if reply.Ok {
		t.Fatal("waited Ok=false for negative lag_threshold")
	}
}

// --- Apply offset-synced ---

// selfReplInfo - INFO replication of your instance (replica).
func selfReplInfo(slaveOffset int, link string, syncInProgress int) string {
	return "# Replication\r\n" +
		"role:slave\r\n" +
		"master_host:203.0.113.5\r\n" +
		"master_link_status:" + link + "\r\n" +
		"master_sync_in_progress:" + strconv.Itoa(syncInProgress) + "\r\n" +
		"slave_repl_offset:" + strconv.Itoa(slaveOffset) + "\r\n"
}

// sourceReplInfo - INFO replication of the external master source.
func sourceReplInfo(masterOffset int) string {
	return "# Replication\r\n" +
		"role:master\r\n" +
		"master_repl_offset:" + strconv.Itoa(masterOffset) + "\r\n"
}

const (
	selfAddr   = "127.0.0.1:6379"
	sourceAddr = "203.0.113.5:6379"
)

func applyOffset(t *testing.T, m *RedisModule, extra map[string]any) *pluginv1.ApplyEvent {
	t.Helper()
	params := map[string]any{
		"addr":            selfAddr,
		"source_addr":     sourceAddr,
		"password":        secretPass,
		"source_password": masterSourcePass,
	}
	for k, v := range extra {
		params[k] = v
	}
	stream := &applyStream{}
	if err := m.replica().Apply(&pluginv1.ApplyRequest{State: "offset-synced", Params: mustStruct(t, params)}, stream); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	return stream.final()
}

// TestApplyOffsetSynced_CaughtUp - link up + sync done + lag<=thr -> caught_up=true,
// changed=false is constructive. Main happy-path safety-gate.
func TestApplyOffsetSynced_CaughtUp(t *testing.T) {
	self := &offsetConn{infoReply: selfReplInfo(1000, "up", 0), dbsize: 42}
	source := &offsetConn{infoReply: sourceReplInfo(1000), dbsize: 42}
	m := offsetModule(selfAddr, self, source)

	fin := applyOffset(t, m, map[string]any{"skip_checksum": true})
	if fin == nil || fin.Failed {
		t.Fatalf("expected success, got %+v", fin)
	}
	if fin.Changed {
		t.Error("offset-synced: changed must be false by design (probe)")
	}
	out := fin.GetOutput().GetFields()
	if !out["caught_up"].GetBoolValue() {
		t.Error("waited caught_up=true (link up + sync done + lag 0)")
	}
	if got := out["lag_bytes"].GetNumberValue(); got != 0 {
		t.Errorf("lag_bytes=%v, waited 0 (offset equal)", got)
	}
	if out["master_sync_in_progress"].GetBoolValue() {
		t.Error("master_sync_in_progress=true, waited false")
	}
}

// TestApplyOffsetSynced_LagExceedsThreshold - lag > lag_threshold -> caught_up=false
// (link up, sync done, but the data has not yet been caught up). Key case gate.
func TestApplyOffsetSynced_LagExceedsThreshold(t *testing.T) {
	// master is 500 bytes ahead, threshold 100 -> not caught up.
	self := &offsetConn{infoReply: selfReplInfo(1000, "up", 0)}
	source := &offsetConn{infoReply: sourceReplInfo(1500)}
	m := offsetModule(selfAddr, self, source)

	fin := applyOffset(t, m, map[string]any{"skip_checksum": true, "lag_threshold": 100})
	out := fin.GetOutput().GetFields()
	if out["caught_up"].GetBoolValue() {
		t.Error("waited caught_up=false: lag 500 > threshold 100")
	}
	if got := out["lag_bytes"].GetNumberValue(); got != 500 {
		t.Errorf("lag_bytes=%v, waited 500", got)
	}
}

// TestApplyOffsetSynced_LagWithinThreshold - lag <= lag_threshold -> caught_up=true
// (lag tolerance is accepted by the operator via lag_threshold).
func TestApplyOffsetSynced_LagWithinThreshold(t *testing.T) {
	self := &offsetConn{infoReply: selfReplInfo(1000, "up", 0)}
	source := &offsetConn{infoReply: sourceReplInfo(1050)} // lag 50
	m := offsetModule(selfAddr, self, source)

	fin := applyOffset(t, m, map[string]any{"skip_checksum": true, "lag_threshold": 100})
	if !fin.GetOutput().GetFields()["caught_up"].GetBoolValue() {
		t.Error("waited caught_up=true: lag 50 <= threshold 100")
	}
}

// TestApplyOffsetSynced_SyncInProgress - master_sync_in_progress==1 -> caught_up=false
// EVEN if the offset agrees (full-sync is in progress, the data is not consistent).
func TestApplyOffsetSynced_SyncInProgress(t *testing.T) {
	self := &offsetConn{infoReply: selfReplInfo(1000, "up", 1)} // sync is in progress
	source := &offsetConn{infoReply: sourceReplInfo(1000)}      // offset agreed
	m := offsetModule(selfAddr, self, source)

	fin := applyOffset(t, m, map[string]any{"skip_checksum": true})
	out := fin.GetOutput().GetFields()
	if out["caught_up"].GetBoolValue() {
		t.Error("waited caught_up=false: master_sync_in_progress==1 (full-sync in progress)")
	}
	if !out["master_sync_in_progress"].GetBoolValue() {
		t.Error("expected master_sync_in_progress=true in Output")
	}
}

// TestApplyOffsetSynced_LinkDown - master_link_status:down -> caught_up=false.
func TestApplyOffsetSynced_LinkDown(t *testing.T) {
	self := &offsetConn{infoReply: selfReplInfo(1000, "down", 0)}
	source := &offsetConn{infoReply: sourceReplInfo(1000)}
	m := offsetModule(selfAddr, self, source)

	fin := applyOffset(t, m, map[string]any{"skip_checksum": true})
	if fin.GetOutput().GetFields()["caught_up"].GetBoolValue() {
		t.Error("waited caught_up=false: master_link_status:down")
	}
}

// TestApplyOffsetSynced_ChecksumDBSize - !skip_checksum -> DBSIZE of both in Output.
func TestApplyOffsetSynced_ChecksumDBSize(t *testing.T) {
	self := &offsetConn{infoReply: selfReplInfo(1000, "up", 0), dbsize: 40}
	source := &offsetConn{infoReply: sourceReplInfo(1000), dbsize: 42}
	m := offsetModule(selfAddr, self, source)

	fin := applyOffset(t, m, nil) // skip_checksum=false (default)
	out := fin.GetOutput().GetFields()
	if got := out["dbsize_source"].GetNumberValue(); got != 42 {
		t.Errorf("dbsize_source=%v, waited 42", got)
	}
	if got := out["dbsize_replica"].GetNumberValue(); got != 40 {
		t.Errorf("dbsize_replica=%v, waited 40", got)
	}
	// DBSIZE is called on both connections.
	if !hasCall(source.calls, "DBSIZE") {
		t.Errorf("DBSIZE not called on source: %v", source.calls)
	}
	if !hasCall(self.calls, "DBSIZE") {
		t.Errorf("DBSIZE not called on replica: %v", self.calls)
	}
}

// TestApplyOffsetSynced_SkipChecksum - skip_checksum=true -> DBSIZE is NOT called,
// There are no dbsize fields in Output.
func TestApplyOffsetSynced_SkipChecksum(t *testing.T) {
	self := &offsetConn{infoReply: selfReplInfo(1000, "up", 0)}
	source := &offsetConn{infoReply: sourceReplInfo(1000)}
	m := offsetModule(selfAddr, self, source)

	fin := applyOffset(t, m, map[string]any{"skip_checksum": true})
	if hasCall(self.calls, "DBSIZE") || hasCall(source.calls, "DBSIZE") {
		t.Errorf("skip_checksum: DBSIZE should not be called: self=%v source=%v", self.calls, source.calls)
	}
	if _, ok := fin.GetOutput().GetFields()["dbsize_source"]; ok {
		t.Error("skip_checksum: dbsize_source should not be in Output")
	}
}

// TestApplyOffsetSynced_SecondConnectUsesSourceSecrets - the second connection goes to
// source_addr with source_password (NOT your password). Proves the isolation of creds.
func TestApplyOffsetSynced_SecondConnectUsesSourceSecrets(t *testing.T) {
	self := &offsetConn{infoReply: selfReplInfo(1000, "up", 0)}
	source := &offsetConn{infoReply: sourceReplInfo(1000)}
	m := offsetModule(selfAddr, self, source)

	_ = applyOffset(t, m, map[string]any{"skip_checksum": true})
	if source.cfg.addr != sourceAddr {
		t.Errorf("the second connection did not go to source_addr: %q", source.cfg.addr)
	}
	if source.cfg.password != masterSourcePass {
		t.Errorf("the second connection does not carry source_password: %q", source.cfg.password)
	}
	if self.cfg.password != secretPass {
		t.Errorf("the first connection does not carry its own password: %q", self.cfg.password)
	}
	if !source.closed {
		t.Error("the second connection (source) is not closed")
	}
}

// TestApplyOffsetSynced_SourceTLS_SecondConnectUsesTLS - source_tls=true:
// The SECOND connection (to an external source) goes via TLS with source_tls_ca, and the connection to
// YOUR OWN instance - according to its tls parameters. Proves brought TLS-to-origin
// to offset-synced: source-secrets and source-TLS are isolated from their own.
func TestApplyOffsetSynced_SourceTLS_SecondConnectUsesTLS(t *testing.T) {
	self := &offsetConn{infoReply: selfReplInfo(1000, "up", 0)}
	source := &offsetConn{infoReply: sourceReplInfo(1000)}
	m := offsetModule(selfAddr, self, source)

	const sourceCA = "-----BEGIN CERTIFICATE-----\nSOURCE-CA-OFFSET\n-----END CERTIFICATE-----"
	_ = applyOffset(t, m, map[string]any{
		"skip_checksum": true,
		"source_tls":    true,
		"source_tls_ca": sourceCA,
	})

	// Second connection (source) - TLS is enabled, source CA is forwarded.
	if !source.cfg.tls.enabled {
		t.Error("source_tls=true did not reach the second connection (TLS to the source is not enabled)")
	}
	if source.cfg.tls.caPEM != sourceCA {
		t.Errorf("source_tls_ca did not reach the second connection: %q", source.cfg.tls.caPEM)
	}
	// Your connection should NOT inherit source-TLS (isolation): tls is not specified -> false.
	if self.cfg.tls.enabled {
		t.Error("your connection mistakenly enabled TLS from source_tls (no isolation)")
	}
}

// TestApplyOffsetSynced_OwnTLSIndependentOfSource - custom tls=true and source_tls=true
// are read from DIFFERENT fields: tls/tls_ca takes its connection, the source is
// source_tls/source_tls_ca. Proves the separability of two TLS contexts.
func TestApplyOffsetSynced_OwnTLSIndependentOfSource(t *testing.T) {
	self := &offsetConn{infoReply: selfReplInfo(1000, "up", 0)}
	source := &offsetConn{infoReply: sourceReplInfo(1000)}
	m := offsetModule(selfAddr, self, source)

	const ownCA = "-----BEGIN CERTIFICATE-----\nOWN-CA\n-----END CERTIFICATE-----"
	const srcCA = "-----BEGIN CERTIFICATE-----\nSRC-CA\n-----END CERTIFICATE-----"
	_ = applyOffset(t, m, map[string]any{
		"skip_checksum": true,
		"tls":           true,
		"tls_ca":        ownCA,
		"source_tls":    true,
		"source_tls_ca": srcCA,
	})

	if !self.cfg.tls.enabled || self.cfg.tls.caPEM != ownCA {
		t.Errorf("your connection: expected tls with ownCA, got enabled=%v ca=%q", self.cfg.tls.enabled, self.cfg.tls.caPEM)
	}
	if !source.cfg.tls.enabled || source.cfg.tls.caPEM != srcCA {
		t.Errorf("source connection: expected tls with srcCA, got enabled=%v ca=%q", source.cfg.tls.enabled, source.cfg.tls.caPEM)
	}
}

// TestApplyOffsetSynced_NoSourceTLS_SecondConnectPlaintext - without source_tls
// second plaintext connection (back-compat: tls=false -> enabled=false).
func TestApplyOffsetSynced_NoSourceTLS_SecondConnectPlaintext(t *testing.T) {
	self := &offsetConn{infoReply: selfReplInfo(1000, "up", 0)}
	source := &offsetConn{infoReply: sourceReplInfo(1000)}
	m := offsetModule(selfAddr, self, source)

	_ = applyOffset(t, m, map[string]any{"skip_checksum": true})
	if source.cfg.tls.enabled {
		t.Error("without source_tls the second connection should not include TLS")
	}
}

// TestApplyOffsetSynced_NoSecretLeak - neither your password nor the source password is leaked to
// events (IS-invariant ADR-010).
func TestApplyOffsetSynced_NoSecretLeak(t *testing.T) {
	self := &offsetConn{infoReply: selfReplInfo(1000, "up", 0), dbsize: 5}
	source := &offsetConn{infoReply: sourceReplInfo(1000), dbsize: 5}
	m := offsetModule(selfAddr, self, source)

	params := map[string]any{
		"addr":            selfAddr,
		"source_addr":     sourceAddr,
		"password":        secretPass,
		"source_password": masterSourcePass,
	}
	stream := &applyStream{}
	_ = m.replica().Apply(&pluginv1.ApplyRequest{State: "offset-synced", Params: mustStruct(t, params)}, stream)

	assertEventsNoSecret(t, stream)
	for i, ev := range stream.sent {
		if strings.Contains(ev.GetMessage(), masterSourcePass) {
			t.Errorf("event[%d].Message carries source_password: %q", i, ev.GetMessage())
		}
	}
}

// TestApplyOffsetSynced_SourceConnectFailure_DoesNotLeak - connection to the source
// fell, the text contains source_password -> sanitized.
func TestApplyOffsetSynced_SourceConnectFailure_DoesNotLeak(t *testing.T) {
	self := &offsetConn{infoReply: selfReplInfo(1000, "up", 0)}
	m := &RedisModule{
		connect: func(_ context.Context, cfg connConfig) (redisConn, error) {
			if cfg.addr == selfAddr {
				self.cfg = cfg
				return self, nil
			}
			return nil, errors.New("dial failed for AUTH " + cfg.password) // source password in text
		},
	}
	stream := &applyStream{}
	_ = m.replica().Apply(&pluginv1.ApplyRequest{
		State: "offset-synced",
		Params: mustStruct(t, map[string]any{
			"addr":            selfAddr,
			"source_addr":     sourceAddr,
			"password":        secretPass,
			"source_password": masterSourcePass,
		}),
	}, stream)

	fin := stream.final()
	if fin == nil || !fin.Failed {
		t.Fatalf("expected failed=true on the source connection file, got %+v", fin)
	}
	assertEventsNoSecret(t, stream)
	for i, ev := range stream.sent {
		if strings.Contains(ev.GetMessage(), masterSourcePass) {
			t.Errorf("event[%d].Message carries source_password for connection failure: %q", i, ev.GetMessage())
		}
	}
}

// TestApplyOffsetSynced_MissingOffset_NotCaughtUp - not on your INFO
// slave_repl_offset (abnormal input: addr not a replica) -> caught_up=false, not panic.
func TestApplyOffsetSynced_MissingOffset_NotCaughtUp(t *testing.T) {
	self := &offsetConn{infoReply: "# Replication\r\nrole:master\r\nmaster_link_status:up\r\n"} // no slave_repl_offset
	source := &offsetConn{infoReply: sourceReplInfo(1000)}
	m := offsetModule(selfAddr, self, source)

	fin := applyOffset(t, m, map[string]any{"skip_checksum": true})
	if fin.GetOutput().GetFields()["caught_up"].GetBoolValue() {
		t.Error("waited caught_up=false: slave_repl_offset is missing (addr is not a replica)")
	}
}
