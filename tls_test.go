package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"strings"
	"testing"
	"time"

	pluginv1 "github.com/souls-guild/soul-stack/proto/plugin/gen/go/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

// genCertPEM returns (certPEM, keyPEM) self-signed ECDSA certificate -
// material for L0 client-cert / CA checks (valid for x509.AppendCertsFromPEM
// and tls.X509KeyPair). No network/files.
func genCertPEM(t *testing.T) (certPEM, keyPEM string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("genkey: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "redis-l0"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IsCA:         true,
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	certPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	keyPEM = string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
	return certPEM, keyPEM
}

// --- buildTLSConfig (pure function) ---

func TestBuildTLSConfig_DisabledReturnsNil(t *testing.T) {
	cfg, err := buildTLSConfig(tlsParams{enabled: false, caPEM: "ignored"})
	if err != nil {
		t.Fatalf("buildTLSConfig: %v", err)
	}
	if cfg != nil {
		t.Fatal("tls is disabled -> *tls.Config must be nil (plaintext connection)")
	}
}

func TestBuildTLSConfig_CALoadedVerifyOn(t *testing.T) {
	caPEM, _ := genCertPEM(t)
	cfg, err := buildTLSConfig(tlsParams{enabled: true, caPEM: caPEM})
	if err != nil {
		t.Fatalf("buildTLSConfig: %v", err)
	}
	if cfg == nil {
		t.Fatal("tls is enabled -> *tls.Config must be non-nil")
	}
	if cfg.RootCAs == nil {
		t.Fatal("CA PEM is transferred -> RootCAs must be loaded")
	}
	if cfg.InsecureSkipVerify {
		t.Fatal("default - VERIFY certificate (skip_verify is not set -> false)")
	}
}

func TestBuildTLSConfig_SkipVerifyPropagated(t *testing.T) {
	cfg, err := buildTLSConfig(tlsParams{enabled: true, skipVerify: true})
	if err != nil {
		t.Fatalf("buildTLSConfig: %v", err)
	}
	if !cfg.InsecureSkipVerify {
		t.Fatal("tls_skip_verify=true -> InsecureSkipVerify must be skipped")
	}
}

func TestBuildTLSConfig_ClientCertWhenPresent(t *testing.T) {
	caPEM, _ := genCertPEM(t)
	certPEM, keyPEM := genCertPEM(t)
	cfg, err := buildTLSConfig(tlsParams{enabled: true, caPEM: caPEM, certPEM: certPEM, keyPEM: keyPEM})
	if err != nil {
		t.Fatalf("buildTLSConfig: %v", err)
	}
	if len(cfg.Certificates) != 1 {
		t.Fatalf("client-cert (cert+key) -> exactly 1 Certificate, got %d (mTLS)", len(cfg.Certificates))
	}
}

func TestBuildTLSConfig_BadCAErrors(t *testing.T) {
	_, err := buildTLSConfig(tlsParams{enabled: true, caPEM: "-----BEGIN CERTIFICATE-----\nnot-pem\n-----END CERTIFICATE-----"})
	if err == nil {
		t.Fatal("broken CA PEM -> error")
	}
}

func TestBuildTLSConfig_CertWithoutKeyErrors(t *testing.T) {
	certPEM, _ := genCertPEM(t)
	_, err := buildTLSConfig(tlsParams{enabled: true, certPEM: certPEM})
	if err == nil {
		t.Fatal("tls_cert without tls_key -> error (mTLS pair only together)")
	}
}

// TestBuildTLSConfig_CABundleLoadsBoth - gluing two CA PEMs (CA-rollover rotate_tls:
// scenario gives tls_ca = old new CA) -> x509-pool receives BOTH
// (AppendCertsFromPEM parses several PEM blocks in a row). Fixes that the bundle string
// from compute.tls_ca is valid for the plugin without editing the plugin itself.
func TestBuildTLSConfig_CABundleLoadsBoth(t *testing.T) {
	caOld, _ := genCertPEM(t)
	caNew, _ := genCertPEM(t)
	bundle := caOld + "\n" + caNew // so compute.tls_ca merges the old + new CA
	cfg, err := buildTLSConfig(tlsParams{enabled: true, caPEM: bundle})
	if err != nil {
		t.Fatalf("buildTLSConfig(bundle): %v", err)
	}
	if cfg == nil || cfg.RootCAs == nil {
		t.Fatal("bundle CA -> RootCAs must be loaded")
	}
	if got := len(cfg.RootCAs.Subjects()); got != 2 {
		t.Fatalf("bundle of TWO CAs -> the pool must contain both certificates, got %d", got)
	}
}

// --- parseTLS + forwarding to connConfig via connect injection ---

// TestApply_TLSParamsReachConnect - all state paths (command/config/replica/
// sentinel) read TLS from params via parseConnConfig -> tls gets into
// connConfig, which the connect injection sees. cluster connects via nodes-map,
// tested separately (TestApplyCluster_TLSReachesNodeConnect).
func TestApply_TLSParamsReachConnect(t *testing.T) {
	caPEM, _ := genCertPEM(t)
	certPEM, keyPEM := genCertPEM(t)

	var captured connConfig
	m := &RedisModule{
		connect: func(_ context.Context, cfg connConfig) (redisConn, error) {
			captured = cfg
			return &fakeConn{}, nil
		},
	}
	_ = m.command().Apply(&pluginv1.ApplyRequest{
		State: "run",
		Params: mustStruct(t, map[string]any{
			"addr":            "127.0.0.1:7379",
			"password":        secretPass,
			"args":            []any{"PING"},
			"tls":             true,
			"tls_ca":          caPEM,
			"tls_cert":        certPEM,
			"tls_key":         keyPEM,
			"tls_skip_verify": false,
		}),
	}, &applyStream{})

	if !captured.tls.enabled {
		t.Fatal("tls=true did not reach connConfig.tls.enabled")
	}
	if captured.tls.caPEM != caPEM {
		t.Error("tls_ca did not reach connConfig.tls.caPEM")
	}
	if captured.tls.certPEM != certPEM || captured.tls.keyPEM != keyPEM {
		t.Error("client-cert (tls_cert/tls_key) did not reach connConfig.tls")
	}
	if captured.tls.skipVerify {
		t.Error("tls_skip_verify=false should forward false (default secure)")
	}

	// From the same params we build the final *tls.Config - it is correct (CA + client-cert).
	cfg, err := buildTLSConfig(captured.tls)
	if err != nil {
		t.Fatalf("buildTLSConfig from the arrived params: %v", err)
	}
	if cfg.RootCAs == nil || len(cfg.Certificates) != 1 || cfg.InsecureSkipVerify {
		t.Fatalf("tls.Config is incomplete: RootCAs=%v certs=%d skip=%v", cfg.RootCAs != nil, len(cfg.Certificates), cfg.InsecureSkipVerify)
	}
}

func TestApply_TLSDefaultOffPlaintext(t *testing.T) {
	var captured connConfig
	m := &RedisModule{
		connect: func(_ context.Context, cfg connConfig) (redisConn, error) {
			captured = cfg
			return &fakeConn{}, nil
		},
	}
	_ = m.command().Apply(&pluginv1.ApplyRequest{
		State:  "run",
		Params: mustStruct(t, map[string]any{"addr": "127.0.0.1:6379", "args": []any{"PING"}}),
	}, &applyStream{})

	if captured.tls.enabled {
		t.Fatal("tls is not set -> enabled should be false (plaintext, back-compat)")
	}
	cfg, err := buildTLSConfig(captured.tls)
	if err != nil || cfg != nil {
		t.Fatalf("tls disabled -> buildTLSConfig nil,nil; got cfg=%v err=%v", cfg, err)
	}
}

// TestApplyCluster_TLSReachesNodeConnect - cluster-state reads tls from params and
// sends it to the connConfig of each node (only network_outbound, its vault
// no). We intercept connect, give the already-formed cluster -> the sample node sees
// TLS parameters.
func TestApplyCluster_TLSReachesNodeConnect(t *testing.T) {
	caPEM, _ := genCertPEM(t)
	var sawTLS bool
	m := &RedisModule{
		connect: func(_ context.Context, cfg connConfig) (redisConn, error) {
			if cfg.tls.enabled && cfg.tls.caPEM == caPEM {
				sawTLS = true
			}
			// already-formed: CLUSTER INFO ok + our nodes are in place -> no-op,
			// but connect to the first master has already occurred (TLS forwarding has been proven).
			return &clusterConn{
				info:  "cluster_state:ok\ncluster_known_nodes:1\n",
				nodes: "id0 10.0.0.1:6379@16379 myself,master - 0 0 0 connected 0-16383\n",
			}, nil
		},
	}
	stream := &applyStream{}
	_ = m.cluster().Apply(&pluginv1.ApplyRequest{
		State: "created",
		Params: mustStruct(t, map[string]any{
			"nodes":              map[string]any{"n0.example.com": map[string]any{"addr": "10.0.0.1:6379"}},
			"replicas_per_shard": 0,
			"password":           secretPass,
			"tls":                true,
			"tls_ca":             caPEM,
		}),
	}, stream)

	if !sawTLS {
		t.Fatal("cluster: TLS parameters (tls/tls_ca) did not reach the connConfig node")
	}
}

// TestTLS_NoLeakOfKeyInEvents - PEM client-key does NOT leak into events even for
// connection error whose text contains a key (worst-case: the driver put PEM in
// error). redactError cuts; Failed event without PEM. Symmetry with
// TestApply_ConnectFailure_DoesNotLeakPassword, but for tls_key.
func TestTLS_NoLeakOfKeyInEvents(t *testing.T) {
	_, keyPEM := genCertPEM(t)
	m := &RedisModule{
		connect: func(_ context.Context, cfg connConfig) (redisConn, error) {
			// worst-case: the error carries BOTH the password AND the PEM key.
			return nil, errStr("dial failed: AUTH " + cfg.password + " key=" + cfg.tls.keyPEM)
		},
	}
	stream := &applyStream{}
	_ = m.command().Apply(&pluginv1.ApplyRequest{
		State: "run",
		Params: mustStruct(t, map[string]any{
			"addr":     "127.0.0.1:7379",
			"password": secretPass,
			"args":     []any{"PING"},
			"tls":      true,
			"tls_key":  keyPEM,
		}),
	}, stream)

	for _, e := range stream.sent {
		if strings.Contains(e.GetMessage(), secretPass) {
			t.Fatalf("password leaked in event: %q", e.GetMessage())
		}
		// Sufficient evidence of a PEM key is the block header; the full key is multi-line.
		if strings.Contains(e.GetMessage(), "PRIVATE KEY") {
			t.Fatalf("PEM client-key leaked in event: %q", e.GetMessage())
		}
	}
}

// errStr - error from the string (without fmt, so as not to drag unnecessary imports here).
type errStr string

func (e errStr) Error() string { return string(e) }

// the bridge to structpb in this file is already covered by mustStruct from impl_test.go.
var _ = structpb.NewStruct
