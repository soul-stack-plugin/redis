// TLS connection of the redis plugin. Manages redis.conf TLS
// directives: statement enables TLS on
// cluster, and the plugin MUST connect via TLS - otherwise in only-TLS mode
// (`port 0`, plain closed) it will not reach Redis at all.
//
// Security model (security-memory: insecure = EXPLICIT opt-out, default
// secure): with tls=true, the default plugin CHECKS the server certificate
// (RootCAs from the passed PEM CA). Client-cert (mTLS) - optional. Disable
// verification can ONLY be done with explicit tls_skip_verify=true (false by default).
//
// PEM comes WHOLE to params (scenario resolves from Vault in the render phase and
// transmits the value), the plugin does not provide its Vault access (capability -
// network_outbound). PEM parameters (tls_ca/tls_cert/tls_key) are marked secret in
// manifest and are masked by the output layer by the key name (shared/audit masks
// tls_key/tls_cert/tls_ca) - do not appear in events/logs/errors.
package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"

	"google.golang.org/protobuf/types/known/structpb"
)

// tlsParams - raw TLS connection parameters from params, separate from *tls.Config:
// keep the PEM lines before building the config so that the factory is a pure function and
// tested without a live socket (L0: buildTLSConfig over fake PEMs).
type tlsParams struct {
	enabled    bool
	caPEM      string // PEM CA for server certificate verification (RootCAs)
	certPEM    string // PEM client-cert for mTLS (optional; with keyPEM)
	keyPEM     string // PEM client-key for mTLS (optional; together with certPEM)
	skipVerify bool   // EXPLICIT opt-out of certificate verification (default false)
}

// parseTLS extracts TLS parameters from params. All fields are optional: tls
// missing/false -> enabled=false, plaintext connection (back-compat for
// installations without TLS). PEM strings are kept separate from anything that falls into
// events (as password - IS invariant ADR-010).
//
// ONLY missing or `false` reads as plaintext. A `tls` that is neither — the string
// "true", a number — never reaches here: [object.Apply] refuses it against the
// declared type first (params.go, NIM-778). It used to read as plaintext, which
// sent the password over the wire in the clear and reported the step reconciled.
func parseTLS(f map[string]*structpb.Value) tlsParams {
	return tlsParams{
		enabled:    boolOrDefault(f["tls"], false),
		caPEM:      stringOrEmpty(f["tls_ca"]),
		certPEM:    stringOrEmpty(f["tls_cert"]),
		keyPEM:     stringOrEmpty(f["tls_key"]),
		skipVerify: boolOrDefault(f["tls_skip_verify"], false),
	}
}

// parseSourceTLS - TLS parameters for connecting to an EXTERNAL source (source_tls*),
// separate from their own tls* (offset-synced opens a SECOND connection to someone else's
// master with its own CA/verification). parseTLS symmetry, prefix source_.
// mTLS pair (source_tls_cert/source_tls_key) and skip_verify are also supported for
// uniformity, although the pilot usually expects only source_tls + source_tls_ca.
func parseSourceTLS(f map[string]*structpb.Value) tlsParams {
	return tlsParams{
		enabled:    boolOrDefault(f["source_tls"], false),
		caPEM:      stringOrEmpty(f["source_tls_ca"]),
		certPEM:    stringOrEmpty(f["source_tls_cert"]),
		keyPEM:     stringOrEmpty(f["source_tls_key"]),
		skipVerify: boolOrDefault(f["source_tls_skip_verify"], false),
	}
}

// buildTLSConfig builds *tls.Config from tlsParams. Returns nil, nil when TLS
// not enabled (caller builds a plaintext connection). Error - only on broken PEM
// (CA did not parse / client-cert is invalid): error text does NOT contain PEM
// (x509/tls generate errors without embedding the source material; just in case
// caller edits them using keyPEM).
//
// Pure function (no I/O) -> L0 checks the result directly: RootCAs loaded,
// skip_verify is forwarded, client-cert is added if available - without live Redis.
func buildTLSConfig(p tlsParams) (*tls.Config, error) {
	if !p.enabled {
		return nil, nil
	}
	cfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: p.skipVerify, //nolint:gosec // EXPLICIT opt-out operator (tls_skip_verify), default false - verification is enabled
	}

	// RootCAs from the passed CA PEM (server certificate verification). Without
	// skip_verify and without a CA, the verification will go through the system trust pool - for
	// private PKI is usually a failure, so CA in TLS mode is practically
	// required; empty CA with skip_verify=false leaving the system pool
	// (legitimate for a publicly trusted certificate).
	if p.caPEM != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(p.caPEM)) {
			return nil, fmt.Errorf("tls_ca: failed to parse PEM CA certificate")
		}
		cfg.RootCAs = pool
	}

	// Client-cert (mTLS) - optional only if BOTH (cert+key) are sent.
	// One without the other is an operator configuration error (clear text without PEM).
	switch {
	case p.certPEM != "" && p.keyPEM != "":
		pair, err := tls.X509KeyPair([]byte(p.certPEM), []byte(p.keyPEM))
		if err != nil {
			return nil, fmt.Errorf("tls_cert/tls_key: invalid client-cert pair (mTLS)")
		}
		cfg.Certificates = []tls.Certificate{pair}
	case p.certPEM != "" || p.keyPEM != "":
		return nil, fmt.Errorf("tls_cert and tls_key are specified only TOGETHER (mTLS client-cert)")
	}

	return cfg, nil
}
