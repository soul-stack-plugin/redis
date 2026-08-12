// The go-redis adapter: everything that touches the real driver lives here, so
// handler.go can be exercised against a fake client with no server in sight.
package config

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/types/known/structpb"
)

// connParams is how this module reaches the server.
type connParams struct {
	host     string
	port     int
	db       int
	username string
	password string
	timeout  time.Duration

	tlsEnable     bool
	tlsCAPath     string
	tlsSkipVerify bool
}

func parseConnParams(s *structpb.Struct) (connParams, error) {
	if s == nil {
		return connParams{}, errors.New("params: missing")
	}
	fields := s.GetFields()

	host, err := requireString(s, "host")
	if err != nil {
		return connParams{}, err
	}

	c := connParams{
		host:          host,
		port:          intValueOr(fields["port"], 6379),
		db:            intValueOr(fields["db"], 0),
		timeout:       time.Duration(intValueOr(fields["timeout_seconds"], 10)) * time.Second,
		tlsEnable:     boolValue(fields["tls_enable"]),
		tlsSkipVerify: boolValue(fields["tls_skip_verify"]),
	}
	if v, ok := stringValue(fields["login_username"]); ok {
		c.username = v
	}
	if v, ok := stringValue(fields["login_password"]); ok {
		c.password = v
	}
	if v, ok := stringValue(fields["tls_ca_path"]); ok {
		c.tlsCAPath = v
	}
	// A password with no user is legitimate — that is how you authenticate as
	// `default`. A user with no password cannot authenticate at all.
	if c.username != "" && c.password == "" {
		return connParams{}, errors.New("params: login_username without login_password cannot authenticate")
	}
	return c, nil
}

func dial(ctx context.Context, c connParams) (configClient, error) {
	opts := &redis.Options{
		Addr:         fmt.Sprintf("%s:%d", c.host, c.port),
		DB:           c.db,
		Username:     c.username,
		Password:     c.password,
		DialTimeout:  c.timeout,
		ReadTimeout:  c.timeout,
		WriteTimeout: c.timeout,
	}
	if c.tlsEnable {
		cfg := &tls.Config{
			ServerName: c.host,
			// InsecureSkipVerify is opt-in and documented: an unverified TLS
			// connection to a database is a channel anyone on the path can
			// impersonate.
			InsecureSkipVerify: c.tlsSkipVerify, //nolint:gosec // opt-in, documented
			MinVersion:         tls.VersionTLS12,
		}
		if c.tlsCAPath != "" {
			pem, err := os.ReadFile(c.tlsCAPath)
			if err != nil {
				return nil, fmt.Errorf("tls_ca_path: %w", err)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("tls_ca_path: %s holds no PEM certificate", c.tlsCAPath)
			}
			cfg.RootCAs = pool
		}
		opts.TLSConfig = cfg
	}

	client := redis.NewClient(opts)
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, err
	}
	return &goRedisClient{c: client}, nil
}

type goRedisClient struct{ c *redis.Client }

func (g *goRedisClient) Close() error { return g.c.Close() }

// Get reads one glob pattern.
//
// The reply shape moved with the protocol — a flat [name, value, …] array under
// RESP2, a map under RESP3, which go-redis negotiates by default — but unlike
// `ACL GETUSER` this command has a typed wrapper in the driver that folds both
// forms into map[string]string. Verified against 6.2 / 7.4 / 8 under both
// protocols. Use the typed call rather than a raw Do wherever one exists; the
// raw form is where the RESP3 trap lives.
func (g *goRedisClient) Get(ctx context.Context, pattern string) (map[string]string, error) {
	return g.c.ConfigGet(ctx, pattern).Result()
}

// Set writes the pairs in one command. Redis 7.0+ applies a multi-parameter
// CONFIG SET atomically — verified live: a bad second argument leaves the first
// one untouched. Redis 6.2 rejects the form entirely (arity error) without
// applying anything, which is what makes the caller's fallback safe.
func (g *goRedisClient) Set(ctx context.Context, pairs []setting) error {
	args := make([]any, 0, len(pairs)*2+2)
	args = append(args, "CONFIG", "SET")
	for _, p := range pairs {
		args = append(args, p.name, p.value)
	}
	return g.c.Do(ctx, args...).Err()
}

func (g *goRedisClient) Rewrite(ctx context.Context) error {
	return g.c.ConfigRewrite(ctx).Err()
}
