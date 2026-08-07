// The go-redis adapter: everything that touches the real driver lives here, so
// acl.go can be exercised against a fake client with no server in sight.
package acl

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
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

func dial(ctx context.Context, c connParams) (aclClient, error) {
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

func (g *goRedisClient) SetUser(ctx context.Context, username string, rules []string) error {
	args := make([]any, 0, len(rules)+3)
	args = append(args, "ACL", "SETUSER", username)
	for _, r := range rules {
		args = append(args, r)
	}
	return g.c.Do(ctx, args...).Err()
}

func (g *goRedisClient) DelUser(ctx context.Context, username string) error {
	return g.c.Do(ctx, "ACL", "DELUSER", username).Err()
}

func (g *goRedisClient) Save(ctx context.Context) error {
	return g.c.Do(ctx, "ACL", "SAVE").Err()
}

func (g *goRedisClient) GetUser(ctx context.Context, username string) (aclUser, bool, error) {
	res, err := g.c.Do(ctx, "ACL", "GETUSER", username).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return aclUser{}, false, nil
		}
		return aclUser{}, false, err
	}
	if res == nil {
		return aclUser{}, false, nil
	}
	fields, ok := asFieldMap(res)
	if !ok {
		return aclUser{}, false, fmt.Errorf("ACL GETUSER: unexpected reply type %T", res)
	}
	return parseGetUser(fields), true, nil
}

// asFieldMap folds either reply shape into one map. Under RESP2 `ACL GETUSER`
// answers with a flat [field, value, field, value, …] array; under RESP3 — which
// go-redis negotiates by default — it answers with a map. A parser that knows
// only the array form fails against a modern client with "unexpected reply
// type", and no amount of faking the client surfaces that.
func asFieldMap(v any) (map[string]any, bool) {
	switch t := v.(type) {
	case map[string]any:
		return t, true
	case map[any]any:
		out := make(map[string]any, len(t))
		for k, val := range t {
			if s, ok := k.(string); ok {
				out[s] = val
			}
		}
		return out, true
	case []any:
		out := make(map[string]any, len(t)/2)
		for i := 0; i+1 < len(t); i += 2 {
			if s, ok := t[i].(string); ok {
				out[s] = t[i+1]
			}
		}
		return out, true
	default:
		return nil, false
	}
}

// parseGetUser reads the field/value reply of `ACL GETUSER`.
//
// The reply shape also moved between server versions: on 6.2 `keys` and
// `channels` are arrays of bare patterns and there is no `selectors` field,
// while on 7.x/8 they are strings carrying the `~` and `&` prefixes. Both are
// folded into one string here — not to produce a version-independent canonical
// form, which is not possible, but so that two snapshots from the SAME server
// compare like for like. That is all the diff needs.
func parseGetUser(fields map[string]any) aclUser {
	var u aclUser
	for _, f := range toStrings(fields["flags"]) {
		switch f {
		case "on":
			u.enabled = true
		case "nopass":
			u.nopass = true
		}
	}
	u.passwords = toStrings(fields["passwords"])
	u.commands = flatten(fields["commands"])
	u.keys = flatten(fields["keys"])
	u.channels = flatten(fields["channels"])
	u.selectors = parseSelectors(fields["selectors"])
	return u
}

// parseSelectors renders each selector (Redis 7.0+) to one comparable string.
// Selectors are extra permission sets attached to a user; a module that ignores
// them reports "no change" while an extra selector quietly grants access.
func parseSelectors(v any) []string {
	items, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(items))
	for _, item := range items {
		fields, ok := asFieldMap(item)
		if !ok {
			continue
		}
		parts := make([]string, 0, len(fields))
		for name, v := range fields {
			parts = append(parts, name+"="+flatten(v))
		}
		// Map iteration order is random; sort so the rendering is stable and the
		// diff does not fire on ordering alone.
		sort.Strings(parts)
		out = append(out, strings.Join(parts, " "))
	}
	return out
}

// flatten turns either reply shape — a string or an array of strings — into one
// string.
func flatten(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return strings.Join(toStrings(v), " ")
}

func toStrings(v any) []string {
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(arr))
	for _, item := range arr {
		if s, ok := item.(string); ok {
			out = append(out, s)
		}
	}
	return out
}
