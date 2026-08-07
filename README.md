# soul-mod-redis

A module bundle for Redis. One artifact, one approval, one signature — today it serves the `acl` module, and `config` and `info` join it by being added to `internal/` and named in `cmd/soul-mod-redis/main.go`.

A SoulModule plugin for Soul Stack, built on `sdk/module` over the gRPC-stdio handshake (ADR-020).

## Addressing — the artifact does not name itself

There is no `namespace:`, no `name:`, no publisher anywhere in this repository. The artifact knows only its own modules. The first level of the address is the alias an operator picks when registering the source, so the same bytes serve

```
redis.acl.present              registered as `redis`
redis-community.acl.present    registered as `redis-community`
```

Two publishers of the same subject therefore cannot collide — the operator names both. The host invokes a module by subcommand (`soul-mod-redis acl`); `soul-mod-redis schema` prints the artifact's own schema document.

## The `acl` module

- `present` — the user exists, enabled as requested, with the given password and rules. Writes only when something actually differs.
- `absent` — the user is deleted; no-op when it is already gone.

### What this does that driving `redis-cli` does not

**The rule diff goes through Redis instead of through string matching.** What `ACL GETUSER` returns is Redis' own rendering, and it does not look like what you wrote:

```
you write:   +@read +SET -DEBUG
Redis stores: -@all +@read +set -debug
```

Worse, the rendering depends on the order the rules were applied in — `-DEBUG +SET +@read` comes back as `-@all -debug +set +@read` — and Redis 6.2 returns `keys` and `channels` as arrays without their `~`/`&` prefixes where 7.x returns strings with them, and minimises the command set differently again. Comparing the desired rules against that text reports a change on every single run.

So this module pushes the desired rules through a **disabled, passwordless scratch user**, reads back what the server stored, and compares that against the managed user's snapshot. Both renderings come from the same server, so they are comparable by construction — no per-version canonicaliser to keep in sync. The scratch user is created `off nopass`, cannot be authenticated as, and is deleted before the call returns.

The same step doubles as validation: a rule Redis will not parse fails against the scratch user, leaving the managed user untouched.

**The password is compared, not re-applied.** Redis stores an unsalted SHA-256 hex digest, so the desired password is checked against it directly — an unchanged password reports no change, with no "update password" flag to remember. Pass `password_hash` instead of `password` when the plaintext should never reach the module at all.

**Say nothing about the credential and the existing one is kept.** That is the reading anyone expects, and it holds even though every write starts with `reset`: the digest `ACL GETUSER` hands out is exactly the digest `ACL SETUSER` accepts back, so the current password is carried across the reset as a `#<hash>` token. The one case where there is nothing to keep is a user that does not exist yet — a new enabled user has to be given `password`, `password_hash` or `nopass`, because otherwise it could never authenticate.

**Rules are never reordered.** Order is part of the meaning: `+@all -@admin` grants everything except administrative commands, while `-@admin +@all` collapses to plain `+@all` — full admin. Sorting a rule list is a privilege escalation, so this module passes the list through exactly as written.

### Parameters

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `host` | string | yes | Redis host. |
| `port` | int | no (`6379`) | |
| `db` | int | no (`0`) | Only selects the connection's db; ACLs are server-wide. |
| `login_username` | string | no | User to authenticate as. Needs ACL access — `+@read` is not enough. |
| `login_password` | string (secret) | no | Vault-ref, resolved keeper-side. |
| `tls_enable` / `tls_ca_path` / `tls_skip_verify` | bool / string / bool | no | |
| `timeout_seconds` | int | no (`10`) | |
| `username` | string | yes | User to manage. Validated, not escaped. |
| `enabled` | bool | no (`true`) | `on` / `off`. |
| `password` | string (secret) | no | Vault-ref. Compared against the stored SHA-256. Omit all three credential fields to keep the existing password. |
| `password_hash` | string | no | The same password as a 64-char hex digest, applied as Redis' `#<hash>` token. |
| `nopass` | bool | no (`false`) | Accept any password. |
| `rules` | list | no | Rule tokens **in the order Redis should apply them**. |
| `persist` | bool | no (**`true`**) | Run `ACL SAVE` so the change survives a restart. See below. |
| `allow_self_modification` | bool | no (`false`) | See gates below. |
| `allow_default_user` | bool | no (`false`) | See gates below. |

The full input contract is [`internal/acl/def.go`](internal/acl/def.go) — the Go value the schema is generated from. There is no manifest to read, and nothing to keep in step with it by hand.

### Gates

Redis prevents none of these; the module does.

- **Deleting the user you authenticated as** succeeds in Redis and takes the credentials away from the next run, including the one that would put the user back. Always refused.
- **Managing the user you authenticated as** is refused unless `allow_self_modification: true`. Every write starts with `reset`, so a narrower rule set strips this connection's own permissions mid-run.
- **Managing `default`** is refused unless `allow_default_user: true`. `default off` locks out every unauthenticated client the moment it lands, and with `persist` on that survives a restart. Deleting `default` is refused outright — Redis does not allow it either.

### Persistence — `persist` is on by default, and you need to know what that means

`ACL SETUSER` changes **live in memory only**. Without a save, the user is gone at the next restart — which means `present` stops being true without anyone touching anything. So this module runs `ACL SAVE` by default.

Two consequences, both sharp:

- **It needs an `aclfile` directive on the server.** Without one Redis has nowhere to write, `ACL SAVE` returns `ERR This Redis instance is not configured to use an ACL file`, and **the step fails**. It does not quietly report success: the change did land in memory, but claiming durability that is not there is worse than stopping. Either give the server an `aclfile`, or set `persist: false` to say out loud that an in-memory-only change is what you want.
- **`ACL SAVE` rewrites the whole file**, including users this module knows nothing about. Do not point it at a `users.acl` that something else renders from a template: whichever ran last wins, and the loser's users disappear at the next restart.

Redis enforces single ownership at the other end too: a server configured with both `user` lines in `redis.conf` and an `aclfile` refuses to start.

### Rule portability

Rules are passed through verbatim, so portability across server versions is yours to manage. Notably, `acl-pubsub-default` is `allchannels` on 6.2 and `resetchannels` on 7.x/8, so `&pattern` needs an explicit `resetchannels` before it to work on both. `%R~`/`%W~` key patterns and selectors need 7.0+. The module reports the server's own error rather than rewriting your rules to fit.

### Usage

The state is the third level of the address, not a separate key. `redis` below is the registration alias, not something this artifact declares:

```yaml
- module: redis.acl.present
  params:
    host: "cache-01.internal"
    login_username: admin
    login_password: "${ vault('secret/keeper/redis-admin#password') }"
    username: app
    password: "${ vault('secret/keeper/app-credentials#redis_password') }"
    rules:
      - "~cache:*"
      - "~session:*"
      - "resetchannels"
      - "&events:*"
      - "+@read"
      - "+@write"
      - "-DEBUG"
    # persist defaults to true — the server needs an `aclfile` for that to work
```

### Output

```yaml
register:
  redis_acl_result:
    action: created | altered | dropped | noop
    username: app
    changes: "password, commands"
    persisted: true
```

## Layout

```
cmd/soul-mod-redis/main.go   the bundle: compat window + the list of modules
internal/acl/def.go          what the `acl` module offers — the schema source
internal/acl/acl.go          what it does
internal/acl/client.go       the go-redis adapter
```

A second module is `internal/<name>/` with its own `def.go` plus one entry in `main.go`. Nothing else changes: not the artifact name, not the registration, not the signature.

## Build

```sh
make build   # compile to dist/, then stamp the schema into the artifact
make check   # fmt + vet + test + build + verify
```

`make build` produces two files. `dist/soul-mod-redis` carries its schema as a trailer appended after the executable, so Keeper can read the disclosure **without running the binary** — at `plugin.allow` the artifact is not approved yet, and executing it would be the thing approval is meant to gate. `dist/schema.json` is the same bytes on their own, for `soul-lint`, which should not have to download an artifact to check a destiny.

`make verify` is the CI gate: the stamped schema must still match what the artifact reports. It fails closed — a missing or damaged trailer is an error, never a fall back to the sibling file.

Two things worth knowing about `dist/`:

- **`go build` on its own leaves an unstamped artifact.** Building by hand or from an IDE overwrites the stamped binary with a bare one; `make verify` then reports `artifact carries no schema trailer`. Build through `make`.
- **Use `make clean` if a stamp ever goes wrong.** `go build -o` skips rewriting an output whose build ID already matches, so a damaged artifact survives a plain rebuild.

`soul-mod` itself comes from the SDK this bundle already depends on, so nothing needs installing — `make` runs it with `go run`. Installing it as a binary instead (`make check SOUL_MOD=soul-mod`) is not possible yet: `go install` refuses the SDK because its `go.mod` carries a `replace` directive, which `go install pkg@version` forbids outright. Tracked as NIM-492, together with the reason the pin below is a pseudo-version.

### Which core this builds against

The bundle SDK landed on the core's `release/R5` and has not reached its `main` yet, so `go.mod` pins `github.com/souls-guild/soul-stack/sdk` to that commit and supplies a version for `proto/plugin` that the SDK's own `go.mod` leaves as an unresolvable placeholder. Both lines are commented where they sit and both go away once the core cuts a tag. A Keeper older than the compat window in `cmd/soul-mod-redis/main.go` cannot load this artifact at all — the bundle form is new.

## Tests

- **L0** (`internal/acl/acl_test.go`) — fake client, no server. Covers create / no-op under a differing server rendering / rule-order drift / password compare and rotation / selector drift / scratch-user lifecycle / the three lockout gates / parameter validation. `make test`.

- **Guards** (`internal/acl/def_test.go`, `cmd/soul-mod-redis/bundle_test.go`) — on the description rather than on the behaviour, because the description and the implementation are two declarations of one contract and nothing in the language ties them together. Every state `def.go` declares must be one `Apply` serves; the bundle must validate and print a canonical document, or `soul-mod stamp` refuses it at build time; and an artifact asked for a module it does not serve must refuse rather than fall through to the only one it has.

- **L1** (`internal/acl/live_test.go`, build tag `live`) — a real server. These are the ones that matter: the diff is built on Redis' own rendering, and a fake cannot prove the rendering behaves as assumed.

  Two servers are needed, because persistence is on by default and both of its outcomes are worth pinning: one with an `aclfile` where `ACL SAVE` works, one without where it must fail loudly.

  ```sh
  mkdir -p /tmp/redis-acl && chmod 777 /tmp/redis-acl
  printf 'port 6379\nsave ""\naclfile /etc/redis/users.acl\n' > /tmp/redis-acl/redis.conf
  printf 'user default on nopass ~* &* +@all\n' > /tmp/redis-acl/users.acl
  chmod 666 /tmp/redis-acl/*
  docker run -d --rm --name r-aclfile -p 6388:6379 -v /tmp/redis-acl:/etc/redis \
      redis:7-alpine redis-server /etc/redis/redis.conf
  docker run -d --rm --name r-plain   -p 6387:6379 redis:7-alpine redis-server --save ''

  REDIS_ADDR=127.0.0.1:6388 REDIS_ADDR_NO_ACLFILE=127.0.0.1:6387 make l1
  ```

  Both variables are required rather than defaulted — an integration test that silently skips looks green and proves nothing. CI runs this tier against Redis 6.2, 7 and 8 on every push, because the rendering the diff is built on is exactly what changes between those versions.

## Releases

Each tag publishes `soul-mod-redis-linux-amd64` and `soul-mod-redis-linux-arm64`, a `.sha256` beside each, and `schema.json`.

Both binaries are static: the build sets `CGO_ENABLED=0`, so nothing is linked against the build host's libc and the artifact runs on a host nobody chose in advance — musl included. They are built and stamped on native runners of their own architecture, because reading a schema back out of an artifact means executing it.

`schema.json` is published once rather than per architecture. The document is derived from Go values and says nothing about the platform, so the two builds produce it byte for byte identical; the release verifies that rather than assuming it.

## License

Apache 2.0 — see [LICENSE](LICENSE). The core is BSL 1.1, but `sdk/` and `proto/plugin/`, which every plugin statically links, are Apache 2.0 precisely so that plugin binaries stay permissive.

## Author

Soul Stack Core Team.
