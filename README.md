# soul-mod-redis

A module bundle for Redis. One artifact, one approval, one signature — today it serves `acl` and `config`, and `info` joins them by being added to `internal/` and named in `cmd/soul-mod-redis/main.go`.

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

## The `config` module

- `present` — the named runtime parameters hold the requested values. Writes only when something actually differs.
- `read` — reads parameters into the register. A pure read; never reports a change.

### What this does that driving `redis-cli` does not

**The verdict comes from the server, not from comparing strings.** Redis stores a normalised form and hands *that* back:

```
you write:    maxmemory: 100mb        Redis stores: 104857600
you write:    notify-keyspace-events: KEA   Redis stores: AKE
you write:    appendfsync: EVERYSEC   Redis stores: everysec
```

and the multipliers are not the ones anyone guesses — `1k` is 1000 while `1kb` is 1024. A module that compares what you asked for against what is stored reports a change on every single run. This one canonicalises **only to decide whether a write is worth attempting**, then re-reads and takes `changed` from the before/after values the server itself produced. A canonicalisation miss costs one redundant `CONFIG SET`; it cannot produce a false "changed".

Case is deliberately **not** folded, even though it would save a write on `appendfsync`: in `notify-keyspace-events`, `K` and `k` mean different things.

**The write is atomic where the server supports it.** Redis 7.0+ applies a multi-parameter `CONFIG SET` all-or-nothing. Redis 6.2 rejects the form outright, having applied nothing, so the module falls back to one write per parameter — not atomic, and the output says `atomic: false` rather than pretending. `require_atomic: true` fails instead of falling back. A rejection that is *not* about arity — a bad value, an immutable parameter — never triggers the fallback: the server already rolled the whole command back, and retrying one by one would apply the half it refused as a unit.

**An unknown parameter is a failure, not a silent skip.** `CONFIG GET` answers nothing for a parameter the server does not have, and the parameter set differs a lot between versions — 169 on 6.2, 202 on 7.4, 302 on 8.

### Parameters

| parameter | type | required | notes |
|---|---|---|---|
| `host` / `port` / `db` | string / int / int | host only | configuration is server-wide; `db` only selects the connection's db |
| `login_username` / `login_password` | string | no | needs `config\|get` and `config\|set`; `+@read` is not enough. Password must be a vault-ref |
| `tls_enable` / `tls_ca_path` / `tls_skip_verify` | bool / string / bool | no | |
| `timeout_seconds` | int | no | default 10 |
| `settings` | map | **yes** | parameter name → desired value |
| `persist` | bool | no | **default true** — `CONFIG REWRITE` after a change |
| `allow_connectivity_change` | bool | no | default false; see Gates |
| `require_atomic` | bool | no | default false |

`read` takes the connection parameters plus `patterns` (glob list, default `["*"]`) and `reveal_secrets` (default false).

### Gates

`allow_connectivity_change` refuses the parameters that can cut the path the module manages the server through — `port`, `bind`, `protected-mode`, `requirepass`, `maxclients`, the `tls-*` material. This is not theoretical:

- `CONFIG SET port` moves the listener and the connection that issued it never gets a reply.
- `protected-mode yes` makes every later command from a non-loopback client fail with `DENIED` — **including the one that would turn it back off**. Recovering needs a restart.

`CONFIG GET requirepass` returns the password in the clear. `requirepass`, `masterauth`, `tls-key-file-pass` and `tls-client-key-file-pass` are withheld from the output as `(redacted)`; a password change is still reported as a change, only the two values are hidden. `read` withholds them too unless `reveal_secrets: true` — the register is not a secret store.

### Persistence — what `persist` converges and what it does not

The module converges the **runtime**. It reads `CONFIG GET` and writes `CONFIG SET`, and never reads the server's configuration file. So a value someone set by hand without saving is invisible to it: the runtime is correct, the module reports no change, and the value reverts on the next restart. If the file itself must be the source of truth, render the file and restart the server — that is a different job.

`persist: true` issues `CONFIG REWRITE` after a change. A server started **without a config file** has nowhere to write and the step **fails** rather than reporting a durability it cannot deliver; the message says the values *are* applied in memory, because they are. Redis rewrites the file under its own uid and reformats it — a multi-pair `save` comes back one directive per line — so the file's owner and mode afterwards are Redis' business, not this module's.

### Usage

```yaml
- name: Tune the cache
  module: redis.config.present
  params:
    host: cache-01.internal
    login_username: admin
    login_password: vault:secret/data/redis#admin_password
    settings:
      maxmemory: 100mb
      maxmemory-policy: allkeys-lru
      appendfsync: everysec

- name: What is this server running with?
  module: redis.config.read
  params:
    host: cache-01.internal
    patterns: ["maxmemory*", "appendonly"]
  register: redis_cfg
```

### Output

```yaml
register:
  redis_config_result:
    action: altered | noop
    changes: "maxmemory: 0 -> 104857600"
    changed_parameters: [maxmemory]
    persisted: true
    atomic: true
```

`read` registers `config` (name → value), `count` and `redacted`.

### Dry-run

`config` implements `Plan` and declares `PlanReadSafe` (ADR-031): drift is determined with `CONFIG GET` and nothing else. Its answer matches Apply's up to canonicalisation — a value stored in a form the module cannot predict shows as drift in Plan and as a no-op in Apply. The error is one-directional on purpose: Plan may over-report a change, never under-report one.

`acl` deliberately does **not** declare the marker. Its rule diff has to push the desired rules through a scratch user, which is a write, so it takes default-deny on dry-run rather than claiming a purity it does not have.

## Layout

```
cmd/soul-mod-redis/main.go   the bundle: compat window + the list of modules
internal/acl/def.go          what the `acl` module offers — the schema source
internal/acl/acl.go          what it does
internal/acl/client.go       the go-redis adapter
internal/config/def.go       what the `config` module offers
internal/config/config.go    what it does
internal/config/client.go    its go-redis adapter
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

- **L0** (`internal/acl/acl_test.go`, `internal/config/config_test.go`) — fake client, no server. `acl` covers create / no-op under a differing server rendering / rule-order drift / password compare and rotation / selector drift / scratch-user lifecycle / the three lockout gates. `config` covers no-op vs real change / a memory unit that only *looks* different / a normalisation the module cannot predict / the atomic and fallback write paths / the persistence outcomes / redaction / the connectivity gate. Both fakes render **differently from their input** on purpose: a fake that echoes what it is given makes every idempotence test pass by construction. `make test`.

- **Guards** (`internal/*/def_test.go`, `cmd/soul-mod-redis/bundle_test.go`) — on the description rather than on the behaviour, because the description and the implementation are two declarations of one contract and nothing in the language ties them together. Every state `def.go` declares must be one `Apply` serves; every output key `Apply` emits must be one `def.go` declares, or a scenario's `register:` reads a field nobody documented; the bundle must validate and print a canonical document, or `soul-mod stamp` refuses it at build time; and an artifact asked for a module it does not serve must refuse **by name** rather than fall through to one it has.

  That last guard asserts the *reason* and not the exit code, which it learned the hard way: a known module also exits non-zero without a socket, so the original exit-code check kept passing after `config` moved from "unknown name to reject" to a real module — and silently stopped testing anything.

- **L1** (`internal/*/live_test.go`, build tag `live`) — real servers. These are the ones that matter: both modules are built on what Redis actually stores, and a fake cannot prove the storing behaves as assumed.

  Four addresses, because every persistence outcome is worth pinning and version drift is the point:

  ```sh
  D=/tmp/redis-l1 && mkdir -p $D && chmod 777 $D
  printf 'port 6379\nmaxmemory 0\naclfile /etc/redis/users.acl\n' > $D/redis.conf
  : > $D/users.acl && chmod 666 $D/*
  docker run -d --rm --name r-full -p 6387:6379 -v $D:/etc/redis \
      redis:7.4 redis-server /etc/redis/redis.conf
  docker run -d --rm --name r-62   -p 6386:6379 redis:6.2
  docker run -d --rm --name r-bare -p 6389:6379 redis:7.4

  make l1 REDIS_ADDR=127.0.0.1:6387 REDIS_ADDR_62=127.0.0.1:6386 \
          REDIS_ADDR_NO_ACLFILE=127.0.0.1:6389 REDIS_ADDR_NOFILE=127.0.0.1:6389
  ```

  `r-full` has both a config file and an aclfile, so `CONFIG REWRITE` and `ACL SAVE` both work; `r-bare` has neither, so both must fail loudly; `r-62` is where the multi-parameter `CONFIG SET` does not exist and the `ACL GETUSER` reply has a different shape. Every variable is required rather than defaulted — an integration test that silently skips looks green and proves nothing. CI runs this tier against Redis 6.2, 7 and 8 on every push, because what changes between those versions is exactly what both modules are built on.

  Durability is checked by **restarting the server**, not by trusting the `persisted` flag the module set itself.

## Releases

Each tag publishes `soul-mod-redis-linux-amd64` and `soul-mod-redis-linux-arm64`, a `.sha256` beside each, and `schema.json`.

Both binaries are static: the build sets `CGO_ENABLED=0`, so nothing is linked against the build host's libc and the artifact runs on a host nobody chose in advance — musl included. They are built and stamped on native runners of their own architecture, because reading a schema back out of an artifact means executing it.

`schema.json` is published once rather than per architecture. The document is derived from Go values and says nothing about the platform, so the two builds produce it byte for byte identical; the release verifies that rather than assuming it.

## License

Apache 2.0 — see [LICENSE](LICENSE). The core is BSL 1.1, but `sdk/` and `proto/plugin/`, which every plugin statically links, are Apache 2.0 precisely so that plugin binaries stay permissive.

## Author

Soul Stack Core Team.
