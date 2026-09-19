# redis

A SoulModule bundle for Redis: one artifact, one approval, one signature, seven objects —
`acl`, `cluster`, `command`, `instance`, `replica`, `sentinel`, `user`. Built on
`sdk/module` over the gRPC-stdio handshake (ADR-020).

## Addressing — the artifact does not name itself

There is no `namespace:`, no `name:`, no publisher anywhere in this repository. The
artifact knows only its own modules. The first level of the address is the alias an
operator picks when registering the source, so the same bytes serve

```
redis.acl.present              registered as `redis`
redis-community.acl.present    registered as `redis-community`
```

Level 2 is the OBJECT the module manages and level 3 the action it brings about
(ADR-020, amendment 2026-09-02).

## Layout

Flat, one package. The objects are `obj_*.go`, one file each, and the bundle they are
assembled into is `bundle.go`. This is the layout the implementation was grown in while
it lived in the soul-stack tree, and it moved here whole in NIM-868; the `cmd/` +
`internal/<object>/` arrangement this repository carried until then belonged to a
partial implementation of one object and was replaced rather than migrated to.

## The schema document

`schema.json` is GENERATED from the `module.Def` values in Go — never hand-edited. It is
derived by running the artifact's own `schema` subcommand, and `soul-mod stamp` writes
it both as a trailer on the binary and as the file beside it (NIM-377).

```sh
make schema     # build, stamp, verify, and refuse a schema.json that drifted
```

★ **soul-stack vendors a copy of this document** at `examples/module/redis/schema.json`,
pinned to a commit of this repository, and checks the two against each other in its
`check-plugin-schema` gate. A change that moves the document therefore needs a pin bump
there, or that gate goes red — which is the intended signal, not an accident.

## Dependency on the core

Two published modules, by version, with no `replace`:

```
github.com/souls-guild/soul-stack/sdk
github.com/souls-guild/soul-stack/proto/plugin
```

`make no-replace` refuses a `replace` directive, because one would make this repository
buildable only next to a soul-stack checkout — the coupling moving it here undid
(ADR-011).

## The gate

```sh
make check      # fmt, vet, no-replace, test -race, schema
```

## License

Apache 2.0 — see [LICENSE](LICENSE). The core is BSL 1.1; plugins, the SDK and the
plugin protos are permissive so that a third-party plugin binary does not inherit it
(ADR-016, amendment 2026-07-09).
