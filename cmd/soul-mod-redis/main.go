// Command soul-mod-redis is a module bundle for Redis: one artifact, one
// approval, one signature covering every module that manages a Redis server.
//
// The host dispatches on the first argument — `soul-mod-redis acl` serves the
// `acl` module over gRPC-stdio (ADR-020), `soul-mod-redis schema` prints the
// artifact's own schema document, which is what `soul-mod stamp` reads back and
// writes into the binary as a trailer.
//
// The artifact declares no identity of its own: no namespace, no publisher, no
// subject name. Which address space these modules land in is decided when an
// operator registers the source and picks an alias, so the same bytes serve
// `redis.acl.present` under one registration and `redis-community.acl.present`
// under another.
package main

import (
	"github.com/soul-stack-plugin/redis/internal/acl"
	"github.com/soul-stack-plugin/redis/internal/config"
	"github.com/souls-guild/soul-stack/sdk/module"
)

// bundle is a named value rather than a literal inside the call so a test can
// validate the exact thing main serves. `info` joins the list here when it
// exists, and nothing else about the artifact changes.
var bundle = module.Bundle{
	Compat:  module.Compat{Keeper: ">=0.9 <2.0"},
	Modules: []module.Def{acl.Module, config.Module},
}

func main() { module.ServeBundle(bundle) }
