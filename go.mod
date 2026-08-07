module github.com/soul-stack-plugin/redis

go 1.26.4

require (
	github.com/redis/go-redis/v9 v9.7.0
	github.com/souls-guild/soul-stack/proto/plugin v0.0.0
	github.com/souls-guild/soul-stack/sdk v0.0.0-20260807021519-3b6a399b634d
	google.golang.org/grpc v1.82.1
	google.golang.org/protobuf v1.36.11
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	golang.org/x/net v0.56.0 // indirect
	golang.org/x/sys v0.46.0 // indirect
	golang.org/x/text v0.39.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260615183401-62b3387ff324 // indirect
)

// The bundle SDK is pinned to the commit where it landed on the core's
// release/R5 (NIM-377). Move to a tagged version once R5 reaches main.
//
// The replace below is not a local-development escape hatch — it is what makes
// the repository build for anyone who clones it. The SDK's own go.mod requires
// `proto/plugin v0.0.0`, a placeholder that resolves only through a
// relative-path replace inside the core repo, and a replace in a dependency is
// ignored by the module that consumes it. So the version has to be supplied
// from here. Drop this line once the SDK requires a real version (NIM-492).
replace github.com/souls-guild/soul-stack/proto/plugin => github.com/souls-guild/soul-stack/proto/plugin v0.0.0-20260807021519-3b6a399b634d
