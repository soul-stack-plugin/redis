# Makefile for the soul-mod-redis bundle.
#
# The artifact goes to dist/ and is stamped with its own schema. The name is no
# longer load-bearing — keeper takes the single executable it finds in dist/ —
# but stamping is: an unstamped artifact carries no disclosure, and a reader
# that cannot read the disclosure fails closed rather than approving blind.
#
# SOUL_MOD defaults to running the tool out of the SDK the bundle already
# depends on, so a fresh clone needs nothing installed. Point it at an installed
# binary (go install github.com/souls-guild/soul-stack/sdk/cmd/soul-mod@latest)
# to skip the compile on every invocation.

BIN_DIR  := dist
BINARY   := $(BIN_DIR)/soul-mod-redis
SOUL_MOD ?= go run github.com/souls-guild/soul-stack/sdk/cmd/soul-mod

.PHONY: build verify test l1 fmt fmt-check vet check clean

# CGO_ENABLED=0 is not a micro-optimisation: `net` pulls in the cgo resolver by
# default, and the resulting artifact is linked against the build host's libc.
# An artifact that gets downloaded and run on a host nobody chose has to be
# static — and a cross-compiled arm64 build is static anyway, so without this
# the two released binaries would not even be the same kind of thing.
build:
	@mkdir -p $(BIN_DIR)
	@CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o $(BINARY) ./cmd/soul-mod-redis
	@$(SOUL_MOD) stamp $(BINARY)

# The CI gate: the stamped schema must still match the code. A stale stamp is
# worse than no stamp — everything downstream trusts it over the source it can
# no longer see.
verify:
	@$(SOUL_MOD) verify $(BINARY)

# -count=1 rather than a bare `go test`: a cached result is an answer about a
# tree that may no longer be the one on disk.
test:
	@go test -count=1 ./...

# L1 against real servers. Every address is required on purpose: an integration
# test that silently skips looks green and proves nothing.
#
#   REDIS_ADDR            a modern server (7.x/8) started WITH a config file AND
#                         an aclfile — `acl` needs the second to run ACL SAVE,
#                         `config` needs the first to run CONFIG REWRITE
#   REDIS_ADDR_62         a Redis 6.2 server: no multi-parameter CONFIG SET, a
#                         different reply shape for ACL GETUSER, a smaller
#                         parameter set
#   REDIS_ADDR_NO_ACLFILE a server with no aclfile, so ACL SAVE fails
#   REDIS_ADDR_NOFILE     a server started with no config file, so CONFIG
#                         REWRITE fails. One server can serve both of the last
#                         two: what matters is that neither save has anywhere to
#                         write.
l1:
	@test -n "$(REDIS_ADDR)" || { echo "l1: set REDIS_ADDR=host:port (server WITH a config file and an aclfile)"; exit 1; }
	@test -n "$(REDIS_ADDR_62)" || { echo "l1: set REDIS_ADDR_62=host:port (a Redis 6.2 server)"; exit 1; }
	@test -n "$(REDIS_ADDR_NO_ACLFILE)" || { echo "l1: set REDIS_ADDR_NO_ACLFILE=host:port (server WITHOUT an aclfile)"; exit 1; }
	@test -n "$(REDIS_ADDR_NOFILE)" || { echo "l1: set REDIS_ADDR_NOFILE=host:port (server WITHOUT a config file)"; exit 1; }
	@REDIS_ADDR=$(REDIS_ADDR) REDIS_ADDR_62=$(REDIS_ADDR_62) \
		REDIS_ADDR_NO_ACLFILE=$(REDIS_ADDR_NO_ACLFILE) REDIS_ADDR_NOFILE=$(REDIS_ADDR_NOFILE) \
		go test -tags live -count=1 ./...

fmt:
	@gofmt -w .

# `check` uses this and not `fmt`, because a gate that rewrites the tree it is
# checking can never fail: run it in CI and every branch is formatted by
# definition. This one only reports.
fmt-check:
	@out="$$(gofmt -l .)"; \
	  if [ -n "$$out" ]; then echo "gofmt: not formatted:"; echo "$$out"; exit 1; fi

vet:
	@go vet ./...

check: fmt-check vet test build verify
	@echo "check: $(BINARY) ok"

clean:
	@rm -rf $(BIN_DIR)
