# redis — the gate that used to live in soul-stack's `check-plugin-schema` now lives
# beside the artifact it checks (NIM-868, following NIM-825).
#
# The plugin is its OWN Go module and depends on the core only through two published
# ones (ADR-011): `sdk` and `proto/plugin`, by version and with no `replace`. That is
# what makes this repository buildable on its own, and a `replace` creeping back in
# is the one change that would quietly re-couple it to a checkout of soul-stack.
#
# ★ soul-stack pins a commit of this repository and checks its own vendored
# `examples/module/redis/schema.json` against what `soul-mod stamp` derives here. So
# `schema` below is not only this repo's gate: a change that moves the document and
# is not accompanied by a pin bump there turns soul-stack's `check-plugin-schema` red.
.DEFAULT_GOAL := help

BIN     := redis
SOULMOD ?= soul-mod

.PHONY: help
help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | sort | \
	  awk 'BEGIN{FS=":.*?## "}{printf "%-18s %s\n", $$1, $$2}'

.PHONY: build
build: ## Build the artifact into dist/ and stamp it
	GOWORK=off go build -o dist/$(BIN) .
	@$(SOULMOD) stamp dist/$(BIN) >/dev/null

.PHONY: verify
verify: ## Refuse an artifact whose trailer is absent or disagrees with dist/schema.json
	@$(SOULMOD) verify dist/$(BIN) >/dev/null
	@echo "verify: the artifact's trailer is readable and matches the document beside it"

.PHONY: test
test: ## Unit tests with the race detector
	GOWORK=off go test -race -count=1 ./...

.PHONY: vet
vet: ## go vet
	GOWORK=off go vet ./...

.PHONY: fmt
fmt: ## Refuse unformatted sources
	@out=$$(GOWORK=off gofmt -l .); \
	  if [ -n "$$out" ]; then echo "gofmt: $$out" >&2; exit 1; fi

.PHONY: no-replace
no-replace: ## Refuse a `replace` in go.mod — it would re-couple this repo to a soul-stack checkout
	@if grep -q '^replace' go.mod; then \
	  echo "go.mod carries a replace directive. This repository depends on the core through" >&2; \
	  echo "published sdk/proto-plugin versions only (ADR-011); a replace makes it buildable" >&2; \
	  echo "only next to a soul-stack checkout, which is what moving it here undid." >&2; \
	  exit 1; \
	fi
	@echo "no-replace: go.mod depends on published versions only"

.PHONY: schema
schema: build ## schema.json is what `soul-mod stamp` derives from the Go value
	@$(SOULMOD) verify dist/$(BIN) >/dev/null
	@if ! diff -q dist/schema.json schema.json >/dev/null; then \
	  echo "schema.json is NOT what the artifact publishes — re-run: $(SOULMOD) stamp dist/$(BIN)" >&2; \
	  diff -u schema.json dist/schema.json | head -40 >&2; exit 1; \
	fi
	@echo "schema: schema.json is what \`soul-mod stamp\` derives, and verify is green"

.PHONY: check
check: fmt vet no-replace test schema ## The whole gate
	@echo "check: green"
