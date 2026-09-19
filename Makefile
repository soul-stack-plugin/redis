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
check: fmt vet no-replace test schema ## The whole gate (needs soul-mod on PATH)
	@echo "check: green"

# ci — what a PUBLIC runner can actually run, which is everything but `schema`.
#
# `soul-mod` is built from souls-guild/soul-stack and that repository is PRIVATE, so a
# runner here has no way to obtain the tool: `go install .../sdk/cmd/soul-mod@version` is
# refused outright (the published sdk module's go.mod carries a `replace`), and cloning
# the core needs a credential this repository does not have.
#
# ★ The schema document is NOT therefore unchecked. The core repository vendors a copy of
# it and gates it in `check-plugin-schema`, which builds THIS artifact at a pinned commit,
# runs the real `soul-mod stamp`, and refuses unless the two are byte-identical. That gate
# runs in its `make check`. What is lost here is only the earlier warning, so run the full
# `make check` locally before pushing a change to the bundle.
.PHONY: ci
ci: fmt vet no-replace test ## The gate minus `schema` — for runners with no access to the private core
	@echo "ci: green — NOTE: 'schema' did not run (soul-mod needs the private core repo)."
	@echo "ci: the document is gated by check-plugin-schema in soul-stack, against a pinned"
	@echo "ci: commit of this repository. Run 'make check' locally before changing the bundle."
