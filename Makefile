# Fault-Tolerant Payment Ledger — developer entry points.

SHELL := /bin/bash
GO ?= go

# Local (non-container) defaults. Override to point at another environment.
export LEDGER_DATABASE_URL ?= postgres://ledger:ledger@localhost:5432/ledger?sslmode=disable
export LEDGER_REDIS_ADDR ?= localhost:6379
export LEDGER_KAFKA_BROKERS ?= localhost:9092
export LEDGER_API_URL ?= http://localhost:8080

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

.PHONY: build
build: ## Build all binaries into ./bin
	$(GO) build -o bin/ ./cmd/...

.PHONY: fmt
fmt: ## Format the code
	gofmt -w $(shell find . -name '*.go' -not -path './vendor/*')

.PHONY: fmt-check
fmt-check: ## Fail if any file is not gofmt-clean
	@unformatted=$$(gofmt -l $$(find . -name '*.go' -not -path './vendor/*')); \
	if [ -n "$$unformatted" ]; then echo "not gofmt-clean:"; echo "$$unformatted"; exit 1; fi
	@echo "gofmt: clean"

.PHONY: vet
vet: ## Run go vet
	$(GO) vet ./...

.PHONY: lint
lint: fmt-check vet ## Run golangci-lint (falls back to gofmt + vet if absent)
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run ./...; \
	else \
		echo "golangci-lint not installed; ran gofmt + go vet instead."; \
		echo "install: https://golangci-lint.run/welcome/install/"; \
	fi

.PHONY: test
test: ## Run unit tests (no external infrastructure required)
	$(GO) test ./...

.PHONY: test-integration
test-integration: ## Run integration tests against PostgreSQL, Kafka and Redis
	LEDGER_TEST_INTEGRATION=1 $(GO) test -tags=integration -count=1 -timeout=15m ./tests/...

.PHONY: test-race
test-race: ## Run the full suite under the race detector
	LEDGER_TEST_INTEGRATION=1 $(GO) test -race -tags=integration -count=1 -timeout=20m ./...

.PHONY: test-all
test-all: test test-integration ## Run unit and integration tests

.PHONY: migrate
migrate: ## Apply database migrations
	$(GO) run ./cmd/migrate -direction up

.PHONY: migrate-down
migrate-down: ## Revert all database migrations
	$(GO) run ./cmd/migrate -direction down

.PHONY: migrate-status
migrate-status: ## Show applied migration versions
	$(GO) run ./cmd/migrate -direction status

.PHONY: up
up: ## Start the full stack
	docker compose up --build -d

.PHONY: down
down: ## Stop the stack and remove volumes
	docker compose down -v

.PHONY: logs
logs: ## Tail service logs
	docker compose logs -f api outbox-worker webhook-worker

.PHONY: seed
seed: ## Create and fund the demo accounts
	$(GO) run ./cmd/seed -url $(LEDGER_API_URL)

.PHONY: demo
demo: ## Run the end-to-end demonstration against a running stack
	./scripts/demo.sh

.PHONY: replay
replay: ## Replay events for a transaction: make replay TX=<transaction-id>
	@if [ -z "$(TX)" ]; then \
		echo "usage: make replay TX=<transaction-id> [FROM=<rfc3339>] [TO=<rfc3339>]"; exit 1; \
	fi
	$(GO) run ./cmd/replay -transaction $(TX) $(if $(FROM),-from $(FROM),) $(if $(TO),-to $(TO),)

.PHONY: reconcile
reconcile: ## Run reconciliation against the running API
	curl -fsS $(LEDGER_API_URL)/v1/reconciliation | python3 -m json.tool

.PHONY: smoke
smoke: ## Bring the stack up and verify it serves traffic
	./scripts/smoke.sh

.PHONY: clean
clean: ## Remove build artefacts
	rm -rf bin
