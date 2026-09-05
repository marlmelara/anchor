# Anchor developer commands.
#
# Host ports differ from the Postgres/Redis defaults so Anchor can run beside
# another project's containers on the same machine.
DSN ?= postgres://anchor:anchor@localhost:5433/anchor?sslmode=disable
REDIS ?= redis://localhost:6380/0

export ANCHOR_DATABASE_URL := $(DSN)
export ANCHOR_REDIS_URL := $(REDIS)

.PHONY: help up down migrate test test-unit test-race lint tidy-check ci fmt vet build clean rebuild verify psql redis-cli cover

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

up: ## Start Postgres and Redis and wait for health
	docker compose up -d
	@echo "waiting for containers to report healthy..."
	@for i in $$(seq 1 60); do \
		pg=$$(docker inspect --format='{{.State.Health.Status}}' anchor-postgres 2>/dev/null); \
		rd=$$(docker inspect --format='{{.State.Health.Status}}' anchor-redis 2>/dev/null); \
		if [ "$$pg" = "healthy" ] && [ "$$rd" = "healthy" ]; then echo "ready"; exit 0; fi; \
		sleep 1; \
	done; echo "containers did not become healthy" >&2; exit 1

down: ## Stop containers and delete their volumes
	docker compose down -v

migrate: ## Apply pending migrations
	go run ./cmd/anchorctl migrate

rebuild: ## Recompute the runs and steps projections from run_events
	go run ./cmd/anchorctl rebuild

verify: ## Check the projections against the fold without changing them
	go run ./cmd/anchorctl rebuild -verify

test: up migrate ## Run every test, including those needing Postgres
	ANCHOR_TEST_DATABASE_URL="$(DSN)" go test ./...

test-unit: ## Run only the tests that need no database
	go test ./internal/journal/ ./internal/idem/

test-race: up migrate ## Run every test under the race detector
	ANCHOR_TEST_DATABASE_URL="$(DSN)" go test -race ./...

cover: up migrate ## Report test coverage
	ANCHOR_TEST_DATABASE_URL="$(DSN)" go test ./... -coverprofile=coverage.out
	go tool cover -func=coverage.out | tail -1

fmt: ## Format all Go source
	gofmt -w .

vet: ## Run go vet
	go vet ./...

lint: ## Run gofmt, vet, and golangci-lint exactly as CI does
	@test -z "$$(gofmt -l .)" || { echo "unformatted files:"; gofmt -l .; exit 1; }
	go vet ./...
	@command -v golangci-lint >/dev/null 2>&1 \
		|| { echo "golangci-lint not installed: brew install golangci-lint"; exit 1; }
	golangci-lint run --timeout=5m

tidy-check: ## Fail if go.mod or go.sum are not tidy
	go mod tidy
	git diff --exit-code go.mod go.sum

ci: tidy-check lint up migrate ## Run everything CI runs, locally
	ANCHOR_TEST_DATABASE_URL="$(DSN)" go test -race -count=1 ./...
	go run ./cmd/anchorctl rebuild -verify

build: ## Build all binaries into ./bin
	go build -o bin/ ./cmd/...

clean: ## Remove build output
	rm -rf bin coverage.out

psql: ## Open a psql shell on the Anchor database
	docker exec -it anchor-postgres psql -U anchor -d anchor

redis-cli: ## Open a redis-cli shell
	docker exec -it anchor-redis redis-cli
