# Developer shortcuts. On Windows without `make`, copy the command after each
# target into your terminal (they are all listed in docs/07-running.md).

.DEFAULT_GOAL := help
.PHONY: help proto build test test-race test-integration test-e2e lint fmt up up-apps down logs clean load

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'

proto: ## Regenerate Go code from .proto files
	bash scripts/gen-proto.sh

build: ## Compile all binaries into ./bin
	go build -o bin/ ./cmd/...

test: ## Unit tests (no external dependencies)
	go test ./...

test-race: ## Unit tests with the race detector (needs cgo/gcc)
	go test -race -count=1 ./...

test-integration: ## Repository tests against real Postgres (needs: make up)
	go test -tags=integration -count=1 ./test/integration/...

test-e2e: ## Full-system tests (needs: make up-apps)
	go test -tags=e2e -count=1 ./test/e2e/...

lint: ## Static analysis
	go vet ./...
	golangci-lint run

fmt: ## Format code
	gofmt -w .

up: ## Start infrastructure (Postgres, Kafka, Kafka UI)
	docker compose up -d

up-apps: ## Start infrastructure + all three services in Docker
	docker compose --profile apps up -d --build

down: ## Stop everything (keeps data)
	docker compose --profile apps down

clean: ## Stop everything and DELETE all data volumes
	docker compose --profile apps down -v

logs: ## Tail service logs
	docker compose --profile apps logs -f order-service payment-service notification-service

load: ## Fire 500 orders with 20 workers
	go run ./cmd/shopctl load -n 500 -c 20
