SHELL := /bin/bash
MODULE := github.com/udaykishore-resu/recon-stream
BIN    := recon-stream
IMAGE  ?= ghcr.io/udaykishore-resu/$(BIN)
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)
GOFLAGS_TEST ?= -race -count=1 -p 1

.PHONY: all build run run-full test test-short cover vet lint tidy gen docker helm-lint compose-down demo clean help

all: build

## build: compile the service binary into ./bin
build:
	CGO_ENABLED=0 go build -trimpath -ldflags '$(LDFLAGS)' -o bin/$(BIN) ./cmd/$(BIN)

## run: run with the in-memory store, no external services
run: build
	RECON_STORE=memory RECON_LOG_LEVEL=info ./bin/$(BIN)

## run-full: start Kafka (KRaft), Postgres and an OTel collector, then run against them
run-full: build
	docker compose -f deploy/docker-compose.yaml up -d
	RECON_STORE=postgres \
	RECON_POSTGRES_DSN=postgres://recon:recon@localhost:5432/recon?sslmode=disable \
	RECON_KAFKA_ENABLED=true RECON_KAFKA_BROKERS=localhost:9092 \
	RECON_OTEL_ENDPOINT=localhost:4318 \
	./bin/$(BIN)

## compose-down: stop the local full stack
compose-down:
	docker compose -f deploy/docker-compose.yaml down -v

## test: unit + integration tests with the race detector
test:
	go test $(GOFLAGS_TEST) ./...

## cover: coverage report (domain packages must stay >= 70%)
cover:
	go test $(GOFLAGS_TEST) -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -n 1

## vet: go vet
vet:
	go vet ./...

## lint: golangci-lint (install: https://golangci-lint.run)
lint:
	golangci-lint run ./...

## tidy: go mod tidy
tidy:
	go mod tidy

## gen: regenerate examples/legs.json
gen:
	go run ./examples/gen -out examples/legs.json

## demo: run the end-to-end example against a running service (RECON_URL)
demo:
	./examples/demo.sh

## docker: build the container image
docker:
	docker build --build-arg VERSION=$(VERSION) -t $(IMAGE):$(VERSION) .

## helm-lint: lint the Helm chart
helm-lint:
	helm lint deploy/helm/$(BIN)
	helm template $(BIN) deploy/helm/$(BIN) > /dev/null

clean:
	rm -rf bin coverage.out

help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/^## /  /'
