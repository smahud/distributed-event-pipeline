# Makefile for distributed-event-pipeline

.PHONY: build test lint fmt vet run clean docker-build docker-run docker-stop deps

# Build variables
BINARY_NAME=consumer
PRODUCER_BINARY=producer
DLQ_BINARY=dead-letter
BUILD_DIR=./bin
VERSION=$(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT=$(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_TIME=$(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS=-ldflags "-X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.buildTime=$(BUILD_TIME)"

# Go commands
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOVET=$(GOCMD) vet
GOFMT=gofmt
GOLINT=golangci-lint

# Default target
all: build

# Build all binaries
build: deps
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/consumer
	$(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(PRODUCER_BINARY) ./cmd/producer
	$(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(DLQ_BINARY) ./cmd/dead-letter

# Build for production (stripped)
build-prod: deps
	@mkdir -p $(BUILD_DIR)
	CGO_ENABLED=1 GOOS=linux $(GOBUILD) $(LDFLAGS) -a -installsuffix cgo -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/consumer
	CGO_ENABLED=1 GOOS=linux $(GOBUILD) $(LDFLAGS) -a -installsuffix cgo -o $(BUILD_DIR)/$(PRODUCER_BINARY) ./cmd/producer
	CGO_ENABLED=1 GOOS=linux $(GOBUILD) $(LDFLAGS) -a -installsuffix cgo -o $(BUILD_DIR)/$(DLQ_BINARY) ./cmd/dead-letter

# Download dependencies
deps:
	$(GOCMD) mod download
	$(GOCMD) mod tidy

# Run tests
test:
	$(GOTEST) -v -race -coverprofile=coverage.out ./...

# Run tests with coverage
test-coverage: test
	$(GOCMD) tool cover -html=coverage.out -o coverage.html

# Lint code
lint:
	$(GOLINT) run ./...

# Format code
fmt:
	$(GOFMT) -w .

# Vet code
vet:
	$(GOVET) ./...

# Run consumer locally
run:
	$(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/consumer
	$(BUILD_DIR)/$(BINARY_NAME)

# Run producer locally
run-producer:
	$(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(PRODUCER_BINARY) ./cmd/producer
	$(BUILD_DIR)/$(PRODUCER_BINARY)

# Clean build artifacts
clean:
	$(GOCLEAN)
	rm -rf $(BUILD_DIR)
	rm -f coverage.out coverage.html

# Docker commands
docker-build:
	docker build -t ghcr.io/smahud/distributed-event-pipeline:latest .
	docker build -t ghcr.io/smahud/distributed-event-pipeline:$(VERSION) .

docker-push:
	docker push ghcr.io/smahud/distributed-event-pipeline:latest
	docker push ghcr.io/smahud/distributed-event-pipeline:$(VERSION)

docker-run:
	docker-compose up -d

docker-stop:
	docker-compose down

docker-logs:
	docker-compose logs -f

# Docker compose with build
docker-up:
	docker-compose up -d --build

# Development helpers
dev-up:
	docker-compose up -d zookeeper kafka clickhouse redis

dev-down:
	docker-compose down

# Generate mocks (if using mockgen)
mocks:
	@which mockgen > /dev/null || (go install github.com/golang/mock/mockgen@latest)
	go generate ./...

# Install development tools
install-tools:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install github.com/golang/mock/mockgen@latest
	go install golang.org/x/tools/cmd/goimports@latest

# Verify module
verify:
	$(GOCMD) mod verify

# Tidy module
tidy:
	$(GOCMD) mod tidy

# Help
help:
	@echo "Available targets:"
	@echo "  build          - Build all binaries"
	@echo "  build-prod     - Build production binaries (Linux, stripped)"
	@echo "  deps           - Download and tidy dependencies"
	@echo "  test           - Run tests with race detector"
	@echo "  test-coverage  - Run tests and generate coverage report"
	@echo "  lint           - Run linter"
	@echo "  fmt            - Format code"
	@echo "  vet            - Vet code"
	@echo "  run            - Build and run consumer"
	@echo "  run-producer   - Build and run producer"
	@echo "  clean          - Clean build artifacts"
	@echo "  docker-build   - Build Docker image"
	@echo "  docker-push    - Push Docker image"
	@echo "  docker-run     - Start docker-compose services"
	@echo "  docker-stop    - Stop docker-compose services"
	@echo "  docker-up      - Start docker-compose with build"
	@echo "  dev-up         - Start dev dependencies only"
	@echo "  dev-down       - Stop dev dependencies"
	@echo "  mocks          - Generate mocks"
	@echo "  install-tools  - Install development tools"
	@echo "  verify         - Verify module"
	@echo "  tidy           - Tidy module"