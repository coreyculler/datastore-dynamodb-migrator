# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
GOFMT=gofmt
GOLINT=golangci-lint

# Binary info
BINARY_NAME=datastore-dynamodb-migrator
BINARY_PATH=./$(BINARY_NAME)

# Build info
VERSION ?= dev
BUILD_TIME := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
GIT_COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
LDFLAGS=-ldflags "-X main.Version=$(VERSION) -X main.BuildTime=$(BUILD_TIME) -X main.GitCommit=$(GIT_COMMIT)"

# Default target
.PHONY: all
all: clean build

# Build the binary
.PHONY: build
build:
	@echo "Building $(BINARY_NAME)..."
	$(GOBUILD) $(LDFLAGS) -o $(BINARY_PATH) -v ./main.go

# Clean build artifacts
.PHONY: clean
clean:
	@echo "Cleaning..."
	$(GOCLEAN)
	@rm -f $(BINARY_PATH)
	@rm -f $(BINARY_NAME).exe
	@rm -rf ./bin/
	@rm -rf ./build/
	@rm -rf ./dist/
	@rm -f *.test
	@rm -f *.out
	@rm -f coverage.html coverage.out coverage.txt

# Run tests
.PHONY: test
test:
	@echo "Running tests..."
	$(GOTEST) -v ./...

# Run tests with coverage
.PHONY: test-coverage
test-coverage:
	@echo "Running tests with coverage..."
	$(GOTEST) -coverprofile=coverage.out ./...
	$(GOCMD) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Format code
.PHONY: fmt
fmt:
	@echo "Formatting code..."
	$(GOFMT) -s -w .

# Lint code (requires golangci-lint to be installed)
.PHONY: lint
lint:
	@echo "Linting code..."
	$(GOLINT) run

# Tidy dependencies
.PHONY: tidy
tidy:
	@echo "Tidying dependencies..."
	$(GOMOD) tidy

# Download dependencies
.PHONY: deps
deps:
	@echo "Downloading dependencies..."
	$(GOMOD) download

# Build for multiple platforms
.PHONY: build-all
build-all: clean
	@echo "Building for multiple platforms..."
	@mkdir -p ./bin
	GOOS=linux GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o ./bin/$(BINARY_NAME)-linux-amd64 ./main.go
	GOOS=darwin GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o ./bin/$(BINARY_NAME)-darwin-amd64 ./main.go
	GOOS=darwin GOARCH=arm64 $(GOBUILD) $(LDFLAGS) -o ./bin/$(BINARY_NAME)-darwin-arm64 ./main.go
	GOOS=windows GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o ./bin/$(BINARY_NAME)-windows-amd64.exe ./main.go

# Install the binary to $GOPATH/bin
.PHONY: install
install:
	@echo "Installing $(BINARY_NAME)..."
	$(GOBUILD) $(LDFLAGS) -o $(GOPATH)/bin/$(BINARY_NAME) ./main.go

# Run the application
.PHONY: run
run: build
	@echo "Running $(BINARY_NAME)..."
	$(BINARY_PATH)

# Development build (faster, no optimizations)
.PHONY: dev
dev:
	@echo "Building development version..."
	$(GOBUILD) -o $(BINARY_PATH) ./main.go

# Help target
.PHONY: help
help:
	@echo "Available targets:"
	@echo "  build       - Build the binary"
	@echo "  clean       - Clean build artifacts"
	@echo "  test        - Run tests"
	@echo "  test-coverage - Run tests with coverage report"
	@echo "  fmt         - Format code"
	@echo "  lint        - Lint code (requires golangci-lint)"
	@echo "  tidy        - Tidy dependencies"
	@echo "  deps        - Download dependencies"
	@echo "  build-all   - Build for multiple platforms"
	@echo "  install     - Install binary to GOPATH/bin"
	@echo "  run         - Build and run the application"
	@echo "  dev         - Quick development build"
	@echo "  help        - Show this help message" 