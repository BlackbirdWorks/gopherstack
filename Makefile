.PHONY: build install-deps lint test integration-test clean

BINARY_NAME=gopherstack
GOLANGCI_LINT_VERSION=v1.64.5

build:
	go build -o bin/$(BINARY_NAME) main.go

install-deps:
	@echo "Checking for golangci-lint..."
	@if ! command -v golangci-lint >/dev/null 2>&1; then \
		echo "Installing golangci-lint..."; \
		if command -v brew >/dev/null 2>&1; then \
			brew install golangci-lint; \
		else \
			echo "Homebrew not found. Falling back to curl..."; \
			curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin $(GOLANGCI_LINT_VERSION); \
		fi \
	else \
		echo "golangci-lint is already installed."; \
		if command -v brew >/dev/null 2>&1; then \
			echo "Upgrading golangci-lint via brew..."; \
			brew upgrade golangci-lint || true; \
		fi \
	fi

lint: install-deps
	golangci-lint run ./...

test:
	go test -v ./dynamodb/...

integration-test:
	@echo "Running integration tests..."
	go test -v -tags=integration -parallel 4 ./test/integration/...

clean:
	rm -rf bin/
