.PHONY: build install-deps lint lint-fix test integration-test clean demo all

BINARY_NAME=gopherstack

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
	@echo "Checking for fieldalignment..."
	@if ! command -v fieldalignment >/dev/null 2>&1; then \
		echo "Installing fieldalignment..."; \
		go install golang.org/x/tools/go/analysis/passes/fieldalignment/cmd/fieldalignment@latest; \
	else \
		echo "fieldalignment is already installed."; \
	fi

lint: install-deps
	golangci-lint run ./...

lint-fix: install-deps
	@echo "Running fieldalignment..."
	fieldalignment -fix ./...
	@echo "Running golangci-lint with --fix..."
	golangci-lint run --fix ./...

test:
	go tool gotestsum --format pkgname -- -shuffle on -short ./...

total-coverage:
	@echo "Running all tests with combined coverage..."
	go tool gotestsum --format pkgname -- -shuffle on -tags=e2e -coverpkg=./... -coverprofile=coverage.out -covermode=atomic ./... ./test/integration/... ./test/e2e/...
	go tool cover -func=coverage.out
	go tool cover -html=coverage.out -o coverage.html

clean:
	rm -rf bin/

upgrade:
	go get -u ./...
	go mod tidy

bench:
	go test -bench=. -benchmem ./...

demo: 
	docker compose down
	docker compose build
	docker compose up -d

all: 
	make lint-fix
	make total-coverage
	