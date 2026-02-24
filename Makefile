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
	go tool gotestsum --format pkgname -- -race -shuffle on -short ./...

total-coverage:
	@echo "Running all tests with combined coverage..."
	go tool gotestsum --format pkgname -- -race -shuffle on -timeout 20m -tags=e2e -coverpkg=./... -coverprofile=coverage.out -covermode=atomic ./... ./test/integration/... ./test/e2e/...
	go tool cover -func=coverage.out
	go tool cover -html=coverage.out -o coverage.html

clean:
	rm -rf bin/

FLOWBITE_VERSION=4.0.1
HTMX_VERSION=2.0.8

upgrade-static:
	@echo "Checking for latest static asset versions..."
	$(eval NEW_FLOWBITE_VERSION=$(shell curl -s https://registry.npmjs.org/flowbite/latest | jq -r .version))
	$(eval NEW_HTMX_VERSION=$(shell curl -s https://registry.npmjs.org/htmx.org/latest | jq -r .version))
	@if [ "$(FLOWBITE_VERSION)" != "$(NEW_FLOWBITE_VERSION)" ]; then \
		echo "Upgrading Flowbite: $(FLOWBITE_VERSION) -> $(NEW_FLOWBITE_VERSION)"; \
		sed -i '' "s/FLOWBITE_VERSION=$(FLOWBITE_VERSION)/FLOWBITE_VERSION=$(NEW_FLOWBITE_VERSION)/" Makefile; \
	fi
	@if [ "$(HTMX_VERSION)" != "$(NEW_HTMX_VERSION)" ]; then \
		echo "Upgrading HTMX: $(HTMX_VERSION) -> $(NEW_HTMX_VERSION)"; \
		sed -i '' "s/HTMX_VERSION=$(HTMX_VERSION)/HTMX_VERSION=$(NEW_HTMX_VERSION)/" Makefile; \
	fi
	@echo "Downloading static assets..."
	@mkdir -p dashboard/static/vendor
	curl -sSfL https://cdn.jsdelivr.net/npm/flowbite@$(NEW_FLOWBITE_VERSION)/dist/flowbite.min.css -o dashboard/static/vendor/flowbite.min.css
	curl -sSfL https://cdn.jsdelivr.net/npm/flowbite@$(NEW_FLOWBITE_VERSION)/dist/flowbite.min.js -o dashboard/static/vendor/flowbite.min.js
	curl -sSfL https://unpkg.com/htmx.org@$(NEW_HTMX_VERSION)/dist/htmx.min.js -o dashboard/static/vendor/htmx.min.js
	curl -sSfL https://cdn.tailwindcss.com -o dashboard/static/vendor/tailwind.min.js

upgrade: upgrade-static
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
