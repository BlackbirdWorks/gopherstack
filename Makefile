.PHONY: build install-deps install-tofu lint lint-fix test integration-test terraform-test e2e-test total-coverage clean demo all

BINARY_NAME=gopherstack

build:
	go build -o bin/$(BINARY_NAME) .

install-deps:
	@echo "Checking for golangci-lint..."
	@if ! command -v golangci-lint >/dev/null 2>&1; then \
		echo "Installing golangci-lint..."; \
		if command -v brew >/dev/null 2>&1; then \
			brew install golangci-lint; \
		else \
			echo "Homebrew not found. Trying go install from source..."; \
			GOMODCACHE=$$(mktemp -d) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest || { \
				echo "go install failed; cloning and building from source..."; \
				TMPDIR=$$(mktemp -d) && git clone --depth=1 https://github.com/golangci/golangci-lint "$${TMPDIR}/golangci-lint" && \
				cd "$${TMPDIR}/golangci-lint" && go build -o "$$(go env GOPATH)/bin/golangci-lint" ./cmd/golangci-lint; \
			}; \
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

TOFU_VERSION ?= latest

install-tofu:
	@mkdir -p bin
	@if [ -x bin/tofu ]; then \
		echo "OpenTofu is already installed at bin/tofu"; \
	else \
		echo "Downloading OpenTofu..."; \
		if [ "$(TOFU_VERSION)" = "latest" ]; then \
			TOFU_VER=$$(curl -sS https://get.opentofu.org/tofu/api.json | jq -r '[.versions[].id | select(contains("-") | not)][0]'); \
		else \
			TOFU_VER=$(TOFU_VERSION); \
		fi; \
		OS=$$(uname -s | tr '[:upper:]' '[:lower:]'); \
		ARCH=$$(uname -m); \
		if [ "$$ARCH" = "x86_64" ]; then ARCH="amd64"; fi; \
		if [ "$$ARCH" = "aarch64" ]; then ARCH="arm64"; fi; \
		echo "Downloading OpenTofu $$TOFU_VER ($$OS/$$ARCH)..."; \
		curl -sSfL "https://github.com/opentofu/opentofu/releases/download/v$${TOFU_VER}/tofu_$${TOFU_VER}_$${OS}_$${ARCH}.zip" -o bin/tofu.zip; \
		unzip -o bin/tofu.zip tofu -d bin/; \
		rm bin/tofu.zip; \
		chmod +x bin/tofu; \
		echo "OpenTofu $$TOFU_VER installed to bin/tofu"; \
	fi

lint: install-deps
	golangci-lint run ./...
	go tool govulncheck ./...

lint-fix: install-deps
	@echo "Running fieldalignment..."
	fieldalignment -fix ./...
	@echo "Running golangci-lint with --fix..."
	golangci-lint run --fix ./...

test:
	go tool gotestsum --format pkgname -- -race -shuffle on -short ./...

integration-test:
	go tool gotestsum --format pkgname -- -race -shuffle on -timeout 10m ./test/integration/...

terraform-test: install-tofu
	PATH="$$PWD/bin:$$PATH" go tool gotestsum --format pkgname -- -v -race -parallel 8 -timeout 10m ./test/terraform/...

e2e-test:
	go tool gotestsum --format pkgname -- -race -shuffle on -timeout 10m -tags=e2e ./test/e2e/...

total-coverage:
	$(eval COVERPKGS := $(shell go list ./... | grep -v -E '(test/|/demo$$|/modules/|/teststack$$)' | tr '\n' ',' | sed 's/,$$//'))
	@echo "Running unit tests with coverage..."
	go tool gotestsum --format pkgname -- -race -shuffle on -short -timeout 5m -coverpkg=$(COVERPKGS) -coverprofile=unit-coverage.out -covermode=atomic ./...
	@echo "Running integration tests with coverage..."
	go tool gotestsum --format pkgname -- -race -shuffle on -timeout 10m -coverpkg=$(COVERPKGS) -coverprofile=integration-coverage.out -covermode=atomic ./test/integration/...
	@echo "Running terraform tests with coverage..."
	go tool gotestsum --format pkgname -- -race -timeout 10m -coverpkg=$(COVERPKGS) -coverprofile=terraform-coverage.out -covermode=atomic ./test/terraform/...
	@echo "Running E2E tests with coverage..."
	go tool gotestsum --format pkgname -- -race -shuffle on -timeout 10m -tags=e2e -coverpkg=$(COVERPKGS) -coverprofile=e2e-coverage.out -covermode=atomic ./test/e2e/...
	@echo "Merging coverage profiles..."
	@echo "mode: atomic" > coverage.out
	@tail -n +2 unit-coverage.out >> coverage.out
	@tail -n +2 integration-coverage.out >> coverage.out
	@tail -n +2 terraform-coverage.out >> coverage.out
	@tail -n +2 e2e-coverage.out >> coverage.out
	@rm -f unit-coverage.out integration-coverage.out terraform-coverage.out e2e-coverage.out
	go tool cover -func=coverage.out | tail -1
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

upgrade: upgrade-static install-tofu
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
