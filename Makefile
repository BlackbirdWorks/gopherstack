.PHONY: build ui-install ui-lint ui-check ui-lint-fix ui-fmt ui-fmt-fix ui-test ui-build install-deps install-tofu lint lint-fix test integration-test terraform-test e2e e2e-test total-coverage clean demo all dev-mcp-install dev-mcp-check pgo docs

BINARY_NAME=gopherstack
VERSION_PKG=github.com/blackbirdworks/gopherstack/pkgs/version
BUILD_VERSION=$(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")

build: ui-build
	go build \
		-trimpath \
		-ldflags "-w -s -X $(VERSION_PKG).Build=$(BUILD_VERSION)" \
		-o bin/$(BINARY_NAME) .

ui-install:
	PATH="/opt/homebrew/bin:$(PATH)" npm --prefix ui ci

ui-lint: ui-install
	PATH="/opt/homebrew/bin:$(PATH)" npm --prefix ui run lint

ui-check: ui-install
	PATH="/opt/homebrew/bin:$(PATH)" NODE_OPTIONS="--max-old-space-size=4096" npm --prefix ui run check

ui-lint-fix: ui-install
	PATH="/opt/homebrew/bin:$(PATH)" npm --prefix ui run lint:fix

ui-fmt: ui-install
	PATH="/opt/homebrew/bin:$(PATH)" npm --prefix ui run fmt:check

ui-fmt-fix: ui-install
	PATH="/opt/homebrew/bin:$(PATH)" npm --prefix ui run fmt

ui-test: ui-install
	PATH="/opt/homebrew/bin:$(PATH)" NODE_OPTIONS="--max-old-space-size=4096" npm --prefix ui run test:coverage

ui-build: ui-install
	mkdir -p dashboard/static/spa
	find dashboard/static/spa -mindepth 1 ! -name '.keep' -exec rm -rf {} +
	PATH="/opt/homebrew/bin:$(PATH)" NODE_OPTIONS="--max-old-space-size=4096" npm --prefix ui run build
	touch dashboard/static/spa/.keep

build-linux:
	CGO_ENABLED=0 GOOS=linux go build \
		-trimpath \
		-ldflags "-w -s -X $(VERSION_PKG).Build=$(BUILD_VERSION)" \
		-o bin/$(BINARY_NAME) .

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
	@$(MAKE) --no-print-directory dev-mcp-install

# Dev-only MCP servers consumed by Claude Code / compatible IDE clients via
# the project's .mcp.json. NOT linked into the gopherstack runtime binary.
# Safe to skip in CI / production builds.
dev-mcp-install:
	@echo "Checking dev MCP server CLIs (read by .mcp.json)..."
	@command -v gopls >/dev/null 2>&1 || { \
		echo "Installing gopls..."; \
		go install golang.org/x/tools/gopls@latest; \
	}
	@command -v mcp-language-server >/dev/null 2>&1 || { \
		echo "Installing mcp-language-server..."; \
		go install github.com/isaacphi/mcp-language-server@latest; \
	}
	@command -v terraform-mcp-server >/dev/null 2>&1 || { \
		echo "Installing terraform-mcp-server..."; \
		go install github.com/hashicorp/terraform-mcp-server/cmd/terraform-mcp-server@latest; \
	}
	@command -v npx >/dev/null 2>&1 || echo "WARN: npx not found — Playwright MCP needs Node.js."
	@echo "Dev MCP CLIs ready. Playwright MCP runs on demand via 'npx -y @playwright/mcp@latest'."

dev-mcp-check:
	@bash scripts/dev-mcp-check.sh

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

lint: install-deps ui-lint ui-fmt ui-check
	golangci-lint run --timeout 20m ./...
	go tool govulncheck ./...

lint-fix: install-deps ui-lint-fix ui-fmt-fix
	@echo "Running fieldalignment..."
	fieldalignment -fix ./...
	@echo "Running golangci-lint with --fix..."
	golangci-lint run --fix ./...

test:
	go tool gotestsum --format pkgname -- -race -shuffle on -short ./...

integration-test: build-linux
	go tool gotestsum --format pkgname -- -race -shuffle on -timeout 10m ./test/integration/...

terraform-test: install-tofu
	PATH="$$PWD/bin:$$PATH" go tool gotestsum --format pkgname -- -v -race -parallel 8 -timeout 45m ./test/terraform/...

e2e: e2e-test

e2e-test: ui-build
	go tool gotestsum --format pkgname -- -race -shuffle on -timeout 10m -tags=e2e ./test/e2e/...

total-coverage: build-linux
	$(eval COVERPKGS := $(shell go list ./... | grep -v -E '(test/|/demo$$|/modules/|/teststack$$)' | tr '\n' ',' | sed 's/,$$//'))
	@echo "Running unit tests with coverage..."
	go tool gotestsum --format pkgname -- -race -shuffle on -short -timeout 5m -coverpkg=$(COVERPKGS) -coverprofile=unit-coverage.out -covermode=atomic ./...
	@echo "Running integration tests with coverage..."
	go tool gotestsum --format pkgname -- -race -shuffle on -timeout 10m -coverpkg=$(COVERPKGS) -coverprofile=integration-coverage.out -covermode=atomic ./test/integration/...
	@echo "Running terraform tests with coverage..."
	go tool gotestsum --format pkgname -- -race -parallel 8 -timeout 20m -coverpkg=$(COVERPKGS) -coverprofile=terraform-coverage.out -covermode=atomic ./test/terraform/...
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

upgrade-static:
	@echo "Static assets are managed by npm in the ui directory."

upgrade: upgrade-static install-tofu
	go get -u ./...
	go mod tidy


bench:
	go test -bench=. -benchmem ./...

# Regenerate the repo-root default.pgo Profile-Guided Optimization profile
# that `go build` auto-consumes. See scripts/pgo.sh and the "Profile-Guided
# Optimization (PGO)" section in README.md for details and knobs.
pgo:
	bash scripts/pgo.sh

# Regenerate per-service README.md files from services/<svc>/PARITY.md audit
# manifests, and the category-grouped service table injected into the root
# README.md. See cmd/gendocs.
docs:
	go run ./cmd/gendocs

demo: ui-build
	docker compose down
	docker compose build
	docker compose up -d

all: 
	make lint-fix
	make total-coverage
