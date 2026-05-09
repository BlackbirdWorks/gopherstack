#!/usr/bin/env bash
# dev-mcp-check.sh — verify dev MCP servers + their CLIs are present.
# Dev tooling only. Not used by the gopherstack runtime.
#
# Run manually or wire into Claude Code via .claude/settings.local.json:
#   {
#     "hooks": {
#       "SessionStart": [
#         { "matcher": "*", "hooks": [{ "type": "command",
#             "command": "bash scripts/dev-mcp-check.sh --quiet" }] }
#       ]
#     }
#   }

set -u

QUIET=0
[[ "${1:-}" == "--quiet" ]] && QUIET=1

say() { [[ "$QUIET" -eq 1 ]] || echo "$@"; }
warn() { echo "WARN: $*" >&2; }

missing=()
check_bin() {
  local bin="$1" hint="$2"
  if command -v "$bin" >/dev/null 2>&1; then
    say "  ok  $bin"
  else
    warn "$bin missing — $hint"
    missing+=("$bin")
  fi
}

say "dev MCP check (.mcp.json):"
check_bin gopls                "go install golang.org/x/tools/gopls@latest"
check_bin mcp-language-server  "go install github.com/isaacphi/mcp-language-server@latest"
check_bin terraform-mcp-server "go install github.com/hashicorp/terraform-mcp-server/cmd/terraform-mcp-server@latest"
check_bin npx                  "install Node.js (Playwright MCP runs via npx)"

if [[ ! -f .mcp.json ]]; then
  warn ".mcp.json not found at repo root"
  exit 1
fi

if [[ ${#missing[@]} -gt 0 ]]; then
  warn "missing: ${missing[*]} — run: make dev-mcp-install"
  exit 0   # non-fatal; agents can still operate without MCPs
fi

say "all dev MCP CLIs present."
