# Dev MCP servers

This repo ships a project-scoped `.mcp.json` at the root with three dev-only MCP servers:

| MCP server              | Purpose                                                       | CLI                       |
|-------------------------|---------------------------------------------------------------|---------------------------|
| `gopls`                 | Go symbol lookup, definitions, references via gopls LSP        | `mcp-language-server`     |
| `terraform`             | Provider/registry docs lookup for `test/terraform/` fixtures   | `terraform-mcp-server`    |
| `playwright`            | Browser automation against the dashboard at `:8000/dashboard`  | `npx @playwright/mcp`     |

**These MCPs are dev tooling only. They are never embedded in the gopherstack runtime binary.** The Go process never reads `.mcp.json`. Enterprises that ban MCP at runtime are unaffected — Claude Code (or compatible IDE) on a developer's machine is the only consumer.

## Session start checklist

On every new session in this repo:

1. Run `make dev-mcp-check` (or `bash scripts/dev-mcp-check.sh --quiet`) and confirm the four CLIs (`gopls`, `mcp-language-server`, `terraform-mcp-server`, `npx`) are present.
2. If anything is missing, run `make dev-mcp-install` once.
3. If an MCP tool call fails mid-session, re-run `make dev-mcp-check` before falling back to raw `grep` / `find`.

## Optional: auto-check via SessionStart hook

Add to `.claude/settings.local.json` (gitignored, per-developer):

```json
{
  "hooks": {
    "SessionStart": [
      { "matcher": "*", "hooks": [
        { "type": "command", "command": "bash scripts/dev-mcp-check.sh --quiet" }
      ]}
    ]
  }
}
```
