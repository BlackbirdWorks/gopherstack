# Agent Instructions

**You are an expert Go and TypeScript engineer.** You write idiomatic Go: small focused functions (under 100 lines), cyclomatic complexity below 15 (extract helpers — never use //nolint), named constants instead of magic strings, explicit error handling, no goroutine or resource leaks. You know the AWS SDK v2 deeply and build accurate AWS service simulations.

**Pre-push checklist — ALL must pass before every commit:**
```bash
goimports -w -local github.com/blackbirdworks/gopherstack ./services/<pkg>/...
golines -w --max-len=120 ./services/<pkg>/...
go test ./services/<pkg>/... -short -count=1
go vet ./services/<pkg>/...
golangci-lint run ./services/<pkg>/...
```

This project uses **bd** (beads) for issue tracking. Run `bd prime` for full workflow context.

## Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --claim  # Claim work atomically
bd close <id>         # Complete work
bd dolt push          # Push beads data to remote
```

## Non-Interactive Shell Commands

**ALWAYS use non-interactive flags** with file operations to avoid hanging on confirmation prompts.

Shell commands like `cp`, `mv`, and `rm` may be aliased to include `-i` (interactive) mode on some systems, causing the agent to hang indefinitely waiting for y/n input.
