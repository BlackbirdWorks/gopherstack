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

`make lint` also runs mulint (mutex misuse detection) via `go vet -vettool`.
mulint cannot follow mutex wrappers across package boundaries, so it does
**not** see `lockmetrics.RWMutex` (used almost everywhere in this repo) — it
only guards direct `sync.Mutex`/`sync.RWMutex` use.


**Logging — context-aware only (no exceptions):**
- NEVER call `slog.Default()` or construct an ad-hoc `slog.New(...)` in service/production
  code, and NEVER embed or store a `*slog.Logger` on a struct/global. The single allowed
  `slog.Default()` is the fallback inside `pkgs/logger.Load`; the single allowed `slog.New`
  is `pkgs/logger.NewLogger`.
- Always fetch the logger from context: `logger.Load(ctx)`, and prefer the `*Context`
  methods (`InfoContext`/`WarnContext`/...) so the request/worker fields propagate.
- The root logger is created once at startup and injected into `ctx`. Enrich it, never
  replace it: `logger.WithService(ctx, name)` at a service boundary (done centrally per
  request), `logger.AddAttrs(ctx, ...)` for extra fields, and
  `logger.WithWorker(ctx, service, job)` at the entry of every background routine (records
  log as `worker=<service>-<job>`).
- Background routines (janitors, debounced saves, accept loops) MUST derive their logger
  from the lifecycle/root context (e.g. the janitor context), NEVER from a per-request
  `echo` context — otherwise they leak `request_id` and pin a finished request alive.
- The logger is a pointer carried in `ctx`; enrichment is copy-on-write (`slog.Logger.With`
  returns a new logger). Do NOT mutate it, do NOT call `slog.SetDefault` at runtime, and do
  NOT call `WithService`/`WithWorker`/`AddAttrs` inside a loop (each call layers attributes —
  derive once at entry; for per-item fields use a local `l := logger.Load(ctx).With(...)`).
- Do NOT introduce `context.Background()`/`context.TODO()` in service code or tests to
  satisfy a signature — thread the real request/root context (in tests use `t.Context()`).

**Checkpoint commits for long tasks:** For tasks taking >1 hour, commit and push every ~30 minutes:
```bash
git add -A && git commit -m "WIP: checkpoint" && git push origin HEAD
```
This prevents losing 3+ hours of work if your session dies (rate limits kill sessions after ~180min).
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

## Caveman mode

Respond terse like smart caveman. All technical substance stay. Only fluff die.

Rules:
- Drop: articles (a/an/the), filler (just/really/basically), pleasantries, hedging
- Fragments OK. Short synonyms. Technical terms exact. Code unchanged.
- Pattern: [thing] [action] [reason]. [next step].
- Not: "Sure! I'd be happy to help you with that."
- Yes: "Bug in auth middleware. Fix:"

Switch level: /caveman lite|full|ultra|wenyan
Stop: "stop caveman" or "normal mode"

Auto-Clarity: drop caveman for security warnings, irreversible actions, user confused. Resume after.

Boundaries: code/commits/PRs written normal.

## Dev MCP servers (read on session start)

`.mcp.json` at repo root wires dev-only MCPs: `gopls` (Go symbols), `terraform` (provider docs), `playwright` (dashboard browser). Dev tooling only — never embedded in the gopherstack runtime binary.

On session start: run `make dev-mcp-check`. If CLIs missing, run `make dev-mcp-install`. Details: `.agent/rules/dev-mcp.md`.

<!-- BEGIN BEADS INTEGRATION v:1 profile:minimal hash:46cd31e7 -->
## Beads Issue Tracker

This project uses **bd (beads)** for issue tracking. Run `bd prime` to see full workflow context and commands.

### Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --claim  # Claim work
bd close <id>         # Complete work
```

### Rules

- Use `bd` for ALL task tracking — do NOT use TodoWrite, TaskCreate, or markdown TODO lists
- Run `bd prime` for detailed command reference and session close protocol
- Use `bd remember` for persistent knowledge — do NOT use MEMORY.md files

**Architecture in one line:** issues live in a local Dolt DB; sync uses `refs/dolt/data` on your git remote; `.beads/issues.jsonl` is a passive export. See https://github.com/gastownhall/beads/blob/main/docs/core-concepts/sync-concepts.md for details and anti-patterns.

## Agent Context Profiles

The managed Beads block is task-tracking guidance, not permission to override repository, user, or orchestrator instructions.

- **Conservative (default)**: Use `bd` for task tracking. Do not run git commits, git pushes, or Dolt remote sync unless explicitly asked. At handoff, report changed files, validation, and suggested next commands.
- **Minimal**: Keep tool instruction files as pointers to `bd prime`; use the same conservative git policy unless active instructions say otherwise.
- **Team-maintainer**: Only when the repository explicitly opts in, agents may close beads, run quality gates, commit, and push as part of session close. A current "do not commit" or "do not push" instruction still wins.

## Session Completion

This protocol applies when ending a Beads implementation workflow. It is subordinate to explicit user, repository, and orchestrator instructions.

1. **File issues for remaining work** - Create beads for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **Handle git/sync by active profile**:
   ```bash
   # Conservative/minimal/default: report status and proposed commands; wait for approval.
   git status

   # Team-maintainer opt-in only, unless current instructions forbid it:
   git pull --rebase
   bd dolt push
   git push
   git status
   ```
5. **Hand off** - Summarize changes, validation, issue status, and any blocked sync/commit/push step

**Critical rules:**
- Explicit user or orchestrator instructions override this Beads block.
- Do not commit or push without clear authority from the active profile or the current user request.
- If a required sync or push is blocked, stop and report the exact command and error.
<!-- END BEADS INTEGRATION -->

<!-- BEGIN BEADS CODEX SETUP: generated by bd setup codex -->
## Beads Issue Tracker

Use Beads (`bd`) for durable task tracking in repositories that include it. Use the `beads` skill at `.agents/skills/beads/SKILL.md` (project install) or `~/.agents/skills/beads/SKILL.md` (global install) for Beads workflow guidance, then use the `bd` CLI for issue operations.

### Quick Reference

```bash
bd ready                # Find available work
bd show <id>            # View issue details
bd update <id> --claim  # Claim work
bd close <id>           # Complete work
bd prime                # Refresh Beads context
```

### Rules

- Use `bd` for all task tracking; do not create markdown TODO lists.
- Run `bd prime` when Beads context is missing or stale. Codex 0.129.0+ can load Beads context automatically through native hooks; use `/hooks` to inspect or toggle them.
- Keep persistent project memory in Beads via `bd remember`; do not create ad hoc memory files.

**Architecture in one line:** issues live in a local Dolt DB; sync uses `refs/dolt/data` on your git remote; `.beads/issues.jsonl` is a passive export. See https://github.com/gastownhall/beads/blob/main/docs/core-concepts/sync-concepts.md for details and anti-patterns.
<!-- END BEADS CODEX SETUP -->
