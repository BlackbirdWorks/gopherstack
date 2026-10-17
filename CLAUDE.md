# Project Instructions for AI Agents

## Project Memories (read these)

Read `.claude/memories/*.md` before writing code:

- `.claude/memories/pkgs-catalog.md` — when to use each `pkgs/` package; locking/map rules (coarse `lockmetrics.RWMutex` per backend, `safemap` only for isolated maps); prefer consolidated pkgs over reimplementing.
- `.claude/memories/parity-principles.md` — no-stub rule, wire-shape verification against the real AWS SDK, known bug classes, de-stub hygiene.

<!-- BEGIN BEADS INTEGRATION v:1 profile:minimal hash:ca08a54f -->
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

## Session Completion

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
   - Run `make bd-audit` and reconcile any TRAILER MISMATCH / TYPO'D TRAILER ID
     findings. Run it on the branch, before the PR is squashed: this repo
     squash-merges, and only 8 of 155 `Closes` trailers survive into main, so
     the trailer is gone by the time main sees it. `bd` never parsed commit
     messages in the first place (gopherstack-101r).
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   git add .beads/issues.jsonl   # bd's auto-export hook usually does this
   git push
   git status  # MUST show "up to date with origin"
   ```

   `.beads/issues.jsonl` is the source of truth for issue state and rides along
   on the normal `git push`. Do NOT run `bd dolt push` — bd runs Dolt embedded
   here with no remote configured, so it prints "No remote is configured —
   skipping" and exits 0. It looks like it worked and does nothing.
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds
<!-- END BEADS INTEGRATION -->


## Conventions & Patterns

### Comments: short and pointed, or none at all

Go is readable on its own. Comments in this repo have grown into walls of
prose that flood agent context and cost more than they explain. Default to
**no comment**. When one earns its place, keep it to a line or two.

Write a comment only when it says something the code cannot:

- **Why**, not what — a non-obvious tradeoff, a constraint, a deliberate
  choice that looks wrong at a glance.
- **A landmine** — "not ctx: cancelled before cleanups run", "do NOT bump
  this, it discards user snapshots".
- **A verified external fact** — an AWS wire shape with its SDK source
  (`serializers.go:6891`), an error code the SDK requires.
- **Godoc on exported identifiers** — one line, as Go convention expects.

Do not write:

- Restatements of the next line (`// increment i`).
- Multi-paragraph essays on how a function works, or narrated history of
  what a past pass changed and why — that belongs in the commit message or
  a bd issue, not the source.
- Section banners, decorative dividers, changelogs in comments.

If the explanation genuinely needs paragraphs, the code probably needs
decomposing instead — and note this repo already bans
cyclop/gocyclo/gocognit/funlen nolints for the same reason.

Cleanup of existing over-commented code is a separate future pass; the rule
applies to everything written from now on.
