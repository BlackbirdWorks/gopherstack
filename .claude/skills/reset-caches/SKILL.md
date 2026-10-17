---
name: reset-caches
description: Whole-system dev-box reset — kill running gopherstack servers, test binaries and gopherstack/testcontainers containers, then clear Go/lint/Docker caches, stale temp dirs, old task logs and clean pushed worktrees. Use when the user says "reset". For cache-only cleanup (e.g. disk filling mid-work) use --soft.
---

Run `.claude/skills/reset-caches/reset.sh` and report what was stopped and the before/after free space.

- "Reset" means a whole-system reset (user, 2026-10-17): it stops running `gopherstack serve` processes (including the long-running one on 8123/8081/10000-10002/1883), `*.test` binaries, and every `gopherstack-*` or testcontainers container, so no stale test state survives. Agents' in-flight test containers die too — only run it when the user asks.
- `--soft`: skip the kill phase; cache cleanup only, safe while builds run (entries younger than `AGE_MIN`=60 min kept if a Go build is active).
- `--force`: wipe the Go cache fully even if a build is active (implied by a full reset).
- `--other-scratch`: also remove other Claude sessions' scratch dirs untouched for a day.
- Worktrees: removes every linked worktree that is clean and fully pushed; dirty or unpushed ones are listed and kept.
- Docker: prunes unused images, build cache and unattached volumes after the kill phase.
- Never deletes the module cache (`go env GOMODCACHE`) or anything tracked in the repo.
