---
name: reset-caches
description: Reclaim disk on this dev box — Go build cache, stale go-build temp dirs, golangci-lint cache, stray agent caches, old task logs, clean pushed worktrees, and unused Docker images/build cache/volumes. Use when the user says "reset", "clean up caches", or disk is filling.
---

Run `.claude/skills/reset-caches/reset.sh` and report the before/after free space.

- Default is safe while builds run: if a Go compile/link or golangci-lint is active, cache entries younger than `AGE_MIN` (60) minutes are kept. Pass `--force` only when nothing is building.
- `--other-scratch` also removes other Claude sessions' scratch dirs untouched for a day — only when the user asks.
- Worktrees: removes every linked worktree that is clean and fully pushed; dirty or unpushed ones are listed and kept.
- Docker: prunes unused images, build cache and unattached volumes; running containers and their volumes are untouched. Never stop or kill containers or processes (user processes on 8123/8081/10000-10002/1883).
- Never deletes `~/go/pkg/mod` (module cache) or anything in the repo itself.
