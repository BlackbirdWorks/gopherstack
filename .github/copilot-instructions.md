# Session Management

- Commit and push after completing each task.
- Your max session is 1 hour — when approaching the limit, create `checkpoint.md` at the repo root (what is done, what remains, any blockers) and push. Remove `checkpoint.md` when the full issue is complete.
- Run `make test` after each task before committing. Resolve all lint issues via `make lint-fix`.
- Min test coverage is 85%.
- Add integration tests in `test/integration/` as you go for everything you implement.
- Run `make build` before pushing.
