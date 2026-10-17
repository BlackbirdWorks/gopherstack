# Project Instructions for AI Agents

## pkgs/ usage and locking

Reuse `pkgs/` instead of reimplementing in a service (e.g. `awstime.Epoch` would have prevented the QuickSight/IoT epoch-timestamp bugs).

- Lock granularity follows invariant granularity. One coarse `lockmetrics.RWMutex` per service backend guards all its maps; AWS ops are cross-map transactions and `Snapshot()` needs one consistent view.
- `safemap.Map` only for genuinely isolated single-map state (token stores, caches, janitor-swept stores). Never for backend resource maps: per-map locks tear cross-map state and risk deadlock.
- No raw `sync.Mutex`/`sync.RWMutex` in services.
- Per-map init/Reset/Snapshot/Restore boilerplate is a lifecycle problem, not a locking one (planned fix: `gopherstack-drp`). Persistence JSON field names must be preserved exactly.
- ARNs: `arn`, never hand-format. IDs: `awsid.New("vpc-")`, not `uuid.NewString()[:N]`.
- Timestamps: `awstime`; `awstime.Epoch()` for epoch-seconds JSON numbers.
- Pagination: `page.Page[T]`, not hand-rolled NextToken/cursor logic.
- Case-insensitive identifiers (e.g. RDS DBInstanceIdentifier): `strs` (`Fold`, `Equal`, `ContainsFold`), not per-service `strings.ToLower`/`EqualFold`.
- Outbound HTTP: `httpclient` (own cloned transport); never `&http.Client{}` without a Transport (guarded by `TestNoOutboundHTTPOnDefaultTransport`).
- All logging goes through `pkgs/logger`. Nil-safe pointer deref: `ptrconv`. Tags: `tags`. Shared errors: `awserr`.

## AWS parity rules

- No stubs: no no-op handlers, hardcoded/empty returns, fabricated IDs, or `&StubOutput{}`. Every routed SDK op must mutate/read real backend state, return AWS-accurate shapes and error codes, and persist (wire into the service's `persistence.go` `backendSnapshot`). If an op can't be implemented yet, say so; never ship a half-working stub.
- Verify wire shapes against the real `aws-sdk-go-v2` serializers/deserializers, not your handler's own output. Known bug classes:
  - wrong XML list wrappers (`<member>` vs `<Item>`), wrong root elements, missing response-root nesting when the output has no httpPayload member
  - ISO8601 strings where the JSON protocol expects epoch-seconds numbers
  - a hardcoded `xml:"StubResponse"` XMLName overriding the runtime root
  - stub registrations overwriting real handlers (EC2 uses `registerStubOpsIfAbsent`; real handlers must win)
  - missing `errCodeLookup` entries surfacing not-found as 500 InternalFailure
- Unit tests are not parity proof. SDK-driven `test/integration/*_parity_test.go` caught 8 client-breaking wire bugs that green unit tests missed. Run `make build-linux` before `go test ./test/integration/...`.
- Grep-based stub hunting has false positives: an empty envelope returned AFTER real backend logic is correct (void-result ops). Read the backend method before flagging; conversely, a real-looking op may filter a never-populated map.
- De-stubbing: remove the stub registration, delete the orphaned stub handler, drop it from bare-call stub test manifests, run `golangci-lint`.
- PARITY.md front-matter `items_still_open:` is the only authoritative open list (`gopherstack-anjf`); never `gaps:`/`deferred:` or body text. A pass that fixes an item must remove it in the same commit. Dated body sections are history only (`make parity-lint` catches stale claims).

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
