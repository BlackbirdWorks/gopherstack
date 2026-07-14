# Parity Phase 4 — Kickoff

**Branch:** `parity-4` (off `main`). **Project:** gopherstack — a LocalStack-style AWS emulator (Go 1.26, module `github.com/blackbirdworks/gopherstack`).

> Read this first, then `bd prime` (beads context) and `.claude/memories/*.md`.

---

## North star — LocalStack parity

gopherstack emulates AWS. The goal is **parity with LocalStack**: first Community-level per-service correctness (done for the top services), then **LocalStack Pro cross-service interconnectivity** (Phase 4, this branch). Deep, gated-green, no stubs.

## Phase status

- **Phase 2 — Parity sweep, top-30 AWS services** ✅ done (on `main`). ~19k LOC genuine fixes; every service found real bugs. Each service has a `services/<svc>/PARITY.md` manifest.
- **Phase 3 — Datalayer refactor to `pkgs/store`** ✅ complete but **NOT on this branch** — it lives on `parity-sweep-3` / PR #2382 (203 commits, unmerged into `main` as of parity-4 creation). All 154 backends converted from raw maps to the generic `pkgs/store` datalayer (`Table[V]`/`Registry`/`Index[V]`), plus follow-up fixes. **Phase 4 branches off `main` per instruction, so it does NOT include the datalayer work** — reconcile once #2382 merges (rebase parity-4 on updated main, or merge main in). Phase 4 (cli.go interconnect wiring) is largely independent of whether backends use maps or store, so this is workable but keep it in mind for any backend-touching change.
- **Phase 4 — LocalStack Pro interconnectivity parity** ← THIS BRANCH (`gopherstack-wdw`).

## Phase 4 scope — LocalStack Pro cross-service interconnectivity

Match LocalStack **Pro**'s cross-service matrix. The deliverable is a **web-verified Pro-vs-gopherstack gap artifact**, then implementing the missing links with **real delivery, no stubs**.

1. **Web-research** LocalStack Pro's interconnectivity matrix (docs.localstack.cloud): IAM enforcement, real ECS/EKS/Fargate execution, EventBridge Pipes, full Step Functions service integrations, Cognito Lambda triggers, advanced API Gateway, App services, X-Ray, etc.
2. **Diff** that matrix against gopherstack's current cross-service wiring (primarily `cli.go` — where SNS→Lambda/Firehose, EventBridge targets, etc. are wired) and the per-service PARITY.md ledgers.
3. **Implement** the missing links — real event delivery/execution, no disguised no-ops.
4. **Deliverable:** the web-verified Pro-vs-gopherstack matrix artifact + closed gaps.

Known Community-level interconnect gaps already filed (check bd): EventBridge non-core targets, ASG→EC2, ASG/ECS→ELBv2.

---

## The workflow — "parity sweep" methodology

**Orchestration (global VM directive):** main thread orchestrates; delegate substantive work to **≤2 concurrent `sonnet` subagents**, delegation depth 1 (subagents never spawn subagents). Main thread only: plan / dispatch / verify / integrate / git.

**Per service (audit or fix):**
- One sonnet subagent, depth-1, edits ONLY its `services/<svc>/`. Shared `pkgs/`/`cli.go` findings → REPORT as follow-up (never edit from a per-service agent — parallel agents collide on shared files).
- **git-safety ABSOLUTE:** subagents use read-only git only (`status`/`diff`/`show`/`log`) — NO stash/checkout/reset/add/commit. The **main thread commits** (`git add services/<svc>/` + push).
- Gate **scoped** to the service (NOT whole-repo `go build ./...` — it times out ~3min): `go build ./services/<svc>/...` + `-race` test + `go vet` + `go fix -diff` (empty) + `golangci-lint run ./services/<svc>/...` (0 issues).
- **No-stub rule (core):** never ship stub/disguised-no-op methods. Verify wire-shape against the real AWS SDK.
- Each audit **writes/updates `services/<svc>/PARITY.md`** (schema: `services/_PARITY_TEMPLATE.md`) — YAML-frontmatter ledger: `last_audit_commit`, `sdk_module`+version, per-op/family `{wire,errors,state,persist}` status, `gaps` (with bd ids), `deferred`, `leaks`.
- **Re-audit protocol:** don't rescan a service — read its PARITY.md, `git diff <last_audit_commit>..HEAD -- services/<svc>/`, check the SDK for new ops; audit only changed/new surface, trust unchanged `ok` rows.

**Cross-service interconnect (the Pro-parity heart of Phase 4):** wired in `cli.go` (real delivery, no stubs). Since `cli.go` is shared, interconnect changes are **main-thread work** or serialized single-agent — never parallel per-service agents.

**Signature-safety:** exported-signature changes used outside a service (esp. `cli.go`) → report prominently, prefer additive. (History: a cloudwatch signature change broke cli.go PutMetricData; a PutEvents signature change broke two cli.go call sites — both cross-package, missed by scoped gates.)

**Tests:** table-driven (`Test_Thing` + cases + `t.Run`, isolated per-subtest state).

## Tracking & conventions

- **bd (beads)** for ALL task tracking — `bd prime` for full workflow, `bd ready` / `bd show <id>` / `bd update <id> --claim` / `bd close <id>`. Do NOT use TodoWrite/markdown TODOs.
- **`bd remember`** for persistent knowledge (NOT MEMORY.md). Search: `bd memories <keyword>`.
- Read `.claude/memories/pkgs-catalog.md` and `.claude/memories/parity-principles.md` before coding.
- **Session close:** `git pull --rebase` → `bd dolt push` → `git push` → confirm "up to date with origin".

## Key references

- bd memories: `parity-principle-never-ship-stub-methods`, `pkgs-catalog`, `parity-manifest-per-service`, `test-style-table-tests`, `file-naming-descriptive`, `no-nolint-funlen-cyclo-refactor-instead`.
- `services/_PARITY_TEMPLATE.md` — the PARITY.md schema.
- `cli.go` — cross-service interconnect wiring (the Phase 4 focus).

## Resume command (new session)

```
git status && git log --oneline -5 && bd prime && bd ready
```
Then read this file + `.claude/memories/*.md`, and start Phase 4 at step 1 (web-research the LocalStack Pro matrix).
