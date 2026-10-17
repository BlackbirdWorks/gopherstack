# Checkpoint — parity batch, 2026-10-17

## Status (read this first)

**One open PR: #2493**, branch `feat/parity-batch-2026-10-17` (worktree
`~/gs-wt/parity-batch`). ~62 commits ahead of `main`. Every finished item is
merged into it; the user wants **one mega PR per batch**, not a PR per item,
because CI is slow.

- Last push: `fb6d6aff7` + this checkpoint commit. CI was mid-run at stop.
- Recurring CI noise: `Analyze (go)` / `codeql (go)` fail with "The runner has
  received a shutdown signal" (exit 143). That is GitHub runner kills, not code.
  The real code-scanning check is `CodeQL`; its only open alert is GO-2026-5932
  (pre-existing, go.mod, unreachable symbol).
- Earlier in the session: ARC (#2486) and PGO (#2484) merged to `main`.

## What #2493 contains

| Area | bd | Notes |
|---|---|---|
| Organizations async CreateAccount | cefu7.50 | opt-in `--lifecycle-organizations` |
| Organizations created accounts usable | cefu7.53 | `OrganizationAccountAccessRole` + AdministratorAccess in the new account |
| Cognito Identity real STS creds, role mappings, CustomRoleArn, principal tags | cefu7.47 | |
| Generated ID shapes + `TestNoUndeclaredUUIDTruncation` | cefu7.51 | |
| EC2/EFS/Route53Resolver 17-hex IDs (`pkgs/awsid`) | — | |
| IoT MQTT port flag, Jobs topics, policy validation, opt-in cert auth + thing-group policies | cefu7.49 | `--iot-mqtt-port`, `--iot-mqtt-auth` |
| SSM ec2messages agent protocol, Session Manager data channel, port forwarding | cefu7.35 | |
| Backup reports to S3, real restores, async jobs | cefu7.48 | |
| Timestream Query missing-table errors | cefu7.60 | |
| Multi-account: 12-digit access key → account; SQS/DDB/STS per account | cefu7.52, .66 | precedence: header > resolved principal > 12-digit key > default |
| IAM partitioned per account; enforcement fails closed; no nested account locks | cefu7.65 | |
| Outbound HTTP own transport (`pkgs/httpclient`) + guard | cefu7.64 | |
| MemoryDB Redis data plane (`pkgs/redisengine`), ElastiCache/MemoryDB keyspace snapshots | cefu7.57, .61 | |
| DocumentDB / Neptune / Redshift opt-in docker engines | cefu7.54, .55, .56 | Redshift Data API runs real SQL |
| RDS engine: replicas (dump-seeded), snapshot/PITR restores with data, port change | cefu7.59 (partial) | |
| Amazon MQ: all listener ports, TLS, multi-AZ nodes, TLS readiness | cefu7.58 | |
| Test reliability: stack-set async guard, opcensus scope fix, race/flake fixes | l1oo4, zjylp | |

## Unfinished

- **RDS MySQL/MariaDB IAM auth proxy** — stopped mid-task. WIP committed on
  branch `feat/rds-mysql-iam-auth` (`0bde07d48`, pushed, builds and vets, tests
  NOT run, NOT merged into the batch). Remaining for cefu7.59 after it: custom
  cluster endpoint listeners, streaming replication, PITR to the requested time,
  persisted snapshot dumps.

## Waiting on the user

1. **Root directory cleanup (requested).** Move the 264 root `package main`
   files (236 `cli_*`) to `cmd/gopherstack/`; update Makefile, Dockerfiles,
   goreleaser, CI workflows, `default.pgo` location, repo-wide guard tests that
   walk `services/` by relative path, docs. Tidy root clutter (this file could
   move to `docs/`, `PARITY_PHASE4_KICKOFF.md`, `parity.md`, `GEMINI.md`,
   `cli_route_path_gates.txt`, stray `gopherstack` binary). Open question: the
   five `cli_azure*` test files must move with the package — pure moves, but
   they are azure-owned.
2. **"Reset" kill phase.** User defined reset = kill running gopherstack
   servers, `*.test` binaries and `gopherstack-*`/testcontainers containers,
   then clear caches. The cache part is in `.claude/skills/reset-caches/reset.sh`
   (local, gitignored). Adding the kill phase was blocked by the permission
   classifier — needs a permission rule or the user runs it.
3. **New-dependency items** (need go.mod approval): new AWS services
   (cefu7.1/.2/.4/.5/.6), S3 Select Parquet (.36), Lambda micro-VMs (.7),
   Athena SQL engine (.8), testcontainers modules (.12), SDK suites (.11).
4. Remaining large P3s: MWAA real Airflow (.62), Managed Flink jobs (.63).

## Lessons from this session

- Gate pipelines with `set -o pipefail`; `cmd | tail -1 && git push` pushed a
  lint failure once.
- CI unit tests run every package with `-race -shuffle on -short`; agents must
  gate with `-race`. Integration tests (`-tags integration`) only run in CI —
  grep them when tightening validation.
- CodeQL `go/allocation-size-overflow` ignores clamps; build a fixed header and
  `slices.Concat(hdr, payload)`.
- Persistence golden `-update` rewrites unrelated `>` lines to `>`;
  hand-revert so the diff is only your lines (some committed lines legitimately
  contain `>`).
- The bd export hook writes to the main checkout, not worktrees: run
  `bd export -o .beads/issues.jsonl` inside the batch worktree before pushing,
  and re-close issues if a pull re-imports older state.
- `/mnt/fast` filled three times from per-worktree Go build caches; run the
  `reset-caches` skill before it does.
- Agents miss cross-cutting gates; re-run root, lint, persistence and docs on
  the merged batch before every push.
