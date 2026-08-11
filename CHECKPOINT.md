# Checkpoint — parity campaign handoff

Branch `chore/parity-upgrade`, tip `494298f97`, level with origin, tree clean.

## State

CI is green and the branch is merge-ready. Verified at the end of the session:

| Gate | Result |
|---|---|
| `go build ./...` | clean |
| `go vet ./...` | clean |
| `golangci-lint run ./...` | 0 issues |
| `go test ./...` | exit 0 |
| `make check-pins` | 161/161 |
| `make docs` + `git diff --exit-code` | clean |

Backlog: 80 ready (`bd ready`). Nothing is stranded mid-flight.

## Blockers for the next session

1. **Subagent cap hit (200/200).** Raise `CLAUDE_CODE_MAX_SUBAGENTS_PER_SESSION` or no dispatching is possible.
2. **`bd dolt push` is a no-op** (filed). No remote configured; it prints usage and exits 0. The real source of truth is `.beads/issues.jsonl` in git.
3. **Agents lint package-scoped** (filed). `test/integration` is linted by nobody until a repo-wide run — that let a `govet` shadow reach a commit. Run `golangci-lint run ./...` before committing, not just the touched service.

## How the loop ran

Orchestrate only; ≤2 sonnet subagents; verify every claim independently before committing. Each dispatch carried: no nested subagents, no commit/push, no `make docs` (orchestrator regenerates), `git stash` banned, comments default to none, table-driven tests with `t.Parallel()` in outer and each subtest, never nolint cyclop/gocyclo/gocognit/funlen.

**Verification that actually caught things**, in order of value:

- **Neuter the fix, watch the test go red.** Non-negotiable. Ten of my own neuter attempts produced false greens — three broke compilation (orphaned variable, unused import), one missed its target line, one drove a code path the test never reached, one was a no-op edit (deleted and reinserted the same block), one missed an `,omitempty` suffix. **Always confirm the edit landed before trusting either colour.**
- **Check the SDK/botocore claim yourself.** Doc comments wrap across lines, so a grep for a whole sentence misses them. Deserializers use `strings.EqualFold("X", errorCode)`, not literal `case "X"`.
- **Repeat race checks 20 times.** A 1-in-6 race shows zero in six runs about a third of the time; that nearly produced a false "already fixed".
- **Spot-check the issue before dispatching.** Two issues were stale, two of mine had wrong scope, one premise was simply false.

## Bug classes that kept paying

Ranked by how often they were real:

1. **Request field read under a name the API never sends** — value silently discarded, call reports success. Invisible to compilation and to any test written against the same wrong name. Only a model-vs-struct diff finds it. 16 real bugs across two protocol families. Worst instance returned *another task's data* because an operation was copy-pasted from its sibling.
2. **Parameter parsed then ignored** — filters and flags accepted and dropped, so a caller believes the request narrowed something. Eight instances, including one hardcoded `false` while the backend already implemented the behaviour.
3. **Accepting an ID for a resource that does not exist** and reporting success. Ten services. The tell is asymmetry: create validates, update doesn't; or one sibling in the same file checks and the others don't.
4. **State mutated before validation** — rejected request already wrote some fields. 15 instances.
5. **Fabricated wire fields/values** — four cases where a field name or enum value simply doesn't exist on the real type.
6. **More restrictive than AWS** — rarer but real: an allowlist holding 42 of an enum's 117 values, a policy limit set to the wrong constant, deletes refusing resources the API deletes idempotently.

## Judgement rules that held up

- **Absent beats plausible-but-wrong.** A fabricated formula, metric or limit that a client can read and act on is worse than an obvious gap. One invented sampling formula was reverted for this.
- **Audit before fixing, and report the table even if you fix nothing.** Ratios: 48 error-type suspects → 6 real; 62 enum allowlists → 9; 131 wire candidates → 16; 66 low-confidence → 4. Treating a candidate list as a defect list would have produced dozens of wrong changes.
- **Prefer under-enforcing to guessing.** Adding validation is how you create the opposite bug. Where the model doesn't pin a rule, leave the case accepted and say so.
- **A recorded excuse is a hypothesis.** "Tests depend on the lenient behaviour" collapsed twice — the tests were asserting the gap. But three other blockers held up under the same scrutiny, so test each rather than assuming either way.
- **Dead plumbing reads as working.** A filter over a permanently-empty list, or a detail flag gating nothing, is worse than the absence. Declined several times deliberately.
- **Size nested shapes before typing them.** Model the shallow ones, leave genuinely deep ones opaque — a half-modelled structure can't be distinguished from an unimplemented one by a client.

## Entrenching tests

~80 tests in this campaign asserted gopherstack's own broken output — a nonexistent enum value, a fabricated ARN, a wrong wire name, an invalid schedule expression. When a test blocks a fix, check whether the test is the bug before working around it. Several were the only thing holding a bug in place.

## Repo specifics worth keeping

- `goconst` reports the **oldest** occurrence, so it can name a line you never touched while your new literal is the cause. Prove attribution by removing your file and re-running.
- `make docs` regenerates READMEs from `PARITY.md`. Two agents each reverting the other's rows shipped a stale docs commit twice. **One party owns regeneration** — the orchestrator, at commit time.
- `git restore` on a file with unstaged work reverts to the index and silently discards it. That shipped a broken pin gate.
- Concurrent agents share the working tree, so a root-scoped gate is only trustworthy in a quiet moment. Build an isolated `git worktree` when it matters.
- Snapshot versions: never bump. Additive `omitempty` fields only; round-trip them through persistence and test it.
