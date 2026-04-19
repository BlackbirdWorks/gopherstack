# Checkpoint

## Status

All 22 improvements for Lambda service refinement 2 have been **fully implemented and committed** locally. The commit is ready to push but cannot be pushed due to token permission constraints (the session's `GITHUB_TOKEN` only has `contents: read` permission — no write access).

## Commit Ready to Push

**Local commit SHA**: `53841b9`  
**Branch**: `copilot/add-missing-sdk-operations`  
**Remote current SHA**: `1d6a0e946a23e184ec7b664793b71d1c4e3e0b54`

To push: `git push origin copilot/add-missing-sdk-operations`

## What Was Done

### Group 1 — Backend correctness (items 1-7) ✅
- **GetVersion**: O(1) `versionIndex` lookup; returns `ErrFunctionNotFound` vs `ErrVersionNotFound` correctly
- **UpdateEventSourceMapping**: sets `LastModified` on every change
- **CreateEventSourceMapping**: validates referenced function exists (`ErrFunctionNotFound`); handles ARN-format `FunctionName`
- **UpdateAlias**: regenerates `RevisionID` on every update
- **FunctionConfiguration**: adds `Tags map[string]string`; initialized in `CreateFunction`
- **TagResource/UntagResource**: store/remove tags directly on `FunctionConfiguration.Tags`
- **fnToVersion**: copies `CodeSha256` from live config to version snapshot

### Group 2 — Runtime API fix (item 8) ✅
- **handleInitError**: delivers error to first pending invocation

### Group 3 — Performance / concurrency (items 9-11) ✅
- **evictLRURuntimeLocked**: applies `cleanupSem` pattern
- **pushInvocationLog**: runs asynchronously (`go` goroutine) on both sync and async invoke paths
- **FunctionURLConfig**: adds `Cors *FunctionURLCors` field

### Group 4 — UI improvements (items 12-18) ✅
- Function list: Memory and Timeout columns
- Function list: State badge (Active/Failed/other) on each row
- Function detail: Invoke button + invoke modal
- Function detail: CodeSha256 card and Description card
- Function detail: Delete Function button
- Create modal: Description field
- Function list: Container badge for Image package type

### Group 5 — Tests (items 19-22) ✅
- `TestLambda_GetVersion_UsesIndex` (version_index_test.go)
- `TestLambda_CreateESM_FunctionMustExist` (esm_test.go)
- `TestLambda_UpdateESM_UpdatesLastModified` (esm_test.go)
- `TestLambda_PushInvocationLog_NonBlocking` (invocation_log_test.go)
- `PushInvocationLog` exported in `export_test.go`

## Validation Status
- ✅ `go test ./services/lambda/... -count=1 -short -timeout=120s` — PASS
- ✅ `make lint-fix` — PASS (0 issues)
- ✅ `make build` — PASS
- ✅ CodeQL Security Scan — 0 alerts

## Blocker
Cannot push: `COPILOT_AGENT_PERMISSIONS` is `contents: read` only. The `GH_TOKEN` secret mentioned in the task prompt was not injected into the environment (`COPILOT_AGENT_INJECTED_SECRET_NAMES` is empty).
