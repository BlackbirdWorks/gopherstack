# Checkpoint — Refinement Check 3 of 3 (STS)

## Status: COMPLETE (push blocked by token permissions)

## What is Done
All work is committed locally in 2 commits ahead of `origin/copilot/polish-ui-credential-display`:

### Commit `0217a30` — Backend changes
- MFA validation: `GetSessionToken` with `SerialNumber` requires `TokenCode`
- Tag count enforcement in `AssumeRole` and `GetFederationToken` (max 50)
- Audience count enforcement in `GetWebIdentityToken` (max 10)
- Proportional `calculatePackedPolicySize()` (replaces hardcoded `50`)
- `GetFederationToken` accepts `PolicyArns`
- `Reset()` zeroes all 11 atomic op counters
- New errors: `ErrMFACodeRequired`, `ErrTooManyTags`, `ErrTooManyAudiences`

### Commit `38d94ca` — UI + integration tests
- UI: `formatDuration` helper (Xh Ym format)
- UI: AssumeRole duration slider + ExternalId field + session token preview
- UI: MFA serial/token code fields in Session Token section
- UI: Full GetFederationToken section (name, duration, credentials)
- UI: validator input `id="validator-access-key-id"`
- UI: all 11 op counters in `opCountCards`
- Integration tests: GetSessionToken, GetFederationToken, DecodeAuthorizationMessage, GetAccessKeyInfo, AssumeRole tag count exceeded

## Blocker
`git push` returns HTTP 403 — token lacks `repo` write scope.
Run: `git push origin copilot/polish-ui-credential-display`

## Verification
- `make test` → 25,945 tests pass, 0 failures
- `make lint` → 0 issues
- `make build` → success
- `go build -tags=e2e ./test/e2e/...` → success
