---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: account
sdk_module: aws-sdk-go-v2/service/account@v1 (audited against the public AWS Account
  Management API reference https://docs.aws.amazon.com/accounts/latest/reference/,
  not a vendored SDK copy -- aws-sdk-go-v2/service/account is not in this repo's
  go.sum, so botocore/service-2.json + the API reference docs were used as the
  wire-shape source of truth)
last_audit_commit: cafbb3e2
last_audit_date: 2026-07-13
overall: A            # fresh audit found a full wire-protocol rewrite's worth of genuine bugs
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  GetContactInformation: {wire: ok, errors: ok, state: ok, persist: ok}
  PutContactInformation: {wire: ok, errors: ok, state: ok, persist: ok, note: now validates the 6 AWS-required fields (AddressLine1/City/CountryCode/FullName/PhoneNumber/PostalCode); previously unvalidated}
  GetAlternateContact: {wire: ok, errors: ok, state: ok, persist: ok}
  PutAlternateContact: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAlternateContact: {wire: ok, errors: ok, state: ok, persist: ok}
  ListRegions: {wire: ok, errors: ok, state: ok, persist: ok, note: MaxResults now range-validated (1-50) per AWS contract}
  GetRegionOptStatus: {wire: ok, errors: ok, state: ok, persist: ok}
  EnableRegion: {wire: ok, errors: ok, state: ok, persist: ok, note: "transitions straight to terminal ENABLED rather than through a transient ENABLING window -- see deferred"}
  DisableRegion: {wire: ok, errors: ok, state: ok, persist: ok, note: "same immediate-terminal-state simplification as EnableRegion (DISABLING)"}
  GetPrimaryEmail: {wire: ok, errors: ok, state: ok, persist: ok, note: AccountId is required per AWS contract; now validated (was previously unenforced/nonexistent)}
  StartPrimaryEmailUpdate: {wire: ok, errors: ok, state: ok, persist: ok, note: AccountId now required per AWS contract}
  AcceptPrimaryEmailUpdate: {wire: ok, errors: ok, state: ok, persist: ok, note: AccountId now required per AWS contract}
  GetAccountInformation: {wire: ok, errors: ok, state: ok, persist: ok, note: "operation did not exist before this audit -- only a fictitious DescribeAccount stood in for it"}
  PutAccountName: {wire: ok, errors: ok, state: ok, persist: ok}
# Families audited as a group (when per-op is impractical):
families:
  routing: {status: ok, note: "full rewrite -- see bugs below"}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "Account Management is not wired into cli.go's Provider list (no `&accountbackend.Provider{}` entry, no import) -- the service is unreachable from the running gopherstack server even after this fix. Added services/account/provider.go (matching the workspaces/organizations Provider pattern) so wiring is a one-line cli.go change, but cli.go is out of scope for this sweep (hard constraint: no shared-file edits). Needs a bd issue + a cli.go-touching follow-up PR."
  - "AccountId targeting of org member accounts is not modeled: GetAlternateContact/PutAlternateContact/DeleteAlternateContact/GetContactInformation/PutContactInformation/ListRegions/GetRegionOptStatus/EnableRegion/DisableRegion/GetAccountInformation/PutAccountName accept an optional AccountId (as AWS's wire contract requires) but operate on the single InMemoryBackend regardless of its value -- there is no per-member-account backend. GetPrimaryEmail/StartPrimaryEmailUpdate/AcceptPrimaryEmailUpdate validate AccountId is present (matching AWS's required-field contract) but likewise don't scope by it. Consistent with this service having always been a single-account backend; true multi-account modeling is a larger cross-service (Organizations-integration) project."
  - "EnableRegion/DisableRegion transition directly to the terminal state (ENABLED/DISABLED) instead of an async ENABLING/DISABLING window that a client would poll GetRegionOptStatus to observe. Real AWS takes minutes-to-hours; gopherstack completes immediately. This also means the documented ConflictException (\"enable while DISABLING\") can never actually fire here -- the window doesn't exist to race into. Not fixed: adding real async state to a Snapshot/Restore-backed backend risks non-deterministic tests and races under -race for a benefit (exercising a transient status) most callers/waiters don't depend on. Revisit if a bd issue specifically needs the transient states simulated."
  - "AccessDeniedException/TooManyRequestsException are wired into writeBackendError's classification table (so a backend error carrying that AWS exception name in its message would map to the correct HTTP status/code) but nothing in this backend's logic currently generates either -- there is no auth/permission model or throttle simulation in this service. Dead-but-correct code path; not a bug, just unexercised."
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - none (every routed op audited this pass)
leaks: {status: clean, note: "no goroutines/janitors in this service; single coarse sync.RWMutex guards all backend state"}
---

## Notes

**This was a from-scratch rewrite, not a bugfix pass.** The pre-existing implementation was
built against an entirely fictitious wire protocol: GET/PUT/DELETE verbs on RESTful-looking
paths (`/account`, `/account/contact`, `/account/alternateContact`, `/regions`,
`/regions/enable`, `/regions/{name}`, ...) with parameters like `AlternateContactType`,
`RegionName`, `regionOptStatusContains`, `maxResults`/`nextToken` passed as **query-string
parameters**. Real AWS Account Management (`account-2021-02-01`, protocol **restJson1**) has
no GET/PUT/DELETE operations at all: every one of its 14 operations is a **POST** to a fixed,
operation-named path (e.g. `POST /getContactInformation`, `POST /putContactInformation`,
`POST /listRegions`), and every parameter — including `AccountId`, `AlternateContactType`,
`RegionName`, `MaxResults`, `NextToken`, `RegionOptStatusContains` — travels in the **JSON
request body**, never the query string or URL path. A real `aws-sdk-go-v2/service/account`
client would 404/InvalidAction against literally every call the old handler served. This was
verified against the public AWS API reference
(https://docs.aws.amazon.com/accounts/latest/reference/) and botocore's
`account/2021-02-01/service-2.json` model, since `aws-sdk-go-v2/service/account` is not
vendored in this repo's go.sum.

**Two routed operations were entirely fictitious**: `DescribeAccount` and `CloseAccount` do
not exist anywhere in the real Account Management API
(https://docs.aws.amazon.com/accounts/latest/reference/API_Operations.html lists all 15 real
ops — 14 implemented here plus `GetGovCloudAccountInformation`, out of scope). `CloseAccount`
is an **AWS Organizations** operation (already correctly implemented in
`services/organizations`); Account Management has no account-closure capability at all. The
real op this service was missing entirely is `GetAccountInformation`
(`POST /getAccountInformation` → flat `{AccountId, AccountName, AccountCreatedDate,
AccountState}`, no wrapper) — added, backed by a new `accountCreatedDate time.Time` field
(iso8601 wire format per the smithy model's explicit `timestampFormat: iso8601` trait on
`AccountCreatedDate` — this is one of the few AWS JSON-protocol timestamps that does NOT use
epoch-seconds, so `pkgs/awstime.Epoch` does not apply here; verified against botocore's model
before choosing `time.RFC3339` over `awstime.Epoch`). `AccountState` always reports `ACTIVE`:
there is no operation in this service that can change it (confirmed above).

**Also found and fixed while rebuilding the wire layer**:
- The error envelope never set the `X-Amzn-Errortype` header, only the body's `__type` field.
  aws-sdk-go-v2's restJson1 deserializer resolves the exception type from the header first
  (falling back to `__type`); omitting it causes a generic/unknown client-side error instead
  of the modeled exception (matches the documented pattern in
  `services/apigatewaymanagementapi/handler.go`).
- `writeBackendError`'s fallback error code was `"InternalServerError"` — not an AWS Account
  Management exception name at all (the real one is `InternalServerException`). Also added
  the missing `ConflictException`/`AccessDeniedException`/`TooManyRequestsException` mappings
  (Get/PutAlternateContact... etc.'s real error set) so any future backend error carrying one
  of those names maps correctly instead of falling through to 500/InternalServerException.
- `PutContactInformation` accepted and stored *any* `ContactInformation`, including a
  completely empty one, with zero validation — a disguised no-validation write. Added the 6
  AWS-required-field checks (AddressLine1, City, CountryCode, FullName, PhoneNumber,
  PostalCode; the other 6 fields are genuinely optional per the API reference).
- `GetPrimaryEmail`/`StartPrimaryEmailUpdate`/`AcceptPrimaryEmailUpdate` did not require
  `AccountId` at all (it wasn't even a parameter in the old query-string-based handler). Real
  AWS marks `AccountId` **required** on these three specifically (unlike every other op, where
  it's optional and defaults to the caller's account) — added the validation. Not modeled:
  actually routing by AccountId to a different backend/member account (see gaps).
- The service had no `provider.go` and was not referenced anywhere in `cli.go`'s Provider
  list — confirmed via repo-wide grep that `gopherstack/services/account` is imported nowhere
  outside this package's own tests. The entire service was dead code, unreachable from the
  running gopherstack server, independent of any wire-shape bugs. Added
  `services/account/provider.go` (mirrors `services/workspaces/provider.go`) so wiring it in
  is a one-line cli.go change; cli.go itself is out of scope for this sweep (see gaps).

**Snapshot version bumped 1 → 2** (`persistence.go`): the `Closed` scalar was removed (no
operation ever sets a meaningful closed state — the old `CloseAccount` was itself fictitious)
and `AccountCreatedDate` was added. Per this file's existing discard-on-mismatch policy, a v1
snapshot is discarded cleanly (backend re-initializes, gets a fresh `accountCreatedDate`)
rather than partially decoded with a zero creation date.

**Test layout**: consolidated the ad-hoc `parity_b_test.go` / `parity_pass1_test.go` /
`parity_pass5_test.go` files (banned naming per this repo's CODE STYLE rules, and in any case
entirely wire-format-obsolete after the rewrite) into `handler_test.go` (one file per source
file: `handler.go` ↔ `handler_test.go`, `backend.go` ↔ `backend_test.go`,
`persistence.go` ↔ `persistence_test.go`). No test coverage was dropped in the consolidation —
every distinct scenario from the removed files was ported to the new POST/JSON-body wire
format.

**Trap for the next auditor**: `GetAccountInformation`'s response is a **flat** JSON object
(`{"AccountId":..., "AccountName":..., "AccountCreatedDate":..., "AccountState":...}`) — no
`{"Account": {...}}` wrapper, unlike almost every other Get* op in this service which wraps
its payload under a matching key (`GetContactInformation` → `{"ContactInformation": ...}`,
`GetAlternateContact` → `{"AlternateContact": ...}`). This looks like an inconsistency but
is correct per the AWS API reference's documented response syntax — don't "fix" it to match
the other ops' wrapper convention.
