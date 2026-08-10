---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: account
sdk_module: aws-sdk-go-v2/service/account@v1.35.4 (now a real go.mod/go.sum
  dependency, added this pass via `go get` -- prior passes only fetched it
  read-only into GOMODCACHE. Also added services/account/sdk_completeness_test.go,
  which was entirely missing before this pass -- this service had zero
  SDK-completeness coverage and no test/integration/account_test.go, unlike
  every other service in the repo.)
last_audit_commit: fca4a71a1
last_audit_date: 2026-08-10
overall: A            # sdk_completeness_test.go added and green (16/16 real ops
                       # covered), GetPrimaryEmailUpdateStatus/GetGovCloudAccountInformation
                       # implemented for real, and a genuine cross-service routing bug
                       # (services/inspector2's RouteMatcher swallowing /enableRegion and
                       # /disableRegion) was found and fixed by the new SDK-driven
                       # integration suite -- see test/integration/account_test.go and gaps.
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  GetContactInformation: {wire: ok, errors: ok, state: ok, persist: ok, note: "re-confirmed against real deserializers.go: modeled errors are AccessDenied/InternalServer/ResourceNotFound/TooManyRequests/Validation -- matches"}
  PutContactInformation: {wire: ok, errors: ok, state: ok, persist: ok, note: "re-confirmed field set (6 required + 6 optional) against types.ContactInformation; no ResourceNotFoundException in modeled errors, none produced -- matches"}
  GetAlternateContact: {wire: ok, errors: ok, state: ok, persist: ok, note: "re-confirmed against real deserializers.go"}
  PutAlternateContact: {wire: ok, errors: ok, state: ok, persist: ok, note: "re-confirmed all 5 required fields (Type/EmailAddress/Name/PhoneNumber/Title) against validators.go"}
  DeleteAlternateContact: {wire: ok, errors: ok, state: ok, persist: ok, note: "re-confirmed against real deserializers.go"}
  ListRegions: {wire: ok, errors: ok, state: ok, persist: ok, note: "MaxResults 1-50 range re-confirmed against the live API reference page (client SDK model has no generated range validator for it, but the docs page explicitly states 'Valid Range: Minimum value of 1. Maximum value of 50.') -- not invented"}
  GetRegionOptStatus: {wire: ok, errors: fixed, state: ok, persist: ok, note: "BUG FIXED: an unrecognized RegionName was misclassified as ResourceNotFoundException/404. Real GetRegionOptStatus's modeled error set (deserializers.go awsRestjson1_deserializeOpErrorGetRegionOptStatus) is only AccessDenied/InternalServer/TooManyRequests/Validation -- NO ResourceNotFoundException. Confirmed against the live API reference page's Errors section too. Now returns ValidationException/400 (errors.go's errRegionNotFound)."}
  EnableRegion: {wire: ok, errors: fixed, state: ok, persist: ok, note: "same bug/fix as GetRegionOptStatus: modeled errors are AccessDenied/Conflict/InternalServer/TooManyRequests/Validation, no ResourceNotFound. transitions straight to terminal ENABLED rather than through a transient ENABLING window -- see gaps."}
  DisableRegion: {wire: ok, errors: fixed, state: ok, persist: ok, note: "same bug/fix as GetRegionOptStatus/EnableRegion. Same immediate-terminal-state simplification as EnableRegion (DISABLING) -- see gaps."}
  GetPrimaryEmail: {wire: ok, errors: ok, state: ok, persist: ok, note: "AccountId required, re-confirmed against validators.go's validateOpGetPrimaryEmailInput"}
  StartPrimaryEmailUpdate: {wire: ok, errors: fixed, state: ok, persist: ok, note: "AccountId/PrimaryEmail required, re-confirmed against validators.go. BUG FIXED: now returns ConflictException when PrimaryEmail equals the account's current primaryEmail -- types.ConflictException's doc comment (types/errors.go) names 'change an account's root user email to an email address which is already in use' as a trigger, and deserializers.go's awsRestjson1_deserializeOpErrorStartPrimaryEmailUpdate models ConflictException for this op; the one email this single-account backend can ever know is 'already in use' is its own current primaryEmail. Cross-account email collision remains unmodeled -- see gaps."}
  AcceptPrimaryEmailUpdate: {wire: ok, errors: ok, state: ok, persist: ok, note: "AccountId/Otp/PrimaryEmail required, re-confirmed against validators.go and serializers.go's exact wire field names (AccountId/Otp/PrimaryEmail)"}
  GetAccountInformation: {wire: ok, errors: ok, state: ok, persist: ok, note: "re-confirmed real op (exists in aws-sdk-go-v2/service/account@v1.34.0, added after the aws-sdk-go v1 classic SDK's July-2024 feature freeze, which is why the v1 SDK vendored in this repo's module cache doesn't have it). Flat response confirmed via serializer (no wrapper). AccountCreatedDate confirmed ISO8601 (smithytime.ParseDateTime in deserializers.go), not epoch -- RFC3339 in account_info.go is correct."}
  PutAccountName: {wire: ok, errors: ok, state: ok, persist: ok, note: "re-confirmed real op, AccountId optional/AccountName required per validators.go"}
  GetPrimaryEmailUpdateStatus: {wire: ok, errors: ok, state: ok, persist: ok, note: "new. AccountId optional (no validateOp* func in validators.go, unlike Get/StartPrimaryEmail/AcceptPrimaryEmailUpdate). Response {Status, UpdatedAt}; UpdatedAt confirmed epoch-seconds (smithytime.ParseEpochSeconds in deserializers.go) -- NOT ISO8601 like AccountCreatedDate, uses awstime.Epoch. Modeled errors AccessDenied/InternalServer/ResourceNotFound/TooManyRequests/Validation. ResourceNotFoundException fires when no update was ever started."}
  GetGovCloudAccountInformation: {wire: ok, errors: ok, state: ok, persist: n/a, note: "new. StandardAccountId optional. Response {AccountState, GovCloudAccountId} using types.AwsAccountState (a distinct-but-identically-valued enum from GetAccountInformation's types.AccountState -- do not conflate them). This backend models a single, standalone, non-organization-member account (see the AccountId-targeting gap below), so no account it simulates ever has a linked GovCloud pair -- always returns ResourceNotFoundException, matching the API reference's own documented example 3 response for that exact case. Nothing to persist: state is a constant (no linking op exists anywhere in this service)."}
# Families audited as a group (when per-op is impractical):
families:
  routing: {status: ok, note: "full rewrite -- see bugs below"}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "AccountId targeting of org member accounts is not modeled: GetAlternateContact/PutAlternateContact/DeleteAlternateContact/GetContactInformation/PutContactInformation/ListRegions/GetRegionOptStatus/EnableRegion/DisableRegion/GetAccountInformation/PutAccountName/GetPrimaryEmailUpdateStatus/GetGovCloudAccountInformation accept an optional AccountId/StandardAccountId (as AWS's wire contract requires) but operate on the single InMemoryBackend regardless of its value -- there is no per-member-account backend. GetPrimaryEmail/StartPrimaryEmailUpdate/AcceptPrimaryEmailUpdate validate AccountId is present (matching AWS's required-field contract) but likewise don't scope by it. Consistent with this service having always been a single-account backend; true multi-account modeling is a larger cross-service (Organizations-integration) project. GetGovCloudAccountInformation's always-ResourceNotFoundException behavior is a direct consequence: services/organizations already models a GovCloudAccountID linked at CreateGovCloudAccount time, but wiring account<->organizations the way grafana<->networkmanager already cross-link would require touching cli.go, out of this pass's scope. Re-verified 2026-08-10: this is NOT a silent-drop bug -- handler.go decodes AccountId/StandardAccountId into every request struct (e.g. handleGetContactInformation's req.AccountID, handleGetGovCloudAccountInformation's req.StandardAccountID) and enforces it as required exactly where validators.go's validateOpGetPrimaryEmailInput/validateOpStartPrimaryEmailUpdateInput/validateOpAcceptPrimaryEmailUpdateInput require it (GetPrimaryEmail/StartPrimaryEmailUpdate/AcceptPrimaryEmailUpdate only) -- the field is read and validated, just not used for routing. Genuinely blocked on a multi-account backend model (per-member-account state), not a lookup gopherstack already has the data for; services/organizations models Email per member account but wiring account<->organizations would mean editing cli.go, excluded from this pass."
  - "EnableRegion/DisableRegion transition directly to the terminal state (ENABLED/DISABLED) instead of an async ENABLING/DISABLING window that a client would poll GetRegionOptStatus to observe. Real AWS takes minutes-to-hours; gopherstack completes immediately. This also means the documented ConflictException (\"enable while DISABLING\") can never actually fire here -- the window doesn't exist to race into. Not fixed: adding real async state to a Snapshot/Restore-backed backend risks non-deterministic tests and races under -race for a benefit (exercising a transient status) most callers/waiters don't depend on. Revisit if a bd issue specifically needs the transient states simulated. Re-verified 2026-08-10: this is a legitimate simplification, not a wire gap -- RegionOptStatusEnabling/RegionOptStatusDisabling ARE present in gopherstack's own enum (regions.go) and match the pinned SDK's types.RegionOptStatus.Values() (types/enums.go:110-117) exactly; nothing was missing from the source. No response field contradicts the simplification either: EnableRegionOutput/DisableRegionOutput (api_op_EnableRegion.go/api_op_DisableRegion.go) carry no fields at all besides ResultMetadata, and GetRegionOptStatusOutput (api_op_GetRegionOptStatus.go:62-75) is just {RegionName, RegionOptStatus} with no PendingChange/ETA field a caller could catch out of sync."
  - "AcceptPrimaryEmailUpdate's real AcceptPrimaryEmailUpdateOutput reports Status ACCEPTED immediately, then asynchronously transitions to COMPLETED once the change actually propagates. This simulator does not model that async completion tail -- ACCEPTED is the terminal status GetPrimaryEmailUpdateStatus reports here, matching the EnableRegion/DisableRegion async-window gap above. PrimaryEmailUpdateStatusCompleted/Failed are modeled (matching the real enum) but never produced."
  - "AccessDeniedException/TooManyRequestsException are wired into writeBackendError's classification table (so a backend error carrying that AWS exception name in its message would map to the correct HTTP status/code) but nothing in this backend's logic currently generates either -- there is no auth/permission model or throttle simulation in this service. Dead-but-correct code path; not a bug, just unexercised. ResourceUnavailableException (GetGovCloudAccountInformation's modeled error set) is the same: wired to 424, never produced. Re-verified 2026-08-10: this is legitimate and repo-consistent -- gopherstack has no per-request IAM policy evaluation or request-rate throttling in this service (nor is one specific to Account Management alone), so there is no real signal to trigger AccessDenied/TooManyRequests from. Not fabricating a synthetic rate-limit/permission model to force these to fire."
  - "ConflictException's documented 'email address already in use' trigger is now simulated for the case this single-account backend can actually detect: StartPrimaryEmailUpdate rejects a target PrimaryEmail equal to the account's own current primaryEmail (2026-08-10 fix). The genuinely cross-account collision (targeting a *different* AWS account's email) remains unmodeled -- consistent with the AccountId/single-backend gap above: there is no second account's email to compare against. AcceptPrimaryEmailUpdate also models ConflictException (deserializers.go's awsRestjson1_deserializeOpErrorAcceptPrimaryEmailUpdate) but has no independent trigger to simulate: any self-collision is already caught at StartPrimaryEmailUpdate time, before a pending update (and its OTP) would exist to Accept."
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - none (every routed op audited this pass)
leaks: {status: clean, note: "no goroutines/janitors in this service; single coarse sync.RWMutex guards all backend state; re-confirmed every lock path defer-releases and DeleteAlternateContact leaves no ghost map/table row"}
---

## Notes

**2026-08-07 pass (gopherstack-303i)**: this service was the only one of 161 with zero
SDK-completeness coverage -- no `sdk_completeness_test.go`, and
`aws-sdk-go-v2/service/account` wasn't even in `go.mod`. Added both. Running the completeness
check for the first time surfaced two real SDK operations this handler never routed:
`GetPrimaryEmailUpdateStatus` and `GetGovCloudAccountInformation` (both added to the SDK after
the v1.34.0 audit this PARITY.md previously cited). Both are now implemented for real -- see
the `ops:` table above for their wire shapes and the `gaps:` entries for what they don't
model and why.

**A genuine cross-service routing bug, found only by the new SDK-driven integration suite**:
`test/integration/account_test.go`'s `EnableRegion`/`DisableRegion` subtests failed against
the real running server with `501 NotImplementedException` from **Inspector2**, not Account.
`services/inspector2/handler.go`'s `RouteMatcher` matched requests by raw path-prefix,
including bare `"/enable"` and `"/disable"` entries with no SigV4-service-name gate --
`strings.HasPrefix("/enableRegion", "/enable")` is true, so Inspector2's handler claimed
Account's `/enableRegion`/`/disableRegion` requests before Account's own (SigV4-service-gated)
`RouteMatcher` ever saw them. Confirmed via Inspector2's own `{method, path}` dispatch table
(`handler.go`'s route map) that `/enable`/`/disable` are meant as exact fixed paths with no
children -- real Inspector2 has no `/enableFoo` sub-resource, so prefix matching was never
correct even before Account existed. Fixed by requiring exact-path equality for those two
entries specifically (`services/inspector2/handler.go`), leaving every genuine
directory-style prefix (`/filters/`, `/status/`, ...) untouched. `go test ./services/inspector2/...`
and `golangci-lint run ./services/inspector2/...` both still pass. This is exactly the
"RouteMatcher prefix collision" bug class already known in this repo (services swallowing
each other's paths) -- per that precedent, the fix is narrowing the matcher, never raising
`MatchPriority`. Account's own unit tests never caught this because they call
`h.Handler()(c)` directly, bypassing the shared router where the collision actually lives --
this is precisely why an SDK-driven integration suite is required for an honest A grade
(`.claude/memories/parity-principles.md` rule 3).

Also fixed while re-diffing the integration test against the real client: two of my own
integration-test assertions were wrong, not the service --
`GetAccountInformationOutput.AccountState` is `types.AccountState`, a distinct
(identically-valued) enum from `types.AwsAccountState` (used only by
`GetGovCloudAccountInformationOutput`); and `PutContactInformation`'s missing-required-field
case can't be observed as a wire `ValidationException` through the real SDK client at all --
`validators.go`'s `validateContactInformation` blocks it client-side before any request is
built. The server-side path this would have proven is already covered directly by
`handler_test.go`'s `TestHandler_PutContactInformation_RequiredFields`.

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
  is a one-line cli.go change. **Since resolved**: as of this pass's audit, `cli.go` imports
  `accountbackend "github.com/blackbirdworks/gopherstack/services/account"` and registers
  `&accountbackend.Provider{}` (wired by a separate concurrent pass on this branch, not this
  audit) -- the service is now reachable from the running server.

**2026-07-23 re-audit**: `aws-sdk-go-v2/service/account` still isn't in this repo's go.sum, but
`go mod download github.com/aws/aws-sdk-go-v2/service/account@latest` (v1.34.0) into
GOMODCACHE gave read-only access to the real generated client source (api_op_*.go,
serializers.go/deserializers.go, types/{types,enums,errors}.go) without touching this repo's
go.mod/go.sum -- a strictly stronger field-diff source than the previous pass's
API-reference-only audit. This confirmed the previous pass's shapes/fields/required-ness are
all correct, and found one real bug:

- **`GetRegionOptStatus`/`EnableRegion`/`DisableRegion` misclassified an unrecognized
  `RegionName` as `ResourceNotFoundException`/404.** Diffing
  `awsRestjson1_deserializeOpErrorGetRegionOptStatus` /
  `...ErrorEnableRegion` / `...ErrorDisableRegion` in the real SDK's `deserializers.go` shows
  their modeled error sets are `AccessDeniedException`, `InternalServerException`,
  `TooManyRequestsException`, `ValidationException`, and (Enable/DisableRegion only)
  `ConflictException` — **`ResourceNotFoundException` is not a possible error for any of the
  three.** Cross-checked against the live API reference pages for all three operations, whose
  Errors sections list the identical four/five exceptions with no ResourceNotFoundException.
  Fixed: `errRegionNotFound` (`errors.go`) now carries the `ValidationException:` prefix
  instead of `ResourceNotFoundException:`, so `writeBackendError` classifies it as
  ValidationException/400. Updated `handler_regions_test.go`'s
  `TestHandler_GetRegionOptStatus`'s `unknown_region` case and renamed
  `TestHandler_EnableDisableRegion_NotFound` →
  `TestHandler_EnableDisableRegion_UnknownRegionName` to assert 400/ValidationException instead
  of 404/ResourceNotFoundException. Added `regions_test.go` (previously regions.go had no
  backend-level test file of its own, only handler-level coverage) with direct
  `TestBackend_*` coverage of this error-code contract plus the previously-untested
  `errInvalidNextToken` path (`ListRegions` with an undecodable pagination cursor), also added
  at the handler level (`TestHandler_ListRegions_InvalidNextToken`).
- Every other operation's request/response field set, required-field list, and modeled error
  set was re-diffed against the real client source and found to already match (see per-op
  `note`s in the ops table above for what was specifically re-checked).

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

**2026-08-10 pass (gopherstack-sobi)**: re-audited the four items this service's FOLLOW-UP
issue recorded as needing further look. `sdk_module` pin (`account@v1.35.4`) re-checked
against `go.mod` — still current, not stale, so no prior claim in this file rested on a wrong
SDK version.

- **AccountId multi-account/member targeting**: re-confirmed NOT a silent-drop bug —
  `handler.go` decodes `AccountId`/`StandardAccountId` into every request struct and enforces
  it as required exactly where the pinned SDK's `validators.go` requires it
  (`GetPrimaryEmail`/`StartPrimaryEmailUpdate`/`AcceptPrimaryEmailUpdate` only). The field is
  read and validated, just never used to route to different backend state. Genuinely blocked
  on a multi-account backend model (per-member-account state), which is a different, larger
  problem than a lookup gopherstack already has data for — not fixed, left as documented gap.
- **EnableRegion/DisableRegion ENABLING/DISABLING async window**: re-confirmed legitimate, not
  a false claim. Both enum values are present in gopherstack's own `RegionOptStatus`
  (`regions.go`) and match the pinned SDK's `types.RegionOptStatus.Values()`
  (`types/enums.go:99-117`) exactly — no missing enum value. No response field contradicts the
  instant-completion simplification either: `EnableRegionOutput`/`DisableRegionOutput`
  (`api_op_EnableRegion.go`/`api_op_DisableRegion.go`) carry no fields besides
  `ResultMetadata`, and `GetRegionOptStatusOutput` (`api_op_GetRegionOptStatus.go:62-75`) is
  just `{RegionName, RegionOptStatus}` — no `PendingChange`/ETA field a caller could observe
  as inconsistent. Not fixed, per this file's existing rationale (async state risks
  nondeterministic tests under `-race` for a benefit most callers don't depend on).
- **ConflictException email-already-in-use (BUG FIXED)**: the backend genuinely holds enough
  state to detect one real case of this — `StartPrimaryEmailUpdate` now rejects a target
  `PrimaryEmail` equal to the account's own current `primaryEmail` with `ConflictException`
  (`errPrimaryEmailInUse` in `errors.go`), matching `types.ConflictException`'s doc comment
  (pinned SDK's `types/errors.go`) and `deserializers.go`'s
  `awsRestjson1_deserializeOpErrorStartPrimaryEmailUpdate`, which both confirm ConflictException
  is a modeled error for this op with exactly this trigger. Verified test-first: added
  `TestBackend_StartPrimaryEmailUpdate_ConflictsOnCurrentEmail` (`account_info_test.go`),
  confirmed it failed against the pre-fix backend (`update succeeded, want error`), then added
  the check and confirmed it passes; added a matching handler-level 409 test
  (`TestHandler_PrimaryEmail_StartConflictsOnCurrentEmail`,
  `handler_account_info_test.go`). The broader cross-account collision (targeting a different
  AWS account's email) remains unmodeled — no second account's email exists in this
  single-account backend to compare against.
- **AccessDenied/TooManyRequests never generated**: re-confirmed legitimate — this service has
  no per-request IAM policy evaluation or throttle simulation (nor does any comparable
  gopherstack service model one per-op), so there is no real signal to trigger either from.
  Left as documented dead-but-correct wiring; not fabricating a synthetic rate-limit model.

Adjacent sweep (enum values no client would recognize, required response fields never
serialized, config accepted-then-discarded) found nothing further: every enum in `models.go`
(`RegionOptStatus`, `ContactType`, `State`, `PrimaryEmailUpdateStatus`) matches the pinned
SDK's `types/enums.go` values exactly, and every response struct's fields
(`GetAccountInformationOutput`, `GetGovCloudAccountInformationOutput`, `ListRegionsOutput`,
`GetContactInformationOutput`, `GetAlternateContactOutput`, `StartPrimaryEmailUpdateOutput`,
`AcceptPrimaryEmailUpdateOutput`, `GetPrimaryEmailUpdateStatusOutput`,
`GetPrimaryEmailOutput`) were re-checked field-by-field against the pinned SDK's
`api_op_*.go` files with none missing.
