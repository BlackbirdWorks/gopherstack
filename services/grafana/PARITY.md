---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: grafana
sdk_module: aws-sdk-go-v2/service/grafana@v1.38.3   # real go.mod dependency now (go get run this pass)
last_audit_commit: 76edcd082d866f9264d5f994ee7414ea1b65da0e   # HEAD at implementation start; diff from here forward
last_audit_date: 2026-08-01
# Grade B: this is a from-scratch implementation, not a fix to pre-existing code, so "A =
# genuine fixes found" does not literally apply -- there was nothing to fix. B ("already-
# accurate, proven op-by-op") is the closer fit: every one of the 25 operations' wire shapes
# was read directly from serializers.go/deserializers.go (never assumed from the Go struct
# field names alone -- two real traps were only visible there: AssociateLicense's
# GrafanaToken is an HTTP header, not a body/query field, and ListVersions' workspaceId is
# the query param "workspace-id" with a hyphen, unlike every other operation's "workspaceId").
# Real SDK round-trip tests (services/grafana/sdk_roundtrip_helper_test.go, following
# services/databrew's pattern) caught two further wire bugs during this pass before they
# shipped: AssociateLicense and DisassociateLicense's "not in a valid state" cases were
# initially modeled as ConflictException, but those two operations' own
# deserializeOpErrorAssociateLicense/DisassociateLicense functions do not list
# ConflictException among the exception shapes they recognize -- a real caller's
# errors.As(err, &types.ConflictException{}) would silently never match. Fixed: AssociateLicense
# now reports ValidationException for that case, and DisassociateLicense treats
# "no license to remove" as an idempotent no-op rather than inventing a wire-incompatible
# error. See "Errors" section below for the full per-operation exception-type table this was
# built from.
overall: B
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
# All 25 ops are now routed, modeled with real backend state, and persisted via
# InMemoryBackend.Snapshot/Restore (services/grafana/persistence.go).
ops:
  CreateWorkspace: {wire: ok, errors: ok, state: ok, persist: ok, note: "POST /workspaces; CREATING -> ACTIVE after a 100ms simulated delay (workspaces.go)"}
  DescribeWorkspace: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /workspaces/{workspaceId}"}
  UpdateWorkspace: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT /workspaces/{workspaceId}; merges onto existing state, requires ACTIVE/DEGRADED, UPDATING -> ACTIVE"}
  DeleteWorkspace: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETE /workspaces/{workspaceId}; cascades apiKeys/serviceAccounts/tokens/permissions synchronously (workspace_update.go)"}
  ListWorkspaces: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /workspaces, paginated via pkgs/page"}
  DescribeWorkspaceAuthentication: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /workspaces/{workspaceId}/authentication"}
  UpdateWorkspaceAuthentication: {wire: ok, errors: ok, state: ok, persist: ok, note: "POST /workspaces/{workspaceId}/authentication; validates IdpMetadata url-xor-xml and SAML-requires-samlConfiguration"}
  DescribeWorkspaceConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /workspaces/{workspaceId}/configuration; opaque JSON blob stored/returned verbatim"}
  UpdateWorkspaceConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT /workspaces/{workspaceId}/configuration; grafanaVersion upgrade-only, validated against versions.go's static list"}
  AssociateLicense: {wire: ok, errors: ok, state: ok, persist: ok, note: "POST /workspaces/{workspaceId}/licenses/{licenseType}; GrafanaToken read from Grafana-Token HEADER, not body -- see handler_license.go"}
  DisassociateLicense: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETE /workspaces/{workspaceId}/licenses/{licenseType}; idempotent no-op when nothing to remove (no ConflictException on this op's wire)"}
  ListVersions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GET /versions; workspaceId query param is \"workspace-id\" (hyphenated), confirmed via serializers.go -- not \"workspaceId\""}
  ListPermissions: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /workspaces/{workspaceId}/permissions, paginated, filterable by groupId/userId/userType"}
  UpdatePermissions: {wire: ok, errors: ok, state: ok, persist: ok, note: "PATCH /workspaces/{workspaceId}/permissions; real partial-failure batch -- a malformed instruction (empty users) lands in Errors, valid instructions in the same batch still apply"}
  CreateWorkspaceApiKey: {wire: ok, errors: ok, state: ok, persist: ok, note: "POST /workspaces/{workspaceId}/apikeys; SecondsToLive validated 1..2592000 (30 days)"}
  DeleteWorkspaceApiKey: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETE /workspaces/{workspaceId}/apikeys/{keyName}"}
  CreateWorkspaceServiceAccount: {wire: ok, errors: ok, state: ok, persist: ok, note: "POST /workspaces/{workspaceId}/serviceaccounts; IsDisabled wire-typed as *string (\"true\"/\"false\"), not *bool -- preserved as-is, confirmed via types.go"}
  DeleteWorkspaceServiceAccount: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETE /workspaces/{workspaceId}/serviceaccounts/{serviceAccountId}; cascades its tokens"}
  ListWorkspaceServiceAccounts: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /workspaces/{workspaceId}/serviceaccounts, paginated"}
  CreateWorkspaceServiceAccountToken: {wire: ok, errors: ok, state: ok, persist: ok, note: "POST .../serviceaccounts/{id}/tokens; returns the plaintext key exactly once (ServiceAccountTokenSummaryWithKey), never re-exposed by List"}
  DeleteWorkspaceServiceAccountToken: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETE .../serviceaccounts/{id}/tokens/{tokenId}"}
  ListWorkspaceServiceAccountTokens: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET .../serviceaccounts/{id}/tokens, paginated; summary shape has no Key field"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "POST /tags/{resourceArn}; full percent-encoded ARN as one path segment, handled via rawPathSegments (s3tables-style RawPath + per-segment url.PathUnescape)"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETE /tags/{resourceArn}; TagKeys as repeated ?tagKeys= query param"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /tags/{resourceArn}"}
# Families audited as a group (when per-op is impractical):
families:
  route-matcher: {status: ok, note: "handler.go's routeRequest dispatch tree; RouteMatcher prefixes on /workspaces, /versions, /tags/; MatchPriority = PriorityPathVersioned"}
gaps:
  - "AccessDeniedException, ServiceQuotaExceededException, and ThrottlingException are real, wire-declared SDK error types (types/errors.go) this emulator has no trigger path for: no auth/IAM-policy model and no per-account quota tracking. Documented, not hidden -- see errors.go's apiError doc comment."
  - "WorkspaceStatus's *_FAILED variants and DEGRADED are wire-accurate constants (models.go) but nothing in this backend ever transitions a workspace into them -- every simulated async transition (CREATING/UPDATING/UPGRADING/VERSION_UPDATING) always resolves to ACTIVE, never a failure state. A future pass could wire chaos-injection (pkgs/chaos) to drive these."
  - "Cross-service validation (IAM role existence for WorkspaceRoleArn, VPC/subnet/security-group existence for VpcConfiguration, Organizations OU existence for WorkspaceOrganizationalUnits, SSO user/group existence for ListPermissions/UpdatePermissions) is NOT performed -- every such field is accepted as an opaque string, matching the real Grafana API's own wire contract (none of these are validated fields on the Go SDK types either), but a stricter emulator could cross-check services/iam, services/ec2, services/organizations, services/identitystore."
  - "ListVersions' static version list (8.4/9.4/10.4 in store.go's grafanaVersions) is a reasonable stand-in, not the real AWS-supported set, which is operational data that changes over time and isn't encoded in the Go SDK module at all."
leaks: {status: clean, note: "Handler.Reset()/Backend.Reset() close every workspace's tags.Tags before clearing; InMemoryBackend.Close() stops the worker.Group backing every scheduled CREATING/UPDATING/etc. transition timer"}
---

## Implementation summary

All 25 operations are implemented with real backend state (no stubs): a genuine workspace
lifecycle state machine (CREATING/UPDATING/UPGRADING/VERSION_UPDATING all resolve to ACTIVE
after a simulated 100ms delay via `pkgs/worker`, mirroring `services/eks`'s
`scheduleClusterActivation` pattern), AWS SSO / SAML authentication sub-state, API keys and
service-account tokens with real expiry (`pkgs/awstime.Epoch` for the wire's epoch-seconds
timestamp fields), licence association/disassociation, a many-to-many permissions grant
table with real batch partial-failure semantics, and full tag support wired into
`resourcegroupstaggingapi` (`cli.go`'s `wireTaggingGrafana`).

**File layout**: `models.go` (stored-state types) / `wire.go` + `wire_convert.go` (JSON wire
shapes and their conversion to/from stored state) / `store.go` + `store_setup.go` (the
`InMemoryBackend`, one coarse `lockmetrics.RWMutex` guarding every collection since nearly
every operation reads-or-mutates workspace-scoped state) / `workspaces.go` +
`workspace_update.go` (CRUD + lifecycle) / `authentication.go` / `configuration.go` /
`license.go` / `versions.go` / `permissions.go` / `api_keys.go` / `service_accounts.go` /
`service_account_tokens.go` / `tags.go` (backend logic) / `handler.go` + one `handler_*.go`
per operation family (HTTP routing/dispatch) / `persistence.go` / `errors.go` / `consts.go`
/ `provider.go`.

## ARN format — settled and how it was verified

The manifest's prior audit correctly flagged that the resource-path segment of the ARN could
not be confirmed from the local SDK checkout alone (`ResourceArn` on
`TagResource`/`UntagResource`/`ListTagsForResource` is a bare `*string` with no `@resource`
pattern trait reproduced in the generated Go). This pass resolved it by reading
**terraform-provider-aws's own ARN-construction code**
(`internal/service/grafana/workspace.go`, fetched via `WebFetch` against
`raw.githubusercontent.com` since the AWS Service Authorization Reference page itself
returned no fetchable body): the resource is built as

```go
return c.RegionalARN(ctx, "grafana", "/workspaces/"+id)
```

confirming the full ARN shape is `arn:{partition}:grafana:{region}:{account}:/workspaces/{id}`
-- note the leading `/` baked into the resource segment itself (not a separate `resource/id`
or `resource:id` convention), matching what the prior audit had cited from AWS's general
documentation but could not verify from the SDK. `store.go`'s `WorkspaceARN` implements this
via `arn.Build("grafana", region, accountID, "/workspaces/"+id)`. Since the resource segment
itself contains a literal `/`, the real SDK client percent-encodes it as `%2F` when building
the `/tags/{resourceArn}` request URI -- `services/grafana/tags_test.go`'s
`TestTagResourceRoundTrip_ARNContainsSlash` exercises exactly this through a real
`aws-sdk-go-v2` client to prove `rawPathSegments`' `RawPath` + per-segment
`url.PathUnescape` handling (copied from `services/s3tables`'s proven fix) survives it.

The ARN's *service* segment (`grafana`) was already settled by the prior audit via the
classic `aws-sdk-go`'s `ServiceName`/`SigningName` constants and needed no re-verification.

## Errors — per-operation exception-type table

Verified by reading every operation's own `awsRestjson1_deserializeOpError<Op>` switch in
`deserializers.go` (not assumed from the shared `types/errors.go` declarations, which list
all seven possible shapes without saying which operations actually use which — see the
prior audit's own caution about this). Full table (operations with an unusual set called
out): every operation accepts `AccessDeniedException`/`InternalServerException`/
`ThrottlingException` (never triggered by this emulator — see gaps); most also accept
`ResourceNotFoundException` and `ValidationException`. **`ConflictException` is NOT
recognized by**: `AssociateLicense`, `DisassociateLicense`, `DescribeWorkspace`,
`DescribeWorkspaceConfiguration`, `ListPermissions`, `ListTagsForResource`, `ListVersions`,
`ListWorkspaces`, `TagResource`, `UntagResource`, `UpdatePermissions` — this emulator never
raises one from those operations (confirmed by re-reading every error path in this pass).
`ServiceQuotaExceededException` is recognized only by the five `Create*` operations plus
`CreateWorkspace` itself (never triggered — no quota model). `DescribeWorkspaceConfiguration`
uniquely does **not** accept `ValidationException` at all (it takes no body, so there is
nothing to validate) — this emulator's implementation never attempts to return one there.

## Deliberately simplified (honest, not hidden)

1. **No fault/failure states.** Every async transition always resolves to ACTIVE; the
   `*_FAILED`/`DEGRADED` statuses are real wire constants with no trigger path.
2. **`ListVersions`' version list is static** (`8.4`/`9.4`/`10.4`), not AWS's actual current
   catalog (which is operational data, not SDK-encoded).
3. **No cross-service existence validation** for `WorkspaceRoleArn` (IAM), `VpcConfiguration`
   (EC2), `WorkspaceOrganizationalUnits` (Organizations), or SSO user/group IDs in
   `ListPermissions`/`UpdatePermissions` — every such field is accepted as an opaque string,
   which matches the real Grafana API's own wire contract (none of these are described as
   validated against another service in the SDK's doc comments either).
4. **Workspace IDs (`g-<10 hex chars>`) and numeric service-account/token IDs** are reasonable
   emulations, not confirmed byte-for-byte against a real workspace's ID format (the SDK
   types carry no pattern trait for `WorkspaceId`).
5. **`UpdatePermissions`'s partial-failure trigger** is deliberately narrow (an instruction
   with zero `Users` fails; everything else succeeds) — the real API's full validation
   surface for this operation isn't documented in the Go SDK types alone, so this is a
   defensible, honest subset rather than a guess at the complete rule set.

## Tests

`services/grafana/*_test.go`: `sdk_completeness_test.go` (empty exception list, all 25 ops),
plus real-`aws-sdk-go-v2`-client round-trip tests for every operation family (workspace
lifecycle + validation + cascade-on-delete, authentication incl. SAML union validation,
configuration + version upgrade-only enforcement, license associate/disassociate incl. the
two wire-shape fixes described above, permissions incl. partial-failure batch semantics, API
keys, service accounts + tokens incl. cascade, and the ARN-with-embedded-slash tag round
trip) — following `services/databrew`'s `newRoundTripClient` pattern (a real SDK client
against an `httptest.Server` wired through the same `pkgs/service` registry/router used in
production), which is what actually caught the `AssociateLicense`/`DisassociateLicense`
`ConflictException` wire bugs described above; ad-hoc JSON assertions against
`h.Handler()(c)` directly would not have.
