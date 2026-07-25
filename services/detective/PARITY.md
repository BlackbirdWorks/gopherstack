---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: detective
sdk_module: aws-sdk-go-v2/service/detective@v1.39.1   # version audited against
last_audit_commit: 40f059288a40c1d9b7956624bb288861e2e0651d
last_audit_date: 2026-07-23
overall: A            # A = genuine fixes found; B = already-accurate, proven op-by-op
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateGraph: {wire: ok, errors: ok, state: ok, persist: ok, note: idempotent-per-account matches AWS docs}
  DeleteGraph: {wire: ok, errors: ok, state: ok, persist: ok, note: "cleans investigations/datasources/orgConfigs, not just members/tags"}
  ListGraphs: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateMembers: {wire: ok, errors: ok, state: ok, persist: ok, note: "already-invited accounts go to UnprocessedAccounts per AWS docs; MemberDetail now includes InvitationType and DatasourcePackageIngestStates (see notes)"}
  DeleteMembers: {wire: ok, errors: ok, state: ok, persist: ok}
  GetMembers: {wire: ok, errors: ok, state: ok, persist: ok, note: "MemberDetail now includes InvitationType and DatasourcePackageIngestStates"}
  ListMembers: {wire: ok, errors: ok, state: ok, persist: ok, note: "MemberDetail now includes InvitationType and DatasourcePackageIngestStates; NextToken already opaque base64"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  AcceptInvitation: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT /invitation matches SDK method"}
  RejectInvitation: {wire: ok, errors: ok, state: ok, persist: ok}
  ListInvitations: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - NextToken is now an opaque base64 offset (encodePageToken/decodePageToken) instead of the raw next GraphArn; MemberDetail now includes InvitationType and DatasourcePackageIngestStates"}
  DisassociateMembership: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchGetGraphMemberDatasources: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchGetMembershipDatasources: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDatasourcePackages: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - NextToken is now an opaque base64 offset instead of the raw next package-name key"}
  UpdateDatasourcePackages: {wire: ok, errors: ok, state: ok, persist: ok, note: "always transitions to STARTED, no real ingest pipeline to fail - acceptable simplification"}
  StartMonitoringMember: {wire: ok, errors: ok, state: partial, persist: ok, note: "precondition status ACCEPTED_BUT_DISABLED is never reached elsewhere in the backend (AcceptInvitation goes straight to ENABLED), so this op can never succeed on a member reached only through normal API flow; see gaps"}
  GetInvestigation: {wire: ok, errors: ok, state: ok, persist: ok}
  ListIndicators: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - real aws-sdk-go-v2 types.Indicator has IndicatorType + IndicatorDetail (a union of 8 type-specific sub-structs: FlaggedIpAddressDetail, ImpossibleTravelDetail, NewAsoDetail, NewGeolocationDetail, NewUserAgentDetail, RelatedFindingDetail, RelatedFindingGroupDetail, TTPsObservedDetail) and has NO Title member at all. This emulator previously returned a gopherstack-invented free-text Title field instead of IndicatorDetail -- deleted and replaced with the real union shape (interfaces.go IndicatorDetail + 8 sub-detail structs, handler_investigations.go indicatorDetailToJSON). Also added the two previously-missing IndicatorType values (NEW_ASO, NEW_USER_AGENT) to builtInIndicators so all 8 real enum values are producible and filterable."}
  ListInvestigations: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - NextToken is now an opaque base64 offset instead of the raw next InvestigationId"}
  StartInvestigation: {wire: ok, errors: ok, state: ok, persist: ok, note: "EntityType is not a real input field (StartInvestigationInput has no EntityType member); derived server-side from EntityArn's role/ or user/ resource segment. ScopeStartTime/ScopeEndTime are required per SDK and validated as such."}
  UpdateInvestigationState: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeOrganizationConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DisableOrganizationAdminAccount: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - AWS docs: 'Deletes the organization behavior graph.' Now deletes the graph(s) referenced by the current org admin(s) via the deleteGraphLocked helper shared with DeleteGraph (cascading members/investigations/tags/datasources/orgConfigs cleanup), then clears orgAdmins. Within this emulator's one-graph-per-account model, EnableOrganizationAdminAccount always designates the account's sole graph as the org graph, so deleting it on Disable is the faithful behavior."}
  EnableOrganizationAdminAccount: {wire: ok, errors: ok, state: ok, persist: ok, note: "auto-creates a behavior graph when the account has none, per AWS docs. Fixed this pass - now enforces AWS's singular Detective-administrator-account-per-organization/Region model (ListOrganizationAdminAccounts and the Administrator SDK type both describe it in the singular): a second Enable call replaces the existing orgAdmins entry instead of appending a duplicate, which previously let repeated Enable calls accumulate multiple conflicting Administrators in ListOrganizationAdminAccounts output."}
  ListOrganizationAdminAccounts: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - NextToken is now an opaque base64 offset instead of the raw next AccountId (now effectively unreachable in practice since EnableOrganizationAdminAccount enforces at most one admin, but kept consistent with the other list ops)"}
  UpdateOrganizationConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
# Families audited as a group (when per-op is impractical):
families:
  route_matcher: {status: ok, note: "every REST path + HTTP method verified byte-for-byte against aws-sdk-go-v2 serializers.go opPath/request.Method for all 29 ops; matches exactly. handler_test.go exercises h.RouteMatcher()(c) and h.ExtractOperation(c) directly (not just h.Handler()) to prove the matcher itself, not just the dispatch switch, since unit tests calling h.Handler()(c) bypass RouteMatcher."}
  wire_timestamps: {status: ok, note: "smithytime.ParseDateTime/FormatDateTime confirms restjson1 Detective uses ISO8601 datetime strings (NOT epoch numbers) for CreatedTime/InvitedTime/UpdatedTime/DelegationTime/ScopeStartTime/ScopeEndTime; handler.go's \"2006-01-02T15:04:05.000Z\" format is a valid (always-3-decimal) RFC3339 the real client parses fine, vs. SDK's \"2006-01-02T15:04:05.999Z\" (trailing-zero-trimmed) output format - both are valid ISO8601, no bug"}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "StartMonitoringMember's precondition (member status ACCEPTED_BUT_DISABLED) is unreachable through normal API flow: AcceptInvitation transitions INVITED straight to ENABLED, mirroring the AWS happy path, but real Detective can also land a member in ACCEPTED_BUT_DISABLED (data-volume-too-high / volume-unknown edge cases per MemberDisabledReason) which this emulator does not model. Not fixed this pass: real AWS determines this state via internal GuardDuty volume telemetry with no documented client-controllable trigger, so modeling a way to reach it would mean inventing a control surface that does not exist in the real API rather than emulating one -- a larger, speculative feature, not a wire/state bug fix."
  - "MemberDetail still omits DisabledReason, VolumeUsageInBytes (deprecated), VolumeUsageUpdatedTime (deprecated), PercentOfGraphUtilization (deprecated), PercentOfGraphUtilizationUpdatedTime (deprecated), and VolumeUsageByDatasourcePackage. InvitationType and DatasourcePackageIngestStates were fixed this pass (see CreateMembers/GetMembers/ListMembers/ListInvitations notes). The remaining fields are volume/analytics telemetry this emulator does not model (no real data-ingest pipeline), and DisabledReason has no valid state to populate since ACCEPTED_BUT_DISABLED is unreachable (see the StartMonitoringMember gap above) -- all are optional fields real clients already treat as absent-safe, so omitting them is wire-legal, just incomplete. Low priority."
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - "Detective Organizations edge cases beyond the base Enable/Disable/List/Describe/Update surface (delegated-admin-account transfer, cross-region graph semantics) — out of scope for a single-region single-account emulator."
  - "UpdateOrganizationConfiguration's AutoEnable flag has no side effect: real AWS auto-enables Detective for new Organizations member accounts as they join the org. This emulator has no Organizations-service integration to source account-join events from, so AutoEnable is stored and returned correctly (DescribeOrganizationConfiguration) but never drives member auto-creation. Out of scope for a single-account emulator with no cross-service org simulation."
leaks: {status: clean, note: "DeleteGraph purges investigations/datasources/orgConfigs for the deleted graph ARN (not just members/tags). DisableOrganizationAdminAccount now reuses the same deleteGraphLocked cascade (see EnableOrganizationAdminAccount/DisableOrganizationAdminAccount notes above), so org-graph deletion via Disable is leak-free too. Verified via TestDeleteGraph_CleansUpDependentState and TestDisableOrganizationAdminAccount_DeletesGraph, both asserting the deleted ARN is absent from a post-delete Snapshot()/ListGraphs()."}
---

## Notes

Protocol: restjson1. All 29 CreateGraph..UpdateOrganizationConfiguration ops route
through a single `handleREST` switch keyed by `classifyPath(method, path)`; every
path/method pair was diffed byte-for-byte against
`aws-sdk-go-v2/service/detective@v1.39.1/serializers.go`'s
`awsRestjson1_serializeOpHttpBindings*Input` functions (opPath + request.Method) and
matches exactly, including the PUT-vs-POST split on `/invitation`
(AcceptInvitation=PUT, RejectInvitation lives at a different path
`/invitation/removal`=POST) and the GET/POST/DELETE split on `/tags/{ResourceArn}`.

Real bugs fixed in prior passes (see `ops:` above for detail): CreateMembers
UnprocessedAccounts reporting on re-invite; StartInvestigation trusting a
client-supplied (non-existent) `EntityType` field instead of deriving it from
`EntityArn`, plus missing `ScopeStartTime`/`ScopeEndTime` required-field
validation; EnableOrganizationAdminAccount never auto-creating a graph;
DeleteGraph leaking datasource/orgConfig/investigation state.

Real bugs fixed this pass (see `ops:` above for detail):

1. **`ListIndicators`/`Indicator` had a fabricated `Title` field with no
   basis in the real SDK.** `aws-sdk-go-v2/service/detective/types.Indicator`
   has exactly two members: `IndicatorType` and `IndicatorDetail` (a
   union-like struct with one of 8 type-specific sub-details populated:
   `FlaggedIpAddressDetail`, `ImpossibleTravelDetail`, `NewAsoDetail`,
   `NewGeolocationDetail`, `NewUserAgentDetail`, `RelatedFindingDetail`,
   `RelatedFindingGroupDetail`, `TTPsObservedDetail`) — there is no `Title`
   member anywhere in the shape. A real client parsing this emulator's
   `Title` string would silently drop it (deserializer ignores unknown
   keys) and get an empty `IndicatorDetail` on every indicator. Fixed by
   deleting the invented `Title` field and adding the real `IndicatorDetail`
   union (`interfaces.go`), wiring `builtInIndicators` (`investigations.go`)
   to populate the correct sub-detail per `IndicatorType`, and adding a
   `indicatorDetailToJSON` encoder (`handler_investigations.go`) with the
   exact wire field names byte-diffed against `deserializers.go`. Also added
   the two previously-missing `IndicatorType` enum values (`NEW_ASO`,
   `NEW_USER_AGENT`) so all 8 real values are producible/filterable.
2. **`MemberDetail` was missing `InvitationType` and
   `DatasourcePackageIngestStates`**, two real (non-deprecated)
   `MemberDetail` wire members. `InvitationType` is always `"INVITATION"` in
   this emulator (every member reaches a graph through the CreateMembers
   invite flow — there is no Organizations-auto-enable path to produce
   `"ORGANIZATION"`). `DatasourcePackageIngestStates` mirrors the graph-wide
   datasource ingest map, matching the simplification already used by
   `BatchGetGraphMemberDatasources`. Fixed in `models.go`
   (`toMemberDetail`), `interfaces.go`, `members.go`, and
   `handler_members.go`.
3. **`EnableOrganizationAdminAccount` accumulated duplicate `orgAdmins`
   entries** on repeated calls instead of replacing the existing
   designation, contradicting AWS's singular
   Detective-administrator-account-per-organization/Region model
   (`ListOrganizationAdminAccounts`/`Administrator` are both documented in
   the singular). Fixed in `administrator.go` to replace in place.
4. **`DisableOrganizationAdminAccount` did not delete the organization
   behavior graph**, contradicting AWS docs: "Removes the Detective
   administrator account in the current Region. Deletes the organization
   behavior graph." Fixed by extracting a `deleteGraphLocked` cascade helper
   (shared with `DeleteGraph`) in `graphs.go` and calling it for each
   admin's `GraphArn` before clearing `orgAdmins` in `administrator.go`.
5. **List pagination tokens were not uniformly opaque**: `ListInvitations`,
   `ListInvestigations`, `ListOrganizationAdminAccounts`, and
   `ListDatasourcePackages` returned the raw next item's identifier
   (GraphArn/InvestigationId/AccountId/package name) as `NextToken` instead
   of the opaque `base64(offset)` token every other Detective list op
   (`ListGraphs`/`ListMembers`/`ListIndicators`) already used. Wire-legal
   either way (AWS never guarantees token structure), but leaked internal
   identifiers to callers. Normalized all four to `encodePageToken`/
   `decodePageToken`.

"Looks-wrong-but-correct" traps for the next auditor:

- `AcceptInvitation` (and `AcceptInvitation`-adjacent flows) go straight from
  `INVITED` to `ENABLED`, never landing in `ACCEPTED_BUT_DISABLED`. This matches
  the AWS happy path (admission control passing); do not "fix" this into a
  detour through `ACCEPTED_BUT_DISABLED` without a concrete reason to model
  data-volume admission control.
- Timestamp format `"2006-01-02T15:04:05.000Z"` (always 3 decimals) vs. the SDK's
  own output format `"2006-01-02T15:04:05.999Z"` (trailing zeros trimmed) are both
  valid ISO8601/RFC3339 the real client parses identically — not a wire bug.
- `ErrGraphNotFound`/`ErrMemberNotFound`'s `Error()` text is literally the
  exception type string (`"ResourceNotFoundException"`), so the JSON response
  body's `message` field duplicates `__type`. Inelegant but not a wire-shape
  violation — AWS SDKs do not assert exact message text.
- `ListOrganizationAdminAccounts` pagination (`decodePageToken`/`encodePageToken`)
  is now effectively unreachable through the public API: since
  `EnableOrganizationAdminAccount` enforces at most one `orgAdmins` entry,
  `admins` never exceeds length 1, so `NextToken` can never be produced. Kept
  the opaque-token code path anyway for internal consistency with the other
  three list ops it was fixed alongside (`ListInvitations`,
  `ListInvestigations`, `ListDatasourcePackages`) — do not read the lack of a
  reachable pagination test for this one op as an oversight.
- `ErrAlreadyHasGraph` (`errors.go`) is dead/unused: `CreateGraph` is
  intentionally idempotent per AWS docs ("If the same account calls
  CreateGraph with the same administrator account, it always returns the
  same behavior graph ARN"), so nothing ever returns this error. Left as-is
  — it maps to a real `ConflictException` (not an invented error code), it's
  exported API surface, and removing it is out of scope for this pass.
