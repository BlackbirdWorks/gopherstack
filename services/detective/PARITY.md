---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: detective
sdk_module: aws-sdk-go-v2/service/detective@v1.39.1   # version audited against
last_audit_commit: 40f059288a40c1d9b7956624bb288861e2e0651d
last_audit_date: 2026-07-13
overall: A            # A = genuine fixes found; B = already-accurate, proven op-by-op
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateGraph: {wire: ok, errors: ok, state: ok, persist: ok, note: idempotent-per-account matches AWS docs}
  DeleteGraph: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - now cleans investigations/datasources/orgConfigs, not just members/tags"}
  ListGraphs: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateMembers: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - already-invited accounts now go to UnprocessedAccounts per AWS docs, not silently re-returned as processed"}
  DeleteMembers: {wire: ok, errors: ok, state: ok, persist: ok}
  GetMembers: {wire: ok, errors: ok, state: ok, persist: ok}
  ListMembers: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  AcceptInvitation: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT /invitation matches SDK method"}
  RejectInvitation: {wire: ok, errors: ok, state: ok, persist: ok}
  ListInvitations: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateMembership: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchGetGraphMemberDatasources: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchGetMembershipDatasources: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDatasourcePackages: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDatasourcePackages: {wire: ok, errors: ok, state: ok, persist: ok, note: "always transitions to STARTED, no real ingest pipeline to fail - acceptable simplification"}
  StartMonitoringMember: {wire: ok, errors: ok, state: partial, persist: ok, note: "precondition status ACCEPTED_BUT_DISABLED is never reached elsewhere in the backend (AcceptInvitation goes straight to ENABLED), so this op can never succeed on a member reached only through normal API flow; see gaps"}
  GetInvestigation: {wire: ok, errors: ok, state: ok, persist: ok}
  ListIndicators: {wire: ok, errors: ok, state: ok, persist: ok}
  ListInvestigations: {wire: ok, errors: ok, state: ok, persist: ok}
  StartInvestigation: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - EntityType is not a real input field (StartInvestigationInput has no EntityType member); now derived server-side from EntityArn's role/ or user/ resource segment. ScopeStartTime/ScopeEndTime are required per SDK and are now validated as such."}
  UpdateInvestigationState: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeOrganizationConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DisableOrganizationAdminAccount: {wire: ok, errors: ok, state: partial, persist: ok, note: "AWS docs: 'Deletes the organization behavior graph.' Current impl only clears orgAdmins, does not delete the graph - see gaps"}
  EnableOrganizationAdminAccount: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed - now auto-creates a behavior graph when the account has none, per AWS docs"}
  ListOrganizationAdminAccounts: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateOrganizationConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
# Families audited as a group (when per-op is impractical):
families:
  route_matcher: {status: ok, note: "every REST path + HTTP method verified byte-for-byte against aws-sdk-go-v2 serializers.go opPath/request.Method for all 29 ops; matches exactly. New handler_test.go exercises h.RouteMatcher()(c) and h.ExtractOperation(c) directly (not just h.Handler()) to prove the matcher itself, not just the dispatch switch, since unit tests calling h.Handler()(c) bypass RouteMatcher."}
  wire_timestamps: {status: ok, note: "smithytime.ParseDateTime/FormatDateTime confirms restjson1 Detective uses ISO8601 datetime strings (NOT epoch numbers) for CreatedTime/InvitedTime/UpdatedTime/DelegationTime/ScopeStartTime/ScopeEndTime; handler.go's \"2006-01-02T15:04:05.000Z\" format is a valid (always-3-decimal) RFC3339 the real client parses fine, vs. SDK's \"2006-01-02T15:04:05.999Z\" (trailing-zero-trimmed) output format - both are valid ISO8601, no bug"}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "StartMonitoringMember's precondition (member status ACCEPTED_BUT_DISABLED) is unreachable through normal API flow: AcceptInvitation transitions INVITED straight to ENABLED, mirroring the AWS happy path, but real Detective can also land a member in ACCEPTED_BUT_DISABLED (data-volume-too-high / volume-unknown edge cases per MemberDisabledReason) which this emulator does not model. Deferred: modeling those admission-control edge cases is a larger feature, not a wire/state bug."
  - "DisableOrganizationAdminAccount does not delete the organization behavior graph (AWS docs: 'Deletes the organization behavior graph.'). Not fixed this pass: this emulator's one-graph-per-account model does not distinguish an org behavior graph from a personal one, so forcibly deleting b.graphs on Disable risks destroying state a test/flow expects to persist independently, without a clear real-world precedent to validate against in this simplified model."
  - "MemberDetail response omits several AWS-optional/deprecated fields never populated: DisabledReason, VolumeUsageInBytes (deprecated), VolumeUsageUpdatedTime (deprecated), PercentOfGraphUtilization (deprecated), PercentOfGraphUtilizationUpdatedTime (deprecated), InvitationType, VolumeUsageByDatasourcePackage, DatasourcePackageIngestStates. All are optional/analytics fields real clients treat as absent-safe; not stubs since the op itself is real, just an unpopulated optional field. Low priority."
  - "List pagination tokens are not uniformly opaque: ListGraphs/ListMembers/ListIndicators use base64(offset) via encodePageToken, but ListInvitations/ListInvestigations/ListOrganizationAdminAccounts/ListDatasourcePackages use the raw next item's ID/ARN as the token. Both are wire-legal (AWS never guarantees token structure to callers) but the latter leaks internal identifiers. Stylistic/hygiene, not a wire-shape bug."
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - "Detective Organizations edge cases beyond the base Enable/Disable/List/Describe/Update surface (delegated-admin-account transfer, cross-region graph semantics) — out of scope for a single-region single-account emulator."
leaks: {status: clean, note: "DeleteGraph fixed to also purge investigations/datasources/orgConfigs for the deleted graph ARN (previously only cleaned members/tags), closing an unbounded-growth leak across repeated CreateGraph/DeleteGraph cycles. Verified via TestDeleteGraph_CleansUpDependentState asserting the deleted ARN is absent from a post-delete Snapshot()."}
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

Real bugs fixed this pass (see `ops:` above for detail):

1. **CreateMembers silently "succeeded" on re-invite of an existing member**
   instead of reporting it via `UnprocessedAccounts`, contradicting the documented
   contract: "The accounts that CreateMembers was unable to process. This list
   includes accounts that were already invited..." (`backend.go` `CreateMembers`).
2. **StartInvestigation trusted a client-supplied `EntityType`** field that does not
   exist on the real `StartInvestigationInput` wire shape at all — Detective derives
   entity type server-side from whether the `EntityArn` resource segment is
   `role/...` or `user/...`. Real SDK clients never send `EntityType`, so every
   investigation created via a real client had `EntityType: ""` in `GetInvestigation`/
   `ListInvestigations` output. Fixed by adding `deriveEntityType(entityARN)` and
   removing the input-trust path (`backend.go`, `interfaces.go`, `handler.go`).
   Also added the missing required-field validation for `ScopeStartTime`/
   `ScopeEndTime` (both "This member is required" on the real input shape); previously
   an absent value silently parsed as `time.Time{}` instead of `ValidationException`.
3. **EnableOrganizationAdminAccount never created a behavior graph** when the
   designated account had none, leaving `GraphArn: ""` on the `OrgAdmin` record
   forever, contradicting AWS docs: "If the account does not have Detective enabled,
   then enables Detective for that account and creates a new behavior graph."
   Fixed via a shared `createGraphLocked` helper used by both `CreateGraph` and
   `EnableOrganizationAdminAccount`.
4. **DeleteGraph leaked datasource/orgConfig/investigation state** keyed by the
   deleted graph's ARN (only members and tags were cleaned up). Not externally
   observable as wrong behavior (a fresh `CreateGraph` always mints a new ARN,
   and the deleted ARN correctly 404s), but an unbounded per-cycle memory leak.
   Fixed by extending the cleanup list.

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
