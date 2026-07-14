---
service: ssoadmin
sdk_module: aws-sdk-go-v2/service/ssoadmin@v1.38.0
last_audit_commit: ecdacf06
last_audit_date: 2026-07-12
overall: A            # 3 genuine wire/logic bugs fixed; rest of the surface was already accurate
ops:
  DescribeRegion: {wire: ok, errors: ok, state: ok, persist: ok, note: "was a disguised no-op stub (always returned {\"Region\":{}}); now reads real per-instance region state"}
  AddRegion: {wire: ok, errors: ok, state: ok, persist: ok, note: "now returns Status (was {}); ADDING status seeded"}
  RemoveRegion: {wire: ok, errors: ok, state: ok, persist: ok, note: "now returns Status: REMOVING (was {}); lazy-pruned on next List/Describe instead of deleted synchronously"}
  ListRegions: {wire: ok, errors: ok, state: ok, persist: ok, note: "wire keys were wrong (Region/RegionScopeType, neither exists on real RegionMetadata); fixed to RegionName/Status/IsPrimaryRegion/AddedDate"}
  PutApplicationAccessScope: {wire: ok, errors: ok, state: ok, persist: ok, note: "AuthorizedTargets was accepted on the wire and silently dropped; now stored"}
  GetApplicationAccessScope: {wire: ok, errors: ok, state: ok, persist: ok, note: "AuthorizedTargets was hardcoded to []; now returns the real stored targets"}
  ListApplicationAccessScopes: {wire: ok, errors: ok, state: ok, persist: ok, note: "returned []string of scope names; real ScopeDetails is []{Scope,AuthorizedTargets} objects -- a real SDK client failed to deserialize the old shape entirely"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "quota-exceeded returned __type TooManyTagsException, which does not exist in ssoadmin's error model; fixed to ServiceQuotaExceededException"}
  CreatePermissionSet: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribePermissionSet: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPermissionSets: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdatePermissionSet: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePermissionSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "correctly returns ConflictException while account assignments exist"}
  CreateAccountAssignment: {wire: ok, errors: ok, state: ok, persist: ok, note: "async status lazily IN_PROGRESS->SUCCEEDED on first Describe poll, not stuck forever"}
  DeleteAccountAssignment: {wire: ok, errors: ok, state: ok, persist: ok}
  ProvisionPermissionSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "same lazy-transition pattern as CreateAccountAssignment"}
  AttachManagedPolicyToPermissionSet: {wire: ok, errors: ok, state: ok, persist: ok}
  DetachManagedPolicyFromPermissionSet: {wire: ok, errors: ok, state: ok, persist: ok}
  PutInlinePolicyToPermissionSet: {wire: ok, errors: ok, state: ok, persist: ok}
  PutPermissionsBoundaryToPermissionSet: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  Instance: {status: ok, note: "CreateInstance/DescribeInstance/DeleteInstance/ListInstances/UpdateInstance; lazy CREATE_IN_PROGRESS->ACTIVE and DELETE_IN_PROGRESS cascade-prune on ListInstances verified correct"}
  PermissionSet+Policies: {status: ok, note: "managed/inline/customer-managed/permissions-boundary attach-detach all mutate real per-permission-set state; audited above"}
  AccountAssignment: {status: ok, note: "Create/Delete/List + CreationStatus/DeletionStatus polling all verified real (no disguised no-ops, no stuck-forever async status)"}
  Application+Assignment+AccessScope+AuthMethod+Grant+SessionConfig: {status: ok, note: "AccessScope family had the AuthorizedTargets bug (fixed); AuthMethod/Grant already stored full structured bodies via json.RawMessage correctly"}
  TrustedTokenIssuer: {status: ok, note: "CRUD + OIDC JWT config validated against real ssoadmin URL/enum constraints"}
  InstanceAccessControlAttributeConfiguration: {status: ok, note: "lazy CREATION_IN_PROGRESS->ENABLED transition on first Describe, matches other async patterns in this backend"}
  Region: {status: ok, note: "whole family was broken (see ops above): DescribeRegion was a stub, ListRegions/AddRegion/RemoveRegion used wire field names that don't exist on the real RegionMetadata type. Fixed with ADDING/ACTIVE/REMOVING lazy-transition + lazy-prune pattern consistent with the rest of the backend"}
  Tags: {status: ok, note: "TagResource/UntagResource/ListTagsForResource span instance/permission-set/application/trusted-token-issuer resources; quota exception type fixed"}
gaps:
  - "RegionMetadata.IsPrimaryRegion is always false -- this backend has no concept of an instance's 'home' region distinct from AddRegion-added regions, so DescribeRegion/ListRegions never report a primary region. Real AWS sets it for the region where the instance was originally enabled. Low-value to model fully (most callers key off RegionName+Status); left as a known simplification rather than a partial/incorrect implementation. (bd: none filed -- flagging for triage)"
deferred: []
leaks: {status: clean, note: "no new goroutines/janitors introduced; region pruning happens synchronously inside the existing coarse lock, same pattern as cascadeDeleteInstance"}
---

## Notes

- Protocol is awsjson1.1, single POST endpoint, `X-Amz-Target: SWBExternalService.<Op>` --
  verified byte-for-byte against `serializers.go`'s `SetHeader("X-Amz-Target")` calls in the
  real SDK. `RouteMatcher`/`ExtractOperation` prefix-match this correctly; no bug found here.
- `CreatedDate` and other timestamps use `float64(t.Unix())` (whole-second epoch), matching the
  real deserializer's `ParseEpochSeconds` (JSON number). This differs from `pkgs/awstime.Epoch`
  only in sub-second precision, which AWS SSO Admin resources don't need -- not a bug, just a
  style difference from the pkgs helper; not worth an intrusive refactor across ~40 call sites.
- **Region wire-shape bug (the big one this sweep):** `RegionMetadata` had invented field names
  (`Region`, `RegionScopeType`) that don't exist anywhere in the real
  `ssoadmin.types.RegionMetadata` shape (`AddedDate`, `IsPrimaryRegion`, `RegionName`, `Status`).
  A real AWS SDK client parsing `ListRegions`/`DescribeRegion` responses from the old code would
  either silently get zero-valued fields (List, same-shape-tolerant JSON decode) or in
  `DescribeRegion`'s case get literally nothing (handler was a fixed-stub returning
  `{"Region":{}}` regardless of request, ignoring `InstanceArn`/`RegionName` entirely and never
  touching the backend). Existing unit tests asserted on the wrong field names
  (`region["Region"]`, `region["RegionScopeType"]`) -- classic "unit tests are not parity proof"
  trap flagged in `.claude/memories/parity-principles.md`. Fixed end-to-end: backend struct,
  `AddRegion`/`RemoveRegion` now return `Status` (matching `AddRegionOutput.Status` /
  `RemoveRegionOutput.Status`, previously silently dropped), `ListRegions`/`DescribeRegion` use
  real field names, and `RemoveRegion` now leaves a lazily-pruned `REMOVING` entry instead of
  deleting synchronously (mirrors the existing `DeleteInstance` -> `DELETE_IN_PROGRESS` ->
  cascade-prune-on-list pattern already used elsewhere in this backend).
- **ApplicationAccessScope data-loss bug:** `PutApplicationAccessScope` parsed `Scope` but not
  `AuthorizedTargets` from the request body, so the field was silently discarded on every call.
  `GetApplicationAccessScope` then returned a hardcoded `AuthorizedTargets: []` regardless of
  what a real caller had set. Worse, `ListApplicationAccessScopes` returned `Scopes` as a bare
  `[]string` of scope names; the real op returns `[]ScopeDetails` (`{Scope, AuthorizedTargets}`
  objects) and the real deserializer (`awsAwsjson11_deserializeDocumentScopeDetails`) does a hard
  type-assert to `map[string]interface{}` per element -- a real SDK client calling
  `ListApplicationAccessScopes` against the old code would get a deserialization **error**, not
  just wrong data. Fixed: `applicationScopes` is now `map[string]map[string][]string]` (appArn ->
  scope -> targets), `ListApplicationAccessScopes` returns `[]ScopeDetails`, and nil target lists
  are normalized to `[]` (not JSON `null`) since `AuthorizedTargets` is not a required member on
  `GetApplicationAccessScopeOutput`.
- **Tag-quota exception type bug:** exceeding the 50-tags-per-resource limit on `TagResource`
  returned `__type: TooManyTagsException`. That exception does not exist anywhere in
  `ssoadmin/types/errors.go` -- the real ssoadmin error model is exactly `AccessDeniedException`,
  `ConflictException`, `InternalServerException`, `ResourceNotFoundException`,
  `ServiceQuotaExceededException`, `ThrottlingException`, `ValidationException`. A real client
  using typed error handling (`errors.As(&types.ServiceQuotaExceededException{})`) would never
  match the emulator's response. Fixed to `ServiceQuotaExceededException` (HTTP 400, matching the
  convention already used for this exception in `services/apprunner`).
- Persistence: `RegionMetadata` and `applicationScopes` both changed JSON shape this sweep, so
  `ssoadminSnapshotVersion` was bumped 1 -> 2. Old snapshots are cleanly discarded (`ResetAll` +
  re-seed) via the existing version-mismatch path in `Restore`, not partially misinterpreted.
- Two interface parameter-name mismatches were cleaned up in passing (`CreateAccountAssignment`/
  `DeleteAccountAssignment` in `interfaces.go` had `principalType, principalID` reversed relative
  to the actual `backend.go` implementation and every call site in `handler.go`). Types matched
  so this was not a compile-time or runtime bug -- Go doesn't check interface parameter names --
  but it was actively misleading documentation for the next person implementing
  `StorageBackend`, so it's fixed alongside the real bugs above.
- `DescribeRegion` was not in the SDK-completeness gap list nor `notImplemented` in
  `sdk_completeness_test.go` -- it was already claimed as "supported" in
  `GetSupportedOperations()` and dispatched to a handler, just one that never touched real state.
  This is exactly the "grep-based stub hunting has false positives" trap from
  `parity-principles.md` #4 in reverse: it read as a real op (dispatch table entry, handler func,
  200 response) but was a disguised stub. Caught by cross-referencing the real
  `DescribeRegionOutput` shape against what the handler actually returned, not by grepping for
  "stub"/"TODO" text.
