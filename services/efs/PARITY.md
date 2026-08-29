---
service: efs
sdk_module: aws-sdk-go-v2/service/efs@v1.44.4   # version audited against
last_audit_commit: 2516ed984b0172a43275ab37c70f0cac8f6bc807
last_audit_date: 2026-08-29
overall: A            # gopherstack-6flj follow-up (2026-08-29): write-only-state sweep. 2 real bugs
                      # found and fixed -- CreateFileSystemInput.Backup was silently dropped (a
                      # real SDK client's Backup:true never enabled DescribeBackupPolicy), and
                      # Destination.StatusMessage was never modeled at all (dormant in this
                      # backend, which never produces a non-ENABLED replication status, but wired
                      # for wire-shape completeness). 2026-08-20 pass's 3 fabricated-member
                      # removals stand, re-verified.
                      # 2026-08-29 wrapper-key sweep (query/path/header key hunt, cross-service
                      # with apigateway/transfer/appconfig): every REQUEST-direction Query/URI/
                      # Header binding in efs@v1.44.4 serializers.go checked op-by-op against this
                      # handler's actual parameter reads. Found efs CLEAN of the wrong-key class --
                      # every filter/pagination query param (FileSystemId, AccessPointId,
                      # MountTargetId, CreationToken, Marker/MaxItems, NextToken/MaxResults,
                      # tagKeys) is read under its exact real key. Two pre-existing gaps recorded,
                      # not fixed: DeleteReplicationConfiguration's deletionMode (no cross-account/
                      # region concept to differ on) and ListTagsForResource/DescribeTags pagination
                      # (already flagged deferred below; tag maps are small and bounded in practice).
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateFileSystem:                  {wire: fixed, errors: ok, state: ok, persist: ok, note: "gopherstack-6flj follow-up: the real Backup *bool request member (api_op_CreateFileSystem.go -- default false, but true when AvailabilityZoneName is set) had no field at all in createFileSystemBody/CreateFileSystemRequest -- a real SDK client's Backup:true was silently dropped, and DescribeBackupPolicy always reported DISABLED regardless. Added; also implements the documented One-Zone default-flip (Backup omitted + AvailabilityZoneName set -> ENABLED)."}
  DescribeFileSystems:               {wire: ok, errors: ok, state: ok, persist: ok, note: "pagination data-loss bug fixed this pass, see notes"}
  DeleteFileSystem:                  {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateFileSystem:                  {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateFileSystemProtection:        {wire: ok, errors: ok, state: ok, persist: ok}
  CreateMountTarget:                 {wire: fixed, errors: ok, state: ok, persist: ok, note: "IpAddressType/Ipv6Address (dual-stack) support added this pass -- was a real gap, not previously documented. Also removed fabricated MountTargetArn/SecurityGroups from the response -- types.MountTargetDescription has neither field at all."}
  DescribeMountTargets:              {wire: ok, errors: ok, state: ok, persist: ok, note: "Ipv6Address emitted when set; pagination data-loss bug fixed 2026-07-23; fabricated MountTargetArn/SecurityGroups removed 2026-08-20, see notes"}
  DeleteMountTarget:                 {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeMountTargetSecurityGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyMountTargetSecurityGroups:   {wire: ok, errors: fixed, state: ok, persist: ok, note: "SecurityGroupLimitExceeded now 400 not 409"}
  CreateAccessPoint:                 {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAccessPoints:              {wire: ok, errors: ok, state: ok, persist: ok, note: "pagination data-loss bug fixed this pass, see notes"}
  DeleteAccessPoint:                 {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource:                       {wire: fixed, errors: ok, state: ok, persist: ok, note: "was unreachable via real SDK -- see route-matcher fix below. Re-checked (wrapper-key sweep) against the sfn TagResource map/array bug class: efs's Tags is []types.Tag, array of {Key,Value} (api_op_TagResource.go:42, serializers.go:2883-2898), matching this emulator's []tagEntry{Key,Value} exactly -- genuinely clean, confirmed via a real-client round-trip test (tag_resource_sdk_test.go)."}
  UntagResource:                     {wire: fixed, errors: ok, state: ok, persist: ok, note: "was unreachable via real SDK -- see route-matcher fix below"}
  ListTagsForResource:               {wire: fixed, errors: ok, state: ok, persist: ok, note: "was unreachable via real SDK -- see route-matcher fix below"}
  DescribeTags:                      {wire: ok, errors: ok, state: ok, persist: ok, note: "legacy GET-only op, distinct path from TagResource family; pagination (Marker/MaxItems) not applied server-side -- deferred, see gaps"}
  CreateTags:                        {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteTags:                        {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeLifecycleConfiguration:    {wire: ok, errors: ok, state: ok, persist: ok}
  PutLifecycleConfiguration:         {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (gopherstack-hnyl): isValidTransitionToIA/isValidTransitionToArchive were hand-copied lists each missing AFTER_1_DAY and each wrongly accepting values from other fields (TransitionToIA took a nonexistent \"NONE\"; TransitionToArchive took AFTER_1_ACCESS, which belongs to TransitionToPrimaryStorageClassRules, plus a typo'd AFTER_90_DAYS_1). Both now derive from types.TransitionToIARules.Values()/types.TransitionToArchiveRules.Values()."}
  CreateReplicationConfiguration:    {wire: fixed, errors: ok, state: ok, persist: ok, note: "Destination.LastReplicatedTimestamp populated (epoch-seconds) at creation since 2026-07-23; 2026-08-20: removed fabricated FileSystemArn/AvailabilityZoneName/KmsKeyId from Destination response entries and added the real RoleArn field, see notes; 2026-08-21: Destination.Region (required output member, types/types.go:116-119) now defaulted to the source region for same-region replication (DestinationToCreate.Region is optional on input) -- see gopherstack-r80d batch 17 note below; 2026-08-29: Destination.StatusMessage (a real, non-required types.Destination member) was never modeled in ReplicationDestination at all -- added, but dormant: this backend's replication Status is always synchronously ENABLED (never PAUSED/ERROR), so no code path yet writes a non-empty value. Wired for wire-shape completeness, no test (indistinguishable from the pre-fix behavior on an always-empty field, same reasoning as route53resolver's ResolverRuleAssociation.StatusMessage)."}
  DeleteReplicationConfiguration:    {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-08-29 wrapper-key sweep: REQUEST direction verified against efs@v1.44.4 serializers.go. deletionMode query param (serializers.go:906) never read -- gap, not a bug: this backend models a single account/region, so ALL_CONFIGURATIONS vs LOCAL_CONFIGURATION_ONLY has no distinguishable backing state to differ on"}
  DescribeReplicationConfigurations: {wire: fixed, errors: ok, state: ok, persist: ok, note: "NextToken/MaxResults pagination implemented 2026-07-23; LastReplicatedTimestamp int64 epoch-seconds since 2026-07-23; 2026-08-20: same fabricated-field/RoleArn fix as CreateReplicationConfiguration, both share destinationToResponse; 2026-08-29: shares CreateReplicationConfiguration's StatusMessage fix, see its entry"}
  DescribeFileSystemPolicy:          {wire: ok, errors: ok, state: ok, persist: ok}
  PutFileSystemPolicy:               {wire: ok, errors: fixed, state: ok, persist: ok, note: "malformed/oversized policy now returns InvalidPolicyException (400), not ValidationException -- ValidationException isn't even in botocore's PutFileSystemPolicy error catalog (BadRequest, InternalServerError, FileSystemNotFound, InvalidPolicyException, IncorrectFileSystemLifeCycleState)"}
  DeleteFileSystemPolicy:            {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeBackupPolicy:              {wire: ok, errors: ok, state: ok, persist: ok}
  PutBackupPolicy:                   {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAccountPreferences:        {wire: ok, errors: ok, state: ok, persist: n/a, note: "account-level, not resource state"}
  PutAccountPreferences:             {wire: ok, errors: ok, state: ok, persist: n/a}
families:
  FileSystem:        {status: ok, note: "CRUD + Update + Protection audited op-by-op; epoch timestamps, SizeInBytes nesting, FileSystemProtection nesting all verified byte-for-byte against aws-sdk-go-v2 deserializers"}
  MountTarget:        {status: ok, note: "CRUD + SecurityGroups audited; SecurityGroupLimitExceeded status code fixed (was 409, AWS uses 400 per botocore efs/service-2.json); IpAddressType/Ipv6Address dual-stack support added this pass (was a real, previously-undocumented gap -- CreateMountTargetInput.IpAddressType/Ipv6Address and MountTargetDescription.Ipv6Address exist in the real SDK types but gopherstack had no fields for them at all)"}
  AccessPoint:        {status: ok, note: "CRUD + ClientToken idempotency + PosixUser/RootDirectory shapes audited"}
  Tags:                {status: ok, note: "route-matcher bug fixed -- see below; CreateTags/DeleteTags legacy ops verified separately, correct as-is"}
  BackupPolicy:        {status: ok}
  LifecycleConfiguration: {status: ok}
  FileSystemPolicy:   {status: ok, note: "InvalidPolicyException vs ValidationException distinction fixed this pass -- previously deferred, now closed for real (both malformed-JSON and oversized-policy paths)"}
  ReplicationConfiguration: {status: ok, note: "pagination implemented + Destination timestamp typing/population fixed this pass -- previously deferred, now closed for real; 2026-08-21 required-output Region fix, see below"}
  AccountPreferences: {status: ok}
gaps:
  - FileSystemLimitExceeded / AccessPointLimitExceeded (account-level Service Quota errors, HTTP 403) are not simulated. Unlike SecurityGroupLimitExceeded (a fixed, non-adjustable per-mount-target structural limit of 5, which IS enforced), these are adjustable per-account Service Quotas with high documented defaults (hundreds to low thousands depending on resource/file-system type) that operators can raise via the Service Quotas console. There is no account-quota-configuration model anywhere in this backend to hang an enforceable, configurable threshold off of, and hardcoding an arbitrary number risks breaking legitimate high-volume test/load usage of the mock for no wire-shape or state-correctness benefit (no SDK client behavior differs based on whether this specific 403 is reachable). Deferred; see items_still_open in the audit receipt for the full reasoning.
  - DescribeTags (the legacy GET-only op, distinct from the resource-tags family) does not apply Marker/MaxItems pagination server-side -- always returns the full tag set in one page. Low priority: EFS caps tags per resource at 50 (maxTagsPerResource), so a single page is always sufficient in practice; a real client would never actually see a second page from real AWS either at that low a cap.
deferred:
  - DescribeTags pagination (Marker/MaxItems) -- see gaps; capped at 50 tags/resource so unreachable in practice.
  - FileSystemLimitExceeded / AccessPointLimitExceeded account-quota simulation -- see gaps; no account-quota-config model exists to hang a real threshold off.
leaks: {status: clean, note: "single self-terminating goroutine (fsActivationDelay simulation in CreateFileSystem) guards against concurrent deletion via a Get-under-lock check before mutating state; only active when fsActivationDelay>0, which is zero (disabled) outside parity tests. No new goroutines/tickers added this pass (mount-target IPv6 fields, replication pagination, and LastReplicatedTimestamp are all synchronous state mutations)."}
---

## Notes

Protocol: **restjson1**, path-versioned under `/2015-02-01/...`.

### Bugs found and fixed this pass (2026-08-20)

This pass was a targeted wrapper-key / nesting-level / fabricated-member sweep
(the same campaign that found ~45 bugs across 18 other services this session),
re-derived directly from `aws-sdk-go-v2/service/efs@v1.44.4`'s deserializers
and `types/types.go`, not from the prior 2026-07-23 audit's own notes. That
prior audit was performed against SDK v1.41.12 (its own "Verification method"
section says so) even though the header pinned v1.44.4 at the time -- the SDK
was bumped v1.41.12 -> v1.44.0 on 2026-08-05 (`build(deps): bump all 169 AWS
service SDK modules`, commit `7e220f1efd`) and v1.44.0 -> v1.44.4 on
2026-08-19 (`fix(deps): remediate security vulnerabilities`, commit
`1b0b3b8fd1`), both after that audit ran. Re-checking efs's 31-op surface
against the currently-pinned v1.44.4 found no operation additions/removals
(same 31 ops both versions), so the op inventory itself was not stale --
these two bugs are new findings, not shape drift from the version bump. None
of the 2026-07-23 pass's "FIXED" claims (pagination data loss, IPv6 mount
targets, error status codes, InvalidPolicyException, replication pagination
+ timestamp typing) failed re-derivation against v1.44.4; they were spot
verified via `go test` and by re-reading the relevant deserializer functions,
and stand as before.

7. **`mtToResponse` (`handler_mount_targets.go`, used by both `CreateMountTarget`
   and `DescribeMountTargets`) fabricated two members with no case at all in the
   real deserializer.** `aws-sdk-go-v2/service/efs@v1.44.4`'s
   `types.MountTargetDescription` (`types/types.go`) has exactly eleven fields --
   `FileSystemId`, `LifeCycleState`, `MountTargetId`, `SubnetId`,
   `AvailabilityZoneId`, `AvailabilityZoneName`, `IpAddress`, `Ipv6Address`,
   `NetworkInterfaceId`, `OwnerId`, `VpcId` -- and no ARN field and no
   `SecurityGroups` field at all; `deserializers.go`'s
   `awsRestjson1_deserializeDocumentMountTargetDescription` declares cases for
   exactly those eleven and nothing else. gopherstack additionally emitted a
   `MountTargetArn` (unconditionally) and a `SecurityGroups` list (when
   non-empty) on every `CreateMountTarget` and `DescribeMountTargets` response
   entry -- both silently dropped by a real SDK client, since mount targets have
   no ARN concept in the real API at all, and security groups are exposed only
   via the separate `DescribeMountTargetSecurityGroups` operation (a different,
   bare-list response shape, left untouched and still correct). This is the
   dominant bug-class pattern from this session's campaign: a member generalized
   from a wider/adjacent shape (`FileSystemArn` exists on `FileSystemDescription`;
   `SecurityGroups` exists on `CreateMountTargetInput` as an INPUT field and on
   `DescribeMountTargetSecurityGroupsOutput` as its own operation's whole
   payload) leaking onto a narrower type that has neither. Two existing tests had
   locked in the fabricated `MountTargetArn` field by name --
   `TestMountTargetArn` (asserted the ARN was present and matched a pattern) and
   one assertion inside `TestDescribeMountTargets_AccessPointIdFilter_HTTP` --
   and a third, `TestMountTargetSecurityGroups`, asserted the fabricated
   `SecurityGroups` list on the `CreateMountTarget` response body. All three
   fixed: `TestMountTargetArn` renamed to `TestMountTargetArn_NotOnWire` and
   inverted to assert absence; the AccessPointId-filter test's assertion swapped
   to check `MountTargetId` instead; `TestMountTargetSecurityGroups` rewritten to
   assert `SecurityGroups` is absent from the create response and instead verify
   the value persisted via a follow-up `DescribeMountTargetSecurityGroups` call
   (the correct, real operation for observing it). New round-trip test:
   `TestMountTargetDescription_NoFabricatedSecurityGroups`
   (`wire_sdk_roundtrip_test.go`) -- a raw-body absence assertion, since
   `types.MountTargetDescription` has no field to bind either fabricated key to.

8. **`rcToResponse` (`handler_replication.go`, used by `CreateReplicationConfiguration`
   and `DescribeReplicationConfigurations`) serialized `ReplicationConfiguration.Destinations`
   directly via Go struct tags, leaking three request-only fields onto the
   response wire.** gopherstack's single `ReplicationDestination` Go struct was
   reused for both the incoming request body (shaped like the real SDK's
   request-side `types.DestinationToCreate`, which legitimately has
   `AvailabilityZoneName`/`KmsKeyId`/`RoleArn` as input fields --
   `serializers.go`'s `awsRestjson1_serializeDocumentDestinationToCreate`) and the
   stored/output value, marshaled straight through `json.Marshal`'s struct-tag
   reflection for the response. But the real response-side `types.Destination`
   (`types/types.go`) has only seven fields -- `FileSystemId`,
   `LastReplicatedTimestamp`, `OwnerId`, `Region`, `RoleArn`, `Status`,
   `StatusMessage` -- and `deserializers.go`'s
   `awsRestjson1_deserializeDocumentDestination` declares cases for exactly those
   seven. `FileSystemArn`, `AvailabilityZoneName`, and `KmsKeyId` (the last of
   which also had the wrong JSON key case, `KmsKeyID` instead of `KmsKeyId`) were
   present on every emitted destination -- `FileSystemArn` unconditionally (it's
   actively computed via `arn.Build` in `replication.go` for internal bookkeeping),
   the other two whenever a caller supplied them on the request -- all three
   silently dropped by a real SDK client, since `types.Destination` has no field
   to bind them to. Conversely, the real, always-meaningful `RoleArn` field
   (present on both the request and response types) was missing from
   gopherstack's struct entirely -- found incidentally while fixing the other
   three, not hunted separately, and trivial to add since the struct was already
   being touched.

   Fixed by adding a `destinationToResponse` builder (matching the
   `fsToResponse`/`mtToResponse`/`apToResponse` convention already used
   elsewhere in this package) that emits exactly the seven real
   `types.Destination` fields, and routing `rcToResponse` through it instead of
   marshaling `rc.Destinations` directly; added the `RoleArn` field to
   `ReplicationDestination` (flows through automatically since
   `CreateReplicationConfiguration`'s `copy(dests, destinations)` already copies
   the whole struct) and corrected `KmsKeyID`'s request-parsing json tag from
   `"KmsKeyID"` to `"KmsKeyId"`. One existing test,
   `TestReplicationConfiguration_DestinationHasArnAndOwner`, explicitly asserted
   `FileSystemArn` was present and looked like a valid ARN ("Real AWS generates a
   destination file system and includes its ARN and owning account" -- the ARN
   half of that claim is false per the real SDK); renamed to
   `TestReplicationConfiguration_DestinationHasOwnerNoArn` and inverted to assert
   `FileSystemArn`'s absence on the raw body while keeping the legitimate
   `FileSystemId`/`OwnerId`/`Status` assertions. New round-trip tests:
   `TestReplicationDestination_NoFabricatedFields` (raw-body absence assertion,
   since `types.Destination` has no field to bind the three fabricated keys to)
   and `TestReplicationDestination_RoleArnRoundTrips` (typed real-SDK-client
   round-trip through `CreateReplicationConfiguration` and
   `DescribeReplicationConfigurations`, proving the newly-added `RoleArn` field
   actually reaches the wire in both directions).

Both fixes were proven by hand-revert: reintroducing each removed fabrication
(or reverting `rcToResponse`/`mtToResponse` to their pre-fix bodies) reproduced
the exact predicted test failures (`MountTargetArn`/`SecurityGroups`/
`FileSystemArn`/`AvailabilityZoneName`/`KmsKeyId` present-when-should-be-absent),
and restoring the fix reproduced a byte-identical file (verified via `md5sum`
before/after on all three touched files: `handler_mount_targets.go`,
`handler_replication.go`, `models.go`).

### Bugs found and fixed 2026-07-23 pass

0. **Cross-cutting pagination data-loss bug (critical, pre-existing, newly caught):**
   the shared `paginate()` helper in `services/efs/store.go` -- used by
   `DescribeFileSystems`, `DescribeMountTargets`, `DescribeAccessPoints`, and (as of
   this pass) `DescribeReplicationConfigurations` -- silently **dropped exactly one
   item at every page boundary**. `paginate()` computes `next := keyFn(items[maxItems])`,
   i.e. the key of the first item NOT included in the current page. But the resume
   branch treated an incoming marker as "the key of an item already delivered" and
   skipped past it (`items = items[idx+1:]`), when that item had in fact never been
   returned at all. Net effect: a client paginating N resources at page size P would
   observe strictly fewer than N total across all pages (one lost per page boundary
   crossed) -- e.g. 10 file systems at page size 3 previously yielded only 8 (3+3+2),
   never landing on item index 3 or index 7. This is a genuine parity-breaking
   correctness bug for any real SDK client (or the AWS CLI, or Terraform) that
   actually follows `NextMarker`/`NextToken` across more than one page, not a
   cosmetic wire-shape nit.

   **Why the prior "ok" ratings on FileSystem/MountTarget/AccessPoint pagination
   missed this**: every existing pagination test checked page-1's length and that
   `NextMarker` was non-empty, but none of them followed the marker through *every*
   page and checked the *union* against the full created set. One test
   (`TestDescribeFileSystems_PaginationMarker`, pre-existing) had in fact **encoded
   the data loss as the expected behavior** in its own comment ("marker = first item
   of next page, skipped on resume") and asserted a 3-page walk over 10 items landing
   on only 8 -- a documented-as-intentional bug. Caught this pass by a
   `DescribeReplicationConfigurations` pagination regression test written to check
   the union of all pages against the total created (not just page-1's count), which
   is the right invariant for any cursor-based list API and is what the AWS ops
   themselves guarantee.

   Fixed by resuming at `items[idx:]` (inclusive of the matched marker item) instead
   of `items[idx+1:]`. Updated every affected test:
   `TestDescribeFileSystems_PaginationMarker` (rewritten to walk pages via a loop and
   assert the union equals the full created set, replacing the old fixed 3-page
   script that baked in the bug), `TestDescribeMountTargets_Pagination_HTTP`,
   `TestDescribeFileSystems_Pagination`, `TestDescribeMountTargets_Pagination`,
   `TestDescribeAccessPoints_Pagination` (the last three strengthened to check the
   union of pages, not just page-1's length).

1. **Route-matcher bug (critical): TagResource / UntagResource / ListTagsForResource were
   unreachable by real SDK clients.** `services/efs/handler.go`'s `RouteMatcher()` and
   `parseEFSPath` routed these three ops under `/2015-02-01/tags/{id}`, but
   `aws-sdk-go-v2/service/efs`'s actual serializers (`serializers.go`,
   `awsRestjson1_serializeOpTagResource` / `OpUntagResource` / `OpListTagsForResource`) send
   them to `/2015-02-01/resource-tags/{ResourceId}` -- a path the old `RouteMatcher` never
   recognized at all (no `resource-tags` prefix in the matcher), so a real SDK client's
   `TagResource` call would fail to match any route in gopherstack's router entirely. The
   `/2015-02-01/tags/{FileSystemId}` path is reserved for the separate, deprecated,
   **GET-only** `DescribeTags` op. Existing unit tests (`handler_test.go`) called
   `h.Handler()(c)` directly with hand-built requests reusing the wrong `/2015-02-01/tags/`
   path for Tag/List/Untag, which bypassed `RouteMatcher()` entirely and hid the bug -- this
   is the same test-shape trap noted in the parity-principles doc (unit tests are not parity
   proof). Fixed by adding a `pathResourceTags` constant, wiring it into `RouteMatcher()` and
   `parseEFSPath` via a new `parseResourceTagsRoute`, and narrowing the old `parseTagsRoute`
   (renamed `parseLegacyTagsRoute`) to GET-only -> `DescribeTags`. All affected tests updated
   to hit `/2015-02-01/resource-tags/{id}` for Tag/Untag/List, with new route-matcher-driven
   regression cases added to `TestHandlerRouteMatching` (`tag_resource`, `list_tags`,
   `untag_resource`, `describe_tags_legacy`, `tags_legacy_path_post_unmatched_operation`) so a
   future edit can't silently reintroduce the collision without a matcher-level test failing.

2. **SecurityGroupLimitExceeded returned HTTP 409, AWS returns 400.** Verified against
   botocore's `efs/service-2.json` (`httpStatusCode: 400`) -- this is a client input-validation
   error (too many security groups per mount target, max 5), not a resource conflict. Three
   pre-existing tests (`parity_a_test.go`, `handler_refinement2_test.go` x2) had locked in the
   wrong 409 expectation from an earlier audit; updated alongside the fix, plus two new cases
   added to `handler_test.go` (`TestMountTargetCRUD`, `TestDescribeMountTargetSecurityGroups`).

3. **PolicyNotFound returned HTTP 400, AWS returns 404.** Verified against botocore's
   `efs/service-2.json` (`httpStatusCode: 404`). A prior audit's test
   (`TestBatch2_DescribeFileSystemPolicy_PolicyNotFound` in `handler_batch2_audit_test.go`)
   had explicitly asserted 400 "matching AWS EFS behaviour" -- that assertion was itself wrong;
   fixed alongside `handler_test.go`'s `TestFileSystemPolicy`.

4. **PutFileSystemPolicy used ValidationException for malformed/oversized policy JSON;
   botocore's error catalog for this op doesn't even include ValidationException.**
   `efs/service-2.json`'s `PutFileSystemPolicy` operation lists exactly
   `BadRequest, InternalServerError, FileSystemNotFound, InvalidPolicyException,
   IncorrectFileSystemLifeCycleState` as possible errors -- no `ValidationException`.
   Added `ErrInvalidPolicy` (`InvalidPolicyException`, HTTP 400 per botocore) and switched
   both the malformed-JSON and oversized-policy (>20KB) validation paths in
   `file_system_policy.go` to use it instead of the generic `ErrValidation`. Updated
   `file_system_policy_test.go` (added an oversized-policy case, added a `NotErrorIs
   efs.ErrValidation` guard so the two error kinds can never silently collapse back
   together) and added `TestPutFileSystemPolicy_InvalidPolicyRejected` in
   `handler_file_system_policy_test.go` to lock in the HTTP-level `ErrorCode`.

5. **DescribeReplicationConfigurations had no pagination at all** (always returned every
   replication configuration for the account/region in a single page, ignoring
   `NextToken`/`MaxResults` entirely) **and `ReplicationDestination.LastReplicatedTimestamp`
   was a dormant, never-populated `string` field** where the real SDK's
   `types.Destination.LastReplicatedTimestamp` is `*time.Time` (epoch-seconds on the wire
   under restjson1). Fixed both: `DescribeReplicationConfigurations` now takes
   `marker string, maxItems int` and pages the unfiltered list via the shared `paginate()`
   helper (same convention as FileSystems/MountTargets/AccessPoints), keyed on
   `SourceFileSystemID`; single-ID lookups (by source or destination file system ID) remain
   unpaginated, matching the existing `describeByIDOrFilter` convention. `LastReplicatedTimestamp`
   changed from `string` to `int64` (epoch-seconds, `omitempty` so it naturally omits when
   unset) and is now populated at `CreateReplicationConfiguration` time -- the mock completes
   its (synchronous, in-memory) initial sync immediately, so the destination is caught up as
   of creation time; real AWS instead leaves it unset until an actual background sync
   completes, which is not something this mock simulates asynchronously. Added
   `TestDescribeReplicationConfigurations_Pagination` and
   `TestReplicationConfiguration_DestinationLastReplicatedTimestamp` in
   `handler_replication_test.go`; updated the two direct backend call sites in
   `persistence_test.go` for the new 4-arg signature.

6. **CreateMountTarget had no support for `IpAddressType` / `Ipv6Address` (dual-stack /
   IPv6-only mount targets) at all** -- not documented as a gap in the prior audit, found by
   field-diffing `aws-sdk-go-v2/service/efs`'s `CreateMountTargetInput` /
   `MountTargetDescription` types directly against `services/efs/models.go`. The real SDK's
   `CreateMountTargetInput.IpAddressType` (`IPV4_ONLY` / `IPV6_ONLY` / `DUAL_STACK`, defaults
   to `IPV4_ONLY` when omitted per the SDK doc comment) and `.Ipv6Address` fields, plus
   `MountTargetDescription.Ipv6Address` on the output side, had no equivalents anywhere in
   gopherstack's EFS mock. Added `IPAddressType`/`IPv6Address` to `MountTarget` and
   `CreateMountTargetRequest` (`models.go`), request-body parsing in
   `handler_mount_targets.go`, enum validation + default-to-`IPV4_ONLY` in
   `mount_targets.go`'s `CreateMountTarget` (returns `ValidationException` for an unknown
   `IpAddressType`, matching the existing `PerformanceMode`/`ThroughputMode` validation
   convention in `file_systems.go`), and `Ipv6Address` (only, not `IpAddressType` --
   `MountTargetDescription` doesn't have an `IpAddressType` output member) in the
   `mtToResponse` wire response. Added `mount_target_ip_address_type_test.go` covering the
   enum validation and the `Ipv6Address` wire round-trip.

Neither of the two original error-status fixes (#2, #3) changes SDK-client-observable retry
behavior (aws-sdk-go-v2's deserializer error-dispatch switches on the `X-Amzn-ErrorType`
header/body error code, not the raw HTTP status, and both old and new status codes fall
outside the 429/5xx retryable range) -- but they are genuine wire-shape deviations from real
AWS worth fixing for parity, and would be observable to any caller inspecting raw HTTP status
codes directly. Fix #4 (InvalidPolicyException) is likewise same-status-different-code (both
400), same reasoning. Fix #0 (pagination data loss) and #6 (IPv6 mount targets) are both
directly client-observable regardless of status-code nuance.

### Verification method

2026-07-23 pass: wire shapes (timestamps, error status codes, list-response keys,
query-param names, request/response field sets) were cross-checked directly against
`aws-sdk-go-v2/service/efs@v1.41.12`'s generated `serializers.go` / `deserializers.go` /
`types/types.go` / `types/errors.go` / per-op `api_op_*.go` files (in the local Go module
cache), plus `botocore`'s `efs/service-2.json` service model (installed locally via pip, read
via `gzip`+`json` since the installed copy ships gzip-compressed) for the authoritative
per-error `httpStatusCode` table and per-operation error catalogs. That pass additionally
wrote/strengthened pagination tests that walk *every* page and assert the *union* against the
full created set (not just a single page's length), which is what caught bug #0 above -- a
class of bug invisible to single-page assertions.

2026-08-20 pass: re-derived against the now-pinned `aws-sdk-go-v2/service/efs@v1.44.4` (the
module was bumped twice since the prior audit -- see the note under "Bugs found and fixed
this pass (2026-08-20)" above). Method: for every op, read its own live
`awsRestjson1_deserializeOp<Op>` `HandleDeserialize` to confirm which `deserializeOpDocument`
path is actually called (all 31 efs ops call theirs directly on the whole decoded body --
none hit the flat/dead-code trap that bit glacier/appmesh this session, since no efs op's
Output has a single httpPayload-tagged member), then for every emitted field of every
response-shaped type, matched it against that type's own
`awsRestjson1_deserializeDocument<Type>` case list and `types/types.go` field list --
never assumed a field carries over from a same-named or adjacent type. Cross-checked every
enum's validation set (`LifeCycleState`, `PerformanceMode`, `ThroughputMode`,
`TransitionToIARules`, `TransitionToArchiveRules`, `TransitionToPrimaryStorageClassRules`,
`ReplicationStatus`, `ResourceIdType`, `IpAddressType`, `Status`,
`ReplicationOverwriteProtection`) against `types/enums.go`'s `Values()` -- all clean;
`lifecycle_config.go`'s IA/Archive validators already derive from `Values()` directly (fixed
2026-07-23) so they cannot drift on a future SDK bump.

### Looks-wrong-but-correct traps (for the next auditor)

- Mount targets have **no ARN and no `SecurityGroups` member on the wire at all** in real
  AWS -- `types.MountTargetDescription` genuinely has neither field (see bug #7 above).
  `SecurityGroups` IS a real field, but only as an INPUT on `CreateMountTargetInput` (settable
  at creation) and as the entire bare-list OUTPUT of the separate
  `DescribeMountTargetSecurityGroups` operation (`handler_mount_target_security_groups.go`,
  untouched and correct) -- never as an output member of `CreateMountTarget`/
  `DescribeMountTargets` themselves. Don't re-add either field to `mtToResponse` on the
  assumption that "surely a real filesystem resource has security groups in its description";
  verify against `types/types.go` first.
- Similarly, a replication `Destination` entry has no ARN, no `AvailabilityZoneName`, and no
  `KmsKeyId` on the wire -- those three exist only on the request-side `DestinationToCreate`
  sibling type (see bug #8 above). `destinationToResponse` in `handler_replication.go` is the
  single source of truth for what a `Destination` may emit; don't widen it by copying fields
  from `ReplicationDestination`'s (request-shaped) struct tags.
- `DescribeTags` and `ListTagsForResource` share one handler
  (`handleListTagsForResource`) in `dispatchTagAndMiscOps` despite being different real AWS
  operations at different paths. This is correct: both `DescribeTagsOutput` and
  `ListTagsForResourceOutput` use the same wire key (`Tags`, an array of `{Key, Value}`), so
  reusing the handler is not a wire-shape bug -- don't "fix" this by splitting them apart.
- `CreateFileSystem`'s idempotent-retry path (identical `CreationToken` + identical args)
  returns HTTP 200 with the existing file system, while a fresh create returns 201. This
  matches the existing `ErrCreationTokenExists` handling and is intentional, not a status-code
  bug.
- `DeleteFileSystemPolicy`'s real AWS `responseCode` is 200 per botocore, but gopherstack
  returns 204 (`NoContent`). Left as-is: `aws-sdk-go-v2`'s restjson1 deserializers accept any
  `2xx` for void-result ops (`response.StatusCode < 200 || >= 300` is the only check across
  every `HandleDeserialize` in `deserializers.go`), so this deviation is wire-invisible to real
  SDK clients. Not worth the diff churn to "fix" -- documented here so it isn't mistakenly
  flagged as a live bug by a future sweep.
- `paginate()`'s marker is the key of the first NOT-yet-returned item (computed as
  `keyFn(items[maxItems])`), and resuming with that marker must start **at** (inclusive of)
  the matched index (`items[idx:]`), not after it. If a future edit "simplifies" this back to
  `items[idx+1:]`, it silently reintroduces bug #0 above -- there is no compiler or type-system
  guard against this, only the pagination-union tests. Don't change this without re-reading the
  bug-history comment directly above `paginate()` in `store.go`.

### 2026-08-21: required-output Region dropped for same-region replication (gopherstack-r80d batch 17)

`cmd/requiredoutputfields` flags `CreateReplicationConfiguration`/
`DescribeReplicationConfigurations`'s nested `Destination` type (efs@v1.44.4
`types/types.go:109-153`) as requiring `FileSystemId`, `Region`, and `Status`
("This member is required.") -- invisible to the per-op scan since
`ReplicationConfigurationDescription.Destinations` is itself only a `[]Destination`,
not inlined.

`FileSystemId` and `Status` were already unconditionally defaulted in
`replication.go`'s `CreateReplicationConfiguration`. `Region` was not: the loop only
read `dests[i].Region` into a local `destRegion` fallback for computing the synthetic
destination `FileSystemArn`, never writing the fallback back onto the struct itself.
`ReplicationDestination.Region` (models.go) is tagged `json:"Region,omitempty"`, so for
any client that omits `Region` on a `DestinationToCreate` -- the documented path for
same-region replication (`efs@v1.44.4 types/types.go:225-231`: "To create a file system
that uses Regional storage, specify the ... Region ...", no "This member is required."
on the input type at all) -- the required output key vanished from the wire entirely.

Fixed by defaulting `dests[i].Region` to the source region up front, same as
`Status`/`OwnerID`. Proven via a real `aws-sdk-go-v2/service/efs` client round trip
(`wire_output_required_r80d_test.go`) that creates a replication configuration with no
`Region` on the destination and asserts the real client's typed `Region` field is
non-nil and correct; hand-reverted against `HEAD:services/efs/replication.go`
(confirmed-failing), restored, md5sum byte-identical.

Not a bug, disclosed for the next auditor: the real `Destination` struct has exactly 7
response fields (`FileSystemId`, `Region`, `Status`, `LastReplicatedTimestamp`,
`OwnerId`, `RoleArn`, `StatusMessage`) -- gopherstack's `ReplicationDestination` also
carries `FileSystemArn` and `AvailabilityZoneName`, neither of which exists on the real
wire type at all. These are harmless extraneous keys (ignored by a real restjson1
client, which decodes only fields it declares) rather than a required-field
violation, so left as-is rather than removed as part of this cut -- out of scope for a
required-*missing*-field audit.

### 2026-08-29: write-only-state sweep (gopherstack-6flj follow-up)

Method: for each domain struct in `models.go`, enumerated every field the backend
persists, then checked which real operation can read it back, per family
(FileSystem, MountTarget, AccessPoint, Replication, LifecycleConfiguration,
FileSystemPolicy, BackupPolicy, AccountPreferences). Cross-checked every
List/Describe-shaped SDK input struct's own field list against gopherstack's
matching wire-input struct (not just the response side), since an accepted
request field that's silently dropped is the same bug class in reverse.
`enumcheck`/`acceptguard`/`zeroguard`/`xmlitemwrap` (repo-wide, grepped for
`services/efs`) found nothing for this service.

**`CreateFileSystemInput.Backup` silently dropped (critical, request-side):**
`api_op_CreateFileSystem.go`'s `CreateFileSystemInput.Backup *bool` ("Specifies
whether automatic backups are enabled... Default is false. However, if you
specify an AvailabilityZoneName, the default is true") had no counterpart at
all in `createFileSystemBody`/`CreateFileSystemRequest` -- a real SDK client's
`Backup: aws.Bool(true)` was accepted by JSON unmarshal (unknown-field-tolerant)
and then discarded, so a follow-up `DescribeBackupPolicy` always reported
`DISABLED` regardless of what the client asked for at creation time. This is
the "accepted from a request and never stored" write-only-state pattern.
Fixed: added `Backup *bool` end to end (`createFileSystemBody` ->
`CreateFileSystemRequest` -> `CreateFileSystem`'s new
`enableBackup`/`backupStore` write, mirroring the documented One-Zone
default-flip when `Backup` is omitted but `AvailabilityZoneName` is set).
Proven by hand-revert: reverting the three call sites reproduced
`TestCreateFileSystem_BackupRoundTrips`/`TestCreateFileSystem_OneZoneDefaultsBackupEnabled`
failing (`DescribeBackupPolicy` returning `DISABLED` instead of `ENABLED`);
restored, tests pass (`wire_sdk_roundtrip_test.go`).

**`Destination.StatusMessage` never modeled (dormant):** see
`CreateReplicationConfiguration`'s ops entry above. Real, non-required
`types.Destination` member with no gopherstack field to source a value from at
all; the backend's replication `Status` never transitions to `PAUSED`/`ERROR`
(always synchronous `ENABLED`), so the fix is real but currently unreachable --
flagged, not manufactured into a fake failure scenario.

**Confirmed clean by this sweep (not re-litigating the 2026-08-20 pass, but
independently re-derived against the same pinned SDK)**: `ResolverEndpoint`... n/a
(that's route53resolver) -- for EFS: `FileSystemDescription` (18 fields, all
present including nested `SizeInBytes`/`FileSystemProtection`),
`MountTargetDescription` (11 fields, no ARN/SecurityGroups, matches the
2026-08-20 fix), `AccessPointDescription` (10 fields, all present),
`ReplicationConfigurationDescription`/`Destination` (6 + 7 fields), `LifecyclePolicy`
(3 fields), `BackupPolicy` (1 field), `ResourceIdPreference` (`ResourceIdType`
+ `Resources`, the latter a fixed `[FILE_SYSTEM, MOUNT_TARGET]` value since
this mock's ID-preference setting always applies to both -- not fabricated).
`CreateMountTargetInput`/`UpdateFileSystemInput` request-side field sets also
verified complete against `api_op_*.go`.

Everything else in this service's required-output surface came back clean: `PosixUser`
(`Uid`/`Gid`) and `CreationInfo` (`Permissions`/`OwnerUid`/`OwnerGid`) -- both nested,
optional-parent domain structs reachable only through `AccessPoint.PosixUser`/
`RootDirectory.CreationInfo` -- have no `omitempty` on any of their own required
members in `models.go`, so they're never dropped once the optional parent is present.
`FileSystemDescription`'s 9 required members and `MountTargetDescription`'s 4 are all
built unconditionally into their response maps (`fsToResponse`/`mtToResponse`).
`DescribeMountTargetSecurityGroups`'s `SecurityGroups` and `DescribeTags`'s `Tags` are
both always non-nil, always-present keys. `BackupPolicy.Status` is always present.
