---
service: efs
sdk_module: aws-sdk-go-v2/service/efs@v1.44.4   # version audited against
last_audit_commit: d59548b925a89fc0b11453a8877e95ae59073158
last_audit_date: 2026-07-23
overall: A            # cross-cutting pagination data-loss bug fixed + 4 gaps closed for real
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateFileSystem:                  {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeFileSystems:               {wire: ok, errors: ok, state: ok, persist: ok, note: "pagination data-loss bug fixed this pass, see notes"}
  DeleteFileSystem:                  {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateFileSystem:                  {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateFileSystemProtection:        {wire: ok, errors: ok, state: ok, persist: ok}
  CreateMountTarget:                 {wire: ok (fixed), errors: ok, state: ok, persist: ok, note: "IpAddressType/Ipv6Address (dual-stack) support added this pass -- was a real gap, not previously documented"}
  DescribeMountTargets:              {wire: ok (fixed), errors: ok, state: ok, persist: ok, note: "Ipv6Address now emitted when set; pagination data-loss bug fixed this pass, see notes"}
  DeleteMountTarget:                 {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeMountTargetSecurityGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyMountTargetSecurityGroups:   {wire: ok, errors: ok (fixed), state: ok, persist: ok, note: "SecurityGroupLimitExceeded now 400 not 409"}
  CreateAccessPoint:                 {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAccessPoints:              {wire: ok, errors: ok, state: ok, persist: ok, note: "pagination data-loss bug fixed this pass, see notes"}
  DeleteAccessPoint:                 {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource:                       {wire: ok (fixed), errors: ok, state: ok, persist: ok, note: "was unreachable via real SDK -- see route-matcher fix below"}
  UntagResource:                     {wire: ok (fixed), errors: ok, state: ok, persist: ok, note: "was unreachable via real SDK -- see route-matcher fix below"}
  ListTagsForResource:               {wire: ok (fixed), errors: ok, state: ok, persist: ok, note: "was unreachable via real SDK -- see route-matcher fix below"}
  DescribeTags:                      {wire: ok, errors: ok, state: ok, persist: ok, note: "legacy GET-only op, distinct path from TagResource family; pagination (Marker/MaxItems) not applied server-side -- deferred, see gaps"}
  CreateTags:                        {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteTags:                        {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeLifecycleConfiguration:    {wire: ok, errors: ok, state: ok, persist: ok}
  PutLifecycleConfiguration:         {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (gopherstack-hnyl): isValidTransitionToIA/isValidTransitionToArchive were hand-copied lists each missing AFTER_1_DAY and each wrongly accepting values from other fields (TransitionToIA took a nonexistent \"NONE\"; TransitionToArchive took AFTER_1_ACCESS, which belongs to TransitionToPrimaryStorageClassRules, plus a typo'd AFTER_90_DAYS_1). Both now derive from types.TransitionToIARules.Values()/types.TransitionToArchiveRules.Values()."}
  CreateReplicationConfiguration:    {wire: ok (fixed), errors: ok, state: ok, persist: ok, note: "Destination.LastReplicatedTimestamp now populated (epoch-seconds) at creation, simulating an instant initial sync -- was dormant/unset before this pass"}
  DeleteReplicationConfiguration:    {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeReplicationConfigurations: {wire: ok (fixed), errors: ok, state: ok, persist: ok, note: "NextToken/MaxResults pagination implemented this pass (was previously always a single unpaginated page); LastReplicatedTimestamp now int64 epoch-seconds matching types.Destination.LastReplicatedTimestamp *time.Time wire shape, and populated"}
  DescribeFileSystemPolicy:          {wire: ok, errors: ok, state: ok, persist: ok}
  PutFileSystemPolicy:               {wire: ok, errors: ok (fixed), state: ok, persist: ok, note: "malformed/oversized policy now returns InvalidPolicyException (400), not ValidationException -- ValidationException isn't even in botocore's PutFileSystemPolicy error catalog (BadRequest, InternalServerError, FileSystemNotFound, InvalidPolicyException, IncorrectFileSystemLifeCycleState)"}
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
  ReplicationConfiguration: {status: ok, note: "pagination implemented + Destination timestamp typing/population fixed this pass -- previously deferred, now closed for real"}
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

### Bugs found and fixed this pass (2026-07-23)

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

All wire shapes (timestamps, error status codes, list-response keys, query-param names,
request/response field sets) were cross-checked directly against
`aws-sdk-go-v2/service/efs@v1.41.12`'s generated `serializers.go` / `deserializers.go` /
`types/types.go` / `types/errors.go` / per-op `api_op_*.go` files (in the local Go module
cache), plus `botocore`'s `efs/service-2.json` service model (installed locally via pip, read
via `gzip`+`json` since the installed copy ships gzip-compressed) for the authoritative
per-error `httpStatusCode` table and per-operation error catalogs. This pass additionally
wrote/strengthened pagination tests that walk *every* page and assert the *union* against the
full created set (not just a single page's length), which is what caught bug #0 above -- a
class of bug invisible to single-page assertions.

### Looks-wrong-but-correct traps (for the next auditor)

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
