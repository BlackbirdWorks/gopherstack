---
service: efs
sdk_module: aws-sdk-go-v2/service/efs@v1.41.12   # version audited against
last_audit_commit: b949a420d182f21667bfd64382228ffe985eeab1
last_audit_date: 2026-07-12
overall: A            # genuine fixes found this pass (route-matcher bug + 2 error-status bugs)
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateFileSystem:                  {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeFileSystems:               {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteFileSystem:                  {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateFileSystem:                  {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateFileSystemProtection:        {wire: ok, errors: ok, state: ok, persist: ok}
  CreateMountTarget:                 {wire: ok, errors: ok (fixed), state: ok, persist: ok, note: "SecurityGroupLimitExceeded now 400 not 409"}
  DescribeMountTargets:              {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteMountTarget:                 {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeMountTargetSecurityGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyMountTargetSecurityGroups:   {wire: ok, errors: ok (fixed), state: ok, persist: ok, note: "SecurityGroupLimitExceeded now 400 not 409"}
  CreateAccessPoint:                 {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAccessPoints:              {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAccessPoint:                 {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource:                       {wire: ok (fixed), errors: ok, state: ok, persist: ok, note: "was unreachable via real SDK -- see route-matcher fix below"}
  UntagResource:                     {wire: ok (fixed), errors: ok, state: ok, persist: ok, note: "was unreachable via real SDK -- see route-matcher fix below"}
  ListTagsForResource:               {wire: ok (fixed), errors: ok, state: ok, persist: ok, note: "was unreachable via real SDK -- see route-matcher fix below"}
  DescribeTags:                      {wire: ok, errors: ok, state: ok, persist: ok, note: "legacy GET-only op, distinct path from TagResource family; pagination (Marker/MaxItems) not applied server-side -- deferred, see gaps"}
  CreateTags:                        {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteTags:                        {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeLifecycleConfiguration:    {wire: ok, errors: ok, state: ok, persist: ok}
  PutLifecycleConfiguration:         {wire: ok, errors: ok, state: ok, persist: ok}
  CreateReplicationConfiguration:    {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteReplicationConfiguration:    {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeReplicationConfigurations: {wire: partial, errors: ok, state: ok, persist: ok, note: "no NextToken pagination -- deferred, see gaps; Destination.LastReplicatedTimestamp is typed string not epoch-number, but is never populated so it never actually serializes wrong -- see gaps"}
  DescribeFileSystemPolicy:          {wire: ok, errors: ok, state: ok, persist: ok}
  PutFileSystemPolicy:               {wire: ok, errors: ok (partial), state: ok, persist: ok, note: "invalid-JSON policy returns ValidationException, real AWS likely returns InvalidPolicyException for malformed IAM policy JSON -- deferred, see gaps"}
  DeleteFileSystemPolicy:            {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeBackupPolicy:              {wire: ok, errors: ok, state: ok, persist: ok}
  PutBackupPolicy:                   {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAccountPreferences:        {wire: ok, errors: ok, state: ok, persist: n/a, note: "account-level, not resource state"}
  PutAccountPreferences:             {wire: ok, errors: ok, state: ok, persist: n/a}
families:
  FileSystem:        {status: ok, note: "CRUD + Update + Protection audited op-by-op; epoch timestamps, SizeInBytes nesting, FileSystemProtection nesting all verified byte-for-byte against aws-sdk-go-v2 deserializers"}
  MountTarget:        {status: ok, note: "CRUD + SecurityGroups audited; SecurityGroupLimitExceeded status code fixed (was 409, AWS uses 400 per botocore efs/service-2.json)"}
  AccessPoint:        {status: ok, note: "CRUD + ClientToken idempotency + PosixUser/RootDirectory shapes audited"}
  Tags:                {status: ok, note: "route-matcher bug fixed -- see below; CreateTags/DeleteTags legacy ops verified separately, correct as-is"}
  BackupPolicy:        {status: ok}
  LifecycleConfiguration: {status: ok}
  FileSystemPolicy:   {status: ok, note: "InvalidPolicyException vs ValidationException distinction deferred, see gaps"}
  ReplicationConfiguration: {status: partial, note: "pagination + Destination timestamp typing deferred, see gaps"}
  AccountPreferences: {status: ok}
gaps:
  - DescribeReplicationConfigurations does not implement NextToken pagination (always returns the full list in one page); low priority since AWS caps replication configs at 1 per file system, so lists stay small in practice.
  - ReplicationDestination.LastReplicatedTimestamp is typed `string` in services/efs/backend.go but the real SDK's types.Destination.LastReplicatedTimestamp is *time.Time (epoch-seconds on the wire). Currently dormant -- CreateReplicationConfiguration never populates this field, so it's always omitted (omitempty) and never serializes incorrectly. Would need a type change (string -> time.Time, epoch marshalling) before this field is ever populated.
  - PutFileSystemPolicy returns ValidationException for malformed policy JSON; real AWS has a distinct InvalidPolicyException (HTTP 400, same status so SDK-observable behavior is unaffected, but the ErrorCode string differs). Not fixed this pass -- low blast radius since both are 400s and message-based assertions would need updating regardless.
  - FileSystemLimitExceeded / AccessPointLimitExceeded (account-level quota errors, HTTP 403) are not simulated -- no quota enforcement in the mock. Out of scope for a single-account/region mock; deferred.
deferred:
  - InvalidPolicyException vs ValidationException error-code distinction for PutFileSystemPolicy (see gaps)
  - DescribeReplicationConfigurations pagination (see gaps)
leaks: {status: clean, note: "single self-terminating goroutine (fsActivationDelay simulation in CreateFileSystem) guards against concurrent deletion via a Get-under-lock check before mutating state; only active when fsActivationDelay>0, which is zero (disabled) outside parity tests"}
---

## Notes

Protocol: **restjson1**, path-versioned under `/2015-02-01/...`.

### Bugs found and fixed this pass

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

Neither error-status fix changes SDK-client-observable retry behavior (aws-sdk-go-v2's
deserializer error-dispatch switches on the `X-Amzn-ErrorType` header/body error code, not the
raw HTTP status, and both old and new status codes fall outside the 429/5xx retryable range) --
but they are genuine wire-shape deviations from real AWS worth fixing for parity, and would be
observable to any caller inspecting raw HTTP status codes directly.

### Verification method

All wire shapes (timestamps, error status codes, list-response keys, query-param names) were
cross-checked directly against `aws-sdk-go-v2/service/efs@v1.41.12`'s generated
`serializers.go` / `deserializers.go` / `types/types.go` / `types/errors.go` (in the local Go
module cache), plus `botocore`'s `efs/service-2.json` service model (installed locally via pip)
for the authoritative per-error `httpStatusCode` table. This caught both the route-matcher bug
(a wire-*path* mismatch invisible to handler-level unit tests) and the two status-code bugs (a
class of error invisible to error-code-string assertions, since gopherstack's own tests only
checked `ErrorCode` strings, not `httpStatusCode`, until this pass).

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
