---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: s3tables
sdk_module: aws-sdk-go-v2/service/s3tables@v1.14.3   # version audited against
last_audit_commit: 9eeab1c09                         # HEAD when this manifest was written
last_audit_date: 2026-07-24
overall: A            # replication family had a real (not just deferred) wire-shape bug; now fixed
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateTableBucket: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now applies encryptionConfiguration/storageClassConfiguration/tags from request body instead of discarding them"}
  GetTableBucket: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTableBuckets: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: continuationToken/maxBuckets/prefix/type were silently ignored; now paginates via pkgs/page and filters"}
  DeleteTableBucket: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascade to namespaces/tables/tags/replication/expiry verified correct"}
  PutTableBucketPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  GetTableBucketPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteTableBucketPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  PutTableBucketMaintenanceConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  GetTableBucketMaintenanceConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  PutTableBucketEncryption: {wire: ok, errors: ok, state: ok, persist: ok}
  GetTableBucketEncryption: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteTableBucketEncryption: {wire: ok, errors: ok, state: ok, persist: ok}
  PutTableBucketMetricsConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  GetTableBucketMetricsConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteTableBucketMetricsConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  PutTableBucketStorageClass: {wire: ok, errors: ok, state: ok, persist: ok}
  GetTableBucketStorageClass: {wire: ok, errors: ok, state: ok, persist: ok}
  PutTableBucketReplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: request/response were a fabricated {tableBucketARN, replicationConfiguration:{destinations}} shape with an invented destinationBucketARN field and no status/versionToken in the Put response (204 instead of required 200 body); now {configuration:{role,rules:[{destinations:[{destinationTableBucketARN}]}]}} with versionToken optimistic concurrency"}
  GetTableBucketReplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: see PutTableBucketReplication note -- Get had the same fabricated top-level shape"}
  DeleteTableBucketReplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now accepts+enforces the optional versionToken query param"}
  CreateNamespace: {wire: ok, errors: ok, state: ok, persist: ok}
  GetNamespace: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteNamespace: {wire: ok, errors: ok, state: ok, persist: ok}
  ListNamespaces: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: continuationToken/maxNamespaces/prefix were silently ignored; now paginates + filters"}
  CreateTable: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now applies encryptionConfiguration/storageClassConfiguration/tags from request body instead of discarding them"}
  GetTable: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: real GetTableInput accepts tableArn alone OR tableBucketARN+namespace+name; only the triple was honored before, so ARN-only callers always got 400"}
  DeleteTable: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTables: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: continuationToken/maxTables/prefix were silently ignored; now paginates + filters"}
  RenameTable: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateTableMetadataLocation: {wire: ok, errors: ok, state: ok, persist: ok}
  GetTableMetadataLocation: {wire: ok, errors: ok, state: ok, persist: ok}
  PutTableMaintenanceConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  GetTableMaintenanceConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  GetTableMaintenanceJobStatus: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: status map was always {} regardless of configured maintenance types; now one entry per configured type reporting the real JobStatus enum's Not_Yet_Run value (this backend runs no background jobs)"}
  GetTableEncryption: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: was hardcoded AES256 regardless of actual config; now reflects table override -> bucket default -> AES256 fallback"}
  GetTableStorageClass: {wire: ok, errors: ok, state: ok, persist: ok}
  PutTablePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  GetTablePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteTablePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  PutTableReplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same class of bug as PutTableBucketReplication -- request body was flat {destinations} with invented destinationBucketARN, response was empty (204) instead of the required {status,versionToken}; now real {role,rules:[{destinations:[{destinationTableBucketARN}]}]} + versionToken optimistic concurrency, backed by a new typed store.Table[TableReplicationConfig] replacing the old map[string]bool + map[string]map[string]any pair"}
  GetTableReplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: response was {configuration:<raw map>, versionToken:\"\"} (hardcoded empty token, invented destination field); now real {configuration:{role,rules},versionToken}"}
  DeleteTableReplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: versionToken is a required DeleteTableReplicationInput member on the real API but was previously ignored entirely (deletion always succeeded with no token, and no NotFound distinction for 'never configured'); now required + checked against the stored token (ConflictException on mismatch)"}
  GetTableReplicationStatus: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: destinations was always [] regardless of configured replication rules; now one ReplicationDestinationStatusModel entry per configured destination (replicationStatus: completed, since this backend performs no real cross-bucket replication and applies config synchronously)"}
  PutTableRecordExpirationConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: settings.days (retention period) was accepted on the wire but silently discarded -- TableRecordExpiryConfig had no field for it; status casing also normalized to the real lowercase enum (enabled/disabled, not ENABLED/DISABLED)"}
  GetTableRecordExpirationConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: response was a fabricated top-level {tableARN,status} shape; the real GetTableRecordExpirationConfigurationOutput has a single required configuration member ({status,settings:{days}}) and no tableARN field at all"}
  GetTableRecordExpirationJobStatus: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: hardcoded status \"SUCCEEDED\", which matches no value of the real TableRecordExpirationJobStatus enum (NotYetRun/Successful/Failed/Disabled); now NotYetRun when expiration is enabled, Disabled otherwise (this backend runs no background jobs)"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
# Families audited as a group (when per-op is impractical):
families:
  route-matcher: {status: ok, note: "verified every op's HTTP method + path prefix against aws-sdk-go-v2/service/s3tables@v1.14.3 serializers.go (49/49 ops); tableBucketARN path segments correctly URL-decoded as single segments via rawPathSegments (RawPath + url.PathUnescape per segment, not naive Split), so ARNs containing '/' and ':' route correctly"}
  timestamps: {status: ok, note: "createdAt/modifiedAt correctly use RFC3339 date-time strings (smithytime.ParseDateTime on the client side), NOT epoch-seconds -- restjson1 s3tables model uses date-time trait, unlike some other json services"}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - CreateTable's Metadata field (Iceberg schema at creation) is accepted by the real API but not parsed/stored by this emulator; no read path currently exposes table schema, so this was left deferred rather than half-wired (bd: TODO -- file if schema support becomes a priority)
  - Table bucket names and namespace/table names are not validated against AWS's real naming rules (bucket: 3-63 chars, lowercase+digits+hyphens, no leading/trailing hyphen, reserved prefix/suffix denylist; namespace/table: 1-255 chars, lowercase+digits+underscores ONLY -- no hyphens -- confirmed via https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-buckets-naming.html). Verified this IS a real gap (the aws-sdk-go-v2 client only validates required-ness client-side, so an invalid name reaches the server -- i.e. this emulator -- unrejected). NOT fixed this pass: the existing test corpus pervasively uses hyphenated namespace/table fixture names (e.g. "acme-ns", "test-ns") and t.Name()-derived bucket names containing underscores across ~10+ files outside this pass's scope; enforcing the real character sets would require a coordinated fixture rename across the whole service package, which is a separate, larger undertaking than the wire-shape/state fixes this pass targeted. Confirmed via a scoped experiment (implemented + immediately reverted) that this breaks TestHandler_Table_*, TestHandler_Namespace_CRUD, TestHandler_MaintenanceConfiguration, TestHandler_Encryption, and others. (bd: TODO -- file as a dedicated fixture-rename + validation pass)
leaks: {status: clean, note: "no goroutines/janitors in this service; all state lives in InMemoryBackend's store.Table/map fields guarded by lockmetrics.RWMutex, snapshotted via Handler.Snapshot/Restore delegation to InMemoryBackend"}
---

## Notes

Protocol: restjson1. Verified against `aws-sdk-go-v2/service/s3tables@v1.14.3`
`serializers.go`/`deserializers.go` directly (not against gopherstack's own output).

### Route matcher / ARN-in-path handling
`tableBucketARN` and `resourceArn` (tag ops) appear as path segments and contain
`/` and `:` (e.g. `arn:aws:s3tables:us-east-1:000000000000:bucket/my-bucket`).
`rawPathSegments` in handler.go correctly uses `r.URL.RawPath` (falling back to
`r.URL.Path`) and `url.PathUnescape`s each `/`-delimited raw segment
individually, so a URL-encoded ARN segment (client must percent-encode the `/`
as `%2F` per smithy's `httpLabel` binding) survives as one logical segment
instead of being split. All 49 SDK ops' HTTP method + path template were
cross-checked against `serializers.go` line-by-line; no method/path mismatches
found (this codebase already had this exactly right before this audit).

### GetTable's dual identification modes
Real `GetTableInput` has four *optional* fields: `Name`, `Namespace`,
`TableArn`, `TableBucketARN` — none marked required in the smithy model. A
caller may identify the table either by `tableArn` alone or by the
`tableBucketARN`+`namespace`+`name` triple. Before this audit, gopherstack
only accepted the triple and returned 400 `BadRequestException` for
`tableArn`-only requests — a real wire-shape bug, not a hypothetical: any SDK
caller passing `GetTableInput{TableArn: ...}` would break. Fixed by adding
`InMemoryBackend.GetTableByARN` and branching in `handleGetTable`.

### GetTableEncryption was a disguised no-op
`handleGetTableEncryption` unconditionally returned
`{"sseAlgorithm": "AES256"}` regardless of what `encryptionConfiguration` was
passed to `CreateTable`, and regardless of the owning bucket's own encryption
configuration. There is no `PutTableEncryption` operation in the real API
(encryption can only be set once, at `CreateTable` time, or inherited from the
bucket), so this was pure fabrication for any table created with an SSE-KMS
override. Fixed: `Table` now carries an `Encryption` field (mirroring
`TableBucket.Encryption`); `GetTableEncryption` on the backend resolves
table-override → bucket-default → AES256-default, matching AWS's documented
inheritance model.

### CreateTableBucket / CreateTable silently discarded encryptionConfiguration/storageClassConfiguration/tags
Both `CreateTableBucketInput` and `CreateTableInput` accept optional
`encryptionConfiguration`, `storageClassConfiguration`, and `tags` fields
alongside the required `name`(/`format`). gopherstack's request structs only
ever parsed `name`/`format`, so a bucket or table created with any of these
fields silently lost them — a subsequent `GetTableBucketEncryption` or
`GetTableBucketStorageClass` (etc.) would report the *unconfigured* default
even though the client explicitly configured it at creation. Fixed via
`CreateTableBucketOptions`/`CreateTableOptions` passed through from the
handler; tags are applied via the existing `TagResource` internally (same
lock already held at the correct point in the documented lock order
`muBuckets → muNamespaces → muTables → muState`, so no new lock ordering was
introduced).

### List* pagination was completely absent
`ListTableBuckets`, `ListNamespaces`, and `ListTables` all support
`continuationToken` + a max-results field (`maxBuckets`/`maxNamespaces`/
`maxTables`) + `prefix` on the wire (confirmed via `serializers.go`'s query
bindings), and their outputs include an optional `continuationToken` for the
next page (confirmed via `deserializers.go`). gopherstack ignored all of these
and always returned every matching resource in one page with no
`continuationToken` at all — a caller setting `MaxBuckets: 1` (a common
pattern to bound response size, or exercised by SDK paginators) got every
bucket back in a single unbounded page instead of the requested page size.
Fixed using `pkgs/page.New`/`page.ValidateToken` (matching the pattern already
used by `services/acm`), with a 1000-item default page size when unspecified.
`ListTableBuckets` additionally now respects the `type` filter (`aws` vs
`customer`); every bucket this backend creates is `customer`-typed, so a
`type=aws` filter now correctly returns an empty page instead of ignoring the
filter.

### Traps for the next auditor
- The extra `tableBucketARN` key gopherstack emits on `TableSummary`/
  `NamespaceSummary` list entries has no counterpart in the real
  `TableSummary`/`NamespaceSummary` smithy shapes (checked
  `deserializeDocumentTableSummary`/`NamespaceSummary` in `deserializers.go`
  directly) — this is **not** a bug: unknown JSON fields are silently ignored
  by the SDK's deserializer (`default: _, _ = key, value` in the generated
  switch), and dropping the field would only make debugging integration
  tests harder for no wire-compat benefit. Left as-is intentionally.
- `createdAt`/`modifiedAt` are formatted with the fixed layout
  `"2006-01-02T15:04:05.999Z"` throughout this package rather than
  `pkgs/awstime`. This is correct for s3tables specifically (RFC3339 string,
  not epoch), so do not "fix" it to `awstime.Epoch()` — that would break this
  service. Confirmed by reading `smithytime.ParseDateTime` call sites in
  `deserializers.go`.

### The entire replication family was a fabricated wire shape, not just missing versionToken enforcement
The prior audit's `deferred` note said only that versionToken optimistic
locking wasn't enforced. Re-diffing `PutTableBucketReplication`/
`GetTableBucketReplication`/`PutTableReplication`/`GetTableReplication`/
`DeleteTable(Bucket)Replication` against `serializers.go`/`deserializers.go`
found the shape itself was fabricated, not just missing a field:
- The real `TableBucketReplicationConfiguration`/`TableReplicationConfiguration`
  is `{role, rules: [{destinations: [{destinationTableBucketARN}]}]}` --
  gopherstack modeled a flat `{destinations: [{destinationBucketARN}]}` with
  no `role`/`rules` nesting at all, and `destinationBucketARN` is an
  invented field name (the real one is `destinationTableBucketARN`).
- `GetTableBucketReplicationOutput` is `{configuration, versionToken}` (both
  required) -- gopherstack returned `{tableBucketARN, replicationConfiguration:
  {destinations}}`, an entirely invented top-level shape with a hardcoded
  empty `versionToken` on the table-level `GetTableReplication` sibling.
- `PutTableBucketReplicationOutput`/`PutTableReplicationOutput` both require
  `{status, versionToken}` -- gopherstack returned an empty 204 body,
  silently dropping two required response members.
- `DeleteTableReplicationInput.VersionToken` is a *required* input member
  (delete-time optimistic-concurrency check) -- gopherstack ignored it
  entirely; a delete always succeeded regardless of token.
Fixed by replacing the ad-hoc `map[string]any` config storage with typed
`BucketReplicationConfig`/`TableReplicationConfig` (role + `[]ReplicationRule`
+ `VersionToken`), backed by a new `*store.Table[TableReplicationConfig]`
(mirroring the existing `bucketReplication`/`tableRecordExpiry` off-registry
DTO pattern in persistence.go) replacing the old `tableReplication
map[string]bool` + `tableReplicationConfigs map[string]map[string]any]` pair.
Bumped `s3tablesSnapshotVersion` 1 -> 2 since the persisted shape changed.

### GetTableRecordExpirationConfiguration had the same fabricated-top-level-shape bug, plus enum casing
`GetTableRecordExpirationConfigurationOutput` is `{configuration: {status,
settings: {days}}}` with no top-level `tableARN` field at all --
gopherstack returned `{tableARN, status}`. Also, `TableRecordExpirationStatus`
and `TableRecordExpirationJobStatus` are lowercase/mixed-case smithy enums
(`"enabled"`/`"disabled"`; `"NotYetRun"`/`"Successful"`/`"Failed"`/`"Disabled"`)
-- gopherstack used invented values (`"ENABLED"`/`"DISABLED"`, and a
hardcoded `"SUCCEEDED"` for job status that matches no real enum value at
all). Fixed: `TableRecordExpiryConfig` gained a `Days` field (previously
`settings.days` was accepted on the wire and silently discarded), the
default/normalized status is now the real lowercase wire value, and
`GetTableRecordExpirationJobStatus` reports `NotYetRun`/`Disabled` based on
whether expiration is configured+enabled (this backend runs no background
jobs, so nothing has ever "run").

### GetTableMaintenanceJobStatus's status map was always empty
`GetTableMaintenanceJobStatusOutput.Status` is a map keyed by maintenance
type (`icebergCompaction`/`icebergSnapshotManagement`), one entry per type
actually configured via `PutTableMaintenanceConfiguration` -- gopherstack
returned `{}` unconditionally regardless of configuration. Fixed to report
one entry per configured type with the real `JobStatus` enum's
`"Not_Yet_Run"` value (note the different casing convention from
`TableRecordExpirationJobStatus` above -- confirmed via `enums.go`, not
assumed).

### Investigated but NOT implemented: table bucket / namespace / table naming rules
Confirmed via AWS documentation (not guessed) that real S3 Tables enforces
naming rules server-side that the `aws-sdk-go-v2` client does NOT
pre-validate (its generated `validateOpCreateTableBucketInput` etc. only
check required-ness) -- so an invalid name really does reach this emulator
unrejected today, a genuine gap. Bucket names: 3-63 chars, lowercase+digits+
hyphens, no leading/trailing hyphen, reserved prefix/suffix denylist.
Namespace/table names: 1-255 chars, lowercase+digits+underscores ONLY (no
hyphens). Implementing bucket-name validation was attempted and immediately
reverted after it broke ~10 existing test files that build bucket names from
`t.Name()`-derived strings containing underscores; namespace/table
validation was not even attempted after confirming (via grep) that
hyphenated namespace fixtures like `"acme-ns"`/`"test-ns"` are pervasive
across the test corpus. Left as an explicit, documented gap rather than a
half-enforced or silently-skipped one -- see the `gaps` entry above.
