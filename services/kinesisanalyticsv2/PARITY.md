---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: kinesisanalyticsv2
sdk_module: aws-sdk-go-v2/service/kinesisanalyticsv2@v1.36.22
last_audit_commit: 1c4ee34e
last_audit_date: 2026-07-23
overall: A            # every previously-documented gap either fixed or narrowed to a
                       # deliberately-scoped, explicitly-documented remainder
ops:
  CreateApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "inline ApplicationConfiguration/CloudWatchLoggingOptions were previously silently discarded (fixed pre-existing pass); ApplicationCodeConfiguration/FlinkApplicationConfiguration/EnvironmentProperties/ApplicationSnapshotConfiguration/ApplicationSystemRollbackConfiguration/ApplicationEncryptionConfiguration were accepted-but-not-modeled (this pass's gap) -- now seeded via SeedApplicationConfiguration's extended SeedConfig, still without bumping past version 1. ZeppelinApplicationConfiguration (Studio-notebook-only) remains accepted-but-ignored, see gaps."}
  DescribeApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "applicationDetailOutput previously omitted LastUpdateTimestamp/ConditionalToken/ApplicationVersionCreateTimestamp/ApplicationVersionRolledBackFrom/To/ApplicationVersionUpdatedFrom/ApplicationMaintenanceConfigurationDescription (all now populated); its VpcConfigurationDescriptions was WRONGLY placed at the top level of ApplicationDetail (real AWS has no such field -- it only exists nested inside ApplicationConfigurationDescription) -- this gopherstack-invented field placement is fixed (moved into appConfigDesc, matching real ApplicationConfigurationDescription.VpcConfigurationDescriptions)."}
  UpdateApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "ApplicationConfigurationUpdate (code/Flink/env-properties/snapshot/rollback/encryption/SQL-input-output-refdata/VPC sub-updates), CloudWatchLoggingOptionUpdates, RunConfigurationUpdate, RuntimeEnvironmentUpdate, and ConditionalToken were all accepted-but-ignored; all now implemented (applications.go/application_update_apply.go/handler_application_update.go). ConditionalToken is a deterministic sha256-derived function of (ApplicationARN, ApplicationVersionId) -- see conditionalToken/checkAndBumpVersionOrToken in store.go -- so it needs no extra persisted field and automatically rotates on every version bump. Sub-resource IDs referenced by CloudWatchLoggingOptionUpdates/SqlApplicationConfigurationUpdate/VpcConfigurationUpdates are validated to exist BEFORE the version is bumped (validateUpdateReferences), matching the Add*/Delete* config ops' existing 'find before bumping' convention -- a request naming an unknown ID leaves ApplicationVersionId untouched."}
  DeleteApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "CreateTimestamp request field is now validated against the application's actual CreateTimestamp (epoch-seconds float64 comparison with 1e-3/1ms tolerance, matching smithy-go's millisecond-precision unixTimestamp wire truncation); a mismatch returns InvalidArgumentException instead of silently deleting. DeleteApplication remains synchronous (see gaps, unchanged from prior audit)."}
  ListApplications: {wire: ok, errors: ok, state: ok, persist: ok}
  StartApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "RunConfiguration request field (ApplicationRestoreConfiguration/FlinkRunConfiguration) was never parsed at all -- now applied and echoed back via DescribeApplication's ApplicationConfigurationDescription.RunConfigurationDescription. SqlRunConfigurations remains accepted-but-ignored (see gaps: no per-input starting-position state modeled anywhere, same root cause as DiscoverInputSchema's synthetic limitation)."}
  StopApplication: {wire: partial, errors: ok, state: ok, persist: ok, note: "Force request field (skip the pre-stop snapshot) is not modeled: this backend never auto-snapshots on stop regardless of Force, and real AWS's auto-snapshot naming/visibility convention isn't documented publicly, so fabricating one was avoided as a gopherstack-invented-behavior risk -- see gaps."}
  RollbackApplication: {wire: ok, errors: ok, state: ok, persist: n/a, note: "now also sets ApplicationVersionRolledBackFrom/To (the version rolled back from/to) and ApplicationVersionUpdatedFrom on the resulting live Application, echoed via ApplicationDetail; these three lineage fields are cleared by every subsequent non-rollback version-bumping op (see bumpVersion in store.go) so they never linger as stale rollback markers."}
  DescribeApplicationOperation: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListApplicationOperations: {wire: ok, errors: ok, state: ok, persist: n/a}
  DescribeApplicationVersion: {wire: ok, errors: ok, state: ok, persist: n/a, note: "shares toDetailOutput with DescribeApplication/CreateApplication/UpdateApplication/RollbackApplication, so it picked up every wire-shape fix in this pass automatically."}
  ListApplicationVersions: {wire: ok, errors: ok, state: ok, persist: n/a}
  CreateApplicationSnapshot: {wire: ok, errors: ok, state: ok, persist: ok, note: "unchanged this pass; not re-diffed (files untouched since 782e2a93)."}
  DescribeApplicationSnapshot: {wire: ok, errors: ok, state: ok, persist: ok, note: "unchanged this pass; not re-diffed."}
  ListApplicationSnapshots: {wire: ok, errors: ok, state: ok, persist: ok, note: "unchanged this pass; not re-diffed."}
  DeleteApplicationSnapshot: {wire: ok, errors: ok, state: ok, persist: ok, note: "unchanged this pass; not re-diffed."}
  AddApplicationCloudWatchLoggingOption: {wire: ok, errors: ok, state: ok, persist: ok, note: "real AWS's AddApplicationCloudWatchLoggingOptionOutput carries an OperationId field (unlike most other Add*/Delete* config ops -- verified field-by-field against aws-sdk-go-v2's api_op_AddApplicationCloudWatchLoggingOption.go); gopherstack's response never had one. Fixed: now records an ApplicationOperation and returns OperationId."}
  AddApplicationInput: {wire: ok, errors: ok, state: ok, persist: ok, note: "verified AddApplicationInputOutput has no OperationId field in the real SDK -- correctly has none here."}
  AddApplicationInputProcessingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  AddApplicationOutput: {wire: ok, errors: ok, state: ok, persist: ok, note: "verified AddApplicationOutputOutput has no OperationId field in the real SDK -- correctly has none here."}
  AddApplicationReferenceDataSource: {wire: ok, errors: ok, state: ok, persist: ok, note: "verified AddApplicationReferenceDataSourceOutput has no OperationId field in the real SDK -- correctly has none here."}
  AddApplicationVpcConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "same OperationId gap/fix as AddApplicationCloudWatchLoggingOption -- verified against api_op_AddApplicationVpcConfiguration.go."}
  DeleteApplicationCloudWatchLoggingOption: {wire: ok, errors: ok, state: ok, persist: ok, note: "same OperationId gap/fix as AddApplicationCloudWatchLoggingOption -- verified against api_op_DeleteApplicationCloudWatchLoggingOption.go."}
  DeleteApplicationInputProcessingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "verified no OperationId field in the real SDK -- correctly has none here."}
  DeleteApplicationOutput: {wire: ok, errors: ok, state: ok, persist: ok, note: "verified no OperationId field in the real SDK -- correctly has none here."}
  DeleteApplicationReferenceDataSource: {wire: ok, errors: ok, state: ok, persist: ok, note: "verified no OperationId field in the real SDK -- correctly has none here."}
  DeleteApplicationVpcConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "same OperationId gap/fix as AddApplicationCloudWatchLoggingOption -- verified against api_op_DeleteApplicationVpcConfiguration.go."}
  CreateApplicationPresignedUrl: {wire: ok, errors: ok, state: ok, persist: n/a, note: "unchanged this pass."}
  UpdateApplicationMaintenanceConfiguration: {wire: partial, errors: ok, state: ok, persist: ok, note: "ApplicationMaintenanceWindowEndTime never computed/returned (unchanged gap, low value -- see gaps)."}
  DiscoverInputSchema: {wire: deferred, errors: ok, state: n/a, persist: n/a, note: "unchanged; always returns a fixed synthetic JSON/UTF-8 schema -- real AWS samples live stream data, which this emulator cannot do."}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "unchanged this pass."}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "unchanged this pass."}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "unchanged this pass."}
families:
  error_mapping: {status: ok, note: "unchanged this pass; ConcurrentModificationException mapping (fixed prior pass) also now covers ConditionalToken mismatches (checkAndBumpVersionOrToken returns the same ErrConcurrentModification sentinel as version mismatches)."}
gaps:
  - ZeppelinApplicationConfiguration/ZeppelinApplicationConfigurationUpdate (Managed Service for Apache Flink Studio notebooks: CatalogConfiguration/Glue Data Catalog, CustomArtifactConfiguration/Maven+S3 UDF JARs, DeployAsApplicationConfiguration) are accepted on the wire (to avoid rejecting well-formed requests) but not modeled -- out of scope for this pass given the size of the Flink/SQL core-path work already covered; Studio notebooks are a materially separate feature surface (INTERACTIVE ApplicationMode) from the streaming-application path this pass focused on. (bd: file follow-up)
  - StartApplication's SqlRunConfigurations (per-input InputStartingPositionConfiguration) and FlinkApplicationConfigurationDescription.JobPlanDescription (DescribeApplicationRequest.IncludeAdditionalDetails) are accepted-but-ignored: neither has any backing state anywhere in this emulator (no real stream position tracking, no real Flink job graph), the same root cause as DiscoverInputSchema's documented synthetic-schema limitation. Leniency only.
  - StopApplication's Force field (skip the pre-stop snapshot) is accepted but has no observable effect: this backend never auto-snapshots on stop regardless of Force. Real AWS's auto-snapshot naming/visibility isn't documented publicly enough to model without risking a gopherstack-invented behavior, so it was deliberately left unimplemented rather than fabricated.
  - UpdateApplicationMaintenanceConfiguration's ApplicationMaintenanceWindowEndTime is never computed/returned (pre-existing gap, unchanged, low value -- no client observably depends on the exact window end time).
  - DeleteApplication is synchronous (app removed immediately); real AWS transitions through a DELETING status first. ApplicationStatusDeleting const is defined but unused. Matches the synchronous-delete convention used elsewhere in this codebase; not fixed (pre-existing, unchanged).
  - Real AWS's default-assigned maintenance window (every application gets one automatically at creation, before any UpdateApplicationMaintenanceConfiguration call) is not modeled -- ApplicationMaintenanceConfigurationDescription is only populated in DescribeApplication once UpdateApplicationMaintenanceConfiguration has been called at least once. Pre-existing, unchanged; low value.
deferred:
  - DiscoverInputSchema (inherently synthetic without live stream sampling)
leaks: {status: clean, note: "New Application fields (CodeConfig/FlinkConfig/EnvironmentPropertyGroups/SnapshotsEnabled/RollbackEnabled/EncryptionConfig/RunConfig/version-lineage pointers) all live inside the Application struct itself, not a separate map -- DeleteApplication's existing applications.Delete(...) cleans them up with no new leak surface. The four Add*/Delete* config ops that now call recordOperation (AddApplicationCloudWatchLoggingOption/AddApplicationVpcConfiguration/DeleteApplicationCloudWatchLoggingOption/DeleteApplicationVpcConfiguration) write into the same b.operations[region][name] map DeleteApplication already clears -- verified via TestBackend_AddDeleteVpcAndCWLOption_ReturnOperationID plus the existing DeleteApplication cleanup tests, no new cleanup path needed. go test -race clean at -count=3."}
---

## Notes

Protocol: awsjson1.1 (X-Amz-Target: `KinesisAnalytics_20180523.<Op>`, single POST
endpoint). RouteMatcher/ExtractOperation unchanged this pass.

### Real bugs found and fixed this pass

1. **`UpdateApplication` silently dropped `ApplicationConfigurationUpdate`,
   `CloudWatchLoggingOptionUpdates`, `RunConfigurationUpdate`,
   `RuntimeEnvironmentUpdate`, and `ConditionalToken`** -- the single largest
   gap flagged by the prior audit. Every field is now threaded through:
   `applications.go`'s `UpdateApplication` takes a new `UpdateApplicationParams`
   struct; `application_update_apply.go` applies each sub-field to the live
   `*Application`; `handler_application_update.go` converts the awsjson1.1
   wire shapes. `ConditionalToken` implements the alternative optimistic-
   concurrency check real AWS documents ("use ConditionalToken instead of
   CurrentApplicationVersionId") via a deterministic
   `sha256(ApplicationARN + "#" + ApplicationVersionId)`-derived token (see
   `conditionalToken`/`checkAndBumpVersionOrToken` in `store.go`) that
   automatically rotates on every version bump without a separate persisted
   field.

2. **`applicationDetailOutput.VpcConfigurationDescriptions` was placed at the
   wrong nesting level -- a gopherstack-invented field that doesn't exist in
   real AWS's `ApplicationDetail`.** Real AWS's `ApplicationDetail` struct
   (verified field-by-field against aws-sdk-go-v2's `types.go`) has no
   top-level `VpcConfigurationDescriptions` at all; it only exists nested
   inside `ApplicationConfigurationDescription`. A real SDK client's
   deserializer would simply never populate this field from gopherstack's
   previous (wrong) top-level placement, silently losing VPC config on every
   `DescribeApplication`/`CreateApplication`/`UpdateApplication` response.
   Fixed by moving it into `appConfigDesc` (`handler_applications.go`),
   matching the real nesting exactly.

3. **`CreateApplication`/`UpdateApplication` never modeled
   `ApplicationCodeConfiguration`, `FlinkApplicationConfiguration`,
   `EnvironmentProperties`, `ApplicationSnapshotConfiguration`,
   `ApplicationSystemRollbackConfiguration`, or
   `ApplicationEncryptionConfiguration`** -- accepted on the wire but produced
   no backend state and were never echoed back, so any client (Terraform,
   CloudFormation) reading its own `CreateApplication`/`DescribeApplication`
   response back would see silent drift on every one of these fields. All six
   are now modeled (`models.go`'s `ApplicationCodeConfigDesc`/
   `FlinkApplicationConfigDesc`/etc.), seeded at create time
   (`SeedConfig`/`seedExtendedConfig` in `applications.go`) and updatable via
   `UpdateApplication`'s `ApplicationConfigurationUpdate`. `CheckpointConfiguration`'s
   documented `DEFAULT` behavior ("the application will use
   CheckpointingEnabled: true / CheckpointInterval: 60000 /
   MinPauseBetweenCheckpoints: 5000, even if set to other values") is
   enforced by `applyCheckpointDefaults` -- verbatim from the real API's
   `CheckpointConfiguration.ConfigurationType` documentation.
   `MonitoringConfiguration`/`ParallelismConfiguration` also accept a
   `DEFAULT` `ConfigurationType`, but AWS's public documentation does not
   specify literal forced values for those two the way it does for
   checkpointing, so gopherstack deliberately leaves them as provided rather
   than fabricating undocumented defaults.

4. **`StartApplication`'s `RunConfiguration` request field was never parsed
   at all.** Real clients commonly start a Flink application with
   `ApplicationRestoreConfiguration` set to restore from a snapshot; this was
   silently accepted and discarded. Fixed: `StartApplication` now takes a
   `*RunConfigInput` parameter, stored as `Application.RunConfig` and echoed
   via `ApplicationConfigurationDescription.RunConfigurationDescription`
   (shared with `UpdateApplication`'s `RunConfigurationUpdate`, since real
   AWS uses the identical `ApplicationRestoreConfiguration`/
   `FlinkRunConfiguration` shape for both).

5. **Four `Add*`/`Delete*` config ops were missing `OperationId` in their
   response** -- `AddApplicationCloudWatchLoggingOption`,
   `AddApplicationVpcConfiguration`,
   `DeleteApplicationCloudWatchLoggingOption`,
   `DeleteApplicationVpcConfiguration`. Verified field-by-field against
   aws-sdk-go-v2's `api_op_*.go`: these four (and only these four, among the
   `Add*`/`Delete*` config family) carry an `OperationId` field in real AWS's
   output shape -- an asymmetry in the real API, not a gopherstack oversight
   to "fix" toward consistency. All four backend methods now call
   `recordOperation` and return the ID.

6. **`DeleteApplication`'s `CreateTimestamp` safety check was parsed but
   never validated** -- real AWS uses it as a check that the caller has a
   fresh `DescribeApplication` view before deleting. Fixed: compares the
   request's epoch-seconds value against `awstime.Epoch(app.CreatedAt)`
   and returns `InvalidArgumentException` on mismatch instead of deleting.
   Tolerance is 1e-3 (1ms), not 1e-6: smithy-go's wire encoding for
   `unixTimestamp` (`time.FormatEpochSeconds`/`ParseEpochSeconds`) truncates
   to millisecond precision, while `app.CreatedAt` -- and the `CreateTimestamp`
   a real client reads back from a prior `CreateApplication`/`DescribeApplication`
   response -- carries full nanosecond precision. A real SDK client can only
   ever round-trip `CreateTimestamp` truncated to the millisecond, so a 1e-6
   tolerance rejected every legitimate delete (caught by
   `TestIntegration_KinesisAnalyticsV2_*`); regression test added at
   `TestBackend_DeleteApplication_CreateTimestamp/millisecond-truncated_timestamp_deletes`.

7. **`persistedApplication.MaintenanceWindowStartTime` was declared but never
   assigned or restored** -- a pre-existing field that predates this pass;
   `UpdateApplicationMaintenanceConfiguration` state silently didn't survive
   `Snapshot`/`Restore`. Fixed alongside the `kinesisanalyticsv2SnapshotVersion`
   bump to 2 (which also added persistence for every new `Application` field
   from items 1/3/4 above).

### Traps for the next auditor

- `ConditionalToken` is **computed, not stored** -- `conditionalToken(app)` in
  `store.go` derives it from `(ApplicationARN, ApplicationVersionId)`. Don't
  add a stored field for it; that would require keeping it in sync on every
  version bump for no benefit (the derivation already changes automatically).
- `validateUpdateReferences` (`application_update_apply.go`) MUST run and
  return before `checkAndBumpVersionOrToken` in `UpdateApplication` -- it
  checks every `CloudWatchLoggingOptionUpdates`/`SqlApplicationConfigurationUpdate`/
  `VpcConfigurationUpdates` referenced ID actually exists. If a future change
  moves a mutation before this check, a request naming an unknown ID will
  bump `ApplicationVersionId` and leave a phantom version-history entry
  before failing -- the same bug class the pre-existing Add*/Delete* config
  ops' "find before bump" comments already warn about.
- `bumpVersion` (`store.go`) is now the single place that sets
  `LastUpdateTimestamp`/`ApplicationVersionCreateTimestamp`/
  `ApplicationVersionUpdatedFrom` and clears
  `ApplicationVersionRolledBackFrom/To` on every version-bumping op except
  `RollbackApplication` (which sets the Rolled-Back fields itself since it
  doesn't go through `bumpVersion`). Don't reintroduce a second place that
  increments `ApplicationVersionID` directly (e.g. `app.ApplicationVersionID++`)
  without also calling/mirroring `bumpVersion` -- that would silently freeze
  these lineage fields.
- `CheckpointConfigDesc`'s `DEFAULT`-forcing (`applyCheckpointDefaults`) is
  intentionally NOT mirrored for `MonitoringConfigDesc`/`ParallelismConfigDesc`
  -- this is a verified asymmetry in real AWS's own public API documentation,
  not an inconsistency to "fix".
- `ApplicationOperation.OperationID`/`StartTimestamp`/`EndTimestamp` are wired
  from eight call sites now (Start/Stop/Update/RollbackApplication plus the
  four `OperationId`-bearing config ops from item 5 above) -- don't re-flag
  `b.operations` as unused.
- `operations` and `versions` remain intentionally NOT persisted
  (`persistence.go` only snapshots `applications`/`snapshots` tables) --
  predates this audit, matches pre-Phase-3.3 behavior; don't treat it as a
  newly-introduced gap.
- `kinesisanalyticsv2SnapshotVersion` is now 2 (was 1) -- a v1 on-disk
  snapshot is discarded (not partially decoded) on `Restore`, per the
  existing version-mismatch-discard convention. If you add more
  `persistedApplication` fields, bump to 3 and document why in the constant's
  doc comment, matching the existing pattern.
