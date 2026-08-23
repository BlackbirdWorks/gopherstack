---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: kinesisanalyticsv2
sdk_module: aws-sdk-go-v2/service/kinesisanalyticsv2@v1.41.4
last_audit_commit: 3cec37291
last_audit_date: 2026-08-20
overall: A            # every previously-documented gap either fixed or narrowed to a
                       # deliberately-scoped, explicitly-documented remainder
ops:
  CreateApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "inline ApplicationConfiguration/CloudWatchLoggingOptions were previously silently discarded (fixed pre-existing pass); ApplicationCodeConfiguration/FlinkApplicationConfiguration/EnvironmentProperties/ApplicationSnapshotConfiguration/ApplicationSystemRollbackConfiguration/ApplicationEncryptionConfiguration/ZeppelinApplicationConfiguration were accepted-but-not-modeled (this and a prior pass's gap) -- now seeded via SeedApplicationConfiguration's extended SeedConfig, still without bumping past version 1. ZeppelinApplicationConfiguration (Studio notebook: MonitoringConfiguration/CatalogConfiguration+GlueDataCatalogConfiguration/DeployAsApplicationConfiguration+S3ContentBaseLocation/CustomArtifactsConfiguration+S3orMaven) is now fully typed and echoed via ZeppelinApplicationConfigurationDescription -- sized first (4-level-deep tree, one ArtifactType-discriminated union, ~9 leaf fields across 3 wire variants, no recursion), all shallow and typeable, no part left opaque. Referenced ARNs (GlueDataCatalogConfiguration.DatabaseARN, S3ContentLocation/S3ContentBaseLocation.BucketARN) are stored as plain strings with no cross-service existence check, matching this service's pre-existing convention for every other ARN field (ServiceExecutionRole, S3CodeLocationDesc.BucketARN, KinesisStreamsInputDesc.ResourceARN, etc.) -- this codebase has no cross-service backend-to-backend validation anywhere, so adding it only here would be a new, unprecedented architecture, not a fix. This pass also dropped an invented top-level Tags field from applicationDetailOutput (real ApplicationDetail, types/types.go:179, has no such member -- tags are only retrievable via the separate ListTagsForResource op); harmless to a typed client (unknown JSON keys are ignored) but a genuine shape deviation."}
  DescribeApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "applicationDetailOutput previously omitted LastUpdateTimestamp/ConditionalToken/ApplicationVersionCreateTimestamp/ApplicationVersionRolledBackFrom/To/ApplicationVersionUpdatedFrom/ApplicationMaintenanceConfigurationDescription (all now populated); its VpcConfigurationDescriptions was WRONGLY placed at the top level of ApplicationDetail (real AWS has no such field -- it only exists nested inside ApplicationConfigurationDescription) -- this gopherstack-invented field placement is fixed (moved into appConfigDesc, matching real ApplicationConfigurationDescription.VpcConfigurationDescriptions)."}
  UpdateApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "ApplicationConfigurationUpdate (code/Flink/env-properties/snapshot/rollback/encryption/SQL-input-output-refdata/VPC sub-updates), CloudWatchLoggingOptionUpdates, RunConfigurationUpdate, RuntimeEnvironmentUpdate, and ConditionalToken were all accepted-but-ignored; all now implemented (applications.go/application_update_apply.go/handler_application_update.go). ConditionalToken is a deterministic sha256-derived function of (ApplicationARN, ApplicationVersionId) -- see conditionalToken/checkAndBumpVersionOrToken in store.go -- so it needs no extra persisted field and automatically rotates on every version bump. Sub-resource IDs referenced by CloudWatchLoggingOptionUpdates/SqlApplicationConfigurationUpdate/VpcConfigurationUpdates are validated to exist BEFORE the version is bumped (validateUpdateReferences), matching the Add*/Delete* config ops' existing 'find before bumping' convention -- a request naming an unknown ID leaves ApplicationVersionId untouched. ZeppelinApplicationConfigurationUpdate (this pass's gap) was also accepted-but-ignored; now implemented (applyZeppelinConfigUpdate), merging onto any existing ZeppelinConfig the same way applyFlinkConfigUpdate does. CustomArtifactsConfigurationUpdate reuses the create-time item shape wholesale (verified: real AWS's botocore model has no separate per-item update shape). THIS PASS'S BUG: InputUpdate.InputSchemaUpdate/InputParallelismUpdate and ReferenceDataSourceUpdate.ReferenceSchemaUpdate (same root cause as AddApplicationInput/AddApplicationReferenceDataSource's gap) were accepted-but-ignored -- a code comment even said so explicitly ('InputSchemaUpdate/InputParallelismUpdate are not modeled anywhere in this backend...and are ignored if present on the wire') but this was never surfaced as a PARITY.md gap despite InputSchema being a REQUIRED member one level up. Fixed: InputSchemaUpdateDesc (types/types.go:1336 'InputSchemaUpdate' -- its own Update-suffixed shape, field names RecordFormatUpdate/RecordEncodingUpdate/RecordColumnUpdates, NOT SourceSchema reused) and InputParallelismUpdateDesc now apply in applyInputUpdate, regenerating InAppStreamNames when NamePrefixUpdate or InputParallelismUpdate lands. ReferenceDataSourceUpdate.ReferenceSchemaUpdate is the asymmetric case: real AWS types it plain *SourceSchema (types/types.go:2106), NOT a dedicated Update shape like InputSchemaUpdate -- verified and modeled as such (ReferenceDataSourceUpdate.ReferenceSchemaUpdate *SourceSchemaDesc, reusing the same type as the create/describe sides). Proven via TestUpdateApplication_InputSchemaUpdate_SDKRoundTrip and TestUpdateApplication_ReferenceSchemaUpdate_SDKRoundTrip."}
  DeleteApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "CreateTimestamp request field is now validated against the application's actual CreateTimestamp (epoch-seconds float64 comparison with 1e-3/1ms tolerance, matching smithy-go's millisecond-precision unixTimestamp wire truncation); a mismatch returns InvalidArgumentException instead of silently deleting. DeleteApplication remains synchronous (see gaps, unchanged from prior audit)."}
  ListApplications: {wire: ok, errors: ok, state: ok, persist: ok}
  StartApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "RunConfiguration request field (ApplicationRestoreConfiguration/FlinkRunConfiguration) was never parsed at all -- now applied and echoed back via DescribeApplication's ApplicationConfigurationDescription.RunConfigurationDescription. SqlRunConfigurations was accepted-but-ignored, and its InputId was never validated: this pass found it DOES have somewhere to land -- real AWS's InputDescription (not RunConfigurationDescription, which has no such field) carries a per-input InputStartingPositionConfiguration -- so it is now validated (unknown InputId -> ResourceNotFoundException, checked BEFORE ApplicationStatus is mutated to RUNNING) and stored/echoed on the matching InputDescription."}
  StopApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "Force request field was not even parsed (worse than accepted-but-ignored) -- now parsed and enforces real AWS's documented Flink-only restriction ('You can only force stop a Managed Service for Apache Flink application' -- api_op_StopApplication.go doc comment, aws-sdk-go-v2/service/kinesisanalyticsv2@v1.41.4): Force=true on a SQL-1_0 application now returns InvalidArgumentException. Force's state-broadening effect (permitting stop from STARTING/UPDATING/STOPPING/AUTOSCALING) has no observable effect here since this backend's ApplicationStatus is only ever READY/RUNNING (synchronous lifecycle, same structural gap as DeleteApplication's unused ApplicationStatusDeleting -- confirmed no other status is ever assigned). The pre-stop auto-snapshot itself remains unimplemented: confirmed via AWS's own 'Deep dive into the Amazon Managed Service for Apache Flink application lifecycle' blog post that the auto-snapshot's naming/visibility is still not documented publicly, so fabricating one continues to be avoided as a gopherstack-invented-behavior risk -- see gaps."}
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
  AddApplicationInput: {wire: ok, errors: ok, state: ok, persist: ok, note: "verified AddApplicationInputOutput has no OperationId field in the real SDK -- correctly has none here. THIS PASS'S BUG: real AWS's Input shape (types/types.go:1125) has InputSchema as a REQUIRED member and InputParallelism as optional -- gopherstack's inputConfig/InputDescription modeled neither, so a real client's InputSchema (the column/format mapping the operation exists to configure) was silently dropped and never echoed back by DescribeApplication, and InAppStreamNames (documented on Input.NamePrefix: '...creates one or more...in-application streams with the names MyInApplicationStream_001, MyInApplicationStream_002...') was never populated at all. Fixed: added SourceSchemaDesc/RecordFormatDesc/MappingParametersDesc/InputParallelismDesc to models.go, wired into inputConfig (request) and InputDescription (response), and added inAppStreamNames() to synthesize the documented '<NamePrefix>_NNN' names. Proven via TestAddApplicationInput_InputSchema_SDKRoundTrip (wire_sdk_roundtrip_test.go) and hand-revert (removing the two assignment lines reproduces 'InputSchema silently dropped by the real client's deserializer', confirmed then restored byte-identical)."}
  AddApplicationInputProcessingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  AddApplicationOutput: {wire: ok, errors: ok, state: ok, persist: ok, note: "verified AddApplicationOutputOutput has no OperationId field in the real SDK -- correctly has none here. Output/OutputDescription/OutputUpdate/DestinationSchema re-verified field-by-field against types/types.go:1782-1810,1839 this pass -- all fields present, DestinationSchema correctly flat (RecordFormatType only, no MappingParameters -- real AWS's DestinationSchema has none)."}
  AddApplicationReferenceDataSource: {wire: ok, errors: ok, state: ok, persist: ok, note: "verified AddApplicationReferenceDataSourceOutput has no OperationId field in the real SDK -- correctly has none here. THIS PASS'S BUG: real AWS's ReferenceDataSource shape (types/types.go:2048) has ReferenceSchema as a REQUIRED member -- gopherstack's refDataSourceConfig/ReferenceDataSourceDescription never modeled it, silently dropping it. Fixed: added ReferenceSchema *SourceSchemaDesc to both. Proven via TestAddApplicationReferenceDataSource_ReferenceSchema_SDKRoundTrip and hand-revert (same symptom class as AddApplicationInput's InputSchema)."}
  AddApplicationVpcConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "same OperationId gap/fix as AddApplicationCloudWatchLoggingOption -- verified against api_op_AddApplicationVpcConfiguration.go."}
  DeleteApplicationCloudWatchLoggingOption: {wire: ok, errors: ok, state: ok, persist: ok, note: "same OperationId gap/fix as AddApplicationCloudWatchLoggingOption -- verified against api_op_DeleteApplicationCloudWatchLoggingOption.go."}
  DeleteApplicationInputProcessingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "verified no OperationId field in the real SDK -- correctly has none here."}
  DeleteApplicationOutput: {wire: ok, errors: ok, state: ok, persist: ok, note: "verified no OperationId field in the real SDK -- correctly has none here."}
  DeleteApplicationReferenceDataSource: {wire: ok, errors: ok, state: ok, persist: ok, note: "verified no OperationId field in the real SDK -- correctly has none here."}
  DeleteApplicationVpcConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "same OperationId gap/fix as AddApplicationCloudWatchLoggingOption -- verified against api_op_DeleteApplicationVpcConfiguration.go."}
  CreateApplicationPresignedUrl: {wire: ok, errors: ok, state: ok, persist: n/a, note: "unchanged this pass."}
  UpdateApplicationMaintenanceConfiguration: {wire: partial, errors: ok, state: ok, persist: ok, note: "ApplicationMaintenanceWindowEndTime never computed/returned (unchanged gap, low value -- see gaps)."}
  DiscoverInputSchema: {wire: deferred, errors: ok, state: n/a, persist: n/a, note: "the synthetic JSON/UTF-8 placeholder schema itself is unchanged (real AWS samples live stream data, which this emulator cannot do -- confirmed this was NEVER made to error, contrary to this pass's starting premise: the synthetic response has existed since the op's introduction, commit 0d4fdada4). Fixed real wire bugs found while re-checking it: the request's ServiceExecutionRole (required by botocore's DiscoverInputSchemaRequest) was wired to the wrong key 'RoleARN' and never validated -- a real client's ServiceExecutionRole was silently dropped and an empty/absent one never rejected; InputStartingPositionConfiguration was a flat string instead of the real nested object; the response's InputSchema.SourceSchema was missing RecordColumns entirely (a required member -- the previous response couldn't even satisfy its own required fields, and RecordColumns is the field a real client actually needs to configure its application's input schema, the operation's whole purpose)."}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "unchanged this pass."}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "unchanged this pass."}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "unchanged this pass."}
families:
  error_mapping: {status: ok, note: "unchanged this pass; ConcurrentModificationException mapping (fixed prior pass) also now covers ConditionalToken mismatches (checkAndBumpVersionOrToken returns the same ErrConcurrentModification sentinel as version mismatches)."}
gaps:
  - FlinkApplicationConfigurationDescription.JobPlanDescription (DescribeApplicationRequest.IncludeAdditionalDetails) remains accepted-but-ignored: it is real AWS's Apache Flink job graph/scheduling plan (see the Apache Flink "Jobs and Scheduling" docs JobPlanDescription's own doc comment links to), which requires an actual Flink job compiler to produce -- structural, same class as DiscoverInputSchema's synthetic-schema limitation. Confirmed still genuinely unmodelable this pass; IncludeAdditionalDetails isn't even parsed by describeApplicationInput. Leniency only.
  - StopApplication's Force field now enforces the Flink-only restriction and is stored, but the pre-stop auto-snapshot itself is still not modeled: real AWS's auto-snapshot naming/visibility convention isn't documented publicly enough to fabricate (re-confirmed this pass via AWS's own "Deep dive into the Amazon Managed Service for Apache Flink application lifecycle" blog, which describes that a snapshot is taken but not how it's named or surfaced) -- deliberately left unimplemented rather than invented.
  - UpdateApplicationMaintenanceConfiguration's ApplicationMaintenanceWindowEndTime is never computed/returned (pre-existing gap, unchanged, low value -- no client observably depends on the exact window end time).
  - ZeppelinApplicationConfiguration's referenced ARNs (GlueDataCatalogConfiguration.DatabaseARN, S3ContentLocation/S3ContentBaseLocation.BucketARN) are not validated to exist in a Glue/S3 backend -- matches every other ARN field in this service (ServiceExecutionRole, KinesisStreamsInputDesc.ResourceARN, etc.), none of which are cross-service-validated; this codebase has no cross-service backend-to-backend validation mechanism anywhere. Not a Zeppelin-specific gap.
  - DeleteApplication is synchronous (app removed immediately); real AWS transitions through a DELETING status first. ApplicationStatusDeleting const is defined but unused. Matches the synchronous-delete convention used elsewhere in this codebase; not fixed (pre-existing, unchanged).
  - Real AWS's default-assigned maintenance window (every application gets one automatically at creation, before any UpdateApplicationMaintenanceConfiguration call) is not modeled -- ApplicationMaintenanceConfigurationDescription is only populated in DescribeApplication once UpdateApplicationMaintenanceConfiguration has been called at least once. Pre-existing, unchanged; low value.
deferred:
  - DiscoverInputSchema (inherently synthetic without live stream sampling)
leaks: {status: clean, note: "New Application fields (CodeConfig/FlinkConfig/EnvironmentPropertyGroups/SnapshotsEnabled/RollbackEnabled/EncryptionConfig/RunConfig/version-lineage pointers) all live inside the Application struct itself, not a separate map -- DeleteApplication's existing applications.Delete(...) cleans them up with no new leak surface. The four Add*/Delete* config ops that now call recordOperation (AddApplicationCloudWatchLoggingOption/AddApplicationVpcConfiguration/DeleteApplicationCloudWatchLoggingOption/DeleteApplicationVpcConfiguration) write into the same b.operations[region][name] map DeleteApplication already clears -- verified via TestBackend_AddDeleteVpcAndCWLOption_ReturnOperationID plus the existing DeleteApplication cleanup tests, no new cleanup path needed. go test -race clean at -count=3."}
---

## Notes

- 2026-08-22, gopherstack-r80d batch 31 (required-output-member audit):
  kinesisanalyticsv2 (6 required output fields / 33 ops, 6 ops-with-required
  per a fresh `cmd/requiredoutputfields` run, cross-checked against an
  independent standalone `go/ast` walk of `kinesisanalyticsv2@v1.41.4`'s
  `api_op_*.go` files -- both agreed exactly at 6). Module resolved directly:
  directory `kinesisanalyticsv2` == SDK module
  `aws-sdk-go-v2/service/kinesisanalyticsv2@v1.41.4` per `go.mod`, with no
  `dirModuleOverride` entry and no import of the sibling v1 module -- verified
  by grepping this service's own source for its SDK import, distinct from
  `services/kinesisanalytics`, which imports `aws-sdk-go-v2/service/kinesisanalytics`
  and was not touched by this batch.

  All 6 flagged ops -- `CreateApplication`, `DescribeApplication`,
  `DescribeApplicationSnapshot`, `ListApplications`, `RollbackApplication`,
  `UpdateApplication` -- are the "one wrapper key" shape this campaign has
  named repeatedly (pinpoint/bedrockagent precedent): 5 of 6 wrap
  `*types.ApplicationDetail` (types.go:179-252, 5 required members --
  ApplicationARN/ApplicationName/ApplicationStatus/ApplicationVersionId/
  RuntimeEnvironment), `ListApplications` wraps `[]types.ApplicationSummary`
  (types.go:439-471, the same 5 required members), `DescribeApplicationSnapshot`
  wraps `*types.SnapshotDetails` (types.go:2349-2376, 3 required --
  ApplicationVersionId/SnapshotName/SnapshotStatus). Unlike the tagged
  structs this campaign usually finds bugs in, gopherstack's own wire types
  (`applicationDetailOutput`, handler_applications.go:154-174;
  `applicationSummary`, models.go:413-420; `snapshotDetail`, models.go:435-441)
  tag every one of these members with no `omitempty`, so shape 1 of this
  campaign's bug class cannot occur syntactically here. Checked shape 2
  instead (never populated on some write path): `toDetailOutput`
  (handler_applications.go:715-747), `toSummary` (models.go:423-431), and
  `toSnapshotDetail` (models.go:444-451) all read straight from the backend's
  `Application`/`Snapshot` structs, whose ApplicationARN/ApplicationName/
  ApplicationStatus/RuntimeEnvironment/ApplicationVersionID are all set
  unconditionally in `CreateApplication` (applications.go:14-51) and never
  cleared afterward (`ApplicationStatus` only ever transitions
  Ready<->Running, applications.go:341,405, both real non-empty values);
  `RollbackApplication`/`UpdateApplication` share the same `toDetailOutput`
  call, so they inherit the same guarantee. Result: 0 bugs. No code changes.

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

### Follow-up pass (gopherstack-uci4, 2026-08-11)

Re-examined the four gaps this pass's predecessor deferred. Two premises did
not hold up:

- `StopApplication`'s `Force` field wasn't merely accepted-and-ignored, it
  wasn't parsed at all -- `startStopApplicationInput` had no `Force` field.
  Now parsed and enforces real AWS's Flink-only force-stop restriction
  (SQL-1_0 + Force=true -> `InvalidArgumentException`); the auto-snapshot
  itself is still deliberately unfabricated (naming/visibility genuinely
  undocumented, re-verified via AWS's own lifecycle blog post).
- `DiscoverInputSchema` was never made to error -- it has returned the same
  synthetic placeholder since its introduction (`0d4fdada4`). What it *did*
  have were real wire bugs: `ServiceExecutionRole` (required) was wired to
  a wrong key (`RoleARN`, never validated), `InputStartingPositionConfiguration`
  was a flat string instead of a nested object, and the response's
  `RecordColumns` (required) was omitted entirely. Fixed the wire; left the
  synthetic placeholder itself alone.

`StartApplication`'s `SqlRunConfigurations` turned out to have somewhere
real to land: `InputDescription.InputStartingPositionConfiguration` (not
`RunConfigurationDescription`, which real AWS has no such field on). Now
validated (unknown `InputId` -> `ResourceNotFoundException`, checked before
`ApplicationStatus` flips to `RUNNING`) and echoed. `JobPlanDescription`
was confirmed genuinely structural (a real Flink job graph) and left alone.

`ZeppelinApplicationConfiguration` was sized before typing: 4 levels deep
(config -> sub-config -> nested struct -> scalar leaf), one
`ArtifactType`-discriminated union (`CustomArtifactConfiguration`'s
S3-vs-Maven choice), ~9 leaf fields total across the create/describe/update
variants, no recursion. Fully typeable -- nothing left opaque. Referenced
ARNs (`DatabaseARN`, `BucketARN`) are plain strings with no cross-service
existence check, matching every other ARN field in this service (this
codebase has no cross-service backend validation anywhere, so adding it only
for Zeppelin would be new architecture, not a fix).

`Application.ZeppelinConfig` and `InputDescription.InputStartingPositionConfiguration`
are additive (`omitempty`); `kinesisanalyticsv2SnapshotVersion` was **not**
bumped -- both are wired into `persistedApplication`/`toPersistedApp`/
`fromPersistedApp` and round-trip-tested
(`TestPersistence_ZeppelinConfigSurvivesRoundTrip`).

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

### Follow-up pass (2026-08-20)

Wrapper-key/nested-shape sweep of every documented `*Configuration`/
`*ConfigurationDescription`/`*ConfigurationUpdate` triple in this service.
Found and fixed one real, previously-undocumented bug family: real AWS's
`Input` shape (`types/types.go:1125`) has `InputSchema` as a **required**
member (the column/format mapping the whole operation exists to configure)
and `InputParallelism` as optional, and `ReferenceDataSource`
(`types/types.go:2048`) has `ReferenceSchema` as required -- none of the
three were modeled anywhere in this backend. `application_config_update.go`
even had a code comment acknowledging `InputSchemaUpdate`/
`InputParallelismUpdate` were "not modeled...and are ignored if present on
the wire", but this was never surfaced in PARITY.md's `gaps` list despite
being a REQUIRED member, so the service's prior "A grade" didn't account for
it. `InAppStreamNames` (`InputDescription`, documented on `Input.NamePrefix`'s
own doc comment: "...creates one or more...in-application streams with the
names MyInApplicationStream_001, MyInApplicationStream_002...") was likewise
never populated.

Fixed across all three directions:
- `Input`/`InputDescription`: added `InputSchema`/`InputParallelism` to
  `inputConfig` (request) and `InputDescription` (response), plus
  `inAppStreamNames()` to synthesize the documented `<NamePrefix>_NNN` names.
- `InputUpdate`: added `InputSchemaUpdate`/`InputParallelismUpdate`, applied
  in `applyInputUpdate` (`application_update_apply.go`), regenerating
  `InAppStreamNames` when either lands.
- `ReferenceDataSource`/`ReferenceDataSourceDescription`/
  `ReferenceDataSourceUpdate`: added `ReferenceSchema`/`ReferenceSchemaUpdate`.
  Confirmed a genuine wire asymmetry while modeling this: real AWS's
  `InputUpdate.InputSchemaUpdate` is its own dedicated shape
  (`InputSchemaUpdate`, `RecordFormatUpdate`/`RecordEncodingUpdate`/
  `RecordColumnUpdates` -- Update-suffixed field names), but
  `ReferenceDataSourceUpdate.ReferenceSchemaUpdate` is typed plain
  `*SourceSchema` (`types/types.go:2106`), reusing the create/describe shape
  verbatim with NO renaming. Modeled distinctly (`InputSchemaUpdateDesc` vs.
  reusing `SourceSchemaDesc` for the reference-data side) rather than
  assuming symmetry.

All five new types (`SourceSchemaDesc`, `RecordFormatDesc`,
`MappingParametersDesc`, `CSVMappingParametersDesc`/`JSONMappingParametersDesc`,
`InputParallelismDesc`, `InputSchemaUpdateDesc`, `InputParallelismUpdateDesc`)
are additive (`omitempty`) fields on `InputDescription`/
`ReferenceDataSourceDescription`, which `persistedApplication` embeds
directly -- no new persistence wiring needed, `kinesisanalyticsv2SnapshotVersion`
correctly left at 2 (same additive convention as the prior Zeppelin/
`InputStartingPositionConfiguration` pass). Proven with four real-SDK
round-trip tests (`wire_sdk_roundtrip_test.go`, new file, driven through
`pkgs/service`'s router exactly like `services/emrserverless/wire_sdk_roundtrip_test.go`)
and one hand-revert (removing `buildInputDescription`'s two new assignment
lines reproduces "InputSchema silently dropped by the real client's
deserializer" exactly, then restored byte-identical).

Also dropped one harmless-but-invented member found incidentally while
diffing `ApplicationDetail` field-by-field: `applicationDetailOutput` carried
a top-level `Tags` field that real `ApplicationDetail` (`types/types.go:179`)
does not have (tags are retrieved only via the separate
`ListTagsForResource` op). A typed client ignores unknown JSON keys so this
was never observably wrong, but it was a real shape deviation; removed for
fidelity.

Every other documented triple (`ApplicationConfiguration`/`SqlApplicationConfiguration`/
`FlinkApplicationConfiguration`/`ApplicationCodeConfiguration`/`CodeContent`/
`S3ContentLocation`/`CheckpointConfiguration`/`MonitoringConfiguration`/
`ParallelismConfiguration`/`EnvironmentProperties`/`ApplicationSnapshotConfiguration`/
`ApplicationSystemRollbackConfiguration`/`ApplicationEncryptionConfiguration`/
`VpcConfiguration`/`ZeppelinApplicationConfiguration`+its four sub-configs/
`Output`/`DestinationSchema`) was independently re-verified field-by-field
against this pass's own reading of `types/types.go` (not re-trusted from the
prior audit's notes) and found to match, including the previously-flagged
landmine (`EnvironmentProperties.PropertyGroups` vs.
`EnvironmentPropertyDescriptions.PropertyGroupDescriptions` vs.
`EnvironmentPropertyUpdates.PropertyGroups` -- the third one reuses the
create-side name, unrenamed, and gopherstack has this right). Enum values
emitted by non-test code (`DEFAULT`, `DELETING`, `JSON`, `READY`, `RUNNING`,
`SUCCESSFUL`) were grepped and cross-checked against `types/enums.go` --
all real, no fabricated constants.
