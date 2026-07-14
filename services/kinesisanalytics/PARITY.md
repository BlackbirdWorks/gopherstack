---
service: kinesisanalytics
sdk_module: aws-sdk-go-v2/service/kinesisanalytics@v1.30.21
last_audit_commit: d6bfd3a1
last_audit_date: 2026-07-13
overall: A            # real fixes found: wire-shape bugs across the UpdateApplication nested
                       # payloads, S3ReferenceDataSource field-name swap, tag-limit modeling,
                       # non-modeled error codes, missing required-field validation
ops:
  CreateApplication: {wire: ok, errors: fixed, state: fixed, persist: ok, note: "fixed: tags were never validated (no key/value checks, no cap enforcement); tag-limit error was LimitExceededException instead of modeled TooManyTagsException"}
  DeleteApplication: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeApplication: {wire: ok, errors: ok, state: ok, persist: ok}
  ListApplications: {wire: ok, errors: ok, state: ok, persist: ok, note: "HasMoreApplications/ExclusiveStartApplicationName pagination already correct, no NextToken"}
  StartApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "READY->STARTING->RUNNING transition via launchTransition goroutine, already correct"}
  StopApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "RUNNING->STOPPING->READY transition, already correct"}
  UpdateApplication: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "InputUpdates/OutputUpdates/ReferenceDataSourceUpdates nested payloads used the wrong wire shape end to end -- see Notes. InputParallelismUpdate was entirely unimplemented (input parallelism could not be changed via UpdateApplication at all)."}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: fixed, state: ok, persist: ok, note: "tag-limit error was LimitExceededException instead of modeled TooManyTagsException; cap was 200 instead of the real 50 user-defined-tag limit"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  AddApplicationCloudWatchLoggingOption: {wire: ok, errors: fixed, state: ok, persist: ok, note: "cap-exceeded error switched from non-modeled LimitExceededException to modeled InvalidArgumentException"}
  AddApplicationInput: {wire: ok, errors: fixed, state: fixed, persist: ok, note: "NamePrefix (required member) was never validated; cap-exceeded error switched LimitExceededException -> InvalidArgumentException"}
  AddApplicationInputProcessingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  AddApplicationOutput: {wire: fixed, errors: fixed, state: fixed, persist: ok, note: "Output.Name (required member) was never validated; cap-exceeded error switched LimitExceededException -> InvalidArgumentException"}
  AddApplicationReferenceDataSource: {wire: fixed, errors: fixed, state: ok, persist: ok, note: "S3ReferenceDataSource.RoleARN wire field was wrong -- real field is ReferenceRoleARN; cap-exceeded error switched LimitExceededException -> InvalidArgumentException"}
  DeleteApplicationCloudWatchLoggingOption: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteApplicationInputProcessingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteApplicationOutput: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteApplicationReferenceDataSource: {wire: ok, errors: ok, state: ok, persist: ok}
  DiscoverInputSchema: {wire: ok, errors: ok, state: deferred, persist: n/a, note: "intentional fixed-shape stub -- see gaps. Also fixed an inert wire-shape bug in the unused S3Configuration sub-field (ReferenceRoleARN -> RoleARN) while auditing this op"}
families:
  updateNestedPayloads: {status: fixed, note: "InputUpdate/OutputUpdate/ReferenceDataSourceUpdate's Kinesis*/Lambda/S3/InputProcessingConfiguration/InputSchema/InputParallelism sub-objects all carry AWS-suffixed field names (ResourceARNUpdate, RoleARNUpdate, BucketARNUpdate, FileKeyUpdate, ReferenceRoleARNUpdate, RecordColumnUpdates, RecordEncodingUpdate, RecordFormatUpdate, CountUpdate) distinct from their Add* counterparts -- gopherstack reused the Add* Go types (unsuffixed field names) for all of them, so every real aws-sdk-go-v2 client request updating an existing input/output/reference-data-source's Kinesis/Firehose/Lambda/S3 config, schema, or parallelism silently failed to decode (fields landed as zero values) and then overwrote the stored sub-object with those zero values -- UpdateApplication looked like it succeeded (200 OK, version bumped) but corrupted the target field to empty strings instead of applying the caller's update. Fixed by giving each Update-suffixed shape its own Go type with the correct JSON tags."}
gaps:
  - "DiscoverInputSchema returns a fixed canned schema/sample records regardless of input; real schema inference from a live Kinesis/Firehose stream or S3 object requires an actual sampling+type-inference engine this emulator does not have. Documented as an intentional stub in the handler (handleDiscoverInputSchema doc comment); response shape matches the real wire format so SDK consumers parse it without error. (bd: TBD)"
  - "Application/applicationDetail carry ServiceExecutionRole and RuntimeEnvironment fields that DO NOT EXIST anywhere in the real aws-sdk-go-v2/service/kinesisanalytics@v1.30.21 model (verified: grep for both identifiers across the whole SDK package returns zero hits outside unrelated client-config internals). These are additive/extra JSON keys in our responses; real SDK clients silently ignore unknown fields, so this doesn't break wire compatibility, but it is not a real field either -- ServiceExecutionRole is entirely unreachable from a real client (CreateApplicationInput has no such member) and RuntimeEnvironment is hardcoded to \"SQL-1_0\" for display only. Left as-is this sweep: removing them would require an Application persistence-shape/version bump and touches ~10 tests for zero real-client-facing benefit (extra fields are harmless). Worth a dedicated cleanup pass. (bd: TBD)"
  - "statusAutoScaling/statusForceStopping/statusMaintenance/statusRollingBack/statusRolledBack constants in backend.go are marked //nolint:deadcode \"AWS status constant\" but are NOT part of the real v1 ApplicationStatus enum (only DELETING/STARTING/STOPPING/READY/RUNNING/UPDATING exist per types/enums.go) -- they appear to be copied from the v2 (kinesisanalyticsv2) ApplicationStatus enum, which has more values. Unused dead code today so no functional bug, but the doc comment is misleading; a future pass should either delete them or correct the comment. (bd: TBD)"
  - "inputUpdate.InputStartingPositionConfiguration is accepted server-side but does not exist on the real InputUpdate shape (real InputUpdate only has InputId/InputParallelismUpdate/InputProcessingConfigurationUpdate/InputSchemaUpdate/Kinesis*InputUpdate/NamePrefixUpdate). Harmless surplus -- no real client ever sends it since the field isn't in their SDK type -- but noted so a future audit doesn't assume it's reachable in practice. (bd: TBD)"
deferred: []
leaks: {status: clean, note: "launchTransition/DeleteApplication background goroutines are bounded by b.svcCtx (NewInMemoryBackendWithContext) and tracked in b.cancelFuncs, canceled on Reset(); no per-request or unbounded goroutines introduced this sweep"}
---

## Notes

Protocol: **awsjson1.1**, single POST endpoint, `X-Amz-Target: KinesisAnalytics_20150814.<Op>`
dispatch (verified against handler.go's `kinesisanalyticsTargetPrefix` -- correctly uses the
older 20150814 date, not v2's 20180523). Timestamps (`CreateTimestamp`/`LastUpdateTimestamp`) are
epoch-seconds `float64` with sub-second precision, verified against
`aws-sdk-go-v2/service/kinesisanalytics` deserializers.go's `smithytime.ParseEpochSeconds` --
already correct.

### Real bugs fixed this sweep

1. **UpdateApplication's nested Input/Output/ReferenceDataSource Update payloads used the wrong
   wire shape entirely** (services/kinesisanalytics/models.go, backend.go). This is the
   highest-impact fix: every "*Update" sub-object nested under `ApplicationUpdate.InputUpdates[]`,
   `.OutputUpdates[]`, and `.ReferenceDataSourceUpdates[]` in the real API carries an "Update"
   suffix on every leaf field (`ResourceARNUpdate`/`RoleARNUpdate` instead of
   `ResourceARN`/`RoleARN`, `BucketARNUpdate`/`FileKeyUpdate`/`ReferenceRoleARNUpdate` instead of
   `BucketARN`/`FileKey`/`RoleARN`, `RecordColumnUpdates`/`RecordEncodingUpdate`/
   `RecordFormatUpdate` instead of `RecordColumns`/`RecordEncoding`/`RecordFormat`,
   `CountUpdate` instead of `Count`) -- verified against
   `aws-sdk-go-v2/service/kinesisanalytics/serializers.go`'s
   `awsAwsjson11_serializeDocumentKinesisStreamsInputUpdate`/`...OutputUpdate`/
   `...S3ReferenceDataSourceUpdate`/`...InputSchemaUpdate`/`...InputLambdaProcessorUpdate`/
   `...InputParallelismUpdate` functions. gopherstack instead reused the *Add* request Go types
   (unsuffixed field names) for these Update payloads. Consequence: a real aws-sdk-go-v2 client
   calling `UpdateApplication` to change an existing input's Kinesis/Firehose source, an output's
   Kinesis/Firehose/Lambda destination, a reference data source's S3 location, an input's schema,
   or an input's parallelism count would have every field silently decode as a zero value (wrong
   JSON key), and the handler would then overwrite the stored sub-object with that zero value --
   the response was `200 OK` with the version bumped, but the target field was corrupted to empty
   strings instead of updated. This is a disguised-no-op-plus-corruption bug: it looked like
   success from the client's perspective (no error) but silently discarded the caller's real
   intent while also destroying the previous value. `InputParallelismUpdate` was not modeled at
   all, meaning input parallelism could never be changed via `UpdateApplication` under any
   payload. Fixed by giving each Update-suffixed nested shape its own Go type
   (`kinesisStreamsInputUpdateConfig`, `kinesisFirehoseInputUpdateConfig`,
   `kinesisStreamsOutputUpdateConfig`, `kinesisFirehoseOutputUpdateConfig`,
   `lambdaOutputUpdateConfig`, `s3ReferenceDataSourceUpdateConfig`,
   `inputProcessingConfigUpdateInput`/`lambdaProcessorUpdateInput`, `inputSchemaUpdateInput`,
   `inputParallelismUpdateConfig`) with the correct "Update"-suffixed JSON tags, adding
   `InputParallelismUpdate` support end to end, and implementing `InputSchemaUpdate` as a
   field-by-field partial patch (matching its distinct "Update"-suffixed shape) rather than a
   whole-object replace -- unlike `ReferenceSchemaUpdate`, which genuinely does reuse the full
   `SourceSchema` shape verbatim (confirmed via `types.ReferenceDataSourceUpdate.ReferenceSchemaUpdate
   *SourceSchema`) and so correctly remains a whole-object replace.
   Covered by `TestHandler_UpdateApplication_NestedWireShapes` and
   `TestHandler_UpdateApplication_InputSchemaUpdateIsPartialPatch` (handler_test.go), which
   round-trip real AWS-shaped JSON through the handler and assert the backend state actually
   changed.

2. **S3ReferenceDataSource's IAM role field used the wrong wire name** (models.go, handler.go,
   backend.go). Every other role-ARN-bearing shape in this API uses `RoleARN`, but
   `S3ReferenceDataSource`/`S3ReferenceDataSourceDescription` uniquely uses `ReferenceRoleARN` --
   verified against `aws-sdk-go-v2/service/kinesisanalytics/types.go` and
   `deserializers.go`/`serializers.go`'s `case "ReferenceRoleARN":` handling. gopherstack used
   `RoleARN` for both the `AddApplicationReferenceDataSource` request and the
   `DescribeApplication` response, meaning a real client's `ReferenceRoleARN` value was silently
   dropped on the way in (decoded as empty) and never populated on the way out (client's
   `S3ReferenceDataSourceDescription.ReferenceRoleARN` field always came back nil). Fixed by
   renaming the field on `S3ReferenceDataSourceDesc` (response) and `s3ReferenceDataSourceConfig`
   (Add request) to `ReferenceRoleARN`, and adding the correctly-suffixed
   `s3ReferenceDataSourceUpdateConfig` for the Update payload (see bug 1). While auditing this
   area, also fixed the *unused* `S3Configuration` sub-field on `DiscoverInputSchema` (which
   correctly uses plain `RoleARN`, not `ReferenceRoleARN` -- the two shapes had been swapped
   relative to each other in the original code, even though `DiscoverInputSchema` is a stub that
   never reads this field today).

3. **CreateApplication never validated tags at all** (backend.go `CreateApplication`). Every
   other tag-mutating op (`TagResource`) ran incoming tags through `validateAndMergeTags`
   (key/value length, `aws:`-prefix rejection, per-resource cap), but `CreateApplication` copied
   `Tags` straight into the new `Application` with no validation whatsoever -- a `CreateApplication`
   call with an invalid tag key or an unbounded tag count silently succeeded, produced a resource
   real AWS would have rejected with `CodeValidationException`/`TooManyTagsException`. Fixed by
   calling `validateAndMergeTags(nil, tags)` before creating the application, matching
   `CreateApplicationInput`'s modeled `TooManyTagsException`/`InvalidArgumentException` error
   surface (verified via `aws-sdk-go-v2/service/kinesisanalytics/deserializers.go`'s
   `awsAwsjson11_deserializeOpErrorCreateApplication`).

4. **Tag-limit-exceeded used the wrong error code, and the wrong limit** (backend.go, handler.go).
   `maxTagsPerResource` was 200 (the generic default many other services use); the real KDA v1
   limit is 50 user-defined tags (AWS docs, and mirrored in the `TagResource`/`CreateApplication`
   doc comments in the SDK source: "The maximum number of user-defined application tags is 50").
   Separately, exceeding the cap returned `LimitExceededException`, but AWS models this case as a
   dedicated `TooManyTagsException` on both `CreateApplication` and `TagResource` (confirmed via
   the per-operation modeled error lists in deserializers.go -- `TooManyTagsException` appears only
   on `CreateApplication`/`TagResource`/`UntagResource`, `LimitExceededException` only on
   `CreateApplication` for the *application-count* cap). Fixed: `maxTagsPerResource` is now 50, and
   a new `ErrTooManyTags` sentinel maps to the `TooManyTagsException` code, checked ahead of the
   generic `awserr.ErrConflict` case in `handler.go`'s error switch so it isn't shadowed by the
   `LimitExceededException` mapping.

5. **Add-time per-application-resource caps (Input/Output/ReferenceDataSource/
   CloudWatchLoggingOption) used a non-modeled error code** (backend.go). All four
   `AddApplication*` ops returned `LimitExceededException` when the resource's hard cap (1 input,
   3 outputs, 1 reference data source, 50 CloudWatch logging options) was reached, but none of
   these four operations model `LimitExceededException` in their AWS API definition -- their
   modeled error set is `{ConcurrentModificationException, InvalidArgumentException,
   ResourceInUseException, ResourceNotFoundException, UnsupportedOperationException}` (verified
   via deserializers.go's per-op `awsAwsjson11_deserializeOpError*` functions).
   `LimitExceededException` is reserved for the *application-count* cap on `CreateApplication`.
   These are hard architectural caps (SQL apps support exactly one input, etc.), not adjustable
   service quotas, so `InvalidArgumentException` is the correct modeled fit. Fixed all four to use
   `ErrValidation` (InvalidArgumentException) instead of `ErrLimitExceeded`.

6. **`Input.NamePrefix` and `Output.Name` (both required members in the real API) were never
   validated** (backend.go `convertInputConfig`/`convertOutputConfig`). A request omitting either
   field was silently accepted, producing an input with an empty `NamePrefix` (which also breaks
   `InAppStreamNames` derivation -- `inAppStreamNames` returns nil for an empty prefix) or an
   output with an empty `Name`, neither of which real AWS would ever allow (both are
   `smithy.NewErrParamRequired` in the SDK's client-side validators, and the server enforces the
   same server-side). Fixed by rejecting empty `NamePrefix`/`Name` with `InvalidArgumentException`.

### Verified clean (no bug, but worth recording so the next audit doesn't re-flag)

- **Route matcher / target prefix**: `KinesisAnalytics_20150814.` -- correctly uses the v1 date,
  distinct from kinesisanalyticsv2's `KinesisAnalyticsV2_20180523.`. `ExtractOperation` correctly
  falls back to `"Unknown"` when the header is absent or doesn't match, so the `dispatch` map
  lookup fails closed rather than routing garbage.
- **Persistence**: `Handler.Snapshot`/`Restore` (persistence.go) delegate to
  `InMemoryBackend.Snapshot`/`Restore`, which version-gate (`kinesisanalyticsSnapshotVersion`) and
  go through `store.Registry.SnapshotAll`/`RestoreAll` for the `apps` table -- already correctly
  wired, confirmed via `TestInMemoryBackend_FullStateSnapshotRestoreRoundTrip`
  (persistence_test.go), which round-trips every sub-resource kind across two regions.
  `CloudWatchLoggingOptionUpdate`'s wire shape (`CloudWatchLoggingOptionId`,
  `LogStreamARNUpdate`, `RoleARNUpdate`) was already correct -- verified against
  `types.CloudWatchLoggingOptionUpdate` and `serializers.go`'s
  `awsAwsjson11_serializeDocumentCloudWatchLoggingOptionUpdate`; this was the one Update-suffixed
  nested shape that did NOT have the bug described in fix #1 above.
- **Lifecycle transitions**: `StartApplication` (READY -> STARTING -> RUNNING) and
  `StopApplication` (RUNNING -> STOPPING -> READY) both correctly gate on the real API's
  documented precondition ("application status must be READY to start" /
  "can only stop a RUNNING application"), and `launchTransition`'s background goroutine actually
  advances the transient state after `transitionDelay` -- not a disguised no-op; a client polling
  `DescribeApplication` will observe the terminal state within `transitionDelay` (50ms).
  `UpdateApplication`'s optimistic-concurrency check (`CurrentApplicationVersionId` vs
  `ApplicationVersionId`) is real and enforced, as is every `Add*`/`Delete*` sub-resource op's
  `checkAndBumpVersion` call.
- **ApplicationStatus enum**: DELETING/STARTING/STOPPING/READY/RUNNING/UPDATING match
  `types.ApplicationStatus`'s six real v1 values exactly (see gaps for the unused v2-only
  constants that shouldn't be there but are dead code).
- **ListApplications**: `ApplicationSummaries`/`HasMoreApplications` pagination shape, and the
  absence of a `NextToken`, already matches `types.ApplicationSummary` /
  `ListApplicationsOutput` -- no cursor-based pagination in the real v1 API.
