---
service: kinesisanalytics
sdk_module: aws-sdk-go-v2/service/kinesisanalytics@v1.30.21
last_audit_commit: 6e7056ac
last_audit_date: 2026-07-24
overall: A            # real fixes found: deleted three gopherstack-invented surfaces
                       # (ServiceExecutionRole/RuntimeEnvironment fields, five non-real
                       # ApplicationStatus constants, InputUpdate.InputStartingPositionConfiguration),
                       # and closed a whole class of missing required-field validation across
                       # Input/Output/ReferenceDataSource/InputProcessingConfiguration/SourceSchema
                       # that let malformed requests silently succeed instead of failing with
                       # InvalidArgumentException like real AWS.
ops:
  CreateApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "ServiceExecutionRole request field DELETED (gopherstack-invented -- CreateApplicationInput has no such member in the real SDK, verified via grep across the whole module). Inputs[]/Outputs[] now route through the same hardened convertInputConfig/convertOutputConfig validation as AddApplicationInput/AddApplicationOutput (see families below)."}
  DeleteApplication: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "ServiceExecutionRole/RuntimeEnvironment response fields DELETED (gopherstack-invented, present nowhere in ApplicationDetail)."}
  ListApplications: {wire: ok, errors: ok, state: ok, persist: ok, note: "HasMoreApplications/ExclusiveStartApplicationName pagination correct, no NextToken -- matches real ListApplicationsInput/Output exactly."}
  StartApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "READY->STARTING->RUNNING transition via launchTransition goroutine, correct."}
  StopApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "RUNNING->STOPPING->READY transition, correct."}
  UpdateApplication: {wire: fixed, errors: ok, state: ok, persist: ok, note: "InputUpdate.InputStartingPositionConfiguration field DELETED (gopherstack-invented -- the real InputUpdate shape has no such member; starting-position changes are only ever accepted via StartApplication's InputConfigurations). ReferenceDataSourceUpdate.ReferenceSchemaUpdate (a whole-object SourceSchema replace, per its doc) now runs through the same required-field validation as a fresh ReferenceSchema."}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  AddApplicationCloudWatchLoggingOption: {wire: ok, errors: ok, state: ok, persist: ok}
  AddApplicationInput: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "Input.InputSchema (a required member per validators.go's validateInput -- authoritative over the doc comment, which doesn't call it out) was never validated; a request omitting it was silently accepted with a nil InputSchema. Also added: KinesisStreamsInput/KinesisFirehoseInput.ResourceARN+RoleARN required-when-the-sub-object-is-present; InputProcessingConfiguration.InputLambdaProcessor required-when-InputProcessingConfiguration-is-present, and its own ResourceARN/RoleARN required."}
  AddApplicationInputProcessingConfiguration: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "InputId and InputProcessingConfiguration (both required members) were never validated -- a request omitting InputProcessingConfiguration silently cleared/no-op'd instead of being rejected. Now validates both, plus InputProcessingConfiguration.InputLambdaProcessor and its ResourceARN/RoleARN, matching validateOpAddApplicationInputProcessingConfigurationInput/validateInputProcessingConfiguration/validateInputLambdaProcessor."}
  AddApplicationOutput: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "KinesisStreamsOutput/KinesisFirehoseOutput/LambdaOutput.ResourceARN+RoleARN required-when-the-sub-object-is-present was never validated -- added, matching validateKinesisStreamsOutput/validateKinesisFirehoseOutput/validateLambdaOutput."}
  AddApplicationReferenceDataSource: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "ReferenceSchema's own required members (RecordFormat.RecordFormatType restricted to JSON/CSV, RecordColumns non-nil, each RecordColumn.Name/SqlType, JSON/CSVMappingParameters sub-fields) were never validated -- only top-level presence was checked. Added full validateSourceSchema-equivalent validation, shared with AddApplicationInput's InputSchema and UpdateApplication's ReferenceSchemaUpdate via the same convertSourceSchema helper."}
  DeleteApplicationCloudWatchLoggingOption: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteApplicationInputProcessingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteApplicationOutput: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteApplicationReferenceDataSource: {wire: ok, errors: ok, state: ok, persist: ok}
  DiscoverInputSchema: {wire: ok, errors: fixed, state: deferred, persist: n/a, note: "fixed: the request previously did zero validation and always returned a canned 200 OK schema regardless of input (empty request, both a streaming source AND an S3Configuration, or a source missing its own required sub-fields all incorrectly succeeded). Now enforces exactly-one-of-{ResourceARN+RoleARN, S3Configuration{BucketARN,FileKey,RoleARN}} plus InputProcessingConfiguration's usual required-field contract, rejecting malformed requests with InvalidArgumentException like every other modeled error path on this op. The successful-path response content remains an intentional fixed-shape sample -- see gaps."}
families:
  requiredFieldValidation: {status: fixed, note: "A whole class of nested required-member validation gaps, all verified against aws-sdk-go-v2/service/kinesisanalytics/validators.go (the authoritative client-side validator source, distinct from -- and occasionally contradicting -- doc comments): Input.InputSchema; KinesisStreamsInput/KinesisFirehoseInput/KinesisStreamsOutput/KinesisFirehoseOutput/LambdaOutput.ResourceARN+RoleARN (required whenever their parent sub-object is supplied at all); InputProcessingConfiguration.InputLambdaProcessor (required whenever InputProcessingConfiguration is supplied) and its own ResourceARN/RoleARN; SourceSchema.RecordFormat.RecordFormatType (restricted to the real two-value RecordFormatType enum, JSON/CSV -- previously only enforced for Output.DestinationSchema, not for Input.InputSchema or ReferenceDataSource.ReferenceSchema); SourceSchema.RecordColumns (required, non-nil) and each RecordColumn's Name/SqlType; JSONMappingParameters.RecordRowPath and CSVMappingParameters.RecordRowDelimiter/RecordColumnDelimiter (required whenever their parent variant is supplied). Previously these gaps meant a malformed request (missing schema, missing role ARN on a nested Kinesis/Lambda sub-object, empty processing configuration, invalid record-format type) was silently accepted and stored with zero-valued/absent fields instead of being rejected with InvalidArgumentException -- a disguised-corruption bug in the same family as the UpdateApplication wire-shape bug fixed in a prior sweep. Centralized in new helpers (validateResourceRoleARN, convertInputProcessingConfig, convertSourceSchema + validateRecordFormatType/validateMappingParameters/validateRecordColumns in applications.go) shared across CreateApplication/AddApplicationInput/AddApplicationOutput/AddApplicationInputProcessingConfiguration/AddApplicationReferenceDataSource/UpdateApplication's ReferenceSchemaUpdate."}
  updateNestedPayloads: {status: ok, note: "InputUpdate/OutputUpdate/ReferenceDataSourceUpdate's Kinesis*/Lambda/S3/InputProcessingConfiguration/InputSchema/InputParallelism sub-objects all correctly carry AWS-suffixed field names (ResourceARNUpdate, RoleARNUpdate, BucketARNUpdate, FileKeyUpdate, ReferenceRoleARNUpdate, RecordColumnUpdates, RecordEncodingUpdate, RecordFormatUpdate, CountUpdate), each with its own dedicated Go type -- verified against aws-sdk-go-v2/service/kinesisanalytics/serializers.go's per-shape awsAwsjson11_serializeDocument* functions. InputSchemaUpdate is correctly applied as a field-by-field partial patch; ReferenceSchemaUpdate is correctly applied as a whole-object SourceSchema replace (confirmed via types.ReferenceDataSourceUpdate.ReferenceSchemaUpdate *SourceSchema)."}
gaps:
  - "DiscoverInputSchema's successful-path response is a fixed synthetic schema/sample-records payload regardless of a well-formed request's actual source; real schema inference from a live Kinesis/Firehose stream or S3 object requires an actual sampling+type-inference engine this emulator does not have. This sweep hardened the REQUEST side (see DiscoverInputSchema op note and TestHandler_DiscoverInputSchema) so malformed requests are correctly rejected with InvalidArgumentException instead of always synthesizing success -- but the successful-path content itself remains a documented, wire-shape-correct stub (handleDiscoverInputSchema doc comment). (bd: TBD)"
  - "statusUpdating (\"UPDATING\", a real ApplicationStatus enum value per types/enums.go) is currently unused: UpdateApplication applies changes synchronously and never transiently sets the application's status to UPDATING the way StartApplication/StopApplication transition through STARTING/STOPPING. Real AWS documents a brief UPDATING window while an update is processed. Not a fabricated value (unlike the five deleted v2-only status constants -- see Notes) and not user-visible today since this emulator's UpdateApplication has no asynchronous gap for a client to observe it during, but a future sweep could add a launchTransition-style transient state for full fidelity. (bd: TBD)"
deferred: []
leaks: {status: clean, note: "launchTransition/DeleteApplication background goroutines remain bounded by b.svcCtx (NewInMemoryBackendWithContext) and tracked in b.cancelFuncs, canceled on Reset(). No new goroutines, maps, or per-request state introduced this sweep -- all changes were request-validation logic in the existing conversion helpers (applications.go/handler_*.go), which return early with an error and mutate no backend state on the rejected path."}
---

## Notes

Protocol: **awsjson1.1**, single POST endpoint, `X-Amz-Target: KinesisAnalytics_20150814.<Op>`
dispatch (verified against handler.go's `kinesisanalyticsTargetPrefix` -- correctly uses the
older 20150814 date, not v2's 20180523). Timestamps (`CreateTimestamp`/`LastUpdateTimestamp`) are
epoch-seconds `float64` with sub-second precision, verified against
`aws-sdk-go-v2/service/kinesisanalytics` deserializers.go's `smithytime.ParseEpochSeconds` --
correct.

### Real bugs fixed this sweep

1. **Three gopherstack-invented surfaces DELETED**, none of which exist anywhere in
   `aws-sdk-go-v2/service/kinesisanalytics@v1.30.21` (verified by grepping the whole downloaded
   SDK module source, including generated serializers/deserializers/validators, not just
   `types.go`):
   - `Application.ServiceExecutionRole` / `Application.RuntimeEnvironment` and their mirrors on
     `createApplicationInput`/`applicationDetail`. `CreateApplicationInput` has no
     `ServiceExecutionRole` member at all (confirmed against `api_op_CreateApplication.go`), and
     neither field appears on `ApplicationDetail`. The only "RuntimeEnvironment" hits in the SDK
     module are unrelated client-config internals (`aws.RuntimeEnvironment` for
     `DefaultsMode` resolution in `options.go`/`api_client.go`), not an API field. Removed from
     `Application`/`createApplicationInput`/`applicationDetail` (models.go),
     `CreateApplication`'s signature (applications.go, store.go's `StorageBackend` interface),
     and `toApplicationDetail` (handler_applications.go). All backend/handler test call sites
     updated (export_test.go, persistence_test.go, isolation_test.go).
   - `statusAutoScaling`/`statusForceStopping`/`statusMaintenance`/`statusRollingBack`/
     `statusRolledBack` constants (store.go), marked `//nolint:deadcode "AWS status constant"`
     but not part of the real v1 `ApplicationStatus` enum (`types/enums.go`'s
     `ApplicationStatus.Values()` returns exactly `DELETING/STARTING/STOPPING/READY/RUNNING/
     UPDATING` -- six values, no more). These five were copied from kinesisanalyticsv2's larger,
     distinct `ApplicationStatus` enum. Deleted; the doc comment on the remaining six real
     constants now states the real enum explicitly so a future audit doesn't need to re-derive
     it.
   - `inputUpdate.InputStartingPositionConfiguration` (models.go) and its handling in
     `applyOneInputUpdate` (application_update.go): the real `InputUpdate` shape
     (`types.InputUpdate`) has exactly `InputId`/`InputParallelismUpdate`/
     `InputProcessingConfigurationUpdate`/`InputSchemaUpdate`/`KinesisFirehoseInputUpdate`/
     `KinesisStreamsInputUpdate`/`NamePrefixUpdate` -- no starting-position member. A real
     client's `UpdateApplication` call can never change an input's starting position; that's
     only reachable via `StartApplication`'s `InputConfigurations` (`types.InputConfiguration`,
     which legitimately does carry `InputStartingPositionConfiguration` -- confirmed distinct
     from `InputUpdate`). Deleted the field and its dead-in-practice handling.

2. **A whole class of missing required-field validation, closed** (applications.go,
   handler_inputs.go, handler_reference_data.go, application_update.go). Verified against
   `aws-sdk-go-v2/service/kinesisanalytics/validators.go`, which is the authoritative source for
   what AWS actually requires client-side (and, by strong inference, server-side) -- doc
   comments alone are occasionally wrong or incomplete (e.g. `Input.InputSchema`'s doc comment
   doesn't say "required" but `validateInput` unconditionally requires it). Before this sweep,
   several nested required members were accepted as absent/empty and silently stored that way
   (a `200 OK` with a corrupted/incomplete resource) instead of being rejected with
   `InvalidArgumentException` -- the same disguised-corruption bug class as the `UpdateApplication`
   nested-payload wire-shape bug fixed in a prior sweep, just at the required-field-presence
   layer instead of the field-naming layer. Fixed:
   - `Input.InputSchema` (required on both `CreateApplication`'s `Inputs[]` and
     `AddApplicationInput`, since both route through `convertInputConfig`).
   - `KinesisStreamsInput`/`KinesisFirehoseInput`/`KinesisStreamsOutput`/`KinesisFirehoseOutput`/
     `LambdaOutput`'s `ResourceARN`+`RoleARN`, required whenever that sub-object is supplied at
     all (new shared `validateResourceRoleARN` helper).
   - `InputProcessingConfiguration.InputLambdaProcessor`, required whenever
     `InputProcessingConfiguration` itself is supplied -- previously an empty
     `InputProcessingConfiguration{}` was silently dropped instead of rejected (new
     `convertInputProcessingConfig` helper, shared by `AddApplicationInput`,
     `AddApplicationInputProcessingConfiguration`, and `DiscoverInputSchema`'s optional
     processing-config field). `AddApplicationInputProcessingConfiguration` additionally never
     validated its own required `InputId`/`InputProcessingConfiguration` members at all.
   - `SourceSchema.RecordFormat.RecordFormatType` restricted to the real two-value
     `RecordFormatType` enum (`JSON`/`CSV`) -- previously only enforced on
     `Output.DestinationSchema`, not on `Input.InputSchema` or
     `ReferenceDataSource.ReferenceSchema`/`ReferenceSchemaUpdate`, despite all four using the
     identically-typed field. `SourceSchema.RecordColumns` (required, non-nil) and each
     `RecordColumn.Name`/`SqlType` (required), and `JSONMappingParameters.RecordRowPath`/
     `CSVMappingParameters.RecordRowDelimiter`/`RecordColumnDelimiter` (required whenever their
     parent variant is supplied) -- all previously unvalidated. Consolidated into
     `convertSourceSchema` (now returning an error), shared by `AddApplicationInput`'s
     `InputSchema`, `AddApplicationReferenceDataSource`'s `ReferenceSchema`, and
     `UpdateApplication`'s `ReferenceSchemaUpdate` (a whole-object replace per its doc, so the
     same required-field contract legitimately applies there too -- unlike `InputSchemaUpdate`,
     which is a genuine partial patch and was correctly left alone).

3. **`DiscoverInputSchema` accepted any request shape and always returned a canned success**
   (handler_inputs.go). An empty request, a request supplying both `ResourceARN` and
   `S3Configuration`, or a source missing its own required sub-fields (`RoleARN` for the
   streaming-source path; `BucketARN`/`FileKey`/`RoleARN` for `S3Configuration`, all three
   `This member is required` per `types.S3Configuration`) all incorrectly returned `200 OK`.
   Added `validateDiscoverInputSchemaInput`, enforcing exactly-one-of-{streaming source, S3
   source} plus each source's required sub-fields, rejecting malformed requests with
   `InvalidArgumentException` -- one of `DiscoverInputSchema`'s modeled errors (confirmed via
   `deserializers.go`'s `awsAwsjson11_deserializeOpErrorDiscoverInputSchema`, alongside
   `ResourceProvisionedThroughputExceededException`/`ServiceUnavailableException`/
   `UnableToDetectSchemaException`). The successful-path response content remains an
   intentionally fixed synthetic sample (see gaps) -- this fix is about the request side, not
   fabricating real schema inference.

### Verified clean (no bug, but worth recording so the next audit doesn't re-flag)

- **Route matcher / target prefix**: `KinesisAnalytics_20150814.` -- correctly uses the v1 date,
  distinct from kinesisanalyticsv2's `KinesisAnalyticsV2_20180523.`. `ExtractOperation` correctly
  falls back to `"Unknown"` when the header is absent or doesn't match, so the `dispatch` map
  lookup fails closed rather than routing garbage.
- **Persistence**: `Handler.Snapshot`/`Restore` (persistence.go) delegate to
  `InMemoryBackend.Snapshot`/`Restore`, which version-gate (`kinesisanalyticsSnapshotVersion`) and
  go through `store.Registry.SnapshotAll`/`RestoreAll` for the `apps` table -- confirmed via
  `TestInMemoryBackend_FullStateSnapshotRestoreRoundTrip` (persistence_test.go), which round-trips
  every sub-resource kind across two regions and survives the `CreateApplication` signature change
  from this sweep.
- **Lifecycle transitions**: `StartApplication` (READY -> STARTING -> RUNNING) and
  `StopApplication` (RUNNING -> STOPPING -> READY) both correctly gate on the real API's
  documented precondition, and `launchTransition`'s background goroutine actually advances the
  transient state after `transitionDelay` (50ms). `UpdateApplication`'s optimistic-concurrency
  check (`CurrentApplicationVersionId` vs `ApplicationVersionId`) is real and enforced, as is
  every `Add*`/`Delete*` sub-resource op's `checkAndBumpVersion` call.
- **ApplicationStatus enum**: the six remaining real constants
  (DELETING/STARTING/STOPPING/READY/RUNNING/UPDATING) match `types.ApplicationStatus`'s six real
  v1 values exactly (see gaps for `statusUpdating`'s currently-unused transient-state status).
- **Cascade cleanup on delete / no ghost rows**: inputs/outputs/reference-data-sources/tags are
  all plain fields embedded directly on `Application` (not separate top-level maps), so
  `DeleteApplication` removing the `Application` row from `b.apps` inherently removes every
  sub-resource with it -- there is no separate cleanup step to forget. Confirmed no orphaned
  per-sub-resource maps exist anywhere in store.go/store_setup.go.
- **ListApplications**: `ApplicationSummaries`/`HasMoreApplications` pagination shape, and the
  absence of a `NextToken`, matches `types.ApplicationSummary` / `ListApplicationsOutput` -- no
  cursor-based pagination in the real v1 API.
