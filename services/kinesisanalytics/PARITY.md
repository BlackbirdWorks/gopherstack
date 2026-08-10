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
  DiscoverInputSchema: {wire: ok, errors: fixed, state: ok, persist: n/a, note: "fixed twice over: (1) the request previously did zero validation and always returned a canned 200 OK schema regardless of input -- now enforces exactly-one-of-{ResourceARN+RoleARN, S3Configuration{BucketARN,FileKey,RoleARN}} plus InputProcessingConfiguration's usual required-field contract, rejecting malformed requests with InvalidArgumentException. (2) the successful-path response was ALSO fabricated even for well-formed requests (a canned COL_1/value1 schema for any ResourceARN/S3Configuration, including nonexistent ones) -- now DiscoverInputSchema actually samples real records via new SetKinesisStreamReader/SetS3ObjectReader hooks (discover_schema.go) and infers RecordColumns from them, returning UnableToDetectSchemaException (a real, previously-unused error on this op) when it cannot reach or sample the source. cli.go now wires both: S3 directly (kaBk.SetS3ObjectReader(s3Bk), no adapter -- s3.InMemoryBackend.GetObject satisfies S3ObjectReader with the real SDK types) and Kinesis via a new kinesisAnalyticsStreamReaderAdapter (cli.go) bridging onto KinesisStreamReader's narrow shape, both proven end to end through the real HTTP dispatch by TestInitializeServices_KinesisAnalyticsKinesisS3Wiring (cli_kinesisanalytics_kinesis_s3_wiring_test.go). A Firehose-sourced ResourceARN still has no reader at all -- see gaps -- and correctly returns UnableToDetectSchemaException rather than something misleading."}
families:
  requiredFieldValidation: {status: fixed, note: "A whole class of nested required-member validation gaps, all verified against aws-sdk-go-v2/service/kinesisanalytics/validators.go (the authoritative client-side validator source, distinct from -- and occasionally contradicting -- doc comments): Input.InputSchema; KinesisStreamsInput/KinesisFirehoseInput/KinesisStreamsOutput/KinesisFirehoseOutput/LambdaOutput.ResourceARN+RoleARN (required whenever their parent sub-object is supplied at all); InputProcessingConfiguration.InputLambdaProcessor (required whenever InputProcessingConfiguration is supplied) and its own ResourceARN/RoleARN; SourceSchema.RecordFormat.RecordFormatType (restricted to the real two-value RecordFormatType enum, JSON/CSV -- previously only enforced for Output.DestinationSchema, not for Input.InputSchema or ReferenceDataSource.ReferenceSchema); SourceSchema.RecordColumns (required, non-nil) and each RecordColumn's Name/SqlType; JSONMappingParameters.RecordRowPath and CSVMappingParameters.RecordRowDelimiter/RecordColumnDelimiter (required whenever their parent variant is supplied). Previously these gaps meant a malformed request (missing schema, missing role ARN on a nested Kinesis/Lambda sub-object, empty processing configuration, invalid record-format type) was silently accepted and stored with zero-valued/absent fields instead of being rejected with InvalidArgumentException -- a disguised-corruption bug in the same family as the UpdateApplication wire-shape bug fixed in a prior sweep. Centralized in new helpers (validateResourceRoleARN, convertInputProcessingConfig, convertSourceSchema + validateRecordFormatType/validateMappingParameters/validateRecordColumns in applications.go) shared across CreateApplication/AddApplicationInput/AddApplicationOutput/AddApplicationInputProcessingConfiguration/AddApplicationReferenceDataSource/UpdateApplication's ReferenceSchemaUpdate."}
  updateNestedPayloads: {status: ok, note: "InputUpdate/OutputUpdate/ReferenceDataSourceUpdate's Kinesis*/Lambda/S3/InputProcessingConfiguration/InputSchema/InputParallelism sub-objects all correctly carry AWS-suffixed field names (ResourceARNUpdate, RoleARNUpdate, BucketARNUpdate, FileKeyUpdate, ReferenceRoleARNUpdate, RecordColumnUpdates, RecordEncodingUpdate, RecordFormatUpdate, CountUpdate), each with its own dedicated Go type -- verified against aws-sdk-go-v2/service/kinesisanalytics/serializers.go's per-shape awsAwsjson11_serializeDocument* functions. InputSchemaUpdate is correctly applied as a field-by-field partial patch; ReferenceSchemaUpdate is correctly applied as a whole-object SourceSchema replace (confirmed via types.ReferenceDataSourceUpdate.ReferenceSchemaUpdate *SourceSchema)."}
gaps:
  - "DiscoverInputSchema now does real sampling+inference (discover_schema.go: newline-delimited-JSON sampling, per-key BOOLEAN/INTEGER/DOUBLE/VARCHAR(N) type inference, sorted-alphabetical column order) instead of a fixed synthetic schema, and cli.go wires both readers it needs: S3 directly (kaBk.SetS3ObjectReader(s3Bk), no adapter -- s3.InMemoryBackend.GetObject satisfies S3ObjectReader with the real SDK types) and Kinesis via kinesisAnalyticsStreamReaderAdapter (cli.go), which bridges kinesis.InMemoryBackend's real ctx+typed-struct ListShards/GetShardIterator/GetRecords (services/kinesis/records.go, shards.go) onto KinesisStreamReader's narrow (streamName string, limit int) shape. Both proven through the actual composition root (not the wiring helper called directly) by TestInitializeServices_KinesisAnalyticsKinesisS3Wiring (cli_kinesisanalytics_kinesis_s3_wiring_test.go), which deletes its own wireKinesisAnalyticsCrossService call site to confirm the test goes red. Firehose delivery streams as a DiscoverInputSchema source remain genuinely unimplemented, not just unwired: firehose.InMemoryBackend has no accessor to read back buffered/recently-ingested records at all (it's flush-oriented), and adding one is outside services/kinesisanalytics. A Firehose-sourced request (and any request before either reader existed) correctly reports UnableToDetectSchemaException (a real, previously-unused SDK error type for this exact op -- see errors.go) instead of fabricating a 200 -- covered by the same wiring test's firehose_source_reports_unable_to_detect_schema subtest."
  - "statusUpdating (\"UPDATING\", a real ApplicationStatus enum value per types/enums.go) is unused by design, not by omission: it is present in source (matches the wire enum exactly, not a gap in the enum itself), but UpdateApplication is genuinely synchronous here -- it validates, applies, and bumps ApplicationVersionId/LastUpdateTimestamp atomically under the backend lock before returning, so a client can never observe an intermediate state where those fields disagree. This is the same shape as the emrserverless-SUBMITTED and elasticsearch-Processing precedents judged legitimate simplifications: the transient state is unreachable because nothing async ever exists to be caught mid-transition, not because a field is missing or inconsistent. No code change made for this item."
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

4. **`DiscoverInputSchema`'s successful-path response was ALSO a fixed synthetic sample**, even
   for well-formed requests naming a source that plainly can't exist (e.g.
   `arn:...:stream/test`) -- confirmed by running `TestHandler_DiscoverInputSchema` against the
   pre-fix code: both the streaming-source and S3-source cases returned `200 OK` with the same
   canned `COL_1`/`value1` schema regardless of what `ResourceARN`/`S3Configuration` named. Added
   real sampling+inference (discover_schema.go): `KinesisStreamReader`/`S3ObjectReader` are new
   narrow interfaces (interfaces.go, mirroring services/cloudwatch's `FirehosePutter` /
   services/firehose's `KinesisReader`/`S3Storer` cross-service pattern) that a sibling backend
   satisfies once wired via the new `SetKinesisStreamReader`/`SetS3ObjectReader`; when wired,
   `DiscoverInputSchema` samples up to 10 newline-delimited-JSON records and infers a
   `RecordColumn` per observed key (BOOLEAN/INTEGER/DOUBLE/VARCHAR(N), sorted-alphabetical
   order). `s3.InMemoryBackend.GetObject` satisfies `S3ObjectReader` directly, with no adapter,
   proven by `TestS3ObjectReader_SatisfiedByRealS3Backend`. Neither hook is wired by cli.go (out
   of this pass's scope -- see gaps for exactly what wiring would close the gap), so in the
   shipped server every request now returns `UnableToDetectSchemaException` -- a real,
   previously-unused error on this exact op (see errors.go) -- rather than a fabricated 200.
   `TestDiscoverInputSchema_S3SamplesRealRecords`/`_KinesisSamplesRealRecords` prove the sampling
   +inference logic itself is correct once a reader is wired, using fake test doubles for the
   same reason services/firehose's own `KinesisReader` tests do (no cli.go-side adapter exists to
   bridge onto `kinesis.InMemoryBackend`'s real ctx+typed-struct methods).

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
  v1 values exactly -- `statusUpdating` is a correct, present enum value, just unreachable
  because `UpdateApplication` is genuinely synchronous (verdict: legitimate simplification, same
  class as emrserverless's SUBMITTED-only run status and elasticsearch's unreached Processing
  state -- see gaps for the reasoning).
- **Cascade cleanup on delete / no ghost rows**: inputs/outputs/reference-data-sources/tags are
  all plain fields embedded directly on `Application` (not separate top-level maps), so
  `DeleteApplication` removing the `Application` row from `b.apps` inherently removes every
  sub-resource with it -- there is no separate cleanup step to forget. Confirmed no orphaned
  per-sub-resource maps exist anywhere in store.go/store_setup.go.
- **ListApplications**: `ApplicationSummaries`/`HasMoreApplications` pagination shape, and the
  absence of a `NextToken`, matches `types.ApplicationSummary` / `ListApplicationsOutput` -- no
  cursor-based pagination in the real v1 API.
