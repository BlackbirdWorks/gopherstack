---
service: iotanalytics
sdk_module: aws-sdk-go-v2/service/iotanalytics@v1.32.0
last_audit_commit: be69d5ece
last_audit_date: 2026-07-24
overall: A            # RunPipelineActivity real per-activity transforms, ListDatasetContents schedule filters, CreateDatasetContent versionId, DatastorePartitions validation, lambda/deviceRegistryEnrich/deviceShadowEnrich cross-service wiring, math functions
ops:
  CreateChannel: {wire: ok, errors: ok, state: ok, persist: ok, note: "now validates tags (key/value charset, aws: prefix, max 50) before create, matching TagResource"}
  DescribeChannel: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateChannel: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteChannel: {wire: ok, errors: ok, state: ok, persist: ok}
  ListChannels: {wire: ok, errors: ok, state: ok, persist: ok, note: "cursor pagination correct: Snapshot() is Name-ascending, cursor thresholds on Name"}
  SampleChannelData: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDatastore: {wire: ok, errors: ok, state: ok, persist: ok, note: "now validates tags before create (see CreateChannel)"}
  DescribeDatastore: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDatastore: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDatastore: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDatastores: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDataset: {wire: ok, errors: ok, state: ok, persist: ok, note: "now validates tags before create (see CreateChannel)"}
  DescribeDataset: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDataset: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDataset: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDatasets: {wire: ok, errors: ok, state: ok, persist: ok}
  CreatePipeline: {wire: ok, errors: ok, state: ok, persist: ok, note: "now validates tags before create (see CreateChannel)"}
  DescribePipeline: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdatePipeline: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePipeline: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPipelines: {wire: ok, errors: ok, state: ok, persist: ok}
  StartPipelineReprocessing: {wire: ok, errors: ok, state: ok, persist: ok}
  CancelPipelineReprocessing: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchPutMessage: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDatasetContent: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED: now accepts and honors an explicit versionId body field (CreateDatasetContentInput.VersionId), previously silently discarded -- duplicate explicit versionId against the same dataset now returns ResourceAlreadyExistsException instead of being accepted. Still always synchronously SUCCEEDED (no CREATING/FAILED simulation) -- acceptable simplification, see Notes"}
  GetDatasetContent: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED: now honors $LATEST / $LATEST_SUCCEEDED (uppercase, as sent by the SDK) in addition to an omitted versionId; previously matched only a non-wire-accurate lowercase '$latest'"}
  ListDatasetContents: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED: pagination cursor was VersionID-threshold (random UUID, unrelated to the CreationTime-descending sort) -- now offset-based. FIXED: underlying sort used slices.SortFunc (unstable) over second-resolution timestamps, so tied entries could reorder between calls -- now slices.SortStableFunc with a reversed-input tiebreak (see Notes). FIXED: scheduleTime was missing entirely from DatasetContentSummary (a real field, distinct from creationTime) and the scheduledBefore/scheduledOnOrAfter query filters were unimplemented -- both now implemented (see Notes)"}
  DeleteDatasetContent: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED: omitted versionId previously deleted ALL content versions; AWS defaults to $LATEST_SUCCEEDED (exactly one version). Now also honors explicit $LATEST / $LATEST_SUCCEEDED"}
  DescribeLoggingOptions: {wire: ok, errors: ok, state: ok, persist: ok}
  PutLoggingOptions: {wire: ok, errors: ok, state: ok, persist: ok}
  RunPipelineActivity: {wire: ok, errors: ok, state: ok, persist: n/a, note: "FIXED: addAttributes/removeAttributes/selectAttributes/filter/math now perform real per-activity transforms (see Notes and pipeline_expr.go); channel/datastore remain pass-through (correct: real source/sink activities); lambda/deviceRegistryEnrich/deviceShadowEnrich now invoke the real Lambda/IoT backends when cli.go's wireIoTAnalyticsCrossService has wired them (see Notes); math now supports the documented math-operators-functions.html function library; filter's LIKE/IN/BETWEEN remain unimplemented -- see items_still_open"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  routing: {status: ok, note: "RouteMatcher + parseIoTAnalyticsPath verified path-prefix and HTTP-method-for-method against every awsRestjson1_serializeOpHttpBindings*/request.Method in aws-sdk-go-v2/service/iotanalytics@v1.32.0/serializers.go -- all 33 ops match (paths, GET/POST/PUT/DELETE, query param names incl. includeStatistics/maxMessages/maxResults/nextToken/resourceArn/tagKeys/versionId/scheduledBefore/scheduledOnOrAfter)"}
  timestamps: {status: ok, note: "creationTime/lastUpdateTime/lastMessageArrivalTime/completionTime/startTime/endTime/scheduleTime all epoch-seconds JSON numbers (awstime-equivalent; models.go epochSeconds), matches smithytime.ParseEpochSeconds/FormatEpochSeconds in the real deserializers/serializers"}
gaps:
  - "GetDatasetContent always returns an empty entries array (no S3-backed data URIs) since this backend has no S3 delivery integration -- consistent with CreateDatasetContent's synchronous SUCCEEDED simulation, not tracked as a bug."
  - "items_still_open: RunPipelineActivity's lambda/deviceRegistryEnrich/deviceShadowEnrich now invoke the real Lambda/IoT backends (cli.go's wireIoTAnalyticsCrossService -> InMemoryBackend.SetLambdaBackend/SetThingRegistry/SetThingShadowStore, following the same LambdaInvoker pattern SNS/Firehose/SecretsManager already use). lambda batches payloads by BatchSize and round-trips a JSON object array through InvokeFunction, matching the documented contract (docs.aws.amazon.com/iotanalytics/latest/userguide/pipeline-activities-lambda.html: \"the Lambda function must receive and return a JSON object array\"); deviceRegistryEnrich/deviceShadowEnrich call iot:DescribeThing/iot:GetThingShadow and store the result under Attribute (CloudFormation docs for AWS::IoTAnalytics::Pipeline DeviceRegistryEnrich/DeviceShadowEnrich). A missing Thing/shadow or a Lambda invoke error fails the RunPipelineActivity call (ErrPipelineActivityFailed) rather than silently passing the message through, since these AWS calls genuinely fail when their target doesn't exist. Only remaining gap: when no Lambda/IoT backend is registered in a given deployment (SetLambdaBackend/SetThingRegistry/SetThingShadowStore never called), these activities still pass through unchanged -- there is nothing to invoke."
  - "items_still_open: RunPipelineActivity's math expression language (pipeline_expr.go) now additionally implements AWS's documented function library (docs.aws.amazon.com/iotanalytics/latest/userguide/math-operators-functions.html: abs/acos/asin/atan/atan2/ceil/cos/cosh/exp/ln/log/mod/power/round/sign/sin/sinh/sqrt/tan/tanh/trunc). filter/math still do NOT implement LIKE, IN, or BETWEEN. Reason: unlike the math function library, no citable AWS documentation for filter's operators beyond '=, !=, <, <=, >, >=, AND, OR, NOT' was found (docs.aws.amazon.com/iotanalytics/latest/APIReference/API_RunPipelineActivity.html and the userguide's pipeline-activities-filter.html describe it only as \"an expression that looks like an SQL WHERE clause\", with no operator/function reference page equivalent to math's) -- extending the grammar with LIKE/IN/BETWEEN would be inventing behavior against an unpublished spec, not closing a documented gap."
deferred: []
leaks: {status: clean, note: "no goroutines/janitors owned by this backend; svcCtx is only used to seed test helpers (AddChannelInternal etc.). pipeline_expr.go's tokenizer/parser/evaluator is pure, synchronous, and per-call -- no new goroutines, tickers, or shared mutable state introduced."}
---

## Notes

- Protocol: restjson1. Service ARNs: `arn:aws:iotanalytics:<region>:<account>:<type>/<name>` via `pkgs/arn.Build`.
- `/channels` path is shared with MediaPackage/MediaTailor matchers at the same routing
  priority; the RouteMatcher disambiguates via `httputils.ExtractServiceFromRequest` (SigV4
  service name), not path alone. Verified correct.
- **Tag validation gap (fixed):** `CreateChannel`/`CreateDatastore`/`CreateDataset`/
  `CreatePipeline` accepted arbitrary tags (bad charset, `aws:` prefix, >50 tags) at creation
  time even though the identical tags would be rejected by a subsequent `TagResource` call on
  the same resource. AWS validates tags identically regardless of which API attached them.
  Fixed by calling `validateTags(req.Tags)` in all four create handlers before conversion to
  the internal map; `validateTags` now also enforces the 50-tag cap on the incoming batch
  itself (in addition to `TagResource`'s existing incremental cap against the existing set).
- **`$LATEST` / `$LATEST_SUCCEEDED` versionId (fixed):** AWS's `GetDatasetContent` and
  `DeleteDatasetContent` accept the sentinel strings `$LATEST` and `$LATEST_SUCCEEDED`
  (uppercase, sent verbatim as the `versionId` query param by the SDK) and default to
  `$LATEST_SUCCEEDED` when the query param is omitted entirely. The old code matched only a
  lowercase `"$latest"` literal that no real client ever sends, so any client passing the
  actual AWS sentinel value fell through to exact-match-by-UUID and got a spurious 404. Worse,
  `DeleteDatasetContent` treated an omitted `versionId` as "delete every version for this
  dataset" -- AWS deletes exactly one (the latest `SUCCEEDED` version). Both are fixed in
  `backend.go` (`latestSucceededContent` helper, `deleteDatasetContentVersion` helper). Since
  `CreateDatasetContent` always synthesizes `Status: SUCCEEDED` synchronously, `$LATEST` and
  `$LATEST_SUCCEEDED` coincide in this backend today, but the distinct code paths are kept
  because `DatasetContent.Status` is a real field or future async simulation would silently
  regress this.
- **`ListDatasetContents` pagination cursor (fixed):** the handler compared
  `content.VersionID <= cursor` to decide what to skip, but `ListDatasetContents` sorts by
  `CreationTime` descending -- `VersionID` is a random UUID with no relation to that order.
  Unlike `ListChannels`/`ListDatastores`/`ListDatasets`/`ListPipelines` (whose `Name`-keyed
  `store.Table.Snapshot()` is naturally ascending by the same field used as the cursor), this
  meant a `nextToken` cursor from page 1 would skip or repeat an effectively arbitrary subset
  of page 2. Fixed by switching to an offset-encoded token (`encodeNextToken(strconv.Itoa(end))`)
  in `handleListDatasetContents`.
- **`ListDatasetContents` sort stability (fixed, found while fixing pagination):**
  `CreationTime` is `epochSeconds` (second resolution). Content versions created within the
  same wall-clock second (e.g. a tight test loop, or a client bursting `CreateDatasetContent`
  calls) tie on `CreationTime`. The prior code used `slices.SortFunc`, which the stdlib
  explicitly documents as **not stable** -- tied entries could come back in a different
  relative order on every call to `ListDatasetContents`, even with zero mutation in between.
  That nondeterminism alone would have broken correct pagination even after the cursor fix
  above (page 1 and page 2 are two separate backend calls). Fixed by reversing the
  creation-order copy before `slices.SortStableFunc`, which makes ties resolve
  deterministically as "most-recently-inserted first" and keeps repeated calls
  byte-for-byte identical.
- **`RunPipelineActivity` real per-activity transforms (fixed):** previously every activity
  type, including `addAttributes`/`removeAttributes`/`selectAttributes`/`filter`/`math`, was
  pass-through regardless of the requested activity -- a real gap, since AWS applies real
  transforms for these. `pipeline_expr.go` adds a self-contained tokenizer, recursive-descent
  parser, and evaluator for the SQL-like expression language `filter` and `math` carry
  (literals, message-attribute identifiers, `+ - * / %`, `= != <> < <= > >=`, `AND/OR/NOT`,
  parentheses). `pipelines.go` wires per-activity-type handling into `RunPipelineActivity`
  (now takes the typed `PipelineActivity` the client sent, not an untyped
  `map[string]any` the old code never even inspected):
  `addAttributes`/`removeAttributes`/`selectAttributes` mutate the decoded JSON message
  object; `filter` evaluates the expression per payload and drops non-matching (or
  unparsable) payloads, matching a real filter activity removing messages from the pipeline;
  `math` evaluates the expression and stores the numeric result under `Attribute`. A
  per-message failure (non-JSON payload, unknown attribute, malformed expression, type
  mismatch) is a soft failure -- the payload is left unchanged (transforms/math) or dropped
  (filter) rather than failing the whole `RunPipelineActivity` call, matching a single bad
  message failing only its own activity step. `channel`/`datastore` remain pass-through
  (correct: real source/sink activities).
- **`RunPipelineActivity` lambda/deviceRegistryEnrich/deviceShadowEnrich cross-service wiring
  (fixed):** these three activities were pass-through with a note claiming this backend "has
  no wiring" for cross-service calls -- the same stale claim this parity campaign found and
  fixed for sagemaker's S3 read, ELB's EC2/ACM/IAM checks, and glacier's S3 write-back. The
  wiring pattern already exists (SNS/Firehose/SecretsManager's `LambdaInvoker` +
  `SetLambdaBackend`, IoT's `DescribeThing`/`GetThingShadow` used elsewhere in `cli.go`) and
  applies here unchanged. `services/iotanalytics/interfaces.go` adds `LambdaInvoker`,
  `ThingRegistry`, `ThingShadowStore`; `cli.go`'s `wireIoTAnalyticsCrossService` (called from
  `wireStorageAndSecretsIntegrations`) wires the real Lambda backend directly (it already
  satisfies `LambdaInvoker`) and adapts the IoT backend's `DescribeThing`/`GetThingShadow`
  (`iotAnalyticsThingRegistryAdapter`/`iotAnalyticsThingShadowAdapter`) into the map-shaped
  interfaces `pipelines.go` uses. `RunPipelineActivity` now takes a `ctx` parameter to thread
  through to `InvokeFunction`. Per-activity behavior: `lambda` batches payloads by
  `BatchSize` and round-trips a JSON object array through `InvokeFunction("RequestResponse")`
  per AWS's documented contract; `deviceRegistryEnrich`/`deviceShadowEnrich` call
  `DescribeThing`/`GetThingShadow` once per activity call (the target `ThingName` is a fixed
  activity field, not per-message) and store the result under `Attribute` on every payload.
  Unlike the per-message soft-failure convention above, a missing Thing/shadow or a Lambda
  invoke/response error fails the whole call (`ErrPipelineActivityFailed`) rather than passing
  the message through unchanged -- a real AWS `iot:DescribeThing`/`iot:GetThingShadow`/Lambda
  invoke against a nonexistent target genuinely fails, and silently returning the original
  message would be the same silent-drop bug class this campaign has been hunting. When no
  Lambda/IoT backend is registered at all (`SetLambdaBackend`/`SetThingRegistry`/
  `SetThingShadowStore` never called), these three activities still pass through unchanged --
  there's nothing to invoke, which is an environment characteristic, not a bug. Proven by
  `cli_iotanalytics_lambda_iot_wiring_test.go`, which drives `initializeServices` (the actual
  `cli.go` composition root) rather than calling the wiring helper directly.
- **`RunPipelineActivity` math function library (fixed):** `math` only implemented arithmetic
  (`+ - * / %`); AWS documents a real function library at
  `math-operators-functions.html` (`abs/acos/asin/atan/atan2/ceil/cos/cosh/exp/ln/log/mod/
  power/round/sign/sin/sinh/sqrt/tan/tanh/trunc`, each `func(Decimal[, Decimal])` per that
  page's exact signatures -- `trunc` takes a second `int` argument, `atan2`/`mod`/`power` take
  two `Decimal`s, the rest take one). This was previously mischaracterized as part of an
  "undocumented AWS superset" alongside filter's `LIKE`/`IN`/`BETWEEN`; the math function list
  specifically is real, citable, and now implemented in `pipeline_expr.go`
  (`mathFuncs1`/`mathFuncs2`, `funcCallNode`). `filter`'s grammar beyond comparisons/logical
  operators remains genuinely undocumented (no equivalent operator/function reference page
  exists for filter) and is intentionally not extended -- see `items_still_open`.
- **`CreateDatasetContent` explicit `versionId` (fixed):** `CreateDatasetContentInput` has a
  real `versionId` body field the old handler never read (the handler didn't even parse a
  request body). Now `handleCreateDatasetContent` parses `createDatasetContentRequest` and
  `InMemoryBackend.CreateDatasetContent(datasetName, versionID string)` uses the caller's
  `versionID` when non-empty (generating a UUID only when it's empty, as before), rejecting a
  duplicate with `ErrAlreadyExists` (409) instead of silently accepting it. AWS's docs say
  specifying `versionId` requires the dataset to use a `DeltaTimer` filter; this backend
  accepts it unconditionally rather than modeling that restriction, since enforcing it would
  require simulating `DeltaTimer`-driven dataset content generation this backend does not
  otherwise implement.
- **`ListDatasetContents` `scheduleTime` field + `scheduledBefore`/`scheduledOnOrAfter`
  filters (fixed):** `DatasetContentSummary.ScheduleTime` ("the time the creation of the
  dataset contents was scheduled to start", distinct from `CreationTime`, "the actual time
  ... was started") was missing entirely from `DatasetContent`/`datasetContentSummary`, and
  the `scheduledBefore`/`scheduledOnOrAfter` query filters on `ListDatasetContentsInput` were
  unimplemented. `DatasetContent.ScheduleTime` is now set equal to `CreationTime` in
  `CreateDatasetContent` -- this backend only ever creates dataset content synchronously via
  a direct API call (no background cron simulation of a dataset's `Schedule` trigger), which
  is exactly the case where real AWS also sets `scheduleTime == creationTime` (a manually
  invoked `CreateDatasetContent` wasn't fired by a schedule). `handleListDatasetContents` now
  parses the `scheduledBefore`/`scheduledOnOrAfter` query params (RFC3339 date-time strings,
  matching `smithytime.FormatDateTime`) and filters on `ScheduleTime` before pagination.
- **`DatastorePartitions` validation (fixed):** `CreateDatastore`/`UpdateDatastore` accepted
  any `DatastorePartitions` shape, including partition entries with neither
  `attributePartition` nor `timestampPartition` set, both set, or a set variant with an empty
  `attributeName`. The real SDK's client-side validators (`validatePartition` /
  `validateTimestampPartition`) require `attributeName` on whichever variant is set; a raw
  HTTP caller bypassing SDK-side validation would still need to satisfy this server-side.
  `validateDatastorePartitions`/`validateDatastorePartitionEntry` in `store.go` now enforce
  exactly-one-variant-set plus a non-empty `attributeName`, returning `InvalidRequestException`
  otherwise. Partition count/nesting cardinality limits are not enforced -- no SDK client-side
  validator surfaces a specific limit to diff against, and AWS's server-side limits for a
  deprecated service are not independently documented; this is treated as an intentional
  non-issue rather than a gap.
- Persistence: `channels`/`datastores`/`datasets`/`pipelines` are `store.Table[T]` (key =
  `Name`, no secondary index needed -- `resolveARNResource` parses the ARN's resource segment
  back into a name rather than reverse-indexing). `tags`/`channelMessages`/`datasetContents`
  are plain maps folded into `backendSnapshot` alongside `registry.SnapshotAll()`. `Handler`
  delegates `Snapshot`/`Restore` to the backend via the `Snapshottable` interface -- verified
  present and wired correctly, nothing to fix.
- `TestSDKCompleteness` (sdk_completeness_test.go) confirms all 33 SDK ops are handled with
  zero entries in the `notImplemented` acknowledgement list.
