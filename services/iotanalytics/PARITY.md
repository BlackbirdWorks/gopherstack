---
service: iotanalytics
sdk_module: aws-sdk-go-v2/service/iotanalytics@v1.32.0
last_audit_commit: a910ab55a
last_audit_date: 2026-07-13
overall: A            # genuine fixes found (tag validation, dataset-content versionId semantics, pagination)
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
  CreateDatasetContent: {wire: ok, errors: ok, state: ok, persist: ok, note: "always synchronously SUCCEEDED (no CREATING/FAILED simulation) -- acceptable simplification, see Notes"}
  GetDatasetContent: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED: now honors $LATEST / $LATEST_SUCCEEDED (uppercase, as sent by the SDK) in addition to an omitted versionId; previously matched only a non-wire-accurate lowercase '$latest'"}
  ListDatasetContents: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED: pagination cursor was VersionID-threshold (random UUID, unrelated to the CreationTime-descending sort) -- now offset-based. FIXED: underlying sort used slices.SortFunc (unstable) over second-resolution timestamps, so tied entries could reorder between calls -- now slices.SortStableFunc with a reversed-input tiebreak (see Notes)"}
  DeleteDatasetContent: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED: omitted versionId previously deleted ALL content versions; AWS defaults to $LATEST_SUCCEEDED (exactly one version). Now also honors explicit $LATEST / $LATEST_SUCCEEDED"}
  DescribeLoggingOptions: {wire: ok, errors: ok, state: ok, persist: ok}
  PutLoggingOptions: {wire: ok, errors: ok, state: ok, persist: ok}
  RunPipelineActivity: {wire: ok, errors: ok, state: partial, persist: n/a, note: "pass-through only -- see gaps"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  routing: {status: ok, note: "RouteMatcher + parseIoTAnalyticsPath verified path-prefix and HTTP-method-for-method against every awsRestjson1_serializeOpHttpBindings*/request.Method in aws-sdk-go-v2/service/iotanalytics@v1.32.0/serializers.go -- all 33 ops match (paths, GET/POST/PUT/DELETE, query param names incl. includeStatistics/maxMessages/maxResults/nextToken/resourceArn/tagKeys/versionId)"}
  timestamps: {status: ok, note: "creationTime/lastUpdateTime/lastMessageArrivalTime/completionTime/startTime/endTime all epoch-seconds JSON numbers (awstime-equivalent; models.go epochSeconds), matches smithytime.ParseEpochSeconds/FormatEpochSeconds in the real deserializers/serializers"}
gaps:
  - "RunPipelineActivity returns payloads unchanged regardless of the requested pipelineActivity (addAttributes/removeAttributes/selectAttributes/filter/math/lambda/deviceRegistryEnrich/deviceShadowEnrich all no-op pass through; only channel/datastore source activities are pass-through in real AWS too). Implementing real per-activity-type transforms (esp. filter/math expression evaluation) is a distinct, large scope -- file as follow-up bd issue rather than a partial/half-correct expression engine."
  - "GetDatasetContent always returns an empty entries array (no S3-backed data URIs) since this backend has no S3 delivery integration -- consistent with CreateDatasetContent's synchronous SUCCEEDED simulation, not tracked as a bug."
deferred:
  - "ListDatasetContents scheduledBefore/scheduledOnOrAfter query filters (present on ListDatasetContentsInput) -- not implemented; low-traffic filter, no existing caller exercises it"
  - "DatastorePartitions cardinality/shape validation (AWS limits on partition count/nesting) -- not audited this pass"
leaks: {status: clean, note: "no goroutines/janitors owned by this backend; svcCtx is only used to seed test helpers (AddChannelInternal etc.)"}
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
- `RunPipelineActivity` is intentionally left as a documented gap rather than a partial fix:
  implementing per-activity-type semantics (especially `filter`'s and `math`'s
  IoT-Analytics-specific expression languages) is out of scope for a bug-fix sweep and risks
  a half-correct expression evaluator being worse than an honest pass-through. `channel` and
  `datastore` source/sink activities are legitimately pass-through in real AWS too, so the
  existing test coverage (which only exercises `channel`) does not reflect a false positive.
- Persistence: `channels`/`datastores`/`datasets`/`pipelines` are `store.Table[T]` (key =
  `Name`, no secondary index needed -- `resolveARNResource` parses the ARN's resource segment
  back into a name rather than reverse-indexing). `tags`/`channelMessages`/`datasetContents`
  are plain maps folded into `backendSnapshot` alongside `registry.SnapshotAll()`. `Handler`
  delegates `Snapshot`/`Restore` to the backend via the `Snapshottable` interface -- verified
  present and wired correctly, nothing to fix.
- `TestSDKCompleteness` (sdk_completeness_test.go) confirms all 33 SDK ops are handled with
  zero entries in the `notImplemented` acknowledgement list.
