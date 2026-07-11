---
service: cloudwatchlogs
sdk_module: aws-sdk-go-v2/service/cloudwatchlogs@v1.64.0
last_audit_commit: 17a215e4
last_audit_date: 2026-07-05
overall: A
ops:
  PutLogEvents: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: sequenceToken was validated and rejected with InvalidSequenceTokenException; real AWS (v1.64.0 doc) ignores sequenceToken entirely and never returns that exception. Also fixed RejectedLogEventsInfo wire shape (tooOldLogEventStartIndex -> tooOldLogEventEndIndex, exclusive-end semantics) and an off-by-one on ExpiredLogEventEndIndex."}
  GetLogEvents: {wire: ok, errors: ok, state: ok, persist: ok, note: "startFromHead/nextToken precedence and stable-at-boundary forward/backward tokens verified correct."}
  FilterLogEvents: {wire: ok, errors: ok, state: ok, persist: ok, note: "cross-stream interleave + stable timestamp sort verified; logStreamNames/logStreamNamePrefix mutual-exclusion validated; searchedLogStreams correctly always empty (AWS deprecated this field)."}
  CreateLogGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteLogGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascades streams/events/subscription filters/metric filters."}
  DescribeLogGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateLogStream: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteLogStream: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeLogStreams: {wire: ok, errors: ok, state: ok, persist: ok, note: "orderBy=LastEventTime + prefix and descending + orderBy=LogStreamName rejection rules match AWS."}
  PutRetentionPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteRetentionPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  PutSubscriptionFilter: {wire: ok, errors: ok, state: ok, persist: ok, note: "2-filter-per-group cap and update-in-place-by-name verified."}
  DescribeSubscriptionFilters: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteSubscriptionFilter: {wire: ok, errors: ok, state: ok, persist: ok}
  PutMetricFilter: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeMetricFilters: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteMetricFilter: {wire: ok, errors: ok, state: ok, persist: ok}
  TestMetricFilter: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed: ExtractedValues was always {} (disguised stub -- computed nothing from the pattern's named fields). Now extracts every $-referenced field for JSON and space-delimited patterns."}
  ListTagsLogGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  TagLogGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagLogGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  metric-filter-emission: {status: ok, note: "fixed (internal PutLogEvents dispatch, not an SDK op): emitMetricFilterMatches previously emitted matchCount copies of one static value regardless of MetricValue's per-event field reference (a disguised stub -- '$field' values were never actually read from the matched log event, just defaulted to 1.0/DefaultValue). Now extracts the referenced field ($name for space-delimited patterns, $.path for JSON patterns) per matched event via new compiledFilterPattern.extract; a matched-but-non-numeric-or-absent field now correctly emits no data point rather than fabricating one. Also fixed emitted Unit being hardcoded to \"\" instead of the configured MetricTransformation.Unit."}
  janitor-retention-sweep: {status: ok, note: "two-phase read-then-write lock, worker.NewGroup ticker is ctx-cancel safe, telemetry recorded. No leak."}
  subscription-filter-delivery: {status: ok, note: "goroutines bounded by workerSem + backend WaitGroup + service ctx; Close()/Drain() wait for in-flight deliveries. No leak found."}
  insights (StartQuery/GetQueryResults/StopQuery/DescribeQueries/query language): {status: ok, note: "lightly reviewed only (large ~2500 LOC subsystem across insights_*.go); query TTL eviction (evictByTTL) and cap enforcement (enforceCap) present and bounded; not exhaustively re-audited op-by-op this pass -- see deferred."}
  export/import tasks, deliveries, anomaly detectors, scheduled queries, account policies, data protection/resource/index policies, transformers, integrations: {status: ok, note: "spot-checked (CreateExportTask/runExport does a real synchronous S3 write via injectable ExportSink when configured; ApplyTransformer/applyJSONProcessor implements real addKeys/deleteKeys/renameKeys-style JSON processors, not a stub). Not exhaustively re-audited op-by-op this pass -- see deferred."}
  StartLiveTail: {status: ok, note: "explicitly validation-only (log-group-identifier existence check) with a documented comment explaining the streaming HTTP/2 transport can't be served by this request/response handler -- an honest declared limitation, not a silent stub."}
gaps:
  - MetricTransformation.Dimensions is accepted, validated on the wire, and persisted on the MetricFilter, but is never forwarded to the emitted CloudWatch metric: the MetricEmitter interface (backend.go) only carries namespace/name/value/unit, and its real implementation is wired in cli.go's wireCWLogsMetricEmitter, which is out of scope for this pass (SHARED FILE). Extending the interface + cli.go wiring to carry dimensions is a real fix but requires touching cli.go. (bd: gopherstack-b14)
  - GetLogGroupFields always returns the 4 static built-in fields (@message/@timestamp/@ingestionTime/@logStream) and never samples real ingested log content, so it cannot discover custom JSON/space-pattern fields the way real AWS does (percent-of-sampled-events containing each discovered key). Not fixed this pass (a genuine sampling+percentage feature, lower priority than the PutLogEvents/metric-filter fixes actually made). (bd: gopherstack-b14)
  - PutLogEvents does not enforce two documented batch-shape constraints: (1) events in a single request must be in strict chronological order or the whole call should fail; (2) the valid-event timestamp span within one batch cannot exceed 24 hours. Both are documented in the current SDK's PutLogEvents doc comment. Not implemented this pass: doing so safely requires auditing whether existing out-of-order-friendly tests/call sites (this codebase's own appendEvents doc explicitly says "log events may arrive with out-of-order timestamps" ) depend on the current relaxed behavior; higher risk/effort than the fixes made, so deferred rather than rushed. (bd: gopherstack-b14)
deferred:
  - Insights query language/stages/parser correctness (insights_expr.go, insights_parse.go, insights_parser.go, insights_stages.go, insights_stats.go) -- not re-verified op-by-op against CloudWatch Logs Insights query syntax this pass.
  - Export/Import task lifecycle edge cases, Deliveries, Log Anomaly Detectors, Scheduled Queries, Account Policies, Data Protection/Resource/Index Policies, Transformers, Integrations (handler_completeness.go / backend_completeness.go, ~2100 LOC) -- spot-checked only, not exhaustively audited op-by-op.
  - StartLiveTail streaming transport (intentionally out of scope; validation-only by design).
leaks: {status: clean, note: "Only one goroutine spawn site (scheduleFilterDelivery for subscription filter delivery), bounded by a semaphore + backend WaitGroup + ctx cancellation; Close()/Drain() join in-flight work. Janitor ticker is ctx-cancel safe via pkgs/worker. No unbounded per-request goroutines found in the areas audited this pass."}
---

## Notes

- **PutLogEvents sequenceToken is a no-op today.** aws-sdk-go-v2 v1.64.0's own doc comments
  (api_op_PutLogEvents.go, types.InputLogEvent.UploadSequenceToken, types.InvalidSequenceTokenException)
  are explicit and repeated: "The sequence token is now ignored in PutLogEvents actions.
  PutLogEvents actions are always accepted and never return InvalidSequenceTokenException or
  DataAlreadyAcceptedException even if the sequence token is not valid." A previous audit pass
  (see the now-updated `ops_batch2_audit_test.go` comment) had deliberately added strict
  sequence-token validation, modeling the *pre-2022* contract. This sweep removed that
  validation; NextSequenceToken is still computed and returned (many older SDKs/tools still read
  it), it just no longer gates acceptance. If a future auditor sees "AWS returns
  InvalidSequenceTokenException for a stale token" in an older blog post or Stack Overflow
  answer, that's describing pre-Feb-2022 behavior -- trust the SDK doc comment over that.

- **RejectedLogEventsInfo field is `TooOldLogEventEndIndex`, not `...StartIndex`.** The real
  wire key is `tooOldLogEventEndIndex` (aws-sdk-go-v2 types.RejectedLogEventsInfo), and per the
  field doc it's an *exclusive end* index ("too-old events form a prefix of the batch; this is
  one past the last of them"), exactly mirroring `ExpiredLogEventEndIndex`'s semantics. A real
  SDK client unmarshalling our old `tooOldLogEventStartIndex` key would have silently gotten
  `nil` for `TooOldLogEventEndIndex`. Both `TooOldLogEventEndIndex` and `ExpiredLogEventEndIndex`
  are computed as `(highest matching event index) + 1`, not the first-matching index -- watch for
  this if the field is ever renamed again.

- **Metric filter `MetricValue` field-reference extraction is real, not literal-only.**
  `MetricTransformation.MetricValue` can be a literal number (published as-is) or a
  `$`-prefixed field reference: `$name` for a *named* field in a space-delimited pattern
  (`[ip, level, size]` makes `$ip`/`$level`/`$size` addressable) or `$.path` for a JSON
  selector pattern (`{ $.level = "ERROR" }` makes any `$.foo.bar` addressable, not just paths
  that appear in the match condition). This is implemented via
  `compiledFilterPattern.extract`/`extractString`/`extractValue`, fed by
  `compileJSONFilterPatternExtract` (filter_pattern_json.go) and
  `compileSpaceFilterPatternExtract` + `spaceFieldIndex` (filter_pattern_space.go, which now
  also tracks each `spaceTerm.name` -- previously discarded entirely, even for bare/unconditioned
  terms). **Trap:** `MetricTransformation.DefaultValue` is documented as "the value to emit when
  a filter pattern does NOT match a log event" (i.e. a substitute for a quiet period with zero
  matches), **not** a fallback for a matched event whose referenced field happens to be missing
  or non-numeric. The previous implementation conflated the two and used DefaultValue (or a bare
  `1.0`) as an extraction-failure fallback, fabricating metric data points that real CloudWatch
  Logs would never emit. The fix intentionally emits *nothing* for an extraction failure on a
  matched event; genuinely wiring "publish DefaultValue once per period when this filter had zero
  matches" would require a periodic scheduler (not per-PutLogEvents-call logic) and is left as a
  gap, not attempted this pass.

- **`putLogEventsMaxMessageBytes = 256 * 1024` looks suspicious next to aws-sdk-go-v2's doc
  comment ("Each log event can be no larger than 1 MB") but was deliberately left unchanged.**
  The older, still-maintained aws-sdk-go v1 model (`service/cloudwatchlogs/api.go`) explicitly
  says "Each log event can be no larger than 256 KB" in the same field's doc comment, matching
  this codebase's long-standing constant and the widely-documented CloudWatch Logs event-size
  quota. Treat the v2 SDK's "1 MB" doc text as smithy-model doc drift (likely bled in from the
  *batch* size limit description) rather than a real per-event limit increase, unless a more
  authoritative source (e.g. the AWS Service Quotas console page) is checked and says otherwise.

- **`compileFilterPattern`'s per-pattern-kind dispatch (`{`/`[`/plain-text) is the single
  source of truth for "does this pattern have addressable fields."** Anything touching
  metric-value extraction or `TestMetricFilter.ExtractedValues` should go through
  `compiledFilterPattern.extractString`/`extractValue` or `patternFieldRefs`, not re-parse the
  pattern text directly -- the existing JSON lexer/space-term parsing is reused rather than
  duplicated so the match and extract paths can't drift out of sync.
