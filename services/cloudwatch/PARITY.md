---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: cloudwatch
sdk_module: aws-sdk-go-v2/service/cloudwatch@v1.55.1
last_audit_commit: 58eec068
last_audit_date: 2026-07-05
overall: A            # ~1.1k LOC of genuine fixes (624/223 non-test, ~500 net test) this pass
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  PutMetricData: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass — see Notes: fabricated UnprocessedMetricData wire field removed, all-or-nothing semantics, Values/Counts array support added, NaN/Inf/range validation added"}
  GetMetricStatistics: {wire: ok, errors: ok, state: ok, persist: ok, note: "proven correct: period-aligned buckets, Average/Sum/Min/Max/SampleCount, extended-statistic percentiles via collectRawBuckets, anomaly band annotation"}
  GetMetricData: {wire: ok, errors: ok, state: ok, persist: ok, note: "proven correct: metric-math expressions (topo-sorted), ScanBy asc/desc, MaxDatapoints pagination with resumable cursor, PartialData/ArithmeticError messages, cross-account AccountId returns empty not error"}
  ListMetrics: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass — RecentlyActive=PT3H filter was parsed nowhere (silently ignored); now validated and enforced"}
  PutMetricAlarm: {wire: ok, errors: ok, state: ok, persist: ok}
  PutCompositeAlarm: {wire: ok, errors: ok, state: ok, persist: ok, note: "AlarmRule AND/OR/NOT parsing with cycle + depth-limit detection proven correct"}
  DescribeAlarms: {wire: ok, errors: ok, state: ok, persist: ok, note: "separate MetricAlarms/CompositeAlarms lists, alarmType/stateValue/prefix filters all correct"}
  DescribeAlarmsForMetric: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAlarmHistory: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass — Action-history entries for composite alarms were hardcoded AlarmType=MetricAlarm"}
  DeleteAlarms: {wire: ok, errors: ok, state: ok, persist: ok}
  SetAlarmState: {wire: ok, errors: ok, state: ok, persist: ok, note: "fires actions only on real transition, correct action-list selection per new state, composite re-evaluation cascades"}
  EnableAlarmActions: {wire: ok, errors: ok, state: ok, persist: ok}
  DisableAlarmActions: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  PutDashboard: {wire: partial, errors: ok, state: ok, persist: ok, note: "DashboardValidationMessages field is real (unlike PutMetricData's) but always empty — body is stored verbatim with no JSON/widget-schema validation (bd: gopherstack-3ro)"}
  GetDashboard: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDashboards: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDashboards: {wire: ok, errors: ok, state: ok, persist: ok}
  PutAlarmMuteRule: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAlarmMuteRule: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAlarmMuteRule: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAlarmMuteRule: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAlarmMuteRules: {wire: ok, errors: ok, state: ok, persist: ok}
  PutAnomalyDetector: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAnomalyDetector: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAnomalyDetectors: {wire: ok, errors: ok, state: ok, persist: ok}
  PutInsightRule: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateInsightRule: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteInsightRules: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeInsightRules: {wire: ok, errors: ok, state: ok, persist: ok}
  EnableInsightRules: {wire: ok, errors: ok, state: ok, persist: ok}
  DisableInsightRules: {wire: ok, errors: ok, state: ok, persist: ok}
  GetInsightRuleReport: {wire: ok, errors: ok, state: ok, persist: ok}
  ListManagedInsightRules: {wire: ok, errors: ok, state: ok, persist: ok}
  PutManagedInsightRules: {wire: ok, errors: ok, state: ok, persist: ok}
  PutMetricStream: {wire: ok, errors: ok, state: ok, persist: ok}
  GetMetricStream: {wire: ok, errors: ok, state: ok, persist: ok}
  ListMetricStreams: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateMetricStream: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteMetricStream: {wire: ok, errors: ok, state: ok, persist: ok}
  StartMetricStreams: {wire: ok, errors: ok, state: ok, persist: ok}
  StopMetricStreams: {wire: ok, errors: ok, state: ok, persist: ok}
  PutMetricFilter: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeMetricFilters: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteMetricFilter: {wire: ok, errors: ok, state: ok, persist: ok}
  TestMetricFilter: {wire: ok, errors: ok, state: ok, persist: deferred, note: "stateless preview op, nothing to persist"}
  DescribeAlarmContributors: {wire: ok, errors: ok, state: ok, persist: ok}
  GetMetricWidgetImage: {wire: ok, errors: ok, state: n/a, persist: n/a, note: "rendering-only op; PNG output not byte-compared against real AWS"}
# Families audited as a group (when per-op is impractical):
families:
  alarm-evaluation-state-machine: {status: ok, note: "FIXED this pass — breachesThreshold was missing the LessThanLowerThreshold comparison operator entirely (fell through to default:false, so alarms configured with it never fired). All 4 TreatMissingData modes (missing/notBreaching/breaching/ignore) proven correct in countBreachingPeriods/evaluateMetricAlarmState, including ignore's 'maintain current state when no data' rule and M-of-N DatapointsToAlarm."}
  alarm-action-dispatch: {status: ok, note: "FIXED this pass — composite-alarm action history mistagged AlarmType=MetricAlarm (see DescribeAlarmHistory). SNS/Lambda/EC2-automate/AutoScaling-policy ARN routing, best-effort delivery (failures logged, other actions still run), EC2 InstanceId dimension extraction all proven correct. Actual SNS/Lambda/EC2/ASG client wiring lives in cli.go (out of scope per task boundary) — only the in-package dispatch/selection logic was audited/fixed."}
  error-codes: {status: ok, note: "ResourceNotFoundException/InvalidParameterValue/InvalidParameterCombination/LimitExceeded all HTTP 400 (correct for CloudWatch's query/XML protocol, which never uses 404); InternalFailure is 500. Spot-checked across alarms/dashboards/mute-rules/anomaly-detectors/insight-rules/metric-streams/metric-filters."}
  persistence: {status: ok, note: "backendSnapshot/persistence.go covers metrics, alarms, composite alarms, alarm history, dashboards, anomaly detectors, insight rules, metric streams, alarm mute rules, metric filters; field names unchanged by this pass (MetricDatum gained Values/Counts/Has* fields, additive only, so existing snapshots restore unchanged for the fields they populate)"}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - PutDashboard never validates DashboardBody JSON/widget schema, so DashboardValidationMessages is always empty even for malformed input (bd: gopherstack-3ro)
  - PutMetricData does not enforce AWS's timestamp acceptance window (2 weeks past / 2 hours future) (bd: gopherstack-pyv)
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - widget.go / widget_draw.go / widget_font.go (GetMetricWidgetImage PNG rendering internals — not a wire-shape or state-correctness concern, only visual fidelity)
  - metric-stream Firehose delivery payload format (OutputFormat json/opentelemetry0.7 byte-level shape)
  - insight-rule Definition/Schema JSON-body validation depth (accepted verbatim, like PutDashboard)
leaks: {status: clean, note: "Janitor (janitor.go) owns the single alarm-eval + metric-sweep goroutine, ctx-cancel-aware, StartWorker only spawns it for *InMemoryBackend. storeDatum/filterAlivePoints reslice (not just filter) to release oversized backing arrays (#60 total-metrics counter avoids O(namespaces) walks). No new goroutines/tickers/maps introduced this pass; PutMetricData's validate-then-commit split does not change lock-hold duration character (still one write-lock section)."}
---

## Notes

CloudWatch here speaks **AWS Query (XML) protocol** for the classic SDK path (`Action=` form
POST, `<Foo Response>` root, `ResponseMetadata>RequestId`) and **rpc-v2-cbor** for
`aws-sdk-go-v2/service/cloudwatch@v1.55+` (routed via `X-Amz-Target`/CBOR path,
`/service/GraniteServiceVersion20100801/operation/<Op>`). Every op has two independent encoders
(`handler.go` XML, `rpcv2cbor.go` CBOR) that must be checked separately — a fix in one does not
imply the other is correct.

### The big one: PutMetricData has NO partial-success shape

`aws-sdk-go-v2/service/cloudwatch/api_op_PutMetricData.go`'s `PutMetricDataOutput` struct has
**zero fields** besides `ResultMetadata`. The real query-protocol response body is:

```xml
<PutMetricDataResponse xmlns="...">
  <ResponseMetadata><RequestId>...</RequestId></ResponseMetadata>
</PutMetricDataResponse>
```

There is no `UnprocessedMetricData` — that concept exists for other AWS batch APIs (e.g. SQS
`SendMessageBatch`) but **not** for `PutMetricData`. Before this pass, gopherstack's handler
accepted the whole batch, stored every valid datum, and returned HTTP 200 with a fabricated
`<UnprocessedMetricData>` list describing which entries were "unprocessed" (bad StatisticSet
combos, bad StorageResolution, namespace-cap overflow). A real SDK client would never produce or
parse that field — it isn't in the generated deserializer. This is a **wire-shape bug that also
changes API semantics**: real CloudWatch validates the whole request and either accepts all of it
or rejects all of it with a single API error (a datum-level problem anywhere in the batch fails
the entire call, HTTP 400, nothing gets stored). Fixed by splitting `PutMetricData` into a
validate-the-whole-batch pass (`validatePutMetricDataBatch`, no mutation) and a commit pass, only
reached if validation passes.

**Trap for the next auditor**: don't assume every CloudWatch write op lacks a partial-result
field just because PutMetricData does — `PutDashboard`'s `DashboardValidationMessages` and
`DeleteInsightRules`/`DisableInsightRules`/`EnableInsightRules`/`DeleteMetricFilter`'s per-name
`Failures` list are real, generated-from-model fields. Always check the actual SDK struct
(`grep -n "type <Op>Output struct" -A 20` in the unzipped SDK module) before assuming a
partial-failure shape is fabricated OR before assuming one doesn't exist.

### Values/Counts array (new feature this pass)

`MetricDatum.Values`/`.Counts` (parallel arrays, up to 150 unique values, each with an occurrence
count, default count 1 when `Counts` is omitted) was not parsed at all in either wire path —
neither the form parser nor the CBOR decoder read the `Values`/`Counts` member, so data submitted
this way was silently dropped with no error and no stored datapoints. This is CloudWatch's
mechanism for "publish a distribution, get real percentiles back" without pre-aggregating into a
StatisticSet. Implemented: parsing (form + CBOR), validation (mutual exclusion with Value/
StatisticSet via new `Has{Value,StatisticSet,ValuesArray}` presence flags — presence, not a
"non-zero" check, which is what AWS actually validates), `aggregateValuesCounts` for
Sum/SampleCount/Min/Max (computed once, in `storeDatum`, so every caller — not just the two wire
parsers — gets consistent aggregation), and `expandValuesCounts` for exact percentile
reconstruction (proportionally capped at `maxStatSetExpand` samples, unlike the StatisticSet path
which has to *synthesize* a distribution from only Sum/Min/Max/Count).

### LessThanLowerThreshold silently never fired

`ComparisonOperator` has 7 real values (`aws-sdk-go-v2/service/cloudwatch/types/enums.go`), not 6:
`GreaterThan{,OrEqualTo}Threshold`, `LessThan{,OrEqualTo}Threshold`,
`LessThanLowerOrGreaterThanUpperThreshold`, **`LessThanLowerThreshold`**, and
`GreaterThanUpperThreshold`. `breachesThreshold`'s `switch` had no case for
`LessThanLowerThreshold` and fell through to `default: return false` — any alarm configured with
that anomaly-detection operator would never breach, ever, silently. Distinct from
`LessThanLowerOrGreaterThanUpperThreshold` (which fires on either bound); `LessThanLowerThreshold`
only fires on the lower-bound breach.

### Composite-alarm action history mistagging

`executeActions` hardcoded `alarmTypeName="MetricAlarm"` on every Action-history entry it wrote,
even when firing actions for a **composite** alarm (via `SetAlarmState` on a composite alarm
directly, or via `fireCompositeTransitions` when a child alarm's state change cascades). Since
`DescribeAlarmHistory` filters by `AlarmType`, querying a composite alarm's history with
`AlarmType=CompositeAlarm` would miss its own fired-action entries (they were mistagged as
MetricAlarm), while `AlarmType=MetricAlarm` would incorrectly include them. Fixed by threading the
real alarm-type string through both `executeActions` call sites.

### ListMetrics RecentlyActive

`RecentlyActive` only accepts the literal string `"PT3H"` (AWS's only documented value); anything
else is `InvalidParameterValue`. It restricts results to metrics with at least one datapoint in
the last 3 hours (from *now*, not from the request's implicit time context — there is no
StartTime/EndTime on ListMetrics). Was not parsed in either wire path before this pass, so the
filter was silently a no-op (every metric always returned regardless of the param).

### Already-correct traps (do not re-flag)

- `populateBuckets`/`collectRawBuckets` bucket index is `timestamp.Unix() / period` (aligned to
  Unix-epoch boundaries), which is intentional and matches how CloudWatch aligns period
  boundaries for GetMetricStatistics — it is **not** a bug that buckets aren't aligned to the
  request's StartTime.
- `GetMetricData` cross-account queries (`AccountId` set to a different account) return an empty
  result for that query ID rather than erroring — this is intentional (documented behavior: local
  emulator has no cross-account metrics, but AWS itself just returns nothing rather than failing
  the whole request for an inaccessible-but-valid account).
- `expandDatumValues`'s StatisticSet synthetic expansion (one sample at Min, one at Max, remainder
  at residual mean) looks like it's fabricating data, but it's a deliberate, documented
  approximation for extended-statistic (percentile) queries over StatisticSet-only data, which
  AWS itself can only do exactly when `SampleCount==1` or `Min==Max` (see the PutMetricData SDK
  doc comment) — this is explicitly a best-effort approximation.
- `cwMaxMetricNamesPerNamespace`/`cwMaxTotalMetricRecords` are **emulator-only** memory-safety
  caps, not modeled AWS service quotas — CloudWatch's real per-account metric quotas are not
  synchronously enforced by PutMetricData. Do not "fix" the LimitExceeded error code/behavior
  without first checking whether AWS actually enforces a limit at write time (it does not, for
  the metrics-count quota).
