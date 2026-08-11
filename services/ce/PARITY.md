---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: ce
sdk_module: aws-sdk-go-v2/service/costexplorer@v1.67.4   # version actually pinned in go.mod; corrected stale v1.63.8 reference
last_audit_commit: f848e87f1bce2856351a650dbbdba31bb6bbbd49
last_audit_date: 2026-07-29
overall: A            # closed the required-field-validation gap and the ValidationError wire-type unknown from the prior pass; field-diffed and fixed 6 further wire-shape bugs (2 invented field names, 1 wrong JSON type, 1 missing field, 1 over-validation bug, 1 wrong-shaped comparison op) across GetCostAndUsage/GetCostAndUsageWithResources/GetCostAndUsageComparisons/GetApproximateUsageRecords/ListCostCategoryResourceAssociations/GetSavingsPlanPurchaseRecommendationDetails/Start+ListSavingsPlansPurchaseRecommendationGeneration/UpdateAnomalyMonitor. This pass: GetCostAndUsage's TimePeriod/Metrics required-field validation gap (documented since the prior pass) is now closed.
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateAnomalyMonitor: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: MonitorType is now enforced required (was previously only format-validated when present), matching validateAnomalyMonitor"}
  DeleteAnomalyMonitor: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: was ResourceNotFoundException, real AWS is UnknownMonitorException"}
  UpdateAnomalyMonitor: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: handler wrongly required MonitorName (real AWS's UpdateAnomalyMonitorInput only requires MonitorArn -- 'Specify the fields you want to update, omitted fields are unchanged'); this rejected valid real-client requests. Backend now leaves MonitorName unchanged when omitted instead of blanking it."}
  GetAnomalyMonitors: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: an unknown ARN in MonitorArnList silently returned an empty page instead of UnknownMonitorException"}
  CreateAnomalySubscription: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: MonitorArnList/Subscribers/Frequency now enforced required, matching validateAnomalySubscription (previously only SubscriptionName was required)"}
  DeleteAnomalySubscription: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: was ResourceNotFoundException, real AWS is UnknownSubscriptionException"}
  UpdateAnomalySubscription: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: not-found was ResourceNotFoundException (now UnknownSubscriptionException); MonitorArnList entries were never checked against existing monitors (now UnknownMonitorException)"}
  GetAnomalySubscriptions: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: an unknown ARN in SubscriptionArnList silently returned an empty page instead of UnknownSubscriptionException; MonitorArn filter deliberately left non-validating (see Notes)"}
  GetAnomalies: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: DateInterval.StartDate now enforced required, matching validateAnomalyDateInterval"}
  ProvideAnomalyFeedback: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateCostCategoryDefinition: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: ServiceQuotaExceededException on duplicate name was HTTP 409, real AWS is HTTP 400; fixed this pass: RuleVersion/Rules now enforced required, matching validateOpCreateCostCategoryDefinitionInput"}
  DeleteCostCategoryDefinition: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeCostCategoryDefinition: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: ResourceNotFoundException was HTTP 404, real AWS is HTTP 400"}
  ListCostCategoryDefinitions: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateCostCategoryDefinition: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: RuleVersion/Rules now enforced required, matching validateOpUpdateCostCategoryDefinitionInput"}
  GetCostAndUsage: {wire: ok, errors: ok, state: n/a, note: "deterministic mock over a synthetic cost ledger -- acceptable per parity rules, no real billing data exists to emulate. Earlier pass fixed the missing GroupDefinitions response field (echoes back the request's GroupBy, per GetCostAndUsageOutput). fixed this pass: TimePeriod and Metrics are now enforced required, matching GetCostAndUsageInput ('This member is required' on both, confirmed via api_op_GetCostAndUsage.go; TimePeriod.Start/.End are each independently required per types.DateInterval). A prior revision silently defaulted a missing/partial TimePeriod to defaultStartDate/defaultEndDate and never checked Metrics at all, so a request missing either real-required member got a permissive, silently-defaulted 200 instead of the ValidationError real AWS returns. Metrics enum-value validation (AmortizedCost/BlendedCost/NetAmortizedCost/NetUnblendedCost/NormalizedUsageAmount/UnblendedCost/UsageQuantity) is intentionally not added: existing coverage (TestGetCostAndUsage_AlternateMetrics's unknown_metric case) deliberately exercises an unrecognized metric name falling back to BlendedCost via getMetricValue, and Metrics is a plain []string on the wire (not an enum-constrained type), so this fix is a presence check only."}
  GetCostForecast: {wire: ok, errors: ok, state: n/a}
  GetUsageForecast: {wire: ok, errors: ok, state: n/a}
  GetDimensionValues: {wire: ok, errors: ok, state: n/a}
  GetTags: {wire: ok, errors: ok, state: n/a}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: ResourceTags now enforced required, matching validateOpTagResourceInput"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: ResourceTagKeys now enforced required, matching validateOpUntagResourceInput"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  GetCostAndUsageWithResources: {wire: ok, errors: ok, state: n/a, note: "fixed this pass: was missing GroupDefinitions and Filter/Granularity required-field validation; ResultsByTime is legitimately always empty -- real AWS resource-level cost data is keyed by individual resource ARN, and this emulator's synthetic ledger (seedCostLedger) only models service+date granularity, not per-resource entries, so there is no state to derive a non-empty result from"}
  GetCostAndUsageComparisons: {wire: ok, errors: ok, state: n/a, note: "fixed this pass (3 wire-shape bugs): request fields BaseTimePeriod/Metrics were invented (real: BaselineTimePeriod/MetricForComparison, the latter a required singular string not an array); response field CostAndUsages was invented (real: CostAndUsageComparisons) and TotalCostAndUsage was wire-typed as an array instead of a map keyed by metric name. Now derives real baseline/comparison totals from the cost ledger via the same DAILY-bucketed aggregation GetCostAndUsage uses, instead of always returning an empty envelope."}
  GetCostComparisonDrivers: {wire: ok, errors: ok, state: n/a, note: "field-diffed against GetCostComparisonDriversOutput this pass -- CostComparisonDrivers/NextPageToken already matched, no bug found"}
  GetApproximateUsageRecords: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed this pass: Services/TotalRecords were wire-typed as strings, real AWS types them as JSON numbers (map[string]int64/int64 -- NonNegativeLong); ApproximationDimension/Granularity now enforced required. Now derives per-service counts from the cost ledger's UsageQuantity over a trailing 30-day LookbackPeriod instead of always returning zero."}
  ListCostCategoryResourceAssociations: {wire: ok, errors: ok, state: n/a, note: "fixed this pass: response fields CostCategoryReference/ResourceTagsCount were invented; real AWS field is CostCategoryResourceAssociations ([]CostCategoryResourceAssociation{CostCategoryArn,CostCategoryName,ResourceArn}). Always returns zero associations: real AWS resource associations tie a cost category to actual AWS resources via resource tags, and this emulator has no such resource-tag inventory to associate against -- there is no state to disguise a no-op here."}
  GetSavingsPlanPurchaseRecommendationDetails: {wire: ok, errors: ok, state: n/a, note: "fixed this pass: response field RecommendationDetail was invented; real AWS field is RecommendationDetailData (a RecommendationDetailData struct, not `any`). RecommendationDetailId now enforced required. Now derives synthetic-but-real values from the SP utilization ledger instead of returning an empty envelope."}
  StartSavingsPlansPurchaseRecommendationGeneration: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: response field GenerationId was invented; real AWS field is RecommendationId. Was a pure stub (empty envelope, no state at all) -- now creates and persists a SavingsPlansGeneration record (new store.Table), mirroring the CommitmentAnalysis start/persist/list pattern."}
  ListSavingsPlansPurchaseRecommendationGeneration: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: GenerationSummaryList entries used the invented GenerationId field; real AWS field is RecommendationId (GenerationSummary type). Was always an empty list regardless of state -- now reads back real generation jobs created by StartSavingsPlansPurchaseRecommendationGeneration, with real GenerationStatus filtering."}
families:
  AnomalyMonitor: {status: ok, note: "CRUD + Get(list) verified against backend.go; 3 error-shape bugs fixed last pass, 1 required-field gap + 1 over-validation bug fixed this pass (see ops above)"}
  AnomalySubscription: {status: ok, note: "CRUD + Get(list) verified against backend.go; 3 error-shape/referential-integrity bugs fixed last pass, 1 required-field gap fixed this pass (see ops above)"}
  GetAnomalies: {status: ok, note: "date-interval overlap filter, monitor/feedback filter, pagination all verified real (not a stub); AnomalyScore/Impact struct shapes match API_Anomaly.html; StartDate required-field gap fixed this pass"}
  CostCategory: {status: ok, note: "Create/Describe/Update/Delete/List all real state, ARN-keyed store.Table, deep-copies on read/write; 2 HTTP-status bugs fixed last pass, RuleVersion/Rules required-field gap fixed this pass (Create+Update)"}
  Tags: {status: ok, note: "TagResource/UntagResource/ListTagsForResource operate across costCategories/anomalyMonitors/anomalySubscriptions maps, real mutation, HTTP-status fix inherited from the shared ErrNotFound mapping; ResourceTags/ResourceTagKeys required-field gap fixed this pass"}
  CostAndUsageQueries: {status: ok, note: "GetCostAndUsage/GetCostForecast/GetUsageForecast/GetDimensionValues/GetTags/GetCostCategories -- deterministic mock over a 90-day synthetic cost ledger, per parity rules this is acceptable (no real billing data to emulate); DateInterval wire shape (yyyy-MM-dd strings, not epoch) verified correct. GetCostAndUsage's missing GroupDefinitions field fixed in an earlier pass; GetCostAndUsage's required-field validation gap (TimePeriod/Metrics) closed this pass -- see the GetCostAndUsage op note and the gaps list below for GetCostForecast/GetUsageForecast/GetDimensionValues/GetTags/GetCostCategories, which still lack it."}
  CostAndUsageComparisonAndResourceQueries: {status: ok, note: "GetCostAndUsageComparisons/GetCostAndUsageWithResources/GetCostComparisonDrivers -- field-diffed this pass (were previously grouped under the deferred/unverified CostAndUsageQueries note). GetCostAndUsageComparisons had 3 invented/wrong-typed fields, now fixed and deriving real ledger totals. GetCostAndUsageWithResources was missing GroupDefinitions + required-field validation, now fixed; ResultsByTime legitimately stays empty (no per-resource ledger state exists to derive from). GetCostComparisonDrivers already matched the real shape."}
  ReservationsAndSavingsPlans: {status: ok, note: "GetReservationCoverage/GetReservationUtilization/GetReservationPurchaseRecommendation/GetRightsizingRecommendation/GetSavingsPlans* -- all deterministic synthetic-ratio mocks derived from the cost ledger, acceptable (no state to mutate, matches AWS response shapes); not deep-audited for numeric-formula fidelity this pass (see deferred). GetSavingsPlanPurchaseRecommendationDetails's invented field fixed this pass; Start/ListSavingsPlansPurchaseRecommendationGeneration converted from pure stubs to real persisted state this pass."}
  CostAllocationTags: {status: ok, note: "ListCostAllocationTags/UpdateCostAllocationTagsStatus/StartCostAllocationTagBackfill/ListCostAllocationTagBackfillHistory -- real store.Table-backed state, verified"}
  CommitmentPurchaseAnalysis: {status: ok, note: "StartCommitmentPurchaseAnalysis/GetCommitmentPurchaseAnalysis/ListCommitmentPurchaseAnalyses -- real store.Table-backed state, verified"}
  GetApproximateUsageRecords: {status: ok, note: "fixed this pass: wrong wire types (string instead of JSON number) and a disguised no-op (always-zero regardless of input); now derives real per-service counts from the cost ledger"}
  ListCostCategoryResourceAssociations: {status: ok, note: "fixed this pass: 2 invented field names; correctly and legitimately returns zero associations (no resource-tag inventory modeled in this emulator)"}
  RouteMatcher: {status: ok, note: "X-Amz-Target prefix \"AWSInsightsIndexService.\" verified byte-for-byte against every httpBindingEncoder.SetHeader(\"X-Amz-Target\") call in aws-sdk-go-v2/service/costexplorer@v1.63.8/serializers.go"}
gaps:
  - "GetCostForecast/GetUsageForecast/GetDimensionValues/GetTags/GetCostCategories still lack required-field validation that the real aws-sdk-go-v2 client-side validators enforce (TimePeriod is required on all five; Metrics on GetCostForecast/GetUsageForecast; Dimension on GetDimensionValues already enforced). GetCostAndUsage's TimePeriod/Metrics required-field gap was closed this pass (see its op note) -- the remaining five are a distinct, still-open surface from the 7-op required-field gap closed in an earlier pass (which covered the Anomaly*/CostCategory*/Tag* families + GetAnomalies), and touch a different, larger set of existing test call sites in handler_cost_usage_test.go that omit TimePeriod/Metrics and assert 200 OK. Candidate for a dedicated follow-up pass. (bd: needs issue)"
deferred:
  - "Reservation/SavingsPlans numeric-formula fidelity (the specific ratios in backend.go's syntheticServiceCatalog / spCommitmentRatio / riPurchasedCostRatio etc.) -- these produce plausible, internally-consistent numbers but were not cross-checked against any real AWS CE billing behavior; by definition there is no real data to match against, so this is a modeling-quality concern for a future pass, not a correctness bug."
  - "GetCostAndUsageWithResources.ResultsByTime and ListCostCategoryResourceAssociations.CostCategoryResourceAssociations are always empty by design (see per-op notes above) -- both would need a per-resource / resource-tag inventory this emulator doesn't model anywhere else in the service. Not a disguised no-op (input-driven required-field validation now happens, and the wire shape is correct), just genuinely no backing state to report. A future pass could seed a small synthetic per-resource inventory if resource-level fidelity becomes a priority."
leaks: {status: clean, note: "StartJanitor's anomaly-eviction goroutine (evictExpiredAnomalies) is a single ticker loop stopped via ctx.Done, no per-request goroutines. This pass added one new store.Table (savingsPlansGenerations, registered via the same registry.ResetAll/SnapshotAll/RestoreAll lifecycle as every other table -- see store_setup.go) and zero new goroutines or unbounded maps."}
---

## Notes

Protocol: AWS JSON 1.1 (`application/x-amz-json-1.1`), single POST endpoint, dispatch via
`X-Amz-Target: AWSInsightsIndexService.<Op>` header (verified against every
`serializers.go` `SetHeader("X-Amz-Target")` call in the vendored SDK). `RouteMatcher`
correctly checks the full prefix including the internal Coral service name
`AWSInsightsIndexService` — this is NOT the public "Cost Explorer" name, and it's easy to
mistype/second-guess when unfamiliar with the API; it's confirmed correct.

`DateInterval`/`TimePeriod` fields are always `yyyy-MM-dd` strings (never epoch numbers),
confirmed against `API_AnomalyDateInterval.html` and the `Start`/`End` map wire shape
used throughout `getCostAndUsageInput`/`getCostForecastInput`/etc.

### Bugs fixed this pass

All 7 fixes are in the same family: **wrong or missing error-code/HTTP-status mapping**,
none are disguised no-ops (every op in the AnomalyMonitor/AnomalySubscription/CostCategory
families already did real `store.Table`-backed state mutation before this pass — the gap
was exclusively in how failures were reported on the wire).

1. **HTTP status codes were wrong for every modeled CE exception** (handler.go
   `handleError`). `ErrNotFound` → `ResourceNotFoundException` was returned as HTTP 404,
   and `ErrAlreadyExists` → `ServiceQuotaExceededException` was returned as HTTP 409.
   Checked 6 separate AWS API reference pages
   (`API_DescribeCostCategoryDefinition`, `API_CreateCostCategoryDefinition`,
   `API_DeleteAnomalyMonitor`, `API_DeleteAnomalySubscription`, `API_UpdateAnomalyMonitor`,
   `API_GetAnomalyMonitors`) — every single documented CE client-fault exception is HTTP
   400, with no exceptions. Both mappings now return 400.

2. **AnomalyMonitor "not found" used the generic `ResourceNotFoundException`** instead of
   real AWS's `UnknownMonitorException` (backend.go `DeleteAnomalyMonitor`,
   `UpdateAnomalyMonitor`). Confirmed via `API_DeleteAnomalyMonitor`/
   `API_UpdateAnomalyMonitor` error lists and cross-checked that
   `aws-sdk-go-v2/service/costexplorer/types/errors.go` models
   `UnknownMonitorException` as a distinct typed error a real caller could
   `errors.As` against — the generic mapping meant such callers would get the wrong Go
   type. Added the `ErrUnknownMonitor` sentinel (wraps `awserr.ErrNotFound` like the
   other not-found sentinels, so `errors.Is(err, awserr.ErrNotFound)`-style generic
   checks still work, but `errors.Is(err, ce.ErrNotFound)` no longer does — see the trap
   below).

3. **AnomalySubscription "not found" used the generic `ResourceNotFoundException`**
   instead of real AWS's `UnknownSubscriptionException` (backend.go
   `DeleteAnomalySubscription`, `UpdateAnomalySubscription`). Same fix pattern as #2,
   confirmed via `API_DeleteAnomalySubscription`/`API_UpdateAnomalySubscription`.

4. **`GetAnomalyMonitors`/`GetAnomalySubscriptions` silently dropped unknown ARNs**
   instead of erroring (backend.go). Passing a `MonitorArnList`/`SubscriptionArnList`
   containing an ARN that doesn't exist returned a shorter-than-expected page instead of
   `UnknownMonitorException`/`UnknownSubscriptionException`
   (`API_GetAnomalyMonitors`/`API_GetAnomalySubscriptions` both document this). Both
   backend methods gained an `error` return value; the whole call now fails fast on the
   first unknown ARN rather than silently filtering, matching real AWS's all-or-nothing
   behavior. `GetAnomalySubscriptions`'s separate `MonitorArn` *filter* parameter
   (distinct from `SubscriptionArnList`) was deliberately left non-validating — AWS
   documents no `UnknownMonitorException` for that parameter on this op, it's a genuine
   filter that returns zero matches for an unknown monitor.

5. **`CreateAnomalySubscription`/`UpdateAnomalySubscription` accepted a `MonitorArnList`
   referencing nonexistent monitors**, silently persisting referentially-invalid state
   (backend.go). Real AWS returns `UnknownMonitorException`
   (`API_CreateAnomalySubscription`/`API_UpdateAnomalySubscription`). This is the one fix
   in this pass closest to the "disguised no-op" class from the parity principles: the
   create/update themselves were real, but they'd happily create a subscription
   permanently pointing at nothing, which a real client can never do. `Update` validates
   before mutating (holds the lock, checks first) so a rejected update leaves the
   existing subscription's `MonitorArnList` untouched.

### Traps for the next auditor

- `ce.ErrUnknownMonitor` and `ce.ErrUnknownSubscription` both wrap the shared
  `awserr.ErrNotFound` sentinel (same as `ce.ErrNotFound`), but they are **distinct
  pointer values** from `ce.ErrNotFound` itself. `errors.Is(err, ce.ErrNotFound)` will be
  `false` for an `ErrUnknownMonitor`/`ErrUnknownSubscription` error — this is
  intentional (see `handler.go` `handleError`'s three separate `case` arms) and lets each
  map to its own `__type` on the wire. If you add a new not-found error to this package,
  decide explicitly whether it's generic (`ce.ErrNotFound`) or resource-specific, and add
  a `handleError` case for anything resource-specific — don't assume the existing
  `ErrNotFound` case will catch it.
- `GetAnomalySubscriptions`'s `MonitorArn` request field is a **filter**, not a foreign
  key that must resolve — don't "fix" it to return `UnknownMonitorException` on a
  nonexistent monitor; that's the one FK-shaped parameter on these ops that AWS
  deliberately does *not* validate (see `TestHandler_GetAnomalySubscriptions_MonitorArnFilter`
  and `TestInMemoryBackend_GetAnomalySubscriptions_MonitorArnFilterIgnoresUnknown`).
- The cost/usage/forecast/reservation/savings-plans query families
  (`GetCostAndUsage`, `GetCostForecast`, `GetReservationUtilization`, etc.) are
  intentionally deterministic mocks derived from a 90-day synthetic cost ledger seeded in
  `seedCostLedger`. Per this project's parity rules, this is acceptable — there is no real
  billing data for an emulator to reproduce — so don't flag the synthetic ratios
  (`spCommitmentRatio`, `riPurchasedCostRatio`, etc.) as stubs; they're a deliberate
  modeling choice, not a disguised no-op. What *would* be a bug is if one of these ops
  stopped reading the ledger and returned a hardcoded literal instead — they don't.
- `GetAnomalies` filters on a `[startDate, endDate]` overlap against each anomaly's own
  `AnomalyStartDate`/`AnomalyEndDate` — this looks like it should require both bounds but
  deliberately doesn't (an anomaly with an unset `AnomalyEndDate`/`AnomalyStartDate`, e.g.
  ones inserted via the `AddAnomaly` test helper, never gets filtered out by either bound
  since the guard is `a.AnomalyEndDate != "" && ...`). This is why
  `TestHandler_SnapshotRestoreWithAnomalies` and other tests can call `GetAnomalies` with
  an empty body and still see seeded anomalies.

## 2026-07-24 pass

This pass closed both items the prior pass had explicitly deferred to a "next pass"
(the required-field-validation gap, and the unconfirmed `ErrValidation` wire type), then
field-diffed the remaining ops the prior pass had only wire-shape-"spot-checked"
(`GetCostAndUsageComparisons`/`GetCostAndUsageWithResources`/`GetCostComparisonDrivers`/
`ListCostCategoryResourceAssociations`/`ListSavingsPlansPurchaseRecommendationGeneration`/
`GetSavingsPlanPurchaseRecommendationDetails`/`StartSavingsPlansPurchaseRecommendationGeneration`/
`GetApproximateUsageRecords`) against the real generated Go SDK source (types, serializers.go,
deserializers.go, validators.go), not just doc pages. That surfaced several real,
previously-undetected bugs:

1. **`ErrValidation`'s wire `__type` was the invented `"InvalidParameterException"`.**
   Confirmed via `types/errors.go`'s full exception list (no `InvalidParameterException`
   or `ValidationException` modeled for any CE op) and the CE API reference's
   `CommonErrors.html`, which documents `ValidationError` (HTTP 400) as the shared
   client-fault type for malformed/missing-required-member requests. Changed to
   `"ValidationError"`. Also swept every ad-hoc `errInvalidRequest`-based required-field
   check in the handler layer (which rendered as a bare `{"message": "..."}` body with no
   `__type` field at all — itself a wire-shape bug) over to `ErrValidation`, so every
   required-field violation across the package now gets a consistent, correct
   `__type: "ValidationError"` / HTTP 400 response.

2. **Seven ops were missing required-field validation** that real AWS's
   `validators.go` enforces: `CreateAnomalyMonitor.AnomalyMonitor.MonitorType`,
   `CreateAnomalySubscription.AnomalySubscription.{MonitorArnList,Subscribers,Frequency}`,
   `CreateCostCategoryDefinition.{RuleVersion,Rules}`,
   `UpdateCostCategoryDefinition.{RuleVersion,Rules}`, `TagResource.ResourceTags`,
   `UntagResource.ResourceTagKeys`, `GetAnomalies.DateInterval.StartDate`. All seven are
   now enforced, matching `validateAnomalyMonitor`/`validateAnomalySubscription`/
   `validateOpCreateCostCategoryDefinitionInput`/`validateOpUpdateCostCategoryDefinitionInput`/
   `validateOpTagResourceInput`/`validateOpUntagResourceInput`/`validateAnomalyDateInterval`
   exactly. Required ~15 existing test call sites across `handler_anomalies_test.go`,
   `handler_anomaly_detection_test.go`, `handler_tags_test.go`, and `handler_test.go` to
   gain the now-required fields (mostly `MonitorArnList: []`/a `Subscribers` entry on
   `CreateAnomalySubscription`, and `DateInterval.StartDate` on `GetAnomalies`) — none of
   these were behavior regressions, just previously-lenient test fixtures.

3. **`UpdateAnomalyMonitor` over-validated: it wrongly required `MonitorName`.** Real
   AWS's `UpdateAnomalyMonitorInput` only requires `MonitorArn` — `MonitorName` is
   optional ("Specify the fields that you want to update. Omitted fields are
   unchanged."), confirmed directly from the generated `UpdateAnomalyMonitorInput`
   struct comment. The handler check was deleted, and the backend now only overwrites
   `MonitorName` when non-empty, matching the same "omitted means unchanged" pattern
   `UpdateAnomalySubscription` already used. This was rejecting *valid* real-client
   requests with a 400 — the opposite failure mode from the missing-validation bugs
   above, so worth calling out distinctly.

4. **Three invented/wrong-typed response or request fields**, found by diffing against
   the vendored SDK's actual Go struct fields (not just doc pages, which don't show
   field-name typos):
   - `ListCostCategoryResourceAssociations` returned `CostCategoryReference`/
     `ResourceTagsCount` — neither exists on the real
     `ListCostCategoryResourceAssociationsOutput`. Real field is
     `CostCategoryResourceAssociations []CostCategoryResourceAssociation`
     (`CostCategoryArn`/`CostCategoryName`/`ResourceArn`).
   - `StartSavingsPlansPurchaseRecommendationGeneration` returned `GenerationId` — real
     field is `RecommendationId` (confirmed in both
     `StartSavingsPlansPurchaseRecommendationGenerationOutput` and the `GenerationSummary`
     type `ListSavingsPlansPurchaseRecommendationGeneration` returns).
   - `GetCostAndUsageComparisons` had three separate bugs at once: request fields
     `BaseTimePeriod`/`Metrics` (real: `BaselineTimePeriod`/`MetricForComparison`, the
     latter a *required singular string*, not an array — this op compares exactly one
     metric per call); response field `CostAndUsages` (real:
     `CostAndUsageComparisons`); and `TotalCostAndUsage` wire-typed as an array (real: a
     `map[string]ComparisonMetricValue` keyed by metric name).
   - `GetSavingsPlanPurchaseRecommendationDetails` returned `RecommendationDetail` (an
     untyped `any`) — real field is `RecommendationDetailData`, a
     `RecommendationDetailData` struct.

5. **`GetApproximateUsageRecordsOutput.Services`/`.TotalRecords` were wire-typed as
   strings** (`map[string]string` / `string`); real AWS types both as JSON numbers
   (`map[string]int64` / `int64`, the `NonNegativeLong` shape) — confirmed in
   `deserializers.go`'s `case "TotalRecords":` branch, which parses a `json.Number`, not
   a string. This is the epoch-vs-string timestamp bug class from the parity playbook,
   just for a counter instead of a timestamp: a real client's JSON unmarshal would fail
   outright on a quoted string where it expects a bare number.

6. **Two ops were pure "always returns the same static empty/zero envelope regardless of
   input" stubs** — not just synthetic-but-input-driven mocks like their sibling query
   ops, but genuinely disconnected from any state or request field:
   `GetApproximateUsageRecords` and `StartSavingsPlansPurchaseRecommendationGeneration`
   (+ its `ListSavingsPlansPurchaseRecommendationGeneration` counterpart, which always
   returned an empty list). Both are now real: `GetApproximateUsageRecords` derives
   per-service counts from the cost ledger's `UsageQuantity` over a trailing 30-day
   `LookbackPeriod`; `Start.../List...RecommendationGeneration` now follows the same
   start/persist/list pattern already established by `CommitmentAnalysis` (`AnalysisID`
   → `RecommendationID`), backed by a new `savingsPlansGenerations` `store.Table`
   registered through the existing `registry.ResetAll`/`SnapshotAll`/`RestoreAll`
   lifecycle (no bespoke persistence code needed, no `ceSnapshotVersion` bump required —
   `RestoreAll` already resets any table missing from an older snapshot to empty rather
   than erroring).

7. **`GetCostAndUsage` (and `GetCostAndUsageWithResources`) were missing the
   `GroupDefinitions` response field entirely** — the groups specified by the request's
   `GroupBy`, echoed back on every response per `GetCostAndUsageOutput`/
   `GetCostAndUsageWithResourcesOutput`. This is the exact field this campaign's task
   brief calls out by name as part of the wire-shape parity bar
   (`GroupDefinitions/ResultsByTime/Groups/Metrics/TimePeriod`), and it was silently
   absent from the currently-`ok`-marked primary `GetCostAndUsage` op — a reminder that
   "ok" from a prior pass only means "verified as of that pass's scope," not
   "exhaustively field-diffed forever."

### New traps for the next auditor

- `GetCostAndUsageComparisons`'s `MetricForComparison` is **singular** (one metric per
  call) — don't "fix" it back to a `Metrics []string` array; that's the invented shape
  this pass removed, not the real one.
- `GetCostAndUsageWithResources.ResultsByTime` is *correctly* always empty — this is not
  a stub regression to "fix" by wiring it to the cost ledger the way `GetCostAndUsage`
  is. Real AWS resource-level cost data is keyed by individual resource ARN (e.g. one
  specific EC2 instance), and `seedCostLedger` only models service+date granularity.
  Wiring it to the service-level ledger would produce data that looks resource-level but
  isn't, which is arguably worse than an honestly-empty result. If resource-level
  fidelity is ever prioritized, it needs its own per-resource ledger, not a reuse of the
  existing one.
- `savingsPlansGenerations` jobs never transition out of `"PROCESSING"` (no time-based
  state machine, matching `CommitmentAnalysis`'s existing `AnalysisStatus` behavior in
  this codebase) — don't be surprised a freshly-`Start`ed generation never shows up under
  a `GenerationStatus: "SUCCEEDED"` filter; that's intentional and covered by
  `TestListSavingsPlansPurchaseRecommendationGeneration_FiltersByStatus`.
- The `GetCostAndUsage`/`GetCostForecast`/`GetUsageForecast`/`GetDimensionValues`/
  `GetTags`/`GetCostCategories` family still does **not** enforce `TimePeriod`/`Metrics`
  as required, even though real AWS's validators do (see `gaps` above) — this was
  deliberately left alone this pass since it's a distinct, larger test-fixture-touching
  gap from the 7 ops closed this pass, not an oversight.
