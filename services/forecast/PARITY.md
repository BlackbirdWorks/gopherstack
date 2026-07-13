---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: forecast
sdk_module: aws-sdk-go-v2/service/forecast@v1.42.0
last_audit_commit: 987784da
last_audit_date: 2026-07-13
overall: A            # genuine fixes found: TagResource/UntagResource/ListTagsForResource
                       # missing existence check; List* missing NextToken validation
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass — now 404s on unknown ARN (see gaps: none remaining)"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass — now 404s on unknown ARN"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass — now 404s on unknown ARN"}
  GetAccuracyMetrics: {wire: ok, errors: ok, state: ok, persist: n/a, note: "deterministic synthetic metrics, verified in prior pass — not touched this pass"}
  DeleteResourceTree: {wire: ok, errors: ok, state: ok, persist: ok}
  StopResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ResumeResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListMonitorEvaluations: {wire: ok, errors: ok, state: ok, persist: ok}
# Families audited as a group (when per-op is impractical):
families:
  DatasetGroup: {status: ok, note: "Create/Describe/Update/Delete/List verified; CREATE_PENDING->ACTIVE on first Describe; Update replaces DatasetArns wholesale (correct, not merged)"}
  Dataset: {status: ok, note: "Create/Describe/Delete/List verified; Schema/DataFrequency/Domain field retention correct"}
  DatasetImportJob: {status: ok, note: "S3Config.Path required -> CREATE_FAILED on missing path, matches known emulator convention documented in handler_audit1_test.go"}
  Predictor: {status: ok, note: "Create/Describe/Delete/List + CreateAutoPredictor/DescribeAutoPredictor verified; PerformAutoML/PerformHPO/HyperParameterTuningJobConfig retained"}
  Forecast: {status: ok, note: "Create/Describe/Delete/List verified; epoch-seconds CreationTime/LastModificationTime via awstime.Epoch"}
  "ForecastExportJob/PredictorBacktestExportJob/ExplainabilityExport/WhatIfAnalysis/WhatIfForecast/WhatIfForecastExport/Monitor/Explainability":
    status: ok
    note: "generic addCRUD-driven lifecycle (Create/Describe/List/Delete) shares the same describe()/list()/delete() backend paths already verified for the higher-traffic families; no per-family divergence found"
  ListOperations_Pagination: {status: ok, note: "fixed this pass — malformed NextToken now returns InvalidNextTokenException instead of silently restarting from page 0 (page.ValidateToken wired into listOutput)"}
  Tags: {status: ok, note: "fixed this pass — Tag/Untag/ListTagsForResource now validate the ARN exists via arnIndex before mutating/reading tag state"}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - >-
    No cross-resource FK/state validation on Create*: CreateForecast accepts any
    PredictorArn string without checking the predictor exists or is ACTIVE;
    CreateDatasetImportJob accepts any DatasetArn; CreateForecastExportJob accepts
    any ForecastArn; etc. Real AWS returns ResourceNotFoundException for a
    dangling reference (and arguably ResourceInUseException / InvalidInputException
    for a referenced resource that hasn't reached ACTIVE). NOT fixed this pass:
    the existing test suite (handler_test.go, parity_a/c_test.go, handler_audit1_test.go)
    deliberately uses placeholder non-ARN strings like "predictor"/"forecast" as
    FK values across ~15 test cases, so adding FK validation is a cross-cutting
    change that would need to rewrite most of the existing test fixtures in the
    same pass — out of scope for a surgical bug-fix pass. Needs a dedicated pass.
  - >-
    No enum validation on Domain (CreateDataset/CreateDatasetGroup), DatasetType,
    DataFrequency, ImportMode, or other AWS-modeled enum fields — any string is
    accepted where AWS would return InvalidInputException for a value outside the
    enum. Only resource *names* are validated (charset + 256-char max).
  - >-
    Delete* never returns ResourceInUseException for a resource that still has
    dependents (e.g. deleting a DatasetGroup that still has Datasets, or a
    Predictor that still has Forecasts) — delete always succeeds if the resource
    exists. DeleteResourceTree is the only op that models the AWS
    cascade-delete-children behavior; the single-resource Delete* ops do not
    check for dependents at all.
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - CreateDataset/CreateDatasetGroup Domain and other enum-field validation
  - Cross-resource FK existence/state validation on Create*
  - Delete* ResourceInUseException-on-dependents modeling
leaks: {status: clean, note: "no goroutines/janitors in this service; Reset()/Snapshot()/Restore() all take b.mu correctly; no lock held across a call that could deadlock"}
---

## Notes

- Protocol: awsjson1.1, single POST endpoint, `X-Amz-Target: AmazonForecast.<Op>`.
  Verified against real SDK generated code (aws-sdk-go-v2/service/forecast@v1.42.0):
  target prefix `"AmazonForecast."` matches `newServiceMetadataMiddleware_op*`
  registrations. RouteMatcher/ExtractOperation in handler.go are correct.

- Status lifecycle: this emulator uses a lazy-transition model — a resource is
  created in `CREATE_PENDING` (or `CREATE_FAILED` for DatasetImportJob when
  S3Config.Path is empty) and flips to `ACTIVE` the *first time* `Describe*` is
  called on it (`InMemoryBackend.describe` in backend.go). This looks like it
  skips `CREATE_IN_PROGRESS` entirely, but it is intentional and does NOT hang a
  polling client: the first poll observes `CREATE_PENDING`, every subsequent
  poll observes `ACTIVE`. This is a "looks-wrong-but-correct" trap — do not
  "fix" it by adding a `CREATE_IN_PROGRESS` state without checking
  handler_audit1_test.go's `TestAudit1_Forecast_StatusTransitions` and the
  `TestHandler_ResourceLifecycles` table in handler_test.go first, both of
  which assert exactly this two-poll transition.

- Real bugs fixed this pass (see `git diff` for `services/forecast/{backend,handler}.go`):
  1. `TagResource`/`UntagResource`/`ListTagsForResource` (backend.go) accepted
     any ARN string, including ones that never identified a created resource —
     silently wrote/read an orphaned entry in the `tags` map instead of
     returning `ResourceNotFoundException`. Real AWS models
     `ResourceNotFoundException` on all three ops (confirmed against
     `deserializers.go` in the SDK module: `awsAwsjson11_deserializeOpErrorTagResource`
     / `...UntagResource` / `...ListTagsForResource` all switch on
     `ResourceNotFoundException`). Fixed by checking `b.arnIndex` before
     mutating/reading tag state.
  2. `List*` operations (all families, via the shared `listOutput` in
     handler.go) never validated `NextToken` — a malformed token silently
     decoded to page offset 0 and restarted pagination instead of erroring.
     Real AWS models `InvalidNextTokenException` on every List operation
     (confirmed in `deserializers.go`). Fixed by calling
     `pkgs/page.ValidateToken` in `listOutput` and returning the new
     `ErrInvalidNextToken` sentinel, mapped to 400 `InvalidNextTokenException`
     in `Handler.handleError` — following the same sentinel-per-error-type
     pattern already used for `ErrNotFound`/`ErrAlreadyExists`/`ErrValidation`
     (and mirroring `services/polly`'s existing `ErrInvalidNextToken` handling).

- Persistence: `Handler.Snapshot`/`Restore` already delegate to
  `InMemoryBackend.Snapshot`/`Restore` (persistence.go), which uses
  `store.Registry` for the per-kind resource tables and persists the raw
  `evaluations`/`tags` maps directly; `arnIndex` is deliberately NOT persisted
  and is rebuilt from the restored tables (`rebuildARNIndex`). This was already
  wired correctly before this pass — no persistence gap found for the two ops
  fixed above (tags round-trip through the persisted `Tags` map; the
  ARN-existence check added this pass uses the always-rebuilt `arnIndex`, so
  it works identically pre- and post-restore).
