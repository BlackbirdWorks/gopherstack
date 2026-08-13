---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: forecast
sdk_module: aws-sdk-go-v2/service/forecast@v1.44.4
last_audit_commit: 80757023
last_audit_date: 2026-08-13
overall: A            # 2026-08-13: closed gopherstack-wl0s (required-presence validation):
                       # CreateExplainability's ExplainabilityConfig; CreateForecastExportJob's,
                       # CreatePredictorBacktestExportJob's, CreateExplainabilityExport's, and
                       # CreateWhatIfForecastExport's shared Destination; and CreatePredictor's
                       # ForecastHorizon/InputDataConfig/FeaturizationConfig (three fields, not
                       # the two the originating audit named) were stored and echoed via the
                       # generic-CRUD cloneMap passthrough but never required present. All are
                       # now enforced via requiredPresenceFields in validation.go, keyed by
                       # action name (not resourceKind) because CreatePredictor and
                       # CreateAutoPredictor share kindPredictor but have different required
                       # fields. See "Required-presence validation on Create*" note below.
                       # 2026-08-10: closed gopherstack-4vpt (nested FK existence validation):
                       # CreatePredictor's InputDataConfig.DatasetGroupArn, CreateAutoPredictor's
                       # DataConfig.DatasetGroupArn, and CreateDatasetGroup/UpdateDatasetGroup's
                       # DatasetArns list now resolve against the backend before mutating state.
                       # See "Nested/list FK existence validation" note below; the two gaps this
                       # closes are removed from the gaps list.
                       # 2026-07-31: pkgs/sdkcheck reverse check found a fabricated "UpdateDataset" operation --
                       # real Forecast has no such op (only UpdateDatasetGroup exists in the dataset family);
                       # a prior pass's addCRUD("Dataset", ..., update=true) call both advertised AND dispatched
                       # it, so this was reachable, not just a documentation typo. Fixed by flipping the flag to
                       # false, which deletes the fabricated route entirely (nothing legitimate depended on it --
                       # no test exercised it, and this file's own Dataset family note already only claimed
                       # Create/Describe/Delete/List). See ops block below.
                       # 2026-07-23: this pass closed all three named gaps/deferred items from the prior
                       # audit: Domain/DatasetType/ImportMode/DataFrequency field validation,
                       # cross-resource FK existence validation on Create*, and Delete*
                       # status-gating (ResourceInUseException). See "Real bugs fixed this
                       # pass" below for the corrected understanding of the third item.
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAccuracyMetrics: {wire: ok, errors: ok, state: ok, persist: n/a, note: "deterministic synthetic metrics, not touched this pass"}
  DeleteResourceTree: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascade delete bypasses the new Delete* status gate by design -- see note below"}
  StopResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ResumeResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListMonitorEvaluations: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDatasetGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "Domain required + enum-validated (InvalidInputException); DatasetArns (optional list, fixed 2026-08-10) must all resolve to existing Datasets"}
  UpdateDatasetGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed 2026-08-10 -- DatasetArns (required field, per validators.go) must all resolve to existing Datasets; empty list legal (ArnList shape has no min), missing field is InvalidInputException"}
  CreateDataset: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass -- Domain/DatasetType required + enum-validated, Schema required, DataFrequency format-validated"}
  CreateDatasetImportJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass -- DatasetArn must resolve to an existing Dataset (ResourceNotFoundException otherwise); ImportMode enum-validated when present"}
  CreatePredictorBacktestExportJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed 2026-07-23 -- PredictorArn must resolve to an existing Predictor; fixed 2026-08-13 (gopherstack-wl0s) -- Destination now required present"}
  CreateForecast: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass -- PredictorArn must resolve to an existing Predictor"}
  CreateForecastExportJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed 2026-07-23 -- ForecastArn must resolve to an existing Forecast; fixed 2026-08-13 (gopherstack-wl0s) -- Destination now required present"}
  CreateExplainability: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed 2026-07-23 -- ResourceArn must resolve to an existing Predictor or Forecast (real AWS accepts either); fixed 2026-08-13 (gopherstack-wl0s) -- ExplainabilityConfig now required present"}
  CreateExplainabilityExport: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed 2026-07-23 -- ExplainabilityArn must resolve to an existing Explainability; fixed 2026-08-13 (gopherstack-wl0s) -- Destination now required present"}
  CreateMonitor: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass -- ResourceArn must resolve to an existing Predictor"}
  CreateWhatIfAnalysis: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass -- ForecastArn must resolve to an existing Forecast"}
  CreateWhatIfForecast: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass -- WhatIfAnalysisArn must resolve to an existing WhatIfAnalysis"}
  CreateWhatIfForecastExport: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed 2026-07-23 -- WhatIfForecastArns (list) must all resolve to existing WhatIfForecasts; also corrected the field name itself (was erroneously WhatIfAnalysisArn in the emulator's own prior test fixtures -- real CreateWhatIfForecastExportInput has no such field); fixed 2026-08-13 (gopherstack-wl0s) -- Destination now required present"}
  "DeleteDatasetGroup/DeleteDataset/DeleteDatasetImportJob/DeletePredictor/DeleteForecast/DeleteForecastExportJob/DeleteExplainability/DeleteWhatIfAnalysis/DeleteWhatIfForecast/DeleteWhatIfForecastExport/DeleteMonitor":
    {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass -- now reject a resource still CREATE_PENDING with ResourceInUseException, matching each op's documented \"you can delete only X that have a status of ACTIVE or CREATE_FAILED\" precondition. DeletePredictorBacktestExportJob/DeleteExplainabilityExport deliberately excluded: their SDK doc comments carry no status precondition at all, so they remain deletable in any status."}
# Families audited as a group (when per-op is impractical):
families:
  DatasetGroup: {status: ok, note: "Create/Describe/Update/Delete/List verified; CREATE_PENDING->ACTIVE on first Describe; Update replaces DatasetArns wholesale (correct, not merged); Domain required+enum-validated. 2026-08-10: DatasetArns is FK-validated on both Create (optional field, per-entry existence check when present) and Update (required field per validators.go's validateOpUpdateDatasetGroupInput, but the underlying ArnList shape sets no minimum length so an empty list is legal and clears the group)."}
  Dataset: {status: ok, note: "Create/Describe/Delete/List verified; Schema/DataFrequency/Domain/DatasetType field retention correct; Domain/DatasetType required+enum-validated and DataFrequency format-validated this pass. 2026-07-31: a fabricated \"UpdateDataset\" route (addCRUD update=true) was found wired and advertised even though this family note never claimed Update -- real Forecast has no such op; deleted, see header note."}
  DatasetImportJob: {status: ok, note: "S3Config.Path required -> CREATE_FAILED on missing path, matches known emulator convention (documented in TestDatasetImportJobs_S3Validation); DatasetArn FK-validated this pass"}
  Predictor: {status: ok, note: "Create/Describe/Delete/List + CreateAutoPredictor/DescribeAutoPredictor verified; PerformAutoML/PerformHPO/HyperParameterTuningJobConfig retained. 2026-08-10: InputDataConfig.DatasetGroupArn (CreatePredictor) / DataConfig.DatasetGroupArn (CreateAutoPredictor) are now FK-validated when that nested config block is present in the request (validatePredictorFieldsLocked in validation.go) -- both operations route to kindPredictor, so presence of the parent field name distinguishes which shape is in play. 2026-08-13 (gopherstack-wl0s): CreatePredictor's ForecastHorizon/InputDataConfig/FeaturizationConfig are now required-present (requiredPresenceFields, keyed by action name so CreateAutoPredictor -- whose SDK input has no FeaturizationConfig field at all and only requires PredictorName -- is unaffected)."}
  Forecast: {status: ok, note: "Create/Describe/Delete/List verified; epoch-seconds CreationTime/LastModificationTime via awstime.Epoch; PredictorArn FK-validated this pass"}
  "ForecastExportJob/PredictorBacktestExportJob/ExplainabilityExport/WhatIfAnalysis/WhatIfForecast/WhatIfForecastExport/Monitor/Explainability":
    status: ok
    note: "generic addCRUD-driven lifecycle (Create/Describe/List/Delete) shares the same describe()/list()/delete() backend paths already verified for the higher-traffic families; every family's required ARN-reference field is now FK-validated (see ops table); Delete* status-gated per family (see ops table)"
  ListOperations_Pagination: {status: ok, note: "malformed NextToken returns InvalidNextTokenException (page.ValidateToken wired into listOutput); not touched this pass"}
  Tags: {status: ok, note: "Tag/Untag/ListTagsForResource validate the ARN exists via arnIndex before mutating/reading tag state; not touched this pass"}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - >-
    Delete* never returns ResourceInUseException for a resource that still
    has *dependents* (e.g. deleting a Predictor that still has Forecasts).
    This is DELIBERATE, not an oversight: the real Amazon Forecast SDK doc
    comments for every Delete* op (DeletePredictor, DeleteDatasetGroup,
    DeleteForecast, ...) describe the ResourceInUseException precondition
    purely in terms of the target resource's OWN status ("you can delete
    only predictor that have a status of ACTIVE or CREATE_FAILED"), never in
    terms of dependents -- DeleteDatasetGroup's doc comment explicitly says
    "This operation deletes only the dataset group, not the datasets in the
    group" with no blocking behavior. The PRIOR audit's framing of this gap
    ("Delete* never returns ResourceInUseException for a resource that still
    has dependents") does not match the real API and has been corrected:
    what real AWS actually models is a self-status precondition, which this
    pass implemented (see validateDeletableLocked in validation.go and the
    Delete* ops table above).
deferred: []            # all three deferred items from the prior audit (Domain/DatasetType/
                         # DataFrequency/ImportMode enum validation; cross-resource FK
                         # existence validation on Create*; Delete* status/ResourceInUse
                         # modeling) were implemented in the 2026-07-23/31 passes; the two
                         # residual FK gaps they left open (nested Predictor FK,
                         # DatasetGroup.DatasetArns) were closed 2026-08-10 (gopherstack-4vpt).
leaks: {status: clean, note: "no goroutines/janitors in this service; Reset()/Snapshot()/Restore() all take b.mu correctly; the new validateCreateFieldsLocked/validateDeletableLocked helpers are called from within create()/delete() while b.mu is already held (no additional locking, no lock-order risk); no lock held across a call that could deadlock"}
---

## Notes

- Protocol: awsjson1.1, single POST endpoint, `X-Amz-Target: AmazonForecast.<Op>`.
  Verified against real SDK generated code (aws-sdk-go-v2/service/forecast@v1.42.0):
  target prefix `"AmazonForecast."` matches `newServiceMetadataMiddleware_op*`
  registrations. RouteMatcher/ExtractOperation in handler.go are correct.

- Status lifecycle: this emulator uses a lazy-transition model — a resource is
  created in `CREATE_PENDING` (or `CREATE_FAILED` for DatasetImportJob when
  S3Config.Path is empty) and flips to `ACTIVE` the *first time* `Describe*` is
  called on it (`InMemoryBackend.describe` in store.go). This looks like it
  skips `CREATE_IN_PROGRESS` entirely, but it is intentional and does NOT hang a
  polling client: the first poll observes `CREATE_PENDING`, every subsequent
  poll observes `ACTIVE`. This is a "looks-wrong-but-correct" trap — do not
  "fix" it by adding a `CREATE_IN_PROGRESS` state without checking
  `TestHandler_ResourceLifecycles` (handler_test.go) and
  `TestStatusTransitions_PendingActiveDelete` (store_test.go) first, both of
  which assert exactly this two-poll transition.

- **Delete\* now status-gates on CREATE_PENDING (real bug fixed this pass).**
  Real Amazon Forecast's Delete\* API doc comments each state a precondition
  like "You can delete only predictor that have a status of ACTIVE or
  CREATE_FAILED" — a resource still `CREATE_PENDING` (this emulator's stand-in
  for AWS's `CREATE_IN_PROGRESS`) is not yet deletable and returns
  `ResourceInUseException` (400). This is implemented as a declarative
  per-kind table (`deletableStatuses` in validation.go) rather than a single
  global rule because the real API's precondition differs slightly per kind:
  `DeletePredictorBacktestExportJob` and `DeleteExplainabilityExport` carry no
  documented status precondition at all in the SDK and remain deletable in any
  status; `DeleteMonitor` additionally allows `ACTIVE_STOPPED`/`CREATE_STOPPED`
  (mapped here to this emulator's own single `STOPPED` convention, shared by
  every other stoppable kind for the same reason). `DeleteResourceTree`
  deliberately bypasses this gate: it is a distinct AWS operation with its own
  cascade semantics, and its SDK doc comment carries no per-resource status
  precondition of its own.

- **Cross-resource FK existence validation on Create\* (real bug fixed this
  pass, was the top-listed gap in the prior audit).** Every Create\* operation
  whose input carries a *required* top-level ARN-reference field (per
  aws-sdk-go-v2/service/forecast's validators.go — see `createFKSpecs` in
  validation.go) now resolves that reference against the backend's `arnIndex`
  before creating the resource: a missing field is `InvalidInputException`
  (matching the SDK's client-side "This member is required" rule), a
  non-existent reference is `ResourceNotFoundException` (matching real
  Amazon Forecast, confirmed against `deserializers.go`, which wires
  `ResourceNotFoundException` into every Create\* op's error switch).
  `CreateExplainability`'s `ResourceArn` is validated against *either* a
  Predictor or a Forecast ARN, matching real AWS (Explainability can be
  computed for either resource type).

- **Nested/list FK existence validation (real bug fixed 2026-08-10,
  gopherstack-4vpt).** The prior pass above covered every *top-level* required
  ARN-reference field; two references were left open because they don't fit
  that shape:
  - `CreatePredictor`'s `InputDataConfig.DatasetGroupArn` and
    `CreateAutoPredictor`'s `DataConfig.DatasetGroupArn` are one level nested.
    Both operations route to the same `kindPredictor` create path, so
    `validatePredictorFieldsLocked` (validation.go) distinguishes them by
    which parent field name (`InputDataConfig` vs `DataConfig`) is present in
    the request, not by operation identity, and only validates when that
    parent block is present in the payload.
  - `CreateDatasetGroup`/`UpdateDatasetGroup`'s `DatasetArns` list. Per
    botocore's `forecast/2018-06-26/service-2.json.gz` model:
    `CreateDatasetGroupRequest` does not list `DatasetArns` as required (an
    absent or empty list is legal), while `UpdateDatasetGroupRequest` does
    (the field key itself must be present) — but the underlying `ArnList`
    shape declares no `min` length, so a present-but-empty list is legal on
    Update too (it clears the group's datasets, matching the op's "Replaces
    the datasets in a dataset group" doc comment). Each list entry, when
    present, must resolve to an existing Dataset; the first dangling entry
    fails fast with `ResourceNotFoundException` (no documented AWS behavior
    suggests batching all missing entries into one response).
  All four are validated before any mutation (`InMemoryBackend.create`/
  `update` call the validators while holding `b.mu`, before touching the
  resource table).

- **Enum/format validation on Create\* (real bug fixed this pass, was the
  second-listed gap in the prior audit).** `CreateDatasetGroup`/`CreateDataset`
  now require `Domain` and reject a value outside `types.Domain`'s seven enum
  members (RETAIL, CUSTOM, INVENTORY_PLANNING, EC2_CAPACITY, WORK_FORCE,
  WEB_TRAFFIC, METRICS); `CreateDataset` additionally requires `DatasetType`
  (validated against `types.DatasetType`'s three members) and `Schema`.
  `CreateDatasetImportJob`'s optional `ImportMode`, when present, is validated
  against `types.ImportMode`'s two members (FULL, INCREMENTAL). `DataFrequency`
  is a special case: unlike the three fields above, it has **no** corresponding
  `types.X` enum in the SDK at all (confirmed: `grep DataFrequency
  aws-sdk-go-v2/service/forecast/types/*.go` returns nothing) — it's
  server-validated free text per the field's doc comment, not a
  client-side-smithy-validated enum. This emulator therefore applies a format
  check (optional 1–2 digit interval + Y/M/W/D/H/min unit) rather than an
  enum-membership check, and treats the field as optional (real AWS's doc
  text only requires it for RELATED_TIME_SERIES datasets, and even then only
  in prose).

- **Required-presence validation on Create\* passthrough fields (real bug
  fixed 2026-08-13, gopherstack-wl0s).** The generic-CRUD `create()` path
  (store.go's `cloneMap`) stores and echoes the whole input map, so a
  supplied value for these fields already round-tripped fine through
  Describe\* — verified per field, not assumed: `CreateExplainability`'s
  `ExplainabilityConfig`; `CreateForecastExportJob`'s,
  `CreatePredictorBacktestExportJob`'s, `CreateExplainabilityExport`'s, and
  `CreateWhatIfForecastExport`'s shared `Destination`; and `CreatePredictor`'s
  `ForecastHorizon`, `InputDataConfig`, and `FeaturizationConfig`. What was
  missing was rejecting a request that omitted one of these fields, even
  though `aws-sdk-go-v2/service/forecast@v1.44.4/validators.go`'s
  `validateOpCreate*Input` functions mark each of them required. All are now
  checked by `requiredPresenceFields` in validation.go, keyed by **action
  name**, not `resourceKind`: `CreatePredictor` and `CreateAutoPredictor`
  both route to `kindPredictor`, but `CreateAutoPredictorInput` only requires
  `PredictorName` (its `ForecastHorizon`/`DataConfig` are optional, and it has
  no `FeaturizationConfig` field at all) — a kind-keyed table would have
  wrongly rejected valid `CreateAutoPredictor` requests, which the FK
  reference table below sidesteps by nesting on the parent field name instead
  but presence-of-the-whole-input-struct can't. The originating audit named
  only 2 of `CreatePredictor`'s 3 unvalidated required fields (missed
  `InputDataConfig`); this fix covers all 3, confirmed against
  `validateOpCreatePredictorInput`. `InputDataConfig`'s presence check also
  surfaces `InputDataConfig.DatasetGroupArn`'s pre-existing nested FK check
  (`validatePredictorFieldsLocked`), which previously never fired for
  `CreatePredictor` because no test ever supplied `InputDataConfig` at all.

- Persistence: `Handler.Snapshot`/`Restore` already delegate to
  `InMemoryBackend.Snapshot`/`Restore` (persistence.go), which uses
  `store.Registry` for the per-kind resource tables and persists the raw
  `evaluations`/`tags` maps directly; `arnIndex` is deliberately NOT persisted
  and is rebuilt from the restored tables (`rebuildARNIndex`). No persistence
  gap found for the validation logic added this pass: it reads `arnIndex`
  (always rebuilt from the tables, pre- and post-restore) and never itself
  needs to persist any new state.
