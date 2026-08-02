---
service: personalize
sdk_module: aws-sdk-go-v2/service/personalize@v1.47.11
sibling_sdk_modules: [aws-sdk-go-v2/service/personalizeruntime@v1.36.2]  # GetRecommendations/GetPersonalizedRanking; see the Runtime family below
last_audit_commit: 12cf224d
last_audit_date: 2026-07-23
overall: A
ops:
  CreateDatasetGroup: {wire: fixed, errors: ok, state: fixed, persist: ok, note: 'added domain enum validation (ECOMMERCE/VIDEO_ON_DEMAND, or empty for a Custom group) -- an unrecognized value previously succeeded silently'}
  DescribeDatasetGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDatasetGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDatasetGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDataset: {wire: fixed, errors: ok, state: fixed, persist: ok, note: 'added FK validation on datasetGroupArn/schemaArn (ResourceNotFoundException for a dangling reference) and datasetType enum validation (case-insensitive INTERACTIONS/ITEMS/USERS/ACTIONS/ACTION_INTERACTIONS) -- both previously unvalidated'}
  DescribeDataset: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDataset: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDataset: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDatasets: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateSchema: {wire: fixed, errors: ok, state: fixed, persist: ok, note: 'added domain enum validation (ECOMMERCE/VIDEO_ON_DEMAND, or empty), same as CreateDatasetGroup'}
  DescribeSchema: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteSchema: {wire: ok, errors: ok, state: ok, persist: ok}
  ListSchemas: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateSolution: {wire: fixed, errors: ok, state: fixed, persist: ok, note: 'this pass: added FK validation on datasetGroupArn (always required) and recipeArn (required only when performAutoML is false); added eventType (a plain CreateSolutionInput member that was completely unread) and solutionConfig (opaque round-trip) and autoMLResult (populated with a deterministic bestRecipeArn when performAutoML is true). Prior pass: added performAutoTraining (default true)/performIncrementalUpdate, previously silently dropped'}
  DescribeSolution: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateSolution: {wire: fixed, errors: ok, state: fixed, persist: ok, note: 'this pass: now populates latestSolutionUpdate (types.SolutionUpdateSummary-shaped) on every successful call, absent until the first update, matching the real API. Prior pass: was reading performAutoML/performHPO, fields that do not exist on the real UpdateSolutionInput -- real SDK calls were a silent no-op. Now reads performAutoTraining/performIncrementalUpdate (*bool, nil = unchanged)'}
  DeleteSolution: {wire: ok, errors: ok, state: ok, persist: ok}
  ListSolutions: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateSolutionVersion: {wire: fixed, errors: ok, state: fixed, persist: ok, note: 'added FK validation on solutionArn; solutionConfig is now inherited from the parent solution onto the version (the configuration actually used to train it), matching the real SolutionVersion.solutionConfig field'}
  DescribeSolutionVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  ListSolutionVersions: {wire: ok, errors: ok, state: ok, persist: ok}
  StopSolutionVersionCreation: {wire: fixed, errors: ok, state: fixed, persist: ok, note: 'was setting status to "STOPPED", not a valid SolutionVersion.Status enum member; fixed to "CREATE STOPPED"'}
  GetSolutionMetrics: {wire: ok, errors: ok, state: ok, persist: n/a}
  CreateCampaign: {wire: fixed, errors: ok, state: fixed, persist: ok, note: 'added FK validation on solutionVersionArn and campaignConfig (enableMetadataWithRecommendations/itemExplorationConfig/etc., opaque round-trip) support -- both previously missing'}
  DescribeCampaign: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateCampaign: {wire: fixed, errors: ok, state: fixed, persist: ok, note: 'added FK validation on solutionVersionArn (when supplied), campaignConfig support, and latestCampaignUpdate (types.CampaignUpdateSummary-shaped) population on every successful call -- previously the real UpdateCampaignInput.campaignConfig member was silently dropped and no update history was tracked'}
  DeleteCampaign: {wire: ok, errors: ok, state: ok, persist: ok}
  ListCampaigns: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateEventTracker: {wire: fixed, errors: ok, state: fixed, persist: ok, note: added FK validation on datasetGroupArn}
  DescribeEventTracker: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteEventTracker: {wire: ok, errors: ok, state: ok, persist: ok}
  ListEventTrackers: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateFilter: {wire: fixed, errors: ok, state: fixed, persist: ok, note: added FK validation on datasetGroupArn}
  DescribeFilter: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteFilter: {wire: ok, errors: ok, state: ok, persist: ok}
  ListFilters: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateRecommender: {wire: fixed, errors: ok, state: fixed, persist: ok, note: 'added FK validation on datasetGroupArn and recipeArn (against the built-in recipe catalog); recommenderConfig now round-trips in full (previously only minRecommendationRequestsPerSecond was extracted from the sub-object -- enableMetadataWithRecommendations/itemExplorationConfig/etc. were silently dropped, a disguised-partial-implementation bug)'}
  DescribeRecommender: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateRecommender: {wire: fixed, errors: ok, state: fixed, persist: ok, note: 'recommenderConfig is a required member on the real UpdateRecommenderInput and is now enforced (was silently optional); now round-trips in full (see CreateRecommender) and populates latestRecommenderUpdate on every successful call, absent until the first update'}
  DeleteRecommender: {wire: ok, errors: ok, state: ok, persist: ok}
  ListRecommenders: {wire: ok, errors: ok, state: ok, persist: ok}
  StartRecommender: {wire: ok, errors: ok, state: ok, persist: ok}
  StopRecommender: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateMetricAttribution: {wire: fixed, errors: ok, state: fixed, persist: ok, note: 'this pass: added FK validation on datasetGroupArn. Prior pass: metrics is a required field on the real API and was silently ignored; now required + stored'}
  DescribeMetricAttribution: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateMetricAttribution: {wire: fixed, errors: ok, state: fixed, persist: ok, note: 'real request uses addMetrics/removeMetrics, not a metrics replacement; was silently dropped'}
  DeleteMetricAttribution: {wire: ok, errors: ok, state: ok, persist: ok}
  ListMetricAttributions: {wire: ok, errors: ok, state: ok, persist: ok}
  ListMetricAttributionMetrics: {wire: fixed, errors: ok, state: fixed, persist: ok, note: 'was a hardcoded fabricated 2-entry list ignoring the actual attribution; now returns the attribution''s real, paginated Metrics'}
  CreateDatasetImportJob: {wire: fixed, errors: ok, state: fixed, persist: ok, note: added FK validation on datasetArn}
  DescribeDatasetImportJob: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDatasetImportJobs: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDatasetExportJob: {wire: fixed, errors: ok, state: fixed, persist: ok, note: added FK validation on datasetArn}
  DescribeDatasetExportJob: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDatasetExportJobs: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateBatchInferenceJob: {wire: fixed, errors: ok, state: fixed, persist: ok, note: added FK validation on solutionVersionArn}
  DescribeBatchInferenceJob: {wire: ok, errors: ok, state: ok, persist: ok}
  ListBatchInferenceJobs: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateBatchSegmentJob: {wire: fixed, errors: ok, state: fixed, persist: ok, note: added FK validation on solutionVersionArn}
  DescribeBatchSegmentJob: {wire: ok, errors: ok, state: ok, persist: ok}
  ListBatchSegmentJobs: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDataDeletionJob: {wire: fixed, errors: ok, state: fixed, persist: ok, note: added FK validation on datasetGroupArn}
  DescribeDataDeletionJob: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDataDeletionJobs: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeRecipe: {wire: ok, errors: ok, state: n/a, persist: n/a}
  ListRecipes: {wire: ok, errors: ok, state: n/a, persist: n/a}
  DescribeFeatureTransformation: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAlgorithm: {wire: ok, errors: ok, state: n/a, persist: n/a}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  GetRecommendations: {wire: ok, errors: ok, state: ok, persist: n/a, note: "real personalizeruntime.Client op (confirmed by name against aws-sdk-go-v2/service/personalizeruntime), not personalizesdk.Client -- pkgs/sdkcheck's reverse check flagged this as 'phantom' only because it compared against the control-plane client; sdk_completeness_test.go now checks it against personalizeruntimesdk.Client (2026-07-31, gopherstack-vhw2)"}
  GetPersonalizedRanking: {wire: ok, errors: ok, state: ok, persist: n/a, note: "same as GetRecommendations -- real personalizeruntime.Client op, now checked against the correct sibling client"}
families:
  DatasetGroup/Dataset/Schema: {status: fixed, note: 'ARNs, timestamps (awstime.Epoch), field shapes verified against types.DatasetGroup/Dataset/DatasetSchema deserializers; Schema correctly has no status field (matches real API). This pass: domain enum validation on DatasetGroup/Schema, datasetType enum validation + datasetGroupArn/schemaArn FK validation on Dataset (see ops)'}
  Solution/SolutionVersion: {status: fixed, note: 'This pass: datasetGroupArn/recipeArn/solutionArn FK validation, eventType/solutionConfig/autoMLResult/latestSolutionUpdate wire fields added (see ops) -- verified against types.Solution/types.SolutionVersion field-by-field. Prior pass: CreateSolution/UpdateSolution wire bug fixed; StopSolutionVersionCreation status-string bug fixed. Deferred: SolutionVersion still does not model datasetGroupArn/eventType/performAutoML/performHPO/performIncrementalUpdate/recipeArn/failureReason (copies of the parent Solution''s fields at training time) or Solution.latestSolutionVersion (a SolutionVersionSummary of the most recent version) -- see deferred'}
  Campaign/EventTracker/Filter/Recommender: {status: fixed, note: 'Create/Describe/Update/Delete/List field shapes verified against types.CampaignSummary/EventTrackerSummary/FilterSummary/RecommenderSummary -- gopherstack returns a superset (extra fields harmless, ignored by real deserializers per default case). This pass: datasetGroupArn FK validation on EventTracker/Filter, datasetGroupArn+solutionVersionArn+recipeArn FK validation on Campaign/Recommender, campaignConfig/recommenderConfig full round-trip + latestCampaignUpdate/latestRecommenderUpdate (see ops)'}
  MetricAttribution: {status: fixed, note: 'Prior pass: metrics/addMetrics/removeMetrics/ListMetricAttributionMetrics fixed. This pass: datasetGroupArn FK validation added (see ops)'}
  Async jobs (DatasetImportJob/DatasetExportJob/BatchInferenceJob/BatchSegmentJob/DataDeletionJob): {status: fixed, note: 'no Delete/Update ops in the real API either -- gopherstack correctly omits them; Create/Describe/List shapes verified. This pass: datasetArn/solutionVersionArn/datasetGroupArn FK validation added to every Create* op (see ops)'}
  Recipe/Algorithm/FeatureTransformation: {status: ok, note: built-in read-only catalogs, ARNs/status/timestamps verified}
  Tags: {status: ok, note: 'tagKey/tagValue round-trip verified; arnExists() FK check spans all 16 resource tables correctly'}
  Runtime (GetRecommendations/GetPersonalizedRanking): {status: ok, note: 'ValidateCampaign/ValidateCampaignOrRecommender FK checks present and correct -- this pass extended the same validate-parent-existence discipline to every control-plane Create* op, closing the inconsistency previously noted here. UPDATE (2026-07-31, reverse sdkcheck sweep, gopherstack-vhw2): both are real aws-sdk-go-v2/service/personalizeruntime ops, not personalize ops -- added the module to go.mod and pointed sdk_completeness_test.go at it directly. That client also has a third op, GetActionRecommendations, which this Handler does not implement (listed as notImplemented in the completeness check; not otherwise audited this sweep).'}
gaps:
  - >-
    SolutionVersion does not model datasetGroupArn/eventType/performAutoML/
    performHPO/performIncrementalUpdate/recipeArn/failureReason -- real AWS
    copies these from the parent Solution onto each trained version (verified
    against types.SolutionVersion). Discovered field-diffing this pass;
    additive-only, low-traffic fields, deferred rather than fixed alongside
    the FK-validation/config-round-trip work (needs a bd issue).
  - >-
    Solution.latestSolutionVersion (types.SolutionVersionSummary of the most
    recently created version) is not modeled on DescribeSolution -- would
    require a cross-table lookup (scan solutionVersions for the given
    solutionArn, pick the max by CreationDateTime) that the current
    describeSolution/solutionToMap pure-function shape doesn't have backend
    access to do; deferred rather than plumbed through piecemeal this pass
    (needs a bd issue).
deferred:
  - >-
    SolutionVersion/Solution.latestSolutionVersion missing fields (see gaps)
  - >-
    itemExplorationConfig/trainingDataConfig/AutoMLConfig/AutoTrainingConfig/
    HPOConfig/EventsConfig sub-objects inside CampaignConfig/
    RecommenderConfig/SolutionConfig are round-tripped opaquely (whatever the
    caller sends comes back unmodified on Describe/List) rather than deeply
    typed/interpreted server-side -- matches the DataSource/JobOutput/
    MetricsOutputConfig pattern used elsewhere in this backend for deeply
    nested optional AWS structures. This is sufficient for wire-shape parity
    (an emulator has no training loop to actually interpret HPO/AutoML
    hyperparameters against) but is noted here since it is a deliberate
    modeling simplification, not an oversight.
leaks: {status: clean, note: no goroutines/janitors in this backend; all state is synchronous map/table mutation under lockmetrics.RWMutex. This pass added no new goroutines, tickers, or persistence-relevant fields requiring cleanup.}
---

## Notes

- **Protocol**: awsjson1.1, single POST endpoint, `X-Amz-Target:
  AmazonPersonalize.<Op>` (control plane) or
  `AmazonPersonalizeRuntime.<Op>` (GetRecommendations/GetPersonalizedRanking).
  `Handler.RouteMatcher` accepts both prefixes; `ExtractOperation` strips
  whichever matched. Confirmed both prefixes are exercised by
  handler_test.go / handler_runtime_test.go.

- **Invented op removed this pass**: `DeleteSolutionVersion` was registered
  and routed (`handler.go` `buildOps`) but has no equivalent in the real
  `aws-sdk-go-v2/service/personalize` v1.47.11 `Client` (verified: no
  `api_op_DeleteSolutionVersion.go`, no `Client.DeleteSolutionVersion`
  method -- the real API can only delete a whole solution and all its
  versions together, via `DeleteSolution`). A raw/boto3 caller hitting
  `X-Amz-Target: AmazonPersonalize.DeleteSolutionVersion` got `200` from
  gopherstack vs `UnknownOperationException` from real AWS. Per
  parity-principles (delete gopherstack-invented ops not in the real SDK),
  the route, handler function, and backend method were all removed;
  `TestPersonalize_DeleteSolutionVersion_NotARealOperation` locks that the
  operation now falls through to the standard "operation not implemented"
  `InvalidInputException` path like any other unrecognized op name, and that
  the targeted solution version is left untouched.

- **Systemic FK-validation gap closed this pass**: every `Create*` op that
  references a parent resource ARN (`datasetGroupArn`, `solutionArn`,
  `solutionVersionArn`, `datasetArn`, `schemaArn`, `recipeArn`) now validates
  that the parent actually exists, returning `ResourceNotFoundException` for
  a dangling reference the same way real AWS does. This touched 12 Create
  ops (the 11 documented in the prior audit pass plus `CreateSolution`'s
  `datasetGroupArn`/`recipeArn`, which the prior audit's gap list omitted,
  and `CreateMetricAttribution`'s `datasetGroupArn`, found field-diffing this
  pass) and required rewriting every test fixture that relied on the lenient
  behavior -- `handler_test.go`'s `personalizeCreateCampaign` helper (and the
  new `personalizeCreateDatasetGroup`/`personalizeCreateSchema`/
  `personalizeCreateDataset`/`personalizeCreateSolution`/
  `personalizeCreateSolutionVersion` helpers it now composes) build a real
  parent chain instead of a made-up ARN. `handler_fk_validation_test.go` is a
  new table-driven test locking all 16 dangling-parent-ARN rejection paths
  (some ops have two independently-validated FK fields, e.g.
  `CreateDataset`'s `datasetGroupArn`+`schemaArn` and `CreateRecommender`'s
  `datasetGroupArn`+`recipeArn`). `recipeArn` validation is checked against
  the built-in recipe catalog (`recipeExists`, `recipes.go`) rather than a
  mutable resource table. `UpdateCampaign`'s optional `solutionVersionArn` is
  validated the same way when supplied (`TestPersonalize_
  UpdateCampaign_ValidatesSolutionVersionArn`). The Personalize Runtime ops
  (`GetRecommendations`/`GetPersonalizedRanking`) already validated via
  `ValidateCampaign`/`ValidateCampaignOrRecommender` before this pass -- the
  control plane now applies the same discipline consistently.

- **CampaignConfig/RecommenderConfig/SolutionConfig modeled this pass**: all
  three were previously completely unmodeled (Recommender's handler quietly
  extracted only `minRecommendationRequestsPerSecond` from
  `recommenderConfig` and dropped everything else -- a disguised
  partial-implementation bug). They now round-trip opaquely (whatever the
  caller sends on Create/Update comes back unmodified on Describe/List),
  matching the `DataSource`/`JobOutput`/`MetricsOutputConfig` pattern already
  used elsewhere in this backend for deeply nested optional AWS structures.
  Additionally: `latestCampaignUpdate`/`latestRecommenderUpdate`/
  `latestSolutionUpdate` (types.CampaignUpdateSummary/
  RecommenderUpdateSummary/SolutionUpdateSummary) are now populated on every
  successful Update call and correctly *absent* (not a fabricated empty
  object) until the first update, matching each type's real doc comment;
  `autoMLResult` (types.AutoMLResult.bestRecipeArn) is now populated when
  `performAutoML` is true; `eventType` (a plain `CreateSolutionInput` member
  that was completely unread) now round-trips; `SolutionVersion.solutionConfig`
  is now inherited from the parent `Solution` at training time. One real bug
  caught by the new tests during this pass: `AutoMLResult` was initially
  keyed `recipeArn` (copy-pasted from an unrelated constant) instead of the
  real wire field name `bestRecipeArn` -- caught by
  `TestPersonalize_Solution_ConfigEventTypeAndAutoMLResult` before landing.
  `UpdateRecommenderInput.recommenderConfig` is a *required* member on the
  real API (unlike the optional field on Create) and is now enforced.

- **DatasetType/Domain enum validation added this pass**: `CreateDataset`
  now rejects a `datasetType` outside the documented
  `INTERACTIONS`/`ITEMS`/`USERS`/`ACTIONS`/`ACTION_INTERACTIONS` set
  (case-insensitive, matching the real API's documented acceptance, even
  though the SDK models the field as a plain `*string` rather than a typed
  smithy enum -- there is no `types.DatasetType` to diff against, only the
  API documentation). `CreateDatasetGroup`/`CreateSchema` now reject a
  `domain` outside the real `types.Domain` enum (`ECOMMERCE`/
  `VIDEO_ON_DEMAND`); an empty/omitted domain remains valid since it
  produces a Custom, not Domain, dataset group/schema.

- **Status-string trap (real bug class)**: this backend short-circuits every
  resource straight to its terminal state on Create (`ACTIVE` immediately,
  skipping `CREATE PENDING`/`CREATE IN_PROGRESS`). That skip-the-
  intermediate-states pattern is a *deliberate, codebase-wide
  simplification* and is NOT a bug -- real state is mutated, ARNs are real,
  Describe/List reflect it correctly, and it is the correct call for a
  synchronous emulator. Do NOT re-flag it.
  However, **landing on an invalid enum string** is a real bug, and one
  existed: `StopSolutionVersionCreation` set `SolutionVersion.Status` to the
  bare string `"STOPPED"`, which is not a member of the real
  `SolutionVersion.Status` wire enum (only `CREATE PENDING`, `CREATE
  IN_PROGRESS`, `ACTIVE`, `CREATE FAILED`, `CREATE STOPPING`, `CREATE
  STOPPED` are valid, per the `types.SolutionVersion.Status` doc comment in
  aws-sdk-go-v2). Fixed to `"CREATE STOPPED"`. When auditing status
  transitions elsewhere, check the *value* against the type's own doc-listed
  enum, not just "does a transition happen."

- **UpdateSolution wire-shape bug (fixed prior pass)**: the real
  `UpdateSolutionInput` only carries `performAutoTraining` and
  `performIncrementalUpdate` (both optional `*bool`, nil = leave unchanged).
  `performAutoML`/`performHPO` are creation-only, immutable fields that do
  not exist on `UpdateSolutionInput` at all. gopherstack's `UpdateSolution`
  was reading `performAutoML`/`performHPO` from the request and mutating
  those fields -- meaning a real aws-sdk-go-v2 client calling
  `client.UpdateSolution(ctx, &UpdateSolutionInput{PerformAutoTraining:
  aws.Bool(false)})` would silently no-op (the field it actually sent was
  never read) while `performAutoML`/`performHPO` got silently reset to
  `false` (since those JSON keys were absent from the real payload). This is
  the "disguised no-op via wrong field names" bug class -- always check the
  Update variant's *own* Input struct rather than assuming it mirrors
  Create's.

- **CreateMetricAttribution/UpdateMetricAttribution/ListMetricAttributionMetrics
  (fixed prior pass)**: `metrics` is a *required* field on
  `CreateMetricAttributionInput` (list of `{eventType, expression,
  metricName}`) and was completely unread by gopherstack -- Create succeeded
  without it. `UpdateMetricAttributionInput` mutates the metric list via
  `addMetrics`/`removeMetrics` (by `metricName`), not a `metrics` replacement
  field. `ListMetricAttributionMetricsOutput.Metrics` must reflect what was
  actually configured; gopherstack was returning a hardcoded 2-entry
  fabricated list (`"click"`/`"purchase"`) regardless of what the caller
  created -- a textbook disguised no-op / fabricated-data bug. All three now
  round-trip through a real `MetricAttribution.Metrics []MetricAttribute`
  field on the backend struct.

- **Extra fields on List summaries are harmless.** gopherstack's
  `listCampaigns`/`listSolutions`/`listDatasets`/etc. reuse the same
  `*ToMap` function for both `Describe*` (full type) and `List*`
  (`*Summary` type, which is a strict subset of fields in the real API).
  Real aws-sdk-go-v2 deserializers `default: _, _ = key, value` on unknown
  keys, so returning extra fields on a List response is not a wire-shape
  bug -- confirmed by reading `deserializers.go` for
  `CampaignSummary`/`SolutionSummary`/`DatasetSummary`/etc. Do not flag this
  pattern again without first confirming a *required* summary field is
  actually missing (none were).

- Persistence: `Handler.Snapshot`/`Restore` correctly delegate to
  `InMemoryBackend.Snapshot`/`Restore` (`persistence.go`), which round-trips
  all 16 `store.Table`-registered collections plus the plain `tags` map via
  `store.Registry.SnapshotAll`/`RestoreAll`. Verified via
  `persistence_test.go`'s table-driven per-resource-type coverage (updated
  this pass to seed real parent chains for the FK-validated Create ops
  instead of dangling made-up ARNs). No gaps found here.
