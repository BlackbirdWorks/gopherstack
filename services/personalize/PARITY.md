---
service: personalize
sdk_module: aws-sdk-go-v2/service/personalize@v1.47.11
last_audit_commit: 69eefabd
last_audit_date: 2026-07-13
overall: A
ops:
  CreateDatasetGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeDatasetGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDatasetGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDatasetGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDataset: {wire: ok, errors: ok, state: ok, persist: ok, note: no FK check on datasetGroupArn/schemaArn -- see gaps}
  DescribeDataset: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDataset: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDataset: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDatasets: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateSchema: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeSchema: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteSchema: {wire: ok, errors: ok, state: ok, persist: ok}
  ListSchemas: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateSolution: {wire: fixed, errors: ok, state: fixed, persist: ok, note: added performAutoTraining (default true)/performIncrementalUpdate, previously silently dropped}
  DescribeSolution: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateSolution: {wire: fixed, errors: ok, state: fixed, persist: ok, note: 'was reading performAutoML/performHPO, fields that do not exist on the real UpdateSolutionInput -- real SDK calls were a silent no-op. Now reads performAutoTraining/performIncrementalUpdate (*bool, nil = unchanged)'}
  DeleteSolution: {wire: ok, errors: ok, state: ok, persist: ok}
  ListSolutions: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateSolutionVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: no FK check on solutionArn -- see gaps}
  DescribeSolutionVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteSolutionVersion: {wire: gap, errors: ok, state: ok, persist: ok, note: routed op has no real AWS Personalize API equivalent -- see gaps}
  ListSolutionVersions: {wire: ok, errors: ok, state: ok, persist: ok}
  StopSolutionVersionCreation: {wire: fixed, errors: ok, state: fixed, persist: ok, note: 'was setting status to "STOPPED", not a valid SolutionVersion.Status enum member; fixed to "CREATE STOPPED"'}
  GetSolutionMetrics: {wire: ok, errors: ok, state: ok, persist: n/a}
  CreateCampaign: {wire: ok, errors: ok, state: ok, persist: ok, note: no FK check on solutionVersionArn -- see gaps}
  DescribeCampaign: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateCampaign: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteCampaign: {wire: ok, errors: ok, state: ok, persist: ok}
  ListCampaigns: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateEventTracker: {wire: ok, errors: ok, state: ok, persist: ok, note: no FK check on datasetGroupArn -- see gaps}
  DescribeEventTracker: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteEventTracker: {wire: ok, errors: ok, state: ok, persist: ok}
  ListEventTrackers: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateFilter: {wire: ok, errors: ok, state: ok, persist: ok, note: no FK check on datasetGroupArn -- see gaps}
  DescribeFilter: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteFilter: {wire: ok, errors: ok, state: ok, persist: ok}
  ListFilters: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateRecommender: {wire: ok, errors: ok, state: ok, persist: ok, note: no FK check on datasetGroupArn/recipeArn -- see gaps}
  DescribeRecommender: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateRecommender: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteRecommender: {wire: ok, errors: ok, state: ok, persist: ok}
  ListRecommenders: {wire: ok, errors: ok, state: ok, persist: ok}
  StartRecommender: {wire: ok, errors: ok, state: ok, persist: ok}
  StopRecommender: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateMetricAttribution: {wire: fixed, errors: ok, state: fixed, persist: ok, note: 'metrics is a required field on the real API and was silently ignored; now required + stored'}
  DescribeMetricAttribution: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateMetricAttribution: {wire: fixed, errors: ok, state: fixed, persist: ok, note: 'real request uses addMetrics/removeMetrics, not a metrics replacement; was silently dropped'}
  DeleteMetricAttribution: {wire: ok, errors: ok, state: ok, persist: ok}
  ListMetricAttributions: {wire: ok, errors: ok, state: ok, persist: ok}
  ListMetricAttributionMetrics: {wire: fixed, errors: ok, state: fixed, persist: ok, note: 'was a hardcoded fabricated 2-entry list ignoring the actual attribution; now returns the attribution''s real, paginated Metrics'}
  CreateDatasetImportJob: {wire: ok, errors: ok, state: ok, persist: ok, note: no FK check on datasetArn -- see gaps}
  DescribeDatasetImportJob: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDatasetImportJobs: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDatasetExportJob: {wire: ok, errors: ok, state: ok, persist: ok, note: no FK check on datasetArn -- see gaps}
  DescribeDatasetExportJob: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDatasetExportJobs: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateBatchInferenceJob: {wire: ok, errors: ok, state: ok, persist: ok, note: no FK check on solutionVersionArn -- see gaps}
  DescribeBatchInferenceJob: {wire: ok, errors: ok, state: ok, persist: ok}
  ListBatchInferenceJobs: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateBatchSegmentJob: {wire: ok, errors: ok, state: ok, persist: ok, note: no FK check on solutionVersionArn -- see gaps}
  DescribeBatchSegmentJob: {wire: ok, errors: ok, state: ok, persist: ok}
  ListBatchSegmentJobs: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDataDeletionJob: {wire: ok, errors: ok, state: ok, persist: ok, note: no FK check on datasetGroupArn -- see gaps}
  DescribeDataDeletionJob: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDataDeletionJobs: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeRecipe: {wire: ok, errors: ok, state: n/a, persist: n/a}
  ListRecipes: {wire: ok, errors: ok, state: n/a, persist: n/a}
  DescribeFeatureTransformation: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAlgorithm: {wire: ok, errors: ok, state: n/a, persist: n/a}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  GetRecommendations: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetPersonalizedRanking: {wire: ok, errors: ok, state: ok, persist: n/a}
families:
  DatasetGroup/Dataset/Schema: {status: ok, note: 'ARNs, timestamps (awstime.Epoch), field shapes verified against types.DatasetGroup/Dataset/DatasetSchema deserializers; Schema correctly has no status field (matches real API)'}
  Solution/SolutionVersion: {status: ok, note: 'CreateSolution/UpdateSolution wire bug fixed this pass (see ops); StopSolutionVersionCreation status-string bug fixed this pass'}
  Campaign/EventTracker/Filter/Recommender: {status: ok, note: 'Create/Describe/Update/Delete/List field shapes verified against types.CampaignSummary/EventTrackerSummary/FilterSummary/RecommenderSummary -- gopherstack returns a superset (extra fields harmless, ignored by real deserializers per default case)'}
  MetricAttribution: {status: fixed, note: 'metrics/addMetrics/removeMetrics/ListMetricAttributionMetrics fixed this pass (see ops)'}
  Async jobs (DatasetImportJob/DatasetExportJob/BatchInferenceJob/BatchSegmentJob/DataDeletionJob): {status: ok, note: 'no Delete/Update ops in the real API either -- gopherstack correctly omits them; Create/Describe/List shapes verified'}
  Recipe/Algorithm/FeatureTransformation: {status: ok, note: built-in read-only catalogs, ARNs/status/timestamps verified}
  Tags: {status: ok, note: 'tagKey/tagValue round-trip verified; arnExists() FK check spans all 16 resource tables correctly'}
  Runtime (GetRecommendations/GetPersonalizedRanking): {status: ok, note: 'ValidateCampaign/ValidateCampaignOrRecommender FK checks present and correct (the one place in this backend that does validate parent existence)'}
gaps:
  - >-
    Create* ops for Dataset, SolutionVersion, Campaign, EventTracker, Filter,
    Recommender, DatasetImportJob, DatasetExportJob, BatchInferenceJob,
    BatchSegmentJob, DataDeletionJob do not validate that referenced parent
    ARNs (datasetGroupArn, solutionArn, solutionVersionArn, datasetArn,
    recipeArn) actually exist -- real AWS returns ResourceNotFoundException
    for a dangling reference; gopherstack currently succeeds. Systemic across
    ~11 create paths; deferred as a single follow-up rather than fixed
    piecemeal this pass (would also require updating existing tests that rely
    on the lenient behavior, e.g. handler_audit1_test.go's
    a1PersonalizeCreateCampaign helper). Contrast: GetRecommendations/
    GetPersonalizedRanking (runtime ops) DO validate via
    ValidateCampaign/ValidateCampaignOrRecommender, so the validation
    machinery exists and precedent is set -- just not wired into the control-
    plane Create ops. (needs a bd issue)
  - >-
    "DeleteSolutionVersion" is registered and routed
    (handler.go buildOps) but has no equivalent in the real
    aws-sdk-go-v2/service/personalize v1.47.11 Client (verified: no
    api_op_DeleteSolutionVersion.go, no Client.DeleteSolutionVersion method).
    Real SDK clients never construct this request, so it's inert for
    aws-sdk-go-v2 wire compatibility, but a raw/boto3 caller hitting
    X-Amz-Target: AmazonPersonalize.DeleteSolutionVersion gets 200 from
    gopherstack vs UnknownOperationException from real AWS. Left in place
    (no test depends on its absence and CLAUDE.md forbids destructive
    unrequested removals without clear payoff); flagged for the next auditor
    to decide whether to remove or keep as a deliberate convenience op.
  - >-
    CampaignConfig/RecommenderConfig sub-objects
    (enableMetadataWithRecommendations, itemExplorationConfig,
    trainingDataConfig, syncWithLatestSolutionVersion) and the
    latestCampaignUpdate/latestRecommenderUpdate/latestSolutionUpdate/
    autoMLResult/solutionConfig summary objects on Describe* responses are
    not modeled -- additive-only optional fields, low traffic, deferred.
deferred:
  - CampaignConfig/RecommenderConfig/SolutionConfig nested nested nested optional structures (see gaps)
  - DatasetType enum validation (INTERACTIONS/ITEMS/USERS/ACTIONS/ACTION_INTERACTIONS) on CreateDataset
  - Domain enum validation (ECOMMERCE/VIDEO_ON_DEMAND) on CreateDatasetGroup/CreateSchema
leaks: {status: clean, note: no goroutines/janitors in this backend; all state is synchronous map/table mutation under lockmetrics.RWMutex}
---

## Notes

- **Protocol**: awsjson1.1, single POST endpoint, `X-Amz-Target:
  AmazonPersonalize.<Op>` (control plane) or
  `AmazonPersonalizeRuntime.<Op>` (GetRecommendations/GetPersonalizedRanking).
  `Handler.RouteMatcher` accepts both prefixes; `ExtractOperation` strips
  whichever matched. Confirmed both prefixes are exercised by
  handler_audit1_test.go.

- **Status-string trap (real bug class, fixed this pass)**: this backend
  short-circuits every resource straight to its terminal state on Create
  (`ACTIVE` immediately, skipping `CREATE PENDING`/`CREATE IN_PROGRESS`).
  That skip-the-intermediate-states pattern is a *deliberate, codebase-wide
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

- **UpdateSolution wire-shape bug (fixed this pass)**: the real
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
  (fixed this pass)**: `metrics` is a *required* field on
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

- **FK/parent-existence validation is intentionally inconsistent** across
  this backend: the Personalize Runtime ops (`GetRecommendations`,
  `GetPersonalizedRanking`) validate that `campaignArn`/`recommenderArn`
  resolve to a real resource via `ValidateCampaign`/
  `ValidateCampaignOrRecommender`, but essentially every control-plane
  `Create*` op skips FK validation on its parent ARN (see `gaps`). This is a
  real, systemic parity gap (AWS returns `ResourceNotFoundException` for a
  dangling reference), deliberately deferred rather than fixed piecemeal --
  fixing it touches ~11 Create ops and would require rewriting
  `handler_audit1_test.go`'s `a1PersonalizeCreateCampaign` helper (which
  currently creates a `SolutionVersion` against a `solutionArn` that was
  never actually created) and similar shortcuts throughout the test suite.

- **`DeleteSolutionVersion` is a fabricated op** (see `gaps`) -- the real
  Personalize API has no way to delete an individual solution version at
  all (only `DeleteSolution` removes the whole solution and its versions).
  Left in place; flag but do not assume it's automatically wrong to keep.

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
  `persistence_test.go`'s table-driven per-resource-type coverage. No gaps
  found here.
