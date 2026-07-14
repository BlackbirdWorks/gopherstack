service: sagemaker
sdk_module: aws-sdk-go-v2/service/sagemaker@v1.236.0   # version audited against
last_audit_commit: 32733a415                            # HEAD when this manifest was written
last_audit_date: 2026-07-12
overall: A            # ~166 LOC genuine fix found in the highest-traffic Endpoint family

# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateModel: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeModel: {wire: ok, errors: ok, state: ok, persist: ok}
  ListModels: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteModel: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateEndpointConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeEndpointConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  ListEndpointConfigs: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteEndpointConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateEndpoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass — see Notes"}
  DescribeEndpoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass — ProductionVariants now populated with AWS-accurate ProductionVariantSummary shape"}
  ListEndpoints: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateEndpoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass — see Notes"}
  UpdateEndpointWeightsAndCapacities: {wire: ok, errors: ok, state: ok, persist: ok, note: "updated to new ProductionVariantSummary field names as part of this pass"}
  DeleteEndpoint: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateTrainingJob: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeTrainingJob: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTrainingJobs: {wire: ok, errors: ok, state: ok, persist: ok}
  StopTrainingJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "routes to FSM (InProgress->Stopping->Stopped)"}
  DeleteTrainingJob: {wire: ok, errors: ok, state: ok, persist: ok}
  AddTags: {wire: ok, errors: ok, state: ok, persist: ok, note: "covers models/endpoints/endpoint-configs/training jobs/notebooks/HPO jobs/processing/transform/clusters/domains/feature-groups/pipelines/experiments/trials/trial-components/actions/algorithms/model-packages/associations"}
  ListTags: {wire: ok, errors: ok, state: ok, persist: ok, note: "paginated via offset NextToken, sagemakerDefaultPageSize=100"}
  DeleteTags: {wire: ok, errors: ok, state: ok, persist: ok}

# Families audited as a group (when per-op is impractical):
families:
  model_endpoint_config_crud: {status: ok, note: "CreateModel/DescribeModel/ListModels/DeleteModel and CreateEndpointConfig/DescribeEndpointConfig/ListEndpointConfigs/DeleteEndpointConfig verified op-by-op against handler.go + backend.go: correct ARN building via pkgs/arn, epoch timestamps via epochSeconds (float64 unix seconds, matches awsjson1.1 numeric timestamp), errCodeLookup-equivalent sentinel wiring (awserr.New wraps ErrNotFound/ErrConflict, handler.go handleError maps to ValidationException/ResourceInUse), persistence.go backendSnapshot wiring confirmed for both models and endpointConfigs keyed by region."
  endpoint_lifecycle: {status: ok, note: "CreateEndpoint/UpdateEndpoint/DescribeEndpoint/DeleteEndpoint/ListEndpoints + UpdateEndpointWeightsAndCapacities audited and FIXED — see Notes. FSM-driven Creating/Updating -> InService transitions (backend_accuracy.go scheduleEndpointTransition) verified correct after fix."}
  training_job: {status: ok, note: "CreateTrainingJob(Full)/DescribeTrainingJob(Full)/ListTrainingJobs(Filtered)/StopTrainingJob(FSM)/DeleteTrainingJob/UpdateTrainingJob verified: InProgress->Completed FSM populates ModelArtifacts, BillableTimeInSeconds, SecondaryStatusTransitions with epoch timestamps; StopTrainingJobFSM drives InProgress->Stopping->Stopped."}
  tags: {status: ok, note: "AddTags/ListTags/DeleteTags verified against findTagMapLocked, which indexes ~20 resource kinds by ARN. Not-found path returns ValidationException (400), matching real AWS TagKeys validation error class."}
  processing_transform_job: {status: deferred, note: "Spot-checked dispatch wiring only (CreateProcessingJob/CreateTransformJob route to real, non-stub handlers with FSM completion in backend_accuracy.go/backend_batch2.go/backend_batch3.go). Not wire-audited field-by-field this pass."}
  notebook_instance: {status: deferred, note: "CreateNotebookInstance/Start/Stop use a real FSM (backend_accuracy.go, statuses Pending/InService/Stopping/Stopped). Not wire-audited field-by-field this pass."}
  hyperparameter_tuning_job: {status: deferred, note: "CreateHyperParameterTuningJob/Describe/List/Stop/Delete present with real backend state (backend_new_ops.go). Not wire-audited this pass."}
  domain_app_userprofile_space: {status: deferred, note: "Not audited this pass; large SageMaker Studio surface in backend_stateful_ops.go / backend_batch2.go."}
  pipeline_pipeline_execution: {status: deferred, note: "Not audited this pass; backend_pipeline_ops.go / backend_pipeline_versions.go."}
  experiment_trial_trial_component: {status: deferred, note: "Not audited this pass."}
  feature_store: {status: deferred, note: "Not audited this pass; backend_feature_store.go."}
  model_package_model_package_group: {status: deferred, note: "Not audited this pass (batch2 family)."}
  automl_job: {status: deferred, note: "Not audited this pass."}
  lineage_action_artifact_context_association: {status: deferred, note: "Not audited this pass; backend_lineage.go is large."}
  edge_deployment_device_fleet: {status: deferred, note: "Not audited this pass; backend_edge_deployment.go."}
  labeling_job: {status: deferred, note: "Not audited this pass; backend_labeling.go."}
  hub_hub_content: {status: deferred, note: "Not audited this pass; backend_hub.go."}
  cluster: {status: deferred, note: "Not audited this pass; backend_cluster.go."}
  inference_recommendations_edge_packaging: {status: deferred, note: "Not audited this pass."}
  training_plan: {status: deferred, note: "Not audited this pass; backend_training_plan_ext.go."}
  monitoring_schedule_workteam_compilation_job: {status: deferred, note: "Not audited this pass (batch2 family)."}

gaps:                     # known divergences NOT fixed — link bd issue ids
  - "Pagination across the service is a hand-rolled integer-offset NextToken (parseNextToken/strconv.Atoi) rather than pkgs/page's opaque-token helper. Functionally correct (AWS clients treat NextToken as opaque) and internally consistent, but is a pkgs-catalog convention deviation across ~15 call sites. Not fixed this pass — refactor is cross-cutting and out of budget for a single-family sweep. (no bd issue filed yet)"
  - "ProductionVariantSummary.VariantStatus is populated with a single synthetic {Status: \"Creating\"|\"InService\"} entry, not a full AWS VariantStatus enum/message model (StatusMessage is always empty, no DeployedImages/CapacityReservationConfig/ManagedInstanceScaling/RoutingConfig fields). Sufficient for status-polling clients; deeper fidelity deferred. (no bd issue filed yet)"
  - "9+ families deferred entirely (see families: above) — Domain/App/UserProfile/Space, Pipeline, Experiment/Trial, FeatureStore, ModelPackage, AutoML, Lineage, EdgeDeployment, Labeling, Hub, Cluster, TrainingPlan, MonitoringSchedule/Workteam/CompilationJob — none wire-audited this pass, only spot-checked for stub-vs-real dispatch wiring. Next audit pass should pick 2-3 of these per sweep given the service's size (~47k LOC)."

deferred:                 # consciously not audited this pass (scope) — next pass targets
  - processing_transform_job (wire-shape field audit)
  - notebook_instance (wire-shape field audit)
  - hyperparameter_tuning_job
  - domain_app_userprofile_space
  - pipeline_pipeline_execution
  - experiment_trial_trial_component
  - feature_store
  - model_package_model_package_group
  - automl_job
  - lineage_action_artifact_context_association
  - edge_deployment_device_fleet
  - labeling_job
  - hub_hub_content
  - cluster
  - inference_recommendations_edge_packaging
  - training_plan
  - monitoring_schedule_workteam_compilation_job

leaks: {status: clean, note: "Endpoint/TrainingJob/Notebook FSM transitions all use b.runDelayed(b.lifecycleCtx, ...) which is cancelled by Handler.Shutdown -> Backend.Shutdown (resetLifecycleContext). No raw goroutines found outside this pattern in the audited families."}

---

## Notes

**Bug fixed this pass (CreateEndpoint/DescribeEndpoint/UpdateEndpoint wire + state gap):**

Before this fix, `InMemoryBackend.CreateEndpoint` (backend_new_ops.go) did two things wrong,
both in the highest-traffic Endpoint family:

1. It never validated that `EndpointConfigName` referenced an existing EndpointConfig — AWS
   returns `ValidationException: Could not find endpoint configuration "..."` for
   `CreateEndpoint`/`UpdateEndpoint` against an unknown config; gopherstack silently created
   the endpoint anyway.
2. `Endpoint.ProductionVariants` was typed as `[]ProductionVariant` (the *EndpointConfig-time*
   config shape: `InitialVariantWeight`/`InitialInstanceCount`) and was **never populated** —
   `CreateEndpoint` left it nil. Since `DescribeEndpoint`/`ListEndpoints` serialize this field
   directly, every `DescribeEndpoint` response silently omitted `ProductionVariants` entirely
   (a disguised no-op: the field existed in the struct and even had a JSON tag, but the write
   path that should populate it was missing). Real AWS `DescribeEndpoint` always returns
   `ProductionVariants` as `[]ProductionVariantSummary`, a *different* shape from
   `ProductionVariant`: `CurrentWeight`/`DesiredWeight` (not `InitialVariantWeight`),
   `CurrentInstanceCount`/`DesiredInstanceCount` (not `InitialInstanceCount`), plus
   `VariantStatus`. `UpdateEndpointWeightsAndCapacitiesFull` was also silently mutating the
   wrong field names (`InitialVariantWeight`/`InitialInstanceCount`) which happened to compile
   only because both structs used to share the same type — this was itself latent evidence the
   op never actually worked end-to-end against a real AWS-shaped response.

Fix: added `ProductionVariantSummary`/`ProductionVariantStatus` types matching the real SDK
(`aws-sdk-go-v2/service/sagemaker/types.ProductionVariantSummary`); `CreateEndpoint` now 404s
via `ErrEndpointConfigNotFound` when the config doesn't exist, and populates
`Desired*`/`VariantStatus:[{Status:"Creating"}]` from the config's variants; `UpdateEndpoint`
does the same 404 check and rebuilds Desired* from the new config while carrying forward
`Current*` from the previously-deployed variant (matches AWS: old capacity keeps serving
traffic until the new config finishes rolling out); `scheduleEndpointTransition` (the
Creating/Updating -> InService FSM timer) now sets `Current* = Desired*` and
`VariantStatus:[{Status:"InService"}]` the moment the endpoint reaches `InService`, matching
real AWS's converged-state behavior. Files: `backend_new_ops.go`, `backend_accuracy.go`. Tests
added/extended in `handler_accuracy2_test.go`
(`TestHandler_DescribeEndpoint_FullResponse`, `TestHandler_DescribeEndpoint_EventuallyInService`,
new `TestHandler_CreateEndpoint_UnknownEndpointConfig`).

**Looks-wrong-but-correct traps for the next auditor:**
- `awserr.New("ValidationException", awserr.ErrNotFound)` — the string literal passed as `msg`
  is NOT the `__type` value sent on the wire; `handler.go`'s `handleError` hardcodes
  `__type: "ValidationException"` for any error matching `errors.Is(err, awserr.ErrNotFound)`
  regardless of the sentinel's own message text. The message string only ends up embedded in
  the human-readable `message` field via `fmt.Errorf("%w: ...", sentinel, ...)`. This is
  correct/intentional, not a bug — don't "fix" the redundant-looking string literal.
- Tag lookups (`findTagMapLocked`) intentionally search ~20 different ARN-index maps in
  priority order; this is not a stub or inefficiency to "simplify" — every resource kind that
  supports `AddTags`/`ListTags`/`DeleteTags` needs its own branch since there's no unified
  resource registry (see pkgs-catalog.md's noted `gopherstack-drp` planned fix for exactly this
  kind of per-map boilerplate, which is a cross-service concern, not sagemaker-specific).
- Pagination throughout sagemaker is a hand-rolled stringified integer offset
  (`parseNextToken`/`strconv.Itoa`), not `pkgs/page`. This deviates from the pkgs-catalog
  convention but is wire-compatible (AWS `NextToken` is client-opaque) — flagged as a gap
  above, not fixed, since it's a cross-cutting refactor far outside a single-family bug-fix
  budget.
- Protocol is JSON (`awsjson1.1`, `X-Amz-Target: SageMaker.<Op>`), not REST-XML/REST-JSON.
  Timestamps are epoch-seconds `float64` via `epochSeconds()`, not ISO8601 strings — this is
  correct for this protocol; do not "fix" to ISO8601 strings.
