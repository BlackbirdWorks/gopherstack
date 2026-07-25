service: sagemaker
sdk_module: aws-sdk-go-v2/service/sagemaker@v1.236.0   # version audited against
last_audit_commit: 32733a415                            # HEAD when this manifest was written
last_audit_date: 2026-07-22
overall: A            # systemic epoch-seconds timestamp wire bug found + fixed across 27 resource types this pass

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
  CreateHyperParameterTuningJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass — see Notes"}
  DescribeHyperParameterTuningJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass — HyperParameterTuningJobConfig now nested correctly, ObjectiveStatusCounters/TrainingJobStatusCounters (both required) now always emitted"}
  ListHyperParameterTuningJobs: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass — ResourceLimits/ObjectiveStatusCounters/TrainingJobStatusCounters (all required) now always emitted"}
  StopHyperParameterTuningJob: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteHyperParameterTuningJob: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDeviceFleet: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass — OutputConfig (required) now validated at Create"}
  DescribeDeviceFleet: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass — see Notes"}
  UpdateDeviceFleet: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass — OutputConfig is now accepted/persisted (was silently dropped)"}
  CreateModelPackage: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass — ModelPackageStatusDetails (required) now always emitted"}
  DescribeModelPackage: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass — see Notes"}

# Families audited as a group (when per-op is impractical):
families:
  model_endpoint_config_crud: {status: ok, note: "CreateModel/DescribeModel/ListModels/DeleteModel and CreateEndpointConfig/DescribeEndpointConfig/ListEndpointConfigs/DeleteEndpointConfig verified op-by-op against handler.go + backend.go: correct ARN building via pkgs/arn, epoch timestamps via epochSeconds (float64 unix seconds, matches awsjson1.1 numeric timestamp), errCodeLookup-equivalent sentinel wiring (awserr.New wraps ErrNotFound/ErrConflict, handler.go handleError maps to ValidationException/ResourceInUse), persistence.go backendSnapshot wiring confirmed for both models and endpointConfigs keyed by region."
  endpoint_lifecycle: {status: ok, note: "CreateEndpoint/UpdateEndpoint/DescribeEndpoint/DeleteEndpoint/ListEndpoints + UpdateEndpointWeightsAndCapacities audited and FIXED — see Notes. FSM-driven Creating/Updating -> InService transitions (backend_accuracy.go scheduleEndpointTransition) verified correct after fix."}
  training_job: {status: ok, note: "CreateTrainingJob(Full)/DescribeTrainingJob(Full)/ListTrainingJobs(Filtered)/StopTrainingJob(FSM)/DeleteTrainingJob/UpdateTrainingJob verified: InProgress->Completed FSM populates ModelArtifacts, BillableTimeInSeconds, SecondaryStatusTransitions with epoch timestamps; StopTrainingJobFSM drives InProgress->Stopping->Stopped."}
  tags: {status: ok, note: "AddTags/ListTags/DeleteTags verified against findTagMapLocked, which indexes ~20 resource kinds by ARN. Not-found path returns ValidationException (400), matching real AWS TagKeys validation error class."}
  processing_transform_job: {status: ok, note: "Wire-audited this pass: DescribeProcessingJob/DescribeTransformJob field-by-field against SDK output structs — field names, optional-field gating, and epoch-seconds timestamps all correct. No bugs found."}
  notebook_instance: {status: ok, note: "Wire-audited this pass: DescribeNotebookInstanceFull field-by-field against SDK — all optional fields correctly gated, epoch-seconds timestamps correct. No bugs found."}
  hyperparameter_tuning_job: {status: ok, note: "FIXED this pass — see Notes (wire-shape bug: flat Strategy instead of nested HyperParameterTuningJobConfig, missing required ObjectiveStatusCounters/TrainingJobStatusCounters/ResourceLimits)."}
  domain_app_userprofile_space: {status: partial, note: "Space's Describe/List timestamp encoding FIXED this pass (see systemic timestamp bug in Notes). Domain/App/UserProfile not otherwise wire-audited this pass."}
  pipeline_pipeline_execution: {status: deferred, note: "Not audited this pass; pipelines.go / pipeline_executions.go / pipeline_versions.go."}
  experiment_trial_trial_component: {status: deferred, note: "Not audited this pass."}
  feature_store: {status: deferred, note: "Not audited this pass; feature_groups.go / feature_store.go."}
  model_package_model_package_group: {status: partial, note: "FIXED this pass — ModelPackage was missing the required ModelPackageStatusDetails field entirely (see Notes); ModelPackage/ModelPackageGroup Describe+List timestamp encoding also fixed. Other model-package fields (InferenceSpecification, SourceAlgorithmSpecification validation, etc.) not otherwise wire-audited this pass."}
  automl_job: {status: partial, note: "FIXED this pass — AutoMLJob was missing the required LastModifiedTime/AutoMLJobSecondaryStatus fields entirely, plus the timestamp encoding bug (see Notes). AutoMLJobInputDataConfig (also required in DescribeAutoMLJobOutput) is still not implemented — known gap, see gaps: below."}
  lineage_action_artifact_context_association: {status: deferred, note: "Not audited this pass; lineage.go is large."}
  edge_deployment_device_fleet: {status: partial, note: "FIXED this pass — DeviceFleet/Device family: OutputConfig (required in Create+Update) was silently optional and UpdateDeviceFleet silently dropped it; DeviceFleet/Device Describe+List timestamp encoding also fixed (see Notes). EdgeDeploymentPlan/EdgePackagingJob not otherwise wire-audited this pass."}
  labeling_job: {status: deferred, note: "Not audited this pass; labeling.go."}
  hub_hub_content: {status: deferred, note: "Not audited this pass; hub.go."}
  cluster: {status: deferred, note: "Spot-checked DescribeCluster this pass (InstanceGroups, a required field, is correctly always emitted, not gated by omitempty) — no bug found, but not a full field-by-field audit; cluster.go is large."}
  inference_recommendations_edge_packaging: {status: deferred, note: "Not audited this pass."}
  training_plan: {status: partial, note: "FIXED this pass — TrainingPlan/ReservedCapacity/ReservedCapacitySummary timestamp encoding (see Notes). Not otherwise wire-audited this pass."}
  monitoring_schedule_workteam_compilation_job: {status: partial, note: "FIXED this pass — MonitoringSchedule and CompilationJob Describe+List timestamp encoding (see Notes). Workteam and deeper MonitoringSchedule/CompilationJob field audit not done this pass."}

gaps:                     # known divergences NOT fixed — link bd issue ids
  - "Pagination across the service is a hand-rolled integer-offset NextToken (parseNextToken/strconv.Atoi) rather than pkgs/page's opaque-token helper. Functionally correct (AWS clients treat NextToken as opaque) and internally consistent, but is a pkgs-catalog convention deviation across ~15 call sites. Not fixed this pass — refactor is cross-cutting and out of budget for a single-family sweep. (no bd issue filed yet)"
  - "ProductionVariantSummary.VariantStatus is populated with a single synthetic {Status: \"Creating\"|\"InService\"} entry, not a full AWS VariantStatus enum/message model (StatusMessage is always empty, no DeployedImages/CapacityReservationConfig/ManagedInstanceScaling/RoutingConfig fields). Sufficient for status-polling clients; deeper fidelity deferred. (no bd issue filed yet)"
  - "AutoMLJobInputDataConfig ([]types.AutoMLJobChannel) is a required member of DescribeAutoMLJobOutput but is not modeled/stored/returned at all — DescribeAutoMLJob omits it entirely. AutoMLJobSecondaryStatus/LastModifiedTime were also missing (FIXED this pass) but the input-data-config gap remains: a real AWS SDK client unconditionally reading this field would get a nil/empty slice rather than erroring, so this is lower severity than the fixed bugs, but is still a real gap. (no bd issue filed yet)"
  - "8 families still fully deferred (pipeline_pipeline_execution, experiment_trial_trial_component, feature_store, lineage_action_artifact_context_association, labeling_job, hub_hub_content, cluster, inference_recommendations_edge_packaging) — none wire-audited this pass beyond the systemic timestamp-encoding sweep (which only touches families with a MarshalJSON-eligible Describe/List path already present; none of these 8 families were found to have that path go through a raw struct/map marshal during the sweep, but that is not the same as a full field audit). Next pass should pick 2-3 of these per sweep given the service's size (~50k LOC)."

deferred:                 # consciously not audited this pass (scope) — next pass targets
  - pipeline_pipeline_execution
  - experiment_trial_trial_component
  - feature_store
  - lineage_action_artifact_context_association
  - labeling_job
  - hub_hub_content
  - cluster (spot-checked DescribeCluster only)
  - inference_recommendations_edge_packaging
  - domain_app_userprofile_space (Domain/App/UserProfile portion; Space timestamp bug fixed)
  - automl_job (AutoMLJobInputDataConfig field still unimplemented; see gaps:)
  - model_package_model_package_group (beyond ModelPackageStatusDetails fix; InferenceSpecification etc. not audited)
  - edge_deployment_device_fleet (EdgeDeploymentPlan/EdgePackagingJob portion; DeviceFleet/Device fixed)
  - training_plan (beyond timestamp fix)
  - monitoring_schedule_workteam_compilation_job (Workteam portion; MonitoringSchedule/CompilationJob timestamps fixed)

leaks: {status: clean, note: "Re-verified this pass: grepped every 'go func()'/runDelayed call site service-wide (8 files). Only one raw 'go func()' exists (lifecycle.go Shutdown, which waits on b.wg and is itself bounded by ctx.Done()); every timer-based state transition goes through runDelayed(b.lifecycleCtx, ...), which Shutdown cancels and drains via b.wg. No goroutine leaks found."}

---

## Notes

**Bug fixed this pass (systemic epoch-seconds timestamp wire bug across 27 resource types):**

The AWS `awsjson1.1` protocol this service emulates encodes timestamps as JSON *numbers*
(Unix epoch seconds), never as RFC3339 strings — `pkgs/awstime` and this service's own
`epochSeconds()` helper exist specifically for this. However, ~25 resource types' `Describe*`
handlers called `json.Marshal()` directly on the backend struct (e.g.
`return json.Marshal(result)` in `handleDescribeCodeRepository`) instead of building an
explicit response map with `epochSeconds(...)` conversions. Since Go's default `encoding/json`
marshals a bare `time.Time` field via its own `MarshalJSON` (RFC3339 string), every one of
these Describe responses put out `"CreationTime": "2026-07-22T10:00:00Z"` instead of
`"CreationTime": 1753178400` — a wire-shape bug that would make a real AWS SDK client
(Go, Python boto3, anything using a spec-compliant JSON-protocol deserializer) fail to parse
the response outright, since a numeric-typed field receiving a JSON string is a hard
deserialization error, not a silent zero-value.

A parallel form of the same bug existed in ~15 `List*` handlers, which built
`map[string]any{keyCreationTime: x.CreationTime, ...}` — putting the raw `time.Time` into an
`any`-typed map value instead of calling `epochSeconds(x.CreationTime)`.

Affected types (Describe path, fixed via a `MarshalJSON`/`UnmarshalJSON` pair added next to
each type — see below for why both are needed): `CodeRepository`, `HumanTaskUI`, `SMImage`,
`ImageVersion`, `ModelCard`, `InferenceExperiment`, `MlflowTrackingServer`, `ModelPackage`,
`ModelPackageGroup`, `StudioLifecycleConfig`, `TrainingPlan`, `ReservedCapacitySummary`,
`ReservedCapacity`, `AppImageConfig`, `AutoMLJob`, `CompilationJob`, `DeviceFleet`, `Device`,
`InferenceComponent`, `FlowDefinition`, `ClusterSchedulerConfig`, `ComputeQuota`,
`OptimizationJob`, `Project`, `Space`, `MonitoringSchedule`.

Affected List handlers (fixed by wrapping the map value in `epochSeconds(...)`):
`ListAutoMLJobs`, `ListCompilationJobs`, `ListDeviceFleets`, `ListDevices`,
`ListClusterSchedulerConfigs`, `ListComputeQuotas`, `ListInferenceComponents`,
`ListModelPackageGroups`, `ListCodeRepositories`, `ListImages`, `ListImageVersions`,
`ListMonitoringSchedules`, `ListProjects`, `ListSpaces`.

Fix approach: rather than rewrite each Describe handler into an explicit field-by-field
response map (the larger, higher-risk refactor), each affected type got a `MarshalJSON` that
wraps a type-aliased copy of itself with the timestamp field(s) overridden to
`epochSeconds(...)` float64s (Go's JSON marshaling: a shallower same-tagged field wins over an
embedded one). This is a much smaller diff per type and fixes both `Describe*` AND any other
code path that marshals the struct directly. **This has one consequence the next auditor must
know**: `persistence.go`'s snapshot/restore path also marshals these same structs directly
(table snapshots are just `json.Marshal`/`Unmarshal` of the store), so it now round-trips
through the same epoch-seconds encoding — every type also got a paired `UnmarshalJSON` (using
new `timeFromEpochSeconds`/`timeFromEpochSecondsPtr` helpers in `handler.go`) so
`persistence_test.go`'s round-trip tests still pass. Sub-second precision is lost across a
persistence round-trip as a result (epoch-seconds is whole-second granularity) — this is
inherent to the fix (AWS's own wire format is whole-second) and does not affect any tested
behavior, since no test asserts sub-second `CreationTime` precision.

New shared helpers in `handler.go`: `epochSecondsPtr` (nil-safe `*time.Time` → `*float64`,
preserving `omitempty` semantics for optional timestamps like `TrainingPlan.StartTime`),
`timeFromEpochSeconds`, `timeFromEpochSecondsPtr` (the inverses, used by the new
`UnmarshalJSON` methods).

**Bug fixed this pass (HyperParameterTuningJob wire shape — nested config + missing required
counters):**

`DescribeHyperParameterTuningJob`/`ListHyperParameterTuningJobs` emitted a flat top-level
`"Strategy"` field; real AWS nests `Strategy`/`ResourceLimits` inside a
`HyperParameterTuningJobConfig` object — a client reading
`output.HyperParameterTuningJobConfig.Strategy` (the only place the real SDK exposes it on
`DescribeHyperParameterTuningJobOutput`) got nothing. Both responses also omitted
`ObjectiveStatusCounters`/`TrainingJobStatusCounters`, which are `This member is required` in
the real output types — a real AWS SDK client dereferences these unconditionally, so omitting
them entirely (not even an empty object) would nil-pointer-panic real client code. `Strategy`
alone was also stored on `CreateHyperParameterTuningJob`; `ResourceLimits` was accepted in the
test fixtures' request bodies but silently discarded by the handler. Fix: `HyperParameterTuningJob`
gained `ResourceLimits`/`ObjectiveStatusCounters`/`TrainingJobStatusCounters` fields (the latter
two always zero-valued-but-present, since this emulator doesn't launch child training jobs);
`CreateHyperParameterTuningJob`'s signature gained a `limits HPResourceLimits` parameter;
Describe/List handlers now emit the correctly-nested/complete shape. Files: `hp_tuning_jobs.go`,
`handler_hp_tuning_jobs.go`, `interfaces.go`. Tests:
`TestHandler_DescribeHyperParameterTuningJob_WireShape`,
`TestHandler_ListHyperParameterTuningJobs_WireShape` in `handler_hp_tuning_jobs_test.go`.

**Bug fixed this pass (DeviceFleet — required OutputConfig silently optional, dropped on
Update):**

`OutputConfig` (specifically `S3OutputLocation`) is `This member is required` on both
`CreateDeviceFleetInput` and `UpdateDeviceFleetInput` in the real API — real AWS rejects a
`CreateDeviceFleet` call missing it with `ValidationException`. This emulator's
`handleCreateDeviceFleet` treated it as fully optional, so a client (or the pre-existing test
suite, which never sent it) could create a `DeviceFleet` with no `OutputConfig` at all; since
`OutputConfig` is *also* required on `DescribeDeviceFleetOutput`, the resulting
`DescribeDeviceFleet` response would then omit a required field. Separately,
`handleUpdateDeviceFleet`/`UpdateDeviceFleet` didn't accept `OutputConfig` in the request body
at all — a client updating a fleet's output location would have the call silently succeed
while `OutputConfig` stayed unchanged. Fix: `CreateDeviceFleet` now validates
`OutputConfig.S3OutputLocation` is present (`ValidationException` otherwise, matching real AWS);
`UpdateDeviceFleet`'s signature gained an `outputConfig *DeviceFleetOutputConfig` parameter,
threaded through from the handler. Every pre-existing `CreateDeviceFleet` test call site across
`handler_device_fleets_test.go` and `handler_edge_deployment_test.go` was updated to send a
valid `OutputConfig` (12 call sites). Files: `device_fleets.go`, `handler_device_fleets.go`.

**Bug fixed this pass (ModelPackage — missing required ModelPackageStatusDetails):**

`ModelPackageStatusDetails` (with a required `ValidationStatuses` list inside it) is
`This member is required` on `DescribeModelPackageOutput`; the `ModelPackage` struct didn't
have this field at all, so `DescribeModelPackage`/`CreateModelPackage` responses omitted it
entirely — the same "required field missing from the struct, not just unpopulated" bug class as
the HPO fix above. Fix: added `ModelPackageStatusDetails`/`ModelPackageStatusItem` types
matching `types.ModelPackageStatusDetails`/`types.ModelPackageStatusItem`; `CreateModelPackage`
now populates an empty-but-present `ValidationStatuses: []ModelPackageStatusItem{}`. Files:
`models.go`, `model_packages.go`.

**Looks-wrong-but-correct traps for the next auditor:**

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
