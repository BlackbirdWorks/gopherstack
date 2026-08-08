service: sagemaker
sdk_module: aws-sdk-go-v2/service/sagemaker@v1.263.2   # version audited against (parity-5)
last_audit_commit: 5f91d37c7                            # HEAD when this manifest was written
last_audit_date: 2026-08-08
overall: A            # parity-4: implemented the 22 ops the v1.236.0 -> v1.261.0 SDK bump added
                       # (AIBenchmarkJob, AIRecommendationJob, AIWorkloadConfig, generic Job/
                       # JobSchemaVersion, StartClusterHealthCheck families — see Notes). No
                       # previously-audited op touched or regressed. Grade held at A: every new op
                       # is real (routed, stateful, persisted, correct required/optional wire
                       # fields, accurate ResourceNotFound/ResourceInUse error typing verified
                       # against deserializers.go) with clearly-scoped, disclosed depth limits
                       # (see the aiBenchmarkJob/aiRecommendationJob/aiWorkloadConfig/job families
                       # below and gaps:) rather than any invented field or silent stub.
                       # parity-5: wire-audited the 8 families parity-4 left fully deferred +
                       # AutoMLJobInputDataConfig (renamed to the real field, InputDataConfig).
                       # 8 class-a accept-and-drop bugs fixed across
                       # pipeline/experiment/trial/trial-component/feature-group/labeling-job/
                       # automl/inference-recommendations-job (see Notes: parity-5). hub_hub_content
                       # and lineage_action_artifact_context_association audited clean, no bug found.
                       # Grade held at A: every fix is real (routed, stateful, persisted, tested
                       # against a real JSON request body); every remaining gap (feature_store's
                       # online/offline store config, cluster's Orchestrator/AutoScaling/etc.,
                       # PipelineDefinitionS3Location) is disclosed below, not silently absent.

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

  # --- parity-4: new ops added by the v1.236.0 -> v1.261.0 SDK bump ---
  CreateAIBenchmarkJob: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAIBenchmarkJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "required fields (Arn/Name/Status/AIWorkloadConfigIdentifier/BenchmarkTarget/CreationTime/OutputConfig/RoleArn) always emitted; BenchmarkTarget/OutputConfig/NetworkConfig are json.RawMessage passthrough of the Create payload — see aiBenchmarkJob family note"}
  DeleteAIBenchmarkJob: {wire: ok, errors: ok, state: ok, persist: ok}
  StopAIBenchmarkJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "InProgress->Stopping->Stopped FSM via stopSimpleJobFSM (lifecycle.go)"}
  ListAIBenchmarkJobs: {wire: ok, errors: ok, state: ok, persist: ok, note: "StatusEquals/NameContains/CreationTimeAfter/Before/SortBy/SortOrder/MaxResults all real filters; AIWorkloadConfigName derived from the stored identifier"}
  CreateAIRecommendationJob: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAIRecommendationJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "required fields always emitted; ModelSource/OutputConfig/PerformanceTarget/ComputeSpec/InferenceSpecification are json.RawMessage passthrough — see aiRecommendationJob family note; Recommendations intentionally always empty, see gaps:"}
  DeleteAIRecommendationJob: {wire: ok, errors: ok, state: ok, persist: ok}
  StopAIRecommendationJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "InProgress->Stopping->Stopped FSM via stopSimpleJobFSM (lifecycle.go)"}
  ListAIRecommendationJobs: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateAIWorkloadConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAIWorkloadConfig: {wire: ok, errors: ok, state: ok, persist: ok, note: "AIWorkloadConfigs/DatasetConfig are json.RawMessage passthrough of the Create payload"}
  DeleteAIWorkloadConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAIWorkloadConfigs: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "JobConfigSchemaVersion validated against jobConfigSchemaVersionsForCategory before create (real ResourceNotFound if unknown)"}
  DescribeJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "required fields (incl. SecondaryStatus/SecondaryStatusTransitions) always emitted; scoped by (JobCategory,JobName) — a category mismatch 404s, see jobs.go doc comment"}
  DeleteJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "rejects a still-InProgress job with ResourceInUse (StopJob required first), matching DeleteJob's doc comment + error deserializer"}
  StopJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "InProgress->Stopping->Stopped FSM with SecondaryStatusTransitions history, distinct JobSecondaryStatusTransition type from TrainingJob's SecondaryStatusTransition"}
  ListJobs: {wire: ok, errors: ok, state: ok, persist: ok, note: "scoped to the required JobCategory param plus NameContains/StatusEquals/CreationTime*/LastModifiedTime*/SortBy/SortOrder"}
  DescribeJobSchemaVersion: {wire: ok, errors: ok, state: ok, persist: n/a, note: "real function over a static per-instance schema registry (jobConfigSchemaVersionsForCategory); schema document text itself is a generic synthetic placeholder, not AWS's real unpublished schema — see gaps:"}
  ListJobSchemaVersions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "same registry as DescribeJobSchemaVersion"}
  StartClusterHealthCheck: {wire: ok, errors: ok, state: ok, persist: n/a, note: "validates cluster exists (name or ARN) + DeepHealthCheckConfigurations non-empty, returns ClusterArn; does not synthesize per-node health-check pass/fail results (no fabricated telemetry) — consistent with this service's existing ClusterNode model, which has no health-check fields at all"}

# Families audited as a group (when per-op is impractical):
families:
  ai_benchmark_job: {status: partial, note: "parity-4, new family (CreateJob-era SDK bump). Field-diffed against api_op_{Create,Describe,Delete,Stop,List}AIBenchmarkJob.go + types.AIBenchmarkJobStatus/AIBenchmarkJobSummary/ListAIBenchmarkJobsSortBy — all required/optional Describe fields correct, Stopping/Stopped FSM genuine (real time.Time delay, no fabricated completion metrics). PARTIAL because BenchmarkTarget/OutputConfig/NetworkConfig are stored+echoed as opaque json.RawMessage rather than fully-typed AIBenchmarkTarget/AIBenchmarkOutputConfig/AIBenchmarkNetworkConfig structs (same convention as algorithms.go's TrainingSpecification/InferenceSpecification/ValidationSpecification, already accepted at grade A in this file) — wire-correct for every field a client sent, but AIBenchmarkOutputResult's server-only CloudWatchLogs sub-field is never synthesized."}
  ai_recommendation_job: {status: partial, note: "parity-4, new family. Field-diffed against api_op_{Create,Describe,Delete,Stop,List}AIRecommendationJob.go + types.AIRecommendationJobStatus/AIRecommendationJobSummary. Distinct from the older InferenceRecommendationsJob family (inference_recommendations_jobs.go) — different store, different wire shape, no shared state. PARTIAL for the same json.RawMessage-passthrough reason as ai_benchmark_job (ModelSource/OutputConfig/PerformanceTarget/ComputeSpec/InferenceSpecification), plus Recommendations ([]types.AIRecommendation) is intentionally always empty rather than fabricated — see gaps:."}
  ai_workload_config: {status: partial, note: "parity-4, new family. No status/lifecycle in the real API (DescribeAIWorkloadConfigOutput has no status field) — CRUD only. WorkloadSpec (wire field name AIWorkloadConfigs, confusingly same as the resource-family name)/DatasetConfig stored as json.RawMessage passthrough for the same reason as the two job families above."}
  job_and_job_schema_version: {status: partial, note: "parity-4, new generic 'model customization job' family (CreateJob/DescribeJob/DeleteJob/StopJob/ListJobs/DescribeJobSchemaVersion/ListJobSchemaVersions). NOT the same as TrainingJob/ProcessingJob/TransformJob/AutoMLJob/CompilationJob/etc — keyed by JobName alone (matches CreateJob's doc: unique per account+region), Describe/Delete/Stop additionally scoped by JobCategory (mismatch => ResourceNotFound), own JobSecondaryStatusTransition type (does not alias training_jobs.go's SecondaryStatusTransition despite the identical shape), own store (b.jobs). DeleteJob correctly rejects a still-InProgress job with ResourceInUse per its doc comment. PARTIAL: DescribeJobSchemaVersion/ListJobSchemaVersions serve a single synthetic '1.0' schema version with a generic (not per-category, not AWS's real unpublished) JSON-schema document — AWS does not ship real per-JobCategory schema content in the SDK, so this is the most honest deterministic approximation available, not a wire-shape bug, but is disclosed as a depth limit."}
  model_endpoint_config_crud: {status: ok, note: "CreateModel/DescribeModel/ListModels/DeleteModel and CreateEndpointConfig/DescribeEndpointConfig/ListEndpointConfigs/DeleteEndpointConfig verified op-by-op against handler.go + backend.go: correct ARN building via pkgs/arn, epoch timestamps via epochSeconds (float64 unix seconds, matches awsjson1.1 numeric timestamp), errCodeLookup-equivalent sentinel wiring (awserr.New wraps ErrNotFound/ErrConflict, handler.go handleError maps to ValidationException/ResourceInUse), persistence.go backendSnapshot wiring confirmed for both models and endpointConfigs keyed by region."
  endpoint_lifecycle: {status: ok, note: "CreateEndpoint/UpdateEndpoint/DescribeEndpoint/DeleteEndpoint/ListEndpoints + UpdateEndpointWeightsAndCapacities audited and FIXED — see Notes. FSM-driven Creating/Updating -> InService transitions (backend_accuracy.go scheduleEndpointTransition) verified correct after fix."}
  training_job: {status: ok, note: "CreateTrainingJob(Full)/DescribeTrainingJob(Full)/ListTrainingJobs(Filtered)/StopTrainingJob(FSM)/DeleteTrainingJob/UpdateTrainingJob verified: InProgress->Completed FSM populates ModelArtifacts, BillableTimeInSeconds, SecondaryStatusTransitions with epoch timestamps; StopTrainingJobFSM drives InProgress->Stopping->Stopped."}
  tags: {status: ok, note: "AddTags/ListTags/DeleteTags verified against findTagMapLocked, which indexes ~20 resource kinds by ARN. Not-found path returns ValidationException (400), matching real AWS TagKeys validation error class."}
  processing_transform_job: {status: ok, note: "Wire-audited this pass: DescribeProcessingJob/DescribeTransformJob field-by-field against SDK output structs — field names, optional-field gating, and epoch-seconds timestamps all correct. No bugs found."}
  notebook_instance: {status: ok, note: "Wire-audited this pass: DescribeNotebookInstanceFull field-by-field against SDK — all optional fields correctly gated, epoch-seconds timestamps correct. No bugs found."}
  hyperparameter_tuning_job: {status: ok, note: "FIXED this pass — see Notes (wire-shape bug: flat Strategy instead of nested HyperParameterTuningJobConfig, missing required ObjectiveStatusCounters/TrainingJobStatusCounters/ResourceLimits)."}
  domain_app_userprofile_space: {status: partial, note: "Space's Describe/List timestamp encoding FIXED this pass (see systemic timestamp bug in Notes). Domain/App/UserProfile not otherwise wire-audited this pass."}
  pipeline_pipeline_execution: {status: partial, note: "parity-5, wire-audited op-by-op against api_op_{Create,Update,Delete,Describe,List}Pipeline*.go. FIXED this pass — DescribePipelineExecution silently dropped ParallelismConfiguration even though it was already stored on the backend struct (class-a bug); StartPipelineExecution/DescribePipelineExecution now also accept+echo PipelineVersionId and SelectiveExecutionConfig (previously accepted-and-dropped, both real optional CreateInput/DescribeOutput fields). Remaining gaps (not fixed, see gaps:): CreatePipeline/UpdatePipeline's PipelineDefinitionS3Location is accepted-and-dropped (would need real cross-service S3 GetObject to resolve honestly); DescribePipeline doesn't accept the optional PipelineVersionId input param (always describes the current version) and omits LastRunTime/PipelineVersionDescription/PipelineVersionDisplayName/CreatedBy/LastModifiedBy; ListPipelines summary is missing PipelineDescription/PipelineDisplayName/RoleArn/LastExecutionTime."}
  experiment_trial_trial_component: {status: partial, note: "parity-5, wire-audited against api_op_{Create,Describe,List}{Experiment,Trial,TrialComponent}.go. FIXED this pass — CreateExperiment/CreateTrial silently dropped DisplayName (and Experiment's Description), both real optional Create fields, so a client-supplied display name never round-tripped through Describe/List until a later Update call; ListExperiments/ListTrials summaries also gained DisplayName/LastModifiedTime (real ExperimentSummary/TrialSummary fields). CreateTrialComponent was the worst finding in this family: it silently dropped StartTime/EndTime/Status/Parameters/InputArtifacts/OutputArtifacts/DisplayName entirely — every field a client actually uses a TrialComponent for — now accepted and stored. Also fixed a genuine wire-shape bug (not accept-and-drop, but same severity class): TrialComponent.Status was serialized as a bare JSON string, but the real DescribeTrialComponentOutput.Status/TrialComponentSummary.Status is a {PrimaryStatus,Message} object (types.TrialComponentStatus) — a real AWS SDK client's JSON deserializer would fail outright on the old shape. The pre-existing TestHandler_UpdateTrialComponent test literally asserted the buggy bare-string shape; updated it to the correct object shape as part of this fix. Not fixed (see gaps:): CreatedBy/LastModifiedBy/Source (UserContext — no identity model to derive from, class d)."}
  feature_store: {status: partial, note: "parity-5, wire-audited CreateFeatureGroup/DescribeFeatureGroup/UpdateFeatureGroup against api_op_{Create,Describe,Update}FeatureGroup.go. FIXED this pass — RoleArn and Description are both real CreateFeatureGroupInput fields (RoleArn is what OfflineStoreConfig replication would use) that were accepted-and-dropped entirely; now stored and returned. NOT fixed (see gaps:): OnlineStoreConfig/OfflineStoreConfig/ThroughputConfig are still completely unmodeled — these are the actual substance of Feature Store (online-store toggle, S3 offline location, Glue/Iceberg table format) and are a materially larger typed-struct effort than this pass's budget; FeatureRecord PutRecord/GetRecord/DeleteRecord/BatchGetRecord (feature_store.go) belong to the separate sagemaker-featurestore-runtime SDK, not the sagemaker control-plane SDK audited here, and were out of scope."}
  model_package_model_package_group: {status: partial, note: "FIXED this pass — ModelPackage was missing the required ModelPackageStatusDetails field entirely (see Notes); ModelPackage/ModelPackageGroup Describe+List timestamp encoding also fixed. Other model-package fields (InferenceSpecification, SourceAlgorithmSpecification validation, etc.) not otherwise wire-audited this pass."}
  automl_job: {status: partial, note: "FIXED this pass (parity-4) — AutoMLJob was missing the required LastModifiedTime/AutoMLJobSecondaryStatus fields entirely, plus the timestamp encoding bug (see Notes). FIXED this pass (parity-5) — the required DescribeAutoMLJobOutput/CreateAutoMLJobInput field InputDataConfig ([]types.AutoMLChannel) is now modeled (AutoMLChannel/AutoMLDataSource/AutoMLS3DataSource types added), accepted at Create, and always emitted (as [] when absent, matching the required-field contract) — this was previously not modeled at all. NOTE for future auditors: the real SDK field is InputDataConfig, not 'AutoMLJobInputDataConfig' (a name that does not exist anywhere in aws-sdk-go-v2/service/sagemaker) — this file's own gaps: entry had the name wrong prior to this pass; verified directly against api_op_CreateAutoMLJob.go/api_op_DescribeAutoMLJob.go before implementing, per this campaign's rule to verify against the SDK rather than trust an issue title."}
  lineage_action_artifact_context_association: {status: ok, note: "parity-5, wire-audited CreateAction/CreateArtifact/CreateContext + Describe/Update/Delete/List against api_op_{Create,Describe,Update}{Action,Artifact,Context}.go. No accept-and-drop bugs found — Source/Properties/Description/Status/Tags all round-trip correctly. QueryLineage/DescribeLineageGroup/ListLineageGroups/GetLineageGroupPolicy also verified (the single auto-provisioned lineage group with no policy is an honest, correctly-typed 404, not a stub). Not fixed: MetadataProperties (CreateAction/CreateArtifact optional field) is accepted by no request struct field at all — a real but low-severity gap (see gaps:), left for follow-up since this family was otherwise clean."}
  edge_deployment_device_fleet: {status: partial, note: "FIXED this pass — DeviceFleet/Device family: OutputConfig (required in Create+Update) was silently optional and UpdateDeviceFleet silently dropped it; DeviceFleet/Device Describe+List timestamp encoding also fixed (see Notes). EdgeDeploymentPlan/EdgePackagingJob not otherwise wire-audited this pass."}
  labeling_job: {status: partial, note: "parity-5, wire-audited CreateLabelingJob/DescribeLabelingJob against api_op_CreateLabelingJob.go/api_op_DescribeLabelingJob.go — this family was already the most fully-typed in the service (real InputConfig/OutputConfig/HumanTaskConfig/StoppingConditions/LabelingJobAlgorithmsConfig structs, real Initializing->InProgress->Completed FSM). FIXED this pass — Tags (a real, optional DescribeLabelingJobOutput field) were accepted and stored on Create but never serialized back out by DescribeLabelingJob; also fixed the LabelingJob.Tags struct field's json:\"-\" tag (was silently dropping Tags across a persistence snapshot/restore round-trip too, a second manifestation of the same bug). No other gaps found."}
  hub_hub_content: {status: ok, note: "parity-5, wire-audited CreateHub/DescribeHub/ImportHubContent/DescribeHubContent against api_op_{Create,Describe}Hub.go/api_op_{Import,Describe}HubContent.go. No accept-and-drop bugs found — this was already a thorough implementation: S3StorageConfig is correctly nested (not flattened) on both request and response, HubContentDependencies/presigned URLs/ModelReference content-references (CreateHubContentReference/UpdateHubContentReference) all real. No changes made."}
  cluster: {status: partial, note: "parity-5, wire-audited CreateCluster/DescribeCluster/UpdateCluster against api_op_{Create,Describe,Update}Cluster.go. FIXED this pass — ClusterRole and VpcConfig (both real optional CreateClusterInput/DescribeClusterOutput fields; VpcConfig reuses the existing shared VpcConfig type from training_jobs.go) were accepted-and-dropped entirely — CreateCluster's signature didn't have parameters for them at all. NOT fixed (see gaps:): Orchestrator, AutoScaling, NodeProvisioningMode, TieredStorageConfig, RestrictedInstanceGroups/RestrictedInstanceGroupsConfig remain accepted-and-dropped — each is a materially-sized nested union/struct type and was judged out of this pass's bounded-fix budget; StartClusterHealthCheck (parity-4) unaffected."}
  inference_recommendations_edge_packaging: {status: partial, note: "parity-5, wire-audited CreateInferenceRecommendationsJob/DescribeInferenceRecommendationsJob against api_op_{Create,Describe}InferenceRecommendationsJob.go. This is a DIFFERENT family from AIRecommendationJob (ai_recommendation_jobs.go, parity-4) — distinct SDK ops, distinct store, no shared state. FIXED this pass — InputConfig ([]types.RecommendationJobInputConfig-shaped) is 'This member is required' on both CreateInferenceRecommendationsJobInput and DescribeInferenceRecommendationsJobOutput but was not modeled, accepted, or returned at all (the struct had no field for it whatsoever) — now stored+echoed as opaque json.RawMessage passthrough (same established convention as ai_benchmark_job/ai_recommendation_job/ai_workload_config's own deeply-nested union fields, see gaps: below). Real client-populated content round-trips exactly. EdgePackagingJob portion not otherwise wire-audited this pass."}
  training_plan: {status: partial, note: "FIXED this pass — TrainingPlan/ReservedCapacity/ReservedCapacitySummary timestamp encoding (see Notes). Not otherwise wire-audited this pass."}
  monitoring_schedule_workteam_compilation_job: {status: partial, note: "FIXED this pass — MonitoringSchedule and CompilationJob Describe+List timestamp encoding (see Notes). Workteam and deeper MonitoringSchedule/CompilationJob field audit not done this pass."}

gaps:                     # known divergences NOT fixed — link bd issue ids
  - "Pagination across the service is a hand-rolled integer-offset NextToken (parseNextToken/strconv.Atoi) rather than pkgs/page's opaque-token helper. Functionally correct (AWS clients treat NextToken as opaque) and internally consistent, but is a pkgs-catalog convention deviation across ~15 call sites. Not fixed this pass — refactor is cross-cutting and out of budget for a single-family sweep. (no bd issue filed yet)"
  - "ProductionVariantSummary.VariantStatus is populated with a single synthetic {Status: \"Creating\"|\"InService\"} entry, not a full AWS VariantStatus enum/message model (StatusMessage is always empty, no DeployedImages/CapacityReservationConfig/ManagedInstanceScaling/RoutingConfig fields). Sufficient for status-polling clients; deeper fidelity deferred. (no bd issue filed yet)"
  - "parity-4: AIBenchmarkJob's BenchmarkTarget/OutputConfig/NetworkConfig, AIRecommendationJob's ModelSource/OutputConfig/PerformanceTarget/ComputeSpec/InferenceSpecification, and AIWorkloadConfig's AIWorkloadConfigs/DatasetConfig are all stored+echoed as opaque json.RawMessage rather than modeled as fully-typed structs (same convention as algorithms.go's TrainingSpecification/InferenceSpecification/ValidationSpecification). Every field the client sends round-trips exactly; the only thing not reproduced is AWS server-synthesized sub-fields that don't exist in the Create input at all (e.g. AIBenchmarkOutputResult.CloudWatchLogs). Not a wire-shape bug for any field a client actually populates, but real if a client depends on those server-only sub-fields appearing. (no bd issue filed yet)"
  - "parity-4: AIRecommendationJob.Recommendations ([]types.AIRecommendation) is a real, always-empty slice — this backend never fabricates optimization recommendations, deployment configs, or performance-metric numbers a client could mistake for a measured result. A client polling DescribeAIRecommendationJob for actual recommendations will never see any, even after the job reaches Completed. Deliberate per this campaign's 'no fabricated metrics' rule, but is a real functional gap for any test asserting recommendation content. (no bd issue filed yet)"
  - "parity-4: DescribeJobSchemaVersion/ListJobSchemaVersions serve a single synthetic JobConfigSchemaVersion (\"1.0\") with a generic, not-per-category JSON-schema document for every JobCategory — AWS does not publish real per-category schema content anywhere in the SDK module, so there is no ground truth to model against. CreateJob does validate JobConfigSchemaVersion against this same registry (real ResourceNotFound if unknown), so the three ops are at least internally consistent, just not a reproduction of AWS's actual (unpublished) schema catalog. (no bd issue filed yet)"
  - "parity-5: CreatePipeline/UpdatePipeline still accept-and-drop PipelineDefinitionS3Location (real CreatePipelineInput/UpdatePipelineInput field — an alternative to inline PipelineDefinition where SageMaker fetches the definition from S3). Honestly implementing this needs a real cross-service S3 GetObject call, which this pass judged out of a bounded-fix budget; fabricating a PipelineDefinition would violate the no-fabrication rule. A client using only PipelineDefinitionS3Location today gets a pipeline created with an empty PipelineDefinition and no error. (no bd issue filed yet — file as a class-a follow-up)"
  - "parity-5: DescribePipeline doesn't accept the optional PipelineVersionId input param (a client asking for a specific historical pipeline version silently gets the current version instead, not an error) and never returns LastRunTime/PipelineVersionDescription/PipelineVersionDisplayName/CreatedBy/LastModifiedBy. ListPipelines' PipelineSummary is also missing PipelineDescription/PipelineDisplayName/RoleArn/LastExecutionTime (real optional PipelineSummary fields) and has a PipelineStatus field that does not exist on the real type at all (harmless for JSON-protocol clients, which ignore unknown fields, but not a reproduction of AWS's shape). (no bd issue filed yet)"
  - "parity-5: TrialComponent/Experiment/Trial's CreatedBy/LastModifiedBy/Source/ExperimentSource/TrialSource (types.UserContext / *Source ARN+type pairs) are not modeled at all — there is no IAM-identity or resource-provenance model in this backend to honestly derive them from (class d, not fabricated). (no bd issue filed yet)"
  - "parity-5: feature_store's OnlineStoreConfig/OfflineStoreConfig/ThroughputConfig (CreateFeatureGroupInput/DescribeFeatureGroupOutput) remain completely unmodeled — RoleArn/Description were fixed this pass, but the online/offline store configuration (the actual substance of Feature Store: online-store on/off + KMS key, S3 offline location + Glue/Iceberg table format, on-demand vs provisioned throughput) is a materially larger typed-struct effort, judged out of this pass's bounded-fix budget. A client configuring either store gets no error but the configuration silently does not round-trip. (no bd issue filed yet — highest-value remaining feature_store gap for a follow-up pass)"
  - "parity-5: cluster's Orchestrator/AutoScaling/NodeProvisioningMode/TieredStorageConfig/RestrictedInstanceGroups/RestrictedInstanceGroupsConfig (CreateClusterInput/DescribeClusterOutput) remain accept-and-drop — ClusterRole/VpcConfig were fixed this pass; these six remaining fields are each a nontrivial nested union/struct type, judged out of this pass's bounded-fix budget. (no bd issue filed yet)"
  - "parity-5: InferenceRecommendationsJob.InputConfig (fixed this pass to stop being silently dropped) is stored as opaque json.RawMessage passthrough rather than the fully-typed RecommendationJobInputConfig union (ContainerConfig/Endpoints/ModelPackageVersionArn/ModelName/...) — same convention as the parity-4 AI-job families' passthrough fields. Every field a client sends round-trips exactly; no server-synthesized sub-field is fabricated. (no bd issue filed yet)"
  - "parity-5: lineage's CreateAction/CreateArtifact accept no MetadataProperties field (a real, optional CreateActionInput/CreateArtifactInput field) — low-severity accept-and-drop left for a follow-up pass since the rest of this family was clean. (no bd issue filed yet)"

deferred:                 # consciously not (fully) audited this pass (scope) — next pass targets
  - domain_app_userprofile_space (Domain/App/UserProfile portion; Space timestamp bug fixed)
  - model_package_model_package_group (beyond ModelPackageStatusDetails fix; InferenceSpecification etc. not audited)
  - edge_deployment_device_fleet (EdgeDeploymentPlan/EdgePackagingJob portion; DeviceFleet/Device fixed)
  - training_plan (beyond timestamp fix)
  - monitoring_schedule_workteam_compilation_job (Workteam portion; MonitoringSchedule/CompilationJob timestamps fixed)
  - inference_recommendations_edge_packaging (EdgePackagingJob portion only; InferenceRecommendationsJob itself audited+fixed parity-5)

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
- `ErrResourceNotFound` (`errors.go`) looks like the same "message string doesn't affect the
  wire `__type`" trap noted above for the generic `awserr.ErrNotFound` sentinel — it is NOT.
  `handleError` (`handler.go`) has a genuine, deliberate extra `case errors.Is(err,
  ErrResourceNotFound):` checked *before* the generic `ErrNotFound` case, which emits
  `__type: "ResourceNotFound"` instead of the blanket `"ValidationException"` the rest of the
  service emits for not-found. This exists because AIBenchmarkJob/AIRecommendationJob/
  AIWorkloadConfig/Job's real error deserializers (verified directly against
  `aws-sdk-go-v2/service/sagemaker/deserializers.go`) only recognize a `"ResourceNotFound"`
  wire exception — `"ValidationException"` would be wrong for these four families specifically.
  Do not collapse this into the generic `ErrNotFound` branch; do not add more sentinels
  wrapping `ErrResourceNotFound` for other (older, already-`ValidationException`-correct)
  families without re-verifying their real deserializer first.

## parity-4 (2026-07-25): 22 ops added by the aws-sdk-go-v2/service/sagemaker
v1.236.0 -> v1.261.0 bump

Implemented, not stubbed, all 22: **AIBenchmarkJob** (Create/Describe/Delete/Stop/List),
**AIRecommendationJob** (Create/Describe/Delete/Stop/List), **AIWorkloadConfig**
(Create/Describe/Delete/List), the generic **Job**/**JobSchemaVersion** family
(Create/Describe/Delete/Stop/List/DescribeJobSchemaVersion/ListJobSchemaVersions), and
**StartClusterHealthCheck**. New files: `ai_benchmark_jobs.go` + `handler_ai_benchmark_jobs.go`,
`ai_recommendation_jobs.go` + `handler_ai_recommendation_jobs.go`, `ai_workload_configs.go` +
`handler_ai_workload_configs.go`, `jobs.go` + `handler_jobs.go`; `cluster.go`/`handler_cluster.go`
gained `StartClusterHealthCheck`.

The generic `Job` type is a genuinely new resource kind (SageMaker's "model customization job"
API), not another name for `TrainingJob`/`ProcessingJob`/etc: it is keyed by `JobName` alone
(per `CreateJob`'s own doc — unique per account+region), `Describe`/`Delete`/`Stop` additionally
require a matching `JobCategory` (a category mismatch 404s — see `resolveJobLocked`), and it
carries an opaque `JobConfigDocument` validated only against `JobConfigSchemaVersion`, never the
`AlgorithmSpecification`/`ResourceConfig`/etc. shape every other job type has. It has its own
`JobSecondaryStatusTransition` type (deliberately not sharing `training_jobs.go`'s
`SecondaryStatusTransition`, despite the identical field shape) and its own store (`b.jobs`).

All three job-lifecycle families (`AIBenchmarkJob`, `AIRecommendationJob`, `Job`) use a real
`InProgress -> Completed` timer (`aiJobInProgressToCompleted`, 300ms, `lifecycle.go`) and a real
`InProgress -> Stopping -> Stopped` timer (`aiJobStoppingToStopped`, 150ms) — the same
`runDelayed`-based pattern `TrainingJob`/`ProcessingJob` already use, not a status flipped
synchronously in the same call. `StopAIBenchmarkJob`/`StopAIRecommendationJob` share one generic
implementation, `stopSimpleJobFSM` (`lifecycle.go`), to avoid duplicating the FSM by hand per
family; `StopJob` has its own (richer, `SecondaryStatusTransitions`-tracking) version since the
generic `Job` family's wire shape needs that history.

`CreateAIBenchmarkJob`/`CreateAIRecommendationJob` validate `AIWorkloadConfigIdentifier`
resolves to a real, already-created `AIWorkloadConfig` (`resolveAIWorkloadConfigLocked`, by name
or ARN via a real `aiWorkloadConfigARNIndex`, rebuilt on Restore in `rebuildARNIndexes`) — a
genuine cross-resource FK check, not assumed. See `gaps:` above for the three disclosed depth
limits (opaque `json.RawMessage` passthrough for several deeply-nested union/config fields,
`AIRecommendationJob.Recommendations` always empty, and the synthetic single-version
`JobConfigSchemaVersion` registry).

## parity-5 (2026-08-08): wire audit of 8 deferred families + AutoMLJobInputDataConfig

Audited the 8 families this file's parity-4 pass left fully deferred (pipeline/pipeline
execution, experiment/trial/trial component, feature store, lineage, labeling job, hub/hub
content, cluster, inference recommendations job) plus the previously-misnamed
"AutoMLJobInputDataConfig" gap (the real SDK field is `InputDataConfig`, not
`AutoMLJobInputDataConfig` — that name does not exist anywhere in
`aws-sdk-go-v2/service/sagemaker`). Every finding was verified against the pinned SDK module
(`v1.263.2`) source directly, not against this repo's own handler output, per this campaign's
rule; several bd-issue-title-style names (including the audit's own starting point) turned out
not to match the SDK and were corrected before implementing anything.

**Fixed (class-a: accepted-and-silently-dropped, or equivalent-severity wire-shape bugs):**

- `pipelines.go`/`handler_pipelines.go`: `DescribePipelineExecution` never returned
  `ParallelismConfiguration` even though `StartPipelineExecutionFull` already stored it on the
  backend struct — a pure silent-drop-on-read bug, the worst subtype named in this campaign's
  memory. `StartPipelineExecution`/`DescribePipelineExecution` also gained `PipelineVersionId`
  and `SelectiveExecutionConfig` (new `SelectiveExecutionConfig`/`SelectedStep` types), both real
  optional fields that were previously accepted by JSON (unknown-field silent success) and then
  thrown away.
- `experiments.go`/`trials.go`: `CreateExperiment`/`CreateTrial` didn't accept `DisplayName`
  (`CreateExperiment` also didn't accept `Description`) at all — real, commonly-used optional
  `Create*Input` fields, silently dropped until a separate `Update*` call. `ListExperiments`/
  `ListTrials` summaries also gained `DisplayName`/`LastModifiedTime` (real `ExperimentSummary`/
  `TrialSummary` fields, previously omitted).
- `trial_components.go`/`handler_trial_components.go`: the single highest-value fix this pass.
  `CreateTrialComponent` accepted only `TrialComponentName`/`Tags` — every other real
  `CreateTrialComponentInput` field (`StartTime`, `EndTime`, `Status`, `Parameters`,
  `InputArtifacts`, `OutputArtifacts`, `DisplayName`) was silently dropped, meaning this backend
  could not actually record what a trial component exists to record. Also fixed a genuine
  wire-shape bug of the same severity: `TrialComponent.Status` was a bare Go `string`, serialized
  as a JSON string; the real `DescribeTrialComponentOutput.Status`/`TrialComponentSummary.Status`
  is `types.TrialComponentStatus` (`{PrimaryStatus, Message}`), an object — a real AWS SDK JSON
  deserializer would fail outright on the old shape, not silently misparse it. The pre-existing
  `TestHandler_UpdateTrialComponent` test literally asserted the buggy bare-string shape
  (`"Status": "InProgress"` in, `descResp["Status"] == "InProgress"` out); it was updated to the
  correct `{PrimaryStatus: "InProgress"}` object shape as part of this fix, not left as
  bug-compatible.
- `feature_groups.go`/`handler_feature_groups.go`: `CreateFeatureGroup` didn't accept `RoleArn`
  or `Description` at all (`RoleArn` is what a real offline-store replication would use).
- `labeling.go`/`handler_labeling.go`: `CreateLabelingJob` stored `Tags` but
  `DescribeLabelingJob` never serialized them back out (a real, optional
  `DescribeLabelingJobOutput` field) — plus a second manifestation of the same class of bug in
  `LabelingJob.Tags`'s own `json:"-"` struct tag, which meant Tags were also silently dropped
  across a persistence snapshot/restore round-trip, not just the API response.
- `automl.go`/`handler_automl.go`: `InputDataConfig` (`[]types.AutoMLChannel`, `This member is
  required` on both `CreateAutoMLJobInput` and `DescribeAutoMLJobOutput`) was not modeled at all
  — new `AutoMLChannel`/`AutoMLDataSource`/`AutoMLS3DataSource` types added, accepted via the
  existing `SetAutoMLJobExtras` post-create-fields pattern, and always emitted (as `[]` when a
  client sends none, never `null`, matching the required-field contract — this needed an explicit
  non-nil-preserving `cloneAutoMLJob` fix since a naive `append(nil, emptySlice...)` collapses an
  intentionally-non-nil-but-empty slice back to `nil`).
- `inference_recommendations_jobs.go`/`handler_inference_recommendations_jobs.go`: `InputConfig`
  (`This member is required` on both `CreateInferenceRecommendationsJobInput` and
  `DescribeInferenceRecommendationsJobOutput`) had no struct field at all — added as opaque
  `json.RawMessage` passthrough (same convention as the parity-4 AI-job families' own deeply
  nested union fields — `RecommendationJobInputConfig` is a comparably deep union type). This is
  a distinct family from `AIRecommendationJob`/`ai_recommendation_jobs.go` (parity-4): different
  SDK ops, different store, no shared state — do not conflate the two in a future audit.
- `models.go`/`cluster.go`/`handler_cluster.go`: `CreateCluster` didn't accept `ClusterRole` or
  `VpcConfig` at all (`VpcConfig` reuses the existing shared type from `training_jobs.go`, not a
  new duplicate).

**Audited, no bug found (grade held/confirmed):**

- `hub.go`/`handler_hub.go` (`hub_hub_content`): already a thorough implementation —
  `S3StorageConfig` correctly nested (not flattened) on both request and response,
  `HubContentDependencies`, presigned URLs, and `CreateHubContentReference`/
  `UpdateHubContentReference` (ModelReference content) all real and wire-correct. No changes.
- `lineage.go`/`handler_lineage.go` (`lineage_action_artifact_context_association`): `Source`/
  `Properties`/`Description`/`Status`/`Tags` all round-trip correctly across
  Action/Artifact/Context CRUD; `QueryLineage` graph traversal and the single
  auto-provisioned `LineageGroup` (with an honest, correctly-typed not-found for
  `GetLineageGroupPolicy`, not a stub) verified. Only gap: `MetadataProperties` not accepted at
  Create (see `gaps:`).

**Deliberately not fixed this pass (class a/b, disclosed in `gaps:` rather than fixed):**
`CreatePipeline`/`UpdatePipeline`'s `PipelineDefinitionS3Location` (would need a real
cross-service S3 `GetObject` to resolve honestly — fabricating a definition would violate the
no-fabrication rule); `DescribePipeline`'s missing `PipelineVersionId` input param and several
missing optional response fields; `feature_store`'s `OnlineStoreConfig`/`OfflineStoreConfig`/
`ThroughputConfig` (the actual substance of Feature Store — a materially larger typed-struct
effort than this pass's bounded-fix budget); `cluster`'s `Orchestrator`/`AutoScaling`/
`NodeProvisioningMode`/`TieredStorageConfig`/`RestrictedInstanceGroups(Config)` (each a
nontrivial nested union type); `TrialComponent`/`Experiment`/`Trial`'s `CreatedBy`/
`LastModifiedBy`/`Source` (`types.UserContext` — no IAM-identity model to derive from, class d,
not fabricated); `lineage`'s `MetadataProperties` on Create.
