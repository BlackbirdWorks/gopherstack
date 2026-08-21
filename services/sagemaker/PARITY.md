service: sagemaker
sdk_module: aws-sdk-go-v2/service/sagemaker@v1.263.2   # version audited against (parity-5)
last_audit_commit: 5f91d37c7                            # HEAD when this manifest was written
last_audit_date: 2026-08-08
                       # parity-6: fixed the actual gopherstack-e39w gap. parity-5's own note
                       # ("AutoMLJobInputDataConfig ... does not exist anywhere in
                       # aws-sdk-go-v2/service/sagemaker") was wrong — it IS real, it's the
                       # required field on CreateAutoMLJobV2Input (types.go / api_op_CreateAutoMLJobV2.go:91,
                       # []types.AutoMLJobChannel), not CreateAutoMLJobInput (V1). CreateAutoMLJobV2/
                       # DescribeAutoMLJobV2 were routed to the V1 handlers (handler_catalog.go),
                       # so V2's required AutoMLJobInputDataConfig/AutoMLProblemTypeConfig were
                       # silently dropped on every V2 request. Both ops now have dedicated
                       # handlers/wire shapes (handler_automl_v2.go/automl_v2.go); V1's own
                       # handleDescribeAutoMLJob was also changed to build an explicit response
                       # map instead of json.Marshal-ing the shared AutoMLJob struct directly,
                       # since the struct now carries V2-only fields too and would otherwise leak
                       # them into V1 responses for a job created via V2. See Notes: parity-6.
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
  domain_app_userprofile_space: {status: partial, note: "Space's Describe/List timestamp encoding FIXED parity-4 (see systemic timestamp bug in Notes). FIXED this pass (parity-7, gopherstack-oc9v) — this family was the largest concentration of anonymous inline request structs in the service (part of the 362 counted repo-wide) and had never been wire-audited; converted all 19 Create/Describe/List/Delete/Update handlers across Domain/App/Space/UserProfile to named types and found real gaps, not just a tooling blind spot. See Notes: parity-7 for the full list; highlights: CreateDomain was missing DefaultUserSettings entirely — a 'This member is required' CreateDomainInput field — so it was silently accepted-and-dropped rather than rejected; CreateApp had no way to create a Space-owned app at all (CreateAppInput.SpaceName, the real alternative to UserProfileName, didn't exist on the wire struct), so any client without a UserProfile could never launch an app even though this backend has supported Spaces since spaces.go; ListDomains/ListApps/ListSpaces/ListUserProfiles all silently ignored MaxResults and had none of ListApps'/ListSpaces'/ListUserProfiles' real SortBy/SortOrder/*Equals/*Contains filter-and-sort fields — the exact 'parsed field, silently ignored' defect class this campaign targets. All now real: MaxResults caps the page via paginateSlice, SortBy/SortOrder reorder by CreationTime/LastModifiedTime, UserProfileNameEquals/SpaceNameEquals/SpaceNameContains/UserProfileNameContains narrow the result set. DefaultSpaceSettings/DomainSettings/DomainSettingsForUpdate/UserSettings/OwnershipSettings/SpaceSettings/SpaceSharingSettings/ResourceSpec are carried as opaque json.RawMessage passthrough (established convention, see ai_workload_configs.go) rather than fully typed — these are all deeply-nested union/config shapes (UserSettings alone has ~20 app-specific sub-configs) out of this pass's budget; every field a client actually sends round-trips exactly. UpdateDomain went from a pure no-op (only bumped LastModifiedTime) to a real partial update of AppNetworkAccessType/AppSecurityGroupManagement/HomeEfsFileSystemCreation/TagPropagation/VpcId/SubnetIds/DefaultUserSettings/DefaultSpaceSettings/DomainSettingsForUpdate. See gaps: for what's still not modeled (DescribeApp/DescribeDomain's remaining server-derived/identity fields, UserSettings' internal structure)."}
  pipeline_pipeline_execution: {status: partial, note: "parity-5, wire-audited op-by-op against api_op_{Create,Update,Delete,Describe,List}Pipeline*.go. FIXED this pass — DescribePipelineExecution silently dropped ParallelismConfiguration even though it was already stored on the backend struct (class-a bug); StartPipelineExecution/DescribePipelineExecution now also accept+echo PipelineVersionId and SelectiveExecutionConfig (previously accepted-and-dropped, both real optional CreateInput/DescribeOutput fields). FIXED this pass (parity-6) — DescribePipeline now accepts the optional PipelineVersionId input (previously ignored, always describing the current version regardless; an unknown version now correctly errors instead of silently returning the current one) and returns LastRunTime (derived as the max StartTime across the pipeline's PipelineExecutions, or omitted if it has never run — a real, not fabricated, value). FIXED this pass (gopherstack-i359, session 2) — CreatePipeline/UpdatePipeline's PipelineDefinitionS3Location (api_op_CreatePipeline.go:59, api_op_UpdatePipeline.go:43) was previously accepted-and-dropped; honoring it for real needed a cross-service S3 GetObject call (out of scope that session — cli.go's S3 wiring was owned elsewhere), so it was rejected explicitly with a ValidationException instead of silently ignored. FIXED for real this pass (gopherstack-i359, session 3) — CreatePipeline/UpdatePipeline now fetch the real object through the backend's wired S3Accessor (services/sagemaker/s3pipeline.go, cli.go's wireSageMakerS3, same registry pattern as wireMGNS3/wireDynamoDBS3) and use its body as PipelineDefinition. The ValidationException path is retained only for the genuinely-unreadable case (no S3 backend wired, or GetObject/read failure against a real bucket/key) — an honest error, not a fabricated definition. Remaining gaps (not fixed, see gaps:): DescribePipeline still omits PipelineVersionDescription/PipelineVersionDisplayName/CreatedBy/LastModifiedBy; ListPipelines summary is missing PipelineDescription/PipelineDisplayName/RoleArn/LastExecutionTime."}
  experiment_trial_trial_component: {status: partial, note: "parity-5, wire-audited against api_op_{Create,Describe,List}{Experiment,Trial,TrialComponent}.go. FIXED this pass — CreateExperiment/CreateTrial silently dropped DisplayName (and Experiment's Description), both real optional Create fields, so a client-supplied display name never round-tripped through Describe/List until a later Update call; ListExperiments/ListTrials summaries also gained DisplayName/LastModifiedTime (real ExperimentSummary/TrialSummary fields). CreateTrialComponent was the worst finding in this family: it silently dropped StartTime/EndTime/Status/Parameters/InputArtifacts/OutputArtifacts/DisplayName entirely — every field a client actually uses a TrialComponent for — now accepted and stored. Also fixed a genuine wire-shape bug (not accept-and-drop, but same severity class): TrialComponent.Status was serialized as a bare JSON string, but the real DescribeTrialComponentOutput.Status/TrialComponentSummary.Status is a {PrimaryStatus,Message} object (types.TrialComponentStatus) — a real AWS SDK client's JSON deserializer would fail outright on the old shape. The pre-existing TestHandler_UpdateTrialComponent test literally asserted the buggy bare-string shape; updated it to the correct object shape as part of this fix. Not fixed (see gaps:): CreatedBy/LastModifiedBy/Source (UserContext — no identity model to derive from, class d)."}
  feature_store: {status: partial, note: "parity-5, wire-audited CreateFeatureGroup/DescribeFeatureGroup/UpdateFeatureGroup against api_op_{Create,Describe,Update}FeatureGroup.go. FIXED this pass — RoleArn and Description are both real CreateFeatureGroupInput fields (RoleArn is what OfflineStoreConfig replication would use) that were accepted-and-dropped entirely; now stored and returned. FIXED this pass (parity-6) — OnlineStoreConfig/OfflineStoreConfig/ThroughputConfig (CreateFeatureGroupInput/DescribeFeatureGroupOutput) are now fully modeled and round-trip: OnlineStoreConfig (EnableOnlineStore/StorageType/SecurityConfig.KmsKeyId/TtlDuration), OfflineStoreConfig (S3StorageConfig/DataCatalogConfig/TableFormat/DisableGlueTableCreation), ThroughputConfig (ThroughputMode/ProvisionedRead+WriteCapacityUnits — one Go type serves both CreateFeatureGroupInput.ThroughputConfig and DescribeFeatureGroupOutput.ThroughputConfigDescription since their fields are identical). NOT fixed (see gaps:): UpdateFeatureGroup's OnlineStoreConfigUpdate/ThroughputConfigUpdate (a distinct, separate update path from Create's fields, out of this pass's scope); LastUpdateStatus/OfflineStoreStatus/FailureReason/OnlineStoreTotalSizeBytes (DescribeFeatureGroupOutput fields describing async store-creation progress, not modeled); FeatureRecord PutRecord/GetRecord/DeleteRecord/BatchGetRecord (feature_store.go) belong to the separate sagemaker-featurestore-runtime SDK, not the sagemaker control-plane SDK audited here, and were out of scope."}
  model_package_model_package_group: {status: partial, note: "FIXED this pass — ModelPackage was missing the required ModelPackageStatusDetails field entirely (see Notes); ModelPackage/ModelPackageGroup Describe+List timestamp encoding also fixed. Other model-package fields (InferenceSpecification, SourceAlgorithmSpecification validation, etc.) not otherwise wire-audited this pass."}
  automl_job: {status: partial, note: "FIXED this pass (parity-4) — AutoMLJob was missing the required LastModifiedTime/AutoMLJobSecondaryStatus fields entirely, plus the timestamp encoding bug (see Notes). FIXED this pass (parity-5) — the required DescribeAutoMLJobOutput/CreateAutoMLJobInput field InputDataConfig ([]types.AutoMLChannel) is now modeled (AutoMLChannel/AutoMLDataSource/AutoMLS3DataSource types added), accepted at Create, and always emitted (as [] when absent, matching the required-field contract). CORRECTED+FIXED this pass (parity-6) — parity-5's note that 'AutoMLJobInputDataConfig does not exist in the SDK' was itself wrong: it is the required field on CreateAutoMLJobV2Input ([]types.AutoMLJobChannel, CreateAutoMLJobV2Input:91), a real, distinct-from-V1 field. CreateAutoMLJobV2/DescribeAutoMLJobV2 were routed to the V1 handlers and so silently dropped it (plus the required AutoMLProblemTypeConfig union) on every V2 request — the actual bug gopherstack-e39w asked for. Both ops now have their own handlers (handler_automl_v2.go) with the correct V2 wire shape: AutoMLJobInputDataConfig ([]AutoMLJobChannel, a narrower type than V1's AutoMLChannel — no TargetAttributeName/SampleWeightAttributeName), AutoMLProblemTypeConfig (5-member tagged union, carried opaque per gaps: below), AutoMLProblemTypeConfigName (derived from which union member is present), AutoMLComputeConfig/DataSplitConfig/SecurityConfig/ModelDeployConfig (all small flat types, fully modeled). handleDescribeAutoMLJob (V1) was also changed from json.Marshal(struct) to an explicit response map, since the shared AutoMLJob struct now carries V2-only fields that would otherwise leak into a V1 Describe of a V2-created job."}
  lineage_action_artifact_context_association: {status: ok, note: "parity-5, wire-audited CreateAction/CreateArtifact/CreateContext + Describe/Update/Delete/List against api_op_{Create,Describe,Update}{Action,Artifact,Context}.go. No accept-and-drop bugs found — Source/Properties/Description/Status/Tags all round-trip correctly. QueryLineage/DescribeLineageGroup/ListLineageGroups/GetLineageGroupPolicy also verified (the single auto-provisioned lineage group with no policy is an honest, correctly-typed 404, not a stub). FIXED (gopherstack-cgq3) — ListAssociations was missing CreatedAfter/CreatedBefore/DestinationType/MaxResults/SortBy/SortOrder (six of eleven real ListAssociationsInput members; the audit that found this counted six, but SourceType was also absent and is fixed alongside them) — the request had been an anonymous inline struct with only SourceArn/DestinationArn/AssociationType/NextToken, invisible to field-audit tooling (gopherstack-oc9v); now a named listAssociationsInput. All six (seven) fields are real filters/sorts, not accept-and-drop: SourceType/DestinationType resolve the entity's type via the existing lineageEntityLookup; CreatedAfter/CreatedBefore filter on Association.CreationTime; SortBy/SortOrder reorder by SourceArn/DestinationArn/SourceType/DestinationType/CreationTime (default); MaxResults truncates via the existing paginateSlice helper. Proven with TestHandler_ListAssociations_Filters/_Sort/_MaxResults, which assert on the actual narrowed/reordered/paginated result set, not just on the parsed request. FIXED this pass (parity-8, gopherstack-oc9v) — the remaining 19 inline `struct{...}` request declarations in this family (CreateArtifact/DescribeArtifact/UpdateArtifact/DeleteArtifact/ListArtifacts, CreateContext/DescribeContext/UpdateContext/DeleteContext/ListContexts, DescribeAction/UpdateAction/DeleteAction/ListActions, DeleteAssociation, DescribeLineageGroup/ListLineageGroups/GetLineageGroupPolicy, QueryLineage) converted to named types and wire-audited; MetadataProperties (the gap this note flagged since parity-5) is now real on both CreateArtifact and CreateAction; DeleteArtifact's Source alternative identity, five real filter/sort/pagination fields each on ListArtifacts/ListContexts/ListActions, ListLineageGroups' CreatedAfter/CreatedBefore/SortBy/SortOrder/MaxResults, and QueryLineage's Filters/MaxResults/NextToken are all now real. See Notes: parity-8 for the full list and for what remains disclosed rather than modeled (QueryFilters.Types)."}
  edge_deployment_device_fleet: {status: partial, note: "FIXED this pass — DeviceFleet/Device family: OutputConfig (required in Create+Update) was silently optional and UpdateDeviceFleet silently dropped it; DeviceFleet/Device Describe+List timestamp encoding also fixed (see Notes). EdgeDeploymentPlan/EdgePackagingJob not otherwise wire-audited this pass."}
  labeling_job: {status: partial, note: "parity-5, wire-audited CreateLabelingJob/DescribeLabelingJob against api_op_CreateLabelingJob.go/api_op_DescribeLabelingJob.go — this family was already the most fully-typed in the service (real InputConfig/OutputConfig/HumanTaskConfig/StoppingConditions/LabelingJobAlgorithmsConfig structs, real Initializing->InProgress->Completed FSM). FIXED this pass — Tags (a real, optional DescribeLabelingJobOutput field) were accepted and stored on Create but never serialized back out by DescribeLabelingJob; also fixed the LabelingJob.Tags struct field's json:\"-\" tag (was silently dropping Tags across a persistence snapshot/restore round-trip too, a second manifestation of the same bug). No other gaps found."}
  hub_hub_content: {status: ok, note: "parity-5, wire-audited CreateHub/DescribeHub/ImportHubContent/DescribeHubContent against api_op_{Create,Describe}Hub.go/api_op_{Import,Describe}HubContent.go. No accept-and-drop bugs found — this was already a thorough implementation: S3StorageConfig is correctly nested (not flattened) on both request and response, HubContentDependencies/presigned URLs/ModelReference content-references (CreateHubContentReference/UpdateHubContentReference) all real. No changes made."}
  cluster: {status: partial, note: "parity-5, wire-audited CreateCluster/DescribeCluster/UpdateCluster against api_op_{Create,Describe,Update}Cluster.go. FIXED parity-5 — ClusterRole and VpcConfig (both real optional CreateClusterInput/DescribeClusterOutput fields; VpcConfig reuses the existing shared VpcConfig type from training_jobs.go) were accepted-and-dropped entirely — CreateCluster's signature didn't have parameters for them at all. FIXED this pass (gopherstack-i359) — AutoScaling (types.ClusterAutoScalingConfig, Mode/AutoScalerType; DescribeCluster reports the required Status as InService, mirroring instanceGroupStatusInService's existing no-async-provisioning convention), NodeProvisioningMode (plain string), and TieredStorageConfig (types.ClusterTieredStorageConfig, Mode/InstanceMemoryAllocationPercentage) are now accepted on Create+Update and returned by Describe. Orchestrator (types.ClusterOrchestrator) is also now modeled — confirmed via botocore sagemaker/2017-07-24@1.43.56 service-2.json (`shapes.ClusterOrchestrator.type == \"structure\"`, not `\"union\"`) and serializers.go:27593-27612 that despite AWS's docs saying 'exactly one of Eks or Slurm', this is a plain struct with two independent optional members, not a discriminated wire union — so both fields decode independently and the exactly-one rule is enforced as a runtime ValidationException (api_op_CreateCluster.go:76-78) instead of a union tag. ALSO FIXED this pass (gopherstack-i359) — a persistence bug found while wiring the above: ClusterRole and VpcConfig (parity-5's fix) were never added to persistedCluster (persistence.go's hand-maintained Cluster DTO), so both were silently dropped across Snapshot/Restore even though CreateCluster/DescribeCluster round-tripped them correctly in memory; fixed alongside the four new fields. NOT fixed (see gaps:): RestrictedInstanceGroups/RestrictedInstanceGroupsConfig — judged too large to model faithfully within this pass's budget (ClusterRestrictedInstanceGroupSpecification alone nests EnvironmentConfig->FSxLustreConfig, a real 3-member InstanceStorageConfig union, and ScheduledUpdateConfig->DeploymentConfiguration->RollingDeploymentPolicy/AlarmDetails — six more nested types beyond the top-level spec); left entirely untouched rather than partially modeled. Re-examined a third time (gopherstack-i359, session 3): same conclusion, with the scope confirmed even larger than previously written up — see gaps: for the session-3 detail, including a wholly separate RestrictedInstanceGroupsConfig field this campaign hadn't previously named. StartClusterHealthCheck (parity-4) unaffected."}
  inference_recommendations_edge_packaging: {status: partial, note: "parity-5, wire-audited CreateInferenceRecommendationsJob/DescribeInferenceRecommendationsJob against api_op_{Create,Describe}InferenceRecommendationsJob.go. This is a DIFFERENT family from AIRecommendationJob (ai_recommendation_jobs.go, parity-4) — distinct SDK ops, distinct store, no shared state. FIXED this pass — InputConfig ([]types.RecommendationJobInputConfig-shaped) is 'This member is required' on both CreateInferenceRecommendationsJobInput and DescribeInferenceRecommendationsJobOutput but was not modeled, accepted, or returned at all (the struct had no field for it whatsoever) — now stored+echoed as opaque json.RawMessage passthrough (same established convention as ai_benchmark_job/ai_recommendation_job/ai_workload_config's own deeply-nested union fields, see gaps: below). Real client-populated content round-trips exactly. EdgePackagingJob portion not otherwise wire-audited this pass."}
  training_plan: {status: partial, note: "FIXED this pass — TrainingPlan/ReservedCapacity/ReservedCapacitySummary timestamp encoding (see Notes). Not otherwise wire-audited this pass."}
  monitoring_schedule_workteam_compilation_job: {status: partial, note: "FIXED this pass — MonitoringSchedule and CompilationJob Describe+List timestamp encoding (see Notes). Workteam and deeper MonitoringSchedule/CompilationJob field audit not done this pass."}
  studio_lifecycle_config: {status: ok, note: "FIXED this pass (gopherstack-5wj0) — CreateStudioLifecycleConfig accepted a request body with no field for StudioLifecycleConfigContent at all, even though it is 'This member is required' on CreateStudioLifecycleConfigRequest (botocore sagemaker service-2.json) and is also part of DescribeStudioLifecycleConfigResponse. Every real client's script content was silently discarded and Create succeeded without it, where real AWS would reject the request. Now required, stored, and returned by Describe."}

gaps:                     # known divergences NOT fixed — link bd issue ids
  - "Pagination across the service is a hand-rolled integer-offset NextToken (parseNextToken/strconv.Atoi) rather than pkgs/page's opaque-token helper. Functionally correct (AWS clients treat NextToken as opaque) and internally consistent, but is a pkgs-catalog convention deviation across ~15 call sites. Not fixed this pass — refactor is cross-cutting and out of budget for a single-family sweep. (no bd issue filed yet)"
  - "ProductionVariantSummary.VariantStatus is populated with a single synthetic {Status: \"Creating\"|\"InService\"} entry, not a full AWS VariantStatus enum/message model (StatusMessage is always empty, no DeployedImages/CapacityReservationConfig/ManagedInstanceScaling/RoutingConfig fields). Sufficient for status-polling clients; deeper fidelity deferred. (no bd issue filed yet)"
  - "parity-4: AIBenchmarkJob's BenchmarkTarget/OutputConfig/NetworkConfig, AIRecommendationJob's ModelSource/OutputConfig/PerformanceTarget/ComputeSpec/InferenceSpecification, and AIWorkloadConfig's AIWorkloadConfigs/DatasetConfig are all stored+echoed as opaque json.RawMessage rather than modeled as fully-typed structs (same convention as algorithms.go's TrainingSpecification/InferenceSpecification/ValidationSpecification). Every field the client sends round-trips exactly; the only thing not reproduced is AWS server-synthesized sub-fields that don't exist in the Create input at all (e.g. AIBenchmarkOutputResult.CloudWatchLogs). Not a wire-shape bug for any field a client actually populates, but real if a client depends on those server-only sub-fields appearing. (no bd issue filed yet)"
  - "parity-4: AIRecommendationJob.Recommendations ([]types.AIRecommendation) is a real, always-empty slice — this backend never fabricates optimization recommendations, deployment configs, or performance-metric numbers a client could mistake for a measured result. A client polling DescribeAIRecommendationJob for actual recommendations will never see any, even after the job reaches Completed. Deliberate per this campaign's 'no fabricated metrics' rule, but is a real functional gap for any test asserting recommendation content. (no bd issue filed yet)"
  - "parity-4: DescribeJobSchemaVersion/ListJobSchemaVersions serve a single synthetic JobConfigSchemaVersion (\"1.0\") with a generic, not-per-category JSON-schema document for every JobCategory — AWS does not publish real per-category schema content anywhere in the SDK module, so there is no ground truth to model against. CreateJob does validate JobConfigSchemaVersion against this same registry (real ResourceNotFound if unknown), so the three ops are at least internally consistent, just not a reproduction of AWS's actual (unpublished) schema catalog. (no bd issue filed yet)"
  - "parity-5: DescribePipeline never returns PipelineVersionDescription/PipelineVersionDisplayName/CreatedBy/LastModifiedBy (PipelineVersionId input + LastRunTime output FIXED parity-6, see Notes). ListPipelines' PipelineSummary is also missing PipelineDescription/PipelineDisplayName/RoleArn/LastExecutionTime (real optional PipelineSummary fields) and has a PipelineStatus field that does not exist on the real type at all (harmless for JSON-protocol clients, which ignore unknown fields, but not a reproduction of AWS's shape). (no bd issue filed yet)"
  - "parity-5: TrialComponent/Experiment/Trial's CreatedBy/LastModifiedBy/Source/ExperimentSource/TrialSource (types.UserContext / *Source ARN+type pairs) are not modeled at all — there is no IAM-identity or resource-provenance model in this backend to honestly derive them from (class d, not fabricated). (no bd issue filed yet)"
  - "parity-6: feature_store's UpdateFeatureGroup does not accept OnlineStoreConfigUpdate/ThroughputConfigUpdate (CreateFeatureGroupInput's OnlineStoreConfig/OfflineStoreConfig/ThroughputConfig FIXED parity-6, see Notes — this is the separate Update-path pair of fields, out of that fix's scope) — nor LastUpdateStatus/OfflineStoreStatus/FailureReason/OnlineStoreTotalSizeBytes (DescribeFeatureGroupOutput fields describing async store-creation progress this backend has no notion of, since store creation is synchronous here). (no bd issue filed yet)"
  - "gopherstack-i359 (session 3, re-confirmed): cluster's RestrictedInstanceGroups/RestrictedInstanceGroupsConfig (CreateClusterInput/UpdateClusterInput/DescribeClusterOutput) remain accept-and-drop — Orchestrator/AutoScaling/NodeProvisioningMode/TieredStorageConfig were fixed in session 2 (see cluster: note above); PipelineDefinitionS3Location was fixed for real in session 3 (see pipeline_pipeline_execution: note above). RestrictedInstanceGroups was re-examined a third time this session rather than deferred by default, and the scope is confirmed larger than session 2's write-up: ClusterRestrictedInstanceGroupSpecification (types/types.go:5622) nests EnvironmentConfig->FSxLustreConfig (2 required fields), a real 3-member ClusterInstanceStorageConfig union (types/types.go:5107, EbsVolumeConfig/FsxLustreConfig/FsxOpenZfsConfig — confirmed a genuine Go interface union, not a struct-with-business-rule like ClusterOrchestrator turned out to be), and ScheduledUpdateConfig->DeploymentConfiguration->RollingDeploymentPolicy->CapacitySizeConfig (x2)/AutoRollbackConfiguration []AlarmDetails — 8 new leaf/union types, not 6, once the union's 3 members and RollingDeploymentPolicy's nested CapacitySizeConfig are counted individually. On top of that, CreateClusterInput/UpdateClusterInput/DescribeClusterOutput carry a SEPARATE field this campaign had not previously named — RestrictedInstanceGroupsConfig (types/types.go:5598) -> ClusterSharedEnvironmentConfig (types/types.go:5727, a required FSxLustreConfig + a required FSxLustreDeletionPolicy enum) — meaning the honest scope of 'RestrictedInstanceGroups' is two independent top-level fields, not one. Modeling all of this without shaving any field (this campaign's explicit rule, restated for this issue) is comparable in size to the entire session-2 pass that modeled Orchestrator/AutoScaling/NodeProvisioningMode/TieredStorageConfig combined. Left entirely untouched a third time, now with this deeper accounting on record so a future pass can scope it accurately instead of re-deriving the type tree from scratch. (no bd issue filed yet)"
  - "parity-5: InferenceRecommendationsJob.InputConfig (fixed this pass to stop being silently dropped) is stored as opaque json.RawMessage passthrough rather than the fully-typed RecommendationJobInputConfig union (ContainerConfig/Endpoints/ModelPackageVersionArn/ModelName/...) — same convention as the parity-4 AI-job families' passthrough fields. Every field a client sends round-trips exactly; no server-synthesized sub-field is fabricated. (no bd issue filed yet)"
  - "parity-5: lineage's CreateAction/CreateArtifact accept no MetadataProperties field (a real, optional CreateActionInput/CreateArtifactInput field) — low-severity accept-and-drop left for a follow-up pass since the rest of this family was clean. (no bd issue filed yet)"
  - "parity-6: CreateAutoMLJobV2/DescribeAutoMLJobV2's AutoMLProblemTypeConfig is a 5-member tagged union (ImageClassificationJobConfig/TabularJobConfig/TextClassificationJobConfig/TextGenerationJobConfig/TimeSeriesForecastingJobConfig), each itself a materially large nested struct (e.g. TabularJobConfig alone has CandidateGenerationConfig/FeatureSpecificationS3Uri/Mode/ProblemType/TargetAttributeName/...). Carried as opaque json.RawMessage passthrough, same established convention as this file's other deeply-nested unions (ai_benchmark_job/ai_recommendation_job/inference_recommendations_job) — every field a client sends round-trips exactly; only AutoMLProblemTypeConfigName (which member is present) is derived, not the member's internal fields. (no bd issue filed yet)"
  - "parity-6: DescribeAutoMLJobV2Output's BestCandidate/PartialFailureReasons/ResolvedAttributes/AutoMLJobArtifacts/EndTime/FailureReason/ModelDeployResult are not modeled — these are server-synthesized/derived fields that mirror V1 DescribeAutoMLJobOutput's pre-existing, disclosed depth limit (V1 has never modeled BestCandidate/ResolvedAttributes/etc. either); not a V2-specific regression, just not newly fixed by this pass. (no bd issue filed yet)"
  - "parity-7 (gopherstack-oc9v): Domain's DefaultUserSettings/DefaultSpaceSettings/DomainSettings, UserProfile's UserSettings, Space's OwnershipSettings/SpaceSettings/SpaceSharingSettings, and App's ResourceSpec are all carried as opaque json.RawMessage passthrough rather than fully-typed structs — UserSettings alone has ~20 app-specific sub-configs (JupyterServerAppSettings, KernelGatewayAppSettings, CanvasAppSettings, CodeEditorAppSettings, SpaceStorageSettings, ...), each individually as large as a small family already in this file. Every field a client sends round-trips exactly; no server-synthesized sub-field is fabricated. (no bd issue filed yet)"
  - "parity-7 (gopherstack-oc9v): DescribeApp/DescribeDomain still omit several real optional output-only fields this pass didn't add backend state for: App's EffectiveTrustedIdentityPropagationStatus/BuiltInLifecycleConfigArn/FailureReason/LastHealthCheckTimestamp/LastUserActivityTimestamp; Domain's FailureReason/HomeEfsFileSystemId/SecurityGroupIdForDomainBoundary/SingleSignOnApplicationArn/SingleSignOnManagedApplicationInstanceId/HomeEfsFileSystemKmsKeyId (deprecated, superseded by KmsKeyId which IS modeled). These are server-derived/lifecycle fields with no synchronous backend process to derive them from truthfully; left absent rather than fabricated. (no bd issue filed yet)"

deferred:                 # consciously not (fully) audited this pass (scope) — next pass targets
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

## parity-6 (2026-08-08): CreateAutoMLJobV2/DescribeAutoMLJobV2 dedicated handlers

Fixes the actual gap bd issue `gopherstack-e39w` asked for. parity-5's own note claiming
`AutoMLJobInputDataConfig` "does not exist anywhere in aws-sdk-go-v2/service/sagemaker" was
wrong — verified directly against the pinned SDK (`v1.263.2`): it is
`CreateAutoMLJobV2Input.AutoMLJobInputDataConfig` (`api_op_CreateAutoMLJobV2.go:91`, `This
member is required`, `[]types.AutoMLJobChannel`), and also the corresponding
`DescribeAutoMLJobV2Output` field. It is a real, V2-only field distinct from V1's
`InputDataConfig` ([]types.AutoMLChannel) — parity-5 fixed the V1 field under a name that made
this look already-handled, when the V2 gap (what the issue title actually named) was untouched.

`handler_catalog.go` routed `CreateAutoMLJobV2`/`DescribeAutoMLJobV2` to the identical V1
handlers (`handleCreateAutoMLJob`/`handleDescribeAutoMLJob`). A V2 request's
`AutoMLJobInputDataConfig` and `AutoMLProblemTypeConfig` — both `This member is required` on
`CreateAutoMLJobV2Input` — were unknown JSON fields to the V1 request struct and silently
dropped; `DescribeAutoMLJobV2` never had a way to emit them back regardless. Full field-by-field
divergence (verified against `api_op_{Create,Describe}AutoMLJob{,V2}.go`):

- **Input, required, name differs**: `InputDataConfig` (V1) vs `AutoMLJobInputDataConfig` (V2) —
  different field name AND different element type (`types.AutoMLChannel` has
  `TargetAttributeName`/`SampleWeightAttributeName`; `types.AutoMLJobChannel` does not).
- **Input, required, V2 only**: `AutoMLProblemTypeConfig` (a 5-member tagged union —
  `ImageClassificationJobConfig`/`TabularJobConfig`/`TextClassificationJobConfig`/
  `TextGenerationJobConfig`/`TimeSeriesForecastingJobConfig`). V1 has no equivalent required
  field — the closest V1 fields, `ProblemType` (optional enum) and `AutoMLJobConfig`, are both
  optional and structurally unrelated.
- **Input, optional, V2 only**: `AutoMLComputeConfig`, `DataSplitConfig`, `SecurityConfig`. V1
  has none of these.
- **Input, optional, V1 only**: `AutoMLJobConfig`, `GenerateCandidateDefinitionsOnly`,
  `ProblemType`.
- **Input, optional, both (same type)**: `AutoMLJobObjective`, `ModelDeployConfig`, `Tags`.
- **Output**: mirrors the input divergence — `DescribeAutoMLJobV2Output` additionally returns
  `AutoMLProblemTypeConfigName` (derived, not a Create input) and `AutoMLComputeConfig`; V1's
  `DescribeAutoMLJobOutput` additionally returns `AutoMLJobConfig`/`ProblemType`/
  `GenerateCandidateDefinitionsOnly`. `ResolvedAttributes` exists on both but as different types
  (`types.ResolvedAttributes` vs `types.AutoMLResolvedAttributes`) — neither is modeled by this
  backend (pre-existing V1 depth limit, not new).
- **Create*Output for both versions is identical**: just `AutoMLJobArn`.

**Decision: separate handlers, not a shared one.** The required-field divergence alone rules out
a shared handler — a JSON struct that satisfies both `InputDataConfig`/`AutoMLJobInputDataConfig`
without misnaming one of them cannot exist, and `AutoMLProblemTypeConfig` has no V1 analogue to
silently reuse. `handleCreateAutoMLJobV2`/`handleDescribeAutoMLJobV2` (new `handler_automl_v2.go`)
were added; both versions still share the same `b.autoMLJobs` store and the same `AutoMLJob`
struct (AWS job names are unique across V1/V2 in the same account+region), but each op now
parses/emits its own accurate field subset via an explicit `map[string]any` response rather than
relying on the struct's own JSON tags.

This required also changing `handleDescribeAutoMLJob` (V1) from `json.Marshal(result)` (the
struct's default tags) to the same explicit-map style: since `AutoMLJob` now carries V2-only
fields (`AutoMLJobInputDataConfig`, `AutoMLProblemTypeConfig`), a V1 `Describe` of a job created
via `CreateAutoMLJobV2` would otherwise leak them into the V1 response shape — caught by a test
(`TestHandler_DescribeAutoMLJobV1_OmitsV2Fields`) asserting isolation both directions.

`AutoMLProblemTypeConfigName` is derived at Describe time from which single top-level key is
present in the opaque `AutoMLProblemTypeConfig` payload (`automlProblemTypeConfigName` in
`automl_v2.go`), matching the member->JSON-key mapping in `serializers.go`'s
`awsAwsjson11_serializeDocumentAutoMLProblemTypeConfig` — this is a real, verifiable derivation,
not a guess. `AutoMLProblemTypeConfig`'s member configs themselves (`TabularJobConfig` etc.) are
carried opaque (see `gaps:`), consistent with this service's established convention for other
deeply-nested unions.

Pre-fix verification: wrote `TestHandler_CreateAutoMLJobV2_RoundTrip` (table-driven,
full/minimal cases) against the pre-fix code first — both subtests failed with `DescribeAutoMLJobV2
must always emit AutoMLJobInputDataConfig`, confirming the field was silently absent from the V2
Describe response entirely (the exact bug class this issue names). All AutoML tests pass after
the fix; `go build ./...`, `go test -race ./services/sagemaker/...`, and
`golangci-lint run ./services/sagemaker/...` are clean.

**Bounded remainder (feature_store, DescribePipeline), same pass:**

`feature_groups.go`'s `CreateFeatureGroupOptions`/`FeatureGroup` gained `OnlineStoreConfig`
(`EnableOnlineStore`/`StorageType`/`SecurityConfig.KmsKeyId`/`TtlDuration`), `OfflineStoreConfig`
(`S3StorageConfig`/`DataCatalogConfig`/`TableFormat`/`DisableGlueTableCreation`), and
`ThroughputConfig` (`ThroughputMode`/`ProvisionedReadCapacityUnits`/`ProvisionedWriteCapacityUnits`)
— all verified field-by-field against `types.{OnlineStoreConfig,OfflineStoreConfig,
ThroughputConfig,ThroughputConfigDescription,S3StorageConfig,DataCatalogConfig,
OnlineStoreSecurityConfig,TtlDuration}` in `types/types.go`. None of these are unions; all are
small flat structs, so all are fully typed rather than carried opaque. `ThroughputConfig` (Create
input) and `ThroughputConfigDescription` (Describe output) are distinct SDK type names with
identical fields — this backend uses one Go type, `ThroughputConfig`, for both, since the wire
shape is the same either direction. Pre-fix verification:
`TestHandler_CreateFeatureGroup_StoreConfigsRoundTrip` failed with `OnlineStoreConfig must
round-trip` (the field was entirely absent from the Describe response) before the fix.

`pipelines.go`'s `DescribePipeline` gained a `versionID int64` parameter
(`DescribePipelineInput.PipelineVersionId`) — when non-zero, it looks up that version in the
existing `pipelineVersionsStore` (already populated by every `CreatePipeline`/`UpdatePipeline`
call, per parity-5's pipeline-version-history work) and substitutes that version's
`PipelineDefinition`; an unknown version ID now returns `ErrPipelineNotFound` instead of silently
falling back to the current version. It also now returns `lastRunTime`
(`DescribePipelineOutput.LastRunTime`), computed as the max `StartTime` across the pipeline's
`PipelineExecution`s (matched by `PipelineArn`) — a real derived value, omitted (not zero-faked)
when the pipeline has never run. Pre-fix verification:
`TestHandler_DescribePipeline_PipelineVersionId/version_1_returns_original_definition` and
`/unknown_version_is_not_found` both failed (version 1 returned the current v2 definition; the
unknown version 99 returned 200 with the current pipeline instead of erroring), and
`TestHandler_DescribePipeline_LastRunTime` failed with `a pipeline that has run must emit
LastRunTime` — confirming the field was never emitted regardless of execution history.

Not attempted, per this issue's explicit scope: `PipelineDefinitionS3Location` (needs a real
cross-service S3 fetch) and the cluster family's six nested union/struct types
(`Orchestrator`/`AutoScaling`/`NodeProvisioningMode`/`TieredStorageConfig`/
`RestrictedInstanceGroups(Config)`).

Gates for the full pass: `go build ./...`, `go test -race ./services/sagemaker/...`, and
`golangci-lint run ./services/sagemaker/...` all clean, zero `nolint:{cyclop,gocyclo,gocognit,
funlen}` added.

## gopherstack-i359 (2026-08-09): cluster's five nested types + PipelineDefinitionS3Location rejection

Closes most of the "not attempted" list parity-5 left at the end of its section: five of
`cluster`'s six remaining nested types (`Orchestrator`/`AutoScaling`/`NodeProvisioningMode`/
`TieredStorageConfig`, plus a pre-existing persistence bug in `ClusterRole`/`VpcConfig`), and
`PipelineDefinitionS3Location`'s accept-and-drop. `RestrictedInstanceGroups(Config)` remains
untouched — see below.

**`ClusterOrchestrator` is not a wire union.** AWS's docs for `Orchestrator`
(`api_op_CreateCluster.go:72-78`, `sagemaker@v1.263.2`) read like a discriminated union
("you must provide exactly one orchestrator configuration: either Eks or Slurm"), and this issue
flagged it as "likely a union" needing care. Checked against botocore
(`sagemaker/2017-07-24@1.43.56 service-2.json.gz`, `metadata.protocol == "json"`,
`metadata.jsonVersion == "1.1"`): `shapes.ClusterOrchestrator.type == "structure"`, not
`"union"`. `serializers.go:27593-27612`'s `awsAwsjson11_serializeDocumentClusterOrchestrator`
confirms: it emits `Eks` and `Slurm` as two independent optional object keys, not a tagged
member. So `ClusterOrchestrator` is modeled here as a plain struct with two `*optional` pointer
fields (`ClusterOrchestratorEksConfig{ClusterArn}`, `ClusterOrchestratorSlurmConfig{
SlurmConfigStrategy}`), and the "exactly one" business rule is enforced as a runtime
`ValidationException` (`validateClusterOrchestratorLocked` in `cluster.go`), not a Go interface
union like `pkgs`/other services use for real smithy `@union` shapes.

**Fully modeled, all small flat structs verified field-by-field against `types/types.go`**
(`sagemaker@v1.263.2`): `ClusterAutoScalingConfig` (`Mode`/`AutoScalerType`, :4492),
`ClusterOrchestrator`/`ClusterOrchestratorEksConfig`/`ClusterOrchestratorSlurmConfig` (:5456,
:5470, :5483), `ClusterTieredStorageConfig` (`Mode`/`InstanceMemoryAllocationPercentage`, :5847),
and `NodeProvisioningMode` (a plain string enum, `ClusterNodeProvisioningMode`, :2674 — currently
only one real value, `Continuous`, but stored/echoed as an opaque string like `NodeRecovery`
already is, not validated against the enum). `DescribeClusterOutput.AutoScaling` uses a distinct
`ClusterAutoScalingConfigOutput` type that adds a required `Status` field (:4507) — this backend
reports it as `InService` once `AutoScaling` is set, the same no-async-provisioning convention
`instanceGroupStatusInService` already uses for instance groups; no other field is fabricated.
`Orchestrator`/`TieredStorageConfig` use the *same* Go SDK type on both `CreateClusterInput` and
`DescribeClusterOutput` (confirmed by reading both `api_op_CreateCluster.go` and
`api_op_DescribeCluster.go`), so no separate output shape was needed for those two.

**Left entirely untouched: `RestrictedInstanceGroups`/`RestrictedInstanceGroupsConfig`.**
`ClusterRestrictedInstanceGroupSpecification` (`types/types.go:5622`) has ~10 fields of its own
and nests `EnvironmentConfig` (`->FSxLustreConfig`), a real 3-member `ClusterInstanceStorageConfig`
union (`EbsVolumeConfig`/`FsxLustreConfig`/`FsxOpenZfsConfig`, a genuine Go `interface` union this
time — confirmed via `types/types.go:5107`'s `isClusterInstanceStorageConfig()` marker methods),
and `ScheduledUpdateConfig` (`->DeploymentConfiguration->RollingDeploymentPolicy`/
`AlarmDetails`) — six more nested types beyond the top-level spec. Per this campaign's rule
(medialive precedent, restated in this issue): a nested config whose fields are only partly
parsed is worse than an absent one, since callers can't tell what survived. Rather than shave
fields off this one to fit it into the same pass as the other four, it was left completely alone
— no parsing, no partial struct, no explicit-rejection error either (the issue's explicit
guidance was to "leave that one untouched," which explicit rejection would not be). Full support
would need all six additional types modeled, including the real union.

**Found and fixed in passing: a pre-existing `ClusterRole`/`VpcConfig` persistence bug.**
parity-5 added `ClusterRole`/`VpcConfig` to `Cluster`, `CreateClusterOptions`, and the
Create/Describe handlers, but never added them to `persistedCluster`
(`persistence.go`'s hand-maintained DTO for `Cluster`, needed because `Cluster.Nodes` carries
`json:"-"`) — so both fields round-tripped correctly in memory but were silently dropped across
every `Snapshot`/`Restore` cycle. Found while adding the four new fields to the same DTO; fixed
alongside them. Pre-fix repro (in a throwaway `git worktree` at the pre-`gopherstack-i359` HEAD,
using only fields that already existed then): create a cluster with `ClusterRole`+`VpcConfig`,
`Snapshot`, `Restore` into a fresh backend, `DescribeCluster` — `ClusterRole` came back `""` and
`VpcConfig` came back absent. `TestPersistenceRoundtrip_ClusterFullFields` now guards this
(and the four new fields) permanently.

**`PipelineDefinitionS3Location` rejected explicitly, not silently dropped.**
`CreatePipelineInput`/`UpdatePipelineInput` (`api_op_CreatePipeline.go:59`,
`api_op_UpdatePipeline.go:43`) both accept `PipelineDefinitionS3Location` (`Bucket`/`ObjectKey`/
optional `VersionId`, `types/types.go:17313`) as an S3-backed alternative to inline
`PipelineDefinition`. Honoring it for real needs a cross-service S3 `GetObject` call — the
registry-wiring pattern `cli.go` uses for `wireStepFunctionsServiceIntegrations`/
`wireAppConfigDeployments` — which touches `cli.go`, owned by another agent this session, so it
is out of scope here. Rather than continue accepting-and-silently-dropping the field (a client
relying on it today gets a pipeline created with an empty `PipelineDefinition` and no error, the
worst failure mode), `handleCreatePipelineFull`/`handleUpdatePipelineFull` now reject any request
that sets it with a `ValidationException`, following this service's own established
explicit-rejection precedent (`images.go`'s `UpdateImage` rejecting unsupported
`DeleteProperties` values via `ErrValidation`). Full support would need: a wired
`services/s3.InMemoryBackend` reference (or interface, matching `organizations_directory.go`'s
pattern in `cloudformation`), a `GetObject(bucket, key, versionID)` call resolving the definition
body, and using it as `PipelineDefinition` — real work, not attempted here.

Pre-fix verification: `TestPrefixCheck_ClusterRoleAndVpcConfig_SurviveSnapshotRestore` (temporary,
run against a `git worktree` at pre-`gopherstack-i359` HEAD, then discarded) failed as described
above. The `AutoScaling`/`Orchestrator`/`NodeProvisioningMode`/`TieredStorageConfig`/
`PipelineDefinitionS3Location` behaviors didn't exist in any form before this pass — there is no
meaningful "before" state beyond "the field is not in the request struct at all," already
established by reading the pre-edit `cluster.go`/`handler_cluster.go`/`handler_pipelines.go`.

All new/changed behavior verified through the real `aws-sdk-go-v2/service/sagemaker@v1.263.2`
client (`newTestSageMakerClient`), not hand-built JSON bodies:
`TestHandler_CreateCluster_NestedTypes_RealClient`,
`TestHandler_UpdateCluster_NestedTypes_RealClient`,
`TestHandler_CreatePipeline_S3Location_Rejected_RealClient`. Snapshot version not bumped — every
new field is additive with `omitempty`. Gates: `go build ./...`,
`go test -race ./services/sagemaker/... .`, and `golangci-lint run ./services/sagemaker/...` all
clean; zero `nolint:{cyclop,gocyclo,gocognit,funlen}` added.

## gopherstack-i359 (session 3, 2026-08-10): real S3 pipeline definitions; RestrictedInstanceGroups re-confirmed deferred

Closes the two items session 2 left open: wires real S3 fetching for
`PipelineDefinitionS3Location` (`cli.go` was owned by another agent in session 2), and makes a
fresh, deeper-researched call on `RestrictedInstanceGroups` rather than repeating the prior
deferral without re-checking it.

**`PipelineDefinitionS3Location` now genuinely fetches from S3.** New
`services/sagemaker/s3pipeline.go`: an `S3Accessor` interface
(`GetObject(ctx, *s3.GetObjectInput) (*s3.GetObjectOutput, error)`, identical shape to
`services/mgn/s3import.go`'s own `S3Accessor` — both are satisfied directly by
`services/s3.InMemoryBackend`, no adapter needed), `InMemoryBackend.SetS3Backend`/`s3Backend`
(same lock-guarded-field pattern as `services/mgn`), and `readPipelineDefinitionFromS3` (fetches,
caps the read at 64MiB — matching `services/mgn`'s identical import-source safety cap — and
errors via the sentinel `errPipelineDefinitionUnreadable` on a missing backend, missing
bucket/key, or an empty object). `cli.go` gained `wireSageMakerS3` (new function, same shape as
the pre-existing `wireMGNS3`/`wireDynamoDBS3`) called from `wireStorageAndSecretsIntegrations`.
`handleCreatePipelineFull`/`handleUpdatePipelineFull` now resolve `PipelineDefinitionS3Location`
through `readPipelineDefinitionFromS3` and use the fetched body as `PipelineDefinition`, instead
of unconditionally rejecting it. The `ValidationException` rejection path is retained, but now
only fires for the genuinely-unreadable case (no S3 backend wired, object missing, or a real
`GetObject`/read failure) — an honest error, never a fabricated definition, consistent with this
campaign's no-fabrication rule.

Per this repo's non-negotiable wiring-test requirement: `cli_sagemaker_s3_pipeline_wiring_test.go`
drives `initializeServices(appCtx)` (the function `Run()` actually calls, not `wireSageMakerS3`
called directly) through a real `aws-sdk-go-v2/service/sagemaker` client, creates a bucket and
object through the real S3 backend, calls `CreatePipeline` with `PipelineDefinitionS3Location`,
and asserts the fetched body round-trips through `DescribePipeline`. Verified with teeth: deleted
the `wireSageMakerS3(...)` call site from `wireStorageAndSecretsIntegrations` (not the helper
function) and re-ran the test — it failed with `ValidationException: ... no S3 backend
configured`, confirming the test is sensitive to the actual composition-root call site, not just
the helper's own correctness. Restored the call site and confirmed green again.

Package-level tests: `handler_pipelines_test.go` renamed the two prior
`*_S3Location_Rejected(_RealClient)` tests to `*_S3Location_UnreadableRejected(_RealClient)` (same
assertions — `newTestHandler` never wires an S3 backend, so the unreadable path still fires and
still returns `ValidationException`) and added `TestHandler_CreatePipeline_S3Location_Fetched`/
`TestHandler_UpdatePipeline_S3Location_Fetched` against a lightweight in-package
`mockPipelineS3` (mirrors `services/mgn`'s own test-only `mockS3` helper). Pre-fix verification:
copied the new/changed test files into a throwaway `git worktree` at the pre-session-3 HEAD
(pre-fix `s3pipeline.go` doesn't exist there) — `go vet` failed with `h.Backend.SetS3Backend
undefined`, confirming the new tests exercise code that did not exist before this pass; worktree
discarded after.

**Persistence: no DTO change needed, but guarded anyway.** `Pipeline` has no hand-maintained
persisted DTO (unlike `Cluster`'s `persistedCluster`, needed only because `Cluster.Nodes` carries
`json:"-"`) — it round-trips generically through `registry.SnapshotAll`/`RestoreAll` using its own
JSON tags, and `PipelineDefinition` already existed as a field before this pass. So there was
nothing new to add to `persistence.go`. Added
`TestPersistenceRoundtrip_PipelineDefinitionFromS3` anyway, both as a regression guard on the
new code path (a pipeline created from a fetched S3 definition round-trips its
`PipelineDefinition` through `Snapshot`/`Restore` like any other pipeline) and as a tripwire
against a future hand-maintained Pipeline DTO silently forgetting the field, the exact bug class
session 2 found for `Cluster`. Snapshot version not bumped — no new persisted field exists.

**`RestrictedInstanceGroups`: re-examined, still deferred — the scope is larger than previously
written up, not smaller.** Read directly against `types/types.go` (`sagemaker@v1.263.2`) rather
than trusting session 2's summary. Confirmed real: `ClusterInstanceStorageConfig`
(`types/types.go:5107`) is declared `interface { isClusterInstanceStorageConfig() }` with three
member wrapper types (`ClusterInstanceStorageConfigMemberEbsVolumeConfig`/
`MemberFsxLustreConfig`/`MemberFsxOpenZfsConfig`) — a genuine discriminated union, unlike
`ClusterOrchestrator` (session 2 found that one is a plain struct despite reading like a union in
prose). `serializers.go`'s `case *types.ClusterInstanceStorageConfigMemberEbsVolumeConfig:` etc.
confirm each member serializes under its own field name, the expected union wire shape.

Beyond the union, the full type tree under `ClusterRestrictedInstanceGroupSpecification`
(`types/types.go:5622`, the `CreateClusterInput`/`UpdateClusterInput` shape) is:
`EnvironmentConfig` (`types/types.go:8395`) `->` `FSxLustreConfig` (`types/types.go:9152`, 2
required fields); `InstanceStorageConfigs []ClusterInstanceStorageConfig` (the union above, whose
3 members reference `ClusterEbsVolumeConfig`/`ClusterFsxLustreConfig`/`ClusterFsxOpenZfsConfig`,
`types/types.go:4548`/`4683`/`4704`); and `ScheduledUpdateConfig` (`types/types.go:20564`) `->`
`DeploymentConfiguration` (`types/types.go:7106`) `->` `RollingDeploymentPolicy`
(`types/types.go:20006`, itself nesting `CapacitySizeConfig` twice, `types/types.go:3824`) plus
`AutoRollbackConfiguration []AlarmDetails` (`types/types.go:841`). That is 8 new leaf/union types
once the union's members and `RollingDeploymentPolicy`'s nested `CapacitySizeConfig` are each
counted, not the "six" session 2 wrote down.

On top of that: `CreateClusterInput`/`UpdateClusterInput`/`DescribeClusterOutput` all also carry
`RestrictedInstanceGroupsConfig` (`types/types.go:5598`) — a field session 2's write-up never
named — which requires its own `SharedEnvironmentConfig` (`types/types.go:5727`,
`ClusterSharedEnvironmentConfig`: a required `FSxLustreConfig` plus a required
`FSxLustreDeletionPolicy` enum). So "`RestrictedInstanceGroups`" is honestly two independent
top-level fields, not one, and the combined faithful-modeling effort is comparable in size to all
four of session 2's cluster fixes (`Orchestrator`/`AutoScaling`/`NodeProvisioningMode`/
`TieredStorageConfig`) combined — while this session's other mandatory deliverable
(`PipelineDefinitionS3Location`, including its non-negotiable `cli.go` wiring-test proof) already
consumed a full pass's budget on its own.

Per this campaign's standing rule (medialive precedent, restated for this issue in session 2 and
again here): a nested config whose fields are only partly parsed is worse than an absent one,
because callers cannot tell what survived. Splitting the type tree to fit the remaining budget
would violate that rule as surely as skipping validation would. So `RestrictedInstanceGroups`/
`RestrictedInstanceGroupsConfig` are left untouched a third time — this time with the full,
verified type tree on record (`gaps:` entry above) so a future pass can scope and budget for it
accurately in one sitting, rather than re-deriving it from scratch a fourth time.

Gates for this session: `go build ./...`, `go test -race ./services/sagemaker/... .`, and
`golangci-lint run ./services/sagemaker/...` all clean; zero
`nolint:{cyclop,gocyclo,gocognit,funlen}` added.

## parity-7 (2026-08-13, gopherstack-oc9v): Domain/App/Space/UserProfile inline-struct sweep

gopherstack-oc9v sized a repo-wide blind spot: handlers that declare their request as an
anonymous inline `struct{...}` are invisible to both wire-sweep tools, which match on named
types. sagemaker held 362 of the repo's 1487 candidates — the largest concentration, and the
only service proven (via `ListAssociations`, fixed gopherstack-cgq3) to hide real bugs.

Per `PARITY.md`'s own frontmatter/families at the start of this session: sagemaker was already
graded A with an extensive per-op/family audit history (parity-4/5/6). The `domain_app_
userprofile_space` family was explicitly marked `deferred`/`partial` — "Domain/App/UserProfile
not otherwise wire-audited this pass" — making it the correct, honestly-scoped starting point:
real uncovered surface, not a re-derivation of already-verified work.

**Enumerated vs. converted vs. audited:** all 19 inline `struct{...}` request declarations
across `handler_domains.go` (5), `handler_apps.go` (4), `handler_spaces.go` (5), and
`handler_user_profiles.go` (5) were converted to named types (`createDomainInput`,
`describeDomainInput`, `listDomainsInput`, `deleteDomainInput`, `updateDomainInput`, and the
equivalent for App/Space/UserProfile) and wire-audited field-by-field against the pinned SDK
(`v1.263.2`: `api_op_{Create,Describe,List,Delete,Update}{Domain,App,Space,UserProfile}.go`).
This is a small slice of the repo-wide 362/1487, scoped deliberately (see gopherstack-oc9v's own
"work in deterministic order, state exactly where you stopped" instruction) rather than a shallow
pass over all of them — see that issue for what remains repo-wide.

**Findings, classified (a=absent entirely, b=wrong name, c=deliberately unmodelled):**

- (a) `CreateDomainInput.DefaultUserSettings` — `This member is required` — did not exist on the
  wire struct at all; a real client's mandatory field was silently accepted-and-dropped instead
  of rejected. Now required (`ValidationException` if absent) and stored as opaque
  `json.RawMessage` passthrough (`domains.go`).
- (a) `CreateAppInput.SpaceName` — the real, documented alternative to `UserProfileName`
  ("The name of the space. If this value is not set, then UserProfileName must be set.") — did
  not exist on the wire struct. A client with only a Space (no UserProfile) could never launch an
  app through `CreateApp`, even though this backend has modeled Spaces since `spaces.go`. Fixed:
  `CreateApp`/`DescribeApp`/`DeleteApp` now accept `SpaceName` as an alternative identity to
  `UserProfileName`, validated as mutually exclusive-and-required (one, not both, not neither).
- (a) `ListDomainsInput.MaxResults`, `ListAppsInput.{MaxResults,SortBy,SortOrder,
  SpaceNameEquals,UserProfileNameEquals}`, `ListSpacesInput.{MaxResults,SortBy,SortOrder,
  SpaceNameContains}`, `ListUserProfilesInput.{MaxResults,SortBy,SortOrder,
  UserProfileNameContains}` — none of these nine real filter/sort/pagination fields were modeled
  anywhere in the family; every `List*` silently used a fixed page size and insertion-order-ish
  sort regardless of what the client asked for. This is the exact "parsed field, silently
  ignored" defect class gopherstack-oc9v exists to find. All nine are now real: `MaxResults`
  caps the page via the existing `paginateSlice` helper; `SortBy`/`SortOrder` reorder by
  `CreationTime`/`LastModifiedTime` (the real `AppSortKey`/`SpaceSortKey`/`UserProfileSortKey`
  enum values, confirmed against `types/enums.go`); the four `*Equals`/`*Contains` filters narrow
  the result set.
- (c) `CreateAppInput.ResourceSpec`, `CreateSpaceInput.{OwnershipSettings,SpaceSettings,
  SpaceSharingSettings}`, `CreateUserProfileInput.UserSettings`,
  `CreateDomainInput.{DefaultSpaceSettings,DomainSettings}`, `UpdateDomainInput.
  DomainSettingsForUpdate` — deeply-nested config/union shapes (`UserSettings` alone has ~20
  app-specific sub-configs). Modeled as opaque `json.RawMessage` passthrough, the established
  convention in this file (`ai_workload_configs.go`, `algorithms.go`) for shapes materially
  larger than a single pass's budget — every field a client sends round-trips exactly through
  Create→Describe and a persistence Snapshot/Restore cycle; nothing is fabricated.
- (a) `CreateUserProfileInput.{SingleSignOnUserIdentifier,SingleSignOnUserValue}` — simple flat
  strings, previously absent; now modeled and round-tripped.
- `UpdateDomain` was a pure no-op beyond bumping `LastModifiedTime` — none of
  `UpdateDomainInput`'s nine real optional fields were accepted. Now a real partial update
  (`UpdateDomainOptions`/`applyUpdateDomainOptions`): each field overwrites only when the client
  supplies it, leaving the rest of the domain untouched, matching AWS's partial-update semantics.

**A second bug the conversion itself surfaced** (the exact pattern gopherstack-oc9v warned about
— "conversion itself surfaces gaps"): adding `SpaceName` to `appKey` fixed the request-shape gap,
but `store_domain.go`'s `appsStore`/`appsStoreRO` had their own separately-hand-written `keyFn`
closures that built the `App` table's primary key — and those were not updated alongside
`appKey`. The result: `CreateApp` computed its duplicate-check key with the new 5-field
`appKeyString` (including `SpaceName`), but `store.Table.Put` computed a *different* key via the
stale 4-field closure, so a Space-owned app was stored under one key and looked up under another
— `DescribeApp` returned `ResourceNotFound` for an app that had just been created successfully.
Caught immediately by `TestHandler_CreateApp_SpaceOwned` (added this pass) before this reached
any shared branch; fixed by updating both closures in `store_domain.go` to match `appKey`'s new
shape.

**Tests:** every fix above has a table-driven or targeted test that asserts on the actual
narrowed/reordered/capped/rejected result — not just that the request parsed. Verified against
unfixed code by temporarily reverting three representative fixes (the `DefaultUserSettings`
requiredness check, `ListDomains`' `MaxResults` wiring, and `ListApps`' `UserProfileNameEquals`
filter) one at a time and confirming the corresponding test fails, then restoring — the same
protocol used for the rest. `TestPersistenceRoundtrip_Domain` confirms the new fields survive a
Snapshot/Restore cycle, not just an in-process Describe.

**Not touched this pass:** `DescribeApp`/`DescribeDomain`'s remaining server-only derived fields
(see `gaps:` above); the internal structure of `UserSettings`/`SpaceSettings`/etc.; any of the
other 343 (362 minus the 19 converted here) inline structs elsewhere in this service —
gopherstack-oc9v remains open for those.

Gates for this session: `go build ./...`, `go vet ./services/sagemaker/...`,
`go test -race ./services/sagemaker/...`, `go fix -diff ./services/sagemaker/...` (no diff), and
`golangci-lint run ./services/sagemaker/...` all clean; zero
`nolint:{cyclop,gocyclo,gocognit,funlen}` added.

## parity-8 (2026-08-21, gopherstack-oc9v): ML Lineage family (Action/Artifact/Context/
Association/LineageGroup/QueryLineage) inline-struct sweep

Second pass of the gopherstack-oc9v campaign, sized at 362 candidate anonymous inline request
structs in sagemaker, 343 remaining after parity-7's Domain/App/Space/UserProfile family (19
converted). Per PARITY.md's own boundary note from parity-7 ("any of the other 343 ... elsewhere
in this service — gopherstack-oc9v remains open for those"), this pass took the next coherent,
self-contained family: all 19 remaining anonymous `struct{...}` request declarations in
`handler_lineage.go` (`grep -c 'var req struct {' handler_lineage.go` = 19, exactly matching
343's per-file count) — CreateArtifact/DescribeArtifact/UpdateArtifact/DeleteArtifact/
ListArtifacts, CreateContext/DescribeContext/UpdateContext/DeleteContext/ListContexts,
DescribeAction/UpdateAction/DeleteAction/ListActions, DeleteAssociation,
DescribeLineageGroup/ListLineageGroups/GetLineageGroupPolicy, QueryLineage. **324 of sagemaker's
362 inline structs now remain** (362 − 19 parity-7 − 19 parity-8). This pass did not touch any
other family; PARITY.md's remaining 40+ `partial`/`deferred` entries and every other service file
are unaudited by this pass.

This family had already been wire-audited for *content* correctness in parity-5 (`lineage_action_
artifact_context_association: {status: ok, ...}`) and was clean except two disclosed gaps:
`ListAssociations`' six-then-seven absent members (fixed gopherstack-cgq3, the proof case for the
whole campaign) and `MetadataProperties` on `CreateAction`/`CreateArtifact`. Converting the
remaining 19 to named types re-confirmed the parity-5 finding was accurate, then went further:
diffing every converted struct field-by-field against `aws-sdk-go-v2/service/sagemaker@v1.263.2`
turned up five more absent-member groups parity-5's narrower "is Source/Properties/Tags correct"
pass had not been scoped to catch.

**Absent members added, per op, with SDK file:line:**

- `CreateArtifactInput.MetadataProperties` (`types/types.go:13617..13631`, `*types.
  MetadataProperties{CommitId,GeneratedBy,ProjectId,Repository}`) — the parity-5-disclosed gap,
  now fixed: a flat 4-string struct, no reason to defer it to opaque passthrough like the
  Domain/App family's genuinely-huge configs. Threaded through `Artifact.MetadataProperties` and
  returned by `DescribeArtifact`. **`CreateActionInput.MetadataProperties`
  (`api_op_CreateAction.go`) had the identical gap** on `createActionRequest` — a named type, so
  technically outside this pass's inline-struct scope, but fixed alongside CreateArtifact's since
  it is the same root cause on the same shared type; threaded through `Action.MetadataProperties`
  and returned by `DescribeAction`.
- `DeleteArtifactInput.Source` (`api_op_DeleteArtifact.go:28-37`) — entirely absent. Real docs
  (docs.aws.amazon.com/sagemaker/latest/APIReference/API_DeleteArtifact.html): "Deletes an
  artifact. Either ArtifactArn or Source must be specified" — neither field is marked required on
  the Go struct because it's an either/or. Before this fix, `DeleteArtifact` unconditionally
  required `ArtifactArn`, so a client that (correctly, per the real API) supplied only `Source`
  got a `ValidationException` for a well-formed request. Now `Source.SourceUri` is a real
  alternative identity (`artifactArnBySourceURI`, deterministic lowest-ARN tie-break when
  multiple artifacts share a `SourceUri` — undocumented by AWS, disclosed here rather than
  guessed at silently).
- `ListArtifactsInput.{CreatedAfter,CreatedBefore,MaxResults,SortBy,SortOrder}`,
  `ListContextsInput.{CreatedAfter,CreatedBefore,MaxResults,SortBy,SortOrder}`,
  `ListActionsInput.{CreatedAfter,CreatedBefore,MaxResults,SortBy,SortOrder}` (`api_op_
  List{Artifacts,Contexts,Actions}.go`) — fifteen fields across three ops, all silently ignored
  before this pass (fixed page size, arbitrary ARN/name order). The exact "parsed field, silently
  dropped" defect class this campaign exists to find, same shape as parity-7's `ListDomains`/
  `ListApps`. All fifteen are now real: `MaxResults` caps the page via `paginateSlice`;
  `SortBy`/`SortOrder` reorder by `CreationTime` (default, all three) or `Name`
  (`ListContexts`/`ListActions` only — `SortArtifactsBy` has a single enum value, `CreationTime`,
  `types/enums.go:9056-9061`, so `ListArtifacts` correctly has no `Name` sort key);
  `CreatedAfter`/`CreatedBefore` filter on `CreationTime`. `ListContexts`/`ListActions` share one
  new generic helper (`filterSortPaginateByNameOrTime`, `list_helpers.go`) since their filter/sort
  shape is identical apart from field accessors — `ListArtifacts` does not share it since it lacks
  the `Name` sort key.
- `ListLineageGroupsInput.{CreatedAfter,CreatedBefore,MaxResults,SortBy,SortOrder}` (`api_op_
  ListLineageGroups.go`) — absent, and easy to dismiss as pointless since this backend only ever
  has one auto-provisioned lineage group. That's exactly why it was still a real bug: before this
  fix, `ListLineageGroups` returned the singleton unconditionally regardless of what
  `CreatedAfter`/`CreatedBefore` asked for — a `CreatedAfter` window that should exclude the one
  group still silently returned it. Fixed for real (`TestHandler_ListLineageGroups_CreatedWindow`
  asserts a future `CreatedAfter` returns an *empty* list). `SortBy`/`SortOrder` are accepted but
  are a genuine, disclosed no-op: no ordering of a 0-or-1-element list is observable, documented
  as such on `ListLineageGroupsParams` rather than silently doing nothing without saying so.
- `QueryLineageInput.{Filters,MaxResults,NextToken}` (`api_op_QueryLineage.go`) — absent
  entirely; `QueryLineage` returned every reachable vertex/edge with no filtering or pagination.
  `MaxResults`/`NextToken` now real (vertices paginated via `paginateSlice`; real docs describe
  both as bounding "the number of vertices", not edges — `api_op_QueryLineage.go:34,38` — so
  `Edges` is the full, unpaginated edge set between surviving vertices, not further paginated).
  `Filters` (`types.QueryFilters`, `types/types.go:19078-19108`) is mostly real:
  `LineageTypes`/`Properties`/`CreatedAfter`/`CreatedBefore`/`ModifiedAfter`/`ModifiedBefore` all
  narrow the result set against the vertex's resolved Action/Artifact/Context detail (a vertex
  that isn't a tracked Action/Artifact/Context — e.g. a `TrainingJob`/`Model`/`Endpoint` ARN — is
  excluded whenever any of these five filters is set, since this backend has no truthful
  timestamp/properties to check it against). **Disclosed, not modeled:** `Filters.Types` (matches
  entities by their AWS resource type, e.g. `DataSet`/`Model`/`Endpoint`) is parsed but not
  enforced — this backend has no per-service entity-type resolver for arbitrary ARNs outside
  Action/Artifact/Context, and building one is out of this pass's scope (it would mean threading
  type resolution through every other service this backend's lineage graph can reference).

**Bugs found beyond the wire diff:** none of the storage-key-inconsistency shape (`SpaceName`/
`appKey`) this campaign has twice found before — this family adds no new identity field to any
primary key (Artifact keyed by ARN, Context/Action keyed by name, both unchanged by this pass;
`DeleteArtifact`'s `Source` is an alternate *lookup* path onto the existing ARN key, not a new key
component). The two bugs of a different, still-real shape are the `ListLineageGroups` window-filter
gap and the pre-existing `ListArtifactsInput` requiring `ArtifactArn` even when the real API
accepts `Source` alone (`DeleteArtifact`) — both are "the field was accepted or partially modeled,
but the business rule around it was wrong or absent," the class of bug this campaign was
calibrated to expect but that a wire-field diff alone reliably surfaces once the fields exist to
diff.

**Tests:** every fix has a real-`aws-sdk-go-v2`-client round-trip test (`newTestSageMakerClient`,
not a raw-JSON-body `doSageMakerRequest` call) asserting on the actual behavior — narrowed/
reordered/paginated result sets, a `DescribeArtifact`/`DescribeAction` response actually carrying
`MetadataProperties`, a `DeleteArtifact` that actually deletes when only `Source` is given.
Verified against unfixed code by hand-reverting eight representative fixes one at a time
(`CreateArtifact`/`CreateAction` MetadataProperties, `DeleteArtifact` Source fallback, `ListArtifacts`
CreatedAfter/CreatedBefore/MaxResults/SortOrder, `QueryLineage` Filters, `QueryLineage` MaxResults/
NextToken, `ListLineageGroups` CreatedAfter/CreatedBefore, `ListContexts`/`ListActions`
SortBy=Name, `ListContexts`/`ListActions` CreatedAfter/CreatedBefore) and confirming the
corresponding test fails with the predicted symptom, then restoring — files verified byte-
identical (`md5sum`) to their pre-revert state afterward.

**Not touched this pass:** the other 324 (362 − 19 parity-7 − 19 parity-8) inline structs
elsewhere in this service — `handler_hub.go` (15), `handler_pipelines.go` (14), `handler_mlflow.go`
(14), `handler_model_packages.go` (12), `handler_notebook_instances.go` (11), `handler_images.go`
(11), `handler_edge_deployment.go` (11), and the rest — gopherstack-oc9v remains open for those.

Gates for this session: `go build ./...`, `go vet -tags e2e ./...`, `go vet -tags integration
./...`, `gofmt -l ./services/sagemaker` (empty), `go test -race ./services/sagemaker/...`, `go fix
-diff ./services/sagemaker/...` (no diff), and `golangci-lint run ./services/sagemaker/...` all
clean; zero `nolint:{cyclop,gocyclo,gocognit,funlen}` added (two `nolint:dupl` added on
`ListContexts`/`ListActions`, matching this repo's 98 existing precedents for that specific
linter, disclosed since it isn't in the banned group).

## parity-9 (2026-08-21, gopherstack-oc9v): Hub / HubContent family inline-struct sweep

Third pass of the gopherstack-oc9v campaign. Per parity-8's own boundary note ("324 of
sagemaker's 362 inline structs now remain ... `handler_hub.go` (15), `handler_pipelines.go` (14),
`handler_mlflow.go` (14), `handler_model_packages.go` (12) ... "), this pass took the largest
remaining single file, verified by `grep -c 'var req struct {' handler_hub.go` = 15 before
starting. All 15 were converted to named types (`createHubInput`, `describeHubInput`,
`listHubsInput`, `updateHubInput`, `deleteHubInput`, `importHubContentInput`,
`describeHubContentInput`, `listHubContentsInput`, `listHubContentVersionsInput`,
`deleteHubContentInput`, `createHubContentReferenceInput`, `deleteHubContentReferenceInput`,
`createHubContentPresignedURLsInput`, `updateHubContentInput`,
`updateHubContentReferenceInput`) and wire-audited field-by-field against the pinned SDK
(confirmed `v1.263.2` from `go.mod`, matching parity-7/8's assumption). **309 of sagemaker's 362
inline structs now remain** (362 − 19 − 19 − 15); `handler_hub.go` itself now has zero. This pass
did not touch `handler_pipelines.go`/`handler_mlflow.go`/`handler_model_packages.go` or any other
family — all still open for gopherstack-oc9v.

**Enumerated vs. converted vs. audited:** of the 15 ops, 11 already matched the real SDK struct
exactly once named (`CreateHub`, `DescribeHub`, `UpdateHub`, `DeleteHub`, `ImportHubContent`,
`DescribeHubContent`, `DeleteHubContent`, `CreateHubContentReference`,
`DeleteHubContentReference`, `UpdateHubContent`, `UpdateHubContentReference`). The other 4 had
absent members:

- `ListHubsInput` (`api_op_ListHubs.go:29-58`) — was missing **seven** of its nine fields:
  `CreationTimeAfter`, `CreationTimeBefore`, `LastModifiedTimeAfter`, `LastModifiedTimeBefore`,
  `MaxResults`, `SortBy`, `SortOrder` (only `NameContains`/`NextToken` existed). The exact "parsed
  field, silently dropped" class this campaign exists to find, at the largest count yet found in
  one op. All seven now real: the four timestamp windows filter on `CreationTime`/
  `LastModifiedTime`; `MaxResults` caps the page via `paginateSlice`; `SortBy` orders by
  `HubName`/`CreationTime`/`HubStatus` (real `HubSortBy` enum, `types/enums.go:3929-3944` — a
  fourth value, `AccountIdOwner`, has no distinguishing order in this single-account-per-region
  backend and is disclosed as a no-op tiebreak, the same shape as parity-8's
  `ListLineageGroups.SortBy`); `SortOrder` reorders ascending/descending. No default is documented
  by AWS for either field (checked docs.aws.amazon.com/sagemaker/latest/APIReference/API_ListHubs.html
  directly — neither the SDK struct comments nor the HTML docs state one), so the pre-existing
  unconditional HubName-ascending behavior was kept as the disclosed fallback rather than guessed.
- `ListHubContentsInput` (`api_op_ListHubContents.go:29-58`) — missing `CreationTimeAfter`,
  `CreationTimeBefore`, `MaxResults`, `MaxSchemaVersion`, `SortBy`, `SortOrder` (six of ten real
  fields). All six now real, same shape as above; `MaxSchemaVersion` is a new filter class this
  campaign hadn't hit yet — a `"\d{1,4}.\d{1,4}.\d{1,4}"` dotted-version upper bound compared via a
  new `compareDottedVersions` helper (`hub.go`), not a timestamp or a plain string.
- `ListHubContentVersionsInput` (`api_op_ListHubContentVersions.go:29-62`) — missing
  `CreationTimeAfter`, `CreationTimeBefore`, `MaxResults`, `MaxSchemaVersion`, `MinVersion`,
  `SortBy`, `SortOrder` (seven of eleven real fields) — `MinVersion` was a real lower-bound version
  filter previously entirely unimplementable since the field didn't exist on the wire at all.
- `CreateHubContentPresignedUrlsInput` (`api_op_CreateHubContentPresignedUrls.go:29-58`) —
  missing `AccessConfig`, `MaxResults`, `NextToken` (three of seven real fields). `MaxResults`
  (real documented default 100, confirmed via
  docs.aws.amazon.com/sagemaker/latest/APIReference/API_CreateHubContentPresignedUrls.html) and
  `NextToken` now paginate the real URL list via `paginateSlice`. `AccessConfig`
  (`types.PresignedUrlAccessConfig{AcceptEula,ExpectedS3Url}`, `types/types.go:17716-17729`) is
  modeled and round-tripped as `PresignedURLAccessConfig` but **disclosed, not enforced**: this
  backend has no concept of "gated" hub content requiring EULA acceptance to reject against, and no
  independently-resolved S3 URL to validate `ExpectedS3Url`'s consistency claim against — the same
  disclosed-no-op shape as parity-8's `ListLineageGroups.SortBy`/`SortOrder`, not a fabricated
  business rule.

**Bugs found beyond the wire diff:** none of the storage-key-inconsistency shape this campaign has
twice found before (`SpaceName`/`appKey` in parity-7). Every op in this family is keyed by
`HubName`/`HubContentType`/`HubContentName`/`HubContentVersion`, none of which changed shape this
pass — checked explicitly, no new field participates in a primary key here. One accept-and-drop
bug of the class this campaign is calibrated to expect, beyond the raw absent-field count: before
this pass, `CreateHubContentPresignedUrls` always returned every generated URL unconditionally
regardless of `MaxResults`/`NextToken` (both silently absent from the wire struct), so a client
capping the page size would have unknowingly received the full unpaginated set. In practice this
is observable only when `HubContentDependencies` is non-empty (2+ URLs) — and this backend (like
the real `ImportHubContent`/`CreateHubContentReference` request shapes it mirrors) has **no request
field that ever populates `HubContentDependencies`**, so every reachable call produces at most one
URL and the truncation path, while now implemented for real, is not exercisable through any public
request shape. Disclosed in `TestHandler_CreateHubContentPresignedUrls_AccessConfigAndPaging`'s doc
comment rather than fabricating a dependency-populating input this pass didn't add.

**Tests:** every fix has a real-`aws-sdk-go-v2`-client round-trip test (`newTestSageMakerClient`)
asserting on actual behavior — narrowed/reordered/paginated result sets, not just that the request
parsed. Verified against unfixed code by hand-reverting three representative fixes one at a time
(`ListHubs`' full `CreationTimeAfter/Before/LastModifiedTimeAfter/Before/SortBy/SortOrder/
MaxResults` wiring, `ListHubContents`' `MaxSchemaVersion` wiring, `ListHubContentVersions`'
`MinVersion` wiring) and confirming the corresponding tests failed with the predicted symptom
(wrong count, wrong order, or wrong membership), then restoring — `handler_hub.go`/`hub.go`
verified byte-identical (`md5sum`) to their pre-revert state afterward.

**Not touched this pass:** the other 309 (362 − 19 − 19 − 15) inline structs elsewhere in this
service — `handler_pipelines.go` (14), `handler_mlflow.go` (14), `handler_model_packages.go` (12),
`handler_notebook_instances.go` (11), `handler_images.go` (11), `handler_edge_deployment.go` (11),
and the rest — gopherstack-oc9v remains open for those. `HubContent`'s response-side
`OriginalCreationTime` (present on the real `HubContentInfo` summary per
docs.aws.amazon.com/sagemaker/latest/APIReference/API_HubContentInfo.html, absent from this
backend's `hubContentInfoSummary`) is a response-field gap, not a request-struct one, so it is
outside this pass's inline-struct scope — disclosed here rather than silently left unmentioned.

Gates for this session: `go build ./...`, `go vet ./services/sagemaker/...`, `go vet -tags e2e
./...`, `go vet -tags integration ./...`, `gofmt -l ./services/sagemaker` (empty), `go test -race
./services/sagemaker/...`, `go fix -diff ./services/sagemaker/...` (no diff), and `golangci-lint
run ./services/sagemaker/...` all clean; zero `nolint` of any kind added (fixed the `gocognit`/
`goconst`/`golines`/`fieldalignment`/shadow findings golangci-lint raised mid-pass by decomposing
`ListHubContents` into a `hubContentMatchesListParams` helper, reusing the existing
`keyCreationTime` constant and a new `keyHubContentStatus` constant, reordering two structs'
fields, and renaming three shadowed test `err`s — rather than suppressing any of them).

## parity-10 (2026-08-21, gopherstack-oc9v): Pipeline / PipelineExecution family inline-struct sweep

Fourth pass of the gopherstack-oc9v campaign. Per parity-9's boundary note ("309 of sagemaker's 362
inline structs now remain ... `handler_pipelines.go` (14), `handler_mlflow.go` (14), `handler_
model_packages.go` (12) ..."), this pass took `handler_pipelines.go`, verified by `grep -c 'var req
struct {' handler_pipelines.go` = 14 before starting. All 14 were converted to named types
(`retryPipelineExecutionInput`, `stopPipelineExecutionInput`, `sendPipelineExecutionStepSuccess
Input`, `sendPipelineExecutionStepFailureInput`, `listPipelineExecutionStepsInput`, `createPipeline
Input`, `updatePipelineInput`, `startPipelineExecutionInput`, `listPipelineParametersForExecution
Input`, `describePipelineInput`, `listPipelinesInput`, `deletePipelineInput`, `describePipeline
ExecutionInput`, `listPipelineExecutionsInput`) and wire-audited field-by-field against the pinned
SDK (`v1.263.2`, confirmed from `go.mod`, matching prior passes' assumption). **295 of sagemaker's
362 inline structs now remain** (362 − 19 − 19 − 15 − 14); `handler_pipelines.go` itself now has
zero. This pass did not touch `handler_mlflow.go`/`handler_model_packages.go` or any other family —
all still open for gopherstack-oc9v.

**Enumerated vs. converted vs. audited:** of the 14 ops, 4 already matched the real SDK struct
exactly once named (`StopPipelineExecution`, `UpdatePipeline`, `DescribePipeline`, `DescribePipeline
Execution`, `DeletePipeline` — modulo `ClientRequestToken`, see below). The rest had absent members
or, in two ops, fields that do not exist on the real wire at all:

- `ListPipelinesInput` (`api_op_ListPipelines.go:29-58`) — was missing **six** of its seven
  optional fields: `CreatedAfter`, `CreatedBefore`, `MaxResults`, `PipelineNamePrefix`, `SortBy`,
  `SortOrder` (only `NextToken` existed). The exact "parsed field, silently dropped" class this
  campaign exists to find. All six now real: `CreatedAfter`/`CreatedBefore` filter on
  `CreationTime`; `PipelineNamePrefix` filters by prefix; `MaxResults` caps the page via
  `paginateSlice`; `SortBy` orders by `CreationTime` (documented default,
  docs.aws.amazon.com/sagemaker/latest/APIReference/API_ListPipelines.html: "The default is
  CreatedTime") or `Name`; `SortOrder` has no documented default (confirmed by fetching the same
  page — only `SortBy`'s default is stated), so ascending is kept as the disclosed fallback,
  matching `ListHubs`'/`ListLineageGroups`' precedent.
- `ListPipelineExecutionsInput` (`api_op_ListPipelineExecutions.go:29-62`) — missing `CreatedAfter`,
  `CreatedBefore`, `MaxResults`, `SortBy`, `SortOrder` (five of seven real fields, only
  `PipelineName`/`NextToken` existed). Same shape and same fix pattern as `ListPipelines`; `SortBy`
  orders by `CreationTime` (documented default) or `PipelineExecutionArn`.
- `ListPipelineExecutionStepsInput` (`api_op_ListPipelineExecutionSteps.go:29-43`) — missing
  `MaxResults` and `SortOrder` (the op has no `SortBy`, sorting always by `CreationTime`/`StartTime`
  per its documented default); previously hardcoded ascending-by-`StepName`.
- `ListPipelineParametersForExecutionInput` (`api_op_ListPipelineParametersForExecution.go:29-42`)
  — missing `MaxResults`; previously returned every parameter unconditionally.
- `RetryPipelineExecutionInput` (`api_op_RetryPipelineExecution.go:29-45`) — missing
  `ParallelismConfiguration` entirely. Real docs: "if specified, overrides the parallelism
  configuration of the parent pipeline" — implying the *default*, unspecified case still applies
  the parent pipeline's configuration. Before this fix, a retried execution carried no parallelism
  configuration at all, not even the one its own pipeline was created with; now
  `RetryPipelineExecution` defaults to the parent `Pipeline.ParallelismConfiguration` via the
  existing `findPipelineByARNLocked` helper (`pipeline_versions.go`) when the caller doesn't
  override it.
- `StartPipelineExecutionInput` (`api_op_StartPipelineExecution.go:29-63`) — missing
  `MlflowExperimentName`. Threaded through to `PipelineExecution.MlflowExperimentName` and returned
  as `DescribePipelineExecutionOutput.MLflowConfig.MlflowExperimentName` (`types.MLflowConfiguration`,
  `types/types.go:13862`). **Disclosed, not modeled:** `MLflowConfig.MlflowResourceArn` (the
  tracking-server ARN) is left absent — this backend has no notion of which MLflow tracking server
  (`handler_mlflow.go`, a separate op family untouched by this pass) an execution is attached to, so
  fabricating an ARN would be a guess, not a fact.
- `SendPipelineExecutionStepSuccessInput`/`SendPipelineExecutionStepFailureInput` (`api_op_
  SendPipelineExecutionStepSuccess.go:29-43`, `api_op_SendPipelineExecutionStepFailure.go:29-42) —
  the real wire shape is `CallbackToken` (+ `OutputParameters` for Success, `FailureReason` for
  Failure) and nothing else. **The previous handler read two fields, `PipelineExecutionArn` and
  `StepName`, that do not exist on either real input type at all** — no real `aws-sdk-go-v2` client
  can ever populate them, since AWS resolves the target step from the opaque `CallbackToken` alone.
  `OutputParameters` — entirely absent before this pass — is the real gap: before this fix, a
  callback step's output parameters were silently discarded. Fixed for real: `PipelineExecutionStep`
  now carries `OutputParameters` and a `CallbackToken` field, both returned via `ListPipelineExecution
  Steps`' new `Metadata.Callback` (`types.CallbackStepMetadata`, `types/types.go:3641` — `SqsQueueUrl`
  is not modeled, since this backend never notifies a real SQS queue). This backend has no
  pipeline-definition step graph to generate distinct per-step callback tokens the way real AWS
  does, so — disclosed rather than silently narrowed — it treats the caller-supplied `CallbackToken`
  as the target execution's ARN (matching the existing test suite's own usage before this pass) and
  can record at most one trackable callback step per execution, under a fixed step name.
- `ClientRequestToken`, required on six of these fourteen ops (`RetryPipelineExecution`,
  `StopPipelineExecution`, `CreatePipeline`, `DeletePipeline`, `StartPipelineExecution`, and — via
  `SendPipelineExecutionStepSuccess`/`Failure` — two more), is a pure client-side idempotency token
  with no server-observable effect and, per a repo-wide grep, is not modeled by any op in this
  service — omitted here too rather than introducing the service's first (inert) instance of it.

**Bugs found beyond the wire diff:** three, all beyond a raw field-presence count:

1. `ListPipelines`/`ListPipelineExecutions` silently dropping every filter/sort control except
   `NextToken` (and, for the latter, `PipelineName`) — the "parsed field, silently dropped" class,
   at the largest per-op count this campaign has found outside `ListHubs`.
2. `SendPipelineExecutionStepSuccess`/`Failure` reading two fields no real client can ever send
   (`PipelineExecutionArn`, `StepName`) while silently dropping the one real field
   (`OutputParameters`) that exists beyond the identifier — the inverse of every prior finding in
   this campaign, which were all "real field present in the model, absent from the wire." Converting
   surfaced a wire-shape *fabrication*, not just an omission.
3. `RetryPipelineExecution` producing a retried execution with no parallelism configuration at all,
   even when the parent pipeline had one — silently narrower than both the explicit-override and the
   implicit-inherit paths the real API documents.

**Storage-key check:** none of this family's ops changed a primary key's shape. `Pipeline` is keyed
by `PipelineName`, `PipelineExecution` by `PipelineExecutionArn`, `PipelineExecutionStep` by
`ExecutionArn|StepName` (`pipelineExecutionStepsKey`) — the fixed callback step name introduced by
this pass replaces a caller-controllable value with a constant, which *narrows* addressability
(disclosed above) but does not change the key's shape or introduce inconsistency with any other
computation of it (there is exactly one `keyFn`, in `store_domain.go`'s `pipelineExecStepsStore`,
consistent before and after).

**Response-side completeness fixed alongside:** `PipelineSummary` (`ListPipelines`) was returning
only 5 of 8 real response fields (missing `PipelineDescription`, `PipelineDisplayName`, `RoleArn`,
`LastExecutionTime` — all data the backend already stored or could derive) and `PipelineExecution
Summary` (`ListPipelineExecutions`) only 3 of 6 (missing `PipelineExecutionDisplayName`,
`PipelineExecutionDescription`, `PipelineExecutionFailureReason`). Both fixed via a new exported
`(*InMemoryBackend).PipelineLastExecutionTime` helper (reusing the `latestExecutionStartTime` logic
`DescribePipeline`'s `LastRunTime` already relied on) and straightforward field copies. These are
response-side, not request-struct, gaps, so strictly outside this pass's inline-struct scope —
fixed anyway since they were adjacent, free, and directly observable by any real client.

**Tests:** every fix has a real-`aws-sdk-go-v2`-client round-trip test (`newTestSageMakerClient`)
asserting on actual behavior — narrowed/reordered/paginated result sets, `MLflowConfig` actually
present on `DescribePipelineExecution`, a retried execution's `ParallelismConfiguration` actually
inheriting or overriding, `ListPipelineExecutionSteps`' `Metadata.Callback` actually round-tripping
`OutputParameters`. Verified against unfixed code by hand-reverting six representative fixes one at
a time (`ListPipelines`' `PipelineNamePrefix`, `ListPipelineExecutions`' `CreatedAfter`/
`CreatedBefore`, `RetryPipelineExecution`'s `ParallelismConfiguration` inheritance/override,
`SendPipelineExecutionStepSuccess`'s `OutputParameters`, `StartPipelineExecution`'s
`MlflowExperimentName`, `ListPipelineParametersForExecution`'s `MaxResults`) and confirming each
corresponding test failed with the predicted symptom, then restoring — `pipelines.go`/`pipeline_
executions.go`/`handler_pipelines.go` verified byte-identical (`md5sum`) to their pre-revert state
afterward.

**Disclosed, not tested:** `ListPipelineExecutionSteps`' `MaxResults` truncation branch is real,
working code that cannot be exercised through any public request shape on this backend — Success/
Failure both write under the same fixed callback-step key, so a single execution can have at most
one step record ever, and `ListPipelineExecutionSteps` filters to one execution at a time. Same
shape as parity-9's `CreateHubContentPresignedUrls` disclosure: the test proves `MaxResults=1`
returns the one step with no `NextToken`, and says why a second page can't exist, rather than
fabricating a multi-step scenario this backend cannot produce.

Gates for this session: `go build ./...`, `go vet ./services/sagemaker/...`, `go vet -tags e2e
./...`, `go vet -tags integration ./...`, `gofmt -l ./services/sagemaker` (empty), `go test -race
./services/sagemaker/...`, `go fix -diff ./services/sagemaker/...` (no diff), and `golangci-lint run
./services/sagemaker/...` all clean; zero `nolint` of any kind added (fixed `golines`,
two `govet shadow`s, `prealloc`, `staticcheck S1016`, `testifylint`, and two `fieldalignment`
findings golangci-lint raised mid-pass — the latter two via `fieldalignment -fix` reordering
`PipelineExecutionStep`/`PipelineExecution`'s fields — rather than suppressing any of them).

Next by size, per parity-9's own list: `handler_mlflow.go` (14), `handler_model_packages.go` (12).
