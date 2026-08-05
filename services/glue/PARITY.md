---
service: glue
sdk_module: aws-sdk-go-v2/service/glue@v1.152.0
last_audit_commit: a7f9c5fb2
last_audit_date: 2026-08-05
overall: A            # 31 newly-shipped ops (business glossary, asset catalog, dashboard/session-endpoint) implemented for real; all 7 previously-tracked gaps + 10 deferred families remain closed from the prior pass
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateDatabase: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDatabase: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDatabases: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDatabase: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass (gopherstack-qd3.3): DatabaseInput/Database gained Parameters, LocationUri, CreateTableDefaultPermissions ([]PrincipalPermissions -> DataLakePrincipal), and TargetDatabase (*DatabaseIdentifier), field-diffed against types.DatabaseInput/types.Database. CreateDatabase/UpdateDatabase now clone (previously CreateDatabase returned the live map-stored pointer, same bug class as the prior pass's GetTables fix)"}
  CreateTable: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: added Parameters/Owner/Retention to Table+TableInput, and full StorageDescriptor (InputFormat/OutputFormat/SerdeInfo/Parameters/BucketColumns/SortColumns/Compressed/NumberOfBuckets/StoredAsSubDirectories) and Column.Parameters, all previously silently dropped"}
  GetTable: {wire: ok, errors: ok, state: ok, persist: ok}
  GetTables: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: was returning live backend *Table pointers uncloned (lock-bypass mutation/data-race risk); now clones like GetTable/SearchTables"}
  UpdateTable: {wire: ok, errors: ok, state: ok, persist: ok, note: "same field-completeness fix as CreateTable"}
  DeleteTable: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchDeleteTable: {wire: ok, errors: ok, state: ok, persist: ok}
  GetTableVersions: {wire: ok, errors: ok, state: ok, persist: ok, note: "not re-verified in depth this pass; existing coverage looked correct"}
  DeleteTableVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchDeleteTableVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  CreatePartition: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: BatchCreatePartition (which CreatePartition delegates to) never checked the parent table existed, silently storing orphaned partitions against a nonexistent db/table; now returns EntityNotFoundException per AWS contract. Also added Partition/PartitionInput.Parameters + Partition.CreationTime/CatalogId"}
  BatchCreatePartition: {wire: ok, errors: ok, state: ok, persist: ok, note: "same table-existence fix as CreatePartition"}
  GetPartition: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPartitions: {wire: ok, errors: ok, state: ok, persist: ok, note: "expression filter (segment) not re-verified in depth this pass"}
  BatchGetPartition: {wire: ok, errors: ok, state: ok, persist: ok, note: "SEVERE fix this pass: was a disguised stub — always returned an empty Partitions list regardless of backend state, with a comment falsely claiming \"the mock backend has no partition storage\". Now looks up each PartitionsToGet entry via GetPartition and reports misses in UnprocessedKeys per the real BatchGetPartitionResponse shape"}
  UpdatePartition: {wire: ok, errors: ok, state: ok, persist: ok, note: "now also persists Parameters through both the in-place and rename paths"}
  BatchUpdatePartition: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePartition: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchDeletePartition: {wire: ok, errors: ok, state: ok, persist: ok}
  CreatePartitionIndex: {wire: ok, errors: ok, state: ok, persist: ok, note: "not re-verified in depth this pass"}
  GetPartitionIndexes: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePartitionIndex: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateCrawler: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass (additively, gopherstack-qd3.1/qd3.2): CreateCrawler's positional signature is called from services/cloudformation (external package) so it was kept unchanged; CreateCrawlerWithOptions(...,CrawlerOptions) now also carries CrawlerSecurityConfiguration/SchemaChangePolicy/RecrawlPolicy/LineageConfiguration/LakeFormationConfiguration. CrawlerTarget/CrawlerTargets now models all 8 real target kinds (S3/JDBC/Catalog/DynamoDB/Delta/Hudi/Iceberg/MongoDB), field-diffed against types.CrawlerTargets — previously only S3/JDBC/Catalog were modeled"}
  GetCrawler: {wire: ok, errors: ok, state: ok, persist: ok}
  GetCrawlers: {wire: ok, errors: ok, state: ok, persist: ok}
  ListCrawlers: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateCrawler: {wire: ok, errors: ok, state: ok, persist: ok, note: "same CrawlerOptions/target-kind fix as CreateCrawler; also fixed a missing CrawlerRunningException guard (UpdateCrawler previously allowed updating a RUNNING/STARTING/STOPPING crawler, unlike DeleteCrawler which already checked this)"}
  DeleteCrawler: {wire: ok, errors: ok, state: ok, persist: ok}
  StartCrawler: {wire: ok, errors: ok, state: ok, persist: ok}
  StopCrawler: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateCrawlerSchedule: {wire: ok, errors: ok, state: ok, persist: ok}
  StartCrawlerSchedule: {wire: ok, errors: ok, state: ok, persist: ok}
  StopCrawlerSchedule: {wire: ok, errors: ok, state: ok, persist: ok}
  GetCrawlerMetrics: {wire: ok, errors: ok, state: ok, persist: ok, note: "not re-verified in depth this pass"}
  CreateJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: added Job.MaxCapacity + NotificationProperty (previously missing entirely — the MaxCapacity vs WorkerType/NumberOfWorkers axis named explicitly in the audit brief), plus AWS's documented mutual-exclusion validation between MaxCapacity and WorkerType/NumberOfWorkers"}
  GetJob: {wire: ok, errors: ok, state: ok, persist: ok}
  GetJobs: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchGetJobs: {wire: ok, errors: ok, state: ok, persist: ok, note: "not re-verified in depth this pass"}
  UpdateJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "same MaxCapacity/NotificationProperty fix as CreateJob"}
  DeleteJob: {wire: ok, errors: ok, state: ok, persist: ok}
  StartJobRun: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass (gopherstack-qd3.4): StartJobRunWithOptions adds real per-run overrides (WorkerType/NumberOfWorkers/MaxCapacity/Timeout/NotificationProperty/SecurityConfiguration) on top of the job-defaults path added last pass, matching StartJobRunRequest and enforcing the MaxCapacity vs WorkerType/NumberOfWorkers mutual-exclusion rule at the run level too. Also fixed a wire-error-code bug: exceeding ExecutionProperty.MaxConcurrentRuns returned generic InvalidInputException instead of the documented ConcurrentRunsExceededException (confirmed in deserializers.go's StartJobRun error switch) — new ErrConcurrentRunsExceeded sentinel, also wired into StartWorkflowRun's new MaxConcurrentRuns check (workflows family)"}
  GetJobRun: {wire: ok, errors: ok, state: ok, persist: ok}
  GetJobRuns: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchStopJobRun: {wire: ok, errors: ok, state: ok, persist: ok}
  GetJobBookmark: {wire: ok, errors: ok, state: ok, persist: ok, note: "not re-verified in depth this pass"}
  ResetJobBookmark: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  GetTags: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  connections: {status: ok, note: "fixed this pass: field-diffed Connection/ConnectionInput against types.Connection/types.ConnectionInput and added Description, MatchCriteria ([]string), and PhysicalConnectionRequirements (AvailabilityZone/SubnetId/SecurityGroupIdList — used e.g. by NETWORK-type connections in place of ConnectionProperties), all previously silently dropped. CreateConnectionWithOptions/UpdateConnectionWithOptions added additively (CreateConnection/UpdateConnection kept for existing callers). Not modeled: AthenaProperties/SparkProperties/PythonProperties/AuthenticationConfiguration/CompatibleComputeEnvironments — newer OAuth/compute-environment fields judged out of scope for this pass (no auth-flow simulation exists anywhere in this backend)."}
  triggers: {status: ok, note: "fixed this pass (gopherstack-qd4.1): Trigger gained Description, WorkflowName, and EventBatchingCondition (BatchSize/BatchWindow); TriggerCondition gained CrawlerName and CrawlState (types.Condition supports crawler-state predicates, not just job-state — was entirely unmodeled); TriggerAction gained SecurityConfiguration/NotificationProperty/Timeout (types.Action fields silently dropped). CreateTrigger/UpdateTrigger now enforce AWS's documented 'max 2 crawler actions per trigger' soft limit (about-triggers.html), returning InvalidInputException over the limit. WorkflowName is create-only (not part of TriggerUpdate, confirmed against types.TriggerUpdate) so UpdateTrigger does not accept it."}
  workflows: {status: partial, note: "fixed this pass: Workflow gained MaxConcurrentRuns (previously silently dropped), now enforced in StartWorkflowRun and returning the correct ConcurrentRunsExceededException (see StartJobRun note — same fix applied to both run-starting ops). Still not modeled: Graph (WorkflowGraph nodes/edges linking the workflow's triggers/jobs/crawlers), LastRun, BlueprintDetails, WorkflowRunStatistics (ErroredActions/FailedActions/RunningActions/etc.) — building a real workflow DAG that tracks trigger->job/crawler topology and per-action run statistics is a substantial feature (would need to model workflow membership resolution across all three resource kinds) not completed this pass; genuinely deferred, not a stub (StartWorkflowRun/GetWorkflowRun/GetWorkflowRuns/PutWorkflowRunProperties/ResumeWorkflowRun/StopWorkflowRun all do real state mutation)."}
  dev_endpoints: {status: ok, note: "fixed this pass: DevEndpoint/DevEndpointInput were previously missing ~20 of ~24 real fields (RoleArn, SecurityGroupIds, SubnetId, WorkerType, GlueVersion, NumberOfWorkers/Nodes, PublicKey(s), ExtraJarsS3Path/ExtraPythonLibsS3Path, SecurityConfiguration, VpcId, AvailabilityZone, YarnEndpointAddress/PrivateAddress/PublicAddress, FailureReason, LastUpdateStatus, ZeppelinRemoteSparkInterpreterPort, CreatedTimestamp/LastModifiedTimestamp) — CreateDevEndpoint took only a bare name. Field-diffed against types.DevEndpoint/CreateDevEndpointInput/UpdateDevEndpointInput and added all of them. RoleArn is a real AWS-required field and is now validated as such (was previously accepted as empty, which real AWS rejects). UpdateDevEndpoint gained AddPublicKeys/DeletePublicKeys/PublicKey/DeleteArguments (previously only AddArguments worked). Network address fields (VpcId/YarnEndpointAddress/PrivateAddress/PublicAddress) are deterministic mock values, not real network state — there is no VPC/networking simulation in this backend, consistent with every other service."}
  security_configurations: {status: ok, note: "fixed this pass: EncryptionConfiguration was missing DataQualityEncryption (DataQualityEncryptionMode/KmsKeyArn), field-diffed against types.EncryptionConfiguration — CloudWatchEncryption/JobBookmarksEncryption/S3Encryption were already modeled. CreateSecurityConfiguration/GetSecurityConfiguration/DeleteSecurityConfiguration/ListSecurityConfigurations all do real state mutation; cloneSecurityConfig's shallow-copy pattern audited and confirmed safe (no field is ever mutated post-creation, same reasoning as the data_quality_rulesets finding below)."}
  schema_registry: {status: partial, note: "fixed this pass: RegisterSchemaVersion never validated its SchemaDefinition against the schema's DataFormat — CreateSchema's initial definition IS validated (validateSchemaDefinition), but every subsequent RegisterSchemaVersion call silently accepted arbitrarily malformed AVRO/JSON/PROTOBUF content, a real correctness gap now fixed by reusing the same validator. GetSchemaByDefinition was already implemented for real (found not to be a stub, contrary to the prior ledger's 'still not audited' note). Still not modeled: compatibility-mode enforcement (BACKWARD/FORWARD/FULL/etc. — RegisterSchemaVersion never checks a new definition against Compatibility, which would require a real schema-compatibility-diffing algorithm per DataFormat) and validateAvroSchema/validateJSONSchema/validateProtobufSchema remain surface-level (JSON well-formedness + minimal structural markers, not full grammar validation) — both would require pulling in real schema-parsing libraries, out of scope for this pass (no new go.mod dependencies permitted)."}
  data_quality_rulesets: {status: partial, note: "fixed this pass: CreateDataQualityRuleset/UpdateDataQualityRuleset silently dropped Description entirely (real CreateDataQualityRulesetInput/UpdateDataQualityRulesetInput both document it) and CreateDataQualityRuleset was also missing TargetTable (DataQualityTargetTable: TableName/DatabaseName/CatalogId) and DataQualitySecurityConfiguration — all field-diffed against types.CreateDataQualityRulesetInput and added via new CreateDataQualityRulesetWithOptions. Re-confirmed the prior pass's finding that CreateDataQualityRuleset/StartDataQualityRulesetEvaluationRun returning their live map-stored pointer is not an actual bug (handlers only read immutable identity fields). Still not modeled: DQDL syntax / rule-type validation — the Ruleset string is stored and returned verbatim with no grammar checking, would require a real DQDL parser, out of scope for this pass."}
  ml_transforms: {status: partial, note: "fixed this pass: CreateMLTransform/UpdateMLTransform silently dropped GlueVersion/WorkerType/NumberOfWorkers/MaxCapacity (the MLTransform model already had these fields from a prior pass, but neither Create nor Update ever wired them from the wire request — a genuine 'field exists on the model but is unreachable' gap) plus MaxRetries/Timeout/Schema ([]SchemaColumn)/TransformEncryption (MlUserDataEncryption+TaskRunSecurityConfigurationName), none of which existed at all. Field-diffed against types.MLTransform/CreateMLTransformRequest/UpdateMLTransformRequest. Added CreateMLTransformWithOptions plus the same MaxCapacity-vs-WorkerType/NumberOfWorkers mutual-exclusion validation used elsewhere (CreateJob/CreateCrawler/StartJobRun). Still not modeled: EvaluationMetrics (FindMatchesMetrics precision/recall/F1/confusion-matrix) — this backend never runs a real ML evaluation, so there is no real metric to report; StartMLEvaluationTaskRun creates a real task-run record but does not fabricate evaluation numbers, which would be a stub-shaped lie rather than an honest gap."}
  blueprints: {status: ok, note: "fixed this pass: CreateBlueprint took only a bare Name — real CreateBlueprintInput requires BlueprintLocation (the S3 path Glue reads the blueprint from) and also supports Description/Tags, all silently unsupported. UpdateBlueprint similarly took only Name; real UpdateBlueprintInput requires BlueprintLocation and supports Description. Blueprint (the response/Get type) was also missing BlueprintLocation/BlueprintServiceLocation/Description/ParameterSpec/ErrorMessage/CreatedOn/LastModifiedOn — field-diffed against types.Blueprint and added. BlueprintLocation is now validated as required on both Create and Update, matching AWS. Not modeled: LastActiveDefinition — this duplicates Blueprint's own top-level fields in the common case (only differs after a failed update, which this backend does not simulate), so leaving it out does not create an observable gap for any currently-modeled failure path."}
  user_defined_functions: {status: ok, note: "fixed this pass: UserDefinedFunction was missing FunctionType (types.UserDefinedFunction/UserDefinedFunctionInput both document it — was entirely unmodeled, meaning Athena/Redshift-Spectrum-style scalar-function metadata was silently dropped) and CatalogId (every other catalog-scoped resource in this backend — Database/Table/Partition — already models CatalogID; UDF was the one exception). Also fixed a wire-shape bug in the other direction: the local model had a `FunctionArn` field with `json:\"FunctionArn\"` that does NOT exist on the real wire type at all (confirmed against types.UserDefinedFunction) — a fabricated extra field that, while harmless to JSON-tolerant clients, is not real AWS-accurate shape; changed to `json:\"-\"` (internal-only, used for TagResource) so GetUserDefinedFunction/GetUserDefinedFunctions responses now match the real shape exactly."}
  resource_policy: {status: ok, note: "fixed this pass: PutResourcePolicy silently dropped PolicyExistsCondition (MUST_EXIST/NOT_EXIST) and PolicyHashCondition entirely — every call unconditionally created/overwrote the policy regardless of what a caller passed, defeating the optimistic-concurrency guard those fields exist for. Worse, DeleteResourcePolicy's PolicyHashCondition parameter was already plumbed from the wire into the backend method but the backend signature discarded it as `_ string` — any caller's hash was ignored and the policy always deleted. Both now enforce the conditions and return the documented ConditionCheckFailureException (new sentinel ErrResourcePolicyConditionFailed, mapped in handler.go's handleError) or EntityNotFoundException (MUST_EXIST-but-missing) on mismatch. Interface signature PutResourcePolicy gained two params (existsCondition, hashCondition). Fixed this pass (gopherstack-qd4.2): EnableHybrid (TRUE/FALSE) is now accepted, validated as a well-formed enum, and recorded per-policy — previously silently dropped without even being read off the wire. AWS's documented precondition ('must be TRUE if you have already used the Management Console to grant cross-account access') can never actually trigger in this backend because Lake Formation console-grant state is not modeled anywhere in gopherstack, so both TRUE and FALSE correctly succeed unconditionally, matching real AWS behavior for any account with no console grants."}
  integration_resource_properties: {status: ok, note: "fixed this pass (found while auditing the deferred families, not previously tracked in this ledger): GetIntegrationResourceProperty/CreateIntegrationResourceProperty/UpdateIntegrationResourceProperty/ListIntegrationResourceProperties and GetIntegrationTableProperties all returned the live map-stored pointer with its SourceProperties/TargetProperties (or SourceTableConfig/TargetTableConfig) maps uncloned. UpdateIntegrationResourceProperty/UpdateIntegrationTableProperties reassign those same map fields in place under the lock, while Get/Create's callers read them after the lock is released — a genuine data race, same bug class as the prior pass's GetTables fix. Fixed by cloning (new cloneIntegrationResourceProperty helper + inline clone for the table-properties Get)."}
  glossaries: {status: ok, note: "NEW this pass (parity-4, SDK bump to v1.149.0 revealed 31 new ops): CreateGlossary/GetGlossary/UpdateGlossary/DeleteGlossary/ListGlossaries and CreateGlossaryTerm/GetGlossaryTerm/UpdateGlossaryTerm/DeleteGlossaryTerm/ListGlossaryTerms field-diffed against the SDK's Create/Get/Update output shapes (Glossary reuses one struct for all three since they share exactly Id/Name/Description; same for GlossaryTerm). DeleteGlossary enforces AWS's documented 'cannot delete while it still contains terms' ConflictException (confirmed in deserializers.go's error switch). DeleteGlossaryTerm additionally disassociates the term from every asset/iterable-form-item that referenced it (not separately documented by the op's own shape, but the same referential-integrity discipline this backend already applies elsewhere, e.g. BatchDeleteTable cascading to partitions) -- covered by TestGlue_AssociateGlossaryTerms_TableDriven/deleting_glossary_term_cascades_to_asset. Glossary/GlossaryTerm IDs are opaque generated IDs (gls-/term- prefix + short uuid), matching that Name is not unique and Identifier is always used for lookup in the real shapes."}
  asset_catalog: {status: ok, note: "NEW this pass: AssetType (PutAssetType/GetAssetType/DeleteAssetType/ListAssetTypes) and Asset (PutAsset/GetAsset/UpdateAsset/DeleteAsset/SearchAssets) field-diffed against the SDK. PutAssetType validates every referenced FormTypeIdentifier exists (EntityNotFoundException) -- an inferred FK check, not explicitly documented, but matches the FormType<-AssetType ownership DeleteFormType's own ConflictException already implies. PutAsset requires an existing AssetTypeId. DeleteAssetType has NO documented ConflictException (confirmed absent from deserializers.go's error switch, unlike DeleteFormType/DeleteGlossary), so deleting an asset type still referenced by assets is allowed -- deliberately not inventing an undocumented guard. AssociateGlossaryTerms/DisassociateGlossaryTerms validate both the asset and every glossary term ID exist. SearchAssets supports SearchText (case-insensitive substring on Name/Description) plus FilterClause's full union shape (AndAllFilters/OrAnyFilters/AttributeFilter/MapFilter, all 6 SearchFilterOperator values, decoded as a plain struct rather than reproducing the SDK's Go-side interface union since this backend only ever decodes, never encodes, the filter -- see search_assets.go's file doc comment) and Sort. MapFilter is scoped to the 'Forms' map attribute (the only map-shaped Asset field); AttributeFilter covers Name/Description/Id/AssetTypeId/CreatedAt/UpdatedAt."}
  form_types_and_attachments: {status: ok, note: "NEW this pass: FormType (PutFormType/GetFormType/DeleteFormType/ListFormTypes) is upsert-keyed by Name (AWS documents 'if a form type with the given name already exists, it is updated' for the sibling PutAssetType, and PutFormType's own required-uppercase-first-letter validation strongly implies the same identity-by-name shape); FormType.Id is set equal to Name since the real ID-generation algorithm is not discoverable from the public SDK shapes alone -- the same class of simplification this file already accepts for DevEndpoint's mock network fields (see PARITY notes below). PutAttachment/DeleteAttachment attach forms either directly to an asset or (via IterableFormName+ItemIdentifier) to an item within one of the asset's iterable forms; BatchGetIterableForms/ListIterableForms are read-only per the SDK, so an iterable-form item's entire existence in this backend is derived from PutAttachment having targeted it at least once -- there is no other creation path in the 31-op surface this pass covers (see iterableFormItemRecord's doc comment in assets.go). This is modeled as a deliberately NOT-store.Table raw nested map (InMemoryBackend.iterableFormItems) because its key is a 3-level nested collection, not a single value's own field; it is still fully covered by Snapshot/Restore (see state_and_persistence)."}
  dashboard_and_session_endpoint: {status: partial, note: "NEW this pass: GetDashboardUrl (JOB/SESSION dashboard URL) and GetSessionEndpoint (interactive session Spark Connect endpoint) both do a REAL existence check against this backend's job/session tables (EntityNotFoundException on an unknown resource, InvalidInputException on a ResourceType other than JOB/SESSION) and GetSessionEndpoint additionally real-checks the session isn't STOPPED/STOPPING (IllegalSessionStateException, confirmed as a documented error for this op). The URL/auth-token VALUES themselves are deterministic mock data, not backed by a real Glue Studio console or Spark Connect listener -- the same modeling choice already established and accepted in this file for DevEndpoint's YarnEndpointAddress/PrivateAddress/PublicAddress (no VPC/networking simulation exists anywhere in gopherstack). Marked partial rather than ok only because GetSessionEndpoint's state gate had to work around a PRE-EXISTING, unrelated gap noted while implementing this: Session.Status is set to PROVISIONING on CreateSession and nothing in this backend ever advances it to READY (no reconciler transition exists for sessions, unlike crawlers/job-runs/workflow-runs), so gating GetSessionEndpoint on READY would make it permanently unreachable in this backend; it is gated on 'not STOPPED/STOPPING' instead. Flagging the missing PROVISIONING->READY session transition for a future pass rather than expanding this one's scope to fix session lifecycle."}
  error_codes_global: {status: ok, note: "SEVERE systemic fix this pass: the shared ErrValidation sentinel wired \"ValidationException\" as its wire __type — confirmed against aws-sdk-go-v2/service/glue/deserializers.go that the vast majority of Create/Update/Delete operations (CreateDatabase, CreateTable, CreateJob, CreateCrawler, CreateTrigger, CreateBlueprint, CreateCustomEntityType, CreateUsageProfile, tag validation, ...) document InvalidInputException instead. Changed the shared sentinel + handler.go's hardcoded mapping to InvalidInputException, and fixed the ~8 existing tests that had encoded the wrong wire code. Also fixed awserrFromDetail (handler_stubs.go), which always wrapped batch-operation ErrorDetail as awserr.ErrNotFound regardless of the actual ErrorCode string — so e.g. an AlreadyExistsException detail from BatchCreatePartition surfaced to CreatePartition callers as EntityNotFoundException. Not touched: IdempotentParameterMismatchException, ResourceNumberLimitExceededException, OperationTimeoutException, ConcurrentModificationException remain unused — no account-level quota/concurrency-conflict modeling exists to trigger them realistically (bd: gopherstack-qd3.5)"}
  BatchGetDataQualityRulesetEvaluationRun: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-08-05 (SDK v1.152.0, new op): in: RunIds*[]string; out: Runs[]DataQualityRulesetEvaluationRun, RunsNotFound[]string; errors: InternalServiceException/InvalidInputException/OperationTimeoutException (no EntityNotFoundException -- unknown IDs go in RunsNotFound instead, confirmed absent from the op's own error switch). Real batch lookup against the same dataQualityEvalRuns table GetDataQualityRulesetEvaluationRun already reads, following BatchGetCrawlers' found/missing split shape exactly (crawlers.go)."}
  data_catalog_export_configuration: {status: partial, note: "2026-08-05 (SDK v1.152.0, new ops): Get/PutDataCatalogExportConfiguration. Unlike DataCatalogEncryptionSettings, these ops carry no CatalogId at all (confirmed absent from both Input structs) -- modeled as one backend-global (account+region) singleton, matching GetGlueIdentityCenterConfiguration's existing pattern (identity_center.go). PutDataCatalogExportConfiguration validates ExportSetting is ENABLED or DISABLED (InvalidInputException otherwise) and really stores EncryptionConfiguration/CreatedAt/UpdatedAt; GetDataCatalogExportConfiguration returns the real DISABLED default when never configured (same rationale already documented for GetDataCatalogEncryptionSettings' empty-default return). state=partial only because Status mirrors ExportSetting SYNCHRONOUSLY: real AWS transitions through ENABLING/DISABLING before settling (an actual async S3 Tables export pipeline standing up/tearing down), which this backend has nothing to simulate -- honest immediate settlement, not a fabricated transient state, but also not the real eventually-consistent timing. S3TableBucketArn has no corresponding field anywhere in PutDataCatalogExportConfigurationInput, so it is never populated -- see gaps."}
gaps:
  - "2026-08-05: DataCatalogExportConfiguration.S3TableBucketArn (GetDataCatalogExportConfigurationOutput field) is real AWS-managed state -- the actual S3 Tables bucket ARN backing the export -- with no corresponding input field anywhere in this API (confirmed absent from PutDataCatalogExportConfigurationInput). There is no way to honestly derive it, so it is always left empty rather than fabricated."
  - "2026-08-05: DataCatalogExportConfiguration.Status's ENABLING/DISABLING transient states (real AWS's async S3 Tables export pipeline standing up/tearing down) are not modeled -- this backend has no such pipeline, so Status settles to ENABLED/DISABLED synchronously with the Put call. Honest (no fabricated FAILED occurrences or invented settlement delay), just not eventually-consistent like real AWS."
  # All 7 gaps tracked at the start of this pass are fixed — see the ops/families
  # notes above for each. Kept here (marked FIXED) rather than deleted so the
  # bd issue IDs remain traceable; close the corresponding bd issues separately.
  - "FIXED this pass: CrawlerTarget missing DynamoDBTargets/DeltaTargets/HudiTargets/IcebergTargets/MongoDBTargets (bd: gopherstack-qd3.1)"
  - "FIXED this pass: CreateCrawler/UpdateCrawler missing SchemaChangePolicy, RecrawlPolicy, LineageConfiguration, CrawlerSecurityConfiguration, LakeFormationConfiguration (bd: gopherstack-qd3.2)"
  - "FIXED this pass: DatabaseInput/Database missing Parameters, LocationUri, CreateTableDefaultPermissions, TargetDatabase (bd: gopherstack-qd3.3)"
  - "FIXED this pass: StartJobRun has no per-run capacity/argument overrides (bd: gopherstack-qd3.4)"
  - "STILL OPEN: IdempotentParameterMismatchException/ResourceNumberLimitExceededException/OperationTimeoutException/ConcurrentModificationException are documented Glue exceptions never returned by this backend — no account-level quota/idempotency-token/concurrency-conflict state exists anywhere in this backend to trigger them realistically, and fabricating one would mean inventing arbitrary quota numbers not sourced from AWS docs. (bd: gopherstack-qd3.5). Note: ConcurrentRunsExceededException — a DIFFERENT, distinct exception from ConcurrentModificationException — WAS found unused and fixed this pass (see StartJobRun/workflows notes above); do not conflate the two when re-auditing."
  - "FIXED this pass: Trigger/TriggerAction missing Description, EventBatchingCondition, WorkflowName, and the AWS \"max 2 crawler actions per trigger\" soft limit is now enforced (bd: gopherstack-qd4.1)"
  - "FIXED this pass: PutResourcePolicy did not model EnableHybrid (bd: gopherstack-qd4.2)"
  - "NEW gap found this pass, STILL OPEN: TagResource/UntagResource's tagResource() dispatcher (tags.go) only recognizes Database/Crawler/Job/DataQualityRuleset/Connection/Trigger/Workflow ARNs. Blueprint/DevEndpoint/MLTransform/UserDefinedFunction/CustomEntityType all support Tags at creation (UDF/MLTransform via a separate ARN-keyed tag store; Blueprint/DevEndpoint via an inline Tags field added this pass) but calling TagResource/UntagResource against their ARN post-creation returns EntityNotFoundException. Pre-existing pattern gap, not introduced this pass (Blueprint/DevEndpoint had no tags capability at all before); flagging for a future pass rather than expanding this one's scope further."
  - "NEW gap FOUND (not introduced) this pass (parity-4): Session.Status is set to PROVISIONING on CreateSession and this backend has no reconciler transition that ever advances it to READY, unlike crawlers/job-runs/workflow-runs which all do reach a terminal running/ready state. This was surfaced while implementing GetSessionEndpoint (bd note: had to gate on 'not STOPPED/STOPPING' instead of the more natural READY check -- see dashboard_and_session_endpoint family note). Fixing session lifecycle is out of scope for this pass; flagging for whichever pass owns sessions.go."
deferred:
  # Every family below was field-diffed against the pinned SDK this pass (none
  # left un-audited). Families now fully closed (status: ok in the table above)
  # are removed from this list; families with a genuine remaining gap keep a
  # one-line pointer to the families note above (which has the full reasoning).
  - "workflows: Graph/LastRun/BlueprintDetails/WorkflowRunStatistics not modeled (real DAG-topology feature, out of scope this pass)"
  - "schema registry: compatibility-mode enforcement (BACKWARD/FORWARD/FULL) and full AVRO/JSON/PROTOBUF grammar validation depth (would need real schema-parsing libraries; no new go.mod deps permitted)"
  - "data quality rulesets: DQDL syntax / rule-type validation (would need a real DQDL parser)"
  - "ML transforms: EvaluationMetrics (FindMatchesMetrics) — no real ML evaluation is ever run, so there is no real metric to report"
leaks: {status: clean, note: "backend_reconciler.go's managed goroutine (StartReconciler/StopReconciler/reconcileLoop) already exits deterministically on ctx.Done() or the stop channel with a WaitGroup — no unmanaged 'go b.runReconciler()' leak. Verified with go test -race this pass too; no new goroutines/timers/tickers introduced (all new run-tracking state — DevEndpoint/Blueprint/MLTransform fields, StartJobRunOptions, CrawlerOptions additions — is plain struct state guarded by the existing coarse b.mu, not new concurrency). No new ghost-map-row risk: no new child/FK resource maps were introduced this pass (all additions are fields on existing resource structs or new sub-structs embedded inline), so no new cascade-delete paths were needed."}
---

## Notes

- **Protocol**: json-1.1 (`X-Amz-Target: AWSGlue.<Op>`, `application/x-amz-json-1.1`),
  confirmed against `aws-sdk-go-v2/service/glue/deserializers.go`'s
  `awsAwsjson11_deserializeOpError<Op>` switch statements. Error responses use
  `{"__type": "<ExceptionName>", "message": "..."}`.

- **ValidationException vs InvalidInputException (important, easy to re-flag by
  mistake)**: Glue's SDK error model genuinely contains BOTH exception types.
  `ValidationException` IS a real type in `types/errors.go`, and a handful of newer
  operations (confirmed: `DeleteConnectionType`) do declare it as a documented error.
  But the overwhelming majority of hand-validation call sites in this backend
  (name-length checks, tag-limit checks, required-field checks across
  Create/Update/Delete for databases/tables/jobs/crawlers/triggers/blueprints/
  custom-entity-types/usage-profiles) correspond to AWS operations whose deserializer
  switch lists `InvalidInputException`, not `ValidationException` — confirmed by
  reading the actual `awsAwsjson11_deserializeOpErrorCreateXxx` functions in
  `deserializers.go` for CreateDatabase, CreateTable, CreateJob, CreateCrawler,
  CreateTrigger, CreateBlueprint, CreateCustomEntityType, CreateUsageProfile. Since
  `ErrValidation` is one shared sentinel used everywhere, the fix picks the option
  that's correct for the large majority of call sites. Do not "fix" this back to
  ValidationException without checking the SDK deserializer for the specific op in
  question first.

- **`awserrFromDetail` (handler_stubs.go)**: single-item AWS ops that are implemented
  by calling a batch backend method with a one-element slice (CreatePartition →
  BatchCreatePartition, DeletePartition → BatchDeletePartition) surface
  `errs[0].ErrorDetail` as a real Go error via this helper. It must switch on
  `d.ErrorCode` to pick the matching sentinel (AlreadyExists vs NotFound vs generic
  invalid-parameter) — do not revert it to unconditionally wrapping
  `awserr.ErrNotFound`, or AlreadyExistsException details get reported to SDK callers
  as EntityNotFoundException.

- **StorageDescriptor is shared by Table AND Partition** in real Glue (partitions
  carry their own StorageDescriptor that can override table-level SerDe/format
  settings). Because `CreateTable`/`UpdateTable`/`BatchCreatePartition`/
  `UpdatePartition` already copy the whole `StorageDescriptor` struct by value from
  the request input, adding fields to the `StorageDescriptor`/`Column` type
  definitions was sufficient to flow them through end-to-end — the remaining real
  work was fixing `cloneTable`/`clonePartition`/`cloneCrawler` to deep-copy the new
  nested maps/slices/pointers (Parameters maps, SerdeInfo pointer, BucketColumns/
  SortColumns slices, per-Column Parameters) so that `GetTable`/`GetPartitions`
  callers can't mutate live backend state through the returned pointers.

- **`CreateCrawler`/`UpdateCrawler` signature is called from
  `services/cloudformation/resources_phase5.go`** (outside this package) with the
  original 5-arg / 4-arg positional signatures. Per the audit's signature-safety
  rule, those signatures were left untouched; new capability (Schedule, Classifiers,
  Configuration, TablePrefix, Description) was added via new
  `CreateCrawlerWithOptions`/`UpdateCrawlerWithOptions` methods that the old methods
  now delegate to with a zero-value `CrawlerOptions`. The `StorageBackend` interface
  gained the two new methods additively; `InMemoryBackend` is the only implementer
  (verified — no mocks reference `StorageBackend` in this package's tests), so this
  is safe.

- **GetTables aliasing bug**: `GetTables` was the one read path in the whole backend
  that hadn't been updated to clone before returning (`GetDatabases`, `GetCrawlers`,
  `GetJobs`, `GetConnections`, `SearchTables`, `GetPartition(s)` all already cloned).
  Fixed to match the established pattern; verified no other `Get*` list method has
  the same gap.

## This pass (parity-4 campaign: SDK bump to v1.149.0 revealed 31 new ops, HEAD `a7f9c5fb2`)

The Go SDK module was bumped from `aws-sdk-go-v2/service/glue@v1.137.2` to
`@v1.149.0`, which shipped a new operation surface `TestSDKCompleteness`
caught as missing: `AssociateGlossaryTerms`, `BatchGetIterableForms`,
`CreateGlossary`, `CreateGlossaryTerm`, `DeleteAsset`, `DeleteAssetType`,
`DeleteAttachment`, `DeleteFormType`, `DeleteGlossary`, `DeleteGlossaryTerm`,
`DisassociateGlossaryTerms`, `GetAsset`, `GetAssetType`, `GetDashboardUrl`,
`GetFormType`, `GetGlossary`, `GetGlossaryTerm`, `GetSessionEndpoint`,
`ListAssetTypes`, `ListFormTypes`, `ListGlossaries`, `ListGlossaryTerms`,
`ListIterableForms`, `PutAsset`, `PutAssetType`, `PutAttachment`,
`PutFormType`, `SearchAssets`, `UpdateAsset`, `UpdateGlossary`,
`UpdateGlossaryTerm` -- three coherent new resource families (business
glossary + terms; asset catalog: asset types, assets, form types,
attachments, iterable form items; and Spark monitoring
dashboard/interactive-session endpoint lookups) plus a handful of standalone
ops. All 31 were implemented for real (none parked in `notImplemented`) --
see the `glossaries`, `asset_catalog`, `form_types_and_attachments`, and
`dashboard_and_session_endpoint` family notes above for the full field-diff
and error-code reasoning per family.

**Ownership/cascade summary** (see `ownership_and_cascade` in the return
receipt for the full version): a `Glossary` owns zero or more
`GlossaryTerm`s (`DeleteGlossary` blocked by `ConflictException` while any
exist; `DeleteGlossaryTerm` cascades to disassociate the term from every
`Asset`/iterable-form-item that referenced it). An `Asset` has exactly one
`AssetType` (validated to exist on `PutAsset`); an `AssetType` references one
or more `FormType`s via its `Forms` map (validated to exist on
`PutAssetType`; `DeleteFormType` blocked by `ConflictException` while any
`AssetType` still references it, per AWS's own documented error).
`Attachment`s hang off either an `Asset` directly or an item within one of
the `Asset`'s iterable forms (e.g. a table asset's "columns"); iterable-form
items have no dedicated create operation in this 31-op surface, so their
existence is entirely derived from `PutAttachment` having targeted them at
least once (`ItemIdentifier`+`IterableFormName`) -- `DeleteAsset` cascades to
delete all of an asset's iterable-form items.

**Two honest, narrowly-scoped compromises** (both documented in the family
notes above, neither hides anything in `notImplemented`): (1) `FormType`/
`AssetType` IDs are set equal to their (unique, upsert-keyed) `Name` rather
than an AWS-opaque generated ID, since the real ID-generation format is not
derivable from the public SDK shapes -- the same class of "deterministic mock
value, real backing state" choice this file already accepts for `DevEndpoint`
network fields. (2) `GetDashboardUrl`/`GetSessionEndpoint` real-check that the
target JOB/SESSION exists (and, for sessions, is not
stopped/stopping) but return deterministic mock URL/token values, since this
backend has no real Glue Studio console or Spark Connect listener -- again
matching the `DevEndpoint` precedent rather than inventing new fabricated
infrastructure.

**One pre-existing, unrelated gap surfaced (not introduced) while
implementing `GetSessionEndpoint`**: `Session.Status` is set to
`PROVISIONING` on `CreateSession` and this backend has no reconciler
transition that ever advances it to `READY` (unlike crawlers/job-runs/
workflow-runs, which all do reach a terminal state via the managed
reconciler). `GetSessionEndpoint` was designed around this by gating on "not
STOPPED/STOPPING" rather than requiring `READY`, so the gap does not block
this pass's new functionality; flagged in `gaps` above for whichever pass
owns `sessions.go`.

**Decomposition**: split by resource family into new files matching this
service's existing one-family-per-file layout --
`glossaries.go`/`handler_glossaries.go` (Glossary + GlossaryTerm +
Associate/Disassociate), `assets.go`/`handler_assets.go` (AssetType + Asset +
Attachments + IterableForms + SearchAssets), `search_assets.go` (SearchAssets'
filter-clause union parser/evaluator, split out of `assets.go` to keep that
file to CRUD), `forms.go`/`handler_forms.go` (FormType), and
`dashboard.go`/`handler_dashboard.go` (GetDashboardUrl/GetSessionEndpoint). No
function required a `//nolint:cyclop|gocyclo|gocognit|funlen` suppression;
`golang.org/x/tools/go/analysis/passes/fieldalignment -fix` was re-run across
the package (same tool used in the parity-3 pass) to keep every new struct
`govet`-fieldalignment-clean.

**New regression tests** (all in `handler_test.go`, an existing file --
`services/glue` already had zero test files using a live
`aws-sdk-go-v2/service/glue` client; every existing test in this package
round-trips through the real HTTP handler/router path via the `doGlueRequest`
helper already defined in `handler_test.go`, and the new tests follow that
same established convention): `TestGlue_Glossary_TableDriven`,
`TestGlue_AssociateGlossaryTerms_TableDriven`,
`TestGlue_AssetCatalog_CRUD_TableDriven`,
`TestGlue_AssetAttachmentsAndSearch_TableDriven`,
`TestGlue_Dashboard_TableDriven`. `persistence_test.go`'s existing
`TestInMemoryBackend_SnapshotRestore_FullState` (the package's designated
full-backend-state Snapshot/Restore regression test) was extended to seed and
verify a glossary+term+asset-type+form-type+asset+iterable-form-item chain,
confirming the new `store.Table`-backed resources AND the raw
`iterableFormItems` map all survive a Snapshot/Restore round trip.

## This pass (parity-3 campaign: full deferred-family sweep, HEAD `6467046d`)

This pass's mandate was different from prior passes: instead of auditing only
rows marked `partial`/`deferred` for drift, it worked through the **7 tracked
gaps** and **all 10 deferred whole-resource families** end-to-end, field-diffing
each against the pinned SDK (`aws-sdk-go-v2/service/glue@v1.137.2`) rather than
trusting the no-stub check alone.

**Gaps (7/7 closed)**: CrawlerTarget's 5 missing target kinds (DynamoDB/Delta/
Hudi/Iceberg/MongoDB), CreateCrawler/UpdateCrawler's 5 missing policy fields
(SchemaChangePolicy/RecrawlPolicy/LineageConfiguration/
CrawlerSecurityConfiguration/LakeFormationConfiguration), Database's 4 missing
fields (Parameters/LocationUri/CreateTableDefaultPermissions/TargetDatabase),
StartJobRun's per-run overrides, Trigger's 3 missing fields plus the
max-2-crawler-actions limit, and PutResourcePolicy's EnableHybrid. One gap
(qd3.5, the four unused quota/idempotency exceptions) is honestly left open —
see the gaps list for why fabricating quota numbers would be worse than an
honest gap.

**Deferred families (10/10 audited, 7 now `ok`, 3 `partial` with a named,
scoped remainder)**: connections, triggers, dev_endpoints, security_configurations,
blueprints, and user_defined_functions are now fully field-diffed and closed.
workflows, schema_registry, data_quality_rulesets, and ml_transforms each got
real, tractable fixes (see families notes) but keep one deep, genuinely
out-of-scope gap each (DAG-graph modeling, schema-compatibility algorithms, a
DQDL parser, and ML evaluation-metric computation respectively) — none of
which can be honestly faked without either a new dependency or inventing data
that isn't real.

**Two additional bug classes found and fixed while doing the field-diffs (not
on the original gaps list)**:

1. **Epoch-seconds timestamp bug** (the exact bug class flagged in this pass's
   brief, previously found in sagemaker): `BlueprintRun.StartedOn`,
   `ColumnStatisticsTaskRun.StartedOn`, `DQRuleRecommendationRun.StartedOn`, and
   `MaterializedViewRefreshRun.StartedOn` were modeled as raw `time.Time` with a
   JSON tag, which `encoding/json` renders as an RFC3339 string — but glue is
   awsjson1.1, which expects a JSON number (epoch seconds) for every timestamp.
   `BlueprintRun` and `ColumnStatisticsTaskRun` reach the wire via `any`-typed
   handler outputs (`GetBlueprintRun`/`GetBlueprintRuns`/
   `GetColumnStatisticsTaskRun`), so this was a real, reachable client-breaking
   bug, not just internal-state hygiene. Fixed by switching all four to
   `float64` (matching every other timestamp field in the package, e.g.
   `JobRun.StartedOn`, `WorkflowRun.StartedOn`). Locked down by a new
   regression test, `TestStartedOn_IsEpochSecondsNumber`
   (`timestamp_wire_shape_test.go`), which decodes the actual HTTP response
   JSON and asserts the field is a `float64`, not a string.
2. **ConcurrentRunsExceededException never returned**: StartJobRun's
   `ExecutionProperty.MaxConcurrentRuns` check returned generic
   `InvalidInputException` instead of the documented
   `ConcurrentRunsExceededException` (confirmed in deserializers.go's
   `awsAwsjson11_deserializeOpErrorStartJobRun` switch). New
   `ErrConcurrentRunsExceeded` sentinel now used by both StartJobRun and the
   new StartWorkflowRun `MaxConcurrentRuns` enforcement.

**Decomposition**: `StartJobRunWithOptions` grew past `gocognit`'s complexity
threshold while absorbing the per-run-override logic; split into
`checkJobConcurrencyLocked` (concurrency-limit check) and
`resolveJobRunOverrides` (pure function resolving job-defaults-vs-per-run-
override precedence) — no `//nolint:gocognit` used.

**Naming hygiene**: ran `golang.org/x/tools/go/analysis/passes/fieldalignment`
with `-fix` across the package (was already lint-clean; this pass's additions
temporarily regressed it) and renamed every new/touched AWS-`Id`-suffixed Go
field to the idiomatic `...ID`/`...IDs` form (`SubnetID`, `VpcID`, `CatalogID`,
`AccountID`, `SecurityGroupIDs`, `LocationURI`, etc.) — matching the
convention already used elsewhere in this file (`CatalogID`, `RoleArn`'s
sibling fields) — rather than reaching for `//nolint:revive,stylecheck`
suppressions. JSON wire tags (`"SubnetId"`, `"CatalogId"`, ...) are untouched;
only the Go-side identifiers changed.

## Follow-ups filed as SHARED-FILE / cross-service (this pass)

No code changes were needed outside `services/glue/` this pass. Every backend
method whose signature changed (`PutResourcePolicy` +1 param,
`StartJobRun`→kept + new `StartJobRunWithOptions`, `CreateConnection`→kept +
new `CreateConnectionWithOptions`, `CreateBlueprint`/`UpdateBlueprint` gained
required params, `CreateDevEndpoint` gained required params,
`UpdateDataQualityRuleset` +1 param, `CreateDataQualityRuleset`→kept + new
`CreateDataQualityRulesetWithOptions`, `CreateMLTransform`→kept + new
`CreateMLTransformWithOptions`) was checked against
`services/cloudformation/resources_glue.go`, the one cross-package caller of
Glue backend methods. `go build ./services/cloudformation/...` passes.
`CreateCrawler`/`CreateConnection`/`CreateTrigger`/`CreateDatabase` signatures
used by cloudformation were kept unchanged (options added via new
`...WithOptions` methods, same pattern as the prior pass's
`CreateCrawlerWithOptions`); cloudformation never calls
`CreateBlueprint`/`CreateDevEndpoint`/`PutResourcePolicy`/
`CreateDataQualityRuleset`/`CreateMLTransform`/`UpdateDataQualityRuleset`
directly, so those breaking signature changes are safe.

## Previous pass (re-audit at HEAD `a8c6614b`, no local drift since `ce30166a`)

`git diff ce30166a..HEAD -- services/glue/` was empty (the ledger's real baseline —
the recorded `last_audit_commit: 704d7cda` did not exist in this branch's history;
`704d7cda` turned out to be an unrelated STS commit, so per the re-audit protocol
`ce30166a`, the commit that last touched this file, was used as the actual baseline).
SDK pin unchanged at `v1.137.2`. With zero drift, all rows the previous pass marked
`ok` were trusted as-is; this pass audited only the rows marked `partial`/`deferred`
and found genuine, narrowly-scoped bugs in triggers, schema registry, resource
policy, and (previously untracked) integration resource/table properties — see the
`families` notes above for each. Full details on all six fixes are in those notes;
summary: two more `Get*`-returns-live-pointer data races (schema registry,
integration properties — same bug class as the prior pass's `GetTables` fix),
`StartTrigger`/`StopTrigger` ignoring the ON_DEMAND-trigger state rule and
`StartTrigger` never actually firing an ON_DEMAND trigger's actions (a disguised
stub for that type's entire purpose), and `PutResourcePolicy`/`DeleteResourcePolicy`
silently dropping their optimistic-concurrency condition parameters.

New regression tests: `parity_pass5_test.go`.

## Follow-ups filed as SHARED-FILE / cross-service (NOT edited this pass)

No code changes were needed outside `services/glue/` this pass. The
`PutResourcePolicy` interface-signature change (two new params) and the additive
`Trigger.StartOnCreation`/`TriggerAction.CrawlerName` fields were checked against
`services/cloudformation/resources_phase5.go`, the one cross-package caller of Glue
backend methods (`gluebackend.CreateCrawler`/`BatchCreatePartition`/`CreateJob`/
`CreateTrigger`) — none of those call sites touch `PutResourcePolicy`, and the
`Trigger{}` struct literal it builds is unaffected by new additive fields, so no
follow-up is needed there.

Separately (found, NOT fixed — pre-existing, unrelated to this pass's Glue changes):
`go build ./services/cloudformation/...` currently fails on
`services/route53/*` — `rc.backends.Route53.Backend.CreateHostedZone` is called
with 4 args but the (concurrently-edited-by-another-agent) Route53 backend method
now takes 5. This is outside `services/glue/` and was left untouched per this
task's scope; flagging for whichever pass owns Route53/CloudFormation.
