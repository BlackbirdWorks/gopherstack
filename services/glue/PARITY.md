---
service: glue
sdk_module: aws-sdk-go-v2/service/glue@v1.152.0
last_audit_commit: a7f9c5fb2
last_audit_date: 2026-08-08
overall: A            # gopherstack-dol3 this pass: tag-ARN dispatch fixed for Blueprint/DevEndpoint/MLTransform/UDF (plus a real creation/update tag-loss bug found alongside it); workflow Graph+LastRun now derived from real trigger/run state; one real, AWS-quota-verified ResourceNumberLimitExceededException (dev endpoints) added. EvaluationMetrics, DQDL/compatibility parsing, and 3 of 4 quota/idempotency exceptions remain honestly deferred -- see notes below.
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
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass (gopherstack-dol3): tagResource()'s ARN dispatcher (tags.go) only recognized Database/Crawler/Job/DataQualityRuleset/Connection/Trigger/Workflow; Blueprint/DevEndpoint/MLTransform/UserDefinedFunction ARNs all returned EntityNotFoundException. Added findBlueprintByARN/findDevEndpointByARN/findMLTransformByARN/findUDFByARN and wired all 4 into TagResource/UntagResource/GetTags/TaggedResources. Also found (not just dispatch): MLTransform/UserDefinedFunction had NO Tags field at all -- CreateMLTransformWithOptions/CreateUserDefinedFunction already called the internal tagResource(ARN, tags) at creation time, but it silently no-op'd against the undispatched ARN, so creation-time tags were lost entirely (not merely unreachable). Added Tags fields to both structs (json:\"-\", matching Blueprint/DevEndpoint's existing internal-only pattern -- confirmed types.MLTransform/types.UserDefinedFunction have no Tags field on the real wire either). Second, separate bug found alongside: UpdateMLTransform/UpdateUserDefinedFunction replace the whole stored record with the caller's input; neither UpdateMLTransformRequest nor UpdateUserDefinedFunctionInput carries Tags on the real wire (confirmed -- AWS updates tags only via TagResource/UntagResource), so every Update call was silently wiping any previously-set tags. Both Update methods now carry existing.Tags forward explicitly."}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "see TagResource note -- same dispatch fix."}
  GetTags: {wire: ok, errors: ok, state: ok, persist: ok, note: "see TagResource note -- same dispatch fix."}
families:
  connections: {status: ok, note: "fixed this pass: field-diffed Connection/ConnectionInput against types.Connection/types.ConnectionInput and added Description, MatchCriteria ([]string), and PhysicalConnectionRequirements (AvailabilityZone/SubnetId/SecurityGroupIdList — used e.g. by NETWORK-type connections in place of ConnectionProperties), all previously silently dropped. CreateConnectionWithOptions/UpdateConnectionWithOptions added additively (CreateConnection/UpdateConnection kept for existing callers). Not modeled: AthenaProperties/SparkProperties/PythonProperties/AuthenticationConfiguration/CompatibleComputeEnvironments — newer OAuth/compute-environment fields judged out of scope for this pass (no auth-flow simulation exists anywhere in this backend)."}
  triggers: {status: ok, note: "fixed this pass (gopherstack-qd4.1): Trigger gained Description, WorkflowName, and EventBatchingCondition (BatchSize/BatchWindow); TriggerCondition gained CrawlerName and CrawlState (types.Condition supports crawler-state predicates, not just job-state — was entirely unmodeled); TriggerAction gained SecurityConfiguration/NotificationProperty/Timeout (types.Action fields silently dropped). CreateTrigger/UpdateTrigger now enforce AWS's documented 'max 2 crawler actions per trigger' soft limit (about-triggers.html), returning InvalidInputException over the limit. WorkflowName is create-only (not part of TriggerUpdate, confirmed against types.TriggerUpdate) so UpdateTrigger does not accept it."}
  workflows: {status: partial, note: "fixed this pass (gopherstack-qd3.5-era fix retained): Workflow gained MaxConcurrentRuns, enforced in StartWorkflowRun, returning ConcurrentRunsExceededException. NEW this pass (gopherstack-dol3): Workflow.Graph and Workflow.LastRun are now real, derived fields -- GetWorkflow/BatchGetWorkflows gained IncludeGraph (confirmed on GetWorkflowInput/BatchGetWorkflowsInput; Graph is only populated when set, matching AWS). Graph (WorkflowGraph{Nodes,Edges}) is built by workflowGraphLocked (workflow_graph.go) purely from real state: every Trigger with WorkflowName==this workflow becomes a TRIGGER node (with real TriggerDetails.Trigger, confirmed types.TriggerNodeDetails.Trigger), each trigger's TriggerAction.JobName/CrawlerName become downstream JOB/CRAWLER nodes+edges, each trigger's TriggerPredicate.Conditions become upstream JOB/CRAWLER nodes+edges -- no fabricated topology. Node.UniqueId is \"<kind>/<name>\" (real ID-gen algorithm not discoverable from the SDK, same simplification already accepted here for FormType.Id). LastRun is the most recent entry from real StartWorkflowRun history (b.workflowRuns), absent until a run has actually happened. Still not modeled: BlueprintDetails (this workflow was never itself created from a Blueprint in this backend -- CreateWorkflow has no blueprint-origin concept, so the field would always be empty/unreachable, a structural non-gap rather than a deferred one), WorkflowRunStatistics (ErroredActions/FailedActions/RunningActions/etc.), and WorkflowRun.Graph/GetWorkflowRun's own IncludeGraph -- these all require correlating individual job/crawler runs to the specific workflow run that triggered them, and this backend has no such correlation anywhere (StartJobRun/crawler-start paths don't record a WorkflowRunId); adding it is a distinct, substantial feature (would touch every job/crawler run-creation path), not completed this pass. Confirmed genuinely deferred, not a stub -- StartWorkflowRun/GetWorkflowRun/GetWorkflowRuns/PutWorkflowRunProperties/ResumeWorkflowRun/StopWorkflowRun all do real state mutation, and the new Graph/LastRun fields are 100% derived from real state, nothing fabricated."}
  dev_endpoints: {status: ok, note: "fixed this pass: DevEndpoint/DevEndpointInput were previously missing ~20 of ~24 real fields (RoleArn, SecurityGroupIds, SubnetId, WorkerType, GlueVersion, NumberOfWorkers/Nodes, PublicKey(s), ExtraJarsS3Path/ExtraPythonLibsS3Path, SecurityConfiguration, VpcId, AvailabilityZone, YarnEndpointAddress/PrivateAddress/PublicAddress, FailureReason, LastUpdateStatus, ZeppelinRemoteSparkInterpreterPort, CreatedTimestamp/LastModifiedTimestamp) — CreateDevEndpoint took only a bare name. Field-diffed against types.DevEndpoint/CreateDevEndpointInput/UpdateDevEndpointInput and added all of them. RoleArn is a real AWS-required field and is now validated as such (was previously accepted as empty, which real AWS rejects). UpdateDevEndpoint gained AddPublicKeys/DeletePublicKeys/PublicKey/DeleteArguments (previously only AddArguments worked). Network address fields (VpcId/YarnEndpointAddress/PrivateAddress/PublicAddress) are deterministic mock values, not real network state — there is no VPC/networking simulation in this backend, consistent with every other service. NEW this pass (gopherstack-dol3): CreateDevEndpoint now enforces AWS's real, published default quota 'Max development endpoint per account: 25' (docs.aws.amazon.com/general/latest/gr/glue.html, verified via WebFetch this pass, not from memory) via a new ErrResourceNumberLimitExceeded sentinel -> ResourceNumberLimitExceededException, confirmed present in CreateDevEndpoint's real error catalog (deserializers.go's awsAwsjson11_deserializeOpErrorCreateDevEndpoint switch). See gap-list note on the other three quota/idempotency exceptions for why only this one resource kind got a limit this pass."}
  security_configurations: {status: ok, note: "fixed this pass: EncryptionConfiguration was missing DataQualityEncryption (DataQualityEncryptionMode/KmsKeyArn), field-diffed against types.EncryptionConfiguration — CloudWatchEncryption/JobBookmarksEncryption/S3Encryption were already modeled. CreateSecurityConfiguration/GetSecurityConfiguration/DeleteSecurityConfiguration/ListSecurityConfigurations all do real state mutation; cloneSecurityConfig's shallow-copy pattern audited and confirmed safe (no field is ever mutated post-creation, same reasoning as the data_quality_rulesets finding below)."}
  schema_registry: {status: partial, note: "fixed this pass: RegisterSchemaVersion never validated its SchemaDefinition against the schema's DataFormat — CreateSchema's initial definition IS validated (validateSchemaDefinition), but every subsequent RegisterSchemaVersion call silently accepted arbitrarily malformed AVRO/JSON/PROTOBUF content, a real correctness gap now fixed by reusing the same validator. GetSchemaByDefinition was already implemented for real (found not to be a stub, contrary to the prior ledger's 'still not audited' note). Still not modeled: compatibility-mode enforcement (BACKWARD/FORWARD/FULL/etc. — RegisterSchemaVersion never checks a new definition against Compatibility, which would require a real schema-compatibility-diffing algorithm per DataFormat) and validateAvroSchema/validateJSONSchema/validateProtobufSchema remain surface-level (JSON well-formedness + minimal structural markers, not full grammar validation) — both would require pulling in real schema-parsing libraries, out of scope for this pass (no new go.mod dependencies permitted)."}
  data_quality_rulesets: {status: partial, note: "fixed this pass: CreateDataQualityRuleset/UpdateDataQualityRuleset silently dropped Description entirely (real CreateDataQualityRulesetInput/UpdateDataQualityRulesetInput both document it) and CreateDataQualityRuleset was also missing TargetTable (DataQualityTargetTable: TableName/DatabaseName/CatalogId) and DataQualitySecurityConfiguration — all field-diffed against types.CreateDataQualityRulesetInput and added via new CreateDataQualityRulesetWithOptions. Re-confirmed the prior pass's finding that CreateDataQualityRuleset/StartDataQualityRulesetEvaluationRun returning their live map-stored pointer is not an actual bug (handlers only read immutable identity fields). Still not modeled: DQDL syntax / rule-type validation — the Ruleset string is stored and returned verbatim with no grammar checking, would require a real DQDL parser, out of scope for this pass."}
  ml_transforms: {status: partial, note: "fixed this pass: CreateMLTransform/UpdateMLTransform silently dropped GlueVersion/WorkerType/NumberOfWorkers/MaxCapacity (the MLTransform model already had these fields from a prior pass, but neither Create nor Update ever wired them from the wire request — a genuine 'field exists on the model but is unreachable' gap) plus MaxRetries/Timeout/Schema ([]SchemaColumn)/TransformEncryption (MlUserDataEncryption+TaskRunSecurityConfigurationName), none of which existed at all. Field-diffed against types.MLTransform/CreateMLTransformRequest/UpdateMLTransformRequest. Added CreateMLTransformWithOptions plus the same MaxCapacity-vs-WorkerType/NumberOfWorkers mutual-exclusion validation used elsewhere (CreateJob/CreateCrawler/StartJobRun). Still not modeled: EvaluationMetrics (FindMatchesMetrics precision/recall/F1/confusion-matrix) — this backend never runs a real ML evaluation, so there is no real metric to report; StartMLEvaluationTaskRun creates a real task-run record but does not fabricate evaluation numbers, which would be a stub-shaped lie rather than an honest gap. Re-confirmed this pass (gopherstack-dol3): still correctly absent, still no code anywhere references EvaluationMetrics/FindMatchesMetrics. Also fixed this pass: Tags were entirely lost, both at creation (see TagResource note) and on every Update (Tags now carried forward explicitly)."}
  blueprints: {status: ok, note: "fixed this pass: CreateBlueprint took only a bare Name — real CreateBlueprintInput requires BlueprintLocation (the S3 path Glue reads the blueprint from) and also supports Description/Tags, all silently unsupported. UpdateBlueprint similarly took only Name; real UpdateBlueprintInput requires BlueprintLocation and supports Description. Blueprint (the response/Get type) was also missing BlueprintLocation/BlueprintServiceLocation/Description/ParameterSpec/ErrorMessage/CreatedOn/LastModifiedOn — field-diffed against types.Blueprint and added. BlueprintLocation is now validated as required on both Create and Update, matching AWS. Not modeled: LastActiveDefinition — this duplicates Blueprint's own top-level fields in the common case (only differs after a failed update, which this backend does not simulate), so leaving it out does not create an observable gap for any currently-modeled failure path."}
  user_defined_functions: {status: ok, note: "fixed this pass: UserDefinedFunction was missing FunctionType (types.UserDefinedFunction/UserDefinedFunctionInput both document it — was entirely unmodeled, meaning Athena/Redshift-Spectrum-style scalar-function metadata was silently dropped) and CatalogId (every other catalog-scoped resource in this backend — Database/Table/Partition — already models CatalogID; UDF was the one exception). Also fixed a wire-shape bug in the other direction: the local model had a `FunctionArn` field with `json:\"FunctionArn\"` that does NOT exist on the real wire type at all (confirmed against types.UserDefinedFunction) — a fabricated extra field that, while harmless to JSON-tolerant clients, is not real AWS-accurate shape; changed to `json:\"-\"` (internal-only, used for TagResource) so GetUserDefinedFunction/GetUserDefinedFunctions responses now match the real shape exactly. Fixed this pass (gopherstack-dol3): Tags were entirely lost, both at creation (see TagResource note) and on every Update (Tags now carried forward explicitly). Separately noted, not fixed (out of this pass's tag-dispatch scope): the wire's createUserDefinedFunctionInput.Tags field (handler_user_defined_functions.go) has no equivalent on the real CreateUserDefinedFunctionInput at all (confirmed against the pinned SDK) -- real AWS clients never send it and can only tag a UDF post-creation via TagResource, which now works correctly; the extra accepted-but-non-standard input field is pre-existing and harmless (unreachable by any real SDK client) but is not itself AWS-accurate shape."}
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
  - "PARTIALLY FIXED this pass (gopherstack-dol3, was gopherstack-qd3.5): re-researched the 4 quota/idempotency exceptions properly this time (WebFetch against real AWS docs, not memory) instead of leaving all 4 blanket-open. FIXED: ResourceNumberLimitExceededException, now real and enforced for CreateDevEndpoint against AWS's actual published default quota (docs.aws.amazon.com/general/latest/gr/glue.html: 'Max development endpoint per account: 25', confirmed present in CreateDevEndpoint's real error catalog). AWS publishes real default quotas for many other Glue resource kinds too (jobs: 2,000; databases: 10,000; crawlers: 1,000; triggers: 1,000; workflows: 1,000; connections: 1,000; security configurations: 250; ML transforms: 100; functions per account/database: 100) — dev endpoints was implemented as the one cleanly testable example (small enough to create N+1 in a fast unit test); wiring the rest is a mechanical follow-up now that the real numbers are sourced and cited here, not a research problem. STILL OPEN, now with real reasoning instead of a blanket statement: (1) IdempotentParameterMismatchException — AWS's real doc text is 'The same unique identifier was associated with two different records' (verified via WebFetch against docs.aws.amazon.com/glue/latest/webapi/API_CreateJob.html); it appears in the real error catalogs of CreateCustomEntityType/CreateDevEndpoint/CreateJob/CreateMLTransform/CreateSession/CreateTrigger/UpdateDataQualityRuleset/UpdateWorkflow (verified by scanning deserializers.go's per-op error switches), but NONE of these operations have a ClientToken/RequestToken field on their real Input types (verified) unlike e.g. CreateDataQualityRuleset which DOES have ClientToken but is NOT in this exception's op list — so the exact 'two different records, same identifier' trigger condition for the ops that actually list it isn't confidently derivable from the SDK alone, and guessing wrong risks silently changing today's correct AlreadyExistsException behavior for a duplicate Name. Genuinely deferred pending real AWS testing/documentation, not skipped out of laziness. (2) ConcurrentModificationException — real text: 'Two processes are trying to modify a resource simultaneously.' Structurally unreachable in this backend by design: every operation holds the coarse per-backend b.mu.Lock for its full duration (see pkgs-catalog.md's locking rule), so two operations against the same resource can never actually race at the data level — there is no real concurrent-modification condition to detect. (3) OperationTimeoutException — real text: 'The operation timed out.' No operation in this synchronous in-memory backend can genuinely time out; would require fabricating an arbitrary timeout threshold with nothing real behind it. (bd: gopherstack-qd3.5/dol3). Note: ConcurrentRunsExceededException — a DIFFERENT, distinct exception from ConcurrentModificationException — WAS found unused and fixed in a prior pass (see StartJobRun/workflows notes above); do not conflate the two when re-auditing."
  - "FIXED this pass: Trigger/TriggerAction missing Description, EventBatchingCondition, WorkflowName, and the AWS \"max 2 crawler actions per trigger\" soft limit is now enforced (bd: gopherstack-qd4.1)"
  - "FIXED this pass: PutResourcePolicy did not model EnableHybrid (bd: gopherstack-qd4.2)"
  - "FIXED this pass (gopherstack-dol3): TagResource/UntagResource/GetTags now recognize Blueprint/DevEndpoint/MLTransform/UserDefinedFunction ARNs — see the TagResource/UntagResource/GetTags op notes above for the full fix (dispatch + the deeper creation/update tag-loss bugs found alongside it). STILL OPEN: CustomEntityType has no ARN or Tags concept modeled in this backend at all (no ARN-building helper, no Tags field, CreateCustomEntityType's wire input doesn't even accept tags) — out of this pass's scope (the bd issue named Blueprint/DevEndpoint/MLTransform/UDF specifically, not CustomEntityType), and adding it from scratch is a larger lift than extending the other four's existing-but-undispatched Tags support."
  - "NEW gap FOUND (not introduced) this pass (parity-4): Session.Status is set to PROVISIONING on CreateSession and this backend has no reconciler transition that ever advances it to READY, unlike crawlers/job-runs/workflow-runs which all do reach a terminal running/ready state. This was surfaced while implementing GetSessionEndpoint (bd note: had to gate on 'not STOPPED/STOPPING' instead of the more natural READY check -- see dashboard_and_session_endpoint family note). Fixing session lifecycle is out of scope for this pass; flagging for whichever pass owns sessions.go."
deferred:
  # Every family below was field-diffed against the pinned SDK this pass (none
  # left un-audited). Families now fully closed (status: ok in the table above)
  # are removed from this list; families with a genuine remaining gap keep a
  # one-line pointer to the families note above (which has the full reasoning).
  - "workflows: Graph and LastRun are now real (gopherstack-dol3, see workflows op note); BlueprintDetails/WorkflowRunStatistics/WorkflowRun.Graph remain unmodeled -- would need job/crawler runs correlated to the workflow run that triggered them, which this backend does not track anywhere (see workflows op note for the full reasoning)"
  - "schema registry: compatibility-mode enforcement (BACKWARD/FORWARD/FULL) and full AVRO/JSON/PROTOBUF grammar validation depth (would need real schema-parsing libraries; no new go.mod deps permitted) -- sized this pass (gopherstack-dol3), see 'DQDL and schema-compatibility sizing' note below; not started"
  - "data quality rulesets: DQDL syntax / rule-type validation (would need a real DQDL parser) -- sized this pass (gopherstack-dol3), see 'DQDL and schema-compatibility sizing' note below; not started"
  - "ML transforms: EvaluationMetrics (FindMatchesMetrics) — no real ML evaluation is ever run, so there is no real metric to report (re-confirmed gopherstack-dol3, still correctly absent)"
  - "quota/idempotency exceptions: ResourceNumberLimitExceededException now real for CreateDevEndpoint (gopherstack-dol3); IdempotentParameterMismatchException/OperationTimeoutException/ConcurrentModificationException remain open with real (not blanket) reasoning -- see the quota/idempotency gap-list note above"
  - "tag ARN dispatch: Blueprint/DevEndpoint/MLTransform/UserDefinedFunction fixed (gopherstack-dol3); CustomEntityType still has no ARN/Tags concept at all, out of scope -- see the tag-dispatch gap-list note above"
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

## This pass (gopherstack-dol3: tag ARN dispatch, workflow DAG, quota exceptions, DQDL sizing)

Five-part bd issue; did the first four accurately, sized the fifth without
starting it (per the issue's own instruction: assess DQDL size first, don't
start it if it's a project of its own).

1. **Tag ARN dispatch (Blueprint/DevEndpoint/MLTransform/UDF)** — done. See
   the `TagResource`/`UntagResource`/`GetTags` op notes above for the
   dispatch fix plus the two deeper bugs found alongside it (MLTransform/UDF
   had no Tags field at all, so creation-time tags were silently lost, not
   just unreachable; and `UpdateMLTransform`/`UpdateUserDefinedFunction`
   replace the whole record and were silently wiping tags on every update).
   Checked first whether a shared cross-service tag-ARN-dispatch mechanism
   exists in `pkgs/` per this repo's convention of reusing consolidated
   packages: it does not — `pkgs/tags` is a plain concurrency-safe map type,
   `pkgs/arn` only builds ARN strings, neither does resource-type dispatch.
   Other multi-resource-type services either replicate Glue's per-kind
   `find*ByARN` chain (no shared package) or sidestep the problem entirely
   with a flat ARN-keyed side map (ECS-style, only viable for single-kind
   resources or when tags aren't stored inline per typed resource, which
   Glue's `TaggedResources` doc comment already documents as a deliberate
   prior-pass choice). Extended the existing chain-of-if pattern rather than
   introducing a new abstraction.

2. **Workflow DAG (Graph/LastRun/statistics)** — Graph and LastRun done for
   real; statistics (and WorkflowRunStatistics/WorkflowRun.Graph generally)
   confirmed to need real per-run job/crawler-run correlation this backend
   doesn't have anywhere, so left absent. See the `workflows` op note above.

3. **ml-transform EvaluationMetrics** — re-confirmed correctly absent (no
   real ML evaluation is ever run in this backend); no code change. Fixed a
   related pre-existing bug (Tags being lost) while in this file for #1.

4. **Quota/idempotency/concurrency exceptions** — implemented one real,
   AWS-quota-verified exception (`ResourceNumberLimitExceededException` for
   `CreateDevEndpoint`, quota = 25, sourced from
   `docs.aws.amazon.com/general/latest/gr/glue.html` via WebFetch this pass,
   not memory). The other three were re-researched with real AWS
   documentation (WebFetch against `API_CreateJob.html`'s Errors section for
   the exact exception text) instead of the prior pass's blanket "no state to
   trigger these" — see the quota/idempotency gap-list note above for the
   full per-exception reasoning on why `IdempotentParameterMismatchException`
   was left open (real trigger condition not confidently derivable from the
   SDK alone) and why `ConcurrentModificationException`/
   `OperationTimeoutException` are structurally unreachable (coarse per-backend
   lock means no operation can ever race; nothing in a synchronous in-memory
   backend can genuinely time out).

5. **Schema-registry compatibility checking + DQDL validation — sized, not
   started.** Both are genuinely substantial, separable projects:
   - **DQDL** (Data Quality Definition Language) is AWS Glue's own rule DSL
     for `CreateDataQualityRuleset`'s `Ruleset` string (e.g. `Rules = [
     IsComplete "col", ColumnValues "col" between 0 and 100 ]`). A real
     implementation needs a lexer + parser for the full grammar (dozens of
     rule types — IsComplete, IsUnique, ColumnValues, ColumnCount, RowCount,
     Completeness, Uniqueness, DataFreshness, custom-SQL rules, composite
     AND/OR/NOT expressions, thresholds with `>`/`<`/`between`/percentages —
     see `docs.aws.amazon.com/glue/latest/dg/dqdl.html`), plus a decision on
     how much of it to actually *evaluate* against real table data (today
     `data_quality_stats.go`'s results are backend-tracked scores, not
     computed from the ruleset). This is comparable in scope to
     `pkgs/dynamodb/expr` (DynamoDB's expression parser) or a small SQL
     WHERE-clause parser — a standalone package, not a file-sized change.
   - **Schema-registry compatibility** needs a real per-`DataFormat`
     schema-diffing algorithm (AVRO/JSON/PROTOBUF each have their own
     compatibility rules — e.g. Avro's reader/writer schema resolution:
     field addition requires a default, type widening rules, etc.) for each
     of BACKWARD/FORWARD/FULL/BACKWARD_ALL/FORWARD_ALL/FULL_ALL. Realistic
     options are (a) hand-write simplified per-format diffing (weeks of edge
     cases to get right, easy to be subtly wrong in a way that's worse than
     absent) or (b) pull in a real schema library per format — the prior
     pass's ledger already noted "no new go.mod dependencies permitted" as a
     hard constraint, which rules out (b) without a policy exception.
   - **What it needs to be picked up**: (i) a decision on whether new
     go.mod dependencies are permitted for this specific feature (schema
     libraries especially — hand-rolled AVRO/PROTOBUF compatibility
     checking from scratch is high-risk); (ii) its own bd issue(s), separate
     from the other four items in this one, given the size difference; (iii)
     for DQDL specifically, a decision on scope — syntax validation only
     (reject malformed `Ruleset` strings, still don't evaluate rules against
     data) is a meaningfully smaller first slice than full rule evaluation,
     and would be the natural place to start.
   Not started this pass, per the issue's own instruction not to start it if
   it's a project of its own.

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
