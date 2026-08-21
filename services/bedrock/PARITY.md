service: bedrock
sdk_module: aws-sdk-go-v2/service/bedrock@v1.66.4
last_audit_commit: 5ee940036  # gopherstack-7ux2 (2026-08-13) fixed after this hash was recorded; hash not yet known at edit time
last_audit_date: 2026-08-13
overall: A            # RESTORED A-->A (parity-5, 2026-07-31, follow-up pass): the
                      # dispatchDocumentOps routing bug that caused the prior A->A- downgrade
                      # is fixed and proven. Re-verified both real wire shapes against the
                      # vendored SDK's request snapshots (aws-sdk-go-v2/service/bedrockagent
                      # IngestKnowledgeBaseDocuments.request.snap: PUT to the base
                      # .../documents path; ListKnowledgeBaseDocuments.request.snap: POST to
                      # that same base path; DeleteKnowledgeBaseDocuments.request.snap: POST
                      # to .../documents/deleteDocuments) before touching dispatch, per
                      # .claude/memories/parity-principles.md #2. dispatchDocumentOps now
                      # routes PUT-on-base to real Ingest and POST-on-base (GET too, as
                      # harmless leniency) to real List; DeleteKnowledgeBaseDocuments is now
                      # carved out by its real /deleteDocuments sub-path in
                      # dispatchDataSourceIDRoutes (handler_data_sources.go), alongside the
                      # pre-existing GetKnowledgeBaseDocuments /getDocuments carve-out --
                      # matching the fabricated-op finding below: the PUT-means-Update
                      # convenience route this bug shared a method with (handleUpdateKBDocuments
                      # / Backend.UpdateKnowledgeBaseDocuments) was itself fabricated (no such
                      # real operation) and is now deleted rather than left dead, per
                      # .claude/memories/parity-principles.md #5 (de-stub hygiene).
                      # TestKBDocumentsCRUD was rewritten to drive Ingest/List/Delete by their
                      # real method+path instead of the emulator's-own-wrong POST/GET/PUT/DELETE
                      # convention it previously encoded; TestKBDocumentsRealWireRouting was
                      # added as a dedicated regression test and confirmed failing against the
                      # pre-fix code (POST to base 404'd as a Validation error, not reaching a
                      # list) before applying the fix. See the AgentsHandler gaps entry below
                      # for the full history. Everything else: every named gap fixed for real;
                      # ARP sub-resource path model is a documented, still-open exception that
                      # did not block the prior A grade either. parity-4 (SDK v1.56.0 -> v1.66.0 bump): 10 new ops implemented for real, field-diffed against v1.66.0; grade held at A, see families.AdvancedPromptOptimizationJob/AccountDataRetention/ResourcePolicy below.

# Per-op status. wire=response/request shape vs SDK; errors=code+HTTP status;
# state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateGuardrail: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed — policy configs (contentPolicyConfig etc.) were nested under a fabricated \"policies\" wrapper object; real SDK sends them as top-level fields. Was silently dropping every guardrail's actual content/topic/word/PII/grounding config for real clients."}
  GetGuardrail: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed — output nesting (same bug, output side uses \"contentPolicy\" not \"contentPolicyConfig\", still top-level not nested). Also now honors ?guardrailVersion= to return the immutable numbered-version snapshot instead of always returning DRAFT."}
  ListGuardrails: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateGuardrail: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed — same policies-wrapper bug on input. Also removed a fabricated ConflictException gate that rejected updates once any numbered version existed; real AWS always allows editing DRAFT regardless of published versions."}
  DeleteGuardrail: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed — now honors ?guardrailVersion= (delete one numbered version vs. delete DRAFT+all versions), and no longer leaks orphaned GuardrailVersion rows when the whole guardrail is deleted."}
  CreateGuardrailVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed — GuardrailVersion now snapshots name/messaging/policies/arn at creation time (was only {guardrailId, version, description}), so numbered versions are actually immutable and independently retrievable via GetGuardrail(id, version)."}
  ListFoundationModels: {wire: ok, errors: ok, state: ok, persist: n/a, note: "seeded static catalog; shape verified against types.FoundationModelSummary"}
  GetFoundationModel: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetFoundationModelAvailability: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed — response only included agreementAvailability; authorizationStatus, entitlementAvailability, modelId, and regionAvailability are ALL required GetFoundationModelAvailabilityOutput fields and were silently zero-valued for any real client that inspects them. Now returns all five."}
  CreateFoundationModelAgreement: {wire: ok, errors: ok, state: ok, persist: ok}
  ListFoundationModelAgreementOffers: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed — this is a per-model OFFER CATALOG lookup keyed by a required modelId PATH parameter (real path: GET /list-foundation-model-agreement-offers/{modelId}), NOT a list of agreements the account has already created. gopherstack previously served it from the invented path \"/foundation-model-agreement-offers\" (no modelId) and returned {modelId} entries for every CreateFoundationModelAgreement call — a completely different resource, missing the required offerToken/termDetails fields. Now returns one deterministic, wire-shape-valid offer (offerToken/offerId/termDetails) per requested modelId."}
  DeleteFoundationModelAgreement: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed — was routed as DELETE /delete-foundation-model-agreement/{modelId} (path-param, wrong method); real SDK sends POST /delete-foundation-model-agreement with modelId in the JSON body. Also removed a fabricated no-op-on-empty-id 204 short-circuit; missing modelId is now a ValidationException."}
  CreateProvisionedModelThroughput: {wire: ok, errors: ok, state: ok, persist: ok}
  GetProvisionedModelThroughput: {wire: ok, errors: ok, state: ok, persist: ok}
  ListProvisionedModelThroughputs: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateProvisionedModelThroughput: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed — was routed on PUT (real SDK sends PATCH, so real clients could never reach this op); also accepted a fabricated \"modelId\"/\"modelUnits\" body (AWS has no unit-resize capability on Update, only desiredModelId + desiredProvisionedModelName, wrong JSON keys too). Now PATCH + desiredModelId/desiredProvisionedModelName, with name-uniqueness enforced on rename."}
  DeleteProvisionedModelThroughput: {wire: ok, errors: ok, state: ok, persist: ok}
  GetModelInvocationLoggingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  PutModelInvocationLoggingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteModelInvocationLoggingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateEvaluationJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED 2026-08-11 -- request field was wire-tagged tags; the real CreateEvaluationJobRequest field is jobTags, so every real client's tags were silently discarded"}
  GetEvaluationJob: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED 2026-08-20 (gopherstack-r80d, required-output sweep) -- OutputDataConfig (required, api_op_GetEvaluationJob.go:78) had no input parsing, no model field, and no response key at all; RoleArn was present but conditionally omitted when empty (harmless in practice: CreateEvaluationJob's real SDK client already requires roleArn, so it is always set by the time Get runs). Now OutputDataConfig round-trips Create->Get (empty {validators:[]}-style present-not-absent when omitted on Create) and roleArn is unconditional. NOT fixed: JobType (required, api_op_GetEvaluationJob.go:72) has no counterpart anywhere -- real AWS derives it from which variant of the EvaluationConfig union (Automated|Human) the caller supplied on Create, and gopherstack does not model that union at all. SEPARATELY DISCOVERED, more severe, NOT fixed: CreateEvaluationJobInput's real evaluationConfig/inferenceConfig are polymorphic union objects (types.EvaluationConfig/types.EvaluationInferenceConfig); gopherstack's createEvaluationJobInput instead expects evaluationConfig as a bare ARRAY and inferenceConfig as a flat object with unrelated field names. A real aws-sdk-go-v2 client's CreateEvaluationJob request 400s against gopherstack today whenever it supplies these two required members (confirmed while writing this fix's proof test) -- this op is effectively unreachable by a real client with real config content, independent of the OutputDataConfig fix. Both left open as file-follow-up: fixing them means redesigning the union parsing, out of this pass's required-output-member scope."}
  ListEvaluationJobs: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed — took zero params and always returned the full unbounded table in one page, ignoring nextToken entirely (unlike every sibling List op). Now supports nextToken/statusEquals/nameContains/creationTimeAfter/creationTimeBefore query filters via a real ListEvaluationJobsInput, mirroring ListModelInvocationJobs' filter pattern. applicationTypeEquals/sortBy/sortOrder still unhandled — see gaps."}
  BatchDeleteEvaluationJob: {wire: ok, errors: ok, state: ok, persist: ok}
  StopEvaluationJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed — was routed as DELETE /evaluation-jobs/{id} (plural); real SDK sends POST /evaluation-job/{id}/stop (SINGULAR, different HTTP verb). Completely unreachable by real clients before this fix — a route-matcher-class bug."}
  CreateModelCustomizationJob: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "fixed — customModelName (required, distinct from jobName per bedrock@v1.66.4 CreateModelCustomizationJobRequest) was accepted nowhere and silently dropped; the job's output model was never materialized as a CustomModel at all, so it could never be listed/gotten. Now validated as required and, on job completion, becomes a real CustomModel with a real baseModelArn (gopherstack-2wuv). FIXED 2026-08-13 (gopherstack-ii4c): three more required members -- RoleArn, OutputDataConfig, TrainingDataConfig (api_op_CreateModelCustomizationJob.go:66,75,80) -- were also accepted nowhere; the job had no IAM role, no output location, and no training data. Now validated (OutputDataConfig.S3Uri is itself required) and echoed on Get/List."}
  GetModelCustomizationJob: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED 2026-08-20 (gopherstack-r80d) -- ValidationDataConfig (required, api_op_GetModelCustomizationJob.go:85) had no counterpart anywhere: not parsed from the optional CreateModelCustomizationJobInput.ValidationDataConfig, not tracked on the model, not in the response shape at all (the \"member with no struct field at all\" class, matching iam's JobCompletionDate from the input-side sweep). Now parsed (validators[].s3Uri) and always emitted present, even {validators:[]} when the caller supplied none -- required-but-inapplicable means present-and-empty, not absent, matching this campaign's established convention."}
  ListModelCustomizationJobs: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed — per-item summaries reused GetModelCustomizationJob's outputModelArn/outputModelName wire keys; real ListModelCustomizationJobs uses customModelArn/customModelName instead (bedrock@v1.66.4 ModelCustomizationJobSummary via botocore), so those two fields silently deserialized to nil for every real SDK caller. Split into a dedicated summary shape (gopherstack-2wuv). sortBy/sortOrder still don't vary the sort field (always CreationTime) — see gaps."}
  StopModelCustomizationJob: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateCustomModel: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED 2026-08-11 -- request field was wire-tagged tags; the real CreateCustomModelRequest field is modelTags, so every real client's tags were silently discarded"}
  GetCustomModel: {wire: ok, errors: ok, state: ok, persist: ok, note: "now returns baseModelArn/customizationType/jobArn/jobName for customization-job output models (jobArn/jobName NULL for CreateCustomModel imports, matching bedrock@v1.66.4 GetCustomModelResponse) (gopherstack-2wuv)"}
  ListCustomModels: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed — baseModelArnEquals/foundationModelArnEquals now implemented: they match a completed CreateModelCustomizationJob's output model (real baseModelArn from baseModelIdentifier) and correctly never match a CreateCustomModel import, which has no base model in its wire input to filter on (gopherstack-2wuv). sortBy/sortOrder still don't vary the sort field (always CreationTime) — see gaps."}
  DeleteCustomModel: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateCustomModelDeployment: {wire: ok, errors: ok, state: ok, persist: ok}
  GetCustomModelDeployment: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed — List/Get/Update/Delete were routed under a fabricated \"/custom-model-deployments\" path; real SDK uses the SAME base path as Create (\"/model-customization/custom-model-deployments\") for all five ops. Completely unreachable by real clients before this fix."}
  ListCustomModelDeployments: {wire: ok, errors: ok, state: ok, persist: ok, note: "same fix as GetCustomModelDeployment"}
  UpdateCustomModelDeployment: {wire: ok, errors: ok, state: ok, persist: ok, note: "same path fix, PLUS: the shared Handler() body-reader only read request bodies for POST/PUT, never PATCH — so even with the path fixed, this PATCH op's body was silently discarded (fabricated no-op). Both fixed."}
  DeleteCustomModelDeployment: {wire: ok, errors: ok, state: ok, persist: ok, note: "same path fix as GetCustomModelDeployment"}
  CreateInferenceProfile: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "FIXED 2026-08-13 (gopherstack-ii4c) -- required member ModelSource (api_op_CreateInferenceProfile.go:48, the CopyFrom ARN this profile tracks) was accepted nowhere; the profile got a name but no model link. Now validated as required and echoed back on Get/List as the required Models list (api_op_GetInferenceProfile.go:62); this backend does not expand a system-defined profile's CopyFrom into its per-region constituent models, so Models always has exactly one entry."}
  GetInferenceProfile: {wire: ok, errors: ok, state: ok, persist: ok}
  ListInferenceProfiles: {wire: ok, errors: ok, state: ok, persist: ok, note: "nextToken pagination only; real AWS's sole extra filter (typeEquals: SYSTEM_DEFINED|APPLICATION) not implemented — see gaps"}
  DeleteInferenceProfile: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateModelCopyJob: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "FIXED (gopherstack-4sov) -- required member TargetModelName (api_op_CreateModelCopyJob.go:44) was accepted nowhere; the handler never read it and the backend fabricated its own target name (\"custom-model/copy-\"+id) instead, the opposite failure from a dropped field. Now validated as required (400 if missing) and used verbatim to build TargetModelArn (\"custom-model/\"+targetModelName) and stored on ModelCopyJob.TargetModelName. Proven via a real aws-sdk-go-v2 client round trip (TestParity_ModelCopyJob_TargetModelNameRoundTrip) that fails against the unfixed handler."}
  GetModelCopyJob: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED 2026-08-20 (gopherstack-r80d) -- SourceAccountId (required, api_op_GetModelCopyJob.go:55-59) was dropped entirely. Derived honestly from the account segment already embedded in the stored SourceModelArn (this backend's own ARNs, never fabricated), not a new tracked field."}
  ListModelCopyJobs: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateModelImportJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed — accepted only {jobName,tags}, silently dropping importedModelName, roleArn, and modelDataSource, all three \"This member is required\" on the real CreateModelImportJobInput. GetModelImportJob/ListModelImportJobs responses were therefore always missing importedModelName/roleArn/modelDataSource too. Now parses and stores all three; response includes them."}
  GetModelImportJob: {wire: ok, errors: ok, state: ok, persist: ok}
  ListModelImportJobs: {wire: fixed, errors: ok, state: ok, persist: ok, note: "gopherstack-uult: reused modelImportJobToOutput (the Get-shape converter) unscoped, leaking roleArn/modelDataSource/tags -- none of which types.ModelImportJobSummary declares (creationTime/jobArn/jobName/status/endTime/importedModelArn/importedModelName/lastModifiedTime only). Fixed with a dedicated modelImportJobToSummary."}
  GetImportedModel: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed — response invented a \"status\" field with no basis in the real GetImportedModelOutput shape (ImportedModel has no lifecycle status of its own), and used \"createdAt\" instead of the real \"creationTime\" key, while omitting the required modelArn/modelName/jobArn/jobName fields entirely. Now matches the real shape (modelArn, modelName, jobArn, jobName, creationTime, modelDataSource); the invented status field is deleted."}
  ListImportedModels: {wire: ok, errors: ok, state: ok, persist: n/a, note: "same field-shape fix as GetImportedModel (per-item). Also fixed: previously took zero params and returned every imported model unfiltered/unpaginated; now supports nameContains + creationTimeAfter/Before + nextToken."}
  DeleteImportedModel: {wire: ok, errors: ok, state: ok, persist: n/a, note: "status code fixed 204 -> 200 for consistency with DeleteImportedModelOutput's empty (non-204-specified) real shape, matching this service's other verified-ok Delete ops."}
  CreateModelInvocationJob: {wire: fixed, errors: ok, state: ok, persist: ok, note: "was routed under the PLURAL \"/model-invocation-jobs\" path; real SDK uses the SINGULAR \"/model-invocation-job\" for Create/Get/Stop (List alone is plural). Completely unreachable by real clients before that fix. gopherstack-7ux2: the handler also silently dropped modelId/roleArn/inputDataConfig/outputDataConfig/clientRequestToken from the request body even though the backend already accepted them via CreateModelInvocationJobInput opts -- so Get/List could never honestly source them either. Now parses and stores all five."}
  GetModelInvocationJob: {wire: fixed, errors: ok, state: ok, persist: ok, note: "same singular-path fix as Create. gopherstack-7ux2: response omitted modelId/inputDataConfig/outputDataConfig/roleArn/submitTime, all five \"This member is required\" on the real GetModelInvocationJobOutput (bedrock@v1.66.4 api_op_GetModelInvocationJob.go), and emitted a \"creationTime\" key the real shape doesn't have at all (harmless to a real client, which discards unknown keys, but still wrong). Fixed via a shared modelInvocationJobToSummary converter: submitTime reuses the existing CreationTime domain field (no separate creationTime key on the wire), the other four are now sourced from the Create fix above."}
  ListModelInvocationJobs: {wire: fixed, errors: ok, state: ok, persist: ok, note: "fixed — handler called Backend.ListModelInvocationJobs(nil), silently discarding every query param (statusEquals/nameContains/sortBy/sortOrder/nextToken/submitTimeAfter/submitTimeBefore) even though the backend already implements the full filter/sort/paginate logic. Classic disguised no-op: real-looking op, dead capability. Now parses and wires all of them. gopherstack-7ux2: summary also omitted modelId/inputDataConfig/outputDataConfig/roleArn/submitTime, all five required on types.ModelInvocationJobSummary (types/types.go:5592-5722) -- same converter and same Create-side fix as GetModelInvocationJob above. Confirmed types.ModelInvocationJobSummary carries no tags member, so job.Tags correctly stays off both Get and List (unlike ListModelImportJobs' summary just above, this shape needed members added, not removed)."}
  StopModelInvocationJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed — was DELETE on the plural list path; real SDK sends POST /model-invocation-job/{id}/stop (singular + /stop suffix, same pattern as StopEvaluationJob)."}
  CreateMarketplaceModelEndpoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed — EndpointConfig (SageMaker execution role/instance type/instance count/KMS key) is a required CreateMarketplaceModelEndpointInput field and was previously not parsed/stored at all, so every Get/List response was missing the required endpointConfig field. Now parsed, stored, and round-tripped."}
  GetMarketplaceModelEndpoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "now includes endpointConfig — see CreateMarketplaceModelEndpoint"}
  ListMarketplaceModelEndpoints: {wire: ok, errors: ok, state: ok, persist: ok, note: "now includes endpointConfig per item; nextToken pagination only, real AWS's sole extra filter (modelSourceEquals) not implemented — see gaps"}
  UpdateMarketplaceModelEndpoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed — request body's required EndpointConfig field was accepted but never parsed/applied (the op only bumped updatedAt, a disguised no-op). Now parses {\"endpointConfig\":{\"sageMaker\":{...}}} and applies it to the stored endpoint; omitting endpointConfig on PATCH now correctly preserves the existing config rather than erroring."}
  DeleteMarketplaceModelEndpoint: {wire: ok, errors: ok, state: ok, persist: ok}
  RegisterMarketplaceModelEndpoint: {wire: ok, errors: ok, state: ok, persist: ok}
  DeregisterMarketplaceModelEndpoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed — was routed as POST /.../deregistration (a path AWS doesn't have); real SDK sends DELETE on the SAME /.../registration path Register uses (method-only disambiguation). Completely unreachable by real clients before this fix."}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: n/a}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  GetUseCaseForModelAccess: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed — full redesign. Real GetUseCaseForModelAccessOutput.FormData is a required raw []byte payload wire-encoded as {\"formData\":\"<base64>\"}; gopherstack previously served a fabricated {useCaseType,useCaseDescription} JSON object from the typo'd path \"/usecase-for-model-access\". Now GET /use-case-for-model-access returns base64(storedBytes)."}
  PutUseCaseForModelAccess: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed — same redesign as Get, PLUS method: real SDK sends POST, gopherstack previously used PUT. Was 100% unreachable by real clients (wrong path AND method AND body shape) before this fix."}
  ListEnforcedGuardrailsConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed — full redesign. Real AWS models this as a ConfigId-keyed catalog of AccountEnforcedGuardrailOutputConfiguration entries (guardrailIdentifier/guardrailVersion/inputTags HONOR|IGNORE/modelEnforcement/owner/createdBy/updatedBy) at GET /enforcedGuardrailsConfiguration with nextToken pagination; gopherstack previously modeled it as bare guardrailId+guardrailVersion pairs at the invented kebab-case path \"/enforced-guardrail-configuration\" with no pagination. New backend validates guardrailIdentifier resolves to a real guardrail."}
  PutEnforcedGuardrailConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed — same redesign as List. Real PUT body is {configId?, guardrailInferenceConfig:{guardrailIdentifier,guardrailVersion,inputTags,modelEnforcement?}}; omitting configId creates a new config, supplying an existing one updates in place. inputTags is validated to HONOR|IGNORE."}
  DeleteEnforcedGuardrailConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed — real AWS takes ConfigId as a PATH parameter (DELETE /enforcedGuardrailsConfiguration/{configId}); gopherstack previously took guardrailId as a QUERY parameter on the invented path. 100% unreachable by real clients before this fix (in addition to modeling the wrong resource)."}
  # parity-4: 10 ops added by the aws-sdk-go-v2/service/bedrock bump from
  # v1.56.0 to v1.66.0. All implemented for real (routing, backend state,
  # request parsing, response wire shape, error codes/HTTP status,
  # Snapshot/Restore persistence) and field-diffed against v1.66.0's
  # api_op_*.go/serializers.go/deserializers.go — none were dumped into a
  # notImplemented list.
  CreateAdvancedPromptOptimizationJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "field-diffed against CreateAdvancedPromptOptimizationJobInput/Output. Real AWS returns HTTP 200 with only {jobArn}; validates the 4 required fields (jobName, inputConfig.s3Uri, outputConfig.s3Uri, modelConfigurations 1-5 items each with a modelId) as ValidationException."}
  GetAdvancedPromptOptimizationJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "field-diffed against GetAdvancedPromptOptimizationJobOutput. Real AWS's response has NO field for the optimized prompt text at all — results are written to the caller's S3 OutputConfig location, entirely outside the API. Never fabricates one; see AdvancedPromptOptimizationJob's doc comment in models.go and TestHandler_AdvancedPromptOptimizationJobLifecycle's 'never fabricates a result' case."}
  ListAdvancedPromptOptimizationJobs: {wire: ok, errors: ok, state: ok, persist: n/a, note: "field-diffed against ListAdvancedPromptOptimizationJobsInput/Output. Real AWS has no name/status filter for this op (unlike sibling List ops) — only sortBy(CreationTime)/sortOrder/maxResults/nextToken, all implemented via paginate (honors real MaxResults, unlike paginateBedrockSlice used by some sibling Lists)."}
  StopAdvancedPromptOptimizationJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "real AWS returns HTTP 200 empty body. Transitions InProgress -> Stopped directly, skipping the real intermediate 'Stopping' status — same simplification this service already makes for StopModelCustomizationJob/StopEvaluationJob/StopModelInvocationJob (see advanced_prompt_optimization_jobs.go doc comment); not a new gap class."}
  BatchDeleteAdvancedPromptOptimizationJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed proactively — real path is POST /advanced-prompt-optimization-job/batch-delete, SINGULAR, distinct from the PLURAL /advanced-prompt-optimization-jobs every other op in this family uses (same bug class this campaign already fixed for StopEvaluationJob/CreateModelInvocationJob elsewhere in this service; caught during field-diffing, not via a bug report, since this op is new). Validates 1-25 jobIdentifiers per real AWS; real HTTP 202 modeled as 202."}
  GetAccountDataRetention: {wire: ok, errors: ok, state: ok, persist: ok, note: "field-diffed against GetAccountDataRetentionOutput. Mode is a required field on the real shape; an account that never called Put still gets a value back, defaulting to \"default\" (types.DataRetentionModeDefault) rather than a zero value."}
  PutAccountDataRetention: {wire: ok, errors: ok, state: ok, persist: ok, note: "field-diffed against PutAccountDataRetentionInput/Output. Validates mode is one of default|none|provider_data_share|inherit (types.DataRetentionMode's 4 enum values) as ValidationException."}
  GetResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "core-bedrock flavor (POST/GET/DELETE /resource-policy, field name \"resourcePolicy\", no revisionId) — DISTINCT operation from bedrock-agent's own GetResourcePolicy (PUT/GET/DELETE /resourcepolicy, no hyphen, field name \"policy\", WITH revisionId, scoped to knowledge bases only); see resource_policy.go's package doc comment for the full field-diff of both flavors against their respective SDKs. Real AWS docs the target only as \"a Bedrock resource\" with no documented ARN-pattern allowlist; this backend validates the ARN is Bedrock-shaped AND resolves to a resource it actually models (guardrail/custom model/custom model deployment/provisioned model throughput/automated reasoning policy/prompt router/inference profile/marketplace endpoint) rather than accepting any string. Agent-domain resources (agents/flows/prompts/knowledge bases) are out of this validation's reach by construction, not by omission: Handler and AgentsHandler use separate InMemoryBackend instances in this codebase (see provider.go), so core bedrock's Handler never holds agent-domain state regardless of what the validator checks for."}
  PutResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "core-bedrock flavor — see GetResourcePolicy entry. Real AWS returns HTTP 201 (verified against the AWS API Reference, not just the SDK — an unusual deviation from this service's other Put-op convention of 200), which the response op models correctly."}
  DeleteResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "core-bedrock flavor — see GetResourcePolicy entry. Real AWS returns HTTP 200 empty body."}

families:
  AdvancedPromptOptimizationJob: {status: ok, note: "new family, parity-4. See the 5 ops entries above. Backend models the real job lifecycle (InProgress -> Completed via the janitor, or -> Stopped) honestly; produces no fabricated optimization result, matching the real wire shape's total absence of one."}
  AccountDataRetention: {status: ok, note: "new family, parity-4. See GetAccountDataRetention/PutAccountDataRetention ops entries."}
  ResourcePolicy: {status: ok, note: "new family, parity-4, TWO DISTINCT real operation families sharing an op name — core bedrock (guardrails/custom models/etc.) and bedrock-agent (knowledge bases only, with optimistic-concurrency revisionId). See GetResourcePolicy/PutResourcePolicy/DeleteResourcePolicy ops entries and resource_policy.go's package doc comment. bedrock-agent's knowledge-base ARN regex is intentionally widened to accept hyphens (real AWS documents pure alphanumeric KB IDs) because this backend's own CreateKnowledgeBase generates hyphenated IDs like \"kb-00000001\" — narrowing to the real character class would make every gopherstack-issued KB ARN unmatchable by this backend's own validator; documented in resource_policy.go."}
  AutomatedReasoningPolicy: {status: partial, note: "high-value route-reachability bugs fixed this pass: UpdateAutomatedReasoningPolicy, UpdateAutomatedReasoningPolicyTestCase, and UpdateAutomatedReasoningPolicyAnnotations were all routed on PUT; real SDK sends PATCH for all three, so all three were 100% unreachable by real clients before this fix (same bug class as UpdateProvisionedModelThroughput/UpdateMarketplaceModelEndpoint, fixed in earlier passes). NOT fixed this pass, and NOT reclassified to ok — see gaps: the build-workflow-scoped sub-resource path model (annotations, next-scenario, test-results, ExportAutomatedReasoningPolicyVersion) has deeper invented-path issues than a route fix can address; UpdateAutomatedReasoningPolicyTestCase's handler doesn't parse its request body at all (disguised no-op even now that it's reachable). FIXED (gopherstack-lx5h) — GetAutomatedReasoningPolicy's response (handler_automated_reasoning_policies.go handleGetAutomatedReasoningPolicy) dropped definitionHash and version, both required per GetAutomatedReasoningPolicyOutput and already tracked on the model (policy.DefinitionHash/policy.Version); now emitted. Also dropped the required policyId, not tracked as its own field on the model — derived honestly via policyIDFromARN, the path segment CreateAutomatedReasoningPolicy itself embedded when building policy.PolicyArn (arn.Build(..., \"automated-reasoning-policy/\"+id)), not fabricated. Also removed a \"status\" key the handler emitted that has no counterpart anywhere in the real GetAutomatedReasoningPolicyOutput (verified against its full deserializer switch: createdAt/definitionHash/description/kmsKeyArn/name/policyArn/policyId/updatedAt/version, no status) — harmless to any real client (unknown keys are ignored) but wrong wire shape. kmsKeyArn remains correctly absent: the model tracks no per-policy KMS key at all, and the real doc says the field is omitted entirely when none was provided at creation, so an absent field here is honest, not a gap. FIXED (gopherstack-4sov) — this note previously named only UpdateAutomatedReasoningPolicyTestCase's disguised no-op; the identical drops-the-defining-field defect existed in three more ops, undercounted here until now. StartAutomatedReasoningPolicyBuildWorkflow never read the request body at all, dropping both required members BuildWorkflowType and SourceContent (api_op_StartAutomatedReasoningPolicyBuildWorkflow.go:37-53) — worse, gopherstack's own route for it was also wrong (bare .../build-workflows POST; real path is .../build-workflows/{buildWorkflowType}/start, serializers.go:8008), so no real client's Start request had ever reached the handler regardless of body parsing. UpdateAutomatedReasoningPolicy dropped required PolicyDefinition (its input struct held only Description) and its response returned a fabricated \"status\" key instead of the real required definitionHash. UpdateAutomatedReasoningPolicyAnnotations dropped BOTH required body members (Annotations, LastUpdatedAnnotationSetHash) and answered with an empty 200 body instead of the real required annotationSetHash/buildWorkflowId/policyArn/updatedAt — Annotations had been invisible to a literal-match scanner because sibling op GetAutomatedReasoningPolicyAnnotations independently emits its own \"annotations\" response key, a false-negative mechanism documented as recurring across this campaign. All three now read their full request body, validate the required members, and return the real response shape. GetAutomatedReasoningPolicyAnnotations also gained the required annotationSetHash it was missing (it doubles as the concurrency token Update's LastUpdatedAnnotationSetHash checks against; validated as present but not matched, same non-enforcement already established for CreateAutomatedReasoningPolicyVersion's lastUpdatedDefinitionHash). Get/ListAutomatedReasoningPolicyBuildWorkflow also gained buildWorkflowType, now real backing state captured at Start. Proven via real aws-sdk-go-v2 client round trips (handler_automated_reasoning_policies_typed_test.go) that fail against each unfixed handler when hand-reverted. NOT fixed, left deliberately inert and documented: GetAutomatedReasoningPolicyBuildWorkflowResultAssets still ignores the required AssetType filter, but resultAssets is always [] regardless (no result-asset content generator in this backend), so the filter miss is currently unobservable by any test, real-client or otherwise — revisit only if this backend starts producing real result-asset content. UpdateAutomatedReasoningPolicyTestCase remains the disguised no-op already named above, not touched by this pass; the build-workflow-scoped sub-resource path gaps already named above also remain open. FIXED 2026-08-20 (gopherstack-r80d, required-output-member sweep): six more required-output-member bugs in this family, all confirmed via real-client round trips (wire_output_required_r80d_test.go) that fail against each unfixed handler. GetAutomatedReasoningPolicyBuildWorkflow/ListAutomatedReasoningPolicyBuildWorkflows dropped required CreatedAt/UpdatedAt (api_op_GetAutomatedReasoningPolicyBuildWorkflow.go:59-78) -- the model tracked neither field at all; now set at Start and bumped at Cancel. GetAutomatedReasoningPolicyAnnotations dropped 4 of its 6 required members (BuildWorkflowId/Name/PolicyArn/UpdatedAt, api_op_GetAutomatedReasoningPolicyAnnotations.go:62-80), returning only annotations/annotationSetHash; UpdatedAt is now lazily minted and persisted the same way annotationSetHash already was (new arpAnnotationsUpdatedAt map, store.go, intentionally not snapshotted for the same reason). GetAutomatedReasoningPolicyBuildWorkflowResultAssets dropped required PolicyArn (api_op_GetAutomatedReasoningPolicyBuildWorkflowResultAssets.go:67-70). GetAutomatedReasoningPolicyTestCase returned the test case's fields inlined at the top level instead of wrapped under the required \"testCase\" key (deserializers.go:7223-7233) -- a real client's required TestCase decoded nil regardless of PolicyArn happening to be present. GetAutomatedReasoningPolicyTestResult/ListAutomatedReasoningPolicyTestResults returned a flat, differently-keyed object with no \"testResult\"/wrapped-item shape at all (types.go:2055-2092 requires policyArn/testCase/testRunStatus/updatedAt nested under each result) -- same wrong-response-shape class as opensearch's GetIndex from the input-side sweep. All six fixed without fabricating data: CreatedAt/UpdatedAt/PolicyArn/BuildWorkflowId were either already-tracked real state or derivable from function parameters; testRunStatus is hardcoded to COMPLETED, the same simplification this family already made pre-fix (backend never runs a real test)."}
  PromptRouter: {status: ok, note: "fixed — field-diffed for real this pass (previously only spot-checked). CreatePromptRouterInput's required FallbackModel/Models/RoutingCriteria fields (and optional Description) were silently dropped entirely, so every Get/List response was missing them (all required on GetPromptRouterOutput/PromptRouterSummary) and Type was never set. ListPromptRouters returned the wrong top-level key (\"promptRouters\" vs real \"promptRouterSummaries\"), had no pagination, and ignored the real typeEquals filter. DeletePromptRouter used 204 instead of this service's established 200-for-empty-Delete convention. All fixed."}
  ImportedModel: {status: ok, note: "fixed — field-diffed for real this pass (previously only spot-checked). See GetImportedModel/ListImportedModels/DeleteImportedModel/CreateModelImportJob ops entries above for the specific wire-shape and filter/pagination fixes."}
  UseCaseForModelAccess: {status: ok, note: "fixed — full redesign this pass, see GetUseCaseForModelAccess/PutUseCaseForModelAccess ops entries."}
  EnforcedGuardrailConfiguration: {status: ok, note: "fixed — full redesign this pass, see List/Put/DeleteEnforcedGuardrailConfiguration ops entries."}
  FoundationModelAgreement: {status: ok, note: "fixed — field-diffed for real this pass (previously only spot-checked, and the note itself was wrong: ListFoundationModelAgreementOffers is NOT a resource-shape question, it's a completely different operation than gopherstack implemented). See ListFoundationModelAgreementOffers/DeleteFoundationModelAgreement ops entries."}
  FoundationModelAvailability: {status: ok, note: "fixed — field-diffed for real this pass. See GetFoundationModelAvailability ops entry."}

gaps:
  - "gopherstack-r80d (2026-08-20, required-output-member sweep): two related,
    NOT-fixed gaps found while fixing GetEvaluationJob's OutputDataConfig (see
    its ops entry above for the full detail). (1) GetEvaluationJobOutput's
    required JobType (api_op_GetEvaluationJob.go:72) has no counterpart --
    real AWS derives it from which variant (Automated|Human) of the
    EvaluationConfig union the caller supplied on Create, and gopherstack
    does not model that union. (2) SEPARATELY, more severe: real
    CreateEvaluationJobInput's evaluationConfig/inferenceConfig are
    polymorphic union objects; gopherstack's own request parser instead
    expects evaluationConfig as a bare array and inferenceConfig as an
    unrelated flat shape, so a real aws-sdk-go-v2 client's CreateEvaluationJob
    request 400s whenever it actually supplies real config content --
    confirmed while writing wire_output_required_r80d_test.go. Fixing either
    means redesigning the union parsing/response shape, out of this pass's
    required-output-member scope. (bd: file follow-up)"
  - "AgentsHandler (bedrock-agent sub-API, handler_agents_dispatch.go) GetSupportedOperations
    phantom-triage pass (parity-5, 2026-07-31): the reverse sdkcheck (gopherstack-vhw2,
    checked against bedrockagentsdk.Client) previously flagged 7 fabricated entries.
    5 were genuinely fabricated (no such bedrock-agent operation exists) and delisted:
    CreateAgentVersion (real AWS creates a new agent version only via PrepareAgent, already
    advertised and correctly wired at the canonical POST .../agentversions/DRAFT path);
    DeletePromptVersion/GetPromptVersion/ListPromptVersions (real AWS gets/deletes a specific
    prompt version via GetPrompt/DeletePrompt's promptVersion query param on the base
    /prompts/{id}/ path and lists versions via ListPrompts' promptIdentifier param — no
    distinct operation); UpdateKnowledgeBaseDocuments (real IngestKnowledgeBaseDocuments,
    already advertised, both adds and updates documents — no separate update call). All 5
    remain wired as non-canonical internal routes (used by this package's own test suite)
    but are unreachable by any real bedrock-agent SDK client and are no longer advertised —
    see the inline comments at each list entry in handler_agents_dispatch.go for routing
    detail per case. UPDATE (parity-5, 2026-07-31, follow-up pass): UpdateKnowledgeBaseDocuments
    is the one exception to 'remain wired' above — its route shared the PUT method with real
    IngestKnowledgeBaseDocuments on the same base path (see the dispatchDocumentOps gaps
    entry below), so re-plumbing PUT to the real op left it with no route at all; its handler
    (handleUpdateKBDocuments) and backend method (Backend.UpdateKnowledgeBaseDocuments) were
    deleted rather than left as dead code, per .claude/memories/parity-principles.md #5. The
    other 4 (CreateAgentVersion, DeletePromptVersion/GetPromptVersion/ListPromptVersions)
    are unaffected and remain wired as described. The other 2 (GetAgentMemory/DeleteAgentMemory) are real AWS operations
    but on bedrock-agent-runtime (a separate data-plane client this repo does not vendor as
    its own service), not bedrock-agent (the control-plane client the completeness check
    tests against) — correctly implemented and left advertised; the check will keep flagging
    them for that reason. (bd: file follow-up)"
  - "FIXED (parity-5, 2026-07-31, follow-up pass) — was: 'SEVERE, discovered while
    investigating the UpdateKnowledgeBaseDocuments phantom above (parity-5/phantom-triage,
    2026-07-31): dispatchDocumentOps (handler_knowledge_base_documents.go)... dispatches
    purely by HTTP method (GET/POST/PUT/DELETE) instead of the real per-path operation
    names... ListKnowledgeBaseDocuments and DeleteKnowledgeBaseDocuments, BOTH real,
    already-advertised operations, are UNREACHABLE via their real wire shape today...
    Downgraded overall: A->A- for this.' Re-verified all three real wire shapes against
    the vendored SDK's request snapshots (aws-sdk-go-v2/service/bedrockagent
    IngestKnowledgeBaseDocuments.request.snap: PUT base path;
    ListKnowledgeBaseDocuments.request.snap: POST base path;
    DeleteKnowledgeBaseDocuments.request.snap: POST .../deleteDocuments) before touching
    dispatch, per .claude/memories/parity-principles.md #2. dispatchDocumentOps now
    handles only the base .../documents path (PUT->Ingest, POST/GET->List;
    dispatchDataSourceIDRoutes only reaches it once the /getDocuments and
    /deleteDocuments sub-paths have already been carved out by exact match, so a
    dsSuffix check inside dispatchDocumentOps itself guards against any other
    unexpected suffix reaching it). DeleteKnowledgeBaseDocuments is now carved out in
    dispatchDataSourceIDRoutes by its real /deleteDocuments sub-path, the same way
    GetKnowledgeBaseDocuments already was. The fabricated PUT-means-Update convenience
    route this bug shared a method with (handleUpdateKBDocuments,
    Backend.UpdateKnowledgeBaseDocuments — see the UpdateKnowledgeBaseDocuments phantom
    finding this gap was originally discovered investigating) is now genuinely
    unreachable rather than internally-wired-but-fabricated, so both were DELETED per
    .claude/memories/parity-principles.md #5 (de-stub hygiene) instead of left dead.
    dispatchDataSourceIDRoutes was split into dispatchDataSourceIngestionRoutes and
    dispatchDataSourceDocumentRoutes (handler_data_sources.go) to keep its cyclomatic
    complexity under the repo's cyclop gate after adding the new deleteDocuments case.
    TestKBDocumentsCRUD (handler_knowledge_base_documents_test.go) and its two
    ingest-then-verify siblings (TestAccuracy_KBDocuments_IngestWithBDAParsingStrategy,
    TestAccuracy_KBDocuments_GetSpecificDocuments), plus one call site in
    handler_agent_knowledge_base_associations_test.go, were rewritten off the
    emulator's-own-wrong POST=ingest/GET=list/PUT=update/DELETE=delete convention onto
    the real PUT=ingest/POST=list/POST-to-deleteDocuments=delete wire shapes. Added
    TestKBDocumentsRealWireRouting as a dedicated regression test asserting each of
    PUT and POST on the base path reaches its correct handler; confirmed failing
    against the pre-fix code (POST to the base path 404'd as a ValidationException,
    silently treated as an empty Ingest, never reaching List) before applying the fix.
    Restored overall: A-->A. (bd: file follow-up closed)"
  - "FIXED (gopherstack-7znk): AutomatedReasoningPolicy sub-resource path model —
    Get/UpdateAutomatedReasoningPolicyAnnotations, GetAutomatedReasoningPolicyNextScenario,
    Get/ListAutomatedReasoningPolicyTestResult(s), and StartAutomatedReasoningPolicyTestWorkflow
    are now build-workflow-scoped (.../build-workflows/{buildWorkflowId}/...), matching
    bedrock@v1.66.4 serializers.go:3874/:4122/:4282/:5937/:8117; arpAnnotations is now
    keyed by (policyARN, buildWorkflowID). ExportAutomatedReasoningPolicyVersion now
    routes GET (not POST) at /automated-reasoning-policies/{policyArn}/export with no
    separate {version} segment (serializers.go:3603) — a versioned export passes the
    versioned ARN itself; an unversioned (draft) ARN 404s since gopherstack does not
    track a separate draft policy definition to export. The two previously-invented
    endpoints with no direct real-AWS path shape, isARPTestCaseRunPath
    (\"/test-cases/{id}/run\") and \"/versions/{version}/export\", were corrected onto
    their real counterparts (StartAutomatedReasoningPolicyTestWorkflow's real shape
    takes an optional testCaseIds list in the body, not a single test case in the
    path) rather than deleted, since both operations do exist in real AWS. All 6
    re-verified individually against the pinned SDK per
    .claude/memories/parity-principles.md #2 before changing routes. (bd: gopherstack-7znk closed)"
  - "UpdateAutomatedReasoningPolicyTestCase: now reachable (PATCH fixed), but handleUpdateARPTestCase never reads/parses the request body — it's a disguised no-op that only echoes testCaseId/policyArn back. Needs real UpdateAutomatedReasoningPolicyTestCaseInput field support (expression/inputText/expectedAggregatedFindingsResult per the real SDK). (bd: file follow-up)"
  - "ListCustomModels and ListModelCustomizationJobs: sortBy is parsed but never changes the sort field (always CreationTime, real AWS's default) — no ValidationException on an unrecognized value either. Low risk. (bd: file follow-up)"
  - "ListInferenceProfiles: missing the real typeEquals (SYSTEM_DEFINED|APPLICATION) filter. ListMarketplaceModelEndpoints: missing the real modelSourceEquals filter. Both low-risk (nextToken pagination already correct). (bd: file follow-up)"
  - "ListEvaluationJobs: applicationTypeEquals filter and sortBy/sortOrder not implemented (statusEquals/nameContains/creationTimeAfter/creationTimeBefore/nextToken now are, see ops entry). (bd: file follow-up)"
  - "RegisterMarketplaceModelEndpoint: real RegisterMarketplaceModelEndpointInput requires both endpointIdentifier and modelSourceIdentifier in the body; gopherstack's handler takes only the path-param ID and never reads/validates a request body. Not touched this pass — spotted while field-diffing the surrounding marketplace-endpoint family but out of this pass's named scope. (bd: file follow-up)"
  - "bedrock-agent DeleteResourcePolicy (parity-4): the real response's revisionId field is documented only as \"the revision identifier after the resource policy was deleted\" — ambiguous whether AWS mints a fresh post-delete marker or echoes the just-deleted policy's own revision. gopherstack returns the latter (the deleted policy's own RevisionID), a defensible reading but unverified against a real API response. Low risk: DeleteResourcePolicy's real Input has no further use for this value (only Put/subsequent-Delete's expectedRevisionId does, and a deleted resource has no policy left to update). (bd: file follow-up if a real captured response ever surfaces to confirm/refute)"
  - "ListAdvancedPromptOptimizationJobs (parity-4): does not validate sortBy against the real single allowed value (CreationTime) — an unrecognized value is silently ignored rather than raising ValidationException. Same low-risk shape as this service's other List ops' unvalidated sort/filter params (see ListCustomModels/ListModelCustomizationJobs gap above). (bd: file follow-up)"

deferred: []
# Every item previously listed here (AutomatedReasoningPolicy full wire re-verification,
# PromptRouter, ImportedModel, FoundationModelAgreement / FoundationModelAvailability) was
# field-diffed for real this pass. AutomatedReasoningPolicy is downgraded from a blanket
# "spot-checked, ok" to "partial" (see families) rather than reclassified to ok, since its
# sub-resource path model has real, documented gaps above -- not silently marked done.

leaks: {status: clean, note: "no new goroutines, tickers, or unregistered maps introduced this pass. enforcedGuardrailConfigs remains a single store.Table (now ConfigID-keyed instead of guardrailID-keyed) with no cascade-cleanup requirement (real AWS does not cascade-delete AccountEnforcedGuardrailConfig rows when the referenced guardrail is deleted, so gopherstack doesn't either). All new/changed backend methods (PutEnforcedGuardrailConfiguration, PutUseCaseForModelAccess, CreatePromptRouter, ListImportedModels, CreateModelImportJob, UpdateMarketplaceModelEndpoint, ListFoundationModelAgreementOffers, DeleteFoundationModelAgreement) acquire b.mu.Lock/RLock and release via defer on every path, including early-return validation-error paths. janitor.go's single ticker is unchanged.

  parity-4: advancedPromptOptimizationJobs and resourcePolicies are both real
  store.Table registrations (store_setup.go), reset via registry.ResetAll
  like every other table, with no cascade-cleanup requirement (deleting a
  guardrail/custom model/etc. does not cascade-delete its resourcePolicies
  row, matching real AWS's lack of documented cascade behavior here either).
  accountDataRetention is a single-pointer field (same shape as
  loggingConfig), reset to nil in both resetAuxState (core Handler.Reset)
  and Restore's resetRawState. All new/changed backend methods
  (CreateAdvancedPromptOptimizationJob, GetAdvancedPromptOptimizationJob,
  ListAdvancedPromptOptimizationJobs, StopAdvancedPromptOptimizationJob,
  BatchDeleteAdvancedPromptOptimizationJob, GetAccountDataRetention,
  PutAccountDataRetention, PutResourcePolicy, GetResourcePolicy,
  DeleteResourcePolicy, PutKnowledgeBaseResourcePolicy,
  GetKnowledgeBaseResourcePolicy, DeleteKnowledgeBaseResourcePolicy)
  acquire b.mu.Lock/RLock and release via defer on every path, including
  early-return validation-error paths. janitor.go's single ticker gained one
  more per-tick call (AdvanceAdvancedPromptOptimizationJobStatuses); no new
  ticker was added. AgentsHandler.Reset() additionally resets
  resourcePolicyRevisionCounter now (the table itself was already covered by
  registry.ResetAll, but the standalone revision counter needed an explicit
  reset alongside this method's other manually-listed counters)."}
