service: bedrock
sdk_module: aws-sdk-go-v2/service/bedrock@v1.56.0
last_audit_commit: 01dbe288
last_audit_date: 2026-07-12
overall: A            # genuine fixes found, several critical (see gaps for what's left)

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
  CreateProvisionedModelThroughput: {wire: ok, errors: ok, state: ok, persist: ok}
  GetProvisionedModelThroughput: {wire: ok, errors: ok, state: ok, persist: ok}
  ListProvisionedModelThroughputs: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateProvisionedModelThroughput: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed — was routed on PUT (real SDK sends PATCH, so real clients could never reach this op); also accepted a fabricated \"modelId\"/\"modelUnits\" body (AWS has no unit-resize capability on Update, only desiredModelId + desiredProvisionedModelName, wrong JSON keys too). Now PATCH + desiredModelId/desiredProvisionedModelName, with name-uniqueness enforced on rename."}
  DeleteProvisionedModelThroughput: {wire: ok, errors: ok, state: ok, persist: ok}
  GetModelInvocationLoggingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  PutModelInvocationLoggingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteModelInvocationLoggingConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateEvaluationJob: {wire: ok, errors: ok, state: ok, persist: ok}
  GetEvaluationJob: {wire: ok, errors: ok, state: ok, persist: ok}
  ListEvaluationJobs: {wire: ok, errors: ok, state: ok, persist: ok, note: "no pagination (returns full unbounded list, no nextToken/maxResults) — see gaps"}
  BatchDeleteEvaluationJob: {wire: ok, errors: ok, state: ok, persist: ok}
  StopEvaluationJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed — was routed as DELETE /evaluation-jobs/{id} (plural); real SDK sends POST /evaluation-job/{id}/stop (SINGULAR, different HTTP verb). Completely unreachable by real clients before this fix — a route-matcher-class bug."}
  CreateModelCustomizationJob: {wire: ok, errors: ok, state: ok, persist: ok}
  GetModelCustomizationJob: {wire: ok, errors: ok, state: ok, persist: ok}
  ListModelCustomizationJobs: {wire: ok, errors: ok, state: ok, persist: ok}
  StopModelCustomizationJob: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateCustomModel: {wire: ok, errors: ok, state: ok, persist: ok}
  GetCustomModel: {wire: ok, errors: ok, state: ok, persist: ok}
  ListCustomModels: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteCustomModel: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateCustomModelDeployment: {wire: ok, errors: ok, state: ok, persist: ok}
  GetCustomModelDeployment: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed — List/Get/Update/Delete were routed under a fabricated \"/custom-model-deployments\" path; real SDK uses the SAME base path as Create (\"/model-customization/custom-model-deployments\") for all five ops. Completely unreachable by real clients before this fix."}
  ListCustomModelDeployments: {wire: ok, errors: ok, state: ok, persist: ok, note: "same fix as GetCustomModelDeployment"}
  UpdateCustomModelDeployment: {wire: ok, errors: ok, state: ok, persist: ok, note: "same path fix, PLUS: the shared Handler() body-reader only read request bodies for POST/PUT, never PATCH — so even with the path fixed, this PATCH op's body was silently discarded (fabricated no-op). Both fixed."}
  DeleteCustomModelDeployment: {wire: ok, errors: ok, state: ok, persist: ok, note: "same path fix as GetCustomModelDeployment"}
  CreateInferenceProfile: {wire: ok, errors: ok, state: ok, persist: ok}
  GetInferenceProfile: {wire: ok, errors: ok, state: ok, persist: ok}
  ListInferenceProfiles: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteInferenceProfile: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateModelCopyJob: {wire: ok, errors: ok, state: ok, persist: ok}
  GetModelCopyJob: {wire: ok, errors: ok, state: ok, persist: ok}
  ListModelCopyJobs: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateModelImportJob: {wire: ok, errors: ok, state: ok, persist: ok}
  GetModelImportJob: {wire: ok, errors: ok, state: ok, persist: ok}
  ListModelImportJobs: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateModelInvocationJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed — was routed under the PLURAL \"/model-invocation-jobs\" path; real SDK uses the SINGULAR \"/model-invocation-job\" for Create/Get/Stop (List alone is plural). Completely unreachable by real clients before this fix."}
  GetModelInvocationJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "same singular-path fix as Create"}
  ListModelInvocationJobs: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed — handler called Backend.ListModelInvocationJobs(nil), silently discarding every query param (statusEquals/nameContains/sortBy/sortOrder/nextToken/submitTimeAfter/submitTimeBefore) even though the backend already implements the full filter/sort/paginate logic. Classic disguised no-op: real-looking op, dead capability. Now parses and wires all of them."}
  StopModelInvocationJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed — was DELETE on the plural list path; real SDK sends POST /model-invocation-job/{id}/stop (singular + /stop suffix, same pattern as StopEvaluationJob)."}
  CreateMarketplaceModelEndpoint: {wire: ok, errors: ok, state: ok, persist: ok}
  GetMarketplaceModelEndpoint: {wire: ok, errors: ok, state: ok, persist: ok}
  ListMarketplaceModelEndpoints: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateMarketplaceModelEndpoint: {wire: partial, errors: ok, state: partial, persist: ok, note: "route fixed — was PUT (real SDK sends PATCH, unreachable before); Handler() PATCH-body-read gap also fixed. Still doesn't parse/apply the request body's endpointConfig fields (pre-existing gap, out of this pass's scope) — see gaps."}
  DeleteMarketplaceModelEndpoint: {wire: ok, errors: ok, state: ok, persist: ok}
  RegisterMarketplaceModelEndpoint: {wire: ok, errors: ok, state: ok, persist: ok}
  DeregisterMarketplaceModelEndpoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed — was routed as POST /.../deregistration (a path AWS doesn't have); real SDK sends DELETE on the SAME /.../registration path Register uses (method-only disambiguation). Completely unreachable by real clients before this fix."}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: n/a}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}

families:
  AutomatedReasoningPolicy: {status: ok, note: "not in this pass's priority list; spot-checked path tree (build-workflows/test-cases/versions/annotations) against serializers.go, shapes look consistent. Not exhaustively re-verified — deferred for a future pass."}
  PromptRouter: {status: ok, note: "deferred — spot check only, not a named priority family this pass"}
  ImportedModel: {status: ok, note: "deferred — spot check only"}
  UseCaseForModelAccess: {status: gap, note: "path typo (\"/usecase-for-model-access\" vs real \"/use-case-for-model-access\") AND method (PUT vs real POST) AND body shape (real PutUseCaseForModelAccessInput is a raw FormData []byte payload, not JSON {useCaseType,useCaseDescription}). Deep semantic mismatch beyond a route fix; deliberately NOT touched this pass — flagging as a gap rather than a partial/misleading fix. Currently 100% unreachable by real SDK clients (harmless: also non-functional before)."}
  EnforcedGuardrailConfiguration: {status: gap, note: "path typo (\"/enforced-guardrail-configuration\" vs real \"/enforcedGuardrailsConfiguration\") AND the whole resource model differs: real API is ConfigId-keyed with a GuardrailInferenceConfig object; this implementation models it as guardrailId+guardrailVersion pairs. Deep semantic mismatch beyond a route fix; deliberately NOT touched this pass. Currently 100% unreachable by real SDK clients (harmless: also non-functional before)."}

gaps:
  - "UseCaseForModelAccess (Get/Put): wrong path, wrong method, and wrong body shape (real op takes raw bytes, not JSON). Needs a small redesign, not a route fix. (bd: file follow-up)"
  - "EnforcedGuardrailConfiguration (List/Put/Delete): wrong path and a completely different resource model than real AWS (ConfigId-keyed vs this impl's guardrailId+version pairs); DeleteEnforcedGuardrailConfiguration real shape takes a path-param configId, this impl takes a query-param guardrailId. Needs a small redesign, not a route fix. (bd: file follow-up)"
  - "UpdateMarketplaceModelEndpoint: request body (endpointConfig) is accepted but never parsed/applied — the op only bumps updatedAt. Pre-existing, not touched this pass beyond the route/method fix. (bd: file follow-up)"
  - "ListEvaluationJobs has no pagination (nextToken/maxResults) even though every sibling List op (CustomModels, ModelCustomizationJobs, etc.) supports nextToken via paginateBedrockSlice. Functionally returns MORE data than real AWS would in one page, not less — low risk, but worth aligning. (bd: file follow-up)"
  - "Most List ops (CustomModels, ModelCustomizationJobs, InferenceProfiles, MarketplaceModelEndpoints, Guardrails' non-identifier filters) only support nextToken pagination, not nameContains/statusEquals-style filters. Same shape as AWS's minimum viable page-through, but filter params are silently ignored rather than honored. ListModelInvocationJobs was the one exception fixed this pass because its backend already had full filter support sitting unused."
  - "ARP (AutomatedReasoningPolicy) family was spot-checked, not line-by-line re-verified against serializers.go this pass — scope was the 12 named priority families. Deferred to next audit."

deferred:
  - AutomatedReasoningPolicy (full wire re-verification)
  - PromptRouter
  - ImportedModel
  - FoundationModelAgreement / FoundationModelAvailability (routing looks consistent, response shapes not deeply verified)

leaks: {status: clean, note: "janitor.go's single ticker (PMT/CustomizationJob/CopyImportJob status advancers) unchanged. New GuardrailVersion snapshot fields add no new goroutines, timers, or maps — reuse the existing guardrailVersions store.Table. DeleteGuardrail's whole-guardrail path now also purges matching guardrailVersions rows (previously orphaned every numbered version on delete — a real, now-fixed state leak)."}
