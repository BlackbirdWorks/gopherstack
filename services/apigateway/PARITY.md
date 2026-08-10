---
service: apigateway
sdk_module: aws-sdk-go-v2/service/apigateway@v1.42.4
last_audit_commit: 01f7563b
last_audit_date: 2026-07-23
overall: A            # closed all 5 documented gaps + 3 deferred items from the 2026-07-11 sweep: RestApi.{ApiStatus,ApiStatusMessage,DisableExecuteApiEndpoint,EndpointAccessMode}, Stage.DocumentationVersion, ApiKey.StageKeys (Create + PATCH /stages), UsagePlan per-route throttle PATCH, Stage canarySettings.stageVariableOverrides PATCH, MethodSetting.{CacheDataEncrypted,UnauthorizedCacheControlHeaderStrategy} + their PATCH paths, and 2 concrete instances of the top-level-scalar-PATCH-remove gap (RestApi./description, Authorizer./identitySource). Found+fixed 2 new bugs while doing so (see Notes): a multi-op-per-request PATCH clobbering bug in the 3 resolvers this sweep touches, and UpdateUsagePlan returning an unprotected pointer into backend state. Found+documented (not fixed, out of assigned scope) a pre-existing UpdateDomainName PATCH gap.
# 2026-08-08 follow-up (bd: gopherstack-vvsy): fixed the multi-op-per-request clobbering bug in the remaining 6 resolvers; added applyDomainNamePatchOp so UpdateDomainName's nested "/endpointConfiguration/*" and "/mutualTlsAuthentication/*" PATCH paths no longer silently no-op; pointer-ified DomainName's certificateArn/regionalCertificateArn (a 3rd concrete PATCH-remove-on-scalar fix); re-verified UsagePlan throttle PATCH path shape against a fresh patch-operations.html fetch (already correct, no change needed). See gaps below for what's still open.
# 2026-08-09 follow-up (bd: gopherstack-npq5): added the DomainName/UsagePlan fields left missing by the prior follow-up — DomainName.{CertificateName,RegionalCertificateName,OwnershipVerificationCertificateARN} (*string on UpdateDomainNameInput, remove-supported per patch-operations.html) and .{ManagementPolicy,Policy,RoutingMode,EndpointAccessMode} (plain string, replace-only); UsagePlan.ProductCode (*string on UpdateUsagePlanInput, remove-supported). All seven flow through the existing single-segment PATCH machinery (applyTopLevelPatchOp + removableTopLevelScalar) with no new resolver code needed. Corrected the ticket: endpointConfiguration/vpcEndpointIds, which the ticket listed under DomainName, is documented only under UpdateRestApi's table, not UpdateDomainName's — left unmodeled here as a RestApi-scoped gap, out of this fix's scope. Verified against a live fetch of patch-operations.html plus aws-sdk-go-v2/service/apigateway@v1.42.4's deserializers.go (wire field names match exactly). Proven via both a pre-fix-failing unit suite and two real aws-sdk-go-v2-client integration tests (test/integration/apigateway_audit_test.go) that fail against the pre-fix binary (200 OK, field silently empty) and pass post-fix.
ops:
  UpdateStage: {wire: ok, errors: ok, state: ok, persist: ok, note: "prior sweep: PATCH semantics rewritten (/variables/{name}, canary-promotion copy op, /canarySettings/*, /accessLogSettings/*, per-route method settings, cacheCluster* fields). This sweep: documentationVersion field + PATCH added; /canarySettings/stageVariableOverrides whole-map-replace PATCH added; caching/dataEncrypted + caching/unauthorizedCacheControlHeaderStrategy per-route PATCH properties added"}
  UpdateRestApi: {wire: ok, errors: ok, state: ok, persist: ok, note: "prior sweep: PATCH /binaryMediaTypes/{escaped} add/remove merge, minimumCompressionSize coercion. This sweep: ApiStatus/ApiStatusMessage/DisableExecuteApiEndpoint/EndpointAccessMode fields added (Create + Update + PATCH replace); Description switched to *string so PATCH remove on /description actually clears it (was a silent no-op) — see Notes"}
  UpdateAccount: {wire: ok, errors: ok, state: ok, persist: ok, note: "CloudwatchRoleARN field added to UpdateAccountInput (previously unsettable at all); /throttle/{rateLimit,burstLimit} nested PATCH now merges"}
  UpdateUsagePlan: {wire: ok, errors: ok, state: ok, persist: ok, note: "prior sweep: /apiStages add/remove (value 'restApiId:stage') merge, len()>0 fix. This sweep: per-route throttle overrides added (/apiStages/{id:stage}/throttle/{resourcePath}/{httpMethod}[/rateLimit|burstLimit], remove of the whole entry at 5 segments, add/replace of one field at 6); also fixed UpdateUsagePlan returning an unprotected pointer into backend state (now returns a defensive copy like every other Update*). 2026-08-09 (gopherstack-npq5): ProductCode field added (*string, remove-supported) — was accepted-and-silently-dropped before — see Notes"}
  UpdateGatewayResponse: {wire: ok, errors: ok, state: ok, persist: ok, note: "now backed by a dedicated merge-based backend method (was reusing PutGatewayResponse's full-replace, silently wiping ResponseParameters/ResponseTemplates/StatusCode on every partial PATCH); /responseParameters/{key} and /responseTemplates/{key} per-entry PATCH added"}
  UpdateApiKey: {wire: ok, errors: ok, state: ok, persist: ok, note: "prior sweep: enabled bool coercion, customerId field. This sweep: StageKeys field + PATCH /stages add/remove added (value '{restApiId}/{stageName}', deprecated-for-usage-plans per the SDK doc comment but still real and wire-modeled) — see Notes"}
  UpdateUsage: {wire: ok, errors: ok, state: ok, persist: ok, note: "this sweep: verified against AWS's patch-operations.html + CLI reference that the real (and only) supported path is the single-segment scalar /remaining, NOT a per-date path as the prior code comment and test claimed; behavior was already correct (the backend loop only reads map values, not keys) but doc/test were misleading — corrected both, see Notes"}
  UpdateRequestValidator: {wire: ok, errors: ok, state: ok, persist: ok, note: "validateRequestBody/validateRequestParameters bool coercion fixed"}
  UpdateMethod: {wire: ok, errors: ok, state: ok, persist: ok, note: "apiKeyRequired bool coercion fixed"}
  UpdateAuthorizer: {wire: ok, errors: ok, state: ok, persist: ok, note: "prior sweep: authorizerResultTtlInSeconds int coercion. This sweep: IdentitySource switched to *string so PATCH remove on /identitySource actually clears it (was a silent no-op, AWS-documented as supported) — see Notes"}
  UpdateDeployment: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDomainName: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-08-08 follow-up: applyDomainNamePatchOp added for /endpointConfiguration/{types,ipAddressType} and /mutualTlsAuthentication/{truststoreUri,truststoreVersion} (previously entirely unhandled, silently no-opped); certificateArn/regionalCertificateArn switched to *string so PATCH remove actually clears them. 2026-08-09 (gopherstack-npq5): certificateName/regionalCertificateName/ownershipVerificationCertificateArn (*string, remove-supported) and managementPolicy/policy/routingMode/endpointAccessMode (plain string, replace-only) added — all seven were accepted-and-silently-dropped before, now fields on DomainName + UpdateDomainNameInput. endpointConfiguration/vpcEndpointIds NOT added here: verified against a fresh patch-operations.html fetch that it's a UpdateRestApi-only path, not UpdateDomainName's, contra the tracking ticket — see Notes"}
  UpdateBasePathMapping: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDocumentationPart: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDocumentationVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateIntegration: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateIntegrationResponse: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateMethodResponse: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateModel: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateVpcLink: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateClientCertificate: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateRestApi: {wire: ok, errors: ok, state: ok, persist: ok, note: "this sweep: DisableExecuteApiEndpoint/EndpointAccessMode inputs wired; ApiStatus always AVAILABLE (gopherstack creates RestApis synchronously, no UPDATING/PENDING/FAILED transition)"}
  GetRestApi: {wire: ok, errors: ok, state: ok, persist: ok, note: "this sweep: apiStatus/apiStatusMessage/disableExecuteApiEndpoint/endpointAccessMode now included in the response"}
  GetRestApis: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteRestApi: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "{proxy+} greedy + trie-based routing (bd gopherstack fix #1403), parent-child tree verified"}
  GetResource: {wire: ok, errors: ok, state: ok, persist: ok}
  GetResources: {wire: ok, errors: ok, state: ok, persist: ok, note: "embed=[methods] param honored"}
  DeleteResource: {wire: ok, errors: ok, state: ok, persist: ok}
  PutMethod: {wire: ok, errors: ok, state: ok, persist: ok, note: "authorizationType validated against NONE/AWS_IAM/CUSTOM/COGNITO_USER_POOLS; CUSTOM/COGNITO_USER_POOLS require authorizerId (400 otherwise)"}
  GetMethod: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteMethod: {wire: ok, errors: ok, state: ok, persist: ok}
  PutMethodResponse: {wire: ok, errors: ok, state: ok, persist: ok}
  GetMethodResponse: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteMethodResponse: {wire: ok, errors: ok, state: ok, persist: ok}
  PutIntegration: {wire: ok, errors: ok, state: ok, persist: ok, note: "type validated (MOCK/AWS/AWS_PROXY/HTTP/HTTP_PROXY); VTL request/response templates real (vtl.go)"}
  GetIntegration: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteIntegration: {wire: ok, errors: ok, state: ok, persist: ok}
  PutIntegrationResponse: {wire: ok, errors: ok, state: ok, persist: ok}
  GetIntegrationResponse: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteIntegrationResponse: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDeployment: {wire: ok, errors: ok, state: ok, persist: ok, note: "real snapshot of resources/methods/integrations at deploy time (apiData/apiSnapshot); inline stage create/update via stageName param"}
  GetDeployment: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDeployments: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDeployment: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateStage: {wire: ok, errors: ok, state: ok, persist: ok, note: "prior sweep: cacheCluster{Enabled,Size,Status} fields. This sweep: documentationVersion field added, wired through the stageSnapshot DTO for persistence"}
  GetStage: {wire: ok, errors: ok, state: ok, persist: ok, note: "this sweep: documentationVersion now included in the response"}
  GetStages: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteStage: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateAuthorizer: {wire: ok, errors: ok, state: ok, persist: ok, note: "TOKEN/REQUEST/COGNITO_USER_POOLS identitySource + TTL; cache bounded (bd gopherstack #1403)"}
  GetAuthorizer: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAuthorizers: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAuthorizer: {wire: ok, errors: ok, state: ok, persist: ok}
  TestInvokeAuthorizer: {wire: ok, errors: ok, state: ok, persist: n/a}
  CreateApiKey: {wire: ok, errors: ok, state: ok, persist: ok, note: "prior sweep: customerId field. This sweep: StageKeys ([]types.StageKey -> validated + formatted '{restApiId}/{stageName}' strings, referenced stage must exist or NotFoundException) added — see Notes"}
  GetApiKey: {wire: ok, errors: ok, state: ok, persist: ok, note: "customerId (prior sweep) and stageKeys (this sweep) now included in the response"}
  GetApiKeys: {wire: ok, errors: ok, state: ok, persist: ok, note: "customerId (prior sweep) and stageKeys (this sweep) now included per item"}
  DeleteApiKey: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateUsagePlan: {wire: ok, errors: ok, state: ok, persist: ok}
  GetUsagePlan: {wire: ok, errors: ok, state: ok, persist: ok}
  GetUsagePlans: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteUsagePlan: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateUsagePlanKey: {wire: ok, errors: ok, state: ok, persist: ok}
  GetUsagePlanKey: {wire: ok, errors: ok, state: ok, persist: ok}
  GetUsagePlanKeys: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteUsagePlanKey: {wire: ok, errors: ok, state: ok, persist: ok}
  GetUsage: {wire: ok, errors: ok, state: ok, persist: n/a}
  UpdateUsage: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateModel: {wire: ok, errors: ok, state: ok, persist: ok}
  GetModel: {wire: ok, errors: ok, state: ok, persist: ok}
  GetModels: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteModel: {wire: ok, errors: ok, state: ok, persist: ok}
  GetModelTemplate: {wire: ok, errors: ok, state: ok, persist: n/a}
  CreateRequestValidator: {wire: ok, errors: ok, state: ok, persist: ok}
  GetRequestValidator: {wire: ok, errors: ok, state: ok, persist: ok}
  GetRequestValidators: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteRequestValidator: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateBasePathMapping: {wire: ok, errors: ok, state: ok, persist: ok}
  GetBasePathMapping: {wire: ok, errors: ok, state: ok, persist: ok}
  GetBasePathMappings: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteBasePathMapping: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDomainName: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDomainName: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDomainNames: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDomainName: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDomainNameAccessAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDomainNameAccessAssociations: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDomainNameAccessAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  RejectDomainNameAccessAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDocumentationPart: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDocumentationPart: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDocumentationParts: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDocumentationPart: {wire: ok, errors: ok, state: ok, persist: ok}
  ImportDocumentationParts: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDocumentationVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDocumentationVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDocumentationVersions: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDocumentationVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAccount: {wire: ok, errors: ok, state: ok, persist: ok}
  GetTags: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  TestInvokeMethod: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetGatewayResponse: {wire: ok, errors: ok, state: ok, persist: ok}
  GetGatewayResponses: {wire: ok, errors: ok, state: ok, persist: ok}
  PutGatewayResponse: {wire: ok, errors: ok, state: ok, persist: ok, note: "unchanged: still a correct full replace for the real PUT operation"}
  DeleteGatewayResponse: {wire: ok, errors: ok, state: ok, persist: ok}
  GenerateClientCertificate: {wire: ok, errors: ok, state: ok, persist: ok}
  GetClientCertificate: {wire: ok, errors: ok, state: ok, persist: ok}
  GetClientCertificates: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteClientCertificate: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateVpcLink: {wire: ok, errors: ok, state: ok, persist: ok}
  GetVpcLink: {wire: ok, errors: ok, state: ok, persist: ok}
  GetVpcLinks: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteVpcLink: {wire: ok, errors: ok, state: ok, persist: ok}
  GetExport: {wire: ok, errors: ok, state: ok, persist: n/a, note: "Swagger 2.0 + OAS 3.0 export, real per-API/stage synthesis"}
  GetSdk: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetSdkType: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetSdkTypes: {wire: ok, errors: ok, state: ok, persist: n/a}
  ImportApiKeys: {wire: ok, errors: ok, state: ok, persist: ok}
  ImportRestApi: {wire: ok, errors: ok, state: ok, persist: ok}
  PutRestApi: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  proxy_invocation: {status: ok, note: "proxy.go: MOCK/AWS/AWS_PROXY/HTTP/HTTP_PROXY dispatch, VTL passthrough (WHEN_NO_MATCH/WHEN_NO_TEMPLATES/NEVER), Lambda invoke via injected LambdaInvoker, usage-plan enforcement returns real 429 (LimitExceededException/TooManyRequestsException) via writeThrottleResponse — separate, already-correct code path from the control-plane handleError switch"}
  authorizers_runtime: {status: ok, note: "TOKEN/REQUEST/COGNITO_USER_POOLS resolution + JWKS validation via injected JWKSProvider, TTL-bounded cache (bd gopherstack #1403 fixed prior unbounded growth)"}
  patch_semantics: {status: ok, note: "REWRITTEN this sweep — see Notes; was the single biggest gap in the service"}
gaps:
  - "PATCH 'remove' on bare top-level SCALAR fields is a no-op EXCEPT for the instances now fixed (RestApi./description, Authorizer./identitySource, DomainName./certificateArn + /regionalCertificateArn + /certificateName + /regionalCertificateName + /ownershipVerificationCertificateArn, and UsagePlan./productCode — all via *string Update*Input fields, verified against patch-operations.html as remove-supported paths on their resources' Update tables). Every OTHER Update*Input still uses a zero-value-means-not-provided check, so explicit remove still can't be distinguished from absence there. Audited against patch-operations.html's full per-resource table: almost every other top-level scalar (UpdateApiKey's customerId/description/enabled/name, UpdateAccount's cloudwatchRoleArn, UpdateStage's description/cacheCluster*/tracingEnabled/clientCertificateId, UpdateUsagePlan's name/description, UpdateModel's description/schema, UpdateRequestValidator's fields, UpdateResource's fields, UpdateVpcLink's fields, etc.) is documented replace-only with remove NOT supported, so no fix is needed there. Map/list-valued fields (variables, binaryMediaTypes, apiStages, responseParameters/Templates, methodSettings, stageKeys) support remove correctly because their merge goes through a full non-nil replacement value. (bd: gopherstack-vvsy, gopherstack-npq5)"
  - "FIXED (bd: gopherstack-vvsy): the multi-op-per-request PATCH clobbering bug (resource-specific resolvers re-deriving their starting map/struct from CURRENT BACKEND STATE instead of checking out[field] for what an earlier op in the SAME request already staged) is now fixed in all six resolvers that had it (applyStageVariablePatch, applyStageCanaryPatch, applyStageAccessLogPatch, applyRestAPIPatchOp's binaryMediaTypes case, applyAccountPatchOp, applyGatewayResponsePatchOp), via the same stagedValue[T] helper the prior sweep introduced. Verified by Test_ApplyStructuredPatch_MultiOpSameRequest (stage variables/canary/accessLog) plus Test_ApplyStructuredPatch_{RestAPIBinaryMediaTypesMultiOp,AccountThrottleMultiOp,GatewayResponseMultiOp} — each drives a real two-op PATCH request through the handler and asserts both ops land; each test was confirmed to fail against the pre-fix code (git-stashing patch.go alone) before the fix landed."
  - "FIXED (bd: gopherstack-vvsy): applyResourcePatchOp now has a case for opUpdateDomainName (applyDomainNamePatchOp), handling the nested paths \"/endpointConfiguration/types\" (add/remove), \"/endpointConfiguration/ipAddressType\" (replace), and \"/mutualTlsAuthentication/{truststoreUri,truststoreVersion}\" (add/replace/remove) — DomainName.MutualTLSAuthentication and EndpointConfiguration.IPAddressType are new fields added this follow-up. Verified by Test_ApplyStructuredPatch_DomainNameNestedPaths (four ops across two nested fields in one request, plus a remove). FIXED (bd: gopherstack-npq5, 2026-08-09): the remaining top-level scalars patch-operations.html's UpdateDomainName table documents — /certificateName, /regionalCertificateName, /ownershipVerificationCertificateArn (remove-supported, now *string on UpdateDomainNameInput) and /managementPolicy, /policy, /routingMode, /endpointAccessMode (replace-only, plain string) — are now DomainName/UpdateDomainNameInput fields; all seven route through the existing applyTopLevelPatchOp fallback, no new resolver needed. Verified against a live patch-operations.html fetch and aws-sdk-go-v2/service/apigateway@v1.42.4's deserializers.go for wire field names, and proven via real aws-sdk-go-v2 client integration tests (TestIntegration_APIGatewayAudit_UpdateDomainNameDocumentedFields). One item the tracking ticket listed under DomainName was found to actually belong to UpdateRestApi instead: /endpointConfiguration/vpcEndpointIds appears only in patch-operations.html's UpdateRestApi table, not UpdateDomainName's — left unmodeled here (UpdateRestApi doesn't handle nested endpointConfiguration paths at all yet; a RestApi-scoped gap, out of this fix's scope)."
  - "The exact property-path strings for per-route stage method settings (stageMethodSettingProperty in patch.go, e.g. \"logging/dataTrace\", \"caching/dataEncrypted\", \"caching/unauthorizedCacheControlHeaderStrategy\") were fetched and verified this sweep directly against the live AWS documentation page https://docs.aws.amazon.com/apigateway/latest/api/patch-operations.html (UpdateStage table) — every string in the map matches exactly, including the two new caching/* entries added this sweep. Still not backed by an SDK-level typed enum (PatchOperation.Path is a free string in aws-sdk-go-v2), so this remains a doc-fetch verification rather than a compile-time guarantee; re-verify if AWS changes the doc."
  - "UsagePlan.apiStages per-route throttle PATCH path shape RE-VERIFIED this follow-up (bd: gopherstack-vvsy) directly against a fresh fetch of patch-operations.html's UpdateUsagePlan table: \"/apiStages/{apidId:stageName}/throttle/{resourcePath}/{httpMethod}\" (remove-only) and the same path + \"/rateLimit\" or \"/burstLimit\" (add/replace) — gopherstack's usagePlanThrottlePathMinSegs=5 segmentation and the \"restApiId:stage\" colon-separated composite key both match AWS's own path notation exactly (AWS's doc literally uses the \"{apidId:stageName}\" spelling as the path placeholder). No wire capture example exists for the exact /apiStages add/remove Value string format, but the colon convention is corroborated by the path notation itself. No code change was needed — the existing implementation already matches."
deferred:
  - "ApiKey.StageKeys's PATCH /labels add/remove path (listed in patch-operations.html's UpdateApiKey table) still has no corresponding field anywhere in aws-sdk-go-v2/service/apigateway/types.ApiKey (re-verified this sweep) — likely a stale doc artifact from a pre-Tags API generation. Nothing to implement against; distinct from /stages, which this sweep DID implement (see Notes)."
leaks: {status: clean, note: "no new goroutines/tickers/persistent state introduced this sweep — all new code (StageKeyInput resolution, patch.go's new resolvers/stagedValue helper) is request-scoped and synchronous under the existing coarse b.mu; UpdateUsagePlan's missing defensive copy (return p instead of a copy, found while extending it for per-route throttle) was also fixed, closing a latent aliasing hole where a caller mutating the returned *UsagePlan would have corrupted backend state directly"}
---

## Notes

This sweep's finding was concentrated in ONE architectural gap rather than spread
across many small op bugs: **every single Update*/PATCH operation in this
service shared one flatten function (`handler.go`'s old `normalizePatchBody` /
`flattenPatchOps`) that could only express "replace one top-level field."**
Two concrete, previously-silent bugs fell out of that:

1. **PatchOperation.Value is always a JSON *string* on the wire**, even for
   bool/int targets — confirmed by reading aws-sdk-go-v2's actual serializer
   (`awsRestjson1_serializeDocumentPatchOperation` calls `ok.String(*v.Value)`
   unconditionally). So a real client PATCHing e.g.
   `{"op":"replace","path":"/tracingEnabled","value":"true"}` was handing this
   service the raw bytes `"true"` (a JSON string) to unmarshal directly into
   `UpdateStageInput.TracingEnabled *bool` — a JSON type-mismatch that made the
   whole PATCH request error out. This affected every non-string top-level
   PATCH field across the service: `tracingEnabled`, `cacheClusterEnabled`,
   `minimumCompressionSize`, API key `enabled`, `validateRequestBody`,
   `validateRequestParameters`, `apiKeyRequired`, `authorizerResultTtlInSeconds`.
   Fixed via `patch.go`'s `patchFieldKind` table + `coerceTopLevelPatchValue`.

2. **Multi-segment PATCH paths were silently dropped.** The old flatten took
   the *entire* remaining path after the leading `/` as one bogus flat field
   name (e.g. `/variables/apiKey` became the map key literally
   `"variables/apiKey"`), which matches no `Update*Input` json tag, so the
   edit vanished with no error. This meant the single most common real-world
   API Gateway PATCH usage — **setting one stage variable** — never worked at
   all, alongside per-route method settings, binary-media-type membership,
   usage-plan API-stage membership, gateway-response parameter/template
   entries, and canary-deployment promotion (which AWS models as a `"copy"`
   op — a verb the old flatten didn't implement at all, since it only handled
   `"add"`/`"replace"`, and silently skipped `"remove"` too).

Rewritten in `patch.go` (new file) with per-resource resolvers that read
current backend state and merge the touched entry, since the Update* backend
methods replace a map/struct field wholesale when it's provided — see the
file's package doc for the full design rationale and the exact PATCH path
shapes each resolver targets (stage variables, canary promotion, per-route
method settings, REST API binary media types, account throttle, usage-plan
API stages, gateway-response parameters/templates).

**Independent bug found and fixed while auditing UpdateGatewayResponse**: its
action handler reused `PutGatewayResponse` (a correct full-replace for the
real PUT operation) for the PATCH operation too. Since PUT semantics
unconditionally overwrite `StatusCode`/`ResponseParameters`/`ResponseTemplates`
with whatever the (now-partial) flattened PATCH body happened to contain, ANY
partial PATCH — even a plain single-field `/statusCode` replace — silently
wiped whichever of the other two fields wasn't part of that particular PATCH
call. Added a dedicated `InMemoryBackend.UpdateGatewayResponse` that merges
field-by-field (falling back to AWS's implicit per-responseType default when
no custom response has been PUT yet), and wired the Update action to it.

**Independent bug found and fixed in `InMemoryBackend.UpdateUsagePlan`**: it
only applied `input.APIStages` when `len(input.APIStages) > 0`, so a PATCH
that removes the last remaining API stage (producing a correctly-empty, but
non-nil, slice) was silently ignored. Changed the guard to `!= nil`.

**Stage cache-cluster fields were entirely missing.** AWS's `Stage` (and
`CreateStage`/`UpdateStage` inputs) carry `CacheClusterEnabled`,
`CacheClusterSize`, and a derived `CacheClusterStatus`
(`AVAILABLE`/`NOT_AVAILABLE`) — none existed on gopherstack's `Stage` struct.
Added all three, wired through `CreateStage`/`UpdateStage`.

**`UpdateAccountInput` was missing `CloudwatchRoleARN` entirely** — the single
most common real-world reason to call `UpdateAccount` (wiring a CloudWatch
Logs role for API Gateway execution logging) had no way to reach the backend
at all. Added the field and its backend wiring.

### PATCH-op semantics traps (for the next auditor)

- **`PatchOperation.Value` is always a JSON string on the wire**, regardless
  of the target field's real type. Never copy it into a flattened body
  verbatim unless the target field actually is a Go `string`.
- **A PATCH `path` is a JSON Pointer**: `~1` decodes to `/`, `~0` decodes to
  `~`, and the `~1` substitution must happen *before* `~0` (so `~01` decodes
  to `~1`, not to `/`). Get the order backwards and escaped values silently
  corrupt.
- **Per-route stage method-settings paths have NO `"methodSettings"` path
  segment.** They're addressed directly as
  `/{resourcePath}/{httpMethod}/{category}/{property}`, where `resourcePath`
  is itself JSON-Pointer-escaped (its own internal `/` becomes `~1`) or the
  literal wildcard `*`. A path segment that starts with `~1` or is exactly
  `*` is the tell that you're looking at a method-settings patch, not a
  plain field name — every genuine top-level Stage field name is a bare
  identifier and never starts with `~` or equals `*`.
- **`UsagePlan.apiStages` PATCH uses a single-segment path** (`/apiStages`)
  with the API stage identified entirely by `value` — the string
  `"{restApiId}:{stage}"` — not by a nested path segment. Don't assume every
  list-membership PATCH nests the identifying key into the path; this one
  doesn't.
- **`"copy"` is a real, AWS-documented op** (not just `add`/`replace`/`remove`)
  used for canary-deployment promotion:
  `{"op":"copy","from":"/canarySettings/deploymentId","path":"/deploymentId"}`.
  Its `from` value must be resolved against the resource's *current* stored
  state, not against the request body (there's no `from` value in the patch
  document itself to read it from).
- **PUT vs PATCH on the same resource can have different replace semantics.**
  `PutGatewayResponse` (real PUT) is correctly a full replace. Reusing it
  verbatim for the PATCH operation on the same resource is a bug — PATCH must
  merge only the touched fields with current state. Watch for this pattern
  (`opUpdateX` action calling the same backend function as `opPutX`)
  elsewhere in this codebase; it wasn't audited beyond GatewayResponse this
  sweep.
- **`len(slice) > 0` is the wrong presence check for "was this field provided
  in the patch."** It's indistinguishable from "provided but now empty" and
  silently drops the empty-result case (found in `UpdateUsagePlan.APIStages`).
  Use `!= nil` for slice/map fields that a merge might legitimately want to
  empty out.

Protocol confirmed: REST-JSON (`restjson1`) — HTTP verb + path routing per
resource, JSON request/response bodies, `application/x-amz-json-1.1` on
errors, epoch-seconds timestamps (`unixEpochTime`/`pkgs/awstime`-equivalent
inline type) — not a single json-1.0/1.1 RPC target the way most other
services in this codebase are.

## 2026-07-11 re-audit sweep

No local drift since ce30166a (`git diff` over `services/apigateway/` between
the two commits is empty) and no SDK version bump (`aws-sdk-go-v2/service/apigateway`
still pinned at v1.38.6), so this sweep audited only the ledger's documented
`gaps` plus a general due-diligence pass rather than re-verifying every `ok`
row from scratch.

**Real bug found and fixed: `ApiKey.CustomerId` was entirely absent.**
aws-sdk-go-v2's `types.ApiKey`/`types.CreateApiKeyInput`/`UpdateApiKeyInput`
(via `PatchOperations`) all carry `CustomerId` (an AWS Marketplace SaaS
integration identifier) — confirmed by reading the vendored SDK's
`types/types.go` and `api_op_CreateApiKey.go`. gopherstack's `APIKey`,
`CreateAPIKeyInput`, and `UpdateAPIKeyInput` structs had no such field at all,
so a real client's `customerId` was silently dropped on create, never
returned by Get/GetApiKeys, and unpatchable (AWS's `patch-operations.html`
lists `/customerId` as a `replace`-supported UpdateApiKey path). Added the
field to all three structs and wired it through
`InMemoryBackend.CreateAPIKey`/`UpdateAPIKey`; no `patch.go` change was needed
since `/customerId` is a single-segment scalar path that the existing generic
top-level PATCH fallback already handles correctly for string fields. Covered
by `TestBatch2Ops_ApiKey_CustomerID` (create/get/patch round-trip).
`apiKeys` is a "clean" (non-DTO) persisted table, so the new field persists
automatically.

**Almost-bug, verified false via WebFetch against AWS's live docs — logged so
the next auditor doesn't repeat the investigation.** `UpdateUsage`'s PATCH
routing looked suspicious: `applyResourcePatchOp` (patch.go) has no case for
`opUpdateUsage`, so any *multi-segment* PATCH path falls through to
`applyTopLevelPatchOp`, which explicitly no-ops any path containing `/` after
the leading slash. Every other resource with a real-world multi-segment PATCH
path (stage variables, per-route method settings, usage-plan API stages, ...)
got an explicit resolver in a prior sweep, and `UpdateUsage`'s doc comment
*said* its patch paths were per-date (`"date -> new remaining quota"`),
which would have been multi-segment (`/{date}/{usageIndex}`) and thus
silently broken. Fetched AWS's `patch-operations.html` UpdateUsage table
*and* the `aws apigateway update-usage` CLI reference example directly:
both agree the one and only supported path is the single-segment scalar
`/remaining` — there is no per-date path at all. Under that real shape the
existing code was already correct (the backend's merge loop only reads the
flattened map's *values*, never its keys, so the misleading "date" key name
never mattered). Fixed the stale/misleading doc comment on
`InMemoryBackend.UpdateUsage` and the test in `handler_destub_test.go` that
was asserting against the wrong (`/2024-01-01`) path shape, and strengthened
that test to assert the actual overridden `remaining` value instead of just
key presence.

**Checked but deferred**: `ApiKey.StageKeys` (`/stages` add/remove) is
present in the SDK but explicitly marked deprecated in
`CreateApiKeyInput.StageKeys`'s doc comment ("should not be used"); left
unimplemented (see `deferred`). The `/labels` PATCH path AWS's
patch-operations.html lists for UpdateApiKey has no corresponding field
anywhere in the current SDK's `types.ApiKey` — likely stale documentation
with nothing to implement against.

No other rows changed. Gates: `go build`/`go vet`/`go test -race`/`go fix
-diff`/`golangci-lint run`, all scoped to `./services/apigateway/...`, pass
clean both before and after this sweep's edits.

## 2026-07-23 sweep

Closed all 5 documented `gaps` and all 3 `deferred` items from the 2026-07-11
sweep. Field-diffed every new field/path against the vendored
`aws-sdk-go-v2/service/apigateway@v1.38.6` types (`types.go`,
`api_op_*.go`, `serializers.go`) and, for PATCH path shapes, against a live
fetch of https://docs.aws.amazon.com/apigateway/latest/api/patch-operations.html
(the previous sweep's `gaps` entry #5 flagged the method-settings property
strings as unverified against a typed enum — no such enum exists in the SDK,
so this sweep instead fetched the actual doc table and confirmed every
existing string plus the two added this sweep match exactly).

**Deferred item 1 (RestApi cosmetic fields) — all four fields are real,
confirmed in `types.go`**: `ApiStatus` (enum `UPDATING`/`AVAILABLE`/`PENDING`/
`FAILED`), `ApiStatusMessage`, `DisableExecuteApiEndpoint`,
`EndpointAccessMode` (enum `BASIC`/`STRICT`). All four are present in
`CreateRestApiInput` too (not create-then-immutable), confirmed by reading
`api_op_CreateRestApi.go`. `ApiStatus` is AWS-managed/read-only (no PATCH path
in patch-operations.html); gopherstack sets it to `AVAILABLE` unconditionally
on create since RestApi creation here is always synchronous with no
UPDATING/PENDING/FAILED transition to model. `DisableExecuteApiEndpoint` and
`EndpointAccessMode` are both real PATCH paths (`patch-operations.html`'s
UpdateRestApi table: both replace-only, no add/remove) — wired through
`patchFieldKind`'s bool-coercion table for the former (a wire string like
`"true"` must coerce to a JSON bool before hitting the `*bool` field).

**Deferred item 2 (Stage.DocumentationVersion) — real field, real PATCH
path**: added to `Stage`, `CreateStageInput`, `UpdateStageInput`, and (since
Stage is a DTO'd table, unlike RestApi/ApiKey/UsagePlan's "clean" tables) the
`stageSnapshot` DTO in `persistence.go` — a field added to `Stage` alone
without the DTO update would silently NOT persist across Snapshot/Restore,
the exact bug class `pkgs-catalog.md`'s "clean/dirty table split" note warns
about.

**Deferred item 3 (ApiKey.StageKeys) — implemented, contrary to the prior
sweep's decision to leave it out.** Re-read the SDK doc comment: it says
"DEPRECATED FOR USAGE PLANS ... should not be used" as *guidance*, not a
removal — the field is still fully present and functional in
`CreateApiKeyInput.StageKeys` (`[]types.StageKey`, object form: `restApiId`/
`stageName`), `CreateApiKeyOutput.StageKeys`/`GetApiKeyOutput.StageKeys`/
`UpdateApiKeyOutput.StageKeys` (`[]string`, confirmed serialized as
`{restApiId}/{stageName}` by reading `awsRestjson1_serializeDocumentStageKey`
in `serializers.go`), and `UpdateApiKey`'s `/stages` PATCH path (add/remove,
`patch-operations.html`'s UpdateApiKey table). AWS deprecating a field in
favor of a newer mechanism (usage plans) doesn't make the field non-functional
or out of scope for parity — a real client can still call it and expects a
real response, so implementing it is the correct call under this campaign's
no-stub rule. `CreateApiKey` validates each referenced REST API + stage
exists (`NotFoundException` otherwise, mirroring `CreateUsagePlanKey`'s
existing FK-validation pattern) and formats survivors as
`{restApiId}/{stageName}` via the new `formatAPIKeyStageKey` helper. Also
re-confirmed the prior sweep's finding that `/labels` (a second ApiKey PATCH
path `patch-operations.html` lists) has no corresponding field anywhere in
`types.ApiKey` — left in `deferred` since there's genuinely nothing to wire
it to.

**Gap "PATCH remove on top-level scalars" — narrowed, not closed.** The
architectural fix (pointer-ify every Update*Input field) is out of this
sweep's budget across all ~15 resources, but two concrete instances are now
real: `UpdateRestAPIInput.Description` and `UpdateAuthorizerInput.IdentitySource`
both became `*string`, and their handler-level wire structs
(`updateRestAPIHandlerInput` embeds `UpdateRestAPIInput` directly;
`updateAuthorizerInput` in `handler_authorizers.go` is a separate
hand-written struct that had to be migrated too, since it doesn't embed the
backend input type) plus `patch.go`'s new `removableTopLevelScalar` table
(gating exactly which action+field pairs get an explicit `""` write on
`remove`, vs. every other field which still silently no-ops) make `remove`
on `/description` (RestApi) and `/identitySource` (Authorizer) actually work
end to end. Verified both are genuinely the only top-level-scalar
`op:remove`-supported paths on their respective resources per
`patch-operations.html` (every other remove-supported path on every other
resource's table is either already map/list-handled, or — for
UpdateDomainName — entirely unhandled for an unrelated reason; see the new
`gaps` entry on that).

**Two bugs found (not assigned, found while extending adjacent code) and
fixed:**

1. `InMemoryBackend.UpdateUsagePlan` returned `p, nil` — a pointer straight
   into the backend's own stored `*UsagePlan`, not a defensive copy, unlike
   every other `Update*` method in this service (`cp := *x; return &cp`). A
   caller mutating the returned value would have corrupted backend state
   without going through the lock. Found while extending this method's PATCH
   coverage for per-route throttle; fixed to match the established pattern.
2. Multiple PATCH ops in one request targeting the *same* merged field
   (discovered via `Test_ApplyStructuredPatch_UsagePlanPerRouteThrottle`,
   which legitimately sets both `rateLimit` and `burstLimit` for one route in
   a single request — a very plausible real-client pattern) clobbered each
   other: `applyStageMethodSettingPatch` (and, before this sweep's fix, the
   two new UsagePlan/ApiKey resolvers) each independently re-derived their
   starting map from **current backend state**, ignorant of what an earlier
   op in the same request had already staged into `out`. The last op's
   `setJSONValue(out, field, ...)` call wins, discarding earlier ones. Added
   `stagedValue[T]` (a small generic helper) and wired it into the three
   resolvers this sweep's new code touches
   (`applyStageMethodSettingPatch`, `applyUsagePlanAPIStageMembershipPatch`
   + `applyUsagePlanThrottlePatch` via the new `currentUsagePlanAPIStages`
   helper, `applyAPIKeyPatchOp`). The same bug pattern exists, unfixed, in
   six pre-existing resolvers this sweep didn't need to touch
   (`applyStageVariablePatch`, `applyStageCanaryPatch`,
   `applyStageAccessLogPatch`, `applyRestAPIPatchOp`'s binaryMediaTypes case,
   `applyAccountPatchOp`, `applyGatewayResponsePatchOp`) — every existing
   test for those only ever sends one op per request per field, so the bug
   was never exercised. Logged as a `gaps` entry rather than silently fixed
   everywhere, since a blanket fix across 6 more call sites was judged
   outside this sweep's assigned scope (5 gaps + 3 deferred, all now
   addressed) and deserves its own focused verification pass.

**New gap found (not fixed, out of scope): `UpdateDomainName`'s PATCH
semantics.** `applyResourcePatchOp`'s switch (`patch.go`) has no case for
`opUpdateDomainName`, so every nested/multi-segment DomainName PATCH path
(`/mutualTlsAuthentication/truststoreUri`, `/certificateName`,
`/endpointConfiguration/types/{type}`, etc. — all real, per
`patch-operations.html`'s UpdateDomainName table, which has more distinct
paths than any other resource in this service) falls through to
`applyTopLevelPatchOp`, which no-ops anything containing `/` after the
leading slash. This predates this sweep and was not one of the 5 assigned
gaps/3 deferred items, so left unfixed here — flagging for a dedicated
`domain_names` PATCH-semantics sweep, since it looks like a comparably-sized
gap to the one this whole PATCH rewrite effort (see the 2026-07-\* sweeps
above) already closed for every other resource.

Gates: `go build`, `go vet`, `go test -race -count=1`, `gofmt -l` (clean),
`golangci-lint run` (0 issues), and a grep for banned
`cyclop`/`gocyclo`/`gocognit`/`funlen` nolints (empty) — all scoped to
`./services/apigateway/...` — pass clean after this sweep's edits. One
cyclop violation surfaced mid-sweep (`applyStageCanaryPatch` hit 17 after
adding the `stageVariableOverrides` case, max 15) and was resolved by
extracting the per-property switch into `applyStageCanaryProp`, not a
nolint.
