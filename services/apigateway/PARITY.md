---
service: apigateway
sdk_module: aws-sdk-go-v2/service/apigateway@v1.38.6
last_audit_commit: 83adaebe
last_audit_date: 2026-07-11
overall: A            # re-audit sweep: no SDK/local drift since ce30166a; found+fixed one real gap (ApiKey.customerId) plus a misleading-wire-shape doc/test fix on UpdateUsage
ops:
  UpdateStage: {wire: ok, errors: ok, state: ok, persist: ok, note: "PATCH semantics rewritten this sweep: /variables/{name}, canary-promotion copy op, /canarySettings/*, /accessLogSettings/*, per-route method settings, cacheCluster* fields added"}
  UpdateRestApi: {wire: ok, errors: ok, state: ok, persist: ok, note: "PATCH /binaryMediaTypes/{escaped} add/remove now merges (was silently dropped); minimumCompressionSize string->int coercion fixed"}
  UpdateAccount: {wire: ok, errors: ok, state: ok, persist: ok, note: "CloudwatchRoleARN field added to UpdateAccountInput (previously unsettable at all); /throttle/{rateLimit,burstLimit} nested PATCH now merges"}
  UpdateUsagePlan: {wire: ok, errors: ok, state: ok, persist: ok, note: "/apiStages add/remove (value 'restApiId:stage') now merges; fixed InMemoryBackend.UpdateUsagePlan's len()>0 check so removing the last API stage actually applies"}
  UpdateGatewayResponse: {wire: ok, errors: ok, state: ok, persist: ok, note: "now backed by a dedicated merge-based backend method (was reusing PutGatewayResponse's full-replace, silently wiping ResponseParameters/ResponseTemplates/StatusCode on every partial PATCH); /responseParameters/{key} and /responseTemplates/{key} per-entry PATCH added"}
  UpdateApiKey: {wire: ok, errors: ok, state: ok, persist: ok, note: "top-level enabled bool now coerced from its string-typed PATCH value (see Notes #1); this sweep added the missing customerId field (create/get/patch) — see Notes"}
  UpdateUsage: {wire: ok, errors: ok, state: ok, persist: ok, note: "this sweep: verified against AWS's patch-operations.html + CLI reference that the real (and only) supported path is the single-segment scalar /remaining, NOT a per-date path as the prior code comment and test claimed; behavior was already correct (the backend loop only reads map values, not keys) but doc/test were misleading — corrected both, see Notes"}
  UpdateRequestValidator: {wire: ok, errors: ok, state: ok, persist: ok, note: "validateRequestBody/validateRequestParameters bool coercion fixed"}
  UpdateMethod: {wire: ok, errors: ok, state: ok, persist: ok, note: "apiKeyRequired bool coercion fixed"}
  UpdateAuthorizer: {wire: ok, errors: ok, state: ok, persist: ok, note: "authorizerResultTtlInSeconds int coercion fixed"}
  UpdateDeployment: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDomainName: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateBasePathMapping: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDocumentationPart: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDocumentationVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateIntegration: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateIntegrationResponse: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateMethodResponse: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateModel: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateVpcLink: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateClientCertificate: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateRestApi: {wire: ok, errors: ok, state: ok, persist: ok}
  GetRestApi: {wire: ok, errors: ok, state: ok, persist: ok}
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
  CreateStage: {wire: ok, errors: ok, state: ok, persist: ok, note: "cacheCluster{Enabled,Size,Status} fields added this sweep"}
  GetStage: {wire: ok, errors: ok, state: ok, persist: ok}
  GetStages: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteStage: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateAuthorizer: {wire: ok, errors: ok, state: ok, persist: ok, note: "TOKEN/REQUEST/COGNITO_USER_POOLS identitySource + TTL; cache bounded (bd gopherstack #1403)"}
  GetAuthorizer: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAuthorizers: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAuthorizer: {wire: ok, errors: ok, state: ok, persist: ok}
  TestInvokeAuthorizer: {wire: ok, errors: ok, state: ok, persist: n/a}
  CreateApiKey: {wire: ok, errors: ok, state: ok, persist: ok, note: "customerId (AWS Marketplace SaaS integration field, types.CreateApiKeyInput.CustomerId) added this sweep — was entirely absent from CreateAPIKeyInput/APIKey, silently dropped on create"}
  GetApiKey: {wire: ok, errors: ok, state: ok, persist: ok, note: "customerId now included in the response"}
  GetApiKeys: {wire: ok, errors: ok, state: ok, persist: ok, note: "customerId now included per item"}
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
  - "PATCH 'remove' on bare top-level SCALAR fields (e.g. /description, /policy) is still a no-op: every Update*Input's backend merge uses a zero-value-means-not-provided check (if input.X != \"\" / != nil), so an explicit remove can't be distinguished from absence without adding presence-tracking (pointer types or an explicit field mask) to every Update*Input across ~15 resources. Map/list-valued fields (variables, binaryMediaTypes, apiStages, responseParameters/Templates, methodSettings) DO support remove correctly (this sweep) because their merge goes through a full non-nil replacement value. (bd: gopherstack-0s6, follow-up)"
  - "UsagePlan per-api-stage throttle overrides via PATCH path /apiStages/{restApiId}:{stage}/throttle/{resourcePath}~1{httpMethod}/{rateLimit,burstLimit} are not implemented (only whole-apiStage add/remove via the single-segment /apiStages path is). (bd: gopherstack-0s6, follow-up)"
  - "Stage CanarySettings.StageVariableOverrides nested PATCH (/canarySettings/stageVariableOverrides/{name}) is not implemented; canarySettings/{deploymentId,percentTraffic,useStageCache} are."
  - "MethodSetting.CacheDataEncrypted and UnauthorizedCacheControlHeaderStrategy have no field on gopherstack's MethodSetting struct at all (predates this sweep), so their PATCH property paths (caching/dataEncrypted, caching/unauthorizedCacheControlHeaderStrategy) are unrecognized and fall through as no-ops."
  - "The exact property-path strings for per-route stage method settings (stageMethodSettingProperty in patch.go, e.g. \"logging/dataTrace\") are a best-effort mapping from AWS's PATCH-operations reference docs, not verified against an SDK-level enum (PatchOperation.Path is a free string in aws-sdk-go-v2 with no typed catalog to check against). Flag for correction if a live wire capture disagrees."
deferred:
  - "RestApi.ApiStatus/ApiStatusMessage/DisableExecuteApiEndpoint/EndpointAccessMode (present in aws-sdk-go-v2 types.RestApi, absent from gopherstack's RestAPI struct) — cosmetic/status-only fields, low client impact, out of scope this pass."
  - "Stage.DocumentationVersion (present in AWS's Stage type) not modeled."
  - "ApiKey.StageKeys (types.ApiKey.StageKeys / types.CreateApiKeyInput.StageKeys) not modeled, so CreateApiKey's stageKeys and UpdateApiKey's PATCH /stages add/remove are unimplemented. Checked this sweep against aws-sdk-go-v2 CreateApiKeyInput's doc comment: 'DEPRECATED FOR USAGE PLANS - Specifies stages associated with the API key. ... This parameter is deprecated and should not be used.' Low real-world impact; deferred. The PATCH /labels add/remove path from patch-operations.html has no corresponding field anywhere in aws-sdk-go-v2/service/apigateway/types.ApiKey either (likely a stale doc artifact from a pre-Tags API generation) — nothing to implement against."
leaks: {status: clean, note: "no new goroutines/tickers/persistent state introduced this sweep — patch.go is pure request-scoped transform code; authorizer cache and resource routing trie growth were already bounded by a prior sweep (bd gopherstack #1403)"}
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
