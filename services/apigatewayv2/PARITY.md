---
service: apigatewayv2
sdk_module: aws-sdk-go-v2/service/apigatewayv2@v1.37.4
last_audit_commit: e50f52dce
last_audit_date: 2026-08-28
overall: A            # write-only-state sweep pass (this pass, 2026-08-28). Existing
                       # wire_field_fixes_test.go (ListRoutingRules wrapper key, Portal
                       # PublishStatus) was a PARTIAL prior pass, not a finished one -- per this
                       # campaign's protocol, treated as a signal to dig deeper rather than skip.
                       # Ran the write-only-state method (what does each backend persist, what real
                       # op reads it back) across the Api/Stage/Route/Integration/Authorizer/
                       # Deployment/DomainName/VpcLink/RoutingRule families. Found one real bug:
                       # UpdateAuthorizer's AuthorizerResultTtlInSeconds/EnableSimpleResponses were
                       # plain int32/bool with a truthy/nonzero guard (not *int32/*bool like the
                       # real SDK), so a client's documented way to explicitly disable caching
                       # (TTL=0) or simple responses (false) was silently dropped -- fixed, see
                       # UpdateAuthorizer row and Notes. enumcheck: 0 findings in this service.
                       # apigatewayv2 is REST-shaped (path-bound members via echo routes in
                       # handler.go, e.g. /v2/apis/{apiId}/authorizers/{authorizerId}), confirmed
                       # against the vendored SDK's httpBindingEncoder-based serializers.go/
                       # api_op_*.go for the ops this pass touched. Did not re-verify every op in
                       # this large service (24k lines) -- see gaps for scope not reached.
                       # ---- query/header-to-non-string-field sweep (this pass, 2026-08-29) ----
                       # Hunted for query/header values fed into a non-string Go field without
                       # conversion (the apigateway-v1 Limit-into-JSON-body class). No merging
                       # pattern here (nothing merges query values into the JSON body) and no
                       # hard-fail found. Inventoried every non-string query/header/path member
                       # across all 103 ops: MaxResults is *string on every Get*/List sibling
                       # except ListRoutingRules (*int32, serializers.go:6988) -- all correctly
                       # parsed via apigwPaginationParams/strconv. Found and fixed two inert
                       # (SILENT) params: ExportApi's IncludeExtensions (*bool) and
                       # ListRoutingRules' MaxResults/NextToken were declared but never read. See
                       # ExportApi/ListRoutingRules rows.
                       # ---- prior pass's note follows ----
                       # gopherstack-0xs7 follow-up pass. Verified against live code (not
                       # PARITY.md prose) that gopherstack-e81/2tx/jni0 were all still genuinely
                       # open, then closed the real parts of each: RoutingRule Actions/Conditions
                       # are now typed unions (gopherstack-e81, see Notes #12); UpdateRoute now
                       # blocks route-key changes and UpdateStage blocks all changes on
                       # quick-create-managed resources (gopherstack-2tx, partial -- see gaps);
                       # ImportApi/ReimportApi now read+validate basepath/failOnWarnings and
                       # implement basepath=prepend (gopherstack-jni0, partial -- see gaps). Also
                       # swept for and fixed three "state mutated before validation" bugs
                       # (UpdateRoute, UpdateAPI, UpdateDomainName -- see Notes #13) and two
                       # under-validated RoutingRule inputs (priority range, action/condition
                       # required sub-fields and API/stage FK existence). Portal/PortalProduct/
                       # ProductPage/ProductRestEndpointPage family re-counted against botocore:
                       # 26 operations (31 including RoutingRule's 5), all 26 already implemented
                       # with real backend state (not stubs) -- the family is NOT the large
                       # unmodelled surface a prior pass's note speculated it might be.
                       # ---- prior pass's note follows ----
                       # re-audit pass (parity-3 campaign). The previously recorded
                       # last_audit_commit (d6fae6df) was a ledger bug, not a valid baseline: that
                       # commit's own message is "parity(apigateway): ..." and its diffstat touches
                       # only services/apigateway (the v1 REST API service), never
                       # services/apigatewayv2 -- it was almost certainly pasted from the wrong
                       # session. The real predecessor commit (the one that last wrote this file)
                       # is efc42cbc4 ("Parity 4"), confirmed via `git log -- services/apigatewayv2/
                       # PARITY.md`; corrected here. Diffing efc42cbc4..HEAD showed zero local drift
                       # to apigatewayv2/*.go (the two intervening commits touching this repo were a
                       # docs/gendocs rewrite and a pure-reorg refactor of *other* services), same
                       # pinned SDK version. Independent field-diff of the in-scope surface (Apis,
                       # Stages, Routes, Integrations (+responses), RouteResponses, Authorizers,
                       # Deployments, DomainNames (+ApiMappings), VpcLinks, Models, ExportApi, Tags)
                       # against aws-sdk-go-v2/service/apigatewayv2@v1.33.7/types/types.go and the
                       # per-op api_op_*.go input/output structs turned up five more genuinely
                       # missing wire fields the prior pass's field-diff missed (Integration.
                       # CredentialsArn, Api.IpAddressType, CreateApi/UpdateApi's quick-create-only
                       # CredentialsArn, Api.ImportInfo/Warnings, DomainName.RoutingMode) plus a real
                       # fix for the previously-deferred authorizerCache leak (bd gopherstack-wmh,
                       # now closed) and a newly-found ImportApi/ReimportApi query-param gap (bd
                       # gopherstack-jni0, deferred -- see gaps). All fixed for real except the
                       # newly-filed gap. RoutingRule wire:partial rows, the quick-create
                       # immutability gap (gopherstack-2tx), and the Portal/PortalProduct family
                       # (out of this pass's declared scope, per the task's op list) were
                       # re-confirmed as still accurate/deliberately out of scope, not re-touched.
ops:
  CreateApi: {wire: fixed, errors: fixed, state: ok, persist: ok, note: "routeKey+target quick-create shortcut was entirely unimplemented -- CreateAPIInput had no such fields at all, so real quick-create requests silently created a bare API with no route/integration/stage (fixed by a prior pass, see Notes #6). This pass: ipAddressType and quick-create's credentialsArn were ALSO entirely absent from CreateAPIInput -- fixed, see Notes #8-9."}
  GetApi: {wire: fixed, errors: ok, state: ok, persist: ok, note: "Api.ipAddressType/importInfo/warnings were entirely absent -- fixed, see Notes #8"}
  GetApis: {wire: fixed, errors: ok, state: ok, persist: ok, note: "same Api shape fix as GetApi"}
  UpdateApi: {wire: fixed, errors: fixed, state: fixed, persist: ok, note: "routeKey/target (\"part of quick create\" per SDK doc comments) were also entirely absent from UpdateAPIInput (fixed by a prior pass, see Notes #6). This pass: ipAddressType and quick-create's credentialsArn were ALSO entirely absent from UpdateAPIInput -- fixed, see Notes #8-9. Also: was mutating Name/Description/etc. before validating ipAddressType and the quick-create routeKey/target/credentialsArn fields, so a rejected update could leave those partially applied -- fixed, see Notes #13."}
  DeleteApi: {wire: ok, errors: ok, state: ok, persist: ok, note: "now also purges authorizerCache entries for the API's authorizers on cascade delete -- see Notes #11"}
  ImportApi: {wire: fixed, errors: fixed, state: ok, persist: ok, note: "basepath and failOnWarnings query params (SetQuery in serializers.go, not body fields) are now read and validated instead of silently ignored; basepath=prepend now prefixes route paths with the spec's declared base path. basepath=split and failOnWarnings-triggered rollback remain unimplemented -- bd gopherstack-jni0, narrowed, see gaps. Api.importInfo/warnings shape itself is correct (Notes #8) but always empty since the emulator never generates import warnings, so failOnWarnings has no observable effect yet."}
  ReimportApi: {wire: fixed, errors: fixed, state: ok, persist: ok, note: "same basepath/failOnWarnings fix as ImportApi -- bd gopherstack-jni0, narrowed"}
  ExportApi: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed (gopherstack-h910): OutputType (required query param 'outputType', verified against validateOpExportApiInput/serializeOpHttpBindingsExportApiInput) was ignored and JSON was always returned. Now required (400 if missing/invalid) and YAML actually serializes via gopkg.in/yaml.v3 when requested. Also fixed (query/header wrapper-key sweep, this pass): IncludeExtensions (real *bool query param, api_op_ExportApi.go:52, serializers.go:3975) was never read, so AWS extension keys (x-amazon-apigateway-authtype and friends) were always emitted; now defaults true (AWS's documented default) and false strips them recursively. StageName/ExportVersion remain unwired -- StageName would need per-stage route filtering this backend's route model doesn't support (routes are API-level, not stage-scoped); ExportVersion is a cosmetic knob on the exported doc's own metadata, not state this backend tracks. Left absent rather than fabricated."}
  CreateRoute: {wire: ok, errors: ok, state: ok, persist: ok, note: "HTTP routeKey format + WS $connect/$disconnect/$default/custom validated; auth type NONE/AWS_IAM/JWT/CUSTOM enforced"}
  GetRoute: {wire: ok, errors: ok, state: ok, persist: ok}
  GetRoutes: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateRoute: {wire: ok, errors: fixed, state: fixed, persist: ok, note: "was mutating RouteKey before validating AuthorizationType, so a rejected update (bad auth type) could still leave a changed route key -- fixed by validating the whole input before mutating anything, see Notes #13. Also now rejects a route-key change on a quick-create $default route (gopherstack-2tx, see Notes #14)."}
  DeleteRoute: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateIntegration: {wire: fixed, errors: fixed, state: ok, persist: ok, note: "was missing tlsConfig, hardcoded 29000ms ceiling/default for HTTP APIs (should be 30000ms), no connectionType default/validation (fixed by a prior pass). This pass: credentialsArn was ALSO entirely absent -- fixed, see Notes #7."}
  GetIntegration: {wire: fixed, errors: ok, state: ok, persist: ok, note: "credentialsArn fix, see Notes #7"}
  GetIntegrations: {wire: fixed, errors: ok, state: ok, persist: ok, note: "same Integration shape fix as GetIntegration"}
  UpdateIntegration: {wire: fixed, errors: fixed, state: ok, persist: ok, note: "same protocol-aware timeout + connectionType validation applied (prior pass); credentialsArn fixed this pass, see Notes #7"}
  DeleteIntegration: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateIntegrationResponse: {wire: ok, errors: ok, state: ok, persist: ok}
  GetIntegrationResponse: {wire: ok, errors: ok, state: ok, persist: ok}
  GetIntegrationResponses: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateIntegrationResponse: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteIntegrationResponse: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateRouteResponse: {wire: ok, errors: ok, state: ok, persist: ok}
  GetRouteResponse: {wire: ok, errors: ok, state: ok, persist: ok}
  GetRouteResponses: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateRouteResponse: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteRouteResponse: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateStage: {wire: fixed, errors: ok, state: ok, persist: ok, note: "was missing clientCertificateId (WS-only) and Tags -- fixed"}
  GetStage: {wire: fixed, errors: ok, state: ok, persist: ok}
  GetStages: {wire: fixed, errors: ok, state: ok, persist: ok}
  UpdateStage: {wire: fixed, errors: fixed, state: ok, persist: ok, note: "now rejects any modification of a quick-create $default stage (gopherstack-2tx, see Notes #14)"}
  DeleteStage: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAccessLogSettings: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteRouteSettings: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteRouteRequestParameter: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteCorsConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDeployment: {wire: ok, errors: ok, state: ok, persist: ok, note: "autoDeploy interaction verified"}
  GetDeployment: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDeployments: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDeployment: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDeployment: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateAuthorizer: {wire: ok, errors: ok, state: ok, persist: ok, note: "JWT issuer/audience + REQUEST identitySource/payloadFormatVersion/enableSimpleResponses/TTL all modeled and enforced on the data plane (http_proxy.go, authorizer.go)"}
  GetAuthorizer: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAuthorizers: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAuthorizer: {wire: fixed, errors: fixed, state: fixed, persist: ok, note: "write-only-state bug (gopherstack-wire-sweep, this pass): AuthorizerResultTtlInSeconds/EnableSimpleResponses were plain int32/bool (not *int32/*bool like the real SDK's UpdateAuthorizerInput, api_op_UpdateAuthorizer.go) with a truthy/nonzero guard, so a real client's documented way to disable caching (TTL=0) or simple responses (false) via Update was silently dropped, leaving the previous value forever. The Authorizer response shape also carried omitempty on both fields, which would have hidden a real 0/false value as an absent key on GetAuthorizer/ListAuthorizers -- also fixed. Round-trip test in wire_field_fixes_test.go. Follow-up sweep (this pass, wrapper-key sweep): the same != \"\" guard bug also affected the other four string fields of UpdateAuthorizerInput. Fixed three (AuthorizerURI, AuthorizerCredentialsArn, AuthorizerPayloadFormatVersion): none is required at CreateAuthorizer time (unlike Name), so a client explicitly clearing one -- e.g. dropping AuthorizerCredentialsArn to switch to resource-based Lambda permissions, per its own doc ('don't specify this parameter') -- is a legitimate state, not an error; converted to *string with a nil check. Response side (Authorizer.AuthorizerURI/AuthorizerCredentialsArn/AuthorizerPayloadFormatVersion, models.go) intentionally kept omitempty, unlike TTL/EnableSimpleResponses above -- these three are commonly N/A altogether (e.g. a JWT authorizer never sets AuthorizerURI at all), and stripping omitempty would put spurious empty keys on the common case rather than only the rare explicit-clear case. Left Name unfixed as a silent-ignore: unlike the other three, Name IS required at CreateAuthorizer ('This member is required'), so no authorizer has a valid empty-Name state -- converted to *string too, but an explicit empty value is now rejected with a BadRequestException (fixed handleUpdate's generic error mapping in handler.go, which had never routed ErrBadRequest to 400 for any Update op, to make this correct) instead of either silently ignored or silently applied. Round-trip tests: wire_field_fixes_test.go (TestUpdateAuthorizer_URICredentialsAndPayloadVersionCanBeCleared, TestUpdateAuthorizer_EmptyNameRejected)."}
  DeleteAuthorizer: {wire: ok, errors: ok, state: ok, persist: ok, note: "now purges authorizerCache entries for this authorizer -- see Notes #11 (bd gopherstack-wmh, closed)"}
  ResetAuthorizersCache: {wire: ok, errors: ok, state: ok, persist: n/a, note: "cache is in-memory only by design"}
  CreateModel: {wire: ok, errors: ok, state: ok, persist: ok}
  GetModel: {wire: ok, errors: ok, state: ok, persist: ok}
  GetModels: {wire: ok, errors: ok, state: ok, persist: ok}
  GetModelTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateModel: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteModel: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDomainName: {wire: fixed, errors: ok, state: ok, persist: ok, note: "was missing mutualTlsAuthentication and domainNameArn (fixed by a prior pass). This pass: routingMode was ALSO entirely absent -- fixed, see Notes #10."}
  GetDomainName: {wire: fixed, errors: ok, state: ok, persist: ok, note: "routingMode fix, see Notes #10"}
  GetDomainNames: {wire: fixed, errors: ok, state: ok, persist: ok, note: "same DomainName shape fix as GetDomainName"}
  UpdateDomainName: {wire: fixed, errors: fixed, state: fixed, persist: ok, note: "routingMode fix, see Notes #10. This pass: was also mutating Tags/DomainNameConfigurations/MutualTLSAuthentication before validating RoutingMode, so a rejected update could leave those partially applied -- fixed, see Notes #13."}
  DeleteDomainName: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateApiMapping: {wire: ok, errors: ok, state: ok, persist: ok}
  GetApiMapping: {wire: ok, errors: ok, state: ok, persist: ok}
  GetApiMappings: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateApiMapping: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteApiMapping: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateVpcLink: {wire: ok, errors: ok, state: ok, persist: ok}
  GetVpcLink: {wire: ok, errors: ok, state: ok, persist: ok}
  GetVpcLinks: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateVpcLink: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteVpcLink: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateRoutingRule: {wire: fixed, errors: fixed, state: ok, persist: ok, note: "Actions/Conditions are now typed AWS union shapes (RoutingRuleAction/RoutingRuleActionInvokeAPI, RoutingRuleCondition/RoutingRuleMatchBasePaths/RoutingRuleMatchHeaders/RoutingRuleMatchHeaderValue) instead of []map[string]any passthrough, with required-subfield and FK (target api/stage must exist) validation, plus RoutingRulePriority's modeled [1,1000000] range -- gopherstack-e81, closed, see Notes #12."}
  GetRoutingRule: {wire: fixed, errors: ok, state: ok, persist: ok, note: "same typed-shape fix as CreateRoutingRule"}
  ListRoutingRules: {wire: fixed, errors: ok, state: ok, persist: ok, note: "same typed-shape fix as CreateRoutingRule. Also fixed (query/header wrapper-key sweep, this pass): MaxResults/NextToken (real *int32/*string query params, api_op_ListRoutingRules.go:40-45, serializers.go:6988 -- the one List op in this service where MaxResults is int32, unlike every Get*/List sibling's *string MaxResults) were never read at all, so every rule always came back in one page regardless of the limit a client asked for. Now paginates via the shared apigwPaginationParams/page.New path like every other List/Get collection op."}
  PutRoutingRule: {wire: fixed, errors: fixed, state: ok, persist: ok, note: "same typed-shape + validation fix as CreateRoutingRule"}
  DeleteRoutingRule: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: fixed, errors: fixed, state: ok, persist: ok, note: "now supports stage ARNs (arn:.../apis/{id}/stages/{name}) in addition to apis/vpclinks/domainnames; 404s were surfacing as 500 for stage ARNs before the errStageNotFound check was added to the handler"}
  UntagResource: {wire: fixed, errors: fixed, state: ok, persist: ok}
  GetTags: {wire: fixed, errors: fixed, state: ok, persist: ok}
families:
  Portal/PortalProduct/ProductPage/ProductRestEndpointPage (preview APIGW "portals" feature): {status: ok, note: "gopherstack-0xs7 pass counted the family against botocore apigatewayv2/2018-11-29: 26 operations (CreatePortal/GetPortal/ListPortals/UpdatePortal/DeletePortal/PreviewPortal/PublishPortal/DisablePortal, the same 5 for PortalProduct, Create/List/Get/Update/Delete for ProductPage and ProductRestEndpointPage, Get/Put/DeletePortalProductSharingPolicy). All 26 are implemented with real backend state in portals.go/handler_portals.go (confirmed via GetSupportedOperations() and backend method presence) -- NOT a large unmodelled surface as a prior pass's note speculated. PreviewPortal returns the live Portal (a reasonable preview simulation, not a stub). 2026-08-23 (manifest harvest): did the field-level wire audit this note deferred, against aws-sdk-go-v2/service/apigatewayv2@v1.37.4's api_op_{Create,Update,Get}Portal.go/types.PortalSummary. Found and fixed 3 real accept-and-drop bugs on the Portal type: CreatePortalInput/UpdatePortalInput.IncludedPortalProductArns (a *required* PortalSummary member) and .RumAppMonitorName were decoded off the wire into nothing (no backing field existed) and silently dropped on both Create and Update; PublishPortalInput.Description ('When the portal is published, this description becomes the last published description' -- api_op_PublishPortal.go) was decoded but never used, and GetPortalOutput.LastPublished/LastPublishedDescription had no backing field at all. Added Portal.IncludedPortalProductArns/RumAppMonitorName/LastPublished/LastPublishedDescription (models.go), wired through CreatePortal/UpdatePortal/handlePublishPortal (portals.go/handler_portals.go). GetPortalOutput.Preview/StatusException remain correctly unmodeled -- see gaps. UpdatePortalInput is ALSO missing Authorization/EndpointConfiguration/PortalContent entirely (all three real, optional UpdatePortalInput members -- api_op_UpdatePortal.go); NOT fixed this pass, newly disclosed as a gap (see below) rather than rushed alongside the three accept-and-drop fixes. FIXED (constraint sweep, this pass): ListPortals/ListPortalProducts/ListProductPages/ListProductRestEndpointPages all declare real maxResults/nextToken query params (query-bound, confirmed via each op's own httpBindings serializer) but the handlers called the backend with no pagination args at all -- every item always came back on one page. Wired through apigwPaginationParams/page.New, the same pattern GetApis etc. already use. ListPortalProducts/ListProductPages/ListProductRestEndpointPages' ResourceOwner/ResourceOwnerAccountId query params remain unfiltered: PortalProduct/ProductPage/ProductRestEndpointPage carry no ownership-account field to filter on, so honoring them would mean inventing a model field -- left as a disclosed gap, not fixed."}
  WebSocket @connections data plane (apigatewaymanagementapi): {status: ok, note: "delegated to services/apigatewaymanagementapi via SetManagementAPIBackend; out of scope for this apigatewayv2-only sweep"}
gaps:
  - "Quick-create route/stage immutability partially enforced (gopherstack-2tx, narrowed): UpdateRoute
    now rejects a route-key change on an apiGatewayManaged route (\"You can't modify the $default
    route key\") and UpdateStage now rejects any modification of an apiGatewayManaged stage (\"You
    can't modify the $default stage\") -- both backed by BadRequestException, which IS in
    UpdateRoute/UpdateStage's modeled error set (service-2.json). Still NOT enforced:
    DeleteRoute/DeleteStage/DeleteIntegration on a managed resource. Deliberately not extended
    there: those three operations' error sets in service-2.json list only NotFoundException/
    TooManyRequestsException, no BadRequestException or ConflictException, so there is no
    wire-verifiable error code to reject with -- guessing one would violate the wire-verification
    principle the same way UpdateRoute/UpdateStage's prior deferral (re-confirmed open, then
    narrowed this pass) originally cited."
  - "ImportApi/ReimportApi's basepath query param now supports \"prepend\" (prefixes route paths
    with the spec's declared base path -- Swagger 2 basePath or OpenAPI 3 servers[0].url's path).
    \"split\" is not implemented (falls back to ignore-like behavior): API Gateway's split
    semantics (part of the base path becomes an ApiMapping key, part stays in routes) aren't
    described by the SDK wire model, only by prose docs, so implementing it would mean guessing
    at unverified behavior. failOnWarnings is now read and validated (boolean) but has no
    observable effect: the emulator's OpenAPI import (parseOpenAPISpec/applyOpenAPIToAPI) never
    generates import warnings for any spec it accepts (see Notes #8), so there is never a warning
    for failOnWarnings to escalate into an error. Not fabricating warning-generation heuristics to
    manufacture an effect -- see the existing trap note on API.ImportInfo/Warnings below. bd:
    gopherstack-jni0, narrowed to these two residual items."
deferred:
  - "2026-08-23 (manifest harvest): UpdatePortal's real UpdatePortalInput (aws-sdk-go-v2/service/apigatewayv2@v1.37.4's api_op_UpdatePortal.go) has optional Authorization/EndpointConfiguration/PortalContent members letting a caller replace a portal's auth config, domain/cert config, or displayed content post-creation -- gopherstack's UpdatePortalInput (models.go) has no fields for any of the three, so a real client sending them gets no error but no effect either. All three are already-modeled types (used by CreatePortal) and Create's existing validateCreatePortal{Authorization,EndpointConfiguration,Content} helpers look reusable for a nil-check-and-replace Update path; not implemented this pass to keep the fix scoped to the three accept-and-drop bugs found and closed alongside this note (IncludedPortalProductArns/RumAppMonitorName/LastPublished(Description), see the family's ops-table note) -- newly disclosed, not previously known."
  - PortalProduct / ProductPage / ProductRestEndpointPage field-level wire audit still not re-verified field-by-field against botocore (only Portal itself got a field-level audit this pass -- see the family's ops-table note)
  - ImportApi/ReimportApi basepath=split; failOnWarnings real effect (see gaps, bd gopherstack-jni0)
  - Quick-create DeleteRoute/DeleteStage/DeleteIntegration rejection (see gaps, bd gopherstack-2tx)
leaks: {status: clean, note: "portalProductSharingPolicies cleanup on DeletePortalProduct already covered by leak_internal_test.go from a prior sweep; authorizerCache entries are now purged on DeleteAuthorizer/DeleteApi (bd gopherstack-wmh, fixed and closed this pass -- see Notes #11), not merely TTL-bounded; no goroutines/janitors in this package"}
---

## Notes

Protocol = REST-JSON (`awsRestjson1` in the SDK's serializers/deserializers, confirmed by
reading `aws-sdk-go-v2/service/apigatewayv2@v1.33.7/serializers.go`). Timestamps use
`__timestampIso8601` (RFC3339 string), not epoch seconds — see `models.go` `isoTime` — this is
correct for apigatewayv2 and should NOT be "fixed" to `awstime.Epoch()`; that would be a
regression (epoch-seconds is a `json-1.0`/query-XML pattern from unrelated services, not REST-JSON
timestampIso8601).

Genuine bugs found and fixed this pass (all confirmed against `aws-sdk-go-v2/service/apigatewayv2@v1.33.7/types/types.go`):

1. **Integration.TlsConfig was entirely absent.** Real `Integration`/`CreateIntegrationInput`/
   `UpdateIntegrationInput` carry a `tlsConfig{serverNameToVerify}` for private (VPC_LINK)
   integrations. Added `IntegrationTLSConfig` and wired it through Create/Update with a
   deep-copy helper (`cloneIntegrationTLSConfig`) so backend state can't alias caller memory.

2. **Integration timeout ceiling/default was NOT protocol-aware.** The backend hardcoded
   29,000ms (`integrationTimeoutMax`) as both the validation ceiling and the zero-value default
   for every API, regardless of protocol. Real AWS: HTTP APIs allow up to 30,000ms (default
   30,000ms), WebSocket APIs allow up to 29,000ms (default 29,000ms) — see the SDK's doc comment
   on `Integration.TimeoutInMillis`. Before this fix, HTTP API integrations with an unset timeout
   were under-defaulted to 29000 instead of 30000, and a valid HTTP API timeout of e.g. 29500ms
   was wrongly rejected as `BadRequestException`. Fixed via `integrationTimeoutMaxFor(protocolType)`
   threaded through `CreateIntegration`/`UpdateIntegration`/`validateTimeoutInMillis`.

3. **Integration.ConnectionType had no default and no validation.** Real AWS defaults
   `connectionType` to `INTERNET` when unset and validates the enum (`INTERNET`|`VPC_LINK`),
   requiring `connectionId` when `VPC_LINK` is specified. Before this fix, `GetIntegration`
   on an integration created without an explicit connectionType returned `""` instead of
   `"INTERNET"`, and a `VPC_LINK` integration with no `connectionId` silently succeeded.

4. **Stage.ClientCertificateID (WebSocket-only) and Stage.Tags were entirely absent.** Real
   `Stage` carries `clientCertificateId` and its own `tags` map (a Stage is independently
   taggable via `arn:aws:apigateway:{region}::/apis/{apiId}/stages/{stageName}`, confirmed by
   the `Tags` field on the SDK's `Stage` type). Added both fields, wired `clientCertificateId`
   through Create/UpdateStage, and extended `TagResource`/`UntagResource`/`GetTags` to resolve
   the nested stage ARN shape (`parseStageARN` + `lookupStageLocked`) since stages — unlike
   APIs/VPC links/domain names — need two path segments (apiId + stageName), not one, to
   resolve. The tag handlers in `handler.go` were also missing an `ErrStageNotFound` → 404
   mapping, so a not-found stage tag lookup would have surfaced as a 500 instead of 404 (the
   exact bug class called out in `parity-principles.md` #2).

5. **DomainName.MutualTlsAuthentication and DomainName.DomainNameArn were entirely absent.**
   Real `DomainName` carries `mutualTlsAuthentication{truststoreUri,truststoreVersion,
   truststoreWarnings}` and `domainNameArn`. Added both, wired through Create/UpdateDomainName
   (`domainNameArn` is computed once at creation and stable across updates, matching AWS
   behavior since it is not settable). `truststoreWarnings` is always empty because the
   emulator has no S3 truststore object to validate — this matches the "no warnings" case for
   a well-formed request; it does not represent unvalidated input.

6. **CreateApi's `routeKey`+`target` "quick create" shortcut was entirely unimplemented** (this
   pass's re-audit, commit range ce30166a..d6fae6df — the ledger's prior gap description
   ("Integration ApiGatewayManaged / Stage ApiGatewayManaged not tracked", bd gopherstack-2tx)
   understated the actual severity: the SDK's `CreateApiInput` carries `RouteKey *string` and
   `Target *string` ("The route target must always be prefixed with `integrations/`..."; "For
   Lambda integrations, specify a function ARN. The type of the integration will be HTTP_PROXY
   or AWS_PROXY, respectively"), but gopherstack's `CreateAPIInput` had no such fields at all —
   `encoding/json` silently dropped them on decode, so a real quick-create request (e.g. `aws
   apigatewayv2 create-api --target ...`, one of the most common ways to stand up an HTTP API)
   succeeded but produced an API with *no* route, integration, or stage whatsoever. Fixed:
   - Added `RouteKey`/`Target` to `CreateAPIInput` (wire keys `routeKey`/`target`, confirmed
     against `serializers.go`).
   - Added `ApiGatewayManaged` → **`APIGatewayManaged`** (Go naming, `revive` var-naming) bool
     field, wire key `apiGatewayManaged`, to `Route`, `Stage`, and `Integration` (confirmed
     present on all four AWS types — `API`, `Integration`, `Route`, `Stage` — in `types.go`;
     `API.ApiGatewayManaged` was deliberately NOT added since nothing in this emulator ever
     marks an *API* itself managed — that flag covers a different mechanism, CloudFormation/SAM
     tooling-created APIs, not quick create).
   - `CreateAPI` now validates routeKey/target (HTTP-only, both-or-neither, valid HTTP route key
     format) and, when both are set, calls new `quickCreateLocked` to auto-provision: an
     integration (`HTTP_PROXY` for a URL target, `AWS_PROXY` for a Lambda ARN target, detected
     by `isLambdaFunctionARN`), a `$default` route targeting `integrations/{id}`, and an
     auto-deployed `$default` stage — all three marked `apiGatewayManaged: true`. Extracted
     `CreateIntegration`'s validation/defaulting body into `buildIntegration` so quick-create
     reuses the exact same AWS-realistic defaults (payload format version, passthrough
     behavior, connection type, protocol-aware timeout ceiling) instead of a second,
     drift-prone copy.
   - `UpdateApi` also carries `RouteKey`/`Target` in the real SDK ("This property is part of
     quick create... you can update a quick-created target, but you can't remove it from an
     API"), also entirely absent from `UpdateAPIInput`. Fixed the same way: each field
     independently updates the API's existing managed route/integration (found via
     `APIGatewayManaged`, since AWS doesn't expose an explicit back-reference and there is at
     most one of each per quick-created API), and returns `ErrBadRequest` if the API has no
     managed route/integration to update (rather than silently no-opping).
   - Added `APIGatewayManaged` to the `stageSnapshot`/`routeSnapshot`/`integrationSnapshot`
     persistence DTOs (`persistence.go`) — additive field, snapshot version not bumped (see the
     version-bump criterion in that file's doc comment: only breaking shape changes need a
     bump).
   - Deliberately NOT implemented this pass (see `gaps`): enforcing that a managed
     route/stage/integration actually rejects mutation/deletion the way real AWS does. The SDK
     doc comments describe the restriction in prose but the exact error code/HTTP status isn't
     derivable from `serializers.go`/`deserializers.go` (it's server-side validation, not part
     of the wire format), and guessing at it would be the same "fabricated behavior" failure
     mode `parity-principles.md` warns against for stubs.

7. **Integration.CredentialsArn was entirely absent.** Real `Integration`/`CreateIntegrationInput`/
   `UpdateIntegrationInput` all carry `credentialsArn` (confirmed at `types.go:614` and in
   `api_op_CreateIntegration.go`/`api_op_UpdateIntegration.go`/`api_op_GetIntegration.go`) --
   the IAM role ARN (or `arn:aws:iam::*:user/*` passthrough sentinel) API Gateway assumes to
   invoke an `AWS`/`AWS_PROXY` integration's backend. `encoding/json` silently dropped it on
   decode; `GetIntegration` always returned `""` regardless of what a caller sent. Added the
   field to `Integration`/`CreateIntegrationInput`/`UpdateIntegrationInput`, wired it through
   `buildIntegration` (so `CreateIntegration` and `CreateApi`'s quick-create path share it) and
   `applyIntegrationUpdate`, and added it to the `integrationSnapshot` persistence DTO.

8. **Api.IpAddressType, Api.ImportInfo, and Api.Warnings were entirely absent.** Real `Api`
   carries `ipAddressType` (`ipv4`|`dualstack`, confirmed in `types.go:104` and
   `CreateApiInput`/`UpdateApiInput`/`ImportApiOutput`/`ReimportApiOutput`), `importInfo`
   (validation feedback from `ImportApi`/`ReimportApi` about ignored OpenAPI properties), and
   `warnings` (warning messages when `failOnWarnings` is set). `ipAddressType` was silently
   dropped on `CreateApi`/`UpdateApi` decode and `GetApi` always returned `""` instead of AWS's
   default (`"ipv4"`). Added all three fields to `API`, `ipAddressType` to
   `CreateAPIInput`/`UpdateAPIInput` with default-to-`ipv4` + enum validation
   (`validateIPAddressType`). `ImportInfo`/`Warnings` are always empty: `API` is a "clean"
   (non-DTO) persisted table so no `persistence.go` change was needed, and the emulator's
   `parseOpenAPISpec`/`applyOpenAPIToAPI` never generates import warnings, which correctly
   represents the "well-formed input" response case (same precedent as `TruststoreWarnings` in
   Notes #5) rather than a stub -- see gaps for the related `basepath`/`failOnWarnings`
   query-param gap this uncovered (gopherstack-jni0).

9. **CreateApiInput's and UpdateApiInput's quick-create-only CredentialsArn were entirely
   absent.** Real `CreateApiInput`/`UpdateApiInput` both carry `credentialsArn` ("part of quick
   create... specifies the credentials required for the integration"), independent of the
   `routeKey`/`target` fields a prior pass already fixed (Notes #6). Added `CredentialsArn` to
   both inputs; `CreateAPI`'s quick-create path (`quickCreateLocked`) now threads it into the
   auto-provisioned integration's `CredentialsArn`, and `UpdateAPI` independently replaces the
   managed integration's credentials via the new `applyQuickCreateCredentialsUpdateLocked`
   (mirroring `applyQuickCreateUpdateLocked`'s existing routeKey/target handling, including the
   same `ErrBadRequest` when the API has no quick-create integration to update).

10. **DomainName.RoutingMode was entirely absent.** Real `DomainName`/`CreateDomainNameInput`/
    `UpdateDomainNameInput` all carry `routingMode` (`API_MAPPING_ONLY`|`ROUTING_RULE_ONLY`|
    `ROUTING_RULE_THEN_API_MAPPING`, confirmed in `types.go:297-304` and the two input structs).
    Added the field with default-to-`API_MAPPING_ONLY` + enum validation
    (`validateRoutingMode`). The `ROUTING_RULE_*` modes only take semantic effect together with
    RoutingRule resources on the domain name, which are explicitly out of this pass's scope
    (RoutingRule's typed-union gap is tracked separately, gopherstack-e81) -- this fix is wire
    completeness (store/return the field correctly) only, not RoutingRule enforcement.

11. **authorizerCache entries were never purged on delete (bd gopherstack-wmh, now fixed and
    closed).** `authorizerCache` (`authorizers.go`) caches REQUEST-authorizer allow/deny
    decisions keyed by `authorizerId + "\n" + identity-source-values`, but neither
    `DeleteAuthorizer` nor `DeleteApi`'s cascade delete purged entries for the deleted
    authorizer(s) -- they only self-healed via TTL expiry or lazy eviction on `get`. Added
    `authorizerCache.purge(authorizerID)` (prefix-matches and deletes every cached entry for
    that authorizer, across all identity-source values) and wired it into
    `handleDeleteAuthorizer` (purge the one authorizer) and `handleDeleteAPI` (snapshot the
    API's authorizer IDs via `GetAuthorizers` before the cascade delete removes them, then purge
    each afterward). This was a leak-adjacent correctness gap, not a wire bug -- a stale cached
    `allow` decision could keep authorizing requests against a route for up to
    `authorizerResultTtlInSeconds` (max 3600s) after the authorizer or its API was deleted.

Genuine bugs found and fixed in the `gopherstack-0xs7` follow-up pass (confirmed against
`aws-sdk-go-v2/service/apigatewayv2@v1.37.4/types/types.go` and
`botocore/data/apigatewayv2/2018-11-29/service-2.json.gz`):

12. **`RoutingRule.Actions`/`Conditions` were untyped `[]map[string]any` instead of AWS's
    modeled union shapes (bd gopherstack-e81, closed).** Sized first per this session's appmesh
    precedent: the real shapes are only 6 small structs at max depth 3 (`RoutingRuleAction` ->
    `RoutingRuleActionInvokeApi{ApiId,Stage,StripBasePath}`; `RoutingRuleCondition` ->
    `RoutingRuleMatchBasePaths{AnyOf}` and/or `RoutingRuleMatchHeaders{AnyOf
    []RoutingRuleMatchHeaderValue{Header,ValueGlob}}`) -- shallow enough to model properly rather
    than leave opaque. Added the 6 types plus `validateRoutingRuleActions`/
    `validateRoutingRuleConditions` (required-subfield checks per `types.go:1280-1353`'s
    `// This member is required` doc comments) and `validateRoutingRulePriority` (the modeled
    `RoutingRulePriority` range, min 1 max 1,000,000, `service-2.json` shape `RoutingRulePriority`
    -- previously unvalidated entirely). Also added `validateRoutingRuleActionTargetsLocked`: each
    action's `InvokeApi.ApiId`/`Stage` must reference an API/stage that actually exists (previously
    any string succeeded, an "operation accepting an ID for a resource that does not exist and
    reporting success" bug). `CreateRoutingRule`/`PutRoutingRule` validate before mutating/writing
    (`PutRoutingRule` previously mutated the existing rule's Priority/Actions/Conditions with zero
    validation). `routingRuleSnapshot`'s persistence DTO field types were updated to match; no
    snapshot version bump (JSON field names unchanged, only the Go type of two existing fields).

13. **Three `Update*` backends mutated fields before validating the whole input, so a rejected
    request could still leave earlier fields in the same call changed.** The session's most
    recurrent bug class. `UpdateRoute` set `r.RouteKey` before validating `AuthorizationType`, so
    e.g. `{routeKey: "POST /x", authorizationType: "BOGUS"}` returned `BadRequestException` but
    left the route key changed. `UpdateAPI` mutated `Name`/`Description`/etc. before validating
    `IPAddressType` and the quick-create `routeKey`/`target`/`credentialsArn` fields (which
    themselves validate against the API's existing managed route/integration). `UpdateDomainName`
    mutated `Tags`/`DomainNameConfigurations`/`MutualTLSAuthentication` before validating
    `RoutingMode`. Fixed by splitting each into a pure-validation pass (no mutation) that runs
    first, then a mutation pass that runs only once every field validates --
    `validateRouteKeyUpdate`/`validateRouteAuthUpdate`/`applyRouteUpdate` (routes.go),
    `validateQuickCreateUpdateLocked`/`applyQuickCreateUpdateMutateLocked` (apis.go, replacing the
    old `applyQuickCreateUpdateLocked`/`applyQuickCreateCredentialsUpdateLocked` which validated
    and mutated in the same pass), and reordering `UpdateDomainName`'s `RoutingMode` check ahead of
    its other field mutations.

14. **Quick-create managed-resource immutability, narrowed (bd gopherstack-2tx).** Real AWS:
    "You can't modify the $default route key" (`Route.ApiGatewayManaged` doc) and "You can't
    modify the $default stage" (`Stage.ApiGatewayManaged` doc). `UpdateRoute` now rejects a
    route-key change when `r.APIGatewayManaged` (other fields on a managed route remain
    updatable, matching the doc's route-*key*-specific wording); `UpdateStage` now rejects any
    modification of a managed stage (matching the doc's unqualified "can't modify"). Both return
    `BadRequestException`, which is in `UpdateRoute`/`UpdateStage`'s modeled error set
    (`service-2.json`). `DeleteRoute`/`DeleteStage`/`DeleteIntegration` remain unenforced: their
    modeled error sets contain only `NotFoundException`/`TooManyRequestsException`, no error code
    that fits "rejected because managed" -- guessing one would be the same fabrication risk this
    gap's original deferral (2026-07-05) correctly flagged.

15. **Route reachability (bd gopherstack-l5ir).** Every one of the 103 real apigatewayv2 ops was
    extracted from `apigatewayv2@v1.37.4` serializers.go (`request.Method` +
    `httpbinding.SplitURI(...)` in each op's `awsRestjson1_serializeOp<Op>.HandleSerialize`) and
    diffed against this service's route table. Zero mismatches -- all 103 method+path pairs
    resolve to the correct op via `ExtractOperation`, including the shared-path/method-only
    disambiguation used by `GetTags`/`TagResource`/`UntagResource` (all `/v2/tags/{ResourceArn}`)
    and `PublishPortal`/`DisablePortal` (both `/v2/portals/{id}/publish`, POST vs DELETE) -- unlike
    cloudfront's `TagResource`/`UntagResource` bug (both `POST /tagging` distinguished only by an
    `Operation=` query param the router ignored), apigatewayv2's tag ops are genuinely
    method-disambiguated in the real SDK, so switching on method here is correct, not a latent bug.
    No op in this service is distinguished by a query parameter or bare flag. Added as a permanent
    test, `TestExtractOperation_SDKRouteTable` in `handler_paths_sdk_diff_test.go` (one subtest per
    op), rather than left as a one-off audit.

Traps for the next auditor (don't re-flag):

- `arnResourceType` (single `type/id` suffix) intentionally does NOT handle Stage ARNs — Stage
  tagging goes through the separate `parseStageARN` (4-segment `apis/{id}/stages/{name}`) checked
  *before* falling through to `arnResourceType` in `TagResource`/`UntagResource`/`GetTags`. This
  is correct, not a missed generalization — Stages are the only nested (parent + child) taggable
  resource in this API.
- The hand-formatted `"arn:aws:apigateway:" + region + "::/..."` ARN construction (not
  `pkgs/arn`) is a pre-existing convention in this file (see `RoutingRuleARN`, now also
  `DomainNameArn`); left as-is for consistency rather than partially migrating one call site.
- `Portal`/`PortalProduct` preview-feature code was spot-checked in the parity-3 pass and
  confirmed fully operation-complete (26/26 ops, see `families`) in the `gopherstack-0xs7`
  follow-up; field-level wire-shape depth still not audited — don't assume "26/26 present" means
  "every field on those 26 is correct."
- `RoutingRule` `Actions`/`Conditions` are typed as of `gopherstack-0xs7` (Notes #12) — do not
  revert to `[]map[string]any` "for round-trip fidelity." That reasoning was evaluated and
  superseded: `RoutingRuleAction`/`RoutingRuleActionInvokeApi`/`RoutingRuleCondition`/
  `RoutingRuleMatchBasePaths`/`RoutingRuleMatchHeaders`/`RoutingRuleMatchHeaderValue` are only
  6 small structs at a max depth of 3 (confirmed by reading `types.go:1259-1353`,
  `aws-sdk-go-v2/service/apigatewayv2@v1.37.4`) — well inside "shallow enough to model properly,"
  not the "genuinely deep nested union" case where opaque passthrough is the right call.
- `quickCreateLocked`'s auto-created `$default` stage name reuses the existing `routeKeyDefault`
  constant (`proxy.go`) for its literal value rather than a new stage-specific constant — same
  string (`"$default"`), and introducing a second constant with the identical value would have
  tripped `goconst`. The name is a slight misnomer when read at the stage call site; this is
  intentional, not an oversight.
- `API.ImportInfo`/`API.Warnings` (Notes #8) always marshal as omitted/empty. This is NOT an
  unfinished stub to "complete" by inventing warning-generation heuristics — real AWS only
  populates them when its (unspecified, business-logic) OpenAPI validation actually finds
  something to flag, and the emulator's `parseOpenAPISpec` tolerates any input it's given, so
  "nothing to flag" is the correct, non-fabricated response for every import this emulator can
  currently perform. Do not add speculative warning text.
- If a future auditor's ledger baseline (`last_audit_commit`) is not an ancestor of the current
  branch, don't assume it was merely rebased/squashed — check whether the commit even touches
  `services/apigatewayv2/` at all (`git show --stat <hash>`). This pass's recorded baseline
  (`d6fae6df`) belonged entirely to the sibling `services/apigateway` (v1 REST API) service; the
  real baseline was recovered via `git log -- services/apigatewayv2/PARITY.md`.
