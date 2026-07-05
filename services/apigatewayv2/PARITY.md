---
service: apigatewayv2
sdk_module: aws-sdk-go-v2/service/apigatewayv2@v1.33.7
last_audit_commit: 0b1194b6
last_audit_date: 2026-07-05
overall: A            # genuine fixes found and applied this pass (see gaps/notes); most of the
                       # ~18.5k LOC surface (3 prior parity sweeps: #398/#511, #963, #1404, #1627,
                       # #2060/#2197, #2333/#2339/#2342, #2381) was already accurate op-by-op.
ops:
  CreateApi: {wire: ok, errors: ok, state: ok, persist: ok}
  GetApi: {wire: ok, errors: ok, state: ok, persist: ok}
  GetApis: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateApi: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteApi: {wire: ok, errors: ok, state: ok, persist: ok}
  ImportApi: {wire: ok, errors: ok, state: ok, persist: ok}
  ReimportApi: {wire: ok, errors: ok, state: ok, persist: ok}
  ExportApi: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateRoute: {wire: ok, errors: ok, state: ok, persist: ok, note: "HTTP routeKey format + WS \$connect/\$disconnect/\$default/custom validated; auth type NONE/AWS_IAM/JWT/CUSTOM enforced"}
  GetRoute: {wire: ok, errors: ok, state: ok, persist: ok}
  GetRoutes: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateRoute: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteRoute: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateIntegration: {wire: fixed, errors: fixed, state: ok, persist: ok, note: "was missing tlsConfig, hardcoded 29000ms ceiling/default for HTTP APIs (should be 30000ms), no connectionType default/validation -- all fixed"}
  GetIntegration: {wire: fixed, errors: ok, state: ok, persist: ok}
  GetIntegrations: {wire: fixed, errors: ok, state: ok, persist: ok}
  UpdateIntegration: {wire: fixed, errors: fixed, state: ok, persist: ok, note: "same protocol-aware timeout + connectionType validation applied"}
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
  UpdateStage: {wire: fixed, errors: ok, state: ok, persist: ok}
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
  UpdateAuthorizer: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAuthorizer: {wire: ok, errors: ok, state: ok, persist: ok}
  ResetAuthorizersCache: {wire: ok, errors: ok, state: ok, persist: n/a, note: "cache is in-memory only by design"}
  CreateModel: {wire: ok, errors: ok, state: ok, persist: ok}
  GetModel: {wire: ok, errors: ok, state: ok, persist: ok}
  GetModels: {wire: ok, errors: ok, state: ok, persist: ok}
  GetModelTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateModel: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteModel: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDomainName: {wire: fixed, errors: ok, state: ok, persist: ok, note: "was missing mutualTlsAuthentication and domainNameArn -- fixed"}
  GetDomainName: {wire: fixed, errors: ok, state: ok, persist: ok}
  GetDomainNames: {wire: fixed, errors: ok, state: ok, persist: ok}
  UpdateDomainName: {wire: fixed, errors: ok, state: ok, persist: ok}
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
  CreateRoutingRule: {wire: partial, errors: ok, state: ok, persist: ok, note: "Actions/Conditions are untyped map[string]any passthrough, not AWS union shapes -- gopherstack-e81"}
  GetRoutingRule: {wire: partial, errors: ok, state: ok, persist: ok}
  ListRoutingRules: {wire: partial, errors: ok, state: ok, persist: ok}
  PutRoutingRule: {wire: partial, errors: ok, state: ok, persist: ok}
  DeleteRoutingRule: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: fixed, errors: fixed, state: ok, persist: ok, note: "now supports stage ARNs (arn:.../apis/{id}/stages/{name}) in addition to apis/vpclinks/domainnames; 404s were surfacing as 500 for stage ARNs before the errStageNotFound check was added to the handler"}
  UntagResource: {wire: fixed, errors: fixed, state: ok, persist: ok}
  GetTags: {wire: fixed, errors: fixed, state: ok, persist: ok}
families:
  Portal/PortalProduct/ProductPage/ProductRestEndpointPage/RoutingRule (preview APIGW "portals" feature): {status: ok, note: "out of this pass's declared scope (task enumerated the classic Apis/Routes/Integrations/Stages/Deployments/Authorizers/ApiMappings/DomainNames/VpcLinks/ExportApi/models/tags surface); spot-checked, wire shapes look reasonable, not deep-audited"}
  WebSocket @connections data plane (apigatewaymanagementapi): {status: ok, note: "delegated to services/apigatewaymanagementapi via SetManagementAPIBackend; out of scope for this apigatewayv2-only sweep"}
gaps:
  - Integration ApiGatewayManaged / Stage ApiGatewayManaged not tracked for quick-create flows (bd: gopherstack-2tx)
  - authorizerCache not purged on DeleteAPI; self-heals via TTL, low severity (bd: gopherstack-wmh)
  - RoutingRule Actions/Conditions untyped passthrough instead of AWS union shapes (bd: gopherstack-e81)
deferred:
  - Portal / PortalProduct / ProductPage / ProductRestEndpointPage families (newer preview feature, not in this pass's declared op list)
  - RoutingRule typed action/condition validation (see gaps)
leaks: {status: clean, note: "portalProductSharingPolicies cleanup on DeletePortalProduct already covered by leak_internal_test.go from a prior sweep; authorizerCache is TTL-bounded (see gaps, low severity, not an unbounded leak); no goroutines/janitors in this package"}
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

Traps for the next auditor (don't re-flag):

- `arnResourceType` (single `type/id` suffix) intentionally does NOT handle Stage ARNs — Stage
  tagging goes through the separate `parseStageARN` (4-segment `apis/{id}/stages/{name}`) checked
  *before* falling through to `arnResourceType` in `TagResource`/`UntagResource`/`GetTags`. This
  is correct, not a missed generalization — Stages are the only nested (parent + child) taggable
  resource in this API.
- The hand-formatted `"arn:aws:apigateway:" + region + "::/..."` ARN construction (not
  `pkgs/arn`) is a pre-existing convention in this file (see `RoutingRuleARN`, now also
  `DomainNameArn`); left as-is for consistency rather than partially migrating one call site.
- `Portal`/`PortalProduct`/routing-rule-adjacent code is a newer, separate APIGWv2 "portals"
  preview feature; it was spot-checked but is intentionally out of this pass's declared scope
  (the task's op list is the classic HTTP/WebSocket control plane).
