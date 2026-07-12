---
service: apigatewayv2
sdk_module: aws-sdk-go-v2/service/apigatewayv2@v1.33.7
last_audit_commit: d6fae6df
last_audit_date: 2026-07-11
overall: A            # re-audit pass: zero local drift since the prior sweep (ce30166a, the
                       # commit that actually authored this ledger -- the previously recorded
                       # last_audit_commit 0b1194b6 was not an ancestor of this branch, so per
                       # protocol ce30166a was used as the diff baseline instead), same pinned SDK
                       # version, so the changed/new-surface scan was empty. Auditing the ledger's
                       # non-ok CreateApi/UpdateApi rows (they were marked "ok" but the CreateApi
                       # quick-create shortcut turned out to be entirely unimplemented -- see
                       # notes) turned up one real, previously-missed bug class; fixed it. The
                       # RoutingRule wire:partial rows and the two low-severity gaps were
                       # re-confirmed as still accurate/deliberately deferred, not re-touched.
ops:
  CreateApi: {wire: fixed, errors: fixed, state: ok, persist: ok, note: "routeKey+target quick-create shortcut was entirely unimplemented -- CreateAPIInput had no such fields at all, so real quick-create requests silently created a bare API with no route/integration/stage. Fixed: see Notes."}
  GetApi: {wire: ok, errors: ok, state: ok, persist: ok}
  GetApis: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateApi: {wire: fixed, errors: fixed, state: ok, persist: ok, note: "routeKey/target (\"part of quick create\" per SDK doc comments) were also entirely absent from UpdateAPIInput; fixed alongside CreateApi -- see Notes."}
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
  - authorizerCache not purged on DeleteAPI; self-heals via TTL, low severity (bd: gopherstack-wmh)
  - RoutingRule Actions/Conditions untyped passthrough instead of AWS union shapes (bd: gopherstack-e81)
  - "Quick-create route/stage immutability not enforced: AWS docs state the $default route
    key can't be modified and the $default stage can't be modified once quick-created, but
    this pass only added the apiGatewayManaged flag + the Create/UpdateApi provisioning
    behavior itself (gopherstack-2tx's original scope) -- it does NOT block
    UpdateRoute/DeleteRoute/UpdateStage/DeleteStage on a managed route/stage, or
    DeleteIntegration on a managed integration. Deliberately deferred: the exact AWS error
    code/message for these rejections isn't verifiable from the Go SDK alone (it's
    server-side business logic, not encoded in serializers.go), and guessing at unverified
    error shapes would itself violate the wire-verification principle. Needs a bd issue
    (not yet filed by this pass)."
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
- The `RoutingRule` `wire: partial` rows (CreateRoutingRule/GetRoutingRule/ListRoutingRules/
  PutRoutingRule) were re-examined this pass, not just trusted at face value: the untyped
  `[]map[string]any` passthrough actually round-trips real client bytes *more* faithfully than
  a hand-typed union reimplementation would (since it echoes back exactly what the client sent,
  keyed identically), so it is not itself a wire bug — the real gap is the total absence of
  AWS's structural validation (required-field, exactly-one-of-union enforcement, priority range/
  uniqueness). Client-side `aws-sdk-go-v2` already rejects the two Actions/Conditions-are-
  `required` violations locally via smithy validation before ever sending a request, which
  lowers the real-world blast radius for the SDK-shaped clients this emulator targets. Still
  correctly tracked as `gopherstack-e81`; not re-touched.
- `quickCreateLocked`'s auto-created `$default` stage name reuses the existing `routeKeyDefault`
  constant (`proxy.go`) for its literal value rather than a new stage-specific constant — same
  string (`"$default"`), and introducing a second constant with the identical value would have
  tripped `goconst`. The name is a slight misnomer when read at the stage call site; this is
  intentional, not an oversight.
