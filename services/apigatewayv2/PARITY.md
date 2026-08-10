---
service: apigatewayv2
sdk_module: aws-sdk-go-v2/service/apigatewayv2@v1.37.4
last_audit_commit: efc42cbc4
last_audit_date: 2026-07-23
overall: A            # re-audit pass (parity-3 campaign). The previously recorded
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
  UpdateApi: {wire: fixed, errors: fixed, state: ok, persist: ok, note: "routeKey/target (\"part of quick create\" per SDK doc comments) were also entirely absent from UpdateAPIInput (fixed by a prior pass, see Notes #6). This pass: ipAddressType and quick-create's credentialsArn were ALSO entirely absent from UpdateAPIInput -- fixed, see Notes #8-9."}
  DeleteApi: {wire: ok, errors: ok, state: ok, persist: ok, note: "now also purges authorizerCache entries for the API's authorizers on cascade delete -- see Notes #11"}
  ImportApi: {wire: partial, errors: ok, state: ok, persist: ok, note: "basepath and failOnWarnings are HTTP query params (not body fields) on the real ImportApiInput and are silently ignored by gopherstack's handler -- newly found this pass, deferred, bd gopherstack-jni0. Api.importInfo/warnings shape itself is now correct (Notes #8) but always empty since the emulator never generates import warnings."}
  ReimportApi: {wire: partial, errors: ok, state: ok, persist: ok, note: "same basepath/failOnWarnings gap as ImportApi -- bd gopherstack-jni0"}
  ExportApi: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateRoute: {wire: ok, errors: ok, state: ok, persist: ok, note: "HTTP routeKey format + WS \$connect/\$disconnect/\$default/custom validated; auth type NONE/AWS_IAM/JWT/CUSTOM enforced"}
  GetRoute: {wire: ok, errors: ok, state: ok, persist: ok}
  GetRoutes: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateRoute: {wire: ok, errors: ok, state: ok, persist: ok}
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
  UpdateDomainName: {wire: fixed, errors: ok, state: ok, persist: ok, note: "routingMode fix, see Notes #10"}
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
  - RoutingRule Actions/Conditions untyped passthrough instead of AWS union shapes (bd: gopherstack-e81)
  - "Quick-create route/stage immutability not enforced: AWS docs state the $default route
    key can't be modified and the $default stage can't be modified once quick-created, but
    this pass only added the apiGatewayManaged flag + the Create/UpdateApi provisioning
    behavior itself (gopherstack-2tx's original scope) -- it does NOT block
    UpdateRoute/DeleteRoute/UpdateStage/DeleteStage on a managed route/stage, or
    DeleteIntegration on a managed integration. Deliberately deferred: the exact AWS error
    code/message for these rejections isn't verifiable from the Go SDK alone (it's
    server-side business logic, not encoded in serializers.go), and guessing at unverified
    error shapes would itself violate the wire-verification principle. Tracked by
    gopherstack-2tx (re-confirmed still open/accurate this pass, commented with the narrowed
    remaining scope; not re-touched)."
  - "ImportApi/ReimportApi's basepath and failOnWarnings params are HTTP query-string params
    on the real SDK (confirmed in serializers.go: SetQuery(\"basepath\")/SetQuery
    (\"failOnWarnings\")), not JSON body fields, and gopherstack's handleImportAPI/
    handleReimportAPI never read them, so they're silently ignored. Newly found this pass.
    Deferred rather than rushed: the OpenAPI import subsystem (parseOpenAPISpec/
    applyOpenAPIToAPI) is already a minimal best-effort parser with no basePath extraction,
    and failOnWarnings has no observable effect regardless since the emulator never
    generates import warnings (see Notes #8, importInfo/warnings are always empty for a
    well-formed spec). bd: gopherstack-jni0."
deferred:
  - Portal / PortalProduct / ProductPage / ProductRestEndpointPage families (newer preview feature, not in this pass's declared op list)
  - RoutingRule typed action/condition validation (see gaps)
  - ImportApi/ReimportApi basepath/failOnWarnings query params (see gaps, bd gopherstack-jni0)
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
