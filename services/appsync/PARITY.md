---
service: appsync
sdk_module: aws-sdk-go-v2/service/appsync@v1.56.4
last_audit_commit: 198990e82
last_audit_date: 2026-08-15
overall: A            # 2026-07-24: systemic route-matcher/method bugs fixed across nearly every family; the two remaining gaps from the 2026-07-12 pass (StartSchemaMerge, Start/GetDataSourceIntrospection) are now implemented for real
                      # 2026-07-31: pkgs/sdkcheck reverse check found ExecuteGraphQL wrongly advertised/documented as a real SDK op (it isn't -- see its ops-block note); corrected, route left wired as internal data-plane scaffolding. Grade held at A: a documentation defect, not a served-client bug.
                      # 2026-07-31 (second pass, browser parity): RouteMatcher's /v2/apis-vs-ApiGatewayV2 disambiguation (see its doc comment) checked only the User-Agent header, which a browser cannot set (Fetch spec) -- the AWS SDK for JavaScript in a browser puts its SDK identification in X-Amz-User-Agent instead, so every browser dashboard request through /v2/apis silently fell through to API Gateway V2 or S3. Fixed via the new pkgs/service.MatchesUserAgentMarker helper (checks both headers, case-insensitively -- the JS SDK's marker is "api/AppSync", PascalCase, vs aws-sdk-go-v2's lowercase "api/appsync"), shared with the identical bug class fixed the same pass in mediastoredata/docdb/neptune. Grade held at A: fixed, not deferred.
                      # 2026-08-07 (gopherstack-ivwh): ExecuteGraphQL's field resolution silently ignored a UNIT resolver's Code (APPSYNC_JS) field entirely -- only VTL RequestMappingTemplate/ResponseMappingTemplate were ever applied, so a Code-configured resolver behaved as if it had no mapping at all, and PIPELINE resolvers (Kind="PIPELINE"+PipelineConfig) were never executed as a chain at all (resolveField only ever looked at resolver.DataSourceName directly). Both fixed for real: Code-configured UNIT resolvers now run their request/response handlers through the existing documented-subset JS evaluator (jseval.go); PIPELINE resolvers now execute each Function in PipelineConfig order, threading ctx.prev.result between them, then the resolver's own after-mapping. Also fixed a related VTL gap: renderVTL had no $context.prev.result support at all (only $context.result existed), which would have made pipeline function request templates silently render "$ctx.prev.result.x" as a literal string instead of the previous function's field. DataSourceIntrospection's introspected *content* remains a documented structural gap (needs RDS Data API cross-service integration); see gaps.
                      # 2026-08-15 (gopherstack-6flj wrapper-key sweep): this file's extensive "wire: ok" history was re-verified independently against the real deserializer's own case list (not trusted on faith, per that issue's flagship kafka finding). Layer-1 wrapper keys came back entirely clean. 7 layer-2/3 bugs found and fixed: SourceApiAssociation's status field used the wrong wire key ("associationStatus", a sibling-trap copy from the genuinely-different ApiAssociation type -- real key is "sourceApiAssociationStatus", deserializers.go:16488); EventConfig.LogConfig, DataSource.MetricsConfig and Resolver.MetricsConfig were all real, accepted request fields silently discarded on both Create and Update (discarded-input class); GraphqlApi.EnvironmentVariables leaked real customer-set env-var values into GetGraphqlApi/ListGraphqlApis/CreateGraphqlApi/UpdateGraphqlApi, a field the real GraphqlApi type does not have at all (env vars are only ever exposed via the dedicated Get/PutGraphqlApiEnvironmentVariables ops); GraphqlApi.Owner (real member, "the account owner") was unmodeled despite the backend already holding the account ID. Grade held at A: all fixed, not deferred, except the always-disclosed structural gaps below. Full detail in services/_WRAPPER_KEY_SWEEP_REMAINDER.md's "appsync (this session)" section.
ops:
  CreateGraphqlApi: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-08-15: added real \"owner\" member (account owner), previously unmodeled despite the account ID already being on hand"}
  GetGraphqlApi: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-08-15: fixed EnvironmentVariables leaking into the GraphqlApi wire object (json:\"-\" now; real type has no such member at all -- env vars belong only to the dedicated Get/PutGraphqlApiEnvironmentVariables ops); added \"owner\""}
  UpdateGraphqlApi: {wire: ok, errors: ok, state: ok, persist: ok, note: "was unreachable — handler only accepted PATCH/PUT (405 on real SDK's POST); fixed, PATCH/PUT kept as alias. 2026-08-15: same EnvironmentVariables-leak fix as GetGraphqlApi"}
  ListGraphqlApis: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-08-15: same EnvironmentVariables-leak fix as GetGraphqlApi"}
  DeleteGraphqlApi: {wire: ok, errors: ok, state: ok, persist: ok}
  StartSchemaCreation: {wire: ok, errors: ok, state: ok, persist: ok}
  GetSchemaCreationStatus: {wire: ok, errors: ok, state: ok, persist: ok}
  GetIntrospectionSchema: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDataSource: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-08-15: added real \"metricsConfig\" member (ENABLED/DISABLED), previously discarded entirely on both create and update. apiId/tags fields on the wire object are fabricated (not on the real DataSource type at all) but harmless and disclosed, not fixed -- see remainder file"}
  GetDataSource: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDataSource: {wire: ok, errors: ok, state: ok, persist: ok, note: "was unreachable (PUT-only); fixed, PUT kept as alias. 2026-08-15: metricsConfig now round-trips (see CreateDataSource note)"}
  ListDataSources: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDataSource: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateResolver: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-08-15: added real \"metricsConfig\" member (ENABLED/DISABLED), previously discarded entirely on both create and update. apiId field on the wire object is fabricated (not on the real Resolver type) but harmless, disclosed not fixed"}
  GetResolver: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateResolver: {wire: ok, errors: ok, state: ok, persist: ok, note: "was unreachable (PUT/PATCH-only); fixed, PUT/PATCH kept as alias. 2026-08-15: metricsConfig now round-trips (see CreateResolver note)"}
  ListResolvers: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteResolver: {wire: ok, errors: ok, state: ok, persist: ok}
  ListResolversByFunction: {wire: ok, errors: ok, state: ok, persist: ok}
  # ExecuteGraphQL is intentionally NOT listed as an advertised SDK op here.
  # 2026-07-31 CORRECTION: the row that used to live at this position ("wire:
  # ok, ...") was inaccurate -- ExecuteGraphQL is not a real AWS AppSync SDK
  # operation at all (verified against aws-sdk-go-v2/service/appsync: `go doc`
  # lists only management operations, no ExecuteGraphQL method; real clients
  # POST GraphQL queries straight to the API's graphqlEndpoint, a request the
  # typed SDK does not model as an operation). Caught by pkgs/sdkcheck's
  # reverse check (commit 12cfe14d5; gopherstack-vhw2 category A). The route
  # (POST /v1/apis/{apiId}/graphql -> handleGraphQL) stays wired -- gopherstack
  # still needs to serve real GraphQL data-plane traffic -- and dispatch keys
  # off the literal "graphql" path segment, not this label, so no client is
  # affected. GetSupportedOperations()/ChaosOperations() no longer advertise
  # it; see opExecuteGraphQL's doc comment in handler.go. Same resolution as
  # CloudFront's GetFunctionAssociations/SetFunctionAssociations and EMR's
  # ListTagsForResource. The route/method plumbing itself was and remains
  # correctly audited (see deferred note below on VTL/JS execution scope).
  AssociateApi: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateApi: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateMergedGraphqlApi: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-08-15: SourceApiAssociation.AssociationStatus wire key fixed, see GetSourceApiAssociation note"}
  AssociateSourceGraphqlApi: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-08-15: same SourceApiAssociation status-key fix as AssociateMergedGraphqlApi"}
  DisassociateMergedGraphqlApi: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateSourceGraphqlApi: {wire: ok, errors: ok, state: ok, persist: ok}
  GetSourceApiAssociation: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-08-15: SourceApiAssociation.AssociationStatus was wired to the wrong key, \"associationStatus\" -- a sibling-trap copy from the genuinely-different ApiAssociation type (domain-name associations), which really does use that plain key. Real key is \"sourceApiAssociationStatus\" (deserializers.go:16488); a real client's typed field was always empty. Fixed; also added the real (never-populated, since merges here always succeed) sourceApiAssociationStatusDetail member"}
  ListSourceApiAssociations: {wire: ok, errors: ok, state: ok, persist: ok, note: "two bugs fixed: (1) real SDK also lists via GET /v1/apis/{apiId}/sourceApiAssociations (apiId-keyed, distinct from the mergedApis-prefixed path) — added; (2) response was wrapped as \"sourceApiAssociations\" instead of the real \"sourceApiAssociationSummaries\" — a real client always got an empty list back. Summary narrowing fixed: now maps to narrow SourceAPIAssociationSummary matching real types.SourceApiAssociationSummary (omits sourceApiAssociationStatus/Detail and config)"}
  UpdateSourceApiAssociation: {wire: ok, errors: ok, state: ok, persist: ok, note: "was unreachable (PUT/PATCH-only); fixed, PUT/PATCH kept as alias. 2026-08-15: same status-key fix as GetSourceApiAssociation"}
  CreateApi: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-08-15: added EventConfig.LogConfig (real member, previously discarded entirely on both create and update -- new EventLogConfig type, distinct 2-field shape from GraphqlApi's LogConfig)"}
  GetApi: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-08-15: EventConfig.LogConfig now round-trips, see CreateApi note"}
  ListApis: {wire: ok, errors: ok, state: ok, persist: ok, note: "response was wrapped as \"items\" instead of the real \"apis\" — disguised no-op, a real client always saw an empty list; fixed, added pagination"}
  UpdateApi: {wire: ok, errors: ok, state: ok, persist: ok, note: "was unreachable (PUT/PATCH-only); fixed, PUT/PATCH kept as alias. 2026-08-15: EventConfig.LogConfig now round-trips, see CreateApi note"}
  DeleteApi: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateApiCache: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (gopherstack-hnyl): isValidAPICacheType was missing R4_LARGE/R4_XLARGE and invented a nonexistent R4_1XLARGE; isValidAPICachingBehavior was missing OPERATION_LEVEL_CACHING and invented a nonexistent FULL_REQUEST_DATA_CACHING. Both now derive from types.ApiCacheType.Values()/types.ApiCachingBehavior.Values()."}
  DeleteApiCache: {wire: ok, errors: ok, state: ok, persist: ok}
  FlushApiCache: {wire: ok, errors: ok, state: ok, persist: ok, note: "real path is DELETE /v1/apis/{apiId}/FlushCache, not /ApiCaches/entries — was unreachable; fixed, old path kept as alias"}
  GetApiCache: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateApiCache: {wire: ok, errors: ok, state: ok, persist: ok, note: "real path is POST /v1/apis/{apiId}/ApiCaches/update, not PUT to the collection path — was unreachable; fixed, old path kept as alias"}
  CreateApiKey: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteApiKey: {wire: ok, errors: ok, state: ok, persist: ok}
  ListApiKeys: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateApiKey: {wire: ok, errors: ok, state: ok, persist: ok, note: "was unreachable (PUT/PATCH-only); fixed, PUT/PATCH kept as alias"}
  CreateChannelNamespace: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteChannelNamespace: {wire: ok, errors: ok, state: ok, persist: ok}
  GetChannelNamespace: {wire: ok, errors: ok, state: ok, persist: ok}
  ListChannelNamespaces: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateChannelNamespace: {wire: ok, errors: ok, state: ok, persist: ok, note: "was unreachable (PUT/PATCH-only); fixed, PUT/PATCH kept as alias"}
  CreateDomainName: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDomainName: {wire: ok, errors: ok, state: ok, persist: ok}
  GetApiAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDomainName: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDomainNames: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDomainName: {wire: ok, errors: ok, state: ok, persist: ok, note: "was unreachable (PUT/PATCH-only); fixed, PUT/PATCH kept as alias"}
  CreateFunction: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteFunction: {wire: ok, errors: ok, state: ok, persist: ok}
  GetFunction: {wire: ok, errors: ok, state: ok, persist: ok}
  ListFunctions: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateFunction: {wire: ok, errors: ok, state: ok, persist: ok, note: "was unreachable (PUT-only); fixed, PUT kept as alias"}
  CreateType: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteType: {wire: ok, errors: ok, state: ok, persist: ok}
  GetType: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTypes: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateType: {wire: ok, errors: ok, state: ok, persist: ok, note: "was unreachable (PUT-only); fixed, PUT kept as alias"}
  GetGraphqlApiEnvironmentVariables: {wire: ok, errors: ok, state: ok, persist: ok}
  PutGraphqlApiEnvironmentVariables: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "real path is GET /v1/tags/{resourceArn}, entirely unreachable at the previously-only-implemented /v1/apis/{apiId}/tags — fixed (both v1 GraphqlApi and v2 Api ARNs), old path kept as alias"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "same fix as ListTagsForResource; TagResource/UntagResource now also work against v2 Api (Event API) resources, not just v1 GraphqlApi"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "same fix as ListTagsForResource"}
  EvaluateCode: {wire: ok, errors: ok, state: ok, persist: n/a, note: "real path is POST /v1/dataplane-evaluatecode (standalone), not /v1/dataplane-evaluations/code — was unreachable; fixed, old path kept as alias"}
  EvaluateMappingTemplate: {wire: ok, errors: ok, state: ok, persist: n/a, note: "real path is POST /v1/dataplane-evaluatetemplate (standalone), not /v1/dataplane-evaluations/template — was unreachable; fixed, old path kept as alias"}
  GetDataSourceIntrospection: {wire: ok, errors: ok, state: ok, persist: ok, note: "real path added (GET /v1/datasources/introspections/{introspectionId}, distinct from the /v1/dataSource-introspections legacy alias); response body rebuilt to the real flat shape (introspectionId/introspectionResult/introspectionStatus/introspectionStatusDetail at the top level, introspectionResult itself {models,nextToken}) instead of the old {introspectionResult: {introspectionId, status, models}} nesting; unknown IDs now correctly 404 (previously always synthesized a fake SUCCESS for ANY id, even ones never started)"}
  ListTypesByAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  StartDataSourceIntrospection: {wire: ok, errors: ok, state: ok, persist: ok, note: "real path added (POST /v1/datasources/introspections); input contract corrected from the invented {apiId, dataSourceName} (not part of the real StartDataSourceIntrospectionInput, which is NOT scoped to any AppSync API/DataSource at all) to the real optional rdsDataApiConfig{databaseName,resourceArn,secretArn}; now persists a real DataSourceIntrospection record (new 'introspections' store.Table) keyed by introspectionId instead of returning an unpersisted random ID with nothing behind it. gopherstack has no real RDS Data API connectivity, so every well-formed request completes synchronously with SUCCESS and an empty models list -- wire shape, error codes and persisted/retrievable state are all real; the *contents* of a genuine introspection (actual RDS table/column data) are out of scope, same category as ExecuteGraphQL's VTL/JS engine scope limit below"}
  StartSchemaMerge: {wire: ok, errors: ok, state: ok, persist: ok, note: "moved from the invented POST /v1/apis/{apiId}/schemaMerge (apiId-only, response {sourceApiSchemaMetadata:[], status}) to the real POST /v1/mergedApis/{mergedApiIdentifier}/sourceApiAssociations/{associationId}/merge, keyed by BOTH mergedApiIdentifier and associationId with response {sourceApiAssociationStatus}; backend signature changed from StartSchemaMerge(apiID) to StartSchemaMerge(mergedAPIID, associationID), now validates and mutates the real SourceAPIAssociation.AssociationStatus (MERGE_SUCCESS) instead of returning a hardcoded SchemaStatus disconnected from any association. The old invented endpoint was deleted outright rather than aliased: an apiId-only request has no way to recover the associationId the real operation requires, so a path-only alias would still be wrong on the request/response shape"}
families:
  GraphqlApi_CRUD: {status: ok, note: "Create/Get/List/Delete already correct; UpdateGraphqlApi POST-method bug fixed"}
  ApiKey_CRUD: {status: ok, note: "expires already epoch-seconds (pkgs/awstime not used but manual int64 matches wire); UpdateApiKey POST-method bug fixed"}
  DataSource_CRUD: {status: ok, note: "UpdateDataSource POST-method bug fixed"}
  Resolver_CRUD: {status: ok, note: "UpdateResolver POST-method bug fixed"}
  Function_CRUD: {status: ok, note: "UpdateFunction POST-method bug fixed"}
  Type_CRUD: {status: ok, note: "UpdateType POST-method bug fixed"}
  Tags: {status: ok, note: "full path rewrite from /v1/apis/{apiId}/tags to real /v1/tags/{resourceArn}, with ARN-embedded-slash handling and v1+v2 API resource support"}
  ApiCache: {status: ok, note: "UpdateApiCache and FlushApiCache both had wrong path+method; fixed"}
  DomainName_and_ApiAssociation: {status: ok, note: "UpdateDomainName POST-method bug fixed; rest already correct"}
  ChannelNamespace_EventApi: {status: ok, note: "CreateApi/GetApi/ListApis/UpdateApi/DeleteApi + ChannelNamespace CRUD; ListApis \"items\"->\"apis\" wrapper bug fixed, UpdateApi/UpdateChannelNamespace POST-method bugs fixed"}
  SourceApiAssociation_and_Merge: {status: ok, note: "Associate/Get/Update/Disassociate + the apiId-keyed List path fixed (2026-07-12 pass); StartSchemaMerge implemented for real this pass at the correct path/keying/response shape (2026-07-24)"}
  DataplaneEvaluation: {status: ok, note: "EvaluateCode/EvaluateMappingTemplate path rewrite from nested /v1/dataplane-evaluations/{code,template} to the real standalone top-level paths"}
  DataSourceIntrospection: {status: ok, note: "implemented for real this pass (2026-07-24): real path, real rdsDataApiConfig-based input contract, real persisted/keyed state, real error codes. No real RDS Data API connectivity in gopherstack, so introspected model content is always an empty list -- documented in items_still_open, not a wire/error/state gap"}
  ExecuteGraphQL_resolvers: {status: fixed, note: "gopherstack-ivwh (2026-08-07): UNIT resolvers configured with APPSYNC_JS Code (instead of VTL templates) now actually run their request/response handlers via jseval.go's documented-subset evaluator, for all three data source types (Lambda/DynamoDB/None) -- previously Code was silently ignored and the resolver behaved as if unmapped. PIPELINE resolvers (Kind=PIPELINE + PipelineConfig) now execute their Function chain for real (each function's own VTL-or-JS request/response mapping against its own data source, ctx.prev.result threaded between them), followed by the resolver's own after-mapping -- previously PIPELINE resolvers were never distinguished from UNIT at all (resolveField read resolver.DataSourceName directly, which a PIPELINE resolver doesn't even set). Also fixed renderVTL, which had no $context.prev.result support (only $context.result existed) -- a pipeline function's request template referencing $ctx.prev.result.x would have rendered the literal string unexpanded. See gaps for the documented-subset limits that remain (pipeline before-mapping, DynamoDB JS resolver helpers)."}
gaps:
  - "PIPELINE resolver before-mapping (RequestMappingTemplate / Code's `request` handler, at the resolver level, not a Function's) is intentionally not evaluated (bd: gopherstack-ivwh). On real AppSync its only observable effects beyond building a request object nothing here consumes are writing to ctx.stash (read by later pipeline functions) and short-circuiting the pipeline via util.error/an early return -- neither of which this evaluator's documented subset implements. Evaluating it and discarding the result would be pointless busywork; skipping it is the honest reflection of what's supported. See executePipeline's doc comment in graphql.go."
  - "The APPSYNC_JS evaluator (jseval.go) supports a documented subset of real JS: `return <object/array/json literal>;`, context member expressions, and the pure util.* helpers (toJson/parseJson/error/appendError/unauthorized) -- not control flow, loops, variable bindings, or DynamoDB-specific helpers like util.dynamodb.get()/put(). A JS DynamoDB resolver must therefore return the raw {operation,key/item} object literal directly (mirroring what a VTL template renders) rather than using util.dynamodb.* sugar. Constructs outside the subset return ErrUnsupportedJSCode rather than a fabricated result -- see jseval.go's doc comment for the full supported-pattern list."
  - "2026-08-15: GraphqlApi missing real dns/enhancedMetricsConfig/mergedApiExecutionRoleArn/wafWebAclArn members -- none tracked anywhere in this backend (merged-API execution role, WAF ACL association, and enhanced metrics config are all unsimulated cross-feature concepts). Api (Event API) missing real created timestamp (optional, not required) and wafWebAclArn, same reason. DataSource missing the deprecated legacy elasticsearchConfig member (real AWS docs steer new integrations to openSearchServiceConfig instead)."
  - "2026-08-15: DataSource/Resolver/Function/ApiCache/APIType/DomainNameConfig each carry a fabricated apiId field on their own wire object (none of the corresponding real types has one -- apiId lives on the URL path only); DataSource also carries a fabricated tags field (the real DataSource type has no tags member, consistent with handler_create_tags_test.go's existing finding that DataSource ARNs aren't a TagResource target). GraphqlApi.Region/CreatedAt/UpdatedAt are also fabricated (no such real members). All harmless -- a real client silently ignores unknown JSON keys -- and disclosed rather than fixed to avoid 6+ call-site changes for no functional benefit; see services/_WRAPPER_KEY_SWEEP_REMAINDER.md's appsync section."
deferred:
  - "CloudTrail-capture chokepoint / pkgs/service integration — not audited (shared/cross-service, out of scope per this task's edit boundary)."
  - "DataSourceIntrospection real model content: gopherstack has no RDS Data API backend to introspect against, so StartDataSourceIntrospection/GetDataSourceIntrospection always complete SUCCESS with an empty models list rather than real table/column data. Wire shape, error codes (BadRequestException on missing/incomplete rdsDataApiConfig, NotFoundException on unknown introspectionId), and persisted per-ID state are all real and field-diffed against the SDK; only the introspected *content* is out of scope. Would require a services/rds (or similar) cross-service integration to fix — out of this task's services/appsync/ edit boundary."
leaks: {status: clean, note: "janitor.go's background goroutine already takes ctx and is started once via StartWorker; no new goroutines, tickers, or unbounded maps were added this (or the prior) pass. The two safemap-style Tags-table lookups (b.apis / b.eventAPIs) reuse existing store.Table entries. This pass added one new store.Table (b.introspections, registered in store_setup.go, generically covered by the existing Snapshot/Restore/ResetAll wiring in persistence.go) — introspection records are NOT scoped to any GraphqlApi/Api/DataSource (matches the real AWS operation, which isn't either), so DeleteGraphqlApi/DeleteApi/DeleteDataSource correctly do NOT cascade-delete them; there is no lock path in the new code without a matching defer-release (verified by -race)."}
---

## Notes

**2026-08-13 (gopherstack-jqh2 pass 3):** re-extracted all 74 ops' real
method+path directly from `appsync@v1.56.4` serializers.go and drove them
through `ExtractOperation` via the new `handler_sdk_route_table_test.go`
(`TestExtractOperation_SDKRouteTable`, one subtest per op, `t.Parallel()`).
All 74 resolved correctly, including the several same-path/different-method
collisions this service's routing depends on
(`/v1/apis/{apiId}/ApiCaches`, `/v1/tags/{arn}`, `/v2/apis/{apiId}`,
`/v1/apis/{apiId}` GET/DELETE/POST). No pre-existing table existed to
check. This confirms the extensive Update*-uses-POST route work from the
2026-07-24/07-31 passes documented below held under the strong per-op SDK
diff method — no new routing bugs found. This test is now the permanent
regression guard for route-table drift.

### The core bug class this sweep found and fixed: Update* uses POST, not PUT/PATCH

AppSync is restjson1. Verified directly against `aws-sdk-go-v2/service/appsync@v1.55.0`'s
`serializers.go`: **every single Update\* operation is serialized as `POST`** to the
same path as the corresponding Get (e.g. `UpdateGraphqlApi` → `POST /v1/apis/{apiId}`,
same path as `GetGraphqlApi`'s `GET` and `DeleteGraphqlApi`'s `DELETE`). AWS AppSync's
REST API never uses HTTP PUT or PATCH for anything — that convention doesn't apply here
the way it does for many other AWS REST services. This handler had every Update*
dispatch gated on `PUT`/`PATCH` only, so **every Update operation** (10 total:
UpdateGraphqlApi, UpdateApi, UpdateDataSource, UpdateFunction, UpdateType,
UpdateResolver, UpdateApiKey, UpdateDomainName, UpdateChannelNamespace,
UpdateSourceApiAssociation) returned `405 MethodNotAllowed` to a real AWS SDK client.
Fixed by adding `POST` as an accepted method everywhere alongside the existing
PUT/PATCH (kept for any non-SDK/manual callers) — a strict superset, no existing
behavior removed.

`UpdateApiCache` and `FlushApiCache` are worse: they live at entirely different paths
from what was implemented (`POST /v1/apis/{apiId}/ApiCaches/update` and
`DELETE /v1/apis/{apiId}/FlushCache`, vs the implemented `PUT .../ApiCaches` and
`DELETE .../ApiCaches/entries`). Fixed the same way — new correct routes added, old
ones kept working as aliases.

### Tags: entirely different top-level path

`TagResource`/`UntagResource`/`ListTagsForResource` are NOT nested under
`/v1/apis/{apiId}/tags` on the wire — the real endpoint is
`/v1/tags/{resourceArn}`, a standalone top-level path taking a full ARN, not an apiId.
This was **completely unreachable** (RouteMatcher didn't even claim `/v1/tags/*`
before this fix — none of the six registered prefixes match it). resourceArn itself
contains `/` (`arn:aws:appsync:region:account:apis/{apiId}`); the AWS SDK
percent-encodes it as `%2F` in the URI label, and since `net/http` decodes the request
path before routing reaches this handler, the ARN's internal slash arrives as an
ordinary extra path segment that must be rejoined (`apiIDFromResourceARN` in
handler.go). Also extended `TagResource`/`UntagResource`/`ListTagsForResource` in
backend.go to check both the `b.apis` (GraphqlApi v1) and `b.eventAPIs` (Api v2 /
Event API) tables — previously only v1 GraphqlApi was taggable even though both
resource kinds share the `apis/{id}` ARN shape and are both valid TagResource targets
on the real API.

### Two disguised-no-op response-wrapper-key bugs (found by cross-checking every
### List* op's JSON field name against the real deserializer)

- `ListApis` wrapped its result as `{"items": [...]}`; the real
  `ListApisOutput` field is `"apis"`. A real SDK client's JSON deserializer only reads
  known field names, so this was silently returning an always-empty list to every
  caller regardless of actual backend state — the classic "real-looking op that's
  actually a stub" pattern flagged in the parity principles doc. Fixed, and pagination
  (`nextToken`/`maxResults`) added to match the other List ops (it had none).
- `ListSourceApiAssociations` wrapped its result as `{"sourceApiAssociations": [...]}`;
  the real `ListSourceApiAssociationsOutput` field is
  `"sourceApiAssociationSummaries"`. Same always-empty-to-a-real-client bug. Fixed. (The
  `"sourceApiAssociations"` string is *also*, confusingly, the correct literal URL path
  segment name for several unrelated endpoints — that usage was untouched, only the
  JSON body wrapper key was wrong.)

All other List* response wrapper keys (`apiKeys`, `channelNamespaces`, `dataSources`,
`domainNameConfigs`, `functions`, `graphqlApis`, `resolvers`, `tags`, `types`) were
independently verified against the real deserializers and are correct.

### RouteMatcher

`/v1/tags`, `/v1/dataplane-evaluatecode`, and `/v1/dataplane-evaluatetemplate` prefixes
were added to `RouteMatcher()` (previously only `/v1/dataplane-evaluations` was
registered, which doesn't match either real path). `TestRouteMatcher_RealPaths` in
`wire_route_parity_test.go` exercises `h.RouteMatcher()` directly (not just
`h.Handler()`) for every path fixed this sweep, per the audit's explicit route-matcher
check — this is the exact bug class ("unit tests bypass the matcher") that hit backup,
eks, s3control, guardduty, cleanrooms, bedrockagent, iotwireless, and pinpoint
previously.

### Deliberate non-breaking-alias strategy

Every fix in this sweep is **additive**: the previously-implemented (AWS-inaccurate)
paths/methods still work exactly as before (PUT/PATCH aliases for Update ops, the old
`/v1/apis/{apiId}/tags` path, the old `/ApiCaches/entries` and `/v1/dataplane-evaluations/*`
paths). Only the *real* AWS SDK-accurate paths/methods were added alongside them. This
was a deliberate choice to fix the (severe) real-client-facing bugs without touching
or re-validating ~15 existing unit tests that exercise the old aliases — reducing risk
in an already-large sweep. A future cleanup pass could remove the aliases once nothing
in-tree depends on them, per de-stub hygiene, but they are not stubs themselves (both
paths reach the same real, fully-implemented business logic) so leaving them is not a
parity violation.

### persistence.go

Read and verified intact — not modified. `Snapshot`/`Restore` already drive every
`*store.Table[V]` on `b.registry` generically (including `eventAPIs`, the v2 Api table
this sweep's Tags fix now also mutates), so no persistence wiring changes were needed;
the new `eventAPI.Tags` mutations are automatically covered by the existing generic
snapshot mechanism.

### 2026-07-24 pass: StartSchemaMerge and Start/GetDataSourceIntrospection implemented for real

The 2026-07-12 audit left these two gaps explicitly untouched ("not even path-aliased")
because a path-only fix would still have been broken on the request/response shape.
This pass reworked both for real, field-diffed against
`aws-sdk-go-v2/service/appsync@v1.55.0`.

**StartSchemaMerge.** The old implementation lived at the invented
`POST /v1/apis/{apiId}/schemaMerge`, keyed only by `apiId`, returning an invented
`{sourceApiSchemaMetadata: [], status}` body. The real operation is
`POST /v1/mergedApis/{mergedApiIdentifier}/sourceApiAssociations/{associationId}/merge`
— keyed by BOTH `mergedApiIdentifier` and `associationId` (a merge always targets one
specific source-API association, never "the merged API" as a whole) — and returns
`{sourceApiAssociationStatus}`. `InMemoryBackend.StartSchemaMerge`'s signature changed
from `(apiID) (SchemaStatus, error)` to `(mergedAPIID, associationID) (string, error)`;
it now validates both the merged API and the association exist (and that the
association actually belongs to that merged API), mutates the real
`SourceAPIAssociation.AssociationStatus` to `MERGE_SUCCESS`, and returns that value —
replacing the old version's hardcoded, association-disconnected `SchemaStatusActive`
return. The invented old endpoint was **deleted outright**, not aliased: an
`apiId`-only request has no way to recover the `associationId` the real operation
requires, so keeping it as a path alias would still have served the wrong contract.
`TestHandler_LegacySchemaMergeEndpointRemoved` locks that the old path now 404s instead
of silently accepting invented-shape requests.

**StartDataSourceIntrospection / GetDataSourceIntrospection.** The old implementation
lived at `/v1/dataSource-introspections[/{id}]` and took `{apiId, dataSourceName}` —
neither field exists on the real `StartDataSourceIntrospectionInput`, which carries
only an optional `rdsDataApiConfig{databaseName,resourceArn,secretArn}` and is **not
scoped to any AppSync GraphqlApi/DataSource at all** (it introspects an RDS Data
API-backed database directly). `GetDataSourceIntrospection` was a pure stub — its own
doc comment said so — that returned a fabricated `SUCCESS` result for literally any
ID string, including ones that were never started. Fixed:

- Real path added: `POST /v1/datasources/introspections` and
  `GET /v1/datasources/introspections/{introspectionId}` (registered in
  `RouteMatcher()`, `parseOperation` via the new `pathSegDatasources` top-level case,
  and `dispatchTopLevel`). The old `/v1/dataSource-introspections` path is kept working
  as a non-breaking alias, rewired to the same corrected backend contract (same
  "deliberate non-breaking-alias strategy" as the rest of this service — see above).
- New backend contract: `StartDataSourceIntrospection(cfg *RDSDataAPIConfig)
  (*DataSourceIntrospection, error)` validates `cfg` and its three required fields
  (`BadRequestException` if missing, matching the real client-side
  `validateRdsDataApiConfig`), then creates and **persists** a real
  `DataSourceIntrospection` record in a new `introspections` store.Table (registered in
  store_setup.go; automatically covered by the existing generic
  Snapshot/Restore/ResetAll wiring in persistence.go — see
  `Test_InMemoryBackend_SnapshotRestore`). `GetDataSourceIntrospection` now looks the
  record up by ID and returns `NotFoundException` for unknown IDs instead of
  fabricating a result.
- Response shapes corrected: `StartDataSourceIntrospectionOutput` is
  `{introspectionId, introspectionStatus, introspectionStatusDetail}` (no
  `introspectionResult` — that field only exists on Get); `GetDataSourceIntrospectionOutput`
  is the flat `{introspectionId, introspectionResult: {models, nextToken},
  introspectionStatus, introspectionStatusDetail}`, not the old
  `{introspectionResult: {introspectionId, status, models}}` nesting.
- New model types (`RDSDataAPIConfig`, `DataSourceIntrospectionModel`,
  `DataSourceIntrospectionModelField(Type)`, `DataSourceIntrospectionModelIndex`,
  `DataSourceIntrospectionResult`, `DataSourceIntrospectionStatus*` constants) added to
  models.go, field-named and -shaped to match
  `aws-sdk-go-v2/service/appsync/types` exactly.
- **Known, documented limitation** (not a wire/error/state gap): gopherstack has no
  real RDS Data API connectivity, so every well-formed request completes synchronously
  with `SUCCESS` and an **empty** `models` list — there is no real database to
  introspect. This is called out explicitly in `deferred` above rather than silently
  passed off as full parity; a real fix would require a cross-service RDS Data API
  integration, out of this task's `services/appsync/` edit boundary.
