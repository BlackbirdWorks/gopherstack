---
service: appsync
sdk_module: aws-sdk-go-v2/service/appsync@v1.55.0
last_audit_commit: 4bece540
last_audit_date: 2026-07-12
overall: A            # systemic route-matcher/method bugs fixed across nearly every family
ops:
  CreateGraphqlApi: {wire: ok, errors: ok, state: ok, persist: ok}
  GetGraphqlApi: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateGraphqlApi: {wire: ok, errors: ok, state: ok, persist: ok, note: "was unreachable — handler only accepted PATCH/PUT (405 on real SDK's POST); fixed, PATCH/PUT kept as alias"}
  ListGraphqlApis: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteGraphqlApi: {wire: ok, errors: ok, state: ok, persist: ok}
  StartSchemaCreation: {wire: ok, errors: ok, state: ok, persist: ok}
  GetSchemaCreationStatus: {wire: ok, errors: ok, state: ok, persist: ok}
  GetIntrospectionSchema: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDataSource: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDataSource: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDataSource: {wire: ok, errors: ok, state: ok, persist: ok, note: "was unreachable (PUT-only); fixed, PUT kept as alias"}
  ListDataSources: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDataSource: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateResolver: {wire: ok, errors: ok, state: ok, persist: ok}
  GetResolver: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateResolver: {wire: ok, errors: ok, state: ok, persist: ok, note: "was unreachable (PUT/PATCH-only); fixed, PUT/PATCH kept as alias"}
  ListResolvers: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteResolver: {wire: ok, errors: ok, state: ok, persist: ok}
  ListResolversByFunction: {wire: ok, errors: ok, state: ok, persist: ok}
  ExecuteGraphQL: {wire: ok, errors: ok, state: ok, persist: ok, note: "path/method audited only; VTL/JS resolver execution semantics not re-audited this pass"}
  AssociateApi: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateApi: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateMergedGraphqlApi: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateSourceGraphqlApi: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateMergedGraphqlApi: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateSourceGraphqlApi: {wire: ok, errors: ok, state: ok, persist: ok}
  GetSourceApiAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  ListSourceApiAssociations: {wire: ok, errors: ok, state: ok, persist: ok, note: "two bugs fixed: (1) real SDK also lists via GET /v1/apis/{apiId}/sourceApiAssociations (apiId-keyed, distinct from the mergedApis-prefixed path) — added; (2) response was wrapped as \"sourceApiAssociations\" instead of the real \"sourceApiAssociationSummaries\" — a real client always got an empty list back"}
  UpdateSourceApiAssociation: {wire: ok, errors: ok, state: ok, persist: ok, note: "was unreachable (PUT/PATCH-only); fixed, PUT/PATCH kept as alias"}
  CreateApi: {wire: ok, errors: ok, state: ok, persist: ok}
  GetApi: {wire: ok, errors: ok, state: ok, persist: ok}
  ListApis: {wire: ok, errors: ok, state: ok, persist: ok, note: "response was wrapped as \"items\" instead of the real \"apis\" — disguised no-op, a real client always saw an empty list; fixed, added pagination"}
  UpdateApi: {wire: ok, errors: ok, state: ok, persist: ok, note: "was unreachable (PUT/PATCH-only); fixed, PUT/PATCH kept as alias"}
  DeleteApi: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateApiCache: {wire: ok, errors: ok, state: ok, persist: ok}
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
  GetDataSourceIntrospection: {wire: gap, errors: gap, state: gap, persist: n/a, note: "NOT fixed this pass — see gaps"}
  ListTypesByAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  StartDataSourceIntrospection: {wire: gap, errors: gap, state: gap, persist: n/a, note: "NOT fixed this pass — see gaps"}
  StartSchemaMerge: {wire: gap, errors: gap, state: gap, persist: n/a, note: "NOT fixed this pass — see gaps"}
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
  SourceApiAssociation_and_Merge: {status: partial, note: "Associate/Get/Update/Disassociate + the apiId-keyed List path fixed; StartSchemaMerge left as a known-broken gap (see below)"}
  DataplaneEvaluation: {status: ok, note: "EvaluateCode/EvaluateMappingTemplate path rewrite from nested /v1/dataplane-evaluations/{code,template} to the real standalone top-level paths"}
  DataSourceIntrospection: {status: gap, note: "NOT touched this pass — path AND input/output contract are both wrong; see gaps"}
gaps:
  - "StartSchemaMerge: wrong path (/v1/apis/{apiId}/schemaMerge instead of the real POST /v1/mergedApis/{mergedApiIdentifier}/sourceApiAssociations/{associationId}/merge) AND wrong semantics (real op merges one specific source-API association, keyed by associationId, not just apiId) AND wrong response shape (returns {sourceApiSchemaMetadata:[], status} instead of the real {sourceApiAssociationStatus}). Completely unreachable by a real SDK client today; fixing requires backend.StartSchemaMerge signature change to (mergedAPIID, associationID) plus a real per-association merge-status field. Left untouched (not even path-aliased) since a path-only fix would still be broken on the request/response shape. No bd issue filed yet — recommend filing one."
  - "StartDataSourceIntrospection / GetDataSourceIntrospection: wrong path (/v1/dataSource-introspections vs the real /v1/datasources/introspections) AND wrong input contract (handler expects {apiId, dataSourceName}; the real StartDataSourceIntrospectionInput only carries an optional rdsDataApiConfig{resourceArn, databaseName} — it is not tied to any existing AppSync API/DataSource at all). GetDataSourceIntrospection's backend comment already documents it as a no-op stub (\"always returns a COMPLETED result\"). Completely unreachable by a real SDK client today, and even a path-only fix would still fail on the input mismatch. Left untouched (not even path-aliased) — needs a real backend rework (new rdsDataApiConfig-keyed introspection record, no apiId/dataSourceName coupling) before it's worth making reachable. No bd issue filed yet — recommend filing one."
deferred:
  - "ExecuteGraphQL / VTL+JS resolver execution semantics (vtl.go, jseval.go, graphql.go) — route/method verified correct only; the query-execution engine itself was out of scope for this route/wire-shape sweep."
  - "CloudTrail-capture chokepoint / pkgs/service integration — not audited (shared/cross-service, out of scope per this task's edit boundary)."
leaks: {status: clean, note: "janitor.go's background goroutine already takes ctx and is started once via StartWorker; no new goroutines, tickers, or unbounded maps were added this pass. The two new safemap-style Tags-table lookups (b.apis / b.eventAPIs) reuse existing store.Table entries — no new state."}
---

## Notes

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
