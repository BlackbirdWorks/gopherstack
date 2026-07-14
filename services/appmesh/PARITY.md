---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: appmesh
sdk_module: aws-sdk-go-v2/service/appmesh@v1.36.2
last_audit_commit: 40f05928
last_audit_date: 2026-07-13
overall: A            # genuine fixes found: the primary response-wrapping bug affected every
                       # Create/Describe/Update/Delete op in the service (28 handler call sites).
ops:
  CreateMesh: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: response now wrapped under mesh key"}
  DescribeMesh: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: response now wrapped under mesh key"}
  UpdateMesh: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: response now wrapped under mesh key; version increments"}
  DeleteMesh: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: response now wrapped under mesh key; in-use check blocks delete while children exist"}
  ListMeshes: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: limit query param now honored (was hardcoded to 100)"}
  CreateVirtualNode: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: response now wrapped under virtualNode key"}
  DescribeVirtualNode: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: response now wrapped under virtualNode key"}
  UpdateVirtualNode: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: response now wrapped under virtualNode key"}
  DeleteVirtualNode: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: response now wrapped under virtualNode key"}
  ListVirtualNodes: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateVirtualRouter: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: response now wrapped under virtualRouter key"}
  DescribeVirtualRouter: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: response now wrapped under virtualRouter key"}
  UpdateVirtualRouter: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: response now wrapped under virtualRouter key"}
  DeleteVirtualRouter: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: response now wrapped under virtualRouter key; blocks delete while routes exist"}
  ListVirtualRouters: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateRoute: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: response now wrapped under route key"}
  DescribeRoute: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: response now wrapped under route key"}
  UpdateRoute: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: response now wrapped under route key"}
  DeleteRoute: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: response now wrapped under route key"}
  ListRoutes: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateVirtualService: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: response now wrapped under virtualService key"}
  DescribeVirtualService: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: response now wrapped under virtualService key"}
  UpdateVirtualService: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: response now wrapped under virtualService key"}
  DeleteVirtualService: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: response now wrapped under virtualService key"}
  ListVirtualServices: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateVirtualGateway: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: response now wrapped under virtualGateway key"}
  DescribeVirtualGateway: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: response now wrapped under virtualGateway key"}
  UpdateVirtualGateway: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: response now wrapped under virtualGateway key"}
  DeleteVirtualGateway: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: response now wrapped under virtualGateway key; blocks delete while gateway routes exist"}
  ListVirtualGateways: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateGatewayRoute: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: response now wrapped under gatewayRoute key"}
  DescribeGatewayRoute: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: response now wrapped under gatewayRoute key"}
  UpdateGatewayRoute: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: response now wrapped under gatewayRoute key"}
  DeleteGatewayRoute: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: response now wrapped under gatewayRoute key"}
  ListGatewayRoutes: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT /v20190125/tag, resourceArn+tags in JSON body — verified against real serializer"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT /v20190125/untag, resourceArn+tagKeys in JSON body"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /v20190125/tags, resourceArn/limit/nextToken as query params"}
families:
  mesh_crud: {status: ok, note: "route matcher, HTTP methods (PUT create/update, GET describe, DELETE, GET list), ARN shape, error codes all verified against real serializer/deserializer source"}
  virtualnode_crud: {status: ok}
  virtualrouter_and_route_crud: {status: ok, note: "route paths correctly use singular /virtualRouter/{name}/routes (AWS API quirk), verified vs real SDK SplitURI"}
  virtualservice_crud: {status: ok}
  virtualgateway_and_gatewayroute_crud: {status: ok, note: "gateway route paths correctly use singular /virtualGateway/{name}/gatewayRoutes"}
  tags: {status: ok}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "meshOwner query param (cross-account shared mesh access) is accepted on Describe/Delete/Create/List paths by real AWS but never read by gopherstack's handlers; this backend has no cross-account model at all. Low priority: shared meshes are an advanced, rarely-emulated feature."
  - "No server-side name validation (AWS enforces a resourceName regex/length constraint on meshName/virtualNodeName/etc.); invalid names are accepted rather than rejected with BadRequestException. Spec bodies are also passed through as opaque JSON with no schema validation against the real MeshSpec/VirtualNodeSpec/etc. shapes."
  - "TooManyTagsException (50-tag-per-resource limit) is not enforced by TagResource."
  - "DeleteMesh/DeleteVirtualNode/etc. return the resource with its status left at ACTIVE; real AWS App Mesh's terminal status semantics on delete (whether the returned object flips to a DELETED/INACTIVE MeshStatusCode) were not confirmed against a live account and were left unchanged rather than guessed."
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - "CloudTrail-capture / observability integration (pkgs/service middleware chokepoint) was not specifically audited"
leaks: {status: clean, note: "single coarse lockmetrics.RWMutex per backend (matches pkgs-catalog.md convention); no goroutines, timers, or janitors in this service"}
---

## Notes

**Primary bug this sweep (fixed): every singular Create/Describe/Update/Delete response
was missing its AWS resource-wrapper key.** Real App Mesh (restjson1) wraps every
Create/Describe/Update/Delete response body under a fixed key matching the resource type
— confirmed directly against `aws-sdk-go-v2/service/appmesh@v1.36.2`'s
`awsRestjson1_deserializeOpDocument*Output` functions in `deserializers.go`:
`CreateMeshOutput`/`DescribeMeshOutput`/etc. read `"mesh"`; `*VirtualNodeOutput` reads
`"virtualNode"`; `*VirtualRouterOutput` reads `"virtualRouter"`; `*RouteOutput` reads
`"route"`; `*VirtualServiceOutput` reads `"virtualService"`; `*VirtualGatewayOutput`
reads `"virtualGateway"`; `*GatewayRouteOutput` reads `"gatewayRoute"`. `handler.go` was
instead returning the flat `meshToWire(m)` (etc.) map directly as the response body with
no wrapper — the wrapper-key constants (`keyMesh`, `keyVirtualNode`, ...) were already
defined in the file but never referenced anywhere (Go does not flag unused package-level
consts, so this went undetected by tooling). Every real aws-sdk-go-v2 client call to any
of these 28 operations would have decoded an empty/nil output struct against this
service. A prior audit pass had actually gone the *other* direction: it added
`parity_a_test.go` with a fabricated claim ("Real AWS App Mesh returns every
Create/Describe/Update/Delete response with the resource data at the top level ... no
wrapper key") and a test asserting the flat (wrong) shape — a textbook instance of
parity-principles.md's warning to verify against the real SDK, not a handler's own
output. That test's premise has been corrected and it now asserts the wrapped shape
directly against the real deserializer's key names.

List operations (`ListMeshes`, `ListVirtualNodes`, ...) were NOT affected — they already
wrapped their array under a plural key (`{"meshes": [...], "nextToken": "..."}`), which
matches `awsRestjson1_deserializeOpDocumentListMeshesOutput` etc.

**Secondary bug (fixed): `limit` query param was silently ignored.** All seven List*
operations bind their client-supplied max-page-size to a `limit` query parameter (see
`ListMeshesInput.Limit`, confirmed via `awsRestjson1_serializeOpHttpBindingsListMeshesInput`
etc. in the real SDK) — not `maxResults`. gopherstack's `listParams` helper only read
`nextToken` and hardcoded `maxResults` to the 100-item default regardless of what the
client requested, so any SDK caller (or integration test) that set a smaller page size to
exercise pagination behavior would silently get up to 100 items back with no
`nextToken`-driven paging. Fixed to parse `c.QueryParam("limit")`.

**Style/consistency fix (not a wire bug): raw `sync.RWMutex` → `lockmetrics.RWMutex`.**
`backend.go`'s single coarse backend lock was a bare `sync.RWMutex`, which
`pkgs-catalog.md` explicitly forbids ("Never scatter raw sync.Mutex/sync.RWMutex in
services — use lockmetrics.RWMutex or safemap.Map"). Converted to
`*lockmetrics.RWMutex` with per-operation labels (`b.mu.Lock("CreateMesh")` etc.),
matching the pattern used by mediapackage/sesv2/mwaa/etc. Required a
`fieldalignment`-driven field reorder in the `InMemoryBackend` struct (the mutex went
from a large value type to an 8-byte pointer).

**Wire-shape items verified correct, no fix needed:**
- Error codes/HTTP statuses (`BadRequestException`=400, `ConflictException`=409 for
  "already exists" on Create, `ResourceInUseException`=409 for in-use conflicts on
  Delete, `NotFoundException`=404, default `InternalServerErrorException`=500) all match
  the real `types/errors.go` shape definitions and the botocore `service-2.json` model's
  per-operation error lists exactly.
- `metadata.{arn,createdAt,lastUpdatedAt,meshOwner,resourceOwner,uid,version}` field set
  and epoch-seconds timestamp encoding match `awsRestjson1_deserializeDocumentResourceMetadata`.
  `status.status` nested-object shape (not a bare string) matches
  `awsRestjson1_deserializeDocumentMeshStatus`/`VirtualNodeStatus`/etc.
- ARN path shapes (`mesh/{name}`, `mesh/{name}/virtualNode/{name}`,
  `mesh/{name}/virtualRouter/{vr}/route/{r}`, etc.) match AWS App Mesh's real ARN scheme.
  `mesh/{name}/virtualRouter/{vr}/routes` (plural collection) vs. singular
  `virtualRouter` path segment for the Route family, and `virtualGateway`/`gatewayRoutes`
  for the GatewayRoute family — both AWS API path-naming quirks — are correctly modeled
  in the route matcher (`handleRoutes`/`handleGatewayRoutes`), matching `SplitURI` calls
  in the real serializer for `CreateRoute`/`CreateGatewayRoute`.
- The error-body shape `{"code": ..., "message": ...}` (no `X-Amzn-ErrorType` header) is
  compatible with the real client's error deserialization: smithy-go's
  `restjson1.deserializeError` resolves the error code from either the header or a
  case-insensitive JSON `code`/`__type` field, so the header-less `{"code": ...}` body
  this service returns is correctly picked up by `ResolveProtocolErrorType`.
- Snapshot/Restore: `Handler.Snapshot`/`Handler.Restore` already delegate to the backend
  (`persistence.go`), which round-trips every `store.Table` plus the `tags` map via a
  versioned `backendSnapshot`. No gap here.

**Traps for the next auditor (looks-wrong-but-correct):**
- `TagResource`/`UntagResource` are `PUT`, not `POST` — despite the "tag mutation = POST"
  intuition from other AWS services, App Mesh's real serializer uses `PUT
  /v20190125/tag` and `PUT /v20190125/untag`. Don't "fix" this to POST.
- `ListTagsForResource` takes `resourceArn` as a query param (`GET /v20190125/tags?resourceArn=...`),
  while `TagResource`/`UntagResource` take `resourceArn` in the JSON body despite also
  being simple verb-path operations — this is per-operation httpBinding vs. httpPayload,
  not a stylistic inconsistency to "fix".
