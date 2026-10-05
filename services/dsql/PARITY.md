---
service: dsql
sdk_module: aws-sdk-go-v2/service/dsql@v1.22.1
last_audit_commit: 5bb0d02ee
last_audit_date: 2026-10-05
overall: A            # A: SDK-driven test/integration suite TestIntegration_DSQL_ClusterLifecycle + TestIntegration_DSQL_MultiRegionPeering (test/integration/dsql_test.go) + every buildable gap closed; remaining divergences are structural_gaps.
ops:
  CreateCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "CREATING, lazily flips to ACTIVE on next read after a short fixed deadline (750ms); a multi-Region cluster settles to PENDING_SETUP until peering completes; multiRegionProperties.clusters peers must exist, be in another Region and share the witness Region"}
  GetCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  ListClusters: {wire: ok, errors: ok, state: ok, persist: ok, note: "opaque nextToken via pkgs/page"}
  UpdateCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "UPDATING, lazily flips back to ACTIVE; multiRegionProperties.clusters peers are validated, and a mutual link (each cluster lists the other) moves both PENDING_SETUP clusters to ACTIVE; KmsEncryptionKey=AWS_OWNED_KMS_KEY reverts to the AWS-owned key per SDK doc comment"}
  DeleteCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "DeletionProtectionEnabled blocks delete (ValidationException, reason=deletionProtectionEnabled); otherwise DELETING, lazily removed on next read; the cluster is unlinked from every peer's multiRegionProperties.clusters"}
  GetClusterPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  PutClusterPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "expectedPolicyVersion optimistic lock; bypassPolicyLockoutSafetyCheck accepted but not evaluated -- see structural_gaps"}
  DeleteClusterPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "expectedPolicyVersion optimistic lock"}
  GetVpcEndpointServiceName: {wire: ok, errors: ok, state: ok, persist: ok, note: "wire-shaped names only; no real PrivateLink plane -- see structural_gaps"}
  CreateStream: {wire: ok, errors: ok, state: ok, persist: ok, note: "CREATING, lazily flips to ACTIVE on next read"}
  GetStream: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteStream: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED 2026-10-01: marks DELETING (StreamStatusDeleting), lazily removed after 500ms; owned streams are also removed when their cluster is purged"}
  ListStreams: {wire: ok, errors: ok, state: ok, persist: ok, note: "opaque nextToken via pkgs/page"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "cluster ARN only, per SDK doc comment"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  Cluster: {status: ok, note: "Create/Get/List/Update/Delete verified end-to-end against the real aws-sdk-go-v2 client over an httptest server -- wire shapes (lowerCamelCase JSON, unlike most restjson1 services in this repo), epoch creationTime, ARN format (arn:aws:dsql:region:account:cluster/id), the <id>.dsql.<region>.on.aws endpoint format, multiRegionProperties/witnessRegion round-trip, deletionProtectionEnabled enforcement, and error deserialization (ResourceNotFoundException/ValidationException/ServiceQuotaExceededException) all round-trip cleanly."}
  ClusterPolicy: {status: ok, note: "Get/Put/Delete round-trip the policy document and an opaque policyVersion token; PutClusterPolicy/DeleteClusterPolicy both honor expectedPolicyVersion (ConflictException on mismatch), matching the SDK's optimistic-concurrency doc comments."}
  Stream: {status: ok, note: "Create/Get/Delete/List verified against the real client; streamIdentifier is scoped to its owning cluster (composite table key), matching the /stream/{clusterId}/{streamId} wire path. Only the Kinesis targetDefinition variant exists in the pinned SDK, so that's the only one implemented."}
  Tags: {status: ok, note: "One generic tag family keyed by cluster ARN, matching real AWS (TagResource/UntagResource/ListTagsForResource operate on dsql:cluster resources only)."}
gaps: []
items_still_open: []
structural_gaps:
  - "No PostgreSQL data plane: a cluster's <id>.dsql.<region>.on.aws endpoint is wire-shaped but nothing listens, so SQL cannot be run. Authentication tokens (the SDK's feature/dsql/auth GenerateDbConnectAuthToken) are client-side SigV4 presigns, not an API operation, and need no server support."
  - "PutClusterPolicy's bypassPolicyLockoutSafetyCheck is stored but not evaluated: the lockout check asks whether the calling principal keeps access, and the emulator has no authenticated caller identity or IAM policy evaluation (same stance as services/kms)."
  - "GetVpcEndpointServiceName returns wire-shaped names only: there is no PrivateLink / VPC endpoint plane to back them."
deferred: []
leaks: {status: clean, note: "leak_main_test.go runs goleak over the package; cluster and stream transitions are lazy deadlines evaluated on read, no goroutines or timers."}
---

## Notes

Initial implementation (2026-09-26, bd issue gopherstack-7r6bz): control-plane
REST-JSON API modeled after services/kinesisvideo (package layout,
lockmetrics, persistence) and services/kafka (manual method+path routing,
since DSQL is a real path-parameter REST API: /cluster/{id},
/cluster/{id}/policy, /stream/{clusterId}/{streamId}, unlike kinesisvideo's
flat action-named paths). Every operation mutates/reads real in-memory state
via pkgs/store.Table + pkgs/lockmetrics.RWMutex, with JSON snapshot/restore
wired into pkgs/persistence (additive inventory row in
pkgs/persistence/testdata/snapshot_inventory.json).

Wire shapes, HTTP methods/paths, and error codes (ConflictException 409,
ResourceNotFoundException 404, ValidationException 400,
ServiceQuotaExceededException 402) were verified against the pinned
aws-sdk-go-v2/service/dsql@v1.22.1 request_snapshot/*.snap and
response_snapshot/*.snap fixtures (the module's own smithy-generated
request/response byte fixtures), not just the Go struct definitions. DSQL is
unusual among this repo's restjson1 services in emitting lowerCamelCase JSON
field names (clientToken, deletionProtectionEnabled, ...) rather than
PascalCase.

Two pre-existing, unrelated services' RouteMatchers over-claimed a path
prefix DSQL's real wire shape also needs, both fixed by guarding the other
service's claim rather than raising DSQL's MatchPriority (per
.claude/memories -- route-matcher-prefix-collision):

- Inspector2 unconditionally claimed the "/cluster/" prefix for its one real
  operation (POST /cluster/get); added to its existing
  ambiguousRouteMatchPrefixes map (the same mechanism it already uses for
  /findings/, /members/, /configuration/), gated by its existing
  isInspector2Request helper.
- EKS unconditionally claimed the "/clusters/" prefix; DSQL's
  GetVpcEndpointServiceName lives at the real wire path
  /clusters/{id}/vpc-endpoint-service-name (plural "clusters", unlike every
  other DSQL cluster operation's singular "/cluster/{id}"), confirmed against
  request_snapshot/GetVpcEndpointServiceName.request.snap. EKS has no such
  operation, so that exact suffix is carved out via a new
  isDSQLVpcEndpointServiceNamePath helper.

`go run ./cmd/routecollisions` before/after: both new collisions are
(guarded/guarded); no new unguarded collisions.

### 2026-10-01: DeleteStream DELETING state and orphaned streams

DeleteStream now returns and reports DELETING (types.StreamStatusDeleting) before lazy removal, and a purged cluster takes its streams with it (previously GetStream kept resolving streams of a gone cluster and they leaked). Proven by TestDeleteStream and TestDeleteCluster_RemovesOwnedStreams.

2026-10-01: reqfielddiff flags `ListStreams.MaxResults` as undeclared but it is read (`max-results` query, schema-bound in dsql@v1.22.1); locked by `TestListStreams_MaxResultsPaginates`.

### 2026-10-05: A grade, multi-Region peering

Grade moves B to A on `TestIntegration_DSQL_ClusterLifecycle` and `TestIntegration_DSQL_MultiRegionPeering` (test/integration/dsql_test.go, real SDK client against the Docker image) plus closing the one buildable gap the B grade named.

- Multi-Region peering is now modelled instead of echoed. A cluster created with multiRegionProperties settles to PENDING_SETUP (not ACTIVE) until a mutual link exists: cluster B created with `clusters=[A]` stays PENDING_SETUP, and UpdateCluster on A with `clusters=[B]` completes the handshake and moves both to ACTIVE. Peer ARNs are validated (malformed ARN or a peer with no witness Region is ValidationException, an unknown cluster ResourceNotFoundException, same-Region peer or witness-Region mismatch ValidationException), and DeleteCluster unlinks the cluster from every peer. The peer-validation error choices are this emulator's reading of the documented ValidationException/ResourceNotFoundException set; AWS publishes no per-case table. Covered by `TestMultiRegionPeeringLifecycle` and `TestMultiRegionPeerValidation` (multiregion_test.go). A surviving peer is not demoted back to PENDING_SETUP when its partner is deleted: no source documents that transition.
- The 750ms/500ms lifecycle deadlines are a deliberate compression of real provisioning time, not an open gap: every state is reachable and observable (CREATING, UPDATING, DELETING, PENDING_SETUP), clients poll them normally.
- The remaining divergences are recorded as structural_gaps (no SQL engine, no caller identity for the lockout check, no PrivateLink plane).
