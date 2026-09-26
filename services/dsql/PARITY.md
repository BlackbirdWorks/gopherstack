---
service: dsql
sdk_module: aws-sdk-go-v2/service/dsql@v1.22.1
last_audit_commit: 5a0bc403e  # HEAD at audit time, pre-commit
last_audit_date: 2026-09-26
overall: B            # new service, control plane only, unit-tested against the real SDK client
ops:
  CreateCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "CREATING, lazily flips to ACTIVE on next read after a short deadline -- see items_still_open"}
  GetCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  ListClusters: {wire: ok, errors: ok, state: ok, persist: ok, note: "opaque nextToken via pkgs/page"}
  UpdateCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "UPDATING, lazily flips back to ACTIVE; KmsEncryptionKey=AWS_OWNED_KMS_KEY reverts to the AWS-owned key per SDK doc comment"}
  DeleteCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "DeletionProtectionEnabled blocks delete (ValidationException, reason=deletionProtectionEnabled); otherwise DELETING, lazily removed on next read"}
  GetClusterPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  PutClusterPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "expectedPolicyVersion optimistic lock; bypassPolicyLockoutSafetyCheck accepted but not evaluated -- see items_still_open"}
  DeleteClusterPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "expectedPolicyVersion optimistic lock"}
  GetVpcEndpointServiceName: {wire: ok, errors: ok, state: ok, persist: ok, note: "wire-shaped names only; no real PrivateLink plane -- see items_still_open"}
  CreateStream: {wire: ok, errors: ok, state: ok, persist: ok, note: "CREATING, lazily flips to ACTIVE on next read"}
  GetStream: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteStream: {wire: ok, errors: ok, state: ok, persist: ok, note: "removed immediately -- see items_still_open"}
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
items_still_open:
  - "CREATING/UPDATING/DELETING transient cluster and stream states use a short, fixed lazy
    deadline (750ms for clusters, 500ms for streams) rather than a background reconciler or an
    AWS-realistic multi-second/multi-minute provisioning time: a client that reads twice in a
    row observes the terminal state almost immediately. This is a deliberate simplification
    (explicitly authorized as either an honest immediate-ACTIVE or a lazy deadline) chosen so
    terraform-provider-aws's ClusterActiveWaiter/ClusterNotExistsWaiter (2s minimum poll
    interval) always observes the terminal state on their very first poll."
  - "PutClusterPolicy's bypassPolicyLockoutSafetyCheck is accepted and stored on the wire
    request but never evaluated: real AWS parses the policy document and refuses to apply one
    that would lock the caller out of the cluster unless this flag is set. This backend has no
    IAM policy evaluation engine (no service in this repo does), so every PutClusterPolicy call
    succeeds regardless of the flag -- structural, out of scope for this pass."
  - "GetVpcEndpointServiceName returns wire-shaped serviceName/clusterVpcEndpoint values but
    there is no real VPC/PrivateLink plane behind them -- structural, matches how every other
    VPC-endpoint-service-name-style operation in this repo (e.g. services/rds) is handled."
  - "DeleteStream removes the stream synchronously rather than lingering through a DELETING
    state first: real AWS's StreamStatus enum includes DELETING, but nothing else in this
    backend or in terraform-provider-aws observes a stream's intermediate delete state, so this
    is behaviorally equivalent for any client that only checks for ResourceNotFoundException
    afterward."
  - "Multi-Region peering (multiRegionProperties.clusters) is stored and echoed back exactly as
    given but not enforced: creating/updating a cluster with peer cluster ARNs does not
    validate that those peers exist or reciprocally link back to this cluster. Each dsql
    backend instance is a single account/region process, matching how every other
    multi-region-aware service in this repo (e.g. services/dynamodbstreams's global tables)
    treats cross-region state as opaque input."
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
