---
service: dsql
sdk_module: aws-sdk-go-v2/service/dsql@v1.22.3
last_audit_commit: 5bb0d02ee
last_audit_date: 2026-10-10
overall: A            # A: SDK-driven test/integration suite TestIntegration_DSQL_ClusterLifecycle + TestIntegration_DSQL_MultiRegionPeering (test/integration/dsql_test.go) + every buildable gap closed; remaining divergences are structural_gaps.
ops:
  CreateCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "a replayed clientToken returns the same cluster; CREATING, lazily flips to ACTIVE on next read after a short fixed deadline (750ms); a multi-Region cluster settles to PENDING_SETUP until peering completes; multiRegionProperties.clusters peers must exist, be in another Region and share the witness Region"}
  GetCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  ListClusters: {wire: ok, errors: ok, state: ok, persist: ok, note: "opaque nextToken via pkgs/page"}
  UpdateCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "clientToken replays the recorded response; UPDATING, lazily flips back to ACTIVE; multiRegionProperties.clusters peers are validated, and a mutual link (each cluster lists the other) moves both PENDING_SETUP clusters to ACTIVE; KmsEncryptionKey=AWS_OWNED_KMS_KEY reverts to the AWS-owned key per SDK doc comment"}
  DeleteCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "clientToken (query client-token) replays the recorded response; DeletionProtectionEnabled blocks delete (ValidationException, reason=deletionProtectionEnabled); otherwise DELETING, lazily removed on next read; the cluster is unlinked from every peer's multiRegionProperties.clusters"}
  GetClusterPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  PutClusterPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "expectedPolicyVersion optimistic lock; bypassPolicyLockoutSafetyCheck accepted but not evaluated -- see structural_gaps; clientToken replays the recorded response (a reused token with different parameters is ConflictException)"}
  DeleteClusterPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "expectedPolicyVersion optimistic lock; clientToken (query client-token) replays the recorded response"}
  GetVpcEndpointServiceName: {wire: ok, errors: ok, state: ok, persist: ok, note: "wire-shaped names only; no real PrivateLink plane -- see structural_gaps"}
  CreateStream: {wire: ok, errors: ok, state: ok, persist: ok, note: "a replayed clientToken returns the same stream; CREATING, lazily flips to ACTIVE on next read"}
  GetStream: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteStream: {wire: ok, errors: ok, state: ok, persist: ok, note: "clientToken (query client-token) replays the recorded response. FIXED 2026-10-01: marks DELETING (StreamStatusDeleting), lazily removed after 500ms; owned streams are also removed when their cluster is purged"}
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
  - "Data plane opt-in (DSQL_ENGINE=docker, or --engines=auto/docker with a reachable runtime; default stub keeps the control plane only). The cluster endpoint <id>.dsql.<region>.on.aws is not resolvable and clients cannot assume port 5432: connect to host/port from GET /_gopherstack/dsql/clusters/{id}; tokens are generated for the control-plane endpoint (also accepted: endpoint:port, loopback:port). The CA is served at /_gopherstack/dsql/ca.pem."
  - "Cluster data is ephemeral: each data plane is a fresh postgres container with no volume, so a restart or snapshot restore re-creates clusters (CREATING until up) with an empty database."
  - "UNVERIFIED (recalled, not in the User Guide pages read): the 7-day maximum token lifetime (SigV4 limit used), the text of the login failure (28P01 'password authentication failed for user') and of unsupported-feature rejections (SQLSTATE 0A000 '<feature> is not supported in Aurora DSQL'); DSQL publishes neither."
  - "Unsupported-feature rejection covers CREATE TRIGGER, temporary tables, TRUNCATE, VACUUM, CREATE TABLESPACE, CREATE DATABASE and LANGUAGE plpgsql functions at the statement start only (not inside multi-statement strings or DO blocks). Foreign keys and sequences are accepted: the current User Guide documents both as supported (the bd issue text predates that). Plain CREATE INDEX is passed through, not rejected."
  - "CREATE INDEX ASYNC runs a plain (blocking) CREATE INDEX in a background job: CONCURRENTLY deadlocks with a session in sys.wait_for_job, so a failed unique build leaves no INVALID index (DSQL leaves one). Old completed/failed rows are pruned on the next submit as documented; job_id is 26 base32 characters as in the docs, object_id is the index oid once built."
  - "DSQL session rules are not enforced: Repeatable Read is not forced (container default Read Committed, so sys.wait_for_job inside a Repeatable Read transaction never sees the job finish), one DDL per transaction, the 3,000-row transaction limit, one-hour connection timeout, C collation."
  - "The cluster resource policy (PutClusterPolicy) is not evaluated at connect time; only the caller's identity policies are, and only when IAM enforcement is on. AWS IAM GRANT/REVOKE and sys.iam_pg_role_mappings are modelled (roles are logged in with an engine-private password), other sys.* views are absent."
  - "The postgres TLS front proxy duplicates the framing and startup code in services/rds/engine_proxy.go; a shared pkgs/ package is a follow-up."
structural_gaps:
  - "bypassPolicyLockoutSafetyCheck (CreateCluster, PutClusterPolicy) is accepted but not evaluated: the lockout check asks whether the calling principal keeps access, and the emulator has no authenticated caller identity or IAM policy evaluation (same stance as services/kms)."
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

## 2026-10-05 (gopherstack-uox6 pass 12, value semantics)

GetCluster/CreateCluster EncryptionDetails.KmsKeyArn now reports an ARN for a KmsEncryptionKey given as key ID or alias (CreateClusterInput.KmsEncryptionKey accepts ARN, ID or alias, api_op_CreateCluster.go:94-97; EncryptionDetails.KmsKeyArn, types.go:70). Proof: `TestCluster_KmsKeyReportedAsARN`. Recorded, unchanged: DeletionProtectionEnabled has no SDK-stated default (the API docs say true), so it stays false when omitted.

## 2026-10-05 (zeroguard omitted-vs-zero audit)

Tool false positives (cmd/zeroguard): required path/identifier members (Name, *Id, *Arn), Put* operations that replace the whole resource (PutRule, PutPermission, PutResourcePolicy, PutCodeBinding), and PatchOperations-based Update* ops.

### 2026-11-01: PostgreSQL data plane (gopherstack-49qct.79)

Docker engine (`services/dsql/engine*.go`, `proxy.go`, `bootstrap.go`, `jobs.go`, `shim.go`, `iam_auth.go`): one postgres container per cluster via pkgs/pgengine plus a TLS proxy on a portalloc port. A cluster stays CREATING until the container accepts connections (and the usual deadline has passed), turns FAILED if the launch fails, and DeleteCluster/Reset/shutdown remove the container (deletion protection keeps it). Proven by `TestEngineActivation`/`TestEngineTeardown` (fake launcher, fake clock) and `TestDockerDataPlane` (real container, pgx, token from github.com/aws/aws-sdk-go-v2/feature/dsql/auth).

Sources: token format is feature/dsql/auth@v1.2.8 auth_token_generator.go (SigV4 query presign of https://<endpoint>/?Action=DbConnect|DbConnectAdmin, signing name dsql, 900s default expiry, scheme stripped). User Guide pages: working-with-create-index-async (CREATE [UNIQUE] INDEX ASYNC returns job_id; sys.jobs columns job_id, status, details, job_type, class_id, object_id, object_name, start_time, update_time; statuses submitted/processing/failed/completed; sys.wait_for_job returns a Boolean; unique failure detail 'Found duplicate key while validating index for UCVs'; completed/failed jobs pruned after 30 minutes), using-database-and-iam-roles (admin role with dsql:DbConnectAdmin, other roles with dsql:DbConnect plus AWS IAM GRANT/REVOKE and sys.iam_pg_role_mappings), working-with-postgresql-compatibility-migration-guide (single built-in database postgres; no temporary tables, triggers, PL/pgSQL, TRUNCATE; VACUUM system-managed; sslmode=require clients).

Auth design: the proxy asks for a cleartext password (only after TLS), parses it as a DSQL token, and checks the cluster host (or loopback:port), region, expiry/skew, role-to-action match (DbConnectAdmin only for `admin`, DbConnect only for other roles), the SigV4 signature against the secret in the emulator's IAM or STS credential store, the caller's dsql:DbConnect[Admin] identity policy when IAM enforcement is on, and for non-admin roles an AWS IAM GRANT mapping. It then logs in to the container with an engine-private password; client passwords never reach the database. Non-token passwords are rejected (DSQL has no passwords); there is no plain-password mode.

### 2026-10-10 (realism pass)

- Path identifiers must match `^[a-z0-9]{26}$` (ValidationException); ListClusters/ListStreams reject `max-results` outside 1..100 and opaque-token garbage; tags reject `aws:` keys, keys over 128 and values over 256 chars; policies must be JSON objects.
- ResourceNotFoundException now carries `resourceId`/`resourceType`.
- `--lifecycle-dsql` / `DSQL_LIFECYCLE_DELAY` overrides the CREATING/UPDATING/DELETING windows (0 keeps the built-in 750ms clusters, 500ms streams).
