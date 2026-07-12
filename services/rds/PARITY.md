# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: rds
sdk_module: aws-sdk-go-v2/service/rds@v1.116.2
last_audit_commit: dad3e28d
last_audit_date: 2026-07-11
overall: B+           # already-accurate on nearly all of ~163 routed ops (6+ prior audit
                       # passes: batch1-3, refinement1-4, sweeps 2-3, #2213/#2226/#2227/#2329/
                       # #2334/#2339/#2380/#2381/#2382); services/rds had zero local drift
                       # since the prior audit (ce30166a) and the vendored SDK version is
                       # unchanged (v1.116.2, all 163 ops still routed), so this pass targeted
                       # exactly the items the prior ledger flagged as "spot-checked, not
                       # re-verified op-by-op" (DB shard groups, zero-ETL integrations) and
                       # found a real, previously-unnoticed wire-shape bug affecting 10 ops.
ops:
  DeleteDBInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDBCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeDBInstances: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDBInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyDBInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  StartDBInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  StopDBInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  RebootDBInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDBInstanceReadReplica: {wire: ok, errors: ok, state: ok, persist: ok}
  PromoteReadReplica: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDBCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyDBCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeDBClusters: {wire: ok, errors: ok, state: ok, persist: ok, note: "no Filters support — gap"}
  CreateDBSnapshot: {wire: ok, errors: ok, state: ok, persist: ok}
  CopyDBSnapshot: {wire: ok, errors: ok, state: ok, persist: ok}
  RestoreDBInstanceFromDBSnapshot: {wire: ok, errors: ok, state: ok, persist: ok}
  RestoreDBInstanceToPointInTime: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDBClusterSnapshot: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDBParameterGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyDBParameterGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  ResetDBParameterGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDBSubnetGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateOptionGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDBShardGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "wire nesting bug fixed this pass — see gaps/Notes"}
  DeleteDBShardGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "wire nesting bug fixed this pass — see gaps/Notes"}
  ModifyDBShardGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "wire nesting bug fixed this pass — see gaps/Notes"}
  RebootDBShardGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "wire nesting bug fixed this pass — see gaps/Notes"}
  DescribeDBShardGroups: {wire: ok, errors: ok, state: ok, persist: ok, note: "list shape was already correct"}
  CreateIntegration: {wire: ok, errors: ok, state: ok, persist: ok, note: "wire nesting bug fixed this pass — see gaps/Notes"}
  DeleteIntegration: {wire: ok, errors: ok, state: ok, persist: ok, note: "wire nesting bug fixed this pass — see gaps/Notes"}
  ModifyIntegration: {wire: ok, errors: ok, state: ok, persist: ok, note: "wire nesting bug fixed this pass — see gaps/Notes"}
  DescribeIntegrations: {wire: ok, errors: ok, state: ok, persist: ok, note: "list shape was already correct"}
  CreateCustomDBEngineVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "wire nesting + wrong field name fixed this pass — see gaps/Notes"}
  DeleteCustomDBEngineVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "wire nesting bug fixed this pass — see gaps/Notes"}
  ModifyCustomDBEngineVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "wire nesting + wrong field name fixed this pass — see gaps/Notes"}
  CreateTenantDatabase: {wire: ok, errors: ok, state: ok, persist: ok, note: "verified this pass — real AWS output nests under <TenantDatabase>, matches gopherstack"}
  DeleteTenantDatabase: {wire: ok, errors: ok, state: ok, persist: ok, note: "verified this pass"}
  ModifyTenantDatabase: {wire: ok, errors: ok, state: ok, persist: ok, note: "verified this pass"}
  DescribeTenantDatabases: {wire: ok, errors: ok, state: ok, persist: ok, note: "verified this pass"}
  CreateDBSecurityGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "verified this pass — real AWS output nests under <DBSecurityGroup>, matches gopherstack"}
  AuthorizeDBSecurityGroupIngress: {wire: ok, errors: ok, state: ok, persist: ok, note: "verified this pass"}
  RevokeDBSecurityGroupIngress: {wire: ok, errors: ok, state: ok, persist: ok, note: "verified this pass"}
  ModifyCertificates: {wire: ok, errors: ok, state: ok, persist: ok, note: "verified this pass — nests under <Certificate>, matches"}
  ModifyDBRecommendation: {wire: ok, errors: ok, state: ok, persist: ok, note: "verified this pass — nests under <DBRecommendation>, matches"}
  CreateDBProxy/DeleteDBProxy/ModifyDBProxy: {wire: ok, errors: ok, state: ok, persist: ok, note: "spot-verified this pass — nests under <DBProxy>, matches (family already ok per prior audits)"}
  PurchaseReservedDBInstancesOffering: {wire: ok, errors: ok, state: ok, persist: ok, note: "spot-verified this pass — nests under <ReservedDBInstance>, matches"}
families:
  db_instance_lifecycle: {status: ok, note: "creating->available->modifying/deleting state machine via instanceReadyAt + self-terminating reconciler goroutine (backend.go scheduleReconcilerLocked); verified transitions, DeletionProtection guard, already-deleting guard"}
  db_cluster_lifecycle: {status: ok, note: "cluster members, reader/writer endpoint synthesis, ServerlessV2ScalingConfiguration, start/stop/failover/reboot all mutate real state"}
  snapshots_manual_automated: {status: ok, note: "CreateDBSnapshot/CopyDBSnapshot/Delete/Describe/Restore all real; SnapshotType manual vs automated distinguished; final-snapshot-on-delete gap fixed this pass (see Notes)"}
  parameter_groups: {status: ok, note: "apply-method immediate vs pending-reboot honored in ModifyDBParameterGroup/ApplyPendingMaintenanceAction path; Reset/Copy real"}
  subnet_groups: {status: ok, note: "CRUD verified against DBSubnetGroup shape"}
  option_groups: {status: ok, note: "CRUD + Copy + option add/remove real"}
  read_replicas: {status: ok, note: "source linkage bidirectional (ReplicaSourceDBInstanceIdentifier / ReadReplicaIdentifiers), promote clears linkage, cross-region replica path uses defaults when source not locally resolvable"}
  events_and_subscriptions: {status: ok, note: "ring-buffered Events (maxEvents cap prevents unbounded growth); EventSubscription CRUD + source-identifier add/remove real"}
  engine_versions_and_orderable_options: {status: ok, note: "DescribeDBEngineVersions/DescribeOrderableDBInstanceOptions/DescribeDBMajorEngineVersions all backed by real (small, static) catalogs — not a stub since callers get consistent, well-shaped data; no engine-name validation on Create (see gaps)"}
  tags: {status: ok, note: "AddTagsToResource/RemoveTagsFromResource/ListTagsForResource use pkgs/tags-style per-ARN map, cleaned up on every delete path (instance, cluster, snapshot, option group, param group, cluster endpoint — verified via TestRDSBackend_TagsCleanedUpOnDelete table)"}
  pagination: {status: ok, note: "Marker/MaxRecords via pkgs/page.Page[T] (paginateDescribe) — consistent across all Describe* ops"}
  describe_filters: {status: partial, note: "DescribeDBInstances Filters (db-cluster-id/db-instance-id/dbi-resource-id/domain/engine) added this pass; other Describe ops (DescribeDBClusters, DescribeDBSnapshots, DescribeDBClusterSnapshots, DescribeEvents) still ignore Filters — see gaps"}
  global_clusters: {status: ok, note: "Create/Modify/Delete/Describe + Remove/Failover/SwitchoverGlobalCluster real"}
  blue_green_deployments: {status: ok, note: "Create/Describe/Delete/Switchover real (refinement1)"}
  db_proxies: {status: ok, note: "proxy/proxy-target/proxy-target-group/proxy-endpoint CRUD real (refinement3)"}
  reserved_instances: {status: ok, note: "Purchase + Describe(Offerings) real"}
  performance_insights: {status: ok, note: "GetPerformanceInsightsMetrics requires seeded data via SetPerformanceInsightsData — not a fabricated-on-the-fly stub; batch3_test.go.rej/.patch cruft from a prior sweep's already-applied fix removed this pass"}
  error_codes: {status: ok, note: "awserr sentinels map to correct AWS fault codes (DBInstanceNotFound, DBInstanceAlreadyExists, InvalidDBInstanceState, DBSnapshotNotFound, InvalidParameterValue, InvalidParameterCombination) with correct HTTP status via errCodeLookup in handler.go"}
  leaks: {status: ok, note: "single reconciler goroutine per backend; self-terminates when instanceReadyAt/clusterReadyAt both empty (no ticker leak); no unbounded maps found — events ring-buffered at maxEvents"}
  db_shard_groups: {status: ok, note: "Aurora Limitless shard groups — CRUD + Reboot real state; wire-shape bug (extra nesting) on Create/Delete/Modify/Reboot FIXED this pass, see gaps/Notes"}
  integrations: {status: ok, note: "zero-ETL Redshift integrations — CRUD real state; wire-shape bug (extra nesting) on Create/Delete/Modify FIXED this pass, see gaps/Notes"}
  custom_db_engine_versions: {status: ok, note: "wire-shape bug (extra nesting + wrong field name for description) on Create/Delete/Modify FIXED this pass, see gaps/Notes"}
  tenant_databases: {status: ok, note: "re-verified this pass against the real SDK's CreateTenantDatabaseOutput/DeleteTenantDatabaseOutput/ModifyTenantDatabaseOutput shapes (these DO nest under <TenantDatabase>, unlike shard groups/integrations) — no bug found, ledger's prior 'spot-checked only' caveat is now resolved to ok"}
  db_security_groups: {status: ok, note: "re-verified this pass (EC2-Classic legacy) — CreateDBSecurityGroupOutput/AuthorizeDBSecurityGroupIngressOutput/RevokeDBSecurityGroupIngressOutput all nest under <DBSecurityGroup> in the real SDK, matches gopherstack; no bug found, ledger's prior 'spot-checked only' caveat is now resolved to ok"}
gaps:
  - DescribeDBClusters, DescribeDBSnapshots, DescribeDBClusterSnapshots, DescribeEvents still ignore Filters (DescribeDBInstances Filters support was added in a prior pass) — not fixed this pass (scope/time); follow-up under gopherstack-bgl
  - DB instance/cluster/snapshot/parameter-group identifiers are compared case-sensitively (Go map keys); real AWS treats DBInstanceIdentifier etc. as case-insensitive (e.g. creating "MyDB" then "mydb" should collide with DBInstanceAlreadyExistsFault but does not here) — not fixed: normalizing would touch every resource map across ~30K LOC and was judged too invasive for a scoped, low-risk pass; flagged for a dedicated follow-up
  - CreateDBInstance/CreateDBCluster do not validate the Engine name against a known-engine list; any string is accepted (real AWS returns InvalidParameterValue for an unsupported engine) — not fixed: many existing tests rely on the current permissive behavior with ad hoc engine strings; changing this is a larger, separately-scoped hardening task
  - CreateDBShardGroup/DeleteDBShardGroup/ModifyDBShardGroup/RebootDBShardGroup and CreateIntegration/DeleteIntegration/ModifyIntegration and CreateCustomDBEngineVersion/DeleteCustomDBEngineVersion/ModifyCustomDBEngineVersion (10 ops total) previously wrapped their response fields one XML level too deep (e.g. `<CreateDBShardGroupResult><DBShardGroup><DBShardGroupIdentifier>...`) when the real aws-sdk-go-v2 output for all 10 is a FLAT shape with no such wrapper (`<CreateDBShardGroupResult><DBShardGroupIdentifier>...`) — FIXED this pass, see Notes. A real aws-sdk-go-v2 client's query-XML deserializer only looks for named fields as direct children of the `<XxxResult>` element, so every field on these 10 ops (including the identifier needed to address the resource in a follow-up call) previously came back empty/zero to a real SDK client, even though the emulator's backend state was correct.
  - CreateCustomDBEngineVersion/ModifyCustomDBEngineVersion additionally serialized the description field under the wrong element name (`DatabaseInstallationFilesS3BucketName` instead of `DBEngineVersionDescription`) — FIXED this pass alongside the nesting fix, see Notes.
deferred:
  - Activity streams (StartActivityStream/StopActivityStream/ModifyActivityStream) — spot-checked only, not re-verified against the real SDK wire shape this pass (scope/time); given the wire-shape bug class found in shard-groups/integrations this pass, this family should be prioritized in the next audit
  - DB shard groups / integrations (Aurora Limitless / zero-ETL) — CRUD *state* logic was previously spot-checked as real (not a stub); this pass went further and verified wire shape op-by-op against the real SDK, finding and fixing the nesting bug above. Field *coverage* is still partial (e.g. Integration doesn't model Tags/KMSKeyId/CreateTime/Errors, DBShardGroup doesn't model DBShardGroupArn/DBShardGroupResourceId/PubliclyAccessible on the wire) — not fixed, judged lower priority than the nesting bug since partial-but-correctly-shaped field coverage is a common, accepted pattern elsewhere in this emulator (e.g. DBInstance doesn't model every AWS field either)
leaks: {status: clean, note: "reconciler goroutine (backend.go:scheduleReconcilerLocked) is per-backend, started lazily, and exits its own loop once both instanceReadyAt and clusterReadyAt are empty; Close() is a documented no-op given this. No time.Sleep/unbounded-map patterns found in non-test files."}

## Notes

- Protocol: RDS uses the AWS Query (XML) protocol, version `2014-10-31`, XML namespace
  `http://rds.amazonaws.com/doc/2014-10-31/`. Every response wraps in
  `<ActionResponse><ActionResult>...</ActionResult></ActionResponse>` except where the op's
  SDK output has no members (in which case an empty result element is still correct — do not
  flag as a stub).

- **DeleteDBInstance / DeleteDBCluster final-snapshot contract (fixed this pass).** Real AWS
  requires exactly one of `SkipFinalSnapshot=true` or a non-empty `FinalDBSnapshotIdentifier`
  (`FinalDBClusterSnapshotIdentifier` for clusters); supplying both is
  `InvalidParameterCombination`, as is supplying neither. Before this pass the emulator's
  `DeleteDBInstance`/`DeleteDBCluster` took only an identifier and silently behaved as if
  `SkipFinalSnapshot=true` always — no validation, and no final snapshot was ever created even
  when a client explicitly asked for one. This is exactly the "disguised stub" bug class from
  `.claude/memories/parity-principles.md` #4: the delete itself was real (removed real state),
  but a whole documented, commonly-exercised parameter contract was silently ignored.
  Fixed by adding `DeleteDBInstanceWithOptions`/`DeleteDBClusterWithOptions` (additive — the
  existing `DeleteDBInstance(id)`/`DeleteDBCluster(id)` single-arg methods are kept unchanged
  and now delegate with `skipFinalSnapshot=true` since they are called by
  `services/cloudformation/resources_phase2.go` and `resources_phase4.go`, which are outside
  this audit's edit scope). AWS resolves the target resource before validating the snapshot
  parameter combination — a delete against a nonexistent instance/cluster returns
  `DBInstanceNotFound`/`DBClusterNotFound` even when `SkipFinalSnapshot`/
  `FinalDBSnapshotIdentifier` are also invalid or missing; order the checks accordingly (existence
  first) or existing/incoming client integration tests that intentionally omit both params
  against a missing resource will regress to the wrong error code.

- **DescribeDBInstances Filters (fixed this pass, partial).** AWS's DescribeDBInstances
  accepts `Filters.Filter.N.Name` / `Filters.Filter.N.Values.member.M` with recognized names
  `db-cluster-id`, `db-instance-id`, `dbi-resource-id`, `domain`, `engine`. Multiple values
  within one filter OR together; multiple filters AND together. An unrecognized filter name
  is `InvalidParameterValue`. `domain` is accepted (to avoid rejecting otherwise-valid client
  requests) but is not modeled as a real predicate since this emulator has no Directory
  Service domain-membership state — every instance vacuously "matches" a domain filter. The
  same Filters shape is documented on `DescribeDBClusters`, `DescribeDBSnapshots`,
  `DescribeDBClusterSnapshots`, and `DescribeEvents` but was out of scope to also implement
  this pass; see gaps.

- **`newManualSnapshotLocked` / `newManualClusterSnapshotLocked`.** Both `CreateDBSnapshot`/
  `CreateDBClusterSnapshot` and the new delete-time final-snapshot path build the same
  `DBSnapshot`/`DBClusterSnapshot` shape, so the struct-building logic was extracted into a
  `*Locked` helper (caller must already hold `b.mu`) shared by both call sites — avoids
  duplicating the AWS shape twice and risking the two copies drifting apart.

- **Case-sensitive identifiers (not fixed, documented gap).** Every resource map in
  `backend.go` (`instances`, `clusters`, `snapshots`, `parameterGroups`, ...) is keyed by the
  identifier string exactly as given. Real RDS identifiers are case-insensitive persistent
  handles (AWS lower-cases them internally), so `CreateDBInstance("MyDB", ...)` followed by
  `CreateDBInstance("mydb", ...)` should collide with `DBInstanceAlreadyExistsFault` in real
  AWS but creates two independent instances here. This is a real, if narrow, wire-behavior gap
  found while auditing `CreateDBInstance`/`DeleteDBInstance`, but normalizing it correctly
  touches every map (create/lookup/delete) across instances, clusters, snapshots, parameter
  groups, option groups, subnet groups, global clusters, etc. — a change of that breadth
  deserves its own focused, fully-tested pass rather than a partial fix bundled into this one
  (a half-normalized service, where instance IDs fold case but snapshot IDs don't, would be
  worse than the current consistent-but-wrong behavior).

- The stray `batch3_test.go.rej` / `batch3_test_pi.patch` files (tracked in git, dated the day
  before this audit) were leftover artifacts of a previously-applied patch — diffed against
  `batch3_test.go` and confirmed the patch's content (deterministic-vs-flaky
  `TestPerformanceInsights_*` fixes) was already live in the tracked test file. Removed as
  dead weight; not a behavior change.

- **DBShardGroup / Integration / CustomDBEngineVersion single-object response nesting bug
  (fixed this pass).** `services/rds/handler_completeness.go` modeled the responses for
  `CreateDBShardGroup`, `DeleteDBShardGroup`, `ModifyDBShardGroup`, `RebootDBShardGroup`,
  `CreateIntegration`, `DeleteIntegration`, `ModifyIntegration`, `CreateCustomDBEngineVersion`,
  `DeleteCustomDBEngineVersion`, and `ModifyCustomDBEngineVersion` the same way as the
  well-established `CreateDBInstance`/`CreateDBCluster`/etc. pattern: a scalar wrapper struct
  nested one level under the result, e.g.
  `xml:"CreateDBShardGroupResult>DBShardGroup"`. That pattern is correct for DBInstance/
  DBCluster/DBSnapshot/etc. because `CreateDBInstanceOutput` genuinely nests its payload under
  a `DBInstance *types.DBInstanceType` field. But `CreateDBShardGroupOutput`,
  `CreateIntegrationOutput`, and `CreateCustomDBEngineVersionOutput` (verified directly against
  `aws-sdk-go-v2/service/rds@v1.116.2`'s `api_op_*.go` output structs and `deserializers.go`)
  are all **flat** — `ComputeRedundancy`, `DBShardGroupIdentifier`, `IntegrationName`, `Engine`,
  etc. sit directly on the output struct, not inside a nested sub-object. Confirmed against
  `awsAwsquery_deserializeOpDocumentCreateDBShardGroupOutput` in `deserializers.go`: the
  generated deserializer's `switch` only matches field names as *direct children* of the
  `<CreateDBShardGroupResult>` node; an unrecognized child element name (like a stray
  `<DBShardGroup>` wrapper) falls through unmatched and its entire subtree — including the real
  field values one level deeper — is silently skipped. A real aws-sdk-go-v2 client calling any
  of these 10 ops against the old code would therefore get back a `*XxxOutput` with every field
  zero-valued, including the identifier fields (`DBShardGroupIdentifier`, `IntegrationName`,
  `Engine`/`EngineVersion`) client code typically needs from a Create response to address the
  resource in a follow-up call — a silent, high-impact wire break despite the backend state
  being entirely real (this is the "disguised stub" bug class from
  `.claude/memories/parity-principles.md` #2/#4: a real-looking nested-struct response that is
  wrong purely in its XML chain depth). `DescribeDBShardGroups`/`DescribeIntegrations` were
  NOT affected — those really do return a list (`DBShardGroups []types.DBShardGroup` /
  `Integrations []types.Integration`), so the existing `xmlDBShardGroupList`/
  `xmlIntegrationList` nesting is correct and was left unchanged; `toXMLDBShardGroup`/
  `toXMLIntegration` helpers are still used for that list path. `TenantDatabase` and
  `DBSecurityGroup` responses were checked against the same risk and found NOT to have this bug
  — their real SDK outputs (`CreateTenantDatabaseOutput.TenantDatabase *types.TenantDatabase`,
  `CreateDBSecurityGroupOutput.DBSecurityGroup *types.DBSecurityGroup`) genuinely do nest, so
  gopherstack's existing nested-wrapper responses for those were already correct.
  Fix: the 10 broken response structs now carry each field with the full
  `xml:"CreateDBShardGroupResult>FieldName"` chain individually (same technique already used
  a few lines below for `ModifyCurrentDBClusterCapacityResult`), so Go's `encoding/xml` emits
  all fields as flat siblings under one `<XxxResult>` element instead of nesting them under an
  extra wrapper element. Also fixed in the same pass: `CreateCustomDBEngineVersion`/
  `ModifyCustomDBEngineVersion` serialized their description field under the wrong element name
  entirely (`DatabaseInstallationFilesS3BucketName`, which isn't even a field on the real
  output) instead of the real `DBEngineVersionDescription` — fixed alongside the nesting change.
  `TestCreateCustomDBEngineVersionCRUD` in `accuracy_test.go` had encoded the old, wrong nested
  shape as its expected XML structure (exactly the "unit tests are not parity proof" trap from
  `.claude/memories/parity-principles.md` #3 — the test was green because it was written against
  the emulator's own bugged output, not against the real SDK shape); updated to assert the flat
  shape instead. Added `TestCreateDBShardGroup_WireShapeIsFlat` and
  `TestCreateIntegration_WireShapeIsFlat` regression tests that unmarshal the HTTP response body
  with the real (flat) field layout to guard against regressing back to the nested shape.

- No goroutine/ticker/map leaks found. The single background reconciler
  (`scheduleReconcilerLocked`) is started lazily per backend on first
  `CreateDBInstance`/`ModifyDBInstance`/etc. and exits its own `for` loop once
  `len(b.instanceReadyAt) == 0 && len(b.clusterReadyAt) == 0`, so it does not run forever in a
  backend that settles into a steady state. `events` is capped at `maxEvents` (ring-buffer
  trim). No `context.Background()`-rooted unbounded goroutines outside `fis.go` (chaos
  fault-injection, out of this pass's scope) were found in non-test `.go` files.
