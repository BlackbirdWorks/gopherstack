# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: rds
sdk_module: aws-sdk-go-v2/service/rds@v1.116.2
last_audit_commit: 23de3aab
last_audit_date: 2026-07-05
overall: B            # already-accurate on nearly all of ~140 routed ops (5+ prior audit
                       # passes: batch1-3, refinement1-4, #2213/#2226/#2227/#2329/#2334/#2339);
                       # this pass found and fixed 2 genuine wire/behavior gaps (final-snapshot
                       # contract on delete, DescribeDBInstances Filters) rather than a
                       # ground-up rewrite.
ops:
  DeleteDBInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass — see gaps"}
  DeleteDBCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass — see gaps"}
  DescribeDBInstances: {wire: ok, errors: ok, state: ok, persist: ok, note: "Filters added this pass"}
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
gaps:
  - DeleteDBInstance/DeleteDBCluster previously ignored SkipFinalSnapshot/FinalDBSnapshotIdentifier/DeleteAutomatedBackups entirely (silently always skipped the final snapshot with no validation) — FIXED this pass (bd: gopherstack-bgl)
  - DescribeDBInstances previously ignored the Filters parameter entirely — FIXED this pass for db-cluster-id/db-instance-id/dbi-resource-id/domain/engine (bd: gopherstack-bgl)
  - DescribeDBClusters, DescribeDBSnapshots, DescribeDBClusterSnapshots, DescribeEvents still ignore Filters — same shape of gap as DescribeDBInstances before this pass, not fixed (scope/time); follow-up under gopherstack-bgl
  - DB instance/cluster/snapshot/parameter-group identifiers are compared case-sensitively (Go map keys); real AWS treats DBInstanceIdentifier etc. as case-insensitive (e.g. creating "MyDB" then "mydb" should collide with DBInstanceAlreadyExistsFault but does not here) — not fixed: normalizing would touch every resource map across ~30K LOC and was judged too invasive for a scoped, low-risk pass; flagged for a dedicated follow-up
  - CreateDBInstance/CreateDBCluster do not validate the Engine name against a known-engine list; any string is accepted (real AWS returns InvalidParameterValue for an unsupported engine) — not fixed: many existing tests rely on the current permissive behavior with ad hoc engine strings; changing this is a larger, separately-scoped hardening task
deferred:
  - DB shard groups / integrations / tenant databases (Aurora Limitless / zero-ETL) — spot-checked as real (not stubs) but not re-verified op-by-op against the newest SDK field additions this pass
  - Activity streams / DB security groups (EC2-Classic legacy) — spot-checked only
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

- No goroutine/ticker/map leaks found. The single background reconciler
  (`scheduleReconcilerLocked`) is started lazily per backend on first
  `CreateDBInstance`/`ModifyDBInstance`/etc. and exits its own `for` loop once
  `len(b.instanceReadyAt) == 0 && len(b.clusterReadyAt) == 0`, so it does not run forever in a
  backend that settles into a steady state. `events` is capped at `maxEvents` (ring-buffer
  trim). No `context.Background()`-rooted unbounded goroutines outside `fis.go` (chaos
  fault-injection, out of this pass's scope) were found in non-test `.go` files.
