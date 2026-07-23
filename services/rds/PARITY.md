# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: rds
sdk_module: aws-sdk-go-v2/service/rds@v1.116.2
last_audit_commit: PENDING_COMMIT  # working tree not committed by this pass (git use was out of
                                    # scope); set to the actual commit hash when this diff lands.
last_audit_date: 2026-07-23
overall: A-            # this pass: (1) found+fixed a real leak (DeleteDBCluster did not
                       # cascade-delete custom cluster endpoints/tags -- ghost rows accumulated
                       # forever); (2) added Filters support to DescribeDBClusters/
                       # DescribeDBSnapshots/DescribeDBClusterSnapshots (DescribeEvents Filters
                       # confirmed NOT a gap -- real AWS documents it as "not currently
                       # supported" too); (3) added missing Marker/MaxRecords pagination to
                       # DescribeDBClusterSnapshots and DescribeEvents (both previously returned
                       # every row unpaginated); (4) field-diffed and added missing wire fields
                       # DBCluster.DbClusterResourceId, DBClusterSnapshot.DbClusterResourceId +
                       # SnapshotType, DBSnapshot.DbiResourceId; (5) de-deferred Activity Streams
                       # by field-diffing against the real SDK -- found and fixed a disguised-stub
                       # bug (ModifyActivityStream emitted an invented "AuditPolicy" XML element
                       # that doesn't exist on the real ModifyActivityStreamOutput; the real field
                       # is "PolicyStatus") plus an error-code bug (cluster-not-found returned
                       # InvalidParameterValue instead of DBClusterNotFoundFault); (6) found and
                       # fixed a systemic error-code bug: the rdsErrorCode mapping table was
                       # missing the "Fault" suffix real AWS uses for DBCluster*/
                       # DBClusterSnapshot*/DBClusterEndpoint*/DBClusterAutomatedBackup*/
                       # GlobalCluster*/BlueGreenDeployment*/Integration*/OptionGroup* error
                       # codes (confirmed against aws-sdk-go-v2's types/errors.go ErrorCode()
                       # methods -- AWS is inconsistent about this suffix, so each was verified
                       # individually, not assumed), AND was missing DBProxy*/
                       # ActivityStream* entries entirely, causing those errors to fall through
                       # to an unmapped "" code and a client-facing 500 InternalFailure instead
                       # of the correct 400 response. Prior overall B+ reflected 6+ audit passes
                       # on ~163 routed ops; this pass's fixes affect real, previously-unnoticed
                       # client-visible bugs across a wide swath of that surface (every DBCluster/
                       # DBClusterSnapshot/DBClusterEndpoint/GlobalCluster/BlueGreenDeployment/
                       # Integration/OptionGroup/DBProxy not-found-or-already-exists error, plus
                       # every activity-stream operation and every cluster-endpoint-bearing
                       # cluster delete), which is why the grade moves to A-. Remaining known
                       # gaps (case-sensitive identifiers, no Engine validation, DBShardGroup/
                       # Integration partial field coverage) are unchanged from the prior audit
                       # and still judged out of scope for a bounded pass -- see gaps/deferred.
ops:
  DeleteDBInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDBCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascade-delete of cluster endpoints/tags FIXED this pass — see leaks"}
  StartActivityStream: {wire: ok, errors: ok, state: ok, persist: ok, note: "de-deferred this pass — see families/activity_streams"}
  StopActivityStream: {wire: ok, errors: ok, state: ok, persist: ok, note: "de-deferred this pass — see families/activity_streams"}
  ModifyActivityStream: {wire: ok, errors: ok, state: ok, persist: ok, note: "wire-shape + error-code bugs FIXED this pass — see families/activity_streams"}
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
  DescribeDBClusters: {wire: ok, errors: ok, state: ok, persist: ok, note: "Filters support added this pass — see families/describe_filters"}
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
  pagination: {status: ok, note: "Marker/MaxRecords via pkgs/page.Page[T] (paginateDescribe) — consistent across all Describe* ops; DescribeDBClusterSnapshots and DescribeEvents were missing pagination entirely (returned every row regardless of MaxRecords) — FIXED this pass, see Notes"}
  describe_filters: {status: ok, note: "DescribeDBInstances Filters (db-cluster-id/db-instance-id/dbi-resource-id/domain/engine) added prior pass; DescribeDBClusters (clone-group-id/db-cluster-id/db-cluster-resource-id/domain/engine), DescribeDBSnapshots (db-instance-id/db-snapshot-id/dbi-resource-id/snapshot-type/engine), and DescribeDBClusterSnapshots (db-cluster-id/db-cluster-snapshot-id/snapshot-type/engine) Filters added THIS pass. DescribeEvents Filters intentionally left unimplemented: the real aws-sdk-go-v2 DescribeEventsInput.Filters doc comment reads literally 'This parameter isn't currently supported' — the emulator already matches real AWS by accepting-but-ignoring it, which is NOT a gap (prior ledger incorrectly listed it as one)"}
  global_clusters: {status: ok, note: "Create/Modify/Delete/Describe + Remove/Failover/SwitchoverGlobalCluster real"}
  blue_green_deployments: {status: ok, note: "Create/Describe/Delete/Switchover real (refinement1)"}
  db_proxies: {status: ok, note: "proxy/proxy-target/proxy-target-group/proxy-endpoint CRUD real (refinement3)"}
  reserved_instances: {status: ok, note: "Purchase + Describe(Offerings) real"}
  performance_insights: {status: ok, note: "GetPerformanceInsightsMetrics requires seeded data via SetPerformanceInsightsData — not a fabricated-on-the-fly stub; batch3_test.go.rej/.patch cruft from a prior sweep's already-applied fix removed this pass"}
  error_codes: {status: ok, note: "awserr sentinels map to correct AWS fault codes with correct HTTP status (400, uniformly, per the AWS Query-protocol convention — status does not vary by fault type, only the <Code> element does) via rdsErrorCode() in handler_dispatch.go. FIXED this pass: field-diffed the whole mapping table against aws-sdk-go-v2's types/errors.go ErrorCode() methods (the ground truth for wire codes) and found (a) a systemic missing-'Fault'-suffix bug on DBClusterNotFound(Fault)/DBClusterAlreadyExists(Fault)/DBClusterSnapshotNotFound(Fault)/DBClusterSnapshotAlreadyExists(Fault)/DBClusterEndpointNotFound(Fault)/DBClusterEndpointAlreadyExists(Fault)/DBClusterAutomatedBackupNotFound(Fault)/GlobalClusterNotFound(Fault)/GlobalClusterAlreadyExists(Fault)/BlueGreenDeploymentNotFound(Fault)/BlueGreenDeploymentAlreadyExists(Fault)/IntegrationNotFound(Fault)/IntegrationAlreadyExists(Fault)/OptionGroupNotFound(Fault)/OptionGroupAlreadyExists(Fault) — 15 codes total, each individually confirmed against the real SDK since AWS is inconsistent about the suffix (DBInstanceNotFound genuinely has none); and (b) ErrDBProxyAlreadyExists/ErrDBProxyEndpointAlreadyExists/ErrCannotDeleteDefaultProxyEndpoint/ErrActivityStreamAlreadyStarted/ErrActivityStreamNotStarted had NO entry in the mapping table at all, so errors.Is never matched and these fell through to an unmapped code → 500 InternalFailure instead of the correct 400 client error. See Notes and TestRDSErrorCodes_FaultSuffix (error_codes_test.go)."}
  leaks: {status: ok, note: "single reconciler goroutine per backend; self-terminates when instanceReadyAt/clusterReadyAt both empty (no ticker leak); FOUND and FIXED this pass: DeleteDBCluster did not cascade-delete the deleted cluster's custom cluster endpoints (or their tags) — a real ghost-row leak, see top-level leaks: entry below"}
  db_shard_groups: {status: ok, note: "Aurora Limitless shard groups — CRUD + Reboot real state; wire-shape bug (extra nesting) on Create/Delete/Modify/Reboot FIXED this pass, see gaps/Notes"}
  integrations: {status: ok, note: "zero-ETL Redshift integrations — CRUD real state; wire-shape bug (extra nesting) on Create/Delete/Modify FIXED this pass, see gaps/Notes"}
  custom_db_engine_versions: {status: ok, note: "wire-shape bug (extra nesting + wrong field name for description) on Create/Delete/Modify FIXED this pass, see gaps/Notes"}
  tenant_databases: {status: ok, note: "re-verified this pass against the real SDK's CreateTenantDatabaseOutput/DeleteTenantDatabaseOutput/ModifyTenantDatabaseOutput shapes (these DO nest under <TenantDatabase>, unlike shard groups/integrations) — no bug found, ledger's prior 'spot-checked only' caveat is now resolved to ok"}
  db_security_groups: {status: ok, note: "re-verified this pass (EC2-Classic legacy) — CreateDBSecurityGroupOutput/AuthorizeDBSecurityGroupIngressOutput/RevokeDBSecurityGroupIngressOutput all nest under <DBSecurityGroup> in the real SDK, matches gopherstack; no bug found, ledger's prior 'spot-checked only' caveat is now resolved to ok"}
  activity_streams: {status: ok, note: "de-deferred this pass: field-diffed Start/Stop/ModifyActivityStream against aws-sdk-go-v2's StartActivityStreamOutput/StopActivityStreamOutput/ModifyActivityStreamOutput. Start/Stop already matched (flat KinesisStreamName/KmsKeyId/Status/Mode/ApplyImmediately fields, correct — these ops were never affected by the shard-group/integration nesting bug class since their outputs were always flat in gopherstack). ModifyActivityStream had a real disguised-stub bug: it emitted an invented <AuditPolicy> element that does not exist on the real output (the real field is PolicyStatus, of type ActivityStreamPolicyStatus) and omitted the real KinesisStreamName/Mode members — FIXED, see Notes. Also fixed: cluster-not-found on all three ops returned InvalidParameterValue instead of the correct DBClusterNotFoundFault. Test coverage was previously zero for this family; added activity_stream_test.go (lifecycle, not-found, and backend-error-path tests)."}
gaps:
  - DB instance/cluster/snapshot/parameter-group identifiers are compared case-sensitively (Go map keys); real AWS treats DBInstanceIdentifier etc. as case-insensitive (e.g. creating "MyDB" then "mydb" should collide with DBInstanceAlreadyExistsFault but does not here) — STILL NOT FIXED (re-assessed this pass, same conclusion as prior audits): normalizing would touch every resource map across ~30K LOC and was judged too invasive for a scoped, low-risk pass; flagged for a dedicated follow-up
  - CreateDBInstance/CreateDBCluster do not validate the Engine name against a known-engine list; any string is accepted (real AWS returns InvalidParameterValue for an unsupported engine) — STILL NOT FIXED (re-assessed this pass, same conclusion as prior audits): many existing tests rely on the current permissive behavior with ad hoc engine strings; changing this is a larger, separately-scoped hardening task
  - DBShardGroup/Integration field coverage is still partial (Integration doesn't model Tags/KMSKeyId/CreateTime/Errors, DBShardGroup doesn't model DBShardGroupArn/DBShardGroupResourceId/PubliclyAccessible on the wire) — STILL NOT FIXED this pass (scope/time went to the higher-impact error-code and Filters/pagination gaps instead); judged lower priority since partial-but-correctly-shaped field coverage is a common, accepted pattern elsewhere in this emulator
  - CreateDBShardGroup/DeleteDBShardGroup/ModifyDBShardGroup/RebootDBShardGroup and CreateIntegration/DeleteIntegration/ModifyIntegration and CreateCustomDBEngineVersion/DeleteCustomDBEngineVersion/ModifyCustomDBEngineVersion (10 ops total) previously wrapped their response fields one XML level too deep (e.g. `<CreateDBShardGroupResult><DBShardGroup><DBShardGroupIdentifier>...`) when the real aws-sdk-go-v2 output for all 10 is a FLAT shape with no such wrapper (`<CreateDBShardGroupResult><DBShardGroupIdentifier>...`) — FIXED in a prior pass, see Notes. A real aws-sdk-go-v2 client's query-XML deserializer only looks for named fields as direct children of the `<XxxResult>` element, so every field on these 10 ops (including the identifier needed to address the resource in a follow-up call) previously came back empty/zero to a real SDK client, even though the emulator's backend state was correct.
  - CreateCustomDBEngineVersion/ModifyCustomDBEngineVersion additionally serialized the description field under the wrong element name (`DatabaseInstallationFilesS3BucketName` instead of `DBEngineVersionDescription`) — FIXED in a prior pass alongside the nesting fix, see Notes.
deferred:
  - DB shard groups / integrations (Aurora Limitless / zero-ETL) field *coverage* is still partial — see gaps (moved out of deferred into gaps this pass since it's a well-understood, scoped, bounded gap rather than an unverified family)
leaks: {status: fixed, note: "FOUND and FIXED this pass: DeleteDBCluster (DeleteDBClusterWithOptions in db_clusters.go) removed the cluster itself but did NOT cascade-delete its custom DB cluster endpoints or their tags — DescribeDBClusterEndpoints kept returning ghost rows pointing at a deleted cluster forever, and b.clusterEndpoints only ever shrank via an explicit DeleteDBClusterEndpoint call, so the map grew unboundedly across create/delete cycles in any long-running client (exactly the 'no ghost map rows after delete — cascade-clean instances/endpoints on cluster delete' invariant this audit was scoped to check). Fixed by adding deleteClusterEndpointsLocked (db_clusters.go), called from DeleteDBClusterWithOptions under the existing b.mu write lock, alongside the pre-existing tags/fisFailoverFaults/clusterRoles cleanup. Regression tests: TestDeleteDBCluster_CascadeDeletesClusterEndpoints (cluster_endpoints_test.go, verifies via DescribeDBClusterEndpoints) and a new cluster_endpoint_cascade_via_cluster_delete case added to the existing TestRDSBackend_TagsCleanedUpOnDelete table (tags_test.go). Separately re-verified this pass and still clean: the single reconciler goroutine (lifecycle.go:scheduleReconcilerLocked) is per-backend, started lazily, and exits its own loop once both instanceReadyAt and clusterReadyAt are empty (ticker.Stop() deferred); the two FIS fault-injection goroutines in fault_injection.go/handler_db_clusters.go are ctx-bound (one blocks on ctx.Done(), the other races a time.Timer against ctx.Done(), both Stop()/cleanup correctly). No time.Sleep/context.Background()-rooted unbounded goroutine patterns found in non-test files."}

## Notes

- Protocol: RDS uses the AWS Query (XML) protocol, version `2014-10-31`, XML namespace
  `http://rds.amazonaws.com/doc/2014-10-31/`. Every response wraps in
  `<ActionResponse><ActionResult>...</ActionResult></ActionResponse>` except where the op's
  SDK output has no members (in which case an empty result element is still correct — do not
  flag as a stub).

- **2026-07-23 pass summary.** This pass targeted the items the prior ledger flagged as gaps
  (Filters coverage) and deferred (Activity Streams), plus a fresh leak/error-code audit per
  the campaign's standing invariants. Six independent, verified fixes:

  1. **Ghost-row leak (found + fixed).** `DeleteDBClusterWithOptions` (`db_clusters.go`) deleted
     the cluster but left its custom `DBClusterEndpoint`s (and their tags) behind forever —
     `DescribeDBClusterEndpoints` kept returning rows for a deleted cluster, and `b.clusterEndpoints`
     only ever shrank via an explicit `DeleteDBClusterEndpoint` call. Fixed with a new
     `deleteClusterEndpointsLocked` helper invoked from the existing delete path. See the
     top-level `leaks:` entry for the full writeup and test names.

  2. **`Filters` support added to `DescribeDBClusters`, `DescribeDBSnapshots`,
     `DescribeDBClusterSnapshots`.** Mirrors the `DescribeDBInstances` Filters pattern added in a
     prior pass (`parseDescribeFilters` + an `isKnownXFilterName`/`applyXFilters`/
     `matchesAllXFilters` trio per op, each field-diffed against the real SDK's documented
     `Supported Filters` list for that op). Unmodeled-but-real filter names (`clone-group-id`,
     `domain`) are accepted and vacuously match everything, mirroring the existing `domain`
     precedent on `DescribeDBInstances`. **Correction to the prior ledger:** `DescribeEvents`
     Filters was listed as a gap, but `DescribeEventsInput.Filters`'s doc comment in
     aws-sdk-go-v2 literally reads "This parameter isn't currently supported" — real AWS itself
     doesn't implement it, so the emulator's existing accept-and-ignore behavior there was
     already correct; this is not a gap and has been removed from the gaps list.

  3. **Missing pagination added to `DescribeDBClusterSnapshots` and `DescribeEvents`.** Both
     returned every row unconditionally regardless of `MaxRecords`/`Marker`, even though both
     real outputs (`DescribeDBClusterSnapshotsOutput`, `DescribeEventsOutput`) carry a `Marker`
     field. Wired through the existing `paginateDescribe` helper for consistency with every other
     Describe op. `DescribeEvents` sorts by `(CreatedAt, SourceIdentifier)` for a stable order.

  4. **Missing wire fields added**, confirmed against `aws-sdk-go-v2/service/rds@v1.116.2`'s
     `types.DBCluster`/`types.DBClusterSnapshot`/`types.DBSnapshot`: `DBCluster.DbClusterResourceId`,
     `DBClusterSnapshot.DbClusterResourceId` + `SnapshotType` (the latter was previously never set
     at all on cluster snapshots, unlike instance snapshots which already distinguished
     manual/automated), and `DBSnapshot.DbiResourceId`. `CopyDBClusterSnapshot`/`CopyDBSnapshot`
     were also missing several fields their real outputs carry (`EngineVersion`,
     `PercentProgress`, `StorageEncrypted` on cluster-snapshot copy; `SnapshotType` on
     snapshot copy) — filled in alongside the new fields since the same struct literals needed
     touching anyway.

  5. **Activity Streams de-deferred** (`activity_stream.go`, `handler_activity_stream.go`).
     `StartActivityStream`/`StopActivityStream` already matched the real flat
     `StartActivityStreamOutput`/`StopActivityStreamOutput` shapes. `ModifyActivityStream` had a
     disguised-stub bug: it serialized an invented `<AuditPolicy>` XML element that does not
     exist anywhere on the real `ModifyActivityStreamOutput` (verified against
     `aws-sdk-go-v2/service/rds@v1.116.2/api_op_ModifyActivityStream.go`) — the real field for
     the policy lock state is `PolicyStatus`. A real SDK client parsing this response would never
     see the value the emulator was trying to communicate, since it was under the wrong XML tag
     entirely. Fixed by renaming the field to `PolicyStatus` and adding the real
     `KinesisStreamName`/`Mode` members that were also missing. Separately, all three ops
     returned `InvalidParameterValue` for a nonexistent cluster instead of the correct
     `DBClusterNotFoundFault` — fixed to use `ErrClusterNotFound`. This family had zero test
     coverage before this pass; added `activity_stream_test.go`.

  6. **Systemic error-code "Fault"-suffix bug (found + fixed) in `rdsErrorCode()`**
     (`handler_dispatch.go`). AWS is inconsistent about whether a wire error code carries a
     trailing "Fault" (`DBInstanceNotFound` has none; `DBClusterNotFoundFault` does), so each
     entry below was individually confirmed against `aws-sdk-go-v2/service/rds@v1.116.2/types/errors.go`'s
     generated `ErrorCode()` methods — the authoritative source for what a real RDS server puts
     on the wire — rather than assumed from a uniform convention. Fifteen codes were missing the
     suffix real AWS uses: `DBClusterNotFoundFault`, `DBClusterAlreadyExistsFault`,
     `DBClusterSnapshotNotFoundFault`, `DBClusterSnapshotAlreadyExistsFault`,
     `DBClusterEndpointNotFoundFault`, `DBClusterEndpointAlreadyExistsFault`,
     `DBClusterAutomatedBackupNotFoundFault`, `GlobalClusterNotFoundFault`,
     `GlobalClusterAlreadyExistsFault`, `BlueGreenDeploymentNotFoundFault`,
     `BlueGreenDeploymentAlreadyExistsFault`, `IntegrationNotFoundFault`,
     `IntegrationAlreadyExistsFault`, `OptionGroupNotFoundFault`, `OptionGroupAlreadyExistsFault`.
     Separately and more severely: `ErrDBProxyAlreadyExists`, `ErrDBProxyEndpointAlreadyExists`,
     `ErrCannotDeleteDefaultProxyEndpoint`, `ErrActivityStreamAlreadyStarted`, and
     `ErrActivityStreamNotStarted` had **no entry at all** in the mapping table — since each
     `awserr.New(...)` sentinel is a distinct `*wrappedError` pointer even when two sentinels
     share the same message string, `errors.Is(opErr, m.sentinel)` never matched any mapping
     entry for these five, so `rdsErrorCode()` returned `""` and `handleOpError` fell through to
     a **500 InternalFailure** instead of the correct 400 client error — exactly the
     "missing errCodeLookup entries → not-found errors surfacing as 500 InternalFailure" bug
     class from `.claude/memories/parity-principles.md` #2, just for conflict/already-exists
     faults instead of not-found ones. Regression test: `TestRDSErrorCodes_FaultSuffix`
     (`error_codes_test.go`), 15 table cases covering every fixed family end-to-end through the
     HTTP handler. The four newly-extracted string constants this fix needed to stay
     `goconst`-clean (`filterNameDBClusterID`, `filterNameDBInstanceID`, `filterNameEngine`,
     `filterNameDomain`, `filterNameDbiResourceID`, `filterNameSnapshotType`,
     `snapshotTypeManual`, `errCodeInvalidDBClusterStateFault`) live in `shared.go`.

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
