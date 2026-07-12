---
service: elasticache
sdk_module: aws-sdk-go-v2/service/elasticache@v1.51.11
last_audit_commit: 1b31b73f
last_audit_date: 2026-07-12
overall: B            # already-accurate op-by-op, with a real error-code/HTTP-status
                       # bug class found and fixed across ~60 call sites, plus two
                       # disguised-stub validation gaps wired up (see Notes/gaps).
                       # 2026-07-12 re-audit: zero drift since ce30166a (sweep3, this
                       # ledger's baseline), SDK still v1.51.11 with the same 75 ops,
                       # all gates green, no new bugs found (see final note below).
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
ops:
  CreateCacheCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: CacheClusterNotFound 400->404; added SnapshotName restore (was silently ignored)"}
  DeleteCacheCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: CacheClusterNotFound 400->404"}
  DescribeCacheClusters: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: CacheClusterNotFound 400->404; ShowCacheNodeInfo/pagination verified ok"}
  ModifyCacheCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: CacheParameterGroupNotFound 400->404; InvalidParameterGroupFamily->InvalidParameterValue (real code doesn't exist)"}
  RebootCacheCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateReplicationGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: ReplicationGroupAlreadyExists/CacheParameterGroupNotFound status; added SnapshotName restore"}
  DeleteReplicationGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: ReplicationGroupNotFound -> ReplicationGroupNotFoundFault, 400->404"}
  DescribeReplicationGroups: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same as above; NodeGroups/PendingModifiedValues/UserGroupIds wire shapes verified ok"}
  ModifyReplicationGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: wired dead ErrTransitEncryptionModeInvalid sentinel into validateTransitEncryptionModify + error mapping (disguised stub: guard never ran)"}
  TestFailover: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: ReplicationGroupNotFound code/status"}
  IncreaseReplicaCount: {wire: ok, errors: ok, state: ok, persist: ok}
  DecreaseReplicaCount: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyReplicationGroupShardConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: ErrClusterModeRequired was returned by backend but never mapped by the handler -> fell through to 500 InternalFailure; now 400 InvalidParameterCombination"}
  CreateCacheParameterGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteCacheParameterGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: CacheParameterGroupNotFound 400->404"}
  DescribeCacheParameterGroups: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same"}
  ModifyCacheParameterGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  ResetCacheParameterGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeCacheParameters: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: CacheParameterGroupNotFound 400->404"}
  DescribeEngineDefaultParameters: {wire: ok, errors: ok, state: ok, persist: n/a}
  CreateCacheSubnetGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteCacheSubnetGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: code CacheSubnetGroupNotFound -> CacheSubnetGroupNotFoundFault (AWS keeps the Fault suffix on the wire for this one specifically; status stays 400)"}
  DescribeCacheSubnetGroups: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same code fix"}
  ModifyCacheSubnetGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same code fix"}
  CreateCacheSecurityGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  AuthorizeCacheSecurityGroupIngress: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: CacheSecurityGroupNotFound 400->404"}
  RevokeCacheSecurityGroupIngress: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same"}
  DeleteCacheSecurityGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same"}
  DescribeCacheSecurityGroups: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same"}
  CreateSnapshot: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteSnapshot: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: SnapshotNotFoundFault 400->404"}
  DescribeSnapshots: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same; automatic vs manual source filter verified ok"}
  CopySnapshot: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: SnapshotNotFoundFault 400->404"}
  DescribeEvents: {wire: ok, errors: ok, state: ok, persist: n/a}
  CreateServerlessCache: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyServerlessCache: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: ServerlessCacheNotFound -> ServerlessCacheNotFoundFault, 400->404"}
  DeleteServerlessCache: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same"}
  DescribeServerlessCaches: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same"}
  CreateServerlessCacheSnapshot: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: ServerlessCacheNotFound code; ServerlessCacheSnapshotNotFoundFault status 400->404"}
  CopyServerlessCacheSnapshot: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: ServerlessCacheSnapshotNotFoundFault status 400->404"}
  DeleteServerlessCacheSnapshot: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same"}
  DescribeServerlessCacheSnapshots: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same"}
  ExportServerlessCacheSnapshot: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateUser: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyUser: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: UserNotFound 400->404; InvalidParameterValueException -> InvalidParameterValue (real wire code has no Exception suffix)"}
  DeleteUser: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: UserNotFound 400->404"}
  DescribeUsers: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same"}
  CreateUserGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: code UserGroupAlreadyExistsFault -> UserGroupAlreadyExists (no Fault suffix on the wire)"}
  ModifyUserGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: UserGroupNotFound 400->404"}
  DeleteUserGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same"}
  DescribeUserGroups: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same"}
  CreateGlobalReplicationGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteGlobalReplicationGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: GlobalReplicationGroupNotFoundFault status 400->404"}
  DescribeGlobalReplicationGroups: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same"}
  ModifyGlobalReplicationGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same"}
  DisassociateGlobalReplicationGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same"}
  FailoverGlobalReplicationGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same"}
  IncreaseNodeGroupsInGlobalReplicationGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same"}
  DecreaseNodeGroupsInGlobalReplicationGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same"}
  RebalanceSlotsInGlobalReplicationGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same"}
  DescribeReservedCacheNodes: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: ReservedCacheNodeNotFound 400->404"}
  DescribeReservedCacheNodesOfferings: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed: ReservedCacheNodesOfferingNotFound 400->404"}
  PurchaseReservedCacheNodesOffering: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: ReservedCacheNodesOfferingNotFound 400->404; ReservedCacheNodeAlreadyExists 409->404 (AWS models this AlreadyExists fault as 404, not 409/400)"}
  DescribeCacheEngineVersions: {wire: ok, errors: ok, state: n/a, persist: n/a}
  DescribeServiceUpdates: {wire: ok, errors: ok, state: n/a, persist: n/a}
  DescribeUpdateActions: {wire: ok, errors: ok, state: n/a, persist: n/a}
  BatchApplyUpdateAction: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchStopUpdateAction: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAllowedNodeTypeModifications: {wire: ok, errors: ok, state: n/a, persist: n/a}
  StartMigration: {wire: ok, errors: ok, state: ok, persist: ok}
  TestMigration: {wire: ok, errors: ok, state: ok, persist: ok}
  CompleteMigration: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  AddTagsToResource: {wire: ok, errors: ok, state: ok, persist: ok}
  RemoveTagsFromResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  cache_clusters: {status: ok, note: "engine redis/memcached/valkey, node type, num nodes, creating->available->modifying->deleting->rebooting all observable via lifecycle overlay; cache nodes list w/ endpoints; DescribeCacheClusters ShowCacheNodeInfo+pagination correct"}
  replication_groups: {status: ok, note: "primary/replica, node groups/shards, multi-AZ, automatic failover, cluster mode, IncreaseReplicaCount/DecreaseReplicaCount/TestFailover/global datastore all present and real; NodeGroups/PendingModifiedValues/UserGroupIds XML wrappers verified against api-2.json"}
  cache_parameter_groups: {status: ok, note: "Create/Modify/Delete/Describe/Reset + DescribeCacheParameters + DescribeEngineDefaultParameters all real; default-group protection (ErrParameterGroupDefaultNotModifiable -> InvalidCacheParameterGroupState) verified wired"}
  cache_subnet_groups: {status: ok}
  cache_security_groups: {status: ok}
  snapshots: {status: ok, note: "automatic vs manual source tracked (SnapshotSource field), CopySnapshot real; CreateCacheCluster/CreateReplicationGroup SnapshotName restore was a genuine gap (see gaps below), now fixed"}
  serverless_caches: {status: ok}
  users_and_user_groups: {status: ok, note: "RBAC access string, authentication (password/IAM/NoPasswordRequired) all real"}
  reserved_nodes: {status: ok}
  service_updates_and_events: {status: ok, note: "DescribeEvents wire shape (Event/Events/Marker) verified against api-2.json exactly"}
  tags: {status: ok, note: "Add/Remove/List via ARN; ErrResourceNotFound correctly surfaces as InvalidARN (matches AWS's own tag-op behavior for a resource ARN that doesn't resolve)"}
  timestamps: {status: ok, note: "RFC3339 ISO8601 strings used throughout -- CORRECT for this query/XML protocol; do NOT flag as an epoch-seconds bug (awstime.Epoch is for json/rest-json protocols only, not applicable here)"}
gaps:
  - "State-transition guards (InvalidCacheClusterStateFault/InvalidReplicationGroupStateFault/InvalidCacheParameterGroupStateFault-for-non-default-groups) are not enforced: Modify/Delete succeed even while a resource's PendingStatus is still creating/modifying/deleting when SetLifecycleDelay > 0. The lifecycle-overlay mechanism (backend_lifecycle.go) tracks the transient state precisely enough to support this, but no mutating op consults it before proceeding. Deliberately NOT implemented this pass: TestLifecycleFullVariantsAreObservable (backend_lifecycle_test.go:395) explicitly asserts that a Modify call immediately after Create (while still \"creating\") succeeds and reports \"modifying\" -- adding a guard would flip that test's expected outcome, and the risk of narrowing scope incorrectly (AWS's exact allow/deny matrix per state per op isn't verified) outweighed the value this pass. Left for a follow-up with SDK-parity evidence per transition. (bd: gopherstack-y8l follow-up, not filed as a separate issue this pass -- see Notes)"
  - "MaxRecords bounds (AWS requires 20-100 for most Describe* ops, InvalidParameterValueException otherwise) are accepted verbatim without range validation across all paginated ops. Shared pattern likely present in most query-protocol services in this repo, not elasticache-specific; deferred as a cross-service concern rather than a targeted elasticache fix."
deferred:
  - "Full data-plane snapshot/restore fidelity (actual key-value RDB dump/reload through miniredis) -- CreateCacheCluster/CreateReplicationGroup SnapshotName now validates existence and inherits engine/node-type metadata (the real API-contract behavior verified against api-2.json), but does not replay the source's actual key data into the restored miniredis instance. Flagged as a possible future enhancement, not a wire-shape/error-code bug."
  - "Quota-exceeded faults (ClusterQuotaForCustomerExceededFault, NodeQuotaForClusterExceededFault, CacheParameterGroupQuotaExceededFault, etc.) are not modeled -- no artificial resource limits are enforced. Standard for an emulator; not audited further this pass."
leaks: {status: clean, note: "zero goroutines/timers/tickers in the entire package (grepped `go func`, `time.AfterFunc`, `time.NewTicker`, `time.NewTimer` -- no hits outside tests). The lifecycle mechanism (backend_lifecycle.go) is deliberately goroutine-free: transient status + deadline overlaid on read, reaped lazily on the next write (pruneRegionLocked). Confirmed no leak regressions from this pass's changes."}
---

## Notes

**Protocol**: query/XML (`Version=2015-02-02`), matching `aws-sdk-go-v2/service/elasticache`'s
`awsAwsquery` (de)serializers. All list wrappers (`CacheNode`, `NodeGroup`, `NodeGroupMember`,
`Tag`, `Parameter`, `Subnet`, `Event`, `CacheParameterGroup`, `EC2SecurityGroup`, `member` for
unlabeled lists like `UserGroupIds`) were cross-checked directly against
`aws-sdk-go@v1.55.5/models/apis/elasticache/2015-02-02/api-2.json`'s `locationName` metadata --
all correct, no wrapper-name bugs found this pass.

**Timestamps are RFC3339 strings, not epoch seconds** -- this is CORRECT for ElastiCache's
query/XML protocol. `pkgs/awstime.Epoch` (used to fix QuickSight/IoT-style bugs) is only for
JSON/rest-json protocols; do not "fix" the RFC3339 formatting here in a future sweep, it isn't
a bug.

**The major finding this pass: a systematic error-code / HTTP-status wire bug class.**
Cross-referencing every `xmlError(...)` call site in handler.go/handler_ops2.go/handler_new_ops.go
against `aws-sdk-go@v1.55.5`'s `api-2.json` (`error.code` and `error.httpStatusCode` per
exception shape) turned up ~60 call sites with one or both of:
  - a wire `<Code>` string missing (or wrongly carrying) the AWS `Fault` suffix. AWS is
    genuinely inconsistent about this per-shape -- e.g. `CacheClusterNotFoundFault` serializes
    as bare `CacheClusterNotFound`, but `ReplicationGroupNotFoundFault`,
    `ServerlessCacheNotFoundFault`, `GlobalReplicationGroupNotFoundFault`, and
    `CacheSubnetGroupNotFoundFault` keep the `Fault` suffix on the wire. There is no
    reliable shortcut here -- each fault shape's `error.code` in api-2.json must be checked
    individually; don't assume a pattern.
  - the wrong HTTP status. Nearly every `*NotFoundFault` in this API is modeled `404`, but the
    emulator used `400` almost everywhere (a few, like `CacheSubnetGroupNotFoundFault`, really
    are `400` per AWS's own model -- again, no shortcut, check api-2.json per shape).
    `ReservedCacheNodeAlreadyExistsFault` is modeled `404` too (not the `409`/`400` one might
    expect for an "already exists" fault).

  This matters because aws-sdk-go-v2's query-protocol error deserializer is a per-operation
  hardcoded `switch` keyed on the exact `<Code>` string, generated strictly from that
  operation's modeled `errors` list in api-2.json (see `deserializers.go`,
  `awsAwsquery_deserializeOpError<Op>`). A wrong code string doesn't just cosmetically differ
  -- it means the SDK can't match any case and falls back to a generic `smithy.GenericAPIError`,
  so callers lose `errors.As` typed-fault handling entirely. New regression test
  `Test_ErrorWireShapesMatchAWS` in `handler_parity_sweep3_test.go` asserts the SDK actually
  deserializes into the exact typed fault for a representative case per family, plus the exact
  HTTP status, which is what makes this a wire-shape proof and not a bare `err != nil` check.

**Trap for the next auditor**: `SnapshotNotFoundFault` is NOT in `CreateCacheCluster`'s or
`CreateReplicationGroup`'s modeled `errors` list in api-2.json, even though both operations
accept a `SnapshotName` restore parameter. A missing/invalid snapshot on these two ops
correctly surfaces as `InvalidParameterValueException` (wire code `InvalidParameterValue`,
400) -- NOT `SnapshotNotFoundFault` (404), even though that would be the intuitive choice and
is what every other snapshot-consuming op in this API does use. Verified directly against the
per-operation `errors` array in api-2.json; do not "fix" this to SnapshotNotFoundFault later,
it would break wire fidelity, not improve it.

**Disguised-stub pattern found**: `ErrTransitEncryptionModeInvalid` and `ErrClusterModeRequired`
were both declared error sentinels with real intent (message text describing exactly the
validation AWS performs), but `ErrTransitEncryptionModeInvalid` was never returned from any
code path (dead sentinel -- the guard simply didn't exist in `applyModifyOptsLocked`), and
`ErrClusterModeRequired` WAS returned by the backend but had no case in the handler's error
mapping, so it fell through to a 500 `InternalFailure` instead of the correct 400
`InvalidParameterCombination`. Both are fixed this pass. When auditing error sentinels in this
package (or others), grep each declared `Err*` var for non-test reference count -- a count of
1 (declaration only) or a backend-only reference (no handler-side `errors.Is` case) both
indicate a disguised stub.

**Lock discipline**: single `*lockmetrics.RWMutex` (`b.mu`) guards all `InMemoryBackend` maps,
consistent with `pkgs-catalog.md`'s coarse-lock rule. When adding a cross-resource validation
inside an already-locked method (e.g. the new snapshot-restore lookup in
`CreateReplicationGroupFull`), reach for the store directly (`b.snapshotsStore(region)[name]`)
rather than calling a public `Describe*`/`List*` method that re-acquires `b.mu` -- the mutex is
not reentrant and the public accessors take their own `RLock`/`Lock`.

**Known-accurate, don't re-flag**: `TestLifecycleFullVariantsAreObservable` intentionally allows
Modify while a resource's `PendingStatus` is still `"creating"` (see gaps above) -- this is an
existing, deliberate design choice from a prior sweep to keep the lifecycle mechanism purely
observational, not a bug this pass introduced or should "fix" without first confirming AWS's
exact state-transition matrix per operation.

**2026-07-12 re-audit (no code changes)**: the ledger's recorded `last_audit_commit`
(`e7830377`, a cloudfront-only commit hash) was not an ancestor of HEAD, so per the re-audit
protocol this pass used `ce30166a` (the commit that authored/last touched this ledger, "Parity
sweep 3 (#2382)") as the drift baseline instead. `git diff ce30166a..HEAD --
services/elasticache/` is empty -- no local drift to audit. `aws-sdk-go-v2/service/elasticache`
is still pinned at `v1.51.11` (unchanged `go.mod`/`go.sum`), and its `api_op_*.go` surface is
still exactly the same 75 operations already covered 1:1 by the `ops:` table above -- no new
ops to wire up. Spot-checked for regressions: no `//nolint:funlen|gocyclo|cyclop|gocognit`
anywhere in the package, no stub/TODO/FIXME/unimplemented markers outside the legitimate
`EngineStub = "stub"` docker-engine-mode config constant (a real feature value, not a code
stub). All five scoped gates are green: `go build`, `go vet`, `go fix -diff` (empty),
`go test -race` (pass), `golangci-lint run` (0 issues). No real bugs found this pass --
`gaps` below (state-transition guards) remains the only known, deliberately-deferred item and
is unchanged.
