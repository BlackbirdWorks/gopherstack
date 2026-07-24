---
service: elasticache
sdk_module: aws-sdk-go-v2/service/elasticache@v1.51.11
last_audit_commit: d5e1073d1
last_audit_date: 2026-07-24
overall: A-           # 2026-07-24 pass: implemented the two documented gaps from the
                       # prior ledger (state-transition guards, MaxRecords bounds), and
                       # field-diffing users/user-groups against aws-sdk-go-v2 turned up
                       # a genuine wire-shape bug class the "ok" status had been masking:
                       # a gopherstack-invented `NoPasswordRequired` field was serialized
                       # in User's Create/Modify/Delete/DescribeResult in place of the
                       # real `Authentication{Type,PasswordCount}` struct and
                       # `UserGroupIds` list (both entirely absent from the wire), and a
                       # gopherstack-invented `Description` field was serialized on
                       # UserGroup (the real type has none), while UserGroup's real
                       # `ReplicationGroups` field was left entirely unwired despite a
                       # placeholder model field existing. All fixed this pass (see
                       # Notes). Grade held at A- rather than A because two intentionally
                       # deferred items remain (data-plane snapshot restore fidelity,
                       # quota-exceeded faults) -- both are pre-existing, reasoned
                       # deferrals, not new gaps.
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
ops:
  CreateCacheCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: CacheClusterNotFound 400->404; added SnapshotName restore (was silently ignored)"}
  DeleteCacheCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed (2026-07-24): InvalidCacheClusterState guard -- rejects delete while status != available (creating/modifying/deleting), matching AWS; wire-verified via TestStateGuardRejectsMutationWhilePending"}
  DescribeCacheClusters: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: CacheClusterNotFound 400->404; ShowCacheNodeInfo/pagination verified ok; MaxRecords [20,100] now enforced (2026-07-24)"}
  ModifyCacheCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: CacheParameterGroupNotFound 400->404; InvalidParameterGroupFamily->InvalidParameterValue (real code doesn't exist); (2026-07-24) InvalidCacheClusterState guard added"}
  RebootCacheCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "(2026-07-24) InvalidCacheClusterState guard added -- cannot reboot a non-available cluster"}
  CreateReplicationGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: ReplicationGroupAlreadyExists/CacheParameterGroupNotFound status; added SnapshotName restore"}
  DeleteReplicationGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: ReplicationGroupNotFound -> ReplicationGroupNotFoundFault, 400->404; (2026-07-24) InvalidReplicationGroupState guard added"}
  DescribeReplicationGroups: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same as above; NodeGroups/PendingModifiedValues/UserGroupIds wire shapes verified ok; MaxRecords [20,100] now enforced"}
  ModifyReplicationGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: wired dead ErrTransitEncryptionModeInvalid sentinel; (2026-07-24) InvalidReplicationGroupState guard added to the wire-routed ModifyReplicationGroupFull path"}
  TestFailover: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: ReplicationGroupNotFound code/status; (2026-07-24) InvalidReplicationGroupState guard added"}
  IncreaseReplicaCount: {wire: ok, errors: ok, state: ok, persist: ok, note: "(2026-07-24) InvalidReplicationGroupState guard added"}
  DecreaseReplicaCount: {wire: ok, errors: ok, state: ok, persist: ok, note: "(2026-07-24) InvalidReplicationGroupState guard added"}
  ModifyReplicationGroupShardConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: ErrClusterModeRequired mapping; (2026-07-24) InvalidReplicationGroupState guard added"}
  CreateCacheParameterGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteCacheParameterGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: CacheParameterGroupNotFound 400->404"}
  DescribeCacheParameterGroups: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same; MaxRecords [20,100] now enforced; handler deduped via describeListChecked"}
  ModifyCacheParameterGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  ResetCacheParameterGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeCacheParameters: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: CacheParameterGroupNotFound 400->404; MaxRecords [20,100] now enforced"}
  DescribeEngineDefaultParameters: {wire: ok, errors: ok, state: n/a, persist: n/a, note: "MaxRecords [20,100] now enforced"}
  CreateCacheSubnetGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteCacheSubnetGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: code CacheSubnetGroupNotFound -> CacheSubnetGroupNotFoundFault (Fault suffix kept on the wire for this one; status stays 400)"}
  DescribeCacheSubnetGroups: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same code fix; MaxRecords [20,100] now enforced (400, matching this op's own NotFound status)"}
  ModifyCacheSubnetGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same code fix"}
  CreateCacheSecurityGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  AuthorizeCacheSecurityGroupIngress: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: CacheSecurityGroupNotFound 400->404"}
  RevokeCacheSecurityGroupIngress: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same"}
  DeleteCacheSecurityGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same"}
  DescribeCacheSecurityGroups: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same; MaxRecords [20,100] now enforced"}
  CreateSnapshot: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteSnapshot: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: SnapshotNotFoundFault 400->404"}
  DescribeSnapshots: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same; automatic vs manual source filter verified ok; MaxRecords [20,100] now enforced"}
  CopySnapshot: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: SnapshotNotFoundFault 400->404"}
  DescribeEvents: {wire: ok, errors: ok, state: ok, persist: n/a, note: "MaxRecords [20,100] now enforced"}
  CreateServerlessCache: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyServerlessCache: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: ServerlessCacheNotFound -> ServerlessCacheNotFoundFault, 400->404; (2026-07-24) InvalidServerlessCacheStateFault guard added to both the wire-routed ModifyServerlessCache and the ModifyServerlessCacheFull variant"}
  DeleteServerlessCache: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same; (2026-07-24) InvalidServerlessCacheStateFault guard added"}
  DescribeServerlessCaches: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same; MaxRecords [20,100] now enforced; handler deduped via describeListChecked"}
  CreateServerlessCacheSnapshot: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: ServerlessCacheNotFound code; ServerlessCacheSnapshotNotFoundFault status 400->404"}
  CopyServerlessCacheSnapshot: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: ServerlessCacheSnapshotNotFoundFault status 400->404"}
  DeleteServerlessCacheSnapshot: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same"}
  DescribeServerlessCacheSnapshots: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same; MaxRecords [20,100] now enforced"}
  ExportServerlessCacheSnapshot: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateUser: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed (2026-07-24): DELETED gopherstack-invented `NoPasswordRequired` wire output field (types.User/CreateUserResult have no such field); now serializes the real Authentication{Type,PasswordCount} struct and UserGroupIds list. Handles AuthenticationMode.Type (password/no-password-required/iam, translated to output's password/no-password/iam) + AuthenticationMode.Passwords / legacy top-level Passwords (1-2, else InvalidParameterValue) + legacy NoPasswordRequired bool. New CreateUserWithAuth backend method carries the full model; CreateUser(bool) kept as a thin legacy wrapper so existing call sites are unaffected"}
  ModifyUser: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: UserNotFound 400->404; InvalidParameterValueException -> InvalidParameterValue; (2026-07-24) added AppendAccessString (was unhandled -- ModifyUserInput has both AccessString and AppendAccessString), Engine, and the same Authentication-model handling as CreateUser via new ModifyUserWithAuth"}
  DeleteUser: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: UserNotFound 400->404; (2026-07-24) response now includes Authentication/UserGroupIds like the other User-returning ops"}
  DescribeUsers: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same; (2026-07-24) Authentication/UserGroupIds wire fix (see CreateUser); MaxRecords [20,100] now enforced; handler deduped via describeListChecked"}
  CreateUserGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: code UserGroupAlreadyExistsFault -> UserGroupAlreadyExists; (2026-07-24) DELETED gopherstack-invented `Description` field (types.UserGroup/CreateUserGroupInput have no such field/param) from both input parsing and wire output; now wires the real ReplicationGroups field (reverse of a ReplicationGroup's UserGroupIds, computed fresh on every response -- was previously a dead, always-empty model field)"}
  ModifyUserGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: UserGroupNotFound 400->404; (2026-07-24) ReplicationGroups wire fix (see CreateUserGroup)"}
  DeleteUserGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same; (2026-07-24) Description removed, ReplicationGroups wired"}
  DescribeUserGroups: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same; (2026-07-24) Description removed, ReplicationGroups wired; MaxRecords [20,100] now enforced; handler deduped via describeListChecked"}
  CreateGlobalReplicationGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteGlobalReplicationGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: GlobalReplicationGroupNotFoundFault status 400->404; (2026-07-24) InvalidGlobalReplicationGroupState guard added"}
  DescribeGlobalReplicationGroups: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same; MaxRecords [20,100] now enforced; handler deduped via describeListChecked (no state guard here -- Describe doesn't require availability)"}
  ModifyGlobalReplicationGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same; (2026-07-24) InvalidGlobalReplicationGroupState guard added"}
  DisassociateGlobalReplicationGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same; (2026-07-24) InvalidGlobalReplicationGroupState guard added"}
  FailoverGlobalReplicationGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same; (2026-07-24) InvalidGlobalReplicationGroupState guard added"}
  IncreaseNodeGroupsInGlobalReplicationGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same; (2026-07-24) InvalidGlobalReplicationGroupState guard added"}
  DecreaseNodeGroupsInGlobalReplicationGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same; (2026-07-24) InvalidGlobalReplicationGroupState guard added"}
  RebalanceSlotsInGlobalReplicationGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same; (2026-07-24) InvalidGlobalReplicationGroupState guard added"}
  DescribeReservedCacheNodes: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: ReservedCacheNodeNotFound 400->404; MaxRecords [20,100] now enforced"}
  DescribeReservedCacheNodesOfferings: {wire: ok, errors: ok, state: n/a, persist: n/a, note: "fixed: ReservedCacheNodesOfferingNotFound 400->404; MaxRecords [20,100] now enforced"}
  PurchaseReservedCacheNodesOffering: {wire: partial, errors: ok, state: ok, persist: ok, note: "fixed: ReservedCacheNodesOfferingNotFound 400->404; ReservedCacheNodeAlreadyExists 409->404. Gap (not fixed this pass): the response's RecurringCharges list (types.ReservedCacheNode/ReservedCacheNodesOffering.RecurringCharges) is always empty -- this emulator does no pricing modeling. Often accurate in practice (All/Partial-Upfront offerings commonly have zero recurring charges) but not verified against a real recurring-charge case; low priority, see items_still_open"}
  DescribeCacheEngineVersions: {wire: ok, errors: ok, state: n/a, persist: n/a}
  DescribeServiceUpdates: {wire: ok, errors: ok, state: n/a, persist: n/a, note: "MaxRecords [20,100] now enforced"}
  DescribeUpdateActions: {wire: ok, errors: ok, state: n/a, persist: n/a, note: "MaxRecords [20,100] now enforced"}
  BatchApplyUpdateAction: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchStopUpdateAction: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAllowedNodeTypeModifications: {wire: ok, errors: ok, state: n/a, persist: n/a}
  StartMigration: {wire: ok, errors: ok, state: ok, persist: ok, note: "no state guard added -- migration ops legitimately run while status is \"migrating\", not \"available\"; adding the generic guard here would be wrong, not an improvement (see Notes)"}
  TestMigration: {wire: ok, errors: ok, state: ok, persist: ok, note: "same as StartMigration"}
  CompleteMigration: {wire: ok, errors: ok, state: ok, persist: ok, note: "same as StartMigration -- must succeed while status=\"migrating\""}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  AddTagsToResource: {wire: ok, errors: ok, state: ok, persist: ok}
  RemoveTagsFromResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  cache_clusters: {status: ok, note: "engine redis/memcached/valkey, node type, num nodes, creating->available->modifying->deleting->rebooting all observable via lifecycle overlay; cache nodes list w/ endpoints; DescribeCacheClusters ShowCacheNodeInfo+pagination correct; (2026-07-24) InvalidCacheClusterState guard on Modify/Delete/Reboot"}
  replication_groups: {status: ok, note: "primary/replica, node groups/shards, multi-AZ, automatic failover, cluster mode, IncreaseReplicaCount/DecreaseReplicaCount/TestFailover/global datastore all present and real; NodeGroups/PendingModifiedValues/UserGroupIds XML wrappers verified against api-2.json; (2026-07-24) InvalidReplicationGroupState guard on every mutating op except migration ops (see Notes)"}
  cache_parameter_groups: {status: ok, note: "Create/Modify/Delete/Describe/Reset + DescribeCacheParameters + DescribeEngineDefaultParameters all real; default-group protection (ErrParameterGroupDefaultNotModifiable -> InvalidCacheParameterGroupState) verified wired"}
  cache_subnet_groups: {status: ok}
  cache_security_groups: {status: ok}
  snapshots: {status: ok, note: "automatic vs manual source tracked (SnapshotSource field), CopySnapshot real; CreateCacheCluster/CreateReplicationGroup SnapshotName restore was a genuine gap, now fixed (a prior pass)"}
  serverless_caches: {status: ok, note: "(2026-07-24) InvalidServerlessCacheStateFault guard on Modify/Delete"}
  users_and_user_groups: {status: ok, note: "(2026-07-24) MAJOR wire-shape fix: User's Authentication{Type,PasswordCount} + UserGroupIds were entirely absent from the wire response (a gopherstack-invented NoPasswordRequired boolean stood in their place); UserGroup's real ReplicationGroups field was unwired and a gopherstack-invented Description field was serialized instead. The prior ledger's 'RBAC access string, authentication (password/IAM/NoPasswordRequired) all real' note was WRONG -- IAM/password auth type was never distinguishable on the wire, only a boolean. Both fixed; see ops table and Notes"}
  reserved_nodes: {status: ok, note: "RecurringCharges list always empty (no pricing model) -- see PurchaseReservedCacheNodesOffering note; low-priority, not fixed this pass"}
  service_updates_and_events: {status: ok, note: "DescribeEvents wire shape (Event/Events/Marker) verified against api-2.json exactly"}
  tags: {status: ok, note: "Add/Remove/List via ARN; ErrResourceNotFound correctly surfaces as InvalidARN (matches AWS's own tag-op behavior for a resource ARN that doesn't resolve)"}
  timestamps: {status: ok, note: "RFC3339 ISO8601 strings used throughout -- CORRECT for this query/XML protocol; do NOT flag as an epoch-seconds bug (awstime.Epoch is for json/rest-json protocols only, not applicable here)"}
gaps: []
  # Both gaps in the 2026-07-12 ledger are fixed this pass:
  #  - State-transition guards: implemented for cache clusters, replication groups
  #    (all mutating ops except migration -- see Notes), serverless caches, and global
  #    replication groups. requireAvailableLocked in lifecycle.go is the shared guard;
  #    TestLifecycleFullVariantsAreObservable was updated (it previously asserted the
  #    now-fixed incorrect behavior) and TestStateGuardRejectsMutationWhilePending is a
  #    new wire-level regression test (SDK client -> typed fault + HTTP 400) covering
  #    every guarded resource family.
  #  - MaxRecords bounds: parsePagination now rejects MaxRecords outside [20,100] (or
  #    non-numeric) with InvalidParameterValue/400, applied to all ~19 paginated
  #    Describe*/List* call sites via the new parsePaginationChecked/describeListChecked
  #    helpers. TestHandler_DescribeCacheClusters_MaxRecordsOutOfRange locks this. NOTE:
  #    this was flagged as a "cross-service concern" in the prior ledger -- it is now
  #    fixed for elasticache specifically; other services were not touched.
deferred:
  - "Full data-plane snapshot/restore fidelity (actual key-value RDB dump/reload through miniredis) -- CreateCacheCluster/CreateReplicationGroup SnapshotName validates existence and inherits engine/node-type metadata (the real API-contract behavior verified against api-2.json), but does not replay the source's actual key data into the restored miniredis instance. Flagged as a possible future enhancement, not a wire-shape/error-code bug. Unchanged this pass."
  - "Quota-exceeded faults (ClusterQuotaForCustomerExceededFault, NodeQuotaForClusterExceededFault, CacheParameterGroupQuotaExceededFault, etc.) are not modeled -- no artificial resource limits are enforced. Standard for an emulator; not audited further this pass. Unchanged."
leaks: {status: clean, note: "zero goroutines/timers/tickers in the entire package (grepped `go func`, `time.AfterFunc`, `time.NewTicker`, `time.NewTimer` -- no hits outside tests), reconfirmed 2026-07-24 after this pass's changes. The lifecycle mechanism (lifecycle.go) is deliberately goroutine-free: transient status + deadline overlaid on read, reaped lazily on the next write (pruneRegionLocked). The new requireAvailableLocked guard adds no new state, locks, or allocations beyond a single overlayStatus() call already computed for the read path."}
---

## Notes

**Protocol**: query/XML (`Version=2015-02-02`), matching `aws-sdk-go-v2/service/elasticache`'s
`awsAwsquery` (de)serializers. All list wrappers (`CacheNode`, `NodeGroup`, `NodeGroupMember`,
`Tag`, `Parameter`, `Subnet`, `Event`, `CacheParameterGroup`, `EC2SecurityGroup`, `member` for
unlabeled lists like `UserGroupIds`) were cross-checked directly against
`aws-sdk-go-v2/service/elasticache@v1.51.11`'s `deserializers.go` -- all correct except the
User/UserGroup bugs fixed this pass (see below).

**2026-07-24 pass -- state-transition guards (the prior ledger's gap #1, now fixed)**:
verified via `aws-sdk-go-v2/service/elasticache@v1.51.11/deserializers.go`'s per-operation
error-deserializer switch (ground truth for which faults an op recognizes) plus AWS docs, that
essentially every mutating cache-cluster/replication-group/serverless-cache/
global-replication-group op models an `Invalid<Resource>State(Fault)`. A resource must be
`available` before accepting a new Modify/Delete/TestFailover/failover-style call; AWS returns
this fault (400) otherwise, e.g. while still `creating` from a prior call. Implemented as
`requireAvailableLocked` in `lifecycle.go`, called from every applicable backend mutator with a
new set of sentinel errors (`ErrClusterNotAvailable`, `ErrReplicationGroupNotAvailable`,
`ErrServerlessCacheNotAvailable`, `ErrGlobalReplicationGroupNotAvailable`) mapped to
`InvalidCacheClusterState` / `InvalidReplicationGroupState` (no `Fault` suffix on the wire,
verified against the deserializer's exact case-string) / `InvalidServerlessCacheStateFault` /
`InvalidGlobalReplicationGroupState` respectively. Deliberately NOT applied to
StartMigration/TestMigration/CompleteMigration: these operate correctly while status is
`"migrating"`, a state the generic `available`-only guard would incorrectly reject. Since the
default `SetLifecycleDelay` is 0 (transitions are instant), this guard is a no-op for the vast
majority of existing tests -- it is only observable when a test explicitly configures a
lifecycle delay, exactly as intended.

**2026-07-24 pass -- MaxRecords bounds (the prior ledger's gap #2, now fixed for elasticache)**:
AWS docs confirm every paginated Describe*/List* op models `MaxRecords` as `[20,100]`,
`InvalidParameterValueException` otherwise. `parsePagination` in `handler.go` now rejects
out-of-range or non-numeric values; `parsePaginationChecked` and the new generic
`describeListChecked[T]` helper centralize the boilerplate across all ~19 call sites (both to
avoid ~19 copies of the same 4-line check, and because the resulting duplication would otherwise
trip the `dupl` linter). One existing test (`TestHandler_DescribeCacheClusters_Pagination`) used
`MaxRecords: 3`, which is below AWS's real minimum -- fixed to use the modeled minimum of 20
with enough records to still prove a second page exists, rather than encoding invalid input as
if it were valid.

**2026-07-24 pass -- User/UserGroup wire-shape bugs (found via field-diffing, not in the prior
ledger's gaps list)**: the prior ledger marked `users_and_user_groups: ok` with the note "RBAC
access string, authentication (password/IAM/NoPasswordRequired) all real" -- this was incorrect.
Field-diffing `types.User` (`ARN`, `AccessString`, `Authentication`, `Engine`,
`MinimumEngineVersion`, `Status`, `UserGroupIds`, `UserId`, `UserName`) against the emulator's
`userXML`/`User` model found:
  - a **gopherstack-invented** `NoPasswordRequired` boolean was being serialized in
    `CreateUserResult`/`ModifyUserResult`/`DeleteUserResult`/`DescribeUsersResult` -- the real
    `User` output shape has NO such field. DELETED.
  - the real `Authentication` struct (`Type`: `password`/`no-password`/`iam`, `PasswordCount`)
    was entirely absent from every User response. ADDED (new `authenticationXML`, `AuthType`/
    `PasswordCount` fields on the `User` model).
  - the real `UserGroupIds` list (a user's group memberships, echoed back on every User
    response) was entirely absent. ADDED, computed fresh on every response
    (`userGroupIDsLocked`) rather than persisted, matching how AWS derives it.
  - `CreateUserInput`/`ModifyUserInput`'s `AuthenticationMode` (`Type` + `Passwords`, up to 2)
    and `ModifyUserInput`'s `AppendAccessString` were entirely unhandled. ADDED, with the
    correct **input-vs-output enum spelling mismatch** handled explicitly: input
    `no-password-required` (`types.InputAuthenticationTypeNoPassword`) serializes as output
    `no-password` (`types.AuthenticationTypeNoPassword`) -- verified against
    `types/enums.go`; conflating the two would have been a new, subtler bug.
  - New backend methods `CreateUserWithAuth`/`ModifyUserWithAuth` carry the full model;
    `CreateUser(bool)`/`ModifyUser(bool)` are now thin legacy wrappers so the ~15 existing
    direct-backend test call sites needed no changes.

  Similarly, `types.UserGroup` (`ARN`, `Engine`, `MinimumEngineVersion`, `PendingChanges`,
  `ReplicationGroups`, `ServerlessCaches`, `Status`, `UserGroupId`, `UserIds`) has **no**
  `Description` field, and neither does `CreateUserGroupInput`/`ModifyUserGroupInput` -- a
  gopherstack-invented `Description` param/field existed on both the input parsing and the wire
  output. DELETED (required removing the `description` parameter from
  `CreateUserGroup`/`CreateUserGroupValidated`, updating ~14 test call sites). The real
  `ReplicationGroups` field (the reverse of a ReplicationGroup's `UserGroupIds` -- which
  replication groups a user group is attached to) was left completely unwired despite a
  placeholder `AssignedReplicationGroupIDs` model field existing since a prior pass (a disguised
  stub: the field existed but nothing ever populated it). Now computed fresh on every response
  (`userGroupReplicationGroupIDsLocked`), mirroring the User fix.

  `ServerlessCaches []string` (which serverless caches a user group is associated with) was
  NOT added -- this emulator has no existing mechanism tracking that association anywhere
  (unlike the ReplicationGroup<->UserGroup link, which already existed one-directionally via
  `ReplicationGroup.UserGroupIDs`), and fabricating one would be new-feature scope beyond a wire
  fix. Left as a known small gap (see items_still_open in the agent receipt).

**Trap for the next auditor (unchanged)**: `SnapshotNotFoundFault` is NOT in `CreateCacheCluster`'s
or `CreateReplicationGroup`'s modeled `errors` list in api-2.json, even though both operations
accept a `SnapshotName` restore parameter. A missing/invalid snapshot on these two ops correctly
surfaces as `InvalidParameterValueException` (wire code `InvalidParameterValue`, 400) -- NOT
`SnapshotNotFoundFault` (404). Do not "fix" this later, it would break wire fidelity.

**Trap for the next auditor (new this pass)**: when adding a new `Invalid<X>State(Fault)`
guard, check the deserializer's exact case string per operation before assuming a `Fault` suffix
pattern -- `InvalidCacheClusterState`/`InvalidReplicationGroupState`/
`InvalidGlobalReplicationGroupState` have NO suffix on the wire, but
`InvalidServerlessCacheStateFault` DOES. Also: do not add this guard to
StartMigration/TestMigration/CompleteMigration -- they must work precisely because the resource
is NOT `available` (it's `migrating`).

**Lock discipline (unchanged)**: single `*lockmetrics.RWMutex` (`b.mu`) guards all
`InMemoryBackend` maps. `requireAvailableLocked`, `userGroupIDsLocked`, and
`userGroupReplicationGroupIDsLocked` all assume the caller already holds `b.mu` (Lock or
RLock) and read other backend maps directly (e.g. `b.replicationGroupsStore(region)`) rather
than through a public method, since the mutex is not reentrant.

**Disguised-stub pattern found again this pass**: `UserGroup.AssignedReplicationGroupIDs` was a
model field with a docstring ("populated when bound to RG") and a test asserting it starts
empty -- but nothing anywhere ever set it to non-empty. This is the same class of bug as last
pass's dead `ErrTransitEncryptionModeInvalid` sentinel: a field/error that *looks* wired because
it exists and has a plausible-sounding comment, but has zero non-test, non-declaration
references. When auditing, grep every model field for write-sites, not just read-sites.

**Known-accurate, don't re-flag**: `TestLifecycleIntermediateStatesObservable` already advances
its fake clock past the create delay before calling Modify (`clock.advance(2 * delay)` at
handler-observable-state-check time) -- it was NOT affected by the new state guard and required
no changes. Only `TestLifecycleFullVariantsAreObservable` (which called Modify immediately after
Create, before any clock advance) needed updating, since it was asserting the pre-fix incorrect
behavior.

**2026-07-12 re-audit (superseded)**: the previous ledger's op-by-op table was accurate as far
as it went, but two real bugs were hiding behind blanket `wire: ok` / `status: ok` markings on
`users_and_user_groups` that a pure error-code/HTTP-status audit (that pass's focus) wouldn't
have caught -- they required an actual field-by-field diff of the response struct against
`types.User`/`types.UserGroup`, which is why this pass explicitly re-diffed every family's
wire shape rather than trusting prior "ok" statuses at face value, per this campaign's
instructions.
