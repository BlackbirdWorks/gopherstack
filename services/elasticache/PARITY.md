---
service: elasticache
sdk_module: aws-sdk-go-v2/service/elasticache@v1.56.4
last_audit_commit: 95db4e412
last_audit_date: 2026-08-10
overall: A            # gopherstack-nojq: wired UserGroup.ServerlessCaches (real
                       # reverse-association, same pattern as ReplicationGroups); added
                       # published-quota enforcement for CacheSubnetGroupQuotaExceeded/
                       # CacheSubnetQuotaExceededFault/ServerlessCacheQuotaForCustomer
                       # ExceededFault (real docs.aws.amazon.com quota-limits.html
                       # defaults, previously entirely unmodeled); confirmed
                       # RecurringCharges is genuinely unreproducible live-pricing state,
                       # not a modeling gap (see Notes). 2026-07-24 pass: implemented the
                       # two documented gaps from the
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
                       # placeholder model field existing. All fixed that pass (see
                       # Notes). 2026-07-25 pass: field-diffed serverless_caches (which
                       # the 2026-07-24 ledger marked "ok" without a full field diff) and
                       # found the SAME bug class again -- serverlessCacheXML only wired
                       # 5 of 13 real ServerlessCache fields, silently dropping
                       # CreateTime/DailySnapshotTime/KmsKeyId/MajorEngineVersion/
                       # SecurityGroupIds/SnapshotRetentionLimit/SubnetIds/UserGroupId
                       # from every Create/Modify/Delete/DescribeServerlessCache response
                       # despite the domain model already storing all of them; same for
                       # ServerlessCacheSnapshot's CreateTime. Both fixed. Grade held at
                       # A- rather than A because two real gaps remained: ServerlessCache.
                       # CacheUsageLimits and ServerlessCacheSnapshot's ExpiryTime/KmsKeyId/
                       # BytesUsedForCache/ServerlessCacheConfiguration were unmodeled.
                       # 2026-07-25 pass #2: implemented both gaps end to end (request
                       # parsing, backend state, response wire shape, persistence), verified
                       # via real SDK-client round trips per this campaign's "critical
                       # lesson" for this exact service. While wiring CacheUsageLimits'
                       # *request* path, found a much more severe, previously-undiscovered
                       # bug: the actual wire-routed CreateServerlessCache/
                       # ModifyServerlessCache handlers only ever parsed ServerlessCacheName/
                       # Description/Engine from the request, silently dropping every other
                       # real request field (KmsKeyId, DailySnapshotTime, MajorEngineVersion,
                       # SecurityGroupIds, SubnetIds, SnapshotRetentionLimit, UserGroupId,
                       # Tags, and now CacheUsageLimits) -- the response-side wire-shape fix
                       # from the prior pass was real, but nothing on the actual dispatched
                       # create/modify path ever populated those fields to begin with. Fixed
                       # by routing both handlers through the existing (previously
                       # test-only) CreateServerlessCacheFull/ModifyServerlessCacheFull
                       # backend methods. gaps: is now empty -- see Notes. The two
                       # `deferred:` items (data-plane snapshot restore fidelity,
                       # quota-exceeded faults) remain standard, reasoned emulator
                       # deferrals, not gaps: blockers, consistent with how this campaign
                       # treats equivalent deferred items in every other service.
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
  CreateCacheSubnetGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "(2026-08-10) now enforces CacheSubnetGroupQuotaExceeded (300/Region) and CacheSubnetQuotaExceededFault (20/group) -- AWS's documented default quotas, docs.aws.amazon.com/AmazonElastiCache/latest/dg/quota-limits.html"}
  DeleteCacheSubnetGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: code CacheSubnetGroupNotFound -> CacheSubnetGroupNotFoundFault (Fault suffix kept on the wire for this one; status stays 400)"}
  DescribeCacheSubnetGroups: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same code fix; MaxRecords [20,100] now enforced (400, matching this op's own NotFound status)"}
  ModifyCacheSubnetGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same code fix; (2026-08-10) now enforces CacheSubnetQuotaExceededFault (20/group) when the new subnet list would exceed it -- verified this op's error deserializer does NOT recognize CacheSubnetGroupQuotaExceeded (only Create does), so only the per-group cap applies here"}
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
  CreateServerlessCache: {wire: ok, errors: ok, state: ok, persist: ok, note: "(2026-08-10) now enforces ServerlessCacheQuotaForCustomerExceededFault (40/Region, AWS's documented default, quota-limits.html) -- both the wire-routed path (CreateServerlessCacheFull) and the legacy 3-arg CreateServerlessCache. (2026-07-25 #1) serverlessCacheXML was only wiring 5 of 13 real ServerlessCache fields (ARN/ServerlessCacheName/Description/Status/Engine + Endpoint/ReaderEndpoint) -- CreateTime/DailySnapshotTime/KmsKeyId/MajorEngineVersion/SecurityGroupIds/SnapshotRetentionLimit/SubnetIds/UserGroupId were silently dropped despite the domain model already storing all of them; fixed. (2026-07-25 #2) found a much more severe bug while wiring CacheUsageLimits: the wire-routed handler only ever parsed ServerlessCacheName/Description/Engine from the request and called the crippled 3-arg CreateServerlessCache backend method, silently dropping every other real request field on create (not just CacheUsageLimits -- KmsKeyId/DailySnapshotTime/MajorEngineVersion/SecurityGroupIds/SubnetIds/SnapshotRetentionLimit/UserGroupId/Tags too, despite the response-side wire-shape fix above being correct). Fixed by routing through CreateServerlessCacheFull; CacheUsageLimits now fully implemented (request parsing, backend storage, response wire shape)"}
  ModifyServerlessCache: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: ServerlessCacheNotFound -> ServerlessCacheNotFoundFault, 400->404; (2026-07-24) InvalidServerlessCacheStateFault guard added to both the wire-routed ModifyServerlessCache and the ModifyServerlessCacheFull variant; (2026-07-25 #1) same wire-shape fix as CreateServerlessCache; (2026-07-25 #2) same request-parsing fix as CreateServerlessCache -- now routes through ModifyServerlessCacheFull, threading UserGroupId/DailySnapshotTime/SnapshotRetentionLimit/SecurityGroupIds/CacheUsageLimits/RemoveUserGroup, previously all silently dropped on the real wire path"}
  DeleteServerlessCache: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same; (2026-07-24) InvalidServerlessCacheStateFault guard added; (2026-07-25) same wire-shape fix as CreateServerlessCache"}
  DescribeServerlessCaches: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same; MaxRecords [20,100] now enforced; handler deduped via describeListChecked; (2026-07-25) same wire-shape fix as CreateServerlessCache -- verified end to end via a real SDK client round trip, not just a backend-struct assertion (TestHandler_ServerlessCache_WireShapeFieldsSurfaced, extended this pass with CacheUsageLimits cases in TestHandler_ServerlessCache_NestedGapFields)"}
  CreateServerlessCacheSnapshot: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: ServerlessCacheNotFound code; ServerlessCacheSnapshotNotFoundFault status 400->404; (2026-07-25 #1) added missing CreateTime wire field (domain model already stored it as CreatedAt, never wired); (2026-07-25 #2) closed the ServerlessCacheSnapshot gap: now accepts+stores KmsKeyId (inherited from the source ServerlessCache when not explicitly given), sets BytesUsedForCache to the real (non-fabricated) value \"0\" (this emulator's serverless caches have no backing data-plane engine, unlike Cluster's embedded miniredis, so 0 is the literal true size of what it actually stores), and populates ServerlessCacheConfiguration from the source cache's Engine/MajorEngineVersion/Name at snapshot time. ExpiryTime deliberately stays unset: real AWS only sets it for automated snapshots, and every snapshot this emulator creates is \"manual\" (no background automated-snapshot scheduler exists)"}
  CopyServerlessCacheSnapshot: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: ServerlessCacheSnapshotNotFoundFault status 400->404"}
  DeleteServerlessCacheSnapshot: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same"}
  DescribeServerlessCacheSnapshots: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same; MaxRecords [20,100] now enforced; (2026-07-25 #1) CreateTime wire fix; (2026-07-25 #2) ExpiryTime/KmsKeyId/BytesUsedForCache/ServerlessCacheConfiguration wire fix, see CreateServerlessCacheSnapshot"}
  ExportServerlessCacheSnapshot: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateUser: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed (2026-07-24): DELETED gopherstack-invented `NoPasswordRequired` wire output field (types.User/CreateUserResult have no such field); now serializes the real Authentication{Type,PasswordCount} struct and UserGroupIds list. Handles AuthenticationMode.Type (password/no-password-required/iam, translated to output's password/no-password/iam) + AuthenticationMode.Passwords / legacy top-level Passwords (1-2, else InvalidParameterValue) + legacy NoPasswordRequired bool. New CreateUserWithAuth backend method carries the full model; CreateUser(bool) kept as a thin legacy wrapper so existing call sites are unaffected"}
  ModifyUser: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: UserNotFound 400->404; InvalidParameterValueException -> InvalidParameterValue; (2026-07-24) added AppendAccessString (was unhandled -- ModifyUserInput has both AccessString and AppendAccessString), Engine, and the same Authentication-model handling as CreateUser via new ModifyUserWithAuth"}
  DeleteUser: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: UserNotFound 400->404; (2026-07-24) response now includes Authentication/UserGroupIds like the other User-returning ops"}
  DescribeUsers: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same; (2026-07-24) Authentication/UserGroupIds wire fix (see CreateUser); MaxRecords [20,100] now enforced; handler deduped via describeListChecked"}
  CreateUserGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: code UserGroupAlreadyExistsFault -> UserGroupAlreadyExists; (2026-07-24) DELETED gopherstack-invented `Description` field (types.UserGroup/CreateUserGroupInput have no such field/param) from both input parsing and wire output; now wires the real ReplicationGroups field (reverse of a ReplicationGroup's UserGroupIds, computed fresh on every response -- was previously a dead, always-empty model field); (2026-08-10) now also wires the real ServerlessCaches field the same way (reverse of ServerlessCache.UserGroupId) -- see users_and_user_groups"}
  ModifyUserGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: UserGroupNotFound 400->404; (2026-07-24) ReplicationGroups wire fix (see CreateUserGroup); (2026-08-10) ServerlessCaches wire fix (see CreateUserGroup)"}
  DeleteUserGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same; (2026-07-24) Description removed, ReplicationGroups wired; (2026-08-10) ServerlessCaches wire fix (see CreateUserGroup)"}
  DescribeUserGroups: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same; (2026-07-24) Description removed, ReplicationGroups wired; MaxRecords [20,100] now enforced; handler deduped via describeListChecked; (2026-08-10) ServerlessCaches wire fix (see CreateUserGroup)"}
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
  PurchaseReservedCacheNodesOffering: {wire: partial, errors: ok, state: ok, persist: ok, note: "fixed: ReservedCacheNodesOfferingNotFound 400->404; ReservedCacheNodeAlreadyExists 409->404. Deferred (investigated 2026-08-10, gopherstack-nojq, not a fixable gap): RecurringCharges is always empty. Confirmed via the real API docs (API_ReservedCacheNodesOffering.html: RecurringCharges is an optional, undocumented-content array; API_DescribeReservedCacheNodesOfferings.html's own example response shows a NON-empty RecurringCharges for a Heavy-Utilization offering, RecurringChargeAmount 0.123/Hourly) that real AWS's RecurringCharges is live Price-List state tied to OfferingType/node-type/region/time, not a static per-shape default -- there is no published, deterministic algorithm to reproduce specific $ amounts, so leaving it empty rather than fabricating a number is the correct call under this campaign's no-fabrication rule. This emulator's 3 builtin offerings are all 'All Upfront' (see builtinReservedOfferings), for which an empty/zero recurring charge is the economically expected case anyway -- not verified against a live 'All Upfront' AWS response, but the closest defensible reading. See Notes."}
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
  cache_subnet_groups: {status: ok, note: "(2026-08-10) CacheSubnetGroupQuotaExceeded (300/Region) and CacheSubnetQuotaExceededFault (20/group) now enforced on Create/Modify -- see ops table"}
  cache_security_groups: {status: ok}
  snapshots: {status: ok, note: "automatic vs manual source tracked (SnapshotSource field), CopySnapshot real; CreateCacheCluster/CreateReplicationGroup SnapshotName restore was a genuine gap, now fixed (a prior pass)"}
  serverless_caches: {status: ok, note: "(2026-08-10) ServerlessCacheQuotaForCustomerExceededFault (40/Region) now enforced on Create -- see ops table. (2026-07-24) InvalidServerlessCacheStateFault guard on Modify/Delete. (2026-07-25 #1) MAJOR wire-shape fix, same bug class as 2026-07-24's users_and_user_groups fix: serverlessCacheXML only wired 5/13 real ServerlessCache fields and serverlessCacheSnapshotXML was missing CreateTime entirely, despite the domain model already storing everything needed. Verified via a real SDK-client round trip (TestHandler_ServerlessCache_WireShapeFieldsSurfaced), not just backend-struct assertions. ServerlessCache.CacheUsageLimits and ServerlessCacheSnapshot's ExpiryTime/KmsKeyId/BytesUsedForCache/ServerlessCacheConfiguration were left unmodeled (see gaps). (2026-07-25 #2) closed both gaps end to end, AND found+fixed a more severe bug while doing so: the wire-routed CreateServerlessCache/ModifyServerlessCache handlers only ever parsed 3 of the real ~12 request fields, so a real client's create/modify request lost almost all its data on the actual dispatched path even though the response-side mapping was already correct -- fixed by routing through the existing CreateServerlessCacheFull/ModifyServerlessCacheFull backend methods. gaps: now empty; see Notes and TestHandler_ServerlessCache_NestedGapFields"}
  users_and_user_groups: {status: ok, note: "(2026-08-10, gopherstack-nojq) UserGroup.ServerlessCaches (types.UserGroup.ServerlessCaches) is now wired -- the reverse of ServerlessCache.UserGroupId, computed fresh on every response via the new userGroupServerlessCacheIDsLocked (mirrors the existing userGroupReplicationGroupIDsLocked/ReplicationGroups pattern exactly). This supersedes the 2026-07-24 note below, which left it unwired on the (at-the-time correct) grounds that no association mechanism existed yet -- ServerlessCache.UserGroupID was added later that same day and the reverse lookup was simply never added alongside it. Verified end to end with a real elasticachesdk.Client round trip (TestHandler_UserGroup_ServerlessCachesWireShape) and a Snapshot/Restore persistence test (TestBackend_Persistence_UserGroupServerlessCaches), per this campaign's 'unit tests against gopherstack's own structs are not parity proof' rule. (2026-07-24) MAJOR wire-shape fix: User's Authentication{Type,PasswordCount} + UserGroupIds were entirely absent from the wire response (a gopherstack-invented NoPasswordRequired boolean stood in their place); UserGroup's real ReplicationGroups field was unwired and a gopherstack-invented Description field was serialized instead. The prior ledger's 'RBAC access string, authentication (password/IAM/NoPasswordRequired) all real' note was WRONG -- IAM/password auth type was never distinguishable on the wire, only a boolean. Both fixed; see ops table and Notes"}
  reserved_nodes: {status: ok, note: "RecurringCharges list always empty -- investigated 2026-08-10 (gopherstack-nojq) and confirmed this is genuinely unreproducible live AWS Price-List state, not a modeling gap; see PurchaseReservedCacheNodesOffering note and Notes"}
  service_updates_and_events: {status: ok, note: "DescribeEvents wire shape (Event/Events/Marker) verified against api-2.json exactly"}
  tags: {status: ok, note: "Add/Remove/List via ARN; ErrResourceNotFound correctly surfaces as InvalidARN (matches AWS's own tag-op behavior for a resource ARN that doesn't resolve)"}
  timestamps: {status: ok, note: "RFC3339 ISO8601 strings used throughout -- CORRECT for this query/XML protocol; do NOT flag as an epoch-seconds bug (awstime.Epoch is for json/rest-json protocols only, not applicable here)"}
gaps:
  - "(2026-08-10, sdk_module pin correction v1.51.11 -> v1.56.4) The SDK bump added fields this backend does not model: ServerlessCache gained NetworkType and StorageEncryptionType (now 19 real fields, not the 13 the 2026-07-25 'every wired field' claim below was field-diffed against); ReplicationGroup gained Durability/EffectiveDurability/StorageEncryptionType; Snapshot gained Durability. None of these four fields appear anywhere in this service's Go files. Not fixed this pass (pin-correction only, no behavior changes) -- flagging so the 'ServerlessCache is fully wired' and 'ReplicationGroup field-diffed' notes below aren't read as still-exhaustive."
  # Both gaps found 2026-07-25 are fixed as of the 2026-07-25 pass #2:
  #  - ServerlessCache.CacheUsageLimits: full DataStorage{Unit,Maximum,Minimum}/
  #    ECPUPerSecond{Maximum,Minimum} modeling, request parsing (query-protocol
  #    "CacheUsageLimits.DataStorage.*"/"CacheUsageLimits.ECPUPerSecond.*" fields,
  #    verified against awsAwsquery_serializeDocumentCacheUsageLimits/DataStorage/
  #    ECPUPerSecond), backend storage (CreateServerlessCacheFull/
  #    ModifyServerlessCacheFull), and response wire shape (cacheUsageLimitsXML).
  #  - ServerlessCacheSnapshot.ExpiryTime/KmsKeyId/BytesUsedForCache/
  #    ServerlessCacheConfiguration: KmsKeyId accepted on CreateServerlessCacheSnapshot
  #    (inherits from the source cache when absent), BytesUsedForCache set to the
  #    real value "0" (no fabrication -- this emulator has no data-plane engine
  #    backing serverless caches), ServerlessCacheConfiguration populated from the
  #    source cache's Engine/MajorEngineVersion/Name at snapshot time, ExpiryTime
  #    deliberately left unset (real AWS only sets it for automated snapshots, and
  #    this emulator never produces one -- see the ServerlessCacheSnapshot doc
  #    comment in models.go).
  # Both gaps in the 2026-07-12 ledger are fixed as of the 2026-07-24 pass:
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
  - "Full data-plane snapshot/restore fidelity (actual key-value RDB dump/reload through miniredis) -- CreateCacheCluster/CreateReplicationGroup SnapshotName validates existence and inherits engine/node-type metadata (the real API-contract behavior verified against api-2.json), but does not replay the source's actual key data into the restored miniredis instance. Investigated 2026-08-10 (gopherstack-nojq): CacheSnapshot (models.go) stores only metadata (engine/nodeType/etc), never key-value contents, so there is nothing to replay today; this is NOT a structural blocker -- github.com/alicebob/miniredis/v2's Miniredis.Keys()/DB(i) expose enough to enumerate and dump real key data at CreateSnapshot time. But a faithful implementation means handling every Redis data type (string/list/hash/set/sorted-set) plus TTLs, a new CacheSnapshot field to carry the dump through persistence, and replay logic on two call sites (CreateCacheCluster, CreateReplicationGroup) -- a genuine multi-type feature, not a small fix, and out of proportion to this pass's scope. Left deferred rather than half-implemented (e.g. strings-only) per parity-principles.md's no-stub rule."
  - "Quota-exceeded faults: PARTIALLY addressed 2026-08-10 (gopherstack-nojq) -- CacheSubnetGroupQuotaExceeded (300/Region), CacheSubnetQuotaExceededFault (20/subnet-group), and ServerlessCacheQuotaForCustomerExceededFault (40/Region) are now enforced, using AWS's own documented default quotas (docs.aws.amazon.com/AmazonElastiCache/latest/dg/quota-limits.html, fetched and quoted verbatim in this pass) and the exact Fault each operation's real error deserializer recognizes (aws-sdk-go-v2/service/elasticache@v1.51.11/deserializers.go). The same quota-limits.html table also publishes 'Nodes per Region' (300, Fault NodeQuotaForCustomerExceeded), 'Nodes per cluster' (60 Memcached / 90 cluster-mode, Fault NodeQuotaForClusterExceeded), 'Parameter groups per Region' (300, CacheParameterGroupQuotaExceeded), 'Users per Region' (2000, UserQuotaExceeded), 'User groups per Region' (200, UserGroupQuotaExceeded), 'Users per user group' (100), 'User groups per replication group' (1), and 'Serverless snapshots per day per cache' (24, ServerlessCacheSnapshotQuotaExceededFault) -- all real, deterministic, and CreateCacheCluster's own error deserializer confirms it recognizes NodeQuotaForCustomerExceeded/NodeQuotaForClusterExceeded specifically (not just ClusterQuotaForCustomerExceeded, which quota-limits.html does NOT publish a default for and is correctly left unimplemented, per parity-principles.md's no-fabrication rule). None of these were implemented this pass: the issue asked to rank and implement what could be done faithfully rather than attempt every quota shallowly, and the three chosen (both subnet-group ops, and serverless cache creation) were judged the cleanest, most directly client-reachable, single-resource-invariant wins. 'Nodes per Region' in particular is a cross-resource aggregate (must sum node counts across every cache cluster AND every replication group's node groups in the region, not just Table.Len() on one map) -- a materially bigger and more error-prone change than the three implemented, and was deliberately left for a focused follow-up rather than risked here. The single-resource ones (Parameter/User/UserGroup quotas, Nodes-per-cluster) follow the identical implementable pattern already proven by this pass (Table.Len()/field-len >= const -> sentinel error -> xmlError with the exact wire code) and are good next-pass candidates, not open questions."
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

**2026-07-25 pass -- serverless_caches wire-shape bugs (same class as 2026-07-24's
users_and_user_groups fix, found again on a family the previous pass had marked "ok")**:
this campaign's instruction is that an empty `gaps:`/clean `ok` status is not itself evidence
of parity, so `serverless_caches` (last touched 2026-07-24 only for its state-guard fix, not a
full field diff) was field-diffed fresh against
`aws-sdk-go-v2/service/elasticache@v1.51.11/types/types.go` and
`awsAwsquery_deserializeDocumentServerlessCache`/`awsAwsquery_deserializeDocumentServerlessCacheSnapshot`
directly. Found: `serverlessCacheXML` (`handler_serverless.go`) mapped only 5 of the real
`ServerlessCache` shape's 13 members (`ARN`/`ServerlessCacheName`/`Description`/`Status`/`Engine`,
plus `Endpoint`/`ReaderEndpoint`) -- `CreateTime`/`DailySnapshotTime`/`KmsKeyId`/
`MajorEngineVersion`/`SecurityGroupIds`/`SnapshotRetentionLimit`/`SubnetIds`/`UserGroupId` were
silently dropped from every `CreateServerlessCache`/`ModifyServerlessCache`/
`DeleteServerlessCache`/`DescribeServerlessCaches` response, despite the domain `ServerlessCache`
Go struct already storing every one of those values (`serverless.go` populates them at
create/modify time; they just were never read back out into the XML wire struct). This is
purely a missing-wire-mapping bug, not a missing-data gap -- confirmed by checking that
`ServerlessCache.KmsKeyID`/`UserGroupID`/`SubnetIDs`/etc. are all real, populated fields on the
domain model. Fixed by expanding `serverlessCacheXML` to cover every wired field, plus new
`securityGroupIDsXML`/`subnetIDsXML` wrapper types (their list items use dedicated per-list
element names `SecurityGroupId`/`SubnetId`, NOT the generic `member` locationName User's
`UserGroupIds` list uses -- verified against the deserializer, would have been a second,
subtler bug to get this wrong). `FullEngineVersion` was deliberately left unset rather than
synthesized from `Engine`+`MajorEngineVersion`, since no verified real format exists for that
combination and a plausible-but-wrong guess would violate parity-principles.md's
no-fabrication rule. Same missing-`CreateTime` bug found and fixed on
`serverlessCacheSnapshotXML`.

**Why the 2026-07-24 pass's `serverless_caches: ok` didn't catch this**: that pass's serverless
work was scoped to state-transition guards (a genuinely different concern -- when a mutation is
*allowed*, not what its *response* contains) and error-code fixes; it never re-diffed the
response body's field set against `types.ServerlessCache`, so the `ok` status was carried
forward from an earlier, less rigorous pass. This is the second time in two passes that a
blanket `ok`/`gaps: []` status masked a real, substantial wire-shape gap on this service --
worth remembering that "no gaps filed" and "clean field diff done" are not the same claim.

**Verification method note**: the existing `TestHandler_DescribeServerlessCache_UserGroupId`
test asserts `UserGroupID` directly against the Go-level backend struct returned by
`DescribeServerlessCaches`, not against the actual XML the SDK client parses -- it would NOT
have caught this bug (the backend struct always had the field; only the XML mapping was
missing). The new `TestHandler_ServerlessCache_WireShapeFieldsSurfaced`/
`TestHandler_ServerlessCacheSnapshot_CreateTimeSurfaced` tests drive a real generated
`elasticachesdk.Client` against an `httptest` server instead, exercising the actual wire
encode/decode round trip -- this is the same "unit tests are not parity proof" lesson
`parity-principles.md` rule 3 already documents from other services' sweeps, now reconfirmed
here.

**2026-07-25 pass #2 -- closing the CacheUsageLimits/ServerlessCacheSnapshot gaps, and a more
severe bug found while doing so**: implemented `ServerlessCache.CacheUsageLimits`
(`DataStorage{Unit,Maximum,Minimum}`/`ECPUPerSecond{Maximum,Minimum}`, field-diffed against
`types.CacheUsageLimits`/`types.DataStorage`/`types.ECPUPerSecond` and the query-protocol
request field names `CacheUsageLimits.DataStorage.*`/`CacheUsageLimits.ECPUPerSecond.*` via
`awsAwsquery_serializeDocumentCacheUsageLimits`/`DataStorage`/`ECPUPerSecond`) and
`ServerlessCacheSnapshot.ExpiryTime`/`KmsKeyId`/`BytesUsedForCache`/
`ServerlessCacheConfiguration` end to end.

While wiring `CacheUsageLimits` request parsing, found that `h.createServerlessCache`
(`handler_serverless.go`, the actual handler `CreateServerlessCache` dispatches to) only ever
read `ServerlessCacheName`/`Description`/`Engine` from the form and called the crippled 3-arg
`Backend.CreateServerlessCache` -- **every other real `CreateServerlessCacheInput` member was
silently dropped on the actual wire-routed create path**, including all the fields the
2026-07-25 pass #1 fix had just made correct on the *response* side. A probe test
(`client.CreateServerlessCache` with `KmsKeyId`/`DailySnapshotTime` set, then reading them back
from the same response) confirmed both came back empty. `h.modifyServerlessCache` had the same
bug (only `Description` was ever read). This is exactly the "critical lesson" flagged for this
service: `TestHandler_ServerlessCache_WireShapeFieldsSurfaced` (pass #1's regression test) seeds
the backend directly via `CreateServerlessCacheFull` and only checks that `DescribeServerlessCaches`
maps the response correctly -- it exercises the *response* wire shape, never the actual *request*
parsing path a real client's `CreateServerlessCache` call goes through, so it could not have
caught this. Fixed by routing both handlers through the existing (previously test-only)
`CreateServerlessCacheFull`/`ModifyServerlessCacheFull` backend methods, parsing the full real
request shape (`KmsKeyId`, `DailySnapshotTime`, `MajorEngineVersion`, `SecurityGroupIds.SecurityGroupId.N`,
`SubnetIds.SubnetId.N`, `SnapshotRetentionLimit`, `UserGroupId`, `RemoveUserGroup`, `Tags.Tag.N.Key/Value`,
and now `CacheUsageLimits`).

`ServerlessCacheSnapshot`'s three remaining fields: `BytesUsedForCache` is set to the literal,
non-fabricated value `"0"` -- this emulator's serverless caches have no backing data-plane
engine at all (unlike `Cluster`, which uses an embedded `miniredis` instance), so `0` is the
true size of what this emulator actually stores, not a guess. `KmsKeyId` is accepted on
`CreateServerlessCacheSnapshot` and defaults to the source `ServerlessCache`'s own `KmsKeyID`
when not explicitly given. `ServerlessCacheConfiguration` is populated from the source cache's
current `Engine`/`MajorEngineVersion`/`Name` at snapshot-creation time (`ServerlessCacheConfigSnapshot`
in `models.go`) -- genuine, already-available data, not a new feature. `ExpiryTime` is
deliberately left unset for every snapshot: real AWS only populates it for automatically-created
snapshots (expiry driven by the source cache's `SnapshotRetentionLimit`), never for manual or
copied ones, and this emulator's `CreateServerlessCacheSnapshot`/`CopyServerlessCacheSnapshot`
only ever produce `"manual"`-type snapshots (no background automated-snapshot scheduler exists,
a pre-existing, still-accurate `deferred:` item) -- so leaving it unset is the honestly-correct
value, not an incomplete one.

New table test `TestHandler_ServerlessCache_NestedGapFields` (`handler_serverless_test.go`)
drives a real `elasticachesdk.Client` against an `httptest` server for every case (per the
"critical lesson" instruction for this pass): request-field threading on create,
`CacheUsageLimits` on create and modify, `ServerlessCacheSnapshot` KMS-key inheritance vs
explicit override, and `BytesUsedForCache`/`ExpiryTime`/`ServerlessCacheConfiguration`.

**2026-08-10 pass (gopherstack-nojq) -- three follow-up items ranked, two implemented, one
investigated and confirmed unreproducible**:

1. **`UserGroup.ServerlessCaches` wired (implemented)**: the 2026-07-24 pass had explicitly left
   this unwired because "this emulator has no existing mechanism tracking that association
   anywhere" -- true when written, but `ServerlessCache.UserGroupID` was added to the domain
   model later that same pass (for the `serverless_caches` wire-shape fix) and the reverse
   lookup was simply never revisited. `types.UserGroup.ServerlessCaches` (verified against
   `aws-sdk-go-v2/service/elasticache@v1.51.11/types/types.go:2586` and the deserializer's
   `awsAwsquery_deserializeDocumentUGServerlessCacheIdList`, unlabeled `<member>` list, same
   wrapper as `ReplicationGroups`/`UserIds`) is now computed fresh on every `UserGroup` response
   via `userGroupServerlessCacheIDsLocked` (`user_groups.go`), mirroring
   `userGroupReplicationGroupIDsLocked` exactly. Wired into all four ops that return a
   `UserGroup` (Create/Modify/Delete/DescribeUserGroups). `TestHandler_UserGroup_
   ServerlessCachesWireShape` drives a real SDK client end to end (not a backend-struct
   assertion); `TestBackend_Persistence_UserGroupServerlessCaches` confirms the association
   survives Snapshot/Restore (the field itself isn't persisted -- like
   `AssignedReplicationGroupIDs`, it's recomputed from the already-persisted
   `ServerlessCache.UserGroupID` on every read, so nothing new needed adding to
   `persistence.go`).

2. **Quota-exceeded faults, partially implemented**: `docs.aws.amazon.com/AmazonElastiCache/
   latest/dg/quota-limits.html` was fetched directly (not recalled from memory -- an earlier
   attempt at recalling ElastiCache quotas via a summarizing web tool produced plausible-looking
   but fabricated numbers that did not match the real page; the page was re-fetched with a
   prompt to quote its table verbatim before any number was used here) and publishes concrete,
   deterministic default quotas: Nodes per Region (300), Nodes per cluster (60 Memcached / 90
   cluster-mode), Parameter groups per Region (300), **Subnet groups per Region (300)**,
   **Subnets per subnet group (20)**, **Serverless caches per Region (40)**, Serverless
   snapshots per day per cache (24), Users per Region (2000), User groups per Region (200),
   Users per user group (100), User groups per replication group (1). Cross-checked against
   `aws-sdk-go-v2/service/elasticache@v1.51.11/deserializers.go`'s per-operation error switch to
   confirm each corresponding `*QuotaExceeded(Fault)` type is actually recognized by the
   relevant Create/Modify op (not just declared somewhere in `types/errors.go`) and to get the
   exact wire `<Code>` string per op -- this mattered: `CacheSubnetGroupQuotaExceeded` (no
   `Fault` suffix on the wire) is recognized only by `CreateCacheSubnetGroup`, while
   `CacheSubnetQuotaExceededFault` (with `Fault` suffix) is recognized by both
   `CreateCacheSubnetGroup` and `ModifyCacheSubnetGroup`; getting the suffix or the
   per-op set wrong would have been a second, subtler bug. HTTP status (400 for all three) was
   confirmed against the API reference's per-error `Errors` section (`API_CreateCacheSubnetGroup.
   html`, `API_CreateServerlessCache.html`), fetched verbatim rather than assumed. Implemented:
   `CacheSubnetGroupQuotaExceeded`/`CacheSubnetQuotaExceededFault` on
   `CreateSubnetGroup`/`CreateSubnetGroupFull`/`ModifySubnetGroup`, and
   `ServerlessCacheQuotaForCustomerExceededFault` on `CreateServerlessCacheFull` (the real
   wire-routed create path) and the legacy `CreateServerlessCache`. `Test_ErrorWireShapesMatchAWS`
   (`handler_error_test.go`) gained three new table cases, each driving a real
   `elasticachesdk.Client` to the boundary and asserting the exact typed fault via
   `requireFault[T]` plus the HTTP status the SDK observed -- consistent with every other case in
   that table. Not implemented (see `deferred:`): the region-wide `Nodes per Region` quota (a
   cross-resource aggregate across cache clusters AND replication group node groups, materially
   more work than the three single-resource quotas done here) and the remaining single-resource
   quotas (Parameter groups, Users, UserGroups, Nodes-per-cluster), left as scoped, well-cited
   next-pass candidates rather than attempted shallowly across all nine in one pass.

3. **`RecurringCharges` always empty -- investigated, confirmed genuinely unreproducible, not
   fixed**: `API_ReservedCacheNodesOffering.html` documents `RecurringCharges` as an optional
   array with no specified default content. Critically, `API_DescribeReservedCacheNodesOfferings.
   html`'s own worked example response shows a **non-empty** `RecurringCharges` for a
   `Heavy Utilization` offering (`RecurringChargeAmount` 0.123, `Hourly`) -- proving real AWS's
   value is genuinely offering-dependent, live Price-List state, not a static per-shape default
   that could be hardcoded once and be correct. There is no published algorithm or fixed table
   mapping `OfferingType`/`CacheNodeType`/region to a specific recurring-charge dollar amount
   anywhere in the SDK or API docs (unlike the quotas above, which ARE published constants) --
   inventing one would be exactly the fabrication `parity-principles.md` and this issue's
   instructions forbid. This emulator's three `builtinReservedOfferings()` are all `All Upfront`
   (`reserved_nodes.go`), for which a zero/absent recurring charge is the economically expected
   case (all cost paid upfront, no ongoing charge) -- a defensible reading of the existing
   behavior, though not verified against a live `All Upfront` AWS response (none was found in
   the docs; only the `Heavy Utilization` example exists, and that offering type is legacy/
   pre-2017 and not modeled by this emulator at all). Left unchanged: this is the "genuinely
   unreproducible" case, not the "wrongly assumed absent" case, per this issue's framing.
