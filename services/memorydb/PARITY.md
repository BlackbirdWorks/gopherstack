---
service: memorydb
sdk_module: aws-sdk-go-v2/service/memorydb@v1.36.4
last_audit_commit: 437393d5
last_audit_date: 2026-07-31
overall: A            # 2026-07-31: pkgs/sdkcheck reverse check found ExportSnapshot wrongly advertised/documented as a real SDK op (it isn't -- MemoryDB has no export-to-S3 API at all; see its ops-block note). Corrected, route left wired as internal test scaffolding. Grade held at A: unreachable by real traffic either way, since MemoryDB dispatches purely by X-Amz-Target and no real client can send this target.
                       # 2026-07-23: this pass: field-diffed every core response/request wire type
                       # (Cluster, MultiRegionCluster/RegionalCluster, ReservedNode/
                       # ReservedNodesOffering, User, SubnetGroup/Subnet, ParameterGroup/
                       # Parameter/MultiRegionParameter, Snapshot, EngineVersion,
                       # ServiceUpdate) against deserializers.go's authoritative case
                       # lists. Found and fixed 10 gopherstack-INVENTED fields/filters that
                       # do not exist anywhere in the real SDK (deleted per the no-fabrication
                       # rule), 6 real fields missing from the wire shape (added), a
                       # HTTP-status-code gap (confirmed via aws-sdk-go v1's api-2.json model
                       # and fixed), 2 latent Source-not-set bugs, a request/response
                       # value-space mismatch, and implemented the previously-deferred
                       # Cluster.Status creating->available lifecycle (opt-in, default-off).
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: clusterObject dropped 3 fabricated fields (Tags, MultiRegionParameterGroupName, NumberOfReplicasPerShard -- none exist on real types.Cluster, confirmed via awsAwsjson11_deserializeDocumentCluster's 29-key case list); added the real MultiRegionClusterName request field (was parsed nowhere) with FK validation against an existing multi-Region cluster; Status now supports the opt-in creating->available lifecycle overlay (see families.lifecycle)."}
  DescribeClusters: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now uses the paginateItems helper (handler.go) like every other list op, replacing the hand-rolled cursor loop; Status overlay applied per lifecycle.go."}
  DeleteCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchUpdateCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  FailoverShard: {wire: ok, errors: ok, state: ok, persist: ok, note: "no-op failover simulation (event only); acceptable for a mock, matches other services' failover stubs"}
  ListAllowedNodeTypeUpdates: {wire: ok, errors: ok, state: ok, persist: n/a, note: "re-verified: ListAllowedNodeTypeUpdatesOutput is exactly ScaleUpNodeTypes/ScaleDownNodeTypes, matches"}
  CreateACL: {wire: ok, errors: ok, state: ok, persist: ok, note: "re-verified: aclObject (ARN, Clusters, MinimumEngineVersion, Name, PendingChanges, Status, UserNames) matches types.ACL's 7-key deserializer case list exactly"}
  DescribeACLs: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteACL: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateACL: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateSubnetGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: subnetGroupObject/subnetEntry were missing SupportedNetworkTypes (both group- and subnet-level) and each Subnet's AvailabilityZone -- all confirmed real fields via awsAwsjson11_deserializeDocumentSubnetGroup/...Subnet. Added with sensible mock defaults (ipv4-only, round-robin AZs derived from the group's ARN region)."}
  DescribeSubnetGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteSubnetGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateSubnetGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateUser: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: userObject dropped a fabricated \"Engine\" field -- confirmed absent from types.User's 7-key deserializer case list (AccessString, ACLNames, ARN, Authentication, MinimumEngineVersion, Name, Status)"}
  DescribeUsers: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteUser: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateUser: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateParameterGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "re-verified: parameterGroupObject (ARN, Description, Family, Name) matches types.ParameterGroup's 4-key deserializer case list exactly"}
  DescribeParameterGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteParameterGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateParameterGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeParameters: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed: parameterObject dropped fabricated \"ChangeType\"/\"Source\" fields -- confirmed absent from types.Parameter's 6-key deserializer case list (AllowedValues, DataType, Description, MinimumEngineVersion, Name, Value)"}
  ResetParameterGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTags: {wire: ok, errors: ok, state: ok, persist: n/a}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateSnapshot: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: snapshotObject's top-level \"SnapshotCreationTime\" and \"SnapshotType\" were both fabricated at this level -- confirmed types.Snapshot's real deserializer case list is only ARN, ClusterConfiguration, DataTiering, KmsKeyId, Name, Source, Status (7 keys). SnapshotCreationTime actually belongs to types.ShardDetail, nested inside ClusterConfiguration.Shards (not modeled, see gaps); SnapshotType duplicated Source and was deleted service-wide (internal Snapshot.SnapshotType field removed too). Added the real, previously-missing DataTiering field, populated from the source cluster."}
  DescribeSnapshots: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: Source request filter previously string-compared directly against internal automated/manual storage values, but DescribeSnapshotsInput.Source's real accepted values are \"system\"/\"user\" (per its own doc comment) -- a real client's Source=system/user would have matched zero snapshots. normalizeSnapshotSource (snapshots.go) now maps system->automated, user->manual, while still leniently accepting automated/manual directly."}
  CopySnapshot: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: dst snapshot never set Source at all (only the now-deleted SnapshotType), so a Source-filtered DescribeSnapshots would never match a copied snapshot -- a real state bug, not just wire-label. Now sets Source and carries DataTiering forward from the source snapshot."}
  DeleteSnapshot: {wire: ok, errors: ok, state: ok, persist: ok}
  # ExportSnapshot is intentionally NOT listed as an advertised SDK op here.
  # 2026-07-31 CORRECTION: the row that used to live at this position ("wire:
  # ok, ...", "mock export ... matches other services") was inaccurate --
  # ExportSnapshot is not a real AWS MemoryDB SDK operation at all (verified
  # against botocore's memorydb service-2.json: only CopySnapshot/
  # CreateSnapshot/DeleteSnapshot/DescribeSnapshots exist under the snapshot
  # family; MemoryDB, unlike ElastiCache, has no export-to-S3 API). Caught by
  # pkgs/sdkcheck's reverse check (commit 12cfe14d5; gopherstack-vhw2 category
  # A). MemoryDB dispatches purely by X-Amz-Target header value, so a real
  # client can never send this target and the route (a validate-and-return
  # no-op) was already unreachable by real traffic; it stays wired as
  # internal test scaffolding, unadvertised. See handler.go's comment on the
  # GetSupportedOperations() entry. Same resolution as DAX's
  # ResetParameterGroup and EMR's ListTagsForResource.
  DescribeEngineVersions: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed: engineVersionObject dropped a fabricated \"Description\" field -- confirmed absent from types.EngineVersionInfo's 4-key deserializer case list (Engine, EnginePatchVersion, EngineVersion, ParameterGroupFamily); kept internally on the EngineVersion model as seed-table documentation only."}
  DescribeEvents: {wire: ok, errors: ok, state: ok, persist: ok, note: "re-verified: eventObject (Date, Message, SourceName, SourceType) matches types.Event's 4-key deserializer case list exactly"}
  CreateMultiRegionCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: multiRegionClusterObject was missing the real \"Clusters\" ([]RegionalCluster) and \"TLSEnabled\" fields -- both confirmed on types.MultiRegionCluster. Clusters is now populated from actual per-Region Cluster records referencing this multi-Region cluster by name (RegionalClustersFor, multi_region_clusters.go)."}
  DeleteMultiRegionCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeMultiRegionClusters: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: ShowClusterDetails was parsed but never gated anything (multiRegionClusterObject had no Clusters field to gate); now mirrors DescribeClusters' ShowShardDetails convention -- Clusters is populated only when ShowClusterDetails is true."}
  UpdateMultiRegionCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAllowedMultiRegionClusterUpdates: {wire: ok, errors: ok, state: ok, persist: n/a}
  DescribeMultiRegionParameterGroups: {wire: ok, errors: ok, state: ok, persist: ok, note: "re-verified: multiRegionParameterGroupObject (ARN, Description, Family, Name) matches types.MultiRegionParameterGroup's 4-key deserializer case list exactly"}
  DescribeMultiRegionParameters: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed: previously reused parameterObject (types.Parameter's shape), silently dropping \"Source\" -- confirmed types.MultiRegionParameter is a DISTINCT shape from types.Parameter that additionally carries Source (values: user | system | engine-default). New multiRegionParameterObject type added for this op only."}
  DescribeReservedNodes: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: describeReservedNodesRequest dropped a fabricated \"ReservedNodeId\" filter -- real DescribeReservedNodesInput has only ReservationId (confirmed via api_op_DescribeReservedNodes.go), no ReservedNodeId at all."}
  DescribeReservedNodesOfferings: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed: ReservedNodesOffering dropped a fabricated \"UsagePrice\" field -- confirmed absent from types.ReservedNodesOffering's 6-key deserializer case list (Duration, FixedPrice, NodeType, OfferingType, RecurringCharges, ReservedNodesOfferingId)."}
  PurchaseReservedNodesOffering: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: ReservedNode had a fabricated \"ReservedNodeId\" field (used as the internal store key and filter target) and was MISSING the real \"ReservedNodesOfferingId\" field entirely -- confirmed types.ReservedNode has no ReservedNodeId at all (11-key deserializer case list: ARN, Duration, FixedPrice, NodeCount, NodeType, OfferingType, RecurringCharges, ReservationId, ReservedNodesOfferingId, StartTime, State). Also fixed a values-swapped bug where the response's ReservationId field actually held the offering ID and vice versa. Also dropped the fabricated \"UsagePrice\" field (same as the offering type)."}
  DescribeServiceUpdates: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: serviceUpdateObject was missing the real \"Engine\" field (confirmed on types.ServiceUpdate); added. types.ServiceUpdate also has \"ClusterName\"/\"NodesUpdated\" (a real ServiceUpdate is scoped per-cluster) which are NOT modeled here -- this backend treats service updates as global objects with no cluster association, so populating those would require fabricating placeholder data; left as a gap rather than faked (see gaps). DescribeServiceUpdatesInput.ClusterNames is parsed but not applied as a filter for the same reason."}
families:
  errors: {status: ok, note: "systemic fix (prior pass): writeBackendError previously collapsed every NotFound/AlreadyExists/Conflict error into fabricated generic types that do not exist anywhere in MemoryDB's real API surface. errCodeLookup (handler.go) maps each of the package's 19 sentinel errors to its confirmed real __type."}
  http_status_codes: {status: ok, note: "fixed this pass: errCodeLookup's HTTP statuses were a 404/409/400 categorization (NotFound->404, AlreadyExists/InUse->409); confirmed via aws-sdk-go v1's model (models/apis/memorydb/2021-01-01/api-2.json) that every one of MemoryDB's ~53 exception shapes has an empty \"error\" trait ({}, no httpStatusCode override) -- the JSON-protocol default for an unoverridden client-fault shape is 400, meaning real AWS returns 400 uniformly for every fault in this service, never 404 or 409. Also confirmed deserializers.go resolves error identity purely from the __type/code field (resolveProtocolErrorType), never from response.StatusCode, so this had zero effect on real-client error-type resolution either way -- but the status code itself was wrong. errCodeLookup and the coarse category-based fallback in writeBackendError both now use http.StatusBadRequest uniformly; ~59 existing test assertions (404/409 -> 400) updated across every affected test file, including 2 raw-int-literal (404/409, not http.Status*) test files that a naive grep for the named constants would have missed."}
  fabricated_fields: {status: ok, note: "systemic sweep this pass: field-diffed every core wire type's Go struct against its own deserializers.go case list (the authoritative source -- types.go doc comments alone were double-checked against this since a prior finding showed a doc-comment-derived field can still be wrong about which TYPE it belongs to). Found and DELETED 10 gopherstack-invented fields/filters that appear nowhere in the real SDK: ReservedNode.ReservedNodeId (used as the internal store/filter key -- also removed from describeReservedNodesRequest), ReservedNode.UsagePrice, ReservedNodesOffering.UsagePrice, Cluster response's Tags/MultiRegionParameterGroupName/NumberOfReplicasPerShard (3 fields), User response's Engine, Parameter's ChangeType/Source (2 fields), Snapshot response's top-level SnapshotType, Snapshot response's top-level SnapshotCreationTime (the real field of that name belongs to a different, nested type -- types.ShardDetail -- not top-level Snapshot), EngineVersionInfo response's Description. Also ADDED 6 real fields that were missing: ReservedNode.ReservedNodesOfferingId, SubnetGroup/Subnet.SupportedNetworkTypes + Subnet.AvailabilityZone (3 fields), Snapshot.DataTiering, ServiceUpdate.Engine, MultiRegionCluster.Clusters + TLSEnabled (2 fields), MultiRegionParameter.Source (via a new distinct multiRegionParameterObject type, since types.MultiRegionParameter is NOT the same shape as types.Parameter). Every deletion/addition is cited against its specific deserializers.go function and confirmed field/absent-field."}
  lifecycle: {status: ok, note: "implemented this pass (was gap: \"Cluster.Status is always available immediately\"): goroutine-free creating->available status overlay, mirroring services/elasticache/lifecycle.go's mechanism exactly. A Cluster records a transient PendingStatus + AvailableAt deadline (markCreatingLocked); every read/write path that surfaces a Cluster (clusterView) overlays the transient status until the backend clock (injectable via SetClock, for deterministic tests) passes the deadline. Default lifecycleDelay is zero -> transitions are instant, so this is 100% backward compatible with every pre-existing test; opt in via SetLifecycleDelay. Does not (yet) implement deleting-state simulation or shard/node-level transitions, only cluster creating->available -- scoped to exactly what the prior pass's gap called out."}
  timestamps: {status: ok, note: "prior pass: 5 TStamp wire-shape bugs fixed (Event.Date, ReservedNode.StartTime, ServiceUpdate.ReleaseDate/AutoUpdateStartDate, and what was believed to be Snapshot.SnapshotCreationTime). This pass found the last one was fixed at the WRONG location in the wire shape -- SnapshotCreationTime is not a top-level Snapshot field at all; see fabricated_fields. The epoch-seconds format fix itself was correct, just misplaced; the field is now deleted from the top level rather than moved to its real location (types.ShardDetail nested in ClusterConfiguration.Shards), which remains unmodeled -- see gaps."}
  pointer_aliasing: {status: ok, note: "prior pass, still holds: Create*/Copy*/Export* ops clone before returning."}
  persistence: {status: ok, note: "Handler exposes Snapshot(ctx)/Restore(ctx,[]byte) delegating straight to InMemoryBackend; backendSnapshot versioning (memorydbSnapshotVersion, still 1) unaffected by this pass's field additions/removals -- all additive/subtractive struct field changes are backward/forward compatible with encoding/json's default zero-value behavior, no version bump needed."}
  route_matcher: {status: ok, note: "unchanged this pass: single X-Amz-Target-prefixed POST endpoint, all GetSupportedOperations entries reachable through dispatch."}
gaps:                     # known divergences NOT fixed this pass
  - "ClusterConfiguration.Shards ([]ShardDetail) is not modeled: real AWS's Snapshot.ClusterConfiguration carries a full per-shard array (Configuration/ShardConfiguration sub-object with Slots/ReplicaCount, Name, Size, SnapshotCreationTime -- confirmed via types.ShardDetail and its deserializer). snapshotClusterConfig has none of this. Not fixed: would require designing new shard-level snapshot metadata tracking this backend does not currently have (Size/per-shard SnapshotCreationTime aren't derivable from anything already tracked); fabricating plausible-looking values would itself violate the no-stub rule."
  - "ServiceUpdate.ClusterName/NodesUpdated are not modeled: a real ServiceUpdate entry is scoped to one specific cluster it applies to (the real API effectively returns one row per (update, affected cluster) pair). This backend models service updates as global objects with zero cluster association, so DescribeServiceUpdatesInput.ClusterNames is parsed but not applied as a filter, and the two per-cluster fields cannot be populated without fabricating an association. Not fixed: needs real design work on how service updates relate to clusters, not a shallow field add."
  - "DescribeSnapshotsInput.ShowDetail (real field, gates whether ClusterConfiguration is included in the response, mirroring ShowShardDetails/ShowClusterDetails elsewhere in this service) is not implemented -- ClusterConfiguration is currently always included when non-empty, with no request-side toggle. Not fixed this pass; same shape of fix as the ShowClusterDetails work done for DescribeMultiRegionClusters, just not reached."
deferred:                 # consciously not audited this pass (scope) -- next pass targets
  - "Byte-for-byte audit of nested shardObject/nodeObject beyond the fields already spot-checked (Name, Status, Slots, Nodes, NumberOfNodes on Shard; AvailabilityZone, CreateTime, Endpoint, Name, Status on Node) -- these matched exactly against types.Shard/types.Node's deserializer case lists when checked this pass, but the full request-shape interaction with real Slots math (16384 keyspace distribution) was not independently verified against live AWS."
  - "MultiRegionCluster.Clusters' RegionalCluster.Status semantics beyond \"reflects the underlying Cluster.Status\" -- real AWS may report a distinct Region-membership status (e.g. \"active\"/\"creating\"/\"deleting\" scoped to the multi-Region relationship itself) rather than just mirroring the Regional cluster's own Status; not independently confirmable without live AWS."
  - "PurchaseReservedNodesOffering / DescribeReservedNodesOfferings RecurringCharges frequency/amount realism (values are mock placeholders, e.g. \"Hourly\") -- wire shape (RecurringChargeAmount/RecurringChargeFrequency) confirmed correct, but the actual pricing data is illustrative, not derived from any real AWS price list (same caveat as every other service's pricing mocks)."
leaks: {status: clean, note: "no goroutines, timers, or janitor loops added this pass -- the new lifecycle.go overlay mechanism is pure functions over stored fields (PendingStatus/AvailableAt/clock), identical in shape to services/elasticache's proven goroutine-free design; b.mu remains the sole coarse lock (still a plain sync.RWMutex, not lockmetrics.RWMutex -- a pre-existing convention deviation from the pkgs/lockmetrics rule, not introduced or fixed this pass, flagged for a future pass since changing the lock type across ~30 call sites is out of scope for a wire-parity pass)."}
---

## Notes

**Protocol**: awsjson1.1 (`X-Amz-Target: AmazonMemoryDB.<Op>`), single POST endpoint.
Confirmed against `aws-sdk-go-v2/service/memorydb@v1.33.12`'s `deserializers.go`/`serializers.go`.

**This pass's method**: for every core wire type, extracted the authoritative field list
directly from its own `awsAwsjson11_deserializeDocument<Type>` function in `deserializers.go`
(the literal `case "FieldName":` list a real client's JSON deserializer recognizes) rather than
trusting `types.go`'s doc comments alone, then diffed that list 1:1 against gopherstack's Go
struct. This caught a class of bug the prior pass's doc-comment-based review missed: a field
name can be *real* (e.g. `SnapshotCreationTime`) while being attached to the *wrong type* in
gopherstack's wire shape (it belongs to `types.ShardDetail`, nested inside
`Snapshot.ClusterConfiguration.Shards`, not top-level `Snapshot`) -- syntactically identical to
a fabricated field from a wire-correctness standpoint (a real client's top-level `Snapshot`
deserializer has no such key and would simply ignore it), but a different root cause than an
outright invention like `ReservedNode.ReservedNodeId`.

**Ten fabricated fields deleted, six real fields added** -- full list and citations in
`families.fabricated_fields` above. Two of the fabricated fields were more than cosmetic:
`ReservedNode.ReservedNodeId` was the *store key* (used for lookups/filtering), and removing it
required re-keying `store_setup.go`'s `reservedNodeKeyFn` onto the real `ReservationId` field and
fixing a values-swapped bug in `PurchaseReservedNodesOffering` (the response's `ReservationId`
field actually held the *offering* ID and vice versa -- a real state-correctness bug, not just a
label issue). `Snapshot.SnapshotType` (fabricated, duplicated `Source`) was set independently of
`Source` at two call sites (`CopySnapshot`, `DeleteClusterWithSnapshot`'s final-snapshot path)
that never set `Source` at all -- meaning a `Source`-filtered `DescribeSnapshots` would silently
never match snapshots created via those two paths. Deleting `SnapshotType` and consolidating on
`Source` as the single source of truth fixed this as a side effect.

**`DescribeSnapshotsInput.Source`'s real values are `"system"`/`"user"`, not
`"automated"`/`"manual"`**: confirmed via `api_op_DescribeSnapshots.go`'s doc comment ("If set to
system... If set to user..."), while the *response* field `Snapshot.Source` documents its own
values as `"automated"`/`"manual"`. This is a genuine asymmetry in real AWS's own API (the
request-side filter accepts different strings than what the response echoes back). A real client
filtering by `Source: "system"` would previously have matched zero snapshots, since this backend
string-compared the raw filter value against its internal `"automated"`/`"manual"` storage.
`normalizeSnapshotSource` (`snapshots.go`) now maps the real request values to the internal
storage convention while still leniently accepting `"automated"`/`"manual"` directly (a caller
passing the response-side value back in as a filter is a reasonable, harmless thing to support).

**HTTP status codes are uniformly 400, not 404/409/400 by category**: confirmed via
`aws-sdk-go` v1's model file (`models/apis/memorydb/2021-01-01/api-2.json`) -- every one of
MemoryDB's ~53 exception shapes has an empty `"error"` trait (no `httpStatusCode` override), and
the JSON-protocol default for an unoverridden client-fault shape is 400. This has zero effect on
a real `aws-sdk-go-v2` client's typed-error resolution (`deserializers.go` resolves error
identity purely from the `__type`/`code` field, confirmed by reading the top of every
`awsAwsjson11_deserializeOpError*` function -- `response.StatusCode` is never consulted for that
purpose), but the status code on the wire itself was wrong relative to real AWS. Fixed in both
`errCodeLookup` and the coarse category-based fallback in `writeBackendError`; ~59 existing test
assertions across 13 test files updated from 404/409 to 400 (including two files using raw int
literals `404`/`409` rather than the named `http.Status*` constants, which a naive
grep-and-replace on the constant names alone would have missed).

**Cluster.Status now supports an opt-in creating->available lifecycle** (`lifecycle.go`),
closing the prior pass's largest deferred gap. Mirrors `services/elasticache/lifecycle.go`'s
proven goroutine-free design exactly: `SetLifecycleDelay`/`SetClock` are no-ops by default (zero
delay = instant transition, identical to every pre-existing test's expectations), and only
activate the `PendingStatus`/`AvailableAt` overlay when a test explicitly opts in. Scoped
narrowly to Cluster creation (the exact gap that was called out) -- does not add deleting-state
simulation, shard/node-level transitions, or apply to other resource types.

**Error `__type` is resource-specific, never generic** (prior pass, still holds): MemoryDB's
error model (`types/errors.go`) defines ~55 fault types and *zero* generic ones.
`errCodeLookup` (`handler.go`) maps each of the package's 19 sentinel errors to its confirmed
real `__type`.

**In-use state faults use `Invalid*StateFault`, not a made-up `*InUseFault`** (prior pass, still
holds): `DeleteSubnetGroup` is the one exception with a dedicated `SubnetGroupInUseFault`.

**Timestamps are epoch-seconds JSON numbers, not RFC3339 strings** (prior pass, still holds, see
`families.timestamps` above for this pass's SnapshotCreationTime correction).

**`Authentication.Type` output enum is `password | no-password | iam`** (prior pass, still
holds), never `no-password-required`.

**Not real bugs, ruled out this pass** (documented so a future auditor doesn't re-flag them):
`ACL`/`ParameterGroup`/`MultiRegionParameterGroup`/`Event`/`ListAllowedNodeTypeUpdatesOutput`
wire shapes were all re-verified field-for-field against their deserializers.go case lists and
found to already match exactly, with zero fabricated or missing fields -- these are genuinely
clean, not merely unaudited. (2026-07-31 correction: the note previously here comparing
`ExportSnapshot`'s mock export to "every other service's snapshot-export mock" was itself
wrong -- `ExportSnapshot` is not a real MemoryDB operation at all; see its ops-block entry
above.) `b.mu` being a plain `sync.RWMutex` rather
than `pkgs/lockmetrics.RWMutex` is a pre-existing convention deviation, not a leak or
correctness bug (every lock path is still properly `defer`-released) -- flagged under `leaks`
for a future pass rather than churned here, since retrofitting the metrics wrapper across ~30
call sites is unrelated to wire-shape parity and carries its own regression risk.
