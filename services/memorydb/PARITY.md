---
service: memorydb
sdk_module: aws-sdk-go-v2/service/memorydb@v1.33.12
last_audit_commit: 437393d5
last_audit_date: 2026-07-12
overall: A            # genuine fixes found across auth-type wire value, 5 epoch-timestamp
                       # fields (request+response), a systemic error-__type bug affecting
                       # every NotFound/AlreadyExists/InUse error in the service, a missing
                       # SubnetGroupInUse state check, and pointer-aliasing hygiene.
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeClusters: {wire: ok, errors: ok, state: ok, persist: ok, note: "hand-rolled cursor pagination instead of the paginateItems helper other ops use; functionally fine, just inconsistent style (not fixed, see gaps)"}
  DeleteCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchUpdateCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  FailoverShard: {wire: ok, errors: ok, state: ok, persist: ok, note: "no-op failover simulation (event only); acceptable for a mock, matches other services' failover stubs"}
  ListAllowedNodeTypeUpdates: {wire: ok, errors: ok, state: ok, persist: n/a}
  CreateACL: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeACLs: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteACL: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: was reporting the fabricated ACLInUseFault; now InvalidACLStateFault (real AWS fault, confirmed against DeleteACL's op-specific error list in deserializers.go)"}
  UpdateACL: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateSubnetGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeSubnetGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteSubnetGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: was missing any in-use check at all -- deleted subnet groups still referenced by a live cluster; now returns SubnetGroupInUseFault (confirmed real fault name)"}
  UpdateSubnetGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateUser: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: Authentication.Type wire value was the fabricated no-password-required; real AWS enum only has password|iam|no-password"}
  DescribeUsers: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteUser: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: UserInUseFault (fabricated) -> InvalidUserStateFault (real, confirmed against DeleteUser's op-specific error list)"}
  UpdateUser: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: previously stored AuthenticationMode.Type verbatim with no validation/normalization, unlike CreateUser -- now shares CreateUser's normalizeAuthType/validateAuthPasswordCombo"}
  CreateParameterGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeParameterGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteParameterGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateParameterGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeParameters: {wire: ok, errors: ok, state: ok, persist: n/a}
  ResetParameterGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTags: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed: fabricated ResourceNotFoundFault -> InvalidARNFault (the only NotFound-family fault ListTags/TagResource/UntagResource's models actually define)"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateSnapshot: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: SnapshotCreationTime was an RFC3339 string; real TStamp shape is epoch seconds (confirmed via deserializers.go ParseEpochSeconds)"}
  DescribeSnapshots: {wire: ok, errors: ok, state: ok, persist: ok}
  CopySnapshot: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteSnapshot: {wire: ok, errors: ok, state: ok, persist: ok}
  ExportSnapshot: {wire: ok, errors: ok, state: ok, persist: n/a, note: "mock export (no real S3 write); acceptable, matches other services"}
  DescribeEngineVersions: {wire: ok, errors: ok, state: ok, persist: n/a}
  DescribeEvents: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: both request (StartTime/EndTime) and response (Date) TStamp fields were RFC3339 strings; a real client sending StartTime/EndTime would have gotten a SerializationException on every call. Now json.Number request parsing + awstime.Epoch response, matching DescribeEventsInput's real epoch-seconds serialization."}
  CreateMultiRegionCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteMultiRegionCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeMultiRegionClusters: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateMultiRegionCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAllowedMultiRegionClusterUpdates: {wire: ok, errors: ok, state: ok, persist: n/a}
  DescribeMultiRegionParameterGroups: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: ErrMultiRegionParameterGroupNotFound's embedded fault name was literally wrong (\"ParameterGroupNotFoundFault\" instead of \"MultiRegionParameterGroupNotFoundFault\", a real, distinct SDK type)"}
  DescribeMultiRegionParameters: {wire: ok, errors: ok, state: ok, persist: n/a}
  DescribeReservedNodes: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeReservedNodesOfferings: {wire: ok, errors: ok, state: ok, persist: n/a}
  PurchaseReservedNodesOffering: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: StartTime was an RFC3339 string; real TStamp shape is epoch seconds"}
  DescribeServiceUpdates: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: ReleaseDate/AutoUpdateStartDate were date-only strings (\"2024-06-01\"); real TStamp shapes are epoch seconds -- a real client would have failed to deserialize every DescribeServiceUpdates response"}
families:
  errors: {status: ok, note: "systemic fix: writeBackendError previously collapsed every NotFound/AlreadyExists/Conflict error into fabricated generic types (ResourceNotFoundException/ResourceInUseException/InvalidRequestException) that do not exist anywhere in MemoryDB's real API surface -- it has no generic fault types at all, every fault is resource-specific (ClusterNotFoundFault, ACLNotFoundFault, ...). Added errCodeLookup (handler.go) mapping each of the package's ~19 sentinel errors to its confirmed real __type from aws-sdk-go-v2/service/memorydb/types/errors.go; a real client's errors.As(&types.ClusterNotFoundFault{}) would never have matched before this fix. HTTP status codes were left as the pre-existing 404/409/400 categorization (not changed to AWS's uniform 400-for-all-client-faults) since the SDK resolves error identity from the __type/code field, not the HTTP status, and 59 existing tests already assert on the pre-existing status codes."}
  timestamps: {status: ok, note: "5 separate TStamp wire-shape bugs fixed (Event.Date request+response, ReservedNode.StartTime, ServiceUpdate.ReleaseDate/AutoUpdateStartDate, Snapshot.SnapshotCreationTime), all following the same root cause: RFC3339/date strings emitted or expected where awsjson1.1 requires a JSON number of epoch seconds. buildShards' nodeObject.CreateTime was already correct (float64(time.Now().Unix())), confirming this was an inconsistency rather than a deliberate simplification."}
  pointer_aliasing: {status: ok, note: "CreateParameterGroup/CreateSnapshot/CopySnapshot/ExportSnapshot/CreateMultiRegionCluster returned the live table entry instead of a clone, unlike ~90% of the file's other read paths (see backend.go's \"Deep-copy helpers\" section). Not independently reproducible as a failing -race case today because the current wire converters (toSnapshotObject etc.) don't happen to serialize the affected Tags/Parameters maps, but fixed for consistency with the established convention -- a future field addition to those converters would otherwise silently reintroduce a live data race against concurrent TagResource/Update* calls."}
  persistence: {status: ok, note: "Handler exposes Snapshot(ctx)/Restore(ctx,[]byte) delegating straight to InMemoryBackend (persistence.go); registered correctly, no silent-unregistration risk. backendSnapshot versioning (memorydbSnapshotVersion) and the region-nested-table split are sound."}
  route_matcher: {status: ok, note: "single X-Amz-Target-prefixed POST endpoint (AmazonMemoryDB.), all 44 GetSupportedOperations entries are reachable through dispatch (dispatchCoreOps -> dispatchNewOps -> dispatchSnapshotAndEngineOps/dispatchMultiRegionOps/dispatchParameterAndShardOps); verified every case in each switch/map has a live handler function, none orphaned or shadowed."}
gaps:                     # known divergences NOT fixed this pass
  - "Cluster.Status is always \"available\" immediately on CreateCluster; real AWS transitions creating -> available. Not fixed (broad behavioral change, no bd issue filed yet)."
  - "DescribeClusters hand-rolls its own NextToken cursor loop in handler.go instead of using the paginateItems helper (or pkgs/page) every other list op in this file uses. Functionally equivalent, just inconsistent; not fixed (style/dedup only, no wire-visible bug)."
  - "writeBackendError's HTTP status codes (404/409/400 by category) were left as-is rather than aligned to AWS's uniform 400-for-all-client-faults convention for awsjson1.1 FaultClient errors -- would need re-verification against 59 existing status-code assertions plus live-AWS confirmation before changing; the __type field (which the SDK actually uses to resolve typed errors) is now correct regardless."
deferred:                 # consciously not audited this pass (scope) -- next pass targets
  - "Byte-for-byte wire audit of nested Shard/Node objects beyond the timestamp field (shardObject/nodeObject in handler.go's buildShards) -- spot-checked only, CreateTime already correct."
  - "MultiRegionCluster full lifecycle wire shape beyond the fields already checked (no per-region-cluster nested list verified against real MultiRegionCluster type)."
  - "DescribeReservedNodesOfferings/PurchaseReservedNodesOffering pricing-field completeness (RecurringCharges shape) beyond the StartTime fix."
leaks: {status: clean, note: "no goroutines, timers, or janitor loops in this service; Purge() is called externally on a shared schedule and holds b.mu for its duration like every other operation."}
---

## Notes

**Protocol**: awsjson1.1 (`X-Amz-Target: AmazonMemoryDB.<Op>`), single POST endpoint.
Confirmed against `aws-sdk-go-v2/service/memorydb@v1.33.12`'s `deserializers.go`/`serializers.go`.

**Error `__type` is resource-specific, never generic**: MemoryDB's error model
(`types/errors.go`) defines ~55 fault types and *zero* generic ones. There is no
`ResourceNotFoundException`/`ResourceInUseException`/`InvalidRequestException` anywhere
in the real API. Before this pass, `writeBackendError` collapsed every backend sentinel
error into one of those three fabricated buckets by `awserr` category
(`ErrNotFound`/`ErrAlreadyExists`/`ErrConflict`). A real `aws-sdk-go-v2` client
type-switches on the response's `__type` (header `X-Amzn-ErrorType` or body `__type`/`code`
field, resolved by `resolveProtocolErrorType`) via `errors.As(&types.ClusterNotFoundFault{},
...)`, so this was a systemic, whole-service parity gap, not a per-op oversight. The fix
(`errCodeLookup` in `handler.go`) is table-driven and checked before the old
category-based switch, which is retained only as a fallback for any future sentinel that
isn't (yet) cataloged.

**In-use state faults use `Invalid*StateFault`, not a made-up `*InUseFault`**: verified by
reading each `Delete*` operation's own error case list in `deserializers.go` --
`DeleteACL` only recognizes `ACLNotFoundFault`/`InvalidACLStateFault`/
`InvalidParameterValueException`; `DeleteUser` only recognizes `InvalidUserStateFault`/
`UserNotFoundFault`/`InvalidParameterValueException`. `DeleteSubnetGroup` is the one
exception that *does* have a dedicated `SubnetGroupInUseFault` -- and gopherstack had no
in-use check for subnet groups at all (a real state-correctness gap, not just a wire-label
issue), fixed alongside.

**Timestamps are epoch-seconds JSON numbers, not RFC3339 strings**: awsjson1.1's default
`TStamp` wire format is a JSON number of seconds since epoch (`smithytime.ParseEpochSeconds`
/`FormatEpochSeconds` in the SDK). This bit both directions: response fields serialized as
RFC3339 strings (`Event.Date`, `ReservedNode.StartTime`, `ServiceUpdate.ReleaseDate`/
`AutoUpdateStartDate`, `Snapshot.SnapshotCreationTime`) would fail to deserialize on a real
client ("expected TStamp to be a JSON Number, got string instead"), and the *request* side
(`DescribeEventsInput.StartTime`/`EndTime`, unmarshaled into `*time.Time` which expects a
quoted string) would have rejected every real client's `StartTime`/`EndTime`-filtered
`DescribeEvents` call outright with a `SerializationException`. Use `pkgs/awstime.Epoch`
for response fields; for request fields, unmarshal into `encoding/json.Number` and parse
manually (see `parseEpochRequestTime` in `backend.go`) since `time.Time`'s `UnmarshalJSON`
only accepts RFC3339. `services/dax` and `services/apigateway` already carry the same
pattern (`eventResponse.Date float64` / `unixEpochTime`) -- worth generalizing into
`pkgs/awstime` in a future pass instead of re-deriving per service.

**`Authentication.Type` output enum is `password | no-password | iam`**, never
`no-password-required` (confirmed: that string does not appear anywhere in
`aws-sdk-go-v2/service/memorydb`). `no-password-required` is fine to keep accepting as a
lenient request-side alias (real AWS's request-side `InputAuthenticationType` only defines
`password`/`iam`, with `Type` omitted meaning no password), but must never be the
*stored/returned* value -- `normalizeAuthType` in `backend.go` is now the single place that
performs this normalization, shared by both `CreateUser` and `UpdateUser` (the latter
previously stored the raw request string with no validation at all).

**Not real bugs, ruled out during this pass** (documented so a future auditor doesn't
re-flag them): `DeleteACL`/`DeleteSubnetGroup`/`DeleteUser`/`DeleteParameterGroup`/
`DeleteSnapshot` returning the live (un-cloned) table entry is safe -- the entry is removed
from its table and its ARN index in the same locked critical section, so nothing can
mutate it afterward. `ExportSnapshot`'s "export" being a pure read (no real S3 write) matches
every other service's snapshot-export mock. `nodeObject.CreateTime` in `buildShards` was
already emitting epoch seconds correctly before this pass.
