---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: neptune
sdk_module: aws-sdk-go-v2/service/neptune@v1.44.1
last_audit_commit: 087cb59186751418d9d49b88434f13cf214c7609
last_audit_date: 2026-07-12
overall: A            # genuine fixes found across error codes, wire shapes, and state mutation
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
families:
  DBCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "ClusterCreateTime was hardcoded to a fixed 2024-01-01 literal for every cluster (fixed: real timestamp per creation, including restore paths which previously omitted it and DBClusterResourceID entirely). FailoverDBCluster was a disguised no-op (fixed: real writer/reader promotion via DBClusterMembers.IsClusterWriter, with TargetDBInstanceIdentifier support and InvalidDBClusterStateFault when no reader exists). PromoteReadReplicaDBCluster remains describe-only -- see gaps."}
  DBInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "InstanceCreateTime field was entirely absent from the model/wire shape (fixed: added and populated on create). RebootDBInstance intentionally stays a state-preserving op (matches AWS's eventual-consistency behavior for reboot; DescribeDBInstances shows 'available' immediately either way)."}
  DBClusterParameterGroup: {wire: ok, errors: ok, state: partial, persist: ok, note: "Error codes were wrong for the whole family (see below). Create/Delete/Describe/Copy are real; Modify/Reset are legitimate no-ops in the sense that this backend has no per-parameter value store at all -- DescribeDBClusterParameters always returns an empty list, consistently. Not touched this pass; see gaps."}
  DBParameterGroup: {wire: ok, errors: ok, state: partial, persist: ok, note: "Same parameter-value-store gap as DBClusterParameterGroup."}
  DBSubnetGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  ClusterSnapshot: {wire: ok, errors: ok, state: ok, persist: ok, note: "Multiple real bugs fixed this pass: (1) SnapshotCreateTime/ClusterCreateTime fields were entirely absent from the model; (2) CopyDBClusterSnapshot silently dropped Port/AllocatedStorage/KmsKeyID/IAMDatabaseAuthenticationEnabled/PercentProgress instead of copying them from the source; (3) ModifyDBClusterSnapshotAttribute/DescribeDBClusterSnapshotAttributes were a disguised no-op pair (Modify validated params and discarded them; Describe always returned an empty attribute list) AND Modify's response body omitted the required *Result XML element entirely, which makes the real aws-sdk-go-v2 client fail every call with a smithy.DeserializationError even though gopherstack answered HTTP 200 -- both fixed with a real RestoreAttributeValues store on DBClusterSnapshot, correct list-item wire shape (AttributeValues is a repeated <AttributeValue> list, was a single string), and the correct ValuesToAdd.AttributeValue.N / ValuesToRemove.AttributeValue.N wire param names (was ValuesToAdd.member.N, which a real client never sends, so Modify's add/remove would have silently no-opped forever even after the rest of the fix)."}
  EventSubscription: {wire: ok, errors: ok, state: ok, persist: ok, note: "DescribeEvents always returns an empty list -- no event log exists in this backend (consistent no-op, not a disguised one; see deferred)."}
  GlobalCluster: {wire: ok, errors: ok, state: partial, persist: ok, note: "CreateGlobalCluster/DescribeGlobalClusters/DeleteGlobalCluster/RemoveFromGlobalCluster mutate real state. ModifyGlobalCluster/FailoverGlobalCluster/SwitchoverGlobalCluster are disguised no-ops (return an unchanged clone) -- not fixed this pass, see gaps."}
  ClusterEndpoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "DeleteDBClusterEndpoint returned an empty response body; the real DeleteDBClusterEndpointOutput echoes the deleted endpoint's fields as a flat (non-nested) payload and the SDK deserializer hard-fails without a *Result element -- fixed (backend now returns the deleted endpoint; handler renders it under DeleteDBClusterEndpointResult, matching CreateDBClusterEndpointResponse's existing flat-under-Result shape). ModifyDBClusterEndpoint only supports EndpointType, not StaticMembers/ExcludedMembers member-list updates -- see gaps."}
  Tags: {wire: ok, errors: ok, state: ok, persist: ok}
  Maintenance: {wire: ok, errors: ok, state: deferred, persist: n/a, note: "ApplyPendingMaintenanceAction's response body omitted the required ApplyPendingMaintenanceActionResult/ResourcePendingMaintenanceActions element -- same GetElement-hard-fail bug class as ModifyDBClusterSnapshotAttribute and DeleteDBClusterEndpoint above -- fixed (now echoes ResourceIdentifier back). There is no real pending-maintenance-action queue in this backend (DescribePendingMaintenanceActions always returns empty), so ApplyPendingMaintenanceAction remains semantically a no-op beyond the wire fix -- see gaps. Also corrected the DescribePendingMaintenanceActionsResult XML shape while touching these types: it was a flat single-level list wrongly tagging items as <ResourcePendingMaintenanceActions> with bare Action/Description fields; real AWS nests a per-resource <ResourcePendingMaintenanceActions><ResourceIdentifier/><PendingMaintenanceActionDetails><PendingMaintenanceAction>... This was unreachable (the list is always empty) so it never manifested as a live bug, but the type is now correct for when/if a real queue is added."}
  StaticCatalog: {status: ok, note: "DescribeDBEngineVersions, DescribeOrderableDBInstanceOptions, DescribeValidDBInstanceModifications, DescribeEngineDefault(Cluster)Parameters -- all correctly modeled as static/hardcoded catalog data, matching how AWS itself treats these (not a stub; there is no per-account mutable state for engine version catalogs)."}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - DBCluster/DBParameterGroup families have no real per-parameter value store: ModifyDBParameterGroup, ModifyDBClusterParameterGroup, ResetDBParameterGroup, ResetDBClusterParameterGroup validate the group exists but never persist parameter key/value changes, and DescribeDBParameters/DescribeDBClusterParameters always return an empty list regardless of what was "set". Real fix needs a parameters map keyed by group name plus family-specific default parameter catalogs.
  - GlobalCluster ModifyGlobalCluster/FailoverGlobalCluster/SwitchoverGlobalCluster are disguised no-ops -- they validate the global cluster exists and return an unchanged clone, but never mutate GlobalClusterMembers/IsWriter or Status the way a real failover/switchover would.
  - PromoteReadReplicaDBCluster is describe-only; it never mutates cluster state (no read-replica/standalone promotion modeled).
  - ModifyDBClusterEndpoint only mutates EndpointType; it silently ignores StaticMembers.member.N / ExcludedMembers.member.N even though the real API accepts them.
  - ApplyPendingMaintenanceAction/DescribePendingMaintenanceActions have no backing pending-action queue (nothing is ever "pending" to apply); the response is now wire-correct but the underlying feature is unimplemented.
  - DescribeEvents always returns an empty list; there is no event log backing this backend.
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - DBClusterParameterGroup / DBParameterGroup real parameter-value tracking (see gaps)
  - GlobalCluster failover/switchover state semantics (see gaps)
leaks: {status: clean, note: "No goroutines, timers, or janitors in this service. Handler exposes Snapshot(ctx)/Restore(ctx, []byte) with the exact required signatures, delegating straight to InMemoryBackend's own Snapshot/Restore (persistence.go); the compile-time `var _ StorageBackend = (*InMemoryBackend)(nil)` assertion in interfaces.go keeps this from silently drifting. All 9 region-qualified resource collections plus the global (partition-scoped) GlobalCluster table round-trip through backendSnapshot; clusterRoles and tags (raw nested maps, not store.Table) persist directly."}
---

## Notes

Protocol: **query/xml**, single POST with `Action=<OpName>&Version=2014-10-31` form
params (`RouteMatcher` checks `Content-Type: application/x-www-form-urlencoded`,
`User-Agent` containing `api/neptune`, and `Version=2014-10-31` in the parsed
body -- it does NOT check `Action` itself, so any Neptune-shaped POST is claimed
before dispatch validates the action name). Response root element name is
`<OpName>Response` with an `xmlns="http://rds.amazonaws.com/doc/2014-10-31/"`
attribute (Neptune's API reuses RDS's 2014-10-31 XML namespace verbatim -- this
is not a copy-paste bug, it's how the real service actually answers).

**List wire shape**: Neptune (like RDS) uses *named* list-item elements, not the
generic query-protocol `<member>` wrapper -- e.g. `<DBClusters><DBCluster>...`,
not `<DBClusters><member>...`. Verified against the SDK's generated
`deserializeDocument*List` functions op-by-op for DBCluster, DBInstance,
DBClusterSnapshot, DBSubnetGroup, DBClusterParameterGroup, DBClusterMember,
EventSubscription, GlobalCluster. All matched already; this is NOT the "10 ops
nested one level too deep" bug class RDS hit -- Neptune's response-root nesting
(`<XResponse><XResult><Field>...`) was already correct everywhere audited.

**The real, recurring bug class this pass**: several "void" response types
looked like legitimate no-payload query-protocol responses (an empty
`<OpResponse xmlns="..."/>`) but the real AWS operation is NOT void -- it wraps
a `<OpResult>` element the SDK's deserializer unconditionally fetches via
`decoder.GetElement("OpResult")`, and a *missing* element there is a hard
`smithy.DeserializationError` on the client side, not a zero-valued struct.
This is easy to miss by eyeballing gopherstack's own code (an empty struct
"looks" like a correct void response) and easy to miss by grepping ("stub
hunting" flags real logic that ends in an empty return as correct per
parity-principles rule #4) -- the only way to catch it is checking the SDK's
per-op Output struct AND its `HandleDeserialize` to see whether `GetElement`
is called unconditionally. Confirmed broken (now fixed) for
`ModifyDBClusterSnapshotAttribute`, `ApplyPendingMaintenanceAction`, and
`DeleteDBClusterEndpoint`. Confirmed genuinely void (left alone, verified no
`GetElement` call in the op's `HandleDeserialize`) for: `DeleteDBSubnetGroup`,
`DeleteDBClusterParameterGroup`, `DeleteDBParameterGroup`,
`AddTagsToResource`, `RemoveTagsFromResource`, `AddRoleToDBCluster`,
`RemoveRoleFromDBCluster`. A `Test_SDKRoundTrip_*` suite
(`handler_sdk_roundtrip_test.go`) now drives the real `aws-sdk-go-v2/service/neptune`
client against an `httptest.Server`-wrapped handler for the three fixed ops,
specifically to catch this class of bug in future regressions -- string-matching
the raw XML body (as most other tests in this package do) cannot catch it,
since the offending response *looks* fine as a string.

**Error code strings are NOT `TypeName` minus a mechanical suffix.** Neptune's
generated API model is inconsistent about keeping "Fault" on the wire `<Code>`
value: `DBClusterNotFoundFault` keeps it (`"DBClusterNotFoundFault"`), but
`DBInstanceNotFoundFault` drops it (`"DBInstanceNotFound"`). Verified every
code string against each fault type's `ErrorCode()` method in
`aws-sdk-go-v2/service/neptune/types/errors.go` AND cross-checked against the
actual per-operation `EqualFold` switch in `deserializers.go` (the type exists
doesn't mean every op error-switches on it). Fixed 9 wrong entries in
`neptuneErrorCode()`: `DBInstanceNotFound(Fault)`,
`DBInstanceAlreadyExists(Fault)`, `DBSubnetGroupAlreadyExists(Fault)`,
`SubscriptionNotFound(Fault)`, `SubscriptionAlreadyExist(Fault)`,
`InvalidDBInstanceState(Fault)`. The **cluster-parameter-group family is the
most surprising case**: there is no distinct
`DBClusterParameterGroupAlreadyExists`/`...NotFound` fault for
`CreateDBClusterParameterGroup`/`DeleteDBClusterParameterGroup`/
`ModifyDBClusterParameterGroup`/`ResetDBClusterParameterGroup` -- they all
reuse the plain (non-cluster) `DBParameterGroupAlreadyExists`/
`DBParameterGroupNotFound` codes. A `DBClusterParameterGroupNotFound` fault
does exist in the model, but only for *other* ops that reference a cluster's
parameter group by name (`CreateDBCluster`/`ModifyDBCluster`/
`RestoreDBCluster*`) -- this backend doesn't validate that reference, so it
never needs that code. Getting this wrong doesn't break the HTTP
status/message a client sees, but it silently breaks `errors.As(err,
&types.DBInstanceNotFoundFault{})`-style typed error matching in any caller
that relies on it, which is invisible to string-matching-based tests (exactly
why every test in this package that asserts on an error code was re-verified
against the corrected strings, not just left passing against the old wrong
ones).

**Timestamps** are emitted as ISO8601/RFC3339 strings (`ClusterCreateTime`,
`SnapshotCreateTime`, `InstanceCreateTime`), matching `smithytime.ParseDateTime`
on the client side -- Neptune's query/xml protocol does NOT use epoch-seconds
numbers (that's a json/rest-json protocol convention; `pkgs/awstime.Epoch` does
not apply here). `ClusterCreateTime` was previously a hardcoded
`"2024-01-01T00:00:00Z"` literal shared by every cluster ever created -- fixed
to `time.Now().UTC().Format(time.RFC3339)` at actual creation time (also
applied to the two restore paths, which previously omitted the field --
and `DBClusterResourceID` -- entirely).

**Looks-wrong-but-correct**: `RebootDBInstance` and `PromoteReadReplicaDBCluster`
returning state-preserving responses without registering a transient
"rebooting"/"available" status flip is intentional-ish, not a stub in the
disguised-no-op sense -- AWS's own reboot is asynchronous and a `Describe`
issued moments later typically already shows `available` again, so a
synchronous emulator collapsing that window to a no-op is a reasonable
approximation. `PromoteReadReplicaDBCluster` is listed as a gap above because
Neptune doesn't really have RDS-style standalone read-replica instances to
promote (its cross-region replication is entirely global-cluster-based), so
what this op should even mutate in this backend is genuinely unclear without
more real-AWS testing -- flagged for the next pass rather than guessed at.
