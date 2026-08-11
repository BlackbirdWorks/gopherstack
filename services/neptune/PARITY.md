---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: neptune
sdk_module: aws-sdk-go-v2/service/neptune@v1.48.4
last_audit_commit: 087cb59186751418d9d49b88434f13cf214c7609
last_audit_date: 2026-08-11
overall: A            # every previously-open gap this pass either genuinely fixed or re-verified as correct-as-is
                      # 2026-07-31 (browser parity pass): RouteMatcher checked only the User-Agent header for the "api/neptune" marker, which a browser cannot set (Fetch spec forbids scripts from setting User-Agent) -- the AWS SDK for JavaScript in a browser puts its SDK identification in X-Amz-User-Agent instead, so every browser dashboard Neptune request (@aws-sdk/client-neptune) fell through unmatched. Also confirmed the marker itself needed case-insensitive matching: the JS SDK's serviceId-derived marker is "api/Neptune" (PascalCase), not aws-sdk-go-v2's lowercase "api/neptune". Fixed via the new pkgs/service.MatchesUserAgentMarker helper, shared with the identical bug class fixed the same pass in mediastoredata/docdb/appsync. Grade held at A: fixed, not deferred.
                      # 2026-08-11 (gopherstack-gt9o NetworkType pass): closed the recorded NetworkType/SupportedNetworkTypes/NetworkTypeNotSupportedFault gap. NetworkType threaded end-to-end for DBCluster (CreateDBCluster/ModifyDBCluster input, IPV4 default, Describe echo) and DBInstance (inherited from parent cluster at create time, no input member of its own -- verified absent from CreateDBInstanceInput/ModifyDBInstanceInput). SupportedNetworkTypes modeled on DBSubnetGroup/OrderableDBInstanceOption but deliberately left empty (no CIDR data to derive it honestly) and NetworkTypeNotSupportedFault deliberately left unwired (no state to detect the real trigger condition) -- see gaps below for the reasoning on both. Snapshot version constant unchanged (1); additive omitempty fields only.
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
families:
  DBCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "ClusterCreateTime was hardcoded to a fixed 2024-01-01 literal for every cluster (fixed: real timestamp per creation, including restore paths which previously omitted it and DBClusterResourceID entirely). FailoverDBCluster was a disguised no-op (fixed: real writer/reader promotion via DBClusterMembers.IsClusterWriter, with TargetDBInstanceIdentifier support and InvalidDBClusterStateFault when no reader exists). PromoteReadReplicaDBCluster re-verified this pass against the SDK: its own doc comment on both the operation and its DBClusterIdentifier field says 'Not supported.' -- gopherstack's describe-only echo (no state mutation) is therefore the CORRECT behavior for a genuinely-unsupported op, not a stub; reclassified from gap to ok. NetworkType FIXED this pass: gained on CreateDBCluster/ModifyDBCluster input (neptune@v1.48.4 api_op_CreateDBCluster.go:171/api_op_ModifyDBCluster.go:136, plain *string wire member 'NetworkType') and echoed on Describe; unspecified-on-create defaults to IPV4 per the SDK's documented default (api_op_CreateDBCluster.go:161), matching real AWS always answering a concrete value. Accepted as any string, not validated against IPV4/DUAL (no smithy enum backs it)."}
  DBInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "InstanceCreateTime field was entirely absent from the model/wire shape (fixed: added and populated on create). RebootDBInstance intentionally stays a state-preserving op (matches AWS's eventual-consistency behavior for reboot; DescribeDBInstances shows 'available' immediately either way). NetworkType FIXED this pass: CreateDBInstanceInput/ModifyDBInstanceInput carry no NetworkType member of their own (verified against the SDK -- absent from both input structs), matching the doc comment on DBInstance.NetworkType ('Inherited from the DB cluster'); now captured from the parent cluster's NetworkType at instance-create time and echoed on Describe."}
  DBClusterParameterGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: ModifyDBClusterParameterGroup/ResetDBClusterParameterGroup were disguised no-ops -- they validated the group and the Parameters.Parameter.N.* the real client sends, then discarded every value, so DescribeDBClusterParameters always answered empty regardless of what was 'set'. Added a real per-group ParameterValue override store (parameter_catalog.go) seeded against a documented Neptune engine-parameter catalog (neptune_query_timeout, neptune_enable_audit_log, neptune_streams, neptune_result_cache, neptune_dfe_query_engine, neptune_ml_iam_role, neptune_lab_mode, neptune_shard_hash_partitions), enforcing the real static-parameter/pending-reboot ApplyMethod rule and the non-modifiable-parameter rule, with ResetAllParameters and per-parameter reset both wired to real state. DescribeEngineDefaultClusterParameters now returns that catalog instead of an always-empty list. Delete cascades the override store (no ghost rows)."}
  DBParameterGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "Same fix as DBClusterParameterGroup, sharing the catalog/override-store logic in parameter_catalog.go (real Neptune parameter names are shared across both instance- and cluster-level groups)."}
  DBSubnetGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "SupportedNetworkTypes modeled (real StringList wire shape) but never populated -- see the gaps entry below for why."}
  ClusterSnapshot: {wire: ok, errors: ok, state: ok, persist: ok, note: "Multiple real bugs fixed this pass: (1) SnapshotCreateTime/ClusterCreateTime fields were entirely absent from the model; (2) CopyDBClusterSnapshot silently dropped Port/AllocatedStorage/KmsKeyID/IAMDatabaseAuthenticationEnabled/PercentProgress instead of copying them from the source; (3) ModifyDBClusterSnapshotAttribute/DescribeDBClusterSnapshotAttributes were a disguised no-op pair (Modify validated params and discarded them; Describe always returned an empty attribute list) AND Modify's response body omitted the required *Result XML element entirely, which makes the real aws-sdk-go-v2 client fail every call with a smithy.DeserializationError even though gopherstack answered HTTP 200 -- both fixed with a real RestoreAttributeValues store on DBClusterSnapshot, correct list-item wire shape (AttributeValues is a repeated <AttributeValue> list, was a single string), and the correct ValuesToAdd.AttributeValue.N / ValuesToRemove.AttributeValue.N wire param names (was ValuesToAdd.member.N, which a real client never sends, so Modify's add/remove would have silently no-opped forever even after the rest of the fix)."}
  EventSubscription: {wire: ok, errors: ok, state: ok, persist: ok, note: "DescribeEvents FIXED this pass (see the top-level Events family below) -- it is dispatched from this family's handler file but is not itself an EventSubscription op, so it is tracked separately."}
  GlobalCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: ModifyGlobalCluster/FailoverGlobalCluster/SwitchoverGlobalCluster were disguised no-ops (validated the global cluster and returned an unchanged clone). ModifyGlobalCluster's interface signature didn't even accept the new values a caller sent -- now applies DeletionProtection/EngineVersion/NewGlobalClusterIdentifier (rename, including ARN) for real. Failover/Switchover now flip GlobalClusterMembers[].IsWriter to promote TargetDbClusterIdentifier -- when the target already is a tracked member it is promoted directly; when it resolves to a real DB cluster in the account but was never attached (this backend has no separate 'join global cluster' op the way real Neptune's CreateDBCluster-time GlobalClusterIdentifier attachment works), it is attached as the new writer, demoting the prior one; a target this backend cannot resolve at all is left as a no-op rather than erroring, since it cannot distinguish a legitimate not-yet-modeled cross-region secondary from a typo. CreateGlobalCluster/DescribeGlobalClusters/DeleteGlobalCluster/RemoveFromGlobalCluster were already real."}
  ClusterEndpoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "DeleteDBClusterEndpoint returned an empty response body; the real DeleteDBClusterEndpointOutput echoes the deleted endpoint's fields as a flat (non-nested) payload and the SDK deserializer hard-fails without a *Result element -- fixed (backend now returns the deleted endpoint; handler renders it under DeleteDBClusterEndpointResult, matching CreateDBClusterEndpointResponse's existing flat-under-Result shape). ModifyDBClusterEndpoint FIXED this pass: it silently ignored StaticMembers.member.N/ExcludedMembers.member.N even though the real API accepts and applies them -- now replaces the respective member list when a non-empty list is supplied (nil vs explicitly-empty is indistinguishable on this wire format, matching CreateDBClusterEndpoint's existing convention for the same two fields)."}
  Tags: {wire: ok, errors: ok, state: ok, persist: ok}
  Events: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: DescribeEvents always returned an empty list -- there was no event log backing this backend at all, so the response was empty regardless of what a caller had actually done (a genuine gap, not a legitimate no-op: AWS's own DescribeEvents surfaces real account activity). Added a bounded per-region event log (events.go, maxEventsLogPerRegion=500) fed by recordEvent calls from the key cluster/instance/snapshot lifecycle mutators (create/delete/start/stop/failover), with SourceIdentifier/SourceType/StartTime/EndTime/Duration/EventCategories filtering matching DescribeEventsInput's real fields (AWS's default 60-minute lookback window is honored when neither StartTime nor Duration is given)."}
  Maintenance: {wire: ok, errors: ok, state: ok, persist: ok, note: "ApplyPendingMaintenanceAction's response body omitted the required ApplyPendingMaintenanceActionResult/ResourcePendingMaintenanceActions element -- same GetElement-hard-fail bug class as ModifyDBClusterSnapshotAttribute and DeleteDBClusterEndpoint above -- fixed (now echoes ResourceIdentifier back). FIXED FURTHER this pass: added a real pending-maintenance-action queue (maintenance.go), keyed by resource ARN -> action name, since real AWS populates this from system-side upgrade/security-patch availability data this backend has no equivalent of; AddPendingMaintenanceActionInternal seeds it for callers/tests the same way AddClusterInternal/AddSnapshotInternal/AddParameterGroupInternal seed their resources. ApplyPendingMaintenanceAction now genuinely mutates CurrentApplyDate/OptInStatus per AWS's immediate/next-maintenance/undo-opt-in semantics (validated as an enum), and DescribePendingMaintenanceActions returns genuinely-queued actions filtered by the db-cluster-id/db-instance-id Filters AWS documents, never emitting an empty ResourcePendingMaintenanceActions entry (matching AWS). Also corrected the DescribePendingMaintenanceActionsResult XML shape while touching these types: it was a flat single-level list wrongly tagging items as <ResourcePendingMaintenanceActions> with bare Action/Description fields; real AWS nests a per-resource <ResourcePendingMaintenanceActions><ResourceIdentifier/><PendingMaintenanceActionDetails><PendingMaintenanceAction>..., now also carrying AutoAppliedAfterDate/CurrentApplyDate/ForcedApplyDate/OptInStatus."}
  StaticCatalog: {status: ok, note: "DescribeDBEngineVersions, DescribeOrderableDBInstanceOptions, DescribeValidDBInstanceModifications -- correctly modeled as static/hardcoded catalog data (not a stub; there is no per-account mutable state for engine version catalogs). DescribeEngineDefault(Cluster)Parameters moved out of this family this pass: they now return the real parameter catalog (see DBParameterGroup/DBClusterParameterGroup above) instead of an always-empty list, which was a genuine gap masquerading as static-catalog behavior -- an empty catalog is not the same thing as a hardcoded non-empty one."}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "SupportedNetworkTypes on DBSubnetGroup/OrderableDBInstanceOption is modeled (field exists, real StringList wire shape via xmlSupportedNetworkTypeList) but permanently left empty (nil pointer, omitted from the wire): this backend tracks subnets as opaque ID strings only (no IPv4/IPv6 CIDR data) and the orderable-options catalog is static/hardcoded with no per-instance-class capability source, so there is no honest basis to compute AWS's real derived value -- inventing IPV4/DUAL support a client could filter on would be worse than omitting the field. Not fixable without modeling real subnet CIDR data."
  - "NetworkTypeNotSupportedFault (neptune@v1.48.4 types/errors.go:1417, wire code \"NetworkTypeNotSupported\") is intentionally NOT wired into errors.go's lookup table. Real AWS raises it when a requested NetworkType is incompatible with the target DB subnet group's actual IPv4/IPv6 CIDR support -- this backend has no CIDR data (see SupportedNetworkTypes gap above) to genuinely detect that condition, and inventing a rejection rule would be the more-restrictive-than-AWS bug class this repo explicitly avoids. NetworkType itself is accepted as any string (client-side SDK type is a bare *string, not a smithy enum -- verified: no NetworkType entry in aws-sdk-go-v2/service/neptune/types/enums.go), never validated against IPV4/DUAL."
  - "RestoreDBClusterFromSnapshot/RestoreDBClusterToPointInTime do not accept or echo NetworkType, consistent with their existing minimal option surface (already missing StorageType/HostedZoneID/MasterUsername/etc., a pre-existing gap out of scope for this pass). CreateDBCluster/ModifyDBCluster do carry NetworkType (the SDK input member exists only on these 4 ops; only the 2 implemented ones were wired)."
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - The Neptune engine-parameter catalog in parameter_catalog.go (8 parameters) is a documented, representative approximation, not a byte-for-byte mirror of AWS's real DescribeEngineDefaultParameters catalog (which is server-side data, not part of the SDK, and was not independently verified against a live Neptune account this pass) -- functionally correct (real persistence, real validation, real Describe reflection) but the exact parameter set/count may not match AWS's live catalog.
  - GlobalCluster Failover/Switchover member-promotion for a target that is neither an existing member, an ARN, nor a locally-known DB cluster identifier is a silent no-op rather than an error -- real AWS would reject an unresolvable target, but this backend has no "join global cluster" operation to have modeled a genuine not-yet-attached secondary, so it cannot distinguish that case from a typo without one.
leaks: {status: clean, note: "No goroutines, timers, or janitors in this service (still true after this pass's additions -- the new pending-maintenance-action queue and events log are plain maps/slices guarded by the existing coarse b.mu, not background workers). Handler exposes Snapshot(ctx)/Restore(ctx, []byte) with the exact required signatures, delegating straight to InMemoryBackend's own Snapshot/Restore (persistence.go); the compile-time `var _ StorageBackend = (*InMemoryBackend)(nil)` assertion in interfaces.go keeps this from silently drifting. All 9 region-qualified resource collections plus the global (partition-scoped) GlobalCluster table round-trip through backendSnapshot; clusterRoles/tags/parameterOverrides/clusterParameterOverrides/pendingMaintenanceActions/eventsLog (raw maps, not store.Table) persist directly. DeleteDBParameterGroup/DeleteDBClusterParameterGroup now cascade-delete their parameter override entry (parameterOverrides/clusterParameterOverrides) the same way DeleteDBCluster already cascaded instances/endpoints/tags -- no ghost override rows survive a deleted group."}
---

## Notes

Protocol: **query/xml**, single POST with `Action=<OpName>&Version=2014-10-31` form
params (`RouteMatcher` checks `Content-Type: application/x-www-form-urlencoded`,
`api/neptune` present in either `User-Agent` or `X-Amz-User-Agent` -- see
`pkgs/service.MatchesUserAgentMarker` and the 2026-07-31 browser-parity note above --
and `Version=2014-10-31` in the parsed
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

## 2026-07-23 pass: closing the 6 gaps / 2 deferred items from 2026-07-12

All six gaps and both deferred items from the prior audit were re-examined
against the SDK and either genuinely fixed (real state mutation + correct
wire shape) or reclassified to `ok` with evidence -- none were reclassified
on a no-stub basis alone.

**PromoteReadReplicaDBCluster is not a stub -- the SDK says the op itself is
"Not supported."** `api_op_PromoteReadReplicaDBCluster.go`'s doc comment on
both `(*Client).PromoteReadReplicaDBCluster` and
`PromoteReadReplicaDBClusterInput.DBClusterIdentifier` reads exactly "Not
supported." -- this is RDS's shared Aurora API surface carried into Neptune's
model without a Neptune-specific implementation behind it. gopherstack's
existing describe-only echo (no state mutation) is therefore the *correct*
emulation of a genuinely-inert real operation, not a disguised no-op; this
was previously flagged as an open gap because the correct behavior was
"genuinely unclear without more real-AWS testing" -- reading the SDK's own
doc comment settles it without needing a live account.

**The parameter-value-store gap (DBParameterGroup/DBClusterParameterGroup)
required a real catalog, not just a map.** Modify/Reset validated the group
and parsed `Parameters.Parameter.N.{ParameterName,ParameterValue,ApplyMethod}`
(confirmed against `awsAwsquery_serializeDocumentParametersList` in the SDK's
serializers.go -- the list member wrapper is `Parameter`, not the generic
query-protocol `member`) but then discarded every value, so
DescribeDBParameters/DescribeDBClusterParameters and
DescribeEngineDefaultParameters/DescribeEngineDefaultClusterParameters always
answered with an empty list. Fixed with a per-group `ParameterValue` override
store (`region|groupName` -> parameter name -> value, following the same
plain-nested-map convention as `clusterRoles`/`tags`) merged against a new
canonical parameter catalog (`parameter_catalog.go`) on every Describe. The
catalog also let two more real AWS rules become enforceable for the first
time: static parameters (`neptune_enable_audit_log`, `neptune_streams`,
`neptune_shard_hash_partitions`) require `ApplyMethod=pending-reboot`
(`ApplyMethod=immediate` is rejected, matching real AWS's
static/dynamic-ApplyMethod compatibility rule), and
`neptune_shard_hash_partitions` -- a system-controlled parameter -- rejects
modification outright (`IsModifiable=false`). AWS's exact default parameter
catalog is server-side data, not part of the SDK, so the 8-parameter set
modeled here is a documented representative approximation (see `deferred`
above), not a verified byte-for-byte mirror.

**GlobalCluster Modify/Failover/Switchover needed real member-list mutation,
not just an accepted request.** `ModifyGlobalCluster`'s *interface signature*
previously took only a `globalClusterID` -- it could not have applied a
caller's `DeletionProtection`/`EngineVersion` even if it wanted to, since
those values were never passed in. Fixed by adding `GlobalClusterModifyOptions`
(also covering `NewGlobalClusterIdentifier`, i.e. rename, which real
AWS's `ModifyGlobalClusterInput` also accepts) and real
`GlobalClusterMembers[].IsWriter` promotion in Failover/Switchover.
Real AWS requires `TargetDbClusterIdentifier` to already be an attached
secondary; this backend has no equivalent of Neptune's actual attachment
mechanism (a DB cluster joins a global database via `CreateDBCluster`'s
`GlobalClusterIdentifier` parameter, which this backend's `CreateDBCluster`
does not model), so a target that resolves to a real local DB cluster but
isn't yet a tracked member is attached as the new writer as the closest
achievable analogue, and a target this backend cannot resolve at all is a
no-op rather than a rejection (see `deferred`).

**ModifyDBClusterEndpoint's StaticMembers/ExcludedMembers gap was a pure
parsing omission.** The real API's `StaticMembers`/`ExcludedMembers` list
params serialize with the generic query-protocol `member` wrapper
(`awsAwsquery_serializeDocumentStringList`, confirmed against
serializers.go), same as most other Neptune list params elsewhere in this
codebase already handle via `parseMemberList` -- the handler simply never
called it for these two fields. Fixed.

**The pending-maintenance-action queue and the event log are the two
"structurally missing feature" gaps** (as opposed to "wrong wire shape" or
"discards input") -- there was no backing store for either at all, so
`DescribePendingMaintenanceActions`/`DescribeEvents` were always empty
regardless of real activity. Both AWS features are populated by data this
in-memory emulator doesn't generate on its own (system-side
upgrade/patch-availability data for maintenance actions; real account event
history for events), so both now have `AddPendingMaintenanceActionInternal`-
style seeding (maintenance.go) or are fed by `recordEvent` calls placed at
the point of real state changes this backend already performs (events.go:
cluster/instance create+delete, cluster start+stop+failover, snapshot
create) -- genuine, queryable, filterable state rather than a disguised
no-op in either direction.
