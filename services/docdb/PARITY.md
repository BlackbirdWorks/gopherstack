---
service: docdb
sdk_module: aws-sdk-go-v2/service/docdb@v1.48.11
last_audit_commit: 04b49136
last_audit_date: 2026-07-23
overall: A            # this pass: 3 real feature gaps closed (GlobalCluster members, real events log, real pending-maintenance queue), 2 disguised no-op bugs fixed (ResetDBClusterParameterGroup, CreateEventSubscription arg-swap), 1 wire-field gap fixed (EventSubscription response), 2 cosmetic gaps closed
ops:
  # DBCluster family
  CreateDBCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed prior pass: AvailabilityZones + VpcSecurityGroupIds request field names were wrong (see families.DBCluster). This pass: now records a real activity-log event on create (see Events family)."}
  DescribeDBClusters: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed prior pass: AvailabilityZones response was over-nested (extra <Name> child)"}
  DeleteDBCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "this pass: now records a real activity-log event on delete"}
  ModifyDBCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  StopDBCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "this pass: now records a real activity-log event"}
  StartDBCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "this pass: now records a real activity-log event"}
  FailoverDBCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "this pass: now records a real activity-log event"}
  RestoreDBClusterFromSnapshot: {wire: ok, errors: ok, state: ok, persist: ok}
  RestoreDBClusterToPointInTime: {wire: ok, errors: ok, state: ok, persist: ok}
  # DBInstance family
  CreateDBInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed prior pass: error codes were DBInstanceNotFoundFault/DBInstanceAlreadyExistsFault, real wire codes have no Fault suffix. This pass: now records a real activity-log event on create."}
  DescribeDBInstances: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDBInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "this pass: now records a real activity-log event on delete"}
  ModifyDBInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  RebootDBInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  # DBSubnetGroup family
  CreateDBSubnetGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed prior pass: SubnetIds request field name was SubnetIds.member.N, real is SubnetIds.SubnetIdentifier.N -- every subnet ID from a real client was silently dropped"}
  DescribeDBSubnetGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDBSubnetGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyDBSubnetGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed prior pass: same SubnetIds field-name bug as Create"}
  # DBClusterParameterGroup family (AWS reuses the plain RDS DBParameterGroup fault codes here, not DBClusterParameterGroup...Fault)
  CreateDBClusterParameterGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed prior pass: error codes were DBClusterParameterGroupNotFoundFault/AlreadyExistsFault, real wire codes are DBParameterGroupNotFound/DBParameterGroupAlreadyExists (no Cluster, no Fault)"}
  DescribeDBClusterParameterGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDBClusterParameterGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyDBClusterParameterGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed prior pass: Parameters request field name was Parameters.member.N.ParameterName, real is Parameters.Parameter.N.ParameterName -- every parameter from a real client was silently ignored (disguised no-op hidden by the wrong field name). Already had a real per-group ParameterValue override store (map[string]string on DBClusterParameterGroup) -- confirmed NOT a disguised no-op unlike the sibling ResetDBClusterParameterGroup bug found this pass."}
  CopyDBClusterParameterGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  ResetDBClusterParameterGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: was a disguised no-op -- validated the group and returned an unchanged clone without ever touching pg.Parameters, so ResetAllParameters=true or a per-parameter Parameters list from a real client silently did nothing. Now parses ResetAllParameters + Parameters.Parameter.N.ParameterName (reusing the same wire member name ModifyDBClusterParameterGroup uses) and genuinely clears the requested override(s) back to the engine default."}
  DescribeDBClusterParameters: {wire: ok, errors: ok, state: ok, persist: ok, note: "this pass: ApplyMethod field added (was entirely absent from the wire response -- cosmetic gap closed, AWS's Parameter shape always carries it)"}
  DescribeEngineDefaultClusterParameters: {wire: ok, errors: n/a, state: ok, persist: n/a, note: "this pass: ApplyMethod field added, same fix as DescribeDBClusterParameters"}
  # DBClusterSnapshot family
  CreateDBClusterSnapshot: {wire: ok, errors: ok, state: ok, persist: ok, note: "this pass: now records a real activity-log event on create"}
  DescribeDBClusterSnapshots: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDBClusterSnapshot: {wire: ok, errors: ok, state: ok, persist: ok, note: "this pass: now records a real activity-log event on delete"}
  CopyDBClusterSnapshot: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: copy previously omitted a fresh SnapshotCreateTime (left zero-valued) -- now stamps the copy's own creation time instead of leaving it blank, matching AWS's genuinely-new-resource semantics"}
  DescribeDBClusterSnapshotAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyDBClusterSnapshotAttribute: {wire: ok, errors: ok, state: ok, persist: ok}
  # EventSubscription family
  CreateEventSubscription: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed prior pass: error codes were SubscriptionNotFoundFault/SubscriptionAlreadyExistFault, real wire codes are SubscriptionNotFound/SubscriptionAlreadyExist (no Fault). FIXED this pass, two bugs: (1) the handler passed sourceIDs/eventCategories to Backend.CreateEventSubscription in the wrong positional order (the backend signature is (eventCategories, sourceIDs)), so a real client's SourceIds silently came back as EventCategoriesList and vice versa -- invisible to every pre-existing test since none checked both lists in one request; (2) Enabled was accepted on the wire but never parsed/stored/echoed -- now defaults to true (AWS's default for a new subscription) when unspecified and is a real, mutable field."}
  DescribeEventSubscriptions: {wire: ok, errors: ok, state: ok, persist: ok, note: "this pass: response now carries EventCategoriesList/EventSubscriptionArn/Enabled/CustomerAwsId/SubscriptionCreationTime, all previously entirely absent from xmlEventSubscription (see families.EventSubscription)"}
  DeleteEventSubscription: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyEventSubscription: {wire: ok, errors: ok, state: ok, persist: ok, note: "this pass: Enabled is now a real, wire-visible mutation (was silently dropped, same gap as Create)"}
  AddSourceIdentifierToSubscription: {wire: ok, errors: ok, state: ok, persist: ok}
  RemoveSourceIdentifierFromSubscription: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeEventCategories: {wire: ok, errors: n/a, state: ok, persist: n/a}
  DescribeEvents: {wire: ok, errors: n/a, state: ok, persist: ok, note: "FIXED this pass: previously always returned an empty event list (no real event log was modeled at all). Added a bounded per-region event log (events_log.go, maxEventsLogPerRegion=500) fed by recordEvent calls from the key cluster/instance/snapshot lifecycle mutators (create/delete/stop/start/failover), with SourceIdentifier/SourceType/StartTime/EndTime/Duration/EventCategories filtering matching DescribeEventsInput's real fields (AWS's default 60-minute lookback window honored when neither StartTime nor Duration is given). Mirrors the already-completed neptune service's identical fix."}
  # GlobalCluster family
  CreateGlobalCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: SourceDBClusterIdentifier is now resolved (as an ARN or a bare identifier looked up in the caller's region) and, when it names a real cluster, added as the initial writer GlobalClusterMember -- previously stored but never turned into a member at all."}
  DescribeGlobalClusters: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: GlobalClusterMembers now reflects real membership instead of always answering an empty list"}
  DeleteGlobalCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyGlobalCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  FailoverGlobalCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: TargetDbClusterIdentifier now genuinely promotes a member to writer (or attaches a resolvable-but-not-yet-tracked real cluster as the new writer, demoting the prior one) via promoteGlobalClusterWriter -- previously a pure status-flip no-op with respect to membership"}
  SwitchoverGlobalCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: same real member-promotion fix as FailoverGlobalCluster (this backend has no failure window distinguishing the two operations' data-loss guarantees, so both share promoteGlobalClusterWriter)"}
  RemoveFromGlobalCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: DbClusterIdentifier (accepted as ARN or bare identifier) now genuinely deletes the matching GlobalClusterMember -- previously a pure no-op since no member list existed to remove from"}
  # Tags
  ListTagsForResource: {wire: ok, errors: n/a, state: ok, persist: ok}
  AddTagsToResource: {wire: ok, errors: ok, state: ok, persist: ok}
  RemoveTagsFromResource: {wire: ok, errors: n/a, state: ok, persist: ok}
  # Misc/static
  DescribeDBEngineVersions: {wire: ok, errors: n/a, state: ok, persist: n/a}
  DescribeOrderableDBInstanceOptions: {wire: ok, errors: n/a, state: n/a, persist: n/a}
  DescribeCertificates: {wire: ok, errors: n/a, state: n/a, persist: n/a}
  ApplyPendingMaintenanceAction: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: previously validated params but never checked whether the action was actually queued, and always answered an empty PendingMaintenanceActionDetails regardless of OptInType. Added a real per-resource-ARN pending-action queue (pending_maintenance.go) with AddPendingMaintenanceActionInternal to seed it (mirroring AWS's own system-side upgrade/patch-availability data this backend has no equivalent of), enforcing immediate/next-maintenance/undo-opt-in semantics for real against CurrentApplyDate/OptInStatus. Applying an action never queued for a resource is a harmless no-op (matches AWS's own opt-in semantics), not an error and not a fabricated entry. Mirrors the already-completed neptune service's identical fix."}
  DescribePendingMaintenanceActions: {wire: ok, errors: n/a, state: ok, persist: ok, note: "FIXED this pass: previously always returned an empty list; now reflects the real queue (see ApplyPendingMaintenanceAction), filtered by ResourceIdentifier when given, never emitting an entry with an empty PendingMaintenanceActionDetails (matches AWS)."}
families:
  DBCluster: {status: ok, note: "3 confirmed wire bugs fixed prior pass (response AvailabilityZones over-nesting; AvailabilityZones/VpcSecurityGroupIds request field names). This pass: added real activity-log event recording (create/delete/stop/start/failover) feeding the now-real DescribeEvents -- core state machine unchanged and still real (status transitions, deletion-protection guard, final-snapshot-on-delete, param/subnet group FK checks)."}
  DBInstance: {status: ok, note: "error-code bug fixed prior pass: DBInstanceNotFoundFault/DBInstanceAlreadyExistsFault -> DBInstanceNotFound/DBInstanceAlreadyExists (no Fault suffix). This pass: added real activity-log event recording (create/delete). CreateDBInstance/ModifyDBInstance/DeleteDBInstance/RebootDBInstance state mutation and DBClusterMember/writer derivation (GetClusterMembers) remain real."}
  DBClusterParameterGroup: {status: ok, note: "prior pass fixed the DBParameterGroup...-not-DBClusterParameterGroup...-Fault error-code family and the ModifyDBClusterParameterGroup Parameters.Parameter.N wire-field-name bug. THIS PASS found and fixed a second disguised no-op in the same family: ResetDBClusterParameterGroup validated the group and returned an unchanged clone without ever touching pg.Parameters -- neither ResetAllParameters=true nor a per-parameter Parameters list from a real client did anything. Now genuinely clears the requested override(s). Also closed the cosmetic ApplyMethod-field gap on DescribeDBClusterParameters/DescribeEngineDefaultClusterParameters."}
  DBSubnetGroup: {status: ok, note: "2 bugs fixed prior pass: SubnetIds.member.N vs SubnetIds.SubnetIdentifier.N field-name bug; DBSubnetGroupAlreadyExistsFault vs DBSubnetGroupAlreadyExists asymmetric Fault-suffix bug. No changes this pass -- re-verified as correct-as-is."}
  DBClusterSnapshot: {status: ok, note: "wire shapes and error codes verified correct prior pass, no changes needed there. THIS PASS: added real activity-log event recording (create/delete) and fixed CopyDBClusterSnapshot's missing fresh SnapshotCreateTime (cosmetic gap, now closed)."}
  EventSubscription: {status: ok, note: "error-code bug fixed prior pass (SubscriptionNotFoundFault/SubscriptionAlreadyExistFault -> no-Fault, singular \"Exist\"). THIS PASS found and fixed two real bugs: (1) a genuine sourceIDs/eventCategories argument-order swap in handleCreateEventSubscription's call into the backend -- both are []string so nothing type-checked it away, and no pre-existing test exercised both lists in the same request to catch it; (2) xmlEventSubscription entirely omitted EventCategoriesList/EventSubscriptionArn/Enabled/CustomerAwsId/SubscriptionCreationTime -- a real client reading back the categories or ARN it just set on Create always saw them silently dropped even though the backend tracked EventCategories correctly internally. Enabled is now a real, request-accepted, backend-stored, wire-echoed field on both Create and Modify (previously accepted on neither end)."}
  GlobalCluster: {status: ok, note: "FIXED this pass, closing the prior pass's flagged gap: types.GlobalCluster.GlobalClusterMembers now has a real backing field (GlobalClusterMember: DBClusterArn/IsWriter/Readers/SynchronizationStatus). CreateGlobalCluster attaches a resolvable SourceDBClusterIdentifier as the initial writer; FailoverGlobalCluster/SwitchoverGlobalCluster genuinely promote TargetDbClusterIdentifier via promoteGlobalClusterWriter (attaching a resolvable-but-not-yet-tracked real cluster as the new writer when it isn't already a member, matching the already-completed neptune service's identical precedent); RemoveFromGlobalCluster genuinely deletes the matching member. A target this backend cannot resolve at all (neither an existing member, an ARN, nor a known local cluster identifier) is left as a no-op rather than erroring, for the same reason neptune's precedent gives: this backend has no separate \"join global cluster\" operation (real DocDB clusters join via CreateDBCluster-time GlobalClusterIdentifier attachment, not modeled here, matching neptune) to have modeled a genuine not-yet-attached secondary, so it cannot distinguish that case from a typo."}
  Tags: {status: ok, note: "AddTagsToResource/RemoveTagsFromResource/ListTagsForResource verified real (region-scoped ARN keying via regionFromARN, upsert-by-key semantics). Wire shape (TagList>Tag, flat Key/Value) matches awsAwsquery_deserializeDocumentTagList exactly. No changes this pass."}
  ClusterEndpoint: {status: n/a, note: "VERIFIED this pass, not a gap: real Amazon DocumentDB has NO cluster-endpoint API at all (no CreateDBClusterEndpoint/ModifyDBClusterEndpoint/DeleteDBClusterEndpoint/DescribeDBClusterEndpoints anywhere in aws-sdk-go-v2/service/docdb@v1.48.11 -- confirmed by listing every api_op_*.go file in the module). This is an RDS/Neptune-only feature this campaign's task description generically mentioned for the RDS-cluster family, but DocDB's own API surface genuinely does not have it. gopherstack correctly has zero cluster-endpoint code for this service; adding any would be inventing an op that doesn't exist on the real wire."}
gaps:
  # (none currently open -- every item flagged in the prior pass was fixed this pass; see items_still_open in the audit receipt for anything this pass could not fully verify)
deferred:
  - GlobalCluster member-promotion for a Failover/Switchover target that is neither an existing member, an ARN, nor a locally-known DB cluster identifier is a silent no-op rather than an error -- real AWS would reject an unresolvable target, but this backend has no "join global cluster" operation to have modeled a genuine not-yet-attached secondary (same documented precedent as the already-completed neptune service), so it cannot distinguish that case from a typo without one.
leaks: {status: clean, note: "no goroutines, no time.After/NewTicker/Tick anywhere in the package (still true after this pass's additions -- the new pending-maintenance-action queue and events log in pending_maintenance.go/events_log.go are plain maps guarded by the existing single lockmetrics.RWMutex, not background workers); backend is a synchronous in-memory store, Snapshot/Restore correctly delegate through Handler for cli.go's setupPersistence registration. eventsLog is bounded per region (maxEventsLogPerRegion=500, oldest entries trimmed) so it cannot grow unbounded in a long-lived process. Both new maps round-trip through backendSnapshot (persistence.go) alongside the pre-existing Tags map -- verified by TestPersistenceRoundTrip_NewState. pendingMaintenanceActions/eventsLog are deliberately NOT cascade-cleared on cluster/instance/snapshot delete: an activity-log event must remain visible after its source resource is gone (that's the point of an activity log, matching AWS's own event-retention behavior), and a queued maintenance action against a since-deleted resource is inert (never returned to anyone querying by the now-nonexistent resource identifier) rather than a live leak -- same precedent as the already-completed neptune service."}
---

## Notes

Protocol: query/XML (`Version=2014-10-31`), single POST with `Action=` form param, same
family as RDS and Neptune (all three descend from a shared Smithy model lineage). Response
root element is `<{Action}Response>` with a required `<{Action}Result>` child wrapping the
payload for every op that returns data -- verified every response type in handler.go carries
this (`xml:"...Result>Field"` or a `Result` struct tagged `xml:"...Result"`), so no response
is missing the `*Result` wrapper the SDK's `decoder.GetElement("...Result")` unconditionally
requires (the neptune/rds bug class the prior audit was specifically asked to check for).

**Prior-pass bug class (fixed, re-verified this pass): AWS's inconsistent wire-code naming
across DocDB's own resource families.** Three sub-patterns, all confirmed directly against
`deserializers.go`'s `awsAwsquery_deserializeOpError*` switches (never trust the Go SDK type
name -- the `Fault` suffix is a Go naming convention, not necessarily what's on the wire):
(1) most DocDB-native resources (DBCluster, DBClusterSnapshot, GlobalCluster) keep the
`Fault` suffix on the wire; (2) DBInstance and one DBSubnetGroup case drop it (asymmetric
even within DBSubnetGroup itself); (3) DBClusterParameterGroup operations reuse the
RDS-inherited plain `DBParameterGroupNotFound`/`DBParameterGroupAlreadyExists` codes, and
EventSubscription similarly uses bare `SubscriptionNotFound`/`SubscriptionAlreadyExist`
(singular "Exist", no Fault). No new instances of this bug class found this pass.

**Prior-pass bug class (fixed, re-verified this pass): wrong request member-element
names.** `AvailabilityZones`, `VpcSecurityGroupIds`, `SubnetIds`, and `Parameters` (on
`ModifyDBClusterParameterGroup`) each use a resource-specific XML list member name rather
than the generic `member` most other DocDB lists use. Getting the member name wrong means
`url.Values.Get(key)` never finds the value under any key the parser tries, so the field
silently parses as empty/nil with no error raised anywhere. `ResetDBClusterParameterGroup`
reuses the same `Parameters.Parameter.N.ParameterName` wire shape (confirmed via
`awsAwsquery_serializeDocumentParametersList`, shared with Modify) -- the new
`parseDBClusterParameterNames` helper added this pass reads it correctly from the start.

**This pass's dominant bug class: disguised no-ops and missing response fields, not wire
member-name mistakes.** Two ops (`ResetDBClusterParameterGroup`,
`CreateEventSubscription`'s sourceIDs/eventCategories argument order) validated their inputs
correctly and returned a plausible-looking 200 OK while silently doing nothing (or the wrong
thing) with real caller-supplied data -- both invisible to `rr.Code == 400`-style or
single-field `Contains` assertions, which is exactly why parity-principles rule #3 requires
SDK-driven round-trip checks rather than trusting green unit tests alone. One family
(`EventSubscription`'s response shape) was missing wire fields entirely
(`EventCategoriesList`/`EventSubscriptionArn`/`Enabled`/`CustomerAwsId`/
`SubscriptionCreationTime`) despite the backend already tracking the underlying data
correctly -- a pure serialization gap, not a state-machine bug.

**Three real feature gaps closed this pass, mirroring the already-completed neptune
service's identical fixes for the same DocDB/Neptune/RDS-family operations:**
`GlobalCluster.GlobalClusterMembers` (real member tracking via `global_clusters.go`'s
`promoteGlobalClusterWriter`), `DescribeEvents` (real bounded per-region event log via
`events_log.go`), and `ApplyPendingMaintenanceAction`/`DescribePendingMaintenanceActions`
(real per-resource-ARN pending-action queue via `pending_maintenance.go`, seeded for tests
via `AddPendingMaintenanceActionInternal` the same way `AddDBClusterInternal` et al. seed
other resources). None of these are goroutines/tickers -- all are plain maps guarded by the
existing coarse `lockmetrics.RWMutex`, matching the pkgs-catalog locking rule.

**Verified NOT a gap:** DocDB has no cluster-endpoint API at all in the real SDK (unlike
RDS/Neptune) -- confirmed by enumerating every `api_op_*.go` file in
`aws-sdk-go-v2/service/docdb@v1.48.11`. gopherstack correctly has zero code for this feature
in the docdb service; this was independently field-diffed this pass, not assumed.
