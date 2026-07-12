---
service: docdb
sdk_module: aws-sdk-go-v2/service/docdb@v1.48.11
last_audit_commit: 7d9bd038
last_audit_date: 2026-07-12
overall: A            # genuine fixes found: 3 error-code families + 1 response-nesting bug + 4 request-field-name bugs
ops:
  # DBCluster family
  CreateDBCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: AvailabilityZones + VpcSecurityGroupIds request field names were wrong (see families.DBCluster)"}
  DescribeDBClusters: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: AvailabilityZones response was over-nested (extra <Name> child)"}
  DeleteDBCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyDBCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  StopDBCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  StartDBCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  FailoverDBCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  RestoreDBClusterFromSnapshot: {wire: ok, errors: ok, state: ok, persist: ok}
  RestoreDBClusterToPointInTime: {wire: ok, errors: ok, state: ok, persist: ok}
  # DBInstance family
  CreateDBInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: error codes were DBInstanceNotFoundFault/DBInstanceAlreadyExistsFault, real wire codes have no Fault suffix"}
  DescribeDBInstances: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDBInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyDBInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  RebootDBInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  # DBSubnetGroup family
  CreateDBSubnetGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: SubnetIds request field name was SubnetIds.member.N, real is SubnetIds.SubnetIdentifier.N -- every subnet ID from a real client was silently dropped"}
  DescribeDBSubnetGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDBSubnetGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyDBSubnetGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same SubnetIds field-name bug as Create"}
  # DBClusterParameterGroup family (AWS reuses the plain RDS DBParameterGroup fault codes here, not DBClusterParameterGroup...Fault)
  CreateDBClusterParameterGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: error codes were DBClusterParameterGroupNotFoundFault/AlreadyExistsFault, real wire codes are DBParameterGroupNotFound/DBParameterGroupAlreadyExists (no Cluster, no Fault)"}
  DescribeDBClusterParameterGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDBClusterParameterGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyDBClusterParameterGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: Parameters request field name was Parameters.member.N.ParameterName, real is Parameters.Parameter.N.ParameterName -- every parameter from a real client was silently ignored (disguised no-op hidden by the wrong field name)"}
  CopyDBClusterParameterGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  ResetDBClusterParameterGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeDBClusterParameters: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeEngineDefaultClusterParameters: {wire: ok, errors: n/a, state: ok, persist: n/a, note: "static built-in parameter defaults, no ApplyMethod field carried (minor, non-breaking)"}
  # DBClusterSnapshot family
  CreateDBClusterSnapshot: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeDBClusterSnapshots: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDBClusterSnapshot: {wire: ok, errors: ok, state: ok, persist: ok}
  CopyDBClusterSnapshot: {wire: ok, errors: ok, state: ok, persist: ok, note: "copy omits a fresh SnapshotCreateTime (minor, non-breaking)"}
  DescribeDBClusterSnapshotAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyDBClusterSnapshotAttribute: {wire: ok, errors: ok, state: ok, persist: ok}
  # EventSubscription family
  CreateEventSubscription: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: error codes were SubscriptionNotFoundFault/SubscriptionAlreadyExistFault, real wire codes are SubscriptionNotFound/SubscriptionAlreadyExist (no Fault)"}
  DescribeEventSubscriptions: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteEventSubscription: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyEventSubscription: {wire: ok, errors: ok, state: ok, persist: ok}
  AddSourceIdentifierToSubscription: {wire: ok, errors: ok, state: ok, persist: ok}
  RemoveSourceIdentifierFromSubscription: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeEventCategories: {wire: ok, errors: n/a, state: ok, persist: n/a}
  DescribeEvents: {wire: ok, errors: n/a, state: deferred, persist: n/a, note: "always returns an empty event list -- no real event log is modeled; same pattern as the already-audited neptune service (see gaps)"}
  # GlobalCluster family (deferred -- see gaps)
  CreateGlobalCluster: {wire: ok, errors: ok, state: partial, persist: ok, note: "SourceDBClusterIdentifier is stored but not validated against an existing cluster, and no GlobalClusterMembers subresource is created (see gaps)"}
  DescribeGlobalClusters: {wire: partial, errors: ok, state: ok, persist: ok, note: "always returns an empty GlobalClusterMembers list (see gaps)"}
  DeleteGlobalCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyGlobalCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  FailoverGlobalCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  SwitchoverGlobalCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  RemoveFromGlobalCluster: {wire: ok, errors: ok, state: partial, persist: ok, note: "no-ops with respect to membership because no member list exists to remove from (see gaps)"}
  # Tags
  ListTagsForResource: {wire: ok, errors: n/a, state: ok, persist: ok}
  AddTagsToResource: {wire: ok, errors: ok, state: ok, persist: ok}
  RemoveTagsFromResource: {wire: ok, errors: n/a, state: ok, persist: ok}
  # Misc/static
  DescribeDBEngineVersions: {wire: ok, errors: n/a, state: ok, persist: n/a}
  DescribeOrderableDBInstanceOptions: {wire: ok, errors: n/a, state: n/a, persist: n/a}
  DescribeCertificates: {wire: ok, errors: n/a, state: n/a, persist: n/a}
  ApplyPendingMaintenanceAction: {wire: ok, errors: ok, state: deferred, persist: n/a, note: "validates params but does not check the resourceARN exists or track any maintenance-action state; same pattern as neptune"}
  DescribePendingMaintenanceActions: {wire: ok, errors: n/a, state: deferred, persist: n/a, note: "always returns an empty list; same pattern as neptune"}
families:
  DBCluster: {status: ok, note: "3 confirmed wire bugs fixed: (1) response AvailabilityZones nested an extra <Name> child the SDK deserializer never reads (awsAwsquery_deserializeDocumentAvailabilityZones reads element text directly), producing empty-string AZs on every real client; (2) request AvailabilityZones.member.N should be AvailabilityZones.AvailabilityZone.N (awsAwsquery_serializeDocumentAvailabilityZones); (3) request VpcSecurityGroupIds.member.N should be VpcSecurityGroupIds.VpcSecurityGroupId.N (awsAwsquery_serializeDocumentVpcSecurityGroupIdList). All three meant every real SDK/Terraform client's AZ and security-group selections were silently dropped or corrupted. Verified against deserializers.go/serializers.go for CreateDBCluster/DescribeDBClusters/ModifyDBCluster/DeleteDBCluster/Stop/Start/Failover/RestoreFromSnapshot/RestoreToPointInTime. Core state machine (status transitions, deletion-protection guard, final-snapshot-on-delete, param/subnet group FK checks) is real, non-stub logic."}
  DBInstance: {status: ok, note: "error-code bug fixed: DBInstanceNotFoundFault/DBInstanceAlreadyExistsFault -> DBInstanceNotFound/DBInstanceAlreadyExists (no Fault suffix on the wire; confirmed against awsAwsquery_deserializeOpErrorDeleteDBInstance et al.). CreateDBInstance/ModifyDBInstance/DeleteDBInstance/RebootDBInstance state mutation and DBClusterMember/writer derivation (GetClusterMembers) are real."}
  DBClusterParameterGroup: {status: ok, note: "biggest finding of this pass: (1) error codes were entirely wrong -- DocDB reuses the plain RDS DBParameterGroup...Fault codes (DBParameterGroupNotFound/DBParameterGroupAlreadyExists/InvalidDBParameterGroupState) for every DBClusterParameterGroup op; our code used DBClusterParameterGroup...Fault, which doesn't exist on the wire at all and always fell back to a generic untyped error for real clients. (2) ModifyDBClusterParameterGroup's Parameters.member.N.ParameterName should be Parameters.Parameter.N.ParameterName (awsAwsquery_serializeDocumentParametersList) -- every parameter update from a real client was silently ignored, a disguised no-op invisible to string-matching tests because the handler never saw the parameters at all. Confirmed against deserializers.go for Create/Modify/Delete/Copy/Reset/DescribeDBClusterParameterGroups/DescribeDBClusterParameters."}
  DBSubnetGroup: {status: ok, note: "2 bugs fixed: (1) SubnetIds.member.N should be SubnetIds.SubnetIdentifier.N (awsAwsquery_serializeDocumentSubnetIdentifierList) -- every subnet ID from a real client was silently dropped, so CreateDBSubnetGroup/ModifyDBSubnetGroup always persisted an empty subnet list even though the handler answered 200 OK. (2) DBSubnetGroupAlreadyExistsFault -> DBSubnetGroupAlreadyExists (no Fault suffix on this one specifically; DBSubnetGroupNotFoundFault does keep the Fault suffix -- confirmed asymmetric per-op in deserializers.go)."}
  DBClusterSnapshot: {status: ok, note: "wire shapes and error codes (DBClusterSnapshotNotFoundFault/AlreadyExistsFault, both with Fault suffix) verified correct against deserializers.go, no changes needed. Create/Delete/Copy/RestoreFromSnapshot all real state, including final-snapshot-on-delete."}
  EventSubscription: {status: ok, note: "error-code bug fixed: SubscriptionNotFoundFault/SubscriptionAlreadyExistFault -> SubscriptionNotFound/SubscriptionAlreadyExist (no Fault suffix, and note the real fault type name itself is singular \"Exist\" not \"Exists\")."}
  GlobalCluster: {status: partial, note: "error codes (GlobalClusterNotFoundFault/AlreadyExistsFault) verified correct, state transitions for Modify/Failover/Switchover/Delete/Describe are real. NOT fixed this pass: types.GlobalCluster.GlobalClusterMembers ([]GlobalClusterMember, tracking each member cluster's ARN/IsWriter/Readers) has no equivalent field on our GlobalCluster struct at all -- CreateGlobalCluster never adds the SourceDBClusterIdentifier as a member, DescribeGlobalClusters always reports an empty member list, and RemoveFromGlobalCluster is a pure no-op with respect to membership (there is nothing to remove from). This is a real feature gap, not a wire-shape bug -- fixing it needs a new struct field + wire type + persistence wiring, which is out of scope for this pass given GlobalCluster's lower traffic priority. See gaps."}
  Tags: {status: ok, note: "AddTagsToResource/RemoveTagsFromResource/ListTagsForResource verified real (region-scoped ARN keying via regionFromARN, upsert-by-key semantics). Wire shape (TagList>Tag, flat Key/Value) matches awsAwsquery_deserializeDocumentTagList exactly."}
gaps:
  - GlobalCluster has no GlobalClusterMembers subresource: CreateGlobalCluster doesn't add the source cluster as a member, DescribeGlobalClusters always reports an empty member list, RemoveFromGlobalCluster is a members-list no-op (bd: file follow-up)
  - DescribeEvents / DescribePendingMaintenanceActions / ApplyPendingMaintenanceAction have no real backing state (always return empty / don't validate the target resource exists) -- this matches the already-audited neptune service's precedent for the same DocDB/Neptune/RDS-family operations, not a new regression
  - xmlDBClusterParameter (DescribeDBClusterParameters / DescribeEngineDefaultClusterParameters) omits the optional ApplyMethod field AWS's Parameter shape carries -- cosmetic, does not break deserialization
  - CopyDBClusterSnapshot doesn't stamp a fresh SnapshotCreateTime on the copy (source snapshot's various fields are copied but not this one) -- cosmetic
deferred:
  - GlobalCluster member-tracking feature work (see gaps)
leaks: {status: clean, note: "no goroutines, no time.After/NewTicker/Tick anywhere in the package; backend is a synchronous in-memory store guarded by a single lockmetrics.RWMutex per the pkgs-catalog rule, Snapshot/Restore correctly delegate through Handler for cli.go's setupPersistence registration"}
---

## Notes

Protocol: query/XML (`Version=2014-10-31`), single POST with `Action=` form param, same
family as RDS and Neptune (all three descend from a shared Smithy model lineage). Response
root element is `<{Action}Response>` with a required `<{Action}Result>` child wrapping the
payload for every op that returns data -- verified every response type in handler.go carries
this (`xml:"...Result>Field"` or a `Result` struct tagged `xml:"...Result"`), so no response
is missing the `*Result` wrapper the SDK's `decoder.GetElement("...Result")` unconditionally
requires (the neptune/rds bug class this audit was specifically asked to check for).

**The one bug class that actually bit this service, twice over, is AWS's inconsistent
wire-code naming across DocDB's own resource families** -- not response-root nesting (only
one instance of that, in AvailabilityZones). Three concrete sub-patterns found this pass,
all confirmed directly against `deserializers.go`'s `awsAwsquery_deserializeOpError*`
switches (never trust the Go SDK type name, e.g. `types.DBInstanceNotFoundFault` -- the
`Fault` suffix is a Go naming convention, not necessarily what's on the wire):

1. Most DocDB-native resources (DBCluster, DBClusterSnapshot, GlobalCluster) keep the
   `Fault` suffix on the wire (`DBClusterNotFoundFault`, `GlobalClusterNotFoundFault`, ...).
2. DBInstance and one DBSubnetGroup case (`DBSubnetGroupAlreadyExists`, but *not*
   `DBSubnetGroupNotFoundFault`, which does keep `Fault` -- asymmetric even within one
   resource) drop the `Fault` suffix.
3. DBClusterParameterGroup operations don't use a `DBClusterParameterGroup...` code at all --
   they reuse the RDS-inherited plain `DBParameterGroupNotFound` /
   `DBParameterGroupAlreadyExists` / `InvalidDBParameterGroupState` codes, and
   EventSubscription similarly uses bare `SubscriptionNotFound` /
   `SubscriptionAlreadyExist` (singular "Exist", no Fault).

A wire code that doesn't match the SDK's switch falls through to `smithy.GenericAPIError`
rather than the typed fault, which is invisible to any test that only checks
`rr.Code == 400` or does a raw string-contains on the response body (as most of this
package's pre-existing tests did) -- it only surfaces when a real `aws-sdk-go-v2` client
does `errors.As(err, &typedFault)`, exactly what `handler_sdk_roundtrip_test.go`'s
`*_IsTyped` tests now exercise.

The second bug class -- wrong **request** member-element names -- is arguably more severe
because it silently drops data rather than misrepresenting an error: `AvailabilityZones`,
`VpcSecurityGroupIds`, `SubnetIds`, and `Parameters` (on `ModifyDBClusterParameterGroup`)
each use a resource-specific XML list member name (`AvailabilityZone`,
`VpcSecurityGroupId`, `SubnetIdentifier`, `Parameter`) rather than the generic `member`
most other DocDB lists use (`EnabledCloudwatchLogsExports`, `TagKeys`,
`CloudwatchLogsExportConfiguration.{Enable,Disable}LogTypes` all correctly use `member`,
confirmed against `awsAwsquery_serializeDocumentLogTypeList`/`serializeDocumentKeyList`).
Getting the member name wrong means `url.Values.Get(key)` never finds the value under any
key our parser tries, so the field silently parses as empty/nil with no error raised
anywhere -- a real Terraform `aws_docdb_cluster` resource specifying
`availability_zones`/`vpc_security_group_ids`, or `aws_docdb_subnet_group` specifying
`subnet_ids`, or `ModifyDBClusterParameterGroup` specifying `parameters`, would have those
values disappear against gopherstack while working fine against real AWS. This class of bug
is only catchable by decoding through the real SDK's *request* serializer (which is what
`handler_sdk_roundtrip_test.go` now does end-to-end via an httptest server + real
`docdbsdk.Client`) -- no amount of string-matching the handler's own output can catch a bug
in what the handler *reads*.

`DescribeEvents`, `DescribePendingMaintenanceActions`, and `ApplyPendingMaintenanceAction`
intentionally have no real backing state, mirroring the already-audited `neptune` service's
identical operations (`services/neptune/handler.go`'s `handleDescribeEvents` is the same
always-empty shape). Not treated as a new bug for this pass; flagged as a gap for
future work if these ops become load-bearing for a use case.
