---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: redshift
sdk_module: aws-sdk-go-v2/service/redshift@v1.65.4
last_audit_commit: 0fe7aaf4d
last_audit_date: 2026-08-08
overall: A            # RESTORED FROM A- (2026-07-25 follow-up pass, bd gopherstack-0eyk): the
                       # Create/ModifyRedshiftIdcApplicationResult missing-inner-<RedshiftIdcApplication>
                       # -wrapper bug that caused the prior A- downgrade is now fixed (see
                       # families.IdcApplication and gaps history below). Verified against
                       # awsAwsquery_deserializeOpDocumentCreate/ModifyRedshiftIdcApplicationOutput in
                       # aws-sdk-go-v2/service/redshift@v1.65.0/deserializers.go before fixing. Tests
                       # strengthened to assert the literal nested envelope
                       # (<CreateRedshiftIdcApplicationResult><RedshiftIdcApplication>, same for Modify,
                       # plus Describe's <member> wrapping) instead of loose substring Contains checks,
                       # so this class of bug can't silently regress again. Nothing else found holding
                       # the grade down this pass.
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  RestoreFromClusterSnapshot: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed prior pass: Cluster.Tags nil-panic + stuck-in-restoring lifecycle bug"}
  ModifyCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed prior pass: Encrypted/EnhancedVpcRouting tri-state (*bool). PendingModifiedValues never serialized -- confirmed inert, see Notes, not re-flagged as a gap"}
  GetClusterCredentials: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed prior pass: Expiration now serialized"}
  GetClusterCredentialsWithIAM: {wire: ok, errors: ok, state: ok, persist: n/a}
  ResizeCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED THIS PASS: now populates activeResizes (SUCCEEDED, AllowCancelResize=false) so DescribeResize/CancelResize observe a resize triggered via the real API op, not just AddActiveResizeInternal test seeding -- see gaps history"}
families:
  Cluster: {status: ok, note: "CreateCluster/DeleteCluster/DescribeClusters/RebootCluster/PauseCluster/ResumeCluster/RotateEncryptionKey/ModifyClusterIamRoles/ModifyClusterMaintenance verified. FIXED THIS PASS: xmlCluster never embedded Tags inline (real Cluster.Tags []Tag) -- every cluster response silently omitted tags a real client would expect on the object itself, not just via DescribeTags. Also added SnapshotScheduleIdentifier/SnapshotScheduleState (see SnapshotSchedule below)."}
  Tags: {status: ok, note: "CreateTags/DeleteTags/DescribeTags verified. See Cluster row for the inline-Tags wire gap fixed this pass."}
  ClusterParameterGroup: {status: ok, note: "no changes needed"}
  ClusterSubnetGroup: {status: ok, note: "FIXED 2026-08-08 (bd gopherstack-emho): CreateClusterSubnetGroup previously accepted a fabricated 'VpcId' request param not present in the real CreateClusterSubnetGroupInput (confirmed against awsAwsquery_serializeOpDocumentCreateClusterSubnetGroupInput in aws-sdk-go-v2/service/redshift@v1.65.4/serializers.go -- real fields are only ClusterSubnetGroupName/Description/SubnetIds/Tags). Handler no longer reads it. The response's VpcId field IS real on ClusterSubnetGroup (types.ClusterSubnetGroup.VpcId), normally derived by AWS from the subnets' own VPC, but this backend has no EC2 cross-reference to derive it from (Provider.Init does not wire an EC2 backend into Redshift, and Subnet only tracks SubnetIdentifier/SubnetStatus, no VPC linkage) -- left honestly empty rather than fabricated, matching the EndpointAccess precedent below. AddSubnetGroupInternal (test-seeding only, not wire-reachable) can still set it directly."}
  ClusterSecurityGroup: {status: ok, note: "no changes needed"}
  Snapshot/ClusterSnapshot: {status: ok, note: "no changes needed this pass"}
  ClusterCredentials: {status: ok}
  Resize: {status: ok, note: "FIXED THIS PASS, see ResizeCluster op row"}
  DataShare: {status: ok, note: "Associate/Authorize/Deauthorize/Reject/Disassociate/DescribeDataShares* field-diffed against types.DataShare. FIXED: DataShareType was completely absent from the model/wire (real Cluster... err DataShare.DataShareType, defaults to INTERNAL, the only enum value); now serialized. All mutation ops confirmed to mutate the store.Table-returned pointer in place (not stubs)."}
  EventSubscription/Events: {status: ok, note: "field-diffed against types.EventSubscription/Event. FIXED: EventSubscription.SubscriptionCreationTime was computed (SubscriptionCreated) but never serialized into any response; now emitted as RFC3339. DescribeEventCategories/DescribeEvents verified against SDK shapes, no other gaps found."}
  ScheduledAction: {status: ok, note: "FIXED THIS PASS (major): TargetAction was parsed as a single flat top-level string param and never serialized in ANY response -- real CreateScheduledActionInput.TargetAction is a nested ScheduledActionType{PauseCluster|ResumeCluster|ResizeCluster} struct sent as TargetAction.ResizeCluster.ClusterIdentifier=... etc (query-protocol nested member convention), and the object is meaningless without it. Rebuilt as a real tagged-union type (ScheduledActionTarget) with correct nested request parsing (parseTargetAction) and response serialization (targetActionToXML), verified symmetric against both serializers.go and deserializers.go. Also fixed: Enable request param was completely ignored (State was hardcoded ACTIVE forever); now a real tri-state *bool driving ACTIVE/DISABLED. FIXED 2026-08-08 (bd gopherstack-emho): NextInvocations was previously unmodeled; this backend's Schedule field already carries a real at()/cron() expression, so a real evaluator (schedule.go) now computes it instead of leaving it fabricated or perpetually empty -- unparseable/unsupported expressions (e.g. rate(), which real Redshift does not accept here) still yield an honest empty list. StartTime/EndTime remain unmodeled -- see items_still_open."}
  UsageLimit: {status: ok, note: "Create/Delete/Describe/Modify field-diffed, real state mutation confirmed. FIXED 2026-08-08 (bd gopherstack-emho): Tags were accepted and stored on create but never echoed on the wire -- xmlUsageLimit now includes Tags>Tag via the existing tagMapToKVList/parseRedshiftTags shared helpers (same convention as Integration/Qev2IdcApplication), verified against awsAwsquery_deserializeDocumentUsageLimit's Tags case in deserializers.go."}
  SnapshotCopyGrant: {status: ok, note: "Create/Delete/Describe field-diffed, real state mutation confirmed. FIXED 2026-08-08 (bd gopherstack-emho): Tags now echoed on the wire (Tags>Tag), same fix pattern and SDK verification as UsageLimit above (awsAwsquery_deserializeDocumentSnapshotCopyGrant)."}
  SnapshotSchedule: {status: ok, note: "FIXED THIS PASS (real no-op found): ModifyClusterSnapshotSchedule validated ClusterIdentifier/ScheduleIdentifier existence but never recorded the association anywhere -- a textbook no-stub violation (looked like it worked, did nothing). Now sets/clears Cluster.SnapshotScheduleIdentifier/SnapshotScheduleState (real Cluster wire fields, confirmed against types.Cluster), and SnapshotSchedule.AssociatedClusters/AssociatedClusterCount are derived live by scanning clusters for a match and serialized correctly (AssociatedClusters>member>ClusterIdentifier/ScheduleAssociationState). Round-trip verified with a dedicated test."}
  SnapshotCopy: {status: ok, note: "Enable/Disable/ModifySnapshotCopyRetentionPeriod field-diffed, real state mutation confirmed, no changes needed"}
  AuthenticationProfile: {status: ok, note: "field-diffed against types.AuthenticationProfile (no Tags field on this type in the real SDK, confirmed), no changes needed"}
  ResourcePolicy: {status: ok, note: "FIXED THIS PASS: error code ErrResourcePolicyNotFound was a fabricated 'ResourcePolicyNotFound' string -- real GetResourcePolicy/PutResourcePolicy/DeleteResourcePolicy return ResourceNotFoundFault for a missing policy (confirmed against the op error-dispatch table in deserializers.go), now fixed."}
  HsmClientCertificate/HsmConfiguration: {status: ok, note: "Create/Delete/Describe field-diffed, real state mutation confirmed. FIXED 2026-08-08 (bd gopherstack-emho): Create handlers previously passed nil for tags unconditionally (never parsing Tags.Tag.N.* from the request) and the wire never echoed them; both now parse via parseRedshiftTags and serialize via tagMapToKVList, verified against awsAwsquery_deserializeDocumentHsmClientCertificate/HsmConfiguration's Tags case. Also found and fixed while verifying: CreateHsmConfiguration read the IP address request param as 'HsmIPAddress' but the real wire param is case-different 'HsmIpAddress' (confirmed against awsAwsquery_serializeOpDocumentCreateHsmConfigurationInput) -- url.Values lookups are case-sensitive, so a real SDK client's HsmIpAddress was silently dropped on every call; fixed."}
  CustomDomainAssociation: {status: ok, note: "field-diffed, no changes needed to Create/Delete/Describe/Modify wire shapes. FIXED: ErrCustomDomainAlreadyExists was a fabricated 'CustomDomainAssociationAlreadyExistsFault' code -- no such fault exists in the real SDK; the real conflict fault for CreateCustomDomainAssociation is CustomCnameAssociationFault (confirmed against the op's error-dispatch table), now fixed."}
  EndpointAccess: {status: ok, note: "FIXED THIS PASS (major param-shape bug): CreateEndpointAccess/ModifyEndpointAccess read/wrote a fabricated 'VpcId' parameter that does not exist anywhere in CreateEndpointAccessInput/ModifyEndpointAccessInput -- real requests carry SubnetGroupName/ResourceOwner/VpcSecurityGroupIds (Create) and VpcSecurityGroupIds only (Modify); VpcId on the response is *derived* from the subnet group, not settable directly. Rebuilt CreateEndpointAccess/ModifyEndpointAccess signatures and wire parsing/serialization around the real fields (SubnetGroupName, ResourceOwner, VpcSecurityGroupIds -> VpcSecurityGroups>VpcSecurityGroup list on the response), with VpcID derived via a ClusterSubnetGroup lookup when SubnetGroupName is known. VpcEndpoint (network interfaces) intentionally left unmodeled -- reconfirmed 2026-08-08: real types.VpcEndpoint.NetworkInterfaces needs AvailabilityZone/PrivateIpAddress/NetworkInterfaceId/SubnetId per ENI, none of which this backend's Subnet type carries (no CIDR/AZ data at all), and VpcEndpointId would have to be a fabricated ID with no real ENI allocation behind it -- left absent rather than invented, see items_still_open."}
  EndpointAuthorization: {status: ok, note: "AuthorizeEndpointAccess/RevokeEndpointAccess/DescribeEndpointAuthorization field-diffed against types.EndpointAuthorization, no changes needed"}
  Integration: {status: ok, note: "FIXED THIS PASS: (1) CreateIntegration read 'KmsKeyId' but the real wire param is case-different 'KMSKeyId' (confirmed against the query-protocol serializer) -- url.Values lookups are case-sensitive, so this silently dropped the KMS key for every real client call; (2) tags use 'TagList' not 'Tags' on this op specifically (unlike every other Create* op in this service) and were not parsed at all -- added parseTagListPrefixed and wired it in, response now includes Tags; (3) CreateTime was never serialized -- added; (4) ModifyIntegration was missing IntegrationName (real ModifyIntegrationInput supports renaming), added with existing-name-conflict handling."}
  IdcApplication: {status: ok, note: "FIXED THIS PASS (bd gopherstack-0eyk): CreateRedshiftIdcApplicationResult/ModifyRedshiftIdcApplicationResult were serializing redshiftIdcAppXML's fields directly under the Result element; the real deserializer (awsAwsquery_deserializeOpDocumentCreateRedshiftIdcApplicationOutput/...Modify... in aws-sdk-go-v2/service/redshift@v1.65.0/deserializers.go, confirmed by reading it directly) requires them nested one level deeper under an inner <RedshiftIdcApplication> element -- a real SDK client parsing either response previously got every field as zero-value. Both response structs' xml tags fixed to `...Result>RedshiftIdcApplication`, matching the sibling Qev2IdcApplication family's pattern. DescribeRedshiftIdcApplications's <member> list wrapping and DeleteRedshiftIdcApplication (no response body) were re-checked against the same deserializers.go and confirmed already correct -- no changes needed there. Tests strengthened: Create/Modify success cases now assert the literal nested envelope string, not just substring presence of field values, so this class of bug is caught going forward; Describe's list_all case likewise now asserts the <member> wrapping explicitly. FIXED 2026-08-08 (bd gopherstack-emho): ApplicationType ('None'/'Lakehouse' enum) was unmodeled -- CreateIdcApplication now accepts and stores it (confirmed real request field via awsAwsquery_serializeOpDocumentCreateRedshiftIdcApplicationInput), echoed on Create/Describe/Modify responses; it is create-only, matching real ModifyRedshiftIdcApplicationInput which has no field for it (confirmed against awsAwsquery_serializeOpDocumentModifyRedshiftIdcApplicationInput). ServiceIntegrations deliberately left unmodeled -- it is a 3-level-deep tagged union (ServiceIntegrationsUnion -> {LakeFormation,Redshift,S3AccessGrants} -> per-family scope unions), disproportionate to this pass's scope; see items_still_open. AuthorizedTokenIssuerList/SsoTagKeys/IdcManagedApplicationArn/IdcOnboardStatus/IdentityNamespace remain unmodeled too."}
  Qev2IdcApplication: {status: ok, note: "NEW FAMILY THIS PASS (2026-07-25, SDK v1.62.3 -> v1.65.0 added CreateQev2IdcApplication/DeleteQev2IdcApplication/DescribeQev2IdcApplications/ModifyQev2IdcApplication). Confirmed via aws-sdk-go-v2/service/redshift@v1.65.0/types.Qev2IdcApplication and the Create/Delete/Describe/Modify Input/Output shapes that this is a DISTINCT resource from RedshiftIdcApplication, not a sub-resource -- no shared ID space, no cross-reference field either direction, and Qev2IdcApplication has no IamRoleArn (RedshiftIdcApplication's federated-auth role) at all. Implemented as its own store.Table/model/handler file pair. Wire-diffed field-by-field against serializers.go/deserializers.go: Create/Modify responses correctly nest the inner <Qev2IdcApplication> element (the bug found in the sibling family above, avoided here); Describe response uses real Marker/MaxRecords pagination (this op IS paginated in the real API, unlike DescribeRedshiftIdcApplications which this backend never paginates) implemented via the exact same sorted-snapshot/marker-cutoff convention as DescribeClusters; list items use <member> wrapping (confirmed against awsAwsquery_deserializeDocumentQev2IdcApplicationList); Tags round-trip via Tags.Tag.N.Key/Value on create and Tags>Tag on responses, matching this package's tagMapToKVList/parseRedshiftTags helpers exactly (real field name is 'Tags', not 'TagList' as CreateIntegration idiosyncratically uses). Cardinality: name-keyed uniqueness -> Qev2IdcApplicationAlreadyExists (real fault code, confirmed against types/errors.go; no separate quota fault exists for this family, unlike RedshiftIdcApplicationQuotaExceededFault). Modify only accepts IdcDisplayName (real ModifyQev2IdcApplicationInput has no other mutable field) -- IdcInstanceArn/Qev2IdcApplicationName verified immutable post-creation and covered by a regression test."}
  ReservedNode: {status: ok, note: "AcceptReservedNodeExchange/PurchaseReservedNodeOffering/Describe*/GetReservedNodeExchange* field-diffed, real state mutation confirmed. FIXED 2026-08-08 (bd gopherstack-emho): RecurringCharges is now derived from the node's own UsagePrice (this backend's real per-offering pricing model, see defaultReservedNodeOfferings) -- a No Upfront offering's nonzero UsagePrice produces one RecurringCharges>RecurringCharge{Hourly} entry, an All Upfront offering's zero UsagePrice produces none, verified against awsAwsquery_deserializeDocumentRecurringChargeList's RecurringCharges>RecurringCharge wrapper. ReservedNodeOfferingType remains unmodeled -- see items_still_open."}
  TableRestoreStatus/RestoreTableFromClusterSnapshot: {status: ok, note: "FIXED THIS PASS: SnapshotIdentifier was parsed from the request and then explicitly discarded (bound to `_`), never stored -- now stored and serialized. RequestTime was computed but never serialized on ANY response (RestoreTableFromClusterSnapshotResult only echoed TableRestoreRequestId+Status) -- now serialized as RFC3339 on both RestoreTableFromClusterSnapshot and DescribeTableRestoreStatus. Also fixed the response's TargetTableName wire tag to the real 'NewTableName' (TableRestoreStatus has no TargetTableName field in the real SDK). SourceSchemaName/TargetSchemaName/ProgressInMegaBytes/TotalDataInMegaBytes/EnableCaseSensitiveIdentifier intentionally left unmodeled -- see items_still_open."}
  Partner: {status: ok, note: "FIXED THIS PASS (severe, systemic): AddPartner/DeletePartner/DescribePartners/UpdatePartnerStatus all read/wrote a fabricated 'PartnerIntegrationId' parameter/wire-field name -- no such name exists anywhere in the real SDK (AddPartnerInput/Output, DeletePartnerInput/Output, UpdatePartnerStatusInput/Output, and PartnerIntegrationInfo all use 'PartnerName', confirmed against every relevant api_op_*.go and the DescribePartners deserializer). Every real client's PartnerName value was silently dropped on every request, and every response field a real client tried to read came back empty. Fixed across all 4 ops plus the internal error message text. Regression test locks in the exact wire element name."}
  Descriptive/static ops: {status: ok, note: "DescribeAccountAttributes, DescribeClusterVersions, DescribeClusterTracks, DescribeOrderableClusterOptions, DescribeStorage, DescribeNodeConfigurationOptions, DescribeClusterDbRevisions, ListRecommendations, ModifyAquaConfiguration, ModifyClusterDbRevision, ModifyLakehouseConfiguration, GetIdentityCenterAuthToken, RegisterNamespace/DeregisterNamespace spot-checked: real state mutation/derivation confirmed (e.g. ListRecommendations derives from live cluster state, not canned), no-stub scan (grep for notImplemented/TODO/stub) clean. NOT exhaustively field-diffed element-by-element this pass -- see items_still_open."}
  Redshift Serverless: {status: deferred, note: "ServerlessHandler in handler_serverless.go (Namespace/Workgroup/Snapshot/UsageLimit/ScheduledAction/Credentials, 25 ops) is a separate JSON-protocol API surface (different AWS service ID: redshift-serverless), not touched this pass -- see items_still_open. SIZED 2026-08-08 (bd gopherstack-emho): confirmed LARGE, assessed but explicitly not started. aws-sdk-go-v2/service/redshiftserverless is not even a pinned go.mod dependency in this repo (only redshift and redshiftdata are) -- every existing op in this surface was built without ever field-diffing against the real SDK, so a real pass needs (1) adding the module dependency, (2) a from-scratch field-level audit of all 25 existing ops against JSON (not query-XML) wire shapes, and (3) whole missing resource families this backend has zero code for today (EndpointAccess, CustomDomainAssociation, ResourcePolicy, RecoveryPoint, SnapshotCopyConfiguration, TableRestoreStatus, Tagging, ListManagedWorkgroups, restore-from-snapshot/recovery-point ops). This is a different protocol needing its own verification methodology, not a same-shaped extension of this pass's query-XML field additions -- deliberately not begun; flagged for a dedicated follow-up."}
gaps: []          # bd gopherstack-0eyk (IdcApplication missing inner <RedshiftIdcApplication>
                   # wrapper) FIXED this pass -- see families.IdcApplication above for detail.
deferred: []      # all 17 prior deferred families field-diffed in the 2026-07-22 pass, see families above
leaks: {status: clean, note: "reviewed reconciler.go: StartReconciler/StopReconciler use a WaitGroup + stop channel, idempotent, no per-cluster goroutines. New Qev2IdcApplication store.Table this pass introduces no goroutines/tickers -- registered through the existing store.Registry the same way every other table is (store_setup.go), snapshotted/restored generically via registry.SnapshotAll/RestoreAll, no bespoke persistence code added."}
---

## Notes

### 2026-08-08 pass: remaining-fields sweep (bd gopherstack-emho)

Modeled the 8 fields tracked by bd gopherstack-emho, each verified against
`aws-sdk-go-v2/service/redshift@v1.65.4`'s serializers.go/deserializers.go before
writing: `UsageLimit`/`SnapshotCopyGrant`/`HsmClientCertificate`/`HsmConfiguration`
`Tags` (all use the existing shared `Tags>Tag` wire convention via
`tagMapToKVList`/`parseRedshiftTags` -- no new tag mechanism added),
`IdcApplication.ApplicationType`, `ReservedNode.RecurringCharges` (derived from
the node's own real `UsagePrice`, not fabricated), `ScheduledAction.NextInvocations`
(a real `at()`/`cron()` evaluator, `schedule.go`, since `Schedule` already carries a
real expression), and `ClusterSubnetGroup.VpcId` (removed the fabricated
`CreateClusterSubnetGroupInput.VpcId` request param real clients never send; the
response field is left honestly empty absent EC2 cross-reference data). Also found
and fixed while verifying HSM: `CreateHsmConfiguration` read the request's IP
address as `HsmIPAddress`, but the real wire param is `HsmIpAddress` (case-different)
-- silently dropped for every real SDK client.

Deliberately left unmodeled, with reasoning recorded per-field: `IdcApplication.
ServiceIntegrations` (3-level-deep tagged union, disproportionate), `EndpointAccess.
VpcEndpoint` (no per-ENI AZ/IP/subnet data in this backend to derive it from).

Assessed the Redshift Serverless surface (`handler_serverless.go` + `serverless*.go`,
25 ops, JSON protocol) per this issue's instruction to size before starting: LARGE,
not started. `aws-sdk-go-v2/service/redshiftserverless` is not even a pinned go.mod
dependency here, so none of the existing 25 ops have ever been field-diffed against
the real SDK, and whole resource families (EndpointAccess, CustomDomainAssociation,
ResourcePolicy, RecoveryPoint, SnapshotCopyConfiguration, TableRestoreStatus,
Tagging, ListManagedWorkgroups) have zero code today. Flagged for a dedicated
follow-up issue rather than begun partially.

Protocol: query/XML (`Version=2012-12-01`), same envelope convention as EC2 -- see
`redshiftXMLNS`/`marshalXML` in handler.go. Timestamps are wire-formatted as RFC3339
strings (`time.Now().UTC().Format(time.RFC3339)`), matching `smithytime.ParseDateTime`
used by the SDK's query-XML deserializer. Do not switch to epoch numbers for this
service -- that's a JSON-protocol convention used elsewhere (`pkgs/awstime.Epoch`),
not query-XML.

Real AWS error `ErrorCode()` strings are NOT consistent about a trailing "Fault"
suffix -- some fault types' `ErrorCode()` strip it (`ClusterNotFoundFault` ->
`"ClusterNotFound"`), others keep it (`HsmConfigurationNotFoundFault` ->
`"HsmConfigurationNotFoundFault"`), and some resource families use an entirely
different fault than their name would suggest (data share lookup failures use
`InvalidDataShareFault`, not a `DataShareNotFound`-shaped fault; a resource-policy
lookup failure uses the generic `ResourceNotFoundFault`). Every sentinel in
errors.go was individually checked against `aws-sdk-go-v2/service/redshift@v1.62.3/
types/errors.go`'s `ErrorCode()` bodies this pass -- do not "clean up" perceived
inconsistency in that file without re-checking the SDK source per-sentinel.
`resolveErrCode` (handler.go) now derives the wire `<Code>` directly from each
sentinel's own `.Error()` text via `errCodeSentinels` instead of a second duplicated
string table, specifically to prevent the two from silently drifting apart again
(that drift is exactly how the IdcApplication error-code bug happened).

### 2026-07-25 pass: Qev2IdcApplication (new SDK ops) + IdcApplication envelope gap found

The Go SDK modules were bumped (v1.62.3 -> v1.65.0), adding 4 new operations:
`CreateQev2IdcApplication`, `DescribeQev2IdcApplications`, `ModifyQev2IdcApplication`,
`DeleteQev2IdcApplication` -- the Query Editor V2 IAM Identity Center application family.
Implemented for real (routing, backend state in a new `qev2IdcApplications` `store.Table`,
request parsing, response wire shapes field-diffed against
`aws-sdk-go-v2/service/redshift@v1.65.0`'s `types.Qev2IdcApplication` and the
Create/Delete/Describe/Modify Input/Output shapes' own `serializers.go`/`deserializers.go`,
correct fault codes, Snapshot/Restore via the existing generic `store.Registry` machinery).
See `models.go`, `qev2_idc_applications.go`, `handler_qev2_idc_applications.go`, and the new
table cases in `handler_idc_applications_test.go`.

Confirmed `Qev2IdcApplication` is a resource **distinct from** `RedshiftIdcApplication`
(the family added in the 2026-07-22 pass), not a sub-resource of it: no shared ARN/ID space,
no cross-reference field in either direction, and `Qev2IdcApplication` has no `IamRoleArn` at
all (that field only exists on `RedshiftIdcApplication`, which uses it to invoke the IDC
Identity Center API for cluster-level federated auth; Query Editor V2's IdC application has no
equivalent need). Stored and routed entirely separately from the existing family.

While field-diffing the sibling `RedshiftIdcApplication` family closely enough to be sure the
two didn't need to share wiring, found that its Create/Modify response envelopes are missing a
nesting level the real deserializer requires (see `gaps` above and
`families.IdcApplication`) -- left unfixed as out of this pass's declared scope, tracked
instead of silently absorbed into the "ok" rating.

### 2026-07-25 follow-up: IdcApplication envelope gap fixed (bd gopherstack-0eyk)

Fixed the gap tracked above. Confirmed directly against
`aws-sdk-go-v2/service/redshift@v1.65.0/deserializers.go`:
`awsAwsquery_deserializeOpDocumentCreateRedshiftIdcApplicationOutput` and
`...ModifyRedshiftIdcApplicationOutput` both look for a `RedshiftIdcApplication` child element
inside the `...Result` element (`case strings.EqualFold("RedshiftIdcApplication", t.Name.Local)`).
`createIdcApplicationResponse.Result` and `modifyIdcApplicationResponse.Result` in
`handler_idc_applications.go` were tagged `xml:"CreateRedshiftIdcApplicationResult"` /
`xml:"ModifyRedshiftIdcApplicationResult"` with no inner element, so a real SDK client would
decode an empty struct for every Create/Modify call. Fixed both tags to
`...Result>RedshiftIdcApplication`, matching `createQev2IdcApplicationResponse`'s existing
correct `CreateQev2IdcApplicationResult>Qev2IdcApplication` pattern in the sibling file.

Also re-verified `DescribeRedshiftIdcApplicationsResult>RedshiftIdcApplications>member` against
`awsAwsquery_deserializeDocumentRedshiftIdcApplicationList` (list items unwrapped via `member`,
confirmed correct, no change) and `DeleteRedshiftIdcApplication` (real
`DeleteRedshiftIdcApplicationOutput` deserializer parses no body at all -- the handler's
response struct correctly carries no `Result` field, no change needed).

The prior audit missed this because the existing tests asserted only substring presence
(`wantContains: []string{"CreateRedshiftIdcApplicationResponse", "my-app"}`), which passes
whether or not the wrapper element exists. Strengthened `TestHandler_CreateIdcApplication`,
`TestHandler_ModifyIdcApplication`, and `TestHandler_DescribeIdcApplications`'s `success`/
`list_all` cases to assert the literal nested envelope string (e.g.
`<CreateRedshiftIdcApplicationResult><RedshiftIdcApplication>`), the same way the Qev2 sibling
tests already did -- a regression to the old flat shape now fails the table test directly.

### Bugs fixed this pass (2026-07-22)

This pass audited every family PARITY.md previously listed as `deferred:` (17) plus
the 2 `gaps:` items, field-diffing wire shapes against
`aws-sdk-go-v2/service/redshift@v1.62.3`'s serializers.go/deserializers.go/api_op_*.go
rather than trusting the absence of stub patterns. Full detail is in the `families`
table above; the highlights, roughly in order of severity:

1. **`IdcApplication` family was entirely unreachable by real clients.** The
   dispatch table registered handlers under `CreateIdcApplication` etc. instead of
   the real action names `CreateRedshiftIdcApplication` etc. — every real SDK call
   got `InvalidAction`. Also had swapped `IdcInstanceArn`/`IamRoleArn` XML tags
   (values transposed on the wire), wrong request param names, wrong response
   envelope names, and fabricated error codes. All fixed; see handler.go's
   `buildOpsGroup3` and handler_idc_applications.go.

2. **`Partner` family used a fabricated `PartnerIntegrationId` name everywhere**
   instead of the real `PartnerName` — every request/response field for
   AddPartner/DeletePartner/DescribePartners/UpdatePartnerStatus was affected. See
   handler_partners.go and partners.go.

3. **`ScheduledAction.TargetAction`** — the single field that determines what a
   scheduled action actually does — was parsed as a flat string and never
   serialized in any response at all. Rebuilt as the real nested
   `PauseCluster|ResumeCluster|ResizeCluster` tagged union with correct
   `TargetAction.ResizeCluster.ClusterIdentifier=...`-style nested request parsing
   and response serialization. See models.go, scheduled_actions.go,
   handler_scheduled_actions.go.

4. **`ModifyClusterSnapshotSchedule` was a real no-op past input validation** — it
   checked the cluster and schedule both existed and then did nothing, so the
   association was never recorded anywhere and could never be observed. Fixed to
   set/clear `Cluster.SnapshotScheduleIdentifier` (a real Cluster wire field this
   backend wasn't tracking at all) and derive `SnapshotSchedule.AssociatedClusters`
   live from it.

5. **`ResizeCluster` gap closed**: now populates `activeResizes` so
   `DescribeResize`/`CancelResize` can observe a resize triggered through the real
   API op (previously only the `AddActiveResizeInternal` test-seed helper could).

6. **`Cluster.Tags` was never embedded inline** on any Cluster-returning response
   (CreateCluster, DescribeClusters, ModifyCluster, ...) — real `Cluster.Tags
   []Tag` is a first-class field on the object itself, not just reachable via the
   separate `DescribeTags` API. Required a `toXMLCluster` -> `Handler` method
   conversion (to reach `DescribeTags`) plus a `toXMLClusterWithTags` split to
   avoid an O(n²) `DescribeTags` re-scan inside `handleDescribeClusters`'s loop.

7. **`EndpointAccess`/`Integration` used fabricated or mis-cased parameter names**
   (`VpcId` doesn't exist on `CreateEndpointAccessInput`/`ModifyEndpointAccessInput`
   — real fields are `SubnetGroupName`/`VpcSecurityGroupIds`; `CreateIntegration`'s
   KMS key param is `KMSKeyId`, not `KmsKeyId`, and its tags param is `TagList`, not
   `Tags`). Both rebuilt around the real wire shapes.

8. Smaller wire-completeness fixes: `DataShare.DataShareType`,
   `EventSubscription.SubscriptionCreationTime`, `TableRestoreStatus.
   SnapshotIdentifier` (previously discarded, not just unserialized) and
   `RequestTime`, and `ResourcePolicy`/`CustomDomainAssociation`'s fabricated error
   codes (`ResourcePolicyNotFound` -> `ResourceNotFoundFault`;
   `CustomDomainAssociationAlreadyExistsFault` -> `CustomCnameAssociationFault`).

Every fix above has a dedicated regression test (see handler_*_test.go files
touched this pass) asserting the corrected wire shape/behavior, not just that the
handler doesn't error.

### Bugs fixed in prior passes (kept for history)

1. `RestoreFromClusterSnapshot` nil `Tags` panic (snapshots.go) — every cluster
   value must have `Tags` initialized; `RestoreFromClusterSnapshot` omitted it,
   crashing `DescribeTags` the instant a snapshot-restored cluster existed.
2. `RestoreFromClusterSnapshot` cluster stuck in `"restoring"` forever — no
   lifecycle transition was scheduled to advance it to `"available"`.
3. `ModifyCluster` `Encrypted`/`EnhancedVpcRouting` could never be turned off —
   both are `*bool` on the real SDK; a plain `bool` couldn't distinguish
   "unspecified" from "explicitly false".
4. `GetClusterCredentials` dropped `Expiration` — computed but never serialized.

### Traps for the next auditor

- `resolveErrCode`'s `errCodeSentinels` table derives the wire `<Code>` from each
  sentinel's own `.Error()` text (see Notes above on the Fault-suffix
  inconsistency). If you add a new sentinel, verify its exact `ErrorCode()` string
  against `aws-sdk-go-v2/service/redshift@v1.62.3/types/errors.go` individually —
  do not assume the pattern from a neighboring sentinel.
- `ScheduledAction.TargetAction`'s `NextInvocations`/`StartTime`/`EndTime` are
  intentionally NOT modeled (empty list / never set) — this backend is
  synchronous/instant-apply and has no cron/at-expression evaluator to compute
  real next-invocation times. An empty `NextInvocations` list is valid per the AWS
  docs (not "must always have up to 5 entries"), so this is a deliberate scope
  bound, not a bug.
- `EndpointAccess.VpcEndpoint` (the nested network-interface/address list) is
  intentionally NOT modeled — would require simulating ENI allocation per subnet,
  out of proportion to this backend's fidelity level elsewhere.
- `ClusterSubnetGroup`'s `CreateClusterSubnetGroupInput` accepting a `VpcId`
  parameter is a PRE-EXISTING fabrication (not touched this pass, not part of the
  audited family list) — the real SDK has no such field (VPC is derived from the
  subnets). Left alone to avoid uncontrolled scope creep into a family this pass
  didn't own; flag for the next audit if `ClusterSubnetGroup` is revisited.
- `ResizeCluster`'s `AllowCancelResize` is always `false` immediately after a
  resize (since this backend applies resizes instantly/synchronously) — a
  `CancelResize` call right after `ResizeCluster` will correctly get
  `InvalidClusterState`, not `ResizeNotFound`. This is intentional, matching real
  AWS's behavior once a resize has actually completed, not a bug.
- The `ApplyImmediately` parameter on `ModifyCluster` is NOT part of the real
  `ModifyClusterInput` wire shape — confirmed again this pass, still intentional
  and covered by its own test (`TestParity_ModifyCluster_ApplyImmediately`). Do
  not remove it.
- `RebootCluster` flips status to `"rebooting"` then immediately back to
  `"available"` within the same call — consistent instant-apply simplification,
  not a bug.
- `DeleteClusterParameterGroup`/similar delete ops still do not special-case
  AWS's `default.*` parameter group protection. Not touched this pass (out of the
  audited family list) — candidate for the next audit if `ClusterParameterGroup`
  is revisited.

### items_still_open (genuinely deferred, NOT reclassified as ok on a no-stub basis)

These are real, identified wire-completeness gaps within families that are
otherwise correctly wired (routing/params/errors/state all verified real) — kept
open rather than silently fixed because each would require non-trivial new
modeling (nested nested nested types, nested list-of-object shapes, nested
nested response subtrees) disproportionate to the traffic these fields see:

- `IdcApplication`: `AuthorizedTokenIssuerList`, `ServiceIntegrations` (3-level
  nested tagged union -- see families.IdcApplication), `SsoTagKeys`,
  `IdcManagedApplicationArn`, `IdcOnboardStatus`, `IdentityNamespace` not
  modeled. (`ApplicationType` FIXED 2026-08-08, see families.IdcApplication.)
- `ReservedNode`: `ReservedNodeOfferingType` not modeled.
  (`RecurringCharges` FIXED 2026-08-08, see families.ReservedNode.)
- `TableRestoreStatus`: `SourceSchemaName`, `TargetSchemaName`,
  `ProgressInMegaBytes`, `TotalDataInMegaBytes`, `EnableCaseSensitiveIdentifier`
  not modeled (this backend's restores complete instantly, so Progress/Total are
  always 0 in practice even if added).
- `EndpointAccess`: `VpcEndpoint` (nested network-interface list) not modeled --
  no per-ENI AZ/IP/subnet data exists in this backend to derive it honestly from
  (see families.EndpointAccess).
- `ScheduledAction`: `StartTime`/`EndTime` not modeled (`NextInvocations` FIXED
  2026-08-08, see families.ScheduledAction).
- `ClusterSubnetGroup`/`EndpointAccess` `VpcId`: honestly empty by default --
  this backend has no EC2 cross-reference to derive a real VPC from subnet IDs
  (see families.ClusterSubnetGroup, FIXED 2026-08-08: the fabricated
  CreateClusterSubnetGroupInput.VpcId request param that used to seed this is
  now removed).
- Descriptive/static ops family: spot-checked (no-stub, real derivation
  confirmed) but not exhaustively field-diffed element-by-element this pass.
- Redshift Serverless (`handler_serverless.go`): separate JSON-protocol API
  surface (`redshift-serverless` service ID), entirely out of scope for this
  query-XML-focused pass — needs its own audit pass against
  `aws-sdk-go-v2/service/redshiftserverless`.
