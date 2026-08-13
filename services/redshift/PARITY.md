---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: redshift
sdk_module: aws-sdk-go-v2/service/redshift@v1.65.4 + aws-sdk-go-v2/service/redshiftserverless@v1.38.5 (pinned in go.mod 2026-08-13, bd gopherstack-0w2p; see "Redshift Serverless" family row)
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
                       # (2026-08-13, gopherstack-3jqz, required-member sweep pass 3):
                       # RegisterNamespace/DeregisterNamespace fixed for real (see families.
                       # NamespaceRegistration) after this manifest's own "Descriptive/static ops" row
                       # falsely claimed them spot-checked. That same re-check turned up two more
                       # real, unfixed no-stub violations of the identical shape in the same family
                       # (ModifyAquaConfiguration, ModifyLakehouseConfiguration), flagged here rather
                       # than fixed in that pass.
                       # FIXED (2026-08-13, gopherstack-6xxt): both ModifyAquaConfiguration and
                       # ModifyLakehouseConfiguration now read and validate ClusterIdentifier for
                       # real (ClusterNotFoundFault on a miss) -- see families.AquaConfiguration/
                       # families.LakehouseConfiguration below. Grade holds at A.
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  RestoreFromClusterSnapshot: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed prior pass: Cluster.Tags nil-panic + stuck-in-restoring lifecycle bug"}
  ModifyCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed prior pass: Encrypted/EnhancedVpcRouting tri-state (*bool). PendingModifiedValues never serialized -- confirmed inert, see Notes, not re-flagged as a gap"}
  GetClusterCredentials: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed prior pass: Expiration now serialized"}
  GetClusterCredentialsWithIAM: {wire: ok, errors: ok, state: ok, persist: n/a}
  ResizeCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED THIS PASS: now populates activeResizes (SUCCEEDED, AllowCancelResize=false) so DescribeResize/CancelResize observe a resize triggered via the real API op, not just AddActiveResizeInternal test seeding -- see gaps history"}
  RegisterNamespace: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (gopherstack-3jqz, required-member sweep pass 3): took `_ url.Values`, ignoring required ConsumerIdentifiers/NamespaceIdentifier (api_op_RegisterNamespace.go:33,41) entirely and returning static XML with no state change -- see families.NamespaceRegistration below."}
  DeregisterNamespace: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (gopherstack-3jqz), same bug and fix as RegisterNamespace -- see families.NamespaceRegistration below."}
  ModifyAquaConfiguration: {wire: ok, errors: ok, state: ok, persist: n/a, note: "FIXED (gopherstack-6xxt): took `_ url.Values`, ignoring the required ClusterIdentifier (api_op_ModifyAquaConfiguration.go) and performing no existence check -- see families.AquaConfiguration below."}
  ModifyLakehouseConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (gopherstack-6xxt): took `_ url.Values`, ignoring ClusterIdentifier plus CatalogName/LakehouseIdcApplicationArn/LakehouseIdcRegistration/LakehouseRegistration, and returned a bare empty response -- see families.LakehouseConfiguration below."}
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
  Descriptive/static ops: {status: ok, note: "RE-AUDITED gopherstack-3jqz (required-member sweep pass 3): the prior claim here -- 'RegisterNamespace/DeregisterNamespace spot-checked: real state mutation/derivation confirmed' -- was FALSE; both took `_ url.Values`, read neither ConsumerIdentifiers nor NamespaceIdentifier, and returned static XML with no state change at all. Moved out of this family (now families.NamespaceRegistration, fixed for real). Re-checking every other op this line vouched for: ListRecommendations and GetIdentityCenterAuthToken hold up -- both genuinely read and validate their input (ListRecommendations derives recommendations from DescribeClusters(id) and surfaces a real ClusterNotFoundFault for an unknown id; GetIdentityCenterAuthToken requires and checks IdentityCenterApplicationArn). DescribeAccountAttributes/DescribeClusterVersions/DescribeClusterTracks/DescribeOrderableClusterOptions/DescribeStorage/DescribeNodeConfigurationOptions/DescribeClusterDbRevisions are legitimately static/filter-less (already disclosed by 'NOT exhaustively field-diffed' below, not a new finding). Two more real bugs of the exact same shape as RegisterNamespace were found here by the same 'does the handler even read `vals`' check (ModifyAquaConfiguration, ModifyLakehouseConfiguration) and moved out to their own families below, same as NamespaceRegistration -- FIXED gopherstack-6xxt, see families.AquaConfiguration/families.LakehouseConfiguration. Restored to ok now that both are real."}
  NamespaceRegistration: {status: ok, note: "FIXED (gopherstack-3jqz, required-member sweep pass 3): RegisterNamespace/DeregisterNamespace previously ignored `_ url.Values` -- the entire request -- and returned static XML with no state change; see the ops: entries above. Both are the awsAwsquery_* (Query) protocol (redshift@v1.65.4 serializers.go), confirmed NOT the stale awsQuery_* prefix the repo's SDK-shape tooling defaults to detecting. NamespaceIdentifier is a union (NamespaceIdentifierUnion: ProvisionedIdentifier{ClusterIdentifier} or ServerlessIdentifier{NamespaceIdentifier,WorkgroupIdentifier}, confirmed against awsAwsquery_serializeDocumentNamespaceIdentifierUnion) arriving as dotted query keys (NamespaceIdentifier.ProvisionedIdentifier.ClusterIdentifier / NamespaceIdentifier.ServerlessIdentifier.{NamespaceIdentifier,WorkgroupIdentifier}), ConsumerIdentifiers as ConsumerIdentifiers.member.N via the existing parseStringList helper. Both variants now validate against REAL backend state before accepting: ProvisionedIdentifier checks b.clusters (ClusterNotFound if missing, InvalidClusterState if not 'available' -- both error codes taken from the op's own declared awsAwsquery_deserializeOpErrorRegisterNamespace/DeregisterNamespace switch, the same three-fault set for both ops: ClusterNotFound/InvalidClusterState/InvalidNamespaceFault), ServerlessIdentifier checks b.slNamespaces/b.slWorkgroups (InvalidNamespaceFault if either is missing) -- this package already models Redshift Serverless namespaces/workgroups internally (serverless.go), so this is real cross-reference validation, not a fabricated check. A new NamespaceRegistration record (namespace_registration.go, persisted via the standard store.Registry/store.Table mechanism) tracks ConsumerIdentifiers/Status per namespace identity; DeregisterNamespace removes exactly the given consumers from the existing set (real AWS scopes deregistration per-consumer, not per-namespace) rather than deleting the whole record. Status is always 'Registering'/'Deregistering' -- confirmed these are the ONLY two enum values NamespaceRegistrationStatus declares (types/enums.go); there is no describe/list operation anywhere in this SDK version for a client to observe a terminal state, so returning the in-flight status on every call is the real, complete contract, not a partial implementation. Proven via TestSDKRoundTrip_RegisterNamespace (real aws-sdk-go-v2 client, six subtests covering both union variants' accept/reject paths, hand-verified to fail against the unfixed handler) and TestNamespaceRegistration_ConsumerIdentifiersStateMutation (drives the backend directly, since there is no wire-level Describe to round-trip the consumer-list mutation through)."}
  AquaConfiguration: {status: ok, note: "FIXED (gopherstack-6xxt): handleModifyAquaConfiguration previously took `_ url.Values`, ignoring the required ClusterIdentifier (api_op_ModifyAquaConfiguration.go) entirely, performing no existence check, and always returning a canned AquaConfigurationStatus=auto/AquaStatus=disabled that didn't even match this backend's own DescribeClusters convention (toXMLClusterWithTags already emits disabled/disabled for every cluster's inline AquaConfiguration). The real op is documented retired (\"Calling this operation does not change AQUA configuration. Amazon Redshift automatically determines whether to use AQUA\") but still requires and existence-checks ClusterIdentifier -- ClusterNotFound is declared in its own error switch (awsAwsquery_deserializeOpErrorModifyAquaConfiguration: ClusterNotFound/InvalidClusterState/UnsupportedOperation). New backend method ModifyAquaConfiguration(id) (cluster_mgmt.go) does the real existence check; the response now shares a single defaultAquaConfig() helper (handler.go) with toXMLClusterWithTags so the two can never diverge again. InvalidClusterState/UnsupportedOperation left undeclared/unused -- no real precondition for either is documented for this retired op, matching this service's existing convention of not inventing trigger conditions for declared-but-unreachable exceptions (see glue's OperationTimeoutException reasoning for the same judgment call in a sibling service)."}
  LakehouseConfiguration: {status: ok, note: "FIXED (gopherstack-6xxt): handleModifyLakehouseConfiguration previously took `_ url.Values`, ignoring ClusterIdentifier plus CatalogName/LakehouseIdcApplicationArn/LakehouseIdcRegistration/LakehouseRegistration (api_op_ModifyLakehouseConfiguration.go) and returning a bare empty response. Classic Cluster (models.go) had no CatalogArn/LakehouseRegistrationStatus fields at all despite both being real, confirmed types.Cluster members (aws-sdk-go-v2/service/redshift@v1.65.4/types/types.go:153,343) -- this backend already modeled the equivalent state for Redshift Serverless (Namespace.CatalogArn/LakehouseRegistrationStatus, families.Redshift Serverless above), so the classic version was simply left behind; now added to Cluster and echoed on every Cluster-returning response (xmlCluster/toXMLClusterWithTags), not just this op's own. New backend method ModifyLakehouseConfiguration (lakehouse.go) follows UpdateLakehouseConfigurationSL's (serverless_lakehouse.go) existing carry-forward-when-omitted pattern: CatalogArn is derived via arn.Build(\"glue\",...,\"catalog/\"+CatalogName) same as the serverless sibling, and a new cluster-keyed store.Table (ClusterLakehouseConfig) holds LakehouseIdcApplicationArn, which has no Cluster member on the real wire either -- observable only through this op's own response, same convention as ServerlessLakehouseConfig. SECOND-LAYER FIND beyond the bd issue's stated scope: LakehouseIdcApplicationArn, when the caller is setting a new one, is now validated against this backend's own RedshiftIdcApplication store (idc_applications.go) via a lock-safe inline scan (idcApplicationExistsLocked) -- real cross-reference validation this backend can perform because it already models that resource, returning RedshiftIdcApplicationNotExists (declared in this op's own error switch, reusing the existing ErrIdcApplicationNotFound sentinel) on a miss; the Serverless sibling has no equivalent IDC-application backend to check against, so it does not do this. SECOND-LAYER FIND: DryRun does NOT map to a DryRunException here the way the Serverless sibling's UpdateLakehouseConfiguration does -- confirmed absent from awsAwsquery_deserializeOpErrorModifyLakehouseConfiguration's declared switch (ClusterNotFound/DependentServiceAccessDenied/DependentServiceUnavailableFault/InvalidClusterState/RedshiftIdcApplicationNotExists/UnauthorizedOperation/UnsupportedOperation, no DryRun-shaped fault) -- ModifyLakehouseConfigurationInput.DryRun's own doc text ('validates the request without actually modifying the lakehouse configuration') is honored literally instead: a successful DryRun runs every validation and returns the would-be result as a normal 200, without persisting it. DependentServiceAccessDenied/DependentServiceUnavailableFault/UnauthorizedOperation/InvalidClusterState remain undeclared/unused -- no real precondition for any is discoverable from this backend's state, left honest rather than inventing triggers."}
  Redshift Serverless: {status: ok, note: "AUDITED AND PARTLY FIXED 2026-08-08 (bd gopherstack-hsfm). aws-sdk-go-v2/service/redshiftserverless was still not a go.mod dependency; fetched via `go get ...@v1.38.5` to populate GOMODCACHE for field-diffing serializers.go/deserializers.go/types directly (not from memory/docs), then `go mod tidy` dropped it again afterward since the fix (like the rest of this repo) hand-rolls JSON wire structs rather than importing SDK types at runtime -- no persistent new dependency. SEVERE FINDING: this whole 25-op surface used REST-style path/verb routing (/redshift-serverless/namespaces, GET/POST/PATCH/DELETE) that NO real client ever sends -- confirmed every awsAwsjson11_serializeOp* in serializers.go POSTs to \"/\" with an X-Amz-Target header and puts all fields (including resource identifiers) in the JSON body. RouteMatcher required the REST path prefix, so a real SDK client's request never matched at all: all 25 ops were unroutable, the same unreachable-service bug class found in opsworks (gopherstack-vjj2) but total instead of partial. FIXED: RouteMatcher/ExtractOperation rewritten to X-Amz-Target dispatch (PriorityHeaderExact, matching redshiftdata.Handler's existing pattern in this package); every handler decodes resource identifiers from the body instead of the URL. Also fixed while rewriting (all confirmed against deserializers.go before fixing): ServerlessScheduledAction's status field used wire key \"status\" but the real ScheduledActionResponse field is \"state\" (types.State, ACTIVE/DISABLED) -- ScheduledActionResponse has no \"status\" field at all; StartTime/EndTime and GetCredentialsOutput's Expiration/NextRefreshTime were RFC3339 strings but the real wire format is epoch-seconds JSON numbers (awstime.Epoch, same bug class as the QuickSight/IoT precedent in parity-principles.md); Schedule/TargetAction were flat strings but the real shapes are tagged-union JSON objects ({\"cron\":...}/{\"at\":...} and {\"createSnapshot\":{...}}) -- now passed through as json.RawMessage (accurate shape, no fabricated execution semantics); CreateScheduledActionInput.RoleArn (a REQUIRED real field) was completely absent from the request struct, so every real client's roleArn was silently dropped and unrecoverable -- now required and stored; Enabled/ScheduledActionDescription were also dropped, now threaded through; ScheduledActionUUID and the fabricated scheduledActionArn field (not a real ScheduledActionResponse member) were fixed to match the real shape. Also fixed accepted-then-dropped (a) fields: Namespace.DefaultIamRoleArn, ManageAdminPassword/AdminPasswordSecretKmsKeyId (with a fabricated-but-consistent secretsmanager ARN, same convention as this backend's other resource ARNs); DeleteNamespace's FinalSnapshotName/FinalSnapshotRetentionPeriod now actually create a final snapshot; CreateSnapshot's retentionPeriod; Workgroup's ConfigParameters/MaxCapacity/Port/IpAddressType/TrackName/PricePerformanceTarget/EnhancedVpcRouting/ExtraComputeForAutomaticOptimization/PubliclyAccessible; GetCredentials' DurationSeconds and the previously entirely-absent NextRefreshTime response field; List*'s MaxResults, which was hardcoded to 0 and silently ignored on every List call regardless of protocol. Error envelope switched from ad hoc 404/409 status codes to the real awsJson1.1 convention (HTTP 400 for every client-fault exception, confirmed by the absence of any per-exception status override in types/errors.go). Deliberately left unfixed, each independently verified absent from all reachable output: Tags on Create* (defers to the excluded Tagging family below), AdminUserPassword (real API never echoes it either), Namespace.RedshiftIdcApplicationArn (accepted by the real API but not a field on types.Namespace -- no observable output surface exists for it among these 25 ops), ScheduledActionResponse.NextInvocations (this service's cron format is unwrapped, unlike classic Redshift's cron(...)/at(...) strings that schedule.go already evaluates -- adapting that evaluator is a reasonable follow-up, not done this pass), Snapshot's backup-progress/size/cross-account-restore-access fields (this backend creates snapshots instantaneously, so progress fields have no real driving state; restore-access fields are populated via the excluded ResourcePolicy family), GetCredentials' CustomDomainName lookup (depends on the excluded CustomDomainAssociation family). Full field-by-field audit table with file:line citations recorded in bd gopherstack-hsfm's close reason. Whole missing resource families (EndpointAccess, CustomDomainAssociation, ResourcePolicy, RecoveryPoint, SnapshotCopyConfiguration, TableRestoreStatus, Tagging, ListManagedWorkgroups, restore-from-snapshot) still have zero code -- see items_still_open. TAGGING AND CUSTOMDOMAINASSOCIATION BUILT 2026-08-09 (bd gopherstack-w8g2): TagResource/UntagResource/ListTagsForResource and Create/Get/List/Update/DeleteCustomDomainAssociation implemented against the pinned botocore redshift-serverless/2021-04-21/service-2.json model (json protocol, confirmed via metadata.protocol), not the aws-sdk-go-v2 module (kept out of go.mod per this issue's constraint -- verified via TagList's Tag{key,value} shape, not a JSON map). Confirmed only Namespace/Workgroup/Snapshot accept a create-time \"tags\" list (CreateUsageLimitRequest/CreateScheduledActionRequest have none) and that none of Namespace/Workgroup/Snapshot echo a \"tags\" field on their own GET/response shape -- tags are stored in a new resourceArn-keyed store.Table (slResourceTags) reachable only via ListTagsForResource, proven with a handler-level round trip (TestServerless_TagResource_RoundTrip) plus a persistence Snapshot/Restore round trip. CustomDomainAssociation modeled per Association{customDomainCertificateArn,customDomainCertificateExpiryTime,customDomainName,workgroupName} (Create/Get/Update responses are flat, NOT wrapped in an envelope key, unlike every other serverless resource -- confirmed against the Response shapes directly; Delete has zero response members); customDomainCertificateExpiryTime uses SyntheticTimestamp_date_time (ISO8601 string), NOT the epoch-seconds Timestamp shape GetCredentials' Expiration/NextRefreshTime use -- confirmed as a genuine per-field wire-format difference, not an inconsistency to \"fix\". Real Workgroup also carries customDomainName/customDomainCertificateArn/customDomainCertificateExpiryTime directly (added to the Workgroup struct, mirrored on associate/update/delete). GetCredentials now resolves workgroupName via customDomainName per GetCredentialsRequest's documented either-or requirement. EndpointAccess/ResourcePolicy/RecoveryPoint/SnapshotCopyConfiguration/TableRestoreStatus/ListManagedWorkgroups/restore ops deliberately NOT attempted this pass -- see items_still_open. RESOURCEPOLICY AND SNAPSHOTCOPYCONFIGURATION BUILT 2026-08-10 (bd gopherstack-w8g2): Get/Put/DeleteResourcePolicy implemented as a new resourceArn-keyed store.Table[ServerlessResourcePolicy] (slResourcePolicies), distinct from classic Redshift's own resourcePolicies table/methods (same op names, different protocol and sentinel error, disambiguated with an SL suffix on the backend methods). Envelope convention (`{\"resourcePolicy\": {...}}`) and DeleteResourcePolicyResponse's zero members both confirmed against service-2.json -- the flat-response oddity found in CustomDomainAssociation does NOT generalize here. Create/Update/Delete/ListSnapshotCopyConfiguration implemented as a new store.Table[ServerlessSnapshotCopyConfiguration] (slSnapshotCopyConfig) plus a sortedStringIndex for List's deterministic pagination; CreateSnapshotCopyConfiguration validates namespaceName against the existing namespace store (ResourceNotFoundException on a miss). This backend does not simulate real cross-region replication, consistent with how Namespace/Workgroup/Snapshot are already handled -- only the configuration object itself is tracked. One business rule was deliberately NOT invented: service-2.json documents no one-configuration-per-namespace constraint, so none is enforced (unlike classic Redshift's EnableSnapshotCopy, which this backend does gate one-per-cluster, but that is a different family entirely). EndpointAccess/RecoveryPoint/TableRestoreStatus/ListManagedWorkgroups/restore ops remain unbuilt -- see items_still_open. RECOVERYPOINT AND TABLERESTORESTATUS BUILT 2026-08-10 (bd gopherstack-w8g2, entangled group): Get/ListRecoveryPoints, RestoreFromRecoveryPoint, RestoreTableFromSnapshot, RestoreTableFromRecoveryPoint, Get/ListTableRestoreStatus implemented. RecoveryPoint has NO create operation anywhere in service-2.json (\"Recovery points are created every 30 minutes and kept for 24 hours\", confirmed on the RecoveryPoint shape's own documentation) -- this backend generates exactly one recovery point per workgroup at CreateWorkgroup time instead of running a real 30-minute scheduler (generateRecoveryPointLocked, serverless_recovery.go), matching this service's existing instant-apply convention (e.g. snapshots created instantaneously); an AddRecoveryPointInternal test-seed method exists for tests that need more than one, not wired to any wire-reachable op, same convention as AddSnapshotInternal etc. RestoreFromSnapshot (namespace-level restore from a Snapshot, no recovery point involved) was deliberately NOT built this pass -- it does not depend on RecoveryPoint and was excluded from this entangled group by design; still open, see items_still_open. Timestamp formats verified to genuinely differ within this one family: RecoveryPoint.recoveryPointCreateTime is SyntheticTimestamp_date_time (ISO8601 string, confirmed against both service-2.json and awsAwsjson11_deserializeDocumentRecoveryPoint's smithytime.ParseDateTime call), while TableRestoreStatus.requestTime is the bare Timestamp shape (epoch-seconds JSON number, confirmed against awsAwsjson11_deserializeDocumentTableRestoreStatus's smithytime.ParseEpochSeconds call) -- two timestamp fields in the same entangled group, two different wire formats, both re-verified rather than assumed from the nearer-looking sibling. RestoreFromRecoveryPointSL additionally validates that the given workgroupName belongs to the given namespaceName (the same Namespace-Workgroup FK relationship CreateWorkgroup already enforces) -- not a fabricated recovery-point-specific rule, just this backend's existing invariant applied here too. ServerlessTableRestoreStatus.Status is set to SUCCEEDED immediately (this backend applies every restore synchronously, consistent with the rest of this service) rather than left IN_PROGRESS forever the way classic Redshift's own TableRestoreStatus is (a pre-existing, out-of-scope quirk in table_restore.go, not touched); ProgressInMegaBytes/TotalDataInMegaBytes are honestly left at zero/omitted rather than fabricated, since this backend has no real data to move. EndpointAccess and ListManagedWorkgroups remain unbuilt -- see items_still_open. ENDPOINTACCESS, LISTMANAGEDWORKGROUPS, RESTOREFROMSNAPSHOT AND CONVERTRECOVERYPOINTTOSNAPSHOT BUILT 2026-08-10 (bd gopherstack-w8g2, final pass -- closes the issue): Create/Get/List/Update/DeleteEndpointAccess implemented as a new endpointName-keyed store.Table[ServerlessEndpointAccess] (slEndpointAccesses), distinct from classic Redshift's own cluster-keyed EndpointAccess (endpoint_access.go) -- real CreateEndpointAccessRequest requires workgroupName/subnetIds (individual subnet IDs), not clusterIdentifier/subnetGroupName, confirmed against CreateEndpointAccessRequest/UpdateEndpointAccessRequest/EndpointAccess in service-2.json and cross-checked against types.EndpointAccess in aws-sdk-go-v2/service/redshiftserverless@v1.38.5/types/types.go. Per this issue's explicit instruction to check how classic Redshift's own EndpointAccess handled the same judgment call: confirmed families.EndpointAccess above left the entire nested VpcEndpoint object (network interfaces) absent rather than invented, and the identical problem exists here in a slightly different shape -- real types.VpcEndpoint carries vpcEndpointId/vpcId/networkInterfaces (each NetworkInterface needing availabilityZone/privateIpAddress/networkInterfaceId/subnetId, confirmed against types.NetworkInterface), none of which this backend tracks anywhere (no EC2 cross-reference wired into Redshift at all, same finding as families.ClusterSubnetGroup). Followed the same precedent exactly: vpcEndpoint is left absent from every response rather than partially fabricated (e.g. a real-looking vpcEndpointId with no ENI behind it). VpcSecurityGroups IS modeled (unlike VpcEndpoint) since it only echoes client-supplied IDs, the same shape as classic's own VpcSecurityGroupMembership, reusing its \"active\" status convention (endpointStatusActive) since both are the identical real shape. ListEndpointAccessRequest's vpcId filter is deliberately not accepted for the same reason -- nothing honest to filter against. DeleteEndpointAccessResponse echoes the deleted object (confirmed against service-2.json: it carries a real \"endpoint\" member, unlike DeleteResourcePolicy/DeleteCustomDomainAssociation's zero-member responses). LISTMANAGEDWORKGROUPS: per this issue's instruction to check whether the \"thin, no real backing state\" judgment holds -- it does. ListManagedWorkgroupsRequest.sourceArn is documented and pattern-constrained as a Glue Data Catalog database/catalog ARN (`^arn:aws[a-z-]*:glue:...`, confirmed in the SourceArn shape), meaning ManagedWorkgroupListItem represents a workgroup Glue/Lake Formation auto-provisions when federated queries run against shared data -- confirmed by grep that this package has zero Glue Data Catalog or Lake Formation integration anywhere (AssociateDataShareConsumer is classic Redshift's unrelated data-sharing feature, not this). Implemented as an honest, correctly-shaped, always-empty response (ListManagedWorkgroupsSL) rather than inventing entries -- no store.Table needed since there is no create path, real or otherwise, that could ever populate one. RESTOREFROMSNAPSHOT: RestoreFromSnapshotRequest requires namespaceName/workgroupName (confirmed against service-2.json) with the identical \"name of the namespace to restore ... to/into\" wording convention and required-field shape RestoreFromRecoveryPointRequest already uses -- by that symmetry, both are treated as pre-existing resources here too (same design RestoreFromRecoveryPointSL established in the prior pass), validated via the same Namespace-Workgroup FK check. Resolves snapshotName or snapshotArn (either, mutually exclusive per the real request) via the same ARN-suffix-stripping convention GetServerlessSnapshot already uses. manageAdminPassword/adminPasswordSecretKmsKeyId are threaded through onto the namespace (a real, easy-to-honor field, not left as an inert accepted-then-dropped parameter) but only in the true direction -- false does not clear existing Secrets-Manager fields, since real AWS's documented false-branch behavior (\"uses the admin credentials the namespace or cluster had at the time the snapshot was taken\") is data this backend cannot reconstruct, so it is left untouched rather than fabricated. Real AWS restores a namespace's storage layer in place; this backend does not simulate real data content, so once the lookup/FK checks pass, the existing Namespace is returned unchanged, same as RestoreFromRecoveryPointSL. CONVERTRECOVERYPOINTTOSNAPSHOT: recoveryPointId/snapshotName both required (confirmed against service-2.json); implemented by writing a new ServerlessSnapshot from the recovery point's namespace linkage (NamespaceName/NamespaceArn) plus the target namespace's AdminUsername when resolvable, reusing the exact same snapshotName-conflict check and arn.Build/store/index-insert/putServerlessTagsLocked sequence CreateServerlessSnapshot already uses. All four verified to genuinely fail beforehand: temporarily removed their slDispatchTable entries (file copy, not git stash) and reran the new tests -- every one flipped from real behavior to \"unknown operation\" ValidationException/400, confirmed, then the entries were restored. go.mod/go.sum confirmed unmodified (git status clean before and after fetching aws-sdk-go-v2/service/redshiftserverless@v1.38.5 and aws-sdk-go-v2/service/redshift@v1.65.4 into GOMODCACHE via `go get` then reverting) and `go mod tidy` produced no diff. This closes bd gopherstack-w8g2: all nine originally-missing serverless families now have real code. GO.MOD PIN + NINE FIELD GAPS + PHANTOM FIELD FIXED 2026-08-13 (bd gopherstack-0w2p/8v8v/mbcq): aws-sdk-go-v2/service/redshiftserverless was STILL not a go.mod dependency despite the note above (the 2026-08-08 `go get`/`go mod tidy` round-trip left no persistent pin, exactly as documented) -- every audit of this surface, including the one that produced this entry's own predecessors, was reading whatever version happened to be in a dev machine's module cache. Fixed properly this time: added `github.com/aws/aws-sdk-go-v2/service/redshiftserverless v1.38.5` as an explicit go.mod requirement (v1.38.5 chosen deliberately -- confirmed via `go list -m -json` that it shares the exact same release timestamp, 2026-08-05T18:20:26Z, as the already-pinned redshift@v1.65.4 and redshiftdata@v1.43.4, i.e. the same upstream release batch, rather than the newer v1.38.6 sitting alone in the module graph), and added TestSDKCompleteness_Serverless (sdk_completeness_test.go) so `go mod tidy` has a real import to keep -- this package hand-rolls JSON wire structs and imports no SDK types at runtime, so without that test the requirement would be silently stripped again on the next tidy. That completeness test immediately surfaced 10 SDK operations with zero code that no prior audit had caught (CreateReservation, GetIdentityCenterAuthToken, GetReservation, GetReservationOffering, GetTrack, ListReservationOfferings, ListReservations, ListTracks, UpdateLakehouseConfiguration, UpdateSnapshot -- separate feature surfaces: capacity reservations, tracks, lakehouse config, IDC token vending, plus a plain UpdateSnapshot gap); filed as gopherstack-irh7, deliberately NOT built this pass (out of scope), listed in the test's notImplemented slice with a comment. Re-verified gopherstack-8v8v and gopherstack-mbcq's findings against the now-pinned v1.38.5 source directly (api_op_*.go/types/types.go in GOMODCACHE) rather than trusting the prior audit's citations: all held exactly as reported, no findings changed -- the module cache copy the prior audit read from was already v1.38.5, same as what's now pinned. FIXED gopherstack-8v8v: UpdateNamespace accepted a `dbName` request field and mutated Namespace.DBName from it (serverless_namespaces.go); UpdateNamespaceInput has no dbName member at all (confirmed against api_op_UpdateNamespace.go -- a namespace's database name cannot be changed after creation), while CreateNamespaceInput does have one (real, kept). Field and mutation removed; UpdateNamespaceParams no longer carries DBName. FIXED gopherstack-mbcq's nine gaps, each re-verified against api_op_*.go before fixing: (1) AdminUserPassword added to CreateNamespace/UpdateNamespace -- the only way to set an explicit admin password outside the ManageAdminPassword/Secrets-Manager path; as a credential it is read from the wire, threaded through *Params structs, but explicitly discarded (`_ = p.AdminUserPassword`, documented) before ever reaching the Namespace struct -- same accept-but-never-store convention this package's own CreateCluster already uses for classic Redshift's MasterUserPassword (handler.go/cluster_mgmt.go), and consistent with real AWS itself: types.Namespace has no adminUserPassword member either, so no client can ever observe whether this backend stores it. Proven never echoed by TestServerless_Namespace_AdminUserPassword_NeverEchoed (asserts the literal secret string is absent from the raw response body, not just the decoded struct). (2) RedshiftIdcApplicationArn added to CreateNamespace, same accept-then-discard treatment -- real types.Namespace has no such member either (confirmed against types/types.go), so this is write-only on the real API too, not merely on this backend. (3) MaintainIntegration added to RestoreFromSnapshot (RestoreFromSnapshotParams) -- accepted but inert, documented: this backend does not model data-sharing/zero-ETL/S3-event integration state on namespaces at all, so there is nothing to maintain or drop. (4) ActivateCaseSensitiveIdentifier added to the shared slTableRestoreReq/RestoreTableFromSnapshotParams used by both RestoreTableFromSnapshot and RestoreTableFromRecoveryPoint -- accepted but inert, documented: this backend never executes queries against a restored table, so there is no case-sensitive identifier matching to gate. Five real filter gaps fixed (all previously accepted-and-silently-ignored, each proven to narrow a multi-item result set by a new test, not just parse): ListSnapshots gained EndTime/StartTime (bound SnapshotCreateTime, epoch-seconds on the wire per serializers.go, reusing the existing slEpochFromPtr helper), NamespaceArn (compares against the already-stored ServerlessSnapshot.NamespaceArn), and OwnerAccount; ListRecoveryPoints gained EndTime/StartTime bounding RecoveryPointCreateTime; ListWorkgroups gained OwnerAccount; GetSnapshot gained OwnerAccount; ListUsageLimits gained UsageType (compares against the already-stored ServerlessUsageLimit.UsageType). OwnerAccount on all three (ListSnapshots/ListWorkgroups/GetSnapshot) is honestly single-account: this backend never simulates cross-account snapshot/workgroup sharing for the serverless surface (AuthorizeSnapshotAccess is not part of this API; ServerlessSnapshot.AccountsWithRestoreAccess is declared for wire shape but never populated), so every resource's real owner is b.accountID -- a non-empty OwnerAccount that doesn't match b.accountID is implemented as matching nothing, same as real AWS would return for an inaccessible cross-account resource, not left as a silently-ignored no-op. Re-confirmed DO-NOT-TOUCH: ListEndpointAccess's VpcId omission (serverless_endpoint_access.go) is still correct and was left untouched -- this backend never derives a real vpcId for any endpoint, so there remains nothing honest to filter against. FOUR OF THE TEN GAPS FROM gopherstack-irh7 FIXED, ONE FAMILY DELIBERATELY DEFERRED 2026-08-13 (bd gopherstack-v4wu): UpdateSnapshot (retentionPeriod is optional and nilable, confirmed against api_op_UpdateSnapshot.go -- omitting it leaves the stored value unchanged, proven by TestServerless_UpdateSnapshot_OmittedRetentionPeriodUnchanged) now completes the Snapshot CRUD family. GetTrack/ListTracks return a static two-entry catalog (current/trailing, both at this backend's single modelVersion10 release) -- the same precedent classic Redshift's own DescribeClusterTracks already set for the identical real-world enumeration (see families.Descriptive/static ops); UpdateTargets is honestly left empty since there is no second release to invent an upgrade path to. UpdateLakehouseConfiguration writes real Namespace.CatalogArn/LakehouseRegistrationStatus (both confirmed present on types.Namespace but previously entirely absent from this backend's Namespace struct -- a genuine pre-existing wire gap, not new fabrication) plus a new namespaceName-keyed store.Table (slLakehouseConfig, serverless_lakehouse.go) for LakehouseIdcApplicationArn, which has no Namespace member at all and is therefore kept out of every other namespace response, observable only via this op's own response, matching the AdminUserPassword accept-then-scope-limited convention already used elsewhere in this family; DryRun=true returns the real DryRunException (confirmed in service-2.json: \"request was successful, but dry run was enabled\") without mutating state, verified by TestServerless_UpdateLakehouseConfiguration_DryRun. LakehouseRegistrationStatus's exact string values (\"Registered\"/\"Deregistered\") are a direct derivation from the client's own LakehouseRegistration request value, not an invented vocabulary -- real AWS documents no enum for this field (plain *string in types.Namespace). GetIdentityCenterAuthToken mints a synthetic opaque token after validating every named workgroup actually exists (a real FK check classic Redshift's own same-named operation, handler_idc_applications.go, does not even perform) -- following the identical honest-limitation precedent classic Redshift's sibling op of the same name already established (no real IAM Identity Center backend exists here to mint a real token). DELIBERATELY NOT BUILT: the reservation-capacity family (CreateReservation/GetReservation/GetReservationOffering/ListReservationOfferings/ListReservations) -- judged as fabrication rather than honest emulation and left in sdk_completeness_test.go's notImplemented slice; see items_still_open for the full reasoning, which turns on ReservationOffering's AWS-set commercial pricing having no fixed SDK-enumerable catalog to derive from (unlike classic Redshift's own ReservedNode, whose curated offering catalog -- see families.ReservedNode -- keys off a small, real, AWS-documented hardware node-type list, not free-floating commercial rates) and this family having zero pre-existing backend state. New store.Table (slLakehouseConfig) registered/reset/persisted via the standard store.Registry mechanism, no snapshot version bump (additive Tables map); wiring proven load-bearing by temporarily removing both the store_setup.go registration and the slDispatchTable entries and confirming the new tests fail (nil-pointer panic and ValidationException \"unknown operation\" respectively) before restoring."}
gaps: []          # bd gopherstack-0eyk (IdcApplication missing inner <RedshiftIdcApplication>
                   # wrapper) FIXED this pass -- see families.IdcApplication above for detail.
deferred: []      # all 17 prior deferred families field-diffed in the 2026-07-22 pass, see families above
leaks: {status: clean, note: "reviewed reconciler.go: StartReconciler/StopReconciler use a WaitGroup + stop channel, idempotent, no per-cluster goroutines. New Qev2IdcApplication store.Table this pass introduces no goroutines/tickers -- registered through the existing store.Registry the same way every other table is (store_setup.go), snapshotted/restored generically via registry.SnapshotAll/RestoreAll, no bespoke persistence code added."}
---

## Notes

### 2026-08-13 pass: ModifyAquaConfiguration, ModifyLakehouseConfiguration (classic Redshift) (bd gopherstack-6xxt)

Follow-up to gopherstack-3jqz below: fixes the two no-op stubs that audit
flagged but left out of scope. Both `handleModifyAquaConfiguration` and
`handleModifyLakehouseConfiguration` (`handler_cluster_mgmt.go`) took
`_ url.Values`, ignoring their required `ClusterIdentifier` entirely with no
existence check. See `families.AquaConfiguration`/`families.LakehouseConfiguration`
above for the full account; short version:

- **`ModifyAquaConfiguration`** now existence-checks `ClusterIdentifier`
  (`ClusterNotFound`, declared in the op's own error switch) via a new
  `ModifyAquaConfiguration(id)` backend method. The canned response is
  unavoidable by design -- the real op is documented retired ("Calling this
  operation does not change AQUA configuration") -- but it now shares a
  single `defaultAquaConfig()` helper with `toXMLClusterWithTags` instead of
  hardcoding a second, different canned value (the stub previously returned
  `AquaConfigurationStatus=auto`, while every `DescribeClusters` response
  already returned `disabled` for the same field -- a real client got a
  different answer depending which op it called).
- **`ModifyLakehouseConfiguration`** required a bigger fix: classic
  `Cluster` had no `CatalogArn`/`LakehouseRegistrationStatus` fields at all,
  even though both are confirmed real `types.Cluster` members
  (`aws-sdk-go-v2/service/redshift@v1.65.4/types/types.go:153,343`) -- this
  backend already modeled the equivalent state for Redshift Serverless
  (`Namespace.CatalogArn`/`LakehouseRegistrationStatus`, wired the same day),
  so the classic version was simply left behind. Added to `Cluster` and
  wired into `xmlCluster`/`toXMLClusterWithTags` so every cluster-returning
  op echoes them, not just this op's own response. The backend method
  (`lakehouse.go`) follows `UpdateLakehouseConfigurationSL`'s existing
  carry-forward-when-omitted pattern almost exactly (same `arn.Build`
  derivation, same "existing value survives a call that only touches one
  field" behavior), with a new cluster-keyed `ClusterLakehouseConfig` store
  table for `LakehouseIdcApplicationArn` (no `Cluster` member on the real
  wire, same as its Serverless counterpart).
- **Two second-layer findings**, from reading the op's full real
  input/output rather than only `ClusterIdentifier`: (1)
  `LakehouseIdcApplicationArn`, when the caller sets a new one, is now
  validated against this backend's own `RedshiftIdcApplication` store
  (`idc_applications.go`) -- real cross-reference validation this backend
  can perform because it already models that resource; a miss returns
  `RedshiftIdcApplicationNotExists`, declared in this op's own error switch.
  The Serverless sibling has no such backend to check against, so it
  doesn't do this -- not a discrepancy, a capability this family happens to
  have. (2) `DryRun` does **not** map to a `DryRunException` here the way
  the Serverless sibling's `UpdateLakehouseConfiguration` does -- confirmed
  absent from `awsAwsquery_deserializeOpErrorModifyLakehouseConfiguration`'s
  declared switch. `DryRun`'s own doc text ("validates the request without
  actually modifying the lakehouse configuration") is honored literally
  instead: a successful dry run runs every validation and returns the
  would-be result as an ordinary 200, without persisting it. Assuming the
  Serverless sibling's DryRunException behavior here would have been wrong.
- Error codes used (`ClusterNotFound`, `RedshiftIdcApplicationNotExists`)
  both come from each op's own declared switch.
  `InvalidClusterState`/`UnsupportedOperation` (both ops) and
  `DependentServiceAccessDenied`/`DependentServiceUnavailableFault`/
  `UnauthorizedOperation` (Lakehouse only) remain declared but unused -- no
  real precondition for any is discoverable from this backend's state, left
  honest rather than inventing triggers, matching this repo's existing
  convention for declared-but-unreachable exceptions (see glue's
  `OperationTimeoutException` reasoning in the sibling service's own
  PARITY.md for the same judgment call).

Tests: table-driven handler tests for both ops (existence check, missing
ClusterIdentifier, IDC-application cross-reference miss) plus dedicated
tests for DryRun (no mutation), persistence/carry-forward across separate
calls, and a real `aws-sdk-go-v2` client round trip
(`TestSDKRoundTrip_ModifyLakehouseConfiguration`) proving
`CatalogArn`/`ClusterIdentifier`/`LakehouseIdcApplicationArn`/
`LakehouseRegistrationStatus` decode correctly and that `DescribeClusters`
decodes the same `Cluster.CatalogArn`/`LakehouseRegistrationStatus` fields
this pass added. All new/changed tests hand-verified to fail against the
pre-fix handlers (temporarily reverted both handler functions to their old
stub bodies, confirmed the expected failures, restored).

Gates run this pass, all green: `go build`, `go vet`, `go test -race`,
`go fix -diff` (no diff), `golangci-lint run` (0 issues).

### 2026-08-13 pass: UpdateSnapshot, GetTrack/ListTracks, UpdateLakehouseConfiguration, GetIdentityCenterAuthToken; reservation family deliberately deferred (bd gopherstack-v4wu)

Follow-up to gopherstack-0w2p/8v8v/mbcq below: `TestSDKCompleteness_Serverless`
found ten operations with zero code (filed as gopherstack-irh7, duplicate of
this issue). This pass implements four of the ten and documents why the
fifth -- the reservation-capacity family -- is a deliberate gap rather than
an oversight. See the `Redshift Serverless` family row's final addendum for
the full account; short version:

- **`UpdateSnapshot`** (the most conspicuous gap: Create/Get/List/Delete
  snapshot all existed, so CRUD symmetry was broken) now completes the
  family. `RetentionPeriod` is optional and nilable on the real
  `UpdateSnapshotInput` (confirmed against `api_op_UpdateSnapshot.go`) --
  omitting it leaves the stored value unchanged, proven by
  `TestServerless_UpdateSnapshot_OmittedRetentionPeriodUnchanged`, and the
  retention-period change itself is proven observable through a second
  `GetSnapshot` call, not just the `UpdateSnapshot` response
  (`TestServerless_SnapshotCRUD`).
- **`GetTrack`/`ListTracks`** return a static two-entry catalog (`current`,
  `trailing`, both at this backend's single `modelVersion10` release) --
  the exact same precedent classic Redshift's own `DescribeClusterTracks`
  already set for the identical real-world enumeration
  (`handler_cluster_info.go`). `UpdateTargets` (the list of newer versions a
  track could update to) is honestly left empty: this backend has one static
  release, so there is no second version to invent an upgrade path to.
- **`UpdateLakehouseConfiguration`** writes `Namespace.CatalogArn`/
  `LakehouseRegistrationStatus` -- both confirmed present on real
  `types.Namespace` (`types/types.go`) but previously entirely absent from
  this backend's `Namespace` struct, a genuine pre-existing wire gap this
  pass also closes, not new fabrication. `LakehouseIdcApplicationArn` has NO
  `Namespace` member at all in the real SDK, so it is kept out of every other
  namespace response and lives in a new namespace-keyed store table
  (`slLakehouseConfig`, `serverless_lakehouse.go`), observable only through
  this operation's own response -- proven to survive a later call that
  changes only the registration status
  (`TestServerless_UpdateLakehouseConfiguration_RegisterAndAssociate`) and
  through a full persistence round trip
  (`TestInMemoryBackend_FullStateRoundTrip`). `DryRun: true` returns the real
  `DryRunException` (confirmed in the pinned `service-2.json`: "the request
  was successful, but dry run was enabled so no action was taken") without
  mutating state -- initially modeled this as a 200 with preview data before
  reading the deserializer's error-set comment, which is exactly the kind of
  mistake this repo's SDK-shape discipline exists to catch.
  `LakehouseRegistrationStatus`'s exact string values (`"Registered"`/
  `"Deregistered"`) are a direct derivation from the client's own
  `LakehouseRegistration` request value, not an invented vocabulary -- real
  AWS documents no enum for this field (a plain `*string`, confirmed in
  `types/types.go`).
- **`GetIdentityCenterAuthToken`** mints a synthetic opaque token after
  validating every named workgroup actually exists in this backend -- an FK
  check classic Redshift's own operation of the identical name
  (`handleGetIdentityCenterAuthToken`, `handler_idc_applications.go`) does
  not even perform. The token-minting approach itself follows that sibling
  operation's own precedent, already judged acceptable by a prior audit
  (`families.Descriptive/static ops` above lists it as spot-checked ok): no
  real IAM Identity Center backend exists here to mint a real token, so a
  synthetic opaque value is the honest ceiling, not a shortcut invented for
  this pass.
- **Reservation-capacity family deliberately NOT built**
  (`CreateReservation`/`GetReservation`/`GetReservationOffering`/
  `ListReservationOfferings`/`ListReservations`), still listed in
  `sdk_completeness_test.go`'s `notImplemented` slice. Judgment call, argued
  both ways: this package already has a directly analogous precedent
  (classic Redshift's `ReservedNode`/`defaultReservedNodeOfferings`,
  `reserved_nodes.go`) -- a curated, fabricated-but-consistent catalog of
  offering IDs/prices, graded `ok` by a prior audit. Built the same way here,
  a `CreateReservation` implementation would be structurally
  straightforward (`PurchaseReservedNodeOffering` is the exact same shape:
  no cluster/namespace reference, just an offering ID and a billing
  commitment). What tips this the other way: `ReservedNode`'s offering
  catalog keys off `NodeType` (`dc2.large`, `ra3.xlplus`, ...), a small, real,
  AWS-published hardware SKU list that constrains what a "curated" catalog
  can honestly contain. `ReservationOffering` has no such anchor -- it is
  `{Capacity RPUs, HourlyCharge, UpfrontCharge, CurrencyCode, OfferingType}`,
  free-floating commercial pricing AWS derives from its own live rate cards
  (confirmed: `ListReservationOfferings`'s own doc comment is "Returns the
  current reservation offerings in your account" -- "current" implying rates
  that move, not a fixed catalog). A gopherstack client cannot tell a
  plausible-looking price from a real one, and unlike a wrong ARN or status
  code (which fails loudly), a wrong dollar figure silently corrupts any
  cost-projection logic built on top of it. This is the invented-capability
  case parity-principles.md warns about, not the same kind of judgment call
  the `ReservedNode` precedent already settled -- and this family additionally
  has zero pre-existing backend state (no `CreateReservation` call has ever
  run here) to hang a reservation's identity on, unlike every other op built
  this pass, each of which extended CRUD symmetry or wire completeness on an
  already-real resource. Recorded here rather than silently reclassified so
  the next audit can revisit the call with both arguments in view.

Gates run this pass, all green: `go build`, `go vet`, `go test -race`,
`go fix -diff` (no diff), `golangci-lint run` (0 issues). New/changed tests
verified to have teeth, not just pass vacuously: temporarily removed the five
new `slDispatchTable` entries (file edit, reverted after) and confirmed every
new handler test flips to `ValidationException`/"unknown operation"; and
separately removed the `slLakehouseConfig` `store_setup.go` registration and
confirmed `TestInMemoryBackend_FullStateRoundTrip` panics with a nil-pointer
dereference (proving the table wiring, not just the test assertions, is
load-bearing) before restoring both files.

### 2026-08-13 pass: Redshift Serverless go.mod pin, phantom UpdateNamespace.DBName, nine request-member gaps (bd gopherstack-0w2p/8v8v/mbcq)

See the `Redshift Serverless` family row above for the full account. Short
version: `aws-sdk-go-v2/service/redshiftserverless` was pinned into `go.mod`
for real this time (v1.38.5, matched to the same upstream release batch as
the already-pinned `redshift`/`redshiftdata`), kept alive against `go mod
tidy` by a new `TestSDKCompleteness_Serverless` test rather than a bare
reference import -- that test doubles as real verification tooling and
immediately found 10 entirely-unimplemented operations (filed as
gopherstack-irh7, not built this pass). Re-verifying the prior audit's
field-level findings against the newly-pinned source changed nothing: the
module cache copy it was read from was already v1.38.5. Fixed:
`UpdateNamespace`'s phantom `dbName` field/mutation (gopherstack-8v8v,
removed); `AdminUserPassword` on Create/UpdateNamespace (credential,
accepted then explicitly discarded, never persisted or echoed);
`RedshiftIdcApplicationArn` on CreateNamespace; `MaintainIntegration` on
RestoreFromSnapshot; `ActivateCaseSensitiveIdentifier` on both table-restore
ops; and five real filter gaps (ListSnapshots EndTime/StartTime/
NamespaceArn/OwnerAccount, ListRecoveryPoints EndTime/StartTime,
ListWorkgroups OwnerAccount, GetSnapshot OwnerAccount, ListUsageLimits
UsageType) -- OwnerAccount implemented as an honest single-account
comparison against `b.accountID`, not a no-op. `ListEndpointAccess`'s VpcId
omission re-confirmed correct, left untouched.

### 2026-08-10 pass: Redshift Serverless EndpointAccess, ListManagedWorkgroups, RestoreFromSnapshot, ConvertRecoveryPointToSnapshot (bd gopherstack-w8g2, final pass)

Eighth/ninth of the nine originally-missing serverless families, plus the two
restore/convert ops left aside earlier -- this closes bd gopherstack-w8g2.
Confirmed against the pinned `botocore` `redshift-serverless/2021-04-21/
service-2.json.gz` (protocol `json` 1.1, botocore 1.43.56) and cross-checked
against `aws-sdk-go-v2/service/redshiftserverless@v1.38.5`'s serializers.go/
deserializers.go/types (pulled into GOMODCACHE via `go get` for this pass,
confirmed `git status --porcelain go.mod go.sum` clean before and after, and
`go mod tidy` a no-op).

**EndpointAccess** was explicitly flagged as needing "the same
no-per-ENI-AZ/IP-data judgement call classic Redshift's EndpointAccess
already made." Re-read `endpoint_access.go`/`handler_endpoint_access.go`
first: classic leaves the entire nested `VpcEndpoint`/network-interface
object absent from every response rather than fabricating a `vpcEndpointId`
with no real ENI allocation behind it (see `families.EndpointAccess`'s
addendum). The serverless `EndpointAccess` shape has the identical problem
in a different nesting: real `types.VpcEndpoint.NetworkInterfaces` needs
`AvailabilityZone`/`PrivateIpAddress`/`NetworkInterfaceId`/`SubnetId` per
ENI (confirmed against `types.NetworkInterface`), and this backend tracks
none of it anywhere (no EC2 cross-reference wired into Redshift at all,
same finding `families.ClusterSubnetGroup` already recorded). Followed the
classic precedent exactly: `vpcEndpoint` is omitted from every response,
not partially invented. What IS modeled: `address`/`endpointArn`/
`endpointCreateTime`/`endpointName`/`endpointStatus`/`port`/`subnetIds`/
`workgroupName`/`vpcSecurityGroups` -- all real, all backed by state this
backend actually holds. `vpcSecurityGroups` reuses classic's own
`VpcSecurityGroupMembership`-equivalent shape and its `"active"` status
convention (`endpointStatusActive`), since it is the identical real type.
`ListEndpointAccessRequest`'s `vpcId` filter is deliberately not accepted,
for the same no-vpcId-data reason. `DeleteEndpointAccessResponse` echoes
the deleted object (confirmed against service-2.json: unlike
`DeleteResourcePolicy`/`DeleteCustomDomainAssociation`'s zero-member
responses, this one carries a real `endpoint` member).

**ListManagedWorkgroups** was called "thin, with no real backing state
without data-sharing consumer modeling." Checked rather than assumed: it
holds. `ListManagedWorkgroupsRequest.sourceArn` is pattern-constrained to a
Glue Data Catalog database/catalog ARN in service-2.json, meaning
`ManagedWorkgroupListItem` represents a workgroup Glue/Lake Formation
auto-provisions for federated queries against shared data -- confirmed by
grepping this package for any Glue Data Catalog or Lake Formation
integration (none; `AssociateDataShareConsumer` is classic Redshift's
unrelated data-sharing feature). Implemented as an honest,
correctly-shaped, always-empty response (`ListManagedWorkgroupsSL`) -- no
`store.Table` added, since nothing in this backend could ever populate one.

**RestoreFromSnapshot** requires `namespaceName`/`workgroupName`, with the
identical "name of the namespace to restore ... to/into" wording and
required-field shape `RestoreFromRecoveryPointRequest` already uses; by
that symmetry it reuses the same already-reviewed design
`RestoreFromRecoveryPointSL` established in the prior pass (both resources
must pre-exist, FK-checked, no real data movement simulated -- the existing
`Namespace` is returned unchanged). `snapshotName`/`snapshotArn` resolve via
the same ARN-suffix-stripping convention `GetServerlessSnapshot` already
uses. `manageAdminPassword`/`adminPasswordSecretKmsKeyId` are threaded onto
the namespace in the true direction only -- the documented false-branch
behavior ("uses the admin credentials the namespace or cluster had at the
time the snapshot was taken") is data this backend cannot reconstruct, so
it is left untouched rather than fabricated.

**ConvertRecoveryPointToSnapshot** requires `recoveryPointId`/
`snapshotName`; writes a new `ServerlessSnapshot` from the recovery point's
own namespace linkage, reusing `CreateServerlessSnapshot`'s exact
conflict-check/arn.Build/store/index/tag sequence.

All four verified to genuinely fail beforehand: their `slDispatchTable`
entries were removed in a file copy (not `git stash`), the new tests
rerun, and every one flipped from real behavior to `"unknown operation"`
`ValidationException`/400 -- confirmed, then the entries were restored and
the diff checked identical to before.

One new `store.Table` registered this pass (`slEndpointAccesses`) plus its
`sortedStringIndex` -- no snapshot version bump, same additive convention
every prior family in this issue used. `ListManagedWorkgroupsSL` needed no
new table at all, since it has no backing state to hold.



Fifth/sixth/seventh of the nine originally-missing serverless families, taken
together deliberately as the "entangled group" the prior two passes flagged
and left for last: `RecoveryPoint`, `TableRestoreStatus`, and the two
restore operations that consume a recovery point
(`RestoreFromRecoveryPoint`, `RestoreTableFromRecoveryPoint`), plus
`RestoreTableFromSnapshot` (grouped with `TableRestoreStatus` since both
table-level restores write into the same result store). Confirmed against
the pinned `botocore` `redshift-serverless/2021-04-21/service-2.json.gz`
(protocol `json` 1.1, botocore 1.43.56) and cross-checked against
`aws-sdk-go-v2/service/redshiftserverless@v1.38.5`'s serializers.go/
deserializers.go already sitting in GOMODCACHE from prior passes (not
re-added to go.mod, confirmed via `git status --porcelain go.mod go.sum`
before and after, and `go mod tidy` producing no diff):

- **Where recovery points come from**: real AWS creates them automatically,
  "every 30 minutes ... kept for 24 hours" (the `RecoveryPoint` shape's own
  documentation in service-2.json) -- there is no `CreateRecoveryPoint`
  operation anywhere in this API's operation list, confirmed by enumerating
  every operation name. No wire-reachable create op was added (that would
  have been exactly the fabricated-API mistake this issue's instructions
  warned against). Instead, `generateRecoveryPointLocked`
  (`serverless_recovery.go`) creates exactly one recovery point per
  namespace+workgroup pair at `CreateWorkgroup` time, inline under the same
  lock `CreateWorkgroup` already holds (same pattern `DeleteNamespace`
  already uses for its final-snapshot write) -- a workgroup is required to
  exist before recovery points make sense at all (`RecoveryPoint.workgroupName`
  is a real field), and this backend has no real 30-minute scheduler
  infrastructure to run, consistent with its existing instant-apply
  simplifications elsewhere (snapshots created instantaneously, resizes
  applied synchronously). `AddRecoveryPointInternal` is a test-only seed
  helper (not wired to any op), matching the existing
  `AddSnapshotInternal`/`AddReservedNodeInternal` convention in this package.
- `GetRecoveryPointRequest` keys on `recoveryPointId`; `ListRecoveryPointsRequest`
  optionally filters by `namespaceArn`/`namespaceName` (both accepted; this
  backend does not filter by the request's `startTime`/`endTime` window --
  see items_still_open) and paginates via the existing shared
  `maxResults`/`nextToken` convention. Both response envelopes
  (`{"recoveryPoint": ...}` / `{"recoveryPoints": [...]}`) confirmed against
  `awsAwsjson11_deserializeOpDocumentGetRecoveryPointOutput`/
  `...ListRecoveryPointsOutput`.
- **Timestamp formats genuinely differ within this one entangled group**:
  `RecoveryPoint.recoveryPointCreateTime` is shape `SyntheticTimestamp_date_time`
  (ISO8601 string; confirmed both in service-2.json and via
  `awsAwsjson11_deserializeDocumentRecoveryPoint`'s `smithytime.ParseDateTime`
  call in deserializers.go), while `TableRestoreStatus.requestTime` is the
  bare `Timestamp` shape (epoch-seconds JSON number; confirmed via
  `awsAwsjson11_deserializeDocumentTableRestoreStatus`'s
  `smithytime.ParseEpochSeconds` call) -- re-verified per field rather than
  assumed from the nearer-looking sibling, same discipline the
  Tagging/CustomDomainAssociation pass applied to
  `customDomainCertificateExpiryTime` vs `GetCredentials`' `expiration`.
- `RestoreFromRecoveryPointRequest` requires `namespaceName`/`recoveryPointId`/
  `workgroupName` (all three required, confirmed in service-2.json);
  response is `{"namespace": {...}, "recoveryPointId": "..."}` (confirmed via
  `awsAwsjson11_deserializeOpDocumentRestoreFromRecoveryPointOutput`). This
  backend does not simulate real data movement (consistent with
  `CreateSnapshotCopyConfiguration`'s cross-region copy also not simulating
  real replication), so once FK checks pass -- namespace exists, workgroup
  exists, workgroup belongs to that namespace (this backend's existing
  Namespace-Workgroup invariant, not a fabricated recovery-point-specific
  rule), recovery point exists -- the existing `Namespace` is returned
  unchanged. `RestoreFromSnapshot` (namespace-level restore from a
  `Snapshot`, no recovery point involved at all) was deliberately **not**
  built this pass: it does not depend on `RecoveryPoint` and was excluded
  from this entangled group by design, matching the original issue's
  own framing ("split per family ... they are independent") -- still open,
  see items_still_open.
- `RestoreTableFromSnapshotRequest`/`RestoreTableFromRecoveryPointRequest`
  both require `namespaceName`/`newTableName`/`sourceDatabaseName`/
  `sourceTableName`/`workgroupName` plus their respective
  `snapshotName`/`recoveryPointId` (confirmed in service-2.json); both
  responses envelope under `tableRestoreStatus`
  (`awsAwsjson11_deserializeOpDocumentRestoreTableFrom{Snapshot,RecoveryPoint}Output`).
  Modeled as `ServerlessTableRestoreStatus` (`serverless.go`), distinct from
  classic Redshift's cluster-keyed `TableRestoreStatus` in `models.go` (same
  concept, different protocol, different resource keys -- disambiguated the
  same way `ServerlessResourcePolicy` was kept distinct from classic
  Redshift's `ResourcePolicy`). `Status` is set to `SUCCEEDED` immediately
  (this backend restores synchronously, consistent with the rest of this
  service) rather than left `IN_PROGRESS` forever the way classic Redshift's
  own `TableRestoreStatus` currently is (a pre-existing quirk in
  `table_restore.go`, out of this pass's scope, not touched).
  `ProgressInMegaBytes`/`TotalDataInMegaBytes` are honestly left at
  zero/omitted (`omitempty`) rather than fabricated -- this backend has no
  real data to move, same reasoning classic Redshift's own
  `items_still_open` entry already gives for the identical fields.
  `GetTableRestoreStatusRequest` keys on `tableRestoreRequestId`;
  `ListTableRestoreStatusRequest` optionally filters by
  `namespaceName`/`workgroupName`.

Proven with a persistence round trip extending
`TestInMemoryBackend_FullStateRoundTrip`: the `rt-workgroup` seed already in
that test auto-generates a recovery point (no new seed call needed for
that part), and a `RestoreTableFromSnapshotSL` call was added to seed a
`ServerlessTableRestoreStatus`. Verified the round trip has teeth two ways,
both reverted immediately after confirming the intended failure: (1)
temporarily renamed the two new tables' `store.Register` name strings so
the two failure modes could not be conflated, which -- as expected, since
Snapshot and Restore ran within the same process against the same renamed
keys -- did NOT reproduce data loss (a useful negative result, not just a
placeholder check); (2) temporarily commented out the two new
`rebuildFromKeys` calls in `rebuildServerlessIndexes`, which DID make
`TestInMemoryBackend_FullStateRoundTrip` fail with `"[]" should have 1
item(s), but has 0` on the new `ListRecoveryPointsSL`/`ListTableRestoreStatusSL`
assertions -- proving those two lines are load-bearing, not decorative.
Also verified the two new handler test files
(`handler_serverless_recovery_test.go`,
`handler_serverless_table_restore_test.go`) fail against the pre-pass
codebase: copied them into a `git worktree add --detach HEAD` checkout (not
`git stash`) with none of this pass's backend/handler files present, and
every new test failed there -- `unknown operation` `ValidationException`
(400) where the current tree returns 200/`ResourceNotFoundException`, since
none of these operations existed in `slDispatchTable` before this pass.

Two new `store.Table`s (`slRecoveryPoints`, `slTableRestoreStatuses`) plus
two new `sortedStringIndex`es (`slRecoveryPointIdx`,
`slTableRestoreStatusIdx`) registered/wired the same way every prior
serverless table is -- no snapshot version bump, `Registry.Tables` stays an
additive `map[string]json.RawMessage`.

Deliberately not modeled: `ListRecoveryPointsRequest`'s `startTime`/`endTime`
filter window (accepted nowhere -- not parsed at all, rather than parsed and
silently ignored, since this backend's single auto-generated recovery point
per workgroup has no meaningful creation-time range to filter against
without inventing one); `ConvertRecoveryPointToSnapshot` (a real op that
converts a recovery point into a `Snapshot` -- not part of this issue's
named restore-op list and not required for `RestoreFromRecoveryPoint`/
`RestoreTableFrom{Snapshot,RecoveryPoint}` to work, left for a future pass);
`RestoreFromSnapshot` (namespace-level restore from a `Snapshot`, no
recovery point dependency -- deliberately excluded from this entangled
group, see above); `TooManyTagsException`/`ServiceQuotaExceededException`
preconditions on the restore ops, consistent with this service's existing
unsimulated-quota precedent. EndpointAccess and ListManagedWorkgroups
remain unbuilt -- see items_still_open.

### 2026-08-10 pass: Redshift Serverless ResourcePolicy + SnapshotCopyConfiguration (bd gopherstack-w8g2)

Third and fourth of the nine originally-missing serverless families (two of
nine already done in the 2026-08-09 pass below); picked as the most
self-contained of the remaining seven -- neither depends on any other unbuilt
family (RecoveryPoint, TableRestoreStatus and the restore ops are entangled
with each other; EndpointAccess additionally needs the same
no-per-ENI-AZ/IP-data judgment call `families.EndpointAccess` already made for
classic Redshift; ListManagedWorkgroups has no real backing state in this
backend without data-sharing consumer modeling). Confirmed against the pinned
`botocore` `redshift-serverless/2021-04-21/service-2.json.gz` (protocol `json`
1.1) via `python3 -c "import gzip, json; ..."` against the installed botocore
1.43.56 package, cross-checked against
`aws-sdk-go-v2/service/redshiftserverless@v1.38.5`'s serializers.go/
deserializers.go already sitting in GOMODCACHE from the prior passes (not
re-added to go.mod):

- `GetResourcePolicyRequest`/`PutResourcePolicyRequest`/
  `DeleteResourcePolicyRequest` all key on `resourceArn`; `PutResourcePolicyRequest`
  additionally requires `policy` (a JSON string, opaque to this backend).
  `ResourcePolicy` (the response shape) is just `{policy, resourceArn}` --
  confirmed via `awsAwsjson11_deserializeDocumentResourcePolicy` in
  deserializers.go and the `ResourcePolicy` shape in service-2.json.
  `GetResourcePolicy`/`PutResourcePolicy` both envelope the object under
  `resourcePolicy` (NOT flat -- the CustomDomainAssociation flat-response
  oddity does not generalize to every serverless family, confirmed by reading
  `awsAwsjson11_deserializeOpDocumentGetResourcePolicyOutput` directly rather
  than assuming either convention). `DeleteResourcePolicyResponse` has zero
  members, same convention as `DeleteCustomDomainAssociationResponse`.
  Real errors are `GetResourcePolicy`/`DeleteResourcePolicy`:
  `ResourceNotFoundException` on a missing policy; `PutResourcePolicy` also
  lists `ConflictException`/`ServiceQuotaExceededException` in its error set,
  but the precondition that trips them is undocumented and not simulated
  (`PutResourcePolicy` is a create-or-replace upsert here, same as the real
  API's stated behavior).
- `SnapshotCopyConfiguration` mirrors `CreateSnapshotCopyConfigurationInput`'s
  required `namespaceName`/`destinationRegion` plus optional
  `destinationKmsKeyId`/`snapshotRetentionPeriod`, and an
  id/arn pair generated the same way every other serverless resource's is
  (`randomHex`/`arn.Build`). `UpdateSnapshotCopyConfigurationInput` only
  mutates `snapshotRetentionPeriod` (confirmed against
  `awsAwsjson11_serializeOpDocumentUpdateSnapshotCopyConfigurationInput`).
  `DeleteSnapshotCopyConfigurationResponse` DOES echo the deleted object under
  `snapshotCopyConfiguration` (`"required": ["snapshotCopyConfiguration"]` in
  service-2.json) -- the opposite convention from
  `DeleteResourcePolicy`/`DeleteCustomDomainAssociation`, worth flagging since
  it would be easy to assume Delete responses are uniformly empty across this
  service. `ListSnapshotCopyConfigurationsResponse` envelopes under the
  plural `snapshotCopyConfigurations`, with `namespaceName` as an optional
  server-side filter and `maxResults`/`nextToken` on the existing shared
  pagination convention.

Both proven with a persistence round trip in
`TestInMemoryBackend_FullStateRoundTrip`: verified the round trip actually
catches data loss (not just green-by-construction) by temporarily
de-registering both new `store.Table`s from `store_setup.go` in a scratch copy
and confirming the test fails with `no resource policy for ...`, then
restoring the real registration. Two new `store.Table`s (`slResourcePolicies`,
`slSnapshotCopyConfig`) plus one new `sortedStringIndex`
(`slSnapshotCopyConfigIdx`) registered/wired the same way every prior
serverless table is -- no snapshot version bump.

Deliberately not modeled: a one-configuration-per-namespace constraint for
`CreateSnapshotCopyConfiguration` -- service-2.json documents no such rule for
this family (unlike classic Redshift's `EnableSnapshotCopy`, which this same
backend does gate one-per-cluster), so none was invented.
`TooManyTagsException`/`ServiceQuotaExceededException`/`ConflictException`
preconditions remain unsimulated, consistent with this service's existing
`TooManyTagsException` precedent. EndpointAccess, RecoveryPoint,
TableRestoreStatus, ListManagedWorkgroups, and the restore-from-snapshot/
recovery-point ops remain unbuilt -- see items_still_open.

### 2026-08-09 pass: Redshift Serverless Tagging + CustomDomainAssociation (bd gopherstack-w8g2)

Split out of bd gopherstack-hsfm's "nine resource families do not exist" list
per that issue's own instruction to pick two at a time; these two were named
because Tagging is what creation-time `Tags` on the other Create ops defer to,
and CustomDomainAssociation is what `GetCredentials.CustomDomainName` depends
on. Built both, full account in the `Redshift Serverless` family row's
addendum above. Confirmed via the pinned `botocore`
`redshift-serverless/2021-04-21/service-2.json.gz` model (not
`aws-sdk-go-v2/service/redshiftserverless`, which stayed out of `go.mod` per
this issue's constraint -- fetched into `/tmp` only, diffed, discarded):

- `TagResourceRequest`/`ListTagsForResourceRequest`'s `tags` member is a
  `TagList` of `{key, value}` structs, not a JSON map -- a different shape
  than this package's classic-Redshift XML `Tags>Tag` convention.
- Only `CreateNamespaceRequest`/`CreateWorkgroupRequest`/`CreateSnapshotRequest`
  have a `tags` member; `CreateUsageLimitRequest`/`CreateScheduledActionRequest`
  do not, and neither `UpdateNamespaceRequest` nor `UpdateWorkgroupRequest` do.
- `Namespace`/`Workgroup`/`Snapshot` (the response shapes) have NO `tags`
  field at all -- confirmed by listing every member of each shape. Tags are
  therefore write-only through Create*/TagResource and read-only through
  ListTagsForResource; there is no "echo tags on the resource itself" wire
  gap to fix here, unlike the classic-Redshift `Cluster.Tags` bug this same
  file's 2026-07-22 pass found. Implemented as a new resourceArn-keyed
  `store.Table[slResourceTagSet]` (`serverless_tags.go`), not attached to any
  existing resource struct.
- `Create/Get/UpdateCustomDomainAssociationResponse` serialize the
  association's fields directly at the top level -- NOT wrapped in an
  envelope key the way every other serverless resource response is (e.g.
  `{"namespace": {...}}`). `DeleteCustomDomainAssociationResponse` has zero
  members. Both confirmed by reading the shapes directly, not assumed from
  the sibling ops' pattern.
- `Association.customDomainCertificateExpiryTime` uses shape
  `SyntheticTimestamp_date_time` (`timestampFormat: iso8601`), while
  `GetCredentialsResponse.expiration`/`nextRefreshTime` use the bare
  `Timestamp` shape (epoch-seconds JSON number, no explicit format) --
  genuinely different wire formats for two timestamp fields in the same
  service, both re-confirmed by reading the shape definitions rather than
  copying the nearer-looking convention.
- Real `Workgroup` carries `customDomainName`/`customDomainCertificateArn`/
  `customDomainCertificateExpiryTime` directly (added to the `Workgroup`
  struct), so `CreateCustomDomainAssociationSL`/`UpdateCustomDomainAssociationSL`/
  `DeleteCustomDomainAssociationSL` mirror the association onto its workgroup,
  not just into the separate association store.
- `GetCredentialsRequest.customDomainName` is a real, documented alternative
  to `workgroupName` ("The custom domain name or the workgroup name must be
  included in the request") -- `handleGetCredentials` now resolves it via the
  new association store before falling through to the existing
  workgroup-keyed credential logic.

Proven with a round trip at both layers: `TestServerless_TagResource_RoundTrip`
(HTTP-level Create-with-tags -> ListTagsForResource) and
`TestInMemoryBackend_FullStateRoundTrip` (Snapshot/Restore preserves both the
new tag store and a custom domain association). Two new `store.Table`s
(`slResourceTags`, `slCustomDomainsSL`) registered the same way every other
table in this package is -- no snapshot version bump, since `Registry.Tables`
is an additive `map[string]json.RawMessage` and `RestoreAll` already resets
any table absent from an older snapshot to empty.

Deliberately not modeled: `TooManyTagsException` (no tag-count cap enforced),
`AccessDeniedException` on the custom-domain ops (no IAM simulation in this
backend, consistent with the rest of this service). EndpointAccess,
ResourcePolicy, RecoveryPoint, SnapshotCopyConfiguration, TableRestoreStatus,
ListManagedWorkgroups, and the restore-from-snapshot/recovery-point ops were
not attempted this pass -- each still needs its own follow-up issue per
gopherstack-hsfm's original sizing note.

### 2026-08-08 pass: Redshift Serverless audit + wire-shape fixes (bd gopherstack-hsfm)

See the `Redshift Serverless` family row above for the full account. Short
version: the entire 25-op surface used invented REST routing that no real
`aws-sdk-go-v2` client could ever reach (real protocol is awsJson1.1: POST "/"
with an `X-Amz-Target` header, all fields in the body) -- fixed by switching to
the same `X-Amz-Target` dispatch pattern `redshiftdata.Handler` already uses.
Also fixed while rewriting: `ScheduledActionResponse`'s wire key is `state`,
not `status` (which doesn't exist on that type); `StartTime`/`EndTime` and
`GetCredentialsOutput.Expiration`/`NextRefreshTime` are epoch-seconds numbers,
not RFC3339 strings; `Schedule`/`TargetAction` are tagged-union JSON objects,
not flat strings; `CreateScheduledActionInput.RoleArn` (a required real field)
was entirely unmodeled. Several accepted-then-dropped fields threaded through
(`Namespace.DefaultIamRoleArn`, `DeleteNamespace`'s final-snapshot params,
`Workgroup`'s advanced-config fields, `List*`'s `MaxResults` which was
hardcoded to 0 on every call). SDK obtained via `go get
github.com/aws/aws-sdk-go-v2/service/redshiftserverless@v1.38.5` to field-diff
serializers.go/deserializers.go directly; `go mod tidy` dropped it again
afterward since nothing in the fix imports the SDK package at runtime (this
repo hand-rolls wire structs everywhere), so no persistent new go.mod
dependency was added. Whole resource families this backend has zero code for
(EndpointAccess, CustomDomainAssociation, ResourcePolicy, RecoveryPoint,
SnapshotCopyConfiguration, TableRestoreStatus, Tagging, ListManagedWorkgroups,
restore-from-snapshot) remain unbuilt -- each needs its own follow-up issue,
not a shared one, given the size this family already proved to be.

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
not started this pass. Picked up and audited in the 2026-08-08 gopherstack-hsfm
pass above -- see that section and the `Redshift Serverless` family row for what
was found and fixed.

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
- Descriptive/static ops family: RE-AUDITED 2026-08-13 (gopherstack-3jqz) --
  the "spot-checked, no-stub, real derivation confirmed" claim previously
  here was false for two ops (see families.Descriptive/static ops for the
  full account): `ModifyAquaConfiguration` and `ModifyLakehouseConfiguration`
  both ignore their required `ClusterIdentifier` entirely, with no
  existence/state validation and (for ModifyLakehouseConfiguration) no
  modeled response state at all. Not fixed this pass (out of scope) --
  genuinely open, not reclassified. `ListRecommendations`/
  `GetIdentityCenterAuthToken` re-confirmed real. The remaining
  Describe*/static-catalog ops are still not exhaustively field-diffed
  element-by-element (filters/pagination params like Marker/MaxRecords/
  ClusterVersion/NodeType are accepted-and-ignored on several of them, not
  yet audited for severity).
- Redshift Serverless (`handler_serverless.go`): separate JSON-protocol API
  surface (`redshift-serverless` service ID). AUDITED 2026-08-08 (bd
  gopherstack-hsfm, see the family row and Notes section above) -- routing and
  several field-level wire bugs fixed. Tagging and CustomDomainAssociation
  FIXED 2026-08-09, ResourcePolicy and SnapshotCopyConfiguration FIXED
  2026-08-10 (bd gopherstack-w8g2, see the family row's addenda) --
  `GetCredentials.CustomDomainName` lookup is no longer open. RecoveryPoint,
  TableRestoreStatus, `RestoreFromRecoveryPoint`, `RestoreTableFromSnapshot`
  and `RestoreTableFromRecoveryPoint` FIXED 2026-08-10 (bd gopherstack-w8g2,
  the entangled group -- see the family row's addendum). Still open within
  the audited ops: `Namespace.AdminUserPassword`/`RedshiftIdcApplicationArn`,
  `Snapshot.backup-progress-and-size` fields/cross-account restore-access
  lists, `ScheduledActionResponse.NextInvocations` (needs schedule.go's cron
  evaluator adapted to serverless's unwrapped cron string format),
  `ListRecoveryPointsRequest`'s `startTime`/`endTime` filter window (not
  parsed at all) -- each independently verified as either legitimately
  un-derivable by this backend or deferred to an excluded resource family
  (see the family row for per-field reasoning). EndpointAccess,
  ListManagedWorkgroups, `RestoreFromSnapshot` and
  `ConvertRecoveryPointToSnapshot` FIXED 2026-08-10 (bd gopherstack-w8g2,
  final pass -- see the family row's addendum). This closes gopherstack-w8g2:
  all nine originally-missing serverless families now have real code.
  `EndpointAccess.VpcEndpoint` (nested `vpcEndpointId`/`vpcId`/
  `networkInterfaces`) remains unmodeled -- the same no-per-ENI-AZ/IP-data
  judgment call `families.EndpointAccess` already made for classic Redshift,
  confirmed to apply identically here (see the family row's addendum).
  `UpdateSnapshot`, `GetTrack`/`ListTracks`, `UpdateLakehouseConfiguration`
  and `GetIdentityCenterAuthToken` FIXED 2026-08-13 (bd gopherstack-v4wu, see
  the family row's addendum below) -- the reservation-capacity family
  (`CreateReservation`/`GetReservation`/`GetReservationOffering`/
  `ListReservationOfferings`/`ListReservations`) is DELIBERATELY DEFERRED,
  not fixed: `ReservationOffering` carries AWS-set commercial pricing
  (`HourlyCharge`/`UpfrontCharge`/`CurrencyCode`) with no fixed, SDK-enumerable
  catalog to model honestly against (unlike classic Redshift's own
  `ReservedNode`, whose offerings key off a small, real, AWS-documented
  node-type catalog -- see `families.ReservedNode` above), and this family has
  zero pre-existing backend state (no `CreateReservation` call has ever run
  against this backend) to hang a reservation's identity on. Still tracked in
  `sdk_completeness_test.go`'s `notImplemented` slice.
