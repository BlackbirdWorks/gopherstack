---
service: opensearch
sdk_module: aws-sdk-go-v2/service/opensearch@v1.75.4
sibling_sdk_modules: [aws-sdk-go-v2/service/opensearchserverless@v1.34.4]  # AOSS ops this Handler also implements (serverlessOperations()); see families.serverless
last_audit_commit: acb2e23f9  # gopherstack-uult (2026-08-13) fixed after this hash was recorded; hash not yet known at edit time
last_audit_date: 2026-08-14  # gopherstack-7185: response shapes of Create/Delete/Modify ops
                              # swept. 1 bug found and fixed (DeleteIndex response envelope --
                              # see the `indices` family and items_still_open notes).
# ERROR path verified 2026-08-29 (wrapper-key-sweep pass): audited every op's
# deserializeOpError<Op> switch (opensearch@v1.75.4 deserializers.go, 96 ops
# extracted N-of-N) against this Handler's writeError call sites. 7 bugs found
# and fixed: ListMigrations, AddDataSource, AddDirectQueryDataSource, AddTags,
# RemoveTags each emitted a code their own op does not model (fixed to the
# ValidationException each op actually models); CreateApplication emitted
# ResourceAlreadyExistsException (unmodeled) instead of ConflictException
# (modeled); GetUpgradeHistory/GetUpgradeStatus silently swallowed a
# ResourceNotFoundException-shaped backend error and returned a fabricated
# 200 success instead (missing-error class) -- both now propagate the real
# error. See error_sentinel_fixes_test.go (real-SDK errors.As assertions,
# each confirmed failing pre-fix). handler_applications_test.go/
# handler_data_sources_test.go/handler_tags_test.go had 4 pre-existing tests
# asserting the old wrong codes as correct; corrected alongside the fix.
overall: A            # RAISED from A- (parity-5, this pass). The two gaps that previously held the grade
                      # down -- AttachDataSource's workspaceConfiguration/workspaceId, and StartMigration's
                      # MigrationOptions.Workspace/ExportOptions/ConflictResolution -- are now built to the
                      # full extent aws-sdk-go-v2/service/opensearch@v1.75.0 actually defines. Correction to
                      # the prior pass's audit note: it described MigrationOptions.Workspace as "optional" --
                      # it is not. types.MigrationOptions.Workspace carries "This member is required" in the
                      # SDK doc comment, and this backend was not enforcing that at all before this pass (a
                      # real, independently fixable bug, not just a modeling gap). A new Workspace type
                      # (models.go) tracks the target-workspace side effect of both ops as a real,
                      # store-backed resource (workspaces.go): WorkspaceConfigurationInput's required
                      # Name/WorkspaceType and documented "mutually exclusive with workspaceId" contract are
                      # now enforced on AttachDataSource; MigrationWorkspace's required-when-omitted check,
                      # its "specify either WorkspaceId or createWorkspace" contract, and
                      # ConflictResolution's exhaustive CREATE_NEW_COPIES/overwrite enum are now enforced on
                      # StartMigration; a WorkspaceId reference on either op is validated for both existence
                      # and correct application scoping instead of accepted as any string. Cascade-deleted on
                      # DeleteApplication and round-trips through Snapshot/Restore (verified in
                      # TestInMemoryBackend_SnapshotRestore_FullState). What this deliberately stops short
                      # of: a full CRUD resource model. Confirmed by grepping every api_op_*.go in the SDK
                      # for "Workspace" -- there is no CreateWorkspace/GetWorkspace/ListWorkspaces/
                      # DeleteWorkspace operation anywhere, and no output struct in the entire service
                      # (not AttachDataSourceOutput, not DescribeDataSourceAttachmentOutput, not
                      # GetMigrationOutput/MigrationSummary) ever echoes a WorkspaceId back to the caller.
                      # This is genuinely, structurally true of the real AWS API, not a backend shortcoming
                      # -- a workspace created via either op is write-only from a real client's perspective
                      # too. ExportOptions/ConflictResolution are validated then intentionally discarded
                      # (never persisted), matching the same "parsed but not stored" precedent
                      # services/appconfig's StartExperimentRun DeploymentParameters already established,
                      # since GetMigrationOutput/MigrationSummary never echo them back either.
                      # CORRECTION 2026-08-30: the "ListDataSourceAttachments/ListMigrations still
                      # ignore maxResults/nextToken" gap this note used to point to is stale on both
                      # halves -- ListMigrations was already fixed by the 2026-08-30
                      # unstable-pagination-order sweep on this same branch (see the migrations family
                      # note below), which never updated this earlier note; ListDataSourceAttachments
                      # is now fixed too (gopherstack-6nr4-adjacent pass, see that family note below).
                      # This is exactly the "PARITY manifests bury fix status" class (gopherstack-anjf):
                      # a newer dated section sorted below this stale one. No open gap remains here.
ops:
  CreateDomain: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed DomainId (required field, was missing) and IdentityCenterOptions wire key (see Notes). FIXED gopherstack-5wj0: SoftwareUpdateOptions was read/written under the wrong wire key EnableSoftwareUpdateOptions (confirmed against serializers.go:1319-1321 and deserializers.go:21789-21790, aws-sdk-go-v2/service/opensearch@v1.75.4 -- both directions use object.Key(\"SoftwareUpdateOptions\")), so a real client's request value was silently discarded and any response value the backend did set was unparseable by a real SDK client's typed struct"}
  DescribeDomain: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeDomains: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDomain: {wire: ok, errors: ok, state: ok, persist: ok, note: "this pass added cascade-cleanup of inbound/outbound connections owned by the domain (see cross_cluster_connections)"}
  ListDomainNames: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed wire key EngineVersion->EngineType and value shape (full version string -> engine family); engineType filter param/logic was already correct"}
  UpdateDomainConfig: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed IdentityCenterOptions wire key; added DryRun=true support (previously always mutated even when DryRun requested). FIXED gopherstack-5wj0: same EnableSoftwareUpdateOptions/SoftwareUpdateOptions wire-key bug as CreateDomain (see above), since both share domainJSON for request decoding. NOT fixed (see gaps): EngineMode (a real, distinct optional UpdateDomainConfigRequest field) is entirely absent -- no established backend concept to hook it into."}
  DescribeDomainConfig: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed IdentityCenterOptions wire key. FIXED gopherstack-5wj0: same EnableSoftwareUpdateOptions/SoftwareUpdateOptions wire-key bug as CreateDomain (domainConfigFields shares the response shape)"}
  ListTags: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /2021-01-01/tags?arn=; not-found ARN returns empty TagList (no ResourceNotFoundException in SDK op docs) -- verified intentional, not a bug"}
  AddTags: {wire: ok, errors: ok, state: ok, persist: ok}
  RemoveTags: {wire: ok, errors: ok, state: ok, persist: ok}
  CancelDomainConfigChange: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass: DryRun=true was mutating state (clearing LastChangeID) exactly like the earlier UpdateDomainConfig DryRun bug; path/request/response shape (POST domain/{name}/config/cancel, CancelledChangeIds/CancelledChangeProperties/DryRun) already matched the SDK and is now field-diff-verified"}
  StartServiceSoftwareUpdate: {wire: ok, errors: ok, state: ok, persist: ok}
  CancelServiceSoftwareUpdate: {wire: ok, errors: ok, state: ok, persist: ok}
  RollbackServiceSoftwareUpdate: {wire: ok, errors: ok, state: ok, persist: ok, note: "new op this pass (SDK bump to v1.75.0). Operates on the same Domain.ServiceSoftware state Start/CancelServiceSoftwareUpdate already track rather than an invented parallel history: rolling back a PENDING_UPDATE install cancels it (identical transition to Cancel) and reports RollbackAvailable=true; with no update ever performed, or none currently pending, RollbackAvailable is honestly reported false rather than erroring, matching the response shape's documented purpose. Response field-diffed against types.RollbackServiceSoftwareOptions (CurrentVersion/NewVersion/Description/RollbackAvailable, PascalCase wire keys). Notable finding: the live AWS API reference documents ResourceNotFoundException at HTTP 409 for this op -- NOT the classic domain-family 404 CancelServiceSoftwareUpdate uses -- confirmed against the doc page directly (not derivable from the Go SDK's generated code, which carries no HTTP status metadata for exceptions) and cross-checked against 3 other newly-added ops (AttachDataSource, RegisterCapability, StartMigration) which document the identical 409 convention. Implemented as documented; flagged here since it's a real, non-obvious behavioral split within one service."}
families:
  cross_cluster_connections:
    status: ok
    note: >
      Field-diffed against types.InboundConnection/OutboundConnection/DomainInformationContainer
      this pass. Real bugs found and fixed: (1) DescribeInboundConnections/Accept/Reject/Delete
      emitted lowercase-keyed flat JSON (connectionId/status as a bare string) instead of the real
      nested shape (ConnectionId, ConnectionMode, ConnectionStatus{StatusCode,Message},
      LocalDomainInfo/RemoteDomainInfo{AWSDomainInformation{...}}); (2) RejectInboundConnection and
      DeleteInboundConnection on an unknown ID silently fabricated a fake 200 success instead of
      404 ResourceNotFoundException; (3) CreateOutboundConnection went straight to ACTIVE instead
      of PENDING_ACCEPTANCE and never produced a corresponding InboundConnection, so
      Accept/Reject/DescribeInboundConnections were unreachable outside test seeding -- added
      mirroring (shared ConnectionId, swapped Local/RemoteDomainInfo) plus required-field
      validation and ConnectionMode/ConnectionProperties(SkipUnavailable/Endpoint) support;
      (4) DeleteDomain did not cascade-clean owned connections -- fixed.
  vpc_endpoints:
    status: ok
    note: >
      Field-diffed against types.VpcEndpoint/VpcEndpointSummary/VPCDerivedInfo/VpcEndpointErrorCode
      this pass. Fixed: (1) DescribeVpcEndpoints error code was the invented "EndpointNotFound"
      string, not the real enum value "ENDPOINT_NOT_FOUND"; (2) DeleteVpcEndpoint's
      VpcEndpointSummary was missing the required DomainArn/VpcEndpointOwner fields;
      (3) CreateVpcEndpoint/UpdateVpcEndpoint echoed the request-shape VpcOptions
      (SecurityGroupIds/SubnetIds) verbatim instead of the response-shape VPCDerivedInfo, which
      also carries server-derived AvailabilityZones/VPCId -- added synthesized derivation (see
      Notes, "reasonable non-stub default" like DryRunResults.DeploymentType); (4) Create/Update
      accepted a nil/empty DomainArn or VpcOptions with no validation. (5, gopherstack-uult,
      2026-08-13) ListVpcEndpoints and ListVpcEndpointsForDomain both marshaled the raw
      []*VpcEndpoint domain slice (leaking Endpoint, VpcOptions, and the internal-only
      StatusUntil clock field) instead of types.VpcEndpointSummary's four members
      (DomainArn/Status/VpcEndpointId/VpcEndpointOwner); fixed with a shared
      toVpcEndpointSummary scoped converter, mirroring the pattern DeleteVpcEndpoint's
      VpcEndpointSummary response already used correctly. (6, gopherstack-r80d, required OUTPUT
      member sweep) ListVpcEndpoints, ListVpcEndpointsForDomain, and ListVpcEndpointAccess all
      omitted NextToken, a required member of all three Output structs
      (api_op_ListVpcEndpoints.go, api_op_ListVpcEndpointsForDomain.go,
      api_op_ListVpcEndpointAccess.go) -- a real client's required *string NextToken always decoded
      nil. This backend is single-page for all three, so the correct value is always an empty
      string rather than omitted; fixed by adding jsonKeyNextToken: "" to each response. Proven via
      TestVpcEndpointListOps_NextTokenPresent_RealClient (wire_output_required_r80d_test.go), which
      fails against the unfixed decode for all three ops. (7, gopherstack-rz6y, 2026-08-29) The (5)
      fix above only covered the List paths (they route through toVpcEndpointSummary); the same
      StatusUntil leak was still reachable through CreateVpcEndpoint/UpdateVpcEndpoint/
      DescribeVpcEndpoints, which marshal the raw *VpcEndpoint struct directly and so still emitted
      "statusUntil" whenever an endpoint carried a non-zero value (only possible via
      DeleteVpcEndpoint with SetProcessingDelay > 0, still-visible during its DELETING window).
      types.VpcEndpoint (opensearch@v1.75.4 types/types.go:3442) has no such member. Fixed by
      changing StatusUntil's tag to json:"-" on VpcEndpoint (the same audit found
      InboundConnection/OutboundConnection/Capability's StatusUntil fields never actually reach the
      wire -- all three already go through dedicated converter functions that omit it, so no
      change was needed there). Proven via TestVpcEndpoint_RawBody_NoLeakedStatusUntil
      (wire_field_fixes_test.go), which fails against the unfixed tag.
  packages:
    status: ok
    note: >
      Field-diffed against types.PackageDetails/DomainPackageDetails/PackageStatus/
      DomainPackageStatus this pass. Fixed real invented-enum-value bugs: (1) Package.PackageStatus
      was set to "ACTIVE" on create, but PackageStatus has no ACTIVE value at all -- only AVAILABLE
      (ACTIVE belongs to the *different* DomainPackageStatus enum); (2) DissociatePackage(s) set
      State to the invented "DISSOCIATED", which does not exist in DomainPackageStatus (real values
      are ASSOCIATING/ASSOCIATION_FAILED/ACTIVE/DISSOCIATING/DISSOCIATION_FAILED) -- fixed to
      DISSOCIATING, mirroring the DELETING-on-instant-removal pattern used elsewhere; (3)
      ListPackagesForDomain (GET domain/{name}/packages) and ListDomainsForPackage (GET
      packages/{id}/domains) returned the wrong wire shape entirely -- raw Package objects / bare
      domain-name strings instead of DomainPackageDetailsList -- fixed to emit
      PackageID/DomainName/DomainPackageStatus/PackageName/PackageType per element.
      gopherstack-m53b (required-member sweep pass 4): UpdatePackageScope decoded its required
      PackageUserList under the JSON tag "PackageScopeOperationConfig", which does not exist on
      the real wire (api_op_UpdatePackageScope.go:29-48 confirms the top-level member is literally
      PackageUserList) -- every real client's list was silently dropped and the field always
      decoded to nil. The response also fabricated a "PackageScopeOperationStatus" key not present
      on UpdatePackageScopeOutput, and never echoed PackageUserList back at all even though the
      real Output requires it. Fixed the wire key, added a real PackageUserList field to Package
      (json:"-", internal-only -- no other Package/PackageDetails response carries package scope),
      and implemented actual ADD/REMOVE/OVERRIDE semantics in UpdatePackageScope (previously a
      pure no-op that just re-read the package). Proven via Test_SDKRoundTrip_UpdatePackageScope
      (handler_update_package_scope_test.go), which fails against the unfixed decode.
  indices:
    status: ok
    note: >
      gopherstack-m53b (required-member sweep pass 4): CreateIndex/UpdateIndex read top-level
      Mappings/Settings/Aliases fields from the request body, but the real
      CreateIndexInput/UpdateIndexInput (api_op_CreateIndex.go:37-60, api_op_UpdateIndex.go:32-52,
      opensearch@v1.75.4) carry a single IndexSchema member typed document.Interface -- a smithy
      document (arbitrary JSON value, no fixed schema on the wire). A real client's entire payload
      was silently dropped and the index was created/updated with no schema at all. Also found via
      full-shape read: the real response is `{"Status": "CREATED"|"UPDATED"}` (types.IndexStatus,
      the op's only output member) -- the handler's prior response reused a Get/Delete-shaped
      envelope (IndexName/IndexStatus/Mappings/Settings/Aliases/DocumentCount) that never carried
      the wire-required "Status" key at all, so a real client's *CreateIndexOutput.Status/
      *UpdateIndexOutput.Status stayed the empty string even once the request body was fixed.
      Fixed by adding DomainIndex.IndexSchema (any, stored/echoed verbatim -- not parsed into
      Mappings/Settings/Aliases, since the smithy document has no fixed internal shape to parse
      against) and a dedicated {"Status": ...} response for these two ops specifically, matching
      the real Output exactly. Proven via Test_SDKRoundTrip_CreateIndex_IndexSchema/
      Test_SDKRoundTrip_UpdateIndex_IndexSchema (handler_indices_test.go), which fail against the
      unfixed decode.

      gopherstack-r80d (required OUTPUT member sweep): GetIndex's response shape, left unverified
      by the pass above, was checked and found wrong -- GetIndexOutput's only member is IndexSchema
      (api_op_GetIndex.go: "This member is required."), but the handler still returned the
      Get/Delete-shaped IndexName/IndexStatus/Mappings/Settings/Aliases/DocumentCount envelope, so
      a real client's required *GetIndexOutput.IndexSchema always decoded nil. Fixed by returning
      {"IndexSchema": ...} instead: the raw stored document when the index carries one (created via
      the real CreateIndex/UpdateIndex path), or one synthesized from Mappings/Settings/Aliases for
      indices created via the classic path. DeleteIndex's Status field was re-checked and confirmed
      already correct (fixed in an earlier pass, see the handler's own comment). Proven via
      TestGetIndex_IndexSchema_RealClient (wire_output_required_r80d_test.go), which fails against
      the unfixed decode; TestHTTPDocumentCRUDAndSearch's GetIndex assertion (previously checking
      the invented DocumentCount field, which isn't part of the real response) was updated to match.
  applications:
    status: ok
    note: >
      Field-diffed against types.Application*/GetDefaultApplicationSetting(Input/Output)/
      PutDefaultApplicationSetting(Input/Output) this pass. GetDefaultApplicationSettings/
      PutDefaultApplicationSettings were a wholesale gopherstack invention -- wrong URL
      (/application/settings/default instead of the real top-level
      /2021-01-01/opensearch/defaultApplicationSetting), wrong shape (ApplicationType +
      DefaultApplicationSettings[] key/value list; the real API has neither field -- it's a single
      lowercase applicationArn string plus setAsDefault bool). Deleted the invented
      AppSetting/defaultAppSettings machinery and replaced with the real single-ARN
      GetDefaultApplicationSetting/PutDefaultApplicationSetting. Also added the CreatedAt/
      LastUpdatedAt/Endpoint fields GetApplication/ListApplications/UpdateApplication require
      that were previously missing entirely, and removed a Status field UpdateApplicationOutput
      does not have on the real API. CreateApplicationInput's legacy lowercase
      iamIdentityCenterOptions shape (confirmed different from Domain's IdentityCenterOptions,
      per prior pass's note) was left untouched -- still correct.
  reserved_instances:
    status: ok
    note: >
      Field-diffed against types.ReservedInstance/ReservedInstanceOffering/
      ReservedInstancePaymentOption this pass. Fixed: (1) ReservedInstance.State was set to
      "ACTIVE" (uppercase, matching this API's usual enum convention) but the real field is
      documented freeform lowercase-hyphenated (payment-pending/active/payment-failed/retired) --
      fixed to "active"; (2) DescribeReservedInstanceOfferings/DescribeReservedInstances ignored
      the real offeringId/reservationId query-string filters entirely (confirmed via
      serializers.go SetQuery("offeringId")/SetQuery("reservationId")) -- added filtering.
      InstanceType/PaymentOption/CurrencyCode enum values were already correct.
  scheduled_actions:
    status: ok
    note: >
      Field-diffed against types.ScheduledAction and the real URL paths (confirmed via
      serializers.go) this pass. The entire routing was wrong: gopherstack served
      GET/PUT /2021-01-01/opensearch/scheduledActions(/update) as top-level, DomainName-in-body
      endpoints; the real API is domain-scoped --
      GET /domain/{DomainName}/scheduledActions and PUT /domain/{DomainName}/scheduledAction/update
      (singular "scheduledAction", DomainName from the URL path). The request body was also
      entirely invented: gopherstack accepted a full ScheduledAction object (Id/Type/Severity/
      Description/ScheduledBy/Status/ScheduledTime/Mandatory/Cancellable) letting callers
      fabricate arbitrary state; the real UpdateScheduledActionInput is
      {ActionID, ActionType, ScheduleAt, DesiredStartTime} and can only *reschedule* an action
      that already exists (real AWS creates scheduled actions automatically ahead of
      service-software updates / JVM tuning; there is no create-via-update backdoor). Rewrote to
      the real routes/shape; UpdateScheduledAction now 404s on an unknown ActionID+ActionType
      pair instead of silently creating one. Added AddScheduledActionInternal (export_test.go) for
      test seeding, matching the SeedInboundConnection/AddPackageInternal pattern used elsewhere
      for AWS-auto-created resources.
  data_sources_direct_query:
    status: ok
    note: >
      Field-diffed against types.DataSource(Type)/DataSourceDetails/DirectQueryDataSource(Type)/
      GetDataSourceOutput this pass. Found and fixed a serious wire-shape bug: types.DataSourceType
      and types.DirectQueryDataSourceType are tagged unions on the wire (e.g.
      {"S3GlueDataCatalog":{"RoleArn":"..."}} / {"CloudWatchLog":{}}), not plain enum strings.
      gopherstack stored the decoded union as a Go string via json.Marshal, then re-marshaled that
      *string* through the response struct's own `string` field -- producing a JSON string
      containing escaped JSON (`"DataSourceType":"{\"S3GlueDataCatalog\":{}}"`) instead of a
      nested object, which a real AWS SDK client cannot deserialize into the union type. Fixed by
      switching DataSourceType to json.RawMessage end-to-end (model, backend signatures,
      persistence DTO) so it round-trips as a genuine nested object. Also fixed: (1)
      GetDataSource wrapped its response in an invented "DataSource" envelope and used lowercase
      field keys -- real GetDataSourceOutput's fields (DataSourceType/Description/Name/Status) are
      top-level; (2) GetDirectQueryDataSource/List used lowercase keys and the internal field name
      "Name" instead of the real "DataSourceName" (domain-level DataSource genuinely uses "Name" --
      confirmed these are different field names on different resources, not a typo); (3)
      UpdateDataSource was routed as an invented POST domain/{name}/updateDataSource
      (Name-in-body) instead of the real PUT domain/{name}/dataSource/{Name}, and never accepted
      the required DataSourceType or optional Status fields; (4) UpdateDirectQueryDataSource never
      accepted DataSourceType, which real AWS requires on every update call; (5) DataSource had no
      Status field at all (real DataSourceStatus: ACTIVE/DISABLED) -- added, defaults to ACTIVE.
      NOT fixed (gopherstack-5wj0): UpdateDirectQueryDataSource also accepts no
      DataSourceAccessPolicy field (a real, optional PolicyDocument-shaped
      UpdateDirectQueryDataSourceRequest member) -- DirectQueryDataSource has no reserved field to
      store it in, so wiring it through would mean modeling a new resource attribute end to end,
      out of this pass's scope.
  serverless:
    status: deferred
    note: >
      UPDATE (2026-08-23, manifest-harvest pass): started the field-level audit. One real bug
      found and fixed (DeleteCollection not-found mapped to 500 InternalServerException instead
      of the real 404 ResourceNotFoundException -- serverlessErrorTable was missing the
      ErrDomainNotFound entry). DeletionProtection confirmed as a genuine unimplemented-behavior
      gap (request field silently dropped, so a "protected" collection can always be deleted) but
      left deferred -- fixing it right also needs UpdateCollection, which this Handler doesn't
      implement at all. Re-counted the un-advertised-op gap at 25 real ops (not ~15): the
      CollectionGroup/LifecyclePolicy/VpcEndpoint families, the Index family, Tag*Resource,
      Account Settings, and GetPoliciesStats. See the dated section at the end of this file for
      full detail. Field-level wire/state audit still out of scope this pass -- see
      items_still_open. UPDATE
      (2026-07-31, reverse sdkcheck sweep, gopherstack-vhw2): the "structurally blocked, not in
      go.mod" reasoning below is now stale for the operation-naming half of this problem --
      aws-sdk-go-v2/service/opensearchserverless was added to go.mod this sweep (needed for the
      sdk_completeness_test.go multi-client check, see below) and pkgs/sdkcheck's reverse check
      was run against it directly. Confirmed 14 of serverlessOperations()'s 22 entries are real
      opensearchserverless.Client ops (BatchGetCollection, CreateCollection, DeleteCollection,
      ListCollections; Create/Delete/Get/List/UpdateAccessPolicy;
      Create/Delete/Get/List/UpdateSecurityConfig) that the reverse check only flagged because it
      was comparing against opensearchsdk.Client instead of the client that owns them --
      sdk_completeness_test.go now checks this family against opensearchserverlesssdk.Client
      separately. The other 8 (CreateEncryptionPolicy/CreateNetworkPolicy/
      DeleteEncryptionPolicy/DeleteNetworkPolicy/GetEncryptionPolicy/ListEncryptionPolicies/
      ListNetworkPolicies/UpdateEncryptionPolicy) were genuinely fabricated -- confirmed absent
      from opensearchserverless.Client at every released SDK version back to v1.0.0. Real AWS
      discriminates encryption vs. network security policies with a "type" request field on a
      single CreateSecurityPolicy/DeleteSecurityPolicy/GetSecurityPolicy/ListSecurityPolicies/
      UpdateSecurityPolicy operation family, not by operation name. Renamed
      serverlessOperations() to the 5 real names (net -3, 22 -> 19 ops); the underlying route
      handlers (handleServerlessEncryptionPolicyRoutes/handleServerlessNetworkPolicyRoutes) are
      untouched -- this was purely an operation-naming/advertising fix, not a wire-shape fix. Full
      field-diff of Collection/AccessPolicy/SecurityConfig/SecurityPolicy request/response shapes
      against opensearchserverless's real Input/Output types is still not done -- that's the
      "needs its own audit pass" work below, now unblocked (module present) but not attempted this
      sweep.
  data_source_attachments:
    status: ok
    note: >
      New family this pass (AttachDataSource/DetachDataSource/DescribeDataSourceAttachment/
      ListDataSourceAttachments, SDK bump to v1.75.0). Field-diffed against
      types.DataSourceAttachmentSummary and the live AWS API reference (AttachDataSource's response
      shape is not fully derivable from the Go SDK alone -- see Notes). Wired into real, existing
      state rather than a parallel universe: dataSourceArn must resolve to an actual tracked domain
      (via the existing domainsByARN index) or serverless collection (via slCollections) --
      resolveDataSourceRefLocked, shared with the migrations family below -- and is rejected with
      ResourceNotFoundException otherwise. Status is not a static echo: PENDING/ATTACHED reflects
      the referenced resource's real, already-modeled processing state (domainProcessing /
      resolveCollectionStatus), and a PENDING attachment lazily settles to FAILED after the
      documented 24-hour window if the resource never becomes active (resolveAttachmentStatus,
      lifecycle_test.go covers all three transitions with a controlled clock). Idempotent re-attach
      (same application+dataSourceArn returns the existing attachment) matches the op's documented
      behavior. DeleteApplication now cascades attachment cleanup, matching the DeleteDomain cascade
      precedent from a prior pass.
      FIXED THIS PASS: AttachDataSourceInput's optional workspaceConfiguration/workspaceId are now
      modeled against a real Workspace resource (models.go/workspaces.go). workspaceConfiguration
      creates a new Workspace linked to the application (Name/WorkspaceType both validated
      required, WorkspaceType checked against its documented closed enum
      OBSERVABILITY/SECURITY_ANALYTICS/SEARCH); workspaceId validates an existing Workspace scoped
      to the same application; the two are rejected as mutually exclusive when both are supplied,
      matching the SDK's own doc comment. See the "overall" grade note above for why this stops at
      validate-and-track rather than a full CRUD resource (the SDK defines no
      Get/List/DeleteWorkspace operation and no output ever echoes a WorkspaceId, so nothing more
      is derivable from the real API). Cascade-deleted on DeleteApplication.
      FIXED 2026-08-30: ListDataSourceAttachments previously ignored maxResults/nextToken entirely
      (not query-bound on this op -- confirmed against its own
      awsRestjson1_serializeOpDocumentListDataSourceAttachmentsInput, opensearch@v1.75.4
      serializers.go: both are real JSON body members, "maxResults"/"nextToken", unlike
      ListMigrations' HTTP-query binding for the same concept -- each op's own serializer settles
      it, not a shared family convention). Now paginated via pkgs/page (default page size 50, per
      ListDataSourceAttachmentsInput.MaxResults' documented default); b.dataSourceAttachmentsByApp
      is a pkgs/store.Index, whose Get() is insertion-ordered and stable across calls, so no
      additional sort was needed before paginating it. Also found and fixed alongside it: the
      backend never validated the application existed at all (silently returned an empty list for
      an unknown application ID instead of the ResourceNotFoundException every sibling op in this
      family already returns -- AttachDataSource/DetachDataSource/DescribeDataSourceAttachment all
      check b.applications.Has first). Proven via
      handler_data_source_attachments_pagination_test.go's TestListDataSourceAttachments_SDKPagination
      (real aws-sdk-go-v2 client, 5 attachments, MaxResults=2, asserts the union of every page
      equals the seeded set) and TestListDataSourceAttachments_UnknownApplication; both confirmed
      failing against pre-fix code.
  capabilities:
    status: ok
    note: >
      New family this pass (RegisterCapability/DeregisterCapability/GetCapability). Field-diffed
      against types.CapabilityBaseRequestConfig/CapabilityBaseResponseConfig/
      CapabilityExtendedResponseConfig/CapabilityStatus/CapabilityFailure. The only union member
      any of these currently define is types.AIConfig, an empty struct with zero fields (confirmed
      via the SDK's own serializeDocumentAIConfig/deserializeDocumentAIConfig, both no-ops) -- so
      "capabilityConfig" on the wire is always exactly {"aiConfig":{}}, and there is genuinely
      nothing beyond applicationId/capabilityName/status to validate or persist. Wired into real
      state: ApplicationId must reference an existing application (b.applications) or the call is
      rejected. Status transitions creating->active using the same processingDelay-driven
      lazy-settle pattern already used for serverless collections (resolveCapabilityStatus mirrors
      resolveCollectionStatus); DeregisterCapabilityOutput's status is the fixed string "deleting"
      per its own doc ("Returns deleting when the capability is being removed"), not a fabricated
      transient window. DeleteApplication cascades capability cleanup.
  insights:
    status: ok
    note: >
      New family this pass (ListInsights/DescribeInsightDetails/InsightFeedback). Field-diffed
      against types.Insight/InsightEntity/InsightField/InsightFeedbackEntity and the enum families
      (InsightEntityType/InsightPriorityLevel/InsightStatus/InsightType/InsightFeedbackThumbs/
      InsightResponseStatus). This emulator has no analytics engine to genuinely produce insights,
      so per the task's own instruction ("an empty list is honest and a fabricated one is not"):
      ListInsights always returns an empty Insights array (never a fabricated recommendation), and
      DescribeInsightDetails/InsightFeedback always report ResourceNotFoundException for any
      InsightId, since no insight can genuinely exist to describe or give feedback on. This is not
      a stub, though -- Entity validation is real and wired into existing state
      (ValidateInsightEntity): a DomainName entity must reference a domain that actually exists in
      this backend (the same check every other domain-scoped op performs), an Account entity is
      accepted for any non-empty value, and any other Type is rejected as ValidationException.
  migrations:
    status: ok
    note: >
      New family this pass (StartMigration/GetMigration/ListMigrations). Field-diffed against
      types.MigrationOptions/MigrationSource/MigrationSummary/MigrationError/MigrationWorkspace/
      ExportOptions. Wired into real, existing state: migrationOptions.source.datasourceArn is
      validated against the same resolveDataSourceRefLocked check the data source attachment family
      uses (must be a real, tracked domain or serverless collection ARN), and applicationId must
      reference an existing application. Status genuinely transitions PENDING -> IN_PROGRESS ->
      SUCCEEDED against the backend's clock (resolveMigrationStatus, reusing the same
      processingDelay knob every other transient-window resource in this backend already uses --
      with no configured delay it settles straight to its terminal state, matching the
      "historical fast behaviour" documented on SetProcessingDelay; lifecycle_test.go exercises the
      full window with a controlled clock). ExportedCount/ImportedCount are always 0 rather than a
      fabricated non-zero number: this emulator has no saved-object store (dashboards/
      visualizations/index-patterns/searches) to actually migrate, so it honestly "succeeds" at
      migrating zero real objects instead of inventing migrated content a client would treat as
      real. DeleteApplication cascades migration-job cleanup.
      FIXED THIS PASS: MigrationOptions.Workspace is now enforced as the required field the SDK
      documents it to be (previously not enforced at all -- a real bug, not just an unmodeled
      field): omitted entirely, it is rejected; CreateWorkspace=true requires Name and creates a
      real Workspace linked to the application; WorkspaceId validates an existing Workspace scoped
      to the same application; specifying both is rejected as mutually exclusive, matching
      "Specify either this parameter or createWorkspace". ConflictResolution is validated against
      its documented exhaustive enum (CREATE_NEW_COPIES, overwrite -- the inconsistent casing is
      the SDK doc text itself, not a transcription error). ExportOptions.Objects elements are
      validated for the SDK's required Id/Type per element. ExportOptions/ConflictResolution
      remain parsed-then-discarded (never persisted) rather than stored: this emulator still has
      no saved-object store to actually apply them against, and neither GetMigrationOutput nor
      MigrationSummary carry a field to echo them back through even if it did -- same "accepted,
      validated, not stored" precedent services/appconfig's StartExperimentRun
      DeploymentParameters already established. See the "overall" grade note above for why the
      Workspace side of this stops at validate-and-track rather than full CRUD.
      CORRECTION 2026-08-30: this note previously said ListMigrations still ignored
      maxResults/nextToken. That was already stale when read -- the 2026-08-30
      unstable-pagination-order sweep on this same branch fixed it (paginated via pkgs/page,
      reading b.migrationsByApp -- a pkgs/store.Index, insertion-ordered and stable -- so no sort
      was needed) but never updated this earlier note. No open gap remains here; see that sweep's
      dated section below for the fix detail.
gaps: []
deferred:
  - serverless
leaks: {status: clean, note: "no goroutines/janitors in this service; coarse lockmetrics.RWMutex per backend, no per-map locks introduced. This pass's DeleteDomain connection-cascade iterates Table.All() (a fresh snapshot slice per the existing convention) while deleting, same safe pattern as the pre-existing package/index/data-source cascades. New this pass: DeleteApplication now cascades data source attachments, capabilities, and migration jobs using the identical clone-then-delete pattern (Table.All()/Index.Get results are fresh/cloned slices, safe to range over while deleting)."}
---

## Notes

### Required OUTPUT member sweep (2026-08-14, gopherstack-r80d)

Extracted every field marked `This member is required.` at the top level of
an `<Op>Output` struct across all 96 `opensearch@v1.75.4` operations (parsed
from the pinned SDK's `api_op_*.go` files), yielding 21 required output
members across 17 ops -- the same extraction tool used and validated for the
route53 pass of this same sweep (known-answer match against kinesis's
`DescribeLimits`, negative match against `ListShards`).

Every one of the 17 ops was read end-to-end to confirm each required field
is actually written. Found and fixed **4** silently-unset required output
members:

- `GetIndex`: wrong response shape entirely -- returned the Get/Delete
  metadata envelope instead of the real `{"IndexSchema": ...}` shape, so the
  required `IndexSchema` field always decoded nil. This corrects an
  incorrect claim in an earlier pass's note (see items_still_open) that
  GetIndex's envelope was already right.
- `ListVpcEndpoints`, `ListVpcEndpointsForDomain`, `ListVpcEndpointAccess`:
  all omitted the required `NextToken`, echoed here as an empty string since
  this backend is single-page for all three.

`DescribeInsightDetails`'s required `Fields` member is genuinely unsettable
without fabrication, not a bug: this backend has no analytics engine, so
every call to that op already errors (`ResourceNotFoundException`) rather
than returning a success response — a deliberate, already-disclosed no-stub
design (see the `insights` family note above), so there is no success path
where a zero-valued `Fields` could leak to a caller.

The remaining 15 required output fields across the other 13 ops (all
`AuthorizeVpcEndpointAccess`, `CreateIndex`/`DeleteIndex`/`UpdateIndex`,
`CreateVpcEndpoint`/`UpdateVpcEndpoint`/`DeleteVpcEndpoint`,
`DescribeDomain`/`DescribeDomainConfig`/`DescribeDomains`,
`DescribeVpcEndpoints`, `UpdateDomainConfig`) were confirmed correctly
populated by reading each handler's response-construction code.
**opensearch is settled for this bug class**: every required output member
across every op that has one has been read and checked.

**Re-verified 2026-08-28** (gopherstack-r80d, independent re-check after the
issue's closure reason was found undocumented): re-ran
`go run ./cmd/requiredoutputfields` (still 21 fields/17 ops, unchanged since
2026-08-14) and re-read all 17 handlers plus the nested `DomainStatus`
struct's own 4 required members (`ARN`/`ClusterConfig`/`DomainId`/
`DomainName`, opensearch@v1.75.4 types/types.go:1377-1401) against
`toDomainStatusJSON` -- all still correctly populated from real backend
state. `AuthorizedPrincipal`/`VpcEndpointSummary`/`VpcEndpointError`/
`DomainConfig` (the other nested response types the 17 ops wrap) carry zero
required members of their own in the pinned SDK, confirmed by direct read,
not inferred. 0 new findings; go build/vet/test -race/golangci-lint all
clean on this service. No regression since the 2026-08-14 pass.

### Reverse sdkcheck sweep (2026-07-31) -- 8 fabricated serverless policy op names found and renamed

`pkgs/sdkcheck`'s reverse check (gopherstack-vhw2) flagged 22 `serverlessOperations()`
entries as "phantom" (not exported methods on `opensearchsdk.Client`). Added
`aws-sdk-go-v2/service/opensearchserverless` to go.mod and verified each by name:

- **14 real, sibling-owned**: `BatchGetCollection`, `CreateCollection`, `DeleteCollection`,
  `ListCollections`, `Create/Delete/Get/List/UpdateAccessPolicy`,
  `Create/Delete/Get/List/UpdateSecurityConfig` -- all real `opensearchserverless.Client`
  ops, flagged only because the reverse check compared them against `opensearchsdk.Client`
  (the classic managed-domain client) instead of the AOSS client that owns them.
  `sdk_completeness_test.go` now splits `GetSupportedOperations()` and checks the AOSS half
  against `opensearchserverlesssdk.Client` directly.
- **8 fabricated -- never real at any SDK version, renamed**: `CreateEncryptionPolicy`,
  `CreateNetworkPolicy`, `DeleteEncryptionPolicy`, `DeleteNetworkPolicy`,
  `GetEncryptionPolicy`, `ListEncryptionPolicies`, `ListNetworkPolicies`,
  `UpdateEncryptionPolicy`. Confirmed absent from `opensearchserverless.Client` at v1.0.0
  through v1.34.2 -- AOSS has always discriminated encryption vs. network security policies
  with a `type` request field on one `CreateSecurityPolicy`/`DeleteSecurityPolicy`/
  `GetSecurityPolicy`/`ListSecurityPolicies`/`UpdateSecurityPolicy` operation family, never
  by separate operation names. Renamed `serverlessOperations()`'s entries to the 5 real
  names (net -3, 22 -> 19 ops; `TestHandlerOpsLen`/`TestOpenSearchHandler_GetSupportedOperations`
  updated from 118 to 115 total ops). This was purely an operation-name/advertising fix: the
  HTTP route handlers underneath (`handleServerlessEncryptionPolicyRoutes`/
  `handleServerlessNetworkPolicyRoutes`, `/encryptionpolicies`/`/networksecuritypolicies`)
  are untouched, since `ExtractOperation` doesn't route serverless paths at all currently
  (chaos fault injection isn't wired up for this family either way) -- see
  families.serverless for what's still not field-diffed.
- No test was found asserting the 8 fabricated names as data (only test *function names*
  like `TestServerless_CreateEncryptionPolicy`, which exercise the unchanged HTTP routes, not
  the operation-name list).

Gates run scoped to `services/cloudfront`, `services/eventbridge`, `services/iot`,
`services/opensearch`, `services/personalize` (four other services triaged in the same
sdkcheck sweep): `go build ./...`, `go vet`, `go test -race -count=1`, `gofmt -l`,
`golangci-lint run` all clean; `git diff --stat go.mod go.sum` non-empty by design -- see
eventbridge's PARITY.md for the shared go.mod/go.sum justification (four new sibling SDK
deps added across the five services in this sweep, one shared `aws-sdk-go-v2` core patch bump).

**IdentityCenterOptions wire-key bug (real, fixed):** the SDK (as of v1.59.0)
renamed the CreateDomainInput/UpdateDomainConfigInput/DomainStatus/DomainConfig
field from the deprecated `IamIdentityCenterOptions` (nested fields
`IamIdentityCenterArn`, `IamRoleForIdentityCenterApplicationArn`) to
`IdentityCenterOptions` (nested fields `IdentityCenterInstanceARN`, `RolesKey`,
`SubjectKey`, output-only `IdentityCenterApplicationARN`/`IdentityStoreId`).
gopherstack still spoke the deprecated shape for Domain create/update/describe,
so any current aws-sdk-go-v2 client setting this option would silently no-op
(gopherstack would never see the field, and would never emit it back). Fixed
by renaming the backend/wire types and JSON tags to match the current SDK.
**Trap for next auditor:** `CreateApplicationInput` (a *different* resource,
the OpenSearch UI "Application") genuinely still uses the deprecated lowercase
`iamIdentityCenterOptions` key per the SDK -- do not "fix" that one to match
Domain's `IdentityCenterOptions`, they are legitimately different shapes for
different resources. Re-confirmed this pass while reworking the applications
family.

**DomainId (required field, was missing):** `types.DomainStatus.DomainId` is
marked "This member is required" in the SDK but gopherstack's
`domainStatusJSON` never populated it. Real AWS format is
`"{accountId}/{domainName}"`. Added `Domain.DomainID`, computed at
`CreateDomain` time, threaded through `DescribeDomain(s)`/`DeleteDomain`/
`UpdateDomainConfig` (all return a copy of the stored `Domain`).

**ListDomainNames wire-key bug (real, fixed):** `types.DomainInfo` (the
`DomainNames[]` element) carries the coarse engine family under wire key
`EngineType` with value `"OpenSearch"` or `"Elasticsearch"` -- NOT the full
version string (`"OpenSearch_2.11"`) under `EngineVersion` that
`DescribeDomain`'s `DomainStatus` returns. gopherstack was emitting
`EngineVersion: "OpenSearch_2.11"` for ListDomainNames entries, which real SDK
clients would silently drop (unknown field) leaving `EngineType` permanently
empty. Fixed to emit `EngineType` derived from the stored `EngineVersion` via
the existing `isOpenSearchEngine` helper (the `engineType` *query-filter*
logic was already correct -- only the response shape was wrong).

**UpdateDomainConfig DryRun (real, fixed):** `UpdateDomainConfigInput.DryRun`
was accepted by no code path -- the domainJSON request struct didn't even
parse it, so every UpdateDomainConfig call mutated the domain regardless of
DryRun. Added `PreviewDomainConfig` (same field-merge logic as
`UpdateDomainConfig`, applied to a copy, under `RLock`, never persisted) and
wired `DryRun: true` to call it instead, returning `DryRunResults` alongside
`DomainConfig`. `DryRunResults.DeploymentType` is AWS-internal-computed
(Blue/Green vs DynamicUpdate vs Undetermined vs None) with no public
algorithm; gopherstack always reports `"DynamicUpdate"` with a generic
message -- a reasonable non-stub default (the important behavioral fix is
non-mutation), but a future pass could refine deployment-type heuristics if a
consumer depends on it.

**CancelDomainConfigChange DryRun (real, fixed this pass):** the exact same
bug class as UpdateDomainConfig's DryRun above, on a different op --
`CancelDomainConfigChange` unconditionally cleared `Domain.LastChangeID`
regardless of the `DryRun` flag, so a dry-run cancel call silently cancelled
the pending change for real. Fixed to only clear `LastChangeID` when
`dryRun == false`; the reported `CancelledChangeIds` list is unaffected
either way (real AWS reports what *would be* cancelled on a dry run too).

**ListTags on unknown ARN → empty TagList, not 404 (verified correct, not a
bug):** the SDK's `ListTagsInput`/`ListTagsOutput` docs list no
`ResourceNotFoundException`; only `BaseException`/`ValidationException`/
`InternalException` are documented for the op family. gopherstack's
`handleListTags` returning an empty `TagList` for an ARN with no matching
domain matches this -- do not "fix" this to 404 without new evidence.

**Snapshot version bumped 1→2** (`persistence.go`) because the `Domain`
struct's JSON shape changed (`iamIdentityCenterOptions` →
`identityCenterOptions`, added `domainID`). Old snapshots are cleanly
discarded on restore (existing version-mismatch handling), not partially
misdecoded.

**Snapshot version bumped 2→3 this pass** because several "clean" registered
tables' value types changed shape: `InboundConnection`/`OutboundConnection`
(added ConnectionMode/StatusMessage/structured Local·RemoteDomainInfo, see
cross_cluster_connections above), `VpcEndpoint.VpcOptions` (now
server-enriched with AvailabilityZones/VPCId), and the "dirty" DTO
`dataSourceSnapshot`/live `DataSource`/`DirectQueryDataSource`
(`DataSourceType` string → `json.RawMessage`). Also removed the
`defaultAppSettings` snapshot field (invented mechanism, deleted) and added
`defaultApplicationArn`. Old snapshots are cleanly discarded on restore
(existing version-mismatch handling), not partially misdecoded.

**Protocol:** restjson1 throughout (confirmed via serializers.go /
`awsRestjson1_*` generated code for every op path referenced this pass,
including all newly-verified families).

**SDK bumped v1.59.0 -> v1.75.0 this pass** (gopherstack-o2j's parity-4
campaign), adding 14 new operations: `AttachDataSource`,
`DeregisterCapability`, `DescribeDataSourceAttachment`,
`DescribeInsightDetails`, `DetachDataSource`, `GetCapability`,
`GetMigration`, `InsightFeedback`, `ListDataSourceAttachments`,
`ListInsights`, `ListMigrations`, `RegisterCapability`,
`RollbackServiceSoftwareUpdate`, `StartMigration`. All 14 are now genuinely
implemented (routing, backend state, request parsing, field-diffed response
wire shapes, error codes, Snapshot/Restore) -- see the `data_source_attachments`
/ `capabilities` / `insights` / `migrations` families and the
`RollbackServiceSoftwareUpdate` op entry above. None were added to a
`notImplemented`/stub list.

**"arn" field on AttachDataSource/DetachDataSource/DescribeDataSourceAttachment
outputs mirrors dataSourceArn (real, confirmed via live docs, not derivable
from the Go SDK alone):** the live AWS API reference documents `arn` and
`dataSourceArn` with the byte-identical description ("The Amazon Resource
Name (ARN) of the domain") on all three of these ops -- almost certainly a
doc-generation artifact reusing templated text, but since it is the *only*
textual evidence available (the field has no other documented meaning, and
there is no separate "domain ARN" concept in AttachDataSourceInput to
distinguish it from `dataSourceArn`), `toDataSourceAttachmentJSON` sets `Arn`
to the same value as `DataSourceArn` rather than inventing a distinct
semantic. Flagged here explicitly so a future auditor with better evidence
(e.g. a captured real response) can correct this without re-deriving the
reasoning.

**ResourceNotFoundException documented at HTTP 409, not 404, for the entire
new "OpenSearch application" op family (real, confirmed against the live AWS
API reference, not the Go SDK):** unlike every classic domain-scoped
ResourceNotFoundException in this service (404, e.g. DescribeDomain,
CancelServiceSoftwareUpdate), the live docs for AttachDataSource,
RegisterCapability, StartMigration, and RollbackServiceSoftwareUpdate all
document ResourceNotFoundException at HTTP 409. This is not visible in the Go
SDK's generated code at all (client-side deserializers dispatch on the
server's response status code and don't encode a canonical one), so this
required fetching the actual API reference pages rather than field-diffing
generated Go source. Applied uniformly to all 14 new ops' ResourceNotFoundException
mappings (writeAttachmentError/writeCapabilityError/writeMigrationError/
writeInsightError/handleRollbackServiceSoftwareUpdate) on the assumption the
whole newer op family shares the convention; only 4 of the 14 were directly
confirmed against the live docs (fetching all 14 individually was out of
this pass's time budget) -- worth spot-checking the remaining 10
(DetachDataSource, DescribeDataSourceAttachment, ListDataSourceAttachments,
DeregisterCapability, GetCapability, ListInsights, DescribeInsightDetails,
InsightFeedback, GetMigration, ListMigrations) against live docs in a future
pass if a consumer depends on the exact status code.

**AIConfig is a genuinely empty struct (real, confirmed via SDK source):**
`types.AIConfig` has zero fields, and its own serializer/deserializer
(`serializeDocumentAIConfig`/`deserializeDocumentAIConfig`) read/write
nothing -- confirmed directly in the installed SDK source, not inferred. This
is why the `capabilities` family has no `CapabilityConfig` state to persist
beyond the capability's existence/name/status.

## items_still_open (this pass)

- **serverless** (OpenSearch Serverless collections/policies): partially
  audited 2026-08-23 (manifest-harvest pass, see the dated section at the end
  of this file) -- one real bug fixed (DeleteCollection not-found -> 500
  instead of 404), DeletionProtection confirmed as a real but correctly
  deferred gap (needs UpdateCollection too), and the missing-op count
  corrected to 25 (not ~15). Full field-level wire/state diff of
  Collection/AccessPolicy/SecurityConfig/SecurityPolicy is still not done.
  Still needs a dedicated audit pass.
- **Un-re-verified ops outside the assigned scope/deferred list**: GetCompatibleVersions,
  ListVersions, DescribeDomainAutoTunes, DescribeDomainChangeProgress,
  DescribeDomainHealth, DescribeDomainNodes, DescribeDryRunProgress,
  DescribeInstanceTypeLimits, GetDomainMaintenanceStatus, GetUpgradeHistory,
  GetUpgradeStatus, ListDomainMaintenances, ListInstanceTypeDetails,
  StartDomainMaintenance, and the index/document data-plane ops
  (CreateIndex/DeleteIndex/GetIndex/UpdateIndex) were not touched or
  field-diffed this pass (they were not in the original 1-gap/8-deferred list
  this pass was scoped to fix). Not reclassified either direction; still
  whatever state the prior pass left them in. UPDATE (gopherstack-l5ir,
  see the "Route reachability sweep" note above): route-level (method+path)
  correctness for every op in this list is now verified and permanently
  tested -- five of them (GetDomainMaintenanceStatus, GetUpgradeHistory,
  GetUpgradeStatus, ListDomainMaintenances, StartDomainMaintenance,
  ListInstanceTypeDetails, CreateIndex) were actually unreachable/misrouted
  and are now fixed. Field-level wire-shape diffing of these ops' request/
  response bodies is still outstanding -- route correctness is not the same
  claim as wire-shape completeness. UPDATE (gopherstack-m53b): CreateIndex
  and UpdateIndex are now field-diffed and fixed -- see the `indices` family
  above. GetIndex/DeleteIndex still reuse that same fix's response envelope
  but their own wire shape was not independently re-verified against
  api_op_GetIndex.go/api_op_DeleteIndex.go this pass. UPDATE (gopherstack-7185,
  2026-08-14): DeleteIndex now field-diffed and FIXED -- DeleteIndexOutput's
  ONLY member is Status (types.IndexStatus, api_op_DeleteIndex.go:44-53), but
  DeleteIndex was reusing that same GetIndex-shaped envelope (IndexName/
  Mappings/Settings/Aliases/IndexStatus/DocumentCount), none of which is the
  real field, so a real client's *DeleteIndexOutput.Status was always empty
  even though the index was genuinely deleted. Now returns the same
  {"Status": "DELETED"} shape CreateIndex/UpdateIndex already use. Proven via
  Test_SDKRoundTrip_DeleteIndex_Status (handler_indices_test.go), which fails
  against the pre-fix envelope. CORRECTION (gopherstack-r80d, 2026-08-14): this
  note's claim that "GetIndex's full index-metadata response shape is
  correct" was itself wrong -- GetIndexOutput's only member is IndexSchema
  (api_op_GetIndex.go), not the metadata envelope either. See the `indices`
  family note above for the fix; GetIndex/DeleteIndex are now both settled.
  UPDATE (cmd/enumcheck sweep, 1d6e40d1a): UpgradeDomain field-diffed and
  FIXED -- UpgradeDomainOutput (api_op_UpgradeDomain.go:59-79) has
  AdvancedOptions/ChangeProgressDetails/DomainName/PerformCheckOnly/
  TargetVersion/UpgradeId, no StepStatus member at all (that name belongs to
  types.UpgradeStepItem, a GetUpgradeHistory/GetUpgradeStatus type). The
  handler emitted an invented `"StepStatus": "REQUESTED"` key -- "REQUESTED"
  is also not a member of UpgradeStatus (IN_PROGRESS/SUCCEEDED/
  SUCCEEDED_WITH_ISSUES/FAILED) -- which a real client silently discards on
  decode (unknown JSON keys aren't errors), so the bug was invisible to any
  test that only inspects the decoded typed struct. Now emits UpgradeId/
  DomainName/TargetVersion/PerformCheckOnly (echoed from the real request);
  AdvancedOptions/ChangeProgressDetails have no backing state in this
  synchronous backend, so they're left absent rather than fabricated. Removed
  from the not-field-diffed list above. See
  TestUpgradeDomain_RealSDKClient/TestUpgradeDomain_RawBody_NoInventedStepStatus
  (wire_field_fixes_test.go).
- **VpcEndpoint's derived AvailabilityZones/VPCId, Application's Endpoint,
  and CancelDomainConfigChange's absence of per-property
  CancelledChangeProperties** are synthesized/omitted non-stub defaults (no
  public algorithm to replicate) -- flagged in-line above, not full parity
  gaps but worth revisiting if a consumer ever depends on exact values.
- **This pass's new families (data_source_attachments, capabilities,
  insights, migrations, RollbackServiceSoftwareUpdate)**: workspace/
  export-options fields not modeled (no workspace/saved-object store exists
  at all in this backend -- a genuinely separate feature area, not merely
  unaudited), List ops ignore pagination params, and the HTTP-409-for-
  ResourceNotFoundException convention was directly confirmed for only 4 of
  the 14 new ops against live AWS docs (applied uniformly to the rest by
  inference) -- see the `gaps` list and the SDK-bump/409-convention Notes
  above for the full reasoning.

### Route reachability sweep (bd gopherstack-l5ir) -- 22 unreachable/misrouted ops found and fixed

All 96 real classic-opensearch (control-plane) ops were extracted from
`opensearch@v1.75.4` serializers.go (`request.Method` + `httpbinding.SplitURI(...)`
in each op's `awsRestjson1_serializeOp<Op>.HandleSerialize`) and diffed against
this service's route table -- the same method that found cloudfront's 35
routing bugs (gopherstack-o31x). opensearch turned out to be a second genuine
hotspot: **22 of 96 ops were unreachable or misrouted at their real path**,
against zero in the 76 REST services swept before this pass. All 22 are fixed;
`TestExtractOperation_SDKRouteTable` (`handler_paths_sdk_diff_test.go`, one
subtest per op) is the permanent regression guard -- 96/96 pass.

The bugs, by shape:

1. **Sibling-path confusion (12 ops).** The real API mixes several distinct
   path roots that gopherstack had collapsed onto one: `ListDomainNames`/
   `ListPackagesForDomain` use the un-prefixed `/2021-01-01/domain` root (no
   `/opensearch/` segment -- a historical holdover from the pre-rename
   Elasticsearch Service API), not `/2021-01-01/opensearch/domain`.
   `DescribeDomains` is `POST /2021-01-01/opensearch/domain-info`, not
   `GET /domain/describe`. `ListApplications` is
   `GET /2021-01-01/opensearch/list-applications`, a sibling of `/application`,
   not nested under it. `DescribeReservedInstanceOfferings` is its own
   `/reservedInstanceOfferings` path, not `/reservedInstances/offerings`.
   `GetUpgradeHistory`/`GetUpgradeStatus` are `/upgradeDomain/{name}/history`
   and `/upgradeDomain/{name}/status`, not nested under the domain prefix as
   `/domain/{name}/upgradeHistory`/`/upgrades`. `StartDomainMaintenance`/
   `ListDomainMaintenances`/`GetDomainMaintenanceStatus` all use the literal
   segment `domainMaintenance`/`domainMaintenances` (camelCase, no `/`
   separator from "domain"), not a bare `/maintenance` suffix -- the old
   suffix check could never match since `"...Maintenance"` never contains the
   substring `"/maintenance"`.
2. **Wrong HTTP method (2 ops).** `UpdateDomainConfig` is POST, not PUT.
   `DissociatePackage` is POST, not DELETE.
3. **Whole-request-in-body ops routed as if ID were in the URL (5 ops).**
   `UpdateVpcEndpoint` (`POST /vpcEndpoints/update`, `VpcEndpointId` in body,
   no URL binding at all), `DescribePackages`/`UpdatePackage`/
   `UpdatePackageScope` (`POST /packages/describe`|`update`|`updateScope`,
   `PackageID` in body), and `CreateIndex` (`POST /domain/{name}/index`, no
   `{IndexName}` URL segment -- unlike Get/Delete/UpdateIndex, which do carry
   IndexName in the URL) were all wired as per-ID sub-resource routes reading
   an identifier from the URL that real clients never put there. This is
   exactly the "serializer has no real URL binding" shape gopherstack-4nek
   flagged as the class every cloudfront bug shared.
4. **Wrong query-vs-path binding (1 op).** `ListInstanceTypeDetails` reads
   `EngineVersion` from a query param; the real op binds it as a URI label
   (`GET /instanceTypeDetails/{EngineVersion}?instanceType=...`), so every
   real request's engine version was silently dropped.
5. **DescribeInboundConnections/DescribeOutboundConnections (2 ops).**
   Real clients POST to `.../inboundConnection/search` and
   `.../outboundConnection/search`; gopherstack served them off a bare GET on
   the connection root instead.

None of the 22 is discriminated by a query parameter or bare flag the way
cloudfront's `?WithTags`/`Operation=Tag|Untag` were -- this hotspot's bugs are
all path/method shape, not discriminator confusion. `ExtractOperation` was
also rewritten in the same pass to mirror the corrected dispatch tree op-for-op
(it was previously best-effort and silently wrong for most domain sub-routes,
e.g. every GET under `/domain/{name}/...` falling through to `DescribeDomain`
regardless of suffix) -- it now backs the permanent test directly. Existing
tests that encoded the old wrong paths (PUT for UpdateDomainConfig, GET
`/opensearch/domain` for ListDomainNames, `/maintenance` for the maintenance
trio, etc.) were corrected to the real shapes, not preserved.

**Not covered by this pass:** the 19 OpenSearch Serverless (AOSS) ops
(`serverlessOperations()`) -- those belong to the separate
`opensearchserverless` SDK client/protocol and were out of scope (see
`items_still_open` above). The index/document *data-plane* sub-routes this
emulator invents under `/index/{name}/_doc`, `/_search`, `/_count` are not
real SDK control-plane operations and were left as-is.

# 2026-08-21 gopherstack-g479 (ad hoc map[string]any blind spot)
3 of the "Un-re-verified ops outside the assigned scope/deferred list" above
now fixed, found via a new go/types-based map-literal/index-assign kind
scanner (map[string]any{} literals had zero automated coverage before this
pass -- the prior passes' struct-field diffing couldn't see them):

- **DescribeDomainHealth**: {wire: fixed, errors: ok, state: ok, persist: n/a} --
  `TotalShards`/`DataNodeCount`/`WarmNodeCount`/`ActiveAvailabilityZoneCount`/
  `TotalUnAssignedShards` are all `NumberOfShards`/`NumberOfNodes`/`NumberOfAZs`
  shapes, which deserialize as JSON *strings* (confirmed against
  `aws-sdk-go-v2/service/opensearch@v1.75.4`'s `deserializers.go`,
  `awsRestjson1_deserializeOpDocumentDescribeDomainHealthOutput`) -- gopherstack
  emitted raw numbers, failing with `"expected NumberOfNodes to be of type
  string, got json.Number instead"`. Also dropped two invented keys that are
  not members of this shape at all: `ActiveShards` and `UnAssignedShards`
  (real member is `TotalUnAssignedShards`), and `DocumentCount`, which has no
  real per-domain-health equivalent (`DomainDocumentCount` remains the
  correct, separately-modeled aggregate).
- **DescribeDomainChangeProgress** (`GetChangeProgress`): {wire: fixed, errors: ok, state: ok, persist: n/a} --
  `ChangeProgressStatusDetails.StartTime`/`LastUpdatedTime` deserialize from a
  `json.Number` via `ParseEpochSeconds`, not an RFC3339 string; failed with
  `"expected UpdateTimestamp to be a JSON Number, got string instead"`.
- **DescribeInstanceTypeLimits**: {wire: fixed, errors: ok, state: ok, persist: ok} --
  the static instance-limits table's `MinimumInstanceCount` was the string
  constant `"1"` (`MaximumInstanceCount` siblings were already correctly
  numeric); real member is a `json.Number`. Failed with `"expected
  MinimumInstanceCount to be json.Number, got string instead"`.

All 3 proven via real `aws-sdk-go-v2/service/opensearch` client round trips
(`wire_maplit_fixes_test.go`), hand-reverted/confirmed-failing with the SDK's
own error text/restored/`md5sum`-verified byte-identical. Gates: `go build`,
`go vet`, `gofmt -l` (clean), `go test -race` (pass), `golangci-lint run`
(0 issues). `last_audit_commit` left unchanged -- this pass's own commit sha
is not known at edit time.

## gopherstack-y1zn (2026-08-21): unknown-key sweep, one confirmed bug

`ListInstanceTypeDetails`: {wire: fixed, errors: ok, state: n/a, persist: n/a} --
`AppLogEnabled` (singular "Log") was the wire key for every returned
`InstanceTypeDetails` entry; the real member (types.InstanceTypeDetails,
deserializers.go) is `AppLogsEnabled` (plural). A real client's
`AppLogsEnabled` field decoded nil regardless of the emitted value. Proven via
a real `aws-sdk-go-v2/service/opensearch` client round trip
(`TestListInstanceTypeDetails_AppLogsEnabledKey_RealClient`,
wire_maplit_fixes_test.go), hand-reverted/confirmed-failing/restored/
`md5sum`-verified byte-identical.

64 opensearch candidates from the gopherstack-us9u/g479 map-literal scanner's
526-key unknown-key bucket were triaged this pass (see gopherstack-y1zn for
the full campaign): 22 are the Elasticsearch-compatible data-plane surface
(handler_indices.go, not the control-plane SDK); 33 resolve cleanly against
the sibling `opensearchserverless` module once compared against the right
module (the dir-hosts-two-modules false positive already named above), except
3 (`networkPolicyDetail`/`networkPolicySummaries` in handler_serverless.go)
which are genuine wire-key bugs on a REST-path dispatcher this repo's own
gopherstack-92ft note already documents as unreachable by any real AOSS client
(bedrockagent-style dead duplicate; the live JSON-RPC dispatcher,
handler_serverless_jsonrpc.go, already emits the correct
`securityPolicyDetail`/`securityPolicySummaries`) -- not touched, per that
precedent. 3 were stale (already fixed by the prior gopherstack-g479 pass
before this session started; the scan snapshot predates that commit). 1
(`LimitsByRole.data`) is a `map[string]types.Limits` dynamic role-name key, not
a struct field -- correctly absent from the SDK's per-key case-switch table by
construction, not a bug.

# 2026-08-23 manifest-harvest pass (serverless field-level audit, partial)

Took the `serverless` family's still-open item ("field-level wire/state audit
still out of scope this pass", `items_still_open` above). Started the audit
against `aws-sdk-go-v2/service/opensearchserverless@v1.34.4`; found and fixed
one real bug, confirmed one genuine unimplemented-behavior gap (deferred,
correctly out of pass scope), and re-counted the un-advertised-op gap.

- **`DeleteCollection`: not-found mapped to the wrong HTTP status/error
  code -- FIXED.** `DeleteServerlessCollection` (`serverless.go:267`) returns
  the shared `ErrDomainNotFound` sentinel on an unknown id, but
  `serverlessErrorTable()` (`handler_serverless_jsonrpc.go`) never listed it,
  so `awserr.Classify` fell through to the `serverlessInternalError()`
  fallback: a real client got HTTP 500 `InternalServerException` instead of
  the real AOSS HTTP 404 `ResourceNotFoundException`
  (`opensearchserverless@v1.34.4/types/errors.go`'s `ResourceNotFoundException`,
  confirmed as the exception `DeleteCollection` is documented to return).
  Fixed by adding `ErrDomainNotFound: {Code: "ResourceNotFoundException",
  HTTPStatus: http.StatusNotFound}` to `serverlessErrorTable()`. Proven via a
  real `opensearchserverless.Client.DeleteCollection` call against an unknown
  id (`TestServerless_RealSDKClient_DeleteCollection_NotFound`,
  `handler_serverless_real_client_test.go`) -- confirmed failing
  (`StatusCode: 500 ... InternalServerException: internal error`) against the
  unfixed table, passing after, hand-reverted/restored/`md5sum`-verified
  byte-identical. Gates: `go build`, `go vet`, `gofmt -l` (clean),
  `go test ./services/opensearch/...` (pass), `golangci-lint run` (0 issues).
- **`CollectionDetail`/`CreateCollectionDetail.DeletionProtection` --
  confirmed real gap, correctly deferred, not fixed this pass.** The real
  `CreateCollectionInput`/`UpdateCollectionInput` both carry a
  `DeletionProtection` field ("When set to ENABLED, the collection cannot be
  deleted" -- doc comment on both). `ServerlessCollection` (`serverless.go`)
  has no such field at all: `jrCreateCollection` silently drops the request
  field, and `DeleteServerlessCollection` has nothing to check, so a
  protected collection can always be deleted -- real AWS-divergent behavior,
  not just a missing echo field. Not fixed this pass: doing it right needs
  `UpdateCollection` too (the only other op that can flip the flag post-create,
  per the real API) which this Handler doesn't implement at all (next
  finding), so a `CreateCollection`-only half-fix would model a flag nothing
  else can ever change -- a correctness trap, not a shortcut. Left for the
  dedicated serverless pass along with `UpdateCollection`.
- **Un-advertised real AOSS ops: re-counted at 25, not ~15.** Enumerated
  every `func (c *Client)` in `opensearchserverless@v1.34.4`'s `api_op_*.go`
  files: 44 real ops total against this Handler's `serverlessOperations()`
  list of 19. The 25 missing: `UpdateCollection`; the entire
  `CollectionGroup` family (`Create/Delete/Update/List/BatchGet`); the entire
  `LifecyclePolicy` family (`Create/Delete/Update/List/BatchGet`,
  plus `BatchGetEffectiveLifecyclePolicy`); the entire `VpcEndpoint` family
  (`Create/Delete/Update/List/BatchGet`); the `Index` family
  (`Create/Delete/Get/Update` -- a real control-plane resource, distinct from
  this package's own invented classic-OpenSearch `/index/{name}` data-plane
  routes); `TagResource`/`UntagResource`/`ListTagsForResource`;
  `GetAccountSettings`/`UpdateAccountSettings`; `GetPoliciesStats`. None of
  these are field-level gaps on an existing op -- they are whole operations
  this Handler has never implemented, each needing its own request/response
  wire types, backend state, and routing. Out of scope for a single pass;
  noted here so the next auditor doesn't re-derive the count from scratch.
  Full field-level diff of the 19 already-advertised ops' Collection/
  AccessPolicy/SecurityConfig/SecurityPolicy shapes beyond `DeletionProtection`
  above is still not done and remains this family's main open item.

## 2026-08-29 ordering-bug audit (paginate-before-filter, iam class) -- clean, no code change

Audited for the recently-found iam-class bug (a filter applied to an already-paginated page instead
of to the full set before pagination, with truncation sometimes computed to hide the loss). Grepped
every handler for `NextToken`/`nextToken`/`MaxResults`/`maxResults`/`IsTruncated`: only 4 files
reference pagination at all (`handler_insights.go`, `handler_vpc_endpoints.go`, `handler_advanced.go`,
plus the `NextToken` JSON-key constant in `handler.go`).

- `handleListInsights` (`handler_insights.go`): always returns an empty list -- this backend has no
  analytics engine to generate insights from (documented in-code); no filter or pagination logic to
  get wrong.
- `handleVersionsRoutes` / ListVersions (`handler_advanced.go`): paginates a fixed, hardcoded version
  catalog by `nextToken`/`maxResults`; no filter parameter exists on this op at all, so there is no
  order to get wrong.
- `handleVpcEndpointRootRoutes`/`handleVpcEndpointIDRoutes` (`handler_vpc_endpoints.go`): the
  `ListVpcEndpoints*` ops return every stored item unpaginated (hardcoded empty `NextToken` in the
  response, documented in-code as a required-but-inert response member) -- no truncation is ever
  claimed, so no client can be misled into thinking there's more.

No other List/Describe operation in this service implements `NextToken`/`MaxResults` pagination in
either handler or backend (confirmed by the same grep across all of `services/opensearch`), so there
is no cursor for a filter-ordering bug to hide behind anywhere else in this service. Zero findings;
no files changed.

## 2026-08-29 constraint-parameter sweep (filters/pagination never applied) -- 6 operations fixed

Measured collection-returning operations from each op's own Input struct in the pinned SDK
(`opensearch@v1.75.4`), not from the verb: 22 ops carry `Filters`/a named filter field/`Statuses`/
`MaxResults`/`NextToken`. The 08-29 ordering-bug audit above already established that *no* op in this
service implemented `MaxResults`/`NextToken` pagination at all -- this pass turned that same absence
into six concrete fixes, all previously "never read" (class 1) or "never bound" (class 3):

- **`DescribeInboundConnections`/`DescribeOutboundConnections`**
  (`inbound_connections.go`/`outbound_connections.go`/`handler_inbound_connections.go`/
  `handler_outbound_connections.go`): the handler never read the POST body at all -- `Filters`,
  `MaxResults`, `NextToken` were all silently discarded, every connection was always returned in one
  unbounded page. Fixed: `Filters` entries named `"connection-id"` now restrict the result
  (OR-within-values, matching `API_Filter.html`: "must match at least one of the specified values");
  `MaxResults` (capped at the documented maximum of 100, `API_DescribeInboundConnections.html`) and
  `NextToken` now paginate via `pkgs/page`. **Restraint**: neither `API_Filter.html` nor
  `api_op_Describe*Connections.go` enumerates a closed set of valid `Filter.Name` values for this
  operation (unlike most AWS filter APIs) -- I did not invent additional names (e.g.
  `local-domain-info.domain-name`) from outside knowledge; only `connection-id` is applied, and any
  other `Name` is a documented no-op. Shared filter+pagination logic factored into a generic
  `filterAndPageConnections[T any]` helper (`inbound_connections.go`) used by both operations --
  avoids the duplicate-bug-per-copy pattern the brief warns about, since both connection kinds now
  share one implementation instead of two.
- **`ListApplications`** (`applications.go`/`handler_applications.go`): the handler didn't read the
  query string at all (GET, all three params query-bound per `serializers.go`'s
  `awsRestjson1_serializeOpHttpBindingsListApplicationsInput`). Fixed: repeated `statuses` query
  values, `maxResults`, `nextToken` are now honored. Every application this backend creates is
  implicitly `ACTIVE` (`DeleteApplication` removes its record immediately, no `DELETING` window), so a
  `Statuses` filter that excludes `ACTIVE` now correctly returns empty rather than every application.
- **`ListDomainMaintenances`** (`domain_maintenance.go`/`handler.go`): `Action`/`Status`/
  `MaxResults`/`NextToken` are all query-bound (`awsRestjson1_serializeOpHttpBindingsListDomainMaintenancesInput`);
  the handler ignored all four and returned the domain's full history (capped at 200 records per
  domain, `advanced.go:114`) in one page regardless. Fixed: both filters and pagination now applied.
- **`ListMigrations`** (`migrations.go`/`handler_migrations.go`): `applicationId`/`status` were
  already correctly read from the query string and applied -- confirmed correct, not touched.
  `maxResults`/`nextToken` were not read at all; fixed to paginate via `pkgs/page`.
- **`DescribePackages`** (`packages.go`/`handler_packages.go`): the handler already read `Filters`
  entries but matched only `Name: "PackageID"`; `DescribePackagesFilterName`
  (`types/enums.go`) has six values -- `PackageID`, `PackageName`, `PackageStatus`, `PackageType`,
  `EngineVersion`, `PackageOwner`. Fixed `PackageName`/`PackageStatus`/`PackageType` (fields this
  backend's `Package` actually tracks) plus `MaxResults`/`NextToken` pagination. **Gap left**:
  `EngineVersion`/`PackageOwner` have no corresponding field on `Package` at all -- a structural gap,
  documented in code and here rather than fabricated.

**Confirmed already correct, not touched**: `DescribeReservedInstances`/`DescribeReservedInstanceOfferings`
(`reserved_instances.go`) already filter correctly by `reservationId`/`offeringId`; pagination was not
added -- `DescribeReservedInstanceOfferings` serves a small hardcoded static catalog
(`staticReservedInstanceOfferings()`) and per-account reserved-instance counts are realistically small,
so an unbounded page is not an observable bug here (restraint call, matching the brief's "catalogue of
three entries" guidance). `ListInsights`'s `SortOrder`/`TimeRange`/`MaxResults`/`NextToken` are accepted
but structurally inert -- this backend has no analytics engine to generate insights at all
(`handler_insights.go`'s own doc comment, confirmed correct pre-existing reasoning, not re-litigated).

Gates: `go build ./services/opensearch/...`, `go vet ./...` (repo-wide, since backend method
signatures changed), `go test ./services/opensearch/... -race -count=1` (pass), `golangci-lint run
./services/opensearch/...` (0 issues after fixing dupl via the shared generic helper above,
fieldalignment, gosec G109 by using `int` instead of `int32` for internal maxResults plumbing, and
golines). New tests in `list_filter_params_test.go` drive the real typed SDK client
(`opensearchsdk.Client`) for every fix above except the `ListMigrations` seed step, which uses the
backend directly to avoid re-deriving `StartMigration`'s unrelated `MigrationOptions.Workspace`/
`resolveDataSourceRefLocked` validation chain -- the read path under test (`ListMigrations` pagination)
still goes through the real client.

**2026-08-30 (unstable-pagination-order sweep, wrapper-key-sweep branch)**: `DescribePackages`
(`packages.go`), when called with no `PackageID` filter, built its unfiltered result from
`b.packages.All()` -- an unspecified-order map walk (`pkgs/store`'s `Table.All` doc) -- with no
sort at all before `pkgs/page.New`'s offset-based pagination. `page.New`'s own doc says it "creates
a Page from a fully sorted slice"; this call site did not honor that contract, so a client paging
with `MaxResults` smaller than the package count could drop or duplicate a package at a page
boundary even though `PackageID` (the table's own key) is unique -- offset pagination over an
unstable order breaks the same way marker pagination does. Fixed by reading via
`b.packages.Snapshot()` instead of `.All()` -- `Snapshot()` sorts by the table's own key
(`PackageID`) ascending, deterministically. The `len(ids) > 0` branch (filtering to explicit
`PackageID`s from the request) was already safe -- it iterates the caller-supplied `ids` slice, not
a map.

Every other paginated `List*`/`Describe*` site in this service was audited this pass and confirmed
already safe: `DescribeInboundConnections`/`DescribeOutboundConnections` (`inbound_connections.go`'s
shared `filterAndPageConnections`) sort by `ConnectionID`, the table's own key; `ListApplications`
(`applications.go`) sorts by `ID`, the table's own key; `ListDomainMaintenances`
(`domain_maintenance.go`) reads a direct per-key slice (`b.domainMaintenances[domainName]`), not a
map range; `ListMigrations` (`migrations.go`) reads via `store.Index.Get`, which is
insertion-ordered, not a map range.

Proof: `TestDescribePackages_PaginationOrderIsReproducible` (`handler_packages_test.go`) creates 60
packages, walks them with `MaxResults=7` across `NextToken`-resumed pages (real
`opensearchsdk.Client`), and asserts the concatenation reproduces the set exactly with no
drops/duplicates, looped 30 times; failed reliably against the unfixed code (drops and triplicate
counts observed), passes after the `.Snapshot()` fix.

Gates: `go build ./services/opensearch/...`, `go vet ./services/opensearch/...`,
`go test -race -count=1 ./services/opensearch/...` (pass), `golangci-lint run
./services/opensearch/...` (0 issues). Work left uncommitted per this pass's instructions.
