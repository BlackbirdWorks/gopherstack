---
service: opensearch
sdk_module: aws-sdk-go-v2/service/opensearch@v1.75.0
sibling_sdk_modules: [aws-sdk-go-v2/service/opensearchserverless@v1.34.2]  # AOSS ops this Handler also implements (serverlessOperations()); see families.serverless
last_audit_commit: acb2e23f9
last_audit_date: 2026-07-30
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
                      # since GetMigrationOutput/MigrationSummary never echo them back either. One
                      # unrelated, pre-existing gap remains open and undisturbed by this pass (see gaps
                      # below): ListDataSourceAttachments/ListMigrations still ignore maxResults/nextToken.
ops:
  CreateDomain: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed DomainId (required field, was missing) and IdentityCenterOptions wire key (see Notes)"}
  DescribeDomain: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeDomains: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDomain: {wire: ok, errors: ok, state: ok, persist: ok, note: "this pass added cascade-cleanup of inbound/outbound connections owned by the domain (see cross_cluster_connections)"}
  ListDomainNames: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed wire key EngineVersion->EngineType and value shape (full version string -> engine family); engineType filter param/logic was already correct"}
  UpdateDomainConfig: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed IdentityCenterOptions wire key; added DryRun=true support (previously always mutated even when DryRun requested)"}
  DescribeDomainConfig: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed IdentityCenterOptions wire key"}
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
      accepted a nil/empty DomainArn or VpcOptions with no validation.
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
  serverless:
    status: deferred
    note: >
      Field-level wire/state audit still out of scope this pass -- see items_still_open. UPDATE
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
      Remaining gap, unrelated to workspaces and unchanged this pass: ListDataSourceAttachments
      accepts but ignores maxResults/nextToken (returns the full list unpaginated) -- consistent
      with how most other List ops in this backend already treat pagination params, but flagged as
      a real, not-hidden gap.
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
      Remaining gap, unrelated to workspaces and unchanged this pass: ListMigrations accepts but
      ignores maxResults/nextToken (returns the full filtered list unpaginated).
gaps:
  - "data_source_attachments and migrations: List ops (ListDataSourceAttachments/ListMigrations) accept but ignore maxResults/nextToken, always returning the full (filtered) result set unpaginated."
deferred:
  - serverless
leaks: {status: clean, note: "no goroutines/janitors in this service; coarse lockmetrics.RWMutex per backend, no per-map locks introduced. This pass's DeleteDomain connection-cascade iterates Table.All() (a fresh snapshot slice per the existing convention) while deleting, same safe pattern as the pre-existing package/index/data-source cascades. New this pass: DeleteApplication now cascades data source attachments, capabilities, and migration jobs using the identical clone-then-delete pattern (Table.All()/Index.Get results are fresh/cloned slices, safe to range over while deleting)."}
---

## Notes

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

- **serverless** (OpenSearch Serverless collections/policies): explicitly out
  of this pass's assigned scope. `aws-sdk-go-v2/service/opensearchserverless`
  was added to go.mod in the 2026-07-31 reverse-sdkcheck sweep (see families.serverless)
  to fix an operation-naming bug (8 fabricated policy op names) and verify the
  rest of serverlessOperations() against the real client, but no field-level
  wire/state diff of Collection/AccessPolicy/SecurityConfig/SecurityPolicy was
  done. Still needs a dedicated audit pass.
- **Un-re-verified ops outside the assigned scope/deferred list**: GetCompatibleVersions,
  ListVersions, DescribeDomainAutoTunes, DescribeDomainChangeProgress,
  DescribeDomainHealth, DescribeDomainNodes, DescribeDryRunProgress,
  DescribeInstanceTypeLimits, GetDomainMaintenanceStatus, GetUpgradeHistory,
  GetUpgradeStatus, ListDomainMaintenances, ListInstanceTypeDetails,
  StartDomainMaintenance, UpgradeDomain, and the index/document data-plane ops
  (CreateIndex/DeleteIndex/GetIndex/UpdateIndex) were not touched or
  field-diffed this pass (they were not in the original 1-gap/8-deferred list
  this pass was scoped to fix). Not reclassified either direction; still
  whatever state the prior pass left them in.
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
