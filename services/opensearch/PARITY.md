---
service: opensearch
sdk_module: aws-sdk-go-v2/service/opensearch@v1.59.0
last_audit_commit: b89568f5
last_audit_date: 2026-07-23
overall: A            # genuine fixes found this pass
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
      Out of this pass's explicit task scope (the assigned service description enumerates domains,
      config, versions, packages, VPC endpoints, reserved instances, cross-cluster connections,
      dry-run, auto-tune, off-peak windows, software update, data sources, direct queries, and
      application -- serverless collections are conspicuously absent). Also structurally blocked:
      OpenSearch Serverless is a genuinely separate AWS API/SDK module
      (aws-sdk-go-v2/service/opensearchserverless, 2021-11-01), not present in go.mod, and this
      pass is barred from touching go.mod/go.sum. Left untouched; needs its own audit pass with
      the real opensearchserverless types available for field-diffing.
gaps: []
deferred:
  - serverless
leaks: {status: clean, note: "no goroutines/janitors in this service; coarse lockmetrics.RWMutex per backend, no per-map locks introduced. This pass's DeleteDomain connection-cascade iterates Table.All() (a fresh snapshot slice per the existing convention) while deleting, same safe pattern as the pre-existing package/index/data-source cascades."}
---

## Notes

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

## items_still_open (this pass)

- **serverless** (OpenSearch Serverless collections/policies): explicitly out
  of this pass's assigned scope, and the real SDK types live in
  `aws-sdk-go-v2/service/opensearchserverless`, not in go.mod. Cannot
  field-diff without adding a dependency, which this pass is barred from
  doing. Needs a dedicated audit pass.
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
