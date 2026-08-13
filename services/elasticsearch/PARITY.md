---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: elasticsearch
sdk_module: aws-sdk-go-v2/service/elasticsearchservice@v1.45.4
last_audit_commit: 8dc21e834
last_audit_date: 2026-08-13
overall: A            # gopherstack-p2mx pass: fixed CancelDomainConfigChange's borrowed-shape response and
                       # CreateVpcEndpoint/UpdateVpcEndpoint's VpcOptions map[string]string that made every
                       # real-SDK-client request 400 -- see Notes. Route audit (51/51) reconfirmed, no new
                       # routing bugs. VPCOptions VPCId/AvailabilityZones and Processing remain documented gaps
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateElasticsearchDomain: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-08-10 pass added AdvancedSecurityOptions.SAMLOptions, AutoTuneOptions.MaintenanceSchedules, and DeploymentStrategyOptions -- previously accepted-but-dropped or entirely unmodeled; see Notes"}
  DescribeElasticsearchDomain: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeElasticsearchDomains: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteElasticsearchDomain: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDomainNames: {wire: ok, errors: ok, state: ok, persist: ok, note: "route bug fixed this pass -- was served at the wrong path; see Notes"}
  UpdateElasticsearchDomainConfig: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-08-10 pass added AdvancedSecurityOptions.SAMLOptions, AutoTuneOptions.MaintenanceSchedules, and DeploymentStrategyOptions; see Notes"}
  DescribeElasticsearchDomainConfig: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-08-10 pass fixed AutoTuneOptions.Options/Status to use their real distinct shapes (types.AutoTuneOptions/types.AutoTuneStatus, not the DomainStatus response's AutoTuneOptionsOutput/generic OptionStatus) and added MaintenanceSchedules + DeploymentStrategyOptions; see Notes"}
  CancelDomainConfigChange: {wire: fixed, errors: ok, state: ok, persist: n/a, note: "FIXED (gopherstack-p2mx) -- was echoing DescribeElasticsearchDomainConfig's DomainConfig-wrapped body (a borrowed shape, wrong operation entirely) instead of CancelDomainConfigChangeOutput's own {CancelledChangeIds,CancelledChangeProperties,DryRun}; DryRun was also never read from the request. Now returns the real shape: empty CancelledChangeIds/CancelledChangeProperties (this backend has no pending-change queue -- every config change already applied synchronously, so there is truly nothing to report as cancelled) and DryRun echoed from the request. Prior wire: ok was false; the old unit test asserted the wrong (bug-matching) shape and was corrected alongside the fix"}
  AddTags: {wire: ok, errors: ok, state: ok, persist: ok}
  RemoveTags: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTags: {wire: ok, errors: ok, state: ok, persist: ok}
  StartElasticsearchServiceSoftwareUpdate: {wire: ok, errors: ok, state: ok, persist: ok, note: "route bug fixed this pass -- see Notes"}
  CancelElasticsearchServiceSoftwareUpdate: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteElasticsearchServiceRole: {wire: ok, errors: ok, state: ok, persist: n/a}
  UpgradeElasticsearchDomain: {wire: ok, errors: ok, state: ok, persist: ok}
  GetUpgradeHistory: {wire: ok, errors: ok, state: ok, persist: n/a, note: "no upgrade-history state tracked; always returns empty list"}
  GetUpgradeStatus: {wire: ok, errors: ok, state: ok, persist: n/a, note: "always reports SUCCEEDED; no async upgrade state"}
  DescribeDomainAutoTunes: {wire: ok, errors: ok, state: ok, persist: n/a, note: "always empty; no auto-tune state modeled"}
  DescribeDomainChangeProgress: {wire: ok, errors: ok, state: ok, persist: n/a, note: "always COMPLETED; changes apply synchronously"}
  GetCompatibleElasticsearchVersions: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListElasticsearchVersions: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListElasticsearchInstanceTypes: {wire: ok, errors: ok, state: ok, persist: n/a}
  DescribeElasticsearchInstanceTypeLimits: {wire: ok, errors: ok, state: ok, persist: n/a}
  CreatePackage: {wire: ok, errors: ok, state: ok, persist: ok, note: "this pass added required PackageSource (S3BucketName/S3Key) validation; also deleted invented ZIP-PLUGIN package type -- see Notes. 2026-08-10: added CreatedAt/LastUpdatedAt"}
  DescribePackages: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-08-10: response now includes CreatedAt/LastUpdatedAt; ErrorDetails always omitted (no COPY_FAILED state modeled)"}
  UpdatePackage: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-08-10: LastUpdatedAt now advances on update"}
  DeletePackage: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociatePackage: {wire: ok, errors: ok, state: ok, persist: ok}
  DissociatePackage: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPackageVersionHistory: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListDomainsForPackage: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListPackagesForDomain: {wire: ok, errors: ok, state: ok, persist: n/a}
  CreateVpcEndpoint: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED (gopherstack-p2mx) -- request/response VpcOptions was map[string]string; real wire shape is types.VPCOptions/{SecurityGroupIds,SubnetIds} (request) and types.VPCDerivedInfo (response, same two fields plus unmodeled AvailabilityZones/VPCId -- matches the identical domain-level VPCOptions simplification). A real SDK client always serializes VpcOptions as {SecurityGroupIds:[...],SubnetIds:[...]}, so json.Unmarshal into map[string]string failed on every real call with a security group or subnet -- CreateVpcEndpoint 400'd unconditionally for any non-toy client. Reused the already-correct vpcOptionsRequestJSON/vpcDerivedInfoJSON/toVPCDerivedInfoJSON machinery built for domain-level VPCOptions (handler_domains.go) -- CreateVpcEndpointInput.VpcOptions is the literal same SDK type. Prior wire: ok was false; existing unit tests asserted the broken shape (flat VpcId/SubnetId keys) and were corrected. Proven via a real aws-sdk-go-v2 client round-trip (handler_sdk_roundtrip_test.go), verified to fail against the unfixed code by hand-revert"}
  DescribeVpcEndpoints: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListVpcEndpoints: {wire: fixed, errors: ok, state: ok, persist: n/a, note: "FIXED (gopherstack-lx5h) — dropped required NextToken (ListVpcEndpointsOutput, deserializers.go). Single-page emulator (never truncated) so no data is lost, but a required pointer left nil could panic a client that dereferences it unconditionally; now always emitted as an empty string. Prior wire: ok was false"}
  ListVpcEndpointsForDomain: {wire: fixed, errors: ok, state: ok, persist: n/a, note: "FIXED (gopherstack-lx5h) — same required-NextToken gap and fix as ListVpcEndpoints above. Prior wire: ok was false"}
  UpdateVpcEndpoint: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED (gopherstack-p2mx) -- same VpcOptions map[string]string bug and fix as CreateVpcEndpoint above. Prior wire: ok was false"}
  DeleteVpcEndpoint: {wire: ok, errors: ok, state: ok, persist: ok}
  AuthorizeVpcEndpointAccess: {wire: ok, errors: ok, state: ok, persist: ok}
  RevokeVpcEndpointAccess: {wire: ok, errors: ok, state: ok, persist: ok}
  ListVpcEndpointAccess: {wire: fixed, errors: ok, state: ok, persist: n/a, note: "FIXED (gopherstack-lx5h) — same required-NextToken gap and fix as ListVpcEndpoints (ListVpcEndpointAccessOutput, deserializers.go). Prior wire: ok was false"}
  CreateOutboundCrossClusterSearchConnection: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeOutboundCrossClusterSearchConnections: {wire: ok, errors: ok, state: ok, persist: n/a}
  DeleteOutboundCrossClusterSearchConnection: {wire: ok, errors: ok, state: ok, persist: ok}
  AcceptInboundCrossClusterSearchConnection: {wire: ok, errors: ok, state: ok, persist: ok}
  RejectInboundCrossClusterSearchConnection: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteInboundCrossClusterSearchConnection: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeInboundCrossClusterSearchConnections: {wire: ok, errors: ok, state: ok, persist: n/a}
  DescribeReservedElasticsearchInstanceOfferings: {wire: ok, errors: ok, state: ok, persist: n/a}
  DescribeReservedElasticsearchInstances: {wire: ok, errors: ok, state: ok, persist: ok}
  PurchaseReservedElasticsearchInstanceOffering: {wire: ok, errors: ok, state: ok, persist: ok}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "Domains never transition through a Processing/creating state -- CreateElasticsearchDomain \
     returns Processing=false / DomainProcessingStatus=Active immediately, and Endpoint is \
     populated synchronously too, so every field a real client would poll on (Processing, \
     DomainProcessingStatus, Endpoint, and DescribeElasticsearchDomainConfig's per-field \
     OptionStatus.State) is self-consistently 'already done'. Re-verified 2026-08-10 \
     (gopherstack-toz8): checked whether any client-visible action (Create, \
     UpdateElasticsearchDomainConfig, Delete) should visibly flip Processing to true -- this \
     backend applies all three synchronously with no async work to represent, so there is \
     nothing for a transient Processing=true to model faithfully; a fake timed delay would be \
     invented state, not parity. Confirmed deliberate simplification, not a stub -- SDK callers \
     that poll DescribeElasticsearchDomain waiting for Processing==false succeed immediately \
     instead of spinning. Separately (not in scope this pass): ElasticsearchDomainStatus.Created/ \
     Deleted (types.go:958-966) are not modeled at all, unlike Processing/DomainProcessingStatus \
     which are (see toDomainStatusJSON)."
  - "VPCOptions.VPCId and .AvailabilityZones are never populated on Describe/domain-status \
     responses -- deriving them would require a cross-service EC2 subnet/VPC lookup this \
     backend does not perform (SubnetIds/SecurityGroupIds are correctly modeled and echoed). \
     Matches services/opensearch's identical, already-accepted simplification. Needs cli.go \
     wiring to close: this service has no reference to any EC2 backend today (grep confirms no \
     ec2 import in services/elasticsearch), so VPCId/AvailabilityZones would need either (a) an \
     EC2 lookup interface (mirroring how services/elasticsearch already takes a DNSRegistrar \
     interface, store_setup.go) that cli.go wires to the real services/ec2 backend when both \
     services are registered, or (b) a shared pkgs/ helper cli.go injects both backends into. \
     Either way the wiring decision belongs in cli.go, which this pass does not touch."
  - "AutoTuneOptions.RollbackOnDisable (types.AutoTuneOptions, Update-only -- it is not a \
     member of the Create-only types.AutoTuneOptionsInput) is not modeled. Not filed as a bd \
     issue this pass: this backend has no rollback state machine to act on it, and it is a \
     narrower field than the two this pass targeted (SAMLOptions/MaintenanceSchedules)."
deferred: []              # this pass's target deferred item (DescribeElasticsearchDomainConfig per-field OptionStatus) is now implemented; remaining edges tracked under gaps above
leaks: {status: clean, note: "no goroutines/janitors in this service; Snapshot/Restore close domain Tags before replacing state (verified in persistence.go). This pass also fixed domainCopy (store.go) to deep-clone AdvancedOptions/VPCOptions/CognitoOptions/AdvancedSecurityOptions/AutoTuneOptions/LogPublishingOptions -- previously AdvancedOptions (and now the five new option fields) were shallow-copied, so a caller mutating the map/slice on a DescribeDomain result would have silently mutated the backend's stored state. Not a resource leak, but a real aliasing bug fixed alongside the new fields it would otherwise have applied to as well. 2026-08-10: extended the same deep-clone treatment to AdvancedSecurityOptions.SAMLOptions (and its Idp pointer) and AutoTuneOptions.MaintenanceSchedules (and each element's Duration pointer), which would otherwise have reintroduced the identical aliasing bug for the newly-added nested pointers/slices."}
---

## Notes

Protocol: **restjson1**. Base path prefix `/2015-01-01/`.

### 2026-08-13 pass (gopherstack-p2mx): first full audit + two real bugs found and fixed

This issue was filed on the premise that `services/elasticsearch` had **no** PARITY.md
at all. That premise was false by the time this pass started: the file already existed
(added by the 2026-07-12/07-24/08-10 passes above, `last_audit_commit: 59ab8f6a`,
graded A) and was current -- it already reflected the `0190c00b0`
NextToken fix on `ListVpcEndpoints`/`ListVpcEndpointAccess`/`ListVpcEndpointsForDomain`.
Rather than skip the pass on that basis, this session treated the existing file as a
baseline to re-verify (per parity-principles.md's own re-audit protocol) instead of
trusting it blind, and found two real bugs the prior passes' route-level and
field-level checks had missed:

1. **`CancelDomainConfigChange` was returning the wrong operation's response shape**
   (`handler_domain_config.go`, previously line ~447) -- a textbook instance of the
   "operation reimplemented as a different operation entirely" bug class. The handler
   called `buildDomainConfigOutput(d)`, which builds `{"DomainConfig": {...}}` --
   the `DescribeElasticsearchDomainConfig`/`UpdateElasticsearchDomainConfig` response
   shape. Real `CancelDomainConfigChangeOutput`
   (confirmed via `awsRestjson1_deserializeOpDocumentCancelDomainConfigChangeOutput`,
   deserializers.go:747) is `{CancelledChangeIds: []string, CancelledChangeProperties:
   []types.CancelledChangeProperty, DryRun: *bool}` -- an entirely different shape with
   no overlapping keys. None of the three real fields are required, so a real SDK
   client wouldn't panic (restjson1 ignores unknown response keys and leaves absent
   optional fields nil) -- but it would silently get `CancelledChangeIds`,
   `CancelledChangeProperties`, and `DryRun` as permanently nil/false regardless of
   what it asked for, and the request's own `DryRun` member was never read at all. The
   existing unit test (`TestElasticsearchHandler_CancelDomainConfigChange`) asserted
   the wrong (bug-matching) shape -- `wantContains: []string{"DomainConfig",
   "ElasticsearchVersion"}` -- so it passed green against the bug, another instance of
   parity-principles.md rule 3. Fixed: the handler now reads `DryRun` from the request
   body and returns the real shape with empty `CancelledChangeIds`/
   `CancelledChangeProperties` (this backend has no pending-config-change queue --
   every change already applies synchronously, so "nothing is ever pending to cancel"
   is the honest answer, not a stub) and `DryRun` echoed from the request. Test
   corrected to assert the real keys; a new `Test_SDKRoundTrip_CancelDomainConfigChange`
   (`handler_sdk_roundtrip_test.go`) drives the real SDK client and asserts `DryRun`
   round-trips, verified to fail against the unfixed code by hand-revert.

2. **`CreateVpcEndpoint`/`UpdateVpcEndpoint`'s `VpcOptions` was `map[string]string`**
   (`models.go`, `vpc_endpoints.go`, `handler_vpc_endpoints.go`) instead of the real
   `types.VPCOptions` shape (`{SecurityGroupIds: []string, SubnetIds: []string}`,
   confirmed via `awsRestjson1_serializeDocumentVPCOptions`, serializers.go:5038, and
   `awsRestjson1_deserializeDocumentVPCDerivedInfo` for the response side,
   deserializers.go:15133). This is a client-breaking bug, not a cosmetic one: a real
   `aws-sdk-go-v2` client always serializes `VpcOptions` as
   `{"SecurityGroupIds":["sg-..."],"SubnetIds":["subnet-..."]}` -- decoding that into
   `map[string]string` fails outright (`json: cannot unmarshal array into Go value of
   type string`), so `CreateVpcEndpoint` 400'd with `ValidationException: invalid JSON
   body` for *every* real caller that supplied a security group or subnet, which is to
   say every real caller. `test/integration/elasticsearch_test.go`'s
   `TestIntegration_Elasticsearch_VpcEndpointList_NextToken` even carried a comment
   noting this as a known, out-of-scope, unfixed bug ("gopherstack-elsewhere") -- it
   was in scope for this pass and is now fixed. The fix reuses the
   `vpcOptionsRequestJSON`/`vpcDerivedInfoJSON`/`toVPCDerivedInfoJSON` machinery
   `handler_domains.go` already built for domain-level `VPCOptions`, since
   `CreateVpcEndpointInput.VpcOptions` is the literal same SDK type
   (`*types.VPCOptions`) -- no new wire-shape modeling was needed, just correcting
   which existing shape this operation used. `models.go`'s `VpcEndpoint.VpcOptions`
   field changed from `map[string]string` to the existing `VPCOptions` model type;
   `vpc_endpoints.go`'s deep-copy helper and `store.go`'s `vpcEndpointCopy` were
   updated to clone the two slices instead of a map. `AvailabilityZones`/`VPCId` on the
   response (`types.VPCDerivedInfo`'s other two members) are left unmodeled, matching
   the identical, already-accepted domain-level VPCOptions simplification (see gaps
   below) -- not a new gap, the same one extended to a second operation pair that
   shares the type. Existing unit tests asserted the broken flat-key shape (`VpcId`,
   `SubnetId` as top-level string values) and were corrected to the real
   `SecurityGroupIds`/`SubnetIds` array shape. A new
   `Test_SDKRoundTrip_CreateVpcEndpoint_VpcOptions` (`handler_sdk_roundtrip_test.go`)
   drives the real SDK client with a real `types.VPCOptions` request and asserts the
   response round-trips both fields; verified to fail against the unfixed code
   (`ValidationException: invalid JSON body`) by hand-revert.

**Not a bug, documented for the next auditor**: `ListVpcEndpoints`/
`ListVpcEndpointsForDomain` return the same `vpcEndpointJSON` shape (including
`Endpoint` and `VpcOptions`) for every list entry, but the real
`ListVpcEndpointsOutput.VpcEndpointSummaryList` is `[]types.VpcEndpointSummary`, a
narrower shape with only `DomainArn`/`Status`/`VpcEndpointId`/`VpcEndpointOwner` --
no `Endpoint` or `VpcOptions`. restjson1 clients ignore unknown response keys, so this
is inert (proven by the existing `ListVpcEndpoints`/`ListVpcEndpointAccess` SDK
round-trip test continuing to pass unmodified), but it's excess surface a future change
to `vpcEndpointJSON` could accidentally turn into a real bug. Left as-is this pass
(not required-field-related, not client-breaking) but worth tightening in a future
pass. Same observation applies to `DeleteVpcEndpoint`'s `VpcEndpointSummary` response.

**Route audit reconfirmed, not repeated from scratch**: the bd issue this pass closes
(gopherstack-p2mx) cited a prior route audit (gopherstack-4nek) that traced all 51 ops
in `buildOps()` plus all three prefix-router chains in `handler.go` against the SDK's
`serializers.go` method/path pairs, 51/51 match, zero routing bugs -- see "Route audit
method" below, which predates this pass and was spot-checked (not re-run end-to-end)
against the two ops touched here; both were already correctly routed.

**Bug-class coverage for this pass**: class 3 (borrowed shapes/behaviour) accounted for
both bugs found -- `CancelDomainConfigChange` borrowed a different operation's entire
response shape, and `CreateVpcEndpoint`/`UpdateVpcEndpoint` borrowed the wrong Go type
for a field two operations happen to share with domain-level `VPCOptions`. Spot-checked
for classes 1/2/4 (required-input-never-read, required-output-never-populated,
empty-struct inputs) across `PurchaseReservedElasticsearchInstanceOffering`,
`CreateOutboundCrossClusterSearchConnection`, `AuthorizeVpcEndpointAccess`,
`RevokeVpcEndpointAccess`, the four inbound/outbound connection lifecycle ops,
`UpgradeElasticsearchDomain`, `StartElasticsearchServiceSoftwareUpdate`, and all
no-required-input read-only ops (`GetCompatibleElasticsearchVersions`,
`ListElasticsearchVersions`, `ListElasticsearchInstanceTypes`,
`DescribeElasticsearchInstanceTypeLimits`, `GetPackageVersionHistory`,
`ListDomainsForPackage`, `ListPackagesForDomain`, `DescribeDomainAutoTunes`,
`DescribeDomainChangeProgress`, `GetUpgradeHistory`, `GetUpgradeStatus`,
`CancelElasticsearchServiceSoftwareUpdate`, `DeleteElasticsearchServiceRole`) --
none had unread required inputs, unpopulated required outputs, or `struct{}`-typed
inputs hiding real required members. `CreateElasticsearchDomain`/
`UpdateElasticsearchDomainConfig`/`CreatePackage` were not re-audited field-by-field
this pass (already exhaustively covered by the 2026-07-24/08-10 passes above, files
unchanged since `59ab8f6a`) per the manifest's own re-audit protocol.

### 2026-08-10 pass (gopherstack-toz8 follow-up): SAMLOptions, MaintenanceSchedules, DeploymentStrategyOptions, Package timestamps

Five items were bundled in this follow-up issue; ranked by real-client likelihood and
implemented to full depth where feasible, per parity-principles.md rule 1 (no
half-modeled fields) and this campaign's standing "model faithfully or leave and say
why" rule:

1. **`AdvancedSecurityOptions.SAMLOptions` + `AutoTuneOptions.MaintenanceSchedules`**
   (highest priority -- this is the exact "accepted but silently dropped" bug class
   this campaign targets). Both were previously parsed as `json.RawMessage` purely to
   avoid rejecting the request, then discarded. Now fully modeled:
   - `models.go`: added `SAMLIdp`, `SAMLOptions`, `Duration`, `AutoTuneMaintenanceSchedule`
     types, and `SAMLOptions`/`MaintenanceSchedules` fields on `AdvancedSecurityOptions`/
     `AutoTuneOptions`. Field names/requiredness verified against
     `aws-sdk-go-v2/service/elasticsearchservice@v1.45.4`: `validateSAMLIdp`
     (validators.go:1091-1107, both `EntityId`/`MetadataContent` required whenever `Idp`
     is present) and the `SAMLOptionsInput`/`SAMLOptionsOutput`/`AutoTuneMaintenanceSchedule`/
     `Duration` struct declarations in types.go (all plain structs, none are smithy
     unions). `SAMLOptionsOutput` has no `MasterUserName`/`MasterBackendRole` members, so
     (like the pre-existing `MasterUserOptions` treatment) those two are stored but never
     echoed back, matching real AWS.
   - **Found and fixed a second, deeper bug while wiring this in**: the
     `DescribeElasticsearchDomainConfig`/`UpdateElasticsearchDomainConfig` response's
     `DomainConfig.AutoTuneOptions` field was using the *DomainStatus* response's shape
     (`types.AutoTuneOptionsOutput` -- `State`/`ErrorMessage` only) for its `Options`
     member, and the generic `elasticsearchConfigStatus`/`OptionStatus` shape for its
     `Status` member. Neither is correct: per the pinned SDK,
     `AutoTuneOptionsStatus.Options` is `*types.AutoTuneOptions`
     (`DesiredState`/`MaintenanceSchedules`/`RollbackOnDisable`, types.go:283-300) and
     `AutoTuneOptionsStatus.Status` is `*types.AutoTuneStatus` (types.go:344-371) --
     the *only* DomainConfig field with a non-generic Status shape, confirmed against
     `awsRestjson1_deserializeDocumentAutoTuneOptionsStatus`/`...AutoTuneStatus`
     (deserializers.go:9590-9700). `MaintenanceSchedules` could not be bolted onto the
     old (wrong) shape without perpetuating that bug, so `handler_domain_config.go` now
     has dedicated `domainConfigAutoTuneOptionsJSON`/`autoTuneStatusJSON`/
     `autoTuneConfigValue` types and `toDomainConfigAutoTuneOptionsJSON`/
     `autoTuneConfigStatus` builders. `AutoTuneStatus.State` (`ENABLED`/`DISABLED`,
     `AutoTuneState` enum) maps directly from `DesiredState` -- no
     `ENABLE_IN_PROGRESS`/`DISABLE_IN_PROGRESS` transition window, the same synchronous
     simplification already applied elsewhere in this service. The DomainStatus
     response's own `AutoTuneOptions` (`toAutoTuneOptionsJSON`) was already correct and
     is untouched. A pre-existing unit test
     (`TestElasticsearchHandler_UpdateDomainConfig_SecurityFields`) asserted the old
     (wrong) shape's `State` field inside `Options` and was corrected to assert
     `DesiredState` in `Options` and `State` in `Status` separately -- textbook case of
     parity-principles.md rule 3 ("unit tests are not parity proof").
   - `AutoTuneOptions.RollbackOnDisable` (Update-only, no Create equivalent) is
     deliberately NOT modeled -- see gaps.
2. **`DeploymentStrategyOptions`**: real, simple field (`types.DeploymentStrategyOptions`
   has one required member, `DeploymentStrategy`, enum `Default`/`CapacityOptimized` --
   types/enums.go:130-136), present on `CreateElasticsearchDomainInput`,
   `UpdateElasticsearchDomainConfigInput`, `ElasticsearchDomainStatus` (flat, not
   Status-wrapped), and `ElasticsearchDomainConfig` (Status-wrapped with the generic
   `OptionStatus`, confirmed types.go:861-862/550-561). Added end-to-end with request
   validation and defaults to `"Default"` on the DomainConfig response when unset,
   matching the enum's default value.
3. **`Package.CreatedAt`/`LastUpdatedAt`/`ErrorDetails`**: `CreatedAt`/`LastUpdatedAt`
   are now set at `CreatePackage` and `LastUpdatedAt` advances on `UpdatePackage`
   (epoch-seconds via `pkgs/awstime.Epoch`, matching restjson1). `ErrorDetails`
   (`types.ErrorDetails{ErrorMessage, ErrorType}`) is modeled as a real type but is
   always `nil` in practice: this backend has no `COPYING`/`COPY_FAILED` state machine
   (packages transition straight to `AVAILABLE`), so there is no natural source for it
   -- documented as structural, not left silently absent.
4. **`VPCOptions.VPCId`/`.AvailabilityZones`**: confirmed still blocked on an EC2
   lookup this service has no access to (no `ec2` import anywhere in
   `services/elasticsearch`) -- see gaps for the specific `cli.go` wiring this would
   need. Not implemented this pass per the exclusion on editing `cli.go`.
5. **Domains never reach `Processing`**: investigated what real AWS does (no
   `elasticsearchservice` waiter exists in the pinned SDK -- confirmed no
   `waiters.go` in the module -- so real clients like Terraform's
   `aws_elasticsearch_domain` hand-roll polling against `DescribeElasticsearchDomain`,
   checking `Processing`/`Endpoint`). Checked whether `Endpoint`,
   `DomainProcessingStatus`, and `DescribeElasticsearchDomainConfig`'s per-field
   `OptionStatus.State` are all self-consistently "instantly ready" together with
   `Processing` (they are: `Endpoint` is set synchronously in `CreateDomain`,
   `OptionStatus.State` is always `"Active"`) -- so there is no field a poll loop would
   see as "done" while `Processing` still lied about it. Verdict: legitimate emulator
   simplification, not a stub; see gaps for the full reasoning and the separate,
   out-of-scope `Created`/`Deleted` field gap this surfaced.

Round-trip persistence coverage added: `persistence_test.go`'s
`domain_advanced_options_preserved` case now also exercises SAMLOptions/
MaintenanceSchedules/DeploymentStrategyOptions, and `package_and_association_preserved`
asserts Package CreatedAt/LastUpdatedAt survive Snapshot/Restore. No snapshot version
bump: every new field is `omitempty`/`omitzero`, additive-only.

### 2026-07-24 pass: CreateElasticsearchDomain field-coverage gaps closed

The 2026-07-12 audit marked `CreateElasticsearchDomain`/`DescribeElasticsearchDomain`/
`UpdateElasticsearchDomainConfig`/`DescribeElasticsearchDomainConfig` all
`wire: ok` on the strength of top-level shape/route verification, but never
field-diffed `CreateElasticsearchDomainInput` member-by-member against
`types.CreateElasticsearchDomainInput`. Doing that this pass found five
request/response members that were **entirely unmodeled** (no struct field,
no request parsing, no response echo) despite the earlier audit's `ok`
rating: `VPCOptions`, `CognitoOptions` (a `CognitoOptions{Enabled: false}`
was hardcoded into every response regardless of input), `LogPublishingOptions`,
`AdvancedSecurityOptions`, and `AutoTuneOptions`. This is the same bug class
parity-principles.md rule 4 warns about ("a 'real-looking' op may be a
disguised stub") — `wire: ok` was recorded from route/top-level-shape
checking, not a real field enumeration. Fixed this pass:

- `models.go`: added `VPCOptions`, `CognitoOptions`, `LogPublishingOption`,
  `AdvancedSecurityOptions`, `AutoTuneOptions` types and wired them into
  `Domain`/`CreateDomainInput`/`UpdateConfig`. Also added
  `CreatedAt`/`ConfigUpdatedAt`/`ConfigVersion` to back the
  `DescribeElasticsearchDomainConfig` `OptionStatus` fix below, and a `Tags`
  map on `CreateDomainInput` so `CreateElasticsearchDomainInput.TagList` can
  apply tags atomically at creation (previously only reachable via a
  separate `AddTags` call after create).
- `handler_domains.go` / `handler_domain_config.go`: request parsing,
  response echo, and real AWS-matching validation for all five —
  `CognitoOptions.Enabled=true` requires `UserPoolId`/`IdentityPoolId`/`RoleArn`;
  `AdvancedSecurityOptions.Enabled && InternalUserDatabaseEnabled` requires
  `MasterUserOptions`; `AutoTuneOptions.DesiredState` is validated against the
  `ENABLED`/`DISABLED` enum. `MasterUserOptions`/`SAMLOptions` are parsed
  (for presence/validation) but never persisted or echoed back, matching
  real AWS's own behavior of never returning credentials on any response.
  `VPCOptions.VPCId`/`AvailabilityZones` are left empty (no EC2 subnet
  lookup modeled), matching services/opensearch's identical simplification.
- `store.go`: `domainCopy` now deep-clones every new option field (plus the
  pre-existing `AdvancedOptions` map, which was previously shallow-copied —
  a real aliasing bug where a caller mutating a `DescribeDomain` result's
  map could mutate backend state; fixed alongside the new fields).
- `packages.go` / `handler_packages.go`: `CreatePackage` now requires
  `PackageSource.S3BucketName`/`S3Key` (`ValidationException` if missing),
  matching `CreatePackageInput.PackageSource` being a required member in
  `types.CreatePackageInput`. Previously flagged as a known gap in the prior
  audit; now closed. The value is stored on `Package` but never echoed back
  (`types.PackageDetails` has no `PackageSource` member — confirmed against
  the SDK).
- **Invented-field deletion**: `validPackageTypes` (models.go) accepted
  `"ZIP-PLUGIN"` in addition to `"TXT-DICTIONARY"`. Checked against
  `aws-sdk-go-v2/service/elasticsearchservice/types.PackageType` — its only
  enum value is `PackageTypeTxtDictionary`. `ZIP-PLUGIN` is valid for the
  *separate* OpenSearch Service API (`opensearch` package's
  `types.PackageType` does have it) but not for this legacy
  `elasticsearchservice` API; gopherstack's value had bled over from the
  sibling service. Deleted per the no-invented-fields rule; a
  `handler_packages_test.go` test case asserting `ZIP-PLUGIN` returned 200
  was corrected to assert 400.
- `handler_domain_config.go`: closed the 2026-07-12 pass's explicitly
  deferred item — `elasticsearchConfigStatus` (backing every
  `DomainConfig.*.Status` field) now carries `CreationDate`/`UpdateDate`
  (epoch-seconds via `pkgs/awstime.Epoch`, matching restjson1's
  `unixTimestamp` wire format) and `UpdateVersion`/`PendingDeletion`,
  matching `types.OptionStatus` exactly. This backend tracks one
  domain-wide `CreatedAt`/`ConfigUpdatedAt`/`ConfigVersion` rather than AWS's
  true per-option granularity (documented as a gap, not a stub — the same
  class of deliberate simplification as the Processing/DomainProcessingStatus
  note below). `ConfigVersion` increments and `ConfigUpdatedAt` advances on
  every `UpdateElasticsearchDomainConfig` call that changes at least one
  field, verified by `TestElasticsearchHandler_DomainConfig_OptionStatus`.

### 2026-07-12 pass: route-matcher bugs found and fixed (both are the
"route-matcher" bug class: unit tests calling `h.Handler()(c)` with a
self-consistent but AWS-wrong path, so green tests hid an unreachable real op)

1. **`ListDomainNames` was served at the wrong path.** AWS routes it at
   `GET /2015-01-01/domain` (no `es/` segment) — confirmed directly from
   `aws-sdk-go-v2/service/elasticsearchservice@v1.39.1`'s
   `awsRestjson1_serializeOpListDomainNames` (`serializers.go`). gopherstack
   had it aliased onto `GET /2015-01-01/es/domain` (the *same* path as
   `CreateElasticsearchDomain`'s POST, just a different verb) — a path that is
   not a real AWS Elasticsearch endpoint at all. A real `aws-sdk-go-v2` client
   calling `ListDomainNames` would 404 against gopherstack (the bare
   `/2015-01-01/domain` path wasn't even matched by the service's
   `RouteMatcher`, so the request wouldn't route to this handler in the first
   place). Fixed by:
   - registering `GET /2015-01-01/domain` in `buildOps()` (reusing the
     existing `elasticsearchDomainPackages` constant, which already covered
     `ListPackagesForDomain`'s `/2015-01-01/domain/{name}/packages` — AWS's ES
     API has two sibling top-level resources, `/es/domain` and `/domain`, and
     this service only modeled the first),
   - broadening `matchElasticsearchExtPaths` to match the bare path (it
     previously only matched `/2015-01-01/domain/...` with a trailing
     segment),
   - moving the `ExtractOperation` mapping from `extractRootDomainOperation`
     (GET case removed — a bare GET on `/es/domain` is not a real op) to
     `extractPackageDomainOp`,
   - removing the dead `handleDomainRoutes` GET-root branch.
   `services/elasticsearch/handler.go:104-112` (buildOps),
   `handler.go:171-183` (matcher), `handler.go:399-421` (extractPackageDomainOp),
   `handler.go:454-472` (extractUpgradeOp — see bug 2), `handler.go:552-560`
   (extractRootDomainOperation), `handler.go:902-917` (handleDomainRoutes).

2. **`StartElasticsearchServiceSoftwareUpdate` was served at the wrong
   path.** AWS routes it at `POST /2015-01-01/es/serviceSoftwareUpdate/start`
   (confirmed from the same serializer file) — gopherstack registered it at
   the *bare* `POST /2015-01-01/es/serviceSoftwareUpdate` (no `/start`
   suffix), which is not a real endpoint (its sibling,
   `CancelElasticsearchServiceSoftwareUpdate`, correctly used `/cancel`). A
   real SDK client's `StartElasticsearchServiceSoftwareUpdate` call would
   404. Fixed by adding the `/start` suffix to the `buildOps()` registration
   and the `ExtractOperation` match in `extractUpgradeOp`.

Both bugs were invisible to the existing unit-test suite because the tests
constructed requests with the same (wrong) path the handler expected —
`handler_test.go`, `handler_refinement1_test.go`, and
`handler_stateful_ops_test.go` all called `GET /2015-01-01/es/domain` for
"ListDomainNames" and `POST /2015-01-01/es/serviceSoftwareUpdate` (no
`/start`) for the software-update start call. Those tests were corrected to
use the real paths (`/2015-01-01/domain` and
`/2015-01-01/es/serviceSoftwareUpdate/start` respectively) as part of this
fix, so they now exercise the actual wire contract instead of the internal
(mis-)implementation.

### Route audit method

Every one of the 51 operations in `GetSupportedOperations()` was
cross-checked method-by-method and path-by-path against
`aws-sdk-go-v2/service/elasticsearchservice@v1.39.1`'s `serializers.go`
(`SplitURI(...)` calls + `request.Method = "..."` assignments are the ground
truth for restjson1 wire routing — more reliable than botocore JSON models
for this exercise since it's the exact code the target SDK client runs). The
two bugs above were the only mismatches; every other op's path prefix, path
parameters, and HTTP verb matched gopherstack's `RouteMatcher` /
`ExtractOperation` / dispatch tables exactly.

### Wire-shape spot checks (all confirmed correct)

- `DescribeVpcEndpoints` / `ListVpcEndpoints` response field names
  (`VpcEndpoints`/`VpcEndpointErrors` vs `VpcEndpointSummaryList`) match
  `types.DescribeVpcEndpointsOutput` / `types.ListVpcEndpointsOutput` exactly
  — these are two different shapes for two different operations and
  gopherstack does not conflate them.
- `DescribeDomainChangeProgress`'s `ChangeProgressStatus.Status` field name
  matches `types.ChangeProgressStatusDetails.Status`.
- `UpgradeElasticsearchDomain`, `GetUpgradeStatus`, `GetUpgradeHistory`,
  `DescribeElasticsearchInstanceTypeLimits` top-level response field names
  all match their respective `api_op_*.go` output structs.
- Domain-status JSON nesting (`DomainStatus` wrapper on
  create/describe/delete, `DomainConfig` wrapper with per-field
  `{Options, Status}` on describe/update-config) matches
  `types.ElasticsearchDomainStatus` / `types.ElasticsearchDomainConfig`.

### Locking / persistence

Coarse `lockmetrics.RWMutex` per backend, consistent with the pkgs-catalog
rule (no per-map locks). `Snapshot`/`Restore` are exposed on `Handler` via
straight delegation to `InMemoryBackend`. Domain `tags.Tags` are explicitly
`.Close()`'d before being discarded on `Reset()` and before being replaced on
`Restore()`, avoiding a Prometheus-metric leak.

### "Looks-wrong-but-correct" traps for the next auditor

- `CreateElasticsearchDomain` / `DescribeElasticsearchDomain` always return
  `Processing: false` and `DomainProcessingStatus: "Active"` immediately —
  this looks like the "domain never finishes creating" stub anti-pattern at
  first glance, but it's actually the *opposite* (and correct) choice for an
  emulator: no artificial async delay, so SDK callers that poll
  `DescribeElasticsearchDomain` waiting for `Processing == false` succeed on
  the first call instead of spinning forever.
- `CancelDomainConfigChange`, `CancelElasticsearchServiceSoftwareUpdate`,
  `DescribeDomainAutoTunes`, `DescribeDomainChangeProgress`,
  `GetUpgradeHistory`, `GetUpgradeStatus` all validate the domain exists and
  then return a fixed/empty payload — these are legitimately void-result ops
  in a backend with no async config-change or auto-tune state machine, not
  disguised stubs (confirmed by reading the corresponding backend.go methods
  before flagging, per parity-principles.md rule 4).
