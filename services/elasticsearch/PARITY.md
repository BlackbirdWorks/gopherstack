---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: elasticsearch
sdk_module: aws-sdk-go-v2/service/elasticsearchservice@v1.39.1
last_audit_commit: 59ab8f6a
last_audit_date: 2026-07-24
overall: A            # field-diff pass: CreateElasticsearchDomain field gaps closed, CreatePackage PackageSource gap closed, one invented field deleted
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateElasticsearchDomain: {wire: ok, errors: ok, state: ok, persist: ok, note: "this pass added VPCOptions/CognitoOptions/AdvancedSecurityOptions/AutoTuneOptions/LogPublishingOptions/TagList, previously entirely unmodeled -- see Notes"}
  DescribeElasticsearchDomain: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeElasticsearchDomains: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteElasticsearchDomain: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDomainNames: {wire: ok, errors: ok, state: ok, persist: ok, note: "route bug fixed this pass -- was served at the wrong path; see Notes"}
  UpdateElasticsearchDomainConfig: {wire: ok, errors: ok, state: ok, persist: ok, note: "this pass added VPCOptions/CognitoOptions/AdvancedSecurityOptions/AutoTuneOptions/LogPublishingOptions"}
  DescribeElasticsearchDomainConfig: {wire: ok, errors: ok, state: ok, persist: ok, note: "this pass added per-field OptionStatus CreationDate/UpdateDate/UpdateVersion/PendingDeletion -- previously only State was modeled; see Notes"}
  CancelDomainConfigChange: {wire: ok, errors: ok, state: ok, persist: ok, note: "synchronous backend, so this is correctly a no-op read-back"}
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
  CreatePackage: {wire: ok, errors: ok, state: ok, persist: ok, note: "this pass added required PackageSource (S3BucketName/S3Key) validation; also deleted invented ZIP-PLUGIN package type -- see Notes"}
  DescribePackages: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdatePackage: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePackage: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociatePackage: {wire: ok, errors: ok, state: ok, persist: ok}
  DissociatePackage: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPackageVersionHistory: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListDomainsForPackage: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListPackagesForDomain: {wire: ok, errors: ok, state: ok, persist: n/a}
  CreateVpcEndpoint: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeVpcEndpoints: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListVpcEndpoints: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListVpcEndpointsForDomain: {wire: ok, errors: ok, state: ok, persist: n/a}
  UpdateVpcEndpoint: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteVpcEndpoint: {wire: ok, errors: ok, state: ok, persist: ok}
  AuthorizeVpcEndpointAccess: {wire: ok, errors: ok, state: ok, persist: ok}
  RevokeVpcEndpointAccess: {wire: ok, errors: ok, state: ok, persist: ok}
  ListVpcEndpointAccess: {wire: ok, errors: ok, state: ok, persist: n/a}
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
     returns Processing=false / DomainProcessingStatus=Active immediately. This is a deliberate \
     simplification (no artificial async delay) rather than a stub; flagging so a future \
     auditor doesn't mistake the always-Active state for a bug in the other direction."
  - "AdvancedSecurityOptions.SAMLOptions and AutoTuneOptions.MaintenanceSchedules are accepted \
     on the wire (parsed as json.RawMessage so unmarshal doesn't reject them) but not modeled \
     or persisted -- SAML SSO and maintenance-window scheduling have no backend state machine. \
     MasterUserOptions (master username/password/ARN) is intentionally never persisted or \
     echoed back either, matching real AWS's own behavior of never returning credentials on \
     any Describe/Create/Update response; the only gap is that this backend also can't act on \
     internal-user-database auth using those credentials. Not filed as a bd issue this pass \
     (no observed consumer exercises SAML or credential-checked internal auth against this \
     emulator)."
  - "VPCOptions.VPCId and .AvailabilityZones are never populated on Describe/domain-status \
     responses -- deriving them would require a cross-service EC2 subnet/VPC lookup this \
     backend does not perform (SubnetIds/SecurityGroupIds are correctly modeled and echoed). \
     Matches services/opensearch's identical, already-accepted simplification."
  - "CreatePackageInput.DeploymentStrategyOptions (on CreateElasticsearchDomain) is not \
     modeled at all -- not in the explicit field list this pass targeted and no observed \
     consumer depends on it."
  - "Package.CreatedAt/LastUpdatedAt/ErrorDetails (types.PackageDetails) are not modeled -- \
     packages are always AVAILABLE synchronously in this backend (no COPYING/COPY_FAILED \
     state machine), so there is no natural timestamp/error-detail source. Not filed as a bd \
     issue this pass (low traffic op)."
deferred: []              # this pass's target deferred item (DescribeElasticsearchDomainConfig per-field OptionStatus) is now implemented; remaining edges tracked under gaps above
leaks: {status: clean, note: "no goroutines/janitors in this service; Snapshot/Restore close domain Tags before replacing state (verified in persistence.go). This pass also fixed domainCopy (store.go) to deep-clone AdvancedOptions/VPCOptions/CognitoOptions/AdvancedSecurityOptions/AutoTuneOptions/LogPublishingOptions -- previously AdvancedOptions (and now the five new option fields) were shallow-copied, so a caller mutating the map/slice on a DescribeDomain result would have silently mutated the backend's stored state. Not a resource leak, but a real aliasing bug fixed alongside the new fields it would otherwise have applied to as well."}
---

## Notes

Protocol: **restjson1**. Base path prefix `/2015-01-01/`.

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
