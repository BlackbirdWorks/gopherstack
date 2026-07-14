---
service: opensearch
sdk_module: aws-sdk-go-v2/service/opensearch@v1.59.0
last_audit_commit: cc66a883
last_audit_date: 2026-07-12
overall: A            # genuine fixes found this pass
ops:
  CreateDomain: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed DomainId (required field, was missing) and IdentityCenterOptions wire key (see Notes)"}
  DescribeDomain: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeDomains: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDomain: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDomainNames: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed wire key EngineVersion->EngineType and value shape (full version string -> engine family); engineType filter param/logic was already correct"}
  UpdateDomainConfig: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed IdentityCenterOptions wire key; added DryRun=true support (previously always mutated even when DryRun requested)"}
  DescribeDomainConfig: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed IdentityCenterOptions wire key"}
  ListTags: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /2021-01-01/tags?arn=; not-found ARN returns empty TagList (no ResourceNotFoundException in SDK op docs) -- verified intentional, not a bug"}
  AddTags: {wire: ok, errors: ok, state: ok, persist: ok}
  RemoveTags: {wire: ok, errors: ok, state: ok, persist: ok}
  CancelDomainConfigChange: {wire: partial, errors: ok, state: ok, persist: ok, note: "deferred: not re-verified against SDK wire shape this pass"}
  StartServiceSoftwareUpdate: {wire: ok, errors: ok, state: ok, persist: ok}
  CancelServiceSoftwareUpdate: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  cross_cluster_connections: {status: deferred, note: "AcceptInboundConnection/RejectInboundConnection/CreateOutboundConnection/etc. skimmed, not wire-verified this pass"}
  vpc_endpoints: {status: deferred, note: "CreateVpcEndpoint/DescribeVpcEndpoints/etc. skimmed, not wire-verified this pass"}
  packages: {status: deferred, note: "CreatePackage/AssociatePackage/etc. skimmed, not wire-verified this pass"}
  applications: {status: deferred, note: "CreateApplication/etc. -- note this op DOES use the legacy lowercase iamIdentityCenterOptions wire key per the SDK (types.IamIdentityCenterOptionsInput), unlike Domain ops; left untouched, only Domain-side IdentityCenterOptions was in scope of the fix this pass"}
  reserved_instances: {status: deferred, note: "not wire-verified this pass"}
  scheduled_actions: {status: deferred, note: "not wire-verified this pass"}
  serverless: {status: deferred, note: "CreateCollection/AccessPolicy/etc. -- separate resource family (2021-11-01 API), not audited this pass"}
  data_sources_direct_query: {status: deferred, note: "not wire-verified this pass"}
gaps:
  - "domain-level connections/VPC-endpoints/packages/applications/serverless families not wire-verified against SDK this pass -- file follow-up bd issue for next sweep"
deferred:
  - cross_cluster_connections
  - vpc_endpoints
  - packages
  - applications
  - reserved_instances
  - scheduled_actions
  - serverless
  - data_sources_direct_query
leaks: {status: clean, note: "no goroutines/janitors in this service; coarse lockmetrics.RWMutex per backend, no per-map locks introduced"}
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
different resources.

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

**Protocol:** restjson1 throughout (confirmed via serializers.go /
`awsRestjson1_*` generated code for every op path referenced this pass).
