---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: elasticsearch
sdk_module: aws-sdk-go-v2/service/elasticsearchservice@v1.39.1
last_audit_commit: 59ab8f6a
last_audit_date: 2026-07-12
overall: A            # 2 route-matcher bugs found that made real ops unreachable via the SDK
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateElasticsearchDomain: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeElasticsearchDomain: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeElasticsearchDomains: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteElasticsearchDomain: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDomainNames: {wire: ok, errors: ok, state: ok, persist: ok, note: "route bug fixed this pass -- was served at the wrong path; see Notes"}
  UpdateElasticsearchDomainConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeElasticsearchDomainConfig: {wire: ok, errors: ok, state: ok, persist: ok}
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
  CreatePackage: {wire: partial, errors: ok, state: ok, persist: ok, note: "PackageSource (S3 bucket/key) not modeled/validated -- see gaps"}
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
  - "CreatePackage does not require/validate PackageSource (S3Bucket/S3Key), unlike real AWS \
     (ValidationException if missing). Backend just ignores it; no client-visible incorrect \
     behavior since the field is never read back, but a strict-input test suite would fail. \
     Not filed as a bd issue this pass (low traffic op, no observed consumer)."
  - "Domains never transition through a Processing/creating state -- CreateElasticsearchDomain \
     returns Processing=false / DomainProcessingStatus=Active immediately. This is a deliberate \
     simplification (no artificial async delay) rather than a stub; flagging so a future \
     auditor doesn't mistake the always-Active state for a bug in the other direction."
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - "Nested field-by-field wire verification of DescribeElasticsearchDomainConfig's per-option \
     Status sub-objects (UpdateVersion, PendingDeletion, CreationDate/UpdateDate epoch fields) \
     was skipped -- top-level Options/Status shape confirmed correct against \
     types.ElasticsearchDomainConfig, but AWS's per-field OptionStatus also carries \
     CreationDate/UpdateDate/UpdateVersion/State/PendingDeletion that gopherstack's \
     elasticsearchConfigValue does not emit. Low risk: consumers (Terraform provider, CLI) key \
     off Options, not the timestamps."
leaks: {status: clean, note: "no goroutines/janitors in this service; Snapshot/Restore close domain Tags before replacing state (verified in persistence.go)"}
---

## Notes

Protocol: **restjson1**. Base path prefix `/2015-01-01/`.

### Bugs found and fixed this pass (both are the "route-matcher" bug class:
unit tests calling `h.Handler()(c)` with a self-consistent but AWS-wrong path,
so green tests hid an unreachable real op)

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
