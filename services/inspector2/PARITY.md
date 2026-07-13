---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: inspector2
sdk_module: aws-sdk-go-v2/service/inspector2@v1.48.2   # version audited against
last_audit_commit: 1e21a848                             # HEAD when this manifest was written
last_audit_date: 2026-07-12
overall: A            # genuine wire-shape fixes found and applied this pass
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  Enable: {wire: ok, errors: ok, state: ok, persist: ok}
  Disable: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchGetAccountStatus: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass — was resourceStatus/double-nested state.status.status, now resourceState + State objects"}
  CreateFilter: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateFilter: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteFilter: {wire: ok, errors: ok, state: ok, persist: ok}
  ListFilters: {wire: ok, errors: ok, state: ok, persist: ok}
  ListFindings: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass — Finding timestamps now epoch-encoded via findingToWire"}
  GetConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  members: {status: ok, note: "AssociateMember/DisassociateMember/GetMember/ListMembers — fixed this pass: Member.updatedAt now epoch-encoded via memberToWire"}
  delegated_admin: {status: ok, note: "Enable/Disable/Get/ListDelegatedAdminAccounts verified against real shape"}
  organization_configuration: {status: ok, note: "Describe/UpdateOrganizationConfiguration verified"}
  ec2_deep_inspection: {status: ok, note: "Get/Update(Org)Ec2DeepInspectionConfiguration, BatchGet/BatchUpdateMemberEc2DeepInspectionStatus verified; no timestamp fields exposed"}
  encryption_key: {status: ok, note: "Get/Reset/UpdateEncryptionKey verified"}
  cis_scan_configuration: {status: ok, note: "Create/Delete/Update/ListCisScanConfigurations verified"}
  cis_session: {status: ok, note: "Start/Stop/SendHealth/SendTelemetry verified; CisSession.startedAt never serialized to the wire so no epoch fix needed"}
  cis_scan_results: {status: ok, note: "GetCisScanReport/GetCisScanResultDetails/ListCisScans/ListCisScanResultsAggregatedBy{Checks,TargetResource} — fixed this pass: ListCisScans emitted a fabricated 'scheduledBy' RFC3339 string (real ScheduledBy is an unrelated *string* field, account/org id); renamed to the real 'scanDate' timestamp field and epoch-encoded it"}
  code_security_integration: {status: partial, note: "Get/ListCodeSecurityIntegrations — fixed this pass: renamed createdAt/updatedAt to real createdOn/lastUpdateOn and epoch-encoded them. Create/Update responses only echo arn+status (safe). Gap: 'details' request payload is accepted but discarded (_ = details) — repository/provider connection details are not modeled"}
  code_security_scan_configuration: {status: partial, note: "Get/ListCodeSecurityScanConfigurations — fixed this pass: Get's updatedAt renamed to real lastUpdatedAt and both createdAt/lastUpdatedAt epoch-encoded (createdAt was a wire-breaking type bug: real GetCodeSecurityScanConfigurationOutput.createdAt is a DateTimeTimestamp). Gap (not fixed, out of scope this pass): real shape nests scan settings under a top-level 'configuration' object with ruleSetCategories/continuousIntegrationScanConfiguration/periodicScanConfiguration/level, and ListCodeSecurityScanConfigurations' summary shape (ownerAccountId, ruleSetCategories, continuousIntegrationScanSupportedEvents, cron schedule) has no relation to Get's shape at all — this backend models a simplified, internally-consistent shape instead. See gaps below"}
  code_security_scan_associations: {status: ok, note: "Batch(Dis)AssociateCodeSecurityScanConfiguration, ListCodeSecurityScanConfigurationAssociations, Start/GetCodeSecurityScan — no timestamp fields exposed"}
  findings_report: {status: ok, note: "Create/Cancel/GetFindingsReportStatus verified; createdAt/destination/filterCriteria/format not exposed (gap, low-traffic async-job status op)"}
  sbom_export: {status: ok, note: "Create/Cancel/GetSbomExport verified; same createdAt-not-exposed gap as findings_report"}
  coverage: {status: gap, note: "ListCoverage/ListCoverageStatistics are hardwired empty (pre-existing, not fixed this pass — no coverage-entry seeding capability exists yet, unlike Finding's SeedFinding)"}
  finding_aggregations: {status: ok, note: "ListFindingAggregations reports real per-account severity breakdown when findings are seeded"}
  usage_totals: {status: gap, note: "ListUsageTotals returns a fixed empty-usage stub (pre-existing, not fixed this pass)"}
  account_permissions: {status: gap, note: "ListAccountPermissions always returns empty (pre-existing, not fixed this pass)"}
  vulnerability_search: {status: gap, note: "SearchVulnerabilities always returns empty (pre-existing, not fixed this pass)"}
  batch_get_code_snippet: {status: gap, note: "always returns empty results (pre-existing, not fixed this pass)"}
  batch_get_finding_details: {status: gap, note: "always returns empty results (pre-existing, not fixed this pass)"}
  batch_get_free_trial_info: {status: ok, note: "fixed this pass — start/end were RFC3339 strings on FreeTrialInfo's required DateTimeTimestamp members (wire-breaking: real deserializer errors 'expected Timestamp to be a JSON Number, got string'); now epoch-encoded"}
  get_clusters_for_image: {status: gap, note: "always returns empty clusters list (pre-existing, not fixed this pass)"}
gaps:
  - "CodeSecurityScanConfiguration Get/List responses use a simplified, internally-consistent shape that diverges structurally from the real API (missing nested 'configuration'/ruleSetCategories/level/continuousIntegrationScanConfiguration; List summary shape has no relation to Get's shape at all in real AWS). Full reshape is a substantial, separate effort — file a bd issue before attempting (gopherstack: file follow-up)."
  - "ListCoverage/ListCoverageStatistics, ListUsageTotals, ListAccountPermissions, SearchVulnerabilities, BatchGetCodeSnippet, BatchGetFindingDetails, GetClustersForImage are all disguised no-ops (hardwired empty/stub responses) predating this audit pass. Lower priority than the wire-shape bugs fixed here since they don't crash real clients, but each is a genuine parity gap — Finding already has a SeedFinding precedent these could follow."
  - "CreateFindingsReport/CreateSbomExport results (GetFindingsReportStatus/GetSbomExport) omit createdAt/destination/filterCriteria/errorMessage fields present in the real API. Not wire-breaking (real client just sees them as unset) but incomplete."
  - "CreateFilter/CreateCisScanConfiguration/CreateCodeSecurityIntegration/CreateCodeSecurityScanConfiguration 'name' fields are not validated against AWS's length/charset constraints (e.g. filter name: 3-64 chars, alnum/dot/underscore/dash). Real AWS returns ValidationException for violations; this backend accepts anything non-empty."
deferred:
  - "Full CIS session lifecycle semantics (health/telemetry payload validation, session expiry) — accepted as no-ops, not audited for correctness beyond routing/basic state."
leaks: {status: clean, note: "no goroutines/janitors in this service; all resource maps are store.Table-backed and cleared by Reset/registry.ResetAll"}
---

## Notes

Protocol: restjson1. All request/response bodies are JSON; most ops are POST with
an explicit action path (e.g. `/findings/list`), a handful use GET/PUT/DELETE
(GetEncryptionKey=GET, Reset/UpdateEncryptionKey=PUT, StartCisSession/StopCisSession/
SendCisSessionHealth/SendCisSessionTelemetry=PUT, TagResource=POST,
UntagResource=DELETE, ListTagsForResource=GET on `/tags/{arn}`).
The route matcher (`RouteMatcher`/`classifyPath`/`classifyAppendixAPath`) was
cross-checked op-by-op against `aws-sdk-go-v2/service/inspector2@v1.48.2`'s
serializers.go (method + SplitURI path per op) this pass: all 75 routed ops
(13 base + 62 appendix-A) match the real SDK's method+path exactly. No
route-matcher bugs found (this class of bug hit other services in prior
sweeps but this service's matcher was already correct).

### Timestamp wire format (the recurring bug class this pass)

Inspector2's restjson1 protocol requires **epoch-seconds JSON numbers** for
every DateTimeTimestamp member (see `pkgs/awstime.Epoch`). Every domain
struct in this backend stores timestamps as `time.Time` for easy backend
arithmetic, which is fine internally, but several handlers were marshaling
those structs (or ad-hoc maps built from them) **directly** via
`encoding/json`, which renders `time.Time` as an RFC3339 string by
default. A real SDK client hitting one of these ops gets either a hard
deserialization error (`expected Timestamp to be a JSON Number, got string
instead` — client-breaking) or a silently-unpopulated field (when the wire
key doesn't match the real field name at all, so the string is just
ignored as an unrecognized key).

Fixed this pass (client-breaking, confirmed via the SDK's own
`deserializers.go` case blocks):
- `BatchGetFindingDetails`/`ListFindings` response `findings[]` —
  firstObservedAt/lastObservedAt/updatedAt (`findingToWire` in handler.go).
- `GetMember`/`ListMembers` response — `member(s).updatedAt`
  (`memberToWire` in handler_appendixa.go).
- `BatchGetFreeTrialInfo` response — `freeTrialInfo[].start`/`.end`.
- `GetCodeSecurityScanConfiguration`'s `createdAt` (key name matched the
  real field, but the emitted type didn't — genuinely client-breaking).

Fixed this pass (non-crashing but silently-wrong — wrong key name so the
real field is unpopulated rather than erroring):
- `ListCisScans` emitted `"scheduledBy": <RFC3339 string>`; the real
  `CisScan.ScheduledBy` is an unrelated `*string` (the scheduling
  account/org id, which this backend does not track) and the real
  timestamp field is `scanDate`. Renamed + epoch-encoded rather than
  fabricating a `scheduledBy` value we have no data for.
- `GetCodeSecurityIntegration`/`ListCodeSecurityIntegrations` used
  `createdAt`/`updatedAt`; real field names are `createdOn`/`lastUpdateOn`.
- `GetCodeSecurityScanConfiguration`'s `updatedAt` key; real field name is
  `lastUpdatedAt` (createdAt's key name was already correct — see above).

`ListFilters` already had this right (see `epochSeconds` — now
`pkgs/awstime.Epoch` after dedup; a hand-rolled duplicate of that pkg
function was removed from handler.go this pass, see pkgs-catalog.md's
reuse-don't-reimplement rule). That existing pattern (build a `map[string]any`
in the handler rather than relying on the domain struct's default JSON
marshaling) is now applied consistently everywhere a timestamped domain
struct reaches the wire. **Trap for the next auditor**: any *new* handler
that does `c.JSON(status, someDomainStruct)` or `c.JSON(status,
someDomainStructSlice)` where the struct has a `time.Time` field is
reintroducing this bug class — route it through a `*ToWire` builder instead.

### BatchGetAccountStatus vs Enable/Disable (looks-wrong-but-correct trap avoided)

These two response families look almost identical but are genuinely
different AWS shapes — don't conflate them:
- `Enable`/`Disable` return `Account` objects: a **flat**
  `resourceStatus: {ec2, ecr, lambda, ...}` of bare Status strings, plus a
  top-level `status` that is itself a bare Status string.
- `BatchGetAccountStatus` returns `AccountState` objects: `resourceState`
  nests a full `State` object (`status`/`errorCode`/`errorMessage`) per
  resource type, and the top-level `state` is itself a `State` object, not
  a bare string.

Before this pass, `handleBatchGetAccountStatus` used the *Enable/Disable*
shape's flat `resourceStatus` key name, and its `state` field was
double-nested (`state.status.status`) — neither matches AccountState.
Fixed via `buildState`/`buildResourceState` in handler.go, keeping
`buildResourceStatus` (the flat shape) only for Enable/Disable.

### Persistence

`Handler.Snapshot`/`Restore` delegate to `InMemoryBackend.Snapshot`/`Restore`
(persistence.go), which drive every `store.Table`-backed resource through
`registry.SnapshotAll()`/`RestoreAll()` plus hand-carried raw maps (tags,
enabledTypes, codeSecurityScans) and single-struct config fields. Verified
this pass: no field on `InMemoryBackend` is missing from the snapshot.
`inspector2SnapshotVersion` (currently 1) guards against decoding an
incompatible/older snapshot shape.

Deliberately **not** touched this pass: the domain structs' own JSON tags
(`Finding.FirstObservedAt json:"firstObservedAt"` etc.) still marshal as
RFC3339 for persistence snapshots — this is correct and must stay that way.
The epoch-encoding fixes above are all in the *handler* layer (wire
responses), never in the domain struct's default marshaling, specifically
so the on-disk snapshot format is untouched by this pass.
