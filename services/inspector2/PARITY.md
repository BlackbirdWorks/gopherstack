---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: inspector2
sdk_module: aws-sdk-go-v2/service/inspector2@v1.53.0   # version audited against
last_audit_commit: 9e3baacb5                            # HEAD when this manifest was written
last_audit_date: 2026-07-29
overall: A            # code_security_scan_configuration reshaped to the real nested 'configuration' wire shape (Create/Get/Update/List), fabricated 'status'/'integrationArn' members deleted, required level/ruleSetCategories now validated; no prior family regressed
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  Enable: {wire: ok, errors: ok, state: ok, persist: ok}
  Disable: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchGetAccountStatus: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateFilter: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this pass — name now validated against AWS's real 3-64 char, alnum/dot/underscore/dash constraint (previously accepted any non-empty string)"}
  UpdateFilter: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteFilter: {wire: ok, errors: ok, state: ok, persist: ok}
  ListFilters: {wire: ok, errors: ok, state: ok, persist: ok}
  ListFindings: {wire: ok, errors: ok, state: ok, persist: ok}
  GetConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  members: {status: ok, note: "unchanged this pass"}
  delegated_admin: {status: ok, note: "unchanged this pass"}
  organization_configuration: {status: ok, note: "unchanged this pass"}
  ec2_deep_inspection: {status: ok, note: "unchanged this pass"}
  encryption_key: {status: ok, note: "unchanged this pass"}
  cis_scan_configuration: {status: ok, note: "unchanged this pass"}
  cis_session: {status: ok, note: "unchanged this pass"}
  cis_scan_results: {status: ok, note: "unchanged this pass"}
  code_security_integration: {status: partial, note: "unchanged this pass — Get/ListCodeSecurityIntegrations verified NOT to expose 'details' on the wire in real AWS either (GetCodeSecurityIntegrationOutput has no details member), so the prior gap note was overstated: the real gap is only CreateCodeSecurityIntegrationOutput's optional 'authorizationUrl' member (OAuth callback URL for GitHub/GitLab-type integrations), which this backend never returns. Left open: gopherstack has no OAuth flow to derive a real authorization URL from, and fabricating one would be worse than omitting it."}
  code_security_scan_configuration: {status: ok, note: "fixed this pass — Create/Get/UpdateCodeSecurityScanConfiguration now nest ruleSetCategories/periodicScanConfiguration/continuousIntegrationScanConfiguration under a required top-level 'configuration' object with level/scopeSettings/name/tags as siblings (confirmed via serializers.go's awsRestjson1_serializeDocumentCodeSecurityScanConfiguration and deserializers.go's Get output deserializer), replacing the prior flat request/response decoding that silently dropped every real client's configuration.* fields. level (required, ACCOUNT|ORGANIZATION) and configuration.ruleSetCategories (required, non-empty, SAST|IAC|SCA) are now validated with ValidationException, where the prior handler defaulted both silently. Deleted the fabricated 'status'/'integrationArn' members: neither exists on the real CodeSecurityScanConfiguration/GetCodeSecurityScanConfigurationOutput shape (field-diffed against api_op_GetCodeSecurityScanConfiguration.go — no such members). UpdateCodeSecurityScanConfiguration now only accepts the nested 'configuration' object (matching UpdateCodeSecurityScanConfigurationInput's real shape — no top-level scopeSettings on update, that member is create-time-only) and re-validates ruleSetCategories on every update, where the prior handler silently accepted a top-level scopeSettings update the real API has no field for. ListCodeSecurityScanConfigurations now emits the real, structurally distinct CodeSecurityScanConfigurationSummary shape (flat ownerAccountId/ruleSetCategories/continuousIntegrationScanSupportedEvents/frequencyExpression/periodicScanFrequency/scopeSettings, no nested 'configuration', no tags/createdAt/level) under the real 'configurations' wire key, confirmed via deserializers.go — the prior handler reused Get's full shape under the wrong 'scanConfigurations' key (that key belongs to the unrelated CIS/connector scan-configuration list endpoints), so a real ListCodeSecurityScanConfigurations client's Configurations field was never populated at all."}
  code_security_scan_associations: {status: ok, note: "unchanged this pass"}
  findings_report: {status: ok, note: "fixed this pass — CreateFindingsReport now accepts and stores filterCriteria/reportFormat (previously discarded/unparsed); GetFindingsReportStatus now echoes destination/filterCriteria/errorMessage (real GetFindingsReportStatusOutput wire keys: destination/errorCode/errorMessage/filterCriteria/reportId/status — confirmed via deserializers.go there is NO createdAt member on the real output at all, correcting the prior audit's gap note)"}
  sbom_export: {status: ok, note: "fixed this pass — CreateSbomExport's request field was the gopherstack-invented 'sbomFormat' key; real CreateSbomExportInput field is 'reportFormat' (confirmed via serializers.go), so every real client's report format was silently dropped. Now reads reportFormat + resourceFilterCriteria, and GetSbomExport echoes format/s3Destination/filterCriteria/errorMessage (real GetSbomExportOutput wire keys, confirmed via deserializers.go; also no createdAt member in the real output)"}
  coverage: {status: ok, note: "fixed this pass — ListCoverage/ListCoverageStatistics were hardwired empty stubs; added SeedCoverage (store.Table[CoverageEntry], real CoveredResource wire shape incl. epoch-encoded lastScannedAt) following the SeedFinding precedent. ListCoverage supports the real accountId/resourceId/resourceType/scanType string filters + pagination. ListCoverageStatistics supports the real groupBy request field (ACCOUNT_ID/RESOURCE_TYPE/SCAN_STATUS_CODE; ECR_REPOSITORY_NAME not modeled, would require the nested ResourceMetadata union) with real per-group counts. Not modeled: CoverageFilterCriteria's tag/date/number-range filter facets, and CoveredResource.resourceMetadata (nested Ec2/EcrImage/EcrRepository/LambdaFunction/CodeRepository metadata union) — both real but omitted (unset on the wire, not wire-breaking)"}
  finding_aggregations: {status: ok, note: "unchanged this pass"}
  usage_totals: {status: ok, note: "fixed this pass — ListUsageTotals now derives real per-account Usage entries (real UsageTotal/Usage wire shape: currency/estimatedMonthlyCost/total/type) from which resource types are Enable'd and how many SeedCoverage entries of each scan type exist, replacing the prior hardwired-empty-usage stub. estimatedMonthlyCost is a documented deterministic placeholder rate (gopherstack has no metering engine and real Inspector pricing is not reproducible in a mock) — the wire shape and field names are what parity requires here, not the dollar amount"}
  account_permissions: {status: ok, note: "fixed this pass — deleted the gopherstack-invented 'status' field from AccountPermission (real Permission shape is operation/service, confirmed via deserializers.go; there is no 'status' member on the real type at all). ListAccountPermissions now returns the real Operation x Service permission matrix (ENABLE_SCANNING/DISABLE_SCANNING/ENABLE_REPOSITORY/DISABLE_REPOSITORY x EC2/ECR/LAMBDA), narrowed by the optional service filter, replacing the prior hardwired-empty stub"}
  vulnerability_search: {status: ok, note: "fixed this pass — deleted the gopherstack-invented 'vulnerabilityId'/'severity' Vulnerability fields (real wire keys are 'id'/'vendorSeverity', confirmed via deserializers.go). Added SeedVulnerability (store.Table[Vulnerability]) following the SeedFinding precedent: real SearchVulnerabilities queries AWS's own global vulnerability intelligence database, which gopherstack has no data source for, so results only ever come from explicitly seeded IDs — real SearchVulnerabilitiesFilterCriteria.vulnerabilityIds is a required exact-ID lookup list, not a free-text query, so this is a faithful (not simplified) model of the real request contract. Not modeled: the nested AtigData/CisaData/Cvss2/Cvss3/Cvss4/Epss/ExploitObserved objects (real but omitted — unset on the wire, not wire-breaking)"}
  batch_get_code_snippet: {status: ok, note: "fixed this pass — added SeedCodeSnippet (store.Table[codeSnippet]) following the SeedFinding precedent: gopherstack has no static analysis engine to derive real code snippet content, so BatchGetCodeSnippet returns seeded content for a finding ARN, or a CODE_SNIPPET_NOT_FOUND error entry (the only CodeSnippetErrorCode meaningful here) for any ARN with none seeded — replacing the prior stub, which silently ignored its input entirely and always returned two empty lists regardless of what was asked for"}
  batch_get_finding_details: {status: ok, note: "fixed this pass — the request handler decoded findingArns into []map[string]any; real BatchGetFindingDetailsInput.findingArns is a plain string array (confirmed via api_op_BatchGetFindingDetails.go), so every real client request of the form {\"findingArns\":[\"arn1\",\"arn2\"]} failed json.Unmarshal with a ValidationException (client-breaking). Fixed to []string. Finding gained optional epssScore/riskScore/cwes/referenceUrls/tools fields (real FindingDetail shape) settable via SeedFinding; BatchGetFindingDetails now returns them for findings that exist, or a FINDING_DETAILS_NOT_FOUND error entry for ARNs that don't, replacing the prior always-empty stub. Not modeled: CisaData/Evidences/ExploitObserved/Ttps nested objects (real but omitted)"}
  batch_get_free_trial_info: {status: ok, note: "unchanged this pass"}
  get_clusters_for_image: {status: ok, note: "fixed this pass — two wire bugs: (1) the request handler decoded a bare 'filterCriteria' map, but real GetClustersForImageInput nests the required resourceId under a 'filter' object (ClusterForImageFilterCriteria), confirmed via serializers.go, so the value was silently dropped on every real request and the required-field validation never ran; (2) the response used 'clusters' but the real wire key is 'cluster' (singular), confirmed via deserializers.go, so a real client's Cluster field was never populated. Now validates the required filter.resourceId (ValidationException if absent) and emits the correct 'cluster' key. Still returns an empty cluster list always: gopherstack has no ECS/EKS cluster-membership tracking to join an ECR image against, and fabricating cluster ARNs would be worse than an honest empty (but now correctly-keyed and validated) result — see gaps below"}
  connectors: {status: ok, note: "new this pass — CreateConnector/UpdateConnector/DeleteConnector/ListConnectors added for the inspector2@v1.53.0 SDK bump. Real ConnectorCloudProvider has exactly one value, AZURE (confirmed via types/enums.go) — there is no GitHub/GitLab connector type in the real API despite that being a natural guess from the 'connector' name; code-repository integrations are the separate, pre-existing CodeSecurityIntegration family, unaffected by this pass. CreateConnectorOutput/UpdateConnectorOutput field-diffed against api_op_CreateConnector.go/api_op_UpdateConnector.go: each returns only connectorArn (confirmed asynchronous — no full Connector echo), which this backend matches rather than inventing a fuller response. Connector wire shape field-diffed against deserializers.go's awsRestjson1_deserializeDocumentConnector: createdAt/updatedAt/health.lastCheckedAt are real 'date-time' (RFC 3339 string, parsed via smithytime.ParseDateTime) timestamps, NOT the unixTimestamp epoch-seconds shape pkgs/awstime.Epoch targets elsewhere in this service — confirmed against the deserializer instead of assumed, avoiding a wire bug class this campaign has hit in other services. Connector authorization lifecycle modeled honestly per this campaign's finding (also hit by securityhub): real ConnectorHealthStatus includes PENDING_AUTHORIZATION for an unfinished external Azure AD app-consent (OAuth) flow, and none of the 6 connector SDK ops drive or observe that step, so this backend creates connectors at EnablementStatus=PENDING_ENABLEMENT / Health.ConnectorStatus=PENDING_AUTHORIZATION and never auto-advances either (UpdateConnector moves EnablementStatus to PENDING_UPDATE, still never auto-resolving to ENABLED/CONNECTED). DeleteConnector's real PENDING_DELETION EnablementStatus value is not modeled: there is no GetConnector operation through which a caller could ever observe an in-between state, so this backend completes the delete synchronously rather than leaving the connector permanently listed as 'pending' and unobservably undeleted. ListConnectors' filterCriteria supports provider/connectorArns/awsConfigConnectorArns (each real filter's Comparison enum has exactly one value, EQUALS, confirmed via types/enums.go) — accounts (meaningless in this single-account emulator) and connectorType (no corresponding field on the real Connector response type to filter against at all) are not modeled, documented rather than silently ignored, following the coverage/vulnerability_search precedent for omitted filter facets."}
  connector_scan_configuration: {status: ok, note: "new this pass — ListConnectorScanConfigurations/UpdateConnectorScanConfiguration added for the inspector2@v1.53.0 SDK bump. There is no CreateConnectorScanConfiguration operation in the real API (confirmed via `go doc .../inspector2`); UpdateConnectorScanConfiguration is the sole write path, keyed by awsConfigConnectorArn rather than connectorArn (confirmed via serializers.go's awsRestjson1_serializeOpDocumentUpdateConnectorScanConfigurationInput). UpdateConnectorScanConfiguration validates that at least one Connector carries the given awsConfigConnectorArn, returning ResourceNotFoundException for an unrecognized one rather than accepting any ID, per this campaign's explicit requirement to validate the connector actually exists. ConnectorScanConfigurationItem's connectorArns member is derived live from the connectors table's byAwsConfigArn secondary index at read time (not stored alongside the scan configuration), matching that it is a live join in the real API, confirmed via deserializers.go's awsRestjson1_deserializeDocumentConnectorScanConfigurationItem."}
gaps:
  - "ListConnectors' ConnectorFilterCriteria.accounts/connectorType facets are not modeled (accounts is meaningless in this single-account emulator; connectorType — CUSTOMER_MANAGED/SERVICE_LINKED — has no corresponding field on the real Connector response type to filter against at all, confirmed via types/types.go). Only provider/connectorArns/awsConfigConnectorArns are supported."
  - "Connector's real PENDING_DELETION EnablementStatus value and ScopeConfiguration's real ACTIVE/ERROR/DISABLED State values are never reached: this backend's connectors never leave PENDING_AUTHORIZATION (no out-of-band Azure OAuth step exists in the SDK to drive them further), so DeleteConnector completes synchronously and every submitted scope setting is always reported PENDING. Both are deliberate, documented simplifications of an inherently external-system-dependent async lifecycle — see the connectors family note above."
  - "CreateCodeSecurityIntegrationOutput's optional 'authorizationUrl' member (real API: OAuth callback URL for GitHub/GitLab-type integrations) is never returned. gopherstack has no OAuth flow to derive a real URL from; omitting it is unset-on-the-wire, not wire-breaking."
  - "GetClustersForImage always returns an empty (but now correctly-keyed, request-validated) cluster list: gopherstack has no ECS/EKS cluster-membership tracking to join an ECR image resourceId against — re-confirmed this pass: neither services/ecs nor services/eks track any image-to-cluster membership state to join against. Would need a SeedClustersForImage capability plus real ECS/EKS service cross-references to close for real; lower priority than the wire-shape bugs already fixed since GetClustersForImage is a low-traffic informational op."
  - "CreateCisScanConfiguration/CreateCodeSecurityIntegration/CreateCodeSecurityScanConfiguration 'name' fields are still not validated against AWS's exact length/charset constraints (unlike CreateFilter, fixed in an earlier pass) — the real per-op constraints were not confirmed against SDK validation-trait metadata this pass. Real AWS returns ValidationException for violations; this backend accepts anything non-empty. Low severity (a client sending an invalid name simply gets a permissive accept instead of a client-side-preventable error)."
  - "CodeSecurityScanConfiguration.scopeSettings and .periodicScanConfiguration/.continuousIntegrationScanConfiguration are still accepted as loosely-typed map[string]any pass-throughs rather than validated against ScopeSettings.projectSelectionScope's (ALL|SPECIFIC) / PeriodicScanConfiguration.frequency's (WEEKLY|MONTHLY|NEVER) / ContinuousIntegrationScanConfiguration.supportedEvents's (PULL_REQUEST|PUSH) real enum constraints — only the outer 'configuration' nesting, required level, and required ruleSetCategories enum were fixed this pass. Real AWS returns ValidationException for enum violations; this backend accepts any value."
  - "CoverageFilterCriteria's tag/date/number-range filter facets (ec2InstanceTags, ecrImageLastInUseAt, lastScannedAt date ranges, etc.) and CoveredResource.resourceMetadata (nested per-resource-type metadata union) are real but not modeled by SeedCoverage/ListCoverage — only the string-comparison facets (accountId/resourceId/resourceType/scanType) are supported."
  - "Vulnerability's nested AtigData/CisaData/Cvss2/Cvss3/Cvss4/Epss/ExploitObserved objects and FindingDetail's CisaData/Evidences/ExploitObserved/Ttps objects are real but not modeled — only scalar/list fields are seedable via SeedVulnerability/SeedFinding."
deferred:
  - "Full CIS session lifecycle semantics (health/telemetry payload validation, session expiry) — accepted as no-ops, not audited for correctness beyond routing/basic state."
leaks: {status: clean, note: "no goroutines/janitors in this service; all resource maps (including the 3 new tables added this pass: coverageEntries, vulnerabilities, codeSnippets) are store.Table-backed and cleared by Reset/registry.ResetAll; every new Lock/RLock call site pairs with an immediately-following defer Unlock/RUnlock"}
---

## Notes

Protocol: restjson1. All request/response bodies are JSON; most ops are POST with
an explicit action path (e.g. `/findings/list`), a handful use GET/PUT/DELETE
(GetEncryptionKey=GET, Reset/UpdateEncryptionKey=PUT, StartCisSession/StopCisSession/
SendCisSessionHealth/SendCisSessionTelemetry=PUT, TagResource=POST,
UntagResource=DELETE, ListTagsForResource=GET on `/tags/{arn}`).
The route matcher (`RouteMatcher`/`classifyPath`/`classifyExtendedPath`) was
cross-checked op-by-op against `aws-sdk-go-v2/service/inspector2@v1.48.2`'s
serializers.go (method + SplitURI path per op) in a prior pass: all 75 routed
ops (13 base + 62 extended) matched the real SDK's method+path exactly. This
pass adds 6 more (all extended/POST-body-dispatched, matching this package's
existing convention): CreateConnector (`/connector/create`), UpdateConnector
(`/connector/update`), DeleteConnector (`/connector/delete`), ListConnectors
(`/connector/list`), ListConnectorScanConfigurations
(`/connectorscanconfigurations/list`), UpdateConnectorScanConfiguration
(`/connectorscanconfiguration/update`) — every path cross-checked against
`aws-sdk-go-v2/service/inspector2@v1.53.0`'s serializers.go
`httpbinding.SplitURI(...)` call for that op, confirming an exact match
(including the plural/singular `connectorscanconfigurations` vs
`connectorscanconfiguration` path segments, which are easy to transpose).
The handler now routes 81 ops total (13 base + 68 extended).

### Connectors and connector scan configuration (new this pass)

The Go SDK module was bumped to `aws-sdk-go-v2/service/inspector2@v1.53.0`
(from `v1.48.2`), which added 6 operations with no prior gopherstack
implementation: `CreateConnector`/`UpdateConnector`/`DeleteConnector`/
`ListConnectors` (a new Azure-cloud-provider "connector" resource family,
`connectors.go`/`handler_connectors.go`) and
`ListConnectorScanConfigurations`/`UpdateConnectorScanConfiguration` (scan
settings keyed by the connector's associated AWS Config connector ARN, same
files). All 6 are genuinely implemented against real backend state
(`store.Table[Connector]` + a `byAwsConfigArn` secondary index +
`store.Table[ConnectorScanConfiguration]`, both flowing through
`b.registry.SnapshotAll()`/`RestoreAll()` automatically — no
`persistence.go`/`inspector2SnapshotVersion` change needed, following the
`coverageEntries`/`vulnerabilities`/`codeSnippets` precedent from the prior
pass), not added to `sdk_completeness_test.go`'s `notImplemented` list. See
the `connectors`/`connector_scan_configuration` family notes above for the
full field-diff and the deliberate, documented authorization-lifecycle
simplifications (connectors never leave `PENDING_AUTHORIZATION` — there is no
SDK operation that could ever drive or observe completion of the real
external Azure OAuth consent step, the same bug class this campaign's
securityhub connector work flagged).

### This pass's wire-shape and invented-field fixes

Every "gap"/"partial" family the prior audit (2026-07-12, commit `1e21a848`)
left open was field-diffed against `aws-sdk-go-v2/service/inspector2@v1.48.2`'s
`types/types.go`, `api_op_*.go`, `serializers.go`, and `deserializers.go` this
pass (not just re-read from the prior notes). No inspector2 source changed
between `1e21a848` and this pass's start (`git log 1e21a848..HEAD --
services/inspector2/` was empty), so every family marked `ok` by the prior
pass was trusted without re-diffing, per the manifest's own re-audit protocol.

**Client-breaking wire bugs fixed** (confirmed via the SDK's own
`serializers.go`/`deserializers.go`):
- `BatchGetFindingDetails` request: `findingArns` was decoded into
  `[]map[string]any`; the real shape is a plain `[]string`. Every real
  client's request of the form `{"findingArns":["arn1","arn2"]}` failed
  `json.Unmarshal` with a 400 ValidationException.
- `CreateSbomExport` request: used the gopherstack-invented `sbomFormat` key;
  the real `CreateSbomExportInput` field is `reportFormat`. Every real
  client's report format was silently dropped (unrecognized key, not a
  decode error, so this one degraded rather than crashed).

**Silently-wrong (wrong key name, not crashing) wire bugs fixed**:
- `GetClustersForImage` request: decoded a bare `filterCriteria` map; the
  real `GetClustersForImageInput` nests the required `resourceId` under a
  `filter` object (`ClusterForImageFilterCriteria`). The required-field
  validation never ran and the value was always dropped.
- `GetClustersForImage` response: emitted `"clusters"`; the real wire key is
  `"cluster"` (singular). A real client's `Cluster` field was never
  populated regardless of backend content.

**Invented fields deleted** (no counterpart in the real SDK types):
- `AccountPermission.Status` (wire key `"status"`) — the real `Permission`
  type has no such member; the real second field is `Service` (wire key
  `"service"`), which this backend never populated at all.
- `Vulnerability.VulnerabilityID`/`Severity` (wire keys `"vulnerabilityId"`/
  `"severity"`) — the real `Vulnerability` type's id field wire key is
  `"id"`, and the closest real severity-like field is `VendorSeverity` (wire
  key `"vendorSeverity"`), a distinct semantic (the reporting vendor's own
  severity label, not an Inspector-computed one).

**Prior gap notes corrected** (the real API turned out not to have the
fields the prior note assumed were missing): `GetFindingsReportStatusOutput`
and `GetSbomExportOutput` have **no `createdAt` member at all** in the real
API (confirmed via `deserializers.go`'s case-block enumeration) — the prior
pass's gap note asking for it was itself slightly wrong. `FindingsReport`/
`SbomExport.CreatedAt` remain backend-internal bookkeeping fields and must
never reach the wire.

### New additive seed capabilities (the SeedFinding precedent, extended)

Real Amazon Inspector populates coverage, code snippets, and vulnerability
intelligence automatically via managed scanning engines and AWS's own global
vulnerability database — none of which gopherstack has an equivalent data
source for. Rather than leaving `ListCoverage`/`ListCoverageStatistics`,
`BatchGetCodeSnippet`, and `SearchVulnerabilities` as permanent
hardwired-empty stubs (LocalStack's behavior for these ops), this pass added
`SeedCoverage`/`SeedCodeSnippet`/`SeedVulnerability` — following the exact
precedent `SeedFinding` established. Each real backing store is a
`store.Table` registered on the registry (so `Snapshot`/`Restore` cover them
for free), and each list/search/batch-get op now does genuine state
lookup/filtering/pagination against seeded data instead of returning a
constant empty envelope. `ListUsageTotals` similarly went from a hardwired
constant to a real derivation from `Enable`d resource types and seeded
coverage counts. `ListAccountPermissions` went from hardwired-empty to
computing the real Operation x Service matrix (gopherstack's mock account
has no IAM engine to evaluate against, so it reports the account as able to
perform every configuration operation — a defensible default, not a
fabrication, since there is no real permission model to be unfaithful to).

### Timestamp wire format

Unchanged from the prior pass's fix set (see git history for
`BatchGetFindingDetails`/`ListFindings`/`GetMember`/`ListMembers`/
`BatchGetFreeTrialInfo`/`ListCisScans`/`GetCodeSecurityIntegration`/
`GetCodeSecurityScanConfiguration`). This pass's new timestamped wire
surfaces (`CoverageEntry.LastScannedAt` via `coverageEntryToWire`,
`Vulnerability.VendorCreatedAt`/`VendorUpdatedAt` via
`vulnerabilitiesToWire`) follow the same pattern: epoch-seconds via
`pkgs/awstime.Epoch`, built by hand in a `*ToWire` function, never via
`encoding/json`'s default `time.Time` marshaling. **Trap for the next
auditor** (unchanged): any *new* handler that does `c.JSON(status,
someDomainStruct)` where the struct has a `time.Time` field reaching the
wire directly is reintroducing this bug class.

### Persistence

Unchanged in structure from the prior pass. This pass added three tables —
`coverageEntries` (`CoverageEntry`, composite key `resourceId/scanType`),
`vulnerabilities` (`Vulnerability`, keyed by `id`), and `codeSnippets`
(`codeSnippet`, keyed by `findingArn`) — all registered via
`store.Register`/`registerAllTables` (`store_setup.go`), so they flow
through `b.registry.SnapshotAll()`/`RestoreAll()` automatically with no
`persistence.go` changes required. `inspector2SnapshotVersion` was **not**
bumped: the registry-driven table snapshot format is additive (a new table
name in the `Tables` map), and `RestoreAll` tolerates a snapshot missing
newer table names (pre-this-pass snapshots simply restore with those three
tables empty).

This (2026-07-25) pass adds two more tables the same way: `connectors`
(`Connector`, keyed by `ConnectorArn`, with a `byAwsConfigArn` secondary
`store.Index` — see `store_setup.go`) and `connectorScanConfigs`
(`ConnectorScanConfiguration`, keyed by `AwsConfigConnectorArn`). Same
additive-table story: no `persistence.go`/`inspector2SnapshotVersion` change,
`TestInMemoryBackend_SnapshotRestore_FullState` (`persistence_test.go`)
extended to seed and round-trip both, including the `byAwsConfigArn` index
(proven by the round-tripped `ConnectorScanConfigurationItem.ConnectorArns`,
which is derived from that index rather than stored).

### Filter name validation

`CreateFilter`'s `name` is now validated against AWS's real constraint (3-64
characters, alphanumeric plus dot/underscore/dash) via `validateFilterName`
in `filters.go`, returning `ValidationException` on violation. The same
constraint was not extended to `CreateCisScanConfiguration`/
`CreateCodeSecurityIntegration`/`CreateCodeSecurityScanConfiguration` this
pass — their exact real per-op name constraints were not confirmed against
the SDK's validation-trait metadata, and guessing wrong would trade one gap
for a different bug (over-strict rejection of valid real-world names). Left
as a documented gap.

### PARITY.md accuracy note

The 2026-07-23 pass's note above (13 ops / 22 families, unchanged counts) was
accurate as of that pass — it added no new ops or families, only upgraded
existing `gap`/`partial` entries. This (2026-07-25) pass is different: the Go
SDK modules were bumped, `aws-sdk-go-v2/service/inspector2` picked up 6 new
operations (`CreateConnector`/`UpdateConnector`/`DeleteConnector`/
`ListConnectors`/`ListConnectorScanConfigurations`/
`UpdateConnectorScanConfiguration`), and `TestSDKCompleteness` failed until
they were routed. All 6 are genuinely implemented (see the `connectors`/
`connector_scan_configuration` family notes above), not added to
`sdk_completeness_test.go`'s `notImplemented` list. The handler's routed-op
count goes from 75 to 81 (13 base + 68 extended, up from 62); the `families:`
entry count goes from 22 to 24. Per this campaign's instructions, `go run
./cmd/gendocs` was deliberately **not** run this pass (it regenerates
unrelated services' READMEs as a side effect) — the badges/README's counts
are stale until the next full `gendocs` regeneration; this manifest is the
source of truth in the interim.
