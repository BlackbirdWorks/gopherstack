---
service: securityhub
sdk_module: aws-sdk-go-v2/service/securityhub@v1.75.0
last_audit_commit: 1659d616
last_audit_date: 2026-07-25
overall: A            # parity-4: 7 new SDK ops (CSPM Connectors CRUD+List, SecurityHub V2 opt-in
                       # Feature enable/disable) implemented for real against v1.75.0, wired into
                       # existing DescribeSecurityHubV2 state; one bonus fix (DescribeSecurityHubV2's
                       # wire shape had invented CreatedAt/UpdatedAt fields instead of the real
                       # SubscribedAt); one honestly-documented lifecycle gap (CSPM Connector health
                       # can never reach CONNECTED -- see gaps). Everything else re-verified accurate
                       # against the bumped SDK; no other new families found beyond the 7 assigned ops.
ops:
  EnableSecurityHub: {wire: ok, errors: ok, state: ok, persist: ok}
  DisableSecurityHub: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeHub: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateSecurityHubConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  GetFindings: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED this pass -- SortCriteria is now applied (sortFindings), see Notes"}
  BatchImportFindings: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED this pass -- re-import now preserves Note/UserDefinedFields/VerificationState/Workflow per AWS's documented semantics, see Notes"}
  BatchUpdateFindings: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateFindings: {wire: ok, errors: ok, state: ok, persist: ok}
  GetFindingHistory: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "FIXED this pass -- BatchImportFindings/BatchUpdateFindings/UpdateFindings now record real FindingHistoryRecord entries (findingHistory map, snapshot-persisted); GetFindingHistory returns them filtered by StartTime/EndTime and paginated. See Notes."}
  CreateInsight: {wire: ok, errors: ok, state: ok, persist: ok}
  GetInsights: {wire: ok, errors: ok, state: ok, persist: ok}
  GetInsightResults: {wire: ok, errors: ok, state: ok, persist: ok, note: "ResultValues always empty (no real aggregation) -- acceptable mock behavior, not a stub since Insight itself is real"}
  UpdateInsight: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteInsight: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchEnableStandards: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchDisableStandards: {wire: ok, errors: ok, state: ok, persist: ok}
  GetEnabledStandards: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeStandards: {wire: ok, errors: ok, state: ok, persist: n/a, note: "static known-standards catalog, matches AWS ARNs/names"}
  DescribeStandardsControls: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateStandardsControl: {wire: ok, errors: ok, state: ok, persist: ok}
  ListStandardsControlAssociations: {wire: ok, errors: ok, state: ok, persist: n/a}
  BatchGetStandardsControlAssociations: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchUpdateStandardsControlAssociations: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateActionTarget: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeActionTargets: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateActionTarget: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteActionTarget: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeProducts: {wire: ok, errors: ok, state: ok, persist: n/a, note: "static known-products catalog"}
  ListEnabledProductsForImport: {wire: ok, errors: ok, state: ok, persist: ok}
  EnableImportFindingsForProduct: {wire: ok, errors: ok, state: ok, persist: ok}
  DisableImportFindingsForProduct: {wire: ok, errors: ok, state: ok, persist: ok}
  GetSecurityControlDefinition: {wire: ok, errors: ok, state: ok, persist: n/a, note: "static known-controls catalog"}
  ListSecurityControlDefinitions: {wire: ok, errors: ok, state: ok, persist: n/a}
  BatchGetSecurityControls: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateSecurityControl: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAutomationRules: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateAutomationRule: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchGetAutomationRules: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchDeleteAutomationRules: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchUpdateAutomationRules: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateMembers: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteMembers: {wire: ok, errors: ok, state: ok, persist: ok}
  GetMembers: {wire: ok, errors: ok, state: ok, persist: ok}
  InviteMembers: {wire: ok, errors: ok, state: fixed, persist: ok, note: "FIXED this pass -- see Notes"}
  ListMembers: {wire: ok, errors: ok, state: ok, persist: ok, note: "onlyAssociated=true filters on MemberStatus==Enabled, which no code path ever sets (member acceptance is a cross-account operation this single-account backend can't model) -- see gaps"}
  DisassociateMembers: {wire: ok, errors: ok, state: ok, persist: ok}
  AcceptAdministratorInvitation: {wire: ok, errors: ok, state: ok, persist: ok}
  AcceptInvitation: {wire: ok, errors: ok, state: ok, persist: ok}
  DeclineInvitations: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteInvitations: {wire: ok, errors: ok, state: ok, persist: ok}
  GetInvitationsCount: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListInvitations: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAdministratorAccount: {wire: ok, errors: ok, state: ok, persist: ok}
  GetMasterAccount: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateFromAdministratorAccount: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateFromMasterAccount: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeOrganizationConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateOrganizationConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  EnableOrganizationAdminAccount: {wire: ok, errors: ok, state: ok, persist: ok}
  DisableOrganizationAdminAccount: {wire: ok, errors: ok, state: ok, persist: ok}
  ListOrganizationAdminAccounts: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateFindingAggregator: {wire: ok, errors: ok, state: ok, persist: ok}
  GetFindingAggregator: {wire: ok, errors: ok, state: ok, persist: ok}
  ListFindingAggregators: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateFindingAggregator: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteFindingAggregator: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateConfigurationPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  GetConfigurationPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateConfigurationPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteConfigurationPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  ListConfigurationPolicies: {wire: ok, errors: ok, state: ok, persist: ok}
  GetConfigurationPolicyAssociation: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED this pass -- see Notes"}
  ListConfigurationPolicyAssociations: {wire: ok, errors: ok, state: ok, persist: ok}
  StartConfigurationPolicyAssociation: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED this pass -- see Notes"}
  StartConfigurationPolicyDisassociation: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED this pass -- see Notes"}
  BatchGetConfigurationPolicyAssociations: {wire: ok, errors: ok, state: ok, persist: ok}
  EnableSecurityHubV2: {wire: ok, errors: ok, state: ok, persist: ok}
  DisableSecurityHubV2: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeSecurityHubV2: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED this pass -- see Notes (parity-4): real DescribeSecurityHubV2Output is {Features, HubV2Arn, SubscribedAt}, not {HubV2Arn, CreatedAt, UpdatedAt} as previously returned; now also reports the Features map (new in v1.75.0)."}
  EnableSecurityHubFeatureV2: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW op (parity-4). Gated on SecurityHub V2 being enabled (matches the real API's documented \"the service must be enabled before you can enable a feature\"); features live in HubV2.Features (map[string]*HubV2Feature), so they persist/reset with the V2 hub itself -- no separate state. Idempotent: re-enabling an already-ENABLED feature is a no-op."}
  DisableSecurityHubFeatureV2: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW op (parity-4). Same gating as EnableSecurityHubFeatureV2. Idempotent: disabling a never-enabled or already-DISABLED feature is a no-op that leaves the Features map unchanged (matches the real API's documented no-op semantics rather than fabricating a DISABLED entry for a feature never touched)."}
  CreateAggregatorV2: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAggregatorV2: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAggregatorsV2: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAggregatorV2: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAggregatorV2: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateAutomationRuleV2: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAutomationRuleV2: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAutomationRulesV2: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAutomationRuleV2: {wire: ok, errors: ok, state: fixed, persist: ok, note: "FIXED this pass -- see Notes"}
  DeleteAutomationRuleV2: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateConnectorV2: {wire: ok, errors: ok, state: ok, persist: ok}
  GetConnectorV2: {wire: ok, errors: ok, state: ok, persist: ok}
  ListConnectorsV2: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateConnectorV2: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteConnectorV2: {wire: ok, errors: ok, state: ok, persist: ok}
  RegisterConnectorV2: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateTicketV2: {wire: ok, errors: ok, state: ok, persist: ok, note: "real SDK exposes only Create for TicketV2 -- no Get/List/Update/Delete to implement"}
  GetFindingsV2: {wire: partial, errors: ok, state: ok, persist: ok, note: "FIXED this pass (gopherstack-8j08) -- DateFilters/MapFilters/IpFilters/BooleanFilters/NestedCompositeFilters, previously accepted on the wire and silently ignored (worse than unsupported: a caller got zero errors and unfiltered results), are now evaluated for the field subset genuinely backed by ASFF data this store carries. NestedCompositeFilters recurses fully (AND/OR, depth-capped) rather than being half-evaluated. See Notes for the full field-by-field crosswalk and what remains unmapped (documented, not fabricated)."}
  BatchUpdateFindingsV2: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "FIXED this pass -- request now parses the real flat wire shape (Comment/SeverityId/StatusId/FindingIdentifiers/MetadataUids, not the nonexistent \"FindingFieldsUpdate\" wrapper); FindingIdentifiers now resolve via CloudAccountUid/FindingInfoUid/MetadataProductUid mapped onto the stored finding's AwsAccountId/Id/ProductArn. MetadataUids entries always report ResourceNotFoundException (documented gap -- this mock has no OCSF ingestion path that would ever hand a caller a real metadata.uid). See Notes."}
  GetFindingStatisticsV2: {wire: ok, errors: ok, state: ok, persist: ok}
  GetFindingsTrendsV2: {wire: ok, errors: ok, state: ok, persist: ok}
  GetResourcesV2: {wire: ok, errors: ok, state: ok, persist: ok, note: "resources derived live from V1 findings' Resources arrays -- reasonable given no separate resource ingestion API exists"}
  GetResourcesStatisticsV2: {wire: ok, errors: ok, state: ok, persist: ok}
  GetResourcesTrendsV2: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeProductsV2: {wire: ok, errors: ok, state: ok, persist: n/a}
  GenerateRecommendedPolicyV2: {wire: ok, errors: ok, state: ok, persist: ok}
  GetRecommendedPolicyV2: {wire: ok, errors: ok, state: ok, persist: ok}
  # CSPM Connectors (parity-4, new in v1.75.0): third-party CLOUD PROVIDER
  # connectors (currently Azure only -- CspmProviderConfiguration is a
  # single-member union) that let Security Hub CSPM ingest findings/resource
  # data from a connected Azure environment. NOT the same family as
  # ConnectorV2/RegisterConnectorV2/TicketV2 above, which are Security Hub V2's
  # ticketing-system (Jira/ServiceNow) connectors -- naming collision in the
  # real API, kept distinct here as CspmConnector (non-V2 struct) vs
  # ConnectorV2. REST-path-based (POST/GET/PATCH/DELETE /connectors[/{id}]),
  # confirmed via serializers.go SetURI("/connectors"),
  # SetURI("/connectors/{ConnectorId+}") for all 5 ops.
  CreateConnector: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW op (parity-4). Field-diffed against api_op_CreateConnector.go + types.CspmProviderConfiguration/AzureProviderConfiguration. Required Name/Provider return 400 if missing (client-side validation middleware in the real SDK, modeled defensively here). See gaps for the connector-status lifecycle limitation."}
  GetConnector: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW op (parity-4). Field-diffed against api_op_GetConnector.go + types.CspmHealthCheck/CspmProviderDetail/AzureDetail. Response nests health under Health{ConnectorStatus,LastCheckedAt,Message,Issues} and provider detail under ProviderDetail{Azure:{...}}, matching the real tagged-union wire shape exactly (input Provider and output ProviderDetail share the same {\"Azure\": AzureDetail} shape, confirmed via serializers.go/deserializers.go)."}
  UpdateConnector: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW op (parity-4). Field-diffed against api_op_UpdateConnector.go + types.AzureUpdateConfiguration -- UpdateConnectorInput has NO Name field (only ConnectorId/Description/Provider), and AzureUpdateConfiguration has no AWSConfigConnectorArn (immutable after create); both are honored here (Name update silently ignored per the real shape's absence of the field; AWSConfigConnectorArn merged forward from the original CreateConnector call rather than dropped)."}
  DeleteConnector: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW op (parity-4). Field-diffed against api_op_DeleteConnector.go -- output is EnablementStatus only (PENDING_DELETION), no ConnectorId/Arn. This mock removes the record synchronously (no background worker to model AWS's async teardown window) but still reports PENDING_DELETION on the delete response itself for wire fidelity."}
  ListConnectors: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW op (parity-4). Field-diffed against api_op_ListConnectors.go + types.CspmConnectorSummary/CspmProviderSummary -- ConnectorStatus/EnablementStatus/ProviderName filters are query params (SetQuery, confirmed via serializers.go), not a JSON body, applied as exact-match filters against the stored connector fields."}
families:
  RouteMatcher: {status: ok, note: "every classifyPath prefix cross-checked against real serializers.go SetURI paths for all ~105 ops in aws-sdk-go-v2 v1.71.2 as of the last full audit; the parity-4 pass additionally verified the 7 new ops' SetURI paths (/connectors, /connectors/{ConnectorId+}, /hubv2/feature/{FeatureName}) against v1.75.0 and confirmed pathClassifiers orders the /connectorsv2 prefix before the new plain /connectors prefix (same ordering trap as /automationrulesv2 vs /automationrules -- see Traps below). RouteMatcher's unambiguous-prefix list covers every prefix classifyPath switches on; /findings and /tags/{arn} correctly disambiguated (Authorization signing-service header / ARN service segment) from other services sharing those prefixes (e.g. Macie2). No unreachable-op bugs found."}
  Persistence: {status: ok, note: "Handler.Snapshot/Restore (persistence.go) delegate to InMemoryBackend.Snapshot/Restore (backend.go), which round-trips every store.Table via registry.SnapshotAll/RestoreAll (store_setup.go) plus the 5 plain-map fields (tags, findings, controlParams, productSubscriptions, orgAdminAccounts) and all scalar/pointer fields. Verified store_setup.go registers exactly the set of *store.Table fields declared on InMemoryBackend -- no orphaned or unregistered table."}
gaps:
  - "ListMembers(onlyAssociated=true) can never return members: filters on MemberStatus==\"Enabled\", but nothing transitions a member to Enabled because member-invitation acceptance is a cross-account action this single-account in-memory backend doesn't model (the member's own account would call AcceptInvitation against ITS OWN backend instance, not the administrator's). Not attempted this pass -- architectural, not a bug-fix-sized change; would need a multi-backend cross-account simulation this service doesn't have."
  - "GetFindingsV2 Filters.CompositeFilters evaluates String/Number/Date/Map/Ip/Boolean filters and NestedCompositeFilters (gopherstack-8j08), but only for the field-name subset in ocsfStringFieldMap/ocsfNumberFieldMap/ocsfDateFieldMap/ipFieldNetworkKeys/mapFilterCandidates (findings_v2.go) that has a genuine ASFF-backed equivalent. Any OcsfStringField/OcsfNumberField/OcsfDateField/OcsfMapField/OcsfIpField/OcsfBooleanField outside those mapped subsets is accepted on the wire but not evaluated -- deliberately, per the no-fabrication rule, rather than guessed at. Remaining unmapped, with reasons: (a) fields with no ASFF concept at all -- OcsfBooleanField compliance.assessments.meets_criteria (ASFF Compliance has no 'assessments'), OcsfMapField databucket.tags (ASFF has no databucket concept), most 'evidences.*'/vendor_attributes.*' string+number fields (ASFF has no evidences/vendor_attributes objects); (b) fields whose only ASFF analog is lossy/ambiguous -- OcsfBooleanField vulnerabilities.is_fix_available (ASFF Vulnerability.FixAvailable is three-valued YES/NO/PARTIAL; collapsing PARTIAL into a bool would misclassify findings); (c) fields that exist in ASFF only nested inside arrays this pass didn't reach -- e.g. vulnerabilities.cve.cvss.base_score (Vulnerabilities[].Cvss[].BaseScore), resources.image.*/resources.modified_time_dt (ASFF Resource has no image/per-resource-modified timestamp). class_name (its closest analog, Types, is a string array, not scalar) remains unmapped from the prior pass. A complete OCSF taxonomy crosswalk is ~70 string + ~14 number fields; this pass closed the DateFilters/MapFilters/IpFilters/BooleanFilters/NestedCompositeFilters gap specifically (the issue's stated priority) plus one bonus NumberFilter field (confidence_score -> ASFF Confidence)."
  - "BatchUpdateFindingsV2 MetadataUids-based finding identification can never resolve (always ResourceNotFoundException): this backend has no OCSF ingestion path that would ever hand a real client a metadata.uid to reference back. Only FindingIdentifiers (CloudAccountUid/FindingInfoUid/MetadataProductUid, mapped onto AwsAccountId/Id/ProductArn) can resolve a finding."
  - "(parity-4) CSPM Connector health ConnectorStatus can never leave UNKNOWN, and EnablementStatus can never reach ENABLED: unlike Connectors V2 (which has a dedicated RegisterConnectorV2 to complete an out-of-band OAuth handshake), the real CreateConnector/GetConnector/UpdateConnector/DeleteConnector/ListConnectors surface has NO companion 'complete authorization' operation at all -- establishing connectivity to the Azure account requires a purely external, provider-side step (granting the AWSConfigConnectorArn role access in the Azure portal) that this mock has no API-observable signal for. Auto-advancing a connector to CONNECTED/ENABLED without any real client action causing it would be a fabricated transition, so CreateConnector leaves it at PENDING_ENABLEMENT/UNKNOWN and UpdateConnector leaves it at PENDING_UPDATE permanently. Not attempted this pass -- architectural (no out-of-band signal exists to model), not a bug-fix-sized change."
deferred: []
leaks: {status: clean, note: "no goroutines, tickers, or background loops in services/securityhub -- pure request-response over an in-memory store.Registry guarded by one lockmetrics.RWMutex. New findingHistory map (findings.go/store.go) follows the same plain-map + coarse-lock pattern as findings/tags -- every read/write path holds b.mu for the duration, no separate lock, no goroutines."}
---

## Notes

### parity-4 pass (2026-07-25): 7 new SDK ops from the v1.71.2 -> v1.75.0 bump

The Go SDK module was bumped, revealing 7 operations added to
`aws-sdk-go-v2/service/securityhub` since the previous audit: `CreateConnector`,
`GetConnector`, `UpdateConnector`, `DeleteConnector`, `ListConnectors` (a new
CSPM third-party cloud-provider connector family -- see the "Traps" note
above for why this is *not* the same as the existing `ConnectorV2` family),
and `EnableSecurityHubFeatureV2`/`DisableSecurityHubFeatureV2` (opt-in feature
toggles scoped to the existing SecurityHub V2 hub state). All 7 were
implemented for real (routing, backend state, request parsing, response wire
shapes field-diffed against the SDK's own `types`/`serializers.go`/
`deserializers.go`, error codes, HTTP status, Snapshot/Restore persistence)
and added to `GetSupportedOperations()` -- none went into the
`TestSDKCompleteness` `notImplemented` list (which stayed empty).

Key design decisions:

- **`EnableSecurityHubFeatureV2`/`DisableSecurityHubFeatureV2` are wired to
  the existing `HubV2` state, not an orphan boolean.** The real API's
  `/hubv2/feature/{FeatureName}` path and its documented "the service must be
  enabled before you can enable a feature" precondition both point at the
  existing V2 hub. Features are stored as `HubV2.Features
  map[string]*HubV2Feature` (new field on the existing struct) rather than a
  separate backend field, so they persist/reset with the V2 hub's own
  lifecycle for free (no new Snapshot/Restore wiring needed) and
  `DescribeSecurityHubV2` -- the existing op -- now reports them, matching
  the real `DescribeSecurityHubV2Output.Features` field that also arrived in
  this SDK bump.
- **CSPM Connectors' authorization lifecycle is modeled honestly, not
  auto-completed.** Unlike Connectors V2 (which has `RegisterConnectorV2` to
  complete an out-of-band OAuth handshake), the real CSPM Connector surface
  has no such operation at all -- see the `gaps` entry above. A connector
  created via `CreateConnector` is left at `EnablementStatus=PENDING_ENABLEMENT`
  / health `ConnectorStatus=UNKNOWN` permanently, since no real client action
  this backend can observe would legitimately advance it further.
- **Bonus fix, found while wiring `Features` into `DescribeSecurityHubV2`:**
  its response previously returned invented `CreatedAt`/`UpdatedAt` fields;
  the real `DescribeSecurityHubV2Output` (confirmed in both v1.71.2 and
  v1.75.0, so this predates the SDK bump) is `{Features, HubV2Arn,
  SubscribedAt}`. Fixed in the same handler function this pass touched
  anyway to add `Features`.

Fresh audit (this service had no PARITY.md before the 2026-07-23 pass).
Persistence (Handler.Snapshot/Restore delegating to InMemoryBackend) was
added recently and verified intact -- no changes needed there.

### Bugs fixed this pass

1. **`handler_configpolicy.go` -- ConfigurationPolicyAssociation `TargetType`
   always empty.** `GetConfigurationPolicyAssociation`,
   `StartConfigurationPolicyAssociation`, and
   `StartConfigurationPolicyDisassociation` all read a `"TargetType"` key out
   of the request's `Target` object. The real wire shape (`types.Target` is a
   Smithy tagged union -- see `serializers.go:34632
   awsRestjson1_serializeDocumentTarget`) never sends that field; the request
   is one of `{"AccountId":...}` / `{"OrganizationalUnitId":...}` /
   `{"RootId":...}` and `TargetType` (`ACCOUNT`/`ORGANIZATIONAL_UNIT`/`ROOT`)
   must be derived from which key is present. Every association response's
   `TargetType` field was silently empty for every real SDK client. Fixed by
   adding `extractConfigPolicyTarget` (derives ID + type from the union) and
   using it at all three call sites. Covered by
   `TestParity_ConfigurationPolicyAssociation_TargetTypeDerived`
   (`parity_d_test.go`).

2. **`backend_members.go` -- `InviteMembers` never validated the account
   exists.** AWS requires `CreateMembers` before `InviteMembers`; inviting an
   account that was never created must land in `UnprocessedAccounts`. The
   previous implementation unconditionally created an `Invitation` for every
   account ID with no existence check, so `UnprocessedAccounts` was always
   empty regardless of input validity -- a disguised no-op on the validation
   path. Fixed to check `b.members.Get(id)` first and populate
   `UnprocessedAccounts` (`ResourceNotFoundException`) for unknown accounts,
   matching the same pattern already used by `DeleteMembers`/`GetMembers`.
   Covered by `TestParity_InviteMembers_UnknownAccountUnprocessed`.

3. **`backend_v2.go` -- `UpdateAutomationRuleV2` silently dropped `Actions`
   updates.** The handler passes the raw decoded JSON request body straight
   through as `updates map[string]any`. A JSON array decodes into `[]any`
   (each element `map[string]any`), but the backend asserted
   `updates["Actions"].([]map[string]any)` directly -- an assertion that can
   never succeed against `[]any`, so every `Actions` update was silently
   dropped while every other field updated fine. Fixed to convert `[]any` ->
   `[]map[string]any` element-by-element, mirroring the pattern already used
   correctly in `BatchUpdateAutomationRules` (V1) and the V2 create handler.
   Covered by `TestParity_UpdateAutomationRuleV2_ActionsApplied`.

### Bugs fixed this pass (2026-07-23 gaps sweep)

4. **`findings.go` -- `GetFindings`/`GetFindingsV2` accepted `SortCriteria`
   but silently discarded it (results returned in map-iteration order,
   effectively random).** Added `sortFindings` (stable multi-key sort over
   `types.SortCriterion`'s `Field`/`SortOrder` "asc"/"desc" wire shape),
   wired into both `GetFindings` and the new `GetFindingsV2`. Covered by
   `TestGetFindings_SortCriteria` (`findings_test.go`).

5. **`findings.go` -- `BatchImportFindings` re-import overwrote
   `Note`/`UserDefinedFields`/`VerificationState`/`Workflow` instead of
   preserving them.** AWS documents ("After a finding is created,
   `BatchImportFindings` cannot be used to update the following finding
   fields...") that these four fields are retained from the finding's
   previous version regardless of what a re-import request supplies.
   `ImportFindings` previously did a flat `maps.Copy` that let any
   subsequent import silently reset a customer's investigation
   Note/Workflow/etc. Fixed with `preserveCustomerManagedFields`, which
   restores (or deletes, if never set) these fields from the prior stored
   version after every re-import. Covered by
   `TestBatchImportFindings_PreservesCustomerManagedFields`.

6. **`findings.go` -- `GetFindingHistory` was a hardcoded stub returning
   `{Records: []}` always; no finding-update history was ever recorded.**
   Added a `findingHistory map[string][]map[string]any` store field
   (snapshot-persisted alongside `findings`, same plain-map pattern) and
   `recordFindingHistory`/`diffFindingFields` helpers. `ImportFindings` now
   records a `FindingCreated: true` entry for new findings and a field-diff
   entry for re-imports; `BatchUpdateFindings` and `UpdateFindings` each
   record a field-diff entry per mutated finding (excluding the
   `CreatedAt`/`UpdatedAt`/`FirstObservedAt`/`LastObservedAt` timestamp
   fields AWS documents as excluded from history). `GetFindingHistory` now
   filters the recorded log by `StartTime`/`EndTime` and paginates it (100
   per page, matching AWS's documented cap). Covered by
   `TestGetFindingHistory_RecordsChanges` and
   `TestGetFindingHistory_UnknownFinding`.

7. **`handler_findings.go` -- `BatchUpdateFindingsV2` read a nonexistent
   `"FindingFieldsUpdate"` wrapper key.** The real
   `BatchUpdateFindingsV2Input` wire shape
   (`aws-sdk-go-v2/service/securityhub/api_op_BatchUpdateFindingsV2.go`) is
   flat: `Comment`, `FindingIdentifiers`
   (`[]types.OcsfFindingIdentifier`), `MetadataUids`, `SeverityId`,
   `StatusId` -- there is no wrapper object, so every real client request
   was silently a no-op. Additionally, `FindingIdentifiers` uses
   `CloudAccountUid`/`FindingInfoUid`/`MetadataProductUid`
   (`types.OcsfFindingIdentifier`), not V1's `ProductArn`/`Id`, so even
   after fixing the wrapper-key bug the old delegation to V1
   `BatchUpdateFindings` could never match a stored finding. Rewrote as a
   dedicated `BatchUpdateFindingsV2` backend method
   (`findings_v2.go`) that parses the flat request fields and resolves
   `CloudAccountUid`/`FindingInfoUid`/`MetadataProductUid` against the
   stored finding's `AwsAccountId`/`Id`/`ProductArn` -- the only viable
   mapping since this mock has no separate OCSF ingestion API (findings
   only ever enter via V1 `BatchImportFindings`). Covered by
   `TestBatchUpdateFindingsV2_WireShape` and
   `TestBatchUpdateFindingsV2_UnmatchedIdentifiers`
   (`findings_v2_test.go`).

8. **`handler_findings.go` -- `GetFindingsV2` `Filters` was passed straight
   to the V1 `matchesFindingFilters`, which looks for top-level
   `Id`/`ProductArn`/etc. keys.** The real `GetFindingsV2` `Filters` wire
   shape is `types.OcsfFindingFilters`:
   `{CompositeFilters: [...], CompositeOperator: "AND"|"OR"}`, each
   `CompositeFilter` holding `StringFilters`/`NumberFilters`/etc. keyed by
   an OCSF field name (`types.OcsfStringField`/`OcsfNumberField`) plus its
   own `Operator`. None of those keys exist in the V1 filter shape, so
   every real V2 client's `Filters` was a complete no-op (matched
   everything) rather than merely "unsorted" -- worse than the PARITY.md
   entry previously on file suggested. Added `matchesFindingFiltersV2` +
   `matchesCompositeFilter`/`matchesOcsfStringFilter`/`matchesOcsfNumberFilter`
   (`findings_v2.go`), which evaluate the real nested shape against a
   field-name-mapped subset of the stored ASFF finding (see
   `ocsfStringFieldMap`/`ocsfNumberFieldMap` and the residual-gap entry
   above). `severity_id`/`status_id` NumberFilters round-trip the
   `SeverityId`/`StatusId` fields `BatchUpdateFindingsV2` itself writes (fix
   #7), giving V2 update + V2 filter a coherent, testable round trip.
   Covered by `TestGetFindingsV2_CompositeFilters`.

### GetFindingsV2 composite filter taxonomy (gopherstack-8j08, this pass)

The previous pass (fix #8 above) evaluated only `StringFilters`/`NumberFilters`
within each `CompositeFilter`; `DateFilters`, `MapFilters`, `IpFilters`,
`BooleanFilters`, and `NestedCompositeFilters` were accepted on the wire and
silently ignored -- worse than an error, since a caller got HTTP 200 and an
unfiltered result set with no indication their filter did nothing. Field-diffed
the full real taxonomy (`types.CompositeFilter`, `types.Ocsf*Filter`,
`types.Ocsf*Field` enums, `types.StringFilter`/`MapFilter`/`DateFilter`/
`IpFilter`/`BooleanFilter`/`NumberFilter`/`DateRange`,
`types.AllowedOperators`/`StringFilterComparison`/`MapFilterComparison`/
`DateRangeComparison`/`DateRangeUnit`) against `aws-sdk-go-v2/service/
securityhub@v1.75.0`'s `types/types.go` and `types/enums.go` directly (not
against this handler's own prior output).

**Filter types implemented this pass**, each restructured into its own small
result-collector (`stringFilterResults`/`numberFilterResults`/
`dateFilterResults`/`mapFilterResults`/`ipFilterResults`/
`booleanFilterResults`/`nestedCompositeFilterResults`) feeding a single
`matchesCompositeFilterDepth` combinator (decomposed to keep CodeFactor's
Complex Method check quiet -- no `nolint`):

- **DateFilters** (`ocsfDateFieldMap`): `finding_info.created_time_dt` ->
  `CreatedAt`, `finding_info.first_seen_time_dt` -> `FirstObservedAt`,
  `finding_info.last_seen_time_dt` -> `LastObservedAt`,
  `finding_info.modified_time_dt` -> `UpdatedAt` -- all genuine ASFF
  finding-level timestamps. Both comparator shapes are implemented:
  absolute `Start`/`End` bounds (`matchesDateStartEnd`), and relative
  `DateRange{Comparison: WITHIN|OLDER_THAN, Unit: DAYS, Value}`
  (`matchesDateRange`) -- `WITHIN` matches at-or-after `now - Value days`,
  `OLDER_THAN` its strict complement. `resources.image.*`/
  `resources.modified_time_dt` have no ASFF equivalent (ASFF's `Resource`
  carries no image/per-resource-modified timestamp) and are unmapped.
- **MapFilters** (`mapFilterCandidates`): `resources.tags` -> per-resource
  `Resources[].Tags`, `finding_info.tags` -> the finding-level
  `UserDefinedFields` map (the closest real ASFF analog to a finding-level
  "tag"), `compliance.control_parameters` -> `Compliance.
  SecurityControlParameters[]{Name,Value[]}`. All four `MapFilterComparison`
  values implemented (`EQUALS`/`NOT_EQUALS`/`CONTAINS`/`NOT_CONTAINS`) via
  `compareMapFilter`, with positive comparisons OR'd and negative ones AND'd
  across multiple candidate values for the same key (mirrors the documented
  same-field combination rule). `databucket.tags` has no ASFF concept at all
  and is unmapped.
- **IpFilters** (`ipFieldNetworkKeys`): `evidences.src_endpoint.ip` ->
  `Network.SourceIpV4`/`SourceIpV6`, `evidences.dst_endpoint.ip` ->
  `Network.DestinationIpV4`/`DestinationIpV6` -- ASFF has no "evidences"
  concept, but `Network`'s source/destination IP fields are the only
  genuinely analogous data this store carries. `IpFilter` has only a `Cidr`
  field (no comparator) -- CIDR containment via `net.ParseCIDR`/
  `IPNet.Contains`, with a bare IP address normalized to an exact-match
  `/32` or `/128` per AWS's documented "CIDR block or single IP" input.
- **BooleanFilters**: only `vulnerabilities.is_exploit_available` is
  evaluated -- `Vulnerability.ExploitAvailable` is a genuine two-valued ASFF
  enum (`YES`/`NO`), so it round-trips to bool cleanly; a finding matches if
  ANY entry in its `Vulnerabilities` array has a matching value.
  `vulnerabilities.is_fix_available` is deliberately NOT evaluated:
  `Vulnerability.FixAvailable` is three-valued (`YES`/`NO`/`PARTIAL`), and
  collapsing `PARTIAL` into either `true` or `false` would silently
  misclassify findings -- worse than leaving it unfiltered.
  `compliance.assessments.meets_criteria` has no ASFF backing at all (no
  "assessments" concept on `Compliance`) and is also unmapped.
- **NumberFilters bonus**: added `confidence_score` -> ASFF's own top-level
  `Confidence` (int 0-100) to `ocsfNumberFieldMap` -- a clean scalar match
  found while auditing the taxonomy, not part of the original gap list.

**NestedCompositeFilters**: recurses fully via `matchesCompositeFilterDepth`
-- each nested `CompositeFilter` is evaluated as its own sub-tree (including
its own further `NestedCompositeFilters`) and the resulting bool joins its
*parent's* result list, combined by the parent's own `Operator`. This was
chosen over half-evaluating (e.g. only reading direct filters and ignoring
nesting) because a partially-evaluated boolean tree returns **wrong**
results, not merely unfiltered ones -- see the task's own warning, confirmed
by a regression-style test case
(`NestedCompositeFilters_AND_recurses_and_requires_both_branches`): a single
finding can't have two different `AwsAccountId` values, so ANDing two
mutually-exclusive nested branches must match zero findings; before this
fix (`NestedCompositeFilters` unevaluated -> empty result list -> vacuous
match-all), that same request would have wrongly matched both seeded
findings. Recursion depth is capped at `maxNestedCompositeDepth = 5` (AWS
documents the real structure as capped at 3 layers; 5 is a defensive margin
against a pathological/hand-crafted request, not a limit real traffic
should approach). Note `types.AllowedOperators` has only `AND`/`OR` -- there
is no logical NOT combinator in the real API; negation is expressed at the
leaf via `NOT_*` comparators (`NOT_EQUALS`/`NOT_CONTAINS`/
`PREFIX_NOT_EQUALS`), not a boolean-tree NOT node, so AND/OR recursion is
the complete real semantics.

**Comparator verification**: `StringFilterComparison`
(`EQUALS`/`PREFIX`/`NOT_EQUALS`/`PREFIX_NOT_EQUALS`/`CONTAINS`/
`NOT_CONTAINS`/`CONTAINS_WORD`) was already correctly implemented by
`compareStringFilter` (reused unchanged) -- confirmed against `types.go`'s
enum values and `StringFilter`'s doc comments describing each comparator's
exact semantics (including the CONTAINS_WORD-only-in-V2-APIs note).
`MapFilterComparison` (`EQUALS`/`NOT_EQUALS`/`CONTAINS`/`NOT_CONTAINS`, no
PREFIX variant -- confirmed the enum has no PREFIX member) implemented fresh
in `compareMapFilter` following the same positive-OR/negative-AND doc
pattern. `DateRangeComparison` (`WITHIN`/`OLDER_THAN`, default `WITHIN` per
doc) and the fact `DateRangeUnit` has only `DAYS` as of this SDK version
were both confirmed directly against `enums.go`. `NumberFilter` was
reconfirmed to have no `Comparison` field at all (`Eq`/`Gt`/`Gte`/`Lt`/`Lte`
only) -- unchanged from the prior pass.

Tests: extended `TestGetFindingsV2_CompositeFilters` (existing table) with
two `confidence_score` cases, and added a new table test
`TestGetFindingsV2_CompositeFilters_DateMapIPBooleanNested` covering every
implemented filter type with paired cases that each narrow to exactly one of
two seeded findings with deliberately divergent field values (proving actual
discrimination, not a "matches everything" false pass), plus the
AND/OR nested-recursion pair described above.

### Route-matcher check

Extracted every op's HTTP method + URI template directly from
`aws-sdk-go-v2/service/securityhub@v1.71.2/serializers.go`
(`awsRestjson1_serializeOpHttpBindings*` / `SplitURI` calls) for all ~105
operations and cross-checked against `classifyPath`'s per-family
`classify*Path` functions in `handler.go`, `handler_members.go`,
`handler_configpolicy.go`, and `handler_v2.go`. All method+path pairs match.
`RouteMatcher` (`handler.go`) was separately checked to confirm every prefix
`classifyPath` switches on is also covered by `RouteMatcher`'s
unambiguous-prefix OR-chain, so no routed op is reachable by `Handler()`
directly (bypassing the matcher, as unit tests do) but unreachable through
the real Echo route registration. No route-matcher bugs found in this
service.

### Traps for the next auditor

- `/automationrulesv2` is a `strings.HasPrefix` superset of `/automationrules`
  (both share the `/automationrules` substring) -- `classifyPath`'s switch
  correctly orders the V2 case before the V1 case. Don't "simplify" that
  ordering.
- (parity-4) Same trap, new pair: `/connectorsv2` is a `strings.HasPrefix`
  superset of the new plain `/connectors` (CSPM connectors). `pathClassifiers`
  in `handler.go` orders `hasPathPrefix(pathConnectorsV2)` before
  `hasPathPrefix(pathConnectors)` -- don't reorder or collapse them. Also note:
  `CreateConnector`/`GetConnector`/etc. (this pass) and
  `CreateConnectorV2`/`GetConnectorV2`/etc. are two *entirely unrelated* real
  AWS features that happen to share the word "connector" -- CSPM connectors
  link to third-party cloud providers (Azure), Connectors V2 link to
  third-party ticketing systems (Jira/ServiceNow). Modeled as distinct Go
  types (`CspmConnector` vs `ConnectorV2`) and distinct backend/handler files
  (`connectors.go`/`handler_connectors.go` vs
  `connectors_v2.go`/`handler_connectors_v2.go`) specifically to avoid
  conflating them.
- `classifyConfigPolicyPath`'s PATCH/DELETE cases match `/configurationPolicy/`
  with explicit exclusions for `create`/`get`/`list` suffixes rather than a
  positive `{Identifier}` pattern -- this is intentional (mirrors the real
  flat-path-segment routing) and correct as long as no real
  `ConfigurationPolicyIdentifier` value is literally `"create"`, `"get"`, or
  `"list"`.
- `BatchUpdateFindings`/`ImportFindings`/`GetFindings` do **not** check
  `hubEnabled` (unlike `UpdateFindings`/insights/action-targets). This was
  investigated and left as-is: AWS's own docs don't clearly state these ops
  require the hub to be enabled, and no existing test asserts either
  behavior, so flipping it risks breaking passing integrations without clear
  spec backing. Revisit if a concrete AWS error transcript surfaces.
