---
service: securityhub
sdk_module: aws-sdk-go-v2/service/securityhub@v1.71.2
last_audit_commit: 5845d0e3
last_audit_date: 2026-07-23
overall: A            # gaps sweep: 5 real fixes (SortCriteria, BatchImportFindings field-preservation, GetFindingHistory, GetFindingsV2 CompositeFilters, BatchUpdateFindingsV2 wire shape + identifier mapping); rest of the surface re-verified accurate
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
  DescribeSecurityHubV2: {wire: ok, errors: ok, state: ok, persist: ok}
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
  GetFindingsV2: {wire: partial, errors: ok, state: ok, persist: ok, note: "FIXED this pass -- Filters.CompositeFilters/CompositeOperator (types.OcsfFindingFilters) is now parsed and applied against the V1 ASFF store (StringFilters+NumberFilters for a mapped field subset; SortCriteria via the same sortFindings as V1). Previously the V1 filter matcher looked for top-level Id/ProductArn keys that never appear in the real V2 request shape, so every V2 filter was silently a no-op. Residual gap: DateFilters/MapFilters/IpFilters/BooleanFilters/NestedCompositeFilters and unmapped OcsfStringField/OcsfNumberField names are not evaluated -- see gaps."}
  BatchUpdateFindingsV2: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "FIXED this pass -- request now parses the real flat wire shape (Comment/SeverityId/StatusId/FindingIdentifiers/MetadataUids, not the nonexistent \"FindingFieldsUpdate\" wrapper); FindingIdentifiers now resolve via CloudAccountUid/FindingInfoUid/MetadataProductUid mapped onto the stored finding's AwsAccountId/Id/ProductArn. MetadataUids entries always report ResourceNotFoundException (documented gap -- this mock has no OCSF ingestion path that would ever hand a caller a real metadata.uid). See Notes."}
  GetFindingStatisticsV2: {wire: ok, errors: ok, state: ok, persist: ok}
  GetFindingsTrendsV2: {wire: ok, errors: ok, state: ok, persist: ok}
  GetResourcesV2: {wire: ok, errors: ok, state: ok, persist: ok, note: "resources derived live from V1 findings' Resources arrays -- reasonable given no separate resource ingestion API exists"}
  GetResourcesStatisticsV2: {wire: ok, errors: ok, state: ok, persist: ok}
  GetResourcesTrendsV2: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeProductsV2: {wire: ok, errors: ok, state: ok, persist: n/a}
  GenerateRecommendedPolicyV2: {wire: ok, errors: ok, state: ok, persist: ok}
  GetRecommendedPolicyV2: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  RouteMatcher: {status: ok, note: "every classifyPath prefix cross-checked against real serializers.go SetURI paths for all ~105 ops in aws-sdk-go-v2 v1.71.2; RouteMatcher's unambiguous-prefix list covers every prefix classifyPath switches on; /findings and /tags/{arn} correctly disambiguated (Authorization signing-service header / ARN service segment) from other services sharing those prefixes (e.g. Macie2). No unreachable-op bugs found."}
  Persistence: {status: ok, note: "Handler.Snapshot/Restore (persistence.go) delegate to InMemoryBackend.Snapshot/Restore (backend.go), which round-trips every store.Table via registry.SnapshotAll/RestoreAll (store_setup.go) plus the 5 plain-map fields (tags, findings, controlParams, productSubscriptions, orgAdminAccounts) and all scalar/pointer fields. Verified store_setup.go registers exactly the set of *store.Table fields declared on InMemoryBackend -- no orphaned or unregistered table."}
gaps:
  - "ListMembers(onlyAssociated=true) can never return members: filters on MemberStatus==\"Enabled\", but nothing transitions a member to Enabled because member-invitation acceptance is a cross-account action this single-account in-memory backend doesn't model (the member's own account would call AcceptInvitation against ITS OWN backend instance, not the administrator's). Not attempted this pass -- architectural, not a bug-fix-sized change; would need a multi-backend cross-account simulation this service doesn't have."
  - "GetFindingsV2 Filters.CompositeFilters only evaluates StringFilters and NumberFilters, and only for the field-name subset in ocsfStringFieldMap/ocsfNumberFieldMap (findings_v2.go) that has a direct scalar ASFF equivalent. DateFilters, MapFilters, IpFilters, BooleanFilters, NestedCompositeFilters, and any OcsfStringField/OcsfNumberField outside the mapped subset (e.g. class_name, which has no scalar ASFF equivalent -- the closest analog, Types, is a string array) are accepted on the wire but not evaluated, matching (not exceeding) the 'basic subset' precedent V1 GetFindings/matchesFindingFilters already established. Full coverage needs a complete OCSF field taxonomy crosswalk (~70 string fields, ~15 number fields alone) -- out of scope for this pass."
  - "BatchUpdateFindingsV2 MetadataUids-based finding identification can never resolve (always ResourceNotFoundException): this backend has no OCSF ingestion path that would ever hand a real client a metadata.uid to reference back. Only FindingIdentifiers (CloudAccountUid/FindingInfoUid/MetadataProductUid, mapped onto AwsAccountId/Id/ProductArn) can resolve a finding."
deferred: []
leaks: {status: clean, note: "no goroutines, tickers, or background loops in services/securityhub -- pure request-response over an in-memory store.Registry guarded by one lockmetrics.RWMutex. New findingHistory map (findings.go/store.go) follows the same plain-map + coarse-lock pattern as findings/tags -- every read/write path holds b.mu for the duration, no separate lock, no goroutines."}
---

## Notes

Fresh audit (this service had no PARITY.md before this pass). Persistence
(Handler.Snapshot/Restore delegating to InMemoryBackend) was added recently
and verified intact -- no changes needed there.

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
