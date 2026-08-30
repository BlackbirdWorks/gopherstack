---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: lakeformation
sdk_module: aws-sdk-go-v2/service/lakeformation@v1.50.4
last_audit_commit:                                # unknown: pass ran without git access at write time, never backfilled -- gopherstack-33in
last_audit_date: 2026-08-15
overall: A            # gopherstack-6flj wrapper-key sweep: GetTemporaryDataLocationCredentials wire-breaking sibling-copy bug fixed, plus 4 adjacent bugs
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  RegisterResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: ExpectedResourceOwnerAccount/WithFederation/WithPrivilegedAccess/HybridAccessEnabled now threaded through to ResourceInfo via RegisterResourceOptions (previously silently dropped -- interface signature change)"}
  UpdateResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "same extended-fields fix as RegisterResource (ExpectedResourceOwnerAccount/WithFederation/HybridAccessEnabled)"}
  DeregisterResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascades permission cleanup for the resource"}
  DescribeResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "LastModified epoch seconds; now also emits ExpectedResourceOwnerAccount/VerificationStatus/HybridAccessEnabled/WithFederation/WithPrivilegedAccess"}
  ListResources: {wire: ok, errors: ok, state: ok, persist: ok, note: "same fixes as DescribeResource; gopherstack-4ly2 wrapper-key sweep: FilterConditionList (RESOURCE_ARN/ROLE_ARN/LAST_MODIFIED, all 11 ComparisonOperator values) was never even parsed into the wire request struct, so every registered resource always came back regardless of the filter -- now honored (resources.go matchesFilterConditions)"}
  GrantPermissions: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: Condition now accepted/persisted; entry.LastUpdated stamped on every grant/merge; Resource union extended (see families below)"}
  RevokePermissions: {wire: ok, errors: ok, state: ok, persist: ok, note: "Condition now accepted; LastUpdated stamped on partial revoke"}
  ListPermissions: {wire: ok, errors: ok, state: ok, persist: ok, note: "WIRE-BREAKING BUG FIXED: request filtered by a flat ResourceArn string; the real ListPermissionsInput has no ResourceArn field at all -- it filters by a nested Resource object (same shape as Grant/RevokePermissions). A real aws-sdk-go-v2 client's ListPermissions call would never have matched anything against the old gopherstack shape. Response PrincipalResourcePermissions now wire-encodes LastUpdated as epoch seconds (permissionEntryWire) and includes Condition/LastUpdatedBy."}
  BatchGrantPermissions: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: entries now use BatchPermissionsRequestEntry with the real API's required Id field (previously entirely absent -- BatchFailureEntry.RequestEntry had no way to correlate back to the caller's request); also now applies the same PermissionsWithGrantOption-subset validation GrantPermissions does, per-entry"}
  BatchRevokePermissions: {wire: ok, errors: ok, state: ok, persist: ok, note: "same Id-field fix as BatchGrantPermissions"}
  CreateLFTag: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteLFTag: {wire: ok, errors: ok, state: ok, persist: ok}
  GetLFTag: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateLFTag: {wire: ok, errors: ok, state: ok, persist: ok}
  ListLFTags: {wire: ok, errors: ok, state: ok, persist: ok, note: "gopherstack-4ly2 wrapper-key sweep: ResourceShareType was accepted nowhere -- FOREIGN now returns no tags (this backend models a single account with no RAM cross-account sharing, so no LF-tag is ever foreign); ALL/unset unchanged"}
  AddLFTagsToResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now rejects non-Database/Table/TableWithColumns Resource kinds (was a permissive superset of what AWS accepts, see gopherstack-kbnu); resourceToKey also fixed to key TableWithColumns distinctly (previously had no case for it at all -- every TableWithColumns resource collided under the same empty-string key)"}
  RemoveLFTagsFromResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "same resource-kind restriction fix as AddLFTagsToResource"}
  GetResourceLFTags: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same resource-kind restriction as AddLFTagsToResource/RemoveLFTagsFromResource; also fixed getResourceLFTagsOutput.LFTagsOnColumns, which was typed []LFTagPair -- the real GetResourceLFTagsOutput.LFTagsOnColumns is []types.ColumnLFTag (Name+LFTags) -- and was never populated by any code path (disguised stub)"}
  CreateDataCellsFilter: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: ColumnWildcard (ExcludedColumnNames) now accepted/persisted; ColumnNames+ColumnWildcard together rejected as InvalidInputException (real API: must specify exactly one); VersionId now assigned. gopherstack-i8lo (2026-08-22): re-verified all four of DataCellsFilter's required members (DatabaseName/Name/TableCatalogId/TableName, types.go:154-173) against the backend -- already fully validated (CreateDataCellsFilter, data_cells_filter.go:47-61); no fix needed. UpdateDataCellsFilter shares the same validation, same file."}
  GetDataCellsFilter: {wire: ok, errors: fixed, state: ok, persist: ok, note: "UNDER-VALIDATION FIXED (gopherstack-i8lo, 2026-08-22): GetDataCellsFilterInput marks all four of TableCatalogId/DatabaseName/TableName/Name required (api_op_GetDataCellsFilter.go:29-48, lakeformation@v1.50.4) -- only Name was enforced. Three checks added, data_cells_filter.go."}
  UpdateDataCellsFilter: {wire: ok, errors: ok, state: ok, persist: ok, note: "same ColumnWildcard/VersionId fix as CreateDataCellsFilter; gopherstack-i8lo (2026-08-22): all four required members already validated, same as Create -- no fix needed"}
  DeleteDataCellsFilter: {wire: ok, errors: fixed, state: ok, persist: ok, note: "OVER-VALIDATION FIXED (gopherstack-i8lo, 2026-08-22): DeleteDataCellsFilterInput marks NO member required (api_op_DeleteDataCellsFilter.go:27-42, lakeformation@v1.50.4) -- gopherstack demanded Name. Check removed; a request without Name now falls through to the existing not-found path instead of a spurious 400."}
  ListDataCellsFilter: {wire: ok, errors: fixed, state: ok, persist: ok, note: "gopherstack-4ly2 (2026-08-21): handler wrongly rejected any request without Table (and Table without DatabaseName) with a 400. ListDataCellsFilterInput marks no member required (api_op_ListDataCellsFilter.go, lakeformation@v1.50.4) -- ListDataCellsFilter's own backend already documented tableCatalogID/databaseName/tableName as optional filters, so the handler's checks were redundant with, and contradicted, the backend's own design. Now Table (and its sub-fields) narrow the listing only when supplied. Two existing tests asserted the wrong 400 (TestListDataCellsFilter_Empty citing a nonexistent 'issue #15', TestListDataCellsFilter_RequiresTable) and were corrected."}
  CreateLFTagExpression: {wire: ok, errors: ok, state: ok, persist: ok, note: "impl moved lf_tag_policy.go -> lf_tag_expression.go (file name was misleading: it implements LFTagExpression, not the distinct LFTagPolicyResource permission-resource kind added this pass)"}
  GetLFTagExpression: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateLFTagExpression: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteLFTagExpression: {wire: ok, errors: ok, state: ok, persist: ok}
  ListLFTagExpressions: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateLakeFormationOptIn: {wire: ok, errors: ok, state: ok, persist: ok, note: "Condition field added (LFOptIn/createLakeFormationOptInInput)"}
  DeleteLakeFormationOptIn: {wire: ok, errors: ok, state: ok, persist: ok, note: "Condition accepted (not part of the match key -- opt-ins are unique per principal+resource per AWS's documented AlreadyExistsException behavior)"}
  ListLakeFormationOptIns: {wire: ok, errors: ok, state: ok, persist: ok, note: "LastModified epoch seconds; Condition now included"}
  CreateLakeFormationIdentityCenterConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed (gopherstack-6flj): ServiceIntegrations (real member, PARITY.md's own prior deferred: claim that no op takes it was wrong) now parsed and stored"}
  DescribeLakeFormationIdentityCenterConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed (gopherstack-6flj): ApplicationStatus removed from the wire response -- real only as Update's request field, a real key on the wrong op/direction; ServiceIntegrations now emitted. ResourceShare (RAM resource-share ARN) still missing -- disclosed in gaps:, this backend has no region at the storage layer to synthesize one honestly"}
  UpdateLakeFormationIdentityCenterConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed (gopherstack-6flj): ShareRecipients was entirely absent from the request struct (silently discarded on every real update, unlike Create/Describe which already handled it) and ServiceIntegrations was unparsed; both now threaded through with nil-vs-explicit-empty-list semantics matching AWS's documented clear-vs-unchanged behavior"}
  DeleteLakeFormationIdentityCenterConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  StartTransaction: {wire: ok, errors: ok, state: ok, persist: ok}
  CancelTransaction: {wire: ok, errors: ok, state: ok, persist: ok}
  CommitTransaction: {wire: ok, errors: ok, state: ok, persist: ok}
  ExtendTransaction: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeTransaction: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTransactions: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteObjectsOnCancel: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetTableObjects: {wire: ok, errors: ok, state: ok, persist: n/a, note: "not persisted (matches pre-existing scope; tableObjects map was never in backendSnapshot)"}
  UpdateTableObjects: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetTemporaryDataLocationCredentials: {wire: ok, errors: fixed, state: ok, persist: n/a, note: "WIRE-BREAKING BUG FIXED (gopherstack-6flj): request struct was copied from the GetTemporaryGlue*Credentials sibling shape (ResourceArn/Permissions/SupportedPermissionTypes) -- the real Input has none of those, only DataLocations ([]string)/CredentialsScope. No real client's request was ever readable; every call failed gopherstack's own required-field check. Response also gained the real, previously-missing AccessibleDataLocations/CredentialsScope members. gopherstack-4ly2 (2026-08-21): the fixed handler still over-validated -- it demanded DataLocations be non-empty, but GetTemporaryDataLocationCredentialsInput marks no member required, DataLocations included, and the backend never uses it as a lookup key (only echoes it back as AccessibleDataLocations). Now optional; TestGetTemporaryDataLocationCredentials_MissingDataLocations (which asserted the wrong 400) was corrected."}
  GetTemporaryGluePartitionCredentials: {wire: ok, errors: ok, state: ok, persist: n/a, note: "checked against its GetTemporaryGlueTableCredentials/GetTemporaryDataLocationCredentials siblings this pass (gopherstack-6flj) -- already correct, no fix needed"}
  GetTemporaryGlueTableCredentials: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed (gopherstack-6flj): real request member S3Path was parsed nowhere; real response member VendedS3Path was entirely missing. Now threaded through together. QuerySessionContext (also real on this op) remains unmodeled -- disclosed in gaps:, a broader query-family feature out of scope for this pass"}
  AssumeDecoratedRoleWithSAML: {wire: ok, errors: ok, state: ok, persist: n/a}
  StartQueryPlanning: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetQueryState: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetQueryStatistics: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetWorkUnits: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetWorkUnitResults: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed (gopherstack-h910): the required WorkUnitId (int64, JSON body field, verified against serializers.go's awsRestjson1_serializeOpDocumentGetWorkUnitResultsInput) was dropped entirely, so any value -- even one addressing a work unit that was never generated -- returned the same result. Now validated against GetWorkUnits' actual output: since GetWorkUnits always returns exactly one range (WorkUnitIDMin/Max both 0), only WorkUnitId=0 is accepted, InvalidInputException otherwise."}
  ListTableStorageOptimizers: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateTableStorageOptimizer: {wire: ok, errors: ok, state: ok, persist: ok}
  SearchDatabasesByLFTags: {wire: ok, errors: ok, state: ok, persist: n/a, note: "real implementation in lf_tags.go, not a stub"}
  SearchTablesByLFTags: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetEffectivePermissionsForPath: {wire: ok, errors: ok, state: ok, persist: ok, note: "unlike ListPermissions, this op genuinely does filter by a flat ResourceArn per the real API -- kept its own ARN-based implementation (permissionMatchesARN) rather than sharing ListPermissions' new Resource-shaped filter. Also now expands LFTagPolicy grants: resolves resourceArn to a Database/Table resource, looks up its actual assigned LF-tags (resourceLFTags), and includes any LFTagPolicy-resource grant whose Expression/ExpressionName is satisfied (AND across tag keys, OR across one key's values) -- previously a tag-policy grant was invisible here entirely since it has no Database/Table field to ARN-match against (gopherstack-kbnu)"}
  GetDataLakePrincipal: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetDataLakeSettings: {wire: ok, errors: ok, state: ok, persist: ok, note: "ExternalDataFilteringAllowList field added (was entirely missing from DataLakeSettings)"}
  PutDataLakeSettings: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  route_matcher: {status: ok, note: "unchanged this pass -- isLakeFormationPath's 61 literal paths still verified byte-for-byte against serializers.go from the prior audit; no new ops added upstream (61 ops, serializers.go byte-identical, from v1.47.3 through the now-current v1.50.4 pin)."}
  resource_union: {status: ok, note: "Resource previously only carried Catalog/Database/Table/TableWithColumns/DataLocation. Added DataCellsFilter/LFTag(LFTagKeyResource)/LFTagExpression/LFTagPolicy(LFTagPolicyResource) -- all real types.Resource union members. GrantPermissions/RevokePermissions/ListPermissions now work end-to-end against every kind (resourceToKey/copyResource/permissionMatchesResource/permissionMatchesResourceType all extended); see handler_permissions_resource_kinds_test.go for coverage of all 6 previously-partial/deferred kinds plus TableWildcard and CatalogResource.Id."}
  permission_enum: {status: ok, note: "isValidPermission previously accepted three gopherstack-INVENTED permission strings that do not exist in types.Permission's Values() at all -- \"CREATE_TAG\" (real name is CREATE_LF_TAG, already separately present), \"CREATE_LAKE_FORMATION_OPT_IN\" (not a Permission at all), and \"SUPER\" (real value is SUPER_USER) -- and was missing the real \"CREATE_LF_TAG_EXPRESSION\" value. All three invented values DELETED, CREATE_LF_TAG_EXPRESSION added. isValidPermission now matches the real 16-member enum exactly."}
gaps:
  - "NOT FIXED (gopherstack-6flj, 2026-08-15): DescribeLakeFormationIdentityCenterConfigurationOutput.ResourceShare (*string, the RAM resource-share ARN AWS creates server-side when ShareRecipients is set) is still never populated. This backend's InMemoryBackend carries no account/region fields at the storage layer (region only exists as Handler.DefaultRegion, set post-construction and never threaded into any backend call in this service), and there is no real RAM cross-service integration (same already-documented gap as AdditionalDetails below). Synthesizing a value would mean either fabricating a region or introducing new region-threading plumbing disproportionate to a single-op fix. Disclosed, not fabricated."
  - "NOT FIXED (gopherstack-6flj, 2026-08-15): GetTemporaryGlueTableCredentialsInput.QuerySessionContext (real, api_op_GetTemporaryGlueTableCredentials.go) is unmodeled anywhere in this service, and likely shared by several query-planning ops (GetWorkUnits/StartQueryPlanning/GetWorkUnitResults use similar context structures). A broader structural feature spanning the query-family ops; out of scope for this pass's discarded-input fixes, which were limited to S3Path/VendedS3Path on this same op."
  - "FIXED (gopherstack-kbnu): PrincipalResourcePermissions.LastUpdatedBy is now populated by GrantPermissions/RevokePermissions/BatchGrantPermissions/BatchRevokePermissions with a synthetic caller ARN derived from awsmeta.Account(ctx) (callerPrincipalARN, credentials.go -- same identity GetDataLakePrincipal reports). Interface signatures gained a ctx context.Context first parameter; all callers updated."
  - "PrincipalResourcePermissions.AdditionalDetails (DetailsMap.ResourceShare, RAM resource-share info) is still never populated. Re-checked this pass: gopherstack DOES have a standalone services/ram package (resource shares, principals, permissions), but there is no cross-service wiring between it and lakeformation anywhere in the codebase (no service in this repo reaches into another service's InMemoryBackend directly -- checked s3<->kms as a second data point, same finding). Populating this would require introducing a new cross-service backend-injection pattern, which is out of scope for a single-service follow-up. Correctly omitted rather than fabricated."
  - "PARTIALLY FIXED (gopherstack-kbnu): LFTagPolicy-based permission grants are now expanded into effective per-resource permissions in GetEffectivePermissionsForPath (resolves the resourceArn to a Database/Table, looks up its actual LF-tags, and evaluates each LFTagPolicy grant's Expression/ExpressionName against them -- AND across tag keys, OR across one key's values, per https://docs.aws.amazon.com/lake-formation/latest/dg/managing-tag-expressions.html). ListPermissions filtered by a concrete resource intentionally still does NOT expand tag-policy grants: AWS's own documented behavior is that LF-Tag-based grants are queried via their own LFTagPolicy/LF_TAG_POLICY_* resource type, not by listing the concrete resource they happen to cover (a tag-based grant 'may not appear in ListPermissions results for specific resources'). SearchTablesByLFTags/SearchDatabasesByLFTags remain untouched (out of scope for this pass -- they answer 'which resources have these tags', not 'what permissions apply to this resource'). No LakeFormation operation in this backend enforces authorization at runtime (permissions are bookkeeping, not an enforcement engine); this pass only makes the LF-Tag-derived permission *record* visible where AWS documents it should be, it does not add access control."
  - "NOT FIXED (gopherstack-4ly2, 2026-08-29): ListPermissionsInput.IncludeRelated (\"show the cell filters on a table resource\") is parsed into the wire request struct but never read. This backend's permissionsList only holds explicitly granted permissions (via Grant/RevokePermissions) -- there are no separately-derived cell-filter permission entries for IncludeRelated to toggle inclusion of, so honoring it would require inventing a synthetic permission-derivation feature. Structural gap, not an unread parameter with real data behind it."
  - "NOT FIXED (gopherstack-4ly2, 2026-08-29): ListTableStorageOptimizersInput.MaxResults/NextToken are parsed but ListTableStorageOptimizers returns the full unpaginated list. Left as reported-but-unfixed: at most 3 StorageOptimizerType values exist per table (COMPACTION/GARBAGE_COLLECTION/RETENTION), so truncation can never actually be observed against any real MaxResults value -- same bug class as the FilterConditionList/ResourceShareType fixes above, but bounded low enough in impact that fix effort went to those instead."
  - "FIXED (gopherstack-kbnu): GetResourceLFTags/AddLFTagsToResource/RemoveLFTagsFromResource now reject Resource kinds other than Database/Table/TableWithColumns with InvalidInputException, matching the documented restriction (\"The database, table, or column resource...\", api_op_GetResourceLFTags.go:30-33 / api_op_AddLFTagsToResource.go:29-31; RemoveLFTagsFromResource states it explicitly: \"Only database, table, or tableWithColumns resource are allowed.\", api_op_RemoveLFTagsFromResource.go:12-14, aws-sdk-go-v2/service/lakeformation@v1.50.4). Was a permissive superset (accepted Catalog/DataLocation/DataCellsFilter/LFTag/LFTagExpression/LFTagPolicy too) -- the same bug class as a glacier-pass finding the same day (gopherstack accepting a clause AWS rejects)."
deferred: []  # previously: Condition/RowFilter AllRowsWildcard, ColumnWildcard, LFTagPolicyResource -- ALL implemented this pass (see resource_union family + CreateDataCellsFilter note). The prior claim that RedshiftScopeUnion/ServiceIntegrationUnion had no routed wire surface was WRONG (disproved gopherstack-6flj, 2026-08-15): ServiceIntegrations is a real member of CreateLakeFormationIdentityCenterConfigurationInput/UpdateLakeFormationIdentityCenterConfigurationInput/DescribeLakeFormationIdentityCenterConfigurationOutput, all three of them routed ops. Now implemented -- see the identity-center ops above and the ServiceIntegration/RedshiftScopeUnion/RedshiftConnect types in models.go.
leaks: {status: clean, note: "no new goroutines/janitors added this pass; all new backend methods take b.mu via existing lockmetrics.RWMutex Lock/RLock with defer Unlock/RUnlock, following the pre-existing pattern."}
---

## Notes

**2026-08-22 (gopherstack-i8lo):** verified the DataCellsFilter op family's
required-member handling in both directions, following up on a report that
had misfiled `CreateDataCellsFilter` as a glue op (it is Lake Formation's;
does not exist in glue at all) and had undercounted its required members
by one (three named, not the real four).

`CreateDataCellsFilterInput` marks one top-level member required --
`TableData *types.DataCellsFilter` (`api_op_CreateDataCellsFilter.go:29-37`,
`lakeformation@v1.50.4`, confirmed identical on `v1.47.3`) -- and
`types.DataCellsFilter` itself marks **four** required, one level down:
`DatabaseName`, `Name`, `TableCatalogId`, `TableName` (`types/types.go:153-173`).
Both `CreateDataCellsFilter` and `UpdateDataCellsFilter`
(`data_cells_filter.go:42-84`, `168-198`) already validated all four before
this pass -- confirmed by the pre-existing table-driven
`TestCreateDataCellsFilter_RequiredFields`/`TestUpdateDataCellsFilter_RequiresAllFields`.
No fix needed on either op; the original report's claim of three
unvalidated members on `CreateDataCellsFilter` was wrong.

Reading the rest of the family (this service already had two
over-validations fixed this session, `GetTemporaryDataLocationCredentials`
and `ListDataCellsFilter` -- gopherstack-4ly2) turned up two real, opposite
bugs the issue never named:

- **`GetDataCellsFilter` -- under-validated.** `GetDataCellsFilterInput`
  marks all four of `TableCatalogId`, `DatabaseName`, `TableName`, `Name`
  required (`api_op_GetDataCellsFilter.go:29-48`; confirmed against the
  official AWS API reference, which lists all four `Required: Yes`). The
  backend (`GetDataCellsFilter`, `data_cells_filter.go:149-153` pre-fix)
  validated only `Name`. Fixed by adding the three missing checks, same
  file. Proof: `TestGetDataCellsFilter_RequiredFields` (table-driven, raw
  JSON -- a real SDK client cannot produce this malformed a request at
  all, since its own generated `validateDataCellsFilter`
  (`validators.go:1259-1279`) rejects the same four client-side before
  dialing out; `TestGetDataCellsFilter_RealSDKClient_RequiredFieldsRejectedClientSide`
  demonstrates that client-side rejection directly, and is why this gap
  was unreachable by any typed caller). Hand-reverted to confirm both new
  tests fail against the pre-fix code, then restored byte-identical
  (md5sum-verified).
- **`DeleteDataCellsFilter` -- over-validated.** `DeleteDataCellsFilterInput`
  marks **none** of its four members required (`api_op_DeleteDataCellsFilter.go:27-42`;
  confirmed against the official AWS API reference, all four
  `Required: No`) -- the mirror of the Get bug above, same op family. The
  backend demanded `Name` be non-empty. Removed; a request omitting Name
  now falls through to the pre-existing not-found path instead of a
  synthetic 400, since nothing in the store is keyed by an empty name.
  Proof: `TestDeleteDataCellsFilter_RealSDKClient_AllFieldsOptional` sends
  a real, all-fields-omitted `DeleteDataCellsFilterInput` through an
  unmodified `aws-sdk-go-v2` client -- a successful dial-out is itself
  proof the client's own validator does not consider any of these fields
  required, since it would otherwise refuse to send the request at all.
  The existing `TestDeleteDataCellsFilter_MissingName` fixture had ratified
  the defect (asserted 400 for empty Name); corrected to assert 404.

Checks considered and **not** added: none. Both directions of this op
family are now confirmed against the pinned SDK's required markers and the
official AWS API reference; no additional ambiguous/conditional members
were found on Create/Update/Get/Delete/List.

`ListDataCellsFilter` was previously fixed for the same over-validation
class (gopherstack-4ly2, see its ops-table note above) and needed no
further change this pass.

**2026-08-15 (gopherstack-6flj wrapper-key sweep):** re-verified all 26
List/Describe/Get ops against the real deserializer/serializer independently
of this file's own prior "A grade" claims. The 26 ops' wrapper keys held
completely clean, but three adjacent ops in the temporary-credentials/
identity-center families the prior passes hadn't reached had real bugs, one
wire-breaking: `GetTemporaryDataLocationCredentials`'s request struct was
copied from its `GetTemporaryGlue*Credentials` siblings
(`ResourceArn`/`Permissions`/`SupportedPermissionTypes`, none of them real
for this op) instead of the real `DataLocations`/`CredentialsScope` shape --
no real client's request was ever readable. Also fixed: `GetTemporaryGlueTableCredentials`'s
missing `S3Path`/`VendedS3Path` pair; `DescribeLakeFormationIdentityCenterConfigurationOutput`
fabricating `ApplicationStatus` (real only as `Update`'s request field, a
real key on the wrong op/direction); and this file's own prior `deferred:`
claim that no routed op takes `ServiceIntegrationUnion` -- disproved, it is
real on `Create`/`Update`/`Describe`, now implemented, and
`UpdateLakeFormationIdentityCenterConfigurationInput` was separately missing
a `ShareRecipients` field entirely. Full detail, including every
hand-revert-and-confirm cycle, in
`services/_WRAPPER_KEY_SWEEP_REMAINDER.md`'s "lakeformation (this session)"
section -- kept short here per this issue's "notes field is saturated"
convention.

**2026-08-15 (gopherstack-3gbe):** investigated whether Lake Formation
shares Omics' (gopherstack-keee) client-side host-prefix-rewrite
reachability gap. It does: **5 ops**, two literal prefixes, confirmed
against the pinned `lakeformation@v1.50.4` module -- `query-`
(StartQueryPlanning `api_op_StartQueryPlanning.go:143`, GetQueryState
`api_op_GetQueryState.go:149`, GetQueryStatistics
`api_op_GetQueryStatistics.go:137`, GetWorkUnits
`api_op_GetWorkUnits.go:242`) and `data-` (GetWorkUnitResults
`api_op_GetWorkUnitResults.go:143`) -- exactly matching gopherstack-3gbe's
filing.

No routing/auth code needed changing. `Handler.RouteMatcher`
(`handler.go:193`) matches on `URL.Path` alone, gated on the SigV4 service
name (already SigV4-scoped and confirmed clean in
`services/_ROUTE_COLLISIONS.md`), and `ExtractOperation` (`handler.go:208`)
is just the path with its leading slash stripped -- the host-prefix rewrite
only ever touches `Host`, never `Path`, so it structurally can't create a
route-table collision here. The reachability gap is a pure client-side
DNS/dial failure, same as Omics.

Found (not introduced) an existing host-prefix workaround:
`handler_work_unit_results_sdk_test.go`'s `disableDataHostPrefix` is applied
to the whole SDK client via `o.APIOptions`, so
`TestGetWorkUnitResults_WorkUnitID_RoundTrip` disables the rewrite for
*every* op it calls (StartQueryPlanning and GetWorkUnits too, both
`query-`, not just GetWorkUnitResults's `data-`) -- it does not exercise a
real, unmodified client, the same class of gap `disableAnalyticsHostPrefix`
was for Omics before gopherstack-keee. Added
`host_prefix_reachability_test.go` following
`services/omics/host_prefix_reachability_test.go`'s before/after pattern: a
before-fix test proving the unmodified client can't dial either prefix, and
an after-fix test that drives StartQueryPlanning -> GetQueryStatistics ->
GetWorkUnits -> GetWorkUnitResults through a redial-to-the-real-listener
transport, leaving the SDK's real, un-disabled rewrite intact on the wire
for both prefixes, and asserts the full round trip succeeds with correctly
decoded values. Gates green: build, vet, race, `go fix -diff` (no diff),
golangci-lint (0 findings).

**2026-08-13 (gopherstack-jqh2 pass 3):** re-extracted all 61 ops' real
method+path directly from `lakeformation@v1.50.4` serializers.go and drove
them through `ExtractOperation` via the new
`handler_sdk_route_table_test.go` (`TestExtractOperation_SDKRouteTable`, one
subtest per op, `t.Parallel()`). Lake Formation's real API uses a static
literal `/<OperationName>` path per op (confirmed directly in
serializers.go), so `ExtractOperation`'s "strip the leading slash" logic is
structurally exact by construction. Went further and diffed all three of
this service's op-name tables against the 61-op SDK set:
`isLakeFormationPath`'s switch (handler.go), `buildOps`' dispatch map
(handler.go), and `GetSupportedOperations`' advertised list — all three
match exactly, no drift between them (the bug shape 4 concern: a parallel
op-resolution table drifting from real dispatch). No pre-existing test
covered this; no new routing bugs found. This test is now the permanent
regression guard for route-table drift.

Freeform: AWS-behavior specifics worth remembering.

- **gopherstack-kbnu follow-up (2026-08-10)**: closed all three named gaps from the prior
  pass's `gaps:` list, to varying degrees -- see the corresponding `gaps:` entries above for
  citations. Summary: `LastUpdatedBy` now stamped via a `ctx`-threaded synthetic caller ARN
  (`GrantPermissions`/`RevokePermissions`/`Batch*` signatures gained a `context.Context`
  first param); `GetEffectivePermissionsForPath` now expands `LFTagPolicy` grants against a
  resource's actual LF-tags (`ListPermissions` intentionally left alone -- matches AWS's
  documented behavior that tag-based grants aren't visible when listing by concrete
  resource); `GetResourceLFTags`/`AddLFTagsToResource`/`RemoveLFTagsFromResource` now reject
  non-Database/Table/TableWithColumns resources. `AdditionalDetails`/RAM stays documented
  as blocked -- confirmed no cross-service backend wiring pattern exists anywhere in this
  repo (checked `services/ram`, and `s3`<->`kms` as a second data point).

  Two adjacent bugs fixed alongside: `resourceToKey` had no `TableWithColumns` case at all
  (every TableWithColumns resource collided under the same `""` key, so
  `AddLFTagsToResource`/`GetResourceLFTags` on unrelated tables leaked tags into each
  other); and `getResourceLFTagsOutput.LFTagsOnColumns` was typed `[]LFTagPair` instead of
  the real `[]types.ColumnLFTag` (`api_op_GetResourceLFTags.go:53`,
  `aws-sdk-go-v2/service/lakeformation@v1.50.4`) -- a disguised stub, since no code path
  had ever populated it.

  Note: this pass's citations were already checked against the repo's actual pinned SDK,
  `aws-sdk-go-v2/service/lakeformation@v1.50.4` (`go.mod`/`go.sum`), even though the
  `sdk_module:` header above still read the stale `v1.47.3` from the last full audit --
  fixed by the gopherstack-u8my pin sweep (header now reads v1.50.4). Diffed v1.47.3 against
  v1.50.4: `types/enums.go`, `types/errors.go`, `serializers.go`, `deserializers.go`, and
  `validators.go` are byte-identical; `types/types.go` gained only a doc-comment addition
  (`DataLakeSettings.Parameters`' `SET_SOURCE_IDENTITY` key). No wire-shape claim in this
  file changed as a result. Separately, re-verifying `permission_enum`'s member count while
  re-pinning found the note itself wrong (fixed above: 16 members, not 15) -- unrelated to
  the version bump, since `enums.go` didn't change between the two pins.

- **`ListPermissions` request shape was wire-broken** (this pass's headline fix):
  gopherstack's `listPermissionsInput` had a flat `ResourceArn string` field, but the real
  `types.ListPermissionsInput` has NO `ResourceArn` field at all -- it filters by `Resource
  *types.Resource`, the same nested union shape `GrantPermissionsInput`/
  `RevokePermissionsInput` use. `GetEffectivePermissionsForPath` is the *only* op in this
  family that genuinely uses a flat `ResourceArn` (confirmed against
  `api_op_GetEffectivePermissionsForPath.go`). A real `aws-sdk-go-v2` client's
  `ListPermissions` call would have serialized `{"Resource": {...}}` and gopherstack would
  have silently ignored it (empty/no filter applied, since the old code read a field the
  client never sent). Fixed by changing `listPermissionsInput.Resource *Resource`,
  `StorageBackend.ListPermissions`'s first parameter, and adding `permissionMatchesResource`
  (per-resource-kind matching, mirroring `resourceToKey`'s union) to replace the old
  `permissionMatchesARN`-based filtering for this op specifically.

- **`Resource` union was missing 4 of 9 real members**: `types.Resource` has
  `Catalog/Database/Table/TableWithColumns/DataLocation/DataCellsFilter/LFTag/
  LFTagExpression/LFTagPolicy`. gopherstack only had the first five. `LFTag`
  (`LFTagKeyResource`) and `LFTagPolicy` (`LFTagPolicyResource`) are real, commonly-used
  permission targets (LF-tag-based access control is a headline Lake Formation feature) --
  granting a permission against an LF-tag or LF-tag-policy resource previously had nowhere
  to put the resource-kind-specific fields on the wire at all. Added all 4 missing kinds
  plus their nested types (`LFTagKeyResource`, `LFTagExpressionResource`,
  `LFTagPolicyResource`, `DataCellsFilterResource`, `TableWildcard`, `ColumnWildcard`,
  `Condition`), extended `resourceToKey`/`copyResource`/`permissionMatchesResource`/
  `permissionMatchesResourceType` (including the `LF_TAG`/`LF_TAG_POLICY`/
  `LF_TAG_POLICY_DATABASE`/`LF_TAG_POLICY_TABLE`/`LF_NAMED_TAG_EXPRESSION` values of
  `types.DataLakeResourceType`, previously entirely unhandled by `permissionMatchesResourceType`'s
  switch).

- **`BatchPermissionsRequestEntry.Id` was entirely missing**: the real
  `types.BatchPermissionsRequestEntry` (used by `BatchGrantPermissionsInput`/
  `BatchRevokePermissionsInput`) has a *required* `Id` member specifically so
  `BatchPermissionsFailureEntry.RequestEntry` (in the response) can be correlated back to
  the request entry that produced it. gopherstack previously reused the plain
  `PermissionEntry` type (no `Id` field) for batch entries, so a caller could never tell
  which of N failed entries a given `Failures[]` item referred to when entries were
  otherwise structurally similar. Added `BatchPermissionsRequestEntry` as its own type with
  `Id string \`json:"Id"\``, used it for `batchGrantPermissionsInput.Entries`/
  `batchRevokePermissionsInput.Entries` and `BatchFailureEntry.RequestEntry`, and added
  request-level validation (`validateBatchPermissionsEntries`) rejecting any entry missing
  `Id` with `InvalidInputException` before touching the backend.

- **Three invented `Permission` enum values, one missing**: `isValidPermission` accepted
  `"CREATE_TAG"` and `"SUPER"` -- neither exists in `types.Permission`'s `Values()`; the
  real names are `CREATE_LF_TAG` (already separately present in gopherstack's list, so
  `CREATE_TAG` was a pure duplicate-with-wrong-name) and `SUPER_USER`. It also accepted
  `"CREATE_LAKE_FORMATION_OPT_IN"`, which is not a `Permission` value at all in any form
  (opt-ins are a separate `CreateLakeFormationOptIn` *operation*, not a grantable
  permission). Deleted all three per the no-invented-values rule. Also added the real
  `"CREATE_LF_TAG_EXPRESSION"` value, which was missing entirely. Verified against
  `types.Permission.Values()` in `enums.go` (16 members) -- gopherstack's list now matches
  exactly.

- **`RegisterResource`/`UpdateResource` dropped 5 real `ResourceInfo` fields on the floor**:
  `registerResourceInput` already parsed `WithFederation`/`HybridAccessEnabled` from the
  wire, but `StorageBackend.RegisterResource(resourceArn, roleArn string)`'s signature had
  no parameter to carry them, so they were read and silently discarded; `ResourceInfo` also
  lacked `ExpectedResourceOwnerAccount`/`VerificationStatus`/`WithFederation`/
  `WithPrivilegedAccess` entirely (real `types.ResourceInfo` has all of these). Fixed by
  adding `RegisterResourceOptions` (a trailing options struct, so the 2-arg call shape
  `RegisterResource(arn, role, opts)` stays mechanically simple at call sites) threaded
  through to a `ResourceInfo` that now carries all 5 fields; `VerificationStatus` is always
  reported `"VERIFIED"` since the emulator never performs real IAM-access verification of
  the registered role.

- **`DataCellsFilter.ColumnWildcard`/`VersionId` were missing**: real
  `types.DataCellsFilter` documents "`You must specify either a ColumnNames list or the
  ColumnWildCard`" -- gopherstack accepted `ColumnNames` only, silently dropping
  `ColumnWildcard` if a client sent it, and never validated the two are mutually exclusive.
  Added `ColumnWildcard *ColumnWildcard` (with `ExcludedColumnNames`) and `VersionId
  string`, a `validateDataCellsFilterColumns` check rejecting both-specified as
  `InvalidInputException`, and VersionId assignment (random hex, mirroring the existing
  synthetic-credential-ID pattern in `credentials.go`) on every Create/Update.

- **`DataLakeSettings.ExternalDataFilteringAllowList` was missing** from the 9-field real
  `types.DataLakeSettings` struct -- added as a 10th field, deep-copied like the other
  `[]DataLakePrincipal` fields in `copyDataLakeSettings`.

- **File rename for clarity**: `lf_tag_policy.go`/`handler_lf_tag_policy.go` implemented
  `CreateLFTagExpression`/`GetLFTagExpression`/etc. (the *LFTagExpression* op family) --
  not the `LFTagPolicyResource` permission-resource kind, which didn't exist in this
  codebase at all before this pass and is now a real, distinct concept (see `resource_union`
  family above). Renamed to `lf_tag_expression.go`/`handler_lf_tag_expression.go` (and the
  paired test file) to stop the name colliding with the new, unrelated `LFTagPolicy`
  concept in future audits.

--- carried forward from the 2026-07-12 audit (files unchanged this pass unless noted above) ---

- **Protocol**: restjson1. All 61 ops POST to a literal `/OperationName` path with no
  path parameters -- verified byte-for-byte against
  `aws-sdk-go-v2/service/lakeformation@v1.47.3`'s `serializers.go` (`SplitURI("/...")`
  calls). `isLakeFormationPath` in `handler.go` matches exactly.

- **Timestamp wire format**: several fields are declared `*time.Time` (or timestamp-typed)
  in the real SDK and MUST serialize as epoch-seconds JSON numbers (restjson1's
  `unixTimestamp` format), never RFC3339 strings: `ResourceInfo.LastModified`,
  `LakeFormationOptInsInfo.LastModified`, `TransactionDescription.TransactionStartTime`/
  `TransactionEndTime`, `TemporaryCredentials.Expiration`,
  `AssumeDecoratedRoleWithSAMLOutput.Expiration`,
  `GetTemporaryGluePartitionCredentialsOutput.Expiration`,
  `GetTemporaryGlueTableCredentialsOutput.Expiration`, and now also
  `PrincipalResourcePermissions.LastUpdated` (new field, wired correctly as epoch seconds
  from the start via `permissionEntryWire`, not a retrofit). See `pkgs/awstime.Epoch`.

- **Credential response nesting quirk**: `GetTemporaryDataLocationCredentialsOutput` nests
  `Expiration` inside a `Credentials: types.TemporaryCredentials` object, but
  `GetTemporaryGluePartitionCredentialsOutput`/`GetTemporaryGlueTableCredentialsOutput`
  return credential fields FLAT with no `Credentials` wrapper -- three sibling "get
  temporary credentials" ops, three different response shapes.

- **Transaction conflict exception types**: `CancelTransaction`/`ExtendTransaction`/
  `DeleteObjectsOnCancel` distinguish `TransactionCommittedException` from
  `TransactionCanceledException` per-op (not interchangeable); see `errTransactionCommitted`
  in `errors.go`/`handler.go`.

## gopherstack-wlo1 (2026-08-22): Handler()'s method-not-allowed branch was untyped

`Handler()`'s own `if c.Request().Method != http.MethodPost { return
c.String(http.StatusMethodNotAllowed, "Method not allowed") }` guard
(handler.go) wrote a bare text/plain 405. LakeFormation is restjson1
(`lakeformation@v1.50.4` `awsRestjson1_` prefix; error decode via
`restjson.GetErrorInfo`), so a real client saw
`smithy.GenericAPIError{Code:"UnknownError"}`.

Reachability: `RouteMatcher` (handler.go) matches purely on URL path
(`isLakeFormationPath`) and the SigV4 credential scope's service component
-- it never inspects the HTTP method -- so a request with any other method
still routes to `Handler()`.

Fixed: uses the existing `writeError(c, http.StatusMethodNotAllowed,
"InvalidInputException", "Method not allowed")` -- `InvalidInputException`
is the same code this file's own `handleError` already uses for
`ErrValidation`, so no new exception vocabulary was introduced.

Proof: `TestHandler_WrongMethodSurfacesInvalidInputException`
(`handler_dispatch_malformed_test.go`) drives a real LakeFormation client's
`GetDataLakeSettings` through a Finalize-stage middleware that rewrites the
request's HTTP method to PUT post-signing. Hand-reverted `handler.go` to
`git show HEAD`, confirmed the test fails with `*json.SyntaxError: "invalid
character 'M' looking for beginning of value"`, restored the fix,
`md5sum`-confirmed byte-identical.

**Per-item-failure sweep (this pass):** checked `AddLFTagsToResource`,
`RemoveLFTagsFromResource` (`Failures []types.LFTagError`) and
`BatchGrantPermissions`/`BatchRevokePermissions` (`Failures
[]types.BatchPermissionsFailureEntry`). All four correctly populate their per-item
`Failures` field: `lf_tags.go`'s `AddLFTagsToResource`/`RemoveLFTagsFromResource`
report `EntityNotFoundException` for an unknown tag key or a tag value outside the
tag's allowed values, while still applying every other pair in the same call;
`permissions.go`'s `BatchGrantPermissions`/`BatchRevokePermissions` surface real
validation failures from `grantPermissionsLocked`/`revokePermissionsLocked` (nil
principal/resource, invalid permission enum, grant-option-not-a-subset-of-permissions)
per entry, continuing to process the rest of the batch. No bugs found in this class.

## 2026-08-29 pagination-helper arithmetic sweep (wrapper-key-sweep campaign)

Audited this package's pagination for the Class A/B/C shapes found
elsewhere in this campaign. No bug found.

The generic `paginate[T]` (`store.go`) is a thin, direct wrapper over
`pkgs/page.New` — this package is the one of the eight audited this pass
that actually reuses the shared helper rather than reimplementing it, at 9
call sites (`data_cells_filter.go`, `lf_tag_expression.go`, `opt_ins.go`,
`table_storage.go`, `resources.go`, `lf_tags.go`, `permissions.go` x2,
`transactions.go`). `pkgs/page` carries its own exhaustive suite
(`pkgs/page/page_test.go`), not re-derived here; a boundary walk and
tampered-token round trip against `ListLFTags` through the real
`aws-sdk-go-v2/service/lakeformation` client
(`pagination_sdk_roundtrip_test.go`) ties that reuse to observable
behaviour.

The two helpers this package hand-rolls instead —
`paginateTaggedTables`/`paginateTaggedDatabases` (`lf_tags.go`, backing
`SearchTablesByLFTags`/`SearchDatabasesByLFTags`, 1 op each) — parse an
offset token via a manual decimal-digit loop rather than `strconv`/
`pkgs/page`, but land on the same offset-clamp algorithm (`startIdx >=
len(list)` before slicing), correct and near-duplicative of `pkgs/page`
rather than buggy. All seven checks pass directly against both
(`pagination_arithmetic_internal_test.go`), including a stale/malformed
token past the end (clamps to an empty page, doesn't panic or restart).

Gates: `go build ./services/lakeformation/...`, `go vet
./services/lakeformation/...` and `go vet ./...` (repo-wide, clean),
`go test -race -count=1 ./services/lakeformation/...`, `golangci-lint run
./services/lakeformation/...` (0 issues). No production code changed this
pass — test-only additions confirming correctness.
