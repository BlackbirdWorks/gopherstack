---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: lakeformation
sdk_module: aws-sdk-go-v2/service/lakeformation@v1.47.3
last_audit_commit: 4691484d9
last_audit_date: 2026-07-24
overall: A            # ListPermissions wire-shape bug + missing Resource union members fixed
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  RegisterResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: ExpectedResourceOwnerAccount/WithFederation/WithPrivilegedAccess/HybridAccessEnabled now threaded through to ResourceInfo via RegisterResourceOptions (previously silently dropped -- interface signature change)"}
  UpdateResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "same extended-fields fix as RegisterResource (ExpectedResourceOwnerAccount/WithFederation/HybridAccessEnabled)"}
  DeregisterResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascades permission cleanup for the resource"}
  DescribeResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "LastModified epoch seconds; now also emits ExpectedResourceOwnerAccount/VerificationStatus/HybridAccessEnabled/WithFederation/WithPrivilegedAccess"}
  ListResources: {wire: ok, errors: ok, state: ok, persist: ok, note: "same fixes as DescribeResource"}
  GrantPermissions: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: Condition now accepted/persisted; entry.LastUpdated stamped on every grant/merge; Resource union extended (see families below)"}
  RevokePermissions: {wire: ok, errors: ok, state: ok, persist: ok, note: "Condition now accepted; LastUpdated stamped on partial revoke"}
  ListPermissions: {wire: ok, errors: ok, state: ok, persist: ok, note: "WIRE-BREAKING BUG FIXED: request filtered by a flat ResourceArn string; the real ListPermissionsInput has no ResourceArn field at all -- it filters by a nested Resource object (same shape as Grant/RevokePermissions). A real aws-sdk-go-v2 client's ListPermissions call would never have matched anything against the old gopherstack shape. Response PrincipalResourcePermissions now wire-encodes LastUpdated as epoch seconds (permissionEntryWire) and includes Condition/LastUpdatedBy."}
  BatchGrantPermissions: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: entries now use BatchPermissionsRequestEntry with the real API's required Id field (previously entirely absent -- BatchFailureEntry.RequestEntry had no way to correlate back to the caller's request); also now applies the same PermissionsWithGrantOption-subset validation GrantPermissions does, per-entry"}
  BatchRevokePermissions: {wire: ok, errors: ok, state: ok, persist: ok, note: "same Id-field fix as BatchGrantPermissions"}
  CreateLFTag: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteLFTag: {wire: ok, errors: ok, state: ok, persist: ok}
  GetLFTag: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateLFTag: {wire: ok, errors: ok, state: ok, persist: ok}
  ListLFTags: {wire: ok, errors: ok, state: ok, persist: ok}
  AddLFTagsToResource: {wire: ok, errors: ok, state: ok, persist: ok}
  RemoveLFTagsFromResource: {wire: ok, errors: ok, state: ok, persist: ok}
  GetResourceLFTags: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDataCellsFilter: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: ColumnWildcard (ExcludedColumnNames) now accepted/persisted; ColumnNames+ColumnWildcard together rejected as InvalidInputException (real API: must specify exactly one); VersionId now assigned"}
  GetDataCellsFilter: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDataCellsFilter: {wire: ok, errors: ok, state: ok, persist: ok, note: "same ColumnWildcard/VersionId fix as CreateDataCellsFilter"}
  DeleteDataCellsFilter: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDataCellsFilter: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateLFTagExpression: {wire: ok, errors: ok, state: ok, persist: ok, note: "impl moved lf_tag_policy.go -> lf_tag_expression.go (file name was misleading: it implements LFTagExpression, not the distinct LFTagPolicyResource permission-resource kind added this pass)"}
  GetLFTagExpression: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateLFTagExpression: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteLFTagExpression: {wire: ok, errors: ok, state: ok, persist: ok}
  ListLFTagExpressions: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateLakeFormationOptIn: {wire: ok, errors: ok, state: ok, persist: ok, note: "Condition field added (LFOptIn/createLakeFormationOptInInput)"}
  DeleteLakeFormationOptIn: {wire: ok, errors: ok, state: ok, persist: ok, note: "Condition accepted (not part of the match key -- opt-ins are unique per principal+resource per AWS's documented AlreadyExistsException behavior)"}
  ListLakeFormationOptIns: {wire: ok, errors: ok, state: ok, persist: ok, note: "LastModified epoch seconds; Condition now included"}
  CreateLakeFormationIdentityCenterConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeLakeFormationIdentityCenterConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateLakeFormationIdentityCenterConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
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
  GetTemporaryDataLocationCredentials: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetTemporaryGluePartitionCredentials: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetTemporaryGlueTableCredentials: {wire: ok, errors: ok, state: ok, persist: n/a}
  AssumeDecoratedRoleWithSAML: {wire: ok, errors: ok, state: ok, persist: n/a}
  StartQueryPlanning: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetQueryState: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetQueryStatistics: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetWorkUnits: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetWorkUnitResults: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListTableStorageOptimizers: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateTableStorageOptimizer: {wire: ok, errors: ok, state: ok, persist: ok}
  SearchDatabasesByLFTags: {wire: ok, errors: ok, state: ok, persist: n/a, note: "real implementation in lf_tags.go, not a stub"}
  SearchTablesByLFTags: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetEffectivePermissionsForPath: {wire: ok, errors: ok, state: ok, persist: ok, note: "unlike ListPermissions, this op genuinely does filter by a flat ResourceArn per the real API -- kept its own ARN-based implementation (permissionMatchesARN) rather than sharing ListPermissions' new Resource-shaped filter"}
  GetDataLakePrincipal: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetDataLakeSettings: {wire: ok, errors: ok, state: ok, persist: ok, note: "ExternalDataFilteringAllowList field added (was entirely missing from DataLakeSettings)"}
  PutDataLakeSettings: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  route_matcher: {status: ok, note: "unchanged this pass -- isLakeFormationPath's 61 literal paths still verified byte-for-byte against serializers.go from the prior audit; no new ops added upstream at v1.47.3."}
  resource_union: {status: ok, note: "Resource previously only carried Catalog/Database/Table/TableWithColumns/DataLocation. Added DataCellsFilter/LFTag(LFTagKeyResource)/LFTagExpression/LFTagPolicy(LFTagPolicyResource) -- all real types.Resource union members. GrantPermissions/RevokePermissions/ListPermissions now work end-to-end against every kind (resourceToKey/copyResource/permissionMatchesResource/permissionMatchesResourceType all extended); see handler_permissions_resource_kinds_test.go for coverage of all 6 previously-partial/deferred kinds plus TableWildcard and CatalogResource.Id."}
  permission_enum: {status: ok, note: "isValidPermission previously accepted three gopherstack-INVENTED permission strings that do not exist in types.Permission's Values() at all -- \"CREATE_TAG\" (real name is CREATE_LF_TAG, already separately present), \"CREATE_LAKE_FORMATION_OPT_IN\" (not a Permission at all), and \"SUPER\" (real value is SUPER_USER) -- and was missing the real \"CREATE_LF_TAG_EXPRESSION\" value. All three invented values DELETED, CREATE_LF_TAG_EXPRESSION added. isValidPermission now matches the real 15-member enum exactly."}
gaps:
  - "PrincipalResourcePermissions.LastUpdatedBy is never populated (stays empty/omitted) -- the real value is the calling principal's ARN, but GrantPermissions/RevokePermissions have no caller-identity context threaded through them (unlike GetDataLakePrincipal, which derives a synthetic identity from awsmeta.Account(ctx) but takes a ctx param GrantPermissions doesn't have). Omitting an optional field is valid per protocol, so this is a completeness gap, not a wire-shape bug. Follow-up: thread ctx into GrantPermissions/RevokePermissions if worth the interface churn."
  - "PrincipalResourcePermissions.AdditionalDetails (DetailsMap.ResourceShare, RAM resource-share info) is never populated -- gopherstack has no RAM integration for Lake Formation resource shares, so this optional field is correctly always omitted rather than fabricated."
  - "LFTagPolicy-based permission grants are stored and returned literally (exact CatalogId+ResourceType+Expression match) but are NOT expanded into effective per-resource permissions -- i.e. GetEffectivePermissionsForPath/SearchTablesByLFTags/SearchDatabasesByLFTags do not cross-reference an LFTagPolicy grant against a table's actual LF-tags to compute implied access. This mirrors gopherstack's existing scope: no LakeFormation operation in this backend enforces authorization at all (permissions are bookkeeping, not an enforcement engine), so this is consistent with pre-existing behavior rather than a new gap, but is called out explicitly since LFTagPolicy is new this pass."
  - "GetResourceLFTags/AddLFTagsToResource/RemoveLFTagsFromResource accept any Resource kind (no restriction to Database/Table/TableWithColumns as AWS's docs describe) -- permissive superset, not under-permissive, so a real client's valid calls are unaffected; not tightened this pass given no observed client-visible symptom."
deferred: []  # previously: Condition/RowFilter AllRowsWildcard, ColumnWildcard, LFTagPolicyResource -- ALL implemented this pass (see resource_union family + CreateDataCellsFilter note). RedshiftScopeUnion/ServiceIntegrationUnion (RedshiftConnect service-integration resource kinds, api_op none of the 61 routed ops reference them directly as request/response fields outside types.go) remain out of scope: no routed operation in the 61-op surface takes a RedshiftScopeUnion/ServiceIntegrationUnion as an input/output field, so there is no wire surface to implement against.
leaks: {status: clean, note: "no new goroutines/janitors added this pass; all new backend methods take b.mu via existing lockmetrics.RWMutex Lock/RLock with defer Unlock/RUnlock, following the pre-existing pattern."}
---

## Notes

Freeform: AWS-behavior specifics worth remembering.

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
  `types.Permission.Values()` in `enums.go` (15 members) -- gopherstack's list now matches
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
