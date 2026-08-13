---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: lakeformation
sdk_module: aws-sdk-go-v2/service/lakeformation@v1.50.4
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
  AddLFTagsToResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now rejects non-Database/Table/TableWithColumns Resource kinds (was a permissive superset of what AWS accepts, see gopherstack-kbnu); resourceToKey also fixed to key TableWithColumns distinctly (previously had no case for it at all -- every TableWithColumns resource collided under the same empty-string key)"}
  RemoveLFTagsFromResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "same resource-kind restriction fix as AddLFTagsToResource"}
  GetResourceLFTags: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same resource-kind restriction as AddLFTagsToResource/RemoveLFTagsFromResource; also fixed getResourceLFTagsOutput.LFTagsOnColumns, which was typed []LFTagPair -- the real GetResourceLFTagsOutput.LFTagsOnColumns is []types.ColumnLFTag (Name+LFTags) -- and was never populated by any code path (disguised stub)"}
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
  GetEffectivePermissionsForPath: {wire: ok, errors: ok, state: ok, persist: ok, note: "unlike ListPermissions, this op genuinely does filter by a flat ResourceArn per the real API -- kept its own ARN-based implementation (permissionMatchesARN) rather than sharing ListPermissions' new Resource-shaped filter. Also now expands LFTagPolicy grants: resolves resourceArn to a Database/Table resource, looks up its actual assigned LF-tags (resourceLFTags), and includes any LFTagPolicy-resource grant whose Expression/ExpressionName is satisfied (AND across tag keys, OR across one key's values) -- previously a tag-policy grant was invisible here entirely since it has no Database/Table field to ARN-match against (gopherstack-kbnu)"}
  GetDataLakePrincipal: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetDataLakeSettings: {wire: ok, errors: ok, state: ok, persist: ok, note: "ExternalDataFilteringAllowList field added (was entirely missing from DataLakeSettings)"}
  PutDataLakeSettings: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  route_matcher: {status: ok, note: "unchanged this pass -- isLakeFormationPath's 61 literal paths still verified byte-for-byte against serializers.go from the prior audit; no new ops added upstream (61 ops, serializers.go byte-identical, from v1.47.3 through the now-current v1.50.4 pin)."}
  resource_union: {status: ok, note: "Resource previously only carried Catalog/Database/Table/TableWithColumns/DataLocation. Added DataCellsFilter/LFTag(LFTagKeyResource)/LFTagExpression/LFTagPolicy(LFTagPolicyResource) -- all real types.Resource union members. GrantPermissions/RevokePermissions/ListPermissions now work end-to-end against every kind (resourceToKey/copyResource/permissionMatchesResource/permissionMatchesResourceType all extended); see handler_permissions_resource_kinds_test.go for coverage of all 6 previously-partial/deferred kinds plus TableWildcard and CatalogResource.Id."}
  permission_enum: {status: ok, note: "isValidPermission previously accepted three gopherstack-INVENTED permission strings that do not exist in types.Permission's Values() at all -- \"CREATE_TAG\" (real name is CREATE_LF_TAG, already separately present), \"CREATE_LAKE_FORMATION_OPT_IN\" (not a Permission at all), and \"SUPER\" (real value is SUPER_USER) -- and was missing the real \"CREATE_LF_TAG_EXPRESSION\" value. All three invented values DELETED, CREATE_LF_TAG_EXPRESSION added. isValidPermission now matches the real 16-member enum exactly."}
gaps:
  - "FIXED (gopherstack-kbnu): PrincipalResourcePermissions.LastUpdatedBy is now populated by GrantPermissions/RevokePermissions/BatchGrantPermissions/BatchRevokePermissions with a synthetic caller ARN derived from awsmeta.Account(ctx) (callerPrincipalARN, credentials.go -- same identity GetDataLakePrincipal reports). Interface signatures gained a ctx context.Context first parameter; all callers updated."
  - "PrincipalResourcePermissions.AdditionalDetails (DetailsMap.ResourceShare, RAM resource-share info) is still never populated. Re-checked this pass: gopherstack DOES have a standalone services/ram package (resource shares, principals, permissions), but there is no cross-service wiring between it and lakeformation anywhere in the codebase (no service in this repo reaches into another service's InMemoryBackend directly -- checked s3<->kms as a second data point, same finding). Populating this would require introducing a new cross-service backend-injection pattern, which is out of scope for a single-service follow-up. Correctly omitted rather than fabricated."
  - "PARTIALLY FIXED (gopherstack-kbnu): LFTagPolicy-based permission grants are now expanded into effective per-resource permissions in GetEffectivePermissionsForPath (resolves the resourceArn to a Database/Table, looks up its actual LF-tags, and evaluates each LFTagPolicy grant's Expression/ExpressionName against them -- AND across tag keys, OR across one key's values, per https://docs.aws.amazon.com/lake-formation/latest/dg/managing-tag-expressions.html). ListPermissions filtered by a concrete resource intentionally still does NOT expand tag-policy grants: AWS's own documented behavior is that LF-Tag-based grants are queried via their own LFTagPolicy/LF_TAG_POLICY_* resource type, not by listing the concrete resource they happen to cover (a tag-based grant 'may not appear in ListPermissions results for specific resources'). SearchTablesByLFTags/SearchDatabasesByLFTags remain untouched (out of scope for this pass -- they answer 'which resources have these tags', not 'what permissions apply to this resource'). No LakeFormation operation in this backend enforces authorization at runtime (permissions are bookkeeping, not an enforcement engine); this pass only makes the LF-Tag-derived permission *record* visible where AWS documents it should be, it does not add access control."
  - "FIXED (gopherstack-kbnu): GetResourceLFTags/AddLFTagsToResource/RemoveLFTagsFromResource now reject Resource kinds other than Database/Table/TableWithColumns with InvalidInputException, matching the documented restriction (\"The database, table, or column resource...\", api_op_GetResourceLFTags.go:30-33 / api_op_AddLFTagsToResource.go:29-31; RemoveLFTagsFromResource states it explicitly: \"Only database, table, or tableWithColumns resource are allowed.\", api_op_RemoveLFTagsFromResource.go:12-14, aws-sdk-go-v2/service/lakeformation@v1.50.4). Was a permissive superset (accepted Catalog/DataLocation/DataCellsFilter/LFTag/LFTagExpression/LFTagPolicy too) -- the same bug class as a glacier-pass finding the same day (gopherstack accepting a clause AWS rejects)."
deferred: []  # previously: Condition/RowFilter AllRowsWildcard, ColumnWildcard, LFTagPolicyResource -- ALL implemented this pass (see resource_union family + CreateDataCellsFilter note). RedshiftScopeUnion/ServiceIntegrationUnion (RedshiftConnect service-integration resource kinds, api_op none of the 61 routed ops reference them directly as request/response fields outside types.go) remain out of scope: no routed operation in the 61-op surface takes a RedshiftScopeUnion/ServiceIntegrationUnion as an input/output field, so there is no wire surface to implement against.
leaks: {status: clean, note: "no new goroutines/janitors added this pass; all new backend methods take b.mu via existing lockmetrics.RWMutex Lock/RLock with defer Unlock/RUnlock, following the pre-existing pattern."}
---

## Notes

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
