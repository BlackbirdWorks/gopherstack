---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: lakeformation
sdk_module: aws-sdk-go-v2/service/lakeformation@v1.47.3
last_audit_commit: 49e505cb
last_audit_date: 2026-07-12
overall: A            # genuine wire-breaking fixes found across timestamp/credential shapes
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  RegisterResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateResource: {wire: ok, errors: ok, state: ok, persist: ok}
  DeregisterResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascades permission cleanup for the resource"}
  DescribeResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: LastModified now epoch seconds, not RFC3339 string"}
  ListResources: {wire: ok, errors: ok, state: ok, persist: ok, note: "same LastModified fix as DescribeResource"}
  GrantPermissions: {wire: ok, errors: ok, state: ok, persist: ok}
  RevokePermissions: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPermissions: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchGrantPermissions: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchRevokePermissions: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateLFTag: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteLFTag: {wire: ok, errors: ok, state: ok, persist: ok}
  GetLFTag: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateLFTag: {wire: ok, errors: ok, state: ok, persist: ok}
  ListLFTags: {wire: ok, errors: ok, state: ok, persist: ok}
  AddLFTagsToResource: {wire: ok, errors: ok, state: ok, persist: ok}
  RemoveLFTagsFromResource: {wire: ok, errors: ok, state: ok, persist: ok}
  GetResourceLFTags: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDataCellsFilter: {wire: partial, errors: ok, state: ok, persist: ok, note: "ColumnWildcard input field silently ignored (gap, not fixed this pass)"}
  GetDataCellsFilter: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDataCellsFilter: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDataCellsFilter: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDataCellsFilter: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateLFTagExpression: {wire: ok, errors: ok, state: ok, persist: ok}
  GetLFTagExpression: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateLFTagExpression: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteLFTagExpression: {wire: ok, errors: ok, state: ok, persist: ok}
  ListLFTagExpressions: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateLakeFormationOptIn: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteLakeFormationOptIn: {wire: ok, errors: ok, state: ok, persist: ok}
  ListLakeFormationOptIns: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: LastModified now epoch seconds, not RFC3339 string"}
  CreateLakeFormationIdentityCenterConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeLakeFormationIdentityCenterConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateLakeFormationIdentityCenterConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteLakeFormationIdentityCenterConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  StartTransaction: {wire: ok, errors: ok, state: ok, persist: ok}
  CancelTransaction: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: already-committed case now returns TransactionCommittedException (was wrongly TransactionCanceledException, which is not even a valid CancelTransaction exception per the SDK model)"}
  CommitTransaction: {wire: ok, errors: ok, state: ok, persist: ok}
  ExtendTransaction: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: already-committed case now returns TransactionCommittedException instead of TransactionCanceledException"}
  DescribeTransaction: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: TransactionStartTime/EndTime now epoch seconds, not RFC3339 strings"}
  ListTransactions: {wire: ok, errors: ok, state: ok, persist: ok, note: "same timestamp fix as DescribeTransaction"}
  DeleteObjectsOnCancel: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed: already-committed case now returns TransactionCommittedException"}
  GetTableObjects: {wire: ok, errors: ok, state: ok, persist: n/a, note: "not persisted (matches pre-existing scope; tableObjects map was never in backendSnapshot)"}
  UpdateTableObjects: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetTemporaryDataLocationCredentials: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed: Expiration now nested inside Credentials as epoch seconds (was a wrong top-level RFC3339 string; real API nests it in TemporaryCredentials)"}
  GetTemporaryGluePartitionCredentials: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed: response is now flat (AccessKeyId/SecretAccessKey/SessionToken/Expiration at top level, epoch seconds) -- was wrongly nested under a Credentials object that the real SDK does not expect for this op"}
  GetTemporaryGlueTableCredentials: {wire: ok, errors: ok, state: ok, persist: n/a, note: "same flat-shape + epoch fix as GetTemporaryGluePartitionCredentials"}
  AssumeDecoratedRoleWithSAML: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed: Expiration now epoch seconds (was RFC3339 string); now honors DurationSeconds instead of hardcoding 1 hour"}
  StartQueryPlanning: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetQueryState: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetQueryStatistics: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetWorkUnits: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetWorkUnitResults: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListTableStorageOptimizers: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateTableStorageOptimizer: {wire: ok, errors: ok, state: ok, persist: ok}
  SearchDatabasesByLFTags: {wire: ok, errors: ok, state: ok, persist: n/a, note: "real implementation in backend_comprehensive.go, not a stub"}
  SearchTablesByLFTags: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetEffectivePermissionsForPath: {wire: ok, errors: ok, state: ok, persist: ok, note: "delegates to ListPermissions filtered by ARN"}
  GetDataLakePrincipal: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetDataLakeSettings: {wire: ok, errors: ok, state: ok, persist: ok}
  PutDataLakeSettings: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  route_matcher: {status: ok, note: "isLakeFormationPath's 61 literal paths verified byte-for-byte against every SplitURI(...) call in the real SDK's serializers.go -- exact 61/61 match, all POST. No route-matcher bug this pass."}
gaps:
  - CreateDataCellsFilter/UpdateDataCellsFilter accept only ColumnNames; ColumnWildcard (exclusion-list wildcard) input is silently dropped -- not a fabricated response, just an unimplemented optional field (bd: file if picked up)
  - RegisterResource accepts WithFederation/HybridAccessEnabled in the request but the backend signature (RegisterResource(resourceArn, roleArn string)) drops them; ResourceInfo also lacks ExpectedResourceOwnerAccount/HybridAccessEnabled/VerificationStatus/WithFederation/WithPrivilegedAccess fields present on the real types.ResourceInfo. Not fixed this pass (would require a StorageBackend interface signature change); flagged for a follow-up bd issue.
  - PrincipalResourcePermissions (ListPermissions/GrantPermissions responses) omits the real API's optional LastUpdated/LastUpdatedBy/Condition/AdditionalDetails fields entirely (never populated) -- acceptable per protocol (optional, omitted when absent) but worth backfilling LastUpdated/LastUpdatedBy for closer parity.
deferred:
  - Condition/RowFilter AllRowsWildcard, ColumnWildcard, LFTagPolicyResource, RedshiftScopeUnion, ServiceIntegrationUnion (newer LF-tag-policy / Redshift-integration resource kinds) -- entire families out of scope this pass.
leaks: {status: clean, note: "StartJanitor goroutine (backend.go) is ctx-scoped with a ticker.Stop() defer and select on ctx.Done(); no new goroutines/janitors added this pass."}
---

## Notes

Freeform: AWS-behavior specifics worth remembering.

- **Protocol**: restjson1. All 61 ops POST to a literal `/OperationName` path with no
  path parameters -- verified byte-for-byte against
  `aws-sdk-go-v2/service/lakeformation@v1.47.3`'s `serializers.go` (`SplitURI("/...")`
  calls). `isLakeFormationPath` in `handler.go` matches exactly; no route-matcher bug
  found this sweep (unlike the backup/eks/s3control/... class of bugs called out in
  parity-principles.md).

- **Timestamp wire format (the main bug class this sweep)**: several fields are declared
  `*time.Time` (or timestamp-typed) in the real SDK and MUST serialize as epoch-seconds
  JSON numbers (restjson1's `unixTimestamp` format), never RFC3339 strings. Confirmed via
  `smithytime.ParseEpochSeconds`/`ParseEpochSeconds` calls in the real deserializer for:
  `ResourceInfo.LastModified`, `LakeFormationOptInsInfo.LastModified`,
  `TransactionDescription.TransactionStartTime`/`TransactionEndTime`,
  `TemporaryCredentials.Expiration`, `AssumeDecoratedRoleWithSAMLOutput.Expiration`,
  `GetTemporaryGluePartitionCredentialsOutput.Expiration`,
  `GetTemporaryGlueTableCredentialsOutput.Expiration`. Before this fix, gopherstack
  emitted Go's default RFC3339-string `time.Time` encoding (or hand-formatted RFC3339
  strings) for all of these -- a real aws-sdk-go-v2 client would hard-fail parsing every
  one of these responses with "expected ...Timestamp to be a JSON Number, got string
  instead". Fixed via wire-only conversion at the handler boundary (`resourceInfoWire`,
  `lfOptInWire`, `transactionWire` in models.go, plus direct field-type changes for the
  ephemeral, never-persisted `SAMLCredentials`/`TemporaryCredentials`), so the internal
  domain types and persistence format are untouched -- see `pkgs/awstime.Epoch`.

- **Credential response nesting quirk**: `GetTemporaryDataLocationCredentialsOutput`
  nests its `Expiration` inside a `Credentials: types.TemporaryCredentials` object, but
  `GetTemporaryGluePartitionCredentialsOutput` and `GetTemporaryGlueTableCredentialsOutput`
  return `AccessKeyId`/`SecretAccessKey`/`SessionToken`/`Expiration` FLAT at the top level
  with no `Credentials` wrapper at all -- three sibling "get temporary credentials" ops
  with three different response shapes. Verified directly against the `api_op_*.go`
  struct definitions (not just the deserializer) since this is a shape difference, not
  just a format difference. gopherstack previously used the nested/wrapped shape for all
  three; now matches the real per-op shape.

- **Transaction conflict exception types**: the real SDK model gives `CancelTransaction`,
  `ExtendTransaction`, and `DeleteObjectsOnCancel` two *different* possible conflict
  exceptions -- `TransactionCommittedException` (transaction already committed) and
  `TransactionCanceledException` (transaction already aborted / write conflict) -- and
  they are NOT interchangeable per op: `CancelTransaction`'s valid exception set (per
  `awsRestjson1_deserializeOpErrorCancelTransaction`'s switch) includes
  `TransactionCommittedException` but NOT `TransactionCanceledException`; `CommitTransaction`'s
  is the mirror image. gopherstack previously routed every `awserr.ErrConflict` (from any
  of these ops) through one hardcoded `TransactionCanceledException`, which is not even a
  legal response for `CancelTransaction` on an already-committed transaction. Fixed with a
  local `errTransactionCommitted` sentinel (wraps `awserr.ErrConflict` so existing
  `errors.Is` checks keep working) checked before the generic conflict case in
  `handler.go`'s `handleError`.

- **Grep-based stub hunting false positive avoided**: `SearchDatabasesByLFTags` /
  `SearchTablesByLFTags` (backend_comprehensive.go) look like they could be a "search
  returns nothing" stub at a glance, but they genuinely scan `b.resourceLFTags` and
  evaluate the AND-of-any-value LF-tag expression semantics -- confirmed real, not
  flagged.

- **DeleteObjectsOnCancel state-check exception**: no exception in the real SDK model
  covers "transaction still ACTIVE" distinctly from "aborted" for this op (only
  `TransactionCanceledException` and `TransactionCommittedException` are valid) -- kept
  the existing `TransactionCanceledException` fallback for the ACTIVE case since it's the
  closest legal option and there's no better-documented AWS behavior to verify against
  without a live account; only added the `TransactionCommittedException` branch for
  the definitively-wrong committed case.
</content>
