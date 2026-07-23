---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: macie2
sdk_module: aws-sdk-go-v2/service/macie2@v1.51.4
last_audit_commit: 82c8a1c8
last_audit_date: 2026-07-23
overall: A                # all 5 prior gaps + both deferred field audits closed this pass; zero gaps/deferred remain
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  EnableMacie: {wire: ok, errors: ok, state: ok, persist: ok}
  DisableMacie: {wire: ok, errors: ok, state: ok, persist: ok}
  GetMacieSession: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateMacieSession: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateAllowList: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAllowList: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAllowList: {wire: fixed, errors: ok, state: ok, persist: ok, note: "route method was PATCH; real SDK sends PUT /allow-lists/{id} -- unreachable via real client before fix"}
  DeleteAllowList: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAllowLists: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateCustomDataIdentifier: {wire: fixed, errors: ok, state: ok, persist: ok, note: "now accepts severityLevels (real CreateCustomDataIdentifierInput field) and threads it through to storage/Get/BatchGet"}
  GetCustomDataIdentifier: {wire: fixed, errors: ok, state: ok, persist: ok, note: "added 'deleted' and 'severityLevels' fields (real GetCustomDataIdentifierOutput has both). Also fixed a real-behavior bug: Get on a soft-deleted identifier previously 404'd -- real AWS soft-deletes (DeleteCustomDataIdentifier never hard-deletes), so Get must keep succeeding with deleted:true; only a never-existed ID 404s now."}
  DeleteCustomDataIdentifier: {wire: ok, errors: ok, state: ok, persist: ok}
  ListCustomDataIdentifiers: {wire: ok, errors: ok, state: ok, persist: ok}
  TestCustomDataIdentifier: {wire: ok, errors: ok, state: ok, persist: n/a}
  BatchGetCustomDataIdentifiers: {wire: fixed, errors: ok, state: ok, persist: n/a, note: "response wrapper key was 'items'; real key is 'customDataIdentifiers', so a real SDK client always deserialized an empty slice. Also added notFoundIdentifierIds, and (this pass) stopped silently excluding soft-deleted identifiers -- now returned with deleted:true, matching BatchGetCustomDataIdentifierSummary's real soft-delete field."}
  CreateFindingsFilter: {wire: ok, errors: ok, state: ok, persist: ok}
  GetFindingsFilter: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateFindingsFilter: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteFindingsFilter: {wire: ok, errors: ok, state: ok, persist: ok}
  ListFindingsFilters: {wire: ok, errors: ok, state: ok, persist: ok}
  GetFindings: {wire: fixed, errors: ok, state: ok, persist: ok, note: "Finding was missing count/partition/sample/schemaVersion/classificationDetails/resourcesAffected (real Finding shape); Severity.score was a float defaulting to 5.0 -- real types.Severity.Score is an int64 1-3, so 5.0 was out-of-range/not wire-compatible with real client expectations. All added; see also CreateSampleFindings note on the 'SENSITIVE_DATA' category bug."}
  ListFindings: {wire: ok, errors: ok, state: ok, persist: n/a, note: "criteria matching supports eq/neq on a handful of fields only -- acceptable reduced-scope emulation, not a stub"}
  CreateSampleFindings: {wire: fixed, errors: ok, state: ok, persist: ok, note: "Category was hardcoded to the INVENTED value 'SENSITIVE_DATA', which is not a valid FindingCategory (real enum is CLASSIFICATION/POLICY) -- deleted and replaced with prefix-derived CLASSIFICATION/POLICY. Findings now also populate count/partition/sample/schemaVersion and, for CLASSIFICATION findings, classificationDetails+resourcesAffected with realistic sample S3 bucket/object data, matching real Macie's sample-finding behavior of using non-empty example data."}
  GetFindingStatistics: {wire: ok, errors: ok, state: ok, persist: n/a}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateClassificationJob: {wire: fixed, errors: ok, state: ok, persist: ok, note: "response was missing jobArn entirely (real CreateClassificationJobOutput has only JobArn+JobId); ClassificationJob had no Arn field at all. Added Arn (json:jobArn), computed via arn.Build, threaded through Create+Describe. This pass: also added allowListIds/customDataIdentifierIds/managedDataIdentifierIds/managedDataIdentifierSelector (real CreateClassificationJobInput fields, previously dropped), and now writes create-time tags into the shared tags map (was only echoed on the job struct, so TagResource-added tags worked but Create-time tags never showed up via ListTagsForResource)."}
  DescribeClassificationJob: {wire: fixed, errors: ok, state: ok, persist: ok, note: "now includes jobArn (see CreateClassificationJob note). This pass, full field audit vs DescribeClassificationJobOutput closed the deferred item: added allowListIds/customDataIdentifierIds/managedDataIdentifierIds/managedDataIdentifierSelector/lastRunErrorStatus/statistics/userPausedDetails. lastRunErrorStatus is always {code:NONE} (no error-injection exists in this emulator); statistics is a static {numberOfRuns:1, approximateNumberOfObjectsToProcess:0} (no execution engine simulates real run progress); userPausedDetails is populated (jobPausedAt/jobExpiresAt, 30-day window) only while jobStatus is USER_PAUSED, matching the real conditional-presence contract, and cleared on any other transition."}
  ListClassificationJobs: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "filterCriteria (includes/excludes, EQ/NE comparators on jobType/jobStatus/name/createdAt -- same reduced-scope emulation as ListFindings' criteria matching) and maxResults/nextToken now actually filter and page instead of always returning every job in one page. JobSummary also gained bucketCriteria/bucketDefinitions (extracted from the stored s3JobDefinition), lastRunErrorStatus, and userPausedDetails to match the real JobSummary shape."}
  UpdateClassificationJob: {wire: fixed, errors: ok, state: ok, persist: ok, note: "transitioning jobStatus to USER_PAUSED now populates userPausedDetails; transitioning away from it clears userPausedDetails again (see DescribeClassificationJob note)"}
  CreateMember: {wire: fixed, errors: ok, state: ok, persist: ok, note: "Member had no Arn field (real GetMemberOutput always has 'arn'); added Arn, computed via arn.Build"}
  GetMember: {wire: fixed, errors: ok, state: ok, persist: ok, note: "json tag for MasteredBy was 'masteredBy'; real wire key is 'masterAccountId' -- fixed"}
  DeleteMember: {wire: ok, errors: ok, state: ok, persist: ok}
  ListMembers: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "onlyAssociated/maxResults/nextToken query params now parsed and honored (real default onlyAssociated=true, hiding DISASSOCIATED members unless the client passes onlyAssociated=false); previously the handler ignored the query entirely and always called ListMembers(false), i.e. always showing every member regardless of association status. Now paginated via the same listPaginated/page.NewHMAC helper as ListAllowLists."}
  DisassociateMember: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateMemberSession: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateInvitations: {wire: ok, errors: ok, state: ok, persist: ok}
  AcceptInvitation: {wire: ok, errors: ok, state: ok, persist: ok}
  DeclineInvitations: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteInvitations: {wire: ok, errors: ok, state: ok, persist: ok}
  GetInvitationsCount: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListInvitations: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAdministratorAccount: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateFromAdministratorAccount: {wire: ok, errors: ok, state: ok, persist: ok}
  GetMasterAccount: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateFromMasterAccount: {wire: ok, errors: ok, state: ok, persist: ok}
  EnableOrganizationAdminAccount: {wire: ok, errors: ok, state: ok, persist: ok}
  DisableOrganizationAdminAccount: {wire: fixed, errors: ok, state: ok, persist: ok, note: "query param was read as 'accountId'; real SDK sends 'adminAccountId' -- was always empty, so DisableOrganizationAdminAccount always 404'd for a real client"}
  ListOrganizationAdminAccounts: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeOrganizationConfiguration: {wire: fixed, errors: ok, state: ok, persist: ok, note: "OrgConfig already had maxAccountLimitReached (added in a prior, undocumented change) -- this pass, deleted OrgConfig.DataSources/Features, two INVENTED fields (a 'dataSources' map and a 'features' array) that are NOT part of the real DescribeOrganizationConfigurationOutput/UpdateOrganizationConfigurationInput shapes (verified against both) and were entirely dead code (never written anywhere in the backend). Real output is exactly {autoEnable, maxAccountLimitReached}."}
  UpdateOrganizationConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAutomatedDiscoveryConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAutomatedDiscoveryConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAutomatedDiscoveryAccounts: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchUpdateAutomatedDiscoveryAccounts: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeBuckets: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetBucketStatistics: {wire: fixed, errors: ok, state: ok, persist: n/a, note: "route method was GET with accountId as a query param; real SDK sends POST /datasources/s3/statistics with accountId in the JSON body -- unreachable via real client before fix. accountId itself is still unused by the (intentionally global, single-account) stats aggregation."}
  BatchGetCustomDataIdentifiers: {wire: fixed, errors: ok, state: ok, persist: n/a}
  GetClassificationExportConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  PutClassificationExportConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  GetClassificationScope: {wire: ok, errors: ok, state: ok, persist: ok}
  ListClassificationScopes: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateClassificationScope: {wire: ok, errors: ok, state: ok, persist: ok}
  GetFindingsPublicationConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  PutFindingsPublicationConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  GetResourceProfile: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateResourceProfile: {wire: ok, errors: ok, state: ok, persist: ok}
  ListResourceProfileArtifacts: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListResourceProfileDetections: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateResourceProfileDetections: {wire: ok, errors: ok, state: ok, persist: ok}
  GetRevealConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateRevealConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  GetSensitiveDataOccurrences: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetSensitiveDataOccurrencesAvailability: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetSensitivityInspectionTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  ListSensitivityInspectionTemplates: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateSensitivityInspectionTemplate: {wire: fixed, errors: ok, state: ok, persist: ok, note: "route method was PATCH; real SDK sends PUT /templates/sensitivity-inspections/{id} -- unreachable via real client before fix"}
  GetUsageStatistics: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetUsageTotals: {wire: fixed, errors: ok, state: ok, persist: n/a, note: "query param was read as 'currencyCode' (not a real GetUsageTotalsInput field at all); real key is 'timeRange' -- fixed extraction/naming. Backend still ignores the value and returns static zeroed totals, matching a no-billing emulator; low functional impact."}
  ListManagedDataIdentifiers: {wire: ok, errors: ok, state: ok, persist: n/a}
  SearchResources: {wire: ok, errors: ok, state: ok, persist: n/a}
# Families audited as a group (when per-op is impractical):
families:
  route_matcher: {status: fixed, note: "RouteMatcher path-prefix matching verified against all serializers.SplitURI() calls in the SDK; found 3 method mismatches (UpdateAllowList PATCH->PUT, UpdateSensitivityInspectionTemplate PATCH->PUT, GetBucketStatistics GET->POST) that made those ops unreachable via a real SDK client despite passing unit tests that called h.Handler() directly with the (wrong) method the handler itself expected."}
  tags: {status: fixed, note: "isKnownARN recognized allow-list/custom-data-identifier/findings-filter resource types but NOT classification-job, even though CreateClassificationJob computes/stores a real jobArn -- fixed this pass: isKnownARN now also recognizes classification-job/{id} ARNs, and CreateClassificationJob writes create-time tags into the shared tags map (same pattern as CreateAllowList/CreateCustomDataIdentifier) so ListTagsForResource sees them immediately."}
# gaps/deferred: empty. All 5 gaps and both deferred field audits from the
# 2026-07-12 pass were closed in the 2026-07-23 pass -- see the per-op notes
# above (GetCustomDataIdentifier, CreateClassificationJob/DescribeClassification
# Job/ListClassificationJobs/UpdateClassificationJob, ListMembers,
# DescribeOrganizationConfiguration, GetFindings/CreateSampleFindings) and the
# tags family note. Nothing reclassified to ok without a real field-diff.
gaps: []
deferred: []
leaks: {status: clean, note: "no goroutines/janitors in this service; all state is coarse-lock-guarded maps/tables behind lockmetrics.RWMutex, reset via registry.ResetAll(). Every backend method that took a lock this pass (CreateClassificationJob, DescribeClassificationJob, ListClassificationJobs, UpdateClassificationJob, ListMembers, GetCustomDataIdentifier, BatchGetCustomDataIdentifiers) releases it via defer; no classification-job-runner goroutine/ticker exists (jobs never actually execute in this emulator, so there is nothing to Shutdown-drain)."}
---

## Notes

- macie2 is restjson1. Verified every op's (method, path) pair against
  aws-sdk-go-v2/service/macie2@v1.51.4's `serializers.go` (grepped every
  `httpbinding.SplitURI(...)` + following `request.Method = ...` pair) --
  this is the authoritative source of truth for route matching, not the
  handler's own parseRESTPath tables (which had drifted from it in 3 places,
  see route_matcher family note above).
- Wire wrapper keys: verified handler response envelope keys against every
  `deserializers.go` `awsRestjson1_deserializeOpDocument<Op>Output` `case "key":`
  block for the high-traffic families (session, allow lists, custom data
  identifiers, findings filters, findings, tags, classification jobs,
  members, buckets, usage). Most wrapper keys already matched; the two
  breaking mismatches were BatchGetCustomDataIdentifiers ("items" vs real
  "customDataIdentifiers") and the two Arn-omission bugs (Member, ClassificationJob).
- GetMacieSession/UpdateMacieSession timestamps are ISO8601 strings
  (`__timestampIso8601` in the SDK, parsed via `smithytime.ParseDateTime`),
  NOT epoch-seconds -- this service does not need `pkgs/awstime.Epoch`.
  `time.RFC3339` is wire-compatible; verified, not a bug.
- GetUsageTotals real query param is `timeRange` (values like
  `MONTH_TO_DATE`/`PAST_30_DAYS`); there is no `currencyCode` input field in
  the real API at all -- the emulator's static zeroed UsageTotal response
  (all `"USD"`, `"0"`) already matches real "no usage data" behavior, so this
  was purely a dead/misnamed query-param-extraction bug, not a data bug.
- Member.Arn wire value follows the real "account-root-style" macie2 ARN
  convention: `arn:PARTITION:macie2:REGION:MEMBER_ACCOUNT_ID:` (no resource
  part, trailing colon) via `arn.Build("macie2", region, accountID, "")`.
  Classification job ARNs use the `classification-job/{jobId}` resource
  suffix, matching the allow-list/custom-data-identifier/findings-filter
  convention already used elsewhere in this backend.
- CreateClassificationJobOutput/JobSummary do NOT have a `jobStatus` field on
  create (only DescribeClassificationJobOutput does); the handler's existing
  `"jobStatus": "RUNNING"` in the CreateClassificationJob response is extra
  (client-ignored) data, not wrong per se -- left as-is rather than removing,
  since removing it risks nothing and adding jobArn was the actual bug.

## 2026-07-23 pass notes (closed all 5 gaps + both deferred field audits)

- **Deleted an invented enum value**: `Finding.Category` was hardcoded to
  `"SENSITIVE_DATA"` in `CreateSampleFindings`, which is not a member of the
  real `FindingCategory` enum (`CLASSIFICATION` / `POLICY` are the only two
  values, per `types/enums.go`). Fixed to derive `CLASSIFICATION` vs
  `POLICY` from the finding type's `"Policy:"` prefix, matching how real
  Macie categorizes findings.
- **Deleted invented dead fields**: `OrgConfig.DataSources`
  (`map[string]any`) and `OrgConfig.Features` (`[]map[string]any`) do not
  exist anywhere in `DescribeOrganizationConfigurationOutput` or
  `UpdateOrganizationConfigurationInput`, and were never written by any
  handler -- pure dead invented surface. Deleted; `OrgConfig` is now exactly
  the real two-field shape.
- **Real behavior-changing fix, not just a field addition**:
  `GetCustomDataIdentifier`/`BatchGetCustomDataIdentifiers` on a
  soft-deleted identifier used to 404. Real Macie never hard-deletes a
  custom data identifier (`DeleteCustomDataIdentifier` docs: "Amazon Macie
  doesn't delete it permanently... it soft deletes the identifier"), and the
  real output shape's nullable `Deleted *bool` field only makes sense if Get
  can still succeed post-delete. Fixed: Get/BatchGet now always succeed for
  a real ID (deleted or not) and report `deleted:true`/`false`; only a
  never-existed ID 404s. Two existing tests
  (`GetCustomDataIdentifier on deleted CDI returns 404`,
  `TestCustomDataIdentifierNoDeletedField`) encoded the old, incorrect
  behavior and were rewritten to assert the corrected behavior
  (`TestCustomDataIdentifierDeletedField`).
- `types.Severity.Score` is `*int64` (values 1-3: Low/Medium/High) in the
  real SDK, not an arbitrary float -- `defaultFindingScore` was `5.0`
  (out of range). Fixed `Severity.Score` to `int64` and the default to `2`
  (Medium).
- `ListJobsFilterCriteria`/`ListMembersInput.OnlyAssociated` wire shapes
  verified against `serializers.go`
  (`awsRestjson1_serializeDocumentListJobsFilterCriteria`/`Term`,
  `onlyAssociated` query param) and `deserializers.go`
  (`awsRestjson1_deserializeDocumentJobSummary`) -- `createdAt` on
  `JobSummary` is ISO8601 (`smithytime.ParseDateTime`), same as
  `DescribeClassificationJob`, not epoch-seconds.
- Classification-job runtime fields (`lastRunErrorStatus`, `statistics`,
  `userPausedDetails`) are populated with realistic-but-static values since
  this emulator has no job-execution engine: `lastRunErrorStatus` is always
  `{code: NONE}`, `statistics` is always `{numberOfRuns: 1,
  approximateNumberOfObjectsToProcess: 0}`. This is an intentional,
  documented reduced-scope emulation (jobs never actually run), not a stub --
  every field that CAN be driven by real request/state data (allowListIds,
  customDataIdentifierIds, managedDataIdentifierIds,
  managedDataIdentifierSelector, userPausedDetails' pause/expiry timestamps)
  is.
- `PolicyDetails` (the policy-finding counterpart to `ClassificationDetails`)
  was intentionally left unimplemented: `CreateSampleFindings` now correctly
  categorizes `"Policy:"`-prefixed findings as `POLICY` and gives them a
  bucket-only `resourcesAffected` (no `s3Object`, matching policy findings
  being bucket-level), but does not synthesize a `policyDetails` value. If a
  future pass needs it, `PolicyDetails`/`FindingAction`/`FindingActor` in
  `types/types.go` are the reference shapes.
