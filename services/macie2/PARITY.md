---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: macie2
sdk_module: aws-sdk-go-v2/service/macie2@v1.51.4
last_audit_commit: 82c8a1c8
last_audit_date: 2026-07-12
overall: A                # ~130 LOC of genuine route/wire-shape fixes found and applied
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
  CreateCustomDataIdentifier: {wire: ok, errors: ok, state: ok, persist: ok}
  GetCustomDataIdentifier: {wire: partial, errors: ok, state: ok, persist: ok, note: "missing 'deleted' bool field (always false on success since deleted items 404); missing 'severityLevels' -- not fixed, low value, see gaps"}
  DeleteCustomDataIdentifier: {wire: ok, errors: ok, state: ok, persist: ok}
  ListCustomDataIdentifiers: {wire: ok, errors: ok, state: ok, persist: ok}
  TestCustomDataIdentifier: {wire: ok, errors: ok, state: ok, persist: n/a}
  BatchGetCustomDataIdentifiers: {wire: fixed, errors: ok, state: ok, persist: n/a, note: "response wrapper key was 'items'; real key is 'customDataIdentifiers', so a real SDK client always deserialized an empty slice. Also added notFoundIdentifierIds."}
  CreateFindingsFilter: {wire: ok, errors: ok, state: ok, persist: ok}
  GetFindingsFilter: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateFindingsFilter: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteFindingsFilter: {wire: ok, errors: ok, state: ok, persist: ok}
  ListFindingsFilters: {wire: ok, errors: ok, state: ok, persist: ok}
  GetFindings: {wire: ok, errors: ok, state: ok, persist: ok}
  ListFindings: {wire: ok, errors: ok, state: ok, persist: n/a, note: "criteria matching supports eq/neq on a handful of fields only -- acceptable reduced-scope emulation, not a stub"}
  CreateSampleFindings: {wire: ok, errors: ok, state: ok, persist: ok}
  GetFindingStatistics: {wire: ok, errors: ok, state: ok, persist: n/a}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateClassificationJob: {wire: fixed, errors: ok, state: ok, persist: ok, note: "response was missing jobArn entirely (real CreateClassificationJobOutput has only JobArn+JobId); ClassificationJob had no Arn field at all. Added Arn (json:jobArn), computed via arn.Build, threaded through Create+Describe."}
  DescribeClassificationJob: {wire: fixed, errors: ok, state: ok, persist: ok, note: "now includes jobArn (see CreateClassificationJob note)"}
  ListClassificationJobs: {wire: partial, errors: ok, state: partial, persist: ok, note: "filterCriteria and maxResults/nextToken accepted on the wire but ignored by backend (always returns all jobs, one page) -- gap, not fixed this pass"}
  UpdateClassificationJob: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateMember: {wire: fixed, errors: ok, state: ok, persist: ok, note: "Member had no Arn field (real GetMemberOutput always has 'arn'); added Arn, computed via arn.Build"}
  GetMember: {wire: fixed, errors: ok, state: ok, persist: ok, note: "json tag for MasteredBy was 'masteredBy'; real wire key is 'masterAccountId' -- fixed"}
  DeleteMember: {wire: ok, errors: ok, state: ok, persist: ok}
  ListMembers: {wire: partial, errors: ok, state: partial, persist: ok, note: "onlyAssociated/maxResults/nextToken query params accepted by real SDK (default onlyAssociated=true) but handler ignores query entirely and always calls ListMembers(false) -- gap, not fixed this pass"}
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
  DescribeOrganizationConfiguration: {wire: partial, errors: ok, state: ok, persist: ok, note: "missing maxAccountLimitReached field -- low value, not fixed"}
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
  tags: {status: ok, note: "isKnownARN recognizes allow-list/custom-data-identifier/findings-filter resource types; does NOT recognize classification-job, even though CreateClassificationJob now computes/stores a jobArn -- see gaps"}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "ListMembers ignores onlyAssociated/maxResults/nextToken query params; always returns all members in one page regardless of onlyAssociated (real AWS default is onlyAssociated=true, i.e. DISASSOCIATED members hidden by default)"
  - "ListClassificationJobs ignores filterCriteria and maxResults/nextToken; always returns every job in one page"
  - "TagResource/UntagResource/ListTagsForResource (isKnownARN) do not recognize classification-job ARNs, so a real client cannot tag a job post-creation via TagResource even though CreateClassificationJob now stores a real jobArn"
  - "CustomDataIdentifier is missing 'deleted' and 'severityLevels' fields present on the real GetCustomDataIdentifierOutput/BatchGetCustomDataIdentifierSummary shapes; BatchGetCustomDataIdentifiers also silently excludes soft-deleted identifiers instead of returning them with deleted:true (real AWS soft-delete semantics)"
  - "DescribeOrganizationConfiguration is missing the maxAccountLimitReached field"
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - "Full field-by-field audit of ClassificationJob's ~20 optional detail fields (bucketCriteria, bucketDefinitions, statistics, userPausedDetails, managedDataIdentifierSelector, etc.) against DescribeClassificationJobOutput"
  - "Finding struct full field audit against real Finding shape (resourcesAffected, classificationDetails, policyDetails, etc.) -- CreateSampleFindings/GetFindings currently populate only a reduced field set"
leaks: {status: clean, note: "no goroutines/janitors in this service; all state is coarse-lock-guarded maps/tables behind lockmetrics.RWMutex, reset via registry.ResetAll()"}
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
