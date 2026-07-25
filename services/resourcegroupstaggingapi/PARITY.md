---
service: resourcegroupstaggingapi
sdk_module: aws-sdk-go-v2/service/resourcegroupstaggingapi@v1.31.8
last_audit_commit: 0e933737
last_audit_date: 2026-07-24
overall: A
ops:
  GetResources: {wire: ok, errors: ok, state: ok, persist: n/a, note: "field-diffed against aws-sdk-go's api-2.json shapes this sweep: TagFilter.Values cap was 256, real TagValueList shape caps at 20 -- fixed. ResourceARNList had no length cap at all; real ResourceARNListForGet shape caps at 100 (distinct from TagResources/UntagResources' 20-ARN ResourceARNListForTagUntag) -- added. PaginationTokenExpiredException was declared in the error model but never producible; an unmatched PaginationToken now returns it instead of silently restarting from page 1. State is cross-service (registered providers), not backend-owned, so no persistence entry applies here."}
  GetTagKeys: {wire: ok, errors: ok, state: ok, persist: n/a, note: "same PaginationTokenExpiredException fix as GetResources; signature changed to return (*GetTagKeysOutput, error)"}
  GetTagValues: {wire: ok, errors: ok, state: ok, persist: n/a, note: "handler-level Key-required check returns InvalidParameterException; same PaginationTokenExpiredException fix as GetResources; signature changed to return (*GetTagValuesOutput, error)"}
  TagResources: {wire: ok, errors: ok, state: ok, persist: n/a, note: "delegates to registered ARN taggers; FailedResourcesMap error codes InvalidParameterException/InternalServiceException match the model's 2-value ErrorCode enum exactly; ResourceARNList (max 20), Tags (TagMap max 50 entries), TagKey (max 128)/TagValue (max 256) all confirmed against api-2.json shapes"}
  UntagResources: {wire: ok, errors: ok, state: ok, persist: n/a, note: "same fix history as TagResources; TagKeys (TagKeyListForUntag max 50) confirmed against api-2.json"}
  GetComplianceSummary: {wire: ok, errors: ok, state: partial, persist: n/a, note: "no tag-policy engine exists anywhere in gopherstack, so NonCompliantResources is always 0 -- an accurate subset of real behavior (no policy attached => zero noncompliant) but not a full implementation; see gap below. This sweep: MaxResults was silently clamped to a default on out-of-range input instead of validating -- real MaxResultsGetComplianceSummary shape declares min:1/max:1000 explicitly (unlike GetResources' ResourcesPerPage, which has NO declared min/max in the model), so out-of-range now returns InvalidParameterException, matching the same explicit-range-with-error pattern already used for TagsPerPage. Signature changed to return (*GetComplianceSummaryOutput, error)."}
  ListRequiredTags: {wire: ok, errors: ok, state: ok, persist: n/a, note: "correctly always empty -- no tag-policy engine to derive required tags from"}
  StartReportCreation: {wire: ok, errors: ok, state: ok, persist: ok, note: "removed fabricated S3BucketRegion input field -- not part of the real AWS API at all (real StartReportCreationInput has only S3Bucket)"}
  DescribeReportCreation: {wire: ok, errors: ok, state: ok, persist: ok, note: "CORRECTED THIS SWEEP: a prior audit had it backwards -- 'NO REPORT' IS a real, documented AWS status value (aws-sdk-go-v2 DescribeReportCreationOutput.Status doc comment and aws-sdk-go's docs-2.json both list it: 'NO REPORT - No report was generated in the last 90 days'), not a gopherstack invention. Two tests (TestDescribeReportCreation_NoReport, TestDescribeReportCreation_NoReportResponseBody) had pinned the wrong (nil-Status) behavior with comments asserting NO REPORT was 'not a real AWS status value' -- rewritten to assert the correct string. Also added the 90-day staleness reversion (a non-RUNNING report older than 90 days now reports NO REPORT instead of its stale terminal status), matching the same doc line."}
families:
  report_lifecycle: {status: ok, note: "RUNNING -> SUCCEEDED transition, ConcurrentModificationException while RUNNING, per-region isolation via store.Table, NO REPORT for never-started/stale (>90d) reports -- all verified against real semantics (see DescribeReportCreation note above for this sweep's correction)"}
  error_codes: {status: ok, note: "prior sweep's core fix: every validation failure in this package was returning __type: ValidationException, which is not a shape in resourcegroupstaggingapi's error model at all (confirmed against aws-sdk-go-v2/service/resourcegroupstaggingapi/types/errors.go and deserializers.go's error-code switch). Fixed to InvalidParameterException. This sweep added the sixth and final error-model member, PaginationTokenExpiredException, which was declared but never producible by any code path -- see GetResources/GetTagKeys/GetTagValues notes above. All 6 error types in the real model (ConcurrentModificationException, ConstraintViolationException, InternalServiceException, InvalidParameterException, PaginationTokenExpiredException, ThrottledException) are now field-diff-confirmed; ConstraintViolationException/ThrottledException remain structurally unreachable because gopherstack has no tag-policy engine or rate limiter (not a wiring bug, an architectural absence tracked by gopherstack-i710)."}
gaps:
  - "GetComplianceSummary/ListRequiredTags always report zero noncompliant / zero required tags because no tag-policy engine exists anywhere in gopherstack (bd: gopherstack-i710)"
  - "Cross-service tag wiring (cli.go wireResourceGroupsTagging) only covers dynamodb/sqs/sns/lambda/kms/secretsmanager; ~90 other services with native TagResource support are not registered, so their tags are invisible to GetResources/GetTagKeys/GetTagValues and their ARNs always fail TagResources/UntagResources. Requires editing cli.go (shared file, out of scope for this service-scoped pass) (bd: gopherstack-3xne). Re-confirmed unchanged this sweep."
  - "ResourceTypeFilters format validation (resourceTypeFilterRE, requiring lowercase 'service[:type]' shape) is stricter than the real API's AmazonResourceType schema, which declares pattern [\\s\\S]* (i.e. no server-side pattern constraint beyond max length 256). Predates this sweep; left unchanged because there is no confirmed evidence of real AWS's actual runtime rejection behavior for malformed resource-type filters (docs describe the convention but the schema doesn't enforce it), and changing validation behavior without positive confirmation risks trading one mismatch for another. Flagged for a future sweep with real-AWS or integration-test verification."
deferred:
  - "Full TagsPerPage/ResourcesPerPage interaction edge cases beyond the cumulative-tag-count cap (e.g. exact AWS behavior when a single oversized resource's tag count alone exceeds TagsPerPage across multiple such resources in a row) -- current fix always keeps at least one resource per page, matching the 'never split a resource across pages' rule, but has not been stress-tested against arbitrarily adversarial tag-count distributions"
  - "ResourceARN max-length (1011 chars per the real ResourceARN shape) is not validated in validateARNList; only structural parseability (awsarn.Parse) is checked. Low-value edge case, not exercised by any known test."
leaks: {status: clean, note: "no goroutines; per-region resourceCache and reportStates are plain maps/store.Table guarded by the coarse lockmetrics.RWMutex; Reset() clears dynamic state but intentionally preserves providers/taggers/untaggers (wired once at startup); Restore() always clears them and requires wireResourceGroupsTagging to be called again by the caller after a restore, which is already documented in persistence.go. Re-verified this sweep: every new/changed lock path (GetComplianceSummary's added validation runs before the lock is taken; pagination error paths return before any lock-guarded mutation) still defer-releases correctly."}
---

## Notes

- **Real resourcegroupstaggingapi has no `ValidationException` error shape.** Its error
  model (`aws-sdk-go-v2/service/resourcegroupstaggingapi/types/errors.go`) is exactly:
  `ConcurrentModificationException`, `ConstraintViolationException`,
  `InternalServiceException`, `InvalidParameterException`,
  `PaginationTokenExpiredException`, `ThrottledException`. Any parameter-validation
  failure in this service is `InvalidParameterException` (400), not `ValidationException`
  -- unlike many other AWS JSON services (DynamoDB, IAM, etc.) that *do* model
  `ValidationException`.

- **`PaginationTokenExpiredException` was declared in the error model for
  `GetResources`/`GetTagKeys`/`GetTagValues` but never producible by any code path** --
  an unmatched/garbage `PaginationToken` silently restarted pagination from page 1
  instead. Real AWS pagination tokens are opaque and expire after 15 minutes; this
  in-memory backend has no encoded timestamp to check, so any token that fails to
  resolve against the current sorted result set (malformed, stale, or referencing a
  since-removed resource/key/value) is now treated as expired, the closest faithful
  emulation given the architecture. `paginateResources`/`paginateStrings`/`findTokenStart`
  now return `ErrPaginationTokenExpired` instead of silently defaulting to index 0;
  `GetTagKeys`/`GetTagValues`/`GetComplianceSummary`'s `StorageBackend` interface
  signatures changed to return `error` to carry this (GetComplianceSummary's signature
  changed for its own, unrelated MaxResults-validation reason -- see below -- since that
  op's error list does *not* include `PaginationTokenExpiredException`).

- **`TagFilter.Values` cap was 256, matching nothing in the real model.** Field-diffing
  `aws-sdk-go`'s `models/apis/resourcegroupstaggingapi/2017-01-26/api-2.json` shows
  `TagValueList: {max: 20, min: 0}` -- the real cap is 20, exactly matching the doc
  comment text ("each key can include up to 20 values") that the previous 256 constant
  silently ignored. Fixed `maxTagFilterValues` to 20.

- **`GetResources`' `ResourceARNList` had no length cap at all.** The real
  `ResourceARNListForGet` shape declares `{max: 100, min: 1}` -- distinct from
  `TagResources`/`UntagResources`' `ResourceARNListForTagUntag` shape, which caps at 20
  (already correctly enforced via `maxARNsPerTagRequest`). Added
  `maxResourceARNListForGet = 100` and a length check in
  `validateResourceARNListExclusivity`.

- **`GetComplianceSummary`'s `MaxResults` was silently clamped to the default on
  out-of-range input instead of validating.** Unlike `GetResources`' `ResourcesPerPage`
  (whose model shape has *no* declared min/max -- silent clamping there is a defensible
  choice), `MaxResultsGetComplianceSummary` explicitly declares `{min: 1, max: 1000}` in
  the real model, the same kind of explicit range `TagsPerPage` already enforces with an
  error. Added `validateComplianceSummaryMaxResults`, matching that existing pattern;
  `GetComplianceSummary`'s `StorageBackend` signature changed to
  `(*GetComplianceSummaryOutput, error)` to carry it.

- **`DescribeReportCreation`'s "NO REPORT" status: a previous audit had this backwards.**
  The prior `PARITY.md` asserted `"NO REPORT"` was "not a real AWS status value" and
  pinned `nil` `Status` via two tests with comments to that effect. Field-diffing the
  actual `aws-sdk-go-v2` doc comment on `DescribeReportCreationOutput.Status` (and
  `aws-sdk-go`'s `docs-2.json`, the same source botocore/smithy generates that comment
  from) shows the opposite: `"NO REPORT - No report was generated in the last 90 days"`
  is explicitly documented as one of exactly four valid status values (`RUNNING`,
  `SUCCEEDED`, `FAILED`, `NO REPORT`). Fixed `DescribeReportCreation` to return the
  literal `"NO REPORT"` string (not `nil`) both when no report has ever been started for
  a region, and when the most recent non-`RUNNING` report is older than the documented
  90-day window (`reportStaleAfter`). Rewrote the two tests that had pinned the wrong
  behavior, and added `TestDescribeReportCreation_StaleReportBecomesNoReport` for the
  90-day transition.

- **Malformed request bodies use `SerializationException`**, matching
  `services/organizations`' convention for the same awsjson1.1 protocol -- this is a
  protocol-level failure (the body never parses far enough to know which operation-level
  error applies), not a modeled per-operation error, so it is intentionally distinct from
  the `InvalidParameterException` used for well-formed-but-invalid parameters.

- **`StartReportCreationInput.S3BucketRegion` was a fabricated field** with no
  counterpart in the real AWS API (`aws-sdk-go-v2/service/resourcegroupstaggingapi`'s
  `StartReportCreationInput` has only `S3Bucket`). Removed along with its one-off test.

- **`GetResources` ResourceARNList mutual exclusion**: real AWS rejects a request that
  combines `ResourceARNList` with `ResourceTypeFilters`, `TagFilters`, or any of
  `ResourcesPerPage`/`TagsPerPage`/`PaginationToken` (`InvalidParameterException`).
  Enforced via `validateResourceARNListExclusivity`.

- **`TagsPerPage`** is validated (100-500, matching the real API's documented range) and
  actually constrains `GetResources` page splits by cumulative tag count (a resource with
  no tags counts as one tag) rather than by resource count alone, via `capByTagCount` +
  `resolveTagsPerPage`, wired into `paginateResources`.

- **`GetComplianceSummary`/`ListRequiredTags` returning empty output is correct, not a
  stub-in-disguise**, given gopherstack has no tag-policy engine anywhere (no
  Organizations policy-attachment model, no policy-document evaluator). An account with
  no tag policy attached genuinely reports zero noncompliant resources and zero required
  tags on real AWS too. Building the actual tag-policy engine is tracked separately (bd:
  gopherstack-i710) since it's a cross-cutting feature, not a resourcegroupstaggingapi-only
  fix.

- **Cross-service tag wiring lives in `cli.go` (`wireResourceGroupsTagging`), out of
  scope for this service-scoped pass (re-confirmed unchanged this sweep).** Only 6 of the
  ~90 services with native `TagResource` support (dynamodb, sqs, sns, lambda, kms,
  secretsmanager) are actually registered as providers/ARN taggers. This backend
  faithfully reports `FailedResourcesMap` with `InvalidParameterException` ("no
  registered tagger handles ARN") for every other service's ARNs, which is the *correct*
  behavior for an unregistered tagger -- the gap is in what's registered, not in how this
  package handles an unregistered ARN. Tracked separately (bd: gopherstack-3xne).

- **Wire shapes for `Tag`, `TagFilter`, `FailureInfo`, `ComplianceDetails`,
  `ResourceTagMapping`, `RequiredTag`, `Summary`/`ComplianceSummary`** were all checked
  field-by-field against `aws-sdk-go-v2/service/resourcegroupstaggingapi/types` and the
  underlying `aws-sdk-go` `api-2.json`/`docs-2.json` shape models, and match (including
  the easy-to-misspell `KeysWithNoncompliantValues`, and the 2-value `FailureInfo.ErrorCode`
  enum -- `InternalServiceException`/`InvalidParameterException` only).
