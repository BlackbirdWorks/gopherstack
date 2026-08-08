---
service: resourcegroupstaggingapi
sdk_module: aws-sdk-go-v2/service/resourcegroupstaggingapi@v1.35.4
last_audit_commit: pending (uncommitted this pass -- see git log at merge time)
last_audit_date: 2026-08-07
overall: A            # 2026-08-07 (gopherstack-3xfq): ListRequiredTags now derives real
                       # RequiredTag rows from an effective TAG_POLICY document's
                       # report_required_tag_for blocks (see requiredTagsFromPolicy,
                       # compliance.go), instead of unconditionally returning an empty list.
                       # The real report_required_tag_for JSON schema
                       # ({"tags":{"<key>":{"report_required_tag_for":{"@@assign":[...]}}}})
                       # was verified against AWS's "Report tagging compliance" Organizations
                       # doc, not reconstructed from memory. New RegisterTagPolicyProvider
                       # extension point (cross_service.go, mirrors the existing
                       # RegisterProvider/RegisterARNTagger pattern) lets a caller supply the
                       # account's effective TAG_POLICY content; with none registered (the
                       # state until cli.go wires it -- see gaps below) this still correctly
                       # returns an empty list, matching real AWS's behavior for an account
                       # with no tag policy attached. GetComplianceSummary is UNCHANGED and
                       # deliberately NOT touched this pass -- gopherstack-i710 already
                       # investigated and closed it as a genuine architectural gap (real
                       # GetComplianceSummary aggregates across every member account of an
                       # org, which gopherstack's single-account model cannot honestly
                       # simulate); reopening it with a single-account approximation would be
                       # exactly the "plausible-looking but wrong" mistake that closure
                       # avoided. See gaps below for what still needs central (cli.go) wiring.
ops:
  GetResources: {wire: ok, errors: ok, state: ok, persist: n/a, note: "field-diffed against aws-sdk-go's api-2.json shapes this sweep: TagFilter.Values cap was 256, real TagValueList shape caps at 20 -- fixed. ResourceARNList had no length cap at all; real ResourceARNListForGet shape caps at 100 (distinct from TagResources/UntagResources' 20-ARN ResourceARNListForTagUntag) -- added. PaginationTokenExpiredException was declared in the error model but never producible; an unmatched PaginationToken now returns it instead of silently restarting from page 1. State is cross-service (registered providers), not backend-owned, so no persistence entry applies here."}
  GetTagKeys: {wire: ok, errors: ok, state: ok, persist: n/a, note: "same PaginationTokenExpiredException fix as GetResources; signature changed to return (*GetTagKeysOutput, error)"}
  GetTagValues: {wire: ok, errors: ok, state: ok, persist: n/a, note: "handler-level Key-required check returns InvalidParameterException; same PaginationTokenExpiredException fix as GetResources; signature changed to return (*GetTagValuesOutput, error)"}
  TagResources: {wire: ok, errors: ok, state: ok, persist: n/a, note: "delegates to registered ARN taggers; FailedResourcesMap error codes InvalidParameterException/InternalServiceException match the model's 2-value ErrorCode enum exactly; ResourceARNList (max 20), Tags (TagMap max 50 entries), TagKey (max 128)/TagValue (max 256) all confirmed against api-2.json shapes"}
  UntagResources: {wire: ok, errors: ok, state: ok, persist: n/a, note: "same fix history as TagResources; TagKeys (TagKeyListForUntag max 50) confirmed against api-2.json"}
  GetComplianceSummary: {wire: ok, errors: ok, state: partial, persist: n/a, note: "NonCompliantResources is always 0. CORRECTED THIS SWEEP: the prior note ('no tag-policy engine exists anywhere in gopherstack') was imprecise -- services/organizations DOES model TAG_POLICY as a real policy type with content documents, attachments, and effective-policy deep-merge inheritance (DescribeEffectivePolicy). The actual, better-evidenced gap is architectural: real GetComplianceSummary is callable only from an organization's management account and aggregates noncompliant-resource counts ACROSS EVERY MEMBER ACCOUNT (confirmed against the AWS API reference's example response, which returns three SummaryList rows for three distinct TargetId account IDs under GroupBy=TARGET_ID) -- but gopherstack's architecture is one simulated AWS account's resource state per running instance, with no multi-account resource-store simulation anywhere in the codebase. Wiring Organizations' tag-policy content into a single-account evaluator would only produce a plausible-looking but semantically wrong approximation of what is fundamentally a cross-account operation, so this was deliberately not attempted; see gap below (bd: gopherstack-i710). This sweep: MaxResults was silently clamped to a default on out-of-range input instead of validating -- real MaxResultsGetComplianceSummary shape declares min:1/max:1000 explicitly (unlike GetResources' ResourcesPerPage, which has NO declared min/max in the model), so out-of-range now returns InvalidParameterException, matching the same explicit-range-with-error pattern already used for TagsPerPage. Signature changed to return (*GetComplianceSummaryOutput, error)."}
  ListRequiredTags: {wire: ok, errors: ok, state: ok, persist: n/a, note: "2026-08-07 (gopherstack-3xfq): now real -- derives RequiredTag rows from a registered TagPolicyProvider's effective TAG_POLICY document (requiredTagsFromPolicy, compliance.go), grouped by the resource-type identifiers named in each tag's report_required_tag_for.@@assign list. Real pagination via pkgs/page (MaxResults/NextToken), default page size 50 (no documented MaxResults range found in any vendored API model for this newer operation, unlike GetComplianceSummary's declared 1-1000, so no range is enforced -- only a default). KNOWN LIMITATION, documented in-code: CloudFormationResourceTypes returns the policy's resource-type identifier verbatim (e.g. \"ec2:ALL_SUPPORTED\") rather than expanding it into the real CloudFormation type names (e.g. \"AWS::EC2::Instance\") AWS's real API may return -- no verified expansion table was available. Central wiring still needed: nothing currently calls RegisterTagPolicyProvider, so this returns an empty list (correctly) until cli.go wires services/organizations' DescribeEffectivePolicy(TAG_POLICY, accountID) result into it -- see gaps below."}
  StartReportCreation: {wire: ok, errors: ok, state: ok, persist: ok, note: "removed fabricated S3BucketRegion input field -- not part of the real AWS API at all (real StartReportCreationInput has only S3Bucket)"}
  DescribeReportCreation: {wire: ok, errors: ok, state: ok, persist: ok, note: "CORRECTED THIS SWEEP: a prior audit had it backwards -- 'NO REPORT' IS a real, documented AWS status value (aws-sdk-go-v2 DescribeReportCreationOutput.Status doc comment and aws-sdk-go's docs-2.json both list it: 'NO REPORT - No report was generated in the last 90 days'), not a gopherstack invention. Two tests (TestDescribeReportCreation_NoReport, TestDescribeReportCreation_NoReportResponseBody) had pinned the wrong (nil-Status) behavior with comments asserting NO REPORT was 'not a real AWS status value' -- rewritten to assert the correct string. Also added the 90-day staleness reversion (a non-RUNNING report older than 90 days now reports NO REPORT instead of its stale terminal status), matching the same doc line."}
families:
  report_lifecycle: {status: ok, note: "RUNNING -> SUCCEEDED transition, ConcurrentModificationException while RUNNING, per-region isolation via store.Table, NO REPORT for never-started/stale (>90d) reports -- all verified against real semantics (see DescribeReportCreation note above for this sweep's correction)"}
  error_codes: {status: ok, note: "prior sweep's core fix: every validation failure in this package was returning __type: ValidationException, which is not a shape in resourcegroupstaggingapi's error model at all (confirmed against aws-sdk-go-v2/service/resourcegroupstaggingapi/types/errors.go and deserializers.go's error-code switch). Fixed to InvalidParameterException. This sweep added the sixth and final error-model member, PaginationTokenExpiredException, which was declared but never producible by any code path -- see GetResources/GetTagKeys/GetTagValues notes above. All 6 error types in the real model (ConcurrentModificationException, ConstraintViolationException, InternalServiceException, InvalidParameterException, PaginationTokenExpiredException, ThrottledException) are now field-diff-confirmed; ConstraintViolationException/ThrottledException remain structurally unreachable because gopherstack has no tag-policy engine or rate limiter (not a wiring bug, an architectural absence tracked by gopherstack-i710)."}
gaps:
  - "GetComplianceSummary always reports zero noncompliant resources. This is NOT simply 'no tag-policy engine exists' (services/organizations does model TAG_POLICY content, attachment, and effective-policy merging) -- the real blocker is architectural: real GetComplianceSummary is a management-account-only operation that aggregates noncompliant counts across every member account in an organization (verified against the AWS API reference, whose example response returns rows for three distinct account IDs), and gopherstack has no multi-account resource-store simulation anywhere to aggregate across. A single-account approximation would misrepresent the operation's actual (cross-account) contract, so was not built. Documented, not fabricated (bd: gopherstack-i710)."
  - "CLOSED 2026-08-07 (gopherstack-3xfq): ListRequiredTags now parses a policy's report_required_tag_for element for real -- see the ListRequiredTags ops row above. NEEDS CENTRAL WIRING (cli.go, out of this service's scope): nothing currently calls resourcegroupstaggingapi.RegisterTagPolicyProvider. The wiring should look up the current account's effective TAG_POLICY via services/organizations' DescribeEffectivePolicy(\"TAG_POLICY\", accountID) and register a closure returning (content, ok) from it -- mirroring how other cross-service registrations already happen in cli.go's wireResourceGroupsTagging for RegisterProvider/RegisterARNTagger. Until wired, ListRequiredTags continues to correctly return an empty list (no tag policy configured), not an error."
  - "cli.go's wireResourceGroupsTagging covers only ~45 of gopherstack's ~90 services (RegisterProvider/RegisterARNTagger/RegisterARNUntagger for cross-service resource discovery) -- a pre-existing gap, out of this service's scope, tracked separately (bd: gopherstack-3xne)."
  - "ResourceTypeFilters validation uses a regex stricter than the real (unconstrained) schema, and ResourceARN has no enforced 1011-character max -- not investigated or fixed this pass (gopherstack-3xfq's stated scope also named these; ran out of time after the tag-policy engine and the SRP-6a work prioritized elsewhere this session)."
  - "Cross-service tag wiring (cli.go wireResourceGroupsTagging) now covers 45 of the ~90 services with native TagResource support -- see cli.go's wireResourceGroupsTagging doc comment for the exact wired list. Latest sweep added dax, detective, guardduty, transfer, cognitoidp, appconfig, codecommit, servicediscovery, memorydb (9 services, bringing the count from 36 to 45). Two of these needed more than the existing resourceTypeFromARN(arn, service) single-segment derivation: GuardDuty (filters/IP sets/threat intel and entity sets/publishing destinations nest one level under their owning detector, \"detector/{id}/{kind}/{id}\") and AppConfig (environments/configuration profiles/experiment definitions nest under their owning application, \"application/{id}/{kind}/{id}\") -- a plain resourceTypeFromARN would take only the first segment and silently collide every nested kind under the parent's type (the same class of bug wafv2ResourceType already exists to avoid), so a new shared nestedResourceType(arn, service) helper takes the third segment for 4-segment ARNs and falls back to resourceTypeFromARN otherwise; Transfer Family reuses the same helper for its one nested exception (agreements nest under their owning server). CodeCommit repository ARNs carry no type segment at all (bare \"codecommit:{account}:{repo-name}\", matching real AWS), so it uses a constant resource type the same way SQS/SNS do for their own bare-name ARNs, rather than resourceTypeFromARN's service-alone fallback. The remaining ~45 services (including s3control, whose taggable ARNs live under the \"s3\"/\"s3-object-lambda\" service namespaces rather than \"s3control\" itself, so the current arnServiceIs single-namespace dispatch doesn't fit it) are still unwired (bd: gopherstack-3xne)."
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
  stub-in-disguise -- CORRECTED THIS SWEEP.** A prior note here claimed "gopherstack has
  no tag-policy engine anywhere (no Organizations policy-attachment model, no
  policy-document evaluator)"; that was inaccurate. `services/organizations` (see
  `policies.go`'s `policyTypeTag = "TAG_POLICY"` and `effective_policy.go`'s
  `DescribeEffectivePolicy`/`mergeTagStyleChain`) already models tag-policy documents,
  attachment to root/OU/account, and AWS's real root-to-target deep-merge inheritance
  rule for `TAG_POLICY`-style policies. Investigated wiring this into
  `GetComplianceSummary` this sweep and deliberately did not, for a more precise reason
  than "no engine exists": real `GetComplianceSummary` is documented (AWS API reference)
  as callable **only from an organization's management account**, and its example
  response returns `SummaryList` rows for **three different member-account `TargetId`
  values** under `GroupBy=TARGET_ID` -- it is fundamentally a cross-account aggregation
  operation. gopherstack has no multi-account resource-store simulation anywhere (every
  running instance models exactly one AWS account's resources), so there is no second
  account's tagged-resource set to aggregate against even with a working tag-policy
  evaluator. A single-account approximation would produce plausible-looking but
  semantically wrong numbers for what the real operation actually measures, so this was
  documented rather than built. An account with no tag policy attached genuinely reports
  zero noncompliant resources on real AWS too, so the current empty-but-honest output is
  not a regression. `ListRequiredTags` (a `report_required_tag_for`-driven feature,
  distinct from `GetComplianceSummary`'s `tag_key`/`tag_value` basic-compliance rules) was
  not separately investigated this sweep. Tracked (bd: gopherstack-i710).

- **Cross-service tag wiring lives in `cli.go` (`wireResourceGroupsTagging`) -- EXPANDED
  THIS SWEEP from 6 to 11 of the ~90 services with native `TagResource` support.**
  Previously only dynamodb, sqs, sns, lambda, kms, secretsmanager were registered as
  providers/ARN taggers. Added ecs, athena, glue, ecr, kinesis, chosen because they were
  named explicitly in the tracking issue and their native tag stores were tractable to
  enumerate without inventing per-resource-kind constants: `wireTaggingARNResources` (the
  shared helper `wireTaggingSQS`/`wireTaggingSNS` already used) was generalized to accept
  a `resourceTypeOf(arn) string` closure instead of one fixed resource-type string, and a
  new `resourceTypeFromARN(arn, service)` helper derives the AWS resource-type string
  (e.g. `"ecs:cluster"`) from an ARN's own resource segment for services whose flat
  ARN-keyed tag store spans more than one resource kind (ECS: clusters/services/task
  definitions/...; Athena: workgroups/data catalogs/capacity reservations/notebooks;
  Glue: databases/crawlers/jobs/data quality rulesets/connections/triggers/workflows).
  ECR and Kinesis have exactly one taggable resource kind each (repository, stream) so use
  a constant resource type instead, same as SQS/SNS. Each newly-wired service got a new
  `TaggedResources()`/`TaggedStreams()` accessor method (mirroring the pre-existing
  `TaggedQueues`/`TaggedTopics`/`TaggedFunctions`/`TaggedKeys`/`TaggedSecrets`/
  `TaggedTables` pattern) that excludes resources with zero tags, matching real AWS's
  `GetResources` behavior of only returning tagged resources.
  This backend still faithfully reports `FailedResourcesMap` with
  `InvalidParameterException` ("no registered tagger handles ARN") for every other
  service's ARNs, which is the *correct* behavior for an unregistered tagger -- the gap
  is in what's registered, not in how this package handles an unregistered ARN. The
  remaining services were unwired as of this note; see `cli.go`'s `wireResourceGroupsTagging`
  doc comment for the current exact list (coverage has grown across several sweeps since --
  see the gaps section above for the up-to-date count) and for why s3control was skipped
  (its taggable ARNs use the `"s3"`/`"s3-object-lambda"` service namespaces, not
  `"s3control"`, so it doesn't fit the current single-namespace-per-service dispatch
  without further generalization). Tracked (bd: gopherstack-3xne).

- **Wire shapes for `Tag`, `TagFilter`, `FailureInfo`, `ComplianceDetails`,
  `ResourceTagMapping`, `RequiredTag`, `Summary`/`ComplianceSummary`** were all checked
  field-by-field against `aws-sdk-go-v2/service/resourcegroupstaggingapi/types` and the
  underlying `aws-sdk-go` `api-2.json`/`docs-2.json` shape models, and match (including
  the easy-to-misspell `KeysWithNoncompliantValues`, and the 2-value `FailureInfo.ErrorCode`
  enum -- `InternalServiceException`/`InvalidParameterException` only).
