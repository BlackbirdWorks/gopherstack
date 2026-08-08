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
  - "cli.go's wireResourceGroupsTagging covers only ~91 of gopherstack's ~90 services (RegisterProvider/RegisterARNTagger/RegisterARNUntagger for cross-service resource discovery) -- a pre-existing gap, out of this service's scope, tracked separately (bd: gopherstack-3xne)."
  - "ResourceTypeFilters validation uses a regex stricter than the real (unconstrained) schema, and ResourceARN has no enforced 1011-character max -- not investigated or fixed this pass (gopherstack-3xfq's stated scope also named these; ran out of time after the tag-policy engine and the SRP-6a work prioritized elsewhere this session)."
  - "Cross-service tag wiring (cli.go wireResourceGroupsTagging) now covers 55 of the ~90 services with native TagResource support -- see cli.go's wireResourceGroupsTagging doc comment for the exact wired list. Latest sweep added accessanalyzer, dlm, ce, mediapackage, swf, fis, codeconnections, mediastore, mwaa, pipes (10 services, bringing the count from 45 to 55). Every new service fit the existing resourceTypeFromARN/constantResourceType dispatch without needing further generalization: accessanalyzer (analyzer/{name}), dlm (policy/{id}), mediastore (container/{name}), pipes (pipe/{name}), and mwaa (environment/{name}) tag exactly one resource kind each, so each uses a constant resource type; ce (costcategory/anomalymonitor/anomalysubscription), mediapackage (channels/origin_endpoints), fis (safety-lever/experiment-template/experiment), and codeconnections (connection/host/repository-link) mix several kinds in one flat ARN-keyed store, so each uses resourceTypeFromARN to derive the type per-ARN. Two needed a documented exception rather than a code change: swf's domain ARN has a literal leading slash before its resource segment (\"arn:aws:swf:region:account:/domain/{name}\", confirmed against swfARNRegex in services/swf/tags.go), which would make resourceTypeFromARN read an empty type from the leading separator, so it uses a constant resource type instead; mwaa's ARNs use the real AWS \"airflow\" service namespace rather than \"mwaa\" (confirmed against every arn.Build call in services/mwaa), which the wiring passes explicitly rather than the package's own name. Several other candidate services (macie2, managedblockchain, mediaconvert, datasync, codedeploy, inspector2, and more) have native TagResource support with what looks like the same flat-ARN shape this sweep wired, but were not pursued this pass -- each would need its own TaggedResources()-style accessor added to its backend (like the ten added this sweep) before it could be wired, and none of that accessor work was started or verified, so none of it is claimed done. s3control remains blocked as before (its taggable ARNs live under the \"s3\"/\"s3-object-lambda\" service namespaces rather than \"s3control\" itself, so the current arnServiceIs single-namespace dispatch doesn't fit it) (bd: gopherstack-3xne)."
  - "This pass (gopherstack-3xne) wired the six services the prior sweep named but did not pursue: macie2, managedblockchain, mediaconvert, datasync, codedeploy, inspector2 -- bringing the count from 55 to 61. Each needed its own new TaggedResources() accessor (none had one): macie2 (allow-list/custom-data-identifier/findings-filter/classification-job, flat b.tags map), managedblockchain (accessors/members/nodes/networks via arnToResource type-switch on each resource's own Tags field -- invitations carry no Tags field and are never tagged), mediaconvert (jobTemplates/jobs/presets/queues, flat b.tags map), datasync (agent/location/task, flat b.tags map -- task executions build a nested ARN but isKnownResource never recognizes them so they can never be tagged), codedeploy (application/deploymentgroup, tags live on Application.Tags/DeploymentGroup.Tags via pkgs/tags.Tags, not a flat map), inspector2 (filter only in practice -- resourceExists gates tagging to filter ARNs or an ARN already seeded into b.tags, and only CreateFilter does that seeding). All six ARN shapes were confirmed against each service's own arn.Build call sites before wiring; none needed a namespace or nested-segment exception like the SWF/MWAA/DAX/Cognito traps found in earlier passes, except codedeploy's application/deploymentgroup ARNs use a colon (not slash) before the resource segment -- resourceTypeFromARN already handles both separators, so no change was needed there. s3control remains blocked as documented (bd: gopherstack-3xne)."
  - "ResourceTypeFilters format validation (resourceTypeFilterRE, requiring lowercase 'service[:type]' shape) is stricter than the real API's AmazonResourceType schema, which declares pattern [\\s\\S]* (i.e. no server-side pattern constraint beyond max length 256). Predates this sweep; left unchanged because there is no confirmed evidence of real AWS's actual runtime rejection behavior for malformed resource-type filters (docs describe the convention but the schema doesn't enforce it), and changing validation behavior without positive confirmation risks trading one mismatch for another. Flagged for a future sweep with real-AWS or integration-test verification."
  - "This pass (gopherstack-3xne, fourth sweep) wired eight more: RAM, Rekognition, Translate, AppStream, MediaTailor, VPCLattice, CodePipeline, KinesisAnalyticsV2 -- bringing the count from 61 to 69. Each needed its own new TaggedResources() accessor (none had one). Two new ARN-namespace traps found, both confirmed against the service's own arn.Build call sites before wiring: VPC Lattice's real ARN service is \"vpc-lattice\", not \"vpclattice\" (services/vpclattice/store.go's arnService constant); Kinesis Data Analytics v2's ARNs use the real AWS \"kinesisanalytics\" namespace, not \"kinesisanalyticsv2\" (services/kinesisanalyticsv2/tags.go), which it shares with the separate, still-unwired kinesisanalytics (v1) service -- wiring v1 later needs the same registration-order ownership check wireTaggingDocDB/wireTaggingNeptune use for their shared \"rds\" namespace, not a plain registerTaggingService call, or it will shadow v2. RAM's real TagResource only ever tags resource shares (confirmed: TagResource checks only b.resourceShares, never the permission or invitation ARNs also built via arn.Build elsewhere in the package), so it uses a constant resource type rather than resourceTypeFromARN. CodePipeline mixes two ARN shapes in one flat store -- a bare-name pipeline ARN (no separator) and a colon-separated \"webhook:{name}\" ARN -- so a small codepipelineResourceType wrapper turns resourceTypeFromARN's bare-name fallback into an explicit \"codepipeline:pipeline\" instead of leaving it as the ambiguous service-alone string. Rekognition's resourceExists gate (already documented in rekognition/tags.go) is narrower than its arn.Build call sites: bare project ARNs are never accepted, only collection, stream processor, and project *version* ARNs are, matching the real API. AppStream only tags the resource kinds it seeds into its tag store at creation time (stacks, app blocks, fleets, applications, images); directory configs and users build ARNs too but are never seeded, so they can never be tagged. opsworks was examined and explicitly skipped: it has native tagging support and a real resourceExists gate already correctly scoped to stack/layer ARNs (per its own doc comment), but the service itself is never registered in cli.go's getServiceProviders chain at all -- no &opsworksbackend.Provider{} entry anywhere -- so it isn't a running service and wiring its tagging would be a no-op (byName[\"OpsWorks\"] is always nil). fsx was also examined and skipped: every one of its CreateFileCache/CreateBackup/CreateDataRepositoryTask/CreateVolume/CreateFileSystem/CreateSnapshot/CreateStorageVirtualMachine functions takes an unexported *createXInput struct, so it cannot be exercised from cli_test.go via the established direct-backend-call pattern without adding HTTP-layer test scaffolding no other subtest in this table uses. s3control remains blocked as documented (bd: gopherstack-3xne)."
  - "This pass (gopherstack-3xne, fifth sweep) wired 21 more: Comprehend, Shield, Transcribe, VerifiedPermissions, WAF Classic, SecurityHub, AppRunner, Route53Resolver, Timestream Write, S3 Tables, WorkMail, Pinpoint, Application Auto Scaling, CodeArtifact, Clean Rooms, App Mesh, Personalize, SESv2, X-Ray, AWS Config, and EventBridge Scheduler -- bringing the count from 69 to 91 (of the roughly 90 originally estimated; the true total was always an approximation). Each got a new TaggedResources() accessor. New ARN-namespace traps, each confirmed against the service's own arn.Build call sites: Timestream Write uses \"timestream\", not \"timestreamwrite\"; Pinpoint uses \"mobiletargeting\", not \"pinpoint\"; SESv2 shares the real \"ses\" namespace with SES v1 (which builds no ARNs of its own today, so unlike kinesisanalytics v1/v2 there is no registration-order collision to guard against yet); AWS Config uses \"config\", not \"awsconfig\" (hand-built ARN strings, not pkgs/arn); Application Auto Scaling's scalable targets build ARNs under the real \"application-autoscaling\" namespace while its scheduled actions and scaling policies build ARNs under \"autoscaling\" instead (matching real AWS's own historical split) -- but TagResource only ever resolves scalable targets, so only \"application-autoscaling\" is wired. Two new nested-ARN shapes needed the existing nestedResourceType helper (\"parent/id/kind/id\", already handled, no new helper needed): WorkMail nests users/groups/resources one level under their organization, Clean Rooms nests seven sub-resource kinds one level under their membership. App Mesh and S3 Tables needed dedicated derivation closures (appmeshResourceType, s3tablesResourceType) since App Mesh nests to varying depths (mesh -> virtualNode/virtualRouter/etc -> route/gatewayRoute, 2/4/6 segments) and S3 Tables nests a table under a namespace under a bucket (5 segments) -- both deeper than nestedResourceType's fixed 4-segment check. The acceptance-gate-narrower-than-arn.Build trap paid off twice more: Shield's TagResource never resolves protection-group ARNs (only protections), and Application Auto Scaling's TagResource never resolves scheduled-action or scaling-policy ARNs (only scalable targets). EXAMINED AND SKIPPED: forecast's only resource-creation path (b.create) is unexported and reachable solely from its own handler's JSON operation dispatch, not a direct backend method the established cli_test.go pattern can call -- wiring was written, then backed out once this was confirmed, rather than shipped untested. s3control remains blocked as documented (bd: gopherstack-3xne). NOT PURSUED, no code written: acm (tags live behind a Handler-level, not Backend-level, ARN dispatch spanning four distinct resource kinds -- not read deeply enough to confirm the pattern fits), amplify, apigateway (leading-slash \"/restapis/{id}\" ARN with no account segment), apigatewayv2, appsync (TagResource takes an apiID, not a full ARN), databrew, emrserverless (leading-slash ARN), iot (TagResourceGeneric's acceptance-gate breadth was not traced), iotanalytics, kafka, organizations (TagResource takes a bare resourceID, not an ARN, so the generic ARN-keyed dispatch needs an adapter that was not written), ssoadmin (TagResource takes both an instanceArn and a resourceArn, and its ARNs use the \"sso\" namespace), and textract -- see cli.go's wireResourceGroupsTagging doc comment for the current exact wired list."
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
