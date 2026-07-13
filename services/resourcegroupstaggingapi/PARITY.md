---
service: resourcegroupstaggingapi
sdk_module: aws-sdk-go-v2/service/resourcegroupstaggingapi@v1.31.8
last_audit_commit: 0e933737
last_audit_date: 2026-07-13
overall: A
ops:
  GetResources: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed wrong error type (was ValidationException, which doesn't exist in this service's model); added ResourceARNList mutual-exclusion validation vs TagFilters/ResourceTypeFilters/pagination params; TagsPerPage now actually caps page splits by cumulative tag count instead of being validated-then-discarded. State is cross-service (registered providers), not backend-owned, so no persistence entry applies here."}
  GetTagKeys: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetTagValues: {wire: ok, errors: ok, state: ok, persist: n/a, note: "handler-level Key-required check now returns InvalidParameterException, not ValidationException"}
  TagResources: {wire: ok, errors: ok, state: ok, persist: n/a, note: "delegates to registered ARN taggers; FailedResourcesMap error codes fixed to InvalidParameterException/InternalServiceException (both already correct); validation errors fixed to InvalidParameterException"}
  UntagResources: {wire: ok, errors: ok, state: ok, persist: n/a, note: "same fix as TagResources"}
  GetComplianceSummary: {wire: ok, errors: ok, state: partial, persist: n/a, note: "no tag-policy engine exists anywhere in gopherstack, so NonCompliantResources is always 0 -- an accurate subset of real behavior (no policy attached => zero noncompliant) but not a full implementation; see gap below"}
  ListRequiredTags: {wire: ok, errors: ok, state: ok, persist: n/a, note: "correctly always empty -- no tag-policy engine to derive required tags from"}
  StartReportCreation: {wire: ok, errors: ok, state: ok, persist: ok, note: "removed fabricated S3BucketRegion input field -- not part of the real AWS API at all (real StartReportCreationInput has only S3Bucket)"}
  DescribeReportCreation: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  report_lifecycle: {status: ok, note: "RUNNING -> SUCCEEDED transition, ConcurrentModificationException while RUNNING, per-region isolation via store.Table -- all verified against real semantics and already covered by existing tests"}
  error_codes: {status: ok, note: "core fix this sweep: every validation failure in this package was returning __type: ValidationException, which is not a shape in resourcegroupstaggingapi's error model at all (confirmed against aws-sdk-go-v2/service/resourcegroupstaggingapi/types/errors.go and deserializers.go's error-code switch, which has no case for it and would fall through to a bare smithy.GenericAPIError on a real client). Fixed to InvalidParameterException, matching every one of GetResources/TagResources/UntagResources/StartReportCreation/GetComplianceSummary's actual documented error set. Malformed JSON request bodies now return SerializationException instead, matching the convention already used by services/organizations for the same awsjson1.1 protocol."
gaps:
  - "GetComplianceSummary/ListRequiredTags always report zero noncompliant / zero required tags because no tag-policy engine exists anywhere in gopherstack (bd: gopherstack-i710)"
  - "Cross-service tag wiring (cli.go wireResourceGroupsTagging) only covers dynamodb/sqs/sns/lambda/kms/secretsmanager; ~90 other services with native TagResource support are not registered, so their tags are invisible to GetResources/GetTagKeys/GetTagValues and their ARNs always fail TagResources/UntagResources. Requires editing cli.go (shared file, out of scope for this service-scoped pass) (bd: gopherstack-3xne)"
deferred:
  - "Full TagsPerPage/ResourcesPerPage interaction edge cases beyond the cumulative-tag-count cap added this sweep (e.g. exact AWS behavior when a single oversized resource's tag count alone exceeds TagsPerPage across multiple such resources in a row) -- current fix always keeps at least one resource per page, matching the 'never split a resource across pages' rule, but has not been stress-tested against arbitrarily adversarial tag-count distributions"
leaks: {status: clean, note: "no goroutines; per-region resourceCache and reportStates are plain maps/store.Table guarded by the coarse lockmetrics.RWMutex; Reset() clears dynamic state but intentionally preserves providers/taggers/untaggers (wired once at startup); Restore() always clears them and requires wireResourceGroupsTagging to be called again by the caller after a restore, which is already documented in persistence.go"}
---

## Notes

- **Real resourcegroupstaggingapi has no `ValidationException` error shape.** Its error
  model (`aws-sdk-go-v2/service/resourcegroupstaggingapi/types/errors.go`) is exactly:
  `ConcurrentModificationException`, `ConstraintViolationException`,
  `InternalServiceException`, `InvalidParameterException`,
  `PaginationTokenExpiredException`, `ThrottledException`. Any parameter-validation
  failure in this service is `InvalidParameterException` (400), not `ValidationException`
  -- unlike many other AWS JSON services (DynamoDB, IAM, etc.) that *do* model
  `ValidationException`. This was the core bug found this sweep: every validation error
  in the package used the wrong wire error code. Fixed via a package-level
  `errCodeInvalidParameter` constant plus `ErrValidation`'s message, both now
  `"InvalidParameterException"`.

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
  gopherstack previously accepted and silently applied all filters together. Added
  `validateResourceARNListExclusivity`.

- **`TagsPerPage` was accepted, range-validated (100-500), and then completely discarded**
  -- a disguised no-op parameter that never affected the response. Real AWS uses it to
  additionally split `GetResources` pages by cumulative tag count (a resource with no
  tags counts as one tag) rather than by resource count alone, and never splits a single
  resource and its tags across two pages. Implemented via `capByTagCount` +
  `resolveTagsPerPage`, wired into `paginateResources`.

- **`GetComplianceSummary`/`ListRequiredTags` returning empty output is correct, not a
  stub-in-disguise**, given gopherstack has no tag-policy engine anywhere (no
  Organizations policy-attachment model, no policy-document evaluator). An account with
  no tag policy attached genuinely reports zero noncompliant resources and zero required
  tags on real AWS too, so this is a deliberately-scoped-down but *accurate* subset of
  behavior, not a fabricated response. Building the actual tag-policy engine is tracked
  separately (bd: gopherstack-i710) since it's a cross-cutting feature, not a
  resourcegroupstaggingapi-only fix.

- **Cross-service tag wiring lives in `cli.go` (`wireResourceGroupsTagging`), out of
  scope for this service-scoped pass.** Only 6 of the ~90 services with native
  `TagResource` support (dynamodb, sqs, sns, lambda, kms, secretsmanager) are actually
  registered as providers/ARN taggers. This backend faithfully reports
  `FailedResourcesMap` with `InvalidParameterException` ("no registered tagger handles
  ARN") for every other service's ARNs, which is the *correct* behavior for an
  unregistered tagger -- the gap is in what's registered, not in how this package
  handles an unregistered ARN. Tracked separately (bd: gopherstack-3xne).

- **Report lifecycle (`StartReportCreation`/`DescribeReportCreation`)** was already
  correctly modeled prior to this sweep: RUNNING -> SUCCEEDED after a simulated window,
  `ConcurrentModificationException` while RUNNING, per-region isolation via
  `store.Table[reportCreationState]`, and a `nil` `Status` (not the non-AWS `"NO REPORT"`
  string) when no report has ever been started for a region. No changes needed.

- **Wire shapes for `Tag`, `TagFilter`, `FailureInfo`, `ComplianceDetails`,
  `ResourceTagMapping`, `RequiredTag`** were all checked field-by-field against
  `aws-sdk-go-v2/service/resourcegroupstaggingapi/types` and match (including the
  easy-to-misspell `KeysWithNoncompliantValues`, already correct and pinned by an
  existing test).
