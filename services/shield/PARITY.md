---
service: shield
sdk_module: aws-sdk-go-v2/service/shield@v1.34.20
last_audit_commit: 2d47b51d4
last_audit_date: 2026-07-29
overall: A            # all documented gaps from the prior sweep closed; one invented op deleted
ops:
  CreateSubscription: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteSubscription: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateSubscription: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeSubscription: {wire: ok, errors: ok, state: ok, persist: ok}
  GetSubscriptionState: {wire: ok, errors: ok, state: ok, persist: n/a}
  CreateProtection: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this sweep -- now enforces subscriptionMaxProtections=1000 and subscriptionMaxProtectionsPerType=100 via checkProtectionQuotas, returning LimitsExceededException (ErrLimitExceeded) when exceeded, matching the limits CreateProtection itself reports via DescribeSubscription"}
  DescribeProtection: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteProtection: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this sweep -- now cascade-deletes the protection's alarConfigs row (was an orphaned-row leak; ApplicationLayerAutomaticResponseConfiguration is a field of the real Protection object, not an independent resource)"}
  ListProtections: {wire: ok, errors: ok, state: ok, persist: ok, note: "InclusionFilters + offset pagination verified against InclusionProtectionFilters"}
  AssociateHealthCheck: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateHealthCheck: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "resolves both Shield protection ARN and resource ARN; resolveShieldProtectionARN partition prefix fixed this sweep, see below"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateDRTLogBucket: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this sweep -- now requires AssociateDRTRole to have been called first (NoAssociatedRoleException/ErrNoAssociatedRole) and enforces the documented 10-bucket cap (LimitsExceededException/ErrLimitExceeded), matching real AWS SRT-authorization prerequisite behavior"}
  DisassociateDRTLogBucket: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateDRTRole: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateDRTRole: {wire: ok, errors: ok, state: ok, persist: ok, note: "idempotent per AWS"}
  DescribeDRTAccess: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this sweep -- RoleArn key is now omitted from the response when unset instead of emitting an empty string, matching the real *string (nil-omitted) field"}
  AssociateProactiveEngagementDetails: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateEmergencyContactSettings: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeEmergencyContactSettings: {wire: ok, errors: ok, state: ok, persist: ok}
  EnableProactiveEngagement: {wire: ok, errors: ok, state: ok, persist: ok}
  DisableProactiveEngagement: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateProtectionGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this sweep -- now enforces subscriptionMaxProtectionGroups=100 and the ARBITRARY-pattern subscriptionMaxMembersPerGroup=10000 quota, returning LimitsExceededException (ErrLimitExceeded) when exceeded"}
  DescribeProtectionGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  ListProtectionGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateProtectionGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this sweep -- now enforces the same ARBITRARY-pattern subscriptionMaxMembersPerGroup=10000 quota as CreateProtectionGroup"}
  DeleteProtectionGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAttacks: {wire: ok, errors: ok, state: ok, persist: ok, note: "TimeRange {FromInclusive/ToExclusive} shape verified against types.TimeRange"}
  DescribeAttack: {wire: ok, errors: ok, state: ok, persist: ok, note: "AttackProperties/SubResources (optional AWS fields) never populated -- acceptable, see gaps"}
  DescribeAttackStatistics: {wire: ok, errors: ok, state: ok, persist: n/a, note: "top-level TimeRange/DataItems, no wrapper -- matches DescribeAttackStatisticsOutput"}
  EnableApplicationLayerAutomaticResponse: {wire: ok, errors: ok, state: ok, persist: ok}
  DisableApplicationLayerAutomaticResponse: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateApplicationLayerAutomaticResponse: {wire: ok, errors: ok, state: ok, persist: ok}
  ListResourcesInProtectionGroup: {wire: ok, errors: ok, state: ok, persist: n/a, note: "derives ALL/BY_RESOURCE_TYPE membership live from the protections table"}
deleted_invented_ops:
  - GetAttackVectorDefinitionVersion: not present anywhere in aws-sdk-go-v2/service/shield@v1.34.20
    (no api_op_GetAttackVectorDefinitionVersion.go, no client method, no mention in types.go/
    enums.go). This was a gopherstack-invented operation with a fabricated
    AttackVectorDefinitionVersion response field. Deleted this sweep: removed from
    GetSupportedOperations, its dispatch case, its handler func + constant in
    handler_attacks.go, and its two dedicated tests in handler_attacks_test.go. TestHandler_OpsLen
    updated 37 -> 36. sdk_completeness_test.go (sdkcheck.CheckCompleteness) does not flag extra
    ops, only missing ones, so this was silently accepted by the completeness gate before deletion.
families:
  protection: {status: ok, note: "CreateProtection/DescribeProtection/DeleteProtection/ListProtections verified against types.Protection -- no CreationTime/Tags fields on the real shape; gopherstack emits an extra CreationTime, harmless (unknown-field-tolerant awsjson1.1 deserializer)"}
  protectionGroup: {status: ok, note: "same extra-CreationTime note as protection; ProtectionGroup has no CreationTime in real SDK either"}
  subscription: {status: ok, note: "TimeCommitmentInSeconds bug (prior sweep) still holds; Limits/SubscriptionLimits nested shapes verified field-by-field against types.SubscriptionLimits/ProtectionLimits/ProtectionGroupLimits -- gopherstack's extra 'MaxProtections' key on ProtectionLimits is fabricated (not a real field) but harmless since it's additive"}
  attack: {status: ok, note: "ListAttacks/DescribeAttack/DescribeAttackStatistics verified against types.AttackSummary/AttackDetail/AttackStatisticsDataItem/AttackVolume"}
  tags: {status: ok, note: "resolveTaggableProtection accepts both Shield protection ARN and resource ARN, matching TagResourceInput.ResourceARN semantics; partition-prefix bug fixed this sweep"}
  drtAccessAndEngagement: {status: ok, note: "AssociateDRTRole/LogBucket (now enforces role-before-bucket prerequisite + 10-bucket cap), proactive engagement state machine (DISABLED->PENDING->ENABLED) verified"}
  alar: {status: ok, note: "ApplicationLayerAutomaticResponseConfiguration nested in Protection response only when set, matching optional-field AWS behavior; cascade-delete-on-DeleteProtection fixed this sweep"}
  quotas: {status: ok, note: "fixed this sweep -- CreateProtection/CreateProtectionGroup/UpdateProtectionGroup/AssociateDRTLogBucket now enforce every quota they themselves report via DescribeSubscription or that real AWS documents (subscriptionMaxProtections, subscriptionMaxProtectionsPerType, subscriptionMaxProtectionGroups, subscriptionMaxMembersPerGroup, 10-bucket DRT log bucket cap), returning LimitsExceededException (new ErrLimitExceeded sentinel) via handler.go's classifyShieldError"}
gaps:
  - "IMPOSSIBLE (re-confirmed gopherstack-kp7b): DescribeAttack/ListAttacks never populate AttackDetail.AttackProperties or AttackDetail.SubResources (both optional AWS fields); simulated/internal attacks only carry AttackVectors/AttackCounters/Mitigations. This is NOT a chaos-coverable gap (chaos only injects error responses, not fabricated success-payload data) and was re-examined against types.AttackProperty/types.Contributor/types.SubResourceSummary in the vendored SDK this pass: AttackProperty.TopContributors is a list of Contributor{Name, Value int64} -- e.g. a source-country name with a traffic-volume count -- and SubResourceSummary.Counters is a list of SummarizedCounter (Average/Max/Median/Sum/N, real statistical aggregates). gopherstack has no real network traffic for a simulated attack to report on, so populating either field would mean inventing plausible-looking contributor names and traffic counts with zero grounding -- exactly the 'invented metrics/counts' this project's honesty rules forbid, not a smaller version of a real feature. Left honestly absent (the real field is optional and simply omitted when Shield has nothing to report, which is what a synthetic attack's true state is). DescribeAttack/ListAttacks remain fully AWS-shape-correct for every field they DO populate."
  - "IMPOSSIBLE (re-confirmed gopherstack-kp7b): LockedSubscriptionException (subscription's first-year AutoRenew lock, changeable only in the last 30 days of the commitment) is not modeled -- UpdateSubscription always allows changing AutoRenew. Deliberately NOT implemented: gopherstack subscriptions are always \"fresh\" (no historical passage of time), so enforcing the real 335-day lock would make UpdateSubscription permanently fail for every subscription in the emulator, which is worse for testability than the current permissive behavior. Documented gap, not a wire bug. (Not chaos-relevant either way: a caller that specifically wants to exercise this __type can already do so via chaos fault injection on UpdateSubscription, same as the three items below.)"
deferred:
  - "ALREADY COVERED BY CHAOS (verified gopherstack-kp7b): OptimisticLockException (concurrent-modification detection via a resource version/etag) is not modeled anywhere -- CreateProtectionGroup/UpdateProtectionGroup/DeleteProtectionGroup/AssociateDRTLogBucket/DisassociateDRTLogBucket/UpdateEmergencyContactSettings all declare it in their real error catalogs but gopherstack's coarse per-backend lock (lockmetrics.RWMutex) makes every mutation atomic, so the race window OptimisticLockException exists to protect against never occurs in this emulator's backend state. Concretely verified this pass: shield.Handler implements ChaosServiceName() -> \"shield\" and ChaosOperations() -> h.GetSupportedOperations() (handler.go), and pkgs/chaos.Middleware is wired globally via registry.Use(chaos.Middleware(faultStore)) in cli.go -- it matches purely on the request's SigV4 service name + X-Amz-Target operation + region and injects an arbitrary caller-specified FaultError{Code, StatusCode} without touching backend state, so a fault rule such as {\"service\":\"shield\",\"operation\":\"UpdateProtectionGroup\",\"error\":{\"code\":\"OptimisticLockException\",\"statusCode\":400}} deterministically returns that exact typed error to a real client with zero backend code changes."
  - "ALREADY COVERED BY CHAOS (verified gopherstack-kp7b): AccessDeniedException / AccessDeniedForDependencyException are never returned -- gopherstack does not model IAM permission checks for any service, Shield included, so there is no backend-state condition to trigger either from. Consistent with the rest of the codebase; not a Shield-specific gap. Same chaos mechanism as OptimisticLockException above makes both reachable on demand for a caller that wants to test its own error-handling path, with zero backend code changes."
  - "ALREADY COVERED BY CHAOS (verified gopherstack-kp7b): InvalidResourceException (thrown by real AWS when a ResourceArn is a well-formed ARN for a supported type but the underlying resource doesn't exist / isn't accessible) is not distinguished from InvalidParameterException (used for malformed/unsupported-type ARNs) because gopherstack has no cross-service resource-existence oracle to check against. Would require wiring Shield's CreateProtection to query other services' backends (elbv2/cloudfront/route53/ec2/globalaccelerator) for resource existence -- that kind of cross-service backend reference is set up at CLI init time (cli.go), out of bounds for this pass (see applicationautoscaling's PARITY.md for the same cli.go-wiring constraint on a different service). Same chaos mechanism as above makes InvalidResourceException reachable on demand in the meantime."
leaks: {status: clean, note: "no goroutines/janitors in this service; all state lives in store.Table-backed maps guarded by lockmetrics.RWMutex; Snapshot/Restore round-trip verified (persistence_test.go). Fixed this sweep: DeleteProtection previously left an orphaned alarConfigs row keyed by the deleted protection's ResourceARN -- a later CreateProtection for the same ResourceARN would incorrectly inherit the stale ALAR config from the deleted protection. Now cascade-cleaned; regression test in protections_test.go (TestInMemoryBackend_DeleteProtectionCascadeCleansALARConfig)."}
---

## Notes

- Protocol: awsjson1.1, single POST endpoint, `X-Amz-Target: AWSShield_20160616.<Op>`.
  Route matcher checked against the real SDK's `awsAwsjson11_serializeOp*` files -- target
  prefix is exactly `AWSShield_20160616.` (verified via `serializers.go`
  `resolveAuthSchemeOptions`/build-target constant strings across every `api_op_*.go`).
- Real `aws-sdk-go-v2` JSON deserializers silently ignore unrecognized response keys
  (`default: _, _ = key, value` in every `awsAwsjson11_deserializeDocument*` case). This
  means gopherstack emitting *extra* fields the real API doesn't have (e.g. `CreationTime`
  on `Protection`/`ProtectionGroup`, `MaxProtections` on `ProtectionLimits`) is harmless --
  do not flag these as bugs on a future pass. Only *missing* or *misnamed* fields the SDK
  actively reads are real bugs (the `TimeCommitmentInSeconds` bug fixed a prior sweep was the
  latter kind).
- Real `types.Subscription.TimeCommitmentInSeconds` is `int64` seconds. gopherstack's
  internal `Subscription.TimeCommitmentInDays` field/JSON tag intentionally kept as *days*
  (readable business value, 365) -- the seconds conversion happens only at serialization
  time in `handler.go` (`secondsPerDay` constant). No persistence/snapshot format change
  was needed since the internal DTO shape didn't change, only the wire response.
  `subscriptionCommitmentDays` in backend.go remains the source of truth in days.
  If this internal field is ever renamed, remember `shieldSnapshotVersion` must be bumped
  per the doc comment in persistence.go.
- `floatSeconds()` in handler.go duplicates `pkgs/awstime.Epoch()` byte-for-byte (both
  compute `float64(t.UnixNano()) / 1e9` with a nanosecond-precision divide, though
  `awstime.Epoch` additionally special-cases the zero `time.Time` to return exactly `0`).
  Not fixed this sweep (out of the "real bug" budget -- purely a reuse/style item, not a
  parity bug since Shield's own timestamps are never zero-valued in practice), but a future
  pass should switch handler.go to `awstime.Epoch` and delete `floatSeconds`/`nanosPerSecond`.
- Error mapping (`classifyShieldError` in handler.go, called from `handleError`) is now a
  data-driven ordered rule table (`shieldErrorRules`) instead of a hardcoded switch, so new
  sentinel -> wire-code mappings can be added without growing handleError's own complexity.
  Fixed this sweep: (1) `ErrSubscriptionRequired` wraps `awserr.ErrConflict` internally (for
  backward-compatible `errors.Is` matching) but was being serialized as
  `ResourceAlreadyExistsException` instead of the real `InvalidOperationException` -- it is
  now listed ahead of the generic `awserr.ErrConflict` rule so the specific mapping wins;
  (2) `errInvalidPaginationToken` fell through to the `default` case and was serialized as a
  500 `InternalErrorException` instead of the real 400 `InvalidPaginationTokenException` --
  this was compounded by a second bug in all three list handlers (handleListProtections,
  handleListProtectionGroups, handleListAttacks in handler_protections.go/
  handler_protection_groups.go/handler_attacks.go), which re-wrapped `decodeOffsetToken`'s
  error as `fmt.Errorf("%w: %s", errInvalidRequest, err.Error())` -- using `%s` (a plain
  string) for the original error discarded the `errInvalidPaginationToken` sentinel from the
  chain entirely, so even after fixing handleError's rule table the specific __type could
  never surface. Fixed by wrapping with `%w` instead (`fmt.Errorf("invalid NextToken: %w",
  err)`), which properly chains errInvalidPaginationToken so errors.Is finds it;
  (3) added `ErrLimitExceeded` -> `LimitsExceededException` and `ErrNoAssociatedRole` ->
  `NoAssociatedRoleException`, both newly raised by the quota/DRT-prerequisite fixes above.
  Regression tests: errors_test.go (TestHandler_ErrorWireType_*).
  All of gopherstack's error responses now use only real Shield `__type` values:
  `ResourceNotFoundException`, `ResourceAlreadyExistsException`, `InvalidParameterException`,
  `InvalidOperationException`, `InvalidPaginationTokenException`, `LimitsExceededException`,
  `NoAssociatedRoleException`, `InternalErrorException`.
- `protectionARN`/`protectionGroupARN` (protections.go/protection_groups.go) previously called
  `arn.Build("shield", "", accountID, resource)` with a hardcoded empty region string, which
  made `arn.PartitionForRegion("")` always resolve to `"aws"` regardless of the backend's actual
  configured region -- so a GovCloud/China/ISO backend would still mint `arn:aws:shield::...`
  protection ARNs instead of `arn:aws-us-gov:shield::...` etc. `arn.Build`'s only
  region-omitted-but-partition-correct special case is `service=="iam"`; rather than adding a
  second special case to the shared `pkgs/arn` package (which every other service also
  depends on), both functions now build the ARN string directly with
  `arn.PartitionForRegion(region)`. `resolveShieldProtectionARN` (tags.go) was updated in
  lockstep -- it now derives the expected `arn:{partition}:shield::` prefix from `b.region`
  instead of hardcoding `arn:aws:shield::`. Regression test:
  `TestInMemoryBackend_TagResourceByShieldARNGovCloudPartition` (tags_test.go).
