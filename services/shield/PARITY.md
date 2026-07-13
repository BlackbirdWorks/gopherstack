---
service: shield
sdk_module: aws-sdk-go-v2/service/shield@v1.34.20
last_audit_commit: 8a90ed96
last_audit_date: 2026-07-13
overall: B            # already-accurate; one genuine wire bug found and fixed op-by-op
ops:
  CreateSubscription: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteSubscription: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateSubscription: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeSubscription: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed this sweep -- was emitting fabricated TimeCommitmentInDays key/unit; real wire field is TimeCommitmentInSeconds (seconds). See handler.go handleDescribeSubscription."}
  GetSubscriptionState: {wire: ok, errors: ok, state: ok, persist: n/a}
  CreateProtection: {wire: ok, errors: ok, state: ok, persist: ok, note: "does not enforce subscriptionMaxProtections/PerType caps -- see gaps"}
  DescribeProtection: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteProtection: {wire: ok, errors: ok, state: ok, persist: ok}
  ListProtections: {wire: ok, errors: ok, state: ok, persist: ok, note: "InclusionFilters + offset pagination verified against InclusionProtectionFilters"}
  AssociateHealthCheck: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateHealthCheck: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "resolves both Shield protection ARN and resource ARN"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateDRTLogBucket: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateDRTLogBucket: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateDRTRole: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateDRTRole: {wire: ok, errors: ok, state: ok, persist: ok, note: "idempotent per AWS"}
  DescribeDRTAccess: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateProactiveEngagementDetails: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateEmergencyContactSettings: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeEmergencyContactSettings: {wire: ok, errors: ok, state: ok, persist: ok}
  EnableProactiveEngagement: {wire: ok, errors: ok, state: ok, persist: ok}
  DisableProactiveEngagement: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateProtectionGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "does not enforce subscriptionMaxProtectionGroups/MaxMembers caps -- see gaps"}
  DescribeProtectionGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  ListProtectionGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateProtectionGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteProtectionGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAttacks: {wire: ok, errors: ok, state: ok, persist: ok, note: "TimeRange {FromInclusive/ToExclusive} shape verified against types.TimeRange"}
  DescribeAttack: {wire: ok, errors: ok, state: ok, persist: ok, note: "AttackProperties/SubResources (optional AWS fields) never populated -- acceptable, see gaps"}
  DescribeAttackStatistics: {wire: ok, errors: ok, state: ok, persist: n/a, note: "top-level TimeRange/DataItems, no wrapper -- matches DescribeAttackStatisticsOutput"}
  GetAttackVectorDefinitionVersion: {wire: ok, errors: ok, state: ok, persist: n/a}
  EnableApplicationLayerAutomaticResponse: {wire: ok, errors: ok, state: ok, persist: ok}
  DisableApplicationLayerAutomaticResponse: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateApplicationLayerAutomaticResponse: {wire: ok, errors: ok, state: ok, persist: ok}
  ListResourcesInProtectionGroup: {wire: ok, errors: ok, state: ok, persist: n/a, note: "derives ALL/BY_RESOURCE_TYPE membership live from the protections table"}
families:
  protection: {status: ok, note: "CreateProtection/DescribeProtection/DeleteProtection/ListProtections verified against types.Protection -- no CreationTime/Tags fields on the real shape; gopherstack emits an extra CreationTime, harmless (unknown-field-tolerant awsjson1.1 deserializer)"}
  protectionGroup: {status: ok, note: "same extra-CreationTime note as protection; ProtectionGroup has no CreationTime in real SDK either"}
  subscription: {status: ok, note: "TimeCommitmentInSeconds bug fixed this sweep; Limits/SubscriptionLimits nested shapes verified field-by-field against types.SubscriptionLimits/ProtectionLimits/ProtectionGroupLimits -- gopherstack's extra 'MaxProtections' key on ProtectionLimits is fabricated (not a real field) but harmless since it's additive"}
  attack: {status: ok, note: "ListAttacks/DescribeAttack/DescribeAttackStatistics verified against types.AttackSummary/AttackDetail/AttackStatisticsDataItem/AttackVolume"}
  tags: {status: ok, note: "resolveTaggableProtection accepts both Shield protection ARN and resource ARN, matching TagResourceInput.ResourceARN semantics"}
  drtAccessAndEngagement: {status: ok, note: "AssociateDRTRole/LogBucket, proactive engagement state machine (DISABLED->PENDING->ENABLED) verified"}
  alar: {status: ok, note: "ApplicationLayerAutomaticResponseConfiguration nested in Protection response only when set, matching optional-field AWS behavior"}
gaps:
  - CreateProtection/CreateProtectionGroup do not enforce the Shield Advanced quota limits they themselves report via DescribeSubscription (subscriptionMaxProtections=1000, subscriptionMaxProtectionsPerType=100, subscriptionMaxProtectionGroups=100, subscriptionMaxMembersPerGroup=10000) -- real AWS returns LimitsExceededException when exceeded; gopherstack currently allows unbounded creation. Feature gap, not a wire bug. (bd: file follow-up issue)
  - resolveShieldProtectionARN (backend.go) hardcodes the "arn:aws:shield::" partition prefix while protectionARN/protectionGroupARN build via arn.Build(service, region, ...) which derives partition from region (aws/aws-cn/aws-us-gov). Only matters for non-default-partition accounts; Shield Advanced protection ARNs are effectively always "aws" partition in practice, so this is very low risk.
  - DescribeAttack/ListAttacks never populate AttackDetail.AttackProperties or AttackDetail.SubResources (both optional AWS fields); simulated/internal attacks only carry AttackVectors/AttackCounters/Mitigations. Acceptable for a synthetic-attack emulator but noted for completeness.
deferred: []
leaks: {status: clean, note: "no goroutines/janitors in this service; all state lives in store.Table-backed maps guarded by lockmetrics.RWMutex; Snapshot/Restore round-trip verified (persistence_test.go)"}
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
  actively reads are real bugs (the `TimeCommitmentInSeconds` bug fixed this sweep was the
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
- Error mapping (`handleError` in handler.go) doesn't inspect real per-op error catalogs
  (e.g. `LimitsExceededException`, `InvalidResourceException`, `OptimisticLockException`,
  `LockedSubscriptionException`, `NoAssociatedRoleException` are never returned -- they'd
  require the limit-enforcement gap above and a few other unimplemented edge conditions to
  ever trigger). All four sentinel error families gopherstack does raise
  (`ResourceNotFoundException`, `ResourceAlreadyExistsException`, `InvalidParameterException`,
  `InternalErrorException`) round-trip correctly through `errCodeLookup`-equivalent
  `errors.Is` dispatch in `handleError`.
