---
service: awsconfig
sdk_module: aws-sdk-go-v2/service/configservice@v1.61.2
last_audit_commit: 0a5200a4
last_audit_date: 2026-07-12
overall: A            # genuine fixes found: wire-shape error-type bugs + several disguised no-ops
ops:
  # --- ConfigurationRecorder family ---
  PutConfigurationRecorder: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeConfigurationRecorders: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now populates the arn field (was missing entirely)"}
  DescribeConfigurationRecorderStatus: {wire: ok, errors: ok, state: ok, persist: ok}
  StartConfigurationRecorder: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: NoAvailableDeliveryChannelException was mis-mapped to ValidationException"}
  StopConfigurationRecorder: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteConfigurationRecorder: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: not-found wire type was 'NoSuchConfigurationRecorder', real SDK is 'NoSuchConfigurationRecorderException'"}
  ListConfigurationRecorders: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateResourceTypes: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: was a disguised no-op (discarded ResourceTypes, fabricated a synthetic recorder for unknown ARNs instead of erroring). Now mutates RecordingGroup.ResourceTypes and errors NoSuchConfigurationRecorderException for unknown recorders"}
  DisassociateResourceTypes: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: was a pure no-op stub; now removes types from RecordingGroup.ResourceTypes"}
  PutServiceLinkedConfigurationRecorder: {wire: partial, errors: partial, state: gap, persist: n/a, note: "intentional minimal stub -- see gopherstack-e0f1"}
  DeleteServiceLinkedConfigurationRecorder: {wire: partial, errors: partial, state: gap, persist: n/a, note: "intentional minimal stub -- see gopherstack-e0f1"}

  # --- DeliveryChannel family ---
  PutDeliveryChannel: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeDeliveryChannels: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDeliveryChannel: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeDeliveryChannelStatus: {wire: ok, errors: ok, state: ok, persist: ok}
  DeliverConfigSnapshot: {wire: partial, errors: partial, state: gap, persist: n/a, note: "intentional minimal stub -- see gopherstack-e0f1"}

  # --- ConfigRule + compliance family ---
  PutConfigRule: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeConfigRules: {wire: ok, errors: partial, state: ok, persist: ok, note: "unknown names in filter silently dropped instead of NoSuchConfigRuleException -- see gopherstack-s7u1"}
  DeleteConfigRule: {wire: ok, errors: ok, state: ok, persist: ok}
  GetComplianceDetailsByConfigRule: {wire: ok, errors: partial, state: ok, persist: ok, note: "unknown rule silently returns empty instead of NoSuchConfigRuleException -- see gopherstack-s7u1"}
  GetComplianceDetailsByResource: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeComplianceByConfigRule: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeComplianceByResource: {wire: partial, errors: partial, state: gap, persist: n/a, note: "intentional minimal stub -- see gopherstack-e0f1"}
  GetComplianceSummaryByConfigRule: {wire: ok, errors: ok, state: ok, persist: ok}
  GetComplianceSummaryByResourceType: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeConfigRuleEvaluationStatus: {wire: ok, errors: ok, state: ok, persist: ok}
  StartConfigRulesEvaluation: {wire: ok, errors: ok, state: ok, persist: ok, note: "real evaluation engine (evaluation.go) -- not a disguised no-op"}
  PutEvaluations: {wire: ok, errors: ok, state: ok, persist: ok}
  PutExternalEvaluation: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteEvaluationResults: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: was a pure no-op stub; now validates the rule exists (NoSuchConfigRuleException) and clears rollup + per-resource evaluations"}
  GetCustomRulePolicy: {wire: ok, errors: ok, state: ok, persist: n/a}
  StartResourceEvaluation: {wire: ok, errors: ok, state: ok, persist: ok}
  GetResourceEvaluationSummary: {wire: ok, errors: ok, state: ok, persist: ok}
  ListResourceEvaluations: {wire: ok, errors: ok, state: ok, persist: ok}

  # --- ConformancePack family ---
  PutConformancePack: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeConformancePacks: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteConformancePack: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeConformancePackStatus: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeConformancePackCompliance: {wire: partial, errors: partial, state: gap, persist: n/a, note: "intentional minimal stub -- see gopherstack-e0f1"}
  GetConformancePackComplianceDetails: {wire: partial, errors: partial, state: gap, persist: n/a, note: "intentional minimal stub -- see gopherstack-e0f1"}
  GetConformancePackComplianceSummary: {wire: partial, errors: partial, state: gap, persist: n/a, note: "intentional minimal stub -- see gopherstack-e0f1"}
  ListConformancePackComplianceScores: {wire: partial, errors: partial, state: gap, persist: n/a, note: "intentional minimal stub -- see gopherstack-e0f1"}

  # --- AggregationAuthorization / ConfigurationAggregator family ---
  PutAggregationAuthorization: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAggregationAuthorizations: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAggregationAuthorization: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: was returning a fabricated NoSuchAggregationAuthorizationException (not a real AWS error for this op); now idempotent, matching AWS"}
  PutConfigurationAggregator: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeConfigurationAggregators: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteConfigurationAggregator: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeConfigurationAggregatorSourcesStatus: {wire: partial, errors: partial, state: gap, persist: n/a, note: "intentional minimal stub -- see gopherstack-e0f1"}
  DescribeAggregateComplianceByConfigRules: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAggregateComplianceByConformancePacks: {wire: partial, errors: partial, state: gap, persist: n/a, note: "intentional minimal stub -- see gopherstack-e0f1"}
  GetAggregateComplianceDetailsByConfigRule: {wire: partial, errors: partial, state: gap, persist: n/a, note: "intentional minimal stub -- see gopherstack-e0f1"}
  GetAggregateConfigRuleComplianceSummary: {wire: partial, errors: partial, state: gap, persist: n/a, note: "intentional minimal stub -- see gopherstack-e0f1"}
  GetAggregateConformancePackComplianceSummary: {wire: partial, errors: partial, state: gap, persist: n/a, note: "intentional minimal stub -- see gopherstack-e0f1"}
  GetAggregateDiscoveredResourceCounts: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAggregateResourceConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchGetAggregateResourceConfig: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: was a disguised no-op (every identifier always unprocessed even though resourceConfigs had real matches); now resolves against local resourceConfigs table"}
  SelectAggregateResourceConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAggregateDiscoveredResources: {wire: partial, errors: partial, state: gap, persist: n/a, note: "intentional minimal stub -- see gopherstack-e0f1"}
  DescribePendingAggregationRequests: {wire: partial, errors: partial, state: gap, persist: n/a, note: "intentional minimal stub -- see gopherstack-e0f1"}
  DeletePendingAggregationRequest: {wire: partial, errors: partial, state: gap, persist: n/a, note: "intentional minimal stub -- see gopherstack-e0f1"}

  # --- RemediationConfiguration family ---
  PutRemediationConfigurations: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeRemediationConfigurations: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteRemediationConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  PutRemediationExceptions: {wire: ok, errors: ok, state: ok, persist: n/a}
  DescribeRemediationExceptions: {wire: ok, errors: ok, state: ok, persist: n/a}
  DeleteRemediationExceptions: {wire: ok, errors: ok, state: ok, persist: n/a}
  StartRemediationExecution: {wire: partial, errors: partial, state: gap, persist: n/a, note: "intentional minimal stub -- see gopherstack-e0f1"}
  DescribeRemediationExecutionStatus: {wire: partial, errors: partial, state: gap, persist: n/a, note: "intentional minimal stub -- see gopherstack-e0f1"}

  # --- OrganizationConfigRule / OrganizationConformancePack family ---
  PutOrganizationConfigRule: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeOrganizationConfigRules: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteOrganizationConfigRule: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeOrganizationConfigRuleStatuses: {wire: ok, errors: ok, state: ok, persist: ok}
  GetOrganizationConfigRuleDetailedStatus: {wire: partial, errors: partial, state: gap, persist: n/a, note: "intentional minimal stub -- see gopherstack-e0f1"}
  GetOrganizationCustomRulePolicy: {wire: ok, errors: ok, state: ok, persist: n/a}
  PutOrganizationConformancePack: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeOrganizationConformancePacks: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteOrganizationConformancePack: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: wire error type was 'OrganizationConformancePackNotFoundException' (does not exist in the real SDK); correct type is 'NoSuchOrganizationConformancePackException'"}
  DescribeOrganizationConformancePackStatuses: {wire: ok, errors: ok, state: ok, persist: ok}
  GetOrganizationConformancePackDetailedStatus: {wire: partial, errors: partial, state: gap, persist: n/a, note: "intentional minimal stub -- see gopherstack-e0f1"}

  # --- RetentionConfiguration family ---
  PutRetentionConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeRetentionConfigurations: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteRetentionConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}

  # --- StoredQuery family ---
  PutStoredQuery: {wire: ok, errors: ok, state: ok, persist: ok}
  GetStoredQuery: {wire: ok, errors: ok, state: ok, persist: ok}
  ListStoredQueries: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteStoredQuery: {wire: ok, errors: ok, state: ok, persist: ok}

  # --- ResourceConfig (Get/List/BatchGet/Select) family ---
  PutResourceConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteResourceConfig: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: was a pure no-op stub, never removed the resource from resourceConfigs despite the table holding real PutResourceConfig data"}
  GetResourceConfigHistory: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDiscoveredResources: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchGetResourceConfig: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: was a disguised no-op (every key always unprocessed even though resourceConfigs had real matches); now resolves against resourceConfigs table"}
  SelectResourceConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDiscoveredResourceCounts: {wire: ok, errors: ok, state: ok, persist: n/a}

  # --- Tags family ---
  TagResource: {wire: ok, errors: ok, state: ok, persist: n/a}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: n/a}

gaps:
  - ~18 cross-account/organization aggregation compliance+status ops are intentional
    minimal stubs (empty lists/summaries) because this emulator has no real
    multi-account model to source data from (bd: gopherstack-e0f1)
  - DescribeConfigRules / GetComplianceDetailsByConfigRule silently drop unknown rule
    names instead of erroring NoSuchConfigRuleException like real AWS
    (bd: gopherstack-s7u1)
  - ErrValidation is mapped to a single generic ValidationException wire type for every
    Put* op instead of AWS's per-op Invalid*Exception taxonomy (InvalidRoleException,
    InvalidRecordingGroupException, InvalidS3KeyPrefixException, etc.)
    (bd: gopherstack-eboy)
deferred:
  - Per-field/per-op AWS validation ordering and exact message text (not audited this pass)
leaks: {status: clean, note: "no goroutines/janitors in this service; single coarse lockmetrics.RWMutex, no leak surface found"}
---

## Notes

- Wire protocol: awsjson1.1, single POST endpoint, `X-Amz-Target:
  StarlingDoveService.<Op>`. Verified the `StarlingDoveService` target prefix and every
  routed op name against `aws-sdk-go-v2/service/configservice@v1.61.2`'s
  `serializers.go`/`deserializers.go` -- all 97 real SDK ops are wired into the dispatch
  table (`buildDispatchTable` + `buildExtendedDispatchTable`), none missing.

- `ConfigurationRecorder`/`DeliveryChannel` use **camelCase** wire field names (`name`,
  `roleARN`, `recordingGroup`, `arn`, `s3BucketName`, ...) -- this is genuinely how AWS
  Config serializes these two shapes (confirmed against the real serializer), unlike
  `ConfigRule`/most other shapes in this API which use PascalCase. Don't "fix" this to
  PascalCase in a future pass -- it would break real-SDK-client compatibility.

- Persistence: `Handler.Snapshot`/`Restore` delegate to `InMemoryBackend`, which uses a
  versioned `backendSnapshot{Tables, Version}` wrapping `store.Registry.SnapshotAll()`
  over 14 registered tables. Six scalar/slice-valued maps (`ruleEvaluations`,
  `resourceHistory`, `resourceTags`, `remediationExceptions`, `customRulePolicies`,
  `orgCustomRulePolicies`) have no `store.Table` identity and are NOT persisted -- this
  is a pre-existing gap (not introduced or fixed this pass), see `persistence.go`'s doc
  comment.

- Bug-class findings this pass (see `.claude/memories/parity-principles.md` bug classes):
  - **Wrong wire error-type strings**: `ErrNotFound`'s wire type was
    `"NoSuchConfigurationRecorder"`; every real config-recorder-not-found error is
    `"NoSuchConfigurationRecorderException"` (confirmed across
    DeleteConfigurationRecorder/StartConfigurationRecorder/StopConfigurationRecorder/
    AssociateResourceTypes/DisassociateResourceTypes deserializers). Any real SDK client
    doing `errors.As(&types.NoSuchConfigurationRecorderException{})` would have silently
    fallen through to a generic `smithy.GenericAPIError` instead.
  - **Wrong wire error-type strings (2)**: `ErrNoSuchOrganizationConformancePack`'s wire
    type was `"OrganizationConformancePackNotFoundException"`, which does not exist
    anywhere in the real SDK's error taxonomy; the real type is
    `"NoSuchOrganizationConformancePackException"`.
  - **Error-family collision**: `ErrNoDeliveryChannel` (StartConfigurationRecorder with no
    delivery channel) shared a switch case with `ErrValidation` and was emitted as
    `"ValidationException"`; the real wire type is `"NoAvailableDeliveryChannelException"`.
  - **Fabricated error that doesn't exist in AWS**: `DeleteAggregationAuthorization`
    invented a `NoSuchAggregationAuthorizationException` 404 for a missing authorization.
    The real op's declared error model (per the generated deserializer) has no not-found
    exception at all -- delete is idempotent in AWS. Now idempotent here too.
  - **Disguised no-ops (real backend state existed but was ignored)**: `AssociateResourceTypes`
    discarded its `ResourceTypes` argument and fabricated a synthetic recorder for any
    unknown ARN instead of erroring; `BatchGetResourceConfig`/`BatchGetAggregateResourceConfig`
    always reported every key/identifier unprocessed even though `resourceConfigs`
    (populated by `PutResourceConfig`) had real matching data; `DeleteResourceConfig` and
    `DisassociateResourceTypes` were pure `return nil` no-ops despite real backend state
    to mutate; `DeleteEvaluationResults` was a pure no-op despite `ruleEvaluations`/
    `ruleResourceEvals` holding real per-rule evaluation state to clear.
