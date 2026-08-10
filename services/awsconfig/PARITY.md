---
service: awsconfig
sdk_module: aws-sdk-go-v2/service/configservice@v1.68.4
last_audit_commit: 198990e82
last_audit_date: 2026-08-07
overall: A            # this pass: implemented the 5 ops the SDK bump (v1.61.2 -> v1.68.0)
                       # revealed as newly-supported and missing from GetSupportedOperations:
                       # PutConnector/GetConnector/ListConnectors/DeleteConnector (a new
                       # Connector family) and PutThirdPartyServiceLinkedConfigurationRecorder
                       # (wired into the existing ConfigurationRecorder model, not a new one --
                       # see its entry below). Prior pass's grade/notes retained unchanged.
ops:
  # --- ConfigurationRecorder family ---
  PutConfigurationRecorder: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: empty/blank name now InvalidConfigurationRecorderNameException, empty roleARN now InvalidRoleException (were both generic ValidationException) -- see gopherstack-eboy"}
  DescribeConfigurationRecorders: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeConfigurationRecorderStatus: {wire: ok, errors: ok, state: ok, persist: ok}
  StartConfigurationRecorder: {wire: ok, errors: ok, state: ok, persist: ok}
  StopConfigurationRecorder: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteConfigurationRecorder: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascade-cleans any ServiceLinkedRecorderLink pointing at the deleted recorder"}
  ListConfigurationRecorders: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateResourceTypes: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateResourceTypes: {wire: ok, errors: ok, state: ok, persist: ok}
  PutServiceLinkedConfigurationRecorder: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed (gopherstack-e0f1): was a no-op stub; now creates a real ACTIVE recorder named AWSConfigurationRecorderFor<Service> (best-effort deterministic casing -- AWS's exact per-service capitalization isn't publicly enumerable), idempotent per ServicePrincipal via a new ServiceLinkedRecorderLink table"}
  DeleteServiceLinkedConfigurationRecorder: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed (gopherstack-e0f1): was a no-op stub; now looks up and deletes the linked recorder, NoSuchConfigurationRecorderException when unknown"}
  PutThirdPartyServiceLinkedConfigurationRecorder: {wire: ok, errors: ok, state: ok, persist: ok, note: "new op (SDK bump v1.61.2 -> v1.68.0). ConfigurationRecorder gained three real wire fields for this (connectorArn/scopeConfiguration/servicePrincipal, field-diffed against the SDK's serializeDocumentConfigurationRecorder) so a third-party service-linked recorder is a genuine RECORDER in the existing model: DescribeConfigurationRecorders/DescribeConfigurationRecorderStatus/ListConfigurationRecorders/DeleteConfigurationRecorder all see and can act on it via the plain recorders table, no orphan. Enforces the real declared constraint (verified against the AWS Config API reference's PutConnector/PutThirdPartyServiceLinkedConfigurationRecorder ConflictException doc: 'the specified service principal does not support multiple configuration recorders and one already exists') -- one service-linked recorder per ServicePrincipal, looked up via a new recordersByServicePrincipal secondary index. Put is create-or-update *conditionally*: same ServicePrincipal+same ConnectorArn updates ScopeConfiguration (idempotent, matching the doc comment); same ServicePrincipal+different ConnectorArn errors ConflictException (not a silent upsert, unlike PutServiceLinkedConfigurationRecorder) -- confirmed against both the doc comment and the deserializer's declared error switch (ConflictException/InsufficientPermissionsException/ValidationException only, no ResourceNotFoundException, so an unknown ConnectorArn errors ValidationException not ErrResourceNotFound)."}

  # --- Connector family (new, SDK bump v1.61.2 -> v1.68.0) ---
  PutConnector: {wire: ok, errors: ok, state: ok, persist: ok, note: "new op. NOT idempotent -- verified against the doc comment ('Connectors cannot be updated -- to update the connector configuration, you must delete all associated configuration recorders, delete the connector, and recreate it') and the declared ConflictException ('a connector already exists for the specified connector configuration'): a repeat PutConnector with a ConnectorConfiguration matching an existing connector errors ConflictException rather than upserting. Requires exactly one provider (Azure, the only one AWS Config documents) with both ClientIdentifier/TenantIdentifier set -- ValidationException otherwise (the SDK's client-side validators.go doesn't itself enforce 'exactly one provider', so this is server-side). Connector Name/Arn are server-generated (PutConnectorInput has no Name field) -- best-effort deterministic naming, same caveat as the existing serviceLinkedRecorderName."}
  GetConnector: {wire: ok, errors: ok, state: ok, persist: ok, note: "new op. Unknown Arn errors ResourceNotFoundException (ErrResourceNotFound), matching the declared error switch (ResourceNotFoundException/ValidationException only)."}
  ListConnectors: {wire: ok, errors: ok, state: ok, persist: ok, note: "new op. Filters by the real 'provider' FilterName (types.ConnectorFilterName's sole enum value); paginated via the existing pkgs/page helper, mirroring DescribeConfigRules' pattern."}
  DeleteConnector: {wire: ok, errors: ok, state: ok, persist: ok, note: "new op. Unknown Arn errors ResourceNotFoundException; the real op's declared error model has no ConflictException for 'still referenced by a recorder', so this backend doesn't invent one either."}

  # --- DeliveryChannel family ---
  PutDeliveryChannel: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: empty/blank name now InvalidDeliveryChannelNameException (was generic ValidationException) -- see gopherstack-eboy"}
  DescribeDeliveryChannels: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteDeliveryChannel: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeDeliveryChannelStatus: {wire: ok, errors: ok, state: ok, persist: ok}
  DeliverConfigSnapshot: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed (gopherstack-e0f1): was a no-op stub; now validates the named channel exists (NoSuchDeliveryChannelException), a recorder is configured (NoAvailableConfigurationRecorderException) and running (NoRunningConfigurationRecorderException), and returns a generated ConfigSnapshotId"}

  # --- ConfigRule + compliance family ---
  PutConfigRule: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeConfigRules: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed (gopherstack-s7u1): unknown name in a non-empty ConfigRuleNames filter now errors NoSuchConfigRuleException instead of silently omitting it; backend signature changed to return an error (~14 call sites across this package updated)"}
  DeleteConfigRule: {wire: ok, errors: ok, state: ok, persist: ok}
  GetComplianceDetailsByConfigRule: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed (gopherstack-s7u1): unknown ConfigRuleName now errors NoSuchConfigRuleException instead of silently returning empty"}
  GetComplianceDetailsByResource: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeComplianceByConfigRule: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeComplianceByResource: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed (gopherstack-e0f1): was an empty-list stub; now rolls per-(rule,resource) evaluations (b.ruleResourceEvals) up per resource, with ComplianceContributorCount and ResourceType/ResourceId/ComplianceTypes filters"}
  GetComplianceSummaryByConfigRule: {wire: ok, errors: ok, state: ok, persist: ok}
  GetComplianceSummaryByResourceType: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeConfigRuleEvaluationStatus: {wire: ok, errors: ok, state: ok, persist: ok}
  StartConfigRulesEvaluation: {wire: ok, errors: ok, state: ok, persist: ok}
  PutEvaluations: {wire: ok, errors: ok, state: ok, persist: ok}
  PutExternalEvaluation: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteEvaluationResults: {wire: ok, errors: ok, state: ok, persist: ok}
  GetCustomRulePolicy: {wire: ok, errors: ok, state: ok, persist: n/a}
  StartResourceEvaluation: {wire: ok, errors: ok, state: ok, persist: ok}
  GetResourceEvaluationSummary: {wire: ok, errors: ok, state: ok, persist: ok}
  ListResourceEvaluations: {wire: ok, errors: ok, state: ok, persist: ok}

  # --- ConformancePack family ---
  PutConformancePack: {wire: fixed, errors: fixed, state: fixed, persist: ok, note: "extended (gopherstack-e0f1): now accepts TemplateBody and, when it's a JSON or YAML CloudFormation-shaped template with AWS::Config::ConfigRule resources, deploys those as real config rules linked to the pack (see conformance_pack_template.go) -- matching real AWS Config, where a conformance pack literally creates managed config rules. FIXED this pass (gopherstack-ag85): (1) TemplateBody now also parses YAML, not JSON-only -- tried as JSON first, falling back to YAML via yamlToJSON; (2) TemplateS3Uri and TemplateSSMDocumentDetails, previously entirely absent from putConformancePackInput and therefore silently dropped by the JSON decoder even when a client sent them, are now parsed off the wire; (3) specifying more than one of TemplateBody/TemplateS3Uri/TemplateSSMDocumentDetails is now rejected with ValidationException, matching PutConformancePackInput's documented \"only one of\" constraint. TemplateS3Uri/TemplateSSMDocumentDetails still deploy zero rules (no S3/SSM fetcher -- see gaps); a request specifying zero sources is still accepted (deploys zero rules) rather than rejected, to avoid breaking this codebase's existing tests that call PutConformancePack with no template purely to set up pack existence -- the exact zero-sources validation is pre-existing deferred scope (gopherstack-eboy), not this pass's target."}
  DescribeConformancePacks: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteConformancePack: {wire: ok, errors: ok, state: ok, persist: ok, note: "extended: cascade-deletes every config rule the pack deployed (and their evaluations), matching AWS"}
  DescribeConformancePackStatus: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeConformancePackCompliance: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed (gopherstack-e0f1): was an empty-list stub; now returns each deployed rule's rolled-up compliance from b.ruleEvaluations, with NoSuchConformancePackException/NoSuchConfigRuleInConformancePackException validation and ComplianceType/ConfigRuleNames filters"}
  GetConformancePackComplianceDetails: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed (gopherstack-e0f1): was an empty-list stub; now returns real per-resource evaluation results for the pack's deployed rules (DetailedEvaluationResult is wire-shape identical to ConformancePackEvaluationResult)"}
  GetConformancePackComplianceSummary: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed (gopherstack-e0f1): was an empty-list stub; now rolls each named pack's deployed rules up into COMPLIANT/NON_COMPLIANT/INSUFFICIENT_DATA per AWS's documented rollup rule, NoSuchConformancePackException for unknown names"}
  ListConformancePackComplianceScores: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed (gopherstack-e0f1): was an empty-list stub; now computes a real percentage compliance score per pack from its rule-resource evaluations, INSUFFICIENT_DATA when none recorded"}

  # --- AggregationAuthorization / ConfigurationAggregator family ---
  PutAggregationAuthorization: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAggregationAuthorizations: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAggregationAuthorization: {wire: ok, errors: ok, state: ok, persist: ok}
  PutConfigurationAggregator: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeConfigurationAggregators: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteConfigurationAggregator: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeConfigurationAggregatorSourcesStatus: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed (gopherstack-e0f1): was an empty-list stub; now derives one status entry per configured AccountAggregationSources/OrganizationAggregationSource account+region, reporting SUCCEEDED (no per-source sync failures modeled), NoSuchConfigurationAggregatorException validation"}
  DescribeAggregateComplianceByConfigRules: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAggregateComplianceByConformancePacks: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed (gopherstack-e0f1): was an empty-list stub; now returns every local conformance pack's rule counts, tagged with the requested account/region, once the aggregator is validated to exist"}
  GetAggregateComplianceDetailsByConfigRule: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed (gopherstack-e0f1): was an empty-list stub; now reuses local per-resource evaluations (mirroring the already-established DescribeAggregateComplianceByConfigRules pattern), echoing the requested accountId/awsRegion, aggregator existence validated"}
  GetAggregateConfigRuleComplianceSummary: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed (gopherstack-e0f1): was an empty-list stub; now derives a single-group compliant/non-compliant rollup keyed by the local account ID or region (GroupByKey), aggregator existence validated"}
  GetAggregateConformancePackComplianceSummary: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed (gopherstack-e0f1): was an empty-list stub; now derives compliant/non-compliant conformance-pack counts for the local account/region group, aggregator existence validated"}
  GetAggregateDiscoveredResourceCounts: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAggregateResourceConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchGetAggregateResourceConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  SelectAggregateResourceConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAggregateDiscoveredResources: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed (gopherstack-e0f1): was an empty-list stub; now returns local discovered resources of the requested type tagged with the local account/region as source, account/region/resourceId filters applied, aggregator existence validated"}
  DescribePendingAggregationRequests: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed (gopherstack-e0f1): was an empty-list stub; now derives pending requests from AggregationAuthorizations this account granted that no local ConfigurationAggregator has yet incorporated into its AccountAggregationSources -- the only genuinely-derivable cross-account state a single-account emulator has"}
  DeletePendingAggregationRequest: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed (gopherstack-e0f1): was a no-op stub; now deletes the underlying AggregationAuthorization, idempotent per its declared error model (no not-found exception)"}

  # --- RemediationConfiguration family ---
  PutRemediationConfigurations: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeRemediationConfigurations: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteRemediationConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "extended: cascade-deletes any recorded remediation executions for the rule too (new remediationExecutions table introduced this pass)"}
  PutRemediationExceptions: {wire: ok, errors: ok, state: ok, persist: n/a}
  DescribeRemediationExceptions: {wire: ok, errors: ok, state: ok, persist: n/a}
  DeleteRemediationExceptions: {wire: ok, errors: ok, state: ok, persist: n/a}
  StartRemediationExecution: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed (gopherstack-e0f1): was a no-op stub; now validates a remediation configuration exists for the rule (NoSuchRemediationConfigurationException) and records a SUCCEEDED execution per resource key (no real SSM Automation runner modeled), readable back via DescribeRemediationExecutionStatus"}
  DescribeRemediationExecutionStatus: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed (gopherstack-e0f1): was an empty-list stub; now returns recorded executions for the rule, optionally filtered by resource key, NoSuchRemediationConfigurationException validation"}

  # --- OrganizationConfigRule / OrganizationConformancePack family ---
  PutOrganizationConfigRule: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeOrganizationConfigRules: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteOrganizationConfigRule: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeOrganizationConfigRuleStatuses: {wire: ok, errors: ok, state: ok, persist: ok}
  GetOrganizationConfigRuleDetailedStatus: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed (gopherstack-e0f1): was an empty-list stub; now returns a single CREATE_SUCCESSFUL MemberAccountStatus for the local account (the only member this single-account emulator can model), NoSuchOrganizationConfigRuleException validation, optional AccountId filter"}
  GetOrganizationCustomRulePolicy: {wire: ok, errors: ok, state: ok, persist: n/a}
  PutOrganizationConformancePack: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeOrganizationConformancePacks: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteOrganizationConformancePack: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeOrganizationConformancePackStatuses: {wire: ok, errors: ok, state: ok, persist: ok}
  GetOrganizationConformancePackDetailedStatus: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed (gopherstack-e0f1): was an empty-list stub; mirrors GetOrganizationConfigRuleDetailedStatus's single-local-account model, NoSuchOrganizationConformancePackException validation"}

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
  DeleteResourceConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  GetResourceConfigHistory: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDiscoveredResources: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchGetResourceConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  SelectResourceConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDiscoveredResourceCounts: {wire: ok, errors: ok, state: ok, persist: n/a}

  # --- Tags family ---
  TagResource: {wire: ok, errors: ok, state: ok, persist: n/a}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: n/a}

gaps:
  - ErrValidation is still mapped to a single generic ValidationException wire type for
    most Put* validation paths. This pass added the three most load-bearing per-op
    Invalid*Exception types (InvalidConfigurationRecorderNameException,
    InvalidRoleException on PutConfigurationRecorder; InvalidDeliveryChannelNameException
    on PutDeliveryChannel). Still generic: InvalidRecordingGroupException,
    InvalidS3KeyPrefixException, InvalidS3KmsKeyArnException, InvalidSNSTopicARNException,
    and the full per-op taxonomy for every other Put* op (bd: gopherstack-eboy, updated
    this pass with a comment noting partial completion -- not closed)
  - PutConformancePack's TemplateS3Uri/TemplateSSMDocumentDetails template sources
    (bd: gopherstack-ag85, JSON+YAML TemplateBody parsing FIXED this pass) still deploy
    zero rules rather than being fetched/parsed: real fetching needs cross-service S3/SSM
    access, which this service has no wiring for -- appsync/vpclattice-style cross-service
    calls in this fleet are wired centrally in cli.go, outside this task's
    services/awsconfig/ edit boundary. Buildable with that wiring in place (not a
    structural impossibility), so kept in gaps rather than structural_gaps. Honest
    limitation: the request is accepted and the source is stored on nothing (not
    fabricated), documented in conformance_pack_template.go/conformance_packs.go.
  - PutConformancePack accepts zero template sources (TemplateBody/TemplateS3Uri/
    TemplateSSMDocumentDetails all empty) without erroring, though real AWS Config
    requires exactly one. This pass added rejection for *more than one* source (a genuine
    new validation, real and tested), but left the zero-sources case alone: this
    codebase's existing test suite routinely calls PutConformancePack with no template
    purely to establish a pack's existence for unrelated assertions (DeleteConformancePack,
    ARN format, etc.), and enforcing the full requirement would need updating every one of
    those call sites' intent, which is per-field validation-taxonomy work already tracked
    under gopherstack-eboy, not this issue's scope.
  - MaxNumberOfConnectorsExceededException (PutConnector's per-account connector-count
    limit) is declared by the real API but its numeric value isn't published anywhere in
    AWS's docs (checked the API reference and the Config service-limits page as of this
    pass -- no "connectors" row exists in either). Not enforced rather than guessing an
    unverifiable number; the wire error type isn't wired into errorWireMappings since
    nothing in this backend raises it. Same caveat as the pre-existing, still-unenforced
    single-customer-managed-recorder limit noted below.
  - The single-customer-managed-configuration-recorder-per-account limit (AWS historically
    allows exactly one) is still unenforced by PutConfigurationRecorder -- ErrAlreadyExists/
    MaxNumberOfConfigurationRecordersExceededException exist in errors.go/handler.go's wire
    mapping but nothing calls them; this predates this pass (PutConfigurationRecorder was
    out of this pass's scope) and is called out here only because this pass's audit of
    "how many recorders can an account have" touched the same code path. The NEW
    PutThirdPartyServiceLinkedConfigurationRecorder's own one-per-ServicePrincipal limit
    (see its ops entry above) IS enforced -- that op's ConflictException is real,
    unlike this pre-existing gap.
deferred:
  - Per-field/per-op AWS validation ordering and exact message text (not audited this pass)
leaks: {status: clean, note: "no goroutines/janitors in this service; single coarse lockmetrics.RWMutex; every new Lock/RLock this pass is defer-released; DeleteConfigurationRecorder cascade-cleans ServiceLinkedRecorderLink rows, DeleteConformancePack cascade-cleans its deployed config rules + evaluations, DeleteRemediationConfiguration cascade-cleans its recorded executions -- no ghost rows found"}
---

## Notes

- Wire protocol: awsjson1.1, single POST endpoint, `X-Amz-Target:
  StarlingDoveService.<Op>`. Verified the `StarlingDoveService` target prefix and every
  routed op name against `aws-sdk-go-v2/service/configservice@v1.68.0`'s
  `serializers.go`/`deserializers.go` -- all 102 real SDK ops (97 from the prior audit's
  v1.61.2 + the 5 this pass added for the v1.68.0 bump: `StarlingDoveService.PutConnector`,
  `.GetConnector`, `.ListConnectors`, `.DeleteConnector`,
  `.PutThirdPartyServiceLinkedConfigurationRecorder`) are wired into the dispatch table,
  none missing -- confirmed via `TestSDKCompleteness`.

- `ConfigurationRecorder`/`DeliveryChannel` use **camelCase** wire field names (`name`,
  `roleARN`, `recordingGroup`, `arn`, `s3BucketName`, ...) -- this is genuinely how AWS
  Config serializes these two shapes (confirmed against the real serializer), unlike
  `ConfigRule`/most other shapes in this API which use PascalCase. Don't "fix" this to
  PascalCase in a future pass -- it would break real-SDK-client compatibility.

- Persistence: `Handler.Snapshot`/`Restore` delegate to `InMemoryBackend`, which uses a
  versioned `backendSnapshot{Tables, Version}` wrapping `store.Registry.SnapshotAll()`.
  The prior pass bumped `awsconfigSnapshotVersion` 1 -> 2, adding three tables:
  `conformancePackRules` (which config rules each conformance pack deployed),
  `remediationExecutions` (StartRemediationExecution history), and
  `serviceLinkedRecorders` (servicePrincipal -> recorder-name links -- kept as its own
  table rather than a field on `ConfigurationRecorder` specifically so it isn't lost by
  round-tripping through `json:"-"`, since `ConfigurationRecorder` is serialized verbatim
  as the real AWS wire response and store.Table's Snapshot/Restore marshal that same
  struct/tags). This pass bumped it 2 -> 3, adding the `connectors` table (Connector
  values keyed by ARN) and a `byServicePrincipal` secondary index on the existing
  `recorders` table -- unlike `serviceLinkedRecorders`, this new index needed no separate
  Table/Tables-map entry: `ConfigurationRecorder` itself gained real
  `ConnectorArn`/`ScopeConfiguration`/`ServicePrincipal` wire fields (field-diffed against
  the SDK's `serializeDocumentConfigurationRecorder`), so the index just derives its key
  from the recorder's own new field and rides the recorders table's existing
  Snapshot/Restore. Six scalar/slice-valued maps (`ruleEvaluations`, `resourceHistory`,
  `resourceTags`, `remediationExceptions`, `customRulePolicies`, `orgCustomRulePolicies`)
  still have no `store.Table` identity and are NOT persisted -- this is a pre-existing gap
  (not introduced or fixed this pass), see `persistence.go`'s doc comment.

- 2026-07-25 pass (SDK bump v1.61.2 -> v1.68.0 revealed 5 new operations): implemented
  all 5 for real rather than adding them to `notImplemented` -- `PutConnector`/
  `GetConnector`/`ListConnectors`/`DeleteConnector` (new Connector family, see their ops
  entries above) and `PutThirdPartyServiceLinkedConfigurationRecorder`. The key
  correctness risk flagged going in was whether a third-party service-linked recorder
  would be an orphan no existing op could observe -- it isn't: `ConfigurationRecorder`
  gained the three real wire fields real AWS Config actually serializes for this case
  (`connectorArn`/`scopeConfiguration`/`servicePrincipal`), so
  `DescribeConfigurationRecorders`/`DescribeConfigurationRecorderStatus`/
  `ListConfigurationRecorders`/`DeleteConfigurationRecorder` all see and can act on it
  through the same `recorders` table every other recorder uses. Two semantics were
  verified against AWS's docs rather than assumed: `PutConnector` is create-only
  (ConflictException on a repeat call with matching configuration, not an upsert) while
  `PutThirdPartyServiceLinkedConfigurationRecorder` is conditionally idempotent (updates
  ScopeConfiguration when the same ServicePrincipal+ConnectorArn repeat, but
  ConflictException when the same ServicePrincipal reuses a different ConnectorArn -- "one
  recorder per service principal" is enforced, unlike the pre-existing, still-unenforced
  single-customer-managed-recorder limit noted in `gaps`).

- 2026-07-24 pass bug-class findings (see `.claude/memories/parity-principles.md` bug
  classes) -- this pass closed all remaining items from the prior audit's `gaps` list
  (`gopherstack-e0f1`, `gopherstack-s7u1`) plus a partial `gopherstack-eboy` fix:
  - **Silently-dropped not-found instead of erroring**: `DescribeConfigRules` and
    `GetComplianceDetailsByConfigRule` used to omit/empty-return for an unknown
    `ConfigRuleName` instead of `NoSuchConfigRuleException`; `DescribeConfigRules`'
    backend signature changed from `([]ConfigRule)` to `([]ConfigRule, error)` (~14
    call sites across this package's tests updated accordingly).
  - **~18 disguised-as-honest empty stubs that were actually derivable**: the prior
    audit (`gopherstack-e0f1`) reasoned these ~18 ops "can't model cross-account state."
    That's true for genuinely cross-account data, but most of these ops' *local* half was
    fully derivable from existing backend state that was simply being ignored:
    conformance-pack rule compliance from `ruleResourceEvals` (once conformance packs
    track which rules they deployed -- a new `conformancePackRules` link table, populated
    by parsing `PutConformancePack`'s `TemplateBody` for `AWS::Config::ConfigRule`
    resources), aggregator source status from the aggregator's own already-stored
    `AccountAggregationSources`/`OrganizationAggregationSource`, aggregate compliance/
    resource-listing ops from local rule-evaluation/resource-config state (mirroring the
    already-"ok" `DescribeAggregateComplianceByConfigRules`/`GetAggregateResourceConfig`
    pattern), pending-aggregation-requests from `AggregationAuthorizations` not yet
    consumed by a local aggregator, remediation execution from `remediationConfigs` (new
    `remediationExecutions` table), and organization detailed-status from treating the
    local account as the org's sole member. Only genuinely un-derivable data (other
    accounts' real resource/compliance state) remains out of scope, and none of these ops
    fabricate it -- they validate real preconditions (aggregator/pack/rule/remediation
    existence) and return real local data.
  - **New cascade-delete surfaces**: two new stateful features this pass introduced
    matching real AWS deletion semantics: `DeleteConformancePack` now cascade-deletes the
    config rules the pack deployed (+ their evaluations), and `DeleteRemediationConfiguration`
    now cascade-deletes recorded remediation executions for the rule -- both to avoid
    introducing new ghost-row classes alongside the new stateful tables.

- Prior-pass bug-class findings (retained for history; see git blame for the fixes):
  wrong wire error-type strings on `ErrNotFound`/`ErrNoSuchOrganizationConformancePack`,
  an error-family collision on `ErrNoDeliveryChannel`, a fabricated
  `NoSuchAggregationAuthorizationException` that doesn't exist in the real SDK, and
  several disguised no-ops (`AssociateResourceTypes`, `BatchGetResourceConfig`,
  `BatchGetAggregateResourceConfig`, `DeleteResourceConfig`, `DisassociateResourceTypes`,
  `DeleteEvaluationResults`) that discarded real backend state instead of acting on it.
