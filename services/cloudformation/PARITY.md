---
service: cloudformation
sdk_module: aws-sdk-go-v2/service/cloudformation@v1.76.1
last_audit_commit: 57873cfd3  # 2026-09-24 25 new resource types added: Glue
                               # Blueprint/CustomEntityType/Workflow (3 types);
                               # DataSync Agent/LocationS3/Task (3 types);
                               # Transfer Profile/Workflow (2 types); AppConfig
                               # Application/Environment/ConfigurationProfile/
                               # DeploymentStrategy (4 types); Macie
                               # AllowList/FindingsFilter (2 types); GuardDuty
                               # Detector/IPSet (2 types); AccessAnalyzer
                               # Analyzer/ArchiveRule (2 types); Amplify
                               # App/Branch (2 types); Batch
                               # SchedulingPolicy/ServiceEnvironment (2 types);
                               # EFS AccessPoint (1 type); Redshift
                               # ClusterParameterGroup/ClusterSubnetGroup (2
                               # types) (380 -> 405 supported types); DataSync,
                               # AppConfig, Macie2, AccessAnalyzer newly wired
                               # into the CloudFormation backend (cli.go
                               # gained datasyncHandler/macie2Handler/
                               # accessanalyzerHandler fields + getters --
                               # FSx was NOT wired: every FSx InMemoryBackend
                               # Create* method takes an unexported input
                               # struct pointer, uncallable from outside the
                               # fsx package, so AWS::FSx::FileSystem/Backup
                               # were skipped rather than stubbed; GuardDuty/
                               # Amplify/AppConfig already had cli.go getters,
                               # just needed BackendsProvider/ServiceBackends
                               # wiring); cfn_attributes_gen.go regenerated
                               # (cmd/cfnattrgen) -- most new attribute names
                               # (Id, Status, ApplicationId, AppId, ...) were
                               # excluded by its goconst-safety rule (already
                               # common literals elsewhere in the package),
                               # which is documented conservative behavior,
                               # not a regression: an excluded attribute falls
                               # back to pre-existing permissive resolution
                               # rather than being wrongly rejected;
                               # prior: 43e38e92b  # 2026-09-24 24 new resource types added: Lambda
                               # CodeSigningConfig; Events Endpoint; Scheduler
                               # ScheduleGroup; AppSync DomainName/GraphQLSchema/
                               # ChannelNamespace (3 types); Route53Resolver
                               # ResolverRuleAssociation/FirewallDomainList/
                               # FirewallRuleGroup/FirewallRuleGroupAssociation/
                               # ResolverQueryLoggingConfig/
                               # ResolverQueryLoggingConfigAssociation/
                               # OutpostResolver (7 types); CloudTrail
                               # EventDataStore/Channel (2 types); Logs
                               # Delivery/DeliveryDestination/DeliverySource/
                               # Integration/LogAnomalyDetector/ScheduledQuery
                               # (6 types); CodeArtifact Domain/Repository/
                               # PackageGroup (3 types, new family -- CodeArtifact
                               # newly wired into the CloudFormation backend,
                               # CodeArtifact field added to ServiceBackends;
                               # GetCodeArtifactHandler already existed on
                               # BackendsProvider/cli.go, so no cli.go change
                               # was needed) (356 -> 380 supported types);
                               # prior: 390f9687e  # 2026-09-24 20 new resource types added: SageMaker Model/
                               # EndpointConfig/Endpoint/NotebookInstance/
                               # NotebookInstanceLifecycleConfig/CodeRepository/Domain/
                               # Pipeline/ModelPackageGroup/FeatureGroup/Project/Workteam/
                               # Image/ImageVersion (14 types); Athena WorkGroup/DataCatalog/
                               # NamedQuery/PreparedStatement/CapacityReservation (5 types);
                               # WAFv2 WebACLAssociation (1 type) (319 -> 339 supported
                               # types); SageMaker and Athena newly wired into the
                               # CloudFormation backend (GetSageMakerHandler/GetAthenaHandler
                               # added to BackendsProvider/ServiceBackends -- cli.go already
                               # implemented both methods for the generic backend registry,
                               # so no cli.go change was needed); prior: 0af53d7f0  # 2026-09-24 28 new resource types added: IAM GroupPolicy/
                               # RolePolicy/UserPolicy/ServerCertificate, ECR
                               # PullThroughCacheRule/RegistryPolicy/RepositoryCreationTemplate,
                               # ElastiCache ParameterGroup/SecurityGroup/GlobalReplicationGroup/
                               # UserGroup, Neptune DBSubnetGroup/DBClusterParameterGroup/
                               # DBParameterGroup/GlobalCluster, DocDB DBSubnetGroup/
                               # DBClusterParameterGroup/GlobalCluster, Backup Framework/
                               # ReportPlan, Glue Classifier/Registry/Schema/
                               # SecurityConfiguration/DevEndpoint, CodeBuild ReportGroup,
                               # Kinesis ResourcePolicy, Lambda ResourcePolicy (291 -> 319
                               # supported types); prior: 1ba59adde  # 2026-09-24 25 new resource types added: IoT ThingType/
                               # ThingGroup/Policy/TopicRuleDestination/RoleAlias/Certificate/
                               # ProvisioningTemplate/Authorizer/DomainConfiguration/
                               # JobTemplate/Dimension/SecurityProfile/CustomMetric/
                               # FleetMetric/BillingGroup/MitigationAction/ScheduledAudit
                               # (17 types); AWS::Config newly wired into the CloudFormation
                               # backend (ConfigRule/ConfigurationRecorder/DeliveryChannel/
                               # ConfigurationAggregator/AggregationAuthorization/
                               # ConformancePack/RemediationConfiguration/StoredQuery, 8
                               # types) (266 -> 291 supported types); prior: e74172daa  # 2026-09-24 25 new resource types added: RDS OptionGroup/
                               # EventSubscription/GlobalCluster/DBProxyEndpoint, Cognito
                               # UserPoolResourceServer/UserPoolIdentityProvider, SSM
                               # MaintenanceWindowTarget/MaintenanceWindowTask/PatchBaseline/
                               # ResourceDataSync/ResourcePolicy, CloudWatch MetricStream/
                               # AnomalyDetector/InsightRule, ApiGateway VpcLink/
                               # ClientCertificate/DocumentationPart/DocumentationVersion,
                               # CloudFront OriginRequestPolicy/KeyGroup/PublicKey/
                               # CloudFrontOriginAccessIdentity/RealtimeLogConfig/
                               # KeyValueStore/ContinuousDeploymentPolicy (241 -> 266
                               # supported types); prior: 6279d75f6  # 2026-09-24 13 new
                               # resource types added (EKS FargateProfile/
                               # Addon/AccessEntry/PodIdentityAssociation/IdentityProviderConfig,
                               # StepFunctions StateMachineVersion/StateMachineAlias, Kinesis
                               # StreamConsumer, the real AWS::KinesisFirehose::DeliveryStream type
                               # name, ECS CapacityProvider/ClusterCapacityProviderAssociations/
                               # TaskSet/PrimaryTaskSet); prior: 05eeb3af7
last_audit_date: 2026-09-24  # prior: 2026-09-24 (28-type IAM/ECR/ElastiCache/Neptune/DocDB/Backup/Glue/CodeBuild/Kinesis/Lambda pass earlier same day)
overall: A            # This pass closed out all 4 documented gaps and independently re-verified/acted
                       # on all 6 documented deferred items (see gaps:/deferred: below for exact
                       # disposition of each -- some fixed, some reclassified to ok after
                       # re-verification, two genuinely still deferred with reasons). It also
                       # field-diffed the previously-unaudited Type Registry family (16 ops, none of
                       # which appeared in this doc's ops: table before) and found + fixed real bugs
                       # there too. Independently of the named gaps/deferred list, this pass found and
                       # fixed two significant previously-undocumented parity bugs: (1) 10 backend map
                       # fields (StackSet instances/operations, type configs/versions, drift detail,
                       # resource-scan items, custom-resource signals, hook progress) were NEVER wired
                       # into Snapshot/Restore at all -- silent data loss across every restart/restore
                       # for StackSets, type registry, drift detection, resource scans, and signaling;
                       # (2) CreateChangeSet never accepted or stored a Capabilities parameter, so
                       # ExecuteChangeSet always called UpdateStack/CreateStack with an empty
                       # StackOptions -- meaning ANY change set touching IAM resources could never
                       # actually be executed regardless of what capabilities the caller declared at
                       # CreateChangeSet time. Prior audit text retained below for history:
                       #
                       # local surface unchanged since prior sweep (ce30166a..HEAD diff is empty for
                       # this service); this pass spot-audited 4 previously fully-deferred families
                       # (StackSets, Stack Refactor, Generated Templates, Resource Scans) and found +
                       # fixed 4 genuine bugs: DeleteStackSet idempotency, DescribeStackRefactor
                       # not-found handling, and an unsuffixed-wire-code repeat of the ChangeSetNotFound
                       # bug class across Generated Templates + Resource Scans (plus two disguised-stub
                       # List* handlers that discarded not-found errors). Remaining deferred families
                       # (Type registry, YAML short-form intrinsics) still not re-proven op-by-op.
ops:
  CreateStack: {wire: ok, errors: ok, state: ok, persist: ok, note: "CAPABILITY_AUTO_EXPAND no longer wrongly satisfies the IAM-resource capability check (backend_parity.go requireIAMCapability); this pass ALSO fixed the inverse gap -- top-level Transform is now parsed (Template.Transform) and requireAutoExpandCapability gates CAPABILITY_AUTO_EXPAND for macro/SAM-using templates, which was previously never enforced at all"}
  UpdateStack: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: missing UPDATE_FAILED stack event on template parse failure; added pre-flight export-in-use block (validateExportsStillInUse); same CAPABILITY_AUTO_EXPAND gate as CreateStack added this pass. gopherstack-cqy3 (THIS PASS): the stored stack policy was never enforced here at all -- SetStackPolicy stored a policy, GetStackPolicy echoed it back, and nothing in between ever read it, so a Deny on Update:Delete/Update:Replace/Update:Modify did nothing. Now checkStackPolicy (stack_policy.go) evaluates every resource change computeChanges would apply (the same diff CreateChangeSet already computes) against the stack's policy before any state mutation; a denied action fails the whole call atomically. Also now accepts StackPolicyDuringUpdateBody (UpdateStackInput field, api_op_UpdateStack.go:223) as a one-shot override that is never persisted. See gaps: for what this does not cover (NotAction/NotResource, parameter-only diffing) and the families: stack_policy_enforcement entry for the evaluation semantics and their sourcing."}
  DeleteStack: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now idempotent (no-op, not ErrStackNotFound) per AWS's unmodeled DeleteStack error surface; added export-in-use block (stackExportsInUse). gopherstack-cqy3 sweep: independently re-verified UpdateTerminationProtection IS enforced here (stack.EnableTerminationProtection gate, stacks.go deleteStackLocked) -- this service was NOT one of the five found with settable-and-unenforced termination protection; no change needed. FIXED 2026-09-24: a child resource already deleted directly through its own service (observed for APIGatewayV2 Integration/Route/Stage) 404'd deleteStackLocked's b.creator.Delete call and left the stack DELETE_FAILED, unlike real CloudFormation which treats a gone resource as already deleted. deleteAPIGatewayV2Integration/Route/Stage now swallow their service's NotFoundException sentinels (ErrIntegrationNotFound/ErrRouteNotFound/ErrStageNotFound/ErrAPINotFound) as success, matching the existing deleteDynamoDBGlobalTable idempotent-delete convention. Other resource types' delete paths were not audited this pass. FIXED 2026-09-24: deleteStackLocked previously deleted resources in unordered map-iteration order; now uses reverseDependencyOrder (resource_deps.go) so a resource is always deleted before whatever it depends on, matching real AWS (see families: resource_provisioning)."}
  DescribeStacks: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateTerminationProtection: {wire: ok, errors: ok, state: ok, persist: ok, note: "gopherstack-cqy3 sweep: verified enforced (see DeleteStack note) -- was missing an ops: table entry despite being routed and correct, now documented"}
  SetStackPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "gopherstack-cqy3: was missing an ops: table entry. Now validates the policy body is well-formed JSON (parseStackPolicyDocument) at set time and rejects malformed input, rather than accepting garbage that would have silently never enforced anything at UpdateStack time. StackPolicyURL is not modeled (this backend has never fetched policies by URL for either Set or Get)"}
  GetStackPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  ListStacks: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeStackEvents: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: was returning the full unpaginated event history every call, ignoring NextToken entirely; now uses pkgs/page like the other List* ops"}
  GetTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeStackResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListStackResources: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeStackResources: {wire: ok, errors: ok, state: ok, persist: ok}
  ListExports: {wire: ok, errors: ok, state: ok, persist: ok}
  ListImports: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateChangeSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now accepts and stores a Capabilities parameter (Capabilities.member.N form field, parseCapabilities) -- previously silently dropped, meaning capabilities declared at CreateChangeSet time were never usable at Execute time (see ExecuteChangeSet note); DescribeChangeSet's response now surfaces Capabilities too, matching the real DescribeChangeSetResult shape. 2026-09-18: same fix applied to ResourceTypes and DisableValidation (api_op_CreateChangeSet.go:254,192) -- also read nowhere and dropped, so a change set's own resource-type allowlist/validation-skip had no effect once executed. Both are now stored on the ChangeSet and threaded into ExecuteChangeSet's internal CreateStack/UpdateStack call, same as Capabilities. IncludeNestedStacks remains unmodeled (see items_still_open). Verified via TestRealClient_StackOptions/create_change_set_resource_types_threaded_to_execute"}
  DescribeChangeSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed error code ChangeSetNotFoundException -> ChangeSetNotFound (SDK deserializer matches the un-suffixed code; see errors.go ChangeSetNotFoundException.ErrorCode()); this pass added the missing Capabilities field to the response"}
  ExecuteChangeSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: no longer executes a FAILED/UNAVAILABLE change set (added InvalidChangeSetStatus gate); on success now clears every other change set for the stack, matching documented AWS behaviour, not just the executed one; fixed ChangeSetNotFound code. THIS PASS fixed a significant additional bug: ExecuteChangeSet always called UpdateStack/CreateStack with an empty StackOptions{} (zero capabilities), because CreateChangeSet never stored Capabilities in the first place -- meaning ANY change set touching IAM resources could never actually be executed regardless of what capabilities the caller declared at CreateChangeSet time. Verified via TestChangeSet_Capabilities_ThreadedToExecute (execute now succeeds with CAPABILITY_IAM, fails with InsufficientCapabilities without it)"}
  DeleteChangeSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed ChangeSetNotFound code"}
  ListChangeSets: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeType: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateStackSet: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateStackSet: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteStackSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: now idempotent (no-op, not StackSetNotFoundException) — SDK's DeleteStackSet error deserializer models only {OperationInProgressException, StackSetNotEmptyException}, no not-found case, mirroring the already-fixed DeleteStack precedent"}
  DescribeStackSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass (was the #1 named gap): full field set now returned, field-diffed against awsAwsquery_deserializeDocumentStackSet -- Parameters, Capabilities, Tags, StackSetARN, AdministrationRoleARN, ExecutionRoleName, PermissionModel, OrganizationalUnitIds, AutoDeployment{Enabled,RetainStacksOnAccountRemoval}, ManagedExecution{Active}. CreateStackSet/UpdateStackSet now accept these via a new StackSetOptions struct (signature change, all callers updated). Regions is intentionally NOT stored on StackSet -- it's computed live from stack instances each call (StackSetRegions) to avoid a second source of truth, mirroring the driftByStackID rationale below. Verified via TestStackSet_DescribeFieldCompleteness"}
  ListStackSets: {wire: ok, errors: ok, state: ok, persist: ok, note: "this pass (constraint-parameter audit): fixed -- Status (cloudformation@v1.76.1 api_op_ListStackSets.go:75-76) was read nowhere, so a real client's Status=DELETED filter silently fell back to returning every StackSet instead of the empty list real AWS would return (DeleteStackSet hard-deletes its row, so no DELETED-status StackSet can ever exist in this backend -- an unfiltered call and a Status=ACTIVE-filtered call are behaviorally identical; only Status=DELETED was actually wrong). Now applies the filter (exact match against StackSetSummary.Status)."}
  CreateStackInstances: {wire: ok, errors: ok, state: ok, persist: ok, note: "real per-account/region child stacks are provisioned (provisionStackInstance), not just recorded rows — verified correct. gopherstack-g7b5: now also accepts DeploymentTargets.OrganizationalUnitIds.member.N (serializers.go's DeploymentTargets/OrganizationalUnitIdList encoders) and resolves each OU to its real member accounts via a wired Organizations backend (services/cloudformation/organizations_directory.go's OrganizationsDirectory interface, satisfied by organizations.InMemoryBackend.ResolveAccountIDsUnderParent, wired in cli.go's wireCloudFormationOrganizations). Requires PermissionModel=SERVICE_MANAGED and ActivateOrganizationsAccess; errors clearly otherwise rather than silently expanding to zero accounts. gopherstack-nirx: DeploymentTargets.AccountFilterType was documented as rejected but the field was never read by the handler (silently dropped, computing a union of Accounts and OU-resolved accounts regardless of the requested filter) — now handler_stack_sets.go's unsupportedAccountFilterType actually rejects INTERSECTION/DIFFERENCE/UNION with ValidationError; only unset/NONE (the union case) is honoured. See TestStackInstances_AccountFilterType"}
  DeleteStackInstances: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "tears down provisioned child stacks via deleteStackLocked — verified correct. gopherstack-g7b5: also accepts DeploymentTargets.OrganizationalUnitIds, same resolution path as CreateStackInstances. CORRECTION 2026-09-11 (required-member sweep pass 4a): 'verified correct' missed that RetainStacks (required, api_op_DeleteStackInstances.go) was never read at all -- the handler always tore down the child stack via deleteStackLocked regardless of what the caller asked. Fixed: RetainStacks is now required and presence-validated; when true, deleteMatchingStackInstances drops only the stack-instance association and leaves the child stack alive. See TestDeleteStackInstances_RetainStacksKeepsChildStack."}
  UpdateStackInstances: {wire: ok, errors: ok, state: ok, persist: ok, note: "gopherstack-g7b5: also accepts DeploymentTargets.OrganizationalUnitIds"}
  ListStackInstances: {wire: ok, errors: ok, state: ok, persist: ok, note: "this pass (constraint-parameter audit): fixed -- handleListStackInstances read only StackSetName/NextToken; StackInstanceAccount, StackInstanceRegion, and Filters (cloudformation@v1.76.1 api_op_ListStackInstances.go) were parsed nowhere, so every call returned every instance in the StackSet regardless of the filter sent. Now applies StackInstanceAccount/StackInstanceRegion (exact match) and Filters entries named DRIFT_STATUS/LAST_OPERATION_ID (matched against StackInstance.DriftStatus/LastOperationID). DETAILED_STATUS is accepted on the wire but left unenforced and documented as a gap: this backend tracks no field distinct from Status, and DetailedStatus's real values (PENDING/RUNNING/SUCCEEDED/FAILED/CANCELLED/INOPERABLE/SKIPPED_SUSPENDED_ACCOUNT) don't correspond to StackInstanceStatus's (CURRENT/OUTDATED/INOPERABLE) closely enough to map one onto the other without fabricating data."}
  DescribeStackInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  DetectStackDrift: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-08-22 (gopherstack-r80d batch 26, NEW ops: row -- had no prior entry): required output StackDriftDetectionId always a real uuid, field-diffed against DetectStackDriftOutput; 0 bugs"}
  DetectStackResourceDrift: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-08-22 (gopherstack-r80d batch 26, NEW ops: row): required output StackResourceDrift wraps types.StackResourceDrift (5 required members one level deeper than the flat op scan: LogicalResourceId/ResourceType/StackId/StackResourceDriftStatus/Timestamp) -- confirmed all 5 always populated on both the normal (compareStackResources) and template-parse-failure fallback path (driftDetailFor); driftXML's required fields carry no xml omitempty tag. 0 bugs"}
  DescribeStackDriftDetectionStatus: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-08-22 (gopherstack-r80d batch 26, NEW ops: row): required DetectionStatus/StackDriftDetectionId/StackId/Timestamp all always populated (set at DetectStackDrift/SimulateDrift time), driftResult XML struct carries no omitempty on any of the four. 0 bugs"}
  DescribeStackResourceDrifts: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-08-22 (gopherstack-r80d batch 26, NEW ops: row): required StackResourceDrifts wraps the same types.StackResourceDrift as DetectStackResourceDrift (5 required members per element); toDriftXML's shared helper confirmed to always populate all 5 for every element on both the detailMap-hit and status-only fallback path. 0 bugs"}
  DetectStackSetDrift: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: was a disguised stub -- recorded a real SUCCEEDED operation but never actually ran per-instance drift comparison, so every stack instance's DriftStatus stayed NOT_CHECKED forever. Now runs the same compareStackResources logic DetectStackDrift uses against each instance's provisioned child stack and updates its DriftStatus (IN_SYNC/DRIFTED) in place. Verified via TestStackSetDrift_UpdatesInstanceDriftStatus (mutates a child-stack resource out of band, confirms DriftStatus flips to DRIFTED on re-detection)"}
  ListStackSetOperations: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeStackSetOperation: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: now returns StackSetNotFoundException when the StackSetName itself doesn't exist (SDK models this as a distinct case from OperationNotFoundException, which is now reserved for a known StackSet with an unknown OperationId). Verified via TestDescribeStackSetOperation_NotFoundErrorCodes"}
  StopStackSetOperation: {wire: ok, errors: ok, state: ok, persist: ok}
  ListStackSetOperationResults: {wire: ok, errors: ok, state: ok, persist: ok}
  ListStackSetAutoDeploymentTargets: {wire: ok, errors: ok, state: ok, persist: ok, note: "gopherstack-g7b5: now groups by the real OrganizationalUnitId recorded on each SERVICE_MANAGED stack instance (see CreateStackInstances note) instead of always synthesizing one placeholder target per account; self-managed instances (no OU) still fall back to the per-account placeholder, matching real AWS semantics"}
  ImportStacksToStackSet: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateStackRefactor: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "SDK models zero errors for this op (fire-and-forget) — verified via deserializers.go, no changes needed. 2026-08-22 (gopherstack-r80d batch 26): required output StackRefactorId re-confirmed always a real uuid.New().String() value, never empty; 0 bugs. CORRECTION 2026-09-11 (required-member sweep pass 4a): StackDefinitions ([]types.StackDefinition, required) was never parsed by anything -- so EnableStackCreation had nothing to create a missing destination stack from, and ExecuteStackRefactor unconditionally required every mapping destination to already exist. Fixed: handler now parses StackDefinitions.member.N.{StackName,TemplateBody,TemplateURL}; ExecuteStackRefactor's new createMissingRefactorStacks creates a missing destination from its matching StackDefinition's TemplateBody when EnableStackCreation is set (still errors ErrStackNotFound otherwise, matching AWS). TemplateURL is parsed but not fetched, consistent with this service's existing CreateStack/UpdateStack (TemplateBody-only). See TestExecuteStackRefactor_StackDefinitions."}
  DescribeStackRefactor: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: unknown StackRefactorId previously returned 200 with an empty Status instead of StackRefactorNotFoundException, the one error this op does model (unlike its Create/Execute/List siblings, which are genuinely fire-and-forget per the SDK model)"}
  ExecuteStackRefactor: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass (gopherstack-g7b5): was a pure status-flip (`r.Status = \"EXECUTE_COMPLETE\"`) that never moved anything, a disguised no-op. CreateStackRefactor now parses ResourceMappings.member.N.{Source,Destination}.{StackName,LogicalResourceId} (verified against serializers.go's ResourceMapping/ResourceLocation encoders); Execute now validates every mapping (source/dest stack + source resource must exist) before mutating, then moves each StackResource entry between b.resources[stackID] maps under the destination's logical ID. Unknown refactor ID or a missing source resource now errors (StackRefactorNotFoundException / ValidationError) instead of silently no-oping. Verified via TestExecuteStackRefactor_MovesResourceBetweenStacks (reads both stacks back via DescribeStackResources)"}
  ListStackRefactors: {wire: ok, errors: ok, state: ok, persist: ok, note: "SDK models zero errors for this op. 2026-08-22 (gopherstack-r80d batch 26): required output StackRefactorSummaries's element type (types.StackRefactorSummary) confirmed to declare ZERO required members in the real SDK model (AST walk of types.go) -- the flat op-level required field is exactly the wrapper array itself, no undercount; array always non-nil via make(...). 0 bugs"}
  ListStackRefactorActions: {wire: ok, errors: ok, state: ok, persist: ok, note: "now derives real MOVE actions from the stored ResourceMappings (previously always empty since CreateStackRefactor never parsed StackDefinitions/mappings at all). 2026-08-22 (gopherstack-r80d batch 26): same as ListStackRefactors -- StackRefactorActions's element type (types.StackRefactorAction) also declares zero required members; array always non-nil. 0 bugs"}
  CreateGeneratedTemplate: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateGeneratedTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed wire code: GeneratedTemplateNotFoundException -> GeneratedTemplateNotFound (SDK's ErrorCode() is unsuffixed, same bug class as the earlier ChangeSetNotFound fix)"}
  DeleteGeneratedTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed wire code: GeneratedTemplateNotFoundException -> GeneratedTemplateNotFound"}
  DescribeGeneratedTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed wire code: GeneratedTemplateNotFoundException -> GeneratedTemplateNotFound"}
  GetGeneratedTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed wire code: GeneratedTemplateNotFoundException -> GeneratedTemplateNotFound"}
  ListGeneratedTemplates: {wire: ok, errors: ok, state: ok, persist: ok}
  StartResourceScan: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeResourceScan: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed wire code: ResourceScanNotFoundException -> ResourceScanNotFound (SDK's ErrorCode() is unsuffixed)"}
  ListResourceScans: {wire: ok, errors: ok, state: ok, persist: ok}
  ListResourceScanResources: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: was silently discarding the not-found error (`_`) and returning 200 with an empty list for an unknown ResourceScanId; SDK models ResourceScanNotFound for this op. Now surfaces it with the correct unsuffixed code"}
  ListResourceScanRelatedResources: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: same disguised-stub pattern as ListResourceScanResources — was discarding the not-found error; now surfaces ResourceScanNotFound"}
  ActivateType: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED (gopherstack-vc2g): handler read form key TypeArn, which ActivateTypeInput does not have -- the real ARN identifier is PublicTypeArn (serializers.go:7181). A caller identifying the type by ARN had the value silently dropped. The prior 'wire: ok, field-diffed' claim only checked the modeled error switch, not the request field names."}
  DeactivateType: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED (gopherstack-vc2g): handler read form key TypeArn; DeactivateTypeInput sends Arn (serializers.go:7751). Same silent-drop bug as ActivateType. The prior 'wire: ok, field-diffed' claim only checked the modeled error switch, not the request field names."}
  RegisterType: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass, field-diffed (SDK models only CFNRegistryException for this op)"}
  DeregisterType: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: handler discarded Backend.DeregisterType's returned error entirely (`_ = h.Backend.DeregisterType(...)`), so an unknown Arn silently returned 200 instead of the TypeNotFoundException the SDK models for this op -- a disguised stub matching the same bug class as the earlier ListResourceScanResources fix. A stale test (TestTypeRegistry_DeregisterNotFound) literally had a comment noting this ('handler currently ignores DeregisterType error'); now asserts the real 400/TypeNotFoundException"}
  PublishType: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass, field-diffed"}
  SetTypeDefaultVersion: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass: same disguised-stub pattern as DeregisterType -- backend never returned an error for an unknown Arn (silent no-op) and the handler discarded the return value too, even though the SDK models TypeNotFoundException for this op. Backend now returns ErrTypeNotFound; handler propagates it. Verified via TestTypeRegistry_SetTypeDefaultVersionNotFound"}
  SetTypeConfiguration: {wire: ok, errors: partial, state: ok, persist: ok, note: "SDK models TypeNotFoundException for this op, but the backend intentionally accepts configuration for ANY type name without requiring prior RegisterType/ActivateType -- this matches real-world usage where first-party AWS types (e.g. AWS::S3::Bucket) can have extension configuration set without ever being explicitly registered in this emulator's type registry (which only models the RegisterType/ActivateType *custom-extension* flow, not the full built-in-type catalog). Left permissive rather than force a wrong not-found on a legitimate first-party-type call; not re-classified as a bug (bd: gopherstack-e5h)"}
  BatchDescribeTypeConfigurations: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this pass (gopherstack-g7b5): the request's TypeConfigurationIdentifiers.member.N was a list of STRUCTS (Type/TypeArn/TypeConfigurationAlias/TypeConfigurationArn/TypeName -- serializers.go:7114/7085), not scalars as the old parseMemberList call assumed, so no identifier was ever actually parsed. Now parses the real struct shape and populates Errors (TypeNotFoundException per identifier with no matching type/config, api_op_BatchDescribeTypeConfigurations.go:47) and UnprocessedTypeConfigurations (identifiers with no TypeName/TypeConfigurationArn to resolve by, :55) for real, instead of leaving them always empty. Verified via TestHandler_BatchDescribeTypeConfigurations"}
  TestType: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass, field-diffed (SDK models only CFNRegistryException)"}
  ListTypes: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass, field-diffed (SDK models only CFNRegistryException)"}
  ListTypeVersions: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass, field-diffed"}
  ListTypeRegistrations: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass, field-diffed"}
  DescribeTypeRegistration: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass, field-diffed"}
  RegisterPublisher: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass, field-diffed"}
  DescribePublisher: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW this pass, field-diffed"}
families:
  resource_provisioning: {status: ok, note: "topoSortResources (resource_deps.go, Kahn's algorithm, deterministic alphabetical tie-break) + provisionResources/rollbackCreateResources (reverse-order rollback, DeletionPolicy Retain/Snapshot honored) verified correct. FIXED 2026-09-24: topoSortResources previously ordered only by explicit DependsOn; now also infers dependencies from Ref/Fn::GetAtt (array and dotted-string form)/Fn::Sub (${Id}/${Id.Attr}, skipping ${!Literal} escapes and Sub's own local variables) anywhere in a resource's Properties, recursively through Fn::If/Fn::Join/Fn::Select/etc, matching real CloudFormation. A dependency cycle (via DependsOn or an inferred reference) now fails with ErrCircularDependency (\"Circular dependency between resources: [...]\") instead of silently falling back to alphabetical order."}
  update_reconciliation: {status: ok, note: "updateResources/rollbackUpdateResources snapshot-and-restore semantics verified correct; deleteStaleResources runs only after all creates/updates succeed, matching AWS ordering. FIXED 2026-09-24: both updateResources' new-resource creation and deleteStaleResources' stale-resource cleanup now use the same inferred-dependency graph (creation in topo order, deletion in its reverse) instead of unordered map iteration / plain alphabetical order."}
  exports_imports: {status: ok, note: "ADDED: delete-blocked-while-imported and update-blocked-while-imported (Export X cannot be deleted as it is in use by Y), the one concretely-named gap from the audit brief that was completely unimplemented before this pass"}
  error_code_mapping: {status: ok, note: "verified against aws-sdk-go-v2 deserializers.go: StackNotFoundException is modeled for exactly one op (ImportStacksToStackSet) — every other stack-lookup op correctly falls back to generic ValidationError, matching real AWS's query-protocol behaviour; this was already correct, not changed"}
  drift_detection: {status: ok, note: "DetectStackDrift / DescribeStackResourceDrifts / legacy SimulateDrift fallback reviewed, logic is internally consistent; NOT re-verified against AWS's real per-property drift diff algorithm this pass (see deferred). 2026-08-22 (gopherstack-r80d batch 26): swept all 4 drift ops (DetectStackDrift, DetectStackResourceDrift, DescribeStackDriftDetectionStatus, DescribeStackResourceDrifts) for required-output-member completeness specifically -- these had no ops: table rows at all before this pass, now added above. 0 bugs; every required member (including types.StackResourceDrift's own 5 required members, one level below the flat per-op scan) confirmed always populated with no omitempty gaps."}
  nested_stacks: {status: ok, note: "CreateNestedStack/DeleteNestedStack correctly reuse createStackLocked/deleteStackLocked under the parent's already-held lock (no double-lock/deadlock); ParentID wiring reviewed, looks correct"}
  stacksets: {status: ok, note: "spot-audited previously (all 17 StackSet/instance/operation ops cross-checked against deserializers.go's modeled error switch per op; DeleteStackSet idempotency fixed then). Prior pass fixed DetectStackSetDrift (was a disguised stub), DescribeStackSetOperation's error-code gap, and closed the DescribeStackSet field-completeness gap. Then (gopherstack-g7b5): services/organizations DOES have a real, queryable OU hierarchy (CreateOrganizationalUnit/ListAccountsForParent/ListOrganizationalUnitsForParent), so the SERVICE_MANAGED/OU-based deployment-target gap was honestly implementable and has been closed — see CreateStackInstances/DeleteStackInstances/UpdateStackInstances/ListStackSetAutoDeploymentTargets notes above. THIS PASS (gopherstack-nirx): AccountFilterType and AccountsUrl remain unimplemented (real edge cases) but AccountFilterType is now actually rejected when set to anything but NONE — the g7b5 pass's claim of explicit rejection was documentation-only, the field was never read by the handler and was silently dropped — see gaps."}
  stack_refactor: {status: ok, note: "spot-audited previously (all 5 ops cross-checked against deserializers.go; DescribeStackRefactor not-found handling fixed then). THIS PASS (gopherstack-g7b5): ExecuteStackRefactor now performs a real resource move — see its ops: note above. ListStackRefactorActions derives real MOVE actions from the stored mappings."}
  generated_templates: {status: ok, note: "spot-audited previously (all 6 ops cross-checked against deserializers.go; unsuffixed-wire-code bug class fixed then, same as ChangeSetNotFound). THIS PASS: independently re-verified all 4 not-found-capable ops still emit the correct unsuffixed GeneratedTemplateNotFound code (handler_generated_templates.go) -- confirms the family: ok classification from the prior pass; the deferred: bullet claiming this family was 'not audited' was stale leftover documentation from BEFORE that prior spot-audit ran and is removed this pass."}
  resource_scans: {status: ok, note: "spot-audited previously (all 5 ops cross-checked against deserializers.go; unsuffixed-wire-code + two disguised-stub List* bugs fixed then). THIS PASS: independently re-verified the fixed error codes are still correct (ResourceScanNotFound, generated_templates.go/handler_generated_templates.go) -- confirms family: ok; same stale deferred: bullet issue as generated_templates, removed."}
  type_registry: {status: ok, note: "NEW this pass: this family (16 ops: DescribeType plus 15 RegisterType/ActivateType/... management ops) had NO ops: table entries at all before this pass despite being fully routed and non-stub -- the deferred: bullet 'not audited this pass' was accurate for every prior pass. Field-diffed all 16 against deserializers.go's per-op modeled error switches. Found + fixed two disguised-stub bugs (DeregisterType, SetTypeDefaultVersion — see ops: above). SetTypeConfiguration/TestType/BatchDescribeTypeConfigurations/RegisterPublisher's non-error-returning backend methods were reviewed and left as-is with reasoning recorded per-op above (SetTypeConfiguration's permissiveness is intentional; BatchDescribeTypeConfigurations' missing Errors/UnprocessedTypeConfigurations fields is a real but low-value gap)."}
  yaml_short_form_intrinsics: {status: ok, note: "NEW this pass: previously deferred as 'not re-verified'. Independent verification found it was actually BROKEN, not merely unverified -- ParseTemplate/parseGenericTemplate called gopkg.in/yaml.v3's Unmarshal directly into typed structs / map[string]any, which silently discards any custom YAML tag and decodes only the tagged node's native scalar/seq/map content. `!Ref MyParam` decoded to the bare string \"MyParam\" instead of the long-form {\"Ref\": \"MyParam\"} every resolveValue-style consumer expects -- every YAML short-form intrinsic (!Ref, !GetAtt, !Sub, !Join, !Select, !Split, !Base64, !Cidr, !ImportValue, !GetAZs, !FindInMap, !And, !Or, !Not, !Equals, !If, !Condition, !Transform) silently degraded to a dead literal string rather than resolving or erroring. Fixed via a new yamlToJSON/normalizeYAMLNode pass that walks the raw *yaml.Node tree (preserving tag info) before the JSON round-trip. Verified via TestParseTemplate_YAMLShortFormIntrinsics (shape-level) and TestCreateStack_YAMLShortFormIntrinsics_Resolve (end-to-end: !Ref/!Sub actually resolve through CreateStack/DescribeStacks Outputs)."}
  stack_policy_enforcement: {status: ok, note: "FIXED this pass (gopherstack-cqy3): UpdateStack never consulted b.stackPolicies at all -- SetStackPolicy wrote, GetStackPolicy echoed, nothing in between read. A Deny on Update:Delete/Update:Replace protecting a resource did nothing; the write succeeded and the protection was cosmetic. Fixed via stack_policy_eval.go (new): parses the policy as Statement[].{Effect,Action,Resource,Condition}, evaluated per resource change UpdateStack computes via the SAME diffTemplates/computeChanges CreateChangeSet already uses (Add/Modify/Remove + a Replacement classification from requiresRecreation) -- confirms the backend CAN determine per-resource update actions today, it just wasn't asked to. checkStackPolicy (stack_policy.go) runs before any stack mutation, so a denied update fails the whole UpdateStack call atomically rather than partially transitioning state. Implemented: Effect Allow/Deny (Deny overrides Allow), Action Update:Modify/Update:Replace/Update:Delete/Update:* with '*' wildcards, Resource LogicalResourceId/<id> with '*' wildcards, Condition StringEquals/StringLike on ResourceType, default-deny-once-a-policy-exists (an update is denied unless some statement explicitly allows it), StackPolicyDuringUpdateBody as a non-persisted one-call override. Disclosed as NOT implemented, not approximated: NotAction/NotResource -- AWS's own docs describe their evaluation as a two-axis (logical-ID-space and resource-type-space evaluated independently, denied only if both axes deny) model distinct from ordinary statement matching, and explicitly recommend against relying on them; statements using them are parsed but never match. Evaluation semantics (Effect/Action/Resource/Condition, default-deny, Deny-overrides-Allow, the NotAction/NotResource two-axis quirk) are TRANSCRIBED FROM AWS'S DOCUMENTATION (https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/protect-stack-resources.html), not the SDK -- the policy body is an opaque string with no wire type in aws-sdk-go-v2, so there is no types/types.go line to cite for it, same disclosure shape as dynamodb's mutual-exclusion messages. StackPolicyDuringUpdateBody's field name/position IS SDK-cited (UpdateStackInput, api_op_UpdateStack.go:223). Verified via TestUpdateStack_StackPolicyEnforcement, driven through the real aws-sdk-go-v2 client: denies block the specific action and leave the resource/template provably unchanged, a permitted action under the same policy still succeeds, default-deny protects a resource no statement names, no-policy-set allows everything, and the override neither leaks into nor is missing from the persisted policy. Hand-reverted the enforcement call and confirmed 5 of the 8 subtests fail (the other 3 are policy-absent/permitted-path/malformed-input assertions that hold regardless of enforcement, by design)."}
  timestamps: {status: ok, note: "Pattern-hunt pass (timestamp encoding class, 2026-08-29): protocol confirmed Query/XML (awsAwsquery_* serializer prefix, cloudformation@v1.76.1) and every *time.Time deserializer call in deserializers.go is smithytime.ParseDateTime, never ParseEpochSeconds -- no per-field trait override anywhere in this SDK. Checked 44 *time.Time occurrences across types/types.go + api_op_*.go (35 in types.go, 9 more Output-only members: DescribeResourceScan.Start/EndTime, GetHookResult.InvokedAt, DescribeChangeSet.CreationTime, DescribeGeneratedTemplate.Creation/LastUpdatedTime, DescribeStackDriftDetectionStatus.Timestamp, DescribeType.LastUpdated/TimeCreated). Every field gopherstack actually emits goes through one of two paths, both verified compatible with ParseDateTime (which tries time.RFC3339Nano and time.RFC3339 among its formats): (1) models.go structs tagged xml:\"Field\" on a plain time.Time -- encoding/xml invokes time.Time.MarshalText (RFC3339Nano), confirmed by a throwaway xml.Marshal repro; (2) handler-local response structs that manually format via .UTC().Format(\"2006-01-02T15:04:05Z\") (handler_stacks.go, handler_stack_resources.go, handler_change_sets.go, handler_drift_detection.go) -- fits time.RFC3339 exactly. 0 wrong-format bugs found. The 9 Output-only fields plus StackSetOperation.CreationTimestamp/EndTimestamp are ABSENT (dropped-field class, not this pass's scope, not fabricated) -- ResourceScan/GeneratedTemplate/TypeSummary/HookResult models have no backing field at all for them."}
  ecr_repository_empty_on_delete: {status: ok, note: "FIXED (gopherstack-gyfh): deleteECRRepository (resources_ecs.go) always passed force=true to ecr.DeleteRepository during stack teardown, so a non-empty AWS::ECR::Repository was silently force-deleted regardless of template content -- a deliberate placeholder left by gopherstack-e4qn (moving the not-empty check into DeleteRepository required every caller to pass force explicitly; force=true exactly preserved the pre-e4qn behavior, which had no emptiness check at all). Now reads the resource's EmptyOnDelete property (props[\"EmptyOnDelete\"].(bool), same props[key].(bool) pattern used throughout resources.go for other boolean properties, e.g. resources_cloudtrail.go/resources_efs.go) and threads it through as force; absent/false blocks deletion of a non-empty repository (ErrRepositoryNotEmpty propagates to a DELETE_FAILED stack event), true force-deletes through existing images. SOURCING: EmptyOnDelete is a real AWS::ECR::Repository property but CloudFormation resource-property schemas are not part of aws-sdk-go-v2, so there is no types/types.go line to cite -- TRANSCRIBED FROM AWS'S CURRENT DOCUMENTATION (https://docs.aws.amazon.com/AWSCloudFormation/latest/TemplateReference/aws-resource-ecr-repository.html, fetched live this pass: \"If true, deleting the repository force deletes the contents of the repository. If false, the repository must be empty before attempting to delete it.\"), same disclosure shape as stack_policy_enforcement below. The default (absent == false == must-be-empty) is not stated as an explicit \"Default:\" line on that page (unlike some other properties there) but follows directly from the property's own if-true/if-false description and from real `aws ecr delete-repository` semantics (a bare boolean CFN property with no stated default and an explicit false-branch description defaults to its zero value); threading props required adding a props parameter to the shared delete-dispatch chain (deleteExtendedResource -> deleteServiceResource -> deleteDataPlatformResource -> deleteNewServiceResource -> deleteComputeStorageResource -> deleteECRRepository), which the top-level Delete already received (res.Properties from stacks.go) but never forwarded past deleteCoreResource. Verified via TestDeleteECRRepository_EmptyOnDelete (table test, both directions: EmptyOnDelete=true forces through a repo holding a pushed image; EmptyOnDelete absent and explicit-false both fail with RepositoryNotEmptyException against the same non-empty repo). No pre-existing test exercised a non-empty repository during CFN teardown (the existing lifecycle tests only create-then-delete freshly-created, always-empty repos), so nothing regressed."}
  call_as: {status: ok, note: "FIXED this pass (gopherstack-xhu2t, 2026-09-13): CallAs (StackSet identity for delegated-administrator calls) was declared on 20 operations' Input structs and silently dropped on every one -- an invalid enum value was accepted as if it were SELF, and DELEGATED_ADMIN was never checked against the target StackSet's PermissionModel. Fixed via validateCallAs (enum check: SELF/DELEGATED_ADMIN only) and validateCallAsForStackSet (rejects DELEGATED_ADMIN against a PermissionModel != SERVICE_MANAGED StackSet, the one observable constraint this backend can honor without simulating an AWS Organizations management/delegated-administrator identity), wired into all 20 ops (see handler_stack_sets.go, handler_template_ops.go). Verified via TestRealClient_StackSetCallAs (realclient_stackset_callas_test.go): invalid enum rejected across a representative spread of ops, DELEGATED_ADMIN rejected against SELF_MANAGED and accepted against SERVICE_MANAGED, SELF (and omitted CallAs) always accepted regardless of PermissionModel."}
  disable_rollback_and_retain_except_on_create: {status: ok, note: "FIXED this pass (gopherstack-xhu2t, 2026-09-13): two real pre-existing bugs found alongside the reqfielddiff sweep, not just dropped fields. (1) stack.DisableRollback (CreateStackInput.DisableRollback) was stored on the Stack but never consulted anywhere -- provisionResources' rollback-on-resource-failure path and failAndRollback's rollback-on-preflight-validation-failure path both always rolled back regardless. Now both check stack.DisableRollback and leave the stack CREATE_FAILED (no rollback events) when set. (2) rollbackCreateResources ignored each resource's DeletionPolicy entirely and always force-deleted during CreateStack rollback -- a Retain/Snapshot-policy resource was silently deleted anyway, the opposite of RetainExceptOnCreate's documented default (\"the default already honors DeletionPolicy on rollback\"). Fixed: rollbackCreateResources now skips the physical delete for a Retain/Snapshot resource unless the caller's RetainExceptOnCreate=true forces it, while still untracking the resource from the stack (matching the pre-existing DeleteStack/deleteStaleResources convention -- untracked-but-not-deleted, exactly like real AWS). CreateStackInput.ResourceTypes (allowlist) and DisableValidation (skip validateIntrinsics) were also previously dropped on CreateStack/UpdateStack and are now enforced (resourceTypeAllowed/validateResourceTypesAllowed, intrinsics_validate.go). Verified via TestRealClient_StackOptions (realclient_stack_options_test.go, 14 subtests): resource-types allowlist rejects a disallowed Type, disable-validation lets an otherwise-invalid template through, retain-except-on-create is proven by HeadBucket-ing the underlying S3 bucket directly after a forced rollback (not DescribeStackResource, which correctly reports not-found once untracked), and disable-rollback is proven by driving ExecuteChangeSet's CreateStack fallback against a nonexistent stack name with a template containing an unsupported resource type, landing on CREATE_FAILED instead of ROLLBACK_COMPLETE."}
  type_registry_filters_and_registration: {status: ok, note: "FIXED this pass (gopherstack-xhu2t, 2026-09-13): ListTypes' Visibility/ProvisioningType, ListTypeRegistrations' RegistrationStatusFilter, and TestType's VersionId were all declared request-side filters that were read nowhere. Now enforced (type_registry.go's ListTypes/ListTypeRegistrations/TestType signatures gained the filter params; TestType validates VersionId against the stored typeVersions). RegisterPublisher now requires AcceptTermsAndConditions=true (was silently accepted with the field absent, contradicting the real API's mandatory-acceptance contract) -- fixed 2 pre-existing tests that had been driving RegisterPublisher without the field (wire_field_fixes_cfn21my_test.go, type_registry_test.go), not weakened, since they were exercising the bug rather than pinning real behavior. Adjacent bug found and fixed: DescribeTypeRegistration only ever populated TypeArn, never the distinct TypeVersionArn field the real response also carries -- now both are emitted. CORRECTION during this pass: an earlier draft of this fix incorrectly added Visibility/ProvisioningType fields to the ListTypes response wire shape (typeXML) and a ProvisioningType field to the TypeSummary model, assuming they were echoed back -- a real-SDK-typed compile error (types.TypeSummary has no such field) caught this; verified against deserializers.go's awsAwsquery_deserializeDocumentTypeSummary that neither is wire-visible on the response (both are request-only filters), and reverted. Verified via TestRealClient_StackOptions's list_types/list_type_registrations/test_type_version_id/register_publisher subtests."}
gaps: []
items_still_open:
  - "changeset_diff.go requiresRecreation() covers only a curated subset of resource types' replacement-forcing properties — expanding it is future work under gopherstack-e5h, not a regression (re-verified 2026-09-18)"
  - "SetTypeConfiguration accepts configuration for any type name without prior registration — intentional permissiveness for first-party AWS types this emulator doesn't catalog fully (bd: gopherstack-e5h; re-verified 2026-09-18)"
  - "StackSets DeploymentTargets.AccountFilterType INTERSECTION/DIFFERENCE/UNION and AccountsUrl are not implemented (only unset/NONE is honoured; other values are rejected with ValidationError, not silently dropped) — no account-filter graph to compute them against (bd: gopherstack-g7b5, gopherstack-nirx; re-verified 2026-09-18)"
  - "ImportStacksToStackSet doesn't tag imported instances with a real OU — ImportStacksToStackSetInput has no DeploymentTargets to source one from (structural, unaffected by the gopherstack-g7b5 OU work; re-verified 2026-09-18)"
  - "StackSetOperations complete synchronously as SUCCEEDED (RUNNING/STOPPING unreachable) — deliberate: cloudformation has no clock/janitor-driven lifecycle anywhere, every op resolves inside its own handler call (gopherstack-b3pm; see families: stacksets for the full writeup and tests; re-verified 2026-09-18)"
  - "Stack policy enforcement doesn't implement NotAction/NotResource (disclosed, not approximated), treats Replacement=='Conditionally' as Update:Replace (errs protective), doesn't model StackPolicyBody/URL at Create/UpdateStack time, and doesn't check parameter-only updates (no TemplateBody diff to compute) — see families: stack_policy_enforcement (gopherstack-cqy3; re-verified 2026-09-18)"
  - "CreateChangeSet's IncludeNestedStacks (api_op_CreateChangeSet.go:209) is read nowhere — changeset_diff.go's computeChanges has no nested-stack awareness to include/exclude against (unmodeled subsystem; DisableValidation/ResourceTypes were the same class of gap and are now fixed, see ops: CreateChangeSet, 2026-09-18)"
  - "UpdateStack.RetainExceptOnCreate is accepted and validated but has nothing to act on outside CreateStack/ExecuteChangeSet's create-fallback (which IS wired): UpdateStack has no resource-level create-then-rollback machinery at all (gopherstack-xhu2t; re-verified 2026-09-18)"
  - "RollbackStack is a status-only stub (flips StackStatus, replays nothing) and drops RoleARN/RetainExceptOnCreate both — same missing rollback machinery as the UpdateStack line above (gopherstack-xhu2t; re-verified 2026-09-18)"
  - "ListResourceScanRelatedResources ignores MaxResults/NextToken and always returns an empty list — this backend computes no cross-resource relationship graph for a scan, so there's nothing to paginate over (gopherstack-xhu2t; re-verified 2026-09-18)"
  - "ActivateType's AutoUpdate/MajorVersion/VersionBump/LoggingConfig/ExecutionRoleArn are all dropped — no multi-version type catalog exists for them to gate (RegisterType stores one version per type, ActivateType hardcodes VersionID \"00000001\"; same class as SetTypeConfiguration above) (gopherstack-xhu2t; re-verified 2026-09-18)"
leaks: {status: clean, note: "no goroutines/janitors/tickers introduced this pass. All fixes are pure control-flow/data changes under the existing b.mu lock discipline (every new lock path already has its matching defer Unlock/RUnlock, verified by reading each new/changed method in full). The persistence fix (10 previously-unpersisted map fields) is the largest change this pass but is snapshot/restore-only -- no new background work, no new maps that need cascade-delete beyond what already existed (stackInstances/stackSetOperations were already correctly cascade-deleted by DeleteStackSet before this pass; this pass only fixed their Snapshot/Restore wiring, not their lifecycle). FIXED (gopherstack-8907, 2026-09-06): DeleteStack cleared driftDetections/driftByStackID via pruneDriftDetections but not resourceDriftStatus[StackID]/resourceDriftDetail[StackID], both populated by DetectStackDrift/DetectStackResourceDrift and persisted verbatim in Snapshot() -- unbounded growth on drift-detect/delete churn (StackID embeds a random UUID, so this is not a wrong-answer-on-recreate case, but it is an unbounded leak observable via the persisted snapshot). Now cleared inside pruneDriftDetections. See TestDeleteStack_ClearsDriftMaps."}
---

## Notes

### 2026-09-24 (parity sweep): 25 new resource types (380 -> 405), 6 new backend families wired

Added real create+delete support for 25 `AWS::*` resource types across 11
service families, each backed by a genuine `InMemoryBackend` call (no
stubs), with Ref/Fn::GetAtt verified against the live AWS CloudFormation
Template Reference docs (fetched this pass):

- **Glue**: Blueprint, CustomEntityType, Workflow (`resources_glue_newer.go`)
- **DataSync**: Agent, LocationS3, Task (`resources_datasync.go`)
- **Transfer**: Profile, Workflow (`resources_transfer_more.go`)
- **AppConfig**: Application, Environment, ConfigurationProfile,
  DeploymentStrategy (`resources_appconfig.go`)
- **Macie**: AllowList, FindingsFilter (`resources_macie.go`)
- **GuardDuty**: Detector, IPSet (`resources_guardduty.go`)
- **AccessAnalyzer**: Analyzer, ArchiveRule (`resources_accessanalyzer.go`)
- **Amplify**: App, Branch (`resources_amplify.go`)
- **Batch**: SchedulingPolicy, ServiceEnvironment (`resources_batch_more.go`)
- **EFS**: AccessPoint (`resources_efs_more.go`)
- **Redshift**: ClusterParameterGroup, ClusterSubnetGroup
  (`resources_redshift_more.go`)

Dispatch wiring lives in `resources_newest_dispatch.go`, chained off the end
of `createNewerSupplementalResource`/`deleteNewerSupplementalResource`
(create/delete-by-physicalID) and off `deletePropsBasedResource`'s default
case via a new `deleteOverflowPropsBasedResource` (delete-needing-sibling-
properties, for AppConfig Environment/ConfigurationProfile, GuardDuty IPSet,
Amplify Branch, and AccessAnalyzer ArchiveRule). The extra indirection
(`deleteOverflowPropsBasedResource`) exists solely to keep
`deletePropsBasedResource`'s own switch -- already at its cyclop budget --
to a single call in its `default` case.

**Backend wiring.** DataSync, AppConfig, Macie2, and AccessAnalyzer were not
previously reachable from CloudFormation at all: `cli.go` gained
`datasyncHandler`/`macie2Handler`/`accessanalyzerHandler` fields, `byName[...]`
assignments, and `GetDataSyncHandler`/`GetMacie2Handler`/
`GetAccessAnalyzerHandler` getters (mirroring the existing pattern for every
other service). GuardDuty, Amplify, and AppConfig already had cli.go getters
from earlier work; they only needed `BackendsProvider` interface entries and
`ServiceBackends` struct fields/extraction wiring in `provider.go`/
`resources.go`.

**FSx was skipped, not stubbed.** Every `fsx.InMemoryBackend` `Create*`
method (`CreateFileSystem`, `CreateBackup`, `CreateStorageVirtualMachine`,
`CreateVolume`, `CreateSnapshot`, ...) takes an *unexported* input struct
pointer (e.g. `*createFileSystemInput`) as its sole parameter -- even though
the methods themselves are exported and part of the exported
`StorageBackend` interface, no code outside the `fsx` package can construct
that argument, so none of them is callable from `services/cloudformation`.
This differs from every other family added this pass (and from FSx's own
sibling packages), where `Create*` takes either plain scalars or an exported
struct. `AWS::FSx::FileSystem` and `AWS::FSx::Backup` were dropped from the
candidate list rather than worked around (e.g. by hand-marshaling JSON
through the HTTP-level op dispatch) to avoid a fragile, unprecedented
integration shape for two resource types.

**cfn_attributes_gen.go regenerated** via
`go run ./cmd/cfnattrgen -spec <cfn-resource-spec.json> -src services/cloudformation -out services/cloudformation/cfn_attributes_gen.go`.
Most of the new types' attribute names that are also common English words
used as struct/JSON keys elsewhere in this package (`Id`, `Status`,
`ApplicationId`, `AppId`, `ConfigurationProfileId`, ...) were excluded by the
generator's own goconst-safety rule -- adding them as new map-key literals
would have tipped an already-frequent literal over golangci-lint's
`goconst` `min-occurrences: 3` threshold. Per the generator's documented
contract this is conservative, not lossy: an attribute absent from the table
falls back to today's pre-existing (permissive) `Fn::GetAtt` resolution
rather than being wrongly rejected as undocumented. Attribute names distinct
enough to clear the threshold (`AgentArn`, `LocationArn`, `EndpointType`,
`ServiceEnvironmentArn`, `ClusterSubnetGroupName`, `ProfileId`,
`WorkflowId`, ...) made it into the table normally.

Tests: `resources_newest_sweep_test.go`, table-driven with `t.Parallel()`
throughout, one real `CreateStack` -> `Fn::GetAtt`/`Outputs` -> `DeleteStack`
round trip per type, asserting the backend object exists after create and is
gone after delete. Several deliberately exercise real `Fn::GetAtt`
cross-references between resources rather than `Ref`: `DataSync::Task`'s
`SourceLocationArn`/`DestinationLocationArn` from two `LocationS3` resources'
`LocationArn` attribute; `AppConfig::Environment`/`ConfigurationProfile`'s
`ApplicationId` from the owning `Application`; `GuardDuty::IPSet`'s
`DetectorId` from the owning `Detector`; `Amplify::Branch`'s `AppId` from the
owning `App`; and `AccessAnalyzer::ArchiveRule`'s `AnalyzerName` from the
owning `Analyzer`'s `Arn` attribute (requiring the same ARN-to-name
conversion `resourceNameFromARN` already provides for
`AccessAnalyzer::Analyzer`'s own ARN-shaped `Ref`).

### 2026-09-24 (GetAtt follow-ups): delete-time attribute stash persists on the stack; Fn::GetAtt on an undocumented attribute is a ValidationError

Two follow-ups to the property-time Fn::GetAtt fix below.

**gopherstack-9e44r.** DeleteStack (and UpdateStack's stale-resource delete
and both rollback paths) rebuilt the physicalIDs map passed to `Delete` from
`stackPhysicalIDsSnapshot` alone -- plain `{logicalID: PhysicalID}`, none of
property-time resolution's side channel. A props-based delete that
re-resolves a sibling `Fn::GetAtt` (e.g. CodeArtifact Repository/PackageGroup
`DomainName: {Fn::GetAtt: [Dom, Name]}`) therefore resolved to the sibling's
physical ID instead of the requested attribute. Fixed two ways:

- `deleteResolveContext` (resources.go) rebuilds the `_Type/<logicalID>`
  side channel and account/region/stack name fresh from `StackResource.Type`
  and the backend every time -- always accurate, no persistence needed.
- The genuinely non-derivable part -- backend-computed attribute values
  stashed as `<logicalID>/<Attr>` at create time (CodeArtifact Domain's
  Name/Owner/EncryptionKey, IAM AccessKey's SecretAccessKey, ServiceDiscovery
  HostedZoneId, ...) -- is now persisted on a new `Stack.ResourceAttrs`
  field, refreshed after every successful CreateStack/UpdateStack
  (`extractAttrStash`, filtered to the currently-live resource set) and
  merged back in by `deleteResolveContext`.

`Stack.ResourceAttrs` is a plain additive `map[string]string` field on a
`store.Table[*Stack]`-registered type, so it rides along in `Snapshot()`'s
existing `Tables["stacks"]` blob with no `backendSnapshot` change and no
`cfnSnapshotVersion` bump. `go test ./pkgs/persistence/ -update` inventory
diff: one new line, `Stack.ResourceAttrs map[string]string`, plus
column-realignment noise on its four struct-tag neighbors from gofmt
regrouping around the new field's comment (no field's tag/type/name
changed, only whitespace). Verified via
`TestDeleteStack_PropertyGetAtt` (resources_delete_getatt_test.go) and by
converting `testCodeArtifactRepository`/`testCodeArtifactPackageGroup`
(resources_newer_types_test.go) from a literal `DomainName` + `DependsOn` to
real `Fn::GetAtt: [Dom, Name]`.

**gopherstack-p7pvq.** `getResourceAttribute`/`getExtraResourceAttribute`
silently fell back to the physical ID for an attribute name a resource type
doesn't support, instead of real CloudFormation's `CreateStack`/`UpdateStack`
ValidationError ("Template error: resource `<X>` does not support attribute
type `<Y>` in Fn::GetAtt"). Fixed by validating `Fn::GetAtt`/`Fn::Sub
${Logical.Attr}` attribute names against a generated table
(`cfn_attributes_gen.go`, `cmd/cfnattrgen`) built from the CloudFormation
resource specification's `ResourceTypes.<T>.Attributes`. A type absent from
the table is untouched (falls back to today's behaviour) -- the table only
covers types with a declared `resTypeXxx` constant in this package AND whose
*entire* documented attribute set can be re-quoted without tripping this
repo's `goconst` (an attribute already re-used as a Go string constant, e.g.
`attrNameArn`/`attrNameName`, is referenced by identifier instead of
re-quoted; a type with even one attribute that can't be safely emitted is
dropped whole, never partially, since a partial table would reject a real
documented attribute the table simply declined to re-quote). This kept the
table to 76 of the ~360 types this backend emulates -- conservative, not
lossy, per the same "unknown falls back" rule.

The check runs synchronously (`preflightGetAttAttributeErr`, called from
`CreateStack`/`UpdateStack` before the stack is created/mutated, unless
`DisableValidation`), unlike this file's other intrinsic-reference checks
(undefined logical ID, unsupported resource Type), which fail the stack
asynchronously (`ROLLBACK_COMPLETE`/`UPDATE_ROLLBACK_COMPLETE`) -- matching
this specific error's own real-AWS `ValidationError` semantics. Fixed two
latent test bugs this validation caught: `testAthenaNamedQuery` GetAtt'd
`Name` (not `NamedQueryId`, NamedQuery's only documented attribute) and
`testCodeArtifactPackageGroup` GetAtt'd `Pattern` (not `Arn`, PackageGroup's
only documented attribute) -- both switched to the documented attribute.
Verified via `TestCreateStack_GetAttAttributeValidation`
(intrinsics_getatt_attribute_test.go): an unknown attribute on a table-listed
type fails through the typed SDK client with a `ValidationError`; a
documented attribute on the same type still works; a type absent from the
table still falls back for any attribute name.

### 2026-09-24 (property-time Fn::GetAtt fix): resolveGetAtt now sees a resource's real type, account ID and region while resolving another resource's Properties

Fixed the bug class the wave-10 entry below originally flagged as
"not fixed, pre-existing": `strProp`/`resolve` resolve a resource's own
Properties at create time by calling `ResolveValue` with a bare
`{params, physicalIDs}` `resolveCtx` -- no `resourceTypes` map, and no
account ID/region, unlike Outputs-time resolution's fully-populated ctx.
`resolveGetAtt`'s stashed-attribute gate switches on `resType`, so an
`Fn::GetAtt` used as an *input property value* for another resource in the
same stack always fell through to the plain-physID fallback for any
attribute that wasn't the physical ID itself.

Rather than widen `ResolveValue`'s signature (which would mean threading a
`resourceTypes` map through the ~450 `create*` methods and ~1,150
`strProp`/`resolve` call sites, most of which never touch GetAtt), a small
side channel is now stashed directly into the `physicalIDs` map every
`create*` method already receives -- `physIDResourceTypeKey(logicalID)` ->
declared type, plus `_AccountId`/`_Region`/`_StackName` (`template.go`,
alongside the pre-existing `_StackId`/`_StackName` custom-resource
convention). `provisionResources` (create) and `applyTemplateToStack`
(update, `stacks.go`) populate it before resource creation starts, from the
same `tmpl.Resources`/`b.accountID`/`b.region` data Outputs-time resolution
already uses. `resolveGetAtt` falls back to this side channel only when its
`ctx.resourceTypes` map is empty (the Outputs/preview paths already pass a
real one, so they're unaffected), and `ResolveValue` seeds
accountID/region/stackName from it. No caller of `strProp`/`resolve`/
`ResolveValue` changed.

Fn::Sub `${Res.Attr}`, Fn::Join and Fn::Select already routed through the
same `resolveGetAtt`/`resolveValueCtx` path, so they're fixed for free.
Fn::ImportValue/Fn::If/Fn::FindInMap/Fn::Transform inside a resource's own
Properties still resolve through a bare-ish context (no exports/
conditions/mappings/macros side channel was added) -- out of scope for this
fix, not a regression it introduces.

Unknown-attribute behaviour is unchanged and intentionally so:
`getResourceAttribute`/`getExtraResourceAttribute` silently fall back to the
physical ID for an attribute name they don't recognize on a *known* type,
both at Outputs time and now at property time -- this backend has no
GetAtt-unknown-attribute error path anywhere (CloudFormation's real
`No attribute named X found` error is unmodeled), so property-time
resolution stays consistent with Outputs-time rather than inventing new
behaviour here.

Simplified `resources_newer_types_test.go`'s `logs_delivery` case to use a
real `Fn::GetAtt: [Dest, Arn]` for `Delivery.DeliveryDestinationArn` instead
of a literal ARN string plus `DependsOn` (Logs::Delivery deletes by its own
physical ID, so this doesn't touch the delete path). The `codeartifact_repository`/
`codeartifact_package_group` cases and the wave-9 MemoryDB
`SubnetGroupName`+`DependsOn` case are left as-is: their delete paths
re-resolve a sibling property (DomainName/SubnetGroupName) from a
`stackPhysicalIDsSnapshot` map that's rebuilt fresh from
`{logicalID: PhysicalID}` at DeleteStack time and carries none of the
create-time attribute stash or side channel, so `Fn::GetAtt` there still
can't recover a value distinct from the physical ID -- a separate,
structural delete-path limitation this pass doesn't touch. New coverage
added in `resources_property_getatt_test.go`
(`TestCreateStack_PropertyTimeGetAtt`): the CodeArtifact and Logs::Delivery
repros above via a real CreateStack round trip, a derived (non-stashed)
attribute case (SQS Queue `Arn` computed via `arn.Build`, fed into an
`AWS::SNS::Subscription`'s `Endpoint` -- the `sqs` protocol's ARN-shape
validation means the pre-fix fallback to the queue's URL-shaped physical ID
would have failed CreateStack outright, not just resolved wrong), and the
same case via `Fn::Sub "${Queue.Arn}"`.

### 2026-09-24 (parity-sweep, wave 10): Lambda/Events/Scheduler/AppSync/Route53Resolver/CloudTrail/Logs/CodeArtifact -- 24 new resource types: 356 -> 380 supported types

Added: `AWS::Lambda::CodeSigningConfig` (resources_lambda_csc.go);
`AWS::Events::Endpoint` (resources_events_endpoint.go);
`AWS::Scheduler::ScheduleGroup` (resources_scheduler_group.go);
`AWS::AppSync::{DomainName,GraphQLSchema,ChannelNamespace}`
(resources_appsync_more.go); `AWS::Route53Resolver::{ResolverRuleAssociation,
FirewallDomainList,FirewallRuleGroup,FirewallRuleGroupAssociation,
ResolverQueryLoggingConfig,ResolverQueryLoggingConfigAssociation,
OutpostResolver}` (resources_route53resolver_more.go);
`AWS::CloudTrail::{EventDataStore,Channel}` (resources_cloudtrail_more.go);
`AWS::Logs::{Delivery,DeliveryDestination,DeliverySource,Integration,
LogAnomalyDetector,ScheduledQuery}` (resources_logs_more.go); and
`AWS::CodeArtifact::{Domain,Repository,PackageGroup}`
(resources_codeartifact.go, new family -- CodeArtifact wasn't wired into
CloudFormation at all before this pass, despite `GetCodeArtifactHandler`
already existing on `BackendsProvider`/cli.go; added the `CodeArtifact`
field to `ServiceBackends` and its `extractAllServiceBackends` wiring in
provider.go). All create* dispatch through a new `createNewerSupplementalResource`/
`deleteNewerSupplementalResource` tier (resources.go), added because
`createMoreSupplementalResource`/`deleteMoreSupplementalResource` (the
MemoryDB/Athena/SageMaker overflow tier from wave 9) were themselves at
their cyclop budget.

Doc-vs-backend Ref mismatches (same class as the Athena::NamedQuery
precedent, resources_athena.go): `Lambda::CodeSigningConfig` and
`CloudTrail::{EventDataStore,Channel}`'s Template Reference pages literally
say "Ref returns the resource name", but none of the three has a Name-keyed
backend identity (CodeSigningConfig has no Name property at all; Create/Delete
for all three take the ARN) -- the ARN is used as Ref/physical ID instead, with
the docs' own non-Arn GetAtt attributes (CodeSigningConfigId; none for
EventDataStore/Channel beyond the ARN itself) stashed or, for EventDataStore,
absent since Name isn't actually a documented GetAtt attribute either.
`Events::Endpoint`'s docs say Ref is the EndpointId ("mystack-Endpoint-ABCDEFGHIJK"-shaped),
but the backend is Name-keyed (`DeleteEndpoint(ctx, name)`); since
`EndpointID = name + "-" + region` deterministically (endpoints.go), Ref
uses the real EndpointId and delete strips the trailing `"-"+region` back
off rather than stashing Name. Several Route53Resolver types' Ref text
("returns the FirewallDomainList object", "the FirewallRuleGroupId") doesn't
name a field distinctly from their documented `Id` GetAtt attribute, so `Id`
(the backend's own key) is used, same imprecision class. Where Ref is fully
undocumented (`Logs::{Delivery,DeliveryDestination,DeliverySource,
Integration}`, `Logs::{LogAnomalyDetector,ScheduledQuery}` -- checked via the
raw page HTML: an empty `<h3>Ref</h3>` with no following paragraph, not
merely a generic sentence), the backend's own primary key is used per the
task's "use the primary identifier" rule (DeliveryId, Name, Name, IntegrationName,
AnomalyDetectorArn, ScheduledQueryArn respectively).

`AppSync::GraphQLSchema` and `AWS::ECS::PrimaryTaskSet` share a "no
independent lifecycle" delete: a GraphQL schema lives and dies with its
owning Api, so delete is a documented no-op (create genuinely calls
`StartSchemaCreation`, so this isn't a stub -- same reasoning as
PrimaryTaskSet's existing no-op delete). `AppSync::ChannelNamespace` is keyed
by `(ApiId, Name)` against `b.eventAPIs`, a *different* table than the
`b.apis` GraphQLApi table `AWS::AppSync::GraphQLApi` populates -- Event APIs
are created via `CreateAPI`, which only `AWS::AppSync::Api` (not implemented
this pass, see skipped list) would wire up; the test seeds an Event API
directly on the backend and passes its ID in as a stack Parameter rather than
provisioning it through CFN.

Skipped (ops missing or out of scope, not stubbed): `Route53Resolver::
{FirewallConfig,ResolverDNSSECConfig,ResolverConfig}` -- no create/delete
API exists in real AWS either (singleton per-VPC config mutated via
Update/Put; CFN itself models Create as an initial Update call and Delete
as a reset-to-default Update, which this backend has no clean way to
distinguish from an ordinary property update without a dedicated "does this
logical ID own the config" tracking layer). `AppSync::ApiCache` -- the
Template Reference page has no "Return values" section at all (no Ref, no
GetAtt documented for this type), so there's no doc basis for its Ref value
even under the "use the primary identifier" fallback rule; `CreateAPICache`/
`DeleteAPICache` exist and are real, so this is a candidate for a future
pass once Ref is settled some other way. `AppSync::{Api,SourceApiAssociation,
DomainNameApiAssociation}` -- would need the Event-API CreateAPI path
(see ChannelNamespace above) wired up first. `CodeArtifact::Package` --
CFN documents this type but the backend has `DeletePackage` with no
corresponding `CreatePackage` (packages are created implicitly via publish,
matching real AWS's own model where `AWS::CodeArtifact::Package` largely
exists for `OriginConfiguration` on an already-published package, not for
provisioning one from scratch) -- no-stub rule leaves this for a pass that
adds package-version-aware create semantics. `CloudWatchLogs::AccountPolicy`
-- backend has `PutResourcePolicy`/`DeleteResourcePolicy` but no
account-scope-specific variant distinct from the existing (already-wired)
`Logs::ResourcePolicy`'s resource-scope semantics.

`resources_newer_types_test.go` (new): one table-driven
`TestCreateStack_NewerTypes` (24 subtests, each `t.Parallel()`) driving a
real CreateStack -> Fn::GetAtt -> DeleteStack -> backend-describe round trip
per type through the real `aws-sdk-go-v2/service/cloudformation` client,
asserting the backend object exists after create and is gone after delete.

### 2026-09-24 (parity-sweep, wave 9): AWS::MemoryDB::{ParameterGroup,SubnetGroup,User,ACL,Cluster} -- 5 new resource types: 339 -> 344 supported types

Unblocks the wave-8 skip note below: `services/memorydb/exports.go` gained
`ExportedCreateClusterRequest`/`ExportedCreateACLRequest`/
`ExportedCreateSubnetGroupRequest`/`ExportedCreateUserRequest`/
`ExportedCreateParameterGroupRequest` (type aliases for the previously
package-private `create*Request` structs -- most already existed there for
external tests; added `ExportedTagEntry`, `ExportedUpdateParameterGroupRequest`,
and `ExportedParameterNameValueEntry` to round out the set). Aliases, not new
types, so `InMemoryBackend.Create*` keeps its exact validated code path and
the memorydb handler's own behavior is unchanged; `go test ./services/memorydb/...`
stayed green with no handler edits. `services/cloudformation/resources_memorydb.go`
(new) adds AWS::MemoryDB::{ParameterGroup,SubnetGroup,User,ACL,Cluster}, calling
`rc.backends.MemoryDB.Backend.Create*` directly through those aliases -- the
MemoryDB backend was already wired into `ServiceBackends`/`BackendsProvider`
(provider.go) by an earlier, unrelated pass, so no provider.go edit was needed
this time.

Ref for all five types is the resource ARN (curl'd the CloudFormation Template
Reference's Return values section for each -- none of the five pages has a
separate "Ref" subsection; the ARN GetAtt entry's own text states "Ref returns
the ARN of the ..." for every one). GetAtt: ParameterGroup/SubnetGroup/User/ACL
each add only `ARN` (SubnetGroup/User/ACL/User's is spelled `ARN`/`Arn`
per-page, immaterial since physicalID already equals the ARN and any
unmatched attrName falls back to physID); SubnetGroup also has
`SupportedNetworkTypes`, hardcoded `"ipv4"` since the backend's own
`defaultSupportedNetworkTypes` is a fixed constant, never per-resource state;
ACL/User also have `Status`, hardcoded `"active"` since CreateACL/CreateUser
always set that status constant synchronously (no lifecycle overlay). Cluster
additionally documents `ClusterEndpoint.Address`, `ClusterEndpoint.Port`,
`ParameterGroupStatus`, and `Status` -- all backend-computed at create time,
so stashed into `physicalIDs[logicalID+"/<Attr>"]` the same way EKS/SageMaker
side-channel attrs are (added `resTypeMemoryDBCluster` to the stashed-attribute
type gate in `resolveGetAtt`, template.go). Delete is name-keyed same as the
wave-8 SageMaker/Athena ARN types, so `sagemakerNameFromARN` (already generic,
just an ARN-tail extractor) is reused rather than duplicated.

`resources_memorydb_test.go` (new): one table-driven `TestCreateStack_MemoryDBTypes`
(5 subtests, each `t.Parallel()`) driving a real CreateStack -> Fn::GetAtt ->
DeleteStack -> `Describe*` round trip per type through the real
`aws-sdk-go-v2/service/cloudformation` client, asserting the memorydb backend
object exists after create and is gone after delete.

Skipped: `AWS::MemoryDB::MultiRegionCluster` is a real, separately-documented
CloudFormation resource type but was out of this pass's five-type mandate
(ParameterGroup/SubnetGroup/User/ACL/Cluster) -- a scope choice, not a
structural blocker, left for a follow-up. `AWS::MemoryDB::MultiRegionParameterGroup`
is not a CloudFormation resource type AWS publishes at all (its template-reference
URL redirects to the TemplateReference index rather than resolving to a page)
-- structurally out of scope, not a gap.

### 2026-09-24 (parity-sweep, wave 8) 20 new resource types: 319 -> 339 supported types

Wired AWS::SageMaker and AWS::Athena into the CloudFormation backend for the first time
(GetSageMakerHandler/GetAthenaHandler added to BackendsProvider in provider.go and to
ServiceBackends/extractAllServiceBackends; cli.go already implemented both methods for the
generic backend registry from an earlier, unrelated pass, so no cli.go edit was needed).

Added AWS::SageMaker::{Model,EndpointConfig,Endpoint,NotebookInstance,
NotebookInstanceLifecycleConfig,CodeRepository,Domain,Pipeline,ModelPackageGroup,
FeatureGroup,Project,Workteam,Image,ImageVersion} (14 types); AWS::Athena::{WorkGroup,
DataCatalog,NamedQuery,PreparedStatement,CapacityReservation} (5 types); and
AWS::WAFv2::WebACLAssociation (1 type, against the WAFv2 backend already wired into
ServiceBackends) -- 20 types total. New files resources_sagemaker.go, resources_athena.go,
resources_wafv2_association.go; SageMaker/Athena added to ServiceBackends and to
resolveGetAtt's stashed-attribute type gate (template.go) for every type whose Ref is an
ARN/ID but which also exposes a documented GetAtt attribute derived from a sibling property
(e.g. Model's Ref is ModelArn but ModelName is also a GetAtt). Test helper
newDependentServiceBackends (resources_dependent_services_test.go) now constructs real
SageMaker/Athena backends for every test built on it.

Every Ref/Fn::GetAtt was checked against the CloudFormation Template Reference's "Return
values" section (curl + tag-stripped read) and, for the composite/undocumented cases, cross-
checked against the CloudFormation Registry resource schema's `primaryIdentifier` (from
github.com/aws-cloudformation/aws-cloudformation-resource-providers-sagemaker, fetched live).
Several SageMaker types document Ref as the resource's ARN while the backend's own Delete
methods are name-keyed (Model, EndpointConfig, Endpoint, NotebookInstance,
NotebookInstanceLifecycleConfig, CodeRepository, ModelPackageGroup, Project, Image) -- same
class as AWS::Backup::Framework's existing precedent (see the 291->319 entry below): the
trailing path segment is extracted from the ARN at delete time (sagemakerNameFromARN).
ImageVersion's ARN embeds both the parent image name and the version number
(.../image-version/<name>/<version>), extracted the same way (sagemakerImageVersionFromARN)
rather than via a sibling property. AWS::SageMaker::Workteam's Template Reference page has no
Ref subsection at all (only Fn::GetAtt Id/WorkteamName, both the work team's own name) --
WorkteamName, this backend's own primary key, is used as the fallback per this task's rule for
undocumented Ref. AWS::Athena::NamedQuery's page text ("Ref returns the resource name") cannot
be this type's real identifier: NamedQuery is backend- and real-AWS-keyed by a generated
NamedQueryId (CreateNamedQuery returns the ID, not the name; GetNamedQuery/DeleteNamedQuery
are ID-only, with no name-keyed lookup at all), and GetAtt NamedQueryId is documented as a
*separate* attribute from Ref -- the same "docs text doesn't match this type's own identifier
model" class already logged for AWS::Glue::Registry and AWS::ECR::RegistryPolicy in the
291->319 entry below. The generated ID is used as Ref/physical ID instead; Name is stashed as
a GetAtt-style attribute. AWS::WAFv2::WebACLAssociation's page documents Ref as
"name|id|scope" -- verified against the CloudFormation registry schema
(aws-cloudformation-resource-providers-wafv2) to be a copy/paste artifact from
AWS::WAFv2::WebACL's own Ref format: WebACLAssociation's only properties are
ResourceArn/WebACLArn (both required, createOnly, and the schema's actual
`primaryIdentifier`), so ResourceArn|WebACLArn (pipe-joined, matching CloudFormation's own
composite-key convention) is used instead. AWS::Athena::CapacityReservation and
AWS::SageMaker::NotebookInstance both need a state transition before their real delete call
will succeed (DeleteCapacityReservation requires CANCELLED/CANCELLING;
DeleteNotebookInstance requires Stopped) -- stack teardown now calls
CancelCapacityReservation / StopNotebookInstance (the synchronous, unconditional variants,
not the InService-only/async FSM ones) immediately before the real delete, mirroring how a
real deploy tool (e.g. the Terraform AWS provider) sequences these deletes.

Delete for AWS::Athena::PreparedStatement needs the sibling WorkGroup property (the backend
key is workGroup+"/"+name, not name alone), so it is wired through the existing
deleteMorePropsBasedResource props-based-delete dispatcher (resources_iam_more.go), the same
pattern AWS::Config::StoredQuery already used.

Skipped: the AWS::MemoryDB::* family (ParameterGroup/SubnetGroup/User/ACL/Cluster) -- the
memorydb.StorageBackend interface's Create* methods all take an *unexported*
`*memorydb.createXRequest` struct type, which services/cloudformation cannot name or
construct from outside the memorydb package (a real Go visibility blocker, not a scope
choice); see items_still_open for the exact methods and the fix this needs. This is a
correction to the previous pass's Notes entry below, which listed MemoryDB as merely
deferred for scope reasons -- it is not accessible at all without a services/memorydb change,
which was out of this pass's services/cloudformation-only mandate. AWS::SageMaker::App and
AWS::SageMaker::UserProfile were also considered and skipped: both have composite
primaryIdentifiers (App: AppName|AppType|DomainId|UserProfileName; UserProfile:
UserProfileName|DomainId, both confirmed via the CloudFormation registry schema) that would
need cross-resource Fn::GetAtt wiring to compose correctly in a template, and this pass found
(while debugging test failures) that Fn::GetAtt resolution used to link one resource's
Properties to a sibling resource created earlier in the same stack does not go through
resolveGetAtt's type-gated physicalIDs stash the way Fn::GetAtt in a stack's Outputs does
(ctx.resourceTypes is empty at that resolution point) -- a pre-existing template.go limitation
unrelated to any of this pass's new types, out of scope to fix here, and not exercised by any
of the 20 types actually added (none of their test templates depend on cross-resource
Fn::GetAtt during creation).

### 2026-09-24 (parity-sweep) 28 new resource types: 291 -> 319 supported types

Added AWS::IAM::{GroupPolicy,RolePolicy,UserPolicy,ServerCertificate} (4); AWS::ECR::
{PullThroughCacheRule,RegistryPolicy,RepositoryCreationTemplate} (3); AWS::ElastiCache::
{ParameterGroup,SecurityGroup,GlobalReplicationGroup,UserGroup} (4); AWS::Neptune::
{DBSubnetGroup,DBClusterParameterGroup,DBParameterGroup,GlobalCluster} (4); AWS::DocDB::
{DBSubnetGroup,DBClusterParameterGroup,GlobalCluster} (3); AWS::Backup::{Framework,ReportPlan}
(2); AWS::Glue::{Classifier,Registry,Schema,SecurityConfiguration,DevEndpoint} (5); and
AWS::CodeBuild::ReportGroup, AWS::Kinesis::ResourcePolicy, AWS::Lambda::ResourcePolicy (3) --
28 types total, all against backends already wired into ServiceBackends (no provider.go/
BackendsProvider changes needed). New files resources_iam_more.go, resources_ecr_more.go,
resources_elasticache_more.go, resources_neptune_more.go, resources_docdb_more.go,
resources_backup_more.go, resources_glue_more.go, resources_misc_more.go, plus the shared
test helper resources_more_resources_test.go; resources.go grew two new dispatch layers
(createMoreSupplementalResource/deleteMoreSupplementalResource and
deleteMorePropsBasedResource) purely to keep createSupplementalResource/
deleteSupplementalResource/deletePropsBasedResource under the cyclop/gocognit limits after
adding 8 new resource-type groups -- no behavior change to the existing dispatch chain.

Every Ref/Fn::GetAtt was checked against the CloudFormation Template Reference's "Return
values" section for that exact type (curl + tag-stripped read), not assumed from a sibling
type. Several are genuinely undocumented and are called out with the primary-identifier
fallback used instead: AWS::ECR::PullThroughCacheRule has no "Return values" section at all
(Ref returns EcrRepositoryPrefix); AWS::ECR::RegistryPolicy's section lists only
Fn::GetAtt RegistryId with no Ref subsection (this backend models one policy per registry, so
the registry ID is used as the primary identifier); AWS::Glue::Schema's section says
"Return values Ref" with no elaborating text (the documented Fn::GetAtt Arn value, SchemaArn,
is used); AWS::Glue::Registry's stated Ref text ("a combination of VersionId|Key|Value") does
not match this resource's own properties at all and reads as a copy/paste error from another
page (it is in fact AWS::Glue::SchemaVersionMetadata's real Ref shape) -- the registry name,
this backend's own primary key, is used instead; AWS::Kinesis::ResourcePolicy's section says
"Return values Ref" with no elaborating text (the target ResourceArn, this backend's own key,
is used). AWS::ElastiCache::ParameterGroup and AWS::ElastiCache::SecurityGroup have no name
property in their CFN schema at all (CloudFormation auto-generates one) -- both generate a
name from the logical ID, matching this file's existing AWS::ElastiCache::UserGroup precedent
in the pre-existing resources_extended.go. AWS::DocDB::GlobalCluster's documented
GlobalClusterResourceId Fn::GetAtt attribute is left unresolved rather than fabricated: this
backend's GlobalCluster struct has no such field (only GlobalClusterArn, which is stashed).

Delete for AWS::IAM::GroupPolicy/RolePolicy/UserPolicy and AWS::Glue::Schema needs a sibling
property (the owning group/role/user/registry name) not embedded in the Ref value, so all four
are wired through the existing deletePropsBasedResource props-based-delete dispatcher (the
same pattern AWS::Config::StoredQuery already used) rather than the physicalID-only path.
AWS::Backup::Framework's Ref is the framework ARN but DeleteFramework is name-keyed, so delete
extracts the name back out of the ARN, mirroring AWS::Backup::BackupVault's own precedent in
the pre-existing resources_backup.go.

Skipped (backend lacks the op, or the backend doesn't exist in this service's dependency
graph): AWS::SageMaker::* and AWS::Athena::* (prioritized types from the task brief) --
neither service backend is wired into CloudFormation's ServiceBackends/BackendsProvider at
all, and wiring one in requires adding a Get<Service>Handler() method to the BackendsProvider
interface AND implementing it in cli.go, which sits outside services/cloudformation/ and was
out of scope for this change (cli.go is also being edited by another agent this session, per
the task brief, making a concurrent edit there doubly unsafe). AWS::ECR::PublicRepository was
considered but skipped: this backend's CreateRepository models private ECR only (a different
AWS service/ARN namespace/attribute shape than ECR Public), and reusing it would misrepresent
the resource rather than emulate it. AWS::WAFv2::WebACLAssociation and the AWS::MemoryDB::*
family were left out only to keep this pass's scope bounded to the 20-30 range requested; both
have real, already-wired backend ops (wafv2.AssociateWebACL/DisassociateWebACL,
memorydb.Create/DeleteParameterGroup etc.) and are good candidates for a follow-up pass.

### 2026-09-24 (parity-sweep) 25 new resource types: 266 -> 291 supported types

Added AWS::IoT::{ThingType,ThingGroup,Policy,TopicRuleDestination,RoleAlias,Certificate,
ProvisioningTemplate,Authorizer,DomainConfiguration,JobTemplate,Dimension,SecurityProfile,
CustomMetric,FleetMetric,BillingGroup,MitigationAction,ScheduledAudit} (17 types; Thing and
TopicRule already existed) and wired AWS::Config into the CloudFormation backend for the first
time: AWS::Config::{ConfigRule,ConfigurationRecorder,DeliveryChannel,ConfigurationAggregator,
AggregationAuthorization,ConformancePack,RemediationConfiguration,StoredQuery} (8 types;
OrganizationConfigRule intentionally excluded -- it targets an AWS Organizations management
account, a different provisioning model than a single-account stack resource). New files
resources_iot_more.go, resources_awsconfig.go; AWSConfig added to ServiceBackends/
BackendsProvider/extractAllServiceBackends (provider.go, resources.go) -- cli.go already had
GetAWSConfigHandler wired for the dashboard, just not threaded into CloudFormation. Each type's
Ref/Fn::GetAtt was verified against the CloudFormation Template Reference's "Return values"
section for that exact type, not assumed from sibling types. Several Refs return a
backend-generated ID (ThingType/ThingGroup/BillingGroup all return TypeId/GroupId/Id, not the
name) while their delete operation takes the name -- these look the ID up via the backend's own
List* call (ListThingTypes/ListThingGroups/ListBillingGroups) rather than needing a logicalID
side-channel, since Delete() is only ever given physicalID + props, not the logicalID. Two Refs
are genuinely undocumented in the CloudFormation reference (AWS::Config::StoredQuery has an empty
Ref section): its resource-type-schema primary identifier is QueryId, so Ref returns that, and
delete re-reads the required QueryName property via the existing props-based delete dispatcher
(deletePropsBasedResource) since QueryId doesn't embed it. AWS::Config::AggregationAuthorization's
Ref is documented as the authorization's own ARN, which conveniently embeds both
AuthorizedAccountId/AuthorizedAwsRegion, so delete parses them back out of the ARN with no extra
lookup. Deterministic ARN attributes (e.g. IoT RoleAlias's RoleAliasArn, Config
ConfigurationAggregator's counter-based ConfigurationAggregatorArn) are computed with the same
format string the backend's own ARN builder uses (verified by reading it), or, where the backend
generates the ARN from an internal counter rather than the name, stashed at create time via the
`physicalIDs[logicalID+"/<AttrName>"]` pattern plus the resolveGetAtt gate in template.go (IoT
ThingType/ThingGroup/BillingGroup/DomainConfiguration/FleetMetric/MitigationAction, Config
ConfigRule/ConfigurationAggregator/ConformancePack/StoredQuery). IoT Certificate delete
deactivates an ACTIVE certificate before deleting (DeleteCertificate rejects ACTIVE, matching real
AWS IoT); IoT ThingType delete deprecates before deleting for the same reason (real AWS also
requires deprecation first, though this backend doesn't model the real 5-minute/300-day wait).
Partial-property gaps (create a real object but skip a subset of its optional nested config,
matching this file's existing convention of not wiring the CFN `Tags` property on any type):
IoT SecurityProfile's Behaviors/AlertTargets/MetricsExportConfig/AdditionalMetricsToRetainV2, IoT
JobTemplate's AbortConfig/TimeoutConfig/JobExecutionsRolloutConfig/JobExecutionsRetryConfig/
PresignedURLConfig/MaintenanceWindows/DestinationPackageVersions, and IoT DomainConfiguration's
AuthorizerConfig/ClientCertificateConfig/ServerCertificateConfig (ServerCertificateArns IS wired,
stashed for its ServerCertificates GetAtt). All 25 have a passing CreateStack ->
Fn::GetAtt-in-Outputs -> backend-state-assertion -> DeleteStack -> backend-state-gone test
(resources_iot_more_test.go, resources_awsconfig_test.go).

### 2026-09-24 (parity-sweep) 25 new resource types: 241 -> 266 supported types

Added AWS::RDS::{OptionGroup,EventSubscription,GlobalCluster,DBProxyEndpoint},
AWS::Cognito::{UserPoolResourceServer,UserPoolIdentityProvider},
AWS::SSM::{MaintenanceWindowTarget,MaintenanceWindowTask,PatchBaseline,ResourceDataSync,
ResourcePolicy}, AWS::CloudWatch::{MetricStream,AnomalyDetector,InsightRule},
AWS::ApiGateway::{VpcLink,ClientCertificate,DocumentationPart,DocumentationVersion}, and
AWS::CloudFront::{OriginRequestPolicy,KeyGroup,PublicKey,CloudFrontOriginAccessIdentity,
RealtimeLogConfig,KeyValueStore,ContinuousDeploymentPolicy} -- the list "skipped for budget" in
the prior 13-type pass below, minus RDS DBProxyTargetGroup, the four Cognito UserPoolUser*
types, and CloudFront MonitoringSubscription (still skipped this pass too, see below). Each is
backed by a real service call (create + delete both mutate real backend state) with Ref/GetAtt
verified against the CloudFormation Template Reference's "Return values" section for that type
(new files resources_rds_more.go, resources_ssm_more.go, resources_cloudwatch_more.go,
resources_apigateway_more.go, resources_cloudfront_more.go, plus two new cases in
resources_cognito.go). Several types have no documented Fn::GetAtt at all
(AWS::CloudWatch::AnomalyDetector, AWS::RDS::{OptionGroup,EventSubscription,GlobalCluster},
AWS::SSM::{PatchBaseline,ResourceDataSync}, AWS::Cognito::UserPool{ResourceServer,
IdentityProvider}) -- confirmed by reading the actual doc page, not assumed. A few Ref values
diverge from a naive physical ID and needed the existing "props-based delete" pattern
(deleteEKSAccessEntry's precedent) because a sibling CFN property (WindowId, UserPoolId,
RestApiId, Namespace/MetricName/Stat) isn't embedded in the returned Ref: MaintenanceWindowTarget/
Task, the two new Cognito types, ApiGateway DocumentationPart/DocumentationVersion, and
CloudWatch AnomalyDetector (delete-only, Ref/GetAtt undocumented so no props-based dispatch
needed for read paths). SSM::ResourcePolicy's delete additionally needs a live PolicyHash lookup
(GetResourcePolicies) since AWS's optimistic-concurrency hash is never a CFN property. Still
skipped (real value uncertain vs. effort, or backend shape mismatch found while implementing):
RDS::DBProxyTargetGroup (backend models target *registration* against an existing default proxy
target group, not an independently creatable/deletable CFN resource), Cognito
UserPoolUser/UserPoolUserToGroupAttachment/UserPoolRiskConfigurationAttachment/
UserPoolUICustomizationAttachment (each is an admin-mutation style resource with weaker
CFN-resource semantics), and CloudFront::MonitoringSubscription (keyed by an existing
Distribution rather than having its own creatable identity). All 25 have a passing
CreateStack -> Fn::GetAtt-in-Outputs -> backend-state-assertion -> DeleteStack ->
backend-state-gone test (resources_rds_more_test.go, resources_cognito_more_test.go,
resources_ssm_more_test.go, resources_cloudwatch_more_test.go, resources_apigateway_more_test.go,
resources_cloudfront_more_test.go).

### 2026-09-24 (parity-sweep) 13 new resource types: 228 -> 241 supported types

Added AWS::EKS::{FargateProfile,Addon,AccessEntry,PodIdentityAssociation,IdentityProviderConfig},
AWS::StepFunctions::{StateMachineVersion,StateMachineAlias}, AWS::Kinesis::StreamConsumer, and
AWS::ECS::{CapacityProvider,ClusterCapacityProviderAssociations,TaskSet,PrimaryTaskSet}, each
backed by a real service call with Ref/GetAtt verified against the CloudFormation docs (see
resources_eks.go, resources_stepfunctions.go, resources_kinesis_more.go, resources_ecs_more.go).
Fixed a real bug found along the way: `AWS::Firehose::DeliveryStream` isn't a real CFN type name
(the CFN spec only has `AWS::KinesisFirehose::DeliveryStream`) and its Ref returned the delivery
stream ARN instead of the documented stream name; now the real type name is supported (old name
kept as an alias for existing tests/templates), Ref returns the name, and Arn is a proper GetAtt
attribute (resources_firehose.go, resources_getatt.go). Skipped for this pass (budget): RDS
{OptionGroup,EventSubscription,GlobalCluster,DBProxyEndpoint,DBProxyTargetGroup}, Cognito
UserPool{ResourceServer,IdentityProvider,User,UserToGroupAttachment,RiskConfigurationAttachment,
UICustomizationAttachment}, SSM {MaintenanceWindowTarget,MaintenanceWindowTask,PatchBaseline,
ResourceDataSync,ResourcePolicy}, CloudWatch {MetricStream,AnomalyDetector,InsightRule},
ApiGateway {VpcLink,ClientCertificate,DocumentationPart,DocumentationVersion}, CloudFront
{OriginRequestPolicy,KeyGroup,PublicKey,CloudFrontOriginAccessIdentity,RealtimeLogConfig,
KeyValueStore,MonitoringSubscription,ContinuousDeploymentPolicy}, and the AWS::Config::* family
(would need AWSConfig wired into ServiceBackends first) -- all have real backend support to build
on, none were started.

### 2026-09-24 (parity-sweep) 20 new resource types: 220 -> 240 supported types

Added 3 AWS::CodeDeploy::* types (wired codedeploy into ServiceBackends) and 17 AWS::EC2::* types
(DHCPOptions, EIPAssociation, EgressOnlyIGW, CustomerGateway, CarrierGateway, InstanceConnectEndpoint, ClientVpn family, IPAM family, CapacityReservation, Host), each real-backend; see resources_codedeploy.go / resources_ec2_more.go.

### 2026-09-24 (parity-sweep) 16 new resource types: 192 -> 208 supported types

Added AWS::EC2::{TransitGateway,TransitGatewayAttachment,TransitGatewayRouteTable,
TransitGatewayRoute}, AWS::IAM::{AccessKey,ServiceLinkedRole,UserToGroupAddition},
AWS::ServiceDiscovery::{PrivateDnsNamespace,HttpNamespace,PublicDnsNamespace,Service,Instance}
(wired the servicediscovery backend into ServiceBackends/BackendsProvider -- CLI already had
GetServiceDiscoveryHandler), AWS::Route53::RecordSetGroup, AWS::Logs::Destination, and
AWS::SQS::QueueInlinePolicy / AWS::SNS::TopicInlinePolicy, each provisioning through its real
service backend with Ref/GetAtt verified against the CloudFormation docs (see
resources_more_managed_types2.go, resources_servicediscovery.go). Fixed a real cross-type bug
found along the way: AWS::IAM::User/Group's Ref returns an ARN in this backend, so a template's
`{"Ref": "MyUser"}` feeding an AccessKey's UserName or a UserToGroupAddition's GroupName/Users
arrived as an ARN where IAM's own APIs need a bare name -- createIAMAccessKey/
createIAMUserToGroupAddition now unwrap it (iamNameFromRefValue). Also found and fixed a
delete-time correctness gap that predates this pass: ResourceCreator.Delete only ever received
a resource's raw (unresolved) Properties, so a `{"Ref": "OtherResource"}` property read during
deletion (IAM AccessKey's UserName, UserToGroupAddition's GroupName/Users, ServiceDiscovery
Instance's ServiceId) resolved to the referenced resource's *logical* ID instead of its physical
ID. Delete's signature now also takes a `stackPhysicalIDs` snapshot (built by its 4 callers in
stacks.go from the stack's live StackResources) so these Refs resolve correctly; this is a
general fix, not scoped to the 3 new callers. Skipped for this pass (budget): AWS::CodeDeploy::*
(3 types, backend exists but not yet wired into ServiceBackends), AWS::EKS::{FargateProfile,Addon}
(2 types). Census of all still-missing AWS::<emulated-service>::* types, from the real
CloudFormationResourceSpecification.json, at scratchpad census/cfn_types_missing.json
(1057 missing, rough commonality-ranked).

### 2026-09-24 topoSortResources infers dependencies from Ref/Fn::GetAtt/Fn::Sub, not just DependsOn

Create (CreateStack, UpdateStack's new resources) and delete (DeleteStack, UpdateStack's stale
cleanup) now order by a real dependency graph (resource_deps.go); a cycle now errors instead of silently reordering.

### 2026-09-24 (parity-sweep) 7 new resource types: 184 -> 191 supported types

Added AWS::EC2::LaunchTemplate, AWS::EC2::VPCEndpoint, AWS::AutoScaling::{ScalingPolicy,
ScheduledAction,LifecycleHook}, AWS::IAM::OIDCProvider, AWS::RDS::DBProxy, each
provisioning through its real service backend with correct Ref/GetAtt (see resources_
more_managed_types.go). Found but did not fix a pre-existing gap: topoSortResources only
honors explicit DependsOn, never infers ordering from Ref/Fn::GetAtt, so templates whose
resources sort alphabetically out of dependency order (e.g. "Endpoint" before "VPC") fail
CreateStack unless DependsOn is added explicitly -- unlike real CloudFormation.

### 2026-09-24 (parity-sweep) DeleteStack already-gone child resource

CI's apigatewayv2 terraform fixture hit DELETE_FAILED when a child
Integration/Route/Stage was deleted directly before DeleteStack ran. Fixed
in resources_apigatewayv2.go (see DeleteStack ops note); tested via
TestDeleteStack_APIGatewayV2ChildAlreadyDeleted.

Protocol: AWS query/XML (`Action=...` form POST, `<FooResponse>` root, `ResponseMetadata>RequestId`).
Errors always serialize as HTTP 400 with `<ErrorResponse><Error><Code>/<Message></Error></ErrorResponse>`
(see `xmlError` in handler.go) — this project doesn't vary HTTP status by error code, matching how
CloudFormation's query protocol reports client errors.

### Verified against the actual SDK model, not assumption

- `aws-sdk-go-v2/service/cloudformation@v1.71.7`'s `types/errors.go` models a `StackNotFoundException`
  with `ErrorCode() == "StackNotFoundException"`, but grepping `deserializers.go` shows it is only ever
  switched on for **ImportStacksToStackSet**. Every other operation that can fail with "stack doesn't
  exist" (CreateStack's implicit lookups, UpdateStack, DeleteStack, DescribeStacks, GetTemplate,
  DescribeStackEvents, ...) has no modeled not-found exception at all, so the real API surfaces those as
  a generic `ValidationError`. This codebase already did that correctly everywhere except it had
  `ErrStackNotFound` as a hard *error* for DeleteStack, which leads to the next point.
- `DeleteStack`'s SDK-generated `awsAwsquery_deserializeOpErrorDeleteStack` has **no error cases at all**
  besides `TokenAlreadyExistsException` — confirming DeleteStack is fire-and-forget/idempotent in real
  AWS (deleting a stack that doesn't exist, or was already deleted, is a silent success). The backend
  previously returned `ErrStackNotFound` for this case; fixed to a no-op.
- `ChangeSetNotFoundException.ErrorCode()` returns `"ChangeSetNotFound"` (no "Exception" suffix), and
  `deserializers.go`'s `case strings.EqualFold("ChangeSetNotFound", errorCode)` confirms the wire code the
  real client matches on. Three handlers (`DescribeChangeSet`/`ExecuteChangeSet`/`DeleteChangeSet`) were
  emitting `"ChangeSetNotFoundException"` — a code the SDK client would never recognize, falling through
  to a generic `smithy.GenericAPIError`. Fixed to the exact modeled code.
- `InvalidChangeSetStatusException.ErrorCode()` returns `"InvalidChangeSetStatus"`. `ExecuteChangeSet` had
  no status gate at all before this pass — it would happily "execute" a change set whose
  `ExecutionStatus` was `UNAVAILABLE` (e.g. one created with zero net changes, which AWS marks
  FAILED/UNAVAILABLE at creation time). Fixed to reject with this exact code, matching AWS's documented
  `ExecuteChangeSet operation failed. Only failed changesets should have an execution status of
  UNAVAILABLE.`-class behaviour.
- The `ExecuteChangeSet` API doc comment in the SDK source states: "When you execute a change set,
  CloudFormation deletes all other change sets associated with the stack because they aren't valid for
  the updated stack." The backend previously deleted only the just-executed change set. Fixed to clear
  the whole per-stack change-set map on success.
- Export-in-use protection ("Export X cannot be deleted as it is in use by Y") was completely absent —
  neither `DeleteStack` nor `UpdateStack` checked whether a stack's export was still referenced via
  `Fn::ImportValue` by another active stack before removing it. This is one of the concretely-named
  high-value gaps in the audit brief. Added `stackExportsInUse` (shared helper, reuses the same
  `collectImportValues` machinery `ListImports` already uses) wired into both `deleteStackLocked` and a
  new `validateExportsStillInUse` pre-flight check in `applyTemplateToStack`. The update-side check is a
  *pre-update* approximation (computed from the pre-update physical-ID snapshot, mirroring where
  `validateImportValues` already runs) rather than a full two-pass plan/apply — acceptable given
  CloudFormation's own pre-flight validation model, but note if a future change makes an export's value
  depend on a resource newly created *by the same update*, this check won't see that value change.
- `CAPABILITY_AUTO_EXPAND` was incorrectly accepted as satisfying the `AWS::IAM::*` capability
  requirement in `requireIAMCapability`. Per AWS docs, `CAPABILITY_AUTO_EXPAND` only authorizes
  macro/transform expansion (e.g. SAM) — it does not grant permission to create IAM resources declared
  directly in the template. Fixed to only accept `CAPABILITY_IAM`/`CAPABILITY_NAMED_IAM`.
- `DescribeStackEvents` ignored `NextToken` and `addEvent`'s own 1000-event-per-stack cap entirely,
  returning the full history in one response. Changed the `StorageBackend` interface method to
  `DescribeStackEvents(nameOrID, nextToken string) (page.Page[StackEvent], error)`, reusing
  `pkgs/page.New` exactly like `ListStacks`/`ListChangeSets`/`ListExports` already do, and now surfaces
  `NextToken` in the XML response. This changed the interface signature — all backend/handler test call
  sites in this package were updated to match (`p, err := b.DescribeStackEvents(name, token); events :=
  p.Data`).
- (2026-07-11 sweep) `DeleteStackSet`'s SDK-generated `awsAwsquery_deserializeOpErrorDeleteStackSet`
  models exactly `{OperationInProgressException, StackSetNotEmptyException}` — no not-found case, same
  pattern as the earlier `DeleteStack` finding above. The backend previously returned
  `ErrStackSetNotFound` (surfaced as `StackSetNotFoundException`) when deleting a StackSet name that
  never existed; fixed to a silent no-op, matching AWS's DeleteStack-style idempotency for this op.
- (2026-07-11 sweep) Spot-audited the previously fully-deferred Stack Refactor family
  (`CreateStackRefactor`/`DescribeStackRefactor`/`ExecuteStackRefactor`/`ListStackRefactors`/
  `ListStackRefactorActions`) against their deserializers. Four of the five have a genuinely *empty*
  modeled error switch (fire-and-forget, same class as `DeleteStack`) — their existing no-op-on-unknown-ID
  behaviour is correct. The exception is `DescribeStackRefactor`, whose deserializer models
  `StackRefactorNotFoundException` — the one op in this family that's NOT fire-and-forget. The backend
  was returning `("", nil)` for an unknown `StackRefactorId`, which the handler serialized as a 200 with
  an empty `<Status/>` — a disguised-stub pattern (real-looking op, but an unpopulated lookup silently
  "succeeding" per parity-principles.md rule 4). Fixed: added `ErrStackRefactorNotFound`, backend now
  returns it, handler maps it to `StackRefactorNotFoundException`.
- (2026-07-11 sweep) Spot-audited the previously fully-deferred Generated Templates and Resource Scans
  families. `GeneratedTemplateNotFoundException.ErrorCode()` and `ResourceScanNotFoundException.ErrorCode()`
  both return their **unsuffixed** wire code (`"GeneratedTemplateNotFound"` / `"ResourceScanNotFound"`) —
  the exact same bug class already found and fixed for `ChangeSetNotFound` in an earlier sweep, but it had
  independently recurred here. Four Generated Template handlers
  (`Update`/`Delete`/`Describe`/`GetGeneratedTemplate`) and one Resource Scan handler
  (`DescribeResourceScan`) were all emitting the `...Exception`-suffixed code, which
  `aws-sdk-go-v2` clients never match against the typed exception (falls back to a generic
  `smithy.GenericAPIError`). Fixed all five to the exact modeled code. Additionally,
  `ListResourceScanResources` and `ListResourceScanRelatedResources` were discarding
  `Backend.ListResourceScanResources`/`ListResourceScanRelatedResources`'s returned error with a bare
  `_`, so an unknown `ResourceScanId` silently produced a 200 with an empty list instead of
  `ResourceScanNotFound` — a disguised-stub pattern per parity-principles.md rule 4 (a `List*` op that
  looks real because it calls into backend logic, but the specific not-found branch was unreachable).
  Fixed both to surface the error.
- (2026-07-23 sweep) **Persistence gap (largest finding this pass):** `store_setup.go`'s
  `registerAllTables` doc comment lists 15 backend map fields deliberately left as plain maps (not
  `store.Table`-registered) because they're nested/one-to-many. Of those 15, only 4
  (`events`/`resources`/`changeSets`/`stackPolicies`) were actually wired into `backendSnapshot` in
  `persistence.go` — the other 10 (`stackInstances`, `stackSetOperations`, `typeConfigs`,
  `handlerProgress`, `signals`, `stackSetOpResults`, `typeVersions`, `resourceScanItems`,
  `resourceDriftStatus`, `resourceDriftDetail`) were never persisted at all. This is silent data loss on
  every restart/restore: StackSet instances/operations, the entire type registry's configuration/version
  history, per-resource drift detail, resource-scan findings, and custom-resource signals all vanished
  across a snapshot/restore cycle. Fixed by adding all 10 fields to `backendSnapshot` with the same
  nil-fallback-on-restore pattern the existing 4 use (`applyNilDefaults`, split out of `Restore` to keep
  it under golangci-lint's cyclop threshold). `driftByStackID` (a reverse index) is deliberately NOT
  persisted directly — like `stackIDIndex`, it's rebuilt from its persisted source table
  (`driftDetections`) in `Restore` (`rebuildDerivedIndexes`), so it can never drift out of sync with the
  data it indexes. Verified via `TestInMemoryBackend_SnapshotRestore_PlainMapFields`.
- (2026-07-23 sweep) **`CreateChangeSet` Capabilities gap:** `CreateChangeSet`'s signature never accepted
  a `Capabilities` parameter at all, despite the real `CreateChangeSetInput.Capabilities` field (and the
  fact that `DescribeChangeSet`'s real output also returns `Capabilities`). Because `ExecuteChangeSet`
  applies a change set by calling `UpdateStack`/`CreateStack` with `StackOptions{}` (zero capabilities),
  this meant ANY change set touching IAM resources could never actually be executed — `ExecuteChangeSet`
  would always hit `requireIAMCapability`'s `InsufficientCapabilities` gate inside `UpdateStack`/
  `CreateStack`, regardless of what capabilities the caller declared at `CreateChangeSet` time. Fixed:
  `ChangeSet` gained a `Capabilities []string` field, `CreateChangeSet` now accepts and stores it (wired
  from the `Capabilities.member.N` form field via the existing `parseCapabilities` helper), and
  `ExecuteChangeSet` now passes `StackOptions{Capabilities: cs.Capabilities}` instead of an empty
  `StackOptions{}`. `DescribeChangeSet`'s response also gained the `Capabilities` field it was missing.
  Verified via `TestChangeSet_Capabilities_ThreadedToExecute` (execute now succeeds with `CAPABILITY_IAM`
  on an IAM-touching template, fails with `InsufficientCapabilities` without it — both cases previously
  behaved identically to "without it" since the parameter was discarded).
- (2026-07-23 sweep) **`DetectStackSetDrift` was a disguised stub:** it recorded a real `SUCCEEDED`
  `StackSetOperation` (so it looked functional in any test that only checked the operation record) but
  never actually compared any stack instance's provisioned child-stack resources against its template —
  every instance's `DriftStatus` stayed `NOT_CHECKED` forever, no matter how many times drift detection
  ran. Fixed: added `detectStackInstanceDrift`, which reuses the exact same `compareStackResources` logic
  the standalone per-stack `DetectStackDrift` already used, resolving each instance's `StackID` back to
  its child stack via `stackIDIndex` and updating `DriftStatus` to `IN_SYNC`/`DRIFTED` in place. Verified
  via `TestStackSetDrift_UpdatesInstanceDriftStatus`, which simulates an out-of-band mutation on a child
  stack's resource (`RecordResourceMutation`) and confirms the instance's `DriftStatus` actually flips to
  `DRIFTED` on re-detection (it previously never would have, at any capability level).
- (2026-07-23 sweep) **YAML short-form intrinsics were silently broken, not merely unverified:**
  `ParseTemplate`'s YAML branch and `parseGenericTemplate` both called `gopkg.in/yaml.v3`'s `Unmarshal`
  directly into typed structs / a bare `map[string]any`. yaml.v3 silently discards any custom YAML tag it
  doesn't recognize as a built-in and decodes only the tagged node's native scalar/sequence/mapping
  content — confirmed via a standalone repro: `!Ref MyParam` decoded to the bare Go string `"MyParam"`,
  and `!Sub "${AWS::StackName}-bucket"` decoded to the raw unresolved string, with **no error and no
  indication anything was lost**. Every `resolveValue`-family consumer in this package expects the
  long-form map representation (`{"Ref": "MyParam"}`, `{"Fn::Sub": "..."}`), so every YAML short-form
  intrinsic — `!Ref`, `!GetAtt`, `!Sub`, `!Join`, `!Select`, `!Split`, `!Base64`, `!Cidr`, `!ImportValue`,
  `!GetAZs`, `!FindInMap`, `!And`, `!Or`, `!Not`, `!Equals`, `!If`, `!Condition`, `!Transform` — silently
  degraded to a dead literal string instead of resolving or erroring, for every template written in YAML
  short form (the common case for hand-written CloudFormation YAML). Fixed via `yamlToJSON`/
  `normalizeYAMLNode`/`normalizeYAMLNodeValue`, which walk the raw `*yaml.Node` tree (so tag information
  is never lost) and expand short-form tags into their long-form map representation before an ordinary
  JSON round-trip through the existing, already-correct JSON-path logic. `!GetAtt`'s dotted scalar short
  form (`LogicalId.Attribute`) is split into the long-form two-element list. Both `ParseTemplate` and
  `parseGenericTemplate` (used by the `Fn::ForEach` language-extension expander) now share this path, so
  a YAML template combining `Fn::ForEach` with short-form intrinsics resolves both correctly together.
  Verified via `TestParseTemplate_YAMLShortFormIntrinsics` (shape-level, all tag kinds) and
  `TestCreateStack_YAMLShortFormIntrinsics_Resolve` (end-to-end: `!Ref`/`!Sub` actually resolve through
  `CreateStack`/`DescribeStacks` `Outputs`, not just at the parse-tree level).

### Traps for the next auditor (looks-wrong-but-correct)

- `createStackFromTemplate`/`applyTemplateToStack` deliberately leave a stack in `CREATE_FAILED` /
  `UPDATE_FAILED` (not `ROLLBACK_COMPLETE` / `UPDATE_ROLLBACK_COMPLETE`) when `ParseTemplate` itself fails
  (malformed JSON/YAML), while every other pre-flight validator (`ValidateParameters`,
  `validateIntrinsics`, `validateImportValues`, and now `validateExportsStillInUse`) transitions all the
  way through to `*_ROLLBACK_COMPLETE`. This asymmetry is intentional and mirrors two genuinely different
  AWS failure classes; don't "fix" it into uniformity. The only real bug here (now fixed) was that the
  `UPDATE_FAILED` branch was silently missing its stack-level `addEvent` call — `CREATE_FAILED`'s sibling
  branch already had it.
- `computeChanges`/`CreateChangeSet` marking a same-template change set `Status: FAILED,
  ExecutionStatus: "UNAVAILABLE"` is correct AWS behaviour (a change set with zero net changes), not a
  bug — a stale unit test (`TestBackend_ExecuteChangeSet/existing_stack`) previously exercised this exact
  case with `wantErr` unset only because `ExecuteChangeSet` had no status gate; it now uses
  `modifiedTemplate` (a real diff) and a new sibling case
  (`existing_stack_no_changes`) explicitly covers the FAILED/blocked path.
- `ExecuteChangeSet`'s two lock windows (state-check-and-flip, then unlocked `UpdateStack`/`CreateStack`
  call, then re-lock to finalize) are pre-existing and intentional — `UpdateStack`/`CreateStack` take
  `b.mu` themselves, so holding it across the call would deadlock. Not touched.
- (2026-07-23 sweep) `Template.UnmarshalYAML`/`TemplateResource.UnmarshalYAML` use the OLD-style
  `gopkg.in/yaml.v3` "obsolete unmarshaler" interface (`func(unmarshal func(any) error) error`), which
  yaml.v3 keeps for backward compat with yaml.v2 code — do NOT "modernize" these to the `*yaml.Node`-based
  interface without checking every call site; they're intentionally written this way to match the
  pre-existing `TemplateResource` pattern. Separately: an embed-based JSON alias trick (`type plain T;
  struct { Extra X; *plain }`) works fine for `UnmarshalJSON` but silently drops every promoted field on
  the YAML path — yaml.v3 does not auto-promote fields from an anonymously embedded pointer the way
  `encoding/json` does. `Template`'s `UnmarshalJSON`/`UnmarshalYAML` both decode into `templatePlain`
  (all fields listed explicitly, both `json` and `yaml` tags) for exactly this reason; don't refactor
  toward the embed trick even though it looks like less boilerplate. Both `Template.UnmarshalJSON` and
  `Template.UnmarshalYAML` are effectively dead code paths now that `ParseTemplate`'s YAML branch
  round-trips through `yamlToJSON` first (so only `UnmarshalJSON` actually runs at parse time) — left in
  place rather than deleted, since they're exported `yaml.Unmarshaler`/`json.Unmarshaler` implementations
  a caller outside this package's `ParseTemplate` could still reasonably invoke directly.

## 2026-08-22: gopherstack-r80d batch 26 -- required-output-member sweep

Module resolves directly: `services/cloudformation` -> `aws-sdk-go-v2/service/cloudformation@v1.76.1`
(no `dirModuleOverride` entry).

Instrument validated three ways before trusting the ranking: the existing
`cmd/requiredoutputfields` char-level brace matcher, a fresh standalone
`go/parser`/`go/ast` walk, and a raw `grep -c "This member is required."
api_op_*.go` sanity total. All three agreed on the op-level shape (10
required fields / 90 ops / 7 ops-with-required); the grep-c total (90)
also counts required *input* members across the same files, as expected.

10 required output fields / 7 ops-with-required splits into two unrelated
families: the standalone drift-detection quartet (`DetectStackDrift`,
`DetectStackResourceDrift`, `DescribeStackDriftDetectionStatus`,
`DescribeStackResourceDrifts` -- none of which had an `ops:` table row at
all before this pass, now added above) and the stack-refactor family
(`CreateStackRefactor`, `ListStackRefactors`, `ListStackRefactorActions`).
Domain-struct depth: `DescribeStackResourceDrifts`/`DetectStackResourceDrift`
both wrap `types.StackResourceDrift`, which itself carries 5 required
members (`LogicalResourceId`/`ResourceType`/`StackId`/`StackResourceDriftStatus`/
`Timestamp`) invisible to the flat per-op scan -- confirmed all 5 always
populated on every reachable path (the normal `compareStackResources`
result and the template-parse-failure fallback in `driftDetailFor`), with
no `xml:",omitempty"` on any of them in `driftXML`. By contrast,
`ListStackRefactors`/`ListStackRefactorActions` wrap `types.StackRefactorSummary`/
`types.StackRefactorAction`, both confirmed via the same AST walk to declare
**zero** required members in the real SDK model -- so for this family the
flat op-level count already is the complete required surface, no undercount
(the opposite of the drift-quartet's shape, and worth recording since a
"one wrapper key" shape doesn't always hide more depth -- it depends on
what the real Smithy model marks required on the wrapped type, not on the
shape alone; same lesson batch 22 drew for rolesanywhere).

0 bugs found. Read all 7 ops end to end against `handler_drift_detection.go`/
`drift_detection.go` and `handler_stack_refactors.go`/`stack_refactors.go`
directly (not grepped), including the shared `toDriftXML`/`driftDetailFor`
helpers and both of `DetectStackResourceDrift`'s code paths (template
parses vs. template-parse-failure fallback). Also spot-checked
`awsAwsquery_deserializeDocumentStackResourceDrift` in the pinned SDK's
`deserializers.go` directly to confirm element-name matching is
case-insensitive (`strings.EqualFold`), so this package's XML tag casing
carries no wire-shape risk independent of the required-field question.

Gates: `go build ./...` (repo-wide, clean except the pre-existing untouched
`services/sagemaker/*` dirt from a concurrent agent's in-flight conversion),
`go vet ./services/emr/... ./services/cloudformation/...` clean, `gofmt -l`
clean, `go test -race ./services/cloudformation/...` passing (10.9s),
`golangci-lint run ./services/emr/... ./services/cloudformation/...` 0
issues. No source changes to this service this batch -- only `PARITY.md`
and `services/_REQUIRED_OUTPUT_CANDIDATES.md` were touched; the batch's one
counted bug was in the sibling `emr` service (tied with cloudformation for
largest remaining candidate after sagemaker), not here.

**2026-08-22 (gopherstack-ifzn) -- RouteMatcher swallowed a body-read failure as a 404,
masking Handler()'s already-typed InternalFailure**: same shape as autoscaling's entry
above (see that entry or gopherstack-3a8t for the full survey/rationale). `RouteMatcher`
now falls back to `service.MatchesUserAgentMarker(r.Header, "api/cloudformation")`
(verified against the pinned `cloudformation@v1.76.1/api_client.go:641`
`AddSDKAgentKeyValue` call) only on the `ReadBody` failure branch. Migrated
`ExtractOperation`/`ExtractResource`/`Handler()` off `r.ParseForm()` onto
`httputils.ReadBody`+`url.ParseQuery`. Handler()'s read-failure branch was also
retyped from `InvalidParameterValue` (wrong -- that's a caller-fault code, not a
server-side read failure) to the service's own existing `InternalFailure` code
(already used elsewhere in this handler, e.g. `handler_type_registry.go`).

Two package test helpers pre-drained the request body via their own
`req.ParseForm()` call before invoking `Handler()`/`ExtractOperation`/`ExtractResource`
directly (`handler_testutil_test.go`'s `postForm`, and two subtests in
`handler_core_test.go`) -- valid only while those functions also called
`r.ParseForm()` themselves (a second, idempotent-on-success call). Once migrated to
`httputils.ReadBody`, that pre-drain left the body empty by the time `ReadBody` ran,
breaking three previously-passing tests (`TestHandler_DetectStackResourceDrift/success`,
`TestHandler_ExtractOperation/describe_stacks`, `TestHandler_ExtractResource/stack_name`).
Fixed by removing the now-redundant pre-drain calls in those three tests --
production traffic never called `req.ParseForm()` before `Handler()` either, so this
brings the tests back in line with real request handling rather than working around it.

Proof: `TestHandler_OversizedBodySurfacesInternalFailure` in
`handler_oversized_body_test.go` drives a real cloudformation SDK client through
`service.NewRegistry`/`service.NewServiceRouter`, confirmed failing pre-fix with
`UnknownError`; passes now with `InternalFailure`. `TestHandler_NormalSizedBodyStillRoutes`
is the regression guard. Gates: `go build`, `go vet`, `gofmt -l` (clean),
`go test -race ./services/cloudformation/...` (pass), `golangci-lint run
./services/cloudformation/...` (0 issues).

**2026-08-29 -- ERROR PATH verified: per-op error code choice audited against
`cloudformation@v1.76.1`'s 90 `deserializeOpError<Op>` switches (0/90 model
anything for EC2-style generic fallback -- this service DOES model typed
per-op exceptions, unlike EC2's 785-op all-generic switch checked in the same
sweep).** 3 bugs fixed, all the "codes emitted but a real client can't
`errors.As` into them for this op" shape:

1. `GetHookResult` (`hooks.go`) returned `"SUCCEEDED", nil` for any unknown
   `HookResultId` instead of raising `HookResultNotFound` (modeled by this op's
   own deserializer). Compounding wire-field bug fixed alongside it:
   `handler_hooks.go` read `HookResultToken`, a field that doesn't exist on the
   wire -- the real `GetHookResultInput` field is `HookResultId`
   (`serializers.go:8480`) -- so every real-client call missed the lookup and
   hit the always-SUCCEEDED path regardless of whether the ID was valid.
2. `DescribeStackInstance` (`stack_instances.go`) never checked whether the
   `StackSetName` itself existed, so an unknown stack set surfaced
   `StackInstanceNotFoundException` instead of `StackSetNotFoundException` --
   both are modeled by this op's own deserializer, so the correct code was
   directly establishable, not a leave-it case.
3. `ListStackSetOperationResults` (`stack_sets.go`) never returned an error at
   all -- an unknown `StackSetName` or `OperationId` silently returned an empty
   `Summaries` list (HTTP 200) instead of `StackSetNotFoundException` /
   `OperationNotFoundException`, both modeled by this op.

A pre-existing test (`hooks_test.go`'s `TestHookResults`) asserted the old
`GetHookResult`-always-succeeds behavior as correct (`"GetHookResult — unknown
token returns SUCCEEDED (no error)"`); updated to assert the real
`HookResultNotFound` 400.

**Left unfixed, no correct code establishable from the deserializer (RESTRAINT --
do not invent a code):** three more asymmetries surfaced by the same per-op
audit, all a real, SDK-modeled exception name emitted by a `Delete`/`Execute` op
whose *own* deserializer switch models nothing for that failure at all (so
neither the current code nor any alternative can be shown correct or incorrect
from the SDK alone):
- `DeleteChangeSet` emits `"ChangeSetNotFound"` (modeled only by
  `DescribeChangeSet`/`DescribeChangeSetHooks`/`ExecuteChangeSet`/`GetTemplate`,
  not by `DeleteChangeSet` itself).
- `ExecuteStackRefactor` emits `"StackRefactorNotFoundException"` (modeled only
  by `DescribeStackRefactor`).
- `DeleteStackSet` emits `"StackSetNotFoundException"` for any non-"not empty"
  failure (modeled by many sibling ops -- `DescribeStackSet`,
  `ListStackInstances`, etc. -- but `DeleteStackSet`'s own deserializer models
  only `OperationInProgressException`/`StackSetNotEmptyException`).

Also noted, out of the error-code class and left: `SetTypeConfiguration` and
`DescribeType`'s registry-miss fallback path never raise `TypeNotFoundException`
(the latter is a documented deliberate convenience fallback, not an oversight);
`ListHookResults` reads wire fields (`HookResultToken`) that don't exist on the
real `ListHookResultsInput` (real fields are `TargetId`/`TargetType`/`TypeArn`/
`Status`/`NextToken`) -- a wire-shape bug, different class, not touched.
`CreateStack`/`UpdateStack`/`DeleteStack`/`RollbackStack`/`ExecuteChangeSet` all
model `TokenAlreadyExistsException` for `ClientRequestToken` reuse, which this
backend doesn't track at all (no idempotency-token infrastructure exists) --
a feature gap, not a sentinel-choice bug, out of proportion to fix in this
sweep.

Gates: `go build ./services/cloudformation/...`, `go vet ./...` (repo-wide;
clean except a pre-existing `services/appconfig` vet failure from a
concurrently-edited service, not this one), `go test -race -count=1
./services/cloudformation/...` (pass), `golangci-lint run --fix
./services/cloudformation/...` (0 issues).

## 2026-08-29: discarded-error sweep -- stack lifecycle unconditionally reported success on resource deletion failure

Campaign-wide hunt for the class where a client-visible failure is discarded
(`_`) instead of reaching its designated place in the response.
`services/cloudformation`'s resource-deletion calls (`stacks.go`'s
`b.creator.Delete(ctx, res.Type, res.PhysicalID, res.Properties)`, which
dispatches to the real per-service backend, e.g. `deleteS3Bucket` ->
`s3.DeleteBucket`, which genuinely fails with `BucketNotEmpty` on a
non-empty bucket) were assigned to `_` at all four places `DeleteStack`/
`CreateStack`/`UpdateStack` delete a resource, and every one of the stack's
four terminal statuses that exist specifically to report this
(`StackStatusDeleteFailed`, `StackStatusRollbackFailed`,
`StackStatusUpdateRollbackFailed` -- confirmed present in the pinned SDK's
`types/enums.go`, cloudformation@v1.76.1) or that already existed but were
unreachable (`statusUpdateFailed` for a stale-resource cleanup failure) were
never actually set. **A stack whose resource genuinely failed to delete was
unconditionally reported `DELETE_COMPLETE`/`ROLLBACK_COMPLETE`/
`UPDATE_COMPLETE`, and the resource itself was dropped from
`DescribeStackResources` even though it still exists.** This is the
`sesv2 SendBulkEmail` shape exactly: the failure had a designated place
(`StackStatus`, which this same code already sets correctly for a dozen
other failure modes) and was not put there.

**Four call sites fixed**, all in `stacks.go`:
- `deleteStackLocked` (`DeleteStack`) -- now sets `DELETE_FAILED` +
  `StackStatusReason` when any resource delete fails, keeps the stack (and
  its still-undeleted resources/events) fully describable for a retry
  instead of purging `b.resources`/`b.events`/`b.stackPolicies`/
  `b.changeSets` as the success path does.
- `rollbackCreateResources` (`CreateStack`'s automatic rollback) -- now
  returns whether every rollback delete succeeded; `provisionResources` sets
  `ROLLBACK_FAILED` instead of `ROLLBACK_COMPLETE` when it didn't, leaving
  the undeleted resource registered.
- `deleteStaleResources` (`UpdateStack`, resources removed from the new
  template) -- now returns success/failure; `updateResources` sets
  `UPDATE_FAILED` instead of proceeding to `UPDATE_COMPLETE` when a stale
  resource can't actually be removed.
- `rollbackUpdateResources` (`UpdateStack`'s automatic rollback) -- same
  shape as the CreateStack case, sets `UPDATE_ROLLBACK_FAILED` instead of
  `UPDATE_ROLLBACK_COMPLETE`.

**A second, dependent bug found while fixing the first**: `createStackLocked`
gated "did CreateStack succeed" on `stack.StackStatus == statusCreateFailed
|| stack.StackStatus == statusRollbackComplete` at two call sites (deciding
whether to overwrite the status with `CREATE_COMPLETE`, and whether to skip
export resolution). Introducing the reachable `ROLLBACK_FAILED` value broke
both: an initial (uncaught) run of the new
`TestBackend_CreateStack_RollbackDeleteFails` produced `CREATE_COMPLETE`
even though `provisionResources` had already correctly set
`ROLLBACK_FAILED` and recorded the right `StackStatusReason` -- the
success-path code simply didn't recognize the new failure status as a
failure and clobbered it. Fixed by replacing both enumerated checks with a
single `isFailedCreateStatus` helper covering all three failure statuses.
This is the shape the campaign brief calls out explicitly: adding a new
terminal status is a ripple change, and every place that gates on "did this
fail" by enumerating known failure statuses (rather than a single
success/failure boolean, as `UpdateStack`'s parallel `applyTemplateToStack
bool` gate already does -- that one needed no fix) is a place the ripple can
be missed.

**Deliberately left alone**: `createStackLocked`'s `OnFailure == "DELETE"`
block (lines ~295-308) still checks only `statusCreateFailed ||
statusRollbackComplete`, not `statusRollbackFailed` -- if automatic rollback
already failed to delete a resource, this block's own unconditional-success
inline deletion (a fifth, smaller instance of the same discarded-error
pattern, not touched this pass) would make it worse, not better, to run.
Left as `ROLLBACK_FAILED` for the caller to inspect/retry rather than
extended to paper over a failed rollback with a fabricated `DELETE_COMPLETE`.

**Confirmed same class, disclosed not fixed**: `stack_instances.go:177`'s
`deleteMatchingStackInstances` discards `b.deleteStackLocked`'s (now rarer,
but still real for e.g. `ErrTerminationProtectionEnabled`) error and
unconditionally drops the instance from `b.stackInstances[stackSetName]`
regardless of whether the child stack's deletion actually succeeded --
same shape, at the stack-set-instance level. Not fixed this pass: doing so
correctly requires first confirming what `StackInstance`'s own status field
should read on a failed teardown (`INOPERABLE` vs leaving it in the list),
which needs its own read of the SDK's `StackInstanceStatus` semantics before
touching it.

Proof: `TestBackend_DeleteStack_ResourceDeleteFails`,
`TestBackend_CreateStack_RollbackDeleteFails`,
`TestBackend_UpdateStack_StaleResourceDeleteFails` (`stacks_test.go`) drive
`DeleteStack`/`CreateStack`/`UpdateStack` end-to-end against a real S3
backend, using a non-empty bucket to force a genuine `BucketNotEmpty`
deletion failure, and assert the resulting `StackStatus` plus that
`DescribeStackResource` still finds the undeleted resource.
`TestBackend_RollbackUpdateResources_DeleteFails` drives
`rollbackUpdateResources` white-box (`RollbackUpdateResourcesForTest`,
`export_test.go`) because `updateResources` creates newly-added resources by
iterating a Go map, making which of two new resources is created first --
and therefore whether it's even in `created` when a sibling fails --
non-deterministic through the public API alone. All four confirmed failing
(reporting the wrong `*_COMPLETE` status, resource silently dropped) against
the pre-fix code before the fix landed.

Gates: `go build ./services/cloudformation/...`, `go vet ./...` (repo-wide,
clean), `go test -race -count=1 ./services/cloudformation/...` (pass),
`golangci-lint run ./services/cloudformation/...` (0 issues).

## Map-walk pagination sweep (2026-08-30, fix/wrapper-key-sweep-rds-cloudwatch-sqs-sns)

Audited every `sort.Slice`/`sort.Strings` call and every `pkgs/page.New`
call site in `services/cloudformation` for the "sort on a tie-prone field
(or no sort at all) over a `store.Table.All()`/raw-Go-map walk, unstable
between calls" bug class. Discriminator: `.All()` on a `store.Table`, or
ranging a raw `map[string]V`/`map[string][]V` by every key, is the bug
source; `store.Table.Snapshot()` (deterministic, key-sorted) and a direct
per-key slice lookup into a raw map (`b.someMap[key]`) are stable across
calls and were left alone even where the sort key itself was tie-prone.

Every `page.New` call in this service hardcodes `cfnDefaultPageSize` (100)
as the limit — none of these ops take a client-supplied page-size input, so
a walk needed >100 records to force a page boundary at all (this matches
real AWS: CloudFormation's List/Describe ops for stacks/stack
sets/exports/etc. genuinely have no MaxResults-equivalent input, confirmed
by no handler in this service even reading one — not a parity gap).

**Bugs found and fixed** (each proven first with 110+ records, or a
constructed tie, walked in pages of 100, 30 iterations, confirmed failing
against unmodified code on iteration 0):

- `ListResourceScans` (generated_templates.go) — no sort at all over
  `resourceScans.All()`. Fixed: sort by `ResourceScanID` (table key).
- `ListGeneratedTemplates` (generated_templates.go) — sorted by
  `GeneratedTemplateName` alone over `generatedTemplates.All()`;
  `CreateGeneratedTemplate` never checks Name for uniqueness. Fixed: added
  `GeneratedTemplateID` (table key) as tiebreak.
- `ListStackSetOperations` (stack_sets.go) — sorted by `CreatedAt` alone
  over a raw `map[string]*StackSetOperation` walk
  (`b.stackSetOperations[stackSetName]`, keyed by operation ID); two
  operations created in the same instant tie. Fixed: added `OperationID`
  (`uuid`-derived, always unique) as tiebreak. Proven via a new
  `AddStackSetOperationInternal` test-seed helper (`export_test.go`)
  constructing 110 same-`CreatedAt` operations.
- `DescribeEvents` (stack_lifecycle.go), the no-`StackName`/all-stacks
  branch — sorted by `Timestamp` (descending) alone over a raw
  `map[string][]StackEvent` walk (`b.events`, keyed by stack ID); two events
  on different stacks sharing an exact Timestamp tie. This branch is really
  reachable: real `DescribeStackEvents` makes `StackName` optional and
  returns events across every stack when omitted, and this backend's own
  handler (`handleDescribeEvents`) passes `form.Get("StackName")` straight
  through, so an empty form field reaches it. Fixed: added `EventID`
  (`uuid`-derived) as tiebreak. Proven via a new `AddStackEventInternal`
  test-seed helper (`export_test.go`) constructing 120 same-Timestamp events
  spread across 4 stacks.

**Confirmed clean (tie-prone sort, but the key is already unique, or the
source is stable) — left unchanged, with the reason:**
- Every sort keyed on a `store.Table`'s own key field over `.All()`
  (`ListCollaborations`… no, that's cleanrooms — for cloudformation:
  `ListStacks`/StackName, `ListStackSets`/StackSetName,
  `ListTypes`/TypeName — also unpaginated, no NextToken anywhere on this
  op, so not even reachable by the bug pattern, `ListExports`/Name,
  `ListImports`/StackName over `stacks.All()`).
- `ListChangeSets` (change_sets.go) sorts a raw `map[string]*ChangeSet`
  walk by `ChangeSetName`, which is that inner map's own key (unique within
  a stack).
- `ListStackResources`/`DescribeStackResources` (stack_resources.go) sort a
  raw `map[string]*StackResource` walk by `LogicalResourceID`, which is
  that map's own key.
- `ListStackInstances` (stack_instances.go) has no sort at all, but reads
  `b.stackInstances[stackSetName]` — a direct per-key slice lookup, not a
  map walk — so insertion order is stable across calls.
- `evictDeletedStacks` (stacks.go) and `trimStackSetOperations`
  (stack_sets.go) are internal eviction/GC helpers, not customer-facing
  paginated listings (no NextToken, no page boundary a client ever walks) —
  a tie in their sort only affects *which* record gets evicted when a cap
  is exceeded, not a drop/duplicate across a page boundary, so left alone
  as out of scope for this bug class (same reasoning as ssm's non-existent
  equivalent, noted there).
- Every `sort.Strings` call (changeset_diff.go, exports.go, stacks.go,
  stack_sets.go) sorts scalar strings directly; two records that legitimately
  share a string value are indistinguishable at that value, so an unstable
  sort permuting their relative order produces byte-identical output either
  way — immune to this bug class by construction, unlike sorting structs by
  a display field.

**Existing-test gap**: no pre-existing test in this package constructed a
tie and walked pages asserting item-identity reproduction. New tests added
this pass (`generated_templates_test.go`, `stack_sets_test.go`) assert exact
reproduction of the full ID set across a 30-iteration page walk.

Gates: `go build ./services/cloudformation/...`, `go vet
./services/cloudformation/...`, `go test -race -count=1
./services/cloudformation/...` (pass), `golangci-lint run
./services/cloudformation/...` (0 issues).

## gopherstack-wl89: DeleteStackInstances teardown-failure divergence, fixed (2026-08-30)

Follow-up to the "discarded-error sweep" entry above, which disclosed but
did not fix `stack_instances.go:177`. Fixed this pass.

**`deleteMatchingStackInstances`** discarded `deleteStackLocked`'s error
*and* unconditionally excluded the instance from `filtered` regardless of
outcome, so a stack instance whose child-stack teardown failed vanished
from the StackSet as if the delete had succeeded — the caller had no way to
learn the child stack still existed. Real CloudFormation documents this
exact case on `StackInstanceStatus`
(cloudformation@v1.76.1 `types/types.go:1894`): *"INOPERABLE: A
DeleteStackInstances operation has failed and left the stack in an unstable
state."* Fixed by keeping the instance in `filtered` on a teardown error,
setting `Status = "INOPERABLE"` / `StatusReason = err.Error()` (matching the
literal convention `provisionStackInstance` already uses for a failed
child-stack *create*), and threading the per-(account,region) failure
through a new `recordStackInstanceDeleteResults`, which records `FAILED` +
`StatusReason` on the matching `StackSetOperationResult` (visible via
`ListStackSetOperationResults`, wire already carried `StatusReason`, no wire
change needed) and flips the operation's own `Status` to `FAILED`
(`StackSetOperationStatus` enum, `types/enums.go:1742` — visible via
`DescribeStackSetOperation`, also no wire change needed).

Forced the failure through the real public API rather than a test hook: a
StackSet template with an `Export`, a stack instance created from it, then
a second, independent stack that imports that export via
`Fn::ImportValue`. `DeleteStackInstances` on the instance now hits the same
`ErrExportInUse` protection `DeleteStack` already enforces
(`exports.go`'s `stackExportsInUse`), which is a real, reachable failure
mode of `deleteStackLocked` completely independent of the `wl89` "not fixed
because it needs a hook" concern — `EnableTerminationProtection` isn't
reachable for a stack-instance's auto-provisioned child stack
(`provisionStackInstance` always passes `StackOptions{}`), but export-in-use
is.

Proof: `TestDeleteStackInstances_SurvivesFailedTeardown`
(`stack_instances_teardown_failure_test.go`), driven through the real
`aws-sdk-go-v2` client (`newTestHandlerAndClientWithBackend`). Confirmed
failing against the pre-fix code (instance not found after delete). Asserts
`DescribeStackInstance`/`ListStackInstances` still find the instance with
`types.StackInstanceStatusInoperable`, `DescribeStackSetOperation` reports
`types.StackSetOperationStatusFailed`, and `ListStackSetOperationResults`
reports `types.StackSetOperationResultStatusFailed` with a `StatusReason`
naming the blocking export.

**Type-registry "reports empty on failure" half of the same issue,
re-verified — status: was NOT actually reachable, defensive fix applied
anyway.** `wl89` names `ListTypes`/`ListTypeVersions`/`TestType`/
`RegisterPublisher` (plus, by the same grep, `ListTypeRegistrations` and
`SetTypeConfiguration`) as discarding their backend call's error
(`_, _ := h.Backend.Foo(...)`) in `handler_type_registry.go`. Read all six
backend methods (`type_registry.go`): every one of them has zero code paths
that return a non-nil error — `ListTypes`/`ListTypeVersions`/
`ListTypeRegistrations` fall back to an empty/full result instead of
erroring on an unknown type, and `TestType`/`RegisterPublisher`/
`SetTypeConfiguration` always succeed. So today the discard cannot actually
mask a real failure — this contradicts the type_registry `status: ok` note
above ("non-error-returning backend methods were reviewed and left as-is
... intentional"), which was right about *why* it's currently harmless but
should have said so instead of leaving `_, _ :=` in place. Wired proper
propagation anyway (`err != nil` → `h.xmlError(c, "CFNRegistryException",
err.Error())`, matching the error this family's own deserializer models for
every one of these ops per `deserializers.go`, and matching
`handleDescribeType`'s existing convention) so a future backend change that
adds a real failure mode (e.g. `TypeNotFoundException` for an unknown
`TypeName`, which real `ListTypeVersions`/`SetTypeConfiguration`/`TestType`
model but this backend doesn't implement) can't silently regress into
reporting an empty success again. Not independently unit-tested: doing so
would require adding a test-only failure hook to the production backend,
which is out of scope and explicitly the kind of fabricated reachability
this pass was told to avoid — `go build`/`go vet`/`go test -race`/
`golangci-lint run` (0 issues) all still pass with the change, and every
existing type-registry test still passes unchanged.

Untouched, per scope: `DeleteStackSet` (`stack_sets.go:128`, already
propagates correctly), and the MaxResults/NextToken parsing gap on
`ListTypes`/`ListTypeVersions`/`ListTypeRegistrations` and the stack-refactor
listings — filed separately, not part of this fix.

Gates: `go build ./services/cloudformation/...`, `go vet ./...` (repo-wide,
clean of cloudformation findings — other services on this branch have
unrelated in-progress failures), `go test -race -count=1
./services/cloudformation/...` (pass), `golangci-lint run
./services/cloudformation/...` (0 issues).

**2026-08-30 — value-semantics sweep (gopherstack-uox6), no bug found, one
regression test added.** Checked every optional filter that is actually read
by a handler for whether the code's empty-case matches its SDK doc comment's
own statement of what absence means: `ListStacks.StackStatusFilter`,
`ListStackInstances.{Filters,StackInstanceAccount,StackInstanceRegion}`,
`ListStackSets.Status`, `DescribeEvents.Filters.FailedEvents`. All four are
clean — each treats an empty/absent filter as "match everything", and
nothing documents a narrower default for any of them.

The flagged candidate (`ListStacksInput.StackStatusFilter`, whose doc reads
"If no StackStatusFilter is specified, summary information for all stacks
is returned (including existing stacks and stacks that have been
deleted)") is correctly implemented — `ListStacks`
(`stack_lifecycle.go:37-69`) applies no status filtering at all when
`statusFilter` is empty, so `DELETE_COMPLETE` stacks (retained, capped by
`evictDeletedStacks`) stay visible in an unfiltered call, matching the
sentence exactly. Added
`TestListStacks_NoFilter_IncludesDeletedStacks`
(`list_stacks_default_test.go`) driving the real SDK client — creates one
active and one deleted stack, calls `ListStacks` with no
`StackStatusFilter`, and asserts both are present. Confirmed the test
actually distinguishes the bug class by temporarily excluding
`DELETE_COMPLETE` from the unfiltered branch (fails as expected), then
restored the file byte-for-byte before landing the test alone.

Several other optional filters — `ListTypeRegistrationsInput.RegistrationStatusFilter`
(doc: "The default is `IN_PROGRESS`"), `ListTypeVersionsInput.DeprecatedStatus`
(doc: "The default is `LIVE`"), `ListTypesInput.{DeprecatedStatus,ProvisioningType,Visibility,Filters,Type}`,
`DescribeStackResourceDriftsInput.StackResourceDriftStatusFilters`, and
`ListStackSetOperationResultsInput.Filters` — are never read by their
handlers at all (`handleListTypeRegistrations`, `handleListTypeVersions`,
`handleListTypes`, `handleDescribeStackResourceDrifts`,
`handleListStackSetOperationResults`). That is the wire-key/field-coverage
axis, already disclosed elsewhere in this campaign, not this pass's
value-semantics axis — recorded here, not fixed, per the discrimination
this campaign draws between "never read" and "read with the wrong empty
case".

No range/bound/date filters exist on any cloudformation list operation, so
the boundary-inclusivity sub-shape does not apply here. No unrecognized-key
class of bug: `ListStacks`/`ListStackSets` filter on closed AWS enums with
no name/value pairing, and `parseStackInstanceFilters` already documents
(and correctly implements) ignoring unrecognized `Filters.member.N.Name`
values rather than rejecting them.

Gates: `go build ./services/cloudformation/...`, `go vet ./...`
(repo-wide, clean), `go test -race -count=1 ./services/cloudformation/...`
(pass, includes the new test), `golangci-lint run
./services/cloudformation/...` (0 issues).

## 2026-08-31 cmd/errtargetaudit sweep: 2 findings, both real (coverage caveat noted)

`go run ./cmd/errtargetaudit -dir cloudformation` reported a coverage warning
(90/258, 35%) and flagged its own output as unverified rather than clean —
the inflated 258 comes from the tool's module list pulling in the full
`dynamodb` and `s3` SDK modules (both legitimately imported by
`resources_dynamodb_supplemental.go`/`resources.go` for CFN-managed resource
types, plus `stacks_test.go`), not from a resolution gap in cloudformation's
own ~90-operation surface. Both flagged findings are genuine cloudformation
operations, verified individually against
`aws-sdk-go-v2/service/cloudformation@v1.76.1/deserializers.go`
(`awsAwsquery_deserializeOpError<Op>` shape) despite the coverage caveat.

**BatchDescribeTypeConfigurations sent `TypeNotFoundException`, a real code —
but a type-level one.** `type_registry.go:173`'s per-item
`BatchDescribeTypeConfigurationsError.ErrorCode` used the same literal as
`ActivateType`/`DeactivateType`/`DeregisterType`/`DescribeType`/`PublishType`
(all correctly `TypeNotFoundException` — they operate on *types*).
`BatchDescribeTypeConfigurations` operates on *type configurations*, and its
own deserializer declares `{CFNRegistryException,
TypeConfigurationNotFoundException}` — no `TypeNotFoundException` at all. The
adjacent human-readable message already said "type configuration not found",
so the code was the only thing not renamed. Fixed to
`TypeConfigurationNotFoundException`. One existing test asserted the wrong
value: `batch_describe_type_configurations_test.go`'s "unknown type name
reports an error" case (`wantErrorCode`), corrected, assertion count
unchanged (1).

**ExecuteStackRefactor sent `StackRefactorNotFoundException` — a code its own
operation model does not declare at all.** Its
`awsAwsquery_deserializeOpErrorExecuteStackRefactor` switch has no `case`
list whatsoever, only `default: return &smithy.GenericAPIError{...}` — this
operation is genuinely modeled with zero typed exceptions (confirmed:
`CreateStackRefactor`/`List*` share this shape; only `DescribeStackRefactor`
declares `StackRefactorNotFoundException`, which is why its own not-found
check stays as-is). Sending the sibling's code here can never reach a typed
branch on any client, whatever string is chosen — this is the refusal case
"the operation's own model declares no type for this condition", not a
remap. `handleExecuteStackRefactor` already had a generic fallback,
`"ValidationError"` (the classic AWS query-protocol generic/gateway code,
correctly on this tool's own `genericProtocolCodes` allowlist), used for
every *other* failure but overridden to the invented
`StackRefactorNotFoundException` specifically for the not-found case. Fixed
by deleting the override — not-found now falls through to the same generic
`ValidationError` every other `ExecuteStackRefactor` failure already used,
still failing (not silently succeeding) on an unknown ID, just without
inventing a code this operation cannot receive. The backend keeps returning
`ErrStackRefactorNotFound` internally (unaffected by this change) so its Go-
level message and `DescribeStackRefactor`'s own check are unchanged.

A stale comment on `DescribeStackRefactor` (`stack_refactors.go`) claimed
`ExecuteStackRefactor` was "fire-and-forget" alongside `CreateStackRefactor`/
`List*` — true of the *typed-exception* model post-fix, but the pre-fix code
directly contradicted it by returning a typed-looking not-found. Narrowed
the comment to `CreateStackRefactor`/`List*` only; a second, pre-existing
comment on `TestDescribeStackRefactor_NotFound`
(`stack_refactors_test.go`) already stated the same "no modeled errors,
correctly fire-and-forget" claim about `ExecuteStackRefactor`, and is now
true rather than aspirational.

One existing test asserted the invented code as correct:
`stack_refactor_move_test.go`'s `TestExecuteStackRefactor_UnknownRefactorErrors`
checked `rec.Body.String()` for `"StackRefactorNotFoundException"`; corrected
to `"ValidationError"`, assertion count unchanged (2: non-200 status +
body-contains). Both failed against the pre-fix source.

No web pages fetched this pass — everything came from the pinned SDK module
cache.

Gates: `go build ./services/cloudformation/...`, `go vet
./services/cloudformation/...`, `go test -race -count=1
./services/cloudformation/...` (pass), `golangci-lint run
./services/cloudformation/...` (0 issues).

## 2026-08-31 pass (gopherstack-21my): first per-item sweep

cloudformation had never had a per-item field-name sweep under this issue
(only wrapper-key-level and response-shape sweeps existed, e.g. sweep1's
`OperationId`/invented-field fixes). Confirmed `awsAwsquery_` (query/XML,
`strings.EqualFold` element matching, the same latent case-only-mismatch
class as REST-XML) from cloudformation@v1.76.1's own `deserializers.go`
before starting.

Covered at both layers: `Stack`/`StackSummary` (DescribeStacks/ListStacks),
`ChangeSet`/`ChangeSetSummary` (DescribeChangeSet/ListChangeSets),
`StackSet`/`StackSetSummary` (DescribeStackSet/ListStackSets),
`StackInstance`/`StackInstanceSummary` (DescribeStackInstance/
ListStackInstances), `TypeSummary` (ListTypes vs. DescribeType),
`StackResource`/`StackResourceSummary` (DescribeStackResource/
DescribeStackResources/ListStackResources — came back clean, see below).
Five real bugs found, all the sibling-shape this issue tracks.

**BUG (fixed): `ListStacks`'s `StackSummary` dropped `StackStatusReason`,
`LastUpdatedTime` and `DeletionTime` entirely.** `types.StackSummary`
(cloudformation@v1.76.1 types/types.go:3102) carries all three; this
backend's own `Stack`/`StackSummary` models already track them
(`stack.StackStatusReason` is set on every failure path, `LastUpdatedTime`
on `UpdateStack`, `DeletionTime` on `DeleteStack`) but the model-level
`StackSummary` type only ever carried `CreationTime`/`DeletionTime`/
`StackID`/`StackName`/`StackStatus` and `ListStacks`'s handler never emitted
`StackStatusReason` at all. `DescribeStacks` shared the `LastUpdatedTime`/
`DeletionTime` half of this gap (its own `stackXML` never declared them
either, despite `s.LastUpdatedTime`/`s.DeletionTime` being available on the
full `Stack` passed in) but already emitted `StackStatusReason` correctly —
the sibling-disagreement shape on that one field, a shared gap on the other
two. Right stack count either way; `StackStatusReason` was unconditionally
blank from `ListStacks`, `LastUpdatedTime`/`DeletionTime` blank from both.
Fixed by extending `StackSummary` (models.go) with `LastUpdatedTime`/
`StackStatusReason`, populating them in `ListStacks` (stack_lifecycle.go),
and adding nil-safe formatting for `LastUpdatedTime`/`DeletionTime` to both
`handleDescribeStacks`' `toXML` and `handleListStacks`' mapping loop
(handler_stacks.go). Test: `TestListStacks_ItemFields_RealClient`
(wire_field_fixes_cfn21my_test.go), seeds a CREATE_FAILED stack (unresolvable
`Fn::ImportValue`) for `StackStatusReason`, an updated stack for
`LastUpdatedTime`, and a deleted stack for `DeletionTime`, asserting all
three through both `ListStacks` and `DescribeStacks` via the real client.
Verified failing pre-fix (all three assertions empty/nil).

**BUG (fixed): `ListChangeSets`'s `ChangeSetSummary` dropped
`ExecutionStatus` and `StatusReason`.** `types.ChangeSetSummary`
(types.go:257) carries both; `DescribeChangeSet` (the singular sibling)
already emits both correctly, and this backend's `ChangeSet` model already
tracks both (`cs.ExecutionStatus` defaults `"AVAILABLE"` at creation and
flips to `"UNAVAILABLE"` with a real `StatusReason` on a no-op change set) —
`ListChangeSets`'s backend method (change_sets.go) just never copied either
field into the summary it builds. Right change-set count, `ExecutionStatus`
always empty (a client cannot tell an executable change set from a dead one
without falling back to `Status`) and `StatusReason` always empty. Fixed by
extending `ChangeSetSummary` (models.go), `ListChangeSets` (change_sets.go)
and `handleListChangeSets`'s `summaryXML` (handler_change_sets.go). Test:
`TestListChangeSets_ItemFields_RealClient`, seeds one change set with real
diff (`ExecutionStatus` stays `AVAILABLE`) and one no-op change set
(`ExecutionStatus` flips `UNAVAILABLE`, `StatusReason` set), asserts both
through `ListChangeSets`. Verified failing pre-fix (both fields empty).

**BUG (fixed), and a stale "field-diffed" comment caught: `DescribeStackSet`
never emitted `TemplateBody`.** `types.StackSet`'s own deserializer
(`awsAwsquery_deserializeDocumentStackSet`) includes a `TemplateBody` case;
this backend's `StackSet` model already tracks it (set at `CreateStackSet`
and `UpdateStackSet`) but the handler's `ssXML` — despite a comment directly
above it claiming it was "the full ... wire shape, field-diffed against ...
awsAwsquery_deserializeDocumentStackSet" — never declared or emitted the
field. A real client could create/update a stack set with a template and
never read it back through `DescribeStackSet`; `GetTemplate` (the analogous
op for plain stacks) has no stack-set equivalent, so this was the only way
to retrieve it. Comment corrected to note the prior false "full" claim and
the one still-real gap (`StackSetDriftDetectionDetails`, unbacked — no
set-level drift model). Fixed in handler_stack_sets.go (`ssXML` +
`stackSetToXML`). Test: `TestStackSet_ItemFields_RealClient`, creates a
stack set with a distinguishable template body, asserts it round-trips
through `DescribeStackSet`. Verified failing pre-fix (empty string).

**BUG (fixed): `ListStackSets`'s summary dropped `Description` even though
the backend already computed it.** `backend.ListStackSets`
(stack_sets.go) already populates `ss.Description` on the `StackSetSummary`
it returns, but `handleListStackSets`'s local `summXML` never declared a
`Description` field, so it was discarded between the backend and the wire
regardless of client input. Fixed by adding the field and (per staticcheck's
own S1016 finding once the two structs matched field-for-field) converting
the summary directly via `summXML(s)` instead of a hand-built literal. Test:
`TestStackSet_ItemFields_RealClient` (same test as above), also asserts
`ListStackSets`'s `Description`. Verified failing pre-fix (empty string).

**BUG (fixed), the loudest of this pass: `ListStackInstances` and
`DescribeStackInstance` emitted a `StackSetName` element that does not exist
on the real type at all.** `types.StackInstance`/`types.StackInstanceSummary`
(types.go:1836+) have no `StackSetName` member whatsoever — only
`StackSetId`. Both gopherstack handlers' local `instXML` had
`StackSetName string xml:"StackSetName,omitempty"`, so a real client's
`StackSetId` — the field used to correlate a stack instance back to its
stack set — was unconditionally empty, even though this backend's own
`StackInstance` model (models.go) already tracks `StackSetID` under the
correct `"StackSetId"` tag (and `StackID`, `StatusReason`, `DriftStatus`,
`LastOperationID`, all likewise tracked and likewise dropped by both
handlers). Not a sibling disagreement — both operations shared the identical
wrong local type. Fixed by rewriting both `instXML`s in handler_stack_sets.go
to match the model (StackSetID/StackID/Account/Region/Status/StatusReason/
DriftStatus/LastOperationID/OrganizationalUnitID). `StackInstanceStatus`
(the nested detailed-status structure) and `LastDriftCheckTimestamp` remained
unemitted at the time — genuine gaps, no state tracked for either. Test:
`TestStackInstance_ItemFields_RealClient`, creates a stack set and a stack
instance via the real client, asserts `StackSetId` and `StackId` through
both `ListStackInstances` and `DescribeStackInstance`. Verified failing
pre-fix (`StackSetId` empty on both).

**Update (gopherstack-eamp): `LastDriftCheckTimestamp` is now modelled.**
`detectStackInstanceDrift` (stack_sets.go) sets it on every instance whose
provisioned stack it actually compares, leaving it nil for instances it
could not check (no provisioned stack found) — matching the real "NULL if
drift detection hasn't been performed" semantics (types.go:2058-2061).
`StackInstanceStatus`/`StackInstanceComprehensiveStatus.DetailedStatus`
remains genuinely unmodelled: this backend has no per-operation lifecycle
distinct from `Status` (PENDING/RUNNING/SUCCEEDED/FAILED/CANCELLED/
SKIPPED_SUSPENDED_ACCOUNT don't map onto CURRENT/OUTDATED/INOPERABLE — see
the existing `ListStackInstancesFilter` doc comment, stack_instances.go:295,
making the same point for the DETAILED_STATUS filter). Left out of the wire
(no field emitted) rather than faked.

**Documented, not fixed: `ChangeSetSummary`'s `ImportExistingResources`,
`IncludeNestedStacks`, `ParentChangeSetId`, `RootChangeSetId`
(types.go:257-304) are absent from the wire.** None are accepted as
`CreateChangeSet` input anywhere in this service, let alone stored per
change set — `ImportExistingResources`/`IncludeNestedStacks` describe
CreateChangeSet request options this backend doesn't model at all, and
`ParentChangeSetId`/`RootChangeSetId` only have meaning for nested-stack
change sets, which this backend doesn't emulate. Not cheaply derivable from
existing state (unlike `ExecutionStatus`/`StatusReason`, fixed above, which
the backend already tracked). Left absent from the wire (omitempty/unset)
rather than emitting a fabricated value; a real fix needs `CreateChangeSet`
to accept and persist these first.

**BUG (fixed): `ListTypes` dropped `DefaultVersionId` and `IsActivated`.**
`types.TypeSummary` carries both; `DescribeType` (the singular sibling)
already emits both correctly, and this backend's type registry already
tracks both (`RegisteredType.DefaultVersion` set at `RegisterType`,
`.IsActivated` set by `ActivateType`/`DeactivateType`) but `ListTypes`'s
model-level `TypeSummary` (models.go) never carried either and the handler
never emitted them. Right type count, `IsActivated` always `false` and
`DefaultVersionId` always empty regardless of activation state. Fixed by
extending `TypeSummary`, populating both in `ListTypes` (type_registry.go)
and wiring them into `handleListTypes`'s `typeXML` (handler_type_registry.go).
Test: `TestListTypes_ItemFields_RealClient`, registers one type and
activates a second, asserts `IsActivated` differs and `DefaultVersionId` is
non-empty. Verified failing pre-fix (`IsActivated` false on the activated
type, `DefaultVersionId` empty).

**NOT a fix — `TypeSummary.Visibility` and `.Description` recorded as
unobservable, not backed.** The model's `Visibility` field (computed from
`t.IsPublished`) does not correspond to any real member on
`types.TypeSummary` at all — the real type has no `Visibility` member — so
wiring it to the response would add a field no client type reads; left as
harmless dead computation, not touched. `Description` (mapped from
`RegisteredType.Configuration`) is real on the wire but this backend never
writes `.Configuration` on any code path (`SetTypeConfiguration` writes to a
separate `typeConfigs` map, not the registry entry), so it is
unconditionally empty upstream of the handler — a genuine gap, not a naming
bug, and not fixed.

**`StackResource`/`StackResourceSummary` family (DescribeStackResource,
DescribeStackResources, ListStackResources) came back CLEAN at the per-item
layer for what's emitted.** All three share one real finding, but it's a
shared, unbacked gap rather than a bug: `types.StackResource`/
`types.StackResourceSummary` both carry `ResourceStatusReason`, `ModuleInfo`
and `DriftInformation`; this backend's `StackResource` domain model
(models.go) tracks only `Status`, with no per-resource failure-reason,
module-info or drift field at all, so none of the three operations can
populate any of them under current state — recorded, not fixed, per this
issue's restraint guidance. `LogicalResourceId`/`PhysicalResourceId`/
`ResourceType`/`ResourceStatus`/timestamp are all correctly named and nested
everywhere checked.

**Wrapping shape**: no call site of any `*Unwrapped` deserializer variant
exists anywhere in `services/cloudformation`, so every list checked this
pass is correctly member-wrapped rather than flattened.

**Case-only mismatches**: none found this pass.

**Hard failures**: none found this pass — every gap above is the silent-blank
class, not a decode error or panic.

**NOT REACHED at either layer this pass**: `DescribeStackEvents`/
`DescribeEvents` (`StackEvent`), `ListExports`/`ListImports` (`Export`, a
3-field type already spot-checked clean at wrapper level and structurally
too simple to carry this class), `DescribeStackResourceDrifts`/
`ListStackInstanceResourceDrifts` (`StackResourceDrift`), `ListResourceScan*`
(`ResourceScanSummary`/`ResourceScanResourceSummary`), `ListHookResults`
(`HookResultSummary`), `ListGeneratedTemplates`, `BatchDescribeTypeConfigurations`
detail shape, `ListStackSetOperations`/`ListStackSetOperationResults`,
`DescribeStackRefactor`/`ListStackRefactors`/`ListStackRefactorActions`,
`DescribeOrganizationsAccess`/publisher ops. These are named so a future pass
continues rather than redoes.

No web pages fetched this pass — everything came from the pinned SDK module
cache (cloudformation@v1.76.1) already vendored in the module cache.

Gates: `go build ./services/cloudformation/...`, `go vet
./services/cloudformation/...`, `go test -race -count=1
./services/cloudformation/...` (pass, 5 new tests, all additions), `golangci-lint
run ./services/cloudformation/...` (0 issues, after a `fieldalignment` reorder
on `ssXML` once `TemplateBody` pushed it over budget and a staticcheck S1016
struct-literal-to-conversion fix on `ListStackSets`'s mapping loop). No
`nolint` directives exist in any file this pass touched.

## 2026-08-31 pass (gopherstack-21my): continuation, named-gap queue

Continuing directly from the pass above; worked the explicit "NOT REACHED"
queue it left: `StackEvent`, `Export`/`Import`, `StackResourceDrift`,
`ResourceScanSummary`, `HookResultSummary`, `GeneratedTemplate`,
`BatchDescribeTypeConfigurations` detail, `StackSetOperation`/
`StackSetOperationResult`, the `StackRefactor` family, and the
organisations-access/publisher operations. Confirmed `awsAwsquery_` again
from this service's own `deserializers.go` before starting.

**BUG (fixed), the loudest finding of this pass: `DescribeEvents` wrapped its
collection under the wrong element AND the wrong type.** `DescribeEventsOutput`
wraps under `"OperationEvents"` holding `[]types.OperationEvent`
(cloudformation@v1.76.1 deserializers.go:27818,
`awsAwsquery_deserializeOpDocumentDescribeEventsOutput`) — a distinct type
from `DescribeStackEvents`' `"StackEvents"`/`types.StackEvent`, despite the
similar name. `handleDescribeEvents` (handler_stacks.go:438) emitted its items
under `"StackEvents"` — a real client's `OperationEvents` slice decoded EMPTY
regardless of how many events existed, a pure layer-1 wrapper-key miss on an
operation the wrapper-key sweep never reached. Separately, the item shape
itself emitted a `StackName` element that is not a member of
`types.OperationEvent` at all (that type has `StackId` but no `StackName`) —
harmless (skipped by the decoder) but removed since it doesn't correspond to
anything a client reads. Fixed by renaming the wrapper to
`OperationEvents>member` and rebuilding the item shape from
`types.OperationEvent`'s real members backed by this service's `StackEvent`
model (EventId/StackId/LogicalResourceId/PhysicalResourceId/ResourceType/
ResourceStatus/ResourceStatusReason/Timestamp); `ClientRequestToken`,
`DetailedStatus`, all `Hook*` fields, `OperationId`, `EventType`,
`OperationStatus`, `OperationType`, `StartTime`/`EndTime` (distinct from
`Timestamp`) and the `Validation*` fields remain unemitted — genuinely
unbacked, no per-event hook/operation-type/validation state tracked anywhere
in this service. Test: `TestDescribeEvents_RealClient`
(wire_field_fixes_cfn21my_test.go), drives `DescribeEvents` through the real
client and asserts a non-empty `OperationEvents` matching the created stack's
`StackId`. Verified failing pre-fix (empty slice). Also fixed an existing
raw-body test, `TestDescribeEvents_FailedEventsFilter`
(describe_events_filter_test.go), which hand-built its own response struct
under the same wrong `StackEvents>member` key its own code used — a
self-agreeing pair that could never have caught this, the exact blind spot
this issue calls out.

**BUG (fixed): `ListStackInstanceResourceDrifts` rebuilt an impoverished
record instead of reusing the fuller detail `DescribeStackResourceDrifts`
already prefers.** The real item type is `types.StackInstanceResourceDriftsSummary`
(types.go:1975) — a distinct sibling of `types.StackResourceDrift` with the
same required members (`LogicalResourceId`, `ResourceType`, `StackId`,
`StackResourceDriftStatus`, `Timestamp`). `backend.ListStackInstanceResourceDrifts`
(stack_instances.go:381) built its result only from `resourceDriftStatus`
(a bare status-per-logical-ID map), instead of preferring
`resourceDriftDetail` the way `DescribeStackResourceDrifts` already does —
so `ResourceType`, `PhysicalResourceId` and `Timestamp` were always
empty/zero even after `DetectStackResourceDrift` had populated
`resourceDriftDetail` for the same resource. Fixed by mirroring
`DescribeStackResourceDrifts`' detail-map-first pattern.

**BUG (fixed), exposed only by the fix above: wrong wrapping shape on
`PropertyDifferences`.** `ListStackInstanceResourceDrifts` marshals the
model's own `StackResourceDrift` type directly (models.go:198) rather than
through the separate `driftXML`/`propertyDiffXML` converter
`DescribeStackResourceDrifts`/`DetectStackResourceDrift` use — and the
model's `PropertyDifferences` field was tagged `xml:"PropertyDifferences"`
with no `>member`, so Go's encoder repeats the parent element once per slice
entry instead of nesting `<member>` children the way
`awsAwsquery_deserializeDocumentPropertyDifferences` requires
(deserializers.go:16348). Silent-empty, and unobservable before the detail-map
fix above since `ListStackInstanceResourceDrifts` never populated
`PropertyDifferences` at all until that fix landed. Fixed the tag to
`xml:"PropertyDifferences>member"` (models.go:207); confirmed this doesn't
affect `DescribeStackResourceDrifts`/`DetectStackResourceDrift`, which never
marshal the model's own tags. Both drift bugs share one test:
`TestListStackInstanceResourceDrifts_ItemFields_RealClient`, which creates a
stack set + instance, forces an out-of-band property change, calls
`DetectStackResourceDrift`, and asserts `ResourceType`, `PhysicalResourceId`,
a non-zero `Timestamp`, and non-empty `PropertyDifferences` all round-trip
through the real client's `ListStackInstanceResourceDrifts`. Verified failing
pre-fix twice: once for the three detail fields (hand-reverting the detail-map
fix), once independently for `PropertyDifferences` (hand-reverting only the
tag, with the detail-map fix left in place).

**BUG (fixed): `StackSetOperation`/`StackSetOperationSummary` both dropped
`CreationTimestamp` — the sibling-blind-spot shape, fourth sighting now.**
Both `types.StackSetOperation` (types.go:2715) and its list sibling
`types.StackSetOperationSummary` (types.go:2972) carry `CreationTimestamp`;
this backend's own `StackSetOperation` model already tracks the equivalent
(`CreatedAt`, set at `recordStackSetOperation`) but neither
`handleDescribeStackSetOperation` nor `handleListStackSetOperations`
(handler_stack_sets.go:648,687) ever emitted it — no disagreement between
singular and plural to notice, only the SDK type shows the gap. Fixed both.
While wiring `ListStackSetOperations`, found the model's *already-plumbed but
never-wired* `StackSetOperationSummary.CreationTime` field
(stack_sets.go:316-320 already copies `op.CreatedAt` into it) carried the
wrong wire name — `xml:"CreationTime"` where the real member is
`"CreationTimestamp"` — moot for behaviour today since the handler builds its
own separate local type rather than marshalling the model directly (same
"model tags aren't what reaches the wire" trap this campaign's 11:09 comment
already burned on), but corrected anyway (models.go:465) since a future
refactor that marshals the model directly would silently inherit the bug.
Also fixed: `DescribeStackSetOperation` never emitted `StackSetId`, a real
member gopherstack could resolve via the existing `DescribeStackSet` lookup
(the stack set name is already the request parameter) even though this
backend doesn't snapshot it per-operation. Test:
`TestStackSetOperation_ItemFields_RealClient`, asserts non-zero
`CreationTimestamp` through both operations and correct `StackSetId` through
`DescribeStackSetOperation`. Verified failing pre-fix (nil timestamp).

**BUG (fixed), an element that is not a member of the real type at all —
second sighting of this exact class in this service.** `types.StackRefactorAction`
(types.go:2118) has no `StackName`, `LogicalResourceId` or `ResourceType`
member whatsoever; the real shape nests source/destination location under
`ResourceMapping.Source`/`.Destination`, each a `types.ResourceLocation`
(types.go:1178, `{LogicalResourceId, StackName}`). gopherstack's
`StackRefactorAction` model (models.go) had flat top-level `StackName`/
`LogicalResourceID`/`ResourceType` fields and the handler marshalled the
model directly — so a real client's `ResourceMapping` was unconditionally nil
despite `backend.ListStackRefactorActions` (stack_refactors.go) already
holding the full source/destination `ResourceMapping` for every action in its
loop variable, just never attaching it to the record. Fixed by adding a
`ResourceMapping` field to the model (models.go:496, populated in
stack_refactors.go), and building a dedicated wire converter,
`toStackRefactorActionXML` (handler_stack_refactors.go:173), that emits the
correct nested `ResourceMapping>Source/Destination` shape instead of the
non-member flat fields. `Entity`, `Detection`, `DetectionReason`,
`ResourceIdentifier`, `TagResources` and `UntagResources` remain unemitted —
genuinely unbacked (this service's refactor model tracks only description,
status and the mapping list). Also fixed in the same pass: `DescribeStackRefactor`
emitted only `Status`, dropping `Description` and `StackRefactorId` — both
already tracked by the backend's `StackRefactor` model but discarded because
`backend.DescribeStackRefactor` (stack_refactors.go:28) returned a bare
status string instead of the record. Changed its signature to return
`*StackRefactor` (only internal caller: handler_stack_refactors.go; `go vet
./...` repo-wide confirmed clean, no external caller broken). `ExecutionStatus`/
`ExecutionStatusReason` (on both `DescribeStackRefactor` and
`StackRefactorSummary`) and `DescribeStackRefactorOutput.StackIds` remain
unemitted: this backend collapses AWS's separate create-phase `Status` and
execute-phase `ExecutionStatus` into one `Status` string
(`CREATE_IN_PROGRESS`/`CREATE_COMPLETE`/`EXECUTE_IN_PROGRESS`/`EXECUTE_COMPLETE`)
— a state-modelling gap on a different axis than this issue's naming class,
filed rather than fixed. Test: `TestStackRefactor_ItemFields_RealClient`,
creates two stacks and a refactor mapping one resource between them, asserts
`Description`/`StackRefactorId` through `DescribeStackRefactor` and the full
nested `ResourceMapping` (both `Source` and `Destination`) through
`ListStackRefactorActions`. Verified failing pre-fix on all three assertions.

**BUG (fixed): `DescribePublisher` dropped `PublisherId`.**
`DescribePublisherOutput` (api_op_DescribePublisher.go) carries `PublisherId`;
`handleDescribePublisher` (handler_type_registry.go:578) already resolves the
publisher by that exact ID but never echoed it back. Fixed by emitting the
resolved ID. `PublisherProfile` and `IdentityProvider` remain unemitted —
genuinely unbacked (this service's `Publisher` model tracks only ID,
connection ARN and status). Test: `TestDescribePublisher_PublisherId_RealClient`,
registers a publisher and asserts `PublisherId` round-trips through
`DescribePublisher`. Verified failing pre-fix (empty string).

**CONFIRMED CLEAN at both layers**: `Export`/`Import` (`ListExports`/
`ListImports`) — every field on `types.Export` and the `Imports` wrapper
matches byte-for-byte against the deserializer, including the flattened
`[]string` shape for `Imports>member`. `DescribeStackEvents`'s own
`StackEvent` item shape (as opposed to its `DescribeEvents` sibling above) —
all emitted fields correctly named; `ClientRequestToken`, `DetailedStatus`
and all `Hook*` members are genuinely unbacked (no hook-per-event state, and
`ClientRequestToken` is never accepted as a request parameter anywhere in
this service). `ActivateOrganizationsAccess`/`DeactivateOrganizationsAccess`
(correctly empty result shapes) and `DescribeOrganizationsAccess` (`Status`
values `"ENABLED"`/`"DISABLED"` match `types.OrganizationStatus` exactly).
`StackResourceDrift`'s core fields via `DescribeStackResourceDrifts`/
`DetectStackResourceDrift`'s `driftXML` converter (unchanged this pass) —
`DriftStatusReason` recorded as unobservable (this service's
`StackResourceDriftStatus` never reaches `UNKNOWN`, the only status
`DriftStatusReason` documents); `ModuleInfo`/`PhysicalResourceIdContext`
recorded as unbacked.

**RECORDED, NOT FIXED (restraint — real gaps, no legal input can populate
them, or a different axis than this issue's naming class):**
- `GeneratedTemplate`/`TemplateSummary` (`DescribeGeneratedTemplate`/
  `ListGeneratedTemplates`): `CreationTime`, `LastUpdatedTime`, `StatusReason`,
  `Progress`, `Resources`, `StackId`, `TemplateConfiguration`, `TotalWarnings`/
  `NumberOfResources` all unbacked — this service's `GeneratedTemplate` model
  (models.go:304) tracks only ID/name/status/body, and `Status` is hardcoded
  `"COMPLETE"` on every path (generated_templates.go), so `StatusReason` in
  particular could never be observed even if wired.
- `ResourceScanSummary`/`DescribeResourceScanOutput` (`ListResourceScans`/
  `DescribeResourceScan`): `StartTime`, `EndTime`, `StatusReason`, `ScanType`,
  `ResourceTypes`, `ResourcesRead`, `ResourcesScanned`, `ScanFilters` all
  unbacked — the `ResourceScan` model (models.go:312) tracks only
  ID/status/percentage, and scans always complete synchronously at 100%.
- `HookResultSummary` (`ListHookResults`): every per-item field beyond
  `Status`/`HookStatusReason` is unobservable for a deeper reason than the
  usual unbacked-field case — no production code path anywhere in this
  service ever calls `b.hookResults.Put`, so a real client can never see a
  non-empty `HookResults` list regardless of what's wired. Structural gap
  (hooks aren't emulated), filed as a different axis. Also recorded, not
  fixed: `ListHookResultsInput`'s real fields are `TargetId`/`TargetType`/
  `TypeArn`/`Status`/`NextToken` (api_op_ListHookResults.go) — this service's
  handler instead reads a `HookResultToken` form field with no real
  counterpart, a request-parsing gap consistent with the same carve-out a
  prior pass used for autoscaling's `PutScalingPolicy` (different bug shape,
  not this issue's response-naming class).
- `BatchDescribeTypeConfigurations`'s `TypeConfigurationDetails.LastUpdated`:
  unbacked — `b.typeConfigs` (type_registry.go) is a bare
  `map[string]string` with no parallel timestamp tracked at
  `SetTypeConfiguration` time.
- `StackSetOperationResultSummary.OrganizationalUnitId`
  (`ListStackSetOperationResults`): the OU value exists locally in
  `CreateStackInstances`' target-resolution loop (`t.ouID`,
  stack_instances.go) but isn't threaded through `recordOpResults`'
  signature (stack_sets.go:275, two call sites) to the per-account/region
  result record — a state-threading gap, not a naming one, deferred rather
  than restructuring that shared helper's signature this pass.
- `StackSetOperationPreferences`, `DeploymentTargets`,
  `StackSetDriftDetectionDetails`, `StatusDetails`, per-operation
  `AdministrationRoleARN`/`ExecutionRoleName`/`RetainStacks`: not snapshotted
  per StackSet operation at all (this backend has no per-op preferences/
  targets model); recorded, not fixed.

**Wrapping shape**: no call site of any `*Unwrapped` deserializer variant
exists anywhere in `services/cloudformation` (re-confirmed), and the one
flattened-vs-member-wrapped bug found this pass (`PropertyDifferences` above)
is fixed.

**Case-only mismatches**: none found this pass.

**Hard failures**: none found this pass — every finding above is the
silent-blank/silent-nil class, not a decode error or panic. (The `StackName`
element removed from `DescribeEvents`' item shape was silently skipped by the
decoder, not an error, since `awsAwsquery_deserializeDocumentOperationEvent`'s
unmatched-element branch calls `decoder.Decoder.Skip()`.)

**Nested-vs-top-level double member**: none found this pass.

**Prior verdicts**: none proved false this pass — no comment or PARITY claim
here was checked and found untrue.

**Pages fetched**: none. Everything came from the pinned SDK module cache
(`cloudformation@v1.76.1`, `~/go/pkg/mod/github.com/aws/aws-sdk-go-v2/service/
cloudformation@v1.76.1`) already resident locally.

**Out-of-scope files**: none touched by this pass. `resources_extended.go`
and `resources_network_and_kms_test.go` show as modified in `git status` but
were edited by a concurrent agent working `services/ec2/` (fixing this
service's `CreateVpc` call sites after an ec2 backend signature change) —
not by this pass.

**NOT REACHED this pass**: `StackResourceSummary`/`StackResourceDetail`
(`ListStackResources`/`DescribeStackResource(s)`, previously marked clean —
not re-verified), `BatchDescribeTypeConfigurations`'s `Errors`/
`UnprocessedTypeConfigurations` shapes (only the `TypeConfigurationDetails`
detail shape named in this pass's queue was checked), `StackSetOperation`'s
`OperationPreferences` nested shape in full, `ListStackSetAutoDeploymentTargets`,
`ImportStacksToStackSet`. Named so a future pass continues rather than redoes.

Gates: `go build ./services/cloudformation/...`, `go vet
./services/cloudformation/...`, `go test -race -count=1
./services/cloudformation/...` (pass, 5 new tests + 1 existing test corrected),
`golangci-lint run ./services/cloudformation/...` (0 issues, after a
`fieldalignment` reorder on the new `stackRefactorActionXML` and a `govet`
shadow fix in `handleDescribeStackSetOperation`). Repo-wide `go vet ./...`
clean (the `DescribeStackRefactor` backend signature change has one internal
caller only). All pre-existing `nolint:lll` directives in files this pass
touched (models.go, handler_stack_sets.go) remain in active use — confirmed
by `golangci-lint`'s 0-issues result, which would have flagged any now-stale
suppression via `nolintlint`.

**DECISION (2026-09-11, gopherstack-b3pm): stack-set operations complete
synchronously, by design — recorded so this isn't re-discovered a third
time.** `gopherstack-101r`'s sweep found `StopStackSetOperation`'s success
path could only be reached by white-box-seeding a `RUNNING` operation,
because `recordStackSetOperation` (`stack_sets.go`) writes every operation as
`SUCCEEDED` the instant it's created — `RUNNING` is unreachable, so nothing
can ever be observed in progress or stopped. That issue asked which of two
things is true: either cloudformation has a clock/janitor lifecycle
elsewhere that stack sets should share, or the service is synchronous
throughout and the gap should be recorded rather than half-fixed on one
resource type.

It's the second one. Grepping the whole package for `Janitor`, `SweepOnce`,
`SetClock`, `reconcile`, `ReadyAt` turns up nothing (the one `reconcile` hit
is a doc-comment word, not a mechanism). `CreateStack` (`stacks.go`) writes
`CREATE_IN_PROGRESS`, calls `createStackFromTemplate` inline, and flips to
`CREATE_COMPLETE` three lines later in the same function call — no
goroutine, no ticker, nothing to advance the status after the handler
returns. Change sets (`change_sets.go`), drift detection
(`drift_detection.go`), stack refactors (`stack_refactors.go`), and every
existing stack-set operation (`ImportStacksToStackSet`, `UpdateStackSet`,
`DetectStackSetDrift`, `CreateStackInstances`/`DeleteStackInstances` via
`recordOpResults`) follow the identical pattern: `*_IN_PROGRESS` and
`*_COMPLETE`/`SUCCEEDED` are both written before the call returns. This is
categorically different from the async patterns this session gave
`services/codebuild` (`9963c5e52`, a janitor-tick-driven build-batch
lifecycle) and `services/rds` (`lifecycle.go`, a `readyAt` timestamp plus
ticker) — both of those services already had a clock-driven lifecycle
mechanism to extend; cloudformation has none, anywhere, for any resource.
Bolting an async `RUNNING`/`STOPPING` lifecycle onto stack-set operations
alone, in an otherwise wall-to-wall-synchronous service, would be
inconsistent and would invite exactly the `time.Sleep`/flake class this
session already hit standing up rds's ticker (see
`.claude/memories/no-time-sleep-in-tests.md`) — for one resource family
whose sibling resources (plain stacks, change sets, drift, refactors) will
never get the same treatment. So: **synchronous completion is kept, on
purpose, for all of cloudformation, not just stack sets.**

`StackSetOperationStatus`'s real enum (cloudformation@v1.76.1
`types/enums.go:1736-1746`) is `RUNNING`/`SUCCEEDED`/`FAILED`/`STOPPING`/
`STOPPED`/`QUEUED` — this backend only ever produces `SUCCEEDED` (or
`FAILED`, from `DeleteStackInstances`' per-pair teardown failures, see
`stack_instances.go:210-213`), so `RUNNING`/`STOPPING`/`QUEUED` are
unreachable, by design. `StopStackSetOperation`'s real modeled error set
(`cloudformation@v1.76.1` `deserializers.go`,
`awsAwsquery_deserializeOpErrorStopStackSetOperation`) is exactly three
cases: `InvalidOperationException`, `OperationNotFoundException`,
`StackSetNotFoundException` — no fourth generic fallback in practice for a
known op. Read the case: since every operation this backend records is born
`SUCCEEDED`, the only reachable outcome of a real `StopStackSetOperation`
call (besides unknown-name/unknown-ID) is "stop a non-`RUNNING` operation",
and `handleStopStackSetOperation` (`handler_stack_sets.go:732-750`) already
maps that (`ErrOperationNotRunning`, from `op.Status != "RUNNING"` in
`StopStackSetOperation`, `stack_sets.go:397-414`) to
`InvalidOperationException` — matching `types.InvalidOperationException`'s
doc comment ("The specified operation isn't valid",
`types/errors.go:253-261`). **This was already correct before this pass; no
code fix was needed for it.** What was missing was a test reaching it
through the real client instead of the white-box map-seed, and the
decision comment this entry now provides. `recordStackSetOperation` now
carries a one-line `// SUCCEEDED synchronously, deliberately` marker citing
this entry.

`stopstacksetoperation_whitebox_test.go`'s `TestStopStackSetOperation_RealClient`
is kept as-is — it is still the only way to exercise Stop's *success*
envelope (there being no reachable `RUNNING` operation to stop for real),
and its own comment already explained why. Added
`TestStopStackSetOperation_AlreadySucceeded_RealClient`
(`stopstacksetoperation_whitebox_test.go`) alongside it: creates a stack set, runs
`UpdateStackSet` to produce a real (`SUCCEEDED`) operation ID through the
public API, then calls `StopStackSetOperation` through the real SDK client
and asserts `InvalidOperationException` comes back — the reachable path
the gap identified, now covered without white-boxing anything. No snapshot
shape changed: `StackSetOperation.Status` already persisted as a plain
string field before this pass.

Gates (gopherstack-b3pm): `go build ./...` (whole module) clean, `go vet
./...` clean, `go test -count=1 ./services/cloudformation/... ./pkgs/persistence/...`
pass, `golangci-lint run ./services/cloudformation/...` 0 issues.

## 2026-09-12 (gopherstack-n3zi)

Added `realclient_stack_management_and_resource_scans_test.go`, covering this service's last 12
typed-client-blind ops (DescribeChangeSetHooks, DescribeGeneratedTemplate,
DescribeOrganizationsAccess, DescribeResourceScan, DescribeTypeRegistration,
ExecuteStackRefactor, GetGeneratedTemplate, ListHookResults,
ListResourceScanRelatedResources, SignalResource, TestType,
UpdateStackInstances) -- typed coverage 78/90 -> 90/90 (0 uncovered).
**Three real bugs found and fixed**, all caught only by a decoded typed-
client value (every call returned 200, nothing status-code-only would have
caught them):

1. `CreateGeneratedTemplate`'s handler never read the request's `Resources`
   member at all (`h.Backend.CreateGeneratedTemplate(name, nil)`,
   hardcoded nil) -- a real client's `Resources` list (the entire point of
   the op) was silently dropped regardless of what was requested, so
   `GetGeneratedTemplate`/`DescribeGeneratedTemplate` always returned an
   empty-Resources template. Fixed by parsing
   `Resources.member.N.{ResourceType,LogicalResourceId}` from the awsQuery
   form (verified against serializers.go's
   `awsAwsquery_serializeDocumentResourceDefinition`) into the
   "Type/LogicalID" strings the existing backend signature accepts.
2. `SignalResource`'s handler discarded the backend's error entirely
   (`_ = h.Backend.SignalResource(...)`) -- a real client's call against a
   nonexistent stack always got a fabricated 200 OK instead of the real
   error. Fixed by returning `h.xmlError` on a non-nil error, matching this
   file's sibling handlers' convention.
3. Three `StackSetOperation.Action` values recorded by
   `CreateStackInstances`/`UpdateStackInstances`/`DeleteStackInstances`
   were fabricated: `"CREATE_INSTANCES"`/`"UPDATE_INSTANCES"`/
   `"DELETE_INSTANCES"` do not exist in the real `StackSetOperationAction`
   enum (only `CREATE`/`UPDATE`/`DELETE`/`DETECT_DRIFT` are real,
   `types/enums.go`, cloudformation@v1.76.1) -- confirmed by
   `StackSetOperation.Action`'s own doc comment: "Create and delete
   operations affect only the specified stack instances ... Update
   operations affect both the StackSet itself, in addition to all
   associated stack instances," i.e. instance-scoped operations report the
   same CREATE/UPDATE/DELETE action as their StackSet-level counterparts,
   not a distinct suffixed value. Fixed all three call sites; one
   pre-existing test (`TestDescribeStackSetOperation_Action`) asserting the
   old fabricated value was corrected, not weakened. `ImportStacksToStackSet`
   recording `"IMPORT"` (also not a real enum value) was left unfixed --
   not hit by this slice's own test, flagged in code for a future pass.

No accept-and-drop findings beyond what's listed above. Gates: `go build
./...` (whole module), `go vet`, `go test -race -count=1`, `golangci-lint
run --new-from-rev=HEAD` (0 issues) all clean. `go run ./cmd/paritylint`
stays at 0 FAIL. No persisted struct fields changed (the Action fix
corrects an existing string field's *value*, not its shape); no version
bump.

## 2026-09-13 (gopherstack-xhu2t tier-1 sweep, awsquery reqfielddiff)

`cmd/reqfielddiff -dir cloudformation` (a request-field, not response-field,
scan -- known to undercount on this awsquery-protocol service, gopherstack-99nj)
went from 45 tier-1 findings to 0 after this pass. Findings grouped by
mechanism:

1. **CallAs dropped on 20 StackSet-family ops** (the dominant finding).
   Fixed via `validateCallAs`/`validateCallAsForStackSet` (handler_stack_sets.go)
   -- see `families: call_as` above for the full writeup. Ops touched:
   CreateStackSet, UpdateStackSet, DeleteStackSet, DescribeStackSet,
   ListStackSets, CreateStackInstances, DeleteStackInstances,
   UpdateStackInstances, ListStackInstances, DescribeStackInstance,
   DetectStackSetDrift, ListStackSetOperations, DescribeStackSetOperation,
   StopStackSetOperation, ListStackSetAutoDeploymentTargets,
   ImportStacksToStackSet, ListStackInstanceResourceDrifts,
   DescribeOrganizationsAccess, ListStackSetOperationResults,
   GetTemplateSummary.
2. **CreateStack options dropped**: ResourceTypes (allowlist),
   DisableValidation, EnableTerminationProtection (StackOptions had no field
   for it at all -- CreateStackInput.EnableTerminationProtection was
   silently discarded end to end, so a stack created with it set to true
   was deletable like any other), RetainExceptOnCreate.
   See `families: disable_rollback_and_retain_except_on_create` above --
   this also surfaced two genuine pre-existing bugs (DisableRollback never
   consulted; DeletionPolicy ignored during rollback, force-deleting Retain
   resources) beyond the reqfielddiff findings themselves.
3. **CreateChangeSet.ChangeSetType dropped**: CREATE/UPDATE/IMPORT was
   accepted but never validated against whether the target stack already
   existed -- a CREATE against an existing stack, or UPDATE/IMPORT against a
   missing one, silently proceeded instead of erroring. Fixed via
   `resolveChangeSetType` (change_sets.go). `ExecuteChangeSet.DisableRollback`
   and `.RetainExceptOnCreate` were also dropped -- now threaded into the
   internal StackOptions passed to CreateStack/UpdateStack.
4. **ListResourceScans.ResourceScanType filter and
   ListResourceScanResources.MaxResults/NextToken dropped**: fixed --
   ListResourceScans now filters by scan type (StartResourceScan always
   produces FULL, so ResourceScan gained a `ScanType` field to filter on);
   ListResourceScanResources now paginates via `pkgs/page` instead of
   returning every result unpaginated.
5. **Type Registry family** (ListTypes.Visibility/ProvisioningType,
   ListTypeRegistrations.RegistrationStatusFilter, TestType.VersionId,
   RegisterPublisher.AcceptTermsAndConditions): fixed -- see
   `families: type_registry_filters_and_registration` above, including the
   self-caught Visibility/ProvisioningType response-field fabrication that
   was reverted after a real-SDK-typed compile error surfaced it.
6. **GetTemplate.TemplateStage / GetTemplateSummary.CallAs**: TemplateStage
   (Original|Processed) was accepted but not validated against its enum;
   now rejects an out-of-enum value with ValidationError via
   `isValidGetTemplateStage`.

Recorded, not fixed (added to `items_still_open` above with full reasoning):
IncludeNestedStacks (api_op_CreateChangeSet.go:209) -- no change-set-level
nested-stack-tree path exists yet (its siblings on that same op,
DisableValidation/ResourceTypes, were fixed 2026-09-18; see ops:
CreateChangeSet above);
UpdateStack.RetainExceptOnCreate and RollbackStack.RetainExceptOnCreate/RoleARN
(no resource-level rollback machinery on either path to apply them to --
RollbackStack is a status-only stub); ListResourceScanRelatedResources.
MaxResults/NextToken (the op's own Resources data is always empty, nothing
to paginate); ActivateType.AutoUpdate/MajorVersion/VersionBump/
LoggingConfig/ExecutionRoleArn (no multi-version type catalog or update
scheduler to apply them to).

New real-client tests, both table-driven with `t.Parallel()` at every
level: `realclient_stackset_callas_test.go`
(`TestRealClient_StackSetCallAs`, 4 subtests) and
`realclient_stack_options_test.go` (`TestRealClient_StackOptions`, 14
subtests).

Persisted-struct changes: `ResourceScan` gained `ScanType string`
(additive, no version bump; `pkgs/persistence/testdata/snapshot_inventory.json`
updated by hand, not via `-update`, since the guard also flagged a
gofmt struct-tag realignment on `ResourceScan`'s two pre-existing fields as
a false-positive "field changed" -- confirmed via a throwaway diff against
the live scan that only whitespace, not any json/xml tag key, changed).

Gates (whole module, foreground): `go build ./...` clean. `go vet .` and
`go vet ./services/cloudformation/...` clean. `go test -race -count=1 -p 2
./services/cloudformation/...` -- PASS (all 14 TestRealClient_StackOptions
subtests, all 4 TestRealClient_StackSetCallAs subtests, full existing suite).
`go test -count=1 ./pkgs/persistence/...` -- PASS for cloudformation and
redshift after the by-hand inventory fix (a concurrent agent's own
in-progress elasticache edits in this shared worktree separately fail this
same suite at times; unrelated to this pass, not touched, confirmed via
`git status --short services/elasticache`). `go test -count=1 -short .`
(root) -- PASS. `go vet -tags integration ./...` and `go vet -tags e2e
./services/cloudformation/... ./services/redshift/... .` -- clean (a
repo-wide `-tags e2e ./...` transiently failed on the same
concurrently-edited elasticache file mid-refactor by the other agent; not
this service, not this pass). `golangci-lint run --concurrency 2
--new-from-rev=HEAD ./services/cloudformation/...` -- 0 issues. `go run
./cmd/paritylint` -- 0 missing-items-still-open FAIL.

## 2026-09-18

Audited the 15 `cmd/overwidecandidates` List ops for Describe-shaped leaks.
Fixed one real leak (`ListStackInstanceResourceDrifts` emitted
`ExpectedProperties`/`ActualProperties`, absent from the real
`StackInstanceResourceDriftsSummary`) and four dropped-but-tracked Summary
members (`ListHookResults.HookResultId`, `ListResourceScans.
PercentageCompleted`, `ListStackSets.AutoDeployment`/`ManagedExecution`/
`PermissionModel`, `ListStacks.ParentId`/`RootId`/`TemplateDescription`).
Re-adjudicated `items_still_open`: fixed `CreateChangeSet`'s dropped
`ResourceTypes`/`DisableValidation` (threaded into `ExecuteChangeSet`, like
`Capabilities`); the other 10 entries are confirmed-accurate, tightened.

## 2026-09-20: Type Registry ARN shape (cognito-ecs-cloudformation-and-wafv2 coverage, `aws_cloudformation_type`)

Real type ARNs are account/region-scoped with `::` hyphenated
(`arn:aws:cloudformation:<region>:<account>:type/resource/Vendor-Svc-Type`);
this backend built a bare `arn:aws:cloudformation:::type/resource/` + the raw
`::`-separated name, which fails `terraform-provider-aws`'s own ARN parser
("expected 4 resource parts"). Fixed via a single `buildTypeARN` helper used
everywhere a type ARN was hand-formatted (`RegisterType`, `ActivateType`,
`DeactivateType`, `DeregisterType`, `PublishType`,
`BatchDescribeTypeConfigurations`, `ListTypeVersions`, `DescribeType`).
`DescribeTypeRegistration`'s `TypeVersionArn` previously duplicated `TypeArn`
verbatim (the prior 2026-09-13 fix populated the field but not with a real
per-version value) — now genuinely distinct (`TypeArn + "/" + versionID`,
matching real `TypeVersionSummary.Arn`). `SetTypeDefaultVersion` only accepted
identification by `Arn`; `aws_cloudformation_type`'s create path always
identifies by `TypeName` instead, so it 404'd immediately after every
register — added the same `Arn`-or-`TypeName` fallback the other type-registry
ops already had. `DescribeType` also now accepts a version-suffixed `Arn`
(stripping and resolving `/<versionId>`), since the resource's `Read` re-reads
by the `TypeVersionArn` it stored as `id`. Proven end-to-end by
`TestTerraform_CognitoEcsCloudformationAndWafv2` (real `aws_cloudformation_type` apply) and unit
tests `TestDescribeType_Registered/lookup_by_version-suffixed_ARN_after_RegisterType`
plus the updated ARN literals across `type_registry_test.go`,
`type_registry_feature_test.go`, `deregister_type_version_test.go`,
`empty_result_element_test.go`, `handler_type_registry_arn_test.go`.
