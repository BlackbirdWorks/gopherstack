# Over-wide List-response candidate list

Built for gopherstack-dv4s (batch five). **Three prior agents rebuilt this
same candidate list from scratch in-session** because the scratch tooling
that generated it lived only in a session scratchpad and did not survive
between sessions — the same waste already named once for the sibling
required-member sweep (gopherstack-569k: "Scratch tooling lived in a session
scratchpad and will not survive - regenerate from the description above
rather than hunting for it"). This file and `cmd/overwidecandidates`
exist so that stops happening here too.

**A future batch should read this file, not rebuild the list.** Regenerate
only to pick up new services, a go.mod version bump, or after resolving
tooling gaps noted below — and when you do, update the "already examined"
table and re-run the ranking, don't just discard this file.

## What "candidate" means, and what it doesn't

A candidate is a List op whose **real AWS SDK Output type** — not
gopherstack's own code — declares a slice of a struct whose name ends in
`Summary`, `Item`, `Brief`, `Entry`, `Ref`, `Preview`, `Metadata` or `Info`.
That is a floor for "AWS itself narrows this op," not a verdict on
gopherstack. Two more steps are required before calling anything a leak:

1. Read gopherstack's own handler/converter for that op and compare the
   emitted key set against the real Summary struct's declared members —
   **field by field, not by name or by analogy with a sibling op** (three
   near-misses already happened doing it by analogy — see gopherstack-dv4s
   notes, medialive ListSignalMaps and ListChannelPlacementGroups).
2. Watch for the **shared-converter signal**: a leak is far more likely
   where a service reuses one converter across two shapes that should
   differ — Describe-vs-List (forecast, stepfunctions) or one scope vs.
   another (cleanrooms: collaboration-scope reusing the membership-scope
   converter). Services with a dedicated inline literal per List op are the
   pattern that has come back clean every time it was checked structurally
   (cleanrooms' own membership-scope ops, most of pass 4's ecs/eks/glue/
   dynamodb/batch/sagemaker).

**Known false-positive classes, already paid for — do not re-derive them:**

- Grepping for a literal `type FooSummary struct` declaration and treating
  its absence as a leak signal. Several services (sagemaker, batch) narrow
  correctly via an inline `map[string]any` with no named struct at all —
  this script's regex works from the *List op's Output struct*, not from
  struct-declaration grep, so it doesn't reproduce that specific mistake,
  but a human reading gopherstack's side still can.
- A top-level-only scan of the Output struct misses the classic REST-XML
  wrapper shape (`FooList{ Items []FooSummary }`, cloudfront's
  `DistributionList.Items []DistributionSummary`). This script follows one
  level of pointer indirection into a single wrapper field to catch that —
  see `depth: "wrapper:<Type>"` in the JSON detail dump — but only one
  level; a doubly-nested wrapper would still be missed.
- Ops whose real Output returns bare strings/ARNs, or the exact same full
  type Describe/Get returns, are **not candidates at all** — AWS itself
  doesn't narrow them, so there is nothing to leak relative to.
- Reasoning about a Summary type's shape "by name" or by analogy with a
  sibling op instead of reading the actual struct declaration has produced
  wrong findings more than once this campaign. Always read the real struct.

## Method

For each `services/<dir>`, resolve the pinned
`aws-sdk-go-v2/service/<mod>@<version>` from `go.mod` (directory name and
module name diverge for 9 services — see `dirModuleOverride` in
`cmd/overwidecandidates/main.go`; go.mod also mixes a `require (...)` block
with 10 standalone `require x v...` lines, and missing that second form
silently drops modules like `bedrockagent`, `vpclattice`, `cleanrooms`,
`omics` from the whole scan — this cost a full rebuild pass to catch). Read
every `api_op_List*.go` `Output` struct from
`$(go env GOMODCACHE)/github.com/aws/aws-sdk-go-v2/service/<mod>@<version>`,
flag slice fields whose element type is a `types.*` struct matching the
name pattern above, plus one level of wrapper indirection.

## Regenerate

```
go run ./cmd/overwidecandidates                 # ranked summary (this table)
go run ./cmd/overwidecandidates -json out.json   # full per-op detail, incl. non-candidate fields and depth
```

No network access required — it only reads `go.mod` and the
already-downloaded module cache. Runs in a few seconds. (An earlier draft of
this tool was written in Python; it was rewritten in Go before being
persisted because `*.py` is repo-gitignored — that draft would have suffered
the exact same lost-on-session-end fate this file exists to prevent.)

**Cross-check performed for this rebuild:** the ranking below reproduces
the counts a prior (non-persisted) run of this same sweep reported for its
top four — cloudformation 15, vpclattice 14, waf 13, bedrockagent 13 — a
strong signal the method is stable across rebuilds, which is the entire
point of writing it down once.

## Already examined for this bug class

Services below are **excluded from the ranked candidate table** — they've
already been swept for over-wide List responses, one way or the other. Do
not re-derive; read the referenced commit/bd issue for detail. New
entries should be added here (and removed from the ranked table) as future
batches clear more of it.

| Service | Result | Ref |
|---|---|---|
| omics | 5/5 leaking ops fixed. The 3 left open by e68817984 (ListAnnotationStores/ListVariantStores/ListAnnotationStoreVersions) were closed this session — see below | e68817984, this session |
| stepfunctions | 6/6 List ops leaking, fixed | a1bc521e6 |
| forecast | 12/12 List ops leaking, fixed | aad4fa967 |
| cleanrooms | 5/6 collaboration-scoped ops leaking (access-boundary bug), fixed. 16 other candidate ops NOT field-diffed — structurally low-risk, not verified | 6a3b883d5 |
| swf | checked, clean | examined alongside forecast, aad4fa967 |
| personalize | 16/16 List ops leaking, one shared root cause, fixed | de3ccfb36, gopherstack-sm02 |
| appconfig | 7 ops leaking, fixed; 3 false "extras are harmless" PARITY.md notes corrected | 333fa3701, gopherstack-xs7l |
| emrserverless | 3 ops leaking, fixed | 268992473, gopherstack-tuh5 |
| codeartifact | 2 ops leaking (one also had an inverse wrong-key bug), fixed | 268992473, gopherstack-tuh5 |
| servicediscovery | 1 op leaking, fixed | 268992473, gopherstack-tuh5 |
| glue | 3 ops leaking (schema-registry group), fixed; re-verified clean on 3 more ops in pass 4 | 58994c889 (gopherstack-uult), pass 4 |
| opensearch | 2 call sites leaking (VPC endpoints), fixed | 58994c889, gopherstack-uult |
| medialive | 1 op leaking (ListChannelPlacementGroups later found NOT over-wide, corrected), fixed; ListInputDevices phantom-field bug found as byproduct | 58994c889 (gopherstack-uult), 58994c889 (correction), c76de6864 (gopherstack-7ux2) |
| bedrock | 1 op leaking (ListModelImportJobs), fixed | 58994c889, gopherstack-uult |
| eks | 1 op leaking (ListInsights), fixed; ListPodIdentityAssociations/ListAssociatedAccessPolicies verified clean in pass 4 | 58994c889 (gopherstack-uult), pass 4 |
| iot | 3 ops leaking (ListCommands, ListPackages, ListPackageVersions), fixed; ListCommandExecutions had a separate wrong-name bug, fixed | 3d4b69050, gopherstack-g3jk, gopherstack-k26u |
| quicksight | 2 ops leaking, fixed | 3d4b69050, gopherstack-g3jk |
| backup | separate wrong-field-name bug (not over-wide), fixed | 3d4b69050, gopherstack-k26u |
| ecs | wrong-shape bug found as byproduct (ListServiceDeployments), fixed; daemon-family ops verified clean in pass 4 | c76de6864 (gopherstack-7ux2), pass 4 |
| cloudfront | verified clean, sampled the SDK-flagged narrow-split candidates plus classic families; ~120 remaining ops relied on SDK-shape classification only, not individually re-read | pass 4 |
| dynamodb | verified clean (ListBackups/Exports/Imports/ContributorInsights); ListExports separately found over-wide and fixed in a different issue | pass 4; 289ce97f9 (gopherstack-e3so) |
| sagemaker | all 26 flagged-by-name files individually read, all correctly narrow via inline `map[string]any` — the false-positive class this script's method is built to avoid re-deriving | pass 4 |
| codebuild | verified low-yield, 12/15 List ops return bare ID strings, not candidates | pass 4 |
| batch | verified clean, every op had an explicit comment citing the real SDK summary struct | pass 4 |
| waf | verified clean, 13/13 candidate ops. Every match-set/rule/ACL family already used a dedicated `*Summary` type, field-verified against `waf@v1.33.4` individually (not by analogy) | this session, `services/waf/PARITY.md` |
| kafka | 7/7 examined. 1 confirmed leak (`ListClusterOperationsV2`) fixed; 2 of the 7 were false positives on re-read (`ListClusters`/`ListClusterOperations` V1 both correctly reuse the same full type Describe returns — AWS itself doesn't narrow V1); 4 verified clean for over-wide specifically (`ListChannels`, `ListNodes`, `ListReplicators`, `ListTopics`), though `ListNodes` has a separate, larger wrong-shape bug filed as gopherstack-mk3t, not over-wide and not fixed | this session, `services/kafka/PARITY.md`, gopherstack-mk3t |

**omics' 3 open leaks, fixed this session** against pinned `omics@v1.49.5`:
`ListAnnotationStores`, `ListVariantStores` and `ListAnnotationStoreVersions`
each marshaled their full domain struct directly (confirmed by direct read
against `types.AnnotationStoreItem`/`VariantStoreItem`/
`AnnotationStoreVersionItem`, `types/types.go`). Each op now builds a
dedicated `*Summary` type instead: `AnnotationStoreSummary` (drops
`NumVersions`/`StoreOptions`/`Tags`), `VariantStoreSummary` (drops `Tags`),
`AnnotationStoreVersionSummary` (drops `Tags`/`StoreName`). Note the exact
leaked-field sets differ per op — `NumVersions`/`StoreOptions` only ever
applied to `AnnotationStore`, not the other two, contrary to how the
originating bd note characterized all three identically; each real type was
read individually rather than by analogy, per the mandatory instruction in
gopherstack-dv4s's notes. Byproduct gaps found and NOT fixed (missing/phantom
fields, the opposite bug class, out of this pass's scope): `VariantStoreItem`
requires `sseConfig`, which `VariantStore` has never tracked at all;
`AnnotationStoreVersionItem` requires `id` and a plain `name` distinct from
`versionName`, neither tracked; and `AnnotationStoreVersion.StoreName` is a
phantom field present on Get too (no such real member exists there either).
Tests: `services/omics/wire_field_additions_test.go`
`TestOmicsStoreLists_OmitGetOnlyFields` (table-driven, 3 subtests), raw-body
assertions, each hand-reverted and confirmed to fail against the pre-fix
code before being counted as proof.

## Off-limits this session (not "already examined" — just not touched)

`route53`, `iam`, `securityhub` and `opensearch` are held by a concurrent
agent on a different sweep this session and were left untouched regardless
of their status here. `opensearch` and `quicksight` happen to already be
examined for this bug class (table above); `route53`, `iam` and
`securityhub` are **not** examined for over-wide responses and remain in
the ranked table below — a future session clear of the conflict should
pick them up.

## Ranked candidates, unexamined (75 services, 301 candidate ops)

Regenerated fresh for this batch by `cmd/overwidecandidates`,
filtered against the "already examined" table above.

| Service | Candidate ops | Ops (real SDK Output struct declares a narrow Summary-shaped slice) |
|---|---|---|
| cloudformation | 15 | ListChangeSets, ListGeneratedTemplates, ListHookResults, ListResourceScans, ListStackInstanceResourceDrifts, ListStackInstances, ListStackRefactors, ListStackResources, ListStackSetAutoDeploymentTargets, ListStackSetOperationResults, ListStackSetOperations, ListStackSets, ListStacks, ListTypeVersions, ListTypes |
| vpclattice | 14 | ListAccessLogSubscriptions, ListDomainVerifications, ListListeners, ListResourceConfigurations, ListResourceEndpointAssociations, ListResourceGateways, ListRules, ListServiceNetworkResourceAssociations, ListServiceNetworkServiceAssociations, ListServiceNetworkVpcAssociations, ListServiceNetworks, ListServices, ListTargetGroups, ListTargets |
| bedrockagent | 13 | ListAgentActionGroups, ListAgentAliases, ListAgentCollaborators, ListAgentKnowledgeBases, ListAgentVersions, ListAgents, ListDataSources, ListFlowAliases, ListFlowVersions, ListFlows, ListIngestionJobs, ListKnowledgeBases, ListPrompts |
| ssm | 10 | ListAssociationVersions, ListCloudConnectors, ListComplianceItems, ListComplianceSummaries, ListDocumentVersions, ListOpsItemEvents, ListOpsItemRelatedItems, ListOpsMetadata, ListResourceComplianceSummaries, ListResourceDataSync |
| athena | 9 | ListCalculationExecutions, ListDataCatalogs, ListExecutors, ListNotebookMetadata, ListNotebookSessions, ListPreparedStatements, ListSessions, ListTableMetadata, ListWorkGroups |
| appmesh | 8 | ListGatewayRoutes, ListMeshes, ListRoutes, ListTagsForResource, ListVirtualGateways, ListVirtualNodes, ListVirtualRouters, ListVirtualServices |
| sesv2 | 8 | ListCustomVerificationEmailTemplates, ListEmailIdentities, ListEmailTemplates, ListExportJobs, ListImportJobs, ListResourceTenants, ListSuppressedDestinations, ListTenants |
| ssoadmin | 8 | ListAccountAssignmentCreationStatus, ListAccountAssignmentDeletionStatus, ListApplicationAuthenticationMethods, ListApplicationGrants, ListInstances, ListPermissionSetProvisioningStatus, ListRegions, ListTrustedTokenIssuers |
| wafv2 | 8 | ListAPIKeys, ListAvailableManagedRuleGroups, ListIPSets, ListManagedRuleSets, ListMobileSdkReleases, ListRegexPatternSets, ListRuleGroups, ListWebACLs |
| iam | 7 | ListAccessKeys, ListOpenIDConnectProviders, ListPoliciesGrantingServiceAccess, ListSAMLProviders, ListSSHPublicKeys, ListServerCertificates, ListServiceSpecificCredentials |
| macie2 | 7 | ListAllowLists, ListClassificationJobs, ListClassificationScopes, ListCustomDataIdentifiers, ListFindingsFilters, ListManagedDataIdentifiers, ListSensitivityInspectionTemplates |
| transcribe | 7 | ListCallAnalyticsJobs, ListMedicalScribeJobs, ListMedicalTranscriptionJobs, ListMedicalVocabularies, ListTranscriptionJobs, ListVocabularies, ListVocabularyFilters |
| apprunner | 6 | ListAutoScalingConfigurations, ListConnections, ListObservabilityConfigurations, ListOperations, ListServices, ListVpcIngressConnections |
| emr | 6 | ListClusters, ListNotebookExecutions, ListSecurityConfigurations, ListSteps, ListStudioSessionMappings, ListStudios |
| fis | 6 | ListActions, ListExperimentTargetAccountConfigurations, ListExperimentTemplates, ListExperiments, ListTargetAccountConfigurations, ListTargetResourceTypes |
| managedblockchain | 6 | ListAccessors, ListMembers, ListNetworks, ListNodes, ListProposalVotes, ListProposals |
| outposts | 6 | ListAssets, ListCapacityTasks, ListCatalogItems, ListOrderableInstanceTypes, ListOrders, ListQuotes |
| s3control | 6 | ListAccessGrants, ListAccessGrantsInstances, ListAccessGrantsLocations, ListCallerAccessGrants, ListStorageLensConfigurations, ListStorageLensGroups |
| securityhub | 6 | ListAutomationRules, ListConfigurationPolicies, ListConfigurationPolicyAssociations, ListConnectors, ListConnectorsV2, ListStandardsControlAssociations |
| accessanalyzer | 5 | ListAccessPreviews, ListAnalyzedResources, ListAnalyzers, ListArchiveRules, ListFindings |
| acm | 5 | ListAcmeAccounts, ListAcmeDomainValidations, ListAcmeEndpoints, ListAcmeExternalAccountBindings, ListCertificates |
| datasync | 5 | ListAgents, ListLocations, ListTagsForResource, ListTaskExecutions, ListTasks |
| iotanalytics | 5 | ListChannels, ListDatasetContents, ListDatasets, ListDatastores, ListPipelines |
| kms | 5 | ListAliases, ListGrants, ListKeyRotations, ListKeys, ListRetirableGrants |
| route53 | 5 | ListCidrBlocks, ListCidrCollections, ListCidrLocations, ListHostedZonesByVPC, ListTrafficPolicies |
| verifiedpermissions | 5 | ListIdentitySources, ListPolicies, ListPolicyStoreAliases, ListPolicyStores, ListPolicyTemplates |
| cloudwatchlogs | 4 | ListAggregateLogGroupSummaries, ListIntegrations, ListLogGroups, ListScheduledQueries |
| directoryservice | 4 | ListADAssessments, ListCertificates, ListIpRoutes, ListSchemaExtensions |
| grafana | 4 | ListPermissions, ListWorkspaceServiceAccountTokens, ListWorkspaceServiceAccounts, ListWorkspaces |
| inspector2 | 4 | ListCodeSecurityIntegrations, ListCodeSecurityScanConfigurationAssociations, ListCodeSecurityScanConfigurations, ListConnectorScanConfigurations |
| lambda | 4 | ListFunctionVersionsByCapacityProvider, ListLayerVersions, ListLayers, ListProvisionedConcurrencyConfigs |
| awsconfig | 3 | ListConfigurationRecorders, ListConnectors, ListStoredQueries |
| cloudtrail | 3 | ListImportFailures, ListImports, ListTrails |
| cloudwatch | 3 | ListAlarmMuteRules, ListDashboards, ListMetricStreams |
| codepipeline | 3 | ListPipelineExecutions, ListPipelines, ListWebhooks |
| comprehend | 3 | ListDocumentClassifierSummaries, ListEntityRecognizerSummaries, ListFlywheels |
| ec2 | 3 | ListImagesInRecycleBin, ListSnapshotsInRecycleBin, ListVolumesInRecycleBin |
| elasticsearch | 3 | ListDomainNames, ListVpcEndpoints, ListVpcEndpointsForDomain |
| iotwireless | 3 | ListEventConfigurations, ListPositionConfigurations, ListWirelessGatewayTaskDefinitions |
| kinesisanalyticsv2 | 3 | ListApplicationOperations, ListApplicationVersions, ListApplications |
| networkmanager | 3 | ListAttachmentRoutingPolicyAssociations, ListConnectPeers, ListCoreNetworks |
| organizations | 3 | ListPolicies, ListPoliciesForTarget, ListTargetsForPolicy |
| ram | 3 | ListPermissionVersions, ListPermissions, ListResourceSharePermissions |
| resiliencehub | 3 | ListAppAssessments, ListAppVersions, ListApps |
| resourcegroups | 3 | ListGroupResources, ListGroupingStatuses, ListTagSyncTasks |
| s3tables | 3 | ListNamespaces, ListTableBuckets, ListTables |
| serverlessrepo | 3 | ListApplicationDependencies, ListApplicationVersions, ListApplications |
| workmail | 3 | ListMailDomains, ListOrganizations, ListPersonalAccessTokens |
| apigatewayv2 | 2 | ListPortalProducts, ListPortals |
| ce | 2 | ListCommitmentPurchaseAnalyses, ListSavingsPlansPurchaseRecommendationGeneration |
| elasticbeanstalk | 2 | ListPlatformBranches, ListPlatformVersions |
| guardduty | 2 | ListInvestigations, ListMalwareProtectionPlans |
| iotdataplane | 2 | ListRetainedMessages, ListSubscriptions |
| lakeformation | 2 | ListLakeFormationOptIns, ListResources |
| mq | 2 | ListBrokers, ListUsers |
| route53resolver | 2 | ListFirewallDomainLists, ListFirewallRuleGroups |
| s3 | 2 | ListObjectAnnotations, ListObjectVersions |
| scheduler | 2 | ListScheduleGroups, ListSchedules |
| secretsmanager | 2 | ListSecretVersionIds, ListSecrets |
| ses | 2 | ListReceiptRuleSets, ListTemplates |
| amplify | 1 | ListJobs |
| appsync | 1 | ListSourceApiAssociations |
| bedrockruntime | 1 | ListAsyncInvokes |
| cloudfrontkeyvaluestore | 1 | ListKeys |
| codeconnections | 1 | ListRepositoryLinks |
| codestarconnections | 1 | ListRepositoryLinks |
| databrew | 1 | ListRulesets |
| kinesis | 1 | ListStreams |
| kinesisanalytics | 1 | ListApplications |
| mediastoredata | 1 | ListItems |
| mgn | 1 | ListNetworkMigrationDefinitions |
| networkmonitor | 1 | ListMonitors |
| rolesanywhere | 1 | ListSubjects |
| shield | 1 | ListAttacks |
| sqs | 1 | ListMessageMoveTasks |

Excluded entirely: `qldb`, `qldbsession` (no aws-sdk-go-v2 dependency to
diff against), `opsworks` (imports the SDK path but has no corresponding
`go.mod` entry — worth someone checking whether that's a dead import or a
missing pin).
