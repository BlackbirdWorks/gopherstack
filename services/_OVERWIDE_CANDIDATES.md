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
| cloudformation | 15/15 candidates read individually against `cloudformation@v1.76.1` types.go. Every op already had a dedicated inline/`type Foo` Summary — no over-wide leak found. Two side findings, neither fits this bug class so neither was fixed here: (1) `ListStackInstanceResourceDrifts` shares the `StackResourceDrift` Go struct with `DescribeStackResourceDrifts` (the Describe-vs-List shared-converter signal fired), and that struct declares `ExpectedProperties`/`ActualProperties` which real `types.StackInstanceResourceDriftsSummary` doesn't have — but the List backend method (`stack_instances.go` `ListStackInstanceResourceDrifts`) only ever populates StackID/LogicalResourceID/StackResourceDriftStatus, so those two fields are structurally always empty and, with `omitempty`, never reach the wire; not a live leak, no fixture could fail against it, not fixed. (2) `ListStackInstances`' dedicated `instXML` emits a `StackSetName` field that doesn't exist on either the real full `StackInstance` type or `StackInstanceSummary` — a phantom-field bug (wrong shape), not a Get-field leak; not fixed under this issue, same disposition as kafka's `ListNodes` (gopherstack-mk3t). | this session |
| vpclattice | 14/14 candidates read against `vpclattice@v1.25.5` types.go. Every op uses a dedicated hand-built `*Summary` type declared once in `interfaces.go`, each verified field-by-field a strict subset of its real SDK counterpart (never a superset) — both at the Go-struct layer and at the `*ToJSON` wire-serialization layer (`serviceSummaryToJSON`, `listenerSummaryToJSON`, etc., each with its own function distinct from the Get-shaped `*ToJSON`). Zero leaks. | this session |
| bedrockagent | 13/13 candidates read against `bedrockagent@v1.58.4` types.go. 2 confirmed leaks, both fixed (see below); 11 clean. The shared-converter grep (checking each backend `List*` method's return type for a dedicated `*Summary` vs. the full Get type) found both leaks in under a minute, before any field-by-field read — `ListAgentKnowledgeBases` returned `[]*AgentKnowledgeBase` (Get-shaped) and `ListFlowVersions` built `*FlowVersionSummary` but the struct itself over-declared fields matching `FlowVersion` (Get-shaped). `ListAgentCollaborators`/`ListIngestionJobs` also lacked a dedicated Summary type but were verified clean field-by-field — the shared Go struct happens to already track exactly the real Summary's field set (gopherstack never modeled the one extra Get-only field either type has: `ClientToken`, `FailureReasons`), so absence of a dedicated type name is a necessary-but-not-sufficient signal, not proof of a leak. Side finding, not fixed (different bug class, same disposition as kafka's `ListNodes`): `AgentCollaborator` carries a `CollaboratorStatus` field that doesn't exist on real `AgentCollaborator` or `AgentCollaboratorSummary` at all — a phantom field present identically on both Get and List, not a Get-only leak. | this session, gopherstack-dv4s |
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
| ssm | 10/10 examined against `ssm@v1.73.4`. 2 confirmed leaks (`ListAssociationVersions` returned `[]Association` leaking Overview/InstanceId/LastUpdateAssociationDate instead of `types.AssociationVersionInfo`; `ListDocumentVersions` returned `[]DocumentVersion` leaking document `Content` string instead of `types.DocumentVersionInfo`), both fixed with dedicated summary types; 8 verified clean (`ListCloudConnectors`, `ListComplianceItems`, `ListComplianceSummaries`, `ListOpsItemEvents`, `ListOpsItemRelatedItems`, `ListOpsMetadata`, `ListResourceComplianceSummaries`, `ListResourceDataSync`) | this session, `services/ssm/wire_field_fixes_test.go` |
| athena | 9/9 examined against `athena@v1.60.4`. Verified clean, all 9 ops already use dedicated summary structs | this session |
| wafv2 | 8/8 examined against `wafv2@v1.60.4`. Verified clean, all 8 ops narrow via inline map summaries | this session |
| appmesh | 8/8 examined against `appmesh@v1.36.4`. Verified clean, all 8 ops already use dedicated `*Summary` structs | this session |
| sesv2 | 8/8 examined against `sesv2@v1.60.1`. Verified clean, all 8 ops modeled via `wire_output.go` narrow DTOs | this session |
| ssoadmin | 8/8 examined against `ssoadmin@v1.43.1`. Verified clean, all 8 ops use dedicated summary views | this session |
| macie2 | 7/7 examined against `macie2@v1.48.4`. Verified clean, all 7 ops already use dedicated summary structs | this session |
| transcribe | 7/7 examined against `transcribe@v1.54.4`. Verified clean, all 7 ops already use dedicated summary structs | this session |
| apprunner | 6/6 examined against `apprunner@v1.44.4`. Verified clean, all 6 ops already use dedicated summary structs | this session |
| emr | 6/6 examined against `emr@v1.43.4`. Verified clean, all 6 ops already use dedicated summary structs | this session |
| fis | 6/6 examined against `fis@v1.40.4`. 2 confirmed leaks (`ListExperimentTemplates` returned full `experimentTemplateDTO` with targets/actions/logConfig/roleArn/stopConditions; `ListExperiments` returned full `experimentDTO` with targets/actions/logConfig/roleArn/stopConditions/startTime/endTime/executionId), both fixed with `experimentTemplateSummaryDTO` and `experimentSummaryDTO`; 4 verified clean (`ListActions`, `ListTargetResourceTypes`, `ListTargetAccountConfigurations`, `ListExperimentTargetAccountConfigurations`) | this session, `services/fis/wire_field_test.go` |
| managedblockchain | 6/6 examined against `managedblockchain@v1.30.4`. Verified clean, all 6 ops already use dedicated summary structs | this session |
| outposts | 6/6 examined against `outposts@v1.46.4`. Verified clean, all 6 ops modeled via `wire.go` dedicated DTOs | this session |
| s3control | 6/6 examined against `s3control@v1.54.4`. Verified clean, all 6 ops use dedicated XML list result DTOs | this session |
| acm | 5/5 examined against `acm@v1.43.4`. Verified clean, all 5 ops already use dedicated summary structs | this session |
| datasync | 5/5 examined against `datasync@v1.45.4`. Verified clean, all 5 ops already use dedicated list entry DTOs | this session |
| iotanalytics | 5/5 examined against `iotanalytics@v1.31.4`. Verified clean, all 5 ops already use dedicated summary structs | this session |
| kms | 5/5 examined against `kms@v1.50.4`. Verified clean, all 5 ops already use dedicated list entry structs | this session |
| verifiedpermissions | 5/5 examined against `verifiedpermissions@v1.30.4`. Verified clean, all 5 ops already use dedicated view structs | this session |
| cloudwatchlogs | 4/4 examined against `cloudwatchlogs@v1.81.1`. Verified clean, all 4 ops already use dedicated summary shapes | this session |
| directoryservice | 4/4 examined against `directoryservice@v1.40.4`. Verified clean, all 4 ops already use dedicated summary structs | this session |
| grafana | 4/4 examined against `grafana@v1.30.4`. Verified clean, all 4 ops modeled via `wire.go` dedicated DTOs | this session |
| inspector2 | 4/4 examined against `inspector2@v1.35.4`. Verified clean, all 4 ops already use dedicated summary models | this session |
| lambda | 4/4 examined against `lambda@v1.101.2`. 2 confirmed leaks (`ListLayers` and `ListLayerVersions` leaked `Content` with `CodeSize`); both fixed by omitting `Content`; 2 verified clean (`ListFunctionVersionsByCapacityProvider`, `ListProvisionedConcurrencyConfigs`) | this session, `services/lambda/wire_field_test.go` |
| accessanalyzer | 5/5 examined against `accessanalyzer@v1.45.4`. Verified clean, all 5 ops already use dedicated summary structs | this session |
| awsconfig | 3/3 examined against `configservice@v1.48.4`. Verified clean, all 3 ops already use dedicated summary structs | this session |
| cloudtrail | 3/3 examined against `cloudtrail@v1.43.4`. Verified clean, all 3 ops use dedicated narrow summary views | this session |
| cloudwatch | 3/3 examined against `cloudwatch@v1.49.4`. Verified clean, all 3 ops already use dedicated summary structs | this session |
| codepipeline | 3/3 examined against `codepipeline@v1.41.4`. Verified clean, all 3 ops already use dedicated summary structs | this session |
| comprehend | 3/3 examined against `comprehend@v1.38.4`. Verified clean, all 3 ops already use dedicated summary structs | this session |
| ec2 | 3/3 examined against `ec2@v1.268.0`. Verified clean, all 3 ops use dedicated XML list result DTOs | this session |
| elasticsearch | 3/3 examined against `elasticsearchservice@v1.32.4`. Verified clean, all 3 ops use dedicated summary structs | this session |
| iotwireless | 3/3 examined against `iotwireless@v1.30.4`. Verified clean, all 3 ops use dedicated summary structs | this session |
| kinesisanalyticsv2 | 3/3 examined against `kinesisanalyticsv2@v1.31.4`. Verified clean, all 3 ops use dedicated summary structs | this session |
| networkmanager | 3/3 examined against `networkmanager@v1.37.4`. Verified clean, all 3 ops modeled via dedicated `wire.go` DTOs | this session |
| organizations | 3/3 examined against `organizations@v1.36.4`. Verified clean, all 3 ops use dedicated summary structs | this session |
| ram | 3/3 examined against `ram@v1.39.4`. Verified clean, all 3 ops use dedicated summary structs | this session |
| resiliencehub | 3/3 examined against `resiliencehub@v1.30.4`. Verified clean, all 3 ops modeled via dedicated `wire.go` DTOs | this session |
| resourcegroups | 3/3 examined against `resourcegroups@v1.31.4`. Verified clean, all 3 ops use dedicated summary structs | this session |
| s3tables | 3/3 examined against `s3tables@v1.5.0`. Verified clean, all 3 ops use dedicated summary structs | this session |
| serverlessrepo | 3/3 examined against `serverlessapplicationrepository@v1.31.4`. Verified clean, all 3 ops use dedicated summary structs | this session |
| workmail | 3/3 examined against `workmail@v1.33.4`. Verified clean, all 3 ops use dedicated summary structs | this session |
| apigatewayv2 | 2/2 examined against `apigatewayv2@v1.37.4`. Verified clean, all 2 ops use dedicated summary structs | this session |
| ce | 2/2 examined against `costexplorer@v1.44.4`. Verified clean, all 2 ops use dedicated summary structs | this session |
| elasticbeanstalk | 2/2 examined against `elasticbeanstalk@v1.37.4`. Verified clean, all 2 ops use dedicated summary structs | this session |
| guardduty | 2/2 examined against `guardduty@v1.49.4`. Verified clean, all 2 ops use dedicated summary structs | this session |
| iotdataplane | 2/2 examined against `iotdataplane@v1.30.4`. Verified clean, all 2 ops use dedicated summary structs | this session |
| lakeformation | 2/2 examined against `lakeformation@v1.40.4`. Verified clean, all 2 ops use dedicated wire structs | this session |
| mq | 2/2 examined against `mq@v1.30.4`. Verified clean, all 2 ops use dedicated summary structs | this session |
| route53resolver | 2/2 examined against `route53resolver@v1.48.4`. Verified clean, all 2 ops use dedicated metadata structs | this session |
| s3 | 2/2 examined against `s3@v1.96.0`. Verified clean, all 2 ops use dedicated XML list result DTOs | this session |
| scheduler | 2/2 examined against `scheduler@v1.31.4`. Verified clean, all 2 ops use dedicated summary structs | this session |
| secretsmanager | 2/2 examined against `secretsmanager@v1.42.4`. Verified clean, all 2 ops use dedicated summary structs | this session |
| ses | 2/2 examined against `ses@v1.33.4`. Verified clean, all 2 ops use dedicated XML list result DTOs | this session |
| amplify | 1/1 examined against `amplify@v1.36.4`. Verified clean, `ListJobs` uses dedicated summary struct | this session |
| appsync | 1/1 examined against `appsync@v1.56.4`. 1 confirmed leak (`ListSourceApiAssociations` leaked `sourceApiAssociationStatus`/`Detail` and `Config`); fixed by mapping to dedicated `SourceAPIAssociationSummary` | this session, `services/appsync/wire_field_test.go` |
| bedrockruntime | 1/1 examined against `bedrockruntime@v1.27.0`. Verified clean, `ListAsyncInvokes` uses dedicated summary struct | this session |
| cloudfrontkeyvaluestore | 1/1 examined against `cloudfrontkeyvaluestore@v1.15.4`. Verified clean, `ListKeys` uses dedicated summary struct | this session |
| codeconnections | 1/1 examined against `codeconnections@v1.16.4`. Verified clean, `ListRepositoryLinks` uses dedicated summary struct | this session |
| codestarconnections | 1/1 examined against `codestarconnections@v1.30.4`. Verified clean, `ListRepositoryLinks` uses dedicated summary struct | this session |
| databrew | 1/1 examined against `databrew@v1.32.4`. Verified clean, `ListRulesets` uses dedicated summary struct | this session |
| kinesis | 1/1 examined against `kinesis@v1.46.4`. Verified clean, `ListStreams` returns standard StreamNames string list | this session |
| kinesisanalytics | 1/1 examined against `kinesisanalytics@v1.30.4`. Verified clean, `ListApplications` uses dedicated summary struct | this session |
| mediastoredata | 1/1 examined against `mediastoredata@v1.30.4`. Verified clean, `ListItems` uses dedicated summary struct | this session |
| mgn | 1/1 examined against `mgn@v1.40.4`. Verified clean, `ListNetworkMigrationDefinitions` uses dedicated summary struct | this session |
| networkmonitor | 1/1 examined against `networkmonitor@v1.16.4`. Verified clean, `ListMonitors` uses dedicated summary struct | this session |
| rolesanywhere | 1/1 examined against `rolesanywhere@v1.30.4`. Verified clean, `ListSubjects` uses dedicated summary struct | this session |
| shield | 1/1 examined against `shield@v1.30.4`. Verified clean, `ListAttacks` uses dedicated summary struct | this session |
| sqs | 1/1 examined against `sqs@v1.50.4`. Verified clean, `ListMessageMoveTasks` uses dedicated summary struct | this session |

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

## Ranked candidates, unexamined (72 services, 259 candidate ops)

Regenerated fresh for batch five by `cmd/overwidecandidates`, filtered
against the "already examined" table above. Batch six (this session) cleared
cloudformation, vpclattice and bedrockagent (42 ops) off the top of this
list without a tool re-run — the counts below are stale by exactly those
three rows; a future session should either skip them by hand (as done here)
or regenerate.

| Service | Candidate ops | Ops (real SDK Output struct declares a narrow Summary-shaped slice) |
|---|---|---|
| iam | 7 | ListAccessKeys, ListOpenIDConnectProviders, ListPoliciesGrantingServiceAccess, ListSAMLProviders, ListSSHPublicKeys, ListServerCertificates, ListServiceSpecificCredentials |
| securityhub | 6 | ListAutomationRules, ListConfigurationPolicies, ListConfigurationPolicyAssociations, ListConnectors, ListConnectorsV2, ListStandardsControlAssociations |
| route53 | 5 | ListCidrBlocks, ListCidrCollections, ListCidrLocations, ListHostedZonesByVPC, ListTrafficPolicies |

Excluded entirely: `qldb`, `qldbsession` (no aws-sdk-go-v2 dependency to
diff against), `opsworks` (imports the SDK path but has no corresponding
`go.mod` entry — worth someone checking whether that's a dead import or a
missing pin).
