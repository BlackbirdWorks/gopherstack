# Gopherstack Parity Audit — LocalStack & AWS Emulation Gaps

> Deep-dive audit of gopherstack's AWS emulation fidelity against real AWS behaviour
> and LocalStack feature coverage. Every finding below was confirmed by reading the
> referenced handler/backend source (citations are `file:line`). Audit date: 2026-06-05.
>
> This document is a **behavioural & compatibility** audit. It complements:
> - `PARITY.md` — a changelog of already-fixed wire-protocol/parity bugs.
> - `STATUS.md` / `MISSING.md` — service-level coverage matrices (note: both are **stale** —
>   they list ~45 services, but `services/` actually contains **148**).

## How to read this

Gopherstack is a **high-fidelity** emulator. Many cross-service integrations that
matter for realistic testing — SNS→SQS fan-out, S3 event notifications, Lambda
event-source mappings, real Lambda container execution, real KMS crypto, real API
Gateway invocation, optional IAM enforcement — are genuinely implemented. This audit
deliberately focuses on the **gaps** that remain, so it reads more negatively than the
codebase deserves. Severity legend:

| Severity | Meaning |
|---|---|
| 🔴 **High** | Silently wrong / breaks common real-world usage (false success, dropped delivery, unsupported core intrinsic). |
| 🟠 **Medium** | Feature accepted but ignored, or fidelity materially diverges from AWS. |
| 🟡 **Low** | Niche / simplified semantics / cosmetic. |

---

## 1. Cross-service integration gaps (behavioural)

These are the highest-impact gaps because integration tests depend on them.

| # | Sev | Service | Gap | Citation |
|---|-----|---------|-----|----------|
| 1 | 🔴 | **Step Functions** | A `Task` whose `Resource` is an **unsupported `arn:aws:states:::` integration** (ECS, Glue, Batch, EMR, SageMaker, API Gateway, EventBridge, etc.) silently passes the input through as a **successful output** — the service is never invoked. Tests get false-green. | `services/stepfunctions/asl/executor.go:963` |
| 2 | 🔴 | **SNS → Lambda / Firehose** | Subscriptions with protocol `lambda` or `firehose` are accepted but **never deliver**. Only SQS (and HTTP/S) consume the publish emitter; `collectPublishTargets` builds no Lambda/Firehose deliveries. | `services/sns/backend.go:1426` |
| 3 | 🔴 | **EventBridge → Step Functions** | A rule target that is a Step Functions state-machine ARN hits the `default` branch ("unsupported target ARN type") and **never starts an execution**. Lambda/SQS/SNS/Kinesis/Firehose/ECS targets do work. | `services/eventbridge/delivery.go:378` |
| 4 | 🔴 | **Firehose (Kinesis source)** | `KinesisStreamAsSource` delivery streams store the source config but **never poll the source stream** — no records ever reach S3. `PutRecord`/`PutRecordBatch` also reject non-DirectPut streams. | `services/firehose/backend.go:447` |
| 5 | 🟠 | **Lambda SQS ESM** | Partial-batch-failure reporting is ignored: when the invocation returns no error, **all** received messages are deleted regardless of a `batchItemFailures` response, so `ReportBatchItemFailures` re-queue semantics are lost. | `services/lambda/event_source_poller.go:615` |
| 6 | 🟠 | **Firehose destinations** | Only S3 destinations deliver. Redshift is a stub; Elasticsearch/OpenSearch/Splunk/HTTP destinations are not delivered. | `services/firehose/backend.go:230` |
| 7 | 🟠 | **SNS→SQS envelope** | The non-raw notification envelope omits the `MessageAttributes` map AWS includes, so consumers reading attributes from the JSON body get nothing (raw delivery is unaffected). `Signature` is a random UUID, not verifiable. | `services/sqs/sns_delivery.go:173` |
| 8 | 🟡 | **Lambda DDB-stream ESM** | All DynamoDB-stream consumption goes through one hard-coded shard ID, so multi-shard streams aren't faithfully partitioned. | `services/lambda/event_source_poller.go:22` |

**Confirmed GOOD (not gaps):** SNS→SQS fan-out with full FilterPolicy/RawMessageDelivery/DLQ
(`services/sqs/sns_delivery.go`), S3 events → SQS/SNS/Lambda/EventBridge with key filters
(`services/s3/notification.go`), real Lambda container execution (`services/lambda/runtime_api.go`),
Lambda ESM polling for Kinesis/SQS/DDB, DynamoDB expression engine/streams/transactions/TTL,
SQS FIFO/dedup/visibility/DLQ, Step Functions Map/Parallel/Retry/Catch/waitForTaskToken.

---

## 2. CloudFormation: template-engine gaps

CFN is unusually faithful for an emulator — it provisions by calling **real service
backends** (50+ backends wired in `ServiceBackends`, `services/cloudformation/resources.go:73`),
with ~114 registered resource types, real nested stacks, change sets, and `Fn::ImportValue`
exports. But the template intrinsic engine has major holes:

| # | Sev | Gap | Citation |
|---|-----|-----|----------|
| 9 | 🔴 | **`Fn::GetAtt` is completely unsupported.** A `{"Fn::GetAtt": [...]}` value falls through to `fmt.Sprintf("%v", val)`, producing a garbage string. GetAtt is one of the most-used intrinsics — many real templates will silently mis-provision. | `services/cloudformation/template.go:387` |
| 10 | 🔴 | **No pseudo-parameters.** `AWS::Region`, `AWS::AccountId`, `AWS::StackName`, `AWS::Partition`, `AWS::NoValue`, `AWS::URLSuffix` resolve to the literal ref string (e.g. the string `"AWS::Region"`). | `services/cloudformation/template.go:375` |
| 11 | 🟠 | **`Fn::Sub` is shallow** — only `${param}`/`${logicalId}`; no `${Resource.Attribute}` (GetAtt-style) refs and no second-arg variable-map form. | `services/cloudformation/template.go:431` |
| 12 | 🟠 | **Drift detection is fake** — `DetectStackDrift`/`DetectStackResourceDrift` unconditionally report `IN_SYNC` / 0 drifted resources with no real comparison. | `services/cloudformation/backend_ext.go:18` |
| 13 | 🟡 | **Missing intrinsics:** `Fn::Base64`, `Fn::GetAZs`, `Fn::Cidr`, `Fn::Transform`, `Fn::Length`, `Fn::ToJsonString`. | `services/cloudformation/template.go:387` |

**GOOD:** `Ref`, `Fn::Sub` (basic), `Fn::Join`, `Fn::Split`, `Fn::Select`, `Fn::FindInMap`,
`Fn::If`, `Fn::ImportValue`, nested stacks, change sets.

---

## 3. Single-service behavioural fidelity gaps

| # | Sev | Service | Gap | Citation |
|---|-----|---------|-----|----------|
| 14 | 🔴 | **Secrets Manager** | `RotateSecret` **does not invoke the rotation Lambda**. With a `RotationLambdaARN` set, it just assigns a fresh UUID as the new value — the customer's createSecret/setSecret/testSecret/finishSecret logic never runs. Staging-label transitions are simulated. | `services/secretsmanager/backend.go:1201` |
| 15 | 🟠 | **CloudWatch** | **Metric alarms never auto-evaluate.** `PutMetricAlarm` defaults to `INSUFFICIENT_DATA` and only `SetAlarmState` (a manual API call) changes state — there is no background evaluator comparing thresholds against stored datapoints. (Metrics, math, and composite-alarm rule evaluation over child states are real.) | `services/cloudwatch/backend.go:1010` |
| 16 | 🟠 | **Athena** | `GetQueryResults` always returns an **empty result set** — queries are accepted and tracked as metadata but never executed. | `services/athena/handler.go:634` |
| 17 | 🟠 | **EC2** | **No intermediate instance states.** `RunInstances` jumps straight to `running`; Start/Stop skip `pending`/`stopping`/`shutting-down`. The constants exist but are never assigned, so a poll never observes a transitional state. | `services/ec2/backend.go:519` |
| 18 | 🟠 | **S3** | Lifecycle **storage-class Transitions are parsed and ignored** ("transitions are not enforced"). Expiration + AbortIncompleteMultipartUpload do work. | `services/s3/janitor.go:526` |
| 19 | 🟠 | **S3** | **Bucket replication is config-only** — `PutBucketReplication` stores the XML but no objects are ever replicated to the destination bucket. | `services/s3/bucket_ops.go:1371` |
| 20 | 🟠 | **S3** | Lambda notification configs have **no key-filter support** (the `lambdaConfiguration` struct lacks a `Filter` field), unlike the SQS/SNS paths — prefix/suffix filters on Lambda targets are silently ignored. | `services/s3/notification.go:43` |
| 21 | 🟡 | **SSM** | SecureString encryption uses a **hard-coded package-level mock key**, not the KMS backend or the parameter's `KeyID`. `KeyID` is stored but unused, so per-key isolation / KMS key policies don't apply. (Crypto itself is real AES-256-GCM and respects `WithDecryption`.) | `services/ssm/backend.go:51` |
| 22 | 🟡 | **Kinesis** | `SubscribeToShard` (enhanced fan-out) is **single-shot**, returning available records once instead of the continuous ~5-min HTTP/2 push stream. Records are real. | `services/kinesis/backend.go:1198` |
| 23 | 🟡 | **S3** | `WriteGetObjectResponse` (S3 Object Lambda) is a no-op `200`. | `services/s3/handler_stubs.go:417` |

**GOOD:** KMS real AES-256-GCM / RSA-OAEP / ECDSA / HMAC crypto with encryption-context AAD
(`services/kms/crypto.go`), API Gateway real invocation of AWS_PROXY/AWS/HTTP/MOCK integrations
with VTL + authorizers (`services/apigateway/proxy.go`), IAM real policy evaluator + optional
enforcement middleware behind `--enforce-iam` (`services/iam/middleware.go:66`), STS real session
credentials (`services/sts/backend.go:483`), EC2 optional Docker-backed compute
(`services/ec2/docker_compute.go`).

---

## 4. Operation-coverage gaps (acknowledged `notImplemented`)

Each service's `services/<svc>/sdk_completeness_test.go` lists AWS SDK operations explicitly
acknowledged as **not implemented** in a `notImplemented := []string{…}` slice. The handler answers
a subset of the service's API; these ops return an unknown-operation error.

**Exactly 19 services have an incomplete completeness test** (a non-empty `notImplemented` slice),
totalling **1,011 acknowledged-missing operations**. The summary table below gives counts; the
**complete operation-by-operation work list for all 1,011 ops is in [Appendix A](#appendix-a--complete-missing-operation-inventory-all-1011-ops)**.
Every other service carries an **empty** slice, i.e. full op-level coverage of its SDK surface.

| Service | # missing ops | Representative missing ops |
|---|---:|---|
| **quicksight** | 187 | CreateFolder, CreateTemplate, CreateTheme, CreateTopic, CreateVPCConnection, CreateIAMPolicyAssignment, … |
| **medialive** | 103 | CreateMultiplex, CreateCluster, CreateNode, CreateNetwork, BatchUpdateSchedule, CreateChannelPlacementGroup, … |
| **workspaces** | 76 | CreateWorkspaceImage, CreateWorkspaceBundle, CreateIpGroup, CreateConnectionAlias, CreateWorkspacesPool, … |
| **appstream** | 72 | CreateAppBlock, CreateApplication, CreateImageBuilder, CreateEntitlement, CreateDirectoryConfig, … |
| **securityhub** | 66 | CreateConfigurationPolicy, CreateMembers, CreateFindingAggregator, CreateAutomationRuleV2, … |
| **directoryservice** | 64 | ConnectDirectory, CreateTrust, CreateComputer, AddIpRoutes, CreateLogSubscription, … |
| **inspector2** | 62 | AssociateMember, CreateSbomExport, CreateFindingsReport, BatchGetCodeSnippet, … |
| **guardduty** | 57 | CreateMembers, CreatePublishingDestination, CreateThreatEntitySet, DescribeMalwareScans, … |
| **rekognition** | 56 | CompareFaces, CreateProject, CreateProjectVersion, CreateFaceLivenessSession, CreateUser, … |
| **macie2** | 55 | CreateClassificationJob, CreateMember, DescribeBuckets, CreateInvitations, … |
| **fsx** | 37 | CreateVolume, CreateSnapshot, CreateStorageVirtualMachine, CreateFileCache, CreateDataRepositoryTask, … |
| **workmail** | 36 | CreateMobileDeviceAccessRule, CreateAvailabilityConfiguration, CreateIdentityCenterApplication, … |
| **datasync** | 32 | CreateLocationNfs/Smb/Efs/FsxLustre/ObjectStorage/AzureBlob/Hdfs, … |
| **apprunner** | 25 | CreateConnection, CreateVpcConnector, CreateAutoScalingConfiguration, AssociateCustomDomain, … |
| **mediatailor** | 24 | CreateProgram, CreateLiveSource, CreatePrefetchSchedule, GetChannelSchedule, … |
| **accessanalyzer** | 23 | CreateAccessPreview, CheckNoPublicAccess, GenerateFindingRecommendation, ApplyArchiveRule, … |
| **detective** | 19 | AcceptInvitation, StartInvestigation, ListIndicators, BatchGetGraphMemberDatasources, … |
| **rolesanywhere** | 13 | DeleteCrl, DisableCrl, EnableCrl, GetCrl, ImportCrl, ListCrls, GetSubject, PutAttributeMapping, … |
| **mediapackage** | 4 | CreateHarvestJob, DescribeHarvestJob, ListHarvestJobs, RotateIngestEndpointCredentials |
| **TOTAL** | **1,011** | across 19 services |

These are mostly newer / LocalStack-Pro-tier or rarely-used management operations. Core data
services (S3, DynamoDB, SQS, SNS, Lambda, Kinesis, EC2, IAM, KMS, STS, …) carry an **empty**
`notImplemented` slice — full op-level coverage of the SDK surface.

---

## 5. Persistence / Snapshot data-loss gaps

108 services define a `services/<svc>/persistence.go` with a `backendSnapshot` (or `dbSnapshot`)
struct that **enumerates only a subset of backend fields**. Any backend state not named in the
struct is silently dropped on `Snapshot`/`Restore`. This allowlist pattern is inherently fragile:
adding a new backend map without updating `persistence.go` introduces a silent restart-data-loss bug.

| # | Sev | Service | Finding | Citation |
|---|-----|---------|---------|----------|
| 24 | 🔴 | **EC2** | Worst offender. The backend has 100+ field maps; the snapshot persists only ~38. **Dropped on restore:** VPN gateways/connections, customer gateways, IPAMs & pools, Verified Access (instances/groups/endpoints/trust-providers), Traffic Mirror (sessions/filters/targets), Recycle Bin (images/snapshots/volumes), Reserved Instances (+ offerings/listings/modifications), managed prefix lists, Client VPN endpoints, carrier gateways, fleets/spot-fleets, Network Insights, transit-gateway routing maps, and many attribute maps (address/image attributes, EBS encryption defaults, serial-console access, block-public-access, …). | `services/ec2/persistence.go:20` vs `services/ec2/backend.go:184` |
| 25 | 🟠 | **DynamoDB** | `exports` and `imports` (export/import task records) are durable state but not in `dbSnapshot` → lost on restore. (`txnTokens`, `fisReplicationPaused`, `streamSeq` omissions are intentional/transient.) | `services/dynamodb/persistence.go:11` vs `services/dynamodb/store.go:115` |
| 26 | 🟡 | **Kinesis** | `onDemandStreamCountLimit` resets to default on restore; `fisThroughputFaults` (transient) dropped. | `services/kinesis/persistence.go:8` |

**GOOD:** CloudWatch's snapshot covers all 10 data maps + a separate handler-tag snapshot — no
data gap (`services/cloudwatch/persistence.go:9`).

---

## 6. LocalStack drop-in compatibility gaps

These don't affect emulation correctness but break **drop-in replacement** for LocalStack tooling.

| # | Sev | Gap | Detail | Citation |
|---|-----|-----|--------|----------|
| 27 | 🔴 | **Region isolation is not universal** | See §8 — this is the single largest cross-cutting compatibility gap and now has its own section. | — |
| 28 | 🟠 | **No CORS** | No CORS middleware is registered, so **browser-based** AWS SDK (`@aws-sdk/*` in a webpage) calls fail preflight. LocalStack returns permissive CORS headers. | `cli.go:1985` (middleware chain) |
| 29 | 🟠 | **Lambda packaging** | **Image-based functions only** (`PackageType: Image`). Zip uploads, S3 code delivery, and inline code — all supported by LocalStack — are rejected. | README "Lambda (image-based only)" |
| 30 | 🟡 | **Thin env-config surface** | Only `AWS_ACCESS_KEY_ID/SECRET/REGION/DEFAULT_REGION` are read from the environment; LocalStack's rich `SERVICES`, `PERSISTENCE`, `DEBUG`, `GATEWAY_LISTEN`, `LAMBDA_*` knobs have no equivalent (gopherstack uses CLI flags instead). | grep `os.Getenv` in `cmd/`, `cli.go`, `pkgs/config` |
| 31 | 🟡 | **No SigV4 verification** | No request-signature validation anywhere in the pipeline. This **matches** LocalStack's default (expected), but means issued STS credentials aren't cryptographically enforced on later calls. | (no `sigv4` verify outside vendored SDK) |

**GOOD:** Startup **init hooks** are supported (`pkgs/inithooks`), analogous to LocalStack's
`init/ready.d` lifecycle. A health endpoint and the root `/` GET both report running state.

### Service-directory coverage vs LocalStack
Gopherstack's 148 service directories are a **superset** of the common LocalStack service list.
The only notable named service on that list with **no** gopherstack directory is:

- **`account`** (AWS Account Management API) — absent.

(See §4: directory presence ≠ full op coverage — quicksight/medialive/workspaces/etc. exist as
directories but have large `notImplemented` slices.)

---

## 7. Suggested priority order

1. **CFN `Fn::GetAtt` + pseudo-parameters** (#9, #10) — unblocks the largest class of real templates.
2. **Step Functions silent pass-through** (#1) — turns false-green tests into honest failures or real integrations.
3. **SNS→Lambda fan-out** (#2) and **EventBridge→Step Functions** (#3) — common event-driven patterns.
4. **Firehose Kinesis-source polling** (#4) and **Lambda SQS partial-batch failures** (#5).
5. **Universal region support** (§8, #27) — every service must isolate state per region; this is the single largest cross-cutting correctness gap.
6. **EC2 persistence allowlist** (#24) — close the largest restart-data-loss surface.
7. **Secrets Manager rotation Lambda invocation** (#14) and **CloudWatch alarm auto-evaluation** (#15).

---

## 8. Region support is mandatory — and currently inconsistent 🔴

**Requirement: every service must isolate its state per AWS region.** In real AWS (and in
LocalStack), a resource created in `us-east-1` is **not** visible from `eu-west-1`. A bucket, table,
queue, function, secret, or stream lives in exactly one region, and a `List*`/`Describe*` call only
returns the resources in the caller's region. This is foundational behaviour that multi-region
applications, disaster-recovery tests, and cross-region replication tests depend on. Gopherstack
must honour it **uniformly across all 148 services**, not as a per-service opt-in.

### Current state: region isolation is the exception, not the rule

The plumbing exists but is wired into only a handful of services:

- **Region extraction** is available via `httputils.ExtractRegionFromRequest` (parses the SigV4
  `Credential=.../<region>/...` scope, falling back to a default) — `pkgs/httputils/httputils.go:308`.
- **Only ~10 of 148 services consume it.** Direct callers: `kinesis`, `kms`, `sns`, `sqs`,
  `secretsmanager`, `mwaa`, `pinpoint`. Context-based callers: `dynamodb` and `s3` inject the region
  into the request context (`services/dynamodb/handler.go:374`, `services/s3/handler.go:318`) and
  read it back via `regionContextKey{}`. A couple more (`s3control`, `memorydb`) partition some maps
  by region.
- **The other ~138 services store a single `Region string` on the backend** (≈136 backends declare a
  scalar `Region`/`region` field) and **do not partition their data maps by region**. The field is
  used to build ARNs in responses, but every `List*`/`Describe*` returns **all** resources regardless
  of the caller's region. So a table/instance/topic created against `us-east-1` is fully visible when
  the client targets `ap-southeast-2`.

Even the "region-aware" services are only **partially** so — e.g. S3 tracks a bucket→region index
(`services/s3/backend_memory.go:109`) and rejects cross-region bucket access, but most services that
read the region use it only for ARN construction, not for state isolation or for filtering list
results.

### What "supported" must mean (acceptance criteria)

For each service, region support is complete only when **all** of the following hold:

1. **Ingress:** the handler resolves the request region for every operation (SigV4 scope →
   `X-Amz-*` region hints → configured default), and threads it through `context.Context`.
2. **Storage:** backend state is keyed by region (e.g. `map[region]map[id]*Resource`, or a `region`
   field that is part of every lookup key), so two regions never share a resource namespace.
3. **Reads:** `List*` / `Describe*` / `Get*` only return resources in the caller's region; a lookup
   for a resource that exists in another region returns the AWS `NotFound`/`NoSuch*` error, not the
   foreign resource.
4. **ARNs & cross-region references:** emitted ARNs embed the owning region, and operations that name
   a foreign-region ARN behave like AWS (most fail; a documented few, e.g. DynamoDB global tables,
   KMS multi-region keys, Route 53 (global), IAM/STS (global), are deliberately cross-region).
5. **Global services stay global:** IAM, STS, Route 53, CloudFront, WAF (global scope), and
   Organizations must **not** be region-partitioned — their state is shared across regions by design.
6. **Persistence:** the region key survives `Snapshot`/`Restore` (see §5) so regional isolation is
   stable across restarts.
7. **Tests:** a per-service test creates a resource in region A and asserts it is invisible/!found in
   region B (and visible in A). A shared `sdkcheck`-style helper should enforce this so new services
   can't regress.

### Suggested approach

Introduce a shared region-resolution middleware (or extend the existing dispatch path) that always
populates `regionContextKey{}` for **every** service, then migrate backends one cluster at a time to
region-keyed storage — starting with the highest-value stateful services (DynamoDB, S3, SQS, SNS,
Kinesis, Lambda, EC2, Secrets Manager, SSM, CloudWatch). Track per-service region-support status in a
matrix (✅ isolated / ⚠️ region read but not isolated / ❌ single-region) and drive it to all-✅,
excluding the genuinely-global services listed above.

---

## Appendix A — Complete missing-operation inventory (all 1,011 ops)

Every operation below currently returns an unknown-operation error (it is listed in the service's
`notImplemented` slice in `services/<svc>/sdk_completeness_test.go`). This is the authoritative,
exhaustive work list for closing operation-level parity — implement the op in the handler dispatch
table + backend, then move it out of `notImplemented`. Ordered by descending gap size.

### `quicksight` — 187 ops

`BatchCreateTopicReviewedAnswer`, `BatchDeleteTopicReviewedAnswer`, `CreateAccountCustomization`, `CreateAccountSubscription`, `CreateActionConnector`, `CreateBrand`, `CreateCustomPermissions`, `CreateFolder`, `CreateFolderMembership`, `CreateIAMPolicyAssignment`, `CreateOAuthClientApplication`, `CreateRefreshSchedule`, `CreateRoleMembership`, `CreateTemplate`, `CreateTemplateAlias`, `CreateTheme`, `CreateThemeAlias`, `CreateTopic`, `CreateTopicRefreshSchedule`, `CreateVPCConnection`, `DeleteAccountCustomization`, `DeleteAccountCustomPermission`, `DeleteAccountSubscription`, `DeleteActionConnector`, `DeleteBrand`, `DeleteBrandAssignment`, `DeleteCustomPermissions`, `DeleteDataSetRefreshProperties`, `DeleteDefaultQBusinessApplication`, `DeleteFolder`, `DeleteFolderMembership`, `DeleteIAMPolicyAssignment`, `DeleteIdentityPropagationConfig`, `DeleteOAuthClientApplication`, `DeleteRefreshSchedule`, `DeleteRoleCustomPermission`, `DeleteRoleMembership`, `DeleteTemplate`, `DeleteTemplateAlias`, `DeleteTheme`, `DeleteThemeAlias`, `DeleteTopic`, `DeleteTopicRefreshSchedule`, `DeleteUserCustomPermission`, `DeleteVPCConnection`, `DescribeAccountCustomization`, `DescribeAccountCustomPermission`, `DescribeAccountSettings`, `DescribeAccountSubscription`, `DescribeActionConnector`, `DescribeActionConnectorPermissions`, `DescribeAnalysisDefinition`, `DescribeAnalysisPermissions`, `DescribeAssetBundleExportJob`, `DescribeAssetBundleImportJob`, `DescribeAutomationJob`, `DescribeBrand`, `DescribeBrandAssignment`, `DescribeBrandPublishedVersion`, `DescribeCustomPermissions`, `DescribeDashboardDefinition`, `DescribeDashboardPermissions`, `DescribeDashboardSnapshotJob`, `DescribeDashboardSnapshotJobResult`, `DescribeDashboardsQAConfiguration`, `DescribeDataSetPermissions`, `DescribeDataSetRefreshProperties`, `DescribeDataSourcePermissions`, `DescribeDefaultQBusinessApplication`, `DescribeFolder`, `DescribeFolderPermissions`, `DescribeFolderResolvedPermissions`, `DescribeIAMPolicyAssignment`, `DescribeIpRestriction`, `DescribeKeyRegistration`, `DescribeOAuthClientApplication`, `DescribeQPersonalizationConfiguration`, `DescribeQuickSightQSearchConfiguration`, `DescribeRefreshSchedule`, `DescribeRoleCustomPermission`, `DescribeSelfUpgradeConfiguration`, `DescribeTemplate`, `DescribeTemplateAlias`, `DescribeTemplateDefinition`, `DescribeTemplatePermissions`, `DescribeTheme`, `DescribeThemeAlias`, `DescribeThemePermissions`, `DescribeTopic`, `DescribeTopicPermissions`, `DescribeTopicRefresh`, `DescribeTopicRefreshSchedule`, `DescribeVPCConnection`, `GenerateEmbedUrlForAnonymousUser`, `GenerateEmbedUrlForRegisteredUser`, `GenerateEmbedUrlForRegisteredUserWithIdentity`, `GetDashboardEmbedUrl`, `GetFlowMetadata`, `GetFlowPermissions`, `GetIdentityContext`, `GetSessionEmbedUrl`, `ListActionConnectors`, `ListAssetBundleExportJobs`, `ListAssetBundleImportJobs`, `ListBrands`, `ListCustomPermissions`, `ListFlows`, `ListFolderMembers`, `ListFolders`, `ListFoldersForResource`, `ListIAMPolicyAssignments`, `ListIAMPolicyAssignmentsForUser`, `ListIdentityPropagationConfigs`, `ListOAuthClientApplications`, `ListRefreshSchedules`, `ListRoleMemberships`, `ListSelfUpgrades`, `ListTemplateAliases`, `ListTemplates`, `ListTemplateVersions`, `ListThemeAliases`, `ListThemes`, `ListThemeVersions`, `ListTopicRefreshSchedules`, `ListTopicReviewedAnswers`, `ListTopics`, `ListVPCConnections`, `PredictQAResults`, `PutDataSetRefreshProperties`, `SearchActionConnectors`, `SearchAnalyses`, `SearchDashboards`, `SearchDataSets`, `SearchDataSources`, `SearchFlows`, `SearchFolders`, `SearchTopics`, `StartAssetBundleExportJob`, `StartAssetBundleImportJob`, `StartAutomationJob`, `StartDashboardSnapshotJob`, `StartDashboardSnapshotJobSchedule`, `UpdateAccountCustomization`, `UpdateAccountCustomPermission`, `UpdateAccountSettings`, `UpdateActionConnector`, `UpdateActionConnectorPermissions`, `UpdateAnalysisPermissions`, `UpdateApplicationWithTokenExchangeGrant`, `UpdateBrand`, `UpdateBrandAssignment`, `UpdateBrandPublishedVersion`, `UpdateCustomPermissions`, `UpdateDashboardLinks`, `UpdateDashboardPermissions`, `UpdateDashboardPublishedVersion`, `UpdateDashboardsQAConfiguration`, `UpdateDataSetPermissions`, `UpdateDataSourcePermissions`, `UpdateDefaultQBusinessApplication`, `UpdateFlowPermissions`, `UpdateFolder`, `UpdateFolderPermissions`, `UpdateIAMPolicyAssignment`, `UpdateIdentityPropagationConfig`, `UpdateIpRestriction`, `UpdateKeyRegistration`, `UpdateOAuthClientApplication`, `UpdatePublicSharingSettings`, `UpdateQPersonalizationConfiguration`, `UpdateQuickSightQSearchConfiguration`, `UpdateRefreshSchedule`, `UpdateRoleCustomPermission`, `UpdateSelfUpgrade`, `UpdateSelfUpgradeConfiguration`, `UpdateSPICECapacityConfiguration`, `UpdateTemplate`, `UpdateTemplateAlias`, `UpdateTemplatePermissions`, `UpdateTheme`, `UpdateThemeAlias`, `UpdateThemePermissions`, `UpdateTopic`, `UpdateTopicPermissions`, `UpdateTopicRefreshSchedule`, `UpdateUserCustomPermission`, `UpdateVPCConnection`

### `medialive` — 103 ops

`AcceptInputDeviceTransfer`, `BatchDelete`, `BatchStart`, `BatchStop`, `BatchUpdateSchedule`, `CancelInputDeviceTransfer`, `ClaimDevice`, `CreateChannelPlacementGroup`, `CreateCloudWatchAlarmTemplate`, `CreateCloudWatchAlarmTemplateGroup`, `CreateCluster`, `CreateEventBridgeRuleTemplate`, `CreateEventBridgeRuleTemplateGroup`, `CreateMultiplex`, `CreateMultiplexProgram`, `CreateNetwork`, `CreateNode`, `CreateNodeRegistrationScript`, `CreatePartnerInput`, `CreateSdiSource`, `CreateSignalMap`, `DeleteChannelPlacementGroup`, `DeleteCloudWatchAlarmTemplate`, `DeleteCloudWatchAlarmTemplateGroup`, `DeleteCluster`, `DeleteEventBridgeRuleTemplate`, `DeleteEventBridgeRuleTemplateGroup`, `DeleteMultiplex`, `DeleteMultiplexProgram`, `DeleteNetwork`, `DeleteNode`, `DeleteReservation`, `DeleteSchedule`, `DeleteSdiSource`, `DeleteSignalMap`, `DescribeAccountConfiguration`, `DescribeChannelPlacementGroup`, `DescribeCluster`, `DescribeInputDevice`, `DescribeInputDeviceThumbnail`, `DescribeMultiplex`, `DescribeMultiplexProgram`, `DescribeNetwork`, `DescribeNode`, `DescribeOffering`, `DescribeReservation`, `DescribeSchedule`, `DescribeSdiSource`, `DescribeThumbnails`, `GetCloudWatchAlarmTemplate`, `GetCloudWatchAlarmTemplateGroup`, `GetEventBridgeRuleTemplate`, `GetEventBridgeRuleTemplateGroup`, `GetSignalMap`, `ListAlerts`, `ListChannelPlacementGroups`, `ListCloudWatchAlarmTemplateGroups`, `ListCloudWatchAlarmTemplates`, `ListClusterAlerts`, `ListClusters`, `ListEventBridgeRuleTemplateGroups`, `ListEventBridgeRuleTemplates`, `ListInputDevices`, `ListInputDeviceTransfers`, `ListMultiplexAlerts`, `ListMultiplexes`, `ListMultiplexPrograms`, `ListNetworks`, `ListNodes`, `ListOfferings`, `ListReservations`, `ListSdiSources`, `ListSignalMaps`, `ListVersions`, `PurchaseOffering`, `RebootInputDevice`, `RejectInputDeviceTransfer`, `RestartChannelPipelines`, `StartDeleteMonitorDeployment`, `StartInputDevice`, `StartInputDeviceMaintenanceWindow`, `StartMonitorDeployment`, `StartMultiplex`, `StartUpdateSignalMap`, `StopInputDevice`, `StopMultiplex`, `TransferInputDevice`, `UpdateAccountConfiguration`, `UpdateChannelClass`, `UpdateChannelPlacementGroup`, `UpdateCloudWatchAlarmTemplate`, `UpdateCloudWatchAlarmTemplateGroup`, `UpdateCluster`, `UpdateEventBridgeRuleTemplate`, `UpdateEventBridgeRuleTemplateGroup`, `UpdateInputDevice`, `UpdateMultiplex`, `UpdateMultiplexProgram`, `UpdateNetwork`, `UpdateNode`, `UpdateNodeState`, `UpdateReservation`, `UpdateSdiSource`

### `workspaces` — 76 ops

`AcceptAccountLinkInvitation`, `AssociateConnectionAlias`, `AssociateIpGroups`, `AssociateWorkspaceApplication`, `AuthorizeIpRules`, `CopyWorkspaceImage`, `CreateAccountLinkInvitation`, `CreateConnectClientAddIn`, `CreateConnectionAlias`, `CreateIpGroup`, `CreateStandbyWorkspaces`, `CreateUpdatedWorkspaceImage`, `CreateWorkspaceBundle`, `CreateWorkspaceImage`, `CreateWorkspacesPool`, `DeleteAccountLinkInvitation`, `DeleteClientBranding`, `DeleteConnectClientAddIn`, `DeleteConnectionAlias`, `DeleteIpGroup`, `DeleteWorkspaceBundle`, `DeleteWorkspaceImage`, `DeployWorkspaceApplications`, `DeregisterWorkspaceDirectory`, `DescribeAccount`, `DescribeAccountModifications`, `DescribeApplicationAssociations`, `DescribeApplications`, `DescribeBundleAssociations`, `DescribeClientBranding`, `DescribeClientProperties`, `DescribeConnectClientAddIns`, `DescribeConnectionAliasPermissions`, `DescribeConnectionAliases`, `DescribeCustomWorkspaceImageImport`, `DescribeImageAssociations`, `DescribeIpGroups`, `DescribeWorkspaceAssociations`, `DescribeWorkspaceImagePermissions`, `DescribeWorkspaceImages`, `DescribeWorkspaceSnapshots`, `DescribeWorkspacesPools`, `DescribeWorkspacesPoolSessions`, `DisassociateConnectionAlias`, `DisassociateIpGroups`, `DisassociateWorkspaceApplication`, `GetAccountLink`, `ImportClientBranding`, `ImportCustomWorkspaceImage`, `ImportWorkspaceImage`, `ListAccountLinks`, `ListAvailableManagementCidrRanges`, `MigrateWorkspace`, `ModifyAccount`, `ModifyCertificateBasedAuthProperties`, `ModifyClientProperties`, `ModifyEndpointEncryptionMode`, `ModifySamlProperties`, `ModifySelfservicePermissions`, `ModifyStreamingProperties`, `ModifyWorkspaceAccessProperties`, `ModifyWorkspaceCreationProperties`, `RegisterWorkspaceDirectory`, `RejectAccountLinkInvitation`, `RestoreWorkspace`, `RevokeIpRules`, `StartWorkspacesPool`, `StopWorkspacesPool`, `TerminateWorkspacesPool`, `TerminateWorkspacesPoolSession`, `UpdateConnectClientAddIn`, `UpdateConnectionAliasPermission`, `UpdateRulesOfIpGroup`, `UpdateWorkspaceBundle`, `UpdateWorkspaceImagePermission`, `UpdateWorkspacesPool`

### `appstream` — 72 ops

`AssociateAppBlockBuilderAppBlock`, `AssociateApplicationFleet`, `AssociateApplicationToEntitlement`, `AssociateSoftwareToImageBuilder`, `BatchAssociateUserStack`, `BatchDisassociateUserStack`, `CopyImage`, `CreateAppBlock`, `CreateAppBlockBuilder`, `CreateAppBlockBuilderStreamingURL`, `CreateApplication`, `CreateDirectoryConfig`, `CreateEntitlement`, `CreateExportImageTask`, `CreateImageBuilder`, `CreateImageBuilderStreamingURL`, `CreateImportedImage`, `CreateStreamingURL`, `CreateThemeForStack`, `CreateUpdatedImage`, `CreateUsageReportSubscription`, `CreateUser`, `DeleteAppBlock`, `DeleteAppBlockBuilder`, `DeleteApplication`, `DeleteDirectoryConfig`, `DeleteEntitlement`, `DeleteImage`, `DeleteImageBuilder`, `DeleteImagePermissions`, `DeleteThemeForStack`, `DeleteUsageReportSubscription`, `DeleteUser`, `DescribeAppBlockBuilderAppBlockAssociations`, `DescribeAppBlockBuilders`, `DescribeAppBlocks`, `DescribeApplicationFleetAssociations`, `DescribeApplications`, `DescribeAppLicenseUsage`, `DescribeDirectoryConfigs`, `DescribeEntitlements`, `DescribeImageBuilders`, `DescribeImagePermissions`, `DescribeImages`, `DescribeSessions`, `DescribeSoftwareAssociations`, `DescribeThemeForStack`, `DescribeUsageReportSubscriptions`, `DescribeUsers`, `DescribeUserStackAssociations`, `DisableUser`, `DisassociateAppBlockBuilderAppBlock`, `DisassociateApplicationFleet`, `DisassociateApplicationFromEntitlement`, `DisassociateSoftwareFromImageBuilder`, `DrainSessionInstance`, `EnableUser`, `ExpireSession`, `GetExportImageTask`, `ListEntitledApplications`, `ListExportImageTasks`, `StartAppBlockBuilder`, `StartImageBuilder`, `StartSoftwareDeploymentToImageBuilder`, `StopAppBlockBuilder`, `StopImageBuilder`, `UpdateAppBlockBuilder`, `UpdateApplication`, `UpdateDirectoryConfig`, `UpdateEntitlement`, `UpdateImagePermissions`, `UpdateThemeForStack`

### `securityhub` — 66 ops

`AcceptAdministratorInvitation`, `AcceptInvitation`, `BatchUpdateFindingsV2`, `CreateAggregatorV2`, `CreateAutomationRuleV2`, `CreateConfigurationPolicy`, `CreateConnectorV2`, `CreateFindingAggregator`, `CreateMembers`, `CreateTicketV2`, `DeclineInvitations`, `DeleteAggregatorV2`, `DeleteAutomationRuleV2`, `DeleteConfigurationPolicy`, `DeleteConnectorV2`, `DeleteFindingAggregator`, `DeleteInvitations`, `DeleteMembers`, `DescribeOrganizationConfiguration`, `DescribeProductsV2`, `DescribeSecurityHubV2`, `DisableOrganizationAdminAccount`, `DisableSecurityHubV2`, `DisassociateFromAdministratorAccount`, `DisassociateFromMasterAccount`, `DisassociateMembers`, `EnableOrganizationAdminAccount`, `EnableSecurityHubV2`, `GenerateRecommendedPolicyV2`, `GetAdministratorAccount`, `GetAggregatorV2`, `GetAutomationRuleV2`, `GetConfigurationPolicy`, `GetConfigurationPolicyAssociation`, `GetConnectorV2`, `GetFindingAggregator`, `GetFindingStatisticsV2`, `GetFindingsTrendsV2`, `GetFindingsV2`, `GetInvitationsCount`, `GetMasterAccount`, `GetMembers`, `GetRecommendedPolicyV2`, `GetResourcesStatisticsV2`, `GetResourcesTrendsV2`, `GetResourcesV2`, `InviteMembers`, `ListAggregatorsV2`, `ListAutomationRulesV2`, `ListConfigurationPolicies`, `ListConfigurationPolicyAssociations`, `ListConnectorsV2`, `ListFindingAggregators`, `ListInvitations`, `ListMembers`, `ListOrganizationAdminAccounts`, `RegisterConnectorV2`, `StartConfigurationPolicyAssociation`, `StartConfigurationPolicyDisassociation`, `UpdateAggregatorV2`, `UpdateAutomationRuleV2`, `UpdateConfigurationPolicy`, `UpdateConnectorV2`, `UpdateFindingAggregator`, `UpdateOrganizationConfiguration`, `BatchGetConfigurationPolicyAssociations`

### `directoryservice` — 64 ops

`AcceptSharedDirectory`, `AddIpRoutes`, `AddRegion`, `CancelSchemaExtension`, `ConnectDirectory`, `CreateComputer`, `CreateConditionalForwarder`, `CreateHybridAD`, `CreateLogSubscription`, `CreateTrust`, `DeleteADAssessment`, `DeleteConditionalForwarder`, `DeleteLogSubscription`, `DeleteTrust`, `DeregisterCertificate`, `DeregisterEventTopic`, `DescribeADAssessment`, `DescribeCAEnrollmentPolicy`, `DescribeCertificate`, `DescribeClientAuthenticationSettings`, `DescribeConditionalForwarders`, `DescribeDirectoryDataAccess`, `DescribeDomainControllers`, `DescribeEventTopics`, `DescribeHybridADUpdate`, `DescribeLDAPSSettings`, `DescribeRegions`, `DescribeSettings`, `DescribeSharedDirectories`, `DescribeTrusts`, `DescribeUpdateDirectory`, `DisableCAEnrollmentPolicy`, `DisableClientAuthentication`, `DisableDirectoryDataAccess`, `DisableLDAPS`, `DisableRadius`, `EnableCAEnrollmentPolicy`, `EnableClientAuthentication`, `EnableDirectoryDataAccess`, `EnableLDAPS`, `EnableRadius`, `ListADAssessments`, `ListCertificates`, `ListIpRoutes`, `ListLogSubscriptions`, `ListSchemaExtensions`, `RegisterCertificate`, `RegisterEventTopic`, `RejectSharedDirectory`, `RemoveIpRoutes`, `RemoveRegion`, `ResetUserPassword`, `ShareDirectory`, `StartADAssessment`, `StartSchemaExtension`, `UnshareDirectory`, `UpdateConditionalForwarder`, `UpdateDirectorySetup`, `UpdateHybridAD`, `UpdateNumberOfDomainControllers`, `UpdateRadius`, `UpdateSettings`, `UpdateTrust`, `VerifyTrust`

### `inspector2` — 62 ops

`AssociateMember`, `BatchAssociateCodeSecurityScanConfiguration`, `BatchDisassociateCodeSecurityScanConfiguration`, `BatchGetCodeSnippet`, `BatchGetFindingDetails`, `BatchGetFreeTrialInfo`, `BatchGetMemberEc2DeepInspectionStatus`, `BatchUpdateMemberEc2DeepInspectionStatus`, `CancelFindingsReport`, `CancelSbomExport`, `CreateCisScanConfiguration`, `CreateCodeSecurityIntegration`, `CreateCodeSecurityScanConfiguration`, `CreateFindingsReport`, `CreateSbomExport`, `DeleteCisScanConfiguration`, `DeleteCodeSecurityIntegration`, `DeleteCodeSecurityScanConfiguration`, `DescribeOrganizationConfiguration`, `DisableDelegatedAdminAccount`, `DisassociateMember`, `EnableDelegatedAdminAccount`, `GetCisScanReport`, `GetCisScanResultDetails`, `GetClustersForImage`, `GetCodeSecurityIntegration`, `GetCodeSecurityScan`, `GetCodeSecurityScanConfiguration`, `GetDelegatedAdminAccount`, `GetEc2DeepInspectionConfiguration`, `GetEncryptionKey`, `GetFindingsReportStatus`, `GetMember`, `GetSbomExport`, `ListAccountPermissions`, `ListCisScanConfigurations`, `ListCisScanResultsAggregatedByChecks`, `ListCisScanResultsAggregatedByTargetResource`, `ListCisScans`, `ListCodeSecurityIntegrations`, `ListCodeSecurityScanConfigurationAssociations`, `ListCodeSecurityScanConfigurations`, `ListCoverage`, `ListCoverageStatistics`, `ListDelegatedAdminAccounts`, `ListFindingAggregations`, `ListMembers`, `ListUsageTotals`, `ResetEncryptionKey`, `SearchVulnerabilities`, `SendCisSessionHealth`, `SendCisSessionTelemetry`, `StartCisSession`, `StartCodeSecurityScan`, `StopCisSession`, `UpdateCisScanConfiguration`, `UpdateCodeSecurityIntegration`, `UpdateCodeSecurityScanConfiguration`, `UpdateEc2DeepInspectionConfiguration`, `UpdateEncryptionKey`, `UpdateOrganizationConfiguration`, `UpdateOrgEc2DeepInspectionConfiguration`

### `guardduty` — 57 ops

`AcceptAdministratorInvitation`, `AcceptInvitation`, `CreateMalwareProtectionPlan`, `CreateMembers`, `CreatePublishingDestination`, `CreateThreatEntitySet`, `CreateTrustedEntitySet`, `DeclineInvitations`, `DeleteInvitations`, `DeleteMalwareProtectionPlan`, `DeleteMembers`, `DeletePublishingDestination`, `DeleteThreatEntitySet`, `DeleteTrustedEntitySet`, `DescribeMalwareScans`, `DescribeOrganizationConfiguration`, `DescribePublishingDestination`, `DisableOrganizationAdminAccount`, `DisassociateFromAdministratorAccount`, `DisassociateFromMasterAccount`, `DisassociateMembers`, `EnableOrganizationAdminAccount`, `GetAdministratorAccount`, `GetCoverageStatistics`, `GetInvitationsCount`, `GetMalwareProtectionPlan`, `GetMalwareScan`, `GetMalwareScanSettings`, `GetMasterAccount`, `GetMemberDetectors`, `GetMembers`, `GetOrganizationStatistics`, `GetRemainingFreeTrialDays`, `GetThreatEntitySet`, `GetTrustedEntitySet`, `InviteMembers`, `ListCoverage`, `ListInvitations`, `ListMalwareProtectionPlans`, `ListMalwareScans`, `ListMembers`, `ListOrganizationAdminAccounts`, `ListPublishingDestinations`, `ListThreatEntitySets`, `ListTrustedEntitySets`, `SendObjectMalwareScan`, `StartMalwareScan`, `StartMonitoringMembers`, `StopMonitoringMembers`, `GetUsageStatistics`, `UpdateMalwareProtectionPlan`, `UpdateMalwareScanSettings`, `UpdateMemberDetectors`, `UpdateOrganizationConfiguration`, `UpdatePublishingDestination`, `UpdateThreatEntitySet`, `UpdateTrustedEntitySet`

### `rekognition` — 56 ops

`AssociateFaces`, `CompareFaces`, `CopyProjectVersion`, `CreateDataset`, `CreateFaceLivenessSession`, `CreateProject`, `CreateProjectVersion`, `CreateUser`, `DeleteDataset`, `DeleteProject`, `DeleteProjectPolicy`, `DeleteProjectVersion`, `DeleteUser`, `DescribeDataset`, `DescribeProjectVersions`, `DescribeProjects`, `DetectCustomLabels`, `DetectFaces`, `DetectLabels`, `DetectModerationLabels`, `DetectProtectiveEquipment`, `DetectText`, `DisassociateFaces`, `DistributeDatasetEntries`, `GetCelebrityInfo`, `GetCelebrityRecognition`, `GetContentModeration`, `GetFaceDetection`, `GetFaceLivenessSessionResults`, `GetFaceSearch`, `GetLabelDetection`, `GetMediaAnalysisJob`, `GetPersonTracking`, `GetSegmentDetection`, `GetTextDetection`, `ListDatasetEntries`, `ListDatasetLabels`, `ListMediaAnalysisJobs`, `ListProjectPolicies`, `ListUsers`, `PutProjectPolicy`, `RecognizeCelebrities`, `SearchUsers`, `SearchUsersByImage`, `StartCelebrityRecognition`, `StartContentModeration`, `StartFaceDetection`, `StartFaceSearch`, `StartLabelDetection`, `StartMediaAnalysisJob`, `StartPersonTracking`, `StartProjectVersion`, `StartSegmentDetection`, `StartTextDetection`, `StopProjectVersion`, `UpdateDatasetEntries`

### `macie2` — 55 ops

`AcceptInvitation`, `BatchGetCustomDataIdentifiers`, `BatchUpdateAutomatedDiscoveryAccounts`, `CreateClassificationJob`, `CreateInvitations`, `CreateMember`, `DeclineInvitations`, `DeleteInvitations`, `DeleteMember`, `DescribeBuckets`, `DescribeClassificationJob`, `DescribeOrganizationConfiguration`, `DisableOrganizationAdminAccount`, `DisassociateFromAdministratorAccount`, `DisassociateFromMasterAccount`, `DisassociateMember`, `EnableOrganizationAdminAccount`, `GetAdministratorAccount`, `GetAutomatedDiscoveryConfiguration`, `GetBucketStatistics`, `GetClassificationExportConfiguration`, `GetClassificationScope`, `GetInvitationsCount`, `GetMasterAccount`, `GetMember`, `GetResourceProfile`, `GetRevealConfiguration`, `GetSensitiveDataOccurrences`, `GetSensitiveDataOccurrencesAvailability`, `GetSensitivityInspectionTemplate`, `GetUsageStatistics`, `GetUsageTotals`, `ListAutomatedDiscoveryAccounts`, `ListClassificationJobs`, `ListClassificationScopes`, `ListInvitations`, `ListManagedDataIdentifiers`, `ListMembers`, `ListOrganizationAdminAccounts`, `ListResourceProfileArtifacts`, `ListResourceProfileDetections`, `ListSensitivityInspectionTemplates`, `GetFindingsPublicationConfiguration`, `PutClassificationExportConfiguration`, `PutFindingsPublicationConfiguration`, `SearchResources`, `UpdateAutomatedDiscoveryConfiguration`, `UpdateClassificationJob`, `UpdateClassificationScope`, `UpdateMemberSession`, `UpdateOrganizationConfiguration`, `UpdateResourceProfile`, `UpdateResourceProfileDetections`, `UpdateRevealConfiguration`, `UpdateSensitivityInspectionTemplate`

### `fsx` — 37 ops

`AssociateFileSystemAliases`, `CancelDataRepositoryTask`, `CopyBackup`, `CopySnapshotAndUpdateVolume`, `CreateAndAttachS3AccessPoint`, `CreateDataRepositoryAssociation`, `CreateDataRepositoryTask`, `CreateFileCache`, `CreateSnapshot`, `CreateStorageVirtualMachine`, `CreateVolume`, `CreateVolumeFromBackup`, `DeleteDataRepositoryAssociation`, `DeleteFileCache`, `DeleteSnapshot`, `DeleteStorageVirtualMachine`, `DeleteVolume`, `DescribeDataRepositoryAssociations`, `DescribeDataRepositoryTasks`, `DescribeFileCaches`, `DescribeFileSystemAliases`, `DescribeS3AccessPointAttachments`, `DescribeSharedVpcConfiguration`, `DescribeSnapshots`, `DescribeStorageVirtualMachines`, `DescribeVolumes`, `DetachAndDeleteS3AccessPoint`, `DisassociateFileSystemAliases`, `ReleaseFileSystemNfsV3Locks`, `RestoreVolumeFromSnapshot`, `StartMisconfiguredStateRecovery`, `UpdateDataRepositoryAssociation`, `UpdateFileCache`, `UpdateSharedVpcConfiguration`, `UpdateSnapshot`, `UpdateStorageVirtualMachine`, `UpdateVolume`

### `workmail` — 36 ops

`AssumeImpersonationRole`, `CancelMailboxExportJob`, `CreateAvailabilityConfiguration`, `CreateIdentityCenterApplication`, `CreateMobileDeviceAccessRule`, `DeleteAvailabilityConfiguration`, `DeleteEmailMonitoringConfiguration`, `DeleteIdentityCenterApplication`, `DeleteIdentityProviderConfiguration`, `DeleteMobileDeviceAccessOverride`, `DeleteMobileDeviceAccessRule`, `DeletePersonalAccessToken`, `DeleteRetentionPolicy`, `DescribeEmailMonitoringConfiguration`, `DescribeIdentityProviderConfiguration`, `DescribeInboundDmarcSettings`, `DescribeMailboxExportJob`, `GetDefaultRetentionPolicy`, `GetImpersonationRoleEffect`, `GetMobileDeviceAccessEffect`, `GetMobileDeviceAccessOverride`, `GetPersonalAccessTokenMetadata`, `ListAvailabilityConfigurations`, `ListMailboxExportJobs`, `ListMobileDeviceAccessOverrides`, `ListMobileDeviceAccessRules`, `ListPersonalAccessTokens`, `PutEmailMonitoringConfiguration`, `PutIdentityProviderConfiguration`, `PutInboundDmarcSettings`, `PutMobileDeviceAccessOverride`, `PutRetentionPolicy`, `StartMailboxExportJob`, `TestAvailabilityConfiguration`, `UpdateAvailabilityConfiguration`, `UpdateMobileDeviceAccessRule`

### `datasync` — 32 ops

`CreateLocationAzureBlob`, `CreateLocationEfs`, `CreateLocationFsxLustre`, `CreateLocationFsxOntap`, `CreateLocationFsxOpenZfs`, `CreateLocationFsxWindows`, `CreateLocationHdfs`, `CreateLocationNfs`, `CreateLocationObjectStorage`, `CreateLocationSmb`, `DescribeLocationAzureBlob`, `DescribeLocationEfs`, `DescribeLocationFsxLustre`, `DescribeLocationFsxOntap`, `DescribeLocationFsxOpenZfs`, `DescribeLocationFsxWindows`, `DescribeLocationHdfs`, `DescribeLocationNfs`, `DescribeLocationObjectStorage`, `DescribeLocationSmb`, `UpdateLocationAzureBlob`, `UpdateLocationEfs`, `UpdateLocationFsxLustre`, `UpdateLocationFsxOntap`, `UpdateLocationFsxOpenZfs`, `UpdateLocationFsxWindows`, `UpdateLocationHdfs`, `UpdateLocationNfs`, `UpdateLocationObjectStorage`, `UpdateLocationS3`, `UpdateLocationSmb`, `UpdateTaskExecution`

### `apprunner` — 25 ops

`AssociateCustomDomain`, `CreateAutoScalingConfiguration`, `CreateConnection`, `CreateObservabilityConfiguration`, `CreateVpcConnector`, `CreateVpcIngressConnection`, `DeleteAutoScalingConfiguration`, `DeleteConnection`, `DeleteObservabilityConfiguration`, `DeleteVpcConnector`, `DeleteVpcIngressConnection`, `DescribeAutoScalingConfiguration`, `DescribeCustomDomains`, `DescribeObservabilityConfiguration`, `DescribeVpcConnector`, `DescribeVpcIngressConnection`, `DisassociateCustomDomain`, `ListAutoScalingConfigurations`, `ListConnections`, `ListObservabilityConfigurations`, `ListServicesForAutoScalingConfiguration`, `ListVpcConnectors`, `ListVpcIngressConnections`, `UpdateDefaultAutoScalingConfiguration`, `UpdateVpcIngressConnection`

### `mediatailor` — 24 ops

`ConfigureLogsForChannel`, `ConfigureLogsForPlaybackConfiguration`, `CreateLiveSource`, `CreatePrefetchSchedule`, `CreateProgram`, `DeleteChannelPolicy`, `DeleteFunction`, `DeleteLiveSource`, `DeletePrefetchSchedule`, `DeleteProgram`, `DescribeLiveSource`, `DescribeProgram`, `GetChannelPolicy`, `GetChannelSchedule`, `GetFunction`, `GetPrefetchSchedule`, `ListAlerts`, `ListFunctions`, `ListLiveSources`, `ListPrefetchSchedules`, `PutChannelPolicy`, `PutFunction`, `UpdateLiveSource`, `UpdateProgram`

### `accessanalyzer` — 23 ops

`ApplyArchiveRule`, `CancelPolicyGeneration`, `CheckAccessNotGranted`, `CheckNoNewAccess`, `CheckNoPublicAccess`, `CreateAccessPreview`, `CreateServiceLinkedAnalyzer`, `DeleteServiceLinkedAnalyzer`, `GenerateFindingRecommendation`, `GetAccessPreview`, `GetAnalyzedResource`, `GetFindingRecommendation`, `GetFindingsStatistics`, `GetFindingV2`, `GetGeneratedPolicy`, `ListAccessPreviewFindings`, `ListAccessPreviews`, `ListAnalyzedResources`, `ListFindingsV2`, `ListPolicyGenerations`, `StartPolicyGeneration`, `UpdateAnalyzer`, `ValidatePolicy`

### `detective` — 19 ops

`AcceptInvitation`, `BatchGetGraphMemberDatasources`, `BatchGetMembershipDatasources`, `DescribeOrganizationConfiguration`, `DisableOrganizationAdminAccount`, `DisassociateMembership`, `EnableOrganizationAdminAccount`, `GetInvestigation`, `ListDatasourcePackages`, `ListIndicators`, `ListInvestigations`, `ListInvitations`, `ListOrganizationAdminAccounts`, `RejectInvitation`, `StartInvestigation`, `StartMonitoringMember`, `UpdateDatasourcePackages`, `UpdateInvestigationState`, `UpdateOrganizationConfiguration`

### `rolesanywhere` — 13 ops

`DeleteCrl`, `DisableCrl`, `EnableCrl`, `GetCrl`, `ImportCrl`, `ListCrls`, `UpdateCrl`, `GetSubject`, `ListSubjects`, `PutAttributeMapping`, `DeleteAttributeMapping`, `PutNotificationSettings`, `ResetNotificationSettings`

### `mediapackage` — 4 ops

`CreateHarvestJob`, `DescribeHarvestJob`, `ListHarvestJobs`, `RotateIngestEndpointCredentials`

