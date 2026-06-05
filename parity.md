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
acknowledged as **not implemented**. The handler answers a subset of the service's API; these
ops return an unknown-operation error. Largest gaps (exact slice lengths):

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
| **rolesanywhere** | 13 | (see `services/rolesanywhere/sdk_completeness_test.go`) |

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
| 27 | 🔴 | **Health endpoint path** | Gopherstack serves `/_gopherstack/health`, not LocalStack's **`/_localstack/health`**. Tools that probe LocalStack readiness — `awslocal`, `cdklocal`, the Testcontainers/`localstack` modules — won't recognise it. No compat alias exists. | `cli.go:1992` |
| 28 | 🟠 | **Default port** | Default `8000`, not LocalStack's edge port **`4566`**. Anything hard-coded to `4566` needs reconfiguration. | `cmd/awsgs/main.go:30` |
| 29 | 🟠 | **No CORS** | No CORS middleware is registered, so **browser-based** AWS SDK (`@aws-sdk/*` in a webpage) calls fail preflight. LocalStack returns permissive CORS headers. | `cli.go:1985` (middleware chain) |
| 30 | 🟠 | **Region isolation is inconsistent** | Only ~8 handlers call `ExtractRegionFromRequest`; most backends are single-region, so resources created in `us-east-1` are visible from other regions. S3 is region-aware (`bucketIndex name→region`), but this is not applied uniformly. | `pkgs/httputils/httputils.go:308`; `services/s3/backend_memory.go:109` |
| 31 | 🟠 | **Lambda packaging** | **Image-based functions only** (`PackageType: Image`). Zip uploads, S3 code delivery, and inline code — all supported by LocalStack — are rejected. | README "Lambda (image-based only)" |
| 32 | 🟡 | **Thin env-config surface** | Only `AWS_ACCESS_KEY_ID/SECRET/REGION/DEFAULT_REGION` are read from the environment; LocalStack's rich `SERVICES`, `PERSISTENCE`, `DEBUG`, `GATEWAY_LISTEN`, `LAMBDA_*` knobs have no equivalent (gopherstack uses CLI flags instead). | grep `os.Getenv` in `cmd/`, `cli.go`, `pkgs/config` |
| 33 | 🟡 | **No SigV4 verification** | No request-signature validation anywhere in the pipeline. This **matches** LocalStack's default (expected), but means issued STS credentials aren't cryptographically enforced on later calls. | (no `sigv4` verify outside vendored SDK) |

**GOOD:** Startup **init hooks** are supported (`pkgs/inithooks`), analogous to LocalStack's
`init/ready.d` lifecycle. The root `/` GET and `/_gopherstack/health` both report running state.

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
5. **EC2 persistence allowlist** (#24) — close the largest restart-data-loss surface.
6. **LocalStack `/_localstack/health` alias + `:4566` option** (#27, #28) — cheap, unlocks drop-in tooling.
7. **Secrets Manager rotation Lambda invocation** (#14) and **CloudWatch alarm auto-evaluation** (#15).
