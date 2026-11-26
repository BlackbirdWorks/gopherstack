<p align="center">
  <img src="assets/logo.png" width="640" alt="Gopherstack">
</p>

<p align="center">
  <strong>Local AWS, Azure, GCP and OCI APIs in one Go binary.</strong>
</p>

<p align="center">
  <a href="https://github.com/blackbirdworks/gopherstack/actions/workflows/ci.yml"><img src="https://github.com/blackbirdworks/gopherstack/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://github.com/blackbirdworks/gopherstack/actions/workflows/release.yml"><img src="https://github.com/blackbirdworks/gopherstack/actions/workflows/release.yml/badge.svg" alt="Release"></a>
  <a href="https://codecov.io/gh/BlackbirdWorks/gopherstack"><img src="https://codecov.io/gh/BlackbirdWorks/gopherstack/branch/main/graph/badge.svg" alt="codecov"></a>
  <a href="https://www.codefactor.io/repository/github/blackbirdworks/gopherstack"><img src="https://www.codefactor.io/repository/github/blackbirdworks/gopherstack/badge" alt="CodeFactor"></a>
  <a href="https://scorecard.dev/viewer/?uri=github.com/BlackbirdWorks/gopherstack"><img src="https://api.scorecard.dev/projects/github.com/BlackbirdWorks/gopherstack/badge" alt="OpenSSF Scorecard"></a>
  <a href="https://pkg.go.dev/github.com/blackbirdworks/gopherstack"><img src="https://pkg.go.dev/badge/github.com/blackbirdworks/gopherstack.svg" alt="Go Reference"></a>
  <a href="#services"><img src=".badges/operations.svg" alt="PARITY entries"></a>
  <a href="#services"><img src=".badges/services.svg" alt="AWS services"></a>
  <a href="#services"><img src=".badges/parity.svg" alt="parity"></a>
  <a href="go.mod"><img src=".badges/go.svg" alt="go"></a>
  <a href="LICENSE"><img src=".badges/license.svg" alt="license"></a>
</p>

Gopherstack is an in-memory emulator for AWS, Azure, GCP and Oracle Cloud (OCI) APIs. Point an SDK, the AWS CLI, Terraform or CDK at `http://localhost:8000`. No cloud account is needed.

## Quick start

```bash
docker run --rm -p 8000:8000 ghcr.io/blackbirdworks/gopherstack:latest
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
aws --endpoint-url http://localhost:8000 s3 mb s3://my-bucket
aws --endpoint-url http://localhost:8000 dynamodb list-tables
```

The dashboard is at <http://localhost:8000/dashboard>. Health check: `/_gopherstack/health`.

> [!IMPORTANT]
> This project is vibe coded.

## Install

**Docker**

```bash
docker run -p 8000:8000 ghcr.io/blackbirdworks/gopherstack:latest
```

**Release archive**: download `gopherstack_<version>_<OS>_<arch>` from [Releases](https://github.com/blackbirdworks/gopherstack/releases). It is a `.tar.gz`, or a `.zip` on Windows. Builds exist for linux, darwin and windows on amd64 and arm64, except windows/arm64.

**Go** (Go 1.27+)

```bash
go install github.com/blackbirdworks/gopherstack@latest
```

**Helm**

```bash
helm install gopherstack ./charts/gopherstack
```

**Source**

```bash
git clone https://github.com/blackbirdworks/gopherstack && cd gopherstack && go run .
```

**Desktop app**: `gopherstack-desktop_<version>_<OS>_<arch>` from Releases. Build it with `task build-desktop` (needs CGO and webview libraries).

The binary also manages a local instance (`start`, `stop`, `status`, `logs`, `reset`, `snapshot`, `pod`, `replicate`, `mcp`). See [CLI commands](docs/cli.md), or run `gopherstack --help`.

> [!NOTE]
> Lambda images, ECS tasks and the `docker` engines need a Docker or Podman socket. Everything else runs without one. See [Docker](docs/docker.md).

Terraform, OpenTofu and CDK: override the service endpoints, or set `AWS_ENDPOINT_URL=http://localhost:8000` for tools that honor it. See [Migrating from LocalStack](docs/migration.md).

## Documentation

Published at <https://gopherstack.blackbirdworks.dev>.

| Page | Topic |
|------|-------|
| [Quickstart](docs/quickstart.md) | Install, flags, persistence, pods |
| [Docker](docs/docker.md) | Compose, GitHub Actions, Docker-backed features |
| [Kubernetes](docs/kubernetes.md) | Helm chart |
| [Testcontainers](docs/testcontainers.md) | Go, Python and Node modules |
| [Configuration](docs/configuration.md) | Every flag and environment variable |
| [CLI commands](docs/cli.md) | start, stop, status, logs, reset, snapshot, pod, replicate, mcp |
| [Engines](docs/engines.md) | Container-backed engine modes |
| [Persistence](docs/persistence.md) | State across restarts |
| [Chaos](docs/chaos.md) | Fault injection |
| [Security groups](docs/security-groups.md) | Enforcement |
| [MCP server](docs/mcp.md) | Control Gopherstack from AI assistants |
| [Migrating from LocalStack](docs/migration.md) | Differences and flag mapping |
| [Architecture](docs/architecture/README.md) | Internals |
| [Examples](examples/README.md) | Runnable demos |
| [Azure](AZURE.md) | Azure services and ports |

<!-- BEGIN GENERATED SERVICES -->
## Services

Each service links to a page with its audited operations and known gaps, generated from its `PARITY.md`. Search and filter them all on the [docs site](https://gopherstack.blackbirdworks.dev/services/).

**Parity** is the grade from the latest audit. **PARITY Entries** counts the `ops:` entries in `PARITY.md` (not raw operations); a dash means the service is tracked by feature family.

**266 services:** 169 A, 92 B, 3 C, 2 Removed.

<details>
<summary><strong>Compute</strong> (6 services)</summary>

| Service | Parity | PARITY Entries | Notes |
|---|---|---|---|
| [App Runner](services/apprunner/README.md) | A | 37 | clean |
| [Auto Scaling](services/autoscaling/README.md) | A | 66 | 2 structural gaps |
| [Batch](services/batch/README.md) | A | 48 | 3 structural gaps |
| [EC2](services/ec2/README.md) | A | — | 22 families; 4 gaps; 18 structural gaps; 5 deferred |
| [Elastic Beanstalk](services/elasticbeanstalk/README.md) | A | 47 | 4 structural gaps |
| [Lambda](services/lambda/README.md) | A | — | 11 families; 3 gaps; 11 structural gaps |

</details>

<details>
<summary><strong>Containers</strong> (3 services)</summary>

| Service | Parity | PARITY Entries | Notes |
|---|---|---|---|
| [ECR](services/ecr/README.md) | A | 58 | 2 structural gaps; 2 deferred |
| [ECS](services/ecs/README.md) | A | 65 | 4 gaps; 5 structural gaps |
| [EKS](services/eks/README.md) | A | 71 | 5 structural gaps; 1 deferred |

</details>

<details>
<summary><strong>Storage</strong> (9 services)</summary>

| Service | Parity | PARITY Entries | Notes |
|---|---|---|---|
| [Backup](services/backup/README.md) | A | 66 | 1 gap; 3 structural gaps |
| [Data Lifecycle Manager](services/dlm/README.md) | A | 8 | clean |
| [EFS](services/efs/README.md) | A | 31 | 1 deferred |
| [FSx](services/fsx/README.md) | A | — | 13 families; 2 structural gaps |
| [S3](services/s3/README.md) | A | 26 | 4 structural gaps |
| [S3 Control](services/s3control/README.md) | A | 44 | 1 gap; 6 structural gaps; 2 deferred |
| [S3 Files](services/s3files/README.md) | B | 21 | 1 structural gap |
| [S3 Glacier](services/glacier/README.md) | A | 33 | clean |
| [S3 Tables](services/s3tables/README.md) | A | 49 | clean |

</details>

<details>
<summary><strong>Database</strong> (15 services)</summary>

| Service | Parity | PARITY Entries | Notes |
|---|---|---|---|
| [DAX](services/dax/README.md) | A | 21 | 1 structural gap; 1 deferred |
| [DocumentDB](services/docdb/README.md) | A | 55 | 5 structural gaps; 1 deferred |
| [DynamoDB](services/dynamodb/README.md) | A | — | 15 families; 3 structural gaps; 2 deferred |
| [DynamoDB Streams](services/dynamodbstreams/README.md) | A | 4 | clean |
| [ElastiCache](services/elasticache/README.md) | A | 75 | 4 structural gaps; 1 deferred |
| [MemoryDB](services/memorydb/README.md) | A | 45 | 3 gaps; 2 structural gaps; 3 deferred |
| [Neptune](services/neptune/README.md) | A | — | 13 families; 6 gaps; 2 structural gaps; 2 deferred |
| [QLDB](services/qldb/README.md) | Removed | — | removed service |
| [QLDB Session](services/qldbsession/README.md) | Removed | — | removed service |
| [RDS](services/rds/README.md) | A | 52 | 5 gaps; 7 structural gaps |
| [RDS Data](services/rdsdata/README.md) | A | 7 | 4 structural gaps |
| [Redshift](services/redshift/README.md) | A | 9 | 5 structural gaps |
| [Redshift Data](services/redshiftdata/README.md) | A | 12 | 4 structural gaps; 1 deferred |
| [Timestream Query](services/timestreamquery/README.md) | A | 12 | 8 gaps; 1 structural gap |
| [Timestream Write](services/timestreamwrite/README.md) | A | 19 | 1 structural gap |

</details>

<details>
<summary><strong>Networking & Content Delivery</strong> (16 services)</summary>

| Service | Parity | PARITY Entries | Notes |
|---|---|---|---|
| [API Gateway](services/apigateway/README.md) | A | 123 | 1 structural gap; 2 deferred |
| [API Gateway Management API](services/apigatewaymanagementapi/README.md) | A | 3 | 2 deferred |
| [API Gateway v2](services/apigatewayv2/README.md) | A | 77 | 1 structural gap |
| [ARC Zonal Shift](services/arczonalshift/README.md) | A | 15 | 1 structural gap |
| [App Mesh](services/appmesh/README.md) | A | 38 | 1 structural gap |
| [Cloud Map](services/servicediscovery/README.md) | A | 30 | 4 structural gaps; 1 deferred |
| [CloudFront](services/cloudfront/README.md) | A | 60 | 4 structural gaps; 4 deferred |
| [CloudWatch Network Monitor](services/networkmonitor/README.md) | A | 12 | 1 deferred |
| [ELB (Classic)](services/elb/README.md) | A | 29 | 1 deferred |
| [ELBv2](services/elbv2/README.md) | A | 51 | 2 structural gaps; 2 deferred |
| [Route 53](services/route53/README.md) | A | 67 | 3 deferred |
| [Route 53 Recovery Cluster](services/route53recoverycluster/README.md) | A | 4 | clean |
| [Route 53 Recovery Control Config](services/route53recoverycontrolconfig/README.md) | A | 25 | 1 structural gap |
| [Route 53 Recovery Readiness](services/route53recoveryreadiness/README.md) | A | 32 | 1 structural gap |
| [Route 53 Resolver](services/route53resolver/README.md) | A | 72 | 2 gaps; 1 structural gap; 1 deferred |
| [VPC Lattice](services/vpclattice/README.md) | A | 73 | 3 structural gaps |

</details>

<details>
<summary><strong>Messaging & Integration</strong> (13 services)</summary>

| Service | Parity | PARITY Entries | Notes |
|---|---|---|---|
| [Amazon MQ](services/mq/README.md) | A | 25 | 2 structural gaps; 1 deferred |
| [AppSync](services/appsync/README.md) | A | 74 | 1 gap; 1 structural gap; 1 deferred |
| [EventBridge](services/eventbridge/README.md) | A | 66 | 3 structural gaps; 2 deferred |
| [EventBridge Pipes](services/pipes/README.md) | A | 10 | 2 structural gaps |
| [EventBridge Scheduler](services/scheduler/README.md) | A | 12 | clean |
| [Pinpoint](services/pinpoint/README.md) | A | 51 | 2 structural gaps; 3 deferred |
| [SES](services/ses/README.md) | A | 71 | 3 structural gaps; 1 deferred |
| [SES v2](services/sesv2/README.md) | A | 116 | 2 structural gaps |
| [SNS](services/sns/README.md) | A | 34 | 1 structural gap; 1 deferred |
| [SQS](services/sqs/README.md) | A | 20 | 1 structural gap |
| [SWF](services/swf/README.md) | A | 39 | clean |
| [Step Functions](services/stepfunctions/README.md) | A | 37 | 2 gaps; 1 structural gap |
| [WorkMail](services/workmail/README.md) | A | 92 | 3 structural gaps |

</details>

<details>
<summary><strong>Analytics</strong> (16 services)</summary>

| Service | Parity | PARITY Entries | Notes |
|---|---|---|---|
| [Athena](services/athena/README.md) | A | 27 | 3 gaps; 6 structural gaps; 1 deferred |
| [Clean Rooms](services/cleanrooms/README.md) | A | — | 18 families; 5 structural gaps |
| [EMR](services/emr/README.md) | A | 65 | 7 structural gaps |
| [EMR Serverless](services/emrserverless/README.md) | A | 22 | 1 structural gap |
| [Elasticsearch](services/elasticsearch/README.md) | A | 51 | 1 structural gap |
| [Glue](services/glue/README.md) | A | 59 | 3 gaps; 5 structural gaps; 2 deferred |
| [Glue DataBrew](services/databrew/README.md) | A | 44 | 1 structural gap |
| [Kinesis](services/kinesis/README.md) | A | 39 | 5 structural gaps |
| [Kinesis Analytics](services/kinesisanalytics/README.md) | A | 20 | clean |
| [Kinesis Analytics v2](services/kinesisanalyticsv2/README.md) | A | 33 | 1 structural gap; 1 deferred |
| [Kinesis Data Firehose](services/firehose/README.md) | A | 12 | 4 structural gaps |
| [Lake Formation](services/lakeformation/README.md) | A | 61 | 2 structural gaps |
| [Managed Streaming for Kafka](services/kafka/README.md) | A | 64 | 2 gaps; 1 structural gap |
| [Managed Workflows for Apache Airflow](services/mwaa/README.md) | A | 12 | 1 structural gap; 1 deferred |
| [OpenSearch](services/opensearch/README.md) | A | 19 | 3 gaps; 1 structural gap |
| [QuickSight](services/quicksight/README.md) | A | 81 | 7 structural gaps |

</details>

<details>
<summary><strong>Security</strong> (13 services)</summary>

| Service | Parity | PARITY Entries | Notes |
|---|---|---|---|
| [ACM](services/acm/README.md) | A | 39 | 4 structural gaps; 1 deferred |
| [ACM PCA](services/acmpca/README.md) | A | 23 | 1 structural gap |
| [Detective](services/detective/README.md) | A | 29 | 4 structural gaps; 2 deferred |
| [GuardDuty](services/guardduty/README.md) | A | 66 | 3 structural gaps |
| [Inspector](services/inspector2/README.md) | A | 13 | 4 structural gaps; 1 deferred |
| [KMS](services/kms/README.md) | A | 54 | 1 gap; 1 structural gap; 1 deferred |
| [Macie](services/macie2/README.md) | A | 81 | clean |
| [Secrets Manager](services/secretsmanager/README.md) | A | 24 | 1 structural gap; 2 deferred |
| [Security Hub](services/securityhub/README.md) | A | 120 | 1 gap; 4 structural gaps |
| [Shield](services/shield/README.md) | A | 36 | 1 structural gap; 1 deferred |
| [Verified Permissions](services/verifiedpermissions/README.md) | A | 34 | clean |
| [WAF](services/waf/README.md) | A | 4 | 3 structural gaps |
| [WAFv2](services/wafv2/README.md) | A | 59 | 2 structural gaps |

</details>

<details>
<summary><strong>Identity & Access</strong> (12 services)</summary>

| Service | Parity | PARITY Entries | Notes |
|---|---|---|---|
| [Cognito Identity](services/cognitoidentity/README.md) | A | 23 | 4 deferred |
| [Cognito Identity Provider](services/cognitoidp/README.md) | A | 71 | 2 gaps; 3 structural gaps |
| [Directory Service](services/directoryservice/README.md) | A | 80 | 2 structural gaps; 1 deferred |
| [IAM](services/iam/README.md) | A | 38 | 2 gaps; 3 structural gaps |
| [IAM Access Analyzer](services/accessanalyzer/README.md) | A | 39 | 5 structural gaps; 1 deferred |
| [IAM Identity Center (SSO)](services/ssoadmin/README.md) | A | 56 | clean |
| [IAM Identity Center OIDC](services/ssooidc/README.md) | B | 4 | 1 structural gap |
| [IAM Identity Center Portal](services/sso/README.md) | B | 4 | 1 structural gap |
| [IAM Roles Anywhere](services/rolesanywhere/README.md) | A | 30 | 2 structural gaps |
| [Identity Store](services/identitystore/README.md) | A | 19 | 1 deferred |
| [STS](services/sts/README.md) | A | 11 | 1 structural gap; 1 deferred |
| [Sign-In](services/signin/README.md) | B | 11 | 1 gap; 1 structural gap |

</details>

<details>
<summary><strong>Management & Governance</strong> (18 services)</summary>

| Service | Parity | PARITY Entries | Notes |
|---|---|---|---|
| [Account](services/account/README.md) | A | 18 | 3 structural gaps; 1 deferred |
| [AppConfig](services/appconfig/README.md) | A | 56 | 1 structural gap |
| [AppConfig Data](services/appconfigdata/README.md) | A | 2 | clean |
| [Application Auto Scaling](services/applicationautoscaling/README.md) | A | 14 | 2 structural gaps; 2 deferred |
| [Cloud Control API](services/cloudcontrol/README.md) | A | 8 | 2 gaps; 3 structural gaps |
| [CloudFormation](services/cloudformation/README.md) | A | 73 | 6 gaps; 2 structural gaps |
| [CloudTrail](services/cloudtrail/README.md) | A | 60 | 4 structural gaps |
| [CloudWatch](services/cloudwatch/README.md) | A | 55 | 1 structural gap; 16 deferred |
| [CloudWatch Logs](services/cloudwatchlogs/README.md) | A | 86 | 9 structural gaps |
| [Config](services/awsconfig/README.md) | A | 102 | 2 structural gaps; 1 deferred |
| [Cost Explorer](services/ce/README.md) | A | 37 | 5 structural gaps; 2 deferred |
| [Fault Injection Simulator](services/fis/README.md) | A | 26 | 2 structural gaps; 1 deferred |
| [OpsWorks](services/opsworks/README.md) | A | 32 | 2 structural gaps; 1 deferred |
| [Organizations](services/organizations/README.md) | A | 63 | 2 structural gaps |
| [Resource Access Manager](services/ram/README.md) | A | 36 | 1 structural gap; 2 deferred |
| [Resource Groups](services/resourcegroups/README.md) | A | 23 | 2 structural gaps |
| [Resource Groups Tagging API](services/resourcegroupstaggingapi/README.md) | A | 9 | 2 structural gaps; 1 deferred |
| [Systems Manager](services/ssm/README.md) | A | 105 | 14 structural gaps |

</details>

<details>
<summary><strong>Developer Tools</strong> (10 services)</summary>

| Service | Parity | PARITY Entries | Notes |
|---|---|---|---|
| [Amplify](services/amplify/README.md) | A | 37 | 3 structural gaps |
| [CodeArtifact](services/codeartifact/README.md) | A | 48 | 3 structural gaps |
| [CodeBuild](services/codebuild/README.md) | A | 59 | 2 structural gaps; 1 deferred |
| [CodeCommit](services/codecommit/README.md) | A | 80 | 1 structural gap |
| [CodeConnections](services/codeconnections/README.md) | A | 27 | clean |
| [CodeDeploy](services/codedeploy/README.md) | A | 47 | 3 structural gaps; 1 deferred |
| [CodePipeline](services/codepipeline/README.md) | A | 22 | 1 gap; 7 structural gaps; 1 deferred |
| [CodeStar Connections](services/codestarconnections/README.md) | A | 27 | 2 structural gaps |
| [Serverless Application Repository](services/serverlessrepo/README.md) | A | 14 | clean |
| [X-Ray](services/xray/README.md) | A | 38 | 6 structural gaps; 1 deferred |

</details>

<details>
<summary><strong>Machine Learning</strong> (13 services)</summary>

| Service | Parity | PARITY Entries | Notes |
|---|---|---|---|
| [Bedrock](services/bedrock/README.md) | A | 80 | 3 structural gaps |
| [Bedrock Agent](services/bedrockagent/README.md) | A | 77 | 3 structural gaps; 2 deferred |
| [Bedrock Runtime](services/bedrockruntime/README.md) | A | 11 | 5 structural gaps |
| [Comprehend](services/comprehend/README.md) | A | 28 | 1 structural gap; 1 deferred |
| [Forecast](services/forecast/README.md) | A | 21 | clean |
| [Personalize](services/personalize/README.md) | A | 74 | clean |
| [Polly](services/polly/README.md) | A | 10 | clean |
| [Rekognition](services/rekognition/README.md) | A | 50 | 3 structural gaps; 3 deferred |
| [SageMaker](services/sagemaker/README.md) | A | 70 | 19 structural gaps |
| [SageMaker Runtime](services/sagemakerruntime/README.md) | A | 3 | 1 structural gap |
| [Textract](services/textract/README.md) | A | 25 | 2 structural gaps; 1 deferred |
| [Transcribe](services/transcribe/README.md) | A | 44 | 6 gaps; 2 structural gaps |
| [Translate](services/translate/README.md) | A | 19 | 2 structural gaps |

</details>

<details>
<summary><strong>Media</strong> (6 services)</summary>

| Service | Parity | PARITY Entries | Notes |
|---|---|---|---|
| [MediaConvert](services/mediaconvert/README.md) | A | 34 | 2 structural gaps; 1 deferred |
| [MediaLive](services/medialive/README.md) | A | — | 26 families; 1 gap; 1 structural gap |
| [MediaPackage](services/mediapackage/README.md) | A | 19 | 1 deferred |
| [MediaStore](services/mediastore/README.md) | A | 21 | clean |
| [MediaStore Data](services/mediastoredata/README.md) | A | 5 | 1 structural gap; 1 deferred |
| [MediaTailor](services/mediatailor/README.md) | A | 48 | 2 structural gaps |

</details>

<details>
<summary><strong>IoT</strong> (4 services)</summary>

| Service | Parity | PARITY Entries | Notes |
|---|---|---|---|
| [IoT Analytics](services/iotanalytics/README.md) | A | 34 | 1 structural gap |
| [IoT Core](services/iot/README.md) | A | 88 | 2 gaps; 4 structural gaps |
| [IoT Data Plane](services/iotdataplane/README.md) | A | 11 | 1 structural gap; 1 deferred |
| [IoT Wireless](services/iotwireless/README.md) | A | 21 | 2 structural gaps |

</details>

<details>
<summary><strong>Migration & Transfer</strong> (3 services)</summary>

| Service | Parity | PARITY Entries | Notes |
|---|---|---|---|
| [DataSync](services/datasync/README.md) | A | 53 | 3 structural gaps; 1 deferred |
| [Database Migration Service](services/dms/README.md) | A | 97 | 3 gaps; 3 structural gaps |
| [Transfer Family](services/transfer/README.md) | A | — | 20 families; 4 structural gaps |

</details>

<details>
<summary><strong>Azure</strong> (17 services)</summary>

| Service | Parity | PARITY Entries | Notes |
|---|---|---|---|
| [Azure AI Search](services/azuresearch/README.md) | B | 19 | 4 gaps |
| [Azure App Configuration](services/azureappconfig/README.md) | B | 14 | 2 gaps |
| [Azure Blob Storage](services/azureblob/README.md) | C | 35 | 4 gaps; 3 deferred |
| [Azure Cache for Redis](services/azureredis/README.md) | B | 13 | 9 gaps |
| [Azure Container Registry](services/azurecontainerregistry/README.md) | B | 24 | 3 gaps |
| [Azure Cosmos DB](services/cosmosdb/README.md) | A | 35 | clean |
| [Azure Event Grid](services/azureeventgrid/README.md) | B | 10 | 8 gaps |
| [Azure Event Hubs](services/azureeventhubs/README.md) | B | 16 | 4 gaps |
| [Azure Files](services/azurefile/README.md) | B | 42 | 2 gaps |
| [Azure Functions](services/azurefunctions/README.md) | B | 17 | 4 gaps |
| [Azure Key Vault](services/azurekeyvault/README.md) | B | 15 | 1 gap |
| [Azure Monitor Logs](services/azuremonitor/README.md) | B | 20 | 4 gaps |
| [Azure Queue Storage](services/azurequeue/README.md) | C | 13 | 5 gaps; 1 deferred |
| [Azure SQL Database](services/azuresql/README.md) | B | 14 | 8 gaps |
| [Azure Service Bus](services/azureservicebus/README.md) | B | 25 | 4 gaps; 4 deferred |
| [Azure Table Storage](services/azuretable/README.md) | C | 14 | 5 gaps; 1 deferred |
| [Microsoft Graph](services/azuregraph/README.md) | B | 37 | 5 gaps |

</details>

<details>
<summary><strong>GCP</strong> (29 services)</summary>

| Service | Parity | PARITY Entries | Notes |
|---|---|---|---|
| [API Gateway](services/gcpapigateway/README.md) | B | 25 | 1 gap |
| [Artifact Registry](services/gcpartifactregistry/README.md) | B | 52 | 3 gaps |
| [BigQuery](services/gcpbigquery/README.md) | B | 28 | 8 gaps |
| [Cloud Bigtable](services/gcpbigtable/README.md) | B | 67 | 1 gap |
| [Cloud Build](services/gcpcloudbuild/README.md) | B | 21 | 4 gaps |
| [Cloud DNS](services/gcpdns/README.md) | B | — | 8 families; 1 gap |
| [Cloud Datastore](services/gcpdatastore/README.md) | B | 13 | clean |
| [Cloud Functions](services/gcpfunctions/README.md) | B | 8 | 4 gaps |
| [Cloud IAM](services/gcpiam/README.md) | B | 31 | 4 gaps |
| [Cloud Logging](services/gcplogging/README.md) | B | 43 | 3 gaps |
| [Cloud Monitoring](services/gcpmonitoring/README.md) | B | 54 | 4 gaps |
| [Cloud Run](services/gcprun/README.md) | B | 34 | 3 gaps |
| [Cloud SQL](services/gcpsql/README.md) | B | 52 | 5 gaps |
| [Cloud Scheduler](services/gcpscheduler/README.md) | B | 10 | clean |
| [Cloud Spanner](services/gcpspanner/README.md) | B | 38 | 1 gap |
| [Cloud Tasks](services/gcpcloudtasks/README.md) | B | 20 | clean |
| [Compute Engine](services/gcpcompute/README.md) | B | 1 | 6 gaps |
| [Eventarc](services/gcpeventarc/README.md) | B | 47 | 3 gaps |
| [Google Cloud Batch](services/gcpbatch/README.md) | B | 14 | 4 gaps |
| [Google Cloud Firestore](services/gcpfirestore/README.md) | B | 17 | 3 gaps |
| [Google Cloud KMS](services/gcpkms/README.md) | B | 35 | 4 gaps |
| [Google Cloud Pub/Sub](services/gcppubsub/README.md) | B | 38 | 3 gaps |
| [Google Cloud Resource Manager and Service Usage](services/gcpresourcemanager/README.md) | B | 39 | 3 gaps |
| [Google Cloud Storage](services/gcpstorage/README.md) | B | 39 | 4 gaps |
| [Google Secret Manager](services/gcpsecretmanager/README.md) | B | 18 | clean |
| [Identity Platform](services/gcpidentitytoolkit/README.md) | B | 1 | clean |
| [Kubernetes Engine](services/gcpcontainer/README.md) | B | — | 5 families; 4 gaps |
| [Memorystore for Redis](services/gcpredis/README.md) | B | 14 | 3 gaps |
| [Workflows](services/gcpworkflows/README.md) | B | 15 | 1 gap |

</details>

<details>
<summary><strong>OCI</strong> (21 services)</summary>

| Service | Parity | PARITY Entries | Notes |
|---|---|---|---|
| [OCI API Gateway](services/ociapigateway/README.md) | B | 52 | 3 gaps |
| [OCI Artifacts and Container Registry](services/ociartifacts/README.md) | B | 37 | 1 gap |
| [OCI Autonomous Database](services/ocidatabase/README.md) | B | 21 | 5 gaps |
| [OCI Container Engine for Kubernetes](services/ocicontainerengine/README.md) | B | 50 | 1 gap |
| [OCI Container Instances](services/ocicontainerinstances/README.md) | B | 18 | 2 gaps |
| [OCI Core Services](services/ocicore/README.md) | B | 358 | 4 gaps |
| [OCI DNS](services/ocidns/README.md) | B | 52 | 2 gaps |
| [OCI Events](services/ocievents/README.md) | B | 6 | 2 gaps |
| [OCI File Storage](services/ocifilestorage/README.md) | B | 74 | 3 gaps |
| [OCI Functions](services/ocifunctions/README.md) | B | 16 | 2 gaps |
| [OCI Identity](services/ociidentity/README.md) | B | 97 | 7 gaps |
| [OCI Load Balancer](services/ociloadbalancer/README.md) | B | 61 | 3 gaps |
| [OCI Logging](services/ocilogging/README.md) | B | 32 | 2 gaps |
| [OCI Monitoring](services/ocimonitoring/README.md) | B | 18 | 3 gaps |
| [OCI MySQL HeatWave Database Service](services/ocimysql/README.md) | B | 63 | 3 gaps |
| [OCI NoSQL Database Cloud Service](services/ocinosql/README.md) | B | 27 | 2 gaps |
| [OCI Notifications](services/ocinotifications/README.md) | B | 16 | 1 gap |
| [OCI Object Storage](services/ociobjectstorage/README.md) | B | 57 | 5 gaps |
| [OCI Queue](services/ociqueue/README.md) | B | 19 | 1 gap |
| [OCI Streaming](services/ocistreaming/README.md) | B | 30 | 1 gap |
| [OCI Vault](services/ocivault/README.md) | B | 62 | 2 gaps |

</details>

<details>
<summary><strong>Other</strong> (42 services)</summary>

| Service | Parity | PARITY Entries | Notes |
|---|---|---|---|
| [Amp](services/amp/README.md) | B | 44 | 2 gaps; 2 structural gaps |
| [AppStream 2.0](services/appstream/README.md) | A | 44 | 5 structural gaps |
| [Appintegrations](services/appintegrations/README.md) | B | 23 | 1 structural gap |
| [Azurearm](services/azurearm/README.md) | B | 21 | 42 gaps; 3 deferred |
| [Azurestoragevhost](services/azurestoragevhost/README.md) | B | 2 | 1 deferred |
| [Bcmdataexports](services/bcmdataexports/README.md) | B | 12 | clean |
| [Bedrockagentcore](services/bedrockagentcore/README.md) | B | 32 | 7 gaps; 2 structural gaps |
| [Bedrockagentcorecontrol](services/bedrockagentcorecontrol/README.md) | B | 171 | 6 gaps; 3 structural gaps |
| [Budgets](services/budgets/README.md) | B | 26 | 5 gaps |
| [Cloudfrontkeyvaluestore](services/cloudfrontkeyvaluestore/README.md) | A | 6 | 2 structural gaps |
| [Cloudhsmv2](services/cloudhsmv2/README.md) | B | 18 | 3 structural gaps |
| [Codegurureviewer](services/codegurureviewer/README.md) | B | 14 | 2 structural gaps |
| [Connect](services/connect/README.md) | B | 397 | 8 gaps; 2 structural gaps |
| [Cur](services/cur/README.md) | B | 7 | clean |
| [Directconnect](services/directconnect/README.md) | A | 73 | 8 structural gaps; 1 deferred |
| [Dsql](services/dsql/README.md) | A | 16 | 7 gaps; 2 structural gaps |
| [Ecrpublic](services/ecrpublic/README.md) | A | 23 | 3 structural gaps |
| [Globalaccelerator](services/globalaccelerator/README.md) | B | 56 | 1 gap; 2 structural gaps |
| [Grafana](services/grafana/README.md) | A | 25 | 1 structural gap |
| [HealthOmics](services/omics/README.md) | A | — | 25 families; 5 structural gaps; 1 deferred |
| [Kafkaconnect](services/kafkaconnect/README.md) | A | 19 | 1 gap; 3 structural gaps |
| [Kinesisvideo](services/kinesisvideo/README.md) | A | 32 | 3 structural gaps |
| [Lambdaweb](services/lambdaweb/README.md) | B | 20 | 3 gaps; 1 structural gap |
| [Lightsail](services/lightsail/README.md) | A | — | 29 families; 6 structural gaps; 2 deferred |
| [Managed Blockchain](services/managedblockchain/README.md) | A | 27 | 3 structural gaps |
| [Marketplaceentitlementservice](services/marketplaceentitlementservice/README.md) | B | 1 | 1 gap; 1 structural gap |
| [Marketplacemetering](services/marketplacemetering/README.md) | B | 4 | 2 structural gaps |
| [Mgn](services/mgn/README.md) | A | 95 | 8 structural gaps; 1 deferred |
| [Networkfirewall](services/networkfirewall/README.md) | B | 85 | 4 structural gaps |
| [Networkmanager](services/networkmanager/README.md) | A | 95 | 2 structural gaps |
| [Oam](services/oam/README.md) | B | 15 | 1 structural gap |
| [Outposts](services/outposts/README.md) | A | 45 | 7 structural gaps |
| [Pricing](services/pricing/README.md) | B | 5 | 4 gaps |
| [Resiliencehub](services/resiliencehub/README.md) | A | 63 | 7 structural gaps |
| [Resourceexplorer2](services/resourceexplorer2/README.md) | B | 29 | 1 gap; 1 structural gap |
| [Rum](services/rum/README.md) | B | 20 | 1 gap; 2 structural gaps |
| [S3vectors](services/s3vectors/README.md) | B | 21 | 1 gap; 1 structural gap |
| [Servicecatalog](services/servicecatalog/README.md) | B | 90 | 1 structural gap |
| [Servicequotas](services/servicequotas/README.md) | B | 26 | 2 gaps; 1 structural gap |
| [Support](services/support/README.md) | A | 20 | 1 structural gap; 1 deferred |
| [Timestreaminfluxdb](services/timestreaminfluxdb/README.md) | B | 24 | 2 gaps; 3 structural gaps |
| [WorkSpaces](services/workspaces/README.md) | A | 34 | 1 gap; 2 structural gaps |

</details>
<!-- END GENERATED SERVICES -->

## Development

```bash
task --list         # all targets
task test           # unit tests (-race -short)
task lint-changed   # lint packages touched by the branch
task docs           # regenerate the service table above and services/*/README.md
task pgo            # regenerate default.pgo (profile-guided optimization)
```

Needs Go 1.27+. Docker or Podman only for container-backed features.

## Versioning

Tags are `v1.<major>.<minor>`. The Release workflow offers `minor` and `major` bumps. The module path has no `/vN` suffix, so the leading `1` stays. `v1.0.x` is retired and never reused: the Go proxy has cached those versions permanently.

## License

[MIT](LICENSE)
