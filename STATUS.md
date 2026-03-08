# Gopherstack Feature Parity & Status Matrix

This document tracks the implementation status of AWS services in Gopherstack, specifically focusing on feature parity with Localstack.

> **Note:** Gopherstack implements 42 services, covering 34 Localstack Community (Free) Tier services and 8 Localstack Pro (Paid) Tier services, offering significant competitive advantages.

## Localstack Community (Free) Tier Parity

| Service | API Implemented | Has Dashboard UI | Has Terraform Tests |
| --- | --- | --- | --- |
| **ACM** | ✅ Yes | ✅ Yes | ✅ Yes |
| **API Gateway** | ✅ Yes | ✅ Yes | ✅ Yes |
| **AWS Config** | ✅ Yes | ✅ Yes | ✅ Yes |
| **CloudFormation** | ✅ Yes | ✅ Yes | ✅ Yes |
| **CloudWatch** | ✅ Yes | ✅ Yes | ✅ Yes |
| **CloudWatch Logs** | ✅ Yes | ✅ Yes | ✅ Yes |
| **Cognito IDP** | ✅ Yes | ❌ No | ❌ No |
| **DynamoDB** | ✅ Yes | ✅ Yes | ✅ Yes |
| **EC2** | ✅ Yes | ✅ Yes | ✅ Yes |
| **EventBridge** | ✅ Yes | ✅ Yes | ✅ Yes |
| **Firehose** | ✅ Yes | ✅ Yes | ✅ Yes |
| **IAM** | ✅ Yes | ✅ Yes | ✅ Yes |
| **Kinesis** | ✅ Yes | ✅ Yes | ✅ Yes |
| **KMS** | ✅ Yes | ✅ Yes | ✅ Yes |
| **Lambda** | ✅ Yes | ✅ Yes | ✅ Yes |
| **OpenSearch** / Elasticsearch | ✅ Yes | ✅ Yes | ✅ Yes |
| **Redshift** | ✅ Yes | ✅ Yes | ✅ Yes |
| **Resource Groups** | ✅ Yes | ✅ Yes | ✅ Yes |
| **Resource Groups Tagging API** | ✅ Yes | ❌ No | ❌ No |
| **Route 53** | ✅ Yes | ✅ Yes | ✅ Yes |
| **Route 53 Resolver** | ✅ Yes | ✅ Yes | ✅ Yes |
| **S3** | ✅ Yes | ✅ Yes | ✅ Yes |
| **S3 Control** | ✅ Yes | ✅ Yes | ✅ Yes |
| **Scheduler** | ✅ Yes | ✅ Yes | ✅ Yes |
| **Secrets Manager** | ✅ Yes | ✅ Yes | ✅ Yes |
| **SES** | ✅ Yes | ✅ Yes | ✅ Yes |
| **SNS** | ✅ Yes | ✅ Yes | ✅ Yes |
| **SQS** | ✅ Yes | ✅ Yes | ✅ Yes |
| **SSM** | ✅ Yes | ✅ Yes | ✅ Yes |
| **Step Functions** | ✅ Yes | ✅ Yes | ✅ Yes |
| **STS** | ✅ Yes | ✅ Yes | ❌ No |
| **Support API** | ✅ Yes | ✅ Yes | ❌ No |
| **SWF** | ✅ Yes | ✅ Yes | ✅ Yes |
| **Transcribe** | ✅ Yes | ✅ Yes | ❌ No |

## Localstack Pro (Paid) Tier Services implemented in Gopherstack Free

Localstack locks these services behind a paid tier. Gopherstack offers them natively for free.

| Service | API Implemented | Has Dashboard UI | Has Terraform Tests |
| --- | --- | --- | --- |
| **AppSync** | ✅ Yes | ❌ No | ✅ Yes |
| **ECR** | ✅ Yes | ❌ No | ✅ Yes (with Lambda) |
| **ECS** | ✅ Yes | ❌ No | ✅ Yes |
| **ElastiCache** | ✅ Yes | ✅ Yes | ✅ Yes |
| **FIS** | ✅ Yes | ❌ No | ❌ No |
| **IoT** | ✅ Yes | ❌ No | ❌ No |
| **IoT Data Plane** | ✅ Yes | ❌ No | ❌ No |
| **RDS** | ✅ Yes | ✅ Yes | ✅ Yes |

## LocalStack Enterprise Features in Gopherstack Free

Chaos Engineering is locked behind the LocalStack Enterprise (paid) tier. Gopherstack offers it for free.

| Feature | Implemented | Dashboard UI | REST API |
| --- | --- | --- | --- |
| **Chaos API** — fault injection and network effects | ✅ Yes | ✅ Yes | ✅ Yes |

The Chaos API supports:
- Per-service, per-region, per-operation fault injection with configurable error codes and probability
- Dynamic latency simulation (fixed, range, jitter)
- Real-time activity log
- Auto-discovery of all 42 injectable services via `GET /_gopherstack/chaos/targets`

## Summary of Missing Components

*These have been logged as GitHub Issues and should be tackled incrementally.*

### Missing UIs (Dashboard)
- AppSync (Pro)
- Cognito IDP (Free)
- ECR (Pro)
- ECS (Pro)
- FIS (Pro)
- IoT (Pro)
- IoT Data Plane (Pro)
- Resource Groups Tagging API (Free)

### Missing Terraform Tests (`test/terraform/fixtures/`)
- Cognito IDP (Free)
- FIS (Pro)
- IoT (Pro)
- IoT Data Plane (Pro)
- Resource Groups Tagging API (Free)
- STS (Free)
- Support API (Free)
- Transcribe (Free)

## Not Implemented Features (Localstack Parity Gap)

The following services are supported by Localstack (either Free or Pro tier) but are not currently implemented in Gopherstack.

| Service | API Implemented | Has Dashboard UI | Has Terraform Tests |
| --- | --- | --- | --- |
| **Account** | ❌ No | ❌ No | ❌ No |
| **Acm Pca** | ❌ No | ❌ No | ❌ No |
| **Amplify** | ❌ No | ❌ No | ❌ No |
| **APIgatewaymanagementapi** | ❌ No | ❌ No | ❌ No |
| **APIgatewayv2** | ❌ No | ❌ No | ❌ No |
| **Appconfig** | ❌ No | ❌ No | ❌ No |
| **Appconfigdata** | ❌ No | ❌ No | ❌ No |
| **Application Autoscaling** | ❌ No | ❌ No | ❌ No |
| **Athena** | ❌ No | ❌ No | ❌ No |
| **Autoscaling** | ❌ No | ❌ No | ❌ No |
| **Backup** | ❌ No | ❌ No | ❌ No |
| **Batch** | ❌ No | ❌ No | ❌ No |
| **Bedrock** | ❌ No | ❌ No | ❌ No |
| **Bedrock Runtime** | ❌ No | ❌ No | ❌ No |
| **Ce** | ❌ No | ❌ No | ❌ No |
| **Cloudcontrol** | ❌ No | ❌ No | ❌ No |
| **Cloudfront** | ❌ No | ❌ No | ❌ No |
| **Cloudtrail** | ❌ No | ❌ No | ❌ No |
| **Codeartifact** | ❌ No | ❌ No | ❌ No |
| **Codebuild** | ❌ No | ❌ No | ❌ No |
| **Codecommit** | ❌ No | ❌ No | ❌ No |
| **Codeconnections** | ❌ No | ❌ No | ❌ No |
| **Codedeploy** | ❌ No | ❌ No | ❌ No |
| **Codepipeline** | ❌ No | ❌ No | ❌ No |
| **Codestar Connections** | ❌ No | ❌ No | ❌ No |
| **Cognito Identity** | ❌ No | ❌ No | ❌ No |
| **Dms** | ❌ No | ❌ No | ❌ No |
| **Docdb** | ❌ No | ❌ No | ❌ No |
| **Dynamodbstreams** | ❌ No | ❌ No | ❌ No |
| **Efs** | ❌ No | ❌ No | ❌ No |
| **Eks** | ❌ No | ❌ No | ❌ No |
| **Elasticbeanstalk** | ❌ No | ❌ No | ❌ No |
| **Elastictranscoder** | ❌ No | ❌ No | ❌ No |
| **Elb** | ❌ No | ❌ No | ❌ No |
| **Elbv2** | ❌ No | ❌ No | ❌ No |
| **Emr** | ❌ No | ❌ No | ❌ No |
| **Emr Serverless** | ❌ No | ❌ No | ❌ No |
| **Glacier** | ❌ No | ❌ No | ❌ No |
| **Glue** | ❌ No | ❌ No | ❌ No |
| **Identitystore** | ❌ No | ❌ No | ❌ No |
| **Iotanalytics** | ❌ No | ❌ No | ❌ No |
| **Iotwireless** | ❌ No | ❌ No | ❌ No |
| **Kafka** | ❌ No | ❌ No | ❌ No |
| **Kinesisanalytics** | ❌ No | ❌ No | ❌ No |
| **Kinesisanalyticsv2** | ❌ No | ❌ No | ❌ No |
| **Lakeformation** | ❌ No | ❌ No | ❌ No |
| **Managedblockchain** | ❌ No | ❌ No | ❌ No |
| **Mediaconvert** | ❌ No | ❌ No | ❌ No |
| **Mediastore** | ❌ No | ❌ No | ❌ No |
| **Mediastore Data** | ❌ No | ❌ No | ❌ No |
| **Memorydb** | ❌ No | ❌ No | ❌ No |
| **Mq** | ❌ No | ❌ No | ❌ No |
| **Mwaa** | ❌ No | ❌ No | ❌ No |
| **Neptune** | ❌ No | ❌ No | ❌ No |
| **Organizations** | ❌ No | ❌ No | ❌ No |
| **Pinpoint** | ❌ No | ❌ No | ❌ No |
| **Pipes** | ❌ No | ❌ No | ❌ No |
| **Qldb** | ❌ No | ❌ No | ❌ No |
| **Qldb Session** | ❌ No | ❌ No | ❌ No |
| **Ram** | ❌ No | ❌ No | ❌ No |
| **Rds Data** | ❌ No | ❌ No | ❌ No |
| **Redshift Data** | ❌ No | ❌ No | ❌ No |
| **Sagemaker** | ❌ No | ❌ No | ❌ No |
| **Sagemaker Runtime** | ❌ No | ❌ No | ❌ No |
| **Serverlessrepo** | ❌ No | ❌ No | ❌ No |
| **Servicediscovery** | ❌ No | ❌ No | ❌ No |
| **Sesv2** | ❌ No | ❌ No | ❌ No |
| **Shield** | ❌ No | ❌ No | ❌ No |
| **Sso Admin** | ❌ No | ❌ No | ❌ No |
| **Textract** | ❌ No | ❌ No | ❌ No |
| **Timestream Query** | ❌ No | ❌ No | ❌ No |
| **Timestream Write** | ❌ No | ❌ No | ❌ No |
| **Transfer** | ❌ No | ❌ No | ❌ No |
| **Verifiedpermissions** | ❌ No | ❌ No | ❌ No |
| **Wafv2** | ❌ No | ❌ No | ❌ No |
| **Xray** | ❌ No | ❌ No | ❌ No |
