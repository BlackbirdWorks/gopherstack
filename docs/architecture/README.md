# Architecture Overview

Gopherstack is a single Go binary that implements the AWS wire protocol for over 25 services. All state is kept in memory with optional disk persistence.

## Design principles

- **Single binary** — all services run in one process; no containers, no databases, no external dependencies (except Docker for Lambda).
- **Native wire protocol** — every service speaks the exact same JSON/XML/form-encoded protocol as the real AWS SDK, so any AWS SDK works without modification.
- **In-memory by default** — startup is sub-100 ms and teardown is instant. Persistence is opt-in.
- **Production-shaped** — the same service interfaces, ARN formats, and pagination patterns as real AWS, so code that works locally works in production.

## Request routing

Gopherstack uses [Echo](https://echo.labstack.com/) as the HTTP server. Each service registers a `Registerable` handler that implements:

```go
type Registerable interface {
    RouteMatcher() Matcher       // decides whether this request belongs to this service
    MatchPriority() int          // higher = matched first (breaks ties)
    ExtractOperation(*echo.Context) string  // human-readable op name for metrics/logs
    Handle(*echo.Context) error  // processes the request
}
```

On every incoming request the registry walks all registered handlers in priority order and calls `RouteMatcher()`. The first match wins. This allows services to share a single port without URL-prefix collisions (e.g. S3 path-style vs. virtual-hosted style).

Priority constants (lower number = higher priority):
- `PriorityHeaderExact` — matched by exact `X-Amz-Target` value
- `PriorityTargetPrefixed` — matched by `X-Amz-Target` prefix
- `PriorityPathSubdomain` — matched by URL path or form field
- `PriorityFallback` — last resort

### Telemetry wrapping

Every handler is wrapped with `WrapEchoHandler` which:
1. Injects the service name into the logger context (`logger.AddAttrs`).
2. Records a Prometheus counter and histogram for every operation.
3. Logs the operation name, duration, and any errors.

Global middlewares (e.g. CORS, request ID, latency injection) run outside the telemetry timer.

## In-memory backends

Each service has an `InMemoryBackend` (or `InMemoryDB` for DynamoDB) that stores state in Go maps protected by `sync.RWMutex`. Backends are created at startup and shared for the lifetime of the process.

### Persistence

Backends that implement the `Persistable` interface:

```go
type Persistable interface {
    Snapshot() []byte    // serialize current state to JSON
    Restore([]byte) error // restore state from JSON
}
```

When `--persist` (`PERSIST=true`) is set:
1. At startup, `Manager.RestoreAll` loads snapshots from disk and calls `Restore`.
2. On any mutation, `Manager.Notify(serviceName)` triggers a 500 ms debounced `Snapshot` + write to the `FileStore`.
3. On SIGTERM/SIGINT, `Manager.SaveAll` writes all pending snapshots synchronously before exit.

Snapshots are stored in `~/.gopherstack/data/<service>/snapshot` (or `/data/` in containers).

## Regions

One process can serve several regions. The shared middleware resolves the request region from the SigV4
credential scope, then `X-Amz-Region`, then the configured default, and stores it on the context
(`awsmeta.Region(ctx)`).

- **Global by AWS definition** (no region key): IAM, Route 53, CloudFront, Organizations, STS global endpoint, WAF classic, ECR Public (AWS serves it only from us-east-1), Network Manager (a global service homed in us-west-2; its ARNs carry no region).
  S3 bucket names are globally unique, but each bucket has a region.
- **Regional with per-request keys**: ssm, cloudwatchlogs, memorydb, sqs, sns, dynamodb, kms, kinesis, elb, firehose, acm, acmpca, batch,
  codepipeline, dms, elasticbeanstalk, emr, kinesisanalyticsv2, sagemaker, route53resolver, elasticsearch, codeartifact, codeconnections,
  codestarconnections, databrew, directoryservice, identitystore, dynamodbstreams, kinesisanalytics, mediastore, mediastoredata, mwaa, networkmonitor, resourcegroups, rolesanywhere,
  textract, workmail, rdsdata, redshiftdata, timestreamquery, and most others.
- **Regional via sibling handlers**: s3control, lightsail, glue, ecr, ec2, ecs, autoscaling, cloudformation, elbv2, codedeploy, lambda,
  eks, mq, redshift (and Serverless), opensearch, appsync, ses, apprunner, codebuild, emrserverless, guardduty, securityhub, xray,
  transfer, awsconfig, applicationautoscaling, dax, vpclattice, cloudtrail, fsx, accessanalyzer, amplify, apigatewaymanagementapi, appconfig,
  appconfigdata, appmesh, appstream, bedrock, bedrockagent, bedrockruntime, cleanrooms, comprehend, datasync, detective, directconnect, dlm,
  glacier, grafana, inspector2, iotanalytics, iotdataplane, iotwireless, kafkaconnect, kinesisvideo, lakeformation, macie2, managedblockchain, mediaconvert, medialive,
  mediapackage, mediatailor, mgn, omics, opsworks, outposts, personalize, pinpoint, polly, quicksight, ram, rekognition, resiliencehub,
  s3tables, sagemakerruntime, serverlessrepo, ssoadmin (Identity Center instances are regional), swf, timestreamwrite, transcribe, translate,
  verifiedpermissions, workspaces and others build one sibling handler per extra region (`pkgs/regionpeers`); snapshots add an optional `regions` key.
- **Cross-service calls follow the originating resource's region**: SQS and CloudWatch Logs publish metrics to the CloudWatch
  of the emitting queue or log group's region; Step Functions, Scheduler and the tagging bridge reach the ECS (and Glue)
  backend of the ARN or execution region via `regionpeers.Backend`. Auto Scaling launches and terminates instances in its own
  region's EC2 (the group passes its region on the context), registers ELBv2 targets in the target group ARN's region (as does
  ECS), and CloudWatch alarm scaling actions run the policy in the policy ARN's region. CloudFormation builds one resource
  creator per region (`ServiceBackends.forRegion`) so every resource lands in the stack's region; CodeDeploy resolves EC2
  targets in the deployment group's region. The tagging bridge lists the request region and resolves Tag/Untag by ARN region for
  Athena, Glue, ECR, Backup, CodeCommit, Cloud Map, Lightsail, Cognito, SESv2 and CodeDeploy. Lambda invocations from SNS, SQS
  event source mappings, EventBridge, Step Functions, IoT, Firehose, CloudWatch Logs, S3, Cognito and API Gateway resolve the
  function by its ARN region (else the request region); every region's Lambda shares one container runtime and runs its own
  event source poller. Classic ELB and Firehose read the region from the shared request metadata, so Auto Scaling, SNS,
  EventBridge, Pipes, IoT and CloudWatch Logs reach the right region's load balancer or delivery stream. Grafana accepts EC2
  resources of any region, MGN uses its own region's EC2, Resilience Hub resolves EC2 source ARNs by region, and the Lightsail
  CloudFormation export creates the stack in its own region. OpenSearch is per region: a Firehose domain-ARN destination indexes into the domain of the ARN's region. Lambda MQ event source
  mappings find the broker in the broker ARN's region (MQ and EKS siblings share the home docker runtime and bounded port
  allocator), AppSync resolvers call DynamoDB in the data source's region (else the API's) and Lambda in the function ARN's region
  (else the API's), and GraphQL requests find their API by id across regions. WAFv2 keys REGIONAL resources by the request region
  (no siblings; CLOUDFRONT scope stays global, as AWS requires US East for it). RDS, DocumentDB, Neptune, Kafka, Scheduler, Pipes
  and Cognito Identity already isolate regions internally.
  CodePipeline Build, Invoke and Deploy actions call CodeBuild, Lambda and CodeDeploy in the pipeline's region, CloudTrail records
  each captured management event in the region the call targeted, and Application Auto Scaling checks DynamoDB tables in its own
  region. The tagging bridge also follows the region for App Runner, EMR Serverless, GuardDuty, Security Hub, X-Ray, Transfer, Config,
  Application Auto Scaling, DAX and VPC Lattice.
  AppConfig deployments publish into the AppConfig Data backend of the deployment's region, API Gateway v2 registers each WebSocket
  connection with the Management API of the API's region, and IoT rule actions write to the IoT Analytics channel and read the IoT
  Data Plane shadow of the rule's region (all regions share the one MQTT broker). Direct Connect gateways, associations and proposals are account-global and always served by the home backend. The tagging bridge also follows the region for Access
  Analyzer, AppConfig, App Mesh, AppStream, Clean Rooms, Comprehend, DataSync, Detective, Direct Connect, DLM, Grafana and Inspector.
  RDS Data finds the Aurora cluster in the region its ARN names, Timestream Query mirrors scheduled-query tags into the Timestream
  Write backend of the ARN's region, Redshift Data runs Firehose COPY statements in the delivery stream's region, and SageMaker Runtime
  validates endpoints through the SageMaker backend of the request region. CloudFormation provisions Kafka Connect, Kinesis Video, Macie,
  SWF and Resilience Hub resources in the stack's region. The tagging bridge also follows the region for Macie, Managed Blockchain,
  MediaConvert, MediaPackage, MediaTailor, OpsWorks, Personalize, Pinpoint, RAM, Rekognition, S3 Tables, Identity Center, SWF, Timestream
  Write, Transcribe, Translate, Verified Permissions, MGN, Outposts and Resilience Hub. QLDB and QLDB Session are removed (AWS end of support).
- **Still single-region per process**: services not listed in `region_isolation_cases_test.go` (same-named resources in two
  regions collide); a `knownCollision` case there fails once such a service is fixed.

## DNS server

An optional embedded DNS server (based on `miekg/dns`) can be enabled with `--dns-addr :10053`. Services that create network-addressable resources (RDS, Redshift, ElastiCache, OpenSearch) automatically register/deregister synthetic hostnames when resources are created/deleted.

See [dns.md](dns.md) for configuration and OS integration.

## Service integrations

Some services are wired together at startup:

- **SNS → SQS**: SNS fan-out delivers to subscribed SQS queues in-process.
- **EventBridge → Lambda/SQS/SNS**: Target invocations happen in the same process.
- **Step Functions → SQS/SNS/DynamoDB**: Task integrations call the in-process backends.
- **Lambda ESM → SQS**: Event source mappings poll the SQS backend and invoke Lambda containers.

## Port allocator

Services that open their own TCP ports (ElastiCache `embedded`, OpenSearch `docker`, RDS `docker`) use a shared `portalloc.Allocator` that assigns ports from the configured range (`PORT_RANGE_START`–`PORT_RANGE_END`, default 10000–10100).

## Further reading

- [ElastiCache engine modes](elasticache.md)
- [DNS setup](dns.md)
- [Lambda runtime](lambda.md)
