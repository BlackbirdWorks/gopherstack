# Gopherstack Roadmap — LocalStack Free Tier Parity

> **Goal:** Feature parity with LocalStack's free/community tier (~30 AWS services).
> **Current state:** 7 services (DynamoDB, S3, SSM Parameter Store, IAM, STS, SNS, SQS) with 150+ total operations.

---

## Current Coverage

### DynamoDB — ~90% parity

| Feature | Status |
|---------|--------|
| Table CRUD | Done |
| Item CRUD (Get, Put, Update, Delete) | Done |
| BatchGetItem / BatchWriteItem | Done |
| Query / Scan with expressions | Done |
| GSI / LSI | Done |
| Transactions (TransactWriteItems, TransactGetItems) | Done |
| DynamoDB Streams | Done |
| TTL | Done |
| Conditional writes | Done |
| Pagination | Done |

**Gaps to close:**
- [x] UpdateTable (modify provisioned throughput, add/remove GSI)
- [ ] DescribeContinuousBackups / point-in-time recovery stubs
- [ ] PartiQL support (ExecuteStatement, BatchExecuteStatement)
- [ ] TagResource / UntagResource / ListTagsOfResource
- [ ] Table export / import stubs

### S3 — ~75% parity

| Feature | Status |
|---------|--------|
| Bucket CRUD | Done |
| Object CRUD | Done |
| Multipart uploads | Done |
| Versioning | Done |
| Object tagging | Done |
| Checksums (CRC32, CRC32C, SHA1, SHA256) | Done |
| ListObjectsV2 | Done |
| Virtual-hosted style routing | Done |

**Gaps to close:**
- [ ] Presigned URLs
- [ ] CopyObject
- [ ] Bucket lifecycle configuration (expiration rules)
- [ ] Bucket notifications (events to SQS/SNS/Lambda)
- [ ] CORS configuration
- [ ] Bucket policies
- [ ] ACLs (GetObjectAcl, PutObjectAcl, GetBucketAcl, PutBucketAcl)
- [ ] Object lock / legal hold
- [ ] ListObjectVersions (API endpoint)
- [ ] ListMultipartUploads / ListParts

### SSM Parameter Store — ~80% parity

| Feature | Status |
|---------|--------|
| Put/Get/Delete parameters | Done |
| Batch get | Done |
| SecureString with mock KMS | Done |
| Parameter history | Done |

**Gaps to close:**
- [ ] GetParametersByPath (hierarchical path queries)
- [ ] DescribeParameters (filtering/pagination)
- [ ] Parameter tags
- [ ] StringList parameter type

---

## Platform Infrastructure Needed

### Internal DNS

Services like ElastiCache, OpenSearch, Redshift, and RDS return **endpoint hostnames** to clients (e.g., `my-cluster.abc123.us-east-1.cache.amazonaws.com`). Clients then connect to those hostnames directly. Gopherstack needs an internal DNS resolver so that:

- Created resources get synthetic hostnames that actually resolve
- The DNS server runs embedded (e.g., using `miekg/dns`) on a configurable port (default 53 or 10053)
- Clients can be pointed at it via `--dns` Docker flag or `/etc/resolv.conf`
- Hostnames follow AWS naming conventions per service:
  - ElastiCache: `{cluster-id}.{random}.{region}.cache.amazonaws.com`
  - OpenSearch: `search-{domain}.{region}.es.amazonaws.com`
  - RDS: `{instance-id}.{random}.{region}.rds.amazonaws.com`
  - Redshift: `{cluster-id}.{random}.{region}.redshift.amazonaws.com`
- All synthetic hostnames resolve back to `127.0.0.1` (or the Gopherstack container IP)

### Port Range Management

Services that expose real network endpoints (ElastiCache, OpenSearch, RDS, Lambda function URLs) need dedicated ports. Gopherstack needs a **port allocator**:

- Configurable port range via `PORT_RANGE_START` / `PORT_RANGE_END` (e.g., `10000-10100`)
- Central `PortAllocator` that hands out ports from the pool, tracks usage, and reclaims on resource deletion
- Services request a port when creating a resource (e.g., `CreateCacheCluster`) and release it on delete
- Health checks on allocated ports to detect zombie listeners
- Docker: expose the range in `docker-compose.yml` / Dockerfile
- The main API port (default 8000) stays separate — only resource-level endpoints use the range

### Docker Integration (Lambda Runtimes)

Go can't natively execute Python/Node.js/Java Lambda functions. The solution is **Docker-based runtime containers**, similar to how LocalStack does it:

- Use the Docker SDK for Go (`github.com/docker/docker/client`) to manage containers
- On `Lambda.Invoke`, spin up (or reuse) a container from the appropriate runtime image:
  - `public.ecr.aws/lambda/python:3.12`
  - `public.ecr.aws/lambda/nodejs:20`
  - `public.ecr.aws/lambda/go:al2023` (or run Go binaries directly on the host)
  - `public.ecr.aws/lambda/java:21`
  - Custom images via `PackageType: Image`
- **Container lifecycle:**
  - Cold start: pull image → create container → mount code → invoke via Runtime Interface Client (RIC)
  - Warm containers: keep alive for reuse with a configurable idle timeout
  - Container pool: pre-warm N containers per runtime for lower latency
  - Cleanup janitor: reap idle containers after timeout
- **Code delivery:**
  - `ZipFile`: extract to temp dir, bind-mount into container
  - `S3Bucket/S3Key`: pull from Gopherstack's own S3 service
  - `ImageUri`: pull and run directly
- **Runtime API:** Implement the [Lambda Runtime API](https://docs.aws.amazon.com/lambda/latest/dg/runtimes-api.html) HTTP endpoints (`/runtime/invocation/next`, `/runtime/invocation/{id}/response`) so standard Lambda runtimes work unmodified
- **Optional Docker-free mode:** For Go-only Lambda functions, support direct binary execution without Docker (compile and exec the handler binary on the host)
- **Requires Docker:** Lambda support is gated behind Docker availability — if Docker is not present, Lambda API returns a clear error

---

## New Services Needed

### Tier 1 — High Impact (Core serverless & messaging)

These are the most commonly used LocalStack services and cover the majority of real-world testing scenarios.

#### SQS (Simple Queue Service) ✅ **DONE**
- CreateQueue / DeleteQueue / ListQueues / GetQueueUrl / GetQueueAttributes / SetQueueAttributes ✅
- SendMessage / ReceiveMessage / DeleteMessage / ChangeMessageVisibility ✅
- SendMessageBatch / DeleteMessageBatch ✅
- Standard queues and FIFO queues (.fifo suffix, deduplication, message groups) ✅
- Dead-letter queue configuration (RedrivePolicy) ✅
- Message visibility timeout ✅
- Long polling (WaitTimeSeconds) ✅
- Purge queue ✅
- Message attributes ✅
- Dashboard UI ✅

#### SNS (Simple Notification Service) ✅ **DONE**
- CreateTopic / DeleteTopic / ListTopics / GetTopicAttributes / SetTopicAttributes ✅
- Subscribe / Unsubscribe / ListSubscriptions / ListSubscriptionsByTopic ✅
- Publish / PublishBatch ✅
- Subscription protocols: SQS, HTTP/HTTPS, Lambda, email (stub) ✅
- Subscription filter policies ✅
- FIFO topics ✅
- Message attributes ✅
- Dashboard UI ✅

#### Lambda
- CreateFunction / DeleteFunction / GetFunction / ListFunctions / UpdateFunctionCode / UpdateFunctionConfiguration
- Invoke (RequestResponse and Event invocation types)
- **Runtime execution via Docker containers** (Python, Node.js, Java, .NET, Ruby, custom images)
- **Go binary direct execution** (no Docker needed for Go handlers)
- Implement Lambda Runtime API so official AWS base images work unmodified
- Warm container pooling with configurable idle timeout
- Environment variables
- Event source mappings (SQS, DynamoDB Streams, Kinesis)
- Layers (mount as additional overlay in container)
- Function URLs (allocated from port range)
- Aliases and versions

#### IAM (Identity & Access Management) ✅ **DONE**
- Users: CreateUser, DeleteUser, ListUsers, GetUser ✅
- Roles: CreateRole, DeleteRole, ListRoles, GetRole, AssumeRole ✅
- Policies: CreatePolicy, DeletePolicy, AttachUserPolicy, AttachRolePolicy, ListPolicies ✅
- Groups: CreateGroup, DeleteGroup, AddUserToGroup ✅
- Access keys: CreateAccessKey, DeleteAccessKey, ListAccessKeys ✅
- Instance profiles ✅
- Dashboard UI for resource management ✅
- _Note: Policy enforcement is out of scope (LocalStack free tier doesn't enforce either)_

#### STS (Security Token Service) ✅ **DONE**
- AssumeRole ✅
- GetCallerIdentity ✅
- GetSessionToken ✅
- AssumeRoleWithWebIdentity ✅
- GetFederationToken ✅
- Dashboard UI ✅

#### KMS (Key Management Service)
- CreateKey / DescribeKey / ListKeys / ListAliases
- CreateAlias / DeleteAlias
- Encrypt / Decrypt / GenerateDataKey / ReEncrypt
- Key rotation (EnableKeyRotation, GetKeyRotationStatus)
- Grants
- _Replace current mock KMS in SSM with real KMS service_

#### Secrets Manager
- CreateSecret / GetSecretValue / PutSecretValue / DeleteSecret
- ListSecrets / DescribeSecret
- UpdateSecret
- Secret versioning (AWSCURRENT, AWSPREVIOUS staging labels)
- RestoreSecret
- Tag support

### Tier 2 — Medium Impact (Event-driven & orchestration)

#### API Gateway (REST APIs)
- CreateRestApi / DeleteRestApi / GetRestApis
- Resources, methods, integrations
- Deployments and stages
- Lambda proxy integration
- Mock integrations
- Request/response mapping templates

#### CloudFormation
- CreateStack / DeleteStack / UpdateStack / DescribeStacks / ListStacks
- Resource creation for all implemented services
- Outputs, parameters, mappings, conditions
- Stack events
- Change sets
- _Coverage limited to resources backed by implemented services_

#### Step Functions
- CreateStateMachine / DeleteStateMachine / ListStateMachines / DescribeStateMachine
- StartExecution / StopExecution / DescribeExecution / ListExecutions / GetExecutionHistory
- Standard and Express workflows
- State types: Task, Choice, Wait, Parallel, Map, Pass, Succeed, Fail
- Error handling (Retry, Catch)
- Lambda and service integrations

#### EventBridge
- CreateEventBus / DeleteEventBus / ListEventBuses
- PutRule / DeleteRule / ListRules / DescribeRule
- PutTargets / RemoveTargets / ListTargetsByRule
- PutEvents
- Target types: Lambda, SQS, SNS, Step Functions
- Event pattern matching
- Scheduled rules (cron/rate expressions)

#### CloudWatch (Metrics & Logs)
- **Metrics:** PutMetricData, GetMetricData, GetMetricStatistics, ListMetrics
- **Logs:** CreateLogGroup, CreateLogStream, PutLogEvents, GetLogEvents, FilterLogEvents, DescribeLogGroups, DescribeLogStreams, DeleteLogGroup
- **Alarms (basic):** PutMetricAlarm, DescribeAlarms, DeleteAlarms
- Metric math expressions (stub)

### Tier 3 — Lower Impact (Specialized services)

#### Kinesis Streams
- CreateStream / DeleteStream / DescribeStream / ListStreams
- PutRecord / PutRecords / GetRecords / GetShardIterator
- Shard splitting / merging
- Enhanced fan-out (basic)

#### Route 53
- CreateHostedZone / DeleteHostedZone / ListHostedZones
- ChangeResourceRecordSets / ListResourceRecordSets
- Record types: A, AAAA, CNAME, MX, TXT, NS, SOA

#### SES (Simple Email Service)
- SendEmail / SendRawEmail
- VerifyEmailIdentity / ListIdentities
- _Emails captured locally, not actually sent_

#### Redshift (stubs)
- CreateCluster / DeleteCluster / DescribeClusters
- _Returns synthetic DNS endpoint via internal DNS_
- _Metadata only, no actual query engine_

#### ElastiCache
- CreateCacheCluster / DeleteCacheCluster / DescribeCacheClusters
- CreateReplicationGroup / DeleteReplicationGroup / DescribeReplicationGroups
- _Returns synthetic DNS endpoints via internal DNS (e.g., `my-cluster.abc.us-east-1.cache.amazonaws.com`)_
- _Optionally proxy to a real Redis/Valkey instance on an allocated port from the port range_
- _Without a real backend, return metadata stubs with valid-looking endpoints_

#### OpenSearch / Elasticsearch
- CreateDomain / DeleteDomain / DescribeDomain / ListDomainNames
- _Returns synthetic DNS endpoint via internal DNS (e.g., `search-my-domain.us-east-1.es.amazonaws.com`)_
- _Optionally proxy to a real OpenSearch instance on an allocated port from the port range_
- _Without a real backend, return metadata stubs with valid-looking endpoints_

#### EC2 (basic stubs)
- DescribeInstances / RunInstances / TerminateInstances
- DescribeSecurityGroups / CreateSecurityGroup
- DescribeVpcs / DescribeSubnets
- _Metadata stubs only — no actual compute_

#### AWS Config (stubs)
- PutConfigurationRecorder / DescribeConfigurationRecorders
- PutDeliveryChannel
- _Minimal stubs for IaC compatibility_

#### ACM (Certificate Manager)
- RequestCertificate / DescribeCertificate / ListCertificates / DeleteCertificate
- _No real TLS — stub certificate metadata_

#### S3 Control (stubs)
- GetPublicAccessBlock / PutPublicAccessBlock
- _Account-level S3 settings_

#### Resource Groups / Tagging
- CreateGroup / DeleteGroup / ListGroups
- GetResources (tag-based resource queries)
- _Cross-service tag aggregation_

---

## Developer Experience Features

### Infrastructure

- [x] **Single-port routing** — All services accessible on one port (service router with 7 services) ✅
- [x] **Docker image** — Already published
- [x] **Docker Compose support** — Already supported
- [x] **CLI flags / env config** — Already implemented via Kong
- [x] **Web dashboard** — Fully refactored with responsive sidebar navigation ✅
- [x] **Dark mode UI** — Theme-aware CSS with automatic light/dark switching ✅
- [x] **Prometheus metrics** — Already implemented
- [x] **OpenTelemetry tracing** — Already implemented
- [x] **Demo data seeding** — Already implemented

### Gaps to close

- [ ] **`awslocal`-style CLI wrapper** — Thin wrapper or docs for `aws --endpoint-url`
- [ ] **Init hooks** — Run user scripts on startup to seed resources (create queues, buckets, etc.)
- [ ] **Health endpoint** — `/_gopherstack/health` reporting status of all services
- [ ] **Persistence** — Optional on-disk state persistence across restarts (LocalStack charges for this, so this would be a differentiator)
- [ ] **Testcontainers module** — Official Go Testcontainers module for Gopherstack
- [ ] **Terraform compatibility docs** — Guide for using Gopherstack with Terraform AWS provider
- [ ] **CDK compatibility docs** — Guide for using Gopherstack with AWS CDK

---

## Suggested Milestone Plan

### v0.5 — Messaging & Identity Foundation ✅ **COMPLETE**
- SQS (standard + FIFO queues) ✅
- SNS (topics, subscriptions, SQS/HTTP targets) ✅
- STS (AssumeRole, GetCallerIdentity) ✅
- IAM (full resource implementation, no enforcement) ✅
- Dashboard UI for all services ✅
- Merged main branch (DynamoDB, S3, SSM enhancements) ✅
- Fixed test performance (unit: 6.3s, integration: 39.3s, e2e: 27.5s, no hangs) ✅
- Refactored dashboard UI: navbar → sidebar, dark mode support ✅

**Remaining for v0.5:**
- KMS (encrypt/decrypt, key management)
- Secrets Manager
- Health endpoint
- SSM gaps (GetParametersByPath, DescribeParameters)

### v0.6 — Platform Infrastructure
- **Port allocator** (configurable range, central allocation/release, health checks)
- **Internal DNS server** (embedded `miekg/dns`, synthetic hostnames → 127.0.0.1)
- **Docker integration layer** (Go Docker SDK client, image pull, container lifecycle, volume mounts)
- Init hooks (run user scripts on startup)

### v0.7 — Serverless Core
- Lambda (Docker-based runtimes for Python/Node/Java, direct exec for Go)
- Lambda Runtime API implementation
- Warm container pooling and idle reaping
- API Gateway (REST, Lambda proxy integration)
- Event source mappings (SQS → Lambda, DynamoDB Streams → Lambda)
- Function URLs (via port allocator)
- S3 gaps (CopyObject, presigned URLs, lifecycle, notifications)

### v0.8 — Orchestration & Observability
- Step Functions
- EventBridge
- CloudWatch (metrics + logs)
- Persistence (optional on-disk snapshots)

### v0.9 — Infrastructure-as-Code
- CloudFormation (stacks for all implemented services)
- Terraform compatibility testing & docs
- Testcontainers module

### v0.10 — Long Tail & Networked Services
- Kinesis Streams
- Route 53
- SES
- ElastiCache (metadata stubs + optional Redis proxy via DNS/port range)
- OpenSearch (metadata stubs + optional proxy via DNS/port range)
- EC2 / Redshift stubs (with synthetic DNS endpoints)
- ACM / AWS Config / S3 Control / Resource Groups stubs

### v1.0 — Production Ready
- Full test coverage across all services
- Performance benchmarks vs LocalStack
- Migration guide from LocalStack
- Comprehensive documentation

---

## Competitive Advantages to Highlight

Gopherstack already has or can build advantages over LocalStack:

1. **No Docker required for core services** — Single Go binary for DynamoDB/S3/SQS/SNS/etc. Docker only needed for Lambda runtimes (and optional proxied services like ElastiCache/OpenSearch)
2. **Persistence for free** — LocalStack charges for persistence; Gopherstack can offer it in the base product
3. **No account/auth required** — LocalStack is dropping its open-source edition (March 2026); Gopherstack remains fully open
4. **Native Go performance** — Faster startup, lower memory footprint than LocalStack's Python runtime
5. **Built-in web dashboard** — Already ships with a resource browser
6. **Built-in observability** — Prometheus + OpenTelemetry out of the box
