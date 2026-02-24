# Gopherstack Roadmap — LocalStack Free Tier Parity

> **Goal:** Feature parity with LocalStack's free/community tier (~30 AWS services).
> **Current state:** 12 services, ~178 operations, plus platform infrastructure (DNS, port allocator, Docker, init hooks).

---

## Current Coverage (v0.5–v0.7 Complete)

### DynamoDB — ~95% parity (27 operations)

| Feature | Status |
|---------|--------|
| Table CRUD (CreateTable, DeleteTable, DescribeTable, ListTables, UpdateTable) | Done |
| Item CRUD (GetItem, PutItem, UpdateItem, DeleteItem) | Done |
| BatchGetItem / BatchWriteItem | Done |
| Query / Scan with expressions | Done |
| GSI / LSI | Done |
| Transactions (TransactWriteItems, TransactGetItems) | Done |
| DynamoDB Streams (DescribeStream, GetRecords, GetShardIterator, ListStreams) | Done |
| TTL (UpdateTimeToLive, DescribeTimeToLive, background reaper) | Done |
| Conditional writes | Done |
| Pagination | Done |
| PartiQL (ExecuteStatement, BatchExecuteStatement) | Done |
| TagResource / UntagResource / ListTagsOfResource | Done |

**Remaining gaps:**
- [ ] DescribeContinuousBackups / point-in-time recovery stubs
- [ ] Table export / import stubs

### S3 — ~90% parity (26 operations)

| Feature | Status |
|---------|--------|
| Bucket CRUD (CreateBucket, DeleteBucket, ListBuckets, HeadBucket) | Done |
| Object CRUD (PutObject, GetObject, HeadObject, DeleteObject, DeleteObjects) | Done |
| ListObjects / ListObjectsV2 | Done |
| ListObjectVersions | Done |
| CopyObject | Done |
| Multipart uploads (Create, UploadPart, Complete, Abort) | Done |
| ListMultipartUploads / ListParts | Done (v0.8) |
| Versioning (GetBucketVersioning, PutBucketVersioning) | Done |
| Object tagging (PutObjectTagging, GetObjectTagging, DeleteObjectTagging) | Done |
| Checksums (CRC32, CRC32C, SHA1, SHA256) | Done |
| Compression support | Done |
| BucketACL (GetBucketAcl, PutBucketAcl) | Done |

**Remaining gaps:**
- [ ] Presigned URLs
- [ ] Bucket lifecycle configuration (expiration rules)
- [ ] Bucket notifications (events to SQS/SNS/Lambda)
- [ ] CORS configuration
- [ ] Bucket policies
- [ ] Object lock / legal hold

### SQS — ~95% parity (17 operations) ✅

| Feature | Status |
|---------|--------|
| CreateQueue / DeleteQueue / ListQueues / GetQueueUrl | Done |
| GetQueueAttributes / SetQueueAttributes | Done |
| SendMessage / ReceiveMessage / DeleteMessage | Done |
| ChangeMessageVisibility / ChangeMessageVisibilityBatch | Done |
| SendMessageBatch / DeleteMessageBatch | Done |
| Standard + FIFO queues (.fifo, deduplication, message groups) | Done |
| Dead-letter queue (RedrivePolicy) | Done |
| Message visibility timeout | Done |
| Long polling (WaitTimeSeconds) | Done |
| PurgeQueue | Done |
| Message attributes | Done |
| TagQueue / UntagQueue / ListQueueTags | Done |
| SNS → SQS cross-service delivery | Done |
| Dashboard UI | Done |

### SNS — ~90% parity (12 operations) ✅

| Feature | Status |
|---------|--------|
| CreateTopic / DeleteTopic / ListTopics | Done |
| GetTopicAttributes / SetTopicAttributes | Done |
| Subscribe / ConfirmSubscription / Unsubscribe | Done |
| ListSubscriptions / ListSubscriptionsByTopic | Done |
| Publish / PublishBatch | Done |
| Subscription protocols: SQS, HTTP/HTTPS, Lambda, email (stub) | Done |
| Subscription filter policies | Done |
| FIFO topics | Done |
| Message attributes | Done |
| Dashboard UI | Done |

### Lambda — Image + Zip (7 operations) ✅

| Feature | Status |
|---------|--------|
| CreateFunction / GetFunction / ListFunctions / DeleteFunction | Done |
| UpdateFunctionCode / UpdateFunctionConfiguration | Done |
| Invoke (RequestResponse + Event) | Done |
| `PackageType: Image` — Docker container execution | Done |
| Lambda Runtime API (`/2018-06-01/runtime/invocation/*`) | Done |
| Per-function Runtime API server on dedicated port | Done |
| Warm container pool with idle timeout + reaper | Done |
| Environment variables passed to containers | Done |
| Graceful degradation when Docker unavailable | Done |
| `PackageType: Zip` — extract zip, bind-mount into AWS base image | Done (v0.8) |
| Runtime → base image mapping (python, nodejs, java, dotnet, ruby, provided) | Done (v0.8) |
| ZipFile inline + S3Bucket/S3Key code delivery | Done (v0.8) |

**Remaining gaps:**
- [ ] Event source mappings (SQS → Lambda, DynamoDB Streams → Lambda)
- [ ] Function URLs (via port allocator)
- [ ] Aliases and versions

### IAM — ~90% parity (25 operations) ✅

| Feature | Status |
|---------|--------|
| Users: CreateUser, DeleteUser, ListUsers, GetUser | Done |
| Roles: CreateRole, DeleteRole, ListRoles, GetRole | Done |
| Policies: CreatePolicy, DeletePolicy, ListPolicies, GetPolicy, GetPolicyVersion | Done |
| AttachUserPolicy / AttachRolePolicy / ListAttachedUserPolicies / ListAttachedRolePolicies | Done |
| Groups: CreateGroup, DeleteGroup, AddUserToGroup | Done |
| Access keys: CreateAccessKey, DeleteAccessKey, ListAccessKeys | Done |
| Instance profiles: CreateInstanceProfile, DeleteInstanceProfile, ListInstanceProfiles | Done |
| Dashboard UI | Done |
| _Policy enforcement out of scope (LocalStack free tier doesn't enforce either)_ | — |

### STS — ~95% parity (5 operations) ✅

| Feature | Status |
|---------|--------|
| AssumeRole | Done |
| GetCallerIdentity | Done |
| GetSessionToken | Done |
| DecodeAuthorizationMessage | Done |
| GetAccessKeyInfo | Done |
| Dashboard UI | Done |

**Note:** LocalStack also has AssumeRoleWithWebIdentity and GetFederationToken — these were previously listed as done but the handler shows DecodeAuthorizationMessage and GetAccessKeyInfo instead. Verify if the roadmap's prior claims were aspirational.

### KMS — ~90% parity (17 operations) ✅

| Feature | Status |
|---------|--------|
| CreateKey / DescribeKey / ListKeys | Done |
| CreateAlias / DeleteAlias / ListAliases | Done |
| Encrypt / Decrypt / GenerateDataKey / ReEncrypt | Done |
| EnableKey / DisableKey | Done |
| EnableKeyRotation / DisableKeyRotation / GetKeyRotationStatus | Done |
| ScheduleKeyDeletion / CancelKeyDeletion | Done |
| Dashboard UI | Done |

**Remaining gaps:**
- [ ] Grants (CreateGrant, ListGrants, RevokeGrant)
- [ ] GenerateDataKeyWithoutPlaintext
- [ ] Key policies

### Secrets Manager — ~90% parity (8 operations) ✅

| Feature | Status |
|---------|--------|
| CreateSecret / GetSecretValue / PutSecretValue | Done |
| DeleteSecret / RestoreSecret | Done |
| ListSecrets / DescribeSecret / UpdateSecret | Done |
| Secret versioning (AWSCURRENT, AWSPREVIOUS) | Done |
| Dashboard UI | Done |

**Remaining gaps:**
- [ ] TagResource / UntagResource / ListSecretTags
- [ ] RotateSecret (rotation Lambda integration)
- [ ] Secret replication stubs

### SSM Parameter Store — ~95% parity (8 operations) ✅

| Feature | Status |
|---------|--------|
| PutParameter / GetParameter / DeleteParameter | Done |
| GetParameters (batch) / DeleteParameters (batch) | Done |
| GetParameterHistory | Done |
| GetParametersByPath (hierarchical queries) | Done |
| DescribeParameters (filtering/pagination) | Done |
| SecureString with KMS | Done |
| Dashboard UI | Done |

**Remaining gaps:**
- [ ] Parameter tags (AddTagsToResource, ListTagsForResource)

---

## Platform Infrastructure ✅ Complete

### Internal DNS ✅
- Embedded DNS server (`pkgs/dns`) using `miekg/dns`
- Synthetic AWS-style hostnames → configurable resolve IP (default 127.0.0.1)
- UDP/TCP support, configurable port
- CLI: `--dns-addr` / `--dns-resolve-ip` (`DNS_ADDR` / `DNS_RESOLVE_IP`)

### Port Range Management ✅
- Central port allocator (`pkgs/portalloc`) with configurable range
- Thread-safe Acquire/Release with IsListening health check
- CLI: `--port-range-start` / `--port-range-end` (`PORT_RANGE_START` / `PORT_RANGE_END`)
- Used by Lambda for per-function Runtime API servers

### Docker Integration ✅
- Docker SDK client wrapper (`pkgs/docker`)
- Image pull, container lifecycle, volume mounts
- Warm container pool with configurable idle timeout + reaper
- Used by Lambda for container-based function execution (Image + Zip)
- Zip Lambda: extracts zip → bind-mounts at `/var/task` in AWS base image container
- Runtime → base image mapping: `python3.12` → `public.ecr.aws/lambda/python:3.12`, etc.

### Init Hooks ✅
- User shell script execution on startup (`pkgs/inithooks`)
- Per-script timeout, sequential execution
- CLI: `--init-script` / `INIT_SCRIPTS`

### Health Endpoint ✅
- `/_gopherstack/health` reporting all registered services

---

## Developer Experience ✅

- [x] Single-port routing — all 10 services on one port via service router
- [x] Docker image
- [x] Docker Compose support
- [x] CLI flags / env config via Kong
- [x] Web dashboard with sidebar navigation for all services
- [x] Dark mode UI with automatic theme switching
- [x] Prometheus metrics + operation tracking
- [x] OpenTelemetry tracing
- [x] Demo data seeding (`--demo`)
- [x] Init hooks for resource seeding on startup

**DX gaps remaining:**
- [ ] `awslocal`-style CLI wrapper or docs for `aws --endpoint-url`
- [ ] Persistence — on-disk state snapshots across restarts (differentiator vs LocalStack)
- [ ] Testcontainers module for Go
- [ ] Terraform compatibility docs
- [ ] CDK compatibility docs

---

## Remaining Milestones (v0.8+)

### v0.8 — Lambda Zip + Integrations & S3 Gaps ✅ (In Progress)

**Completed in this cycle:**
- ✅ Zip Lambda support (`PackageType: Zip` — extract zip, bind-mount into AWS base image container)
- ✅ Runtime → base image mapping (`python3.12` → `public.ecr.aws/lambda/python:3.12`, etc.)
- ✅ S3-based code delivery (pull zip from Gopherstack's own S3 service via S3CodeFetcher)
- ✅ Lambda Dashboard UI: function list, function detail (config, env vars, runtime), invoke button with JSON payload editor
- ✅ DynamoDB Dashboard: PartiQL tab on table detail page
- ✅ S3: ListMultipartUploads / ListParts
- ✅ S3: GetSupportedOperations updated (CopyObject, ListObjectVersions, ListMultipartUploads, ListParts)

**Remaining:**
- [ ] Event source mappings (SQS → Lambda, DynamoDB Streams → Lambda)
- [ ] Function URLs (via port allocator + DNS)
- [ ] Lambda aliases and versions
- [ ] S3: presigned URLs
- [ ] S3: Bucket lifecycle configuration, CORS, bucket policies

### v0.9 — API Gateway & Event-Driven ✅
- **API Gateway** (REST APIs) — Done (v0.9)
  - CreateRestApi / DeleteRestApi / GetRestApi / GetRestApis
  - Resources: GetResources / GetResource / CreateResource / DeleteResource
  - Methods: PutMethod / GetMethod / DeleteMethod
  - Integrations: PutIntegration / GetIntegration / DeleteIntegration (MOCK, AWS, HTTP types)
  - Deployments and stages: CreateDeployment / GetDeployments / GetStages / GetStage / DeleteStage
  - Dashboard UI: API list, resource tree, method/integration detail, deployment management
- **EventBridge** — Done (v0.9)
  - CreateEventBus / DeleteEventBus / ListEventBuses / DescribeEventBus
  - PutRule / DeleteRule / ListRules / DescribeRule / EnableRule / DisableRule
  - PutTargets / RemoveTargets / ListTargetsByRule
  - PutEvents with event log (last 1000 events)
  - Dashboard UI: event bus list, rules with targets, event log viewer

**Remaining gaps (v0.9):**
- [ ] API Gateway: Request/response mapping templates (VTL)
- [ ] EventBridge: Scheduled rules (cron/rate expressions)

### v0.10 — Orchestration & Observability ✅
- **Step Functions** ✅
  - CreateStateMachine / DeleteStateMachine / ListStateMachines / DescribeStateMachine
  - StartExecution / StopExecution / DescribeExecution / ListExecutions / GetExecutionHistory
  - Standard and Express workflows
  - Auto-succeed stub execution (no ASL interpreter)
  - Dashboard UI: state machine list, execution list per state machine, **execution detail with history events**
- **CloudWatch Metrics** ✅
  - PutMetricData / GetMetricStatistics / ListMetrics (AWS query/XML protocol)
  - PutMetricAlarm / DescribeAlarms / DeleteAlarms (basic)
  - Dashboard UI: metric namespace browser, alarm status
- **CloudWatch Logs** ✅
  - CreateLogGroup / DeleteLogGroup / DescribeLogGroups
  - CreateLogStream / DescribeLogStreams
  - PutLogEvents / GetLogEvents / FilterLogEvents
  - Dashboard UI: log group list, log stream viewer, **stream event viewer with search/filter**

**Remaining gaps (v0.10):**
- [ ] Step Functions: actual ASL state machine interpreter (Task, Choice, Wait, Parallel, Map, Pass, Succeed, Fail states)
- [ ] Step Functions: Lambda and service integrations via Task states
- [ ] CloudWatch Metrics: GetMetricData (extended query with MetricDataQuery)
- [ ] CloudWatch Logs: Lambda container stdout/stderr → CloudWatch Logs wiring

### v0.11 — Infrastructure-as-Code ✅
- **CloudFormation** ✅
  - CreateStack / DeleteStack / UpdateStack / DescribeStacks / ListStacks
  - Resource creation: AWS::S3::Bucket, AWS::DynamoDB::Table, AWS::SQS::Queue, AWS::SNS::Topic, AWS::SSM::Parameter, AWS::KMS::Key, AWS::SecretsManager::Secret
  - Outputs, parameters, intrinsic functions (Ref, Fn::Sub, Fn::Join)
  - Stack events, change sets (CreateChangeSet, DescribeChangeSet, ExecuteChangeSet, DeleteChangeSet, ListChangeSets)
  - GetTemplate
  - JSON and YAML template support
  - Dashboard UI: stack list, stack detail (resources, outputs, events)
  - _Coverage limited to resources backed by implemented services_
- Terraform compatibility testing & docs
- CDK compatibility docs
- Testcontainers module for Go

### v0.12 — Streaming & DNS-Bound Services
- **Kinesis Streams**
  - CreateStream / DeleteStream / DescribeStream / ListStreams
  - PutRecord / PutRecords / GetRecords / GetShardIterator
  - ListShards
  - Kinesis → Lambda event source mapping
  - Dashboard UI: stream list, shard viewer, put record form, record viewer
- **ElastiCache** (see [ElastiCache Design](#elasticache-design) below)
  - CreateCacheCluster / DeleteCacheCluster / DescribeCacheClusters / ListTagsForResource
  - CreateReplicationGroup / DeleteReplicationGroup / DescribeReplicationGroups
  - `Engine` field: `redis` (→ `redis:7-alpine`) or `valkey` (→ `valkey/valkey:8-alpine`)
  - Engine toggle via `ELASTICACHE_ENGINE` env var: `embedded` (default), `docker`, or `stub`
  - Dashboard UI: cluster list, cluster detail (nodes, endpoint, engine version, status), create/delete
- **OpenSearch / Elasticsearch** (same engine toggle pattern)
  - CreateDomain / DeleteDomain / DescribeDomain / ListDomainNames
  - Engine toggle via `OPENSEARCH_ENGINE` env var: `docker` or `stub`
  - Dashboard UI: domain list, domain detail (endpoint, status, engine version), create/delete

### v0.13 — Long Tail Stubs
- **Route 53**
  - CreateHostedZone / DeleteHostedZone / ListHostedZones
  - ChangeResourceRecordSets / ListResourceRecordSets
  - Wire into internal DNS (user-defined records actually resolve)
  - Dashboard UI: hosted zone list, record set editor
- **SES (Simple Email Service)**
  - SendEmail / SendRawEmail / SendBulkEmail
  - VerifyEmailIdentity / ListIdentities
  - Emails captured locally, not actually sent
  - Dashboard UI: sent email inbox viewer (from, to, subject, body, timestamp)
- **EC2 (basic stubs)**
  - DescribeInstances / RunInstances / TerminateInstances
  - DescribeSecurityGroups / CreateSecurityGroup
  - DescribeVpcs / DescribeSubnets
  - Metadata stubs only — no actual compute
  - Dashboard UI: instance list, security group list, VPC/subnet viewer
- **Redshift (stubs)**
  - CreateCluster / DeleteCluster / DescribeClusters
  - Returns synthetic DNS endpoint via internal DNS
  - Metadata only, no query engine
- **ACM (Certificate Manager)**
  - RequestCertificate / DescribeCertificate / ListCertificates / DeleteCertificate
  - Stub certificate metadata, no real TLS
- **AWS Config (stubs)**
  - PutConfigurationRecorder / DescribeConfigurationRecorders / PutDeliveryChannel
- **S3 Control (stubs)**
  - GetPublicAccessBlock / PutPublicAccessBlock
- **Resource Groups / Tagging**
  - CreateGroup / DeleteGroup / ListGroups / GetResources
  - Cross-service tag aggregation
- **SWF (Simple Workflow Service)** — basic stubs
- **Kinesis Firehose** — basic delivery stream stubs
- **S3 remaining gaps**
  - ACLs, bucket notifications, object lock / legal hold

### v1.0 — Documentation & Production Ready
- **Service documentation** (one page per service):
  - Supported operations with request/response examples
  - Known limitations vs real AWS
  - Configuration options (env vars, CLI flags)
  - Code examples (Go, Python, Node.js) using standard AWS SDKs
- **Getting started guide:**
  - Bare binary quickstart (download, run, connect)
  - Docker Compose quickstart
  - Migrating from LocalStack
- **Architecture guides:**
  - ElastiCache engine modes explained (embedded vs docker vs stub)
  - DNS setup guide (per-platform: macOS, Linux, Docker)
  - Lambda runtime guide (Image vs Zip, base image mapping)
  - Port range and resource allocation
- **Integration guides:**
  - Terraform with Gopherstack
  - AWS CDK with Gopherstack
  - Testcontainers module usage
  - CI/CD setup (GitHub Actions, GitLab CI)
- `awslocal`-style CLI wrapper
- Full test coverage across all services
- Performance benchmarks vs LocalStack
- Testcontainers module for Go

### v1.1 — Service Integration Depth
- **API Gateway** enhancements:
  - Lambda proxy integration (invoke Lambda on route hit)
- **EventBridge** enhancements:
  - Target fan-out (deliver events to Lambda/SQS/SNS targets)
  - Event pattern matching (filter events by source, detail-type, and field patterns)

---

## Missing Dashboard UI (by milestone)

Every service should have a dashboard page following the existing pattern (sidebar nav, list → detail, CRUD actions). Here's what's needed for future services:

| Milestone | Service | Dashboard UI Needed |
|-----------|---------|-------------------|
| v0.8 | DynamoDB | PartiQL tab on table detail page — SQL-style query editor with syntax highlighting, execute button, results table (backend already supports ExecuteStatement / BatchExecuteStatement) |
| v0.8 | Lambda | Function list, function detail (config, env vars, last invocation), invoke button with payload editor, invocation log |
| v0.9 | API Gateway | API list, resource tree with methods, integration detail, test endpoint button |
| v0.9 | EventBridge | Event bus list, rule list with targets, event log viewer, put event form |
| v0.10 | Step Functions | State machine list, visual execution graph (ASL → flowchart), execution history with per-state status |
| v0.10 | CloudWatch Metrics | Metric namespace browser, time-series sparkline charts, alarm status indicators |
| v0.10 | CloudWatch Logs | Log group list, log stream viewer with search/filter, live tail with auto-scroll |
| v0.12 | Kinesis | Stream list, shard viewer, put record form, record viewer |
| v0.12 | ElastiCache | Cluster list (engine badge: Redis/Valkey), node detail (endpoint, port, status), create with engine selector |
| v0.12 | OpenSearch | Domain list, domain detail (endpoint, engine version), create/delete |
| v0.13 | Route 53 | Hosted zone list, record set table with inline editing |
| v0.13 | SES | Sent email inbox (sortable table: from, to, subject, timestamp), email detail with body preview |
| v0.13 | EC2 | Instance table (ID, state, type), security group viewer, VPC/subnet tree |

---

## ElastiCache Design

### Engine Selection

Two config axes control ElastiCache behavior:

```
# Backend mode — how the cache process runs
ELASTICACHE_ENGINE=embedded|docker|stub    (default: embedded)
--elasticache-engine=embedded|docker|stub

# Cache engine — Redis or Valkey (maps to AWS Engine field)
# Determined per-cluster by the `Engine` field in CreateCacheCluster:
#   Engine=redis   → uses Redis-compatible backend
#   Engine=valkey  → uses Valkey-compatible backend (default if omitted)
```

### Mode: `embedded` (default — zero dependencies)

Uses [`alicebob/miniredis`](https://github.com/alicebob/miniredis), a pure-Go in-memory Redis/Valkey-compatible implementation. No Docker, no external process, no DNS needed.

- On `CreateCacheCluster`, Gopherstack starts a `miniredis` instance on a port from the port allocator
- The API returns `localhost:{port}` as the endpoint — your Redis/Valkey client connects directly
- Behaves like a real Redis: supports GET, SET, HSET, LPUSH, pub/sub, Lua scripting, etc.
- Both `Engine=redis` and `Engine=valkey` use the same `miniredis` backend (Valkey is wire-compatible with Redis)
- On `DeleteCacheCluster`, the miniredis instance is stopped and the port is released
- **No DNS needed** — endpoint address is `localhost`, port is from the port range
- **This is the recommended mode for local development** — nothing to install, works everywhere

```go
// Developer's code — works identically against real AWS or Gopherstack
cluster, _ := client.DescribeCacheClusters(ctx, &elasticache.DescribeCacheClustersInput{
    CacheClusterId: aws.String("my-cache"),
})
endpoint := cluster.CacheClusters[0].CacheNodes[0].Endpoint
redisClient := redis.NewClient(&redis.Options{
    Addr: fmt.Sprintf("%s:%d", *endpoint.Address, endpoint.Port),
})
```

**DNS routing:** None. Endpoint is `localhost:{port}`. No DNS resolver configuration needed.

### Mode: `docker` (higher fidelity)

Spins up a real Redis or Valkey container via the Docker SDK (same pattern as Lambda).

- On `CreateCacheCluster`, selects image based on `Engine` field:
  - `Engine=redis` → pulls `redis:7-alpine`
  - `Engine=valkey` → pulls `valkey/valkey:8-alpine`
- Starts container on an allocated port from the port range
- Returns a synthetic DNS hostname via internal DNS: `{cluster-id}.{hash}.{region}.cache.amazonaws.com`
- Warm container pool with idle reaping (same as Lambda)
- **Requires Docker + DNS configuration** (see [DNS on a Developer Laptop](#dns-on-a-developer-laptop) below)

**DNS routing:** The synthetic hostname resolves via Gopherstack's internal DNS to the configured resolve IP (default `127.0.0.1`). The port is embedded in the endpoint metadata. Your client connects to `{resolved-ip}:{port}`. DNS must be configured so your app can resolve `*.cache.amazonaws.com` — see the DNS section below for per-platform setup.

### Mode: `stub` (API-only)

Returns valid-looking API responses with synthetic endpoints and DNS hostnames, but nothing is actually listening. Useful for testing IaC templates (CloudFormation/Terraform) where you only need the API to accept the calls.

**DNS routing:** Hostnames are registered in internal DNS (they resolve to `127.0.0.1`) but no process is listening on the port. Useful for verifying that your infrastructure code correctly reads and passes endpoint values.

---

## DNS on a Developer Laptop

The internal DNS server is only needed when services return synthetic AWS-style hostnames (ElastiCache in `docker` mode, OpenSearch in `docker` mode, Redshift, etc.). **Most developers won't need it** — the `embedded` mode for ElastiCache returns `localhost:{port}` which needs no DNS at all.

For services that do use synthetic hostnames, here's how DNS resolution works in each deployment model:

### Docker Compose (simplest — recommended for DNS-bound services)

```yaml
services:
  gopherstack:
    image: gopherstack
    ports:
      - "8000:8000"
      - "10000-10100:10000-10100"   # port range for ElastiCache, Lambda, etc.
      - "10053:10053/udp"           # DNS
    environment:
      - DNS_ADDR=0.0.0.0:10053
      - DNS_RESOLVE_IP=gopherstack  # resolve to container's own IP
      - PORT_RANGE_START=10000
      - PORT_RANGE_END=10100
      - ELASTICACHE_ENGINE=docker

  your-app:
    build: .
    dns: gopherstack               # <-- your app resolves *.amazonaws.com via Gopherstack
    environment:
      - AWS_ENDPOINT_URL=http://gopherstack:8000
    depends_on:
      - gopherstack
```

All containers in the compose network use Gopherstack's DNS. Synthetic hostnames like `my-cache.abc.us-east-1.cache.amazonaws.com` resolve to the Gopherstack container IP automatically. No host-level configuration needed.

### Bare Binary on macOS

macOS supports per-domain DNS resolvers. Create a file that routes `*.amazonaws.com` to Gopherstack's DNS:

```bash
# Start Gopherstack with DNS on port 10053
gopherstack --dns-addr=127.0.0.1:10053 --elasticache-engine=docker

# Route amazonaws.com lookups to Gopherstack's DNS
sudo mkdir -p /etc/resolver
echo "nameserver 127.0.0.1" | sudo tee /etc/resolver/cache.amazonaws.com
echo "port 10053" | sudo tee -a /etc/resolver/cache.amazonaws.com
# Repeat for other domains:
# /etc/resolver/es.amazonaws.com      (OpenSearch)
# /etc/resolver/rds.amazonaws.com     (RDS)
```

Now `dig my-cache.abc.us-east-1.cache.amazonaws.com` resolves to `127.0.0.1` and your Redis client connects via the port range.

**Teardown:** `sudo rm /etc/resolver/cache.amazonaws.com`

### Bare Binary on Linux

Use `systemd-resolved` split DNS:

```bash
# Start Gopherstack with DNS on port 10053
gopherstack --dns-addr=127.0.0.1:10053 --elasticache-engine=docker

# Route amazonaws.com lookups (systemd-resolved)
resolvectl dns lo 127.0.0.1:10053
resolvectl domain lo ~cache.amazonaws.com ~es.amazonaws.com
```

Or add to `/etc/systemd/resolved.conf.d/gopherstack.conf`:
```ini
[Resolve]
DNS=127.0.0.1:10053
Domains=~cache.amazonaws.com ~es.amazonaws.com
```

### Just Skip DNS (simplest — recommended)

**Use `embedded` mode and you don't need DNS at all.** The endpoint is `localhost:{port}`. This is how most developers should use Gopherstack day-to-day:

```bash
# Bare binary — no Docker, no DNS, no config
gopherstack

# Your app connects to localhost:8000 for AWS APIs
# ElastiCache endpoints are localhost:{allocated-port}
# Everything just works
```

DNS is only needed for:
1. Docker-backed ElastiCache/OpenSearch where you want real AWS-style hostnames
2. CloudFormation templates that reference endpoint hostnames
3. Integration tests that validate endpoint format

---

## LocalStack Free Tier Parity Scorecard

| Service | LocalStack Free | Gopherstack | Status |
|---------|:-:|:-:|--------|
| S3 | Yes | Yes | ~80% — missing CopyObject, presigned URLs, lifecycle, CORS, policies |
| DynamoDB | Yes | Yes | ~95% — minor stubs missing |
| SQS | Yes | Yes | ~95% ✅ |
| SNS | Yes | Yes | ~90% ✅ |
| Lambda | Yes | Yes | Image + Zip — core invoke works ✅ |
| IAM | Yes | Yes | ~90% — no enforcement (same as LocalStack) ✅ |
| STS | Yes | Yes | ~95% ✅ |
| KMS | Yes | Yes | ~90% — missing grants ✅ |
| Secrets Manager | Yes | Yes | ~90% ✅ |
| SSM (Parameter Store) | Yes | Yes | ~95% ✅ |
| CloudFormation | Yes | Yes | Core CRUD + resource creation + change sets ✅ (v0.11) |
| CloudWatch Metrics | Yes | Yes | Core API (PutMetricData, GetMetricStatistics, ListMetrics, alarms) ✅ (v0.10) |
| CloudWatch Logs | Yes | Yes | Core API (log groups, streams, events) ✅ (v0.10) |
| API Gateway (REST) | Yes | Yes | Core CRUD + mock integrations ✅ (v0.9) |
| Step Functions | Yes | Yes | Core CRUD + stub execution ✅ (v0.10) |
| EventBridge | Yes | Yes | Core CRUD + event log ✅ (v0.9) |
| Kinesis Streams | Yes | No | v0.12 |
| Kinesis Firehose | Yes | No | v0.13 |
| Route 53 | Yes | No | v0.13 |
| SES | Yes | No | v0.13 |
| EC2 (basic) | Yes | No | v0.13 |
| ACM | Yes | No | v0.13 |
| AWS Config | Yes | No | v0.13 |
| Redshift | Yes | No | v0.13 |
| OpenSearch | Yes | No | v0.12 |
| Elasticsearch | Yes | No | v0.12 (alias of OpenSearch) |
| S3 Control | Yes | No | v0.13 |
| Resource Groups | Yes | No | v0.13 |
| SWF | Yes | No | v0.13 |
| Transcribe | Yes | No | Not planned |

**Current: 10/30 services (33%) — 6 more milestones to full parity**

---

## Competitive Advantages

1. **No Docker required for core services** — Single Go binary for DynamoDB/S3/SQS/SNS/IAM/STS/KMS/SSM/Secrets Manager. Docker only needed for Lambda and optional Docker-backed ElastiCache/OpenSearch
2. **Embedded ElastiCache** — Real Redis (via `miniredis`) running inside the Go binary. No Docker, no DNS, no external processes. `ELASTICACHE_ENGINE=embedded` is the default — just run the binary and connect
3. **Persistence for free** — LocalStack charges for persistence; Gopherstack can offer it in the base product
4. **No account/auth required** — LocalStack is dropping its open-source edition (March 2026); Gopherstack remains fully open
5. **Native Go performance** — Faster startup, lower memory footprint than LocalStack's Python runtime
6. **Built-in web dashboard** — Full resource browser for all 10 services with dark mode
7. **Built-in observability** — Prometheus metrics + OpenTelemetry tracing out of the box
8. **Progressive complexity** — Start with a bare binary (zero deps), add Docker for Lambda/Redis, add DNS only if you need AWS-style hostnames. Most devs never need Docker at all
