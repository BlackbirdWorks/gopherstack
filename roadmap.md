# Gopherstack Roadmap — LocalStack Free Tier Parity

> **Goal:** Feature parity with LocalStack's free/community tier (~30 AWS services).
> **Current state:** 18 services, ~230 operations, full platform infrastructure, dashboard UI for all services.

---

## Current Coverage (v0.5–v0.15 Complete)

### DynamoDB — ~95% parity (29 operations) ✅

Table CRUD, item CRUD, batch ops, query/scan with expressions, GSI/LSI, transactions, DynamoDB Streams, TTL with background reaper, conditional writes, pagination, PartiQL (ExecuteStatement, BatchExecuteStatement), tagging. Backup stubs: DescribeContinuousBackups, UpdateContinuousBackups, ExportTableToPointInTime, DescribeExport, ListExports. Full dashboard UI with PartiQL tab.

### S3 — ~97% parity (35 operations) ✅

Bucket CRUD, object CRUD, ListObjects/V2, ListObjectVersions, CopyObject, multipart uploads (Create, UploadPart, Complete, Abort, ListMultipartUploads, ListParts), versioning, object tagging, checksums (CRC32, CRC32C, SHA1, SHA256), compression, BucketACL. Presigned URLs (GET/PUT with expiry validation). Bucket policies (PutBucketPolicy, GetBucketPolicy, DeleteBucketPolicy). CORS configuration (PutBucketCors, GetBucketCors, DeleteBucketCors) with OPTIONS preflight. Lifecycle configuration (PutBucketLifecycleConfiguration, GetBucketLifecycleConfiguration, DeleteBucketLifecycleConfiguration). Notification configuration (PutBucketNotificationConfiguration, GetBucketNotificationConfiguration). Full dashboard UI with folder navigation, file preview, metadata/tagging.

**Remaining gaps:**
- [ ] Lifecycle rule enforcement (background expiration janitor)
- [ ] Notification event delivery (emit events to SQS/SNS/Lambda on object operations)
- [ ] Object lock / legal hold

### SQS — ~95% parity (17 operations) ✅

Full queue CRUD, send/receive/delete, batch operations, FIFO queues with deduplication and message groups, dead-letter queues, visibility timeout, long polling, purge, tagging, SNS→SQS cross-service delivery. Dashboard UI with queue browser and message viewer.

### SNS — ~90% parity (12 operations) ✅

Topic CRUD, subscribe/confirm/unsubscribe, publish/publishBatch, subscription protocols (SQS, HTTP/HTTPS, Lambda, email stub), filter policies, FIFO topics, message attributes. Dashboard UI with topic and subscription management.

### Lambda — Image + Zip + ESM (11 operations) ✅

CreateFunction, GetFunction, ListFunctions, DeleteFunction, UpdateFunctionCode, UpdateFunctionConfiguration, Invoke (RequestResponse + Event). PackageType Image (Docker) and Zip (bind-mount into AWS base image). Lambda Runtime API, warm container pool, environment variables, S3-based code delivery, graceful degradation when Docker unavailable. Event source mappings: CreateEventSourceMapping, GetEventSourceMapping, ListEventSourceMappings, DeleteEventSourceMapping — Kinesis→Lambda polling with background worker. Dashboard UI with function list, detail, and invoke button.

**Remaining gaps:**
- [ ] Function URLs (via port allocator)
- [ ] Aliases and versions
- [ ] Lambda container stdout/stderr → CloudWatch Logs wiring

### IAM — ~92% parity (24 operations) ✅

Users, roles, policies, groups, access keys, instance profiles, attach/detach policies (AttachRolePolicy, DetachRolePolicy, AttachUserPolicy). Dashboard UI. Policy enforcement out of scope (same as LocalStack free tier).

### STS — ~95% parity (5 operations) ✅

AssumeRole, GetCallerIdentity, GetSessionToken, DecodeAuthorizationMessage, GetAccessKeyInfo. Dashboard UI.

### KMS — ~95% parity (25 operations) ✅

Key CRUD, aliases, encrypt/decrypt/GenerateDataKey/GenerateDataKeyWithoutPlaintext/ReEncrypt, enable/disable, key rotation, scheduled deletion, grants (CreateGrant, ListGrants, RevokeGrant, RetireGrant, ListRetirableGrants), key policies (PutKeyPolicy, GetKeyPolicy). Dashboard UI with key detail.

### Secrets Manager — ~95% parity (11 operations) ✅

Create, get, put, delete, restore, list, describe, update. Secret versioning (AWSCURRENT, AWSPREVIOUS). TagResource, UntagResource. RotateSecret stub (creates new version). Dashboard UI with secret detail.

**Remaining gaps:**
- [ ] RotateSecret with actual Lambda invocation

### SSM Parameter Store — ~95% parity (11 operations) ✅

Put, get, delete (single + batch), GetParameterHistory, GetParametersByPath, DescribeParameters, SecureString with KMS. Parameter tags (AddTagsToResource, RemoveTagsFromResource, ListTagsForResource). Dashboard UI with history and put modal.

### API Gateway — REST APIs + Lambda Proxy (19 operations) ✅

REST API CRUD, resources, methods, integrations (MOCK, AWS, HTTP, AWS_PROXY), deployments, stages. Lambda proxy integration: `AWS_PROXY` integration type converts HTTP requests to Lambda proxy event JSON, invokes Lambda, converts response. `/proxy/{apiId}/{stageName}/{path}` routing. Dashboard UI with API list, resource tree, method/integration detail.

**Remaining gaps:**
- [ ] Request/response mapping templates (VTL)

### EventBridge — Full Event Processing (14 operations) ✅

Event bus CRUD, rules (put/delete/list/describe/enable/disable), targets (put/remove/list), PutEvents with event log (last 1000). Target fan-out delivery to Lambda/SQS/SNS targets. Event pattern matching (source, detail-type, nested detail field patterns — exact match, prefix, exists, numeric ranges, anything-but). Scheduled rules with cron/rate expression parsing and background scheduler. Dashboard UI with event bus list, rules, event log viewer. Supports both `AmazonEventBridge.*` and `AWSEvents.*` target prefixes.

### Step Functions — ASL Interpreter (9 operations) ✅

State machine CRUD, start/stop/describe/list executions, GetExecutionHistory. Standard and Express workflows. Full ASL state machine interpreter: Pass, Task (Lambda invocation), Choice (StringEquals, NumericGreaterThan, BooleanEquals, And, Or, Not), Wait, Succeed, Fail, Parallel, Map. ResultPath, InputPath, OutputPath support. Catch error handling for Task states. Dashboard UI with state machine list, execution history. Supports both `AmazonStates.*` and `AWSStepFunctions.*` target prefixes.

### CloudWatch Metrics (7 operations) ✅

PutMetricData, GetMetricStatistics, GetMetricData, ListMetrics, PutMetricAlarm, DescribeAlarms, DeleteAlarms. Supports both legacy query/XML protocol AND rpc-v2-cbor (Smithy RPCv2) protocol used by AWS SDK v2 ≥ cloudwatch@v1.55. Dashboard UI with namespace browser and alarm status.

### CloudWatch Logs (8 operations) ✅

Log group CRUD, log stream CRUD, PutLogEvents, GetLogEvents, FilterLogEvents. Dashboard UI with log group list, stream viewer, search/filter.

### CloudFormation (12 operations) ✅

Stack CRUD (create, update, delete, describe, list), stack events, change sets (create, describe, execute, delete, list), GetTemplate. Resource creation for 12 resource types (S3::Bucket, DynamoDB::Table, SQS::Queue, SNS::Topic, SSM::Parameter, KMS::Key, SecretsManager::Secret, Lambda::Function, Events::Rule, StepFunctions::StateMachine, Logs::LogGroup, ApiGateway::RestApi). Intrinsic functions (Ref, Fn::Sub, Fn::Join), JSON + YAML. Dashboard UI with stack list, detail, events.

### Kinesis Streams (10 operations) ✅

CreateStream, DeleteStream, DescribeStream, DescribeStreamSummary, ListStreams, PutRecord, PutRecords, GetShardIterator (TRIM_HORIZON, LATEST, AT_SEQUENCE_NUMBER, AFTER_SEQUENCE_NUMBER), GetRecords, ListShards. Shard management with configurable shard count, partition key hashing, sequence number ordering. Kinesis→Lambda event source mapping with background poller. Dashboard UI with stream list, shard viewer, put record form.

### ElastiCache (7 operations) ✅

CreateCacheCluster, DeleteCacheCluster, DescribeCacheClusters, ListTagsForResource, CreateReplicationGroup, DeleteReplicationGroup, DescribeReplicationGroups. Three engine modes: embedded (miniredis — zero deps), docker (real Redis/Valkey containers), stub (API-only). Redis + Valkey engine support. Dashboard UI with cluster list and cluster detail.

---

## Platform Infrastructure ✅ Complete

- **Internal DNS** — `pkgs/dns` using `miekg/dns`, synthetic AWS-style hostnames, UDP/TCP, configurable port
- **Port Range Management** — `pkgs/portalloc`, thread-safe acquire/release, configurable range
- **Docker Integration** — `pkgs/docker`, container lifecycle, warm pool with idle reaping, Lambda Image + Zip
- **Init Hooks** — `pkgs/inithooks`, user shell scripts on startup
- **Health Endpoint** — `/_gopherstack/health` with all registered services
- **Cross-Service Event Bus** — `pkgs/events`, SNS→SQS delivery, DynamoDB/S3 event emission

---

## Developer Experience ✅

- [x] Single-port routing — all 18 services on one port via priority-based service router
- [x] Docker image + Docker Compose support
- [x] CLI flags / env config via Kong
- [x] Web dashboard with sidebar navigation for all 18 services
- [x] Dark mode UI with automatic theme switching (HTMX + Flowbite + Tailwind)
- [x] Prometheus metrics + operation tracking
- [x] OpenTelemetry tracing
- [x] Demo data seeding (`--demo`)
- [x] Init hooks for resource seeding on startup
- [x] Testcontainers module for Go (`modules/gopherstack`)
- [x] Terraform compatibility docs (README)
- [x] CDK compatibility docs (README)

**DX gaps remaining:**
- [ ] `awslocal`-style CLI wrapper or docs for `aws --endpoint-url`
- [ ] Persistence — on-disk state snapshots across restarts (differentiator vs LocalStack)

---

## Test Coverage Summary

| Layer | Tests | Coverage |
|-------|-------|----------|
| Unit tests | ~150+ files | All 18 services + platform packages |
| Integration tests | 96 tests | All 18 services + cross-service (SNS→SQS, EventBridge→SQS, Kinesis→Lambda, StepFunctions ASL) |
| E2E / browser tests | 31 tests | Playwright — all 18 services have dashboard tests |

---

## Remaining Milestones

Each version is scoped to 2–3 tasks completable in a single focused session (~1 hour each). Tasks include the specific files to create/modify, operations to implement, and tests to write.

### v0.16 — S3 Deep Gaps

**Task 1: S3 lifecycle rule enforcement**
- Add background goroutine in `s3/janitor.go` to expire objects matching lifecycle rules — scan buckets with lifecycle config, delete objects past expiration days, filter by prefix
- Reuse existing janitor/reaper pattern from DynamoDB TTL
- Unit tests in `s3/handler_test.go` — set lifecycle with 0-day expiration, trigger janitor, verify objects deleted
- Integration test in `test/integration/s3_lifecycle_test.go` — set lifecycle, put objects, wait for expiration, verify objects gone

**Task 2: S3 notification event delivery**
- Wire notification config into `pkgs/events` emitter — on PutObject/DeleteObject, emit event to configured SQS/SNS/Lambda targets
- Add `S3NotificationEvent` type in `pkgs/events/types.go` with S3 event JSON envelope format (Records array with eventSource, eventName, s3 bucket/object info)
- Resolve target ARN to service call (SQS SendMessage, SNS Publish, Lambda Invoke)
- Unit tests in `s3/handler_test.go` — set notification config, put object, verify event emitted
- Integration test in `test/integration/s3_notification_test.go` — set notification with SQS target, put object, receive event from SQS

### v0.17 — S3 Object Lock + Lambda Features

**Task 1: S3 object lock / legal hold**
- Add `PutObjectLockConfiguration` / `GetObjectLockConfiguration` handlers in `s3/handler.go` — enable object lock on bucket
- Add `PutObjectRetention` / `GetObjectRetention` / `PutObjectLegalHold` / `GetObjectLegalHold` handlers
- Store retention mode (GOVERNANCE/COMPLIANCE), retain-until-date, and legal hold status per object version in `s3/backend_memory.go`
- Block DeleteObject when object is locked or under legal hold — return `AccessDenied`
- Unit tests in `s3/handler_test.go` — lock object, attempt delete (expect 403), remove hold, delete succeeds

**Task 2: Lambda function URLs**
- Add `CreateFunctionUrlConfig` / `GetFunctionUrlConfig` / `DeleteFunctionUrlConfig` handlers in `lambda/handler.go`
- Allocate a port from `pkgs/portalloc` for the function URL endpoint
- Start an HTTP listener on the allocated port that forwards requests to the Lambda invoke path (convert HTTP request → Lambda event JSON → invoke → convert response)
- Register synthetic DNS hostname via `pkgs/dns` — `{function-name}.lambda-url.{region}.on.aws`
- Return the URL in the API response (`FunctionUrl` field)
- Unit tests in `lambda/handler_test.go` — create/get/delete URL config
- Integration test in `test/integration/lambda_url_test.go` — create function, create URL, HTTP GET to URL, verify response

### v0.18 — Lambda Versions + Integration Polish

**Task 1: Lambda aliases and versions**
- Add `PublishVersion` / `GetFunctionConfiguration` (with qualifier) / `ListVersionsByFunction` handlers in `lambda/handler.go`
- Add `CreateAlias` / `GetAlias` / `ListAliases` / `UpdateAlias` / `DeleteAlias` handlers
- Store version snapshots (immutable copies of function config) and alias mappings (alias name → version number) in `lambda/backend.go`
- Support `Qualifier` parameter on `Invoke` — resolve alias → version → function config
- Unit tests in `lambda/handler_test.go` — publish version, create alias pointing to version, invoke via alias

**Task 2: Lambda → CloudWatch Logs wiring**
- Wire Lambda container stdout/stderr to CloudWatch Logs in `lambda/docker.go` — on invoke, capture container logs, create log group `/aws/lambda/{function-name}` and log stream, call PutLogEvents
- Pass CloudWatch Logs backend reference to Lambda service in `cli.go`
- Integration test in `test/integration/lambda_logs_test.go` — invoke Lambda, check CloudWatch Logs for function output

**Task 3: SecretsManager rotation with Lambda**
- Update `RotateSecret` handler in `secretsmanager/handler.go` — accept rotation Lambda ARN, invoke Lambda with rotation event JSON (Step: createSecret → setSecret → testSecret → finishSecret)
- Wire Lambda invoker into SecretsManager in `cli.go`
- Unit tests in `secretsmanager/handler_test.go` — rotate secret invokes Lambda with correct event format

### v0.19 — API Gateway VTL + Route 53

**Task 1: API Gateway request/response mapping templates**
- Add VTL template rendering in `apigateway/vtl.go` — parse Velocity Template Language strings, support `$input.json()`, `$input.path()`, `$input.body`, `$context.requestId`, `$util.escapeJavaScript()`
- Apply request mapping template before integration call, response mapping template after
- Unit tests in `apigateway/vtl_test.go` — render templates with mock input, verify JSON transformation

**Task 2: Route 53 service**
- Create `route53/` directory with standard service structure
- Create `route53/backend.go` — hosted zone store with `HostedZone` struct (ID, name, record sets), `ResourceRecordSet` struct (name, type, TTL, records)
- Create `route53/handler.go` — REST-style XML protocol (Route 53 uses `/2013-04-01/hostedzone/` path prefix)
- Implement operations: `CreateHostedZone`, `DeleteHostedZone`, `ListHostedZones`, `GetHostedZone`, `ChangeResourceRecordSets` (CREATE, DELETE, UPSERT actions), `ListResourceRecordSets`
- Wire into `pkgs/dns` — when a record set is created/updated, register it with the internal DNS server so it actually resolves
- Register service in `cli.go`, add to `internal/teststack/teststack.go`
- Unit tests in `route53/handler_test.go` — create hosted zone, add A/CNAME records, list records, delete zone
- Integration test in `test/integration/route53_test.go` — create hosted zone, add record, verify DNS resolution

### v0.20 — Route 53 Dashboard + SES Service

**Task 1: Route 53 Dashboard UI**
- Create `dashboard/templates/route53/index.html` — hosted zone list (name, record count, ID)
- Create `dashboard/templates/route53/zone_detail.html` — record set table with inline editing (name, type, TTL, value), create/delete record buttons
- Create `dashboard/route53_handlers.go` — list zones page, zone detail page, create/delete record handlers
- Add Route 53 to sidebar navigation in `dashboard/templates/layout.html`
- E2E test in `test/e2e/route53_test.go` — verify dashboard renders

**Task 2: SES service**
- Create `ses/` directory with standard service structure
- Create `ses/backend.go` — email store with `Email` struct (from, to, subject, body HTML, body text, timestamp, message ID), verified identities list
- Create `ses/handler.go` — form-encoded XML protocol (same pattern as IAM/CloudFormation)
- Implement operations: `SendEmail` (capture email to in-memory store, return message ID), `SendRawEmail`, `VerifyEmailIdentity`, `ListIdentities`, `GetIdentityVerificationAttributes` (auto-verify all identities), `DeleteIdentity`
- No actual email sending — all emails captured locally for inspection
- Register service in `cli.go`, add to `internal/teststack/teststack.go`
- Unit tests in `ses/handler_test.go` — verify identity, send email, list identities
- Integration test in `test/integration/ses_test.go` — verify identity, send email via SDK, verify email captured

### v0.21 — SES Dashboard + EC2 Service

**Task 1: SES Dashboard UI**
- Create `dashboard/templates/ses/index.html` — sent email inbox table (from, to, subject, timestamp) with email count badge, verified identities list
- Create `dashboard/templates/ses/email_detail.html` — email detail with HTML body preview, headers, raw message
- Create `dashboard/ses_handlers.go` — inbox page, email detail page, verify identity form
- Add SES to sidebar navigation in `dashboard/templates/layout.html`
- E2E test in `test/e2e/ses_test.go` — verify dashboard renders, email list shows sent emails

**Task 2: EC2 basic stubs**
- Create `ec2/` directory with standard service structure
- Create `ec2/backend.go` — instance store with `Instance` struct (ID, state, type, AMI, VPC/subnet, security groups, launch time), `SecurityGroup` struct (ID, name, VPC, rules), `VPC` struct (ID, CIDR), `Subnet` struct (ID, VPC, CIDR, AZ)
- Create `ec2/handler.go` — form-encoded XML protocol with `Action` parameter routing (same pattern as IAM)
- Implement operations: `RunInstances` (create instance metadata, assign `i-` prefixed ID, state=running), `DescribeInstances` (filter by instance ID, state), `TerminateInstances` (set state=terminated), `DescribeSecurityGroups`, `CreateSecurityGroup`, `DeleteSecurityGroup`, `DescribeVpcs`, `DescribeSubnets`, `CreateVpc`, `CreateSubnet`
- Metadata stubs only — no actual compute, no networking
- Pre-populate a default VPC and subnet on service init
- Register service in `cli.go`, add to `internal/teststack/teststack.go`
- Unit tests in `ec2/handler_test.go` — run instance, describe, terminate, security group CRUD, VPC/subnet describe

### v0.22 — EC2 Dashboard + OpenSearch

**Task 1: EC2 Dashboard UI**
- Create `dashboard/templates/ec2/index.html` — tabbed interface: instances table (ID, state badge, type, launch time), security groups table (ID, name, VPC), VPC/subnet tree view
- Create `dashboard/ec2_handlers.go` — instances page, security groups page, VPC page
- Add EC2 to sidebar navigation in `dashboard/templates/layout.html`
- E2E test in `test/e2e/ec2_test.go` — verify dashboard renders

**Task 2: OpenSearch service**
- Create `opensearch/` directory with standard service structure
- Create `opensearch/backend.go` — domain store with `Domain` struct (name, ARN, engine version, endpoint, status, cluster config)
- Create `opensearch/handler.go` — JSON REST protocol with path-based routing (`/2021-01-01/opensearch/domain/`)
- Implement operations: `CreateDomain` (store domain metadata, allocate port if docker mode, register DNS hostname), `DeleteDomain`, `DescribeDomain`, `ListDomainNames`
- Add `OPENSEARCH_ENGINE` config flag — `docker` (start OpenSearch container) or `stub` (API-only, default)
- Register service in `cli.go`, add to `internal/teststack/teststack.go`
- Unit tests in `opensearch/handler_test.go` — create domain, describe, list, delete
- Integration test in `test/integration/opensearch_test.go` — create domain, verify endpoint returned, delete domain

### v0.23 — OpenSearch Dashboard + ACM + Redshift

**Task 1: OpenSearch Dashboard UI**
- Create `dashboard/templates/opensearch/index.html` — domain list (name, engine version, endpoint, status) with create/delete buttons
- Create `dashboard/templates/opensearch/domain_detail.html` — domain detail (endpoint, engine version, cluster config, status)
- Create `dashboard/opensearch_handlers.go` — list domains page, domain detail page, create/delete handlers
- Add OpenSearch to sidebar navigation in `dashboard/templates/layout.html`
- E2E test in `test/e2e/opensearch_test.go` — verify dashboard renders

**Task 2: ACM + Redshift stubs**
- Create `acm/` — `RequestCertificate` (generate synthetic ARN, status=ISSUED), `DescribeCertificate`, `ListCertificates`, `DeleteCertificate`. Store cert metadata (domain, ARN, status, type). No real TLS.
- Create `redshift/` — `CreateCluster` (synthetic endpoint via DNS), `DeleteCluster`, `DescribeClusters`. Store cluster metadata (ID, endpoint, status, node type). No query engine.
- Unit tests, register in cli.go, add to teststack
- Dashboard page for each: list view with create/delete

### v0.24 — Long Tail Stubs

**Task 1: AWS Config + S3 Control + Resource Groups stubs**
- Create `awsconfig/` — `PutConfigurationRecorder`, `DescribeConfigurationRecorders`, `StartConfigurationRecorder`, `PutDeliveryChannel`, `DescribeDeliveryChannels`. Stub storage only.
- Create `s3control/` — `GetPublicAccessBlock`, `PutPublicAccessBlock`, `DeletePublicAccessBlock`. Store per-account public access block settings.
- Create `resourcegroups/` — `CreateGroup`, `DeleteGroup`, `ListGroups`, `GetGroup`, `GetResources`. Cross-service tag aggregation (query tags from all services).
- Unit tests, register in cli.go, add to teststack
- Dashboard page for each: list view

**Task 2: SWF + Kinesis Firehose stubs**
- Create `swf/` — `RegisterDomain`, `ListDomains`, `DeprecateDomain`, `RegisterWorkflowType`, `ListWorkflowTypes`, `StartWorkflowExecution`, `DescribeWorkflowExecution`. Minimal workflow metadata stubs.
- Create `firehose/` — `CreateDeliveryStream`, `DeleteDeliveryStream`, `DescribeDeliveryStream`, `ListDeliveryStreams`, `PutRecord`, `PutRecordBatch`. Store records in memory (no actual delivery).
- Unit tests, register in cli.go, add to teststack
- Dashboard page for each: list view

### v1.0 — Documentation & Production Ready

**Task 1: Service documentation**
- Create `docs/services/` directory with one markdown file per service (e.g., `dynamodb.md`, `s3.md`)
- Each doc: supported operations table, request/response examples using AWS CLI, known limitations vs real AWS, configuration options (env vars, CLI flags)
- Code examples in Go, Python, Node.js using standard AWS SDKs with `endpoint_url` override

**Task 2: Getting started + architecture guides**
- `docs/quickstart.md` — download binary, run `gopherstack`, connect with AWS CLI
- `docs/docker.md` — Docker Compose quickstart with all services
- `docs/migration.md` — migrating from LocalStack (endpoint config, feature comparison)
- `docs/architecture/elasticache.md` — engine modes explained
- `docs/architecture/dns.md` — DNS setup per platform (macOS, Linux, Docker)
- `docs/architecture/lambda.md` — Image vs Zip, base image mapping, Runtime API
- `docs/integration/terraform.md`, `docs/integration/cdk.md`, `docs/integration/testcontainers.md`, `docs/integration/ci-cd.md`

**Task 3: CLI wrapper + persistence + benchmarks**
- Create `cmd/awsgs` CLI wrapper — thin wrapper around `aws` CLI that sets `--endpoint-url` automatically
- Add persistence mode in `pkgs/persistence/` — serialize in-memory state to disk (JSON/gob), restore on startup, CLI flag `--persist` / `PERSIST=true`
- Create `bench/` directory with comparative benchmarks vs LocalStack — startup time, operation latency, memory usage
- Document results in `docs/benchmarks.md`

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
| S3 | Yes | Yes | ~97% ✅ |
| DynamoDB | Yes | Yes | ~95% ✅ |
| SQS | Yes | Yes | ~95% ✅ |
| SNS | Yes | Yes | ~90% ✅ |
| Lambda | Yes | Yes | Image + Zip + ESM ✅ |
| IAM | Yes | Yes | ~92% ✅ |
| STS | Yes | Yes | ~95% ✅ |
| KMS | Yes | Yes | ~95% ✅ |
| Secrets Manager | Yes | Yes | ~95% ✅ |
| SSM (Parameter Store) | Yes | Yes | ~95% ✅ |
| CloudFormation | Yes | Yes | 12 ops + 12 resource types ✅ |
| CloudWatch Metrics | Yes | Yes | 7 ops + RPCv2 CBOR ✅ |
| CloudWatch Logs | Yes | Yes | Core API ✅ |
| API Gateway (REST) | Yes | Yes | 19 ops + Lambda proxy ✅ |
| Step Functions | Yes | Yes | 9 ops + ASL interpreter ✅ |
| EventBridge | Yes | Yes | 14 ops + fan-out + patterns + scheduler ✅ |
| Kinesis Streams | Yes | Yes | 10 ops + Lambda ESM ✅ |
| ElastiCache | Yes | Yes | 7 ops + 3 engine modes ✅ |
| Route 53 | Yes | No | v0.19 |
| SES | Yes | No | v0.20 |
| EC2 (basic) | Yes | No | v0.21 |
| OpenSearch | Yes | No | v0.22 |
| Elasticsearch | Yes | No | v0.22 (alias of OpenSearch) |
| Redshift | Yes | No | v0.23 |
| ACM | Yes | No | v0.23 |
| AWS Config | Yes | No | v0.24 |
| S3 Control | Yes | No | v0.24 |
| Resource Groups | Yes | No | v0.24 |
| SWF | Yes | No | v0.24 |
| Kinesis Firehose | Yes | No | v0.24 |
| Transcribe | Yes | No | Not planned |

**Current: 18/30 services (60%) — 9 milestones to v1.0**

---

## Competitive Advantages

1. **No Docker required for core services** — Single Go binary for 18 AWS services. Docker only needed for Lambda and optional Docker-backed ElastiCache/OpenSearch
2. **Embedded ElastiCache** — Real Redis (via `miniredis`) running inside the Go binary. No Docker, no DNS, no external processes. `ELASTICACHE_ENGINE=embedded` is the default
3. **Persistence for free** — LocalStack charges for persistence; Gopherstack can offer it in the base product
4. **No account/auth required** — LocalStack is dropping its open-source edition (March 2026); Gopherstack remains fully open
5. **Native Go performance** — Faster startup, lower memory footprint than LocalStack's Python runtime
6. **Built-in web dashboard** — Full resource browser for all 18 services with dark mode, HTMX-powered interactions
7. **Built-in observability** — Prometheus metrics + OpenTelemetry tracing out of the box
8. **Progressive complexity** — Start with a bare binary (zero deps), add Docker for Lambda/Redis, add DNS only if you need AWS-style hostnames. Most devs never need Docker at all
