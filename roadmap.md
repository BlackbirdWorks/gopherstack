# Gopherstack Roadmap — LocalStack Free Tier Parity

> **Goal:** Feature parity with LocalStack's free/community tier (~30 AWS services).
> **Current state:** 16 services, ~213 operations, full platform infrastructure, dashboard UI for all services.

---

## Current Coverage (v0.5–v0.11 Complete)

### DynamoDB — ~95% parity (24 operations) ✅

Table CRUD, item CRUD, batch ops, query/scan with expressions, GSI/LSI, transactions, DynamoDB Streams, TTL with background reaper, conditional writes, pagination, PartiQL (ExecuteStatement, BatchExecuteStatement), tagging. Full dashboard UI with PartiQL tab.

**Remaining gaps:**
- [ ] DescribeContinuousBackups / point-in-time recovery stubs
- [ ] Table export / import stubs

### S3 — ~90% parity (24 operations) ✅

Bucket CRUD, object CRUD, ListObjects/V2, ListObjectVersions, CopyObject, multipart uploads (Create, UploadPart, Complete, Abort, ListMultipartUploads, ListParts), versioning, object tagging, checksums (CRC32, CRC32C, SHA1, SHA256), compression, BucketACL. Full dashboard UI with folder navigation, file preview, metadata/tagging.

**Remaining gaps:**
- [ ] Presigned URLs
- [ ] Bucket lifecycle configuration (expiration rules)
- [ ] Bucket notifications (events to SQS/SNS/Lambda)
- [ ] CORS configuration
- [ ] Bucket policies
- [ ] Object lock / legal hold

### SQS — ~95% parity (17 operations) ✅

Full queue CRUD, send/receive/delete, batch operations, FIFO queues with deduplication and message groups, dead-letter queues, visibility timeout, long polling, purge, tagging, SNS→SQS cross-service delivery. Dashboard UI with queue browser and message viewer.

### SNS — ~90% parity (12 operations) ✅

Topic CRUD, subscribe/confirm/unsubscribe, publish/publishBatch, subscription protocols (SQS, HTTP/HTTPS, Lambda, email stub), filter policies, FIFO topics, message attributes. Dashboard UI with topic and subscription management.

### Lambda — Image + Zip (7 operations) ✅

CreateFunction, GetFunction, ListFunctions, DeleteFunction, UpdateFunctionCode, UpdateFunctionConfiguration, Invoke (RequestResponse + Event). PackageType Image (Docker) and Zip (bind-mount into AWS base image). Lambda Runtime API, warm container pool, environment variables, S3-based code delivery, graceful degradation when Docker unavailable. Dashboard UI with function list, detail, and invoke button.

**Remaining gaps:**
- [ ] Event source mappings (SQS → Lambda, DynamoDB Streams → Lambda)
- [ ] Function URLs (via port allocator)
- [ ] Aliases and versions

### IAM — ~90% parity (23 operations) ✅

Users, roles, policies, groups, access keys, instance profiles, attach/detach policies. Dashboard UI. Policy enforcement out of scope (same as LocalStack free tier).

### STS — ~95% parity (5 operations) ✅

AssumeRole, GetCallerIdentity, GetSessionToken, DecodeAuthorizationMessage, GetAccessKeyInfo. Dashboard UI.

### KMS — ~90% parity (17 operations) ✅

Key CRUD, aliases, encrypt/decrypt/GenerateDataKey/ReEncrypt, enable/disable, key rotation, scheduled deletion. Dashboard UI with key detail.

**Remaining gaps:**
- [ ] Grants (CreateGrant, ListGrants, RevokeGrant)
- [ ] GenerateDataKeyWithoutPlaintext
- [ ] Key policies

### Secrets Manager — ~90% parity (8 operations) ✅

Create, get, put, delete, restore, list, describe, update. Secret versioning (AWSCURRENT, AWSPREVIOUS). Dashboard UI with secret detail.

**Remaining gaps:**
- [ ] TagResource / UntagResource
- [ ] RotateSecret (rotation Lambda integration)

### SSM Parameter Store — ~95% parity (8 operations) ✅

Put, get, delete (single + batch), GetParameterHistory, GetParametersByPath, DescribeParameters, SecureString with KMS. Dashboard UI with history and put modal.

**Remaining gaps:**
- [ ] Parameter tags (AddTagsToResource, ListTagsForResource)

### API Gateway — REST APIs (19 operations) ✅

REST API CRUD, resources, methods, integrations (MOCK, AWS, HTTP), deployments, stages. Dashboard UI with API list, resource tree, method/integration detail.

**Remaining gaps:**
- [ ] Lambda proxy integration (invoke Lambda on route hit)
- [ ] Request/response mapping templates (VTL)

### EventBridge (14 operations) ✅

Event bus CRUD, rules (put/delete/list/describe/enable/disable), targets (put/remove/list), PutEvents with event log (last 1000). Dashboard UI with event bus list, rules, event log viewer.

**Remaining gaps:**
- [ ] Target fan-out (deliver events to Lambda/SQS/SNS targets)
- [ ] Event pattern matching (filter by source, detail-type, field patterns)
- [ ] Scheduled rules (cron/rate expressions)

### Step Functions (9 operations) ✅

State machine CRUD, start/stop/describe/list executions, GetExecutionHistory. Standard and Express workflows, auto-succeed stub execution. Dashboard UI with state machine list, execution history.

**Remaining gaps:**
- [ ] ASL state machine interpreter (Task, Choice, Wait, Parallel, Map, Pass, Succeed, Fail)
- [ ] Lambda and service integrations via Task states

### CloudWatch Metrics (6 operations) ✅

PutMetricData, GetMetricStatistics, ListMetrics, PutMetricAlarm, DescribeAlarms, DeleteAlarms. AWS query/XML protocol. Dashboard UI with namespace browser and alarm status.

**Remaining gaps:**
- [ ] GetMetricData (MetricDataQuery)

### CloudWatch Logs (8 operations) ✅

Log group CRUD, log stream CRUD, PutLogEvents, GetLogEvents, FilterLogEvents. Dashboard UI with log group list, stream viewer, search/filter.

**Remaining gaps:**
- [ ] Lambda container stdout/stderr → CloudWatch Logs wiring

### CloudFormation (12 operations) ✅

Stack CRUD (create, update, delete, describe, list), stack events, change sets (create, describe, execute, delete, list), GetTemplate. Resource creation for 7 resource types (S3::Bucket, DynamoDB::Table, SQS::Queue, SNS::Topic, SSM::Parameter, KMS::Key, SecretsManager::Secret). Intrinsic functions (Ref, Fn::Sub, Fn::Join), JSON + YAML. Dashboard UI with stack list, detail, events.

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

- [x] Single-port routing — all 16 services on one port via priority-based service router
- [x] Docker image + Docker Compose support
- [x] CLI flags / env config via Kong
- [x] Web dashboard with sidebar navigation for all 16 services
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
| Unit tests | ~150+ files | All 16 services + platform packages |
| Integration tests | ~48 tests | DynamoDB (24), S3 (6), Lambda (2), SNS, SQS, STS, KMS, SecretsManager, SSM, Terraform |
| E2E / browser tests | ~37 tests | Playwright — all 16 services have dashboard tests |

**Integration test gaps** (unit-tested but no SDK-level integration tests yet):
- [ ] IAM — role assumption, policy attachment
- [ ] EventBridge — event routing, rule execution
- [ ] CloudWatch Metrics — metric publishing, alarm evaluation
- [ ] CloudWatch Logs — log stream operations
- [ ] API Gateway — API creation, method configuration
- [ ] Step Functions — state machine execution
- [ ] CloudFormation — stack operations beyond Terraform test

---

## Remaining Milestones

Each task below is scoped to be completable in a single focused session (~1 hour). Tasks include the specific files to create/modify, operations to implement, and tests to write.

### v0.12 — Existing Service Consolidation

Close gaps in the 16 implemented services before adding new ones.

**Task 1: S3 presigned URLs** ✅
- Add `GeneratePresignedUrl` support to `s3/handler.go` — accept `X-Amz-Expires` query param on GET/PUT, return signed URL with HMAC token
- Add presigned URL verification middleware in `s3/handler.go` — validate token on incoming requests with `X-Amz-Signature` query param
- Unit tests in `s3/handler_test.go` — generate URL, fetch object via presigned URL, expired URL returns 403
- Integration test in `test/integration/s3_presigned_test.go` — use AWS SDK `PresignClient` to generate and consume URLs

**Task 2: S3 bucket policies** ✅
- Add `PutBucketPolicy` / `GetBucketPolicy` / `DeleteBucketPolicy` handlers in `s3/handler.go` — store JSON policy document per bucket
- Add policy field to bucket model in `s3/backend_memory.go`
- Update `s3/handler.go` `GetSupportedOperations()` to include new ops
- Unit tests in `s3/handler_test.go` — put/get/delete policy, policy persists across requests
- No enforcement needed (same as LocalStack) — just store and return the document

**Task 3: S3 CORS configuration** ✅
- Add `PutBucketCors` / `GetBucketCors` / `DeleteBucketCors` handlers in `s3/handler.go` — parse XML CORS rules, store per bucket
- Add CORS preflight handling — respond to OPTIONS requests with configured CORS headers
- Add CORS fields to bucket model in `s3/backend_memory.go`
- Unit tests in `s3/handler_test.go` — set CORS, verify OPTIONS response headers, delete CORS

**Task 4: S3 bucket lifecycle configuration**
- Add `PutBucketLifecycleConfiguration` / `GetBucketLifecycleConfiguration` / `DeleteBucketLifecycleConfiguration` handlers in `s3/handler.go`
- Store lifecycle rules per bucket (expiration days, prefix filters, status enabled/disabled)
- Add background goroutine in `s3/janitor.go` to expire objects matching lifecycle rules (reuse existing janitor pattern)
- Unit tests in `s3/handler_test.go` — set lifecycle, verify expiration after TTL

**Task 5: S3 bucket notifications**
- Add `PutBucketNotificationConfiguration` / `GetBucketNotificationConfiguration` handlers in `s3/handler.go`
- Store notification config per bucket (target ARN: SQS queue, SNS topic, or Lambda function ARN; event types: `s3:ObjectCreated:*`, `s3:ObjectRemoved:*`)
- Wire into existing `pkgs/events` emitter — on PutObject/DeleteObject, emit event to configured targets
- Add `S3NotificationEvent` type in `pkgs/events/types.go` with S3 event JSON envelope format
- Unit tests in `s3/handler_test.go` — set notification config, verify event emission on object operations

**Task 6: S3 object lock / legal hold**
- Add `PutObjectLockConfiguration` / `GetObjectLockConfiguration` handlers in `s3/handler.go` — enable object lock on bucket
- Add `PutObjectRetention` / `GetObjectRetention` / `PutObjectLegalHold` / `GetObjectLegalHold` handlers
- Store retention mode (GOVERNANCE/COMPLIANCE), retain-until-date, and legal hold status per object version in `s3/backend_memory.go`
- Block DeleteObject when object is locked or under legal hold — return `AccessDenied`
- Unit tests in `s3/handler_test.go` — lock object, attempt delete (expect 403), remove hold, delete succeeds

**Task 7: Lambda event source mappings**
- Add `CreateEventSourceMapping` / `DeleteEventSourceMapping` / `ListEventSourceMappings` / `GetEventSourceMapping` / `UpdateEventSourceMapping` handlers in `lambda/handler.go`
- Add `EventSourceMapping` model in `lambda/models.go` — UUID, function ARN, event source ARN (SQS queue ARN or DynamoDB stream ARN), batch size, enabled flag
- Add polling goroutine in `lambda/event_source_poller.go` — for SQS mappings: call SQS ReceiveMessage, invoke Lambda with SQS event JSON, delete messages on success; for DynamoDB Streams mappings: call GetRecords, invoke Lambda with DynamoDB event JSON
- Wire poller startup in `cli.go` — start after both Lambda and SQS/DynamoDB services are initialized
- Unit tests in `lambda/handler_test.go` — CRUD operations for mappings
- Integration test in `test/integration/lambda_event_source_test.go` — create SQS queue, create Lambda function, create mapping, send message to SQS, verify Lambda was invoked

**Task 8: Lambda function URLs**
- Add `CreateFunctionUrlConfig` / `GetFunctionUrlConfig` / `DeleteFunctionUrlConfig` handlers in `lambda/handler.go`
- Allocate a port from `pkgs/portalloc` for the function URL endpoint
- Start an HTTP listener on the allocated port that forwards requests to the Lambda invoke path (convert HTTP request → Lambda event JSON → invoke → convert response)
- Register synthetic DNS hostname via `pkgs/dns` — `{function-name}.lambda-url.{region}.on.aws`
- Return the URL in the API response (`FunctionUrl` field)
- Unit tests in `lambda/handler_test.go` — create/get/delete URL config
- Integration test in `test/integration/lambda_url_test.go` — create function, create URL, HTTP GET to URL, verify response

**Task 9: Lambda aliases and versions**
- Add `PublishVersion` / `GetFunctionConfiguration` (with qualifier) / `ListVersionsByFunction` handlers in `lambda/handler.go`
- Add `CreateAlias` / `GetAlias` / `ListAliases` / `UpdateAlias` / `DeleteAlias` handlers
- Store version snapshots (immutable copies of function config) and alias mappings (alias name → version number) in `lambda/backend.go`
- Support `Qualifier` parameter on `Invoke` — resolve alias → version → function config
- Unit tests in `lambda/handler_test.go` — publish version, create alias pointing to version, invoke via alias

**Task 10: KMS grants + key policies** ✅
- Add `CreateGrant` / `ListGrants` / `RevokeGrant` / `RetireGrant` / `ListRetirableGrants` handlers in `kms/handler.go`
- Add `GenerateDataKeyWithoutPlaintext` handler — same as GenerateDataKey but omit plaintext from response
- Add `PutKeyPolicy` / `GetKeyPolicy` handlers — store JSON policy document per key
- Store grants in `kms/backend.go` — grant ID, grantee principal, operations list, constraints
- Unit tests in `kms/handler_test.go` — create grant, list grants, revoke grant, generate data key without plaintext, put/get key policy

**Task 11: Secrets Manager tags + rotation stub** ✅
- Add `TagResource` / `UntagResource` / `ListSecretTags` handlers in `secretsmanager/handler.go` (reuse the tag map pattern from DynamoDB)
- Add `RotateSecret` handler — accept rotation Lambda ARN, create new version with `AWSPENDING` staging label, invoke the rotation Lambda if configured, move labels on completion
- Store tags per secret in `secretsmanager/backend.go`
- Unit tests in `secretsmanager/handler_test.go` — tag/untag/list tags, rotate secret creates new version

**Task 12: SSM parameter tags + DynamoDB backup stubs** ✅
- Add `AddTagsToResource` / `RemoveTagsFromResource` / `ListTagsForResource` handlers in `ssm/handler.go`
- Store tags per parameter in `ssm/backend.go`
- Add `DescribeContinuousBackups` / `UpdateContinuousBackups` stubs in `dynamodb/handler.go` — return synthetic backup description with PointInTimeRecovery status
- Add `ExportTableToPointInTime` / `DescribeExport` / `ListExports` stubs in `dynamodb/handler.go` — return synthetic export metadata
- Unit tests in `ssm/handler_test.go` — add/remove/list tags
- Unit tests in `dynamodb/handler_test.go` — describe backups returns valid response

**Task 13: CloudWatch GetMetricData + Lambda→CloudWatch Logs wiring**
- Add `GetMetricData` handler in `cloudwatch/handler.go` — accept `MetricDataQuery` array, resolve each query against stored metrics, return `MetricDataResult` array with timestamps and values
- Wire Lambda container stdout/stderr to CloudWatch Logs in `lambda/docker.go` — on invoke, capture container logs, create log group `/aws/lambda/{function-name}` and log stream, call PutLogEvents
- Pass CloudWatch Logs backend reference to Lambda service in `cli.go`
- Unit tests in `cloudwatch/handler_test.go` — put metric data, query via GetMetricData, verify results
- Integration test in `test/integration/lambda_logs_test.go` — invoke Lambda, check CloudWatch Logs for function output

**Task 14: CloudFormation resources for newer services**
- Add resource handlers in `cloudformation/resources.go` for:
  - `AWS::Lambda::Function` — call Lambda CreateFunction with properties from template
  - `AWS::Events::Rule` — call EventBridge PutRule + PutTargets
  - `AWS::StepFunctions::StateMachine` — call StepFunctions CreateStateMachine
  - `AWS::Logs::LogGroup` — call CloudWatch Logs CreateLogGroup
  - `AWS::ApiGateway::RestApi` — call API Gateway CreateRestApi
- Each resource needs: create handler, delete handler, physical resource ID generation
- Unit tests in `cloudformation/resources_test.go` — create stack with each new resource type, verify resource exists, delete stack, verify cleanup

**Task 15: Integration tests for under-tested services**
- `test/integration/iam_test.go` — create user, create role, create policy, attach policy to role, create access key, list attached policies, delete chain
- `test/integration/eventbridge_test.go` — create event bus, put rule, put targets, put events, list rules, describe rule, delete chain
- `test/integration/cloudwatch_test.go` — put metric data, get metric statistics, put alarm, describe alarms, delete alarm
- `test/integration/cloudwatchlogs_test.go` — create log group, create log stream, put log events, get log events, filter log events, delete chain
- `test/integration/apigateway_test.go` — create REST API, create resource, put method, put integration, create deployment, get stages, delete chain
- `test/integration/stepfunctions_test.go` — create state machine, start execution, describe execution, get execution history, stop execution, delete chain
- `test/integration/cloudformation_test.go` — create stack with S3+DynamoDB+SQS resources, describe stack, list stack events, delete stack, verify resources cleaned up
- `test/integration/sns_sqs_cross_test.go` — create SNS topic, create SQS queue, subscribe queue to topic, publish message, receive from queue, verify message envelope

### v0.13 — Service Integration Depth

**Task 1: API Gateway → Lambda proxy integration**
- Add `AWS_PROXY` integration type handling in `apigateway/handler.go` — when a deployment is created with `AWS_PROXY` integration, register an HTTP route on the API Gateway's stage URL
- On incoming request to stage URL: convert HTTP request to API Gateway Lambda proxy event JSON (method, path, headers, query params, body), invoke Lambda function, convert Lambda response JSON to HTTP response (status code, headers, body)
- Add route listener setup in `apigateway/proxy.go` — allocate port from portalloc for each deployment stage, start HTTP server
- Wire Lambda service reference into API Gateway in `cli.go`
- Unit tests in `apigateway/handler_test.go` — create API with Lambda proxy, verify proxy event format
- Integration test in `test/integration/apigateway_lambda_test.go` — create Lambda function, create REST API with Lambda proxy integration, deploy, HTTP request to stage URL, verify response

**Task 2: EventBridge target fan-out**
- Add target delivery engine in `eventbridge/delivery.go` — when PutEvents is called, evaluate each rule's event pattern against incoming events, for matching rules deliver to each target
- Support target types: Lambda (invoke function), SQS (send message), SNS (publish message) — resolve target ARN to service call
- Wire Lambda, SQS, SNS service references into EventBridge in `cli.go`
- Add event pattern matching in `eventbridge/pattern.go` — match by `source`, `detail-type`, and nested `detail` field patterns (exact match, prefix, numeric ranges)
- Unit tests in `eventbridge/pattern_test.go` — test pattern matching for exact match, prefix, exists, numeric comparison
- Unit tests in `eventbridge/delivery_test.go` — mock targets, verify delivery on pattern match
- Integration test in `test/integration/eventbridge_fanout_test.go` — create event bus, create rule with SQS target, put event, receive from SQS queue

**Task 3: EventBridge scheduled rules**
- Add cron/rate expression parser in `eventbridge/schedule.go` — parse `rate(5 minutes)` and `cron(0 12 * * ? *)` expressions
- Add scheduler goroutine in `eventbridge/scheduler.go` — evaluate enabled rules with schedule expressions, fire PutEvents on schedule
- Start scheduler in `cli.go` with configurable tick interval
- Unit tests in `eventbridge/schedule_test.go` — parse rate expressions, parse cron expressions, next fire time calculation

**Task 4: Step Functions ASL interpreter — state types**
- Add ASL parser in `stepfunctions/asl/parser.go` — parse JSON state machine definition into typed state structs
- Implement state executors in `stepfunctions/asl/executor.go`:
  - `Pass` — pass input to output with optional `Result` and `ResultPath`
  - `Succeed` / `Fail` — terminal states
  - `Wait` — sleep for `Seconds` or until `Timestamp`
  - `Choice` — evaluate choice rules (StringEquals, NumericGreaterThan, BooleanEquals, And, Or, Not) and branch
  - `Task` — placeholder for service integrations (next task)
  - `Parallel` / `Map` — execute branches concurrently, collect results
- Replace auto-succeed stub in `stepfunctions/backend.go` — run ASL interpreter on StartExecution, record state transition history events
- Unit tests in `stepfunctions/asl/parser_test.go` — parse sample state machines
- Unit tests in `stepfunctions/asl/executor_test.go` — execute Pass, Choice, Wait, Succeed, Fail, Parallel state machines with expected outputs

**Task 5: Step Functions → Lambda Task integration**
- Add Lambda task handler in `stepfunctions/asl/task_lambda.go` — when Task state has `Resource` matching `arn:aws:lambda:*`, invoke Lambda function with state input as payload, use response as state output
- Wire Lambda service reference into Step Functions in `cli.go`
- Support `ResultPath` and `OutputPath` for result placement
- Support `Retry` and `Catch` error handling — retry on Lambda errors with backoff, catch and transition to fallback state
- Integration test in `test/integration/stepfunctions_lambda_test.go` — create Lambda function, create state machine with Task state calling Lambda, start execution, verify execution completed with Lambda output

### v0.14 — Kinesis

**Task 1: Kinesis Streams service — backend + handler**
- Create `kinesis/` directory with standard service structure following existing patterns (e.g., `sqs/`)
- Create `kinesis/backend.go` — in-memory stream store with shard management: `Stream` struct (name, ARN, status, shards), `Shard` struct (ID, hash key range, records buffer, sequence numbers)
- Create `kinesis/handler.go` — register as Registerable service with `X-Amz-Target: Kinesis_20131202.*` header matching
- Implement operations: `CreateStream` (create stream with configurable shard count, default 1), `DeleteStream`, `DescribeStream` / `DescribeStreamSummary`, `ListStreams`
- Implement data operations: `PutRecord` (assign sequence number, route to shard by partition key hash), `PutRecords` (batch put), `GetShardIterator` (TRIM_HORIZON, LATEST, AT_SEQUENCE_NUMBER, AFTER_SEQUENCE_NUMBER), `GetRecords` (return records after iterator position, advance iterator), `ListShards`
- Register service in `cli.go` with appropriate priority
- Add to `internal/teststack/teststack.go`
- Unit tests in `kinesis/handler_test.go` — CRUD streams, put/get records, shard iterator types, sequence number ordering

**Task 2: Kinesis Dashboard UI**
- Create `dashboard/templates/kinesis/index.html` — stream list table (name, shard count, status, ARN) with create/delete buttons
- Create `dashboard/templates/kinesis/stream_detail.html` — shard list, put record form (partition key + data input), record viewer showing latest records per shard
- Create `dashboard/kinesis_handlers.go` — list streams page, stream detail page, put record form handler, get records handler
- Add Kinesis to sidebar navigation in `dashboard/templates/layout.html` under Integration Services
- Wire Kinesis backend into dashboard in `dashboard/ui.go`
- E2E test in `test/e2e/kinesis_test.go` — verify dashboard renders, stream list shows created streams

**Task 3: Kinesis integration tests + Lambda event source**
- Integration test in `test/integration/kinesis_test.go` — create stream, put records, get shard iterator, get records, verify data integrity, list shards, delete stream
- Add Kinesis → Lambda event source mapping support in `lambda/event_source_poller.go` — poll GetRecords for Kinesis stream, invoke Lambda with Kinesis event JSON format
- Integration test in `test/integration/kinesis_lambda_test.go` — create Kinesis stream, create Lambda function, create event source mapping, put record to stream, verify Lambda invoked

### v0.15 — ElastiCache

See [ElastiCache Design](#elasticache-design) below.

**Task 1: ElastiCache service — embedded mode (miniredis)**
- Create `elasticache/` directory with standard service structure
- Create `elasticache/backend.go` — cluster store with `Cluster` struct (ID, engine, status, endpoint, port, node type, miniredis instance pointer)
- Create `elasticache/handler.go` — register as Registerable service, form-encoded XML protocol (same pattern as CloudFormation/IAM)
- Implement operations: `CreateCacheCluster` (start miniredis instance on allocated port, return `localhost:{port}` endpoint), `DeleteCacheCluster` (stop miniredis, release port), `DescribeCacheClusters`, `ListTagsForResource`
- Add `go get github.com/alicebob/miniredis/v2` dependency
- Add `ELASTICACHE_ENGINE` config flag in `cli.go` — `embedded` (default), `docker`, `stub`
- Register service in `cli.go`, add to `internal/teststack/teststack.go`
- Unit tests in `elasticache/handler_test.go` — create cluster, describe, verify endpoint, delete cluster
- Integration test in `test/integration/elasticache_test.go` — create cluster, connect with Redis client to endpoint, SET/GET a key, delete cluster

**Task 2: ElastiCache Docker mode + replication groups**
- Add Docker mode in `elasticache/backend.go` — on CreateCacheCluster with `ELASTICACHE_ENGINE=docker`: pull `redis:7-alpine` (or `valkey/valkey:8-alpine` for `Engine=valkey`), start container on allocated port, register synthetic DNS hostname via `pkgs/dns`
- Add `CreateReplicationGroup` / `DeleteReplicationGroup` / `DescribeReplicationGroups` handlers
- Add stub mode — return valid API responses with synthetic endpoints but no process listening
- Unit tests in `elasticache/handler_test.go` — replication group CRUD, engine selection logic

**Task 3: ElastiCache Dashboard UI**
- Create `dashboard/templates/elasticache/index.html` — cluster list table (ID, engine badge Redis/Valkey, status, endpoint, node type) with create/delete buttons
- Create `dashboard/templates/elasticache/cluster_detail.html` — cluster detail (nodes list with endpoint and port, engine version, status, configuration endpoint)
- Create `dashboard/elasticache_handlers.go` — list clusters page, cluster detail page, create/delete handlers
- Add ElastiCache to sidebar navigation in `dashboard/templates/layout.html`
- Wire backend into dashboard in `dashboard/ui.go`
- E2E test in `test/e2e/elasticache_test.go` — verify dashboard renders, cluster list shows created clusters

### v0.16 — Route 53 & SES

**Task 1: Route 53 service**
- Create `route53/` directory with standard service structure
- Create `route53/backend.go` — hosted zone store with `HostedZone` struct (ID, name, record sets), `ResourceRecordSet` struct (name, type, TTL, records)
- Create `route53/handler.go` — REST-style XML protocol (Route 53 uses `/2013-04-01/hostedzone/` path prefix)
- Implement operations: `CreateHostedZone`, `DeleteHostedZone`, `ListHostedZones`, `GetHostedZone`, `ChangeResourceRecordSets` (CREATE, DELETE, UPSERT actions), `ListResourceRecordSets`
- Wire into `pkgs/dns` — when a record set is created/updated, register it with the internal DNS server so it actually resolves
- Register service in `cli.go`, add to `internal/teststack/teststack.go`
- Unit tests in `route53/handler_test.go` — create hosted zone, add A/CNAME records, list records, delete zone
- Integration test in `test/integration/route53_test.go` — create hosted zone, add record, verify DNS resolution via dig/lookup

**Task 2: Route 53 Dashboard UI**
- Create `dashboard/templates/route53/index.html` — hosted zone list (name, record count, ID)
- Create `dashboard/templates/route53/zone_detail.html` — record set table with inline editing (name, type, TTL, value), create/delete record buttons
- Create `dashboard/route53_handlers.go` — list zones page, zone detail page, create/delete record handlers
- Add Route 53 to sidebar navigation in `dashboard/templates/layout.html`
- E2E test in `test/e2e/route53_test.go` — verify dashboard renders

**Task 3: SES service**
- Create `ses/` directory with standard service structure
- Create `ses/backend.go` — email store with `Email` struct (from, to, subject, body HTML, body text, timestamp, message ID), verified identities list
- Create `ses/handler.go` — form-encoded XML protocol (same pattern as IAM/CloudFormation)
- Implement operations: `SendEmail` (capture email to in-memory store, return message ID), `SendRawEmail`, `VerifyEmailIdentity`, `ListIdentities`, `GetIdentityVerificationAttributes` (auto-verify all identities), `DeleteIdentity`
- No actual email sending — all emails captured locally for inspection
- Register service in `cli.go`, add to `internal/teststack/teststack.go`
- Unit tests in `ses/handler_test.go` — verify identity, send email, list identities
- Integration test in `test/integration/ses_test.go` — verify identity, send email via SDK, verify email captured

**Task 4: SES Dashboard UI**
- Create `dashboard/templates/ses/index.html` — sent email inbox table (from, to, subject, timestamp) with email count badge, verified identities list
- Create `dashboard/templates/ses/email_detail.html` — email detail with HTML body preview, headers, raw message
- Create `dashboard/ses_handlers.go` — inbox page, email detail page, verify identity form
- Add SES to sidebar navigation in `dashboard/templates/layout.html`
- E2E test in `test/e2e/ses_test.go` — verify dashboard renders, email list shows sent emails

### v0.17 — EC2 & OpenSearch

**Task 1: EC2 basic stubs**
- Create `ec2/` directory with standard service structure
- Create `ec2/backend.go` — instance store with `Instance` struct (ID, state, type, AMI, VPC/subnet, security groups, launch time), `SecurityGroup` struct (ID, name, VPC, rules), `VPC` struct (ID, CIDR), `Subnet` struct (ID, VPC, CIDR, AZ)
- Create `ec2/handler.go` — form-encoded XML protocol with `Action` parameter routing (same pattern as IAM)
- Implement operations: `RunInstances` (create instance metadata, assign `i-` prefixed ID, state=running), `DescribeInstances` (filter by instance ID, state), `TerminateInstances` (set state=terminated), `DescribeSecurityGroups`, `CreateSecurityGroup`, `DeleteSecurityGroup`, `DescribeVpcs`, `DescribeSubnets`, `CreateVpc`, `CreateSubnet`
- Metadata stubs only — no actual compute, no networking
- Pre-populate a default VPC and subnet on service init
- Register service in `cli.go`, add to `internal/teststack/teststack.go`
- Unit tests in `ec2/handler_test.go` — run instance, describe, terminate, security group CRUD, VPC/subnet describe

**Task 2: EC2 Dashboard UI**
- Create `dashboard/templates/ec2/index.html` — tabbed interface: instances table (ID, state badge, type, launch time), security groups table (ID, name, VPC), VPC/subnet tree view
- Create `dashboard/ec2_handlers.go` — instances page, security groups page, VPC page
- Add EC2 to sidebar navigation in `dashboard/templates/layout.html`
- E2E test in `test/e2e/ec2_test.go` — verify dashboard renders

**Task 3: OpenSearch service**
- Create `opensearch/` directory with standard service structure
- Create `opensearch/backend.go` — domain store with `Domain` struct (name, ARN, engine version, endpoint, status, cluster config)
- Create `opensearch/handler.go` — JSON REST protocol with path-based routing (`/2021-01-01/opensearch/domain/`)
- Implement operations: `CreateDomain` (store domain metadata, allocate port if docker mode, register DNS hostname), `DeleteDomain`, `DescribeDomain`, `ListDomainNames`
- Add `OPENSEARCH_ENGINE` config flag — `docker` (start OpenSearch container) or `stub` (API-only, default)
- Register service in `cli.go`, add to `internal/teststack/teststack.go`
- Unit tests in `opensearch/handler_test.go` — create domain, describe, list, delete
- Integration test in `test/integration/opensearch_test.go` — create domain, verify endpoint returned, delete domain

**Task 4: OpenSearch Dashboard UI**
- Create `dashboard/templates/opensearch/index.html` — domain list (name, engine version, endpoint, status) with create/delete buttons
- Create `dashboard/templates/opensearch/domain_detail.html` — domain detail (endpoint, engine version, cluster config, status)
- Create `dashboard/opensearch_handlers.go` — list domains page, domain detail page, create/delete handlers
- Add OpenSearch to sidebar navigation in `dashboard/templates/layout.html`
- E2E test in `test/e2e/opensearch_test.go` — verify dashboard renders

### v0.18 — Long Tail Stubs

Each stub service follows the minimal pattern: directory, backend, handler, register in cli.go, unit test, dashboard page.

**Task 1: ACM + Redshift stubs**
- Create `acm/` — `RequestCertificate` (generate synthetic ARN, status=ISSUED), `DescribeCertificate`, `ListCertificates`, `DeleteCertificate`. Store cert metadata (domain, ARN, status, type). No real TLS.
- Create `redshift/` — `CreateCluster` (synthetic endpoint via DNS), `DeleteCluster`, `DescribeClusters`. Store cluster metadata (ID, endpoint, status, node type). No query engine.
- Unit tests, register in cli.go, add to teststack
- Dashboard page for each: list view with create/delete

**Task 2: AWS Config + S3 Control + Resource Groups stubs**
- Create `awsconfig/` — `PutConfigurationRecorder`, `DescribeConfigurationRecorders`, `StartConfigurationRecorder`, `PutDeliveryChannel`, `DescribeDeliveryChannels`. Stub storage only.
- Create `s3control/` — `GetPublicAccessBlock`, `PutPublicAccessBlock`, `DeletePublicAccessBlock`. Store per-account public access block settings.
- Create `resourcegroups/` — `CreateGroup`, `DeleteGroup`, `ListGroups`, `GetGroup`, `GetResources`. Cross-service tag aggregation (query tags from all services).
- Unit tests, register in cli.go, add to teststack
- Dashboard page for each: list view

**Task 3: SWF + Kinesis Firehose stubs**
- Create `swf/` — `RegisterDomain`, `ListDomains`, `DeprecateDomain`, `RegisterWorkflowType`, `ListWorkflowTypes`, `StartWorkflowExecution`, `DescribeWorkflowExecution`. Minimal workflow metadata stubs.
- Create `firehose/` — `CreateDeliveryStream`, `DeleteDeliveryStream`, `DescribeDeliveryStream`, `ListDeliveryStreams`, `PutRecord`, `PutRecordBatch`. Store records in memory (no actual delivery).
- Unit tests, register in cli.go, add to teststack
- Dashboard page for each: list view

### v1.0 — Documentation & Production Ready

**Task 1: Service documentation**
- Create `docs/services/` directory with one markdown file per service (e.g., `dynamodb.md`, `s3.md`)
- Each doc: supported operations table, request/response examples using AWS CLI, known limitations vs real AWS, configuration options (env vars, CLI flags)
- Code examples in Go, Python, Node.js using standard AWS SDKs with `endpoint_url` override

**Task 2: Getting started guides**
- `docs/quickstart.md` — download binary, run `gopherstack`, connect with AWS CLI
- `docs/docker.md` — Docker Compose quickstart with all services
- `docs/migration.md` — migrating from LocalStack (endpoint config, feature comparison)

**Task 3: Architecture & integration guides**
- `docs/architecture/elasticache.md` — engine modes explained
- `docs/architecture/dns.md` — DNS setup per platform (macOS, Linux, Docker)
- `docs/architecture/lambda.md` — Image vs Zip, base image mapping, Runtime API
- `docs/integration/terraform.md`, `docs/integration/cdk.md`, `docs/integration/testcontainers.md`, `docs/integration/ci-cd.md`

**Task 4: CLI wrapper + persistence**
- Create `cmd/awsgs` CLI wrapper — thin wrapper around `aws` CLI that sets `--endpoint-url` automatically
- Add persistence mode in `pkgs/persistence/` — serialize in-memory state to disk (JSON/gob), restore on startup, CLI flag `--persist` / `PERSIST=true`

**Task 5: Performance benchmarks**
- Create `bench/` directory with comparative benchmarks vs LocalStack
- Benchmark startup time, operation latency (DynamoDB PutItem, S3 PutObject, SQS SendMessage), memory usage
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
| S3 | Yes | Yes | ~90% — missing presigned URLs, lifecycle, CORS, policies |
| DynamoDB | Yes | Yes | ~95% ✅ |
| SQS | Yes | Yes | ~95% ✅ |
| SNS | Yes | Yes | ~90% ✅ |
| Lambda | Yes | Yes | Image + Zip ✅ |
| IAM | Yes | Yes | ~90% ✅ |
| STS | Yes | Yes | ~95% ✅ |
| KMS | Yes | Yes | ~90% ✅ |
| Secrets Manager | Yes | Yes | ~90% ✅ |
| SSM (Parameter Store) | Yes | Yes | ~95% ✅ |
| CloudFormation | Yes | Yes | Core CRUD + 7 resource types + change sets ✅ |
| CloudWatch Metrics | Yes | Yes | Core API + alarms ✅ |
| CloudWatch Logs | Yes | Yes | Core API ✅ |
| API Gateway (REST) | Yes | Yes | Core CRUD + mock integrations ✅ |
| Step Functions | Yes | Yes | Core CRUD + stub execution ✅ |
| EventBridge | Yes | Yes | Core CRUD + event log ✅ |
| Kinesis Streams | Yes | No | v0.14 |
| ElastiCache | Yes | No | v0.15 |
| Route 53 | Yes | No | v0.16 |
| SES | Yes | No | v0.16 |
| EC2 (basic) | Yes | No | v0.17 |
| OpenSearch | Yes | No | v0.17 |
| Elasticsearch | Yes | No | v0.17 (alias of OpenSearch) |
| Redshift | Yes | No | v0.18 |
| ACM | Yes | No | v0.18 |
| AWS Config | Yes | No | v0.18 |
| S3 Control | Yes | No | v0.18 |
| Resource Groups | Yes | No | v0.18 |
| SWF | Yes | No | v0.18 |
| Kinesis Firehose | Yes | No | v0.18 |
| Transcribe | Yes | No | Not planned |

**Current: 16/30 services (53%) — 7 milestones to v1.0**

---

## Competitive Advantages

1. **No Docker required for core services** — Single Go binary for 16 AWS services. Docker only needed for Lambda and optional Docker-backed ElastiCache/OpenSearch
2. **Embedded ElastiCache** — Real Redis (via `miniredis`) running inside the Go binary. No Docker, no DNS, no external processes. `ELASTICACHE_ENGINE=embedded` is the default
3. **Persistence for free** — LocalStack charges for persistence; Gopherstack can offer it in the base product
4. **No account/auth required** — LocalStack is dropping its open-source edition (March 2026); Gopherstack remains fully open
5. **Native Go performance** — Faster startup, lower memory footprint than LocalStack's Python runtime
6. **Built-in web dashboard** — Full resource browser for all 16 services with dark mode, HTMX-powered interactions
7. **Built-in observability** — Prometheus metrics + OpenTelemetry tracing out of the box
8. **Progressive complexity** — Start with a bare binary (zero deps), add Docker for Lambda/Redis, add DNS only if you need AWS-style hostnames. Most devs never need Docker at all
