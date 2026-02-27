# Gopherstack Roadmap — LocalStack Free Tier Parity

> **Goal:** Feature parity with LocalStack's free/community tier (~34 AWS services).
> **Current state:** 29 services, ~290 operations, full platform infrastructure, dashboard UI for all services.

---

## Current Coverage (v0.5–v0.24 Complete)

### DynamoDB — ~95% parity (31 operations) ✅

Table CRUD (Create, Delete, Describe, List, Update), item CRUD (Put, Get, Delete, Update), batch ops (BatchGetItem, BatchWriteItem), query/scan with expressions, GSI/LSI, transactions (TransactWriteItems, TransactGetItems), DynamoDB Streams (DescribeStream, GetShardIterator, GetRecords, ListStreams), TTL with background reaper (UpdateTimeToLive, DescribeTimeToLive), conditional writes, pagination, PartiQL (ExecuteStatement, BatchExecuteStatement), tagging (TagResource, UntagResource, ListTagsOfResource). Backup stubs: DescribeContinuousBackups, UpdateContinuousBackups, ExportTableToPointInTime, DescribeExport, ListExports. Full dashboard UI with PartiQL tab.

### S3 — ~98% parity (39+ operations) ✅

Bucket CRUD, object CRUD, ListObjects/V2, ListObjectVersions, CopyObject, multipart uploads (Create, UploadPart, Complete, Abort, ListMultipartUploads, ListParts), versioning, object tagging, checksums (CRC32, CRC32C, SHA1, SHA256), compression, BucketACL. Presigned URLs (GET/PUT with expiry validation). Bucket policies (Put/Get/Delete). CORS configuration (Put/Get/Delete) with OPTIONS preflight. Lifecycle configuration (Put/Get/Delete) with background expiration janitor. Notification configuration (Put/Get) with event delivery to SQS/SNS/Lambda. Object lock (PutObjectLockConfiguration, GetObjectLockConfiguration). Object retention (PutObjectRetention, GetObjectRetention). Legal hold (PutObjectLegalHold, GetObjectLegalHold). Full dashboard UI with folder navigation, file preview, metadata/tagging.

### SQS — ~95% parity (17 operations) ✅

Full queue CRUD, send/receive/delete, batch operations, FIFO queues with deduplication and message groups, dead-letter queues, visibility timeout, long polling, purge, tagging, SNS→SQS cross-service delivery. Dashboard UI with queue browser and message viewer.

### SNS — ~90% parity (12 operations) ✅

Topic CRUD, subscribe/confirm/unsubscribe, publish/publishBatch, subscription protocols (SQS, HTTP/HTTPS, Lambda, email stub), filter policies, FIFO topics, message attributes. Dashboard UI with topic and subscription management.

### Lambda — Image + Zip + ESM + URLs + Versions (21 operations) ✅

CreateFunction, GetFunction, ListFunctions, DeleteFunction, UpdateFunctionCode, UpdateFunctionConfiguration, Invoke (RequestResponse + Event). PackageType Image (Docker) and Zip (bind-mount into AWS base image). Lambda Runtime API, warm container pool, environment variables, S3-based code delivery, graceful degradation when Docker unavailable. Event source mappings: CreateEventSourceMapping, GetEventSourceMapping, ListEventSourceMappings, DeleteEventSourceMapping — Kinesis→Lambda polling with background worker. Function URLs: CreateFunctionURLConfig, GetFunctionURLConfig, DeleteFunctionURLConfig — port allocation + DNS. Versions: PublishVersion, ListVersionsByFunction. Aliases: CreateAlias, GetAlias, ListAliases, UpdateAlias, DeleteAlias — qualifier-based invoke. Lambda→CloudWatch Logs wiring (stdout/stderr → log groups). Dashboard UI with function list, detail, and invoke button.

### IAM — ~92% parity (24 operations) ✅

Users, roles, policies, groups, access keys, instance profiles, attach/detach policies (AttachRolePolicy, DetachRolePolicy, AttachUserPolicy, ListAttachedUserPolicies, ListAttachedRolePolicies). Dashboard UI. Policy enforcement out of scope (same as LocalStack free tier).

### STS — ~95% parity (5 operations) ✅

AssumeRole, GetCallerIdentity, GetSessionToken, DecodeAuthorizationMessage, GetAccessKeyInfo. Dashboard UI.

### KMS — ~95% parity (23 operations) ✅

Key CRUD (CreateKey, DescribeKey, ListKeys, DisableKey, EnableKey, ScheduleKeyDeletion, CancelKeyDeletion), cryptography (Encrypt, Decrypt, GenerateDataKey, GenerateDataKeyWithoutPlaintext, ReEncrypt), aliases (CreateAlias, DeleteAlias, ListAliases), key rotation (EnableKeyRotation, DisableKeyRotation, GetKeyRotationStatus), grants (CreateGrant, ListGrants, RevokeGrant, RetireGrant, ListRetirableGrants), key policies (PutKeyPolicy, GetKeyPolicy). Dashboard UI with key detail.

### Secrets Manager — ~95% parity (11 operations) ✅

Create, get, put, delete, restore, list, describe, update. Secret versioning (AWSCURRENT, AWSPREVIOUS). TagResource, UntagResource. RotateSecret with Lambda invocation support. Dashboard UI with secret detail.

### SSM Parameter Store — ~95% parity (11 operations) ✅

Put, get, delete (single + batch), GetParameterHistory, GetParametersByPath, DescribeParameters, SecureString with KMS. Parameter tags (AddTagsToResource, RemoveTagsFromResource, ListTagsForResource). Dashboard UI with history and put modal.

### API Gateway — REST APIs + Lambda Proxy + VTL (19 operations) ✅

REST API CRUD, resources, methods, integrations (MOCK, AWS, HTTP, AWS_PROXY), deployments, stages. Lambda proxy integration: AWS_PROXY converts HTTP→Lambda event JSON→invoke→response. `/proxy/{apiId}/{stageName}/{path}` routing. VTL template rendering for request/response mapping (`$input.json()`, `$input.path()`, `$input.body`, `$context.requestId`). Dashboard UI with API list, resource tree, method/integration detail.

### EventBridge — Full Event Processing (14 operations) ✅

Event bus CRUD, rules (put/delete/list/describe/enable/disable), targets (put/remove/list), PutEvents with event log (last 1000). Target fan-out delivery to Lambda/SQS/SNS targets. Event pattern matching (source, detail-type, nested detail field patterns — exact match, prefix, exists, numeric ranges, anything-but). Scheduled rules with cron/rate expression parsing and background scheduler. Dashboard UI with event bus list, rules, event log viewer.

### Step Functions — ASL Interpreter (9 operations) ✅

State machine CRUD, start/stop/describe/list executions, GetExecutionHistory. Full ASL state machine interpreter: Pass, Task (Lambda invocation), Choice (StringEquals, NumericGreaterThan, BooleanEquals, And, Or, Not), Wait, Succeed, Fail, Parallel, Map. ResultPath, InputPath, OutputPath support. Catch error handling. Dashboard UI with state machine list, execution history.

### CloudWatch Metrics (7 operations) ✅

PutMetricData, GetMetricStatistics, GetMetricData, ListMetrics, PutMetricAlarm, DescribeAlarms, DeleteAlarms. Supports both legacy query/XML and rpc-v2-cbor (Smithy RPCv2) protocol. Dashboard UI with namespace browser and alarm status.

### CloudWatch Logs (8 operations) ✅

Log group CRUD, log stream CRUD, PutLogEvents, GetLogEvents, FilterLogEvents. Lambda container stdout/stderr→CloudWatch Logs wiring. Dashboard UI with log group list, stream viewer, search/filter.

### CloudFormation (12 operations) ✅

Stack CRUD (create, update, delete, describe, list), stack events, change sets (create, describe, execute, delete, list), GetTemplate. Resource creation for 12 resource types (S3::Bucket, DynamoDB::Table, SQS::Queue, SNS::Topic, SSM::Parameter, KMS::Key, SecretsManager::Secret, Lambda::Function, Events::Rule, StepFunctions::StateMachine, Logs::LogGroup, ApiGateway::RestApi). Intrinsic functions (Ref, Fn::Sub, Fn::Join), JSON + YAML. Dashboard UI with stack list, detail, events.

### Kinesis Streams (10 operations) ✅

CreateStream, DeleteStream, DescribeStream, DescribeStreamSummary, ListStreams, PutRecord, PutRecords, GetShardIterator (TRIM_HORIZON, LATEST, AT_SEQUENCE_NUMBER, AFTER_SEQUENCE_NUMBER), GetRecords, ListShards. Kinesis→Lambda event source mapping with background poller. Dashboard UI with stream list, shard viewer, put record form.

### ElastiCache (7 operations) ✅ — LocalStack charges for this

CreateCacheCluster, DeleteCacheCluster, DescribeCacheClusters, ListTagsForResource, CreateReplicationGroup, DeleteReplicationGroup, DescribeReplicationGroups. Three engine modes: embedded (miniredis — zero deps), docker (real Redis/Valkey containers), stub (API-only). Dashboard UI with cluster list and cluster detail.

### EC2 — Basic Stubs (10 operations) ✅

RunInstances, DescribeInstances, TerminateInstances, DescribeSecurityGroups, CreateSecurityGroup, DeleteSecurityGroup, DescribeVpcs, DescribeSubnets, CreateVpc, CreateSubnet. Metadata stubs only — no actual compute (same as LocalStack free tier). Default VPC/subnet pre-populated. Dashboard UI with instances, security groups, VPC/subnet views.

### Route 53 (6 operations) ✅

CreateHostedZone, DeleteHostedZone, ListHostedZones, GetHostedZone, ChangeResourceRecordSets (CREATE, DELETE, UPSERT), ListResourceRecordSets. Wired into `pkgs/dns` for actual DNS resolution. Dashboard UI with hosted zone list and record set management.

### SES (6 operations) ✅

SendEmail, SendRawEmail, VerifyEmailIdentity, ListIdentities, GetIdentityVerificationAttributes, DeleteIdentity. All emails captured locally for inspection — no real sending (same as LocalStack). Dashboard UI with inbox, email detail, identity management.

### OpenSearch (4 operations) ✅

CreateDomain, DescribeDomain, DeleteDomain, ListDomainNames. `OPENSEARCH_ENGINE` config: docker (real OpenSearch container) or stub (API-only, default). Dashboard UI with domain list and domain detail.

### ACM (4 operations) ✅

RequestCertificate, DescribeCertificate, ListCertificates, DeleteCertificate. Synthetic ARNs, status=ISSUED. No real TLS. Dashboard UI with certificate list.

### Redshift (3 operations) ✅

CreateCluster, DeleteCluster, DescribeClusters. Synthetic endpoints via DNS. No query engine (same as LocalStack free tier). Dashboard UI with cluster list.

### AWS Config (5 operations) ✅

PutConfigurationRecorder, DescribeConfigurationRecorders, StartConfigurationRecorder, PutDeliveryChannel, DescribeDeliveryChannels. Stub storage only — no actual configuration tracking (same as LocalStack). Dashboard UI with recorder/channel list.

### S3 Control (3 operations) ✅

GetPublicAccessBlock, PutPublicAccessBlock, DeletePublicAccessBlock. Per-account public access block settings. Dashboard UI.

### Resource Groups (4 operations) ✅

CreateGroup, DeleteGroup, ListGroups, GetGroup. Dashboard UI with group list.

### SWF (7 operations) ✅

RegisterDomain, ListDomains, DeprecateDomain, RegisterWorkflowType, ListWorkflowTypes, StartWorkflowExecution, DescribeWorkflowExecution. Minimal workflow metadata stubs. Dashboard UI with domain/workflow list.

### Kinesis Firehose (6 operations) ✅

CreateDeliveryStream, DeleteDeliveryStream, DescribeDeliveryStream, ListDeliveryStreams, PutRecord, PutRecordBatch. Records stored in memory — no actual delivery (same as LocalStack). Dashboard UI with delivery stream list.

---

## Platform Infrastructure ✅ Complete

- **Internal DNS** — `pkgs/dns` using `miekg/dns`, synthetic AWS-style hostnames, UDP/TCP, configurable port
- **Port Range Management** — `pkgs/portalloc`, thread-safe acquire/release, configurable range
- **Docker Integration** — `pkgs/docker`, container lifecycle, warm pool with idle reaping, Lambda Image + Zip
- **Init Hooks** — `pkgs/inithooks`, user shell scripts on startup
- **Health Endpoint** — `/_gopherstack/health` with all registered services
- **Cross-Service Event Bus** — `pkgs/events`, SNS→SQS, EventBridge→Lambda/SQS/SNS, S3→SQS/SNS/Lambda delivery

---

## Developer Experience ✅

- [x] Single-port routing — all 29 services on one port via priority-based service router
- [x] Docker image + Docker Compose support
- [x] CLI flags / env config via Kong
- [x] Web dashboard with sidebar navigation for all 29 services
- [x] Dark mode UI with automatic theme switching (HTMX + Flowbite + Tailwind)
- [x] Prometheus metrics + operation tracking
- [x] OpenTelemetry tracing
- [x] Demo data seeding (`--demo`)
- [x] Init hooks for resource seeding on startup
- [x] Testcontainers module for Go (`modules/gopherstack`)
- [x] Terraform compatibility (tested with HashiCorp AWS provider v5.0)
- [x] CDK compatibility docs (README)

**DX gaps remaining:**
- [ ] `awslocal`-style CLI wrapper or docs for `aws --endpoint-url`
- [ ] Persistence — on-disk state snapshots across restarts (differentiator vs LocalStack)

---

## Test Coverage Summary

| Layer | Tests | Coverage |
|-------|-------|----------|
| Unit tests | 129 files | All 29 services + platform packages |
| Integration tests | 151+ tests (67 files) | All 29 services + cross-service (SNS→SQS, EventBridge→SQS, Kinesis→Lambda, S3→SQS, StepFunctions ASL) |
| E2E / browser tests | 47 tests (18 files) | Playwright — dashboard tests for 15+ services |
| Terraform tests | 2 scenarios | DynamoDB, S3+SQS via HashiCorp AWS provider v5.0 |

**Test coverage gaps:**
- [ ] API Gateway — unit tests only, no integration tests
- [ ] E2E dashboard tests missing for: EventBridge, StepFunctions, CloudFormation, CloudWatch, CloudWatch Logs, Kinesis, ACM, Redshift, Firehose, Resource Groups, S3 Control, AWS Config, SWF
- [ ] Terraform tests only cover 3 resource types — should cover more services

---

## Remaining Milestones

### v0.25 — EventBridge Scheduler + Route 53 Resolver

Two services in LocalStack's free tier that we don't have yet.

**Task 1: EventBridge Scheduler service**
- Create `scheduler/` directory with standard service structure
- Create `scheduler/backend.go` — schedule store with `Schedule` struct (name, ARN, schedule expression, target ARN, role ARN, state, flexible time window)
- Create `scheduler/handler.go` — JSON protocol with `X-Amz-Target: AWSScheduler.*` header matching
- Implement operations: `CreateSchedule` (store schedule config, parse rate/cron expression), `GetSchedule`, `ListSchedules`, `DeleteSchedule`, `UpdateSchedule`, `TagResource`, `ListTagsForResource`
- Wire scheduler to EventBridge's existing schedule execution engine for actual target triggering (competitive advantage — LocalStack's Scheduler is mocked-only)
- Register service in `cli.go`, add to `internal/teststack/teststack.go`
- Unit tests in `scheduler/handler_test.go` — CRUD operations, schedule expression validation
- Integration test in `test/integration/scheduler_test.go` — create schedule, list, get, update, delete

**Task 2: Route 53 Resolver service**
- Create `route53resolver/` directory with standard service structure
- Create `route53resolver/backend.go` — resolver endpoint store with `ResolverEndpoint` struct (ID, direction, IP addresses, status, VPC ID)
- Create `route53resolver/handler.go` — JSON protocol with `X-Amz-Target: Route53Resolver.*` header matching
- Implement operations: `CreateResolverEndpoint`, `DeleteResolverEndpoint`, `ListResolverEndpoints`, `GetResolverEndpoint`, `CreateResolverRule`, `DeleteResolverRule`, `ListResolverRules`
- Register service in `cli.go`, add to `internal/teststack/teststack.go`
- Unit tests in `route53resolver/handler_test.go` — endpoint and rule CRUD
- Integration test in `test/integration/route53resolver_test.go`

**Task 3: Dashboard UI for both**
- Create `dashboard/templates/scheduler/index.html` — schedule list with create/delete
- Create `dashboard/scheduler_handlers.go`
- Create `dashboard/templates/route53resolver/index.html` — resolver endpoint/rule list
- Create `dashboard/route53resolver_handlers.go`
- Add both to sidebar navigation
- E2E tests for both dashboards

### v0.26 — RDS Service (Competitive Advantage — LocalStack charges for this)

**Task 1: RDS service — stub + Docker mode**
- Create `rds/` directory with standard service structure
- Create `rds/backend.go` — instance store with `DBInstance` struct (ID, engine, status, endpoint, port, master username, DB name, instance class, allocated storage, VPC/subnet group)
- Create `rds/handler.go` — form-encoded XML protocol (same pattern as EC2/IAM)
- Implement operations: `CreateDBInstance` (allocate port, start container if docker mode), `DeleteDBInstance`, `DescribeDBInstances`, `ModifyDBInstance`, `CreateDBSnapshot`, `DescribeDBSnapshots`, `DeleteDBSnapshot`, `CreateDBSubnetGroup`, `DescribeDBSubnetGroups`, `DeleteDBSubnetGroup`
- Add `RDS_ENGINE` config flag — `docker` (start real Postgres/MySQL container) or `stub` (API-only, default)
- Docker mode: on CreateDBInstance, select image based on Engine field (postgres → `postgres:16-alpine`, mysql → `mysql:8-lts`), start container on allocated port, register DNS hostname, return real connectable endpoint
- Register service in `cli.go`, add to `internal/teststack/teststack.go`
- Unit tests in `rds/handler_test.go` — CRUD operations, engine selection, subnet groups

**Task 2: RDS Dashboard UI + integration tests**
- Create `dashboard/templates/rds/index.html` — instance list (ID, engine badge, status, endpoint), subnet groups, snapshots
- Create `dashboard/templates/rds/instance_detail.html` — instance detail with endpoint, engine, configuration
- Create `dashboard/rds_handlers.go` — list/detail/create/delete handlers
- Add RDS to sidebar navigation
- Integration test in `test/integration/rds_test.go` — create instance (stub mode), describe, modify, create snapshot, delete
- E2E test in `test/e2e/rds_test.go` — verify dashboard renders

### v0.27 — Test Coverage Gaps + Minor Stubs

**Task 1: API Gateway integration tests**
- `test/integration/apigateway_test.go` — create REST API, create resource, put method, put integration, create deployment, get stages, verify proxy route, delete chain

**Task 2: Missing E2E dashboard tests**
- Add E2E tests for services without them: EventBridge, StepFunctions, CloudFormation, CloudWatch, CloudWatch Logs, Kinesis, ACM, Firehose, Redshift, S3 Control, AWS Config, SWF, Resource Groups

**Task 3: Transcribe + Support API stubs**
- Create `transcribe/` — `StartTranscriptionJob`, `ListTranscriptionJobs`, `GetTranscriptionJob`. Return synthetic transcription results. No real speech-to-text.
- Create `support/` — `CreateCase`, `DescribeCases`, `ResolveCase`. Fully mocked.
- Unit tests, register in cli.go, add to teststack, dashboard pages for each

### v0.28 — Expanded Terraform Tests

**Task 1: Terraform provider coverage**
- Expand `test/integration/terraform_test.go` to cover additional resource types:
  - `aws_lambda_function` (zip package)
  - `aws_iam_role` + `aws_iam_policy` + `aws_iam_role_policy_attachment`
  - `aws_sns_topic` + `aws_sqs_queue` + `aws_sns_topic_subscription`
  - `aws_kms_key` + `aws_kms_alias`
  - `aws_secretsmanager_secret` + `aws_secretsmanager_secret_version`
  - `aws_ssm_parameter`
  - `aws_route53_zone` + `aws_route53_record`
  - `aws_cloudwatch_log_group`
  - `aws_ses_email_identity`

**Task 2: Terraform data source tests**
- Test Terraform data sources: `data.aws_caller_identity`, `data.aws_region`, `data.aws_iam_policy_document`, `data.aws_s3_bucket`

### v0.29 — Code Quality Refactor

Clean pass across the entire codebase to enforce the style guide in `.github/instructions`. This must happen before the architectural changes in v0.30–v0.31.

**Task 1: Eliminate `break` statements**
- Audit every `for`/`switch` loop across all 29 service packages and `pkgs/`
- Replace `break` in loops with extracted helper functions that use early `return`
- Note: `switch` case fallthrough is fine — the rule targets loop `break` specifically

**Task 2: Eliminate anonymous structs**
- Audit all handler files for inline `var req struct { ... }` patterns (used heavily in request parsing)
- Extract each into a named, unexported type (e.g., `type registerDomainInput struct { ... }`)
- Ensure JSON/XML struct tags are preserved

**Task 3: Interface compliance audit**
- Verify all service backends follow "accept interfaces, return concrete types"
- Ensure all interfaces are small (1-3 methods) and defined near usage, not implementation
- Check for any exported interfaces that shouldn't be — unexport where possible
- Ensure `-er` suffix naming convention on single-method interfaces

**Task 4: Error handling cleanup**
- Ensure all errors are lowercase, no trailing punctuation
- Replace any `fmt.Errorf("...", err)` that should use `%w` for wrapping
- Verify no code both logs and returns the same error (choose one)
- Consolidate duplicate sentinel errors across packages where appropriate

**Task 5: General style enforcement**
- Run `make lint-fix` across entire codebase
- Ensure all exported types/functions/methods have doc comments starting with the name
- Remove any `nolint` directives that can be fixed instead
- Verify table tests everywhere, parallel where possible, `require`/`assert` only (no `t.Fatal`/`t.Error`)
- Ensure all logging uses `log/slog` (no `fmt.Println`, `log.Println`, etc.)

### v0.30 — Container Runtime Abstraction (Docker + Podman)

`pkgs/docker/` already has an `APIClient` interface that isolates the Docker SDK. This milestone generalizes it to support Podman (and any OCI-compatible runtime) via a single env var switch.

**Task 1: Runtime interface + Podman provider**
- Rename `pkgs/docker/` → `pkgs/container/` (keep `pkgs/docker/` as a thin re-export for backwards compat if needed)
- Extract `Runtime` interface from existing `Client`: `CreateAndStart(spec)`, `StopAndRemove(id)`, `AcquireWarm(image)`, `ReleaseContainer(id)`, `PullImage(image)`, `HasImage(image)`, `Ping()`
- Create `DockerRuntime` — wraps existing `realDockerClient` adapter (no behaviour change)
- Create `PodmanRuntime` — connects to Podman socket (`podman.sock` or `CONTAINER_HOST`), implements same `Runtime` interface via Podman's Docker-compatible API
- Add `CONTAINER_RUNTIME` env var / `--container-runtime` CLI flag: `docker` (default) | `podman` | `auto` (detect which socket exists)
- Update `docker.Config` → `container.Config`, add `Runtime` field
- Unit tests: mock-based tests for both runtimes, auto-detection logic

**Task 2: Update all consumers**
- Update Lambda (`lambda/backend.go`, `lambda/provider.go`, `lambda/settings.go`) to use `container.Runtime` instead of `docker.Client`
- Update ElastiCache docker mode (`elasticache/backend.go`) to use `container.Runtime`
- Update OpenSearch docker mode (`opensearch/backend.go`) to use `container.Runtime`
- Update RDS docker mode (`rds/backend.go`, from v0.26) to use `container.Runtime`
- Update `DockerHost` setting to be runtime-aware (Podman rootless uses different networking)
- Integration tests verifying Lambda invoke works with both runtimes (skip Podman tests if socket not available)

**Task 3: Documentation + Compose files**
- Add Podman-specific notes to README (rootless setup, socket path, `CONTAINER_RUNTIME=podman`)
- Create `docker-compose.podman.yml` or document `podman compose` compatibility
- Update `docs/architecture/` with container runtime diagram

### v0.31 — Persistence

State survives restarts. Container-backed mode uses volumes; binary-only mode uses `~/.gopherstack/` (or `GOPHERSTACK_DATA_DIR`). A single env var (`PERSIST=true`) enables it.

**Task 1: Persistence interface + file-based backend**
- Create `pkgs/persistence/` package with `Store` interface: `Save(service, key, data)`, `Load(service, key)`, `Delete(service, key)`, `ListKeys(service)`
- Implement `FileStore` — writes JSON files to `~/.gopherstack/data/{service}/{key}.json` (or `GOPHERSTACK_DATA_DIR`)
- Implement `NullStore` (default) — no-ops, current behaviour preserved
- Add `PERSIST` env var / `--persist` CLI flag: `true` | `false` (default)
- Add `GOPHERSTACK_DATA_DIR` env var / `--data-dir` CLI flag for custom path
- Unit tests for FileStore: write/read/delete/list, concurrent access, corrupt file handling

**Task 2: Wire persistence into services**
- Define `Persistable` interface that backends can implement: `Snapshot() []byte`, `Restore([]byte) error`
- Update each `InMemoryBackend` to implement `Persistable` — serialize state to JSON on writes, restore on startup
- Implement `Persistable` on ALL service backends — every `InMemoryBackend` across all 29+ services must serialize/restore its state
- On startup: if `PERSIST=true`, load snapshots for all registered services
- On mutation: debounced async write (e.g. 500ms after last write) to avoid disk thrashing
- Integration tests: create resources → restart Gopherstack → verify resources still exist

**Task 3: Container volume mode + Docker Compose**
- When running in a container with `PERSIST=true`, default `GOPHERSTACK_DATA_DIR` to `/data` and document volume mount: `-v gopherstack-data:/data`
- Update `docker-compose.yml` to include named volume with persistence enabled
- Update Podman compose file (from v0.30) similarly
- E2E test: docker-compose up → create resources → docker-compose restart → verify resources survive

### v1.0 — Documentation & Production Ready

**Task 1: Service documentation**
- Create `docs/services/` directory with one markdown file per service
- Each doc: supported operations table, request/response examples using AWS CLI, known limitations vs real AWS, configuration options

**Task 2: Getting started + architecture guides**
- `docs/quickstart.md` — download binary, run `gopherstack`, connect with AWS CLI
- `docs/docker.md` — Docker Compose quickstart with all services
- `docs/migration.md` — migrating from LocalStack (endpoint config, feature comparison)
- `docs/architecture/` — ElastiCache engine modes, DNS setup, Lambda runtime, container runtimes, persistence

**Task 3: CLI wrapper + benchmarks**
- Create `cmd/awsgs` CLI wrapper — thin wrapper around `aws` CLI that sets `--endpoint-url` automatically
- Create `bench/` directory with comparative benchmarks vs LocalStack

---

## LocalStack Free Tier Parity Scorecard

| Service | LocalStack Free | Gopherstack | Status |
|---------|:-:|:-:|--------|
| S3 | Yes | Yes | ~98% — full CRUD + lifecycle + notifications + object lock ✅ |
| DynamoDB | Yes | Yes | ~95% — streams, TTL, PartiQL, transactions ✅ |
| SQS | Yes | Yes | ~95% — FIFO, DLQ, batch, long polling ✅ |
| SNS | Yes | Yes | ~90% — filter policies, cross-service delivery ✅ |
| Lambda | Yes | Yes | 21 ops — Image + Zip + ESM + URLs + versions ✅ |
| IAM | Yes | Yes | ~92% — users, roles, policies, groups ✅ |
| STS | Yes | Yes | ~95% ✅ |
| KMS | Yes | Yes | 23 ops — grants, key policies, rotation ✅ |
| Secrets Manager | Yes | Yes | 11 ops — rotation with Lambda ✅ |
| SSM (Parameter Store) | Yes | Yes | 11 ops — SecureString with KMS ✅ |
| CloudFormation | Yes | Yes | 12 ops + 12 resource types ✅ |
| CloudWatch Metrics | Yes | Yes | 7 ops + RPCv2 CBOR ✅ |
| CloudWatch Logs | Yes | Yes | 8 ops + Lambda log wiring ✅ |
| API Gateway (REST) | Yes | Yes | 19 ops + Lambda proxy + VTL ✅ |
| Step Functions | Yes | Yes | 9 ops + full ASL interpreter ✅ |
| EventBridge | Yes | Yes | 14 ops + fan-out + patterns + scheduler ✅ |
| Kinesis Streams | Yes | Yes | 10 ops + Lambda ESM ✅ |
| Kinesis Firehose | Yes | Yes | 6 ops ✅ |
| EC2 (basic) | Yes | Yes | 10 ops — stub (same as LocalStack free) ✅ |
| Route 53 | Yes | Yes | 6 ops + real DNS resolution ✅ |
| SES | Yes | Yes | 6 ops — captured locally ✅ |
| OpenSearch | Yes | Yes | 4 ops + Docker mode ✅ |
| Elasticsearch | Yes | Yes | Alias of OpenSearch ✅ |
| Redshift | Yes | Yes | 3 ops — stub ✅ |
| ACM | Yes | Yes | 4 ops ✅ |
| AWS Config | Yes | Yes | 5 ops — stub (same as LocalStack) ✅ |
| S3 Control | Yes | Yes | 3 ops ✅ |
| Resource Groups | Yes | Yes | 4 ops ✅ |
| SWF | Yes | Yes | 7 ops ✅ |
| EventBridge Scheduler | Yes | No | v0.25 |
| Route 53 Resolver | Yes | No | v0.25 |
| Transcribe | Yes | No | v0.27 (stub — no real speech-to-text) |
| Support API | Yes | No | v0.27 (stub) |
| **ElastiCache** | **No (paid)** | **Yes** | 7 ops + embedded Redis — **free in Gopherstack** ✅ |
| **RDS** | **No (paid)** | No | v0.26 — **will be free in Gopherstack** |

**Current: 29/34 free tier services (85%) + 1 paid-tier service (ElastiCache) included free**

**After v0.27: 33/34 free tier services (97%) + 2 paid-tier services (ElastiCache, RDS) included free**

---

## Competitive Advantages

1. **No Docker required for core services** — Single Go binary for 29 AWS services. Docker only needed for Lambda and optional Docker-backed ElastiCache/OpenSearch/RDS
2. **ElastiCache for free** — Real Redis (via `miniredis`) running inside the Go binary. LocalStack charges for this (Base tier)
3. **RDS for free** (v0.26) — Real Postgres/MySQL via Docker, or API stubs. LocalStack charges for this (Base tier)
4. **Real EventBridge execution** — Pattern matching, fan-out delivery, scheduled rules actually fire. LocalStack free tier is mocked-only
5. **Real Step Functions ASL** — Full state machine interpreter. LocalStack free tier auto-succeeds
6. **Persistence for free** (v0.31) — LocalStack charges for persistence; Gopherstack will offer it in the base product
7. **No account/auth required** — LocalStack is dropping its open-source edition (March 2026); Gopherstack remains fully open
8. **Native Go performance** — Faster startup, lower memory footprint than LocalStack's Python runtime
9. **Built-in web dashboard** — Full resource browser for all 29 services with dark mode, HTMX-powered interactions
10. **Built-in observability** — Prometheus metrics + OpenTelemetry tracing out of the box
11. **Progressive complexity** — Start with a bare binary (zero deps), add Docker/Podman for Lambda/Redis/RDS, add DNS only if you need AWS-style hostnames
12. **Podman support** (v0.30) — First-class Podman support via `CONTAINER_RUNTIME=podman`. LocalStack requires Docker
