# Gopherstack Improvements & Gap Analysis

Comprehensive audit of all services comparing against LocalStack and real AWS behavior.
Covers: resource leaks, memory leaks, race conditions, performance optimizations, missing features, and bugs.

---

## Table of Contents

1. [Critical Cross-Service Issues](#critical-cross-service-issues)
2. [S3](#s3)
3. [DynamoDB](#dynamodb)
4. [SQS](#sqs)
5. [SNS](#sns)
6. [Lambda](#lambda)
7. [IAM](#iam)
8. [STS](#sts)
9. [KMS](#kms)
10. [Secrets Manager](#secrets-manager)
11. [EventBridge](#eventbridge)
12. [CloudWatch](#cloudwatch)
13. [RDS](#rds)
14. [EC2](#ec2)
15. [ECS](#ecs)
16. [Step Functions](#step-functions)
17. [Kinesis](#kinesis)
18. [CloudWatch Logs](#cloudwatch-logs)
19. [CloudFormation](#cloudformation)
20. [Cognito IDP](#cognito-idp)
21. [Firehose](#firehose)
22. [Route 53](#route-53)
23. [AppSync](#appsync)
24. [ElastiCache](#elasticache)
25. [SSM (Systems Manager)](#ssm-systems-manager)
26. [SES](#ses)
27. [AWS Config](#aws-config)
28. [API Gateway](#api-gateway)
29. [ECR](#ecr)
30. [Redshift](#redshift)
31. [OpenSearch](#opensearch)
32. [Batch](#batch)
33. [Shared Packages](#shared-packages)
34. [Summary Matrix](#summary-matrix)

---

## Critical Cross-Service Issues

### Persistence Layer — No fsync Before Rename
- **Location:** `pkgs/persistence/file.go`
- **Issue:** `FileStore.Save()` writes data to a temp file, closes it, and renames — but never calls `tmp.Sync()`. On power loss between Close and Rename, data may be lost or corrupted.
- **Fix:** Add `tmp.Sync()` before `tmp.Close()`.

### Lockmetrics — Prometheus Metric Leak
- **Location:** `pkgs/lockmetrics/lockmetrics.go`
- **Issue:** Every `RWMutex` creation registers Prometheus metrics. `Close()` removes from internal tracking but never unregisters from Prometheus. Long-running services with many table create/delete cycles accumulate thousands of leaked metric instances.
- **Fix:** Cache Prometheus VecCollectors at package level; reuse by label set.

### Handler ResponseWriter — Status Code Logic Bug
- **Location:** `pkgs/handler/response_writer.go`
- **Issue:** `statusCode` initialized to `http.StatusOK` (200) in constructor, so the `if w.statusCode == 0` check in `Write()` is dead code. If `Write()` is called without prior `WriteHeader()`, status tracking may be incorrect.
- **Fix:** Initialize `statusCode` to 0 and handle the implicit 200 in `Write()`.

---

## S3

### Memory Leaks & Resource Issues
- **Tag storage gaps:** Tags in `b.tags` not cleaned up on `CompleteMultipartUpload` failures or `UploadPart` failures — orphaned tag entries accumulate.

### Race Conditions
- **TOCTOU in PutObject:** Object retrieved under `bucket.mu`, then `obj.mu` acquired separately. Between these points another goroutine could modify the object.
- **bucketIndex not atomic with buckets[region]:** Updated separately in `CreateBucket` — brief inconsistency possible if crash occurs between the two writes.

### Performance
- **Multipart parts linear search:** No index on part numbers — `ListParts` and `CompleteMultipartUpload` iterate all parts.
- **ListBuckets sorting outside lock:** Concurrent `CreateBucket` during sort may produce inconsistent results.

### Missing Features (vs LocalStack / AWS)
- Object ACLs (`GetObjectAcl` returns 501)
- Bucket tagging operations return 501
- Request Payment, Intelligent-Tiering not implemented
- Bucket policies stored but never evaluated/enforced
- Object Lock retention only enforced on DELETE, not GET/PUT
- Encryption config stored but objects not actually encrypted
- Lifecycle tag filtering not supported; only Days-based, not Date-based expiration
- Replication config stored but never executed
- Logging config stored but no logs generated
- Checksum values accepted but never computed or verified
- Version ID format uses Unix nanoseconds instead of AWS random alphanumeric strings

---

## DynamoDB

### Memory Leaks
- **Stream ring buffer:** `maxStreamRecords = 1000` limits count but no TTL. Heavy writes with deep copies on every mutation generate massive memory pressure. Slice reslicing pattern for eviction is allocation-heavy.
- **Expression cache unbounded growth:** LRU cache has no TTL; entries persist indefinitely.

### Race Conditions
- **Shallow copy in Query/Scan:** Pointer snapshots taken but nested map/list attributes could be modified by concurrent writes before snapshot items are fully processed.
- **Batch operations race:** `BatchGetItem` table structure could change during concurrent reads if `UpdateTable` runs.
- **Transaction token cleanup incomplete:** `txnPending` entries not cleaned by janitor — only `txnTokens` are swept.

### Performance
- **Full index copy fallback:** Unknown PK values in query fall back to O(n) full index copy.
- **Scan sort every call:** `sortScanResults()` sorts candidates on every scan — no caching for paginated scans.
- **Batch goroutine overhead:** Single-table batch requests still spawn unnecessary goroutines.

### Missing Features
- No GSI backfill simulation when creating GSI on existing table
- No GSI provisioning limits/throttling
- Backup size calculated as `len(items) * 400` instead of actual item sizes
- Point-in-time recovery flag present but not used for actual recovery
- No encryption support (SSE-KMS, SSE-S3)
- No DAX caching layer
- PartiQL exists but not integrated
- No Kinesis Data Streams export

---

## SQS

### Memory Leaks
- **Unbounded deduplication IDs:** FIFO dedup IDs only pruned on `ReceiveMessage`, not on `SendMessage`. Send-only queues leak entries indefinitely.
- **Queue notify channel never closed:** Buffered channel prevents full cleanup.

### Bugs
- **Missing WaitTimeSeconds validation:** Accepts unlimited wait times (should enforce 0–20s max per AWS) — potential DoS vector.
- **Missing message size validation:** No enforcement of `MaximumMessageSize` (262,144 bytes).
- **Batch response ordering:** `SendMessageBatch` response doesn't maintain input order.

### Race Conditions
- **Long-poll notify channel race:** Multiple concurrent receivers may miss notifications on shared buffered channel.
- **ChangeMessageVisibility concurrent mutations:** Multiple threads modifying `InFlightMessage.VisibleAt` without full synchronization.

---

## SNS

### Resource Leaks
- **HTTP delivery goroutine leak:** If `deliverHTTP` hangs indefinitely, semaphore slots never released. Repeated publishes exhaust goroutines.
- **Untracked delivery goroutines:** Spawned without `WaitGroup`, orphaned on shutdown.
- **Unbounded HTTP response body reading:** Large responses not bounded.

### Bugs
- **Silent delivery failures:** All HTTP errors ignored, no retry mechanism — violates AWS delivery semantics.
- **Invalid context usage:** Uses `context.Background()` instead of request context for event emission.
- **FilterPolicy JSON unbounded:** No size limits on JSON parsing — memory exhaustion possible.

### Missing Features
- FIFO topic support (deduplication, message groups)
- Delivery retry policy
- Dead-letter queue (redrive policy)
- Message TTL / retention
- SMS delivery
- Email delivery
- Platform endpoint actual invocation

---

## Lambda

### Resource Leaks (Critical)
- **Container process leak on timeout:** Timed-out invocations leave Docker containers running, orphaned goroutines accumulate.
- **Unbounded temp dirs:** Per-invocation zip extraction temp dirs only cleaned in `Close()`. Failed extractions leave dirs on disk.
- **Docker container not stopped on failure:** If `startContainer` fails, container is abandoned.
- **Unbounded runtimes map:** Each invoked function name creates a permanent entry — no LRU eviction.

### Race Conditions
- **Runtime server queue deadlock:** Buffered channel size 10 can block indefinitely on concurrent invokes.
- **Pending invocations leak:** `sync.Map` unbounded, result channel send may panic if closed.
- **Concurrency tracking race:** Check-then-increment not atomic — concurrent invokes slip through limits.

### Bugs
- **Async invocations silently dropped:** Queue full → message dropped (AWS queues indefinitely).
- **Reserved concurrency 0 not enforced:** Event invocations bypass limit.

### Missing Features
- SQS / DynamoDB / S3 event source mappings (partial)
- VPC execution
- Custom ECR image support (incomplete)
- SnapStart support
- Async invocation DLQ
- Code signing verification
- Provisioned concurrency enforcement

---

## IAM

### Security Issues
- **Access key secret generation:** Uses UUID (`uuid.New().String()`) instead of cryptographically secure 40-character random strings — reduces entropy.
- **No login profile password validation:** Accepts empty passwords.
- **Policy document JSON not validated:** Accepts malformed policy documents silently.

### Memory Leaks
- **User deletion doesn't clean access keys:** Access keys for deleted users remain in `b.accessKeys` indefinitely.
- **Duplicate policy attachments:** `AttachUserPolicy` appends without dedup check — identical entries accumulate.

### Missing Features
- `AddRoleToInstanceProfile` not implemented — instance profiles non-functional
- `GetSAMLProvider` / `ListSAMLProviders` not implemented
- Missing group operations (`RemoveUserFromGroup`, `PutGroupPolicy`, `GetGroup`)
- Permission boundaries stored but never enforced
- No `GetAccountSummary` implementation

---

## STS

### Memory Leaks
- **Session tokens stored forever:** `b.sessions` map has no expiration cleanup. Every `AssumeRole` call adds an entry that is never garbage-collected — OOM risk under sustained load.
- **No token expiration enforcement:** `GetCallerIdentity` doesn't validate if session is expired.

### Missing Features
- `GetFederationToken` not implemented
- `AssumeRoleWithSAML` / `AssumeRoleWithWebIdentity` not implemented
- No session policy support — `Policy` field parsed but discarded

---

## KMS

### Bugs
- **Key material never rotated:** `EnableKeyRotation` / `DisableKeyRotation` defined but don't actually rotate keys.
- **Encryption context not used:** AAD set to keyID only — custom encryption context from API ignored.

### Memory Leaks
- **Key deletion doesn't purge key material:** `keyMaterials` map entry never deleted after deletion period — deleted keys accumulate indefinitely.

### Missing Features
- `ImportKeyMaterial` / `DeleteImportedKeyMaterial`
- Custom key stores
- Multi-region key replication
- Key policy enforcement (stored but never enforced)
- Grant constraint validation

---

## Secrets Manager

### Memory Leaks
- **Secret versions never purged:** Every `PutSecretValue` adds a version — no maximum retention. Calling 1000× = 1000 versions in memory. `DeleteSecret` only marks deletion.

### Bugs
- **Staging label rotation bug:** `rotateStagingLabels` converts AWSCURRENT to AWSPREVIOUS but has an unused `newVersionID` parameter — old version temporarily shows as current during race.
- **Secret ARN suffix generation race:** Suffix generated before lock acquired; two threads could generate the same random suffix. `rand.Read` failure returns hardcoded `"000000"`.
- **No maximum secret size validation:** AWS limits to 64KB; gopherstack accepts arbitrarily large secrets.

### Missing Features
- Resource policies stubbed (stored but never evaluated)
- No cross-region replication
- No rotation Lambda retry on failure
- `ListSecretVersionIds` not implemented

---

## EventBridge

### Resource Leaks
- **Goroutine leak in event delivery:** If target service hangs, goroutine stays blocked. `Close()` calls `wg.Wait()` with no timeout — service shutdown hangs indefinitely.
- **Unbounded `lastFired` map in scheduler:** Tracks all scheduled rule ARNs with cleanup only between ticks — orphaned entries from deleted rules accumulate.

### Missing Features
- No Dead-Letter Queues for failed delivery
- No Event Replay / Archives
- No partner event sources
- No Input Transformer validation
- No cross-account / cross-region routing

---

## CloudWatch

### Bugs
- **Context leak in alarm actions:** `executeActions()` uses `context.Background()` instead of passed context. Lambda invocations cannot be cancelled during shutdown.
- **Potential deadlock in `SetAlarmState`:** Lock released before action execution — between unlock and execution, other threads can modify alarms causing composite alarm inconsistency.
- **No circular dependency detection:** Composite alarm A → B → A could loop infinitely with no recursion limit.

### Memory Issues
- **Unbounded metric storage:** Capped per-metric (1000 points) but no namespace-level limit and no time-based retention. Memory DoS via metric flooding possible.

### Missing Features
- Anomaly detection alarms
- Full metric math expression support
- Percentile statistics (p99, p99.9)
- CloudWatch Logs metric filters integration
- Alarm suppression / inhibition

---

## RDS

### Race Conditions
- **DNS deregistration race:** `DeleteDBInstance` DNS deregistration happens after lock release. Another thread could create instance with same endpoint between delete and deregister.
- **Tag mutation vulnerability:** `AddTagsToResource` modifies tag slice directly — concurrent `ListTagsForResource` could see partial updates.

### Missing Features
- `RestoreDBInstanceFromDBSnapshot` / `RestoreDBInstanceToPointInTime`
- `CopyDBSnapshot`
- Global database operations
- Automated backup management / retention policies
- Multi-AZ failover simulation
- Performance Insights, database activity streams
- IAM database authentication
- RDS Proxy, Blue/Green deployments

---

## EC2

### Resource Leaks
- **ENI not cleaned on instance termination:** `TerminateInstances()` only changes state to TERMINATED but does NOT delete associated ENIs. Long-running servers accumulate orphaned ENI objects.
- **No secondary private IP cleanup:** `UnassignPrivateIPAddresses()` removes IPs from slice but doesn't reset allocation indices. `nextPrivateIPIndex` only increments.

### Bugs
- **No cascade cleanup for subnet/VPC deletion:** `DeleteSubnet()` and `DeleteVpc()` don't verify or clean up dependent resources (instances, ENIs, security groups, route tables).
- **Tag operations on non-existent resources succeed silently.**

---

## ECS

### Resource Leaks (Critical)
- **Docker container leak on error:** If container creation succeeds but `ContainerStart()` fails, the container is never removed.
- **Multi-container tracking bug:** `containers` map uses `task.TaskArn` as key, but multiple containers per task share the same key — only last container tracked. `StopTask()` only stops the last container.
- **No cascade cleanup on cluster deletion:** Docker containers continue running after ECS cluster deletion.

### Memory Leaks
- **Task definition revision accumulation:** All revisions stored indefinitely — no cleanup/archival. Memory grows linearly with updates.

---

## Step Functions

### Memory Leaks
- **Execution history unbounded:** History events appended indefinitely with no limit. Long-running executions (>5K transitions) accumulate 10–20MB per execution. AWS limits to ~100K events.
- **Tombstone cleanup only on normal exit:** Deleted state machines accumulate tombstones in `deletedExecs` map indefinitely.

### Race Conditions
- **Tag handler lock release before read:** `removeTags` releases RLock before reading tag data — use-after-free if concurrent delete.

### Performance
- **Quadratic goroutine explosion:** Deeply nested Parallel+Map states can spawn 2^N goroutines with no bound.
- **Unbounded Map state semaphore:** No maximum for `MaxConcurrency` — ASL can allocate arbitrarily large channel buffers.

### Missing Features
- Activity task polling
- Synchronous Express Execution Mode
- State machine versioning
- CloudWatch Logs / X-Ray integration

---

## Kinesis

### Memory Leaks
- **Records never cleaned by retention period:** Records accumulate indefinitely despite `RetentionPeriod` field. AWS enforces 24-hour default.
- **FIS throughput faults only cleaned per-stream:** No global cleanup — rapidly created/deleted streams leak fault entries.

### Performance
- **Shard trimming O(n) on every record at capacity:** `shard.Records[1:]` creates a new slice allocation for every `PutRecord` after 10K limit. Should use ring buffer.
- **Linear sequence position search:** `findSequencePosition()` does O(n) scan through up to 10K records — should use binary search.

### Missing Features
- Retention period enforcement
- Enhanced monitoring metrics (accepted but unused)
- Full `SubscribeToShard` streaming response
- Resharding status tracking

---

## CloudWatch Logs

### Memory Leaks
- **Unbounded event storage:** Log streams grow indefinitely. No max events per stream, no total log group size limits, no age-based cleanup based on retention policy.
- **Subscription filters stored indefinitely:** No cleanup on log group deletion.

### Bugs
- **Wrong query statistics:** `RecordsMatched` set to `len(allEvents)` after timestamp filtering — should track actual regex-matched records.
- **Filter pattern matching too simple:** Uses `strings.Contains()` — AWS uses wildcard patterns like `[ERROR]` syntax.

### Race Conditions
- **TOCTOU in `StartQuery`:** RLock released, query executed unprotected, then write lock acquired. `b.events` could change between release and acquire.

### Missing Features
- Retention policy not enforced (accepted but never applied)
- No metric filters / CloudWatch Metrics integration
- Tags lost on persistence restore (stored separately from LogGroup struct)
- Incomplete Insights query engine (no aggregation functions)
- Single log group queries only (array parameter ignored)

---

## CloudFormation

### Memory Leaks (Critical)
- **Resources map not deleted on stack deletion:** `DeleteStack()` iterates resources to delete them but never calls `delete(b.resources, stack.StackID)` — every deleted stack leaks its resources map entry.
- **`stackIDIndex` never cleaned:** Deleted stack IDs accumulate indefinitely.
- **Changeset map not cleaned on stack deletion.**
- **Drift detections never pruned:** `driftDetections` map grows unbounded.

### Bugs
- **No rollback on provisioning failure:** `provisionResources` fails but doesn't delete partial resources already created.

### Missing Features
- No nested stack support
- Change set diffs don't contain actual change details
- Only ~161 of 350+ AWS resource types supported
- Stack policies accepted but never enforced

---

## Cognito IDP

### Bugs
- **No confirmation code validation:** `ConfirmSignUp` accepts any non-empty string as code.
- **No password validation on ConfirmSignUp.**

### Memory Leaks
- **No refresh token tracking/validation:** Tokens issued but never stored for revocation.
- **Users accumulate indefinitely:** No archival or deletion mechanism.

### Missing Features (Major)
- Refresh token flow (`REFRESH_TOKEN_AUTH`)
- MFA support (TOTP, SMS, email OTP)
- Challenge flows (`RespondToAuthChallenge`)
- Device tracking
- Group management (`CreateGroup`, `AdminAddUserToGroup`)
- Identity providers (SAML, OAuth, OIDC)
- Custom user attributes
- Pre/post-auth Lambda triggers

---

## Firehose

### Bugs
- **Buffer overflow:** `rec[len(rec)-1]` accessed without checking if `rec` is empty — will panic on empty byte slice.
- **Lambda transform errors silently return original records:** Failed transformations go unnoticed.

### Race Conditions
- **Unlocked state during flush:** `PutRecord()` unlocks mutex BEFORE calling `flushStream()` — concurrent modifications possible during flush.
- **`bufferSizeBytes` increment not atomic:** Concurrent puts may lose size increments, causing flush threshold misses.

### Missing Features
- Only S3 destinations — no HTTP endpoint, Splunk, DataDog
- No record format conversion
- No dynamic partitioning

---

## Route 53

### Bugs
- **DNS record leak on zone delete:** If DNS registrar is nil at creation but set later, records created without registrar can't be cleaned.
- **Health check status hardcoded to "Healthy":** No way to test failover scenarios.

### Performance
- **`ListResourceRecordSets` full sort every call:** O(n log n) for potentially large zones.
- **Zone ID generation expensive:** `rand.Read()` called per character in loop.

### Missing Features
- Only A and CNAME have DNS registration — AAAA, MX, NS, SRV, TXT, CAA, PTR accepted but not registered
- Weighted / Geolocation / Latency-based routing stored but not enforced
- No traffic policy support
- No query logging
- No VPC associations for private zones

---

## AppSync

### Bugs
- **Nil pointer dereference in data source lookup:** No nil check after map lookup in `graphql.go` — will panic if resolver references non-existent data source.
- **Schema re-parsed on every GraphQL execution:** `gqlparser.LoadSchema()` called every time even if schema unchanged. Should cache parsed schema.
- **VTL template errors silently return original value.**

### Missing Features
- HTTP / OpenSearch data source support (defined in models but not implemented)
- WebSocket subscription support
- Query result caching
- Resolver pipeline support
- Batch/async Lambda invocation
- X-Ray tracing integration

---

## ElastiCache

### Resource Leaks
- **Miniredis instance not closed on all error paths:** If `CreateClusterWithOptions` fails after miniredis started (e.g., parameter group check), the Redis instance leaks ports and memory.

### Bugs
- **Resource cleanup race in `CreateClusterWithOptions`:** Nested lock acquired after initial operation; if lock acquisition fails between unlock and `DeleteCluster`, double-delete error possible.
- **Potential nil pointer in `ListTagsForResource`:** Multiple `clone()` calls on potentially nil Tags.

### Missing Features
- No automatic failover / primary-replica promotion
- No multi-AZ support
- No cluster scaling (horizontal or vertical)
- No online resharding
- No event notifications
- No parameter group validation against family
- No engine version upgrades
- No maintenance window support

---

## SSM (Systems Manager)

### Memory Leaks
- **Unbounded parameter history:** Every `PutParameter` appends a history entry with no size limit. AWS SSM limits to 100 versions per parameter.
- **Expired commands never cleaned:** Commands have `ExpiresAfter` but no cleanup mechanism — `b.commands` and `b.commandInvocations` maps grow indefinitely.
- **Document version accumulation:** Every `UpdateDocument` appends to version list with no limit.

### Bugs
- **Command status hardcoded to "Success":** Never changes regardless of execution.

### Missing Features
- Parameter version limit enforcement (AWS: 100 max)
- Automatic command expiry cleanup
- Parameter labels
- Parameter change history with change reason

---

## SES

### Bugs (Critical)
- **Race condition in `VerifyEmailIdentity`:** `b.mu.Lock()` and `b.mu.Unlock()` without `defer`. If an error occurs before Unlock, the mutex is permanently held — **deadlocks the entire SES service**.

### Memory Leaks
- **Unbounded email storage:** `b.emails` slice grows indefinitely with every `SendEmail` — no size limit, no cleanup, no TTL. Persistence snapshot grows unbounded.

### Performance
- **O(n) `GetEmailByID`:** Linear search through all emails. Should use map for O(1) lookup.

### Missing Features
- Email templates (Create/Update/Get/List)
- File attachments
- Configuration sets
- Bounce/complaint handling
- Email authentication (DKIM/SPF)
- Suppression list
- Send statistics / quotas

---

## AWS Config

### Bugs
- **No recorder state validation:** `StartConfigurationRecorder()` doesn't verify delivery channel exists. AWS requires at least one before starting.

### Missing Features (Extensive)
- Config rules evaluation engine
- Aggregated view / aggregators
- Conformance packs
- Compliance tracking
- Configuration items / resource config history
- Remediation
- Resource recording group filtering

---

## API Gateway

### Bugs
- **Resource tree orphaning:** Deleting parent resources leaves children in `d.resources` as unreachable orphans.
- **Method integration not mandatory:** Allows methods without integration — AWS returns error on invocation.

### Missing Features
- Request/response models and schemas
- Request validators enforcement in proxying
- Authorizer invocation (created but never called)
- CORS support / gateway responses
- Throttling / rate limiting
- Usage plans / API keys
- Caching configuration
- Custom domain names
- VPC links
- OpenAPI/Swagger import

---

## ECR

### Bugs
- **Silent JSON parsing error:** `json.Unmarshal` error explicitly ignored — malformed requests produce empty/default responses instead of validation errors.

### Missing Features (Critical)
- No image storage or management (PutImage, GetImage, ListImages, DescribeImages)
- No image layer tracking
- No image lifecycle policies
- No image scanning
- No tag mutability enforcement
- ECR is essentially non-functional for actual image operations

---

## Redshift

### Bugs
- **Missing cluster state transitions:** Clusters created with hardcoded `"available"` status — never transition through creating → available → deleting lifecycle.
- **Unsafe DNS registrar cleanup:** `SetDNSRegistrar` uses Lock/Unlock without defer.

### Missing Features
- Cluster snapshots (Create/Restore/Delete/Describe)
- Multiple node type support (always single hardcoded LEADER)
- Parameter group management
- Enhanced VPC routing
- Cluster security groups

---

## OpenSearch

### Bugs
- **Silent error suppression in tag operations:** `AddTags`, `RemoveTags`, and `ListTags` all silently ignore errors — client receives success even when domain doesn't exist.
- **Hardcoded domain config:** `DescribeDomainConfig` returns fake values, not actual domain settings.

### Performance
- **O(n) domain lookup by ARN:** `findDomainByARN()` linear scan. Should use ARN → Domain map.
- **TOCTOU in `ListDomainNames`:** Gets list then describes each separately — domain could be deleted between calls.

---

## Batch

### Missing Features (Critical)
- **Job execution non-functional:** `SubmitJob` returns dummy response, `ListJobs`/`DescribeJobs` return empty. No job tracking, status transitions, or actual execution.
- **No state persistence:** No `Snapshot()`/`Restore()` — all data lost on restart.
- Job queue priority scheduling, dependencies, array jobs, timeouts, retry logic all missing.

---

## Shared Packages

### `pkgs/persistence` — Data Loss Risk
- **No fsync before rename:** `FileStore.Save()` writes to temp file, closes, and renames without `Sync()`. Power loss can corrupt or lose data.

### `pkgs/lockmetrics` — Metric Leak
- **Prometheus metrics never unregistered:** Each `RWMutex` creates metrics; `Close()` only removes from tracking map. Long-running apps with dynamic resources leak thousands of metric instances.
- **`WithLabelValues()` hot-loop overhead:** Hash lookup on every lock/unlock. Should cache label values.

### `pkgs/handler` — Status Code Bug
- **ResponseWriter:** `statusCode` initialized to 200, making the `if statusCode == 0` check in `Write()` dead code.

### `pkgs/events` — Listener Panic Risk
- **No panic recovery:** If a listener panics, the entire `Emit()` crashes. Should add recover wrapper.
- **Inefficient unsubscribe:** `removeByID` uses `append([:i], [i+1:]...)` — should use in-place copy.

---

## Summary Matrix

| Service | Resource Leaks | Memory Leaks | Race Conditions | Performance Issues | Missing Features |
|---------|:-:|:-:|:-:|:-:|:-:|
| **S3** | ⚠️ | ⚠️ | ⚠️ | ⚠️ | 🔴 |
| **DynamoDB** | — | 🔴 | ⚠️ | ⚠️ | ⚠️ |
| **SQS** | — | 🔴 | ⚠️ | — | ⚠️ |
| **SNS** | 🔴 | ⚠️ | ⚠️ | ⚠️ | 🔴 |
| **Lambda** | 🔴 | 🔴 | 🔴 | ⚠️ | 🔴 |
| **IAM** | — | ⚠️ | ⚠️ | ⚠️ | ⚠️ |
| **STS** | — | 🔴 | ⚠️ | — | ⚠️ |
| **KMS** | — | ⚠️ | — | ⚠️ | ⚠️ |
| **Secrets Manager** | — | 🔴 | ⚠️ | ⚠️ | ⚠️ |
| **EventBridge** | 🔴 | ⚠️ | ⚠️ | — | ⚠️ |
| **CloudWatch** | — | 🔴 | 🔴 | — | ⚠️ |
| **RDS** | — | — | ⚠️ | ⚠️ | 🔴 |
| **EC2** | 🔴 | ⚠️ | — | — | ⚠️ |
| **ECS** | 🔴 | ⚠️ | ⚠️ | — | ⚠️ |
| **Step Functions** | — | 🔴 | ⚠️ | ⚠️ | ⚠️ |
| **Kinesis** | — | 🔴 | — | 🔴 | ⚠️ |
| **CW Logs** | — | 🔴 | ⚠️ | ⚠️ | ⚠️ |
| **CloudFormation** | — | 🔴 | — | ⚠️ | 🔴 |
| **Cognito IDP** | — | ⚠️ | — | ⚠️ | 🔴 |
| **Firehose** | — | ⚠️ | ⚠️ | — | ⚠️ |
| **Route 53** | ⚠️ | — | — | ⚠️ | ⚠️ |
| **AppSync** | — | — | ⚠️ | ⚠️ | 🔴 |
| **ElastiCache** | 🔴 | ⚠️ | ⚠️ | ⚠️ | 🔴 |
| **SSM** | — | 🔴 | — | ⚠️ | ⚠️ |
| **SES** | — | 🔴 | 🔴 | ⚠️ | 🔴 |
| **Config** | — | — | — | — | 🔴 |
| **API Gateway** | ⚠️ | — | — | ⚠️ | 🔴 |
| **ECR** | — | — | ⚠️ | — | 🔴 |
| **Redshift** | — | — | ⚠️ | — | 🔴 |
| **OpenSearch** | — | — | ⚠️ | ⚠️ | ⚠️ |
| **Batch** | — | ⚠️ | — | ⚠️ | 🔴 |
| **Shared Pkgs** | — | ⚠️ | — | ⚠️ | — |

**Legend:** 🔴 Critical | ⚠️ Moderate | — None/Minimal

### Priority Fix Order

**P0 — Immediate (production-breaking):**
1. SES `VerifyEmailIdentity` deadlock (missing defer on mutex unlock)
2. Lambda container process leaks on timeout
3. ECS multi-container tracking bug (only last container tracked/stopped)
4. CloudFormation `DeleteStack` resources map memory leak
5. Firehose empty record panic (`rec[len(rec)-1]` without length check)
6. Persistence `FileStore.Save()` missing fsync

**P1 — High (data loss / resource exhaustion):**
1. STS session tokens stored forever (no expiration cleanup)
2. Kinesis records never expire despite retention period
3. CloudWatch Logs unbounded event storage
4. SSM unbounded parameter history growth
5. Secrets Manager unbounded version accumulation
6. EventBridge goroutine leak in delivery (no shutdown timeout)
7. SNS HTTP delivery goroutine leak
8. EC2 ENI not cleaned on instance termination

**P2 — Medium (correctness / parity):**
1. SQS deduplication ID accumulation
2. DynamoDB transaction pending token cleanup
3. CloudWatch composite alarm circular dependency detection
4. CloudWatch alarm action context leak
5. Cognito IDP missing auth flows (MFA, challenges, refresh tokens)
6. API Gateway authorizer invocation
7. Batch job execution implementation

**P3 — Low (feature gaps for LocalStack parity):**
1. S3 bucket policy enforcement
2. S3 encryption implementation
3. DynamoDB PartiQL integration
4. SNS FIFO topic support
5. Route 53 weighted/geo routing enforcement
6. AppSync subscription support
7. ECR image storage and management
