# Refactor: AWS Realism Gaps

Audit of Gopherstack against LocalStack free-tier parity. Items are grouped by type: **realism gaps** (response format/behavior doesn't match real AWS) and **missing features** (action not implemented at all).

---

## DynamoDB

### Realism Gaps

- [ ] **BatchGetItem/BatchWriteItem never return UnprocessedItems** — Always returns an empty map. AWS returns unprocessed items when the 16 MB response limit is hit or when provisioned throughput is exceeded. Should simulate at least the size-limit case.
- [ ] **No table status lifecycle** — CreateTable returns ACTIVE immediately. AWS transitions through CREATING → ACTIVE. LocalStack simulates this. Same applies to DeleteTable (should go through DELETING). Add a configurable tick duration via env var / CLI flag (e.g. `--dynamodb-create-delay` / `DYNAMODB_CREATE_DELAY`, default `0s` for instant creation). A value of `0` skips the lifecycle entirely for fast unit tests; a non-zero value (e.g. `500ms`, `2s`) enables the CREATING → ACTIVE transition after the specified delay to simulate realistic behavior.
- [ ] **DescribeTable missing fields** — Response (`dynamodb/table_ops.go:275-289`) is missing: `CreationDateTime`, `TableSizeBytes`, `TableId`, `TableArn`, `BillingModeSummary`, `LatestStreamArn` (when streams enabled). `ItemCount` is present but the others are not returned.

### Missing Features

- [ ] **PartiQL (ExecuteStatement, BatchExecuteStatement)** — LocalStack free tier supports this.

### Out of Scope

- **Global Tables (CreateGlobalTable, UpdateGlobalTable, DescribeGlobalTable)** — Not worth implementing. LocalStack free tier only stubs the v2019 API and doesn't replicate streams across regions. Gopherstack's region-partitioned storage already lets you create same-named tables in different regions independently. Real replication, conflict resolution, and eventual consistency can't be meaningfully simulated in-memory. Teams that need Global Tables test against real AWS.

---

## S3

### Realism Gaps

- [ ] **Conditional requests not honored** — ETag and Last-Modified are returned in responses but `If-None-Match`, `If-Modified-Since`, `If-Match`, `If-Unmodified-Since` headers on GET/HEAD are ignored. Should return 304 Not Modified or 412 Precondition Failed as appropriate.
- [ ] **PutBucketAcl / GetBucketAcl no-op** — PUT returns 200 OK but discards the ACL (`s3/object_ops.go:70`); GET returns 501. Should at minimum store and return canned ACLs.
- [ ] **Content-Encoding / Content-Disposition not preserved** — These headers are not extracted from PutObject, not stored, and not returned on GetObject. AWS round-trips all standard HTTP metadata headers.
- [ ] **ListObjects v1 does not handle Delimiter** — `backend_memory.go:648-720` ignores the `Delimiter` parameter and never returns `CommonPrefixes`. ListObjectsV2 handles this correctly but v1 does not.
- [ ] **DeleteObjects no max key validation** — No check that input has at most 1000 keys. AWS rejects requests exceeding this limit.
- [ ] **PutObject Content-MD5 not validated** — `Content-MD5` header is not extracted or verified against actual body hash. AWS returns `BadDigest` on mismatch.

### Missing Features

- [ ] **Presigned URLs** — No support for generating or honoring presigned GET/PUT URLs. LocalStack supports this.
- [ ] **ListMultipartUploads / ListParts** — Cannot enumerate in-progress multipart uploads or their parts.
- [ ] **Bucket Tagging (GetBucketTagging / PutBucketTagging / DeleteBucketTagging)** — Not implemented.
- [ ] **CORS configuration (GetBucketCors / PutBucketCors / DeleteBucketCors)** — Not implemented.

---

## SQS

### Realism Gaps

- [ ] **`MD5OfMessageAttributes` not computed** — Only `MD5OfBody` is calculated (`sqs/backend.go:59-64`). AWS also returns `MD5OfMessageAttributes` when message attributes are present. Field missing from Message struct.
- [ ] **FIFO MessageGroupId ordering not enforced** — `MessageGroupID` is stored on messages (`sqs/backend.go:268`) but messages with the same group ID are not guaranteed FIFO delivery. They're appended to a single queue list without per-group ordering.

### Missing Features

---

## SNS

### Realism Gaps

- [ ] **Message filter policies stored but not evaluated** — FilterPolicy is saved on the subscription (`sns/backend.go:166`) but Publish does not filter messages through it (`sns/backend.go:247-258`). All subscribers receive all messages.
- [ ] **Subscription confirmation flow** — SubscribeResult should set SubscriptionArn to `"PendingConfirmation"` for HTTP/HTTPS protocols until ConfirmSubscription is called. Currently `PendingConfirmation` field exists (`sns/models.go:19`) but is always `false`.
- [ ] **MessageStructure JSON not handled** — `MessageStructure: "json"` parameter is accepted but ignored (`sns/backend.go:233`). Should parse the message body as JSON keyed by protocol (e.g. `{"default": "...", "sqs": "..."}`) and deliver protocol-specific payloads.
- [ ] **Subscription protocol not validated** — No validation that `Protocol` is one of the valid values (`email`, `email-json`, `http`, `https`, `sqs`, `lambda`, `sms`). Any string is accepted.

### Missing Features

- [ ] **TagResource / UntagResource / ListTagsForResource** — No tagging support.

---

## STS

### Missing Features

- [ ] **GetAccessKeyInfo** — Returns the account ID for an access key.
- [ ] **DecodeAuthorizationMessage** — Decodes error messages from encoded authorization failures.

---

## KMS

### Realism Gaps

- [ ] **KeyMetadata missing fields** — `kms/models.go:45-59` is missing: `KeyManager` (CUSTOMER vs AWS), `Origin` (AWS_KMS vs EXTERNAL), `KeySpec` (SYMMETRIC_DEFAULT, RSA_2048, etc.), `EncryptionAlgorithms`. AWS SDKs may expect these in DescribeKey responses.

### Missing Features

- [ ] **CreateGrant / RetireGrant / ListGrants / RevokeGrant** — No grant management. LocalStack free tier supports basic grants.
- [ ] **GetKeyPolicy / PutKeyPolicy** — Cannot manage key policies.
- [ ] **ScheduleKeyDeletion / CancelKeyDeletion** — Cannot delete keys with a waiting period.
- [ ] **DisableKey / EnableKey** — Cannot toggle key state (only `EnableKeyRotation`/`DisableKeyRotation` exist, which are different operations).

---

## Secrets Manager

### Missing Features

- [ ] **TagResource / UntagResource** — No secret tagging support.
- [ ] **RotateSecret** — Cannot configure or trigger rotation.
- [ ] **GetResourcePolicy / PutResourcePolicy / DeleteResourcePolicy** — No resource policy management.

---

## SSM Parameter Store

### Realism Gaps

- [ ] **Parameter name validation missing** — No enforcement of hierarchy rules (no double slashes `//`), valid character set (alphanumeric, `.`, `-`, `_`, `/`), or max length (2048 chars).

### Missing Features

- [ ] **LabelParameterVersion / GetParametersByPath with labels** — Cannot attach labels to parameter versions.
- [ ] **AddTagsToResource / RemoveTagsFromResource / ListTagsForResource** — No parameter tagging.

---

## General / Cross-Cutting

### Realism Gaps

- [ ] **`x-amz-request-id` header not generated** — No request ID is set in HTTP response headers for any service. AWS returns this on every response and SDKs log it for debugging. Should generate a UUID per request via middleware.

---

## Region & Account Configuration

### Approach

Introduce a centralized config that all services receive at construction time. Both values should be configurable via CLI flags / env vars in `cli.go`, with sensible defaults:

```
--account-id / ACCOUNT_ID   (default: "000000000000")
--region     / REGION        (default: "us-east-1")       ← already exists
```

The `REGION` env var already exists (`cli.go:76`) and is passed to the AWS SDK config, but individual service backends ignore it and use their own hardcoded constants. The fix is:
1. Add `ACCOUNT_ID` as a new CLI flag / env var (default `000000000000`) in `cli.go`.
2. Pass both `accountID` and `defaultRegion` from the CLI config into every service backend constructor.
3. Remove all per-service `MockAccountID`, `accountID`, `MockRegion`, `region` constants and replace with the injected values.
4. Each service's ARN builder uses the injected account ID and the request-extracted region (falling back to the injected default region).

### Realism Gaps

- [ ] **ARN regions hardcoded to us-east-1** — KMS (`kms/backend.go:196`), Secrets Manager (`secretsmanager/backend.go:99`), SNS (`sns/backend.go:30`), SQS (`sqs/backend.go:69`), and DynamoDB Streams (`dynamodb/streams_ops.go:324-328`) all bake `us-east-1` into ARNs regardless of the region in the request. Should use request region with fallback to the centralized default from `cli.go`.
- [ ] **No region extraction for SNS, SQS, KMS, Secrets Manager** — DynamoDB and S3 parse region from the Authorization header SigV4 credential scope. SNS, SQS, KMS, and Secrets Manager skip this entirely and fall through to a hardcoded constant. A shared `extractRegionFromRequest` utility should be used by all services, falling back to the centralized default region.
- [ ] **DynamoDB Stream ARN uses wrong account ID** — Stream ARNs are built with `123456789012` (`dynamodb/streams_ops.go:324-328`) while table operations use whatever region comes from context. The stream ARN should derive from the injected account ID and the parent table's region.
- [ ] **Region fallback chain not standardized** — DynamoDB checks: Authorization header → `X-Amz-Region` header → default. S3 adds: body `LocationConstraint`. Other services don't check at all. Should converge on a single shared fallback chain where the final fallback is the centralized `REGION` config value.
- [ ] **S3 bucket ARNs never generated** — S3 never constructs `arn:aws:s3:::bucket-name` for buckets. Needed for policy statements and cross-service references.
- [ ] **No DynamoDB table ARN generated** — Tables track region but never produce `arn:aws:dynamodb:<region>:<account>:table/<name>`. Needed for IAM policy resource fields and stream ARN derivation.
- [ ] **S3 buckets are region-scoped but should be globally unique** — Storage is `map[region]map[name]*Bucket` (`s3/backend_memory.go:40`), which allows two buckets with the same name in different regions. Real AWS enforces **global uniqueness** — `CreateBucket` must reject a name that already exists in *any* region. `ListBuckets` already returns cross-region (correct), but `CreateBucket` / `HeadBucket` need a global name check.
- [ ] **SNS, SQS, KMS, Secrets Manager are single-region flat maps** — These services use `map[name]*Resource` with no region key. Resources cannot coexist across regions (they'd collide in the same map), and all ARNs are stamped `us-east-1`. To match AWS behavior, these should be partitioned by region like DynamoDB (`map[region]map[name]*Resource`) so that the same queue/topic/key name can exist independently in different regions.

---

## Cross-Service Integration

### Current Gaps

- [ ] **SNS → SQS message delivery** — Highest priority. Event-driven architectures depend on this.
- [ ] **DynamoDB Streams → Lambda trigger** — Not simulated (acknowledged as out of scope for now).
- [ ] **S3 Event Notifications → SQS/SNS/Lambda** — Not simulated, stub this but don't implement any Lambda features.
- [ ] **No cross-service ARN validation** — SNS subscriptions referencing SQS queue ARNs are never verified against actual SQS state. Same for SQS redrive policies referencing DLQ ARNs.

### Proposed Inter-Service Event Bus

To enable realistic cross-service communication without tightly coupling backends, introduce a lightweight internal event bus. This is how LocalStack wires services together and is the cleanest path to parity.

#### Architecture

```
┌─────────────┐     Publish()      ┌──────────────┐     Deliver()      ┌─────────────┐
│  SNS Backend│ ──────────────────► │  Event Bus   │ ──────────────────► │ SQS Backend │
└─────────────┘                    │  (in-memory)  │                    └─────────────┘
                                   │              │
┌─────────────┐   PutObject()      │  Fan-out by  │     Deliver()      ┌─────────────┐
│  S3 Backend │ ──────────────────► │  topic/type  │ ──────────────────► │ SNS Backend │
└─────────────┘                    │              │                    └─────────────┘
                                   │              │
┌─────────────┐   StreamRecord()   │              │     Deliver()      ┌─────────────┐
│ DDB Backend │ ──────────────────► │              │ ──────────────────► │  (future)   │
└─────────────┘                    └──────────────┘                    └─────────────┘
```

#### Design

1. **`EventBus` interface** — lives in a shared `internal/bus` package:
   ```go
   type Event struct {
       Source    string            // "sns", "s3", "dynamodb-streams"
       Type     string            // "sns:Publish", "s3:ObjectCreated:Put", "dynamodb:INSERT"
       Region   string
       SourceARN string           // ARN of the originating resource
       Payload  json.RawMessage   // service-specific envelope
   }

   type Subscriber func(ctx context.Context, event Event) error

   type EventBus interface {
       Publish(ctx context.Context, event Event)
       Subscribe(eventType string, fn Subscriber)
   }
   ```

2. **In-memory fan-out implementation** — channel-based, async, with optional configurable delivery delay to simulate real AWS latency. No persistence needed; matches the in-memory philosophy of the rest of the project.

3. **Service integration points** — each service opts in with minimal coupling:
   - **SNS Publish** → emits `sns:Publish` event. An SQS subscriber registered at startup checks if the target subscription endpoint is an SQS ARN, resolves it to the SQS backend, and calls `SendMessage` internally.
   - **S3 PutObject / DeleteObject** → emits `s3:ObjectCreated:*` / `s3:ObjectRemoved:*`. If bucket notification configuration exists, the bus routes to SNS/SQS.
   - **DynamoDB Streams** → already generates stream records. Emitting them onto the bus enables future Lambda trigger simulation.

4. **Wiring** — the top-level `cli.go` / `main` creates the bus and passes it to each service backend at construction time. Services that don't need it simply ignore it.

#### Benefits

- Services stay independently testable (bus is an interface, easily mocked).
- Adding new integrations (e.g., EventBridge, Lambda) means adding a new subscriber, not modifying existing backends.
- Matches how LocalStack routes events internally.
- No network overhead — direct function calls through the bus.

#### Implementation Order

1. Define `EventBus` interface and in-memory implementation in `internal/bus/`.
2. Wire SNS Publish → SQS delivery (resolves the critical gap).
3. Add SNS filter policy evaluation before delivery.
4. Wire S3 event notifications → SNS/SQS.
5. Wire DynamoDB Streams onto the bus for future consumers.

---

## Integration Test Coverage Gaps

Only DynamoDB, S3, and SSM have integration tests. Five services have **zero integration test coverage** — all their testing is unit-only via mocked backends. This matters because integration tests validate the full HTTP request → handler → backend → response chain against the real AWS SDK.

### Missing Integration Tests

- [ ] **SQS** — No integration tests. Add tests for: queue lifecycle, send/receive/delete message flow, visibility timeout enforcement, batch operations with partial failures, FIFO deduplication.
- [ ] **SNS** — No integration tests. Add tests for: topic lifecycle, subscribe/publish/unsubscribe flow, message attributes, batch publish, HTTP endpoint delivery.
- [ ] **STS** — No integration tests. Add tests for: AssumeRole via SDK, GetCallerIdentity, credential format validation.
- [ ] **KMS** — No integration tests. Add tests for: key lifecycle via SDK, encrypt/decrypt roundtrip, alias resolution, key rotation status.
- [ ] **Secrets Manager** — No integration tests. Add tests for: secret lifecycle via SDK, version management, delete/restore flow, binary secret handling.

### Existing Coverage Gaps

- [ ] **S3 Multipart Upload** — Only unit-tested. Add integration tests for: large file upload, concurrent part uploads, abort mid-upload, complete with wrong ETags.
- [ ] **S3 CopyObject** — Implemented but no dedicated integration test. Add test for: copy within bucket, copy across buckets, metadata preservation.
- [ ] **DynamoDB Scan** — Has query integration tests but scan-specific pagination and filter expression tests are thin.

---

## Dashboard UI Gaps

Backend features that are implemented but not fully exposed in the dashboard. DynamoDB is the gold standard (95% coverage); other services lag behind.

### SQS (60% dashboard coverage)

- [ ] **No message send UI** — Backend supports SendMessage but dashboard has no form to compose and send messages to a queue.
- [ ] **No message receive/peek UI** — Backend supports ReceiveMessage but dashboard cannot browse or peek at messages in a queue.
- [ ] **No queue detail view** — Queue attributes (visibility timeout, delay, retention period) are not shown in a dedicated detail page.

### SNS (55% dashboard coverage)

- [ ] **No subscription management UI** — Backend supports Subscribe/Unsubscribe but dashboard has no UI to add or remove subscriptions from a topic.
- [ ] **No publish message UI** — Backend supports Publish but dashboard has no form to send a test message to a topic.
- [ ] **Topic detail view empty** — Template exists but has no content; subscription list, publish form, and topic attributes are not rendered.

### KMS (40% dashboard coverage)

- [ ] **No key creation UI** — Backend supports CreateKey but dashboard only shows a list view with no create action.
- [ ] **No key detail view** — Cannot view key metadata, rotation status, or aliases for a key.
- [ ] **No encrypt/decrypt UI** — Backend supports Encrypt/Decrypt but not exposed in dashboard.

### Secrets Manager (50% dashboard coverage)

- [ ] **No create secret UI** — Backend supports CreateSecret but dashboard has no create form.
- [ ] **No secret update/delete UI** — Backend supports PutSecretValue/DeleteSecret but not exposed.
- [ ] **No version history view** — Cannot see previous secret versions or stage labels.

### SSM (75% dashboard coverage)

- [ ] **No parameter history viewer** — Backend supports GetParameterHistory but dashboard has no version history panel.

### STS (30% dashboard coverage)

- [ ] **No AssumeRole UI** — Backend supports AssumeRole but dashboard only shows GetCallerIdentity info.

---

## Summary by Priority

| Priority | Item | Service |
|----------|------|---------|
| **Critical** | Pipe centralized `REGION` default into all service backends | All |
| **Critical** | SNS → SQS delivery (event bus) | SNS / SQS |
| **Critical** | ARN regions hardcoded, ignoring request region | KMS / SecretsManager / SNS / SQS / DDB Streams |
| **Critical** | `x-amz-request-id` header missing on all responses | All |
| High | No region extraction for SNS, SQS, KMS, Secrets Manager | All |
| High | DescribeTable missing CreationDateTime, TableSizeBytes, TableArn | DynamoDB |
| High | Presigned URLs | S3 |
| High | Filter policy evaluation | SNS |
| High | DynamoDB table ARN / S3 bucket ARN never generated | DynamoDB / S3 |
| High | Integration tests for SQS, SNS, STS, KMS, Secrets Manager | All |
| High | SNS topic detail / subscription management UI | SNS |
| High | SQS message send/receive UI | SQS |
| High | KMS key creation + detail UI | KMS |
| High | Secrets Manager create/update/delete UI | Secrets Manager |
| Medium | Conditional request headers (If-None-Match etc.) | S3 |
| Medium | UnprocessedItems in batch ops | DynamoDB |
| Medium | ConfirmSubscription | SNS |
| Medium | MessageStructure JSON handling | SNS |
| Medium | Grants (CreateGrant etc.) | KMS |
| Medium | KeyMetadata missing fields (KeyManager, Origin, KeySpec) | KMS |
| Medium | TagResource across all services | All |
| Medium | Region fallback chain not standardized | All |
| Medium | S3 buckets allow duplicate names across regions (should be globally unique) | S3 |
| Medium | SNS/SQS/KMS/SecretsManager not partitioned by region | SNS / SQS / KMS / SecretsManager |
| Medium | MD5OfMessageAttributes not computed | SQS |
| Medium | FIFO MessageGroupId ordering not enforced | SQS |
| Medium | S3 Content-Encoding / Content-Disposition not preserved | S3 |
| Medium | ListObjects v1 Delimiter / CommonPrefixes | S3 |
| Medium | S3 Multipart / CopyObject integration tests | S3 |
| Medium | SSM parameter history viewer UI | SSM |
| Low | Table status lifecycle (CREATING → ACTIVE) | DynamoDB |
| Low | PartiQL support | DynamoDB |
| Low | CORS configuration | S3 |
| Low | DeleteObjects max 1000 key validation | S3 |
| Low | PutObject Content-MD5 validation | S3 |
| Low | SSM parameter name validation | SSM |
| Low | ScheduleKeyDeletion | KMS |
| Low | DisableKey / EnableKey | KMS |
| Low | RotateSecret | Secrets Manager |
| Low | LabelParameterVersion | SSM |
| Low | SNS subscription protocol validation | SNS |
| Low | STS AssumeRole UI | STS |
| Low | GetAccessKeyInfo | STS |
| Low | DecodeAuthorizationMessage | STS |
