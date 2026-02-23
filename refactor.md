# Refactor: AWS Realism Gaps

Audit of Gopherstack against LocalStack free-tier parity. Items are grouped by type: **realism gaps** (response format/behavior doesn't match real AWS) and **missing features** (action not implemented at all).

---

## DynamoDB

### Realism Gaps

- [ ] **BatchGetItem/BatchWriteItem never return UnprocessedItems** — Always returns an empty map. AWS returns unprocessed items when the 16 MB response limit is hit or when provisioned throughput is exceeded. Should simulate at least the size-limit case.
- [ ] **No table status lifecycle** — CreateTable returns ACTIVE immediately. AWS transitions through CREATING → ACTIVE. LocalStack simulates this. Same applies to DeleteTable (should go through DELETING). Add a configurable tick duration via env var / CLI flag (e.g. `--dynamodb-create-delay` / `DYNAMODB_CREATE_DELAY`, default `0s` for instant creation). A value of `0` skips the lifecycle entirely for fast unit tests; a non-zero value (e.g. `500ms`, `2s`) enables the CREATING → ACTIVE transition after the specified delay to simulate realistic behavior.

### Missing Features

- [x] **UpdateTable** — Cannot modify provisioned throughput, add/remove GSIs, or toggle streams on an existing table.
- [x] **TagResource / UntagResource / ListTagsOfResource** — No tagging support for tables.
- [ ] **PartiQL (ExecuteStatement, BatchExecuteStatement)** — LocalStack free tier supports this.

### Out of Scope

- **Global Tables (CreateGlobalTable, UpdateGlobalTable, DescribeGlobalTable)** — Not worth implementing. LocalStack free tier only stubs the v2019 API and doesn't replicate streams across regions. Gopherstack's region-partitioned storage already lets you create same-named tables in different regions independently. Real replication, conflict resolution, and eventual consistency can't be meaningfully simulated in-memory. Teams that need Global Tables test against real AWS.

---

## S3

### Realism Gaps

- [ ] **Conditional requests not honored** — ETag and Last-Modified are returned in responses but `If-None-Match`, `If-Modified-Since`, `If-Match`, `If-Unmodified-Since` headers on GET/HEAD are ignored. Should return 304 Not Modified or 412 Precondition Failed as appropriate.
- [ ] **PutBucketAcl / GetBucketAcl no-op** — PUT returns 200 OK but discards the ACL; GET returns 501. Should at minimum store and return canned ACLs.
- [ ] **ListObjectsV2 KeyCount field** — Verify KeyCount reflects the actual count of returned keys (not the truncated total).

### Missing Features

- [ ] **Presigned URLs** — No support for generating or honoring presigned GET/PUT URLs. LocalStack supports this.
- [ ] **CopyObject response metadata** — CopyObject exists but verify it returns `CopyObjectResult` XML with `ETag` and `LastModified` matching the new copy.
- [ ] **ListMultipartUploads / ListParts** — Cannot enumerate in-progress multipart uploads or their parts.
- [ ] **Bucket Tagging (GetBucketTagging / PutBucketTagging / DeleteBucketTagging)** — Not implemented.
- [ ] **CORS configuration (GetBucketCors / PutBucketCors / DeleteBucketCors)** — Not implemented.

---

## SQS

### Realism Gaps

- [x] **MessageSystemAttribute `ApproximateFirstReceiveTimestamp` / `SentTimestamp`** — Verify these are populated on ReceiveMessage responses. AWS always includes them.
- [x] **ReceiveMessage MaxNumberOfMessages clamping** — AWS caps at 10; verify Gopherstack enforces this.
- [x] **Queue ARN in GetQueueAttributes** — Verify `QueueArn` attribute is returned and correctly formatted.

### Missing Features

- [x] **Redrive Policy / Dead Letter Queue** — RedrivePolicy attribute not handled. Messages that exceed maxReceiveCount are never moved to a DLQ.
- [ ] **ChangeMessageVisibilityBatch** — Batch version of ChangeMessageVisibility not implemented.
- [ ] **TagQueue / UntagQueue / ListQueueTags** — No queue tagging support.

---

## SNS

### Realism Gaps

- [ ] **Message filter policies stored but not evaluated** — FilterPolicy is saved on the subscription but Publish does not filter messages through it. All subscribers receive all messages.
- [ ] **Publish response MessageId format** — Verify MessageId is a valid UUID matching AWS format.
- [ ] **Subscription confirmation flow** — SubscribeResult should set SubscriptionArn to `"PendingConfirmation"` for HTTP/HTTPS protocols until ConfirmSubscription is called.

### Missing Features

- [x] **SNS → SQS delivery** — Publishing to a topic with SQS subscriptions does not enqueue messages into the target queue. This is the most critical cross-service gap.
- [ ] **SNS → Lambda invocation** — Not implemented.
- [x] **ConfirmSubscription** — No action handler for confirming HTTP/HTTPS endpoint subscriptions.
- [ ] **TagResource / UntagResource / ListTagsForResource** — No tagging support.

---

## IAM

### Realism Gaps

- [ ] **Policy document not validated** — CreatePolicy accepts any string as the policy document. AWS validates JSON structure and IAM policy grammar.
- [ ] **CreateUser / CreateRole missing default fields** — Verify responses include `CreateDate`, `Path`, and `Arn` in the correct format.

### Missing Features

- [x] **ListAttachedRolePolicies** — Cannot list which policies are attached to a role.
- [x] **ListAttachedUserPolicies** — Cannot list which policies are attached to a user.
- [x] **GetPolicy / GetPolicyVersion** — Cannot retrieve a stored policy document back.
- [ ] **DetachUserPolicy / DetachRolePolicy** — Verify these are implemented (may exist but not visible in dispatch).
- [ ] **ListGroupsForUser / ListUserPolicies / ListRolePolicies** — Missing query operations.

---

## STS

### Realism Gaps

- [ ] **AssumeRole credential format** — Verify AccessKeyId starts with `ASIA` (temporary credentials) and SessionToken is a plausible-length string. AWS SDKs may validate these prefixes.

### Missing Features

- [ ] **GetSessionToken** — Common action for generating temporary credentials without role assumption.
- [ ] **GetAccessKeyInfo** — Returns the account ID for an access key.
- [ ] **DecodeAuthorizationMessage** — Decodes error messages from encoded authorization failures.

---

## KMS

### Realism Gaps

- [ ] **Encrypt output CiphertextBlob encoding** — Verify the response returns base64-encoded ciphertext matching AWS format (prefixed with key metadata).
- [ ] **GenerateDataKey response format** — Verify both `Plaintext` and `CiphertextBlob` are returned as base64, and `KeyId` is the full ARN.

### Missing Features

- [ ] **CreateGrant / RetireGrant / ListGrants / RevokeGrant** — No grant management. LocalStack free tier supports basic grants.
- [ ] **GetKeyPolicy / PutKeyPolicy** — Cannot manage key policies.
- [ ] **ScheduleKeyDeletion / CancelKeyDeletion** — Cannot delete keys with a waiting period.
- [ ] **DisableKey / EnableKey** — Cannot toggle key state.

---

## Secrets Manager

### Realism Gaps

- [ ] **DeleteSecret without ForceDeleteWithoutRecovery** — AWS defaults to a 7-30 day recovery window. Verify Gopherstack respects `RecoveryWindowInDays` vs `ForceDeleteWithoutRecovery` flag behavior.
- [ ] **DescribeSecret VersionIdsToStages** — Verify the response includes the full version-to-stage mapping.

### Missing Features

- [ ] **TagResource / UntagResource** — No secret tagging support.
- [ ] **RotateSecret** — Cannot configure or trigger rotation.
- [ ] **GetResourcePolicy / PutResourcePolicy / DeleteResourcePolicy** — No resource policy management.

---

## SSM Parameter Store

### Realism Gaps

- [ ] **GetParametersByPath recursive flag** — Verify `Recursive` parameter properly returns nested path hierarchies (e.g., `/app/db/host` when path is `/app`).
- [ ] **DescribeParameters filtering** — Verify `ParameterFilters` are actually applied (Name, Type, KeyId filters).

### Missing Features

- [ ] **LabelParameterVersion / GetParametersByPath with labels** — Cannot attach labels to parameter versions.
- [ ] **AddTagsToResource / RemoveTagsFromResource / ListTagsForResource** — No parameter tagging.

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

- [x] **Account ID inconsistency across services** — STS uses `123456789012` (`sts/models.go:10`), every other service uses `000000000000` (`iam/models.go:12`, `kms/models.go:10`, `secretsmanager/models.go:10`, `sns/backend.go:27`, `sqs/types.go:10`). Unify to a single value injected from `cli.go` via env var `ACCOUNT_ID` (default `000000000000`).
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
- [ ] **S3 Event Notifications → SQS/SNS/Lambda** — Not simulated.
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

## Summary by Priority

| Priority | Item | Service |
|----------|------|---------|
| **Critical** | Add `ACCOUNT_ID` env var, unify all services to use it | All |
| **Critical** | Pipe centralized `REGION` default into all service backends | All |
| **Critical** | SNS → SQS delivery (event bus) | SNS / SQS |
| **Critical** | ARN regions hardcoded, ignoring request region | KMS / SecretsManager / SNS / SQS / DDB Streams |
| High | No region extraction for SNS, SQS, KMS, Secrets Manager | All |
| High | UpdateTable | DynamoDB |
| High | Presigned URLs | S3 |
| High | ListAttachedRolePolicies / ListAttachedUserPolicies | IAM |
| High | Redrive Policy / DLQ | SQS |
| High | Filter policy evaluation | SNS |
| High | DynamoDB table ARN / S3 bucket ARN never generated | DynamoDB / S3 |
| Medium | Conditional request headers (If-None-Match etc.) | S3 |
| Medium | UnprocessedItems in batch ops | DynamoDB |
| Medium | GetSessionToken | STS |
| Medium | ConfirmSubscription | SNS |
| Medium | Grants (CreateGrant etc.) | KMS |
| Medium | Policy document validation | IAM |
| Medium | TagResource across all services | All |
| Medium | Region fallback chain not standardized | All |
| Medium | S3 buckets allow duplicate names across regions (should be globally unique) | S3 |
| Medium | SNS/SQS/KMS/SecretsManager not partitioned by region | SNS / SQS / KMS / SecretsManager |
| Low | Table status lifecycle (CREATING → ACTIVE) | DynamoDB |
| Low | PartiQL support | DynamoDB |
| Low | CORS configuration | S3 |
| Low | ScheduleKeyDeletion | KMS |
| Low | RotateSecret | Secrets Manager |
| Low | LabelParameterVersion | SSM |
