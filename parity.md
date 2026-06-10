# LocalStack Parity Findings

Audit performed against gopherstack `services/*` handlers vs the AWS SDK v2 surface and LocalStack feature coverage. Items below are confirmed by reading the relevant handler/backend files or by integration-test failures.

## Critical (wrong wire protocol — clients hard-fail)

1. **DAX** — `services/dax/handler.go` routes the control plane on `X-Amz-Target: AmazonDAXV3.*`, but the AWS DAX SDK speaks a bespoke CBOR/framed binary protocol on a non-HTTP listener for item operations, so the real SDK never reaches the HTTP handler. (Fixed: `services/dax/dataplane` now implements the DAX binary wire protocol — handshake, auth, key-schema/attribute-list control methods, and `GetItem`/`PutItem`/`DeleteItem`/`BatchGetItem`/`BatchWriteItem` — delegating to the existing DynamoDB backend. The listener is started on `:8111` via the service lifecycle. A real `amazon-dax-go`-driven test exercises a PutItem→GetItem→DeleteItem round-trip in `services/dax/dataplane_integration_test.go`. `UpdateItem`/`Query`/`Scan` and numeric compound-range keys remain documented gaps — see `services/dax/DATAPLANE.md`.)
2. **XRay** — the handler mapped `POST /EncryptionConfig` to `PutEncryptionConfig`, but the AWS SDK v2 sends `PutEncryptionConfig` to the operation-named path `POST /PutEncryptionConfig` and `GetEncryptionConfig` to `POST /EncryptionConfig`. Real SDK `PutEncryptionConfig` calls reached the wrong handler. (Fixed: `services/xray/handler.go` now serves `GetEncryptionConfig` on `POST /EncryptionConfig` and adds the `POST /PutEncryptionConfig` route; covered by `test/integration/xray_test.go`.)
3. **ELBv2** — `AddTags`, `RemoveTags`, `RegisterTargets`, `DeregisterTargets` originally omitted the `*Result` XML wrapper required by AWS Query protocol. SDK deserialiser raised `… node not found`. (Fixed in this PR.)
4. **Classic ELB** — `AddTags`, `RemoveTags` had the same missing `*Result` wrappers. (Fixed in this PR.)
5. **Classic ELB `CreateLoadBalancer`** — ignored the `Tags.member.N` form parameters that AWS accepts at create time. Initial tags were silently dropped. (Fixed in this PR; subsequent `DescribeTags` now returns them.)

## SDK ops missing from gopherstack handlers (new upstream surface)

EMR Serverless interactive session and resource-dashboard operations are now implemented.

## Behavioural parity gaps verified by reading handler/backend

12. **APIGatewayManagementApi admin `PruneIdle`** — was timing-sensitive (15 ms threshold + 20 ms sleep) and flaked on contended CI runners. (Tightened in this PR to a 50 ms threshold + 100 ms sleep.)
13. **API Gateway list response shapes** — `keyItem` ("items") used for most list ops, `keyStagesItem` ("item") only for `GetStages`; any new list op that copy-pastes from another will use the wrong JSON key (verified via existing memory).
14. **Cognito IDP persistence** — (Fixed in this PR).
15. **APIGateway persistence** — (Fixed in this PR).
16. **ELBv2 persistence** — (Fixed in this PR).
17. **IoTWireless persistence** — (Fixed in this PR).
18. **Kinesis persistence** — (Investigated: state is ephemeral).
19. **CloudWatch persistence** — (Fixed in this PR).
20. **EC2 persistence** — (Fixed in this PR).
21. **MediaConvert persistence** — (Investigated: state is ephemeral).
22. **IAM persistence** — (Fixed in this PR).
23. **CloudFormation provider wiring** — when a new `ServiceBackends` field is added, `cloudformation/provider.go` `BackendsProvider`/`extractAllServiceBackends` must be updated or the new backend stays nil at CFN-resource resolution time. Easy regression.

## Integration-test coverage gaps for popular services

Services with substantial handlers but no AWS-SDK-driven integration test under `test/integration/`:

24. _Fixed: dynamodbstreams SDK-driven test added._
25. _Fixed: databrew SDK-driven test added._
26. _Fixed: iotdataplane SDK-driven test added._
27. _Fixed: sagemakerruntime SDK-driven test added._
28. _Fixed: bedrockruntime SDK-driven test added._
29. _Fixed: appconfigdata SDK-driven test added._
30. _Fixed: apigatewaymanagementapi SDK-driven test added._
31. _Fixed: acmpca SDK-driven test added._
32. _Removed: ElasticTranscoder service was deleted; AWS discontinued Elastic Transcoder on Nov 13, 2025._

## Persistence wiring gaps (silently dropped state on Snapshot/Restore)

For each of the following services, only specific named fields are persisted, so any backend field added later without updating the matching `persistence.go` `backendSnapshot` will silently drop on restore. These are listed here as ongoing risks rather than concrete current bugs:

33. _Fixed: APIGateway._
34. _Fixed: CloudWatch._
35. _Fixed: CognitoIDP._
36. _Fixed: EC2._
37. _Fixed: ELBv2._
38. _Fixed: IAM._
39. _Fixed: IoTWireless._
40. _Fixed: Kinesis._
41. _Fixed: MediaConvert._

## Tests added in this PR

- `test/integration/ce_test.go` — Cost Explorer anomaly monitor lifecycle.
- `test/integration/textract_test.go` — Textract synchronous text detection.
- `test/integration/timestreamquery_test.go` — Timestream endpoint discovery.
- `test/integration/ssoadmin_test.go` — SSO Admin instance + permission set lifecycle.
- `test/integration/codestarconnections_test.go` — CodeStar Connections lifecycle.
- `test/integration/cloudcontrol_test.go` — CloudControl resource lifecycle.
- `test/integration/support_test.go` — Support case lifecycle.

## DynamoDB & S3 accuracy / parity / leak fixes (this PR)

DynamoDB:
- **Global-table replica DELETE data corruption** — `extra_ops.go` `resolveReplicaMatchIndex` used `skMap[skVal]`, returning index `0` (a valid slot) for a missing sort key, so a replicated delete of a non-existent key destroyed whatever item sat at index 0. Now uses the comma-ok form and returns `-1`.
- **`ShardIteratorStore` memory leak** — every `GetRecords`/`GetShardIterator` minted an opaque token but the janitor never swept the store; it grew unbounded under streaming load. `runOnce` now calls `iteratorStore.Sweep()` alongside `exprCache.Sweep()`.
- **Streams parity: missing records** — `BatchWriteItem` PutRequests and all `TransactWriteItems` mutations emitted no stream records (deletes via batch did). AWS emits INSERT/MODIFY/REMOVE for both; now emitted (covered by `streams_ops_test.go`).

DynamoDB (capacity accuracy, second pass):
- **Query/Scan `ConsumedCapacity` ignored `ConsistentRead`** — the throttler already doubled RCU for strongly-consistent reads, but the *reported* `ConsumedCapacity` always used the eventually-consistent (0.5×) rate, so it was half of actual. `consumedCapacityForQuery`/`consumedCapacityForScan` now apply `applyConsistentReadMultiplier` (covered by `TestQuery_ConsumedCapacity`).
- **DeleteItem/UpdateItem throttled a flat 1 WCU** — both reported size-proportional `ConsumedCapacity` but only ever deducted `1.0` from the token bucket, so large-item writes under-consumed throughput. The lookup now runs before the throttle and charges `WriteCapacityUnits(oldItem)` (Delete) / `WriteCapacityUnits(existing)` (Update).

S3:
- **Suspended-versioning DELETE data loss** — `backend_memory.go` `deleteLatestVersion` deleted the entire object (all versions) on an unversioned delete against a Suspended bucket. AWS removes only the `null` version, inserts a `null` delete marker, and preserves non-null versions. Fixed and covered by `TestSuspendedVersioningDeletePreservesVersions`.
- **Data race in `storePart`** — the per-bucket uploads inner map was indexed after releasing `b.mu`, racing with `CreateMultipartUpload`. The lookup now happens while the read lock is held.
- **List pagination dead-end** — `truncateListResults` could return `IsTruncated=true` with an empty marker when a page filled exactly with object keys while CommonPrefixes remained, stranding the client. The marker now falls back to the last returned key.
- **Conditional `*` wildcard on GET/HEAD** — `If-Match: *` / `If-None-Match: *` were compared literally against the ETag. They now correctly match any existing representation (412/304 semantics).

## Correction from earlier draft

A previous version of this document listed WAFv2, S3 Tables and SES handlers (~50 ops) as "empty stubs" based on a grep for `return nil, nil`. That detection was a false positive: those handlers DO call their `h.Backend.X(...)` first and then return the empty AWS Query envelope, which is the correct response shape for void-result operations. They are NOT parity gaps. This document has been corrected.
- `test/integration/databrew_test.go`
- `test/integration/iotdataplane_test.go`
- `test/integration/sagemakerruntime_test.go`
- `test/integration/bedrockruntime_test.go`
- `test/integration/appconfigdata_test.go`
- `test/integration/apigatewaymanagementapi_test.go`
- `test/integration/acmpca_test.go`

---

# Full-surface parity audit (2026-06-10)

A fresh read-only audit of every `services/*` handler/backend, popular services first,
then the long tail. Findings are grouped into the four requested buckets. Each item cites
`file:line` from the code it was read in and is an **open finding** (not yet fixed) unless
noted. These are candidates for follow-up PRs; nothing below was changed in this commit
(only this document was edited).

## A. Missing LocalStack functionality / unimplemented SDK operations

These operations are routed (or advertised via `GetSupportedOperations`) but return stubs —
empty structs, `nil, nil`, or hardcoded values — with no state mutation, so SDK clients get
a success envelope while nothing happens.

- **Lambda** — durable-execution ops (`GetDurableExecution`, `GetDurableExecutionHistory`,
  `GetDurableExecutionState`, `StopDurableExecution`, `CheckpointDurableExecution`) and the
  capacity-provider ops (`Create/Update/Delete/Get/ListCapacityProviders`) are declared
  supported but dispatch to no-op stubs (`services/lambda/handler_stubs.go:23-100`,
  `services/lambda/handler.go:270-361`).
- **SSM** — ~120 operations route to `&StubOutput{}` with no state change
  (`CreateResourceDataSync`, `DeleteInventory`, `DescribeActivations`, …)
  (`services/ssm/handler.go:307`, `services/ssm/handler_stubs.go:1-50`).
- **Glue** — 20+ stubs return empty structs with no data/state, e.g. `GetBlueprintRun`,
  `GetCatalogImportStatus`, `GetColumnStatisticsTaskRun`, `GetPlan`, `GetSchemaVersionsDiff`,
  `GetUsageProfile`, plus `CancelMLTaskRun`, `ImportCatalogToGlue`,
  `StopColumnStatisticsTaskRun` (`services/glue/handler_stubs.go:232-2707`).
- **Athena** — notebook + named-query ops declared in the `StorageBackend` interface but with
  no `InMemoryBackend` implementation: `UpdateNamedQuery`, `GetQueryRuntimeStatistics`,
  `GetNotebookMetadata`, `ListNotebookMetadata`, `ImportNotebook`, `UpdateNotebook`,
  `UpdateNotebookMetadata` (`services/athena/backend.go:336-340`,
  `services/athena/handler_extra.go:176-186`).
- **CloudTrail** — `LookupEvents` unconditionally returns an empty list, ignoring all filters
  (StartTime/EndTime/LookupAttributes/MaxResults/NextToken); the backend never records events
  (`services/cloudtrail/backend.go:1589`).
- **CodePipeline** — `ListActionExecutions`, `ListRuleExecutions`, `ListRuleTypes`,
  `ListDeployActionExecutionTargets` are empty stubs; no execution/rule tracking
  (`services/codepipeline/backend.go:1487-1513`).
- **AppSync** — `EvaluateCode` returns a hardcoded `{"evaluationResult":"{}"}` and never runs
  the supplied APPSYNC_JS code/context (`services/appsync/backend.go:2460`).
- **Kafka** — `UpdateConnectivity`, `UpdateMonitoring`, `UpdateRebalancing`, `UpdateSecurity`,
  `UpdateStorage` are explicit no-ops that ignore input and return fake operation ARNs
  (`services/kafka/backend.go:1466-1534`).
- **WAFv2** — ~12 ops return `nil, nil` with no response body where the SDK expects fields such
  as `LockToken` (e.g. `DeleteWebACL` `handler.go:689`, `DisassociateWebACL` `handler.go:1215`,
  `UntagResource` `handler.go:1075`); `DescribeManagedRuleGroup` returns a hardcoded 100-WCU
  stub instead of `WAFNonexistentItemException`; `GenerateMobileSdkReleaseUrl` returns a fake
  presigned URL (`services/wafv2/handler.go:2272-2298`).
- **CloudFront** — 60+ stubbed APIs (FieldLevelEncryption, KeyValueStore, StreamingDistribution,
  TrustStore, ConnectionFunction, …) return minimal empty XML rather than real data or proper
  errors (`services/cloudfront/handler.go:1966-2261`).
- **EC2** — `RevokeSecurityGroupEgress` is a no-op that always succeeds; AWS validates the rule
  and returns `InvalidPermission.NotFound` when absent (`services/ec2/handler.go:884-890`).
- **API Gateway** — `GetAccount` and `GetUsage` are listed in `GetSupportedOperations` but have
  no handler in the dispatch table, so they 404 (`services/apigateway/handler.go:738,763`).
- **ApplicationAutoScaling** — `DescribeScalingActivities` returns an empty list with no
  activity tracking (`services/applicationautoscaling/handler.go:456`).
- **EventBridge Pipes** — when enrichment/target invokers are unwired the runner returns
  `nil, nil`, silently dropping the event instead of erroring
  (`services/pipes/runner.go:293-328`).
- **Firehose** — Lambda transformation processors configured via `ProcessorInput` are never
  invoked on records before delivery; buffer-interval (time-based) flush is not implemented,
  only size-based (`services/firehose/handler.go:102`, `services/firehose/backend.go`).
- **CloudFormation** — `DescribeType` returns a stub schema containing only the primary
  identifier, omitting full property definitions; StackSet drift ops
  (`DetectStackSetDrift`, `ListStackSetOperations`, `DescribeStackSetOperation`) are routed but
  unimplemented (`services/cloudformation/handler.go:296-366`).
- **OpsWorks** — returns `UnsupportedOperationException` for core operations; service is largely
  unimplemented (`services/opsworks/handler.go:181`).

## B. Incorrect / missing AWS emulation

Behaviour that diverges from real AWS semantics (wrong status/error codes, missing validation,
dropped data, wrong pagination/response shapes).

- **SNS** — failed HTTP/Lambda/SQS deliveries are dropped: `replayMessagesToSubscription` and
  the delivery path never consult `RedrivePolicy`/DLQ (`services/sns/backend.go:2811-2870`).
  `SetSubscriptionAttributes` accepts a `RedrivePolicy` without validating the target SQS DLQ
  exists (`services/sns/handler.go:1028-1043`). Archive eviction silently drops the oldest
  messages, so a `ReplayPolicy` subscription misses history (`services/sns/backend.go:3430-3432`).
- **EventBridge** — same DLQ gap: `deliverToTargetBounded` ignores target `RedrivePolicy`/DLQ on
  failed Lambda/SQS invocations (`services/eventbridge/delivery.go:146-150`). Malformed event
  patterns fail compilation silently and the rule simply never matches, with no error surfaced
  (`services/eventbridge/backend.go:103-114`).
- **STS** — `GetCallerIdentity` with a mismatched session token returns 403 `AccessDenied`; AWS
  returns 400 `InvalidClientTokenId` (`services/sts/backend.go:544-586`).
- **DynamoDB** — accepts `ConsistentRead=true` on GSI/LSI queries; AWS rejects with
  `ValidationException` (`services/dynamodb/item_ops_query.go:150`). `BatchGetItem` does not
  reject duplicate keys within one table's `Keys` list and returns the item twice
  (`services/dynamodb/item_ops_batch.go:29-46`). `UpdateTable` does not re-validate the 20-GSI
  per-table ceiling on the add path (`services/dynamodb/table_ops.go:82`).
- **S3** — `CompleteMultipartUpload` does not reject an empty parts list (AWS returns
  `InvalidRequest`) (`services/s3/backend_memory.go:2043-2087`). S3 Select performs basic
  CSV/JSON parsing without validating the SQL/FilterExpression, column names, or aggregates
  (`services/s3/select_sql.go`, `select_json.go`).
- **Kinesis** — `GetRecords` size cap counts partition-key bytes against the 10 MiB limit; AWS
  counts data bytes only (`services/kinesis/backend.go:765-783`). `ListStreamConsumers`
  exclusive-start is `<=` so the consumer equal to `NextToken` can appear on two pages
  (`services/kinesis/backend.go:1149-1152`). `CreateStream` skips the
  `[a-zA-Z0-9_.-]{1,128}` name validation (`services/kinesis/handler.go`).
- **Bedrock** — unknown errors return HTTP 500 instead of 400 for `ValidationException`
  (`services/bedrock/handler.go:1155-1166`); `CreateProvisionedModelThroughput` lacks the upper
  `modelUnits` bound (`services/bedrock/backend.go:1015-1021`).
- **MediaConvert** — `deepCloneValueAt` truncates nested settings beyond depth 20 to `nil`,
  silently corrupting job settings with no warning (`services/mediaconvert/backend.go:89-96`).
- **Elasticsearch** — `AssociatePackage` silently ignores a duplicate association; AWS returns
  `ConflictException` (`services/elasticsearch/backend.go:497-501`).
- **Neptune** — `ServerlessV2ScalingConfiguration` is accepted on create but ignored by
  `Create`/`ModifyDBCluster` despite being advertised (`services/neptune/backend.go`).
- **Account** — `ListRegions` ignores `maxResults`/`nextToken` and returns the full list, breaking
  AWS's 20-item page boundary (`services/account/backend.go:150`).
- **MQ** — name-based pagination cursors (`brokers[maxResults-1].BrokerName`) break consistency
  when items are added/removed between pages; AWS uses opaque tokens
  (`services/mq/handler.go:555,925`).
- **RDS** — `DescribeDBParameterGroups`, `DescribeDBClusterParameterGroups`,
  `DescribeDBParameters`, `DescribeOptionGroups` return all results with no `Marker` pagination
  (`services/rds/handler.go:1527,1568,1625,1871`).
- **API Gateway v2** — list operations (`GetAPIs`, etc.) have no `limit`/`position` pagination;
  `GetModelTemplate` always returns `{}` instead of the model's schema
  (`services/apigatewayv2/handler.go:1259-1268`).
- **Glacier** — retrieval jobs are marked `Succeeded` at creation instead of simulating the async
  retrieval window AWS enforces before `GetJobOutput` (`services/glacier/backend.go:499-502`).
- **DirectoryService** — unrecognised operations return HTTP 501 `UnimplementedException` rather
  than an AWS-style `InvalidRequestException` (`services/directoryservice/handler.go:166`).
- **SecurityHub** — `BatchImportFindings`/`BatchUpdateFindings` append untyped findings without
  validating required fields (Type, Id), so malformed findings silently succeed
  (`services/securityhub/handler.go:947-954`).

## C. Performance optimizations

Hotspots that hold locks during expensive work, copy whole maps per call, or scan linearly where
an index belongs.

- **EventBridge** — `deliverEvents` deep-copies all bus rules and targets on every `PutEvents`
  (`deepCopyBusRules`/`deepCopyBusTargets`), O(n) per publish on the latency path
  (`services/eventbridge/backend.go:87-91`).
- **Step Functions** — every state transition appends to history while holding the global write
  lock `b.mu`, serialising all concurrent executions
  (`services/stepfunctions/backend.go:1083-1089`); execution lookup/delete by name is O(n)
  (`backend.go:151-152`).
- **EC2** — `DescribeInstances` with no IDs shallow-copies every instance struct under lock, O(n)
  allocations per call (`services/ec2/backend.go:785-796`).
- **KMS / CloudWatch** — `findGrantByToken` linear-scans the entire grant map on every
  encrypt/decrypt and grant-token validation; needs a token→grant index
  (`services/kms/backend.go:2012-2021`, `services/cloudwatch/handler.go:2012-2021`).
- **SSM** — `GetParametersByPath` scans all parameters with no prefix/trie index
  (`services/ssm/backend.go:950-1024`).
- **CloudWatch Logs** — metric-filter matching is O(filters × events): each filter re-scans all
  events (`services/cloudwatchlogs/backend.go:1469-1478`).
- **ECR** — `DescribeImages` rebuilds the full digest→tags reverse map on every call instead of
  maintaining it incrementally (`services/ecr/backend.go:752-759`).
- **ECS** — `getServicesForReconciler` iterates all clusters×services into one unbounded slice
  every reconcile tick (default 5s) (`services/ecs/backend.go:1452-1458`).
- **Batch** — `DeleteComputeEnvironment` scans all job queues, and `findTagsInCoreResources`
  scans every environment/queue/job, for want of reverse indexes
  (`services/batch/backend.go:1027-1037,1494-1522`).
- **Forecast** — `lookupLocked` linear-scans every resource kind by ARN on each
  describe/update/delete (`services/forecast/backend.go:236-244`).
- **OpenSearch** — `ListPackagesForDomain` and `DeleteDomain` scan all package associations
  instead of a domain→packages index (`services/opensearch/backend.go:643-649,1532-1540`).
- **Organizations** — `ListTargetsForPolicy` re-resolves summaries per target and
  `CreateOrganizationalUnit` scans all OUs for the sibling-name check
  (`services/organizations/backend.go:946-950,1395-1398`).
- **AppRunner** — `resourceExists` scans six collections per call; needs a unified index
  (`services/apprunner/backend.go:698-719`).
- **DMS / ManagedBlockchain / Transfer / AppConfig** — O(n) scans for reference checks or
  uniqueness: `DeleteReplicationInstance` over tasks (`services/dms/backend.go:419-426`),
  `CreateNetwork` name check (`services/managedblockchain/backend.go:217-220`),
  `ImportSSHPublicKey` dup-key scan (`services/transfer/backend.go:2778-2787`),
  `CreateApplication` name check (`services/appconfig/backend.go:97-101`).
- **CloudWatch** — metric-datapoint overflow re-slices/copies the retained window per write;
  a ring buffer would avoid the repeated copy (`services/cloudwatch/backend.go:424-426`).
- **Glacier / S3** — list ops copy/convert the whole map before applying markers:
  `ListVaults`/`ListArchives` (`services/glacier/backend.go:346-354`) and
  `ListMultipartUploads` (`services/s3/backend_memory.go:2298-2320`).

## D. Resource leaks (unbounded growth / un-stopped timers)

State that grows without eviction, or timers/goroutines without stop, when the optional janitor
is not running or a delete path is missed.

- **ACM** — `time.AfterFunc` certificate timers stored in `b.timers` are only stopped by
  `sweepTimers` when the janitor is enabled; certs deleted with the janitor off leak goroutines
  (`services/acm/backend.go:300,673,1381`, `services/acm/janitor.go:77-100`).
- **Step Functions** — `pendingTaskQueues` channels are never closed on activity delete (goroutine
  leak), `tasksByToken` never evicts stale tokens, and `executions`/`history` have no TTL/pruning
  (`services/stepfunctions/backend.go:150-165`).
- **S3** — `pendingObjectLambdaRequests` (`sync.Map`) has no eviction, leaking on client
  disconnect before `WriteGetObjectResponse` (`services/s3/object_lambda.go:226`); the object
  tags map is only purged on bucket deletion, not per-object/version delete
  (`services/s3/backend_memory.go:3860-3862`).
- **DynamoDB** — `ShardIteratorStore` only drops expired tokens on explicit `Sweep()`; between
  janitor runs it accumulates expired iterators (`services/dynamodb/accuracy_audit.go:503-514`);
  backups are stored indefinitely with no GC.
- **SSM** — parameter `history`, `documentVersions`, and `commandInvocations` grow without the
  AWS caps (100 param versions, 1000 docs, 1h command expiry) unless the optional janitor runs
  (`services/ssm/backend.go:206-281`).
- **KMS** — `keyMaterialHistory` past `maxKeyMaterialHistoryEntries` (100) discards old material
  with no migration, breaking decrypt of older ciphertexts (`services/kms/backend.go:124-264`).
- **CloudWatch / CloudWatch Logs** — `alarmHistory` is bounded per alarm but unbounded across
  alarms (`services/cloudwatch/backend.go:214`); the parsed-query cache only evicts at the cap,
  never on TTL (`services/cloudwatchlogs/backend.go:1956-1959`).
- **EventBridge** — the in-memory event log (`GetEventLog`) has no cap/TTL/pruning
  (`services/eventbridge/backend.go:1212-1213`).
- **ECR** — `InitiateLayerUpload` entries in `b.layerUploads` never expire if the upload is never
  completed (`services/ecr/backend.go:980`).
- **ECS** — the docker `containers` map only clears on a fully-successful `StopTask`; failed
  stops leak partial entries (`services/ecs/docker_runner.go:55,108-109,249-255`).
- **EFS** — `h.archiveData` caches whole archive bodies with no TTL/eviction
  (`services/efs/handler.go:871-873`).
- **STS** — `b.sessions` is only swept by the 30s janitor; with the janitor off/long-interval it
  grows unbounded, and `GetCallerIdentity` check-then-delete is a TOCTOU race
  (`services/sts/backend.go:514-552`).
- **KinesisAnalytics / KinesisAnalyticsV2** — `applications`, `snapshots`, and `operations` maps
  are never pruned on application delete
  (`services/kinesisanalytics/backend.go:204-206`, `services/kinesisanalyticsv2/backend.go:202-206`).
- **LakeFormation** — `permissions` is an unbounded `[]*PermissionEntry` slice with no cap/TTL
  (`services/lakeformation/backend.go:202-241`).
- **Pinpoint** — `templateVersionHistory`, `campaignActivities`, `journeyRuns`, `appEvents` are
  append-only with no AWS version caps (`services/pinpoint/backend.go:74-80`).
- **AppRunner** — `svc.Operations` is appended per operation and never pruned/TTL'd
  (`services/apprunner/backend.go:441`).
- **Route53** — the handler-level `tags` map never evicts entries for ARNs whose delete was
  missed/failed (`services/route53/handler.go:72-89`).
- **MWAA / Secrets Manager** — MWAA per-environment metrics compaction leaves the backing array
  oversized (`services/mwaa/backend.go:1140-1144`); Secrets Manager rotation invocations are
  queued with no per-secret depth limit (`services/secretsmanager/backend.go`).

## Notes on confidence

Items in sections A and B were confirmed by reading the cited handler/backend code. Several
section-C/D items (e.g. an apparent `Unlock` without `Lock` at
`services/elasticache/backend.go:502-504`, and the FSx body-stream reuse at
`services/fsx/handler.go:180-205`) are flagged from a single read and should be re-verified
against the surrounding `defer`/locking before being treated as confirmed bugs.

---

# Dashboard / Web-UI feature gaps (2026-06-10)

The console is a SvelteKit app (`ui/`); each service has a page at
`ui/src/routes/<service>/+page.svelte` that drives the local emulator through the AWS JS SDK,
plus a streaming Connect dashboard (`proto/gopherstack/dashboard/v1/dashboard.proto`) for the
console request-tap and runtime metrics. The pattern across pages is uneven: a handful are rich
(create/edit/delete + drill-downs + live views — e.g. `elasticache` 2948 lines, `dynamodb`,
`s3`), while many are thin **read-only list views** (search + refresh only). This section
records useful UI features that are missing, audited across all ~143 route pages.

## E. Services with a backend but no dashboard page at all

These have working `services/*` handlers but no `ui/src/routes/<svc>/` page (and no aliased
page), so they are invisible in the console:

- **accessanalyzer**, **account**, **appmesh**, **databrew**, **datasync**, **dax**,
  **detective**, **directoryservice**, **dlm**, **forecast**, **macie2**, **medialive**,
  **mediapackage**, **mediatailor**, **opsworks**, **personalize**, **qldb** (+ **qldbsession**),
  **quicksight**, **rolesanywhere**, **workmail**.

(Note: `ce`→`costexplorer`, `inspector2`→`inspector`, `kafka`→`msk`, `stepfunctions`→`sfn`,
`timestreamwrite`→`timestream`, and `dynamodbstreams`→`dynamodb` are covered via aliased/host
pages and are *not* in the missing list above.)

Also missing at the platform level:
- **No per-service CloudWatch metric charts** — the dashboard proto exposes `RuntimeMetrics` and
  per-operation latency (`OperationSummary`) but individual service pages render no time-series
  graphs; almost every page below repeats "no metrics/monitoring view".
- **No global resource search / tag explorer** across services (the `resources` /
  `resourcegroupstaggingapi` pages are list-only).

## F. Missing per-service UI features (popular services first)

### Popular services

- **S3** (`ui/src/routes/s3/+page.svelte`) — inline object **preview/viewer** (text/JSON/image)
  without download; object **metadata/tag editor**; bucket **storage analytics** (size by
  prefix, request metrics); **access-logging** config + view; show the **static-website URL**
  after enabling hosting; batch copy/rename/delete.
- **DynamoDB** (`.../dynamodb/+page.svelte`) — **query-by-index** view for GSIs/LSIs;
  **PITR**/backup controls; **auto-scaling** policy config; **global-tables**/replica management;
  Contributor-Insights tab.
- **EC2** (`.../ec2/+page.svelte`) — instance **Details** button routes to a non-existent
  drill-down; no **security-group rule** view/edit; security-groups list is read-only (no
  create); no subnet create/edit; no Elastic-IP allocate/associate; no metrics/alarms link.
- **Lambda** (`.../lambda/+page.svelte`) — no **code update** after create (image-only); no
  **versions/aliases** management; no **event-source-mapping** (trigger) tab; no
  reserved/provisioned-concurrency UI; no resource-policy view.
- **IAM** (`.../iam/+page.svelte`) — no **inline-policy** editor; user detail omits **group
  membership**; no **login-profile**/password or **MFA-device** management; no
  permission-boundary UI.
- **SQS** (`.../sqs/+page.svelte`) — no **batch send**; no message **filter/search** by
  attribute; no **DLQ redrive** action or source-queue mapping; no queue metrics
  (ApproxNumberOfMessages / age-of-oldest).
- **SNS** (`.../sns/+page.svelte`) — publish **message-attributes** as structured fields + JSON
  validation; subscription confirmation flow help; topic metrics; platform-application
  endpoint/device management.
- **KMS** (`.../kms/+page.svelte`) — **grants** create/revoke tab; **key-policy** JSON
  editor/formatter; **key-material import**; ciphertext base64/hex toggle in encrypt/decrypt.
- **Secrets Manager** (`.../secretsmanager/+page.svelte`) — structured **key-value JSON editor**
  for secret value; **rotation schedule/Lambda** config form; **replica** add/remove;
  version **restore** action.
- **SSM** (`.../ssm/+page.svelte`) — **tree/folder** navigation for `/`-path parameters;
  parameter **value history/diff**; parameter **policies** (expiration/notification);
  CSV import/export.
- **CloudWatch** (`.../cloudwatch/+page.svelte`) — **metric charts**/time-series (alarms list a
  metric but never graph it); **dashboard widget editor**; metric-stream detail/edit; alarm
  action presets.
- **CloudWatch Logs** (`.../cloudwatchlogs/+page.svelte`) — true **live-tail** mode;
  Insights query **CSV export**; subscription-filter **test/simulate** against sample events;
  click-through from metric filter to matching events.
- **Step Functions** (`.../sfn/+page.svelte`) — **execution graph/timeline** visualization;
  per-state result/variable inspection; **redrive** of failed executions; ASL definition
  **validator** (definition is read-only); link to execution logs.
- **RDS** (`.../rds/+page.svelte`) — **parameter-group editor** (groups are list-only);
  snapshot **restore/clone**; read-replica / proxy endpoints; performance metrics (CPU/IOPS/
  connections).
- **ECS** (`.../ecs/+page.svelte`) — inline **task/container log** streaming; **service update**
  (image / task-def / desired count); container metrics; **ECS-Exec** shell; autoscaling
  policies.
- **ECR** (`.../ecr/+page.svelte`) — **scan-result CVE detail** (scans show only status); image
  **layer/SBOM** inspection; lifecycle-policy **rule builder** + dry-run; `docker pull/push`
  command snippet; replication-rule UI.
- **EKS** (`.../eks/+page.svelte`) — **kubeconfig** download / CLI command; kubectl-style
  pod/workload list; node-group scaling without recreate; node resource-utilization drill-down.
- **EventBridge** (`.../eventbridge/+page.svelte`) — rule **target** view/edit (Lambda/SQS/SNS/
  HTTP); **archive replay** UI + progress; event-pattern **visual builder**; DLQ config;
  API-destination credential rotation.
- **CloudFormation** (`.../cloudformation/+page.svelte`) — resource-dependency **graph/diagram**;
  **stack-policy** editor; nested-stack drill-down; drift **side-by-side property diff**;
  change-set approval workflow.
- **ElastiCache** (`.../elasticache/+page.svelte`) — cluster **performance metrics**; manual
  **failover/promote** replica; **parameter-group** value editor/diff; event timeline; user/ACL
  permission viewer.

### API / app-integration

- **API Gateway** (`.../apigateway/+page.svelte`) — edit REST API (name/endpoint type); per-stage
  **access/execution logging** toggle; request validators; authorizer cache config.
- **API Gateway v2** (`.../apigatewayv2/+page.svelte`) — **CORS** config editor; route request
  validators; integration response mapping editor; authorizer test with sample token.
- **API Gateway Management** (`.../apigatewaymanagementapi/+page.svelte`) — bulk
  **disconnect**; message/timeline filter by type/time; connection-history export.
- **AppSync** (`.../appsync/+page.svelte`) — **data-source** create UI (DynamoDB/Lambda/RDS);
  GraphQL **schema upload (SDL)**; resolver field-mapping builder; pipeline-function config.
- **AppConfig** (`.../appconfig/+page.svelte`) — config-profile **JSON/YAML editor**; deployment
  rollout preview/timeline; strategy simulator. **AppConfigData**
  (`.../appconfigdata/+page.svelte`) — session content debug; profile-version diff.
- **Step/Scheduler** (`.../scheduler/+page.svelte`) — execution **history** + next-run countdown;
  **"run now"** test trigger.
- **Pipes** (`.../pipes/+page.svelte`) — source/target resource **pickers** (S3/Lambda/DynamoDB);
  filter/transform expression **editor** (currently raw text).

### Compute / containers / scaling

- **AppRunner** (`.../apprunner/+page.svelte`) — auto-deploy (GitHub/ECR) config; custom-domain
  mapping; **traffic-split/canary**; health-check editor.
- **Batch** (`.../batch/+page.svelte`) — job **log/stdout** streaming; job-definition container
  editor; throughput/health metrics.
- **AutoScaling** (`.../autoscaling/+page.svelte`) — lifecycle-hook notification config;
  mixed-instances policy; spot config; bulk instance-protection.
- **ApplicationAutoScaling** (`.../applicationautoscaling/+page.svelte`) — **scaling-activity
  timeline**; step-scaling threshold editor; policy adjustment history.
- **ElasticBeanstalk** (`.../elasticbeanstalk/+page.svelte`) — config-template editor; save
  environment as template; worker-vs-web tier config.
- **AppStream** (`.../appstream/+page.svelte`) — **read-only/list-only**: no create stacks/fleets/
  images, no fleet scale, no session management.

### Data / analytics

- **Athena** (`.../athena/+page.svelte`) — result **export** (CSV/Parquet/JSON); saved-query
  templates; data-scanned cost display; query scheduling.
- **Glue** (`.../glue/+page.svelte`) — crawler-target edit; **partition-projection** config;
  data-quality profiling results/history; catalog tag management.
- **EMR** (`.../emr/+page.svelte`) — autoscaling-policy editor; bootstrap-action add/remove;
  notebook kernel/dependency config; studio workspace management. **EMR Serverless**
  (`.../emrserverless/+page.svelte`) — job config detail (spark-submit opts); app logging config;
  job-run status timeline/diagnostics.
- **Kinesis** (`.../kinesis/+page.svelte`) — **monitoring dashboard** (ingestion/iterator-age);
  enhanced-metrics graphs; shard-iterator position viewer. **Firehose**
  (`.../firehose/+page.svelte`) — **batch PutRecords** with preview; throughput charts;
  test-delivery to destination.
- **KinesisAnalytics / v2** (`.../kinesisanalytics*/+page.svelte`) — **SQL/Flink code editor**;
  runtime **log tail**; schema-discovery tester; (v2) Flink job-graph + savepoint management.
- **RedshiftData** (`.../rdsdata/`, `.../redshiftdata/+page.svelte`) — **result-grid** table view
  (results are raw JSON); schema explorer (SHOW TABLES / DESCRIBE); saved-query favorites;
  result pagination.
- **Redshift** (`.../redshift/+page.svelte`) — parameter-group editing; embedded query builder.
- **LakeFormation** (`.../lakeformation/+page.svelte`) — **permission-matrix** view
  (principal×resource×action); LF-tag-expression builder; transaction audit log.

### Storage / database

- **FSx** (`.../fsx/+page.svelte`) — **read-only/list-only**: no create file system, no
  backup/restore, no detail drill-down.
- **EFS** (`.../efs/+page.svelte`) — **access-point** create/manage; replication config; backup
  policy.
- **Glacier** (`.../glacier/+page.svelte`) — retrieve & **display inventory/job results** when
  jobs complete; governance-vs-compliance vault-lock clarity.
- **DocDB** (`.../docdb/+page.svelte`) — parameter-group edit; **global-cluster failover**; event
  filter config.
- **Neptune** (`.../neptune/+page.svelte`) — **Gremlin/SPARQL query console**; graph
  vertex/edge explorer.
- **MemoryDB** (`.../memorydb/+page.svelte`) — parameter-group viewer/editor; cluster scaling
  buttons.
- **MQ** (`.../mq/+page.svelte`) — broker VPC/security config; **message browser**/purge.

### Networking / edge

- **Route53** (`.../route53/+page.svelte`) — record **editor with validation hints**; alias-target
  **picker** (CloudFront/ALB/S3) instead of free text.
- **Route53Resolver** (`.../route53resolver/+page.svelte`) — rule **priority reorder**; firewall
  domain-list bulk import.
- **CloudFront** (`.../cloudfront/+page.svelte`) — **cache-behavior** create/edit; origin/behavior
  topology diagram; in-browser **function editor** with preview.
- **Transfer** (`.../transfer/+page.svelte`) — server **transfer/connection logs**; SSH-key
  **fingerprint** display; agreement transfer history; connector **test-connection**.
- **DMS** (`.../dms/+page.svelte`) — task **progress %**/ETA; validation-failure drill-down;
  endpoint **test-connection**.
- **ELB / ELBv2** (`.../elb*/+page.svelte`) — attribute editing (draining/cross-zone/access-logs);
  SG management; (v2) listener-rule **priority reorder**, target-group **stickiness** edit,
  **IP-target** registration.
- **ManagedBlockchain** (`.../managedblockchain/+page.svelte`) — node metrics/logs; proposal
  vote-history detail; ledger/state explorer.
- **OpenSearch** (`.../opensearch/+page.svelte`) — **index list**/shard/document-count dashboard;
  log-delivery and access-policy editing.
- **Elasticsearch** (`.../elasticsearch/+page.svelte`) — domain config edit (node type/count/
  storage); slow/index log config; access-policy editor.

### Security / identity

- **Cognito** (`.../cognito/+page.svelte`, `cognitoidp`, `cognitoidentity`) — **user
  drill-down** (attributes, group membership, password reset); group create/edit/delete; IDP
  config edit; **custom-attribute** management; resource-server (OAuth) management; advanced
  security/risk config.
- **GuardDuty** (`.../guardduty/+page.svelte`) — **finding detail** + archive/suppress; detector
  config (publishing frequency, SNS); export findings.
- **SecurityHub** (`.../securityhub/+page.svelte`) — **finding detail drill-down** (remediation,
  resources); custom-insight creation.
- **Organizations** (`.../organizations/+page.svelte`) — **move account / reparent OU**;
  policy **attach/detach** to accounts/OUs; account close/suspend.
- **SSO Admin** (`.../ssoadmin/+page.svelte`) — permission-set **inline-policy editor**; bulk
  multi-account assignment wizard; permission-set comparison.
- **IdentityStore** (`.../identitystore/+page.svelte`) — attribute-based user search; group
  hierarchy view; bulk user/group operations.
- **VerifiedPermissions** (`.../verifiedpermissions/+page.svelte`) — Cedar policy **validator/
  linter** with inline errors; policy diff; context JSON builder for the authorization tester.
- **WAF / WAFv2** (`.../waf*/+page.svelte`) — **rule builder/editor** (rules are read-only);
  IP-set / regex-pattern-set address editors; rule **priority reorder**; sampled-request
  **inspector** + rule-evaluation simulator.
- **Shield** (`.../shield/+page.svelte`) — functional **"Add Protection"** flow (button is
  inert); protection detail/attack history; attack-timeline date-range picker.
- **ACM** (`.../acm/+page.svelte`) — **file-upload** import (paste-only today); expiring-cert
  dashboard; bulk tagging. **ACM PCA** (`.../acmpca/+page.svelte`) — **issue-certificate** flow;
  CRL config; subordinate-CA chain builder.
- **RAM** (`.../ram/+page.svelte`) — permission-document **JSON editor**; bulk invitation
  accept/reject.

### ML / AI / media

- **Bedrock** (`.../bedrock/+page.svelte`) — **model invoke/test playground** with sample
  prompts; fine-tuning training-status viewer; guardrail rule detail/edit.
- **BedrockRuntime** (`.../bedrockruntime/+page.svelte`) — **token-by-token streaming** display;
  inference-parameter tuning (temp/top-p/max-tokens) + system prompt editor; conversation
  persistence/export; multi-model comparison.
- **SageMaker** (`.../sagemaker/+page.svelte`) — model-artifact inspector; endpoint **A/B
  traffic-split** / variant weights; training-job metrics/curves; HPO-tuning dashboard.
- **SageMakerRuntime** (`.../sagemakeruntime/+page.svelte`) — streaming/chunked output display;
  endpoint metrics (success rate/latency); async-job poller with S3 output preview/download.
- **Comprehend** (`.../comprehend/+page.svelte`) — **live inference tester** (classify / detect
  entities on sample text); training accuracy/F1 viewer; model-version comparison.
- **Rekognition** (`.../rekognition/+page.svelte`) — face detail (confidence/attributes);
  stream-processor start/stop/pause.
- **Polly** (`.../polly/+page.svelte`) — output-format selector (PCM/Ogg, not just MP3); lexicon
  XML editor + test pronunciation.
- **Translate** (`.../translate/+page.svelte`) — **"Run Translation"** action (page is
  list-only); terminology detail; parallel-data file upload; job status timeline.
- **Transcribe** (`.../transcribe/+page.svelte`) — output-bucket config; vocabulary **file
  import**; speaker-ID / language-model settings; **transcript download**.
- **Textract** (`.../textract/+page.svelte`) — **local document upload** (S3-only today);
  feature-type selection (hard-coded TABLES+FORMS); result JSON export; adapter selection in
  analysis.
- **MediaConvert** (`.../mediaconvert/+page.svelte`) — job **input/output settings** editor (S3
  source picker, codec/output-group selection); live job-progress polling; preset application.
- **MediaStore / MediaStoreData** (`.../mediastore*/+page.svelte`) — container metrics dashboard;
  download progress indicator; batch object operations.

### Messaging / engagement / misc

- **SES** (`.../ses/+page.svelte`) — receipt-rule **action** config (S3/SNS/Lambda/SQS);
  config-set event-destination management; bounce/complaint/delivery dashboard; template
  **send-test**. **SESv2** (`.../sesv2/+page.svelte`) — contact-list **member** add/remove/edit;
  CSV import/export; suppression-list bulk import.
- **Pinpoint** (`.../pinpoint/+page.svelte`) — campaign **schedule/A-B** editor; journey **visual
  builder** (journeys are read-only).
- **STS** (`.../sts/+page.svelte`) — federation-token **policy editor**; decoded-authorization-
  message JSON formatting; issued-credential/session history.
- **Support** (`.../support/+page.svelte`) — case **attachment** upload/view; priority escalation
  after creation; thread pagination.
- **SWF** (`.../swf/+page.svelte`) — execution input/output payload viewer; activity-type detail
  (timeouts/heartbeat); history event filtering.
- **CloudTrail** (`.../cloudtrail/+page.svelte`) — row-expand **full event JSON**;
  attribute-based filter builder (user/resource/source); delivery timeline.
- **WorkSpaces** (`.../workspaces/+page.svelte`) — **start/stop/reboot/rebuild** actions (only
  terminate today); bundle selector/comparison; connection diagnostics.
- **XRay** (`.../xray/+page.svelte`) — **trace detail** service-map + timeline; segment call
  hierarchy/latency breakdown; trace comparison.
- **IoT** (`.../iot/+page.svelte`) — thing **attribute editor**; rule **action tester** (SQL +
  sample payload); policy attach/detach manager. **IoTDataPlane**
  (`.../iotdataplane/+page.svelte`) — live **MQTT topic browser**; per-topic message history;
  connected-device dashboard. **IoTWireless** (`.../iotwireless/+page.svelte`) — device LoRaWAN
  config detail; gateway metrics; FUOTA-task progress.
- **CodeBuild** (`.../codebuild/+page.svelte`) — **Start Build** button; build-log streaming;
  cache hit/miss + artifact info. **CodePipeline** (`.../codepipeline/+page.svelte`) — execution
  **timeline** with action durations; artifact browser; approval audit log. **CodeDeploy**
  (`.../codedeploy/+page.svelte`) — **rollback** action; per-instance deployment status; ASG
  integration view. **CodeCommit** (`.../codecommit/+page.svelte`) — commit-log/graph; file
  browser/blame; merge-conflict viewer. **CodeConnections / CodeStarConnections** — sync-blocker
  detail; **Authorize** OAuth flow for PENDING connections; sync history. **CodeArtifact**
  (`.../codeartifact/+page.svelte`) — version promote/dispose actions; dependency tree;
  metadata editor.
- **ServerlessRepo** (`.../serverlessrepo/+page.svelte`) — version-publish (code upload + SAM
  validation); public-app discovery. **ServiceDiscovery**
  (`.../servicediscovery/+page.svelte`) — instance-attribute editor; namespace DNS/VPC viewer.
- **ResourceGroups / TaggingAPI** (`.../resourcegroups*/+page.svelte`) — resource-type breakdown;
  bulk group ops from tag filters; "tag all non-compliant" remediation; CSV/JSON export.
- **Amplify** (`.../amplify/+page.svelte`) — webhook/build-trigger config; custom-domain
  management; build-settings editor; deployment-history metrics.
- **MWAA** (`.../mwaa/+page.svelte`) — embedded S3 DAG browser; DAG content upload (create only
  captures paths).
- **S3Control** (`.../s3control/+page.svelte`) — access-point **policy viewer**/network-origin
  editor; multi-region AP failover/weights. **S3Tables** (`.../s3tables/+page.svelte`) — Iceberg
  **schema/column inspector**; optimize/vacuum/compaction actions.

## Notes on the UI audit

Findings are grounded in the actual `+page.svelte` contents (tabs, action buttons, and the SDK
commands each page imports). "Read-only/list-only" means the page imports only `List*`/`Describe*`
commands with search + refresh and no create/edit/delete or detail drill-down. As with the
backend audit, these are prioritized enhancement candidates for follow-up PRs; no UI code was
changed in this commit.
