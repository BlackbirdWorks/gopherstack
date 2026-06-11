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

> **Implementation status (branch `parity/mega-v2`).** A first pass on the
> Popular-services group has shipped the following per-service UI features
> (all wired to the live AWS JS SDK, no placeholders):
>
> - **SQS** — batch send (`SendMessageBatch`) modal with up to 10 entries +
>   per-entry failure reporting; client-side message filter by body / message
>   attribute. (DLQ redrive was already present as the Move-Tasks tab.)
> - **SNS** — structured message-attribute editor (Name / DataType / Value
>   fields) with a JSON mode that validates and round-trips between the two.
> - **KMS** — ciphertext base64⇄hex display toggle across encrypt / decrypt /
>   re-encrypt; key-policy "Format JSON" button + inline JSON validation that
>   disables Save on parse errors. (Grants tab was already present.)
> - **Secrets Manager** — structured key-value editor for the secret value
>   (auto-detects flat-JSON secrets) with a Plaintext fallback mode.
> - **SSM** — `/`-path folder **tree** navigation (Flat/Tree toggle) with
>   collapsible folders, in addition to the flat parameter list.
> - **Lambda** — Event-Source-Mapping (**Triggers**) panel: list, create
>   (SQS/DynamoDB/Kinesis), enable/disable, delete.
> - **Athena** — query-result **export** to CSV and JSON.
> - **CloudWatch Logs** — Insights query **CSV export**.
>
> **§F remaining** (everything below this note is still outstanding) — the
> remaining Popular-services items (S3 inline preview / analytics / website URL
> / batch ops; DynamoDB query-by-index / PITR / auto-scaling / global-tables;
> EC2 SG-rule editor / subnet & EIP management / drill-down; Lambda
> code-update / versions-aliases / concurrency; IAM inline-policy / group
> membership / login-profile / MFA; SNS topic-metrics graphs; CloudWatch metric
> charts / dashboard widget editor; Step Functions execution graph / redrive /
> validator; RDS parameter-group editor / snapshot restore / metrics; ECS / ECR
> / EKS / EventBridge / CloudFormation / ElastiCache items) **and the entire
> API/app-integration, Compute, Data/analytics, Storage/database,
> Networking/edge, Security/identity, ML/AI/media, and Messaging groups** below
> have not yet been implemented. They remain accurate enhancement candidates.

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

## §E / §F implementation status (branch `parity/mega-v2`)

**§E — backend-only services given a dashboard page (DONE, 18 of 21):**
Added list/detail SvelteKit pages at `ui/src/routes/<svc>/+page.svelte` for
**accessanalyzer, account, appmesh, databrew, datasync, dax, detective, directoryservice,
dlm, forecast, macie2, medialive, mediapackage, mediatailor, personalize, quicksight,
rolesanywhere, workmail**. Each is wired to real backend data via the typed AWS JS SDK client
(through the gopherstack endpoint), registered in `implementedDashboardRouteIds` and
`sidebarCategories` in `ui/src/lib/nav.ts`, with a `getXClient` factory in
`ui/src/lib/aws-client.ts`. Pages follow the existing fsx/shield template: tabbed
list views (one tab per primary `List*`/`Describe*` resource), client-side search, refresh,
status pills, and graceful empty/error states. App Mesh, MediaTailor (VOD), and WorkMail
(users/groups/resources) expose a parent-id filter input because their child `List*` calls
require a `meshName` / `SourceLocationName` / `OrganizationId`; QuickSight exposes an editable
`AwsAccountId` input (defaults to `000000000000`).

**§E remaining (deferred, 3):**
- **opsworks** — DEFERRED: `@aws-sdk/client-opsworks` publishes no release in the
  `3.1053.x`/`@smithy/core@3.24.x` line used by this UI; pinning it forces an incompatible
  `@smithy/core` that breaks the entire SDK bundle. Re-add once a compatible client version
  ships, or proxy via the dashboard Connect API instead of the JS SDK.
- **qldb / qldbsession** — DEFERRED: no backend implementation exists under
  `services/qldb*` (only a README), so there is no real data to wire; `qldbsession` is a
  data-plane companion with no standalone page in any case.

**§F — per-service UI features: NOT STARTED in this pass.**
All §F enhancements (S3 object preview, DynamoDB query-by-index, EC2 SG editing, Lambda
versions/aliases, IAM inline policies, the per-service CloudWatch metric charts, the global
resource/tag search, etc.) remain open. This pass prioritized making the 18 invisible
backend-only services reachable in the console (§E) before deepening existing pages (§F). The
full §F checklist above is unchanged and remains the backlog for follow-up dashboard PRs.

---

# Test-coverage & remaining-functionality audit (2026-06-10, pass 2)

Goal: **eclipse LocalStack's coverage entirely.** This pass inventories where gopherstack is
*not* yet exercised by integration tests or Terraform, and deep-dives the still-thin services
those gaps correlate with. Counts: **202** SDK-driven tests under `test/integration/`, and a
Terraform suite under `test/terraform/*` (per-service dirs + `fixtures/*` with
`success`/`import`/`drift` variants). Only `parity.md` is edited here.

## G. Integration-test coverage gaps (`test/integration/`)

Services with **no SDK-driven integration test** of their own (after accounting for aliased
coverage — `dynamodb`/`dynamodbstreams` are covered by the `ddb_*` suite and `cognitoidp` by
`cognito`):

`accessanalyzer`, `account`, `appmesh`, `apprunner`, `appstream`, `comprehend`, `datasync`,
`dax` (only an in-package `services/dax/dataplane_integration_test.go`, nothing under
`test/integration/`), `detective`, `directoryservice`, `dlm`, `forecast`, `fsx`, `guardduty`,
`inspector2`, `macie2`, `medialive`, `mediapackage`, `mediatailor`, `opsworks`, `personalize`,
`polly`, `qldb`/`qldbsession` (service removed — README only), `quicksight`, `rekognition`,
`rolesanywhere`, `securityhub`, `translate`, `transcribe`, `waf`, `workmail`, `workspaces`.

Highest-value integration tests to add first (popular + heavily used by IaC/tools):
- **fsx**, **apprunner**, **appstream**, **workspaces** — large op surfaces (FSx, AppRunner,
  AppStream, WorkSpaces each have 1000s of LOC) but zero end-to-end create→describe→delete proof.
- **comprehend / translate / polly / transcribe / rekognition** — exercise the inference ops
  (even canned) to lock response shapes against the AWS SDK deserialiser.
- **guardduty / securityhub / inspector2 / macie2 / detective / accessanalyzer** — finding/
  detector lifecycle round-trips.
- **datasync / directoryservice / waf (classic) / appmesh** — common in Terraform stacks.

## H. Terraform-test coverage gaps (`test/terraform/`)

Services with **no Terraform fixture or module** (no `test/terraform/<svc>` and no
`test/terraform/fixtures/<svc>`):

`accessanalyzer`, `account`, `appmesh`, `apprunner`, `appstream`, `comprehend`, `databrew`,
`datasync`, `dax`, `detective`, `directoryservice`, `dlm`, `forecast`, `fsx`, `guardduty`,
`inspector2`, `macie2`, `medialive`, `mediapackage`, `mediastoredata`, `mediatailor`,
`opsworks`, `personalize`, `polly`, `qldb`, `quicksight`, `rekognition`, `rolesanywhere`,
`securityhub`, `transcribe`, `translate`, `waf`, `workmail`, `workspaces`.

The existing fixtures pattern (`success.tf` + `import.tf` + `drift.tf`) should be replicated for
these. Terraform is the strongest parity signal because the AWS provider validates response
shapes, waiters, and `Read`-after-`Create` drift — so each new fixture closes many latent gaps at
once. Highest-value to add: **fsx**, **apprunner**, **appstream**, **workspaces**, **waf**
(classic), **datasync**, **directoryservice**, **dlm**, **guardduty**, **securityhub** — all
appear in real Terraform AWS-provider acceptance suites.

Also worth adding even where a fixture exists today: **multi-resource / cross-service** Terraform
modules (the `*-comprehensive` and `mega-batch-*` dirs are the right model) for services that
only have a single-resource `success.tf`.

## I. Op-level functionality gaps in the still-thin (untested) services

These are the services most likely to surprise a real SDK/Terraform client, found by reading
their handlers/backends. They fall into two buckets — **canned-inference** (returns deterministic
but non-real results, same as LocalStack) and **empty-stub** (advertised op returns empty/no
state, which breaks create→describe round-trips).

### Inference / ML (canned results — parity-neutral vs LocalStack, but a chance to exceed it)

- **Comprehend** — `detectSentiment` is keyword-matching with hardcoded 0.99/0.97 scores
  (`services/comprehend/handler.go:546-567`); `detectEntities` tags every Capitalised word as
  `PERSON`@0.99 (`handler.go:569-583`); `DetectDominantLanguage` always returns `en`
  (`handler.go:637-645`). No `BatchDetect*` async job result computation.
- **Translate** — `translateText` returns the input unchanged (`services/translate/handler.go:487-510`);
  `translateDocument` echoes the document (`handler.go:512-535`); configured terminologies are
  ignored by translation.
- **Polly** — `SynthesizeSpeech` returns a `POLLY:format:rate:voice:text` marker string instead
  of audio bytes (`services/polly/backend.go:266-296`); speech marks are stubbed `time:0`
  (`backend.go:692-699`).
- **Transcribe** — completed jobs carry synthetic text "This is a synthetic transcription result
  for {job}." (`services/transcribe/backend.go:267-300`); no real media read; call-analytics
  jobs not actually analysed (`handler.go:809-821`).
- **Rekognition** — `DetectLabels`/`DetectText`/`DetectModerationLabels`/`RecognizeCelebrities`/
  `DetectProtectiveEquipment` are routed but return empty result sets
  (`services/rekognition/handler_appendixa.go`); `SearchFacesByImage` ignores the image and
  returns a fixed 90.0 similarity (`backend.go:494-521`).

### Security findings (empty-stub — adding seedable findings would *exceed* LocalStack)

- **GuardDuty** — `GetMalwareProtectionPlan`, `SendObjectMalwareScan` are listed in
  `GetSupportedOperations` but have no handler (`services/guardduty/handler.go:219,262`);
  member-detector state isn't tracked; member/invitation maps grow unbounded.
- **SecurityHub** — `BatchGetAutomationRules` returns `[]` (`services/securityhub/backend.go:1786`);
  `ListEnabledProductsForImport` always empty (`backend.go:1470`); `GetFindingStatistics` returns
  empty aggregation; `DescribeStandards` returns hardcoded default controls (`backend.go:1136-1164`).
- **Inspector2** — `ListFindings` is "stub — always empty" (`services/inspector2/backend.go:347`);
  the `Finding` struct is a "minimal stub" (`backend.go:92`); CIS-scan ops
  (`GetCisScanReport`/`GetCisScanResultDetails`/`ListCisScans`) return empty
  (`backend_appendixa.go:693-708`).
- **Macie2** — `DescribeBuckets`/`GetBucketStatistics`/`SearchResources` explicitly return empty
  ("no real S3 scanning") (`services/macie2/backend_appendixa.go:1155-1350`).
- **Detective** — investigations and datasource ingest state are not persisted; `StartInvestigation`
  mints an ID but stores nothing; `ListIndicators` returns hardcoded stubs
  (`services/detective/handler.go:1109-1160`).
- **AccessAnalyzer** — `GetFindingsStatistics` advertised but unrouted (404); `StartResourceScan`
  is a no-op and findings never transition ACTIVE→ARCHIVED via archive rules
  (`services/accessanalyzer/handler.go:506-521`).

### Media / data (missing sub-resource ops break Terraform `Read`)

- **MediaTailor** — `DescribeChannel`/`DescribeSourceLocation`/`DescribeVodSource`/
  `DescribeLiveSource`/`DescribeProgram` return empty objects and `StartChannel`/`StopChannel`
  don't transition state (`services/mediatailor/handler.go:739-1333`).
- **MediaPackage** — no PackagingConfiguration CRUD and no `PutLifecyclePolicy`/`GetLifecyclePolicy`;
  `RotateIngestEndpointCredentials` always succeeds without backing state
  (`services/mediapackage/handler.go:749-763`).
- **MediaLive** — `CreateInputDeviceMaintenanceWindow` and `ListClusterAlerts` don't change/return
  real state.
- **Forecast** — `GetAccuracyMetrics` returns empty results (`services/forecast/backend.go:333-343`);
  no Explainability ops.
- **Personalize** — `GetRecommendations` (the core real-time inference op) is absent from the route
  table; `DescribeFeatureTransformation` returns a fabricated response
  (`services/personalize/handler.go:1254-1265`).
- **DataSync** — `UpdateTaskExecution` mutates no state (`services/datasync/handler.go:907-920`);
  FSx-Windows location ops missing.
- **DirectoryService** — certificate ops (`Register/Deregister/Describe/ListCertificate(s)`) and
  conditional-forwarder ops are absent; `RestoreFromSnapshot` returns success without doing work
  (`services/directoryservice/handler.go:546-569`).
- **QuickSight** — asset-bundle export and folder-permission ops delegate to appendix handlers that
  may not track real state (`services/quicksight/handler_appendixa.go:92-94`); needs op-by-op
  verification.

### Large surfaces needing op-by-op verification

- **AppStream** (120 backend funcs / ~8.2k LOC) and **WorkSpaces** (96 funcs / ~7.3k LOC) are
  substantial, but a sub-pass flagged some advertised sub-resource ops (AppBlock/ImageBuilder/
  Entitlements for AppStream; IpGroups/ConnectionAliases/Bundles/Images/Pools for WorkSpaces) as
  possibly unrouted to backend methods. These need a precise handler↔backend op diff rather than
  the size-only check done here, then an integration + Terraform fixture to lock them in.
- **DAX** — backend `Snapshot()`/`Restore()` persistence hooks are declared but unimplemented
  (`services/dax/interface.go:60-61`), so DAX state is dropped on snapshot/restore.
- **QLDB / qldbsession** — service was removed (README only) after AWS's 2025-07-31 deprecation;
  the empty `services/qldb/` dir is dead weight, not a parity gap.

## J. Eclipsing LocalStack — highest-leverage themes

Consolidated priorities that would push gopherstack past LocalStack's coverage:

1. **Close every empty-stub finding op** in the six security services so detectors/findings can be
   seeded and round-tripped — LocalStack returns empty here too, so this is pure upside.
2. **Add Terraform `success`+`import`+`drift` fixtures for the 34 services in §H**, prioritising
   the ones in the AWS provider's acceptance suite (fsx, apprunner, appstream, workspaces, waf,
   datasync, directoryservice, dlm, guardduty, securityhub) — Terraform validation closes the most
   latent shape/waiter gaps per unit of effort.
3. **Add integration tests for the 33 untested services in §G**, starting with the large-surface
   ones (fsx, apprunner, appstream, workspaces) where regressions are currently invisible.
4. **Wire the missing DLQ/RedrivePolicy paths (SNS, EventBridge), pagination (RDS, API Gateway v2,
   Account, MQ), and persistence hooks (DAX, and any backend whose `persistence.go` omits a field)**
   from §B/§D — these are correctness gaps a real client will hit immediately.
5. **Surface the 20 backend-only services in the dashboard (§E)** and add per-service metric charts
   using the already-defined `OperationSummary`/`RuntimeMetrics` proto — a console that visualises
   every service is something LocalStack's open tier does not offer.

## Notes on pass 2

The §G/§H service lists are exact (computed from the test tree). §I findings were read from the
cited handlers/backends; the inference "canned result" items are intentional mocks (flagged as
opportunities, not bugs). The AppStream/WorkSpaces sub-resource items are flagged as
*needs-verification* because the size check (120/96 backend funcs) contradicts the raw "unrouted"
estimate — a precise op diff is required before treating them as confirmed.

---

# Platform, CloudFormation & deep-accuracy audit (2026-06-10, pass 3)

Pass 3 looks at the dimensions that most determine "can a real IaC stack / SDK app run unchanged":
CloudFormation resource-type breadth, platform features LocalStack ships, the cross-service event
glue, and op-level AWS accuracy in the popular services. **Headline: the engine is already strong**
— DynamoDB, SQS, and Lambda (real Docker runtimes) are close to AWS-accurate, and almost every
cross-service trigger is wired. The remaining gaps are specific and enumerated below. Only
`parity.md` is edited.

## K. CloudFormation resource-type coverage

The CFN engine wires **~111 `AWS::*` resource types** and a solid intrinsic-function set:
`Ref` (incl. pseudo-params), `Fn::GetAtt`, `Fn::Sub`, `Fn::Join`, `Fn::Select`, `Fn::Split`,
`Fn::FindInMap`, `Fn::If`, `Fn::Equals`, `Fn::ImportValue`; plus Conditions, Mappings, Parameters,
Outputs/Exports, `DependsOn`, nested stacks, and dynamic refs
(`{{resolve:ssm:…}}` / `{{resolve:secretsmanager:…}}`) — see
`services/cloudformation/template.go:236-498` and `dynamic_refs.go:21-200`.

**Genuinely missing, common, real CFN resource types** (curated — excludes non-types like
`AWS::S3::BucketVersioning` which are `Bucket` properties, not resources):
- **API Gateway v2 (blocks most HTTP-API templates):** `AWS::ApiGatewayV2::Integration`, `::Route`,
  `::Authorizer`, `::DomainName`, `::ApiMapping`, `::Stage` (Stage exists; Integration/Route do not).
- **API Gateway v1:** `AWS::ApiGateway::Model`, `::RequestValidator`, `::Authorizer`, `::ApiKey`,
  `::UsagePlan`, `::UsagePlanKey`, `::DomainName`, `::BasePathMapping`, `::Account`, `::GatewayResponse`.
- **Logs:** `AWS::Logs::LogStream`, `::MetricFilter`, `::SubscriptionFilter`, `::ResourcePolicy`,
  `::QueryDefinition`.
- **Events:** `AWS::Events::Connection`, `::ApiDestination`, `::Archive`, `::EventBusPolicy`.
- **KMS:** `AWS::KMS::Alias`, `::ReplicaKey`.
- **Cognito:** `AWS::Cognito::IdentityPool`, `::IdentityPoolRoleAttachment`, `::UserPoolDomain`,
  `::UserPoolGroup`.
- **EC2:** `AWS::EC2::Volume`, `::VolumeAttachment`, `::NetworkInterface`, `::VPCPeeringConnection`,
  `::NetworkAcl`(+`Entry`), `::KeyPair`, `::SecurityGroupIngress`/`Egress` (standalone), `::FlowLog`.
- **ELBv2:** `AWS::ElasticLoadBalancingV2::ListenerRule`.
- **Lambda:** `AWS::Lambda::EventInvokeConfig`, `::Url`.
- **AutoScaling for ECS/etc.:** `AWS::ApplicationAutoScaling::ScalableTarget`, `::ScalingPolicy`.
- **Secrets Manager:** `AWS::SecretsManager::RotationSchedule`, `::ResourcePolicy`,
  `::SecretTargetAttachment`.
- **SSM:** `AWS::SSM::Document`, `::MaintenanceWindow`, `::Association`.
- **DynamoDB:** `AWS::DynamoDB::GlobalTable`.
- **Glue:** `AWS::Glue::Crawler`, `::Table`, `::Trigger`, `::Connection`, `::Partition`.
- **AppSync:** `AWS::AppSync::DataSource`, `::Resolver`, `::FunctionConfiguration`, `::ApiKey`.
- **Step Functions:** `AWS::StepFunctions::Activity`.
- **Policies:** `AWS::SNS::TopicPolicy`.
- **CloudFront:** `AWS::CloudFront::Function`, `::OriginAccessControl`, `::CachePolicy`,
  `::ResponseHeadersPolicy`.
- **Extensibility (high value):** `AWS::CloudFormation::CustomResource` / `Custom::*` (custom
  resources), `AWS::CloudFormation::Macro` (template macros), `WaitCondition`/`WaitConditionHandle`.

Custom resources and macros are the biggest single gap for "eclipse LocalStack" — many real
templates (and CDK output) depend on `Custom::` Lambda-backed resources.

## L. Platform-feature parity vs LocalStack

Checklist of LocalStack platform capabilities (✅ present / ◑ partial / ❌ missing), with
file:line:

- ✅ **Health endpoint** — `GET /_gopherstack/health` (`cli.go:1993,2030`) returns status,
  services, runtime metrics. ◑ no separate `/_localstack/info`-style endpoint (folded into health).
- ✅ **Reset endpoint** — `POST /_gopherstack/reset[?service=…]` resets `Resettable` services
  (`cli.go:1994,2056`).
- ◑ **Persistence** — `--persist` + `--data-dir` with debounced auto-snapshot
  (`cli.go:409,5144`, `pkgs/persistence/manager.go`). **Missing: an explicit save/load (Cloud-Pods
  style) API endpoint** — persistence is background-only; you can't trigger/export a snapshot via API.
- ✅ **Init hooks** — `--init-script`/`INIT_SCRIPTS` run shell scripts on startup
  (`pkgs/inithooks/inithooks.go`, `cli.go:1859`); arguably more flexible than LocalStack's
  `init/ready.d`.
- ✅ **Embedded DNS** — `--dns-addr` resolves Lambda/Route53/RDS/Redshift/OpenSearch/ElastiCache/EC2
  hostnames (`pkgs/dns/dns.go`, `cli.go:1966-1974`).
- ❌ **SigV4 request-signature validation** — auth headers are parsed for region/service routing
  only, never cryptographically verified (`pkgs/httputils/httputils.go:306-326`). Any credentials
  are accepted. (LocalStack Pro can enforce IAM; even an *opt-in* validation mode would exceed the
  open tier.)
- ❌ **Multi-account / multi-region isolation** — a single fixed `--account-id`/`--region`; the
  account/region in the request is ignored, so state is not partitioned per account or region
  (`pkgs/config/config.go`). This is a significant parity gap — LocalStack keys stores by
  account+region.
- ◑ **Protocol coverage** — query/EC2, JSON (`x-amz-target`), rest-JSON, rest-XML all handled
  (`pkgs/service/jsondisp.go`, `priorities.go`). **Missing: CBOR** (used by newer DynamoDB/Kinesis
  SDKs and timestream) — not implemented.
- ❌ **HTTPS/TLS listener** — HTTP only; no `ListenAndServeTLS`/cert flags (`cli.go:4307-4311`).
  Some SDKs/tools default to HTTPS endpoints.
- ◑ **Single edge-port multiplexing** — services share one HTTP listener via a priority router
  (`pkgs/service/router.go`), but there's no LocalStack-style `:4566` edge with host/SNI-based
  service routing + TLS.

Highest-leverage platform gaps to close: **multi-account/region isolation**, **optional SigV4/IAM
enforcement mode**, **CBOR**, **TLS**, and a **persistence save/load API**.

## M. Cross-service event/integration wiring (largely a strength)

Most "service A triggers service B" glue is implemented — this is an area where gopherstack already
matches or beats LocalStack's open tier. Confirmed working (file:line):
- **S3 notifications → SQS / SNS / Lambda / EventBridge** with filtering
  (`services/s3/notification.go:358-464`).
- **SNS → SQS / Lambda / Firehose** with filter policies, raw delivery, DLQ
  (`services/sqs/sns_delivery.go:46-97`, `services/sns/lambda_firehose_delivery.go:79-119`).
- **Lambda ESM polling for SQS / DynamoDB Streams / Kinesis** with batch-item-failure reporting
  (`services/lambda/event_source_poller.go:320-795`).
- **EventBridge rule → Lambda/SQS/SNS/StepFunctions/ECS/Kinesis/Firehose** with retries, DLQ, event-age
  (`services/eventbridge/delivery.go:215-488`).
- **EventBridge Pipes** source→enrichment→target and **Scheduler** → 8 target types with backoff+DLQ
  (`services/pipes/runner.go:287-379`, `services/scheduler/runner.go:281-421`).
- **CloudWatch alarm → SNS actions** (`services/cloudwatch/backend.go:1617-1640`).
- **Firehose → S3 + Lambda transform** (`services/firehose/transform.go:38-90`).
- **Step Functions task → Lambda/SNS/SQS/DynamoDB** integrations (`services/stepfunctions/integrations.go`).

Remaining wiring gaps:
- ◑ **CloudWatch Logs subscription filter → Lambda/Kinesis/Firehose** — `deliverToFilters` hands the
  encoded batch to an external `SubscriptionDeliverer` but does **no destination-ARN type routing in
  the backend itself** (`services/cloudwatchlogs/backend.go:1548-1602`); verify all three
  destination types actually deliver end-to-end (and add an integration test).
- **SNS → HTTP/HTTPS and email/email-json** delivery — confirm these subscription protocols deliver
  (only SQS/Lambda/Firehose were positively traced).
- **DLQ/RedrivePolicy on the SNS subscription and EventBridge target paths** — see §B; failed HTTP/
  Lambda deliveries should land in a DLQ.

## N. Deep op-level AWS accuracy in popular services

Read-confirmed accuracy (good news, recorded so it isn't re-flagged): **DynamoDB** — PartiQL
(`partiql.go:142-641`), TransactWrite atomicity + cancellation reasons
(`transact_ops.go:30-124`), TTL sweep every 5s (`janitor.go:79-100`), all 4 stream view types,
export-to-S3; **SQS** — FIFO dedup window + content-based SHA-256 dedup, group ordering,
visibility extension, DelaySeconds, max-receive→DLQ; **Lambda** — real Docker runtimes
(`backend.go:2300-2319`), RequestResponse/Event/DryRun, function URLs, layers, env, response
streaming; **S3** — correct multipart ETag (`backend_memory.go:2129`), conditional requests +
range GET, object-lock/legal-hold enforcement, CORS preflight.

Specific **not-emulated / inaccurate** behaviors to close:
- **S3 — SigV4 presigned-URL signature not verified** (`services/s3/presign.go:42-45`): the
  signature is extracted but never validated, so any presigned URL is accepted. (Mirrors the
  platform-level SigV4 gap; an opt-in verification mode would exceed LocalStack.)
- **S3 — requester-pays not modeled** (no `x-amz-request-payer` handling) and
  **transfer-acceleration** stores status but doesn't change the endpoint
  (`services/s3/extra_backend.go:38-71`).
- **EC2 — IMDSv2 not enforced**: `MetadataOptions` are stored but there's no `169.254.169.254`
  metadata endpoint and no token TTL/enforcement (`services/ec2/backend.go:78-92,758-816`).
- **EC2 — security-group rules not evaluated**: rules are stored but no traffic/ACL simulation;
  network isolation is not enforced.
- **EC2 — routing/NAT/IGW and EBS/Spot are structural stubs**: route tables/NAT/IGW don't route
  packets, EBS snapshots don't capture data, spot has no market price/interruption
  (`services/ec2/backend_ec2core.go:362-401`).
- **Lambda — SnapStart not implemented** (no config parsed or honored).

## O. Concrete integration + Terraform test targets (to lock parity in)

To maximize int/terraform coverage (the strongest parity signal), the highest-value additions,
each as a `success.tf`(+`import.tf`+`drift.tf`) fixture and/or an SDK round-trip test:
1. **API Gateway v2 full stack** — `Api`+`Integration`+`Route`+`Stage`+`Authorizer` via Terraform
   *and* via CFN (exercises the missing CFN types in §K) — currently HTTP-API templates can't deploy.
2. **CloudFormation custom resource** — a `Custom::` Lambda-backed resource round-trip (closes the
   single biggest CFN gap).
3. **Cross-service event e2e tests** — S3→Lambda, SNS→SQS, EventBridge→StepFunctions,
   DynamoDB-Streams→Lambda, CW-Logs-subscription→Firehose — assert the *target* actually received
   the event (locks the §M wiring against regressions).
4. **Fixtures for the 34 services in §H**, prioritising fsx, apprunner, appstream, workspaces, waf,
   datasync, directoryservice, dlm, guardduty, securityhub.
5. **Multi-account/region test** — once isolation lands (§L), a test that writes to two accounts/
   regions and asserts state separation.
6. **`*-comprehensive` Terraform modules** for Logs (MetricFilter/SubscriptionFilter), Cognito
   (IdentityPool + role attachment), Glue (Crawler/Table/Trigger), and AppSync (DataSource/Resolver)
   — all common in real stacks and currently un-fixtured at the resource level.

## Notes on pass 3

The CFN supported-count (~111) and intrinsic list are from the engine's registry/template code;
the "missing types" list was curated down to real, common `AWS::*` resources (the raw grep also
surfaced several non-types and niche resources, omitted here). Platform and op-accuracy items are
cited to the files read. The cross-service section is deliberately recorded as a *strength* with
only the true partials called out, to keep the parity picture honest rather than inflating the
gap count.

---

# P. Actionable line-level issue backlog (2026-06-10, pass 4)

A discrete, fixable backlog for a downstream fixing agent. Each item is a single change with a
`file:line` and a suggested fix. Found by op-level fidelity hunts (response shapes, error codes,
validation, pagination, SDK-waiter blockers). **Two caveats for the fixer:**
- **Pagination direction**: many items below are "token cursor off-by-one" (`start = i` vs
  `start = i + 1`, or `keys[n-1]` vs `keys[n]`). These are only bugs if the token *generation* and
  *lookup* disagree. Before flipping, confirm the convention: if `NextToken` is set to the **last
  returned** item, the next-page lookup must **skip** it (`start = i + 1`); if it's the **first
  un-returned** item, the lookup must **include** it (`start = i`). Fix whichever side is
  inconsistent, and add a two-page integration test.
- **`omitzero` is NOT a bug here** — `go.mod` is `go 1.26`, and `encoding/json` honors `omitzero`
  (Go 1.24+). Any earlier note suggesting `omitzero` is invalid is withdrawn.

## Pagination — cursor off-by-one / duplicate-or-skip across pages

These share a root cause (token-vs-lookup mismatch) and are high-value because they corrupt
multi-page listings:
- **ECR** — `services/ecr/handler.go:556-565` (`DescribeRepositories`) includes the token item
  (`start = i`) while the token is the last-of-page → duplicate; same at
  `handler.go:900-906` (`DescribeImages`) and `:954-963` (`ListImages`). The paired token-build
  uses `repos[MaxResults]`/`imgs[MaxResults]` index (`:571,911,969`) — reconcile both sides.
- **QuickSight** — `start = i` in every paginator: `backend.go:517` (Namespaces), `:676` (Groups),
  `:784` (GroupMembers), `:941` (Users), `:1102` (DataSources), `:1239` (DataSets), `:1347`
  (Ingestions), `:1479` (Dashboards), `:1640` (Analyses) → each duplicates the cursor row.
- **DataBrew** — `services/databrew/paginate_helper.go:22` (`keys[endIdx-1]`) and
  `backend.go:812` (`reversed[endIdx-1].RunID`) emit the last-of-page as the token.
- **MQ** — `services/mq/handler.go:555` (`brokers[maxResults-1].BrokerName`).
- **AutoScaling** — `services/autoscaling/handler.go:439`
  (`groups[maxRecords-1].AutoScalingGroupName`).
- **ELBv2** — `services/elbv2/handler.go:385` (`lbs[pageSize-1].LoadBalancerArn`).

## Pagination — MaxResults/Limit ignored or no NextToken in output

- **Cognito IDP** — `MaxResults`/`Limit` ignored and output struct lacks `NextToken`:
  `ListUserPools` (`services/cognitoidp/handler.go:572,575`), `ListUserPoolClients` (`:710,713`),
  `ListUsers` (`:1103,1115`). Add `NextToken` to the output structs and honor the limit.
- **CodePipeline** — output structs missing `NextToken`: `ListPipelineExecutions`
  (`services/codepipeline/handler.go:1044`), `ListWebhooks` (`:1225`), `ListActionExecutions`
  (`:1566`), `ListActionTypes` (`:1592`), `ListRuleExecutions` (`:1705`); and
  `ListPipelineExecutions` ignores the params entirely (`:1048-1071`).
- **Athena** — `ListQueryExecutions` hardcodes `NextToken: ""` and ignores `MaxResults`/`NextToken`
  (`services/athena/handler.go:601`).
- **IoT** — `ListThings` (`services/iot/handler.go:1986-2001`), `ListTopicRules` (`:2003-2018`),
  `ListPolicies` (`:1972-1984`) accept/return no pagination.
- **SecurityHub** — `intFromBody` returns `0` for missing `MaxResults` with no sensible default
  (`services/securityhub/handler.go:926-930`).

## Pagination — bounds validation (return the AWS error instead of accepting any value)

- **KMS** — `Limit` not clamped to AWS max (50 for `ListResourceTags`, 1000 for `ListKeys`/
  `ListAliases`): `services/kms/handler.go:648-675`, `services/kms/backend.go:587-592`. Raise
  `ErrLimitExceeded` (already in the table, `handler.go:1040`) on overflow.
- **IAM** — `parseMaxItems` accepts any positive int; enforce 1–1000
  (`services/iam/handler.go:1757-1771`).
- **SSM** — list/describe ops accept arbitrary `MaxResults`; enforce per-op bounds (typically 1–50)
  (`services/ssm/handler.go`).
- **Secrets Manager** — `ListSecrets` doesn't bound `MaxResults` (1–100)
  (`services/secretsmanager/models.go:64-68`).
- **Cognito IDP** — no bound on `MaxResults`/`Limit` (Cognito cap ~60)
  (`services/cognitoidp/handler.go:567`).
- **RAM** — list ops don't validate/ default `MaxResults` (cap 100) (`services/ram/handler.go`).
- **CloudFormation** — `ListStacks`/`ListStackResources`/`ListExports` don't bound `MaxResults`
  (1–100) (`services/cloudformation/handler.go`).
- General: several services silently **reset to page 1 on an invalid/garbage NextToken** instead
  of returning `InvalidParameterException` (e.g. ECR, Route53) — decide on validation + error.

## SDK-waiter blockers (Describe never reaches the terminal state a waiter polls)

- **Lambda** — `CreateFunction` returns `State:"Active"` immediately
  (`services/lambda/handler.go:1490`); AWS returns `Pending`→`Active`. Also
  `LastUpdateStatus` defaults to empty (`services/lambda/models.go:113`) where AWS returns
  `Successful|InProgress|Failed`. SDK waiters `FunctionActiveV2`/`FunctionUpdatedV2` can't match.
  Mirror the DynamoDB create→active delay pattern (`dynamodb/table_ops.go:114-123`).
- **Glue** — `StopCrawler` sets state `STOPPING` but the reconciler only does `RUNNING→READY`, so
  the crawler hangs in `STOPPING` forever (`services/glue/backend.go:2039-2055`). Add
  `STOPPING→STOPPED` (or set `READY`).
- Audit other long-waiter services (RDS/ECS/EKS/CFN) for the same pattern — Describe must
  eventually report `available`/`ACTIVE`/`CREATE_COMPLETE`.

## Response-shape / field-name fidelity

- **SNS XML tag case** (Query-protocol element names must be PascalCase) in
  `services/sns/models.go`: `ListPhoneNumbersOptedOutResult.NextToken` `nextToken`→`NextToken`
  (`:485`) and `PhoneNumbers` `phoneNumbers>member`→`PhoneNumbers>member` (`:486`);
  `CheckIfPhoneNumberIsOptedOutResult.IsOptedOut` `isOptedOut`→`IsOptedOut` (`:404`);
  `GetSMSAttributesResult.Attributes` `attributes>entry`→`Attributes>entry` (`:440`);
  `XMLAttributeEntry` `key`/`value`→`Key`/`Value` (`:100-101`).
- **SQS** — `jsonListDeadLetterSourceQueuesResp` JSON key `queueUrls`→`QueueUrls`
  (`services/sqs/handler.go:518`).
- **Cognito IDP** — `TokenResult` JSON keys are lowercase `idToken`/`accessToken`/`refreshToken`;
  AWS `AuthenticationResult` expects `IdToken`/`AccessToken`/`RefreshToken`
  (`services/cognitoidp/tokens.go:103-106`) — **verify this struct is the wire response** before
  changing. `userSummary` Go field `UserLastModified` vs JSON `UserLastModifiedDate` mismatch
  (`handler.go:1111`); `Enabled` bools lack `omitempty` where sibling structs have it
  (`:916,973,1112`).
- **`NextToken` omitempty** (AWS omits the field on the last page; emitting `""` differs):
  StepFunctions `handler.go:318,336,341,375,428,460`; EventBridge `handler.go:392,403,422`;
  ACM input `handler.go:136`; ACM PCA input `handler.go:287`.
- **S3 XML element presence** (verify against AWS, these are deliberate-encoding calls):
  `ListMultipartUploadsResult.Prefix` should NOT be `omitempty` (AWS always emits `<Prefix>`)
  (`services/s3/model.go:305`); confirm `ListBucketResult.Prefix`/`Contents`/`CommonPrefixes`
  emission matches AWS (`model.go:27,31,32,48`).
- **DynamoDB DescribeTable** — `StreamSpecification` should always carry an explicit
  `StreamEnabled` bool (false when disabled), not be omitted/conditional
  (`services/dynamodb/table_ops.go:661,708`); ensure `BillingModeSummary` is always present
  (`:622`).
- **DynamoDB Scan** — `ScannedCount` must reflect pre-filter count while `Count` is post-filter;
  currently both derive from the filtered slice (`services/dynamodb/item_ops_scan.go:136-137`).
- **EC2 `DescribeInstanceStatus`** — `instanceStatusItem` omits `SystemStatus` and `InstanceStatus`
  health objects (`services/ec2/handler_ext.go:34-49`).
- **RDS** — `BackupRetentionPeriod` is `omitempty` but AWS always returns it
  (`services/rds/handler.go:1241`).
- **API Gateway** — list responses use a single `item` wrapper key; AWS uses operation-specific
  embedded keys — verify each list op's wrapper (`services/apigateway/handler.go`).

## Input validation / error-code fidelity

- **Lambda** — `validateMemoryAndTimeout` checks timeout but not memory (128–10240 MB, 1-MB steps)
  (`services/lambda/handler.go:1377-1387`); confirm `CreateFunction` returns HTTP 201
  (`handler.go`, cf. `audit_gaps_test.go:106`).
- **RDS** — no range check on `AllocatedStorage` (20–65536) (`services/rds/handler.go:564-570`);
  `MonitoringInterval>0` should require `MonitoringRoleArn` (`:593-607`); `ErrInvalidParameter`
  maps to `InvalidParameterValue` where AWS often returns `InvalidParameterCombination`/
  `…Exception` (`:1087`).
- **EC2** — `RequestSpotFleet` doesn't validate `TargetCapacity>=1`
  (`services/ec2/backend_spot_fleet.go:229`); `CreateVolume` KMS key not validated
  (`handler_ext.go:775`).
- **STS** — `dispatchAssumeRole` should pre-validate `DurationSeconds` range (900–43200) before the
  backend (`services/sts/handler.go:252-260`).
- **Cognito IDP** — `AdminSetUserPassword` skips the pool password-policy check that
  `ConfirmForgotPassword` applies (`services/cognitoidp/backend.go:955` vs `:824`).
- **S3** — `DeleteObjects` over-limit uses generic `ErrInvalidArgument`; AWS returns
  `MalformedXML`/specific `1000`-keys error (`services/s3/bucket_ops.go:837`); `MaxKeys>1000`
  should be clamped, not honored verbatim (`backend_memory.go:4148`).
- **KMS** — encryption-context-size error should match AWS wording/exception
  (`services/kms/backend.go:634`).

## IAM policy evaluation (parity-relevant, verify)

- Confirm whether IAM actually **evaluates** policies for authorization (and whether
  `SimulatePrincipalPolicy`/`SimulateCustomPolicy` are real or canned). If requests are always
  allowed, an opt-in enforcement mode + a working policy simulator would exceed LocalStack's open
  tier. (Cross-reference the platform-level "no SigV4/IAM enforcement" finding in §L.)

## Notes on pass 4

These are read-confirmed at the cited lines. The pagination-direction items are flagged for
convention-check (above) rather than blind flipping. Items marked "verify" (Cognito `TokenResult`,
S3 `<Prefix>` emission, API Gateway wrappers, IAM evaluation) need a quick AWS-shape confirmation
before the fix. The `omitzero` "bug" reported by one sub-pass was rejected (`go 1.26` supports it).
This backlog is intentionally line-level so it can be burned down item-by-item; it does not
duplicate the category-level findings in §A–§O.

## Pass 4 — implementation status (fixing agent, 2026-06-10)

A fixing agent verified each §P item against current code. Many were false positives (see below);
the genuine ones were fixed with table-driven tests.

**Fixed (file → change):**
- **Cognito IDP pagination + bounds** — `cognitoidp/handler.go`: `ListUserPools`/`ListUserPoolClients`
  now honor MaxResults + emit NextToken; `ListUsers` honors Limit + PaginationToken. Added
  `validateCognitoMaxResults` (1–60, else `InvalidParameterException`). Backends already sorted, so
  pagination cursors are stable.
- **Cognito IDP AdminSetUserPassword** — `cognitoidp/backend.go`: now enforces the pool password
  policy (was skipped vs `ConfirmForgotPassword`); returns `InvalidPasswordException`.
- **Glue StopCrawler** — `glue/backend.go`: STOPPING crawlers now transition STOPPING→READY via the
  reconciler instead of hanging in STOPPING forever.
- **RDS** — `rds/handler.go`: `AllocatedStorage` now range-checked (20–65536); `BackupRetentionPeriod`
  response field no longer `omitempty` (AWS always emits it). Added `ErrInvalidParameterCombination`.
- **KMS** — `kms/backend.go` + `handler.go`: `ListKeys`/`ListAliases` Limit bounded to 1–1000,
  `ListResourceTags` Limit bounded to 1–50, out-of-range → `ValidationException`.
- **IAM** — `iam/handler.go`: `parseMaxItems` clamps MaxItems to ≤1000 (AWS upper bound).
- **CodePipeline** — `codepipeline/handler.go`: `ListPipelineExecutions` now honors maxResults +
  emits nextToken (previously ignored both); `ListWebhooks`/`ListActionExecutions`/`ListActionTypes`/
  `ListRuleExecutions` output structs gained the NextToken field.
- **Athena** — `athena/handler.go`: `ListQueryExecutions` now honors MaxResults (cap 50) + NextToken
  and omits NextToken on the last page (was hardcoded `""`).
- **IoT** — `iot/handler.go`: `ListThings`/`ListTopicRules`/`ListPolicies` now paginate via
  maxResults + nextToken/nextMarker.
- **EC2 DescribeInstanceStatus** — `ec2/handler_ext.go`: emits `systemStatus`/`instanceStatus` health
  objects (status "ok" + reachability "passed" when running) so SDK `InstanceStatusOk` waiter works.
- **S3** — `s3/object_ops.go`: DeleteObjects >1000 keys now returns `MalformedXML` (was generic
  `InvalidArgument`); `s3/bucket_ops.go`: ListObjects MaxKeys>1000 explicitly clamped to 1000;
  `s3/model.go`: `ListMultipartUploadsResult.Prefix` no longer `omitempty` (AWS always emits `<Prefix>`).
- **StepFunctions / EventBridge** — output-struct `NextToken` fields gained `,omitempty` so the last
  page omits the field (StepFunctions list*Output ×4; EventBridge listEventBuses/listRules/
  listTargetsByRule).

**Verified already-correct / false positives (NO change — would have regressed AWS fidelity):**
- **All "pagination cursor off-by-one" items** (ECR, QuickSight, DataBrew, MQ, AutoScaling, ELBv2):
  each is internally consistent — token is the first-un-returned item with `start = i` (include), or
  the last-of-page item with `start = i+1` (skip). The convention-check caveat applies; none were bugs.
- **SNS XML tag casing** (`isOptedOut`, `phoneNumbers`, `nextToken`, attribute `key`/`value`/`entry`):
  the AWS SDK deserializes these case-insensitively (`strings.EqualFold`), and AWS's real wire format
  for the legacy SMS APIs is lowercase. Current code already matches AWS; PascalCasing would diverge.
- **SQS `queueUrls`**: AWS `ListDeadLetterSourceQueues` genuinely uses lowercase `queueUrls`
  (confirmed in SDK deserializer, case-sensitive JSON). Current code is correct.
- **Cognito `TokenResult` casing**: `TokenResult` is an internal struct; the wire response is
  `authResult` which already uses `IdToken`/`AccessToken`/`RefreshToken`. `UserLastModified` already
  has the `UserLastModifiedDate` JSON tag. `Enabled` correctly lacks `omitempty`.
- **DynamoDB Scan ScannedCount**: `doScan` already increments per-candidate (pre-filter); `Count` is
  post-filter. Correct.
- **DynamoDB DescribeTable StreamSpecification**: AWS omits StreamSpecification when streams were
  never enabled; current behavior matches. `BillingModeSummary` already always present.
- **Lambda `validateMemoryAndTimeout`**: already validates memory (128–10240). `LastUpdateStatus`
  already defaults to `Successful`.
- **SecretsManager ListSecrets MaxResults**: already bounded 1–100 via `validateMaxResults`.
- **SecurityHub `intFromBody`**: returns 0, but `GetFindings`/`paginateSlice` already default 0→100.
- **CloudFormation ListStacks/ListExports/ListStackResources MaxResults**: these AWS ops have **no**
  MaxResults parameter (only NextToken); nothing to bound.
- **S3 `ListBucketResult.Prefix`**: already lacks `omitempty` (AWS-correct).

**Deferred / not done (remaining §P):**
- **Lambda CreateFunction State Pending→Active delay** (`lambda/handler.go:1490`): returns Active
  immediately; SDK `FunctionActiveV2` waiter still succeeds (just doesn't wait), so not a correctness
  bug. Mirroring the DynamoDB create→active delay is a fidelity nicety — deferred.
- **EC2 RequestSpotFleet TargetCapacity≥1** (`ec2/backend_spot_fleet.go`): AWS permits 0-capacity
  fleets and an existing test (`TestRequestSpotFleet_ZeroCapacity`) codifies that; left as `>= 0`.
- **STS DurationSeconds pre-validation in dispatch** (`sts/handler.go`): the backend already validates
  the 900–43200 range with the correct error; moving it earlier is stylistic only — deferred.
- **RDS MonitoringInterval>0 requires MonitoringRoleArn**: AWS-accurate, but existing accuracy test
  `TestMonitoringIntervalValidation` asserts it is accepted without a role; not changed to avoid
  breaking the branch's test contract. `ErrInvalidParameterCombination` was added for future use.
- **RAM list ops MaxResults bound (cap 100)** (`ram/handler.go`): list ops don't parse MaxResults at
  all; adding validation + pagination across ~10 ops is a broad change — deferred.
- **SSM list/describe per-op MaxResults bounds** (`ssm/handler.go`): broad, many ops — deferred.
- **CodePipeline ListWebhooks/ListActionExecutions/ListActionTypes/ListRuleExecutions**: NextToken
  field added to output structs, but actual paging not implemented (backend returns single page) —
  deferred full pagination.
- **ACM / ACM PCA input `NextToken` omitempty** (`acm/handler.go:136`, `acmpca/handler.go:287`): these
  are request (input) structs; omitempty there does not affect the server's wire response — no-op,
  deferred.
- **API Gateway list-op wrapper keys** (`apigateway/handler.go`): needs per-op AWS-shape confirmation
  — deferred (verify item).
- **IAM policy evaluation / SimulatePrincipalPolicy real vs canned** — research/verify item, not a
  discrete line fix — deferred (cross-refs §L platform finding).
- **KMS encryption-context-size error wording** (`kms/backend.go:634`) — minor wording fidelity,
  deferred.

---

# Q. Actionable backlog — additional services (2026-06-10, pass 5)

Second wave of the line-level backlog, covering the services not combed in §P. Same caveats apply
(confirm pagination *direction* before flipping a cursor; `omitzero` is valid under `go 1.26`).

## Pagination — cursor off-by-one (token item returned twice)

Same `start = i` → `start = i + 1` root cause as the §P cluster:
- **AccessAnalyzer** — `services/accessanalyzer/backend.go:421-422` (`ListFindings`).
- **Detective** — `services/detective/backend.go:318` (`ListGraphs`) and `:503` (`ListMembers`).
- **RolesAnywhere** — `services/rolesanywhere/backend.go:1069` (`paginate`); **plus a fully broken
  token generator** at `backend.go:1087-1093` (`nextTokenFromSlice` always returns `""` — the
  `if next < len(all)` guard is inverted, so pagination never advances). Also
  `handler.go:1247-1251` `parsePageParams` silently drops non-digit input instead of erroring.
- **DocDB / Neptune** — `services/docdb/handler.go:2310`, `services/neptune/handler.go:2410`:
  index-based marker has no upper-bounds guard for a decoded marker that exceeds the slice (add
  `if start > len(items) { return empty }`).
- **ResourceGroupsTaggingAPI** — `services/resourcegroupstaggingapi/backend.go:700+`: `GetResources`
  uses a raw integer-offset token that returns wrong results if items are deleted between calls →
  switch to an opaque key-based token.

## Pagination — MaxResults/NextToken ignored or absent in output

- **ApplicationAutoScaling** — output structs missing `NextToken` while the input accepts
  `MaxResults`: `services/applicationautoscaling/handler.go:276` (`DescribeScalableTargets`),
  `:403` (`DescribeScalingPolicies`), `:446` (`DescribeScalingActivities`), `:579`
  (`DescribeScheduledActions`); none validate `MaxResults` 1–50 (`:280-289,407-418,450,553-569`).
- **SSO Admin** — ~19 list ops hardcode `NextToken: nil` instead of computing a cursor
  (`services/ssoadmin/handler.go:390,563,770,844,1037,1531,1566,1592,1618,1634,1662,1883,2070,2093,2116,2203,2253,2370,2407`).
- **Macie2** — `handleListFindings` ignores `MaxResults`/`NextToken` (hardcoded `0`/`""`) and
  doesn't read the body (`services/macie2/handler.go:1194,753-755`); `handleListFindingsFilters`
  takes no body so can't paginate and also **drops the backend error**
  (`handler.go:1164-1167,1165`).
- **MediaConvert** — `NextToken` declared in the output struct but never populated:
  `handleListJobs` (`services/mediaconvert/handler.go:757,868`), `handleSearchJobs`
  (`:1226,1266`) — truncates without offset tracking.
- **MediaPackage** — `handleListChannels` (`services/mediapackage/handler.go:469`),
  `handleListOriginEndpoints` (`:591`), `handleListHarvestJobs` (`:743`) hardcode `maxResults=0`,
  `nextToken=""`.
- **Forecast** — list dispatch doesn't parse `MaxResults`/`NextToken` from the request
  (`services/forecast/handler.go:191`).
- **Polly** — `ListSpeechSynthesisTasks` (`services/polly/handler.go:472`) and `ListLexicons`
  (`:536`) always emit a `NextToken` key even when empty (should be conditional/omitted).
- **CodeStarConnections / CodeConnections** — output structs missing `NextToken`:
  `services/codestarconnections/handler.go:358` (`ListConnections`),
  `services/codeconnections/handler.go:903` (`ListRepositoryLinks`).
- **Account** — `handleListRegions` hardcodes `maxResults=0`, never reads the query param
  (`services/account/handler.go:205`).
- **Route53Resolver** — `ListFirewallDomainLists`/`ListFirewallRules`/etc. accept/return no
  `NextToken` (`services/route53resolver/handler.go`).
- **X-Ray** — `ListResourcePolicies`/`ListRetrievedTraces` have no `NextToken`
  (`services/xray/handler.go`).
- **CloudWatch** — `listResult.NextToken` (XML) lacks `omitempty`, emits empty element
  (`services/cloudwatch/handler.go:967`).

## Bounds / required-field validation

- **AppConfig** — `appConfigPaginationParams` doesn't bound `max_results` (1–100)
  (`services/appconfig/handler.go:1925-1938`).
- **Amplify** — `maxResults` upper bound (1–100) unvalidated (`services/amplify/handler.go:415-418`).
- **Batch** — `ListJobs` doesn't require `JobQueue`/`JobStatus`
  (`services/batch/handler.go:1204-1224`).
- **Elasticsearch** — `InstanceCount` (1–20) and `EBSOptions.VolumeSize/VolumeType` unvalidated on
  create and update (`services/elasticsearch/handler.go:894,900-901,1044-1045`).
- **OpenSearch** — `InstanceCount` max unvalidated and `PurchaseReservedInstanceOffering` returns
  404 for validation errors that should be 400
  (`services/opensearch/handler.go:3236-3250`); `OffPeakWindowOptions`/`IamIdentityCenterOptions`
  lack `omitempty` (`:652`).
- **S3 Control** — `CreateJob` `Priority` not bounded 0–256 (`services/s3control/handler.go:1759`).
- **Glacier** — `ListJobs` limit has no lower bound (`>= 1`) (`services/glacier/handler.go:1112-1117`).
- **Account** — `PutAlternateContact` doesn't validate required fields
  (`services/account/handler.go:243-248`).
- **Inspector2** — `CreateFilter` doesn't validate required `FilterCriteria`/`Name`
  (`services/inspector2/handler.go:468-475`).
- **RedshiftData** — `ExecuteStatement` doesn't enforce that exactly one of
  `ClusterIdentifier`/`WorkgroupName` is set (`services/redshiftdata/handler.go:237-272`).
- **Cost Explorer** — `GetCostAndUsage`/`GetDimensionValues` don't validate
  `TimePeriod.Start < End` or `YYYY-MM-DD` format (`services/ce/handler.go`).
- **MWAA** — confirm `MaxResults` cap matches AWS (`services/mwaa/handler.go:405-407`).

## Response-shape / field-name fidelity

- **DynamoDB Streams** — `GetRecords` response omits `MillisBeforeExpiration`
  (`services/dynamodbstreams/handler.go:264`).
- **API Gateway Management** — `GoneException` returned as
  `{"message":"GoneException",…}` instead of `{"__type":"GoneException","message":…}`
  (`services/apigatewaymanagementapi/handler.go:177-179`).
- **RDS Data** — response field `generatedFields` should be lowercase to match the rest-JSON wire
  shape (`services/rdsdata/handler.go:233` — verify the codebase's RDS-Data key convention).
- **Serverless Repo** — `versionResponse` missing `SourceCodeArchiveUrl`
  (`services/serverlessrepo/handler.go:564`).
- **DAX** — `ClusterDiscoveryEndpoint` is `omitempty` but AWS always returns it in describe
  (`services/dax/handler.go:355`).
- **Glacier** — `Marker` pointer fields should serialize as the AWS string shape
  (`services/glacier/models.go:208,214` — verify).
- **MemoryDB** — `ServiceUpdate.Status` / `SnsTopicStatus` returned without enum validation
  (`services/memorydb/handler.go:1374,1608,1622`).
- **Scheduler** — `flexibleTimeWindowOutput.MaximumWindowInMinutes` (int) lacks `omitempty`, emits
  `0` when Mode=OFF (`services/scheduler/handler.go:902`).
- **Support** — `RecentCommunications` pointer lacks `omitempty` (may serialize `null`)
  (`services/support/handler.go:246`).
- **Bedrock Runtime** — confirm `InvokeModel` returns the model body at the root, not wrapped in a
  container struct (`services/bedrockruntime/handler.go` — verify against the SDK).
- **Elastic Beanstalk** — AutoScaling fields in `EnvironmentDescription` returned as strings rather
  than structured objects (`services/elasticbeanstalk/handler.go` — verify).
- **Account** — `Details.ID` JSON tag case (`Id` vs `id`) — verify against the Account API
  (`services/account/backend.go:39`).

## Error-code / HTTP-status fidelity

- **OpsWorks** — unknown action returns HTTP 501; AWS returns 400 `ValidationException`
  (`services/opsworks/handler.go:181`).
- **Support** — case-not-found returns generic 404; AWS uses `CaseIdNotFound`/`UnknownCaseException`
  — split the error mapping (`services/support/handler.go:234-235`).
- **Route53Resolver** — no mapping for `InvalidParameterException` (invalid CIDR/IP/domain) →
  returns generic error (`services/route53resolver/handler.go`).
- **S3 Tables** — inconsistent `ResourceNotFoundException` (404) vs `ValidationException` (400)
  mapping (`services/s3tables/handler.go`).

## SDK-waiter blocker

- **Forecast** — `describe()` clones the resource *before* applying the status transition, so the
  returned object reports the stale `CREATE_PENDING` while internal state flips to `ACTIVE`; SDK
  waiters never observe `ACTIVE` (`services/forecast/backend.go:157-161`). Apply the transition to
  the stored resource first, then clone.

## Notes on pass 5

The two least-productive sub-passes (messaging/iot/streaming and parts of media) yielded fewer
confident items, so this section is weighted toward pagination and validation where the bugs are
unambiguous. Items tagged "verify" need a quick AWS-shape/SDK confirmation (RDS-Data key case,
Glacier marker, Bedrock-Runtime body wrapping, Elastic Beanstalk AS fields, Account `Id` case)
before applying. Combined, §P + §Q give a downstream agent on the order of ~110 discrete,
file:line-scoped fixes; the pagination off-by-one and "MaxResults ignored / NextToken absent"
clusters are the highest-value because they break real multi-page SDK and Terraform listings.

---

# R. Deep-comb backlog — popular-service sub-operations (2026-06-10, pass 6)

Earlier passes sampled the top-level ops of the biggest services; this pass combs their many
sub-operations. Highest-value findings first (auth correctness, CFN fidelity). Same caveats apply.

## Cognito — authentication correctness (high value; security-relevant)

- **`token_use` claim not validated** — `ParseAccessToken` accepts any well-formed JWT, so an **ID
  token is accepted where an access token is required** (`GetUser`
  `services/cognitoidp/backend.go:843-849`, `GlobalSignOut` `:1643-1656`). Add
  `claims["token_use"] == "access"` enforcement in `tokens.go:212-230`.
- **`auth_time` not preserved across refresh** — `REFRESH_TOKEN_AUTH` sets `AuthTime: now.Unix()`,
  overwriting the original (`services/cognitoidp/backend.go:1156`). Store and reuse the original
  `auth_time` (breaks `GlobalSignOut`-before-revoke semantics otherwise).
- **`ConfirmSignUp` accepts an empty code** — guard is
  `if user.ConfirmCode != "" && code != user.ConfirmCode`, so when no code was stored an empty
  `code` confirms the user (`services/cognitoidp/backend.go:539`). Reject if either side is empty.
- **`ConfirmSignUp` error ordering** — should return `ExpiredCodeException` before
  `CodeMismatchException`; currently checks match after expiry inconsistently
  (`backend.go:535-541`).
- **Identity pools — empty `Logins` bypasses auth** — `GetCredentialsForIdentity` skips validation
  when the request `Logins` map is empty even though the identity has logins, handing out creds
  without a token (`services/cognitoidentity/backend.go:431-436`). Reject with `NotAuthorized`.

## CloudFormation — lifecycle, errors & intrinsics

- **Error-code mapping** — `CreateStack` returns `AlreadyExistsException` for *all* backend errors
  (incl. insufficient capabilities / bad role ARN) (`services/cloudformation/handler.go:586`);
  `UpdateStack` collapses everything to `ValidationError` (`:617`). Map
  `ErrInsufficientCapabilities`→`InsufficientCapabilitiesException`, bad role→`ValidationError`.
- **HTTP status** — all CFN XML errors use 400; AWS Query/CFN returns most operational errors as
  200 with the fault in the body (`handler.go:452`) — verify and align.
- **Unsupported resource type silently succeeds** — `createExtendedResource` returns `("", nil)` for
  an unknown `AWS::*` type instead of failing the stack
  (`services/cloudformation/resources.go:185`). Return an error so the stack goes
  `CREATE_FAILED` as AWS does.
- **`Fn::GetAtt` on an unknown attribute** silently returns the physical ID instead of erroring
  (`template.go:881-912`); **`Fn::Sub` `${Logical.Attr}`** doesn't validate the logical ID exists
  (`template.go:566-568`); **`Fn::ImportValue`** leaves an unresolved sentinel marker in outputs
  with no downstream error (`template.go:545-554`).
- **Capabilities** — comparison is case-sensitive (`backend_parity.go:205`; AWS upper-cases) and the
  `Capabilities` array is `omitempty` so an empty array is dropped from `DescribeStacks`
  (`handler.go:671`).
- **Missing response fields** — `DescribeStacks` omits `DisableRollback`
  (`handler.go:674,692`); `DescribeStackResource` and `ListStackResources` summaries omit
  `ResourceStatusReason` (`handler.go:1001,1057`).
- **Pagination** — `ListStacks` ignores `MaxItems`, always returns up to 100
  (`backend.go:1054`).
- **Dynamic-ref iteration off-by-one** — the 100-iteration cap exits the loop without erroring on
  the 100th `{{resolve:…}}` (`dynamic_refs.go:53,107-111`).
- **ChangeSet `ExecutionStatus`** hardcoded `AVAILABLE` even when there are zero changes (should be
  `UNAVAILABLE`) (`backend.go:1108`).

## EC2 — sub-resource ops (ClientVPN, ENI, NACL, snapshots, volumes)

- **Missing response fields**: `DescribeClientVpnTargetNetworks` item lacks `AssociationId`/`Status`
  (`services/ec2/handler_batch4.go:206`); `CreateImage` response lacks `State`
  (`handler_deepdive_ops.go:162`); `DescribeVpcEndpoints` item lacks `SubnetIds`/`RouteTableIds`
  (`:179`); `DescribeNetworkAcls` item lacks `Associations` (`handler_refinement2.go:217-221`);
  subnet-CIDR-reservation items lack `Status` (`handler_batch2.go:350-357`,
  `handler_batch3.go:330-335`).
- **Wrong XML element names/case**: ClientVPN routes use `routes` not `clientVpnRouteSet`
  (`handler_batch4.go:667-668`); VPN-connection route uses `DestinationCIDR` not
  `destinationCidrBlock` (`handler_batch2.go:384`).
- **Wrong error codes**: ENI in-use is `InvalidParameterValue.NetworkInterfaceInUse`; AWS uses
  `InvalidNetworkInterfaceID.InUse` (`backend_ext.go:50`, `handler.go:1178-1179`).
- **Validation**: `ModifyVolume` silently swallows `strconv.Atoi` parse errors on `Iops`/`Size`
  (defaults to 0) instead of returning `InvalidParameterValue`
  (`handler.go:616-617`); `DisassociateClientVpnTargetNetwork` treats the `AssociationId` form value
  as a subnet ID (`handler_batch4.go:605-620`).
- **Pagination**: `DescribeSnapshots` (`handler_refinement2.go:72-93`) and `DescribeNetworkAcls`
  (`handler_deepdive_ops.go:151-155`) responses have no `NextToken`.

## S3 — multipart & sub-resource serialization

- **`ListPartsResult.NextPartNumberMarker` is typed `int` but AWS returns it as a string**
  (`services/s3/model.go:368`; the handler already string-converts at
  `multipart_ops.go:415-417`) — change the field type and pass the parsed value through
  (`multipart_ops.go:391` passes a string where the backend wants int32).
- **`ListMultipartUploadsResult` has no `CommonPrefixes` field** so delimiter-grouped uploads can't
  return prefixes (`services/s3/model.go:302-313`).
- **`CommonPrefixes` should be `omitempty`** on `ListBucketResult`/`ListBucketV2Result` so an empty
  set omits the element (`model.go:32,48`).
- **`AbortMultipartUpload` doesn't call `h.setOperation`** so it's missing from operation
  metrics/console (`multipart_ops.go:275`).
- **`GetBucketLogging` empty response doesn't set the `xmlns` attribute**
  (`bucket_ops.go:1358-1362`).
- **`HeadObject` on a delete marker** returns `MethodNotAllowed` via the generic error path instead
  of a 404 with `x-amz-delete-marker: true` (`object_ops.go:197-199`, `errors.go:136-140`).

## IAM & DynamoDB sub-ops

- **IAM `GetGroup` ignores `MaxItems`/`Marker`** and returns all members though the response has
  `IsTruncated`/`Marker` (`services/iam/handler.go:1147-1157`) — add paginated
  `GetGroupUsers`.
- **DynamoDB `ItemCollectionMetrics.ItemCollectionKey` is set to the full item** instead of just the
  partition+sort key (`services/dynamodb/item_ops_crud.go:215`) — extract key attrs via the table
  key schema (cf. the correct `UpdateItem` path).
- **DynamoDB LSI 10 GB collection limit not enforced** — `PutItem` never raises
  `ItemCollectionSizeLimitExceededException` for LSI tables (`item_ops_crud.go:209-218`); and
  `SizeEstimateRangeGB` is hardcoded `[0,1]`.

## Remaining services (line-level)

- **VerifiedPermissions JSON key case** — list input structs use lowercase `nextToken`/`maxResults`
  where AWS uses `NextToken`/`MaxResults`
  (`services/verifiedpermissions/handler.go:335,336,346,347,637,648,658,664,665,675,841,848`);
  `CreatePolicyStore` doesn't bound `description` length (255) (`:262-270`).
- **CloudControl** — `ResourceNotFoundException` returns HTTP 404; the CloudControl JSON protocol
  uses 400 (`services/cloudcontrol/handler.go:144`).
- **Timestream** — `ListBatchLoadTasks` output struct omits `NextToken`
  (`services/timestreamwrite/handler.go:1296`); `Query` doesn't return a `NextToken` for paged
  results (`services/timestreamquery/handler.go:273-310`).
- **SageMaker Runtime** — the session header formats expiry as RFC3339 rather than an HTTP-date
  (RFC 7231) (`services/sagemakerruntime/handler.go:228`).
- **EMR Serverless** — `ListApplications` doesn't bound `maxResults` (1–50)
  (`services/emrserverless/handler.go:575-582`).
- **MediaStore Data** — `ListItems` doesn't bound `MaxResults` (≤1000)
  (`services/mediastoredata/handler.go:269-300`); **MediaStore** returns `BadRequestException` for
  an unrecognized `X-Amz-Target` where AWS returns `UnrecognizedClientException`
  (`services/mediastore/handler.go:173`).
- **IoT Analytics** — list responses use `omitempty` on `Items`, dropping the empty array AWS
  always returns (`services/iotanalytics/handler.go:556-576`).
- **Identity Store** — `IsMemberInGroups`/`ListUsers` lack `MaxResults` bound enforcement
  (`services/identitystore/handler.go:450-452,836-838`).

## Notes on pass 6

The Cognito auth items are the highest-value in this whole document — they're correctness/security
gaps (token-type confusion, auth bypass with empty logins) that LocalStack's open tier also gets
wrong, so fixing them is differentiation, not catch-up. The CFN intrinsic-error items make failing
templates fail *correctly* (today several silently succeed). A handful of EC2/S3/DDB items are
tagged for shape-verification against the SDK before applying. With §P+§Q+§R the line-level backlog
now exceeds ~150 discrete fixes.

---

# §G/§H/§O test-coverage progress (parity/mega-v2)

Integration + Terraform tests added on this branch to close the §G, §H, and §O gaps. All compile
under `go vet -tags=integration ./test/integration/...` and `go vet ./test/terraform/...`; they
exercise real SDK / terraform-provider-aws lifecycles (create→read/list→update/delete) and assert
AWS-accurate fields, not smoke tests.

## §G integration tests added (`test/integration/`)

Each is an SDK round-trip against the in-container stack:

- **comprehend** — DetectSentiment (POSITIVE/NEGATIVE/NEUTRAL keyword paths), DetectDominantLanguage,
  EntityRecognizer create→describe→list→delete.
- **translate** — TranslateText (explicit + auto source), Terminology import→get→list→delete.
- **polly** — SynthesizeSpeech (audio stream + content-type), Lexicon put→get→list→delete.
- **rekognition** — Collection create→describe→list→delete (the only stateful resource).
- **guardduty** — Detector and Filter create→get/describe→list→delete.
- **accessanalyzer** — Analyzer and ArchiveRule create→get→list→delete.
- **detective** — Graph create→list→delete.
- **apprunner** — Service (image source) and Connection create→describe/list→delete.
- **fsx** — FileSystem (Lustre) and Backup create→describe→delete.
- **datasync** — Agent and Task (two NFS locations) create→describe→list→delete.
- **directoryservice** — Directory (SimpleAD) create→describe→delete.
- **workspaces** — IpGroup and ConnectionAlias create→describe→delete.
- **appstream** — Stack and Fleet create→describe→delete.
- **securityhub** — Insight create→get→delete (hub-enable tolerated as shared state).
- **macie2** — CustomDataIdentifier create→get→list→delete (regex round-trip).
- **inspector2** — Filter create→list→delete.
- **appmesh** — Mesh and VirtualNode create→describe→list→delete.
- **forecast** — DatasetGroup create→describe→list→delete.
- **personalize** — DatasetGroup create→describe→list→delete.
- **rolesanywhere** — TrustAnchor create→get→list→delete.
- **dax** — SubnetGroup and ParameterGroup create→describe→delete.
- **mediapackage** — Channel create→describe→list→delete.
- **mediatailor** — SourceLocation create→describe→list→delete (HTTP base-URL round-trip).
- **workmail** — Organization create→describe→delete + nested Group create→list→delete.
- **quicksight** — Group (default namespace) create→describe→list→delete.
- **medialive** — InputSecurityGroup create→describe→list→delete (whitelist CIDR round-trip).

## §H / §O Terraform fixtures added (`test/terraform/`)

New `parity_mega_test.go` (own provider block with the §H endpoints) + fixtures under
`test/terraform/fixtures/`:

- **guardduty/success** — `aws_guardduty_detector`.
- **securityhub/success** — `aws_securityhub_account`.
- **workspaces/ipgroup** — `aws_workspaces_ip_group` (two CIDR rules).
- **appstream/stack** — `aws_appstream_stack`.
- **waf/ipset** — classic `aws_waf_ipset` + `aws_waf_rule`.
- **fsx/lustre** — VPC + subnet + `aws_fsx_lustre_file_system`.

## §G/§H/§O remaining (deferred)

- **Integration**: `opsworks`, `account` — AWS SDK v2 modules are not in `go.mod`, so no client can
  be built; deferred until the modules are vendored. `quicksight` asset-bundle/folder-permission
  ops and large-surface AppStream (AppBlock/ImageBuilder/Entitlements) / WorkSpaces
  (Bundles/Images/Pools) sub-resources still need the precise handler↔backend op diff from §I
  before locking in.
- **Terraform**: remaining §H services not yet fixtured — `apprunner`, `comprehend`, `databrew`,
  `datasync`, `directoryservice` (`ds`), `dlm`, `detective`, `forecast`, `macie2`, `medialive`,
  `mediapackage`, `mediastoredata`, `mediatailor`, `personalize`, `polly`, `quicksight`,
  `rekognition`, `rolesanywhere`, `transcribe`, `translate`, `workmail`. Also the §O cross-service
  event e2e (S3→Lambda asserting target receipt), CFN custom-resource round-trip, API Gateway v2
  full-stack-via-CFN, and the `*-comprehensive` multi-resource modules for Logs/Cognito/Glue/AppSync
  remain open.
- **Backend notes surfaced by these tests** (for §P/Q/R agents — not fixed here): per §I,
  MediaTailor `DescribeChannel`/`DescribeProgram`, GuardDuty malware-protection ops, SecurityHub
  `BatchGetAutomationRules`/`GetFindingStatistics`, Inspector2 `ListFindings`, and Macie2
  `DescribeBuckets` remain empty-stub; the added tests deliberately target the stateful ops that
  do round-trip and avoid asserting on those known-empty paths.

---

# Q/R implementation status (pass-5/6 line-level fixes)

Implemented genuine items from §Q (pass 5) and §R (pass 6). Each was verified against current
code first; many flagged items were confirmed false-positives and skipped (applying them would have
regressed fidelity).

## Implemented (with table-driven tests)

- **Cognito IDP** (`tokens.go`, `backend.go`): enforce `token_use=="access"` in `ParseAccessToken`
  (rejects an ID token at GetUser/GlobalSignOut); preserve original `auth_time` across
  `REFRESH_TOKEN_AUTH` (stored on `refreshTokenEntry`); `ConfirmSignUp` rejects an empty/cleared
  stored code for an unconfirmed user while keeping re-confirm idempotent.
- **Cognito Identity** (`backend.go`): `GetCredentialsForIdentity` rejects an empty `Logins` map for
  an authenticated identity (closes the auth-bypass) with `NotAuthorized`.
- **CloudFormation** (`handler.go`, `backend.go`, `dynamic_refs.go`): CreateStack/UpdateStack map
  backend errors to distinct AWS codes (AlreadyExistsException / InsufficientCapabilitiesException /
  ValidationError); empty change set → `FAILED` / `UNAVAILABLE`; DescribeStacks always serializes
  `DisableRollback`; `resolveDynamicRef` off-by-one fixed (exactly-limit refs now resolve).
- **RolesAnywhere** (`backend.go`, `handler.go`): fixed `nextTokenFromSlice` (always returned ""),
  so pagination advances; `parsePageParams` returns ValidationException for non-numeric maxResults.
- **OpsWorks** (`handler.go`): unknown action → HTTP 400 ValidationException (was 501).
- **VerifiedPermissions** (`handler.go`): CreatePolicyStore bounds description at 150 chars.
- **EMR Serverless** (`handler.go`): ListApplications/ListJobRuns/ListJobRunAttempts bound
  maxResults to 1-50.
- **MediaStore Data** (`handler.go`): ListItems bounds MaxResults to 1-1000.
- **Identity Store** (`handler.go`): ListUsers bounds MaxResults to 1-100.
- **Batch** (`handler.go`): ListJobs requires `jobQueue` (jobStatus stays optional).
- **Polly** (`handler.go`): ListSpeechSynthesisTasks/ListLexicons omit NextToken when empty.
- **API Gateway Management** (`handler.go`): GoneException returned in rest-json shape
  (`X-Amzn-Errortype` header + body `__type`, human-readable `message`).
- **S3 Control** (`backend.go`): CreateJob rejects a negative Priority.
- **Account** (`handler.go`): PutAlternateContact validates the five required fields.

## Verified false-positives (skipped — applying would regress fidelity)

- **AccessAnalyzer `ListFindings` / Detective `ListGraphs`,`ListMembers` off-by-one**: the page
  token is the *first item of the next page*, so `start = i` is correct; `start = i+1` would skip an
  item.
- **DocDB / Neptune marker upper-bounds**: both `applyDocDBMarker`/`applyNeptuneMarker` already
  guard `start >= len(items)`.
- **CFN `ListStacks` MaxItems**: AWS ListStacks has no MaxItems parameter (NextToken-only).
- **CFN Capabilities case-insensitivity**: AWS capabilities are case-sensitive; lowercasing would be
  less accurate.
- **VerifiedPermissions `nextToken`/`maxResults` casing**: the whole service uses camelCase
  (awsjson1_0); PascalCase would break consistency.
- **CloudControl `ResourceNotFoundException` 404→400**: the modeled error carries `@httpError(404)`.
- **DynamoDB Streams `MillisBeforeExpiration`**: no such field on DDB Streams GetRecords (that is
  Kinesis `MillisBehindLatest`).
- **Scheduler `MaximumWindowInMinutes` omitempty**: it already has `omitempty`.
- **Support `RecentCommunications` omitempty**: it already has `omitempty`.
- **Account `ListRegions` maxResults**: already reads the query param; **Account `Details.Id`
  casing**: PascalCase is consistent and AWS-accurate.
- **Glacier `ListJobs` lower bound**: already validated (`n < minListLimit`).
- **MediaStore unrecognized X-Amz-Target → UnrecognizedClientException**: that exception is for
  invalid credentials, not a bad target; BadRequestException is more defensible.

## Deferred (genuine but invasive / lower-confidence — not done here)

- **CFN `Fn::GetAtt`/`Fn::Sub`/`Fn::ImportValue` error propagation** and **unsupported-resource-type
  failure**: require threading `error` through the entire string-returning intrinsic resolver and
  reclassifying intentionally-stubbed (valid-but-unimplemented) resource types vs. true unknowns —
  large refactor with high regression risk against the existing stub fallbacks.
- **Inspector2 `CreateFilter` requires `filterCriteria`** and **RedshiftData `ExecuteStatement`
  exactly-one of ClusterIdentifier/WorkgroupName**: both are AWS-accurate but the existing test
  suites create these resources without those fields as ubiquitous fixtures, so enforcing the
  constraint cascades into dozens of unrelated test updates.
- **ApplicationAutoScaling / SSO Admin / Macie2 / MediaConvert / MediaPackage / Forecast NextToken
  population**: real token pagination needs deterministic ordering (lists are built from map
  iteration) plus backend signature changes across many ops — sizeable, deferred.
- **AppConfig/Amplify/Glacier/MWAA/Cost Explorer/Elasticsearch/OpenSearch bounds & shape "verify"
  items**: shared paginate helpers return no error (ripples to many callers) or have ambiguous exact
  bounds (AppConfig 1-50 vs the note's 1-100); left for a focused follow-up.
- **DAX `ClusterDiscoveryEndpoint` omitempty**, **Support CaseIdNotFound 400/`__type`**: ambiguous
  vs. the codebase's established 404/`{"message":...}` convention; low value.
