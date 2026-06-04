# LocalStack Parity Findings

Audit performed against gopherstack `services/*` handlers vs the AWS SDK v2 surface and LocalStack feature coverage. Items below are confirmed by reading the relevant handler/backend files or by integration-test failures.

## Critical (wrong wire protocol — clients hard-fail)

1. **DAX** — `services/dax/handler.go:71` routes on `X-Amz-Target: AmazonDAXV3.*`, but the AWS DAX SDK speaks a bespoke TLS/framed binary protocol on a non-HTTP listener. Real SDK never reaches this handler. (Surfaced via failing integration test.)
2. **XRay** — the handler mapped `POST /EncryptionConfig` to `PutEncryptionConfig`, but the AWS SDK v2 sends `PutEncryptionConfig` to the operation-named path `POST /PutEncryptionConfig` and `GetEncryptionConfig` to `POST /EncryptionConfig`. Real SDK `PutEncryptionConfig` calls reached the wrong handler. (Fixed: `services/xray/handler.go` now serves `GetEncryptionConfig` on `POST /EncryptionConfig` and adds the `POST /PutEncryptionConfig` route; covered by `test/integration/xray_test.go`.)
3. **ELBv2** — `AddTags`, `RemoveTags`, `RegisterTargets`, `DeregisterTargets` originally omitted the `*Result` XML wrapper required by AWS Query protocol. SDK deserialiser raised `… node not found`. (Fixed in this PR.)
4. **Classic ELB** — `AddTags`, `RemoveTags` had the same missing `*Result` wrappers. (Fixed in this PR.)
5. **Classic ELB `CreateLoadBalancer`** — ignored the `Tags.member.N` form parameters that AWS accepts at create time. Initial tags were silently dropped. (Fixed in this PR; subsequent `DescribeTags` now returns them.)

## SDK ops missing from gopherstack handlers (new upstream surface)

EMR Serverless interactive session and resource-dashboard operations are now implemented.

## Behavioural parity gaps verified by reading handler/backend

12. **APIGatewayManagementApi admin `PruneIdle`** — was timing-sensitive (15 ms threshold + 20 ms sleep) and flaked on contended CI runners. (Tightened in this PR to a 50 ms threshold + 100 ms sleep.)
13. **API Gateway list response shapes** — `keyItem` ("items") used for most list ops, `keyStagesItem` ("item") only for `GetStages`; any new list op that copy-pastes from another will use the wrong JSON key (verified via existing memory).
14. **Cognito IDP persistence** — `Snapshot/Restore` only persists fields declared in `services/cognitoidp/persistence.go`; any newly added backend state on `InMemoryBackend` will silently drop on restore.
15. **APIGateway persistence** — only persists APIs, apiKeys, basePathMappings, domainNames, domainNameAccessAssociations, usagePlans, usagePlanKeys; ResourcePolicies, Stages, Deployments, Models, RequestValidators etc. are NOT persisted across `Snapshot/Restore`.
16. **ELBv2 persistence** — Snapshot/Restore only persists fields declared in `services/elbv2/persistence.go`; new fields silently drop.
17. **IoTWireless persistence** — same pattern; partial backend snapshot.
18. **Kinesis persistence** — only `Streams`, `ResourcePolicies`, `AccountID`, `Region` survive snapshot; consumer registrations, enhanced fan-out and shard iterators do not.
19. **CloudWatch persistence** — alarms, metric streams and dashboards that aren't in `backendSnapshot` are silently dropped on restore.
20. **EC2 persistence** — `services/ec2/persistence.go` `backendSnapshot` does not include all newer types (e.g. transit gateways, vpc endpoints), so anything outside the snapshot is lost.
21. **MediaConvert persistence** — same partial-snapshot pattern.
22. **IAM persistence** — same partial-snapshot pattern.
23. **CloudFormation provider wiring** — when a new `ServiceBackends` field is added, `cloudformation/provider.go` `BackendsProvider`/`extractAllServiceBackends` must be updated or the new backend stays nil at CFN-resource resolution time. Easy regression.

## Integration-test coverage gaps for popular services

Services with substantial handlers but no AWS-SDK-driven integration test under `test/integration/`:

24. **dynamodbstreams** — no SDK-driven test (DDB stream consumption regressions undetected).
25. **databrew** — no SDK-driven test.
26. **iotdataplane** — no SDK-driven test.
27. **sagemakerruntime** — no SDK-driven test.
28. **bedrockruntime** — no SDK-driven test.
29. **appconfigdata** — no SDK-driven test.
30. **apigatewaymanagementapi** — no SDK-driven test (no WebSocket integration coverage).
31. **acmpca** — no SDK-driven test.
32. _Removed: ElasticTranscoder service was deleted; AWS discontinued Elastic Transcoder on Nov 13, 2025._

## Persistence wiring gaps (silently dropped state on Snapshot/Restore)

For each of the following services, only specific named fields are persisted, so any backend field added later without updating the matching `persistence.go` `backendSnapshot` will silently drop on restore. These are listed here as ongoing risks rather than concrete current bugs:

33. APIGateway (citation in memory).
34. CloudWatch (citation in memory).
35. CognitoIDP (citation in memory).
36. EC2 (citation in memory).
37. ELBv2 (citation in memory).
38. IAM (citation in memory).
39. IoTWireless (citation in memory).
40. Kinesis (citation in memory).
41. MediaConvert (citation in memory).

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

S3:
- **Suspended-versioning DELETE data loss** — `backend_memory.go` `deleteLatestVersion` deleted the entire object (all versions) on an unversioned delete against a Suspended bucket. AWS removes only the `null` version, inserts a `null` delete marker, and preserves non-null versions. Fixed and covered by `TestSuspendedVersioningDeletePreservesVersions`.
- **Data race in `storePart`** — the per-bucket uploads inner map was indexed after releasing `b.mu`, racing with `CreateMultipartUpload`. The lookup now happens while the read lock is held.
- **List pagination dead-end** — `truncateListResults` could return `IsTruncated=true` with an empty marker when a page filled exactly with object keys while CommonPrefixes remained, stranding the client. The marker now falls back to the last returned key.
- **Conditional `*` wildcard on GET/HEAD** — `If-Match: *` / `If-None-Match: *` were compared literally against the ETag. They now correctly match any existing representation (412/304 semantics).

## Correction from earlier draft

A previous version of this document listed WAFv2, S3 Tables and SES handlers (~50 ops) as "empty stubs" based on a grep for `return nil, nil`. That detection was a false positive: those handlers DO call their `h.Backend.X(...)` first and then return the empty AWS Query envelope, which is the correct response shape for void-result operations. They are NOT parity gaps. This document has been corrected.
