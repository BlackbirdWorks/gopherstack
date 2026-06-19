# gopherstack Parity Roadmap

**Purpose.** This is a *forward-looking* plan for reaching and exceeding LocalStack
parity. It replaces the previous per-PR audit log; that history lives in git. Every
item below is open or planned work. The goal throughout: **match every feature
LocalStack's open tier provides, and exceed it** with real (not canned) emulation,
Terraform-validated round-trips, and SDK integration tests.

**How to use this doc.** Pick from a priority tier, confirm the cited code is still
accurate (the codebase moves fast — re-verify before starting), implement with
table-driven tests, and run the pre-push gate in `AGENTS.md`. When an item lands,
delete it from here. Keep this doc a plan, not a changelog.

**Working principles (apply to every item):**
- **Region support** — services with regional resources must derive region from the
  ctxbag (`awsmeta.Region(ctx)`), not a hardcoded literal or a construction-time
  default. The ctxbag is populated for every request by `awsMetaMiddleware` (`cli.go`).
- **ctxbag for AWS metadata** — read account/region/partition/request-id via
  `pkgs/awsmeta` off the context; do not re-derive from the raw `*http.Request`.
- **Consistent logger** — log only via `logger.Load(ctx)`; never embed a `*slog.Logger`
  on a backend/handler struct. The request logger is already scoped per request and
  tagged `service=<name>` by the registry — inherit it, don't replace it.
- **No stubs that lie** — an advertised op must mutate/return real state or return the
  AWS-accurate error. A success envelope over a no-op is a parity bug.

**Baseline already in place (don't re-do):** core services (DynamoDB, SQS, Lambda
on real Docker, S3, EC2 CRUD, KMS, IAM, CloudWatch) are close to AWS-accurate;
most cross-service event wiring works (§ "Cross-service wiring" below is a strength);
~202 integration tests and a Terraform suite exist; SigV4 validation, opt-in TLS,
embedded DNS, init hooks, persistence, and the ctxbag/logger middleware are wired.

---

## Tier P0 — correctness gaps a real client hits immediately

These break SDK/Terraform/CFN round-trips or silently drop data. Highest ROI.

### Emulation accuracy
- **DLQ / RedrivePolicy delivery paths.** Failed HTTP/Lambda/SQS deliveries are dropped
  instead of routed to a DLQ:
  - SNS subscription delivery + `replayMessagesToSubscription` ignore `RedrivePolicy`
    (`services/sns/backend.go`); `SetSubscriptionAttributes` accepts a `RedrivePolicy`
    without validating the target SQS DLQ exists.
  - EventBridge `deliverToTargetBounded` ignores target `RedrivePolicy`/DLQ
    (`services/eventbridge/delivery.go`).
- **Pagination — ignored `MaxResults`/absent `NextToken`** (clients loop forever or
  miss pages): RDS `DescribeDB*ParameterGroups`/`DescribeDBParameters`/`DescribeOptionGroups`
  (`services/rds/handler.go`); API Gateway v2 list ops (`services/apigatewayv2/handler.go`);
  MQ uses name-based cursors that break on concurrent add/remove (`services/mq/handler.go`).
- **DynamoDB validation gaps.** Accepts `ConsistentRead=true` on GSI/LSI queries (AWS →
  `ValidationException`, `item_ops_query.go`); `BatchGetItem` returns duplicate keys twice
  (`item_ops_batch.go`); `UpdateTable` doesn't re-check the 20-GSI ceiling (`table_ops.go`).
- **S3 `CompleteMultipartUpload`** does not reject an empty parts list (AWS →
  `InvalidRequest`) (`services/s3/backend_memory.go`).
- **Persistence hooks that drop state.** DAX `Snapshot()`/`Restore()` are declared but
  unimplemented (`services/dax/interface.go`), so DAX state vanishes on snapshot/restore.
  Audit every backend whose `persistence.go` enumerates named fields — any field added
  later without updating `backendSnapshot` silently drops on restore.

### Tests (lock the above in)
- **Cross-service event e2e** asserting the *target received* the event: S3→Lambda,
  SNS→SQS, EventBridge→StepFunctions, DynamoDB-Streams→Lambda,
  CW-Logs-subscription→Firehose. These guard the wiring that is currently a strength.
- **CFN custom-resource round-trip** (`Custom::`/`AWS::CloudFormation::CustomResource`,
  Lambda-backed) — single biggest CloudFormation gap.

---

## Tier P1 — feature breadth to match/exceed LocalStack

### A. Empty-stub finding ops in security services (pure upside — LocalStack returns empty too)
Make findings/detectors seedable and round-trippable. (`inspector2` `ListFindings` is
already seedable — model the rest on it.)
- **GuardDuty** — `GetMalwareProtectionPlan` / `SendObjectMalwareScan` lack handlers;
  member-detector state untracked; member/invitation maps grow unbounded.
- **SecurityHub** — `BatchGetAutomationRules` → `[]`; `ListEnabledProductsForImport`
  always empty; `GetFindingStatistics` empty; `DescribeStandards` hardcoded.
- **Macie2** — `DescribeBuckets` / `GetBucketStatistics` / `SearchResources` return empty.
- **Detective** — investigation + datasource-ingest state not persisted; `ListIndicators`
  hardcoded.
- **AccessAnalyzer** — `StartResourceScan` is a no-op; findings never transition
  ACTIVE→ARCHIVED via archive rules.

### B. Real (non-canned) inference — exceed LocalStack
These return deterministic mock results today (parity-neutral). Replacing with light real
logic is a differentiator:
- **Comprehend** — sentiment is keyword-match w/ hardcoded scores; entities tag every
  capitalised word `PERSON`; `DetectDominantLanguage` always `en`.
- **Translate** — `translateText`/`translateDocument` echo input; terminologies ignored.
- **Polly** — `SynthesizeSpeech` returns a marker string, not audio; speech marks stubbed.
- **Transcribe** — synthetic transcript text; no media read; call-analytics not analysed.
- **Rekognition** — `DetectLabels`/`DetectText`/`DetectModerationLabels`/etc. return empty;
  `SearchFacesByImage` ignores the image (fixed 90.0 similarity).

### C. Media / data sub-resource ops (missing ops break Terraform `Read`)
- **MediaTailor** — `Describe*` return empty; `Start/StopChannel` don't transition state.
- **MediaPackage** — no PackagingConfiguration CRUD, no `Put/GetLifecyclePolicy`.
- **MediaLive** — `CreateInputDeviceMaintenanceWindow` / `ListClusterAlerts` are no-ops.
- **Forecast** — no Explainability ops.
- **Personalize** — `GetRecommendations` (core inference op) absent from the route table;
  `DescribeFeatureTransformation` fabricated. (Needs the `personalize-runtime` endpoint.)
- **DirectoryService** — certificate ops + conditional-forwarder ops absent;
  `RestoreFromSnapshot` returns success without work.

### D. Other stubbed/incorrect ops
- **CloudTrail** — `LookupEvents` always empty, ignores all filters; events never recorded.
- **CodePipeline** — `ListActionExecutions`/`ListRuleExecutions`/`ListRuleTypes` empty; no
  execution/rule tracking.
- **AppSync** — `EvaluateCode` returns hardcoded `{}`; never runs the APPSYNC_JS code.
- **Kafka** — `Update{Connectivity,Monitoring,Rebalancing,Security,Storage}` are no-ops.
- **WAFv2** — ~12 ops return `nil,nil` with no body where the SDK expects `LockToken`;
  `DescribeManagedRuleGroup` hardcoded; `GenerateMobileSdkReleaseUrl` fake.
- **CloudFront** — 60+ stubbed APIs return minimal empty XML rather than data/errors.
- **EC2** — `RevokeSecurityGroupEgress` is a no-op (AWS → `InvalidPermission.NotFound`).
- **OpsWorks** — largely unimplemented (`UnsupportedOperationException`).

### Tests
- **Terraform fixtures** (`success.tf` + `import.tf` + `drift.tf`) for the still-unfixtured
  services — Terraform is the strongest parity signal (validates shapes, waiters, drift):
  `apprunner`, `comprehend`, `databrew`, `datasync`, `directoryservice`, `dlm`, `detective`,
  `forecast`, `macie2`, `medialive`, `mediapackage`, `mediastoredata`, `mediatailor`,
  `personalize`, `polly`, `quicksight`, `rekognition`, `rolesanywhere`, `transcribe`,
  `translate`, `workmail`.
- **`*-comprehensive` multi-resource Terraform modules** for Logs (Metric/Subscription
  filters), Cognito (IdentityPool + role attachment), Glue (Crawler/Table/Trigger), AppSync
  (DataSource/Resolver) — common in real stacks, currently un-fixtured at resource level.
- **API Gateway v2 full stack** (`Api`+`Integration`+`Route`+`Stage`+`Authorizer`) via
  Terraform *and* CFN.
- **Integration tests** for `opsworks` and `account` (blocked: SDK v2 modules not in
  `go.mod` — vendor first); precise handler↔backend op-diff then tests for AppStream
  (AppBlock/ImageBuilder/Entitlements) and WorkSpaces (Bundles/Images/Pools) sub-resources.

---

## Tier P2 — platform, performance, leaks, and the big architectural items

### ctxbag / region migration (incremental, now safe)
The ctxbag is populated for every request, so backends can migrate off their
`DefaultRegion` struct field to `awsmeta.Region(ctx)` (keeping the field as fallback
default) one service at a time. Track per-service; no behavioural gap remains because the
per-request region they derive already matches what the middleware stores. Audit for any
remaining hardcoded `"us-east-1"` in non-test production paths and route them through the
ctxbag.

### Platform features vs LocalStack
- **Multi-account / multi-region isolation** *(largest gap)* — state is not partitioned by
  account/region; the request's account/region is ignored. Cross-cutting re-architecture of
  every backend's state-keying + persistence format. Plan + incremental path in
  `MULTI_ACCOUNT.md`. Once it lands, add a test writing to two accounts/regions asserting
  separation.
- **CBOR protocol** — not implemented; newer DynamoDB/Kinesis/Timestream SDKs use it.
- **Persistence save/load API** — persistence is background-only; add an explicit
  Cloud-Pods-style export/import endpoint (today there's no API to trigger/export a snapshot).
- **Single edge-port multiplexing** — no LocalStack-style `:4566` edge with host/SNI-based
  routing (services share one listener via the priority router instead).

### CloudFormation resource-type coverage (backends mostly exist; just wire the providers)
- **API Gateway v1:** `Model`, `RequestValidator`, `Authorizer`, `ApiKey`, `UsagePlan`,
  `UsagePlanKey`, `DomainName`, `BasePathMapping`, `Account`, `GatewayResponse`.
- **API Gateway v2:** `DomainName`, `ApiMapping`.
- **Events:** `ApiDestination`, `EventBusPolicy`. **KMS:** `ReplicaKey`.
- **Cognito:** `IdentityPool`, `IdentityPoolRoleAttachment`, `UserPoolDomain`, `UserPoolGroup`.
- **EC2:** `VPCPeeringConnection`, `NetworkAcl`(+`Entry`), `KeyPair`, standalone
  `SecurityGroupIngress`/`Egress`, `FlowLog`. **ELBv2:** `ListenerRule`.
- **Lambda:** `EventInvokeConfig`, `Url` (methods exist on the concrete backend but not the
  `StorageBackend` interface — widen the interface or type-assert).
- **ApplicationAutoScaling:** `ScalableTarget`, `ScalingPolicy`.
- **Secrets Manager:** `RotationSchedule`, `SecretTargetAttachment`.
- **SSM:** `MaintenanceWindow`, `Association`. **DynamoDB:** `GlobalTable`.
- **Glue:** `Crawler`, `Table`, `Trigger`, `Connection`, `Partition`.
- **AppSync:** `DataSource`, `Resolver`, `FunctionConfiguration`, `ApiKey`.
- **Extensibility (high value):** `Macro`, `WaitCondition`/`WaitConditionHandle`.

### EC2 deep accuracy (structural — each is a sub-project)
- **IMDSv2** not enforced (no `169.254.169.254` endpoint, no token TTL).
- **Security-group traffic evaluation** (rules are *validated* but not *evaluated* against a
  packet path — no network isolation).
- **Routing/NAT/IGW** don't route packets; **EBS snapshots** don't capture data; **Spot** has
  no market price / interruption.

### Performance hotspots (re-verify file:line before optimizing)
- **EventBridge** — `deliverEvents` deep-copies all bus rules+targets on every `PutEvents`.
- **Step Functions** — history append holds the global write lock (serialises executions);
  execution lookup/delete by name is O(n).
- **EC2** — `DescribeInstances` (no IDs) shallow-copies every instance under lock.
- **KMS** — `findGrantByToken` linear-scans the grant map per encrypt/decrypt (needs index).
- **SSM** — `GetParametersByPath` scans all parameters (needs prefix/trie index).
- **CloudWatch Logs** — metric-filter matching is O(filters × events).
- **ECR / ECS / Batch / Forecast / OpenSearch / Organizations / AppRunner / DMS** — various
  O(n) reference/uniqueness scans for want of reverse indexes.
- **CloudWatch** — datapoint overflow re-slices the retained window per write (ring buffer).

### Resource leaks (open)
- **LakeFormation** — `permissions` is an unbounded `[]*PermissionEntry`. `RevokePermissions`
  drops zero-permission entries, so it's grant-proportional rather than churn, but the right
  fix is a `(principal,resource)`-keyed map (also fixes the O(n) `findPermissionEntry` scan).
- **Step Functions** — `pendingTaskQueues` channels never closed on activity delete (goroutine
  leak); `tasksByToken` never evicts; `executions`/`history` have no TTL.
- **Others to re-verify** (some may already have caps/janitors — confirm before acting):
  S3 `pendingObjectLambdaRequests` eviction; KMS `keyMaterialHistory` past-cap discard;
  STS `GetCallerIdentity` check-then-delete TOCTOU; Kinesis Analytics v1/v2 maps on app delete;
  Pinpoint append-only histories; Secrets Manager rotation-queue depth.

### Dashboard (exceed LocalStack — a console that visualises every service)
- **20 backend-only services with no UI page:** accessanalyzer, account, appmesh, databrew,
  datasync, dax, detective, directoryservice, dlm, forecast, macie2, medialive, mediapackage,
  mediatailor, opsworks, personalize, qldb, quicksight, rolesanywhere, workmail.
- **Per-service CloudWatch metric charts** — the dashboard proto already exposes
  `RuntimeMetrics` + per-op latency (`OperationSummary`); no page renders time-series yet.
- **Global resource search / tag explorer** across services.

---

## Cross-service wiring — a strength (keep covered, don't regress)
Implemented and worth protecting with the P0 e2e tests above: S3 notifications→SQS/SNS/
Lambda/EventBridge; SNS→SQS/Lambda/Firehose/HTTP(S)/email; Lambda ESM for SQS/DDB-Streams/
Kinesis; EventBridge rule→8 targets w/ retries+DLQ; Pipes + Scheduler; CloudWatch alarm→SNS;
Firehose→S3+Lambda transform; Step Functions task integrations; CW-Logs subscription→Lambda/
Kinesis/Firehose.

---

## Confidence / caveats
Items are read-confirmed against the cited files but the tree changes constantly — **re-verify
each before starting**, since prior passes may have closed it. The "canned inference" items are
intentional mocks flagged as opportunities, not bugs. AppStream/WorkSpaces sub-resource gaps
need a precise handler↔backend op diff (the size-only check was inconclusive). `golangci-lint`
in some environments is built against an older Go than the module targets (go1.26.x) and may
refuse to run — use `go vet` + `gofmt` as the fallback gate.
