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
*(2026-06-19 re-verification: EventBridge DLQ, RDS + API Gateway v2 pagination, DynamoDB
GSI-ConsistentRead + BatchGetItem-duplicate validation, S3 empty-parts rejection, DAX
Snapshot/Restore, and EC2 RevokeSecurityGroupEgress were all confirmed FIXED and removed
from this list. Re-verify before re-adding.)*
- **SNS DLQ — Lambda/Firehose subscription paths.** HTTP/HTTPS delivery correctly routes
  failures to the subscription DLQ, but `deliverToLambdaSubscriptions` /
  `deliverToFirehoseSubscriptions` (`services/sns/backend.go:1831-1832`) deliver through the
  event emitter with no DLQ/redrive on failure. Also, `SetSubscriptionAttributes`
  (`services/sns/backend.go:~1031`) validates the `RedrivePolicy` JSON shape but does **not**
  verify the `deadLetterTargetArn` SQS queue actually exists.
- **MQ pagination** uses name-based cursors (`brokers[maxResults-1].BrokerName`) that break
  consistency when items are added/removed between pages; AWS uses opaque tokens
  (`services/mq/handler.go`). *(Verify — other pagination items in this area are now fixed.)*
- **DynamoDB `UpdateTable`** does not re-validate the 20-GSI per-table ceiling on the add
  path (`services/dynamodb/table_ops.go`). *(Verify.)*
- **Persistence hooks that drop state.** Audit every backend whose `persistence.go`
  enumerates named fields — any field added later without updating `backendSnapshot`
  silently drops on restore. (DAX is now implemented; use it as the reference pattern.)

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
- **Glue stubs + missing validation** (`services/glue/handler_stubs.go`): `CreateIntegration{Resource,Table}Property` accept empty input and
  return success with no backend call (~440-455); `DescribeConnectionType`,
  `GetDataQualityModelResult`, `GetIntegration{ResourceProperty,TableProperties}` return empty
  structs (~1307,1763,1916,1932); `CancelMLTaskRun` / `GetBlueprintRun` /
  `GetColumnStatisticsTaskRun` / `CancelStatement` return success on missing required IDs instead
  of `ValidationException` (~228,1398,1615,245). Lowest-risk wins: add the required-field
  validation first.
- **Redshift** — `GetIdentityCenterAuthToken` returns a hardcoded `stub-auth-token` valid until
  2099, ignoring all input (`services/redshift/handler_completeness.go:~882`).

### Tests
- **Terraform fixtures** (`success.tf` + `import.tf` + `drift.tf`) — Terraform is the strongest
  parity signal (validates shapes, waiters, drift). **NOTE: this list is stale** — 19 of the
  originally-listed services were since fixtured. See the *Audit refresh (2026-06-19) → Test
  coverage* section below for the accurate current gap lists (Terraform-missing: accessanalyzer,
  account, appmesh, bedrockagent, cleanrooms, dax, inspector2, networkmonitor, omics, opsworks;
  plus ~84 single-resource fixtures needing import/drift).
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
per-request region they derive already matches what the middleware stores.

**Confirmed region-support gaps — hardcoded region/account in ARN builders** (a resource
created in `eu-west-1` still reports a `us-east-1` ARN, breaking region-aware clients and
multi-region Terraform). Fix pattern: thread `ctx` into the `Create*` path and build ARNs
from `awsmeta.Region(ctx)` / `awsmeta.Account(ctx)` with a default-region fallback — see
**IoT Analytics** (`services/iotanalytics/backend.go`), done as the reference exemplar.
Remaining (verify file:line before starting):
- **API Gateway v1** — `CreateDomainName` regional-domain hostname (`backend.go:~1623`) and
  `CreateDomainNameAccessAssociation` ARN (`backend.go:~1666`) hardcode `us-east-1`. **Blocked
  on a dispatch refactor:** the handler dispatches via `actionFn = func([]byte) (int, any,
  error)` with 106 closures and no `ctx`, so threading the request region requires widening
  `actionFn` to carry `context.Context` (or the request) across all 106 sites first. The
  backend methods themselves are trivial to make ctx-aware once the dispatch carries it. Do
  the `actionFn` signature change as its own PR, then this becomes a 2-line fix.
- ~~**API Gateway v2**~~ — *done*: `CreateAPI`/`CreateDomainName`/`CreateRoutingRule` now
  thread `ctx` and build the `execute-api` endpoint hostname + routing-rule ARN from
  `regionFromCtx(ctx)` (CFN provisioners pass `context.Background()` → default region, no
  regression). A second reference exemplar alongside IoT Analytics.
- ~~**MediaPackage**~~ — *done*: ingest (`newIngestEndpoints`) and egress (`CreateOriginEndpoint`)
  endpoint URLs hardcoded `us-east-1` while the resource ARNs already used the backend's
  configured `region`; the URLs now use `b.region` too (consistency fix). Note: this service
  carries a construction-time `b.region` field, so it is in the "region-on-backend" class
  below, not the per-request-ctx class — the URLs now at least honor the configured region.
- **Region-on-backend services (larger migration).** Several services store `region`/`accountID`
  on the backend at construction (from CLI) and build ARNs from those fields, so they already
  honor the *configured* region but not the *per-request* ctxbag region. Converting these to
  per-request `awsmeta.Region(ctx)` is a multi-site change per service (threading `ctx` through
  every `Create*`). Largest/most-used: **IoT** (~13 ARN sites in `services/iot/backend.go` via
  `b.region`/`b.accountID`), plus **Athena** (package-level `arnRegion`/`arnAccount` consts),
  **Personalize**, **EFS** (managed-KMS-key const). Lower priority than the literal-`us-east-1`
  bugs since they already reflect the configured region.
- **SecurityHub** — the built-in standards ARNs hardcode `us-east-1`
  (`services/securityhub/backend.go:~189-217`). More involved than a single builder: the
  ARNs are package-level static data threaded through `DescribeStandards`,
  `BatchEnableStandards` (subscription matching), `GetEnabledStandards`, and
  `DescribeStandardsControls`, with tests asserting `us-east-1` ARNs. Fix needs region
  templated at request time across those 4 methods, with matching normalized on the
  region-less standard path. (Note: the CIS 1.2.0 `ruleset` ARN is genuinely region-less in
  AWS — keep it as-is.)
- **MemoryDB** (`services/memorydb/handler.go:~1600`) and **Kafka/MSK**
  (`services/kafka/handler.go:~1500`) silently fall back to `us-east-1` when the region is
  empty/unparseable instead of using the ctxbag region.
- Sweep `services/` for other `:us-east-1:` literals in non-test ARN construction.

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

## Audit refresh — new findings (2026-06-19, four parallel sweeps)

A fresh four-way audit (region/ctxbag/logger · emulation accuracy · perf/leaks · test
coverage). Only NEW items not already listed above are recorded here; each is file:line-cited
and tagged with confidence. **Documentation only — nothing fixed in this pass.** Known
false-positive pattern excluded: a handler that calls `h.Backend.X(...)` and then returns
`nil, nil` is the correct empty-envelope shape for void ops (Tag/Untag/Delete) — NOT a stub.

### Region / ctxbag / account literals (new)
- **apigateway/proxy.go:559** — `buildAuthorizerEvent` hardcodes `us-east-1` + `000000000000`
  in the Lambda-authorizer `methodArn`; the func has `*http.Request` (ctx reachable but unused).
- **iotanalytics/backend.go:1127-1130** — `resolveARNResource` ARN-prefix matching still
  hardcodes `us-east-1:000000000000` (follow-up to the Create-path fix already shipped: prefix
  matching wasn't updated, so cross-region ARNs won't resolve). EASY.
- **iotwireless/backend_ops.go:~71** — `wirelessGatewayTaskDefARN` hardcodes
  `us-east-1:000000000000`; backend has region/account fields. EASY.
- **backup/handler.go:4079** — `handleCreateTieringConfiguration` uses `h.Backend.Region()` for
  region but hardcodes `000000000000` for the account in the vault ARN. EASY.
- **memorydb/handler.go:1663,1692** — `buildShards` hardcodes AZs `us-east-1a/b/c` and the
  `*.memorydb.us-east-1.amazonaws.com` endpoint FQDN (region/AZ should derive from request).
- **elasticache/handler.go:965** — `toCacheCluster` hardcodes `CustomerAvailabilityZone:
  "us-east-1a"`.
- **ce/handler.go:1697,1763,1767** — `handleGetSavingsPlansCoverage` /
  `…PurchaseRecommendation` emit synthetic `Region: us-east-1` + `AccountId: 000000000000`
  (handlers have ctx; not using awsmeta). EASY.
- **sns/backend.go:374,2277** — SNS message-signing `certURL` hardcodes
  `https://sns.us-east-1.amazonaws.com/...` (region-specific in real AWS); signer built at init.
- **athena/backend.go:22,26** — package-level `arnRegion="us-east-1"` const + presigned-notebook
  URL base hardcode region (region-on-backend/static-const class).
- Verify: **route53/handler.go:~2064** (observer-region fallback), **transfer/handler.go:~3176**
  (account in mock callback) — lower-value, confirm before acting.

### Logger discipline (new) — pkgs/logger forbids embedded `*slog.Logger`; use `logger.Load(ctx)`
- **Embedded logger struct fields:** `stepfunctions/backend.go:190` (`InMemoryBackend.logger`,
  set via `SetLogger`), `iot/broker.go:24` (`Broker.logger`) + `:105` (`ruleHook.logger`),
  `dax/dataplane/server.go:85` (`Server.logger`). These pin a logger at construction instead of
  inheriting the request-scoped one.
- **Ad-hoc `slog.Default()` in production logic** (~55 non-persistence sites): `lambda/provider.go:38`,
  `lambda/backend.go` (async/URL-server/layer paths), `lambda/runtime_api.go` (init/pending),
  `ecs/reconciler.go` (2), `ecs/provider.go`, `ecr/provider.go`, `iot/provider.go`,
  `mediaconvert/janitor.go`, `cloudwatch/backend.go` (SNS-action delivery). Background loops
  (reconcilers/janitors) have no request ctx, so these need a service-scoped logger captured at
  startup rather than `slog.Default()`.

### Emulation accuracy — validation / error-code / pagination (new, verify each)
- **stepfunctions/handler.go:480-521** — `createStateMachineAction` doesn't validate `Name`
  presence/pattern (AWS → `ValidationException`); `updateStateMachineAction` (526-554) doesn't
  validate `StateMachineArn` format.
- **rds/handler.go:323** — form-parse failure returns `500` instead of `400 ValidationException`.
- **resourcegroups/handler.go:381-397** — `handleCreateGroup` doesn't validate non-empty `Name`.
- **detective/handler.go:266** — returns `501 NotImplementedException` where AWS returns
  `400 InvalidInputException`.
- **transcribe/handler.go** — returns generic `InternalFailureException` where AWS returns
  `ValidationException` for invalid params.
- Name/enum validation gaps (return success on invalid input where AWS rejects): SNS CreateTopic
  FIFO `.fifo`-suffix rule when `FifoTopic=true` (`sns/handler.go:462`); SQS CreateQueue name
  pattern + 80-char limit; SQS ReceiveMessage `MaxNumberOfMessages>10`; Lambda CreateFunction
  name rules; EventBridge PutRule / CreateEventBus name length/pattern; DynamoDB CreateTable
  `BillingMode` enum; Kinesis CreateStream `ShardCount>0`; APIGW CreateIntegration required
  `Type`; Scheduler CreateSchedule cron format; IAM CreateRole `AssumeRolePolicyDocument` valid
  JSON. (All "verify against current code" — several may already validate.)
- **Possible lying stub:** `s3tables/handler.go:726` `DeleteTableBucketEncryption` returns
  `nil,nil` with no apparent backend mutation — verify it actually clears config (distinct from
  the void-op false-positive pattern). `networkmonitor` Delete/Tag ops similar — verify.

### Performance / resource leaks (new)
- **KMS `findGrantByToken`** (`kms/backend.go:2227-2233`) — O(regions × tokens) nested scan on
  the Encrypt/Decrypt hot path; needs a token→grant index. **KMS `ListGrants`**
  (`:2287-2298`) — O(n) scan of all grants, no keyID index. (Supersedes the older single-line
  KMS note above with exact location.)
- **EventBridge `ListRuleNamesByTarget`** (`eventbridge/backend.go:2369-2380`) — O(targets ×
  rules) triple-nested scan, no TargetArn→rule index.
- **OpenSearch** (`opensearch/backend.go:497,499`) — `upgradeHistory` and `domainMaintenances`
  are append-only per-domain lists with no cap/TTL (unbounded under long-running/stress use).
- **Pipes** (`pipes/backend.go:844`) — `enrichmentCallCount` global counter increments
  unbounded; verify whether it's pruned per-pipe / reset.
- Verify: **OpenSearch** package-association bidirectional index (`backend.go:479-484`) — confirm
  `packageAssociations`/`domainPackages` are updated atomically to avoid index drift.
- **Corrections (re-verified as already OK/fixed — do not re-flag):** SSM
  `GetParametersByPath` is now O(log n + k) via `paramNamesSorted`; CloudWatch Logs
  metric-filter matching is bounded by per-group filter caps; SSM param history capped at 100;
  CloudWatch `alarmHistory` rolling-trimmed; ACM cert timers `Stop()`ed in `DeleteCertificate`;
  StepFunctions `pendingTaskQueues` channel closed+deleted on `DeleteActivity`.

### Test coverage — corrected current inventory (supersedes the stale §H list above)
Since the prior audit, **19 services gained Terraform fixtures and integration tests**
(apprunner, comprehend, databrew, datasync, directoryservice, detective, forecast, macie2,
medialive, mediapackage, mediastoredata, mediatailor, personalize, polly, quicksight,
rekognition, rolesanywhere, transcribe, translate, workmail). Accurate remaining gaps:
- **No SDK integration test (10):** account, bedrockagent, cleanrooms, dlm, networkmonitor,
  omics, opsworks, qldb, qldbsession, vpclattice. (account/opsworks blocked on SDK modules not
  in `go.mod`; qldb/qldbsession are deprecated — likely drop rather than test.)
- **No Terraform fixture/module (12):** accessanalyzer, account, appmesh, bedrockagent,
  cleanrooms, dax, inspector2, networkmonitor, omics, opsworks, qldb, qldbsession.
- **Both gaps (highest priority):** account, bedrockagent, cleanrooms, dlm, omics, opsworks,
  networkmonitor, vpclattice.
- **Thin coverage:** only **5** fixtures have full success+import+drift (dynamodb, s3,
  secretsmanager, sns, sqs); ~84 of ~157 fixtures are single-resource `success.tf` only —
  adding `import.tf`/`drift.tf` + multi-resource modules is the biggest parity-signal win.
  Single-op integration smoke tests to expand: bedrockruntime, ce, detective, shield,
  rekognition, forecast.

---

## Confidence / caveats
Items are read-confirmed against the cited files but the tree changes constantly — **re-verify
each before starting**, since prior passes may have closed it. The "canned inference" items are
intentional mocks flagged as opportunities, not bugs. AppStream/WorkSpaces sub-resource gaps
need a precise handler↔backend op diff (the size-only check was inconclusive). `golangci-lint`
in some environments is built against an older Go than the module targets (go1.26.x) and may
refuse to run — use `go vet` + `gofmt` as the fallback gate.

---

## Tier P4 — per-service audit sweep (run last; one task per service)

These are intentionally the lowest priority — a full, uniform pass over **every**
service. A task runner (gastown) loops over each item below as its own unit and runs
a search per service, so each line is self-contained. The canonical task for each
service `<svc>` is:

> Look over `services/<svc>` and make sure we have good AWS emulation, Terraform tests
> for that emulation, and integration tests for that emulation. Make sure we have all
> features LocalStack provides and more. As you scan the code look for performance
> optimizations and resource leaks to fix. The service must have region support if
> applicable and must use the ctxbag (`pkgs/awsmeta`, `awsmeta.Region(ctx)`/
> `awsmeta.Account(ctx)`) for AWS metadata and pass around a consistent logger
> (`logger.Load(ctx)`; never embed a `*slog.Logger` on a struct).

Per-service tasks:

- [ ] **accessanalyzer** — audit `services/accessanalyzer`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **account** — audit `services/account`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **acm** — audit `services/acm`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **acmpca** — audit `services/acmpca`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **amplify** — audit `services/amplify`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **apigateway** — audit `services/apigateway`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **apigatewaymanagementapi** — audit `services/apigatewaymanagementapi`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **apigatewayv2** — audit `services/apigatewayv2`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **appconfig** — audit `services/appconfig`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **appconfigdata** — audit `services/appconfigdata`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **applicationautoscaling** — audit `services/applicationautoscaling`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **appmesh** — audit `services/appmesh`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **apprunner** — audit `services/apprunner`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **appstream** — audit `services/appstream`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **appsync** — audit `services/appsync`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **athena** — audit `services/athena`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **autoscaling** — audit `services/autoscaling`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **awsconfig** — audit `services/awsconfig`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **backup** — audit `services/backup`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **batch** — audit `services/batch`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **bedrock** — audit `services/bedrock`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **bedrockagent** — audit `services/bedrockagent`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **bedrockruntime** — audit `services/bedrockruntime`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **ce** — audit `services/ce`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **cleanrooms** — audit `services/cleanrooms`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **cloudcontrol** — audit `services/cloudcontrol`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **cloudformation** — audit `services/cloudformation`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **cloudfront** — audit `services/cloudfront`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **cloudtrail** — audit `services/cloudtrail`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **cloudwatch** — audit `services/cloudwatch`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **cloudwatchlogs** — audit `services/cloudwatchlogs`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **codeartifact** — audit `services/codeartifact`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **codebuild** — audit `services/codebuild`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **codecommit** — audit `services/codecommit`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **codeconnections** — audit `services/codeconnections`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **codedeploy** — audit `services/codedeploy`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **codepipeline** — audit `services/codepipeline`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **codestarconnections** — audit `services/codestarconnections`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **cognitoidentity** — audit `services/cognitoidentity`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **cognitoidp** — audit `services/cognitoidp`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **comprehend** — audit `services/comprehend`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **databrew** — audit `services/databrew`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **datasync** — audit `services/datasync`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **dax** — audit `services/dax`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **detective** — audit `services/detective`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **directoryservice** — audit `services/directoryservice`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **dlm** — audit `services/dlm`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **dms** — audit `services/dms`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **docdb** — audit `services/docdb`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **dynamodb** — audit `services/dynamodb`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **dynamodbstreams** — audit `services/dynamodbstreams`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **ec2** — audit `services/ec2`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **ecr** — audit `services/ecr`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **ecs** — audit `services/ecs`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **efs** — audit `services/efs`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **eks** — audit `services/eks`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **elasticache** — audit `services/elasticache`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **elasticbeanstalk** — audit `services/elasticbeanstalk`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **elasticsearch** — audit `services/elasticsearch`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **elb** — audit `services/elb`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **elbv2** — audit `services/elbv2`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **emr** — audit `services/emr`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **emrserverless** — audit `services/emrserverless`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **eventbridge** — audit `services/eventbridge`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **firehose** — audit `services/firehose`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **fis** — audit `services/fis`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **forecast** — audit `services/forecast`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **fsx** — audit `services/fsx`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **glacier** — audit `services/glacier`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **glue** — audit `services/glue`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **guardduty** — audit `services/guardduty`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **iam** — audit `services/iam`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **identitystore** — audit `services/identitystore`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **inspector2** — audit `services/inspector2`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **iot** — audit `services/iot`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **iotanalytics** — audit `services/iotanalytics`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **iotdataplane** — audit `services/iotdataplane`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **iotwireless** — audit `services/iotwireless`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **kafka** — audit `services/kafka`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **kinesis** — audit `services/kinesis`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **kinesisanalytics** — audit `services/kinesisanalytics`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **kinesisanalyticsv2** — audit `services/kinesisanalyticsv2`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **kms** — audit `services/kms`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **lakeformation** — audit `services/lakeformation`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **lambda** — audit `services/lambda`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **macie2** — audit `services/macie2`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **managedblockchain** — audit `services/managedblockchain`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **mediaconvert** — audit `services/mediaconvert`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **medialive** — audit `services/medialive`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **mediapackage** — audit `services/mediapackage`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **mediastore** — audit `services/mediastore`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **mediastoredata** — audit `services/mediastoredata`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **mediatailor** — audit `services/mediatailor`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **memorydb** — audit `services/memorydb`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **mq** — audit `services/mq`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **mwaa** — audit `services/mwaa`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **neptune** — audit `services/neptune`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **networkmonitor** — audit `services/networkmonitor`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **omics** — audit `services/omics`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **opensearch** — audit `services/opensearch`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **opsworks** — audit `services/opsworks`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **organizations** — audit `services/organizations`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **personalize** — audit `services/personalize`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **pinpoint** — audit `services/pinpoint`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **pipes** — audit `services/pipes`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **polly** — audit `services/polly`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **qldb** — audit `services/qldb`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **qldbsession** — audit `services/qldbsession`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **quicksight** — audit `services/quicksight`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **ram** — audit `services/ram`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **rds** — audit `services/rds`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **rdsdata** — audit `services/rdsdata`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **redshift** — audit `services/redshift`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **redshiftdata** — audit `services/redshiftdata`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **rekognition** — audit `services/rekognition`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **resourcegroups** — audit `services/resourcegroups`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **resourcegroupstaggingapi** — audit `services/resourcegroupstaggingapi`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **rolesanywhere** — audit `services/rolesanywhere`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **route53** — audit `services/route53`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **route53resolver** — audit `services/route53resolver`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **s3** — audit `services/s3`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **s3control** — audit `services/s3control`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **s3tables** — audit `services/s3tables`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **sagemaker** — audit `services/sagemaker`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **sagemakerruntime** — audit `services/sagemakerruntime`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **scheduler** — audit `services/scheduler`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **secretsmanager** — audit `services/secretsmanager`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **securityhub** — audit `services/securityhub`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **serverlessrepo** — audit `services/serverlessrepo`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **servicediscovery** — audit `services/servicediscovery`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **ses** — audit `services/ses`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **sesv2** — audit `services/sesv2`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **shield** — audit `services/shield`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **sns** — audit `services/sns`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **sqs** — audit `services/sqs`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **ssm** — audit `services/ssm`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **ssoadmin** — audit `services/ssoadmin`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **stepfunctions** — audit `services/stepfunctions`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **sts** — audit `services/sts`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **support** — audit `services/support`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **swf** — audit `services/swf`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **textract** — audit `services/textract`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **timestreamquery** — audit `services/timestreamquery`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **timestreamwrite** — audit `services/timestreamwrite`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **transcribe** — audit `services/transcribe`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **transfer** — audit `services/transfer`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **translate** — audit `services/translate`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **verifiedpermissions** — audit `services/verifiedpermissions`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **vpclattice** — audit `services/vpclattice`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **waf** — audit `services/waf`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **wafv2** — audit `services/wafv2`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **workmail** — audit `services/workmail`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **workspaces** — audit `services/workspaces`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
- [ ] **xray** — audit `services/xray`: AWS emulation accuracy; Terraform + integration tests; match/exceed LocalStack; perf + resource-leak fixes; region support via ctxbag (`awsmeta.Region(ctx)`/`awsmeta.Account(ctx)`); consistent logger (`logger.Load(ctx)`, no embedded `*slog.Logger`).
