# Wrapper-key / nested-shape sweep remainder (gopherstack-6flj)

**63 of 162 services swept, 99 remain** (securityhub added this session —
see its own section near the end of this file for full detail).

Built for gopherstack-6flj. **Every count this issue's own notes carried
forward has turned out wrong, twice, by a large factor** — ec2 was recorded
at "~144 Describe/Get handlers" and was really ~220-264 depending on how you
count; rds was recorded at "130+ remaining" and was really 26. Both errors
were found by reading each service's own `GetSupportedOperations` /
op-name list directly, not by trusting a prior session's note. This file and
`cmd/opcensus` exist so a future session doesn't have to re-derive that list
from scratch, and doesn't have a stale number to (mis)trust in the meantime.

**A future batch should read this file, not rebuild it.** Regenerate via
`go run ./cmd/opcensus`, cross-reference the new output against the "swept"
table below, and update both tables — don't just discard this file. `*.py`
scratch scripts are gitignored here (cost a prior agent its generator on a
sibling sweep); `cmd/opcensus` is a committed Go tool for exactly that
reason.

## What the count means, and what it doesn't

For every `services/<dir>`, `cmd/opcensus` parses every non-test `.go` file,
locates the `GetSupportedOperations` method every service implements (the
dispatcher's own declared operation set — not a doc comment, not a PARITY.md
claim), and collects the operation-name string literals it returns —
following same-package function calls and function-value tables it goes
through (ec2 delegates through ~50 per-family `fooSupportedOps()` functions
via a `[]func() []string` provider table; omics builds its list from a
`sync.OnceValue` dispatch-table constructor; sqs/apigateway name package
consts like `opAddPermission` instead of writing the string inline), and
falling back to a whole-package scan for string-keyed map literals /
index-assignments where the method instead ranges over a struct field
populated in a constructor (rekognition/appstream's `h.ops`).

Ops are bucketed by `List`/`Describe`/`Get` prefix — **a proxy for
"collection or nested-shape response surface,"** the shape of bug this issue
tracks, matching how prior batches sized ec2 ("~144/~220 Describe/Get
handlers") and rds ("48 Describe/Get ops"). **It says nothing about
correctness** — that's still a per-op hand read against the pinned SDK
deserializer. A service with a high count is a big surface to check, not a
confirmed bug count.

### Validated against known-good figures

- **rds**: 48-49 (small variance run-to-run from one const-table op this
  session's refactor resolves slightly differently) — matches the
  session-verified "48 total Describe/Get ops" enumerated by hand from
  `handler_supported_ops.go`'s literal slices in a prior batch.
- **ec2**: 264 — same order of magnitude as the session-verified "~220-264"
  range (`grep -c 'func (h *Handler) handle(Describe|Get)'` gave 261; this
  tool's op-name-list method gives 264; both far above the stale "~144").
- **glue**: 123 (32 list + 4 describe + 87 get) via the `dynamic-fallback`
  path, resolving glue's `glueOpBindings` table-of-structs pattern
  (`for i, b := range glueOpBindings { names[i] = b.name }`) correctly.
- **omics**: 51 via `dynamic-fallback`, resolving the `sync.OnceValue`
  dispatch-table pattern and const-keyed map entries
  (`opCreateReferenceStore: func(...)`) correctly.

### Known limitation: not every service resolves

4 of 162 services could not be resolved by the tool's two fallback tiers
(both print `resolution: unresolved`, count 0) because their
`GetSupportedOperations` returns a struct field populated in a constructor
by a method call the tool doesn't chase (`h.supportedOpsCache = h.buildOps()`)
rather than a range over a literal table:

- **route53resolver** — manually counted via whole-package grep for
  `"(List|Describe|Get)[A-Za-z0-9]*"` string literals: 30 ops (16 List, 14
  Get). Included in the ranked table below with this note.
- **qldb**, **qldbsession** — manually confirmed via the same grep: 0
  List/Describe/Get op names in either service. Both are tiny
  (PartiQL-statement-execution services), genuinely near-zero surface for
  this bug class.
- **ssm** — same unresolved shape (`h.ops` built from family `ssm*Ops()`
  functions merged in `NewHandler`), but ssm is already in the swept table
  below (see gopherstack-enpq's `567e2c4f8`, "all field-diffed" — a
  different bug class than this issue's wrapper-key sweep, but the required-
  member diff there is thorough enough that this issue's own prior notes
  already listed ssm as layer-1 swept).

## Swept (63 of 162) — do not re-sweep without reading the cited work first

Every op in these services has had at least one full layer-1 (wrapper key)
pass; most also have layer-2 (nesting) and layer-3 (backend-tracked-but-
unemitted) passes. Read `bd show gopherstack-6flj` (notes + the one comment)
for per-service detail and commit citations before touching any of these
again — several have explicit "already checked, don't re-flag" notes (e.g.
route53's `ListHostedZonesByVPC` XMLName quirk, cloudfront's root-tag
non-bug, rds's `GlobalClusterMember` shared-name non-bug).

apigateway, appstream, athena, autoscaling, awsconfig, backup, bedrock,
bedrockagent, cleanrooms, cloudformation, cloudfront,
cloudfrontkeyvaluestore, cloudwatch, cloudwatchlogs, codebuild, codecommit,
datasync, dlm, dynamodbstreams, ec2, ecs, eks, elasticache, elbv2, forecast,
glue, guardduty, iam, identitystore, inspector2, iot,
iotwireless, kms, lambda, lightsail, medialive, mgn, **networkmanager**
(this session), networkmonitor, omics,
opensearch, organizations, pinpoint, quicksight, rds, redshift,
resiliencehub, resourcegroupstaggingapi, route53, s3control, s3tables,
sagemaker, secretsmanager, **securityhub** (this session), servicediscovery,
ses, sesv2, sns, sqs, ssm, ssoadmin, stepfunctions, transfer.

Two services have real, extensive wire-shape work under **other** issue
classes (gopherstack-enpq's required-member diff, gopherstack-h910/ctaz's
backend-logic fixes) but **no 6flj-specific wrapper-key pass on record** —
s3 and dynamodb. They are listed in the unswept table below on purpose;
don't assume "heavily worked on" means "settled for this issue."

## Unswept (99 of 162), ranked by List+Describe+Get op count

This is the real remainder — pick from the top, not alphabetically, per this
issue's "blast radius" guidance. **Prefer large counts AND a shared
list-building helper** (grep the service for one converter function/constant
reused across many ops before committing to a target — that's what made
omics near-service-wide and forecast 12-of-12 in earlier batches). Resolution
column: `direct` = literal slice/table read straight out of
`GetSupportedOperations`; `chased` = resolved by following same-package
calls/tables; `dynamic-fallback` = resolved via the whole-package scan
fallback (worth a second look — these services build their op list
dynamically, which often correlates with a shared-converter pattern worth
checking for the sibling-trap bug variant); `manual` = tool-unresolved, hand
counted (see limitations above).

Sum of the L+D+G column across all 99 (networkmanager's 38 and securityhub's
47 removed, both swept prior to/this session): **1,516** candidate ops.

| service | total ops | list | describe | get | L+D+G | resolution |
|---|---:|---:|---:|---:|---:|---|
| s3 | 115 | 12 | 0 | 33 | 45 | chased |
| macie2 | 81 | 15 | 3 | 22 | 40 | direct |
| personalize | 77 | 18 | 18 | 3 | 39 | dynamic-fallback |
| cognitoidp | 129 | 14 | 10 | 13 | 37 | chased |
| apigatewayv2 | 103 | 5 | 0 | 32 | 37 | direct |
| workmail | 97 | 18 | 9 | 9 | 36 | dynamic-fallback |
| waf | 113 | 16 | 0 | 18 | 34 | dynamic-fallback |
| wafv2 | 59 | 13 | 3 | 16 | 32 | direct |
| ce | 47 | 7 | 1 | 23 | 31 | direct |
| vpclattice | 73 | 16 | 0 | 14 | 30 | direct |
| eventbridge | 74 | 16 | 12 | 2 | 30 | direct |
| emr | 65 | 13 | 8 | 9 | 30 | direct |
| route53resolver | 30 | 16 | 0 | 14 | 30 | manual (range target unresolved by tool) |
| kafka | 64 | 15 | 11 | 3 | 29 | direct |
| appsync | 74 | 13 | 0 | 15 | 28 | direct |
| workspaces | 111 | 2 | 24 | 1 | 27 | dynamic-fallback |
| lakeformation | 61 | 8 | 3 | 15 | 26 | direct |
| rekognition | 75 | 9 | 5 | 11 | 25 | dynamic-fallback |
| elasticsearch | 51 | 9 | 12 | 4 | 25 | direct |
| directoryservice | 80 | 6 | 17 | 2 | 25 | direct |
| opsworks | 74 | 1 | 22 | 1 | 24 | direct |
| codeartifact | 48 | 12 | 5 | 7 | 24 | direct |
| cloudtrail | 60 | 11 | 2 | 11 | 24 | direct |
| appconfig | 56 | 12 | 0 | 12 | 24 | direct |
| outposts | 43 | 11 | 0 | 12 | 23 | direct |
| dynamodb | 58 | 7 | 13 | 2 | 22 | direct |
| neptune | 70 | 1 | 20 | 0 | 21 | direct |
| ecr | 58 | 4 | 8 | 9 | 21 | direct |
| xray | 38 | 3 | 0 | 17 | 20 | direct |
| directconnect | 64 | 2 | 18 | 0 | 20 | direct |
| transcribe | 43 | 10 | 1 | 8 | 19 | chased |
| mediatailor | 48 | 9 | 5 | 5 | 19 | direct |
| memorydb | 45 | 3 | 15 | 0 | 18 | direct |
| codedeploy | 47 | 10 | 0 | 8 | 18 | direct |
| accessanalyzer | 39 | 9 | 0 | 9 | 18 | direct |
| elasticbeanstalk | 47 | 4 | 13 | 0 | 17 | direct |
| docdb | 55 | 1 | 16 | 0 | 17 | direct |
| batch | 45 | 7 | 9 | 1 | 17 | direct |
| databrew | 44 | 9 | 7 | 0 | 16 | direct |
| ram | 35 | 10 | 0 | 5 | 15 | direct |
| fis | 26 | 8 | 0 | 7 | 15 | direct |
| codepipeline | 44 | 9 | 0 | 6 | 15 | direct |
| apprunner | 37 | 9 | 6 | 0 | 15 | direct |
| appmesh | 38 | 8 | 7 | 0 | 15 | direct |
| amplify | 37 | 8 | 0 | 7 | 15 | direct |
| acm | 39 | 7 | 5 | 3 | 15 | direct |
| shield | 36 | 5 | 7 | 1 | 13 | direct |
| mediaconvert | 34 | 6 | 1 | 6 | 13 | direct |
| managedblockchain | 27 | 8 | 0 | 5 | 13 | direct |
| kinesis | 39 | 5 | 5 | 3 | 13 | direct |
| glacier | 33 | 6 | 2 | 5 | 13 | direct |
| codestarconnections | 27 | 6 | 0 | 7 | 13 | direct |
| codeconnections | 27 | 6 | 0 | 7 | 13 | direct |
| verifiedpermissions | 34 | 6 | 0 | 6 | 12 | direct |
| mq | 25 | 5 | 7 | 0 | 12 | direct |
| iotanalytics | 34 | 6 | 5 | 1 | 12 | direct |
| fsx | 48 | 1 | 11 | 0 | 12 | direct |
| swf | 39 | 6 | 4 | 1 | 11 | direct |
| support | 16 | 0 | 11 | 0 | 11 | direct |
| emrserverless | 22 | 5 | 0 | 6 | 11 | direct |
| efs | 31 | 1 | 10 | 0 | 11 | direct |
| detective | 29 | 8 | 1 | 2 | 11 | direct |
| cognitoidentity | 23 | 3 | 2 | 6 | 11 | direct |
| textract | 25 | 3 | 0 | 7 | 10 | direct |
| resourcegroups | 23 | 4 | 0 | 6 | 10 | direct |
| rolesanywhere | 30 | 5 | 0 | 4 | 9 | direct |
| redshiftdata | 12 | 5 | 2 | 2 | 9 | direct |
| kinesisanalyticsv2 | 33 | 5 | 4 | 0 | 9 | direct |
| grafana | 25 | 6 | 3 | 0 | 9 | direct |
| acmpca | 23 | 3 | 2 | 4 | 9 | direct |
| translate | 66 | 5 | 1 | 2 | 8 | dynamic-fallback |
| timestreamwrite | 19 | 4 | 4 | 0 | 8 | direct |
| iotdataplane | 14 | 5 | 0 | 3 | 8 | direct |
| account | 16 | 1 | 0 | 7 | 8 | direct |
| mediastore | 21 | 2 | 1 | 4 | 7 | direct |
| mediapackage | 19 | 4 | 3 | 0 | 7 | direct |
| elb | 29 | 0 | 7 | 0 | 7 | direct |
| dax | 21 | 1 | 6 | 0 | 7 | direct |
| sts | 11 | 0 | 0 | 6 | 6 | direct |
| serverlessrepo | 14 | 3 | 0 | 3 | 6 | direct |
| comprehend | 103 | 4 | 2 | 0 | 6 | dynamic-fallback |
| applicationautoscaling | 14 | 1 | 4 | 1 | 6 | direct |
| timestreamquery | 15 | 2 | 3 | 0 | 5 | direct |
| scheduler | 12 | 3 | 0 | 2 | 5 | direct |
| polly | 10 | 2 | 1 | 2 | 5 | direct |
| cloudcontrol | 8 | 2 | 0 | 2 | 4 | direct |
| pipes | 10 | 2 | 1 | 0 | 3 | direct |
| mwaa | 12 | 2 | 0 | 1 | 3 | direct |
| mediastoredata | 5 | 1 | 1 | 1 | 3 | direct |
| kinesisanalytics | 20 | 2 | 1 | 0 | 3 | direct |
| firehose | 12 | 2 | 1 | 0 | 3 | direct |
| bedrockruntime | 11 | 1 | 0 | 1 | 2 | direct |
| appconfigdata | 2 | 0 | 0 | 1 | 1 | direct |
| apigatewaymanagementapi | 3 | 0 | 0 | 1 | 1 | direct |
| sagemakerruntime | 3 | 0 | 0 | 0 | 0 | direct |
| rdsdata | 6 | 0 | 0 | 0 | 0 | direct |
| qldbsession | 0 | 0 | 0 | 0 | 0 | manual (no List/Describe/Get ops in this service) |
| qldb | 0 | 0 | 0 | 0 | 0 | manual (no List/Describe/Get ops in this service) |
| dms | 15 | 0 | 0 | 0 | 0 | dynamic-fallback |

## Method notes for the next session

- **`dms` shows 0/0/0/0/dynamic-fallback** — not confirmed clean, the tool's
  fallback tiers found nothing. Worth a manual check: either DMS genuinely
  has no List/Describe/Get-named ops in gopherstack's dispatch (plausible —
  DMS's real API is heavy on `Describe*` but gopherstack may use different
  verb names for some), or this is a fifth unresolved case the fallback
  logic doesn't cover. Check before trusting the 0.
- **`sagemakerruntime`/`rdsdata` show near-zero counts** — both are
  data-plane services (invoke/execute-statement shaped APIs), genuinely low
  surface for this bug class, consistent with the "small services come back
  clean" pattern from the identitystore/resourcegroupstaggingapi/
  servicediscovery batch.
- **Casing note**: this table doesn't distinguish protocol. Query/XML
  services (rds, sns, and most others) decode case-insensitively
  (`strings.EqualFold`), so a casing near-miss there is not a bug. JSON-RPC
  services (awsconfig, sqs, cloudwatch, and most `awsAwsjson1{0,1}` services
  per `services/_PROTOCOLS.md`) decode via exact string/map-key match, so
  casing differences **are** real bugs there — awsconfig alone had four
  (`ListDiscoveredResources`, `GetDiscoveredResourceCounts`,
  `BatchGetResourceConfig` both directions, and `ResourceConfigItem`'s four
  fields shared by `GetResourceConfigHistory`/`BatchGetResourceConfig`).
  Confirm protocol from the pinned SDK's `api_client.go` (grep for
  `awsAwsjson1{0,1}_` vs `awsEc2query_`/`awsAwsquery_` function prefixes),
  not from `_PROTOCOLS.md` alone — one of its hand-checked rows was itself
  wrong (this issue's cloudwatch batch found it uses rpc-v2-cbor
  exclusively, not the awsQuery the doc implied).

## Regenerate

```
go run ./cmd/opcensus                # ranked summary to stdout (used to build the table above)
go run ./cmd/opcensus -json out.json # full per-service detail: every op name, not just counts
```

No network access required — it only parses already-checked-out `.go`
source under `services/`. Runs in well under a second.

## awsconfig sweep result (this session)

Full layer-1+2 sweep of all 53 List/Describe/Get ops against
`configservice@v1.68.4` (confirmed JSON-RPC 1.1 / `awsAwsjson11_`,
case-sensitive, from `api_client.go` and the `deserializers.go` function
prefix — not from `_PROTOCOLS.md` alone, though that row was correct here).
9 bugs found and fixed, all citations in the commit/diff:

1. `ListDiscoveredResources` — wrapper key `ResourceIdentifiers` should be
   `resourceIdentifiers` (lowercase; this op alone in the service uses
   lowerCamelCase throughout, both request and response, unlike its
   PascalCase `DescribeXxx` siblings).
2. `ResourceConfigItem` (shared by `GetResourceConfigHistory` and
   `BatchGetResourceConfig`) — all four fields
   (`ResourceType`/`ResourceId`/`Configuration`/`ConfigurationItemCaptureTime`)
   were tagged PascalCase; the real `ConfigurationItem` type they represent
   is lowerCamelCase throughout.
3. `BatchGetResourceConfig` — sibling trap against
   `BatchGetAggregateResourceConfig` (genuinely PascalCase): the plain
   op is lowerCamelCase on **both** the request (`resourceKeys`) and
   response (`baseConfigurationItems`/`unprocessedResourceKeys`) sides. A
   real client's request never carried its resource keys at all.
4. `GetDiscoveredResourceCounts` — wrapper key `TotalDiscoveredResources`
   should be `totalDiscoveredResources`; the required `ResourceCounts`
   per-type breakdown is not modeled (disclosed, needs new `pkgs/store`
   surface to enumerate an `Index`'s group keys with counts).
5. `GetDiscoveredResourceCounts`'s backend method was **also** a hardcoded
   `return 0` stub, independent of the casing bug — fixed to read
   `resourceConfigs.Len()`, matching its `GetAggregateDiscoveredResourceCounts`
   sibling which already did this correctly.
6. `GetComplianceSummaryByConfigRule` — invented response shape: emitted an
   invented `ComplianceSummariesByConfigRule` list (one element, keyed by a
   synthesized `ComplianceType`) where the real op returns a single
   `ComplianceSummary` object with no `ComplianceType` member at all. Fixed
   by reshaping the type and the backend method's return type
   (`[]ComplianceSummary` → `ComplianceSummary`).
7. `GetAggregateConfigRuleComplianceSummary` — missing `GroupByKey` echo
   (a real, always-echoed request member); also inherited fix #6's type
   correction since it embeds the same `ComplianceSummary` type.
8. `GetAggregateConformancePackComplianceSummary` — missing `GroupByKey`
   echo, same shape as #7.
9. `DescribeConformancePackCompliance` — missing the required
   `ConformancePackName` echo field entirely (present on the sibling
   `GetConformancePackComplianceDetails`, which is what made the gap easy to
   miss).

Ratifying tests found and fixed: 2 — `TestComplianceSummaryShape`
(substring-`Contains` assertion that stayed true under the pre-fix shape
because the wrong shape nested a field also spelled "ComplianceSummary" one
level inside the invented list) and `TestAWSConfigHandler_BatchGetResourceConfig`
(hand-built raw JSON body sent `"ResourceKeys"` and asserted
`"BaseConfigurationItems"` — both sides agreed with gopherstack's pre-fix
bug, so the test caught nothing).

`GetAggregateDiscoveredResourceCounts`'s missing `GroupByKey`/
`GroupedResourceCounts` was disclosed, not fixed for the grouped-count part
(no backend surface to source per-group counts from without new modeling);
`GroupByKey` echo alone was fixed.

9 real-aws-sdk-go-v2-client tests added/upgraded in
`services/awsconfig/wire_field_fixes_test.go` and
`services/awsconfig/handler_config_rules_test.go`; every fix hand-reverted
individually (no git, per this session's hard no-git-mutation constraint),
confirmed to fail with the exact predicted symptom, then restored and
diffed byte-identical against the pre-revert file. Gates (build/vet/race/
`go fix -diff`/golangci-lint 0 issues, no cyclop/gocyclo/gocognit/funlen
nolints) all green for `services/awsconfig` and `go test -race ./pkgs/...`.

## pinpoint (this session)

Chosen as the largest unswept service in the ranked table (53 L+D+G ops: 4
List/0 Describe/49 Get) once s3/dynamodb/pinpoint's caveats were accounted
for. Protocol: restjson1, confirmed at pinpoint@v1.42.4 deserializers.go's
`awsRestjson1_deserializeOp*` function prefix and its plain `switch key {
case "Foo":` bodies (no `strings.EqualFold` anywhere in the body-field
switches — 843 `EqualFold` hits in the file are all header/query-param
matching, not body deserialization) — case-sensitive, like awsconfig.

**Methodology trap hit and recovered from before any wrong fix landed**:
every op also has a generated-but-DEAD `awsRestjson1_deserializeOpDocumentX
Output` function with a `case "XResponse":` wrapper switch. These are never
called — `HandleDeserialize` for every op in this service instead feeds the
whole decoded body directly into `awsRestjson1_deserializeDocumentX(&output.X,
shape)`, bypassing the wrapper. Confirmed by reading `HandleDeserialize`
itself (not the OpDocument function) for a dozen ops spanning apps,
campaigns, segments, journeys, templates, channels, endpoints, event
streams, recommenders, export/import jobs. Net effect: gopherstack's
existing flat/unwrapped response bodies are already correct — there is no
service-wide top-level wrapper bug here. **Any future JSON-protocol sweep
must check the real `HandleDeserialize` body, not grep an
`OpDocument...Output` function name and assume it's reachable** — this is
the JSON-protocol analogue of cloudfront's root-tag non-bug from an earlier
batch.

5 real bugs found and fixed, all layer-2/3 (correct outer shape, wrong
nesting or missing required members) — every one verified against the
`awsRestjson1_deserializeDocument<Type>` function actually invoked from the
op's own `HandleDeserialize`:

1. `GetExportJob`/`GetExportJobs`/`GetImportJob`/`GetImportJobs` (+
   `GetSegmentExportJobs`/`GetSegmentImportJobs`, sharing the same response
   type): `ExportJobResponse`/`ImportJobResponse` emitted `RoleArn`/
   `S3UrlPrefix`/`S3Url`/`Format` flat at the top level; the real shape
   nests all of it one level under `Definition` (`types.ExportJobResource`/
   `types.ImportJobResource`, confirmed at deserializers.go's `case
   "Definition":`) — a real client's typed `.Definition` field was `nil`
   regardless of what was persisted. Also dropped a fabricated top-level
   `Arn` field: confirmed absent from both `types.ExportJobResponse`/
   `types.ImportJobResponse` and their real deserializer case lists.
2. `GetApplicationDateRangeKpi`/`GetCampaignDateRangeKpi`/
   `GetJourneyDateRangeKpi`: shared `kpiResult` type never emitted
   `StartTime`/`EndTime`, both `"This member is required."` on all three
   real `*DateRangeKpiResponse` types even though the request's
   `start-time`/`end-time` query params are themselves optional. Fixed by
   parsing the query params (RFC3339) with a 7-day-trailing default when
   absent, echoed back always.
3. `GetJourneyExecutionMetrics`/`GetJourneyExecutionActivityMetrics`/
   `GetJourneyRunExecutionMetrics`/`GetJourneyRunExecutionActivityMetrics`:
   all four response types never emitted `LastEvaluatedTime`, `"This member
   is required."` on every one (pinpoint@v1.42.4 types/types.go). Fixed by
   populating with the current time at response construction (synthetic —
   this backend has no real evaluation-cadence concept).
4. `GetJourneyRuns`: per-item `JourneyRunResponse` never emitted
   `CreationTime`/`LastUpdateTime`, both required on the real type — a real
   client's run items had `RunId`/`Status` but nil times. Also dropped
   `ApplicationId`/`JourneyId` from the per-item JSON tags: confirmed the
   real `JourneyRunResponse`'s field set is only
   `CreationTime/LastUpdateTime/RunId/Status` (app/journey identity comes
   from the URL path, not the item), so these were harmless-but-fabricated
   extra fields.
5. `GetApplicationSettings`: `ApplicationSettingsResource` never emitted
   `JourneyLimits` at all (a real member, `*ApplicationSettingsJourneyLimits`)
   even though its sibling document-shaped members — `CampaignHook`,
   `Limits`, `QuietTime` — were already round-tripped correctly. Fixed by
   adding the same opaque-passthrough-map treatment already used for the
   other three.

**Request side**: checked as part of finding #1 (export/import job
`Definition` fields serialize flat on the request side too — confirmed
correct there via `awsRestjson1_serializeOpHttpBindingsCreateExportJobInput`
+ `awsRestjson1_serializeDocumentExportJobRequest`, so only the response
needed the nesting fix, not both directions this time).

**Ratifying tests found and fixed**: 2 —
`TestExportJobFieldsPersisted`/`TestImportJobFieldsPersisted` in
`export_import_jobs_test.go` were raw-map (`map[string]any`) assertions on
`resp["RoleArn"]`/`resp["S3UrlPrefix"]`/`resp["Arn"]` at the top level —
exactly the flat pre-fix shape — plus asserted `resp["Arn"]` as
`NotEmpty` (the fabricated field). Rewritten as real-SDK-client tests
(`_RealClient` suffix) asserting through `.Definition.RoleArn` etc., which
cannot pass against the unfixed flat shape.

**Phantom ops**: none found — every op string returned by
`GetSupportedOperations`'s per-family helper functions corresponds to a real
op in pinpoint@v1.42.4's `api_op_*.go` files (spot-checked all 53 L+D+G
ops plus every Create/Update/Delete counterpart touched by the fixes above).

**False-positive rate**: 0 among reported bugs — every finding cited the
real `awsRestjson1_deserializeDocument<Type>` function actually reached from
`HandleDeserialize`, file+line, never a doc comment or the dead
`OpDocument...Output` function.

**Disclosed, not fixed** (structural/optional-field gaps — each would need
new backend modeling, not a rename, and none silently drops data the
backend already tracks):
- `CampaignResponse` missing `DefaultState`/`Description`/`HoldoutPercent`
  — not tracked anywhere in the `Campaign` model or its Create/Update
  request types.
- `ActivityResponse` (nested in `GetCampaignActivities`'s `Item`) is
  severely under-modeled — only `ApplicationId`/`CampaignId`/`Id` of 14 real
  fields are present; `End`/`ExecutionMetrics`/`Result`/`ScheduledStart`/
  `Start`/`State`/`SuccessfulEndpointCount`/`TimezonesCompletedCount`/
  `TimezonesTotalCount`/`TotalEndpointCount`/`TreatmentId` would all require
  simulating campaign execution progress, which this backend doesn't do (one
  stub activity record is created at campaign creation and never
  progresses).
- `JourneyResponse` missing `JourneyChannelSettings`/`SendingSchedule`/
  `TimezoneEstimationMethods`.
- `EmailTemplateResponse` missing `Headers` ([]MessageHeader) — not
  accepted on the request side either.
- `RecommenderConfigurationResponse` missing
  `RecommendationsDisplayName`/`RecommendationTransformerUri` — same,
  absent from the create/update request wire types too.
- `EventStream` missing `ExternalId`/`LastUpdatedBy`.
- `Channel` (shared by all 11 channel Get ops + `GetChannels`) missing
  `Id`/`LastModifiedBy` — both are non-required (deprecated/backward-compat
  only) real members; skipped rather than fabricate a plausible-looking but
  unverified value for the deprecated `Id` convention.
- `ExportJobResource.SegmentId`/`SegmentVersion` — the backend's `ExportJob`
  model has no slot for these (unlike `ImportJob`, which already tracks
  `SegmentID` from its generated segment and is wired through correctly).

Tests: 6 real-SDK-client tests
(`services/pinpoint/export_import_jobs_test.go`'s two `_RealClient` rewrites,
`services/pinpoint/wire_field_fixes_test.go`'s 4 new tests). Every fix
hand-reverted individually (no git, per this session's hard
no-git-mutation constraint), confirmed to fail with the exact predicted
symptom — either a compile error (`kpiResult.StartTime`/`EndTime` proven
load-bearing: 6 call sites across 3 backend functions failed to compile
without them) or a runtime assertion failure quoting the exact empty/nil
value — then restored and diffed byte-identical against the pre-revert
file.

Gates: `go build`/`go vet` (scoped to `services/pinpoint` and
`cmd/opcensus` — a sibling session's in-progress `services/securityhub`
work left the full-repo build broken with `undefined: keyProcessingResult`,
confirmed untouched by this session via `git status` and left alone per
this session's instructions), `go test -race`, `go fix -diff` (no diff),
`fieldalignment -fix` (one real hit, `exportJobResponse`, auto-fixed),
golangci-lint (0 issues after that + a `nonamedreturns` fix on the new
`parseKPIDateRange` helper; no cyclop/gocyclo/gocognit/funlen nolints
added) all green for `services/pinpoint`. `go test -race ./pkgs/...` green.

## cloudwatchlogs (this session)

Chosen per the prior session's own note as the next-largest unswept service
(48 L+D+G ops: 11 List/19 Describe/18 Get). `bd show gopherstack-6flj`'s
comments confirm cloudwatchlogs had **not** previously had a 6flj wrapper-key
pass; a different issue (gopherstack-enpq) touched `UpdateAnomaly`'s
suppress-inversion bug and added five absent `Anomaly` members, but that is a
different op family (Anomaly, not AnomalyDetector) and doesn't cover the
List/Describe/Get layer swept here.

PROTOCOL: confirmed `awsAwsjson11_` (JSON-RPC 1.1) from
`cloudwatchlogs@v1.81.1/api_client.go`'s `addProtocolFinalizerMiddlewares`
and the sole prefix present in `deserializers.go` (`grep -o
'awsAwsjson[0-9]*_'` — no `awsRestjson`/`awsEc2query`/`awsAwsquery` prefixes
at all). Case-sensitive, like awsconfig. **All 544 `EqualFold` hits in
`deserializers.go` are in the per-op `deserializeOpError*` functions,
matching against the `errorCode` string** (e.g. `case
strings.EqualFold("InvalidParameterException", errorCode):`) — none are in
a body-field `switch key { case "...":}` block. Spot-checked a dozen
`deserializeOpDocument*Output`/`deserializeDocument*` functions directly:
every one uses a plain `switch key { case "logGroups": ...}`, so a casing
mismatch here is a real bug, not a near-miss.

**Dead-deserializer trap checked and found NOT to apply here** — unlike
pinpoint's restjson1, where `HandleDeserialize` bypasses the generated
`OpDocument*Output` wrapper entirely, cloudwatchlogs's JSON-RPC 1.1
`HandleDeserialize` (e.g. `awsAwsjson11_deserializeOpDescribeLogGroups`,
deserializers.go:4941) decodes the whole body into `shape` and then calls
`awsAwsjson11_deserializeOpDocumentDescribeLogGroupsOutput(&output, shape)`
directly (deserializers.go:4981) — the `OpDocument*Output` function **is**
the real, reached deserializer for every op in this service. Confirmed for
a dozen ops (log groups, streams, queries, anomaly detectors, transformers,
import/export tasks) before citing any of them.

Read all 48 L+D+G ops' response shapes against their own
`awsAwsjson11_deserializeOpDocument<Op>Output` case list (file+line), and
checked the paired `awsAwsjson11_serializeOpDocument<Op>Input` for every op
whose handler reads a filter/identifier field, per this session's
"check the request side too" instruction.

**4 real bugs found and fixed, all on 2 ops in the import-task family:**

1. **`DescribeImportTasks` — broken in both directions (sibling trap).**
   Export and import tasks share this file, and Export genuinely uses
   `taskId` (confirmed: `CancelExportTaskInput`/`DescribeExportTasksInput`
   both serialize `taskId`, serializers.go:8907/9720). Import does **not**
   — `CreateImportTaskInput`/`CancelImportTaskInput` both correctly use
   `importId`/`importRoleArn`/`importSourceArn` (serializers.go:9027,
   8923), but `DescribeImportTasksInput`'s request key is also `importId`
   (serializers.go:9780), and gopherstack's `describeImportTasksInput` read
   `taskId` — the export convention, copied onto import by mistake. A real
   client's `ImportId` filter was silently ignored (the field is optional
   on this op, so the request still succeeded, just returned everything).
   Response side: `DescribeImportTasksOutput`'s wrapper key is `imports`,
   not `importTasks` (deserializers.go:26774,
   `awsAwsjson11_deserializeOpDocumentDescribeImportTasksOutput`) — a real
   client's typed `Imports` field was always empty regardless of backend
   state.
2. **`DescribeImportTaskBatches` — three issues, one of them total-outage
   severity.** Request key is `importId`, not `taskId`
   (serializers.go:9758) — same sibling-trap mistake as above, but this
   field is `required` on this op's handler-side validation, so **every
   real SDK client call failed with `InvalidParameterException: taskId is
   required`, unconditionally**, regardless of what the client sent. This
   op was completely unreachable by any real client before this fix.
   Response wrapper key is `importBatches`, not `importTaskBatches`
   (deserializers.go:9747 request side / the paired Output deserializer,
   case `"importBatches"`). `importId`/`importSourceArn` are also real,
   always-present `DescribeImportTaskBatchesOutput` echo members
   (`api_op_DescribeImportTaskBatches.go`) the handler never emitted, even
   though it already had `input.ImportID` and the looked-up task's
   `ImportSourceArn` on hand — fixed to echo both. `ImportBatches` itself
   stays an empty stub (disclosed below).

**1 real bug found and fixed — invented wrapper (not a sibling trap this
time, a same-file inconsistency):** `GetLogAnomalyDetector` wrapped its
entire response under a fabricated `"anomalyDetector"` key
(`{"anomalyDetector": {...}}`). The real `GetLogAnomalyDetectorOutput`
(`api_op_GetLogAnomalyDetector.go`) has 9 members sitting flat at the top
level, with **no wrapper object at all** — confirmed against
`awsAwsjson11_deserializeOpDocumentGetLogAnomalyDetectorOutput`
(deserializers.go), which switches directly on
`anomalyDetectorStatus`/`detectorName`/etc. The struct that was wrapped
(`LogAnomalyDetector`) also carries `anomalyDetectorArn` — correct for its
other use as `ListLogAnomalyDetectorsOutput`'s per-item shape (that sibling
type, `types.AnomalyDetector`, does have an ARN member), but
`GetLogAnomalyDetectorOutput` has no such member at all. This exact "no
wrapper, members flat at top level" shape was already correctly implemented
for `GetScheduledQuery` in the same file
(`handler_scheduled_queries.go:214`, with its own citing comment) —
`GetLogAnomalyDetector` was the same bug class, just not yet fixed. Every
real SDK client's typed `GetLogAnomalyDetectorOutput` fields
(`AnomalyDetectorStatus`, `DetectorName`, etc.) were nil/zero regardless of
backend state.

**1 real bug found and fixed — backend-tracked-but-unemitted (layer 3):**
`GetTransformer` never emitted `creationTime`/`lastModifiedTime`, both real
`GetTransformerOutput` members (`api_op_GetTransformer.go`). The backend's
`Transformer.CreatedAt` already tracks a timestamp (set on every
`PutTransformer` upsert) but the handler dropped it entirely. Fixed by
emitting `t.CreatedAt.UnixMilli()` for both fields (the backend has no
separate original-creation timestamp once a transformer is updated, so
`CreatedAt` stands in for both — disclosed in-code, not silently
approximated).

**Ratifying tests found and fixed — 2, one per shape variant:**
- `TestHandler_DescribeImportTasks_WireShape` asserted `raw["importTasks"]`
  as if that were the correct key, with a doc comment explicitly claiming
  to "lock the AWS wire shape" while itself encoding the pre-fix bug —
  passed cleanly against broken code because both the handler and the test
  agreed on the wrong key. Rewritten to drive the real SDK client, assert
  `out.Imports` (not a raw map), and prove the `ImportId` request filter
  itself reaches the backend.
- `TestHandler_UpdateLogAnomalyDetector_EnabledPauseResume`'s `getStatus`
  helper asserted `out["anomalyDetector"].(map[string]any)` — the wrong
  wrapper key, present because the pre-fix handler and the test agreed.
  Rewritten to drive the real SDK client and read
  `out.AnomalyDetectorStatus`/`out.DetectorName` directly, which cannot
  compile-pass against a wrapped response.

Also added `TestHandler_DescribeImportTaskBatches_RealClient` (no prior
test drove this op through a real client at all — its only previous
coverage was `TestHandler_ImportTaskBatchesValidation`'s empty-body 400
case, which never sent an id in either key and so couldn't have caught
either direction of the bug) and `TestHandler_GetTransformer_Timestamps`
(same gap: no prior test read `GetTransformerOutput.CreationTime`/
`LastModifiedTime` through a typed client).

**Disclosed, not fixed** (real gaps needing new backend modeling, not a
rename):
- `DescribeImportTaskBatches`'s `ImportBatches` list itself stays an empty
  stub — the backend tracks import tasks but not their per-batch progress
  (no `ImportBatch` model at all). Fixing the wrapper key and id echoes
  doesn't change the empty-list behavior for a real client; a genuine
  round-trip test can't distinguish "correct key, backend has no batches"
  from "wrong key, backend has no batches" here — this fix is client-shape
  correctness, not new data.
- `GetIntegration` never emits `integrationDetails`, a real
  (non-required) `GetIntegrationOutput` member. The real type is a union
  (`types.IntegrationDetails` → `OpenSearchIntegrationDetails`) describing
  provisioned OpenSearch resources (collection ARN, application ARN, data
  access policy) that this backend's `PutIntegration` never simulates
  provisioning — synthesizing plausible-looking ARNs would be fabrication,
  not a rename.
- `GetDataProtectionPolicy` never emits `lastUpdatedTime` (real,
  non-required `GetDataProtectionPolicyOutput` member) — the backend
  stores the policy document as a bare string with no timestamp field.
- `Delivery` (shared by `GetDelivery`/`DescribeDeliveries`) never emits
  `deliveryDestinationType` (real, non-required `types.Delivery` member) —
  would require a join against the `deliveryDestinations` table by ARN at
  response time; the `Delivery` model has no such field or lookup today.
- `Import` (the `DescribeImportTasks` item type) never emits
  `errorMessage`/`importFilter`/`importStatistics` — all real, non-required
  `types.Import` members this backend doesn't simulate import progress or
  failure for.
- `GetLogObject` is structurally out of scope, correctly: it's a true
  HTTP/2 event-stream response (`GetLogObjectOutput.eventStream`,
  confirmed via `api_op_GetLogObject.go`), not a unary JSON body, same
  class as `StartLiveTail`. gopherstack's existing validation-only
  treatment (return a well-formed empty `fieldStream` after validating the
  pointer) was already correct and is unchanged.

**Casing near-misses:** none beyond the key-name bugs already listed above
(no case-only mismatches where the name itself was otherwise right).

**Phantom ops:** none found — every op name in `cwlCoreOps`/
`cwlLatestOps`/`cwlCompletenessOps` corresponds to a real
`api_op_*.go` file in cloudwatchlogs@v1.81.1.

**False-positive rate:** 0 among reported bugs — every finding cites the
real `deserializeOpDocument<Type>`/`serializeOpDocument<Type>Input`
function actually reached from that op's own `HandleDeserialize`/
`addOperation*Middlewares`, file+line, never a doc comment.

Every fix hand-reverted individually (no git, per this session's hard
no-git-mutation constraint), confirmed to fail with the exact predicted
symptom (quoted above), then restored and diffed byte-identical against the
pre-revert file before moving to the next.

Tests: 5 real-SDK-client tests (2 rewritten ratifying tests plus
`TestHandler_DescribeImportTaskBatches_RealClient`,
`TestHandler_GetTransformer_Timestamps`, and
`TestHandler_UpdateLogAnomalyDetector_EnabledPauseResume`'s rewrite) in
`services/cloudwatchlogs/handler_export_tasks_test.go`,
`services/cloudwatchlogs/handler_anomaly_detectors_test.go`, and
`services/cloudwatchlogs/handler_transformers_test.go`.

Gates: `go build`/`go vet`/`go test -race` (scoped to
`services/cloudwatchlogs`), `go fix -diff` (no diff), `golangci-lint run`
(0 issues; one `govet` shadow finding on a test helper's `err` fixed along
the way; no cyclop/gocyclo/gocognit/funlen nolints added) all green.
`go test -race ./pkgs/...` green. Per this session's hard constraints: no
subagents used, no git-mutating commands run (all changes uncommitted —
orchestrator must commit/push), `services/securityhub` untouched (confirmed
via `git status` before starting and again at the end — a sibling session's
in-progress work there, plus separately in-progress `services/inspector2`/
`services/macie2` changes, were both left alone, not mine).

cloudwatchlogs's List/Describe/Get families are now fully swept for this
issue (48/48 ops verified against the real deserializer/serializer). 60 of
162 services swept, 102 remain. Per the ranked table, securityhub (47
L+D+G ops) is next largest, but a sibling session is actively working
there per this session's own observation — s3 (45, `chased` resolution,
flagged as "heavily worked under other issues but not 6flj-swept") or
macie2/guardduty (40 each) are the next candidates that don't collide.

## guardduty (this session)

Chosen as the largest unswept service that doesn't collide with the three
services flagged off-limits by this session's own hard constraints
(`securityhub`, `inspector2`, `macie2` all had a live sibling session's
uncommitted changes, confirmed via `git status` before starting — left
untouched, verified again at the end). Of the remaining candidates, s3 (45
L+D+G ops) was passed over for guardduty (40 ops) because s3's own remainder
note already flags it as large/complex/heavily-touched-under-other-issues
without a dedicated 6flj pass, a poor fit for "settle completely" in one
session; guardduty, by contrast, is a single self-contained REST/JSON
service with no cross-service protocol split, a realistic target to fully
close out.

PROTOCOL: confirmed `awsRestjson1_` from guardduty@v1.85.4's
`deserializers.go` function-prefix grep (only prefix present — no
`awsAwsjson1{0,1}_`/`awsEc2query_`/`awsAwsquery_`). Case-sensitive, like
pinpoint/cloudwatchlogs. All 230 `EqualFold` hits in `deserializers.go` are
either `errorCode` matching in the per-op `deserializeOpError*` functions
(~204) or float special-value parsing (`case strings.EqualFold(jtv, "NaN"):`
/ `"Infinity"` / `"-Infinity"`, ~26, all inside numeric-field decode
branches) — grepped and spot-checked line-by-line; none are in a body-field
`switch key { case "...": }` block, so this service's body-field casing is a
non-issue by construction, not something that needed a live near-miss to
disprove.

**Dead-deserializer trap checked and found NOT to apply** — traced
`HandleDeserialize` for `ListDetectors` (deserializers.go:8551) directly:
it decodes the body into `shape` and calls
`awsRestjson1_deserializeOpDocumentListDetectorsOutput(&output, shape)`
(deserializers.go:8592) itself; no dead `OpDocument...Output` wrapper
switch sits between them for this op, unlike pinpoint's restjson1 shape.
Confirmed this is the general pattern (not spot luck) by reading the
generated `type awsRestjson1_deserializeOp<Op> struct{}`/`HandleDeserialize`
body for `ListDetectors` in full before trusting any other op's
`OpDocument...Output` case list as the real, reached deserializer.

Read all 40 L+D+G ops' response shapes against their own
`awsRestjson1_deserializeOpDocument<Op>Output` case list (file+line via a
per-op grep dump, not hand-transcription), plus the paired
`serializeOp*Input`/`types.go` struct definitions for every op whose
request or response carries a member gopherstack's handler didn't emit or
read.

**This service already had substantial prior work under other issue
classes** (g8k9 backend-tracked-but-unemitted, 21my per-item nesting, m1gl,
h910/ctaz, plus a documented "parity-4" wire-shape audit in
`handler_wireshape_test.go` that fixed 4 bugs: ThreatEntitySet/
TrustedEntitySet missing timestamps, MalwareProtectionPlan's
string-vs-epoch CreatedAt, DescribePublishingDestination's wrong key +
missing tags, GetMalwareScan's wrong-shape mixing) — visible throughout the
handler files as citing comments against the real SDK. That prior work is
why most of the 40 ops (33 of 40) came back genuinely clean: every
Get/List/Describe wrapper key, and every per-item nested shape spot-checked
against its real deserializer case list, matched. The remainder below is
what that prior work had not yet reached.

**3 real bugs found and fixed, all layer-3 (backend already tracked the
data; the handler just never emitted/accepted it) and all following the
same sibling-trap shape: an older shape pair (IPSet/ThreatIntelSet, plain
Filter) missing a field that a newer sibling shape in the same service
(ThreatEntitySet/TrustedEntitySet) already modeled correctly:**

1. **`GetFilter` — missing `createdAt`/`updatedAt`/`version`, three stacked
   gaps on one op.** `Filter.CreatedAt`/`UpdatedAt` were already tracked by
   `CreateFilter`/`UpdateFilter` (filters.go) but `handleGetFilter` never
   emitted either (real `GetFilterOutput.CreatedAt`/`UpdatedAt`, epoch-
   seconds numbers per `awsRestjson1_deserializeOpDocumentGetFilterOutput`'s
   `smithytime.ParseEpochSeconds` call, confirmed non-required but always
   populated once the lifecycle-metadata feature is on, which this backend
   always has). `version` ("Every time the filter is updated, the version
   increments by 1", real doc comment on `GetFilterOutput.Version`) had no
   backing field in the `Filter` model at all. Fixed: added
   `Filter.Version int64`, initialized to 1 in `CreateFilter`, incremented
   in `UpdateFilter`, all three emitted in `handleGetFilter`.
2. **`CreateIPSet`/`UpdateIPSet`/`GetIPSet` and `CreateThreatIntelSet`/
   `UpdateThreatIntelSet`/`GetThreatIntelSet` — `expectedBucketOwner`
   accepted nowhere, tracked nowhere, emitted nowhere.** Real
   `CreateIPSetInput`/`UpdateIPSetInput`/`GetIPSetOutput` (and the
   ThreatIntelSet equivalents) all carry `ExpectedBucketOwner`
   (serializers.go:748 request side, confirmed same key both directions:
   `expectedBucketOwner`). gopherstack's `IPSet`/`ThreatIntelSet` structs
   had no field for it, silently dropping a value a real client supplied on
   create or update — a genuine sibling trap, since the newer
   `ThreatEntitySet`/`TrustedEntitySet` types in the same file set
   (`entity_sets.go`/`handler_entity_sets.go`) already modeled this exact
   field correctly end-to-end (request parse → backend field → conditional
   response emit). Fixed by mirroring that existing pattern onto the older
   pair: added `ExpectedBucketOwner` to both models, threaded it through
   `CreateIPSet`/`UpdateIPSet`/`CreateThreatIntelSet`/`UpdateThreatIntelSet`
   backend signatures and their handlers.
3. **`DescribeOrganizationConfiguration`/`UpdateOrganizationConfiguration`
   — missing `autoEnableOrganizationMembers`, the non-deprecated
   replacement for `autoEnable`.** Real
   `UpdateOrganizationConfigurationInput`/
   `DescribeOrganizationConfigurationOutput` both carry it (NEW/ALL/NONE,
   confirmed same key both directions in serializers.go:7823/
   deserializers.go:3513); the real API doc directly says "we recommend
   using AutoEnableOrganizationMembers" over the deprecated `AutoEnable` —
   this is not a legacy/optional corner, it's the primary modern field. A
   real client setting it via `UpdateOrganizationConfiguration` had the
   value silently dropped, and `DescribeOrganizationConfiguration` never
   echoed it back regardless. Fixed: added `OrgConfig.AutoEnableOrganizationMembers`,
   threaded through the backend method's signature and both handlers.

**Everything else came back clean**, including two internal near-duplicate
pairs that looked like sibling-trap candidates but weren't:
`scanToDescribeMap`/`scanToListMalwareScansMap` (DescribeMalwareScans vs
ListMalwareScans genuinely return two different real shapes, `types.Scan`
vs `types.MalwareScan` — already correctly modeled as two separate
converters with a citing comment from prior work) and `GetMalwareScan`
(deliberately a third, richer shape again, already correct). `GetMembers`/
`ListMembers`/`GetMemberDetectors` all correctly use the real `members` key
(a prior-session comment already flags this as a fixed near-miss against
`GetMemberDetectors`' the wrong `memberDataSources` guess).

**Request side**: checked as part of every fix above — all three are
request+response pairs (the field was missing on both sides, not just one),
confirmed by reading both the `serializeOp*Input`/`serializeDocument*`
functions and the `deserializeOpDocument*Output` functions for each. No
request-only or response-only asymmetry found beyond what's listed.

**Ratifying tests**: none found. No existing test in `filters_test.go`,
`ip_sets_test.go`, `threat_intel_sets_test.go`, or `organization_test.go`
asserted `createdAt`/`updatedAt`/`version`/`expectedBucketOwner`/
`autoEnableOrganizationMembers` at all in either direction — these three
gaps had zero prior coverage (not a wrong assertion staying green, simply
never exercised), consistent with this repo's ~77% never-driven-by-a-real-
client baseline.

**Phantom ops**: none. Extracted all 90 op-name string literals from
`GetSupportedOperations`' backing consts (excluding the `opUnknown =
"Unknown"` sentinel) and confirmed an `api_op_<Name>.go` file exists for
every one in guardduty@v1.85.4.

**False-positive rate**: 0 among reported bugs — every finding cites the
real `deserializeOpDocument<Type>Output`/`serializeOp*Input` function
actually reached from that op's own `HandleDeserialize`, file+line, or the
real `types.go`/`api_op_*.go` struct definition for request-side gaps,
never a doc comment or an assumption.

**Disclosed, not fixed** (structural/optional-field gaps needing new
backend modeling that this session judged too speculative to fabricate,
each independently verified absent from the backend's tracked state):
- `GetDetector`/`DescribeOrganizationConfiguration`'s deprecated
  `dataSources`/`DataSourceConfigurationsResult` legacy member (superseded
  by `features`, not tracked anywhere in this backend's `Detector`/
  `OrgConfig` models — a different concept from `OrgConfig.DataSources`,
  which is a distinct field already correctly modeled and emitted).
- `GetMemberDetectors`' per-item `dataSources`/`features` — the backend's
  member-detector map only ever emits `accountId`/`detectorId`/an
  always-empty `features` list; real `MemberDataSourceConfiguration` has
  more, but this backend has no per-member feature-status model to source
  it from honestly.
- `GetThreatEntitySet`/`GetTrustedEntitySet`'s `errorDetails` — a real,
  optional member only populated when `Status` is an error state; this
  backend's entity sets only ever reach ACTIVE/INACTIVE, so it's correctly
  never emitted rather than fabricated.
- `GetMalwareScan`'s `scanConfiguration`/`scannedResources`/
  `scanResultDetails` — already-disclosed gaps from the prior parity-4
  pass, re-verified still absent from the `MalwareScan` backend model, not
  newly found here.
- `GetFindingsStatistics`'s `nextToken` — the real op supports pagination
  for grouped statistics; this backend computes and returns the full
  grouped list in one call with no pagination cursor concept.
- `ListMalwareProtectionPlans`' per-item `arn` field: gopherstack emits it
  alongside `malwareProtectionPlanId`, but the real
  `MalwareProtectionPlanSummary` type (types.go:2639) has only
  `MalwareProtectionPlanId` — a harmless extra field a real client silently
  ignores (same class as rds's previously-noted `StorageOptimized`), not
  fixed since removing a field a client could already be reading isn't a
  parity improvement.

3 real-SDK-client tests added in `services/guardduty/wire_field_fixes_test.go`
(`TestGetFilter_TimestampsAndVersion`, table-driven
`TestIPSetAndThreatIntelSet_ExpectedBucketOwner` covering both IPSet and
ThreatIntelSet, `TestOrganizationConfiguration_AutoEnableOrganizationMembers`),
all built on the existing `newTestGuardDutyClient` real-SDK-client helper
(`handler_create_tags_test.go`) rather than a new one. Every fix hand-
reverted individually (no git, per this session's hard no-git-mutation
constraint), confirmed to fail with the exact predicted symptom (nil
timestamp field / stale version int / empty-string ExpectedBucketOwner /
empty-string AutoEnableOrganizationMembers, each quoted from the actual
test failure output), then restored and diffed byte-identical against the
pre-revert file before moving to the next.

Gates: `go build`/`go vet`/`go test -race` (scoped to
`services/guardduty`), `go fix -diff` (no diff), `golangci-lint run` (0
issues after a `golines` line-length fix on one call site and a
`fieldalignment` reorder on `OrgConfig` and one inline request struct — no
cyclop/gocyclo/gocognit/funlen nolints added) all green. `go test -race
./pkgs/...` green. Per this session's hard constraints: no subagents used,
no git-mutating commands run (all changes uncommitted — orchestrator must
commit/push), `services/securityhub`/`services/inspector2`/
`services/macie2` untouched (confirmed via `git status` before starting and
again at the end — three sibling sessions' in-progress work there, none of
it mine).

guardduty's List/Describe/Get families are now fully swept for this issue
(40/40 ops verified against the real deserializer/serializer). 61 of 162
services swept, 101 remain. Per the ranked table, securityhub (47 L+D+G
ops) is next largest but still flagged as a live sibling session's
territory as of this session's own `git status` check — s3 (45,
`chased` resolution) or macie2 (40, but also currently a live sibling
session's territory as of this check) are the next candidates; re-check
`git status` for `services/securityhub`/`services/macie2` before picking
either, since both were mid-flight elsewhere as of this session.

## networkmanager (this session)

`git status` at start showed the repo clean except an untracked
`cmd/routecollisions/` (a live sibling session's RouteMatcher-over-claims
sweep per this issue's own assignment note; it grew into
`services/_ROUTE_COLLISIONS.md` and a `routecollisions` binary over the
course of this session — never touched, confirmed again at the end).
securityhub/s3/macie2/inspector2 all showed heavy *other-issue* commit
activity in `git log` right before this session started (gopherstack-n3zi's
round-trip coverage, gopherstack-op3e's RouteMatcher fixes) — none of it a
6flj-specific wrapper-key pass, but picking any of those four risked a live
collision with the sibling sweep, so this session passed on all four in favor
of networkmanager (39 L+D+G per the table, 38 by this session's own direct
enumeration of `GetSupportedOperations`' route table — see method note below),
untouched by any other issue this week and large enough to settle completely
in one session.

**Own enumeration, not the table's count**: grepped every `handler_*.go`
file's route tables for `op: "..."` literals directly (`routeTable()` in
handler.go concatenates 11 per-family route slices) rather than trusting
opencensus's 39 — got 38 (10 List, 1 Describe, 27 Get). The 1-op variance is
inside this file's own documented run-to-run tolerance; not re-derived
further.

**PROTOCOL**: `awsRestjson1_` confirmed as the sole prefix in
networkmanager@v1.44.4's `deserializers.go` (no `awsAwsjson1{0,1}_`/
`awsEc2query_`/`awsAwsquery_`). Case-sensitive. All 538 `EqualFold` hits in
`deserializers.go` are inside `deserializeOpError*` functions matching
`errorCode`, none in a body-field `switch key { case "...": }` block —
grepped for `EqualFold` lines not containing `errorCode` on the same line:
zero matches. Body-field casing is a non-issue by construction here.

**Dead-deserializer trap checked and found NOT to apply**: traced
`(*awsRestjson1_deserializeOpGetSites).HandleDeserialize`
(deserializers.go:9748) in full — it decodes the body into `shape` and calls
`awsRestjson1_deserializeOpDocumentGetSitesOutput(&output, shape)` directly,
the same pattern guardduty and cloudwatchlogs already confirmed for restjson1
in this codebase (unlike pinpoint's genuinely-dead wrapper). Not re-verified
per-op after confirming the general pattern once.

**Layer 1 (wrapper keys), all 38 ops**: dumped every op's own
`awsRestjson1_deserializeOpDocument<Op>Output` case list via a per-op awk/grep
script (file+line implicit in the dump, not hand-transcribed) and compared
against wire.go's response structs. **All 38 top-level wrapper keys matched
exactly** — zero layer-1 bugs. This service's wire.go already carried a
citing doc comment ("confirmed by direct read of
aws-sdk-go-v2/service/networkmanager@v1.44.3's serializers.go/
deserializers.go") from prior (non-6flj) work, which the clean layer-1 result
corroborates.

**Layer 2 (per-item nesting), all major shared types**: dumped every
`awsRestjson1_deserializeDocument<Type>` case list for ~50 nested/shared
types (GlobalNetwork, Site, Device, Link, Connection, Attachment + its 5
subtype envelopes, Peering + TransitGatewayPeering, ConnectPeer +
ConnectPeerSummary, CoreNetwork + CoreNetworkSummary, RouteAnalysis + its
path/endpoint/completion types, NetworkResource, NetworkTelemetry,
OrganizationStatus, error types) and compared field-for-field against
wire.go/types.go. **7 real bugs found and fixed**, all layer-2/3 (correct
outer shape, missing or unwired inner fields) — the classic guardduty-style
"backend has the value one field away, converter never reads it" shape,
recurring across five different resource families rather than concentrated
in one:

1. **`OwnerAccountId` never emitted on Attachment (all 5 subtypes),
   TransitGatewayPeering, RouteAnalysis, or CoreNetworkSummary** — the
   single highest-value finding, a genuine service-wide sibling trap.
   `introspection.go`'s `NetworkResource.AccountId` already correctly reads
   `b.accountID` (confirmed at introspection.go:445/471, pre-existing code),
   but `newAttachmentLocked` (attachments.go, the single shared constructor
   for all 5 attachment subtypes), `CreateTransitGatewayPeering`
   (peerings.go), and `StartRouteAnalysis` (routeanalysis.go) never read it
   at all — real `OwnerAccountID` model fields on `Attachment`/`Peering`/
   `RouteAnalysis` (confirmed present in models.go) sat unset the whole
   time. `CoreNetworkSummary`'s converter (`toCoreNetworkSummaryWire`,
   wire_convert.go) was worse: it **hardcoded `OwnerAccountID: ""`**
   explicitly, a fabricated-empty rather than merely-unset value. Fixed by
   threading `b.accountID` through all four construction paths (one shared
   constructor covers all 5 attachment ops at once) and adding an
   `ownerAccountID` parameter to `toCoreNetworkSummaryWire`, sourced from
   `h.Backend.AccountID()` at its one call site
   (`dispatchListCoreNetworks`). `CoreNetwork` itself (`GetCoreNetwork`'s
   response type) genuinely has no `OwnerAccountId` member in the real SDK —
   confirmed absent from its own deserializer case list — so only
   `CoreNetworkSummary` needed the fix, not both.
2. **`RouteAnalysis.UseMiddleboxes` read from the request into a backend
   parameter explicitly discarded with `_`, never echoed.** `StartRouteAnalysis`'s
   signature was `(..., includeReturnPath, _ bool)` — the handler already
   parsed `req.UseMiddleboxes` and passed it in, the backend method just threw
   it away. Real `GetRouteAnalysisOutput`/`StartRouteAnalysisOutput` both
   carry `UseMiddleboxes` (confirmed in the op's own case list). A real
   client's `UseMiddleboxes: true` request had zero effect and could never be
   observed in the response. Fixed by keeping the parameter and adding a model
   field.
3. **`RouteAnalysis.StartTimestamp` never modeled at all** — a real,
   always-populated `RouteAnalysis` member (`StartTimestamp *time.Time`,
   confirmed in types.go) with no backing field in the model struct. Fixed by
   adding the field, set to `nowUTC()` at `StartRouteAnalysis` time.
4. **`GetNetworkResources`' `NetworkResource.ResourceId`/`.Tags` never
   emitted, service-wide, all 7 resource kinds** — a sibling trap against
   this service's OWN `NetworkTelemetry` type, which correctly emits
   `ResourceId` (confirmed at deserializers.go's `NetworkTelemetry` case
   list) three functions away in the same file. `networkResourceItem`, the
   shared internal struct all 7 of `introspection.go`'s per-kind gatherers
   (site/device/link/connection/core-network/attachment/connect-peer/peering)
   build into before wire conversion, had no `ResourceID`/`Tags` fields at
   all — every source struct's own ID (`SiteID`/`DeviceID`/`LinkID`/
   `ConnectionID`/`CoreNetworkID`/`AttachmentID`/`ConnectPeerID`/`PeeringID`)
   and `Tags` field were one field access away and simply never read. Fixed
   by adding both fields to `networkResourceItem`, populating them in all 7
   gatherers, and threading them through to `networkResourceWire` in
   `dispatchGetNetworkResources`.
5. **`ListCoreNetworks`' `CoreNetworkSummary.Tags` never emitted** — real
   `CoreNetworkSummary` has a `Tags []Tag` member (confirmed in its own
   deserializer case list and types.go) that `toCoreNetworkSummaryWire`
   simply omitted, even though the `CoreNetwork` model it reads from already
   tracks `Tags *tags.Tags` (used correctly by `GetCoreNetwork`'s own
   converter three functions away). Fixed.
6. **`GetConnectPeer`'s `ConnectPeer.LastModificationErrors` never modeled**
   — a real member (`[]ConnectPeerError`, confirmed in types.go) with no
   field on gopherstack's `ConnectPeer` struct at all, the same "declared
   type, honestly never populated" gap `AttachmentError`/`PeeringError`
   already carry a citing comment for elsewhere in this file (this backend
   has no failure-injection engine for any of the three). Added
   `ConnectPeerError` (mirroring `AttachmentError`'s exact 4-field shape:
   Code/Message/RequestID/ResourceArn) and the `LastModificationErrors`
   field, matching house convention rather than leaving the type
   incomplete.
7. **`PeeringError` missing `ResourceArn`** — a direct sibling-trap: its
   4-field twin `AttachmentError` (Code/Message/RequestID/ResourceArn)
   already has it; `PeeringError` had only 3 of the real type's 5 members
   (also missing `MissingPermissionsContext`, left undone — see disclosed
   list). Fixed the `ResourceArn` half since it mirrors an existing correct
   sibling exactly; `toPeeringErrorsWire`'s direct struct-cast conversion
   (`peeringErrorWire(e)`) meant both the model and wire type needed the new
   field added in the same relative position to keep compiling.

**Checked and confirmed correct, not new findings** (candidates that looked
like sibling-trap shapes but were already honestly handled):
`CoreNetworkChangeValues`/`CoreNetworkChangeEventValues` (the real SDK has
two DIFFERENT ~10-14-field types here; gopherstack's shared
`coreNetworkChangeValuesWire` only carries `SegmentName`/
`NetworkFunctionGroupName`, but `corenetworkpolicydiff.go`'s doc comment and
`models.go`'s `CoreNetworkChangeValues` doc comment already disclose this
explicitly as a documented scope reduction — this diff engine does a
document-level JSON diff, not a live-attachment-state correlation, so most of
those fields have nothing real to source); `CoreNetwork.NetworkFunctionGroups`
(`[]struct{}`, already disclosed in models.go's doc comment, "no
policy-execution engine computes them"); `ListCoreNetworkRoutingInformation`/
`GetNetworkRoutes`' empty route lists (already disclosed in
introspection.go/corenetworks.go doc comments, no route-propagation engine
exists); `TransitGatewayRegistrationState` (real type is actually
`TransitGatewayRegistrationStateReason{Code,Message}` — my first grep missed
it by name, but wire.go's `transitGatewayRegistrationStateWire{Code,Message}`
already matches it exactly).

**Request side**: spot-checked the 10 largest/highest-field-count Create/
request bodies (`CreateVpcAttachment`, `StartRouteAnalysis`,
`ListCoreNetworkRoutingInformation`, `GetNetworkRoutes`,
`CreateConnectAttachment`, `CreateTransitGatewayPeering`,
`CreateSiteToSiteVpnAttachment`, `CreateDirectConnectGatewayAttachment`,
`CreateTransitGatewayRouteTableAttachment`, `UpdateVpcAttachment`) against
their real `serializeOpDocument<Op>Input` functions field-for-field. All
clean — the only systematic omission is `ClientToken`, deliberately and
consistently absent from every Create request wire struct in this service (an
idempotency token with no meaningful backend behavior to model), not a
per-op miss.

**Wrong-value check**: none found — every mismatch in this batch was a
missing/dropped field, not a same-key-wrong-enum-value bug.

**Ratifying tests**: none found in any of the three shapes. Grepped every
existing `*_test.go` in the service for `OwnerAccountID`/`UseMiddleboxes`/
`StartTimestamp`/the fields fixed above — zero prior assertions on any of
them in either direction (not a wrong assertion staying green, simply never
exercised, same as guardduty's finding).

**Phantom ops**: none found — cross-referenced all 38 op-name string
literals pulled from `handler_*.go`'s route tables against
`api_op_*.go` files in networkmanager@v1.44.4; every one exists.

**False-positive rate**: 0 among reported bugs — every finding cites the real
`deserializeOpDocument<Type>Output`/`serializeOpDocument<Type>Input`
function's own case list or the real `types.go` struct definition, never a
doc comment or an assumption. Two candidates that looked like bugs on first
read (`CoreNetworkChangeValues` field gap, `NetworkFunctionGroups` stub) were
checked against this service's own doc comments and confirmed already
disclosed rather than reported as new.

7 real-SDK-client tests added in
`services/networkmanager/wire_field_fixes_test.go`
(`TestOwnerAccountID_Attachment`, `TestOwnerAccountID_PeeringAndCoreNetworkSummary`,
`TestGetNetworkResources_ResourceIDAndTags`, `TestListCoreNetworks_Tags`,
`TestRouteAnalysis_OwnerAccountIDStartTimestampUseMiddleboxes` — 5 test
functions, some covering more than one bug each). Every fix hand-reverted
individually (no git, per this session's hard no-git-mutation constraint),
confirmed to fail with the exact predicted symptom (blank `OwnerAccountId`
strings, nil `StartTimestamp`, empty `Tags`/`ResourceId`, quoted in each
revert's test output), then restored and diffed byte-identical against the
pre-revert file before moving to the next. `PeeringError.ResourceArn` and
`ConnectPeerError`/`LastModificationErrors` were NOT given round-trip tests —
both are genuinely unobservable in this backend today (no failure-injection
engine ever populates either error list, matching the pre-existing
`AttachmentError` disclosure), so a test could only assert on an empty list
regardless of correctness; disclosed rather than fabricated.

Gates: `go build`/`go vet`/`go test -race` (scoped to
`services/networkmanager`), `go fix -diff` (no diff), `golangci-lint run` (2
issues found and fixed — a `golines` line-length wrap and a `fieldalignment`
struct reorder on the new `networkResourceItem` fields, via
`fieldalignment -fix` + `golines -w`; 0 issues after; no cyclop/gocyclo/
gocognit/funlen nolints added) all green. `go test -race ./pkgs/...` green.

Per this session's hard constraints: no subagents used (Read/Grep/Bash only),
no git-mutating commands run (all changes uncommitted — orchestrator must
commit/push), `cmd/routecollisions/`/`services/_ROUTE_COLLISIONS.md`/
`routecollisions` (the live sibling RouteMatcher sweep's output) confirmed
untouched via `git status` both before starting and again at the end, no
`gendocs`/`make docs` run.

networkmanager's List/Describe/Get families are now fully swept for this
issue (38/38 ops verified against the real deserializer/serializer). 62 of
162 services swept, 100 remain. Per the ranked table, securityhub (47
L+D+G ops) is next largest; re-check `git status` for
`services/securityhub`/`services/s3`/`services/macie2`/`services/inspector2`
before picking any of them, since all four showed recent non-6flj activity as
of this session (gopherstack-n3zi/op3e work, plus the live RouteMatcher
sweep) — personalize (39, but already had a direct List-scoping fix under
gopherstack-sm02, so may come back mostly clean) or cognitoidp (37) are the
next candidates least likely to collide.

## securityhub (this session)

Chosen as the largest unswept service per the ranked table (116 total ops,
47 L+D+G: 15 List/8 Describe/24 Get). `git status` at start showed the repo
clean except an untracked `cmd/routecollisions/`/`routecollisions`/
`services/_ROUTE_COLLISIONS.md` (the live RouteMatcher-sweep sibling this
issue's assignment note says to avoid) and a modified
`test/integration/kafka_test.go` (that sibling's regression-guard test for a
false positive it found and did NOT fix) — neither touches securityhub, so
it was clear to take. securityhub itself had one prior fix landed just
before this session (`a309b74fc`, already in `git log`) but that was scoped
to `gopherstack-n3zi`/`gopherstack-op3e` (a RouteMatcher collision plus 3
findings-filter bugs it exposed) — 12 ops driven end to end, not a
6flj-scoped L+D+G pass, so all 47 ops here still needed a fresh read.

**PROTOCOL**: `awsRestjson1_` confirmed as the sole prefix in
securityhub@v1.75.4's `deserializers.go` (3,848 hits, no `awsAwsjson1{0,1}_`/
`awsEc2query_`/`awsAwsquery_`). Case-sensitive. All 697 `EqualFold` hits in
`deserializers.go`: 90 lack `errorCode` on the same line, and every one of
those 90 is `case strings.EqualFold(jtv, "NaN"|"Infinity"|"-Infinity"):`
inside numeric-field decode branches (grepped for `EqualFold` lines with
neither `errorCode` nor `NaN`/`Infinity` on them: zero matches). Body-field
casing is a non-issue by construction, same shape as guardduty/
networkmanager's prior restjson1 results in this codebase.

**Dead-deserializer trap checked and found NOT to apply**: traced
`HandleDeserialize` for `GetFindings` in full (deserializers.go:11073) — it
decodes the body into `shape` and calls
`awsRestjson1_deserializeOpDocumentGetFindingsOutput(&output, shape)`
directly (line 11113). Spot-checked four more ops spanning different
families (`ListMembers`, `DescribeStandards`, `GetConfigurationPolicy`,
`ListFindingAggregators`) before trusting the pattern generally, all the
same shape.

Read all 47 L+D+G ops' response shapes against their own
`awsRestjson1_deserializeOpDocument<Op>Output` case list (dumped via a
per-op awk script, file+line implicit), plus every shared nested type
(Member, Invitation, StandardsSubscription, StandardsControl,
ConfigurationPolicySummary, ConfigurationPolicyAssociationSummary,
AutomationRulesMetadataV2, ConnectorSummary, SecurityControlDefinition,
Product, FindingHistoryRecord, GroupByResult, TrendsMetricsResult, and
more) against `types.go`. Findings and BatchImportFindings/BatchUpdateFindings
are confirmed pass-through (stored and returned as opaque
`map[string]any`, never reshaped), so the OCSF/ASFF finding body itself is
structurally immune to this bug class and wasn't swept field-by-field.

**8 real bugs found and fixed**, spanning every variant this issue's brief
calls out:

1. **`ListConfigurationPolicies` — wrong wrapper key, silent-empty
   (flagship pattern).** Emitted `ConfigurationPolicySummaryList`; real key
   is `ConfigurationPolicySummaries`
   (`awsRestjson1_deserializeOpDocumentListConfigurationPoliciesOutput`). A
   real client's typed `.ConfigurationPolicySummaries` was always empty
   regardless of backend state. **Same bug, same fix, on
   `ListConfigurationPolicyAssociations`**: emitted
   `ConfigurationPolicyAssociationSummaryList`, real key is
   `ConfigurationPolicyAssociationSummaries`. Three existing raw-body tests
   in `configuration_policies_test.go` asserted the wrong keys as correct
   (both handler and test agreed on the bug) — rewritten to the real keys.
2. **`ConfigurationPolicySummary.ServiceEnabled` never emitted — value the
   backend already holds, one step from the wire.** The real, required
   `ConfigurationPolicySummary` member (types.go) sits inside the opaque
   `ConfigurationPolicy` document the backend already stores verbatim
   (`p.ConfigurationPolicy["SecurityHub"]["ServiceEnabled"]`, confirmed
   against the real single-variant `types.Policy` union,
   `PolicyMemberSecurityHub`) — never extracted into the List summary.
   Fixed via a new `configPolicyServiceEnabled` helper.
3. **`StandardsSubscription` — wrong key, sibling trap.** Emitted
   `StatusReason`; real key is `StandardsStatusReason`
   (`awsRestjson1_deserializeDocumentStandardsSubscription`). This backend
   never sets a subscription's status-reason (no INCOMPLETE/FAILED
   lifecycle), so the *value* is unobservably nil either way — only the key
   name was wrong, fixed and disclosed as untested for the value (see
   below). A ratifying test explicitly asserted the wrong key's presence as
   correct; renamed to assert the real key.
4. **`GetAdministratorAccount`/`GetMasterAccount` — wrong key, sibling
   trap.** Both real outputs are `*types.Invitation{AccountId, InvitationId,
   InvitedAt, MemberStatus}` (confirmed same type both ops). gopherstack
   emitted `RelationshipStatus` instead of `MemberStatus` — a genuine
   sibling trap against the correctly-named `Invitation` model used three
   lines away by `ListInvitations` in the same file. A real client's typed
   `.MemberStatus` was always empty regardless of backend state. Fixed by
   renaming the `AdminAccount.RelationshipStatus` field itself (4 call
   sites total) to `MemberStatus`.
5. **AutomationRuleV2 family (`GetAutomationRuleV2`, `ListAutomationRulesV2`
   in L+D+G scope; the same shared builder also serves
   `CreateAutomationRuleV2`/`UpdateAutomationRuleV2`) — two stacked bugs,
   one generational sibling trap.** `RuleId` (real member on both
   `GetAutomationRuleV2Output` and `AutomationRulesMetadataV2`, types.go)
   was emitted as `Identifier` — always empty for a real client. `IsTerminal`
   was fabricated: it's a real member on the **V1** `AutomationRulesMetadata`
   only (types.go:872, confirmed still correctly used by this service's own
   V1 `ListAutomationRules`/`BatchGetAutomationRules` three functions away
   in the same file) and does not exist anywhere in the V2 shapes — a
   generational sibling trap, V1's field carried over onto V2 by mistake.
   Also fixed on the **request side**: `CreateAutomationRuleV2Input`/
   `UpdateAutomationRuleV2Input` have no `IsTerminal` member at all
   (confirmed against both real Input structs) — the handler was reading a
   key no real client ever sends; removed the dead read and threaded-through
   backend parameter/model field. Two existing raw-body tests
   (`TestAutomationRulesV2`'s CRUD-lifecycle steps,
   `TestUpdateAutomationRuleV2_ActionsApplied`) asserted `Identifier` as
   correct — rewritten to `RuleId`; the lifecycle test actually panics
   (nil-interface-to-string) against the unfixed key, not just fails an
   assertion.
6. **`ListOrganizationAdminAccounts` — missing required-echo, request side
   never read.** Real `ListOrganizationAdminAccountsInput.Feature` (query
   param, "Defaults to Security Hub CSPM if not specified") is always
   echoed back on `ListOrganizationAdminAccountsOutput.Feature`
   (confirmed both directions in `api_op_ListOrganizationAdminAccounts.go`).
   gopherstack read neither. This backend doesn't track admin accounts
   per-feature, so the echo isn't filtered by it, only reflected back
   (default `"SecurityHub"` when unset) — a real client's typed `.Feature`
   was always empty regardless of the request before this fix.
7. **`ListConnectorsV2` — wrong per-item shape, invented + missing fields
   stacked on one op.** Real `types.ConnectorSummary` (types.go:14833)
   requires a nested `ProviderSummary{ConnectorStatus, ProviderConfiguration,
   ProviderName}` object; gopherstack emitted a flat `Provider` plus a
   top-level `ConnectorStatus`/`UpdatedAt` that don't exist on
   `ConnectorSummary` at all. `ProviderName` is derivable without new
   backend state — mirrored the already-correct V1 `CspmConnector` sibling
   pattern exactly (`extractCspmProviderTag` + `strings.ToUpper`,
   `connectors.go`), which `ConnectorV2` had never picked up. Added a new
   `connectorV2ToSummaryResponse` used only by `ListConnectorsV2`, leaving
   `connectorV2ToResponse` (Create/Update/Register — genuinely different
   real shapes each, disclosed below, out of L+D+G scope) unchanged. A real
   client's typed `.ProviderSummary` was always the zero value regardless
   of backend state.
8. **`DescribeProducts` — backend-tracked-but-unemitted (layer 3).**
   `Product.ProductSubscriptionResourcePolicy` is a real, tracked model
   field (models.go) that the item builder never read — a real,
   non-required `DescribeProductsOutput` member
   (`awsRestjson1_deserializeDocumentProduct`). No seed/create path in this
   backend ever sets a non-empty value today, so (like finding #3) the fix
   is shape-correct but its value is currently unobservable; disclosed as
   untested for the same reason.

**Checked and confirmed correct, not new findings**: `DescribeActionTargets`,
`DescribeHub`, `DescribeSecurityHubV2` (+ nested `FeatureDetail`),
`DescribeOrganizationConfiguration` (+ nested `OrganizationConfiguration`),
`GetInsights`/`GetInsightResults`, `GetEnabledStandards`,
`DescribeStandards`/`DescribeStandardsControls`, `ListStandardsControlAssociations`
(+ both its Summary/Detail sibling item types — genuinely different real
shapes, both matched exactly), `GetConfigurationPolicy`,
`GetConnector`/`ListConnectors` (V1 — has its own citing comment from prior
work, re-verified), `GetConnectorV2` (partially — see disclosed below),
`GetAggregatorV2` (+ `ListAggregatorsV2`, see disclosed below),
`GetFindingAggregator`/`ListFindingAggregators`, `GetSecurityControlDefinition`/
`ListSecurityControlDefinitions` (all but the disclosed `Provider` gap),
`GetFindingHistory` (+ nested `FindingHistoryRecord`/`FindingHistoryUpdateSource`),
`GetFindingsV2`, `GetFindingStatisticsV2`/`GetResourcesStatisticsV2` (+
nested `GroupByResult` — already had citing comments from prior work),
`GetFindingsTrendsV2`/`GetResourcesTrendsV2` (+ nested `TrendsMetricsResult`
— ditto), `GetResourcesV2`, `ListTagsForResource`.

**Request side**: checked as part of every fix above (#2, #5, #6 are all
request-or-both-directions bugs); additionally spot-checked
`CreateConfigurationPolicy`/`CreateConnectorV2`/`CreateAggregatorV2`/
`CreateFindingAggregator` request bodies against their real
`serializeOpDocument<Op>Input` functions — all clean, no additional
request-only gaps found beyond what's listed.

**Wrong-value check**: none found — every mismatch in this batch was a
missing/wrong-named/wrong-shaped field, not a same-key-wrong-enum-value bug.

**Ratifying tests found and fixed — 5, ranging across two of the three
shapes this issue tracks (wrong key; none found with an assertion too weak
to fail)**: the three `ConfigurationPolicy*SummaryList` raw-body assertions
(#1), the `StatusReason` raw-body assertion (#3), and the two
`Identifier`-asserting AutomationRuleV2 tests, one of which panics rather
than merely fails against the unfixed code (#5).

**Phantom ops**: none. Extracted all 117 op-name string literals from
`handler.go`'s `op*` const declarations (116 real + the `opUnknown =
"Unknown"` sentinel) and confirmed an `api_op_<Name>.go` file exists for
every one in securityhub@v1.75.4.

**False-positive rate**: 0 among reported bugs — every finding cites the
real `deserializeOpDocument<Type>Output`/`deserializeDocument<Type>`
function's own case list or the real `types.go`/`api_op_*.go` struct
definition, file+line, never a doc comment or an assumption.

**Disclosed, not fixed** (structural gaps needing new backend modeling this
session judged too speculative to fabricate, or genuinely unobservable
values where only the key/shape was fixable):
- `GetConnectorV2` never emits `EnablementStatus`/`EnablementStatusReason`/
  `KmsKeyArn` (all real, optional `GetConnectorV2Output` members) — the
  `ConnectorV2` model has no enablement-lifecycle concept at all (always
  created `ConnectorStatus: "ACTIVE"`, no PENDING state unlike V1's
  `CspmConnector`), so there's no real value to source these from without
  inventing new backend state.
- `CreateConnectorV2Output`/`UpdateConnectorV2Output`/
  `RegisterConnectorV2Output` each have their own, genuinely different real
  shape from `ConnectorSummary` and from each other (`UpdateConnectorV2Output`
  is just `{ConnectorStatus, EnablementStatus}`, no ID/ARN/Name at all;
  `RegisterConnectorV2Output` is just `{ConnectorId, ConnectorArn}`) — all
  three currently reuse the single `connectorV2ToResponse` builder, which
  matches none of them exactly. Found, not fixed: these are Create/Update
  ops, outside this issue's List/Describe/Get scope, and building three more
  correct-shaped response functions is a larger side quest than this pass's
  settle-securityhub goal justified. Flagging for a future request-side/
  non-L+D+G pass.
- `GetAggregatorV2`/`ListAggregatorsV2` (`AggregatorV2` per-item type) both
  emit harmless extra fields (`CreatedAt`/`UpdatedAt` on Get; the full
  Get-shaped object on List, where the real `types.AggregatorV2` list item
  is genuinely just `{AggregatorV2Arn}`) — not fixed, since nothing real is
  dropped, matching this issue's established "harmless extra field, real
  client ignores it" non-bug precedent (rds `StorageOptimized`, guardduty
  `MalwareProtectionPlanSummary.arn`).
- `GetSecurityControlDefinition`/`ListSecurityControlDefinitions`/
  `BatchGetSecurityControls` never emit the real, optional `Provider`
  member (`SecurityControlsProvider`-typed) — this backend has no
  multi-cloud-provider concept for controls at all (every control is
  implicitly AWS-native), and this session couldn't confirm the enum's
  exact wire spelling from the pinned SDK's `enums.go` in the time
  available, so defaulting to a guessed value was judged worse than
  omitting.
- **`GetRecommendedPolicyV2`/`GenerateRecommendedPolicyV2` — entirely
  invented response shape, the most severe finding this session, not
  fixed.** Real `GetRecommendedPolicyV2Output` is `{Error, NextToken,
  RecommendationSteps, RecommendationType, ResourceArn, Status}` (a
  genuinely async, poll-style op — `GenerateRecommendedPolicyV2Output` is
  empty, just a trigger); gopherstack's `RecommendedPolicyV2` model instead
  synchronously computes and returns `{MetadataUid, Policy, GenerationTime}`,
  none of which exist on the real type. A real client's typed
  `RecommendationSteps`/`ResourceArn`/`Status`/`RecommendationType` fields
  are always nil/empty regardless of backend state today. Not fixed because
  `RecommendationStep` is a non-trivial union type and this backend tracks
  no resource/finding-linkage data to source `ResourceArn`/meaningful step
  content from — fabricating plausible-looking recommendation content would
  be worse than the current gap. **Flag, don't fix**, exactly per this
  issue's own guidance for genuinely-unmodeled invented shapes.
- `FindingHistoryRecord`'s real, optional per-record `NextToken` member
  (types.go) has no natural single value in this backend's pagination model
  (top-level `GetFindingHistoryOutput.NextToken` already covers real
  pagination correctly) — omitted, not fabricated.

3 real-SDK-client tests added in `services/securityhub/wire_field_fixes_test.go`
(`TestGetAdministratorAndMasterAccount_MemberStatus`,
`TestListOrganizationAdminAccounts_FeatureEcho`,
`TestListConnectorsV2_ProviderSummaryShape`), plus 5 existing raw-body tests
rewritten to the real keys (see ratifying-tests above). Every fix hand-
reverted individually (no git, per this session's hard no-git-mutation
constraint), confirmed to fail with the exact predicted symptom (quoted
wrong/empty values, or a panic for the AutomationRuleV2 lifecycle test),
then restored and diffed byte-identical against the pre-revert file before
moving to the next. **Two fixes (#3 `StandardsStatusReason`, #8
`ProductSubscriptionResourcePolicy`) were explicitly NOT given a new
value-asserting test** — the backend never populates either value today, so
a test could only ever assert "still empty" regardless of correctness;
disclosed as untested rather than written as a hollow test. The #5
`IsTerminal` *removal* (as opposed to the `RuleId` rename, which the
lifecycle test does cover) is similarly untestable by assertion — re-adding
the fabricated field back in and rerunning the full suite produced zero
failures, confirmed by hand before concluding it needed disclosure instead
of a test.

Gates: `go build`/`go vet`/`go test -race` (scoped to `services/securityhub`),
`go fix -diff` (no diff), `fieldalignment` (0 findings), `golangci-lint run`
(0 issues after removing one now-stale `//nolint:goconst` — the V1
`IsTerminal` string literal dropped below goconst's 3-occurrence threshold
once the V2 duplicate was deleted — and adding one `//nolint:staticcheck`
for the intentional, in-scope use of the SDK-deprecated-but-still-real
`GetMasterAccount`; no cyclop/gocyclo/gocognit/funlen nolints added) all
green. `go test -race ./pkgs/...` green.

Per this session's hard constraints: no subagents used (Read/Grep/Bash
only), no git-mutating commands run (all changes uncommitted — orchestrator
must commit/push), `cmd/routecollisions/`/`services/_ROUTE_COLLISIONS.md`/
`routecollisions`/`test/integration/kafka_test.go` (the live sibling
RouteMatcher sweep's output) confirmed untouched via `git status` both
before starting and again at the end — a second sibling session's
`services/apigateway/handler.go`/`test/integration/apigateway_quicksight_account_test.go`
changes appeared partway through this session and were also left alone
(confirmed via `git status`, not securityhub-related), no `gendocs`/
`make docs` run.

securityhub's List/Describe/Get families are now fully swept for this issue
(47/47 ops verified against the real deserializer/serializer). 63 of 162
services swept, 99 remain. Per the ranked table, s3 (45 L+D+G ops, `chased`
resolution) is next largest but is flagged in this file's own header as
"heavily worked under other issues but not 6flj-swept" — a poor fit for
settling in one session; macie2 (40) or personalize (39, may come back
mostly clean per gopherstack-sm02) are the next candidates, but re-check
`git status` before picking either given this session's own experience of
two different sibling sessions touching unrelated services mid-flight.
