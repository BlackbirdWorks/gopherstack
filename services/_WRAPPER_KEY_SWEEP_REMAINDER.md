# Wrapper-key / nested-shape sweep remainder (gopherstack-6flj)

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

## Swept (59 of 162) — do not re-sweep without reading the cited work first

Every op in these services has had at least one full layer-1 (wrapper key)
pass; most also have layer-2 (nesting) and layer-3 (backend-tracked-but-
unemitted) passes. Read `bd show gopherstack-6flj` (notes + the one comment)
for per-service detail and commit citations before touching any of these
again — several have explicit "already checked, don't re-flag" notes (e.g.
route53's `ListHostedZonesByVPC` XMLName quirk, cloudfront's root-tag
non-bug, rds's `GlobalClusterMember` shared-name non-bug).

apigateway, appstream, athena, autoscaling, **awsconfig** (this session),
backup, bedrock, bedrockagent, cleanrooms, cloudformation, cloudfront,
cloudfrontkeyvaluestore, cloudwatch, codebuild, codecommit, datasync, dlm,
dynamodbstreams, ec2, ecs, eks, elasticache, elbv2, forecast, glue, iam,
identitystore, inspector2, iot, iotwireless, kms, lambda, lightsail,
medialive, mgn, networkmonitor, omics, opensearch, organizations,
**pinpoint** (this session), quicksight, rds, redshift, resiliencehub,
resourcegroupstaggingapi, route53, s3control, s3tables, sagemaker,
secretsmanager, servicediscovery, ses, sesv2, sns, sqs, ssm, ssoadmin,
stepfunctions, transfer.

Two services have real, extensive wire-shape work under **other** issue
classes (gopherstack-enpq's required-member diff, gopherstack-h910/ctaz's
backend-logic fixes) but **no 6flj-specific wrapper-key pass on record** —
s3 and dynamodb. They are listed in the unswept table below on purpose;
don't assume "heavily worked on" means "settled for this issue."

## Unswept (103 of 162), ranked by List+Describe+Get op count

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

Sum of the L+D+G column across all 103 (pinpoint's 53 removed, swept this
session): **1,689** candidate ops.

| service | total ops | list | describe | get | L+D+G | resolution |
|---|---:|---:|---:|---:|---:|---|
| cloudwatchlogs | 118 | 11 | 19 | 18 | 48 | chased |
| securityhub | 116 | 15 | 8 | 24 | 47 | dynamic-fallback |
| s3 | 115 | 12 | 0 | 33 | 45 | chased |
| macie2 | 81 | 15 | 3 | 22 | 40 | direct |
| guardduty | 90 | 16 | 3 | 21 | 40 | direct |
| personalize | 77 | 18 | 18 | 3 | 39 | dynamic-fallback |
| networkmanager | 186 | 10 | 1 | 28 | 39 | chased |
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
