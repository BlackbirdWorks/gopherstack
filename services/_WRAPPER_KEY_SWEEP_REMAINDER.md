# Wrapper-key / nested-shape sweep remainder (gopherstack-6flj)

**73 of 162 services swept, 89 remain** (apigatewayv2, workmail, wafv2, ce,
waf, and now vpclattice all added this session, in parallel, by different
sessions — see each service's own section at the end of this file for full
detail).

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

## Swept (64 of 162) — do not re-sweep without reading the cited work first

Every op in these services has had at least one full layer-1 (wrapper key)
pass; most also have layer-2 (nesting) and layer-3 (backend-tracked-but-
unemitted) passes. Read `bd show gopherstack-6flj` (notes + the one comment)
for per-service detail and commit citations before touching any of these
again — several have explicit "already checked, don't re-flag" notes (e.g.
route53's `ListHostedZonesByVPC` XMLName quirk, cloudfront's root-tag
non-bug, rds's `GlobalClusterMember` shared-name non-bug).

apigateway, **apigatewayv2** (this session), appstream, athena, autoscaling,
awsconfig, backup, bedrock,
bedrockagent, **ce** (this session), cleanrooms, cloudformation, cloudfront,
cloudfrontkeyvaluestore, cloudwatch, cloudwatchlogs, codebuild, codecommit,
cognitoidp, datasync, dlm, dynamodbstreams, ec2, ecs, eks,
elasticache, elbv2, forecast,
glue, guardduty, iam, identitystore, inspector2, iot,
iotwireless, kms, lambda, lightsail, macie2, medialive,
mgn, networkmanager, networkmonitor, omics,
opensearch, organizations, personalize, pinpoint,
quicksight, rds, redshift,
resiliencehub, resourcegroupstaggingapi, route53, s3,
s3control, s3tables, sagemaker, secretsmanager, securityhub, servicediscovery,
ses, sesv2, sns, sqs, ssm, ssoadmin, stepfunctions, transfer,
**vpclattice** (this session), **waf** (this session), **wafv2** (this
session), **workmail** (this session).

One service still has real, extensive wire-shape work under **other** issue
classes (gopherstack-h910/ctaz's backend-logic fixes) but **no 6flj-specific
wrapper-key pass on record** — dynamodb (s3 moved to swept this session; see
its own section at the end of this file). It is listed in the unswept table
below on purpose; don't assume "heavily worked on" means "settled for this
issue."

## Unswept (89 of 162), ranked by List+Describe+Get op count

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

Sum of the L+D+G column across all 89 (networkmanager's 38, securityhub's
47, macie2's 40, s3's 45, cognitoidp's 37, personalize's 39,
apigatewayv2's 37, workmail's 36, wafv2's 32, ce's 31, waf's 34, and
vpclattice's 30 removed, all swept prior to/this session): **1,155**
candidate ops.

| service | total ops | list | describe | get | L+D+G | resolution |
|---|---:|---:|---:|---:|---:|---|
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

## macie2 (this session)

Chosen as the largest genuinely-unswept service once s3 (flagged as needing
its own dedicated session) and personalize (its systemic List-vs-Get leak
already fixed under gopherstack-sm02, a different issue but the same bug
class, making a from-scratch 6flj pass on it likely low-yield) were set
aside. macie2: 40 L+D+G ops (15 List, 3 Describe, 22 Get), `direct`
resolution. `git status` showed a live sibling RouteMatcher sweep
(`cmd/routecollisions/`, `services/apigateway/handler.go`) and separate
in-progress `services/appconfigdata/`/`services/inspector2/` changes;
`services/macie2` itself was untouched, confirmed again at the end.

Protocol: restjson1, case-sensitive — confirmed via the sole
`awsRestjson1_` deserializer function prefix in
`macie2@v1.54.4/deserializers.go` and a check that all 503 `EqualFold` hits
in that file are `errorCode` header/query matching, none in a body-field
switch. Dead-deserializer trap checked and does NOT apply:
`HandleDeserialize` calls `awsRestjson1_deserializeOpDocument<Op>Output`
directly for every op spot-checked (e.g. `ListFindings`,
`GetBucketStatistics`) — no generated-but-unreachable wrapper layer exists
in this service's codegen.

Full layer-1+2 sweep of all 40 L+D+G ops plus every Create/Update op sharing
a response or request type with one of them (roughly 60 ops read against
the real deserializer/serializer, one at a time — no shared converter
function spans enough ops here to make a service-wide sweep faster than
per-op reads).

**2 real bugs found and fixed**, both the same variant this campaign calls
"a value the backend already holds that never reaches the wire, under a key
name no real client's field would ever match" — not missing wrapper keys,
but wrong scalar key names one level in:

1. `GetBucketStatistics`: `classifiableBucketCount` does not exist on the
   real `GetBucketStatisticsOutput` at all — real key is
   `classifiableObjectCount`, and it's a summed *object* count across
   buckets, not a count of buckets that have any classifiable objects (the
   pre-fix value was semantically a different number even before the key
   mismatch). Also added `objectCount`/`sizeInBytes`, real aggregate fields
   that were missing entirely despite the backend already tracking both
   per-bucket (`S3BucketMetadata.ObjectCount`/`SizeInBytes`, already
   correctly emitted by the per-item `DescribeBuckets` shape) and simply
   never being summed for the aggregate op. `lastUpdated`/
   `sizeInBytesCompressed`/`bucketStatisticsBySensitivity` remain disclosed,
   not fixed — no compression or sensitivity-scan tracking exists in this
   backend to source them from.
2. `GetResourceProfile`: `sensitivityScoreOverride` does not exist on the
   real `GetResourceProfileOutput` — real key is `sensitivityScoreOverridden`
   (past participle). `UpdateResourceProfile` genuinely sets this flag in
   the backend, so a real client's `SensitivityScoreOverridden` was always
   false regardless of whether an override had been applied. Also renamed
   two `ResourceStatistics` fields to match the real deserializer
   (`totalDetectionsWithoutSuppression`→`totalDetectionsSuppressed`,
   `totalItemsSkippedPermissionError`→`totalItemsSkippedPermissionDenied`)
   — `ResourceStatistics` is always the zero-value struct in this backend
   (nothing populates real numbers into it), so this half of the fix is
   disclosed as untested rather than given a hollow test, per this issue's
   own guidance.

**Sibling-trap check, reported clean**: `GetAdministratorAccount`/
`GetMasterAccount` both wrap the real shared `Invitation` type, whose
`relationshipStatus` field name genuinely IS correct for macie2 — confirmed
against `deserializers.go`'s `Invitation` case list. This is the same shape
of concept securityhub got wrong this campaign (`RelationshipStatus` vs
real `MemberStatus`), but it is a *different* real type in macie2's own
SDK, and macie2's version is right. No V1/V2 or other generational pairs
exist in this service.

**3 ratifying tests found and fixed**, all "wrong key/value asserted as
correct": `handler_buckets_test.go` (4 assertion sites across 3 tests
built around the pre-fix `classifiableBucketCount` key and its
bucket-counting semantic, including one table-driven test whose expected
values changed from "count of buckets" to "sum of objects") and
`handler_resource_profiles_test.go` (1 assertion site checking the pre-fix
`sensitivityScoreOverride` response key). Zero found in the
too-weak-to-fail shape.

**Phantom ops**: none — all 96 op consts in `handler.go` have a real
`api_op_*.go` in `macie2@v1.54.4`. **False-positive rate**: 0 — every
finding cites the real `deserializeOpDocument<Type>`/
`deserializeDocument<Type>` function actually reached from
`HandleDeserialize`, file+line, or the real `api_op_*.go`/`types.go`
struct definition when a field is absent from the generated switch
entirely (e.g. `AllowListSummary` has no `tags` member in the real type at
all).

**Harmless-extra-field non-bugs** confirmed (real client silently discards
unknown JSON keys, so left alone): `AllowListSummary.tags`,
`FindingsFilterListItem`'s extra `description`/`position`,
`Member.updatedAt`, `CreateClassificationJobOutput`'s extra `jobStatus`,
`AutomatedDiscoveryAccount`'s extra `email`, `GetResourceProfile`'s extra
`resourceArn`. **Structural/unmodeled gaps** disclosed, not fixed (would
require new backend simulation, not a key-name fix):
`Finding.policyDetails`, `ClassificationDetails.detailedResultsLocation`,
most of `AffectedS3Bucket`/`AffectedS3Object`'s real fields (versioning,
encryption detail, sensitivity score, ...),
`GetAutomatedDiscoveryConfiguration`'s
`classificationScopeId`/`disabledAt`/`firstEnabledAt`/`lastUpdatedAt`/
`sensitivityInspectionTemplateId`, `ResourceStatistics.totalItemsSensitive`,
and `ListResourceProfileArtifacts`'s always-empty result (already disclosed
in this service's own code comment) with its item shape's missing
`classificationResultStatus`/extra `type`.

Every fix hand-reverted individually (no git, per this session's hard
no-git-mutation constraint), confirmed to fail against a real SDK client
with the exact predicted symptom (0 instead of the seeded sums;
`SensitivityScoreOverridden` false instead of true), then restored and
diffed byte-identical against the pre-revert file before moving on. 2 new
real-`aws-sdk-go-v2`-client tests added in the new
`services/macie2/wire_field_fixes_test.go`
(`TestGetBucketStatistics_RealClient`,
`TestUpdateResourceProfile_SensitivityScoreOverridden_RealClient`).

Gates: `go build`/`go vet`/`go test -race` (scoped to `services/macie2`),
`go fix -diff` (no diff), `fieldalignment` (0 findings), `golangci-lint run`
(0 issues, no cyclop/gocyclo/gocognit/funlen nolints) all green. `go test
-race ./pkgs/...` green.

Per this session's hard constraints: no subagents used (Read/Grep/Bash
only), no git-mutating commands run (all changes uncommitted — orchestrator
must commit/push), `cmd/routecollisions/`/`services/apigateway/`/
`services/appconfigdata/`/`services/inspector2/` and their test files left
untouched (confirmed via `git status` before starting and again at the
end), no `gendocs`/`make docs` run.

macie2's List/Describe/Get families are now fully swept for this issue
(40/40 ops verified against the real deserializer/serializer, plus their
sibling Create/Update ops). 64 of 162 services swept, 98 remain. Per the
ranked table, s3 (45 L+D+G ops, `chased` resolution, flagged as needing its
own dedicated session) is next largest; personalize (39, likely
mostly-clean per gopherstack-sm02) or cognitoidp (37, `chased`) are the
next candidates that don't obviously collide with either live sibling
session observed this round — re-check `git status` before picking.

## s3 (this session)

The dedicated session five prior passes deferred this to. Read this file's
method section, `bd show gopherstack-6flj` (comments, not just the
63KB-saturated notes field), and `git show 1217df451` (macie2, the pass
immediately before this one) before starting.

`git status` at start showed the live RouteMatcher sweep
(`cmd/routecollisions/`, `services/_ROUTE_COLLISIONS.md`,
`services/apigateway/handler.go`, `test/integration/tag_routing_test.go`)
plus separate in-progress `services/appconfigdata/`/`services/inspector2/`
changes and a new untracked
`test/integration/apigateway_quicksight_account_test.go` — none touch
`services/s3`; confirmed untouched again at the end.

**s3 had FIVE prior passes under other issue classes (21 bugs, a
ListObjectsV2 allocation fix, two checksum fixes) but no 6flj-scoped
wrapper-key sweep on record** — checked `services/s3/PARITY.md` and
`git log -- services/s3` per this issue's "check, don't trust PARITY claims"
instruction; s3's own notes held up this time (unlike five other services'
this campaign found stale).

**PROTOCOL**: REST-XML, `awsRestxml_` the sole deserializer prefix in
s3@v1.106.5. Per this issue's own s3-specific threat-model note: zero
`GetElement` calls (established under gopherstack-7185, not re-derived) so
the empty-result-on-root-mismatch class is structurally absent; casing is
case-insensitive (`strings.EqualFold` throughout) so every finding below is
a genuinely different/absent element name, confirmed not a casing quirk.

Read the real `awsRestxml_deserializeOp<Op>.HandleDeserialize` (not just an
`OpDocument*Output` function name) for all 45 L+D+G ops (12 List, 0
Describe, 33 Get, per `s3CoreOperations()`/`s3ExtendedOperations()` in
`handler_operations.go` — the same 45 the ranked table already had, no
recount needed). Grouped by shape family per method detail in
`services/s3/PARITY.md`'s new 2026-08-15 section: raw-passthrough config
echoes (CORS/lifecycle/notification/website/encryption/logging/replication/
ownershipControls/publicAccessBlock/analytics/intelligent-tiering/inventory/
metrics/requestPayment/accelerate/policyStatus/abac — each individually
confirmed the real GET deserializer decodes the response root directly as
the same struct the PUT/Create payload root already is, not assumed by
pattern), simple flat-field Get ops (versioning/location/tagging/policy),
the List*Configurations family (double-nesting fix from 2026-07-24 already
carries a real structural-walk regression test, re-verified not re-tested),
List/Get for objects/versions/multipart/parts (Object/ObjectVersion/Part/
MultipartUpload mechanically diffed under gopherstack-3dqa 2026-08-14b,
re-verified via the same case lists), the object-lock family, and the
recently-implemented Object Annotations family (citations in
`object_ops_annotations.go` re-checked against the pinned SDK directly).

**2 real bugs found and fixed:**

1. `ListObjects`/`ListObjectsV2` — `Object.Owner` (real member, shared
   `awsRestxml_deserializeDocumentObject` case list) never emitted at all;
   the shared `ObjectXML` struct had no field for it. `ListObjectsInput` has
   no `FetchOwner` (V1 always includes Owner); `ListObjectsV2Input.FetchOwner`
   was already read into the backend input but never wired to anything (V2
   gates on it). A near-duplicate-shape pair where BOTH sides were broken
   the same way, not a "one got it right" case. Fixed: added
   `ObjectXML.Owner *Owner` and an `includeOwner bool` threaded through the
   shared `mapObjectsToXML` (`true` for V1, `fetch-owner` query param for
   V2).
2. `GetBucketVersioning`/`PutBucketVersioning` — `MFADelete` read from no
   request, stored nowhere, echoed by no response, despite sitting directly
   beside the already-correct `Status` case in
   `awsRestxml_deserializeOpDocumentGetBucketVersioningOutput`. Real
   request-side type (`types.VersioningConfiguration.MFADelete`) is
   `types.MFADelete`; real response-side type
   (`GetBucketVersioningOutput.MFADelete`) is the **different** Go type
   `types.MFADeleteStatus` — same wire strings, two distinct SDK enums, so
   `StoredBucket.MFADelete` is a plain string rather than coupled to either.
   Only emitted once ever configured (matches the real doc: "only returned
   if the bucket has been configured with MFA delete").

**1 severe finding, flagged and NOT fixed** — `GetBucketMetadataConfiguration`/
`GetBucketMetadataTableConfiguration` return the wrong response shape
entirely. Unlike every other config-echo op in this service, these two real
deserializers require a `MetadataConfigurationResult`/
`MetadataTableConfigurationResult` child element of server-*computed* fields
(table bucket ARN/namespace/provisioning status) that are structurally
absent from the client's CREATE request body gopherstack currently echoes
back verbatim — a real typed client's response fields decode to nil/zero
regardless of backend state, for both ops, today. The same
`OpDocument*Output`-with-a-matching-case dead-code trap gopherstack-ob1g
already found once on `GetBucketAbac` — found here by checking each
config-echo op's `HandleDeserialize` individually instead of extending the
pattern from the 17 ops that ARE genuine raw-passthrough. Not fixed: a real
fix needs an S3 Tables table-bucket provisioning model (ARN/namespace/status)
this backend has nowhere at all; fabricating plausible ARNs/status would be
invented data, the exact class this campaign exists to catch. Full detail
and citations in `services/s3/PARITY.md`'s new ops-table row and gaps entry.

**Sibling/near-duplicate shapes**: ListObjects vs ListObjectsV2 is the
clearest pair (see finding #1) — both broken identically, not a
one-correct-one-wrong case. No `AdministratorAccount`/`MasterAccount`-style
Invitation-type mixup exists in s3 (the shape securityhub and macie2 both
hit this campaign); no analogous shared-name-field pair found.

**Backend-held-but-unemitted values**: `Object.Owner` (finding #1) —
`gopherstackName` was already emitted correctly by ListBuckets, GetBucketAcl,
ListObjectVersions, and ListMultipartUploads, just never wired into the one
converter both List ops share. `MFADelete` was NOT an already-held value —
the backend tracked nothing for it before this pass; fixed by adding both
the storage slot and the wire threading together.

**Wrong-value check**: none found beyond the two missing-field fixes above.

**Casing near-misses**: none — REST-XML decodes case-insensitively
throughout; every finding was a genuinely absent/different element name.

**Ratifying tests**: none found needing correction — neither `Owner` nor
`MFADelete` had any prior assertion in either direction anywhere in this
service's existing suite (zero prior coverage, not a wrong assertion staying
green).

**Phantom ops**: none. Cross-referenced all 115 op-name literals from
`s3CoreOperations()`/`s3ExtendedOperations()` against s3@v1.106.5's
`api_op_*.go` files; every one real.

**False-positive rate**: 0 among reported bugs — every finding cites the
real `HandleDeserialize`/`deserializeDocument<Type>` function actually
reached, file+line, never a doc comment or an assumption extended by
pattern from a sibling op.

2 real-SDK-client tests added in the new `services/s3/wire_field_fixes_test.go`
(`TestListObjects_OwnerPopulated` — table-driven V1-always/
V2-default-omits/V2-fetch-owner-true; `TestBucketVersioning_MfaDeleteEcho`).
Every fix hand-reverted individually (no git, per this session's hard
no-git-mutation constraint): the `ObjectXML.Owner` field removal is a
compile error (proving it load-bearing at the type level, not just at
runtime); the V1/V2 wiring and both halves of the `MFADelete`
request/response threading each independently reverted to the exact
predicted runtime failure (nil `Owner` where non-nil expected; empty
`MFADelete` where `"Enabled"` expected), then restored and diffed
byte-identical against the pre-revert file before moving to the next.

Gates: `go build`/`go vet`/`go test -race` (scoped to `services/s3`), `go fix
-diff` (no diff), `fieldalignment` (0 findings), `golangci-lint run` (1
`goimports` formatting nit on the new struct-field comment, fixed with
`gofmt -w`; 0 issues after, no cyclop/gocyclo/gocognit/funlen nolints added)
all green. `go test -race ./pkgs/...` green.

Per this session's hard constraints: no subagents used (Read/Grep/Bash
only), no git-mutating commands run (all changes uncommitted — orchestrator
must commit/push), `cmd/routecollisions/`/`services/_ROUTE_COLLISIONS.md`/
`services/apigateway/handler.go`/`services/appconfigdata/`/
`services/inspector2/`/`test/integration/tag_routing_test.go`/
`test/integration/apigateway_quicksight_account_test.go` (the live sibling
RouteMatcher sweep's in-progress work) confirmed untouched via `git status`
both before starting and again at the end, no `gendocs`/`make docs` run.

s3's List/Describe/Get families are now fully swept for this issue (45/45
ops verified against the real deserializer, one finding flagged rather than
fixed — see `services/s3/PARITY.md` for the full writeup). 65 of 162
services swept, 97 remain. dynamodb is the one remaining service with
extensive wire-shape work under other issue classes but no 6flj-specific
pass; per the ranked table, personalize (39, likely mostly-clean per
gopherstack-sm02) or cognitoidp (37, `chased`) are the next candidates —
re-check `git status` before picking, given how often a sibling session has
appeared mid-flight this campaign.

## cognitoidp (this session)

Chosen per this session's assignment: largest unswept candidate not already
flagged as needing a dedicated session, with personalize's systemic
List-vs-Get leak already fixed under gopherstack-sm02 (a different issue,
same bug class, making a from-scratch pass there likely lower-yield).
cognitoidp: 129 total ops, ranked-table count 37 L+D+G (14 List/10
Describe/13 Get) via `chased` resolution. Own direct enumeration of
`baseSupportedOperations()`/`extendedSupportedOperations()` in
`handler.go` found more List/Describe/Get-prefixed ops than the table
(17 List, 10 Describe, 15 Get = 42) — the extra 5 are ops like
`GetTokensFromRefreshToken`/`GetUserAttributeVerificationCode` that return
non-collection shapes the ranked table's `chased` resolver evidently
didn't bucket the same way. All 42 by this session's own count were swept,
not just the table's 37. `git status` at start and end: clean except one
untracked `services/cloudwatchlogs/zzz_probe_test.go` from an unrelated
sibling session, confirmed untouched.

**PROTOCOL**: `awsAwsjson11_` (JSON-RPC 1.1) confirmed as the sole
deserializer function prefix in
`cognitoidentityprovider@v1.67.4/deserializers.go` (grep `awsAwsjson1[0-9]*_|awsRestjson1_|awsEc2query_|awsAwsquery_`
— only `awsAwsjson11_` present). Case-sensitive, like awsconfig/
cloudwatchlogs/macie2. All 1,129 `EqualFold` hits in the file are
`errorCode` matches inside `deserializeOpError*` functions (grepped for
`EqualFold` lines lacking `errorCode`: zero matches) — confirmed by
tracing `HandleDeserialize` for `AdminGetUser`/`DescribeUserPool`/
`GetUser`/`ListUsers` directly rather than trusting the pattern from a
single op. Body-field casing is therefore a real bug class here, per the
task brief's own framing — unlike query/XML services.

**Dead-deserializer trap checked and found NOT to apply**: traced
`(*awsAwsjson11_deserializeOpListUsers).HandleDeserialize` in full
(deserializers.go:12557) — it decodes the body into `shape` and calls
`awsAwsjson11_deserializeOpDocumentListUsersOutput(&output, shape)`
directly (line 12597), the same JSON-RPC 1.1 pattern already confirmed
non-dead in awsconfig/cloudwatchlogs/macie2. Trusted generally after
confirming for this op, not re-verified per-op.

**Dispatch-table override trap, specific to this service**: cognitoidp
registers most ops from an early ("A"/plain) map and a later
("B"/"Full"/"Accurate", `wrapAccuracy`-wrapped) map via 20+ sequential
`maps.Copy(table, ...)` calls in `dispatchTable()` (handler.go:336-375);
the later call wins on key collision. Several op families have BOTH a
plain handler (older, less complete struct — e.g. `identityProviderType`,
`resourceServerType`, `riskConfigurationType`) and a "Full"/"Accurate"
handler (newer, correct struct — `identityProviderJSON`,
`resourceServerAccurateType`, `riskConfigurationJSON`) defined side by
side, with the plain one dead code once the "Full" one is registered
later in `dispatchTable()`. This looks exactly like the generational
sibling-trap variant on first read (stale struct missing fields) but
isn't one in practice, because the stale struct is never reached —
**confirmed live registration for every op checked by reading
`dispatchTable()`'s call order directly**, not by assuming the "Full"
name always wins. Affected families checked and confirmed correctly
live-wired to the "Full"/"Accurate" struct: `DescribeUserPool`,
`GetUserPoolMfaConfig`, identity providers (Create/Update/Describe/
GetByIdentifier/List), resource servers (Create/Update/Describe/List/
Delete), `DescribeRiskConfiguration`, `GetUICustomization`,
`CreateUserPoolDomain`/`UpdateUserPoolDomain` (but not
`DescribeUserPoolDomain`, which has no "Full" override and stays on the
plain handler — checked and correct as-is), and groups
(Create/Update/Get/List/ListUsersInGroup, but not
`AdminListGroupsForUser`/`DeleteGroup`/`AdminAddUserToGroup`/
`AdminRemoveUserFromGroup`, which have no override).

Read all 42 self-enumerated L+D+G ops' response shapes against their own
`awsAwsjson11_deserializeOpDocument<Op>Output` case list (dumped via a
Python script walking brace-depth per function, file+line implicit), then
diffed the live (per dispatch-table-order) gopherstack struct's JSON tags
field-for-field against every shared nested type reached from those case
lists (`UserType`, `AdminGetUserOutput`, `DeviceType`,
`ProviderDescription`, `UICustomizationType`, `DomainDescriptionType`,
`ClientSecretDescriptorType`, `UserPoolClientDescription`, and more).

**2 real bugs found and fixed, one of them security-relevant:**

1. **`ListUserPoolClients` — wrong per-item shape, leaking `ClientSecret`
   and full OAuth configuration.** The real op's per-item type is
   `types.UserPoolClientDescription` — three fields only (`ClientId`,
   `ClientName`, `UserPoolId`), confirmed at types.go:2514 and the real
   deserializer's own case list (`awsAwsjson11_deserializeOpDocumentListUserPoolClientsOutput`,
   deserializers.go:32248). gopherstack instead reused the full
   `clientDataAccurate` struct (used correctly elsewhere for
   Describe/Create/Update) for every list item, including `ClientSecret`
   in plaintext. A real typed SDK client can't observe the leak (its own
   `UserPoolClientDescription` struct has no field to decode it into,
   same "harmless to a real client" class as other over-emission
   findings this campaign), but the **raw wire body** carried the secret
   value to any caller inspecting the JSON directly — the kind of gap
   this issue exists to catch even when a typed client happens to mask
   it. Fixed by adding a new `userPoolClientSummaryJSON` type mirroring
   the real 3-field shape and changing `handleListUserPoolClientsAccurate`
   to emit it instead of `clientDataAccurate`.
2. **`MFAOptions` never emitted on `ListUsers`/`ListUsersInGroup` —
   backend-tracked-but-unemitted, on two ops via two separate converter
   functions.** `UserType.MFAOptions` (types.go:3161, shared by both
   ops' `Users` list) is a real, non-deprecated member — unlike
   `GetUser`/`AdminGetUserOutput.MFAOptions`, which AWS's own doc comment
   marks "no longer supported... use UserMFASettingList instead"
   (api_op_GetUser.go/api_op_AdminGetUser.go), checked and confirmed
   correctly NOT fixed on those two ops for that reason (a real AWS
   backend wouldn't populate it there either). The backend already
   tracks `User.MFAOptions` (set via `SetUserSettings`/
   `AdminSetUserSettings`, `mfa.go:351,366`) and there was already a
   correctly-tagged wire type for the request side
   (`mfaOptionType{DeliveryMedium,AttributeName}`, models_mfa.go:179) —
   simply never read back out on the List side. `toUserSummary`
   (ListUsers) and `toAdminUserJSON` (ListUsersInGroup, and
   AdminCreateUser's response, which shares the same real `UserType`
   shape) both omitted it. Fixed by adding `MFAOptions` to both
   `userSummary` and `adminUserJSON`, and a `toMFAOptionsWire` helper
   reusing the existing request-side wire type via direct struct
   conversion (identical field layout).

**Sibling/near-duplicate shapes checked, reported clean or already
correctly resolved by dispatch order**: `GetUser` vs `AdminGetUser`
(genuinely different real response shapes — `GetUserOutput` has no
`UserStatus`/dates/`Enabled` at all, confirmed field-for-field, both
correctly minimal); `ListDevices` vs `AdminListDevices` and `GetDevice`
vs `AdminGetDevice` (share one `deviceType` struct, all four fields match
the real `DeviceType`, including a harmless extra `DeviceStatus` field —
real `DeviceType` (types.go:677) has no such member at all, a pure
carryover, harmless since real clients have no field to read it into,
same non-bug class as rds's `StorageOptimized`); identity providers,
resource servers, groups, `DescribeRiskConfiguration`/
`GetUICustomization` (all resolved via the dispatch-table-override trap
above, not independently broken); `AdminGetUserAuthFactors`/
`GetUserAuthFactors` (share the real response shape exactly, both
correct, including the real, easy-to-miss `Username` echo member on both).

**Request side**: spot-checked `AdminCreateUser` (missing real
`ClientMetadata`/`ValidationData` members — Lambda-trigger-context-only
fields with no threading concept in this backend at all, disclosed not
fixed, not a rename), `ListUserPoolClients`/`ListResourceServers`/
`AdminListGroupsForUser`'s pagination request fields (see disclosed
below), and the identity-provider/resource-server Create/Update request
bodies as part of the dispatch-order check above — no additional
request-only key-name bugs found beyond what's listed.

**Wrong-value check**: none found — every finding in this batch was a
missing/over-emitted field or shape, not a same-key-wrong-value bug.

**Casing near-misses**: none found distinct from the two fixes above —
every wrapper key checked matched the real deserializer's case list
exactly (JSON-RPC 1.1's case-sensitivity was confirmed structurally
important per the protocol note, but no live near-miss materialized;
this service's key names were already written in the correct case
throughout).

**Ratifying tests**: none found needing correction. Grepped
`user_pool_clients_handler_test.go`/`user_pool_clients_test.go` for
`ListUserPoolClients` assertions: both existing tests assert only `Len`
and `ClientName`, neither previously asserted (nor now needs to change
for) the shape fix. Grepped for `MFAOptions` in every `*_test.go`: zero
prior assertions in either direction on the List side — never exercised,
not a wrong assertion staying green.

**Phantom ops**: none. Extracted all 129 op-name string literals from
`baseSupportedOperations()`/`extendedSupportedOperations()` and confirmed
an `api_op_<Name>.go` file exists for every one in
cognitoidentityprovider@v1.67.4.

**False-positive rate**: 0 among reported bugs — every finding cites the
real `deserializeOpDocument<Type>Output`/`deserializeDocument<Type>`
function's own case list or the real `types.go`/`api_op_*.go` struct
definition, file+line, confirmed via the actual live dispatch-table
registration (not assumed from a handler function's name).

**Disclosed, not fixed** (structural gaps needing new backend modeling,
or fields AWS itself has deprecated — none silently drop data the backend
already tracks):
- `GetUserPoolMfaConfig`'s `WebAuthnConfiguration` (real, non-required
  member) — no WebAuthn relying-party configuration concept exists
  anywhere in this backend's user-pool MFA model (only per-user
  `WebAuthnCredential`s are tracked, a different real type).
- `GetUICustomization`'s `CSSVersion` (real member) — no CSS-versioning
  concept tracked on `UICustomization`.
- `DescribeUserPoolDomain`'s `Routing` (real member, a newer
  regional-endpoint-routing feature) — no domain-routing-rules concept
  tracked on `UserPoolDomain`.
- `AdminListGroupsForUser` has no `Limit`/`NextToken` pagination at all
  (real op supports both), unlike its sibling `ListGroups`/
  `ListUsersInGroup`, which already correctly paginate via
  `ListGroupsPage`/`ListUsersInGroupPage` backend methods. A real
  client's `Limit` request field is silently ignored (all groups
  returned in one page) rather than honored-with-truncation. Flagged as
  a genuine sibling-trap-shaped gap, not fixed: this backend has no
  existing paginated-lookup-by-user variant to mirror, and adding one is
  new backend surface, not a rename.
- `ListUserPoolClients`/`ListUserPoolClientSecrets` real ops also both
  echo `NextToken` at the top level (confirmed in both real deserializer
  case lists); neither gopherstack struct has the field. Consistent with
  this campaign's established "no truncation model, NextToken would be
  empty either way" non-bug precedent elsewhere (rds, securityhub) since
  neither handler truncates — not fixed, noted for completeness.

3 real-SDK-client tests added in the new
`services/cognitoidp/wire_field_fixes_test.go`
(`TestListUserPoolClients_SummaryShape` — SDK-typed assertions plus a
raw-body check proving no `ClientSecret`/`AllowedOAuthFlows` key reaches
the wire at all; `TestListUsers_MFAOptionsPopulated`;
`TestListUsersInGroup_MFAOptionsPopulated`). Every fix hand-reverted
individually (no git, per this session's hard no-git-mutation
constraint): the `ListUserPoolClients` struct-type revert is a compile
error (proving the shape change load-bearing at the type level, not just
runtime), and separately reverting only the handler while keeping the
new struct reproduced the exact predicted runtime leak (`ClientSecret`
present verbatim in the raw JSON body, quoted in the actual test failure
output); both `MFAOptions` reverts (`toUserSummary` and `toAdminUserJSON`
independently) reproduced the exact predicted empty-slice failure. All
four reverts restored and diffed byte-identical against the pre-revert
files before moving on.

Gates: `go build`/`go vet`/`go test -race` (scoped to
`services/cognitoidp`), `go fix -diff` (no diff), `golangci-lint run` (0
issues, fieldalignment included via govet settings; no cyclop/gocyclo/
gocognit/funlen nolints added) all green. `go test -race ./pkgs/...`
green.

Per this session's hard constraints: no subagents used (Read/Grep/Bash
only), no git-mutating commands run (all changes uncommitted —
orchestrator must commit/push), `services/cloudwatchlogs/zzz_probe_test.go`
(an unrelated sibling session's untracked file, present at both start and
end of this session) confirmed untouched, no `gendocs`/`make docs` run.

cognitoidp's List/Describe/Get families are now swept for this issue
(42/42 self-enumerated ops verified against the real deserializer/live
dispatch registration; layer 1 exhaustive, layer 2/3 covers every major
shared type but not every opaque-blob field inside branding/auth-flow
payloads — see disclosed list above for what's known-incomplete rather
than silently assumed clean). 66 of 162 services swept, 96 remain. Per
the ranked table, personalize (39, likely mostly-clean per
gopherstack-sm02) is now the largest candidate without a live-sibling
collision observed this session; re-check `git status` before picking.

## personalize (this session)

Chosen as the largest unswept service per the ranked table (39 L+D+G: 18
List/18 Describe/3 Get, `dynamic-fallback` resolution). `git status` at
start showed the repo clean except 5 untracked host-prefix-reachability test
files under `services/{cloudwatchlogs,lakeformation,mwaa,servicediscovery,
stepfunctions}/` — a live sibling session's assigned territory per this
session's own instructions, none of it touching personalize; left alone,
confirmed untouched again at the end. Own enumeration of `buildOps()`'s
literal map (a flat `map[string]opFunc` built in one function, no
per-family helper indirection) confirms the table's 39 exactly: 18 List +
18 Describe + 3 Get (GetSolutionMetrics, GetRecommendations,
GetPersonalizedRanking).

personalize was flagged by the prior session's note as "likely mostly-clean"
because gopherstack-sm02 (`de3ccfb36`) already did a careful, well-cited
List-vs-Get rescoping pass across all sixteen collection ops — a **different**
bug class from this issue's own (over-wide response leaking Get-only fields,
not a wrong wire key), but the fix was thorough enough that it also happened
to get every wrapper key and per-item field name right except one. That
prediction held: this was the cleanest large service swept in this campaign
so far by finding count, but not empty.

**PROTOCOL**: `awsAwsjson11_` (JSON-RPC 1.1) confirmed as the sole
deserializer-function prefix in personalize@v1.50.4/deserializers.go (247
`EqualFold` hits total, all either `errorCode` matches or `NaN`/`Infinity`/
`-Infinity` float-parsing branches inside numeric-field decode — zero in a
body-field `switch key { case "...": }` block, confirmed by grepping
`EqualFold` lines lacking `errorCode` and inspecting each of the 24 remaining
hits by hand). Case-sensitive, same distribution as awsconfig/
cloudwatchlogs/macie2/cognitoidp. `handleRuntimeREST`'s two ops
(`GetRecommendations`/`GetPersonalizedRanking`) are dispatched separately —
real `personalizeruntime@v1.36.4` is a *different*, restjson1 SDK client with
no `X-Amz-Target` header at all (`personalizeRuntimeRecommendationsPath`/
`personalizeRuntimeRankingPath`, fixed prior to this session under
gopherstack-92ft) — confirmed restjson1's own body-field switches are also
case-sensitive plain `case "...":` with zero relevant `EqualFold` hits.

**Dead-deserializer trap checked and found NOT to apply, either protocol**:
traced `(*awsAwsjson11_deserializeOpListSolutions).HandleDeserialize`
(deserializers.go:6628) for the classic JSON-RPC service — it decodes the
body into `shape` and calls
`awsAwsjson11_deserializeOpDocumentListSolutionsOutput(&output, shape)`
directly, same pattern as every other awsAwsjson11_ service this campaign has
checked. Also traced `(*awsRestjson1_deserializeOpGetRecommendations
).HandleDeserialize` (personalizeruntime@v1.36.4/deserializers.go:358) for
the runtime client — same direct-call pattern as guardduty/networkmanager's
restjson1, not pinpoint's dead-wrapper shape.

Read all 39 L+D+G ops' response shapes against their own
`awsAwsjson11_deserializeOpDocument<Op>Output` case list (dumped per-op via
awk), plus every List op's per-item `<Type>Summary` deserializer and every
Describe op's full `<Type>` deserializer, field-for-field against
`handler_*.go`'s `*ToMap`/`*SummaryToMap` converters — the same layer-1+2
pass this campaign has run on every other service, extended here to also
verify sm02's already-fixed Summary converters didn't introduce a new
key-name mismatch while rescoping fields (they didn't, except the one bug
below, which sm02 didn't touch — `ListFilters`' top-level key, not a `Filters`-Summary
field).

**2 real bugs found and fixed:**

1. **`ListFilters` — wrong top-level wrapper key, sibling trap, flagship
   silent-empty shape.** Real key is `Filters` (PascalCase) — confirmed at
   `awsAwsjson11_deserializeOpDocumentListFiltersOutput`, `case "Filters":`,
   and independently in `api_op_ListFilters.go`'s
   `ListFiltersOutput.Filters` field. gopherstack emitted `"filters"`
   (lowercase) — the **only** PascalCase top-level wrapper key in this
   entire service; every sibling List op (`ListDatasetGroups`/
   `ListDatasets`/`ListSolutions`/`ListCampaigns`/`ListEventTrackers`/...)
   genuinely uses lowerCamelCase, confirmed per-op via the same awk dump.
   Case-sensitive JSON-RPC 1.1 decode means a real client's typed
   `ListFiltersOutput.Filters` was always empty regardless of backend
   state — the same bug class as awsconfig's `ListDiscoveredResources` and
   cloudwatchlogs's `DescribeImportTasks`, just inverted (one PascalCase
   outlier among lowercase siblings instead of one lowercase outlier among
   PascalCase siblings). Fixed in `handler_filters.go`'s `listFilters`.
2. **`DescribeEventTracker` — backend-tracked-but-unemitted (layer 3),
   this session's instance of the lead-question-2 pattern.** Real
   `EventTracker.AccountId` ("The Amazon Web Services account that owns the
   event tracker", types.go:1224-1227) was never emitted by
   `eventTrackerToMap`, even though the backend already has the exact value
   on hand — `InMemoryBackend.accountID`, the same field used to build every
   ARN this service returns (`personalizeARN`, store.go). No accessor
   existed to read it from a handler file, so one was added
   (`Backend.AccountID()`, mirroring the existing `Backend.Region()`) and
   threaded through `describeEventTracker` into `eventTrackerToMap`. Not
   present on `EventTrackerSummary` (`ListEventTrackers`' item type) —
   confirmed absent from that type's own deserializer case list, so the List
   side correctly stays as-is.

**Sibling-trap / generational-pair check**: no V1/V2 or plain/wrapped pairs
exist in this service (unlike cognitoidp/securityhub) — every resource
family has exactly one Create/Describe/List/Update/Delete set. The one
sibling trap found (`ListFilters`) is a same-service, cross-op casing
outlier, not a versioned pair.

**Request side**: spot-checked every Create/Update op's largest bodies
(`CreateSolution`, `CreateSolutionVersion`, `CreateCampaign`,
`CreateRecommender`, `CreateBatchInferenceJob`, `CreateBatchSegmentJob`,
`CreateDataDeletionJob`, `CreateMetricAttribution`) against their real
`serializeOpDocument<Op>Input` field lists — all already correct, including
`UpdateSolution`'s deliberate omission of `PerformAutoML`/`PerformHPO`
(create-only fields, correctly not accepted on update, confirmed against
the real `UpdateSolutionInput`, which has neither member). No request-side
bugs found this session — the total-outage class this issue calls out
(required-field read under the wrong key) does not appear anywhere in this
service; every op's read keys matched the real request struct's tags.

**Grep for discarded parameters / write-only fields**: no `_ bool`/`_
string`-style discarded backend parameters found (unlike networkmanager's
`UseMiddleboxes`). `FailureReason` (tracked only on `DatasetGroup` and
`SolutionVersion` per `models.go`) is already conditionally emitted on both;
every other Summary/full type missing `failureReason` does so because the
backend genuinely has no such field on that resource's model — each such gap
already carries its own pre-existing citing comment
(`datasetGroupSummaryToMap`/`campaignSummaryToMap`/`filterSummaryToMap`/etc.
all explicitly say so), independently spot-checked against `models.go` and
confirmed accurate rather than trusted at face value.

**Over-wide-field / credential check**: none found. No List item anywhere in
this service reuses a full Get-scoped converter (that's exactly what sm02
already fixed), and no Summary/full type in this service has anything
resembling a secret, token, or credential field — personalize has no
API-key/secret-bearing resource type at all.

**Ratifying tests found and fixed: 1** —
`handler_list_summary_test.go`'s table-driven `TestPersonalize_ListOps_
SummaryShape` (the sm02-era test) called `listSingle(t, h, "ListFilters",
"filters")`, asserting the wrong (lowercase) key as correct; both the
handler and the test agreed on the bug, so it passed cleanly against broken
code. Fixed to `"Filters"`. **Zero** found in the other two ratifying-test
shapes this issue tracks (wrong value; assertion too weak to fail) — grepped
every existing `*_test.go` assertion touching `Filters`/`filters` and
`accountId`/`AccountId`: no other test in either shape references either
field.

**Phantom ops**: none — `sdk_completeness_test.go`'s
`TestSDKCompleteness` already cross-checks every op name in
`GetSupportedOperations()` against the real `personalizesdk.Client`/
`personalizeruntimesdk.Client` method sets (split by the `runtimeOps` map
since gopherstack's single Handler serves both real SDKs), and passed before
and after this session's changes — confirmed as the existing mechanism
rather than re-derived by hand.

**False-positive rate**: 0 among reported bugs — both findings cite the
real `deserializeOpDocument<Type>Output`/`deserializeDocument<Type>` case
list or `types.go` struct definition, file+line, never a doc comment.

Every fix hand-reverted individually (no git, per this session's hard
no-git-mutation constraint), confirmed to fail with the exact predicted
symptom — `require.Len(t, out.Filters, 1)` failing with "`[]` should have 1
item(s), but has 0" for the `ListFilters` key revert, and
`assert.Equal(t, "000000000000", ...)` failing with an empty-string actual
for the `DescribeEventTracker` revert — then restored and diffed
byte-identical against the pre-revert file before moving to the next.

2 real-SDK-client tests added in `services/personalize/wire_field_fixes_test.go`
(`TestListFilters_RealSDKClient`, `TestDescribeEventTracker_AccountID`),
plus a new `newTestPersonalizeClient` helper (the classic JSON-RPC control-
plane client) mirroring the existing `newTestPersonalizeRuntimeClient`
pattern already in `handler_runtime_real_client_test.go` for the separate
restjson1 runtime client.

Gates: `go build`/`go vet`/`go test -race` (scoped to
`services/personalize`), `go fix -diff` (no diff), `fieldalignment` (0
findings), `golangci-lint run` (0 issues, no cyclop/gocyclo/gocognit/funlen
nolints added or present) all green. `go test -race ./pkgs/...` green.

Per this session's hard constraints: no subagents used (Read/Grep/Bash
only), no git-mutating commands run (all `services/personalize` changes
uncommitted — orchestrator must commit/push), the 5 host-prefix-
reachability sibling files confirmed untouched via `git status` both before
starting and again at the end. `.beads/issues.jsonl` appeared staged after
running read-only `bd show`/`bd prime`-style commands — bd's own
auto-export hook, not a manual `git add`; left as-is since undoing it would
itself require a git-mutating command this session may not run. No
`gendocs`/`make docs` run.

personalize's List/Describe/Get families are now fully swept for this issue
(39/39 ops verified against the real deserializer/serializer, both the
classic JSON-RPC control plane and the restjson1 runtime client). 67 of 162
services swept, 95 remain. Per the ranked table, apigatewayv2 (37, `direct`
resolution) is the next largest unswept candidate — re-check `git status`
for live sibling territory before picking.

## apigatewayv2 (this session)

Chosen as the next-largest unswept candidate per the ranked table (37
L+D+G: 5 List/0 Describe/32 Get). `git status` at start showed a live
sibling session mid-flight on `services/personalize` (4 modified files plus
an untracked `wire_field_fixes_test.go`) — avoided entirely, never touched.
Own enumeration of `GetSupportedOperations`' literal slice (handler.go:133,
`direct` resolution, no chasing needed) confirms 37 L+D+G ops exactly
matching the table: 32 `Get*`, 5 `List*` (routing rules, portals, portal
products, product pages, product REST endpoint pages), 0 `Describe*`.

**PROTOCOL**: `awsRestjson1_` confirmed as the sole prefix in
apigatewayv2@v1.37.4's `deserializers.go` (grep for
`awsRestjson1_|awsAwsjson1[01]_|awsEc2query_|awsAwsquery_` found only the
first). Case-sensitive. Of 337 `EqualFold` hits, exactly 3 lack `errorCode`
on the same line, and all three are `case strings.EqualFold(jtv, "NaN"|
"Infinity"|"-Infinity"):` inside numeric-field decode branches
(deserializers.go:22828-22834) — body-field casing is a non-issue by
construction, same shape as every other restjson1 service this issue has
swept so far.

**Dead-deserializer trap checked and found NOT to apply**: traced
`(*awsRestjson1_deserializeOpGetApis).HandleDeserialize` in full
(deserializers.go:6984) — it decodes the body into `shape` and calls
`awsRestjson1_deserializeOpDocumentGetApisOutput(&output, shape)` directly
(line 7020); no dead `OpDocument...Output` wrapper sits between them, the
same pattern guardduty/networkmanager/securityhub already confirmed for
restjson1 in this codebase.

Dumped every op's own `awsRestjson1_deserializeOpDocument<Op>Output` case
list via a per-op awk script (file+line implicit) for all 5 List ops plus
the 12 collection-returning `Get*` ops, and every major shared nested type
(API, Stage, Route, Integration, Deployment, Authorizer, DomainName,
APIMapping, IntegrationResponse, Model, VpcLink, RouteResponse, RoutingRule,
Portal/PortalSummary, PortalProduct/PortalProductSummary,
ProductPage/ProductPageSummaryNoBody,
ProductRestEndpointPage/ProductRestEndpointPageSummaryNoBody) against
`types.go`.

**6 real bugs found and fixed**, spanning several of this issue's variants:

1. **`ListRoutingRules` — wrapper key, flagship pattern, the one true
   layer-1 finding in this service.** Every other List/Get collection op in
   this service wraps its items under `"items"`; `ListRoutingRulesOutput`
   alone uses `"routingRules"` (confirmed at
   apigatewayv2@v1.37.4's api_op_ListRoutingRules.go:56 and
   deserializers.go's `awsRestjson1_deserializeOpDocumentListRoutingRulesOutput`
   case list — `case "routingRules":`, no `"items"` case at all).
   gopherstack's `listRoutingRulesOutput` reused the same `Items
   []RoutingRule json:"items"` shape as every sibling. A real client's typed
   `.RoutingRules` field was always empty regardless of backend state. Zero
   prior test coverage of any kind on this op's handler response shape (only
   backend-level tests existed). Fixed by renaming the field/tag to
   `RoutingRules json:"routingRules"`.
2. **`Portal.PublishStatus` — wrong key AND wrong semantic together, three
   bugs stacked on one field.** gopherstack emitted the portal's publish
   lifecycle under `"status"`; the real `GetPortalOutput`/`PortalSummary`
   member is `"publishStatus"` (types.PublishStatus, six-value enum:
   PUBLISHED/PUBLISH_IN_PROGRESS/PUBLISH_FAILED/DISABLE_IN_PROGRESS/
   DISABLE_FAILED/DISABLED). Two more bugs riding along: (a) `CreatePortal`
   seeded every new portal with `"ACTIVE"` — a value that exists nowhere in
   the real enum, invented outright; fixed by leaving it unset (omitted)
   until first published/disabled, since nothing in the real enum
   represents "never published" either. (b) gopherstack's own
   `UpdatePortalInput` (the real op has no such member at all,
   confirmed against api_op_UpdatePortal.go) exposed `Status` on the
   wire-decoded PATCH body — any real client could set publish state through
   a plain UpdatePortal call, which the real API doesn't allow; fixed by
   tagging it `json:"-"` (kept as an internal-only Go field for
   handlePublishPortal/handleDisablePortal to pass through the same
   `UpdatePortal` backend method). A ratifying test
   (`TestHandler_CreatePortal`) explicitly asserted `"ACTIVE"` as the
   correct value — rewritten to assert the field is empty on creation.
3. **`Portal.LastModified`/`PortalProduct.LastModified` — backend never
   tracked at all, a sibling trap against this service's own
   `ProductPage`/`ProductRestEndpointPage`, which already track and emit
   `LastModified` correctly via the identical `isoTime`-at-create/update
   idiom three structs away in the same file.** Real, required
   `PortalSummary`/`PortalProductSummary` members
   (aws-sdk-go-v2/service/apigatewayv2@v1.37.4's types.go), also present
   (non-required) on `GetPortalOutput`/`GetPortalProductOutput`. Fixed by
   mirroring the existing sibling pattern onto both structs and their
   Create/Update backend methods.
4. **`CreateProductPageInput.DisplayContent` — request side, total data
   loss on every call, the highest-severity finding this session.** Real
   `CreateProductPageInput.DisplayContent` (`*types.DisplayContent{Body,
   Title}`) is **required** on every real `CreateProductPage` call
   (api_op_CreateProductPage.go). gopherstack's `CreateProductPageInput` had
   no field for it at all, and the backend method's own signature discarded
   the whole input with `_ CreateProductPageInput` — every product page was
   created empty regardless of what a real client sent, and the field could
   never be set at all (`UpdateProductPage` was the only way to populate
   it). Fixed by adding the field (opaque `map[string]any` passthrough,
   matching the treatment `ProductPage.DisplayContent` already uses) and
   wiring it through `CreateProductPage`.
5. **`CreateProductRestEndpointPageInput.DisplayContent` — same shape,
   optional field this time, and a genuine same-service sibling trap:
   `UpdateProductRestEndpointPage` already accepts and stores this exact
   field correctly on `ProductRestEndpointPage.DisplayContent`three
   functions away; `CreateProductRestEndpointPage` never did.** Real,
   optional `CreateProductRestEndpointPageInput.DisplayContent`
   (`*types.EndpointDisplayContent`, api_op_CreateProductRestEndpointPage.go).
   Fixed the same way as #4.

**Value the backend already holds that never reached the wire**: none beyond
#3 above (LastModified was tracked nowhere for Portal/PortalProduct, so this
is more "never tracked" than "tracked but unwired" — the closer parallel to
this issue's usual "one field away" pattern is #4/#5, where
`ProductPage`/`ProductRestEndpointPage.DisplayContent` already existed as a
struct field and was already correctly read back by Get/List/Update, just
never accepted on Create).

**Over-wide field / secret check**: none found. Checked every
Authorizer/DomainName/VpcLink field for anything credential-shaped
(`AuthorizerCredentialsArn`, mutual-TLS truststore fields, VPC link security
group/subnet IDs) — all are ARNs/IDs a caller already owns or supplied
themselves, not secrets a caller couldn't otherwise see. **`Portal.LogoURI`
is emitted on every Get/List response but has no real backing member on
`GetPortalOutput`/`PortalSummary` at all** (real `LogoUri` exists only on
the `CreatePortalInput`/`UpdatePortalInput` request side) — a fabricated
response field, but harmless: a real typed client has no field to decode it
into, and it carries no secret or unauthorized data (same non-bug class as
rds's previously-disclosed `StorageOptimized`). Not removed, per this
campaign's established precedent that pulling a field a client could still
be reading via raw JSON isn't a parity improvement.

**Sibling/version pairs checked**: no V1/V2 pairs exist in this service
(that's cognitoidp/securityhub's shape). The real sibling-trap shape here
was intra-service Create/Update asymmetry (#5) and cross-struct field-parity
gaps (#3) rather than a duplicated type pair. No dispatch-registration
traps: `GetSupportedOperations` returns one flat literal slice
(handler.go:133), no `maps.Copy`-family override pattern like cognitoidp's.

**Request side**: checked as part of every fix above (#2's request-side
fabricated field, #4/#5's request-side data loss). No additional
request-only asymmetry found spot-checking the largest Create inputs
(CreateApi, CreateAuthorizer, CreateIntegration, CreateDomainName,
CreateVpcLink) against their real `serializeOpDocument<Op>Input` functions —
all clean.

**Ratifying tests**: 1 found and fixed — `TestHandler_CreatePortal` asserted
`portal.Status == "ACTIVE"` by unmarshaling into gopherstack's own `Portal`
struct (not the real SDK type), so it only proved internal
handler/model self-consistency, not real-AWS shape compliance; passed
cleanly against every bug in #2 simultaneously. Rewritten to assert
`PublishStatus` is empty and `LastModified` is set.

**Tests are NOT exercising a real client for most of this service**: only 3
of 36 test files (`handler_create_tags_test.go`,
`handler_export_api_sdk_test.go`, `sdk_completeness_test.go`) import the real
`aws-sdk-go-v2/service/apigatewayv2` client at all; the other 33 build raw
HTTP requests and unmarshal into gopherstack's own hand-defined structs.
None of the 3 real-client files touched `ListRoutingRules`, `Portal`, or
either product-page Create op before this session — all 5 fixes above had
zero real-client coverage.

**Phantom ops**: none found. All 37 L+D+G op-name string literals in
`GetSupportedOperations` correspond to a real `api_op_*.go` file in
apigatewayv2@v1.37.4 (spot-checked the full 92-op literal slice, not just
the L+D+G subset).

**False-positive rate**: 0 among reported bugs — every finding cites the
real `deserializeOpDocument<Type>Output`/`api_op_*.go` struct definition,
file+line, confirmed reached from that op's own `HandleDeserialize`, never a
doc comment.

6 real-SDK-client tests added in
`services/apigatewayv2/wire_field_fixes_test.go`
(`TestListRoutingRules_WireKey`, `TestPortal_PublishStatusWireKeyAndLifecycle`,
`TestPortalProduct_LastModified`, `TestCreateProductPage_DisplayContent`
drive the real typed SDK client; `TestCreateProductRestEndpointPage_DisplayContent`
drives raw HTTP against gopherstack's own types deliberately — the real
`CreateProductRestEndpointPageInput.DisplayContent` request type
(`*types.EndpointDisplayContent`) and `GetProductRestEndpointPageOutput.DisplayContent`
response type (`*types.EndpointDisplayContentResponse`) are genuinely
different shapes, and gopherstack stores/echoes both as an opaque
`map[string]any` passthrough — the same simplification
`UpdateProductRestEndpointPage` already uses, matched here for parity
between the two ops rather than fought with a mismatched typed assertion).
Every fix hand-reverted individually (no git, per this session's hard
no-git-mutation constraint), confirmed to fail with the exact predicted
symptom (`out.RoutingRules` empty-slice-length assertion; `PUBLISHED`
vs `""` on `GetPortalOutput.PublishStatus`; nil `LastModified` on both
structs; nil `.DisplayContent` on `CreateProductPageOutput`; nil map key on
the raw REST-endpoint-page JSON), then restored and diffed byte-identical
against the pre-revert file before moving to the next.

**Disclosed, not fixed** (real gaps needing new backend modeling this
session judged too speculative to fabricate):
- `PortalSummary`/`GetPortalOutput`'s `IncludedPortalProductArns`,
  `PublishStatus`'s non-DISABLED/PUBLISHED transitional states
  (`*_IN_PROGRESS`/`*_FAILED`), `LastPublished`/`LastPublishedDescription`,
  `Preview`, `RumAppMonitorName`, `StatusException` — this backend has zero
  concept of portal-product association or a publish pipeline beyond the
  binary published/disabled toggle already fixed in #2; gopherstack's own
  `CreatePortalInput`/`UpdatePortalInput` don't even accept
  `IncludedPortalProductArns` from a real client, so there's nothing to
  round-trip yet.
- `GetPortalProductOutput.DisplayOrder` (`*types.DisplayOrder{Contents
  []Section, OverviewPageArn, ProductPageArns}`) — real, accepted on both
  Create/Update requests, but a nested multi-field type with no existing
  backend concept to source it from; not modeled.
- `ProductRestEndpointPageSummaryNoBody`'s `Endpoint`/`Status`/`TryItState`/
  `OperationName`/`StatusException` — `ListProductRestEndpointPages` reuses
  the full `ProductRestEndpointPage` struct rather than the real narrower
  summary shape, but since unknown JSON fields are silently ignored by a
  real client's typed decode, the only observable gap is genuinely-missing
  fields, not extras; these four are real members with no backing model
  state (this backend doesn't simulate REST-endpoint-page publish/try-it
  lifecycle).
- Same reasoning for `PortalSummary`/`PortalProductSummary`/
  `ProductPageSummaryNoBody` vs. gopherstack's List responses reusing the
  full item type: harmless extra fields, not a parity bug by itself (same
  non-bug class as rds's `StorageOptimized`), only the missing-required-field
  gaps above are real.

Gates: `go build`/`go vet`/`go test -race` (scoped to
`services/apigatewayv2`), `go fix -diff` (no diff), `fieldalignment` (0
findings), `golangci-lint run` (0 issues; no cyclop/gocyclo/gocognit/funlen
nolints added or present) all green. `go test -race ./pkgs/...` green.

Per this session's hard constraints: no subagents used (Read/Grep/Bash
only), no git-mutating commands run (all `services/apigatewayv2` changes
uncommitted — orchestrator must commit/push). `services/personalize`
(live sibling session at start) and `services/workmail` (a second sibling
session that started mid-flight — 3 modified files observed via `git
status` partway through this session) both confirmed untouched throughout.

apigatewayv2's List/Describe/Get families are now fully swept for this
issue (37/37 ops verified against the real deserializer/serializer). 68 of
162 services swept, 94 remain. Per the ranked table, workmail (36,
`dynamic-fallback`) is next largest but is a live sibling session's
territory as of this session's own `git status` check; waf (34,
`dynamic-fallback`) or wafv2 (32, `direct`) are the next candidates least
likely to collide — re-check `git status` before picking.

## workmail (this session)

Chosen per this session's explicit assignment: the ranked table's next
candidate was apigatewayv2 (37 L+D+G), but `git status` at start showed it
already had live, uncommitted, growing edits from a sibling session
(`handler_domain_names.go`/`models.go`, a third file `portals.go` appeared
between two checks minutes apart) — confirmed NOT clear, avoided per this
session's own instructions. workmail (36 L+D+G: 18 List/9 Describe/9 Get,
`dynamic-fallback` resolution) was the next-largest candidate the sibling
was not in. Own enumeration of `buildOps()`'s four category-scoped map
builders (`buildOrgAndEntityOps`/`buildMailboxAndDomainOps`/
`buildAccessAndImpersonationOps`/`buildConfigAndTokenOps`, merged via
`maps.Copy` into one flat dispatch table) confirms the table's 36 exactly.

**PROTOCOL**: `awsAwsjson11_` (JSON-RPC 1.1) confirmed as the sole
deserializer-function prefix in
`aws-sdk-go-v2/service/workmail@v1.39.4/deserializers.go` (grep
`awsAwsjson1[0-9]*_|awsRestjson1_|awsEc2query_|awsAwsquery_`). Case-sensitive
— all 434 `EqualFold` hits are either `errorCode` matches or `NaN`/
`Infinity`/`-Infinity` float-parsing branches (3 hits, all inside numeric
decode), zero in a body-field `switch key {...}` block. **Only one real
client** — no separate runtime/data-plane module like personalize's
`personalizeruntime`; every op dispatches through the same
`awsAwsjson11_` deserializer.

**Dead-deserializer trap checked and found NOT to apply**: traced
`(*awsAwsjson11_deserializeOpListUsers).HandleDeserialize` in full
(deserializers.go:8263) — it decodes the body into `shape` and calls
`awsAwsjson11_deserializeOpDocumentListUsersOutput(&output, shape)`
directly (line 8303), the same JSON-RPC 1.1 pattern already confirmed
non-dead in awsconfig/cloudwatchlogs/macie2/cognitoidp/personalize in this
codebase.

Dumped every op's own `awsAwsjson11_deserializeOpDocument<Op>Output` case
list (per-op awk extraction, file+line implicit) and diffed field-for-field
against every gopherstack response/request struct and its real
`types.go`/`api_op_*.go` counterpart, for all 36 L+D+G ops plus every
Create/Update op sharing a response or request type with one of them.

**4 real bugs found and fixed, all the same lead-question-2 shape ("value
the backend already holds, or a real client can already set, that never
reaches the wire") plus one invented-shape/over-wide-field finding:**

1. `ListUsers` never emitted `IdentityProviderIdentityStoreId`/
   `IdentityProviderUserId` (real `types.User` members). The backend
   already tracked both (`DescribeUser` already emitted them correctly,
   confirmed field-for-field) but the `UserSummary` DTO built for `ListUsers`
   had no slot for either, so the converter silently dropped them. Fixed by
   adding both fields to `UserSummary` and `userSummaryResp`.
2. `ListGroupMembers` never emitted `EnabledDate`/`DisabledDate` (real
   `types.Member` members). Unlike finding #1, the backend's `Member` type
   itself already had both fields — the bug was one hop further back:
   `ListGroupMembers`' backend method synthesizes a fresh `Member` value per
   group membership and had already looked up the underlying `User`/`Group`
   record (to read its `Name`) but never copied `EnabledDate`/`DisabledDate`
   from that same lookup. Fixed in `groups.go`, not just the handler
   converter.
3. **`ListMailboxExportJobs` — invented shape, over-wide field, not a
   secret but an ARN leak.** Emitted `RoleArn`/`KmsKeyArn`/`S3Prefix`/
   `ErrorInfo` on every list item. The real `types.MailboxExportJob` (the
   List item type) is genuinely narrower than
   `DescribeMailboxExportJobOutput` — confirmed it has none of those four
   members at all (aws-sdk-go-v2/service/workmail@v1.39.4/types/types.go).
   A prior "parity-4" pass's own doc comment explicitly (and incorrectly)
   claimed "ListMailboxExportJobs reuses the SAME full shape as
   DescribeMailboxExportJob" — a PARITY.md-adjacent false claim, caught by
   reading the real deserializer rather than trusting the existing comment.
   A real typed client can't decode the extra fields (same "harmless to a
   typed client" property as other over-emission findings this campaign),
   but the raw wire body carried an IAM role ARN and a KMS key ARN — not a
   plaintext credential like cognitoidp's `ClientSecret`, but still a
   resource identifier disclosed on every list call that the real API never
   sends there. Removed all four fields from `mailboxExportJobSummaryJSON`
   and its converter.
4. `DescribeResource`/`UpdateResource` never modeled
   `HiddenFromGlobalAddressList` (a real member on both — confirmed on
   `DescribeResourceOutput` and `UpdateResourceInput`). Unlike
   `CreateUser`/`CreateGroup`, the real `CreateResourceInput` does NOT
   accept this field — it's Update-only for resources. The backend's
   `Resource` model had no field for it at all (not "tracked but unemitted,"
   genuinely never modeled). Added the field, threaded through
   `UpdateResource`'s signature (mirroring `UpdateGroup`'s existing
   always-overwrite, non-pointer convention for the same kind of field) and
   `DescribeResource`'s response.

**Sibling/near-duplicate shapes checked, reported clean**: `GetMailDomain`
vs `ListMailDomains` (two different real SDK types, `IsDefault` vs
`DefaultDomain` wire keys — already correctly distinguished by a prior
pass's citing comment, re-verified); `ListGroups` vs `ListGroupsForEntity`
(`types.Group` vs `types.GroupIdentifier`, same prior-pass distinction,
re-verified); `AccessControlRule`'s `IpRanges`/`NotIpRanges` casing
(already correct, has its own regression test); availability
configuration's `EwsProvider` correctly uses the real REDACTED shape
(`RedactedEwsAvailabilityProvider{EwsEndpoint,EwsUsername}`, no
`EwsPassword` field) — checked specifically because this campaign's brief
calls out credential-shaped fields, and this one was already right. No
V1/V2 or other generational sibling pairs exist in this service.

**Request side**: checked as part of every fix above (findings #1-#4 all
have a request-or-storage-side component); this service's request-side
gaps (`CreateResourceInput` genuinely has no `HiddenFromGlobalAddressList`,
confirmed) were distinguished from the response-side fixes rather than
assumed symmetric.

**Grep for discarded parameters / write-only fields**: no `_ bool`/`_
string`-style discarded backend parameters found. Finding #2 is this
service's instance of "value sits one hop away, in an already-looked-up
record, and is simply never copied" rather than a discarded parameter.

**Over-wide-field / credential check**: `ListMailboxExportJobs` (finding
#3) is this service's instance — an ARN leak, not a plaintext secret. No
API-key/client-secret-bearing resource type exists in this service at all
(WorkMail has no analogue to apigatewayv2's API keys or cognitoidp's
`ClientSecret`).

**Ratifying tests found and fixed: 1** —
`TestBugfix_WorkMail_ListMailboxExportJobsFullShape` (from the same prior
"parity-4" pass that introduced finding #3) asserted `RoleArn`/`KmsKeyArn`/
`S3Prefix` as correct on list items; both the handler and the test agreed
with the bug, so it passed cleanly against broken code. Renamed to
`TestBugfix_WorkMail_ListMailboxExportJobsNarrowShape` and rewritten to
assert their absence. **Zero** found in the other two ratifying-test shapes
(wrong value; assertion too weak to fail) — grepped every existing
`*_test.go` assertion touching the four fixed fields in either direction:
no other test referenced any of them before this session.

**Phantom ops**: none — the existing `sdk_completeness_test.go`'s
`TestSDKCompleteness` already cross-checks every `GetSupportedOperations()`
string against the real `workmailsdk.Client`'s method set via
`pkgs/sdkcheck`, passed before and after this session's changes.

**False-positive rate**: 0 among reported bugs — every finding cites the
real `deserializeOpDocument<Type>Output`/`types.go`/`api_op_*.go` struct
definition, file+line, confirmed reached from that op's own
`HandleDeserialize`, never a doc comment (and one existing doc comment —
the "parity-4" claim behind finding #3 — was itself the thing disproven).

**Disclosed, not fixed** (structural gaps needing new backend modeling, or
harmless extra fields a real client can't read into anything):
- `DescribeResource`/`UpdateResource`'s `BookingOptions`
  (`AutoAcceptRequests`/`AutoDeclineConflictingRequests`/
  `AutoDeclineRecurringRequests`) — real, accepted on both, but no
  booking/scheduling concept exists in this backend to source sensible
  values from, and this session could not independently confirm the real
  API's default state for a never-configured resource in the time
  available.
- `DescribeOrganization`'s `InteroperabilityEnabled` always reports
  `false` — no cross-org interoperability concept exists anywhere in this
  backend.
- Two harmless extra fields confirmed absent from their real types but left
  in place (same non-bug class as rds's `StorageOptimized`):
  `DescribeMailboxExportJobOutput`'s extra `JobId` (client already has it
  from the request) and `GetMailDomainOutput`'s extra `DomainName` (ditto).

4 real-SDK-client tests added in the new
`services/workmail/wire_field_fixes_test.go`, reusing the existing
`newWorkMailSDKClient` real-SDK-client helper from `wire_enableddate_test.go`
rather than inventing a new one (`Test_SDKRoundTrip_ListUsers_
IdentityProviderFields`, `Test_SDKRoundTrip_ListGroupMembers_EnabledDate`,
`Test_SDKRoundTrip_ListMailboxExportJobs_NarrowShape` — SDK-typed assertions
plus a raw-body check proving the ARNs no longer reach the wire at all, not
just that a typed client can't decode them —
`Test_SDKRoundTrip_Resource_HiddenFromGlobalAddressList`). Every fix
hand-reverted individually (no git, per this session's hard
no-git-mutation constraint), confirmed to fail with the exact predicted
symptom (empty-string `IdentityProviderUserId`; nil `EnabledDate`; the
fabricated ARN/prefix fields present again on the raw wire body, quoted
from the actual test failure output; `HiddenFromGlobalAddressList` false
after `UpdateResource` set it true), then restored and diffed
byte-identical against the pre-revert file before moving to the next.

Gates: `go build`/`go vet`/`go test -race` (scoped to
`services/workmail`), `go fix -diff` (no diff), `fieldalignment -fix` (one
real hit on `Resource`/`UserSummary` after the new fields were added,
auto-fixed then its stripped doc comments restored by hand), `golangci-lint
run` (0 issues; no cyclop/gocyclo/gocognit/funlen nolints added or present)
all green. `go test -race ./pkgs/...` green.

Per this session's hard constraints: no subagents used (Read/Grep/Bash
only), no git-mutating commands run (all `services/workmail` changes
uncommitted — orchestrator must commit/push), `services/apigatewayv2`
(the live sibling session's territory, confirmed growing from 2 to 3
modified files during this session's own investigation) confirmed
untouched throughout, no `gendocs`/`make docs` run.

workmail's List/Describe/Get families are now fully swept for this issue
(36/36 ops verified against the real deserializer). 69 of 162 services
swept, 93 remain. Per the ranked table, waf (34, `dynamic-fallback`) is
next largest — re-check `git status` for live sibling territory (including
apigatewayv2, still in flight as of this session's own last check) before
picking.

## wafv2 (this session)

Chosen per this session's explicit direction: wafv2/waf are a V1/V2 pair
(`waf` was already swept earlier this campaign, 13 candidates, clean), and
`workmail` (36, next-largest in the ranked table) was a live sibling
session's territory, confirmed via `git status` before starting. wafv2 (32
L+D+G ops: 13 List/3 Describe/16 Get, `direct` resolution) was the largest
remaining candidate that didn't collide.

PROTOCOL: confirmed `awsAwsjson11_` (JSON-RPC 1.1) from
wafv2@v1.77.3's `deserializers.go` function-prefix grep (sole prefix
present — no `awsRestjson1_`/`awsEc2query_`/`awsAwsquery_`), matching the
`AWSWAF_20190729.` X-Amz-Target prefix already in `handler.go`.
Case-sensitive body-field decode, like awsconfig/cloudwatchlogs/guardduty.
328 `EqualFold` hits total in `deserializers.go`; the 12 that don't match
`errorCode` are all `NaN`/`Infinity`/`-Infinity` float-special-value parsing
inside numeric-field decode branches, none in a body-field `switch key {
case "...":}` block — spot-checked line by line, so body-field casing here
is a non-issue by construction. **Second client**: none — `GetSupportedOperations`
is one flat literal string slice (`handler.go`), no dispatch-table trap.

**Dead-deserializer trap checked and found NOT to apply** — traced
`HandleDeserialize` for `ListWebACLs` directly (deserializers.go:5936): it
decodes the body into `shape` and calls
`awsAwsjson11_deserializeOpDocumentListWebACLsOutput(&output, shape)`
itself (deserializers.go:5976) — the `OpDocument*Output` function **is** the
real, reached deserializer here, same JSON-RPC-1.1 shape as
awsconfig/cloudwatchlogs/guardduty, not pinpoint's restjson1 wrapper-bypass
shape.

**V1-versus-V2 comparison**: waf (V1) was re-checked as a reference, not
assumed clean from the campaign's earlier note. No V1-shaped field or
convention was found leaking into wafv2's types — the two services share no
Go types and no V1-only member (e.g. waf's `ChangeToken` state-machine
concept) appears anywhere in wafv2's wire shapes. Clean in both directions;
no securityhub/guardduty-style leak here.

Read all 32 L+D+G ops' response shapes against their own
`awsAwsjson11_deserializeOpDocument<Op>Output` case list (file+line), plus
every nested-type deserializer they call into (`ManagedProductDescriptor`,
`RuleGroup`/`RuleGroupSummary`, `WebACL`/`WebACLSummary`, `IPSet`/
`IPSetSummary`, `RegexPatternSet`, `APIKeySummary`, `ManagedRuleSet`/
`ManagedRuleSetSummary`, `RevenueBreakdown`, etc.), and the paired
`serializeOp*Input` for every op whose request carries a field the handler
reads or discards.

**4 real bugs found and fixed:**

1. **`ListAPIKeys` — wrong wrapper key, the core bug class this issue
   tracks.** Emitted items under `"APIKeys"`; the real
   `ListAPIKeysOutput` wraps them under `"APIKeySummaries"`
   (deserializers.go:21185). A real typed client's `APIKeySummaries` field
   was always empty regardless of how many keys existed — total silent data
   loss for this op, same shape as omics' service-wide `items` bug from the
   first pass on this issue. An existing raw-body test
   (`TestHandler_ListAPIKeys`) asserted `resp["APIKeys"]` as correct and
   passed cleanly against the bug, because the handler and the test agreed
   on the wrong key — a ratifying test, fixed alongside (see below).
2. **`APIKeySummary`/`GetDecryptedAPIKeyOutput` — missing `CreationTimestamp`
   entirely, on both ops.** Real, always-populated member on both shapes
   (deserializers.go's `smithytime.ParseEpochSeconds` case, `APIKey`
   creation time) with no backing field anywhere in gopherstack's `APIKey`
   model at all — not a rename, new modeling. Fixed by adding
   `APIKey.CreatedAt int64` (Unix epoch seconds, matching this service's
   existing epoch-int64 convention for `mobileSdkReleaseInfo.Timestamp`),
   set at `CreateAPIKey`, threaded through `ListAPIKeys`/`GetDecryptedAPIKey`.
3. **`RuleGroup` — sibling trap against `WebACL`, exactly the shape this
   session was told to hunt for.** Real `CreateRuleGroupInput`/
   `UpdateRuleGroupInput`/`GetRuleGroupOutput.RuleGroup` all carry
   `CustomResponseBodies` (`api_op_CreateRuleGroup.go`) — used by
   `CUSTOM_RESPONSE` block actions inside a rule group's own rules, same
   concept `WebACL` already models end-to-end in this same file set
   (`handler_web_acls.go`/`web_acls.go`: accepted on Create/Update, stored,
   cloned, conditionally re-emitted). `RuleGroup` had no field for it at
   all — a real client's `CustomResponseBodies` on `CreateRuleGroup`/
   `UpdateRuleGroup` was silently discarded, and `GetRuleGroup` never had
   anything to echo back regardless. Fixed by mirroring the exact
   WebACL pattern onto RuleGroup: new `RuleGroup.CustomResponseBodies
   json.RawMessage` field, accepted on Create/Update, deep-cloned in
   `cloneRuleGroup` (byte-copy, matching `cloneWebACL`'s pattern), emitted
   conditionally in `GetRuleGroup`.
4. **`DescribeAllManagedProducts`/`DescribeManagedProductsByVendor` —
   backend-tracked-but-unemitted.** Real `ManagedProductDescriptor.
   IsVersioningSupported` (deserializers.go's case list) was never emitted
   by either op, even though the backend's static catalog
   (`managedRuleGroupInfo.VersioningSupported`) already tracks it correctly
   and is already emitted correctly by the sibling op
   `ListAvailableManagedRuleGroups` in the same file. Fixed by emitting it
   on both ops.

**Fabricated field found, disclosed not removed (harmless):**
`DescribeManagedRuleGroup` emits a `"Description"` key that does not exist
anywhere in the real `DescribeManagedRuleGroupOutput` (deserializers.go:20304
-20360's case list is `AvailableLabels`/`Capacity`/`ConsumedLabels`/
`LabelNamespace`/`Rules`/`SnsTopicArn`/`VersionName` — no `Description`
member at all). A real typed client silently ignores unknown JSON keys, so
this is a cosmetic invented field, not a bug — same non-bug class as rds's
previously-noted `StorageOptimized`. Left in place rather than mutated,
to keep this session's fix set to genuinely load-bearing findings; noted
here for a future pass that wants a fabrication-cleanup sweep.

Also noted, not fixed (harmless, same class): `IPSet`/`WebACL`/
`ManagedRuleSet`'s full-object `Get*` responses each fabricate a `LockToken`
member *inside* the nested object (`{"IPSet": {..., "LockToken": ...}}`)
in addition to the real, correctly-placed top-level `LockToken` echo — the
real `IPSet`/`WebACL`/`ManagedRuleSet` types have no `LockToken` member at
all (confirmed against each type's own case list). By contrast the List-op
summary types (`IPSetSummary`/`WebACLSummary`/`RuleGroupSummary`/
`ManagedRuleSetSummary`/`RegexPatternSet`'s Create `Summary`) genuinely DO
have a real `LockToken` member, so those are correct as written — this is
a full-object-vs-summary-type distinction, not a service-wide bug, and
every occurrence is a repeat of the same harmless pattern.

**Over-wide field / secret check**: no leak found. `GetDecryptedAPIKey`
and `ListAPIKeys`' per-item map both include a fabricated `"Scope"` key
absent from the real `GetDecryptedAPIKeyOutput`/`APIKeySummary` types —
harmless (Scope is public request-filter metadata already known to the
caller, not a secret, and a real client's typed struct has no field to
decode it into). `CreateAPIKey`'s only sensitive value (`APIKeyValue`) is
already base64-encoded exactly as the real `APIKey`/`APIKeySummary.APIKey`
wire member is, matching AWS's own opaque-token convention — not plaintext
exposure of anything AWS itself keeps secret, unlike cognitoidp's
`ClientSecret` finding from an earlier batch.

**Discarded input**: `RuleGroup.CustomResponseBodies` (finding #3 above) is
the only one found — a real, non-required request field silently dropped by
both `CreateRuleGroup` and `UpdateRuleGroup`. Not a total-outage-severity
discard like apigatewayv2's `CreateProductPage.DisplayContent` (that field
was required; this one is optional), but the same variant: a real client
setting it got silent data loss with no error.

**Real-client test ratio**: 5 of 21 test files in this service
(`handler_api_keys_test.go` as of this session's rewrite,
`handler_create_tags_test.go`, `handler_rate_based_rules_test.go`,
`sdk_completeness_test.go`, and the new `wire_field_fixes_test.go`) import
the real SDK client; the other 16 unmarshal into raw `map[string]any` or
gopherstack's own request/response structs, which cannot detect a wrong
wire key by construction. `TestHandler_ListAPIKeys` (finding #1's ratifying
test) is a
direct instance: a raw-body assertion on `resp["APIKeys"]` that could only
ever prove the wrong key was present, never that it was wrong. Rewritten to
drive `newTestWAFV2Client` and assert `out.APIKeySummaries`, which cannot
compile-pass, let alone assert-pass, against the unfixed key.

**Ratifying tests**: 1 found and fixed (`TestHandler_ListAPIKeys`, above).
No other existing test asserted a wrapper key or nested field this session
touched, so no other ratifying instances exist among the 4 fixes.

**Phantom ops**: none. All 59 op-name string literals in
`GetSupportedOperations` correspond to a real `api_op_*.go` file in
wafv2@v1.77.3, including the four AI-bot monetization-reporting ops
(`GetRevenueStatistics*`/`ListSettlementRecords`) added in wafv2@v1.76.0.

**False-positive rate**: 0 among reported bugs — every finding cites the
real `deserializeOpDocument<Type>Output`/`deserializeDocument<Type>`/
`serializeOp*Input` function actually reached from that op's own
`HandleDeserialize`, file+line, never a doc comment or an assumption.

**Every fix confirmed to fail against unfixed code**: hand-reverted
individually (no git, per this session's hard no-git-mutation constraint),
confirmed to fail with the exact predicted symptom — `APIKeySummaries`
empty-slice-length mismatch (both `filter_scope_match` and `list_regional`
subtests); `CreationTimestamp` nil; `RuleGroup.CustomResponseBodies` nil map
missing the test's key entirely; `IsVersioningSupported` false on the one
catalog entry that should be true — then restored and diffed byte-identical
against the pre-revert file before moving to the next.

**Disclosed, not fixed** (structural/optional-field gaps needing new
backend modeling this session judged too speculative to fabricate, each
independently verified absent from the backend's tracked state):
- `RuleGroup`/`GetRuleGroupOutput.RuleGroup`'s `AvailableLabels`/
  `ConsumedLabels`/`LabelNamespace` — real, non-required members; unlike the
  static managed-rule-group catalog (which already computes labels from its
  hardcoded rule data via `buildLabelList`), user-created `RuleGroup.Rules`
  is an opaque `[]map[string]any` blob this backend never parses for label
  statements, so there is nothing genuine to compute these from.
- `ManagedProductDescriptor`'s `IsAdvancedManagedRuleSet`/`ProductId`/
  `ProductLink`/`ProductTitle`/`SnsTopicArn` — no backing field anywhere in
  the static `managedRuleGroupInfo` catalog; fabricating plausible-looking
  product IDs/links would be invention, not a rename.
- `WebACL`'s `Capacity`/`LabelNamespace`/`ManagedByFirewallManager`/
  `MonetizationConfig`/`ApplicationConfig`/`DataProtectionConfig`/
  `OnSourceDDoSProtectionConfig`/`{Pre,Post}ProcessFirewallManagerRuleGroups`/
  `RetrofittedByFirewallManager` — none tracked anywhere in the `WebACL`
  model; `Capacity` in particular would need a real WCU-accounting pass
  (summing `CheckCapacity`-style costs across `Rules`) that doesn't exist
  today, a genuine future-modeling gap rather than a quick emit.
- `ManagedRuleSet`/`ManagedRuleSetSummary`'s `Description`/`LabelNamespace`
  — this service has no `CreateManagedRuleSet` op (an existing, prior-session
  PARITY.md-documented gap — Firewall-Manager-only resource, bootstrapped
  here only via `PutManagedRuleSetVersions` on a pre-seeded ID), so there is
  no write path that could ever populate either field honestly.
- `APIKeySummary.Version` — real member, but doc-commented "Internal value
  used by WAF to manage the key"; no defensible way to synthesize an AWS
  internal bookkeeping value.
- `GetRevenueStatistics`/`GetRevenueStatisticsTimeSeries`/
  `ListSettlementRecords`'s `NextMarker` — all three already correctly
  return the full unpaginated result in one call (no backend pagination
  cursor for this all-honest-zeros analytics family), so there's never a
  next page to point to; consistent with the file's existing "honestly
  empty, never fabricated" design already documented in
  `handler_revenue_statistics.go`.

Tests: 4 real-SDK-client tests in the new
`services/wafv2/wire_field_fixes_test.go` (`TestRuleGroup_CustomResponseBodies`,
`TestDescribeAllManagedProducts_IsVersioningSupported`) plus
`TestHandler_ListAPIKeys` (rewritten to drive `newTestWAFV2Client`) and the
new `TestHandler_GetDecryptedAPIKey_CreationTimestamp` in
`services/wafv2/handler_api_keys_test.go`. Three pre-existing direct-backend
test call sites (`handler_permission_policies_test.go`,
`persistence_test.go` x2) updated for `CreateRuleGroup`'s new
`customResponseBodies` parameter — no behavior change, just the added
positional argument.

Gates: `go build`/`go vet`/`go test -race` (scoped to `services/wafv2`), `go
fix -diff` (no diff), `golangci-lint run` (0 issues; no cyclop/gocyclo/
gocognit/funlen nolints added or present) all green. `go test -race
./pkgs/...` green.

Per this session's hard constraints: no subagents used (Read/Grep/Bash
only), no git-mutating commands run (all `services/wafv2` changes
uncommitted — orchestrator must commit/push); `services/workmail` (this
session's assigned-off-limits sibling) and `services/ce` (a second,
unannounced sibling session discovered mid-session via `git status`)
confirmed untouched throughout; no `gendocs`/`make docs` run.

wafv2's List/Describe/Get families are now fully swept for this issue
(32/32 ops verified against the real deserializer/serializer). 70 of 162
services swept, 92 remain. Per the ranked table, waf (34,
`dynamic-fallback`) is next largest — re-check `git status` for live
sibling territory before picking. This session was told waf had already
come back clean across 13 candidates earlier in this campaign, but that
claim wasn't independently re-verified here (no citation for it was found
in this file or in `bd show gopherstack-6flj`'s comments) — a future pass
should confirm it against this issue's own op-by-op standard (all
Describe/List/Get ops, not just 13) rather than trust it secondhand.

## ce (this session)

Chosen as the largest unswept service (31 L+D+G ops: 7 List/1 Describe/23
Get) that a live sibling session wasn't already in — `git status` was clean
at start and the sibling was independently confirmed to be on waf/wafv2 (per
this session's own instructions, later corroborated by wafv2's own section
above landing concurrently in this file). All 31 ops read against
`costexplorer@v1.67.4` before any fix, plus the 16 non-L/D/G ops touched by
the fixes below (Create/Start/Update siblings sharing a response type).

PROTOCOL: confirmed `awsAwsjson11_` (JSON-RPC 1.1) — the only prefix present
in `deserializers.go`. Case-sensitive. All 173 `EqualFold` hits are either
`errorCode` matching or `NaN`/`Infinity`/`-Infinity` float-string parsing
inside numeric-field decode branches (spot-checked every occurrence, none in
a body-field `switch key {}` block) — a non-issue by construction, same as
awsconfig/cloudwatchlogs/guardduty. One client only: grepped the whole
service for a second `costexplorer.NewFromConfig`/`costexplorersdk.` import
and found exactly one, in `handler_error_type_test.go`'s
`newTestCEClient` helper — no separate runtime module.

**Dead-deserializer trap checked and does not apply**: read
`awsAwsjson11_deserializeOpGetCostAndUsage.HandleDeserialize`
(deserializers.go:1404) directly — it decodes the body into `shape` and
calls `awsAwsjson11_deserializeOpDocumentGetCostAndUsageOutput(&output,
shape)` itself; the `OpDocument...Output` function **is** the real, reached
deserializer for every op in this service (JSON-RPC 1.1, same pattern as
every other `awsAwsjson1{0,1}` service this campaign has swept).

**Real-client test ratio: 2 of ~146 tests (about 1.4%) drove a real SDK
client before this session**, both in `handler_error_type_test.go`, and both
about malformed-JSON error handling, not field/wire-shape correctness. Every
other test in the package (`handler_cost_usage_test.go`,
`handler_reservations_test.go`, etc.) calls the handler directly via
`doRequest` and decodes the raw response body into a hand-picked anonymous
struct whose JSON tags the test author chose to match whatever the handler
already emits — the exact shape of test that cannot detect a wrong wire key
by construction, worse even than apigatewayv2's 3/36 and on par with mwaa's
0/12 as a "lots could be hiding" signal.

**6 real bugs found and fixed**, two of them the same class repeated on two
different op-families (a service-wide sibling trap once one instance was
spotted):

1. **`ListCostAllocationTagBackfillHistory`/`StartCostAllocationTagBackfill`
   — internal model emitted directly on the wire.** The backend's
   `BackfillJob` struct (used for snapshot persistence) carries
   lowerCamelCase JSON tags (`backfillFrom`, `backfillStatus`, ...); both
   handlers embedded `*BackfillJob`/`[]*BackfillJob` directly as the response
   type instead of converting to a wire-shape struct, unlike this file's own
   sibling `CostAllocationTag`→`costAllocationTagEntry` converter two
   functions above it. Under this service's case-sensitive JSON-RPC 1.1, a
   real client's typed `BackfillFrom`/`BackfillStatus`/`CompletedAt`/
   `LastUpdatedAt`/`RequestedAt` were nil/empty on every item and on the
   single `BackfillRequest`, regardless of backend state — confirmed against
   `types.CostAllocationTagBackfillRequest`'s deserializer
   (deserializers.go:7192). Fixed with a `backfillRequest` wire struct +
   `toBackfillRequest` converter.
2. **`ListCommitmentPurchaseAnalyses` — the identical bug, same file
   family, same fix shape.** The backend's `CommitmentAnalysis` struct
   (also dual-purposed for persistence) carries the same lowerCamelCase
   tags; `ListCommitmentPurchaseAnalyses` embedded `[]*CommitmentAnalysis`
   directly. A real client's typed `AnalysisId`/`AnalysisStatus`/
   `AnalysisStartedTime`/`EstimatedCompletionTime`/`ErrorCode` were
   nil/empty on every item, confirmed against `types.AnalysisSummary`'s
   deserializer (deserializers.go:6129). A third sibling in the same file
   family, `ListSavingsPlansPurchaseRecommendationGeneration`, already had
   this exact fix applied by a prior pass (its own citing comment says so)
   — this is the "lone outlier among consistent siblings is real" pattern
   inverted: two of three siblings had the bug, one didn't, and finding the
   one correct sibling is what pointed at the pattern. Fixed with an
   `analysisSummary` wire struct + `toAnalysisSummary` converter.
3. **`StartCommitmentPurchaseAnalysis` discarded its entire input.** The
   handler's signature was `_ *startCommitmentPurchaseAnalysisInput` —
   the request body, including the required
   `CommitmentPurchaseAnalysisConfiguration` member
   (`api_op_StartCommitmentPurchaseAnalysis.go`: "This member is
   required"), was never read, validated, or stored. A request missing it
   entirely got a 200 instead of the real API's rejection; a request
   supplying it had the value silently dropped. Same variant as
   apigatewayv2's `CreateProductPage` discarding its input with `_`. Fixed:
   the field is now required (400 if absent), stored on a new
   `CommitmentAnalysis.Configuration any` field, and echoed back verbatim
   on Get/List (confirmed both carry it via `types.GetCommitmentPurchaseAnalysisOutput`/
   `types.AnalysisSummary`'s `CommitmentPurchaseAnalysisConfiguration`
   member) — but *not* echoed on Start's own output, since
   `StartCommitmentPurchaseAnalysisOutput` genuinely has no such member
   (`api_op_StartCommitmentPurchaseAnalysis.go`; only
   `AnalysisId`/`AnalysisStartedTime`/`EstimatedCompletionTime`) — caught
   this as a self-correction via `go vet` after first over-adding the field
   there too.
4. **`GetCommitmentPurchaseAnalysisOutput` had an invented field name.**
   `EstimatedSavings any` named no real member of
   `GetCommitmentPurchaseAnalysisOutput` at all and was never populated
   (dead field, always omitted). The real member is `AnalysisDetails`
   (nested `SavingsPlansPurchaseAnalysisDetails`, deserializers.go:16334) —
   disclosed as not modeled below, since this backend never simulates
   analysis internals. Fixed by removing the fabricated field and adding
   the real `CommitmentPurchaseAnalysisConfiguration` echo instead.
5. **`GetCostCategories` never emitted `CostCategoryNames`, always emitted
   `CostCategoryValues` regardless of whether `CostCategoryName` was set.**
   Real `GetCostCategoriesOutput` (api_op_GetCostCategories.go) documents:
   "If the CostCategoryName key isn't specified in the request, the
   CostCategoryValues fields aren't returned" — implying `CostCategoryNames`
   is what's returned instead. A real client asking "what cost categories
   exist" (the common no-name discovery call) got an empty typed
   `.CostCategoryNames` back every time, with values dumped in the wrong
   field. Fixed: added `Backend.GetCostCategoryNames()` (distinct category
   names, sorted) and branched the handler on `CostCategoryName` presence.
6. **`GetRightsizingRecommendationOutput` never echoed `Configuration`.**
   Real `GetRightsizingRecommendationOutput`
   (api_op_GetRightsizingRecommendation.go) always carries `Configuration`
   (`RecommendationTarget`/`BenefitsConsidered`, server-applied defaults
   `SAME_INSTANCE_FAMILY`/`true` per `types.RightsizingRecommendationConfiguration`'s
   doc comments) — the field was absent from gopherstack's response
   entirely, so a real client's typed `.Configuration` was always nil
   regardless of what it requested. Fixed by echoing the request's
   Configuration (or the documented defaults when absent).

**Over-wide/sensitive-field check**: none of the 6 findings involve a
secret, credential, or a resource ARN the caller couldn't already see —
all are missing/miscased/mislabeled fields or a discarded request value,
not data leakage. `CommitmentAnalysis.Configuration`/`BackfillJob`'s fields
are the caller's own request data being echoed back, not backend-internal
state.

**Sibling/version pairs**: the `CostAllocationTag`→`costAllocationTagEntry`
converter (already correct, cited above as the pattern the two bugs
deviated from) and
`SavingsPlansGeneration`→`generationSummary`/`RecommendationId`-not-`GenerationId`
converter (already correct, own prior-session citing comment) are two
already-correct siblings in the same "start a job / list job history"
shape as the two bugs found — confirming the fix shape rather than
inventing one. `GetSavingsPlansCoverage`/`GetSavingsPlansUtilizationDetails`
(`NextToken`) vs. most other paginated ops (`NextPageToken`) is a genuine,
already-correctly-modeled split in this service (verified per-op against
each op's own deserializer, not assumed) — not a bug, a real AWS CE
convention inconsistency this service already tracks correctly.
`GetSavingsPlansUtilization` (no token field at all) is also correct as
written — a third, already-correct data point on the same axis.

**Ratifying tests found and fixed — 4**, spanning two of the three
documented shapes (wrong key, weak assertion; no wrong-value case found
here):
- `TestHandler_GetCostCategories`'s `returns_all_values_when_no_filter`
  subtest asserted 2 `CostCategoryValues` when no `CostCategoryName` was
  given — exactly the pre-fix bug, passing because the test's own
  expectation was written to match broken behavior. Renamed to
  `returns_all_names_when_no_filter` and rewritten to assert
  `CostCategoryNames` populated / `CostCategoryValues` empty.
- `TestCommitmentPurchaseAnalysis_Lifecycle`'s list-assertion decoded
  `AnalysisSummaryList[].AnalysisId` under the wrong-case tag
  `json:"analysisId"` — passed only because the pre-fix handler emitted
  that same wrong case. Fixed to the real `"AnalysisId"` tag and
  strengthened to assert the decoded ID actually matches the started
  analysis (previously only checked list length).
- `TestCommitmentAnalysis_MultipleStartsListed` decoded
  `AnalysisSummaryList` into `[]map[string]any` and asserted only `Len ==
  3` — a weak-assertion ratifying test that could never fail on a wrong key
  since it never looked at any key. Strengthened to assert each item's
  `AnalysisId` is non-empty and the decoded ID set matches the started IDs.

**Phantom ops**: none — all 47 op-name string literals in
`GetSupportedOperations` confirmed against `api_op_*.go` files in
`costexplorer@v1.67.4`.

**False-positive rate**: 0 among reported bugs — every finding cites the
real `deserializeOpDocument<Type>Output`/`deserializeDocument<Type>`/
`serializeOpDocument<Type>Input` function actually reached from that op's
own `HandleDeserialize`, file+line, or the real `api_op_*.go`/`types.go`
struct definition for request-side/missing-member gaps, never a doc
comment or an assumption. One self-caught false step during the session
(bug 3's fix initially over-added `CommitmentPurchaseAnalysisConfiguration`
to `StartCommitmentPurchaseAnalysisOutput` too) was caught by `go vet`
failing to compile against the real SDK type before any test ran, not
shipped.

**Disclosed, not fixed** (structural/optional-field gaps needing new
backend modeling, each independently verified absent from tracked state):
- `GetCommitmentPurchaseAnalysisOutput`/`AnalysisSummary`'s
  `AnalysisDetails`/`SavingsPlansPurchaseAnalysisDetails` (computed
  estimated-savings figures) and `AnalysisSummary`'s
  `AnalysisCompletionTime` — this backend's commitment analyses never
  leave `PROCESSING` and never compute a savings estimate, so there is no
  non-fabricated value for either.
- `Anomaly`'s `RootCauses` items are missing real `types.RootCause`'s
  `LinkedAccountName`/`Impact` (a nested `RootCauseImpact`) — the backend
  has no anomaly-generation path that populates `RootCauses` at all
  (`AddAnomaly` is exported but uncalled outside tests), so there's no
  live call site to source either from.
- `CostAllocationTag` is missing real `types.CostAllocationTag`'s
  `LastUsedDate` — not tracked anywhere in the backend's cost-allocation-tag
  model; would require joining tag usage against the cost-and-usage ledger,
  new modeling.
- `ActivityResponse`-adjacent gaps not applicable here; the equivalent
  under-modeled areas in this service
  (`ReservationRecommendationDetail`/`RightsizingRecommendation`'s deeper
  nested nested fields, `GetCostComparisonDrivers`'s always-empty list) were
  already correctly disclosed by prior-session citing comments throughout
  `handler_cost_usage.go`/`handler_reservations.go` and re-verified rather
  than re-disclosed here.

**Request-side check**: performed for every fix above (all are
request+response pairs except #6, response-only since `Configuration` is
already read correctly on the request side and only the echo was missing,
and #5, response-shape-selection-only since `GetCostCategoriesInput` has no
corresponding request-side bug).

7 real-SDK-client tests added in the new
`services/ce/wire_field_fixes_test.go`
(`TestBackfillHistory_RealClient`, `TestCommitmentPurchaseAnalysis_RealClient`,
`TestStartCommitmentPurchaseAnalysis_MissingConfigurationReturns400`,
`TestGetCostCategories_NamesVsValues_RealClient`,
`TestGetRightsizingRecommendation_Configuration_RealClient`), plus the 3
ratifying-test rewrites above. Every fix hand-reverted individually (no
git, per this session's hard no-git-mutation constraint), confirmed to fail
with the exact predicted symptom (quoted in each test's failure output —
empty typed fields, wrong enum zero-value, or 200 instead of 400), then
restored and diffed byte-identical against the pre-revert file before
moving to the next.

Gates: `go build`/`go vet`/`go test -race` (scoped to `services/ce`), `go
fix -diff` (no diff), `golangci-lint run` (0 issues after a `golines`
line-length fix on the new test file; no cyclop/gocyclo/gocognit/funlen
nolints added or present) all green. `go test -race ./pkgs/...` green.

Per this session's hard constraints: no subagents used (Read/Grep/Bash
only), no git-mutating commands run (all `services/ce` changes uncommitted
— orchestrator must commit/push); `services/waf`/`services/wafv2` (the
assigned-off-limits sibling territory) confirmed untouched throughout,
verified again at the end via `git status`; no `gendocs`/`make docs` run.

ce's List/Describe/Get families are now fully swept for this issue (31/31
ops verified against the real deserializer/serializer). 71 of 162 services
swept, 91 remain. Per the ranked table, waf (34, `dynamic-fallback`) is
still next largest and still not independently re-verified clean per this
issue's own op-by-op standard (see wafv2's note above) — re-check `git
status` for live sibling territory before picking.

## waf (this session)

Chosen as the largest unswept service per the ranked table (34 L+D+G ops: 16
List/0 Describe/18 Get, `dynamic-fallback` resolution — resolved directly by
reading `buildOps()`'s literal map in `handler.go`, which returns exactly 16
`List*`/18 `Get*` op names, confirming the table's count exactly). `git
status` was clean at the start (no sibling anywhere) and again mid-session
(no local edits of my own — this batch found zero bugs, see below); a check
near the end of the session showed a sibling had since started on
`services/vpclattice/` (10 modified files) — confirmed not colliding with
`services/waf/`, left untouched.

**This service's own `wafv2` sibling note explicitly flagged its "waf
already swept, 13 candidates, clean" claim as unverified** — no citation for
it exists in this file or in `bd show gopherstack-6flj`'s comments. That "13
candidates" claim traces to a *different* issue class
(`services/waf/PARITY.md`'s 2026-08-14 note, gopherstack-dv4s: an
over-wide-response-leak audit of 13 List ops' summary-type field sets, not
this issue's List+Describe+Get wrapper-key/nesting sweep). The two audits
overlap in which ops they touched but check different things; this session
did not trust either claim and independently re-verified all 34 ops from
scratch against the pinned SDK.

PROTOCOL: confirmed `awsAwsjson11_` (JSON-RPC 1.1) — sole prefix in
waf@v1.33.4's `deserializers.go`, `X-Amz-Target: AWSWAF_20150824.<Op>`
confirmed in both the SDK's `serializers.go` and gopherstack's own
`handler.go` (`wafTargetPrefix`). Case-sensitive body-field decode. All 375
`EqualFold` hits in `deserializers.go` are `errorCode` matches in
`deserializeOpError*` functions (this SDK version has no float-special-value
fields at all, so unlike every other service swept this session there isn't
even a `NaN`/`Infinity` category to spot-check) — zero hits in a body-field
`switch key {}` block, confirmed by grep with `errorCode` excluded returning
0 lines. **Second client**: none — grepped the whole service for a second
`waf.NewFromConfig`/`wafsdk.` import path; only `wafsdk` (`aws-sdk-go-v2/
service/waf`) is used anywhere, no separate `wafregional` module is even
pinned in `go.mod` (AWS WAF Classic's regional variant is a distinct legacy
API gopherstack doesn't model at all — out of scope, not a gap).

**Dead-deserializer trap checked and found NOT to apply** — traced
`HandleDeserialize` for `ListWebACLs` directly
(waf@v1.33.4/deserializers.go:7147): it decodes the body into `shape` and
calls `awsAwsjson11_deserializeOpDocumentListWebACLsOutput(&output, shape)`
itself (deserializers.go:7187) — the `OpDocument*Output` function **is** the
real, reached deserializer, same JSON-RPC-1.1 shape as every other
`awsAwsjson1{0,1}` service this campaign has swept.

Read all 34 L+D+G ops' response shapes against their own
`awsAwsjson11_deserializeOpDocument<Op>Output` case list (file+line via a
per-op grep dump), every nested-type deserializer they call into (all 12
resource families' full-detail type plus its dedicated `*Summary` type,
`WafAction`/`WafOverrideAction`/`Predicate`/`FieldToMatch`/
`IPSetDescriptor`/`ByteMatchTuple`/`SizeConstraint`/
`SqlInjectionMatchTuple`/`XssMatchTuple`/`GeoMatchConstraint`/
`ExcludedRule`/`RegexMatchTuple`/`TagInfoForResource`/`Tag`/
`SampledHTTPRequest`/`TimeWindow`/`HTTPRequest`/`HTTPHeader`/
`LoggingConfiguration` — 27 distinct real types total), and the paired
`serializeOp*Input` for every Create/Update/Delete/Put sibling (34 more ops)
whose request carries a field the handler reads or could discard.

**0 bugs found.** Every wrapper key on all 16 List ops matches the real
`ListXxxOutput` case list exactly (`WebACLs`/`Rules`/`IPSets`/
`ByteMatchSets`/`SizeConstraintSets`/`SqlInjectionMatchSets`/
`XssMatchSets`/`GeoMatchSets`/`Rules` again for `ListRateBasedRules`
(confirmed the real op reuses the plain `Rules` key, not a
`RateBasedRules`-named one)/`RegexPatternSets`/`RegexMatchSets`/
`RuleGroups`/`ActivatedRules`/`RuleGroups` again for
`ListSubscribedRuleGroups`/`LoggingConfigurations`/`TagInfoForResource`).
Every wrapper key on all 18 Get ops matches too (`ChangeToken`/
`ChangeTokenStatus`/`WebACL`/`Rule`/`IPSet`/`ByteMatchSet`/
`SizeConstraintSet`/`SqlInjectionMatchSet`/`XssMatchSet`/`GeoMatchSet`/
`Rule` again for `GetRateBasedRule` (real `GetRateBasedRuleOutput.Rule` is
typed `*types.RateBasedRule`, not a "RateBasedRule"-named key — confirmed
against `api_op_GetRateBasedRule.go` directly, not assumed)/`ManagedKeys`/
`RegexPatternSet`/`RegexMatchSet`/`RuleGroup`/`LoggingConfiguration`/
`Policy`/`PopulationSize`+`SampledRequests`+`TimeWindow`). Every one of the
27 nested types' field sets matches the real deserializer's case list
field-for-field, including the two places this issue's brief predicted a
trap and didn't find one:
- **`RuleGroup` (full, 3 fields: `RuleGroupId`/`Name`/`MetricName`) vs
  `RuleGroupSummary` (2 fields, no `MetricName`)** — a genuine
  version/detail-vs-summary pair, correctly differentiated as two distinct
  Go types in `models.go`, each matching its own real deserializer exactly.
  No V1/V2 pair exists in this service at all (WAF Classic has no
  generational split within itself — that's wafv2's relationship to this
  service, already checked from wafv2's side and re-confirmed from waf's
  side this session: no wafv2-shaped field or convention appears anywhere
  in waf's types).
- **`GetRateBasedRuleManagedKeysInput`/`Output`'s `NextMarker`** — parsed on
  the request side but never applied to pagination, which looked at first
  read like the discarded-input variant this issue's brief calls out
  (`_ SomeInput` class). Checked against the real SDK's own doc comment
  before flagging: `GetRateBasedRuleManagedKeysInput.NextMarker` is
  documented "A null value and not currently used. Do not include this in
  your request," and the output's `NextMarker` carries the identical
  doc-commented caveat. Genuinely vestigial on both sides in real AWS itself
  — discarding it is correct behavior, not a bug. Already correctly
  disclosed in `services/waf/PARITY.md`'s `structural_gaps` (`ManagedKeys`
  list itself stays empty, `gopherstack-smld`) for the unrelated reason that
  gopherstack has no live request-rate-tracking subsystem to source real
  managed keys from.

**Sibling-trap / near-duplicate shapes checked, all clean**: the 7
near-identical match-set families (`ByteMatchSet`/`SizeConstraintSet`/
`SqlInjectionMatchSet`/`XssMatchSet`/`GeoMatchSet`/`RegexPatternSet`/
`RegexMatchSet`, deliberately merged into one `handler_match_sets.go` per an
existing file-level comment citing a `dupl` lint reason) each have their own
correctly-keyed wrapper and correctly-shaped summary/full types — no
copy-paste-from-a-sibling mistake found in any of the seven. `Rule`/
`RateBasedRule` (share the `Predicate`/`MatchPredicates` shape) both
correct, distinctly. `LoggingConfiguration` is the same Go type on both
`GetLoggingConfiguration` and `ListLoggingConfigurations`' per-item
shape (no separate summary type exists in the real API either — confirmed,
not assumed).

**Over-wide field / secret check**: none of the response types carry
anything beyond their real member set — no fabricated fields found anywhere
in this sweep (contrast wafv2's session, which found several harmless
fabricated fields in the sibling service; waf itself has none). No
credential/ARN-bearing field exists in any WAF Classic response type at
all.

**Discarded input**: none beyond the already-covered, genuinely-vestigial
`GetRateBasedRuleManagedKeys` `NextMarker` case above. Spot-checked every
Create/Update op's request struct against its real `serializeOp*Input`
field list (`CreateWebACL`/`UpdateWebACL`/`CreateRule`/`UpdateRule`/
`CreateRuleGroup`/`UpdateRuleGroup`/`CreateIPSet`/`UpdateIPSet`/
`CreateRateBasedRule`/`UpdateRateBasedRule`/`PutLoggingConfiguration`/
`PutPermissionPolicy`/`CreateWebACLMigrationStack`) — every field the real
input carries is read and threaded through to the backend; `CreateIPSet`
correctly does *not* accept `IPSetDescriptors` (real
`CreateIPSetInput` has no such member either — descriptors are added only
via `UpdateIPSet`, confirmed against `api_op_CreateIPSet.go`).

**Real-client test ratio: 1 of 90 test functions (about 1.1%) drives a real
SDK client end-to-end** (`TestCreateOps_TagsRoundTrip` in
`handler_create_tags_test.go`). `TestSDKCompleteness`
(`sdk_completeness_test.go`) also imports `wafsdk` but only reflects over
its method set for op-name completeness — it never sends a request or
decodes a response, so it doesn't count toward wire-shape coverage. Every
other test in the other 24 files calls the handler directly or decodes into
gopherstack's own request/response structs, which cannot detect a wrong
wire key by construction. This is the same "worst yet" territory as ce's
1.4% and mwaa's 0% — despite this session's read coming back clean, the
suite itself offers almost no defense against a future wire-shape
regression here.

**Ratifying tests**: not applicable — no bug was found for one to ratify.
Existing tests were read for the ratifying-test check anyway (in case one
of them asserted a shape gopherstack doesn't actually emit, which would
itself be a symptom of a missed bug) — none did.

**Phantom ops**: none. `TestSDKCompleteness` (existing, passed before and
after this session, since nothing changed) already confirms every op in
`GetSupportedOperations()` is either a real `wafsdk.Client` method or
explicitly listed as not implemented (the `notImplemented` slice is empty —
all ops implemented).

**False-positive rate**: n/a — zero findings reported, so there is nothing
to be a false positive. Every "checked, clean" claim above cites the real
`deserializeOpDocument<Type>Output`/`deserializeDocument<Type>`/
`serializeOpDocument<Type>Input` function or the real `api_op_*.go` doc
comment, file+line or file name, never an assumption.

**No fixes, so nothing to hand-revert.** `go build`/`go vet`/`go test
-race` all green for `services/waf` with zero changes made (sanity-checked
the existing baseline rather than skipping verification just because
nothing changed). No `golangci-lint`/`go fix -diff` run since there is no
diff to lint — matches this campaign's established precedent for a
clean-sweep batch with zero code changes (identitystore/
resourcegroupstaggingapi/servicediscovery, sqs/sns).

Per this session's hard constraints: no subagents used (Read/Grep/Bash
only), no git-mutating commands run (moot — no code changes made, only
this remainder file was edited), `services/vpclattice` (a sibling session's
territory, discovered mid-session via `git status`, 10 files modified)
confirmed untouched throughout; no `gendocs`/`make docs` run.

waf's List/Describe/Get families are now fully swept for this issue (34/34
ops verified against the real deserializer/serializer, independently of
either of the two secondhand "already clean" claims this session found and
declined to trust). 72 of 162 services swept, 90 remain. Per the ranked
table, vpclattice (30, `direct`) is next largest but is the live sibling's
own territory (confirmed via `git status`); eventbridge (30, `direct`) or
emr (30, `direct`) are the next candidates that don't collide — re-check
`git status` for live sibling territory before picking either.

## vpclattice (this session)

Chosen as the largest unswept service that no live sibling held: `ce`
(uncommitted modified files, confirmed via `git status`) and `waf` (per this
session's brief, and independently confirmed by waf's own session notes
above, which found vpclattice's in-progress edits and picked waf instead)
were both occupied. `waf` (34 L+D+G) is nominally larger than vpclattice's
30, but was off-limits; of the remaining 30-tier candidates (vpclattice,
eventbridge, emr, route53resolver, all 30), vpclattice was picked first and
turned out self-contained (single-protocol REST/JSON, no cross-service
split).

PROTOCOL: confirmed `awsRestjson1_` from vpclattice@v1.25.5's
`deserializers.go` function-prefix grep (only prefix present). Case-sensitive
body-field switches (`switch key { case "...": }`), confirmed by inspecting
`ListAccessLogSubscriptions`'/`GetAccessLogSubscription`'s deserializers
directly. All 400 `EqualFold` hits in `deserializers.go` are `errorCode`
matches inside the per-op `deserializeOpError*` functions (spot-checked the
first 5 and the last 5) — none are in a body-field switch, so casing is a
non-issue by construction here, like guardduty/pinpoint before it.

**Second client**: none. Only `vpclatticesdk "github.com/aws/aws-sdk-go-v2/service/vpclattice"`
is imported anywhere in this repo outside `services/vpclattice` itself
(cli.go, gendocs, teststack, and the terraform/integration test suites all
use the one client).

**Dead-deserializer trap checked and found NOT to apply** — traced
`ListServices`'s generated `HandleDeserialize` (deserializers.go:10816)
directly: it decodes the body into `shape` and calls
`awsRestjson1_deserializeOpDocumentListServicesOutput(&output, shape)`
itself (deserializers.go:10861); no dead `OpDocument...Output` wrapper sits
unreached between them for this op, and the same pattern was confirmed for
`GetAccessLogSubscription`/`ListAccessLogSubscriptions` before trusting any
other op's `OpDocument...Output` case list as the real, reached
deserializer.

Read all 30 L+D+G ops' response shapes against their own
`awsRestjson1_deserializeOpDocument<Op>Output`/`deserializeDocument<Type>`
case lists (file+line via per-op grep dumps), plus the paired
`serializeOpDocument*Input` functions for every op whose request side was
touched by a fix.

**Sibling/version pairs**: no V1/V2 pair exists for vpclattice itself, but
one clear in-file sibling trap was found and fixed (`RuleAction`'s
`ForwardAction`/`FixedResponseAction` handling was already fully correct —
`ruleActionToJSON`/`extractRuleAction` match the real union shape exactly —
while `RuleMatch`'s `PathMatch` handling, in the very same function
(`ruleMatchToJSON`), used the wrong wrapper key. The correct sibling
(`action`) sat right next to the broken one (`match`) in the same file with
no cross-reference between them). Also reported as already-correct:
`Target`/`TargetSummary`/`TargetFailure` (targets.go),
`ListTagsForResource` (tags.go), `Listener`/`ListenerSummary`
(listeners.go), `ServiceNetworkVpcAssociation` family, `ResourceEndpointAssociation`/
`ServiceNetworkVpcEndpointAssociation` (deliberately, honestly empty —
this backend has no EC2 VPC-endpoint cross-service modeling, matching the
real API's "AWS auto-creates these, vpc-lattice itself has no Create op"
shape; already correctly documented in-code before this session).

**9 real bugs found and fixed, spanning all three layers:**

1. **`AccessLogSubscription`/`AccessLogSubscriptionSummary` —
   `serviceNetworkLogType` tracked by the backend on every create but never
   emitted by either `GetAccessLogSubscription` or
   `ListAccessLogSubscriptions`** (real, non-required member on both
   `GetAccessLogSubscriptionOutput` and `types.AccessLogSubscriptionSummary`,
   deserializers.go). `AccessLogSubscriptionSummary` didn't even have a
   struct field for it. Fixed both.
2. **`RuleMatch.PathMatch` — wrapper-key bug, broken in both directions,
   total functional loss.** `extractPathMatch` (request) and
   `ruleMatchToJSON` (response) both used `"path"`; the real wire key on
   both sides is `"pathMatch"` (serializers.go:6541,
   `awsRestjson1_serializeDocumentHttpMatch`; confirmed same key on the
   response deserializer). A real client's path-match rule condition was
   silently discarded on create (gopherstack never recognized `"pathMatch"`
   in the request) and never echoed back on Get/List regardless. This is
   the flagship "wrong key AND it breaks the write path too" finding this
   session, sitting beside the already-correct `RuleAction` sibling in the
   same function.
3. **`HeaderMatch.CaseSensitive`/new `RuleMatch.PathCaseSensitive` — real
   fields, completely unwired on both sides.** `HeaderMatch.CaseSensitive`
   existed on the struct but neither `extractHeaderMatches` (request) nor
   `ruleMatchToJSON` (response) touched it; `PathMatch.CaseSensitive` had no
   backing field in `RuleMatch` at all. Confirmed real, same-key
   (`"caseSensitive"`) on both request and response for both `HeaderMatch`
   and `PathMatch` (serializers.go:6408/6582). Fixed by wiring both
   directions for `HeaderMatch` and adding the missing field + wiring for
   `PathMatch`.
4. **`ListServiceNetworks` — association counts always 0.**
   `NumberOfAssociatedServices`/`NumberOfAssociatedVPCs` were computed
   fresh only inside `GetServiceNetwork` (mutating the returned struct);
   `ListServiceNetworks`'s `toSummary()` never recomputed them, so every
   list item reported 0 regardless of real associations even though
   `GetServiceNetwork` on the identical object reported correctly. Fixed by
   computing `countSNSAs`/`countSNVAs` in the List loop too, without relying
   on `GetServiceNetwork`'s side-effecting mutation (which also mutates a
   shared stored pointer under an `RLock` — a pre-existing, separate
   concurrency wart, flagged here but not fixed since it's outside this
   issue's wire-shape scope).
5. **`ServiceNetworkSummary` missing `numberOfAssociatedResourceConfigurations`
   entirely.** Real, non-required `ServiceNetworkSummary`-only member (not
   on Get, confirmed by comparing both real deserializer case lists). The
   backend already had `countSNRAs()`, used only for a delete-precondition
   check, never wired to the wire. Fixed: added the field, wired into the
   same `ListServiceNetworks` loop as #4.
6. **`ServiceSummary` missing `lastUpdatedAt`.** Tracked
   (`ServiceSummary.LastUpdatedAt`, already correctly emitted by
   `GetService`'s `serviceToJSON`) but `serviceSummaryToJSON` never emitted
   it — every `ListServices` item had a nil `LastUpdatedAt` for a real
   client regardless of backend state.
7. **`HealthCheckConfig` — `protocolVersion` never echoed, `matcher`
   (`Matcher.HttpCode`) completely unwired on both sides.**
   `HealthCheckConfig.ProtocolVersion` was parsed on create/update but
   `healthCheckToJSON` never emitted it back. `MatcherHTTPCode` had a
   struct field with no request-parsing or response-emitting code at all.
   Confirmed real wire shape `{"matcher": {"httpCode": "..."}}` both
   directions (serializers.go:6489-6494). Fixed both.
8. **`ResourceConfigurationSummary` missing
   `customDomainName`/`groupDomain`/`domainVerificationId`/
   `resourceConfigurationGroupId` entirely** — all four real,
   non-required `ResourceConfigurationSummary` members
   (deserializers.go), all four already present on
   `ResourceConfiguration`/`GetResourceConfigurationOutput` and correctly
   emitted there (except `domainVerificationId`, see #9). The struct had no
   fields for them and `toSummary()` dropped them. Fixed: added fields,
   wired `toSummary()`, extracted the inline `ListResourceConfigurations`
   map into a shared `resourceConfigurationSummaryToJSON` helper mirroring
   `resourceConfigurationToJSON`'s existing conditional-emit pattern.
9. **`CreateResourceConfiguration` discarded three real, directly-settable
   request members entirely: `customDomainName`/
   `domainVerificationIdentifier`/`groupDomain`** (confirmed against
   `CreateResourceConfigurationInput`'s real fields and
   `serializers.go:433/438/443` — `awsRestjson1_serializeOpDocumentCreateResourceConfigurationInput`).
   `handleCreateResourceConfiguration` never read any of the three from the
   body at all, so a real client supplying them had them silently dropped
   — the same "discarded input" shape this issue's brief called out for
   apigatewayv2's `CreateProductPage`. This also explains why `GroupDomain`
   looked permanently unreachable at first: a GROUP-type resource
   configuration's own `GroupDomain` was never settable at all, so every
   CHILD that later inherited it also got `""`. Fixed by threading all
   three through `CreateResourceConfiguration`'s backend signature
   (`groupDomain`, when explicitly given, wins; otherwise CHILD still
   inherits its GROUP parent's value, unchanged). **Also found and fixed
   along the way**: `GetResourceConfiguration`'s own `resourceConfigurationToJSON`
   never emitted `domainVerificationId` either (a distinct omission from
   #8's List-only gap, caught only once the round-trip test exercised Get
   after fixing the Create-side discard) — the real
   `GetResourceConfigurationOutput` always includes it (non-required).

**Over-wide/secret/ARN fields**: none found. No response in this service
carries a secret, credential, or an ARN the caller couldn't already derive
from the resource it just created/looked up.

**Backend-tracked-but-unemitted (lead question 2) hits**: #1, #4, #5, #6,
and #9's `domainVerificationId` gap on Get — five separate instances of
"the backend already had the value on hand and simply never wrote it to
the response," the most of any single layer this session.

**Real-client test ratio**: 1 of 52 existing test functions in
`services/vpclattice` drove a real SDK client end-to-end before this
session (`TestGetService_UnknownServiceSurfacesResourceNotFoundException`
in `handler_error_type_test.go`, via its `newTestVPCLatticeClient` helper).
`TestSDKCompleteness` also imports the real SDK package but only reflects
over its method set for op-name completeness, same non-count as every
other service's `TestSDKCompleteness`. The other 50 test functions drive
the handler directly over raw `map[string]any` bodies/responses, which by
construction cannot catch a wrong wire key. This session added 7 more
real-client tests, bringing it to roughly 8 of 59 (~14%).

**Ratifying test found and fixed**: 1 — `TestRule_CRUD` built its
`CreateRule` request body with `"path": {"match": {"exact": "/api"}}`
(the pre-fix bug's own key) and never asserted the match round-tripped on
the follow-up `GetRule`, so it passed cleanly against broken code purely
because it never checked. Rewritten to use the real `"pathMatch"` key with
`"caseSensitive": true` and to assert the full match structure survives
the round trip through `GetRule`.

**Phantom ops**: none. All 73 op-name string literals in
`GetSupportedOperations` (74 including the `opUnknown` sentinel) map to a
real `api_op_<Name>.go` file in vpclattice@v1.25.5, verified by script
against every `op*` constant in `handler.go`.

**False-positive rate**: 0 among reported bugs — every finding cites the
real `deserializeOpDocument<Type>Output`/`deserializeDocument<Type>`/
`serializeOpDocument*Input` function actually reached from that op's own
`HandleDeserialize`, file+line, never a doc comment or an assumption.

**Disclosed, not fixed** (structural gaps needing new backend modeling this
session judged too speculative to fabricate, each independently verified
absent from the backend's tracked state):
- `ServiceNetworkServiceAssociation`/`ServiceNetworkVpcAssociation`/
  `ServiceNetworkResourceAssociation`'s `failureCode`/`failureMessage`
  (and SNRA's `domainVerificationStatus`/`isManagedAssociation`/
  `privateDnsEntry`/`dnsEntry`) — no failure-state or managed-association
  simulation anywhere in this backend; every association reaches ACTIVE
  deterministically.
- `Service`'s `idleTimeoutSeconds`/`failureCode`/`failureMessage` — no
  backing field or create/update parameter for any of the three.
- `ServiceNetwork`'s `sharingConfig` — no RAM/cross-account-sharing model.
- `ResourceGateway`'s `managedBy`/`serviceManaged` — no
  ownership/ManagedBy-Firewall-Manager-style concept; every gateway here is
  self-managed by construction, which is what these fields would say
  anyway, but synthesizing the exact enum/bool without a real source felt
  like more invention than the gap warranted for a one-session pass.
- `RuleGroup`... n/a (vpclattice has no `RuleGroup` type; not to be
  confused with wafv2's finding in the same file this session).
- `DomainVerification`/`DomainVerificationSummary`'s `tags`/
  `txtMethodConfig` — no TXT-record verification-detail modeling.
  `lastVerifiedTime` **was** fixed on the List side (`handleListDomainVerifications`'s
  inline map never emitted it despite `GetDomainVerification` already doing
  so conditionally) but stays genuinely untested: nothing in this backend
  ever sets `LastVerifiedTime` on any Create/Update path (always nil), so
  the fix is a structural correctness match against Get's existing
  conditional-emit pattern, not something a black-box test can currently
  observe as non-nil.
- `ResourceConfigurationSummary`/`ResourceConfiguration`'s `amazonManaged` —
  no AWS-managed resource-configuration concept in this backend (every
  resource configuration here is user-created).

Tests: 7 new real-SDK-client tests in the new
`services/vpclattice/wire_field_fixes_test.go`
(`TestAccessLogSubscription_ServiceNetworkLogType`,
`TestRule_PathMatchWireKeyAndCaseSensitive`,
`TestListServiceNetworks_AssociationCounts`, `TestListServices_LastUpdatedAt`,
`TestTargetGroup_HealthCheck_ProtocolVersionAndMatcher`,
`TestListResourceConfigurations_GroupId`,
`TestResourceConfiguration_CustomDomainNameAndDomainVerificationId`), plus
the `TestRule_CRUD` ratifying-test rewrite in `handler_rules_test.go`. Every
fix hand-reverted individually (no git, per this session's hard
no-git-mutation constraint — reverts done by re-editing the exact prior
text and restoring it afterward, diffed byte-identical against the
pre-revert file each time), confirmed to fail with the exact predicted
symptom quoted in-code above (empty `ServiceNetworkLogType`, nil
`PathMatch`, both association counts falling back to 0, nil `LastUpdatedAt`,
empty `ProtocolVersion`/missing `Matcher`, empty `GroupDomain`/
`CustomDomainName`/`DomainVerificationId`) before being restored.
`resource_gateway_family_test.go`'s one direct backend call site was updated
for `CreateResourceConfiguration`'s three new trailing parameters (no
behavior change, positional-argument-count only).

Gates: `go build`/`go vet`/`go test -race` (scoped to `services/vpclattice`),
`go fix -diff` (no diff), `golangci-lint run` (0 issues after a `golines`
reformat on one new test's line length; no cyclop/gocyclo/gocognit/funlen
nolints added or present) all green. `go test -race ./pkgs/...` green.

Per this session's hard constraints: no subagents used (Read/Grep/Bash
only), no git-mutating commands run (all `services/vpclattice` changes
uncommitted — orchestrator must commit/push); `services/ce` (a live
sibling's uncommitted work, confirmed via `git status` at the start) and
`services/waf` (per this session's brief, independently confirmed by waf's
own session notes found in this same file) both confirmed untouched
throughout; two new commits (`baa7502c3` ce, `b0f93c529` waf) landed on
this branch mid-session from those sibling sessions — noticed via `git log`,
did not conflict with or require touching anything outside
`services/vpclattice`; no `gendocs`/`make docs` run.

vpclattice's List/Describe/Get families are now fully swept for this issue
(30/30 ops verified against the real deserializer/serializer). 73 of 162
services swept, 89 remain. Per the ranked table, eventbridge and emr (30
each, `direct`) are the next candidates — re-check `git status` for live
sibling territory before picking either.
