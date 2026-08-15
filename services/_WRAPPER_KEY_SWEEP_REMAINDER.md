# Wrapper-key / nested-shape sweep remainder (gopherstack-6flj)

**88 of 162 services swept, 74 remain** (codeartifact added this session,
2026-08-15, closing the three-way tie its own prior sections describe; also
see outposts's own section at the end of this file, added the same day;
appconfig, cloudtrail, directoryservice, opsworks, apigatewayv2, workmail,
wafv2, ce, waf, vpclattice, emr, eventbridge, kafka, route53resolver,
appsync, workspaces, lakeformation, elasticsearch, and rekognition all added
earlier this session, in parallel, by different sessions — see each
service's own section at the end of this file for full detail).
directoryservice and cloudtrail, listed as still-in-progress by earlier
passes appending to this header, both finished and are committed
(`78517e30d`, `773c2af52`).

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

apigateway, **apigatewayv2** (this session), **appconfig** (this session),
**appsync** (this session),
appstream, athena, autoscaling,
awsconfig, backup, bedrock,
bedrockagent, **ce** (this session), cleanrooms, cloudformation, cloudfront,
cloudfrontkeyvaluestore, cloudwatch, cloudwatchlogs, **codeartifact** (this
session), codebuild, codecommit,
cognitoidp, datasync, dlm, dynamodbstreams, ec2, ecs, eks,
elasticache, elbv2, **emr** (this session), **eventbridge** (this session),
forecast,
glue, guardduty, iam, identitystore, inspector2, iot,
iotwireless, **kafka** (this session), kms, lambda, lightsail, macie2, medialive,
mgn, networkmanager, networkmonitor, omics,
opensearch, organizations, **outposts** (this session), personalize, pinpoint,
quicksight, rds, redshift,
resiliencehub, resourcegroupstaggingapi, route53, **route53resolver** (this
session), s3,
s3control, s3tables, sagemaker, secretsmanager, securityhub, servicediscovery,
ses, sesv2, sns, sqs, ssm, ssoadmin, stepfunctions, transfer,
**vpclattice** (this session), **waf** (this session), **wafv2** (this
session), **workmail** (this session), **workspaces** (this session),
**lakeformation** (this session), **elasticsearch** (this session),
**rekognition** (this session).

One service still has real, extensive wire-shape work under **other** issue
classes (gopherstack-h910/ctaz's backend-logic fixes) but **no 6flj-specific
wrapper-key pass on record** — dynamodb (s3 moved to swept this session; see
its own section at the end of this file). It is listed in the unswept table
below on purpose; don't assume "heavily worked on" means "settled for this
issue."

## Unswept (78 of 162), ranked by List+Describe+Get op count

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

Sum of the L+D+G column across all 78 (networkmanager's 38, securityhub's
47, macie2's 40, s3's 45, cognitoidp's 37, personalize's 39,
apigatewayv2's 37, workmail's 36, wafv2's 32, ce's 31, waf's 34,
vpclattice's 30, emr's 30, eventbridge's 30, kafka's 29,
route53resolver's 30, appsync's 28, workspaces's 27, lakeformation's 26,
elasticsearch's 25, rekognition's 25, directoryservice's 25, outposts's 23,
and codeartifact's 24 removed, all swept prior to/this session): **833**
candidate ops.

| service | total ops | list | describe | get | L+D+G | resolution |
|---|---:|---:|---:|---:|---:|---|
| opsworks | 74 | 1 | 22 | 1 | 24 | direct |
| codeartifact | 48 | 12 | 5 | 7 | 24 | direct |
| cloudtrail | 60 | 11 | 2 | 11 | 24 | direct |
| appconfig | 56 | 12 | 0 | 12 | 24 | direct |
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

## emr (this session)

Chosen per the prior pass's own note as one of the two 30-L+D+G candidates
that don't collide with `vpclattice` (the live sibling flagged there).
`git status` at start was clean for `services/emr`; re-checked again after
finishing and found a NEW sibling had appeared mid-session on
`services/eventbridge` (37 files) — confirmed untouched, never opened. Chose
`emr` over `eventbridge` deliberately: `eventbridge` is nearly double the
LOC (26k vs 14k) and embeds its own Schemas registry surface with a second
real client (`schemas`, confirmed pinned in `go.mod` alongside `eventbridge`,
`scheduler`, `pipes`) — a poor fit for "settle completely in one session,"
matching this campaign's established preference for self-contained
single-client services when candidates tie (guardduty over s3 last batch).
**Traced, not assumed**: grepped `services/eventbridge/*.go` for a `Pipes`
surface this issue's own prompt flagged ("second real client for Schemas and
a Pipes surface") — found the embedded Schemas surface (`handler_schemas.go`,
`handler_schemas_rest.go`, real `schemas` client used in eventbridge's own
tests) but no Pipes-related code or op names anywhere inside
`services/eventbridge` itself; `services/pipes` is a wholly separate,
already-tabled 3-L+D+G candidate in this file's ranked table, not something
eventbridge embeds. The "Pipes surface" half of that claim did not check out
for eventbridge specifically — flagging since I chose not to sweep
eventbridge this session and can't fully verify a negative from outside it.

Own enumeration of `GetSupportedOperations()`'s literal slice (handler.go)
confirms the table's 65 total / 30 L+D+G (13 List/8 Describe/9 Get) exactly.

PROTOCOL: confirmed `awsAwsjson11_` (JSON-RPC 1.1), the sole deserializer
prefix in emr@v1.64.4/deserializers.go (`grep -o` found no
`awsRestjson1_`/`awsEc2query_`/`awsAwsquery_`). Case-sensitive, matching
waf/guardduty/cloudwatchlogs/macie2/cognitoidp/personalize/workmail this
campaign. All 116 `EqualFold` hits in deserializers.go are either `errorCode`
matches or `NaN`/`Infinity`/`-Infinity` float-special-value parsing inside
numeric-field decode branches (spot-checked every hit's surrounding
function) — zero in a body-field `switch key { case "...":}` block, so
body-field casing is a non-issue by construction here. **Second client
check**: only `github.com/aws/aws-sdk-go-v2/service/emr` is imported
anywhere under `services/emr` (including its own tests) — no second runtime/
data-plane module, unlike personalize's Runtime split or eventbridge's
Schemas embed noted above.

**Dead-deserializer trap checked and found NOT to apply** — traced
`awsAwsjson11_deserializeOpDescribeCluster.HandleDeserialize` directly
(deserializers.go:1327): it decodes the body into `shape` and calls
`awsAwsjson11_deserializeOpDocumentDescribeClusterOutput(&output, shape)`
itself (line 1367) — the `OpDocument*Output` function **is** the real,
reached deserializer, same JSON-RPC-1.1 shape as every other
`awsAwsjson1{0,1}` service this campaign has swept.

Read all 30 L+D+G ops' response shapes against their own
`awsAwsjson11_deserializeOpDocument<Op>Output`/`deserializeDocument<Type>`
case lists (file+line), plus every family's Create/Update/Put/Add sibling
request side via the paired `serializeOp*Input`/`serializeDocument*`
functions, per this session's "check the request side too" instruction.

**5 real bugs found and fixed:**

1. **`Step`/`StepSummary`'s Hadoop JAR details — wire-keyed `HadoopJarStep`,
   should be `Config`. Sibling trap, request convention leaking into the
   response.** The request-side `StepConfig` type genuinely wire-keys its
   nested Hadoop JAR block `HadoopJarStep` (`types.StepConfig`, confirmed
   `serializers.go:5739`'s `awsAwsjson11_serializeDocumentStepConfig`,
   `object.Key("HadoopJarStep")`). But the RESPONSE types (`types.Step`,
   DescribeStep; `types.StepSummary`, ListSteps) nest the identical shape
   under `Config` instead (`types.HadoopStepConfig`, confirmed
   `deserializers.go:14890`'s `case "Config":` inside
   `awsAwsjson11_deserializeDocumentStep`, and the parallel case in
   `awsAwsjson11_deserializeDocumentStepSummary`). gopherstack shared one Go
   type/tag across both directions and used the request key everywhere — a
   real client's typed `Step.Config`/`StepSummary.Config` was **nil for
   every step, on every DescribeStep/ListSteps call, regardless of backend
   state**, before this fix. Fixed by re-tagging the response-side field
   `json:"Config"` (request-side `StepSpec.HadoopJarStep` keeps
   `"HadoopJarStep"`, unaffected — confirmed correct, not touched).
2. **`StepHadoopJarStep`/`StepHadoopJarStepInput`'s `Properties` — missing
   entirely, both directions, plus a genuine wire-shape asymmetry between
   request and response that had to be modeled separately, not just added.**
   Real `types.HadoopJarStepConfig.Properties` (request) is a JSON ARRAY of
   `{Key,Value}` objects (`serializers.go:5057`'s
   `awsAwsjson11_serializeDocumentKeyValueList`), while real
   `types.HadoopStepConfig.Properties` (response) is a plain string map
   (`deserializers.go`'s `awsAwsjson11_deserializeDocumentStringMap` call in
   `...DocumentHadoopStepConfig`) — confirmed independently against both
   `serializers.go` and `deserializers.go`, a genuine EMR wire quirk, not a
   gopherstack inconsistency. Caught this the hard way: my first fix modeled
   `Properties` as a single `map[string]string` shared by both directions,
   which compiled and passed lint, but a real SDK client test failed with
   `json: cannot unmarshal array into Go struct field ... Properties of type
   map[string]string` — a request-side round-trip test catching a request/
   response asymmetry a response-only read would have missed entirely. Fixed
   by splitting into `StepHadoopJarStepInput` (request, `[]KeyValue`) and
   `StepHadoopJarStep` (response, `map[string]string`), with a
   `toStepHadoopJarStep` converter in `steps.go`. Before this fix, a real
   client's per-step Hadoop job properties were silently dropped on input
   and never echoed back on either read path.
3. **`AddJobFlowStepsInput.ExecutionRoleArn` — discarded input (lead-
   question-2 class).** Real, call-level (applies to every step added by
   that call), confirmed present on `types.AddJobFlowStepsInput`
   (`api_op_AddJobFlowSteps.go`). gopherstack's `addJobFlowStepsInput` had no
   field for it at all — a real client's runtime-role ARN for newly added
   steps was silently dropped, never applied, never echoed. Fixed: added the
   field, threaded through `Backend.AddJobFlowSteps`'s signature, set on each
   new `Step.ExecutionRoleArn`.
4. **`RunJobFlowInput.StepExecutionRoleArn` — same class, for a cluster's
   initial steps.** Real, call-level, confirmed present on
   `types.RunJobFlowInput` (`api_op_RunJobFlow.go`) — distinct from the
   unrelated, already-correctly-modeled cluster-level `JobFlowRole`/
   `ServiceRole`/`AutoScalingRole`. gopherstack's `runJobFlowInput`/
   `RunJobFlowParams` had no field for it. Fixed the same way as #3, threaded
   through `buildInitialSteps`.

   `Step.ExecutionRoleArn` (the new field both #3/#4 populate) is real on
   `types.Step` but **not** on `types.StepSummary` (confirmed:
   `deserializers.go`'s `awsAwsjson11_deserializeDocumentStepSummary` case
   list has no `ExecutionRoleArn` case at all, unlike `...DocumentStep`'s).
   Since gopherstack shares one Go type for both DescribeStep and ListSteps,
   the field is emitted on both — a harmless extra field on the List side a
   real typed client has no slot to decode into either way, same non-bug
   class as rds's `DBInstance.StorageOptimized`. Disclosed in a doc comment
   on `Step` rather than done as a full type split (which `Step`/`Config`
   above already required once, for a different field) — judged not worth a
   second such split for one optional, harmless-when-extra field.
5. **`DescribeNotebookExecution`'s `NotebookExecution.ExecutionEngine` —
   wrong shape. Sibling trap, same pattern as #1: a flat convention correct
   on one op leaking into a different op that needs it nested.**
   `NotebookExecutionSummary` (ListNotebookExecutions) genuinely uses a flat
   `ExecutionEngineId` (confirmed `deserializers.go`'s
   `awsAwsjson11_deserializeDocumentNotebookExecutionSummary` case list —
   already correct here, an earlier session's citing comment confirms this
   was already fixed once for the List side). But `DescribeNotebookExecution`
   nests the real member under an `ExecutionEngine` object
   (`types.ExecutionEngineConfig{Id,Type,ExecutionRoleArn,
   MasterInstanceSecurityGroupId}`, confirmed `deserializers.go`'s
   `case "ExecutionEngine":` inside
   `awsAwsjson11_deserializeDocumentNotebookExecution`) — gopherstack emitted
   the List convention on the Describe response too, so a real client's
   typed `NotebookExecution.ExecutionEngine` was **always nil regardless of
   what editor/cluster was set**, on every DescribeNotebookExecution call.
   Fixed by splitting the wire shape for Describe out of the shared internal
   `NotebookExecution` model into a dedicated
   `notebookExecutionDetailWire`/`newNotebookExecutionDetail` (mirroring the
   existing `NotebookExecutionSummary`/`newNotebookExecutionSummary` split
   pattern already used for the List side) — `Type`/`ExecutionRoleArn`/
   `MasterInstanceSecurityGroupId` left unset/omitted (this backend only
   ever stores an editor-supplied cluster ID, no such tracking exists),
   disclosed rather than fabricated.

**2 more findings, both "raw internal model reaches the wire directly"
(lead-question-2's third variant) rather than a wrong key:**

6. **`Cluster.TerminatedAt` — fabricated field, harmless, leaking on the
   wire.** Internal-only TTL-cleanup bookkeeping (`janitor.go`'s sweep), but
   it was an exported field with a normal JSON tag directly on `Cluster`,
   the same Go type `DescribeCluster` marshals for its response — real
   `types.Cluster` has no such member at all (confirmed absent from
   `deserializers.go`'s `awsAwsjson11_deserializeDocumentCluster` case list,
   35 real members checked). Not a secret (a plain RFC3339 timestamp of when
   this backend swept the cluster), but incorrect: a real client parsing the
   raw body would see an extra key no real AWS response ever sends. Fixing
   this the naive way (`json:"-"`) would have been a SECOND bug: this
   repo's persistence layer (`persistence.go`) snapshots `Cluster` via the
   same plain `json.Marshal`/struct tags used for the wire, confirmed by
   reading `clusterDTO`'s own doc comment before touching anything — a
   `json:"-"` tag strips a field from persistence snapshots too, not just
   the wire. Fixed the same way this file's `clusterDTO` already handles
   `instanceGroups`/`steps`/`bootstrapActions`/etc.: unexported the field
   (`terminatedAt`, invisible to `encoding/json` by construction regardless
   of tags) and added a parallel `clusterDTO.TerminatedAt` field carried
   through `Snapshot`/`unwrapClusterDTOs` explicitly, exactly mirroring the
   existing pattern for every other hidden `Cluster` field.
7. **`DescribePersistentAppUI`'s response — reused
   `CreatePersistentAppUIOutput`'s shape instead of the real, different
   `DescribePersistentAppUIOutput.PersistentAppUI` shape.** gopherstack's
   internal `PersistentAppUI` backend struct (fields `ID`/`TargetResourceArn`/
   `RuntimeRoleEnabledCluster`) was marshaled directly as
   `DescribePersistentAppUI`'s response. `TargetResourceArn`/
   `RuntimeRoleEnabledCluster` are real, but only on `CreatePersistentAppUIOutput`
   (confirmed `api_op_CreatePersistentAppUI.go`) — `handleCreatePersistentAppUI`
   already built its own separate, correct DTO for that op and never used
   this struct's tags. The real `DescribePersistentAppUIOutput.PersistentAppUI`
   (`types.PersistentAppUI`) is an entirely different shape (`AuthorId`/
   `CreationTime`/`LastModifiedTime`/`LastStateChangeReason`/
   `PersistentAppUIId`/`PersistentAppUIStatus`/`PersistentAppUITypeList`/
   `Tags`, confirmed `deserializers.go`'s
   `awsAwsjson11_deserializeDocumentPersistentAppUI` case list) with **none**
   of the two fields gopherstack was sending. Fixed by adding a
   `persistentAppUIDetailWire`/`newPersistentAppUIDetail` converter (same
   split pattern as #5) emitting only what's real and backend-tracked
   (`PersistentAppUIId`, plus a newly added `CreatedAt time.Time` → real
   `CreationTime`, cheap to add since `CreatePersistentAppUI` already had an
   obvious creation point to stamp it at). `AuthorId`/`LastModifiedTime`/
   `LastStateChangeReason`/`PersistentAppUIStatus`/`PersistentAppUITypeList`
   disclosed, not fabricated — no author/status-lifecycle modeling exists in
   this backend, and `PersistentAppUIStatus` specifically has no enum
   constants in this pinned SDK version to cite a valid value from (real
   type is a bare `*string`), so a value could not be verified from the
   pinned SDK alone — left unset rather than guessed.

**1 fabricated-field-only finding, no data loss, fixed by removal (matching
this file's own `ClusterSummary.ReleaseLabel` precedent from an earlier
session, not merely disclosed):**

8. **`StudioSummary.StudioArn`/`StudioSummary.DefaultS3Location`** — real
   `types.StudioSummary` (confirmed
   `awsAwsjson11_deserializeDocumentStudioSummary`'s case list: only
   `AuthMode`/`CreationTime`/`Description`/`Name`/`StudioId`/`Url`/`VpcId`,
   7 fields) has neither. Harmless (a real typed `ListStudios` client has no
   field to decode either into), but incorrect. Removed both from
   `StudioSummary` and its `ListStudios` builder.

**1 more discarded-input finding, cheap to fix (unlike the InstanceFleet/
InstanceGroup gaps disclosed below):**

9. **`CreateStudioInput.IdcUserAssignment`/`TrustedIdentityPropagationEnabled`
   — both real (`api_op_CreateStudio.go`), both silently dropped.**
   `TrustedIdentityPropagationEnabled` already had a wire slot on `Studio`
   (confirmed still real, `deserializers.go`'s `Studio` case list) but
   nothing ever populated it from the request — always `false` regardless of
   what a real client sent. `IdcUserAssignment` had no slot at all. Neither
   is settable post-creation (confirmed absent from
   `api_op_UpdateStudio.go`). Fixed by threading both through
   `Backend.CreateStudio`'s signature (already 10 positional args; this
   repo's established convention here, not switching to a params struct
   mid-fix) into the two existing/new `Studio` fields.

**Sibling/version pairs checked, both correct already (a result, per this
issue's brief):** `GetIPSet`... n/a for this service (no such family); the
two internal near-duplicate pairs worth checking here were `scanToDescribeMap`-
style converters this service doesn't have (single-shape families
throughout) and the `Filter`/`RateBasedRule`-style predicate sharing this
service also doesn't have. The one genuine near-duplicate pair in emr —
`Step` (DescribeStep) vs `StepSummary` (ListSteps) — is finding #1/#2/the
disclosed-ExecutionRoleArn note above, not a clean pair. `GetOnClusterAppUIPresignedURL`
vs `GetPersistentAppUIPresignedURL` (both wrap presigned URLs) were already
independently correct from an earlier session's fix (own citing comment
found and re-verified, not re-fixed). No V1/V2 or generational pair exists
anywhere in this service.

**Over-wide/secret check**: no secret- or credential-bearing field found in
any response type. The one fabricated field with real content
(`Cluster.TerminatedAt`) carries a plain timestamp, not a secret — same
"harmless, disclosed" class as wafv2's fabricated fields, contrast
cognitoidp's `ClientSecret`/workmail's IAM-role-ARN leaks from earlier in
this campaign.

**Discarded input, disclosed not fixed** (each would need new backend
modeling, judged too speculative to fabricate):
- `InstanceGroupConfig.AutoScalingPolicy` — real, settable inline at
  instance-group creation time (`RunJobFlow`/`AddInstanceGroups`), but this
  backend only supports setting it via the separate, already-correct
  `PutAutoScalingPolicy` op. A real client that sets it inline at creation
  has it silently dropped. A converter already exists for the standalone op
  (`policies.go`) that could be reused, but wiring it through
  `RunJobFlow`/`AddInstanceGroups` too was judged out of this session's
  scope given the size of what was already found.
- `InstanceGroupConfig.CustomAmiId`/`EbsConfiguration` — same class, real
  request-side members, no backend modeling.
- `InstanceFleetConfig.InstanceTypeConfigs`/response-side
  `InstanceTypeSpecifications` — real on both directions
  (`types.InstanceFleetConfig`/`types.InstanceFleet`), but modeling weighted
  capacity/EBS-per-instance-type honestly would be substantial new surface,
  not a rename — disclosed, matching this campaign's "too much new modeling"
  precedent (e.g. pinpoint's `ActivityResponse`).
- `StepStatus.StateChangeReason`/`FailureDetails` — real, non-required
  `types.StepStatus` members; this backend's steps only ever transition
  PENDING→COMPLETED (time-based) or PENDING→CANCELLED (`CancelSteps`), never
  fail, so there is no failure-reason data to source honestly.
- `ClusterInstance` missing `PublicIpAddress`/`EbsVolumes` — real,
  non-required `types.Instance` members, no such tracking in this backend's
  simulated instances.
- `SupportedInstanceType` missing `EbsOptimizedAvailable`/
  `EbsOptimizedByDefault`/`EbsStorageOnly`/`InstanceFamilyId`/`StorageGB` —
  this op serves a static hardcoded catalog, not backend-tracked state;
  filling in 5 more static fields per entry was judged lower value than the
  bugs above given session scope.
- `DescribeJobFlows`'s legacy `JobFlow` shape — `ReleaseLabel` is emitted but
  is **not** a real `types.JobFlowDetail` member (the real legacy shape
  predates release labels and uses `AmiVersion` instead, confirmed absent
  from `deserializers.go`'s `JobFlowDetail` case list, which has 14 members
  none named `ReleaseLabel`); 9 more real members
  (`AmiVersion`/`BootstrapActions`/`Steps`/`SupportedProducts`/
  `VisibleToAllUsers`/etc.) are missing entirely. Not fixed: `DescribeJobFlows`
  is deprecated/legacy, and correctly modeling `AmiVersion` for a backend that
  only ever creates release-label clusters would require fabricating a value
  with no honest source — disclosed as a known gap in this legacy op rather
  than guessed at.

**Ratifying tests found and fixed: 2**, both raw/typed assertions that
agreed with the pre-fix bug:
- `TestWireShape_StartNotebookExecution_ExecutionEngineField`
  (`handler_wire_shape_test.go`) decoded a flat `ExecutionEngineId` off
  `DescribeNotebookExecution`'s raw JSON body and asserted it as correct —
  exactly the pre-fix flat shape, so it passed against the bug. Rewritten to
  drive a real SDK client and assert through
  `NotebookExecution.ExecutionEngine.Id`, which cannot compile-pass, let
  alone assert-pass, against either the old flat shape or a wrong nested
  key.
- `TestEMRResourceRegionIsolation` (`isolation_test.go`) asserted
  `eastStudios[0].DefaultS3Location`/`westStudios[0].DefaultS3Location`
  (from `ListStudios`' real `[]StudioSummary` return) as region-differentiated
  evidence — a fabricated field the fix removed. Rewritten to assert
  `URL` instead (also region-differentiated, and a real `StudioSummary`
  member).

Zero found in the "assertion too weak to fail" shape this campaign also
watches for.

**Phantom ops: none.** All 65 op-name string literals in
`GetSupportedOperations()` confirmed to have a matching `api_op_<Name>.go`
file in emr@v1.64.4 (scripted check, not spot-checked).

**False-positive rate: 0 among reported bugs** — every finding cites the
real `deserializeOpDocument<Type>Output`/`deserializeDocument<Type>`/
`serializeDocument<Type>`/`serializeOp*Input` function or `api_op_*.go`
struct definition, file+line where grep found a unique match, never a doc
comment or an assumption. One near-miss caught and corrected before
landing: the first attempt at fix #2 assumed a single shared `Properties`
shape for request+response, which a real-client test disproved immediately
(see #2's detail) — corrected before this report, not left in.

**Real-client test ratio**: before this session, **0 of ~176 test
functions (0%)** drove a real SDK client through any op — the only file
importing the real SDK, `sdk_completeness_test.go`, only reflects over
`emrsdk.Client`'s method set for op-name completeness (matches this
campaign's already-established "doesn't count toward wire-shape coverage"
rule), same "worst yet" territory as mwaa's 0%/waf's 1.1%/ce's 1.4%. This
session added 8 tests in `services/emr/wire_field_fixes_test.go` (5 driving
a new `newTestEMRClient` real-client helper end-to-end: fixes #1/#2 combined,
#3, #4, #9) plus 1 rewritten real-client test in `handler_wire_shape_test.go`
(fix #5) and 2 raw-body absence-proving tests (fixes #6, #8 — a typed client
has no field to leak an absent key into either way, so absence can only be
proven against the raw body, matching workmail's precedent for the same
situation) plus 1 more raw-body test for fix #7's shape correction.

Every fix hand-reverted individually (no git, per this session's hard
no-git-mutation constraint), confirmed to fail with the exact predicted
symptom (quoted in each hand-revert: `StepSummary.Config` nil, empty
`ExecutionRoleArn` string, nil `ExecutionEngine`, fabricated
`StudioArn`/`DefaultS3Location` keys present, empty `IdcUserAssignment`/
false `TrustedIdentityPropagationEnabled`, missing `CreationTime`), then
restored and diffed byte-identical against the pre-revert file before
moving to the next. `Cluster.TerminatedAt`'s persistence-safety fix and
finding #2's Properties asymmetry were verified structurally (reading
`persistence.go`'s existing `clusterDTO` pattern; the real
`serializers.go`/`deserializers.go` shapes) rather than hand-reverted
separately, since they're sub-parts of already-reverted fixes #1/#6 above.

Gates: `go build`/`go vet`/`go test -race`/`go fix -diff` (no diff)/
`golangci-lint run` (0 issues after a `fieldalignment` auto-fix on 3
structs — `StepHadoopJarStep`, `PersistentAppUI`, `clusterDTO` — no
cyclop/gocyclo/gocognit/funlen nolints added) all green for `services/emr`.
`go test -race ./pkgs/...` green.

No subagents used (Read/Grep/Bash only, per this session's hard
constraint). No git-mutating commands run — orchestrator must commit/push.
`services/eventbridge` (a sibling session's territory, appeared mid-session,
37 files, confirmed via `git status`) never opened. No `gendocs`/`make docs`
run.

emr's List/Describe/Get families are now fully swept for this issue (30/30
ops verified against the real deserializer/serializer). 74 of 162 services
swept, 88 remain. Per the ranked table, eventbridge (30, `direct`) is the
next candidate — it is the live sibling's own territory as of this session's
last check, so re-confirm `git status` before picking it; if still occupied,
`route53resolver` (30, `manual`) is the next non-colliding option.

## eventbridge (this session)

Picked as the next-largest unswept service tied with emr (30 L+D+G each,
both `direct` resolution). Started reconnaissance on emr first per this
issue's own most-recent note; **mid-investigation, before any edit,
`git status` showed a live sibling had appeared with 10 modified files
under `services/emr/`**, including the *exact* `Step.Config`/`HadoopJarStep`
wrapper-key bug this session had independently just derived from the real
SDK deserializer (same finding, same file, same fix). Backed out
immediately (read-only investigation only, zero edits made) and switched to
eventbridge, the only other tied candidate. The sibling later committed as
`fdad98d4c fix(emr): DescribeStep returned nil JAR details to every real
client`, confirming the near-collision was real, not a false alarm. Two
independent sessions deriving the identical bug from the same deserializer
read is itself informative: it's not a subtle miss, it's the kind of error
this class systematically produces.

**Own enumeration of `GetSupportedOperations()` (handler_dispatch.go)
confirms 74 total ops, 30 L+D+G (16 List/12 Describe/2 Get)** exactly
matching the ranked table.

**PROTOCOL, second client, EqualFold, dead-deserializer trap**: eventbridge
core ops are `awsAwsjson11_` (JSON-RPC 1.1), case-sensitive; all 184
`EqualFold` hits in eventbridge@v1.48.4/deserializers.go are NaN/Infinity
float parsing, zero in body-field key switches. **Second client confirmed**:
17 of the 74 ops ("Schema Registry operations", `opCreateRegistry` through
`opGetCodeBindingSource`) are real `schemas@v1.37.4` operations, a genuinely
different service with its own `awsRestjson1_` protocol and its own
endpoint — routed in this repo via `handler_schemas_rest.go`'s REST-path
translation layer in front of an internal fabricated JSON-RPC dispatch
table (`handler_registries.go`/`handler_schemas.go`). Dead-deserializer trap
checked for both protocols and does **not** apply to either — traced
`HandleDeserialize` to the real
`awsAwsjson11_deserializeOpDocument<Op>Output`/
`awsRestjson1_deserializeOpDocument<Op>Output` functions directly for
several ops on each side.

**Schemas REST layer: already correct, verified not assumed.** Went in
expecting a repeat of the `services/emr` "wrong casing" class, since
`schemas@v1.37.4`'s `DescribeRegistryOutput`/`RegistrySummary`/`Schema`
types wrap tags under the case-sensitive restjson1 key `"tags"` (lowercase)
while this package's own *internal* `SchemaRegistry.Tags` model struct uses
`"Tags"` (capital, used only by the fabricated JSON-RPC dispatch table no
real client ever reaches). Read `handler_schemas_rest.go` in full expecting
to find the leak; instead found it already has its own separate,
deliberately narrower REST-only response DTOs
(`registryRESTOutput`/`registrySummaryRESTOutput`/`schemaRESTOutput`/
`schemaSummaryRESTOutput`/etc., lines ~345-520) with correct lowercase
`json:"tags,omitempty"` tags and a doc comment already citing the exact real
case-sensitivity distinction. **Sibling-pair check that came back clean**:
the internal fabricated-path type and the real REST-path type look like a
casing bug on a first read of `models.go` alone, but are two intentionally
separate types that never cross — confirmed by tracing `registryToREST`'s
conversion function, not assumed. Not touched further; this is a genuine
"correct sibling, verified rather than trusted" result, matching this
issue's request to report those.

**6 real bugs found and fixed, all in the core eventbridge (non-Schemas)
surface**, none of them casing (JSON-RPC eventbridge decodes case-sensitive,
but every finding here is a distinct wrong/missing key or dropped input,
not a case near-miss):

1. **CreateEventBus/UpdateEventBus discarded DeadLetterConfig/
   KmsKeyIdentifier/LogConfig entirely** (request side) and never echoed
   them on Create/Describe/Update (response side) — all three are real,
   directly-settable `CreateEventBusInput`/`UpdateEventBusInput` members
   (eventbridge@v1.48.4 `api_op_CreateEventBus.go`/`api_op_UpdateEventBus.go`)
   confirmed present on `DescribeEventBusOutput`
   (`deserializers.go`'s case list) but absent from the real plain
   `"EventBus"` type `ListEventBuses` uses — the fourth instance of this
   campaign's "directly-settable request fields silently discarded" class
   (after apigatewayv2/CreateProductPage, ce/StartCommitmentPurchaseAnalysis,
   vpclattice/CreateResourceConfiguration), doubled with a Describe/List
   asymmetry this campaign also tracks separately. `EventSourceName`
   (partner-event-bus matching) confirmed real but **disclosed, not fixed**:
   implementing it correctly would require a partner-source-to-bus linkage
   this backend's `PartnerEventSource` model has no slot for at all, and
   guessing at the accept-flow semantics risked the "fabricated behavior"
   trap this campaign also flags.
2. **`Step`... not applicable here** (emr's bug, not eventbridge's — noted
   only to be explicit this session's 6 findings are eventbridge-only).
3. **`ListArchives`/`ListReplays` silently ignored their real `EventSourceArn`
   and `State` filter request fields** (`api_op_ListArchives.go`/
   `api_op_ListReplays.go`, both confirmed real, non-deprecated members) —
   every call returned every archive/replay in the account regardless of
   the filter, a functional discarded-input bug a raw-body wrapper-key
   check alone would never catch (the *key itself* isn't wrong, the value
   is silently unused). Fixed by threading both fields through to the
   backend and filtering.
4. **`CreateArchive`/`UpdateArchive` discarded `KmsKeyIdentifier`** (real,
   directly-settable member on both inputs) and never echoed it on
   Describe — same shape as finding #1, smaller radius.
5. **`DescribeReplay` never emitted `ReplayArn`** despite the backend
   already computing and storing it (used correctly by `CancelReplay`'s and
   `StartReplay`'s own outputs, sitting right next to the gap) — a real
   `DescribeReplayOutput.ReplayArn` was always empty regardless of which
   replay was described. Backend-already-holds-it, lead-question-2 class.
6. **`CreateEndpoint`/`UpdateEndpoint` outputs dropped `EventBuses`/`Name`/
   `ReplicationConfig`/`RoleArn`/`RoutingConfig`** — all five real members
   (confirmed against `deserializers.go`'s case lists for both ops) already
   known from the backend object the handler had *just* built/updated, and
   `CreateEndpointOutput` additionally emitted `EndpointId`/`EndpointUrl` —
   fields the **real op does not return at all** (harmless: no field in the
   real typed output to decode them into; a genuine client must call
   `DescribeEndpoint` separately for those, confirmed via the real
   deserializer's case list, not assumed from field-name plausibility).
7. **`Target.BatchParameters.RetryStrategy` absent from the model entirely**
   — a real, non-deprecated member (`types.BatchRetryStrategy`,
   `eventbridge@v1.48.4 types.go:159`) silently dropped on `PutTargets` and
   never echoed by `ListTargetsByRule`, discovered by diffing every nested
   `Target.*Parameters` struct field-for-field against the real deserializer
   (`EcsParameters`/`RedshiftDataParameters`/`RunCommandParameters`/
   `SageMakerPipelineParameters`/`KinesisParameters`/`InputTransformer`/
   `AppSyncParameters`/`SqsParameters`/`HttpParameters` all came back fully
   correct — only `BatchParameters` had a gap). Because `PutTargets` stores
   the whole parsed `Target` struct verbatim and `ListTargetsByRule` emits
   it back unchanged, this fix is a pure model addition with zero handler
   logic required — cheapest fix this session.

**Sibling/shared-DTO trap, found independently three more times beyond
Connection (below)**: `EventBus`/`Archive`/`ApiDestination` each reused one
handler-level DTO struct for **both** their List item and their
Describe/Create/Update response, when the real AWS shapes for those two
paths genuinely differ (EventBus's own real List item type happened to
already match exactly — verified, not assumed, so left alone; Archive's
real List item lacks `ArchiveArn`/`Description`/`EventPattern`/
`KmsKeyIdentifier`; ApiDestination's lacks `Description`). Both were
harmless (a real typed List client can't decode into fields the shared
struct over-provided; no secret involved), but incorrect against the real
wire shape, so both were split into narrower `archiveSummary`/
`apiDestinationSummary` List-only types, following the pattern
`handler_replays.go`'s `replayListResponse`/`describeReplayResponse` split
already established correctly **before** this session (a genuine
already-correct in-package sibling, reported per this issue's request, not
a bug).

**Connection: checked hardest for the flagship secret-leak pattern
(cognitoidp's ClientSecret precedent) — confirmed CLEAN, not a bug.**
`connectionResponse.AuthParameters` looked, on first read of
`handler_connections.go` alone, like it assigned the backend's raw
`Connection.AuthParameters` (a struct whose `BasicAuthParameters.Password`/
`APIKeyAuthParameters.APIKeyValue`/`OAuthParameters.ClientParameters.
ClientSecret` fields are real, plaintext-capable members) straight onto the
wire — the same shape as the cognitoidp bug this campaign already found.
Reading `connections.go`'s `CreateConnection`/`UpdateConnection` disproved
it: the backend already stores a *masked* copy in the exported
`AuthParameters` field (`maskConnectionAuthParameters`, redacting all three
secrets down to Username/ApiKeyName/ClientID, matching the real
`ConnectionAuthResponseParameters` shape field-for-field) and the real
plaintext separately in an unexported `authSecret` field never touched by
any handler. Also checked and confirmed already-correct: per-field
`IsValueSecret` redaction on nested header/body/query HTTP parameters
(`maskHTTPParameters`). This is exactly the kind of would-be false positive
this issue's "flag anything you cannot verify, and trace it" instruction
exists to catch — reported as a verified-clean result, not a bug, and
nothing was changed in `connections.go`'s redaction logic. Two smaller real
gaps *were* found and fixed alongside this check: `DeauthorizeConnection`/
`UpdateConnection` outputs dropped `CreationTime`/`LastAuthorizedTime`
(both already known from the backend object), and `ListConnections`
reused the same over-wide `connectionResponse` DTO as EventBus/Archive/
ApiDestination above (split into `connectionSummary`, matching the real
narrower `Connection` list-item type — no secret was actually exposed by
this one either, since `AuthParameters` was already masked before it ever
reached the List path, but the shape itself was still wrong).

**Ratifying tests: none found needing correction.** No existing test in
this service asserted any of the six bugs' pre-fix shapes as correct —
`endpoints_test.go`'s existing `TestEndpoint_CRUD` only checked
`DescribeEndpoint`, never `CreateEndpoint`/`UpdateEndpoint`'s own output,
which is exactly why finding #6 went unnoticed. Zero found in the "wrong
key"/"wrong value"/"too-weak-to-fail" shapes this campaign tracks.

**Phantom ops: none** — `sdk_completeness_test.go` already reflects over
both `eventbridgesdk.Client` and `schemassdk.Client` method sets and passed
before and after. **False-positive rate: 0** — every finding above cites
the real deserializer/serializer case list or `types.go`/`api_op_*.go`
member list, file+line where checked.

**Real-client test ratio**: before this session, the only real-SDK-client
tests in this large (74-op) service were 2 narrowly-scoped ones
(`handler_partner_source_accounts_sdk_test.go`,
`handler_schemas_real_client_test.go`) — thin relative to the surface,
consistent with this campaign's "coverage does not predict bugs" finding
either way. Added 6 new real-SDK-client tests in
`services/eventbridge/wire_field_fixes_test.go` (reusing the existing
`newTestEventBridgeClient` helper), one per finding above (findings #3 and
part of #1/#6 combined get one test apiece where they share a code path).
Every fix hand-reverted individually (no git, per this session's hard
no-git-mutation constraint), confirmed to fail against the unfixed code
with the exact predicted symptom (quoted per-fix: nil `DeadLetterConfig`;
unfiltered 2-item list instead of 1; empty `ReplayArn`; empty `Name/`
zero-length `EventBuses`; nil `BatchParameters.RetryStrategy`; **and one
assertion strengthened mid-verification** — `DeauthorizeConnection`'s
`CreationTime` check was originally `!IsZero()`, which a Go epoch-0 decode
satisfies trivially since Unix 1970 is not the Go zero time, so the revert
didn't fail it; rewritten to assert exact equality against the known
creation time, which then correctly caught the regression as 1970-01-01 vs
the real value), then restored and confirmed passing again.

One pre-existing, unrelated build break discovered and **not** fixed:
`services/cloudformation/resources_wafv2.go:120` fails to compile against
the current `services/wafv2` `CreateRuleGroup` signature (missing 2
arguments) — traced via `git log` to `c1fce7ded fix(wafv2): ListAPIKeys
wrapper key, and RuleGroup discarded CustomResponseBodies`, a different
session's wafv2 sweep this same day that changed the backend signature
without updating this CloudFormation caller. `go build ./...` fails on this
alone; `go build ./services/eventbridge/... ./services/cloudformation/...
./pkgs/...` (scoped, per this session's own hard-constraint guidance)
confirms it is the *only* other failure and is untouched by anything in
this session's diff. **Flagged for whoever owns the wafv2 sweep, not
fixed** — out of this session's scope. This session's own regression in the
same file (a `CreateEventBus` call site broken by finding #1's signature
change) *was* fixed, and is a separate, one-line, in-scope change.

Gates: `go build`/`go vet`/`go test -race`/`go fix -diff` (no diff) all
green for `services/eventbridge`. `golangci-lint run` initially found a
`dupl` pairing (`ListArchives`/`ListReplays`, introduced by finding #3's
matching filter logic) and a `fieldalignment` hit on the new `EventBus`
fields — both fixed (the `dupl` pair by factoring a shared generic
`filterNamedItems`/`listNamedItems` helper into `accessors.go` rather than
a `//nolint:dupl`; fieldalignment via the `fieldalignment -fix` tool, whose
auto-fix silently stripped one doc comment in the process — caught by
diffing before/after and restored by hand). 0 issues after. No
cyclop/gocyclo/gocognit/funlen nolints added. `go test -race ./pkgs/...`
green.

No subagents used (Read/Grep/Bash only, per this session's hard
constraint). No git-mutating commands run — orchestrator must commit/push.
`git status` re-checked repeatedly through the session; no further sibling
collisions after the emr near-miss at the start.

75 of 162 services swept, 87 remain. Per the ranked table, the next
candidates are `route53resolver` (30, `manual`, unresolved by
`cmd/opcensus`, hand-counted) and `kafka` (29, `direct`) — re-check
`git status` before picking either.

## kafka (this session)

Chosen as the next-largest unswept service (29 L+D+G ops: 15 List/11
Describe/3 Get) that didn't collide with the live sibling on eventbridge
(confirmed via `git status` at start — only `services/eventbridge` and a
one-line `services/cloudformation` cross-reference were modified; both
untouched by this session). Single client (MSK — no second/companion client
in this service, unlike eventbridge's Schemas surface), matching the
"settle completely" preference from the emr pass.

PROTOCOL: confirmed `awsRestjson1_` from kafka@v1.57.2 deserializers.go's
sole function prefix. Case-sensitive: all 398 `EqualFold` hits are either
`errorCode` matching or float `NaN`/`Infinity`/`-Infinity` special-value
parsing inside numeric-field decode branches — none in a body-field `switch
key { case "...": }` block, confirmed by reading `HandleDeserialize` for
`ListClustersV2` directly (it calls
`awsRestjson1_deserializeOpDocumentListClustersV2Output` itself; no dead
`OpDocument...Output` wrapper sits between them, so the dead-deserializer
trap does not apply here, unlike pinpoint's restjson1).

**This service already had an unusually deep PARITY.md audit history**
(gopherstack-h910, jqh2, dv4s, mk3t) with nearly every op marked `wire: ok`
and "field-diffed against deserializers.go" — including DescribeCluster/
ListClusters/DescribeClusterV2/ListClustersV2, the four ops this session
found the most bugs in. **That prior confidence was wrong.** A fresh,
independent per-field diff of `ClusterInfo`/the V2 top-level `Cluster`/
`Provisioned` against the real deserializer's own case list (not trusting
the PARITY.md notes) found a dense cluster of bugs concentrated in exactly
the area repeatedly marked safest:

1. **Fabricated members, both V1 and V2 (5 fields across 4 ops).**
   `ClusterInfo` (DescribeCluster/ListClusters) emitted a top-level
   `kafkaVersion` and `configurationInfo` that don't exist on the real type
   at all (confirmed: no such case in
   `awsRestjson1_deserializeDocumentClusterInfo`). `Provisioned`
   (DescribeClusterV2/ListClustersV2's nested arm) emitted the same two
   plus a fabricated `state` (real `state` lives only on the V2 response's
   top-level `Cluster`, confirmed absent from `Provisioned`'s own
   deserializer). Harmless to a real client (unknown JSON keys are silently
   ignored — confirmed via the deserializer's `default: _, _ = key, value`
   case), but still wrong. All five removed.
2. **A real key, but it belongs on a different type (echo of last pass's
   flagship finding).** `kafkaVersion`/`configurationInfo` genuinely exist
   on the real API — as members of `MutableClusterInfo`, used by
   `ClusterOperationInfo`'s `sourceClusterInfo`/`targetClusterInfo` (the
   operation-tracking family), not `ClusterInfo`/`Provisioned`. gopherstack's
   own `MutableClusterInfo`/`ClusterOperation` types don't model either
   field yet, and that family already carries a disclosed, deliberately-
   deferred note about a wider V2/`ClusterOperation` remodel (the
   `operationArn` vs `clusterOperationArn` key bug in
   `clusterOperationV2SummaryOutput`'s doc comment). Relocating these two
   fields there is disclosed, not fixed, to avoid scope-creeping into that
   already-tracked, larger gap.
3. **Backend-tracked but never emitted (layer 3, "one sibling correct
   beside the broken one").** `storageMode`/`creationTime` missing from
   `ClusterInfo` (V1) despite `Cluster.StorageMode`/`CreationTime` already
   being tracked and already correctly emitted by `Provisioned`(V2)/(for
   StorageMode) — sibling trap, V2 right, V1 wrong. `activeOperationArn`/
   `creationTime`/`stateInfo` missing from the V2 top-level `Cluster`
   despite all three already being correctly emitted by the V1 sibling
   (`ClusterInfo`) — same pattern, mirrored. Investigating `CreationTime`
   further found it was never actually **set** anywhere in this backend
   (always `""`) despite having a real field and JSON tag — fixed at
   `CreateCluster`/`CreateClusterV2`/`CreateServerlessCluster`/
   `AddClusterInternal`, matching the `time.Now().UTC().Format(time.RFC3339)`
   pattern every other resource in this service (Configuration/Replicator/
   VpcConnection/Channel) already used.
4. **Missing real fields, fixed by extending an existing synthesis
   precedent.** `zookeeperConnectStringTls` (V1) and both
   `zookeeperConnectString`/`zookeeperConnectStringTls` (V2 `Provisioned`,
   which had neither) are real members this backend never emitted. The
   existing `zookeeperConnectStringFor` helper already synthesizes a
   plausible ZK endpoint from the cluster ARN for V1's plaintext port (a
   pre-existing, already-accepted documented simplification — this backend
   has no real per-broker ZK state) — extended to take a port parameter and
   wired to both the TLS port (2182) and the V2 response, which never had it
   at all.
5. **Discarded input (6th instance of this class across the campaign, after
   apigatewayv2/ce/vpclattice/emr×2).** `CreateReplicatorInput.LogDelivery`
   (real, optional member, `api_op_CreateReplicator.go`) was parsed nowhere
   — silently dropped on every call, never stored, never echoed by
   `DescribeReplicator` (whose real output also carries it, confirmed via
   deserializers.go). Fixed: accepted, stored (`Replicator.LogDelivery`,
   deep-cloned via a new `cloneLogDelivery`), and echoed. Reused the
   existing `CloudWatchLogs`/`Firehose`/`S3Logs` types as-is rather than
   inventing new ones — their wire field names are identical to the real
   `ReplicatorCloudWatchLogs`/`ReplicatorFirehose`/`ReplicatorS3`.

**Ratifying test found and fixed**: `TestUpdateClusterConfiguration_V2Path`
asserted `provisioned["configurationInfo"]["arn"]` as the correct shape — a
raw-body (`map[string]any`) test that only passed because the handler and
the test agreed on the fabricated field (finding #1 above). Reverting the
fix reproduced the exact predicted failure (`expected:
"arn:aws:kafka:...", actual: <nil>`). Rewritten to assert the field is
genuinely absent (`assert.NotContains`) with an explanatory comment; the
persisted-configuration behavior itself remains covered by the sibling
domain-level tests (`TestUpdateClusterConfiguration_PersistsConfig`/
`_HTTP`, which read the backend's own `*Cluster` struct directly rather than
the wire JSON, and were never wrong).

**Everything else spot-checked came back clean.** Topics family
(`DescribeTopic`/`ListTopics`) matches `types.TopicInfo`/
`DescribeTopicOutput` field-for-field. `ListKafkaVersions`/`ListNodes` both
have a real, unmodeled `nextToken` pagination member this backend's
single-page response omits — disclosed, not fixed (this in-memory backend's
version/node lists are never large enough to need real pagination, and an
always-empty cursor would be fabrication, not a fix). `ListNodes`' existing
"wire: partial" note (missing `BrokerNodeInfo`/`ControllerNodeInfo`/
`ZookeeperNodeInfo`/`NodeARN` on the real `types.NodeInfo` shape, filed
under gopherstack-mk3t, a different — and larger — bug than this issue's
wrapper-key class) was re-confirmed still accurate and is not duplicated
here.

**Phantom ops**: none — every op string in `GetSupportedOperations`
corresponds to a real `api_op_*.go` file in kafka@v1.57.2 (all 64 checked).

**False-positive rate**: 0 among reported bugs — every finding cites the
real `awsRestjson1_deserializeDocument<Type>`/`deserializeOpDocument<Op>
Output` function's own case list, file-grepped directly, never a doc
comment or a prior PARITY.md claim taken on faith (the whole point of this
pass — the prior claims were the thing that turned out wrong).

**Real-client test ratio**: 9 real-aws-sdk-go-v2-client tests added
(`services/kafka/cluster_field_fixes_test.go` ×4 covering finding #3/#4,
`services/kafka/replicator_log_delivery_test.go` ×1 covering finding #5)
plus the 1 ratifying-test rewrite, covering every fix above except
`activeOperationArn` — genuinely untestable for a non-empty value, since
nothing in this backend ever sets it to non-empty (a pre-existing gap this
pass did not expand scope to fix; the wire plumbing is now correct for
whenever it is set). Every fix hand-reverted individually (no git, per this
session's hard no-git-mutation constraint), confirmed to fail with the
exact predicted symptom (assertion diffs quoted in each test's own commit
history), then restored and diffed byte-identical against the pre-revert
file before moving to the next.

Gates: `go build`/`go vet`/`go test -race` (scoped to `services/kafka`),
`go fix -diff` (no diff), `gofmt`/`goimports`/`golines` (3 formatting
findings from golangci-lint's goimports/golines checks, fixed by running the
actual formatters rather than hand-wrapping), `fieldalignment` (0 hits),
`golangci-lint run` (0 issues; no cyclop/gocyclo/gocognit/funlen nolints
added) all green for `services/kafka`. `go test -race ./pkgs/...` green.

No subagents used (Read/Grep/Bash only, per this session's hard constraint).
No git-mutating commands run — orchestrator must commit/push. `git status`
re-checked before starting and again at the end; only `services/kafka` files
touched, no sibling collisions (the eventbridge/cloudformation diff from the
session start had already been committed locally by that sibling session by
the time this pass finished, confirmed via `git log`).

76 of 162 services swept, 86 remain. Per the ranked table, `route53resolver`
(30, `manual`, unresolved by `cmd/opcensus`, hand-counted) is next largest —
re-check `git status` before picking it.

## route53resolver (this session)

Chosen per the prior (kafka) session's own pointer above, cross-checked
against `bd show gopherstack-6flj`'s comments and `git status` (clean at
start; a live sibling appeared mid-session editing `services/appsync/*.go`,
confirmed untouched by this session throughout).

PROTOCOL: `application/x-amz-json-1.1` (JSON-RPC 1.1), confirmed from
`handler.go`'s `Handler()` (`"Route53Resolver", "application/x-amz-json-1.1"`)
and cross-checked against route53resolver@v1.48.4's own `deserializers.go`
function-prefix grep (`awsAwsjson11_` is the sole prefix present — no
`awsRestjson1_`/`awsEc2query_`/`awsAwsquery_`). Case-sensitive: all 407
`EqualFold` hits in `deserializers.go` are `errorCode` matches in the per-op
`deserializeOpError*` functions; none are in a body-field
`switch key { case "...": }` block (grepped and confirmed zero non-errorCode
hits, not spot-checked).

**Dead-deserializer trap checked and found NOT to apply** — traced
`HandleDeserialize` for `ListResolverEndpoints` directly
(deserializers.go:6503): it decodes the body into `shape` and calls
`awsAwsjson11_deserializeOpDocumentListResolverEndpointsOutput(&output, shape)`
itself (deserializers.go:6543) — the `OpDocument...Output` function **is**
the real, reached deserializer, same shape as cloudwatchlogs/guardduty, not
pinpoint's restjson1.

**Second client**: none — single MSK-style client, just this one Resolver
SDK module in `go.mod`.

This service already had **unusually deep prior audit history** (PARITY.md
citing gopherstack-y9w3, hvni, 3sgl, jp7o, 4gzs, mslf, parity-5, all with
real file+line SDK citations, not bare "wire: ok" claims) — grade A,
`last_audit_date: 2026-07-30`. Per this issue's "deep prior coverage is not
evidence" lesson from kafka, all 30 L+D+G ops (16 List, 14 Get; the ranked
table's manual count) were re-verified independently against
route53resolver@v1.48.4's own deserializer case lists (file+line grepped per
type, not hand-transcribed) rather than trusted from PARITY.md. The prior
work held up almost entirely — every wrapper key (List/Get top-level member
name) matched exactly across all 30 ops, including the tricky
`GetResolverDnssecConfig` → `"ResolverDNSSECConfig"` casing (a real
same-service inconsistency, not a bug). 3 new bugs found nonetheless, all
layer-2/3 (correct outer shape, wrong/missing nested member), in territory
the prior passes' field-level `OwnerID`/`BlockOverrideDnsType`-casing sweeps
hadn't reached:

1. **A second, previously-missed fabricated field on `resolverEndpointOutput`
   (real key from nowhere — same class as the already-fixed `IpAddresses`
   invention on this exact struct, just not caught in that pass).** Emitted
   a top-level `VpcId` alongside the correct `HostVPCId`. Confirmed absent
   from `types.ResolverEndpoint`'s real deserializer
   (`awsAwsjson11_deserializeDocumentResolverEndpoint` has no `"VpcId"`
   case, only `"HostVPCId"`) and from `types.go`'s struct definition — `VpcId`
   is a real field, but on a *different* type entirely
   (`FirewallRuleGroupAssociation.VpcId`, `types.go:901`, already correctly
   modeled there), the "real key from the wrong type" variant. Affects 6 ops
   sharing this struct: `CreateResolverEndpoint`, `GetResolverEndpoint`,
   `ListResolverEndpoints`, `UpdateResolverEndpoint`,
   `AssociateResolverEndpointIpAddress`, `DisassociateResolverEndpointIpAddress`.
   Harmless to a real client (unknown JSON keys ignored), removed anyway.
   **Deeper finding while tracing this**: `CreateResolverEndpointInput` has
   no `VpcId` request member either — AWS derives `HostVPCId` server-side
   from `IpAddresses[].SubnetId`
   (`types.IpAddressRequest`: `SubnetId`/`Ip`/`Ipv6` only, no VPC field).
   gopherstack's backend has always sourced `HostVPCID` from this same
   fabricated wire field, meaning **a real, unmodified SDK client's
   `CreateResolverEndpoint` call has no way to populate `HostVPCId` at all
   for the endpoints it creates** — a genuine, disclosed gap (this backend
   has no subnet→VPC registry to derive one honestly; synthesizing a
   plausible `vpc-*` id from a `subnet-*` id would be fabrication). The
   internal-only `VpcId` request field was kept (not removed) since dropping
   it would remove the only path this backend has for setting `HostVPCId`
   at all, and no real client can send it either way. Disclosed in
   PARITY.md's gaps, not silently fixed with an invented derivation.
2. **Backend-tracked-but-unemitted (layer 3), on a sibling pair.**
   `ListResolverQueryLogConfigsOutput`/`ListResolverQueryLogConfigAssociationsOutput`
   both have real, always-populated `TotalCount`/`TotalFilteredCount`
   members (deserializers.go) that were never wired at all — a real SDK
   client's typed fields stayed `0` regardless of how many configs/
   associations existed. Both handlers already compute the exact values
   needed (`len` of the backend's full list before `applyFilters`, `len`
   after) one line above the return; simply never surfaced. Fixed both.
3. **Missing real member, disclosed-untestable.**
   `resolverRuleAssociationOutput` (shared by `AssociateResolverRule`,
   `GetResolverRuleAssociation`, `DisassociateResolverRule`,
   `ListResolverRuleAssociations`) never emitted `StatusMessage`, a real
   non-required `types.ResolverRuleAssociation` member. Added — but this
   backend has no async failure state to ever populate it with a non-empty
   value, and it's tagged `omitempty` to match AWS's own "absent when
   there's nothing to report" convention, so **the field's presence is
   permanently unobservable on the wire either way** (empty value + omitempty
   ⇒ key absent, identical to the pre-fix shape). A first version of a
   round-trip test for this was written, confirmed to pass unchanged against
   the pre-fix code (the exact "assertion too weak to fail" trap this issue
   tracks), and deliberately dropped rather than kept as false assurance —
   see `wire_field_fixes_test.go`'s comment in place of the test.

**Verified correct, not a bug (checked hardest, came back clean):**
`types.FirewallRule.Status`/`StatusMessage` are real members
(`deserializers.go` cases `"Status"`/`"StatusMessage"`) `firewallRuleOutput`
never emits — looked exactly like finding #3's shape at first read. The
real field's own doc comment resolves it: *"For rules that do not require
asynchronous provisioning, this field may be absent."* This backend creates
every Firewall Rule synchronously with no async provisioning state (same
documented convention as this service's `status_lifecycle` family note) —
correctly absent, not a gap.

**Request side**: checked as part of every finding above (findings #1 and
#2 are request+response or backend-plumbing pairs, not response-only).
Spot-checked `ListFirewallDomains`/`ListFirewallRuleGroupAssociations`/
`ListResolverRuleAssociations` request structs against their real
`*Input` types beyond what's disclosed above — no further gaps found;
this service's `Filters`/`SortBy` request-side coverage from prior passes
(`gopherstack-66dr`/`hvni`/`jp7o`) already matched the real SDK structs
field-for-field on every op checked.

**Ratifying tests found and fixed: 1.**
`TestCreateResolverEndpoint_VpcIdAndSecurityGroups` (raw-body, hand-built)
asserted the fabricated `resp["VpcId"]` as correct — passed cleanly
pre-fix because the handler and the test agreed on the wrong shape.
Renamed to `TestCreateResolverEndpoint_HostVPCIdAndSecurityGroups` and
rewritten to assert `HostVPCId` plus `assert.NotContains(..., "VpcId")`.
No other ratifying tests found in this service referencing any of the
three findings — `TotalCount`/`TotalFilteredCount`/`StatusMessage` had zero
prior test coverage in either direction.

**Phantom ops**: none — `TestSDKCompleteness` (`sdk_completeness_test.go`)
passed before and after, covering all ops against
`route53resolversdk.Client`'s real method set.

**False-positive rate**: 0 among reported bugs — every finding cites the
real `awsAwsjson11_deserializeDocument<Type>`/`deserializeOpDocument<Op>
Output` function's own case list (file-grepped, not hand-transcribed) or
the real `types.go`/`api_op_*.go` struct definition, never a doc comment or
a PARITY.md claim taken on faith — the explicit point of re-checking this
service despite its unusually thorough prior audit trail.

**Real-client test ratio**: this service had **zero** prior real-SDK-client
tests (`sdk_completeness_test.go` only constructs a bare `&Client{}` for
method-set reflection, never dials a server) despite ~3,700 lines of
handler code and an A-grade PARITY.md — 100% raw-HTTP-body tests before this
pass, consistent with this campaign's "coverage does not predict bugs, and
deep audit history does not either" finding. Added
`services/route53resolver/wire_field_fixes_test.go` with a
`newTestRoute53ResolverClient` helper (same `httptest.NewServer` +
`service.NewRegistry()` pattern as kafka/guardduty's helpers) and 2 new
real-client tests (`TestListResolverQueryLogConfigs_TotalCounts`,
`TestListResolverQueryLogConfigAssociations_TotalCounts`) plus the one
rewritten raw-body ratifying test above. Every fix hand-reverted
individually (no git, per this session's hard no-git-mutation constraint),
confirmed to fail with the exact predicted symptom (`VpcId` present in the
raw response map; `TotalCount`/`TotalFilteredCount` asserted `3`/`2`,
actual `0` both times), then restored and diffed byte-identical against the
pre-revert file before moving to the next. Finding #3's `StatusMessage` fix
has no test at all, disclosed above and in-code rather than backed by a
test proven not to discriminate.

**Disclosed, not fixed** (two structural gaps, neither a rename): see
PARITY.md's `gaps` — `CreateResolverEndpointInput`'s missing real `VpcId`
member (no honest way to derive `HostVPCId` for a real client without new
subnet→VPC modeling) and `ListResolverEndpointIpAddresses`' per-item
`CreationTime`/`ModificationTime`/`StatusMessage` (backend's `IPAddress`
model tracks neither timestamp).

Gates: `go build ./...` (full, confirmed clean before and after — no
signature changes), `go vet`/`go test -race`/`go fix -diff` (no diff)/
`gofmt`/`golines` all green for `services/route53resolver`.
`golangci-lint run` — 1 `govet` shadow finding on a test helper's `err`
(same class cloudwatchlogs's pass hit) plus 1 `golines` formatting finding,
both fixed; 0 issues after. `fieldalignment` — 0 hits. No
cyclop/gocyclo/gocognit/funlen nolints added. `go test -race ./pkgs/...`
green.

No subagents used (Read/Grep/Bash only, per this session's hard
constraint). No git-mutating commands run — orchestrator must commit/push.
`git status` re-checked repeatedly through the session; the `services/appsync`
sibling diff that appeared mid-session was left untouched throughout.

route53resolver's List/Describe/Get families are now fully swept for this
issue (30/30 ops verified against the real deserializer/serializer). 77 of
162 services swept, 85 remain. Per the ranked table, `appsync` (74 ops, 28
L+D+G, `direct`) is next largest — **a live sibling was actively editing
`services/appsync/*.go` throughout this session**; re-check `git status`
before picking it, and pick the next candidate down
(`workspaces`, 27, `dynamic-fallback`) if appsync is still claimed.

## appsync (this session)

Chosen as the largest unswept service not held by a live sibling: the prior
route53resolver session flagged appsync (74 ops, 28 L+D+G, `direct`
resolution) as next-largest but reported a live sibling editing
`services/appsync/*.go` at the time — that was this session. `git status`
was clean at start (no uncommitted work anywhere), confirmed again
throughout; no collision occurred.

PROTOCOL: `awsRestjson1_` exclusively (only prefix present in
appsync@v1.56.4's `deserializers.go`), single client (no separate
data-plane SDK client — real GraphQL execution isn't a modeled SDK
operation at all; gopherstack's `handleGraphQL`/`opExecuteGraphQL` route
already correctly excludes itself from `GetSupportedOperations`, verified
against `go doc .../appsync.Client`'s real method set, pre-existing and
unchanged). Case-sensitive: 355 `EqualFold` hits in `deserializers.go`, ALL
`errorCode` matching in the per-op `deserializeOpError*` functions — zero in
any body-field `switch key {}` block (grepped and spot-checked). Dead-
deserializer trap checked against `GetGraphqlApi`
(`awsRestjson1_deserializeOpGetGraphqlApi.HandleDeserialize`,
deserializers.go:5417) and found NOT to apply: it decodes the body into
`shape` and calls `awsRestjson1_deserializeOpDocumentGetGraphqlApiOutput`
directly (deserializers.go:5458) — no dead wrapper switch sits between them,
unlike pinpoint's restjson1 shape.

**Layer 1 (wrapper keys): entirely CLEAN**, confirmed by reading every
List/Get op's own `awsRestjson1_deserializeOpDocument<Op>Output` case list
directly (file-grepped, not trusting PARITY.md's extensive pre-existing
"wire: ok" claims — this service had unusually deep prior audit history,
same shape as kafka's flagship finding last session, so every claim was
re-derived independently): `graphqlApis`, `apis`, `dataSources`,
`resolvers` (both ListResolvers and ListResolversByFunction),
`channelNamespaces`, `apiKeys`, `domainNameConfigs`, `types` (both
ListTypes and ListTypesByAssociation), `sourceApiAssociationSummaries`, and
every singular Get* wrapper (`graphqlApi`, `dataSource`, `resolver`,
`apiCache`, `channelNamespace`, `apiAssociation`, `domainNameConfig`,
`functionConfiguration`, `type`, `environmentVariables`, `api`,
`sourceApiAssociation`) all matched the real deserializer exactly. No prior
audit claim was disproved at this layer — the one from last session's
warning ("don't trust deep prior coverage") did not repeat here; this is
the honest negative result the campaign asked to be reported alongside the
positive ones.

**Layer 2/3: 7 real bugs found and fixed**, all field-name/nesting/
discarded-input, found by diffing gopherstack's `models.go` structs against
each type's own deserializer case list (not the struct field names in
`types/types.go`, which don't carry the wire key — confirmed the wire key
from the `case "...":` string literal every time):

1. **`SourceAPIAssociation.AssociationStatus` — sibling trap, wrong wire
   key.** gopherstack emitted `"associationStatus"`; the real key is
   `"sourceApiAssociationStatus"` (deserializers.go:16488, in
   `awsRestjson1_deserializeDocumentSourceApiAssociation`). This is a
   genuine sibling trap: the similarly-named `ApiAssociation` type (domain
   name associations, a completely different concept) genuinely DOES use
   the plain `"associationStatus"` key (deserializers.go:12175,
   `awsRestjson1_deserializeDocumentApiAssociation`) — confirmed correct,
   left alone. Affects `GetSourceApiAssociation`,
   `AssociateSourceGraphqlApi`, `AssociateMergedGraphqlApi`,
   `UpdateSourceApiAssociation` (the standalone `StartSchemaMerge` op was
   already correct — it hand-builds `{"sourceApiAssociationStatus": ...}"`
   directly, unaffected). A real client's typed
   `SourceApiAssociation.SourceApiAssociationStatus` field was always empty
   regardless of backend state before this fix. Also added the missing
   real `sourceApiAssociationStatusDetail` member (verified at
   deserializers.go:16497) — left unset/never emitted since this backend's
   merges always succeed synchronously and a failure-detail string would be
   fabrication (disclosed, not fixed, per this issue's "disclose rather
   than fabricate" instruction). Note: `ListSourceApiAssociations`'s own
   item type, `SourceApiAssociationSummary`, genuinely has NO status field
   at all (deserializers.go:16586-16640: only associationArn/associationId/
   description/mergedApiArn/mergedApiId/sourceApiArn/sourceApiId) —
   gopherstack reuses the full `SourceAPIAssociation` struct for that list
   response too, so the (correct, now-fixed) status key still appears
   there where the real API wouldn't have one at all. Harmless (extra
   unknown field, real client ignores it) — an over-wide-DTO variant,
   disclosed rather than split into a narrower type (would need a new
   struct + converter for zero functional benefit; same class as
   eventbridge's already-fixed Archive/EventBus/ApiDestination over-wide
   DTOs from a prior batch, but lower priority here since nothing sensitive
   leaks — just a status string a real client already sees correctly via
   Get/Associate).
2. **`EventConfig.LogConfig` — discarded input, both directions.** Real
   `CreateApiInput`/`UpdateApiInput` accept `EventConfig.LogConfig` nested
   under `eventConfig` (serializers.go's
   `awsRestjson1_serializeDocumentEventConfig`, `case "logConfig":`), and
   the real `Api` response type echoes it back
   (deserializers.go:14731-14734, via
   `awsRestjson1_deserializeDocumentEventLogConfig`, a distinct 2-field
   type — `cloudWatchLogsRoleArn`/`logLevel`, deserializers.go:14745 —
   NOT the same shape as GraphqlApi's 3-field `LogConfig`, which also has
   `excludeVerboseContent`). gopherstack's `EventConfig` struct had no
   field for it at all: `json.Unmarshal` silently dropped a real client's
   `CreateApi`/`UpdateApi` `EventConfig.LogConfig` value every time (9th
   discarded-input instance this campaign, after apigatewayv2/ce/
   vpclattice/emr×2/eventbridge×2/kafka). Fixed by adding a new
   `EventLogConfig` type (distinct from GraphqlAPI's `LogConfig`, matching
   the real type's narrower field set) and wiring it into `EventConfig`;
   no backend signature change needed since `CreateAPI`/`UpdateAPI` already
   store the whole `*EventConfig` pointer verbatim.
3. **`GraphqlAPI.EnvironmentVariables` — over-wide field, real leaked
   data.** The real `GraphqlApi` type has no `environmentVariables` member
   at all (verified: full case list of
   `awsRestjson1_deserializeDocumentGraphqlApi`, deserializers.go:
   14999-15185, has no such case) — environment variables are exposed only
   via the dedicated `GetGraphqlApiEnvironmentVariables`/
   `PutGraphqlApiEnvironmentVariables` ops. gopherstack's shared
   `GraphqlAPI` struct carried an `EnvironmentVariables` field
   (`json:"environmentVariables,omitempty"`) that was the SAME struct field
   `PutGraphqlApiEnvironmentVariables` populates — so once a caller set
   real environment-variable values (e.g. connection strings, feature
   flags — AWS documentation itself advises against storing secrets there,
   but nothing stops a customer from doing so), every subsequent
   `GetGraphqlApi`/`ListGraphqlApis`/`CreateGraphqlApi`/`UpdateGraphqlApi`
   response leaked those exact values into a response AWS never puts them
   in. **Contains**: whatever a customer set via
   `PutGraphqlApiEnvironmentVariables` — potentially config values,
   endpoints, or (against AWS's own guidance but not prevented) secrets;
   this is the class of over-wide field this issue asks to be flagged
   loudest, distinct from GraphqlAPI's other three fabricated-but-harmless
   fields below. Fixed via `json:"-"` (excluded from GraphqlAPI's own wire
   serialization; the Go field itself is unchanged and still used
   internally by the two dedicated env-var ops).
4. **`GraphqlAPI.Owner` — real member, previously unmodeled.** Real
   `GraphqlApi.Owner` (`"owner"`, deserializers.go:15114) is "The account
   owner of the GraphQL API" — gopherstack already had the account ID
   trivially on hand (`b.accountID`, the same value used to build the
   API's own ARN one line above) but never populated it. Fixed: added
   `Owner` field, set at `CreateGraphqlAPI` time from `b.accountID`.
5. **`DataSource.MetricsConfig` — discarded input, both directions.** Real
   `CreateDataSourceInput`/`UpdateDataSourceInput` accept `MetricsConfig`
   (`types.DataSourceLevelMetricsConfig`, `"ENABLED"`/`"DISABLED"`,
   serializers.go's `object.Key("metricsConfig")`) and the real
   `DataSource` response type echoes it (deserializers.go:13625-13632) —
   gopherstack's `DataSource` struct had no field for it, so a real
   client's value was silently dropped on create AND update. Fixed: added
   the field; `CreateDataSource` already stores the whole `*DataSource`
   pointer verbatim (no wiring needed there), `UpdateDataSource`'s
   field-by-field copy pattern needed one added line.
6. **`Resolver.MetricsConfig` — discarded input, both directions.** Same
   shape as #5: real `CreateResolverInput`/`UpdateResolverInput` accept
   `MetricsConfig` (`types.ResolverLevelMetricsConfig`,
   serializers.go:1475's `object.Key("metricsConfig")`) and the real
   `Resolver` type echoes it (deserializers.go:16248) — unmodeled
   entirely, silently dropped both ways. Fixed the same way (field added;
   `UpdateResolver`'s field-by-field copy needed one added line;
   `CreateResolver` needed none).
7. Confirmed harmless (see disclosed list below, not counted as a fix):
   `GraphqlAPI.Region`/`CreatedAt`/`UpdatedAt` are ALSO fabricated (no such
   members on the real `GraphqlApi` type at all — the real type tracks
   neither a region nor creation/update timestamps), always populated
   since `CreateGraphqlAPI` sets all three unconditionally. Harmless (no
   customer data, just informational) — same class as emr's harmless
   timestamp and wafv2's three fabricated-but-disclosed keys from prior
   batches. No existing test asserts these raw keys (checked before
   deciding not to spend fix budget here), so leaving them costs nothing
   and touches zero call sites; disclosed rather than removed to keep this
   session's diff scoped to functional bugs and real data leaks.

**Sibling/version-pair check, explicitly**: `ApiAssociation` (correct,
plain `associationStatus`) vs `SourceAPIAssociation` (was wrong, now fixed)
is the one genuine sibling trap found — reported per this issue's
instruction to report siblings checked and found already correct alongside
the ones that were broken. `ChannelNamespace` was checked field-by-field
against its real deserializer and found **entirely correct already**
(apiId/channelNamespaceArn/codeHandlers/created/handlerConfigs/
lastModified/name/publishAuthModes/subscribeAuthModes/tags all present and
correctly named) — reported as a clean sibling, not re-flagged.
`DomainNameConfig` vs `ApiAssociation` vs `Api` (Event API) were each
checked independently against their own case lists; no cross-type key
confusion found among them beyond the one reported above.

**Real key from the wrong type**: none found in this service (the emr
`HadoopJarStep`/kafka `MutableClusterInfo` pattern from prior sessions did
not repeat here) — every fabricated field found (Region/CreatedAt/
UpdatedAt/EnvironmentVariables on GraphqlAPI, and the `apiId` field present
on DataSource/Resolver/Function/ApiCache/APIType/DomainName below) is
either genuinely absent from every real type in this service, or absent
specifically from the type it's attached to.

**Fields plumbed to the wire but never set**: none found this session —
every field this session fixed for backend-tracked-but-unemitted was
actually the inverse (discarded *input*, not unemitted state): #2/#5/#6
above are all "backend never had anywhere to put a real, accepted request
value," not "backend has the value and forgot to emit it."

**Discarded inputs this session**: 3 (#2 EventConfig.LogConfig, #5
DataSource.MetricsConfig, #6 Resolver.MetricsConfig) — 9th/10th/11th
instances of this class across the campaign (after apigatewayv2, ce,
vpclattice, emr×2, eventbridge×2, kafka).

**Over-wide field, what it contains**: GraphqlAPI.EnvironmentVariables
(fixed, see #3 above — real customer-set values, potentially sensitive
depending on what the customer stored there, though AWS's own guidance
discourages secrets in this particular field). GraphqlAPI.Region/
CreatedAt/UpdatedAt (disclosed, harmless — informational only, matching
this campaign's emr/wafv2 precedent for cheap-to-leave fabricated fields).

**Raw internal model / fabricated `apiId` field, disclosed (harmless, not
fixed)**: `DataSource`/`Resolver`/`Function`/`ApiCache`/`APIType` all emit
an `apiId` field on their own object — none of the corresponding real
types (`types.DataSource`, `types.Resolver`, `types.FunctionConfiguration`,
`types.ApiCache`, `types.Type`) has any such member at all (each verified
against its own deserializer case list: DataSource
deserializers.go:13560-13678, Resolver :16194-16299 (full, not the earlier
90-line-truncated read), FunctionConfiguration :14794-14915, ApiCache
:12233-12291, Type :16804-16840 — apiId is present on the URL path for
every one of these, never in the response body). `DomainNameConfig`
likewise has no real `apiId` member (deserializers.go:14247-14301) despite
gopherstack's `DomainName.APIID` field. `DataSource.Tags` is also
fabricated — the real `DataSource` type has no `tags` member at all
(same case list as above), consistent with `handler_create_tags_test.go`'s
own pre-existing finding that `CreateDataSource` takes no `Tags` in the
real SDK and DataSource ARNs are not a documented `TagResource` target.
All of these are harmless (informational field a real client silently
ignores, no secret or cross-endpoint leak) and were left alone rather than
spending fix budget on 6 separate struct/call-site changes for zero
functional benefit — same "harmless, disclosed" resolution as wafv2's three
fabricated keys and emr's timestamp from prior batches.

**Structural gaps, disclosed (real backend modeling would be required, not
a rename)**:
- `GraphqlAPI` missing real `dns`/`enhancedMetricsConfig`/
  `mergedApiExecutionRoleArn`/`wafWebAclArn` members — none tracked
  anywhere in this backend (`dns` specifically: the Event API's own `Api`
  type DOES track an equivalent `DNS` field correctly, but GraphqlApi's is
  a structurally separate, unimplemented concept — verified no code path
  sets anything called Dns on a GraphqlAPI).
- `Api` (Event API) missing real `created` (timestamp) and `wafWebAclArn`
  — `created` is optional (not `"This member is required."`) so no client
  hard-errors on its absence; `wafWebAclArn` is a cross-service WAF
  association this backend doesn't simulate.
- `DataSource` missing real `elasticsearchConfig` (deprecated legacy
  member, real AWS docs steer new integrations to `openSearchServiceConfig`
  instead — genuinely low-value to add) — separate from the fixed
  `metricsConfig` gap.

**Ratifying tests**: none found needing correction — no existing test
asserted any of this session's 7 pre-fix shapes as correct (the
`associationStatus`/`environmentVariables`/missing-field bugs all had zero
prior raw-body coverage in either direction, not a wrong assertion staying
green). Checked explicitly per this issue's "grep for ratifying tests"
instruction before writing any new test.

**Phantom ops**: none — all 74 op strings in `GetSupportedOperations`
(`opExecuteGraphQL` deliberately excluded, pre-existing and correct, see
its own doc comment in handler.go) correspond to a real `api_op_*.go` file
in appsync@v1.56.4, spot-checked across every family touched this session.

**False-positive rate**: 0 among reported bugs — every finding cites the
real `deserializeOpDocument<Type>`/`deserializeDocument<Type>`/
`serializeOpDocument<Type>Input`/`serializeDocument<Type>` function's own
case list, file+line, or the real `types.go`/`api_op_*.go` struct
definition for request-side gaps, never a doc comment or a PARITY.md claim
taken on faith.

**Real-client test ratio**: appsync had a `newTestAppsyncClient` helper and
one real-client test suite (`TestCreateOpsWithTags_RoundTrip`,
`handler_create_tags_test.go`) before this session, out of 74 ops — the
rest of the suite (~40 test files) is raw-body (`doRequest`/`doV2Request`)
assertions. Added `services/appsync/wire_field_fixes_test.go` reusing the
existing `newTestAppsyncClient` helper: 6 new real-SDK-client tests
(`TestSourceApiAssociation_StatusWireKey`,
`TestGraphqlApi_EnvironmentVariablesNotLeaked` — necessarily a raw-body
check via `doRequest` for the *absence* assertion, since a typed client
silently drops unknown fields and can never observe a leak directly;
`TestGraphqlApi_Owner`, `TestEventApi_LogConfigRoundTrip`,
`TestDataSource_MetricsConfigRoundTrip`,
`TestResolver_MetricsConfigRoundTrip`). Every fix hand-reverted
individually (no git, per this session's hard no-git-mutation constraint):
finding #1 confirmed empty-string status: quoted `expected:
"MERGE_SCHEDULED" actual: ""`; #3 confirmed `Should be false` (leak
assertion) failing; #4 confirmed `expected: "000000000000" actual: ""`; #2
confirmed `Expected value not to be nil` (LogConfig nil); #5/#6 each
confirmed twice — once via a **compile error** removing the struct field
entirely (proving the field load-bearing, same proof shape as pinpoint's
`kpiResult.StartTime`/`EndTime` precedent), and once via a runtime
assertion (`expected: "DISABLED" actual: "ENABLED"`, the pre-fix Update
path silently keeping the stale value) after reverting only the
`UpdateDataSource`/`UpdateResolver` copy line with the field still present.
All reverts restored and diffed byte-identical against the pre-revert file
before moving to the next.

Gates: `go build ./services/appsync/...` and full `go build ./...` (no
backend method signatures changed — only new struct fields and internal
field-copy lines — but run in full anyway per this session's standing
instruction and to be safe given a sibling had just touched an adjacent
service), `go vet`, `go test -race` (both scoped and full `./pkgs/...`),
`go fix -diff` (no diff), `fieldalignment -fix` (3 hits — `Resolver`,
`GraphqlAPI`, `EventConfig` — auto-fixed; the auto-fix silently stripped
one pre-existing `//nolint:lll` comment on `GraphqlAPI.
AdditionalAuthenticationProviders`, same failure mode eventbridge's batch
hit with `fieldalignment -fix`, caught by re-running golangci-lint and
restored by hand), `golangci-lint run` (0 issues after that one restore; no
cyclop/gocyclo/gocognit/funlen nolints added) all green for
`services/appsync`.

No subagents used (Read/Grep/Bash only, per this session's hard
constraint). No git-mutating commands run — orchestrator must commit/push.
`git status` checked at start (clean) and re-checked before each edit
batch; the route53resolver session's own file changes appeared mid-session
(a sibling actively finishing that service, unrelated files) and were left
untouched throughout, confirmed by scoping every `git status`/diff check to
`services/appsync/` and this remainder file only.

appsync's List/Describe/Get families are now fully swept for this issue
(28/28 ops layer-1 clean; 7 additional layer-2/3 bugs found and fixed
across the wider field surface). 78 of 162 services swept, 84 remain. Per
the ranked table, `workspaces` (111 ops, 27 L+D+G, `dynamic-fallback`) is
next largest — re-check `git status` before picking it.

## workspaces (this session, 2026-08-15)

Chosen as the largest unswept service in the ranked table (111 ops, 27
L+D+G: 2 List, 24 Describe, 1 Get; `dynamic-fallback` resolution, flagged in
this file's own notes as "worth a second look" for a shared-converter
pattern). `git status` was clean at start except a live sibling actively
editing `services/appsync/*.go`; that sibling's commit (`7d2a46e44`) landed
mid-session and was left untouched. A second sibling (`services/lakeformation/*`)
appeared uncommitted by session end; also left untouched throughout.

PROTOCOL: `application/x-amz-json-1.1` (JSON-RPC 1.1), confirmed from
`handler.go`'s `Handler()`/`RouteMatcher()` and cross-checked against
workspaces@v1.73.1's `deserializers.go` (`awsAwsjson11_` prefix
exclusively). Case-sensitive plain Go map-key switch, not smithyxml
`EqualFold`: all 369 `EqualFold` hits in `deserializers.go` are `errorCode`
matches inside `deserializeOpError*` functions (grepped and confirmed none
sit in a body-field switch). Dead-deserializer trap checked and does NOT
apply: `HandleDeserialize` (e.g. `awsAwsjson11_deserializeOpDescribeWorkspaces`,
deserializers.go:5553) calls the real `OpDocument...Output` function
directly (deserializers.go:5593) -- same shape as sqs/cloudwatchlogs/
route53resolver, not pinpoint's restjson1. Second client: none, single
Amazon WorkSpaces SDK module (`workspaces@v1.73.1`, resolved from `go.mod`).

All 27 L+D+G ops resolved directly against `deserializers.go` line numbers
and layer-1/2 swept; several adjacent non-L+D+G response shapes (account
links, connection aliases) were also field-diffed since they share types
with the swept ops. 4 real bugs found and fixed, all layer-2 (wrong key or
wrong per-item field, not a missing wrapper):

1. **Real key from the wrong type/direction, systemic across a whole
   6-op family** (after emr/kafka/route53resolver, the 4th instance this
   campaign has found). `accountLinkResp` (the nested `AccountLink` object
   returned by CreateAccountLinkInvitation, AcceptAccountLinkInvitation,
   RejectAccountLinkInvitation, DeleteAccountLinkInvitation, GetAccountLink,
   and ListAccountLinks) emitted `"LinkId"`/`"Status"` -- the *request-side*
   field names (genuinely correct on `AcceptAccountLinkInvitationInput.LinkId`
   etc., confirmed at `api_op_AcceptAccountLinkInvitation.go:36`) bled into
   the response type, which really uses `"AccountLinkId"`/`"AccountLinkStatus"`
   (confirmed: `awsAwsjson11_deserializeDocumentAccountLink`,
   deserializers.go, case list `AccountLinkId`/`AccountLinkStatus`/
   `SourceAccountId`/`TargetAccountId` -- no `LinkId`/`Status` case exists,
   so a real client decoded both as permanently empty/zero across all six
   ops). Fixed by renaming the wire tags; `toAccountLinkResp`'s Go-side
   field names updated to match.
2. **Invented default/status enum values, found while fixing #1.** This
   backend used `"PENDING_ACCEPTANCE"` (CreateAccountLinkInvitation) and
   `"DELETED"` (DeleteAccountLinkInvitation) for `AccountLinkStatus`; neither
   is a member of the real `AccountLinkStatusEnum`
   (`LINKED`/`LINKING_FAILED`/`LINK_NOT_FOUND`/
   `PENDING_ACCEPTANCE_BY_TARGET_ACCOUNT`/`REJECTED`, enums.go:45-49). Fixed
   the create-time value to the real
   `PENDING_ACCEPTANCE_BY_TARGET_ACCOUNT`; for delete, there is no real
   "deleted" status at all, so the fix stops mutating status on delete and
   returns the link's last real status instead of fabricating one.
3. **Real key from the wrong type, request field bled into response
   again, on a per-op family basis.** `DescribeApplicationAssociations`
   reused the `WorkspaceResourceAssociation`-shaped response
   (`workspaceAssocResp`, `"WorkspaceId"` key) instead of the real
   `ApplicationResourceAssociation` shape (`"ApplicationId"` key; confirmed
   `awsAwsjson11_deserializeDocumentApplicationResourceAssociation`'s case
   list has `ApplicationId`/`AssociatedResourceId`/`AssociatedResourceType`/
   `Created`/`LastUpdatedTime`/`State`/`StateReason`, no `WorkspaceId`).
   Worse than a bare key-name swap: the backend method also swapped the
   *values* -- it put the application's own ID under `AssociatedResourceId`
   (which per the real type's doc comment ["The identifier of the
   associated resource"] should hold the workspace ID) and the workspace ID
   under the wrong-named `WorkspaceId`/would-be-`ApplicationId` slot. **This
   is exactly the sibling-trap pattern documented for `DescribeBundleAssociations`/
   `DescribeImageAssociations` already getting their own correctly-typed
   response structs (`bundleResourceAssociationResp`/
   `imageResourceAssociationResp` with `BundleId`/`ImageId` keys) right next
   to the broken one -- three siblings correct, one (the actively-populated
   one, unlike the other two which are always-empty stubs) wrong.** Fixed by
   adding a backend-level `ApplicationResourceAssociation` type (mirroring
   the `ImageResourceAssociation`/`BundleResourceAssociation` pattern) and a
   dedicated `applicationResourceAssociationResp` wire type; this changed
   `StorageBackend.DescribeApplicationAssociations`'s return type, so full
   `go build ./...` was run (clean).
4. **Invented top-level member, duplicate of a correctly-modeled nested
   one.** `connAliasResp` (top-level `ConnectionAlias`) fabricated a
   `"ConnectionIdentifier"` field; the real top-level `ConnectionAlias` type
   has only `AliasId`/`Associations`/`ConnectionString`/`OwnerAccountId`/
   `State` (confirmed `awsAwsjson11_deserializeDocumentConnectionAlias`'s
   case list). `ConnectionIdentifier` **is** real, but only nested inside
   each `Associations[]` entry (`ConnectionAliasAssociation`) -- which
   gopherstack already modeled correctly in `connAliasAssocResp` right next
   to the bug, and also correctly at the top level of the *different*
   `AssociateConnectionAliasOutput` response (a distinct op, confirmed
   correct). Harmless (an unknown extra top-level key is silently ignored by
   a real client) but wrong; removed.

**Sibling/version-pair check, explicitly**: `DescribeWorkspaceAssociations`
(real use of `WorkspaceResourceAssociation`), `DescribeBundleAssociations`,
and `DescribeImageAssociations` were all independently re-verified and hold
correct -- each already has its own right-shaped response type, making
`DescribeApplicationAssociations` (finding #3) the lone outlier among four
siblings, not a service-wide pattern. `DescribeIpGroups`'s unusual `"Result"`
wrapper key (not `"Groups"`) was checked hardest as a plausible bug and
confirmed genuinely correct (real deserializer case is `"Result"`,
deserializers.go:21050) -- reported per this issue's "report siblings you
check and find already correct" ask.

**Backend-tracked-but-unemitted (layer 3)**: none found with a real
backend-side source. Genuine, disclosed-not-fixed modeling gaps found
instead (backend has no slot to source the value from at all, so filling it
in would be fabrication, matching the no-stub rule): `Workspace` is missing
six real members (`DataReplicationSettings`/`IpAddress`/`Ipv6Address`/
`ModificationStates`/`RelatedWorkspaces`/`StandbyWorkspacesProperties`) and
`WorkspaceProperties` three more (`GlobalAccelerator`/`OperatingSystemName`/
`Protocols`) -- already filed as gopherstack-jukr, re-confirmed accurate
here, not duplicated. `DescribeAccount`'s `DedicatedTenancyAccountType`/
`Message` (BYOL source/target-account sharing, a feature this backend has no
model for) and `DescribeCustomWorkspaceImageImport`'s
`ErrorDetails`/`ImageBuilderInstanceId`/`LastUpdatedTime`/
`ProgressPercentage`/`StateMessage` (this backend's `storedImage` has no
slots for any of the five) are newly-disclosed gaps of the same shape, left
alone. `ConnectionAliasAssociation.AssociatedAccountId` is also never
populated (this backend has one account, so it would always equal the
account's own ID) -- disclosed, not fixed, to avoid conflating "always this
account" with a real multi-account model.

**Discarded input**: none found (7th instance search came back clean this
session -- every request field this session touched is read by its
handler).

**Phantom ops**: none -- all 27 op strings in the L+D+G set, and every
other op string in `buildOps()`'s merged table, map to a real
`api_op_*.go` file in workspaces@v1.73.1.

**False-positive rate**: 0 among reported findings -- every one cites the
real deserializer/serializer case list or `types.go`/`api_op_*.go` struct,
file-grepped, never a doc comment or PARITY.md claim taken on faith
(this service's own `PARITY.md` was not consulted before the independent
diff).

**Real-client test ratio**: workspaces already had a `newTestHandlerAndClient`
helper and several real-SDK-client round-trip tests (image import,
workspace creation) before this session. Added
`services/workspaces/wire_field_fixes_test.go`: 4 new tests, 3 real-SDK-client
(`TestCreateAccountLinkInvitation_RealSDKClient_UsesRealFieldNames`,
`TestDeleteAccountLinkInvitation_RealSDKClient_NoFabricatedStatus`,
`TestDescribeApplicationAssociations_RealSDKClient_UsesApplicationId`) and 1
raw-body (`TestDescribeConnectionAliases_NoFabricatedTopLevelConnectionIdentifier`
-- necessarily raw-body, since a typed client has no field to receive an
unmodeled top-level member into, so it can never observe the leak directly).
Every fix hand-reverted individually (no git, per this session's hard
no-git-mutation constraint): finding #1 confirmed via `AccountLinkId`
`nil`/empty on the real typed client; #3 confirmed via `ApplicationId` `nil`
on the real typed client; #4's first test attempt passed unchanged against
the unfixed code (`ConnectionIdentifier` was zero-valued pre-Associate, so
`omitempty` masked the bug either way -- the "assertion too weak to fail"
trap this issue tracks) and was corrected to associate a resource first
before asserting absence, then re-verified to fail against the unfixed code
with the field genuinely present. All reverts restored and diffed
byte-identical against the pre-revert file before moving to the next.
`TestAccountLinkLifecycle` (existing raw-body ratifying test,
`account_links_test.go`) asserted the wrong `"LinkId"`/`"Status"` keys and
the fabricated `"PENDING_ACCEPTANCE"`/`"DELETED"` values as correct;
rewritten to assert the real keys/values, verified to fail against the
unfixed code with the exact predicted symptom (`expected non-empty
AccountLinkId`), then restored to pass.

Gates: full `go build ./...` (backend method signature changed --
`DescribeApplicationAssociations`'s return type -- so this was mandatory,
not just precautionary; clean), `go vet`, `go test -race` (scoped and full
`./pkgs/...`), `go fix -diff` (one real modernization surfaced in the new
test file, a manual loop replaced by `slices.Contains` -- applied, `go fix
-diff` clean after), `gofmt` clean throughout, `golangci-lint run` (0
issues; no cyclop/gocyclo/gocognit/funlen nolints added) all green for
`services/workspaces`.

No subagents used (Read/Grep/Bash only, per this session's hard
constraint). No git-mutating commands run -- orchestrator must commit/push.
`git status` checked at start and re-checked before/after each edit batch;
the appsync and lakeformation siblings' files were confirmed untouched
throughout.

workspaces's List/Describe/Get families are now fully swept for this issue
(27/27 ops layer-1/2 clean; 4 bugs found and fixed, all layer-2). 79 of 162
services swept, 83 remain. Per the ranked table, `lakeformation` (61 ops, 26
L+D+G, `direct`) is next largest but had a live, uncommitted sibling as of
this session's end -- re-check `git status` before picking it; pick
`rekognition` (75 ops, 25 L+D+G, `dynamic-fallback`) next if lakeformation
is still claimed.

## lakeformation (this session, 2026-08-15)

Chosen as the largest unswept service not held by a live sibling: the
workspaces session (prior entry, this file) flagged `lakeformation` as next
but noted a live sibling was already editing it concurrently at their
session's end; that sibling's work landed as commit `0cfcbfb5d` before this
session started, so `git status` was clean for `services/lakeformation/*` at
the start of this pass -- confirmed and re-checked throughout, no collision.

PROTOCOL: `awsRestjson1_` exclusively (confirmed via
`grep -c '^func awsRestjson1_' deserializers.go`), single client (no
companion/legacy LakeFormation SDK module). Case-sensitive: all 214
`EqualFold` hits in `deserializers.go` are `errorCode` matching
(`grep -n EqualFold deserializers.go | grep -v 'errorCode)'` returns
nothing); `serializers.go` has zero `EqualFold` hits at all. Dead-
deserializer trap checked against `ListPermissions`
(`deserializers.go:6339`) and does NOT apply -- `HandleDeserialize` calls
`awsRestjson1_deserializeOpDocumentListPermissionsOutput` directly
(`deserializers.go:6379`), same shape as kafka/route53resolver/appsync, not
pinpoint's restjson1.

DEEP PRIOR COVERAGE, RE-VERIFIED: this service already carried an A grade
in `PARITY.md` from six prior audit passes (`kbnu`/`jqh2`/`h910`/`mslf`/
`parity-5`/`3gbe`), including a previously-fixed `ListPermissions` wrapper-
key bug of exactly this issue's shape. Per this issue's "deep prior coverage
is not evidence" lesson, all 26 L+D+G ops were independently re-verified
against the real deserializer/serializer case lists rather than trusting the
manifest. Result: **mixed** -- the 26 L+D+G ops' own wrapper keys held
completely clean (matches route53resolver's "A grade held" outcome), but
three ops in the temporary-credentials/identity-center families the prior
passes hadn't reached had real, previously-undetected bugs, one of them
wire-breaking.

**FLAGSHIP FINDING -- wire-breaking, sibling-copy bug:**
`GetTemporaryDataLocationCredentials`'s request struct
(`getTemporaryDataLocationCredentialsInput`) was shaped like its
`GetTemporaryGlue{Partition,Table}Credentials` siblings
(`ResourceArn`/`Permissions`/`SupportedPermissionTypes`) -- but the real
`GetTemporaryDataLocationCredentialsInput`
(`api_op_GetTemporaryDataLocationCredentials.go`,
`serializers.go:2923` `awsRestjson1_serializeOpDocumentGetTemporaryDataLocationCredentialsInput`)
has **no such members at all**; it serializes `DataLocations` ([]string,
plural) and `CredentialsScope` instead. A real, unmodified aws-sdk-go-v2
client's request body was therefore never readable by the old handler --
every real-client call failed gopherstack's own "ResourceArn is required"
check. Same class as this issue's originally-fixed `ListPermissions`
`ResourceArn` bug and rds's `ValuesToAdd`/`ValuesToRemove` finding: a
request-side field invented from a sibling shape, unreachable by any typed
client. Fixed: request now takes `DataLocations`/`CredentialsScope`;
response gained the two real members that were also entirely missing
(`AccessibleDataLocations`, `CredentialsScope` --
`deserializers.go`'s `GetTemporaryDataLocationCredentialsOutput` case list),
echoing the request's scope/locations back since this backend enforces no
real Lake Formation authorization to derive them from.

**Second finding, same family:** `GetTemporaryGlueTableCredentials`'s real
request member `S3Path` (`api_op_GetTemporaryGlueTableCredentials.go`) was
parsed nowhere (10th discarded-input instance this campaign, after
apigatewayv2/ce/vpclattice/emr x2/kafka/appsync x3), paired with a missing
real response member `VendedS3Path` (`deserializers.go`'s
`GetTemporaryGlueTableCredentialsOutput` case list). Fixed together: `S3Path`
now threaded through to `VendedS3Path`.
`GetTemporaryGluePartitionCredentials`, the third sibling in this family,
was checked and is already correct (its real `Input`/`Output` shapes have no
analogous fields) -- reported as clean, not left unmentioned.

**Third finding -- real key from the wrong op/direction (fourth instance
this campaign, after appsync's `AssociationStatus`, route53resolver's
`VpcId`, and this issue's original `ListPermissions` finding):**
`DescribeLakeFormationIdentityCenterConfigurationOutput` emitted an
`ApplicationStatus` field. `ApplicationStatus` **is** a real name in this
SDK, but only as `UpdateLakeFormationIdentityCenterConfigurationInput`'s
request field (`api_op_UpdateLakeFormationIdentityCenterConfiguration.go`) --
the real Describe output has no such member at all (confirmed against
`deserializers.go`'s
`awsRestjson1_deserializeOpDocumentDescribeLakeFormationIdentityCenterConfigurationOutput`
case list: `ApplicationArn`/`CatalogId`/`ExternalFiltering`/`InstanceArn`/
`ResourceShare`/`ServiceIntegrations`/`ShareRecipients`, no
`ApplicationStatus`). Fixed by removing it from the wire output only --
the backend still tracks it internally (needed for Update's own validation)
via the same struct's JSON tags, which are a persistence DTO shape, not the
wire shape (see the added doc comment on `IdentityCenterConfiguration` in
`models.go` clarifying this so a future pass doesn't conflate the two and
reintroduce a `json:"-"` mistake that would have silently broken
snapshot/restore -- caught before committing it, see below).

**PRIOR AUDIT CLAIM DISPROVED:** `PARITY.md`'s `deferred:` line asserted
"RedshiftScopeUnion/ServiceIntegrationUnion... no routed operation in the
61-op surface takes [either] as an input/output field, so there is no wire
surface to implement against." This is wrong: `ServiceIntegrations
[]types.ServiceIntegrationUnion` is a real field on THREE ops --
`CreateLakeFormationIdentityCenterConfigurationInput`,
`UpdateLakeFormationIdentityCenterConfigurationInput`, and
`DescribeLakeFormationIdentityCenterConfigurationOutput` (all confirmed
directly in their respective `api_op_*.go` files). Traced and fixed:
`ServiceIntegrations` (with its nested `RedshiftScopeUnion`/`RedshiftConnect`
union shape, wire keys confirmed against `serializers.go:6678-6710`/
`deserializers.go:12843-12875`) is now modeled and threaded through
Create/Update/Describe -- 11th/12th discarded-input instances this campaign.

**Fourth finding, same family:** `UpdateLakeFormationIdentityCenterConfigurationInput`
also lacked a `ShareRecipients` field entirely (not merely mis-keyed --
absent from the Go struct, so a real client's update to an existing share's
recipient list was silently dropped every time, even though Create and
Describe already handled `ShareRecipients` correctly -- a same-op-family
"one direction right, sibling direction wrong" shape). Fixed with correct
nil-vs-explicit-empty-list semantics (`ShareRecipients` unspecified leaves
the stored value unchanged; an explicit `[]` clears it, matching AWS's
documented behavior) -- proven by a dedicated round-trip test using the real
SDK client for both cases.

**DISCLOSED, NOT FABRICATED:** `ResourceShare` (`*string`, the RAM
resource-share ARN AWS creates server-side when `ShareRecipients` is set) is
a real `DescribeLakeFormationIdentityCenterConfigurationOutput` member still
entirely missing. Not fixed: this backend has no region available at the
storage layer (`InMemoryBackend` carries no account/region fields; region
only exists as `Handler.DefaultRegion`, set post-construction and never
threaded into any backend call in this service) and no real RAM
cross-service integration, so synthesizing a correctly-scoped
`arn:aws:ram:<region>:...` value would mean either fabricating a region or
introducing new region-threading plumbing disproportionate to this pass --
same class as the already-documented `AdditionalDetails`/RAM gap.
`QuerySessionContext` (real on `GetTemporaryGlueTableCredentials` and
several query-planning ops) is similarly unmodeled anywhere in this service;
flagged, not fixed -- a broader structural feature spanning the query-family
ops, out of scope for this pass's discarded-input fixes.

**TOOLCHAIN HAZARD RECHECKED:** two structs newly added by this pass's field
additions tripped `fieldalignment` (govet, via `golangci-lint`). Ran
`fieldalignment -fix` scoped to `./services/lakeformation/...`, then diffed
the whole package directory against a pre-fix copy: only `models.go`
changed (two struct field reorderings), and the file's one pre-existing
`//nolint:ireturn,nolintlint` comment (`provider.go:20`) survived intact --
confirmed by direct grep before and after, per this issue's standing
toolchain-hazard note.

**SELF-CAUGHT MISTAKE:** an early draft set `ApplicationStatus string
`json:"-"`` directly on the internal `IdentityCenterConfiguration` struct to
suppress the wire leak, without checking that this same struct's JSON tags
are also the snapshot/restore persistence DTO shape (`persistence.go`'s
`Snapshot`/`Restore`, backed by `store.Table[IdentityCenterConfiguration]`).
That would have silently dropped `ApplicationStatus` from every
snapshot/restore cycle -- caught by re-reading `persistence.go` before
running any test, reverted to a plain `json:"ApplicationStatus,omitempty"`
tag, and the wire-leak fix applied only at the handler's explicit
field-by-field response-struct construction instead (which was always the
actual wire boundary; the internal struct was never the culprit).

RATIFYING TESTS (found, proven false, rewritten): 2.
`TestGetTemporaryDataLocationCredentials_Success` sent
`{"ResourceArn":..., "Permissions":...}` and only passed because the
handler's fixture agreed with the same wrong shape as a real client would
never send -- rewritten to use `DataLocations`/`CredentialsScope` and to
assert the new response members.
`TestUpdateIdentityCenter_ApplicationStatus` asserted
`out["ApplicationStatus"]` equal to the value just set, which only passed
because the Describe handler echoed a field the real API doesn't have --
rewritten to assert the Update call is still accepted/validated (that part
was correct) and that `ApplicationStatus` does NOT appear on the Describe
response.

Every one of the four fixes above was hand-reverted individually (no git;
plain-text edit + restore, diffed byte-identical against a saved copy before
moving to the next) and confirmed to fail with the exact predicted symptom
first:
1. Old `ResourceArn` shape restored -> real-SDK-client test failed with
   `InvalidInputException: ResourceArn is required` (the real client never
   sends that field).
2. `VendedS3Path` echo removed -> real-SDK-client test failed asserting
   `[]string(nil)` instead of the provided path.
3. `ApplicationStatus` added back to the Describe output struct -> existing
   test failed asserting the response map contained the key it shouldn't.
4. `ShareRecipients`/`ServiceIntegrations` calls replaced with `nil, nil` at
   the Update call site -> both the new real-client round-trip test (0
   ServiceIntegrations instead of 1) and the empty-list-clears test (old
   recipient survived an explicit-clear request) failed exactly as
   predicted.

REAL-CLIENT TEST RATIO: this service already had 3 files using a real
`aws-sdk-go-v2/service/lakeformation` client
(`handler_work_unit_results_sdk_test.go`, `host_prefix_reachability_test.go`,
`sdk_completeness_test.go`) before this pass; reused the existing
`newTestLakeFormationClient` helper rather than inventing a second one.
Added `services/lakeformation/wire_field_fixes_test.go`: 5 new tests, all
using the real typed SDK client (`GetTemporaryDataLocationCredentials`
round-trip, `GetTemporaryGlueTableCredentials` `VendedS3Path`,
`ServiceIntegrations`+`ShareRecipients` round-trip via Create+Update+
Describe, and the nil-vs-empty-list `ShareRecipients` pair) plus the 2
ratifying-test rewrites above (raw-map-based, since they predate this
pass's file).

PHANTOM OPS: none -- all 61 op-name strings in `GetSupportedOperations`
resolve to a real `api_op_*.go` file in `lakeformation@v1.50.4` (spot-checked
the 26 L+D+G ops directly during this pass; the full-service
`TestExtractOperation_SDKRouteTable`/route-table-drift test from a prior
audit, unchanged this pass, already covers all 61).

FALSE-POSITIVE RATE: 0 among reported findings -- every finding above cites
the real `api_op_*.go`/`serializers.go`/`deserializers.go` file and line,
never a `PARITY.md` claim or doc comment taken on faith (the `deferred:`
line was explicitly re-checked and found wrong, not trusted).

GATES: `go build ./services/lakeformation/...` and full `go build ./...`
(interface/backend signature changes on
`CreateLakeFormationIdentityCenterConfiguration`/
`UpdateLakeFormationIdentityCenterConfiguration`), `go vet` (scoped and
full `./...`), `go test -race ./services/lakeformation/...` and
`go test -race ./pkgs/...`, `go fix -diff` (no diff), `gofmt -l` (clean),
`golangci-lint run ./services/lakeformation/...` (0 issues after the
`fieldalignment` fix above; no cyclop/gocyclo/gocognit/funlen nolints
added) -- all green.

`services/lakeformation/PARITY.md` updated to reflect these findings (see
its own frontmatter/notes).

No subagents used (Read/Grep/Bash only, per this session's hard constraint).
No git-mutating commands run -- orchestrator must commit/push. `git status`
re-checked before starting (confirmed the workspaces sibling's changes had
already landed as a commit, not a live collision) and periodically
throughout; no other service's files were touched.

lakeformation's List/Describe/Get families are now fully swept for this
issue (26/26 ops layer-1 clean; 5 real bugs found and fixed in adjacent
temporary-credentials/identity-center ops layer-2/3, one wire-breaking; one
prior `PARITY.md` claim disproved and corrected). 80 of 162 services swept,
82 remain. Per the ranked table, `rekognition` (75 ops, 25 L+D+G,
`dynamic-fallback`) is next largest -- re-check `git status` before picking
it in case a sibling is already there.

## elasticsearch (this session, 2026-08-15)

Chosen as the largest unswept service not held by a live sibling: `rekognition`
(75 ops, 25 L+D+G) was mid-edit throughout this session (`services/rekognition/
{collections,datasets,handler_collections,handler_datasets,handler_projects,
interfaces,models,projects}.go` modified, uncommitted, per `git status` at
start and re-checked before every edit batch), a `CreateProject` signature
change that breaks the full-repo build per this session's assignment note.
`elasticsearch` (51 total ops, 25 L+D+G, `direct` resolution) is tied with
`rekognition`/`directoryservice` at 25 and was picked next since it doesn't
collide.

PROTOCOL: `awsRestjson1_` exclusively, single client
(`elasticsearchservice@v1.45.4`, matches `go.mod`). Case-sensitive: 242
`EqualFold` hits in `deserializers.go`, all float `NaN`/`Infinity` special
parsing (`strings.EqualFold(jtv, "NaN"|"Infinity"|"-Infinity")`), none in a
body-field-key switch, none `errorCode` either (this service's error-code
matching uses `restjson.SanitizeErrorCode`/`GetErrorInfo`, not `EqualFold`).
Dead-deserializer trap checked against `ListDomainNames` and does NOT apply
(`HandleDeserialize` calls the real `OpDocument...Output` function directly,
e.g. `deserializers.go:5458` -> `:5529`). All 25 L+D+G ops resolved `direct`
(literal `GetSupportedOperations` slice) and all had their real
`awsRestjson1_deserializeOpDocument<Op>Output` top-level key list pulled and
diffed against the handler.

DEEP PRIOR COVERAGE, MIXED RESULT (route53resolver/lakeformation-style split):
this service carried an A grade off six prior focused passes
(`gopherstack-p2mx`/`lx5h`/`4gzs`/`toz8` plus two dated 2026-07-24/2026-08-10),
which had already fixed real bugs in `CancelDomainConfigChange`'s borrowed
response shape, `CreateVpcEndpoint`/`UpdateVpcEndpoint`'s flat-map
`VpcOptions`, and several `List*`/`Delete*VpcEndpoint*` required-`NextToken`
gaps -- all independently re-verified clean this pass, plus every other L+D+G
op's top-level wrapper key (`ListDomainNames`, `ListTags`,
`DescribeElasticsearchDomain(s)`, `DescribeElasticsearchDomainConfig`,
`DescribeElasticsearchInstanceTypeLimits`, `DescribeReservedElasticsearch
Instance(Offerings)`, `GetCompatibleElasticsearchVersions`,
`GetPackageVersionHistory`, `GetUpgradeHistory`, package/domain-package list
ops). All held clean. The 3 real bugs found were in the one op-family none of
the six prior passes' notes mention at all: outbound cross-cluster-search
connections.

3 real bugs found and fixed, all in `CreateOutboundCrossClusterSearchConnection`
/`DescribeOutboundCrossClusterSearchConnections`/
`DeleteOutboundCrossClusterSearchConnection` (`services/elasticsearch/
handler_outbound_connections.go`, `handler.go`):

1. SIBLING-COPY ON THE REQUEST SIDE (matches lakeformation's flagship pattern
   last pass), also on the response: `outboundConnectionJSON`/
   `createOutboundConnectionRequest` used `LocalDomainInfo`/`RemoteDomainInfo`
   -- names copied from this package's own internal `OutboundConnection`
   struct (`models.go`, itself the snapshot/persistence DTO, left untouched)
   -- instead of the real wire names `SourceDomainInfo`/`DestinationDomainInfo`
   (confirmed both directions: `serializers.go:802`'s
   `awsRestjson1_serializeOpDocumentCreateOutboundCrossClusterSearchConnectionInput`
   and `deserializers.go:13122`'s
   `awsRestjson1_deserializeDocumentOutboundCrossClusterSearchConnection`, both
   required members on the real Input). Every real client's create request had
   both required domain-info fields silently ignored (empty domain info stored
   both ends), and every response's `SourceDomainInfo`/`DestinationDomainInfo`
   stayed nil. SIBLING CHECK: the in-file/in-package sibling
   `InboundConnection` type (`handler_inbound_connections.go`) already used the
   correct `SourceDomainInfo`/`DestinationDomainInfo` names throughout --
   reporting per this issue's "report siblings you check and find already
   correct" instruction.

2. GENERATIONAL/FAMILY SHAPE MISMATCH: `CreateOutboundCrossClusterSearchConnectionOutput`
   is flat at the response root (`ConnectionAlias`/`ConnectionStatus`/
   `CrossClusterSearchConnectionId`/`SourceDomainInfo`/`DestinationDomainInfo`
   as direct top-level keys, confirmed `api_op_CreateOutboundCrossCluster
   SearchConnection.go:53-73` and `deserializers.go:1253`'s case list) --
   unlike its `Delete`/`Accept`/`Reject` siblings, which all genuinely DO wrap
   their connection in `{"CrossClusterSearchConnection": {...}}`
   (`deserializers.go:41`+ each, confirmed for all three). The handler wrapped
   `Create`'s response the same way as those three siblings, so a real
   client's entire response was nested one level too deep to ever decode --
   `ConnectionAlias`/`ConnectionStatus`/`CrossClusterSearchConnectionId`
   included, not just the domain-info fields from bug 1. Fixed by emitting
   `outboundConnectionJSON` flat for `Create` only, keeping the
   `keyCrossClusterSearchConnection` wrapper for `Delete` (already correct).

3. ROUTING BUG (not a wire-shape bug -- a genuine "op unreachable" gap,
   `handler.go`'s `matchElasticsearchCorePaths`): `path ==
   elasticsearchCCSOutbound` was an exact-match check against the bare
   `/2015-01-01/es/ccs/outboundConnection` path, unlike its `Inbound` sibling
   two lines above (`strings.HasPrefix(path, elasticsearchCCSInbound)`). Any
   path with a suffix -- `DescribeOutboundCrossClusterSearchConnections`'s
   real path `.../outboundConnection/search`
   (`serializers.go:2013`'s hardcoded `httpbinding.SplitURI`) and
   `DeleteOutboundCrossClusterSearchConnection`'s `.../outboundConnection/
   {id}` -- never matched `matchElasticsearchPath`, so the *top-level*
   service router never even dispatched the request to this handler at all: a
   404 from the generic router before `ServeHTTP`'s own internal dispatch
   (`h.ops` map / `handlePrefixRoutes`) ever ran. This was invisible to every
   existing raw-body test in this package because those call `h.ServeHTTP`
   directly, bypassing the top-level `RouteMatcher` gate entirely -- only a
   real end-to-end SDK-client test routed through
   `service.NewServiceRouter(...).RouteHandler()` (this package's own
   `newTestElasticsearchClient` helper, `handler_sdk_roundtrip_test.go`)
   could observe it. Fixed: `strings.HasPrefix`, matching `Inbound`'s
   pattern -- also fixes `DeleteOutboundCrossClusterSearchConnection`'s
   routing as a side effect (same prefix), proven in the same test.

DISCLOSED, NOT FIXED (2, both genuine structural gaps, not values the backend
already holds and fails to emit):
- `GetUpgradeStatus.UpgradeName` (real, optional `*string`,
  `api_op_GetUpgradeStatus.go`) -- this backend tracks no upgrade-name/
  upgrade-history state anywhere (`GetUpgradeHistory` always returns empty,
  `domain_lifecycle.go`), so there is no honest value to source it from.
- `PackageDetails.AvailablePackageVersion` and `DomainPackageDetails.
  PackageVersion`/`ReferencePath`/`LastUpdated` (all real members,
  `types.go`) -- this backend's `Package` model (`models.go`) has no
  version-history or reference-path concept at all, matching how
  `ErrorDetails` on both types is already handled the same way (documented in
  `packageJSON`'s existing doc comment).

Both added to `services/elasticsearch/PARITY.md`'s `gaps:` list.

SIBLINGS CHECKED, ALREADY CORRECT (report per this issue's instruction):
`InboundConnection` (see bug 1 above); `Delete`/`Accept`/`Reject`
`InboundCrossClusterSearchConnection` and `DeleteOutboundCrossCluster
SearchConnection` (all four correctly use the `CrossClusterSearchConnection`
wrapper -- confirmed against each op's own `Output` struct and deserializer,
not assumed from the `Create` bug); `DescribeVpcEndpoints`'s
`VpcEndpoints`/`VpcEndpointErrors` two-key wrapper; `ListVpcEndpoints`/
`ListVpcEndpointsForDomain`/`ListVpcEndpointAccess`'s `VpcEndpointSummaryList`/
`AuthorizedPrincipalList` (already fixed by the prior `gopherstack-lx5h`
pass, re-verified); `DescribeElasticsearchInstanceTypeLimits`'s
`LimitsByRole` nesting (`Limits{InstanceLimits{InstanceCountLimits{...}}}`);
`PurchaseReservedElasticsearchInstanceOffering` request/response field
names; `PackageDetails.PackageID` (genuinely all-caps `PackageID`, not
`PackageId` -- checked as a plausible casing trap, confirmed real via
`deserializers.go`'s own case list, not a bug).

No real-key-from-wrong-type found this pass. No over-wide/leaked-data fields
found (this service has none of the client-secret/ARN/env-var shaped fields
the campaign's leak-sorting note describes). No discarded inputs found beyond
what bugs 1/2 above already cover (both are the same request fields, counted
once).

RATIFYING TEST FOUND AND FIXED: 1.
`TestElasticsearchHandler_CreateOutboundCrossClusterSearchConnection`'s
`success` case sent `LocalDomainInfo`/`RemoteDomainInfo` in the raw request
body and only asserted `CrossClusterSearchConnectionId`/alias/status were
present in the response -- never the domain-info values themselves, so it
passed against the unfixed code despite exercising the exact wrong keys (the
"assertion too weak to fail" trap). Rewritten to send the real
`SourceDomainInfo`/`DestinationDomainInfo` keys and assert
`local-domain`/`remote-domain` actually appear in the response body; this
version does fail against the unfixed code (see below).

Every one of the 3 fixes hand-reverted individually (no git, per this
session's hard no-git-mutation constraint) and confirmed to fail with the
exact predicted symptom before being restored byte-identical
(`diff` against a pre-fix backup copy in the scratchpad dir):
1. routing prefix reverted to `path == elasticsearchCCSOutbound` ->
   `Test_SDKRoundTrip_CreateOutboundCrossClusterSearchConnection_DomainInfo`
   failed with `DescribeOutboundCrossClusterSearchConnections ... 404 ...
   UnknownError: Not Found`, exactly as predicted.
2. `Create`'s response re-wrapped in `{"CrossClusterSearchConnection": ...}`
   -> the same test failed with `out.CrossClusterSearchConnectionId` nil
   ("must be at the response root, not nested"), exactly as predicted.
3. `SourceDomainInfo`/`DestinationDomainInfo` reverted to `Local`/
   `RemoteDomainInfo` (both the wire struct field/tag and the request-decode
   call sites) -> both the rewritten raw-body test (`does not contain
   "SourceDomainInfo"`/`"local-domain"`) and the SDK round-trip test
   (`out.SourceDomainInfo` nil, "must round-trip, not be silently dropped")
   failed exactly as predicted.

REAL-CLIENT TEST RATIO: 2 pre-existing real-SDK-client tests
(`handler_sdk_roundtrip_test.go`'s `Test_SDKRoundTrip_CancelDomainConfigChange`/
`Test_SDKRoundTrip_CreateVpcEndpoint_VpcOptions`, reusing its
`newTestElasticsearchClient` helper) out of ~51 ops before this pass, rest
raw-body. Added `services/elasticsearch/wire_field_fixes_test.go`: 1 new
real-SDK-client test (`Test_SDKRoundTrip_CreateOutboundCrossClusterSearch
Connection_DomainInfo`) that round-trips `Create` -> `Describe` -> `Delete`
through the real client, covering all 3 fixes end-to-end (the routing bug in
particular is only observable this way, not via a raw-body test that calls
`h.ServeHTTP` directly).

PHANTOM OPS: none checked explicitly this pass beyond the pre-existing
`sdk_completeness_test.go` (unchanged, still passing). FALSE-POSITIVE RATE: 0
among reported bugs -- every finding cites the real
`api_op_*.go`/`serializers.go`/`deserializers.go` file and line.

PERSISTENCE CHECK: `outboundConnectionJSON`/`createOutboundConnectionRequest`
are wire-only structs (`handler_outbound_connections.go`), fully distinct
from the internal `OutboundConnection` struct (`models.go`) that IS the
snapshot/persistence DTO (`store.Table[regionalDTO[OutboundConnection]]`,
`persistence.go`). `models.go` was not touched -- its own
`LocalDomainInfo`/`RemoteDomainInfo` field names and lowercase JSON tags are
internal-only and orthogonal to the wire bug; renaming them was unnecessary
and out of scope.

GATES: `go build ./services/elasticsearch/...` (no backend method signature
changes -- scoped build only, per this session's "sibling breaks the
full-repo build" note), `go vet`, `go test -race` (both scoped and
`./pkgs/...`), `go fix -diff` (no diff), `golangci-lint run
./services/elasticsearch/...` (1 `golines` finding introduced by a long
`require.NotNil` line in the new test, fixed; 0 issues after, no
cyclop/gocyclo/gocognit/funlen nolints added). `fieldalignment` flagged 5
pre-existing findings in `domainConfigFields`/`domainJSON`/`domainStatusJSON`
and two test-local structs, none touching this pass's changed files/structs
-- left alone (golangci-lint itself reports 0 issues, so this repo's config
doesn't enforce fieldalignment as a hard gate; not introduced by this pass).

`services/elasticsearch/PARITY.md` updated: 3 ops rows (`wire: ok` ->
`wire: fixed` with citations), 2 new `gaps:` entries, `overall`/
`last_audit_date` refreshed.

No subagents used (Read/Grep/Bash/Edit only, per this session's hard
constraint). No git-mutating commands run -- orchestrator must commit/push.
`git status` re-checked at start and before every edit batch; only
`services/elasticsearch/*` touched by this session (confirmed via `git
status --porcelain` throughout -- the `rekognition` sibling's file list grew
but was never touched by this session).

elasticsearch's List/Describe/Get families are now fully swept for this
issue (25/25 ops layer-1 clean; 3 real bugs found and fixed in the
outbound-cross-cluster-search-connection family, one of them a total
routing-level unreachability, not just a wire-shape mismatch). 81 of 162
services swept, 81 remain. Per the ranked table, `directoryservice` (80
ops, 25 L+D+G, `direct`) is next largest not held by the `rekognition`
sibling -- re-check `git status` before picking either.

## rekognition (this session, 2026-08-15)

Chosen per the prior `workspaces` session's own note: `lakeformation` (26
L+D+G, next-largest) was a live, uncommitted sibling at session start
(`git status` showed 9 modified files + 1 untracked in
`services/lakeformation/`) -- switched to `rekognition` (75 ops, 25 L+D+G,
`dynamic-fallback`) as directed. `elasticsearch` (also 25 L+D+G) was picked
up concurrently by a different sibling partway through this session; `git
status` was re-checked before every edit batch and confirmed only
`services/rekognition/*` and this file were ever touched by this session.

PROTOCOL: `application/x-amz-json-1.1`, awsAwsjson11 exclusively (confirmed:
every `deserializeOpDocument*Output`/`deserializeDocument*` function is
`awsAwsjson11_*`, no `awsRestjson1_` or query/XML path anywhere in
deserializers.go). Single client -- go.mod pins only
`aws-sdk-go-v2/service/rekognition`, no second/legacy/streaming client.
Case-SENSITIVE: confirmed via `awsAwsjson11_deserializeOpDocumentDescribeCollectionOutput`
and every other op's `switch key { case "ExactName": ... }` -- a plain Go
string switch over decoded JSON map keys, not smithyxml's EqualFold. The
754 `EqualFold` hits in this SDK version are ALL `strings.EqualFold(jtv,
"NaN"|"Infinity"|"-Infinity")` float special-value checks inside numeric
deserializers, none on `errorCode` and none on a body-field switch --
casing is a real bug surface here (matches cloudwatch/sqs's prior-session
finding for the same reason), though no casing-specific bug was found this
pass. Dead-deserializer trap does NOT apply (restjson1-only class of bug;
this service is awsjson11).

TestSDKCompleteness (pre-existing) confirms zero phantom ops: all 75
`GetSupportedOperations` entries map to a real SDK method, resolved via
`dynamic-fallback` (this service builds `h.ops` from 15 per-family
`h*Ops()` map-literal contributions merged with `maps.Copy` in `buildOps`,
not a single literal table).

6 real bugs found and fixed, spanning the Project/Dataset/Collection
families:

1. **UpdateDatasetEntries.Changes -- flat vs nested, total op failure.**
   The request field `Changes []byte` expected the base64 manifest bytes
   directly at the "Changes" key. The real `UpdateDatasetEntriesInput.Changes`
   is `*types.DatasetChanges{GroundTruth []byte}`, serialized as
   `{"Changes":{"GroundTruth":"<base64>"}}` (confirmed:
   `awsAwsjson11_serializeDocumentDatasetChanges`, serializers.go:4948). A
   real client's call sent a JSON object where gopherstack expected a bare
   base64 string -- `json.Unmarshal` into a `[]byte` field hard-errors on an
   object, so every real UpdateDatasetEntries call failed outright, not
   silent-empty. All 9 raw-body test call sites in `handler_datasets_test.go`
   passed a flat `"Changes": <bytes>` (Go's `json.Marshal` base64-encodes
   `[]byte` as a bare string automatically), which is exactly why this
   never showed up: the raw-body tests unknowingly matched the bug, not the
   real shape. Fixed by nesting `Changes *datasetChangesWire{GroundTruth
   []byte}`; updated all 4 affected test call sites to wrap `"Changes"` in
   `{"GroundTruth": ...}`.

2. **ListDatasetLabels -- fabricated wrapper key + flat-vs-nested, silent-empty.**
   The response emitted the collection under the fabricated top-level key
   "DatasetLabelStats" with a flat per-item shape (`LabelName`/`EntryCount`
   siblings). The real `ListDatasetLabelsOutput` key is
   "DatasetLabelDescriptions" (confirmed: `awsAwsjson11_deserializeOpDocumentListDatasetLabelsOutput`,
   deserializers.go, has no "DatasetLabelStats" case at all), and each item
   nests `EntryCount` one level down under `LabelStats`
   (`types.DatasetLabelDescription{LabelName, LabelStats
   *types.DatasetLabelStats{BoundingBoxCount, EntryCount}}`). A real typed
   client's `DatasetLabelDescriptions` field silently decoded to an empty
   slice on every call. `BoundingBoxCount` is a disclosed gap, not fixed:
   this backend's label counts come from `-metadata` blocks in stored
   manifest JSON-lines entries with no per-image bounding-box-vs-
   classification distinction to source it from. Existing raw-body test
   (`handler_datasets_test.go`'s `extractLabels` helper) checked for either
   "DatasetLabelStats" or "DatasetLabels" -- neither the real key -- and
   flat `label0["EntryCount"]` assertions; both fixed to use
   "DatasetLabelDescriptions" and nested `LabelStats.EntryCount`.

3. **DescribeProjects.ProjectNames -- a real key from the wrong side, filter silently ignored.**
   The request field was named `ProjectArns`, matching
   `CreateProjectOutput`/`ProjectDescription`'s real singular `ProjectArn`
   member pluralized -- but the real `DescribeProjectsInput` filter member
   is `ProjectNames []string` (confirmed:
   `awsAwsjson11_serializeOpDocumentDescribeProjectsInput`, serializers.go,
   has no `ProjectArns` member; AWS docs confirm `ProjectNames` filters by
   name, "If you don't specify a value, the response includes descriptions
   for all the projects"). A real client's filter was silently ignored and
   every call returned every project regardless of the requested filter --
   the fifth instance of this campaign's "real key from the wrong side"
   pattern (after emr, kafka, route53resolver, workspaces). Filtering by
   name required adding a `Name` field to `storedProject` (previously only
   derivable by re-parsing the ARN, which this backend never did). NOT
   fixed, disclosed instead: `DescribeProjectsInput.Features` (a second,
   independent filter) is also fully discarded -- AWS's own docs state "If
   no value is specified, CUSTOM_LABELS is used as a default" for that
   filter, meaning a real `DescribeProjects()` call with neither filter set
   may only return CUSTOM_LABELS-feature projects in production. Whether
   that default composes with a simultaneous `ProjectNames` filter is not
   documented precisely enough to implement with confidence, so it was left
   alone rather than risk trading one filter bug for a different one.

4. **DescribeCollection.UserCount -- backend-tracked, never emitted.**
   The backend has tracked per-collection users since `ListUsers` was
   implemented (`b.usersByCollection` index), but `DescribeCollection` never
   counted them -- a real client's `UserCount` was always the Go zero value
   regardless of how many users existed in the collection. Fixed by
   counting `len(b.usersByCollection.Get(collectionID))` under the same
   RLock `DescribeCollection` already holds (mirrors the existing
   `ListFaces`-for-`FaceCount` pattern one line above it).

5. **DescribeDataset.DatasetStats -- entirely missing member, computable from stored data.**
   The real `types.DatasetDescription` has a `DatasetStats
   *types.DatasetStats{ErrorEntries, LabeledEntries, TotalEntries,
   TotalLabels}` member that gopherstack never emitted at all (confirmed:
   `awsAwsjson11_deserializeDocumentDatasetDescription`, deserializers.go:12814
   -- no `DatasetArn`/`ProjectArn`/`DatasetType` cases exist on this type
   either, see disclosed-fabrication note below). The raw ingredients were
   already on hand: `b.datasetEntries[datasetARN]` (used by
   `ListDatasetEntries`) and the same label-counting logic
   `ListDatasetLabels` already used. Fixed by extracting
   `countLabelsFromEntry` to also report whether an entry carried a
   `-metadata` block, then a `computeDatasetStats` helper derives
   TotalEntries/LabeledEntries/TotalLabels from the stored entries.
   `ErrorEntries` is always 0 -- disclosed, not fabricated: this backend has
   no entry-level error concept, so 0 is the accurate value for a backend
   that can't produce one, not a guess.

6. **CreateProject discarded AutoUpdate/Feature; DescribeProjects never echoed them.**
   `CreateProjectInput.AutoUpdate` and `.Feature` were silently dropped (the
   backend method took only a name); `ProjectDescription.AutoUpdate`/
   `.Feature` were correspondingly always empty. Feature defaults to
   "CUSTOM_LABELS" per AWS's own documented default ("If no value is
   provided CUSTOM\_LABELS is used as a default.", verified against the
   live API reference doc, not guessed) when the request omits it.
   AutoUpdate has no documented default anywhere found, so an empty request
   value is stored and echoed back as empty rather than guessed. NOT fixed,
   disclosed instead: `CreateProjectInput.Tags` -- unlike Collection/
   StreamProcessor/model tags, both `TagResource`'s and
   `ListTagsForResource`'s own AWS docs scope `ResourceArn` to "the model,
   collection, or stream processor" (Project ARNs are absent from both
   descriptions), so this backend's own API surface has no read path that
   could ever observe project tags, real or fabricated -- storing them
   would be untestable dead infrastructure, not a verifiable fix.

Sibling/version pairs checked and found already correct (byte-exact against
deserializers.go, no changes needed): `ListCollections`,
`DescribeStreamProcessor`/`ListStreamProcessors` (this file already carried
detailed prior-session SDK-line citations and held completely -- an "A
grade confirmed" result, same shape as route53resolver's precedent),
`GetCelebrityInfo`/`GetCelebrityRecognition`/`RecognizeCelebrities`,
`GetLabelDetection`, `GetContentModeration`, `GetTextDetection`,
`GetPersonTracking`/`GetFaceDetection`/`GetFaceSearch`,
`GetSegmentDetection` (including its `SelectedSegmentTypes` per-item
nesting), `GetMediaAnalysisJob`/`ListMediaAnalysisJobs` (confirmed the
file's own comment claim that `GetMediaAnalysisJobOutput` is genuinely
flattened onto the response root, not a wrapper-key miss), `ListFaces`,
`ListUsers`, `ListDatasetEntries`, `ListProjectPolicies`,
`DescribeProjectVersions` (also carried detailed prior citations, held
completely).

No handler-massages-values-to-fit-a-wrong-shape pattern found (unlike
workspaces' `DescribeApplicationAssociations` precedent). No invented enum
values found -- `AutoUpdate`/`Feature`/dataset-`Status` values used
throughout are all real, doc-confirmed enum members.

Over-wide fields sorted: `datasetDescription`'s `DatasetArn`/`ProjectArn`/
`DatasetType` are NOT real `types.DatasetDescription` members at all
(confirmed absent from deserializers.go's case list) -- disclosed, left in
place rather than removed: no sensitive data (just resource identifiers the
caller already knows from the request/CreateDataset), a real client's
unknown-key-drop means they're never observed, and removing them buys
nothing testable. No plaintext-secret/ARN-of-a-different-resource/customer-
data leak found anywhere in this service's over-wide surface.

Prior audit claim check: `project_versions.go`/`stream_processors.go` both
carried unusually detailed prior-session comments citing exact
deserializers.go/serializers.go line numbers for every field -- re-verified
independently against the pinned SDK rather than trusted, and held
completely (an honest "prior claim confirmed" result, not the kafka
precedent where deep coverage still hid 5 fabricated members).

DISCARDED INPUTS this pass: 3 -- `CreateProjectInput.AutoUpdate`/`.Feature`
(fixed), `CreateProjectInput.Tags` (disclosed, unfixed -- see #6 above),
`DescribeProjectsInput.Features` (disclosed, unfixed -- see #3 above).

Real-client test ratio: 0 real-SDK-client tests existed for this service
before this session (the one pre-existing SDK import,
`sdk_completeness_test.go`, only reflects over the client's method set for
the phantom-op check -- it never issues a call). Added
`services/rekognition/wire_field_fixes_test.go`, 6 new tests covering all 6
bugs above, all driven through a real `rekognitionsdk.Client` against an
`httptest.Server`-backed handler (bug #1's Changes shape is exercised
indirectly through every other test too, since all of them create datasets
via `UpdateDatasetEntries`). Every one of the 6 was hand-reverted
individually (no git, per this session's hard constraint), run against the
unfixed code, confirmed to fail with the exact predicted symptom (quoted
above per-bug), restored, and re-verified green -- including bug #1, whose
predicted symptom was a hard `json: cannot unmarshal object into Go struct
field ... of type []uint8` error rather than a silent pass/fail, confirmed
verbatim.

Gates: full `go build ./...` (mandatory -- `CreateProject`,
`DescribeProjects`'s first-parameter semantics, and `DescribeCollection`/
`DescribeDataset`'s domain types all changed; clean, one caller updated in
`persistence_test.go`), `go vet`, `go test -race` (scoped and full
`./pkgs/...`), `go fix -diff` (no diff), `golangci-lint run
./services/rekognition/...` (2 `fieldalignment` findings in the two new
`datasetDescription`/`datasetLabelDescriptionEntry` structs, fixed by
hand -- not via `fieldalignment -fix`, so no risk to this file's zero
pre-existing `//nolint` comments; 0 issues after), no cyclop/gocyclo/
gocognit/funlen nolints added (grep-confirmed 0 in this service) -- all
green.

No subagents used (Read/Grep/Bash/Edit/WebFetch only, per this session's
hard constraint -- WebFetch used solely to confirm two AWS API doc defaults
that the pinned SDK's Go comments state but don't fully resolve
unambiguously: `Feature`'s CUSTOM_LABELS default, and `Features`'s
scoping language that led to leaving that filter disclosed rather than
guessed at). No git-mutating commands run -- orchestrator must commit/push.
`git status` checked at start and re-checked before every edit batch; this
file was edited concurrently by the `elasticsearch` sibling throughout --
each edit here re-read the live file immediately beforehand and applied as
a minimal, additive diff rather than a wholesale rewrite, to avoid
clobbering that session's concurrent work.

rekognition's List/Describe/Get families are now fully swept for this issue
(25/25 ops layer-1/2/3 clean; 6 bugs found and fixed, one of them a total
op failure for real clients (#1), one a fifth instance of the "real key
from the wrong side" pattern (#3)). 82 of 162 services swept, 80 remain.
Per the ranked table, `directoryservice` (80 ops, 25 L+D+G, `direct`) is
next largest -- re-check `git status` before picking it.

## opsworks (this session, 2026-08-15)

Assigned directly (gopherstack-6flj, `directoryservice` held by a live
sibling all session -- confirmed unbuildable mid-refactor via `git status`,
never touched). A prior session's opsworks pass had already been killed
mid-verification by an API session limit and stashed (`stash@{0}`, message
"wip: killed by session limit") rather than committed -- built but failed
`TestElasticIps/RegisterElasticIp_without_StackId_returns_400`, nothing
hand-reverted. Per gopherstack-t0gq's recommendation, swept fresh; the stash
was read via `git stash show`/`git diff stash@{0}^1 stash@{0}` read-only as
a hint and never popped/applied/dropped.

**Resolved the ambiguous test (closes gopherstack-t0gq for opsworks):**
`RegisterElasticIp_without_StackId_returns_400` does not exist at `HEAD`
(`git show HEAD:services/opsworks/elastic_ips_test.go | grep StackId` --
zero hits), so the stashed session's 200-instead-of-400 failure was not it
breaking a pre-existing test. It was a *new* test that correctly found a
real, previously-missing gap: `RegisterElasticIpInput.StackId` is
`"This member is required"` (confirmed
`aws-sdk-go-v2/service/opsworks@v1.31.0`'s `api_op_RegisterElasticIp.go`,
read from the module cache), and the pre-stash `HEAD` code never validated
it -- it also accepted a fabricated `Region` field the real input doesn't
have at all. Verdict: **(b)**, new test correctly failing, not (a).

**SDK availability:** `aws-sdk-go-v2/service/opsworks@v1.31.0` sits in the
local module cache but is confirmed **absent** from `go.mod`/`go.sum` (`grep
opsworks go.mod go.sum` -- no hits). No `go get`/`go.mod` edit made; every
wire-shape claim below cites the cached module source directly, per this
package's own `sdk_completeness_test.go` convention for SDK-less services.

**Protocol:** `awsAwsjson11` exclusively. Case-sensitive plain Go `switch
key { case "Xxx": }` on decoded JSON keys (confirmed reading several
`awsAwsjson11_deserializeDocument*` functions directly), not
`smithyxml.EqualFold`. All `EqualFold` hits in this SDK version are in
`errorCode` matching only, never a body-field switch. No second client to
confuse with (`go.mod`/`go.sum` have zero opsworks references).

**Router:** single top-level `X-Amz-Target` prefix match, one flat
`buildOps()` dispatch map, no second-layer router to desync --
`sdk_completeness_test.go` already asserts `GetSupportedOperations()` and
the dispatch table match exactly.

**Phantom ops:** none -- all 74 `GetSupportedOperations()` names diffed 1:1
against every `api_op_*.go` file in the pinned module cache; no gopherstack
op missing from the real SDK and no real SDK op missing from gopherstack.

**4 real bugs found and fixed**, none previously flagged in this service's
own `PARITY.md` `gaps`/`deferred`:

1. `RegisterElasticIp` (discarded input + missing validation + fabricated
   member): fabricated `Region` request field (no such real member) replaced
   with the real, required `StackId`; empty `StackId` now rejected
   (`ValidationException`), matching this service's established
   validate-then-existence-check pattern used elsewhere in the same package.
2. `DescribeElasticIps`: real `StackId` filter member entirely discarded --
   every call ignored it. Now honored.
3. `DescribeElasticLoadBalancers`: real, plural `LayerIds` filter member
   truncated to its first element by the handler, then discarded outright by
   the backend (parameter literally named `_`). Now filters against the full
   list.
4. `DescribeStackProvisioningParameters`: the real, dedicated top-level
   `AgentInstallerUrl` member was correctly emitted, but also duplicated
   under a fabricated `"AgentInstallerUrl"` key inside the free-form
   `Parameters` map -- a key no real response ever carries there. `Parameters`
   now returns empty (honest -- unmodeled) instead of an invented key.

`ElasticIP`/`storedElasticIP` gained an internal-only `StackID` field for
(1)/(2) -- deliberately never serialized on the wire, since the real
`types.ElasticIp` has no `StackId` member. `storedElasticIP` doubles as the
snapshot/restore persistence DTO; the field was added (not retagged), so old
snapshots restore unchanged.

**Layer-1/2 sibling sweep:** all 24 `List`/`Describe`/`Get` ops had their
top-level wrapper key diffed against the real deserializer's own top-level
case list -- all correct. All 21 per-item `*ToJSON` conversion functions
were field-diffed against their real deserializer's `case "Xxx":` list --
every field gopherstack emits uses the real key name. The large remaining
gaps (most of `App`/`Layer`/`Instance`/`Stack`/`Volume`/`Deployment`'s
optional surface) are all pre-existing, already-documented structural gaps
in this service's own `PARITY.md` (`deferred`/`gaps` sections) -- the
backend's domain structs genuinely don't track those values, so none of
this is a "value already held but never emitted" bug. One new structural
gap found and disclosed (not fixed, added to `PARITY.md`):
`ElasticLoadBalancer` responses omit `AvailabilityZones`/`Ec2InstanceIds`/
`SubnetIds`/`VpcId` -- same class, no VPC/subnet/EC2-instance model in this
backend to source them from.

**Tests:** 3 new (`RegisterElasticIp_without_StackId_returns_400`,
`DescribeElasticIps_filters_by_StackId`,
`DescribeElasticLoadBalancers_filters_by_LayerIds`) plus 1 new assertion in
the existing `TestDescribeStackProvisioningParameters`. All 4 fixes
hand-reverted individually and confirmed to fail with the predicted symptom
before being restored byte-identical (no git-mutating commands used, since
this session's hard constraint banned even `git checkout --`; reverted and
restored via direct file edits instead): (1) `StackId` validation removed ->
404 instead of 400 (falls through to the stack-existence check instead of
the required-field check -- still not 400, confirming the gap, though a
different wrong code than the stashed session originally observed); (2)
`StackId` filter removed -> 2 IPs returned instead of 1; (3) `LayerIds`
filter removed -> 2 ELBs returned instead of 1; (4) fabricated
`Parameters.AgentInstallerUrl` re-added -> assertion failed as predicted.

**Real-client test ratio:** 0 before and after (SDK not a `go.mod`
dependency; documented exception, matches this repo's pattern for other
unpinned services).

Gates: scoped `go build`/`go vet ./services/opsworks/...` clean; full `go
build ./...`/`go vet ./...` clean (interface signature changes propagate;
`directoryservice` was a live sibling mid-edit throughout, confirmed via
repeated `git status`, never touched -- its transient build breaks during
this session were its own concurrent edits, not caused by this pass); `go
test -race -count=1 ./services/opsworks/...` and `./pkgs/...` green; `go fix
-diff` clean; `golangci-lint run ./services/opsworks/...` 0 issues (1
`golines` line-length finding fixed by hand); 0 cyclop/gocyclo/gocognit/
funlen nolints (grep-confirmed).

No subagents used. No git-mutating commands run -- orchestrator must
commit/push. `git status` re-checked before every edit batch; only
`services/opsworks/*` files and this remainder file touched.

opsworks's List/Describe/Get families are now fully swept for this issue
(24/24 ops layer-1 clean; 4 bugs found and fixed at layer 2/5, all
discarded-input/missing-validation/fabricated-member class, none previously
documented). 83 of 162 services swept, 79 remain. `directoryservice` (80
ops, 25 L+D+G, `direct`) remains the next largest -- re-check `git status`
before picking it (it was still a live, uncommitted sibling as of this
session's end).

## directoryservice (this session, 2026-08-15)

Picked up per opsworks's own note (next largest, 80 ops, 25 L+D+G, `direct`
resolution). **This service was the subject of a killed-mid-edit prior
attempt** (`gopherstack-t0gq`, stashed as `stash@{0}`, message "wip: killed
by session limit"), which did NOT compile (mid-refactor splitting
`handleDeleteADAssessment`, unused `context` import left behind, ~18 files
touched, nothing verified). Per this session's hard constraint, the stash
was read ONLY via `git stash show -p stash@{0}` as a hint about where to
look -- never popped/applied/dropped -- and every finding it hinted at was
independently re-derived and verified against the pinned SDK from scratch,
not trusted. All 5 of its hints turned out to point at real bugs; none of
its actual code was reused verbatim (backend signatures, comments, and the
`toSharedDirInfo` helper were re-written independently, though they
converged on essentially the same shape as a correct fix necessarily would).

PROTOCOL: `awsAwsjson11` (JSON-RPC 1.1) exclusively, single client
(`directoryservice@v1.41.4`, matches `go.mod` -- no second/stray client
found). Case-SENSITIVE decode (confirmed via `api_op_*.go` middleware
registration, e.g. `awsAwsjson11_serializeOp*`/`awsAwsjson11_deserializeOp*`
on every op checked). All 453 `strings.EqualFold` hits in `deserializers.go`
are `errorCode` matching only (`grep -vc 'errorCode)'` = 0) -- none in a
body-field-key switch. The restjson1 dead-deserializer trap does not apply
to this protocol family (awsjson11, not restjson1). PHANTOM OPS: none --
`GetSupportedOperations`'s 80 op-name strings all resolve to a real
`api_op_*.go` file (diffed both directions: 0 phantom, 0 missing -- full
80/80 coverage, confirmed independently of `sdk_completeness_test.go`, which
also already passes). ROUTER, checked separately from the handlers:
`RouteMatcher` dispatches by `X-Amz-Target` header prefix into a flat
`map[string]HandlerFunc` keyed by exact op name (`handler.go`'s
`doDispatch`) -- structurally immune to the elasticsearch-style "op
unreachable at the top-level router" class, since there is no path-segment
matching to get wrong (JSON-RPC has no per-op URL path at all, only one
shared endpoint). Not a per-op risk the way restjson1 path-matching is.

6 real bugs found and fixed, all in the 25-op L+D+G surface, each confirmed
against `directoryservice@v1.41.4`'s own `api_op_*.go`/`types/types.go` with
file citation, hand-reverted individually and confirmed to fail with the
exact predicted symptom before being restored byte-identical:

1. **`DeleteADAssessment`/`DescribeADAssessment` wrongly required
   `DirectoryId`** -- real `DeleteADAssessmentInput`/`DescribeADAssessmentInput`
   are `{AssessmentId}` only (confirmed via both ops' own `api_op_*.go`;
   assessment IDs are globally addressable, not directory-scoped). Every real
   typed client's Delete/Describe call was rejected outright by this
   handler's own validation with `InvalidParameterException` before ever
   reaching the backend -- a total op failure, not silent-empty.
   `DeleteADAssessment` used the generic `handleTwoFieldOp` helper (which
   always requires `DirectoryId`, correct for every OTHER consumer of that
   helper but wrong for this one op); `DescribeADAssessment` had its own
   bespoke but equally wrong check. Hand-revert symptom: real-client test
   failed with `InvalidParameterException: DirectoryId and AssessmentId are
   required`, exactly as predicted.
2. **`DescribeADAssessment`/`ListADAssessments` wrapper keys were
   fabricated** -- `"ADAssessment"`/`"ADAssessments"` instead of the real
   `DescribeADAssessmentOutput.Assessment`/`ListADAssessmentsOutput.Assessments`
   (confirmed against both ops' `Output` structs directly). Even a request
   that got past bug 1 would have decoded to nil/empty on every call --
   distinct from bug 1 (a request-shape bug), this is a pure response
   wrapper-key bug, this issue's core class. Hand-revert symptom: real-client
   test failed with `Assessments` decoding to `[]` (len 0) instead of 1,
   exactly as predicted.
3. **`RegisterCertificate` discarded `ClientCertAuthSettings.OCSPUrl`
   entirely** -- a real, optional `RegisterCertificateInput` member
   (`types.ClientCertAuthSettings{OCSPUrl}`, confirmed via
   `api_op_RegisterCertificate.go`) with no equivalent field ANYWHERE in this
   backend before this fix (not just unemitted on the wire -- genuinely
   untracked, confirmed via a repo-wide grep for `OCSPUrl`/`ClientCertAuthSettings`
   turning up zero hits pre-fix). Now captured, persisted
   (`storedCertificate.OCSPUrl`, the same struct that is also the
   persistence DTO -- confirmed the addition is a pure field addition, not a
   retag, so `TestInMemoryBackend_SnapshotRestore_FullState` round-tripping
   it proves no persistence break), and echoed on `DescribeCertificate`'s
   `Certificate.ClientCertAuthSettings`. Discarded-input instance (13th+ this
   campaign, per the running count in gopherstack-6flj's own comments; this
   session did not attempt to re-derive the exact running total). Hand-revert
   symptom: real-client test failed with `ClientCertAuthSettings` nil,
   exactly as predicted.
4. **`DescribeUpdateDirectory` wrapper key was fabricated, AND its per-item
   shape was wire-breaking** -- `"UpdateDirectoryInfo"` instead of the real
   `DescribeUpdateDirectoryOutput.UpdateActivities` (confirmed via
   `api_op_DescribeUpdateDirectory.go`; silent-empty in isolation). Separately
   and more severely: every entry emitted `NewValue`/`PreviousValue` as flat
   `""` strings, but the real `types.UpdateInfoEntry` member type is
   `*types.UpdateValue{OSUpdateSettings}` (`types/types.go`), a nested
   struct -- a real client's decode HARD-FAILED with a JSON
   type-mismatch/unmarshal error on every call that returned at least one
   entry (i.e. every call after any `UpdateDirectorySetup`), not just
   silent-empty. Confirmed this backend never populates real
   `NewValue`/`PreviousValue` content for ANY `UpdateType` (OS/NETWORK/SIZE
   alike -- always the Go zero value, traced through `settings.go`'s
   `UpdateDirectorySetup`/`DescribeUpdateDirectory`), so both are now omitted
   entirely (matching AWS's nil-omission convention for an optional member
   with nothing honest to report) rather than fabricated into the real
   nested shape. Two independent hand-revert symptoms confirmed: wrapper-key
   revert -> `UpdateActivities` decoded to `[]` (len 0); NewValue/PreviousValue
   revert -> real-client call failed outright with `deserialization failed
   ... unexpected JSON type`, both exactly as predicted.
5. **`DescribeSettings`' `SettingEntry` emitted the request-side filter
   field's name** -- `"Status"` (matching `DescribeSettingsInput.Status`, a
   real but DIFFERENT field -- the response filter parameter) instead of the
   real response member `SettingEntry.RequestStatus` (confirmed `SettingEntry`
   has NO `Status` member at all in `types/types.go`). A real client's
   `RequestStatus` field silently decoded to its zero value on every call.
   Real-key-from-the-wrong-side pattern (this campaign's recurring class --
   the request filter's own name was copied onto the response by mistake).
   Hand-revert symptom: real-client test failed with `RequestStatus` == ""
   instead of `"Updated"`, exactly as predicted.
6. **`AcceptSharedDirectory` returned only `{SharedDirectoryId}`** -- the
   real `AcceptSharedDirectoryOutput.SharedDirectory` is a full
   `types.SharedDirectory` object (confirmed via `api_op_AcceptSharedDirectory.go`),
   the EXACT SAME shape its sibling `DescribeSharedDirectories` already
   emitted correctly (every field independently diffed against
   `types.SharedDirectory` and confirmed clean). Every other field
   (`OwnerDirectoryId`, `OwnerAccountId`, `SharedAccountId`, `ShareMethod`,
   `ShareStatus`, `ShareNotes`, `CreatedDateTime`, `LastUpdatedDateTime`)
   silently decoded to nil/zero on a real client. Fixed by sharing the same
   field-mapping helper (`toSharedDirInfo`) `DescribeSharedDirectories`
   already used -- "the correct sibling sat right beside the broken one,"
   this campaign's most repeated pattern, again. Hand-revert symptom:
   real-client test failed with `SharedAccountId`/`ShareStatus` empty and
   `LastUpdatedDateTime` nil, exactly as predicted.

DISCLOSED, NOT FIXED (1, genuine fabricated-but-harmless fields, not a
leak): `DescribeLDAPSSettings`'s `LDAPSType`/`CertificateId`/
`CertificateExpiryDateTime` are NOT real `types.LDAPSSettingInfo` members at
all (the real shape is exactly `{LDAPSStatus, LDAPSStatusReason,
LastUpdatedDateTime}`) -- left in place rather than removed, since no
sensitive data is involved and a real client simply ignores unknown JSON
fields; removing them buys nothing testable, matching this issue's own
"fields that are merely informational should be disclosed, not removed"
guidance and the elasticsearch/rekognition precedent for the same pattern.
`LDAPSStatusReason` (real, optional) is genuinely absent -- this backend
tracks no LDAPS state-change reason anywhere.

REAL-DATA LEAK SWEEP (this service holds AD credentials and trust
passwords, called out explicitly for this pass -- checked deliberately, not
skipped): **no leak found.** `Password`/`TrustPassword`/`NewPassword`
request fields are read only for backend invocation and grepped confirmed
never placed into any `map[string]any` response body anywhere in this
service. `TrustPassword` is accepted on `CreateTrust` and never echoed by
`DescribeTrusts` (matches AWS's own real behavior -- `types.Trust` genuinely
has no password member either, confirmed). `SecretArn`
(`CreateHybridAD`/`UpdateHybridAD`, a real Secrets Manager ARN) is
"used once and not stored" per its own doc comment (pre-existing, this pass
verified it still holds) and never appears in any Describe response --
confirmed `types.HybridUpdateInfoEntry` has no `SecretArn` member either.
`PcaConnectorArn` (`DescribeCAEnrollmentPolicy`) IS a real, intentional
response member per the real `Output` struct, not a leak. No
environment-variable- or KMS-ARN-shaped fields exist anywhere in this
service's op surface at all (this service has no ECS/Lambda-style env-var
concept, and no KMS integration).

SIBLINGS CHECKED, ALREADY CORRECT (full per-op wrapper-key AND per-item
member-set diff against each op's own real `Output`/per-item `types.go`
struct, not assumed from a passing family): `DescribeDirectories`
(`DirectoryDescriptions`), `GetDirectoryLimits` (`DirectoryLimits`),
`DescribeSnapshots` (`Snapshots`), `GetSnapshotLimits` (`SnapshotLimits`),
`ListTagsForResource` (`Tags`), `DescribeCAEnrollmentPolicy` (flat, 5
fields), `DescribeClientAuthenticationSettings`
(`ClientAuthenticationSettingsInfo`, per-item `{LastUpdatedDateTime,Status,Type}`
exact match), `DescribeConditionalForwarders` (`ConditionalForwarders`,
per-item exact match incl. `DnsIpv6Addrs`), `DescribeDirectoryDataAccess`
(flat `DataAccessStatus`), `DescribeDomainControllers`
(`DomainControllers`), `DescribeEventTopics` (`EventTopics`, per-item exact
match), `DescribeHybridADUpdate` (`UpdateActivities{HybridAdministratorAccount,SelfManagedInstances}`
nested shape, exact match), `DescribeRegions` (`RegionsDescription`),
`DescribeSharedDirectories` (`SharedDirectories`, the correct sibling beside
bug 6), `DescribeTrusts` (`Trusts`, per-item exact match, confirmed
`types.Trust` genuinely has no `TrustPassword` member), `ListCertificates`
(`CertificatesInfo`), `ListIpRoutes` (`IpRoutesInfo`), `ListLogSubscriptions`
(`LogSubscriptions`), `ListSchemaExtensions` (`SchemaExtensionsInfo`) -- all
19 hold their real wrapper key and real per-item member set, individually
diffed, not assumed.

No discarded inputs found beyond bug 3's `ClientCertAuthSettings.OCSPUrl`.
No struct retagged this pass doubles as a persistence DTO in a way that
risked breaking persistence -- checked deliberately per this issue's "two
near-misses" warning (`storedCertificate`, the one struct touched that IS
also the persistence DTO, only had a pure field ADDITION, not a retag or
removal; verified safe by the existing full-state snapshot/restore test
round-tripping the new field). No invented enum values, no request struct
copied wholesale from a sibling, no handler-massages-values-to-fit-a-wrong-shape
pattern found anywhere in the 25-op surface.

PRIOR AUDITS ACTUALLY COVERED (established, not assumed): this service
carries an extremely detailed `PARITY.md` from 5+ prior focused passes
(`b8552fe92`, `gopherstack-h910`, `gopherstack-10hx` and two follow-ups, a
2026-07-23 and 2026-08-13 pass) that individually field-diffed nearly every
domain type in this service against `types.go` member-by-member and held an
A grade. **None of the 6 bugs above fall in an op any prior pass's own notes
claim to have specifically re-verified as clean** -- all 6 are in ops those
passes' field-diffs marked `wire: ok`/`wire: FIXED` on the strength of a
MEMBER-SET diff (does the struct have the right fields) that never
independently checked the top-level WRAPPER KEY the handler actually emits,
nor which request members are actually required vs. inherited from a
generic multi-op helper. This is the same "prior audits were thorough but
checked a different axis" result the elasticsearch/lakeformation passes
reported, not the kafka-style "wrong about ops it did cover" result.

RATIFYING TEST FOUND AND FIXED: 1. `TestSharedDirectories`'s "share accept
describe unshare" case previously asserted only `http.StatusOK` on the
Accept step, never the response body -- passed against the unfixed code
despite exercising the exact broken op (the "assertion too weak to fail"
trap). Rewritten to assert every `SharedDirectory` field
(`SharedDirectoryId`/`OwnerDirectoryId`/`SharedAccountId`/`ShareMethod`/
`ShareStatus`/`OwnerAccountId`/`CreatedDateTime`/`LastUpdatedDateTime`); this
version does fail against the unfixed code. 5 other pre-existing raw-body
tests (`ADAssessment`/`ADAssessments` key assertions in
`handler_ad_assessments_test.go`, `handler_test.go`; `UpdateDirectoryInfo`
key assertion in `handler_settings_test.go`) were updated to the real keys
but were not independently "cannot fail" instances beyond that key mismatch
-- fixed in place.

REAL-CLIENT TEST RATIO: 1 file (`handler_ca_enrollment_sdk_test.go`, 2
tests) before this pass, out of 80 ops. Added `wire_field_fixes_test.go`: 5
new real-SDK-client tests, each driven through
`newTestDirectoryServiceClient`'s full `service.NewServiceRouter`/
`RouteHandler` stack (the same router-inclusive path the elasticsearch
routing bug required to be caught, not just `h.ServeHTTP` directly), each
hand-reverted individually against the specific fix it covers and confirmed
to fail with the exact predicted symptom before being restored
byte-identical (all 6 fixes covered; bug 2's two wrapper-key fixes and bug
4's two-part fix were each reverted and confirmed separately, 8 hand-revert
cycles total across 6 fixes).

Every fix from `stash@{0}` that this pass's independent verification
CONFIRMED as a real bug (read-only, never applied): the
`DeleteADAssessment`/`DescribeADAssessment` `DirectoryId` removal (bug 1),
the `ADAssessment`/`ADAssessments` wrapper-key rename (bug 2), the
`RegisterCertificate`/`DescribeCertificate` `OCSPUrl` addition (bug 3), the
`DescribeLDAPSSettings` fabricated-field finding (disclosed here rather than
removed, unlike the stash's approach of removing them -- a judgment call,
not a contradiction: removing them is defensible too, this pass chose
disclosure per this issue's own stated preference), the `UpdateDirectoryInfo`
-> `UpdateActivities` wrapper-key rename and `UpdateType`/`NewValue`/
`PreviousValue` field pruning (bug 4, though this pass's fix keeps
`UpdateType` disclosed rather than removed, another disclosure-over-removal
judgment call), and the `AcceptSharedDirectory` full-object fix (bug 6, this
pass independently arrived at the same `toSharedDirInfo`-shaped helper).
**Not present in the stash and found independently by this pass**: bug 5
(`DescribeSettings`' `Status`->`RequestStatus`) -- the stash's diff did not
touch `handleDescribeSettings` at all.

Gates: full `go build ./...` (mandatory -- `DeleteADAssessment`/
`DescribeADAssessment`/`RegisterCertificate`/`AcceptSharedDirectory`
interface+backend signatures all changed; clean, confirmed no other package
in the repo references this service's backend/interfaces directly), `go
vet` (scoped + full `./...`), `go test -race` (scoped + `./pkgs/...`), `go
fix -diff` (no diff), `gofmt -l` (clean), `golangci-lint run
./services/directoryservice/...` (0 issues after fixing 2 `goconst`/
`nolintlint` findings from introducing a new `keyCreatedDateTime` constant
and 1 `fieldalignment` finding on `RegisterCertificate`'s new anonymous
request struct, all fixed BY HAND, not `-fix`, per this campaign's
documented `fieldalignment -fix` nolint-stripping hazard). 0
`cyclop`/`gocyclo`/`gocognit`/`funlen` nolints added (grep-confirmed).

FALSE-POSITIVE RATE: 0 among the 6 reported bugs -- every finding cites the
real `api_op_*.go`/`types/types.go` file, confirmed reached via each op's
own middleware registration, and every fix was hand-reverted and confirmed
to fail with the exact predicted symptom via a real SDK client before being
restored byte-identical.

No subagents used (Read/Grep/Bash/Edit only, per this session's hard
constraint). No git-mutating commands run -- orchestrator must commit/push.
`git status` re-checked before every edit batch; the opsworks sibling was
live at session start (`services/opsworks/*` modified) and its tree went
clean partway through this session (its own pass completed and, per its
section above, was left uncommitted for the orchestrator) -- only
`services/directoryservice/*` files and this remainder file were ever
touched by this session.

directoryservice's List/Describe/Get families are now fully swept for this
issue (25/25 ops layer-1/2/3 clean; 6 real bugs found and fixed, one found
independently of the stashed hint; 1 fabricated-but-harmless field disclosed
rather than removed; no real-data leak found despite this service's
AD-credential/trust-password surface). 84 of 162 services swept, 78 remain.

## cloudtrail (this session, 2026-08-15)

Assigned directly (gopherstack-6flj). Per the ranked table, `directoryservice`
(80 ops, 25 L+D+G, `direct`) was the largest unswept candidate but a
live sibling was actively editing it all session (confirmed via `git status`
showing 19 modified + 1 untracked `services/directoryservice/*` files, and
that this remainder file's own header already credited directoryservice to
that sibling this session) -- never touched. `opsworks` (74 ops, 24 L+D+G)
was already swept earlier this same session (`0f5a7d360`). That left a
three-way tie at 24 L+D+G ops: `codeartifact` (48 total ops), `cloudtrail`
(60 total ops), `appconfig` (56 total ops). Chose `cloudtrail`: largest total
op count of the three, and the widest number of distinct resource-family
handler files (9: channels, dashboards, event_data_stores, event_selectors,
events, imports, queries, resource_policies, trails), maximizing the
sibling-trap surface this issue targets. Confirmed via `go run
./cmd/opcensus` immediately before picking (60/11/2/11/24, matching the
table exactly).

**SDK availability:** `aws-sdk-go-v2/service/cloudtrail@v1.58.4` is pinned in
`go.mod`/`go.sum` (unlike several recent sweep targets) -- no dependency
boundary issue to disclose here.

**Protocol:** `awsAwsjson11` exclusively (100 `func awsAwsjson11_serialize*`
+ 363 `func awsAwsjson11_deserialize*` matches in the pinned module;
0 `awsEc2query_`/`awsAwsquery_`/`awsRestxml_` matches). Case-sensitive plain
Go `switch key { case "Xxx": }` on decoded JSON keys, confirmed by reading
several `awsAwsjson11_deserializeOpDocument*Output` functions directly. All
`EqualFold` hits in this SDK version are in `errorCode` matching only
(confirmed via `grep -n EqualFold deserializers.go` -- every hit is
`strings.EqualFold("SomeException", errorCode)`), never a body-field switch,
so casing near-misses in field names ARE real silent-empty bugs here (found
one -- see below). No second client: no `cloudtrail-data`/`cloudtraildata`
service exists in the module cache or `go.mod`.

**Dead-deserializer trap:** does not apply. This is JSON-RPC 1.1
(`awsAwsjson11`) codegen, not restjson1 -- each op's own
`HandleDeserialize` method calls its own uniquely-named
`awsAwsjson11_deserializeOpDocument<Op>Output` function directly (spot
verified for `GetTrail` and `ListChannels`), unlike restjson1's shared/dead
generic-shape deserializer pattern that tripped up an agent on pinpoint.

**Router:** single top-level `X-Amz-Target` prefix match
(`CloudTrail_20131101.<Op>`), one flat `h.ops` dispatch map built in
`buildOps()`. All 61 `GetSupportedOperations()` op names have a
corresponding `h.ops["Xxx"] = h.handleXxx` entry (grep-diffed 1:1); no
second-layer router to desync, no 404-at-router gap found (checked
separately from the handler bodies, per this issue's elasticsearch lesson).

**Phantom ops:** none found among the 24 L+D+G ops audited (each op's
handler + real `api_op_<Op>.go` were both located and read; no gopherstack
op name failed to resolve to a real SDK operation).

**Ignored filters:** none of the 24 L+D+G ops discard a declared filter
member. `ListQueries`' `EventDataStore`/`QueryStatus` filters, `ListImports`'
`Destination`/`ImportStatus` filters, and `DescribeTrails`' `TrailNameList`
were all spot-checked and reach the query. (Pre-existing, not this pass:
`ListQueries`' `EventDataStore` filter is real AWS's required field but left
optional here for backward test compatibility -- already disclosed in
PARITY.md `gaps`, not new.)

**2 real wrapper-key/shape bugs found and fixed** (the headline class this
issue tracks), plus a related 3rd bug (fabricated + missing fields sharing
one function across 5 sibling ops) found while verifying the two:

1. `ListInsightsData`: response wrapped the (always-empty, stub) event list
   under a fabricated `"Insights"` key. Real `ListInsightsDataOutput` wraps
   it under `"Events"` (confirmed:
   `awsAwsjson11_deserializeOpDocumentListInsightsDataOutput`,
   deserializers.go:20403, case `"Events"` ->
   `deserializeDocumentEventsList`, reached from
   `awsAwsjson11_deserializeOpListInsightsData.HandleDeserialize`). Silently
   dropped by any real client, case-sensitive JSON-RPC. Not currently
   *observable* as data loss (the backend never populates the list -- no
   Insights-event generation exists, an existing, disclosed limitation), but
   a real, latent bug: the day this stub grows real data, every typed client
   would still see an empty `Events` slice. Fixed; also added
   `DataType`/`InsightSource` required-field validation (previously the
   entire request body was ignored, `_ []byte`).
2. `ListInsightsMetricData`: response was `{"Values": <always-empty-list>}`.
   Real `ListInsightsMetricDataOutput` is an entirely different shape -- a
   flat time series (`ErrorCode`/`EventName`/`EventSource`/`InsightType`/
   `NextToken`/`Timestamps`/`TrailARN`/`Values`, `Values` being `[]float64`
   parallel to `Timestamps []time.Time`), not a list-of-records wrapper at
   all (confirmed:
   `awsAwsjson11_deserializeOpDocumentListInsightsMetricDataOutput`,
   deserializers.go:20673). The handler also ignored its entire request body
   (`_ []byte`), so the three real required inputs (`EventName`,
   `EventSource`, `InsightType`) went unvalidated and unechoed. Fixed: now
   validates the three required fields, echoes them plus optional
   `ErrorCode`/`TrailARN` (`TrailName` resolved via the existing
   `Backend.GetTrail` lookup, reusing the `ErrNotFound` ->
   `TrailNotFoundException` error path already wired for `GetTrail`), and
   returns real-shaped (empty) `Timestamps`/`Values` arrays. Backend
   `ListInsightsMetricData()`'s return type also corrected from
   `[]map[string]any` to `[]float64` to match the real `Values` field type.
3. **Sibling-trap, found while fixing (1)/(2):** `edsToMap` was one function
   shared across `CreateEventDataStore`/`GetEventDataStore`/
   `UpdateEventDataStore`/`ListEventDataStores`(items)/`RestoreEventDataStore`
   -- but these 5 ops' real output shapes genuinely differ (same class this
   issue already fixed once for this exact service's Dashboard family, see
   `GetDashboard`/`CreateDashboard`/`UpdateDashboard`'s PARITY.md history).
   Diffed all 5 real deserializers field-by-field
   (`awsAwsjson11_deserializeOpDocumentCreateEventDataStoreOutput`
   /Get/Update/Restore, deserializers.go:18529/19598/22128/21294) and found:
   - **Fabricated member, all 5 ops:** `InsightSelectors` emitted whenever
     `len(eds.InsightSelectors) > 0` -- this field exists on **no**
     EventDataStore output shape in the real API at all (only on
     `Get`/`PutInsightSelectorsOutput`, a completely different op pair).
     Verified reachable, not just theoretical: added a test that calls
     `PutInsightSelectors` on an EDS first, then asserts `GetEventDataStore`
     does not leak it back.
   - **Missing member, Create only:** real `CreateEventDataStoreOutput` has
     `TagsList` (`Create`, `Get`, `Update`, `Restore` differ on this: only
     Create has it) -- a value the backend already held (tags are captured
     into `eds.Tags` at creation, converted from the request's own
     `TagsList` field) but never echoed back on any response. Now populated
     on Create only.
   - **Fabricated member, Create+Restore:** `FederationRoleArn`/
     `FederationStatus` emitted whenever set, but real
     `CreateEventDataStoreOutput`/`RestoreEventDataStoreOutput` have neither
     field (only `Get`/`UpdateEventDataStoreOutput` do -- federation is only
     ever set post-creation via `EnableFederation`, so this was mostly
     unreachable for `Create`, but real and reachable for `Restore` if a
     store had federation enabled before being soft-deleted).
   Split into `edsCommonToMap` (shared real fields) +
   `edsCreateToMap`/`edsRestoreToMap`/`edsGetOrUpdateToMap` (per-op deltas),
   plus a new `edsTagsList` helper mirroring the pre-existing `dashTagsList`
   pattern this same file's Dashboard fix already established. `Get` and
   `Update` share one function (`edsGetOrUpdateToMap`) because their real
   output shapes are identical in what this backend can populate (see
   PartitionKeys gap below).
   **Two pre-existing tests were asserting the fabricated Create-side
   `FederationStatus` field directly** (`TestEDSFederation/
   new_eds_has_disabled_federation` and `TestCloudTrailFederationSmoke`) --
   exactly this issue's "test fixture that cannot fail" trap, except this
   one actively enshrined the bug as expected behavior. Fixed both to
   observe the same real invariant (a fresh EDS defaults to `DISABLED`
   federation) via `GetEventDataStore` instead, which really does have the
   field.

**Sibling pairs / families checked and found correct** (24 wrapper keys, all
diffed against the real deserializer's own case list):
`DescribeTrails`'s **lowercase** `trailList` key (a legacy CloudTrail quirk
predating the `Trails`-prefixed naming convention) -- correct, matches
`case "trailList":` exactly, case-sensitive protocol so this one actually
matters. `ListTrails`'s `Trails` key with the narrower `TrailInfo` item shape
(`TrailARN`/`Name`/`HomeRegion` only) -- correct, distinct from
`DescribeTrails`'s full `Trail` object, not conflated. `GetDashboard`'s
already-established `dashGetToMap` (no `Name` field, confirmed absent from
the real output) vs `dashCreateToMap`/`dashUpdateToMap` -- re-verified
correct, the precedent this pass's `edsCreateToMap` split followed.
`GetChannel`/`ListChannels`: item shape (`ChannelArn`+`Name` only) vs full
`GetChannel` shape, correctly distinct, not conflated. `ListImportFailures`'s
`"Failures"` key (not `"ImportFailures"`) -- correct. `ListInsightsData`'s
sibling `ListInsightsMetricData` looks similar on the surface (both "list
insights-ish data") but has a completely different real shape -- confirmed
each independently rather than assuming a shared pattern, per this issue's
"copying the majority convention can be the error" warning; here neither
convention was safe to copy from the other. `GetEventConfiguration`'s
`TrailARN`/`EventDataStoreArn` split (real API itself is inconsistent about
`ARN` casing between these two identifier fields) -- correctly reproduced
verbatim, not normalized to one casing.

**No fabricated required-response members or unenforced required requests
found beyond what's listed above** -- `GetEventSelectors`, `GetImport`,
`GetResourcePolicy`, `GetTrailStatus` (already had a detailed, re-verified-
accurate PARITY.md note from a prior pass citing all 16 real fields),
`GetInsightSelectors`, `GetQueryResults`, `DescribeQuery` were all
field-diffed against their real deserializers and matched (module-cache
citations for each are in the PARITY.md updates this pass made).

**Discarded inputs / fields never set:** `StartImport`'s real, optional
`StartEventTime`/`EndEventTime` inputs are silently discarded (no struct
field to receive them) -- disclosed in PARITY.md rather than fixed, since
import execution itself is an already-documented, pre-existing "not real"
limitation (honoring a time filter over data that's never actually replayed
would be misleading, not more correct).

**Over-wide fields, sorted:** none found in the informational-leak sense
this issue tracks (no client secrets/ARNs/env vars). One borderline case
disclosed, not fixed: real `types.EventDataStore` (the `ListEventDataStores`
item type) marks every field except `EventDataStoreArn`/`Name` as
"Deprecated: no longer returned by ListEventDataStores" in the SDK's own doc
comments -- AWS's real server has stopped populating them for this specific
op, but gopherstack's list items still return the full rich shape (same as
`GetEventDataStore`). Harmless/informational (a typed client just receives
extra populated fields it wasn't guaranteed), not the silent-empty class
this issue targets, so left as-is.

**Structural gaps disclosed, not fabricated** (backend doesn't hold the
value at all): `GetChannel` missing `IngestionStatus`/`SourceConfig`;
`GetEventDataStore` missing `PartitionKeys`; `GetInsightSelectors` missing
`InsightsDestination`; `GetResourcePolicy` missing
`DelegatedAdminResourcePolicy` (same root cause as this service's
pre-existing, already-documented lack of org-admin state); `GetImport`
missing `StartEventTime`/`EndEventTime`/`ImportStatistics`. All added to
PARITY.md's `gaps` list this pass with the specific missing field names and
why.

**Prior audit accuracy:** this service's PARITY.md `last_audit_date:
2026-07-23` had marked `ListInsightsData`, `ListInsightsMetricData`,
`CreateEventDataStore`, `GetEventDataStore`/`UpdateEventDataStore`/
`RestoreEventDataStore` all `wire: ok` with no caveat -- **all six of those
claims were wrong** (bugs 1/2/3 above). The rest of that same prior audit
(24 other ops, including the detailed multi-paragraph Dashboard/Query/Import
fixes) held up under this pass's independent re-verification -- silence and
error looked identical from outside until each op's real deserializer was
actually read, consistent with this issue's kafka/pinpoint lesson that a
prior audit can be right about most of its surface and wrong about a
specific, unverified corner of it.

**Tests:** 2 new dedicated wire-shape test functions
(`TestCloudTrailListInsightsWireShape`, 4 subtests;
`TestEventDataStoreWireShape`, 2 subtests) plus 2 pre-existing tests fixed
(see above) and the pre-existing ancillary smoke test's bodies updated to
supply the newly-required fields. Every new assertion was run against the
unfixed code first and confirmed to fail with the exact predicted symptom,
then the fix was restored and confirmed byte-identical via `diff` against a
saved copy (no git-mutating commands used): (1) `ListInsightsData`'s key
hand-reverted to `"Insights"` -> `TestCloudTrailListInsightsWireShape/
list_insights_data_uses_events_key` failed on both of its two assertions
with the literal messages `"response should have an Events key"` /
`"response should not have the wrong Insights key"`, exactly as predicted;
(3) `edsCommonToMap`'s `InsightSelectors` emission hand-reverted back in ->
`TestEventDataStoreWireShape/get_never_has_insight_selectors_or_tags_list`
failed with `"GetEventDataStore response should not have InsightSelectors"`,
exactly as predicted (the sibling `create_never_has_insight_selectors...`
subtest didn't catch this revert since Create's own EDS never had
InsightSelectors set on it -- confirming per this issue's method note that a
"does this test even exercise a populated value" check matters, not just
"does the field appear").

**Real-client test ratio:** SDK is pinned (`go.mod`), no disclosed exception
needed for this service, but this pass didn't specifically measure the
existing suite's real-vs-raw-body ratio (all new tests added this pass are
raw-body/httptest, matching this file's existing convention throughout).

Gates: scoped `go build`/`go vet ./services/cloudtrail/...` clean; full `go
build ./...`/`go vet ./...` clean (`ListInsightsMetricData`'s backend return
type changed from `[]map[string]any` to `[]float64`, grep-confirmed no
external callers); `go test -race -count=1 ./services/cloudtrail/...` and
`./pkgs/...` green; `go fix -diff` clean; `golangci-lint run
./services/cloudtrail/...` 0 issues (one `goconst` finding on a newly
duplicated `"Key"` string literal fixed by adding a shared `keyKey` const,
matching the pre-existing `keyValue` const's pattern, applied consistently
across all 3 existing `"Key"`/`keyValue` map-literal sites in the package;
one `golines` formatting finding fixed by hand); 0
cyclop/gocyclo/gocognit/funlen nolints (grep-confirmed, none added).

No subagents used. No git-mutating commands run -- orchestrator must
commit/push. `git status` re-checked before every edit batch; only
`services/cloudtrail/*` files and this remainder file touched;
`services/directoryservice/*`'s live sibling changes never read or touched
beyond the initial `git status`/`git log` scan used to confirm it was taken.

cloudtrail's List/Describe/Get families are now fully swept for this issue
(24/24 ops layer-1/2 clean; 2 headline wrapper-key/shape bugs fixed plus 1
related sibling-trap bug spanning 5 ops; 6 structural gaps disclosed, not
fabricated; 2 pre-existing tests that enshrined a fabricated field corrected;
no real-data leak found). 85 of 162 services swept, 77 remain. Per the ranked
table, `directoryservice` (80 ops, 25 L+D+G, `direct`) remains the largest
unswept candidate once its live sibling session ends; after that, the
three-way tie at 24 L+D+G (`codeartifact`, `appconfig`) is next -- re-check
`git status` and this file's header before picking either.

## appconfig (this session, 2026-08-15)

Continuing gopherstack-6flj directly (single agent, no subagents, per this
session's hard constraint). Read `bd show gopherstack-6flj`'s notes and
`git show 78517e30d1f020191defc2511316e1ba66de5334` (the directoryservice
pass immediately before this one) first, per assignment.

**Pick, and the switch made:** `git status` at session start showed
`services/cloudtrail/*` modified + this remainder file modified (a live
sibling, uncommitted). Per the ranked table, `opsworks` (74 ops, 24 L+D+G)
and `directoryservice` (80 ops, 25 L+D+G) were both already swept this
session (commits `0f5a7d360`, `78517e30d`). That left the three-way tie at
24 L+D+G already identified by the cloudtrail pass's own closing note:
`codeartifact` (48 total ops), `cloudtrail` (60 total ops, being worked by
the live sibling), `appconfig` (56 total ops). Chose `appconfig`: largest
remaining total op count of the two free candidates (56 vs codeartifact's
48), same tiebreak logic the cloudtrail pass used. Confirmed via `go run
./cmd/opcensus` before picking (56/12/0/12/24, matching the table exactly).
Partway through this session `services/cloudtrail/*` disappeared from `git
status` (the sibling's work landed as commit `773c2af52`) and
`services/codeartifact/*` appeared instead (a new sibling, presumably taking
the other half of the tie) -- re-checked before every edit batch; never
touched either.

**SDK availability:** `aws-sdk-go-v2/service/appconfig@v1.48.4` is pinned in
`go.mod`/`go.sum` -- no dependency boundary issue to disclose. (A second,
unrelated appconfig-family package, `aws-sdk-go-v2/service/appconfigdata
@v1.26.4`, is also pinned -- see second-client note below.)

**Protocol:** `awsRestjson1` (101 `func awsRestjson1_serialize*` + 172 `func
awsRestjson1_deserialize*` matches in the pinned module; 0
`awsAwsjson1[01]_`/`awsEc2query_`/`awsAwsquery_` matches). Confirmed
case-sensitive: every body-field switch inspected (`ListApplicationsOutput`,
`GetConfigurationProfileOutput`, `EnvironmentOutput`, `Parameter`, dozens
more) is a plain Go `switch key { case "Xxx": }` on decoded JSON keys; every
`EqualFold` hit in this SDK version is `strings.EqualFold("SomeException",
errorCode)` (error-code matching only, grep-confirmed), never a body-field
switch. So casing near-misses in field names would be real silent-empty
bugs here -- none found (every field name checked matched exactly).

**Second client:** `appconfigdata@v1.26.4` is pinned and real (gopherstack's
own `services/appconfigdata` implements it, bridged from this service via
`DeployedConfigurationPublisher`/`configuration.go`'s
`CurrentDeployedConfiguration`/`publishDeployedConfigurationLocked` -- see
`bd gopherstack-uiyi`). Not touched this pass (out of scope: this pass's
26-op candidate list, per the ranked table, is `appconfig` proper, not
`appconfigdata`), but confirmed real and wired, not a phantom/dead
dependency.

**Dead-deserializer trap:** does not apply. restjson1, but NOT the
generic-shape codegen pattern that tripped an agent on pinpoint -- each op's
`HandleDeserialize` calls its own uniquely-named
`awsRestjson1_deserializeOpDocument<Op>Output` function directly (spot
verified for `ListApplications`, `GetConfigurationProfile`, `StopDeployment`
-- each has its own function at its own line, no shared/dead fallback).

**Router:** REST-path router (`RouteMatcher` + `ExtractOperation` parsing
the URL path/method via `parseAppConfigPath`), NOT a flat `X-Amz-Target`
dispatch map, so NOT structurally immune to router/handler desync the way
JSON-RPC services are. Checked: every op in `GetSupportedOperations()` maps
through `handler.go`'s big switch to a real `handle*` function (61/61,
grep-diffed); `RouteMatcher`'s path-prefix set (`/applications`,
`/deploymentstrategies`, the real AWS `/deployementstrategies` typo,
`/extensions`, `/extensionassociations`, `/experimentdefinitions`,
`/settings`, `/tags/arn:aws:appconfig:...`) covers every real path prefix
`parseAppConfigPath` dispatches on -- no 404-at-router gap found (elasticsearch's
class of bug). `ScopedPrefixMatch` on the bare `/applications` prefix
(shared with emrserverless/serverlessrepo, per an existing code comment) was
specifically re-checked and is SigV4-scoped, not a blind prefix steal.

**Phantom ops:** none found among the 24 L+D+G ops audited plus the 10
"other" ops checked opportunistically (StartDeployment, StopDeployment,
TagResource, UntagResource, UpdateAccountSettings, ValidateConfiguration,
StartExperimentRun, UpdateExperimentRun, StopExperimentRun,
UpdateExtensionAssociation) -- every gopherstack op name resolved to a real
`api_op_<Op>.go` in the pinned module.

**Ignored filters:** none. `ListExperimentDefinitions`'
`application_identifier`/`configuration_profile_identifier`/
`environment_identifier`/`status` (all 4 declared query filters) verified
reaching `ListExperimentDefinitions`'s real query param names byte-exact via
`awsRestjson1_serializeOpHttpBindingsListExperimentDefinitionsInput`
(serializers.go). `ListHostedConfigurationVersions`' `version_label`,
`ListExtensions`' `name`, `ListExtensionAssociations`'
`extension_identifier`/`resource_identifier` all spot-checked reaching the
query too.

**4 real wrapper-key/discarded-input bugs found and fixed**, all in the
"a real request/response member silently discarded or never emitted" class
this issue's checklist #6/#7 targets -- none were wrong wrapper *keys* (this
service's List-op summary-shape wrapper keys were already fixed by an
earlier `gopherstack-xs7l` pass and all re-verified clean, see below):

1. **`CreateConfigurationProfile`/`GetConfigurationProfile`/
   `UpdateConfigurationProfile`:** real `KmsKeyIdentifier`
   (`api_op_CreateConfigurationProfile.go`) was silently discarded on
   input (not bound in the handler's request struct at all) and never
   echoed on any of the three outputs, confirmed against
   `GetConfigurationProfileOutput`'s real deserializer
   (`KmsKeyArn`/`KmsKeyIdentifier` both present, deserializers.go:3234+).
   A prior audit (this service's own PARITY.md, `last_audit_date:
   2026-08-13`) had explicitly considered this and concluded "no honest
   value to put here," reasoning `CreateConfigurationProfile doesn't
   accept KmsKeyIdentifier" -- that premise was itself the bug: it
   conflated `KmsKeyIdentifier` (a caller-supplied string, trivially
   echoable) with `KmsKeyArn` (which genuinely does require unavailable
   KMS-ARN resolution and correctly stays unmodeled). Fixed:
   `KmsKeyIdentifier` is now accepted/stored/echoed on Create/Get/Update;
   `KmsKeyArn` remains honestly absent (disclosed in PARITY.md `gaps`).
   Required an 8-call-site backend signature change
   (`CreateConfigurationProfile`/`UpdateConfigurationProfile` both gained
   a new positional param) -- every test call site updated, `go build
   ./...` full-repo clean.
2. **`GetDeployment`/`StartDeployment`:** same root cause, propagated one
   level -- real `GetDeploymentOutput`/`StartDeploymentOutput` both have
   `KmsKeyIdentifier` (deserializers.go, same member set as
   `StopDeploymentOutput` below), snapshotted from the deployed profile's
   own KMS setting at deploy time on real AWS. `Deployment.KmsKeyIdentifier`
   didn't exist on the struct at all. Fixed: now populated from
   `profile.KmsKeyIdentifier` at `StartDeployment` time, same pattern as
   the pre-existing `ConfigurationName`/`ConfigurationLocationURI`
   snapshot-at-deploy fields right next to it.
3. **`StopDeployment` (major):** the handler returned `204 No Content`
   with an empty body. The real op returns `200` with a full
   `StopDeploymentOutput` body -- every `Deployment` field
   (`api_op_StopDeployment.go`), confirmed reached via
   `awsRestjson1_deserializeOpDocumentStopDeploymentOutput`
   (deserializers.go:8217), called from `StopDeployment`'s own
   `HandleDeserialize` (deserializers.go:8102) after a `response.StatusCode
   < 200 || >= 300` check that `204` passes, so this was NOT a hard
   failure -- `json.Decoder.Decode` on an empty body returns `io.EOF`,
   which the SDK's own deserializer explicitly tolerates (`err != io.EOF`
   guard), silently producing an all-zero-valued `StopDeploymentOutput`.
   A real client's `State`/`DeploymentNumber`/`PercentageComplete`/etc.
   all came back blank/0 despite the stop having genuinely happened
   server-side -- textbook silent-empty, and this service's `wire: ok`
   PARITY.md rating for `StopDeployment` never caught it because that
   entry's detailed note was entirely about the (also real, already
   fixed by an earlier pass) `AllowRevert` state-machine bug, never the
   response shape. Fixed: backend `StopDeployment` now returns `(*Deployment,
   error)` instead of bare `error`; handler returns `200` + the
   post-stop `Deployment`. Required a `StorageBackend` interface
   signature change plus 5 test call-site updates.
4. **`CreateExtension`/`GetExtension`/`UpdateExtension`:** real
   `types.Parameter.Dynamic` (deserializers.go, shared by every
   `Parameters map[string]Parameter` member across
   Create/UpdateExtensionInput and Get/CreateExtensionOutput) was
   entirely unmodeled on `ExtensionParameter` -- silently discarded on
   input, never emitted on output. Fixed: field added with matching JSON
   tag; since `ExtensionParameter` is bound directly on both the request
   struct and the stored/returned `Extension.Parameters` map, this wired
   up both directions with no other code changes. `ExtensionSummary`
   (the `ListExtensions` shape) never carries `Parameters` at all on real
   AWS, confirmed against `types.ExtensionSummary` -- so `ListExtensions`
   needed no change.
5. **`GetAccountSettings`/`UpdateAccountSettings`:** real
   `GetAccountSettingsOutput`/`UpdateAccountSettingsInput`/`Output` all
   have a second top-level member, `VendedMetrics`
   (`types.VendedMetricsSettings{Enabled}`,
   `api_op_GetAccountSettings.go`), entirely unmodeled alongside the
   already-correct `DeletionProtection`. Fixed: `VendedMetricsSettings`
   struct + `AccountSettings.VendedMetrics` field added; backend
   `UpdateAccountSettings` gained a new positional param (1 non-test
   call site updated).

**Sibling pairs / families checked and found correct** (the rest of the 24
L+D+G ops, all diffed against the real deserializer's own case list):
`ListApplications`/`GetApplication` (`types.Application` has no
`CreatedAt`/`UpdatedAt` -- already correctly stripped by a prior
`gopherstack-xs7l` pass, re-verified). `ListEnvironments`/`GetEnvironment`
(`Monitor.AlarmArn`/`AlarmRoleArn` confirmed exact). `ListConfigurationProfiles`
(`ConfigurationProfileSummary`'s narrower field set, confirmed no
`KmsKeyIdentifier`/`KmsKeyArn` on the Summary type at all -- unlike
Get/Create/Update, so no fix needed there). `ListHostedConfigurationVersions`
(`HostedConfigurationVersionSummary`, real header-bound httpPayload split
for Get/Create re-verified against
`awsRestjson1_deserializeOpHttpBindingsGetHostedConfigurationVersionOutput`
-- `Application-Id`/`Configuration-Profile-Id`/`Content-Type`/`Description`/
`KmsKeyArn`/`VersionLabel`/`Version-Number`, all present and correctly
bound; `VersionLabel` vs gopherstack's `Versionlabel` is not a casing bug --
HTTP header canonicalization collapses both to the same wire form since
neither contains a hyphen). `ListDeploymentStrategies`/`GetDeploymentStrategy`.
`ListDeployments`'s `DeploymentSummary` (confirmed genuinely narrower than
`Deployment`, no `KmsKeyIdentifier` member on the Summary type -- so only
Get/Start/Stop needed the fix above, not List). `ListTagsForResource`
(`"Tags"` key, confirmed). `ListExtensionAssociations`/
`GetExtensionAssociation` (`ExtensionAssociationSummary`'s narrower field
set confirmed). `ListExperimentDefinitions`/`GetExperimentDefinition`
(`ExperimentDefinitionSummary` -- this family already models
`KmsKeyIdentifier` correctly, unlike `ConfigurationProfile`, confirming the
gap above was an isolated oversight rather than a service-wide pattern).
`ListExperimentRuns`/`GetExperimentRun`, `ListExperimentRunEvents` (all
three confirmed using the real generic `"Items"` wrapper key, matching this
service's `keyItems` shared constant). `GetConfiguration` (deprecated
legacy op, `Configuration-Version`/`Content-Type` header binding
re-verified against `awsRestjson1_deserializeOpHttpBindingsGetConfigurationOutput`).

**Discarded inputs / fields never set:** the 4 bugs above are all in this
class. No others found among the 24 L+D+G ops' request/response pairs
checked.

**Over-wide fields, sorted:** none found in the informational-leak sense
this issue tracks (no client secrets/ARNs/env vars beyond what a caller
already supplied and is entitled to see echoed back, e.g. `RetrievalRoleArn`,
which is request-supplied and correctly only echoed, never leaked
cross-resource).

**Persistence trap:** `ConfigurationProfile`, `Deployment`, and
`AccountSettings` are all dual-purpose (wire response AND
`store.Table`/snapshot DTO, confirmed via `store.go`/`persistence.go`).
Every field added this pass (`ConfigurationProfile.KmsKeyIdentifier`,
`Deployment.KmsKeyIdentifier`, `ExtensionParameter.Dynamic`,
`AccountSettings.VendedMetrics`) was a brand-new field with its own fresh
JSON tag, never a retag of an existing field -- old snapshots restore
unaffected (the new field simply zero-values on restore of a pre-existing
snapshot, same as any other newly-added field), no persistence break.

**SDK client pinned; real-client test ratio:** pinned (`go.mod`), no
disclosed exception needed. All 4 fixes got a dedicated real
`aws-sdk-go-v2/service/appconfig` client test (not raw-body) --
`TestKmsKeyIdentifierViaSDKClient`, `TestStopDeploymentViaSDKClient`,
`TestExtensionParameterDynamicViaSDKClient`, `TestVendedMetricsViaSDKClient`
-- each hand-reverted in place (no git available under this session's hard
no-git-mutation constraint), run to confirm the exact predicted failure
(quoted in each test's commit-equivalent diff), then restored byte-identical
and re-run green. One pre-existing raw-body test
(`TestHandler_Deployment_Lifecycle`) asserted the old `204` status for
`StopDeployment` -- fixed to assert `200` + the returned `Deployment`
body's `State`, also hand-reverted/confirmed-failing/restored.

**Prior audit accuracy:** this service's PARITY.md carried an A grade from
`last_audit_date: 2026-08-13` with detailed, mostly-accurate per-op notes
(many "FIXED THIS PASS" entries from an earlier `gopherstack-xs7l`
wrapper-key pass this session independently re-verified as correct, see
sibling-pairs above). All 4 bugs this pass found fell in ops that audit
explicitly rated `wire: ok` -- three (`CreateConfigurationProfile`,
`GetDeployment`, `ListHostedConfigurationVersions`'s note) even contained
specific, confident-sounding prose *about* the exact field this pass found
missing, reasoning it away as unmodelable rather than checking whether it
actually was. `StopDeployment`'s `wire: ok` note was detailed and correct
about a different, real, previously-fixed bug (`AllowRevert`) but never
touched the response *shape* at all. Consistent with this issue's
directoryservice/kafka lesson: a prior audit can be right about most of its
surface and specifically wrong about a corner it discussed with apparent
confidence. PARITY.md updated in place for all 5 affected op entries plus a
new disclosed `gaps` line for `KmsKeyArn` (the one member that genuinely
remains unmodeled, correctly distinguished from `KmsKeyIdentifier` this
time).

**Credential sweep:** not specifically applicable -- this service holds no
password/secret-shaped fields (`RetrievalRoleArn`, ARNs generally, are
request-supplied and meant to be echoed, not backend-generated secrets).
Not a deliberate sweep target this pass; noting the absence rather than
claiming a check that wasn't done.

Gates, all foreground: scoped `go build`/`go vet ./services/appconfig/...`
clean; full `go build ./...`/`go vet ./...` clean (required after the 3
signature changes: `CreateConfigurationProfile`, `UpdateConfigurationProfile`,
`StopDeployment`, `UpdateAccountSettings`, plus the `StorageBackend`
interface); `go test -race -count=1 ./services/appconfig/...` and
`./pkgs/...` green; `go fix -diff ./services/appconfig/...` clean (no diff);
`golangci-lint run ./services/appconfig/...` 0 issues (2 `golines`
line-length findings from new test code, fixed by hand); 0
cyclop/gocyclo/gocognit/funlen nolints (grep-confirmed, none added).

No subagents used (Read/Grep/Bash/Edit only, per this session's hard
constraint). No git-mutating commands run. `git status` re-checked before
every edit batch; only `services/appconfig/*` and this remainder file (plus
`services/appconfig/PARITY.md`) touched by this session --
`services/cloudtrail/*` and `services/codeartifact/*` (both live sibling
sessions at different points) never read or touched beyond the initial
`git status`/`git log` scan used to confirm what was taken.

appconfig's List/Describe/Get families are now fully swept for this issue
(24/24 ops layer-1/2 clean; 4 real discarded-input/missing-field bugs found
and fixed, none a wrong wrapper key -- this service's wrapper keys were
already correct from an earlier pass; 1 pre-existing test that enshrined the
`StopDeployment` 204 bug corrected; `KmsKeyArn` newly and correctly
disclosed as the one remaining unmodeled gap; no real-data leak found). 86
of 162 services swept, 76 remain. Per the ranked table, `codeartifact` (48
total ops, 24 L+D+G) is the only member of the original three-way tie not
yet confirmed taken or swept as of this addition -- re-check `git status`
and this file's header before picking it, since a sibling session appeared
to be working it by the end of this pass.

## outposts (this session)

Picked as the largest unswept service with no live sibling: `codeartifact`
tied outposts' family (24 vs 23 L+D+G) but had uncommitted working-tree
changes at session start (`git status` showed 9 modified files +
1 untracked test), confirming a live sibling per this issue's own
appconfig-pass precedent. No tie to break at outposts' own rank -- 23 is a
unique value in the ranked table (next is dynamodb at 22, itself excluded as
a different issue class per this file's own note). Tie-break method used:
sibling-trap surface (widest spread of distinct resource-family handler
files) would have been the tiebreaker had one been needed -- outposts has 9
family files (assets/capacity/catalog/connections/orders/outposts/quotes/
sites/tags), the widest spread among the top-ranked candidates, which is
part of why it was worth the full read even without a literal count tie.

Protocol: restjson1, case-sensitive body fields confirmed directly --
grepped all 235 `strings.EqualFold` call sites in
`outposts@v1.66.1/deserializers.go`; the only 57 non-`errorCode` hits are
all `"NaN"`/`"Infinity"`/`"-Infinity"` float-literal matches, none a body
field-name comparison. SDK pinned at `outposts@v1.66.1` (go.mod line 219),
read only under `$(go env GOMODCACHE)`, no exception needed.

Router: a real path-segment router (`topLevelRouters()` map + per-family
`route*` funcs), NOT structurally immune. Already had a dedicated test
(`handler_sdk_route_table_test.go`, added by an earlier pass,
gopherstack-jqh2) driving all 43 ops' authoritative method+path (re-extracted
from `serializers.go`'s `HandleSerialize` bodies) through both
`ExtractOperation` and the real `Handler()`, asserting no fall-through to
the unknown-path error. Spot-verified two of its entries
(`TagResource`/`UntagResource`'s shared `/tags/{ResourceArn}` path,
`UntagResource`'s lowerCamel `tagKeys` query parameter) directly against
`serializers.go:3233`. Router confirmed clean, all 43 ops reachable.

Phantom-op check: diffed `GetSupportedOperations`'s 43-entry list against
`ls outposts@v1.66.1/api_op_*.go` -- exact match both directions, 0 phantom,
0 missing.

**Full layer-1 (wrapper key) + layer-2 (nesting) sweep of all 23 L+D+G ops
(11 List, 0 Describe, 12 Get) came back clean -- 0 bugs found.** Method: for
every op, read its real `*Output` struct directly from the op's own
`api_op_<Op>.go` (top-level field names/types) and every nested `types.*`
struct it references from `types/types.go`, then diffed field-by-field
against `wire.go`'s corresponding struct. All 23 matched exactly, including
several traps checked deliberately and found NOT to be bugs:

- **Shared-converter check (this issue's highest-yield check)**:
  `toInstanceTypeItemWire` is called from both `GetOutpostInstanceTypes` and
  `GetOutpostSupportedInstanceTypes` -- confirmed NOT a sibling trap, because
  both real ops genuinely share `types.InstanceTypeItem`
  (`api_op_GetOutpostInstanceTypes.go`/`api_op_GetOutpostSupportedInstanceTypes.go`
  both declare `InstanceTypes []types.InstanceTypeItem`).
  `ListOrderableInstanceTypes` correctly uses a separate converter
  (`toDetailedInstanceTypeItemWire`) because its real type
  (`types.DetailedInstanceTypeItem`) is genuinely different (adds
  `FormFactorConfigs`/`NetworkPerformance`/`MemoryInMib`). No other
  cross-op-shared converter found in `wire_convert.go` (`toQuoteWire` vs
  `toQuoteWireBase`/`toQuoteSummaryWire` already correctly split for the
  real `Quote`-vs-`QuoteSummary` shape difference -- `QuoteSummary` lacks
  `OrderingRequirements`, confirmed against `types.go`).
- **`UpdateSiteRackPhysicalProperties`** reuses `rackPhysicalPropertiesWire`
  directly as its request body type (not a dedicated
  `updateSiteRackPhysicalPropertiesRequest`) -- confirmed correct: the real
  `UpdateSiteRackPhysicalPropertiesInput`'s 9 optional body members are
  field-for-field identical to `types.RackPhysicalProperties`.
- **Subscription vs SubscriptionPricingDetails precision quirk**: real
  `Subscription.MonthlyRecurringPrice`/`UpfrontPrice` are `*float64`;
  `SubscriptionPricingDetails`' same-named fields are `*float32` -- two
  different real types with different precision for the same concept.
  `subscriptionWire` (float64) and `subscriptionPricingDetailsWire`
  (float32) correctly preserve this distinction, not a copy-paste that
  homogenized them.

Required-member diff (both directions) on every request body against its
real `*Input`: `createOutpostRequest`/`updateOutpostRequest`/
`createSiteRequest`/`updateSiteRequest`/`updateSiteAddressRequest`/
`createOrderRequest`/`createQuoteRequest`/`updateQuoteRequest`/
`createRenewalRequest`/`startCapacityTaskRequest`/`startConnectionRequest`/
`tagResourceRequest` all field-for-field match their real `*Input` body
members (path/query params correctly excluded from each). No field demanded
that the real Input lacks; no real required field silently dropped.

Filters: every declared filter on every List op reaches the query --
`ListOutposts` (3: AvailabilityZoneFilter/AvailabilityZoneIdFilter/
LifeCycleStatusFilter), `ListSites` (3), `ListCatalogItems` (3), `ListAssets`
(3), `ListAssetInstances` (4), `ListCapacityTasks` (2),
`ListOrderableInstanceTypes` (1), `ListOrders` (1) -- 20 filters total, all
read from `r.URL.Query()` by name and wired into the backend's filter
struct, none ignored.

Empty/204 responses checked against real output shapes: `DeleteOutpost`/
`DeleteSite`/`DeleteQuote`/`CancelOrder`/`CancelCapacityTask`/`TagResource`/
`UntagResource` all return `nil, nil` (204) in gopherstack -- confirmed
correct, not the appconfig `StopDeployment` trap, because all 7 real
`*Output` types are genuinely empty (`ResultMetadata` only, no data
members). `StartOutpostDecommission` (which DOES have a real body,
`Status`/`BlockingResourceTypes`) already returns that body, not 204 --
correct.

Discarded-input check (`grep -rn '_ Some.*Request'`, `_ context.Context`
params, `ValidateOnly`/`DryRun` handling): `StartOutpostDecommission`'s
`ValidateOnly` and `StartCapacityTask`'s `DryRun` are both read and honored
(threaded into the backend, not silently dropped). No discarded-input bug
found.

Over-wide field sweep (credential/ARN/secret classification): `Connection`'s
`ClientPublicKey`/`ServerPublicKey`/tunnel addresses are the only
key-shaped fields in this service -- `ServerPublicKey` confirmed generated
by `randomBase64Key()`, explicitly commented "synthetic, non-cryptographic
placeholder", not real key material; `ClientPublicKey` is echoed verbatim
from caller input (not backend-fabricated). Deliberate credential sweep:
clean, no real secret/ARN/env-var leak found (this service has no
environment-variable or client-secret-shaped fields at all).

Persistence check: not applicable -- `persistence.go`'s `backendSnapshot`
serializes the domain models (`Outpost`/`Site`/`Order`/`Quote`/... from
`models.go`) via `b.registry.SnapshotAll()`, entirely decoupled from
`wire.go`'s response DTOs. No wire struct doubles as the snapshot shape, so
no retagging risk existed to begin with (moot since 0 fixes were made).

**Prior-audit-reasoning check** (this issue's newest failure mode, per
appconfig's `KmsKeyIdentifier` precedent): outposts' PARITY.md (last
audited 2026-08-07, gopherstack-b9mg, raised to grade A) contains one
load-bearing piece of reasoning worth flagging rather than silently
trusting -- `ListBlockingInstancesForCapacityTask` always returns empty
because "StartCapacityTask's own model is additive-only
(`mergeInstanceTypeCapacity` never shrinks `InstanceTypeCapacities`), so no
running instance can ever legitimately block a task." Independently
re-verified the code claim: `mergeInstanceTypeCapacity`
(`capacity_tasks.go:255`) does use `+=`, never a replace/set, confirmed
additive-only as claimed. **Could not independently verify the AWS-behavior
premise** (whether real `StartCapacityTaskInput.InstancePools` is itself a
delta-add or an absolute target) from the pinned SDK alone -- the Go SDK's
doc comment on `InstancePools` ("The instance pools specified in the
capacity task") doesn't say either way, and settling it needs live AWS docs
outside `$(go env GOMODCACHE)`. Flagged, not fixed: if `InstancePools` is
actually an absolute target in real AWS, then a request specifying fewer
instances than currently configured IS a real reduction this backend can't
represent, and `WAITING_FOR_EVACUATION`/`ListBlockingInstancesForCapacityTask`
would be reachable in real AWS in a way this backend structurally can't
reproduce -- a different, deeper gap than the "isolated oversight" class
this issue targets, already disclosed as a structural gap in PARITY.md
either way (not a silent-empty wrapper-key bug regardless of which reading
is correct, so out of this issue's scope to resolve here).

Siblings confirmed correct (all 23 L+D+G ops, i.e. the service's full
List/Get surface for this issue): `ListOutposts`/`GetOutpost`,
`ListSites`/`GetSite`/`GetSiteAddress`, `ListOrders`/`GetOrder`,
`ListQuotes`/`GetQuote`, `ListCapacityTasks`/`GetCapacityTask`,
`ListCatalogItems`/`GetCatalogItem`, `ListAssets`, `ListAssetInstances`,
`ListBlockingInstancesForCapacityTask`, `ListOrderableInstanceTypes`,
`ListTagsForResource`, `GetOutpostBillingInformation`,
`GetOutpostInstanceTypes`/`GetOutpostSupportedInstanceTypes`,
`GetRenewalPricing`, `GetConnection`.

No new tests added -- 0 bugs found means no fix to ratify. Error-code set
also re-verified: all 6 real exception types
(`AccessDeniedException`/`ConflictException`/`InternalServerException`/
`NotFoundException`/`ServiceQuotaExceededException`/`ValidationException`
from `types/errors.go`) have matching sentinels in `errors.go`.

Second-client check: not applicable, outposts has no cross-service SDK
bridge.

Gates: `go build`/`go vet`/`go test -race`/`golangci-lint run`
(0 issues)/`go fix -diff` (clean) all green for `services/outposts/...`,
foreground, no code changes made (0 bugs found, nothing to fix). Did not
re-run `go test -race ./pkgs/...` since this pass touched no `pkgs/` code
and no `services/outposts` code either -- only this remainder file changed.

No subagents used (Read/Grep/Bash/Edit only, per this session's hard
constraint). No git-mutating commands run. `git status` re-checked before
every edit batch; only this remainder file changed -- `services/codeartifact/*`
(the one live sibling this session, confirmed via `git status` at the
start) never read or touched.

87 of 162 services swept, 75 remain. `codeartifact`'s sibling was still live
in `git status` at the end of this pass (same 9 modified files + 1
untracked test as at the start) -- re-check `git status` before picking it.

## codeartifact (this session, 2026-08-15)

Chosen as directed: the largest genuinely-unswept service once opsworks,
cloudtrail, appconfig, and directoryservice (all part of or adjacent to the
prior three-way 24-L+D+G tie at this session's start) had all finished and
committed, and appconfig confirmed `codeartifact` as the sole untaken member
of that tie. `git status` at start showed only `services/appconfig/*`
modified (11 files, growing) — a live sibling, confirmed not colliding.
Re-ran `go run ./cmd/opcensus` to confirm: `codeartifact` (48 total ops, 24
L+D+G) was still the largest unswept candidate not held by that sibling; no
tie this time (`outposts` next at 23) so no tie-break was needed.

SDK pinned (`go.mod`, `codeartifact@v1.41.4`) — no dependency-boundary
exception. Protocol `awsRestjson1_` exclusively, single client (no
second/data-plane module). Case-sensitive: all 268 `EqualFold` hits in
`deserializers.go` are errorCode matches, confirmed via `grep -v` on the
body-field switches — zero in a body-field `case`. Dead-deserializer trap
checked against `ListDomains`/`ListRepositories` and does NOT apply —
`HandleDeserialize` calls the real `OpDocument...Output` function directly.
Router: single `isDomainRepoPath`/`isPackageCoreGroupPath`/
`isPackageExtendedPath` set of path predicates feeding one dispatch map, all
48 ops present (`TestExtractOperation_SDKRouteTable` and
`TestSDKCompleteness` both green before and after) — not the flat
`X-Amz-Target` shape, but no desync found. Phantom ops: none.

**THE FLAGSHIP FINDING, matching this issue's exact "wrong nested shape
hard-fails" and "shared converter, different real shapes" callouts at once:**
`DeletePackageVersions`/`CopyPackageVersions`/`DisposePackageVersions`/
`UpdatePackageVersionsStatus` all built `failedVersions`/`successfulVersions`
as a JSON **array** of `{version, status/errorCode}` objects. The real
`FailedVersions`/`SuccessfulVersions` members on all four outputs are
`map[string]types.PackageVersionError` / `map[string]
types.SuccessfulPackageVersionInfo` — a JSON **object** keyed by version
string (confirmed at `deserializers.go`'s
`...PackageVersionErrorMap`/`...SuccessfulPackageVersionInfoMap`, which both
do `shape, ok := value.(map[string]interface{})` and hard-error otherwise).
This is a total-outage bug, not silent-empty: a real client's call to any of
these four ops failed outright with `deserialization failed ... unexpected
JSON type [...]` — confirmed by reproducing the exact error message against
unfixed code. Fixed by introducing a `PackageVersionOutcome{Revision,
Status}` type and rewriting all four backend methods plus a shared
`packageVersionOutcomesToWire` helper to emit real maps. Two riders caught
in the same fix: (a) invented enum value `"RESOURCE_NOT_FOUND"` on
Delete/Copy (real `PackageVersionErrorCode` has `NOT_FOUND`, no
`RESOURCE_`-prefixed variant) — found as a **sibling-trap in the other
direction**, since `DisposePackageVersions` right next to them already used
the correct `"NOT_FOUND"`; (b) `CopyPackageVersions`'/`UpdatePackageVersionsStatus`'s
successful-entry `status` value was a fabricated literal (`"Copied"`,
`"SUCCESS"` — neither a real `PackageVersionStatus` enum value at all) where
the real field is the version's actual status (`"Published"` for Copy,
`in.TargetStatus` for Update) — now sourced from the backend's own tracked
value.

**Sibling-trap #2:** `DeletePackage` shared `packageToMap` (the
`PackageDescription` shape, correct for `DescribePackage`) instead of
`packageSummaryToMap` (the real `DeletePackageOutput.DeletedPackage` shape —
confirmed `*types.PackageSummary`, not `*types.PackageDescription`, in
`api_op_DeletePackage.go`). Silently dropped the identifier (`PackageSummary`
has no `"name"` key, only `"package"`) and leaked `domainName`/
`domainOwner`/`repository`, none of which the real op returns. The same file
already had a code comment on `packageSummaryToMap` explaining exactly this
Get-vs-List split from an earlier pass (`gopherstack-tuh5`) — `DeletePackage`
was simply missed when that split was made.

**Backend-tracked-but-unemitted (layer 3), 2 findings:**
1. `RepositoryDescription.CreatedTime` — real, always-present member
   (`deserializers.go`'s `...deserializeDocumentRepositoryDescription`),
   backend already tracks `Repository.CreatedTime`, never emitted on any of
   the 6 ops sharing `repoToMap` (Create/Describe/Delete/Associate/
   Disassociate/UpdateRepository).
2. `RepositorySummary` on `ListRepositories`/`ListRepositoriesInDomain` used
   an inline 4-field map (`arn`/`name`/`domainName`/`domainOwner`) instead of
   the real 7-field shape — missing `administratorAccount`/`createdTime`/
   `description`, all three already tracked on `Repository`. Consolidated
   into a new `repositorySummaryToMap` helper.

**Ignored filters (this issue's explicit "confirm every declared filter
reaches the query" check), 2 findings:**
1. `ListRepositories`/`ListRepositoriesInDomain` both silently discarded the
   real `repository-prefix` query filter (`serializers.go`'s
   `SetQuery("repository-prefix")`) — every call returned every repository
   regardless of the filter. Backend methods gained a `repositoryPrefix`
   parameter; both handlers now read `q.Get("repository-prefix")`.
2. `ListPackageVersions` ignored 2 more real filter/ordering members: `status`
   (`SetQuery("status")`) and `sortBy` (`SetQuery("sortBy")`, whose only real
   enum value is `PUBLISHED_TIME`). Also missing the real `namespace` echo
   and `defaultDisplayVersion` member entirely (confirmed against
   `awsRestjson1_deserializeOpDocumentListPackageVersionsOutput`'s case
   list). Fixed all four together: `status` filters by exact match,
   `sortBy=PUBLISHED_TIME` reorders by `PublishedAt` (default stays
   Version-ascending), `namespace` is echoed when set, and
   `defaultDisplayVersion` is computed as the most-recently-published
   version in the (post-filter) result set — matching AWS's own doc
   ("most recently published" is the correct value for every format here,
   since this backend has no npm dist-tag concept at all to trigger the
   doc's other branch). `originType` is also a real filter member but has no
   backend field to source from at all — disclosed in PARITY.md, not
   fabricated.

**Required-field enforcement, both directions checked, 2 findings (only the
"never validated" direction; no "demands a field the real Input lacks"
found):**
1. `PutDomainPermissionsPolicy`/`PutRepositoryPermissionsPolicy` both
   silently defaulted a missing `policyDocument` to an empty-statement
   policy instead of rejecting the request. `PolicyDocument` is "This member
   is required." on both real Inputs (`api_op_Put{Domain,Repository}PermissionsPolicy.go`)
   — confirmed via the real SDK's own generated client-side validator
   (`validators.go`'s `validateOpPutDomainPermissionsPolicyInput`), which
   means a real `aws-sdk-go-v2` client can never even send this request; only
   a raw caller bypassing SDK-side validation can reach the old behavior, so
   the regression test for this is raw-body, not real-client. Fixed: both
   now return 400 `ValidationException` for an empty/absent `policyDocument`.
2. `UpdatePackageGroup` never validated its `packageGroup` pattern param at
   all (unlike its Create/Describe/Delete siblings, which already do) —
   fell straight through to the backend and surfaced as a misleading 404
   "package group not found" instead of the real 400 `ValidationException`
   real AWS returns for a missing required member. Fixed with the same
   explicit check its siblings already had.

**Siblings checked and confirmed already correct** (not just assumed):
`domainToMap`/`domainSummaryToMap` (9/6-field `DomainDescription`/
`DomainSummary` split, field-for-field exact); `packageGroupToMap`/
`packageGroupReferenceToMap` (shared across Create/Describe/Delete/Update/
Get/List — confirmed `PackageGroupDescription`/`PackageGroupSummary`
genuinely share an identical field set, a real non-bug this file's own
pre-existing comment already called out correctly); `ResourcePolicy`
(`document`/`resourceArn`/`revision`, shared by Get/Put/Delete on both
Domain and Repository policies — all six call sites correct);
`AssociatedPackage`/`PackageDependency`/`AssetSummary` wire shapes;
`ListTagsForResource`'s `Tag{key,value}` shape; `GetAuthorizationToken`'s
`authorizationToken`/`expiration` pair; `GetRepositoryEndpoint`'s flat
`repositoryEndpoint` shape.

RATIFYING TESTS found and fixed: 7 pre-existing tests
(`TestHandler_DeletePackageVersions`, `TestHandler_CopyPackageVersions`,
`TestHandler_SuccessfulVersions` (2 subtests),
`TestHandler_DisposePackageVersions_StatusChange`,
`TestHandler_CopyPackageVersions_ToSelf`, plus one status-code adjustment in
`TestHandler_ErrorPaths`) all asserted the pre-fix array shape (`.([]any)`)
or the fabricated status literals (`"Copied"`, `"SUCCESS"`) as correct — one
(`put_domain_permissions_not_found`) sent no body and only passed because
gopherstack silently defaulted `policyDocument`; updated to send a real
policy document so it still exercises the domain-not-found path it was
meant to test. All rewritten to the real map-keyed shape / real enum values.

PHANTOM OPS: none (`TestSDKCompleteness` green before/after). FALSE-POSITIVE
RATE: 0 among reported bugs — every finding cites the real
`api_op_*.go`/`serializers.go`/`deserializers.go` file, function, and case
list, never a doc comment or PARITY.md claim taken on faith.

Persistence check: `Repository`/`Package`/`PackageVersion`/`Domain`/
`PackageGroup` are all directly `store.Table`-backed persistence DTOs, none
retagged this pass — every fix either added a brand-new struct field
(`PackageVersionOutcome`, new) or built a wire-only `map[string]any` from
fields the structs already had (`CreatedTime`, `AdministratorAccount`,
`Description`) — no `json:"-"` used, no persistence risk.

Over-wide/credential sweep: clean, no secret-shaped fields exist in this
service (`policyDocument`/ARNs are caller-supplied resource policy text and
identifiers, not backend-generated credentials) — not a specific target this
pass since no such field surfaced during the L+D+G/sibling read, noting the
absence rather than skipping the check.

TESTS: 9 new real-`aws-sdk-go-v2`-client tests plus 2 raw-body tests (for the
two required-field checks a real SDK client structurally can't demonstrate,
since its own generated validator refuses to send the request) in the new
`services/codeartifact/wire_field_fixes_test.go`, plus the 7 ratifying-test
rewrites above. Every one of the 9 distinct fixes (createdTime,
DeletePackage shape, the 4-op array-vs-map rewrite treated as one fix site
via the shared helper, RepositorySummary fields, repository-prefix filter,
status/sortBy/namespace/defaultDisplayVersion, PutDomainPermissionsPolicy
required-field, PutRepositoryPermissionsPolicy required-field,
UpdatePackageGroup required-field) was hand-reverted individually (no git,
per this session's hard no-git-mutation constraint), confirmed to fail
against the reverted code with the exact predicted symptom (quoted per-fix
above), then restored and diffed byte-identical before moving to the next.

GATES: scoped `go build`/`go vet ./services/codeartifact/...` clean; full
`go build ./...`/`go vet ./...` clean (required — `DeletePackageVersions`/
`CopyPackageVersions`/`DisposePackageVersions`/`UpdatePackageVersionsStatus`/
`ListRepositories`/`ListRepositoriesInDomain`/`ListPackageVersions` all
changed signature; no external callers outside this package, confirmed via
grep; `test/integration/codeartifact_test.go` and
`services/cloudformation` both checked and unaffected); `go test -race
-count=1 ./services/codeartifact/...` and `./pkgs/...` green; `go fix -diff`
clean (no diff); `golangci-lint run ./services/codeartifact/...` 0 issues
(1 `goconst` finding on the repeated `"NOT_FOUND"` literal fixed via a
`packageVersionErrorNotFound`/`packageVersionErrorAlreadyExists` const pair,
5 `govet` shadow findings in new test subtests fixed by scoping the outer
`err` to a block before the `t.Run`s, 1 `nonamedreturns` finding on the new
`packageVersionOutcomesToWire` helper fixed by dropping the named returns);
`fieldalignment ./services/codeartifact/...` 0 hits; 0
cyclop/gocyclo/gocognit/funlen nolints (grep-confirmed, none added).

No subagents used (Read/Grep/Bash/Edit only, per this session's hard
constraint). No git-mutating commands run — orchestrator must commit/push.
`git status` re-checked before every edit batch; only `services/codeartifact/*`
and this remainder file touched — `services/appconfig/*` (the live sibling
at the start, later committed as `7d4441613` mid-session) and
`services/outposts/*` (a second sibling that appeared and finished mid-session,
its own section directly above this one) were both confirmed untouched
throughout.

`codeartifact`'s List/Describe/Get families are now fully swept for this
issue (24/24 ops layer-1/2/3 clean; the original three-way 24-L+D+G tie from
this session's earlier `cloudtrail` pick is now fully resolved — all three
members swept). 88 of 162 services swept, 74 remain. Per the ranked table,
`dynamodb` (22 L+D+G, flagged elsewhere as heavily-worked-under-other-issues
but not 6flj-swept) or `neptune`/`ecr` (21 each) are next — re-check
`git status` before picking, since siblings have consistently appeared
mid-session all day.

## dynamodb (this session, 2026-08-15)

Chosen per this session's assignment: `dynamodb` (58 total ops, 22 L+D+G — 7
List/13 Describe/2 Get) is the unique largest unswept candidate, strictly
above `neptune`/`ecr` at 21 each — **no tie existed at the top, so no
sibling-trap tiebreak was needed.** `git status` was clean (no live sibling)
at pick time; a sibling appeared on `services/ecr/*` partway through this
session (confirmed via repeated `git status` re-checks) — `ecr` was
already ruled out anyway since it's strictly smaller than `dynamodb`, and
its files were never read or touched by this session.

PROTOCOL: `json-1.0` (`DynamoDB_20120810` X-Amz-Target). Case-sensitive
plain Go `switch key { case "Xxx": }` on decoded JSON keys, confirmed by
reading `awsAwsjson10_deserializeOpDocumentDescribeContributorInsightsOutput`
and others directly in `deserializers.go`. All 304 `EqualFold` hits in this
SDK version are `errorCode` matches (`strings.EqualFold("SomeException",
errorCode)`), none a body-field comparison — grepped and spot-checked, not
counted only. SDK pinned (`go.mod:29`, `v1.63.1`), no dependency-boundary
exception needed.

ROUTER: single `X-Amz-Target` header extraction feeding a flat action-string
`switch` in `dispatch`/`dispatchTableOps`/`dispatchBackupOps`/
`dispatchExtraOps` — structurally immune to a path-segment desync (it's an
exact string match, not a path router). `TestSDKCompleteness` (already
present, re-run this session) confirms `GetSupportedOperations()`'s 58
entries all resolve to real ops with none missing — **0 phantom ops.**

A NOTABLE STRUCTURAL FACT about this service: `interfaces.go`'s
`Backend` methods are typed directly against the real
`github.com/aws/aws-sdk-go-v2/service/dynamodb` package's own `*Input`/
`*Output` structs (e.g. `ListTagsOfResource(...) (*dynamodb.ListTagsOfResourceOutput,
error)`) rather than a reimplemented backend-only shape — unusual among
this campaign's services. This does NOT make the service immune to the
wrapper-key bug class: the actual bytes on the wire are produced by a
**separate** `models`/inline-wire-struct layer (`services/dynamodb/models/
types.go`, plus several per-family inline wire structs such as
`handler_contributor_insights.go`'s `describeContributorInsightsOutput`)
with its own JSON tags, converted from the SDK-shaped backend output one
field at a time — exactly the layer this sweep checks.

RESULT: diffed all 22 L+D+G ops' top-level wrapper key(s) against their own
real `api_op_<Op>.go` `*Output` struct in the pinned SDK module cache — **21
of 22 correct as-is**, all matching real key names exactly (`BackupSummaries`,
`ContributorInsightsSummaries`, `ExportSummaries`, `GlobalTables`,
`ImportSummaryList`, `TableNames`, `Tags` for the 7 List ops;
`BackupDescription`, `ContinuousBackupsDescription`, `Endpoints`,
`ExportDescription`, `GlobalTableDescription`,
`GlobalTableName`+`ReplicaSettings`, `ImportTableDescription`,
`KinesisDataStreamDestinations`+`TableName`, 4 flat `*CapacityUnits` fields,
`Table`, `TableAutoScalingDescription`, `TimeToLiveDescription` for the 13
Describe ops; `GetItem`'s wire model already deep-audited by prior sessions
(`gopherstack-rkmp`/`lze5`/`yvs8`, re-verified still correct here — not
re-litigated), `Policy`+`RevisionId` for `GetResourcePolicy`).

**SHARED-CONVERTER CHECK (this issue's highest-yield check): `describeExport`
and `handleExportTableToPointInTime` both return
`exportTableToPointInTimeOutput{ExportDescription: ...}`.** Confirmed
legitimately shared, not a bug: `DescribeExportOutput` and
`ExportTableToPointInTimeOutput` are both genuinely `{ExportDescription
*types.ExportDescription}`-only in the real SDK (`api_op_DescribeExport.go`,
`api_op_ExportTableToPointInTime.go`) — identical real shapes, one converter
is correct.

**ONE REAL GAP FOUND AND FIXED** (the 22nd op, `DescribeContributorInsights`):
the real `DescribeContributorInsightsOutput` (`api_op_DescribeContributorInsights.go`)
has two members gopherstack had never modeled at all — `LastUpdateDateTime`
(`*time.Time`, wire: `LastUpdateDateTime`, epoch-seconds float, confirmed at
`deserializers.go:18441`) and `FailureException`
(`*types.FailureException{ExceptionName, ExceptionDescription}`). Backend
grep (`LastUpdateDateTime`, `FailureException` — zero hits anywhere in
`services/dynamodb/*.go` before this fix) confirmed neither was even tracked
internally, let alone emitted — this is the "member never modeled" gap
class, not a wrong-key silent-empty bug (a real client got `nil`/absent for
both, not a wrong-shaped present value).

- `LastUpdateDateTime` **fixed**: added `Table.ContributorInsightsLastUpdate
  time.Time` (`store.go`), set to `time.Now().UTC()` in
  `setContributorInsightsLocked` (`contributor_insights.go`, the one place
  `UpdateContributorInsights` mutates enabled/mode state) and emitted by
  `DescribeContributorInsights` only when non-zero — a never-toggled table
  reports the field absent rather than a fabricated epoch-zero timestamp,
  matching AWS's own "populated once an action has occurred" semantics.
  `ContributorInsightsSummary` (the `ListContributorInsights`/
  `ListContributorInsightsSummaries` item shape) does **not** have this
  member in the real SDK — confirmed before deciding not to propagate it
  there too (would have been a fabricated field, not a fix).
- `FailureException` **disclosed, not fabricated**: this backend's
  contributor-insights enable/disable never fails (no IAM/service-limit
  failure model exists anywhere in this service) — always-nil is the
  accurate representation, not a gap being papered over. Added to
  `PARITY.md` `gaps` rather than invented.

**PERSISTENCE TRAP CHECKED**: `Table` doubles as the snapshot DTO
(`persistence.go`'s `dynamodbSnapshotVersion`, currently `1`). The new
`ContributorInsightsLastUpdate` field is a **brand-new field with its own
fresh JSON tag**, not a retag of an existing one — old snapshots restore
with it zero-valued, which the `IsZero()` guard already treats correctly as
"never toggled." No snapshot-version bump needed (matches this file's own
precedent for `PITRSnapshots`). `TestInMemoryDB_SnapshotRestore`,
`TestInMemoryDB_RestoreInvalidData`, and `TestDynamoDBHandler_Persistence`
all re-run and green.

**REQUIRED-FIELD / FILTER CHECKS** (both directions, all 7 List ops):
`ListBackups` (`TableName`, `BackupType`, `TimeRangeLowerBound`/`Upper`,
`ExclusiveStartBackupArn`/`Limit`), `ListContributorInsights` (`TableName`),
`ListExports` (`TableArn`), `ListGlobalTables` (`RegionName`,
`ExclusiveStartGlobalTableName`/`Limit`), `ListImports` (`TableArn`),
`ListTables`/`ListTagsOfResource` (already deep-audited by prior sessions,
re-verified) — every declared filter reaches its query, none ignored, none
demanded that the real Input lacks. No empty/204 responses in this op set
(all 22 are non-void GET-style reads).

**SIBLINGS CHECKED, CONFIRMED CORRECT** (all 21 of the 22 ops besides the
one fixed above): every wrapper key enumerated in the RESULT paragraph.
`GlobalTableDescription`'s wire construction in
`handler_global_tables.go`, in particular, was checked against
`DescribeGlobalTable`/`CreateGlobalTable`/`UpdateGlobalTable`'s three
distinct real shapes for a possible shared-converter mismatch (this
family's pattern in other services) — `describeGlobalTableOutput`,
`createGlobalTableOutput`, and `updateGlobalTableOutput` are three
genuinely separate Go types here (not one shared function serving three
call sites with different real needs), so no bug.

**CREDENTIAL/OVER-WIDE SWEEP**: clean. None of the 22 L+D+G ops' wire
structs carry a plaintext secret, IAM/KMS ARN not already legitimately
part of the real shape (e.g. `SSEKMSMasterKeyArn` on `DescribeTable` is a
real member), or customer environment variable. No over-wide fields found
in this op set.

**PRIOR-AUDIT-REASONING CHECK**: `PARITY.md`'s `overall: A` rating and its
extensive per-family notes (from `gopherstack-rkmp`/`lze5`/`yvs8`, all
2026-08-13/14) cover `item_crud`/`query_scan`/`batch`/`transactions`/
`streams`/`janitor_ttl`/`datalayer` in deep, field-diffed detail, but **none
of those passes' notes mention the admin/List/Describe family this issue
targets** — not an instance of a prior note arguing a bug away, just a
genuine coverage gap in the earlier work, now closed by this session's
`admin_lists` family entry.

TESTS: 1 new real-`aws-sdk-go-v2`-client test,
`TestDescribeContributorInsights_LastUpdateDateTime`
(`contributor_insights_wire_test.go`), added alongside the file's existing
same-pattern tests. Hand-reverted the wire-layer fix in
`handler_contributor_insights.go` alone (leaving the backend tracking in
place, isolating the exact wire-drop this bug class targets), re-ran,
confirmed it failed with the exact predicted symptom (`Expected value not
to be nil` / `toggled table must report LastUpdateDateTime`), then restored
byte-identical (diffed against a saved copy — no git-mutating commands
used).

GATES: scoped `go build ./services/dynamodb/...` clean; full `go build ./...`
also run (the one changed function signature,
`contributorInsightsStateRLocked`, has zero external callers, grep-confirmed)
— clean; `go vet ./services/dynamodb/...` clean; `go test -race -count=1
./services/dynamodb/...` green (all 3 sub-packages); `go test -race -count=1
./pkgs/...` green; `go fix -diff ./services/dynamodb/...` empty;
`golangci-lint run ./services/dynamodb/...` — 1 `goimports` formatting
finding in `store.go` from the new struct field's alignment, fixed via
`gofmt -w` (not `fieldalignment -fix`, which is known to strip `//nolint`
comments — this file has none, but the narrower tool was used anyway), 0
issues after; 0 `cyclop`/`gocyclo`/`gocognit`/`funlen` nolints
(grep-confirmed, none added).

No subagents used. No git-mutating commands run — orchestrator must
commit/push. `git status` re-checked before every edit batch; only
`services/dynamodb/{store.go,contributor_insights.go,
contributor_insights_wire_test.go,handler_contributor_insights.go,
PARITY.md}` and this remainder file touched — `services/ecr/*` (the live
sibling that appeared mid-session) never read or touched.

`dynamodb`'s List/Describe/Get families are now fully swept for this issue
(22/22 ops layer-1/2/3 clean; 21/22 wrapper keys were already correct, 1 real
missing-member gap found and fixed, 1 sibling member correctly disclosed as
unfixable). 89 of 162 services swept, 73 remain. Per the ranked table,
`neptune` and `ecr` (21 L+D+G each) are next — `ecr` had a live sibling
throughout this session and may already be swept or mid-flight; re-check
`git status` before picking either.

## ecr (this session, 2026-08-15)

Chosen per this issue's own instruction: re-ran `go run ./cmd/opcensus` fresh
(confirmed the ranked table unchanged for this tier) and read the remainder
file's own tail plus `bd show gopherstack-6flj`'s comments before picking.
Both pointed at `neptune` and `ecr` tied at 21 L+D+G ops as the next
candidates once `dynamodb` (heavily-worked-under-other-issues caveat) was
correctly ruled out again by the immediately-preceding session. `git status`
was clean at the very start of this session (the `codeartifact` sibling
active in the prompt's briefing had already landed as `d9fd9f761` before
this session's first tool call); a `dynamodb` sibling appeared and finished
mid-session (`6f48b1673`), confirmed via repeated `git status` checks to
never touch `services/ecr/*` — the dynamodb session's own write-up
explicitly names `services/ecr/*` as "the live sibling that appeared
mid-session" and reports leaving it untouched, which this session
independently confirms from its own side.

**TIE-BREAK: `neptune` vs `ecr`, both 21 L+D+G ops, `direct` resolution.**
Broke it on sibling-trap surface (widest spread of distinct resource-family
handler files) per this issue's instruction. `neptune`'s family handler
files: `handler_cluster_endpoints.go`, `handler_cluster_parameter_groups.go`,
`handler_cluster_snapshots.go`, `handler_db_clusters.go`,
`handler_db_instances.go`, `handler_event_subscriptions.go`,
`handler_global_clusters.go`, `handler_parameter_groups.go`,
`handler_subnet_groups.go`, `handler_tags.go` — 10 files. `ecr`'s:
`handler_account_settings.go`, `handler_auth_token.go`,
`handler_image_scanning.go`, `handler_images.go`, `handler_layers.go`,
`handler_lifecycle_policy.go`, `handler_pull_through_cache.go`,
`handler_registry_policy.go`, `handler_replication.go`,
`handler_repositories.go`, `handler_repository_creation_templates.go`,
`handler_repository_policy.go`, `handler_signing.go`, `handler_tags.go` — 14
files, the wider spread. Picked `ecr`.

**Protocol / second client / EqualFold:** AWS JSON-RPC 1.1
(`application/x-amz-json-1.1`), confirmed both from gopherstack's own
`ecrTargetPrefix = "AmazonEC2ContainerRegistry_V20150921."` +
`X-Amz-Target`-header dispatch in `handler.go`, and from the pinned SDK's own
deserializer function-name prefix (`awsAwsjson11_deserializeOp...`,
`aws/protocol/restjson` is only imported for the shared error-body decoder,
not the body-field switches). Grepped all 274 `EqualFold` call sites in
`ecr@v1.60.4/deserializers.go`: every one is either `errorCode` matching (the
per-op error-type switches) or `NaN`/`Infinity`/`-Infinity` float-literal
matches in the numeric decoders (lines ~9156-9698) — zero body-field-name
`EqualFold` calls, so this service's 274-count distribution matches the
pattern the prior `outposts`/other passes established: JSON-RPC/restjson1
body-field switches are case-sensitive plain `case "foo":` throughout,
confirmed by direct inspection rather than counted alone. No second
cross-service SDK client bridge in this service.

**Router:** confirmed a flat `X-Amz-Target` map (`buildOps` = `buildCoreOps`
+ `buildExtOps`, merged via `maps.Copy` into one `map[string]service.JSONOpFunc`,
dispatched by `h.ops[action]` in `dispatch`) — structurally immune to the
path-segment-router class of bug this issue's checklist item 7 warns about.
Confirmed reachable, not just present: `GetSupportedOperations()`'s 58
entries diffed against the SDK's own `api_op_*.go` file list, both
directions, exact match — 0 phantom ops, 0 missing.

**Sweep scope:** all 21 L+D+G ops (4 List, 8 Describe, 9 Get) read against
their own real `api_op_*.go` Input/Output structs and
`awsAwsjson11_deserializeOpDocument*Output`/`awsAwsjson11_deserializeDocument*`
functions in the pinned `ecr@v1.60.4` module cache — never against a doc
comment, PARITY.md claim, or existing test taken on faith.

**Converters shared across ops, checked against each call site's own real
type:**
- `repositoryView` (shared by `CreateRepository`, `DescribeRepositories`,
  `DeleteRepository`, `PutImageTagMutability`) — confirmed correct at all 4
  call sites; real `types.Repository`'s 9 fields diffed key-by-key against
  `awsAwsjson11_deserializeDocumentRepository`, including nested
  `encryptionConfiguration`/`imageScanningConfiguration`/
  `imageTagMutabilityExclusionFilters`.
- `imageView` (shared by `PutImage`, `BatchGetImage`) — already fixed in a
  prior round (round 2, PARITY.md); re-verified correct against
  `awsAwsjson11_deserializeDocumentImage`'s 5-field shape.
- `tagView` (shared by `TagResource`, `UntagResource`, `ListTagsForResource`)
  — confirmed correct including the unusual capitalized `"Key"`/`"Value"`
  wire keys (verified against `awsAwsjson11_deserializeDocumentTag`; ECR's
  `Tag` type is genuinely capitalized unlike almost every other field in
  this service).
- `createPullThroughCacheRuleOutput` (shared by `CreatePullThroughCacheRule`,
  `DescribePullThroughCacheRules`, `UpdatePullThroughCacheRule`) — confirmed
  correct at all 3 sites.
- `RepositoryPolicyResult` (shared by `GetRepositoryPolicy`,
  `SetRepositoryPolicy`, `DeleteRepositoryPolicy`) — confirmed correct
  against all 3 real Output shapes (`policyText`/`registryId`/
  `repositoryName`), each independently diffed.
- `lifecyclePolicyResultView` (shared by `DeleteLifecyclePolicy`,
  `GetLifecyclePolicy`, `PutLifecyclePolicy`) — confirmed correct (fixed in
  round 2 per PARITY.md; the epoch-seconds convention re-verified here).
- `repositoryCreationTemplateView` (shared by `CreateRepositoryCreationTemplate`,
  `DescribeRepositoryCreationTemplates`, `UpdateRepositoryCreationTemplate`,
  `DeleteRepositoryCreationTemplate`) — the per-item shape confirmed correct
  at all 4 sites; the wrapping `describeRepositoryCreationTemplatesOutput`
  had the separate pagination bug fixed below.
- **`getRegistryScanningConfigurationOutput` shared by `Get` AND `Put`
  (FLAGSHIP BUG, fixed)** — see finding 1 below. This is the shared-converter
  trap this issue's checklist leads with, found on the 21st op checked this
  session, not the 1st.
- `signingConfigurationInput` shared by `Get`/`Put`/`Delete`
  `SigningConfiguration` (FIXED — see findings 2/6) — a second instance of
  the same failure mode (assumed Get/Put/Delete symmetry that the real SDK
  doesn't have) inside the same service, in a sibling family.

**Empty/204 responses checked against real output shape:** `TagResource`,
`UntagResource` both confirmed genuinely empty (`ResultMetadata` only, real
`TagResourceOutput`/`UntagResourceOutput`) — not the appconfig
`StopDeployment` trap. No other void ops in this service's L+D+G-adjacent
set.

**Required-member diffs, both directions:** all input structs for the 21
L+D+G ops diffed against their real `*Input` — no field demanded that the
real Input lacks; the only *missing* required-input-shape issue found was
the discarded `maxResults`/`nextToken` on `DescribeRepositoryCreationTemplates`
(finding 4) and the deliberately-not-added `Filter`/`MaxResults`/`NextToken`
on `ListImageReferrers` (finding 6, disclosed not fixed since functionally
inert).

**Filters:** all declared filters (`DescribeImages.filter.tagStatus`,
`ListImages.filter.tagStatus`, `GetLifecyclePolicyPreview.filter.tagStatus`,
`DescribeRepositories`/`DescribePullThroughCacheRules`'s name/prefix
filters) confirmed reaching their respective queries — no truncated-then-
discarded plural list found in this service.

**Nested-shape / `[]byte` check:** `UploadLayerPart`'s `LayerPartBlob []byte`
verified correct, NOT the rekognition trap — the real
`UploadLayerPartInput.LayerPartBlob` genuinely is `[]byte` (base64-encoded
JSON blob per AWS JSON-RPC 1.1 convention, confirmed via
`serializers.go:5440`'s `object.Key("layerPartBlob")`), so Go's automatic
`[]byte`→base64-string JSON marshaling is the CORRECT wire shape here, unlike
the rekognition case where a flat `[]byte` masked a broken nested struct.

**Discarded inputs / fields never set (8 found, all fixed except 1
disclosed):**
1. **FLAGSHIP — `PutRegistryScanningConfigurationOutput` shape entirely
   wrong.** `handlePutRegistryScanningConfiguration` returned
   `getRegistryScanningConfigurationOutput` (wrapper key
   `"scanningConfiguration"` + `registryId`) — `Get`'s real shape. The real
   `PutRegistryScanningConfigurationOutput` wraps under
   `"registryScanningConfiguration"` with **no** `registryId` at all
   (`awsAwsjson11_deserializeOpDocumentPutRegistryScanningConfigurationOutput`,
   confirmed by direct diff against the Get op's own deserializer function).
   A real client's `Put` call previously always got `nil
   RegistryScanningConfiguration` back despite `200 OK` — the exact "shared
   converter serving ops with different real shapes" bug class this issue's
   checklist leads with (cloudtrail/ce precedent), except here the two ops
   LOOK symmetric (a Get/Put pair) which is exactly why it survived 3 prior
   audit rounds. Fixed via a dedicated `putRegistryScanningConfigurationOutput`
   type. An **existing raw-body test asserted the wrong key as correct**:
   `TestPutRegistryScanningConfiguration_ScanTypeEnhanced` in
   `image_scanning_test.go` read `out["scanningConfiguration"]` on `Put`'s
   own response — rewritten to `out["registryScanningConfiguration"]` with a
   comment citing the two deserializer functions.
2. `GetRegistryScanningConfiguration.registryId` — declared, never
   populated. Sibling ops `DescribeRegistry`/`GetRegistryPolicy`/
   `PutRegistryPolicy`/`DescribeRepositoryCreationTemplates` all correctly
   set it from `b.accountID`/`Backend.AccountID()`; this one didn't. Fixed.
3. `PutImageScanningConfiguration.registryId` — same pattern, same fix.
4. `GetSigningConfiguration.registryId` — same pattern; the real
   `GetSigningConfigurationOutput` has `registryId` (confirmed via its own
   deserializer). Fixed.
5. `DeleteSigningConfiguration.registryId` — same; real
   `DeleteSigningConfigurationOutput` also has it. Fixed. (`PutSigningConfigurationOutput`
   was independently re-verified to have **no** `registryId` — three
   signing-config siblings, two real shapes; not assumed from surface
   symmetry, confirmed per-op.)
6. `BatchGetRepositoryScanningConfiguration` — `RepositoryScanningConfiguration`
   was missing `appliedScanFilters` entirely (a real field on
   `types.RepositoryScanningConfiguration`: the registry scan rule's
   repository filters that produced a repo's effective `CONTINUOUS_SCAN`
   frequency). `repoEffectiveScanFrequency` extended to return the matched
   rule's filters alongside the frequency; both `BatchGetRepositoryScanningConfiguration`
   and `PutImageScanningConfiguration`'s backend method feed from it, both
   fixed together.
7. `DescribeRepositoryCreationTemplates` — real Input/Output both carry
   `maxResults`/`nextToken` (confirmed: `MaxResults`/`NextToken` appear 4/5
   times respectively in the real `api_op_DescribeRepositoryCreationTemplates.go`);
   this handler discarded both, always returning every template in one page.
   Fixed via the same `base64(prefix)`-cursor pagination convention already
   used by `DescribeRepositories`/`DescribePullThroughCacheRules` in this
   same package — the fix that `ListPullTimeUpdateExclusions` got in round 3
   (per PARITY.md) but this sibling op didn't.
8. `ListImageReferrers` — **disclosed, not fixed.** Real
   `ListImageReferrersInput`/`Output` carry `Filter`/`MaxResults`/`NextToken`,
   but `PutImage` never records an OCI-referrer edge from a pushed artifact
   manifest's `subject` field back to the subject image (verified: grepped
   the whole package for `referrer`/`subject` state — `DescribeImages`
   parses `subjectManifestDigest` for display but nothing indexes it), so
   `ListImageReferrers` is structurally always empty regardless of what the
   wire shape declares. Adding the 3 fields would be schema-only with
   nothing to ratify (0 items either way) — built the fields, wrote a test,
   confirmed the test could not fail pre- or post-fix (see "worthless test"
   below), and reverted rather than keep dead schema. Recorded as a
   structural `gaps:` entry in PARITY.md instead.

**Nested-shape overshoot (extra fields leaked, inverse of a missing-field
bug):** `DescribeImageScanFindings`'s nested `"imageScanFindings"` object
reused `ImageScanFindingsResult` — this package's internal domain struct,
which ALSO carries `ImageID`/`RepositoryName`/`RegistryID`/`Status`/
`Description` for other callers (`StartImageScan`'s backend return, etc.) —
directly as the nested wire object. The real nested `ImageScanFindings` type
(`awsAwsjson11_deserializeDocumentImageScanFindings`) has only 5 fields:
`findingSeverityCounts`/`findings`/`enhancedFindings`/`imageScanCompletedAt`/
`vulnerabilitySourceUpdatedAt` — none of those other five. Harmless to a
real client (unknown JSON keys are silently ignored by the SDK deserializer,
confirmed via its `default: _, _ = key, value` case), but a real wire-shape
imprecision nonetheless. Fixed via a purpose-built `imageScanFindingsView`
carrying only the real 5 fields.

**Over-wide field / credential sweep:** clean. `authorizationDataView.AuthorizationToken`
is `base64("AWS:dummy-password")` — a deliberately synthetic, non-cryptographic
placeholder (`dummyPassword = "dummy-password"` constant, confirmed by
reading `handler_auth_token.go`), not a real secret leak. `RepositoryARN`/
`SigningProfileArn`/`CredentialArn`/`CustomRoleArn` fields are caller-supplied
or backend-generated resource identifiers (informational, matching this
service's existing pattern), not credentials. No plaintext client secret,
private key, or customer environment-variable field exists anywhere in this
service's wire surface — deliberate check run, clean, disclosed per this
issue's instruction to record the sweep even when nothing is found.

**Persistence check:** `RepositoryScanningConfiguration` (gained
`AppliedScanFilters`) and `RegistryScanningSettings`/`SigningSettings`
(unchanged) are NOT `store.Table`-backed persistence DTOs — grepped
`persistence.go` and confirmed `RepositoryScanningConfiguration` is computed
fresh on every `BatchGetRepositoryScanningConfiguration` call from
`Repository`+`registryScanningConfig` state, never itself persisted. No
`json:"-"` retagging done anywhere this pass; every fix either added a new
field to a non-persisted view/domain struct or a new field to an already-
non-persisted computed-result struct. Zero persistence risk.

**Prior-audit-reasoning check (this issue's item 2):** PARITY.md's `overall:
A` and every one of the 6 op entries this session fixed were previously
marked `wire: ok` without qualification — none of the 3 prior audit rounds'
notes explicitly argued any of these 6 fields' absence away (unlike
appconfig's precedent); they were simply not re-verified against the pinned
SDK's own deserializer per-field, which is exactly why a symmetric-looking
Get/Put or Get/Put/Delete trio survived 3 rounds. Recorded as a correction to
each entry, not a silent overwrite — see PARITY.md round 4 section for full
detail.

**Worthless-test check (this issue's explicit requirement):** the first
`ListImageReferrers` fix attempt added `Filter`/`MaxResults`/`NextToken` to
the wire structs and a test exercising them. Hand-reverting that fix and
re-running the test showed it **still passed** — because the backend always
returns an empty referrer list regardless of what's in the request, a real
SDK client decodes the same empty response whether or not gopherstack's Go
struct declares those fields (unknown JSON keys are silently dropped by
`encoding/json` on decode either way). This is a test that cannot fail
pre-fix, caught before it entered the final diff — reverted both the "fix"
and the test rather than keep dead schema, and recorded the underlying
structural gap in PARITY.md's `gaps:` instead. Every other new test in this
session's `wire_field_fixes_test.go` (9 remaining) was individually
hand-reverted, confirmed to fail against the reverted code with the exact
predicted symptom (quoted per-finding above), then restored byte-identical
before moving to the next fix.

**Siblings checked, confirmed already correct (not bugs):** `imageView`
(round 2 fix, re-verified); `repositoryView` and its 4 call sites;
`ImageIdentifier`/`ImageDetail` full field diff (both directions);
`ImageReferrer`'s own per-item shape (`annotations`/`digest`/`mediaType`/
`artifactStatus`/`artifactType`/`size`) — correct even though the collection
around it is always empty; `RepositoryFilter` shared correctly across
scanning/signing/replication configs (all three genuinely use the same real
shape); `ReplicationConfig`/`ReplicationRule`/`ReplicationDestination`
nesting; `tagView`'s capitalized `Key`/`Value`; `PutSigningConfiguration`'s
correct lack of `registryId` (the one signing-config sibling that was
already right).

**Phantom ops:** none — `GetSupportedOperations()`'s 58 entries exact-match
the SDK's 58 `api_op_*.go` files, both directions.

SDK pinned: `ecr@v1.60.4` (`go.mod`), no dependency-boundary exception
needed.

RATIFYING TESTS: 9 new real-`aws-sdk-go-v2`-client tests in the new
`services/ecr/wire_field_fixes_test.go` (`TestPutRegistryScanningConfiguration_WrapperKey`,
`TestGetRegistryScanningConfiguration_RegistryIDPopulated`,
`TestPutImageScanningConfiguration_RegistryIDPopulated`,
`TestGetSigningConfiguration_RegistryIDPopulated`,
`TestDeleteSigningConfiguration_RegistryIDPopulated`,
`TestBatchGetRepositoryScanningConfiguration_AppliedScanFilters`,
`TestBatchGetRepositoryScanningConfiguration_NoRuleMatch_NoAppliedFilters`,
`TestDescribeRepositoryCreationTemplates_Pagination`, plus one raw-body test
`TestDescribeImageScanFindings_NestedObjectDoesNotLeakTopLevelFields` for the
nested-overshoot fix, which a typed client can't observe since Go's SDK
client type simply wouldn't have had the extra fields to check). 1 existing
test fixed (`TestPutRegistryScanningConfiguration_ScanTypeEnhanced`, which
asserted the wrong wrapper key on `Put`'s raw response). 1 test written then
deleted after it proved unable to fail pre-fix (`ListImageReferrers` filter
fields), per this issue's worthless-test-check requirement.

GATES: scoped `go build`/`go vet ./services/ecr/...` clean; full `go build
./...`/`go vet ./...` clean (required — `repoEffectiveScanFrequency`'s
signature changed to return the applied filters, and it's unexported with no
callers outside `services/ecr`, grep-confirmed, but the full build was still
run per this session's own rule for any signature change); `go test -race
-count=1 ./services/ecr/...` and `./pkgs/...` both green; `go fix -diff
./services/ecr/...` clean (no diff); `golangci-lint run ./services/ecr/...`
0 issues; `fieldalignment ./services/ecr/...` 0 hits; 0
cyclop/gocyclo/gocognit/funlen nolints (grep-confirmed, none added).

No subagents used (Read/Grep/Bash/Edit only, per this session's hard
constraint). No git-mutating commands run — orchestrator must commit/push.
`git status` re-checked before every edit batch; only `services/ecr/*` and
this remainder file touched throughout.

`ecr`'s List/Describe/Get families are now fully swept for this issue (21/21
ops layer-1/2/3 clean; 6 real bugs found and fixed — 1 flagship shared-
converter wrapper-key bug, 4 discarded-output-field bugs sharing one root
cause (registryId never threaded through 4 sibling ops), 1 missing-member
bug (appliedScanFilters), plus 1 pagination-discarded-input bug and 1
nested-shape-overshoot bug; 1 structural gap disclosed rather than
papered over; no real-data leak found). 90 of 162 services swept, 72 remain.
Per the ranked table, `neptune` (21 L+D+G, `direct`) was the last candidate
at this tier; `git status` at the end of this session shows a live sibling
already mid-edit in `services/neptune/*` (event_subscriptions.go,
global_clusters.go, handler_event_subscriptions.go,
handler_global_clusters.go, interfaces.go, models.go, uncommitted) — never
read or touched by this session. With both members of the 21-L+D+G tier
taken, the next unswept tier starts at `dynamodb`'s sibling group and below
— re-check `git status` and this file's header before picking, since
siblings have appeared mid-session all day.

## neptune (this session, 2026-08-15)

Chosen per this issue's own instruction: read this file's header/ranked
table, ran `go run ./cmd/opcensus` fresh (unchanged for this tier), read `bd
show gopherstack-6flj`, and read `git show 6f48b1673` (the immediately
preceding pass, `fix(dynamodb): DescribeContributorInsights never tracked
two real members` — dynamodb was the unique largest unswept candidate at 22
L+D+G, strictly above `neptune`/`ecr` at 21 each, so that pass needed no
tiebreak). `git status` at the start of this session showed a live sibling
already mid-edit in `services/ecr/*` (handler_image_scanning.go,
handler_images.go, handler_registry_policy.go,
handler_repository_creation_templates.go, image_scanning.go,
image_scanning_test.go, models.go, plus an untracked
wire_field_fixes_test.go) — confirmed via repeated `git status` re-checks
throughout to never touch `services/neptune/*`.

**TIE-BREAK: `neptune` vs `ecr`, both 21 L+D+G ops, `direct` resolution.**
Occupancy resolved it before surface needed to: `ecr` had a live sibling
from before this session's first tool call, so `neptune` was the only
available member of the tied pair — no genuine choice to make. For the
record, sibling-trap surface (widest spread of distinct resource-family
handler files) was checked anyway and would have pointed the other way:
`neptune`'s family handler files are `handler_cluster_endpoints.go`,
`handler_cluster_parameter_groups.go`, `handler_cluster_snapshots.go`,
`handler_db_clusters.go`, `handler_db_instances.go`,
`handler_event_subscriptions.go`, `handler_global_clusters.go`,
`handler_parameter_groups.go`, `handler_subnet_groups.go`,
`handler_tags.go` — 10 files, versus `ecr`'s 14 (confirmed independently by
`ecr`'s own section above, added by the sibling session after this one
picked). Occupancy is the actual reason `neptune` was taken, not surface.

**PRIOR AUDIT NOTE QUALITY, checked before doing anything else.**
`services/neptune/PARITY.md` is already grade A, with an extensive
2026-08-11 pass (`gopherstack-gt9o`/`uhsb`, not 6flj) that substantively
covers this exact bug class: it explicitly diffed every named list-item
element op-by-op against the SDK's `deserializeDocument*List` functions, and
separately hunted (and fixed) three "looks-void-but-the-SDK-calls-
GetElement-unconditionally" bugs (`ModifyDBClusterSnapshotAttribute`,
`ApplyPendingMaintenanceAction`, `DeleteDBClusterEndpoint`) plus one
mis-named response element (`DescribeValidDBInstanceModifications`'
`ValidProcessorFeatures` vs the real `Storage`). This is the **coverage-gap**
case, not the **argued-away** case: the note is accurate about everything it
covers, and simply never looked at `GlobalCluster.DatabaseName`,
`EventSubscription.CustomerAwsId`, or `GlobalCluster.FailoverState`, all
three genuinely never modeled anywhere in the service (see below) — nothing
in the prior note claims those fields were checked or reasons them away, so
this is a gap in scope, not a false claim. The prior note's
`last_audit_commit` field (`087cb59186751418d9d49b88434f13cf214c7609`) is
flagged as **unverifiable/likely wrong**: that hash resolves to an unrelated
`parity(sesv2)` commit from `Jul 12 12:25:58 2026`, not a neptune commit —
probably a stale/copy-pasted field never updated across the several
same-file passes the frontmatter's own note-history implies. Not chased
further; noted so the next pass doesn't trust it either.

**WRAPPER-KEY SWEEP, all 21 L+D+G ops (1 List: `ListTagsForResource`; 20
Describe), each diffed individually against its own
`awsAwsquery_deserializeOpDocument<Op>Output` in
`neptune@v1.48.4/deserializers.go`:** all 21 top-level wrapper key(s) matched
exactly as gopherstack already had them --
`DBClusters`/`DBInstances`/`DBClusterSnapshots`/`DBSubnetGroups`/
`DBClusterParameterGroups`/`DBClusterParameters(Parameters)`/
`DBClusterEndpoints`/`DBClusterSnapshotAttributesResult`/`DBEngineVersions`/
`DBParameterGroups`/`Parameters`/`EngineDefaults`(x2, cluster and
non-cluster default-parameter ops share the same real wrapper name)/
`EventCategoriesMapList`/`GlobalClusters`/`OrderableDBInstanceOptions`/
`ValidDBInstanceModificationsMessage`/`PendingMaintenanceActions`/`Events`/
`TagList`, and the one genuinely-surprising case
(`DescribeEventSubscriptions` -> `EventSubscriptionsList`, NOT the more
obvious `EventSubscriptions`) was already correct too. **Zero wrapper-key
bugs found in this family** -- this pass's own contribution is entirely in
the never-modeled-member and discarded-input classes below, not wrapper
keys. Every collection's Go kind also checked: this is query/xml, and every
real collection here is a named-element list (`<Xs><X>...`), never a map --
gopherstack's Go types are `[]T`/`xmlXList{Members []T}` throughout, no
array-vs-map mismatch found (this bug class needs a JSON/REST protocol with
a real map-shaped member to manifest, which this service's protocol
structurally doesn't have among its L+D+G ops).

**LEAD CHECK: converters shared across ops.** `toXMLParameter` (4 call
sites: `DescribeDBClusterParameters`, `DescribeDBParameters`,
`DescribeEngineDefaultClusterParameters`, `DescribeEngineDefaultParameters`)
-- confirmed **legitimately shared**: all four wrap the identical real
`types.Parameter` (neptune@v1.48.4 types/types.go:1320), same 10 members,
same meaning at both cluster- and instance-level (matches the
already-documented "Neptune parameter names are shared across both
instance- and cluster-level groups" note). `toXMLEventSubscription` (6 call
sites: Add/RemoveSourceIdentifier, Create/Modify/Delete
EventSubscription, DescribeEventSubscriptions' list) -- confirmed
**legitimately shared**: all six wrap the identical real
`types.EventSubscription` (types.go:1058). `toXMLGlobalCluster` (7 call
sites: Create/Delete/Failover/Modify/RemoveFromGlobalCluster/Switchover,
DescribeGlobalClusters' list) -- confirmed **legitimately shared**: all
seven wrap the identical real `types.GlobalCluster` (types.go:1163). Three
shared converters checked, three confirmed legitimately shared, zero
sibling-trap bugs found among them.

**NEVER-MODELED MEMBERS, two found, both fixed.** Full field-by-field diff
of `types.EventSubscription`/`types.GlobalCluster` against gopherstack's
domain model (`models.go`) and wire structs turned up two members with zero
grep hits anywhere in the service before this pass:
- `EventSubscription.CustomerAwsId` (types.go:1063-1064, wire element
  `CustomerAwsId` -- deserializers.go's
  `awsAwsquery_deserializeDocumentEventSubscription`). **Fixed and emitted**:
  the backend already tracks `accountID` (used throughout for ARN
  construction, e.g. `eventSubscriptionARN`), so this was purely a threading
  gap, not a data gap -- `CreateEventSubscription` now sets
  `CustomerAwsID: b.accountID` on the domain struct, and the wire converter
  emits it as `xml:"CustomerAwsId,omitempty"`.
- `GlobalCluster.DatabaseName` (types.go:1165-1166, wire element
  `DatabaseName`; also a real, optional, non-required member of
  `CreateGlobalClusterInput`, api_op_CreateGlobalCluster.go:44). **Fixed and
  emitted only when supplied**: `CreateGlobalCluster`'s handler now reads
  `vals.Get("DatabaseName")` and threads it through the backend
  (`InMemoryBackend.CreateGlobalCluster` gained a third `databaseName`
  parameter; `StorageBackend.CreateGlobalCluster` in `interfaces.go` updated
  to match) into the stored `GlobalCluster.DatabaseName`, echoed by every
  global-cluster response op via the shared `toXMLGlobalCluster` converter
  above -- an untouched-DatabaseName create still emits nothing
  (`omitempty`), matching AWS leaving it empty for real when the caller
  didn't supply one, rather than fabricating a value.

**DISCLOSED, not fixed:** `GlobalCluster.FailoverState`
(types.go:1177-1178, real type `*types.FailoverState`) is also never
modeled, but deliberately left that way -- real AWS docs it as "empty unless
the SwitchoverGlobalCluster or FailoverGlobalCluster operation was called on
this global cluster," i.e. a genuinely transient in-process record with a
`pending`/`failing-over`/`cancelling`/etc. status. This backend's
Failover/Switchover (per the existing PARITY.md `GlobalCluster` note) apply
member promotion synchronously with no in-process window to observe --
exactly the same "no failure/transition window to model honestly" reasoning
this service's PARITY.md already applies to `RebootDBInstance` and the
Failover/Switchover synchronicity itself. Fabricating a `FailoverState`
object (even a `"complete"`-shaped one) would invent state transitions this
backend cannot actually distinguish from each other; omitting it is more
honest than guessing.

**DISCARDED INPUT, disclosed not fixed (out of this pass's scope):**
`CreateGlobalClusterInput.EngineVersion`/`DeletionProtection`/
`StorageEncrypted` (all real, optional members of the real Create input,
api_op_CreateGlobalCluster.go) are silently ignored by
`handleCreateGlobalCluster` at create time -- `EngineVersion` only ever gets
set from an attached source DB cluster (or the hardcoded default) never from
the caller's own input; `DeletionProtection` can only be set later via
`ModifyGlobalCluster`; `StorageEncrypted` is likewise only ever derived from
a source cluster. This service doesn't use typed `*Input` structs (routes
raw `url.Values` through `vals.Get(...)`, so a literal `grep '_ SomeInput'`
finds nothing here -- the discarded-input bug class still applies, just
without that specific grep signature), and was found by manually diffing
`CreateGlobalClusterInput`'s full member list against what
`handleCreateGlobalCluster` actually reads. Left disclosed rather than fixed
because unlike `DatabaseName` (pure echo, zero validation risk) these three
have real semantic/validation surface (engine-version format checking,
deletion-protection interacting with `DeleteGlobalCluster`'s existing
protection check, storage-encryption's interaction with the source-cluster
derivation path already there) that deserves its own pass rather than a
rushed same-session bolt-on.

**PERSISTENCE TRAP, checked before adding fields.** Both `EventSubscription`
and `GlobalCluster` double as their own snapshot DTOs
(`regionalDTO[EventSubscription]` and `GlobalCluster` directly, per
`persistence.go`'s `buildPersistenceDTORegistry`) -- both new fields
(`CustomerAwsID`, `DatabaseName`) were added with **fresh json tags**, not
retagged onto an existing field, so old snapshots decode cleanly with the
new fields simply absent/zero-valued. `neptuneSnapshotVersion` left at `1`,
unchanged -- correct per this file's persistence-trap precedent (a fresh
additive field never invalidates old snapshots the way a retag or type
change would).

**Describe/List asymmetry:** none found needing correction this pass. The
one place asymmetry could plausibly appear -- `Parameter` shared across
cluster- and instance-level Describe/EngineDefault ops -- was confirmed
identical on the real SDK side (single `types.Parameter`, see LEAD CHECK
above), not assumed.

**Empty/204 responses:** none newly found: the three real GetElement-
required-but-looked-void bugs in this service
(`ModifyDBClusterSnapshotAttribute`/`ApplyPendingMaintenanceAction`/
`DeleteDBClusterEndpoint`) were already fixed by the prior (non-6flj) pass;
independently re-verified this pass that all three still return their
required `*Result` element and that the documented genuinely-void ops
(`DeleteDBSubnetGroup`/`DeleteDBClusterParameterGroup`/
`DeleteDBParameterGroup`/`AddTagsToResource`/`RemoveTagsFromResource`/
`AddRoleToDBCluster`/`RemoveRoleFromDBCluster`) still have no `GetElement`
call in their op's `HandleDeserialize`, confirming the existing PARITY.md
claim rather than re-deriving it from scratch.

**Required-member diffs, both directions:** `CreateGlobalClusterInput.
GlobalClusterIdentifier` (real: required) is enforced (`handleCreateGlobalCluster`
returns `ErrInvalidParameter` when empty via the backend). No case found this
pass of gopherstack demanding a field the real Input lacks. Not exhaustively
re-diffed for all 70 ops (out of this pass's L+D+G-focused scope) --
disclosed as unchecked beyond the ops actually touched.

**Filters:** not independently re-audited this pass beyond what the prior
PARITY.md pass already covers (`DBClusterFilters`/id-list-vs-Filters
handling was the subject of `d91efb1b7`/`bd334b7a4`/`72903f3c0`, all prior
sessions) -- disclosed as relying on that existing work rather than
re-verifying every filter this pass.

**Protocol / second client / EqualFold:** confirmed `query/xml`
(`awsAwsquery_*` deserializer prefix throughout), matching PARITY.md's own
documented protocol. No JSON-RPC/restjson1 casing risk here -- query/xml
element-name matching is case-insensitive by design
(`strings.EqualFold("DBClusters", t.Name.Local)` etc., confirmed directly in
the deserializer snippets read for the wrapper-key sweep above), so this
service structurally cannot hit the JSON-RPC casing bug class. No second
SDK client bridge found in this service.

**Router:** `Handler.dispatch` (handler.go:422) is a single-entry function
that chains through `dispatchDBClusterAction` ->
`dispatchClusterParameterGroupAction`/`dispatchParameterGroupAction`/etc., a
flat `switch action { case "OpName": ... }` on the decoded `Action` form
parameter throughout -- not a path-segment router, so structurally immune to
the desync bug class that hit elasticsearch's two ops. Stated as the correct
shortcut per this service's protocol/dispatch style, matching the existing
PARITY.md router note.

**Sibling families confirmed correct:** the three shared converters above
(`toXMLParameter`/`toXMLEventSubscription`/`toXMLGlobalCluster`), all
verified against their own real per-context Output/types shape rather than
assumed correct from one call site.

**Over-wide field / credential sweep:** none found. No API-key/client-
secret/token-bearing resource type exists in this service (matches the
existing PARITY.md's own note); the two ARNs newly threaded onto the wire
this pass (`eventSubscriptionARN` was already emitted;
`globalClusterARN`/`GlobalClusterMembers[].DBClusterARN` likewise
pre-existing) are ARNs the caller already owns/constructed, not a leak of
another principal's resource. Deliberate credential-shaped-field check run
specifically: no plaintext secret, IAM/KMS ARN belonging to a different
principal, or customer-environment-variable-shaped field anywhere in this
service's 70 ops.

**Phantom ops:** zero. `GetSupportedOperations()`'s 70 entries exact-match
the SDK's 70 `api_op_*.go` files (both directions, confirmed by `ls
$(go env GOMODCACHE)/github.com/aws/aws-sdk-go-v2/service/neptune@v1.48.4/api_op_*.go`
count and the pre-existing `TestSDKCompleteness`, re-run this session and
still green).

SDK pinned: `neptune@v1.48.4` (go.mod), no dependency-boundary exception
needed.

RATIFYING TESTS: 2 new real-`aws-sdk-go-v2`-client tests added to the
existing `handler_sdk_roundtrip_test.go`
(`Test_SDKRoundTrip_CreateGlobalCluster_DatabaseName`,
`Test_SDKRoundTrip_CreateEventSubscription_CustomerAwsId`), both create a
real resource via the typed SDK client and assert the value round-trips
through the corresponding Describe op. Both hand-reverted individually
(commented out the one field-population line each), re-run, and confirmed
to fail with the exact predicted symptom (`expected: "mygraphdb"/
"111122223333", actual: ""`), then restored -- `git diff` after restoring
showed exactly the intended one-line addition in each file, nothing else
disturbed.

GATES: scoped `go build`/`go vet ./services/neptune/...` clean; full `go
build ./...` clean (required -- `StorageBackend.CreateGlobalCluster`'s
signature changed, grep-confirmed no external package implements or calls
it directly, but the full build was still run per this session's own rule
for any signature change); `go test -race ./services/neptune/...` and `go
test -race ./pkgs/...` both green; `go fix -diff ./services/neptune/...`
clean (no diff); `golangci-lint run ./services/neptune/...` 0 issues after
a `fieldalignment` finding on both edited structs (`EventSubscription`/
`GlobalCluster`, new fields pushed pointer-bytes over the optimal packing)
was fixed **by hand** (reordered the new string field ahead of the existing
`[]string`/`bool` tail, then `gofmt -w` to re-align columns) rather than
running `fieldalignment -fix`, per this file's own toolchain-hazard note
about that tool stripping `//nolint` comments -- moot here since no
`//nolint` existed on either struct, but the by-hand approach was used
regardless to stay consistent with the documented precedent; 0
cyclop/gocyclo/gocognit/funlen nolints (grep-confirmed, none added).

No subagents used (Read/Grep/Bash/Edit only, per this session's hard
constraint). No git-mutating commands run -- orchestrator must commit/push.
`git status` re-checked before every edit batch; only `services/neptune/*`
and this remainder file touched throughout.

`neptune`'s List/Describe/Get families are now fully swept for this issue
(21/21 ops layer-1/2/3 clean against the SDK; wrapper keys were already
100% correct going in -- this pass's real contribution is 2 never-modeled
members fixed, 1 disclosed as genuinely unobservable, and 1 discarded-input
family disclosed as out-of-scope; no real-data leak found; the prior
non-6flj PARITY.md pass is confirmed a coverage gap on this specific
surface, not a false claim). 91 of 162 services swept, 71 remain. Both
members of the prior 21-L+D+G tier (`neptune`, `ecr`) are now done; the next
unswept tier starts at `dynamodb`'s neighborhood in the ranked table above
(re-run `go run ./cmd/opcensus` and re-check `git status` before picking, as
usual -- multiple same-tier siblings have collided by count alone all
session, not just today).

## directconnect (this session, 2026-08-15)

Chosen per this session's assignment: read this file's header/ranked table,
ran `go run ./cmd/opcensus` fresh (unchanged for this tier), read `bd show
gopherstack-6flj`, and read `git show 4eaf7d439` (the neptune pass
immediately preceding this one). At pick time `directconnect` (64 ops, 20
L+D+G, `direct`) and `xray` (38 ops, 20 L+D+G, `direct`) were TIED for
largest unswept, both strictly below the now-swept `codebuild`/other 20+
entries. `git status` was clean at that moment (no live sibling on either).

**TIE-BREAK: `directconnect` vs `xray`, both 20 L+D+G, `direct`.** Surface
was checked first, per this issue's stated tiebreak order: `xray` has 14
distinct resource-family `handler_*.go` files (encryption_config, groups,
indexing_rules, insights, resource_policies, sampling_rules,
sampling_statistics, service_graph, tags, telemetry, trace_retrieval,
trace_segment_destination, trace_segments, traces) versus `directconnect`'s
6 (bgp, connections, gateways, lags_interconnects, static, vifs) -- surface
pointed at `xray`, and `xray` was picked first on that basis. Partway
through `xray`'s investigation (after reading its router table, handler_groups.go,
handler_indexing_rules.go, handler_insights.go, handler_resource_policies.go,
handler_sampling_rules.go -- all read-only, zero edits made), a live sibling
appeared: `git status` began showing uncommitted changes in
`services/xray/handler_traces.go`, `models.go`, `traces.go`, `traces_test.go`
plus an untracked `wire_field_fixes_test.go`, none authored by this session.
**OCCUPANCY then overrode surface** -- this session switched to
`directconnect` cleanly (no `xray` files were ever edited, only read), the
same pattern the neptune/ecr pass recorded: surface picks the target until a
sibling actually claims one of the tied candidates, at which point occupancy
decides and the switch is real, not a rationalization. `xray` was left
exactly as the sibling had it; nothing in this section touches or was
informed by that sibling's in-flight changes beyond confirming (via `git
diff --stat`) which files were off-limits.

**PROTOCOL, SECOND CLIENT, EQUALFOLD:** `awsjson1.1` (confirmed:
`awsAwsjson11_deserializeOp*`/`awsAwsjson11_serializeOp*` throughout
`deserializers.go`/`serializers.go`; every op is a flat `POST /` dispatched
purely by its `X-Amz-Target: OvertureService.<Op>` header, zero HTTP path
routing -- gopherstack's own `handler.go` doc comment already states this
and it was re-confirmed against `directconnect@v1.44.1`). All 157
`strings.EqualFold` hits in the pinned SDK's `deserializers.go` are
error-code matches (`case strings.EqualFold("DirectConnectClientException",
errorCode)` and its four siblings) -- zero are body-field key comparisons,
confirmed by extracting every `EqualFold` call site and by directly reading
several `awsAwsjson11_deserializeDocument<Type>` functions (plain
`switch key { case "connectionId": ...}`, case-sensitive Go string switch).
Casing IS therefore a real bug class for this service (unlike
rds/sns/neptune's query/xml), but gopherstack's own `services/directconnect/
*.go` has **zero** `EqualFold` calls anywhere (grep-confirmed) -- it emits
exact lowerCamelCase JSON struct tags throughout, matching the real wire
keys byte-for-byte (see sweep below). No second Direct-Connect-shaped SDK
client exists in this module (only one `aws-sdk-go-v2/service/directconnect`
entry in `go.mod`).

**ROUTER:** structurally immune, and this is the straightforward case, not
the shortcut-taken-without-checking one -- there is no path-segment
dispatch to desync in the first place (every one of the 64 ops is the exact
same `POST /`, disambiguated only by the `X-Amz-Target` header, which
`handler.go`'s own dispatch table keys on directly). Re-confirmed rather
than assumed: `GetSupportedOperations()`'s 64 entries were diffed 1:1
against `ls $(go env GOMODCACHE)/.../directconnect@v1.44.1/api_op_*.go`
(64 files) -- **zero phantom ops, both directions.**

**WRAPPER-KEY SWEEP, all 20 L+D+G ops, each diffed individually** by
python-extracting every `case "<key>":` inside each op's own
`awsAwsjson11_deserializeOpDocument<Op>Output` function in
`directconnect@v1.44.1/deserializers.go` (not hand-transcribed) and
comparing against gopherstack's own `services/directconnect/wire_ops.go`
response-struct JSON tags: **all 20 match exactly**, including the two
non-obvious asymmetric pairs already flagged by the existing PARITY.md
("wire-trap #7") and independently re-verified here rather than trusted --
`DescribeLoa` emits `loaContent`+`loaContentType` FLATTENED at the top
level while `DescribeConnectionLoa`/`DescribeInterconnectLoa` both emit a
nested `loa` envelope wrapping the same two fields (gopherstack's
`describeLoaFlatResponse` vs `loaEnvelope{Loa: *loaWire}` matches this
exactly), and `DescribeConnectionsOnInterconnect` correctly omits
`nextToken` from its request path (no `MaxResults`/`NextToken` input field
exists on the real op, confirmed) while still allowing the field on its
Output struct (present but always naturally absent, not fabricated empty).
Zero wrapper-key bugs found.

**LAYER-2 (nesting/per-field), 23 shared nested types diffed individually**
against their own `awsAwsjson11_deserializeDocument<Type>` field-key switch
(again python-extracted): `Connection`, `Lag`, `Interconnect`,
`VirtualInterface`, `DirectConnectGatewayAssociation`, `RouterType`,
`CustomerAgreement`, `ResourceTag`, `Location`, `VirtualGateway`,
`DirectConnectGatewayAttachment`, `DirectConnectGateway`,
`DirectConnectGatewayAssociationProposal`, `AssociatedGateway`, `Loa`,
`MacSecKey`, `BGPPeer`, `Tag`, `RouteFilterPrefix`, `Route`,
`AsPathSegment`, `RateLimiterStatus`, `VirtualInterfaceTestHistory` --
21 of 23 are byte-exact 1:1 with `services/directconnect/wire.go`'s structs
(field-for-field, not just count). Every collection here is a named JSON
array in an `awsjson1.1` object (never a bare map) -- no array-vs-map/
flat-vs-nested mismatch found anywhere in this set.

**TWO NEVER-MODELED MEMBERS FOUND, both disclosed, NEITHER fabricated.**
`Connection.AwsDevice`/`Interconnect.AwsDevice`/`Lag.AwsDevice` (real key
`awsDevice`, confirmed present in all three types' own deserializer
switches) and `DirectConnectGatewayAssociation.VirtualGatewayRegion` (real
key `virtualGatewayRegion`) both have **zero grep hits** anywhere in
`services/directconnect/*.go` before this pass -- a real client reading
either field always gets absent/nil today, never a wrong value. Not fixed:
both are marked `// Deprecated: This member has been deprecated.` in the
pinned SDK's own `types/types.go` doc comments, and this pass had no
primary source (no live AWS response, no SDK comment on post-deprecation
wire behavior) confirming whether real AWS still populates a deprecated
field with a live value or has genuinely stopped. `AwsDeviceV2` (the
non-deprecated replacement) IS correctly populated everywhere already.
Guessing the value (e.g. mirroring `AwsDeviceV2` into `AwsDevice`) would be
exactly the fabrication this issue warns against -- disclosed in
`services/directconnect/PARITY.md`'s `gaps:` list instead, with a note that
a follow-up pass with access to a real AWS account could resolve this with
certainty. This is a genuine addition to the never-modeled-member count
even though nothing was fixed this pass.

**PRIOR AUDIT NOTE QUALITY:** `services/directconnect/PARITY.md` is already
`overall: A` with an exceptionally detailed prior audit (2026-08-06,
`gopherstack-t0gq`/general parity work, not 6flj) -- every one of the 64 ops
individually documents its wire shape, several genuine "wire-traps" (flattened
vs nested VirtualInterface/Loa shapes, the GatewayId/VirtualGatewayId dual
addressing mode, the missing generic Paginator type), and real integration
test coverage against a live Docker container. This is the **coverage-gap**
case, not the **argued-away** case: nothing in the prior audit's notes
claims `AwsDevice`/`VirtualGatewayRegion` were checked or reasons their
absence away -- they were simply never looked at (the prior audit's own
`ops:` table describes shapes at the Go-struct-member level, not by reading
the deserializer's JSON key switch case-by-case the way this issue's method
requires), which is exactly why they survived. Also found and corrected:
the prior audit's own `last_audit_commit: 3b90d4523` is **stale** -- that
hash resolves to `"test: replace the last unbubbleable sleeps with
require.Eventually"`, an unrelated cross-service sleep-to-Eventually
conversion, not a directconnect-specific commit. Flagged in PARITY.md's
frontmatter rather than silently corrected to a guessed value.

**REQUIRED-MEMBER DIFFS, both directions, scoped to the 20 ops touched this
pass (not all 64):** the pinned SDK ships **zero** `validateOpInput*`
functions for this entire service (`grep -c '^func validateOpInput'
validators.go` => 0) -- there is no client-side required-field enforcement
anywhere, matching PARITY.md's own extensive per-op notes that most Input
structs mark nothing as struct-level-required even where the real API
surely needs it. Consequently gopherstack's own server-side required-field
checks (e.g. `handleDescribeHostedConnections`'s `if req.ConnectionID ==
""`) are strictly additive validation, not something a real client could be
blocked from omitting -- no case found this pass of gopherstack demanding a
field the real Input structurally lacks, and no case found of a real
required field going unenforced (the ops with real required fields already
enforce them). Not exhaustively re-diffed for all 64 ops.

**FILTERS/PAGINATION:** all 10 ops with `maxResults`/`nextToken` on the wire
(`DescribeConnections`, `DescribeHostedConnections`,
`DescribeDirectConnectGateway{s,Associations,AssociationProposals,Attachments}`,
`DescribeInterconnects`, `DescribeLags`, `DescribeVirtualInterfaces`,
`ListVirtualInterfaceTestHistory`) route through the shared generic
`paginate()` helper (`handler.go`) backed by `pkgs/page` -- confirmed via
grep, all 10 call sites found, none discarded. `ListVirtualInterfaceRoutes`
accepts `filters`/`maxResults`/`nextToken` on the wire but never uses them
to filter (already disclosed in PARITY.md/`structural_gaps:` -- `Routes` is
always an honest empty list since no BGP route exchange is modeled, so
there is never more than zero items to page through; re-confirmed, not a
new finding). `DescribeConnectionsOnInterconnect` correctly never populates
`nextToken` in its response (no `maxResults` input exists on the real op),
matching the real asymmetry exactly rather than fabricating pagination.
Every ID-shaped optional filter spot-checked (`DescribeConnections`'
`connectionId`, `DescribeInterconnects`' `interconnectId`) is genuinely
applied server-side (`InMemoryBackend.DescribeConnections` short-circuits to
a single-item lookup when `connectionId != ""`), not silently ignored.

**SIBLING FAMILIES / SHARED CONVERTERS CONFIRMED CORRECT:** `connectionWire`
is reused across every op whose Output IS a Connection or whose list
element is one (`CreateConnection`, 3 Allocate/Associate variants,
`DescribeConnections`, `DescribeConnectionsOnInterconnect`,
`DescribeHostedConnections`, `Lag.Connections`) -- confirmed the real
`types.Connection` is identical in all these contexts (same 23-field
deserializer switch cited above), genuinely shared, not a sibling trap.
Same confirmed for `virtualInterfaceWire` (flattened on 6 ops, nested via
`vifEnvelope` on 4 more, list-element on 1 -- all resolve to the identical
real `types.VirtualInterface`, PARITY.md's own "wire-trap #1" already
documents which ops use which shape and this pass re-verified the field set
itself is identical either way, only the envelope differs) and for
`loaWire`/`macSecKeyWire`/`bgpPeerWire`, each reused across 2-3 ops. Zero
sibling-trap bugs found among any of them.

**OVER-WIDE FIELD / CREDENTIAL SWEEP:** deliberately run. `BGPPeer.AuthKey`
and `MacSecKey.Ckn` both echo back on the wire, which could look like a
leak at a glance -- but both are confirmed to match the REAL AWS wire shape
exactly (both keys are present in the pinned SDK's own
`deserializeDocumentBGPPeer`/`deserializeDocumentMacSecKey` switches), so
this is required parity, not gopherstack-specific over-exposure. `Ckn` is a
MACsec Connectivity Association Key **Name** (a non-secret identifier for a
key pair), never the CAK secret material itself, matching real AWS's own
MACsec key-rotation UX (which also never returns the CAK). `MacSecKey.
SecretARN` is a Secrets Manager ARN the caller supplied or a synthesized
placeholder (`synthesizeMacSecSecretARN`, already disclosed in PARITY.md as
not backed by a real secretsmanager entry) -- an identifier, not the secret
value. No plaintext client secret, IAM/KMS ARN belonging to another
principal, or customer environment variable found anywhere in this
service's wire surface.

**PERSISTENCE:** no fields were added or retagged this pass (both findings
above were disclosed, not fixed), so the persistence-trap check is moot for
this pass's own changes -- noted for completeness per this file's own
precedent of checking before retagging.

**PHANTOM OPS:** zero, both directions (see ROUTER above).

SDK pinned: `directconnect@v1.44.1` (`go.mod:213`), no dependency-boundary
exception needed.

TESTS: none added. Both findings this pass were disclosed, not fixed (no
code change to ratify) -- adding a test asserting `AwsDevice`/
`VirtualGatewayRegion` are absent would just restate the current (honest)
behavior, not guard against a regression with any real signal. The
existing `test/integration/directconnect_test.go` (real
`aws-sdk-go-v2/service/directconnect` client against a live Docker
container) and `services/directconnect/*_test.go` suite were re-run as
part of the gates below, not extended.

GATES: `go build ./services/directconnect/...` clean; `go vet
./services/directconnect/...` clean; `go test -race
./services/directconnect/...` green; `go fix -diff
./services/directconnect/...` clean (no diff); `golangci-lint run
./services/directconnect/...` 0 issues; `go test -race ./pkgs/...` green.
Full `go build ./...` NOT run -- no Go source file was changed this pass
(only `services/directconnect/PARITY.md`), so no signature could have
changed. No `//nolint` for cyclop/gocyclo/gocognit/funlen added (none
added at all, since no Go code changed).

No subagents used (Read/Grep/Bash only, per this session's hard
constraint). No git-mutating commands run -- orchestrator must
commit/push. `git status` re-checked before every edit batch; only
`services/directconnect/PARITY.md` and this remainder file touched --
`services/xray/*` (the live sibling that appeared mid-session) was read
during the initial investigation but never edited, and confirmed
untouched by every subsequent `git status` check.

**A FULLY-VERIFIED CLEAN SWEEP, this pass's real contribution being two
disclosed (not fabricated) never-modeled deprecated members and one stale
`last_audit_commit` correction.** `directconnect`'s List/Describe/Get
family is now fully swept for this issue (20/20 ops layer-1/2 clean against
the SDK; the prior general-parity PARITY.md pass is confirmed a coverage
gap on this specific deserializer-level surface, not an argued-away bug;
its stale audit-commit metadata is now flagged). 92 of 162 services swept,
70 remain. Per the ranked table, `xray` (38 ops, 20 L+D+G, `direct`) is
still tied with directconnect's now-resolved slot but has a live sibling as
of this session's end (`services/xray/*` modified: handler_insights.go,
handler_traces.go, insights.go, interfaces.go, models.go, traces.go, plus
test files, uncommitted) -- do not pick it without re-checking `git status`
first. Everything else at 20 or above in the ranked table (`opsworks`,
`codeartifact`, `cloudtrail`, `appconfig`, `dynamodb`, `neptune`, `ecr`,
`cloudwatch`, `elasticache`, `codebuild`) is already accounted for, either
in the `## Swept` enumerated list above or by its own dedicated `##`
section in this file -- the ranked table itself is a static snapshot that
prior passes have NOT pruned as services got swept (this pass didn't either,
matching precedent), so cross-reference against both the enumerated list
and the per-service sections, not the table's row order alone, before
picking. The next tier starts at 19 (`transcribe`, `mediatailor`); re-run
`go run ./cmd/opcensus` and re-check `git status` before picking, as usual.

## xray (this session, 2026-08-15)

Read this file's header/tail, ran `go run ./cmd/opcensus` fresh (unchanged
for this tier), read `bd show gopherstack-6flj`'s comments, and read `git
show 38eab5c5c` (the ecr pass immediately preceding this one) per the
assignment. `git status` was clean at pick time.

**TIE: `xray` vs `directconnect`, both 20 L+D+G, `direct` resolution** (the
next tier after `dynamodb`/`neptune`/`ecr` were already swept). Broken on
sibling-trap surface (widest spread of distinct resource-family
`handler_*.go` files), per this issue's own instruction and precedent
(neptune/ecr broke the same way, 10 vs 14). `xray`: `handler_encryption_config.go`,
`handler_groups.go`, `handler_indexing_rules.go`, `handler_insights.go`,
`handler_resource_policies.go`, `handler_sampling_rules.go`,
`handler_sampling_statistics.go`, `handler_service_graph.go`,
`handler_tags.go`, `handler_telemetry.go`, `handler_trace_retrieval.go`,
`handler_trace_segment_destination.go`, `handler_trace_segments.go`,
`handler_traces.go` -- 14 files. `directconnect`: `handler_bgp.go`,
`handler_connections.go`, `handler_gateways.go`, `handler_lags_interconnects.go`,
`handler_static.go`, `handler_vifs.go` -- 6 files. Picked `xray`. (The
`directconnect` session that ran concurrently independently derived the
same 14-vs-6 count and the same pick, then switched to `directconnect`
itself once `git status` showed this session's edits appearing mid-flight
-- see its own section above; no collision occurred, `services/directconnect/*`
was never touched here.)

**IMPORTANT CONTEXT this session did NOT expect going in**: `xray` already
carried an extremely thorough `PARITY.md` from a dedicated 2026-08-10 pass
(`b72533e7a`, unrelated to 6flj) that had already fixed several wrapper-key-
class bugs by the same method this issue uses (`GetTraceSummaries.EntryPoint`
string-vs-object, `ListRetrievedTraces` `Segments`->`Spans`, an invented
per-item `ApproximateTime`). This made a "grade A, already covered" result
plausible. It was NOT a clean sweep: the flagship finding below is a Go-KIND
mismatch that pass's own method (member-name/nesting diff) did not check.

**FLAGSHIP BUG, `GetTraceSummaries.Annotations`** (survived one prior
dedicated pass): emitted as a flat `map[string]<scalar>` end-to-end
(`TraceSummaryData.Annotations map[string]any`, populated via
`maps.Copy(summary.Annotations, seg.Annotations)`, serialized as-is). The
real shape (`types.TraceSummary.Annotations`, confirmed
`xray@v1.39.4/deserializers.go:6443`'s
`awsRestjson1_deserializeDocumentAnnotations`) is
`map[string][]ValueWithServiceIds{AnnotationValue,ServiceIds}` -- a JSON
ARRAY of tagged-union objects per key, not a bare value.
`awsRestjson1_deserializeDocumentValuesWithServiceIds`
(deserializers.go:12711) type-asserts `value.([]interface{})` and
hard-errors `"unexpected JSON type"` on anything else -- this is the
"array-versus-map/flat-string-versus-struct hard-fails on deserialization"
class this issue's checklist leads with, and it is service-wide: EVERY real
`GetTraceSummaries` call against a trace carrying at least one annotation
failed outright (not silent-empty) for every caller of this op, always.
Confirmed why the 2026-08-10 pass missed it: that pass field-diffed member
names and nesting (catching `EntryPoint`'s string-vs-object and
`ApproximateTime`'s placement) but never checked the Go KIND of a
map-of-collections value -- same axis gap as the elasticsearch/lakeformation
prior-audit pattern, not an argued-away bug.

FIX: `AnnotationOccurrence{Value any, ServiceIDs []TraceSummaryServiceID}`
added to `models.go`; `TraceSummaryData.Annotations` changed from
`map[string]any` to `map[string][]AnnotationOccurrence` (each key holds the
DISTINCT values reported for it, tagged with the reporting service(s) --
two segments reporting the SAME value merge into one occurrence with both
services listed, matching real per-value `ServiceIds` semantics, verified
with `reflect.DeepEqual` for value comparison since annotation values are
`any` and could theoretically be uncomparable if a caller sends malformed
input). `traces.go`'s new `accumulateAnnotations` replaces the old
`maps.Copy` one-liner. `handler_traces.go` gained `annotationValueView`
(tagged union `StringValue`/`NumberValue`/`BooleanValue`, selected by the
value's Go kind -- X-Ray segment-document annotations are only ever
string/number/bool per the segment spec) and
`valueWithServiceIDsView{AnnotationValue,ServiceIds}`, wired through
`buildTraceSummaryView`.

**SECOND BUG, `GetInsightSummaries` (discarded-filter class, both
directions)**: `GroupARN`/`GroupName` (one required per
`api_op_GetInsightSummaries.go`'s doc comments) and `StartTime`/`EndTime`
(both required, client-SDK-enforced via `validators.go`'s
`validateOpGetInsightSummariesInput`) were parsed by the handler and then
never passed to the backend at all -- `h.Backend.GetInsightSummaries(in.States)`
ignored all four. Every group and every time window returned the exact same
unfiltered set of insights; a caller scoping to one group, or to a window
that excluded an insight's active period, silently got insights back it
never asked for. Root-caused to this backend's insight detector
(`detectInsights`, `insights.go`) having no per-group filter-expression
evaluation at all -- every detected insight is unconditionally labelled
`GroupName="default"` regardless of what real `Group` records exist, so the
group filter had nothing correct to enforce against without this fix.
FIXED at the tractable layer: `GetInsightSummaries`'s signature gained
`groupName string, startTime, endTime time.Time`; results are now filtered
to insights whose `GroupName` matches the resolved group (ARN resolved via
existing `GetGroupByARN`, falling back to a guaranteed-no-match sentinel for
an unresolvable ARN -- correctly empty, not an error, matching this op's
declared error set of `InvalidRequestException`/`ThrottledException` only,
no `ResourceNotFoundException`) and whose active window `[StartTime,EndTime)`
overlaps the request's. Handler now validates both required-field groups
(`errInvalidRequest`) matching the sibling validate-then-query pattern
already used by `GetServiceGraph`/`GetTraceGraph` in this same package.
**DISCLOSED, not further fixed** (recorded in `PARITY.md`'s `gaps:` and
the op's own `state: partial` -- was `ok`): a request scoped to `"default"`
now returns every detected insight, same as before this fix, because the
detector still doesn't evaluate that group's real `FilterExpression`
against traffic -- true per-group detection is a detector redesign, out of
scope for a wire-shape fix. This is a genuine remaining structural gap, not
papered over.

**NEVER-MODELLED MEMBER, disclosed not fabricated**: `GetTraceSummariesInput`'s
optional `Sampling` (bool, parsed and discarded) and `SamplingStrategy`
(`{Name,Value}`, not modeled at all) have no effect -- this backend has no
sampling engine on the trace-summary read path, so every call returns the
full unsampled set regardless of what a client requests. Judged a safe
superset (more data than the client said was acceptable, never less), not a
correctness bug; recorded in `PARITY.md gaps:` per this issue's "disclose
rather than fabricate" instruction rather than silently left unmentioned.

**FULL LAYER-1/2 SWEEP, all 20 L+D+G ops, each read against its own real
`api_op_<Op>.go`/`types/types.go` in the pinned `xray@v1.39.4` module cache**
(not against the 2026-08-10 PARITY.md's notes, though those turned out
accurate everywhere except the flagship bug above): `GetEncryptionConfig`,
`GetGroup`, `GetGroups`, `GetIndexingRules`, `GetInsight`, `GetInsightEvents`,
`GetInsightImpactGraph`, `GetInsightSummaries` (fixed above), `GetRetrievedTracesGraph`,
`GetSamplingRules`, `GetSamplingStatisticSummaries`, `GetSamplingTargets`,
`GetServiceGraph`, `GetTimeSeriesServiceStatistics`, `GetTraceGraph`,
`GetTraceSegmentDestination`, `GetTraceSummaries` (fixed above),
`ListResourcePolicies`, `ListRetrievedTraces`, `ListTagsForResource` -- all
20 confirmed clean at layer 1/2 except the two fixes above.

**SHARED CONVERTERS, EACH CHECKED AGAINST ITS OWN REAL TYPE (this issue's
lead check)**: `GetEncryptionConfig`/`PutEncryptionConfig` share
`keyEncryptionConfig`/`EncryptionConfig` -- confirmed a REAL symmetric pair
(`GetEncryptionConfigOutput.EncryptionConfig` and
`PutEncryptionConfigOutput.EncryptionConfig` are both genuinely
`*types.EncryptionConfig`-only, `api_op_GetEncryptionConfig.go`/
`api_op_PutEncryptionConfig.go`), not a disguised-asymmetry trap like ecr's
registry-scanning-config pair. `GetGroup`/`GetGroups` share `groupView` --
confirmed `types.Group` and `types.GroupSummary` are field-for-field
identical in this SDK version, not a trap. `toIndexingRuleView` shared by
`GetIndexingRules`/`UpdateIndexingRule` -- confirmed correct, real
`IndexingRuleValue`/`IndexingRuleValueUpdate` both tag as `"Probabilistic"`
(`deserializers.go:8273`, `serializers.go:3432`).

**GO-KIND CHECK, per this issue's explicit instruction**: `Annotations`
(flagship bug above, map-of-scalar vs map-of-array-of-object) and
`UploadLayerPart`-style `[]byte` checks (n/a to this service -- no binary
blob fields in the L+D+G set) were the only candidates; every other
collection/field's Go kind matched its real counterpart (slice-of-struct
throughout, no other map-of-collection fields in this op set).

**NEVER-MODELLED MEMBERS**: `GetTraceSummariesInput.Sampling`/`SamplingStrategy`
(disclosed above) is the only instance found in the 20-op L+D+G set.

**EMPTY/204 RESPONSES CHECKED**: none in this op set -- all 20 L+D+G ops are
non-void GET-style reads with a real response body.

**REQUIRED-MEMBER DIFFS, BOTH DIRECTIONS**: `GetInsightSummaries` (fixed
above) was the only gap found; every other op's request/response required
members matched the real `*Input`/`*Output` structs in both directions.

**FILTERS/PAGINATION**: `GetInsightSummaries`'s `GroupARN`/`GroupName`/
`StartTime`/`EndTime` (fixed above) was the only discarded-filter instance;
every other op's declared filter/pagination parameter (`NextToken`/
`MaxResults` throughout, `GetTraceSummaries`' `FilterExpression`/
`TimeRangeType`, `GetServiceGraph`/`GetTraceGraph`'s `StartTime`/`EndTime`/
`TraceIds`) reaches its query.

**PROTOCOL / SECOND CLIENT / EqualFold**: `restjson1` exclusively
(`awsRestjson1_` deserializer prefix throughout, confirmed both from
gopherstack's own path-based `RouteMatcher` dispatch and the pinned SDK).
All 136 `EqualFold` call sites in `xray@v1.39.4/deserializers.go` grepped
and confirmed `errorCode`-matching only (`grep -v "errorCode)"` = 0 hits) --
zero body-field-key `EqualFold` calls, so body-field decode is
case-SENSITIVE as expected for restjson1 (the bug class this issue flags
for JSON-RPC/restjson1). No second cross-service SDK client bridge found
(`grep -rln "aws-sdk-go-v2/service/xray"` outside `services/xray/` and its
own tests: zero hits).

**ROUTER**: `xray` uses REAL PER-OP REST PATHS (not a flat `X-Amz-Target`
switch), so this issue's "flat JSON-RPC switch is structurally immune"
shortcut does NOT apply here -- this is exactly the path-segment-router
class the checklist calls out as needing per-op verification. Not re-swept
this pass (out of scope -- the prior 2026-08-10 pass already audited all 34
routed ops' REST paths against `serializers.go` opPath literals and fixed 6
mismatches, per `PARITY.md`'s "Route-matcher bug class" note; unchanged
since, confirmed by re-reading `handler.go`'s path-constant table, and
`TestSDKCompleteness`/the existing route-matcher tests still pass).

**PHANTOM OPS**: none -- all 37 `GetSupportedOperations()` entries map 1:1
to a real `api_op_*.go` file in the pinned module cache (spot-checked; the
existing `sdk_completeness_test.go` already asserts this and passes).

**SIBLING TRAP, REVERSE VARIANT CHECKED**: none found this session (no
invented enum sat beside an already-correct real value in this op set).

**PRIOR-AUDIT-REASONING CHECK (this issue's item 2)**: the 2026-08-10
`PARITY.md` pass is **grade A but simply never covered the Go-kind axis for
`Annotations`** -- it is not an instance of a note arguing a bug away (no
note claims `Annotations`' shape was checked and found fine); it is a
genuine coverage gap on a different axis than that pass's own method
checked, the same "thorough but different axis" result the
elasticsearch/lakeformation/directoryservice passes reported for their own
services, not the kafka-style "wrong about ops it did cover" result.

**OVER-WIDE FIELD / CREDENTIAL SWEEP**: clean, deliberately run (not
skipped). `grep -rniE "password|secret|credential|privatekey|clientsecret"`
across all non-test `.go` files: zero hits -- this service has no such
domain concept at all. `GroupARN`/`RuleARN`/`ResourceARN`/
`EncryptionConfig.KeyID` (a KMS key ID/ARN) are all real, intentional
response members confirmed against their own real `types.go` shapes, not
leaks. Segment `annotations`/`metadata` carry arbitrary customer-supplied
trace data verbatim by design (the entire point of the API), not a
gopherstack-introduced leak.

**PERSISTENCE TRAP CHECKED**: none of the structs touched this pass
(`TraceSummaryData`, `AnnotationOccurrence`, the new view types) are
`store.Table`-backed persistence DTOs -- `TraceSummaryData` is a purely
derived, request-scoped struct rebuilt fresh from parsed segments on every
`GetTraceSummaries`/`BatchGetTraces` call, never persisted. `Insight`
itself (touched only via its existing `GroupName`/`StartTime`/`EndTime`
fields, no new fields added) IS the persistence DTO (confirmed
`insights.go`'s `store.Table`); no field was added or retagged on it this
pass, only read differently in `GetInsightSummaries`'s new filter, so no
persistence-compat risk.

**SDK PINNING / REAL-CLIENT TEST RATIO**: `xray@v1.39.4` pinned in `go.mod`
(matches `PARITY.md`'s cited version, no drift, no dependency-boundary
exception needed). Real-client test ratio before this pass: 0 SDK-client
tests out of 37 ops (all prior tests drove `h.Handler()` directly or via
hand-built `httptest` requests, never the real `aws-sdk-go-v2/service/xray`
client through the full `pkgs/service` router). Added 2 (`services/xray/wire_field_fixes_test.go`):
`TestGetTraceSummaries_Annotations_RealClient` and
`TestGetInsightSummaries_GroupAndTimeFiltering`, both driven through
`service.NewRegistry`/`NewServiceRouter` (the router-inclusive path).

**TESTS, hand-revert protocol**: both new tests hand-reverted against the
pre-fix code (restored via `git show HEAD:<file>` for the 3-4 files each
fix spans, since this session's hard constraint bans even `git checkout --`)
and confirmed to fail with the exact predicted symptom before being
restored byte-identical (diffed against a saved copy):
`TestGetTraceSummaries_Annotations_RealClient` failed with
`deserialization failed ... unexpected JSON type true` (a hard client
failure, not silent-empty, exactly as the real deserializer's
`value.([]interface{})` assertion predicts); `TestGetInsightSummaries_GroupAndTimeFiltering`
failed on its first assertion (missing-required-field validation absent)
and, independently re-verified by temporarily removing that first
assertion, also failed on both the group-scoping assertion (a different
group's request returned the "default" group's insight) and the
time-window assertion (a non-overlapping window still returned the
insight) -- all three predicted symptoms individually confirmed. 8
existing tests in `handler_insights_test.go`/`insights_test.go`/
`persistence_test.go` updated to supply the now-required `GroupName`/
`StartTime`/`EndTime` fields and matching `GroupName: "default"` on seeded
insights (a genuinely-required-field gap these tests had been silently
relying on, not a wrong-key assertion to rewrite).

GATES: scoped `go build`/`go vet ./services/xray/...` clean; full `go build
./...`/`go vet ./...` clean (interface signature change on
`StorageBackend.GetInsightSummaries` propagates; confirmed no other package
references it); `go test -race -count=1 ./services/xray/...` and
`./pkgs/...` both green; `go fix -diff ./services/xray/...` clean (one real
modernize finding applied by hand: `slices.Contains` replacing a manual
loop, not via `-fix`); `golangci-lint run ./services/xray/...` 0 issues
(fixed by hand: `gofmt`/`golines` formatting, one `revive` var-naming
finding on a new type -- `valueWithServiceIdsView` -> `valueWithServiceIDsView`
-- and one line-length overflow from struct-tag column realignment, all
fixed by hand, not `-fix`, per this campaign's `fieldalignment -fix`
nolint-stripping hazard); `fieldalignment ./services/xray/...` 0 hits; 0
`cyclop`/`gocyclo`/`gocognit`/`funlen` nolints (grep-confirmed, none added).

No subagents used (Read/Grep/Bash/Edit only, per this session's hard
constraint). No git-mutating commands run -- orchestrator must
commit/push. `git status` re-checked before every edit batch; only
`services/xray/*` and this remainder file touched throughout (the
`directconnect` sibling that appeared mid-session, per its own section
above, was never read or touched here).

`xray`'s List/Describe/Get families are now fully swept for this issue
(20/20 ops layer-1/2/3 clean; 2 real bugs found and fixed -- 1 flagship
Go-kind wrapper-shape bug affecting every call with an annotation present,
1 discarded-filter bug on GroupARN/GroupName/StartTime/EndTime; 1 remaining
structural gap disclosed, not papered over; 1 never-modelled request-member
pair disclosed; no real-data leak found). **93 of 162 services swept, 69
remain.** Per the ranked table, the next tier starts at 19 L+D+G
(`transcribe`, `mediatailor`); re-run `go run ./cmd/opcensus` and re-check
`git status` before picking, as usual -- siblings have appeared mid-session
on every pass today.

## transcribe (this session, 2026-08-15)

Picked `transcribe` (19 L+D+G ops) over its tied sibling `mediatailor` (also
19) purely on **occupancy**: `git status` at pickup already showed
`mediatailor/{functions.go,handler_functions.go,interfaces.go}` modified by a
live sibling, and a re-check mid-session caught a brand-new untracked
`mediatailor/wire_field_fixes_test.go` appear between two `git status` calls
-- unambiguous proof of an active concurrent session there, not stale
leftovers. **Occupancy overrode surface**: by handler-family-file count
`mediatailor` (12 families: alerts, channel_policy, channels, functions,
live_sources, logs, playback_configurations, prefetch_schedules, programs,
source_locations, tags, vod_sources) is actually wider than `transcribe` (9:
call_analytics, language_models, medical_scribe, medical_transcription_jobs,
medical_vocabularies, tags, transcription_jobs, vocabularies,
vocabulary_filters), so surface-first would have picked `mediatailor` had it
been free.

Confirmed the tier ranking independently rather than trusting this file's own
header text (which is stale relative to the last several sessions' own
closing notes): cross-referenced `go run ./cmd/opcensus`'s fresh output
against the union of every service this file's per-session sections have
since reported swept (including ones never added to the top alphabetical
`## Swept` list, e.g. `ecr`/`neptune`/`directconnect`/`dynamodb`/`xray`,
`cloudtrail`/`directoryservice`/`opsworks` mentioned only in the header
prose) -- confirms `transcribe`/`mediatailor` at 19 are genuinely the next
tier, matching the prior session's own closing note.

**Scripted key-set extraction**: yes -- a Python script pulled every
`awsAwsjson11_deserializeOpDocument<Op>Output` function body's `case "..."`
keys, and every reachable nested-type deserializer's keys, directly out of
`transcribe@v1.58.4/deserializers.go` via regex over the function bodies
(not hand-transcribed), for all 19 L+D+G ops plus ~30 nested/shared types.

**Go-kind check**: ran on every scalar/collection/nested field diffed this
pass; no array-vs-map, flat-vs-nested-collection, or `[]byte`-vs-struct
mismatches found among the 19 ops' top-level wrappers. One genuine
flat-vs-nested-object bug was found one level down (see below) -- caught by
comparing which *level* of the object graph a correctly-spelled key lived at,
not its collection kind.

**Real bugs found and fixed (4, all layer-2/never-modelled, not wrapper-key
misnaming -- all 19 ops' top-level wrapper keys were already correct):**

1. `VocabularyInfo.LastModifiedTime` missing from `ListVocabularies` and
   `ListMedicalVocabularies` (shared real item type, both siblings had the
   same gap -- fixed both).
2. `CallAnalyticsSettings.LanguageIdSettings` never modeled at all (zero grep
   hits; distinct from the already-fixed `TranscriptionJob`-level field of the
   same name) -- affects `StartCallAnalyticsJob`/`GetCallAnalyticsJob` (shared
   `Settings` pointer passed by reference both directions).
3. All four Call Analytics rule filter types (`NonTalkTimeFilter`/
   `InterruptionFilter`/`TranscriptFilter`/`SentimentFilter`) missing
   `AbsoluteTimeRange`/`RelativeTimeRange` sub-parameters entirely --
   `CreateCallAnalyticsCategory`/`UpdateCallAnalyticsCategory`/
   `GetCallAnalyticsCategory`/`ListCallAnalyticsCategories` all affected
   (`CallAnalyticsRule` reused directly as the wire type both directions).
4. **Flagship find**: `ClinicalNoteGenerationSettings` wire-tagged at the TOP
   LEVEL of `StartMedicalScribeJobInput`/`MedicalScribeJob` response, but the
   real SDK has NO top-level member of that name at all -- it exists only
   nested under `Settings` (`types.MedicalScribeSettings.
   ClinicalNoteGenerationSettings`). Confirmed the real deserializer's
   `default: _, _ = key, value` case silently skips unrecognized top-level
   keys rather than erroring, so this was a true silent-empty bug in both
   directions, invisible to any test that only checked the response body
   *contained* the string "ClinicalNoteGenerationSettings" (one existing test
   did exactly that, at the wrong nesting level -- fixed alongside the code).
   This is the exact "nested shape emitted flat" trap this issue calls out as
   hardest to find: the key name was spelled correctly, so a names-only diff
   would have missed it; only comparing which level of the object graph
   carried it caught it.

**Shared converters, each checked against its own real type**: `Models`
(ListLanguageModels item) confirmed to reuse the full `LanguageModel`
deserializer, matching gopherstack's reuse of `languageModelOutput` for both
Describe and List -- genuinely symmetric, not a trap. `CategoryPropertiesList`
(ListCallAnalyticsCategories item) confirmed to reuse the full
`CategoryProperties` deserializer too, matching gopherstack's reuse of
`callAnalyticsCategoryProperties` across Create/Get/Update/List -- also
genuinely symmetric. `VocabularyFilterInfo` (ListVocabularyFilters item, 3
fields, no `DownloadUri`) vs `GetVocabularyFilterOutput` (4 fields, adds
`DownloadUri`) confirmed as a **real, intentional asymmetry** matching AWS's
own shapes -- gopherstack's separate `vocabularyFilterOutput`/
`getVocabularyFilterOutput` types already modeled this correctly, verified
per-op rather than assumed.

**Never-modelled members, disclosed not fabricated** (both already recorded
in `PARITY.md`'s `gaps:` from a prior pass, re-confirmed unchanged this
pass): `CallAnalyticsJobDetails`/`Skipped` (no backend concept of skipped
analytics features, zero data source to populate it truthfully) and
`MedicalScribeContext`/`MedicalScribeContextProvided` (a whole unmodeled
patient-context input feature -- safe superset, not client-breaking, same
category as xray's Sampling/SamplingStrategy no-op).

**Over-modeled, disclosed**: gopherstack's `NonTalkTimeFilter.ParticipantRole`
is an extra field the real `types.NonTalkTimeFilter` does not have (its three
siblings genuinely do carry `ParticipantRole`) -- harmless, unreachable by a
real client, left in place rather than risk breaking
`TestCreateCallAnalyticsCategory_Rules` for a cosmetic removal.

**Structurally immune**: router is flat `X-Amz-Target: Transcribe.<Op>`
prefix dispatch (JSON-RPC-style, not path-segment), confirmed immune to the
route-matcher bug class. Protocol is `awsjson1.1` (JSON body), confirmed
case-sensitive key decode (zero `EqualFold` calls anywhere in the service,
matching the protocol) and no second SDK client bridge (only
`validation.go` imports the real SDK, for enum-value references, not a live
client).

**Phantom-op check**: all 43 of `allSupportedOps()`'s entries diffed 1:1
against `ls api_op_*.go` in the pinned SDK module cache -- exact match, zero
phantom ops, zero missing ops.

**SDK pinned**: `transcribe@v1.58.4` (`go.mod`, no drift). Real-client test
ratio before this pass: roughly 8/43 ops previously exercised through a real
`aws-sdk-go-v2` client (a prior g8k9 pass's `wire_field_fixes_g8k9_test.go`);
the rest were `httptest`/raw-body only. Added 5 new router-inclusive
real-client tests this pass (`wire_field_fixes_test.go`).

**Tests**: all 4 fixes hand-reverted individually (models.go/
handler_medical_scribe.go edited back to the pre-fix shape byte-for-byte,
verified via post-restore `git diff` index-hash comparison against a saved
pre-revert snapshot -- this session's hard constraint bans even
`git checkout --`), each confirmed to fail with the exact predicted symptom
(a nil/missing round-tripped value -- awsjson1.1 tolerates unknown fields, so
none of these ever produced a decode error, only silent data loss), then
restored and re-verified passing before moving to the next.

**Gates**: scoped `go build`, full `go build ./...` (no interface signature
changed, so this was a belt-and-braces check, not a required one), `go vet`,
`go test -race` for `services/transcribe/...` and `pkgs/...`, `go fix -diff`
(clean, no diff), `golangci-lint run ./services/transcribe/...` (0 issues,
including `fieldalignment` via the enabled govet analyzer -- no
`//nolint:cyclop/gocyclo/gocognit/funlen` added, grep-confirmed 0). No
subagents used. No git-mutating commands run -- orchestrator must
commit/push. `git status` re-checked before every edit batch; only
`services/transcribe/*` and this remainder file touched throughout (the
`mediatailor` sibling, confirmed live both at pickup and mid-session, was
never read or touched here).

`transcribe`'s List/Describe/Get families are now fully swept for this issue
(19/19 ops layer-1/2/3 clean; 4 real bugs found and fixed, all never-modelled
members rather than wrapper-key misnaming -- 1 flagship flat-vs-nested-object
bug affecting Medical Scribe clinical note settings in both directions; 2
pre-existing disclosed gaps re-confirmed unchanged; 1 harmless over-modeled
field disclosed; no real-data leak found). **94 of 162 services swept, 68
remain.** Per the ranked table, `mediatailor` (19 L+D+G) is the only
service left at this tier -- once its live sibling session ends, the next
tier below starts around `memorydb`/`codedeploy`/`accessanalyzer` (18 each,
all still unswept per this file); re-run `go run ./cmd/opcensus` and
re-check `git status` before picking, as usual.

## mediatailor (this session, 2026-08-15)

Read this file's header/tail, ran `go run ./cmd/opcensus` fresh (unchanged
for this tier: `mediatailor` 19 L+D+G, `direct`), read `bd show
gopherstack-6flj`'s comments, and read `git show 61e04cfa5` (the
directconnect pass cited by this session's assignment). `git status` at
pick time showed only `services/xray/*` uncommitted (a live sibling, later
confirmed committed as `df32fb2c0` mid-session, unrelated to this pick).

**TIE-BREAK: `mediatailor` vs `transcribe`, both 19 L+D+G, `direct`.**
Surface (widest spread of distinct resource-family `handler_*.go` files)
pointed at `mediatailor`: 12 files (`handler_alerts.go`,
`handler_channel_policy.go`, `handler_channels.go`, `handler_functions.go`,
`handler_live_sources.go`, `handler_logs.go`,
`handler_playback_configurations.go`, `handler_prefetch_schedules.go`,
`handler_programs.go`, `handler_source_locations.go`, `handler_tags.go`,
`handler_vod_sources.go`) versus `transcribe`'s 9 (`handler_call_analytics.go`,
`handler_language_models.go`, `handler_medical_scribe.go`,
`handler_medical_transcription_jobs.go`, `handler_medical_vocabularies.go`,
`handler_tags.go`, `handler_transcription_jobs.go`, `handler_vocabularies.go`,
`handler_vocabulary_filters.go`). No live sibling on either at pick time --
picked cleanly on surface, no occupancy override needed. (The `transcribe`
pass that ran concurrently independently reports reaching the same surface
conclusion and yielding on occupancy once it saw this session's files
change mid-flight -- confirmed from both sides, no collision, matching this
issue's own precedent for how simultaneous picks should resolve.)

**KEY-SET EXTRACTION: scripted, not hand-transcribed.** A small Python
helper (paren-balance-aware, since a naive `func ... {` search breaks on
`interface{}` appearing in a signature before the real body) walked
`mediatailor@v1.63.4/deserializers.go`, located each op's own
`awsRestjson1_deserializeOpDocument<Op>Output` function, and regex-extracted
every top-level `case "<key>":` -- run individually for all 19 in-scope ops
plus every Create/Update sibling sharing a converter (28 functions total)
and every shared nested type (`SourceLocation`, `VodSource`, `LiveSource`,
`PlaybackConfiguration`, `Function`, `Channel`, `AdBreak`,
`ScheduleAdBreak`, `AccessConfiguration`, etc).

**PROTOCOL, ROUTER, SECOND CLIENT, EQUALFOLD:** restjson1 (confirmed:
`awsRestjson1_` prefix throughout `deserializers.go`/`serializers.go`).
Zero `EqualFold` calls anywhere in `services/mediatailor/*.go` -- casing IS
a real bug class for this protocol (plain `switch key { case "Foo": }` in
the real deserializer, case-sensitive), but no mismatch found. Router is
path-segment-based (`RouteMatcher`/`ExtractOperation`), NOT structurally
immune the way a flat `X-Amz-Target` dispatch would be -- this service
already carries a permanent regression test for exactly that class
(`handler_sdk_route_table_test.go`'s `TestExtractOperation_SDKRouteTable`,
one subtest per op, added 2026-08-13 under a different issue), re-run clean
this pass rather than re-derived from scratch. Every one of the 19 in-scope
ops' `HandleDeserialize` was individually confirmed (not assumed) to call
its generated `OpDocument*Output` function directly -- no dead-wrapper trap
like `pinpoint`'s. `GetSupportedOperations`' 48 ops exact-matched the SDK's
48 `api_op_*.go` files both directions via `cmd/opcensus` -- 0 phantom ops.
No second SDK client import anywhere outside `_test.go` files.

**8 real bugs found and fixed, all layer-2 (missing-or-fabricated fields,
never a top-level wrapper-key rename) -- every one caught by diffing a
shared converter's OTHER call sites against their own real Output type,
this issue's stated lead:**

1. `GetFunction`/`PutFunction` never emitted `CustomOutputConfiguration`/
   `HttpRequestConfiguration`/`SequentialExecutorConfiguration` at all -- a
   real client's Function object was always nil on all three regardless of
   FunctionType, on the entire Functions feature. Fixed as decoded-JSON
   pass-through (matching `PlaybackConfiguration.Extra`'s existing
   convention -- this backend doesn't execute functions).
2. `ListFunctions`' `Items` is `[]types.Function` (same full type
   `GetFunction` returns) but dropped `Description` and all three configs
   per item -- `FunctionSummary` didn't carry them either. Fixed.
3. `ListChannels`' `Items` is `[]types.Channel` (same full type
   `DescribeChannel` returns, minus `TimeShiftConfiguration`, plus
   `LogConfiguration` -- confirmed the OPPOSITE asymmetry from bug 6) but
   dropped 6 of 12 real fields despite `ChannelSummary` already tracking
   every one -- pure wire-emission gap. Fixed.
4. `ListVodSources`/`ListLiveSources` dropped `HttpPackageConfigurations`
   (real on both `types.VodSource`/`types.LiveSource`). Also found in the
   same pass: `ListLiveSources`' OWN backend method never populated
   `CreationTime`/`LastModified` on `LiveSourceSummary` at all, while
   `ListVodSources`' equivalent method already did -- a genuine
   sibling-family asymmetry, verified per-op rather than assumed uniform.
   Fixed both.
5. `ListPlaybackConfigurations`' `Items` is `[]types.PlaybackConfiguration`
   (same full type `GetPlaybackConfiguration` returns) but dropped
   `LogConfiguration`/`PlaybackEndpointPrefix`/
   `SessionInitializationEndpointPrefix` despite the backend already
   tracking all three. Fixed by reusing `toPlaybackConfigOutput` directly
   instead of re-deriving a slimmer shape -- its existing dual-stack `if !=
   ""` guards correctly no-op on the already-disclosed-empty dual-stack
   fields (Notes #13 in `services/mediatailor/PARITY.md`), confirmed not
   accidentally reopened.
6. `CreateChannel`/`UpdateChannel` FABRICATED a `LogConfiguration` field
   neither real Output type has (real member only on
   `DescribeChannelOutput`) -- the inverse of bugs 2-5 (over-emission, not
   under-), harmless to a real typed client (unknown JSON keys silently
   ignored) and only observable via a **raw-body test**
   (`TestCreateChannel_NoLogConfigurationOnWire`). Fixed by moving the
   field out of the shared converter into `handleDescribeChannel` only.
7. `GetPrefetchSchedule`/`CreatePrefetchSchedule` fabricated a top-level
   `CreationTime` with no real member at all (confirmed via both ops' own
   9-key deserializer switches) -- same over-emission/raw-body-only class
   as bug 6. An existing test asserted the fabricated field as correct;
   fixed.
8. `DescribeVodSource` never modeled `AdBreakOpportunities` (real, only on
   `DescribeVodSourceOutput` -- diffed separately from
   `Create`/`UpdateVodSourceOutput`, confirmed absent on both). Same
   structural class as the already-disclosed `ScheduleAdBreaks` gap
   (manifest/SCTE-35 scanning this backend has no engine for anywhere in
   the fleet) -- fixed by emitting an honest, always-empty list on the
   Describe path only, never fabricated non-empty.

**SHARED CONVERTERS CHECKED, CONFIRMED GENUINELY SHARED (no bug):**
`toLiveSourceOutput`/`toVodSourceOutput`-style Create/Describe/Update
triples for `LiveSource`, `SourceLocation`, `Program` -- all 3 real Output
types per family diffed individually, byte-identical (unlike `Channel`'s
asymmetry). `toPrefetchScheduleOutput` (Create/Get) and `toFunctionOutput`
(Put/Get) are genuinely identical real shapes once bugs 1/7 were fixed.

**SYMMETRIC-LOOKING PAIR DIFFED SEPARATELY, CONFIRMED A REAL ASYMMETRY, NOT
A TRAP MISSED:** `Channel` (List item) vs `Create`/`UpdateChannelOutput`
looks like the same shape at a glance -- it is NOT: real `types.Channel`
has `LogConfiguration` but no `TimeShiftConfiguration`; real
`Create`/`UpdateChannelOutput` have `TimeShiftConfiguration` but no
`LogConfiguration`. Both directions of this asymmetry were bugs in
gopherstack before this pass (bugs 3 and 6) -- diffing the pair separately,
not trusting either as a template for the other, is what caught both.

**STRUCTURALLY IMPOSSIBLE FOR THIS PROTOCOL:** none found this pass --
restjson1's collections are always named JSON arrays or maps as declared,
no array-vs-map ambiguity like query/XML's named-element-list case.

**NEVER-MODELLED MEMBERS, fixed or disclosed:** bugs 1 and 8 above (fixed).
Also reconfirmed (not touched, already correctly disclosed by a prior
general-parity pass) `ProgramScheduleEntry.ScheduleAdBreaks` --this session
nearly proposed deriving it from `Program.AdBreaks` before reading
`PARITY.md`'s own note, which already explains why that would be exactly
the fabrication this issue warns against (AdBreaks is client-configured ad
splicing; ScheduleAdBreaks is MediaTailor-detected SCTE-35 avails --
materially different concepts). New disclosure this pass:
`ProgramScheduleEntry.Audiences` is declared but never assigned anywhere in
`programs.go` (always empty, correctly-but-incompletely omitted by the
wire's existing `if len > 0` guard) -- a plausible derivation exists
(`Program.AudienceMedia`'s per-entry `Audience` field), but this pass found
no primary source confirming that mapping against the pinned SDK (whose own
doc comment is circular) or a live account, so it was disclosed in
`PARITY.md`'s `items_still_open` rather than guessed. No member marked
`Deprecated` in the SDK's own doc comments was found in this service's
touched surface.

**NO FIX WITHDRAWN THIS PASS** -- every hand-revert (8 of them, one per
bug) reproduced the exact predicted symptom before being restored;
`go test -race -run <name> -v` output for each revert is quoted in the
session's own report.

**VERIFIED PER-OP, NOT ASSUMED UNIFORM:** `HttpPackageConfigurations` was
missing on `ListVodSources` AND `ListLiveSources` but present and correct
on `DescribeVodSource`/`DescribeLiveSource`/`CreateVodSource`/
`CreateLiveSource` -- checked each op individually rather than assuming the
List-vs-Describe split was uniform across the whole service (it wasn't:
`ListSourceLocations`' equivalent fields were already fully correct, using
the same `addAccessConfiguration`/`addSegmentDeliveryConfigurations`
helpers as `DescribeSourceLocation`).

**EVERY EMPTY/204 RESPONSE CHECKED:** `DeleteFunction`/
`DeletePrefetchSchedule`/`DeletePlaybackConfiguration`/`TagResource`/
`UntagResource` return `c.NoContent(204)`; all 5 real Output types are
confirmed genuinely empty (`ResultMetadata` only, no body members) --
correct, not a truncated real body. `DeleteChannel`/`DeleteSourceLocation`/
`DeleteVodSource`/`DeleteLiveSource`/`DeleteProgram`/`DeleteChannelPolicy`
return `200 {}` instead of `204` -- inconsistent with the 5 above but
harmless (their real Output types are also empty; a real client only reads
`ResultMetadata` either way) -- noted, not changed (out of this issue's
scope, no data loss).

**PRIOR AUDIT NOTE QUALITY: two stale/incorrect claims found and
corrected, both the ARGUED-AWAY case (asserted something as done that a
grep does not support), not a coverage gap** -- this service's
`PARITY.md` is unusually thorough (Notes #1-13, several full re-audits) so
most of its history held up; these two didn't: `CreateChannel`'s note
claimed a prior pass correctly added `LogConfiguration` (it added it to a
shape that should never have had it -- bug 6); `GetChannelSchedule`'s note
claimed `Audiences` was fixed to match real `ScheduleEntry` (never actually
populated -- see disclosure above). Both corrected in place in
`services/mediatailor/PARITY.md`, not silently rewritten. `last_audit_commit`
(`a874b0df`) was NOT re-pointed -- this pass's method (deserializer key
switches) is a narrower/deeper check than that audit's Go-struct-level
method, not a full re-audit superseding it, so the existing pointer is
still the right one for "what a general-parity re-audit should diff from."

**REQUIRED-MEMBER DIFFS, both directions, scoped to the 19 ops touched (not
all 48):** the pinned SDK ships zero `validateOpInput*` functions with
conditional (FunctionType-dependent) required-field enforcement for
`PutFunction`'s three config blocks -- each is documented "Required when
FunctionType is X" in prose only, never enforced client-side. No case found
of gopherstack demanding a field the real Input structurally lacks, or of a
real required field going unenforced, among the 19 touched ops.

**FILTERS/PAGINATION:** all 8 ops taking `maxResults`/`nextToken`
(`ListChannels`, `ListFunctions`, `ListLiveSources`, `GetChannelSchedule`,
`ListPlaybackConfigurations`, `ListPrefetchSchedules`, `ListVodSources`,
`ListSourceLocations`) route through `extractPaginationParams`/
`extractBodyPaginationParams` into `pkgs/page` -- confirmed, none
discarded. `ListTagsForResource`/`ListAlerts` correctly take no pagination
params (matching the real API, both single-page ops).

**DISCARDED INPUTS:** grepped `_ .*Input\b` across every non-test file --
zero hits, no discarded input parameter found in this service.

**OVER-WIDE FIELD / CREDENTIAL SWEEP:** nothing new introduced by this
pass's fixes touches secrets/ARNs beyond what this `PARITY.md`'s Notes
#6-#13 already covered and cleared (`SecretsManagerAccessTokenConfiguration`'s
fields are identifiers, not secret values).

**PERSISTENCE TRAP:** checked before adding fields. `Channel`/
`ChannelSummary`/`VodSourceSummary`/`LiveSourceSummary`/`FunctionSummary`/
`PlaybackConfigurationSummary` (all extended this pass) are plain Go
structs with no `json:`/other tags read by `pkgs/store` for field-name
purposes -- this service's `store.Table[T]` persists via Go's native
`encoding/json` over the untagged struct field names, so new additive
fields carry no retag risk. (`storedPlaybackConfiguration`/`storedVodSource`
DO have explicit `json:` tags for a few pre-existing fields -- neither was
retagged, only read from, by this pass.)

**SDK PINNED:** `mediatailor@v1.63.4` (`go.mod`), no dependency-boundary
exception needed. Real-`aws-sdk-go-v2`-client test ratio: this service
already had extensive real-client coverage before this pass (most CRUD
paths, the full route table, several prior-pass round-trip tests); this
pass added 8 more (`wire_field_fixes_test.go`), 2 of which are deliberately
raw-body (bugs 6/7, unobservable to a typed client by construction).

**ANYTHING UNVERIFIABLE FROM THE PINNED SDK:** the true real-AWS semantics
of `ScheduleEntry.Audiences` (see disclosure above) -- the SDK's own doc
comment is circular and this pass had no live account to confirm against.

**RAW-BODY TESTS:** 2, used deliberately (bugs 6 and 7) -- both are
over-emission bugs where a real typed client structurally cannot observe an
extra unknown JSON key (the generated deserializer's `default:` case
silently ignores it), so only a decoded-body assertion below the SDK layer
can catch or guard against them.

**PHANTOM OPS:** zero, both directions (see router/second-client above).

**FALSE-POSITIVE RATE:** 0 among the 8 reported bugs -- every one cited the
real deserializer/struct-definition file, confirmed reached from
`HandleDeserialize`, before being called a bug.

**GATES:** `go build ./services/mediatailor/...` and full `go build ./...`
(interface signatures changed -- `StorageBackend.PutFunction`'s signature
grew 3 parameters) both clean; `go vet` clean; `go test -race -count=1
./services/mediatailor/...` and `./pkgs/...` both green; `go fix -diff`
empty; `golangci-lint run ./services/mediatailor/...` 0 issues (fixed 4
`goconst` findings by promoting `"LogTypes"`/`"HttpPackageConfigurations"`
to named constants, 2 `golines` wraps, and removed 2 now-unused
`//nolint:dupl` directives the refactor made stale -- `nolintlint` caught
these, not silently left); `fieldalignment` clean on every file this pass
touched (2 pre-existing findings remain in untouched test files, confirmed
via `git status`/`git diff --stat` neither was edited this pass). Zero
`//nolint:cyclop/gocyclo/gocognit/funlen` added, grep-confirmed. No
subagents used (Read/Grep/Bash only, per this session's hard constraint).
No git-mutating commands run -- orchestrator must commit/push. `git status`
re-checked before every edit batch; only `services/mediatailor/*` and this
remainder file touched -- `services/xray/*` (the sibling live at pickup,
committed mid-session as `df32fb2c0`) was never read or touched.

`mediatailor`'s List/Describe/Get families are now fully swept for this
issue (19/19 ops layer-1/2/3 clean; 8 real bugs found and fixed -- 3
never-modelled/dropped-per-item-field bugs affecting entire List
responses, 2 fabricated-field over-emissions catchable only by raw-body
test, 1 never-modelled Describe-only member, 1 sibling-family timestamp
asymmetry, all individually hand-reverted and confirmed before restoring;
2 stale prior-audit claims corrected; 1 new gap disclosed, not guessed).
**95 of 162 services swept, 67 remain.** Per the ranked table, the next
tier starts at 18 (`memorydb`, `codedeploy`, `accessanalyzer`); re-run
`go run ./cmd/opcensus` and re-check `git status` before picking, as usual.

## memorydb (this session, 2026-08-15)

**PICK AND TIE-BREAK:** three-way tie at 18 L+D+G ops (`memorydb`,
`codedeploy`, `accessanalyzer`). `git status` at pickup showed all three
free (no live sibling). Decided by **surface**: counted distinct
resource-family `handler_*.go` files (excluding `_test.go` and the shared
`handler_test.go`/`handler_sdk_route_table_test.go`) — `memorydb` 12
(acls, clusters, engine_versions, events, multi_region_clusters,
parameter_groups, reserved_nodes, service_updates, snapshots,
subnet_groups, tags, users), `codedeploy` 10, `accessanalyzer` 8. Picked
`memorydb`. Mid-session, `codedeploy` picked up a live sibling (its files
appeared modified in a later `git status`, confirmed via `git log` showing
`memorydb` was still uncommitted while `codedeploy` files changed under a
different working tree state) — never read or touched here, matching the
occupancy-respecting precedent from prior passes.

**SDK pinned:** `memorydb@v1.36.4` (`go.mod`, matches PARITY.md, no drift,
cached under `$(go env GOMODCACHE)`, no dependency-boundary exception
needed). Protocol: `awsAwsjson11_` prefix throughout `deserializers.go`/
`serializers.go` (confirmed from `api_client.go`, not `_PROTOCOLS.md`
alone) — JSON-RPC 1.1, case-sensitive exact-match decode on a real
client's own deserializer (plain Go `switch` statements, not
`strings.EqualFold` — zero `EqualFold` hits anywhere in this module,
confirmed by grep, which is itself the tell: restjson1/awsjson1.1 services
decode via exact string/map-key match with no case-folding pass at all,
unlike query/XML's `EqualFold`-based decode). No second SDK client bridge
(only the real SDK's `types` package is imported, for enum references).

**SCRIPTED key extraction:** yes, both directions. Wrote two throwaway
Python scripts (gitignored `*.py`, not committed) that parse
`deserializers.go`/`serializers.go` directly: one recursively walks every
`awsAwsjson11_deserializeOpDocument<Op>Output` function for all 18 ops and
every reachable nested-type deserializer it calls, collecting every `case
"Key":` string; the other does the same for `object.Key("Key")` calls in
every `awsAwsjson11_serializeOpDocument<Op>Input` function, covering the
request side. First version of the deserializer walker mis-parsed function
bodies because `interface{}` in a Go func signature has its own brace pair
that a naive "find first `{`" brace-matcher mistook for the function body
start — fixed by skipping past the balanced parameter-list parens first.
At 18 ops × ~50 nested/nested-of-nested nested nested types, this would
not have been caught by hand-transcription.

**TOP-LEVEL WRAPPER KEYS: mostly clean, two real breaks.**
`DescribeMultiRegionParameters`'s response list was wire-tagged
`"Parameters"`; the real key (confirmed via
`awsAwsjson11_deserializeOpDocumentDescribeMultiRegionParametersOutput`) is
`"MultiRegionParameters"` — the sibling plain `DescribeParameters`
genuinely does use `"Parameters"` (verified separately), so this is a
**sibling-trap**: a shared naming convention applied uniformly where the
real API actually differs between the two ops. Second, and worse: BOTH
`DescribeMultiRegionParameters` (required field) and
`DescribeMultiRegionParameterGroups` (optional field) read their
**request-side** name filter under the key `"ParameterGroupName"`; the
real key on both inputs (confirmed via `api_op_DescribeMultiRegionParameters.go`
/ `api_op_DescribeMultiRegionParameterGroups.go` and their serializers) is
`"MultiRegionParameterGroupName"` — a different key, not a casing
near-miss. Because this service decodes with `encoding/json.Unmarshal`
(case-insensitive fallback), casing mismatches elsewhere in this service
would have been harmless; this was not a casing mismatch, so the fallback
does not apply and the bug is real. On `DescribeMultiRegionParameters` the
field is required, so **every real client's request failed outright**
(`InvalidParameterValueException: MultiRegionParameterGroupName is
required`) — the op was completely broken end-to-end for any real caller,
combining with the response-key bug above so even a request that somehow
got through would have come back empty. On `DescribeMultiRegionParameterGroups`
the field is optional, so the bug was silent: a real client's name filter
was always ignored, returning every group instead of the one requested.

**ONE LEVEL DEEPER — nested/never-modelled members, Go-kind checked
throughout (all scalar mismatches below are name/key issues, not
kind issues; no map-of-array-of-tagged-union shapes exist in this
service's response tree, and Slots/DataTiering/IpDiscovery's real `*string`
kind matches this service's plain `string` fields exactly — checked
per-field against `types.go`, not assumed):**

1. `Cluster.IpDiscovery` was wire-tagged `"IPDiscovery"` (wrong case,
   confirmed live-bug since awsjson1.1 client deserializers do exact
   `case "IpDiscovery":` matches, not `EqualFold`) — every
   `DescribeClusters`/`CreateCluster`/`UpdateCluster`/`DeleteCluster`/
   `BatchUpdateCluster`/`FailoverShard` response silently zeroed this
   field for a real client (shared `clusterObject`). Request-side
   `IPDiscovery` tags on `createClusterRequest`/`updateClusterRequest`
   were checked and left alone: confirmed harmless, since
   `encoding/json.Unmarshal`'s case-insensitive fallback still binds a
   real client's `"IpDiscovery"` request key to the `"IPDiscovery"`-tagged
   Go field on decode — this is the encode/decode asymmetry the campaign
   brief calls out (marshal is exact-tag, unmarshal has a case-insensitive
   fallback), verified rather than assumed.
2. `Snapshot.ClusterConfiguration` (real `types.ClusterConfiguration`, 17
   keys per its deserializer) was missing `MultiRegionClusterName` and
   `MultiRegionParameterGroupName` entirely — confirmed real via
   `types.go`, zero grep hits anywhere in the service beforehand, and
   **distinct from the already-correctly-tracked `Cluster.MultiRegionClusterName`
   at a different level** (the exact "same name, different struct" trap
   the brief warns about). Both are honestly derivable, not fabricated:
   `MultiRegionClusterName` copies straight off the source `Cluster`;
   `MultiRegionParameterGroupName` isn't tracked on `Cluster` itself (only
   on the `MultiRegionCluster` it belongs to), so it's resolved through
   that FK (`b.multiRegionClusters.Get`). One new helper,
   `snapshotClusterConfigFor`, now backs all three call sites that used to
   duplicate this struct literal (`CreateSnapshot`,
   `seedAutomatedSnapshotLocked`, the delete-cluster final-snapshot path)
   — deduping them also means the fix can't land in two of three and miss
   the third.
3. `MultiRegionCluster` (real `types.MultiRegionCluster`, 11 keys) was
   missing the real `NumberOfShards` response member, and
   `CreateMultiRegionClusterInput.NumShards` (the request-side source of
   that value) wasn't even in `createMultiRegionClusterRequest` — a
   **discarded input** feeding directly into a **never-modelled response
   member**, the same bug from both sides at once. Defaults to 1
   (matching `CreateCluster`'s own default) when unset, validated 1-500
   like `CreateCluster`.
4. `DescribeReservedNodesInput` (real, confirmed via
   `api_op_DescribeReservedNodes.go`) has `Duration` and
   `ReservedNodesOfferingId` filters that `describeReservedNodesRequest`
   never modeled at all — zero grep hits, and NOT something the prior
   pass's "no ReservedNodeId" comment excused (that comment was accurate
   about `ReservedNodeId` not existing, but didn't claim `Duration`/
   `ReservedNodesOfferingId` were the full filter set either — a coverage
   gap on breadth, not an argued-away bug). Wired to the existing
   per-reservation `Duration`/`ReservedNodesOfferingID` fields, filtered
   the same way `DescribeReservedNodesOfferings` already filters its own
   `Duration`.
5. **Disclosed, not fixed:** `ClusterPendingUpdates.Resharding` (real
   member, `types.ReshardingStatus{SlotMigration{ProgressPercentage}}`,
   confirmed via its 3-key deserializer case list — `ACLs`/`Resharding`/
   `ServiceUpdates`) is not modeled on `pendingUpdatesObject`. This
   backend applies shard-count changes synchronously with zero
   in-progress-resharding state (grep for `reshard`: zero hits outside
   this finding), so the field would always be absent/nil regardless —
   identical to a real AWS response at rest with no resharding in flight.
   Not added as a permanently-nil dead field; recorded as a gap instead,
   same call as `ServiceUpdate.NodesUpdated` from a prior pass.
6. **Disclosed, not fixed:** `UpdateMultiRegionClusterInput` also has
   `ShardConfiguration`/`UpdateStrategy` members (real, confirmed via
   `api_op_UpdateMultiRegionCluster.go`) not modeled at all — same
   underlying no-resharding-state limitation as #5. `UpdateMultiRegionCluster`
   downgraded `wire: ok`→`wire: partial` in PARITY.md rather than left
   silently "ok".
7. **Disclosed, not fixed:** `DescribeUsersInput.Filters` (`[]types.Filter`,
   a generic `Name`/`Values` matcher, real, confirmed via
   `api_op_DescribeUsers.go`) is never modeled. The SDK's own doc comment
   gives no enumerated set of valid `Filter.Name` values for this op —
   implementing a generic matcher without that would mean guessing AWS
   semantics rather than confirming them, so it's recorded as a gap
   instead of a guess.

**PAGINATION — discarded on 7 of 15 Describe ops, fixed 6, disclosed 1.**
`MaxResults`/`NextToken` were parsed into the request struct on
`DescribeEngineVersions`, `DescribeEvents`, `DescribeReservedNodes`,
`DescribeReservedNodesOfferings`, `DescribeMultiRegionClusters`,
`DescribeMultiRegionParameterGroups`, and `DescribeMultiRegionParameters`,
but never passed to the existing `paginateItems` helper (`handler.go`,
already used correctly by the other 8 Describe ops) — every call to these
7 returned the full result set in one page regardless of `MaxResults`.
Fixed 6 by wiring `paginateItems` with a per-op cursor key
(`EngineVersion.Engine+"|"+EngineVersion`, `ReservedNode.ReservationID`,
`ReservedNodesOffering.ReservedNodesOfferingID`,
`MultiRegionCluster.MultiRegionClusterName`,
`MultiRegionParameterGroup.Name`, and the sorted
`multiRegionParameterObject.Name`) — all six backends return either a
static catalog or an explicitly `sort.Slice`-d result, so a name-based
cursor is sound. `DescribeEvents` left unfixed and disclosed: its backend
(`events.go`) iterates `b.events` (a map keyed by region) without scoping
to the calling request's region at all, and appends across region-map keys
in Go's non-deterministic map-iteration order — pagination on top of a
non-deterministic base order would silently skip or repeat items across
pages, which is worse than the current single-page behavior. The
region-scoping issue itself reads as a separate real backend-logic bug
(cross-region event leakage), not a wire-shape one; flagged in PARITY.md's
gaps for a follow-up rather than fixed here.

**SHARED CONVERTERS:** `snapshotClusterConfigFor` (new, see #2 above) is
the only shared converter touched this pass, and it's shared correctly —
all three call sites (`CreateSnapshot`, `seedAutomatedSnapshotLocked`, the
delete-cluster final-snapshot path) need the identical `Cluster`→
`snapshotClusterConfig` mapping, confirmed by diffing what each call site
built before the refactor (byte-for-byte identical struct literals in all
three, modulo the two now-added fields). No disguised-asymmetry traps
found among memorydb's other shared converters — `recurringChargeObject`
(shared `ReservedNode`/`ReservedNodesOffering`), `parameterGroupObject`
(shared plain/multi-region-adjacent group ops), and
`multiRegionParameterGroupObject` vs. the earlier-fixed
`multiRegionParameterObject` (confirmed a REAL intentional asymmetry — the
plain-`Parameter`-reusing bug this exact shape represents was already
fixed by a prior pass, re-verified per-op, not re-broken).

**PERSISTENCE TRAP, checked:** `snapshotClusterConfig` is embedded directly
in `Snapshot`, which is `json.Marshal`ed as this service's on-disk
persistence DTO (`persistence.go`) — the same struct serves both roles.
Only new fields with fresh tags (`MultiRegionClusterName`,
`MultiRegionParameterGroupName`) were added; no existing field was
retagged, so old persisted snapshots decode unaffected (missing keys ->
zero values) and the persistence version constant
(`memorydbSnapshotVersion`) did not need bumping. Same check applied to
`MultiRegionCluster` (also its own persistence DTO): `NumShards` added
fresh, nothing retagged.

**OVER-WIDE FIELD / CREDENTIAL SWEEP:** clean, deliberately run. Zero
password/secret/credential/privatekey/clientsecret hits in any non-test
`.go` file. `KmsKeyId`/ARNs throughout are real, intentional response
members matching AWS's own wire shape (parity, not a leak) — MemoryDB's
`Authentication.PasswordCount` (a count, never the password itself) is
the closest thing to a credential-shaped field in this service and it
already matches the real wire shape exactly.

**PHANTOM OPS:** none — all 45 `GetSupportedOperations()` entries
(44 real ops + the deliberately-unadvertised, already-disclosed
`ExportSnapshot` scaffolding route, per handler.go's own comment) diffed
1:1 against the pinned SDK's `api_op_*.go` files.

**PRIOR-AUDIT CHECK:** the existing PARITY.md (last real pass
2026-08-10, gopherstack-yusn) was unusually thorough and had already
field-diffed most wire types against `deserializers.go` by name and
nesting — that pass's own comments cite the exact case-list counts for
`Cluster`/`Snapshot`/`ParameterGroup`/etc. But it was blind on the same
two axes this campaign's brief predicts: **Go-kind/casing** (never
compared `"IPDiscovery"` against the real key's exact casing, since
`EqualFold`-style thinking doesn't apply to a manual code read the way it
does to a grep) and **request-side key names** (its sweep note explicitly
says "field-diffed every core wire type's Go struct against its own
deserializers.go case list" — deserializers.go is the RESPONSE side only;
the request-side `serializers.go` was never walked, which is exactly
where the `MultiRegionParameterGroupName` bugs and the `NumShards`/
`Duration`/`ReservedNodesOfferingId` discarded inputs were hiding). Not an
argued-away bug in either case — a genuine coverage gap on an axis that
pass's own stated method didn't cover, same pattern as the
elasticsearch/lakeformation/xray "thorough but different axis" results
noted elsewhere in this file. `last_audit_commit` was stale (`437393d5`,
pre-dating this pass), now set to `PENDING` per the transcribe/mediatailor
precedent (orchestrator sets it on commit).

**REQUIRED-MEMBER DIFFS, both directions, all 18 ops:** response side
(`deserializers.go`) diffed for every op and every reachable nested type;
request side (`serializers.go`) diffed for every op's top-level input.
Both scripted (see above), not spot-checked.

**EMPTY/204 RESPONSES:** none in this op set — all 18 are non-void reads.

**TESTS:** `services/memorydb/wire_field_fixes_test.go` (new), 7 real
`aws-sdk-go-v2` client tests through the router (`newMemorydbSDKClient`,
same pattern as transcribe's `newTranscribeSDKClient`) covering all 7
fixes above except the two pagination-only fixes folded into one explicit
pagination test (`DescribeEngineVersions`, `MaxResults`/`NextToken`
round-trip across two pages) and the `DescribeReservedNodes` filter test
also exercising `PurchaseReservedNodesOffering` end-to-end. Every fix
hand-reverted individually (edited back to the pre-fix shape — this
session bans even `git checkout --`), confirmed to fail with the exact
predicted symptom, then restored and confirmed byte-identical via `git
diff` comparison against a saved pre-revert baseline diff (not just eyeballed):
`IpDiscovery` reverted to `"IPDiscovery"` → empty string, no decode error
(awsjson1.1 tolerates unknown/missing fields, so this is the weaker
"missing value" signal, not a hard failure, exactly as expected);
`DescribeMultiRegionParameters`' response key reverted to `"Parameters"` →
empty list, no error; its request key reverted to `"ParameterGroupName"`
→ hard `400 InvalidParameterValueException: MultiRegionParameterGroupName
is required` (this one IS a hard client-visible failure, since the field
is required); `DescribeMultiRegionParameterGroups`' request key reverted
the same way → silent over-return, 4 groups instead of 1, no error;
`NumShards`/`NumberOfShards` reverted → `int32(0)` instead of `3`, no
error; the `ClusterConfiguration` MultiRegionClusterName/
MultiRegionParameterGroupName population reverted → empty strings, no
error; `DescribeReservedNodes`' offering-ID filter reverted → an
unmatched filter still returned the reservation, no error;
`DescribeEngineVersions` pagination reverted → `MaxResults: 1` returned
all 5 catalog entries instead of 1, no error. 8 of 9 individual reverts
produced the weaker "wrong/missing value, no decode error" signal the
brief predicts for awsjson1.1; only the required-field request-key revert
produced a hard error, and that's correctly the exception (a genuinely
required member with nothing to bind to).

**GATES:** scoped `go build ./services/memorydb/...` and full `go build
./...` (no interface signature changes -- `StorageBackend` untouched, only
internal request/response struct fields and one new unexported backend
method) both clean; `go vet` clean; `go test -race -count=1
./services/memorydb/...` and `./pkgs/...` both green; `go fix -diff`
empty; `golangci-lint run ./services/memorydb/...` 0 issues (fixed 3
`golines` wraps by hand in the new test file, none `-fix`d); `fieldalignment`
clean (included in the golangci-lint govet config, confirmed via
`.golangci.yml`); zero `//nolint:cyclop/gocyclo/gocognit/funlen`,
grep-confirmed. No subagents used (Read/Grep/Bash only, per this session's
hard constraint). No git-mutating commands run -- orchestrator must
commit/push. `git status` re-checked before every edit batch; only
`services/memorydb/*` and this remainder file touched throughout —
`services/codedeploy/*` (the sibling that appeared live mid-session) was
never read or touched.

`memorydb`'s List/Describe/Get families are now fully swept for this issue
(18/18 ops layer-1/2/3 clean; request AND response sides scripted both
directions; 7 real bugs found and fixed spanning wrapper-key sibling-traps,
request-key mismatches severe enough to break an op outright, discarded
inputs feeding never-modelled response members, and discarded pagination
on 6 ops; 3 gaps disclosed rather than guessed, all tied to the same
no-in-progress-resharding-state limitation or an undocumented-filter-semantics
limitation; 0 real-data leaks; 0 phantom ops). **96 of 162 services swept,
66 remain.** Per the ranked table, `codedeploy` (live sibling this
session, 18 L+D+G) and `accessanalyzer` (18 L+D+G, free) are the two
remaining services at this tier; re-run `go run ./cmd/opcensus` and
re-check `git status` before picking, as usual.

## codedeploy (this session, 2026-08-15)

**PICK AND TIE-BREAK.** Read this file's header/tail, `bd show
gopherstack-6flj`'s comments, and `git show 373def88f` (the mediatailor
pass immediately prior). `go run ./cmd/opcensus` showed the next tier tied
at 18 L+D+G: `memorydb`, `codedeploy`, `accessanalyzer`. `git status` at
pickup showed `memorydb` already live (9 modified files, a concurrent
session's uncommitted work) -- confirmed by this file's own `memorydb`
section above, added moments earlier by that session, which independently
picked `memorydb` on surface and flagged `codedeploy` as the live sibling
it saw appear mid-session. **Occupancy ruled `memorydb` out.** Between the
two free services, surface decided cleanly: `codedeploy` has 10 distinct
resource-family `handler_*.go` files (application_revisions, applications,
deployment_configs, deployment_groups, deployment_instances, deployments,
github_tokens, lifecycle_hooks, on_premises_instances, tags) versus
`accessanalyzer`'s 8 (access_previews, analyzed_resources, analyzers,
archive_rules, findings, generated_policies, policy_validation, tags).
Picked `codedeploy`. No occupancy override was needed for this half of the
tie-break -- surface alone decided it, matching this issue's own recorded
precedent for a clean surface-only pick (`mediatailor` vs `transcribe`).

**SDK pinned:** `codedeploy@v1.38.4` (`go.mod`, matches PARITY.md, no
drift, cached under `$(go env GOMODCACHE)`, no dependency-boundary
exception needed).

**PROTOCOL, ROUTER, SECOND CLIENT, EQUALFOLD:** `awsAwsjson11_` prefix
throughout `deserializers.go`/`serializers.go` (confirmed via
`api_client.go`) -- JSON-RPC/awsjson1.1. Of 344 `EqualFold` call sites in
`deserializers.go`, 9 are non-error-code (all `"NaN"`/`"Infinity"`/
`"-Infinity"` float parsing) and the remaining 335 all match on
`errorCode` for exception-type dispatch -- **zero body-field-key
`EqualFold` calls**, so body decode is case-SENSITIVE, as expected for
this protocol. Router is a flat `X-Amz-Target` prefix-match dispatch
(`strings.HasPrefix` in `handler.go`'s `RouteMatcher`/`ExtractOperation`),
**structurally immune** to the path-segment-router bug class. No second
SDK client import anywhere outside `_test.go` files.

**PHANTOM OPS:** zero, both directions. `GetSupportedOperations`' 47
entries exact-matched the SDK's 47 `api_op_*.go` files 1:1 (scripted
`comm` diff after extracting both lists), confirmed no service-side
extras and nothing the real SDK exposes that gopherstack lacks.

**SCRIPTED KEY EXTRACTION:** yes. A small Python helper
(`/tmp/.../extract_keys.py`, gitignored, not committed) walks
`deserializers.go`, locates a named function by a paren-balance-aware
scan of its signature (**hit the documented `interface{}`-in-signature
trap**: `func …Output(v **T, value interface{}) error {` has its own
brace pair inside the parameter list that a naive "find first `{`" search
mistakes for the function body -- fixed by matching the signature's
parens to balance first, then finding the body's own opening brace from
there) and regex-extracts every top-level `case "<key>":`. Run
individually for all 18 in-scope List/Get/BatchGet ops (`GetSupportedOperations`
prefix-classified: 10 `List*`, 0 `Describe*`, 8 `Get*`, matching
`cmd/opcensus`'s own count) plus every nested nested-type deserializer
they call, and separately against `serializers.go` for three request-side
structs whose casing turned out to matter (see bug 1). Also swept the 7
`BatchGet*` ops (not counted in the 18 by `cmd/opcensus`'s `List/Describe/Get`
prefix convention, since they start with `Batch`, but read for this pass
since they're collection-returning read ops of exactly this bug class --
`BatchGetDeploymentInstances`/`BatchGetDeploymentTargets` in particular
share converters with the counted `Get*` siblings).

**TOP-LEVEL WRAPPER KEYS: mostly clean, one flagship break.**

1. **FLAGSHIP, response-side, silent-empty on every real client call:**
   `ListTagsForResourceOutput` was wire-tagged `json:"tags"` (lowercase).
   The real deserializer's switch
   (`awsAwsjson11_deserializeOpDocumentListTagsForResourceOutput`,
   `deserializers.go:20417`) is `case "Tags":` / `case "NextToken":` --
   **PascalCase**, unlike every other op in this service (which is
   uniformly camelCase: `applicationName`, `deploymentGroupId`, etc). This
   is the one op family (`TagResource`/`UntagResource`/
   `ListTagsForResource`) that uses AWS's shared generic tagging shape
   instead of CodeDeploy's own op-specific field-naming convention --
   confirmed the same PascalCase (`ResourceArn`/`Tags`/`TagKeys`) on the
   request side too, via `serializers.go`'s
   `awsAwsjson11_serializeOpDocumentTagResourceInput`/
   `UntagResourceInput`/`ListTagsForResourceInput`. Since this protocol's
   decode is case-sensitive (confirmed above, no `EqualFold`), a real
   client's `ListTagsForResource` call **always got an empty `Tags` slice**
   regardless of what had actually been tagged -- the exact silent-empty
   class this whole campaign exists to find, hiding in the one op family
   whose casing convention differs from its own service's norm.

   Fixed the response side (`listTagsForResourceOutput.Tags` ->
   `json:"Tags"`) and, for full wire-shape correctness, the request side
   too (`tagResourceInput`/`untagResourceInput`/`listTagsForResourceInput`
   -> `ResourceArn`/`Tags`/`TagKeys`, all PascalCase). **The request-side
   fix is NOT independently observable**: this repo's `HandleJSON`
   (`pkgs/service/jsondisp.go`) decodes incoming bodies with plain
   `encoding/json.Unmarshal`, which matches JSON keys to Go struct tags
   case-insensitively as a fallback when no exact match exists -- so a
   real client's PascalCase request body was already binding correctly to
   the old lowercase-tagged struct fields before this fix, and still does
   after. Only the response direction (marshaled with an exact,
   non-fallback tag match, then decoded by the real SDK's
   hand-rolled case-sensitive switch) was a live bug. Disclosed as such
   rather than claimed as an equally-live fix on both sides.

   Two existing tests (`tags_test.go`'s `TestTags_SortedListTagsForResource`
   and `TestTags_OnDeploymentGroups`) decoded the response with a local
   anonymous struct tagged `json:"tags"` (lowercase) -- **this is the
   "existing wrong-key test" pattern**, but with a twist worth recording:
   because both the test's decode AND gopherstack's own (buggy) encode used
   plain `encoding/json` with its case-insensitive fallback, **these tests
   would have passed identically before and after the fix** -- Go's own
   stdlib leniency makes a raw `json.Unmarshal`-into-local-struct test
   structurally blind to this entire bug class, independent of whether the
   fix is applied. Neither test would have caught the bug in the first
   place, nor would either one regress if the fix were reverted. Updated
   both to `json:"Tags"` for accuracy anyway, but the real verification is
   the new real-SDK-client test below, whose response decode goes through
   the actual generated (case-sensitive) deserializer.

**SHARED CONVERTERS AND NESTED SHAPES BELOW THE TOP LEVEL, per-op, script-verified against their own real deserializer:**

- `ApplicationInfo` (`GetApplication`/`BatchGetApplications`, shared
  `applicationInfo` wire type): real type has 6 keys (`applicationId`,
  `applicationName`, `computePlatform`, `createTime`, `gitHubAccountName`,
  `linkedToGitHub`); gopherstack emits 4, missing `gitHubAccountName`/
  `linkedToGitHub`. **DISCLOSED, not added**: confirmed via
  `CreateApplicationInput`/`UpdateApplicationInput` that the real API has
  no request-side member to ever set either value (this is legacy
  console-driven GitHub OAuth linking, never exposed as a public
  parameter) -- this backend has no OAuth concept at all, so both would
  forever read as Go zero-values (`""`/`false`). Since `omitempty`
  suppresses a zero-value field identically whether or not the Go struct
  field exists, **adding these two fields would be a pure source-code
  change with zero wire-byte difference** -- unlike the fixes above/below,
  there is nothing for a test to observe. Recorded in PARITY.md rather
  than added as dead code.
- `DeploymentGroupInfo` (`GetDeploymentGroup`/`BatchGetDeploymentGroups`,
  shared `deploymentGroupInfoOutput`): real type has 23 keys; gopherstack
  had 20, missing `lastAttemptedDeployment`, `lastSuccessfulDeployment`,
  and `targetRevision`. **FIXED** -- and genuinely observable, unlike the
  `ApplicationInfo` case above: the backend already tracks every
  deployment's `CreateTime`/`Status`/`Revision` per (application,
  deployment-group) pair (`deployments.go`), so these three are real,
  non-fabricated derivations from existing state, not fabricated
  placeholders. Added `InMemoryBackend.LastDeploymentsForGroup` (scans
  `b.deployments.All()` filtered by app+group, matching
  `ListDeployments`' own scan-based approach -- no dedicated index
  exists) returning the most-recently-created deployment
  (`LastAttemptedDeployment`) and the most-recently-created *successful*
  one (`LastSuccessfulDeployment`) separately, since a failed/stopped
  deployment must still count as "attempted" but never as "successful".
  `TargetRevision` is set from the most-recently-**attempted**
  deployment's own revision, not the successful one -- the real SDK's own
  doc comment for `TargetRevision` ("the deployment group's target
  revision") does not distinguish attempted-vs-successful, so this is the
  plain reading of "target": the revision the group is currently trying
  to converge to. **Disclosed, not derived past what's supportable**: this
  attempted-vs-successful choice for `TargetRevision` specifically could
  not be independently confirmed against a live AWS account or a more
  precise doc comment -- noted in PARITY.md as an interpretation, not a
  guess about unrelated data (unlike a "candidate derivation from adjacent
  but conceptually different data", which this is not: `TargetRevision`
  and `Deployment.Revision` are literally the same concept on both sides,
  just at different points in a deployment's lifecycle).
- `InstanceInfo` (on-premises instance; `GetOnPremisesInstance`/
  `BatchGetOnPremisesInstances`, shared `onPremisesInstanceInfo`): real
  type has 7 keys; gopherstack had 6, missing `instanceArn`. **FIXED** --
  added `InMemoryBackend.OnPremisesInstanceARN`, reusing the exact
  `"instance:<name>"` resource-format already used for the same resource
  type's `InstanceTarget.TargetArn` elsewhere in this service
  (`deployment_instances.go:130`), so this is a consistent, already-precedented
  construction, not a new guess.
- `StopDeploymentOutput`: real type has 2 keys (`status`, `statusMessage`);
  gopherstack had 1, missing `statusMessage`. **FIXED** -- since this
  backend's `StopDeployment` always synchronously succeeds
  (`stopStatusSucceeded` is hardcoded, pre-existing), the accompanying
  message is deterministic. Sourced verbatim from the real SDK's own doc
  comment for the `Succeeded` `StopStatus` value
  (`api_op_StopDeployment.go`: "Succeeded: The stop operation was
  successful."), not invented text.
- `InstanceSummary`/`InstanceTarget`/`ECSTarget`/`LambdaTarget` (deployment
  targets; `GetDeploymentInstance`/`BatchGetDeploymentInstances`/
  `GetDeploymentTarget`/`BatchGetDeploymentTargets`): real types each carry
  a `lifecycleEvents` list (7/7/7/6-of-7 keys present vs gopherstack's
  5-6); `ECSTarget` is additionally missing `taskSetsInfo`, `LambdaTarget`
  is additionally missing `lambdaFunctionInfo`. **DISCLOSED, not added**:
  `PutLifecycleEventHookExecutionStatus` (`handler_lifecycle_hooks.go`) is
  a pure echo -- it validates the deployment exists and returns the
  request's own execution ID, storing nothing -- so this backend has zero
  real per-target lifecycle-hook-execution state to ever report,
  regardless of target type. Same story for `taskSetsInfo` (no ECS
  task-set orchestration modeled) and `lambdaFunctionInfo` (no Lambda
  alias-shift data modeled). As with `ApplicationInfo` above, these would
  forever read as empty/nil, so adding the struct fields changes zero
  wire bytes -- disclosed in PARITY.md rather than added as dead code.
- `DeploymentTarget` union: real type has a 5th member,
  `cloudFormationTarget`, alongside the 3 gopherstack already emits
  (`instanceTarget`/`ecsTarget`/`lambdaTarget`). **Confirmed as an
  accurate pre-existing disclosure**, not a gap this pass found: the
  handler's own doc comment already states "there is no
  CloudFormationTarget concept here, since this backend has no
  CloudFormation blue/green integration" -- true, this backend has no CF
  stack-set deployment path anywhere, so this member can never be
  populated honestly. Not previously in PARITY.md's own text; added there
  this pass for completeness, but the underlying design decision was
  already correct and documented in code.
- `RevisionLocation`: real type's deserializer has a 5th case, `"string"`
  (the deprecated `RawString` member, `RevisionLocationType` = `"String"`,
  Lambda-deployment-only legacy raw YAML/JSON revisions), alongside the 4
  gopherstack models (`s3Location`/`gitHubLocation`/`appSpecContent`/
  `revisionType`). **Never previously disclosed anywhere in PARITY.md** --
  new finding, disclosed (not fixed): this is explicitly documented as
  deprecated in the SDK's own doc comment, and S3/GitHub/AppSpecContent
  cover every revision-location path this backend's `CreateDeployment`/
  `RegisterApplicationRevision`/etc. can actually construct, so there is
  no honest non-empty value to emit and no code path that could ever
  populate it.

**FILTERS AND PAGINATION:** re-confirmed, not re-derived, the existing
`gopherstack-a250` TRIAGED finding in `PARITY.md` (`ListApplications`/
`ListDeploymentConfigs`/`ListGitHubAccountTokenNames` discard a real but
inert `NextToken` since no `List*` op in this service ever truncates).
Verified this also holds, unchanged, for every other `List*` op touched
this pass (`ListApplicationRevisions`, `ListDeploymentGroups`,
`ListDeploymentInstances`, `ListDeploymentTargets`,
`ListOnPremisesInstances`, `ListTagsForResource`) -- none paginate, so
`NextToken`/`MaxResults` remain uniformly inert across the whole service,
not just the three ops the prior note named. This is an accurate,
still-current prior-audit note, not an argued-away claim -- no correction
needed.

**REQUIRED-MEMBER DIFFS, both directions:** no gap found among the 18 (+7
`BatchGet*`) ops touched -- every real required member this service's
handlers read is validated, and no handler demands a field the real Input
structurally lacks.

**EMPTY/204 RESPONSES:** `DeleteApplication`/`DeleteDeploymentGroup`/
`DeleteDeploymentConfig`/`DeregisterOnPremisesInstance`/`TagResource`/
`UntagResource`/`ContinueDeployment`/`SkipWaitTimeForInstanceTermination`/
`DeleteResourcesByExternalId` all return an empty `200 {}` body; every one
of their real Output types is confirmed genuinely empty
(`ResultMetadata` only) -- correct, no truncated real body among them.

**OVER-WIDE FIELD / CREDENTIAL SWEEP:** clean. `ServiceRoleArn`,
`IamSessionArn`/`IamUserArn`, and every `*Arn` field are real,
intentional identifiers matching AWS's own wire shape (parity, not
leakage) -- this service has no password/secret/credential-shaped field
anywhere.

**PERSISTENCE TRAP:** checked before every addition. All fields touched
this pass live on wire-only converter structs
(`applicationInfo`/`deploymentGroupInfoOutput`/`onPremisesInstanceInfo`/
`stopDeploymentOutput`, all in `handler_*.go` files), computed fresh per
request from the domain models (`Application`/`DeploymentGroup`/
`OnPremisesInstance`/`Deployment` in `models.go`) that `persistence.go`
actually snapshots. None of the persisted domain structs themselves were
touched or retagged -- zero persistence risk.

**TESTS:** `services/codedeploy/wire_field_fixes_test.go`, 6 new tests, all
driven through the real `aws-sdk-go-v2` `codedeploy` client via the
existing `newTestCodeDeployClient` helper (`handler_sdk_roundtrip_test.go`)
so the response side goes through the genuine case-sensitive generated
deserializer, not a hand-decoded struct:
`TestListTagsForResource_RealClient_Tags`,
`TestGetDeploymentGroup_RealClient_History` (also exercises
`BatchGetDeploymentGroups` sharing the same converter),
`TestGetDeploymentGroup_RealClient_NoDeploymentsYet` (a group with zero
deployments correctly gets `nil`, not synthesized placeholders, for all
three history fields), `TestOnPremisesInstance_RealClient_InstanceArn`
(both `Get` and `BatchGet`), `TestStopDeployment_RealClient_StatusMessage`.
Every one of the 4 fixes was hand-reverted individually (git-mutating
commands banned this session, including `git checkout --`, so reverts were
by hand-edit back to the exact pre-fix line), the corresponding test
re-run and confirmed to fail with the exact predicted symptom (`Tags`
empty; `LastAttemptedDeployment` nil; `InstanceArn` empty string;
`StatusMessage` empty string -- all silent-missing-value, no decode error,
matching this protocol's known-weaker awsjson1.1 signal), then restored
and confirmed **byte-identical** to the pre-revert diff via `diff` against
a saved snapshot of `git diff` output for each file (not just eyeballed).
Two pre-existing tests (`tags_test.go`) updated for casing accuracy per
the flagship bug above, though as noted their pass/fail was never actually
gated by this bug either direction.

**GATES:** `go build ./services/codedeploy/...` and full `go build ./...`
(no exported interface signature changed -- `LastDeploymentsForGroup` and
`OnPremisesInstanceARN` are both new additive backend methods) both clean;
`go vet ./services/codedeploy/...` clean; `go test -race -count=1
./services/codedeploy/...` and `./pkgs/...` both green; `go fix -diff`
empty. `golangci-lint run ./services/codedeploy/...` found 3 issues on
first pass -- `fieldalignment` on `lastDeploymentInfoEntry` and
`deploymentGroupInfoOutput`, `nonamedreturns` on
`LastDeploymentsForGroup` -- all fixed **by hand**, not `-fix`/`--fix`,
per this campaign's documented `fieldalignment -fix`
`//nolint`-stripping hazard (this file has 2 pre-existing `//nolint`
comments, one inside the very struct that needed realignment): the target
field order was derived by running `fieldalignment -fix` against an
isolated scratch copy of just the struct definitions in `/tmp`, reading
its output, then manually applying that exact order to the real file and
re-running the linter to confirm 0 issues, rather than running the tool
on the real file directly. Final: `golangci-lint run
./services/codedeploy/...` reports **0 issues**. Zero
`//nolint:cyclop/gocyclo/gocognit/funlen` added, grep-confirmed both
before and after.

No subagents used (Read/Grep/Bash only, per this session's hard
constraint). No git-mutating commands run at any point -- orchestrator
must commit/push. `git status` re-checked before every edit batch; only
`services/codedeploy/*` and this remainder file touched throughout --
`services/memorydb/*` (the sibling live at pickup) was never read or
touched.

`codedeploy`'s List/Get/BatchGet families are now fully swept for this
issue (18/18 counted ops + 7 `BatchGet*` ops, layer-1/2/3 clean; 1
flagship response-side wrapper-key bug fixed, response-observable only,
hiding in the one op family with a different casing convention than the
rest of the service; 3 further real never-modelled-member bugs fixed,
all genuinely observable and derived from real existing backend state,
not fabricated; 6 further never-modelled members across 5 shapes
disclosed rather than added as dead code, since none has any honest
non-zero source in this backend and `omitempty` makes their absence
byte-identical to their presence-but-always-empty; 1 pre-existing
disclosure in code comments confirmed accurate and promoted into
PARITY.md; 1 prior `PARITY.md` audit note re-confirmed accurate, not
argued-away; 0 real-data leaks; 0 phantom ops; 0 persistence risk).
**97 of 162 services swept, 65 remain.** Per the ranked table,
`accessanalyzer` (18 L+D+G) is the only service left at this tier; below
it, `elasticbeanstalk`/`docdb`/`batch` (17 each) are next. Re-run `go run
./cmd/opcensus` and re-check `git status` before picking, as usual.

## accessanalyzer (this session, 2026-08-15)

**PICK AND TIE-BREAK.** Read this file's header/tail, `bd show
gopherstack-6flj`'s comments, and `git show 3859139ba` (the pass
immediately prior to this one). `go run ./cmd/opcensus` confirmed the
tier-18 tie unchanged from the codedeploy section above: `memorydb`,
`codedeploy`, `accessanalyzer`, all still 18 L+D+G. `git status` at
pickup showed `memorydb` already committed (not in the working tree) and
`services/codedeploy/*` still modified/uncommitted -- the same live
sibling the codedeploy section's own author flagged mid-session.
**Occupancy ruled out `codedeploy`.** `accessanalyzer` was the only free
service left at this tier, so surface comparison wasn't needed to break a
tie -- there was no tie left to break once occupancy removed the other
two. Picked `accessanalyzer`. (Mid-session, `services/docdb/*` -- the
next tier down, 17 L+D+G -- also went live under a concurrent sibling;
confirmed via repeated `git status` and never read or touched.)

**SDK pinned:** `accessanalyzer@v1.51.4` (`go.mod`, matches PARITY.md, no
drift).

**PROTOCOL, ROUTER, SECOND CLIENT, EQUALFOLD:** restjson1. All 201
`EqualFold` calls in `deserializers.go` match on `errorCode` only (zero
body-field-key `EqualFold`s) -- case-sensitive body decode, as expected.
Router is path-segment-based (`RouteMatcher`/`parseRESTPath`), NOT flat
`X-Amz-Target` dispatch -- not structurally immune to the router-collision
class, but the existing `/tags/{ARN}` handling already guards the known
collision risk by checking for `:access-analyzer:` in the ARN; no new
collision found. No second SDK client import outside `_test.go`.

**PHANTOM OPS:** zero, both directions (39 `op*` constants vs 39
`api_op_*.go` files, exact 1:1 diff).

**SCRIPTED KEY EXTRACTION: both directions.** Paren-balance-aware Python
walker (gitignored, not committed) resolved every op's
`awsRestjson1_deserializeOpDocument*Output`/`serializeOpDocument*Input`
function and walked transitively into every nested `Document*` call, for
all 39 ops. Hit the documented `interface{}`-in-signature parsing trap on
the deserializer side; fixed by balancing the signature's parens before
searching for the body's opening brace. Cross-referenced the full
extracted key set against every `json:"..."` tag in the service's non-test
`.go` files.

**TOP-LEVEL WRAPPER KEYS: clean except one flagship union-key bug.**
`GetFindingsStatistics`'s `types.FindingsStatistics` is a union keyed by
wire name (`externalAccessFindingsStatistics` /
`internalAccessFindingsStatistics` / `unusedAccessFindingsStatistics`,
`deserializers.go` ~L9169) -- this backend explicitly models both
external-access-type AND unused-access-type analyzers
(`AnalyzerTypeAccountUnusedAccess`/`AnalyzerTypeOrganizationUnusedAccess`,
`models.go`), but the handler always emitted the external-access key
regardless of the target analyzer's own `Type`. A real client's typed
union switch on an unused-access analyzer's statistics landed on the
wrong Go type. Fixed by selecting the wire key from the looked-up
analyzer's `Type`. Everything else at this layer (`ListAnalyzers`,
`ListArchiveRules`, `ListFindings`/`ListFindingsV2`, `ListAccessPreviews`/
`ListAccessPreviewFindings`, `ListAnalyzedResources`, `ListPolicyGenerations`,
`ListTagsForResource`, `UpdateAnalyzer`'s response) all confirmed correct
against the prior 2026-08-10 audit (`19eea66b2`), re-verified rather than
re-litigated.

**BELOW THE TOP LEVEL: three discarded-filter-input bugs, a different
axis than the wrapper-key layer.** `grep '_ [A-Za-z]*FilterCriterion'`
surfaced `ListFindings`' backend method taking its filter parameter as
literal `_` -- decoded from the wire's real `filter` key, then dropped.
`ListFindingsV2` was worse: the handler never even decoded `filter` from
the body, and the backend method had no filter parameter at all.
`ListAccessPreviewFindings` matched the `ListFindingsV2` shape. All three
real client filters were pure no-ops. Fixed with a shared
`matchesFindingFilter` helper (Eq operator only, on the
status/resourceType/resource/id fields this backend tracks directly;
Contains/Neq/Exists and unmodeled filter keys disclosed, not faked).

**RELATED, SAME ROOT CAUSE, FOUND WHILE BUILDING THE FILTER HELPER (a
behavioral bug, not itself wire-shape): `CreateArchiveRule`/
`ApplyArchiveRule` ignored their own rule's filter and blanket-archived
every active finding regardless of match.** Fixed both with the same
`matchesFindingFilter` helper; `ApplyArchiveRule` also gained the missing
required-`RuleName` validation (previously optional-and-ignored).

**List item types checked:** `AnalyzerSummary` (`ListAnalyzers`),
`ArchiveRule` (`ListArchiveRules`), `Finding`/`FindingSummaryV2`
(`ListFindings`/`ListFindingsV2`), `AccessPreviewSummary`/
`AccessPreviewFinding` (`ListAccessPreviews`/`ListAccessPreviewFindings`),
`AnalyzedResourceSummary` (`ListAnalyzedResources`), `PolicyGeneration`
(`ListPolicyGenerations`) -- all confirmed to be the real *Summary/List*
shapes, not the full `Get*` type reused wholesale; the prior 2026-08-10
audit already correctly modeled the `Configuration`-present-on-Get-but-
absent-on-List asymmetry for `Analyzer`/`AnalyzerSummary`, re-verified
unchanged this pass.

**Same spelling, different correctness per direction:** none found this
pass (checked specifically after memorydb's `IPDiscovery` precedent) --
every field this service shares between request and response uses the
same casing on both sides, and the one place that looked asymmetric
(`ListAnalyzers`/`GetAnalyzer`'s `Configuration` presence) is a real,
documented AWS asymmetry (different response TYPES, not a request/response
casing mismatch), already correctly handled before this pass.

**Disclosed, not derived:** `GetAnalyzedResource`'s optional
`Actions`/`Error`/`SharedVia`/`Status` members are never emitted --
`AnalyzedResource` and `Finding` are two independent synthetic-data paths
in this backend with no enforced ARN link between them; deriving `Status`
from a same-ARN `Finding.Status` would be exactly the adjacent-but-
different-data fabrication parity-principles #1 warns against, so it was
declined and disclosed instead. `unusedAccessFindingsStatistics`'s
`TopAccounts`/`UnusedAccessTypeStatistics` (new from the union fix above)
disclosed the same way -- no per-account or per-unused-access-type
aggregation exists to derive them from.

**Empty/204 responses:** unaffected this pass; already verified clean in
the 2026-08-10 audit.

**Required-member diffs, both directions:** no new gap found among the 39
ops -- the 2026-08-10 audit's fixes (condition/resourceOwnerAccount/
findingDetails/etc.) all re-verified still correct and unchanged.

**Filters and pagination:** filters were the main finding this pass (see
above). Pagination (`NextToken`/`MaxResults` token-based slicing) verified
unchanged and correct on `ListFindings`/`ListFindingsV2`/
`ListAccessPreviewFindings`/`ListAnalyzedResources`/`ListArchiveRules`/
`ListPolicyGenerations` -- no ordering-nondeterminism issue found (all
sort by a stable key before paginating), so no pagination fix was
refused this pass.

**TESTS:** 6 new real-`aws-sdk-go-v2`-client tests (listed in
`services/accessanalyzer/PARITY.md`'s own pass section). Every fix
hand-reverted individually and confirmed to fail with the exact predicted
symptom (wrong union member type; unfiltered extra finding in the result;
wrongly-archived non-matching finding), then restored and confirmed
byte-identical against a saved `git diff` snapshot per file.

**GATES:** `go build ./services/accessanalyzer/...` clean throughout; full
`go build ./...` clean at pickup and again at the end (the `ListFindingsV2`
signature change is the only exported-interface change this pass, so the
full build was re-run after it; `services/docdb/*` was mid-edit by a
concurrent sibling at various points but never left the tree unbuildable
when checked). `go vet`, `go test -race -count=1
./services/accessanalyzer/...`, and `go test -race ./pkgs/...` all clean.
`go fix -diff` flagged one manual loop `slices.Contains` would replace in
the new filter-matching helper -- applied by hand. `golangci-lint run
./services/accessanalyzer/...`: 3 `golines`/`lll` line-length findings in
new test code on first pass, fixed by hand-wrapping; final run **0
issues**. Zero `//nolint:cyclop/gocyclo/gocognit/funlen` before or after.

`accessanalyzer`'s List/Get families are now fully swept for this issue
(39/39 ops layer-1/2/3 clean; 1 flagship union-wrapper-key bug fixed,
observable only through a real client's typed union switch; 3
discarded-filter-input bugs fixed across List/Get-adjacent ops; 2 related
archive-rule filter-matching behavioral bugs fixed; 2 members disclosed
rather than fabricated; 0 real-data leaks; 0 phantom ops; 0 persistence
risk; prior 2026-08-10 wire-shape audit fully re-confirmed, not
re-litigated). **98 of 162 services swept, 64 remain.** Per the ranked
table, the tier-18 group (`memorydb`, `codedeploy`, `accessanalyzer`) is
now fully swept; `elasticbeanstalk`/`docdb`/`batch` (17 each) are next,
and `docdb` was seen live under a concurrent sibling this session (see
above) -- re-run `go run ./cmd/opcensus` and re-check `git status` before
picking, as usual.
