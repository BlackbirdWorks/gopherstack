# Per-service wire protocol map

Built for gopherstack-f9sg. Dispatch guidance about protocol has been wrong
twice this campaign — once claiming query/XML decode case-sensitively (it
doesn't; `strings.EqualFold` is used throughout), once claiming sqs and
cloudwatch are query-protocol like their siblings rds/sns (they aren't: sqs
is JSON, cloudwatch is smithy rpc-v2-cbor, both case-**sensitive**). Protocol
determines whether a casing difference is a real bug or a non-issue, and
whether an unrecognised field is silently dropped or rejected — it is a
per-service fact, not something safe to infer from a sibling or from a
service's age. **Cite this table instead of guessing.**

## Method

For each `services/<dir>`, the pinned SDK package(s) it actually imports
were resolved from `go.mod` (not assumed from the directory name — several
directories use a different local name than their SDK module, e.g.
`cognitoidp` → `cognitoidentityprovider`, `dms` → `databasemigrationservice`,
`stepfunctions` → `sfn`). For each resolved package + pinned version, the
real `$(go env GOMODCACHE)/github.com/aws/aws-sdk-go-v2/service/<pkg>@<ver>`
source was read directly:

- **Protocol**: the prefix on the generated `serializers.go` functions
  (`awsRestjson1_`, `awsAwsjson11_`, `awsAwsjson10_`, `awsAwsquery_`,
  `awsRestxml_`, `awsEc2query_`). Two services (cloudwatch, and effectively
  appstream) don't have a standard-prefixed `serializers.go` at all — those
  were resolved by reading `api_client.go`'s `options.Protocol` assignment
  and/or the hand-written extraction code directly (see their rows).
- **Case-sensitivity**: grepped `deserializers.go` for
  `strings.EqualFold(..., t.Name.Local)` (XML/query element-name matching —
  case-insensitive) versus exact `case "FieldName":` switches on a decoded
  JSON map key (case-sensitive). **Not** simple presence/absence of
  `strings.EqualFold` in the file — nearly every service uses `EqualFold`
  for **error-code** dispatch (`case strings.EqualFold("ThrottlingException",
  errorCode):`) regardless of protocol, and JSON-family services additionally
  use it for float special values (`EqualFold(jtv, "NaN")`). Neither of
  those is field-name matching; an early pass of this sweep counted them and
  produced 8 false "case-insensitive" verdicts for restjson1/awsjson1.1
  services (batch, bedrock and its siblings, cleanrooms, ecs, mediaconvert)
  before the signal was narrowed to `Name.Local` specifically. Every
  case-sensitive verdict below was confirmed against an exact `case "X":`
  switch with no accompanying `Name.Local`-anchored `EqualFold`.
- **Unknown-key behaviour**: every deserializer sampled (JSON, XML, query,
  hand-rolled CBOR alike) has no `default:`/error branch on an unmatched
  key — the loop just continues. Confirmed directly in dynamodb (JSON), s3
  (REST-XML), ec2 (EC2-query), appstream and cloudwatch (CBOR); no
  exception found anywhere in the sample. **Silently dropped** is recorded
  for every row that has a decoder to check; rows with no SDK dependency
  are marked N/A.
- Two directories host **more than one SDK client** with **different**
  protocols and get a second row: `redshift` (classic AWS Query + Serverless
  JSON-RPC 1.1 — the oddity named in the task) and `opensearch` (classic
  REST-JSON 1 + Serverless/AOSS JSON-RPC 1.0). `personalize` also hosts a
  second client (`personalizeruntime`, REST-JSON 1) not named in the task,
  found by checking every directory with more than one AWS-SDK import for
  whether the second package was genuinely dispatched or just referenced
  in a test/comment. `bedrock` hosts BedrockAgent-shaped operations
  in-package too, but since bedrock and bedrockagent are both
  restjson1/case-sensitive it doesn't change any protocol fact — noted, not
  split into a second row.
- A directory-to-package match with **only one or two test-only import
  sites** (`ec2`↔`outposts`, `dax`↔SDK `dynamodb`, `cloudformation`↔`s3`/
  `dynamodb`, `cloudwatch`↔`s3`, `eventbridge`↔`pipes`/`schemas`, `sagemaker`
  ↔`s3`, `stepfunctions`↔`dynamodb`/`s3`, `iot`↔`iotdataplane`, etc.) was
  checked and found to be incidental — cross-service integration tests or
  a single type reference, not a second hosted API. Those directories get
  one row for their real (dispatched) client only.

## Spot-check (hand-read against the pinned SDK source, not just script output)

15+ rows were read directly rather than trusted from script output,
including every oddity named in the task and unremarkable samples chosen
without expecting anything interesting:

| Service | What was hand-read | Result |
|---|---|---|
| rds | `awsAwsquery_` prefix in serializers.go; `EqualFold(..., t.Name.Local)` in deserializers.go | query, case-insensitive — confirmed |
| iam | Same, 1182 total `EqualFold` sites (mix of field + error-code) | query, case-insensitive — confirmed |
| cloudformation | `awsAwsquery_` prefix; body decode reads `t.Name.Local` via EqualFold | query, case-insensitive — confirmed |
| route53 | `awsRestxml_` prefix; same EqualFold pattern | restxml, case-insensitive — confirmed |
| sns | `awsAwsquery_` prefix | query, case-insensitive — confirmed |
| sqs | `awsAwsjson10_deserializeDocumentAttributeValue`: exact `case "B":`/`case "N":` switch, zero `Name.Local`; **all** 163 `EqualFold` calls pair with `errorCode` | JSON, case-**sensitive** — confirmed, matches the task's correction |
| cloudwatch | No `deserializers.go` exists; `api_client.go:214` hardcodes `options.Protocol = rpcv2.NewCBOR(...)`; gopherstack's own `services/cloudwatch/rpcv2cbor.go:cborStr` does `v, ok := m[key]` — exact Go map lookup | rpc-v2-cbor, case-**sensitive** — confirmed, matches the task's correction |
| appstream | `serializers.go` has `serializeCBOR_*`/no standard prefix; `deserializeCBOR_AccessEndpoint` does `if key == "EndpointType"` — exact `==` | hand-rolled CBOR bridge, case-sensitive — confirmed, matches the named oddity |
| redshift | `awsAwsquery_` for classic; separately, `handler_serverless_*.go` imports and dispatches real `redshiftserverless` SDK types (JSON-RPC 1.1) | two clients, two protocols — confirmed, matches the named oddity |
| opensearch | `handler_operations.go:132-146`'s `serverlessOperations()` explicitly documents hosting real `opensearchserverless.Client` ops distinct from classic `opensearch.Client` | two clients, two protocols — **found independently, not named in the task** |
| personalize | `handler.go`'s dispatch table wires `GetRecommendations`/`GetPersonalizedRanking` (personalizeruntime ops) alongside classic personalize ops | two clients, two protocols — **found independently, not named in the task** |
| bedrock | `handler_agents_dispatch_test.go` imports `bedrockagentsdk` and drives real `CreateAgent`/`CreateKnowledgeBase`/`CreateAgentAlias` against the bedrock package | hosts BedrockAgent ops in-package, confirmed — but no protocol split needed (both restjson1) |
| opsworks | `grep -in opsworks go.mod` → no match; `go list -m all` piped through `grep opsworks` → no match; code comments cite `opsworks@v1.31.0` | **no pinned SDK dependency** despite 64 files of real code — task explicitly allowed for this case |
| qldb, qldbsession | `ls services/qldb*` → README.md only, 0 `.go` files | no code at all |
| glacier *(unremarkable sample)* | `awsRestjson1_` prefix; `deserializeDocumentDataRetrievalRule` uses exact `case "BytesPerHour":` | restjson1, case-sensitive — as expected, no surprise |
| workmail *(unremarkable sample)* | `awsAwsjson11_` prefix; exact `case "Actions":` etc. | JSON-RPC 1.1, case-sensitive — as expected, no surprise |
| verifiedpermissions *(unremarkable sample)* | `awsAwsjson10_` prefix confirmed directly | JSON-RPC 1.0 — as expected, no surprise |

**Script-vs-hand disagreement found:** the first pass of the case-sensitivity
script counted *any* `strings.EqualFold` call in `deserializers.go` as a
case-insensitivity signal. That produced false "case-insensitive" verdicts
for 8 restjson1/awsjson1.1 services — `batch`, `bedrock`, `bedrockagent`,
`bedrockruntime`, `cleanrooms`, `ecs`, `mediaconvert`, and one more in the
same run — because those files use `EqualFold` for float `NaN`/`Infinity`/
`-Infinity` string-value parsing (`strings.EqualFold(jtv, "NaN")`), which has
nothing to do with field-name matching. Hand-reading `ecs`'s deserializer
surfaced the real pattern (`case strings.EqualFold(jtv, "NaN")` — a *value*,
not a *key*), which is what led to narrowing the script's signal to
`EqualFold(..., t.Name.Local)` specifically. Every row in the table below
reflects the corrected, narrowed signal, re-run across all 166 pinned SDK
packages.

The separate `sdkshape.sh` helper already in this repo
(`.claude/skills/gopherstack-sdk-shape/scripts/sdkshape.sh`) was tried first
and found to have the **same class of bug from the other direction**: its
`detect_protocol()` only checks for a literal `awsQuery_` prefix, which
doesn't exist — the real generated prefix is `awsAwsquery_` — so it reports
`rds` (and every other query-protocol service) as `unknown (no recognized
serializer prefix)`. Not fixed here (no code changes were in scope for this
task), but worth knowing before trusting that script's protocol line for a
query-protocol service.

## Protocol distribution (165 rows: 162 directories + 3 hosted second-client rows)

| Protocol | Count |
|---|---|
| REST-JSON 1 (`awsRestjson1_`) | 70 |
| JSON-RPC 1.1 (`awsAwsjson11_`) | 58 |
| AWS Query/XML (`awsAwsquery_`) | 14 |
| JSON-RPC 1.0 (`awsAwsjson10_`) | 13 |
| REST-XML (`awsRestxml_`) | 4 |
| EC2-Query/XML (`awsEc2query_`) | 1 (ec2) |
| rpc-v2-cbor, Smithy schema-based (`options.Protocol = rpcv2.NewCBOR(...)`) | 1 (cloudwatch) |
| rpc-v2-cbor, hand-rolled bridge (`deserializeCBOR_*`, exact-match) | 1 (appstream) |
| No pinned SDK dependency | 1 (opsworks) |
| No Go code at all | 2 (qldb, qldbsession) |

**Case-sensitivity does not split cleanly along "JSON vs XML" folklore at
the family level** — it splits along **protocol**, and every protocol
observed here is internally consistent: all 14 AWS Query/XML services, the
4 REST-XML services, and EC2-Query are case-**insensitive**; all 70
REST-JSON 1, 58 JSON-RPC 1.1, 13 JSON-RPC 1.0, and both CBOR variants
(despite one being hand-rolled and one being schema-based) are
case-**sensitive**. In other words: XML-family = insensitive,
JSON-family + CBOR = sensitive, no exceptions found in this sweep. The
danger the task called out (sqs/cloudwatch breaking a naive "these four are
all query" assumption) was about **protocol misidentification**, not about
protocol-to-case-sensitivity mapping breaking down — once the protocol is
correctly identified, case-sensitivity followed it in every one of the 166
packages checked here.

## Table

Column order: directory name in `services/` · resolved go.mod package ·
protocol / generated function prefix (or how it was actually determined,
for the two that lack one) · pinned SDK version · decode case-sensitivity
with evidence · unknown-key behaviour · notes.

| Directory | go.mod package | Protocol / prefix | Version | Case-sensitivity | Unknown keys | Notes |
|---|---|---|---|---|---|---|
| accessanalyzer | accessanalyzer | REST-JSON 1 / `awsRestjson1_` | v1.51.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| account | account | REST-JSON 1 / `awsRestjson1_` | v1.35.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| acm | acm | JSON-RPC 1.1 / `awsAwsjson11_` | v1.43.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| acmpca | acmpca | JSON-RPC 1.1 / `awsAwsjson11_` | v1.50.0 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| amplify | amplify | REST-JSON 1 / `awsRestjson1_` | v1.41.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| apigateway | apigateway | REST-JSON 1 / `awsRestjson1_` | v1.42.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| apigatewaymanagementapi | apigatewaymanagementapi | REST-JSON 1 / `awsRestjson1_` | v1.32.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| apigatewayv2 | apigatewayv2 | REST-JSON 1 / `awsRestjson1_` | v1.37.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| appconfig | appconfig | REST-JSON 1 / `awsRestjson1_` | v1.48.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| appconfigdata | appconfigdata | REST-JSON 1 / `awsRestjson1_` | v1.26.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| applicationautoscaling | applicationautoscaling | JSON-RPC 1.1 / `awsAwsjson11_` | v1.45.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| appmesh | appmesh | REST-JSON 1 / `awsRestjson1_` | v1.38.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| apprunner | apprunner | JSON-RPC 1.0 / `awsAwsjson10_` | v1.42.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| appstream | appstream | rpc-v2-cbor / `deserializeCBOR_*` (GENERATED, but under a non-`awsXxx_` scheme) | v1.64.5 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped | CORRECTED 2026-08-14: an earlier revision of this row called these functions hand-written. They are generated, present in the pinned SDK's own serializers.go and deserializers.go — just not under the `awsXxx_` prefix the other protocols use. What IS hand-rolled is gopherstack's own extraction off a `cbor.Map`, which is why this service is case-sensitive. |
| appsync | appsync | REST-JSON 1 / `awsRestjson1_` | v1.56.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| athena | athena | JSON-RPC 1.1 / `awsAwsjson11_` | v1.60.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| autoscaling | autoscaling | AWS Query (XML) / `awsAwsquery_` | v1.70.4 | Case-insensitive (`strings.EqualFold` on element/field name) | Silently dropped |  |
| awsconfig | configservice | JSON-RPC 1.1 / `awsAwsjson11_` | v1.68.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| backup | backup | REST-JSON 1 / `awsRestjson1_` | v1.59.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| batch | batch | REST-JSON 1 / `awsRestjson1_` | v1.68.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| bedrock | bedrock | REST-JSON 1 / `awsRestjson1_` | v1.66.4 | Case-sensitive (exact `case "Field":` / map-key match) | Also implements BedrockAgent-shaped operations in-package (CreateAgent, CreateKnowledgeBase, CreateAgentAlias, etc. — see handler_agents_dispatch_test.go), tested against the real `bedrockagent` SDK client. A separate services/bedrockagent directory *also* exists (own row below) — both are restjson1/case-sensitive so no protocol conflict, but the duplication is worth knowing about. |  |
| bedrockagent | bedrockagent | REST-JSON 1 / `awsRestjson1_` | v1.58.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| bedrockruntime | bedrockruntime | REST-JSON 1 / `awsRestjson1_` | v1.57.1 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| ce | costexplorer | JSON-RPC 1.1 / `awsAwsjson11_` | v1.67.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| cleanrooms | cleanrooms | REST-JSON 1 / `awsRestjson1_` | v1.49.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| cloudcontrol | cloudcontrol | JSON-RPC 1.0 / `awsAwsjson10_` | v1.32.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| cloudformation | cloudformation | AWS Query (XML) / `awsAwsquery_` | v1.76.1 | Case-insensitive (`strings.EqualFold` on element/field name) | Silently dropped |  |
| cloudfront | cloudfront | REST-XML / `awsRestxml_` | v1.67.4 | Case-insensitive (`strings.EqualFold` on element/field name) | Silently dropped |  |
| cloudfrontkeyvaluestore | cloudfrontkeyvaluestore | REST-JSON 1 / `awsRestjson1_` | v1.15.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| cloudtrail | cloudtrail | JSON-RPC 1.1 / `awsAwsjson11_` | v1.58.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| cloudwatch | cloudwatch | rpc-v2-cbor (Smithy schema-based) / `options.Protocol = rpcv2.NewCBOR(...)` (api_client.go:214; no generated deserializers.go) | v1.66.3 | Case-sensitive (gopherstack's own `rpcv2cbor.go` extracts fields via exact Go map lookup `m[key]` off a `cbor.Map`; confirmed via cloudwatch's own test comments, e.g. sdk_alarm_mute_rule_test.go:13-16) | Silently dropped | cloudwatch's whole client speaks rpc-v2-cbor exclusively (no JSON fallback at any version in go.mod). No standard awsAwsjson1x_/awsRestjson1_ functions exist to grep — this is the one service where the "grep the deserializer" recipe doesn't apply and hand-reading is mandatory. |
| cloudwatchlogs | cloudwatchlogs | JSON-RPC 1.1 / `awsAwsjson11_` | v1.81.1 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| codeartifact | codeartifact | REST-JSON 1 / `awsRestjson1_` | v1.41.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| codebuild | codebuild | JSON-RPC 1.1 / `awsAwsjson11_` | v1.72.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| codecommit | codecommit | JSON-RPC 1.1 / `awsAwsjson11_` | v1.36.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| codeconnections | codeconnections | JSON-RPC 1.0 / `awsAwsjson10_` | v1.13.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| codedeploy | codedeploy | JSON-RPC 1.1 / `awsAwsjson11_` | v1.38.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| codepipeline | codepipeline | JSON-RPC 1.1 / `awsAwsjson11_` | v1.49.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| codestarconnections | codestarconnections | JSON-RPC 1.0 / `awsAwsjson10_` | v1.38.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| cognitoidentity | cognitoidentity | JSON-RPC 1.1 / `awsAwsjson11_` | v1.36.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| cognitoidp | cognitoidentityprovider | JSON-RPC 1.1 / `awsAwsjson11_` | v1.67.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| comprehend | comprehend | JSON-RPC 1.1 / `awsAwsjson11_` | v1.43.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| databrew | databrew | REST-JSON 1 / `awsRestjson1_` | v1.42.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| datasync | datasync | JSON-RPC 1.1 / `awsAwsjson11_` | v1.61.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| dax | dax | JSON-RPC 1.1 / `awsAwsjson11_` | v1.32.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| detective | detective | REST-JSON 1 / `awsRestjson1_` | v1.41.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| directconnect | directconnect | JSON-RPC 1.1 / `awsAwsjson11_` | v1.44.1 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| directoryservice | directoryservice | JSON-RPC 1.1 / `awsAwsjson11_` | v1.41.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| dlm | dlm | REST-JSON 1 / `awsRestjson1_` | v1.39.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| dms | databasemigrationservice | JSON-RPC 1.1 / `awsAwsjson11_` | v1.66.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| docdb | docdb | AWS Query (XML) / `awsAwsquery_` | v1.51.4 | Case-insensitive (`strings.EqualFold` on element/field name) | Silently dropped |  |
| dynamodb | dynamodb | JSON-RPC 1.0 / `awsAwsjson10_` | v1.63.1 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| dynamodbstreams | dynamodbstreams | JSON-RPC 1.0 / `awsAwsjson10_` | v1.36.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| ec2 | ec2 | EC2 Query (XML) / `awsEc2query_` | v1.319.1 | Case-insensitive (`strings.EqualFold` on element/field name) | Silently dropped |  |
| ecr | ecr | JSON-RPC 1.1 / `awsAwsjson11_` | v1.60.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| ecs | ecs | JSON-RPC 1.1 / `awsAwsjson11_` | v1.90.0 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| efs | efs | REST-JSON 1 / `awsRestjson1_` | v1.44.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| eks | eks | REST-JSON 1 / `awsRestjson1_` | v1.90.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| elasticache | elasticache | AWS Query (XML) / `awsAwsquery_` | v1.56.4 | Case-insensitive (`strings.EqualFold` on element/field name) | Silently dropped |  |
| elasticbeanstalk | elasticbeanstalk | AWS Query (XML) / `awsAwsquery_` | v1.37.4 | Case-insensitive (`strings.EqualFold` on element/field name) | Silently dropped |  |
| elasticsearch | elasticsearchservice | REST-JSON 1 / `awsRestjson1_` | v1.45.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| elb | elasticloadbalancing | AWS Query (XML) / `awsAwsquery_` | v1.36.4 | Case-insensitive (`strings.EqualFold` on element/field name) | Silently dropped |  |
| elbv2 | elasticloadbalancingv2 | AWS Query (XML) / `awsAwsquery_` | v1.58.5 | Case-insensitive (`strings.EqualFold` on element/field name) | Silently dropped |  |
| emr | emr | JSON-RPC 1.1 / `awsAwsjson11_` | v1.64.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| emrserverless | emrserverless | REST-JSON 1 / `awsRestjson1_` | v1.44.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| eventbridge | eventbridge | JSON-RPC 1.1 / `awsAwsjson11_` | v1.48.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| firehose | firehose | JSON-RPC 1.1 / `awsAwsjson11_` | v1.46.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| fis | fis | REST-JSON 1 / `awsRestjson1_` | v1.40.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| forecast | forecast | JSON-RPC 1.1 / `awsAwsjson11_` | v1.44.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| fsx | fsx | JSON-RPC 1.1 / `awsAwsjson11_` | v1.68.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| glacier | glacier | REST-JSON 1 / `awsRestjson1_` | v1.35.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| glue | glue | JSON-RPC 1.1 / `awsAwsjson11_` | v1.152.0 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| grafana | grafana | REST-JSON 1 / `awsRestjson1_` | v1.38.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| guardduty | guardduty | REST-JSON 1 / `awsRestjson1_` | v1.85.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| iam | iam | AWS Query (XML) / `awsAwsquery_` | v1.58.1 | Case-insensitive (`strings.EqualFold` on element/field name) | Silently dropped |  |
| identitystore | identitystore | JSON-RPC 1.1 / `awsAwsjson11_` | v1.39.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| inspector2 | inspector2 | REST-JSON 1 / `awsRestjson1_` | v1.54.1 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| iot | iot | REST-JSON 1 / `awsRestjson1_` | v1.77.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| iotanalytics | iotanalytics | REST-JSON 1 / `awsRestjson1_` | v1.32.0 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| iotdataplane | iotdataplane | REST-JSON 1 / `awsRestjson1_` | v1.35.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| iotwireless | iotwireless | REST-JSON 1 / `awsRestjson1_` | v1.59.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| kafka | kafka | REST-JSON 1 / `awsRestjson1_` | v1.57.2 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| kinesis | kinesis | JSON-RPC 1.1 / `awsAwsjson11_` | v1.46.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| kinesisanalytics | kinesisanalytics | JSON-RPC 1.1 / `awsAwsjson11_` | v1.33.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| kinesisanalyticsv2 | kinesisanalyticsv2 | JSON-RPC 1.1 / `awsAwsjson11_` | v1.41.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| kms | kms | JSON-RPC 1.1 / `awsAwsjson11_` | v1.55.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| lakeformation | lakeformation | REST-JSON 1 / `awsRestjson1_` | v1.50.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| lambda | lambda | REST-JSON 1 / `awsRestjson1_` | v1.101.2 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| lightsail | lightsail | JSON-RPC 1.1 / `awsAwsjson11_` | v1.58.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| macie2 | macie2 | REST-JSON 1 / `awsRestjson1_` | v1.54.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| managedblockchain | managedblockchain | REST-JSON 1 / `awsRestjson1_` | v1.34.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| mediaconvert | mediaconvert | REST-JSON 1 / `awsRestjson1_` | v1.97.1 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| medialive | medialive | REST-JSON 1 / `awsRestjson1_` | v1.101.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| mediapackage | mediapackage | REST-JSON 1 / `awsRestjson1_` | v1.42.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| mediastore | mediastore | JSON-RPC 1.1 / `awsAwsjson11_` | v1.32.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| mediastoredata | mediastoredata | REST-JSON 1 / `awsRestjson1_` | v1.32.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| mediatailor | mediatailor | REST-JSON 1 / `awsRestjson1_` | v1.63.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| memorydb | memorydb | JSON-RPC 1.1 / `awsAwsjson11_` | v1.36.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| mgn | mgn | REST-JSON 1 / `awsRestjson1_` | v1.48.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| mq | mq | REST-JSON 1 / `awsRestjson1_` | v1.39.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| mwaa | mwaa | REST-JSON 1 / `awsRestjson1_` | v1.43.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| neptune | neptune | AWS Query (XML) / `awsAwsquery_` | v1.48.4 | Case-insensitive (`strings.EqualFold` on element/field name) | Silently dropped |  |
| networkmanager | networkmanager | REST-JSON 1 / `awsRestjson1_` | v1.44.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| networkmonitor | networkmonitor | REST-JSON 1 / `awsRestjson1_` | v1.16.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| omics | omics | REST-JSON 1 / `awsRestjson1_` | v1.49.5 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| opensearch | opensearch | REST-JSON 1 / `awsRestjson1_` | v1.75.4 | Case-sensitive (exact `case "Field":` / map-key match) | Also hosts OpenSearch **Serverless (AOSS)** operations (BatchGetCollection, CreateCollection, CreateSecurityPolicy, ...) via the separate `opensearchserverless` SDK client — see serverlessOperations() in handler_operations.go. No standalone services/opensearchserverless directory exists; see sub-row below. AOSS is JSON-RPC 1.0, unlike classic OpenSearch's REST-JSON 1. |  |
| opensearch *(Serverless/AOSS sub-client)* | opensearchserverless | JSON-RPC 1.0 / `awsAwsjson10_` | v1.34.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped | Second client hosted in the opensearch directory — see note on row above. |
| opsworks | (not in go.mod) | N/A — **no pinned SDK dependency** | N/A | N/A — cannot verify from a pinned client | N/A | Has 64 Go files of hand-written emulation code whose comments cite `aws-sdk-go-v2/service/opsworks@v1.31.0`, but that module is **not** in this repo's go.mod (`go list -m all` confirms absent) — the copy in GOMODCACHE is incidental/stale, not pinned by this project. Protocol cannot be taken "from the pinned client" as instructed because there is no pinned client. Classic AWS OpsWorks used JSON-RPC 1.1 per AWS docs, but that is an unverified inference, not from this repo's pin — do not treat it as authoritative. |
| organizations | organizations | JSON-RPC 1.1 / `awsAwsjson11_` | v1.53.5 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| outposts | outposts | REST-JSON 1 / `awsRestjson1_` | v1.66.1 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| personalize | personalize | JSON-RPC 1.1 / `awsAwsjson11_` | v1.50.4 | Case-sensitive (exact `case "Field":` / map-key match) | Also hosts Personalize **Runtime** operations (GetRecommendations, GetPersonalizedRanking) via the separate `personalizeruntime` SDK client — see handler.go dispatch table. No standalone services/personalizeruntime directory exists; see sub-row below. Runtime is REST-JSON 1, unlike classic Personalize's JSON-RPC 1.1. |  |
| personalize *(Runtime sub-client)* | personalizeruntime | REST-JSON 1 / `awsRestjson1_` | v1.36.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped | Second client hosted in the personalize directory — see note on row above. |
| pinpoint | pinpoint | REST-JSON 1 / `awsRestjson1_` | v1.42.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| pipes | pipes | REST-JSON 1 / `awsRestjson1_` | v1.26.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| polly | polly | REST-JSON 1 / `awsRestjson1_` | v1.60.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| qldb | (none) | N/A — no Go code at all | N/A | N/A | N/A | Directory contains only README.md, zero .go files. No emulator exists to have a protocol. |
| qldbsession | (none) | N/A — no Go code at all | N/A | N/A | N/A | Directory contains only README.md, zero .go files. No emulator exists to have a protocol. |
| quicksight | quicksight | REST-JSON 1 / `awsRestjson1_` | v1.123.1 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| ram | ram | REST-JSON 1 / `awsRestjson1_` | v1.39.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| rds | rds | AWS Query (XML) / `awsAwsquery_` | v1.124.1 | Case-insensitive (`strings.EqualFold` on element/field name) | Silently dropped |  |
| rdsdata | rdsdata | REST-JSON 1 / `awsRestjson1_` | v1.35.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| redshift | redshift | AWS Query (XML) / `awsAwsquery_` | v1.65.4 | Case-insensitive (`strings.EqualFold` on element/field name) | Also hosts **Redshift Serverless** operations via the separate `redshiftserverless` SDK client (handler_serverless_*.go). No standalone services/redshiftserverless directory exists; see sub-row below. This is the multi-protocol oddity named explicitly in the task: classic Redshift is AWS Query/XML, Serverless is JSON-RPC 1.1. |  |
| redshift *(Serverless/AOSS-style sub-client)* | redshiftserverless | JSON-RPC 1.1 / `awsAwsjson11_` | v1.38.5 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped | Second client hosted in the redshift directory — see note on row above. |
| redshiftdata | redshiftdata | JSON-RPC 1.1 / `awsAwsjson11_` | v1.43.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| rekognition | rekognition | JSON-RPC 1.1 / `awsAwsjson11_` | v1.54.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| resiliencehub | resiliencehub | REST-JSON 1 / `awsRestjson1_` | v1.38.3 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| resourcegroups | resourcegroups | REST-JSON 1 / `awsRestjson1_` | v1.36.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| resourcegroupstaggingapi | resourcegroupstaggingapi | JSON-RPC 1.1 / `awsAwsjson11_` | v1.35.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| rolesanywhere | rolesanywhere | REST-JSON 1 / `awsRestjson1_` | v1.26.3 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| route53 | route53 | REST-XML / `awsRestxml_` | v1.65.6 | Case-insensitive (`strings.EqualFold` on element/field name) | Silently dropped |  |
| route53resolver | route53resolver | JSON-RPC 1.1 / `awsAwsjson11_` | v1.48.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| s3 | s3 | REST-XML / `awsRestxml_` | v1.106.5 | Case-insensitive (`strings.EqualFold` on element/field name) | Silently dropped |  |
| s3control | s3control | REST-XML / `awsRestxml_` | v1.73.4 | Case-insensitive (`strings.EqualFold` on element/field name) | Silently dropped |  |
| s3tables | s3tables | REST-JSON 1 / `awsRestjson1_` | v1.18.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| sagemaker | sagemaker | JSON-RPC 1.1 / `awsAwsjson11_` | v1.263.2 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| sagemakerruntime | sagemakerruntime | REST-JSON 1 / `awsRestjson1_` | v1.43.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| scheduler | scheduler | REST-JSON 1 / `awsRestjson1_` | v1.20.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| secretsmanager | secretsmanager | JSON-RPC 1.1 / `awsAwsjson11_` | v1.44.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| securityhub | securityhub | REST-JSON 1 / `awsRestjson1_` | v1.75.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| serverlessrepo | serverlessapplicationrepository | REST-JSON 1 / `awsRestjson1_` | v1.33.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| servicediscovery | servicediscovery | JSON-RPC 1.1 / `awsAwsjson11_` | v1.43.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| ses | ses | AWS Query (XML) / `awsAwsquery_` | v1.37.4 | Case-insensitive (`strings.EqualFold` on element/field name) | Silently dropped |  |
| sesv2 | sesv2 | REST-JSON 1 / `awsRestjson1_` | v1.66.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| shield | shield | JSON-RPC 1.1 / `awsAwsjson11_` | v1.37.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| sns | sns | AWS Query (XML) / `awsAwsquery_` | v1.42.4 | Case-insensitive (`strings.EqualFold` on element/field name) | Silently dropped |  |
| sqs | sqs | JSON-RPC 1.0 / `awsAwsjson10_` | v1.46.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| ssm | ssm | JSON-RPC 1.1 / `awsAwsjson11_` | v1.73.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| ssoadmin | ssoadmin | JSON-RPC 1.1 / `awsAwsjson11_` | v1.43.1 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| stepfunctions | sfn | JSON-RPC 1.0 / `awsAwsjson10_` | v1.45.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| sts | sts | AWS Query (XML) / `awsAwsquery_` | v1.45.4 | Case-insensitive (`strings.EqualFold` on element/field name) | Silently dropped |  |
| support | support | JSON-RPC 1.1 / `awsAwsjson11_` | v1.34.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| swf | swf | JSON-RPC 1.0 / `awsAwsjson10_` | v1.37.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| textract | textract | JSON-RPC 1.1 / `awsAwsjson11_` | v1.43.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| timestreamquery | timestreamquery | JSON-RPC 1.0 / `awsAwsjson10_` | v1.39.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| timestreamwrite | timestreamwrite | JSON-RPC 1.0 / `awsAwsjson10_` | v1.38.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| transcribe | transcribe | JSON-RPC 1.1 / `awsAwsjson11_` | v1.58.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| transfer | transfer | JSON-RPC 1.1 / `awsAwsjson11_` | v1.75.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| translate | translate | JSON-RPC 1.1 / `awsAwsjson11_` | v1.36.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| verifiedpermissions | verifiedpermissions | JSON-RPC 1.0 / `awsAwsjson10_` | v1.36.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| vpclattice | vpclattice | REST-JSON 1 / `awsRestjson1_` | v1.25.5 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| waf | waf | JSON-RPC 1.1 / `awsAwsjson11_` | v1.33.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| wafv2 | wafv2 | JSON-RPC 1.1 / `awsAwsjson11_` | v1.77.3 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| workmail | workmail | JSON-RPC 1.1 / `awsAwsjson11_` | v1.39.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| workspaces | workspaces | JSON-RPC 1.1 / `awsAwsjson11_` | v1.73.1 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
| xray | xray | REST-JSON 1 / `awsRestjson1_` | v1.39.4 | Case-sensitive (exact `case "Field":` / map-key match) | Silently dropped |  |
