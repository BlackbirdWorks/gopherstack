# Required-response-member ranking

Built for gopherstack-r80d (fourth batch). **Three prior agents rebuilt the
sibling over-wide-List candidate list from scratch in-session** because that
scratch tooling lived only in a session scratchpad and did not survive
between sessions (gopherstack-569k / gopherstack-dv4s: "Scratch tooling
lived in a session scratchpad and will not survive"). This file and
`cmd/requiredoutputfields` exist so the same waste does not happen for the
required-OUTPUT-member cut too.

**A future batch should read this file, not rebuild it.** Regenerate only
to pick up new services, a go.mod version bump, or after resolving tooling
gaps noted below — and when you do, update the "already examined" table
below and re-run the ranking, don't just discard this file.

## What the count means, and what it doesn't

For every `services/<dir>`, this counts fields the **real AWS SDK** marks
`This member is required.` on an operation's `<Op>Output` struct, summed
across every op the service has. It is a measure of how much required
OUTPUT surface a service has to check — **not** a verdict on whether
gopherstack populates any of it. That is always a per-op hand read against
the handler; see the settled-services table below for what's actually been
verified.

Density is unpredictable and uncorrelated with op count or protocol:
cloudfront has 1 required output member across 167 ops, while route53 has
108 across 71. **The ranking is the only way to aim the remaining effort** —
op count alone is a poor proxy (quicksight has 277 ops but only 79 required
fields; pinpoint has 122 ops and 120 of them carry at least one required
field).

## Method

For each `services/<dir>`, resolve the pinned
`aws-sdk-go-v2/service/<mod>@<version>` from `go.mod` (directory name and
module name diverge for 9 services — see `dirModuleOverride` in
`cmd/requiredoutputfields/main.go`, the same table `cmd/overwidecandidates`
uses; go.mod also mixes a `require (...)` block with standalone
`require x v...` lines — both forms are matched). Read every
`api_op_<Op>.go` file from
`$(go env GOMODCACHE)/github.com/aws/aws-sdk-go-v2/service/<mod>@<version>`,
and for each `type <Op>Output struct { ... }` walk **blank-line-separated
top-level field blocks with brace-depth tracking** (so a nested struct's own
blank lines never split a block early — the same trap that would otherwise
descend into wrapper/nested types), flagging any field whose doc comment
contains the exact line `This member is required.`

This is a straight port of the input-side sweep's validated method
(gopherstack-569k), inverted from "is this field read" to "is this field
written" per gopherstack-r80d's framing — but this artefact only does the
first half (counting AWS's required surface); the "is it written" half is
the hand-read every batch still has to do per service.

## Validated three ways before being trusted

- **Known-answer positive**: kinesis `DescribeLimits` → exactly
  `OnDemandStreamCount, OnDemandStreamCountLimit, OpenShardCount,
  ShardLimit`. Reproduced exactly.
- **Known-answer negative**: kinesis `ListShards` → zero required output
  fields. Reproduced exactly (op doesn't even appear in the per-op detail).
- **Scale check** against the four settled-service counts named in
  gopherstack-r80d's brief: quicksight **79** (277 ops), iam **61** (176
  ops), securityhub **47** (116 ops), route53 **108** (71 ops), plus the
  explicitly-named cloudfront **1** (167 ops). All five reproduced exactly.
- **Exclusion check**: opsworks, qldb, qldbsession have no pinned
  `aws-sdk-go-v2` dependency and are correctly excluded (162 service dirs →
  159 resolved), matching gopherstack-569k's "OUT OF SCOPE" note for the
  input-side sweep.

## Known false-positive / undercount classes (carried over from prior r80d passes)

This artefact only counts AWS's declared-required surface; it says nothing
about gopherstack's own code. Once you're hand-reading a candidate service,
these are the shapes that have actually produced bugs so far (12 across the
first 8 settled services) — expect more of the same, not a new class:

- **Echo-the-request members.** A value AWS echoes back (e.g. `Marker`)
  conflated with an optional cursor field (`NextMarker`) that looks similar.
  Easy to lose precisely because the response *looks* complete without it —
  route53's four bugs were all this one shape.
- **Pagination tokens on single-page backends** — a required `NextToken`
  dropped because the backend never paginates, so nothing ever exercises the
  path that would need it.
- **A member with no struct field at all.** Grepping the field name finds
  nothing because it was never added to the wire struct — you must diff
  against the SDK shape, not search the handler (iam's
  `JobCompletionDate`).
- **Wrong response shape entirely** — the handler returns a sibling op's
  envelope instead of the real one (opensearch's `GetIndex`).
- **Empty-body success responses** — a 204 or empty body decodes as JSON EOF
  on the real client, not an error, so the call "succeeds" with a zero
  value instead of erroring (lambda's `DeleteCapacityProvider`).
- **Required-but-inapplicable means present-and-empty, not absent.** A
  required output field with nothing real to report should still be emitted
  as an empty string/slice, never omitted — omission is what breaks a real
  client. A prior pass got this backwards for quicksight and was reversed.
- **Disclosed non-fabrication stubs.** Where the backend genuinely lacks the
  data a required field would carry (e.g. an analytics engine gopherstack
  doesn't have), the correct fix is a documented PARITY.md disclosure, not a
  fabricated value. Three such members are disclosed so far (securityhub
  `GetInsightResults.ResultValues`, iam `EntityDetailsList`, opensearch
  `DescribeInsightDetails.Fields`).

## Regenerate

```
go run ./cmd/requiredoutputfields                # ranked summary (this table)
go run ./cmd/requiredoutputfields -json out.json # full per-op detail: op name -> required field names
```

No network access required — it only reads `go.mod` and the
already-downloaded module cache. Runs in a few seconds.

## Already examined for this bug class

Services below are **excluded from the ranked candidate table** — every
required output member across every op has been read end to end against
the handler, not grepped. Do not re-derive; read the referenced
commit/bd issue for detail. New entries should be added here (and removed
from the ranked table) as future batches clear more of it.

| service | required fields | ops | bugs found | ref |
|---|---:|---:|---:|---|
| kinesis | 17 | 39 | yes (2, +2 more from other pass) | gopherstack-r80d batch 1, be789761c |
| lambda | 40 | 85 | yes (1: `DeleteCapacityProvider` empty-body 204) | gopherstack-r80d batch 1 |
| cloudfront | 1 | 167 | 0 | gopherstack-r80d batch 1 |
| route53 | 108 | 71 | yes (4: all `Marker`) | gopherstack-r80d batch 2, cf129a24c |
| opensearch | 21 | 96 | yes (4: `GetIndex` wrong shape + 3x `NextToken`) | gopherstack-r80d batch 2, cf129a24c |
| securityhub | 47 | 116 | 0 (2 disclosed stubs, pre-existing) | gopherstack-r80d batch 3, 3840d77dd |
| iam | 61 | 176 | yes (1: `JobCompletionDate` structurally absent) | gopherstack-r80d batch 3, 3840d77dd |
| quicksight | 79 | 277 | yes (2: `ListSpaces`/`SearchSpaces` `SpaceId`, reversing a prior deliberate omission) | gopherstack-r80d batch 3, 3840d77dd |
| verifiedpermissions | 87 | 34 | 0 (clean; last hand-audited 2026-08-10 with an integration suite) | gopherstack-r80d batch 4 |
| grafana | 34 | 25 | 0 (clean; last hand-audited 2026-08-06 with an integration suite) | gopherstack-r80d batch 4 |
| identitystore | 25 | 19 | 0 (clean; last hand-audited 2026-07-25) | gopherstack-r80d batch 4 |
| pinpoint | 120 | 122 | yes (1: `DeleteUserEndpoints` empty-body 204) | gopherstack-r80d batch 5 |
| bedrock | 172 | 58 (ops with required fields) | yes (9: see batch-6 note below) | gopherstack-r80d batch 6 |
| resiliencehub | 94 | 55 (ops with required fields) | yes (2: `ListAppVersionResources`/`ListUnsupportedAppVersionResources`, same `resolutionId` omitempty bug) | gopherstack-r80d batch 6 |
| transfer | 69 | 52 (ops with required fields) | 0 (clean; already field-diffed against the pinned SDK multiple times, including a prior pass that fixed this exact bug class in the Start* op family) | gopherstack-r80d batch 6 |
| guardduty | 65 | 44 (ops with required fields) | 0 (clean; read end to end, one near-miss (`GetMemberDetectors`'s wire key genuinely is `"members"` despite the SDK's `MemberDataSourceConfigurations` Go field name -- confirmed via the deserializer's own key-switch before flagging, not just the Go struct name) | gopherstack-r80d batch 6 |
| omics | 182 | 40 | yes (4: `CreateAnnotationStore.VersionName`, `AnnotationStoreVersion.Id`/`Name` wire key, `MultipartReadSetUpload.ReferenceArn` omitempty, `VariantStore.SseConfig`) | gopherstack-r80d batch 7 |
| bedrockagent | 154 | 66 (ops with required fields) | yes (8: see batch-7 note below) | gopherstack-r80d batch 7 |
| cleanrooms | 88 | 83 (ops with required fields) | yes (5: `Membership`/`MembershipSummary.MemberAbilities`, `ConfiguredTable.AllowedColumns`/`AnalysisRuleTypes` + `ConfiguredTableSummary.AnalysisRuleTypes`, `ConfiguredTableAssociation.AnalysisRuleTypes`, `ConfiguredAudienceModelAssociationSummary.ConfiguredAudienceModelArn`, `PrivacyBudgetTemplate.AutoRefresh` -- see batch-8 note below) | gopherstack-r80d batch 8 |

19 services settled, 1671 required output fields read end to end. Batch 7
(omics + bedrockagent) added 12 more bugs (omics 4, bedrockagent 8) on top
of whatever the prior batches' running total was -- this file's own running
count was already internally inconsistent between "24" and the bd issue's
own comment stating "34" before this batch touched it; not reconciled here,
out of this batch's scope, count bugs from the per-service rows above if
you need an exact figure. Batch 8 (cleanrooms only) added 5 more bugs, all
proven via real-SDK-client round-trip tests with hand-revert/confirm-fail/
restore/md5sum verification -- see the batch-8 note below for detail.
Earlier history: 24 bugs found across the first 9
(per gopherstack-r80d's brief); verifiedpermissions,
grafana and identitystore (batch 4) came back clean; pinpoint (batch 5) added
one more of the empty-body-204 class first seen in lambda's
`DeleteCapacityProvider` (batch 1); bedrock (batch 6) added 9 more, all in
the AutomatedReasoningPolicy sub-resource family plus two one-off finds
(GetEvaluationJob, GetModelCopyJob) -- see below; resiliencehub (batch 6)
added 2 more, both the same `resolutionId` omitempty-on-required-field bug
in sibling List ops (`ListAppVersionResources`/
`ListUnsupportedAppVersionResources`) -- otherwise exceptionally clean: read
end to end, every other response shape already emitted required-but-empty
arrays correctly (`[]struct{}{}` with no omitempty), matching the standing
convention this campaign established; transfer (batch 6) came back clean,
all 52 ops with required fields read end to end (struct-tag sweep across
every `*Output`/`*Response` type plus direct handler reads for the
non-struct `map[string]any` responses) -- unsurprising, since a prior
general-parity pass had already fixed this exact bug class for this service
(the `StartOperations` family: `StartDirectoryListing` was missing the
required `OutputFileName`, `StartRemoteDelete`/`StartRemoteMove` used wrong
output key names) before this campaign reached transfer by name; guardduty
(batch 6) also came back clean, all 44 ops read end to end directly against
their handler functions (this service builds `map[string]any` responses
inline rather than through tagged structs, so the wire.go struct-tag sweep
technique doesn't apply -- every handler was read by hand instead). One
near-miss worth recording as a method note: `GetMemberDetectors`'s handler
emits the required field under the key `"members"`, which looks wrong
against the SDK's Go field name `MemberDataSourceConfigurations` at a
glance -- reading the real deserializer's key-switch (not just the Go
struct field name) confirmed the wire key genuinely is `"members"`,
so this was correctly not flagged. This is the input-side sweep's
established lesson applied to outputs: verify against the deserializer,
not the field name.

### bedrock (batch 6): 9 bugs, all outside the ops already hardened by prior parity passes

bedrock had already been through several general-parity passes (parity-4,
parity-5, gopherstack-lx5h, gopherstack-4sov, gopherstack-7znk,
gopherstack-ii4c, gopherstack-2wuv) that incidentally did required-output
field-diffing as part of routine wire-shape work, well before this campaign
reached bedrock by name. Read end to end (all 58 ops with required fields,
all 172 required members) against that backdrop -- most of the service was
already clean. The 9 real bugs found this batch cluster almost entirely in
one family: GetAutomatedReasoningPolicyBuildWorkflow/
ListAutomatedReasoningPolicyBuildWorkflows (dropped CreatedAt/UpdatedAt --
not tracked on the model at all), GetAutomatedReasoningPolicyAnnotations
(dropped 4 of 6 required members), GetAutomatedReasoningPolicyBuildWorkflowResultAssets
(dropped PolicyArn), GetAutomatedReasoningPolicyTestCase (wrong response
shape -- test case fields inlined instead of wrapped under the required
"testCase" key), Get/ListAutomatedReasoningPolicyTestResult(s) (wrong
response shape -- flat differently-keyed object instead of the required
"testResult"/nested-item wrapper), plus two unrelated one-offs:
GetModelCopyJob (dropped SourceAccountId, derived honestly from the
already-stored SourceModelArn's own account segment) and GetEvaluationJob
(OutputDataConfig had no wiring at all -- the "member with no struct field"
class). All 9 proven via real aws-sdk-go-v2 client round trips
(services/bedrock/wire_output_required_r80d_test.go) that fail against each
hand-reverted handler; see services/bedrock/PARITY.md's dated 2026-08-20
entries for full SDK file:line citations. Two adjacent, NOT-fixed findings
recorded as gaps rather than bugs (out of this cut's scope, need a union-type
parsing redesign): GetEvaluationJob's required JobType has no real-AWS-shaped
source data to derive it from, and CreateEvaluationJob's real
evaluationConfig/inferenceConfig are polymorphic unions that gopherstack's
own request parser cannot decode at all -- a real SDK client's
CreateEvaluationJob 400s today whenever it supplies real union content,
independent of this batch's fixes.

### bedrockagent (batch 7): 8 bugs, all in nested domain-struct required members the flat op-scan misses

154 required fields / 66 ops-with-required per `cmd/requiredoutputfields`,
but that count only reflects each op's own top-level `<Op>Output` struct.
bedrockagent's real wire shape is almost entirely "one wrapper key = the
whole nested domain object" (`{"agent": {...}}`, `{"agentCollaborator":
{...}}`, etc. -- the same structural pattern pinpoint's batch 5 found), so
the flat op-level scan undercounts the real required surface substantially.
Read all 66 ops end to end AND every domain struct (`Agent`, `AgentVersion`,
`AgentAlias`, `AgentCollaborator`, `AgentActionGroup`, `DataSource`,
`KnowledgeBase`, `Flow`, `FlowVersion`, `FlowAlias`, `Prompt`,
`PromptVersion`) plus their `*Summary` List-element siblings against their
real SDK types directly -- 6 of the 8 bugs found were only visible this way,
not from the tool's op list alone. All 8: `FlowVersion.RoleARN`
(`executionRoleArn`, missing field), `FlowSummary.Arn`/`CreatedAt` (missing
fields), `PromptVersion.UpdatedAt` (missing field), `AgentCollaborator`'s
`UpdatedAt` tagged the wrong wire key (`updatedAt` vs real `lastUpdatedAt`,
affecting Associate/Get/Update/ListAgentCollaborators), `AgentVersion`'s
`IdleSessionTTLInSeconds` (missing field) and `RoleARN` (wrongly
`omitempty`), `AgentVersionSummary.CreatedAt` (missing field),
`ActionGroupSummary.UpdatedAt` (missing field), `AgentAliasSummary.CreatedAt`/
`UpdatedAt` (missing fields). All proven via real `aws-sdk-go-v2/service/
bedrockagent` client round trips (`services/bedrockagent/
wire_output_required_r80d_test.go`), each hand-reverted/confirmed-failing/
restored, md5sum-verified byte-identical. See `services/bedrockagent/
PARITY.md`'s 2026-08-21 entries for full SDK file:line citations.

One finding NOT counted as a bug: `Agent.RoleARN`
(`agentResourceRoleArn`) was also wrongly `omitempty` on a required field,
structurally identical to the `ReferenceArn`-class bug fixed elsewhere in
this campaign (optional on the input, required on the output) -- but unlike
the counted bugs above, this one was left unproven: confirming a real SDK
client can actually trigger the omission (rather than some other codepath
always filling the value first) needs more than the round-trip technique
this campaign standardizes on, and was deprioritized given the batch's time
budget. Fixed anyway (harmless either way) but not claimed as proven --
see `services/bedrockagent/PARITY.md`'s Notes section for the full
reasoning.

### cleanrooms (batch 8): 5 bugs, all "required-but-reachably-empty" tagged omitempty or a missing field

88 required fields / 83 ops-with-required per `cmd/requiredoutputfields`, same
"one wrapper key = the whole nested domain object" wire shape as
pinpoint/bedrockagent, so the flat op count only reflects each op's own
top-level required member -- read all 83 ops AND every domain struct in
`models.go` (~36 types) against `types.go`'s `This member is required.`
annotations directly. Unlike bedrock/bedrockagent, this service had already
been through four prior passes doing real, cited SDK-deserializer field-diff
work (2026-07-24, 2026-08-07 kiqa, 2026-08-13 bv5d, 2026-08-14 dv4s) -- none
of them were looking for this specific bug class (a required field tagged
`omitempty` that is *genuinely reachable empty* from a real client, as
opposed to an invented field or a wrong response key), which is exactly what
this pass targeted and found. One of the five was already found and
explicitly deferred by the 2026-08-14 pass as "the opposite bug direction"
(`ConfiguredAudienceModelAssociationSummary.ConfiguredAudienceModelArn`) --
same "deferred is not fixed" lesson batch 7 already drew from omics/
bedrockagent, reapplied here. The other four:
`Membership`/`MembershipSummary.MemberAbilities` (empty when a collaboration
is created with an empty `creatorMemberAbilities` list -- a valid, Smithy-legal
state), `ConfiguredTable.AllowedColumns`/`AnalysisRuleTypes` +
`ConfiguredTableSummary.AnalysisRuleTypes` (empty before the first analysis
rule is attached -- a common window, not an edge case),
`ConfiguredTableAssociation.AnalysisRuleTypes` (same class), and
`PrivacyBudgetTemplate.AutoRefresh` (optional on input, defaulted to `NONE`
when unspecified rather than left as the empty zero value). A `removeFrom`
helper (`store.go`) had the same nil-on-empty-result bug on the delete-last-rule
path, fixed alongside the two `AnalysisRuleTypes` bugs rather than counted
separately. All 5 proven via real `aws-sdk-go-v2/service/cleanrooms` client
round trips (`services/cleanrooms/wire_output_required_r80d_test.go`),
hand-reverted/confirmed-failing/restored, md5sum-verified byte-identical. Two
already-documented gaps were re-confirmed as genuinely unreachable rather than
fixed (`Schema.Description`/`SchemaStatusDetails` -- no Create path for
schemas exists anywhere in this backend, confirmed via a repo-wide grep for
`b.schemas.Put`; `ProtectedQuerySummary`/`ProtectedJobSummary.ReceiverConfigurations`
-- already named in the pre-existing gaps list). See
`services/cleanrooms/PARITY.md`'s 2026-08-21 entries for full SDK file:line
citations and the "reviewed, not a bug" list of every other `omitempty`
required field checked and found safe (required-on-input too, so never
actually reachable-empty from a real client).

### Why pinpoint's density is 120/122 and not a new bug class

Read end to end (all 122 ops, all 120 required fields) — see
`services/pinpoint/PARITY.md`'s `ops:`/`families:` sections for the
per-op/per-family detail this table intentionally doesn't duplicate.
**pinpoint's near-universal density is a structural artefact of its Smithy
model, not evidence of many small bugs**: virtually every pinpoint
`<Op>Output` has exactly one top-level member (e.g. `ApplicationResponse`,
`EndpointsResponse`, `MessageBody`), and that member is *the entire HTTP
body* via an httpPayload-style binding — confirmed by reading the generated
op-level `HandleDeserialize` (not the `awsRestjson1_deserializeOpDocument*`
helper, which exists but is unused for the top-level op; e.g.
`deserializers.go:6928` is dead for this purpose) for `GetApp`
(`deserializers.go:6821-6852`, calls
`awsRestjson1_deserializeDocumentApplicationResponse` directly on the whole
decoded body, no wrapper key) and `DeleteUserEndpoints`
(`deserializers.go:5461-5482`, same pattern for `EndpointsResponse`). So the
per-op check collapses to one question — does the handler ever return a
non-body (empty/wrong-shape) success — not many per-op scalar checks the
way route53 or (going by field:op ratio) bedrock likely are. Confirmed
every other handler in the package writes a non-nil JSON body on every
success path (grepped every `WriteJSON`/`WriteHeader` call site in
`services/pinpoint/handler_*.go`); `DeleteUserEndpoints` was the sole
exception, matching the empty-body-204 class exactly (lambda's
`DeleteCapacityProvider`, batch 1). The Go SDK's decoder tolerates an empty
body as `io.EOF` (not an error), so the call "succeeds" with a nil pointer
where `EndpointsResponse` is required — confirmed via a real-client test
that fails against the un-reverted handler and passes against the fix
(`services/pinpoint/wire_output_required_r80d_test.go`).

## Ranked candidates (services not yet examined for this bug class)

89 services have >=1 required output field; 70 have zero (nothing to check
for this bug class — a List-only or delete-only service, or one whose ops
only declare optional output members). 159 of 162 service dirs resolved
against a pinned `aws-sdk-go-v2` module; opsworks/qldb/qldbsession excluded
(no SDK dependency). cleanrooms (88, settled batch 8) removed from this table
— see the "Already examined" table above.

```
 459  sagemaker                 ops=403  ops-with-required=188
  60  s3tables                  ops=49   ops-with-required=28
  55  codecommit                ops=79   ops-with-required=31
  54  stepfunctions             ops=37   ops-with-required=23
  44  apprunner                 ops=37   ops-with-required=32
  43  databrew                  ops=44   ops-with-required=41
  41  backup                    ops=109  ops-with-required=13
  38  inspector2                ops=81   ops-with-required=29
  37  vpclattice                ops=73   ops-with-required=16
  36  appmesh                   ops=38   ops-with-required=36
  35  amplify                   ops=37   ops-with-required=33
  34  glue                      ops=299  ops-with-required=17
  31  batch                     ops=45   ops-with-required=15
  30  ce                        ops=47   ops-with-required=18
  30  efs                       ops=31   ops-with-required=6
  30  swf                       ops=39   ops-with-required=17
  28  accessanalyzer            ops=39   ops-with-required=17
  27  cognitoidp                ops=129  ops-with-required=25
  25  emrserverless             ops=22   ops-with-required=14
  22  networkmonitor            ops=12   ops-with-required=7
  20  bedrockruntime            ops=11   ops-with-required=8
  18  cloudfrontkeyvaluestore   ops=6    ops-with-required=5
  18  sesv2                     ops=112  ops-with-required=13
  16  elasticsearch             ops=51   ops-with-required=12
  16  rolesanywhere             ops=30   ops-with-required=16
  15  awsconfig                 ops=102  ops-with-required=12
  15  codeconnections           ops=27   ops-with-required=14
  15  codestarconnections       ops=27   ops-with-required=14
  13  ses                       ops=71   ops-with-required=13
  12  athena                    ops=70   ops-with-required=8
  12  comprehend                ops=85   ops-with-required=6
  11  rekognition               ops=75   ops-with-required=5
  11  timestreamquery           ops=15   ops-with-required=7
  10  cloudformation            ops=90   ops-with-required=7
  10  emr                       ops=65   ops-with-required=6
   9  cognitoidentity           ops=23   ops-with-required=3
   9  kafka                     ops=64   ops-with-required=4
   8  firehose                  ops=12   ops-with-required=5
   7  autoscaling               ops=66   ops-with-required=5
   7  sqs                       ops=23   ops-with-required=4
   6  kinesisanalyticsv2        ops=33   ops-with-required=6
   6  mediastore                ops=21   ops-with-required=6
   6  mediatailor               ops=48   ops-with-required=4
   6  shield                    ops=36   ops-with-required=5
   6  ssoadmin                  ops=79   ops-with-required=6
   6  translate                 ops=19   ops-with-required=2
   5  mgn                       ops=95   ops-with-required=5
   5  redshiftdata              ops=12   ops-with-required=5
   5  scheduler                 ops=12   ops-with-required=5
   4  cloudwatch                ops=50   ops-with-required=3
   4  codepipeline              ops=44   ops-with-required=4
   4  kinesisanalytics          ops=20   ops-with-required=3
   4  lakeformation             ops=61   ops-with-required=3
   4  support                   ops=16   ops-with-required=4
   3  account                   ops=16   ops-with-required=2
   3  dynamodb                  ops=58   ops-with-required=3
   3  s3                        ops=112  ops-with-required=3
   3  s3control                 ops=97   ops-with-required=2
   3  timestreamwrite           ops=19   ops-with-required=3
   2  cloudtrail                ops=60   ops-with-required=1
   2  codeartifact              ops=48   ops-with-required=2
   2  sns                       ops=42   ops-with-required=2
   2  wafv2                     ops=59   ops-with-required=1
   1  acm                       ops=39   ops-with-required=1
   1  applicationautoscaling    ops=14   ops-with-required=1
   1  iotdataplane              ops=11   ops-with-required=1
   1  mediastoredata            ops=5    ops-with-required=1
   1  mwaa                      ops=12   ops-with-required=1
   1  sagemakerruntime          ops=3    ops-with-required=1
   1  waf                       ops=77   ops-with-required=1
```

Notes on the top of this table for the next batch:

- **sagemaker** (459, 403 ops) overlaps the ongoing gopherstack-oc9v
  conversion per gopherstack-569k's note for the input-side sweep — same
  caution likely applies here; check for an in-flight conversion before
  starting.
- **omics settled (batch 7)** — do not re-derive, see the settled-services
  table above and services/omics/PARITY.md's 2026-08-21 entries. The
  concurrent sibling agent's over-wide-List sweep this file previously
  warned about had already finished and committed by the time batch 7
  started (git status was clean).
- **bedrockagent settled (batch 7)** — do not re-derive, see the
  settled-services table above and services/bedrockagent/PARITY.md's
  2026-08-21 entries. **bedrock** (a different service, batch 6) — do not
  re-derive that either, see services/bedrock/PARITY.md's 2026-08-20 entries.
- **cleanrooms settled (batch 8)** — do not re-derive, see the
  settled-services table above and services/cleanrooms/PARITY.md's
  2026-08-21 entries. **s3tables** (60, 49 ops) is now the largest remaining
  single-service reading commitment after sagemaker — it already has an
  extensive prior wire-shape audit (see its own PARITY.md, `overall: A`,
  every op individually marked `wire: ok`, including a prior pass that fixed
  the exact "empty 204 instead of required {status,versionToken}" bug class
  in the replication family) but has NOT yet been checked specifically for
  this campaign's reachable-empty-tagged-omitempty class the way cleanrooms
  just was — don't assume clean without re-checking that specific angle.
- **pinpoint settled (batch 5)** — see the settled-services table above for
  why its 120/122 density was structural (single httpPayload-style body
  member per op), not many per-op scalar checks. Don't re-derive; one bug
  found (`DeleteUserEndpoints`).
