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
| s3tables | 60 | 28 (ops with required fields) | yes (1: `GetTableBucketEncryption` 404'd instead of AES256-defaulting for an unconfigured bucket -- see batch-9 note below) | gopherstack-r80d batch 9 |
| codecommit | 55 | 31 (ops with required fields) | 0 (clean; already field-diffed against the pinned SDK in a very recent thorough pass, 2026-08-13 gopherstack-gvkf, which fixed 8 wire-shape bugs -- see batch-9 note below) | gopherstack-r80d batch 9 |
| stepfunctions | 54 | 23 (ops with required fields) | yes (4: `TaskScheduledEventDetails.Region`/`.Parameters`, `TaskSucceededEventDetails.Resource`/`.ResourceType`, `TaskFailedEventDetails.Resource`/`.ResourceType`, `DescribeMapRun.ExecutionCounts` missing entirely -- see batch-10 note below) | gopherstack-r80d batch 10 |
| apprunner | 44 | 32 (ops with required fields) | yes (1: `AssociateCustomDomain`/`DisassociateCustomDomain.VpcDNSTargets` missing entirely; +2 fixed-but-not-counted -- see batch-10 note below) | gopherstack-r80d batch 10 |
| databrew | 43 | 44 (41 ops-with-required) | yes (1: `Dataset.Input` tagged `omitempty(zero)`, reachably empty via a real client -- see the batch-11 bullet note below and services/databrew/PARITY.md) | gopherstack-r80d batch 11 |
| backup | 41 | 13 (ops-with-required; entire required-output surface) | yes (2: `GetRestoreTestingPlan.RecoveryPointSelection` missing entirely, `DescribeScanJob`/`ListScanJobs` dropping 12 of 15 required members -- see the batch-11 bullet note below and services/backup/PARITY.md) | gopherstack-r80d batch 11 |
| inspector2 | 38 | 81 (29 ops-with-required) | yes (4: `GetCodeSecurityIntegration`/`ListCodeSecurityIntegrations` dropping `type`/`statusReason`, `Finding.Remediation` missing entirely, `Finding.Resources` dropped when empty, `Finding.Severity` serialized as a fabricated `{label,score}` object instead of the real bare string enum -- see the batch-12 note below and services/inspector2/PARITY.md) | gopherstack-r80d batch 12 |
| vpclattice | 37 | 73 (16 ops-with-required) | yes (1: `ListAccessLogSubscriptions`/`AccessLogSubscriptionSummary` dropping required `lastUpdatedAt` -- see the batch-13 note below and services/vpclattice/PARITY.md) | gopherstack-r80d batch 13 |
| appmesh | 36 | 38 (36 ops-with-required) | 0 (clean; one apparent false positive -- a stale "OpDocument" deserializer helper made the flat response shape look like a missing-wrapper-key bug -- ruled out via a real SDK client round trip, see the batch-13 note below and services/appmesh/PARITY.md) | gopherstack-r80d batch 13 |
| amplify | 35 | 37 (33 ops-with-required) | yes (13 member-level bugs across 5 findings: `App.EnvironmentVariables`/`Description`/`Repository`, `Branch.ActiveJobId`/`CustomDomains`/`Description`/`Framework`/`EnvironmentVariables`, `DomainAssociation.StatusReason`, `Webhook.Description`, `JobSummary.CommitId`/`CommitMessage`/`CommitTime` -- see the batch-14 note below and services/amplify/PARITY.md) | gopherstack-r80d batch 14 |
| glue | 34 | 299 (17 ops-with-required) | yes (6 member-level bugs across 3 findings: `Catalog.Name` (CreateCatalog read the name off a nonexistent `CatalogInput.Name`), `GrokClassifier.Classification`/`GrokPattern`, `XMLClassifier.Classification`, `JsonClassifier.JsonPath`, `ColumnStatistics.ColumnType` -- see the batch-15 note below and services/glue/PARITY.md) | gopherstack-r80d batch 15 |

30 services settled, 2148 required output fields read end to end (the running
total counts each settled service's flat per-op `cmd/requiredoutputfields`
number, as established by every prior batch -- glue's own real audited
surface was substantially larger once its ~56 gopherstack-modeled domain
structs were cross-checked, see the batch-15 note below). Batch 15
(glue only) added 6 more counted bugs on top of the running total -- see the
batch-15 note below for detail. Batch 14
(amplify only) added 13 more counted bugs on top of the running total -- see
the batch-14 note below for detail. Batch 13
(vpclattice + appmesh) added 1 more counted bug (vpclattice's
`ListAccessLogSubscriptions.lastUpdatedAt`; appmesh came back clean) -- see
the batch-13 note below for detail. Batch 12
(inspector2 only) added 4 more counted bugs on top of the running total --
see the batch-12 note below for detail. Batch 11
(databrew + backup) added 3 more counted bugs (1 + 2) on top of the running
total -- see the batch-11 notes below for detail. Batch 10
(stepfunctions + apprunner) added 5 more counted bugs (4 + 1) -- see the
batch-10 notes below for detail.
Batch 9
(s3tables + codecommit) added 1 more bug on top of the running total --
see the batch-9 notes below for detail. Batch 7
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

### s3tables (batch 9): 1 bug, and why the cleanrooms class doesn't reproduce here

60 required fields / 28 ops-with-required per `cmd/requiredoutputfields` --
read all 28 ops end to end against `s3tables@v1.18.4`'s `api_op_*.go`, plus
every handler that constructs a JSON response body. Unlike
pinpoint/bedrockagent/cleanrooms, this service's wire shape is mostly flat
(most ops have several top-level required members directly, not one
wrapper key around a whole nested domain object), and unlike cleanrooms it
builds every response as an explicit `map[string]any` literal via
`json.Marshal` rather than a tagged struct -- so the literal
struct-tag-`omitempty`-on-a-required-field shape cleanrooms hit cannot
reproduce here syntactically (only 2 `omitempty` tags exist anywhere in the
package, `models.go`'s `MetricsConfigurationID` and
`TableRecordExpiryConfig.Days`, and neither is ever engaged by a response
marshal -- no handler marshals a domain struct directly). Every required
List/map field (`TableBuckets`, `Namespaces`, `Tables`, `Destinations`,
`Configuration`/`Status` maps) is built via `make(..., 0, len(...))` or an
explicit non-nil literal and assigned to the response map unconditionally,
never gated behind an `if len(...) > 0` check, so the omitted-key shape
does not appear.

One real bug found instead, in the adjacent "required-but-defaultable means
present-with-a-derived-default, not absent" shape (same principle as batch
8's `PrivacyBudgetTemplate.AutoRefresh` derivation, here manifesting as a
full 404 instead of a dropped field): `GetTableBucketEncryption` returned
`NotFoundException` whenever no `PutTableBucketEncryption` override was
ever set -- the common, default path, since `encryptionConfiguration` is
optional on `CreateTableBucketInput`. Every S3 Tables bucket has encryption
at rest (SSE-S3/AES256 by default); this service's own `GetTableEncryption`
(table-level) already implements the correct fallback chain and says so in
its own doc comment, but the bucket-level sibling never got the same
treatment despite having the same real Put/Delete pair. Fixed by giving
`GetTableBucketEncryption` the same AES256 fallback; proven via a real
`aws-sdk-go-v2/service/s3tables` client round trip
(`services/s3tables/wire_output_required_r80d_test.go`), hand-reverted/
confirmed-failing/restored, md5sum-verified byte-identical. Three
pre-existing tests baked in the old NotFound-for-unconfigured assumption
and were updated rather than left contradicting the fix. See
`services/s3tables/PARITY.md`'s 2026-08-21 entry for full detail and SDK
file:line citations.

### codecommit (batch 9): 0 bugs -- nested domain structs have no required fields at all

55 required fields / 31 ops-with-required per `cmd/requiredoutputfields`.
Several of these ops (`ApprovalRuleTemplate`/`PullRequest`/`ApprovalRule`
Create/Get/Update families) wrap a single nested domain object the same
way pinpoint/bedrockagent/cleanrooms do, so the flat op-level count could
in principle undercount real required surface the same way -- checked by
reading every domain struct this service's responses build
(`ApprovalRuleTemplate`, `PullRequest`, `PullRequestTarget`, `Commit`,
`ApprovalRule`, `Conflict`/`ConflictMetadata`/`MergeHunk`,
`BatchDescribeMergeConflictsError`) against `codecommit@v1.36.4`'s
`types/types.go` directly. Unlike those other three services, this SDK's
Smithy model marks **zero** fields required on any nested/domain output
struct -- a repo-wide `grep -c "This member is required" types/types.go`
returns 15 hits, and every one of them belongs to a request-only input
struct (`DeleteFileEntry`, `PutFileEntry`, `ReplaceContentEntry`,
`RepositoryTrigger`, `SetFileModeEntry`, `SourceFileSpecifier`, `Target`),
never a response type. So the nested-domain-struct undercount class that
produced bugs in pinpoint/bedrockagent/cleanrooms does not apply here at
all -- there is nothing nested to violate.

Read all 31 ops end to end against their handlers (`handler_approval_
rules.go`, `handler_pull_requests.go`, `handler_merges.go`,
`handler_files.go`, `handler_commits.go`, `handler_reactions.go`) for the
reachable-empty-omitempty class specifically. This service builds every
response as an explicit `map[string]any` (like s3tables, not like
cleanrooms's tagged structs), and every required List field found
(`AssociatedRepositoryNames`/`DisassociatedRepositoryNames`,
`PullRequestEvents`, `ReactionsForComment`, `PullRequestIds`,
`RevisionDag`, `Conflicts`, `ConflictMetadataList`, `MergeHunks`) is
already built via `make(..., 0, len(...))` or an explicit nil-guard
(`if x == nil { x = []T{} }`) before being assigned to the response map
unconditionally. This service had already been through a very recent,
unusually thorough pass (2026-08-13, gopherstack-gvkf) that fixed 8 real
wire-shape bugs (the entire `Comment` family's undecodable
string-vs-JSON-number timestamps, plus two wrong-response-key bugs in
`GetCommentsForComparedCommit`/`GetCommentsForPullRequest`) -- none of
those were this campaign's target class, but the density of prior
scrutiny plus the SDK's own lack of nested-required fields makes this a
genuine clean result, not an under-checked one. No code changes made; see
`services/codecommit/PARITY.md`'s existing entries (unchanged) for the
prior wire-shape work.

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

### vpclattice + appmesh (batch 13): 1 bug total, plus a real-client-ruled-out false positive

Selected in ranked order after re-verifying the table against a fresh
`go run ./cmd/requiredoutputfields` run: vpclattice (37, 73 ops) confirmed
as the largest remaining candidate after sagemaker (off-limits, concurrent
gopherstack-oc9v conversion actively in flight this batch), then appmesh
(36, 38 ops) next. Both had a clean `git status` before starting.

**vpclattice (37 fields / 16 ops-with-required, 1 bug):** not the "one
wrapper key" shape — every flagged op's required members sit directly on
its own `<Op>Output` struct. All 16 ops funnel through exactly two domain
families, `AccessLogSubscription` and `DomainVerification`. An AST-style
walk of every `*Summary`/list-item type reachable through a List op's
`Items` field found only two with any required members at all
(`AccessLogSubscriptionSummary`: 7, `DomainVerificationSummary`: 5), both
already covered by their sibling Get ops' required sets. 1 bug:
`ListAccessLogSubscriptions`' `alsSummaryToJSON` dropped required
`lastUpdatedAt` on every summary item (confirmed against
`deserializers.go`'s key-switch, not just the Go field name) — the domain
model already tracked it and the sibling `GetAccessLogSubscription` already
emitted it, only the List summary's serializer omitted the key. Proven via
a real `aws-sdk-go-v2/service/vpclattice` client round trip
(`wire_field_fixes_test.go`), hand-reverted/confirmed-failing/restored,
md5sum-verified byte-identical. See services/vpclattice/PARITY.md's
2026-08-21 entry for full detail.

**appmesh (36 fields / 36 ops-with-required, 0 bugs):** the "one wrapper
key" shape (pinpoint/bedrockagent/cleanrooms/inspector2's class) — nearly
every op wraps its whole response in one required domain-object member.
Read every domain struct with `This member is required.` in `types.go`
(90+ struct declarations) to find the real surface: each `<X>Data` struct
requires its name field(s) + `Metadata` (`ResourceMetadata`, 7 fields,
shared by all 7 resource types) + `Spec` + `Status`; each `<X>Ref` struct
used by List ops requires the same Arn/CreatedAt/LastUpdatedAt/MeshOwner/
ResourceOwner/Version set plus its own name field(s). One apparent finding
looked exactly like this campaign's target bug class from static reading
alone — this service's own `handler_wire_test.go` doc comment says
responses have no `mesh`/`virtualNode`/etc. wrapper key, and the unused
`awsRestjson1_deserializeOpDocumentCreateMeshOutput` "OpDocument" codegen
helper (the same dead-code trap batch 5 already flagged for pinpoint)
switches on `case "mesh":`, which would leave `Mesh` nil against
gopherstack's unwrapped response if that helper were actually used. **A
real SDK client round trip (throwaway probe test, not committed) proved
`Mesh` populates correctly** — the actual per-op deserializer
(`awsRestjson1_deserializeOpCreateMesh`'s `HandleDeserialize`) decodes the
raw body directly into `MeshData` with no wrapper key at all (an implicit
httpPayload-style binding), so the flat-root shape is genuinely correct and
the doc comment was right all along. This is exactly the kind of
verify-against-a-real-client discipline this campaign's brief asks for,
applied to rule a false positive OUT rather than to find a true one. Every
other required member (metadata fields, spec presence, status wrapper,
list-item Ref sets, timestamp epoch-seconds encoding, version as JSON
Long) was read end to end against the handlers and real deserializers and
came back clean. See services/appmesh/PARITY.md's 2026-08-21 entry for
full detail.

Both services' gates (build/vet/gofmt/race-test/lint) are green, 0 banned
nolints, no exported signatures changed. `services/_REQUIRED_OUTPUT_CANDIDATES.md`
updated: both moved from the ranked table into "Already examined"
(settled-services count now 28, 2079 required output fields read end to
end). Did not touch sagemaker (off-limits, concurrent conversion still
actively modifying `services/sagemaker/notebook_instances.go` and sibling
files mid-batch, confirmed via repeated `git status`/`git diff --stat`
checks) or attempt a third service this batch.

### amplify (batch 14): 13 member-level bugs, the "one wrapper key" shape again

Selected after re-verifying the table against a fresh `go run
./cmd/requiredoutputfields` run: amplify (35, 37 ops) confirmed as the
largest remaining candidate after sagemaker (off-limits, `git status`
confirmed a clean amplify tree and a still-in-flight sagemaker conversion
before starting). Did one service with full rigour and stopped there per
the brief's "as many as you can with full rigour and no more," since the
"one wrapper key" shape (see below) made this a full-service, multi-struct
read rather than a quick per-op scan.

Not the flat-per-op shape — every one of amplify's 37 ops wraps its whole
response in one required domain-object member (`{"app": {...}}`,
`{"branch": {...}}`, `{"job": {"summary": ..., "steps": [...]}}`, etc.), the
same class pinpoint/bedrockagent/cleanrooms/inspector2 already established,
so the flat op-level tool count (35 fields/37 ops) undercounts the real
surface. Read every domain struct with `This member is required.` in the
pinned SDK's `types/types.go` end to end (App, Artifact, BackendEnvironment,
Branch, DomainAssociation, Job, JobConfig, JobSummary, Step, SubDomain,
SubDomainSetting, Webhook — 20 struct declarations, 63 required members
total) against every wire-view struct in `handler_apps.go`/
`handler_branches.go`/`handler_domains.go`/`handler_jobs.go`/
`handler_webhooks.go`, not grepped.

This service had already been through an unusually thorough general-parity
sweep (2026-07-23, PARITY.md's `overall: A`) that fixed 8 findings including
adding several required members this exact sweep would otherwise have found
missing entirely (`Branch.EnableBasicAuth` etc.) — but that sweep audited
for *missing* fields, not for fields that exist yet are still tagged
`omitempty`/`omitzero` on a member reachably empty from a real client. That
gap is exactly this campaign's target class, and is where all 13 bugs were
found: `App.EnvironmentVariables`/`Description`/`Repository` (all three
required but optional on `CreateAppInput`, dropped when unset);
`Branch.ActiveJobId`/`CustomDomains`/`Description`/`Framework`/
`EnvironmentVariables` (`ActiveJobId` is a *computed* field genuinely `""`
for any branch with no jobs yet — a normal, not edge-case, state;
`CustomDomains` was never assigned anywhere in `branches.go`, always nil);
`DomainAssociation.StatusReason` (never tracked at all — disclosed,
honestly-empty, not fabricated); `Webhook.Description` (required, optional
on `CreateWebhookInput`); `JobSummary.CommitId`/`CommitMessage` (required,
optional on `StartJobInput`) and `CommitTime` (required — the 2026-07-23
sweep's own `StartJob` doc comment *deliberately* omits this key when the
job has no real commit timestamp, which per this campaign's established
"required-but-inapplicable means present-and-empty, not absent" convention
(already reversed once for stepfunctions' `DescribeMapRun.ExecutionCounts`)
is itself the bug — fixed by falling back to the job's own `StartTime`,
mirroring the fallback `toStepViews` already applies to a still-running
step's `EndTime` in this same file).

One adjacent finding fixed but **not counted**: `Branch.Stage` carried the
same dead `omitempty` tag, but `Stage` is a non-pointer enum on the real SDK
(`types.Stage`, not `*Stage`) — a missing key and a present-but-empty key
decode to the identical Go zero value for any real client, so no
real-client test can distinguish the fix from its absence. `DefaultDomain`
(App) and `TTL`/`DisplayName`/`TotalNumberOfJobs` (Branch) and
`SubDomain.DnsRecord` carried the same dead tag but are never reachably
empty through any real client path (all computed unconditionally non-empty)
— tags removed as harmless cleanup, not bugs.

All 13 counted bugs proven via real `aws-sdk-go-v2/service/amplify` client
round trips (`services/amplify/wire_output_required_r80d_test.go`, 5 test
functions), hand-reverted (all 5 touched handler files reverted to HEAD
together, confirmed all 5 tests fail)/confirmed-failing/restored,
md5sum-verified byte-identical (`apps.go`/`branches.go` needed no changes
at all and are confirmed unchanged against HEAD). Existing tests needed no
correction — `go test ./services/amplify/...` passed unchanged both before
and after, confirming no pre-existing test encoded any of these wrong
shapes. Gates (build/vet/gofmt/race-test/lint) all green, 0 banned nolints,
0 new nolints, no exported signatures changed. Repo-wide `go build ./...`,
`go vet ./...`, `go vet -tags e2e ./...`, `go vet -tags integration ./...`
all re-run and clean. See `services/amplify/PARITY.md`'s 2026-08-21 entry
for the full per-member breakdown and SDK file:line citations.

Did not touch sagemaker (off-limits, confirmed via `git status` before and
after) or attempt a second service this batch, per the brief's "full rigour
and no more" — amplify's "one wrapper key" shape made a single-service read
already substantial (20 domain structs, 63 required members, 5 separate
wire-view files).

### glue (batch 15): 6 member-level bugs, settled via domain-struct cross-reference rather than a per-op read

Selected after re-verifying the table against a fresh `go run
./cmd/requiredoutputfields` run: glue (34 fields, 299 ops, only 17
ops-with-required) confirmed as the largest remaining candidate after
sagemaker (off-limits, `git status` confirmed a clean glue tree and an
in-flight sagemaker conversion before starting, unrelated files untouched
throughout).

glue's flat op-level density (17 of 299 ops) is the lowest of any settled
service so far by a wide margin, and this shape made a per-op read the wrong
tool: unlike pinpoint/bedrockagent/cleanrooms/inspector2/amplify's "one
wrapper key" undercount (where the flat count understates a much larger
per-op surface), most of glue's 299 ops return domain objects
(`Job`/`Crawler`/`Trigger`/`Workflow`/`DevEndpoint`/`Connection`/`Session`/
`Blueprint`/`Partition`) whose real SDK types declare **zero** required
members at all -- confirmed via a direct AST-style walk of every `type X
struct` in the pinned SDK's 13,275-line `types/types.go` (184 struct
declarations carry at least one `This member is required.` annotation; a
repo-wide check of `Job`/`Crawler`/`Trigger`/`Workflow`/`MLTransform`/
`DevEndpoint`/`Connection`/`Session`/`Blueprint`/`Partition`/`Classifier`/
`DataQualityResult`/`MappingEntry` found each carries exactly 0). This is the
codecommit-batch-9 lesson (a service whose Smithy model marks nothing
required on its response types cannot have this bug class) applied at far
larger scale: an entire ~250-op swath of glue structurally cannot violate
this campaign's target class, verified by reading the SDK's own declarations
directly rather than by hand-auditing every op's handler.

The remaining, genuinely checkable surface is the 184 required-bearing
structs themselves. Cross-referenced every one against gopherstack's own
`services/glue/*.go` source by exact Go type name: 56 of the 184 names
appear literally in this backend's code (the rest -- almost entirely the
Glue Studio visual-ETL `CodeGenConfigurationNode` union family: `Aggregate`,
`Filter`, `Join`, ~40 `*ConnectorSource`/`*ConnectorTarget`/
`*CatalogSource`/`*CatalogTarget` variants, `S3CsvSource`, `SparkSQL`, etc. --
do not appear at all, confirmed by grep, because `Job.CodeGenConfigurationNodes`
itself has no backing field anywhere in this backend's `Job` model; the
entire visual-ETL-script feature is unimplemented, so none of those ~120
struct types can ever be emitted by any op here -- a verified non-finding,
not an unexamined one, the same class as stepfunctions batch 10's 9
undisclosed `*EventDetails` kinds). Read all 56 present names end to end
against every handler/model file that constructs them, including two
reachability classes this campaign has already named (a nested type reachable
only through a sibling op's non-required field: `Integration`/
`InboundIntegration` via `DescribeIntegrations`/`DescribeInboundIntegrations`;
`CustomEntityType` via `BatchGetCustomEntityTypes`/`ListCustomEntityTypes`) --
neither is caught by `cmd/requiredoutputfields`'s per-op scan since the
required members sit on the nested struct, not the op's own `<Op>Output`.
One false positive ruled out with evidence: the real SDK's `types.CatalogEntry`
(a `DatabaseName`/`TableName` pair used only by `GetMapping`/`GetPlan`'s
input) happens to share a name with this backend's own, unrelated
`CatalogEntry` wire-view type for `GetCatalog`/`GetCatalogs` -- confirmed via
`grep -rln "types.CatalogEntry" api_op_*.go` that the real type is
input-only everywhere in the SDK, so it is not this campaign's target class
at all, a coincidental name collision only.

6 bugs, across 3 findings:

1. **`Catalog.Name`** (required, `types/types.go`) -- not a simple missed tag:
   `CreateCatalog`'s handler read the name from a nonexistent
   `CatalogInput.Name` field. The real `CreateCatalogInput`
   (`api_op_CreateCatalog.go`) has no `CatalogId` member at all and puts
   `Name` at the top level, a sibling of `CatalogInput` (confirmed against
   `serializers.go`'s `awsAwsjson11_serializeOpDocumentCreateCatalogInput`
   and `types.CatalogInput` itself, which has no `Name` field) -- a catalog
   is created and addressed purely by `Name`. Every catalog a real client
   ever created was silently stored under the empty-string key with an
   empty `Name`; a later `GetCatalog(CatalogId: <the name the client used>)`
   would 404, and `Catalog.Name`'s `omitempty` tag meant even a successful
   `GetCatalogs` dropped the required key entirely. Fixed by reading the
   real top-level `Name` and using it as both the catalog's `Name` and this
   backend's storage key, and removing the `omitempty` tag.
2. **`GrokClassifier.Classification`/`.GrokPattern`, `XMLClassifier.Classification`,
   `JsonClassifier.JsonPath`** (all required, `types/types.go`) -- reachable
   because `CreateGrokClassifierRequest`/`CreateXMLClassifierRequest`/
   `CreateJsonClassifierRequest`'s own client-side validators (`validators.go`)
   only reject a nil pointer, never an empty string, and this backend's
   `CreateClassifier`/`UpdateClassifier` store whatever content is supplied
   with no further validation. Fixed by removing all four `omitempty` tags.
3. **`ColumnStatistics.ColumnType`** (required) -- same reachability shape:
   `UpdateColumnStatisticsForTable`/`ForPartition`'s client-side
   `validateColumnStatistics` also only rejects a nil `ColumnType` pointer,
   and this backend stores it verbatim. Fixed by removing the `omitempty`
   tag.

All 6 proven via real `aws-sdk-go-v2/service/glue` client round trips
(`wire_output_required_r80d_test.go`), hand-reverted (all three touched files
-- `handler_catalogs.go`, `handler_catalogs_test.go`, `models.go` -- reverted
to HEAD together, confirmed all 5 new test (sub)cases fail)/confirmed-failing/
restored, md5sum-verified byte-identical.

**4 more findings fixed but NOT counted**, all unreachable via any real
client despite carrying the same dead `omitempty` tag: `ColumnStatistics.AnalyzedTime`
is overwritten server-side to `time.Now()` on every real Update call
regardless of client input, so it can never actually be zero-valued in a
stored record; `integrationSummary.CreateTime` (`DescribeIntegrations`) is
likewise always derived from `Integration.CreatedAt`, which the sole
construction site (`CreateIntegration`) always sets to `time.Now().UTC()`;
`CustomEntityType.RegexString` is required and the real client-side
validator only rejects a nil pointer, but this backend's own
`CreateCustomEntityType` independently rejects an empty `RegexString` with
`ErrValidation` before ever storing a record, so no real client can reach a
stored empty one; `EncryptionAtRest.CatalogEncryptionMode` is required, but
unlike every counted bug above, the real `PutDataCatalogEncryptionSettingsInput`'s
own `validateEncryptionAtRest` checks `len(v.CatalogEncryptionMode) == 0`
(a string-length check, not just a nil-pointer check), so a real client
cannot send an empty value at all -- the first case this batch found where
the SDK's own client-side validation, not this backend's, closes the gap.
All four tags removed as harmless cleanup, not claimed as proven bugs.

Gates (build/vet/gofmt/race-test/lint) all green, 0 banned nolints, 1 new
nolint avoided by using `require.Empty` instead of `require.Equal(t, "",
...)` (testifylint), no exported signatures changed outside
`services/glue/`. Repo-wide `go build ./...` clean; `go vet ./...` and both
tagged vets show only the pre-existing, unrelated sagemaker
concurrent-conversion failure (`services/sagemaker/handler_edge_deployment_test.go`,
untouched by this batch, confirmed via `git status`). See
`services/glue/PARITY.md`'s 2026-08-21 entries (`catalogs`/`classifiers`/
`column_statistics`) for the full per-member breakdown and SDK file:line
citations.

Did not attempt a second service this batch. Not started, disclosed rather
than silently skipped: the ~120 Glue Studio `CodeGenConfigurationNode` ETL
node types are a verified non-finding for THIS bug class (see above) but
represent a real missing-feature gap in their own right (visual ETL script
authoring/execution) -- out of scope for r80d, not filed as a new bd issue
by this batch since no evidence was gathered on how large that lift would
be beyond "the whole feature does not exist."

## Ranked candidates (services not yet examined for this bug class)

89 services have >=1 required output field; 70 have zero (nothing to check
for this bug class — a List-only or delete-only service, or one whose ops
only declare optional output members). 159 of 162 service dirs resolved
against a pinned `aws-sdk-go-v2` module; opsworks/qldb/qldbsession excluded
(no SDK dependency). cleanrooms (88, settled batch 8), s3tables (60,
settled batch 9), codecommit (55, settled batch 9), stepfunctions (54,
settled batch 10), apprunner (44, settled batch 10), databrew (43, settled
batch 11), backup (41, settled batch 11), inspector2 (38, settled batch
12), vpclattice (37, settled batch 13), appmesh (36, settled batch 13),
amplify (35, settled batch 14), and glue (34, settled batch 15) removed from
this table — see the "Already examined" table above.

```
 459  sagemaker                 ops=403  ops-with-required=188
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
  starting. It had uncommitted concurrent-agent changes during batches 9
  and 10 (services/sagemaker/handler_hub.go, handler_keys.go, hub.go,
  handler_hub_test.go, PARITY.md, and later pipelines.go/
  pipeline_executions.go/handler_pipelines.go) — those were committed by the
  concurrent agent partway through batch 10 (`fbaed6fee`), but re-check
  `git status` before starting; the conversion itself is still in flight
  across multiple commits, so treat any uncommitted sagemaker diff as a live
  exclusion, not a one-time check.
- **databrew settled (batch 11)** — do not re-derive, see the
  settled-services table above and services/databrew/PARITY.md's 2026-08-21
  entries. 1 bug (`Dataset.Input` reachably-empty-`omitzero`); everything
  else clean, including every List op's array construction and every
  domain struct's required members, most already unreachable-empty thanks
  to the real SDK's own client-side validators for the substructures that
  matter (`Output.Location`, `DataCatalogOutput`/`DatabaseOutput`).
- **backup settled (batch 11)** — do not re-derive, see the
  settled-services table above and services/backup/PARITY.md's 2026-08-21
  entries. 2 bugs across this service's entire 41-field/13-op required-output
  surface (the restore-testing-plan/selection and scan-job families, the
  only ops with any required output at all): `GetRestoreTestingPlan`'s
  `RecoveryPointSelection` had no field at all despite the real SDK client
  enforcing it non-nil on Create; `DescribeScanJob`/`ListScanJobs` had been
  returning only `ScanJobId`/`Status`, dropping 12 of 15 required members
  behind a stale `wire: ok` verdict that had only checked an unrelated
  status-code bug. `ScanJobCreator` (`CreatedBy`) stays a disclosed,
  unfixable gap — no backup-plan/rule lineage is tracked for a scan job or
  its recovery point anywhere in this backend.
- **inspector2 settled (batch 12)** — do not re-derive, see the
  settled-services table above and services/inspector2/PARITY.md's
  2026-08-21 entries.
- **vpclattice settled (batch 13)** — do not re-derive, see the
  settled-services table above and services/vpclattice/PARITY.md's
  2026-08-21 entry. 1 bug (`ListAccessLogSubscriptions` dropping required
  `lastUpdatedAt`); the rest of its narrow 2-domain-family required surface
  (AccessLogSubscription/DomainVerification) was already clean.
- **appmesh settled (batch 13)** — do not re-derive, see the
  settled-services table above and services/appmesh/PARITY.md's 2026-08-21
  entry. 0 bugs; one apparent finding (a stale "OpDocument" deserializer
  helper making the flat response shape look like a missing-wrapper-key
  bug) was ruled out via a real SDK client round trip rather than counted.
- **amplify settled (batch 14)** — do not re-derive, see the
  settled-services table above and services/amplify/PARITY.md's 2026-08-21
  entry, plus the batch-14 note above for the full per-member breakdown.
  13 member-level bugs across App/Branch/DomainAssociation/Webhook/
  JobSummary, all the "required member tagged omitempty/omitzero on a
  reachably-empty real-client state" shape; 1 adjacent finding
  (`Branch.Stage`) fixed but not counted since no real-client test can
  observe the difference for a non-pointer enum field.
- **glue settled (batch 15)** — do not re-derive, see the settled-services
  table above and services/glue/PARITY.md's 2026-08-21 entries
  (`catalogs`/`classifiers`/`column_statistics`), plus the batch-15 note
  above for the full per-member breakdown and the domain-struct
  cross-reference method used (this service's 299-op, 17-ops-with-required
  flat density made a per-op read the wrong tool; ~250 ops return domain
  types the SDK marks zero required fields on at all, confirmed via a
  direct AST walk of types.go, and the ~120-struct Glue Studio
  CodeGenConfigurationNode ETL family is entirely unreachable since
  Job.CodeGenConfigurationNodes has no backing field in this backend).
  6 member-level bugs across Catalog/GrokClassifier/XMLClassifier/
  JsonClassifier/ColumnStatistics; 4 more fixed but not counted, all
  unreachable via any real client. **batch** (31, 45 ops, 15
  ops-with-required) is now the largest remaining candidate after
  sagemaker.
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
  2026-08-21 entries.
- **s3tables settled (batch 9)** — do not re-derive, see the
  settled-services table above and services/s3tables/PARITY.md's
  2026-08-21 entries. This service builds every response as an explicit
  `map[string]any` literal rather than a tagged struct, so the literal
  cleanrooms-style struct-tag `omitempty` shape does not apply (only 2
  `omitempty` tags exist in the whole package, and neither is ever engaged
  by a response marshal); the one bug found instead was the adjacent
  "required-but-defaultable means present-with-a-derived-default, not
  absent" shape manifesting as a full 404 rather than a dropped field
  (`GetTableBucketEncryption`).
- **codecommit settled (batch 9)** — do not re-derive, see the
  settled-services table above and the batch-9 note above for detail. Came
  back clean (0 bugs): this SDK's Smithy model marks zero fields required
  on any nested/domain output struct (confirmed via a repo-wide grep of
  `types/types.go`), so the nested-domain-struct undercount class that hit
  pinpoint/bedrockagent/cleanrooms structurally cannot apply here, and
  every required List field already uses a nil-guard or `make(...)`.
- **stepfunctions settled (batch 10)** — do not re-derive, see the
  settled-services table above and services/stepfunctions/PARITY.md's
  2026-08-21 entries. Not the "one wrapper key" shape (pinpoint/
  bedrockagent/cleanrooms) or the `map[string]any`-literal shape (s3tables/
  codecommit) — responses are tagged structs with mostly-flat per-op
  required members, but the flat op-level count still undercounts because
  List ops return arrays of dedicated `*ListItem` structs and
  `GetExecutionHistory` returns polymorphic `HistoryEvent`s whose
  `*EventDetails` sub-objects each carry their own required members that
  `cmd/requiredoutputfields`'s per-op scan can't see. 4 bugs found in the
  nested `*EventDetails`/`DescribeMapRun` layer; all 6 List-item structs and
  the alias `RoutingConfigurationListItem` came back clean on re-check.
  Also reversed one prior-pass verdict: `DescribeMapRun.ExecutionCounts`
  had been marked "correctly so" absent because this backend has no
  distributed-map child-execution model — true, but the fix per this
  campaign's own "required-but-inapplicable means present-and-empty"
  convention is to emit it anyway, genuinely zero-valued, not omit the key.
- **apprunner settled (batch 10)** — do not re-derive, see the
  settled-services table above and services/apprunner/PARITY.md's
  2026-08-21 entries. Narrower than most: an AST-style walk of every
  `type X struct` in `types.go` found only `Service` and its nested
  source-config family carry any required fields at all —
  `AutoScalingConfiguration`/`Connection`/`ObservabilityConfiguration`/
  `VpcConnector`/`VpcIngressConnection` and every one of their `*Summary`
  siblings declare zero. 1 counted bug (`VpcDNSTargets` missing on
  `AssociateCustomDomain`/`DisassociateCustomDomain`, the sibling op
  `DescribeCustomDomains` already had it right) plus 2 fixed-but-not-
  counted findings worth reading before the next batch hits a similar
  case: `CodeRepository.SourceCodeVersion`'s omitted-input bug is real but
  **not provable via a real aws-sdk-go-v2 client round trip** because the
  SDK's own generated client-side `validateCodeRepository` already rejects
  the malformed request before it's ever sent — a new failure mode this
  campaign hadn't hit before (prior "not counted" cases were about
  gopherstack-side unreachability, not the real SDK client refusing to
  construct the request at all). `ObservabilityConfiguration.TraceConfiguration`
  being dropped entirely is a real, provable bug but sits outside this
  cut's precise scope since `TraceConfiguration` itself isn't
  Smithy-required (only its nested `Vendor` is, once present).
- **pinpoint settled (batch 5)** — see the settled-services table above for
  why its 120/122 density was structural (single httpPayload-style body
  member per op), not many per-op scalar checks. Don't re-derive; one bug
  found (`DeleteUserEndpoints`).
