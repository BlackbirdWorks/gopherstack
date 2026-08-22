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
| batch | 31 | 45 (15 ops-with-required) | yes (6: `JobDetail.StartedAt`, `DescribeServiceJobOutput.StartedAt`, `ComputeResource.MaxvCpus`, `JobQueueDetail.ComputeEnvironmentOrder`, `QuotaShareCapacityLimit.MaxCapacity`, `ServiceJobRetryStrategy.Attempts` -- see the batch-16 note below and services/batch/PARITY.md) | gopherstack-r80d batch 16 |
| efs | 30 | 31 (6 ops-with-required) | yes (1: `Destination.Region` omitempty, never defaulted for same-region replication -- see the batch-17 note below and services/efs/PARITY.md) | gopherstack-r80d batch 17 |
| ce | 30 | 47 (18 ops-with-required) | 0 (clean; several dead/unreachable `omitempty` tags reviewed and left alone -- see the batch-17 note below and services/ce/PARITY.md) | gopherstack-r80d batch 17 |
| swf | 30 | 39 (17 ops-with-required) | yes (3 findings / 4 member-level fixes: `DecisionTaskCompletedEventAttributes.scheduledEventId`/`.startedEventId` + `PollForDecisionTaskOutput.StartedEventId`, `ChildWorkflowExecutionTimedOutEventAttributes.timeoutType`, `TimerCanceledEventAttributes.startedEventId` -- see the batch-17 note below and services/swf/PARITY.md) | gopherstack-r80d batch 17 |
| accessanalyzer | 28 | 39 (17 ops-with-required) | yes (1: `Location.Span`, nested inside `ValidatePolicyFinding.Locations`, invisible to the flat per-op scan and one level deeper than `ValidatePolicyFinding` itself -- see the batch-18 note below and services/accessanalyzer/PARITY.md) | gopherstack-r80d batch 18 |
| cognitoidp | 27 | 129 (25 ops-with-required) | yes (4: `TermsType.Links`, `ListWebAuthnCredentialsOutput.Credentials`, `ResourceServerScopeType.ScopeName`/`.ScopeDescription`, `NotifyConfigurationType.SourceArn` -- see the batch-19 note below and services/cognitoidp/PARITY.md) | gopherstack-r80d batch 19 |
| emrserverless | 25 | 22 (14 ops-with-required) | yes (2: `JobRun`/`JobRunSummary.releaseLabel` dropped when the owning application's own ReleaseLabel was an explicit empty string; `JobRunAttemptSummary.releaseLabel`/`.stateDetails` hardcoded empty despite both already tracked on the backing JobRun -- see the batch-20 note below and services/emrserverless/PARITY.md) | gopherstack-r80d batch 20 |
| networkmonitor | 22 | 12 (7 ops-with-required) | 0 (clean; read end to end including the nested `MonitorSummary`/`Probe` domain structs -- see the batch-21 note below and services/networkmonitor/PARITY.md) | gopherstack-r80d batch 21 |
| bedrockruntime | 20 | 11 (8 ops-with-required) | 0 (clean; the `Converse`/`ConverseStream` polymorphic union family is real but gopherstack only ever constructs a plain-text `ContentBlock`, a documented scope limit not a dropped field -- see the batch-21 note below and services/bedrockruntime/PARITY.md) | gopherstack-r80d batch 21 |
| cloudfrontkeyvaluestore | 18 | 6 (5 ops-with-required) | 0 (clean; read end to end including the nested `ListKeysResponseListItem` domain struct and the `ETag` HTTP-header binding -- see the batch-21 note below and services/cloudfrontkeyvaluestore/PARITY.md) | gopherstack-r80d batch 21 |
| sesv2 | 18 | 112 (13 ops-with-required) | yes (1: `TrackingOptions.CustomRedirectDomain` tagged omitempty despite being reachably empty whenever a real client sets `HttpsPolicy` alone via `PutConfigurationSetTrackingOptions` -- see the batch-21 note below and services/sesv2/PARITY.md) | gopherstack-r80d batch 21 |
| elasticsearch | 16 | 51 (12 ops-with-required) | 0 (clean; read end to end including the nested `ElasticsearchDomainStatus` domain struct (4 required members, reachable through `DescribeElasticsearchDomain(s)Output`) and the `*Status`-wrapper family reachable through `ElasticsearchDomainConfig` (0 required itself; every `Options`/`Status` pair it wraps is always emitted together, never split) -- see the batch-22 note below) | gopherstack-r80d batch 22 |
| rolesanywhere | 16 | 30 (16 ops-with-required) | 0 (clean; every domain struct reachable through this service's 16 required-output ops -- `TrustAnchorDetail`/`CrlDetail`/`ProfileDetail`/`SubjectDetail` -- carries zero required members in the real Smithy model, confirmed via AST walk, so the flat op-level count is already the complete surface; already through an unusually thorough 2026-08-10 general-parity pass -- see the batch-22 note below) | gopherstack-r80d batch 22 |
| awsconfig | 15 | 102 (12 ops-with-required) | 0 (clean; domain-struct cross-reference found `ConnectorSummary`/`ConfigurationRecorderSummary` add 8 required members the flat op count misses, both fully emitted; two dead `omitempty` tags on `Connector`/`ConnectorSummary` reviewed and ruled out as structurally unreachable -- see the batch-23 note below) | gopherstack-r80d batch 23 |
| codeconnections | 15 | 27 (14 ops-with-required) | 0 (clean; "one wrapper key" shape, `GetResourceSyncStatus`/`GetRepositorySyncStatus` already fixed by a prior pass; one dead `omitempty` tag (`RepositorySyncDefinition.Parent`) reviewed and ruled out -- see the batch-23 note below) | gopherstack-r80d batch 23 |
| codestarconnections | 15 | 27 (14 ops-with-required) | 0 (clean; identical wire shape to codeconnections but a separate implementation -- `GetResourceSyncStatus`'s `InitialRevision`/`TargetRevision` gap already disclosed as a `structural_gap` by a very recent prior pass (gopherstack-7mmd); same dead `omitempty` tag reviewed and ruled out -- see the batch-23 note below) | gopherstack-r80d batch 23 |
| ses | 13 | 71 (13 ops-with-required) | yes (2 findings / 4 member-level fixes: `GetIdentityMailFromDomainAttributes.MailFromDomain`, `GetIdentityNotificationAttributes.BounceTopic`/`ComplaintTopic`/`DeliveryTopic` -- see the batch-24 note below and services/ses/PARITY.md) | gopherstack-r80d batch 24 |
| athena | 12 | 70 (8 ops-with-required) | 0 (clean; this exact bug class was already fixed by a dated prior pass -- `GetSessionEndpoint`/`CreatePresignedNotebookUrl`/`GetResourceDashboard` PARITY.md entries explicitly describe the 3-required-field shapes now emitted; one dead `omitempty` tag on `CapacityReservation.CreationTime` reviewed and ruled out (single unconditional construction site) -- see the batch-24 note below) | gopherstack-r80d batch 24 |
| comprehend | 12 | 85 (6 ops-with-required) | 0 (clean; all 6 `BatchDetect*` ops' required `ErrorList`/`ResultList` already always emitted as non-nil `make(...)` slices; every nested `*ItemResult`/`BatchItemError` type confirmed to declare zero required members via the AST walk, so the flat op-level count is already the complete surface -- see the batch-24 note below) | gopherstack-r80d batch 24 |
| rekognition | 11 | 75 (5 ops-with-required) | 0 (clean; nested-domain-struct check on `MediaAnalysisInput.S3Object`/`MediaAnalysisOutputConfig.S3Bucket` -- both required one level below the flat op-level required fields -- found a prior pass had already wired both correctly, doc-comment citing `validateOpStartMediaAnalysisJobInput`; `CreateFaceLivenessSession`/`GetFaceLivenessSessionResults` always populate `SessionId`/`Status` from non-empty backend state -- see the batch-25 note below) | gopherstack-r80d batch 25 |
| timestreamquery | 11 | 15 (7 ops-with-required) | yes (1: `DescribeScheduledQuery`'s `ScheduledQuery.TargetConfiguration.TimestreamConfiguration` missing required `TimeColumn`/`DimensionMappings` entirely -- see the batch-25 note below and services/timestreamquery/PARITY.md) | gopherstack-r80d batch 25 |

51 services settled, 2563 required output fields read end to end (the running
total counts each settled service's flat per-op `cmd/requiredoutputfields`
number, as established by every prior batch -- glue's own real audited
surface was substantially larger once its ~56 gopherstack-modeled domain
structs were cross-checked, see the batch-15 note below). Batch 25
(rekognition + timestreamquery, tied at 11 each) added 1 more counted bug
(timestreamquery's `DescribeScheduledQuery` `TargetConfiguration.
TimestreamConfiguration.TimeColumn`/`DimensionMappings`; rekognition came back
clean) on top of the running total -- see the batch-25 note below for detail.
`cloudformation` and `emr` (tied at 10 fields each) are now the largest
remaining candidates after sagemaker. Batch 24
(ses + athena + comprehend, tied at 12-13 each) added 2 more counted findings
(4 member-level fixes, both in ses; athena and comprehend both came back
clean) on top of the running total -- see the batch-24 note below for
detail. Batch 23
(awsconfig + codeconnections + codestarconnections, tied at 15 each) added 0
counted bugs -- all three came back clean; see the batch-23 note below for
detail. `ses` (13 fields, 71 ops, 13 ops-with-required) is now the largest
remaining candidate after sagemaker. Batch 22
(elasticsearch + rolesanywhere, re-verified tied at 16 each) added 0 counted
bugs -- both came back clean on a full end-to-end read including every
reachable domain struct -- see the batch-22 note below for detail. Batch 21
(networkmonitor + bedrockruntime + cloudfrontkeyvaluestore + sesv2) added 1
more counted bug (sesv2's `TrackingOptions.CustomRedirectDomain`; the other
three came back clean) on top of the running total -- see the batch-21 note
below for detail. Batch 20
(emrserverless only) added 2 more counted bugs (plus 1 fixed-but-not-counted,
`JobRun.jobDriver` -- see the batch-20 note below) on top of the running
total. Batch 19
(cognitoidp only) added 4 more counted bugs on top of the running total --
see the batch-19 note below for detail. Batch 18
(accessanalyzer only) added 1 more counted bug on top of the running total
-- see the batch-18 note below for detail. Batch 17
(efs + ce + swf, 30 fields each) added 4 more counted bugs (1 + 0 + 3) on top
of the running total -- see the batch-17 note below for detail. Batch 16
(batch only) added 6 more counted bugs on top of the running total -- see the
batch-16 note below for detail. Batch 15
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

### batch (batch 16): 6 member-level bugs, all "required member tagged omitempty in a reachable zero/empty state"

Selected after re-verifying the table against a fresh `go run
./cmd/requiredoutputfields` run and re-reading `services/_REQUIRED_OUTPUT_
CANDIDATES.md`'s own ranked table: with sagemaker (459 fields, off-limits,
concurrent conversion under gopherstack-oc9v) excluded and all settled
services removed, `batch` (31 fields, 45 ops, 15 ops-with-required) is the
largest remaining candidate, ahead of `ce`/`efs`/`swf` (30 each). `git
status` confirmed a clean tree throughout.

Followed the batch-15 method: parsed the pinned SDK's 5,706-line
`types/types.go` with an AST-style walk (blank-line/brace-depth block
splitting, not a grep window) to attribute every `This member is required.`
to its own struct before reading any handler. Found 48 domain structs with
at least one required member (`ComputeEnvironmentDetail`,
`ComputeEnvironmentOrder`, `ComputeResource`, `ConsumableResourceSummary`,
`JobDefinition`, `JobDetail`, `JobQueueDetail`, `JobSummary`,
`ListJobsByConsumableResourceSummary`, `QuotaShareCapacityLimit`,
`QuotaSharePolicy`, `QuotaSharePreemptionConfiguration`,
`QuotaShareResourceSharingConfiguration`, `SchedulingPolicyDetail`,
`SchedulingPolicyListingDetail`, `ServiceEnvironmentDetail`,
`ServiceJobRetryStrategy`, `ServiceJobSummary`, and 30 more, mostly
container/EKS leaf types), read every one against gopherstack's
`services/batch/models.go`/handler struct tags and construction sites. The
op-level flat scan (31 fields/15 ops) undercounts the real surface for the
same reason batch 10's stepfunctions did: several ops (`DescribeComputeEnvironments`,
`DescribeJobQueues`, `DescribeServiceEnvironments`, `RegisterJobDefinition`/
`DescribeJobDefinitions`) return the shared domain struct directly as the
wire type, so their nested required members (e.g. `ComputeResource.MaxvCpus`
inside `ComputeEnvironmentDetail.ComputeResources`) are invisible to a
per-op scan that only sees the op's own top-level required list.

6 bugs, all the dominant class this campaign has now confirmed on every
service it's touched -- a required member tagged `omitempty` in a state a
real client actually reaches, because the real SDK's own client-side
validator (or, in one case, this backend's own weaker validation) only
rejects a nil pointer, not a zero/empty value:

1. `JobDetail.StartedAt` (`DescribeJobs`) and `DescribeServiceJobOutput.StartedAt`
   (`DescribeServiceJob`) -- both required unconditionally even before a job
   reaches RUNNING; nil until this backend's opt-in janitor (never started in
   tests, ticks every 1 minute by default) advances the job, so any
   real-client `Describe*` call on a freshly submitted job saw the key
   vanish entirely instead of decoding a documented zero timestamp.
2. `ComputeResource.MaxvCpus` (`DescribeComputeEnvironments`, via
   `ComputeEnvironmentDetail.ComputeResources`) -- the real client-side
   `validateComputeResource` only rejects a nil `MaxvCpus` pointer, not
   zero, and this backend never validates it at all.
3. `JobQueueDetail.ComputeEnvironmentOrder` (`DescribeJobQueues`) -- required
   unconditionally, but `CreateJobQueueInput` itself declares
   `ComputeEnvironmentOrder`/`ServiceEnvironmentOrder` mutually exclusive, so
   a queue built purely from `serviceEnvironmentOrder` legitimately has an
   empty `ComputeEnvironmentOrder` -- the "required-but-inapplicable means
   present-and-empty, not absent" shape, same principle as stepfunctions
   batch 10's `DescribeMapRun.ExecutionCounts`.
4. `QuotaShareCapacityLimit.MaxCapacity` (`DescribeQuotaShare`/
   `ListQuotaShares`) -- the real client-side `validateQuotaShareCapacityLimit`
   only rejects a nil `MaxCapacity` pointer, not zero, and this backend
   never validates it.
5. `ServiceJobRetryStrategy.Attempts` (`DescribeServiceJob`, via the
   `RetryStrategy` echo) -- the real client-side
   `validateServiceJobRetryStrategy` only rejects a nil `Attempts` pointer,
   not zero (the documented 1-10 range isn't enforced client-side), and
   `SubmitServiceJob` passes it through unvalidated.

All 6 proven via real `aws-sdk-go-v2/service/batch` client round trips
(`services/batch/wire_output_required_r80d_test.go`), hand-reverted
(5 files: `models.go`, `handler_jobs.go`, `handler_service_jobs.go`,
`handler.go`, `job_queues.go`)/confirmed all 6 fail/restored, md5sum-verified
byte-identical.

Ruled out, not counted: `ComputeResource.Type` and `QuotaSharePolicy.
IdleResourceAssignmentStrategy`/`QuotaSharePreemptionConfiguration.
InSharePreemption`/`QuotaShareResourceSharingConfiguration.Strategy` are all
required string enums whose own real-SDK client-side validators reject an
empty string (not just a nil pointer) -- unreachable, the same class glue
batch 15 first identified for `EncryptionAtRest.CatalogEncryptionMode`.
`QuotaShareCapacityLimit.CapacityUnit` is required and reachable-by-AWS's-own-
rules, but this backend's own `CreateQuotaShare`/`UpdateQuotaShare`
independently rejects an empty `capacityUnit` before storage -- stricter
than real AWS, and unreachable through this backend specifically (a
separate, unfixed over-validation gap, out of scope here).
`ListJobsByConsumableResourceSummary.ConsumableResourceProperties` is
required unconditionally, but this op's own backend filter
(`jobReferencesConsumableResource`) requires a non-nil
`ConsumableResourceProperties` before a job is ever included in the result
set, so the omitempty tag on it is dead code, not a reachable drop.

Named, not audited further: four sub-features are entirely unmodeled on
both the input and output side, so their own required members can never
surface -- `EFSVolumeConfiguration.FileSystemId`,
`S3FilesVolumeConfiguration.FileSystemArn`, `EksPersistentVolumeClaim.
ClaimName`, `FirelensConfiguration.Type` (all container/EKS volume or
logging sub-structs gopherstack's `Volume`/`EksVolume`/`ContainerProperties`/
`ContainerDetail` have no fields for at all), and
`NodePropertyOverride.TargetNodes` (`SubmitJob` never accepts a
`nodeOverrides` parameter). Verified structurally absent by grepping
`models.go`'s field lists directly against the real SDK types, not sampled.
Consistent with, and now naming the specifics behind, the service's
pre-existing disclosed multi-node/ECS/EKS-describe-side gap. `ServiceResourceId`
(required `Name`/`Value`) is likewise unreachable -- it only appears inside
`LatestServiceJobAttempt`/`ServiceJobAttemptDetail`/`ServiceJobPreemptedAttempt`,
all part of the already-disclosed unmodeled `attempts`/`capacityUsage`/
`latestAttempt`/`preemptionSummary` family. `ShareAttributes` (required
`ShareIdentifier`) was checked and found already correctly modeled as
`FairsharePolicy`'s `ShareDistribution` with no omitempty bug.

All gates green for `services/batch/` (build/vet/gofmt/race-test/lint, 0
banned nolints, 0 new nolints); repo-wide `go build ./...`, `go vet ./...`,
`go vet -tags e2e ./...`, `go vet -tags integration ./...` all clean too. No
exported signature changed except the addition of the new unexported
`int64OrZero` helper (`handler.go`). Did not attempt a second service this
batch, per the brief's "full rigour and no more" -- `batch`'s 48-struct
domain-cross-reference alone was the full scope for this pass.
services/_REQUIRED_OUTPUT_CANDIDATES.md updated: moved from the ranked table
into "Already examined" (settled-services count now 31, 2179 required
output fields read end to end); `ce`, `efs`, and `swf` are now tied at the
top of the ranked remainder (30 fields each) after sagemaker -- `ce` (47
ops, 18 ops-with-required), `swf` (39 ops, 17 ops-with-required), `efs` (31
ops, 6 ops-with-required).

### ce/efs/swf (batch 17): 4 member-level bugs across 3 findings, taken alphabetically after re-verifying the tie

Selected after re-reading the brief and re-running `go run
./cmd/requiredoutputfields`: `ce`, `efs`, and `swf` confirmed tied at 30
required output fields each (batch 16 had already named this tie; this
batch re-verified it rather than trusting the prior note blind). `git
status` was clean for all three throughout; sagemaker stayed off-limits
(uncommitted `gopherstack-oc9v` conversion changes present the whole
batch). All three were small enough by ops-with-required (efs 6, swf 17, ce
18) that full rigour on all three fit in one batch, unlike glue/batch's
single-service-only batches.

Both efs and swf needed the same tooling correction the input-side sweep
already established for output structs generally, reapplied here at the
implementation level: the line-based AST walk this campaign has used since
batch 15 silently dropped `ChildWorkflowExecutionTerminatedEventAttributes`
from swf's 88-struct `types.go` (a brace-count edge case, not a bug in the
method's *design* -- a doc-comment blank line inside a still-open block
desynced the line-based blank-line/brace-depth tracker for exactly one
struct). Rewritten as a character-level brace matcher and cross-checked
against efs/ce (same struct counts both ways, confirming those two were
never affected) before trusting swf's result. **Any future AST-walk
implementation should verify itself against a character-level brace
matcher once per session, not assume the line-based shortcut generalizes.**

**efs (30 fields / 6 ops-with-required, 1 bug):** flat, `map[string]any`
literal responses (no tagged wire structs), so no domain-struct undercount
risk -- read all 6 ops plus every nested domain struct with required
members (`PosixUser`, `CreationInfo`, both reachable only through
`AccessPoint`'s optional `PosixUser`/`RootDirectory.CreationInfo` fields,
both correctly never-omitempty once their optional parent is present).
1 bug: `Destination.Region` (efs@v1.44.4 types/types.go:116-119, required)
tagged `omitempty` in `ReplicationDestination.Region` and never defaulted
when a `CreateReplicationConfiguration` caller omits it -- the documented
same-region-replication path (`DestinationToCreate.Region` carries no
"This member is required." at all). Fixed by defaulting to the source
region, same as the existing `Status`/`OwnerID` defaults. See
services/efs/PARITY.md's 2026-08-21 entry.

**ce (30 fields / 18 ops-with-required, 0 bugs):** already carried
substantial prior wire-shape scrutiny (this pass's own
`wire_field_fixes_test.go` predates it) -- read all 18 ops plus all 20
domain structs with required members end to end. Every required field
already unconditionally present except a cluster in the commitment-
purchase-analysis family (`GetCommitmentPurchaseAnalysisOutput`/
`StartCommitmentPurchaseAnalysisOutput`'s `AnalysisId`/`AnalysisStatus`/
`AnalysisStartedTime`/`EstimatedCompletionTime`, all tagged `omitempty`)
which are all structurally unreachable: `CommitmentAnalysis` has exactly
one construction site (`CreateCommitmentAnalysis`) and it unconditionally
populates all four -- the same "dead omitempty tag" class batch 16 first
named for `ListJobsByConsumableResourceSummary`. `AnomalyRootCause`'s
optional `Impact` (`RootCauseImpact.Contribution`, required once present)
is correctly never populated at all -- this backend doesn't model root-
cause impact breakdowns, an honest absence, not a bug. `CostCategory`'s
`SplitChargeRules` is tracked on the backend model but never echoed on any
output at all; not counted since `SplitChargeRules` itself isn't
Smithy-required on `CostCategory`, only named here as a general-parity gap
outside this cut's scope. See services/ce/PARITY.md (no dated entry added
since 0 bugs found; existing 2026-07-29 entry stands).

**swf (30 fields / 17 ops-with-required, 3 findings / 4 member-level
fixes):** the "polymorphic `HistoryEvent` sub-object" undercount shape
stepfunctions' batch 10 first named, at larger scale -- 80 of 88 structs in
`types.go` carry required members (nearly the entire
`*EventAttributes`/`*DecisionAttributes` family), all invisible to the
flat per-op scan since every op's own `<Op>Output` is nearly flat. Read
every event type this backend actually emits (`appendHistoryEventLocked`
call sites across `activity_tasks.go`/`decision_tasks.go`/
`decision_orchestration.go`/`workflow_executions.go`/`signals.go`/
`timeout_sweep.go`) against its struct's required set. 3 findings: (1)
`DecisionTaskCompletedEventAttributes.scheduledEventId`/`.startedEventId`
had no struct field at all -- this backend never recorded
`DecisionTaskScheduled`/`DecisionTaskStarted` history events, so the single
most common event in SWF's entire history stream (every decision task
response) dropped both required members, and
`PollForDecisionTaskOutput.StartedEventId` stayed at Go-zero (0) forever;
fixed by mirroring the already-correct Activity* event chain; (2)
`ChildWorkflowExecutionTimedOutEventAttributes.timeoutType` was dropped
because `propagateChildClosureLocked`'s shared base attrs cover every other
Child* closure event's required set but not this one's extra member, and
the TimedOut call site passed `nil` for it; fixed by passing the same
`timeoutTypeStartToClose` constant the sibling `WorkflowExecutionTimedOut`
event already uses two lines above (`ChildWorkflowExecutionTerminated`'s
own `nil` extra was verified correct and left alone -- its required set is
exactly the shared base four); (3) `TimerCanceledEventAttributes.
startedEventId` was dropped because nothing tracked which `TimerStarted`
event a given open `timerId` referred to; fixed by adding
`WorkflowExecution.TimerStartedEventIDs map[string]int64`. All 4
member-level fixes proven via real `aws-sdk-go-v2/service/swf` client round
trips, hand-reverted (4 files together)/confirmed-failing/restored,
md5sum-verified byte-identical; `go test ./services/swf/...` passed
unchanged both before and after (no existing test hard-coded an
event-index/count the two new decision-task events per cycle would have
shifted). `TimerFiredEventAttributes` and 7 other `*EventAttributes` types
(`DecisionTaskTimedOut`, the `LambdaFunction*` family, `ScheduleActivityTaskFailed`,
`RequestCancelActivityTaskFailed`, `RecordMarkerFailed`,
`CompleteWorkflowExecutionFailed`, `FailWorkflowExecutionFailed`) are never
emitted at all by this backend -- named as a missing-feature gap (already
documented for timers; newly named here for the rest), not a
dropped-required-field bug, matching stepfunctions batch 10's precedent for
HistoryEventTypes an emulator can never produce.
`WorkflowType`/`ActivityType.CreationDate` (required, tagged `omitempty`)
is unreachable via any real client the same way ce's commitment-analysis
fields are -- `RegisterWorkflowType`/`RegisterActivityType` always stamp
it; the only code path that skips it (`AddWorkflowTypeInternal`) is a
Go-only test-seed helper no real SDK client can reach. `fieldalignment
-fix` was run on `models.go` after adding two int64/map fields (reordering
only, `git diff`-verified). See services/swf/PARITY.md's 2026-08-21 entry
for full SDK file:line citations.

All three services' gates (build/vet/gofmt/race-test/lint) are green, 0
banned nolints, 0 new nolints, no exported signatures changed outside swf's
internal `activeDecisionTaskRecord`/`DecisionTask`/`WorkflowExecution`
struct fields (none of which cross a package boundary).
`services/_REQUIRED_OUTPUT_CANDIDATES.md` updated: all three moved from the
ranked table into "Already examined" (settled-services count now 34, 2269
required output fields read end to end). Did not touch sagemaker
(off-limits, confirmed via repeated `git status` checks) or attempt a
fourth service this batch.

### cognitoidp (batch 19): 4 bugs, all "client only null-checks the pointer, not its content"

27 fields / 129 ops / 25 ops-with-required per a fresh `cmd/requiredoutputfields`
run, cross-checked against this file before starting (both agreed cognitoidp
is the largest remaining candidate after sagemaker, still off-limits --
`git status` showed its gopherstack-oc9v conversion with uncommitted changes
both before and after this batch). Before trusting the flat count, the
campaign's own instrument was re-validated per the brief: two independent
implementations of a full `types.go` domain-struct walk -- a character-level
brace matcher and a `go/parser`/`go/ast`-based parser -- were built and
cross-checked against each other. They agreed exactly: 93 structs, 29
carrying >=1 required member, 64 required fields summed; a third check
(`grep -c "This member is required."`) also returned 64.

That domain-struct total (64) is larger than the flat per-op count (27) for
the same "one wrapper key hides the domain struct's own required members"
reason established by pinpoint/bedrockagent/accessanalyzer:
`CreateTerms`/`DescribeTerms`/`UpdateTerms` don't appear in the 25-op flat
list at all (their `Terms` field isn't itself required) yet each nests
`types.TermsType`, which alone carries 9 required members.

4 bugs, all a variant of the dominant class this cut names but with an
extra wrinkle not previously logged this precisely: a required `*string`/map
member's real SDK client-side validator only null-checks the pointer, not
its content, so a real client can send an explicit **empty-string** value
where gopherstack's wire type assumed "empty means absent" and tagged the
field `omitempty`. `TermsType.Links` (Create/DescribeTerms omit Links
entirely, since it's optional on input); `ListWebAuthnCredentialsOutput.
Credentials` (omitempty drops a required array the handler already built
non-nil, for a user with zero passkeys); `ResourceServerScopeType.ScopeName`/
`.ScopeDescription` and `NotifyConfigurationType.SourceArn` (both real
`*string` fields the SDK's own validator only nil-checks, confirmed by
reading `validateResourceServerScopeType`/`validateNotifyConfigurationType`
in validators.go directly, not assumed). All 4 proven via real
`aws-sdk-go-v2/service/cognitoidentityprovider` client round trips
(`wire_output_required_r80d_test.go`), hand-reverted/confirmed-failing/
restored, md5sum byte-identical. One more (`AccountTakeoverActionType.
Notify`, a required `bool`) was fixed but not counted: `omitempty` drops
the key exactly when `Notify == false`, but an omitted key and an explicit
`false` decode identically for any real client, so nothing can prove it --
the amplify batch-14 `Branch.Stage` class, reapplied.

Disclosed, not fixed: `EventFeedbackType.Provider` looked like the "no
struct field at all" class until the data path was traced further --
`b.authEvents` (auth_events.go) is only ever read, never written, by any
op in this backend, so `AdminListUserAuthEvents` always returns empty to a
real client and the feedback path can never be reached at all. A
missing-feature gap (this emulator never synthesizes adaptive-auth risk
events), the same class as swf's undelivered `*EventAttributes` types
(batch 17), not this cut's bug. `UserPoolAddOnsType.AdvancedSecurityMode`/
`UsernameConfigurationType.CaseSensitive`/`RefreshTokenRotationType.Feature`
are likewise unmodeled features (zero references anywhere in the package).
`UserPoolType.LambdaConfig`/`AccountRecoverySetting` and
`ManagedLoginBranding.Assets` are opaque `map[string]any` pass-through,
genuinely inapplicable rather than unaudited (the accessanalyzer
`AnalyzerConfiguration` precedent).

All gates green (build/vet/gofmt/race-test/lint, 0 banned nolints, 0 new
nolints, no exported signatures changed). services/_REQUIRED_OUTPUT_CANDIDATES.md
updated: cognitoidp moved from the ranked table into "Already examined"
(settled-services count now 36, 2324 required output fields read end to
end). Did not touch sagemaker (off-limits, `git status` checked before and
after) or attempt a second service this batch, per the brief's "full rigour
and no more" -- see services/cognitoidp/PARITY.md's 2026-08-21 entry for
full detail and SDK file:line citations.

### networkmonitor + bedrockruntime + cloudfrontkeyvaluestore + sesv2 (batch 21): 1 bug, three clean

Instrument re-validated before use per the brief: a character-level brace
matcher, a `go/parser`/`go/ast` walk, and a raw `grep -c "This member is
required."` count were built and cross-checked against each other on every
service touched this batch -- all three agreed exactly every time
(networkmonitor: 52/22; bedrockruntime: 161/90; cloudfrontkeyvaluestore:
52/22; sesv2: 232/143), so no service's result rests on a single
unvalidated tool.

Selected in ranked order after re-verifying the table against a fresh `go
run ./cmd/requiredoutputfields` run (agreed with this file's own ranked
table): networkmonitor (22, largest remaining after sagemaker), then
bedrockruntime (20), then cloudfrontkeyvaluestore (tied with sesv2 at 18,
taken first alphabetically). Did four services this batch since the first
three came back clean quickly (networkmonitor and cloudfrontkeyvaluestore
are small SDKs with almost no output-relevant domain-struct surface once
the input-only structs are excluded; bedrockruntime's large 90-struct
surface is almost entirely the `Converse`/`ConverseStream` polymorphic
union family, which resolved quickly once `handler_converse.go` was read
and found to construct only a single plain-text content block, never any
of the other union variants) -- all four still read end to end, not
sampled.

**networkmonitor (22 fields / 12 ops, 7 ops-with-required, 0 bugs):** the
flat per-op count already captures the real surface almost completely --
only `MonitorSummary` (reachable through `ListMonitorsOutput`'s required
`Monitors` field) and `Probe` (reachable through `GetMonitorOutput`'s
optional `Probes` field) are nested domain structs with their own required
members, and both are correctly modelled as plain non-omitempty strings in
gopherstack's wire structs. `GetMonitorOutput.CreatedAt`/`ModifiedAt` carry
a dead `omitempty` tag (structurally unreachable: `CreateMonitor` is the
sole construction site and unconditionally stamps both). See
services/networkmonitor/PARITY.md's 2026-08-21 entry.

**bedrockruntime (20 fields / 11 ops, 8 ops-with-required, 0 bugs):** 161
required fields across 90 structs once every `api_op_*.go` file and
`types/types.go` are walked together, but the flat 8-op surface already
covers everything gopherstack's Converse/ApplyGuardrail/InvokeGuardrailChecks/
InvokeModel/async-invoke handlers actually construct -- the other ~80
structs (every `ContentBlock`/`GuardrailConverse*`/`*EventAttributes`
variant beyond plain text) are never built at all, a documented,
already-disclosed scope limit (this backend has no image/video/document/
tool-use/search-result content generation), not a dropped required field.
See services/bedrockruntime/PARITY.md's 2026-08-21 entry.

**cloudfrontkeyvaluestore (18 fields / 6 ops, 5 ops-with-required, 0
bugs):** smallest SDK touched this batch (6 ops total). The one nested
domain struct with required members, `ListKeysResponseListItem`
(`Key`/`Value`), backs `ListKeys`' non-required `Items` field and is
already emitted correctly. `ETag` (required on 4 ops) rides an HTTP
header, not the JSON body -- the real deserializer silently leaves it nil
if the header is ever absent, but `handler.go` sets it unconditionally from
a `uuid.NewString()`-backed value that can never be empty. See
services/cloudfrontkeyvaluestore/PARITY.md's 2026-08-21 entry.

**sesv2 (18 fields / 112 ops, 13 ops-with-required, 1 bug):** tied with
cloudfrontkeyvaluestore by the flat count, but 232 required fields across
143 domain structs once walked -- almost entirely `*Input` structs for
this service's large identity/configuration-set/contact/tenant CRUD
surface, not response types (the gap runs the "structural-analysis" note's
other direction: a large struct/field count that does NOT translate into
a large *output*-relevant surface, the opposite of accessanalyzer's
28-flat/114-real gap). 1 bug: `TrackingOptions.CustomRedirectDomain`
(required on `types.TrackingOptions`) was tagged `omitempty` in
`handler_configuration_sets.go`, dropping the key whenever a real client
called `PutConfigurationSetTrackingOptions` with `HttpsPolicy` alone
(`CustomRedirectDomain` is optional on that op's own input) -- the
"required-but-inapplicable means present-and-empty, not absent" shape this
campaign has repeatedly named. Every other output-relevant domain struct
checked this pass (`SuppressedDestination`/`SuppressedDestinationSummary`,
`DedicatedIp`/`DedicatedIpPool`, `EventDestination`, `MailFromAttributes`,
`BulkEmailEntryResult`, `DeliverabilityTestReport`/`IspPlacement`/
`OverallVolume`/`DailyVolume`, `ContactList`, `VdmOptions`/
`DashboardOptions`/`GuardianOptions`, `ArchivingOptions`, `BlacklistEntry`)
came back clean -- most declare zero required members on the pinned SDK at
all, confirmed by reading `types.go` directly rather than assuming from
the struct's presence in gopherstack's domain model. Proven via a real
`aws-sdk-go-v2/service/sesv2` client round trip
(`wire_output_required_r80d_test.go`), hand-reverted/confirmed-failing/
restored, md5sum-verified byte-identical. See services/sesv2/PARITY.md's
2026-08-21 entry for full detail.

All four services' gates (build/vet/gofmt/race-test/lint) are green, 0
banned nolints, 0 new nolints, no exported signatures changed; repo-wide
`go build ./...`, `go vet ./...`, `go vet -tags e2e ./...`, `go vet -tags
integration ./...` all re-run and clean.
`services/_REQUIRED_OUTPUT_CANDIDATES.md` updated: all four moved from the
ranked table into "Already examined" (settled-services count now 41, 2427
required output fields read end to end). Did not touch sagemaker (off-limits,
`git status` checked before and throughout) or `cmd/gendocs`, and did not
attempt a fifth service.

### elasticsearch + rolesanywhere (batch 22): 0 bugs, both clean

Instrument re-validated per the brief before selecting a candidate: the
existing `cmd/requiredoutputfields` (a character-level brace matcher for
locating each `<Op>Output` struct's body, then a blank-line/brace-depth
field-block split for the required-doc-comment scan within it) was run
fresh, then cross-checked against a from-scratch `go/parser`/`go/ast` walk
(a real Go parser, not a text heuristic -- `go/ast.StructType` fields with a
`This member is required.` doc or line comment) written for this batch, plus
a raw `grep -rc "This member is required." <pkg>/*.go <pkg>/types/*.go`
total. All three agreed exactly for both candidates: elasticsearch
16 required output fields / 12 ops-with-required (AST) against 124 total
required-field instances across every struct in the package (grep -c 124,
matched exactly); rolesanywhere 16 / 16 (AST) against 61 total (grep -c 61,
matched exactly). No discrepancy this time, unlike batch 17's swf
line-based-walker miss -- both structures happened not to trigger that
specific edge case.

Confirmed via the fresh `cmd/requiredoutputfields` run and this file's own
ranked table that elasticsearch and rolesanywhere were tied at 16 fields
each, the largest remaining candidates after sagemaker (off-limits all
batch -- `git status` showed only `services/sagemaker/*` dirty throughout,
from a concurrent agent's in-flight conversion, confirmed untouched here).
Took both in one batch since each was narrow (12 and 16 ops-with-required
respectively).

**elasticsearch (16 fields / 51 ops, 12 ops-with-required, 0 bugs):** not a
shallow surface despite the small flat count -- `DescribeElasticsearchDomain`/
`DescribeElasticsearchDomains`'s required `DomainStatus`/`DomainStatusList`
wrap `types.ElasticsearchDomainStatus`, itself carrying 4 more required
members one level deeper (`ARN`/`DomainId`/`DomainName`/
`ElasticsearchClusterConfig`, elasticsearchservice@v1.45.4 types/types.go)
invisible to the flat per-op scan -- read via `toDomainStatusJSON`
(`handler_domains.go`), all four emitted unconditionally, no `omitempty`.
`DescribeElasticsearchDomainConfig`/`UpdateElasticsearchDomainConfig`'s
required `DomainConfig` wraps `types.ElasticsearchDomainConfig`, which
itself declares zero required members (every one of its ~18 sub-fields,
e.g. `AccessPolicies *AccessPoliciesStatus`, is optional) but each
populated sub-object is itself a `{Options, Status}` pair both required
(`AccessPoliciesStatus` et al.) -- read `buildDomainConfigOutput`
(`handler_domain_config.go`) and confirmed every one of the 12 sub-fields
this backend populates constructs both `Options` and `Status`
unconditionally via the shared `elasticsearchConfigValue`/
`domainConfigStatus` helpers, and the one genuinely optional member
(`VPCOptions`) is correctly omitted only when nil and always carries both
sub-fields when present. The remaining 10 ops-with-required (VPC endpoint
and access-authorization family) wrap flat, already-emitted domain objects
(`VpcEndpoint`, `VpcEndpointSummary`, `AuthorizedPrincipal`) that this
service's own PARITY.md documents as already fixed for required-field drops
across 6 prior audit passes (most recently 2026-08-15, gopherstack-6flj) --
re-read end to end rather than trusted, and confirmed still correct
(`handler_vpc_endpoints.go`'s `NextToken` is always emitted as `""`, never
omitted; `VpcEndpointErrors`/`VpcEndpoints`/`VpcEndpointSummaryList`/
`AuthorizedPrincipalList` are always non-nil `make(...)`/literal slices,
never gated behind a length check). No code changes; PARITY.md left
unchanged (this pass added no new finding beyond re-confirming existing
`wire: ok` rows for this specific bug class).

**rolesanywhere (16 fields / 30 ops, 16 ops-with-required, 0 bugs):**
narrower than it looks -- every one of the 16 required-output ops wraps
exactly one nested domain object (`TrustAnchor`/`Crl`/`Profile`, the "one
wrapper key" shape first named by pinpoint/batch 5), but unlike
bedrockagent/amplify/cleanrooms the wrapped domain structs themselves
(`TrustAnchorDetail`/`CrlDetail`/`ProfileDetail`/`SubjectDetail`,
rolesanywhere@v1.26.3 types/types.go) carry **zero** required members in
the real Smithy model -- every field on all four is optional. Confirmed via
the AST walk (no entries for any of the four `*Detail` types in the
required-field listing) before concluding there is no nested-domain-struct
undercount here at all, rather than assuming the "one wrapper key" shape
always hides one (batch 13's appmesh precedent: verify, don't infer from
the shape alone). This service's own PARITY.md already documents an
unusually thorough 2026-08-10 general-parity pass (gopherstack-cf439a0b1)
that fixed 4 real bugs adjacent to this territory (an invented `tags` field
on `TrustAnchorDetail`/`ProfileDetail`, a wrong `TagResource` status code,
missing `ResourceNotFoundException` validation) -- read all 16 handlers
(`handler_trust_anchors.go`, `handler_crls.go`, `handler_profiles.go`,
`handler_attribute_mappings.go`, `handler_notification_settings.go`) end to
end for this cut's specific class (the wrapper key itself ever omitted or
the wrapped object ever empty/nil on a success path) and found every one
constructs a non-nil `map[string]any{keyX: ...}` unconditionally on every
success return; the shared `handleREST` dispatcher's `result == nil` early
return (which would produce the empty-body-204 class) is only reached by
this service's genuinely void-result ops (`TagResource`/`UntagResource`/
etc.), none of which appear in the 16-op required-output set. No code
changes; PARITY.md left unchanged.

Both services' gates (build/vet/gofmt/race-test/lint) are green, 0 banned
nolints, no exported signatures changed (no code changed at all this
batch); repo-wide `go build ./...` re-run and clean (confirmed the only
dirty files anywhere in the tree were the excluded `services/sagemaker/*`).
`services/_REQUIRED_OUTPUT_CANDIDATES.md` updated: both moved from the
ranked table into "Already examined" (settled-services count now 43, 2459
required output fields read end to end); `awsconfig` (15) is now the
largest remaining candidate after sagemaker. Did not attempt a third
service this batch.

### awsconfig + codeconnections + codestarconnections (batch 23): 0 bugs across all three

Instrument re-validated per the brief before selecting: the existing
`cmd/requiredoutputfields` char-level brace matcher was re-run fresh, then
cross-checked against a from-scratch `go/parser`/`go/ast` walk written for
this batch (parses every `api_op_<Op>.go`, walks each `type <Op>Output
struct`'s fields, flags any whose `ast.CommentGroup` contains the line "This
member is required."), plus a raw `grep -c "This member is required."`
totalled across each module's `api_op_*.go` + `types/types.go` as a magnitude
sanity check. All three agreed exactly for all three services: `configservice`
15/15 fields, 12 ops-with-required (grep-c total 211, entirely expected to be
far larger since it counts every input struct's required members too, per
this method's established shape); `codeconnections` 15/15, 14
ops-with-required (grep-c 114); `codestarconnections` 15/15, 14
ops-with-required (grep-c 114, identical to codeconnections since both pin
near-identical Smithy models for what AWS renamed CodeStar Connections to
CodeConnections). No discrepancy.

Confirmed via the fresh `cmd/requiredoutputfields` run and this file's own
ranked table that `awsconfig`/`codeconnections`/`codestarconnections` were
tied at 15 fields each, the largest remaining candidates after sagemaker
(off-limits all batch -- `git status` showed only `services/sagemaker/*`
dirty throughout, from a concurrent agent's in-flight conversion, confirmed
untouched here and re-checked at the end). Also confirmed the module-name
trap the brief calls out: `awsconfig`'s directory maps to the `configservice`
SDK module (`cmd/requiredoutputfields`'s `dirModuleOverride` table, not
inferred from the directory name), while `codeconnections`/
`codestarconnections` need no override -- directory and module name already
match for both. Took all three in one batch since each was narrow (12-14
ops-with-required) and two of the three are near-duplicate wire shapes.

**awsconfig (15 fields / 102 ops, 12 ops-with-required, 0 bugs):** several
ops wrap a single required domain object (`GetConnector`'s `Connector`,
`ListConnectors`' `ConnectorSummaries`, `ListConfigurationRecorders`'
`ConfigurationRecorderSummaries`, `AssociateResourceTypes`/
`DisassociateResourceTypes`' `ConfigurationRecorder`), so read every reachable
domain struct one level deeper against `configservice@v1.68.4`'s
`types/types.go`: `ConnectorSummary` (5 required: `Arn`/`CreatedTime`/`Name`/
`Provider`/`TenantIdentifier`) and `ConfigurationRecorderSummary` (3
required: `Arn`/`Name`/`RecordingScope`) add 8 members the flat op-level scan
misses entirely; `ConfigurationRecorder`, `ConformancePackRuleCompliance`
(the `DescribeConformancePackCompliance` list-item type), and
`ConformancePackEvaluationResult`'s own nested `EvaluationResultIdentifier`
all declare **zero** required members, confirmed via the AST walk rather
than assumed from the wrapper shape (appmesh/rolesanywhere batch-13/22
precedent). All 8 extra members plus the 15 flat ones read end to end
against `connectors.go`/`configuration_recorders.go`/
`handler_conformance_packs.go`/`handler_resources.go` -- every required
member is emitted without `omitempty` except `Connector.
ConnectorConfiguration`/`.CreatedTime` and `ConnectorSummary.CreatedTime`,
which carry `omitempty` despite being required but are structurally
unreachable-empty: `PutConnector` (`connectors.go`) is the sole construction
site for both types (confirmed via a repo-wide grep for `Connector{`/
`ConnectorSummary{`) and unconditionally populates both fields on every
success path, so the tag is dead code, not a reachable drop -- reviewed, not
fixed, the same "dead tag" class amplify batch 14 first named. No code
changes.

**codeconnections (15 fields / 27 ops, 14 ops-with-required, 0 bugs):**
almost entirely the "one wrapper key" shape -- `GetRepositorySyncStatus`/
`GetResourceSyncStatus` wrap a whole `RepositorySyncAttempt`/
`ResourceSyncAttempt`, themselves nesting a further-required `Revision`
(6 required members) and `[]SyncEvent` (3 required members each), so read
every one of those against `codeconnections@v1.13.4`'s `types/types.go`
directly rather than trusting the flat 15/14 count. This exact gap
(`InitialRevision`/`Target`/`TargetRevision` entirely missing) was already
found and fixed by a prior pass -- `handler_repository_sync.go`'s own doc
comments say so explicitly ("previously missing entirely from this response
shape") -- re-read end to end and confirmed the fix is still correctly
wired, not re-flagged as new. `RepositoryLinkInfo` (6 required),
`SyncConfiguration` (8 required), `SyncBlockerSummary`/`SyncBlocker` (1 + 5
required), and `RepositorySyncDefinition` (4 required) all read against
their handlers (`handler_repository_links.go`, `handler_sync_
configurations.go`) and found fully populated without `omitempty` on any
required member. One tagged-`omitempty`-on-a-required-member reviewed and
ruled out: `repositorySyncDefinitionItem.Parent` (required per the real
`RepositorySyncDefinition`) is tagged `omitempty`, but its only value source
is `SyncConfiguration.ResourceName`, and `handleCreateSyncConfiguration`/
`handleUpdateSyncConfiguration` both reject an empty `ResourceName` with
`ErrValidation` before any `SyncConfiguration` -- and therefore any
`RepositorySyncDefinition` -- is ever stored. The real SDK's own
client-side validator only rejects a *nil* `ResourceName` pointer, not an
empty string (`aws-sdk-go-v2/service/codeconnections@v1.13.4/
validators.go:722-748`, `validateOpCreateSyncConfigurationInput`), so this
backend is stricter than real AWS here and the empty state is genuinely
unreachable through it -- the same "stricter than real AWS, unreachable"
class the `batch` service (batch 16) named for `QuotaShareCapacityLimit.
CapacityUnit`. No code changes.

**codestarconnections (15 fields / 27 ops, 14 ops-with-required, 0 bugs):**
same real-AWS wire shape as codeconnections (AWS renamed CodeStar
Connections to CodeConnections; both SDK modules declare identical required
sets) but a separate gopherstack implementation with its own file layout
(`handler_sync_statuses.go`/`handler_sync_blockers.go` split rather than
codeconnections' single `handler_repository_sync.go`) -- read independently,
not assumed identical from the sibling result. Its own
`GetResourceSyncStatus` has the mirror-image gap: `LatestSync.
InitialRevision`/`.TargetRevision` (both required members of
`types.ResourceSyncAttempt`) are never populated, but this is not a new
finding -- `handler_sync_statuses.go`'s own doc comment and
`services/codestarconnections/PARITY.md`'s `structural_gaps` entry (from a
very recent prior pass, `gopherstack-7mmd`) already disclose it in detail,
with a specific no-fabrication justification: this service's data model has
no concept of repository content (no file tree, commit graph, or branch
HEAD) to honestly derive a git commit SHA from, and no customer-facing
operation ever generates one internally, so omitting the whole
`Revision`-bearing wrapper is judged wire-correct, not a stub. Re-read and
confirmed this still matches current behavior; not re-flagged. Same
`RepositorySyncDefinition.Parent` dead-`omitempty`-tag class as
codeconnections, ruled out the same way (`handleCreateSyncConfiguration`
rejects empty `ResourceName` via `errInvalidRequest` before storage). No
code changes.

All three services' gates (build/vet/gofmt/race-test/lint) are green, 0
banned nolints, no exported signatures changed (no code changed at all this
batch); repo-wide `go build ./...` re-run and clean (confirmed the only
dirty files anywhere in the tree were the excluded `services/sagemaker/*`).
`services/_REQUIRED_OUTPUT_CANDIDATES.md` updated: all three moved from the
ranked table into "Already examined" (settled-services count now 46, 2504
required output fields read end to end); `ses` (13 fields, 71 ops, 13
ops-with-required) is now the largest remaining candidate after sagemaker.
Did not attempt a fourth service this batch.

### emrserverless (batch 20): 2 bugs, both the "output-required, input-only-nil-checked" releaseLabel

25 fields / 22 ops / 14 ops-with-required per a fresh `cmd/requiredoutputfields`
run, cross-checked against this file before starting (both agreed
emrserverless is the largest remaining candidate after sagemaker, still
off-limits -- `git status` showed its uncommitted conversion both before
and after this batch). The instrument was re-validated per the brief before
trusting it: a character-level brace matcher and a `go/parser`/`go/ast`
walk of `types/types.go` agreed exactly (40 structs, 15 carrying >=1
required member, 76 required fields summed); a third check
(`grep -c "This member is required."` against the same file) also returned
76.

That domain-struct total (76) is larger than the flat per-op count (25) for
the now-familiar "one wrapper key hides the domain object's own required
members" reason (pinpoint/bedrockagent/cognitoidp), compounded by the
"list of domain-struct elements" undercount (omics/cleanrooms):
`GetApplication`/`UpdateApplication`/`GetJobRun`/`GetSession` each return
one domain object counted as a single required field at the op level, and
`ListApplications`/`ListJobRuns`/`ListJobRunAttempts`/`ListSessions` return
arrays of the same objects, invisible to the per-op scan entirely. 66 of
the 76 domain-struct fields trace back to exactly those 7 wrapper/list
fields (`Application` 7, `ApplicationSummary` 7, `JobRun` 11,
`JobRunSummary` 10, `JobRunAttemptSummary` 11, `Session` 10,
`SessionSummary` 10); the remaining 10
(`CloudWatchLoggingConfiguration`/`Configuration`/`Hive`/
`ImageConfiguration`/`InitialCapacityConfig`/`MaximumAllowedResources`/
`SparkSubmit`/`WorkerResourceConfig`) belong to the opaque
echo-verbatim-as-`any` sub-object family (`applicationConfigFields`/
`JobDriver`/`ConfigurationOverrides`) this backend never independently
constructs, so it structurally cannot drop one of their required members --
whatever valid content a client sends survives untouched.

2 bugs counted, both the same reachability shape cognitoidp batch 19 named:
a real SDK client-side validator that only null-checks a `*string` pointer,
never its content, letting an explicit empty string reach the backend and
then get dropped by an `omitempty`-style conditional. `JobRun`/
`JobRunSummary.releaseLabel` is copied from the owning `Application`'s own
`ReleaseLabel` at `StartJobRun` time; `Application.ReleaseLabel` is copied
verbatim from `CreateApplicationInput.ReleaseLabel`, whose real
`validateOpCreateApplicationInput` only checks `v.ReleaseLabel == nil` --
never its content -- and gopherstack's own `CreateApplication` only
rejects an empty `name`/`type`, not an empty `releaseLabel`. Every job run
started under such an application then had `releaseLabel` vanish entirely
from `GetJobRun`/`ListJobRuns`; fixed by making both map builders emit it
unconditionally, matching the convention `Application`/`Session`'s own
converters already used correctly for the same field name.
`JobRunAttemptSummary.releaseLabel`/`.stateDetails` were a related but
distinct bug: `ListJobRunAttempts`'s synthesized attempt hardcoded both to
`""` under a comment claiming neither was tracked by the backend -- false,
both are already stored on the backing `JobRun` the same function reads six
other fields from. The wire key was never dropped there (present, just
wrong), a data-fidelity bug rather than a dropped-key one, but still inside
this cut's target class since the required member was never actually
populated with real data. Both proven via real
`aws-sdk-go-v2/service/emrserverless` client round trips
(`wire_output_required_r80d_test.go`), hand-reverted/confirmed-failing/
restored, md5sum byte-identical.

Fixed but NOT counted: `JobRun.jobDriver`, guarded the same way as
`releaseLabel` and reachable the same way (`JobDriver` is required on
`types.JobRun` but genuinely optional on `StartJobRunInput` --
`validateOpStartJobRunInput` only validates its content when non-nil).
Unlike `releaseLabel`, this one is unprovable: reading
`awsRestjson1_deserializeDocumentJobDriver` shows an empty `"jobDriver": {}`
object and an entirely absent key both leave the typed field `nil` --
no real client can observe the fix. Documented and fixed anyway (the wire
key is now honestly always present), matching cognitoidp batch 19's
`AccountTakeoverActionType.Notify` precedent for a real-but-unprovable fix.

All gates green (build/vet/gofmt/race-test/lint, 0 banned nolints, 0 new
nolints, no exported signatures changed). services/_REQUIRED_OUTPUT_CANDIDATES.md
updated: emrserverless moved from the ranked table into "Already examined"
(settled-services count now 37, 2349 required output fields read end to
end). Did not touch sagemaker (off-limits, `git status` checked before and
after) or attempt a second service this batch, per the brief's "full rigour
and no more" -- see services/emrserverless/PARITY.md's 2026-08-21 entry for
full detail and SDK file:line citations.

### accessanalyzer (batch 18): 1 bug, one level deeper than a nested-domain-struct undercount

28 fields / 39 ops / 17 ops-with-required per `cmd/requiredoutputfields`
(re-verified via a fresh run, cross-checked against this file, both
agreeing before starting). Before trusting any exemption, the campaign's
own instrument was re-validated per the brief: two *independent*
implementations of a full `types.go` domain-struct walk -- a
character-level brace matcher and a `go/parser`/`go/ast`-based parser (a
real Go parser, not a second hand-rolled text scanner) -- were built and
cross-checked against each other on this service's `types.go`. They agreed
exactly: 117 structs total, 41 carrying >=1 required member, 114 required
fields summed. That domain-struct total (114) is nearly 4x the flat
per-op count (28) -- the gap is fully explained, not just asserted: every
"one wrapper key = the whole nested domain object" op (`GetAccessPreview`,
`GetAnalyzer`, `GetArchiveRule`, `GetGeneratedPolicy`, every `List*`) hides
its domain struct's own required members from the per-op scan, the same
"one wrapper key" shape pinpoint/bedrockagent/cleanrooms established. Two
ops don't even appear in the flat scan's 17-op list at all -- `GetFinding`
and `GetAnalyzedResource`'s own top-level `Finding`/`Resource` fields
aren't themselves Smithy-required -- yet nest `types.Finding`/
`types.AnalyzedResource`, which carry 8 and 7 required members
respectively; a per-op-only scan would have skipped both ops entirely.

Read every op's response-building code end to end against every domain
struct actually reachable from an `Output` field (verified via a
repo-wide grep of every `api_op_*Output` struct's own `types.X` field
types, not just the ones the flat scan flagged, to make sure nothing
reachable was missed). Almost the entire required surface was already
correct -- this service had two dedicated general-parity passes
(2026-08-15 gopherstack-6flj, and gopherstack-kwht before it) that had
already fixed this exact bug class by name for `AccessPreview`/
`AccessPreviewFinding`/`AnalyzedResource`/`AnalyzedResourceSummary`/
`Finding`/`FindingSummary`/`GeneratedPolicyResult`/`JobDetails`/
`ValidatePolicyFinding.FindingDetails`, all re-confirmed rather than
re-litigated. The 13-member `AnalyzerConfiguration`/`Configuration` union
structs (`KmsGrantConfiguration`, `S3BucketAclGrantConfiguration`,
`S3PublicAccessBlockConfiguration`, `VpcConfiguration`, `Trail`) are stored
and echoed back as opaque `json.RawMessage`, never decoded field-by-field
-- genuinely inapplicable to this bug class, not merely unaudited, since
whatever a real client sends comes back byte-for-byte.

**1 bug, one level deeper than the nested-domain-struct undercount class
itself:** `ValidatePolicyFinding.Locations` (required, already correctly
populated as `[]types.Location`) -- but `types.Location` itself requires
both `Path` (already correct) and `Span` (types/types.go:1509-1521), and
`Span` was never emitted at all. This is a *third* undercount shape this
campaign hadn't quite named this way before: not "the op's own required
field is missing a struct field" and not "the domain struct nested behind
a non-required wrapper field has its own dropped members" (the
`GetFinding`/`GetAnalyzedResource` shape above) -- here the *array element
type of an already-correctly-populated required field* has its own
required member missing. Fixed by recovering the real byte range from the
original `policyDocument` text via each value's own `json.RawMessage`
bytes (copied verbatim by `encoding/json`, not re-synthesized) rather than
fabricating a placeholder position, with a step-by-step fallback toward
the document root so `Span` is never dropped even when the exact key a
finding is about is itself absent (e.g. a wholly missing `"Effect"`).
Proven via a real `aws-sdk-go-v2/service/accessanalyzer` client round trip
(`services/accessanalyzer/wire_output_required_r80d_test.go`), one test
asserting `Span` is never nil across 4 finding shapes (including a
2-statement case exercising duplicate-element disambiguation in the
resolver) and a second asserting the span's byte range exactly bounds the
real substring text, hand-reverted/confirmed-failing/restored, md5sum
byte-identical. See services/accessanalyzer/PARITY.md's 2026-08-21 entry
for full detail and SDK file:line citations.

All gates (build/vet/gofmt/race-test/lint, repo-wide `go build ./...`) are
green, 0 banned nolints, 0 new nolints, no exported signatures changed.
`services/_REQUIRED_OUTPUT_CANDIDATES.md` updated: accessanalyzer moved
from the ranked table into "Already examined" (settled-services count now
35, 2297 required output fields read end to end). Did not touch sagemaker
(off-limits, `git status` checked before and after) or services/pipes
(concurrent sibling agent's timestamp-decoding sweep, committed as
c79ebf1b5 partway through this batch) or attempt a second service.

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
amplify (35, settled batch 14), glue (34, settled batch 15), batch (31,
settled batch 16), ce/efs/swf (30 each, settled batch 17),
accessanalyzer (28, settled batch 18), cognitoidp (27, settled
batch 19), emrserverless (25, settled batch 20), and networkmonitor/
bedrockruntime/cloudfrontkeyvaluestore/sesv2 (22/20/18/18, settled batch
21), and elasticsearch/rolesanywhere (16 each, settled batch 22) removed
from this table — see the "Already examined" table above.

```
 459  sagemaker                 ops=403  ops-with-required=188
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
  unreachable via any real client.
- **batch settled (batch 16)** — do not re-derive, see the settled-services
  table above and services/batch/PARITY.md's 2026-08-21 entries, plus the
  batch-16 note above for the full per-member breakdown and the 48-struct
  domain-cross-reference method used. 6 member-level bugs, all the
  "required member tagged omitempty in a reachable zero/empty state" shape:
  `JobDetail.StartedAt`, `DescribeServiceJobOutput.StartedAt`,
  `ComputeResource.MaxvCpus`, `JobQueueDetail.ComputeEnvironmentOrder`,
  `QuotaShareCapacityLimit.MaxCapacity`, `ServiceJobRetryStrategy.Attempts`.
- **ce/efs/swf settled (batch 17)** — do not re-derive, see the
  settled-services table above and services/{ce,efs,swf}/PARITY.md's
  2026-08-21 entries, plus the batch-17 note below for full detail. All
  three verified tied at 30 fields each via a fresh
  `cmd/requiredoutputfields` run before starting; taken in alphabetical
  order, all three completed with full rigour.
- **accessanalyzer settled (batch 18)** — do not re-derive, see the
  settled-services table above and services/accessanalyzer/PARITY.md's
  2026-08-21 entry, plus the batch-18 note below for full detail. Verified
  as the largest remaining candidate after sagemaker via a fresh
  `cmd/requiredoutputfields` run cross-checked against this file before
  starting (both agreed: accessanalyzer 28/39/17).
- **cognitoidp settled (batch 19)** — do not re-derive, see the
  settled-services table above and services/cognitoidp/PARITY.md's
  2026-08-21 entry, plus the batch-19 note below for full detail. Verified
  as the largest remaining candidate after sagemaker via a fresh
  `cmd/requiredoutputfields` run cross-checked against this file before
  starting (both agreed: cognitoidp 27/129/25).
- **emrserverless settled (batch 20)** — do not re-derive, see the
  settled-services table above and services/emrserverless/PARITY.md's
  2026-08-21 entry, plus the batch-20 note below for full detail. Verified
  as the largest remaining candidate after sagemaker via a fresh
  `cmd/requiredoutputfields` run cross-checked against this file before
  starting (both agreed: emrserverless 25/22/14).
- **networkmonitor / bedrockruntime / cloudfrontkeyvaluestore / sesv2
  settled (batch 21)** — do not re-derive, see the settled-services table
  above and each service's PARITY.md 2026-08-21 entry, plus the batch-21
  note above for full detail. Verified via a fresh `cmd/requiredoutputfields`
  run cross-checked against this file before starting (all four agreed:
  networkmonitor 22/12/7, bedrockruntime 20/11/8,
  cloudfrontkeyvaluestore 18/6/5, sesv2 18/112/13).
- **elasticsearch / rolesanywhere settled (batch 22)** — do not re-derive,
  see the batch-22 note below for full detail. Re-verified the tie via a
  fresh `cmd/requiredoutputfields` run and a from-scratch `go/parser`
  AST cross-check (both agreed exactly: elasticsearch 16/51/12,
  rolesanywhere 16/30/16) before starting; both came back clean. **awsconfig
  (15) is now the largest remaining candidate after sagemaker.**
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

### ses + athena + comprehend (batch 24): 2 findings / 4 member-level fixes, both in ses

Instrument re-validated three ways before picking a candidate: existing
`cmd/requiredoutputfields`'s char-level brace matcher, a fresh from-scratch
`go/parser`/`go/ast` walk (written standalone, not reusing any campaign
tooling), and a raw `grep -c "This member is required." api_op_*.go` total
per module. All three agreed exactly: `ses` 13/13 fields (13 ops-with-
required, grep-c 111 total across input+output); `athena` 12/12 (8
ops-with-required); `comprehend` 12/12 (6 ops-with-required). `git status`
showed only `services/sagemaker/*` dirty from the concurrent agent's
conversion throughout, confirmed untouched.

Resolved `ses`'s module deliberately: directory and module both `ses`
(no `dirModuleOverride` entry needed), pinned at `v1.37.4` -- confirmed
distinct from `sesv2` (`v1.66.4`, already settled batch 21, a separate
REST-JSON service with its own go.mod line). `athena`/`comprehend` also
need no override (directory == module name).

**ses (13 fields / 71 ops, 13 ops-with-required, 2 findings / 4 fixes):**
query-XML protocol, not JSON -- the required-member contract is identical
(same Smithy "This member is required." annotations), but the emulator's
own `omitempty` is an `encoding/xml` struct tag here, and its interaction
with the real SDK's query-protocol deserializer needed checking directly
rather than assumed from the JSON-protocol precedent. An AST walk of all 31
domain structs in `ses@v1.37.4/types/types.go` with at least one required
member found the real depth: `GetIdentityDkimAttributes`/
`GetIdentityMailFromDomainAttributes`/`GetIdentityNotificationAttributes`/
`GetIdentityVerificationAttributes` each wrap a `map[string]<Attrs>` whose
value type carries its own required members one level below the flat
op-level scan (`IdentityDkimAttributes`: 2, `IdentityMailFromDomainAttributes`:
3, `IdentityNotificationAttributes`: 4, `IdentityVerificationAttributes`: 1).

Read the real query-protocol deserializer for each (`awsAwsquery_deserializeDocumentIdentity*`,
`deserializers.go`) before flagging anything, because query/XML introduces a
distinction the JSON-protocol passes didn't need to make explicitly: whether
the underlying real-SDK field is a **pointer** or a **non-pointer** Go type
matters for whether an omitted XML element is even detectable by a real
client. For a non-pointer field (`BehaviorOnMXFailure BehaviorOnMXFailure`,
`MailFromDomainStatus CustomMailFromStatus`, `DkimEnabled bool`,
`DkimVerificationStatus VerificationStatus`, `VerificationStatus
VerificationStatus`), the deserializer's `case` never fires when the element
is absent, but the field keeps its Go zero value regardless -- the exact
same value a present-but-empty element (`decoder.Value()` returns `[]byte{},
nil` for a self-closing tag, confirmed in `smithy-go@v1.22.2/encoding/xml/xml_decoder.go`'s
`NodeDecoder.Value`) would also produce. Omitted vs present-empty are
indistinguishable to any real client for these, so a dead `omitempty` tag on
one of them is cleanup, not a fix -- no test can tell the difference. For a
**pointer** field (`MailFromDomain *string`, `BounceTopic`/`ComplaintTopic`/
`DeliveryTopic *string`), the two cases genuinely differ: omitted decodes to
`nil`, present-empty decodes to a non-nil pointer to `""`. This is the
provable half of a required-pointer member tagged `omitempty` in a state a
real client actually reaches (any identity that never called
`SetIdentityMailFromDomain`/`SetIdentityNotificationTopic` -- the default,
common state right after `VerifyEmailIdentity`/`VerifyDomainIdentity`).

2 findings / 4 member-level fixes, both this exact provable-pointer shape:
`GetIdentityMailFromDomainAttributes`'s `xmlMailFromDomainAttributes.
MailFromDomain` (1 fix) and `GetIdentityNotificationAttributes`'s
`xmlNotificationAttributes.BounceTopic`/`ComplaintTopic`/`DeliveryTopic` (3
fixes) were all tagged `xml:"...,omitempty"` despite the real SDK type being
`*string`/required. `BehaviorOnMXFailure`'s dead `omitempty` was also
removed alongside `MailFromDomain` as harmless cleanup (same struct, same
edit, not a distinct proven bug -- see reasoning above). Incidentally found
and fixed, outside this cut's precise required-member scope since the
members aren't Smithy-required: `xmlNotificationAttributes.HeadersInBounce/
HeadersInComplaint/HeadersInDelivery` carried XML tags (`HeadersInBounce`
etc.) that never matched the real deserializer's key names
(`HeadersInBounceNotificationsEnabled` etc.) at all -- these three were
silently dropped by every real client unconditionally, not just in an
edge-case state; fixed alongside since it's the same struct already being
touched. All 4 counted fixes proven via real `aws-sdk-go-v2/service/ses`
client round trips (`services/ses/wire_output_required_r80d_test.go`),
hand-reverted/confirmed-failing/restored, md5sum-verified byte-identical.
Every other required member across all 13 ops (`ConfigurationSet.Name`,
`ReceiptRule.Name`, `EventDestination.Name`/`MatchingEventTypes`,
`MessageId` on every `Send*` op, `DkimTokens`, `VerificationToken`,
`PolicyNames`/`Identities` lists) read end to end and confirmed always
emitted unconditionally (no `omitempty` left on anything genuinely
provable).

**athena (12 fields / 70 ops, 8 ops-with-required, 0 bugs):** already swept
for this exact bug class by a dated, well-documented prior pass -- `services/
athena/PARITY.md`'s existing `GetSessionEndpoint`/`CreatePresignedNotebookUrl`/
`GetResourceDashboard` entries explicitly describe finding and fixing the
identical "response shape missing required members" class this cut targets
(e.g. `GetSessionEndpoint` used to return `{SessionEndpoint: url}` against a
real required 3-member shape `{EndpointUrl, AuthToken,
AuthTokenExpirationTime}`). Re-read all 8 ops-with-required end to end
rather than trusting the prior pass's dates: confirmed still correct, no
regression. One nested-domain-struct check found real depth the flat count
misses -- `GetCapacityReservation`/`ListCapacityReservations` wrap
`types.CapacityReservation`, which itself carries 5 required members
(`AllocatedDpus`/`CreationTime`/`Name`/`Status`/`TargetDpus`) invisible to
the per-op scan. All 5 confirmed correctly emitted except `CreationTime`
(`float64`, tagged `omitempty`) -- reviewed and ruled out as structurally
unreachable: `CreateCapacityReservation` is the sole construction site
(confirmed via a repo-wide grep for `CapacityReservation{`) and
unconditionally sets it to `time.Now().UnixMilli()`-derived epoch seconds,
which is never exactly zero at any real point in time. Same dead-tag class
batch 23 first applied to awsconfig's `Connector`/`ConnectorSummary`.
`GetCapacityAssignmentConfiguration`'s wrapped `CapacityAssignmentConfiguration`
and `ListExecutors`'s nested `ExecutorsSummary`/`Executor.ExecutorId`
confirmed clean (the former has zero required members on the real SDK type
at all; the latter's `ExecutorId` is always non-empty, no `omitempty`). No
code changes.

**comprehend (12 fields / 85 ops, 6 ops-with-required, 0 bugs):** the 6
`BatchDetect*` ops (`Sentiment`/`Entities`/`KeyPhrases`/`Syntax`/
`DominantLanguage`/`TargetedSentiment`) each require `ErrorList`/
`ResultList` -- both already documented `wire: ok` by a prior pass
(`services/comprehend/PARITY.md`'s `BatchDetect-family` row) specifically
for the "if there are no errors in the batch, ErrorList is empty" partial-
failure semantics this cut cares about. Confirmed both are built via
non-nil `make(...)` slices in the shared `batch()` wrapper
(`handler_detection.go`) and returned unconditionally in the response map,
never gated on length. Checked every nested `*ItemResult` type
(`BatchDetectSentimentItemResult`, `...DominantLanguageItemResult`,
`...KeyPhrasesItemResult`, `...EntitiesItemResult`, `...SyntaxItemResult`,
`...TargetedSentimentItemResult`) and `BatchItemError` against
`comprehend@v1.43.4/types/types.go` directly via the AST walk -- all 6
declare **zero** required members in the real Smithy model, so the flat
op-level count (12 fields/6 ops) is already the complete surface with no
nested-domain-struct undercount, unlike ses/athena this batch. No code
changes.

All three services' gates green (build/vet/gofmt/race-test/lint, 0 banned
nolints, 0 new nolints); repo-wide `go build ./...`, `go vet ./...`, `go vet
-tags e2e ./...`, `go vet -tags integration ./...` all clean (only sagemaker
dirty, untouched). No exported signatures changed.
`services/_REQUIRED_OUTPUT_CANDIDATES.md` updated: all three moved into
"Already examined" (settled-services count now 49, 2541 required output
fields read end to end); `rekognition` and `timestreamquery` (tied at 11
each) are now the largest remaining candidates after sagemaker. Did not
attempt a fourth service this batch.

### rekognition + timestreamquery (batch 25): 1 bug, taken alphabetically after re-verifying the tie

Instrument re-validated three ways before picking a candidate: the existing
`cmd/requiredoutputfields` char-level brace matcher, a fresh standalone
`go/parser`/`go/ast` walk, and a raw `grep -c "This member is required."`
total per module's `api_op_*.go` files. All agreed exactly: rekognition
11/11 fields (5 ops-with-required, grep-c 116 total across all api_op
files, input+output structs combined); timestreamquery 11/11 (7
ops-with-required, grep-c 31). No discrepancy. A fresh `go run
./cmd/requiredoutputfields` run confirmed rekognition/timestreamquery still
tied at 11 each, the largest remaining candidates after sagemaker (off-limits
all batch; `git status` showed only `services/sagemaker/*` dirty from a
concurrent agent's conversion throughout, confirmed untouched). Neither
service's directory name diverges from its SDK module name (both resolve
directly: `rekognition`@v1.54.4, `timestreamquery`@v1.39.4) -- confirmed via
`cmd/requiredoutputfields`'s `dirModuleOverride` table, neither is listed.
Took both in one batch, full rigour.

**rekognition (11 fields / 75 ops, 5 ops-with-required, 0 bugs):**
`CreateFaceLivenessSession`/`GetFaceLivenessSessionResults` always populate
`SessionId`/`Status` from non-empty backend state (`face_liveness.go`'s
`uuid.NewString()` and a fixed `jobStatusSucceeded` constant) -- neither
field is ever reachably empty, and both wire tags lack `omitempty` regardless.
`StartMediaAnalysisJob`/`GetMediaAnalysisJob`/`ListMediaAnalysisJobs` funnel
through `GetMediaAnalysisJobOutput`'s 6 required members (`CreationTimestamp`/
`Input`/`JobId`/`OperationsConfig`/`OutputConfig`/`Status`), 2 of which
(`Input`, `OutputConfig`) wrap nested domain structs -- `types.
MediaAnalysisInput.S3Object` and `types.MediaAnalysisOutputConfig.S3Bucket`
are BOTH required one level deeper, invisible to the flat op-level scan
(the same nested-domain-struct undercount class every batch since 6 has
found bugs in). Checked both against `handler_media_analysis.go` directly:
already correctly wired by a prior pass, with an explicit doc comment on
`handleStartMediaAnalysisJob` citing `validateOpStartMediaAnalysisJobInput`
(validators.go) as the source of the required-nested-member analysis --
both are rejected as `ErrValidation` if absent on `CreateMediaAnalysisJob`'s
own required-input path, so both are always non-nil/non-empty by
construction on every stored job, and `mediaAnalysisInputFromDomain`/
`mediaAnalysisOutputConfigFromDomain` always construct non-nil wrapper
structs. `types.MediaAnalysisOperationsConfig` (also required, wraps
`OperationsConfig`) declares zero required members itself (only
`DetectModerationLabels`, optional) -- correctly optional either way.
`ListMediaAnalysisJobsOutput.MediaAnalysisJobs` and `PrepareQuery`-style
list-building are already non-nil via `make(...)`. No code changes.

**timestreamquery (11 fields / 15 ops, 7 ops-with-required, 1 bug):** not
the "one wrapper key" shape -- each op's required members sit directly on
its own `<Op>Output` struct, but 2 of the 7 ops (`DescribeScheduledQuery`,
`ListScheduledQueries`) wrap domain structs with further nested required
members invisible to the flat scan: `types.ScheduledQueryDescription` (6
required: Arn/Name/NotificationConfiguration/QueryString/
ScheduleConfiguration/State) and `types.ScheduledQuery` (3 required:
Arn/Name/State) respectively, each themselves wrapping deeper required
structs (`NotificationConfiguration.SnsConfiguration.TopicArn`,
`ScheduleConfiguration.ScheduleExpression`, and -- only reachable through
`ScheduledQueryDescription`'s optional `TargetConfiguration` ->
`TimestreamConfiguration`, once present -- `DatabaseName`/
`DimensionMappings`/`TableName`/`TimeColumn`, all 4 required). Read every one
of these against `scheduled_queries.go`/`handler_scheduled_queries.go`.

1 bug: `TargetConfiguration.TimestreamConfiguration` was missing 2 of its 4
required members (`TimeColumn`/`DimensionMappings`) entirely -- the request
struct only ever declared `DatabaseName`/`TableName`, so a real client's
fully valid `CreateScheduledQueryInput` (the SDK's own client-side validator
requires all 4 once `TargetConfiguration` is set at all, per
`validateTimestreamConfiguration`) silently lost both on ingest, and
`DescribeScheduledQuery` echoed back an incomplete `TimestreamConfiguration`
forever after. Fixed by adding `TargetTimeColumn`/`TargetDimensionMappings`
to the domain model and threading them through request parsing, the
`StorageBackend` interface, and the response view. Proven via a real
`aws-sdk-go-v2/service/timestreamquery` client round trip
(`wire_output_required_r80d_test.go`), hand-reverted (7 files together)/
confirmed-failing/restored, md5sum-verified byte-identical. See
`services/timestreamquery/PARITY.md`'s 2026-08-21 Notes #12 for full detail.

Reviewed and ruled OUT, not bugs (both services): timestreamquery's
`NotificationConfiguration`/`ScheduleConfiguration` wrapper-omission gates
in `scheduledQueryToView` are unreachable via any real client, because
gopherstack's own `handleCreateScheduledQuery` independently rejects an
empty `TopicArn`/`ScheduleExpression` as `ValidationException` -- stricter
than the real SDK's client-side validators, which only reject a nil pointer
(same ruled-out class batch 23 established for codeconnections'
`RepositorySyncDefinition.Parent`). `PrepareQueryOutput.Columns`
(`types.SelectColumn`, wrapped) declares zero required members in the real
Smithy model; `Query.ColumnInfo`/`PrepareQueryOutput.Parameters`
(`types.ColumnInfo`/`types.ParameterMapping`) both have their required
members (`Type`, `Name`+`Type`) always populated unconditionally by
`marshalColumnInfos`/`inferColumnsFromSQL` -- the one apparent
conditional-omission (`Name` only added if non-empty) is dead code for
`Parameters`, since `inferColumnsFromSQL` always assigns a non-empty
`"param%d"` name to every real parameter it detects.

Both services' gates green (build/vet/gofmt/race-test/lint, 0 banned
nolints, 0 new nolints); repo-wide `go build ./...`,
`go vet ./...`, `go vet -tags e2e ./...`, `go vet -tags integration ./...`
all clean (only sagemaker dirty, untouched). One exported signature changed
(`StorageBackend.CreateScheduledQuery` / `InMemoryBackend.CreateScheduledQuery`
gained 2 trailing parameters) -- all 13 existing direct-backend test call
sites updated, all 2 SDK-client-based tests needed no changes.
`services/_REQUIRED_OUTPUT_CANDIDATES.md` updated: both moved into "Already
examined" (settled-services count now 51, 2563 required output fields read
end to end); `cloudformation` and `emr` (tied at 10 each) are now the largest
remaining candidates after sagemaker. Did not attempt a third service this
batch.
