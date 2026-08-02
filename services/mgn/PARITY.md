---
# PARITY MANIFEST — IMPLEMENTED THIS PASS. See "Implementation summary (this pass)" below the
# frontmatter for the hard-design-problem decisions, corrections found, and gate results. The
# original pre-implementation audit prose (everything from "## Purpose of this document" onward)
# is left otherwise unmodified as the wire-shape ground truth the implementation was built from.
# services/mgn/ does not exist yet (confirmed: no dir before this file was written, no cli.go
# registration, no go.mod entry, zero Go symbols anywhere in the tree -- grepped case-insensitively
# for "\bmgn\b" across services/ and cli.go: zero hits after excluding false positives; grepped for
# "migration"/"Migration": only dms/opensearch/elasticache/ec2/waf unrelated hits, confirmed by
# reading each hit's context, none reference AWS Application Migration Service). This document is a
# wire-shape + behavior SPEC for the implementer, not a record of existing code. No .go files were
# written to produce it; every claim below was read directly from the SDK module cache, grepped/read
# from this repo's existing services, or fetched from botocore's service model / the real Terraform
# AWS provider source (cited per-claim).
service: mgn
sdk_module: aws-sdk-go-v2/service/mgn@v1.48.3   # resolved via `go get .../mgn@latest` in a throwaway
# scratch module (`go mod init probe && go get`, run in this session's scratchpad, NEVER touching
# this repo's go.mod -- another agent was concurrently editing go.mod/go.sum/cli.go during this pass;
# this audit did not read or write any of those three files).
last_audit_commit: 7922e4c4d   # HEAD when this manifest was written; there is no prior MGN code in
# the tree at all, so this is a from-scratch pre-implementation audit, matching the directconnect/
# outposts/resiliencehub audits done in the same pass.
last_audit_date: 2026-08-01
overall: B   # implemented this pass, all 95 ops routed/backed/persisted; see "Implementation
# summary" section immediately below for the hard-design-problem decisions (SeedSourceServer/
# SeedVcenterClient, NetworkMigrationExecutionID auto-vivification, mapper segments left genuinely
# empty), two corrections this pass found in its own pre-implementation audit, and gate results.
# All 95 ops confirmed present in aws-sdk-go-v2/service/mgn@v1.48.3 (`ls api_op_*.go | grep -v
# _test.go | wc -l` => 95, matching this task's ~95 estimate exactly). None are implemented.
# Method/path verified by parsing every awsRestjson1_serializeOp<Op>.HandleSerialize's
# httpbinding.SplitURI(...) literal and request.Method assignment in serializers.go via a Python
# regex pass over the whole file (all 95 matched, not sampled -- see Notes for the extraction
# method). Error sets verified by parsing every op's own awsRestjson1_deserializeOpError<Op> switch
# body in deserializers.go for strings.EqualFold("X", errorCode) case literals (all 95, not sampled
# from the shared types/errors.go list, which enumerates all 8 shapes without saying which ops use
# which -- same trap the directconnect/outposts/resiliencehub audits flagged).
# Grouped by family per this task's own guidance (95 ops is too many for 95 prose blocks); every op
# appears in exactly one family table in the body below.
families:
  source_server_lifecycle: {status: gap, note: "16 ops: DescribeSourceServers, UpdateSourceServer, UpdateSourceServerReplicationType, DeleteSourceServer, ChangeServerLifeCycleState, DisconnectFromService, FinalizeCutover, MarkAsArchived, StartTest, StartCutover, StartReplication, StopReplication, PauseReplication, ResumeReplication, RetryDataReplication, TerminateTargetInstances. No CreateSourceServer op exists anywhere in this 95-op surface -- see gaps."}
  jobs: {status: gap, note: "3 ops: DescribeJobs, DescribeJobLogItems, DeleteJob."}
  launch_configuration: {status: gap, note: "6 ops: per-server GetLaunchConfiguration/UpdateLaunchConfiguration (flattened wire shape, no types.LaunchConfiguration struct exists) plus the separate LaunchConfigurationTemplate family (Create/Delete/Describe/Update)."}
  replication_configuration: {status: gap, note: "6 ops: per-server GetReplicationConfiguration/UpdateReplicationConfiguration (flattened, no types.ReplicationConfiguration struct exists) plus the separate ReplicationConfigurationTemplate family (Create/Delete/Describe/Update)."}
  applications: {status: gap, note: "8 ops: CreateApplication, UpdateApplication, DeleteApplication, ListApplications, ArchiveApplication, UnarchiveApplication, AssociateSourceServers, DisassociateSourceServers."}
  waves: {status: gap, note: "8 ops: CreateWave, UpdateWave, DeleteWave, ListWaves, ArchiveWave, UnarchiveWave, AssociateApplications, DisassociateApplications."}
  connectors: {status: gap, note: "4 ops: CreateConnector, UpdateConnector, DeleteConnector, ListConnectors."}
  vcenter_clients: {status: gap, note: "2 ops: DescribeVcenterClients (the ONLY GET besides the tagging trio), DeleteVcenterClient. No CreateVcenterClient op exists -- see gaps."}
  export_import: {status: gap, note: "8 ops: StartExport/ListExports/ListExportErrors, StartImport/ListImports/ListImportErrors, plus StartImportFileEnrichment/ListImportFileEnrichments which live under the /network-migration/ path despite being about the core-MGN import flow, not network migration -- see wire-shape traps."}
  actions: {status: gap, note: "6 ops: PutSourceServerAction/ListSourceServerActions/RemoveSourceServerAction and the template-scoped PutTemplateAction/ListTemplateActions/RemoveTemplateAction -- post-launch custom SSM-document actions, two distinct but structurally near-identical families."}
  service_init: {status: gap, note: "2 ops: InitializeService, ListManagedAccounts."}
  tagging: {status: gap, note: "3 ops: TagResource/UntagResource/ListTagsForResource, the only ops sharing the /tags/{resourceArn} path and a distinct error set (AccessDenied/InternalServer/ResourceNotFound/Throttling/Validation) from every other op family in this service."}
  network_migration_definitions: {status: gap, note: "13 ops under /network-migration/: CreateNetworkMigrationDefinition, GetNetworkMigrationDefinition, UpdateNetworkMigrationDefinition, DeleteNetworkMigrationDefinition, ListNetworkMigrationDefinitions, GetNetworkMigrationMapperSegmentConstruct, ListNetworkMigrationMapperSegmentConstructs, ListNetworkMigrationMapperSegments, UpdateNetworkMigrationMapperSegment, ListNetworkMigrationMappings, ListNetworkMigrationMappingUpdates, StartNetworkMigrationMapping, StartNetworkMigrationMappingUpdate. This is a structurally separate sub-product (network-topology analysis/codegen/deployment) bolted onto the MGN API namespace -- see Missing simulated functionality."}
  network_migration_analysis_deploy: {status: gap, note: "10 ops under /network-migration/: StartNetworkMigrationAnalysis, ListNetworkMigrationAnalyses, ListNetworkMigrationAnalysisResults, StartNetworkMigrationCodeGeneration, ListNetworkMigrationCodeGenerations, ListNetworkMigrationCodeGenerationSegments, StartNetworkMigrationDeployment, ListNetworkMigrationDeployments, ListNetworkMigrationDeployedStacks, ListNetworkMigrationExecutions. CRITICAL GAP: no op anywhere in this 95-op surface CREATES a NetworkMigrationExecutionID (StartNetworkMigrationMapping/Analysis/CodeGeneration/Deployment all REQUIRE one as input; ListNetworkMigrationExecutions only lists, never creates) -- see gaps."}
gaps:
  - "Zero operations implemented -- from-scratch audit only, per this task's explicit instructions not to write any .go files. All 95 ops need building. (bd: none filed yet by this pass -- filing is the implementer's responsibility per the standard workflow.)"
  - "No CreateSourceServer op exists anywhere in this SDK's 95 operations. In real AWS, a SourceServer record is created only by the MGN Replication Agent (installed on the actual on-prem/cloud source machine) calling an internal, non-public control-plane API to register itself -- that registration call is NOT part of this public SDK surface at all. The only PUBLIC-API path that creates SourceServer records is StartImport's bulk CSV import (types.ImportTaskSummaryServers.CreatedCount confirms StartImport creates them), which is a metadata-only bulk-load mechanism (for migration-wave planning), not a live-replicating-agent registration. An implementer needs a deliberate, explicitly-documented decision for how SourceServer records get seeded in this emulator (e.g. a gopherstack-only synthetic 'RegisterSourceServer'-equivalent, or requiring StartImport as the only creation path) -- there is no way to derive AWS's real internal registration call from this SDK, and inventing one would be fabrication."
  - "No CreateVcenterClient op exists either, for the same reason: VcenterClient records are created by the MGN vCenter connector appliance registering itself, not via any public API in this surface. DescribeVcenterClients/DeleteVcenterClient are read/delete only."
  - "No op creates a NetworkMigrationExecutionID. StartNetworkMigrationMapping, StartNetworkMigrationMappingUpdate, StartNetworkMigrationAnalysis, StartNetworkMigrationCodeGeneration, and StartNetworkMigrationDeployment all take NetworkMigrationExecutionID as a REQUIRED input field (confirmed by reading all five api_op_*.go Input structs directly), and ListNetworkMigrationExecutions only lists existing ones filtered by NetworkMigrationDefinitionID -- it has no create-side counterpart. Either AWS's real console/internal API creates executions through a channel not exposed in this public SDK, or execution creation is an implicit side effect of some other call not documented as such in the Go types. This audit could not resolve which, and does not guess -- an implementer must treat NetworkMigrationExecutionID as coming from an unconfirmed source and pick a defensible convention (e.g. minting one automatically the first time StartNetworkMigrationMapping is called for a given definition with no prior execution), documenting the choice explicitly rather than presenting it as derived from AWS's real behavior."
  - "The Network Migration sub-product (CreateNetworkMigrationDefinition through StartNetworkMigrationDeployment/ListNetworkMigrationDeployedStacks -- 25 of the 95 ops, wire-routed under /network-migration/) analyzes exported on-prem network configuration (SourceEnvironment enum: NSX/VSPHERE/FORTIGATE_FIREWALL/PALO_ALTO_FIREWALL/CISCO_ACI/LOGICAL_MODEL/MODELIZE_IT/AWS_DISCOVERY_COLLECTOR), maps it onto a target AWS network topology (TargetNetworkTopology: ISOLATED_VPC/HUB_AND_SPOKE), generates infrastructure-as-code artifacts (NetworkMigrationCodeGenerationArtifact), and deploys them as real CloudFormation-equivalent stacks (types/types.go's own doc comment on NetworkMigrationDeployedStackDetails: 'Details about a CloudFormation stack that has been deployed as part of the network migration'). None of analysis, code generation, or deployment can be honestly performed by this emulator without either (a) a real network-analysis/codegen engine that does not exist in this repo, or (b) fabricating analysis findings and generated code as free-text strings. The state-bookkeeping shell (definitions, executions, mapper segments/constructs with their CRUD and status enums) is honestly simulatable; the analysis/codegen/deployment CONTENT is not, and should be represented as opaque placeholder text/empty artifact lists clearly flagged as such, never invented realistic-looking network analysis output."
  - "Terraform's AWS provider has ZERO MGN resources: `internal/service/mgn/` (confirmed via GitHub API directory listing) contains only 4 auto-generated boilerplate files (generate.go, service_endpoint_resolver_gen.go, service_endpoints_gen_test.go, service_package_gen.go) with FrameworkResources()/SDKResources() both returning empty slices -- no application.go/source_server.go/wave.go etc. exist. This means, unlike directconnect/outposts, there is no Terraform-provider-source corroboration available at all for any MGN ARN resource-path format (source-server/application/wave/job/launch-configuration-template/replication-configuration-template/connector/vcenter-client/network-migration-definition/...). AWS's own Service Authorization Reference page for MGN returned only a JS-shell body to WebFetch (same failure mode the outposts/grafana audits hit on the same docs.aws.amazon.com domain). The ONLY corroborating evidence found this pass is botocore's service-2.json metadata (`endpointPrefix`/`serviceId`/`signingName` all literally \"mgn\"), which is consistent with (but does not prove) the ARN service segment also being \"mgn\" -- this is the overwhelmingly common case across AWS services but not a guarantee (efs/stepfunctions/several others in this repo's own campaign history diverge). Every specific resource-path segment below (e.g. \"source-server/<id>\") is this audit's best-effort guess from AWS naming convention, NOT a confirmed value -- flagged honestly rather than presented as verified."
  - "No AWS::MGN::* CloudFormation resource type exists in this repo (`grep -rli 'mgn\\b' services/cloudformation/` returned zero hits across all 71 resources_*.go files) -- confirmed absent, not silently skipped. This is consistent with MGN being an operational/orchestration API (agent-driven replication, time-boxed cutover jobs) rather than typical declarative infrastructure; this audit found no evidence AWS's real CloudFormation supports MGN resources either, but that claim is about this repo's tree, not independently verified against AWS's own CFN resource-type registry."
  - "AccountID (an optional field for acting on behalf of a delegated/managed AWS Organizations member account) appears on nearly every legacy per-source-server/job/wave/application op, but is ABSENT from every LaunchConfigurationTemplate/ReplicationConfigurationTemplate/Connector/VcenterClient op and from every one of the 25 /network-migration/ ops (confirmed: `grep -L AccountID api_op_*.go` lists exactly those, 42 files). A full ListManagedAccounts/delegated-admin simulation (real AWS Organizations multi-account MGN management) is a real, non-trivial cross-account feature this audit did not scope in -- an honest first implementation likely just returns the calling account's own resources regardless of AccountID, clearly documented as not simulating cross-account delegation, rather than fabricating other accounts' data."
  - "EC2 instance launch on cutover/test (StartTest/StartCutover -> eventual LaunchedInstance.Ec2InstanceID) is real, launchable functionality in this repo: services/ec2 has a working RunInstances handler (services/ec2/handler_instances_lifecycle.go:119, handleRunInstances) and snapshot creation (services/ec2/handler_snapshots.go), IAM has role creation (services/iam/handler_roles.go), and KMS/EC2 store types for subnets/security groups exist (services/ec2/store.go). A real implementation COULD launch actual gopherstack EC2 instances from LaunchConfiguration/ReplicationConfiguration settings on Job completion rather than returning an invented instance id -- see Cross-service wiring for what this would require and why it is scoped as a follow-on, not a first-pass requirement."
deferred:
  - "Nothing implemented yet, so nothing has been implementation-level-audited beyond the wire-shape/error-set inventory above."
leaks: {status: clean, note: "N/A -- nothing implemented yet, so there is nothing to leak. Next pass (implementation) must revisit this per parity-principles.md: DataReplicationState progression (INITIATING->INITIAL_SYNC->BACKLOG->CONTINUOUS, or ->RESCAN/STALLED/DISCONNECTED), Job status progression (PENDING->STARTED->COMPLETED) for StartTest/StartCutover/TerminateTargetInstances, and any LifeCycleState timer-driven auto-advance (following services/eks's scheduleClusterActivation / services/grafana's analogous pattern, both using pkgs/worker) all need Close()/Reset() wiring, same as every other timer-driven service in this tree."}
---

## Implementation summary (this pass)

All 95 operations implemented: routed via a flat method+operation-name dispatch table
(`handler.go`'s `routes()`, merged from 8 per-family `handler_*.go` builder functions, matching
`operationSegment`'s handling of the `/network-migration/` prefix and the `/tags/{resourceArn}`
trio), backed by real `InMemoryBackend` state (`pkgs/store.Table`/`Index` per resource kind, one
coarse `lockmetrics.RWMutex`), and persisted via `Snapshot`/`Restore` (`persistence.go`).
`sdk_completeness_test.go` passes with an empty exception list. A real `aws-sdk-go-v2/service/mgn`
client round-trips against every major flow (`sdk_roundtrip_test.go`, 13 tests) — this caught two
real wire-shape bugs before they shipped (see "Corrected during implementation" below), not after.

### The hard design problem — decision made

**SourceServer/VcenterClient creation**: `StartImport`/`StartExport` are implemented as fully
honest bookkeeping (`ImportTask`/`ExportTask` progress `PENDING -> STARTED -> SUCCEEDED` on the
same deterministic timer every other async resource in this package uses); `StartImport`'s
`Summary` counts are always zero and `StartExport`'s are a real live count of this account's
Applications/Waves/SourceServers — neither ever reads or fabricates real S3 object content, since
no schema for that content exists anywhere in this SDK to derive one from. Given that leaves the
entire 70-op replication surface unreachable through the public API (the actual point of this
service), this package adds `SeedSourceServer` (`sourceservers.go`) and `SeedVcenterClient`
(`vcenterclients.go`): EXPORTED, non-SDK Go functions, reachable only by calling into this package
directly, never routed by `handler.go`, never described as SDK operations in
`GetSupportedOperations()`. They are the explicit, documented emulator decision the task asked
for — this package's own round-trip tests use them to seed a server before exercising StartTest/
StartCutover/FinalizeCutover/etc. A newly seeded SourceServer starts `NOT_READY`/`INITIATING` and
progresses to `READY_FOR_TEST`/`CONTINUOUS` over 3 `asyncTransitionDelay` ticks
(`sourceservers.go`'s `scheduleReplicationLocked`) — the same honest, deterministic, time-based
walk a real `StartImport`-seeded server would need, just reachable without inventing an S3
file-format schema.

**NetworkMigrationExecutionID**: no op creates one; all 5 `StartNetworkMigration*` ops require one
as input. This package's decision: `resolveOrCreateExecutionLocked` (`networkmigrationjobs.go`)
auto-vivifies a `NetworkMigrationExecution` the first time any of the 5 Start* ops references an
(DefinitionID, ExecutionID) pair not previously seen — generalizing the task's own suggested
convention ("minting one automatically the first time StartNetworkMigrationMapping is called") to
all five entry points, since none is more privileged than the others as a creation trigger. Every
subsequent Start* call against the same pair updates that execution's Activity/Stage/Status in
place rather than minting a second record.

**Mapper segments/constructs**: deliberately the OTHER option the task weighed — "leave the
families genuinely empty and record it as a gap" — rather than a third synthetic seeding seam.
`ListNetworkMigrationMapperSegments`/`ListNetworkMigrationMapperSegmentConstructs` always return
empty (after validating the (definition, execution) scope exists);
`GetNetworkMigrationMapperSegmentConstruct`/`UpdateNetworkMigrationMapperSegment` always 404. No
segment is ever produced by any op (there is no real network-analysis engine to produce one from),
and mapper segments back only bookkeeping display within the already-honest-gapped Network
Migration analysis sub-feature, not the primary 70-op replication surface — a smaller payoff than
SourceServer/VcenterClient's seam, so a second `Seed*` convenience was not added for it.

**Network Migration analysis/codegen/deployment content**: `StartNetworkMigrationAnalysis`/
`CodeGeneration`/`Deployment` progress a shared internal `NetworkMigrationJob` bookkeeping record
(one generic type backing all 5 real job-details SDK shapes, which are byte-identical in field
layout — `networkmigrationjobs.go`) through `PENDING -> STARTED -> SUCCEEDED`, and the parent
execution's own `Status` mirrors it on completion. `ListNetworkMigrationAnalysisResults`/
`ListNetworkMigrationCodeGenerationSegments`/`ListNetworkMigrationDeployedStacks` always return
empty `Items`, even after the parent job SUCCEEDS — real analysis findings, generated
infrastructure code, and deployed CloudFormation stacks all require engines this repo does not
have, and PARITY.md's own honest-gap section is explicit that fabricating this content "would
misrepresent what the emulator actually did."

**Two error generations**: implemented as documented, per-op — `requireInitializedLocked` gates
every legacy op (69 of 95, confirmed by direct per-op extraction — see below), never called by the
tagging trio or any `/network-migration/` op; `classifyMGNError` (`handler.go`) maps all 8 wire
exception shapes, but `errAccessDenied`/`errQuotaExceeded`/`errThrottling` are never actually
constructed by any call site in this pass (no permission/quota/rate-limiter model exists to trigger
them honestly) — documented explicitly in `errors.go` as a real, deliberate gap rather than a
forced fake trigger just to exercise the constructor.

**Flattened-vs-nested outputs**: `sourceServerWire`/`toSourceServerWire` back the 11
flattened-SourceServer ops; `StartTest`/`StartCutover`/`TerminateTargetInstances` return a nested
`jobEnvelope{Job: ...}` instead (`handler_sourceservers.go`); `GetLaunchConfiguration`/
`GetReplicationConfiguration` flatten their own distinct wire shapes with no backing named SDK type
at all, matching internal `LaunchConfiguration`/`ReplicationConfiguration` types this package
invented for exactly that purpose (`models.go`).

**EC2 cutover scope**: narrowed, explicitly. `LaunchedInstance.Ec2InstanceID` is a synthetic,
gopherstack-format ID (`"i-"` + hex, `jobs.go`'s `newSyntheticInstanceID`), never cross-checked
against a real `services/ec2` instance. Real EC2 instance launch on cutover (the directconnect
precedent this task asked to weigh) was assessed and NOT done this pass — flagged explicitly as a
follow-on, not silently skipped.

### Corrected during implementation

1. **`UpdateNetworkMigrationMapperSegmentInput` has only one mutable field, `ScopeTags`** — the
   pre-implementation audit's family-M table said "rest optional (ScopeTags, TargetAccount)",
   but direct re-read of the real Input struct during implementation found no `TargetAccount`
   field on it at all (confirmed: `TargetAccount` only appears on the *segment's own* stored shape,
   never as an input the caller can set via this op) — documented at `networkmigration.go`'s
   `UpdateNetworkMigrationMapperSegment`.
2. **`types.ManagedAccount.AccountId` wire-serializes as `"accountId"` (lowercase `d`), not
   `"accountID"`** — every other `AccountID`-suffixed field in this SDK (69 legacy ops' own
   `AccountID *string`) is the Go field `AccountID` (capital ID) wire-keyed `"accountID"`, but
   `ManagedAccount`'s field is spelled `AccountId` (lowercase `d`) in the Go source itself, which
   this SDK's own lowercase-first-rune convention serializes to `"accountId"`. First caught by
   `TestRoundTrip_ManagedAccounts` failing against the real SDK client, not by static inspection —
   exactly the class of bug this campaign's SDK-round-trip-test standard exists to catch. Fixed in
   `wire.go`'s `managedAccountWire`.

### Judgment calls, each documented at its call site

- ARN resource-path segments (`store.go`'s ARN builders) are this package's best-effort,
  UNCONFIRMED guesses from AWS naming convention (e.g. `"source-server/"`, `"application/"`,
  `"vcenter-client/"`) — Terraform's AWS provider has zero MGN resources to corroborate against
  (confirmed by the pre-implementation audit), so unlike directconnect/outposts there is no
  Terraform-source cross-check available at all for any of the 12 taggable kinds.
- ID formats (`store.go`'s `new*ID` functions) are similarly UNCONFIRMED hex-suffix conventions
  (e.g. `"s-"` + 16 hex for SourceServer) — no doc-comment ID-shape examples exist anywhere in this
  SDK module to confirm against, unlike directconnect's own published `"dxcon-ffabc123"` examples.
- `LifeCycleState`/`DataReplicationState` transition tables (`sourceservers.go`'s package doc
  comment) are this package's own inference from field/enum semantics, not independently
  SDK-confirmed — AWS's real state machine is not published anywhere in this SDK.
- Application/Wave `AggregatedStatus` rollup rules (`applications.go`'s `rollupHealthStatus`/
  `rollupProgressStatus`, `waves.go`'s analogous pair) are documented, invented aggregation rules
  (e.g. a Wave is `LAGGING` if any member Application is `LAGGING`), not SDK-specified.
- Job progression (`jobs.go`) is a deterministic, always-succeeds 4-tick simulation — no
  `JobLogEvent` describing failure/skip/cancel is ever emitted, since no real launch engine exists
  to fail; `JobStatus` itself has no `FAILED` value at all, confirmed by direct SDK read.
- Timestamps: every SDK member typed as a `*string` "DateTime"-suffixed field (confirmed via direct
  read to deserialize via a bare `value.(string)` type assertion, not `smithytime`) is wire-coded as
  an RFC3339 string by this package's own convention (`store.go`'s `nowRFC3339`); every real smithy
  `*time.Time` field (the Network Migration family's `CreatedAt`/`UpdatedAt`/`EndedAt`) is
  epoch-seconds via `pkgs/awstime.Epoch`, confirmed against the SDK's own
  `smithytime.ParseEpochSeconds` deserializer call.

### Gate results (see final report for full output)

`go build ./...`, `go vet ./...`, `go vet -tags e2e ./test/e2e/...`, `gofmt -l`, and
`golangci-lint run ./services/mgn/... .` (0 issues) all clean. `go test -race -count=1
./services/mgn/...` run 3 times, all 3 clean. `grep -rnE '//nolint:.*(funlen|gocyclo|gocognit|cyclop)'
services/mgn/` empty — every complexity issue golangci-lint's `cyclop`/`gocognit` flagged during
this pass (`UpdateLaunchConfigurationTemplate`, `scheduleJobLocked`, `scheduleReplicationLocked`)
was fixed by decomposing into named helper functions, never suppressed.

## Purpose of this document

`services/mgn/` does not exist. This file is a pre-implementation audit: a complete SDK operation
inventory plus a behavioral spec, written so a follow-up implementation pass does not have to
re-derive wire shapes from the SDK source itself. No `.go` files were touched to produce it. All 95
operation names, the wire protocol, every operation's exact per-op exception set, and every
shared type/enum below were read directly from `aws-sdk-go-v2/service/mgn@v1.48.3`'s
`serializers.go` / `deserializers.go` / `types/types.go` / `types/enums.go` / `types/errors.go` /
individual `api_op_*.go` files in the module cache (resolved via a throwaway `go mod init probe &&
go get .../mgn@latest` in the scratch dir — **not** added to this repo's `go.mod`, which another
agent was concurrently editing during this pass).

## 1. Complete SDK operation inventory

**95 operations**, SDK version **`v1.48.3`** (resolved 2026-08-01, whatever `@latest` currently
resolves to — not a version pinned by this audit). This matches the task's ~95 estimate exactly:

`ls api_op_*.go | grep -v _test.go | wc -l` against
`/home/agbishop/go/pkg/mod/github.com/aws/aws-sdk-go-v2/service/mgn@v1.48.3/` returns **95**.

Alphabetically: ArchiveApplication, ArchiveWave, AssociateApplications, AssociateSourceServers,
ChangeServerLifeCycleState, CreateApplication, CreateConnector, CreateLaunchConfigurationTemplate,
CreateNetworkMigrationDefinition, CreateReplicationConfigurationTemplate, CreateWave,
DeleteApplication, DeleteConnector, DeleteJob, DeleteLaunchConfigurationTemplate,
DeleteNetworkMigrationDefinition, DeleteReplicationConfigurationTemplate, DeleteSourceServer,
DeleteVcenterClient, DeleteWave, DescribeJobLogItems, DescribeJobs,
DescribeLaunchConfigurationTemplates, DescribeReplicationConfigurationTemplates,
DescribeSourceServers, DescribeVcenterClients, DisassociateApplications,
DisassociateSourceServers, DisconnectFromService, FinalizeCutover, GetLaunchConfiguration,
GetNetworkMigrationDefinition, GetNetworkMigrationMapperSegmentConstruct,
GetReplicationConfiguration, InitializeService, ListApplications, ListConnectors,
ListExportErrors, ListExports, ListImportErrors, ListImportFileEnrichments, ListImports,
ListManagedAccounts, ListNetworkMigrationAnalyses, ListNetworkMigrationAnalysisResults,
ListNetworkMigrationCodeGenerations, ListNetworkMigrationCodeGenerationSegments,
ListNetworkMigrationDefinitions, ListNetworkMigrationDeployedStacks,
ListNetworkMigrationDeployments, ListNetworkMigrationExecutions,
ListNetworkMigrationMapperSegmentConstructs, ListNetworkMigrationMapperSegments,
ListNetworkMigrationMappingUpdates, ListNetworkMigrationMappings, ListSourceServerActions,
ListTagsForResource, ListTemplateActions, ListWaves, MarkAsArchived, PauseReplication,
PutSourceServerAction, PutTemplateAction, RemoveSourceServerAction, RemoveTemplateAction,
ResumeReplication, RetryDataReplication, StartCutover, StartExport, StartImport,
StartImportFileEnrichment, StartNetworkMigrationAnalysis, StartNetworkMigrationCodeGeneration,
StartNetworkMigrationDeployment, StartNetworkMigrationMapping,
StartNetworkMigrationMappingUpdate, StartReplication, StartTest, StopReplication, TagResource,
TerminateTargetInstances, UnarchiveApplication, UnarchiveWave, UntagResource, UpdateApplication,
UpdateConnector, UpdateLaunchConfiguration, UpdateLaunchConfigurationTemplate,
UpdateNetworkMigrationDefinition, UpdateNetworkMigrationMapperSegment,
UpdateReplicationConfiguration, UpdateReplicationConfigurationTemplate, UpdateSourceServer,
UpdateSourceServerReplicationType, UpdateWave.

### Protocol and routing shape

Protocol is **REST-JSON** (`awsRestjson1_serializeOp<Op>` struct names throughout
`serializers.go`, 95 `HandleSerialize` methods, one per op — confirmed by direct extraction, not
sampled). Unlike a typical REST-JSON service, **every path is an action-style slug, not a resource
path with parameters** — extracted via a Python regex pass over every `HandleSerialize` method body
(all 95 matched):

- **92 of 95 ops are `POST /<OperationName>`** (e.g. `POST /CreateApplication`, `POST
  /StartCutover`) — the operation name IS the path, with zero `{param}` placeholders anywhere
  except the three tagging ops below. This is close to (but not identical to) the awsjson1.1
  action-dispatch shape directconnect uses (`POST /` with an `X-Amz-Target` header) — here the
  action name is IN the path instead of a header, so a gopherstack router can dispatch purely on
  path suffix.
- **`DescribeVcenterClients` is `GET /DescribeVcenterClients`** — the only non-tagging op that is
  not a POST (confirmed by direct extraction of its `HandleSerialize` body; every sibling `Describe*`
  op in this service, e.g. `DescribeSourceServers`/`DescribeJobs`/`DescribeJobLogItems`/
  `DescribeLaunchConfigurationTemplates`/`DescribeReplicationConfigurationTemplates`, is `POST`).
  Do not assume all `Describe*`-named ops share one HTTP method.
- **The tagging trio uses a real resource path**: `GET /tags/{resourceArn}`
  (`ListTagsForResource`), `POST /tags/{resourceArn}` (`TagResource`), `DELETE
  /tags/{resourceArn}` (`UntagResource`) — the only three ops in the entire service with a path
  parameter.
- **25 ops are namespaced under `/network-migration/<OperationName>`** rather than the bare
  `/<OperationName>` every other op uses — see the Network Migration family tables below for the
  full list. Two of those 25 (`StartImportFileEnrichment`, `ListImportFileEnrichments`) are
  semantically about import-file processing, not network topology, yet still live under the
  `/network-migration/` prefix — a naive router keying purely on "does this look like an import
  op" would misroute these two.

### Errors — 8 shared exception shapes, richer than directconnect's 5

All 8 in `types/errors.go`, confirmed by reading the file directly:

- **`AccessDeniedException`** {`Message`, `Code`} — client fault. "Operating denied due to a file
  permission or access check error."
- **`ConflictException`** {`Message`, `Code`, `ResourceId`, `ResourceType`, `Errors
  []ErrorDetails`} — client fault, the richest shape (carries a list of nested `ErrorDetails`, not
  just a flat message).
- **`InternalServerException`** {`Message`} — server fault. Only appears in the tagging trio's
  error set (see below) — no other op in this 95-op service can return it.
- **`ResourceNotFoundException`** {`Message`, `Code`, `ResourceId`, `ResourceType`} — client fault.
- **`ServiceQuotaExceededException`** {`Message`, `Code`, `ResourceId`, `ResourceType`,
  `ServiceCode`, `QuotaCode`, `QuotaValue *int32`} — client fault.
- **`ThrottlingException`** {`Message`, `ServiceCode`, `QuotaCode`, `RetryAfterSeconds *string`} —
  client fault, appears only on the tagging trio and the 25 `/network-migration/` ops, never on any
  legacy (non-network-migration) op — a real, structural split (see below).
- **`UninitializedAccountException`** {`Message`, `Code`} — client fault. "Uninitialized account
  exception" — appears on almost every legacy op (69 of 95), meaning most of this service assumes
  `InitializeService` has already been called for the caller's account; **zero** of the 25
  `/network-migration/` ops and zero of the tagging trio ever return it.
- **`ValidationException`** {`Message`, `Code`, `Reason ValidationExceptionReason`, `FieldList
  []ValidationExceptionField`} — client fault. `ValidationExceptionReason` has 4 values:
  `unknownOperation`, `cannotParse`, `fieldValidationFailed`, `other` (note: lower-camelCase wire
  values, not `SCREAMING_SNAKE_CASE` like every other enum in this service — verified directly in
  `types/enums.go`, not a transcription error in this note).

**Two structurally distinct error-set "generations" exist in this one service**, extracted per-op
from every `awsRestjson1_deserializeOpError<Op>` switch body in `deserializers.go` (all 95 read
individually, not sampled from the shared `types/errors.go` list):

1. **Legacy MGN ops** (everything except tagging and `/network-migration/`): draw from
   `{AccessDenied, Conflict, ResourceNotFound, ServiceQuotaExceeded, UninitializedAccount,
   Validation}` — never `InternalServerException` or `ThrottlingException`.
2. **Tagging trio + all 25 `/network-migration/` ops**: draw from `{AccessDenied, Conflict,
   InternalServer (tagging only), ResourceNotFound, ServiceQuotaExceeded, Throttling, Validation}`
   — never `UninitializedAccountException`. This strongly suggests the Network Migration feature
   was added later, on a newer internal service-generation template that dropped the
   init-required check and added throttling, while the tagging trio (also common to add later)
   picked up the same newer error conventions.

Per-op exact sets are given in each family table below (every one of the 95 read directly, not
inferred from this generational split — the split is a useful mnemonic, not a substitute for the
per-op ground truth).

### Wire-shape traps worth flagging up front (looks-wrong-but-correct, or just easy to miss)

1. **Every per-`SourceServer`-mutation op flattens the FULL `SourceServer` shape (13 fields: 
   `ApplicationID`, `Arn`, `ConnectorAction`, `DataReplicationInfo`, `FqdnForActionFramework`,
   `IsArchived`, `LaunchedInstance`, `LifeCycle`, `ReplicationType`, `SourceProperties`,
   `SourceServerID`, `Tags`, `UserProvidedID`, `VcenterClientID`) directly onto its own Output
   struct** — confirmed by reading `UpdateSourceServer`, `UpdateSourceServerReplicationType`,
   `ChangeServerLifeCycleState`, `FinalizeCutover`, `MarkAsArchived`, `DisconnectFromService`,
   `StartReplication`, `StopReplication`, `PauseReplication`, `ResumeReplication`,
   `RetryDataReplication` output structs directly (11 ops, byte-identical field list each time; no
   `types.SourceServer` struct is ever nested). By contrast, **`StartTest`, `StartCutover`, and
   `TerminateTargetInstances` instead return a nested `Job *types.Job`** — these three are the only
   source-server-adjacent mutations that produce an async Job rather than directly mutating and
   echoing the SourceServer. A generic "serialize the SourceServer as this op's response" helper
   is correct for 11 ops and actively wrong for these 3 — same class of trap as directconnect's
   flattened-vs-nested VirtualInterface issue.
2. **`GetLaunchConfiguration`/`UpdateLaunchConfiguration` and
   `GetReplicationConfiguration`/`UpdateReplicationConfiguration` each flatten their respective
   per-server configuration directly onto the Output struct too — there is NO `types.LaunchConfiguration`
   or `types.ReplicationConfiguration` struct anywhere in this SDK module** (confirmed: `grep -n
   "^type LaunchConfiguration struct\|^type ReplicationConfiguration struct" types/types.go` returns
   nothing; only `LaunchConfigurationTemplate` and `ReplicationConfigurationTemplate` exist as named
   types). An implementer needs a distinct internal representation for "this source server's launch
   configuration" that happens to share most field names with `LaunchConfigurationTemplate` but is
   never the same wire type.
3. **`DescribeVcenterClients` is the only non-tagging `GET`** — see routing section above. A
   router that assumes "every op in this service is POST" (a reasonable inference from the other
   94) will 405 this one specifically.
4. **`StartImportFileEnrichment`/`ListImportFileEnrichments` are wire-routed under
   `/network-migration/` despite being conceptually part of the Export/Import family** (they
   enrich an *import file*, i.e. the same CSV/JSON source-server-inventory files `StartImport`
   consumes, with additional discovered network/segment metadata for the Network Migration
   feature to consume downstream) — grouping them with `StartImport`/`ListImports` by name-pattern
   alone would misroute them.
5. **`AccountID` (optional, "act on behalf of a delegated/managed account") appears on almost every
   legacy per-source-server/job/wave/application op but is completely absent from
   `LaunchConfigurationTemplate`/`ReplicationConfigurationTemplate`/`Connector`/`VcenterClient` ops
   and from all 25 `/network-migration/` ops** — confirmed via `grep -L AccountID api_op_*.go`
   (42 files with no `AccountID` field at all, cross-checked against the family tables below). Do
   not assume every op accepts this field.
6. **`ValidationExceptionReason`'s wire values are lower-camelCase** (`unknownOperation`,
   `cannotParse`, `fieldValidationFailed`, `other`) while literally every other enum in this
   service (`LifeCycleState`, `JobStatus`, `DataReplicationState`, ...) is `SCREAMING_SNAKE_CASE`
   — verified directly in `types/enums.go`, an easy value to get wrong if hand-typed from memory
   of AWS's usual convention.
7. **`InitializeService` itself never returns `UninitializedAccountException`** (errors:
   `AccessDeniedException`/`ValidationException` only) — logically necessary (the whole point of
   the call is to get PAST the uninitialized state) but worth confirming explicitly rather than
   assuming the generational split above applies without exception; it is a legacy-generation op
   by every other signal (no `ThrottlingException`, has `AccountID`... actually `InitializeService`
   itself has no `AccountID` field either, confirmed by direct read — it initializes the CALLING
   account, not a delegated one).
8. **`ListNetworkMigrationDefinitions` is the only op in the entire service with a single-member
   error set**: `AccessDeniedException` alone (confirmed by direct read of its
   `deserializeOpError` switch) — no `ResourceNotFoundException`, no `ValidationException`, not
   even for a presumably-filterable list call.

## Family tables — every one of the 95 operations

All method/path values below come from the Python-regex extraction over `serializers.go` described
above (all 95 matched, not sampled). All error sets come from the equivalent per-op
`strings.EqualFold` extraction over `deserializers.go` (all 95, not sampled). Field lists come from
directly reading each op's `api_op_<Op>.go` Input/Output struct or the shared `types/types.go`
struct it flattens/nests.

### A. Source server lifecycle & data replication control (16 ops)

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| DescribeSourceServers | POST /DescribeSourceServers | `Filters *DescribeSourceServersRequestFilters` (ApplicationIDs[], IsArchived, LifeCycleStates[], ReplicationTypes[], SourceServerIDs[]), MaxResults, NextToken | `Items []SourceServer`, NextToken | UninitializedAccount, Validation |
| UpdateSourceServer | POST /UpdateSourceServer | SourceServerID*, AccountID, `ConnectorAction *SourceServerConnectorAction` | flattened SourceServer (trap #1) | Conflict, ResourceNotFound, UninitializedAccount |
| UpdateSourceServerReplicationType | POST /UpdateSourceServerReplicationType | SourceServerID*, ReplicationType* (AGENT_BASED\|SNAPSHOT_SHIPPING), AccountID | flattened SourceServer | Conflict, ResourceNotFound, UninitializedAccount, Validation |
| DeleteSourceServer | POST /DeleteSourceServer | SourceServerID*, AccountID | empty | Conflict, ResourceNotFound, UninitializedAccount |
| ChangeServerLifeCycleState | POST /ChangeServerLifeCycleState | SourceServerID*, `LifeCycle *ChangeServerLifeCycleStateSourceServerLifecycle`{State: READY_FOR_TEST\|READY_FOR_CUTOVER\|CUTOVER}*, AccountID | flattened SourceServer | Conflict, ResourceNotFound, UninitializedAccount, Validation |
| DisconnectFromService | POST /DisconnectFromService | SourceServerID*, AccountID | flattened SourceServer | Conflict, ResourceNotFound, UninitializedAccount |
| FinalizeCutover | POST /FinalizeCutover | SourceServerID*, AccountID | flattened SourceServer | Conflict, ResourceNotFound, UninitializedAccount, Validation |
| MarkAsArchived | POST /MarkAsArchived | SourceServerID*, AccountID | flattened SourceServer | Conflict, ResourceNotFound, UninitializedAccount |
| StartTest | POST /StartTest | SourceServerIDs*[]string (BATCH — multiple servers, one Job), AccountID, Tags | nested `Job *Job` (trap #1) | Conflict, UninitializedAccount, Validation |
| StartCutover | POST /StartCutover | SourceServerIDs*[]string (batch), AccountID, Tags | nested `Job *Job` | Conflict, UninitializedAccount, Validation |
| StartReplication | POST /StartReplication | SourceServerID*, AccountID | empty | Conflict, ResourceNotFound, ServiceQuotaExceeded, UninitializedAccount, Validation |
| StopReplication | POST /StopReplication | SourceServerID*, AccountID | flattened SourceServer | Conflict, ResourceNotFound, ServiceQuotaExceeded, UninitializedAccount, Validation |
| PauseReplication | POST /PauseReplication | SourceServerID*, AccountID | flattened SourceServer | Conflict, ResourceNotFound, ServiceQuotaExceeded, UninitializedAccount, Validation |
| ResumeReplication | POST /ResumeReplication | SourceServerID*, AccountID | flattened SourceServer | Conflict, ResourceNotFound, ServiceQuotaExceeded, UninitializedAccount, Validation |
| RetryDataReplication | POST /RetryDataReplication | SourceServerID*, AccountID | flattened SourceServer | ResourceNotFound, UninitializedAccount, Validation |
| TerminateTargetInstances | POST /TerminateTargetInstances | SourceServerIDs*[]string (batch), AccountID, Tags | nested `Job *Job` | Conflict, UninitializedAccount, Validation |

Note: `StartReplication`'s empty output is a genuine void-result op (per parity-principles.md rule
4 — confirmed by reading `api_op_StartReplication.go` directly, it really has no output fields
besides `ResultMetadata`), not a disguised stub; every sibling `*Replication` op (Stop/Pause/Resume)
DOES return the flattened SourceServer, so `StartReplication`'s emptiness is a real, deliberate
asymmetry, not an oversight in this table.

### B. Jobs (3 ops)

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| DescribeJobs | POST /DescribeJobs | `Filters *DescribeJobsRequestFilters`{FromDate, JobIDs[], ToDate}, MaxResults, NextToken, AccountID | `Items []Job`, NextToken | UninitializedAccount, Validation |
| DescribeJobLogItems | POST /DescribeJobLogItems | JobID*, MaxResults, NextToken, AccountID | `Items []JobLog`{Event, EventData, LogDateTime}, NextToken | UninitializedAccount, Validation |
| DeleteJob | POST /DeleteJob | JobID*, AccountID | empty | Conflict, ResourceNotFound, UninitializedAccount |

`Job`{JobID*, Arn, CreationDateTime, EndDateTime, InitiatedBy, `ParticipatingServers
[]ParticipatingServer`{SourceServerID*, LaunchStatus, LaunchedEc2InstanceID,
PostLaunchActionsStatus}, Status (JobStatus: PENDING/STARTED/COMPLETED — only 3 values, no FAILED —
see State machines), Tags, Type (JobType: LAUNCH/TERMINATE — only 2 values)}. `JobLogEvent` has 16
values (JOB_START, SERVER_SKIPPED, CLEANUP_START/END/FAIL, SNAPSHOT_START/END/FAIL,
USING_PREVIOUS_SNAPSHOT, CONVERSION_START/END/FAIL, LAUNCH_START/FAILED, JOB_CANCEL, JOB_END) —
these are the honest granular steps a simulated job progression should walk through.

### C. Launch configuration (per-server) + Launch Configuration Templates (6 ops)

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| GetLaunchConfiguration | POST /GetLaunchConfiguration | SourceServerID*, AccountID | flattened (BootMode, CopyPrivateIp, CopyTags, Ec2LaunchTemplateID, EnableMapAutoTagging, LaunchDisposition, Licensing, MapAutoTaggingMpeID, Name, PostLaunchActions, SourceServerID, TargetInstanceTypeRightSizingMethod — NO `types.LaunchConfiguration` struct exists, trap #2) | ResourceNotFound, UninitializedAccount |
| UpdateLaunchConfiguration | POST /UpdateLaunchConfiguration | SourceServerID*, AccountID, all config fields optional (partial update) | same flattened shape | Conflict, ResourceNotFound, UninitializedAccount, Validation |
| CreateLaunchConfigurationTemplate | POST /CreateLaunchConfigurationTemplate | all fields optional (no field marked required in the Go struct at all — confirmed by direct read) incl. `PostLaunchActions`, `Licensing`, `LargeVolumeConf`/`SmallVolumeConf *LaunchTemplateDiskConf`, Tags | `LaunchConfigurationTemplate` (full, 19 fields incl. `LaunchConfigurationTemplateID`, `Arn`) | AccessDenied, UninitializedAccount, Validation |
| DeleteLaunchConfigurationTemplate | POST /DeleteLaunchConfigurationTemplate | LaunchConfigurationTemplateID* | empty | Conflict, ResourceNotFound, UninitializedAccount |
| DescribeLaunchConfigurationTemplates | POST /DescribeLaunchConfigurationTemplates | LaunchConfigurationTemplateIDs[] (optional filter), MaxResults, NextToken | `Items []LaunchConfigurationTemplate`, NextToken | ResourceNotFound, UninitializedAccount, Validation |
| UpdateLaunchConfigurationTemplate | POST /UpdateLaunchConfigurationTemplate | LaunchConfigurationTemplateID*, rest optional | `LaunchConfigurationTemplate` | AccessDenied, ResourceNotFound, UninitializedAccount, Validation |

### D. Replication configuration (per-server) + Replication Configuration Templates (6 ops)

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| GetReplicationConfiguration | POST /GetReplicationConfiguration | SourceServerID*, AccountID | flattened (AssociateDefaultSecurityGroup, BandwidthThrottling, CreatePublicIP, DataPlaneRouting, DefaultLargeStagingDiskType, EbsEncryption, EbsEncryptionKeyArn, InternetProtocol, Name, ReplicatedDisks[], ReplicationServerInstanceType, ReplicationServersSecurityGroupsIDs[], SourceServerID, StagingAreaSubnetId, StagingAreaTags, StorageConfiguration, StoreSnapshotOnLocalZone, UseDedicatedReplicationServer, UseFipsEndpoint — no `types.ReplicationConfiguration` struct exists, trap #2) | ResourceNotFound, UninitializedAccount |
| UpdateReplicationConfiguration | POST /UpdateReplicationConfiguration | SourceServerID*, AccountID, all config fields optional | same flattened shape | AccessDenied, Conflict, ResourceNotFound, UninitializedAccount, Validation |
| CreateReplicationConfigurationTemplate | POST /CreateReplicationConfigurationTemplate | most fields required (AssociateDefaultSecurityGroup*, BandwidthThrottling*, CreatePublicIP*, DataPlaneRouting*, DefaultLargeStagingDiskType*, EbsEncryption*, ReplicationServerInstanceType*, ReplicationServersSecurityGroupsIDs*[], StagingAreaSubnetId*, StagingAreaTags*, UseDedicatedReplicationServer* — unlike its LaunchConfigurationTemplate sibling where nothing is required, confirmed by direct read) | `ReplicationConfigurationTemplate` (full, 19 fields incl. ID/Arn/Tags) | AccessDenied, UninitializedAccount, Validation |
| DeleteReplicationConfigurationTemplate | POST /DeleteReplicationConfigurationTemplate | ReplicationConfigurationTemplateID* | empty | Conflict, ResourceNotFound, UninitializedAccount |
| DescribeReplicationConfigurationTemplates | POST /DescribeReplicationConfigurationTemplates | ReplicationConfigurationTemplateIDs[] (optional filter), MaxResults, NextToken | `Items []ReplicationConfigurationTemplate`, NextToken | ResourceNotFound, UninitializedAccount, Validation |
| UpdateReplicationConfigurationTemplate | POST /UpdateReplicationConfigurationTemplate | ReplicationConfigurationTemplateID*, rest optional | `ReplicationConfigurationTemplate` | AccessDenied, ResourceNotFound, UninitializedAccount, Validation |

Note the required-vs-optional asymmetry between `CreateLaunchConfigurationTemplate` (nothing
required) and `CreateReplicationConfigurationTemplate` (11 fields required) — confirmed by direct
struct read on both, not an inconsistency in this table.

### E. Applications (8 ops)

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| CreateApplication | POST /CreateApplication | Name*, AccountID, Description, Tags | `Application`{ApplicationID, Arn, ApplicationAggregatedStatus, CreationDateTime, Description, IsArchived, LastModifiedDateTime, Name, Tags, WaveID} | Conflict, ServiceQuotaExceeded, UninitializedAccount |
| UpdateApplication | POST /UpdateApplication | ApplicationID*, AccountID, Description, Name | `Application` | Conflict, ResourceNotFound, UninitializedAccount |
| DeleteApplication | POST /DeleteApplication | ApplicationID*, AccountID | empty | Conflict, ResourceNotFound, UninitializedAccount |
| ListApplications | POST /ListApplications | `Filters *ListApplicationsRequestFilters`, MaxResults, NextToken, AccountID | `Items []Application`, NextToken | UninitializedAccount |
| ArchiveApplication | POST /ArchiveApplication | ApplicationID*, AccountID | `Application` (IsArchived=true) | Conflict, ResourceNotFound, ServiceQuotaExceeded, UninitializedAccount |
| UnarchiveApplication | POST /UnarchiveApplication | ApplicationID*, AccountID | `Application` (IsArchived=false) | ResourceNotFound, ServiceQuotaExceeded, UninitializedAccount |
| AssociateSourceServers | POST /AssociateSourceServers | ApplicationID*, SourceServerIDs*[], AccountID | empty | Conflict, ResourceNotFound, ServiceQuotaExceeded, UninitializedAccount |
| DisassociateSourceServers | POST /DisassociateSourceServers | ApplicationID*, SourceServerIDs*[], AccountID | empty | Conflict, ResourceNotFound, UninitializedAccount |

`ApplicationAggregatedStatus`{HealthStatus (HEALTHY/LAGGING/ERROR), LastUpdateDateTime,
ProgressStatus (NOT_STARTED/IN_PROGRESS/COMPLETED), TotalSourceServers} — a rollup over the
Application's associated SourceServers, same shape as `WaveAggregatedStatus` below.

### F. Waves (8 ops)

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| CreateWave | POST /CreateWave | Name*, AccountID, Description, Tags | `Wave`{WaveID, Arn, CreationDateTime, Description, IsArchived, LastModifiedDateTime, Name, Tags, WaveAggregatedStatus} | Conflict, ServiceQuotaExceeded, UninitializedAccount |
| UpdateWave | POST /UpdateWave | WaveID*, AccountID, Description, Name | `Wave` | Conflict, ResourceNotFound, UninitializedAccount |
| DeleteWave | POST /DeleteWave | WaveID*, AccountID | empty | Conflict, ResourceNotFound, UninitializedAccount |
| ListWaves | POST /ListWaves | `Filters *ListWavesRequestFilters`, MaxResults, NextToken, AccountID | `Items []Wave`, NextToken | UninitializedAccount |
| ArchiveWave | POST /ArchiveWave | WaveID*, AccountID | `Wave` (IsArchived=true) | Conflict, ResourceNotFound, ServiceQuotaExceeded, UninitializedAccount |
| UnarchiveWave | POST /UnarchiveWave | WaveID*, AccountID | `Wave` (IsArchived=false) | ResourceNotFound, ServiceQuotaExceeded, UninitializedAccount |
| AssociateApplications | POST /AssociateApplications | WaveID*, ApplicationIDs*[], AccountID | empty | Conflict, ResourceNotFound, ServiceQuotaExceeded, UninitializedAccount |
| DisassociateApplications | POST /DisassociateApplications | WaveID*, ApplicationIDs*[], AccountID | empty | Conflict, ResourceNotFound, UninitializedAccount |

`WaveAggregatedStatus`{HealthStatus (WAVE: HEALTHY/LAGGING/ERROR — distinct enum type from
Application's but identical values), LastUpdateDateTime, ProgressStatus (NOT_STARTED/IN_PROGRESS/
COMPLETED), `ReplicationStartedDateTime` (Wave-only — no Application equivalent), TotalApplications}.
The grouping hierarchy, confirmed structurally: **Wave contains Applications (via
Associate/DisassociateApplications), Application contains SourceServers (via
Associate/DisassociateSourceServers)** — a SourceServer's own `ApplicationID` field is the reverse
pointer, but there's no direct SourceServer<->Wave association at all; it's always mediated through
an Application.

### G. Connectors (4 ops)

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| CreateConnector | POST /CreateConnector | Name*, SsmInstanceID*, `SsmCommandConfig *ConnectorSsmCommandConfig`{CloudWatchOutputEnabled*, S3OutputEnabled*, CloudWatchLogGroupName, OutputS3BucketName}, Tags | `Connector`{ConnectorID, Arn, Name, SsmCommandConfig, SsmInstanceID, Tags} | UninitializedAccount, Validation |
| UpdateConnector | POST /UpdateConnector | ConnectorID*, rest optional | `Connector` | ResourceNotFound, UninitializedAccount, Validation |
| DeleteConnector | POST /DeleteConnector | ConnectorID* | empty | ResourceNotFound, UninitializedAccount, Validation |
| ListConnectors | POST /ListConnectors | `Filters *ListConnectorsRequestFilters`, MaxResults, NextToken | `Items []Connector`, NextToken | UninitializedAccount, Validation |

No `AccountID` field on any Connector op (confirmed) — Connectors, unlike SourceServers/
Applications/Waves, are not delegated-account-scoped in this SDK. A `Connector` represents an SSM
Managed Instance (`SsmInstanceID`) running the MGN connector software that bridges an on-prem
vCenter environment to the AWS control plane — this repo has no SSM Managed Instance concept to
validate `SsmInstanceID` against (not independently confirmed either way this pass).

### H. vCenter clients (2 ops — no create op)

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| DescribeVcenterClients | **GET** /DescribeVcenterClients | MaxResults, NextToken | `Items []VcenterClient`{VcenterClientID, Arn, DatacenterName, Hostname, LastSeenDatetime, SourceServerTags, Tags, VcenterUUID}, NextToken | ResourceNotFound, UninitializedAccount, Validation |
| DeleteVcenterClient | POST /DeleteVcenterClient | VcenterClientID* | empty | ResourceNotFound, UninitializedAccount, Validation |

No `CreateVcenterClient` op exists anywhere in this 95-op surface (confirmed against the full
alphabetical op list above) — see gaps. A `VcenterClient` record is created only by the on-prem
vCenter connector appliance registering itself with AWS; this API surface can only list and delete
what already exists.

### I. Export / Import (8 ops)

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| StartExport | POST /StartExport | S3Bucket*, S3Key*, S3BucketOwner, Tags | `ExportTask`{ExportID, Arn, CreationDateTime, EndDateTime, ProgressPercentage, S3Bucket, S3BucketOwner, S3Key, Status (ExportStatus: PENDING/STARTED/FAILED/SUCCEEDED), Summary{ApplicationsCount, ServersCount, WavesCount}, Tags} | ServiceQuotaExceeded, UninitializedAccount, Validation |
| ListExports | POST /ListExports | `Filters *ListExportsRequestFilters`, MaxResults, NextToken | `Items []ExportTask`, NextToken | UninitializedAccount |
| ListExportErrors | POST /ListExportErrors | ExportID*, MaxResults, NextToken | `Items []ExportTaskError`{ErrorData, ErrorDateTime}, NextToken | UninitializedAccount, Validation |
| StartImport | POST /StartImport | `S3BucketSource *S3BucketSource`*{S3Bucket*, S3Key*, S3BucketOwner}, ClientToken, Tags | `ImportTask`{ImportID, Arn, CreationDateTime, EndDateTime, ProgressPercentage, S3BucketSource, Status (ImportStatus: PENDING/STARTED/FAILED/SUCCEEDED), Summary{Applications{CreatedCount,ModifiedCount}, Servers{CreatedCount,ModifiedCount}, Waves{CreatedCount,ModifiedCount}}, Tags} | Conflict, ResourceNotFound, ServiceQuotaExceeded, UninitializedAccount, Validation |
| ListImports | POST /ListImports | `Filters *ListImportsRequestFilters`, MaxResults, NextToken | `Items []ImportTask`, NextToken | UninitializedAccount, Validation |
| ListImportErrors | POST /ListImportErrors | ImportID*, MaxResults, NextToken | `Items []ImportTaskError`{ErrorData, ErrorDateTime, ErrorType (VALIDATION_ERROR/PROCESSING_ERROR)}, NextToken | UninitializedAccount, Validation |
| StartImportFileEnrichment | **POST /network-migration/StartImportFileEnrichment** | `SourceS3Configuration *EnrichmentSourceS3Configuration`*, `TargetS3Configuration *EnrichmentTargetS3Configuration`*, Tags | `ImportFileEnrichment`{Status (ImportFileEnrichmentStatus: PENDING/STARTED/FAILED/SUCCEEDED/SUCCEEDED_WITH_WARNINGS — 5 values, richer than plain ImportStatus)} | AccessDenied, Conflict, ServiceQuotaExceeded, Throttling, Validation |
| ListImportFileEnrichments | **POST /network-migration/ListImportFileEnrichments** | `Filters *ListImportFileEnrichmentsFilters`, MaxResults, NextToken | `Items []ImportFileEnrichment`, NextToken | Validation only |

`StartImport`'s `ImportTaskSummary.{Applications,Servers,Waves}.CreatedCount/ModifiedCount`
confirms this op is the ONLY public-API mechanism that creates `SourceServer`/`Application`/`Wave`
records in bulk from an external file — see gaps for why this matters for testability.

### J. Post-launch custom actions — source-server-scoped and template-scoped (6 ops)

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| PutSourceServerAction | POST /PutSourceServerAction | ActionID*, ActionName*, DocumentIdentifier*, Order*int32, SourceServerID*, AccountID, Active, Category (ActionCategory), Description, DocumentVersion, ExternalParameters, MustSucceedForCutover, Parameters, TimeoutSeconds | `SourceServerActionDocument` | Conflict, ResourceNotFound, UninitializedAccount, Validation |
| ListSourceServerActions | POST /ListSourceServerActions | SourceServerID*, `Filters *SourceServerActionsRequestFilters`, MaxResults, NextToken, AccountID | `Items []SourceServerActionDocument`, NextToken | ResourceNotFound, UninitializedAccount |
| RemoveSourceServerAction | POST /RemoveSourceServerAction | ActionID*, SourceServerID*, AccountID | empty | ResourceNotFound, UninitializedAccount, Validation |
| PutTemplateAction | POST /PutTemplateAction | ActionID*, ActionName*, DocumentIdentifier*, LaunchConfigurationTemplateID*, Order*int32, Active, Category, Description, DocumentVersion, ExternalParameters, MustSucceedForCutover, OperatingSystem, Parameters, TimeoutSeconds | `TemplateActionDocument` | Conflict, ResourceNotFound, UninitializedAccount, Validation |
| ListTemplateActions | POST /ListTemplateActions | LaunchConfigurationTemplateID*, `Filters *TemplateActionsRequestFilters`, MaxResults, NextToken | `Items []TemplateActionDocument`, NextToken | ResourceNotFound, UninitializedAccount |
| RemoveTemplateAction | POST /RemoveTemplateAction | ActionID*, LaunchConfigurationTemplateID* | empty | ResourceNotFound, UninitializedAccount, Validation |

`ActionCategory` (not read in full this pass — a real enum in `types/enums.go`, worth reading in
full during implementation, not assumed here). `PutTemplateAction` additionally carries
`OperatingSystem *string` (free-form, not typed) so template actions can be gated to specific guest
OSes; `PutSourceServerAction` has no such field (it targets one already-known server, whose OS is
already fixed). Note `RemoveTemplateAction`/`ListTemplateActions` have no `AccountID` field while
`PutTemplateAction` also has none — the whole Template-action family is un-delegated, matching the
Launch/ReplicationConfigurationTemplate family's own lack of `AccountID`.

### K. Service init & managed accounts (2 ops)

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| InitializeService | POST /InitializeService | (no input fields at all) | empty | AccessDenied, Validation |
| ListManagedAccounts | POST /ListManagedAccounts | MaxResults, NextToken | `Items []ManagedAccount`{AccountId}, NextToken | UninitializedAccount, Validation |

`InitializeService` is the account-level "opt in" call every other legacy op's
`UninitializedAccountException` implicitly depends on — an honest simulation needs a per-account
"initialized" flag gating almost every other legacy op (69 of 95, per the errors section above)
until this is called once.

### L. Tagging (3 ops)

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| TagResource | POST /tags/{resourceArn} | ResourceArn* (path param), Tags* map[string]string | empty | AccessDenied, InternalServer, ResourceNotFound, Throttling, Validation |
| UntagResource | DELETE /tags/{resourceArn} | ResourceArn* (path param), TagKeys*[]string | empty | AccessDenied, InternalServer, ResourceNotFound, Throttling, Validation |
| ListTagsForResource | GET /tags/{resourceArn} | ResourceArn* (path param) | `Tags map[string]string` | AccessDenied, InternalServer, ResourceNotFound, Throttling, Validation |

Every taggable resource type in this service carries its own inline `Tags map[string]string` field
already (`Application.Tags`, `Wave.Tags`, `SourceServer.Tags`, `Job.Tags`, `Connector.Tags`,
`VcenterClient.Tags`, `LaunchConfigurationTemplate.Tags`, `ReplicationConfigurationTemplate.Tags`,
`ExportTask.Tags`, `ImportTask.Tags`, `NetworkMigrationDefinitionSummary.Tags`,
`NetworkMigrationExecution.Tags`) — this generic ARN-keyed API is the cross-cutting way to read/
mutate all of them, not a separate tag store. See Cross-service wiring below.

### M. Network Migration — definitions & mapper segments (13 ops, all under `/network-migration/`)

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| CreateNetworkMigrationDefinition | POST | Name*, `TargetNetwork *TargetNetwork`*{Topology* (ISOLATED_VPC/HUB_AND_SPOKE), InboundCidr, InspectionCidr, OutboundCidr}, `TargetS3Configuration`*, Description, ScopeTags, `SourceConfigurations []SourceConfiguration`, Tags, TargetDeployment | `NetworkMigrationDefinitionSummary`{NetworkMigrationDefinitionID, Arn, Name, ScopeTags, SourceEnvironment, Tags} | ServiceQuotaExceeded, Validation |
| GetNetworkMigrationDefinition | POST | NetworkMigrationDefinitionID* | full definition detail | AccessDenied, ResourceNotFound |
| UpdateNetworkMigrationDefinition | POST | NetworkMigrationDefinitionID*, rest optional incl. `TargetNetworkUpdate` | updated definition | AccessDenied, ResourceNotFound, Validation |
| DeleteNetworkMigrationDefinition | POST | NetworkMigrationDefinitionID* | empty | AccessDenied, Conflict, ResourceNotFound |
| ListNetworkMigrationDefinitions | POST | `Filters`, MaxResults, NextToken | `Items []NetworkMigrationDefinitionSummary`, NextToken | **AccessDenied only** (trap #8) |
| GetNetworkMigrationMapperSegmentConstruct | POST | NetworkMigrationDefinitionID*, NetworkMigrationExecutionID*, SegmentID*, ConstructID* (all required — a 4-key composite lookup) | `NetworkMigrationMapperSegmentConstruct` | AccessDenied, ResourceNotFound, Validation |
| ListNetworkMigrationMapperSegmentConstructs | POST | NetworkMigrationDefinitionID*, NetworkMigrationExecutionID*, SegmentID*, `Filters`, MaxResults, NextToken | `Items []NetworkMigrationMapperSegmentConstruct`, NextToken | AccessDenied, ResourceNotFound, Throttling, Validation |
| ListNetworkMigrationMapperSegments | POST | NetworkMigrationDefinitionID*, NetworkMigrationExecutionID*, `Filters`, MaxResults, NextToken | `Items []NetworkMigrationMapperSegment`{SegmentID, Checksum, CreatedAt, Description, JobID, LogicalID, Name, OutputS3Configuration, ReferencedSegments[], ScopeTags, SegmentType (WORKLOAD/APPLIANCE), TargetAccount, UpdatedAt}, NextToken | AccessDenied, ResourceNotFound, Throttling, Validation |
| UpdateNetworkMigrationMapperSegment | POST | NetworkMigrationDefinitionID*, NetworkMigrationExecutionID*, SegmentID*, rest optional (ScopeTags, TargetAccount) | updated segment | AccessDenied, ResourceNotFound, Validation |
| ListNetworkMigrationMappings | POST | NetworkMigrationDefinitionID*, NetworkMigrationExecutionID*, `Filters`, MaxResults, NextToken | mapping job details list | AccessDenied, ResourceNotFound, Throttling, Validation |
| ListNetworkMigrationMappingUpdates | POST | NetworkMigrationDefinitionID*, NetworkMigrationExecutionID*, `Filters`, MaxResults, NextToken | mapping-update job details list | AccessDenied, ResourceNotFound, Throttling, Validation |
| StartNetworkMigrationMapping | POST | NetworkMigrationDefinitionID*, **NetworkMigrationExecutionID*** (required — see gaps: no op creates this ID), SecurityGroupMappingStrategy | `JobID *string` only | AccessDenied, Conflict, ResourceNotFound, ServiceQuotaExceeded, Throttling, Validation |
| StartNetworkMigrationMappingUpdate | POST | NetworkMigrationDefinitionID*, NetworkMigrationExecutionID*, `Constructs []StartNetworkMigrationMappingUpdateConstruct`{ConstructID*, ConstructType*, SegmentID*, `Operation OperationUnion` (union of Delete/Merge/Split/Update operations)}, `Segments []StartNetworkMigrationMappingUpdateSegment` | job details | AccessDenied, Conflict, ResourceNotFound, ServiceQuotaExceeded, Throttling, Validation |

`OperationUnion` members: `OperationUnionMemberDelete` (empty — pure removal),
`OperationUnionMemberMerge`{MergeConstructs []MergeConstruct}, `OperationUnionMemberSplit`
{SplitConstructs []SplitConstruct}, `OperationUnionMemberUpdate`{Excluded, Name, Properties
map[string]string} — a real tagged-union edit-script for reshaping the network mapper's segment
graph, not simple field replacement.

### N. Network Migration — analysis, code generation, deployment, executions (10 ops, all under `/network-migration/`)

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| StartNetworkMigrationAnalysis | POST | NetworkMigrationDefinitionID*, NetworkMigrationExecutionID* | `JobID` | AccessDenied, Conflict, ResourceNotFound, ServiceQuotaExceeded, Throttling, Validation |
| ListNetworkMigrationAnalyses | POST | NetworkMigrationDefinitionID*, NetworkMigrationExecutionID*, `Filters`, MaxResults, NextToken | `NetworkMigrationAnalysisJobDetails`{JobID, CreatedAt, EndedAt, NetworkMigrationDefinitionID, NetworkMigrationExecutionID, Status (NetworkMigrationJobStatus: PENDING/STARTED/SUCCEEDED/FAILED), StatusDetails} list | AccessDenied, ResourceNotFound, Throttling, Validation |
| ListNetworkMigrationAnalysisResults | POST | same scoping keys, `Filters`, MaxResults, NextToken | `NetworkMigrationAnalysisResult`{AnalysisResult *string (free-text finding), AnalyzerType, JobID, Source{SubnetID,VpcID}, Status (NetworkMigrationAnalysisResultStatus: PENDING/STARTED/SUCCEEDED/FAILED), Target} list | AccessDenied, ResourceNotFound, Throttling, Validation |
| StartNetworkMigrationCodeGeneration | POST | NetworkMigrationDefinitionID*, NetworkMigrationExecutionID* | `JobID` | AccessDenied, Conflict, ResourceNotFound, ServiceQuotaExceeded, Throttling, Validation |
| ListNetworkMigrationCodeGenerations | POST | scoping keys, `Filters`, MaxResults, NextToken | `NetworkMigrationCodeGenerationJobDetails` list | AccessDenied, ResourceNotFound, Throttling, Validation |
| ListNetworkMigrationCodeGenerationSegments | POST | scoping keys + SegmentID?, `Filters`, MaxResults, NextToken | `NetworkMigrationCodeGenerationSegment`{`Artifact *NetworkMigrationCodeGenerationArtifact`{ArtifactType (NetworkMigrationCodeGenerationArtifactType), SubType, Status (CodeGenerationOutputFormatStatusDetails)}} list | AccessDenied, ResourceNotFound, Throttling, Validation |
| StartNetworkMigrationDeployment | POST | NetworkMigrationDefinitionID*, NetworkMigrationExecutionID* | `JobID` | AccessDenied, Conflict, ResourceNotFound, ServiceQuotaExceeded, Throttling, Validation |
| ListNetworkMigrationDeployments | POST | scoping keys, `Filters`, MaxResults, NextToken | `NetworkMigrationDeployerJobDetails` list | AccessDenied, ResourceNotFound, Throttling, Validation |
| ListNetworkMigrationDeployedStacks | POST | scoping keys, `Filters`, MaxResults, NextToken | `NetworkMigrationDeployedStackDetails`{Status (NetworkMigrationDeployedStackStatus: CREATE_COMPLETE/CREATE_FAILED/CREATE_STARTED/DELETE_COMPLETE/DELETE_FAILED/DELETE_STARTED)} list — doc comment: "a CloudFormation stack that has been deployed as part of the network migration" | AccessDenied, ResourceNotFound, Throttling, Validation |
| ListNetworkMigrationExecutions | POST | **NetworkMigrationDefinitionID* only** — no create-side op exists (see gaps) | `Items []NetworkMigrationExecution`{Activity (ExecutionStageActivity: MAPPING/MAPPING_UPDATE/CODE_GENERATION/DEPLOY/DEPLOYED_STACKS_DELETION/ANALYZE), CreatedAt, NetworkMigrationDefinitionID, NetworkMigrationExecutionID, Stage (ExecutionStage: same 5 values minus MAPPING_UPDATE), Status (ExecutionStatus: PENDING/STARTED/SUCCEEDED/FAILED), Tags, UpdatedAt}, NextToken | AccessDenied, ResourceNotFound |

`ExecutionStage` (5 values: MAPPING/CODE_GENERATION/DEPLOY/DEPLOYED_STACKS_DELETION/ANALYZE) is the
top-level workflow phase an execution is in; `ExecutionStageActivity` (6 values, adds
MAPPING_UPDATE) is the finer-grained activity within that stage. Both are genuinely orthogonal to
the per-job `NetworkMigrationJobStatus`/`ExecutionStatus` (PENDING/STARTED/SUCCEEDED/FAILED) —
an execution's `Stage`/`Activity` says WHERE it is in the pipeline, `Status` says whether the
CURRENT step succeeded or failed.

## 2. Missing simulated functionality (the real emulation work)

MGN is fundamentally a **replication/cutover orchestration** service (the legacy 70 ops) with a
**network-topology analysis and code-generation/deployment** feature bolted on (the 25
`/network-migration/` ops). The two halves need almost entirely different honesty treatments.

### SourceServer lifecycle — `LifeCycleState` (10 values, confirmed in `types/enums.go`)

`STOPPED`, `NOT_READY`, `READY_FOR_TEST`, `TESTING`, `READY_FOR_CUTOVER`, `CUTTING_OVER`,
`CUTOVER`, `DISCONNECTED`, `DISCOVERED`, `PENDING_INSTALLATION`. `ChangeServerLifeCycleState`'s own
input enum (`ChangeServerLifeCycleStateSourceServerLifecycleState`) only exposes 3 of these as
CALLER-settable targets: `READY_FOR_TEST`, `READY_FOR_CUTOVER`, `CUTOVER` — every other value
(`NOT_READY`, `TESTING`, `CUTTING_OVER`, `DISCONNECTED`, `DISCOVERED`, `PENDING_INSTALLATION`,
`STOPPED`) is system-driven, reached only as a side effect of some other op or the passage of
(simulated) time, never directly settable. A defensible legal-transition table, inferred from field
names and doc comments (NOT independently confirmed against AWS's real state machine, which is not
published in the SDK — flag this inference explicitly if implemented):

- `PENDING_INSTALLATION` → `DISCOVERED`/`NOT_READY` once the (simulated) replication agent
  reports in and `DataReplicationInfo` begins populating.
- `NOT_READY` → `READY_FOR_TEST` once `DataReplicationState` reaches `CONTINUOUS` (a real
  replicated disk with no active backlog).
- `READY_FOR_TEST` → `TESTING` (via `StartTest`) → `READY_FOR_CUTOVER` or back to
  `READY_FOR_TEST` (test failed/reverted, per `LifeCycleLastTest.Reverted`).
- `READY_FOR_CUTOVER` → `CUTTING_OVER` (via `StartCutover`) → `CUTOVER` (via `FinalizeCutover`) or
  back to `READY_FOR_CUTOVER` (`LifeCycleLastCutover.Reverted`).
- `CUTOVER` → `DISCONNECTED` (via `DisconnectFromService`) is the terminal, expected end state
  once the target instance is fully cut over and the source is no longer needed.
- `MarkAsArchived` sets `SourceServer.IsArchived` — a SEPARATE boolean orthogonal to
  `LifeCycleState` (an archived server can be in any lifecycle state; this is a visibility/cleanup
  flag, not a lifecycle transition itself), confirmed by `IsArchived *bool` living directly on
  `SourceServer`, not inside `LifeCycle`.

`LifeCycle` also carries `LastTest`/`LastCutover` sub-records (`Initiated`{ApiCallDateTime, JobID},
`Reverted`{ApiCallDateTime}, `Finalized`{ApiCallDateTime}) — an honest simulation should populate
`JobID` on `Initiated` pointing back at the `StartTest`/`StartCutover` Job that triggered it, giving
a real audit trail rather than independent, disconnected timestamps.

### `DataReplicationInfo` sub-state-machine

`DataReplicationState` (12 values): `STOPPED`, `INITIATING`, `INITIAL_SYNC`, `BACKLOG`,
`CREATING_SNAPSHOT`, `CONTINUOUS`, `PAUSED`, `RESCAN`, `STALLED`, `DISCONNECTED`,
`PENDING_SNAPSHOT_SHIPPING`, `SHIPPING_SNAPSHOT` — the last two only apply when
`ReplicationType == SNAPSHOT_SHIPPING` rather than `AGENT_BASED` (confirmed by field semantics, not
an explicit SDK-encoded constraint). `DataReplicationInitiation.Steps
[]DataReplicationInitiationStep` walks a fixed 12-step sequence (`WAIT` →
`CREATE_SECURITY_GROUP` → `LAUNCH_REPLICATION_SERVER` → `BOOT_REPLICATION_SERVER` →
`AUTHENTICATE_WITH_SERVICE` → `DOWNLOAD_REPLICATION_SOFTWARE` → `CREATE_STAGING_DISKS` →
`ATTACH_STAGING_DISKS` → `PAIR_REPLICATION_SERVER_WITH_AGENT` →
`CONNECT_AGENT_TO_REPLICATION_SERVER` → `START_DATA_TRANSFER` → `SETUP_FSX_PROXY`), each with its
own `DataReplicationInitiationStepStatus` (`NOT_STARTED`/`IN_PROGRESS`/`SUCCEEDED`/`FAILED`/
`SKIPPED`). `DataReplicationInfoReplicatedDisk`{BackloggedStorageBytes, ReplicatedStorageBytes,
RescannedStorageBytes, TotalStorageBytes — all `int64`} is the real per-disk progress ledger.

**There is no real source machine and no real replication agent** — this entire sub-state-machine
is inherently bookkeeping-only. A defensible simulated progression is a deterministic, time-based
walk through the 12 initiation steps followed by a monotonically increasing
`ReplicatedStorageBytes` toward `TotalStorageBytes` (both caller-supplied or defaulted at
`ReplicationConfiguration` creation time), landing on `CONTINUOUS` once fully caught up — NOT a
fabricated "realistic-looking" bandwidth/lag simulation with invented physical throughput numbers,
which would be indistinguishable from real telemetry to a caller and is exactly the kind of
fabrication parity-principles.md warns against. `DataReplicationErrorString` (18 values, e.g.
`AGENT_NOT_SEEN`, `UNSTABLE_NETWORK`, `FAILED_TO_LAUNCH_REPLICATION_SERVER`) should remain unused
in a first pass (no real failure condition exists to trigger them) rather than being randomly
injected to look more "realistic."

### Test → cutover → finalize flow legality

`StartTest`/`StartCutover` both take a BATCH `SourceServerIDs []string` and return one shared
`Job` with `ParticipatingServers []ParticipatingServer` — a single Job can test/cut over multiple
servers together (e.g. all servers in a Wave), each tracked independently via its own
`ParticipatingServer.LaunchStatus` (`PENDING`/`IN_PROGRESS`/`LAUNCHED`/`FAILED`/`TERMINATED`) while
the parent `Job.Status` (`PENDING`/`STARTED`/`COMPLETED` — only 3 values, **no `FAILED`** at the
Job level) presumably reaches `COMPLETED` once every participating server's `LaunchStatus` is
terminal (LAUNCHED or FAILED), never itself becoming a stuck/failed state — an implementer must
decide and document this rollup rule (not encoded in the SDK) rather than presenting it as
AWS-confirmed. Legal precondition: `StartTest` requires `LifeCycleState == READY_FOR_TEST`,
`StartCutover` requires `READY_FOR_CUTOVER`, `FinalizeCutover` (single-server only, unlike its
batch siblings) requires the server currently be in `CUTOVER`-eligible state after a completed
cutover Job — these preconditions are this audit's inference from field/enum semantics, not
independently SDK-confirmed, and should be flagged as a deliberate implementation choice if built
this way.

### Jobs and job logs — an honest async progression

`JobType` has only 2 values: `LAUNCH` (produced by `StartTest`/`StartCutover`) and `TERMINATE`
(produced by `TerminateTargetInstances`). A defensible simulated Job progression: `PENDING` →
`STARTED` (immediately or after a short deterministic delay) → walk `JobLogEvent`s in order
(`JOB_START` → per-participating-server `SNAPSHOT_START`/`SNAPSHOT_END` →
`CONVERSION_START`/`CONVERSION_END` → `LAUNCH_START` → `JOB_END`) while updating each
`ParticipatingServer.LaunchStatus` in step → `Job.Status = COMPLETED`. `DescribeJobLogItems`
should return these as they're "produced" over the deterministic timeline, not all at once —
though returning them all at once with correct final timestamps is a defensible simpler first pass
if explicitly documented as such (per-tick streaming logs are a real-but-optional fidelity
increment, not a correctness requirement).

### Four distinct configuration-family resources — do not conflate

Confirmed as four genuinely separate resource kinds with only namespace-adjacent naming:
1. **Launch configuration** (per-`SourceServer`, via `GetLaunchConfiguration`/
   `UpdateLaunchConfiguration`) — no dedicated Create/Delete op; it presumably comes into
   existence automatically alongside its SourceServer (with defaults, possibly copied from a
   `LaunchConfigurationTemplate` if one is associated — not confirmed how association happens,
   since no `AssociateLaunchConfigurationTemplate`-style op exists in this 95-op surface either).
2. **Launch Configuration Template** (account-level, reusable, via
   Create/Delete/Describe/UpdateLaunchConfigurationTemplate) — has its own ID/ARN/Tags.
3. **Replication configuration** (per-`SourceServer`, via `GetReplicationConfiguration`/
   `UpdateReplicationConfiguration`) — same no-dedicated-create-op pattern as #1.
4. **Replication Configuration Template** (account-level, reusable, via
   Create/Delete/Describe/UpdateReplicationConfigurationTemplate) — has its own ID/ARN/Tags.

**How template → per-server configuration application actually happens is not exposed by any op in
this SDK** — this is a genuine, unresolved gap (similar in kind to the NetworkMigrationExecutionID
gap): an implementer must pick a defensible convention (e.g. new SourceServers inherit the most
recently created enabled template's settings as their initial per-server configuration) and
document it explicitly as invented, not derived.

### Waves and Applications grouping

Confirmed two-level hierarchy: `Wave` ⊃ `Application` (via Associate/DisassociateApplications) ⊃
`SourceServer` (via Associate/DisassociateSourceServers) — see family F/E tables above. Both
`WaveAggregatedStatus`/`ApplicationAggregatedStatus` are rollups (`HealthStatus`,
`ProgressStatus`, counts) that must be recomputed whenever a member SourceServer's `LifeCycle`/
`DataReplicationInfo` changes — this is real, non-trivial rollup logic (a Wave/Application with
one `LAGGING` source server is presumably `LAGGING` overall; the exact aggregation rule is not
SDK-specified and must be invented and documented).

### Connectors, vCenter clients, export/import

**Connectors** and **vCenter clients** are both inherently agent/appliance-registration resources
— see gaps for the concrete consequence (no create op for either). **Export**
(`StartExport`/`ListExports`/`ListExportErrors`) writes a JSON/CSV dump of Applications/Waves/
Servers metadata to a caller-supplied S3 bucket; **Import** (`StartImport`/`ListImports`/
`ListImportErrors`) reads one back in, creating/modifying records per
`ImportTaskSummary.{Applications,Servers,Waves}.{CreatedCount,ModifiedCount}`. Neither this SDK nor
this repo has any S3-file-format schema for what that JSON/CSV actually contains — a real
implementation would need to invent (and clearly flag as invented) a schema, or treat
`StartExport`/`StartImport` as producing/consuming an opaque blob whose row counts are tracked but
whose content is never actually round-tripped meaningfully. **`StartImportFileEnrichment`**
augments an import file with additional discovered network/segment metadata for the Network
Migration feature — genuinely coupled to that sub-product, not the core import flow, despite the
similar naming.

### Post-launch actions

`PostLaunchActions`{Deployment (TEST_AND_CUTOVER/CUTOVER_ONLY/TEST_ONLY), SsmDocuments
[]SsmDocument, S3LogBucket, CloudWatchLogGroupName} describes SSM documents to run against the
newly-launched EC2 instance after a test/cutover. `PostLaunchActionsStatus` /
`JobPostLaunchActionsLaunchStatus` / `PostLaunchActionExecutionStatus` (IN_PROGRESS/SUCCESS/
FAILED) track per-document execution results, keyed to a `ParticipatingServer` within a Job. This
repo has no SSM document execution engine (not independently confirmed this pass, but no SSM
backend was found under `services/`) — an honest simulation can track the STATE (documents listed,
statuses set to a deterministic SUCCESS after a delay) without ever actually running anything,
clearly flagged as bookkeeping-only, matching this campaign's MACsec/BGP-peering precedent from the
directconnect audit.

### Network Migration sub-product — largely bookkeeping-only, explicitly flagged

The 25 `/network-migration/` ops (families M and N above) model: (1) defining a target AWS network
topology and importing on-prem network config exports, (2) analyzing them
(`StartNetworkMigrationAnalysis` → free-text `AnalysisResult` findings), (3) generating
infrastructure code from the analysis (`StartNetworkMigrationCodeGeneration` →
`NetworkMigrationCodeGenerationArtifact`), and (4) deploying that code as real CloudFormation
stacks (`StartNetworkMigrationDeployment` → `NetworkMigrationDeployedStackDetails`, whose own doc
comment says "a CloudFormation stack that has been deployed"). **Steps 2-4 cannot be honestly
performed without a real network-analysis engine and a real code generator, neither of which exists
in this repo.** The state-bookkeeping shell — definitions, executions (once seeded, see gaps),
mapper segments/constructs, and status enums walking their documented values on a deterministic
timer — is honestly buildable. The CONTENT of `AnalysisResult`, generated code artifacts, and
deployed-stack details must remain clearly-flagged placeholders (e.g. an empty or template string),
never fabricated realistic-looking network analysis or generated Terraform/CloudFormation, which
would misrepresent what the emulator actually did.

## 3. Cross-service wiring needed

**Tagging.** `TagResource`/`UntagResource`/`ListTagsForResource` exist (confirmed:
`api_op_TagResource.go`, `api_op_UntagResource.go`, `api_op_ListTagsForResource.go`, family L
above), so this service should be wired into `cli.go`'s `wireResourceGroupsTagging`
(`/home/agbishop/gopherstack/cli.go:5348`), following the `wireTaggingGrafana`/`wireTaggingEFS`
pattern already used for the other 30 wired services (`cli.go:5327-5399`'s own doc comment
enumerates them: dynamodb, sqs, sns, lambda, kms, secretsmanager, ecs, athena, glue, ecr, kinesis,
stepfunctions, cloudfront, eks, batch, wafv2, backup, efs, docdb, neptune, rds, elasticache,
redshift, sagemaker, firehose, opensearch, cloudwatchlogs, mq, emr, grafana). MGN would be the
31st entry, `wireTaggingMGN(bk, byName["MGN"])` (or whatever name string this service registers
itself under — not confirmed here since the service doesn't exist yet to register anything).
Unlike Outposts (one generic tag store shared by 2 resource kinds), MGN has **12 distinct taggable
resource kinds** (Application, Wave, SourceServer, Job, Connector, VcenterClient,
LaunchConfigurationTemplate, ReplicationConfigurationTemplate, ExportTask, ImportTask,
NetworkMigrationDefinitionSummary, NetworkMigrationExecution) all sharing the one
`/tags/{resourceArn}` API — the tag store backing this wiring needs to be ARN-keyed across all 12,
not scoped to a single resource-type map.

**ARN namespace.** Could NOT be confirmed against Terraform provider source this pass — see the
gaps entry above for the full explanation (Terraform's `internal/service/mgn/` package has zero
resource files, only generated client boilerplate). The best available corroborating evidence is
botocore's `service-2.json` metadata, where `endpointPrefix`/`serviceId`/`signingName` are all
literally `"mgn"` (fetched via `raw.githubusercontent.com/boto/botocore/develop/botocore/data/mgn/
2020-02-26/service-2.json`) — consistent with, but not proof of, the ARN service segment also
being `"mgn"` (this repo's own arn.Build helper, `pkgs/arn/arn.go:34`, takes a bare `service`
string parameter with no MGN-specific handling needed since it's a regional, non-global service
like the vast majority already special-cased only for `"iam"`). The exact resource-path segment
for each of the 12 taggable kinds (e.g. `source-server/<id>`, `application/<id>`) is an HONEST
UNKNOWN, not fabricated here — an implementer should verify before hardcoding, following the same
caution the outposts/directconnect audits flagged for their own under-confirmed resource segments.

**EC2/EBS/IAM/KMS/VPC integration for real cutover.** This repo has real, working backends for
every piece MGN's cutover would touch:
- EC2 instance launch: `services/ec2/handler_instances_lifecycle.go:119` (`handleRunInstances`).
- EBS snapshots: `services/ec2/handler_snapshots.go`.
- IAM roles: `services/iam/handler_roles.go` (`func.*CreateRole`).
- KMS: `services/kms/` (full package exists — `EbsEncryptionKeyArn`/`ParametersEncryptionKey` on
  `ReplicationConfiguration`/`LaunchConfigurationTemplate` could resolve against it).
- VPC subnets/security groups: `services/ec2/store.go` (Subnet/SecurityGroup types exist in the
  same EC2 backend RunInstances already reads).

A real implementation **could** launch actual gopherstack EC2 instances from
`ReplicationConfiguration.ReplicationServerInstanceType`/`StagingAreaSubnetId`/
`ReplicationServersSecurityGroupsIDs` (for the replication server, conceptually — though a
replication server is itself an AWS-internal implementation detail never exposed via any field on
`SourceServer`/`Job`, so simulating it may not even be observable to a caller and could be pure
internal bookkeeping) and from `LaunchConfiguration`'s settings on Job completion (for the actual
`LaunchedInstance.Ec2InstanceID`), producing a real, listable EC2 instance rather than an invented
ID string. This is a substantial, real cross-service integration — validating subnet/security-group
IDs against `services/ec2`'s store, actually calling into EC2's RunInstances path, and populating
`LaunchedInstance.Ec2InstanceID` with a real instance ID this repo's own `DescribeInstances` would
then return. Flagged here as a concrete, buildable follow-on, explicitly NOT required for a
first-pass, honestly-gapped implementation (a first pass can validate the ID format and store it
without cross-checking existence, clearly documented as such).

**Grep results for "mgn"/"migration".** `grep -rni "\bmgn\b" services/ cli.go` (word-boundary,
excluding false positives like "management") returns **zero hits**. `grep -rli "migration"
services/ cli.go` returns hits only in `services/dms/*` (AWS Database Migration Service — an
entirely different, already-implemented AWS product), `services/opensearch/*migrations*` (index
migration, unrelated), `services/elasticache/handler_replication_groups.go` (cache replication,
unrelated), and `services/ec2/handler_instance_attrs.go`/`services/waf/handler_web_acls.go` (both
false-positive substring matches on unrelated concepts, confirmed by reading context) — none
reference AWS Application Migration Service. `grep -rli "SourceServer\|StartCutover\|
ReplicationConfigurationTemplate" services/ cli.go` returns only unrelated false positives
(cognitoidp's `ResourceServer`, elasticache's `Serverless`) — confirmed zero real MGN-adjacent
state anywhere in this tree.

**CloudFormation.** `grep -rli "mgn\b" services/cloudformation/` returns zero hits across all 71
`resources_*.go` files in that directory — no `AWS::MGN::*` resource type exists in this repo. This
audit did not independently verify whether AWS's own real CloudFormation supports any MGN resource
type at all (MGN's agent-driven, non-declarative nature makes broad CFN support unlikely, matching
the directconnect/outposts pattern, but that claim is about AWS's product, not this repo's tree).

## Top 5 hardest/riskiest things about implementing this service

1. **No public op creates a `SourceServer`, a `VcenterClient`, or a `NetworkMigrationExecutionID`.**
   Every one of `StartTest`/`StartCutover`/`ChangeServerLifeCycleState`/the entire replication
   family operates on a `SourceServerID` that must already exist, but the only public creation path
   is `StartImport`'s bulk metadata load (itself schema-unspecified) — real AWS creates them via an
   internal agent-registration call this SDK does not expose at all. Testing this service at all
   requires inventing a seeding mechanism and being explicit that it is a gopherstack-only
   convenience, not a simulation of AWS's real onboarding flow. The same problem applies,
   independently, to `VcenterClient` (no create op) and to `NetworkMigrationExecutionID` (required
   input to 5 different Start* ops, created by none of them).
2. **Two structurally distinct wire "generations" coexist in one service** (69 legacy ops with
   `UninitializedAccountException`/no `ThrottlingException`, versus the tagging trio + 25
   `/network-migration/` ops with the reverse) — an implementer building one shared error-mapping
   table risks silently blending them if not built from the actual per-op extraction in this
   document.
3. **The flattened-vs-nested output-shape split** (11 SourceServer-mutation ops flatten the full
   SourceServer onto their Output; `StartTest`/`StartCutover`/`TerminateTargetInstances` instead
   nest a `Job`; `GetLaunchConfiguration`/`GetReplicationConfiguration` flatten their own distinct
   shapes with no backing named type at all) is exactly the kind of "looks like it should share one
   serializer, doesn't" trap parity-principles.md calls out — a router or generic-response helper
   written by extrapolating from a handful of sampled ops will get several of these wrong.
4. **The Network Migration sub-product (25 of 95 ops) requires either a real network-analysis/
   code-generation engine or an explicit, prominent decision to keep its analysis/codegen/
   deployment CONTENT as placeholder text while still honestly progressing its status enums** —
   there is no middle ground that isn't either unbuildable or fabrication, and the temptation to
   generate "plausible-looking" analysis findings or generated code to make the emulator feel more
   complete is exactly the failure mode this campaign's honesty rules exist to prevent.
5. **The ARN resource-path format for all 12 taggable resource kinds is unconfirmed** — Terraform's
   AWS provider has literally zero MGN resources to check against (unlike directconnect/outposts,
   where at least partial Terraform-source corroboration existed), and AWS's own docs pages
   returned only a JS shell to automated fetching. Only the ARN *service segment* (`"mgn"`) has
   indirect corroboration (botocore's endpoint metadata); every specific resource-path segment is
   this audit's best-effort guess from convention, not a confirmed value, and should be verified
   independently before an implementer hardcodes it.
