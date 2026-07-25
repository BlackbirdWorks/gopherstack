---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: emr
sdk_module: aws-sdk-go-v2/service/emr@v1.57.7   # version audited against
last_audit_commit: 68b00b12                     # HEAD when this manifest was written
last_audit_date: 2026-07-24
overall: A                # re-audit after the Phase-3.3 datalayer refactor (store.Table split); found and fixed a
                           # wrong wire field name, an invented request field, three missing RunJobFlow-accepted
                           # inputs, a nil-vs-zero-value Get*Policy bug, and a tags-scope gap (see Notes)
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  RunJobFlow: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-07-24: deleted invented Instances.IamInstanceProfile field (no such member on real JobFlowInstancesConfig), added real top-level JobFlowRole field (-> Ec2InstanceAttributes.IamInstanceProfile); added inline KerberosAttributes/PlacementGroupConfigs/ManagedScalingPolicy/AutoTerminationPolicy support (previously only settable after creation via separate ops); added Instances.InstanceFleets support (previously RunJobFlow could only build instance-group clusters, fleets only attachable post-creation via AddInstanceFleet); prior pass fixed Timeline millis->epoch-seconds"}
  DescribeCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-07-24: added Cluster.KerberosAttributes/PlacementGroups/InstanceCollectionType/AutoTerminate (previously silently dropped/missing); remaining omitted optional Cluster fields (AutoScalingRole aside, e.g. MonitoringConfiguration/LogEncryptionKmsKeyId/OutpostArn/RepoUpgradeOnBoot/RequestedAmiVersion/RunningAmiVersion/MasterPublicDnsName/ExtendedSupport/NormalizedInstanceHours) are acceptable simplifications -- all optional pointer fields a real client sees as nil, not fabricated data"}
  ListClusters: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed missing Status.Timeline in summaries (also fixed the sort, which read that same field); fixed CreatedAfter/CreatedBefore millis->epoch-seconds parsing"}
  TerminateJobFlows: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed EndDateTime millis->epoch-seconds"}
  ModifyCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeJobFlows: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed CreatedAfter/CreatedBefore millis->epoch-seconds parsing"}
  AddJobFlowSteps: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed StepTimeline millis->epoch-seconds"}
  ListSteps: {wire: ok, errors: ok, state: ok, persist: ok, note: "steps now auto-complete on read (were stuck PENDING forever)"}
  DescribeStep: {wire: ok, errors: ok, state: ok, persist: ok, note: "same auto-complete-on-read fix"}
  CancelSteps: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed enum: SUBMITTED/FAILED (was fabricated SUCCESS/QUEUED); added Reason"}
  AddInstanceGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  ListInstanceGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyInstanceGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  AddInstanceFleet: {wire: ok, errors: ok, state: ok, persist: ok}
  ListInstanceFleets: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyInstanceFleet: {wire: ok, errors: ok, state: ok, persist: ok, note: "was a disguised no-op -- looked up the fleet and returned nil without applying TargetOnDemandCapacity/TargetSpotCapacity; now mutates"}
  PutAutoScalingPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  RemoveAutoScalingPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  PutManagedScalingPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  GetManagedScalingPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-07-24: fixed nil-vs-zero-value bug -- ManagedScalingPolicy field is now a pointer with omitempty, so it is omitted from the wire (matching real GetManagedScalingPolicyOutput.ManagedScalingPolicy *T) when no policy is attached, instead of a zero-valued object that would deserialize as a non-nil struct on a real client"}
  RemoveManagedScalingPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  PutAutoTerminationPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAutoTerminationPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-07-24: same nil-vs-zero-value fix as GetManagedScalingPolicy"}
  RemoveAutoTerminationPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateSecurityConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed CreationDateTime ISO8601-string->epoch-seconds"}
  DescribeSecurityConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "same fix"}
  DeleteSecurityConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  ListSecurityConfigurations: {wire: ok, errors: ok, state: ok, persist: ok, note: "SecurityConfigSummary.CreationDateTime was a raw time.Time (RFC3339 on wire); now epoch seconds"}
  GetBlockPublicAccessConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed metadata CreationDateTime ISO8601-string->epoch-seconds"}
  PutBlockPublicAccessConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  GetClusterSessionCredentials: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed ExpiresAt ISO8601-string->epoch-seconds"}
  CreateStudio: {wire: ok, errors: ok, state: ok, persist: ok, note: "CreationTime was a raw time.Time (RFC3339 on wire); now epoch seconds"}
  DescribeStudio: {wire: ok, errors: ok, state: ok, persist: ok}
  ListStudios: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateStudio: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteStudio: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateStudioSessionMapping: {wire: ok, errors: ok, state: ok, persist: ok, note: "CreationTime/LastModifiedTime same fix"}
  GetStudioSessionMapping: {wire: ok, errors: ok, state: ok, persist: ok}
  ListStudioSessionMappings: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateStudioSessionMapping: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteStudioSessionMapping: {wire: ok, errors: ok, state: ok, persist: ok}
  StartNotebookExecution: {wire: ok, errors: ok, state: ok, persist: ok, note: "StartTime/EndTime were raw time.Time (RFC3339 on wire); now epoch seconds"}
  StopNotebookExecution: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeNotebookExecution: {wire: ok, errors: ok, state: ok, persist: ok}
  ListNotebookExecutions: {wire: ok, errors: ok, state: ok, persist: ok, note: "reuses NotebookExecution (extra fields vs real NotebookExecutionSummary are harmless -- clients ignore unknown fields); deferred: not trimmed to the exact summary shape"}
  CreatePersistentAppUI: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribePersistentAppUI: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPersistentAppUIPresignedURL: {wire: ok, errors: ok, state: ok, persist: n/a, note: "2026-07-24: added PresignedURLReady (always true; gopherstack provisions synchronously)"}
  GetOnClusterAppUIPresignedURL: {wire: ok, errors: ok, state: ok, persist: n/a, note: "2026-07-24 SEVERE FIX: response field was named \"URL\" -- GetOnClusterAppUIPresignedURLOutput has no such member, only \"PresignedURL\"/\"PresignedURLReady\"; a real client's output.PresignedURL always deserialized as nil since unknown JSON fields are silently dropped. Renamed and added PresignedURLReady."}
  AddTags: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-07-24: extended to also match Studio resources by ID/ARN, not only clusters -- real AddTagsInput.ResourceId doc explicitly covers \"a cluster identifier or an Amazon EMR Studio ID\"; tagging a studio previously 400'd as resource-not-found"}
  RemoveTags: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-07-24: same Studio-resource fix as AddTags"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-07-24: same Studio-resource fix as AddTags"}
  ListBootstrapActions: {wire: ok, errors: ok, state: ok, persist: ok}
  ListInstances: {wire: partial, errors: ok, state: ok, persist: n/a, note: "synthesized instances are a simplification (no EbsVolumes/PublicIpAddress/etc); acceptable, not re-flagged"}
  ListReleaseLabels: {wire: ok, errors: ok, state: ok, persist: n/a}
  DescribeReleaseLabel: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListSupportedInstanceTypes: {wire: ok, errors: ok, state: ok, persist: n/a}
  SetTerminationProtection: {wire: ok, errors: ok, state: ok, persist: ok}
  SetKeepJobFlowAliveWhenNoSteps: {wire: ok, errors: ok, state: ok, persist: ok}
  SetVisibleToAllUsers: {wire: ok, errors: ok, state: ok, persist: ok}
  SetUnhealthyNodeReplacement: {wire: ok, errors: ok, state: ok, persist: ok}
# Families audited as a group (when per-op is impractical):
families:
  error-mapping: {status: ok, note: "EMR's real error model has exactly two exception types (InvalidRequestException 400, InternalServerException 500) per aws-sdk-go-v2/service/emr/types/errors.go; the deserializeError switch matches __type against these two strings verbatim. Fixed handleError, which returned the non-existent 'ValidationException' for ErrInvalidParameter and 'InternalFailure' for the default/500 case -- neither would deserialize into a typed exception a real client checks with errors.As."
leaks: {status: clean, note: "2026-07-24 re-check after Phase-3.3 datalayer refactor (region-nested maps -> store.Table/store.Index) and this pass's fixes: DeleteStudio still cascades studioSessionMappingDelete for every mapping of the deleted studio (clone-before-delete pattern preserved through the refactor, avoiding an in-place-index-mutation-during-range hazard); janitor sweeps TERMINATED clusters via c.TerminatedAt and clears the arnIndex entry inline; no new goroutines/tickers added this pass. The new taggedResourceTags helper (tags.go), added for Studio tagging support, does a linear scan of studiosInRegion under the lock AddTags/RemoveTags/ListTagsForResource already hold -- no new lock acquisition. effectiveStepStatus remains a pure read-time computation, no persisted mutation, no lock escalation."}
---

## Notes

**Timestamp wire format (root cause of most fixes this pass).** EMR is
awsjson1.1: every `Timestamp` shape (`CreationDateTime`, `ReadyDateTime`,
`EndDateTime`, `StartDateTime`, `ExpiresAt`, `CreationTime`,
`LastModifiedTime`, ...) serializes as `smithytime.FormatEpochSeconds` --
a JSON *number* of seconds since the epoch, fractional part = sub-second
precision -- and deserializes with `smithytime.ParseEpochSeconds`, which
rejects RFC3339 strings outright ("expected Timestamp to be a JSON Number,
got string instead"). Before this pass:
- `Cluster.Status.Timeline` / `StepTimeline` fields were populated with
  `time.Now().UnixMilli()` (milliseconds, and inconsistently typed
  int64 in some spots vs float64 in others) instead of epoch *seconds*.
  A real client parsing these would compute a date millions of years in
  the future.
- `ListClusters`' `CreatedAfter`/`CreatedBefore` request fields (and
  `DescribeJobFlows`' legacy equivalents) were parsed with
  `time.UnixMilli(int64(wireValue))`, treating a wire value that is
  actually epoch *seconds* as milliseconds -- a filter with `CreatedAfter`
  set to "one hour ago" was silently comparing against a timestamp near
  the 1970 epoch, making the date filter effectively a no-op in the
  direction that matters.
- `SecurityConfiguration.CreationDateTime`,
  `blockPublicAccessMeta.CreationDateTime`, and
  `GetClusterSessionCredentials`' `ExpiresAt` were formatted as ISO8601
  strings (`"2006-01-02T15:04:05Z"`) at the handler layer -- a real SDK
  client's deserializer would fail outright on these fields (not just
  produce a wrong value).
- `Studio.CreationTime`, `StudioSummary.CreationTime`,
  `StudioSessionMapping.CreationTime`/`LastModifiedTime`, and
  `NotebookExecution.StartTime`/`EndTime` were plain `time.Time` fields
  with a normal `json` tag, marshaled by `encoding/json`'s default
  RFC3339 string encoding -- same client-breaking failure mode.

Fix: every one of these fields is now `float64` populated via
`pkgs/awstime.Epoch(t)` (or, for request parsing, a small
`epochSecondsToTime` inverse helper in handler.go). Domain structs keep
plain `float64` fields rather than a wrapper type so persistence.go's
existing `json.Marshal(Cluster)`-based snapshot format needed no changes
(it round-trips a float64 field exactly like it round-tripped the old
`time.Time` one -- only the *wire* HTTP response shape was ever wrong).

**`ListClusters` sort was silently broken.** `gatherClusterSummaries`
built each `ClusterSummary.Status` without copying `Timeline` across from
the source `Cluster`, so every summary's `Timeline` map was `nil`.
`ListClusters`' "most recently created first" sort reads
`Status.Timeline["CreationDateTime"]` for its comparator, so with the
field always missing every cluster compared as "equal" and the sort
silently fell through to the ID tie-breaker -- which happens to produce
the same order as true creation-time descending for this backend's
monotonically-increasing IDs, so the bug never showed up in tests that
only checked ID ordering. Real AWS's `ClusterSummary` shape includes
`Status.Timeline`; fixed by copying it through.

**Steps stayed PENDING forever.** Nothing ever transitioned a step out of
PENDING except `CancelSteps` (PENDING -> CANCELLED); there was no
COMPLETED path at all. A real client's `StepCompleteWaiter` (min poll
interval 30s) would poll forever. Added `effectiveStepStatus`, a pure
read-time computation applied in `ListSteps`/`DescribeStep`/`CancelSteps`:
a step still PENDING after `stepCompletionDelay` (3s) since creation is
reported as COMPLETED. This mirrors the existing pattern of clusters
being created directly in WAITING (no simulated
STARTING/BOOTSTRAPPING/RUNNING) -- gopherstack has no real workload to
run, so it simulates near-instant completion rather than a hang. Stored
state is untouched (no extra lock, no persistence changes); only the
returned copy is promoted.

**`CancelSteps` returned enum values that don't exist.** The real
`CancelStepsRequestStatus` enum has exactly two values, `SUBMITTED` and
`FAILED` (`aws-sdk-go-v2/service/emr/types/enums.go`). This backend
returned `"SUCCESS"`/`"QUEUED"`, neither of which the enum defines --
harmless to a client that only checks the field is non-empty (which is
all the pre-existing tests did), but any real code branching on the enum
value would never match. Fixed, and added the `Reason` field the real
shape carries alongside `Status`.

**`ModifyInstanceFleet` was a disguised no-op.** It looked up the fleet by
ID and returned `nil` on match without ever applying
`TargetOnDemandCapacity`/`TargetSpotCapacity` from the request -- contrast
with `ModifyInstanceGroups`, which does apply its modification via
`applyInstanceGroupMod`. Fixed with a matching `applyInstanceFleetMod`;
since gopherstack provisions instantly, `Provisioned*Capacity` is set
alongside `Target*Capacity`. A zero value in either field means "not
specified" (the JSON field is a plain `int`, not a pointer, so unset and
explicit-zero are indistinguishable on the wire either way) -- guarded
with `> 0` so specifying only one capacity doesn't zero the other.

**Error mapping.** EMR's real error model has exactly two exception types:
`InvalidRequestException` (client fault, HTTP 400) and
`InternalServerException` (server fault, HTTP 500) --
`aws-sdk-go-v2/service/emr/types/errors.go`. The generated
`deserializeError` dispatch matches the wire `__type` against these two
strings verbatim (case-insensitively) and falls back to an untyped
`smithy.GenericAPIError` for anything else. `handleError` was returning
`"ValidationException"` for parameter-validation failures (e.g.
`StepConcurrencyLevel` out of range, `IdleTimeout` out of range) and
`"InternalFailure"` for the default/500 case -- neither string is a type
EMR defines, so a real client's `errors.As(&types.InvalidRequestException{})`
/ `errors.As(&types.InternalServerException{})` checks would silently
fail to match. Fixed both to use the two real type names.

**Traps for the next auditor (looks-wrong-but-correct):**
- Clusters are created directly in `WAITING` state (no simulated
  `STARTING`/`BOOTSTRAPPING`/`RUNNING`). This is intentional, not a
  stub: a real client's `ClusterRunning`/`ClusterTerminated` waiter
  never has anything to poll for, so it can't hang. `ReadyDateTime` is
  set equal to `CreationDateTime` for the same reason.
- `Cluster.TerminatedAt` (`json:"TerminatedAt,omitzero"`) is NOT part of
  AWS's real `Cluster` shape -- it's internal bookkeeping the janitor
  uses for its TTL sweep, deliberately left with its own `json` tag so
  it persists across Snapshot/Restore. It leaks onto the wire as an
  extra field a real client would silently ignore; left alone rather
  than risk breaking the janitor's post-restore sweep by changing its
  persisted shape.
- `NotebookExecutionSummary` (real `ListNotebookExecutions` item shape)
  has fewer fields than `NotebookExecution` (no `NotebookParams`, no
  `Tags`) -- gopherstack reuses the full `NotebookExecution` for both
  Describe and List. Extra fields on the wire are harmless (a real SDK
  client ignores unknown JSON fields); not fixed this pass, noted as
  `deferred` above if a future pass wants exact shape parity.

**Not re-audited this pass (unchanged since a prior implicit baseline,
low traffic, or judged out of scope for a first pass):** `Configuration`
recursive classification handling, `SupportedInstanceType` static
catalog contents, `ReleaseLabel`/application-bundle tables, exact
`AutoScalingPolicyDetail.Status` state machine (`ATTACHED` is always
returned; real AWS also has `PENDING`/`ATTACHING`/`DETACHING`/`FAILED`
but this is a reasonable simplification, not a hang risk since it's not
polled by a waiter).

## 2026-07-24 re-audit (post Phase-3.3 datalayer refactor)

Between the 2026-07-12 audit (`d7ff080e`) and this one, `services/emr/`
went through two large mechanical refactors (`Go refactoring 2` #2392,
`Parity 4` #2384) that split the monolithic `backend.go`/`handler.go`/
`handler_*.go` files into today's per-resource-family files and replaced
every region-nested `map[string]map[string]*T` with a `*store.Table[T]` +
`*store.Index[T]` pair (see `store_setup.go`). Since every file touched by
that refactor changed, this pass re-audited the full surface rather than
trusting the prior manifest's per-op rows, per the manifest's own
re-audit protocol. The refactor itself was behavior-preserving; the bugs
below predate it and were simply carried through unnoticed.

**`GetOnClusterAppUIPresignedURLOutput` wire field name was wrong
(severe).** The response sent `{"URL": "..."}`. Real
`GetOnClusterAppUIPresignedURLOutput` has no `URL` member -- only
`PresignedURL` and `PresignedURLReady`. A real SDK client's deserializer
silently ignores unknown JSON fields, so `output.PresignedURL` would
always come back `nil` no matter what gopherstack sent. Fixed the field
name and added `PresignedURLReady` (always `true`, matching
`GetPersistentAppUIPresignedURL`, which already had the right field name
but was also missing `PresignedURLReady`).

**`RunJobFlowInstances.IamInstanceProfile` was an invented field.** Real
`JobFlowInstancesConfig` (the wire type for `RunJobFlow`'s `Instances`
block) has no `IamInstanceProfile` member -- that attribute is set via
the top-level `RunJobFlowInput.JobFlowRole` field instead, and echoed
back as `Cluster.Ec2InstanceAttributes.IamInstanceProfile`. gopherstack
had the field nested one level too deep (inert on the wire, since no
real client ever populates it there) and was entirely missing the real
top-level `JobFlowRole` field, so `IamInstanceProfile` could never
actually be set by a real `RunJobFlow` call. Deleted the invented field,
added the real one.

**`RunJobFlow` couldn't build a fleet-based cluster.** Real
`JobFlowInstancesConfig` accepts either `InstanceGroups` or
`InstanceFleets` (mutually exclusive) to describe a cluster's initial
capacity. gopherstack's `RunJobFlowInstances` only had `InstanceGroups`;
an instance fleet could only be attached to an already-running cluster
via `AddInstanceFleet`, so a fleet-based `RunJobFlow` request would
silently create an empty, group-less, fleet-less cluster. Added
`Instances.InstanceFleets` support (reusing `AddInstanceFleet`'s
construction logic via a new `buildInstanceFleets` helper) and a derived
`Cluster.InstanceCollectionType` (`INSTANCE_GROUP`/`INSTANCE_FLEET`) --
a real field this backend never populated at all.

**`RunJobFlow` silently dropped `KerberosAttributes` and
`PlacementGroupConfigs`.** Both are real `RunJobFlowInput` fields echoed
back on `Cluster` (`KerberosAttributes`, `PlacementGroups`); neither
existed anywhere in gopherstack's request or domain types. Added both,
plus the derived `Cluster.AutoTerminate` field (the real API's inverse of
`Instances.KeepJobFlowAliveWhenNoSteps`) -- three more real `Cluster`
members this backend never populated.

**`RunJobFlow` couldn't accept `ManagedScalingPolicy`/
`AutoTerminationPolicy` inline.** Real `RunJobFlowInput` accepts both at
creation time; gopherstack only ever let a caller attach them afterward
via `PutManagedScalingPolicy`/`PutAutoTerminationPolicy`. Added both,
reusing the same validation (`validateManagedScalingPolicy`,
`validateAutoTerminationPolicy`, the latter extracted from
`PutAutoTerminationPolicy` so both call paths share one bounds check).

**`GetManagedScalingPolicy`/`GetAutoTerminationPolicy` returned a
zero-valued policy instead of omitting the field.** Real
`GetManagedScalingPolicyOutput.ManagedScalingPolicy` (and the
`AutoTerminationPolicy` equivalent) is a pointer that AWS omits from the
wire entirely when no policy is attached -- a real client's
`output.ManagedScalingPolicy` is `nil` in that case. gopherstack always
returned `{"ManagedScalingPolicy":{"ComputeLimits":{...zeroed...}}}` /
`{"AutoTerminationPolicy":{"IdleTimeout":0}}`, which deserializes as a
*non-nil* struct with zeroed fields on a real client -- a
nil-vs-populated distinction any code branching on "is a policy
attached?" would get wrong. Changed both backend `Get*` methods to
return `nil` (not a zero-valued struct) when unset, and both handler
output structs to pointer fields with `omitempty`.

**`AddTags`/`RemoveTags`/`ListTagsForResource` only worked on clusters.**
Real `AddTagsInput.ResourceId`'s doc explicitly says "a cluster
identifier or an Amazon EMR Studio ID" -- Studios are a real taggable
resource. gopherstack's tag ops only ever looked up clusters
(`findClusterByIDOrARN`), so tagging a Studio incorrectly 400'd as
resource-not-found. Added `findStudioByIDOrARN` (studios.go) and a
`taggedResourceTags` dispatcher (tags.go) that tries cluster-then-studio
lookup; all three ops now work against either resource type.

All of the above were verified against
`aws-sdk-go-v2/service/emr@v1.57.7` via `go doc` on the installed module
(same version already in `go.mod`; no SDK version bump needed).
