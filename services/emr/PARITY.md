---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: emr
sdk_module: aws-sdk-go-v2/service/emr@v1.64.4   # bumped from v1.64.0 pin; no new ops, field-diffed Cluster/MonitoringConfiguration/ListInstancesInput this pass
last_audit_commit: 8c56f4eb9                    # HEAD when the 2026-08-07 pass (gopherstack-dqd8) below was written
last_audit_date: 2026-08-07
overall: A                # 2026-08-07 (gopherstack-dqd8): threaded MonitoringConfiguration/LogEncryptionKmsKeyId/
                           # RepoUpgradeOnBoot/RequestedAmiVersion/RunningAmiVersion through RunJobFlow->Cluster
                           # (previously silently dropped, not merely omitted-as-nil -- the emulator was handed
                           # real request data and threw it away); fixed ListInstances to synthesize instance-fleet
                           # instances at all (previously always empty for INSTANCE_FLEET clusters) and wired
                           # InstanceFleetId/InstanceFleetType/InstanceStates filters. OutpostArn/MasterPublicDnsName/
                           # ExtendedSupport/NormalizedInstanceHours reclassified from an undifferentiated "acceptable
                           # simplification" list into structural_gaps with individual justification (see below) --
                           # this is a reclassification of existing nil-omission, not new work; grade held at A on
                           # that basis, not raised for it.
                           # 2026-07-31: reverse-SDK-check triage (pkgs/sdkcheck, commit 12cfe14d5) found and fixed
                           # five real defects the prior "A" (2026-07-25, see note below) missed: a phantom op
                           # (ListTagsForResource -- no such EMR operation exists) wrongly documented "ok" below;
                           # a fabricated ClusterSummary.ReleaseLabel field; a severe StartNotebookExecution wire
                           # bug (wrong JSON tag silently dropped the real ExecutionEngine field, so
                           # NotebookExecution.ExecutionEngineId was ALWAYS empty); and two Studio field-forwarding
                           # bugs (CreateStudio dropped Description, UpdateStudio hardcoded SubnetIds to empty).
                           # All five confirmed independently against aws-sdk-go-v2/service/emr@v1.64.0 (go doc +
                           # types/*.go reads) before fixing, and all five now have regression tests that fail
                           # against the pre-fix code (see per-op notes). Grade held at A because every finding
                           # is now genuinely fixed and re-verified, not because nothing was wrong -- the prior
                           # "A" rating was itself inaccurate on these five points; see the 2026-07-31 section
                           # below for the honest accounting of what that means for trusting older "ok" rows.
                           # 2026-07-25: implemented the interactive-session family (StartSession/GetSession/
                           # ListSessions/TerminateSession/GetSessionEndpoint), 5 ops the SDK bump revealed as
                           # unimplemented. Real emulation throughout: cluster-state gating, cluster-termination
                           # cascade, and a deliberately conservative session-state model (see Notes). No
                           # fabricated fields found this pass -- see per-op notes below for what was field-diffed.
                           # (This "no fabricated fields" claim is the one the 2026-07-31 pass falsified.)
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  RunJobFlow: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-07-24: deleted invented Instances.IamInstanceProfile field (no such member on real JobFlowInstancesConfig), added real top-level JobFlowRole field (-> Ec2InstanceAttributes.IamInstanceProfile); added inline KerberosAttributes/PlacementGroupConfigs/ManagedScalingPolicy/AutoTerminationPolicy support (previously only settable after creation via separate ops); added Instances.InstanceFleets support (previously RunJobFlow could only build instance-group clusters, fleets only attachable post-creation via AddInstanceFleet); prior pass fixed Timeline millis->epoch-seconds"}
  DescribeCluster: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-07-24: added Cluster.KerberosAttributes/PlacementGroups/InstanceCollectionType/AutoTerminate. 2026-08-07: threaded four more real, buildable fields through RunJobFlow -> Cluster that were previously silently dropped even though the emulator held (or was directly given) the data needed to populate them: MonitoringConfiguration (RunJobFlowInput.MonitoringConfiguration, echoed back verbatim -- new CloudWatchLogConfiguration/S3LoggingConfiguration types field-diffed against types.MonitoringConfiguration), LogEncryptionKmsKeyId (RunJobFlowInput.LogEncryptionKmsKeyId, direct passthrough), RepoUpgradeOnBoot (RunJobFlowInput.RepoUpgradeOnBoot, direct passthrough), and RequestedAmiVersion/RunningAmiVersion (both derived from the legacy top-level RunJobFlowInput.AmiVersion field -- gopherstack does no AMI resolution so RunningAmiVersion mirrors what was requested, which is honest for an emulator that performs no real provisioning). OutpostArn/MasterPublicDnsName/ExtendedSupport/NormalizedInstanceHours remain omitted: see structural_gaps."}
  ListClusters: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed missing Status.Timeline in summaries (also fixed the sort, which read that same field); fixed CreatedAfter/CreatedBefore millis->epoch-seconds parsing; 2026-07-31: deleted fabricated ClusterSummary.ReleaseLabel field -- real ClusterSummary has no such member (only Id, Name, Status, ClusterArn, NormalizedInstanceHours, OutpostArn); the field predates this pass (introduced in the Jul-18 refactor, missed by the 2026-07-25 audit's 'no fabricated fields found' claim) and was caught by the new pkgs/sdkcheck-style field diff done for this pass, not by the reverse op-name check"}
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
  CreateStudio: {wire: ok, errors: ok, state: ok, persist: ok, note: "CreationTime was a raw time.Time (RFC3339 on wire); now epoch seconds. 2026-07-31: createStudioInput never declared the real CreateStudioInput.Description field at all, so a real client's Description was silently dropped (unknown-JSON-field-ignored, same failure mode as the StartNotebookExecution bug this pass) even though UpdateStudio -- the sibling op -- already applied the same field correctly, an unintentional asymmetry. Added the field, threaded through Backend.CreateStudio's new description parameter."}
  DescribeStudio: {wire: ok, errors: ok, state: ok, persist: ok}
  ListStudios: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateStudio: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-07-31: handleUpdateStudio hardcoded the backend's subnetIDs argument to \"\" instead of forwarding in.SubnetIDs -- real UpdateStudioInput.SubnetIds was accepted on the wire and silently discarded, never applied to the stored Studio. No test previously exercised UpdateStudio's SubnetIds at all (in fact no test called the UpdateStudio op at all). Backend.UpdateStudio's subnetIDsJSON string parameter (already unused -- `_ = subnetIDsJSON`) was changed to a real []string and now does a full replace when non-empty, matching real UpdateStudioInput.SubnetIds' documented full-replace-not-merge semantics."}
  DeleteStudio: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateStudioSessionMapping: {wire: ok, errors: ok, state: ok, persist: ok, note: "CreationTime/LastModifiedTime same fix"}
  GetStudioSessionMapping: {wire: ok, errors: ok, state: ok, persist: ok}
  ListStudioSessionMappings: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateStudioSessionMapping: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteStudioSessionMapping: {wire: ok, errors: ok, state: ok, persist: ok}
  StartNotebookExecution: {wire: ok, errors: ok, state: ok, persist: ok, note: "StartTime/EndTime were raw time.Time (RFC3339 on wire); now epoch seconds. 2026-07-31 SEVERE FIX: the input's cluster reference was declared with JSON tag \"ExecutionEngineConfig\" (the real *type* name, types.ExecutionEngineConfig) instead of the real top-level *field* name \"ExecutionEngine\" -- a real client's ExecutionEngine was silently dropped by json.Unmarshal (unknown fields are ignored, not errored), so NotebookExecution.ExecutionEngineId was ALWAYS empty regardless of what cluster the caller named. Six existing tests sent the wrong \"ExecutionEngineConfig\" key and none asserted ExecutionEngineId was actually populated, so the bug passed silently; all six corrected to the real \"ExecutionEngine\" key and a new wire-shape test now asserts ExecutionEngineId round-trips."}
  StopNotebookExecution: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeNotebookExecution: {wire: ok, errors: ok, state: ok, persist: ok}
  ListNotebookExecutions: {wire: ok, errors: ok, state: ok, persist: ok, note: "reuses NotebookExecution (extra fields vs real NotebookExecutionSummary are harmless -- clients ignore unknown fields); deferred: not trimmed to the exact summary shape"}
  CreatePersistentAppUI: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribePersistentAppUI: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPersistentAppUIPresignedURL: {wire: ok, errors: ok, state: ok, persist: n/a, note: "2026-07-24: added PresignedURLReady (always true; gopherstack provisions synchronously)"}
  GetOnClusterAppUIPresignedURL: {wire: ok, errors: ok, state: ok, persist: n/a, note: "2026-07-24 SEVERE FIX: response field was named \"URL\" -- GetOnClusterAppUIPresignedURLOutput has no such member, only \"PresignedURL\"/\"PresignedURLReady\"; a real client's output.PresignedURL always deserialized as nil since unknown JSON fields are silently dropped. Renamed and added PresignedURLReady."}
  AddTags: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-07-24: extended to also match Studio resources by ID/ARN, not only clusters -- real AddTagsInput.ResourceId doc explicitly covers \"a cluster identifier or an Amazon EMR Studio ID\"; tagging a studio previously 400'd as resource-not-found"}
  RemoveTags: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-07-24: same Studio-resource fix as AddTags"}
  # ListTagsForResource is intentionally NOT listed as an op here. 2026-07-31
  # CORRECTION: the row that used to live at this position ("ListTagsForResource:
  # {wire: ok, ...}") documented a phantom operation -- real EMR has no such
  # SDK method at all (verified against aws-sdk-go-v2/service/emr@v1.64.0: only
  # AddTags/RemoveTags exist under the tags family; tags are read back via
  # DescribeCluster.Tags/DescribeStudio.Tags). GetSupportedOperations() and
  # ChaosOperations() no longer advertise it. The handler route and backend
  # method of the same name are kept, unadvertised, as internal test/tooling
  # scaffolding (this package's own tests use it to assert stored-tag state
  # without hand-decoding Describe* output) -- same resolution as CloudFront's
  # GetFunctionAssociations/SetFunctionAssociations (see that service's
  # handler.go). Caught by pkgs/sdkcheck's new reverse check (commit
  # 12cfe14d5), which flags supportedOps entries with no real SDK counterpart;
  # confirmed via `go doc` against the installed v1.64.0 module and by grepping
  # the SDK source tree for an api_op_ListTagsForResource.go that does not
  # exist for this client.
  ListBootstrapActions: {wire: ok, errors: ok, state: ok, persist: ok}
  ListInstances: {wire: ok, errors: ok, state: ok, persist: n/a, note: "2026-08-07: two real bugs fixed. (1) buildInstanceList only ever iterated cluster.instanceGroups -- a cluster built with Instances.InstanceFleets (InstanceCollectionType=INSTANCE_FLEET) had zero instances synthesized ever, regardless of ProvisionedOnDemandCapacity/ProvisionedSpotCapacity; ListInstances now also synthesizes fleet instances from that real per-fleet state, split ON_DEMAND/SPOT by the provisioned counts. (2) InstanceFleetId was accepted on the wire and silently ignored (real filter, real backend state, just never wired); also added the previously-entirely-missing InstanceFleetType filter (real ListInstancesInput member) and wired the previously-accepted-but-ignored InstanceStates filter. Remaining simplification: fleet-synthesized instances leave InstanceType blank (omitempty) -- see gaps, not structural_gaps, since it is buildable, just deferred (see note there). EbsVolumes/PublicIpAddress/etc. on group instances unchanged from prior pass (optional, correctly nil)."}
  ListReleaseLabels: {wire: ok, errors: ok, state: ok, persist: n/a}
  DescribeReleaseLabel: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListSupportedInstanceTypes: {wire: ok, errors: ok, state: ok, persist: n/a}
  SetTerminationProtection: {wire: ok, errors: ok, state: ok, persist: ok}
  SetKeepJobFlowAliveWhenNoSteps: {wire: ok, errors: ok, state: ok, persist: ok}
  SetVisibleToAllUsers: {wire: ok, errors: ok, state: ok, persist: ok}
  SetUnhealthyNodeReplacement: {wire: ok, errors: ok, state: ok, persist: ok}
  StartSession: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-07-25 NEW (aws-sdk-go-v2/service/emr@v1.64.0, X-Amz-Target ElasticMapReduce.StartSession): validates ClusterId required + cluster exists + cluster.Status.State in {WAITING, RUNNING} (real doc's own requirement) before creating; a TERMINATED cluster is rejected with InvalidRequestException. Session created in SUBMITTED (types.SessionState) -- see families.session-state-model below for why nothing auto-advances further."}
  GetSession: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-07-25 NEW (X-Amz-Target ElasticMapReduce.GetSession): scoped by both ClusterId and SessionId per real GetSessionInput's two required members; field-diffed Session against types.Session including the awsjson1.1 epoch-seconds timestamps (CreatedAt/UpdatedAt/EndedAt/IdleSince/StartedAt)."}
  ListSessions: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-07-25 NEW (X-Amz-Target ElasticMapReduce.ListSessions): scoped to one cluster (real ListSessionsInput.ClusterId is required, there is no cross-cluster session list); sorted newest-first per real doc; SessionStates filter implemented. MaxResults accepted but not used to size the page, consistent with every other list op in this package (none honor a client page-size hint)."}
  TerminateSession: {wire: ok, errors: ok, state: ok, persist: ok, note: "2026-07-25 NEW (X-Amz-Target ElasticMapReduce.TerminateSession): resolves directly to TERMINATED (skips the real API's intermediate TERMINATING step), matching this backend's own cluster-termination model (terminateSingle: WAITING straight to TERMINATED, no TERMINATING); idempotent on an already-terminated/failed session."}
  GetSessionEndpoint: {wire: ok, errors: ok, state: ok, persist: n/a, note: "2026-07-25 NEW (X-Amz-Target ElasticMapReduce.GetSessionEndpoint): validates cluster and session both exist; Endpoint/AuthToken/AuthTokenExpirationTime/Credentials all populated -- Credentials reuses GetClusterSessionCredentials' existing {\"UsernamePassword\":{...}} wire shape for the same real types.Credentials union (its only member)."}
# Families audited as a group (when per-op is impractical):
families:
  error-mapping: {status: ok, note: "EMR's real error model has exactly two exception types (InvalidRequestException 400, InternalServerException 500) per aws-sdk-go-v2/service/emr/types/errors.go; the deserializeError switch matches __type against these two strings verbatim. Fixed handleError, which returned the non-existent 'ValidationException' for ErrInvalidParameter and 'InternalFailure' for the default/500 case -- neither would deserialize into a typed exception a real client checks with errors.As."}
gaps:
  - "ListInstances synthesized fleet instances leave InstanceType blank: InstanceFleet (unlike InstanceGroup) only tracks aggregate TargetOnDemandCapacity/TargetSpotCapacity/Provisioned* counts, not a per-instance-type breakdown -- AddInstanceFleet's real wire input accepts InstanceTypeConfigs (a weighted list of candidate instance types) but gopherstack's InstanceFleetSpec never captured it at all, a pre-existing gap larger than this pass's ListInstances-synthesis scope. This IS buildable (thread InstanceTypeConfigs through AddInstanceFleet/RunJobFlow's inline fleet spec, pick a type per synthesized instance) but was left out of this pass to stay in scope; leaving InstanceType blank rather than inventing a plausible-looking type avoids fabricating data the backend doesn't have. (bd: gopherstack-dqd8)"
structural_gaps:
  - "Cluster.OutpostArn stays omitted (nil): the real value is derived from which Outpost the launch subnet belongs to, a subnet-to-Outpost topology fact that lives in EC2/Outposts, not in anything RunJobFlowInput passes to EMR directly. Gopherstack's EMR has no cross-service lookup into services/outposts' subnet/Outpost state, and most clusters are not Outpost-launched anyway (nil is the correct value for the common case); wiring a real cross-service resolution is a distinct, larger feature than an EMR-local field fix. (bd: gopherstack-dqd8)"
  - "Cluster.MasterPublicDnsName stays omitted (nil): a real DNS name comes from the EC2 instance actually launched for the master node. Gopherstack's EMR never creates a corresponding EC2 instance (ListInstances synthesizes lightweight ClusterInstance records, not real ec2.Instance resources with their own IP/DNS allocation), so there is no real DNS name to report; inventing one would be exactly the fabrication this campaign removes elsewhere. (bd: gopherstack-dqd8)"
  - "Cluster.ExtendedSupport stays omitted (nil/false): real AWS derives this from a release-label EOL/extended-support enrollment table that AWS updates over time (not encoded anywhere in the SDK types or wire shapes -- the SDK doc literally marks the field 'Reserved'). There is no verifiable source of truth to compute it from, only AWS's changing operational policy data, which does not belong hardcoded into an emulator. (bd: gopherstack-dqd8)"
  - "Cluster.NormalizedInstanceHours stays omitted (nil): real AWS accrues this from actual wall-clock instance runtime multiplied by an m1.small-relative weight per instance type, tracked continuously by the real service as instances run. Gopherstack simulates clusters as provisioned instantly (see instanceGroupStateRunning) with no per-instance runtime clock and no published m1.small-relative weight table in the SDK, so there is no real accrual to report -- approximating it from CreationDateTime alone without AWS's actual weight table would be a fabricated number dressed as a real one. (bd: gopherstack-dqd8)"
leaks: {status: clean, note: "2026-07-24 re-check after Phase-3.3 datalayer refactor (region-nested maps -> store.Table/store.Index) and this pass's fixes: DeleteStudio still cascades studioSessionMappingDelete for every mapping of the deleted studio (clone-before-delete pattern preserved through the refactor, avoiding an in-place-index-mutation-during-range hazard); janitor sweeps TERMINATED clusters via c.TerminatedAt and clears the arnIndex entry inline; no new goroutines/tickers added this pass. The new taggedResourceTags helper (tags.go), added for Studio tagging support, does a linear scan of studiosInRegion under the lock AddTags/RemoveTags/ListTagsForResource already hold -- no new lock acquisition. effectiveStepStatus remains a pure read-time computation, no persisted mutation, no lock escalation."}
session-state-model: {status: ok, note: "2026-07-25: sessions are embedded directly on Cluster (sessions []Session, sessions.go), the same modeling choice already used for steps/instanceGroups/instanceFleets -- real EMR has no cross-cluster ListSessions, only ListSessions(ClusterId), so a child collection keyed by the owning cluster is the correct shape, not a fabrication. State model deliberately does NOT simulate SUBMITTED -> STARTING -> STARTED -> IDLE: unlike effectiveStepStatus's PENDING -> COMPLETED promotion (steps trivially 'succeed' since gopherstack runs no real Hadoop job), reaching IDLE/STARTED for a session would require simulating a real Spark Connect driver booting, which this emulator has no model for at all -- fabricating that progression was judged worse than leaving it SUBMITTED. TerminateSession resolves synchronously (straight to TERMINATED, no TERMINATING window), consistent with terminateSingle's own cluster-termination model."}
session-termination-cascade: {status: ok, note: "2026-07-25: terminateSingle (clusters.go) now calls terminateClusterSessions, which transitions every non-terminal session on a cluster to TERMINATED in the same call that marks the cluster TERMINATED -- a Spark Connect session cannot outlive its cluster. Because sessions are embedded on Cluster rather than a separate store.Table, the janitor's existing TTL sweep (janitor.go, unchanged) removes them for free when it deletes the cluster row; no separate session sweep was needed to avoid orphans, unlike some other cascade-delete bugs this campaign has found elsewhere."}
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

## 2026-07-25: interactive-session family (SDK bump to v1.64.0)

The Go SDK module was bumped from `v1.57.7` to `v1.64.0`, which added five
new operations this backend had never seen: `StartSession`, `GetSession`,
`ListSessions`, `TerminateSession`, `GetSessionEndpoint` -- EMR's
interactive (Spark Connect) session family, one cohesive addition rather
than five unrelated ops. `TestSDKCompleteness` failed on all five until this
pass; all five are now implemented for real (not stubbed/deferred) and
added to `GetSupportedOperations()`.

**Wire shapes** were field-diffed against `aws-sdk-go-v2/service/emr@v1.64.0`
directly (`go doc`, plus reading `serializers.go`/`deserializers.go` for the
awsjson1.1 field-name-equals-Go-struct-name convention this SDK uses
throughout, and for the epoch-seconds `Timestamp` handling every other op in
this file already documents). All five `X-Amz-Target` values are the literal
operation name with the `ElasticMapReduce.` prefix (confirmed in
`serializers.go`, e.g. `httpBindingEncoder.SetHeader("X-Amz-Target").String("ElasticMapReduce.StartSession")`)
-- no surprises there, unlike some other services in this campaign.

**Cluster wiring.** `StartSession` validates: (1) `ClusterId` is present,
(2) the referenced cluster exists, (3) the cluster's `Status.State` is
`WAITING` or `RUNNING` -- real `StartSession`'s own doc: "The cluster must
be in the RUNNING or WAITING state". This backend's clusters are always
created directly in `WAITING` (`buildNewCluster`, unchanged) and never
reach `RUNNING` (no simulated `STARTING`/`BOOTSTRAPPING`/`RUNNING`), so in
practice only `WAITING` passes; `RUNNING` is still checked for correctness
against the real API and in case a future change (or a hand-seeded test
cluster) produces it. A `TERMINATED`/`TERMINATED_WITH_ERRORS` cluster is
rejected with `InvalidRequestException`, satisfying the audit's explicit
ask ("a TERMINATED cluster should not accept a new session"). Sessions are
reachable by `GetSession`/`ListSessions`(both `ClusterId`-scoped, matching
the real API -- there is no cross-cluster session list) and removable by
`TerminateSession`; no session is ever created that isn't observable
through these paths (there is exactly one construction site, `StartSession`
in `sessions.go`, and it always appends to `cluster.sessions` before
returning).

**Modeling choice: sessions embedded on `Cluster`, not a separate
`store.Table`.** Real EMR has no `ListSessions`-across-clusters operation
(`ListSessionsInput.ClusterId` is required), so a session is a genuine
child resource of exactly one cluster -- the same shape steps,
instanceGroups, and instanceFleets already have in this file, all modeled
as unexported slice fields on `Cluster` rather than their own
`store.Table`. This choice was not just for consistency: it also gives
cascade-delete for free (see below) without a second sweep loop.

**Termination cascade (explicit ask from the audit).** `terminateSingle`
(clusters.go, the function `TerminateJobFlows` calls per cluster) now also
calls `terminateClusterSessions` (sessions.go), which transitions every
non-terminal session on that cluster straight to `TERMINATED` in the same
call that marks the cluster `TERMINATED` -- a Spark Connect session cannot
continue running once its underlying cluster is gone. Because sessions live
on the `Cluster` struct rather than a standalone table, the *existing*
janitor TTL sweep (`janitor.go`, not modified) removes every session on a
cluster for free the moment it deletes that cluster's row -- there is no
window where a session could reference a cluster ID the janitor has already
swept, and no second cleanup loop was needed to prevent it. This was a
deliberate check against the "ghost rows surviving a parent delete" bug
class this campaign has found elsewhere (e.g. the studio/session-mapping
cascade already documented above) -- verified by
`TestEMR_TerminateJobFlows_CascadesToSessions` (handler_clusters_test.go).

**Session state model (explicit ask from the audit).** Real `SessionState`
(`aws-sdk-go-v2/service/emr/types/enums.go`) has eight values: `SUBMITTED`,
`STARTING`, `STARTED`, `IDLE`, `BUSY`, `TERMINATING`, `TERMINATED`,
`FAILED`. This backend drives exactly two transitions:
- `StartSession` creates a session in `SUBMITTED` (real `StartSession`'s
  own doc: "When a session is first created, it enters the SUBMITTED
  state" -- not a simplification, the literal required initial value).
- `TerminateSession` (and the termination cascade above) transitions
  directly to `TERMINATED`, skipping the real API's documented
  intermediate `TERMINATING` step. This mirrors `terminateSingle`'s own
  cluster-termination model, which likewise skips a `TERMINATING`
  intermediate and goes straight `WAITING` -> `TERMINATED` -- keeping the
  two termination paths in this file consistent with each other, and
  guaranteeing neither `GetSession` nor `ListSessions` can ever return a
  `TERMINATING` session a real client's poll loop could get stuck on.

**Explicitly refused to auto-advance:** `SUBMITTED` -> `STARTING` ->
`STARTED` -> `IDLE` (the sequence a real Spark Connect session goes through
before it can accept queries) is never simulated, so `IDLE`/`STARTED`/
`BUSY`/`STARTING` are unreachable in this backend, and `FAILED` is never
produced (no simulated failure injection). This is a deliberate difference
from `effectiveStepStatus`'s PENDING -> COMPLETED step promotion: a step
"completing near-instantly" is a reasonable stand-in for a Hadoop job
gopherstack never actually runs (the job trivially "succeeds" because
nothing runs it), but there is no equivalent trivial-success story for a
session reaching `IDLE` -- that would mean fabricating "a real Spark driver
finished booting," which is exactly the kind of invented progression this
parity campaign flags rather than rewards. A client polling `GetSession`
waiting for `IDLE` will see `SUBMITTED` forever (or `TERMINATED`, once
explicitly or cascade-terminated) -- an honest gap, not a hidden one.

**`GetSessionEndpoint` URL/credential construction.** Built consistent with
the two existing synthesized-endpoint patterns already in this file rather
than inventing a new format: the endpoint URL follows
`GetPresignedURL`'s (persistent_app_ui.go) `https://<id>.<region>.<fake-service>.amazonaws.com`
shape (here, `<sessionID>.<region>.spark-connect-emr.amazonaws.com`), and
`Credentials` reuses `GetClusterSessionCredentials`' existing
`{"UsernamePassword":{"Username":...,"Password":...}}` wire shape --
both wrap the same real `types.Credentials` union, whose only member is
`UsernamePassword` (confirmed via `deserializers.go`'s
`awsAwsjson11_deserializeDocumentCredentials`). `AuthTokenExpirationTime`
reuses the existing `sessionCredentialExpiry` (12h) constant
`GetClusterSessionCredentials` already used for the same kind of synthetic
expiry.

**Ops not implemented:** none. All five new operations are implemented for
real, not deferred or stubbed.

Field-diffed against `aws-sdk-go-v2/service/emr@v1.64.0` via `go doc` on
the installed module plus direct reads of `serializers.go`/
`deserializers.go` for the five new ops' request/response shapes,
`types/types.go` for `Session`/`SessionMonitoringConfiguration` and its
three nested logging-config types, and `types/enums.go` for
`SessionState`. `go.mod`/`go.sum` were not touched by this pass (the bump
to v1.64.0 had already landed before this work started).

## 2026-07-31: reverse-SDK-check triage (five findings, five real bugs)

A dashboard sweep, cross-checked against the real
`aws-sdk-go-v2/service/emr@v1.64.0` module directly (`go doc` plus reading
`types/types.go`/the relevant `api_op_*.go` files) before changing
anything, produced five findings. All five were confirmed genuine -- none
were misreadings of the ticket, unlike some findings elsewhere in this
campaign.

**1. Phantom operation: `ListTagsForResource`.** `GetSupportedOperations()`
listed it and `handler_tags.go` fully wired it against `tags.go`, and the
`AddTags`/`RemoveTags`/`ListTagsForResource` ops block above (written
2026-07-24) marked it `wire: ok` -- but no such command exists on the real
EMR SDK client at all (confirmed: `aws-sdk-go-v2/service/emr` has
`api_op_AddTags.go` and `api_op_RemoveTags.go`, no `ListTagsForResource`
file, method, or type of any kind; real tags are read back via
`DescribeCluster.Tags`/`DescribeStudio.Tags`). This is exactly the class of
error `pkgs/sdkcheck`'s new reverse check (commit `12cfe14d5`) exists to
catch -- the forward check (`TestSDKCompleteness`) only ever verified every
*real* op was covered, never that every *advertised* op was real, so this
survived three prior audit passes undetected.
  - **Keep-vs-delete decision:** checked for internal dependents before
    deciding -- `cli.go`, the dashboard, and `ui/src/routes/emr/` do not
    reference `ListTagsForResource` anywhere; only this package's own tests
    (`handler_tags_test.go`, `handler_wire_shape_test.go`,
    `isolation_test.go`, `persistence_test.go`) call it, and they call it as
    a convenient way to assert stored-tag state directly rather than
    hand-decoding `DescribeCluster`/`DescribeStudio` output. Deleting the
    route would require rewriting all of those tests to decode tags from
    Describe* responses instead, for no parity benefit (a real client can
    never reach this route regardless, since it never sends
    `ElasticMapReduce.ListTagsForResource`). Resolution: same as CloudFront's
    `GetFunctionAssociations`/`SetFunctionAssociations` earlier today --
    removed from `GetSupportedOperations()` (and therefore
    `ChaosOperations()`, which delegates to it) so gopherstack no longer
    claims SDK support that doesn't exist, but left routed in `buildOps()`
    as internal test/tooling scaffolding, with a comment on both sites
    explaining why. `TestHandlerOpsLength`/`TestGetSupportedOperations_MatchDispatch`
    (handler_test.go) previously asserted the dispatch table and the
    advertised-ops list were exactly the same size; updated both to expect
    the dispatch table to be exactly `internalOnlyDispatchOps` (1) larger,
    with that constant documented at its declaration rather than a bare `+1`.

**2. Fabricated field: `ClusterSummary.ReleaseLabel`.** The real
`types.ClusterSummary` has exactly six members: `Id`, `Name`, `Status`,
`ClusterArn`, `NormalizedInstanceHours`, `OutpostArn` -- no `ReleaseLabel`.
Deleted the field from the struct (`models.go`) and from
`gatherClusterSummaries` (`clusters.go`). `NormalizedInstanceHours` and
`OutpostArn` remain real, unpopulated members -- an honest omission (a real
client sees them as nil/zero), documented on the type, not fabricated data;
left as-is rather than invented. This field predates today's pass (it was
introduced in the Jul-18 "Go refactoring 2" commit, `9d7e36e00`) and was
missed by the 2026-07-25 audit's explicit claim of "no fabricated fields
found this pass" -- that claim was itself inaccurate at the time it was
written, not just stale.

**3. Severe wire bug: `StartNotebookExecutionInput` dropped `ExecutionEngine`
entirely.** The real input's cluster reference is a top-level
`ExecutionEngine *types.ExecutionEngineConfig{Id, ...}` member. This
handler's `startNotebookExecutionInput` declared it as a nested struct with
JSON tag `"ExecutionEngineConfig"` -- the real *type* name, not the real
*field* name. Because `encoding/json` silently ignores unknown fields
rather than erroring, a real client's `ExecutionEngine` was dropped on
every call, and `NotebookExecution.ExecutionEngineId` was **always empty**
regardless of what cluster the caller named -- not a cosmetic bug, a
complete silent failure of the field that ties a notebook execution to its
cluster. Fixed by renaming the struct field's JSON tag to `"ExecutionEngine"`.
Six existing tests (`handler_notebook_executions_test.go` x5,
`handler_wire_shape_test.go` x1) sent the wrong `"ExecutionEngineConfig"`
key and none of them asserted `ExecutionEngineId` was ever populated in a
response, so the bug produced no visible test failure -- the fourth service
audited today (after quicksight, s3control, lambda) where a test was found
asserting the exact defect it should have caught. All six corrected to send
the real `"ExecutionEngine"` key, and a new
`TestWireShape_StartNotebookExecution_ExecutionEngineField`
(handler_wire_shape_test.go) asserts `ExecutionEngineId` actually round-trips.

**4. `CreateStudio` dropped `Description`.** Real `CreateStudioInput.Description`
is a genuine optional member; `createStudioInput` never declared it at all,
so it was silently ignored the same way (unknown-JSON-field-ignored) as
finding 3 above. `UpdateStudio` (the sibling op) already applies
`Description` correctly, making the omission on `CreateStudio` clearly
unintentional rather than a deliberate simplification. Added the field to
`createStudioInput` and threaded it through a new `description` parameter
on `Backend.CreateStudio` (all eight call sites across the package updated).
New test: `TestWireShape_CreateStudio_Description`.

**5. `UpdateStudio` hardcoded `SubnetIds` to empty.** `handleUpdateStudio`
called `Backend.UpdateStudio` with a literal `""` in the subnet-IDs
position instead of forwarding `in.SubnetIDs` -- accepted on the wire,
documented as applied (the `UpdateStudio` row above already said
`wire: ok`), but never actually applied to the stored `Studio`. No test
previously exercised `UpdateStudio`'s `SubnetIds` handling at all (in fact
no test called the `UpdateStudio` operation at all before today). Fixed by
changing `Backend.UpdateStudio`'s dead `subnetIDsJSON string` parameter
(previously discarded with `_ = subnetIDsJSON`) to a real `[]string`, and
forwarding `in.SubnetIDs` from the handler. Applies a full replace when the
request's `SubnetIds` is non-empty, matching real `UpdateStudioInput.SubnetIds`'
documented semantics ("must also include all of the subnet IDs previously
associated with the Studio" -- i.e. a full-replace contract, not a merge);
an empty/absent list leaves the existing subnets untouched, consistent with
this handler's existing "empty string means unset" convention for
`Name`/`Description`/`DefaultS3Location`. New test:
`TestWireShape_UpdateStudio_AppliesSubnetIds`.

**Regression discipline.** For findings 2 and 3, the new/corrected tests
were run against the pre-fix code (by temporarily reverting the source
change while keeping the corrected test) and confirmed to fail before the
fix landed:
`TestWireShape_ListClusters_NoFabricatedReleaseLabel` failed with "Should be
false" (the fabricated field was present), and
`TestWireShape_StartNotebookExecution_ExecutionEngineField` failed with
`expected: "j-REALCLUSTERID", actual: ""` (the field was silently dropped).
For findings 4 and 5, the corrected tests were written first and confirmed
to fail against the then-unfixed handler (`expected: "a test studio",
actual: ""` and a `SubnetIds` element-count mismatch, respectively) before
the backend/handler fix was applied.

**Relationship to `gopherstack-dqd8`.** That follow-up tracks `Cluster`'s
remaining optional/unpopulated fields (`MonitoringConfiguration`,
`LogEncryptionKmsKeyId`, `OutpostArn`, etc. -- all on the `DescribeCluster`
response type) plus the `ListInstances` synthesis simplification. None of
today's five findings are covered by it: findings 2-5 are on different
types entirely (`ClusterSummary`, `StartNotebookExecutionInput`,
`CreateStudioInput`, `UpdateStudioInput`, none of which `dqd8` mentions),
and finding 1 is an advertised-operations-list problem, not a field-shape
one. The one loose thread worth flagging for whoever picks up `dqd8` next:
`ClusterSummary.NormalizedInstanceHours`/`OutpostArn` (surfaced by finding
2's fix, see above) are the same *kind* of honest omission `dqd8` already
catalogues for `Cluster`, just on the summary type instead -- not a new bug,
but `dqd8`'s title ("optional Cluster fields") doesn't technically cover
`ClusterSummary`, so it's not obviously in scope for whoever closes that
issue either. Left as a note here rather than filed as a new issue, since
it's the same triviality class `dqd8` already exists to hold.

Verified against `aws-sdk-go-v2/service/emr@v1.64.0` (unchanged this pass)
via `go doc` on the installed module and direct reads of `types/types.go`
(`ClusterSummary`, `ExecutionEngineConfig`) and the relevant
`api_op_CreateStudio.go`/`api_op_UpdateStudio.go`/`api_op_StartNotebookExecution.go`
files. `go.mod`/`go.sum` untouched.
