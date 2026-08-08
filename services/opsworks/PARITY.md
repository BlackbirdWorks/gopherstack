service: opsworks
sdk_module: aws-sdk-go-v2/service/opsworks@v1.31.0   # exists in the module cache but is NOT
                                                       # a go.mod dependency of this repo (see
                                                       # note below) — audited by reading the
                                                       # module source directly, not via import.
last_audit_commit: 16830ad82
last_audit_date: 2026-08-08
overall: B            # re-audited live (gopherstack-vjj2) after the 2026-06-03..2026-08-08
                       # unreachability window closed; 2 more real bugs found+fixed via live
                       # HTTP requests, but there is still no SDK-driven test/integration/
                       # suite for this service, so it does not clear this repo's A bar
                       # (gopherstack-parity-audit skill: "A = full integration-suite proof +
                       # every buildable gap closed"). The prior "A" predates this rubric
                       # clarification and was also never exercised by a live request.
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateStack: {wire: ok, errors: ok, state: ok, persist: ok, note: "now validates all 4 required members (Name/Region/DefaultInstanceProfileArn/ServiceRoleArn); wire no longer emits invented 'Status' field"}
  DescribeStacks: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateStack: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteStack: {wire: ok, errors: ok, state: ok, persist: ok, note: cascades layers/instances/apps/deployments/permissions/volumes/rds/ecs}
  CloneStack: {wire: ok, errors: ok, state: ok, persist: ok}
  StartStack: {wire: ok, errors: ok, state: ok, persist: ok}
  StopStack: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateLayer: {wire: ok, errors: ok, state: ok, persist: ok, note: "now validates all 4 required members and restricts Type to the real LayerType enum"}
  DescribeLayers: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateLayer: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteLayer: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "now validates all 3 required members (StackId/LayerIds/InstanceType) and that the target layer exists -- previously silently accepted a nonexistent layer ID"}
  RegisterInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  DeregisterInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  AssignInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "now verifies the target layer exists AND belongs to the same stack as the instance -- previously accepted any layer ID, including nonexistent or cross-stack ones, without checking either"}
  UnassignInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeInstances: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED wire bug -- was emitting a singular 'LayerId' string; real types.Instance has a plural 'LayerIds' []string member, so a real SDK client's Instance.LayerIds field would never have populated from this backend's old response"}
  UpdateInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  StartInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  StopInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  RebootInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateApp: {wire: ok, errors: ok, state: ok, persist: ok, note: "now validates all 3 required members and restricts Type to the real AppType enum; wire no longer emits invented 'Arn' field (real types.App has none)"}
  DescribeApps: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateApp: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteApp: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDeployment: {wire: ok, errors: ok, state: ok, persist: ok, note: "synchronous — completes with Status=successful immediately, no stuck 'running' state"}
  DescribeDeployments: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeCommands: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED -- now restricted to stack/layer ARNs only, matching the real API's documented 'stack or layer's ARN' contract; previously also accepted instance/app ARNs (not real taggable resources)"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "same stack/layer-only restriction as TagResource"}
  ListTags: {wire: ok, errors: ok, state: ok, persist: ok, note: "paginated via sorted-key nextToken; same stack/layer-only restriction as TagResource"}
families:
  UserProfile: {status: ok, note: "CreateUserProfile/DeleteUserProfile/DescribeUserProfiles/UpdateUserProfile/DescribeMyUserProfile/UpdateMyUserProfile all mutate real state and persist"}
  ElasticLoadBalancer: {status: ok, note: "Attach/Detach/Describe all real"}
  ElasticIp: {status: ok, note: "Register/Deregister/Associate/Disassociate/Describe/Update all real"}
  Volume: {status: ok, note: "Register/Deregister/Assign/Unassign/Describe/Update all real. DescribeVolumes now also filters by StackId (real DescribeVolumesInput supports it; this backend previously silently dropped the parameter). Wire no longer emits invented 'StackId' field (real types.Volume has none). AssignVolume now verifies the instance belongs to the same stack the volume was registered with."}
  RdsDbInstance: {status: ok, note: "Register/Deregister/Describe/Update all real. See gaps: DbPassword/Engine/MissingOnRds optional response fields not modeled."}
  EcsCluster: {status: ok, note: "Register/Deregister/Describe all real. FIXED 2026-08-08: DescribeEcsClusters wire emitted an invented 'Status' field -- real types.EcsCluster (SDK v1.31.0) has no such member, only EcsClusterArn/EcsClusterName/StackId/RegisteredAt. Removed from the wire; internal storedEcsCluster.Status kept for bookkeeping only."}
  Permission: {status: ok, note: "SetPermission/DescribePermissions real, composite-keyed by stackID+iamUserArn"}
  AutoScaling: {status: ok, note: "SetTimeBasedAutoScaling/DescribeTimeBasedAutoScaling/SetLoadBasedAutoScaling/DescribeLoadBasedAutoScaling all real"}
  Misc: {status: ok, note: "GrantAccess/DescribeServiceErrors(always empty, correct)/DescribeRaidArrays(always empty, correct)/DescribeAgentVersions(static list)/DescribeOperatingSystems(static list) all match AWS's actual mostly-static/deprecated-service behavior. GetHostnameSuggestion FIXED 2026-08-08 (see gaps-closed note below) -- was entirely unaudited by the previous pass despite being in GetSupportedOperations."}
gaps:                     # divergences from the real API, not fixed this pass
  - "RdsDbInstance responses omit DbPassword, Engine, and MissingOnRds -- all three are real (optional) members of types.RdsDbInstance. DbPassword in particular is unusual: unlike most AWS APIs, the real OpsWorks DescribeRdsDbInstances response actually echoes the password back. Not added this pass -- modeling Engine would require accepting/inferring a DB engine at RegisterRdsDbInstance time (not currently an input), and MissingOnRds requires simulated drift detection this backend has no mechanism for; DbPassword alone would be a small, low-risk addition but was deprioritized against this pass's wire-shape-bug and required-field-validation work."
  - "RegisterInstance/RegisterVolume/RegisterRdsDbInstance/RegisterEcsCluster/SetPermission/CreateUserProfile/AssignVolume do not validate their real-API 'This member is required' string parameters (e.g. RegisterVolume's StackId) for emptiness before using them -- an empty StackId currently falls through to a ResourceNotFoundException (via the not-found lookup) rather than the ValidationException a real client-side-bypassing caller would get. Only CreateStack/CreateLayer/CreateApp/CreateInstance were hardened with required-member validation this pass (these were the ops the previous audit's gap list and this pass's wire-shape sweep specifically flagged); a full required-field sweep across every remaining Register*/Set*/Create* op is deferred to a future pass."
  - "No test/integration/*_parity_test.go suite exists for opsworks (the deprecated SDK isn't a go.mod dependency, so a client-driven integration test needs either vendoring it or hand-rolling raw HTTP requests in the integration-test harness style). This is why overall stays at B rather than A per the gopherstack-parity-audit skill's rubric, even though this pass's live-HTTP verification covered all 73 ops. Building that suite is a real, nontrivial follow-on task, not done this pass."
  - "Error responses (handleError, all branches) are sent with Content-Type: application/json rather than application/x-amz-json-1.1, unlike success responses which correctly get the awsjson1.1 content type from service.HandleTarget. Confirmed harmless for a real aws-sdk-go-v2 client -- deserializers.go's awsAwsjson11_deserializeOpError* functions key off the X-Amzn-ErrorType header and the body's __type/message fields, never Content-Type -- but it's still a wire divergence from a real server. This is a repo-wide pattern (shared by roughly half the awsjson1.1 services grepped, not opsworks-specific), so left unfixed here as out of this pass's bounded scope."
deferred:                 # consciously not audited/implemented this pass (scope)
  - "Full parameter surface of CreateStack/CreateLayer/CreateApp/CreateInstance (ConfigurationManager, ChefConfiguration, VpcId, Attributes, BlockDeviceMappings, etc.) — only the fields this backend's Handler already decodes were audited for wire-shape correctness; AWS's much larger optional parameter surface was not modeled."
  - "AWS's documented AssignInstance business rule (\"You cannot use this action with instances that were created with OpsWorks\" -- i.e. AssignInstance is meant only for RegisterInstance'd on-premises/registered instances, not CreateInstance'd ones) is not enforced. This backend's storedInstance already carries a Registered bool that could gate this, but wiring that check in was judged out of scope for this pass since it wasn't part of the originally-flagged gap list and risks behavior changes beyond the wire-shape/validation fixes made here. AssignInstance DOES now enforce same-stack + layer-existence (see ops.AssignInstance above), which was the explicitly-flagged deferred item."
leaks: {status: clean, note: "No goroutines, timers, or background schedulers in this package — every op is synchronous, so there is nothing to leak. Confirmed no time.AfterFunc/go func/Ticker usage anywhere in services/opsworks/."}
---

## Notes

**Registration (2026-08-08, gopherstack-91e0)**: this package had no `Provider{}`
entry in `cli.go`'s `getServiceProviders` chain from 2026-06-03 (an accidental
drop during an unrelated FSx-service PR's rebase-conflict resolution, per git
history) until this pass. Every audit above, including the grade below, was
performed against code that could not actually receive a request. Now
registered and wired into `wireResourceGroupsTagging`
(`wireTaggingOpsWorks`); `cli_service_registration_test.go` (repo root) now
fails the build if a `services/*/` directory ever again lacks a provider
entry.

**Protocol**: awsjson1.1 (`application/x-amz-json-1.1`), single POST endpoint,
`X-Amz-Target: OpsWorks_20130218.<Op>` dispatch. Route matcher
(`RouteMatcher`) checks the target-prefix only; `ExtractOperation` trims the
prefix and dispatch looks the trimmed name up in `h.ops` (built once at
`NewHandler`/`Reset` from `buildOps()`). `GetSupportedOperations()` and the
dispatch-table keys are asserted equal by `sdk_completeness_test.go` — keep
both lists in sync when adding an op.

**Timestamps**: `CreatedAt`/`CompletedAt`/etc. are formatted as
`"2006-01-02T15:04:05+00:00"` ISO8601 strings (via `time.Time.Format`), not
epoch-seconds numbers. This is correct for OpsWorks — confirmed against the
real SDK's `types.Stack.CreatedAt *string` (and equivalent fields across
Layer/Instance/App/Deployment/Command/EcsCluster) — unlike some other JSON-1.1
services in this repo that use `pkgs/awstime.Epoch`. Do not "fix" this to
epoch format; that would be the actual bug.

**SDK availability**: `github.com/aws/aws-sdk-go-v2/service/opsworks@v1.31.0`
genuinely exists (AWS still generates deprecated-service SDK clients). It is
**not** a dependency of this repo's go.mod/go.sum; this pass fetched it into a
scratch module (`go get` in a throwaway `go.mod` under a temp dir) to read the
real `types.go`/`api_op_*.go` source directly, then discarded the scratch
module. Future audits: do the same, or read
`$(go env GOMODCACHE)/github.com/aws/aws-sdk-go-v2/service/opsworks@<version>`
directly if it's already present in the module cache — do not trust only this
backend's own JSON output as the source of truth for wire shape.

## This pass's fixes (2026-07-23)

Full field-diff of every op's request/response shape against the real SDK's
`types.go`/`api_op_*.go` source (v1.31.0), plus a full read of the previous
audit's `gaps`/`deferred` lists, turned up three previously-known items to
close and three newly-found wire-shape bugs:

1. **`InstancesCount` field names (previously-known gap, now fixed)** —
   `DescribeStackSummary`'s `InstancesCount` used invented field names
   (`Total`, `Starting`, `Stopping`-without-the-rest) that don't exist on the
   real `types.InstancesCount`. Replaced with the exact 19-field real set
   (`Assigning`, `Booting`, `ConnectionLost`, `Deregistering`, `Online`,
   `Pending`, `Rebooting`, `Registered`, `Registering`, `Requested`,
   `RunningSetup`, `SetupFailed`, `ShuttingDown`, `StartFailed`,
   `StopFailed`, `Stopped`, `Stopping`, `Terminated`, `Terminating`,
   `Unassigning` — confirmed against `types.go`). This backend's instance
   state machine only ever produces `online`/`stopped` (see
   `StartInstance`/`StopInstance`), so only `Online`/`Stopped` are ever
   non-zero; the rest exist purely for wire-shape completeness.

2. **Invented `Status`/`StackArn` fields (previously-known gap, now fixed)**
   — `stacksToJSON` emitted a `Status` field not on the real `types.Stack`;
   `DescribeStackProvisioningParameters` emitted a `StackArn` field not on
   the real `DescribeStackProvisioningParametersOutput` (which has only
   `AgentInstallerUrl`/`Parameters`). Both removed from the wire, and the
   now-dead `storedStack.Status` field (only ever set to the constant
   `"running"` and never read anywhere else) was deleted rather than kept as
   inert bookkeeping.

3. **Missing required-field validation (previously-known gap, now fixed for
   the flagged ops)** — `CreateStack` (`Name`/`Region`/
   `DefaultInstanceProfileArn`/`ServiceRoleArn`), `CreateLayer`
   (`Name`/`Shortname`/`StackId`/`Type`-restricted-to-enum), `CreateApp`
   (`Name`/`StackId`/`Type`-restricted-to-enum), and `CreateInstance`
   (`StackId`/`LayerIds`/`InstanceType`) now reject requests missing a
   real-API "This member is required" field with `ValidationException`,
   matching what a real AWS server would do for a raw/non-SDK caller that
   bypasses the SDK's client-side required-field check. `CreateInstance` also
   gained a check that its target layer actually exists (previously
   silently accepted a nonexistent layer ID with no error at all).

4. **NEW: `Instance.LayerIds` wire-shape bug** — `instancesToJSON` emitted a
   singular `"LayerId": "<string>"` field. The real `types.Instance` has no
   such member — only a plural `LayerIds []string`. A real
   `aws-sdk-go-v2` client's `Instance.LayerIds` field would therefore never
   have populated from this backend's `DescribeInstances` response (the
   client silently ignores the unknown `LayerId` key). Fixed by wrapping
   this backend's single-layer-per-instance internal model into a one- or
   zero-element `LayerIds` array on the wire.

5. **NEW: invented `App.Arn` / `Volume.StackId` wire fields** — `appsToJSON`
   emitted an `Arn` field; the real `types.App` has no `Arn` member (apps are
   not independently ARN-addressable in real OpsWorks). `volumesToJSON`
   emitted a `StackId` field; the real `types.Volume` has no `StackId`
   member either. Both removed from the wire (the internal `App.Arn` /
   `Volume.StackID` Go struct fields are kept for internal bookkeeping —
   `Volume.StackID` now also powers `DescribeVolumes`' new `StackId` filter
   — just no longer serialized).

6. **NEW: `TagResource`/`UntagResource`/`ListTags` accepted non-taggable
   ARNs** — the real API's `TagResourceInput`/`UntagResourceInput`/
   `ListTagsInput` doc comments all say "The stack or layer's Amazon
   Resource Number (ARN)" — apps and instances are not independently
   taggable resources on the real API. This backend's `resourceExists`
   previously also matched `:instance/` and `:app/` ARNs, silently allowing
   tagging operations AWS does not support. Restricted to `:stack/`/`:layer/`
   only; tagging an instance or app ARN now returns
   `ResourceNotFoundException`, same as tagging any other nonexistent/
   unsupported resource.

7. **NEW: missing `DescribeVolumes` `StackId` filter** — the real
   `DescribeVolumesInput` supports filtering by `StackId` in addition to
   `InstanceId`/`RaidArrayId`/`VolumeIds`; this backend's `DescribeVolumes`
   signature didn't even accept a stack ID. Added (the `volumesByStack`
   index already existed for `deleteStackAssociations`, so this reused
   existing infrastructure).

8. **NEW: `AssignInstance`/`AssignVolume` cross-stack + existence checks
   (closes the previously-known "deferred" item)** — `AssignInstance`
   previously accepted any `layerIDs[0]` value, including a nonexistent
   layer ID or one belonging to an unrelated stack, with no validation at
   all. `AssignVolume` accepted any instance regardless of which stack it
   belonged to. Both now verify the target exists and belongs to the same
   stack (`AssignInstance` returns `ResourceNotFoundException` for a
   nonexistent layer, `ValidationException` for a cross-stack one;
   `AssignVolume` returns `ValidationException` for a cross-stack instance).
   `UnassignInstance` takes no layer parameter, so there was nothing to
   cross-validate there.

9. **Hygiene: removed dead/invalid status constants** — `instanceStatusStarting`
   (`"starting"`) and `instanceStatusStopping` (`"stopping"`) were unused
   after a previous pass's state-machine fix (nothing ever sets an instance
   to either status anymore) and `"starting"` was never a valid AWS
   OpsWorks instance-status value to begin with (see the real
   `types.Instance.Status` doc comment's enum list). `deploymentStatusRunning`
   was likewise dead (constant defined, never referenced) since
   `CreateDeployment` commits synchronously to `deploymentStatusSuccessful`.
   All three deleted rather than left as inert dead code.

None of these required a `go.mod`/`go.sum` change, a new goroutine/timer, or
touching `cli.go`. All changes stayed within `services/opsworks/`.

## Live re-audit (2026-08-08, gopherstack-vjj2)

The 2026-07-23 pass above (and its `A` grade) was performed while opsworks had
no `Provider{}` entry in `cli.go` — see the Registration note — so nothing in
it had ever been exercised by an actual HTTP request. This pass drove real
requests through a locally built+running server (`bin/gopherstack serve
--persist`, via `.claude/skills/run-gopherstack/driver.sh`) with raw `curl`
setting `X-Amz-Target: OpsWorks_20130218.<Op>` and
`Content-Type: application/x-amz-json-1.1`, since the AWS CLI's bundled
botocore has no `opsworks` service model at all (deprecated service, dropped
from newer botocore data) and can't be used here.

**All 73 ops in `GetSupportedOperations()` were called live and dispatched
correctly** — `buildOps()`'s map and the supported-ops list stayed in sync
(as `sdk_completeness_test.go` already asserted), and no op was found present
in one but missing from the other; there is no "unreachable via dispatch"
finding here; every failure found was a request/response-shape bug in an
otherwise-reachable op, not a routing gap.

Error mapping was checked at the wire, not just "an error occurred": a
missing-required-field `CreateStack` and a bad `Type` enum on `CreateLayer`
both came back `400 ValidationException`; `DescribeStacks`/`GetHostnameSuggestion`
on a nonexistent ID came back `404 ResourceNotFoundException`; an unrecognized
`X-Amz-Target` action came back `400 ValidationException` (not 501) with a
descriptive message — all correct per the real SDK's
`awsAwsjson11_deserializeOpError*` functions (confirmed by reading
`deserializers.go` from the fetched-into-scratch-module SDK source, not
assumed).

Persistence was checked through the real snapshot path, not `Snapshot()`
called directly: started the server with `--persist`, created a stack +
layer + a stack tag, stopped the server (triggers `cli.go`'s
`defer persistManager.SaveAll(ctx)` on SIGTERM), confirmed
`$GOPHERSTACK_DATA_DIR/OpsWorks` was written, restarted, and confirmed
`DescribeStacks`/`DescribeLayers`/`ListTags` returned the same stack, layer,
and tag — a real restart round-trip, not a unit-test `Restore(Snapshot())`
call.

Two real bugs were found this way and fixed (both proven with a failing test
against the pre-fix code in a `git worktree`, per this task's verification
requirement, before being fixed):

1. **`GetHostnameSuggestion` decoded the wrong field and would fail for
   every real client.** The real `GetHostnameSuggestionInput` (confirmed in
   `api_op_GetHostnameSuggestion.go`) has exactly one member, `LayerId`
   (required) — there is no `StackId` on this request at all. This backend's
   handler decoded a `StackId` from the body and its backend method
   `GetHostnameSuggestion(stackID, _ string)` looked up by that `stackID`
   while silently discarding the `layerID` argument entirely. A real SDK
   client only ever sends `LayerId`, so `stackID` would always be empty and
   the call would always fail with `ResourceNotFoundException` — this op was
   functionally broken for every real caller, and the previous pass's
   `PARITY.md` doesn't mention it at all (missed entirely, despite being in
   `GetSupportedOperations()`). Fixed: `StorageBackend.GetHostnameSuggestion`
   now takes only `layerID`, looks it up against `b.layers`, and the handler
   no longer decodes a `StackId`. The response now also echoes back
   `LayerId`, matching the real `GetHostnameSuggestionOutput`'s
   `Hostname`/`LayerId` pair (previously only `Hostname` was returned).
   `stacks_test.go`'s `TestGetHostnameSuggestion` sent only `StackId` and so
   was accidentally testing the bug's contract rather than the real one; it
   now creates a layer and sends `LayerId`, and a copy of the fixed test run
   against the pre-fix code (via `git worktree`) reproduced the live 404.

2. **`DescribeEcsClusters` emitted an invented `Status` field.** The real
   `types.EcsCluster` (confirmed in `types/types.go`) has exactly
   `EcsClusterArn`/`EcsClusterName`/`StackId`/`RegisteredAt` — no `Status`
   member. `ecsClustersToJSON` was serializing this backend's internal
   `storedEcsCluster.Status` (always `"registered"`) onto the wire anyway.
   Removed from the JSON output; the internal field is kept for bookkeeping
   only, matching how `App.Arn`/`Volume.StackId` were handled in the prior
   pass. `ecs_clusters_test.go` gained an `assert.NotContains(t, c, "Status")`
   assertion, confirmed failing against the pre-fix code in a worktree before
   the fix.

Every other response struct returned by a live call in this pass (`Stack`,
`Layer`, `Instance`, `App`, `Deployment`, `Command`, `Volume`, `ElasticIp`,
`ElasticLoadBalancer`, `RdsDbInstance`, `Permission`, `UserProfile`,
`StackSummary`/`InstancesCount`, `TimeBasedAutoScalingConfiguration`,
`LoadBasedAutoScalingConfiguration`, `TemporaryCredential`, `AgentVersion`,
`OperatingSystem`, `DescribeStackProvisioningParametersOutput`) was
field-diffed against the same fetched SDK source and found to emit only a
subset of the real fields — no further invented fields found.

**Why the grade is `B`, not `A`:** every op now proven reachable, correctly
routed, correctly error-mapped at the HTTP boundary, and round-tripping
through real persistence — the specific things this task was scoped to
check. But there is still no `test/integration/*_parity_test.go` suite for
opsworks (blocked on the SDK not being a `go.mod` dependency — see the SDK
availability note above), and the `gopherstack-parity-audit` skill's own
rubric requires that suite for `A`. The prior `A` was ungrounded on two
counts: it predates ever handling a live request, and (independent of that)
it was never actually backed by the integration-suite proof the rubric
requires. `B` reflects genuine, now-verified accuracy without overclaiming
the untouched integration-test gap.
