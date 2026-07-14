service: opsworks
sdk_module: aws-sdk-go-v2/service/opsworks@v1.31.0   # exists in the module cache but is NOT
                                                       # a go.mod dependency of this repo (see
                                                       # note below) — audited by reading the
                                                       # module source directly, not via import.
last_audit_commit: cf40ff4d
last_audit_date: 2026-07-12
overall: A            # 1 genuine disguised-no-op bug class fixed across 5 ops
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateStack: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeStacks: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateStack: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteStack: {wire: ok, errors: ok, state: ok, persist: ok, note: cascades layers/instances/apps/deployments/permissions/volumes/rds/ecs}
  CloneStack: {wire: ok, errors: ok, state: ok, persist: ok}
  StartStack: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — see gaps/notes below"}
  StopStack: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — see gaps/notes below"}
  CreateLayer: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeLayers: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateLayer: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteLayer: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  RegisterInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  DeregisterInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  AssignInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  UnassignInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeInstances: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  StartInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — see gaps/notes below"}
  StopInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — see gaps/notes below"}
  RebootInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED — see gaps/notes below"}
  CreateApp: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeApps: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateApp: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteApp: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateDeployment: {wire: ok, errors: ok, state: ok, persist: ok, note: "synchronous — completes with Status=successful immediately, no stuck 'running' state (already correct, established the convention the Start/Stop fix now follows)"}
  DescribeDeployments: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeCommands: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTags: {wire: ok, errors: ok, state: ok, persist: ok, note: paginated via sorted-key nextToken}
families:
  UserProfile: {status: ok, note: "CreateUserProfile/DeleteUserProfile/DescribeUserProfiles/UpdateUserProfile/DescribeMyUserProfile/UpdateMyUserProfile all mutate real state and persist"}
  ElasticLoadBalancer: {status: ok, note: "Attach/Detach/Describe all real"}
  ElasticIp: {status: ok, note: "Register/Deregister/Associate/Disassociate/Describe/Update all real"}
  Volume: {status: ok, note: "Register/Deregister/Assign/Unassign/Describe/Update all real"}
  RdsDbInstance: {status: ok, note: "Register/Deregister/Describe/Update all real"}
  EcsCluster: {status: ok, note: "Register/Deregister/Describe all real"}
  Permission: {status: ok, note: "SetPermission/DescribePermissions real, composite-keyed by stackID+iamUserArn"}
  AutoScaling: {status: ok, note: "SetTimeBasedAutoScaling/DescribeTimeBasedAutoScaling/SetLoadBasedAutoScaling/DescribeLoadBasedAutoScaling all real"}
  Misc: {status: ok, note: "GrantAccess/DescribeServiceErrors(always empty, correct)/DescribeRaidArrays(always empty, correct)/DescribeAgentVersions(static list)/DescribeOperatingSystems(static list) all match AWS's actual mostly-static/deprecated-service behavior"}
gaps:                     # known divergences NOT fixed this pass — low severity, deprioritized vs the state-machine fix
  - "DescribeStackSummary's InstancesCount JSON uses field names 'Starting'/'Total' that do not exist in the real AWS type (real fields: Assigning, Booting, ConnectionLost, Deregistering, Online, Pending, Rebooting, Registered, Registering, Requested, RunningSetup, SetupFailed, ShuttingDown, StartFailed, StopFailed, Stopped, Stopping, Terminated, Terminating, Unassigning — no Total). Harmless in practice: AWS JSON-1.1 clients ignore unknown response fields, and after the state-machine fix in this pass, 'Starting' is always 0 anyway since Start/Stop now commit synchronously to a terminal status. (bd: none filed — low value fix for a fully AWS-deprecated service)"
  - "stacksToJSON emits an extra 'Status' field and DescribeStackProvisioningParameters emits an extra 'StackArn' field, neither of which exist on the real AWS Stack / DescribeStackProvisioningParametersOutput shapes. Same 'harmless extra field, client ignores it' class as above — not fixed this pass."
  - "CreateStack/CreateLayer/CreateApp do not validate several AWS-required parameters they silently drop (e.g. CreateStack's ServiceRoleArn is required by AWS but unvalidated here; CreateLayer's Type is not restricted to AWS's enum). Only Name emptiness is validated. Not fixed this pass — would need a broader validation pass across all Create* ops, out of scope for this budget."
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - "Full parameter surface of CreateStack/CreateLayer/CreateApp/CreateInstance (ConfigurationManager, ChefConfiguration, VpcId, Attributes, BlockDeviceMappings, etc.) — only the fields this backend's Handler already decodes were audited for wire-shape correctness; AWS's much larger optional parameter surface was not modeled."
  - "AssignInstance/UnassignInstance/AssignVolume do not verify the layer/instance belongs to the same stack as its target — cross-stack assignment is silently permitted. Data-integrity nicety, not a wire or state-machine bug."
leaks: {status: clean, note: "No goroutines, timers, or background schedulers in this package — every op is synchronous, so there is nothing to leak. Confirmed no time.AfterFunc/go func/Ticker usage anywhere in services/opsworks/."}
---

## Notes

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
genuinely exists (AWS still generates deprecated-service SDK clients) and was
present in this machine's Go module cache during this audit, but it is **not**
a dependency of this repo's go.mod/go.sum. A previous audit's
`sdk_completeness_test.go` comment claimed no v2 SDK existed at all; that was
wrong and has been corrected in this pass (see the file's updated doc
comment). Future audits: read the module source directly out of
`$(go env GOMODCACHE)/github.com/aws/aws-sdk-go-v2/service/opsworks@<version>`
(or `go doc` after temporarily `go get`-ing it in a scratch module) rather
than trusting only this backend's own JSON output.

**The one real bug this pass fixed — disguised no-op state machine
(backend.go: `StartInstance`, `StopInstance`, `RebootInstance`, `StartStack`,
`StopStack`)**: all five ops set `Instance.Status` to a *transient* value
("starting" / "stopping") and then never transitioned it further — nothing in
this package (no goroutine, no lazy time-based check on read) ever advanced
an instance out of "starting" to "online" or out of "stopping" to "stopped".
Any client polling `DescribeInstances` waiting for the terminal status (a
very common OpsWorks usage pattern, since — unlike some other AWS
services — the SDK ships no built-in waiter for instance state) would spin
forever. Worse, "starting" is not even a valid AWS OpsWorks instance-status
enum value (`booting`, `connection_lost`, `online`, `pending`, `rebooting`,
`requested`, `running_setup`, `setup_failed`, `shutting_down`, `start_failed`,
`stop_failed`, `stopped`, `stopping`, `terminated`, `terminating` — confirmed
against `types.Instance.Status`'s doc comment in the real SDK). Fixed by
committing directly to the accurate terminal status
(`stopped→online`, `online→stopped`, `→online` for reboot), matching the
synchronous-completion convention `CreateDeployment` already established in
this same backend (it sets `Status: deploymentStatusSuccessful` immediately
rather than parking at the unused `deploymentStatusRunning` transient
constant — that constant is dead code, a leftover confirming the intended
convention). This also incidentally fixed a second-order bug: the old code
unconditionally overwrote `Status` on every call regardless of the instance's
current state, so e.g. `StartInstance` on an already-`online` instance would
regress it to `starting` forever; committing to a fixed terminal value makes
repeat calls idempotent.

No background goroutines/timers were introduced to fix this (that would
require wiring a cancelable-goroutine `Close()` lifecycle like
`services/rds`'s `runDelayed` pattern, which is out of scope for a
services/opsworks/-only change) — the synchronous-commit approach was chosen
because it (a) matches the pattern this backend's own `CreateDeployment`
already uses, (b) fully eliminates the stuck-forever bug with zero leak risk,
and (c) needed no cross-file wiring.
