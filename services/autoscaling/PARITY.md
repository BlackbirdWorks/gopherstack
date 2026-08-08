---
service: autoscaling
sdk_module: aws-sdk-go-v2/service/autoscaling@v1.64.2
last_audit_commit: 1c4ee34e
last_audit_date: 2026-07-23
overall: A            # parity-3 sweep. No aws-sdk-go-v2/service/autoscaling version bump
                       # (still v1.64.2 in go.mod/go.sum). This pass independently
                       # field-diffed the prior pass's "gaps" list against actual code
                       # (per the campaign's "if PARITY.md counts don't match reality,
                       # independently field-diff" instruction) and found it was stale in
                       # two places: the "ASG->EC2" and "ASG/ECS->ELBv2" gaps were already
                       # fixed by a prior, undocumented pass (bd gopherstack-8sk/18k,
                       # confirmed closed; EC2Launcher/ELBv2TargetRegistrar wiring verified
                       # present in ec2_launch.go/elbv2_targets.go) and the
                       # scale-in-lifecycle-hook-gating gap the prior ledger claimed to
                       # have fixed (bd gopherstack-9wo) was in fact genuinely fixed in
                       # code (applyScaleIn/terminationCapacityPreset in
                       # auto_scaling_groups.go) - the bd issue itself was just left open
                       # by mistake, now closed. Real work this pass: (1) implemented the
                       # scheduled-action background scheduler that was the one
                       # deliberately-deferred gap with a live bd id (gopherstack-6ys) -
                       # see families below; (2) wired the 7 CreateAutoScalingGroupInput/
                       # UpdateAutoScalingGroupInput fields the prior ledger listed as
                       # "not attempted" (AvailabilityZoneDistribution,
                       # AvailabilityZoneImpairmentPolicy,
                       # CapacityReservationSpecification, DeletionProtection,
                       # InstanceLifecyclePolicy, InstanceMaintenancePolicy,
                       # SkipZonalShiftValidation), including making DeletionProtection a
                       # real DeleteAutoScalingGroup gate, not just a stored/echoed value;
                       # (3) removed all 7 banned complexity nolints (cyclop/gocognit/
                       # funlen) via decomposition, zero remaining, zero golangci-lint
                       # issues. No leak: `go test -race` clean; the new scheduler goroutine
                       # is ctx-parented and Shutdown-drained via pkgs/worker.SingleRun
                       # (see families below).
ops:
  CreateAutoScalingGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: MixedInstancesPolicy, LifecycleHookSpecificationList, TrafficSources were parsed as no-ops (silently dropped) - now parsed, validated, and registered atomically with the group; initial instances are gated by any launch hook just registered. Prior pass: wired 7 previously-unparsed fields (AvailabilityZoneDistribution, AvailabilityZoneImpairmentPolicy, CapacityReservationSpecification, DeletionProtection, InstanceLifecyclePolicy, InstanceMaintenancePolicy, SkipZonalShiftValidation) - parsed, validated (DeletionProtection enum), stored, and (all but SkipZonalShiftValidation, which real AWS itself never echoes back - verified against types.AutoScalingGroup) projected on Describe. This pass (bd gopherstack-2uti): MixedInstancesPolicy.LaunchTemplate.Overrides.member.N.InstanceRequirements (attribute-based instance-type selection, 24 of 25 sub-fields - see deferred) is now parsed; also fixed a real loop-termination bug in parseLaunchTemplateOverrides - an override carrying only InstanceRequirements (no InstanceType/WeightedCapacity/LaunchTemplateSpecification, the common real-world shape) was indistinguishable from 'no more members', silently truncating every override after it too"}
  DescribeAutoScalingGroups: {wire: ok, errors: ok, state: ok, persist: ok, note: "added MixedInstancesPolicy to the XML projection (was entirely absent from xmlAutoScalingGroup even though the backend model carried it). This pass (bd gopherstack-2uti): projects InstanceRequirements on each override (see CreateAutoScalingGroup)"}
  UpdateAutoScalingGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: MixedInstancesPolicy was not parsed from the request. Prior passes: scale-in path (via applyDesiredCapacityChange) now also gates on a terminating lifecycle hook (bd gopherstack-9wo; re-verified present in code this pass, the bd issue itself was just stale-open); wired the same 7 fields as CreateAutoScalingGroup (see above); each pointer-struct field replaces the group's existing value wholesale when present in the request (matches AWS's opaque-nested-object semantics - there is no partial-field patch for e.g. InstanceMaintenancePolicy). This pass (bd gopherstack-2uti): inherits the InstanceRequirements parsing fix via the shared parseMixedInstancesPolicy/parseLaunchTemplateOverrides helpers"}
  DeleteAutoScalingGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "this pass: DeletionProtection is now a real gate, not just a stored/echoed value - prevent-all-deletion rejects every delete, prevent-force-deletion rejects only ForceDelete=true, matching real AWS's ResourceInUse (ErrorCode) fault. Previously the field didn't exist on the model at all"}
  CreateLaunchConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeLaunchConfigurations: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteLaunchConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeScalingActivities: {wire: ok, errors: ok, state: ok, persist: ok}
  AttachInstances: {wire: ok, errors: ok, state: ok, persist: ok}
  AttachLoadBalancerTargetGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  AttachLoadBalancers: {wire: ok, errors: ok, state: ok, persist: ok}
  AttachTrafficSources: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchDeleteScheduledAction: {wire: ok, errors: ok, state: ok, persist: ok}
  BatchPutScheduledUpdateGroupAction: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: StartTime/EndTime were parsed nowhere and silently dropped; now parsed and stored"}
  CancelInstanceRefresh: {wire: ok, errors: ok, state: ok, persist: ok}
  CompleteLifecycleAction: {wire: ok, errors: ok, state: ok, persist: ok, note: "CRITICAL fix: previously only stopped a timer that was never created anywhere (dead code) and had zero effect on instance state. Now resolves a real pending lifecycle wait (Pending:Wait/Terminating:Wait -> actual transition), looked up by token OR by (group,hook,instance). This pass (bd gopherstack-2uti): ABANDON on a launching hook now terminates AND relaunches a replacement to restore DesiredCapacity (see Notes) - previously it terminated with no replacement, silently leaving the group under capacity"}
  CreateOrUpdateTags: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteLifecycleHook: {wire: ok, errors: ok, state: ok, persist: ok}
  SetDesiredCapacity: {wire: ok, errors: ok, state: ok, persist: ok, note: "scale-out path gates new instances through an active launch hook. This pass: scale-in path now also gates removed instances through an active terminating hook (was previously immediate regardless of hooks; closes bd gopherstack-9wo) via the new applyScaleIn/terminationCapacityPreset machinery - see Notes"}
  TerminateInstanceInAutoScalingGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "CRITICAL fix: now defers actual removal to Terminating:Wait + CompleteLifecycleAction/timeout when a terminating hook is registered, instead of always terminating instantly; also fixed the replacement-instance path never adding the new instance to instanceIndex"}
  PutLifecycleHook: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: NotificationMetadata was never parsed from the request"}
  DescribeLifecycleHooks: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeScheduledActions: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteTags: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeTags: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAutoScalingInstances: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteNotificationConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteScheduledAction: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteWarmPool: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAccountLimits: {wire: ok, errors: ok, state: ok, persist: n/a}
  DescribeAdjustmentTypes: {wire: ok, errors: ok, state: ok, persist: n/a}
  DescribeAutoScalingNotificationTypes: {wire: ok, errors: ok, state: ok, persist: n/a}
  DescribeInstanceRefreshes: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeLifecycleHookTypes: {wire: ok, errors: ok, state: ok, persist: n/a}
  DescribeLoadBalancerTargetGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeLoadBalancers: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeMetricCollectionTypes: {wire: ok, errors: ok, state: ok, persist: n/a}
  DescribeNotificationConfigurations: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribePolicies: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: MinAdjustmentStep and MetricAggregationType were never returned. This pass (bd gopherstack-2uti): now echoes back PredictiveScalingConfiguration (see PutScalingPolicy)"}
  DescribeScalingProcessTypes: {wire: ok, errors: ok, state: ok, persist: n/a}
  DescribeTerminationPolicyTypes: {wire: ok, errors: ok, state: ok, persist: n/a}
  DescribeTrafficSources: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeWarmPool: {wire: ok, errors: ok, state: ok, persist: ok}
  DetachInstances: {wire: ok, errors: ok, state: ok, persist: ok}
  DetachLoadBalancerTargetGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  DetachLoadBalancers: {wire: ok, errors: ok, state: ok, persist: ok}
  DetachTrafficSources: {wire: ok, errors: ok, state: ok, persist: ok}
  DisableMetricsCollection: {wire: ok, errors: ok, state: ok, persist: ok}
  EnableMetricsCollection: {wire: ok, errors: ok, state: ok, persist: ok}
  EnterStandby: {wire: ok, errors: ok, state: ok, persist: ok}
  ExecutePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: StepScaling policies ignored StepAdjustments/MetricValue/BreachThreshold entirely and always used the flat ScalingAdjustment/AdjustmentType path; now selects the matching StepAdjustment interval and validates the required fields. Also routed through applyDesiredCapacityChange so ExecutePolicy scale-out/in now respects SuspendedProcesses, scale-in protection, instanceIndex bookkeeping, and launch-hook gating like SetDesiredCapacity does (previously it duplicated and diverged from that logic). This pass: inherits terminating-hook gating on scale-in for free via the same applyDesiredCapacityChange routing"}
  ExitStandby: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPredictiveScalingForecast: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed: response was missing the required UpdateTime field and returned a wrong-shaped, entirely empty LoadForecast; now returns UpdateTime and a real (though intentionally naive - see Notes) Timestamps/Values series"}
  LaunchInstances: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed 3 bugs: (1) handler read the wrong query param (DesiredCapacity instead of the real RequestedCapacity, so every call silently launched only 1 instance regardless of the requested count); (2) response used the DescribeAutoScalingGroups per-instance shape instead of the real LaunchInstancesOutput InstanceCollection (grouped by AZ/InstanceType with InstanceIds) shape; (3) the backend never added launched instances to instanceIndex, so they could never be found by TerminateInstanceInAutoScalingGroup"}
  PutNotificationConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  PutScalingPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: MetricAggregationType was accepted nowhere on input or output. This pass (bd gopherstack-2uti): PredictiveScalingConfiguration rode along entirely unparsed - accepted with 200 OK, silently discarded (worse than a missing feature: the caller believes predictive scaling is configured and it is not). Now parses the top-level scalar fields (MaxCapacityBreachBehavior/MaxCapacityBuffer/Mode/SchedulingBufferTime) and MetricSpecifications' three predefined-metric variants (PredefinedMetricPairSpecification/PredefinedLoadMetricSpecification/PredefinedScalingMetricSpecification); Customized* variants remain deferred (see deferred)"}
  PutScheduledUpdateGroupAction: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: StartTime/EndTime were parsed nowhere and silently dropped despite the backend model and DescribeScheduledActions XML projection already supporting them"}
  PutWarmPool: {wire: ok, errors: ok, state: ok, persist: ok}
  RecordLifecycleActionHeartbeat: {wire: ok, errors: ok, state: ok, persist: ok, note: "was re-arming a timer that called a no-op (expireHookAction just deleted the map entry); now re-arms to re-resolve with the hook's DefaultResult, and supports lookup by instance ID (not just token)"}
  ResumeProcesses: {wire: ok, errors: ok, state: ok, persist: ok}
  RollbackInstanceRefresh: {wire: ok, errors: ok, state: ok, persist: ok}
  SetInstanceHealth: {wire: ok, errors: ok, state: ok, persist: ok}
  SetInstanceProtection: {wire: ok, errors: ok, state: ok, persist: ok}
  StartInstanceRefresh: {wire: ok, errors: ok, state: ok, persist: ok}
  SuspendProcesses: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  static-describe-types (AdjustmentTypes/NotificationTypes/LifecycleHookTypes/MetricCollectionTypes/ScalingProcessTypes/TerminationPolicyTypes): {status: ok, note: "unchanged this pass; verified op-by-op against the SDK enum lists, all correct"}
  instance-refresh (Start/Cancel/Describe/Rollback): {status: ok, note: "unchanged this pass; wire shapes and status machine (InProgress/Cancelling/RollbackInProgress) verified against SDK"}
  warm-pool (Put/Delete/Describe): {status: ok, note: "unchanged this pass; verified"}
  metrics-collection / suspend-resume-processes / standby: {status: ok, note: "unchanged this pass; verified"}
  ec2-provisioning (ASG->EC2 real instance launch/terminate via EC2Launcher): {status: ok, note: "was gap bd gopherstack-8sk, marked NOT-fixed by the prior ledger; independently field-diffed this pass and found ALREADY fixed by an undocumented earlier pass - services/autoscaling/ec2_launch.go defines EC2Launcher (LaunchInstances/TerminateInstances), auto_scaling_groups.go/instances.go route scale-out/in through it when wired (SetEC2Launcher), bd gopherstack-8sk is closed. Ledger corrected to reflect reality"}
  elbv2-target-registration (ASG->ELBv2 real target register/deregister via ELBv2TargetRegistrar): {status: ok, note: "was gap bd gopherstack-18k, marked NOT-fixed by the prior ledger; independently field-diffed this pass and found ALREADY fixed by the same undocumented earlier pass - services/autoscaling/elbv2_targets.go defines ELBv2TargetRegistrar (RegisterTargets/DeregisterTargets), wired into attach/detach/scale-in paths, bd gopherstack-18k is closed. Ledger corrected to reflect reality"}
  scheduled-action-scheduler (background execution of Put/BatchPutScheduledUpdateGroupAction): {status: ok, note: "NEW this pass, closing bd gopherstack-6ys. Prior passes correctly parsed/persisted StartTime/EndTime/Recurrence but nothing ever evaluated them against wall-clock time - DescribeScheduledActions reflected what was requested, but no action ever fired. Added scheduled_action_cron.go (5-field Unix-cron parser matching AWS's documented Recurrence format: minute hour day-of-month month day-of-week - distinct from EventBridge's 6-field cron() with a year field) and scheduled_action_scheduler.go (ScheduledActionScheduler, a service.BackgroundWorker: 1-minute ticker, wired via pkgs/worker.SingleRun in handler.go's StartWorker/Shutdown so it is ctx-parented and Shutdown-drained like every other service's background worker in this codebase). Each tick applies any due action's MinSize/MaxSize/DesiredCapacity through the same validated capacity path (applyUpdateCapacityLocked) UpdateAutoScalingGroup uses, so it inherits identical validation/error behavior. Covers one-time actions (Recurrence empty, fires once at/after StartTime) and recurring actions (bounded by StartTime/EndTime when set); a new ScheduledAction.LastExecutedTime field (internal bookkeeping, not on the wire - AWS's real ScheduledUpdateGroupAction response type has no equivalent field) prevents re-firing the same occurrence and prevents an invalid action from busy-looping every tick"}
  lifecycle-hook-chaining (multiple hooks on one transition): {status: ok, note: "FIXED this pass (bd gopherstack-9tqg, deferred from bd gopherstack-2uti/b7d3a8485). Registering a second+ hook on the same transition previously armed nothing - see dated Notes section below for the ordering rule, the chain data model, and how it composes with ABANDON's terminate-and-replace"}
gaps:
  - GetPredictiveScalingForecast returns a real, well-shaped, non-empty forecast, but it is a flat naive projection (current DesiredCapacity repeated hourly), not a statistical model - genuinely out of scope for an emulator; documented simplification, see Notes
deferred:
  - InstanceRequirements.BaselinePerformanceFactors (the one field of InstanceRequirements's 25 not modelled - see Notes for the other 24, fixed this pass). It nests a CPU-instance-family reference list (CpuPerformanceFactorRequest.References []PerformanceFactorReferenceRequest) that has no analogue elsewhere in this handler; deliberately not attempted this pass. No bd id filed yet.
  - PredictiveScalingConfiguration.MetricSpecifications[].Customized{Capacity,Load,Scaling}MetricSpecification (the CloudWatch MetricDataQuery/math-expression variant of a predictive scaling metric spec - see Notes for the predefined-metric variants, fixed this pass). Deliberately not attempted this pass: MetricDataQuery is a full CloudWatch metric-math sub-language shared with GetMetricData, out of scope for a PutScalingPolicy fix. No bd id filed yet.
leaks: {status: clean, note: "go test -race passes (verified this pass). The pendingHookTokens timer machinery (the CRITICAL item flagged in a prior sweep) remains real (armed on every gated launch/terminate), Close() stops all of them, DeleteAutoScalingGroup/DeleteLifecycleHook/Purge call cleanupHookTimers, and Restore() re-arms timers for any instance left in a *:Wait state. NEW this pass: the ScheduledActionScheduler's 1-minute ticker goroutine is started via pkgs/worker.SingleRun.Start in Handler.StartWorker and stopped (cancelled + waited-on) via pkgs/worker.SingleRun.Stop in Handler.Shutdown - the exact same ctx-parented/Shutdown-drained shape every other backgroundWorker service in this codebase uses (e.g. secretsmanager's rotation scheduler). TestScheduledActionScheduler_RunFiresAndStopsCleanly explicitly starts the real ticker, waits for it to fire, cancels its context, and asserts Run() returns within 2s. testleak.VerifyTestMain (leak_main_test.go) additionally guards the whole package: any test that started a worker without stopping it would fail the suite."}
---

## Notes

Protocol: EC2 Auto Scaling uses the `query` (form-urlencoded request, XML response)
protocol, `Version=2011-01-01`. Verified against the awsquery serializers/deserializers
in `aws-sdk-go-v2/service/autoscaling@v1.64.2`.

### bd gopherstack-2uti (2026-08-08): PredictiveScalingConfiguration, InstanceRequirements, ABANDON auto-relaunch

Three specific, previously-documented gaps (see prior `gaps`/`deferred` entries this
section replaces), addressed in priority order (worst failure mode first, per the
issue's own priority note - a config accepted and silently discarded is worse than a
missing feature or a documented simplification):

1. **`PutScalingPolicy`/`DescribePolicies`: `PredictiveScalingConfiguration`.**
   Verified the exact query-protocol flattening against
   `aws-sdk-go-v2/service/autoscaling@v1.70.4`'s `serializers.go:5967`
   (`awsAwsquery_serializeDocumentPredictiveScalingConfiguration`) and
   `deserializers.go:16133` before writing any parsing code, rather than inferring it
   from field names: `PredictiveScalingConfiguration.{MaxCapacityBreachBehavior,
   MaxCapacityBuffer,Mode,SchedulingBufferTime}` are flat scalars;
   `MetricSpecifications` is a standard `.member.N.`-flattened list (confirmed against
   `aws-sdk-go-v2@v1.43.4`'s `aws/protocol/query/array.go` - non-flat arrays are
   `<prefix>.<memberName>.<n>`, memberName is always `"member"` for this protocol,
   matching every other list in this handler); each element has an independent
   `TargetValue` plus one of three `{PredefinedMetricType,ResourceLabel}`-shaped
   predefined-metric objects (`PredefinedMetricPairSpecification`/
   `PredefinedLoadMetricSpecification`/`PredefinedScalingMetricSpecification`,
   `types.go:2743/2778/2821`). Added `PredictiveScalingConfiguration`/
   `PredictiveScalingMetricSpecification`/`PredefinedMetricRef` to `models.go` and
   wired parse/echo into `handler_scaling_policies.go`. The `Customized*` metric
   variants (CloudWatch `MetricDataQuery` math expressions) are deliberately not
   modelled - see `deferred`.

2. **`MixedInstancesPolicy.LaunchTemplate.Overrides[].InstanceRequirements`**
   (attribute-based instance-type selection, `types.go:1267`, 25 fields). Modelled 24
   of 25 as `IntRangeRequest`/`FloatRangeRequest` `{Min,Max}` pairs (6 int, 3 float),
   8 `.member.N`-flattened string lists, 3 plain string enums, 3 `*int32` scalars, and
   1 `*bool`; `BaselinePerformanceFactors` (nests a CPU-instance-family reference list
   with no analogue elsewhere in this handler) is the one field deferred - see
   `deferred`. Verified the flattening the same way as (1), directly against
   `serializers.go:5230` (`awsAwsquery_serializeDocumentInstanceRequirements`) and its
   sub-message serializers, and `deserializers.go:12592` for the response side.
   Fixing this also surfaced and fixed a real, independent bug: `parseLaunchTemplateOverrides`'s
   loop-continuation check only looked at
   `InstanceType`/`WeightedCapacity`/`LaunchTemplateSpecification` presence - an
   override carrying only `InstanceRequirements` (no `InstanceType`, the whole point
   of attribute-based selection, and the shape Terraform emits for
   `instance_requirements` blocks) was indistinguishable from "end of list", silently
   truncating every override after it in the same request too. Covered by
   `TestAutoscalingHandler_MixedInstancesPolicyInstanceRequirementsRoundTrip`, which
   asserts a *second* override with a plain `InstanceType` survives past a first,
   `InstanceRequirements`-only override - verified this fails pre-fix (empty
   `<Overrides>`, both members dropped).

3. **ABANDON on a launching lifecycle hook now relaunches a replacement.** AWS's
   lifecycle-hooks docs (`docs.aws.amazon.com/autoscaling/ec2/userguide/lifecycle-hooks.html`,
   "Considerations and limitations", fetched this pass) state: "If an instance is
   launching, continue indicates that your actions were successful, and that Amazon
   EC2 Auto Scaling can put the instance into service. Otherwise, abandon indicates
   that your custom actions were unsuccessful, and that **we can terminate and replace
   the instance**." (emphasis added). The prior implementation terminated the failed
   instance but never replaced it, silently leaving the group permanently under
   `DesiredCapacity` until some unrelated event (a scaling policy, a manual
   `SetDesiredCapacity` call) happened to top it back up. Fixed in
   `applyLifecycleResult`'s launching/ABANDON branch by reusing the exact same
   top-up-to-`DesiredCapacity` pattern `finishTermination`'s `terminationReplace`
   disposition already used for the analogous terminating-hook case
   (`adjustInstances` + `instanceIndex` registration + `gateNewLaunchInstances`, so
   the replacement is itself gated by the same launch hook, matching real AWS - a
   replacement for an abandoned instance is a normal launch, not a bypass). Required
   updating one existing test
   (`TestAutoscalingHandler_LifecycleHookGatesLaunch`/"abandon...") whose assertion
   (`assert.Empty(t, gotInstances, ...)`) encoded the old, AWS-incorrect behavior;
   verified the updated test fails against the pre-fix code (group stuck at 0
   instances despite `DesiredCapacity=1`) and passes after.

   The same docs page also answers the *multiple-hooks-on-one-transition* ordering
   question this issue asked about, for the *terminating* case specifically: "If an
   instance is terminating, both abandon and continue allow the instance to
   terminate. However, **abandon stops any remaining actions, such as other lifecycle
   hooks, and continue allows any other lifecycle hooks to complete**." This confirms
   AWS's model is an ordered chain with a short-circuit-on-abandon semantic, not
   documented true concurrency, and there is no `order`/`priority` field anywhere in
   `PutLifecycleHookInput`/`LifecycleHookSpecification` to determine the chain order -
   the SDK does not determine it, and neither does this doc page. Implementing the
   chain (arm one hook at a time per instance+transition, advance to the next on
   CONTINUE, short-circuit on ABANDON, across all 4 `armLifecycleWait` call sites plus
   the Restore-time `rearmPendingWaits` path) is a materially larger, riskier change
   than (1) and (2) above and was deliberately left as a documented gap rather than
   rushed - see `gaps`.

### bd gopherstack-9tqg (2026-08-08): lifecycle-hook chaining

Implemented the chain deferred by bd `gopherstack-2uti`/b7d3a8485 above: registering a
second (or third, ...) hook on the same transition now actually arms it, instead of
silently doing nothing.

**Ordering rule.** Neither `PutLifecycleHookInput` nor `LifecycleHookSpecification`
(`aws-sdk-go-v2/service/autoscaling@v1.70.4` `api_op_PutLifecycleHook.go:70-140`,
`types.go:1973-2020`) carries an order/priority field, and `DescribeLifecycleHooksOutput`
(`api_op_DescribeLifecycleHooks.go:42-51`) is a plain `[]types.LifecycleHook` with no
ordering metadata either - the SDK does not determine chain order, and the pending-hook
wait itself is not observable to a client beyond the instance's coarse
`LifecycleState` (`Pending:Wait`/`Terminating:Wait`; there is no per-hook field on
`AutoScalingInstanceDetails`). Chose **registration order** as the defensible default
and documented it here and in `lifecycleHookChain`'s doc comment
(`lifecycle_hooks.go`): each `LifecycleHook` gets an internal-only `Sequence` field,
assigned once from a backend counter (`nextHookSeq`) the first time a hook of that name
is registered (`putLifecycleHookLocked`) and preserved across updates to the same hook.
`Sequence` is never sent or accepted on the wire - `handleDescribeLifecycleHooks` builds
`xmlLifecycleHook` field-by-field rather than by converting `LifecycleHook` directly, so
adding it couldn't leak onto the response the way a straight type conversion would have.

**Mechanics.** `armLifecycleWait` now also records the armed hook's name on the
instance (`Instance.LifecycleHookName`). `applyLifecycleResult`, on CONTINUE, looks up
the next hook in `lifecycleHookChain` after the one that just resolved and re-arms the
same instance on it instead of applying the transition's terminal effect; only once the
chain is exhausted does it fall through to `InService`/`finishTermination`. ABANDON
never consults the chain - it goes straight to the terminal effect at whatever position
it occurred, i.e. short-circuits.

**Composing with b7d3a8485's terminate-and-replace.** ABANDON on a launching hook,
at any chain position, still reuses the exact same terminate-and-replace branch
(`removeInstanceByID` + `adjustInstances` + `gateNewLaunchInstances`) that existed
before this pass - chaining only changes *which* hook's resolution can reach that
branch, not the branch itself. The replacement instance is a brand-new `Instance`, so
`gateNewLaunchInstances` arms it via `firstHookInChain`, restarting the launch chain
from hook 1 rather than continuing wherever the abandoned instance's chain position
was. Verified with a three-hook launching test
(`launchChainAbandonShortCircuits`): hook-1 CONTINUE advances to hook-2 (proving the
chain-advance code path actually ran, not merely that a single-hook flow still works),
hook-2 ABANDON terminates-and-replaces without hook-3 ever being armed, and the
replacement is confirmed back at hook-1 (completing hook-3 on it is a no-op; completing
hook-1 is not).

**Restore mid-chain.** The hardest part: `pendingHookTokens` (in-flight timers/action
state, including which hook is currently gating an instance) is deliberately never
persisted, but which hook an instance is paused on cannot be recovered from
`lifecycleHookChain` alone once the group's earlier hooks have already resolved. Added
`Instance.LifecycleHookName` (internal-only, rides along transparently in the existing
`AutoScalingGroup`/`store.Table[AutoScalingGroup]` JSON snapshot - additive field, no
`autoscalingSnapshotVersion` bump needed, same precedent as b7d3a8485's
`PredictiveScalingConfiguration` addition) so `rearmPendingWaits` can look the specific
hook back up by name after `Restore()`, falling back to `firstHookInChain` only when
that hook is empty (pre-chain-tracking snapshot) or gone (deleted while persisted).
`nextHookSeq` itself is not persisted (it is not observable through any AWS API, so
there is nothing to keep byte-identical across a restore); `Restore()` recomputes it as
the max `Sequence` across restored hooks, so hooks registered post-restore chain after
all of them rather than colliding. Verified with a dedicated round-trip test
(`TestAutoscalingHandler_LifecycleHookChainResumesAfterRestore`): a two-hook launch
chain is advanced past hook-1 (now waiting on hook-2), snapshotted, restored into a
fresh backend, and completing hook-1 again post-restore is asserted to be a no-op
(proving the chain did not restart) while completing hook-2 is what actually resolves
it (proving it correctly resumed there) - confirmed failing pre-fix (hook-1 alone
finishes the transition, since pre-fix `rearmPendingWaits` always re-armed the chain's
first hook).

**Data-model change, stated explicitly**: this required two new fields -
`LifecycleHook.Sequence` (chain ordering) and `Instance.LifecycleHookName` (chain
position, for restore) - neither of which exists on AWS's wire types; both are
internal bookkeeping only.

### Parity-3 sweep (2026-07-23): scheduler, 7 CreateASG/UpdateASG fields, ledger correction

This pass found the prior ledger's `gaps` list had drifted from reality in two places:
the "ASG->EC2 real instance provisioning" gap (bd `gopherstack-8sk`) and the
"ASG/ECS->ELBv2 target registration" gap (bd `gopherstack-18k`) were both listed as
"NOT fixed this pass per scope", but both bd issues were actually already `CLOSED`
(2026-07-12) and both `EC2Launcher` (`ec2_launch.go`) and `ELBv2TargetRegistrar`
(`elbv2_targets.go`) are present and wired into the scale-out/scale-in/attach/detach
paths. Grepped for the adapter types and their call sites to confirm before correcting
the ledger - per the campaign's "if PARITY.md counts don't match reality, independently
field-diff and record what you verify" instruction, not just trusting the bd close
reasons. Moved both from `gaps` to `families` as `ok`.

Similarly, bd `gopherstack-9wo` (terminate-lifecycle-hook gating on the
desired-capacity-driven scale-in path) was still `OPEN` in the tracker, but reading
`auto_scaling_groups.go` shows `applyScaleIn`/`terminationCapacityPreset` fully
implementing exactly what the 2026-07-12 re-audit pass's notes (below) describe. The
code was correct; only the bd issue's status was stale. Closed it this pass rather than
re-doing already-complete work.

**Real new work this pass:**

1. **Scheduled-action background scheduler** (bd `gopherstack-6ys`, the one gap in the
   prior ledger with a live, still-open bd id describing a genuine missing feature, not
   ledger drift). Added `scheduled_action_cron.go` (a 5-field Unix-cron parser -
   `minute hour day-of-month month day-of-week`, matching AWS's documented
   `ScheduledUpdateGroupAction.Recurrence` format) and
   `scheduled_action_scheduler.go` (`ScheduledActionScheduler`, wired as a
   `service.BackgroundWorker` via `pkgs/worker.SingleRun` in `handler.go`'s
   `StartWorker`/`Shutdown`, matching the exact lifecycle shape secretsmanager's
   rotation scheduler and every janitor-style worker in this codebase already use).
   Deliberately reuses `applyUpdateCapacityLocked` (the same helper
   `UpdateAutoScalingGroup` calls) to apply a due action's MinSize/MaxSize/
   DesiredCapacity, so scheduled-action capacity changes get identical validation
   behavior to a manual `UpdateAutoScalingGroup` call for free, rather than
   duplicating (and risking diverging from) that logic - the same lesson the prior
   pass's `ExecutePolicy`/`applyDesiredCapacityChange` fix already established for this
   service. A new `ScheduledAction.LastExecutedTime` field (internal bookkeeping only,
   not projected onto the `DescribeScheduledActions` XML response, since AWS's real
   wire type has no equivalent) prevents a one-time action from refiring and prevents a
   since-invalid recurring action from busy-looping every tick forever (it still logs a
   warning and stamps `LastExecutedTime` so it doesn't retry the same occurrence
   indefinitely - see `fireScheduledActionLocked`).

2. **7 previously-unwired `CreateAutoScalingGroupInput`/`UpdateAutoScalingGroupInput`
   fields**: `AvailabilityZoneDistribution`, `AvailabilityZoneImpairmentPolicy`,
   `CapacityReservationSpecification`, `DeletionProtection`, `InstanceLifecyclePolicy`,
   `InstanceMaintenancePolicy`, `SkipZonalShiftValidation`. Field-diffed each nested
   type and its awsquery-serialized param names against
   `aws-sdk-go-v2/service/autoscaling/types` and `serializers.go`/`deserializers.go`
   directly (via `go doc` + reading the generated serializer functions) rather than
   guessing param names from the field names alone - this caught that the real wire
   field is `CapacityReservationIds` (matching `CapacityReservationTarget`'s Go SDK
   field name exactly, no "ID" initialism), which an early pass of this change
   accidentally renamed to `CapacityReservationIDs` while fixing a `revive` lint
   warning on the *Go identifier* (a legitimate rename) before catching that the *wire
   string* used in `parseMembers`/the XML tag must NOT follow that rename. Fixed by
   keeping the Go field named `CapacityReservationIDs` (satisfies `revive`) while the
   `parseMembers` prefix and the `xml:"..."` tag stay `CapacityReservationIds` (matches
   the real wire byte-for-byte). Flagging this here because it is exactly the kind of
   near-miss the next auditor should watch for: an identifier-hygiene lint fix silently
   corrupting a wire-format string literal that happens to share the identifier's name.
   `DeletionProtection` is the one field with real behavioral impact beyond store-and-
   echo: `DeleteAutoScalingGroup` now gates on it (`prevent-all-deletion` blocks every
   delete, `prevent-force-deletion` blocks only `ForceDelete=true`), matching real AWS's
   `ResourceInUseFault` (`ErrorCode()` is `"ResourceInUse"`, not `"ResourceInUseFault"` -
   read the SDK's generated `errors.go` to confirm, don't assume the Go type name is the
   wire code). `SkipZonalShiftValidation` is accepted and stored but never projected on
   `DescribeAutoScalingGroups`, because real AWS's `types.AutoScalingGroup` response
   type has no such field either (confirmed via `go doc`) - it is a one-time
   launch/update validation-bypass flag, not a persistent group attribute.

3. **Removed all 7 banned complexity nolints** (`cyclop`/`gocognit`/`funlen` across
   `handler_scaling_policies.go`, `handler_auto_scaling_groups.go` x2,
   `instances.go`, `auto_scaling_groups.go` x2, `handler_launch_configurations.go`) by
   extracting focused helper functions (e.g. `applyUpdateCapacityLocked`/
   `applyUpdateLaunchSourceFields`/`applyUpdateScalarFields`/`applyUpdatePolicyFields`
   replacing one large `UpdateAutoScalingGroup`; `parseCreateASGSizeFields`/
   `applyUpdateASGSizeFields`/`applyUpdateASGBoolFields` replacing repetitive
   parse-and-check blocks with small param-table-driven loops). Zero
   `golangci-lint` issues after (was already near-zero; the field-table-driven helpers
   this decomposition introduced initially tripped `govet fieldalignment` on their
   anonymous struct literals - fixed by reordering fields, not suppressing).

### Re-audit pass (2026-07-12): scale-in lifecycle-hook gating fix

This pass found no local drift under `services/autoscaling/` since ce30166a (the
commit that actually authored this ledger - the previously-recorded
`last_audit_commit: d0ebe979` was not an ancestor of HEAD, so ce30166a was used as
baseline per the re-audit protocol) and no `aws-sdk-go-v2/service/autoscaling`
dependency bump (still pinned at v1.64.2 in `go.mod`/`go.sum`), even though a sibling
commit in this repo's history bumped other Go/UI dependencies. All `ok` rows above
were therefore trusted unchanged and not re-verified wire-shape-by-wire-shape.

One item explicitly called out in the prior pass's `gaps` list (and filed as bd
`gopherstack-9wo`) was fixed this pass: a registered `EC2_INSTANCE_TERMINATING`
lifecycle hook gated instance removal in `TerminateInstanceInAutoScalingGroup`, but
NOT in the desired-capacity-driven scale-in path shared by `SetDesiredCapacity`,
`UpdateAutoScalingGroup`, and `ExecutePolicy` (all three route through
`applyDesiredCapacityChange`). That path (`services/autoscaling/backend.go`, the old
`removeUnprotectedInstances` helper) always removed instances from
`g.Instances`/`b.instanceIndex` immediately and unconditionally, regardless of any
configured terminating hook - the exact "disguised stub" class this service's ledger
has previously flagged (state mutated, but the one config knob that should have
changed behavior was silently ignored).

Fixed by replacing `removeUnprotectedInstances` with `(*InMemoryBackend).applyScaleIn`,
which keeps the original protected-instance selection algorithm (remove from the end,
skip `ProtectedFromScaleIn`, stop short of target if everything eligible is
protected - now also skipping instances already in `Terminating:Wait` so a second
scale-in call while one is still pending doesn't double-select it) but branches on
whether the group has an active terminating hook:

- **No hook**: unchanged behavior - instances are removed from `g.Instances` and
  `b.instanceIndex` immediately.
- **Hook present**: selected instances are NOT removed. Each is transitioned to
  `Terminating:Wait` in place (staying in `g.Instances`, consistent with the
  `TerminateInstanceInAutoScalingGroup` gating path and the "Traps for the next
  auditor" note below) and a heartbeat timer is armed via the existing
  `armLifecycleWait`/`resolveLifecycleWait`/`finishTermination` machinery, exactly
  like the single-instance path.

The one real complication (the reason the prior pass deferred this): unlike
`TerminateInstanceInAutoScalingGroup`, where `DesiredCapacity`/`MinSize` are only
decremented once the wait resolves (`ShouldDecrementDesiredCapacity`), the
desired-capacity-driven path sets `g.DesiredCapacity = newDesired` **immediately, up
front**, before any instance is actually removed or gated (`applyDesiredCapacityChange`,
the assignment precedes the `switch`). Reusing `finishTermination`'s existing
decrement-or-replace disposition would have double-decremented (or wrongly launched a
replacement to backfill the already-lowered target) once the wait resolved. Fixed by
generalizing the previously-boolean `pendingHookAction.ShouldDecrement` into a
three-way `terminationDisposition` enum (`terminationReplace` / `terminationDecrement`
/ `terminationCapacityPreset`), and giving scale-in-originated waits
`terminationCapacityPreset`: `finishTermination` removes the instance and does
nothing further to capacity bookkeeping for that disposition, since
`applyDesiredCapacityChange` already applied the target capacity before the wait was
even armed. `terminationReplace` is the enum's zero value, preserving the exact
existing fallback behavior for `rearmPendingWaits` (Restore-time re-arming, which
never persisted the original disposition and defaults to the replace behavior, as
before this change).

Net effect: `DescribeAutoScalingGroups` immediately reflects the new
`DesiredCapacity` after a scale-in call (matching real AWS - the target is accepted
immediately), while the actual instance count/`Terminating:Wait` state lags until the
hook resolves, exactly mirroring the single-instance-termination gating path that was
already correct. Covered by new tests: `Test_LifecycleHookGatesDesiredCapacityScaleIn`
(`parity_b_test.go`, full HTTP round-trip through `SetDesiredCapacity` +
`CompleteLifecycleAction`) and two new subtests of
`TestInMemoryBackend_SetDesiredCapacity` (`backend_test.go`) covering the no-hook
immediate-removal path and scale-in-protection interaction at the unit level.

### The lifecycle-hook fix in detail (highest-value finding this pass)

Prior-sweep notes flagged "lifecycle-hook timeout goroutines" as CRITICAL. The
investigation this pass found the real bug was worse than a leak: `pendingHookTokens`,
its `*time.Timer` field, `cleanupHookTimers`, and `expireHookAction` all existed and
looked plausible, but **nothing anywhere ever inserted an entry into
`pendingHookTokens`**. `PutLifecycleHook` stored the hook; `CompleteLifecycleAction`
and `RecordLifecycleActionHeartbeat` only ever *looked up* an entry that could never
exist. The net effect: creating a lifecycle hook had zero effect on any instance
transition. New instances always went straight to `InService`; terminated instances
were always removed immediately. This is the "disguised stub" pattern called out in
`parity-principles.md` #4 - the code looked real (validated params, stored state,
returned 200) but never touched the one thing lifecycle hooks exist for.

Fixed by wiring real gating into every instance-creating and instance-terminating
code path:
- launch gating: `CreateAutoScalingGroup` (initial instances, only relevant once
  `LifecycleHookSpecificationList` was also wired up - see below),
  `applyDesiredCapacityChange` (scale-out, shared by `SetDesiredCapacity`,
  `UpdateAutoScalingGroup`, and now `ExecutePolicy`), `LaunchInstances`, and the
  replacement-instance branch of `TerminateInstanceInAutoScalingGroup`.
- terminate gating: `TerminateInstanceInAutoScalingGroup` only (see gaps: the
  desired-capacity-driven scale-in path in `applyDesiredCapacityChange` was
  deliberately left as immediate removal - see below).
- `CompleteLifecycleAction` and `RecordLifecycleActionHeartbeat` now resolve/re-arm a
  real pending action, looked up by token OR by `(group, hook, instanceId)` since AWS
  allows either.
- Heartbeat timeout expiry calls the same resolution path with the hook's
  `DefaultResult`, so an abandoned wait behaves identically whether it was resolved
  explicitly or by timeout.

**Scope boundary, stated explicitly**: terminate-hook gating was *not* wired into the
`applyDesiredCapacityChange` scale-in path (used by `SetDesiredCapacity` decreasing,
`UpdateAutoScalingGroup`, `ExecutePolicy` scale-in). That path currently still removes
instances immediately regardless of a registered terminating hook. Reason: unlike
`TerminateInstanceInAutoScalingGroup` (a single, self-contained instance removal),
scale-in there interacts with `removeUnprotectedInstances`'s batch compaction and the
desired-capacity bookkeeping for potentially many instances at once; deferring N
removals concurrently, each independently completable/timeoutable, while keeping the
group's effective capacity accounting consistent for concurrent
`DescribeAutoScalingGroups` callers, is a meaningfully bigger state machine than the
single-instance case and was judged too risky to rush. Filed as a known, explicit gap
above rather than silently left broken.

**ABANDON semantics** (launching case fixed bd `gopherstack-2uti`/b7d3a8485; chaining
fixed bd `gopherstack-9tqg` - see dated section below): for a *launching* hook, ABANDON
terminates the pending instance and relaunches a replacement to restore
`DesiredCapacity`, gated by the same launch chain from its first hook. ABANDON at any
position in either chain now also short-circuits every hook still to come, matching
AWS's documented "abandon stops any remaining actions, such as other lifecycle hooks".
For a *terminating* hook, both CONTINUE and ABANDON eventually let the instance
terminate (you cannot veto a termination via a terminating lifecycle hook) - CONTINUE
differs from ABANDON only in whether the next chained hook, if any, gets to run first.

**Multiple-hooks-per-transition chaining** (fixed bd `gopherstack-9tqg`, deferred from
bd `gopherstack-2uti`/b7d3a8485 - see dated section below for the full writeup):
hooks on the same transition now form AWS's documented ordered chain instead of only
the first ever being armed.

**Restore/persistence**: `pendingHookTokens` (in-flight timers) are intentionally not
part of `backendSnapshot` - a `*time.Timer` can't be serialized. `Restore()` sweeps
every instance left in `Pending:Wait`/`Terminating:Wait` by a restored snapshot and
re-arms a timer for it. Without this, an instance restored mid-wait would be stuck in
that state forever, since nothing would ever call `CompleteLifecycleAction`/hit a
timeout for it. Since bd `gopherstack-9tqg` (see dated section below), the re-armed
hook is the one the instance was *actually* waiting on
(`Instance.LifecycleHookName`, itself part of the persisted group so it survives the
round-trip) rather than always the chain's first hook, so a group restored mid-chain
resumes at the right position instead of restarting or getting stuck.

### Other wire-shape bugs fixed this pass

- **`LaunchInstances` param typo**: the handler read `vals.Get("DesiredCapacity")`.
  The real field name (verified against `LaunchInstancesInput` and its awsquery
  serializer) is `RequestedCapacity`. Any real SDK client always sends
  `RequestedCapacity`, so gopherstack's handler saw an empty string every time and
  fell back to launching exactly 1 instance regardless of what was requested.
- **`LaunchInstances` output shape**: `LaunchInstancesOutput.Instances` is
  `[]InstanceCollection` (grouped by `AvailabilityZone`/`InstanceType`, each carrying
  `InstanceIds []string`), NOT a flat list of per-instance
  `LifecycleState`/`HealthStatus` records (that shape belongs to
  `DescribeAutoScalingGroups`/`DescribeAutoScalingInstances`). The handler was reusing
  the wrong XML type. Fixed with a dedicated `xmlInstanceCollection` type and a
  grouping helper.
- **`LaunchInstances` never indexed its instances**: the backend method appended to
  `g.Instances` but never touched `b.instanceIndex`, so an instance launched this way
  could never be found by `TerminateInstanceInAutoScalingGroup` (which looks up
  purely via `instanceIndex`). Same bug existed in the replacement-instance branch of
  `TerminateInstanceInAutoScalingGroup` itself; both fixed.
- **`MixedInstancesPolicy` silently dropped end-to-end**: the backend input struct and
  the `AutoScalingGroup` model both already had a `MixedInstancesPolicy` field, but
  neither `CreateAutoScalingGroup` nor `UpdateAutoScalingGroup`'s handlers ever parsed
  it from the request, and `xmlAutoScalingGroup` didn't even have a field for it in
  the response projection. A request specifying a mixed-instances policy (spot+
  on-demand mixes, very common via Terraform) was accepted with 200 OK and then
  quietly discarded. Fixed: full parse (launch template + overrides + instances
  distribution) on both Create/Update, full XML projection on Describe.
- **`LifecycleHookSpecificationList` never parsed**: `CreateAutoScalingGroupInput`
  (the real AWS one) lets you register lifecycle hooks atomically with group
  creation - this is exactly the wire shape Terraform's `aws_autoscaling_group`
  `initial_lifecycle_hook` block uses. Completely unhandled before this pass; a group
  created with initial hooks would come up with **no hooks at all**. Fixed, and wired
  into the new launch-hook gating so the group's own initial instances are correctly
  gated by a hook registered at creation time.
- **`TrafficSources` never parsed on `CreateAutoScalingGroup`**: only
  Attach/DetachTrafficSources touched this field; Create silently dropped an inline
  `TrafficSources` list. Fixed (reuses the existing `parseTrafficSources` helper).
- **`PutScheduledUpdateGroupAction`/`BatchPutScheduledUpdateGroupAction` dropped
  `StartTime`/`EndTime`**: the backend model and the `DescribeScheduledActions` XML
  projection both already had `StartTime`/`EndTime` fields, but neither handler parsed
  them from the request - the entire point of a "scheduled" action (when it fires)
  was silently discarded on every call. Fixed (parses AWS query-protocol DateTime,
  i.e. RFC3339/ISO8601).
- **`ExecutePolicy` ignored `StepScaling` entirely**: regardless of `PolicyType`, the
  handler always used the flat `ScalingAdjustment`/`AdjustmentType` fields. Real
  `ExecutePolicy` requires `MetricValue`/`BreachThreshold` for a `StepScaling` policy
  and uses `(MetricValue-BreachThreshold)` to select which `StepAdjustment` interval's
  `ScalingAdjustment` applies. Fixed: parses both fields, validates they're required
  for `StepScaling`, and selects the matching step interval
  (`MetricIntervalLowerBound`/`UpperBound`, nil meaning unbounded, exactly as AWS
  documents it).
- **`ExecutePolicy` scale duplicated (and diverged from) `SetDesiredCapacity`'s
  logic**: it called `adjustInstances` directly, bypassing `SuspendedProcesses`
  checks, scale-in protection (`removeUnprotectedInstances`), and `instanceIndex`
  maintenance that `applyDesiredCapacityChange` already does correctly. Fixed by
  routing through the shared helper.
- **`PutLifecycleHook` dropped `NotificationMetadata`**: parsed nowhere despite being
  a plain top-level request field and already present on both the backend model and
  the XML response type. Fixed.
- **`PutScalingPolicy`/`DescribePolicies` dropped `MetricAggregationType`** (request
  and response) and `DescribePolicies` never returned `MinAdjustmentStep`. Fixed.
- **`GetPredictiveScalingForecast` returned an all-empty response** missing the
  required `UpdateTime` field entirely and shaping `LoadForecast` as `[]string`
  (nowhere close to the real `[]LoadForecast` struct list). A full
  `PredictiveScalingMetricSpecification` projection is out of scope (see gaps), but
  the response now includes a real `UpdateTime` and a real, non-empty, correctly-
  shaped `Timestamps`/`Values` series (naive flat projection at current
  `DesiredCapacity` - explicitly documented as a simplification, not a hidden stub).

### Traps for the next auditor

- `Instance.LifecycleState` values `"Pending:Wait"` and `"Terminating:Wait"` are real
  AWS enum values (`types.LifecycleState`), not placeholders - don't "simplify" them
  back to `InService`/removed without re-reading this Notes section.
- An instance appearing in `g.Instances` with `LifecycleState="Terminating:Wait"`
  still counts toward `len(g.Instances)` but the group's `DesiredCapacity`/`MinSize`
  bookkeeping intentionally has NOT yet been decremented for it - that's deferred to
  `finishTermination`. Don't "fix" an apparent capacity/instance-count mismatch during
  a wait without checking for a `*:Wait` instance first.
- `ExecutePolicy` calling `b.applyDesiredCapacityChange` instead of its own
  `adjustInstances` call is intentional (bug fix, not a regression) - see above.
