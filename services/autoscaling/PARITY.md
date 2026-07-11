---
service: autoscaling
sdk_module: aws-sdk-go-v2/service/autoscaling@v1.64.2
last_audit_commit: d0ebe979
last_audit_date: 2026-07-05
overall: A            # ~900 LOC of genuine production-code fixes this pass (+~670 LOC new tests)
ops:
  CreateAutoScalingGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: MixedInstancesPolicy, LifecycleHookSpecificationList, TrafficSources were parsed as no-ops (silently dropped) - now parsed, validated, and registered atomically with the group; initial instances are gated by any launch hook just registered"}
  DescribeAutoScalingGroups: {wire: ok, errors: ok, state: ok, persist: ok, note: "added MixedInstancesPolicy to the XML projection (was entirely absent from xmlAutoScalingGroup even though the backend model carried it)"}
  UpdateAutoScalingGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: MixedInstancesPolicy was not parsed from the request"}
  DeleteAutoScalingGroup: {wire: ok, errors: ok, state: ok, persist: ok}
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
  CompleteLifecycleAction: {wire: ok, errors: ok, state: ok, persist: ok, note: "CRITICAL fix: previously only stopped a timer that was never created anywhere (dead code) and had zero effect on instance state. Now resolves a real pending lifecycle wait (Pending:Wait/Terminating:Wait -> actual transition), looked up by token OR by (group,hook,instance)"}
  CreateOrUpdateTags: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteLifecycleHook: {wire: ok, errors: ok, state: ok, persist: ok}
  SetDesiredCapacity: {wire: ok, errors: ok, state: ok, persist: ok, note: "scale-out path now gates new instances through an active launch hook"}
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
  DescribePolicies: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: MinAdjustmentStep and MetricAggregationType were never returned"}
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
  ExecutePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: StepScaling policies ignored StepAdjustments/MetricValue/BreachThreshold entirely and always used the flat ScalingAdjustment/AdjustmentType path; now selects the matching StepAdjustment interval and validates the required fields. Also routed through applyDesiredCapacityChange so ExecutePolicy scale-out/in now respects SuspendedProcesses, scale-in protection, instanceIndex bookkeeping, and launch-hook gating like SetDesiredCapacity does (previously it duplicated and diverged from that logic)"}
  ExitStandby: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPredictiveScalingForecast: {wire: ok, errors: ok, state: ok, persist: n/a, note: "fixed: response was missing the required UpdateTime field and returned a wrong-shaped, entirely empty LoadForecast; now returns UpdateTime and a real (though intentionally naive - see Notes) Timestamps/Values series"}
  LaunchInstances: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed 3 bugs: (1) handler read the wrong query param (DesiredCapacity instead of the real RequestedCapacity, so every call silently launched only 1 instance regardless of the requested count); (2) response used the DescribeAutoScalingGroups per-instance shape instead of the real LaunchInstancesOutput InstanceCollection (grouped by AZ/InstanceType with InstanceIds) shape; (3) the backend never added launched instances to instanceIndex, so they could never be found by TerminateInstanceInAutoScalingGroup"}
  PutNotificationConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  PutScalingPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: MetricAggregationType was accepted nowhere on input or output"}
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
gaps:
  - ASG->EC2 real instance provisioning is simulated only (Instance is a fake record, not backed by an ec2 resource) (bd: gopherstack-8sk) - NOT fixed this pass per scope
  - ASG/ECS->ELBv2 target registration is simulated only (TargetGroupARNs/LoadBalancerNames are stored but never actually register targets with elbv2) (bd: gopherstack-18k) - NOT fixed this pass per scope
  - Scheduled actions (Put/BatchPut) now correctly persist StartTime/EndTime/Recurrence, but there is no background scheduler goroutine that actually executes them at the scheduled time/cron - Describe reflects what was requested, but nothing fires it. Filed as gopherstack-6ys for follow-up; deliberately not attempted this pass (a correct cron-parsing+ticker engine is a separate, sizable feature, not a quick wire fix, and getting it wrong risks a new leak class)
  - Terminate-lifecycle-hook gating is wired into TerminateInstanceInAutoScalingGroup only, NOT into the desired-capacity-driven scale-in path (SetDesiredCapacity/UpdateAutoScalingGroup/ExecutePolicy decreasing) - that path still removes instances immediately regardless of a registered terminating hook (bd: gopherstack-9wo)
  - Multiple lifecycle hooks of the *same* transition on one group: this simulation gates on a single (deterministic, lowest-named) hook per transition per group, matching the common case; AWS supports N hooks per transition each independently gating the same instance. Documented simplification, see Notes
  - ABANDON on a launch hook terminates the pending instance but does not attempt an automatic relaunch to restore DesiredCapacity (real AWS does retry); documented simplification, see Notes
  - GetPredictiveScalingForecast returns a real, well-shaped, non-empty forecast, but it is a flat naive projection (current DesiredCapacity repeated hourly), not a statistical model - genuinely out of scope for an emulator; documented simplification, see Notes
  - CreateAutoScalingGroupInput fields not wired up: AvailabilityZoneDistribution, AvailabilityZoneImpairmentPolicy, CapacityReservationSpecification, DeletionProtection, InstanceLifecyclePolicy, InstanceMaintenancePolicy, SkipZonalShiftValidation (all newer/niche SDK additions); not attempted this pass - deferred, no bd id filed yet (low real-world usage relative to MixedInstancesPolicy/LifecycleHookSpecificationList, which WERE fixed)
deferred:
  - InstanceRequirements-based MixedInstancesPolicy overrides (attribute-based instance selection) - only InstanceType-based overrides are parsed/returned
  - PredictiveScalingConfiguration (Put/Describe are not in GetSupportedOperations at all - predictive scaling policy *configuration* management, as opposed to GetPredictiveScalingForecast, was out of scope for this pass; confirmed the SDK op list has no separate op for this, it rides inside PutScalingPolicy's PolicyType=PredictiveScaling with a nested config this handler does not parse)
leaks: {status: clean, note: "go test -race passes. The pendingHookTokens timer machinery (the CRITICAL item flagged in prior sweep notes) was previously 100% dead code - nothing ever created a pendingHookAction, so there was no leak *and* no functionality. This pass makes the timers real (armed on every gated launch/terminate) and verified: Close() stops all of them (unchanged, already correct), DeleteAutoScalingGroup/DeleteLifecycleHook/Purge call cleanupHookTimers (unchanged, already correct), and Restore() now re-arms timers for any instance found in a *:Wait state (added - in-flight timers are never persisted, so without this a restored mid-wait instance would be stuck forever)."}
---

## Notes

Protocol: EC2 Auto Scaling uses the `query` (form-urlencoded request, XML response)
protocol, `Version=2011-01-01`. Verified against the awsquery serializers/deserializers
in `aws-sdk-go-v2/service/autoscaling@v1.64.2` (vendored the module zip into
`/tmp/asg_sdk` for this audit; not committed anywhere).

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

**ABANDON semantics simplification**: for a *launching* hook, ABANDON terminates the
pending instance (matches AWS - a failed launch is torn down) but does not attempt an
automatic relaunch to restore `DesiredCapacity` (AWS does retry via its own internal
worker). For a *terminating* hook, both CONTINUE and ABANDON proceed with the
termination once resolved (AWS lets ABANDON/CONTINUE only affect hook-chaining
metadata for termination, not whether the instance is actually terminated - you cannot
veto a termination via a terminating lifecycle hook).

**Multiple-hooks-per-transition simplification**: `findHookForTransition` returns a
single, deterministically-chosen (lowest hook name) hook per transition per group.
Real AWS supports registering several hooks on the same transition, each
independently gating the instance in sequence. The overwhelming majority of
real-world ASG configs register at most one hook per transition; documented here so
the next auditor doesn't mistake this for an oversight.

**Restore/persistence**: `pendingHookTokens` (in-flight timers) are intentionally not
part of `backendSnapshot` - a `*time.Timer` can't be serialized, and this predates
this pass's changes. What's new this pass: `Restore()` now sweeps every instance left
in `Pending:Wait`/`Terminating:Wait` by a restored snapshot and re-arms a timer for it
(using the still-registered hook's HeartbeatTimeout/DefaultResult, or the AWS default
if the hook itself is gone). Without this, an instance restored mid-wait would be
stuck in that state forever, since nothing would ever call
`CompleteLifecycleAction`/hit a timeout for it.

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
