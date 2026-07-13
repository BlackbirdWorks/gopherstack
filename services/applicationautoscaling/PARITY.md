service: applicationautoscaling
sdk_module: aws-sdk-go-v2/service/applicationautoscaling@v1.41.12
last_audit_commit: a7e2082d
last_audit_date: 2026-07-13
overall: A            # real, wire-breaking bugs found and fixed
ops:
  RegisterScalableTarget: {wire: ok, errors: ok, state: ok, persist: ok, note: "upsert confirmed; MinCapacity/MaxCapacity/RoleARN/Tags/SuspendedState all persisted and mutated on update"}
  DeregisterScalableTarget: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascades delete to scaling policies + scheduled actions for the same (ns,resourceId,dimension), matching real AWS"}
  DescribeScalableTargets: {wire: ok, errors: ok, state: ok, persist: ok, note: "real NextToken pagination via generic paginate()"}
  PutScalingPolicy: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED: default PolicyType was TargetTrackingScaling, real AWS/Terraform default is StepScaling. FIXED: PolicyARN used '/' before policyName segment, real ARN uses ':' (scalingPolicy:{uuid}:resource/{ns}/{id}:policyName/{name})"}
  DeleteScalingPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeScalingPolicies: {wire: ok, errors: ok, state: ok, persist: ok, note: "PolicyNames/PolicyARNs filters, pagination all real"}
  DescribeScalingActivities: {wire: ok, errors: ok, state: ok, persist: n/a, note: "scalingActivities intentionally ephemeral (never persisted pre- or post- this audit, matches prior design note in persistence.go); most-recent-first via slices.Backward; real pagination"}
  PutScheduledAction: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED: StartTime/EndTime were typed as JSON strings parsed via time.RFC3339 -- real awsjson1.1 wire format for these fields is a JSON NUMBER of Unix epoch seconds (confirmed against serializers.go: object.Key(\"EndTime\").Double(smithytime.FormatEpochSeconds(...))). A real aws-sdk-go-v2 client sending a scheduled action with StartTime/EndTime would have gotten a 400 from gopherstack every time. FIXED: ScheduledActionARN used '/' before scheduledActionName segment, real ARN uses ':' (scheduledAction:{uuid}:resource/{ns}/{id}:scheduledActionName/{name})"}
  DeleteScheduledAction: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeScheduledActions: {wire: ok, errors: ok, state: ok, persist: ok, note: "StartTime/EndTime/CreationTime/LastModifiedTime output already correctly used epoch seconds via epochSecondsPtr -- only the PutScheduledAction *input* parsing was broken"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPredictiveScalingForecast: {wire: fixed, errors: ok, state: ok, persist: n/a, note: "FIXED: entire op used RFC3339 strings for StartTime/EndTime (request) and CapacityForecast.Timestamps/LoadForecast[].Timestamps/UpdateTime (response) -- all must be epoch-seconds JSON numbers per awsjson1.1. Any real SDK client would fail to deserialize the response (deserializer expects json.Number, got string) and fail to have its request StartTime/EndTime accepted. Forecast data itself remains a flat synthetic 10.0-per-hour simulation (deferred, see gaps)."}
families:
  tagging: {status: ok, note: "TagResource/ListTagsForResource/UntagResource operate on scalable-target ARNs only, matching real AWS (Application Auto Scaling only supports tagging scalable targets)"}
gaps:
  - DescribeScalingActivities ignores the IncludeNotScaledActivities request flag; gopherstack's mock backend never generates "not scaled" activities (no real metric evaluation loop exists to decide not-to-scale), so there is nothing to surface even if honored. Documented as deferred, not fixed this pass.
  - GetPredictiveScalingForecast returns a flat synthetic capacity/load curve (constant 10.0 per hourly point) rather than any real forecasting simulation. This was true before this audit and is unchanged; only the wire format (epoch vs RFC3339) was fixed.
  - PolicyType/ScalableDimension/ServiceNamespace enum values are accepted permissively (no allowlist validation) rather than validated against the real AWS enum lists. Consistent with this codebase's general emulator philosophy of not over-validating; not treated as a bug.
deferred:
  - CloudWatch alarm auto-creation for TargetTrackingScaling/StepScaling policies (real AWS creates backing CloudWatch alarms; gopherstack's scalingPolicySummary.Alarms field exists on the wire type but is never populated) -- would require cross-service CloudWatch integration, out of scope for this pass.
leaks: {status: clean, note: "no goroutines/janitors in this service; all state is synchronous map/slice access under lockmetrics.RWMutex"}
---

## Notes

- Protocol: awsjson1.1, single POST endpoint, `X-Amz-Target: AnyScaleFrontendService.<Op>`.
  Verified the `AnyScaleFrontendService.` prefix against the real SDK's `serializers.go`
  (every op serializer sets exactly this header value) -- matcher in handler.go is correct.

- **Epoch vs RFC3339 timestamps (major bug class this pass)**: awsjson1.1 represents ALL
  timestamps as JSON numbers (Unix epoch seconds, optionally fractional), never as ISO8601
  strings. This was already handled correctly for CreationTime/LastModifiedTime/StartTime/
  EndTime on every Describe* response via the existing `epochSecondsPtr` helper -- the bugs
  were isolated to two ops that had their own hand-rolled RFC3339 string handling instead of
  reusing that pattern: **PutScheduledAction**'s StartTime/EndTime *request* fields, and
  **GetPredictiveScalingForecast** end-to-end (both its request StartTime/EndTime and its
  entire response: CapacityForecast.Timestamps, LoadForecast[].Timestamps, UpdateTime).
  `epochSecondsPtr` now delegates to `pkgs/awstime.Epoch` instead of a hand-rolled
  `float64(t.Unix())` (same output, but reuses the shared helper and gains fractional-second
  precision, matching the codebase-wide convention seen in comprehend/personalize/etc).
  A new `parseEpochSeconds(*float64) *time.Time` helper in handler.go is the input-side
  counterpart, used by both fixed ops.

- **ARN colon-vs-slash quirk**: real Application Auto Scaling ARNs for scaling policies and
  scheduled actions place the trailing `policyName/{name}` or `scheduledActionName/{name}`
  segment after a **colon**, not a slash, e.g.:
  `arn:aws:autoscaling:us-east-2:123456789012:scalingPolicy:{uuid}:resource/ecs/service/my-cluster/my-service:policyName/MyPolicy`
  `arn:aws:autoscaling:us-west-2:123456789012:scheduledAction:{uuid}:resource/dynamodb/table/my-table:scheduledActionName/my-action`
  gopherstack had a slash there for both ARN kinds. Fixed in `backend.go`
  (`PutScalingPolicy`/`PutScheduledAction` ARN construction). Confirmed via AWS
  documentation ARN examples (no vendored SDK example available, since ARN format isn't
  encoded in the Go SDK -- it's purely a server-side string convention).

- **PolicyType default quirk**: real AWS/Terraform (`aws_appautoscaling_policy`) defaults an
  omitted `PolicyType` to `StepScaling`, not `TargetTrackingScaling` -- this is a
  long-standing, documented AWS API default (StepScaling was the original/only policy type
  historically). gopherstack had it backwards. Fixed in `backend.go`'s `PutScalingPolicy`.
  This matters directly for Terraform users: `aws_appautoscaling_policy` resources that omit
  `policy_type` (common for ECS step-scaling configs) would previously have silently gotten
  the wrong policy type back from gopherstack.

- Upsert semantics verified for both `RegisterScalableTarget` (update path in
  `updateExistingTarget`) and `PutScalingPolicy`/`PutScheduledAction` (both check their
  respective secondary index and mutate in place rather than erroring/duplicating on a
  second call with the same key). `DeregisterScalableTarget`'s cascade-delete of policies
  and scheduled actions for the same resource was already correct and matches real AWS
  behavior ("If a scalable target is deregistered ... any scaling policies that were
  specified for the scalable target are deleted").

- `ErrAlreadyExists` is declared in backend.go and referenced in handler.go's error switch,
  but no backend method ever returns it (every Put*/Register* op is upsert-only, by design,
  per real AWS semantics -- there is no create-only path that could conflict). Not dead code
  in the sense of being unreachable-and-harmful; left as-is since removing the sentinel
  would touch the error-handling switch for no behavioral gain.
