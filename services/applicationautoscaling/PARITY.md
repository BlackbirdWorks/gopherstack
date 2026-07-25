service: applicationautoscaling
sdk_module: aws-sdk-go-v2/service/applicationautoscaling@v1.41.12
last_audit_commit: bf3aabe3d
last_audit_date: 2026-07-24
overall: A            # real, wire-breaking bugs found and fixed
ops:
  RegisterScalableTarget: {wire: ok, errors: fixed, state: ok, persist: ok, note: "upsert confirmed; MinCapacity/MaxCapacity/RoleARN/Tags/SuspendedState all persisted and mutated on update. FIXED: over-tag-limit now reports LimitExceededException (RegisterScalableTarget's modeled error set has no TooManyTagsException, confirmed against the vendored SDK's deserializeOpErrorRegisterScalableTarget), not ValidationException."}
  DeregisterScalableTarget: {wire: ok, errors: fixed, state: ok, persist: ok, note: "cascades delete to scaling policies + scheduled actions for the same (ns,resourceId,dimension), matching real AWS. FIXED: ObjectNotFoundException HTTP status was 404, now 400 (see notes)."}
  DescribeScalableTargets: {wire: fixed, errors: fixed, state: ok, persist: ok, note: "FIXED: NextToken is now an opaque base64 cursor (was the raw sort key) and a malformed token now returns InvalidNextTokenException/400 (DescribeScalableTargets' modeled error set includes it). Added PredictedCapacity field (always omitted -- see notes)."}
  PutScalingPolicy: {wire: fixed, errors: fixed, state: fixed, persist: ok, note: "FIXED (prior pass): default PolicyType was TargetTrackingScaling, real default is StepScaling; PolicyARN colon-vs-slash. FIXED this pass: (1) now requires the scalable target to already be registered, raising ObjectNotFoundException otherwise -- PutScalingPolicy's modeled error set includes it and its doc text names 'any operation that depends on the existence of a scalable target'; a client could previously PutScalingPolicy against a namespace/resourceId/dimension that was never registered, which real AWS rejects. (2) PredictiveScalingPolicyConfiguration was accepted by the real API but silently dropped -- now captured, persisted, and echoed by DescribeScalingPolicies. (3) Alarms (CloudWatch alarm references) is a real field on both PutScalingPolicy's and DescribeScalingPolicies' response shapes but was never populated (declared on the wire struct, always empty) -- now synthesizes stable Alarm entries for TargetTrackingScaling (2 alarms, or 1 if DisableScaleIn) and StepScaling (1 alarm); PredictiveScaling correctly gets none. (4) enforces the real, documented AWS quotas: 50 scaling policies/scalable target and 20 step adjustments/step-scaling-policy, raising LimitExceededException."}
  DeleteScalingPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeScalingPolicies: {wire: fixed, errors: fixed, state: ok, persist: ok, note: "FIXED: deleted the invented PolicyARNs filter field/behavior -- confirmed against DescribeScalingPoliciesInput/its serializer in the vendored SDK, real AWS has no such filter (only PolicyNames/ResourceId/ScalableDimension/ServiceNamespace). FIXED: NextToken is now opaque base64 with InvalidNextTokenException on malformed input. FIXED: Alarms/PredictiveScalingPolicyConfiguration now populated (see PutScalingPolicy)."}
  DescribeScalingActivities: {wire: fixed, errors: fixed, state: ok, persist: n/a, note: "scalingActivities intentionally ephemeral; most-recent-first via slices.Backward; NextToken now opaque base64 with InvalidNextTokenException on malformed input. Added Details/NotScaledReasons wire fields (always empty/omitted -- see gaps, IncludeNotScaledActivities is accepted but vacuous)."}
  PutScheduledAction: {wire: fixed, errors: fixed, state: fixed, persist: ok, note: "FIXED (prior pass): StartTime/EndTime epoch-seconds; ARN colon-vs-slash. FIXED this pass: now requires the scalable target to already be registered (ObjectNotFoundException), same rationale as PutScalingPolicy. Enforces the real, documented, non-adjustable AWS quota of 200 scheduled actions/scalable target, raising LimitExceededException."}
  DeleteScheduledAction: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeScheduledActions: {wire: fixed, errors: fixed, state: ok, persist: ok, note: "StartTime/EndTime/CreationTime/LastModifiedTime output already correctly used epoch seconds. FIXED: NextToken now opaque base64 with InvalidNextTokenException on malformed input."}
  ListTagsForResource: {wire: ok, errors: fixed, state: ok, persist: ok, note: "FIXED: not-found now reports ResourceNotFoundException (with ResourceName), not ObjectNotFoundException -- ListTagsForResource/TagResource/UntagResource are modeled with ResourceNotFoundException only, confirmed against each op's deserializeOpError* switch in the vendored SDK."}
  TagResource: {wire: ok, errors: fixed, state: ok, persist: ok, note: "FIXED: not-found -> ResourceNotFoundException (see ListTagsForResource). FIXED: over-tag-limit now reports TooManyTagsException (with ResourceName), not ValidationException -- TagResource is the one op actually modeled with TooManyTagsException."}
  UntagResource: {wire: ok, errors: fixed, state: ok, persist: ok, note: "FIXED: not-found -> ResourceNotFoundException (see ListTagsForResource)."}
  GetPredictiveScalingForecast: {wire: ok, errors: fixed, state: ok, persist: n/a, note: "FIXED (prior pass): epoch-seconds wire format end to end. FIXED this pass: unknown-policy error switched from ObjectNotFoundException to ValidationException -- GetPredictiveScalingForecast's modeled error set is {InternalServiceException, ValidationException} only (confirmed against awsAwsjson11_deserializeOpErrorGetPredictiveScalingForecast in the vendored SDK); a real aws-sdk-go-v2 client's typed-error matching on ObjectNotFoundException would never have fired here. Forecast data itself remains a flat synthetic 10.0-per-hour simulation (deferred, see gaps)."}
families:
  tagging: {status: ok, note: "TagResource/ListTagsForResource/UntagResource operate on scalable-target ARNs only, matching real AWS (Application Auto Scaling only supports tagging scalable targets)"}
  error_types: {status: fixed, note: "Every modeled AWS exception (ConcurrentUpdateException/FailedResourceAccessException/InternalServiceException/InvalidNextTokenException/LimitExceededException/ObjectNotFoundException/ResourceNotFoundException/TooManyTagsException/ValidationException) now has a distinct sentinel in errors.go and a correct HTTP status in handler.go's handleError, matching each type's ErrorFault() classification in the vendored SDK's types/errors.go: FaultServer (ConcurrentUpdateException, InternalServiceException) -> HTTP 500; FaultClient (everything else) -> HTTP 400. Previously ObjectNotFoundException incorrectly returned 404, ValidationException(ErrAlreadyExists) incorrectly returned 409, and TooManyTagsException/LimitExceededException/InvalidNextTokenException/ResourceNotFoundException/ConcurrentUpdateException/FailedResourceAccessException did not exist as distinct types at all (their scenarios either fell through to a generic ValidationException/404 or were simply unreachable)."}
gaps:
  - DescribeScalingActivities accepts IncludeNotScaledActivities (now threaded into the backend filter, and the response shape now has NotScaledReasons/Details fields) but it remains observably vacuous: gopherstack's mock backend never generates "not scaled" activities (no real metric evaluation loop exists to decide not-to-scale), so there is nothing to surface regardless of the flag's value. Verified vacuous, not a fabricated stub -- generating fake not-scaled events would be worse than reporting none.
  - GetPredictiveScalingForecast returns a flat synthetic capacity/load curve (constant 10.0 per hourly point) rather than any real forecasting simulation. Unchanged this pass; only its error-type wire shape was fixed.
  - PolicyType/ScalableDimension/ServiceNamespace enum values are accepted permissively (no allowlist validation) rather than validated against the real AWS enum lists. Consistent with this codebase's general emulator philosophy of not over-validating; not treated as a bug.
  - The "scalable targets per resource type" AWS quota (5,000 for DynamoDB, 3,000 for ECS, 1,500 for Keyspaces, 500 for other resource types, all adjustable) is not enforced. Only the two non-adjustable, resource-type-independent quotas were implemented this pass (50 scaling policies/target, 200 scheduled actions/target) plus the adjustable-but-defaulted 20 step-adjustments/policy quota. Mapping every real AWS resource type to its specific quota bucket for a soft/adjustable, rarely-hit limit was judged out of scope for this pass.
deferred:
  - Full CloudWatch cross-service integration for scaling-policy alarms: real AWS creates genuine backing CloudWatch alarms (visible via cloudwatch:DescribeAlarms) and can fail PutScalingPolicy with FailedResourceAccessException if the scalable target's RoleARN lacks CloudWatch permissions. This pass synthesizes stable, correctly-shaped Alarm entries (name + ARN) on the Application Auto Scaling side so PutScalingPolicy/DescribeScalingPolicies' Alarms field is populated like real AWS instead of always empty, but there is no actual CloudWatch alarm resource created in the cloudwatch service. Real cross-service alarm creation/verification remains out of scope.
  - ConcurrentUpdateException: sentinel (ErrConcurrentUpdate) and correct HTTP 500 status now exist in errors.go/handler.go, but no backend method returns it. gopherstack's backend serializes every operation behind one coarse lockmetrics.RWMutex, so there is no window in which two updates to the same resource can race -- the real AWS scenario (a resource that already has a pending update) has no analogue in a synchronous single-process emulator. Wiring the type without a fabricated trigger condition is the honest option; inventing an artificial pending-update state machine just to exercise this exception would be scope creep unrelated to real client-observable behavior.
  - FailedResourceAccessException: sentinel (ErrFailedResourceAccess) and correct HTTP 400 status now exist, but unreachable -- see the CloudWatch cross-service deferred item above. Requires real cross-service CloudWatch alarm/permission checking, out of scope.
leaks: {status: clean, note: "no goroutines/janitors in this service; all state is synchronous map/slice access under lockmetrics.RWMutex"}
---

## Notes

- Protocol: awsjson1.1, single POST endpoint, `X-Amz-Target: AnyScaleFrontendService.<Op>`.
  Verified the `AnyScaleFrontendService.` prefix against the real SDK's `serializers.go`
  (every op serializer sets exactly this header value) -- matcher in handler.go is correct.

- **HTTP status is NOT resource semantics, it's ErrorFault() (this pass's main bug class)**:
  awsjson1.1/json-protocol services do not use HTTP 404 for "not found" the way REST
  protocols do. Every modeled exception's HTTP status is determined entirely by whether the
  AWS Smithy model classifies it as a client fault (400) or a server fault (500) --
  `ErrorFault()` on each type in `aws-sdk-go-v2/service/applicationautoscaling/types/errors.go`.
  `ObjectNotFoundException`/`ResourceNotFoundException` are BOTH client faults (400) despite
  signaling "not found"; only `ConcurrentUpdateException`/`InternalServiceException` are
  server faults (500). gopherstack previously returned HTTP 404 for ObjectNotFoundException
  and HTTP 409 for the (unreachable) ValidationException/ErrAlreadyExists case -- both wrong.
  A real aws-sdk-go-v2 client doesn't care about the HTTP status for *type resolution* (it
  reads `__type`/`X-Amzn-ErrorType` from the body/header), but does care for retry
  classification (5xx is retryable, 4xx generally is not) -- so the old 404/409 codes could
  cause a real SDK to retry a request that would never succeed, or vice versa.

- **Per-operation modeled error sets are NOT interchangeable (second major bug class)**:
  field-diffing each op's `awsAwsjson11_deserializeOpError<Op>` switch in the vendored SDK's
  `deserializers.go` revealed gopherstack had reused one exception type across ops that model
  different ones for the same "not found" or "too many X" concept:
  - `ListTagsForResource`/`TagResource`/`UntagResource` model **ResourceNotFoundException**,
    never ObjectNotFoundException.
  - `TagResource` models **TooManyTagsException** for its own over-limit case; but
    `RegisterScalableTarget` (which also accepts a `Tags` parameter) has NO
    TooManyTagsException in its modeled set -- its over-limit case must be
    **LimitExceededException** instead.
  - `GetPredictiveScalingForecast`'s modeled set is `{InternalServiceException,
    ValidationException}` only -- no ObjectNotFoundException, unlike every other
    policy/target-keyed op.
  - `PutScalingPolicy`/`PutScheduledAction` (but not `DescribeScalingPolicies` etc.) DO model
    ObjectNotFoundException, because real AWS requires the scalable target to already exist
    for these two ops specifically (`ObjectNotFoundException`'s own doc text: "For any
    operation that depends on the existence of a scalable target, this exception is thrown
    if the scalable target ... does not exist").
  `TooManyTagsException`/`ResourceNotFoundException` both carry a `ResourceName` field on the
  wire (confirmed via their `deserializeDocument*` functions) -- handler.go's
  `marshalResourceError` now emits it; the plain `marshalError` helper is used for exception
  types with no such field.

- **PutScalingPolicy/PutScheduledAction now enforce pre-registration**: previously either op
  could be called against a `(serviceNamespace, resourceId, scalableDimension)` that was
  never passed to `RegisterScalableTarget`, silently creating an orphaned policy/action --
  real AWS rejects this with ObjectNotFoundException, a well-known Terraform gotcha
  (`aws_appautoscaling_policy`/`aws_appautoscaling_scheduled_action` need
  `depends_on = [aws_appautoscaling_target...]` when not otherwise implied). Fixed in
  `scaling_policies.go`/`scheduled_actions.go` via `scalableTargetExists` (added to
  `scalable_targets.go`). This required adding a `RegisterScalableTarget` precondition to
  every existing test that exercises PutScalingPolicy/PutScheduledAction directly (dozens of
  call sites across `handler_scaling_policies_test.go`, `handler_scheduled_actions_test.go`,
  `handler_forecast_test.go`, `pagination_test.go`, `persistence_test.go`) -- see
  `seedTargetNS` in `handler_test.go`.

- **Real, documented AWS quotas now enforced** (source: "Quotas for Application Auto Scaling"
  AWS documentation page): 50 scaling policies per scalable target (not adjustable), 200
  scheduled actions per scalable target (not adjustable), 20 step adjustments per step
  scaling policy (adjustable, but 20 is the real default and gopherstack has no
  per-account-quota-override concept). All three raise `LimitExceededException`. The
  per-resource-type "scalable targets per account" quota was NOT implemented (see gaps --
  it's adjustable, resource-type-dependent, and much less likely to be hit in practice/tests).

- **NextToken is now opaque** (`encodePageToken`/`decodePageToken` in `store.go`, base64 of
  the same sort-key cursor `paginate()` already used): previously the "opaque" NextToken was
  literally the raw sort key (a resource ID, ARN, or composite string) -- syntactically any
  string was a "valid" cursor, so there was no way to detect a malformed NextToken and
  gopherstack could never return InvalidNextTokenException, which all four Describe* ops
  model. `paginate()` now returns `(page, nextToken, error)`; a token that fails to
  base64-decode returns ErrInvalidNextToken. This does not change any client-visible
  behavior for well-behaved clients (they only ever pass back a token gopherstack itself
  issued), only for a client that fabricates or corrupts a token.

- **Alarms/PredictiveScalingPolicyConfiguration were real fields never wired**:
  `scalingPolicySummary.Alarms` existed on the wire struct since before this pass but was
  never populated (confirmed empty in both PutScalingPolicy's response, which didn't even
  declare the field, and DescribeScalingPolicies'). `PredictiveScalingPolicyConfiguration`
  wasn't in gopherstack's input/output structs AT ALL, despite being a real field on both
  `PutScalingPolicyInput` and `types.ScalingPolicy` in the vendored SDK -- a client creating
  a PredictiveScaling policy with a real config had that config silently discarded. Both
  fixed: see `synthesizeAlarms` in `scaling_policies.go` and the new
  `ScalingPolicy.PredictiveScalingConfig` field in `models.go`.

- **`ScalableTarget.PredictedCapacity`/`ScalingActivity.Details`/`ScalingActivity.NotScaledReasons`**:
  three more real wire fields gopherstack's structs didn't declare at all. Added for field
  presence parity; all three are honestly left nil/empty (never fabricated) since nothing in
  gopherstack computes them -- see each field's doc comment in `models.go`.

- **Deleted invented field**: `DescribeScalingPoliciesFilter.PolicyARNs` /
  `describeScalingPoliciesInput.PolicyARNs` did not exist on the real
  `DescribeScalingPoliciesInput` (confirmed against both the Go SDK struct and its
  serializer in `serializers.go` -- the only filters are `PolicyNames`/`ResourceId`/
  `ScalableDimension`/`ServiceNamespace`). Removed from `scaling_policies.go`,
  `handler_scaling_policies.go`, and the now-deleted
  `TestHandler_DescribeScalingPolicies_PolicyARNsFilter` test.

- **ARN colon-vs-slash quirk** (prior pass, unchanged): real Application Auto Scaling ARNs
  for scaling policies and scheduled actions place the trailing `policyName/{name}` or
  `scheduledActionName/{name}` segment after a **colon**, not a slash, e.g.:
  `arn:aws:autoscaling:us-east-2:123456789012:scalingPolicy:{uuid}:resource/ecs/service/my-cluster/my-service:policyName/MyPolicy`

- **PolicyType default quirk** (prior pass, unchanged): real AWS/Terraform
  (`aws_appautoscaling_policy`) defaults an omitted `PolicyType` to `StepScaling`, not
  `TargetTrackingScaling`.

- Upsert semantics verified for `RegisterScalableTarget`, `PutScalingPolicy`, and
  `PutScheduledAction` (all check their secondary index and mutate in place on a second call
  with the same key). `DeregisterScalableTarget`'s cascade-delete of policies and scheduled
  actions for the same resource matches real AWS ("If a scalable target is deregistered ...
  any scaling policies that were specified for the scalable target are deleted").

- `ErrAlreadyExists` remains declared and wired into handleError (HTTP 400, fixed from 409)
  but no backend method returns it -- every Put*/Register* op is upsert-only by design,
  matching real AWS semantics (there is no create-only path that could conflict).
