---
service: scheduler
sdk_module: aws-sdk-go-v2/service/scheduler@v1.17.20   # version audited against
last_audit_commit: 174b1f53                            # HEAD when this audit started
last_audit_date: 2026-07-12
overall: A            # genuine wire-breaking bugs found and fixed (see Notes)
ops:
  CreateSchedule:      {wire: ok, errors: ok, state: ok, persist: ok}
  GetSchedule:         {wire: ok, errors: ok, state: ok, persist: ok, note: "extra non-canonical Tags field, harmless"}
  UpdateSchedule:      {wire: ok, errors: ok, state: fixed, persist: ok, note: "omitted State no longer blanks out the schedule"}
  DeleteSchedule:      {wire: ok, errors: ok, state: ok, persist: ok}
  ListSchedules:       {wire: ok, errors: ok, state: ok, persist: ok}
  CreateScheduleGroup: {wire: fixed, errors: ok, state: ok, persist: ok, note: "Tags was map[string]string, real wire shape is []{Key,Value}"}
  GetScheduleGroup:    {wire: ok, errors: ok, state: ok, persist: ok, note: "extra non-canonical Tags field, harmless"}
  DeleteScheduleGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  ListScheduleGroups:  {wire: ok, errors: ok, state: ok, persist: ok, note: "extra non-canonical Tags field, harmless"}
  TagResource:         {wire: fixed, errors: ok, state: ok, persist: ok, note: "Tags was map[string]string, real wire shape is []{Key,Value}"}
  UntagResource:       {wire: fixed, errors: ok, state: ok, persist: ok, note: "REST TagKeys query param was lowercase+comma-joined, real wire is repeated ?TagKeys=a&TagKeys=b"}
  ListTagsForResource: {wire: fixed, errors: ok, state: ok, persist: ok, note: "Tags output was map[string]string, real wire shape is []{Key,Value}"}
families:
  RouteMatcher: {status: ok, note: "verified every op's REST method+path prefix against aws-sdk-go-v2 serializers.go (SplitURI/request.Method) op by op -- CreateSchedule/CreateScheduleGroup POST, Get* GET, Delete* DELETE, UpdateSchedule PUT /schedules/{Name}, List* GET (no {Name}), tags POST/GET/DELETE on /tags/{ResourceArn}. All match exactly; no drift found."}
  cross-service target delivery: {status: ok, note: "cli.go's wireSchedulerRunner wires ALL 8 Runner invoker interfaces (Lambda, SQS, SNS, StepFunctions, EventBridge, Kinesis, SageMaker, ECS) via wireSchedulerMessaging/wireSchedulerWorkflow/wireSchedulerCompute, called from the main registration path. Verified NOT a gap despite the task brief's hint to check -- only Lambda+StepFunctions were mentioned but all 8 are actually wired."}
gaps:
  - CreateSchedule/CreateScheduleGroup/UpdateSchedule accept ClientToken on the wire but never use it for idempotency; a lost-response retry with the same ClientToken+params returns ConflictException/ResourceNotFoundException instead of AWS's idempotent-replay behavior (return the original result). Name-based uniqueness still prevents true duplicate resources, so this is a narrow edge case, not a data-corruption risk. No idempotency-token pkg exists in pkgs/ to build on; left as a gap rather than a bespoke one-off implementation.
  - GetSchedule/GetScheduleGroup/ListScheduleGroups responses include a "Tags" field that does not exist in the real AWS API shapes (GetScheduleOutput, GetScheduleGroupOutput, ScheduleGroupSummary, ScheduleSummary all lack Tags -- tags are only ever fetched via ListTagsForResource). Harmless (ignored by real SDK deserializers on unknown fields) but non-canonical; left in place to avoid test churn for a field real clients never read.
  - ScheduleSummary.Target in ListSchedules includes RoleArn; the real TargetSummary type only has Arn. Same harmless-extra-field situation as above.
deferred: []
leaks: {status: clean, note: "leak_main_test.go (testleak.VerifyTestMain) passes under -race; no new goroutines/locks introduced by this pass's fixes."}
---

## Notes

- **Protocol**: restjson1. Real clients never send `X-Amz-Target` (that header path in
  handler.go is dead code for genuine AWS SDK traffic, kept for internal
  test/dispatch convenience) -- verified against `aws-sdk-go-v2/service/scheduler`'s
  serializers.go: none of the 12 ops set that header.
- **Wire-breaking bug (the big one): resource tags are a list, not a map.**
  `CreateScheduleGroup.Tags`, `TagResource.Tags`, and `ListTagsForResource.Tags` are
  ALL `[]types.Tag{Key,Value}` on the wire (see
  `awsRestjson1_(de)serializeDocumentTagList` in the SDK), the same shape as most
  other AWS services' `Tag{Key,Value}` lists -- NOT the `map[string]string`
  ("TagMap") shape scheduler happens to use internally for its *ECS task* tags
  (`EcsParameters.Tags` is `[]map[string]string` on the wire, a different,
  unrelated field). Before this fix, gopherstack decoded/encoded these three ops'
  `Tags` as a plain JSON object (`{"key":"value"}`); every real AWS SDK sending
  `Tags: [{"Key":"key","Value":"value"}]` would fail JSON-unmarshal into
  `map[string]string` (type mismatch on an array), and `ListTagsForResource`
  responses would fail to deserialize on the client for the same reason in
  reverse. Fixed via a `resourceTag{Key,Value}` wire type + `tagsFromWire`/
  `tagsToWire` helpers in handler.go; the backend's `map[string]string` internal
  representation is unchanged (it's an internal choice, not on the wire).
- **UntagResource TagKeys query param bug.** The REST binding sends `TagKeys` as a
  **repeated** query parameter (`?TagKeys=a&TagKeys=b`, via
  `encoder.AddQuery("TagKeys", ...)` per key), with that exact capitalization.
  gopherstack's REST-path enrichment read `q.Get("tagKeys")` (wrong case -- would
  never match a real request) and then `strings.Split(..., ",")` (wrong shape --
  real clients never comma-join). Fixed by switching the REST enrichment helpers
  from a `Get(string) string`-only interface to `url.Values` so `q["TagKeys"]`
  (all values) is available, and matching the real capitalization.
- **UpdateSchedule State-omission bug.** `UpdateScheduleInput.State` is optional;
  the real document serializer omits the JSON key entirely when unset
  (`if len(v.State) > 0`), so a real client can update a schedule's expression/
  target/etc. without touching whether it's ENABLED or DISABLED. gopherstack
  unconditionally wrote the (possibly empty) incoming state onto the schedule,
  silently corrupting it to `State: ""` -- which is neither ENABLED nor DISABLED,
  so `Runner.checkAndFireSchedules`'s `s.State != "ENABLED"` gate would silently
  stop firing the schedule forever. Fixed: only overwrite `State` when non-empty.
- **ActionAfterCompletion had no enum validation.** Real values are only `NONE` and
  `DELETE` (`types.ActionAfterCompletion`); gopherstack accepted any string,
  silently no-op'd on invalid values in the runner
  (`handleActionAfterCompletion`'s switch has no default case). Added
  `validateActionAfterCompletion`, called from both `handleCreateSchedule` and
  `handleUpdateSchedule`.
- **RouteMatcher / REST path-to-op mapping verified op-by-op** against
  `aws-sdk-go-v2/service/scheduler`'s `serializers.go` (`httpbinding.SplitURI(...)`
  + `request.Method = ...` per op). All 12 ops match exactly:
  `POST /schedules/{Name}` (CreateSchedule), `GET/DELETE/PUT /schedules/{Name}`
  (Get/Delete/UpdateSchedule), `GET /schedules` (ListSchedules),
  `POST/GET/DELETE /schedule-groups/{Name}` (Create/Get/DeleteScheduleGroup),
  `GET /schedule-groups` (ListScheduleGroups),
  `GET/POST/DELETE /tags/{ResourceArn}` (ListTagsForResource/TagResource/
  UntagResource). No route-matcher drift found this pass.
- **Timestamps**: CreationDate/LastModificationDate/StartDate/EndDate are epoch
  seconds as JSON numbers on both sides (`smithytime.FormatEpochSeconds`/
  `ParseEpochSeconds` in the SDK; gopherstack emits `float64` Unix seconds) --
  correct, no `awstime.Epoch` gap found (this service doesn't use the pkg, but its
  hand-rolled epoch float64 conversion matches the wire).
- **Error shapes**: `ResourceNotFoundException`/`ConflictException`/
  `ValidationException` via `service.JSONErrorResponse{Type: "__type", ...}` --
  matches `restjson.GetErrorInfo`'s `Code`/`__type`/`Message` field lookup.
  HTTP status codes used (404/409/400) are conventional REST mappings; the Go SDK
  itself doesn't gate on HTTP status for exception typing (only on the error-code
  string), so these aren't wire-load-bearing but are kept for other/non-Go SDKs.
- **cli.go cross-service wiring verified complete, not a gap.** The task brief
  flagged "if you find cross-service delivery gaps (schedule -> target execution
  for non-Lambda targets), REPORT them" and mentioned only the Lambda adapter by
  name. Checked cli.go's `wireSchedulerRunner` (called from the main service-wiring
  path): it wires all 8 `Runner` invoker interfaces the scheduler service defines
  (Lambda, SQS, SNS, StepFunctions, EventBridge, Kinesis, SageMaker, ECS) via
  `wireSchedulerMessaging`/`wireSchedulerWorkflow`/`wireSchedulerCompute`. No
  follow-up needed here.
- **"Looks wrong but is correct" traps for the next auditor**:
  - `getScheduleOutput`/`getScheduleGroupOutput`/`scheduleGroupSummary` carrying a
    `Tags map[string]string` field is *not* the same bug as the TagResource/
    ListTagsForResource/CreateScheduleGroup one above -- those ops genuinely don't
    have a `Tags` field in the real API at all, so whatever shape is used there is
    inert (ignored) rather than wire-breaking. Don't "fix" it to `[]resourceTag`
    without also checking whether it's worth the churn (see gaps).
  - `EcsParameters.Tags []scheduleTargetEcsTag` (`{Key,Value}` list) is unrelated
    to the resource-tag bug and was already correct before this pass --
    `TestParity_EcsParametersTaskTagsRoundtrip` in parity_b_test.go covers it.
  - The `X-Amz-Target`/`AWSScheduler.<Op>` JSON-1.1 dispatch path in handler.go is
    dead code for real AWS SDK traffic (restjson1 has no such header) but is kept
    intentionally for internal test convenience (`doSchedulerRequest` in
    handler_test.go uses it) -- don't remove it as "dead code."
