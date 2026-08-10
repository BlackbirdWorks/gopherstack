---
service: scheduler
sdk_module: aws-sdk-go-v2/service/scheduler@v1.20.4   # version audited against
last_audit_commit: 174b1f53                            # HEAD when this audit pass started
last_audit_date: 2026-07-24
overall: A            # genuine wire-breaking and next-invocation-computation bugs found and fixed (see Notes)
ops:
  CreateSchedule:      {wire: ok, errors: ok, state: fixed, persist: ok, note: "ClientToken now idempotent (see Notes); ScheduleExpressionTimezone now validated as a real IANA name"}
  GetSchedule:         {wire: fixed, errors: ok, state: ok, persist: ok, note: "invented non-canonical Tags field deleted"}
  UpdateSchedule:      {wire: ok, errors: ok, state: fixed, persist: ok, note: "ScheduleExpressionTimezone now validated as a real IANA name (prior pass's State-omission fix re-verified still correct, see Notes)"}
  DeleteSchedule:      {wire: ok, errors: ok, state: ok, persist: ok}
  ListSchedules:       {wire: fixed, errors: ok, state: ok, persist: ok, note: "invented Target.RoleArn field deleted (real TargetSummary has only Arn)"}
  CreateScheduleGroup: {wire: ok, errors: ok, state: fixed, persist: ok, note: "ClientToken now idempotent (see Notes)"}
  GetScheduleGroup:    {wire: fixed, errors: ok, state: ok, persist: ok, note: "invented non-canonical Tags field deleted"}
  DeleteScheduleGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascade-delete-all-schedules-in-group re-verified correct against the real API doc comment; async DELETING intermediate state intentionally not modeled, see Notes"}
  ListScheduleGroups:  {wire: fixed, errors: ok, state: ok, persist: ok, note: "invented non-canonical Tags field deleted"}
  TagResource:         {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource:       {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  RouteMatcher: {status: ok, note: "re-verified every op's REST method+path prefix against aws-sdk-go-v2 serializers.go this pass -- no drift; see prior pass's per-op mapping in Notes."}
  next-invocation computation: {status: fixed, note: "at() one-time expressions were validated at Create/Update time but the runner's isDue only matched rate()/cron() prefixes -- an at() schedule could NEVER fire. ScheduleExpressionTimezone was stored/round-tripped on the wire but never applied when evaluating cron/at wall-clock matches (runner always used the poll goroutine's raw time.Time, i.e. implicitly UTC/server-local). StartDate/EndDate were stored/round-tripped but the runner never gated cron/rate firing on them. All three fixed this pass -- see Notes."}
  cross-service target delivery: {status: ok, note: "cli.go's wireSchedulerRunner wires ALL 8 Runner invoker interfaces (Lambda, SQS, SNS, StepFunctions, EventBridge, Kinesis, SageMaker, ECS); unchanged this pass, re-confirmed not a gap."}
gaps: []
deferred: []
leaks: {status: clean, note: "leak_main_test.go (testleak.VerifyTestMain) passes under -race. The runner's poll goroutine remains the only background goroutine (ctx-parented via Handler.StartWorker/Shutdown, unchanged this pass). New state added this pass (Runner.locCache, Handler.idempotency) is plain in-memory data with no goroutines/tickers of its own; both are swept/bounded (locCache via the existing per-poll sweep alongside cronCache; idempotency via TTL-based lazy eviction) and cleared on Handler.Reset."}
---

## Notes (2026-07-24 pass)

- **`at()` one-time schedules could never fire (the big one).** `validateScheduleExpression`
  has always accepted `at(yyyy-mm-ddThh:mm:ss)` at Create/UpdateSchedule time, but
  `Runner.isDue` only recognized the `rate(` and `cron(` prefixes -- any `at()`
  schedule silently sat forever with zero invocations, a genuine
  next-invocation-computation bug (the parity bar this service is held to). Fixed
  by adding `parseAtExpression` (schedule_expression.go) and `Runner.isDueAt`
  (runner.go): an `at()` schedule fires exactly once, the first poll on/after its
  target instant, and never again (tracked via the existing `lastFiredAt` map, the
  same mechanism used for cron's within-minute dedup). Covered by
  `TestScheduler_Runner_AtExpressionFiresOnceThenNeverAgain`,
  `TestScheduler_Runner_AtExpressionNotYetDue`, `TestScheduler_ParseAtExpression`.
- **`ScheduleExpressionTimezone` was stored and echoed back on the wire but had zero
  effect on runtime firing.** Real AWS evaluates cron and at() expressions'
  wall-clock fields against the schedule's `ScheduleExpressionTimezone` (default
  UTC when unset). gopherstack's runner always matched cron fields against the
  poll goroutine's raw `time.Time` with no timezone conversion, and (per the bug
  above) never evaluated `at()` at all. Fixed: `Runner.cachedLocation` resolves and
  caches the `*time.Location` for a schedule's timezone (mirroring the existing
  `cronCache` pattern, swept the same way in `checkAndFireSchedules`), and both
  `isDueCron` and `isDueAt` convert `now` into that location before matching/
  comparing. Also added `validateTimezone` (schedules.go), called from
  Create/UpdateSchedule, rejecting a `ScheduleExpressionTimezone` that isn't a
  resolvable IANA name with `ValidationException` -- an unresolvable name could
  never be evaluated by the runner anyway (previously it silently fell through to
  UTC with no error). Covered by `TestScheduler_Runner_CronRespectsTimezone`,
  `TestScheduler_Runner_AtExpressionRespectsTimezone`,
  `TestScheduler_Runner_LocCacheEviction`,
  `TestCreateSchedule_ScheduleExpressionTimezone_Validation`,
  `TestUpdateSchedule_ScheduleExpressionTimezone_Validation`.
- **`StartDate`/`EndDate` were stored and echoed back on the wire but had zero effect
  on runtime firing.** Real AWS: "invocations might occur on, or after, the
  StartDate"; "invocations might stop on, or before, the EndDate" for recurring
  (cron/rate) schedules, while one-time (`at()`) schedules explicitly ignore both.
  gopherstack's runner never referenced `s.StartDate`/`s.EndDate` at all -- a
  schedule with an `EndDate` in the past kept firing forever, and one with a future
  `StartDate` fired immediately. Fixed via `withinScheduleWindow` (runner.go),
  called from `isDue` for the `rate(`/`cron(` branches only (matching AWS's
  documented at()-ignores-the-window behavior). Covered by
  `TestScheduler_Runner_StartDateGatesRecurringSchedule`,
  `TestScheduler_Runner_EndDateGatesRecurringSchedule`,
  `TestScheduler_Runner_AtExpressionIgnoresStartAndEndDate`.
- **ClientToken idempotency implemented for CreateSchedule/CreateScheduleGroup.**
  The prior pass documented this as an accepted gap ("no idempotency-token pkg
  exists in pkgs/"); this pass implements a bounded, handler-level cache
  (idempotency.go) instead of a new pkgs/ package or a StorageBackend interface
  change: `Handler.idempotency` (a `safemap.Map[string, idempotentResult]`) caches
  a successful Create's ARN by a composite key of (op kind, group, name,
  ClientToken) for `clientTokenTTL` (5 minutes). A retried Create with the same
  ClientToken replays the cached ARN instead of hitting the backend's
  name-uniqueness check and failing with ConflictException. A *different*
  ClientToken (or none) on a colliding name still conflicts, preserving existing
  semantics. `createScheduleGroupInput` was also missing the `ClientToken` field
  entirely (dead on the wire, silently discarded) -- added. `Handler.Reset` now
  also clears the cache. This is intentionally handler-level, not
  backend/StorageBackend-level: it doesn't change `CreateSchedule`/
  `CreateScheduleGroup`'s widely-referenced (25+ call sites across every test file)
  StorageBackend signature, keeping the fix's blast radius contained to the two
  Create handlers. Covered by `idempotency_test.go`
  (`TestCreateSchedule_ClientToken_ReplaysOnRetry`,
  `TestCreateSchedule_NoClientToken_DuplicateNameStillConflicts`,
  `TestCreateSchedule_DifferentClientToken_DuplicateNameStillConflicts`,
  `TestCreateScheduleGroup_ClientToken_ReplaysOnRetry`,
  `TestSchedulerHandler_Reset_ClearsIdempotencyCache`). Deep AWS
  idempotency-mismatch semantics (rejecting a token reused with genuinely
  different parameters) are intentionally NOT modeled -- out of scope for the
  narrow lost-response-retry case this closes.
- **Invented (non-canonical) fields deleted, per this pass's directive to remove
  anything not in the real SDK rather than leave it as a documented gap.** The
  prior pass identified but chose to keep three non-canonical fields as "harmless
  extras"; this pass deletes them and fixes the three tests that had locked them
  in as expected behavior (`TestListSchedules_IncludesTargetSummary` in
  schedules_list_test.go asserted `Target.RoleArn`; `TestGetScheduleGroup_IncludesTags` /
  `TestListScheduleGroups_IncludesTags` in schedule_groups_test.go asserted a
  `Tags` field on Get/ListScheduleGroups -- all three now assert the field's
  *absence* and, where relevant, that the real `ListTagsForResource` path still
  returns the correct tags; renamed to `TestGetScheduleGroup_OmitsTags` /
  `TestListScheduleGroups_OmitsTags`):
  - `GetScheduleOutput`/`GetScheduleGroupOutput`/`ScheduleGroupSummary` do not have
    a `Tags` field in the real API (`aws-sdk-go-v2/service/scheduler/types` --
    tags are only ever fetched via `ListTagsForResource`). Deleted from
    `getScheduleOutput` (handler_schedules.go), `getScheduleGroupOutput` and
    `scheduleGroupSummary` (handler_schedule_groups.go).
  - `ScheduleSummary.Target` (`TargetSummary`) has only `Arn` in the real API, not
    `RoleArn`. Deleted `RoleArn` from `scheduleSummaryTarget`
    (handler_schedules.go).
- **DeleteScheduleGroup cascade-delete re-verified correct, not a gap.** Checked the
  real SDK's doc comment on `Client.DeleteScheduleGroup`
  (`api_op_DeleteScheduleGroup.go`): "Deleting a schedule group results in
  EventBridge Scheduler deleting all schedules associated with the group" --
  confirms gopherstack's synchronous cascade-delete (schedule_groups.go) is the
  correct outcome, not a rejection of non-empty groups. AWS's actual behavior is
  *asynchronous* (the group enters a `DELETING` state --
  `types.ScheduleGroupStateDeleting` exists in the real enum -- and schedules drain
  over time, described as "eventually consistent" in the doc comment);
  gopherstack's synchronous, immediate cascade is a deliberate, reasonable
  emulation simplification (consistent with how this in-memory backend has always
  modeled multi-step AWS async operations) and is left as-is -- modeling the
  `DELETING` intermediate state would require a background sweep goroutine for
  marginal emulation value and was judged out of scope for this pass's parity bar
  (schedule CRUD + correct next-invocation computation + state).
- **Protocol / route-matcher / error-shape / timestamp findings from the prior pass
  re-verified, unchanged**: restjson1, no `X-Amz-Target` on real traffic (kept for
  internal test convenience), REST path-to-op mapping matches
  `aws-sdk-go-v2/service/scheduler`'s `serializers.go` exactly for all 12 ops,
  `ConflictException`/`ResourceNotFoundException`/`ValidationException` via
  `service.JSONErrorResponse` match `restjson.GetErrorInfo`'s field lookup,
  `CreationDate`/`LastModificationDate`/`StartDate`/`EndDate` are epoch-seconds
  JSON numbers on both sides (matches `smithytime.FormatEpochSeconds`/
  `ParseEpochSeconds`), resource tags are `[]{Key,Value}` (`TagList`) not a JSON
  map on `CreateScheduleGroup.Tags`/`TagResource.Tags`/`ListTagsForResource.Tags`,
  `UntagResource`'s `TagKeys` REST query param is repeated
  (`?TagKeys=a&TagKeys=b`) not comma-joined, `UpdateSchedule`'s omitted `State`
  does not blank out the schedule's enabled/disabled status (re-verified this pass
  against `awsRestjson1_serializeOpDocumentUpdateScheduleInput`'s
  `if len(v.State) > 0` guard -- the omission is real client behavior, not a
  gopherstack invention, so preserving the existing value on omission remains the
  correct emulation), `ActionAfterCompletion` enum-validated (`NONE`/`DELETE`
  only).
- **"Looks wrong but is correct" traps for the next auditor**:
  - `EcsParameters.Tags []scheduleTargetEcsTag` (`{Key,Value}` list) is unrelated to
    the resource-tag/invented-field findings above and remains correct -- it's a
    genuine, real `Target.EcsParameters.Tags []map[string]string` field (per-ECS-
    task tags at launch time), covered by
    `TestParity_EcsParametersTaskTagsRoundtrip`.
  - The `X-Amz-Target`/`AWSScheduler.<Op>` JSON-1.1 dispatch path in handler.go is
    dead code for real AWS SDK traffic (restjson1 has no such header) but is kept
    intentionally for internal test convenience (`doSchedulerRequest` in
    handler_test.go uses it) -- don't remove it as "dead code."
  - `Runner.locCache`/`Handler.idempotency` are new pieces of in-memory state added
    this pass; neither owns a goroutine or ticker of its own (both are read/written
    synchronously from the existing poll loop or HTTP handler goroutines), so
    neither needed wiring into `Handler.StartWorker`/`Shutdown`'s ctx-parenting.
