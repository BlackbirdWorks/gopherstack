---
service: eventbridge
sdk_module: aws-sdk-go-v2/service/eventbridge@v1.45.21
last_audit_commit: 9f336807
last_audit_date: 2026-07-05
overall: A
ops:
  CreateEventBus: {wire: ok, errors: ok, state: ok, persist: ok, note: "name length/prefix validation, 200-per-account custom-bus limit enforced across regions"}
  DeleteEventBus: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascades rule/target/index cleanup; default bus protected"}
  ListEventBuses: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeEventBus: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateEventBus: {wire: ok, errors: ok, state: ok, persist: ok}
  PutRule: {wire: ok, errors: ok, state: ok, persist: ok, note: "EventPattern/ScheduleExpression mutual exclusivity + at-least-one enforced; 300-per-bus rule limit; ScheduleExpression validated via parseScheduleExpression (see schedule.go fix below)"}
  DeleteRule: {wire: ok, errors: ok, state: ok, persist: ok, note: "ManagedBy not enforced -- see gaps (gopherstack-ba7)"}
  ListRules: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeRule: {wire: ok, errors: ok, state: ok, persist: ok}
  EnableRule: {wire: ok, errors: ok, state: ok, persist: ok, note: "ManagedBy not enforced -- see gaps (gopherstack-ba7)"}
  DisableRule: {wire: ok, errors: ok, state: ok, persist: ok, note: "ManagedBy not enforced -- see gaps (gopherstack-ba7)"}
  PutTargets: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this sweep: Target was missing 8 of the SDK's target-type-specific parameter structs (AppSyncParameters, EcsParameters, HttpParameters, KinesisParameters, RedshiftDataParameters, RunCommandParameters, SageMakerPipelineParameters, SqsParameters), so any client setting them lost the data silently (json.Unmarshal drops unknown fields). Added all 8 plus their nested types, with required-field validation (EcsParameters.TaskDefinitionArn, KinesisParameters.PartitionKeyPath, RedshiftDataParameters.Database, RunCommandParameters.RunCommandTargets[].Key/Values) mirroring aws-sdk-go-v2's client-side validators, and RetryPolicy bound validation (MaximumRetryAttempts 0-185, MaximumEventAgeInSeconds 60-86400) which the client SDK does not validate locally either. 5-targets-per-rule limit enforced (was already ok)."}
  RemoveTargets: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTargetsByRule: {wire: ok, errors: ok, state: ok, persist: ok, note: "now round-trips all target-type-specific parameters (see PutTargets)"}
  ListRuleNamesByTarget: {wire: ok, errors: ok, state: ok, persist: ok}
  PutEvents: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this sweep: added the 1-10 entries-per-request limit (AWS: PutEventsRequestEntryList min 1/max 10 -- was previously unbounded, a test even fed 1100 entries in a single call) and per-entry required-field validation for Source/DetailType/Detail (AWS: an entry missing any of the three fails individually with InvalidArgument; if NONE of the entries in the batch have all three, AWS fails the whole request). Signature changed additively from `[]EventResultEntry` to `([]EventResultEntry, error)` to carry the new whole-request failures -- see Notes for the signature-safety check performed."}
  PutPartnerEvents: {wire: ok, errors: ok, state: ok, persist: ok, note: "delegates to PutEvents; inherits the same fix"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ActivateEventSource: {wire: ok, errors: ok, state: ok, persist: ok}
  DeactivateEventSource: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeEventSource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListEventSources: {wire: ok, errors: ok, state: ok, persist: ok}
  CancelReplay: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeReplay: {wire: ok, errors: ok, state: ok, persist: ok}
  ListReplays: {wire: ok, errors: ok, state: ok, persist: ok}
  StartReplay: {wire: ok, errors: ok, state: ok, persist: ok, note: "not re-audited op-by-op this sweep beyond a spot check; prior sweeps (c48d08ab) added real replay-worker delivery"}
  CreateApiDestination: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteApiDestination: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeApiDestination: {wire: ok, errors: ok, state: ok, persist: ok}
  ListApiDestinations: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateApiDestination: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateArchive: {wire: ok, errors: ok, state: ok, persist: ok, note: "spot-checked only; not re-audited op-by-op this sweep"}
  DeleteArchive: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeArchive: {wire: ok, errors: ok, state: ok, persist: ok}
  ListArchives: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateArchive: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateConnection: {wire: ok, errors: ok, state: ok, persist: ok, note: "spot-checked only (auth masking, API_KEY/BASIC/OAUTH); not re-audited op-by-op this sweep"}
  DeleteConnection: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeConnection: {wire: ok, errors: ok, state: ok, persist: ok}
  ListConnections: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateConnection: {wire: ok, errors: ok, state: ok, persist: ok}
  DeauthorizeConnection: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateEndpoint: {wire: ok, errors: ok, state: ok, persist: ok, note: "spot-checked only; not re-audited op-by-op this sweep"}
  DeleteEndpoint: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeEndpoint: {wire: ok, errors: ok, state: ok, persist: ok}
  ListEndpoints: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateEndpoint: {wire: ok, errors: ok, state: ok, persist: ok}
  CreatePartnerEventSource: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePartnerEventSource: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribePartnerEventSource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPartnerEventSources: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPartnerEventSourceAccounts: {wire: ok, errors: ok, state: ok, persist: ok}
  TestEventPattern: {wire: ok, errors: ok, state: ok, persist: n/a, note: "delegates to the same compilePattern/matchCompiledPattern engine proved correct this sweep -- see families.event_pattern_matching"}
  PutPermission: {wire: ok, errors: ok, state: ok, persist: ok, note: "spot-checked only; not re-audited op-by-op this sweep"}
  RemovePermission: {wire: ok, errors: ok, state: ok, persist: ok}
  GetEventBusPolicy: {wire: partial, errors: ok, state: ok, persist: ok, note: "not a real EventBridge SDK op (no GetEventBusPolicy/PutEventBusPolicy in aws-sdk-go-v2/service/eventbridge's 53 ops); an internal-only helper reachable via the handler's policyActions() dispatch table but absent from GetSupportedOperations, so no real SDK client can invoke it. Harmless (DescribeEventBus.Policy is the real wire path for reading a bus policy) but not itself a modeled AWS op -- left as-is, not a gap worth fixing."}
  PutEventBusPolicy: {wire: partial, errors: ok, state: ok, persist: ok, note: "same as GetEventBusPolicy -- not a real SDK op"}
families:
  event_pattern_matching: {status: ok, note: "Read pattern.go (559 LOC) in full and cross-checked every documented AWS content-filter operator against matchSpecialMatcher/matchStringMatcher: exact-match arrays, prefix (incl. nested equals-ignore-case form), suffix (incl. nested equals-ignore-case form), exists (true/false, including explicit JSON null counting as present), numeric (paired-operator ranges, all four comparators), anything-but (scalar/list/object forms incl. nested prefix/suffix/wildcard/equals-ignore-case/numeric, each of which may itself be a list), cidr, wildcard (iterative two-pointer glob, no recursion/ReDoS), equals-ignore-case, nested objects (recursive matchObject), $or (top-level and nested), and array-valued event fields (any-element-matches semantics). All correct and already covered by pattern_test.go (519 LOC) + pattern_validation_test.go (129 LOC). No fix needed -- proof only."}
  schema_registry_and_pipes: {status: ok, note: "CreateRegistry..GetCodeBindingSource and CreatePipe..UpdatePipe are separate control planes in real AWS (schemas/pipes SDK modules, not events); not re-audited op-by-op this sweep, no evidence of regressions while reading adjacent PutTargets/PutEvents code."}
gaps:
  - "Rule.ManagedBy is modeled and echoed on Describe/List, and PutRuleInput even lets a caller set it directly (real AWS's PutRule request has no ManagedBy member at all -- it's a server-populated Describe/List-only field), but NO op (PutRule update, DeleteRule, EnableRule, DisableRule, PutTargets, RemoveTargets) checks it before mutating. Real AWS returns ManagedRuleException for all six when the target rule is AWS-service-managed. Not fixed this sweep: no composition-root code anywhere in this repo ever marks an eventbridge rule as managed, so the missing enforcement is currently unreachable/inert in practice, and building it out (new sentinel error + handleError case + internal seeding helper + a real trigger) is a bigger, more speculative change than the codebase's demonstrated usage patterns justify right now. (bd: gopherstack-ba7)"
  - "EventBridge rule-target delivery for non-core targets (Step Functions/ECS/Kinesis/CloudWatch Logs/API destinations) is fully implemented in delivery.go's deliverToTarget dispatch, but wireEventBridgeDelivery in cli.go (composition root, out of services/eventbridge/ and explicitly off-limits this sweep) only populates DeliveryTargets.Lambda/SQS/SNS. Rules with those other target types match correctly but never fire in the running app. Already tracked, not re-fixed. (bd: gopherstack-xoe)"
deferred:
  - "Archives (CreateArchive/UpdateArchive/DeleteArchive/DescribeArchive/ListArchives), replays (StartReplay/CancelReplay/DescribeReplay/ListReplays), connections (Create/Update/Delete/Describe/List/DeauthorizeConnection), API destinations (Create/Update/Delete/Describe/List), and global endpoints (Create/Update/Delete/Describe/List) -- spot-checked while reading adjacent code (all looked real: proper validation, real state, ARNs via arn-style helpers, persistence-backed), but not re-audited op-by-op line-by-line this pass. No evidence of regressions found."
  - "Schema registry (CreateRegistry..GetCodeBindingSource, 17 ops) and Pipes (CreatePipe..UpdatePipe, 5 ops) -- these model separate AWS control planes (schemas/pipes SDK modules), not core EventBridge (events) ops; not audited this pass."
  - "PutPermission/RemovePermission/policy-statement JSON shape (EventBusPolicyStatement.Principal as `any` for both string and object-with-AWS-key forms) -- spot-checked only."
leaks: {status: clean, note: "Re-verified this sweep: PutEvents's async delivery goroutine (b.wg.Go) acquires a workerSem slot or aborts on svcCtx.Done() before delivering, so Close()/Shutdown() cannot leave in-flight goroutines past defaultShutdownTimeout; deliverToTargetBounded applies a per-attempt context.WithTimeout and always cancels it. Scheduler (scheduler.go) and ArchiveJanitor (janitor.go) were not independently re-verified this pass (no changes made in either file), but existing leak_test.go/isolation_test.go continue to pass, including TestEventLog_CappedAtMax and TestEventLog_RetainsMostRecentEvents which now exercise the new 10-entry PutEvents batch cap via a batching loop (previously a single 1100-entry call, which the new limit would reject outright -- test updated, see Notes)."}
---

## Notes

Freeform findings from this sweep (gopherstack-b84), for the next auditor.

### Fixed this sweep (severe/high-value first)

1. **PutTargets silently dropped 8 of the SDK's target-type-specific parameter
   structs.** `Target` (models.go) only modeled
   Input/InputPath/InputTransformer/DeadLetterConfig/RetryPolicy/BatchParameters.
   Any client setting `EcsParameters`, `HttpParameters`, `KinesisParameters`,
   `RedshiftDataParameters`, `RunCommandParameters`,
   `SageMakerPipelineParameters`, `SqsParameters`, or `AppSyncParameters` on a
   target had that configuration vanish on the next
   `ListTargetsByRule`/`DescribeRule` call -- `encoding/json` silently drops
   unknown fields on unmarshal. This is exactly the "disguised stub" class the
   parity principles warn about: `PutTargets` looked fully real (validates,
   stores, indexes) but was quietly incomplete for any ECS/Kinesis/Redshift
   Data API/EC2 Run Command/SageMaker Pipeline/SQS-FIFO/AppSync/API-Gateway
   target. Fixed by adding all 8 structs (with their full nested shapes --
   e.g. `EcsParameters.NetworkConfiguration.AwsvpcConfiguration`,
   `CapacityProviderStrategy`, `PlacementConstraints/Strategy`, ECS task
   `Tags`) verified field-by-field against
   `aws-sdk-go-v2/service/eventbridge/types` (json-1.1 protocol: wire key ==
   Go SDK field name exactly, confirmed against serializers.go), plus
   required-member validation mirroring the SDK's own client-side validators
   (`validateEcsParameters`, `validateKinesisParameters`,
   `validateRedshiftDataParameters`, `validateRunCommandParameters`/`Target`)
   and RetryPolicy bound validation AWS documents but the client SDK does not
   enforce locally (MaximumRetryAttempts 0-185, MaximumEventAgeInSeconds
   60-86400 when set). All additive to `Target`/`PutTargets` -- no exported
   signature changed here.

2. **ScheduleExpression cron day-of-week used the wrong numbering AND didn't
   support day/month names at all**, despite an existing test
   (`cron(0 8 ? * MON-FRI *)`) implying it should. AWS's cron day-of-week
   field is 1-7 with **1 = Sunday**; the matcher compared the raw field token
   directly against Go's `time.Weekday()`, which is 0-6 with **0 = Sunday** --
   an off-by-one that would silently fire scheduled rules one weekday early
   for every numeric day-of-week cron expression. Separately, three-letter
   names (`JAN`-`DEC` for month, `SUN`-`SAT` for day-of-week) were not
   resolved at all: `parseCron` only validates the *field count* (6), so
   `cron(0 8 ? * MON-FRI *)` parsed without error, but at match time
   `matchCronRange`/`matchCronToken` called `strconv.Atoi("MON")`, which
   always fails -- so that rule's schedule silently **never fired**, in any
   timezone, forever. This is the "test looks like it proves X but only
   proves parsing succeeds, not that matching is correct" trap -- worth
   flagging for the next auditor since it's easy to mistake a passing
   `TestSchedule_ParseCron` for evidence the feature works. Fixed by adding a
   `cronFieldKind`-aware token resolver (`cronTokenValue`) that maps
   `JAN`-`DEC`/`SUN`-`SAT` names and converts AWS's 1-7 day-of-week numbering
   to Go's canonical 0-6 at the single point where tokens are resolved, so
   every downstream comparison (exact/range/step) is against canonical Go
   weekday values. Added `TestSchedule_CronDayOfWeek` (11 cases) proving both
   the numbering fix and name support end-to-end via real `NextAfter` fire
   times, not just parse-success.

3. **PutEvents had no 1-10 entries-per-request limit and no per-entry
   required-field validation.** AWS's `PutEventsRequestEntryList` is
   documented min 1/max 10 items (enforced server-side; the client SDK's
   `validateOpPutEventsInput` only checks `Entries != nil`, not length or
   per-entry shape). This backend accepted arbitrarily large batches (a
   pre-existing test literally fed 1100 entries in one call to exercise the
   event-log cap) and never validated that `Source`/`DetailType`/`Detail`
   were present, silently assigning a real EventId and "succeeding" for
   malformed entries that real AWS would reject with a per-entry
   `InvalidArgument` (or fail the whole request if *no* entry in the batch
   has all three -- see `PutEventsRequestEntry.Detail`'s doc comment in
   aws-sdk-go-v2/service/eventbridge/types, which spells out both behaviors).
   Fixed both. The **exported signature of `PutEvents`/`PutPartnerEvents`
   changed additively** from `func(...) []EventResultEntry` to
   `func(...) ([]EventResultEntry, error)` to carry whole-request failures
   (>10 entries, 0 entries, or no entry with all three required fields).
   Signature-safety check performed: grepped every call site repo-wide;
   `cli.go:3364` and `cli_adapters.go:43` call `PutEvents` as a bare statement
   without capturing the return value, so the added return value does not
   break either composition-root call site (verified with `go build ./...`
   before/after). Only in-package callers captured a single return value
   (`sfn_integration.go`'s `SFNPutEvents`, plus several `_test.go` files) and
   were updated to handle the new `error`.
   `TestHandler_PutEvents_Empty` previously asserted an empty `Entries: []`
   batch returns HTTP 200 -- that encoded the wrong AWS behavior (AWS's
   `minItems: 1` constraint makes it a validation error, not a no-op
   success), so it was corrected to expect 400 with a comment explaining why.
   Added `TestAudit_PutEvents_EntryCountLimit` and
   `TestAudit_PutEvents_RequiredFields` (9 cases total) proving the new
   behavior.

### Read and proven already-correct (no fix needed)

- **`pattern.go`'s EventPattern matching engine** (559 LOC) -- read in full
  and cross-checked every AWS content-filter operator: exact-match arrays,
  `prefix`/`suffix` (including the nested `equals-ignore-case` form AWS added
  for case-insensitive prefix/suffix), `exists` (including that an explicit
  JSON `null` value counts as the key being *present*, matching AWS), numeric
  ranges (paired operators, all four comparators), `anything-but` in all its
  forms (scalar, list, and object -- where the object form's inner matcher
  may itself be a list, each element of which negates independently),
  `cidr`, `wildcard` (iterative two-pointer glob matching, so no
  recursion/backtracking blowup on adversarial patterns), nested objects
  (recursive), `$or` (both top-level and nested inside any object, including
  inside `detail`), and the "if the event field is a JSON array, any element
  matching satisfies the matcher" rule. All correct. This is a proof, not a
  fix -- flagging so the next auditor can trust this file without re-reading
  it (per the re-audit protocol: `pattern.go` unchanged since this commit ->
  trust the `ok` row).

### Traps for the next auditor

- A cron/rate schedule test that only asserts **parsing succeeds**
  (`TestSchedule_ParseCron`'s `"weekday"` case) is not proof the schedule
  actually **fires** correctly -- `parseCron` deliberately only validates
  field *count*, not field *content*, so a syntactically-6-field expression
  with content the matcher can't resolve (unsupported names, wrong numbering
  convention) parses cleanly and then simply never matches any candidate
  tick. Always follow a parse-test with a `NextAfter`-driven fire-time
  assertion (see `TestSchedule_CronDayOfWeek`) before trusting a schedule
  expression "supported."
- AWS's day-of-week cron convention is **1-7 with 1 = Sunday**; Go's
  `time.Weekday()` is **0-6 with 0 = Sunday**. Any code comparing a raw cron
  field token against `int(t.Weekday())` needs the offset in
  `cronTokenValue` -- don't reintroduce a direct comparison.
- `GetEventBusPolicy`/`PutEventBusPolicy` (handler.go's `policyActions()`)
  are **not real EventBridge SDK operations** (absent from
  `aws-sdk-go-v2/service/eventbridge`'s 53-op surface; the real wire path for
  reading a bus policy is `DescribeEventBus.Policy`). They're also absent
  from `GetSupportedOperations()`/`ChaosOperations()`, so no real AWS SDK
  client can reach them. Harmless, but don't mistake their presence in the
  dispatch table for a modeled AWS op when doing SDK-completeness sweeps.
- `fieldalignment -fix` (govet, enabled via `enable: [fieldalignment]` in
  `.golangci.yml`) reorders **struct field declarations** but does **not**
  update positional (unkeyed) struct literals elsewhere that depend on the
  old field order -- it silently produces a type-mismatch compile error in
  `_test.go` files that `go build ./...` won't catch (test files aren't
  compiled by `go build`), only `go vet`/`go test` will. If you ever run it
  as an autofix across a package with anonymous-struct test tables using
  positional literals, run `go vet ./...` immediately after and expect to
  convert the affected literals to keyed form.
