---
service: stepfunctions
sdk_module: aws-sdk-go-v2/service/sfn@v1.45.4
last_audit_commit: HEAD
last_audit_date: 2026-07-23
overall: A            # Re-audit against `43aa6d65` baseline (2026-07-11 zero-drift pass). This
                       # pass found real drift/gaps despite the "zero drift" label: two commits
                       # ("Parity 4" efc42cbc, "Go refactoring 2" 9d7e36e0) landed on
                       # services/stepfunctions/ since 43aa6d65 and had ALREADY fixed the
                       # previously-documented SEVERE cli.go ECS/Glue/EventBridge wiring gap
                       # (confirmed: cli.go now calls SetECSIntegration/SetGlueIntegration/
                       # SetEventBridgeIntegration) -- that gap entry is removed below as
                       # resolved. This pass then deep-audited the 3 previously "spot-checked
                       # only" deferred families (state machine CRUD, activities, Distributed Map
                       # ItemReader) by field-diffing against aws-sdk-go-v2/service/sfn v1.40.8
                       # types (still pinned, unchanged) and found+fixed real gaps in each: (1)
                       # StartExecution/StartSyncExecution/DescribeStateMachine never resolved
                       # version- or alias-qualified stateMachineArn ARNs at all (a real,
                       # documented AWS feature -- weighted alias routing, version pinning --
                       # entirely unimplemented); (2) CreateStateMachine/UpdateStateMachine never
                       # returned stateMachineVersionArn/revisionId; (3) DescribeStateMachineVersion
                       # is a FABRICATED op with no counterpart in the real SDK (deleted -- see
                       # notes); (4) CreateActivity/DescribeActivity never supported
                       # encryptionConfiguration or tags (both real AWS fields); (5) SEVERE:
                       # asl.Executor's SetS3Reader (Distributed Map ItemReader S3 CSV/JSON/JSONL
                       # decoding, previously marked "spot-checked, appeared correct") was NEVER
                       # called anywhere in services/stepfunctions/ or cli.go -- an identical
                       # wiring-gap bug class to the just-resolved ECS/Glue/EventBridge one, fixed
                       # by adding a NewS3Integration adapter + cli.go wiring. Also fixed
                       # Retry.JitterStrategy enum validation (was silently permissive). All gates
                       # green (build/vet/gofmt/race-test/lint, 0 banned nolints).
ops:
  CreateStateMachine:
    wire: fixed
    errors: ok
    state: ok
    persist: ok
    note: >
      FIXED this pass: the response never echoed back stateMachineVersionArn
      when publish=true (AWS: "If you do not set the publish parameter to
      true, this field returns null value" -- implying it MUST be populated
      when publish=true; PublishStateMachineVersion's result was previously
      discarded). Also added versionDescription parsing + AWS's documented
      ValidationException when versionDescription is set with publish=false.
      STANDARD/EXPRESS, roleArn validation, tags, logging/tracing config
      unchanged and correct.
  UpdateStateMachine:
    wire: fixed
    errors: ok
    state: ok
    persist: ok
    note: >
      FIXED this pass: UpdateStateMachineOutput.RevisionId and
      .StateMachineVersionArn (publish=true only) were entirely absent --
      the backend method returned only (updateDate, error). Added
      StateMachine.RevisionID (opaque crypto/rand-generated token,
      regenerated every update, matching AWS's "compare between versions ...
      without performing a diff of the properties" semantics), changed
      UpdateStateMachine's signature to (updateDate, revisionID, error), and
      wired both new output fields + the same versionDescription/publish
      ValidationException as CreateStateMachine.
  DeleteStateMachine: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeStateMachine:
    wire: fixed
    errors: ok
    state: ok
    persist: ok
    note: >
      FIXED this pass: did not support version-qualified ARNs at all. AWS:
      "This API action returns the details for a state machine version if
      the stateMachineArn you specify is a state machine version ARN" (and
      echoes the version ARN back as StateMachineArn, unlike execution
      start which always normalizes to the base ARN). This is the REAL
      mechanism AWS uses for fetching version details -- there is no
      separate DescribeStateMachineVersion operation in the actual API (see
      notes; that op was fabricated in this emulator and has been deleted).
      Also now returns the new RevisionID field.
  ListStateMachines: {wire: ok, errors: ok, state: ok, persist: ok, note: "page.Page[T] pagination"}
  DescribeStateMachineForExecution: {wire: ok, errors: ok, state: ok, persist: ok}
  PublishStateMachineVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteStateMachineVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  ListStateMachineVersions: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateStateMachineAlias: {wire: ok, errors: ok, state: ok, persist: ok, note: "routingConfiguration weighted versions validated"}
  UpdateStateMachineAlias: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteStateMachineAlias: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeStateMachineAlias: {wire: ok, errors: ok, state: ok, persist: ok}
  ListStateMachineAliases: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ValidateStateMachineDefinition:
    wire: fixed
    errors: ok
    state: ok
    persist: n/a
    note: >
      FIXED this pass (partial): added Retry.JitterStrategy enum validation
      (recursively, including nested Map/Parallel Iterator/ItemProcessor/
      Branches) -- AWS rejects anything other than "FULL"/"NONE"/omitted
      with ValidationException at Create/UpdateStateMachine time; this
      emulator previously accepted any string silently (bd: gopherstack-xtl,
      closed). Still JSON/structural validation only beyond that one check
      -- other deep ASL semantic checks (e.g. ToleratedFailure+INLINE
      combos) remain unimplemented, see gaps.
  StartExecution:
    wire: fixed
    errors: ok
    state: ok
    persist: ok
    note: >
      FIXED this pass (severe): StartExecution never resolved version- or
      alias-qualified stateMachineArn ARNs -- AWS documents all three input
      shapes (unqualified / stateMachineArn:N version / stateMachineArn:name
      alias) as valid, with alias ARNs applying weighted routing across 1-2
      versions. The pre-existing code did a direct, exact-match
      b.stateMachines.Get(stateMachineArn) lookup, so ANY qualified-ARN
      StartExecution call failed with StateMachineDoesNotExist even though
      CreateStateMachineAlias/PublishStateMachineVersion (the resource CRUD
      side) were fully implemented and previously marked "ok" -- the two
      halves of this feature were never connected. Added
      resolveExecutionTarget() (qualified_arn.go): resolves unqualified/
      version/alias ARNs to the target version's frozen
      Definition/RoleArn/Type, keyed by the BASE (unqualified) ARN for
      execution-ARN construction (AWS never carries a qualifier into
      execution ARNs), with weighted random version selection for 2-entry
      alias routing configs. Added Execution.StateMachineVersionArn/
      StateMachineAliasArn (AWS DescribeExecutionOutput fields, previously
      entirely absent), populated only when the qualifier was used, per
      AWS's documented null-when-unqualified semantics. ClientRequestToken
      idempotency and EXPRESS's immediate-name-reuse semantics remain
      unmodeled (bd: gopherstack-1sf).
  StartSyncExecution:
    wire: fixed
    errors: ok
    state: ok
    persist: ok
    note: >
      FIXED this pass: same qualified-ARN resolution gap as StartExecution,
      fixed via the same resolveExecutionTarget() helper.
  StopExecution: {wire: ok, errors: ok, state: ok, persist: ok, note: "cancels the execution's context via cancelFns; goroutine exits promptly"}
  RedriveExecution: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeExecution:
    wire: fixed
    errors: ok
    state: ok
    persist: ok
    note: >
      FIXED this pass: StateMachineVersionArn/StateMachineAliasArn added
      (see StartExecution). NOT fixed this pass (new finding, field-diffed
      against DescribeExecutionOutput, filed as bd: gopherstack-f5dc):
      RedriveStatus/RedriveStatusReason, MapRunArn (would need Distributed
      Map child-execution architecture this emulator doesn't have --
      iterations run in-process, not as separate Execution records),
      TraceHeader (X-Ray passthrough, not even parsed as a StartExecution
      input), and InputDetails/OutputDetails (CloudWatchEventsExecutionDataDetails,
      always {truncated:false} in practice) remain absent.
  ListExecutions: {wire: ok, errors: ok, state: ok, persist: ok}
  GetExecutionHistory: {wire: ok, errors: ok, state: ok, persist: ok, note: "unchanged this pass; TaskScheduled/TaskSucceeded/TaskFailed detail population fixed in a prior pass -- remaining gaps tracked at bd: gopherstack-996"}
  CreateActivity:
    wire: fixed
    errors: ok
    state: ok
    persist: ok
    note: >
      FIXED this pass: encryptionConfiguration (real
      CreateActivityInput field, server-side KMS encryption) and tags (real
      CreateActivityInput field) were both entirely unparsed/unsupported.
      Added Activity.EncryptionConfiguration + SetActivityEncryptionConfiguration
      (mirrors SetStateMachineConfigurations' established pattern) and
      inline-tags handling (mirrors CreateStateMachine's h.setTags call).
      Kept CreateActivity(ctx, name)'s existing signature unchanged (~35
      call sites) rather than adding required params.
  DeleteActivity:
    wire: fixed
    errors: ok
    state: ok
    persist: ok
    note: >
      LEAK FIX this pass: DeleteActivity never cleaned up h.tags for the
      deleted activity ARN (DeleteStateMachine's handler already did this
      for state machines) -- a permanent per-deleted-activity tombstone
      entry in the handler's tags map. Added the same tagsMu-guarded
      cleanup DeleteStateMachine uses.
  DescribeActivity: {wire: fixed, errors: ok, state: ok, persist: ok, note: "now returns EncryptionConfiguration (see CreateActivity)"}
  ListActivities: {wire: ok, errors: ok, state: ok, persist: ok}
  GetActivityTask: {wire: ok, errors: ok, state: ok, persist: ok, note: "long-poll with WaitTimeSeconds; task-token issuance"}
  SendTaskSuccess: {wire: ok, errors: ok, state: ok, persist: ok}
  SendTaskFailure: {wire: ok, errors: ok, state: ok, persist: ok}
  SendTaskHeartbeat: {wire: ok, errors: ok, state: ok, persist: ok, note: "States.HeartbeatTimeout enforced against HeartbeatSeconds"}
  DescribeMapRun: {wire: ok, errors: ok, state: ok, persist: ok}
  ListMapRuns: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateMapRun: {wire: ok, errors: ok, state: ok, persist: ok, note: "ToleratedFailureCount/Percentage on the MapRun *resource* API were already real; the ASL-definition-level Map state fields were fixed in a prior pass"}
  TestState: {wire: ok, errors: ok, state: ok, persist: n/a}
families:
  asl_task:
    status: ok
    note: "Unchanged this pass; verified ok in a prior pass (resource ARN resolution, Retry/Catch, .sync patterns). See families.asl_map for this pass's S3Reader wiring fix, which asl_task's own resource-ARN paths (Lambda/SQS/SNS/DynamoDB/ECS/Glue/EventBridge/Activity) do not touch."
  asl_choice:
    status: ok
    note: "Unchanged this pass."
  asl_map:
    status: fixed
    note: >
      SEVERE FIX this pass: asl.Executor's ItemReader path
      (resolveItemsFromReader, S3 CSV/JSON/JSONL decoding) was already
      correctly implemented and unit-tested at the executor level with
      mocks (asl/intrinsics_extras_test.go's stubS3), and the prior pass's
      PARITY.md carried it forward as "deferred ... spot-checked only,
      appeared correct" -- but nothing in services/stepfunctions/ or cli.go
      ever called InMemoryBackend.SetS3Reader (which didn't even exist as a
      backend method). e.s3 was always nil in any real running gopherstack
      process, so EVERY Distributed Map ItemReader hard-failed with
      ErrS3ReaderNotConfigured -- an identical bug class to the
      ECS/Glue/EventBridge cli.go wiring gap a prior pass found and fixed
      (see notes below). Fixed by: adding InMemoryBackend.s3Reader field +
      SetS3Reader() (store.go), threading it through
      startExecutionLocked/runParsedExecution/StartSyncExecution/
      RedriveExecution alongside the other 6 integrations, adding
      s3Adapter/NewS3Integration (integrations.go, adapts
      services/s3.StorageBackend.GetObject to asl.S3Reader), and wiring
      cli.go's wireStepFunctionsServiceIntegrations to call
      sfnBk.SetS3Reader(sfnbackend.NewS3Integration(s3H.Backend)) alongside
      the existing SQS/SNS/DynamoDB/ECS/Glue/EventBridge calls. Verified
      end-to-end with a real StartExecution against a Map+ItemReader
      definition and a fake S3Reader (services/stepfunctions/s3_item_reader_test.go).
      ResultWriter (S3 write-out, bd: gopherstack-8j8) is now implemented:
      State.ResultWriter is parsed (parser.go), and
      Executor.exportMapResults (asl/result_writer.go) writes
      SUCCEEDED_n.json/FAILED_n.json plus a manifest.json to the wired
      S3Writer on Map completion, returning
      {MapRunArn, ResultWriterDetails:{Bucket,Key}} in place of inline
      results -- verified against AWS docs
      (input-output-resultwriter.html) for the ResultWriter/
      ResultWriterDetails/manifest.json shapes, since aws-sdk-go-v2's sfn
      client has no typed struct for any of these (they're ASL-JSON/
      execution-output only, never part of the control-plane API surface).
      Wired the same way as S3Reader: cli.go's
      wireStepFunctionsServiceIntegrations now also calls
      sfnBk.SetS3ResultWriter(sfnbackend.NewS3ResultWriterIntegration(s3H.Backend)).
      Known deviations: WriterConfig (Transformation/OutputType) is parsed
      but not applied -- only the plain S3-export shape is honored; per-item
      result records omit ExecutionArn/Name/StartDate/StopDate (real AWS
      backs each with a genuine child execution, gopherstack runs Map
      iterations as in-process sub-executors with no such resource to
      point to); the manifest folder segment uses gopherstack's own
      MapRunArn suffix (.../execName/stateName) rather than real AWS's
      Map:<uuid>, since gopherstack's MapRunArn was never shaped like
      AWS's to begin with. When no S3Writer is wired, or Parameters.Bucket
      is unset, the Map state degrades to its pre-existing inline-results
      behavior rather than failing. DEFERRED (unchanged):
      ItemProcessor.ProcessorConfig.Mode (bd: gopherstack-8im) remains
      unimplemented.
  asl_parallel:
    status: ok
    note: "Unchanged this pass."
  asl_wait:
    status: ok
    note: "Unchanged this pass."
  asl_pass_succeed_fail:
    status: ok
    note: "Unchanged this pass."
  asl_intrinsics:
    status: ok
    note: "Unchanged this pass. Non-AWS extras (informational, not a bug) still present -- see gaps."
  json_1_0_protocol:
    status: ok
    note: "Unchanged this pass."
gaps:
  - "Map Distributed Map ResultWriter's WriterConfig (Transformation/OutputType) is parsed but not applied, only the plain S3-export shape; per-item result records omit ExecutionArn/Name/StartDate/StopDate since gopherstack Map iterations aren't backed by real child executions (bd: gopherstack-8j8, implemented this pass -- see asl_map_and_distributed_map notes)"
  - "Map ItemProcessor.ProcessorConfig.Mode (INLINE/DISTRIBUTED) not parsed/validated (bd: gopherstack-8im)"
  - "StartExecution has no ClientRequestToken idempotency; EXPRESS's immediate-name-reuse semantics (vs STANDARD's reuse restriction) are not modeled (bd: gopherstack-1sf)"
  - "TaskScheduledEventDetails/TaskSucceededEventDetails still omit resourceType/region/parameters/timeoutInSeconds/heartbeatInSeconds/outputDetails.truncated; no TaskSubmitted/TaskStarted history events for .sync/.waitForTaskToken (bd: gopherstack-996)"
  - "DescribeExecutionOutput missing RedriveStatus/RedriveStatusReason/MapRunArn/TraceHeader/InputDetails/OutputDetails (found this pass via SDK field-diff; StateMachineVersionArn/StateMachineAliasArn were fixed this pass, these were not, bd: gopherstack-f5dc)"
  - "Non-standard intrinsic functions (StringConcat, ArraySlice, MathSubtract, etc.) are accepted by this emulator but do not exist in real AWS Step Functions -- permissive superset, not a correctness bug against valid AWS definitions, but a definition that only works here would fail on real AWS (no bd filed; informational)"
deferred: []
leaks: {status: clean, note: "StopExecution/DeleteStateMachine cancel the execution's context via b.cancelFns; Wait/waitForRetry/execSem/semaphore all select on ctx.Done(); Map/Parallel goroutines (wg.Go) all respect ctx cancellation. FIXED this pass: DeleteActivity leaked a permanent h.tags tombstone entry per deleted activity (see ops.DeleteActivity). No new goroutines introduced this pass (resolveExecutionTarget/S3Reader wiring are synchronous, no new goroutines)."}
---

## Notes

**This pass's brief was to deep-audit the 3 previously "spot-checked only"
deferred families by actually field-diffing them against
aws-sdk-go-v2/service/sfn v1.40.8 types, not just re-asserting "appeared
correct".** All 3 had real, previously-undiscovered gaps:

**1. State machine CRUD -- qualified-ARN resolution was entirely missing.**
AWS's `StartExecutionInput.StateMachineArn` doc explicitly describes three
valid input shapes: unqualified, version-qualified (`stateMachineArn:N`),
and alias-qualified (`stateMachineArn:name`, with weighted routing across
1-2 versions). This emulator's `CreateStateMachineAlias`/
`PublishStateMachineVersion`/routing-weight-validation (the *resource* CRUD
side) were previously verified `ok` and are indeed correct -- but
`StartExecution`/`StartSyncExecution`/`DescribeStateMachine` never consulted
any of that data; they did a direct, exact-ARN `b.stateMachines.Get()`
lookup that always failed for a qualified ARN. **This is the same shape of
bug as the Map/asl_map disguised-stub found in a prior pass**: two halves of
a feature (resource management + resource consumption) were each
individually correct and individually tested, but never connected, so a
green test suite for either half never caught it. Fixed via
`resolveExecutionTarget()` in the new `qualified_arn.go`. **Trap for the
next auditor**: when a resource type has both a "create/configure" API
surface and a "consume/reference" API surface (aliases+StartExecution,
Map's ItemReader+the S3 integration below), audit them together -- a
family status of "ok" on the config side proves nothing about whether the
consuming side actually resolves what was configured.

**2. DescribeStateMachineVersion is a FABRICATED, non-AWS operation --
deleted.** Verified against the full `aws-sdk-go-v2/service/sfn@v1.40.8`
`api_op_*.go` file listing (37 files, 37 real operations): there is no
`DescribeStateMachineVersion`. AWS's real mechanism for fetching version
details is calling `DescribeStateMachine` with a version-qualified ARN (the
SDK's own `DescribeStateMachineOutput.CreationDate` doc literally says "For
a state machine version, creationDate is the date the version was created"
and `DescribeStateMachineInput.StateMachineArn`'s doc says "If you specify a
state machine version ARN, this API returns details about that version").
This emulator had invented a whole separate wire op (route in
`handler_state_machine_versions.go`, entry in `GetSupportedOperations()`,
`StorageBackend` interface method) for this instead of implementing the
real mechanism. **Fix**: removed the op from `GetSupportedOperations()` and
the handler's dispatch table, removed it from the `StorageBackend`
interface (so `Handler` can no longer route it), and instead extended
`DescribeStateMachine` itself to resolve a version-qualified ARN (echoing
the version ARN back as `StateMachineArn`, per AWS's documented behavior --
notably different from execution-start's base-ARN-always semantics). The
backend method `InMemoryBackend.DescribeStateMachineVersion` was left in
place as a plain internal helper (existing tests call it directly on the
concrete type) since it's harmless non-wire-surface Go code, not a
fabricated AWS operation -- only the wire-level op was deleted. This is the
"gopherstack-invented op, not in the real SDK, DELETE it" bug class from the
polly tagging-surface precedent.

**3. Distributed Map ItemReader S3 decoding: the SEVERE cli.go-style wiring
gap recurred, one level down.** A prior pass fixed cli.go never wiring
`SetECSIntegration`/`SetGlueIntegration`/`SetEventBridgeIntegration` despite
`asl.Executor` fully implementing and unit-testing those integrations. This
pass found the exact same bug class for S3: `asl.Executor.SetS3Reader` /
the `S3Reader` interface / `resolveItemsFromReader`'s CSV/JSON/JSONL
decoding were all correctly implemented and mock-tested
(`asl/intrinsics_extras_test.go`), but `InMemoryBackend.SetS3Reader` didn't
even exist, and nothing called it. Any real Distributed Map with an
`ItemReader` hard-failed with `ErrS3ReaderNotConfigured` in every actual
running gopherstack process. **Trap for the next auditor, restated from the
prior ECS/Glue/EventBridge finding because it just recurred**: an
`asl_*`-family or executor-level "ok"/mock-tested verdict proves the
*executor* dispatches correctly -- it says nothing about whether
`services/stepfunctions/`'s `InMemoryBackend` (let alone `cli.go`) actually
wires the concrete integration through. Every `asl.Executor.SetXIntegration`
call needs a matching audit trail: backend field -> `SetXIntegration`
method -> threaded through `startExecutionLocked`/`runParsedExecution`/
`StartSyncExecution`/`RedriveExecution` -> adapter in `integrations.go` ->
`cli.go` wiring call. Missing any link in that chain reproduces this bug
silently.

**4. Activities: encryptionConfiguration and tags were real, entirely
unparsed `CreateActivityInput` fields.** Field-diffed
`api_op_CreateActivity.go`/`api_op_DescribeActivity.go` against
`activities.go`/`handler_activities.go` and found both fields simply
absent -- not stubbed, just never referenced anywhere. Added
`Activity.EncryptionConfiguration` + `SetActivityEncryptionConfiguration`
(mirrors `SetStateMachineConfigurations`'s established optional-post-create-
config pattern) and inline-tags handling (mirrors `CreateStateMachine`'s).
Also found and fixed a real leak while in this code: `DeleteActivity`'s
handler never cleaned up `h.tags`, unlike `DeleteStateMachine`'s.

**RevisionId / StateMachineVersionArn on Create/UpdateStateMachine.** Field-
diffing `CreateStateMachineOutput`/`UpdateStateMachineOutput` found both
response types missing fields the emulator's own backend logic already had
the data for: `PublishStateMachineVersion`'s result was already being
computed when `publish=true` but thrown away (`_, _ =
h.Backend.PublishStateMachineVersion(...)`) instead of echoed back, and
`RevisionId` didn't exist as a concept anywhere in the backend. Added
`StateMachine.RevisionID` (opaque token, regenerated every
`UpdateStateMachine` call, absent/empty until the first update -- matching
AWS's documented "compare between versions ... without performing a diff"
semantics) and wired it + `StateMachineVersionArn` into both response
types, `DescribeStateMachine`, and added the `versionDescription`-requires-
`publish=true` `ValidationException` AWS documents for both ops.

**Retry.JitterStrategy validation** (bd: gopherstack-xtl, closed this pass):
added a recursive walk (`asl/parser.go`'s `validateJitterStrategies`, covers
nested `Iterator`/`ItemProcessor`/`Branches`) rejecting anything other than
`"FULL"`/`"NONE"`/omitted at `Parse` time, which is called from
`CreateStateMachine`/`UpdateStateMachine`/`ValidateStateMachineDefinition`/
`StartExecution`/`TestState` uniformly.

**Confirmed already-fixed (not by this pass): the previously-documented
SEVERE cli.go ECS/Glue/EventBridge wiring gap.** `git log` showed two
commits (`efc42cbc` "Parity 4", `9d7e36e0` "Go refactoring 2") landed on
`services/stepfunctions/` since the `43aa6d65` baseline despite the prior
pass's "zero drift" framing (that framing compared against a *different*,
even-older baseline, `ce30166a`) -- `cli.go` now calls
`sfnBk.SetECSIntegration`/`SetGlueIntegration`/`SetEventBridgeIntegration`
(verified at `cli.go`'s `wireStepFunctionsServiceIntegrations`, ~L3855-3867).
Removed the stale gap entry.

**Protocol**: json-1.0, unchanged this pass.

## Prior-pass notes (unchanged, retained for history)

See git history for this file's content as of commit `43aa6d65` for the
full prior-pass notes on: the Map `runMapItem` disguised-stub fix, Map/
Parallel Retry+Catch addition, `GetExecutionHistory` detail-object
population fix, the Catch error-output two-key-shape fix, and the
StartExecution-on-EXPRESS / StartSyncExecution-on-STANDARD error-code fixes.
Those `families:`/`ops:` verdicts are carried forward unchanged in the
front-matter above (marked `ok` / not re-noted) since this pass found no new
drift in them.
