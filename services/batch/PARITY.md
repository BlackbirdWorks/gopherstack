---
service: batch
sdk_module: aws-sdk-go-v2/service/batch@v1.61.1
last_audit_commit: 01dbe288c7a19e4adc701e870bcee3d4907f6a05
last_audit_date: 2026-07-12
overall: A            # real fixes found: SubmitJob validation, two wire-shape bugs, missing fields
ops:
  RegisterJobDefinition: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed timeout + consumableResourceProperties nesting (were flat, real API nests both)"}
  DescribeJobDefinitions: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed name:revision exact-match bug; bare-name still returns all revisions (matches AWS)"}
  DeregisterJobDefinition: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateComputeEnvironment: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeComputeEnvironments: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateComputeEnvironment: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteComputeEnvironment: {wire: ok, errors: ok, state: ok, persist: ok, note: "requires DISABLED state + no referencing queues before delete, matches AWS docs"}
  CreateJobQueue: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeJobQueues: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateJobQueue: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteJobQueue: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascades job cleanup via byQueue index"}
  SubmitJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "was a disguised no-op for jobDefinition (never validated/resolved) and for arrayProperties/shareIdentifier/schedulingPriorityOverride/propagateTags/consumableResourcePropertiesOverride (silently discarded); all fixed. Response was missing jobArn; fixed."}
  DescribeJobs: {wire: ok, errors: ok, state: ok, persist: ok, note: "jobDetail was missing retryStrategy/timeout/arrayProperties/consumableResourceProperties/parameters/dependsOn/shareIdentifier/schedulingPriority/propagateTags even though the backend stored them; now surfaced. container/attempts/isCancelled/isTerminated/platformCapabilities still unmodeled -- see gaps."}
  ListJobs: {wire: ok, errors: ok, state: ok, persist: ok}
  TerminateJob: {wire: ok, errors: ok, state: ok, persist: ok}
  CancelJob: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateConsumableResource: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteConsumableResource: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeConsumableResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateConsumableResource: {wire: partial, errors: ok, state: ok, persist: ok, note: "ConsumableResourceProperty.Quantity is float64; real API uses int64 (Long) -- see gaps"}
  ListConsumableResources: {wire: ok, errors: ok, state: ok, persist: ok}
  ListJobsByConsumableResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  SchedulingPolicy: {status: deferred, note: "Create/Delete/Describe/List/Update not re-verified this pass; no bugs suspected from a skim, but not wire-checked against real SDK"}
  ServiceEnvironment: {status: deferred, note: "Create/Delete/Describe/Update not re-verified this pass"}
  ServiceJob: {status: deferred, note: "Submit/Describe/List/Terminate not re-verified this pass"}
  GetJobQueueSnapshot: {status: deferred, note: "not re-verified this pass"}
gaps:
  - "DescribeJobs (JobDetail) does not model container/attempts/isCancelled/isTerminated/platformCapabilities/nodeDetails/ecsProperties/eksProperties -- these require simulating job execution details, out of scope for this pass (bd: file follow-up)"
  - "ConsumableResourceProperty.Quantity is float64 in Go; real API's ConsumableResourceRequirement.Quantity is int64 (Long). Harmless for whole-number quantities (JSON encodes identically) but wrong for fractional input, which real AWS would reject anyway (bd: file follow-up, low priority)"
  - "JobDefinition has no RetryStrategy field; RegisterJobDefinition doesn't accept/store a default retry strategy even though real AWS Batch supports one at the job-definition level (job-level RetryStrategy via SubmitJob already works) (bd: file follow-up)"
  - "RegisterJobDefinition's EksProperties parameter is hardcoded nil in the handler (pre-existing, not touched this pass -- see handler.go handleRegisterJobDefinition comment)"
deferred:
  - SchedulingPolicy family (full wire-shape re-verification)
  - ServiceEnvironment family (full wire-shape re-verification)
  - ServiceJob family (full wire-shape re-verification)
  - GetJobQueueSnapshot
leaks: {status: clean, note: "janitor.go's advanceJobs/sweep* all take/release the coarse lockmetrics.RWMutex correctly; go test -race clean"}
---

## Notes

Protocol: restjson1. All paths are single-segment POST verbs under `/v1/` except
the three tag operations, which are `/v1/tags/{resourceArn}` with GET (List) /
POST (Tag) / DELETE (Untag) -- confirmed against
aws-sdk-go-v2/service/batch@v1.61.1/serializers.go for every op path + method.
RouteMatcher correctly excludes AppSync (`/v1/apis`), CodeArtifact, and Kafka/MSK
paths that share the `/v1/` prefix.

Batch's error model has only two shapes: `ClientException` (400, fault=client)
and `ServerException` (500, fault=server) -- there is **no** `ResourceNotFoundException`
in this API, unlike most other AWS services. `writeError` mapping ErrNotFound /
ErrAlreadyExists / ErrValidation all to `400 ClientException` is correct AWS
behavior, not a bug. (A stray `fix_handler.patch` committed in a prior sweep
proposed changing this to 404 `ResourceNotFoundException`, which would have been
wrong; it was never applied and has been deleted as debris this pass.)

### Real bugs fixed this pass

1. **SubmitJob accepted any `jobDefinition` string with zero validation** (a
   disguised no-op -- existing tests literally submitted against job
   definitions that were never registered and got HTTP 200). Real AWS Batch
   resolves `jobDefinition` as ARN, `name:revision`, or bare `name` (→ newest
   ACTIVE revision) and rejects anything that doesn't resolve. Fixed via
   `lookupJobDefinitionForSubmit` in backend.go; `Job.JobDefinition` now stores
   the resolved ARN (matching the real `JobDetail.JobDefinition` "ARN,
   required" contract), not the caller's short reference.

2. **`DescribeJobDefinitions` with an explicit `name:revision` reference
   ignored the revision** and returned every revision of that name instead of
   just the one requested. Fixed in `describeJobDefinitionsByNames` (now shared
   revision-parsing via `parseJobDefRevision`, also used by SubmitJob's
   resolver).

3. **`JobDefinition.Timeout` wire shape was wrong**: serialized as a flat
   `"timeoutSeconds": N` integer; real AWS Batch nests it as
   `"timeout": {"attemptDurationSeconds": N}` (confirmed against
   `types.JobDefinition.Timeout *JobTimeout` and the restjson1 deserializer's
   `case "timeout":`). A real SDK client parsing `DescribeJobDefinitions` would
   have silently gotten a nil Timeout on every job definition. Fixed by
   changing the field to `*JobTimeout` with json key `"timeout"`.

4. **`ConsumableResourceProperties` wire shape was wrong** on both
   `RegisterJobDefinition`'s request and `JobDefinition`/`JobDetail`'s
   response: serialized as a bare array; real AWS Batch nests it as
   `{"consumableResourceList": [...]}` (confirmed against
   `types.ConsumableResourceProperties.ConsumableResourceList` and both the
   register-input and job-definition/job-detail-output serializers/
   deserializers -- same nested shape on all three). A real SDK client sending
   the correctly-shaped request would have had the field silently dropped by
   this emulator's flat-array unmarshal. Fixed by introducing a
   `ConsumableResourceProperties{ConsumableResourceList: [...]}` wrapper type
   used consistently on JobDefinition, Job, and both handler input structs.

5. **`SubmitJob` silently discarded `arrayProperties`, `propagateTags`,
   `schedulingPriorityOverride`, `shareIdentifier`, and
   `consumableResourcePropertiesOverride`** from the request -- the handler
   hardcoded zero values for all five even though the backend function already
   accepted and stored them. Fixed by wiring the handler input struct through;
   also extended `DescribeJobs`'s `jobDetail` output to surface these
   (previously-stored-but-never-returned) fields plus `retryStrategy`,
   `timeout`, `parameters`, and `dependsOn`, using the real API's exact key
   names (notably `"schedulingPriority"` on output vs
   `"schedulingPriorityOverride"` on input -- these differ on the wire).

6. **`SubmitJobOutput` was missing `jobArn`.** Real `SubmitJobOutput` has
   `JobId`, `JobArn`, `JobName`; this emulator only returned the first and
   third. Fixed.

7. Removed an extraneous `"timeout"` field from `RegisterJobDefinitionOutput`
   (real output only has `jobDefinitionArn`/`jobDefinitionName`/`revision`) --
   harmless to real clients (unknown fields are ignored) but incorrect; cleaned
   up while touching this code for the Timeout fix above.

8. Deleted `fix_handler.patch`, a stray unapplied and factually-incorrect
   patch file committed in a prior sweep (see error-model note above).

### Verified NOT bugs ("looks wrong but correct")

- `DeleteComputeEnvironment`/`DeleteJobQueue` requiring `DISABLED` state before
  deletion matches documented AWS Batch behavior; not a false strictness bug.
- `CreateComputeEnvironment`/`CreateJobQueue` setting `Status: "VALID"`
  immediately (never modeling a `CREATING` transition) is a deliberate
  simplification that keeps SDK poll loops from hanging; `VALID` is a real
  reachable terminal status, so this is not a "stuck" bug.
- Job lifecycle (SUBMITTED → RUNNABLE/STARTING → RUNNING → SUCCEEDED) is
  actively advanced by `janitor.go`'s `advanceJobs`, not a disguised no-op.
