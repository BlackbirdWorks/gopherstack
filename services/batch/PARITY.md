---
service: batch
sdk_module: aws-sdk-go-v2/service/batch@v1.68.4
last_audit_commit: aad420594dea89bf7e3b745492889fee00ca2eb6
last_audit_date: 2026-07-25
overall: A            # SDK bump (v1.61.1 -> v1.68.0) added 6 new ops (QuotaShare CRUD+List, UpdateServiceJob); all 6 implemented for real this pass, no regressions in previously-audited ops
ops:
  RegisterJobDefinition: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed timeout + consumableResourceProperties nesting (prior pass); this pass wired retryStrategy and eksProperties through the handler (both were previously hardcoded nil/absent)"}
  DescribeJobDefinitions: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed name:revision exact-match bug; bare-name still returns all revisions (matches AWS); retryStrategy now surfaced"}
  DeregisterJobDefinition: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateComputeEnvironment: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeComputeEnvironments: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateComputeEnvironment: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteComputeEnvironment: {wire: ok, errors: ok, state: ok, persist: ok, note: "requires DISABLED state + no referencing queues before delete, matches AWS docs"}
  CreateJobQueue: {wire: ok, errors: ok, state: ok, persist: ok, note: "added jobQueueType + serviceEnvironmentOrder (were entirely unmodeled); rejects mixing computeEnvironmentOrder and serviceEnvironmentOrder, matching documented AWS constraint"}
  DescribeJobQueues: {wire: ok, errors: ok, state: ok, persist: ok, note: "surfaces jobQueueType/serviceEnvironmentOrder"}
  UpdateJobQueue: {wire: ok, errors: ok, state: ok, persist: ok, note: "added serviceEnvironmentOrder param"}
  DeleteJobQueue: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascades job cleanup via byQueue index, now correctly keyed by the queue's ARN (see SubmitJob note)"}
  SubmitJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "REAL BUG found and fixed this pass: Job.JobQueue stored the queue's bare NAME, but JobDetail.JobQueue is documented as the queue's ARN -- a real SDK client parsing DescribeJobs/ListJobsByConsumableResource got the wrong value on every job. Fixed by storing jq.JobQueueArn (matches the existing JobDefinition-stores-ARN pattern) and re-keying the byQueue index (jobsByQueueIdx) off the ARN throughout (listJobIDsForQueue, DeleteJobQueue, GetJobQueueSnapshot). Also: PlatformCapabilities now snapshotted from the resolved job definition at submit time (was entirely absent from the Job model)."}
  DescribeJobs: {wire: partial, errors: ok, state: ok, persist: ok, note: "gap #1 closed: container (derived from the job definition's ContainerProperties + ContainerOverrides, single-container jobs only), isCancelled/isTerminated (set by CancelJob/TerminateJob), and platformCapabilities are now modeled. Still NOT modeled: attempts (never populated -- this emulator doesn't simulate per-attempt retry execution), nodeDetails, ecsProperties, eksProperties (describe-side) -- these require simulating multi-node/ECS/EKS execution details, genuinely out of scope for an in-memory emulator; see items_still_open."}
  ListJobs: {wire: ok, errors: ok, state: ok, persist: ok}
  TerminateJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "sets IsTerminated"}
  CancelJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "sets IsCancelled"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateConsumableResource: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteConsumableResource: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeConsumableResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateConsumableResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "gap #2 closed: ConsumableResourceProperty.Quantity is now int64, matching types.ConsumableResourceRequirement.Quantity (a Long) exactly"}
  ListConsumableResources: {wire: ok, errors: ok, state: ok, persist: ok, note: "REAL BUG found and fixed this pass: wire key was \"consumableResourceSummaryList\"; real ListConsumableResourcesOutput key is \"consumableResources\" -- a real SDK client always saw an empty list. Also added maxResults/nextToken pagination (previously absent) and narrowed the response item shape to match types.ConsumableResourceSummary (no tags/createdAt on this op, unlike DescribeConsumableResource)."}
  ListJobsByConsumableResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "REAL BUG found and fixed this pass: returned the full Job shape under \"jobs\"; real ListJobsByConsumableResourceOutput.Jobs is []ListJobsByConsumableResourceSummary, a narrower/differently-named shape (jobQueueArn not jobQueue, jobStatus not status, plus quantity -- the requested amount of the queried resource). Added maxResults/nextToken pagination."}
  CreateSchedulingPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "REAL BUG found and fixed this pass: handler hardcoded fairsharePolicy to nil regardless of what the caller sent -- SchedulingPolicy backend already accepted/stored it, only the handler wiring was missing"}
  DeleteSchedulingPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeSchedulingPolicies: {wire: ok, errors: ok, state: ok, persist: ok, note: "field-diffed against types.SchedulingPolicyDetail (arn/name/fairsharePolicy/tags) -- matches"}
  ListSchedulingPolicies: {wire: ok, errors: ok, state: ok, persist: ok, note: "REAL BUG found and fixed this pass: returned the full SchedulingPolicy shape; real ListSchedulingPoliciesOutput.SchedulingPolicies is []SchedulingPolicyListingDetail, which has only \"arn\" (no name/fairsharePolicy/tags -- callers use DescribeSchedulingPolicies for those). Added maxResults/nextToken pagination (previously absent)."}
  UpdateSchedulingPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "REAL BUG found and fixed this pass: handler hardcoded fairsharePolicy to nil, same class of bug as CreateSchedulingPolicy"}
  CreateServiceEnvironment: {wire: ok, errors: ok, state: ok, persist: ok, note: "REAL BUG found and fixed this pass: CapacityLimits was missing from the ServiceEnvironment model entirely, even though it's a REQUIRED field on both CreateServiceEnvironmentInput and ServiceEnvironmentDetail -- a real SDK client's CapacityLimits was silently dropped on every create (confirmed: test/integration/batch_test.go's TestIntegration_Batch_ServiceEnvironmentLifecycle already sends CapacityLimits and never verified it round-tripped). Now required and validated (ErrValidation if empty)."}
  DeleteServiceEnvironment: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeServiceEnvironments: {wire: ok, errors: ok, state: ok, persist: ok, note: "added maxResults/nextToken pagination (previously absent; real API supports it)"}
  UpdateServiceEnvironment: {wire: ok, errors: ok, state: ok, persist: ok, note: "added capacityLimits param"}
  SubmitServiceJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "FULL REWRITE this pass -- see families.ServiceJob below for the invented-field deletion and wire-shape fixes"}
  DescribeServiceJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "see families.ServiceJob"}
  ListServiceJobs: {wire: ok, errors: ok, state: ok, persist: ok, note: "see families.ServiceJob; now filters by jobQueue (was serviceEnvironment) and defaults to RUNNING-only when jobStatus is unspecified, matching documented AWS behavior"}
  TerminateServiceJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "input key fixed from \"serviceJob\" to \"jobId\", matching TerminateServiceJobInput exactly"}
  GetJobQueueSnapshot: {wire: ok, errors: ok, state: ok, persist: ok, note: "REAL BUG found and fixed this pass: response used an invented \"timestamp\" field (seconds, float64) instead of the real \"lastUpdatedAt\" (epoch-milliseconds, int64), and each job's earliestTimeAtPosition was likewise wrongly seconds-float instead of epoch-milliseconds-int64. A real SDK client parsing this response got wrong timestamps in both places (silently, since floats decode into *int64 fields as zero, not an error). Field-diffed against types.FrontOfQueueDetail/FrontOfQueueJobSummary; QueueUtilization (optional) is not modeled -- this emulator doesn't track per-share-identifier fair-share utilization stats."}
  UpdateServiceJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW op (SDK bump). Mutates the REAL existing ServiceJob record created by SubmitServiceJob (b.serviceJobs table, keyed by jobId) -- not a fresh/parallel store. Only schedulingPriority is applied, matching UpdateServiceJobInput exactly (jobId + schedulingPriority, both required, no other fields exist on the real input). Rejects with ClientException when the job is already SUCCEEDED or FAILED (terminal), mirroring CancelJob's existing terminal-state guard on regular jobs; also bounds-checks schedulingPriority to the documented 0-9999 range. Covered by TestHandler_UpdateServiceJob (new table test in handler_service_jobs_test.go), including a describeservicejob round-trip proving the mutation lands on the same record."}
  CreateQuotaShare: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW op (SDK bump), part of the QuotaShare family -- see families.QuotaShare below."}
  DescribeQuotaShare: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW op (SDK bump); see families.QuotaShare."}
  UpdateQuotaShare: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW op (SDK bump); see families.QuotaShare."}
  DeleteQuotaShare: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW op (SDK bump); see families.QuotaShare."}
  ListQuotaShares: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW op (SDK bump); see families.QuotaShare. Response item shape is types.QuotaShareDetail, which (unlike DescribeQuotaShareOutput) has NO tags field -- confirmed against deserializers.go's awsRestjson1_deserializeDocumentQuotaShareDetail case list."}
families:
  QuotaShare: "NEW family (SDK bump, v1.61.1 -> v1.68.0): full CRUD+List implemented for real this pass, field-diffed against aws-sdk-go-v2/service/batch@v1.68.0's CreateQuotaShareInput/Output, DescribeQuotaShareInput/Output, UpdateQuotaShareInput/Output, DeleteQuotaShareInput/Output, ListQuotaSharesInput/Output, and types.QuotaShareDetail/QuotaShareCapacityLimit/QuotaSharePreemptionConfiguration/QuotaShareResourceSharingConfiguration (types + serializers.go + deserializers.go + validators.go). QuotaShare is a DISTINCT top-level resource from SchedulingPolicy/FairsharePolicy/ShareIdentifier -- CreateQuotaShareInput has no schedulingPolicyArn or shareIdentifier field at all; it references an existing JobQueue directly via a required jobQueue name-or-ARN parameter (validated against b.lookupJQByNameOrARN, the same helper SubmitServiceJob uses -- an unknown queue is rejected with ClientException/NotFound, not silently accepted). ARN shape confirmed against the AWS API reference's CreateQuotaShare worked example: job-queue/{queueName}/quota-share/{quotaShareName} (nested under the job queue's own ARN, not a bare quota-share/{name} or a SchedulingPolicy-style scheduling-policy/{name}). Real AWS Batch additionally requires the referenced job queue to be in the VALID state before association; this emulator's job queues are always created VALID and never transition away from it (see statusValid in store.go), so that check, while implemented for correctness, is not currently reachable through this backend's own state machine. Enum fields (state, preemptionConfiguration.inSharePreemption, resourceSharingConfiguration.strategy) are validated against their real documented values (ENABLED/DISABLED; ENABLED/DISABLED; RESERVE/LEND/LEND_AND_BORROW) rather than accepted as arbitrary strings. DescribeQuotaShare vs ListQuotaShares: both are POST /v1/... body-based ops (no path templating -- confirmed against serializers.go's SplitURI calls), but their response envelopes differ -- DescribeQuotaShareOutput is a single QuotaShareDetail-shaped object PLUS a tags map; ListQuotaSharesOutput wraps a []types.QuotaShareDetail under \"quotaShares\" (plus nextToken) and QuotaShareDetail itself has no tags field at all. New store.Table (quotaShares, byRegion index) wired into Snapshot/Restore exactly like the seven pre-existing tables (see persistence.go); no snapshot version bump was needed since RestoreAll already resets any registered table absent from older snapshot data to empty (additive-only change, not a shape/meaning change to existing data)."
gaps:
  - "DescribeJobs (JobDetail) still does not model attempts/nodeDetails/ecsProperties/eksProperties(describe-side) -- these require simulating multi-node/ECS/EKS job execution details (per-attempt job execution, multi-node coordination, ECS/EKS placement), genuinely out of scope for an in-memory emulator this pass. Left un-implemented rather than faked (bd: file follow-up)"
  - "ContainerDetail (job-level, EKS-nested EksContainer/EksPodProperties) is missing a few leaf fields real AWS has (imagePullPolicy, imagePullSecrets on EKS container/pod types) -- spot-checked against the real serializer, not exhaustively field-by-field; low priority since these are pass-through config fields with no state-machine implications (bd: file follow-up, low priority)"
deferred: []
leaks: {status: clean, note: "janitor.go's advanceJobs/sweep* all take/release the coarse lockmetrics.RWMutex correctly; every new backend method added this pass (SubmitServiceJob, ListServiceJobs, buildJobContainerDetail, describeResourcesPaginated) follows the same lock-then-defer-unlock pattern; go test -race clean. No new reverse-index maps were introduced that require cascade-cleanup on delete."}
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
behavior, not a bug.

### Gopherstack-invented field DELETED this pass

`ServiceJob.ServiceEnvironment` (and the corresponding `"serviceEnvironment"`
wire field on `SubmitServiceJob`/`ListServiceJobs`/`DescribeServiceJob`) does
**not exist** in the real AWS Batch API. Confirmed against
`aws-sdk-go-v2/service/batch`'s `SubmitServiceJobInput` (fields: `JobName`,
`JobQueue`, `ServiceJobType`, `ServiceRequestPayload`, `ClientToken`,
`RetryStrategy`, `SchedulingPriority`, `ShareIdentifier`, `Tags`,
`TimeoutConfig` -- no `ServiceEnvironment` anywhere) and its
`DescribeServiceJobOutput`/`ServiceJobSummary` (field is `JobQueue`, the
ARN of the job queue the service job runs in). Real AWS Batch associates a
service environment with a job queue via the queue's
`ServiceEnvironmentOrder`, not per-job. Deleted the invented field and
rebuilt `SubmitServiceJob`'s signature/model around `jobQueue` +
`serviceJobType` + `serviceRequestPayload` (all now correctly required,
matching the real API) plus the previously entirely-unmodeled
`retryStrategy`/`timeoutConfig`/`schedulingPriority`/`shareIdentifier`.
Also fixed: every `ServiceJob` wire key was wrong (`serviceJobId`/
`serviceJobArn`/`serviceJobName` vs. the real `jobId`/`jobArn`/`jobName`) --
a real SDK client parsing any ServiceJob response got every field as its
zero value.

### Real bugs fixed this pass (2026-07-23)

See the `ops`/`families` notes above for the full list with SDK-type
citations; summary:

1. **`SubmitJob`/`DescribeJobs`: `Job.JobQueue` stored the queue's bare name,
   not its ARN** (`JobDetail.JobQueue` is documented as the ARN). Fixed by
   storing `jq.JobQueueArn` and re-keying the `byQueue` index off the ARN in
   `listJobIDsForQueue`, `DeleteJobQueue`, and `GetJobQueueSnapshot`.
2. **`ListConsumableResources`: wrong wire key** (`consumableResourceSummaryList`
   vs. real `consumableResources`) -- real clients always saw an empty list.
3. **`ListJobsByConsumableResource`: wrong response shape** (full `Job` vs.
   the real narrower `ListJobsByConsumableResourceSummary`, with
   `jobQueueArn`/`jobStatus` instead of `jobQueue`/`status`, plus `quantity`).
4. **`GetJobQueueSnapshot`: invented `timestamp` (seconds float) instead of
   real `lastUpdatedAt` (epoch-ms int64)**, and `earliestTimeAtPosition` was
   likewise seconds-float instead of epoch-ms-int64.
5. **`CreateSchedulingPolicy`/`UpdateSchedulingPolicy`: `fairsharePolicy`
   hardcoded to `nil`** in the handler regardless of caller input.
6. **`ListSchedulingPolicies`: wrong response shape** (full `SchedulingPolicy`
   vs. the real narrower `SchedulingPolicyListingDetail`, `{arn}` only); also
   added the previously-absent pagination.
7. **`CreateServiceEnvironment`: `CapacityLimits` (a REQUIRED field on both
   the real create input and the detail output) was missing from the model
   entirely** -- silently dropped on every create.
8. **`ServiceJob` family: invented `ServiceEnvironment` field/parameter with
   no basis in the real API, plus every wire key wrong.** Full rewrite; see
   above.
9. **`RegisterJobDefinition`: `retryStrategy` and `eksProperties` were
   accepted by the backend but hardcoded to `nil`/absent in the handler**
   (gap #3 and #4 from the prior pass).
10. **`DescribeJobs`: `container`/`isCancelled`/`isTerminated`/
    `platformCapabilities` were unmodeled** (gap #1 from the prior pass);
    now derived/tracked.
11. **`ConsumableResourceProperty.Quantity` was `float64`**; real
    `ConsumableResourceRequirement.Quantity` is `int64` (gap #2 from the
    prior pass).
12. **`CreateJobQueue`/`UpdateJobQueue`: `jobQueueType` and
    `serviceEnvironmentOrder` were entirely unmodeled** (needed for
    SAGEMAKER_TRAINING service-job queues); added, with the documented
    "can't mix computeEnvironmentOrder and serviceEnvironmentOrder"
    validation.

### Verified NOT bugs ("looks wrong but correct")

- `DeleteComputeEnvironment`/`DeleteJobQueue` requiring `DISABLED` state before
  deletion matches documented AWS Batch behavior; not a false strictness bug.
- `CreateComputeEnvironment`/`CreateJobQueue` setting `Status: "VALID"`
  immediately (never modeling a `CREATING` transition) is a deliberate
  simplification that keeps SDK poll loops from hanging; `VALID` is a real
  reachable terminal status, so this is not a "stuck" bug.
- Job lifecycle (SUBMITTED → RUNNABLE/STARTING → RUNNING → SUCCEEDED) is
  actively advanced by `janitor.go`'s `advanceJobs`, not a disguised no-op.
  It collapses the intermediate RUNNABLE/STARTING states into a single sweep
  transition rather than stepping through each one individually; this is a
  deliberate simplification (documented in code), not a state-machine bug --
  every state the real API defines is still a reachable, observable value.
- `ListServiceJobs` defaulting to RUNNING-only when no `jobStatus` filter is
  given looks like a bug (a freshly-submitted SUBMITTED job won't show up)
  but is real, documented AWS Batch behavior: "If you don't specify a
  status, only RUNNING jobs are returned."

### SDK bump pass (2026-07-25): 6 new operations

The installed `aws-sdk-go-v2/service/batch` module was bumped from v1.61.1 to
v1.68.0, adding 6 operations this backend had neither implemented nor
acknowledged: `CreateQuotaShare`, `DescribeQuotaShare`, `UpdateQuotaShare`,
`DeleteQuotaShare`, `ListQuotaShares` (the new QuotaShare resource family) and
`UpdateServiceJob`. All 6 are implemented for real (routing, backend state,
request parsing, wire-accurate responses, error codes, and
Snapshot/Restore persistence) -- see the `ops`/`families` entries above for
full field-diff citations. No existing (pre-bump) operation was re-audited or
modified this pass beyond what was needed to add `UpdateServiceJob` (which
mutates `ServiceJob`, an existing model) -- `SubmitServiceJob`,
`DescribeServiceJob`, `ListServiceJobs`, and `TerminateServiceJob` are
unchanged from the prior pass's audit.

Key correctness point verified for `UpdateServiceJob`: it mutates the same
`ServiceJob` record `SubmitServiceJob` created (looked up by `jobId` in
`b.serviceJobs`), not a new parallel record -- proven in
`TestHandler_UpdateServiceJob`'s `update_existing` case via a
`describeservicejob` round-trip after the update. A job already in a
terminal status (`SUCCEEDED`/`FAILED`) rejects the update with
`ClientException`, matching the terminal-state guard `CancelJob` already
applies to regular jobs.

`QuotaShare` was checked against the possibility of being a disguised
extension of the existing `SchedulingPolicy`/`FairsharePolicy`/
`ShareIdentifier` model (this service already has fair-share scheduling) and
found to be a genuinely distinct, unrelated resource family: its create
input has no `schedulingPolicyArn` field, and its ARN nests under the
referenced `JobQueue`'s own ARN (`job-queue/{queue}/quota-share/{name}`)
rather than under a scheduling-policy resource. It does, however, reference
a real `JobQueue` (required `jobQueue` parameter, validated against
`b.lookupJQByNameOrARN` -- the same lookup `SubmitServiceJob` uses -- so an
unknown queue name is rejected rather than silently accepted).
