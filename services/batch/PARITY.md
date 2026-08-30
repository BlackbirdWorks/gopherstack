---
# 2026-08-21 gopherstack-r80d batch 16 (required-output cut): 6 required-response-
# member bugs found and fixed at member granularity -- JobDetail.StartedAt
# (DescribeJobs), DescribeServiceJobOutput.StartedAt, ComputeResource.MaxvCpus
# (DescribeComputeEnvironments), JobQueueDetail.ComputeEnvironmentOrder
# (DescribeJobQueues), QuotaShareCapacityLimit.MaxCapacity (Describe/
# ListQuotaShares), ServiceJobRetryStrategy.Attempts (DescribeServiceJob). See
# the dated notes on the affected ops below. last_audit_commit left as-is --
# this was a targeted required-output sweep, not a full re-audit.
service: batch
sdk_module: aws-sdk-go-v2/service/batch@v1.68.4
last_audit_commit: d7f71c4cd  # HEAD after the 2026-08-29 gopherstack-6flj/21my fresh sweep (ComputeEnvironment UnmanagedvCpus/ContainerOrchestrationType/Uuid); prior aad420594 was the 2026-07-25 full audit
last_audit_date: 2026-08-29
overall: A            # SDK bump (v1.61.1 -> v1.68.0) added 6 new ops (QuotaShare CRUD+List, UpdateServiceJob); all 6 implemented for real this pass, no regressions in previously-audited ops
                       # 2026-08-29 (constrain-not-honoured sweep, uncommitted at write time): ListJobs.Filters,
                       # ListConsumableResources.Filters, and ListServiceJobs.MaxResults/NextToken/Filters were all
                       # unplumbed -- see the three op rows and list_jobs_filters_test.go.
ops:
  RegisterJobDefinition: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed timeout + consumableResourceProperties nesting (prior pass); this pass wired retryStrategy and eksProperties through the handler (both were previously hardcoded nil/absent)"}
  DescribeJobDefinitions: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed name:revision exact-match bug; bare-name still returns all revisions (matches AWS); retryStrategy now surfaced"}
  DeregisterJobDefinition: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateComputeEnvironment: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED (this session, gopherstack-6flj reverse-direction sweep): CreateComputeEnvironmentInput.UnmanagedvCpus (api_op_CreateComputeEnvironment.go -- \"only used for fair-share scheduling to reserve vCPU capacity for new share identifiers\") was a real request member this backend parsed nowhere at all (grep for UnmanagedvCpus across services/batch/*.go returned zero hits before this fix). Now parsed, stored, and echoed by DescribeComputeEnvironments. Also FIXED: types.ComputeEnvironmentDetail.ContainerOrchestrationType (\"ECS (default) or EKS\") and .Uuid (\"Unique identifier for the compute environment\") were both entirely unmodeled -- ContainerOrchestrationType is deterministic from whether EksConfiguration was set at creation, and Uuid is generated with github.com/google/uuid at creation like every other resource's Id in this service. Proven via Test_SDKRoundTrip_ComputeEnvironment_UnmanagedvCpus and Test_SDKRoundTrip_ComputeEnvironment_ContainerOrchestrationTypeAndUuid (wire_field_fixes_test.go, new file this session), confirmed failing pre-fix, restored."}
  DescribeComputeEnvironments: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED 2026-08-21 (gopherstack-r80d batch 16, required-output cut): ComputeResource.MaxvCpus is required (types/types.go) but ComputeResources.MaxvCpus (models.go) was tagged omitempty. The real CreateComputeEnvironmentInput's client-side validateComputeResource (validators.go) only rejects a nil MaxvCpus pointer, not zero, and this backend's own validateComputeResourcesForCreate (compute_environments.go) never checks MaxvCpus at all -- so a real client's aws.Int32(0) is a fully reachable, unvalidated state, not a bypass. Fixed by removing the omitempty tag. ComputeResource.Type is also required but was NOT counted: the real SDK's own validator rejects an empty Type string client-side (len(v.Type)==0), so no real client can ever send one -- left as omitempty, unreachable. Proven via a real aws-sdk-go-v2/service/batch client round trip (wire_output_required_r80d_test.go), hand-reverted/confirmed-failing/restored, md5sum-verified byte-identical. FIXED (this session): see CreateComputeEnvironment -- UnmanagedvCpus/ContainerOrchestrationType/Uuid now surfaced here too. STILL NOT modeled: EcsClusterArn (real infrastructure ARN for an ECS cluster this emulator never provisions; no documented/verifiable naming convention found to reproduce, unlike an ARN with a published grammar -- left disclosed, not fabricated) and Context (documented only as \"Reserved.\", no meaning to model)."}
  UpdateComputeEnvironment: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED (this session): UpdateComputeEnvironmentInput.UnmanagedvCpus (api_op_UpdateComputeEnvironment.go, same member/reasoning as CreateComputeEnvironment) was likewise parsed nowhere -- see CreateComputeEnvironment."}
  DeleteComputeEnvironment: {wire: ok, errors: ok, state: ok, persist: ok, note: "requires DISABLED state + no referencing queues before delete, matches AWS docs"}
  CreateJobQueue: {wire: ok, errors: ok, state: ok, persist: ok, note: "added jobQueueType + serviceEnvironmentOrder (were entirely unmodeled); rejects mixing computeEnvironmentOrder and serviceEnvironmentOrder, matching documented AWS constraint"}
  DescribeJobQueues: {wire: ok, errors: ok, state: ok, persist: ok, note: "surfaces jobQueueType/serviceEnvironmentOrder. FIXED 2026-08-21 (gopherstack-r80d batch 16, required-output cut): JobQueueDetail.ComputeEnvironmentOrder is required unconditionally (types/types.go), but JobQueue.ComputeEnvironmentOrder (models.go) was tagged omitempty. CreateJobQueueInput itself declares ComputeEnvironmentOrder and ServiceEnvironmentOrder mutually exclusive (api_op_CreateJobQueue.go doc comment), so a queue built purely from serviceEnvironmentOrder is a routine reachable state with a nil/empty ComputeEnvironmentOrder, not an edge case -- the required key vanished entirely instead of decoding as []. Fixed by removing the omitempty tag (job_queues.go's CreateJobQueue already always builds a non-nil orderCopy via make(), so this alone is enough for the create path; cloneJobQueueWithTags in job_queues.go was also hardened to normalize a nil ComputeEnvironmentOrder to [] defensively, guarding a stale pre-fix persisted snapshot). Proven via a real aws-sdk-go-v2/service/batch client round trip (wire_output_required_r80d_test.go), hand-reverted/confirmed-failing/restored, md5sum-verified byte-identical."}
  UpdateJobQueue: {wire: ok, errors: ok, state: ok, persist: ok, note: "added serviceEnvironmentOrder param"}
  DeleteJobQueue: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascades job cleanup via byQueue index, now correctly keyed by the queue's ARN (see SubmitJob note)"}
  SubmitJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "REAL BUG found and fixed this pass: Job.JobQueue stored the queue's bare NAME, but JobDetail.JobQueue is documented as the queue's ARN -- a real SDK client parsing DescribeJobs/ListJobsByConsumableResource got the wrong value on every job. Fixed by storing jq.JobQueueArn (matches the existing JobDefinition-stores-ARN pattern) and re-keying the byQueue index (jobsByQueueIdx) off the ARN throughout (listJobIDsForQueue, DeleteJobQueue, GetJobQueueSnapshot). Also: PlatformCapabilities now snapshotted from the resolved job definition at submit time (was entirely absent from the Job model)."}
  DescribeJobs: {wire: partial, errors: ok, state: ok, persist: ok, note: "gap #1 closed: container (derived from the job definition's ContainerProperties + ContainerOverrides, single-container jobs only), isCancelled/isTerminated (set by CancelJob/TerminateJob), and platformCapabilities are now modeled. Still NOT modeled: attempts (never populated -- this emulator doesn't simulate per-attempt retry execution), nodeDetails, ecsProperties, eksProperties (describe-side) -- these require simulating multi-node/ECS/EKS execution details, genuinely out of scope for an in-memory emulator; see items_still_open. FIXED 2026-08-21 (gopherstack-r80d batch 16, required-output cut): JobDetail.StartedAt is required unconditionally (types/types.go: 'The Unix timestamp ... for when the job was started ... This member is required'), but the jobDetail wire struct (handler_jobs.go) tagged it omitempty and passed through Job.StartedAt, which stays nil until the janitor (opt-in, never started in tests, ticks every 1 minute by default -- janitor.go) advances the job to RUNNING. Any real client calling DescribeJobs on a freshly-submitted job (SUBMITTED/PENDING/RUNNABLE/STARTING) saw the key vanish entirely. Fixed by changing jobDetail.StartedAt to a plain int64 (json:\"startedAt\", no omitempty) fed through the new int64OrZero(j.StartedAt) helper (handler.go), matching the existing CreatedAt convention on the same struct. Proven via a real aws-sdk-go-v2/service/batch client round trip calling DescribeJobs immediately after SubmitJob (wire_output_required_r80d_test.go), hand-reverted/confirmed-failing/restored, md5sum-verified byte-identical."}
  ListJobs: {wire: partial, errors: ok, state: ok, persist: ok, note: "gopherstack-2wvq (2026-08-22): demands jobQueue unconditionally, but ListJobsInput marks NOTHING required (no validateOpListJobsInput exists in validators.go, unlike ListJobsByConsumableResource which has one) and documents jobQueue/arrayJobId/multiNodeJobId as mutually-exclusive alternates (api_op_ListJobs.go: 'You must specify only one of the following items'). arrayJobId/multiNodeJobId are unmodeled -- this backend has no array-job or multi-node-job child-record model to serve them from (SubmitJob stores ArrayProperties.Size but never spawns child Job records; NodeProperties, the job-definition-side MNP config, has no corresponding per-node Job records either -- confirmed zero hits for ArrayJobId/MultiNodeJobId/ParentJob anywhere in services/batch/). Adding the two fields without that model first would return an empty list for a genuine array/MNP submission -- a confidently-wrong 200, the exact class this issue exists to prevent -- so declined as a feature (child-job spawning at SubmitJob time, new indexes, ArrayPropertiesSummary/NodePropertiesSummary response fields, persisted-model version bump), not a validation deletion. 2026-08-23 (batch7): FIXED the other half -- ListJobs did not default to RUNNING-only when jobStatus was unspecified, though the real API documents exactly that default (api_op_ListJobs.go: 'If you don't specify a status, only RUNNING jobs are returned'), worded almost identically to ListServiceJobs's doc, which already implemented it correctly (service_jobs.go:149). Applied the same wantStatus-defaulting pattern in jobs.go's ListJobs. TestHandler_ListJobs_NoQueue previously asserted the opposite (wrong) behavior for an unfiltered call on a freshly-SUBMITTED job -- corrected to assert empty unfiltered and non-empty with an explicit SUBMITTED filter; five other tests/call sites that implicitly depended on the old all-statuses default (persistence_test.go x2, isolation_test.go, two handler_jobs_test.go cases, test/integration/batch_test.go's ListJobsAllQueues) updated to filter explicitly. Hand-reverted via cp, confirmed TestHandler_ListJobs_NoQueue fails against unfixed code (SUBMITTED job appears with no filter), restored, md5sum-verified byte-identical. FIXED 2026-08-29 (constrain-not-honoured sweep): ListJobsInput.Filters ([]types.KeyValuesPair -- JOB_NAME/JOB_DEFINITION/BEFORE_CREATED_AT/AFTER_CREATED_AT/SHARE_IDENTIFIER, api_op_ListJobs.go) was never plumbed at all -- listJobsInput had no Filters field, so a real client's Filters was silently dropped and the jobStatus-default-RUNNING behavior always applied even when Filters was set. Real AWS documents that supplying Filters makes jobStatus ignored except for a SHARE_IDENTIFIER-only filter set. Fixed: handler_jobs.go now decodes filters into a shared jobs.go KeyValueFilter, ListJobs applies JOB_NAME (case-insensitive, trailing '*' prefix)/JOB_DEFINITION (ARN exact or name prefix)/SHARE_IDENTIFIER (exact)/BEFORE_CREATED_AT/AFTER_CREATED_AT (epoch-ms), AND across filter entries, OR within one entry's Values, and applies the status-ignored-unless-SHARE_IDENTIFIER-only rule. Proven via Test_SDKRoundTrip_ListJobs_Filters (list_jobs_filters_test.go), confirmed failing pre-fix (0 results due to jobStatus still defaulting to RUNNING against SUBMITTED test jobs), then failing for the right reason after removing that first bug (filter simply never applied)."}
  TerminateJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "sets IsTerminated"}
  CancelJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "sets IsCancelled"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateConsumableResource: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteConsumableResource: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeConsumableResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateConsumableResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "gap #2 closed: ConsumableResourceProperty.Quantity is now int64, matching types.ConsumableResourceRequirement.Quantity (a Long) exactly"}
  ListConsumableResources: {wire: ok, errors: ok, state: ok, persist: ok, note: "REAL BUG found and fixed this pass: wire key was \"consumableResourceSummaryList\"; real ListConsumableResourcesOutput key is \"consumableResources\" -- a real SDK client always saw an empty list. Also added maxResults/nextToken pagination (previously absent) and narrowed the response item shape to match types.ConsumableResourceSummary (no tags/createdAt on this op, unlike DescribeConsumableResource). FIXED 2026-08-29 (constrain-not-honoured sweep): ListConsumableResourcesInput.Filters (CONSUMABLE_RESOURCE_NAME, case-insensitive with trailing '*' prefix -- api_op_ListConsumableResources.go) had no counterpart in listConsumableResourcesInput at all -- never plumbed through, so a real client's Filters was silently dropped and every call returned the full unfiltered list. Fixed via the same shared KeyValueFilter/filterValueMatches machinery as ListJobs. Proven via Test_SDKRoundTrip_ListConsumableResources_Filters (list_jobs_filters_test.go), confirmed failing pre-fix (2 results instead of 1)."}
  ListJobsByConsumableResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "REAL BUG found and fixed this pass: returned the full Job shape under \"jobs\"; real ListJobsByConsumableResourceOutput.Jobs is []ListJobsByConsumableResourceSummary, a narrower/differently-named shape (jobQueueArn not jobQueue, jobStatus not status, plus quantity -- the requested amount of the queried resource). Added maxResults/nextToken pagination. RULED OUT 2026-08-21 (gopherstack-r80d batch 16, required-output cut): ListJobsByConsumableResourceSummary.ConsumableResourceProperties is required (types/types.go) even for a job with none, but listJobsByConsumableResourceSummary (handler_consumable_resources.go) tags it omitempty. Not a bug in practice: this op's own backend filter, jobReferencesConsumableResource (consumable_resources.go), requires j.ConsumableResourceProperties != nil before a job is ever included in the result set, so no job this op returns can have a nil ConsumableResourceProperties -- the omitempty is dead code, not a reachable drop. Left as-is; documented rather than changed since no real client can observe a difference."}
  CreateSchedulingPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "REAL BUG found and fixed this pass: handler hardcoded fairsharePolicy to nil regardless of what the caller sent -- SchedulingPolicy backend already accepted/stored it, only the handler wiring was missing. gopherstack-6flj (this session): a SECOND real bug in the same op -- quotaSharePolicy (types.QuotaSharePolicy, a real alternative to fairsharePolicy, distinct from the separate top-level QuotaShare resource family) was parsed nowhere at all. Now modeled end to end (request parse, SchedulingPolicy.QuotaSharePolicy storage, Describe echo)."}
  DeleteSchedulingPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeSchedulingPolicies: {wire: ok, errors: ok, state: ok, persist: ok, note: "field-diffed against types.SchedulingPolicyDetail (arn/name/fairsharePolicy/tags) -- matches. gopherstack-6flj (this session): re-diffed against the current SDK's SchedulingPolicyDetail, which gained a fifth member, quotaSharePolicy, since this note was written -- was entirely unmodeled (a coverage gap in the prior field-diff, not argued-away); now emitted."}
  ListSchedulingPolicies: {wire: ok, errors: ok, state: ok, persist: ok, note: "REAL BUG found and fixed this pass: returned the full SchedulingPolicy shape; real ListSchedulingPoliciesOutput.SchedulingPolicies is []SchedulingPolicyListingDetail, which has only \"arn\" (no name/fairsharePolicy/tags -- callers use DescribeSchedulingPolicies for those). Added maxResults/nextToken pagination (previously absent)."}
  UpdateSchedulingPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "REAL BUG found and fixed this pass: handler hardcoded fairsharePolicy to nil, same class of bug as CreateSchedulingPolicy. gopherstack-6flj (this session): quotaSharePolicy was likewise parsed nowhere on Update -- now applied the same way fairsharePolicy already was (nil means \"leave unchanged\", matching this op's existing partial-update semantics)."}
  CreateServiceEnvironment: {wire: ok, errors: ok, state: ok, persist: ok, note: "REAL BUG found and fixed this pass: CapacityLimits was missing from the ServiceEnvironment model entirely, even though it's a REQUIRED field on both CreateServiceEnvironmentInput and ServiceEnvironmentDetail -- a real SDK client's CapacityLimits was silently dropped on every create (confirmed: test/integration/batch_test.go's TestIntegration_Batch_ServiceEnvironmentLifecycle already sends CapacityLimits and never verified it round-tripped). Now required and validated (ErrValidation if empty)."}
  DeleteServiceEnvironment: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeServiceEnvironments: {wire: ok, errors: ok, state: ok, persist: ok, note: "added maxResults/nextToken pagination (previously absent; real API supports it)"}
  UpdateServiceEnvironment: {wire: ok, errors: ok, state: ok, persist: ok, note: "added capacityLimits param"}
  SubmitServiceJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "FULL REWRITE this pass -- see families.ServiceJob below for the invented-field deletion and wire-shape fixes. gopherstack-6flj (this session): two more real request members, quotaShareName and preemptionConfiguration (types.ServiceJobPreemptionConfiguration), were parsed nowhere -- now modeled (ServiceJob.QuotaShareName/.PreemptionConfiguration)."}
  DescribeServiceJob: {wire: partial, errors: ok, state: ok, persist: ok, note: "see families.ServiceJob. gopherstack-6flj (this session): quotaShareName and preemptionConfiguration now echoed (see SubmitServiceJob). STILL NOT modeled: attempts/capacityUsage/latestAttempt/preemptionSummary -- these require simulating per-attempt SageMaker Training job execution and actual preemption events, genuinely out of scope for an in-memory emulator (same reasoning as DescribeJobs's disclosed attempts/nodeDetails gap above); not reclassified to ok. FIXED 2026-08-21 (gopherstack-r80d batch 16, required-output cut): two required members were dropped in the reachable pre-RUNNING/zero state, same root cause as DescribeJobs.StartedAt above. (1) DescribeServiceJobOutput.StartedAt is required unconditionally (api_op_DescribeServiceJob.go) but was tagged omitempty and nil until the janitor advances the service job; fixed the same way (plain int64, int64OrZero(sj.StartedAt)). (2) ServiceJobRetryStrategy.Attempts is required whenever RetryStrategy is present (types/types.go), but was tagged omitempty on a plain int32; the real SubmitServiceJobInput's client-side validateServiceJobRetryStrategy (validators.go) only rejects a nil Attempts pointer, not zero (the documented 1-10 range isn't enforced client-side), and this backend's SubmitServiceJob passes RetryStrategy through unvalidated -- so a real client's Attempts: aws.Int32(0) round-trips today with the key silently dropped on echo. Fixed by removing the omitempty tag. Both proven via real aws-sdk-go-v2/service/batch client round trips (wire_output_required_r80d_test.go), hand-reverted/confirmed-failing/restored, md5sum-verified byte-identical."}
  ListServiceJobs: {wire: ok, errors: ok, state: ok, persist: ok, note: "see families.ServiceJob; now filters by jobQueue (was serviceEnvironment) and defaults to RUNNING-only when jobStatus is unspecified, matching documented AWS behavior. gopherstack-6flj (this session): ServiceJobSummary's quotaShareName member was likewise unmodeled -- now emitted (ServiceJobSummary has no preemptionConfiguration member at all, confirmed via its own deserializer case list, so nothing else to add here). FIXED 2026-08-29 (constrain-not-honoured sweep): TWO bugs. (1) ListServiceJobsInput.MaxResults/NextToken were entirely unplumbed -- listServiceJobsInput had neither field, so ListServiceJobs always returned every matching service job in one response regardless of maxResults, an unbounded list for a resource with no natural cap. (2) Filters ([]types.KeyValuesPair -- JOB_NAME/SHARE_IDENTIFIER/QUOTA_SHARE_NAME/BEFORE_CREATED_AT/AFTER_CREATED_AT, api_op_ListServiceJobs.go) was likewise never plumbed, same bug class as ListJobs.Filters, including the documented status-ignored-unless-SHARE_IDENTIFIER-or-QUOTA_SHARE_NAME-only exception. Fixed: added maxResults/nextToken (existing paginateMapKeys helper, same pattern as ListJobs) and Filters via the shared KeyValueFilter machinery. Proven via Test_SDKRoundTrip_ListServiceJobs_MaxResultsAndFilters (list_jobs_filters_test.go), confirmed failing pre-fix on both the filter (0 results, since the unplumbed jobStatus default also excluded the SUBMITTED test jobs) and the pagination assertion."}
  TerminateServiceJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "input key fixed from \"serviceJob\" to \"jobId\", matching TerminateServiceJobInput exactly"}
  GetJobQueueSnapshot: {wire: partial, errors: ok, state: ok, persist: ok, note: "REAL BUG found and fixed this pass: response used an invented \"timestamp\" field (seconds, float64) instead of the real \"lastUpdatedAt\" (epoch-milliseconds, int64), and each job's earliestTimeAtPosition was likewise wrongly seconds-float instead of epoch-milliseconds-int64. A real SDK client parsing this response got wrong timestamps in both places (silently, since floats decode into *int64 fields as zero, not an error). Field-diffed against types.FrontOfQueueDetail/FrontOfQueueJobSummary; QueueUtilization (optional) is not modeled -- this emulator doesn't track per-share-identifier fair-share utilization stats. gopherstack-6flj (this session): re-checked GetJobQueueSnapshotOutput's full member set against the pinned SDK -- a THIRD top-level member, frontOfQuotaShares (types.FrontOfQuotaSharesDetail), is also entirely unmodeled and was not mentioned by the prior note at all (a coverage gap, not argued-away). Both frontOfQuotaShares and queueUtilization require simulating quota-share-based job ordering/capacity-usage accounting this backend doesn't do; left disclosed, not faked. STILL wire: partial for this reason, not reclassified to ok."}
  UpdateServiceJob: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW op (SDK bump). Mutates the REAL existing ServiceJob record created by SubmitServiceJob (b.serviceJobs table, keyed by jobId) -- not a fresh/parallel store. Only schedulingPriority is applied, matching UpdateServiceJobInput exactly (jobId + schedulingPriority, both required, no other fields exist on the real input). Rejects with ClientException when the job is already SUCCEEDED or FAILED (terminal), mirroring CancelJob's existing terminal-state guard on regular jobs; also bounds-checks schedulingPriority to the documented 0-9999 range. Covered by TestHandler_UpdateServiceJob (new table test in handler_service_jobs_test.go), including a describeservicejob round-trip proving the mutation lands on the same record."}
  CreateQuotaShare: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW op (SDK bump), part of the QuotaShare family -- see families.QuotaShare below."}
  DescribeQuotaShare: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW op (SDK bump); see families.QuotaShare. FIXED 2026-08-21 (gopherstack-r80d batch 16, required-output cut): QuotaShareCapacityLimit.MaxCapacity is required (types/types.go) but was tagged omitempty (models.go). The real CreateQuotaShareInput's client-side validateQuotaShareCapacityLimit (validators.go) only rejects a nil MaxCapacity pointer, not zero, and this backend's own CreateQuotaShare/UpdateQuotaShare (quota_shares.go) never validates MaxCapacity at all -- a real client's aws.Int32(0) is fully reachable. Fixed by removing the omitempty tag. QuotaShareCapacityLimit.CapacityUnit is also required but NOT counted: unlike MaxCapacity, this backend's own CreateQuotaShare/UpdateQuotaShare independently rejects an empty capacityUnit (quota_shares.go: `if cl.CapacityUnit == \"\"`), so no client, real or synthetic, can ever store one empty here -- left as omitempty, unreachable through this backend's own API (note: this is stricter validation than real AWS, which only rejects a nil pointer -- a separate, unfixed over-validation gap, not in scope for this required-output sweep). Proven via a real aws-sdk-go-v2/service/batch client round trip (wire_output_required_r80d_test.go), hand-reverted/confirmed-failing/restored, md5sum-verified byte-identical."}
  UpdateQuotaShare: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW op (SDK bump); see families.QuotaShare."}
  DeleteQuotaShare: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW op (SDK bump); see families.QuotaShare."}
  ListQuotaShares: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW op (SDK bump); see families.QuotaShare. Response item shape is types.QuotaShareDetail, which (unlike DescribeQuotaShareOutput) has NO tags field -- confirmed against deserializers.go's awsRestjson1_deserializeDocumentQuotaShareDetail case list. Shares the QuotaShareCapacityLimit.MaxCapacity fix described under DescribeQuotaShare above (same struct, same models.go tag)."}
families:
  QuotaShare: "NEW family (SDK bump, v1.61.1 -> v1.68.0): full CRUD+List implemented for real this pass, field-diffed against aws-sdk-go-v2/service/batch@v1.68.0's CreateQuotaShareInput/Output, DescribeQuotaShareInput/Output, UpdateQuotaShareInput/Output, DeleteQuotaShareInput/Output, ListQuotaSharesInput/Output, and types.QuotaShareDetail/QuotaShareCapacityLimit/QuotaSharePreemptionConfiguration/QuotaShareResourceSharingConfiguration (types + serializers.go + deserializers.go + validators.go). QuotaShare is a DISTINCT top-level resource from SchedulingPolicy/FairsharePolicy/ShareIdentifier -- CreateQuotaShareInput has no schedulingPolicyArn or shareIdentifier field at all; it references an existing JobQueue directly via a required jobQueue name-or-ARN parameter (validated against b.lookupJQByNameOrARN, the same helper SubmitServiceJob uses -- an unknown queue is rejected with ClientException/NotFound, not silently accepted). ARN shape confirmed against the AWS API reference's CreateQuotaShare worked example: job-queue/{queueName}/quota-share/{quotaShareName} (nested under the job queue's own ARN, not a bare quota-share/{name} or a SchedulingPolicy-style scheduling-policy/{name}). Real AWS Batch additionally requires the referenced job queue to be in the VALID state before association; this emulator's job queues are always created VALID and never transition away from it (see statusValid in store.go), so that check, while implemented for correctness, is not currently reachable through this backend's own state machine. Enum fields (state, preemptionConfiguration.inSharePreemption, resourceSharingConfiguration.strategy) are validated against their real documented values (ENABLED/DISABLED; ENABLED/DISABLED; RESERVE/LEND/LEND_AND_BORROW) rather than accepted as arbitrary strings. DescribeQuotaShare vs ListQuotaShares: both are POST /v1/... body-based ops (no path templating -- confirmed against serializers.go's SplitURI calls), but their response envelopes differ -- DescribeQuotaShareOutput is a single QuotaShareDetail-shaped object PLUS a tags map; ListQuotaSharesOutput wraps a []types.QuotaShareDetail under \"quotaShares\" (plus nextToken) and QuotaShareDetail itself has no tags field at all. New store.Table (quotaShares, byRegion index) wired into Snapshot/Restore exactly like the seven pre-existing tables (see persistence.go); no snapshot version bump was needed since RestoreAll already resets any registered table absent from older snapshot data to empty (additive-only change, not a shape/meaning change to existing data)."
gaps:
  - "DescribeJobs (JobDetail) still does not model attempts/nodeDetails/ecsProperties/eksProperties(describe-side) -- these require simulating multi-node/ECS/EKS job execution details (per-attempt job execution, multi-node coordination, ECS/EKS placement), genuinely out of scope for an in-memory emulator this pass. Left un-implemented rather than faked (bd: file follow-up)"
  - "ContainerDetail (job-level, EKS-nested EksContainer/EksPodProperties) is missing a few leaf fields real AWS has (imagePullPolicy, imagePullSecrets on EKS container/pod types) -- spot-checked against the real serializer, not exhaustively field-by-field; low priority since these are pass-through config fields with no state-machine implications (bd: file follow-up, low priority)"
  - "gopherstack-6flj (this session): GetJobQueueSnapshotOutput.frontOfQuotaShares and .queueUtilization (types.FrontOfQuotaSharesDetail/QueueSnapshotUtilizationDetail) are unmodeled -- both require simulating quota-share-based job ordering and per-share capacity-usage accounting this backend doesn't do (no scheduler groups RUNNABLE jobs by quota share or tracks utilization at all). frontOfQuotaShares was a previously-unflagged coverage gap in the prior audit's own field-diff note, which named only FrontOfQueueDetail/FrontOfQueueJobSummary and queueUtilization (bd: file follow-up)"
  - "gopherstack-6flj (this session): DescribeServiceJobOutput.attempts/capacityUsage/latestAttempt/preemptionSummary are unmodeled -- same root cause as DescribeJobs's disclosed attempts/nodeDetails gap above (no per-attempt execution simulation), plus preemptionSummary specifically requires this backend to actually preempt service jobs under quota-share contention, which it never does (bd: file follow-up)"
  - "2026-08-21 (gopherstack-r80d batch 16, required-output cut): four volume/logging/multi-node sub-features are entirely unmodeled on both the input and output side, so their own required members (EFSVolumeConfiguration.FileSystemId, S3FilesVolumeConfiguration.FileSystemArn, EksPersistentVolumeClaim.ClaimName, FirelensConfiguration.Type, NodePropertyOverride.TargetNodes, all required per types/types.go) can never be populated -- gopherstack's Volume/EksVolume/ContainerProperties/ContainerDetail structs (models.go) have no fields for EFS/S3/PVC volumes or Firelens log routing at all, and SubmitJob never accepts a nodeOverrides parameter. Verified structurally absent, not sampled: grepped models.go's Volume/EksVolume/ContainerProperties/ContainerDetail field lists directly against the real types.go members. Not new bugs -- consistent with the already-disclosed multi-node/ECS/EKS-describe-side gap above; naming the specific sub-structs here so a future pass doesn't re-derive this (bd: file follow-up, low priority)"
  - "gopherstack-2wvq (2026-08-22): ListJobs requires jobQueue unconditionally when the real API accepts jobQueue OR arrayJobId OR multiNodeJobId as mutually-exclusive alternates (api_op_ListJobs.go). Not a safe deletion: this backend has no array-job or multi-node-job child-record model at all (SubmitJob stores ArrayProperties.Size without spawning children; NodeProperties has no per-node Job records), so serving arrayJobId/multiNodeJobId would mean returning an empty list for a genuine array/MNP submission -- a confidently-wrong 200. Declined as a genuine feature (child-job spawning, new indexes, ArrayPropertiesSummary/NodePropertiesSummary, a persisted-model version bump), not attempted (bd: file follow-up)"
  - "gopherstack-6flj (this session): ComputeEnvironmentDetail.EcsClusterArn (the ARN of the underlying Amazon ECS cluster the compute environment uses) is unmodeled -- this emulator never provisions a real ECS cluster per compute environment, and no documented/verifiable AWS naming convention was found to reproduce (unlike an ARN with a published grammar this emulator can legitimately construct, e.g. WebACL.LabelNamespace in services/wafv2). Left disclosed rather than fabricated. ComputeEnvironmentDetail.Context is also unmodeled but is documented only as \"Reserved.\" with no meaning to model (bd: file follow-up, low priority)"
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

### gopherstack-6flj wrapper-key/nested-shape sweep (this session, 2026-08-15)

Picked via this issue's own method: read `services/_WRAPPER_KEY_SWEEP_REMAINDER.md`'s
header/tail, ran `go run ./cmd/opcensus` fresh (17 L+D+G, tied exactly with
`elasticbeanstalk`), read `bd show gopherstack-6flj` comments, read
`git show 9e0dfab44` (docdb, the pass immediately prior). Started on
`elasticbeanstalk` first (tied on the primary criterion, broken toward it on
total op count: 47 vs `batch`'s 45, mirroring the docdb-vs-elasticbeanstalk
tie-break the prior pass used) but a live sibling started editing
`services/elasticbeanstalk/*` mid-investigation -- `git status` showed 10
files (`environments.go`, `events.go`, `handler.go`,
`handler_application_versions.go`, `handler_environments.go`,
`handler_events.go`, `handler_instances_health.go`,
`handler_managed_actions.go`, `handler_platforms.go`, `models.go`) gain
uncommitted changes with zero edits made by this pass. Occupancy overrode
the pick (no hand-revert needed -- this pass made no edits to
elasticbeanstalk before the collision was noticed); moved to `batch`, the
other 17-op-tier service, confirmed clean via `git status`.

Protocol: `restjson1` (confirmed via `deserializers.go`'s
`awsRestjson1_deserializeOp*` function prefix, not `_PROTOCOLS.md` alone),
JSON body decode is case-SENSITIVE (Go struct `json:` tags matched via
`encoding/json`, not `strings.EqualFold`) -- unlike query/XML services, a
casing mismatch here is a real bug. Scripted key extraction both
directions: response (`deserializers.go`, `case "key":` switch arms inside
each `awsRestjson1_deserializeDocument*`/`*OpDocument*Output` function) and
request (`serializers.go`, `.Key("key")` calls inside each
`awsRestjson1_serializeDocument*`/`*OpDocument*Input` function), via the
same paren-balance-aware Python walker used elsewhere in this campaign,
adapted for restjson1's `map[string]interface{}` switch-over-key shape
instead of query/XML's `strings.EqualFold(t.Name.Local, ...)` shape. All 45
ops resolved `direct` from `GetSupportedOperations`; phantom-op check: zero
(diffed the SDK's 45 `api_op_*.go` op names 1:1 against
`GetSupportedOperations`'s own 45 entries, exact match).

This service already carried an exceptionally thorough prior audit
(`last_audit_commit` unchanged from the SDK-bump pass, `overall: A`, nearly
every op individually field-diffed with SDK-line citations and several
"REAL BUG found and fixed" entries) -- the top-level wrapper key on every
one of the 17 List/Describe/Get ops matched the real deserializer exactly
(no layer-1 bugs at all: `computeEnvironments`, `jobQueues`,
`jobDefinitions`, `jobSummaryList`, `jobs`, `tags`, `schedulingPolicies`,
`consumableResources`, `serviceEnvironments`, `quotaShares` all confirmed
against both the Go struct tags and the extracted deserializer key lists).
The real findings this pass were one layer deeper (this issue's own
"wrapper keys are mostly clean -- the bugs are one level deeper" standing
check held again):

1. **`SchedulingPolicyDetail.quotaSharePolicy` (types.QuotaSharePolicy) was
   entirely unmodeled** on `CreateSchedulingPolicy`, `UpdateSchedulingPolicy`,
   and `DescribeSchedulingPolicies` -- a real, distinct alternative to
   `fairsharePolicy` (confirmed via `types.SchedulingPolicyDetail`'s struct
   definition and both ops' serializer `.Key("quotaSharePolicy")` calls).
   NOT to be confused with the separate top-level `QuotaShare` resource
   family added in the SDK-bump pass -- `QuotaSharePolicy` is a
   single-field struct (`IdleResourceAssignmentStrategy`, real docs:
   "Currently, only FIFO is supported") that lives directly on
   `SchedulingPolicy`, the pre-existing resource. This is exactly the prior
   audit's own field-diff note going stale: it was written against an
   older `SchedulingPolicyDetail` shape (`arn`/`name`/`fairsharePolicy`/
   `tags`, 4 members) and never re-checked after the SDK bump added a 5th.
   Fixed: request parsing on both Create/Update, `SchedulingPolicy.
   QuotaSharePolicy` storage, `DescribeSchedulingPolicies` echo.
2. **`SubmitServiceJobInput.quotaShareName`/`.preemptionConfiguration`
   (types.ServiceJobPreemptionConfiguration) were entirely unmodeled** --
   real request members with zero backend wiring (grep for
   `QuotaShareName`/`PreemptionConfiguration` across `services/batch/*.go`
   returned zero hits before this pass). `PreemptionConfiguration` is a
   single-field struct (`PreemptionRetriesBeforeTermination *int32`,
   request-settable state, not execution-derived) -- distinct from the
   response-only `PreemptionSummary` (actual preemption history, disclosed
   below). Fixed: request parsing, `ServiceJob.QuotaShareName`/
   `.PreemptionConfiguration` storage, `DescribeServiceJob` echo (both
   fields) and `ListServiceJobs`'s narrower `ServiceJobSummary` echo
   (`quotaShareName` only -- confirmed `ServiceJobSummary` has no
   `preemptionConfiguration` member via its own deserializer case list).

DISCLOSED, not fabricated (kept in a separate list from the two fixes
above, each because it requires simulating execution/contention state this
in-memory emulator doesn't model, and inventing plausible values would be
exactly the fabrication this issue warns against):

- `GetJobQueueSnapshotOutput.frontOfQuotaShares`
  (types.FrontOfQuotaSharesDetail) and `.queueUtilization`
  (types.QueueSnapshotUtilizationDetail) -- both require grouping RUNNABLE
  jobs by quota share and tracking per-share capacity usage, which no
  scheduler in this backend does. `queueUtilization` was already disclosed
  by the prior audit; `frontOfQuotaShares` was NOT -- a coverage gap in
  that prior field-diff note (named only `FrontOfQueueDetail`/
  `FrontOfQueueJobSummary` and `queueUtilization`), not an
  argued-away/reconsidered item. Corrected in both the `ops:` entry and a
  new `gaps:` bullet.
- `DescribeServiceJobOutput.attempts`/`.capacityUsage`/`.latestAttempt` --
  same root cause as `DescribeJobs`'s already-disclosed
  `attempts`/`nodeDetails`/`ecsProperties`/`eksProperties` gap (no
  per-attempt execution simulation); extended the existing reasoning to
  `ServiceJob` rather than treating it as a new class of gap.
- `DescribeServiceJobOutput.preemptionSummary`
  (types.ServiceJobPreemptionSummary) -- response-only actual-preemption
  history; this backend never preempts a service job (no quota-share
  contention simulation), so the field can never have real content.
  `PreemptionConfiguration`, by contrast, is the request-driven
  *configuration* for that behavior and IS modeled (fix #2 above) -- the
  two are easy to conflate by name alone; distinguished explicitly via
  in-code comment on `describeServiceJobOutput` and in the `ServiceJob`
  model.

Go kinds checked: `QuotaSharePolicy.IdleResourceAssignmentStrategy` is a
bare string (real type is a single-value string enum, `FIFO` only) --
accepted and stored verbatim, not validated against that one value, matching
this file's existing precedent of not enum-validating `FairsharePolicy`'s
own string-typed sibling fields. `ServiceJobPreemptionConfiguration.
PreemptionRetriesBeforeTermination` is `*int32` (nil is a real, distinct
"unlimited retries" value per the SDK's own doc comment, not just "unset");
modeled as a pointer, not a bare `int32` defaulting to `0`, to preserve that
distinction.

Required-member diffs: none of the four new/echoed members
(`quotaSharePolicy`, `quotaShareName`, `preemptionConfiguration`,
`preemptionConfiguration.preemptionRetriesBeforeTermination`) are
`// This member is required.` per the SDK's own doc comments -- scoped
explicitly, all optional.

Symmetric pair checked, confirmed correct rather than a trap missed:
`ServiceJob.ShareIdentifier` (pre-existing, a `SchedulingPolicy`'s
`FairsharePolicy.ShareDistribution` share label) vs. the new
`QuotaShareName` (a `QuotaShare` resource's name) -- genuinely two
different association mechanisms for two different scheduling-policy
types, both real, both now correctly modeled independently; not a
duplicate/renamed field.

TESTS: `services/batch/handler_sdk_roundtrip_test.go` (new), two tests
using the real `aws-sdk-go-v2/service/batch` client against an in-process
`httptest.Server` (mirrors `services/docdb`'s established
`handler_sdk_roundtrip_test.go` pattern for this campaign, not the
`map[string]any`-decoding `post()` helper most of this package's other
tests use, per this issue's "SDK's own types" requirement):
`Test_SDKRoundTrip_SchedulingPolicy_QuotaSharePolicy` (Create with
`QuotaSharePolicy` set, Describe confirms it round-trips, Update applies a
new value, re-Describe confirms) and
`Test_SDKRoundTrip_ServiceJob_QuotaShareAndPreemption` (SubmitServiceJob
with both new fields, DescribeServiceJob and ListServiceJobs both confirmed
-- the list assertion required explicitly passing
`JobStatus: types.ServiceJobStatusSubmitted`, since `ListServiceJobs`
defaults to RUNNING-only per this file's own already-documented "Verified
NOT bugs" note, and a freshly-submitted job is SUBMITTED, not RUNNING; the
test would have silently asserted against an empty list otherwise -- caught
by re-reading that existing note before writing the assertion, not by a
failing run). Existing `persistence_test.go` coverage extended in place
(not a new file) for both new fields' Snapshot/Restore round-trip:
`TestInMemoryBackend_SnapshotRestore_FullState`'s existing
`CreateSchedulingPolicy`/`SubmitServiceJob` calls now also set
`QuotaSharePolicy`/`QuotaShareName`+`PreemptionConfiguration`, with new
post-restore assertions. `isolation_test.go`'s three existing
`CreateSchedulingPolicy` call sites updated for the new 5th parameter
(passing `nil`, unrelated to what that test verifies).

GATES: **NOT independently confirmed this pass** -- the Bash tool became
unavailable partway through this pass (every invocation, including
trivial ones like `echo`/`pwd`, returned a bare failure with no stdout/stderr)
and did not recover before this pass had to report out. All edits were
instead verified by hand: re-reading every changed section in full via the
Read tool for brace/field/type-name correctness, cross-checking every new
SDK type/enum constant used in the new test file (`types.QuotaSharePolicy`,
`types.QuotaShareIdleResourceAssignmentStrategyFifo`,
`types.ServiceJobPreemptionConfiguration`, `types.ServiceJobTypeSagemaker
Training`, `types.CETypeManaged`) against the pinned SDK's own
`types/enums.go`/`types/types.go` source via Read (not from memory), and
manually tracing every call site of the two signature changes
(`CreateSchedulingPolicy`, `UpdateSchedulingPolicy`, `SubmitServiceJob`) to
confirm each was updated consistently. This is a disclosed exception, not a
silent gap: `go build`/`go vet`/`go test -race`/`go fix -diff`/
`golangci-lint run` for `services/batch/...` and `./pkgs/...` were NOT run
by this pass and must be run (and any resulting fix applied) before this
work is considered done.

## 2026-08-29 gopherstack-6flj/21my fresh sweep (Step 0: prior campaign tags do NOT mean done)

This service already carried an extensive `gopherstack-r80d`/`2wvq`/`6flj`
history (see the 2026-08-15 wrapper-key sweep section above) but no
`wire_field_fixes*_test.go` existed yet (the file this session creates is
new). Swept anyway, per protocol; the last full sweep's own GATES section
disclosed it had NOT independently confirmed `go build`/`go vet`/
`go test -race`/`golangci-lint` due to a tooling outage -- ran all of those
fresh this session as the first step (all passed clean on the pre-existing
code, confirming that pass's uncommitted work was sound before adding to it).

Protocol re-confirmed (not trusted from memory): `awsRestjson1_` deserializer
prefix, path-based POST routing under `/v1/` (`serializers.go`). Dispatch
table diffed 1:1 against the pinned SDK's 45 `api_op_*.go` stems: exact
match (three ops -- `ListTagsForResource`/`TagResource`/`UntagResource` --
are referenced via package constants rather than literal strings in
`GetSupportedOperations`, confirmed by resolving those constants).

Tools run fresh (`enumcheck`, `acceptguard`, `zeroguard`, `xmlitemwrap`):
zero findings for `services/batch/` from any of the four.

Write-only-state sweep, both directions, focused on `ComputeEnvironment`
(the least recently re-audited resource family per the dated notes above --
`RegisterJobDefinition`/`ServiceJob`/`SchedulingPolicy`/`GetJobQueueSnapshot`
all had recent per-field passes; `ComputeEnvironment` last had only the
`MaxvCpus` required-output fix):

- Forward direction (fields the backend persists but never reads back):
  none found -- every `ComputeEnvironment` field this backend stores
  (`ComputeResources`, `EksConfiguration`, `UpdatePolicy`, `Tags`,
  `ServiceRole`, etc.) is already echoed by `DescribeComputeEnvironments`.
- Reverse direction (a Describe op not reading data a sibling Create/Update
  input accepts, or not deriving data this backend already tracks): **three
  real bugs**, all in `ComputeEnvironmentDetail`
  (`aws-sdk-go-v2/service/batch@v1.68.4` `types/types.go`), diffed member-
  by-member against `services/batch/models.go`'s `ComputeEnvironment`:
  1. **`UnmanagedvCpus`** -- a real member of both
     `CreateComputeEnvironmentInput` and `UpdateComputeEnvironmentInput`
     (`api_op_CreateComputeEnvironment.go`/`api_op_UpdateComputeEnvironment.go`:
     "the maximum number of vCPUs expected to be used for an unmanaged
     compute environment... only used for fair-share scheduling to reserve
     vCPU capacity for new share identifiers") that this backend parsed
     nowhere at all -- confirmed via a repo-wide grep for `UnmanagedvCpus`
     returning zero hits before this fix. A real client's value was
     silently dropped on both Create and Update, and never echoed.
  2. **`ContainerOrchestrationType`** -- "The orchestration type of the
     compute environment. The valid values are ECS (default) or EKS."
     Deterministic from whether `EksConfiguration` was set at creation (this
     backend already tracks that); computed once and stored rather than
     re-derived per Describe, since it cannot change after creation either.
  3. **`Uuid`** -- "Unique identifier for the compute environment." An
     opaque AWS-generated identifier this backend never modeled or
     generated, unlike every other resource's `Id`/ARN in this service.
     Generated once at creation with `github.com/google/uuid` (already an
     existing dependency here, used the same way by `SubmitJob`/
     `SubmitServiceJob`'s `jobID` generation).

  Two more real `ComputeEnvironmentDetail` members were checked and
  disclosed rather than fixed: `EcsClusterArn` (the underlying real ECS
  cluster's ARN -- this emulator never provisions one, and no
  documented/verifiable naming convention was found to legitimately
  reproduce it, unlike `LabelNamespace`'s published grammar in the wafv2
  half of this sweep) and `Context` (documented only as "Reserved.", no
  meaning to model). See the new `gaps` entry above.

Also field-diffed `ServiceEnvironmentDetail` and
`DescribeConsumableResourceOutput` in full against their models: no gaps
found (both match exactly).

Fixed by adding `UnmanagedvCpus *int32`, `ContainerOrchestrationType
string`, and `UUID string` (Go naming convention; wire tag stays
`json:"uuid,omitempty"` to match the real key) to `ComputeEnvironment`
(`models.go`); threading `UnmanagedvCpus` through
`createComputeEnvironmentInput`/`updateComputeEnvironmentInput`
(`handler_compute_environments.go`) and the `CreateComputeEnvironment`/
`UpdateComputeEnvironment` `StorageBackend` signatures (`compute_environments.go`
-- all call sites in `isolation_test.go`/`persistence_test.go` updated for the
new parameter); computing `ContainerOrchestrationType` and generating `UUID`
at creation time in `CreateComputeEnvironment`.

Proven via `wire_field_fixes_test.go`'s (new file this session)
`Test_SDKRoundTrip_ComputeEnvironment_UnmanagedvCpus` and
`Test_SDKRoundTrip_ComputeEnvironment_ContainerOrchestrationTypeAndUuid`,
each driving the real `aws-sdk-go-v2/service/batch` client; both confirmed
failing against unmodified code (captured in this session's transcript
before the fix was applied, not hand-reverted after the fact), then
passing after. Full `services/batch/...` suite green (`-race -count=1`)
after the fix; `golangci-lint run --fix` clean (0 issues after renaming
`Uuid` to `UUID` for a `revive` var-naming finding).

NOT independently re-verified this pass (ops unchanged, relying on the
extensive prior audit trail above): JobQueue, JobDefinition, Job,
ConsumableResource, SchedulingPolicy, QuotaShare, ServiceEnvironment, and
ServiceJob families beyond the `ComputeEnvironment` checks above.

## 2026-08-29 (pagination-arithmetic sweep, wrapper-key-sweep-rds-cloudwatch-sqs-sns branch)

Census: exactly one hand-rolled pagination path, and it is not really hand-rolled —
`paginateMapKeys` (`store.go`) delegates entirely to `pkgs/page.NewHMAC`, an HMAC-signed
offset token that `pkgs/page` itself clamps (`start >= len(all)` returns an empty page)
before ever slicing. `describeResourcesPaginated` (the shared "describe by explicit
names, else paginate over all region-scoped entries" generic used by
`DescribeComputeEnvironments`/`DescribeJobQueues`/`DescribeServiceEnvironments`) just
wraps `paginateMapKeys`. No equality-scan cursor, no independent arithmetic to audit.
Verdict: correct by construction (reuses the audited `pkgs/page` package), no bug found.

Added `pagination_arithmetic_test.go`: this service had no pagination test coverage at
all before this pass (raw-JSON or typed-client). New test drives
`DescribeComputeEnvironments` through the real `aws-sdk-go-v2` typed client: a boundary
walk (N=7, page=3, `assert.ElementsMatch` against the full set) plus a stale-cursor case
(take an offset token, delete every compute environment, resume with the stale token —
must return an empty page, not error or hang).

Gates: `go build`, `go vet`, `go test -race -count=1`, `golangci-lint run` — all clean
(`./services/batch/...`).

## 2026-08-30 (gopherstack-4shm WrapOp request-field re-scan, wrapper-key-sweep-rds-cloudwatch-sqs-sns branch)

batch dispatches every op through `service.WrapOp` (44 entries in `buildOps()`,
keyed by lowercase REST path, plus 3 tag ops handled outside that map
entirely). A field scan anchored on literal decode calls alone -- what
earlier campaign passes ran -- would resolve only `TagResource`'s own
`json.Unmarshal` and see **1 of 45 operations (2%)**: the rest of this
service was effectively invisible to that method, gopherstack-4shm's exact
class. The new `cmd/reqfieldscan` tool (built this pass, resolves
`WrapOp`'s second type parameter directly from each `handle*` function's
own signature) reaches **43 of 45 (96%)**; the remaining 2
(`ListTagsForResource`, `UntagResource`) are GET/DELETE requests with no
JSON body to decode at all, correctly unresolved rather than silently
dropped from the denominator.

**One real bug found and fixed**: `UpdateJobQueueInput.SchedulingPolicyArn`
(batch@v1.68.4 `api_op_UpdateJobQueue.go`: "Once a job queue is created,
the fair-share scheduling policy can be replaced but not removed") was
decoded and never passed to `InMemoryBackend.UpdateJobQueue` at all -- every
other field on that same call (`Priority`, `State`,
`ComputeEnvironmentOrder`, `JobStateTimeLimitActions`,
`ServiceEnvironmentOrder`) was threaded through correctly, this one alone
was dropped. Fixed by adding a `schedulingPolicyArn string` parameter to
the backend method (only overwrites when non-empty, matching "replaced but
not removed"). New test
`TestHandler_UpdateJobQueue_SchedulingPolicyArn`
(`handler_job_queues_test.go`) confirmed failing against unmodified code,
then passing; drives `DescribeJobQueues` afterward to assert on the
decoded value, not `err == nil`.

After the fix, `cmd/reqfieldscan -dir batch` reports **0 unread fields**
across all 43 resolved request types (161 fields). The prior clean verdicts
for `JobQueue`/`JobDefinition`/`Job`/`ConsumableResource`/
`SchedulingPolicy`/`QuotaShare`/`ServiceEnvironment`/`ServiceJob` families
(marked "not independently re-verified" in the 2026-08-29 entry above) now
have a mechanical re-scan behind them and hold, this one field excepted.

Gates: `go build`, `go vet`, `go test -race -count=1`, `golangci-lint run`
-- all clean (`./services/batch/...` and `./cmd/reqfieldscan/...`).
