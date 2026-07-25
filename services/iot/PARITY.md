---
service: iot
sdk_module: aws-sdk-go-v2/service/iot@v1.76.0
last_audit_commit: 135882ff405d549b4f7d65c71ade923a40c9fd7b
last_audit_date: 2026-07-25
overall: A-           # 2026-07-25 pass #1: fleet_indexing (previously entirely untouched)
                       # field-diffed and closed out, 2 real wire-shape bugs found+fixed;
                       # job_and_jobtemplate and device_defender remained genuinely partial
                       # (spot-checked, not exhaustively diffed -- large sub-surfaces).
                       # 2026-07-25 pass #2: closed the specifically-flagged AuditFinding gap
                       # (isSuppressed/reasonForNonComplianceCode/taskStartTime, plus the sibling
                       # reasonForNonCompliance field found in the same diff) and wired
                       # ListAuditFindings' checkName/taskId/listSuppressedFindings/time-range
                       # filters end to end -- while doing so, found ListAuditFindings was ALSO
                       # completely unreachable by a real client (routed on GET, real AWS sends
                       # POST); fixed. Separately, field-diffing job_and_jobtemplate's
                       # JobExecution/ListJobExecutionsForJob/ListJobExecutionsForThing found
                       # DescribeJobExecution/CancelJobExecution/DeleteJobExecution were ALSO
                       # completely unreachable by a real client (routed under
                       # /jobs/{jobId}/things/{thingName}[...], but real AWS paths them under
                       # /things/{thingName}/jobs/{jobId}[...]), and that
                       # ListJobExecutionsForJob/ForThing's response nesting was wrong shape
                       # entirely (flat fields instead of the real nested
                       # JobExecutionSummaryForJob/ForThing{jobExecutionSummary} objects) --
                       # both fixed, plus CancelJobExecution/DeleteJobExecution now implement
                       # real force/expectedVersion/statusDetails semantics (previously ignored
                       # entirely). Despite these real, substantial fixes, both
                       # job_and_jobtemplate (JobExecutionsRetryConfig read-path,
                       # presignedUrlConfig, jobProcessDetails rollup counts, schedulingConfig,
                       # maintenanceWindows, destinationPackageVersions, and the more
                       # foundational fact that this emulator never fans a QUEUED JobExecution
                       # out per target at CreateJob time) and device_defender
                       # (StartAuditMitigationActionsTask target resolution, ML-based detect
                       # models, violations, ListAuditFindings.resourceIdentifier filtering)
                       # still have real, substantial, unimplemented sub-surfaces -- both
                       # families stay `partial`, which is why this honestly stays at A-
                       # rather than A. See families: and deferred: below.
ops:
  CreateThing: {wire: ok, errors: ok, state: ok, persist: ok, note: "now accepts+wires billingGroupName (was silently dropped)"}
  DescribeThing: {wire: ok, errors: ok, state: ok, persist: ok, note: "now returns billingGroupName (was omitted entirely)"}
  UpdateThing: {wire: ok, errors: ok, state: ok, persist: ok, note: "AttributePayload.merge default was inverted (defaulted to merge; AWS defaults to replace) and empty-value attribute removal was missing -- both fixed via applyAttributePayload"}
  DeleteThing: {wire: ok, errors: ok, state: ok, persist: ok}
  ListThings: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateThingGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeThingGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "thingGroupMetadata.creationDate was a raw time.Time (RFC3339 string on the wire) instead of epoch-seconds; fixed via awstime.Epoch"}
  UpdateThingGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "same AttributePayload.merge bug as UpdateThing (handler didn't even parse merge from the request); fixed with the same applyAttributePayload helper"}
  DeleteThingGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  AttachPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "was appending without dedup, so double-attach produced duplicate ListAttachedPolicies entries; fixed with appendUnique (AWS attach ops are idempotent/set semantics)"}
  AttachPrincipalPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "same duplicate-entry bug as AttachPolicy; fixed"}
  AttachThingPrincipal: {wire: ok, errors: ok, state: ok, persist: ok, note: "same duplicate-entry bug; fixed"}
  AttachSecurityProfile: {wire: ok, errors: ok, state: ok, persist: ok, note: "same duplicate-entry bug; fixed. Also now returns ResourceNotFoundException for an unknown security profile name instead of silently succeeding (gopherstack-ep0r)"}
  DetachPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAttachedPolicies: {wire: ok, errors: ok, state: ok, persist: ok}
  CreatePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "creationDate/lastModifiedDate were raw time.Time (RFC3339 string) instead of epoch-seconds; fixed via awstime.Epoch"}
  DeletePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPolicies: {wire: ok, errors: ok, state: ok, persist: ok}
  CreatePolicyVersion: {wire: fixed, errors: ok, state: ok, persist: ok, note: "response was missing policyArn (real CreatePolicyVersionOutput has it); fixed"}
  GetPolicyVersion: {wire: fixed, errors: ok, state: ok, persist: ok, note: "used wrong date field name \"createDate\" (real GetPolicyVersionOutput uses \"creationDate\", verified against v1.76.0's awsRestjson1_deserializeOpDocumentGetPolicyVersionOutput -- \"createDate\" is only correct for the ListPolicyVersions summary shape) and was missing generationId/lastModifiedDate + epoch encoding; fixed, added GenerationID to the PolicyVersion domain type"}
  ListPolicyVersions: {wire: fixed, errors: ok, state: ok, persist: ok, note: "createDate was a raw time.Time; fixed via awstime.Epoch"}
  CreateTopicRule: {wire: ok, errors: ok, state: ok, persist: ok}
  GetTopicRule: {wire: fixed, errors: ok, state: ok, persist: ok, note: "rule.createdAt was a raw time.Time (RFC3339 string) instead of epoch-seconds; fixed via awstime.Epoch"}
  DeleteTopicRule: {wire: ok, errors: ok, state: ok, persist: ok}
  ReplaceTopicRule: {wire: ok, errors: ok, state: ok, persist: ok}
  EnableTopicRule: {wire: ok, errors: ok, state: ok, persist: ok}
  DisableTopicRule: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTopicRules: {wire: fixed, errors: ok, state: ok, persist: ok, note: "same createdAt epoch-encoding bug as GetTopicRule; fixed"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "Tag shape uses capitalized Key/Value JSON keys -- verified against real deserializer, this IS correct for IoT (not a bug, see Notes)"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeCertificate: {wire: fixed, errors: ok, state: ok, persist: ok, note: "was missing ownedBy/previousOwnedBy/generationId/certificateMode/customerVersion/validity/transferData (bd: gopherstack-jy57, now closed) and creationDate/lastModifiedDate were raw time.Time instead of epoch-seconds; fully field-diffed against v1.76.0 CertificateDescription and implemented"}
  ListCertificates: {wire: fixed, errors: ok, state: ok, persist: ok, note: "was returning the wrong summary shape (included lastModifiedDate, which real ListCertificates does NOT have; was missing certificateMode) plus the same epoch-encoding bug; fixed to match the real Certificate summary shape exactly (certificateArn/certificateId/certificateMode/creationDate/status). A pre-existing test (TestListCertificates_IncludesLastModifiedDate) asserted the WRONG shape -- rewritten as TestListCertificates_WireShape"}
  DescribeCertificateProvider: {wire: fixed, errors: ok, state: ok, persist: ok, note: "creationDate/lastModifiedDate were raw time.Time instead of epoch-seconds; fixed. Full field set (name/arn/lambdaFunctionArn/accountDefaultForOperations/creationDate/lastModifiedDate) verified against v1.76.0 -- no other gaps"}
  TransferCertificate: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "now accepts+stores transferMessage (was silently dropped) and records TransferDate for transferData"}
  AcceptCertificateTransfer: {wire: ok, errors: fixed, state: fixed, persist: ok, note: "was a near-total stub: wrote a bogus value into the wrong side of the certificateTransfers map for ANY certificate ID (including nonexistent ones), never validated PENDING_TRANSFER state, and never actually moved ownership or changed cert status. Fully reimplemented: validates the cert exists and is pending transfer (ResourceNotFoundException/InvalidRequestException), moves ownedBy -> previousOwnedBy chain, activates/deactivates per SetAsActive, and consumes the pending transfer"}
  RejectCertificateTransfer: {wire: ok, errors: fixed, state: fixed, persist: ok, note: "now requires PENDING_TRANSFER state (InvalidRequestException otherwise), accepts+stores rejectReason, and records TransferRejectDate"}
  CancelCertificateTransfer: {wire: ok, errors: fixed, state: fixed, persist: ok, note: "was an unconditional no-op success (didn't check the cert existed or was pending transfer, didn't revert cert status); now validates and reverts status to INACTIVE"}
  AssociateTargetsWithJob: {wire: ok, errors: fixed, state: fixed, persist: ok, note: "mutated jobTargets for any job ID without checking the job existed; now returns ResourceNotFoundException for an unknown job (gopherstack-ep0r)"}
  DescribeJobExecution: {wire: fixed, errors: ok, state: ok, persist: ok, note: "(2026-07-25 #2) was routed under /jobs/{jobId}/things/{thingName}, a path no real client sends (real AWS: /things/{thingName}/jobs/{jobId}, confirmed against serializers.go http bindings) -- completely unreachable by a real SDK client. Also leaked an invented \"thingName\" field instead of the real \"thingArn\", and was missing statusDetails/versionNumber/forceCanceled/approximateSecondsBeforeTimedOut entirely. Both fixed."}
  CancelJobExecution: {wire: fixed, errors: fixed, state: fixed, persist: ok, note: "(2026-07-25 #2) same routing bug as DescribeJobExecution, fixed. Also silently ignored force/expectedVersion/statusDetails entirely; now rejects an IN_PROGRESS cancel without force=true (InvalidStateTransitionException) and a mismatched expectedVersion (VersionConflictException), matching real CancelJobExecutionInput semantics"}
  DeleteJobExecution: {wire: fixed, errors: fixed, state: fixed, persist: ok, note: "(2026-07-25 #2) same routing bug (real path also carries an executionNumber URI segment), fixed. Also silently ignored force; now rejects deleting a non-terminal (QUEUED/IN_PROGRESS) execution without force=true"}
  ListJobExecutionsForJob: {wire: fixed, errors: ok, state: ok, persist: ok, note: "(2026-07-25 #2) response was flat {jobId,thingName,status} per entry; real ListJobExecutionsForJobOutput.executionSummaries is []JobExecutionSummaryForJob{thingArn, jobExecutionSummary:{...}} (confirmed against awsRestjson1_deserializeDocumentJobExecutionSummaryForJob) -- a real client's deserializer would have found none of the keys it looks for and returned entirely empty summaries. Fixed."}
  ListJobExecutionsForThing: {wire: fixed, errors: ok, state: ok, persist: ok, note: "same bug and fix as ListJobExecutionsForJob, for the sibling JobExecutionSummaryForThing{jobId, jobExecutionSummary:{...}} shape"}
  CancelAuditTask: {wire: ok, errors: fixed, state: fixed, persist: ok, note: "unconditionally set status to CANCELED for any task ID; now returns ResourceNotFoundException for an unknown task and InvalidRequestException if it isn't IN_PROGRESS (gopherstack-ep0r)"}
  CancelAuditMitigationActionsTask: {wire: ok, errors: fixed, state: fixed, persist: ok, note: "same class of bug as CancelAuditTask; fixed identically (gopherstack-ep0r)"}
  ListAuditFindings: {wire: fixed, errors: ok, state: ok, persist: ok, note: "(2026-07-25 #2) was routed on GET; real AWS's ListAuditFindings is POST /audit/findings with filters in a JSON body (confirmed against serializers.go http bindings) -- completely unreachable by a real SDK client. Also ignored every filter field entirely. Both fixed: now POST-routed and implements checkName/taskId/listSuppressedFindings/startTime/endTime filtering (resourceIdentifier filtering remains unimplemented -- see families: device_defender)"}
  DescribeAuditFinding: {wire: fixed, errors: ok, state: ok, persist: ok, note: "(2026-07-25 #2) AuditFinding was missing isSuppressed/reasonForNonComplianceCode/reasonForNonCompliance/taskStartTime entirely (confirmed against awsRestjson1_deserializeDocumentAuditFinding); all four now modeled. taskStartTime is auto-derived from the referenced AuditTask when the finding has a taskId but no explicit taskStartTime"}
  DescribeJob: {wire: fixed, errors: ok, state: ok, persist: ok, note: "documentSource was nested inside \"job\" instead of being a top-level DescribeJobOutput field (verified against v1.76.0); the nested Job object also leaked invented document/documentSource/tags fields that don't exist on real types.Job -- fixed (documentSource promoted to top level, invented fields tagged json:\"-\")"}
  DescribeJobTemplate: {wire: fixed, errors: ok, state: ok, persist: ok, note: "JobTemplate leaked an invented \"tags\" field not present in real DescribeJobTemplateOutput; tagged json:\"-\""}
  AttachPrincipalPolicy_and_11_other_handlers: {wire: fixed, errors: fixed, state: ok, persist: ok, note: "12 handlers (Attach*, AcceptCertificateTransfer, AddThingToBillingGroup/ThingGroup, AssociateSbomWithPackageVersion, AssociateTargetsWithJob, CancelAuditMitigationActionsTask, CancelAuditTask, DescribeEndpoint) returned a raw {\"error\":...} 500 body instead of h.handleError's {__type,message} shape on any backend error. Fixed in the prior pass; this pass found a DEEPER version of the same bug class affecting ~130 call sites (see error_handling family note below)"}
families:
  thing_core: {status: ok, note: "CreateThing/DescribeThing/UpdateThing/DeleteThing/ListThings audited field-by-field against v1.76.0 serializers/deserializers; 2 real bugs found+fixed (billingGroupName, AttributePayload merge default)"}
  thing_group: {status: ok, note: "Create/Describe/Update/Delete/List audited; UpdateThingGroup AttributePayload bug fixed (mirrors UpdateThing); DescribeThingGroup epoch-timestamp bug fixed this pass"}
  thing_type: {status: ok, note: "field-diffed this pass: CreateThingType/DescribeThingType/ListThingTypes/DeprecateThingType/UpdateThingType output shapes all verified against v1.76.0 (CreateThingTypeOutput, DescribeThingTypeOutput, ThingTypeMetadata, ThingTypeProperties). Epoch-timestamp bug (creationDate/deprecationDate) fixed. Only gap: optional mqtt5Configuration in thingTypeProperties not implemented (low-value MQTT5 user-property enrichment feature, not filed as a blocking gap)"}
  policy_attach: {status: ok, note: "Attach/Detach/List for both Policy and PrincipalPolicy audited; duplicate-entry bug on double-attach fixed across all 4 attach ops (Policy/PrincipalPolicy/ThingPrincipal/SecurityProfile); AttachSecurityProfile existence-validation gap fixed this pass"}
  policy_version: {status: ok, note: "CreatePolicyVersion/GetPolicyVersion/ListPolicyVersions field-diffed against v1.76.0 this pass: CreatePolicyVersion was missing policyArn (fixed), GetPolicyVersion used the wrong date field name \"createDate\" instead of \"creationDate\" and was missing generationId/lastModifiedDate (fixed), both had the epoch-timestamp bug (fixed)"}
  tags: {status: ok, note: "TagResource/UntagResource/ListTagsForResource verified real state mutation + correct Key/Value wire casing"}
  topic_rule: {status: ok, note: "Create/Get/Delete/Replace/Enable/Disable/List spot-checked against restjson1 deserializer field names (rule/ruleArn wrapper); epoch-timestamp bug on createdAt fixed this pass"}
  error_handling: {status: fixed, note: "Prior pass fixed 12 handlers that bypassed h.handleError entirely (raw {\"error\":...} 500). This pass found a deeper, broader version of the same bug class: respondErr (the error helper used by ~130 call sites across 21 \"batch2/batch3\" handler files) only recognized ErrResourceNotFound and ErrAlreadyExists -- every other sentinel (ErrThingNotFound, ErrCertificateNotFound, ErrThingGroupNotFound, ErrPolicyVersionNotFound, ErrVersionConflict, ErrDeleteConflict, ErrVersionsLimitExceeded, etc.) fell through to the wrong HTTP status/error code (400 InvalidRequestException or the 500 default instead of the correct 404/409). Fixed by extracting the canonical mapping into a single writeIoTError function shared by both respondErr and Handler.handleError, so every handler now maps every sentinel identically regardless of which helper it calls."}
  timestamps: {status: fixed, note: "NEW bug class found this pass, not previously flagged: Policy, GetPolicyOutput, PolicyVersion, ThingType, ThingGroup, Certificate, CertificateProvider, and TopicRule all stored creationDate/lastModifiedDate/deprecationDate as raw time.Time struct fields that were being embedded directly into map[string]any JSON responses -- json.Marshal renders a bare time.Time as an RFC3339 STRING, but restjson1's DateType wire format requires a JSON NUMBER of epoch seconds (confirmed against v1.76.0's awsRestjson1_deserialize* functions, which reject non-json.Number timestamp values with 'expected ... to be a JSON Number, got string instead'). This is the same bug class documented as previously fixed in sagemaker/glue/ssm. Fixed at every response call site via pkgs/awstime.Epoch(); the internal struct fields remain time.Time (only used for internal storage/persistence, not wire output) so no persistence-format changes were needed. Contrary to the PARITY.md note this superseded, NOT every part of this service already used the float64-epoch convention -- Job/JobTemplate/CACertificate/OutgoingCertificate/provisioning/packages/metrics did, but the 8 struct families above did not."}
  invented_fields: {status: fixed, note: "Job leaked \"tags\"/\"document\"/\"documentSource\" and JobTemplate leaked \"tags\" -- none of these exist on real types.Job/DescribeJobTemplateOutput (verified against v1.76.0's awsRestjson1_deserializeDocumentJob, which has no tags/document/documentSource cases; documentSource is real but only as a top-level DescribeJobOutput field, and document is only retrievable via the separate GetJobDocument operation). Fixed via json:\"-\" tags on the domain struct fields (kept for internal storage) plus promoting documentSource to the DescribeJobOutput top level."}
  certificate: {status: ok, note: "Full CRUD (Create/Register/RegisterWithoutCA/Describe/List/Update/Delete) plus the transfer lifecycle (Transfer/Accept/Reject/Cancel) field-diffed and fixed this pass -- see DescribeCertificate/ListCertificates/AcceptCertificateTransfer/etc. ops above and gopherstack-jy57 (now closed)"}
  certificate_provider: {status: ok, note: "Create/Describe/List/Update/Delete field-diffed against v1.76.0; only bug was the epoch-timestamp encoding on Describe (fixed). Full field set otherwise already correct"}
  job_and_jobtemplate: {status: partial, note: "AssociateTargetsWithJob existence-validation gap fixed (gopherstack-ep0r). DescribeJob/DescribeJobTemplate wire-shape bugs found+fixed a prior pass. (2026-07-25 #2) field-diffed JobExecution/ListJobExecutionsForJob/ListJobExecutionsForThing against v1.76.0 and found two severe, previously-undiscovered bugs: DescribeJobExecution/CancelJobExecution/DeleteJobExecution were routed under a path shape no real client ever sends (completely unreachable), and ListJobExecutionsForJob/ForThing returned the wrong response nesting entirely (a real client's deserializer would see empty summaries). Both fixed, plus JobExecution's statusDetails/versionNumber/forceCanceled/approximateSecondsBeforeTimedOut fields and CancelJobExecution/DeleteJobExecution's force/expectedVersion/statusDetails semantics, all previously unmodeled/ignored. STILL NOT implemented: Job's more advanced optional fields (jobExecutionsRetryConfig read-path, presignedUrlConfig, jobProcessDetails rollup counts, schedulingConfig, maintenanceWindows, destinationPackageVersions), and the more foundational fact that this emulator never fans a QUEUED JobExecution out per target at CreateJob time (CancelJobExecution's create-on-miss fallback is a workaround, not a substitute) -- large sub-surfaces, genuinely left for a future pass. See gopherstack-srzb."}
  device_defender: {status: partial, note: "CancelAuditTask/CancelAuditMitigationActionsTask existence+state validation gaps fixed a prior pass. (2026-07-25 #2) field-diffed AuditFinding against v1.76.0 and closed the previously-flagged gap: isSuppressed/reasonForNonComplianceCode/reasonForNonCompliance/taskStartTime were all missing, now modeled (taskStartTime auto-derived from the referenced AuditTask). Also found ListAuditFindings was routed on GET instead of the real POST (completely unreachable by a real client) and implemented its checkName/taskId/listSuppressedFindings/startTime/endTime filters, previously entirely unimplemented. STILL NOT implemented: ListAuditFindings.resourceIdentifier filtering (its real shape has ~15 optional per-check-type discriminator fields this emulator's synthetic NonCompliantResource map cannot honestly match against without guessing -- see ListAuditFindingsFilter's doc comment in audit.go), and the broader audit/mitigation/detect task families (StartAuditMitigationActionsTask target resolution, ML-based detect models, violations) NOT exhaustively field-diffed this pass. See gopherstack-srzb."}
  fleet_indexing: {status: ok, note: "Field-diffed against v1.76.0 this pass (previously entirely untouched). Two real, previously-unflagged wire-shape bugs found and fixed: (1) SearchIndex's ThingGroupDocument sent a single \"parentGroupName\" string (direct parent only) instead of the real \"parentGroupNames\" LIST field (the full ancestor chain) -- confirmed against awsRestjson1_deserializeDocumentThingGroupDocument, a real client's deserializer would never find the key it looks for under the old shape and silently leave the field empty; also added the missing \"thingGroupDescription\" field. (2) DescribeThingGroup's thingGroupMetadata was completely missing \"rootToParentThingGroups\" (root-first ancestor name+ARN list) -- confirmed against awsRestjson1_deserializeDocumentThingGroupMetadata; not implemented at all previously. Both fixed via a new thingGroupAncestors backend helper (indexing.go) that reconstructs the full chain by walking gopherstack's per-group direct-ParentGroupName links, since the domain model only stores one level per group. (3) GetStatistics' Statistics response was missing \"sumOfSquares\" entirely (types.Statistics has it; confirmed against awsRestjson1_deserializeDocumentStatistics) -- fixed by computing it in computeStatistics alongside the existing sum/variance accumulation. GetCardinality/GetPercentiles/GetBucketsAggregation/DescribeIndex/ListIndices output shapes also field-diffed against their real GetCardinalityOutput/GetPercentilesOutput/GetBucketsAggregationOutput/types.PercentPair/types.Bucket counterparts -- no further gaps found on this pass's sample."}
  billing_group: {status: ok, note: "AddThingToBillingGroup/RemoveThingFromBillingGroup/ListThingsInBillingGroup verified real state mutation via thingBillingGroups map; DescribeThing now surfaces it (see CreateThing/DescribeThing above)"}
  persistence: {status: ok, note: "backendSnapshot/Restore in persistence.go covers all backend maps observed during this audit (policyTargets, thingPrincipals, thingBillingGroups, thingThingGroups, securityProfileTargets, resourceTags, certificateTransfers, etc.); Handler.Snapshot/Restore already delegate correctly -- no gaps found. Certificate struct's new transfer-lifecycle fields (OwnedBy/PreviousOwnedBy/GenerationID/CertificateMode/CustomerVersion/Validity*/Transfer*) round-trip correctly since persistence marshals the full struct, not the handler-layer wire shape."}
gaps: []  # all previously-filed gaps (gopherstack-jy57, gopherstack-ep0r) closed; fleet_indexing
          # closed out a prior pass (3 real bugs found+fixed, see families:); the
          # specifically-flagged AuditFinding gap (isSuppressed/reasonForNonComplianceCode/
          # taskStartTime) closed 2026-07-25 pass #2, along with a severe routing bug on
          # DescribeJobExecution/CancelJobExecution/DeleteJobExecution/ListAuditFindings
          # (unreachable by any real client) and a wrong-shape bug on
          # ListJobExecutionsForJob/ForThing, both found while closing it. job_and_jobtemplate/
          # device_defender remain genuinely partial (real, substantial sub-surfaces still
          # unimplemented -- see families: above), tracked under gopherstack-srzb; this is why
          # overall stays A- rather than A despite gaps: being empty.
deferred:
  - job_and_jobtemplate (Job's advanced optional fields: jobExecutionsRetryConfig read-path,
    presignedUrlConfig, jobProcessDetails rollup counts, schedulingConfig, maintenanceWindows,
    destinationPackageVersions; plus the more foundational fact that this emulator never fans a
    QUEUED JobExecution out per target at CreateJob time -- 2026-07-25 pass #2 closed
    JobExecution's own wire shape, routing, and CancelJobExecution/DeleteJobExecution's
    force/expectedVersion/statusDetails semantics, but these larger items remain)
  - device_defender (audit/mitigation/detect task families beyond the two Cancel ops and
    ListAuditFindings fixed so far -- StartAuditMitigationActionsTask target resolution,
    ML-based detect models, violations, ListAuditFindings.resourceIdentifier filtering)
  - see gopherstack-srzb for the consolidated deferred-family tracking issue (updated this pass
    with current status; fleet_indexing removed from it a prior pass, now closed)
leaks: {status: found_and_fixed, note: "FOUND: Handler.StartWorker launched the embedded MQTT broker in a bare `go func(){ broker.Start(ctx) }()` with no way to wait for it to exit -- Handler didn't implement service.Shutdowner at all, so the broker goroutine had no deterministic drain path on service shutdown (relied entirely on the caller's ctx being cancelled elsewhere, with no join/wait). This is the same 'ctx-parented but not Shutdown-drained' bug class fixed elsewhere via pkgs/worker.SingleRun (see services/autoscaling, services/scheduler for the established pattern). FIXED: added a worker.SingleRun-backed brokerRun field, Broker.Run(ctx) adapter method, and a Handler.Shutdown(ctx) that calls brokerRun.Stop(ctx) and blocks until the broker goroutine actually exits (or ctx is done). Handler now implements both service.BackgroundWorker and service.Shutdowner. Regression test: TestHandlerShutdownDrainsBrokerGoroutine (broker_test.go) starts a real broker and asserts Shutdown returns within 2s of the goroutine actually stopping, not just cancelling and returning immediately."}
---

## Notes

- **IoT is restjson1, not XML** — the "wrong XML list wrappers" bug class from other
  services' parity sweeps doesn't apply here. List/object field names were verified
  directly against `deserializers.go`/`serializers.go` in
  `aws-sdk-go-v2/service/iot@v1.76.0`.
- **Timestamps — CORRECTED from the prior version of this note**: the prior audit pass
  claimed "the service stores/serializes epoch-seconds as `float64` struct fields... [so]
  it was not flagged as a bug." That claim was **only true for part of the service**
  (Job/JobTemplate/CACertificate/OutgoingCertificate/provisioning/packages/metrics). This
  pass found 8 struct families (Policy, GetPolicyOutput, PolicyVersion, ThingType,
  ThingGroup, Certificate, CertificateProvider, TopicRule) that stored `time.Time`
  directly and were marshaled straight into JSON responses — which `encoding/json`
  renders as an RFC3339 **string**, not the epoch-seconds **number** the restjson1
  `DateType` wire format requires. Real AWS SDK deserializers reject a string here
  outright (`"expected ... to be a JSON Number, got string instead"`). All 8 are now
  fixed via `pkgs/awstime.Epoch()` at the handler response-building call site (not by
  changing the struct field type, so no persistence-format changes were needed — the
  internal struct fields are only used for storage/computation, not direct wire
  marshaling). **If re-auditing timestamps in this service, grep for `time.Time` in
  `types.go` and verify every call site that embeds one of those fields into a
  `map[string]any` response wraps it in `awstime.Epoch(...)`.**
- **Looks-wrong-but-correct**: `ListTagsForResource`'s tag objects use capitalized
  `"Key"`/`"Value"` JSON keys (not lowercase `"key"`/`"value"`). Verified directly
  against `awsRestjson1_deserializeDocumentTag` in v1.76.0 — this is genuinely how IoT's
  `Tag` shape serializes, unlike the lowercase convention used by many other AWS
  RESTJSON services. Don't re-flag this.
- **AttributePayload merge semantics**: AWS IoT's `AttributePayload.merge` defaults to
  **false = replace**, not merge — confirmed against `moto`'s reference
  `update_thing`/`update_thing_group`. In both modes, an attribute given an **empty
  string value in the payload is deleted** from the result — this is AWS's documented
  "how to remove an attribute via UpdateThing" mechanism and applies to both UpdateThing
  and UpdateThingGroup. Both are implemented via the shared `applyAttributePayload`
  helper in `backend.go`. Don't revert the "replace is the default" direction without
  re-verifying against real AWS — it's non-obvious and easy to get backwards.
- **Attach op idempotency**: AWS IoT's various Attach* control-plane ops
  (AttachPolicy/AttachPrincipalPolicy/AttachThingPrincipal/AttachSecurityProfile) are
  **set semantics**, not list-append — attaching an already-attached target/principal is
  a no-op success, not a duplicate entry. Fixed via a shared `appendUnique` helper in
  `backend.go`.
- **Certificate transfer lifecycle** (new this pass): AWS IoT's cross-account cert
  transfer is a real state machine — `TransferCertificate` sets `PENDING_TRANSFER` and
  records `(targetAccount, transferMessage)`; `AcceptCertificateTransfer` must validate
  the cert exists AND is `PENDING_TRANSFER`, then moves `ownedBy` → `previousOwnedBy`
  and activates/deactivates per `SetAsActive`; `RejectCertificateTransfer` and
  `CancelCertificateTransfer` both require `PENDING_TRANSFER` and revert to `INACTIVE`
  (with a reject reason recorded for Reject). The certificate's `transferData` wire
  object (`transferDate`/`transferMessage`/`acceptDate`/`rejectDate`/`rejectReason`) is
  only present once a transfer has actually been initiated — real
  `CertificateDescription.transferData` is unset for certs that were never transferred.
  If re-auditing: `AcceptCertificateTransfer` previously validated NOTHING and wrote into
  the *pending-transfers map itself* keyed by any certificate ID (even nonexistent ones)
  — that bug is what `TestCertTransferCount`/`TestPersistenceWithAssociations` exercised
  before this pass; both tests were rewritten to use real certs + real transfers.
- **Persistence is comprehensive**: `persistence.go`'s `backendSnapshot` already covers
  every backend map touched during this audit (including `certificateTransfers`,
  `thingBillingGroups`, `policyTargets`, `thingPrincipals`, `securityProfileTargets`).
  No Handler-level Snapshot/Restore gap was found — `Handler.Snapshot`/`Restore` already
  delegate correctly to the backend when it implements `Snapshottable`.
- **Scope of the 2026-07-23 pass** (commit `135882ff`): closed both previously-filed
  gaps (gopherstack-jy57, gopherstack-ep0r), fixed a real goroutine-leak bug in the
  embedded MQTT broker's Shutdown path, fixed a systemic error-code-mapping bug across
  ~130 call sites, fixed a systemic epoch-timestamp encoding bug across 8 struct
  families, fully closed out `certificate`/`certificate_provider`/`thing_type`/
  `policy_version` families, and made partial progress on `job_and_jobtemplate` and
  `device_defender`. `fleet_indexing` was left entirely untouched, which is why that
  pass's grade stopped at B+.
- **Scope of this pass (2026-07-25)**: closed out `fleet_indexing`, the family the prior
  pass explicitly left untouched. Field-diffed `SearchIndex` (both the `AWS_Things` and
  `AWS_ThingGroups` index result shapes), `DescribeThingGroup`'s `thingGroupMetadata`,
  and `GetCardinality`/`GetStatistics`/`GetPercentiles`/`GetBucketsAggregation` against
  `aws-sdk-go-v2/service/iot@v1.76.0`'s deserializers directly (not against
  gopherstack's own output, per parity-principles.md rule 2). Found and fixed 3 real,
  previously-unflagged wire-shape bugs (see `fleet_indexing`'s `families:` note above):
  a wrong-shaped/wrong-key `parentGroupNames` field and a missing
  `thingGroupDescription` field on `SearchIndex`'s `ThingGroupDocument` results, a
  completely absent `rootToParentThingGroups` field on `DescribeThingGroup`, and a
  missing `sumOfSquares` field on `GetStatistics`. Also spot-checked (not exhaustively
  diffed) `AuditFinding`'s wire shape in `device_defender`: its epoch-timestamp
  encoding (`findingTime`) is correct (already `float64` seconds, consistent with this
  service's Job/JobTemplate/CACertificate timestamp convention), but `isSuppressed`,
  `reasonForNonComplianceCode`, and `taskStartTime` are missing entirely from
  gopherstack's `AuditFinding` type -- left unfixed (large sub-surface, `device_defender`
  remains explicitly `partial`, tracked under gopherstack-srzb along with
  `job_and_jobtemplate`'s remaining advanced-field gaps). This is what justifies A-
  rather than a full A: `fleet_indexing` is now `ok`, but two families remain
  genuinely partial rather than exhaustively verified.
- **Scope of this pass (2026-07-25 #2)**: closed the specifically-flagged `AuditFinding`
  gap (`isSuppressed`/`reasonForNonComplianceCode`/`taskStartTime`) left by the pass
  above, field-diffing against `aws-sdk-go-v2/service/iot@v1.76.0`'s
  `awsRestjson1_deserializeDocumentAuditFinding` directly. Found a fourth real field in
  the same diff, `reasonForNonCompliance` (also entirely missing), and added it too.
  `taskStartTime` is auto-derived from the referenced `AuditTask`'s own `TaskStartTime`
  in `SeedAuditFinding` when a finding has a `taskId` but no explicit `taskStartTime`,
  rather than left unset or requiring every caller to redundantly pass it.

  While closing this, field-diffed `ListAuditFindings` (`ListAuditFindingsInput`) and
  found it was **routed on GET** — real AWS's `ListAuditFindings` is `POST
  /audit/findings` (its filter fields travel in a JSON body, confirmed against
  `serializers.go`'s `awsAwsjson11_serializeOpListAuditFindings`/http bindings), meaning
  the op was **completely unreachable by any real SDK client** before this pass (a real
  client's POST request would never match the GET-only route). This bug was invisible to
  every prior audit pass because the existing test (`TestAuditFinding`) issued a
  hand-constructed GET request that happened to match gopherstack's own (wrong) route,
  rather than going through a real generated SDK client — the exact "tests assert
  against gopherstack's own shape, not real AWS's" trap `parity-principles.md` rule 3
  warns about, just manifesting as a routing bug instead of a field-shape bug this time.
  Fixed the route (GET → POST) and implemented `checkName`/`taskId`/
  `listSuppressedFindings`/`startTime`/`endTime` filtering, previously entirely
  unimplemented (the handler ignored the request body altogether).
  `resourceIdentifier` filtering was deliberately left unimplemented: its real shape has
  roughly 15 optional per-audit-check-type discriminator fields (deviceCertificateId,
  caCertificateId, cognitoIdentityPoolId, iamRoleArn, ...), and this emulator's
  synthetic, freely-shaped `NonCompliantResource map[string]any` cannot honestly
  discriminate against them without guessing per-check-type semantics — see
  `ListAuditFindingsFilter`'s doc comment in `audit.go`.

  Separately, field-diffing `job_and_jobtemplate`'s `JobExecution` shape (explicitly
  flagged as not-yet-diffed by the prior pass) against
  `awsRestjson1_deserializeDocumentJobExecution` found an even more severe version of
  the same routing-bug class: `DescribeJobExecution`/`CancelJobExecution`/
  `DeleteJobExecution` were all routed under `/jobs/{jobId}/things/{thingName}[...]`, a
  path shape no real AWS SDK client has ever sent — real AWS paths these three ops under
  `/things/{thingName}/jobs/{jobId}[...]` (confirmed against `serializers.go`'s http
  bindings for all three operations; `DeleteJobExecution`'s real path additionally
  carries an `/executionNumber/{executionNumber}` URI segment). All three ops were
  therefore **completely unreachable by a real client** — any real request would fall
  through gopherstack's routing entirely and be swallowed by the generic per-Thing CRUD
  dispatcher (`resolveThingsPathOperation`'s `default` branch), which is checked *before*
  the family's own (wrongly-shaped) resolver even runs. Fixed by adding
  `resolveThingJobExecutionOps` (`handler_routing.go`) ahead of that generic fallback,
  removing the old, always-dead `/jobs/{jobId}/things/{thingName}` matching from
  `resolveJobExecutionSubPathOps`, and rewriting `parseJobThingPath` as
  `parseThingJobPath` (`handler_jobs.go`) to parse the real path shape.

  While rewriting these three handlers, also found and fixed: `JobExecution` leaked an
  invented `"thingName"` field on the wire in place of the real `"thingArn"` (real
  `types.JobExecution` has no `thingName` at all); `statusDetails`/`versionNumber`/
  `forceCanceled`/`approximateSecondsBeforeTimedOut` were entirely unmodeled despite all
  being real `JobExecution` fields; and `CancelJobExecution`/`DeleteJobExecution`
  silently ignored `force`/`expectedVersion`/`statusDetails` entirely (a real client
  could never cancel/delete an `IN_PROGRESS`/non-terminal execution, nor would a stale
  `expectedVersion` ever be rejected). Implemented real
  `InvalidStateTransitionException`/`VersionConflictException` semantics (new
  `ErrInvalidStateTransition` sentinel, wired through `writeIoTError`, the single
  error-mapping source of truth from the 2026-07-23 pass).

  Finally, `ListJobExecutionsForJob`/`ListJobExecutionsForThing`'s response shape was
  also wrong: it returned a flat `{"jobId","thingName","status"}` per entry, but real
  AWS's `executionSummaries` is `[]JobExecutionSummaryForJob{thingArn,
  jobExecutionSummary:{executionNumber,queuedAt,startedAt,lastUpdatedAt,status}}` (and
  the `JobExecutionSummaryForThing` sibling with `jobId` instead of `thingArn`) —
  confirmed against `awsRestjson1_deserializeDocumentJobExecutionSummaryForJob`/`ForThing`.
  A real client's deserializer would have found none of the keys it looks for and
  returned entirely empty summaries for every execution. Fixed.

  All of the above (`AuditFinding`'s fields, `ListAuditFindings`' routing+filters,
  `DescribeJobExecution`/`CancelJobExecution`/`DeleteJobExecution`'s routing+wire+state
  semantics, `ListJobExecutionsForJob`/`ForThing`'s response nesting) are covered by new
  table tests: `TestDeviceDefender_AuditFinding_WireFieldsAndFilters`
  (`handler_devicedefender_test.go`) and `TestJobExecution_RoutingAndStateGuards`
  (`handler_jobs_test.go`), plus updates to the pre-existing `TestJobExecutions`/
  `TestJobExecution`/`TestAuditFinding` tests (which previously asserted against the
  wrong, unreachable-by-real-clients path shapes).

  **What remains genuinely partial** (why `job_and_jobtemplate`/`device_defender` stay
  `partial` despite these fixes, holding this service at A- rather than A): Job's more
  advanced optional fields (`jobExecutionsRetryConfig` read-path, `presignedUrlConfig`,
  `jobProcessDetails` rollup counts, `schedulingConfig`, `maintenanceWindows`,
  `destinationPackageVersions`) remain unimplemented, as does the more foundational fact
  that this emulator never fans a `QUEUED` `JobExecution` out per target at `CreateJob`
  time (`CancelJobExecution`'s create-on-miss fallback papers over this for the common
  test case, but it is not a substitute for real per-target execution tracking).
  `device_defender`'s `StartAuditMitigationActionsTask` target resolution, ML-based
  detect models, and violations families remain not exhaustively field-diffed, and
  `ListAuditFindings.resourceIdentifier` filtering remains unimplemented for the reasons
  given above. These are real, substantial, unimplemented sub-surfaces, not proven
  impossibilities — left honestly documented under `deferred:` rather than claimed as
  closed.
