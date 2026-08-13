---
service: iot
sdk_module: aws-sdk-go-v2/service/iot@v1.77.4
sibling_sdk_modules: [aws-sdk-go-v2/service/iotdataplane@v1.35.0]  # device-shadow ops (Get/Update/DeleteThingShadow, ListNamedShadowsForThing); see device_shadows family
last_audit_commit: 2a94081753c196de1bbad6b25b8f9b9a90dce321  # pass #4; pass #5 below is uncommitted at write time
last_audit_date: 2026-08-13
overall: A            # 2026-07-25 pass #4 (this pass): closed the ONE remaining partial
                       # family, security_profiles, the sole reason pass #3 stayed at A-.
                       # CreateSecurityProfile silently dropped Behaviors/AlertTargets/
                       # AdditionalMetricsToRetain/AdditionalMetricsToRetainV2/
                       # MetricsExportConfig entirely (types.CreateSecurityProfileInput,
                       # v1.76.0) -- SecurityProfile never persisted any of them. All five
                       # are now modeled (extending, not duplicating,
                       # ValidateSecurityProfileBehaviors' existing SecurityProfileBehavior/
                       # SecurityProfileBehaviorCriteria shapes per this pass's brief) and
                       # wired end-to-end: request parsing, backend storage, response wire
                       # shape (field-diffed against DescribeSecurityProfileOutput/
                       # UpdateSecurityProfileOutput), and persistence (SecurityProfile
                       # round-trips through the existing store.Table[SecurityProfile]
                       # registry unchanged -- no persistence.go wiring gap, since that
                       # layer already marshals the full struct). UpdateSecurityProfile was
                       # rebuilt from a single description-only field into the real
                       # UpdateSecurityProfileInput shape, including ExpectedVersion's
                       # optimistic-lock semantics and every DeleteX-flag-vs-field mutual-
                       # exclusion rule (previously entirely unmodeled). Closing this also
                       # unblocked ListActiveViolations/ListViolationEvents'
                       # behaviorCriteriaType filter (device_defender family), now
                       # implemented by resolving each violation's owning security
                       # profile's stored Behaviors live. security_profiles is now `ok`; see
                       # its families: entry and the new Scope-of-this-pass note below for
                       # detail. job_and_jobtemplate and device_defender were already closed
                       # by pass #3, below (kept verbatim for history).
                       #
                       # This pass also did the explicitly-required routing sweep ("check
                       # routing while you're there") for every security-profile op, driven
                       # through a real generated AWS SDK v2 client against the actual
                       # service.Router path rather than h.Handler() directly -- three prior
                       # passes each found real routing bugs this way for other op families,
                       # and this family had never been checked this way before. It found two
                       # MORE previously-undiscovered bugs specific to security_profiles: (1) a
                       # RouteMatcher-whitelist gap identical in kind to ListJobs'/the
                       # job-template/mitigationaction families' own prior-pass gaps --
                       # ListSecurityProfiles (plain "/security-profiles", no trailing slash)
                       # and ListSecurityProfilesForTarget ("/security-profiles-for-target")
                       # were both entirely unreachable by a real client despite op dispatch
                       # itself being correct; (2) three wire-shape key-name bugs on
                       # ListSecurityProfiles/ListTargetsForSecurityProfile/
                       # ListSecurityProfilesForTarget's list-entry shapes (invented/full keys
                       # in place of the real, shortened SecurityProfileIdentifier/
                       # SecurityProfileTarget/SecurityProfileTargetMapping keys). Also found
                       # and fixed DetachSecurityProfile's missing existence validation
                       # (AttachSecurityProfile's sibling gopherstack-ep0r fix was never
                       # mirrored onto Detach) and a DeleteSecurityProfile ghost-row leak
                       # (target attachments were never cascade-cleaned). See the
                       # security_profiles families: entry's "ROUTING VERIFIED" paragraph and
                       # the new per-op ops: entries above for full detail.
                       #
                       # --- pass #3 (2026-07-25, superseded by pass #4 above for overall:)
                       # closed both of the two remaining
                       # partial families (job_and_jobtemplate, device_defender), each now
                       # `ok`. Found and fixed a severe, previously-undiscovered bug class:
                       # CreateJob/CreateJobTemplate were routed on POST when real AWS uses
                       # PUT, and GetJobDocument was routed at /jobs/{jobId}/document instead
                       # of the real /jobs/{jobId}/job-document -- all three ops were
                       # completely unreachable by any real SDK client. Separately, found the
                       # RouteMatcher (the layer that decides whether a request even reaches
                       # the IoT handler at all in a real deployment, distinct from the
                       # op-dispatch layer) never matched "/jobs" (no trailing slash, so
                       # ListJobs), the entire "/job-templates" path family, or the entire
                       # "/mitigationactions/" path family (CreateMitigationAction and
                       # siblings) -- all silently 404'd before ever reaching op dispatch.
                       # None of this was visible to any prior pass because every existing
                       # test called h.Handler() directly, bypassing RouteMatcher entirely;
                       # this pass added a real generated AWS SDK v2 client driven through
                       # the actual service.Router path specifically to catch this class of
                       # bug (see TestJob_FanOutAndAdvancedFields_SDKRoundTrip et al.).
                       # Implemented the foundational per-target JobExecution fan-out at
                       # CreateJob/AssociateTargetsWithJob time (previously nonexistent --
                       # CancelJobExecution's create-on-miss fallback was the only thing
                       # papering over it), Job's and JobTemplate's advanced fields
                       # (jobExecutionsRetryConfig, presignedUrlConfig, schedulingConfig,
                       # maintenanceWindows, destinationPackageVersions, computed
                       # jobProcessDetails), StartAuditMitigationActionsTask's target
                       # resolution (was silently ignoring auditCheckToReasonCodeFilter
                       # whenever auditTaskId was also set, and matched reason codes by check
                       # name alone), DetectMitigationActionsTaskSummary's wire shape
                       # (invented "actions" field instead of real "actionsDefinition";
                       # ListDetectMitigationActionsTasks returned a hand-picked 4-field
                       # summary instead of the real, richer shared summary type Describe
                       # uses), DetectMitigationActionExecution's wrong field names
                       # (executionStartTime/executionEndTime instead of real
                       # executionStartDate/executionEndDate), ActiveViolation/ViolationEvent's
                       # missing lastViolationTime/violationEventAdditionalInfo fields, and
                       # ListAuditFindings.resourceIdentifier filtering (previously left
                       # unimplemented as "can't be honestly matched without guessing" --
                       # resolved by modeling a real, fully-typed ResourceIdentifier struct
                       # instead of a freeform map, at which point the filter's per-field
                       # discriminator semantics become the same simple equality-match every
                       # other filter in this service already uses). Also implemented
                       # ListActiveViolations/ListViolationEvents' listSuppressedAlerts filter.
                       # Both target families are now genuinely `ok` -- see families: below.
                       # Overall STAYS at A- rather than A because closing device_defender
                       # surfaced a real, substantial, PREVIOUSLY UNTRACKED gap in a third,
                       # different family: CreateSecurityProfile silently drops every one of
                       # Behaviors/AlertTargets/AdditionalMetricsToRetain(V2)/
                       # MetricsExportConfig -- SecurityProfile never persisted behavior
                       # definitions at all. This is what blocks ListActiveViolations/
                       # ListViolationEvents' remaining behaviorCriteriaType filter (there is
                       # no behavior-criteria-type data anywhere in this backend to filter on)
                       # and is a real, previously-unaudited `security_profiles` family gap
                       # in its own right -- NOT part of job_and_jobtemplate or
                       # device_defender, and explicitly out of scope for this pass, but too
                       # substantial to paper over by silently declaring the service A. See
                       # the new `security_profiles` families: entry and gaps: below.
                       #
                       # --- pass #5 (2026-08-13, gopherstack-oc9v, stays A) ---
                       # Scoped via gopherstack-oc9v's wire-sweep-blind-spot campaign
                       # (anonymous inline request structs are invisible to the repo's
                       # name-regex wire-diff tooling; iot has 79 of them, third-largest
                       # concentration repo-wide). Read this file first per that issue's
                       # instructions: overall was already `A` with every family `ok`
                       # except `fleet_metric`, explicitly `partial`, so this pass scoped
                       # to `fleet_metric` alone rather than re-auditing already-`ok`
                       # families. Converted its 3 remaining inline structs
                       # (UpdateFleetMetric/UpdateCustomMetric/UpdateDimension,
                       # handler_metrics.go) to named types and closed the family -- see
                       # `fleet_metric` below for the two real bugs found (the
                       # UpdateFleetMetric gap noted by a prior pass, plus a
                       # sibling CreateFleetMetric gap the conversion surfaced that no
                       # prior pass had tracked). 76 of iot's 79 inline request structs
                       # remain unconverted; see the campaign note under Notes below for
                       # the full accounting.
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
  DetachSecurityProfile: {wire: fixed, errors: fixed, state: fixed, persist: ok, note: "(pass #4) silently no-op'd for an unknown security profile name instead of returning ResourceNotFoundException -- the same gap AttachSecurityProfile had before gopherstack-ep0r, just never mirrored onto Detach; fixed. Also confirmed reachable through RouteMatcher (already whitelisted via the /security-profiles/ prefix) and now, upon its sibling DeleteSecurityProfile firing, has no ghost-row risk -- see security_profiles family note for the DeleteSecurityProfile cascade-cleanup fix."}
  ListSecurityProfiles: {wire: fixed, errors: ok, state: ok, persist: ok, note: "(pass #4) two real bugs: (1) RouteMatcher whitelist never matched plain \"/security-profiles\" (no trailing slash) -- op dispatch was already correct, but no real client request ever reached it; fixed. (2) securityProfileIdentifiers entries used the full \"securityProfileName\"/\"securityProfileArn\" keys instead of the real, shortened \"name\"/\"arn\" (types.SecurityProfileIdentifier, confirmed against awsRestjson1_deserializeDocumentSecurityProfileIdentifier) -- fixed. Also now paginates via maxResults/nextToken (previously unpaginated)."}
  ListSecurityProfilesForTarget: {wire: fixed, errors: ok, state: ok, persist: ok, note: "(pass #4) same RouteMatcher-whitelist gap as ListSecurityProfiles for \"/security-profiles-for-target\"; fixed. Also, securityProfileTargetMappings entries were missing both the identifier's \"arn\" and the entire sibling \"target\" object real types.SecurityProfileTargetMapping has (confirmed against awsRestjson1_deserializeDocumentSecurityProfileTargetMapping); fixed. Also now paginates."}
  ListTargetsForSecurityProfile: {wire: fixed, errors: ok, state: ok, persist: ok, note: "(pass #4) securityProfileTargets entries used an invented \"securityProfileTargetArn\" key instead of the real \"arn\" (types.SecurityProfileTarget, confirmed against awsRestjson1_deserializeDocumentSecurityProfileTarget); fixed. Already reachable (RouteMatcher whitelists /security-profiles/ as a prefix). Also now paginates."}
  DeleteSecurityProfile: {wire: ok, errors: ok, state: fixed, persist: ok, note: "(pass #4) never cleaned up the deleted profile's securityProfileTargets attachment-map entry, leaving a ghost row a same-named profile re-created later would incorrectly inherit; fixed via cascade-delete."}
  ValidateSecurityProfileBehaviors: {wire: ok, errors: ok, state: ok, persist: n/a, note: "(pass #4) re-verified reachable (POST /security-profile-behaviors/validate, already RouteMatcher-whitelisted via pathValidateSecurityProfileBehaviors) and its standalone validation-only semantics unchanged; its SecurityProfileBehavior/SecurityProfileBehaviorCriteria shapes were extended (not duplicated) to also serve as the real persisted Behaviors shape -- see security_profiles family note."}
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
  CreateJob: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "(pass #3) was routed on POST /jobs/{jobId}; real AWS IoT's CreateJob is PUT /jobs/{jobId} (confirmed against awsRestjson1_serializeOpCreateJob's request.Method) -- completely unreachable by any real SDK client. Fixed. Also now fans a real QUEUED JobExecution out to every resolved target thing (direct thing ARN, or thing-group ARN expanded to direct members) instead of only ever materializing an execution lazily via CancelJobExecution's create-on-miss fallback -- the foundational gap this family was previously flagged for. Also now accepts+stores jobExecutionsRetryConfig/presignedUrlConfig/schedulingConfig (incl. maintenanceWindows)/destinationPackageVersions, all previously entirely unmodeled."}
  CreateJobTemplate: {wire: fixed, errors: ok, state: ok, persist: ok, note: "(pass #3) same POST-vs-PUT routing bug as CreateJob (real AWS: PUT /job-templates/{jobTemplateId}); fixed. Also now accepts+stores jobExecutionsRetryConfig/presignedUrlConfig/destinationPackageVersions/maintenanceWindows -- note maintenanceWindows is a TOP-LEVEL field on JobTemplate, unlike Job's nested schedulingConfig.maintenanceWindows (real AWS has no schedulingConfig on JobTemplate at all; confirmed against both CreateJobTemplateInput and DescribeJobTemplateOutput)."}
  GetJobDocument: {wire: fixed, errors: ok, state: ok, persist: ok, note: "(pass #3) was routed at /jobs/{jobId}/document; real AWS IoT's GetJobDocument path is /jobs/{jobId}/job-document (confirmed against awsRestjson1_serializeOpGetJobDocument's SplitURI call) -- completely unreachable by any real SDK client. Fixed."}
  ListJobs: {wire: ok, errors: ok, state: ok, persist: ok, note: "(pass #3) op dispatch itself was already correct, but the RouteMatcher whitelist (the layer deciding whether a request reaches the IoT handler at all in a real deployment) never matched \"/jobs\" with no trailing slash -- a real ListJobs request never reached op dispatch. Fixed in matchCoreIoTPathSecondary/matchJobAndTemplatePath (handler_routing.go)."}
  ListJobTemplates: {wire: ok, errors: ok, state: ok, persist: ok, note: "(pass #3) same RouteMatcher-whitelist gap as ListJobs -- the entire /job-templates path family (this op plus CreateJobTemplate/DescribeJobTemplate/DeleteJobTemplate) was absent from the whitelist, so no request in that family ever reached the IoT handler in a real deployment. Fixed."}
  DeleteJobTemplate: {wire: ok, errors: ok, state: ok, persist: ok, note: "(pass #3) same RouteMatcher-whitelist gap as ListJobs/ListJobTemplates; fixed"}
  AssociateTargetsWithJob: {wire: fixed, errors: fixed, state: fixed, persist: ok, note: "mutated jobTargets for any job ID without checking the job existed; now returns ResourceNotFoundException for an unknown job (gopherstack-ep0r). (pass #3) response was also missing \"description\" (real AssociateTargetsWithJobOutput has it); newly associated targets are now merged into the job's own Targets list (previously only written to an otherwise-unread jobTargets map, so DescribeJob never reflected them) and immediately fanned out into QUEUED JobExecution rows, matching CreateJob's own fan-out for a CONTINUOUS job's initial targets."}
  DescribeJobExecution: {wire: fixed, errors: ok, state: ok, persist: ok, note: "(2026-07-25 #2) was routed under /jobs/{jobId}/things/{thingName}, a path no real client sends (real AWS: /things/{thingName}/jobs/{jobId}, confirmed against serializers.go http bindings) -- completely unreachable by a real SDK client. Also leaked an invented \"thingName\" field instead of the real \"thingArn\", and was missing statusDetails/versionNumber/forceCanceled/approximateSecondsBeforeTimedOut entirely. Both fixed."}
  CancelJobExecution: {wire: fixed, errors: fixed, state: fixed, persist: ok, note: "(2026-07-25 #2) same routing bug as DescribeJobExecution, fixed. Also silently ignored force/expectedVersion/statusDetails entirely; now rejects an IN_PROGRESS cancel without force=true (InvalidStateTransitionException) and a mismatched expectedVersion (VersionConflictException), matching real CancelJobExecutionInput semantics"}
  DeleteJobExecution: {wire: fixed, errors: fixed, state: fixed, persist: ok, note: "(2026-07-25 #2) same routing bug (real path also carries an executionNumber URI segment), fixed. Also silently ignored force; now rejects deleting a non-terminal (QUEUED/IN_PROGRESS) execution without force=true"}
  ListJobExecutionsForJob: {wire: fixed, errors: ok, state: ok, persist: ok, note: "(2026-07-25 #2) response was flat {jobId,thingName,status} per entry; real ListJobExecutionsForJobOutput.executionSummaries is []JobExecutionSummaryForJob{thingArn, jobExecutionSummary:{...}} (confirmed against awsRestjson1_deserializeDocumentJobExecutionSummaryForJob) -- a real client's deserializer would have found none of the keys it looks for and returned entirely empty summaries. Fixed."}
  ListJobExecutionsForThing: {wire: fixed, errors: ok, state: ok, persist: ok, note: "same bug and fix as ListJobExecutionsForJob, for the sibling JobExecutionSummaryForThing{jobId, jobExecutionSummary:{...}} shape"}
  CancelAuditTask: {wire: ok, errors: fixed, state: fixed, persist: ok, note: "unconditionally set status to CANCELED for any task ID; now returns ResourceNotFoundException for an unknown task and InvalidRequestException if it isn't IN_PROGRESS (gopherstack-ep0r)"}
  CancelAuditMitigationActionsTask: {wire: ok, errors: fixed, state: fixed, persist: ok, note: "same class of bug as CancelAuditTask; fixed identically (gopherstack-ep0r)"}
  ListAuditFindings: {wire: fixed, errors: ok, state: ok, persist: ok, note: "(2026-07-25 #2) was routed on GET; real AWS's ListAuditFindings is POST /audit/findings with filters in a JSON body (confirmed against serializers.go http bindings) -- completely unreachable by a real SDK client. Also ignored every filter field entirely. Both fixed: now POST-routed and implements checkName/taskId/listSuppressedFindings/startTime/endTime filtering (resourceIdentifier filtering remains unimplemented -- see families: device_defender)"}
  DescribeAuditFinding: {wire: fixed, errors: ok, state: ok, persist: ok, note: "(2026-07-25 #2) AuditFinding was missing isSuppressed/reasonForNonComplianceCode/reasonForNonCompliance/taskStartTime entirely (confirmed against awsRestjson1_deserializeDocumentAuditFinding); all four now modeled. taskStartTime is auto-derived from the referenced AuditTask when the finding has a taskId but no explicit taskStartTime"}
  ListAuditFindings_resourceIdentifier: {wire: fixed, errors: ok, state: ok, persist: ok, note: "(pass #3) resourceIdentifier filtering, previously left unimplemented (AuditFinding.NonCompliantResource was a freeform map[string]any that couldn't honestly discriminate against real AWS's ~10 per-check-type ResourceIdentifier fields). Fixed by modeling a real, fully-typed ResourceIdentifier struct (account/caCertificateId/clientId/cognitoIdentityPoolId/deviceCertificateArn/deviceCertificateId/iamRoleArn/issuerCertificateIdentifier/policyVersionIdentifier/roleAliasArn, confirmed against types.ResourceIdentifier) in place of the map, at which point the filter becomes the same per-field-equality-when-set semantics every other filter in this service already uses -- no per-check-type guessing required."}
  StartAuditMitigationActionsTask: {wire: ok, errors: ok, state: fixed, persist: ok, note: "(pass #3) target resolution had two real bugs: (1) when a target set both auditTaskId and auditCheckToReasonCodeFilter, only auditTaskId was ever honored (a switch's first matching case won) -- auditCheckToReasonCodeFilter was silently ignored even though real AWS's AuditMitigationActionsTaskTarget lets both apply together (\"this audit's findings for check X with reason code Y\"). (2) auditCheckToReasonCodeFilter matched by check name alone, ignoring the actual reason-code list value (real AWS filters on the listed codes when non-empty; an empty list for a check means \"any reason code\"). Both fixed in auditMitigationFindingIDs (device_defender.go)."}
  CreateMitigationAction_and_4_siblings: {wire: ok, errors: ok, state: ok, persist: ok, note: "(pass #3) CreateMitigationAction/DescribeMitigationAction/UpdateMitigationAction/DeleteMitigationAction/ListMitigationActions' op-dispatch routing (resolveMitigationActionOps) was already correct, but the RouteMatcher whitelist (the layer deciding whether a request reaches the IoT handler at ALL in a real deployment, checked before op dispatch) never matched the \"/mitigationactions/\" path prefix -- every request to any of these 5 ops 404'd before ever reaching op dispatch in a real deployment. Only caught because this pass added real generated-SDK-client tests driven through the actual service.Router path (existing tests all called h.Handler() directly, bypassing RouteMatcher entirely). Fixed in matchCoreIoTPathSecondary (handler_routing.go)."}
  StartDetectMitigationActionsTask: {wire: ok, errors: ok, state: fixed, persist: ok, note: "(pass #3) now accepts+stores violationEventOccurrenceRange (previously entirely unmodeled)"}
  DescribeDetectMitigationActionsTask: {wire: fixed, errors: ok, state: ok, persist: ok, note: "(pass #3) DetectMitigationActionsTaskSummary's real wire field is \"actionsDefinition\" (a list of full MitigationAction objects with id/name/roleArn/actionParams, confirmed against types.DetectMitigationActionsTaskSummary/types.MitigationAction) -- this emulator instead emitted \"actions\" (a list of bare action-name strings), a field that does not exist on the real type at all. A real client's deserializer would never have found \"actionsDefinition\" and left every task's actions permanently empty. Fixed via a new MitigationActionRefs backend lookup that resolves stored action names to their id/name/roleArn/actionParams at response time. violationEventOccurrenceRange also now surfaced (was entirely absent)."}
  ListDetectMitigationActionsTasks: {wire: fixed, errors: ok, state: ok, persist: ok, note: "(pass #3) previously built a hand-picked 4-field summary ({taskId,taskStatus,taskStartTime,taskEndTime}); real AWS's ListDetectMitigationActionsTasksOutput.Tasks is []types.DetectMitigationActionsTaskSummary -- the EXACT SAME rich type DescribeDetectMitigationActionsTask returns (confirmed against v1.76.0), not a narrower list-only summary (unlike the audit-mitigation side, where ListAuditMitigationActionsTasks genuinely does use a narrower AuditMitigationActionsTaskMetadata type). A real client's deserializer silently dropped target/actionsDefinition/taskStatistics/onlyActiveViolationsIncluded/suppressedAlertsIncluded/violationEventOccurrenceRange from every list entry. Fixed by sharing the same wire-shape builder (detectMitigationTaskSummaryWire) with Describe."}
  ListDetectMitigationActionsExecutions: {wire: fixed, errors: ok, state: ok, persist: ok, note: "(pass #3) DetectMitigationActionExecution's execution-time fields were wire-keyed \"executionStartTime\"/\"executionEndTime\"; real AWS's are \"executionStartDate\"/\"executionEndDate\" (confirmed against awsRestjson1_deserializeDocumentDetectMitigationActionExecution) -- a real client's deserializer would never have found either key and left both permanently unset. Fixed (fields renamed ExecutionStartDate/ExecutionEndDate to match)."}
  ListAuditMitigationActionsTasks: {wire: fixed, errors: ok, state: ok, persist: ok, note: "(pass #3) emitted an invented \"endTime\" key; real types.AuditMitigationActionsTaskMetadata is {taskId, taskStatus, startTime} only (confirmed against v1.76.0). Harmless to a real client (unknown fields are ignored by deserializers), but removed for wire-shape accuracy."}
  ListActiveViolations: {wire: fixed, errors: ok, state: ok, persist: ok, note: "(pass #3) ActiveViolation was missing lastViolationTime and violationEventAdditionalInfo entirely (confirmed against types.ActiveViolation -- real AWS distinguishes \"when the violation started\" from \"when the most recent violation occurred\", the latter updating on every subsequent detection of the same ongoing violation); both now modeled. Also implemented the listSuppressedAlerts filter (previously unimplemented) by adding an internal-only (json:\"-\", real ActiveViolation has no wire field for this) Suppressed flag, directly seedable, mirroring AuditFinding.IsSuppressed's identical simplification elsewhere in this service. (pass #4) behaviorCriteriaType filter now also implemented, resolved live against the owning security profile's now-persisted Behaviors -- see security_profiles below."}
  ListViolationEvents: {wire: fixed, errors: ok, state: ok, persist: ok, note: "same violationEventAdditionalInfo + listSuppressedAlerts fixes as ListActiveViolations, for the sibling ViolationEvent type"}
  DescribeJob: {wire: fixed, errors: ok, state: ok, persist: ok, note: "documentSource was nested inside \"job\" instead of being a top-level DescribeJobOutput field (verified against v1.76.0); the nested Job object also leaked invented document/documentSource/tags fields that don't exist on real types.Job -- fixed (documentSource promoted to top level, invented fields tagged json:\"-\"). (pass #3) now also returns jobExecutionsRetryConfig/presignedUrlConfig/schedulingConfig/destinationPackageVersions, and a computed jobProcessDetails rollup (numberOf{Queued,InProgress,Succeeded,Failed,Rejected,Canceled,Removed}Things + processingTargets) derived live from the backend's real per-target JobExecution rows rather than a separately-maintained (and driftable) counter."}
  DescribeJobTemplate: {wire: fixed, errors: ok, state: ok, persist: ok, note: "JobTemplate leaked an invented \"tags\" field not present in real DescribeJobTemplateOutput; tagged json:\"-\". (pass #3) now also returns jobExecutionsRetryConfig/presignedUrlConfig/destinationPackageVersions/maintenanceWindows, field-diffed separately from Job's own advanced fields (see CreateJobTemplate note on the maintenanceWindows nesting difference)."}
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
  job_and_jobtemplate: {status: ok, note: "(pass #3) CLOSED. Field-diffed exhaustively against v1.76.0. Foundational fan-out gap implemented: CreateJob/AssociateTargetsWithJob now fan a real QUEUED JobExecution out to every resolved target thing (thing ARN direct, or thing-group ARN expanded to direct members -- matching ListThingsInThingGroup's own non-recursive semantics), cascade-cleaned on DeleteThing/DeleteJob. Job's and JobTemplate's advanced fields (jobExecutionsRetryConfig, presignedUrlConfig, schedulingConfig incl. maintenanceWindows for Job / top-level maintenanceWindows for JobTemplate, destinationPackageVersions, computed jobProcessDetails) implemented end to end: request parsing, backend state, response wire shape, persistence. Found and fixed a severe, previously-undiscovered routing bug class: CreateJob and CreateJobTemplate were both routed on POST when real AWS uses PUT (awsRestjson1_serializeOpCreateJob/CreateJobTemplate), and GetJobDocument was routed at /jobs/{jobId}/document instead of the real /jobs/{jobId}/job-document -- all three completely unreachable by any real SDK client. Also found the RouteMatcher whitelist (checked before op dispatch in a real deployment) never matched plain \"/jobs\" (ListJobs) or the entire \"/job-templates\" path family -- both silently 404'd. AssociateTargetsWithJob was missing the real \"description\" output field and never merged newly-associated targets into the job's own Targets list. All fixed. See ops: above for each op's specifics."}
  device_defender: {status: ok, note: "(pass #3) CLOSED for everything within this family's own scope. StartAuditMitigationActionsTask's target resolution fixed (combined auditTaskId+auditCheckToReasonCodeFilter AND semantics, real reason-code-list matching instead of check-name-only). ML-Detect surface (StartDetectMitigationActionsTask and siblings) field-diffed: DetectMitigationActionsTaskSummary's actionsDefinition wire shape fixed (was an invented \"actions\" field), ListDetectMitigationActionsTasks now returns the same rich summary type Describe does (was a hand-picked 4-field subset), DetectMitigationActionExecution's executionStartDate/executionEndDate field names fixed (were wire-keyed wrong), violationEventOccurrenceRange added. Violations surface (ListActiveViolations/ListViolationEvents) field-diffed: lastViolationTime/violationEventAdditionalInfo added, listSuppressedAlerts filter implemented. ListAuditFindings.resourceIdentifier filtering implemented (previously the family's most-cited unimplementable gap) by modeling a real, fully-typed ResourceIdentifier struct instead of a freeform map. Also found the entire \"/mitigationactions/\" path family (CreateMitigationAction and siblings) was absent from the RouteMatcher whitelist -- completely unreachable in a real deployment despite correct op-dispatch routing. (pass #4) ListActiveViolations/ListViolationEvents' behaviorCriteriaType filter is now also implemented, once security_profiles (see below) closed the Behaviors-persistence gap that previously blocked it."}
  security_profiles: {status: ok, note: "(pass #4) CLOSED. CreateSecurityProfile's real input (types.CreateSecurityProfileInput) has Behaviors/AlertTargets/AdditionalMetricsToRetain/AdditionalMetricsToRetainV2/MetricsExportConfig -- this backend's SecurityProfile struct stored NONE of them; the request fields were silently accepted and dropped (the same severe 'dropped request field' bug class flagged elsewhere in this campaign, e.g. elasticache). All five are now modeled on SecurityProfile and wired end-to-end. Extended (rather than duplicated) ValidateSecurityProfileBehaviors' existing SecurityProfileBehavior/SecurityProfileBehaviorCriteria shapes to also be the real persisted Behaviors shape: SecurityProfileBehavior gained MetricDimension/ExportMetric/SuppressAlerts, SecurityProfileBehaviorCriteria gained Value/StatisticalThreshold/MlDetectionConfig (field-diffed against types.Behavior/types.BehaviorCriteria). New types SecurityProfileAlertTarget/SecurityProfileMetricToRetain/SecurityProfileMetricsExportConfig/SecurityProfileMetricDimension/SecurityProfileMetricValue/SecurityProfileStatisticalThreshold/SecurityProfileMLDetectionConfig mirror types.AlertTarget/MetricToRetain/MetricsExportConfig/MetricDimension/MetricValue/StatisticalThreshold/MachineLearningDetectionConfig. DescribeSecurityProfile/UpdateSecurityProfile field-diffed against DescribeSecurityProfileOutput/UpdateSecurityProfileOutput and confirmed to have had the identical gap (UpdateSecurityProfile previously accepted only securityProfileDescription); both rebuilt to return the full real field set, epoch-encoded creationDate/lastModifiedDate. UpdateSecurityProfile now also implements ExpectedVersion's optimistic-lock semantics (-> ErrVersionConflict/VersionConflictException on mismatch, confirmed against awsRestjson1_serializeOpHttpBindingsUpdateSecurityProfileInput -- expectedVersion is a QUERY parameter, not a body field) and every DeleteX-flag-vs-field mutual exclusion rule (deleteBehaviors/deleteAlertTargets/deleteAdditionalMetricsToRetain/deleteMetricsExportConfig, each rejecting InvalidRequestException-mapped ErrValidation when the corresponding field is also supplied in the same call, matching real AWS's documented semantics). Also found and fixed a real 'invented field' leak while field-diffing: SecurityProfile's pre-existing Tags field was surfaced on Describe/Update responses, but real DescribeSecurityProfileOutput/UpdateSecurityProfileOutput have NO \"tags\" field at all (tags are only ever retrievable via the separate ListTagsForResource op) -- fixed via json:\"-\" (same pattern as Job/JobTemplate's previously-fixed leaked \"tags\"). Persistence required no persistence.go changes: SecurityProfile already round-trips via the generic store.Table[SecurityProfile] registry (store.go/store_setup.go), which marshals the full struct -- confirmed by a new persistence regression case seeding a profile with all five previously-dropped fields. Closing this also unblocked device_defender's ListActiveViolations/ListViolationEvents behaviorCriteriaType filter (STATIC/STATISTICAL/MACHINE_LEARNING, types.BehaviorCriteriaType), now implemented via securityProfileBehaviorCriteriaTypeLocked, which resolves a violation's owning security profile's now-real stored Behaviors live -- see device_defender's own families: entry, updated below. ROUTING VERIFIED: with the Behaviors gap closed, every security-profile op (CreateSecurityProfile, UpdateSecurityProfile, DescribeSecurityProfile, ListSecurityProfiles, ListSecurityProfilesForTarget, AttachSecurityProfile, DetachSecurityProfile, ListTargetsForSecurityProfile, ValidateSecurityProfileBehaviors) was driven through a real generated AWS SDK v2 IoT client against the actual service.Router path (newIoTSDKClient/TestSecurityProfile_RoutingWireShapesAndBehaviorCriteriaType_SDKRoundTrip, handler_security_profiles_test.go; also TestHandler_RouteMatcher's list_security_profiles/list_security_profiles_for_target cases), not just h.Handler() directly -- the same class of gate three prior passes each found real bugs in for other op families. This turned up two more, previously-undiscovered bugs in this family specifically: (1) ListSecurityProfiles (GET /security-profiles, no trailing slash) and ListSecurityProfilesForTarget (GET /security-profiles-for-target) were BOTH entirely absent from the RouteMatcher whitelist (matchCoreIoTPathSecondary, handler_routing.go) -- op dispatch itself (resolveSecurityProfileOps) already handled both paths correctly, but a real client's request never reached op dispatch at all in a real deployment; fixed. (2) three wire-shape key-name bugs, confirmed against v1.76.0's awsRestjson1_deserializeDocumentSecurityProfileIdentifier/SecurityProfileTarget/SecurityProfileTargetMapping: ListSecurityProfiles' securityProfileIdentifiers used the full \"securityProfileName\"/\"securityProfileArn\" keys instead of the real, SHORTENED \"name\"/\"arn\" SecurityProfileIdentifier keys; ListTargetsForSecurityProfile's securityProfileTargets used an invented \"securityProfileTargetArn\" key instead of the real \"arn\"; ListSecurityProfilesForTarget's securityProfileTargetMappings nested only {securityProfileIdentifier:{name}} with no arn and no sibling \"target\" object at all, instead of the real {securityProfileIdentifier:{name,arn}, target:{arn}} -- a real client's deserializer would have left the affected fields permanently nil/empty under all three. All three fixed; all three List ops also gained maxResults/nextToken pagination (previously always returned every item in one page, unlike sibling List ops elsewhere in this service). Two smaller bugs found in the same pass: DetachSecurityProfile silently no-op'd for an unknown security profile name instead of returning ResourceNotFoundException (AttachSecurityProfile already had this validation, from gopherstack-ep0r, but it was never mirrored onto Detach); and DeleteSecurityProfile never cleaned up the deleted profile's entry in the securityProfileTargets attachment map, leaving a ghost row that a same-named profile re-created later would incorrectly inherit -- both fixed (TestSecurityProfile_DetachNotFoundAndDeleteCascade)."}
  fleet_indexing: {status: ok, note: "Field-diffed against v1.76.0 this pass (previously entirely untouched). Two real, previously-unflagged wire-shape bugs found and fixed: (1) SearchIndex's ThingGroupDocument sent a single \"parentGroupName\" string (direct parent only) instead of the real \"parentGroupNames\" LIST field (the full ancestor chain) -- confirmed against awsRestjson1_deserializeDocumentThingGroupDocument, a real client's deserializer would never find the key it looks for under the old shape and silently leave the field empty; also added the missing \"thingGroupDescription\" field. (2) DescribeThingGroup's thingGroupMetadata was completely missing \"rootToParentThingGroups\" (root-first ancestor name+ARN list) -- confirmed against awsRestjson1_deserializeDocumentThingGroupMetadata; not implemented at all previously. Both fixed via a new thingGroupAncestors backend helper (indexing.go) that reconstructs the full chain by walking gopherstack's per-group direct-ParentGroupName links, since the domain model only stores one level per group. (3) GetStatistics' Statistics response was missing \"sumOfSquares\" entirely (types.Statistics has it; confirmed against awsRestjson1_deserializeDocumentStatistics) -- fixed by computing it in computeStatistics alongside the existing sum/variance accumulation. GetCardinality/GetPercentiles/GetBucketsAggregation/DescribeIndex/ListIndices output shapes also field-diffed against their real GetCardinalityOutput/GetPercentilesOutput/GetBucketsAggregationOutput/types.PercentPair/types.Bucket counterparts -- no further gaps found on this pass's sample."}
  billing_group: {status: ok, note: "AddThingToBillingGroup/RemoveThingFromBillingGroup/ListThingsInBillingGroup verified real state mutation via thingBillingGroups map; DescribeThing now surfaces it (see CreateThing/DescribeThing above)"}
  persistence: {status: ok, note: "backendSnapshot/Restore in persistence.go covers all backend maps observed during this audit (policyTargets, thingPrincipals, thingBillingGroups, thingThingGroups, securityProfileTargets, resourceTags, certificateTransfers, etc.); Handler.Snapshot/Restore already delegate correctly -- no gaps found. Certificate struct's new transfer-lifecycle fields (OwnedBy/PreviousOwnedBy/GenerationID/CertificateMode/CustomerVersion/Validity*/Transfer*) round-trip correctly since persistence marshals the full struct, not the handler-layer wire shape."}
  fleet_metric: {status: ok, note: "(pass #5, 2026-08-13, gopherstack-oc9v) CLOSED. Prior pass fixed UpdateFleetMetric's dropped expectedVersion but left indexName/aggregationType/aggregationField/queryVersion/unit unfixed. This pass converted handler_metrics.go's 3 remaining anonymous inline request structs (UpdateFleetMetric, UpdateCustomMetric, UpdateDimension -- part of the wire-sweep-blind-spot campaign, gopherstack-oc9v) to named types (UpdateFleetMetricInput/UpdateCustomMetricInput/UpdateDimensionInput, metrics.go), and while doing so field-diffed the whole family against v1.77.4's UpdateFleetMetricInput/CreateFleetMetricInput/DescribeFleetMetricOutput directly. Fixed all 5 of those documented UpdateFleetMetric gaps (indexName, aggregationType, aggregationField, queryVersion, unit all now applied). Also found a SIXTH, previously-untracked gap the same diff surfaced: CreateFleetMetricInput was ALSO missing aggregationField/aggregationType entirely (both `This member is required` on the real type) -- CreateFleetMetric silently dropped them with no error, and FleetMetric never modeled them at all, so DescribeFleetMetric/ListFleetMetrics could never have surfaced them either even if a caller worked around the drop. New `AggregationType{Name,Values}` type (metrics.go) mirrors types.AggregationType; both Create and Update now thread aggregationField/aggregationType through end to end (request parsing, backend storage on FleetMetric, response wire shape -- confirmed against awsRestjson1_deserializeOpDocumentDescribeFleetMetricOutput's \"aggregationField\"/\"aggregationType\" keys, aggregationType nested as {name,values}). UpdateCustomMetric/UpdateDimension's inline structs were already field-complete (DisplayName-only / StringValues-only, matching real UpdateCustomMetricInput/UpdateDimensionInput exactly) -- converted for tooling visibility only, no bug. Regression: TestFleetMetric_AggregationAndUpdateFields (handler_metrics_test.go), verified to fail against the pre-fix code by temporarily reverting the field-wiring."}
  device_shadows: {status: ok, note: "NEW entry (2026-07-31, reverse sdkcheck sweep, gopherstack-vhw2): DeleteThingShadow/GetThingShadow/ListNamedShadowsForThing/UpdateThingShadow are real IoT Data Plane operations, on a separate SDK client (aws-sdk-go-v2/service/iotdataplane) from this service's control-plane client (aws-sdk-go-v2/service/iot) -- confirmed by name against iotdataplane.Client. pkgs/sdkcheck's reverse check was flagging all 4 as 'phantom' only because it compared them against iotsdk.Client instead of iotdataplanesdk.Client; sdk_completeness_test.go now checks this family separately against the correct client (notImplemented: DeleteConnection/GetConnection/GetRetainedMessage/ListRetainedMessages/ListSubscriptions/Publish/SendDirectMessage, the rest of that client's surface, covered instead by the separate services/iotdataplane package -- this Handler's shadow REST routes (handler_shadows.go) and services/iotdataplane's own shadow implementation are a pre-existing duplication across the two packages, not introduced by this fix and not resolved here). No wire-shape field-diff done, naming/completeness only."}
gaps: []
  # The UpdateFleetMetric gap (dropped indexName/aggregationType/
  # aggregationField/queryVersion/unit) closed by pass #5 (2026-08-13, gopherstack-oc9v)
  # -- see fleet_metric: above, which also documents a 6th, previously-untracked gap
  # (CreateFleetMetric dropping aggregationField/aggregationType too) that surfaced
  # only once the family's inline request structs were converted to named types.
  #
  # All families closed as of pass #4 (2026-07-25). security_profiles -- the sole reason
  # pass #3 stayed at A- -- is now `ok` (see its families: entry above): CreateSecurityProfile/
  # UpdateSecurityProfile persist the full real field set, ListActiveViolations/
  # ListViolationEvents' behaviorCriteriaType filter is implemented, and every
  # security-profile op (CreateSecurityProfile, UpdateSecurityProfile, DescribeSecurityProfile,
  # ListSecurityProfiles, ListSecurityProfilesForTarget, AttachSecurityProfile,
  # DetachSecurityProfile, ListTargetsForSecurityProfile, ValidateSecurityProfileBehaviors) was
  # re-verified reachable end to end through the real RouteMatcher, not just callable on the
  # handler -- see the security_profiles families: entry's "routing verified" paragraph for the
  # two additional, previously-undiscovered bugs that check turned up (a RouteMatcher-whitelist
  # gap for ListSecurityProfiles/ListSecurityProfilesForTarget, and three wire-shape key-name
  # bugs on the same two ops plus ListTargetsForSecurityProfile).
deferred: []
  # gopherstack-srzb (job_and_jobtemplate + device_defender consolidated tracking issue) and
  # the security_profiles item that superseded it as pass #3's sole open item are both closed
  # as of this pass. No known deferred work remains for this service.
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

  **Superseded by pass #3 below** — every item in the paragraph immediately above is now
  fixed. It's left in place (rather than deleted) because it accurately documents what
  pass #2 genuinely didn't reach, which is useful context for how the gap closed.

- **Scope of this pass (2026-07-25 pass #3)**: closed both `job_and_jobtemplate` and
  `device_defender` — see their `families:` entries above for the full list of fixes.
  Three points worth calling out beyond the `families:`/`ops:` summaries:

  1. **A new bug class this campaign hadn't yet named for this service: wrong HTTP
     method, not just wrong path.** Every prior routing bug found in this service (and
     most of this campaign) was "wrong path shape." This pass found `CreateJob` and
     `CreateJobTemplate` were both routed on `POST` when real AWS IoT uses `PUT`
     (confirmed directly against `awsRestjson1_serializeOpCreateJob`/
     `CreateJobTemplate`'s `request.Method` assignment in `serializers.go`) — same path,
     wrong verb, same result: completely unreachable by any real SDK client. `GetJobDocument`
     had the more familiar wrong-path variant (`/jobs/{jobId}/document` vs. the real
     `/jobs/{jobId}/job-document`). All three were invisible to every prior pass because
     every existing test (`iotOK`/`iotRequest`) called `h.Handler()` directly with a
     hand-picked method string that happened to match gopherstack's own (wrong) routing —
     exactly the `parity-principles.md` rule 3 trap, just manifesting as a wrong verb
     instead of a wrong field.

  2. **A second, deeper bug class: the RouteMatcher whitelist itself, not op dispatch.**
     This service's `Handler.RouteMatcher()` (`matchIoTPath` and its helpers in
     `handler_routing.go`) is a separate, EARLIER gate than `resolveOperation`/op
     dispatch — in a real deployment via `service.NewServiceRouter`, a request must match
     `RouteMatcher()` before the IoT handler is even invoked, let alone before
     `resolveOperation` picks an op. This pass found THREE path families entirely absent
     from that whitelist despite having perfectly correct op-dispatch logic once
     reached: plain `/jobs` (no trailing slash — `ListJobs`), the entire
     `/job-templates` family, and the entire `/mitigationactions/` family
     (`CreateMitigationAction` and its four siblings, foundational to
     `StartAuditMitigationActionsTask`'s whole workflow). Every one of these requests
     404'd before `resolveOperation` ever ran. This was invisible to every prior pass for
     the same underlying reason as point 1: `iotRequest`/`iotOK` call `h.Handler()`
     directly, which bypasses `RouteMatcher()` entirely (that gate only exists in the
     `service.NewServiceRouter` request path). This pass added `newIoTSDKClient`
     (`handler_jobs_test.go`) — a real generated AWS SDK v2 IoT client driven through an
     actual `httptest.Server` + `service.NewServiceRouter`, matching the pattern already
     established in `services/elasticache` — specifically because catching this bug class
     requires exercising the real routing path, not just the handler function. **If
     re-auditing this service (or any service) for routing bugs: a `RouteMatcher()` gap
     is a distinct bug from a `resolveOperation` gap, and only the SDK-client-through-a-
     real-router pattern can find the former.**

  3. **The foundational per-target `JobExecution` fan-out.** `CreateJob` now resolves
     `input.Targets` (thing ARNs directly, or thing-group ARNs expanded to that group's
     direct members — deliberately non-recursive, matching `ListThingsInThingGroup`'s own
     non-recursive semantics rather than inventing recursive-group-membership tracking
     this backend doesn't otherwise have) and creates a real `QUEUED` `JobExecution` row
     for each resolved thing (`fanOutJobExecutionsLocked`, `jobs.go`).
     `AssociateTargetsWithJob` does the same for newly-added targets, and now also merges
     them into the job's own `Targets` list (previously written only to an
     otherwise-never-read `jobTargets` map, so `DescribeJob` never reflected newly
     associated targets at all — a second, smaller bug found alongside the fan-out work).
     `DeleteThing` cascade-deletes any `JobExecution` rows for that thing (mirroring
     `DeleteJob`'s existing cascade over the same `jobId`/`thingName` key from the other
     side), so a deleted thing never leaves a ghost `JobExecution` behind.
     `CancelJobExecution`'s old create-on-miss fallback is kept as a narrow defensive
     backstop (documented in its own doc comment) for the one case fan-out still can't
     cover — a thing-group target with zero members at `CreateJob` time, later joined by
     a thing without this backend simulating AWS's lazy continuous-job rollout — rather
     than removed outright.

  4. **`ListAuditFindings.resourceIdentifier` filtering, previously the family's most-cited
     "can't be done" item, closed.** The prior pass's reasoning — that
     `AuditFinding.NonCompliantResource`'s freeform `map[string]any` couldn't honestly
     discriminate against real AWS's ~10 per-check-type `ResourceIdentifier` fields — was
     correct as far as it went, but the fix was to stop using a freeform map at all: this
     pass modeled `ResourceIdentifier` (and its `NonCompliantResource` parent) as real,
     fully-typed structs matching `types.ResourceIdentifier`/`types.NonCompliantResource`
     exactly (`account`/`caCertificateId`/`clientId`/`cognitoIdentityPoolId`/
     `deviceCertificateArn`/`deviceCertificateId`/`iamRoleArn`/
     `issuerCertificateIdentifier`/`policyVersionIdentifier`/`roleAliasArn`). Once the
     shape itself is real, the filter's semantics collapse to the same "every field SET
     on the filter must be present and equal on the target" pattern every other filter in
     this service already implements — no per-check-type guessing required, because
     callers (`SeedAuditFinding`) simply populate whichever field is appropriate to the
     check they're simulating, exactly as real AWS does per finding.

  5. **A genuinely new, previously-untracked gap surfaced along the way, in a different
     family.** Investigating `ListActiveViolations`/`ListViolationEvents`'
     `listSuppressedAlerts` and `behaviorCriteriaType` filters led to discovering that
     `CreateSecurityProfile` doesn't persist `Behaviors`/`AlertTargets`/
     `AdditionalMetricsToRetain(V2)`/`MetricsExportConfig` AT ALL — `SecurityProfile`
     never modeled them, despite `ValidateSecurityProfileBehaviors` (a separate,
     standalone validation-only endpoint) already having a reasonably rich
     `SecurityProfileBehavior{Name,Metric,Criteria}` shape that's never connected to an
     actual stored profile. `listSuppressedAlerts` was still honestly implementable (see
     `ListActiveViolations`' `ops:` entry — suppression modeled as a directly-seedable
     flag, mirroring `AuditFinding.IsSuppressed`'s identical precedent elsewhere in this
     exact service), but `behaviorCriteriaType` genuinely is not: there is no
     behavior-criteria-type data anywhere in this backend to filter on, and building it
     would mean implementing the missing `SecurityProfile.Behaviors` persistence first —
     a distinct, substantial project belonging to a `security_profiles` family this
     service has never tracked before, not a `device_defender` sub-item. Filed as a new
     `security_profiles` `families:` entry and `gaps:`/`deferred:` item rather than
     silently worked around or ignored — see those sections above. This is the sole
     reason `overall:` stays `A-` rather than `A` despite both of this pass's two
     assigned families (`job_and_jobtemplate`, `device_defender`) now being genuinely
     `ok`.

- **Scope of this pass (2026-07-25 pass #4)**: closed `security_profiles`, the sole
  family pass #3 left partial, bringing `overall:` to `A`. Two parts:

  1. **Behaviors/AlertTargets/AdditionalMetricsToRetain(V2)/MetricsExportConfig
     persistence**, field-diffed against `types.CreateSecurityProfileInput`/
     `UpdateSecurityProfileInput`/`DescribeSecurityProfileOutput`/
     `UpdateSecurityProfileOutput` (v1.76.0). All five request fields, previously
     silently dropped, are now modeled on `SecurityProfile` and wired end to end:
     request parsing, backend storage, response wire shape, and persistence (no
     `persistence.go` changes needed — `SecurityProfile` already round-trips via the
     generic `store.Table[SecurityProfile]` registry, which marshals the full struct).
     `UpdateSecurityProfile` was rebuilt from a single description-only field into the
     real `UpdateSecurityProfileInput` shape, including `ExpectedVersion`'s
     optimistic-lock semantics (`expectedVersion` is a query parameter, not a body
     field — confirmed against `awsRestjson1_serializeOpHttpBindingsUpdateSecurityProfileInput`)
     and every `DeleteX`-flag-vs-field mutual-exclusion rule. Also fixed an invented-field
     leak found in the same diff: `SecurityProfile.Tags` was surfaced on
     Describe/Update responses, but real `DescribeSecurityProfileOutput`/
     `UpdateSecurityProfileOutput` have no `"tags"` field at all (tags are only
     retrievable via the separate `ListTagsForResource` op) — fixed via `json:"-"`.
     Closing this unblocked `ListActiveViolations`/`ListViolationEvents`'
     `behaviorCriteriaType` filter (`device_defender` family), now resolved live
     against each violation's owning security profile's real stored `Behaviors`.

  2. **The routing sweep the task brief explicitly required** ("check routing while
     you are there — three prior passes each found ops unreachable by a real
     client"). Every security-profile op was driven through a real generated AWS SDK
     v2 IoT client against the actual `service.Router` path (`newIoTSDKClient`,
     already established by pass #3 for exactly this purpose), not just
     `h.Handler()` directly. This family had never been checked this way before, and
     it found two more real, previously-undiscovered bugs of the exact same classes
     prior passes found elsewhere in this service: a `RouteMatcher`-whitelist gap
     (`ListSecurityProfiles`' plain `"/security-profiles"`, no trailing slash — same
     shape as `ListJobs`' own prior-pass gap — and `ListSecurityProfilesForTarget`'s
     `"/security-profiles-for-target"` were both entirely absent from
     `matchCoreIoTPathSecondary`, `handler_routing.go`, despite `resolveSecurityProfileOps`
     already dispatching both correctly), and three wire-shape key-name bugs
     (`ListSecurityProfiles`/`ListTargetsForSecurityProfile`/
     `ListSecurityProfilesForTarget`'s list-entry shapes used invented or
     full-length keys in place of the real, shortened `SecurityProfileIdentifier`/
     `SecurityProfileTarget`/`SecurityProfileTargetMapping` keys — confirmed against
     `awsRestjson1_deserializeDocumentSecurityProfileIdentifier`/`SecurityProfileTarget`/
     `SecurityProfileTargetMapping`). All fixed, along with two smaller bugs found in
     the same sweep: `DetachSecurityProfile` never mirrored `AttachSecurityProfile`'s
     existing `gopherstack-ep0r` existence-validation fix, and `DeleteSecurityProfile`
     never cascade-cleaned its target-attachment map entry (a ghost row a same-named
     profile re-created later would incorrectly inherit). See the `security_profiles`
     `families:` entry's "ROUTING VERIFIED" paragraph and the new per-op `ops:`
     entries above for full detail, and `TestSecurityProfile_RoutingWireShapesAndBehaviorCriteriaType_SDKRoundTrip`/
     `TestSecurityProfile_DetachNotFoundAndDeleteCascade`
     (`handler_security_profiles_test.go`) plus two new `TestHandler_RouteMatcher`
     cases (`handler_routing_test.go`) for the regression coverage.
- **Broker capability addition (parity-5, gopherstack-polh, no grade change --
  `Broker` is internal plumbing, not an AWS `iot` wire op):** `Broker` (broker.go)
  now implements two new methods consumed by `services/iotdataplane`'s
  `MQTTPublisher` interface: `ClientSubscriptions(clientID) (map[string]byte, bool)`,
  reading a connected client's real live subscriptions off mochi-mqtt's
  `cl.State.Subscriptions.GetAll()`, and `SendToClient(clientID, topic, payload, qos) (bool, error)`,
  writing a PUBLISH packet straight to one client's connection via
  `cl.WritePacket`, bypassing topic subscription matching entirely (mirrors
  real AWS `SendDirectMessage`'s documented "receiving client does not need
  to subscribe" semantics). Both are proven against a REAL mochi-mqtt session
  — a live `paho.mqtt.golang` client connected over real TCP loopback, not a
  mock — by `TestBroker_ClientSubscriptionsAndSendToClient` (`broker_test.go`).
  This closes the `services/iotdataplane` `ListSubscriptions`/`SendDirectMessage`
  gaps previously blocked on this exact interface boundary (see
  `services/iotdataplane/PARITY.md`'s gaps list for the resolution writeup).
  `export_test.go` was NOT touched for this — the new test drives the real
  broker entirely through already-exported API (`NewBroker`/`Start`/
  `ClientSubscriptions`/`SendToClient`) plus a real TCP client, no whitebox
  hooks needed.
- **Scope of pass #5 (2026-08-13, gopherstack-oc9v)**: this campaign targets a
  coverage blind spot in the sweep *tooling*, not this file's wire-shape
  content — handlers whose request is an anonymous inline `struct{...}`
  literal generate no candidate for the repo's name-regex wire-diff sweep, so
  a wrong-name or dropped field on one of them is invisible to that tooling
  regardless of how correct the field values themselves are. iot has 79 such
  structs (`grep -c 'var req struct\|var body struct'` across non-test
  `services/iot/*.go`), the third-largest concentration repo-wide behind
  sagemaker (362) and cleanrooms (97).

  Per the campaign's stated method (proven by sagemaker's earlier passes):
  read `PARITY.md` first and scope to what it shows as genuinely uncovered,
  rather than re-deriving already-verified work. This file showed `overall:
  A` with every family `ok` except `fleet_metric`, explicitly `partial` (the
  one item under `gaps:`) — so this pass scoped there. Converted
  `fleet_metric`'s 3 inline structs (`UpdateFleetMetric`,
  `UpdateCustomMetric`, `UpdateDimension`, all in `handler_metrics.go`) to
  named types (`UpdateFleetMetricInput`/`UpdateCustomMetricInput`/
  `UpdateDimensionInput`, `metrics.go`) and field-diffed the whole family
  (`Create`/`Describe`/`List`/`Update`/`Delete` FleetMetric) against
  `aws-sdk-go-v2/service/iot@v1.77.4` directly. This closed the
  gap a prior pass noted (`UpdateFleetMetric` dropping
  `indexName`/`aggregationType`/`aggregationField`/`queryVersion`/`unit`) and
  surfaced a sibling, previously-untracked one on `CreateFleetMetric`
  (`aggregationField`/`aggregationType`, both `This member is required` on
  the real `CreateFleetMetricInput`) that no prior wire-diff pass had found —
  exactly the failure mode gopherstack-oc9v predicts: the bug was invisible
  to the name-regex sweep because `CreateFleetMetricInput` in this codebase
  was *already* a named type (so it wasn't part of the 79-count at all), but
  nobody had actually diffed its field set against the real SDK type before
  this pass, because the campaign that would have prompted that diff had
  never been run. See the `fleet_metric` `families:` entry above for the
  full fix (new `AggregationType{Name,Values}` type, threaded through
  `Create`/`Update`/response wire shape) and
  `TestFleetMetric_AggregationAndUpdateFields` (`handler_metrics_test.go`)
  for the regression, confirmed to fail against the pre-fix code by manually
  reverting the field-wiring and re-running before restoring it.

  `UpdateCustomMetric`/`UpdateDimension`'s inline structs were already
  field-complete against `UpdateCustomMetricInput`/`UpdateDimensionInput` —
  converted to named types for tooling visibility only, no bug found.

  **Not done by this pass, still exposed to the blind spot**: 76 of iot's 79
  anonymous inline request structs remain unconverted (only the 3 in
  `fleet_metric` were addressed) — every op family other than
  `fleet_metric` was left exactly as pass #4 verified it, on the read-first
  finding that those families were already `ok`. Converting the rest and
  wire-diffing each is real, substantial, unstarted work; the next pass on
  this service for this campaign should pick a family (or run a full
  `grep -n 'var req struct\|var body struct' services/iot/*.go` sweep) rather
  than assume `overall: A` means the inline-struct blind spot is closed here
  — it means the *content* that pass #4's tooling could see was verified, not
  that every request shape has been read as a named type against the pinned
  SDK.
