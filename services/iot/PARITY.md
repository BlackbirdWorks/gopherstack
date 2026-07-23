---
service: iot
sdk_module: aws-sdk-go-v2/service/iot@v1.76.0
last_audit_commit: 135882ff405d549b4f7d65c71ade923a40c9fd7b
last_audit_date: 2026-07-23
overall: B+           # broad, real bugs found+fixed this pass (leak, systemic error-code
                       # mapping, systemic epoch-timestamp encoding, certificate transfer
                       # lifecycle, existence-validation gaps, invented-field cleanup);
                       # job_and_jobtemplate/device_defender/fleet_indexing still not
                       # exhaustively field-diffed (see deferred: below)
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
  CancelAuditTask: {wire: ok, errors: fixed, state: fixed, persist: ok, note: "unconditionally set status to CANCELED for any task ID; now returns ResourceNotFoundException for an unknown task and InvalidRequestException if it isn't IN_PROGRESS (gopherstack-ep0r)"}
  CancelAuditMitigationActionsTask: {wire: ok, errors: fixed, state: fixed, persist: ok, note: "same class of bug as CancelAuditTask; fixed identically (gopherstack-ep0r)"}
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
  job_and_jobtemplate: {status: partial, note: "AssociateTargetsWithJob existence-validation gap fixed (gopherstack-ep0r). DescribeJob/DescribeJobTemplate wire-shape bugs found+fixed this pass (see ops above). CreateJob/CreateJobTemplate/CreateJobOutput/CreateJobTemplateOutput verified correct. NOT exhaustively diffed: JobExecution shape, and Job's more advanced optional fields (jobExecutionsRetryConfig read-path, presignedUrlConfig, jobProcessDetails rollup counts, schedulingConfig, maintenanceWindows, destinationPackageVersions) -- large sub-surface, left for a future pass. See gopherstack-srzb."}
  device_defender: {status: partial, note: "CancelAuditTask/CancelAuditMitigationActionsTask existence+state validation gaps fixed this pass (gopherstack-ep0r). Broader audit/mitigation/detect task families (StartAuditMitigationActionsTask target resolution, ListAuditFindings, ML-based detect models, violations) NOT exhaustively field-diffed this pass. See gopherstack-srzb."}
  fleet_indexing: {status: deferred, note: "NOT touched this pass. Spot check: SearchIndex/GetCardinality/GetStatistics/GetPercentiles/GetBucketsAggregation are real (non-stub) implementations that query actual backend state, not placeholders -- but no field-by-field wire diff was done against v1.76.0. See gopherstack-srzb."}
  billing_group: {status: ok, note: "AddThingToBillingGroup/RemoveThingFromBillingGroup/ListThingsInBillingGroup verified real state mutation via thingBillingGroups map; DescribeThing now surfaces it (see CreateThing/DescribeThing above)"}
  persistence: {status: ok, note: "backendSnapshot/Restore in persistence.go covers all backend maps observed during this audit (policyTargets, thingPrincipals, thingBillingGroups, thingThingGroups, securityProfileTargets, resourceTags, certificateTransfers, etc.); Handler.Snapshot/Restore already delegate correctly -- no gaps found. Certificate struct's new transfer-lifecycle fields (OwnedBy/PreviousOwnedBy/GenerationID/CertificateMode/CustomerVersion/Validity*/Transfer*) round-trip correctly since persistence marshals the full struct, not the handler-layer wire shape."}
gaps: []  # both previously-filed gaps (gopherstack-jy57, gopherstack-ep0r) fixed and closed this pass; see families: for the still-partial/deferred sub-surfaces (job_and_jobtemplate, device_defender, fleet_indexing) tracked under gopherstack-srzb
deferred:
  - fleet_indexing (SearchIndex/aggregations -- not touched this pass)
  - job_and_jobtemplate (JobExecution + advanced Job fields: retry config, presigned URL config, process details, scheduling config, maintenance windows -- partial this pass, core CRUD + the filed gap fixed)
  - device_defender (audit/mitigation/detect task families beyond the two Cancel ops fixed this pass)
  - see gopherstack-srzb for the consolidated deferred-family tracking issue (updated this pass with current status)
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
- **Scope of this pass** (2026-07-23, commit `135882ff`): closed both previously-filed
  gaps (gopherstack-jy57, gopherstack-ep0r), fixed a real goroutine-leak bug in the
  embedded MQTT broker's Shutdown path, fixed a systemic error-code-mapping bug across
  ~130 call sites, fixed a systemic epoch-timestamp encoding bug across 8 struct
  families, fully closed out `certificate`/`certificate_provider`/`thing_type`/
  `policy_version` families, and made partial progress on `job_and_jobtemplate` and
  `device_defender`. `fleet_indexing` remains entirely untouched. See gopherstack-srzb
  (updated with current per-family status) for the remaining deferred sub-surfaces.
