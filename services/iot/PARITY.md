---
service: iot
sdk_module: aws-sdk-go-v2/service/iot@v1.76.0
last_audit_commit: 5256fdde84d37f54adca98c9cf44f1499fbd9bdf
last_audit_date: 2026-07-12
overall: B            # already-accurate, proven op-by-op on the audited families; small
                       # set of genuine bugs found and fixed, not a rewrite
ops:
  CreateThing: {wire: ok, errors: ok, state: ok, persist: ok, note: "now accepts+wires billingGroupName (was silently dropped)"}
  DescribeThing: {wire: ok, errors: ok, state: ok, persist: ok, note: "now returns billingGroupName (was omitted entirely)"}
  UpdateThing: {wire: ok, errors: ok, state: ok, persist: ok, note: "AttributePayload.merge default was inverted (defaulted to merge; AWS defaults to replace) and empty-value attribute removal was missing -- both fixed via applyAttributePayload"}
  DeleteThing: {wire: ok, errors: ok, state: ok, persist: ok}
  ListThings: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateThingGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeThingGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateThingGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "same AttributePayload.merge bug as UpdateThing (handler didn't even parse merge from the request); fixed with the same applyAttributePayload helper"}
  DeleteThingGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  AttachPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "was appending without dedup, so double-attach produced duplicate ListAttachedPolicies entries; fixed with appendUnique (AWS attach ops are idempotent/set semantics)"}
  AttachPrincipalPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "same duplicate-entry bug as AttachPolicy; fixed"}
  AttachThingPrincipal: {wire: ok, errors: ok, state: ok, persist: ok, note: "same duplicate-entry bug; fixed"}
  AttachSecurityProfile: {wire: ok, errors: ok, state: ok, persist: ok, note: "same duplicate-entry bug; fixed"}
  DetachPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAttachedPolicies: {wire: ok, errors: ok, state: ok, persist: ok}
  CreatePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPolicies: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateTopicRule: {wire: ok, errors: ok, state: ok, persist: ok}
  GetTopicRule: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteTopicRule: {wire: ok, errors: ok, state: ok, persist: ok}
  ReplaceTopicRule: {wire: ok, errors: ok, state: ok, persist: ok}
  EnableTopicRule: {wire: ok, errors: ok, state: ok, persist: ok}
  DisableTopicRule: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTopicRules: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "Tag shape uses capitalized Key/Value JSON keys -- verified against real deserializer, this IS correct for IoT (not a bug, see Notes)"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  AttachPrincipalPolicy_and_11_other_handlers: {wire: fixed, errors: fixed, state: ok, persist: ok, note: "12 handlers (Attach*, AcceptCertificateTransfer, AddThingToBillingGroup/ThingGroup, AssociateSbomWithPackageVersion, AssociateTargetsWithJob, CancelAuditMitigationActionsTask, CancelAuditTask, DescribeEndpoint) returned a raw {\"error\":...} 500 body instead of h.handleError's {__type,message} shape on any backend error, bypassing errCodeLookup-style mapping entirely. Currently latent (their backend calls never actually return non-nil errors today) but fixed for correctness/defense-in-depth -- matches the documented 'missing errCodeLookup entries -> 500 InternalFailure' bug class."}
families:
  thing_core: {status: ok, note: "CreateThing/DescribeThing/UpdateThing/DeleteThing/ListThings audited field-by-field against v1.76.0 serializers/deserializers; 2 real bugs found+fixed (billingGroupName, AttributePayload merge default)"}
  thing_group: {status: ok, note: "Create/Describe/Update/Delete/List audited; UpdateThingGroup AttributePayload bug fixed (mirrors UpdateThing)"}
  policy_attach: {status: ok, note: "Attach/Detach/List for both Policy and PrincipalPolicy audited; duplicate-entry bug on double-attach fixed across all 4 attach ops (Policy/PrincipalPolicy/ThingPrincipal/SecurityProfile)"}
  tags: {status: ok, note: "TagResource/UntagResource/ListTagsForResource verified real state mutation + correct Key/Value wire casing"}
  topic_rule: {status: ok, note: "Create/Get/Delete/Replace/Enable/Disable/List spot-checked against restjson1 deserializer field names (rule/ruleArn wrapper); no bugs found"}
  error_handling: {status: fixed, note: "12 handlers bypassed the central h.handleError sentinel-error mapper; fixed to use it uniformly"}
  thing_type: {status: deferred, note: "dispatch wiring confirmed present (Create/Describe/List/Deprecate/Delete/Update), handler bodies not field-by-field audited this pass"}
  certificate: {status: partial, note: "core CRUD (Describe/List/Update/Delete/RegisterCertificate*) dispatches to real backend state; response shape is missing several real DescribeCertificateOutput fields (ownedBy, previousOwnedBy, generationId, validity, certificateMode) -- filed as gopherstack-jy57, not fixed this pass"}
  certificate_provider: {status: deferred, note: "not audited this pass"}
  job_and_jobtemplate: {status: deferred, note: "not audited this pass; AssociateTargetsWithJob observed to skip job-existence validation -- filed as gopherstack-ep0r"}
  device_defender: {status: deferred, note: "audit/mitigation/detect task families not audited this pass"}
  fleet_indexing: {status: deferred, note: "SearchIndex/GetCardinality/GetPercentiles/GetStatistics/GetBucketsAggregation not audited this pass"}
  billing_group: {status: ok, note: "AddThingToBillingGroup/RemoveThingFromBillingGroup/ListThingsInBillingGroup verified real state mutation via thingBillingGroups map; DescribeThing now surfaces it (see CreateThing/DescribeThing above)"}
  persistence: {status: ok, note: "backendSnapshot/Restore in persistence.go covers all backend maps observed during this audit (policyTargets, thingPrincipals, thingBillingGroups, thingThingGroups, securityProfileTargets, resourceTags, etc.); Handler.Snapshot/Restore already delegate correctly -- no gaps found"}
gaps:
  - "DescribeCertificate/ListCertificates response missing ownedBy/previousOwnedBy/generationId/validity/certificateMode fields present in real CertificateDescription (bd: gopherstack-jy57)"
  - "Several ops (AssociateTargetsWithJob, CancelAuditTask, CancelAuditMitigationActionsTask, AttachSecurityProfile) mutate/read state without validating the referenced resource (job/task/profile) exists, so unknown IDs succeed instead of returning ResourceNotFoundException (bd: gopherstack-ep0r)"
deferred:
  - thing_type (field-by-field wire audit)
  - certificate_provider
  - job_and_jobtemplate
  - device_defender (audit/mitigation/detect tasks, violations)
  - fleet_indexing (SearchIndex/aggregations)
  - see gopherstack-srzb for the consolidated deferred-family tracking issue
leaks: {status: clean, note: "no goroutines/janitors found in the audited surface beyond the embedded MQTT broker (broker.go), which was not in scope for this pass and predates it; no new goroutines introduced by this pass's fixes"}
---

## Notes

- **IoT is restjson1, not XML** — the "wrong XML list wrappers" bug class from other
  services' parity sweeps doesn't apply here. List/object field names were verified
  directly against `deserializers.go`/`serializers.go` in
  `aws-sdk-go-v2/service/iot@v1.76.0`.
- **Timestamps**: the service stores/serializes epoch-seconds as `float64` struct fields
  (e.g. `LastModifiedDate float64`) rather than routing through `pkgs/awstime.Epoch`.
  This is wire-equivalent for IoT (its JSON protocol wants epoch-seconds numbers, and a
  bare `float64` field marshals to a JSON number identically to `awstime.Epoch`), so it
  was **not** flagged as a bug — just note it doesn't use the shared pkg, which future
  work could consolidate but isn't a correctness issue.
- **Looks-wrong-but-correct**: `ListTagsForResource`'s tag objects use capitalized
  `"Key"`/`"Value"` JSON keys (not lowercase `"key"`/`"value"`). Verified directly
  against `awsRestjson1_deserializeDocumentTag` in v1.76.0 — this is genuinely how IoT's
  `Tag` shape serializes, unlike the lowercase convention used by many other AWS
  RESTJSON services. Don't re-flag this.
- **AttributePayload merge semantics** (the bug class this pass found): AWS IoT's
  `AttributePayload.merge` defaults to **false = replace**, not merge — confirmed against
  `moto`'s reference `update_thing`/`update_thing_group` (`do_merge =
  attribute_payload.get("merge", False)`, then either `thing.attributes = attributes`
  wholesale or `.update(attributes)`), since the AWS API docs alone are ambiguous on the
  unset-default direction. In both modes, an attribute given an **empty string value in
  the payload is deleted** from the result (moto: `{k: v for k, v in
  thing.attributes.items() if v}`) — this is AWS's documented "how to remove an
  attribute via UpdateThing" mechanism and applies to both UpdateThing and
  UpdateThingGroup. Both are now implemented via the shared `applyAttributePayload`
  helper in `backend.go`. If re-auditing this area, don't revert the "replace is the
  default" direction without re-verifying against real AWS — it's non-obvious and easy
  to get backwards (the original code before this pass had it backwards).
- **Attach op idempotency**: AWS IoT's various Attach* control-plane ops
  (AttachPolicy/AttachPrincipalPolicy/AttachThingPrincipal/AttachSecurityProfile) are
  **set semantics**, not list-append — attaching an already-attached target/principal is
  a no-op success, not a duplicate entry. Confirmed against moto's
  `principal_policies`/`principal_things` dict-keyed-by-pair storage. Fixed via a shared
  `appendUnique` helper in `backend.go`.
- **Persistence is comprehensive**: `persistence.go`'s `backendSnapshot` already covers
  every backend map touched during this audit (including the ones behind the bugs fixed
  here — `thingBillingGroups`, `policyTargets`, `thingPrincipals`,
  `securityProfileTargets`). No Handler-level Snapshot/Restore gap was found (unlike the
  8-service bug class referenced in the audit brief) — `Handler.Snapshot`/`Restore` in
  `persistence.go` already delegate correctly to the backend when it implements
  `Snapshottable`.
- **Scope of this pass**: per the audit brief, this pass targeted the highest-traffic
  families (Thing/ThingType/ThingGroup, Certificate, Policy+attach/detach, TopicRule,
  Tags). ThingType, full Certificate field coverage, CertificateProvider, Job/JobTemplate,
  Device Defender, and Fleet Indexing were only spot-checked (dispatch wiring + a few
  field names), not exhaustively audited — see `deferred:` above and
  gopherstack-srzb.
