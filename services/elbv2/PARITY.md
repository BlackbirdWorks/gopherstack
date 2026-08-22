---
service: elbv2
sdk_module: aws-sdk-go-v2/service/elasticloadbalancingv2@v1.58.5   # bumped from v1.54.8 this pass (go.mod already pinned v1.58.5; PARITY.md was stale)
last_audit_commit: 198990e82
last_audit_date: 2026-08-07
overall: A            # this pass: re-verified the 2026-07-05 audit's claims and field-diffed the
                       # two items it had explicitly left deferred/gap'd. Found and fixed two real
                       # bugs: RegexValues (a real SDK field) was entirely unimplemented, and the
                       # AddTrustStoreRevocations request/response shape had a gopherstack-invented
                       # field (plain, non-S3 RevocationContents.member.N) plus a wrong scalar type
                       # (RevocationId modeled as string; real wire type is int64). Also surveyed the
                       # full types.go surface for fields added to the SDK since the last audit;
                       # several genuinely new (2024/2025-era) AWS features are not yet implemented
                       # here -- see deferred below, listed with reasons rather than silently ok'd.
                       # 2026-08-07 pass (bd gopherstack-q1z2): implemented Rule.Transforms
                       # (host-header-rewrite/url-rewrite) end to end on CreateRule/ModifyRule/
                       # DescribeRules -- see families.rule-transforms. Remaining deferred items
                       # (jwt-validation action, TargetHealth AnomalyDetection/AdministrativeOverride,
                       # LoadBalancer IPAM/Outposts fields, MutualAuthentication
                       # AdvertiseTrustStoreCaNames, CreateTargetGroup default-attribute completeness)
                       # not touched this pass -- still real, still out of scope, see deferred/gaps.
ops:
  CreateLoadBalancer: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteLoadBalancer: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeLoadBalancers: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: NotFound errors were HTTP 404 (should be 400, see Notes)"}
  ModifyLoadBalancerAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeLoadBalancerAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  SetSecurityGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  SetSubnets: {wire: ok, errors: ok, state: ok, persist: ok}
  SetIpAddressType: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateTargetGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteTargetGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeTargetGroups: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyTargetGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyTargetGroupAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeTargetGroupAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  RegisterTargets: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: omitted Targets.member.N.Port was stored as 0 instead of defaulting to the target group's port (AWS behaviour), corrupting DescribeTargetHealth/Deregister lookups for any caller that omits Port"}
  DeregisterTargets: {wire: ok, errors: ok, state: ok, persist: ok, note: "same Port-defaulting fix as RegisterTargets"}
  DescribeTargetHealth: {wire: ok, errors: ok, state: ok, persist: ok, note: "Targets.member.N filter now also defaults omitted Port before matching against registered targets"}
  CreateListener: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: AlpnPolicy was modeled/serialized as a bare string; real wire shape is a list (AlpnPolicy.member.N request, <AlpnPolicy><member> response)"}
  DeleteListener: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeListeners: {wire: ok, errors: ok, state: ok, persist: ok, note: "AlpnPolicy list-shape fix applies here too"}
  ModifyListener: {wire: ok, errors: ok, state: ok, persist: ok, note: "AlpnPolicy list-shape fix applies here too"}
  ModifyListenerAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeListenerAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateRule: {wire: ok, errors: ok, state: ok, persist: ok, note: "added fallback to the legacy top-level Values.member.N field for host-header/path-pattern conditions when the modern HostHeaderConfig/PathPatternConfig is absent (both are valid on the real wire). NEW 2026-08-07: Transforms (types.RuleTransform, host-header-rewrite/url-rewrite) now parsed/validated/stored/returned -- see families.rule-transforms"}
  DeleteRule: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeRules: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyRule: {wire: ok, errors: ok, state: ok, persist: ok, note: "same legacy-Values fallback as CreateRule. NEW 2026-08-07: Transforms/ResetTransforms now handled -- ResetTransforms clears Transforms, a non-empty Transforms replaces it, and specifying both is rejected (InvalidParameter), matching ModifyRuleInput's doc comment"}
  SetRulePriorities: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: the priority-conflict error code was fabricated (\"DuplicatePriority\"); real AWS code is \"PriorityInUse\" (PriorityInUseException)"}
  AddTags: {wire: ok, errors: ok, state: ok, persist: ok}
  RemoveTags: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeTags: {wire: ok, errors: ok, state: ok, persist: ok}
  AddListenerCertificates: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeListenerCertificates: {wire: ok, errors: ok, state: ok, persist: ok}
  RemoveListenerCertificates: {wire: ok, errors: ok, state: ok, persist: ok}
  AddTrustStoreRevocations: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed (2026-07-05): response never returned AddTrustStoreRevocationsResult.TrustStoreRevocations at all (empty body) despite the mutation succeeding - classic disguised-stub shape; now echoes the added revocations with RevocationId/RevocationType/NumberOfRevokedEntries/TrustStoreArn. fixed (2026-07-23): the request parser accepted a plain, non-S3 `RevocationContents.member.N` value (e.g. a bare string) as a complete revocation entry - this shape does NOT exist on the real wire (types.RevocationContent is always S3Bucket/S3Key/S3ObjectVersion/RevocationType, verified against serializers.go's awsAwsquery_serializeDocumentRevocationContent); DELETED per the no-invented-fields rule. Also fixed: RevocationId was generated client-request-side as a string (a literal echo of the invented plain field, or a `\"s3-<uuid>\"` for S3-structured entries) - real AWS RevocationId is int64, assigned server-side when AWS parses the uploaded file, never client-supplied (verified against types.TrustStoreRevocation.RevocationId *int64). Now backend-assigned via a monotonic int64 counter (InMemoryBackend.revocationIDCounter, persisted in Snapshot/Restore)."}
  CreateTrustStore: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED (2026-08-13, bd gopherstack-hl3h): the request parser never read CaCertificatesBundleS3Bucket/CaCertificatesBundleS3Key (both required on CreateTrustStoreInput, verified against elasticloadbalancingv2@v1.58.5 api_op_CreateTrustStore.go:34-42) or the optional CaCertificatesBundleS3ObjectVersion; wire: ok was false -- these were silently dropped on every real client call. Now read and stored on TrustStore (CaCertificatesBundleS3Bucket/Key/ObjectVersion, all inert: no real S3 backing to fetch the bundle from, so they never feed NumberOfCaCerts or bundle content -- see GetTrustStoreCaCertificatesBundle's own documented gap below). Not exposed on DescribeTrustStores' response (real AWS doesn't return them there either)."}
  DeleteSharedTrustStoreAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteTrustStore: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAccountLimits: {wire: ok, errors: ok, state: ok, persist: n/a, note: "static limits table verified against AWS defaults"}
  DescribeCapacityReservation: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeSSLPolicies: {wire: ok, errors: ok, state: ok, persist: n/a, note: "static policy list verified against real AWS SSL policy names/ciphers"}
  DescribeTrustStoreAssociations: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeTrustStores: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeTrustStoreRevocations: {wire: ok, errors: ok, state: ok, persist: ok, note: "CRITICAL fix (2026-07-05): response list field was named RevocationContents; real wire field (verified against the SDK deserializer) is TrustStoreRevocations. A real SDK client parsing this response would have silently received an EMPTY list on every call despite the mock holding real revocation data. RevocationId is now int64 (see AddTrustStoreRevocations note, 2026-07-23)."}
  GetResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  GetTrustStoreCaCertificatesBundle: {wire: partial, errors: ok, state: ok, persist: n/a, note: "Location is always empty string; there is no real S3-backed bundle to point to in this emulator. Documented gap, not fixed (see gaps)"}
  GetTrustStoreRevocationContent: {wire: ok, errors: ok, state: ok, persist: n/a, note: "same Location-always-empty gap as GetTrustStoreCaCertificatesBundle (see gaps). FIXED (2026-08-13, bd gopherstack-hl3h): the request parser never read RevocationId (required on GetTrustStoreRevocationContentInput, verified against api_op_GetTrustStoreRevocationContent.go:29-39) and never checked whether the revocation existed on the trust store -- any RevocationId, including one nobody ever assigned, silently returned 200. Now RevocationId is required and validated against the trust store's revocations, returning RevocationIdNotFound (400) when absent, matching the real deserializer's error switch."}
  ModifyCapacityReservation: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyIpPools: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyTrustStore: {wire: ok, errors: ok, state: ok, persist: ok, note: "CRITICAL fix (2026-08-13, bd gopherstack-einq): the request parser read a `Name` param that does not exist on ModifyTrustStoreInput at all (verified against elasticloadbalancingv2@v1.58.5 api_op_ModifyTrustStore.go:33-49 -- the only fields are TrustStoreArn, CaCertificatesBundleS3Bucket, CaCertificatesBundleS3Key, CaCertificatesBundleS3ObjectVersion) -- every real client's call was silently renaming trust stores based on a field AWS never sends, wire: ok was false. Name-reading removed; ModifyTrustStore is now a validating lookup (TrustStoreArn must exist). FIXED (2026-08-13, bd gopherstack-hl3h): CaCertificatesBundleS3Bucket/Key/ObjectVersion are now read and stored on TrustStore, same inert-content model as CreateTrustStore (line above) -- both ops wire the same shape consistently, neither validates the two required fields are non-empty (would break the many existing tests that create/modify trust stores without a bundle; AWS-side rejection of a missing required field is not modeled here, matching this emulator's general non-enforcement of SDK-level 'This member is required' constraints elsewhere)."}
  RemoveTrustStoreRevocations: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed (2026-07-23): RevocationIds.member.N is a list of int64 on the real wire (types.RemoveTrustStoreRevocationsInput.RevocationIds []int64); the mock previously treated each entry as an opaque string. Now parses each member as an int64 (ErrInvalidParameter on a non-numeric entry)."}
families:
  error-codes-and-http-status: {status: ok, note: "SYSTEMIC fix — see Notes. All *NotFound / Duplicate* / ResourceInUse / OperationNotPermitted / InvalidConfigurationRequest / PriorityInUse sentinel errors now map to HTTP 400, matching real AWS query-protocol behaviour (verified against the elasticloadbalancingv2 api-2.json model, which sets httpStatusCode=400 for every exception shape in this service). Previously NotFound errors returned 404 and AlreadyExists/DuplicateListener returned 409, which is REST-JSON-style, not query-protocol-style (EC2, also query-protocol, already uses 400-for-everything in this codebase - confirmed as the established, correct pattern)."
  actions (forward/redirect/fixed-response/authenticate-cognito/authenticate-oidc): {status: ok, note: "verified field-by-field against Action/RedirectActionConfig/FixedResponseActionConfig/ForwardActionConfig/AuthenticateCognitoActionConfig/AuthenticateOidcActionConfig; all wire field names and nesting correct, no changes needed"}
  conditions (host-header/path-pattern/http-header/query-string/source-ip/http-request-method): {status: ok, note: "verified nested *Config wire shapes (HostHeaderConfig.Values.member.N etc.) against RuleCondition; added legacy top-level Values.member.N fallback for host-header/path-pattern (see CreateRule/ModifyRule above). fixed (2026-07-23): RegexValues (types.RuleCondition.RegexValues / HostHeaderConditionConfig.RegexValues / PathPatternConditionConfig.RegexValues / HttpHeaderConditionConfig.RegexValues - valid only for host-header, path-pattern, http-header) is now parsed (nested *Config.RegexValues.member.N form, with a top-level Conditions.member.N.RegexValues.member.N fallback matching the Values precedent) and serialized back on CreateRule/ModifyRule/DescribeRules responses. Condition.RegexValues added to the backend model."}
  rule-transforms (host-header-rewrite/url-rewrite): {status: ok, note: "NEW 2026-08-07 (bd gopherstack-q1z2). Field-diffed against types.RuleTransform/HostHeaderRewriteConfig/UrlRewriteConfig/RewriteConfig and the query-protocol serializers/deserializers (Transforms.member.N.Type, .HostHeaderRewriteConfig.Rewrites.member.M.{Regex,Replace}, .UrlRewriteConfig.Rewrites.member.M.{Regex,Replace} on request; same nested shape under Rule.Transforms on response). CreateRule/ModifyRule/DescribeRules all round-trip Transforms. Validation matches the real API's documented constraints (verified against RuleTransform/ModifyRuleInput doc comments, not invented): Type restricted to host-header-rewrite/url-rewrite, at most one of each type per rule, each RewriteConfig requires both Regex and Replace (both 'This member is required' on the real type), and ModifyRule rejects specifying both Transforms and ResetTransforms in the same request (documented mutually exclusive). ResetTransforms clears Transforms entirely; a non-empty Transforms without ResetTransforms replaces the existing list (same optional-patch semantics as Actions/Conditions)."}
  listener-certificates / ssl-policies / trust-stores (association and revocation add/remove/describe): {status: ok, note: "AddTrustStoreRevocations and DescribeTrustStoreRevocations wire bugs fixed (see ops above); everything else in this family verified correct. FIXED (2026-08-13, bd gopherstack-hl3h): CreateTrustStore/ModifyTrustStore's CaCertificatesBundleS3Bucket/Key/ObjectVersion and GetTrustStoreRevocationContent's RevocationId (see ops above)."}
  target-health-lifecycle (initial-to-healthy transition / draining-to-removed transition / reason codes): {status: ok, note: "healthStateHealthy/unhealthy/initial/draining and Elb.InitialHealthChecking/Target.DeregistrationInProgress/Target.NotRegistered reason codes verified byte-for-byte against types.TargetHealthStateEnum/TargetHealthReasonEnum. Port-defaulting fix applies across Register/Deregister/DescribeTargetHealth (see ops above)."}
  load-balancer-attributes / target-group-attributes / listener-attributes (Modify/Describe): {status: partial, note: "load-balancer-attributes/listener-attributes unchanged this pass, previously verified against real AWS defaults. target-group-attributes: ModifyTargetGroupAttributes/DescribeTargetGroupAttributes wire shape and any explicitly-set key/value round-trip correctly, but CreateTargetGroup's default attribute map (target_groups.go, 5 keys: deregistration_delay.timeout_seconds/stickiness.enabled/stickiness.type/load_balancing.algorithm.type/slow_start.duration_seconds) is missing several attributes real AWS always pre-populates on DescribeTargetGroupAttributes (verified against types.TargetGroupAttribute's doc comment: proxy_protocol_v2.enabled, preserve_client_ip.enabled, stickiness.app_cookie.*, target_group_health.dns_failover.*/unhealthy_state_routing.*, target_health_state.unhealthy.*, deregistration_delay.connection_termination.enabled, load_balancing.algorithm.anomaly_mitigation, target_failover.on_deregistration/on_unhealthy, and lambda.multi_value_headers.enabled for Lambda target groups) - see deferred"}
  capacity-reservation / ip-pools / resource-policy / account-limits / ssl-policies: {status: ok, note: "unchanged this pass; verified op-by-op, all accurate"}
gaps:
  - ASG/ECS -> ELBv2 target registration is cross-service: RegisterTargets/DeregisterTargets/DescribeTargetHealth on the ELBv2 side are correct and complete (verified and improved this pass - see ops), but nothing on the ASG/ECS side calls them when instances/tasks scale (bd: gopherstack-18k) - NOT fixed here, out of scope per task instructions (elbv2-only edits)
  - GetTrustStoreCaCertificatesBundle / GetTrustStoreRevocationContent always return an empty Location (no real S3-backed object to point to) - documented simplification, not a hidden stub (the ops correctly validate the trust store/revocation exist and return 400 TrustStoreNotFound/RevocationIdNotFound otherwise). UPDATED (2026-08-13, bd gopherstack-hl3h): the RevocationIdNotFound check was previously not implemented despite this gap note claiming it was (GetTrustStoreRevocationContent never read RevocationId at all) - now genuinely true, see the op's PARITY note above. CreateTrustStore/ModifyTrustStore's CaCertificatesBundleS3Bucket/Key/ObjectVersion are recorded on TrustStore (same pass) but likewise never used to produce real bundle content, for the same no-real-S3 reason.
  - CreateTargetGroup's default TargetGroupAttributes map only pre-populates 5 of the ~15+ attribute keys real AWS always returns from DescribeTargetGroupAttributes (see target-group-attributes family note above) - explicitly-set attributes still round-trip correctly via ModifyTargetGroupAttributes, so this is a completeness gap in the *defaults*, not a wire-shape bug; deferred rather than rushed because the correct default value differs per target type (instance/ip vs lambda) and expanding the map risks breaking the ~30 existing tests that assert on today's 5-key map. No bd id filed yet - recommend filing one if prioritized.
deferred:
  - "IMPLEMENTED 2026-08-07 (bd gopherstack-q1z2): RuleTransforms -- see families.rule-transforms above."
  - jwt-validation is a real, newer Action type (types.ActionTypeEnum has "jwt-validation" alongside forward/redirect/fixed-response/authenticate-oidc/authenticate-cognito; backed by JwtValidationActionConfig/JwtValidationActionAdditionalClaim) not implemented here at all - not in this task's explicit actions checklist (forward/redirect/fixed-response/authenticate-oidc/authenticate-cognito). No bd id filed yet.
  - AnomalyDetection (types.TargetHealth.AnomalyDetection) and AdministrativeOverride (types.TargetHealth.AdministrativeOverride) are newer DescribeTargetHealth response fields (anomaly mitigation / zonal-shift administrative override status) not modeled on the backend Target/TargetHealthDescription types - not in this task's explicit target-health checklist. No bd id filed yet.
  - LoadBalancer fields added to the SDK since the last full field-diff: IpamPools, EnablePrefixForIpv6SourceNat, CustomerOwnedIpv4Pool, EnforceSecurityGroupInboundRulesOnPrivateLinkTraffic (types.LoadBalancer / CreateLoadBalancerInput) - all Outposts/IPAM/UDP-source-NAT niche features, not in this task's explicit CreateLoadBalancer checklist (subnets/subnetMappings/securityGroups/scheme/ipAddressType). No bd id filed yet.
  - MutualAuthenticationAttributes.AdvertiseTrustStoreCaNames and .TrustStoreAssociationStatus (types.go) are not modeled on the backend MutualAuthentication struct - a newer mTLS/shared-trust-store feature, not in this task's explicit trust-store checklist. No bd id filed yet.
  - AuthenticateCognitoConfig/AuthenticateOidcConfig were verified for field-name accuracy only, not behaviorally exercised (this emulator does not implement actual OIDC/Cognito redirect flows, matching every other gopherstack service's scope for auth actions)
leaks: {status: clean, note: "runHealthReconciler's ticker-based goroutine is unchanged and already correctly stopped by Close(); targetReadyAt/targetDrainingUntil maps were the one place persistence was incomplete (see Notes) - now both fully round-trip through Snapshot/Restore, including a nil-map defensive re-init on Restore for snapshots taken before targetDrainingUntil existed (assigning into a nil top-level map panics on next write, e.g. the next RegisterTargets/DeregisterTargets call after a restore). 2026-07-23: new revocationIDCounter field added under the same coarse b.mu lock as every other backend mutation (AddTrustStoreRevocations already held the write lock); no new lock paths, goroutines, or ghost rows introduced. elbv2SnapshotVersion bumped 1->2 (see Notes) because a v1 snapshot's trustStores table can hold string-shaped RevocationIds that would fail to unmarshal into the new int64 field - handled the same discard-cleanly-on-mismatch way as every other version bump in this codebase, not a partial-decode risk."}
---

## Notes

Protocol: ELBv2 uses the classic AWS "Query" protocol (form-urlencoded request,
`Version=2015-12-01`, XML response with `<OpNameResponse><OpNameResult>...`), the same
family as EC2 and Auto Scaling in this codebase. Verified against
`aws-sdk-go-v2/service/elasticloadbalancingv2@v1.54.8`'s `deserializers.go` /
`types/errors.go`, and cross-checked exact error codes + HTTP statuses against the
upstream `aws-sdk-go@v1.55.5` `models/apis/elasticloadbalancingv2/2015-12-01/api-2.json`
(every exception shape in that model declares `httpStatusCode: 400`, no exceptions).

### Highest-value finding: error HTTP status codes were REST-JSON-shaped, not query-protocol-shaped

Real AWS ELBv2 (and every other query-protocol service) returns **HTTP 400 for every
client error**, including NotFound and AlreadyExists conditions — the SDK's error
deserializer dispatches purely on the `<Code>` XML text (verified by reading
`awsAwsquery_deserializeOpErrorCreateLoadBalancer` etc.), never on HTTP status. Before
this pass, `elbv2ErrorCode` mapped `*NotFound` sentinels to `http.StatusNotFound` (404)
and `*AlreadyExists`/`DuplicateListener` to `http.StatusConflict` (409) — a REST-JSON
convention that doesn't apply here. This is invisible to a Go SDK client (which only
looks at the XML `<Code>`) but wire-inaccurate for anything that inspects the raw HTTP
status (curl-based tooling, non-SDK clients, some retry middlewares). Confirmed EC2 in
this same codebase (also query-protocol) already uses `http.StatusBadRequest`
uniformly for its entire `errCodeLookup` table — i.e. elbv2's 404/409 usage was the
anomaly relative to established codebase convention, not the norm. Fixed by changing
every `NotFound`/`AlreadyExists`/`DuplicateListener` mapping in `elbv2ErrorCode` to 400,
and updated ~45 test assertions across `handler_test.go`,
`handler_accuracy_batch1_test.go`, `handler_accuracy_batch2_test.go`, and
`parity_b_test.go` that had encoded the wrong (404/409) expectation.

### AlpnPolicy wire-shape bug (wrong list wrapper — parity-principles.md bug class #2)

`Listener.AlpnPolicy` was modeled as a bare `string`, parsed via
`vals.Get("AlpnPolicy.member.1")` and serialized as `<AlpnPolicy>value</AlpnPolicy>`.
Real AWS (`types.Listener.AlpnPolicy` is `[]string`, verified against the SDK's
`awsAwsquery_deserializeDocumentAlpnPolicyName` which decodes a `<member>`-wrapped
list) requires `AlpnPolicy.member.N` on requests and `<AlpnPolicy><member>...</member>
</AlpnPolicy>` on responses. Fixed end-to-end: `Listener.AlpnPolicy`,
`CreateListenerInput.AlpnPolicy`, `ModifyListenerInput.AlpnPolicy` are now `[]string`;
the handler parses all `AlpnPolicy.member.N` values; the XML projection uses a
`*xmlStringList` (nil when empty, omitted from the response, matching the
`Certificates` field's existing convention). Added `Test_AlpnPolicyWireShape`
(table-driven: single policy / multiple policies / no policy) covering
CreateListener + DescribeListeners round-trip.

### DescribeTrustStoreRevocations / AddTrustStoreRevocations wire-shape bugs

Two related bugs, same root cause (never checked the SDK deserializer for the exact
result field name):

1. `DescribeTrustStoreRevocationsResult`'s list field was named `RevocationContents`
   in the mock. The real field (verified against
   `awsAwsquery_deserializeOpDocumentDescribeTrustStoreRevocationsOutput`, which only
   recognizes `TrustStoreRevocations` and silently `Skip()`s anything else) is
   `TrustStoreRevocations`. A real SDK client would have received an **empty list on
   every call**, even though the mock's internal state held real revocation data —
   this is the "wrong root element" bug class from parity-principles.md #2, just one
   level deeper (correct outer `DescribeTrustStoreRevocationsResult` root, wrong inner
   list field).
2. `AddTrustStoreRevocations`'s response never included a Result section at all — the
   mutation succeeded server-side but the client got back an empty envelope. Real AWS
   returns `AddTrustStoreRevocationsResult.TrustStoreRevocations` describing exactly
   what was added (RevocationId/RevocationType/NumberOfRevokedEntries/TrustStoreArn).
   This is the "op returns real state but is missing the documented response shape"
   pattern flagged in parity-principles.md #4 — worth double-checking on any op whose
   test coverage only asserts `http.StatusOK` without decoding the body (which is
   exactly how this one stayed hidden: `TestELBv2_TrustStoreFullLifecycle` asserted
   `revRec.Code == 200` but never unmarshalled `revRec.Body`).

Both fixed; `xmlRevocationContent` gained a `TrustStoreArn` field (present on both real
AWS types `TrustStoreRevocation` and `DescribeTrustStoreRevocation`), and
`TestELBv2_TrustStoreFullLifecycle` was corrected to decode against
`TrustStoreRevocations` (not `RevocationContents`) and extended to assert the
`AddTrustStoreRevocations` response body content.

### PriorityInUse error code

`ErrDuplicateRulePriority` was wired to the fabricated code `"DuplicatePriority"`. Real
AWS (verified against `types.PriorityInUseException`/api-2.json) uses
`"PriorityInUse"`. Fixed in `backend.go` and `elbv2ErrorCode`; updated
`TestDuplicateRulePriorityErrorCode` (this test encoded the wrong behaviour, per
parity-principles.md's "fix tests only where they encoded wrong behavior" rule).

### Target port defaulting

`RegisterTargets`/`DeregisterTargets`/`DescribeTargetHealth`'s `Targets.member.N.Port`
is optional on the real wire (`types.TargetDescription.Port *int32`) — when omitted,
AWS defaults it to the target group's configured port. The mock previously stored/
matched on a bare `0` for any caller that omitted Port, which silently broke
`DescribeTargetHealth`/`DeregisterTargets` lookups for such targets (their registered
port would never match a later request's implicit-zero port unless the caller
"happened" to also omit Port every time). Fixed via a new
`Handler.resolveTargetGroupPort`/`defaultTargetPorts` pair applied in all three
handlers — additive, no `StorageBackend` interface signature change.

### Persistence gap: targetDrainingUntil never survived Restore

`InMemoryBackend.targetDrainingUntil` (tracks when a draining/deregistering target
should be actually removed from its target group) was entirely absent from
`backendSnapshot`. After a Restore, any target that was mid-drain at snapshot time
would keep its `HealthState="draining"` forever — `reconcileTargetHealth`'s expiry
goroutine had no record of when to remove it. Separately, `targetReadyAt` WAS in the
snapshot but `Restore()` was missing the standard nil-guard the other maps got,
meaning it could restore as a literal `nil` map for old/empty snapshots; the very next
`RegisterTargets` call assigning into `b.targetReadyAt[tgArn][key]` would then panic
("assignment to entry in nil map"). Both fixed: `targetDrainingUntil` added to the
snapshot type/Snapshot()/Restore(), and both `targetReadyAt`/`targetDrainingUntil` get
a defensive `make(...)` in `Restore()` when nil.

### Legacy top-level `Values` fallback for host-header/path-pattern conditions

AWS's `RuleCondition` has both a modern `HostHeaderConfig`/`PathPatternConfig` (list of
values) and a deprecated top-level `Values` field (single value, still accepted on the
wire per `types.go`'s doc comment). The mock only ever read the modern
`*Config.Values.member.N` form. Added a fallback to the legacy
`Conditions.member.N.Values.member.N` form when the modern form is empty — cheap,
additive, and closes a real (if rarely hit by modern SDKs/Terraform) parsing gap.

### RegexValues on rule conditions (2026-07-23)

`RuleCondition.RegexValues` (and the per-field `HostHeaderConditionConfig.RegexValues`
/ `PathPatternConditionConfig.RegexValues` / `HttpHeaderConditionConfig.RegexValues`)
lets host-header/path-pattern/http-header conditions match by regular expression
instead of exact/wildcard `Values`. Verified the wire shape against
`awsAwsquery_serializeDocumentRuleCondition`/`...HostHeaderConditionConfig` etc.: it's
a `RegexValues.member.N` list, sibling to `Values.member.N`, both at the nested
`*Config` level and (per `RuleCondition.RegexValues`) at the top level. Implemented
end-to-end: `Condition.RegexValues []string` added to the backend model; parsed via
`parseRegexValues` (nested `*Config.RegexValues.member.N` first, top-level
`Conditions.member.N.RegexValues.member.N` fallback, mirroring the existing legacy
`Values` fallback); serialized back via `xmlConditionValuesConfig.RegexValues` /
`xmlHTTPHeaderConfig.RegexValues` (both `*xmlStringList`, nil-when-empty, matching the
`AlpnPolicy` convention). `http-request-method`/`source-ip`/`query-string` conditions
don't support `RegexValues` on the real API and the parser never reads it for them.

### AddTrustStoreRevocations invented field + RevocationId wrong type (2026-07-23)

Two related bugs in the trust-store revocation family, both traced by field-diffing
`types.RevocationContent` / `types.TrustStoreRevocation` against the mock:

1. The request parser (`parseTrustStoreRevocations`, now
   `parseTrustStoreRevocationContents`) accepted a **plain, non-S3** form —
   `RevocationContents.member.N` as a bare string — as a complete revocation entry.
   This shape does not exist on the real wire; `types.RevocationContent` is always
   `S3Bucket`/`S3Key`/`S3ObjectVersion`/`RevocationType` (verified against
   `awsAwsquery_serializeDocumentRevocationContent`, which only ever writes those four
   keys). This was a gopherstack invention flagged in a prior pass's gaps list as
   "worth removing in a future cleanup pass" — deleted per the no-invented-fields rule.
2. `RevocationId` was modeled as a `string` — server-generated as `"s3-" +
   uuid.New().String()` for every entry (there being no plain caller-supplied ID left
   once (1) is deleted). Real AWS `RevocationId` is `*int64`, assigned by AWS itself
   when it parses the uploaded CRL/bundle — never client-supplied (verified against
   `types.TrustStoreRevocation.RevocationId` / `types.DescribeTrustStoreRevocation.
   RevocationId`, both `*int64`). A real SDK client unmarshalling this mock's old
   string-shaped `<RevocationId>s3-3fa8...</RevocationId>` into its generated
   `*int64` field would fail to decode. Fixed: `TrustStoreRevocation.RevocationID`
   is now `int64`; `InMemoryBackend.AddTrustStoreRevocations` assigns each new
   revocation an ID from a monotonic `revocationIDCounter` (same pattern as
   `ruleCounter`), under the existing write lock. `RemoveTrustStoreRevocations`'s
   `RevocationIds.member.N` request field is parsed as `int64` too (real wire type
   `[]int64`, verified against `RemoveTrustStoreRevocationsInput`).
   `elbv2SnapshotVersion` bumped 1→2 because this changes the JSON shape of the
   `trustStores` table (see leaks note above).

### Traps for the next auditor

- `probeTargetHTTP`'s "unreachable → treat as healthy" fallback (backend.go) looks
  backwards at first glance but is intentional: this emulator has no real backend
  server to health-check in the general case, so treating connection failures as
  healthy avoids every mock target group getting stuck "unhealthy" forever just
  because nothing is actually listening on the target's host:port. Don't "fix" this
  without re-reading the comment above it.
- `xmlRevocationContent` is now shared by both `AddTrustStoreRevocationsResult` and
  `DescribeTrustStoreRevocationsResult` — they are two *different* real AWS types
  (`TrustStoreRevocation` vs `DescribeTrustStoreRevocation`) that happen to have
  identical fields on the wire. This is intentional reuse, not a shortcut.
- The `RevocationContents.member.N` (S3Bucket/S3Key/RevocationType) request field name
  is correct and matches real AWS `RevocationContent` — don't confuse it with the
  *response*-side `TrustStoreRevocations` field name fixed this pass; they are
  different names for request vs. response despite both being about "revocation
  content".
- Every `errors: ok` HTTP status in `ops` above now means **400**, not "whatever seems
  RESTful" — re-verify against api-2.json (not intuition) before changing any status
  code in `elbv2ErrorCode`.

**2026-08-22 (gopherstack-ifzn) -- RouteMatcher swallowed a body-read failure as a 404,
masking Handler()'s already-typed InternalFailure**: same shape as autoscaling's entry
(see that entry or gopherstack-3a8t for the full survey/rationale). `RouteMatcher` now
falls back to `service.MatchesUserAgentMarker(r.Header, "api/elasticloadbalancingv2")`
(verified against the pinned `elasticloadbalancingv2@v1.58.5/api_client.go:638`
`AddSDKAgentKeyValue` call) only on the `ReadBody` failure branch. Migrated
`ExtractOperation`/`ExtractResource`/`Handler()` off `r.ParseForm()` onto
`httputils.ReadBody`+`url.ParseQuery`, per the docdb/neptune precedent (gopherstack-bahs).
Proof: `TestHandler_OversizedBodySurfacesInternalFailure` in `handler_oversized_body_test.go`
drives a real ELBv2 SDK client through `service.NewRegistry`/`service.NewServiceRouter`,
confirmed failing pre-fix with `UnknownError`; passes now with `InternalFailure`.
`TestHandler_NormalSizedBodyStillRoutes` is the regression guard. Gates: `go build`,
`go vet`, `gofmt -l` (clean), `go test -race ./services/elbv2/...` (pass),
`golangci-lint run ./services/elbv2/...` (0 issues).
