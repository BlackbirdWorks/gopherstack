---
service: elbv2
sdk_module: aws-sdk-go-v2/service/elasticloadbalancingv2@v1.54.8
last_audit_commit: d118e0d8
last_audit_date: 2026-07-05
overall: A            # ~240 LOC genuine production-code fixes across several severe/systemic bugs (+~260 LOC test changes/additions); most of the surface was already accurate (see families below)
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
  CreateRule: {wire: ok, errors: ok, state: ok, persist: ok, note: "added fallback to the legacy top-level Values.member.N field for host-header/path-pattern conditions when the modern HostHeaderConfig/PathPatternConfig is absent (both are valid on the real wire)"}
  DeleteRule: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeRules: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyRule: {wire: ok, errors: ok, state: ok, persist: ok, note: "same legacy-Values fallback as CreateRule"}
  SetRulePriorities: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: the priority-conflict error code was fabricated (\"DuplicatePriority\"); real AWS code is \"PriorityInUse\" (PriorityInUseException)"}
  AddTags: {wire: ok, errors: ok, state: ok, persist: ok}
  RemoveTags: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeTags: {wire: ok, errors: ok, state: ok, persist: ok}
  AddListenerCertificates: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeListenerCertificates: {wire: ok, errors: ok, state: ok, persist: ok}
  RemoveListenerCertificates: {wire: ok, errors: ok, state: ok, persist: ok}
  AddTrustStoreRevocations: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: response never returned AddTrustStoreRevocationsResult.TrustStoreRevocations at all (empty body) despite the mutation succeeding - classic disguised-stub shape; now echoes the added revocations with RevocationId/RevocationType/NumberOfRevokedEntries/TrustStoreArn"}
  CreateTrustStore: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteSharedTrustStoreAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteTrustStore: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAccountLimits: {wire: ok, errors: ok, state: ok, persist: n/a, note: "static limits table verified against AWS defaults"}
  DescribeCapacityReservation: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeSSLPolicies: {wire: ok, errors: ok, state: ok, persist: n/a, note: "static policy list verified against real AWS SSL policy names/ciphers"}
  DescribeTrustStoreAssociations: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeTrustStores: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeTrustStoreRevocations: {wire: ok, errors: ok, state: ok, persist: ok, note: "CRITICAL fix: response list field was named RevocationContents; real wire field (verified against the SDK deserializer) is TrustStoreRevocations. A real SDK client parsing this response would have silently received an EMPTY list on every call despite the mock holding real revocation data"}
  GetResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  GetTrustStoreCaCertificatesBundle: {wire: partial, errors: ok, state: ok, persist: n/a, note: "Location is always empty string; there is no real S3-backed bundle to point to in this emulator. Documented gap, not fixed (see gaps)"}
  GetTrustStoreRevocationContent: {wire: partial, errors: ok, state: ok, persist: n/a, note: "same Location-always-empty gap as GetTrustStoreCaCertificatesBundle"}
  ModifyCapacityReservation: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyIpPools: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyTrustStore: {wire: ok, errors: ok, state: ok, persist: ok}
  RemoveTrustStoreRevocations: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  error-codes-and-http-status: {status: ok, note: "SYSTEMIC fix — see Notes. All *NotFound / Duplicate* / ResourceInUse / OperationNotPermitted / InvalidConfigurationRequest / PriorityInUse sentinel errors now map to HTTP 400, matching real AWS query-protocol behaviour (verified against the elasticloadbalancingv2 api-2.json model, which sets httpStatusCode=400 for every exception shape in this service). Previously NotFound errors returned 404 and AlreadyExists/DuplicateListener returned 409, which is REST-JSON-style, not query-protocol-style (EC2, also query-protocol, already uses 400-for-everything in this codebase - confirmed as the established, correct pattern)."
  actions (forward/redirect/fixed-response/authenticate-cognito/authenticate-oidc): {status: ok, note: "verified field-by-field against Action/RedirectActionConfig/FixedResponseActionConfig/ForwardActionConfig/AuthenticateCognitoActionConfig/AuthenticateOidcActionConfig; all wire field names and nesting correct, no changes needed"}
  conditions (host-header/path-pattern/http-header/query-string/source-ip/http-request-method): {status: ok, note: "verified nested *Config wire shapes (HostHeaderConfig.Values.member.N etc.) against RuleCondition; added legacy top-level Values.member.N fallback for host-header/path-pattern (see CreateRule/ModifyRule above). RegexValues (a newer AWS condition feature) is not implemented - see deferred."}
  listener-certificates / ssl-policies / trust-stores (association, revocation add/remove/describe): {status: ok, note: "AddTrustStoreRevocations and DescribeTrustStoreRevocations wire bugs fixed (see ops above); everything else in this family verified correct"}
  target-health-lifecycle (initial->healthy transition, draining->removed transition, reason codes): {status: ok, note: "healthStateHealthy/unhealthy/initial/draining and Elb.InitialHealthChecking/Target.DeregistrationInProgress/Target.NotRegistered reason codes verified byte-for-byte against types.TargetHealthStateEnum/TargetHealthReasonEnum. Port-defaulting fix applies across Register/Deregister/DescribeTargetHealth (see ops above)."}
  load-balancer-attributes / target-group-attributes / listener-attributes (Modify/Describe): {status: ok, note: "unchanged this pass; default attribute maps for ALB vs NLB/GWLB verified against real AWS defaults"}
  capacity-reservation / ip-pools / resource-policy / account-limits / ssl-policies: {status: ok, note: "unchanged this pass; verified op-by-op, all accurate"}
gaps:
  - ASG/ECS -> ELBv2 target registration is cross-service: RegisterTargets/DeregisterTargets/DescribeTargetHealth on the ELBv2 side are correct and complete (verified and improved this pass - see ops), but nothing on the ASG/ECS side calls them when instances/tasks scale (bd: gopherstack-18k) - NOT fixed here, out of scope per task instructions (elbv2-only edits)
  - GetTrustStoreCaCertificatesBundle / GetTrustStoreRevocationContent always return an empty Location (no real S3-backed object to point to) - documented simplification, not a hidden stub (the ops correctly validate the trust store/revocation exist and return 400 TrustStoreNotFound/RevocationIdNotFound otherwise)
  - RevocationId is modeled/returned as a string in this mock (echoing the caller-supplied plain content, or a generated "s3-<uuid>" for S3-structured entries); real AWS RevocationId is an int64 assigned by AWS when it parses the uploaded CRL/bundle. Existing tests in this package already encode string-shaped RevocationIds (e.g. "s3://my-bucket/revocations.crl", "1") predating this pass. Reworking this to a real monotonic int64 ID space is a moderate, invasive change (touches the TrustStoreRevocation struct, persistence JSON shape, and ~6 existing tests) for a rarely-exercised feature; deferred rather than rushed. No bd id filed yet - recommend filing one if this is prioritized.
  - The plain (non-S3) `RevocationContents.member.N` request field parsed by parseTrustStoreRevocations does not exist in the real AWS API (RevocationContent is always S3Bucket/S3Key/S3ObjectVersion/RevocationType - verified against types.RevocationContent) - harmless (real clients never send that shape so the branch is simply unreachable in practice), but worth removing in a future cleanup pass
deferred:
  - RegexValues on rule conditions (a newer AWS feature letting host-header/path-pattern/http-header conditions match by regex instead of exact/wildcard Values) - not implemented at all; SDK-side type exists (RuleCondition.RegexValues []string) but this pass did not add parsing/serialization for it
  - AuthenticateCognitoConfig/AuthenticateOidcConfig were verified for field-name accuracy only, not behaviorally exercised (this emulator does not implement actual OIDC/Cognito redirect flows, matching every other gopherstack service's scope for auth actions)
leaks: {status: clean, note: "runHealthReconciler's ticker-based goroutine is unchanged and already correctly stopped by Close(); targetReadyAt/targetDrainingUntil maps were the one place persistence was incomplete (see Notes) - now both fully round-trip through Snapshot/Restore, including a nil-map defensive re-init on Restore for snapshots taken before targetDrainingUntil existed (assigning into a nil top-level map panics on next write, e.g. the next RegisterTargets/DeregisterTargets call after a restore)."}
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
