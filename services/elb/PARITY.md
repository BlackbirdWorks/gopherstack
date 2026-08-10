---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: elb
sdk_module: aws-sdk-go-v2/service/elasticloadbalancing@v1.33.21   # version audited against
last_audit_commit: c9c03908                       # HEAD when this audit began (working tree, pre-commit)
last_audit_date: 2026-07-24
overall: A            # A = genuine fixes found; B = already-accurate, proven op-by-op
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateLoadBalancer: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed InvalidScheme/UnsupportedProtocol/TooManyLoadBalancers/DuplicateTagKeys/TooManyTags error codes (were generic ValidationError); parity-3: inline HTTPS/SSL Listeners.member.N.SSLCertificateId now runs the same ARN-format check as SetLoadBalancerListenerSSLCertificate (was accepted unchecked at creation time, format-checked only on later Set calls); gopherstack-6851: inline SSLCertificateId now also existence-checked against the real ACM/IAM backends when cli.go's wireELBCrossService has wired a CertificateResolver in (nil resolver, e.g. isolated unit tests, stays permissive)"}
  DeleteLoadBalancer: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed missing DeleteLoadBalancerResult wrapper (real SDK GetElement failed)"}
  DescribeLoadBalancers: {wire: ok, errors: ok, state: ok, persist: ok, note: "parity-3: fixed missing LoadBalancerDescription.Policies field -- was entirely absent from the response struct, so every real client saw an always-empty Policies regardless of what stickiness/other policies existed; now populates AppCookieStickinessPolicies/LBCookieStickinessPolicies/OtherPolicies from the LB's policy set"}
  CreateLoadBalancerListeners: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed UnsupportedProtocol error code via shared parseOneListener; parity-3: fixed classic-listeners limit-exceeded error code (was ValidationError, real op's typed-error switch only has InvalidConfigurationRequest/CertificateNotFound/DuplicateListener/LoadBalancerNotFound/UnsupportedProtocol); inline SSLCertificateId now format-validated (see CreateLoadBalancer note); gopherstack-6851: now also existence-checked via the same CertificateResolver as CreateLoadBalancer/SetLoadBalancerListenerSSLCertificate"}
  DeleteLoadBalancerListeners: {wire: ok, errors: ok, state: ok, persist: ok}
  RegisterInstancesWithLoadBalancer: {wire: ok, errors: ok, state: ok, persist: ok, note: "parity-3: deleted invented classic-registered-instances (1000) hard-reject -- real op's typed-error switch only recognizes InvalidInstance/LoadBalancerNotFound, no typed exception exists for exceeding this DescribeAccountLimits-advertised limit, so enforcing it rejected requests a real AWS client would have had accepted"}
  DeregisterInstancesFromLoadBalancer: {wire: ok, errors: ok, state: ok, persist: ok}
  ConfigureHealthCheck: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyLoadBalancerAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeLoadBalancerAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  AddTags: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed TooManyTags error code (was ValidationError); added missing DuplicateTagKeys same-request validation"}
  DescribeTags: {wire: ok, errors: ok, state: ok, persist: ok}
  RemoveTags: {wire: ok, errors: ok, state: ok, persist: ok}
  ApplySecurityGroupsToLoadBalancer: {wire: ok, errors: ok, state: ok, persist: ok, note: "gopherstack-6851: now raises InvalidSecurityGroup for a security group that doesn't resolve against the real EC2 backend, via cli.go's wireELBCrossService (elb.EC2Resolver); nil resolver (e.g. isolated unit tests) stays permissive, matching this package's existing cross-service-resolver convention (services/directconnect/networkmanager)"}
  AttachLoadBalancerToSubnets: {wire: ok, errors: ok, state: ok, persist: ok, note: "gopherstack-6851: now raises SubnetNotFound for a subnet that doesn't resolve against the real EC2 backend, via the same EC2Resolver as ApplySecurityGroupsToLoadBalancer; nil resolver stays permissive"}
  DetachLoadBalancerFromSubnets: {wire: ok, errors: ok, state: ok, persist: ok}
  EnableAvailabilityZonesForLoadBalancer: {wire: ok, errors: ok, state: ok, persist: ok}
  DisableAvailabilityZonesForLoadBalancer: {wire: ok, errors: ok, state: ok, persist: ok}
  SetLoadBalancerListenerSSLCertificate: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed missing Result wrapper; gopherstack-6851: CertificateNotFound now raised for an SSLCertificateId that resolves to neither a real ACM nor a real IAM server certificate, via cli.go's wireELBCrossService (elb.CertificateResolver checks both, since AWS accepts either -- aws-sdk-go-v2/service/elasticloadbalancing@v1.36.4 types/errors.go:36-39); nil resolver stays permissive"}
  SetLoadBalancerPoliciesOfListener: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed missing Result wrapper"}
  SetLoadBalancerPoliciesForBackendServer: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed missing Result wrapper"}
  CreateAppCookieStickinessPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed missing Result wrapper"}
  CreateLBCookieStickinessPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed missing Result wrapper"}
  CreateLoadBalancerPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed missing Result wrapper; fixed PolicyTypeNotFound error code (was generic ValidationError); added missing PublicKeyPolicyType to allowlist; TooManyPolicies not enforced (gap, see below)"}
  DeleteLoadBalancerPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed missing Result wrapper; parity-3: fixed policy-still-in-use error code (was ValidationError, real op's typed-error switch only has InvalidConfigurationRequest/LoadBalancerNotFound -- a ValidationError code would not deserialize into InvalidConfigurationRequestException, so errors.As would silently fail to match on a real client). Proven by Test_SDKRoundTrip_DeleteLoadBalancerPolicyInUse_IsTyped"}
  DescribeAccountLimits: {wire: ok, errors: ok, state: ok, persist: n/a-static}
  DescribeInstanceHealth: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeLoadBalancerPolicies: {wire: ok, errors: ok, state: ok, persist: ok, note: "no-LoadBalancerName sample-policy fallback verified correct vs AWS docs, not a bug"}
  DescribeLoadBalancerPolicyTypes: {wire: ok, errors: ok, state: ok, persist: n/a-static, note: "fixed wrong PolicyTypeNotFound error code (was reusing PolicyNotFound, the policy-instance sentinel)"}
# Families audited as a group (when per-op is impractical):
families:
  snapshot_restore: {status: ok, note: "Handler-level Snapshot/Restore delegation (persistence.go) verified intact; backend.Snapshot/Restore round-trip all LB + policy state incl. tags; version-guarded (v4) against incompatible older snapshots"}
  route_matcher: {status: ok, note: "single query/xml POST matcher (Version=2012-06-01 form field) confirmed reachable for all 29 dispatch-table ops; TestSDKCompleteness passes with empty notImplemented list"}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - gopherstack-6851 FOLLOW-UP addressed this pass: ApplySecurityGroupsToLoadBalancer/AttachLoadBalancerToSubnets now validate SecurityGroups/Subnets against the real EC2 backend (elb.EC2Resolver, wired by cli.go's wireELBCrossService), and CreateLoadBalancer/CreateLoadBalancerListeners/SetLoadBalancerListenerSSLCertificate now validate SSLCertificateId against the real ACM and IAM backends (elb.CertificateResolver, same wiring call). CreateLoadBalancer's own SecurityGroups/Subnets fields (as opposed to Apply/Attach) are NOT existence-checked -- out of scope for this pass, tracked separately if ever needed.
  - CreateLoadBalancerPolicy has no TooManyPolicies limit (AWS models TooManyPoliciesException for this op per the SDK's op-specific error switch, but no default per-LB policy count limit is documented anywhere gopherstack could source a correct number from; fabricating one risked being wrong, so left unenforced rather than guessed). Re-verified gopherstack-6851 2026-08-10: the official quota table at docs.aws.amazon.com/elasticloadbalancing/latest/classic/elb-limits.html lists exactly three Classic ELB quotas (Load Balancers per Region: 20, Listeners per Classic Load Balancer: 100, Registered Instances per Classic Load Balancer: 1,000) and no policies-per-load-balancer quota -- confirmed absent, not just unfound, so still deliberately left unenforced.
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - none — full op-by-op pass completed this round (parity-3: re-verified after the Go-refactoring-2 file split; all backend.go/handler.go logic re-read post-split and re-diffed against the SDK)
leaks: {status: clean, note: "Reset()/Snapshot()/Restore() all close+recreate tags.Tags registries correctly (no Prometheus label leak); DeleteLoadBalancer cascade-deletes policies via policiesByLB index with a cloned slice before delete to avoid corrupting the in-progress scan"}
---

## Notes

Protocol: query/xml (single POST, `Action=` form param, `Version=2012-06-01`). Root
namespace `http://elasticloadbalancing.amazonaws.com/doc/2012-06-01/`.

### Bug class found this pass: missing `<XxxResult>` wrapper elements (8 ops)

`DeleteLoadBalancer`, `SetLoadBalancerListenerSSLCertificate`,
`SetLoadBalancerPoliciesOfListener`, `SetLoadBalancerPoliciesForBackendServer`,
`CreateAppCookieStickinessPolicy`, `CreateLBCookieStickinessPolicy`,
`CreateLoadBalancerPolicy`, and `DeleteLoadBalancerPolicy` all have void (no-payload)
results in the real API, but the real deserializer still unconditionally calls
`NodeDecoder.GetElement("<Op>Result")` before returning success — see
`deserializers.go`'s per-op `awsAwsquery_deserializeOp*` functions, each of which does
`t, err = decoder.GetElement("<Op>Result")` even when there's nothing to decode inside
it. `GetElement` returns `"<name> node not found"` if the element is absent
(`smithy-go@.../encoding/xml/xml_decoder.go:82`). Before this fix, gopherstack's
response structs for these 8 ops omitted the `Result` field/tag entirely, so **every
real aws-sdk-go-v2 call to any of these 8 operations failed client-side with a
deserialization error**, even though the operation succeeded server-side and mutated
state correctly. This is the same bug class documented in rds/neptune/docdb parity
sweeps ("required `XxxResult` wrapper elements"). Fixed by adding an empty
`type <op>Result struct{}` with the correct `xml:"<Op>Result"` tag to each response
struct, matching the pattern already used correctly by
`createLoadBalancerListenersResponse` / `addTagsResponse` / etc.

**Trap for the next auditor**: don't assume a void-result op's response struct is
correct just because it "looks empty and harmless" — check it actually declares a
`Result` field with the right `xml:"<Op>Result"` tag. An empty response struct with
only `XMLName`/`Xmlns`/`ResponseMetadata` fields is *always* wrong for this SDK's
query/xml protocol, because `GetElement` is unconditional in the generated
deserializer regardless of whether the shape has members.

### Error-code parity, verified directly against the SDK's per-op error switch tables

`deserializers.go` has an `awsAwsquery_deserializeOpError<OpName>` function per
operation with a `switch { case strings.EqualFold("<Code>", errorCode): ... }` table
listing exactly which typed exceptions that op can produce. This is the ground truth
for which wire `<Code>` string is expected — cross-referencing every backend/handler
sentinel against these tables (not just `types/errors.go`'s existence list) surfaced:

- `TooManyLoadBalancers` (not `ValidationError`) for `CreateLoadBalancer`'s 20-LB limit
  (`AccessPointNotFoundException`'s sibling `TooManyAccessPointsException`).
- `TooManyTags` (not `ValidationError`) for `AddTags`' / `CreateLoadBalancer`'s 10-tag
  limit.
- `DuplicateTagKeys` — was not validated at all; a same-request duplicate tag key
  silently overwrote instead of erroring. Added to `AddTags` (and transitively to
  `CreateLoadBalancer`'s inline `Tags`, since it calls `Backend.AddTags`).
- `InvalidScheme` (not `ValidationError`) for `CreateLoadBalancer`'s Scheme validation.
- `UnsupportedProtocol` (not `ValidationError`) for a listener's `Protocol` value
  outside HTTP/HTTPS/TCP/SSL.
- `PolicyTypeNotFound` (not `PolicyNotFound`) for an unknown `PolicyTypeName` in both
  `CreateLoadBalancerPolicy` and `DescribeLoadBalancerPolicyTypes` —
  `PolicyTypeNotFoundException` and `PolicyNotFoundException` are two *distinct* typed
  exceptions in the SDK (policy type vs. policy instance); gopherstack was reusing the
  policy-instance one for both.
- `PublicKeyPolicyType` was missing from the `CreateLoadBalancerPolicy` policy-type
  allowlist even though it's a real, built-in Classic ELB policy type (used for
  back-end server authentication, listed by `DescribeLoadBalancerPolicyTypes`) — every
  real attempt to create one was wrongly rejected as "unknown policy type".

All six fixes are proven by `handler_sdk_roundtrip_test.go`, which drives the real
`aws-sdk-go-v2/service/elasticloadbalancing` client against an `httptest` server and
asserts `errors.As` into the exact typed exception struct (e.g.
`*types.TooManyAccessPointsException`), not just an HTTP status code — this is the only
way to prove the wire `<Code>` string round-trips correctly through the real
deserializer's error-type dispatch.

### Confirmed correct (not bugs, don't re-flag)

- `DescribeLoadBalancerPolicies` with no `LoadBalancerName` returning the 4 built-in
  `ELBSample-*`/`ELBSecurityPolicy-*` sample policies (not all policies across the
  account) is genuine documented AWS behavior, not a stub.
- `CreatedTime` is emitted as `time.RFC3339`; the real deserializer parses it with
  `smithytime.ParseDateTime`, which accepts RFC3339 with optional fractional seconds —
  compatible.
- `elb.http.desyncmitigationmode` as an `AdditionalAttributes` key on
  `LoadBalancerAttributes` is a real Classic-ELB attribute (AWS added desync
  mitigation mode to Classic ELB, not just ALB) — not a fabricated attribute.
- `<member>` list-wrapper convention (`LoadBalancerDescriptions`, `Instances`,
  `ListenerDescriptions`, `Tags`, etc.) matches
  `awsAwsquery_deserializeDocument*` list decoders throughout; no over-nesting found
  outside the 8 missing-Result-wrapper ops above.
- Snapshot/Restore Handler-level delegation (`persistence.go`) is intact: `Handler`
  type-asserts its `StorageBackend` to a `snapshotter`/`restorer` interface and
  delegates to `InMemoryBackend.Snapshot`/`Restore`, exactly mirroring the
  `services/securityhub` pattern referenced in its doc comment.

## parity-3 pass (2026-07-24)

Since the 2026-07-12 audit, `services/elb/` went through a full file-split refactor
(`backend.go`/`handler.go` → one file per resource family: `load_balancers.go`,
`listeners.go`, `instances.go`, `policies.go`, `attributes.go`, etc., plus matching
`handler_*.go` files) with no intended behavior change. This pass re-read every
production file post-split and re-diffed each op's wire shape and error codes against
`deserializers.go`'s per-op typed-error switch tables (the same ground-truth method the
2026-07-12 pass established), rather than trusting the refactor was behavior-preserving.
It found four real, in-scope bugs the split either introduced or left unaudited:

1. **`DescribeLoadBalancers`' `LoadBalancerDescription.Policies` field was entirely
   missing from `xmlLoadBalancerDescription`.** Every real client's `DescribeLoadBalancers`
   call saw an always-empty `Policies` (`AppCookieStickinessPolicies`/
   `LBCookieStickinessPolicies`/`OtherPolicies` all nil), regardless of how many
   policies actually existed on the LB. This doesn't fail deserialization (the
   query/xml decoder tolerates missing optional elements, unlike the `<XxxResult>`
   wrapper bug class from the prior pass), so it would never surface as a client error
   — only as silently-wrong data. Fixed by adding `toXMLPolicies` (routes each policy by
   `PolicyTypeName` into the three sub-lists, mirroring `types.Policies`) and threading
   each LB's policies (fetched via the existing `DescribeLoadBalancerPolicies` backend
   method) through to `toXMLLoadBalancer`. Proven by
   `Test_SDKRoundTrip_LoadBalancerPolicies_WireShape`, which creates one policy of each
   kind and asserts the real SDK client's typed `Policies` struct via
   `DescribeLoadBalancers`.

2. **Two ops used the generic `ValidationError` code where the real op's typed-error
   switch requires `InvalidConfigurationRequest`**: `DeleteLoadBalancerPolicy`'s
   policy-still-in-use rejection, and `CreateLoadBalancerListeners`' classic-listeners
   (100) limit-exceeded rejection. Per `deserializers.go`,
   `awsAwsquery_deserializeOpErrorDeleteLoadBalancerPolicy` and
   `awsAwsquery_deserializeOpErrorCreateLoadBalancerListeners` only recognize
   `InvalidConfigurationRequest`/`LoadBalancerNotFound` (plus `CertificateNotFound`/
   `DuplicateListener`/`UnsupportedProtocol` for the latter) — `ValidationError` isn't
   in either switch, so on a real client `errors.As` against the typed exception would
   silently fail to match even though the HTTP status and generic error string looked
   right. Both now use the existing `ErrInvalidConfiguration` sentinel. The dead
   `ErrValidation` sentinel (a byte-for-byte duplicate of `ErrInvalidParameter`'s
   `"ValidationError"` code, used only by these two call sites) was deleted along with
   them. Proven by `Test_SDKRoundTrip_DeleteLoadBalancerPolicyInUse_IsTyped` (typed
   `InvalidConfigurationRequestException` via `errors.As`) and
   `TestAccountLimitMaxListeners` (asserts the `InvalidConfigurationRequest` code
   string in the response body).

3. **`RegisterInstancesWithLoadBalancer` hard-rejected registration past 1000
   instances with an invented error.** `DescribeAccountLimits` correctly advertises
   `classic-registered-instances: 1000` as an account limit (that part is real AWS
   behavior), but the real `RegisterInstancesWithLoadBalancer` op has no typed
   exception for exceeding it — `awsAwsquery_deserializeOpErrorRegisterInstancesWithLoadBalancer`
   only recognizes `InvalidInstance`/`LoadBalancerNotFound`. A real AWS account can
   register past the soft limit (it's advisory, enforced by different means, if at
   all, not by this API rejecting the call); gopherstack's hard 1000-instance cap
   would incorrectly fail requests a real client would have had succeed. Deleted per
   the "delete invented errors not in the real SDK" rule — no replacement behavior was
   substituted since none is documented.

4. **Inline `SSLCertificateId` on `CreateLoadBalancer`/`CreateLoadBalancerListeners`
   skipped the ARN-format check** (`validateCertificateID`, the same regex-based
   `arn:aws:(acm|iam):` check `SetLoadBalancerListenerSSLCertificate` already ran).
   Both code paths share `parseOneListener`, but only the required-non-empty check ran
   there — the format check was applied only when the cert was set *after* creation,
   letting a malformed cert ARN through at LB/listener creation time while rejecting
   the identical string via `SetLoadBalancerListenerSSLCertificate`. Now both paths
   validate identically. This is a same-service, in-scope consistency fix, distinct
   from the pre-existing cross-service `CertificateNotFound` gap noted above (which
   remains a gap: neither path can verify the cert ARN actually *exists* in ACM/IAM,
   only that it's shaped like one).

All four are covered by new/extended tests: `Test_SDKRoundTrip_LoadBalancerPolicies_WireShape`,
`Test_SDKRoundTrip_DeleteLoadBalancerPolicyInUse_IsTyped`, the `malformed_cert_arn_rejected`
case in `TestDuplicateListenerCreateListeners`, and
`TestCreateLoadBalancerRejectsMalformedInlineCertARN`.
