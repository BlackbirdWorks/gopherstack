---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: elb
sdk_module: aws-sdk-go-v2/service/elasticloadbalancing@v1.33.21   # version audited against
last_audit_commit: 49e505cb                       # HEAD when this audit began (working tree, pre-commit)
last_audit_date: 2026-07-12
overall: A            # A = genuine fixes found; B = already-accurate, proven op-by-op
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateLoadBalancer: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed InvalidScheme/UnsupportedProtocol/TooManyLoadBalancers/DuplicateTagKeys/TooManyTags error codes (were generic ValidationError)"}
  DeleteLoadBalancer: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed missing DeleteLoadBalancerResult wrapper (real SDK GetElement failed)"}
  DescribeLoadBalancers: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateLoadBalancerListeners: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed UnsupportedProtocol error code via shared parseOneListener"}
  DeleteLoadBalancerListeners: {wire: ok, errors: ok, state: ok, persist: ok}
  RegisterInstancesWithLoadBalancer: {wire: ok, errors: ok, state: ok, persist: ok}
  DeregisterInstancesFromLoadBalancer: {wire: ok, errors: ok, state: ok, persist: ok}
  ConfigureHealthCheck: {wire: ok, errors: ok, state: ok, persist: ok}
  ModifyLoadBalancerAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeLoadBalancerAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  AddTags: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed TooManyTags error code (was ValidationError); added missing DuplicateTagKeys same-request validation"}
  DescribeTags: {wire: ok, errors: ok, state: ok, persist: ok}
  RemoveTags: {wire: ok, errors: ok, state: ok, persist: ok}
  ApplySecurityGroupsToLoadBalancer: {wire: ok, errors: partial, state: ok, persist: ok, note: "no InvalidSecurityGroup check (would require cross-service EC2 SG lookup); accepts any string"}
  AttachLoadBalancerToSubnets: {wire: ok, errors: partial, state: ok, persist: ok, note: "no InvalidSubnet/SubnetNotFound check (would require cross-service EC2 subnet lookup); accepts any string"}
  DetachLoadBalancerFromSubnets: {wire: ok, errors: ok, state: ok, persist: ok}
  EnableAvailabilityZonesForLoadBalancer: {wire: ok, errors: ok, state: ok, persist: ok}
  DisableAvailabilityZonesForLoadBalancer: {wire: ok, errors: ok, state: ok, persist: ok}
  SetLoadBalancerListenerSSLCertificate: {wire: ok, errors: partial, state: ok, persist: ok, note: "fixed missing Result wrapper; CertificateNotFound not modeled (format-only regex check, no cross-service ACM/IAM lookup)"}
  SetLoadBalancerPoliciesOfListener: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed missing Result wrapper"}
  SetLoadBalancerPoliciesForBackendServer: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed missing Result wrapper"}
  CreateAppCookieStickinessPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed missing Result wrapper"}
  CreateLBCookieStickinessPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed missing Result wrapper"}
  CreateLoadBalancerPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed missing Result wrapper; fixed PolicyTypeNotFound error code (was generic ValidationError); added missing PublicKeyPolicyType to allowlist; TooManyPolicies not enforced (gap, see below)"}
  DeleteLoadBalancerPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed missing Result wrapper"}
  DescribeAccountLimits: {wire: ok, errors: ok, state: ok, persist: n/a-static}
  DescribeInstanceHealth: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeLoadBalancerPolicies: {wire: ok, errors: ok, state: ok, persist: ok, note: "no-LoadBalancerName sample-policy fallback verified correct vs AWS docs, not a bug"}
  DescribeLoadBalancerPolicyTypes: {wire: ok, errors: ok, state: ok, persist: n/a-static, note: "fixed wrong PolicyTypeNotFound error code (was reusing PolicyNotFound, the policy-instance sentinel)"}
# Families audited as a group (when per-op is impractical):
families:
  snapshot_restore: {status: ok, note: "Handler-level Snapshot/Restore delegation (persistence.go) verified intact; backend.Snapshot/Restore round-trip all LB + policy state incl. tags; version-guarded (v4) against incompatible older snapshots"}
  route_matcher: {status: ok, note: "single query/xml POST matcher (Version=2012-06-01 form field) confirmed reachable for all 29 dispatch-table ops; TestSDKCompleteness passes with empty notImplemented list"}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - ApplySecurityGroupsToLoadBalancer / AttachLoadBalancerToSubnets accept any security-group/subnet ID string without validating existence against EC2 state (real AWS returns InvalidSecurityGroup/InvalidSubnet/SubnetNotFound) -- cross-service, out of scope for this pass, needs an EC2 backend lookup hook
  - SetLoadBalancerListenerSSLCertificate / CreateLoadBalancer HTTPS listeners validate SSLCertificateId only by ARN-prefix regex, not cross-service ACM/IAM existence (real AWS returns CertificateNotFound) -- cross-service, out of scope
  - CreateLoadBalancerPolicy has no TooManyPolicies limit (AWS models TooManyPoliciesException for this op per the SDK's op-specific error switch, but no default per-LB policy count limit is documented anywhere gopherstack could source a correct number from; fabricating one risked being wrong, so left unenforced rather than guessed)
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - none — full op-by-op pass completed this round
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
