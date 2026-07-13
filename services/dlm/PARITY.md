---
service: dlm
sdk_module: aws-sdk-go-v2/service/dlm@v1.37.2   # version audited against
last_audit_commit: cafbb3e2                     # HEAD when this manifest was written
last_audit_date: 2026-07-13
overall: A            # genuine fixes found this pass (see ops/notes below)
ops:
  CreateLifecyclePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "now rejects missing Description/ExecutionRoleArn with InvalidRequestException (both are required members of the real CreateLifecyclePolicyInput; previously silently accepted, producing policies no real account could ever create)"}
  GetLifecyclePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "full LifecyclePolicy shape; DateCreated/DateModified are ISO8601 strings on the wire (smithytime.ParseDateTime), not epoch-seconds -- Go's default time.Time JSON marshaling already matches, do not re-flag"}
  GetLifecyclePolicies: {wire: ok, errors: ok, state: ok, persist: ok, note: "fixed: policyIds query parsing used url.Values.Get (first value only) against a repeated-key wire format (policyIds=a&policyIds=b), silently dropping all but the first ID; added missing PolicyType field to the LifecyclePolicySummary wire shape; added resourceTypes/targetTags/tagsToAdd filters (previously accepted on the wire but silently ignored -- disguised no-op)"}
  UpdateLifecyclePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "partial update semantics correct (only non-empty/non-nil fields mutate); PATCH method confirmed in classifyPath"}
  DeleteLifecyclePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "tagKeys query already correctly read as a repeated key (manual SplitSeq loop), unlike the policyIds bug -- no fix needed"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  routing: {status: ok, note: "classifyPath/RouteMatcher correctly disambiguate GET /policies (list) vs GET /policies/{id} (get), and PATCH for UpdateLifecyclePolicy, per generated serializers.go opPath templates. Verified with a matcher-routed test (TestHandler_GetLifecyclePolicies_MatcherRouted) in addition to the pre-existing direct-matcher and direct-Handler() tests."}
gaps:
  - StatusMessage field (present on the real LifecyclePolicy shape) is not modeled -- always absent, since this backend never simulates a policy entering an error/degraded state. Adding an always-empty field would be a no-op with no observable behavior difference, so it was left out rather than added for its own sake.
  - DefaultPolicy / SIMPLIFIED-policy-language top-level Create/Update fields (CopyTags, CreateInterval, CrossRegionCopyTargets, Exclusions, ExtendDeletion, RetainInterval, DefaultPolicy, DefaultPolicyType) are not implemented; only the PolicyDetails-based STANDARD policy path is supported. GetLifecyclePolicy falls back to a minimal synthesized `{"PolicyType": "EBS_SNAPSHOT_MANAGEMENT"}` PolicyDetails when none was stored, which is a reasonable approximation for this out-of-scope area, not a data-corrupting stub.
  - LimitExceededException (the ~100-policies-per-account default AWS quota) is not enforced -- consistent with this codebase's general choice not to simulate account-level service quotas elsewhere.
  - UpdateLifecyclePolicy's Description/ExecutionRoleArn/State request fields are plain `string`, not `*string`, so a request that explicitly sets Description to "" is indistinguishable from omitting it (both no-op). Real AWS would technically accept the former as clearing the field. Low practical value (no SDK caller sends an explicit empty Description) and would require a larger request-shape rework; left as a known limitation rather than fixed.
deferred: []
leaks: {status: clean, note: "no goroutines/janitors; store.Table + lockmetrics.RWMutex only"}
---

## Notes

- Protocol: restjson1. Base paths: `POST /policies` (Create), `GET /policies`
  (list summaries), `GET /policies/{policyId}` (get one), `PATCH
  /policies/{policyId}` (Update), `DELETE /policies/{policyId}` (Delete),
  `/tags/{resourceArn}` (POST/DELETE/GET for Tag/Untag/ListTags).

- **Repeated-query-key wire format**: DLM's list-valued query filters
  (`policyIds`, `resourceTypes`, `targetTags`, `tagsToAdd`) are all serialized
  by the real SDK as the *same query key repeated once per value*
  (`encoder.AddQuery("policyIds")` in a loop), never comma-joined. A handler
  reading these via `url.Values.Get(key)` will silently see only the first
  value. Use `url.Values` map-index (`q["policyIds"]`) to get all of them.
  This was the core bug found this sweep (`handler.go`'s
  `handleGetLifecyclePolicies`), and it had NOT been caught by the pre-existing
  unit tests because none of them exercised more than one filter ID over HTTP.

- **targetTags / tagsToAdd wire format**: each entry is a `"key=value"`
  string (not JSON), matched against `PolicyDetails.TargetTags` ({Key,Value}
  pairs) and against every `PolicyDetails.Schedules[*].TagsToAdd` ({Key,Value}
  pairs) respectively. AWS semantics are ANY-of-list-of-filters against
  ANY-of-the-policy's-tags (i.e. a policy matches if at least one filter pair
  is present somewhere in the relevant tag list); confirmed against
  `types.LifecyclePolicySummary`/`types.PolicyDetails` in the vendored SDK
  (`aws-sdk-go-v2/service/dlm@v1.37.2/types/types.go`).

- **LifecyclePolicySummary vs LifecyclePolicy**: the summary shape used by
  GetLifecyclePolicies additionally carries `PolicyType` (missing before this
  sweep) but does NOT carry `PolicyArn`/`ExecutionRoleArn`/`PolicyDetails`/
  `StatusMessage`/`DateCreated`/`DateModified` -- those are exclusive to the
  full `LifecyclePolicy` shape returned by GetLifecyclePolicy. Do not merge
  the two response builders.

- **Timestamps are ISO8601, not epoch-seconds**: confirmed directly against
  `deserializers.go`'s `awsRestjson1_deserializeDocumentLifecyclePolicy`,
  which calls `smithytime.ParseDateTime` (RFC3339/RFC3339Nano) on
  `DateCreated`/`DateModified`. Go's default `time.Time` JSON marshaling
  (RFC3339Nano) is directly compatible, so `handler.go` emitting `time.Time`
  values as-is is already wire-correct -- do not "fix" this to epoch-seconds
  in a future pass.

- PolicyID format `policy-<16-hex-digit-counter>` (monotonic, not
  cryptographically random) is an intentional, accepted simplification
  consistent with other services in this codebase; AWS's own IDs are opaque
  and clients must not assume a specific format beyond the `policy-` prefix
  (which round-trips correctly).

- Required-field validation added in this sweep (`ExecutionRoleArn` and
  `Description` on CreateLifecyclePolicy) was verified against
  `validators.go`'s `validateOpCreateLifecyclePolicyInput`: those two plus
  `State` are `smithy.NewErrParamRequired` on the client side. `State` was
  deliberately left lenient (defaults to `ENABLED`) to preserve pre-existing,
  intentional behavior documented in this backend and exercised by existing
  tests.
