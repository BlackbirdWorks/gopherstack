---
service: dlm
sdk_module: aws-sdk-go-v2/service/dlm@v1.37.2   # version audited against
last_audit_commit: pending (agent instructed not to run git; set at commit time)
last_audit_date: 2026-07-24
overall: A            # full re-sweep; 3 genuine bugs found and fixed (see ops/notes below)
ops:
  CreateLifecyclePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "rejects missing Description/ExecutionRoleArn with InvalidRequestException (both are required members of the real CreateLifecyclePolicyInput). PolicyDetails is stored as an opaque map[string]any and round-tripped verbatim, so every nested member documented in the real types.PolicyDetails (Actions, CopyTags, CreateInterval, CrossRegionCopyTargets, EventSource, Exclusions, ExtendDeletion, Parameters, PolicyLanguage, PolicyType, ResourceLocations, ResourceType, ResourceTypes, RetainInterval, Schedules[].{CreateRule,RetainRule,FastRestoreRule,ArchiveRule,CrossRegionCopyRules,DeprecateRule,ShareRules,TagsToAdd,VariableTags}, TargetTags) survives Create->Get unmodified; ResourceTypes/TargetTags/Schedules[].TagsToAdd are additionally decoded for GetLifecyclePolicies filtering (see models.go)"}
  GetLifecyclePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "full LifecyclePolicy shape; DateCreated/DateModified are ISO8601 strings on the wire (smithytime.ParseDateTime), not epoch-seconds -- Go's default time.Time JSON marshaling already matches, do not re-flag"}
  GetLifecyclePolicies: {wire: ok, errors: ok, state: ok, persist: ok, note: "policyIds/resourceTypes/targetTags/tagsToAdd all correctly read as repeated query keys via q[key]; PolicyType present on every summary"}
  UpdateLifecyclePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this sweep: Description/ExecutionRoleArn are now *string end-to-end (StorageBackend interface, handler request struct, backend), so an explicit empty string (\"Description\":\"\") clears the field while an omitted key leaves it unchanged -- matches the real UpdateLifecyclePolicyInput, whose serializer (awsRestjson1_serializeOpDocumentUpdateLifecyclePolicyInput) only omits a wire key when the *string pointer itself is nil. State intentionally stays a plain string: SettablePolicyStateValues is a non-pointer value type on the wire and the real serializer only emits it `if len(State) > 0`, so an explicit empty State is not constructible even by the real SDK -- no gap there."}
  DeleteLifecyclePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "FIXED this sweep: tagKeys query values were read via a manual strings.SplitSeq(rawQuery, \"&\") + CutPrefix loop that never percent-decoded the value, unlike the real SDK wire format (encoder.AddQuery(\"tagKeys\") -> url.Values.Encode(), standard percent/plus escaping per encode.go's Encoder.Encode). A tag key containing a space, '=', '&', or '%' would silently fail to match the stored (decoded) key and UntagResource would no-op. Switched to c.Request().URL.Query()[\"tagKeys\"], which decodes correctly."}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  routing: {status: ok, note: "FIXED this sweep: RouteMatcher's /tags/{arn} branch hardcoded an \"arn:aws:dlm:\" prefix check, but pkgs/arn.Build (used by this same backend's CreateLifecyclePolicy to mint PolicyArn) derives the ARN partition from the region (aws-us-gov, aws-cn, aws-iso, aws-iso-b via arn.PartitionForRegion) -- a backend constructed with a GovCloud/China/ISO region produced PolicyArn values the router would then refuse to accept on TagResource/UntagResource/ListTagsForResource, a self-inconsistency within the service (Create/Get worked, Tag* silently 404/unrouted). Replaced with isDLMResourceARN, a partition-agnostic arn:<partition>:dlm:... check. classifyPath/RouteMatcher otherwise correctly disambiguate GET /policies (list) vs GET /policies/{id} (get), and PATCH for UpdateLifecyclePolicy, per generated serializers.go opPath templates."}
gaps:
  - StatusMessage field (present on the real LifecyclePolicy shape) is not modeled -- always absent, since this backend never simulates a policy entering an error/degraded state. Adding an always-empty field would be a no-op with no observable behavior difference, so it was left out rather than added for its own sake.
  - DefaultPolicy / SIMPLIFIED-policy-language top-level Create/Update fields (CopyTags, CreateInterval, CrossRegionCopyTargets, Exclusions, ExtendDeletion, RetainInterval, DefaultPolicy, DefaultPolicyType) are not implemented; only the PolicyDetails-based STANDARD policy path is supported. GetLifecyclePolicy falls back to a minimal synthesized `{"PolicyType": "EBS_SNAPSHOT_MANAGEMENT"}` PolicyDetails when none was stored, which is a reasonable approximation for this out-of-scope area, not a data-corrupting stub.
  - LimitExceededException (the ~100-policies-per-account default AWS quota) is not enforced -- consistent with this codebase's general choice not to simulate account-level service quotas elsewhere.
deferred: []
leaks: {status: clean, note: "no goroutines/janitors; store.Table + lockmetrics.RWMutex only; TagResource/UntagResource/ListTagsForResource operate on the policy's own Tags map (no secondary tag-store row to leak on delete) -- DeleteLifecyclePolicy removes the whole storedPolicy row, tags included"}
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

- Required-field validation added in a prior sweep (`ExecutionRoleArn` and
  `Description` on CreateLifecyclePolicy) was verified against
  `validators.go`'s `validateOpCreateLifecyclePolicyInput`: those two plus
  `State` are `smithy.NewErrParamRequired` on the client side. `State` was
  deliberately left lenient (defaults to `ENABLED`) to preserve pre-existing,
  intentional behavior documented in this backend and exercised by existing
  tests.

- **UntagResource tagKeys must be percent-decoded, not read as a raw
  substring**: confirmed against `encode.go`'s `Encoder.Encode`
  (`req.URL.RawQuery = e.query.Encode()`) -- the real SDK's tagKeys query
  values are standard `url.Values`-encoded (percent/plus escaping), the same
  as every other query parameter DLM sends. The pre-sweep handler read them
  with a manual `strings.SplitSeq(rawQuery, "&")` + `CutPrefix("tagKeys=")`
  loop that never decoded the value -- functionally correct only for tag
  keys containing no characters requiring escaping. Fixed by switching to
  `c.Request().URL.Query()["tagKeys"]` (same repeated-key-safe accessor
  already used for `policyIds`/`resourceTypes`/etc., which also decodes).

- **RouteMatcher's tag-ARN check must be partition-agnostic**: `pkgs/arn.Build`
  (used by `CreateLifecyclePolicy` to mint `PolicyArn`) derives the ARN
  partition from the backend's region via `arn.PartitionForRegion` --
  `aws-us-gov`, `aws-cn`, `aws-iso`, `aws-iso-b`, not always `aws`. The
  pre-sweep `RouteMatcher` hardcoded a `strings.HasPrefix(arn, "arn:aws:dlm:")`
  check on the `/tags/{arn}` branch, so a GovCloud/China/ISO-region backend's
  own `PolicyArn` would fail to route into `TagResource`/`UntagResource`/
  `ListTagsForResource` -- Create/Get worked, tagging silently didn't. Fixed
  with `isDLMResourceARN`, which checks only that segment 2 (0-indexed) of
  the colon-split ARN is `"dlm"`, independent of the partition segment.

- **UpdateLifecyclePolicy Description/ExecutionRoleArn are `*string` end to
  end**: confirmed against `serializers.go`'s
  `awsRestjson1_serializeOpDocumentUpdateLifecyclePolicyInput`, which emits
  `"Description"`/`"ExecutionRoleArn"` on the wire `if v.Description != nil`
  / `if v.ExecutionRoleArn != nil` -- i.e. the real SDK type is a pointer
  specifically so an explicit `""` (clear the field) is representable and
  distinct from an omitted key (no change). `StorageBackend.UpdateLifecyclePolicy`,
  the handler's request struct, and `InMemoryBackend.UpdateLifecyclePolicy`
  all now take `*string` for these two fields. `State` was deliberately left
  as a plain `string`: `SettablePolicyStateValues` is a non-pointer value
  type in the same struct, and its serializer only emits it `if len(v.State)
  > 0` -- the real SDK itself cannot construct a request with an explicit
  empty `State`, so there is no wire state to distinguish.
