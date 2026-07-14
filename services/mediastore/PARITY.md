---
service: mediastore
sdk_module: aws-sdk-go-v2/service/mediastore@v1.29.23
last_audit_commit: dfd2cb83
last_audit_date: 2026-07-13
overall: B            # already-accurate; proven op-by-op, one real (message-text) bug fixed
ops:
  CreateContainer: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeContainer: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteContainer: {wire: ok, errors: ok, state: ok, persist: ok}
  ListContainers: {wire: ok, errors: ok, state: ok, persist: ok, note: "HMAC-signed opaque NextToken via pkgs/page; sorted by Name"}
  PutContainerPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  GetContainerPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteContainerPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "PolicyNotFoundException when unset -- confirmed against real AWS API reference (not idempotent-success)"}
  PutCorsPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  GetCorsPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteCorsPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  PutLifecyclePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  GetLifecyclePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteLifecyclePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  PutMetricPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  GetMetricPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteMetricPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  StartAccessLogging: {wire: ok, errors: ok, state: ok, persist: ok}
  StopAccessLogging: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  Container: {status: ok, note: "CreateContainer/DescribeContainer/DeleteContainer/ListContainers verified end-to-end against the real aws-sdk-go-v2 client over an httptest server (not just unit tests) -- wire shapes, epoch CreationTime, ARN/Endpoint format, error deserialization all round-trip cleanly."}
  ContainerPolicy: {status: ok, note: "Put/Get/Delete round-trip the raw policy JSON string verbatim; Delete returns PolicyNotFoundException when unset, matching AWS."}
  CorsPolicy: {status: ok, note: "Put validates AllowedOrigins/AllowedHeaders non-empty per rule; Get/Delete round-trip full rule set including AllowedMethods/ExposeHeaders/MaxAgeSeconds."}
  LifecyclePolicy: {status: ok, note: "Put/Get/Delete round-trip the raw JSON string verbatim."}
  MetricPolicy: {status: ok, note: "Put validates ContainerLevelMetrics enum and >5-rule limit; Get/Delete round-trip full policy including MetricPolicyRules."}
  Tags: {status: ok, note: "Tag/Untag/ListTagsForResource keyed by ARN via containerNameFromARN; tags also settable at CreateContainer time."}
gaps:
  - MetricPolicyRule.ObjectGroup/ObjectGroupName character-set and length limits (900/30 chars) not enforced server-side -- unreachable via a real SDK client (client-side validators.go only requires non-nil), so low value; only affects raw-HTTP callers that bypass the SDK. (bd: none filed, low priority)
  - CorsPolicy rule-count limit (AWS allows up to 100 rules) not enforced. Same reasoning: no observed client-visible impact. (bd: none filed, low priority)
  - Container lifecycle is instantaneous (CREATING/DELETING transient states are never observed -- CreateContainer returns ACTIVE immediately, DeleteContainer removes synchronously). This is strictly more convenient for callers/waiters than real AWS's async transition and never causes a hang or wrong terminal state, so left as-is; noted here so a future auditor doesn't mistake it for the "stuck in CREATING" bug class.
deferred: []
leaks: {status: clean, note: "No goroutines, timers, or janitors in this service; InMemoryBackend is a single lockmetrics.RWMutex over per-region store.Table maps."}
---

## Notes

- Protocol: awsjson1.1, single POST endpoint, `X-Amz-Target: MediaStore_20170901.<Op>` --
  confirmed byte-for-byte against `aws-sdk-go-v2/service/mediastore@v1.29.23`'s
  `serializers.go` header-setting calls for every operation.
- Verified end-to-end against the **real aws-sdk-go-v2 client** (not just hand-rolled unit
  tests) driving an `httptest.Server` wrapping `Handler.Handler()`: CreateContainer (incl.
  Tags at create time), DescribeContainer, paginated ListContainers (MaxResults=2 across 3
  pages with real NextToken round-trip), Put/Get for ContainerPolicy/CorsPolicy/
  LifecyclePolicy/MetricPolicy, StartAccessLogging/StopAccessLogging, TagResource/
  ListTagsForResource/UntagResource, and the full error-code surface (ValidationException on
  bad name, ContainerInUseException on duplicate create, ContainerNotFoundException via
  `errors.As` into `*types.ContainerNotFoundException`, PolicyNotFoundException on
  double-delete, ValidationException on >5 metric rules). All wire shapes decoded cleanly
  through the SDK's generated deserializers with no manual workarounds required.
- `Container.CreationTime` is epoch-seconds (`float64` via `.Unix()`), matching the SDK
  deserializer's `smithytime.ParseEpochSeconds`. Do not "fix" this to RFC3339 -- that would
  break the real client.
- Error code mapping (`Handler.writeBackendError`) is exhaustive and exact:
  `ContainerNotFoundException` (404), `PolicyNotFoundException` (404, covers
  container/lifecycle/metric policy not-found), `CorsPolicyNotFoundException` (404, CORS
  gets its own type -- do not conflate with `PolicyNotFoundException`),
  `ContainerInUseException` (409, container-already-exists), `ValidationException` (400).
  All confirmed present in `types/errors.go` of the real SDK.
- **Bug found and fixed this pass**: six validation sentinel errors
  (`ErrInvalidContainerName`, `ErrInvalidPolicy`, `ErrCorsRuleInvalid`,
  `ErrInvalidMetricPolicy`, `ErrTooManyMetricRules`, `ErrEmptyTagKey`) had `"ValidationException: "`
  hand-baked into the *message* text, duplicating the `__type` field that
  `writeBackendError`/`JSONErrorResponse` already sends separately. Over the wire this
  produced doubled text like `ValidationException: ValidationException: container name must
  be 1-255 characters...` (confirmed via a real-SDK probe -- `smithy.OperationError`'s
  `Error()` formats as `api error <Type>: <message>`, so the type name appeared twice).
  Fixed by stripping the redundant prefix from all six error strings in `backend.go`; no
  test asserted the old (buggy) exact text, so no call sites needed updating. This is a
  message-content-only fix -- `__type` and HTTP status were already correct, so no client
  behavior keyed off `__type` or status code changes.
- `copyContainer` does a **shallow** pointer-slice copy for `CorsPolicy` (comment explains
  why: rule pointers are only ever replaced wholesale by `PutCorsPolicy`, never mutated
  in-place, and `GetCorsPolicy` only ever hands callers a fresh `[]CorsRule` **value** copy,
  never the pointers) -- this looks like a copy-safety bug at first glance but is not; do
  not "fix" by deep-copying without re-checking that invariant still holds.
- Container names are unique **per region only** (see `store_setup.go`); `containers` is a
  `map[string]*store.Table[Container]` keyed by region, intentionally not registered on a
  `*store.Registry` since the region set is only known at runtime. `persistence.go` snapshots/
  restores each region's table directly.
- `paginationSecret` (HMAC key for `ListContainers` NextToken) is deliberately **not**
  persisted -- regenerated fresh per process start, matching AppConfig/AppConfigData. A
  NextToken issued before a restore will fail its HMAC check afterward; this is an accepted,
  pre-existing limitation shared with sibling services, not a new gap.
