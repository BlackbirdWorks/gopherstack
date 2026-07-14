---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: servicediscovery
sdk_module: aws-sdk-go-v2/service/servicediscovery@v1.39.24   # version audited against
last_audit_commit: 6bf60b6f                       # HEAD when this manifest was written
last_audit_date: 2026-07-13
overall: A            # real bugs found and fixed this pass
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateHttpNamespace: {wire: ok, errors: ok, state: ok, persist: ok}
  CreatePrivateDnsNamespace: {wire: ok, errors: ok, state: ok, persist: ok}
  CreatePublicDnsNamespace: {wire: ok, errors: ok, state: ok, persist: ok}
  GetNamespace: {wire: ok, errors: ok, state: ok, persist: ok}
  ListNamespaces: {wire: ok, errors: ok, state: ok, persist: ok, note: "Filters only implement TYPE/NAME; HTTP_NAME/RESOURCE_OWNER ignored (see gaps)"}
  DeleteNamespace: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateHttpNamespace: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdatePrivateDnsNamespace: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdatePublicDnsNamespace: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateService: {wire: ok, errors: ok, state: ok, persist: ok}
  GetService: {wire: ok, errors: ok, state: ok, persist: ok}
  ListServices: {wire: ok, errors: ok, state: ok, persist: ok, note: "Filters only implement NAMESPACE_ID; RESOURCE_OWNER ignored (see gaps)"}
  DeleteService: {wire: ok, errors: ok, state: fixed, persist: ok, note: "was silently auto-deregistering instances instead of failing ResourceInUse -- fixed, see Notes"}
  UpdateService: {wire: ok, errors: ok, state: ok, persist: ok}
  GetServiceAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateServiceAttributes: {wire: ok, errors: ok, state: ok, persist: ok, note: "no ServiceAttributesLimitExceededException enforcement (see gaps)"}
  DeleteServiceAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  RegisterInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  DeregisterInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  GetInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  ListInstances: {wire: ok, errors: ok, state: ok, persist: ok}
  DiscoverInstances: {wire: ok, errors: ok, state: fixed, persist: n/a, note: "HEALTHY_OR_ELSE_ALL fail-open filter was unimplemented (always 0 results) -- fixed, see Notes"}
  DiscoverInstancesRevision: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetInstancesHealthStatus: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateInstanceCustomHealthStatus: {wire: ok, errors: fixed, state: fixed, persist: ok, note: "CustomHealthNotFound sentinel existed but was never returned -- fixed, see Notes"}
  GetOperation: {wire: ok, errors: ok, state: ok, persist: ok}
  ListOperations: {wire: ok, errors: ok, state: ok, persist: ok, note: "Filters only implement STATUS/TYPE; NAMESPACE_ID/SERVICE_ID/UPDATE_DATE ignored (see gaps)"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: n/a}
  TagResource: {wire: ok, errors: fixed, state: ok, persist: ok, note: "exceeding 50 tags returned InvalidInput instead of TooManyTagsException -- fixed"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  route_matcher: {status: ok, note: "X-Amz-Target prefix Route53AutoNaming_v20170314. verified byte-for-byte against serializers.go"}
  timestamps: {status: fixed, note: "CreateDate/UpdateDate switched from time.Unix() (int64, whole seconds) to pkgs/awstime.Epoch() (float64, sub-second precision) to match AWS's fractional Unix-timestamp wire format"}
  persistence: {status: ok, note: "Handler.Snapshot/Restore delegate to backend; backendSnapshot covers all 4 store.Table-backed resources plus the two raw maps (serviceAttributes, instanceHealthStatuses); versioned and tested (persistence_test.go)"}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "ListNamespaces/ListServices/ListOperations Filters only implement the most common Name values (TYPE/NAME, NAMESPACE_ID, STATUS/TYPE respectively); HTTP_NAME, RESOURCE_OWNER (cross-account sharing, not emulated at all), NAMESPACE_ID/SERVICE_ID/UPDATE_DATE on ListOperations, and Condition (EQ/IN/BETWEEN/BEGINS_WITH, always treated as EQ-on-first-value) are unimplemented"
  - "UpdateServiceAttributes has no attribute-count/size quota enforcement (real AWS: ServiceAttributesLimitExceededException); no documented exact limit found in the SDK comments to implement against with confidence"
  - "GetInstancesHealthStatus/DiscoverInstances never surface HealthStatus=UNKNOWN; real Cloud Map instances backed by an AWS-managed HealthCheckConfig start UNKNOWN until the Route53 health check propagates. Gopherstack has no Route53 health-check subsystem to drive this, so all instances are HEALTHY until explicitly marked UNHEALTHY via UpdateInstanceCustomHealthStatus (which itself requires HealthCheckCustomConfig, now correctly enforced)"
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - "CreateService (namespaceID, name) uniqueness / ServiceAlreadyExists: current backend allows duplicate (namespaceID, name) pairs with documented last-write-wins semantics for DiscoverInstances/DiscoverInstancesRevision lookup (see serviceNsNameKeyFn / DiscoverInstances comments in backend.go). Real AWS types/errors.go defines ServiceAlreadyExists but its exact trigger condition (name collision vs CreatorRequestId retry) isn't unambiguous from SDK doc comments alone; left as-is to avoid an unverified behavior change on top of an already load-bearing design decision from a prior audit pass"
  - "Cross-account/shared-namespace support (ResourceOwner, OwnerAccount, ARN-as-Id acceptance for shared services) -- not emulated anywhere in this backend; single-account model throughout"
leaks: {status: clean, note: "no goroutines/janitors in this service; all state is plain maps/store.Table guarded by lockmetrics.RWMutex"}
---

## Notes

**Route matcher**: `X-Amz-Target: Route53AutoNaming_v20170314.<Op>` confirmed against
`aws-sdk-go-v2/service/servicediscovery@v1.39.24/serializers.go` (`SetHeader("X-Amz-Target").String("Route53AutoNaming_v20170314.<Op>")`
for every operation). No bug here.

**Bugs fixed this pass** (all real, verified against the vendored SDK source, not against
gopherstack's own output):

1. **DeleteService silently cascaded instead of failing** (`handler.go`,
   `handleDeleteService`). Real Cloud Map: "Deletes a specified service and all associated
   service attributes. If the service still contains one or more registered instances, the
   request fails." (`api_op_DeleteService.go` doc comment). The backend's `DeleteService`
   already implemented this correctly (`ErrResourceInUse`, tested directly in
   `persistence_test.go`'s `TestServiceDiscovery_DeleteOrder`), but the HTTP handler bypassed
   it entirely: it pre-emptively called `ListInstances` + `DeregisterInstance` for every
   instance before calling `Backend.DeleteService`, so `DeleteService` could never actually
   fail via the API. This masks a real integration bug class: SDK/Terraform/CDK callers rely
   on `ResourceInUse` to know they must deregister first. Fixed by having the handler call
   `Backend.DeleteService` directly. `TestRefinement1_CascadeDeleteUsesCorrectPrefix` (a prior
   regression test for a since-obsolete prefix-matching bug in the old cascade code) was
   rewritten to assert the correct fail-then-retry flow while preserving its original
   ID-prefix-safety check.

2. **DiscoverInstances HEALTHY_OR_ELSE_ALL always returned zero results** (`backend.go`,
   was `instanceMatchesHealth`). The real `HealthStatusFilter` enum has 4 values including
   `HEALTHY_OR_ELSE_ALL`: "Returns healthy instances, unless none are reporting a healthy
   state. In that case, return all instances. This is also called failing open."
   (`types/enums.go` doc comment via `api_op_DiscoverInstances.go`). The old per-instance
   `instanceMatchesHealth` helper did a strict string-equality match against the filter
   value, so `HEALTHY_OR_ELSE_ALL` never matched any stored status (`"HEALTHY"` or
   `"UNHEALTHY"`) and DiscoverInstances silently returned an empty list for that filter,
   every time. Rewrote as `filterInstancesByHealth`, which computes the healthy subset first
   and falls back to the full candidate set only when that subset is empty.

3. **UpdateInstanceCustomHealthStatus never returned CustomHealthNotFound**
   (`backend.go`). Real Cloud Map: "You can use UpdateInstanceCustomHealthStatus to change
   the status only for custom health checks, which you define using HealthCheckCustomConfig
   when you create a service." (`api_op_UpdateInstanceCustomHealthStatus.go`), and
   `types/errors.go` defines `CustomHealthNotFound` for exactly this. Gopherstack already had
   the `ErrCustomHealthNotFound` sentinel wired into `handleError` -- but nothing in the
   backend ever returned it, so it was dead code and the op silently accepted status updates
   for services with no custom health check at all (`svc.HealthCheckCustomConfig == nil`).
   Added the check. Existing tests that exercised this op against services created without
   `HealthCheckCustomConfig` (`handler_newops_test.go`) were updated to add it, matching what
   real AWS requires for this op to succeed.

4. **Tag-count overflow returned the wrong error type** (`handler.go`, `validateTags`).
   Real Cloud Map has a dedicated `TooManyTagsException`: "The list of tags on the resource
   is over the quota. The maximum number of tags that can be applied to a resource is 50."
   (`types/errors.go`). Gopherstack's `maxTagCount` was already correctly set to 50, but the
   over-limit path returned `ErrInvalidInput` instead. Added `ErrTooManyTags` sentinel and
   wired it into `handleError`; per-tag key/value length and reserved-prefix violations
   correctly remain `InvalidInput` (matches the real "value might exceed length constraints"
   wording for that exception).

5. **CreateDate/UpdateDate truncated to whole seconds** (`handler.go`, `namespaceToMap` /
   `serviceToMap` / `operationToMap`). Real AWS: "The value of CreateDate is accurate to
   milliseconds. For example, the value 1516925490.087 ..." (repeated verbatim across
   `types.Namespace`, `types.Service`, `types.Operation` doc comments) -- i.e. a JSON number
   with a fractional-seconds component, not a whole-second integer. Switched from
   `t.Unix()` (int64) to `pkgs/awstime.Epoch(t)` (float64, sub-second precision), per the
   pkgs catalog's standing guidance to reuse `awstime.Epoch` for this wire format instead of
   hand-rolling it. No prior test asserted an exact `CreateDate` value (only `NotZero`), so
   this was a safe behavioral tightening.

**Traps for the next auditor** (looks-wrong-but-correct):

- `DiscoverInstances`'s `svcMatches[len(svcMatches)-1].ID` "last write wins" lookup and the
  `serviceNsNameKeyFn` comment about services never enforcing `(namespaceID, name)`
  uniqueness are *intentional*, documented in `backend.go`. Don't "fix" this without first
  resolving the `ServiceAlreadyExists` deferred item above -- changing uniqueness semantics
  changes DiscoverInstances' documented last-write-wins contract too.
- The Phase 3.3 `store.Table`/`store.Index` migration (`store_setup.go`) already makes
  service/instance lookups exact-key, not prefix-based, so the old class of "prefix
  collision" bugs that `TestRefinement1_CascadeDeleteUsesCorrectPrefix` was originally
  guarding against can no longer occur through that path; the test was kept (and updated)
  mainly for its DeleteService-ordering coverage now.
