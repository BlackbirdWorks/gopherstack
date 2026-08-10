---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: servicediscovery
sdk_module: aws-sdk-go-v2/service/servicediscovery@v1.43.4   # version audited against
last_audit_commit: 6bf60b6f                       # HEAD when the PRIOR pass wrote this manifest; this pass's commit was not yet cut when the manifest was updated (see re-audit protocol)
last_audit_date: 2026-07-23
overall: A            # real bugs found and fixed this pass
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateHttpNamespace: {wire: ok, errors: ok, state: ok, persist: ok}
  CreatePrivateDnsNamespace: {wire: ok, errors: ok, state: ok, persist: ok}
  CreatePublicDnsNamespace: {wire: ok, errors: ok, state: ok, persist: ok}
  GetNamespace: {wire: fixed, errors: ok, state: ok, persist: ok, note: "response included a Tags field; real types.Namespace has none (tags only via ListTagsForResource) -- fixed, see Notes"}
  ListNamespaces: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "Tags field removed (see GetNamespace); Filters now implement TYPE/NAME/HTTP_NAME/RESOURCE_OWNER with EQ/BEGINS_WITH -- fixed, see Notes"}
  DeleteNamespace: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateHttpNamespace: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdatePrivateDnsNamespace: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdatePublicDnsNamespace: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateService: {wire: fixed, errors: fixed, state: fixed, persist: ok, note: "Tags field removed from response; ServiceAlreadyExists now enforced (case-insensitive within DNS namespaces, case-sensitive within HTTP namespaces) -- fixed, see Notes"}
  GetService: {wire: fixed, errors: ok, state: ok, persist: ok, note: "Tags field removed (see CreateService) -- fixed"}
  ListServices: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "Filters now implement NAMESPACE_ID/RESOURCE_OWNER -- fixed, see Notes"}
  DeleteService: {wire: ok, errors: ok, state: ok, persist: ok, note: "was silently auto-deregistering instances instead of failing ResourceInUse -- fixed prior pass"}
  UpdateService: {wire: ok, errors: ok, state: ok, persist: ok}
  GetServiceAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateServiceAttributes: {wire: ok, errors: ok, state: ok, persist: ok, note: "no ServiceAttributesLimitExceededException enforcement (see gaps)"}
  DeleteServiceAttributes: {wire: ok, errors: ok, state: ok, persist: ok}
  RegisterInstance: {wire: ok, errors: ok, state: fixed, persist: ok, note: "custom-attribute quota (30 count/255 key/1024 value/5000 total, documented) and AWS_INIT_HEALTH_STATUS seeding were unenforced/unimplemented -- fixed, see Notes"}
  DeregisterInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  GetInstance: {wire: ok, errors: ok, state: ok, persist: ok}
  ListInstances: {wire: ok, errors: ok, state: ok, persist: ok}
  DiscoverInstances: {wire: ok, errors: ok, state: fixed, persist: n/a, note: "HEALTHY_OR_ELSE_ALL fixed prior pass; OptionalParameters was parsed but never applied -- fixed this pass, see Notes"}
  DiscoverInstancesRevision: {wire: ok, errors: ok, state: ok, persist: n/a}
  GetInstancesHealthStatus: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateInstanceCustomHealthStatus: {wire: ok, errors: ok, state: ok, persist: ok}
  GetOperation: {wire: ok, errors: ok, state: ok, persist: ok}
  ListOperations: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "response used the full Operation shape (Type/CreateDate/UpdateDate/Targets/ErrorCode/ErrorMessage); real ListOperationsOutput.Operations is []types.OperationSummary{Id,Status} only -- fixed. Filters now implement NAMESPACE_ID/SERVICE_ID/STATUS/TYPE/UPDATE_DATE with EQ/IN/BETWEEN -- fixed, see Notes"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: n/a}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  route_matcher: {status: ok, note: "X-Amz-Target prefix Route53AutoNaming_v20170314. verified byte-for-byte against serializers.go"}
  timestamps: {status: ok, note: "CreateDate/UpdateDate use pkgs/awstime.Epoch() (float64, sub-second precision), matching AWS's fractional Unix-timestamp wire format; fixed prior pass"}
  tags_wire_shape: {status: fixed, note: "GetNamespace/ListNamespaces/CreateService/GetService/ListServices all included a Tags field; real types.Namespace, types.NamespaceSummary, types.Service, types.ServiceSummary have none -- tags are ONLY returned by ListTagsForResource. Fixed by removing Tags from namespaceToMap/serviceToMap; see Notes"}
  filters: {status: fixed, note: "ListNamespaces/ListServices/ListOperations Filters now honor Condition (EQ default, BEGINS_WITH, IN, BETWEEN for UPDATE_DATE) and every documented Name value including RESOURCE_OWNER (single-account model: SELF matches everything, OTHER_ACCOUNTS matches nothing) -- fixed, see Notes"}
  service_name_uniqueness: {status: fixed, note: "CreateService now enforces the documented same-namespace name-collision rule (case-insensitive for DNS namespaces, case-sensitive for HTTP namespaces) and returns ServiceAlreadyExists -- fixed, see Notes"}
  persistence: {status: ok, note: "Handler.Snapshot/Restore delegate to backend; backendSnapshot covers all 4 store.Table-backed resources plus the two raw maps (serviceAttributes, instanceHealthStatuses); versioned and tested (persistence_test.go)"}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "UpdateServiceAttributes has no attribute-count/size quota enforcement (real AWS: ServiceAttributesLimitExceededException); no documented exact limit found in the SDK comments to implement against with confidence (unlike RegisterInstance's instance-attribute quota, which IS documented and was fixed this pass)"
  - "GetInstancesHealthStatus/DiscoverInstances never surface HealthStatus=UNKNOWN; real Cloud Map instances backed by an AWS-managed HealthCheckConfig start UNKNOWN until the Route53 health check propagates. Gopherstack has no Route53 health-check subsystem to drive this, so all instances are HEALTHY until explicitly marked UNHEALTHY via UpdateInstanceCustomHealthStatus (which itself requires HealthCheckCustomConfig, correctly enforced)"
  - "DuplicateRequest ('operation is already in progress', returned by CreateHttpNamespace/CreatePrivateDnsNamespace/CreatePublicDnsNamespace/DeleteNamespace/RegisterInstance/DeregisterInstance per the vendored deserializers) has no genuine trigger path: every op completes synchronously under the backend's coarse write lock, so there is never an observable in-flight/PENDING window for a concurrent duplicate request to collide with. Sentinel intentionally not added (would be dead code with no real trigger, violating the no-stub-without-a-real-path principle)"
  - "ResourceLimitExceeded (CreateHttpNamespace/CreatePrivateDnsNamespace/CreatePublicDnsNamespace/CreateService/RegisterInstance) and RequestLimitExceeded (account-wide API throttling quota) are real SDK error types with no quota numbers documented anywhere in the vendored SDK source (only external doc links, e.g. cloud-map-limits.html) -- left unenforced rather than guessing at unverified thresholds"
deferred:                 # consciously not audited this pass (scope) — next pass targets
  - "Full cross-account/shared-namespace support (OwnerAccount request param, ARN-as-Id acceptance for namespace/service ID fields, real per-resource ResourceOwner tracking) -- not emulated; single-account model throughout. The RESOURCE_OWNER *filter* itself IS now handled this pass (coarse SELF-always-true/OTHER_ACCOUNTS-always-false semantics matching a single-account backend), but that's filtering only, not the underlying sharing model"
leaks: {status: clean, note: "no goroutines/janitors in this service; all state is plain maps/store.Table guarded by lockmetrics.RWMutex"}
---

## Notes

**Route matcher**: `X-Amz-Target: Route53AutoNaming_v20170314.<Op>` confirmed against
`aws-sdk-go-v2/service/servicediscovery@v1.39.24/serializers.go` (`SetHeader("X-Amz-Target").String("Route53AutoNaming_v20170314.<Op>")`
for every operation). No bug here.

**Bugs fixed this pass** (all real, verified against the vendored SDK source, not against
gopherstack's own output):

1. **GetNamespace/ListNamespaces/CreateService/GetService/ListServices returned a Tags field
   that doesn't exist on the real wire shape** (`handler_namespaces.go`'s `namespaceToMap`,
   `handler_services.go`'s `serviceToMap`). Real `types.Namespace` (returned by GetNamespace),
   `types.NamespaceSummary` (ListNamespaces), `types.Service` (CreateService/GetService), and
   `types.ServiceSummary` (ListServices) were all field-diffed against `types/types.go` --
   *none* of them declare a `Tags` field. Tags are only ever returned by
   `ListTagsForResource`. Because JSON unmarshaling silently drops unrecognized fields, this
   extra key was invisible to the real aws-sdk-go-v2 client and would only surface against a
   stricter/custom deserializer -- a real, if subtle, wire-shape divergence. Fixed by dropping
   `Tags` from both `*ToMap` functions; tests that asserted `Tags` presence in these responses
   were rewritten to assert its *absence* and to fetch tags via `ListTagsForResource` instead
   (`TestHandler_NamespaceTagsViaListTagsForResource`, `TestHandler_ServiceTagsViaListTagsForResource`,
   `TestMapToTagEntriesSorted`).

2. **ListOperations used the full `Operation` response shape instead of the real
   `OperationSummary` shape** (`handler_operations.go`). Real
   `ListOperationsOutput.Operations` is `[]types.OperationSummary`, which per
   `types/types.go` has exactly two fields: `Id` and `Status`. Gopherstack's `operationToMap`
   (built for `GetOperation`'s full `Operation` shape) was reused for `ListOperations` too,
   silently leaking `Type`/`CreateDate`/`UpdateDate`/`Targets`/`ErrorCode`/`ErrorMessage` into
   every list-operations response. Fixed by adding a dedicated `operationSummaryToMap`
   (Id+Status only) for `ListOperations`, leaving `GetOperation`'s full-shape `operationToMap`
   untouched. `TestListOperations_SummaryShape` locks the exact key set.

3. **CreateService never enforced the documented same-namespace name-collision rule** (was
   `deferred` in the prior audit pass; now resolved). `api_op_CreateService.go`'s doc comment
   is explicit: "For services that are accessible by DNS queries, you can't create multiple
   services with names that differ only by case (such as EXAMPLE and example) ... However, if
   you use a namespace that's only accessible by API calls [HTTP], then you can create
   services that with names that differ only by case." Added `checkServiceNameAvailable`
   (`services.go`): within a DNS namespace (`DNS_PRIVATE`/`DNS_PUBLIC`), a new service name is
   compared case-insensitively against existing services in the same namespace; within an
   HTTP namespace, case-sensitively. A collision returns the new `ErrServiceAlreadyExists`
   sentinel, wired to the real `ServiceAlreadyExists` error code. Services with no
   `NamespaceId` (`""`) are exempt -- there's no namespace to scope uniqueness to, matching
   `NamespaceId`'s optional status on `CreateServiceInput` (not in `validateOpCreateServiceInput`'s
   required-field list). The prior pass's "traps for the next auditor" note about
   `DiscoverInstances`' last-write-wins lookup is now stale (new creates can no longer produce
   namespace+name duplicates) but the defensive `svcMatches[len(svcMatches)-1]` lookup was left
   in place as a harmless safety net for any duplicates a pre-fix snapshot might still contain.

4. **DiscoverInstances silently ignored OptionalParameters** (`discovery.go`). The request
   struct already parsed `OptionalParameters` (`handler_discovery.go`'s
   `discoverInstancesRequest`) but never passed it to the backend. Real
   `DiscoverInstancesInput.OptionalParameters` doc comment: "Opportunistic filters to scope
   the results based on custom attributes. If there are instances that match both the filters
   specified in both the QueryParameters parameter and this parameter, all of these instances
   are returned. Otherwise, the filters are ignored, and only instances that match the filters
   that are specified in the QueryParameters parameter are returned." Added
   `narrowByOptionalParams`, applied after `QueryParameters` filtering and before health-status
   filtering, implementing exactly that fall-back semantic.

5. **RegisterInstance had no custom-attribute quota enforcement and ignored
   AWS_INIT_HEALTH_STATUS** (`handler.go`'s new `validateInstanceAttributes`, `instances.go`'s
   `RegisterInstance`). `RegisterInstanceInput.Attributes` doc comment: "You can add up to 30
   custom attributes. For each key-value pair, the maximum length of the attribute name is 255
   characters, and the maximum length of the attribute value is 1,024 characters. The total
   size of all provided attributes (sum of all keys and values) must not exceed 5,000
   characters" -- added as `InvalidInput` validation, mirroring the existing `validateTags`
   pattern. Separately: "AWS_INIT_HEALTH_STATUS If the service configuration includes
   HealthCheckCustomConfig, you can optionally use AWS_INIT_HEALTH_STATUS to specify the
   initial status of the custom health check, HEALTHY or UNHEALTHY. If you don't specify a
   value ..., the initial status is HEALTHY." Gopherstack already defaulted unset statuses to
   HEALTHY but never read this attribute at all, so `AWS_INIT_HEALTH_STATUS: UNHEALTHY` was
   silently ignored. `RegisterInstance` now seeds `instanceHealthStatuses` from this attribute
   when the target service has `HealthCheckCustomConfig`.

6. **ListNamespaces/ListServices/ListOperations Filters only implemented the single most
   common `Name` value per filter, always as implicit `EQ`** (was a `gap` in the prior audit
   pass; now resolved). Field-diffed every `Name`/`Condition` combination documented on
   `types.NamespaceFilter`, `types.ServiceFilter`, and `types.OperationFilter`. Added a shared
   `FilterValue` type (`models.go`, `Values []string` + `Condition string`) with a `matches`
   method supporting the real `EQ` (default)/`BEGINS_WITH`/`IN` operators, plus a
   `resourceOwnerMatches` helper for the `RESOURCE_OWNER` filter (this single-account backend
   treats every resource as always `SELF`-owned: `SELF` matches everything, `OTHER_ACCOUNTS`
   matches nothing -- no data is fabricated for a sharing model that doesn't exist here).
   `ListNamespaces` now honors `TYPE`/`NAME`/`HTTP_NAME`/`RESOURCE_OWNER`; `ListServices`
   honors `NAMESPACE_ID`/`RESOURCE_OWNER`; `ListOperations` honors
   `NAMESPACE_ID`/`SERVICE_ID`/`STATUS`/`TYPE`/`UPDATE_DATE`, the last via a new
   `parseEpochSeconds` helper decoding the `BETWEEN` start/end pair (Unix-seconds strings, per
   `OperationFilter`'s doc comment) into a `[start, end]` range matched against `UpdateDate`.

**Traps for the next auditor** (looks-wrong-but-correct):

- The Phase 3.3 `store.Table`/`store.Index` migration (`store_setup.go`) already makes
  service/instance lookups exact-key, not prefix-based, so the old class of "prefix
  collision" bugs that `TestCascadeDeleteUsesCorrectPrefix` was originally guarding against
  can no longer occur through that path; the test was kept for its DeleteService-ordering
  coverage.
- `DiscoverInstances`' `svcMatches[len(svcMatches)-1].ID` "last write wins" lookup is now
  unreachable via any *new* `CreateService` call (name collisions within a namespace now fail
  with `ServiceAlreadyExists`), but was deliberately left in place rather than simplified to
  `svcMatches[0].ID` -- it's a no-op safety net for restoring a persistence snapshot captured
  before this pass, and removing it buys nothing.
- `checkServiceNameAvailable`'s HTTP-namespace case-sensitive / DNS-namespace
  case-insensitive split is *intentional*, not a bug -- it's a direct, deliberate reading of
  `api_op_CreateService.go`'s doc comment distinguishing DNS-discoverable namespaces (where
  case-only name variants would produce ambiguous DNS records) from HTTP-only namespaces
  (which have no DNS record to collide over).
