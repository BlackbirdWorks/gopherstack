---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: outposts
sdk_module: aws-sdk-go-v2/service/outposts@v1.66.1   # go.mod's actual pin at this audit (prior manifest said v1.66.0, stale)
last_audit_commit: 9c8570bbd
last_audit_date: 2026-08-06
# Grade held at B this pass (gopherstack-9ij1 + gopherstack-b9mg). What changed: closed the
# single highest-value gap the prior pass flagged -- services/ec2's RunInstances now really
# consumes this service's Outposts capacity ledger, and TerminateInstances really returns it.
# Added services/ec2's Subnet.OutpostArn (CreateSubnet input, cross-validated against a real
# Outpost) and Instance.OutpostArn (top-level, sibling of Placement -- confirmed via the pinned
# SDK's deserializers.go, NOT nested under Placement as the prior pass's filed issue assumed);
# added services/outposts/capacity_ledger.go's ConsumeCapacity/ReleaseCapacity, called by
# services/ec2's own new cross_service.go (ec2 -> outposts; the reverse of grafana's/mgn's
# direction, chosen because RunInstances must validate/consume synchronously as part of the EC2
# request, not as a background reconciliation read) at RunInstances/TerminateInstances time.
# GetOutpostInstanceTypes now genuinely depletes (a fully-consumed instance type drops out of the
# list, matching real AWS "currently configured" semantics under this pass's capacity-as-available
# model) and ListAssetInstances now returns real running-instance data (InstanceId/InstanceType/
# AssetId/AccountId/AwsServiceName=EC2) recorded by ConsumeCapacity -- not the outposts package
# reading services/ec2's Instance table (that would create an ec2<->outposts import cycle, since
# ec2 already imports outposts); outposts keeps its own minimal runningInstances ledger instead.
# CreateSubnet with a real OutpostArn is accepted; with an unknown one it's rejected
# (InvalidParameterValue, the generic EC2 code -- no dedicated typed exception exists, confirmed
# via aws-sdk-go-v2/service/ec2/types/errors.go); RunInstances exceeding configured capacity is
# rejected with the real, well-known InsufficientInstanceCapacity code. Proven end to end via a
# new test/integration/outposts_test.go case (TestIntegration_Outposts_EC2CapacityCoupling) driving
# the REAL EC2 client: create Outpost + capacity, create an Outpost subnet, RunInstances, observe
# GetOutpostInstanceTypes/ListAssetInstances reflect the drop, TerminateInstances, observe it return.
# NOT raised to A: two smaller gaps this pass's task did not touch remain open and are still
# genuinely buildable, not structural -- Order/CapacityTask's single-hop lifecycle (skips the real
# IN_PROGRESS/DELIVERED/WAITING_FOR_EVACUATION/CANCELLATION_IN_PROGRESS SDK states) and
# quotes.go's buildOrderingRequirements evaluating only 2 of 17 real OrderingRequirementType
# checks. Both were already flagged as "deferred, not unbuildable" by the prior pass and are
# unrelated to Outposts placement/capacity -- see gaps below.
overall: B
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
# All 43 ops are routed, backed by real state, and persisted via InMemoryBackend.Snapshot/Restore
# (persistence.go). "partial" below marks operations where a genuinely unknowable input (no SDK
# enum, no public AWS data) forced a documented, narrower-than-real-AWS behavior -- not a stub.
ops:
  CreateOutpost: {wire: ok, errors: ok, state: ok, persist: ok, note: "POST /outposts (outposts.go); seeds one COMPUTE Asset (assets.go); LifeCycleStatus set to ACTIVE immediately -- see structural_gaps; enforces the real 10-Outposts-per-site quota (ServiceQuotaExceededException) as of this pass"}
  GetOutpost: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /outposts/{OutpostId}, id-or-ARN via resolveOutpostLocked (resolve.go)"}
  DeleteOutpost: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETE /outposts/{OutpostId}; Conflict while a REQUESTED capacity task exists; cascades its seeded Asset(s)"}
  UpdateOutpost: {wire: ok, errors: ok, state: ok, persist: ok, note: "PATCH /outposts/{OutpostId}; merges Description/Name/SupportedHardwareType onto existing state"}
  ListOutposts: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /outposts; AvailabilityZoneFilter/AvailabilityZoneIdFilter/LifeCycleStatusFilter all as repeated PascalCase query params (confirmed via serializers.go, NOT lowerCamel like grafana -- see wire.go), paginated via pkgs/page"}
  StartOutpostDecommission: {wire: ok, errors: ok, state: partial, persist: ok, note: "POST /outposts/{OutpostIdentifier}/decommission; SKIPPED on idempotent replay, REQUESTED otherwise, BLOCKED never occurs (no cross-service blocking-resource data -- see gaps); ValidateOnly performs no mutation"}
  GetOutpostBillingInformation: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /outpost/{OutpostIdentifier}/billing-information (singular path, routed correctly -- see Grade note); accumulates ORIGINAL subscription on order completion (orders.go) and RENEWAL on CreateRenewal (renewals.go)"}
  GetOutpostInstanceTypes: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /outposts/{OutpostId}/instanceTypes; aggregates the CONFIGURED capacity across the Outpost's Assets (mutated by StartCapacityTask completion, and by capacity_ledger.go's ConsumeCapacity/ReleaseCapacity as services/ec2 launches/terminates instances onto it as of this pass -- a fully-depleted instance type drops out of the list), distinct from GetOutpostSupportedInstanceTypes"}
  GetOutpostSupportedInstanceTypes: {wire: ok, errors: ok, state: partial, persist: n/a, note: "GET /outposts/{OutpostIdentifier}/supportedInstanceTypes; returns the static seed catalog filtered by hardware type -- AssetId/OrderId are validated to exist but do not further filter the result (documented simplification, see gaps)"}
  GetRenewalPricing: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GET /outpost/{OutpostIdentifier}/renewal-pricing (singular path, routed correctly); PRICED for an ACTIVE Outpost, UNABLE_TO_PRICE otherwise"}
  CreateRenewal: {wire: ok, errors: ok, state: ok, persist: ok, note: "POST /renewals; ClientToken idempotency implemented (renewals.go's renewalIdempotency cache); pricing is a documented synthetic placeholder formula -- see gaps"}
  CreateSite: {wire: ok, errors: ok, state: ok, persist: ok, note: "POST /sites; OperatingAddress flattened to 3 fields on output, ShippingAddress fully stored but only surfaced via GetSiteAddress; enforces the real 100-sites-per-Region quota (ServiceQuotaExceededException) as of this pass"}
  GetSite: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /sites/{SiteId}, id-or-ARN"}
  UpdateSite: {wire: ok, errors: ok, state: ok, persist: ok, note: "PATCH /sites/{SiteId}; merges Description/Name/Notes"}
  DeleteSite: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETE /sites/{SiteId}; Conflict while any Outpost still references it"}
  ListSites: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /sites; city/countryCode/stateOrRegion filters against OperatingAddress"}
  GetSiteAddress: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /sites/{SiteId}/address; AddressType as query param, returns Shipping or Operating full Address"}
  UpdateSiteAddress: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT /sites/{SiteId}/address; full replacement (not merge); Conflict while the Site has a PREPARING order"}
  UpdateSiteRackPhysicalProperties: {wire: ok, errors: ok, state: ok, persist: ok, note: "PATCH /sites/{SiteId}/rackPhysicalProperties; merges only non-empty fields; same in-progress-order Conflict check"}
  CreateOrder: {wire: ok, errors: ok, state: partial, persist: ok, note: "POST /orders; OrderType always OUTPOST (CreateOrderInput has no OrderType member); single-hop PREPARING -> COMPLETED transition (no IN_PROGRESS/DELIVERED stop) -- see gaps; validates CatalogItemId and consumed Quote; QuoteIdentifier now resolves id-or-ARN, see GetQuote"}
  GetOrder: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /orders/{OrderId}, ID-only (no ARN form on this op)"}
  CancelOrder: {wire: ok, errors: ok, state: ok, persist: ok, note: "POST /orders/{OrderId}/cancel; Conflict once terminal"}
  ListOrders: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /list-orders; OutpostIdentifierFilter singular, paginated"}
  CreateQuote: {wire: ok, errors: ok, state: partial, persist: ok, note: "POST /quotes; single synthesized QuoteOption (not an N-option combinatorial shape); OrderingRequirements covers 2 of 17 real check types this backend has state to evaluate -- see gaps; pricing is a documented synthetic formula"}
  GetQuote: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /quotes/{QuoteIdentifier}; lazily flips CREATED -> EXPIRED past ExpirationDate; QuoteIdentifier now resolves id-or-ARN via resolveQuoteLocked (this pass fixed a real bug -- the prior audit's 'Quotes have no ARN form' note was wrong, GetQuoteInput's own Pattern confirms an ARN-shaped form)"}
  UpdateQuote: {wire: ok, errors: ok, state: ok, persist: ok, note: "PATCH /quotes/{QuoteIdentifier}; OutpostIdentifier tri-state (nil=no-change, empty=clear, value=set) implemented via *string wire field; never returns Conflict (none in this op's wire error set); QuoteIdentifier now resolves id-or-ARN, see GetQuote"}
  DeleteQuote: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETE /quotes/{QuoteIdentifier}; QuoteIdentifier now resolves id-or-ARN, see GetQuote"}
  ListQuotes: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /quotes; no filters, paginated; lazily expires each"}
  CancelCapacityTask: {wire: ok, errors: ok, state: partial, persist: ok, note: "POST /outposts/{OutpostIdentifier}/capacity/{CapacityTaskId}; transitions directly REQUESTED -> CANCELLED (skips the transient CANCELLATION_IN_PROGRESS state -- documented simplification, see gaps)"}
  GetCapacityTask: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET .../capacity/{CapacityTaskId}"}
  StartCapacityTask: {wire: ok, errors: ok, state: partial, persist: ok, note: "POST /outposts/{OutpostIdentifier}/capacity; enforces one-active-task-per-(Outpost,Order); single-hop REQUESTED -> COMPLETED mutates the target Asset's real capacity ledger; WAITING_FOR_EVACUATION never occurs (no cross-service blocking-instance data) -- see gaps; DryRun completes synchronously without mutating capacity"}
  ListCapacityTasks: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /capacity/tasks; status + OutpostIdentifierFilter"}
  ListBlockingInstancesForCapacityTask: {wire: ok, errors: ok, state: partial, persist: n/a, note: "GET .../blockingInstances; validates the capacity task exists, always returns empty. As of this pass real EC2-on-Outposts instance data DOES exist (capacity_ledger.go's runningInstances, see ListAssetInstances) but this op answers a narrower question -- instances blocking a capacity REDUCTION -- and StartCapacityTask's model is additive-only (mergeInstanceTypeCapacity only ever grows InstanceTypeCapacities, never shrinks), so no running instance can ever legitimately block a task in this backend; empty remains the honest answer, not a stub, see gaps"}
  ListAssets: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /outposts/{OutpostIdentifier}/assets; filters by AssetTypeFilter/HostIdFilter/StatusFilter against the seeded Asset(s)"}
  ListAssetInstances: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET .../assetInstances; returns real running-instance data as of this pass (InstanceId/InstanceType/AssetId/AccountId/AwsServiceName=EC2), recorded by capacity_ledger.go's ConsumeCapacity when services/ec2's RunInstances launches onto this Outpost -- AccountIdFilter/AssetIdFilter/AwsServiceFilter/InstanceTypeFilter all wired (repeated PascalCase query params, confirmed via serializers.go)"}
  GetCatalogItem: {wire: ok, errors: ok, state: partial, persist: n/a, note: "GET /catalog/item/{CatalogItemId}; served from seed_data.go's static 3-item catalog, not real AWS data -- see gaps"}
  ListCatalogItems: {wire: ok, errors: ok, state: partial, persist: n/a, note: "GET /catalog/items; ItemClass/EC2Family/SupportedStorage filters over the same static seed"}
  ListOrderableInstanceTypes: {wire: ok, errors: ok, state: partial, persist: n/a, note: "GET /instanceTypes; static 5-entry seed (seed_data.go), also backs GetOutpostInstanceTypes'/GetOutpostSupportedInstanceTypes' VCPU lookups"}
  GetConnection: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /connections/{ConnectionId}; synthetic (non-cryptographic) key/tunnel-address placeholders -- see gaps"}
  StartConnection: {wire: ok, errors: ok, state: ok, persist: ok, note: "POST /connections; validates AssetId exists"}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GET /tags/{ResourceArn}; resolves to Outpost or Site by ARN resource-segment marker"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "POST /tags/{ResourceArn}"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETE /tags/{ResourceArn}; TagKeys via repeated lowerCamel ?tagKeys= query param (the one exception to this service's PascalCase query casing, confirmed via serializers.go:3235)"}
# Families audited as a group (when per-op is impractical):
families:
  tagging: {status: ok, note: "TagResource/UntagResource/ListTagsForResource wired into cli.go's wireResourceGroupsTagging via wireTaggingOutposts, the 31st service. Both Outpost.Tags and Site.Tags share one ARN-keyed store (tagging.go's resolveTaggableLocked), resourceTypeFromARN derives outposts:outpost vs outposts:site per-ARN since this is a two-resource-kind tag store (unlike Grafana's single-kind constantResourceType)."}
  route-matcher: {status: ok, note: "handler.go's routeRequest uses a map-of-topLevelRouteFunc keyed by first path segment (kept cyclomatic complexity low without a nolint) rather than one large switch; RouteMatcher prefixes on all 12 top-level path segments; MatchPriority = PriorityPathVersioned"}
gaps:
  - "ListBlockingInstancesForCapacityTask always returns an empty result after validating the capacity task exists. Real EC2-on-Outposts instance data now exists (capacity_ledger.go's runningInstances, populated by services/ec2's RunInstances as of this pass -- see ListAssetInstances), but this op only has meaning for a capacity REDUCTION a running instance would block, and StartCapacityTask's model here is additive-only (mergeInstanceTypeCapacity never shrinks InstanceTypeCapacities). Buildable if the Order/CapacityTask lifecycle gap below is ever addressed with a real reduction path; empty is the honest answer today, not a stub."
  - "Order/CapacityTask lifecycle uses a single-hop async transition (PREPARING->COMPLETED, REQUESTED->COMPLETED) via pkgs/worker, mirroring services/grafana's scheduleWorkspaceTransition -- the intermediate states each type's SDK enum declares (Order: IN_PROGRESS/DELIVERED; CapacityTask: WAITING_FOR_EVACUATION/CANCELLATION_IN_PROGRESS) are real, wire-accurate constants this emulator never transitions through. Deferred this pass for scope/effort (unrelated to gopherstack-9ij1/gopherstack-b9mg's EC2-capacity-coupling task), not unbuildable: a defensible multi-hop timeline through the same real enum values could be modeled (no rollup *rule* is SDK-encoded, but transitioning through more of the real states is strictly more accurate than fewer, unlike inventing new data)."
  - "quotes.go's buildOrderingRequirements evaluates only 2 of the 17 real OrderingRequirementType checks (OUTPOST_ID_MISSING_ON_QUOTE_ERROR, OUTPOST_ACTIVE_CHECK_ERROR). Deferred, not unbuildable, and unrelated to this pass's task: at least one more (MAXIMUM_ALLOWED_ORDERS_CHECK_ERROR) is plausibly derivable from real order-count state without fabricating AWS data, but was not attempted this pass; the remaining ones (ENTERPRISE_SUPPORT_ERROR, VALID_ZIP_CODE_CHECK_ERROR, etc.) would require a support-plan/address-validation model this backend has no state for."
structural_gaps:
  - "LifeCycleStatus (types.Outpost.LifeCycleStatus is a bare *string) has NO SDK enum type anywhere in this module (confirmed by direct grep of types/enums.go -- zero LifeCycleStatus-named type exists) and the AWS API docs (API_Outpost.html) publish only a generic non-empty-string Pattern, no value set. Unlike the other gaps above, there is no more SDK/doc source to converge on even in principle: ACTIVE on CreateOutpost and PENDING_DECOMMISSION on StartOutpostDecommission success (consts.go) are this implementation's own defensible choice, and will remain so regardless of future effort unless AWS itself publishes an enum."
  - "ListCatalogItems/GetCatalogItem/ListOrderableInstanceTypes are served from a small static seed table (seed_data.go: 3 catalog items, 5 orderable instance types) standing in for AWS's own published, centrally-maintained hardware catalog and pricing model. This is proprietary AWS operational/billing data (which rack/server SKUs are currently orderable, real subscription pricing) with no public machine-readable source anywhere -- not in the SDK, not in Terraform, not in AWS's docs. No amount of implementation effort in this emulator can produce the real values; this is the exact 'no billing/settlement system' case structural_gaps exists for. pricing.go's deterministic formula is the same case: real Outposts subscription pricing is not published data."
  - "Connection/StartConnection key material (ServerPublicKey, tunnel addresses, UnderlayIpAddress -- connections.go) is synthetic and non-cryptographic. Real values require an actual WireGuard cryptographic handshake with real AWS infrastructure during physical Outpost server installation (per both ops' own doc comments) -- there is no data source an emulator could read or compute this from; it is not a knowledge gap, it is a physical-hardware-install-time cryptographic exchange, the same class of thing structural_gaps' 'no physical hardware' clause covers."
  - "ServiceQuotaExceededException has no trigger path on CreateOrder specifically (CreateSite/CreateOutpost now enforce the two real published quotas -- see ops table). No AWS-published default per-account Order quota exists to enforce without fabricating a number, matching services/grafana's identical treatment of AccessDeniedException."
leaks: {status: clean, note: "InMemoryBackend.Reset() closes every Outpost's and Site's tags.Tags before clearing (store.go); Close() stops the worker.Group backing every scheduled Order/CapacityTask transition timer (mirrors services/grafana's scheduleWorkspaceActivation pattern the prior audit called out as the thing to watch for)."}
---

## EC2 capacity-coupling pass (2026-08-06, gopherstack-9ij1 + gopherstack-b9mg)

Closed the single highest-value gap the prior pass identified and explicitly could not build
without an `ec2`-side change: `services/ec2`'s `RunInstances` now really consumes this service's
Outposts capacity ledger, and `TerminateInstances` really returns it. This session owned both
`services/ec2` and `services/outposts`, unblocking the fix.

**`services/ec2` additions** (verified against the pinned `aws-sdk-go-v2/service/ec2@v1.319.1`
checkout, not assumed from the filed issue's guess):
- `Subnet.OutpostArn` (`store.go`), settable via `CreateSubnetWithOutpost` (a new method;
  `CreateSubnet` now delegates to it with `outpostArn=""` so none of the ~30 existing call sites,
  including `services/cloudformation`, needed to change). `CreateSubnetInput.OutpostArn` is a
  flat, top-level `*string` field (confirmed via `serializers.go`'s
  `awsEc2query_serializeOpDocumentCreateSubnetInput`) -- no nesting.
- `Instance.OutpostArn` (`store.go`), populated from the launch subnet at `RunInstances` time.
  **Correction to the filed issue's assumption**: this is a top-level field on `types.Instance`,
  a *sibling* of `Placement`, not `Placement.OutpostArn` -- `types.Placement` (the struct used by
  both `RunInstancesInput.Placement` and `Instance.Placement`) has **no** `OutpostArn` member at
  all (confirmed by reading the full `Placement` struct in `types/types.go`); the response XML
  deserializer reads `outpostArn` and `placement` as two separate elements
  (`awsEc2query_deserializeDocumentInstance`). Surfaced on `RunInstances` and `DescribeInstances`
  responses (`instanceItem.OutpostArn`, XML tag `outpostArn`, sibling of the `placement` element).

**Cross-service wiring** (`services/ec2/cross_service.go`, new file): `ec2` imports
`services/outposts` directly and resolves its handler lazily via `SetAppConfig`/`GetOutpostsHandler`
-- both already exist on `*CLI` from the prior `outposts` pass, so **no `cli.go` edit was needed**.
This is the mirror image of `services/grafana`'s/`services/mgn`'s cross-service direction (they read
`ec2`'s state passively); here `ec2`'s own `RunInstances`/`TerminateInstances` must synchronously
call into `outposts` as part of handling the EC2 request itself (validate-and-consume, or fail the
whole launch), the same pattern `services/mgn`'s `launchParticipantInstanceLocked` already uses to
call `ec2Bk.RunInstances` while holding its own lock -- there is real in-repo precedent for a
cross-service call made while the caller's own backend lock is held, so `RunInstances`'s existing
lock scope did not need restructuring. `outposts` was deliberately **not** made to import `ec2` in
the other direction (that would create an `ec2` <-> `outposts` import cycle, since `ec2` already
imports `outposts`) -- `outposts` instead keeps its own minimal `runningInstances` ledger
(`capacity_ledger.go`), populated by the very cross-service calls `ec2` makes into it, rather than
reading `ec2`'s `Instance` table directly.

**`services/outposts/capacity_ledger.go`** (new file): `ConsumeCapacity(outpostArn, instanceType,
accountID, instanceIDs)` atomically checks-then-decrements `Asset.ComputeAttributes.
InstanceTypeCapacities[].Count` for the Outpost's single seeded Asset (there is still no public
`CreateAsset` op, so multi-asset draining logic was deliberately not built -- see the prior pass's
"Asset seeding" note, unchanged) and records one `runningInstance` row per instance ID; `Count`
represents currently-available capacity, decremented by `ConsumeCapacity`/incremented by
`ReleaseCapacity`, distinct from `StartCapacityTask`'s unrelated (and still additive-only, see
gaps) mutation of the same field. `ReleaseCapacity(instanceID)` looks up and deletes that row,
crediting the unit back. Both return/no-op honestly when the Outposts backend isn't wired (unit
tests constructing `ec2.InMemoryBackend` directly) or the referenced Outpost/Asset no longer
exists, matching `services/grafana`'s established graceful-degradation convention for optional
cross-service backends.

**Errors, verified against the real SDK, not invented**: `aws-sdk-go-v2/service/ec2/types/errors.go`
declares no typed exception for either failure mode (EC2's query-protocol error model predates
smithy's typed-exception generation for most codes). `CreateSubnetWithOutpost` rejecting an unknown
`OutpostArn` maps to the generic `InvalidParameterValue` code, matching this file's existing
treatment of every other no-dedicated-code cross-reference failure (e.g. `ErrCoipCidrNotFound`).
`RunInstances` exceeding available capacity maps to `InsufficientInstanceCapacity`, the real,
well-known EC2 client error for capacity shortfalls (used for AZ/Capacity-Reservation/Outpost
capacity failures alike per AWS's own error-code documentation) -- not fabricated for this pass.

**Proof**: `test/integration/outposts_test.go` gained two new SDK-driven cases --
`TestIntegration_Outposts_EC2CapacityCoupling` drives the full loop through the *real* `aws-sdk-go-v2`
EC2 client end to end (create Outpost, configure capacity via `StartCapacityTask`, `CreateSubnet`
with the real `OutpostArn`, `RunInstances`, observe `GetOutpostInstanceTypes`/`ListAssetInstances`
reflect the drop, a second launch rejected with `InsufficientInstanceCapacity`,
`TerminateInstances`, observe capacity and the asset-instance listing both reverse, and the freed
unit consumable again) and `..._NonexistentOutpostArn` proves the `CreateSubnet`-time rejection.
Both ran green against the Docker container (`make build-linux` + the real test harness), alongside
unit-level coverage in both packages (`services/ec2/cross_service_test.go`,
`services/outposts/capacity_ledger_test.go`) for the permutation/error cases (insufficient capacity,
exact-capacity, unconfigured instance type, unknown Outpost, unwired-backend no-ops, filter
matching) that don't need the full container.

**Not raised to A**: see the frontmatter's grade note -- the Order/CapacityTask single-hop
lifecycle and the 15-of-17 unevaluated `OrderingRequirement` checks are unrelated, pre-existing,
still-buildable gaps this pass's task did not touch.

## Integration-test and gap-closure pass (2026-08-06)

Added `test/integration/outposts_test.go` (10 test funcs, real `aws-sdk-go-v2` client against the
Docker container) -- the first SDK-driven parity proof this service has had; the prior B was proven
only by unit tests + an in-process SDK round-trip harness, which parity-principles.md rule 3
excludes as parity evidence. Coverage: Site/Outpost CRUD + decommission + the seeded Asset, the
real 10-Outposts-per-site quota, catalog items + filters, Quote/Order lifecycle including the new
id-or-ARN Quote resolution, CapacityTask lifecycle including the real capacity-ledger mutation,
Connection lifecycle, tagging across both taggable resource kinds (Outpost and Site -- the exact
surface the repo-wide `/tags/` routing fix targeted), NotFoundException across every resource kind,
and semantic ValidationException cases the SDK's own client-side required-field checks can't
intercept (confirmed via reading `validators.go`: it only checks field presence, never enum content
or string length, so "invalid enum value"/"wrong length" cases are genuine server-side proof, while
"missing required field" cases are not -- the SDK rejects those before the request is ever sent).

Fixed real bugs found by reading `docs.aws.amazon.com/outposts/latest/APIReference/` directly
(never assumed from existing code or field names) for every ID this service generates:
- Outpost/Site/Order/Quote/CapacityTask/LineItem/QuoteOption IDs were all 12 lowercase hex
  characters; the real pattern for every one of them is exactly 17 (e.g. `Outpost.OutpostArn`:
  `^arn:aws([a-z-]+)?:outposts:[a-z\d-]+:\d{12}:outpost/op-[a-f0-9]{17}$`).
- Two wrong prefixes: `CapacityTaskId` was `ct-`, the real prefix is `cap-`
  (`API_GetCapacityTask.html`: `^cap-[a-f0-9]{17}$`); `LineItemId` was `li-`, the real prefix is
  `ooi-` (`API_LineItem.html`: `ooi-[a-f0-9]{17}`); `QuoteOptionId` was `qo-`, the real prefix is
  `oqo-` (`API_Order.html`'s `QuoteOptionIdentifier`: `^oqo-[a-f0-9]{17}$`).
- AssetId and ConnectionId both used a `-` in their generated form; their real patterns
  (`^(\w+)$` and `^[a-zA-Z0-9+/=]{1,1024}$` respectively, from `API_StartConnection.html`) do not
  allow `-` at all -- fixed to drop it.
- CatalogItem seed IDs (`cat-rack-m5` etc.) didn't match the real `OR-[A-Z0-9]{7}` pattern
  (`API_CatalogItem.html`) at all -- replaced with `OR-RACKM05`/`OR-RACKC05`/`OR-SRVC6ID`.
- Found and fixed a real bug, not just a format mismatch: `GetQuoteInput`/`UpdateQuoteInput`/
  `DeleteQuoteInput`/`CreateOrderInput`'s `QuoteIdentifier` all accept an ARN-shaped form
  (`^(arn:...:quote/)?oq-[a-f0-9]{17}$}`, confirmed via `API_GetQuote.html`) -- the prior pass's
  "Quotes have no ARN form" conclusion was wrong. Added `resolveQuoteLocked` (mirrors
  `resolveOutpostLocked`/`resolveSiteLocked`) and wired it into all four operations.
- Implemented real `ServiceQuotaExceededException` enforcement on `CreateSite` (100 sites per
  Region) and `CreateOutpost` (10 Outposts per site), using AWS's own published default quotas
  (`docs.aws.amazon.com/outposts/latest/userguide/outposts-limits.html`) -- previously declared but
  never triggered anywhere, exactly as the prior audit left it.
- Confirmed the "No AWS::Outposts::* CloudFormation resource type" line from the prior audit's
  gaps was never a real parity gap (real AWS CloudFormation has no Outposts support either) and
  dropped it entirely rather than re-filing it as a structural_gap.

Reclassified 3 gaps to `structural_gaps` with individual justification (LifeCycleStatus has no SDK
enum anywhere to converge on; catalog/pricing data is proprietary AWS-published data with no public
source; Connection key material requires a real cryptographic hardware-install exchange) -- see the
frontmatter for why each qualifies under the strict "data source cannot exist" bar, not just
"wasn't verified."

**Why overall stays B, not A**: the single highest-value gap flagged for this pass -- wiring
`services/ec2`'s `RunInstances` to decrement the Outposts capacity ledger -- turned out to be a
genuine architectural blocker, not unfinished work. `services/ec2`'s `Subnet`/`Instance`/
`CapacityReservation` structs carry zero Outpost-placement fields (no `Subnet.OutpostArn`, no
`Instance.Placement.OutpostArn`, no `RunInstances` `Placement.OutpostArn` wire input -- confirmed
by directly grepping `services/ec2/store.go` and `instance_attrs.go`, not assumed). `services/grafana`'s
`cross_service.go` read-only pattern only works because `ec2` already exposed the data grafana
needed (`DescribeSubnets`/`DescribeSecurityGroups`); here `ec2` has no comparable surface to read.
Closing this requires an `ec2`-side change, which is out of this session's `services/outposts/`-only
file-ownership scope -- filed as `gopherstack-9ij1` for a future `ec2`-owning pass. Two smaller gaps
(Order/CapacityTask single-hop lifecycle, 15-of-17 unevaluated `OrderingRequirement` checks) were
also left open, deferred for scope/effort this pass rather than closed -- see `gaps` above for why
each is still genuinely buildable, not structural.

## Implementation summary (2026-08-01 pass)

All 43 operations are implemented with real backend state (no stubs): Outpost/Site CRUD with
one seeded COMPUTE Asset per Outpost (there is no public CreateAsset API to provision one
otherwise), Order/Quote/CapacityTask lifecycles with single-hop async transitions via
`pkgs/worker` (mirroring `services/eks`/`services/grafana`'s pattern), a real capacity ledger
(`Asset.ComputeAttributes.InstanceTypeCapacities`) that `StartCapacityTask` actually mutates and
`GetOutpostInstanceTypes` actually reads, a deterministic (documented-synthetic) pricing model
for quotes/renewals, and full tag support for both Outpost and Site wired into
`resourcegroupstaggingapi` (`cli.go`'s `wireTaggingOutposts`, the 31st tagging-wired service).

**File layout**: `models.go` (stored-state types) / `wire.go` + `wire_convert.go` (JSON wire
shapes, PascalCase document members and query params -- see "New finding" below -- and their
conversion to/from stored state) / `store.go` + `store_setup.go` (`InMemoryBackend`, one coarse
`lockmetrics.RWMutex` since operations routinely cross resource boundaries: CreateOutpost seeds
an Asset, CreateOrder reads a Quote and writes an Order, TagResource resolves an ARN into either
the Outposts or Sites table) / `resolve.go` (shared id-or-ARN resolution) / `outposts.go` /
`sites.go` / `orders.go` / `quotes.go` / `capacity_tasks.go` / `assets.go` / `catalog.go` /
`connections.go` / `renewals.go` / `pricing.go` (backend logic) / `seed_data.go` (static
reference-data tables) / `handler.go` + one `handler_<family>.go` per operation family (HTTP
routing/dispatch) / `persistence.go` / `errors.go` / `consts.go` / `provider.go`.

**Tests**: `sdk_completeness_test.go` (all 43 ops) plus real SDK round-trip tests (following
`services/grafana`'s `sdk_roundtrip_helper_test.go` pattern -- the genuine AWS SDK client against
an `httptest` server, not ad-hoc JSON assertions) across every resource family:
`outposts_test.go`, `sites_test.go`, `orders_test.go`, `quotes_test.go`,
`capacity_tasks_test.go`, `assets_test.go`, `catalog_test.go`, `connections_test.go`,
`tags_test.go`, `persistence_test.go`. All pass under `-race`.

### New finding this pass: query-string and document-member casing is PascalCase, not lowerCamel

The prior audit did not check wire-field casing (it only verified method+path). This pass found
that, unlike `services/grafana` (lowerCamel JSON document members and query params), **every
document member AND every query-string parameter in this service's wire protocol is PascalCase**,
matching the Go SDK's exported field names almost verbatim (e.g. `"OutpostId"`, `"MaxResults"`,
`"AvailabilityZoneFilter"` -- confirmed by grepping every `object.Key("...")` in serializers.go
and every `(Add|Set)Query("...")` call, not assumed from the grafana precedent). The ONE
exception across all 43 operations: `UntagResource`'s `TagKeys` serializes as a repeated
lowerCamel `?tagKeys=` query parameter (`serializers.go:3235`), confirmed by direct grep. Getting
this wrong across the board would have been a silent, hard-to-catch wire-compatibility bug that
unit tests (asserting against the handler's own output) would never have caught -- only the real
SDK round-trip tests would (and did, during development, before the casing was corrected).

### Judgment calls made where the audit flagged a genuine unknown

1. **LifeCycleStatus values** (`ACTIVE`, `PENDING_DECOMMISSION` -- consts.go): the SDK declares
   no enum type at all for this field. Chose immediate `ACTIVE` on create (no invented
   transition workflow with zero SDK backing) and `PENDING_DECOMMISSION` on a successful
   `StartOutpostDecommission`. Both are this implementation's own choice, not AWS fact.
2. **SUPERSEDED by the 2026-08-06 pass, see that section above.** ID/ARN formats for
   Site/Order/Quote/CapacityTask/Asset/Connection (`os-`/`oo-`/`oq-`/`ct-`/`asset-`/`conn-`
   prefixes, `site/<id>` ARN resource segment): at the time, none of these had a confirming
   source and `outpost/<id>` was the only one with in-repo precedent. The 2026-08-06 pass found
   `docs.aws.amazon.com/outposts/latest/APIReference/` publishes exact `Pattern` regexes for all
   of them (fixed 6 real ID-format bugs) and that Quote *does* accept an ARN-shaped identifier on
   input (`GetQuote`/`UpdateQuote`/`DeleteQuote`/`CreateOrder`'s `QuoteIdentifier`) even though it
   has no `QuoteArn` output field -- this pass's "no ARN at all" conclusion for Quote was wrong.
   Order/CatalogItem/Asset/Connection still have no ARN form (unchanged, still correct).
3. **Quote pricing and OrderingRequirements are a deliberately narrow model**, not an attempt to
   fake full AWS-equivalence: a synthetic deterministic pricing formula (documented in
   pricing.go, not real AWS numbers), and only 2 of 17 real `OrderingRequirementType` checks are
   evaluated (the ones this backend has actual state to answer). Fabricating pass/fail for the
   other 15 (e.g. `ENTERPRISE_SUPPORT_ERROR`, which would require a support-plan model this
   backend doesn't have) was rejected as inventing behavior the audit explicitly warned against.
4. **Order/CapacityTask lifecycle collapsed to a single async hop** (PREPARING->COMPLETED,
   REQUESTED->COMPLETED) rather than modeling every intermediate SDK-declared state
   (IN_PROGRESS/DELIVERED, WAITING_FOR_EVACUATION/CANCELLATION_IN_PROGRESS) with no rollup rule
   to base a multi-stage timeline on (the audit's hardest-thing #1). `CancelCapacityTask`
   likewise transitions straight to CANCELLED rather than pausing at the transient
   CANCELLATION_IN_PROGRESS state, since this backend's cancellation is synchronous (there is no
   real hardware-side cleanup to wait for).
5. **Asset seeding**: exactly one COMPUTE asset is created per Outpost at `CreateOutpost` time,
   since there is no public `CreateAsset` operation among the 43 and real Outposts assets arrive
   via physical hardware installation. Documented as a deliberate implementation choice, not an
   attempt to model AWS's actual asset-provisioning process.
6. **`ListAssetInstances`/`ListBlockingInstancesForCapacityTask` always return empty** (after
   validating their required resource exists) rather than inventing placement data -- this
   backend genuinely has no cross-service EC2-on-Outposts instance-placement source, exactly the
   gap the audit's "EC2 capacity coupling" section flagged as out of scope.
7. **`ConflictException.ResourceId`/`ResourceType` are only ever populated for Outpost/Order
   conflicts** (the closed `types.ResourceType` enum's only two members) -- Site/Quote/
   CapacityTask conflicts omit both fields rather than fabricate a `SITE`/`QUOTE` enum value the
   SDK does not declare.
8. **`ServiceQuotaExceededException` has no trigger path** -- no account-level quota model exists
   and no AWS-published default quota number was available to enforce without fabricating one
   (same treatment as `services/grafana`'s `AccessDeniedException`).

### What the audit got right (spot-checked, not re-verified line-by-line here)

The two singular-`/outpost/`-path endpoints, the `/list-orders` action-style slug, the single
`PUT` on `UpdateSiteAddress`, the `EC2Capacity.Quantity`/`.MaxSize` string-typed (not numeric)
fields, `CreateRenewal`'s lone client-token idempotency, and the `OutpostId` vs
`OutpostIdentifier` field-naming inconsistency all matched the audit's findings exactly during
implementation -- confirming the audit's method (reading serializers.go/deserializers.go
directly) was sound.

## Operation count and SDK version (verified, not estimated)

`ls api_op_*.go | grep -v _test.go | wc -l` against
`/home/agbishop/go/pkg/mod/github.com/aws/aws-sdk-go-v2/service/outposts@v1.66.0/` returns **43**,
matching this task's estimate exactly. Resolved via a throwaway scratch module
(`go mod init probe && go get github.com/aws/aws-sdk-go-v2/service/outposts@latest`, run in this
session's scratchpad, never touching this repo's go.mod) -- **v1.66.0** is what `go get @latest`
resolved to at audit time (2026-08-01). This repo's go.mod was not modified by this pass.

The 43 operations, alphabetically: CancelCapacityTask, CancelOrder, CreateOrder, CreateOutpost,
CreateQuote, CreateRenewal, CreateSite, DeleteOutpost, DeleteQuote, DeleteSite, GetCapacityTask,
GetCatalogItem, GetConnection, GetOrder, GetOutpost, GetOutpostBillingInformation,
GetOutpostInstanceTypes, GetOutpostSupportedInstanceTypes, GetQuote, GetRenewalPricing, GetSite,
GetSiteAddress, ListAssetInstances, ListAssets, ListBlockingInstancesForCapacityTask,
ListCapacityTasks, ListCatalogItems, ListOrderableInstanceTypes, ListOrders, ListOutposts,
ListQuotes, ListSites, ListTagsForResource, StartCapacityTask, StartConnection,
StartOutpostDecommission, TagResource, UntagResource, UpdateOutpost, UpdateQuote, UpdateSite,
UpdateSiteAddress, UpdateSiteRackPhysicalProperties.

Protocol is `awsRestjson1` (REST-JSON), confirmed from every serializer's struct name
(`awsRestjson1_serializeOp<Op>`) and every deserializer's error path
(`awsRestjson1_deserializeOpError<Op>`, `restjson.GetErrorInfo`).

## Wire-shape traps worth flagging up front (looks-wrong-but-correct, or just easy to miss)

1. **Two endpoints use a SINGULAR `/outpost/` path** where every other Outpost-family endpoint
   uses plural `/outposts/`: `GetOutpostBillingInformation` (`GET /outpost/{OutpostIdentifier}/billing-information`)
   and `GetRenewalPricing` (`GET /outpost/{OutpostIdentifier}/renewal-pricing`). Verified twice by
   independent grep against `serializers.go` lines 1327 and 1643 -- this is the real wire path, not
   a transcription error in this document. A router that naively prefix-matches `/outposts/` will
   silently 404 these two.
2. **The identifier field name for "the Outpost" is inconsistent across operations.** Some ops
   name it `OutpostId` (`CreateOutpost` -- as the required `SiteId`-analog is different there;
   `GetOutpost`, `DeleteOutpost`, `UpdateOutpost`, `GetOutpostInstanceTypes`), others name the
   identical concept `OutpostIdentifier` (`CancelCapacityTask`, `GetCapacityTask`, `CreateOrder`,
   `CreateRenewal`, `GetOutpostBillingInformation`, `GetOutpostSupportedInstanceTypes`,
   `GetRenewalPricing`, `ListAssetInstances`, `ListAssets`, `ListBlockingInstancesForCapacityTask`,
   `StartCapacityTask`, `StartOutpostDecommission` -- note this one specifically says
   `OutpostIdentifier` even though it decommissions "the Outpost"). Both accept an ID-or-ARN
   string per their doc comments. An implementer building one shared "resolve outpost by
   id-or-arn" helper must accept both field names into it; do not assume a single canonical
   struct field name reused verbatim across every handler.
3. **`ListOrders`'s path is `GET /list-orders`**, an action-style slug, not the expected REST
   collection path `GET /orders` (verified via grep against serializers.go:2390 -- real, not a
   note error). `CancelOrder` similarly uses an action-suffix path (`POST /orders/{OrderId}/cancel`)
   rather than `DELETE /orders/{OrderId}`.
4. **`UpdateSiteAddress` is the only `PUT` in the entire service** (`serializers.go:3610`); every
   other partial-update op in this service is `PATCH`, and it requires the full `Address` struct
   (not a partial merge) per its own doc comment.
5. **`GetOutpostInstanceTypes` vs `GetOutpostSupportedInstanceTypes` are NOT the same query with
   different pagination** -- they answer different questions. The former returns instance types
   *currently configured* on the Outpost (what capacity exists right now); the latter returns
   everything the Outpost's hardware generation *could* support, "generally includ[ing] instance
   types that are not currently configured" per the SDK's own doc comment on
   `GetOutpostSupportedInstanceTypes`. Aliasing one to the other would be a silent, hard-to-catch
   correctness bug, not a stub -- flag prominently for the implementer.
6. **`GetCatalogItem` (`/catalog/item/{id}`, singular) and `ListCatalogItems` (`/catalog/items`,
   plural)** live under sibling-but-different path segments -- confirm both, do not assume one
   implies the other's route prefix.
7. **`EC2Capacity.Quantity` and `.MaxSize` are `*string`, not numeric types**, per
   `types/types.go:322-334` -- these look like they should be `int32` but are wire-typed as
   strings (matches the AssociateLicense-style "looks numeric but isn't" trap class the grafana
   audit warned about).
8. **`CreateRenewal` is the only operation with client-token idempotency auto-fill**
   (`addIdempotencyToken_opCreateRenewalMiddleware`, `api_op_CreateRenewal.go:140-171`) -- if
   `ClientToken` is nil, the SDK client generates one before sending. A real emulator does not need
   to replicate the client-side generation (that happens before the request reaches the server),
   but idempotent-replay semantics keyed on `ClientToken` should be honored server-side if AWS's
   real API does so (not confirmed either way by this SDK checkout; the field is present but its
   idempotency-window server behavior isn't spec'd in Go types).

## State machines to simulate (not a CRUD shell)

- **CapacityTaskStatus** (`REQUESTED` -> `IN_PROGRESS` -> `{FAILED | COMPLETED}`, with side-paths
  `WAITING_FOR_EVACUATION` when blocking EC2 instances exist and the caller chose
  `WAIT_FOR_EVACUATION` in `TaskActionOnBlockingInstances`, and `CANCELLATION_IN_PROGRESS` ->
  `CANCELLED` when `CancelCapacityTask` is called mid-flight). `StartCapacityTask`'s own doc
  comment: only one active capacity task is allowed per (order, Outpost) pair at a time -- a real
  uniqueness invariant, not just a nice-to-have. `ListBlockingInstancesForCapacityTask` and
  `InstancesToExclude` exist specifically to compute whether a task can proceed without operator
  intervention.
- **OrderStatus / LineItemStatus**: `Order.Status` has 11 enum values, 5 explicitly marked
  deprecated in the SDK doc comment (`RECEIVED`, `PENDING`, `PROCESSING`, `INSTALLING`,
  `FULFILLED`) alongside the current set (`PREPARING` -> `IN_PROGRESS` -> `DELIVERED` ->
  `COMPLETED`, or `CANCELLED`/`ERROR`). `LineItem.Status` is a separate, finer-grained 9-value
  enum (`PREPARING`/`BUILDING`/`SHIPPED`/`DELIVERED`/`INSTALLING`/`INSTALLED`/`ERROR`/`CANCELLED`/
  `REPLACED`) -- an Order's overall status is presumably a rollup of its LineItems' individual
  statuses, but the SDK does not encode that rollup rule; an implementer must decide it (document
  the choice, per parity-principles.md's "no invented behavior without saying so").
- **QuoteStatus** (`CREATED` -> `ORDER_SUBMITTED`, or time-based `EXPIRED` via `ExpirationDate`).
  `OrderingRequirement`/`OrderingRequirementType` (17 check-types, e.g.
  `OUTPOST_ACTIVE_CHECK_ERROR`, `MAXIMUM_ALLOWED_ORDERS_CHECK_ERROR`,
  `ENTERPRISE_SUPPORT_ERROR`, `OUTPOST_RENEWAL_REQUIRED_ERROR`) with a per-requirement
  `PASS`/`FAIL`/`EXEMPT` status gate whether `CreateOrder` can actually consume a given quote --
  this is a real business-rule surface, not decorative.
- **Outpost lifecycle** (`LifeCycleStatus`, unmodeled as an enum -- see gaps) plus
  `DecommissionRequestStatus` (`SKIPPED`/`BLOCKED`/`REQUESTED`) from
  `StartOutpostDecommission`, which is a request-acceptance status, not a completion state --
  the real decommission is an out-of-band hardware-return process that this emulator can at most
  flag on the Outpost record, not fully simulate to completion.
- **Site/Order/Outpost relationship**: an Outpost is created via `CreateOutpost(SiteId=...)` --
  belongs to exactly one Site. Capacity growth or hardware fulfillment happens via
  `CreateOrder(OutpostIdentifier=..., LineItems=[{CatalogItemId,Quantity}])`, where each
  `LineItem.CatalogItemId` must resolve against `ListCatalogItems`/`GetCatalogItem`'s catalog.
  `CreateQuote`/`CreateOrder` can alternatively flow through `QuoteIdentifier`/
  `QuoteOptionIdentifier` to pre-price a configuration before ordering. None of these FK
  relationships are optional to simulate faithfully -- an Order referencing a nonexistent
  `OutpostIdentifier` or a Quote referencing a nonexistent `CatalogItemId` should fail, not
  silently succeed.
- **Asset / capacity tracking**: `Asset.ComputeAttributes.InstanceTypeCapacities`
  (`[]AssetInstanceTypeCapacity{Count,InstanceType}`) is the actual capacity ledger backing
  `GetOutpostInstanceTypes`'s response -- `StartCapacityTask` (with `InstancePools`) is the
  operation that should mutate it once a task transitions to `COMPLETED`. `ComputeAttributes.State`
  (`ACTIVE`/`ISOLATED`/`RETIRING`/`INSTALLING`) gates whether an asset can accept new capacity
  tasks at all.
- **Connection/private-connectivity**: `StartConnection`/`GetConnection` model a WireGuard-style
  tunnel used for physical Outpost SERVER installation (per both ops' doc comments, which
  explicitly say "Amazon Web Services uses this action to install Outpost servers" and recommend
  CloudTrail monitoring). This is a narrow, install-time-only flow, not general network
  connectivity -- do not over-build it into a general VPN/tunnel simulation.

## Cross-service wiring needed

**Tagging.** `TagResource`/`UntagResource`/`ListTagsForResource` exist
(`api_op_TagResource.go`, `api_op_UntagResource.go`, `api_op_ListTagsForResource.go`), so this
service should be wired into `cli.go`'s `wireResourceGroupsTagging`
(`/home/agbishop/gopherstack/cli.go:5348`), following the `wireTaggingGrafana` pattern
(`cli.go:6675-6701`, itself calling the generic `wireTaggingCtxARNResources` helper used by
`wireTaggingEFS` at `cli.go:6127-6152`). Both `Outpost.Tags` and `Site.Tags`
(`types/types.go:650`, `:1019`) exist, but there is only ONE generic ARN-keyed tag API shared
across both resource kinds -- the tag store backing this wiring needs to be keyed by the full
ARN (Outpost ARN or Site ARN), not scoped to a single resource-type map like most other
`wireTaggingXxx` functions. `wireResourceGroupsTagging` currently wires exactly 30 services
(`cli.go:5327-5399`'s own doc comment enumerates them); Outposts would be the 31st entry, added
alongside the existing `wireTaggingGrafana(bk, byName["Grafana"])` line as
`wireTaggingOutposts(bk, byName["Outposts"])` (name TBD by however this service registers itself
in `byName`, which is keyed by `service.Registerable.Name()` -- not confirmed here since the
service doesn't exist yet to register anything).

**ARN namespace**: everywhere this repo already constructs or asserts an Outposts-related ARN,
it uses **`outposts`** as the ARN service segment, matching the package name (i.e. this is NOT
one of the seven mismatches the broader campaign found, like `stepfunctions`->`states` or
`efs`->`elasticfilesystem`). Evidence, all from in-repo test fixtures (not production code, since
no Outposts service exists yet to build ARNs in the first place):
- `services/ec2/handler_local_gateway_test.go:21`: `OutpostArn: "arn:aws:outposts:us-east-1:000000000000:outpost/op-1"`
- `services/ec2/local_gateway_test.go:17,91,282`: same `arn:aws:outposts:...:outpost/op-1` shape
- `services/route53resolver/outpost_resolvers_test.go:32` and 20+ other lines in that file: same shape
- `services/route53resolver/persistence_test.go:146`: same shape

All of these are test-only string literals passed through opaque `OutpostArn string` fields (see
below) -- none of them are derived from a real ARN-building function, so this is corroborating
precedent, not primary confirmation. **What could NOT be confirmed this pass**, despite attempts:
the SDK itself carries no `@arn`/resource-pattern trait on any of the three tagging ops'
`ResourceArn *string` fields (same limitation the grafana audit hit -- confirmed by reading
`api_op_TagResource.go`/`api_op_UntagResource.go`/`api_op_ListTagsForResource.go` directly, all
three declare `ResourceArn *string` with no pattern doc). Terraform's `internal/service/outposts`
package (fetched via WebFetch from
`raw.githubusercontent.com/hashicorp/terraform-provider-aws/main/...`) does not construct the
Outpost ARN client-side at all -- unlike Grafana's ARN (which Terraform builds locally via
`RegionalARN`), Outposts' `OutpostArn` is returned directly by the real API in every response
shape (`Outpost.OutpostArn`, `Site.SiteArn`, `Quote.OutpostArn`,
`GetOutpostInstanceTypesOutput.OutpostArn`) and Terraform's `outpost_data_source.go` just does
`d.Set(names.AttrARN, outpost.OutpostArn)`. AWS's own Service Authorization Reference page
(`docs.aws.amazon.com/service-authorization/latest/reference/list_awsoutposts.html`) and User
Guide security page returned only a JS-shell body to WebFetch (same failure mode the grafana audit
hit on the same domain) -- **the exact resource-path segment for Site (`site/<id>`?), Order
(`order/<id>`? `order/<outpost-id>/<order-id>`?), Quote, and CatalogItem ARNs is an HONEST
UNKNOWN**, not fabricated here. Only the Outpost's own `outpost/<id>` segment has any supporting
evidence at all, and even that is in-repo precedent rather than a primary source. An implementer
should treat `outpost/<id>` as reasonable-but-unconfirmed and actively verify Site/Order/Quote/
CatalogItem ARN shapes before hardcoding them, rather than trusting this document's guesses.

**Existing opaque `OutpostArn`/Outposts-adjacent fields elsewhere in the tree** -- all confirmed
to be plain unvalidated strings today, with ZERO cross-service call into anything that could
become this new Outposts service:
- `services/ec2/local_gateway.go:51,93,525` -- `LocalGateway.OutpostArn` and (per
  `handler_secondary_net_test.go:114-125,235-254` and `secondary_net_test.go:130-154`) an
  `OutpostLag`/`SeedOutpostLag`/`DescribeOutpostLags` family, all storing `OutpostArn string` with
  `json:"outpostArn,omitempty"` and no existence check against anything.
- `services/route53resolver/handler_outpost_resolvers.go:17,26,42,57-58,66` -- a full
  `CreateOutpostResolver`/`GetOutpostResolver`/`UpdateOutpostResolver`/`DeleteOutpostResolver`/
  `ListOutpostResolvers` CRUD family (`services/route53resolver/outpost_resolvers_test.go` has 20+
  tests for it) that requires `OutpostArn` be non-empty (`handler_outpost_resolvers.go:57-58`:
  `if in.OutpostArn == "" { return ...ErrValidation }`) but never checks it resolves to a real
  Outpost. Also `handler_resolver_endpoints.go:49,73,132,154` and `resolver_endpoints.go:97` --
  Resolver Endpoints optionally carry an `OutpostArn` too.
- `services/s3control/interfaces.go:27,91-119` -- an `OutpostsBucket` type family
  (`CreateBucket`/`GetBucket`/`ListRegionalBuckets`), keyed purely by `bucketName` with no
  `OutpostId`/ARN field on the bucket itself at all (`handler_bucket_test.go:148-152` explicitly
  notes "has no BucketArn or OutpostId field in the real SDK").
- `services/datasync/handler_locations_test.go:177-187` -- `CreateLocationS3`'s `AgentArns` field
  is documented as the Outposts-agent mechanism (`datasync/PARITY.md:14`: "added AgentArns
  (Outposts) input, real member").
- `services/emr` -- cluster records carry an `OutpostArn` field per
  `services/emr/handler_wire_shape_test.go:610`'s comment listing it among cluster-summary fields.
- `services/ram/handler_resources.go:236-237` -- the Resource Groups' resource-share-type registry
  already lists `{ResourceType: "outposts:Outpost", ServiceName: "outposts"}` as a shareable
  resource type (this is RAM's own catalog of shareable AWS resource *types*, not a live
  connection to any Outposts backend -- it independently corroborates the `outposts` ARN service
  namespace, though).
- `services/elbv2/README.md:26` and `PARITY.md:83` note `IpamPools`/`CustomerOwnedIpv4Pool`/
  `EnforceSecurityGroupInboundRulesOnPrivateLinkTraffic` as unimplemented
  Outposts/IPAM-adjacent `LoadBalancer` fields -- these are genuinely Outposts-adjacent (Outposts
  networking touches customer-owned IP pools) but are a separate, already-tracked ELBv2 gap, not
  something this new service should try to satisfy.

None of the above needs to change for a first Outposts implementation to land -- they're
independent opaque-string fields today and will keep working unmodified. But once a real
`services/outposts` backend exists, a *future* pass could sensibly cross-validate these ARNs
against it (e.g. `route53resolver`'s `CreateOutpostResolver` rejecting an `OutpostArn` that
doesn't resolve via the new service's own `GetOutpost`) -- flagged here as a real follow-on
opportunity, explicitly out of scope for this audit and for a first implementation pass.

**CloudFormation**: `grep -rli outpost services/cloudformation/` returned nothing -- there is no
`AWS::Outposts::*` resource type in `services/cloudformation/resources_*.go`, confirmed by both
the grep and by listing every `resources_*.go` file in that directory (none named for outposts).
This appears to reflect reality, not a gopherstack gap: AWS Outposts is provisioned through a
physical-hardware ordering process (this very API), not typical declarative infrastructure, and
this audit found no evidence AWS's real CloudFormation supports it either (not exhaustively
confirmed against AWS's own CFN resource-type registry, but consistent with Outposts' order/site
workflow being fundamentally a support-ticket-adjacent process rather than a declarative resource
lifecycle).

**EC2 capacity coupling**: real AWS ties `RunInstances` on an Outpost-hosted subnet to that
Outpost's currently configured `InstanceTypeCapacity`. **SUPERSEDED by the 2026-08-06 EC2
capacity-coupling pass, see that section above** -- at the time this note was written (before
either service existed), `services/ec2` had no such coupling and building it was out of scope;
`services/ec2` now has `Subnet.OutpostArn`/`Instance.OutpostArn` and calls into
`services/outposts/capacity_ledger.go` from `RunInstances`/`TerminateInstances`.

## Top 5 hardest/riskiest things about implementing this service (for the caller's final report)

1. **The Order/LineItem/CapacityTask status-rollup rules are not encoded in the SDK** -- the
   relationship between an Order's overall `Status`, its LineItems' individual `Status` values,
   and a CapacityTask's own lifecycle is implied by field names and doc-comment prose, not by any
   machine-checkable contract. Any implementation has to invent a defensible rollup rule and
   document it as a deliberate choice (per parity-principles.md), not hide it as though it were
   AWS's actual behavior.
2. **The ARN format for Site/Order/Quote/CatalogItem resources is a genuine unknown**, not just
   an "SDK doesn't confirm it" formality -- multiple independent lookup paths (SDK trait, real
   Terraform provider source, AWS's own docs pages) all failed to produce a citable answer. Only
   `outpost/<id>` has any supporting evidence, and it's second-hand (in-repo test fixtures, not a
   primary source).
3. **Two field-naming inconsistencies (`OutpostId` vs `OutpostIdentifier` for the same concept,
   and the two singular-`/outpost/`-path endpoints) are easy to implement wrong** if a router or
   ID-resolution helper is written by pattern-matching most operations and extrapolating to the
   rest, rather than checking each operation's own serializer.
4. **Static reference-data operations** (`ListCatalogItems`/`GetCatalogItem`,
   `ListOrderableInstanceTypes`) require a seed dataset that doesn't exist anywhere in the SDK or
   this repo -- there's no way to make these "really AWS-accurate" without external data AWS
   doesn't publish machine-readably, so the honest move is a small defensible static seed (à la
   grafana's `ListVersions`), clearly flagged as a stand-in.
5. **`StartOutpostDecommission`'s "state machine" is mostly a request receipt, not a real async
   completion flow** -- `DecommissionRequestStatus` only has `SKIPPED`/`BLOCKED`/`REQUESTED`, no
   terminal "decommissioned" value modeled in this SDK at all, meaning the real end-state
   presumably lives on `Outpost.LifeCycleStatus` (itself unconfirmed, see gap #2 above) rather
   than on the decommission response -- two unconfirmed-enum problems compounding on the same
   feature.
