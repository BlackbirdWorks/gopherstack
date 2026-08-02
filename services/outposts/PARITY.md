---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: outposts
sdk_module: aws-sdk-go-v2/service/outposts@v1.66.0   # real go.mod dependency now (go get run this pass)
last_audit_commit: 7922e4c4d   # HEAD when the pre-implementation audit was written; this pass
# implemented the full service on top of it (uncommitted at the time this manifest was updated).
last_audit_date: 2026-08-01
# Grade B: from-scratch implementation (nothing pre-existing to fix), every one of the 43
# operations' wire shapes read directly from serializers.go/deserializers.go (never assumed
# from Go struct field names alone), and proven via a real SDK round-trip test harness
# (sdk_roundtrip_helper_test.go, following services/grafana's identical pattern) that caught
# one real routing bug before it shipped: the two singular-`/outpost/`-path handlers
# (GetOutpostBillingInformation, GetRenewalPricing) initially read the OutpostIdentifier from
# the wrong path segment (segs[2], the literal "billing-information"/"renewal-pricing" string)
# instead of segs[1] -- every round-trip test against those two ops 404'd until fixed. See
# "Implementation summary" below for the full judgment-call list and what remains partial.
overall: B
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
# All 43 ops are routed, backed by real state, and persisted via InMemoryBackend.Snapshot/Restore
# (persistence.go). "partial" below marks operations where a genuinely unknowable input (no SDK
# enum, no public AWS data) forced a documented, narrower-than-real-AWS behavior -- not a stub.
ops:
  CreateOutpost: {wire: ok, errors: ok, state: ok, persist: ok, note: "POST /outposts (outposts.go); seeds one COMPUTE Asset (assets.go); LifeCycleStatus set to ACTIVE immediately -- see gaps"}
  GetOutpost: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /outposts/{OutpostId}, id-or-ARN via resolveOutpostLocked (resolve.go)"}
  DeleteOutpost: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETE /outposts/{OutpostId}; Conflict while a REQUESTED capacity task exists; cascades its seeded Asset(s)"}
  UpdateOutpost: {wire: ok, errors: ok, state: ok, persist: ok, note: "PATCH /outposts/{OutpostId}; merges Description/Name/SupportedHardwareType onto existing state"}
  ListOutposts: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /outposts; AvailabilityZoneFilter/AvailabilityZoneIdFilter/LifeCycleStatusFilter all as repeated PascalCase query params (confirmed via serializers.go, NOT lowerCamel like grafana -- see wire.go), paginated via pkgs/page"}
  StartOutpostDecommission: {wire: ok, errors: ok, state: partial, persist: ok, note: "POST /outposts/{OutpostIdentifier}/decommission; SKIPPED on idempotent replay, REQUESTED otherwise, BLOCKED never occurs (no cross-service blocking-resource data -- see gaps); ValidateOnly performs no mutation"}
  GetOutpostBillingInformation: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /outpost/{OutpostIdentifier}/billing-information (singular path, routed correctly -- see Grade note); accumulates ORIGINAL subscription on order completion (orders.go) and RENEWAL on CreateRenewal (renewals.go)"}
  GetOutpostInstanceTypes: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /outposts/{OutpostId}/instanceTypes; aggregates the CONFIGURED capacity across the Outpost's Assets (mutated by StartCapacityTask completion), distinct from GetOutpostSupportedInstanceTypes"}
  GetOutpostSupportedInstanceTypes: {wire: ok, errors: ok, state: partial, persist: n/a, note: "GET /outposts/{OutpostIdentifier}/supportedInstanceTypes; returns the static seed catalog filtered by hardware type -- AssetId/OrderId are validated to exist but do not further filter the result (documented simplification, see gaps)"}
  GetRenewalPricing: {wire: ok, errors: ok, state: ok, persist: n/a, note: "GET /outpost/{OutpostIdentifier}/renewal-pricing (singular path, routed correctly); PRICED for an ACTIVE Outpost, UNABLE_TO_PRICE otherwise"}
  CreateRenewal: {wire: ok, errors: ok, state: ok, persist: ok, note: "POST /renewals; ClientToken idempotency implemented (renewals.go's renewalIdempotency cache); pricing is a documented synthetic placeholder formula -- see gaps"}
  CreateSite: {wire: ok, errors: ok, state: ok, persist: ok, note: "POST /sites; OperatingAddress flattened to 3 fields on output, ShippingAddress fully stored but only surfaced via GetSiteAddress"}
  GetSite: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /sites/{SiteId}, id-or-ARN"}
  UpdateSite: {wire: ok, errors: ok, state: ok, persist: ok, note: "PATCH /sites/{SiteId}; merges Description/Name/Notes"}
  DeleteSite: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETE /sites/{SiteId}; Conflict while any Outpost still references it"}
  ListSites: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /sites; city/countryCode/stateOrRegion filters against OperatingAddress"}
  GetSiteAddress: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /sites/{SiteId}/address; AddressType as query param, returns Shipping or Operating full Address"}
  UpdateSiteAddress: {wire: ok, errors: ok, state: ok, persist: ok, note: "PUT /sites/{SiteId}/address; full replacement (not merge); Conflict while the Site has a PREPARING order"}
  UpdateSiteRackPhysicalProperties: {wire: ok, errors: ok, state: ok, persist: ok, note: "PATCH /sites/{SiteId}/rackPhysicalProperties; merges only non-empty fields; same in-progress-order Conflict check"}
  CreateOrder: {wire: ok, errors: ok, state: partial, persist: ok, note: "POST /orders; OrderType always OUTPOST (CreateOrderInput has no OrderType member); single-hop PREPARING -> COMPLETED transition (no IN_PROGRESS/DELIVERED stop) -- see gaps; validates CatalogItemId and consumed Quote"}
  GetOrder: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /orders/{OrderId}, ID-only (no ARN form on this op)"}
  CancelOrder: {wire: ok, errors: ok, state: ok, persist: ok, note: "POST /orders/{OrderId}/cancel; Conflict once terminal"}
  ListOrders: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /list-orders; OutpostIdentifierFilter singular, paginated"}
  CreateQuote: {wire: ok, errors: ok, state: partial, persist: ok, note: "POST /quotes; single synthesized QuoteOption (not an N-option combinatorial shape); OrderingRequirements covers 2 of 17 real check types this backend has state to evaluate -- see gaps; pricing is a documented synthetic formula"}
  GetQuote: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /quotes/{QuoteIdentifier}; lazily flips CREATED -> EXPIRED past ExpirationDate"}
  UpdateQuote: {wire: ok, errors: ok, state: ok, persist: ok, note: "PATCH /quotes/{QuoteIdentifier}; OutpostIdentifier tri-state (nil=no-change, empty=clear, value=set) implemented via *string wire field; never returns Conflict (none in this op's wire error set)"}
  DeleteQuote: {wire: ok, errors: ok, state: ok, persist: ok, note: "DELETE /quotes/{QuoteIdentifier}"}
  ListQuotes: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /quotes; no filters, paginated; lazily expires each"}
  CancelCapacityTask: {wire: ok, errors: ok, state: partial, persist: ok, note: "POST /outposts/{OutpostIdentifier}/capacity/{CapacityTaskId}; transitions directly REQUESTED -> CANCELLED (skips the transient CANCELLATION_IN_PROGRESS state -- documented simplification, see gaps)"}
  GetCapacityTask: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET .../capacity/{CapacityTaskId}"}
  StartCapacityTask: {wire: ok, errors: ok, state: partial, persist: ok, note: "POST /outposts/{OutpostIdentifier}/capacity; enforces one-active-task-per-(Outpost,Order); single-hop REQUESTED -> COMPLETED mutates the target Asset's real capacity ledger; WAITING_FOR_EVACUATION never occurs (no cross-service blocking-instance data) -- see gaps; DryRun completes synchronously without mutating capacity"}
  ListCapacityTasks: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /capacity/tasks; status + OutpostIdentifierFilter"}
  ListBlockingInstancesForCapacityTask: {wire: ok, errors: ok, state: partial, persist: n/a, note: "GET .../blockingInstances; validates the capacity task exists, always returns empty (no cross-service EC2-on-Outposts placement data -- honest-empty, not a stub, see gaps)"}
  ListAssets: {wire: ok, errors: ok, state: ok, persist: ok, note: "GET /outposts/{OutpostIdentifier}/assets; filters by AssetTypeFilter/HostIdFilter/StatusFilter against the seeded Asset(s)"}
  ListAssetInstances: {wire: ok, errors: ok, state: partial, persist: n/a, note: "GET .../assetInstances; validates the Outpost exists, always returns empty -- same honest-empty EC2-coupling gap as ListBlockingInstancesForCapacityTask"}
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
  - "LifeCycleStatus (bare *string, no SDK enum -- confirmed, see prior audit) is set to ACTIVE immediately on CreateOutpost and PENDING_DECOMMISSION on StartOutpostDecommission success. Both string values are this implementation's own choice (documented in consts.go), not confirmed AWS fact -- no created->active transition workflow is invented since nothing in the SDK describes one."
  - "ARN resource-path format for Site (site/<id>), Order, Quote, CatalogItem, Asset, Connection, and Subscription IDs are UNCONFIRMED formats (op-/os-/oo-/oq-/ct-/asset-/conn-/li-/qo-/sub- prefixes are this implementation's own choice, documented per-generator in store.go). Only the Outpost ARN shape (outpost/<id>) has corroborating in-repo precedent, as the prior audit found. Order/Quote/CatalogItem have no ARN at all in this implementation (not needed by any of the 43 ops; only Outpost and Site are tagged)."
  - "Quote pricing/OrderingRequirements are a documented simplification, not real AWS data: (1) pricing.go's basePriceOneYear/ThreeYears/FiveYears figures are an emulator-invented deterministic formula (no public Outposts pricing data exists to model against) -- real, correctly-typed Currency/MonthlyRecurringPrice/UpfrontPrice fields, synthetic numbers; (2) quotes.go's buildOrderingRequirements evaluates only 2 of the 17 real OrderingRequirementType checks (OUTPOST_ID_MISSING_ON_QUOTE_ERROR, OUTPOST_ACTIVE_CHECK_ERROR) -- the other 15 (ENTERPRISE_SUPPORT_ERROR, VALID_ZIP_CODE_CHECK_ERROR, etc.) are real wire-accurate enum values this backend has no state to evaluate; (3) each Quote synthesizes exactly one QuoteOption with an always-empty Specifications list (no fabricated rack/server physical-spec numbers)."
  - "Order/CapacityTask lifecycle uses a single-hop async transition (PREPARING->COMPLETED, REQUESTED->COMPLETED) via pkgs/worker, mirroring services/grafana's scheduleWorkspaceTransition -- the intermediate states each type's SDK enum declares (Order: IN_PROGRESS/DELIVERED; CapacityTask: WAITING_FOR_EVACUATION/CANCELLATION_IN_PROGRESS) are real, wire-accurate constants this emulator never transitions through, since no rollup rule is encoded anywhere in the SDK (per the prior audit's hardest-thing #1)."
  - "ListAssetInstances and ListBlockingInstancesForCapacityTask always return an empty result after validating their required resources exist -- this backend has no cross-service EC2-on-Outposts instance-placement data (confirmed gap, not scoped to this pass -- see 'EC2 capacity/launch integration' below). This is an honest empty result, not a stub, per parity-principles.md's guidance on real-logic-then-empty-result."
  - "ListCatalogItems/GetCatalogItem/ListOrderableInstanceTypes are served from a small static seed table (seed_data.go: 3 catalog items, 5 orderable instance types) standing in for AWS's own published, centrally-maintained hardware catalog -- a defensible placeholder (a la grafana's ListVersions), not the authoritative AWS catalog, exactly as the prior audit anticipated."
  - "ServiceQuotaExceededException (declared on CreateOutpost/CreateSite/CreateOrder's own wire error sets) has no trigger path in this backend -- no account-level resource-count quota model exists, and no AWS-published default quota values were available to enforce without fabricating a number. Matches services/grafana's identical treatment of AccessDeniedException. Sentinel (errQuotaExceeded) and handleError branch are wired and ready if a future pass adds a real quota."
  - "EC2 capacity/launch integration: services/ec2's RunInstances is not wired to check or decrement this service's Outposts capacity ledger (ComputeAttributes.InstanceTypeCapacities) -- explicitly out of scope for this pass, exactly as the prior audit flagged; a real cross-service feature for a future pass."
  - "No AWS::Outposts::* CloudFormation resource type exists in this repo, and (per the prior audit) AWS's own CloudFormation likely does not support Outposts resources either -- unchanged from the prior audit, not scoped as parity work."
  - "Connection/StartConnection key material (ServerPublicKey, tunnel addresses, UnderlayIpAddress) is synthetic and non-cryptographic (connections.go) -- explicitly documented, matching the prior audit's narrow-scope call on this WireGuard-style, install-time-only flow."
leaks: {status: clean, note: "InMemoryBackend.Reset() closes every Outpost's and Site's tags.Tags before clearing (store.go); Close() stops the worker.Group backing every scheduled Order/CapacityTask transition timer (mirrors services/grafana's scheduleWorkspaceActivation pattern the prior audit called out as the thing to watch for)."}
---

## Implementation summary (this pass)

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
2. **ID/ARN formats for Site/Order/Quote/CapacityTask/Asset/Connection** (`os-`/`oo-`/`oq-`/
   `ct-`/`asset-`/`conn-` prefixes, `site/<id>` ARN resource segment): none of these have any
   confirming source (same conclusion the audit reached). Only `outpost/<id>` has in-repo
   precedent. Order/Quote/CatalogItem/Asset/Connection do not get ARNs at all in this
   implementation, since no operation among the 43 actually requires one (confirmed by
   rereading every op's input/output shape) -- only Outpost and Site ARNs are ever constructed
   or consumed.
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
Outpost's currently configured `InstanceTypeCapacity`. `services/ec2` today has no such coupling
(confirmed: it stores `OutpostArn` as an opaque string on `LocalGateway`/`OutpostLag`, per above,
with no capacity-check hook). Building that coupling is explicitly NOT this audit's scope and
should not be assumed as part of a first Outposts implementation -- flagged as a gap, and as a
concrete idea for a later cross-service pass once both sides exist.

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
