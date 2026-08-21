---
# PARITY MANIFEST SCHEMA — see services/_PARITY_TEMPLATE.md for the schema doc.
service: eks
sdk_module: aws-sdk-go-v2/service/eks@v1.90.4
last_audit_commit: 7c297a53  # gopherstack-uult (2026-08-13) fixed after this hash was recorded; hash not yet known at edit time
last_audit_date: 2026-08-13
overall: A            # route-matcher pass (prior audit) + gaps/deferred closeout pass (this audit)
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  ListClusters: {wire: fixed, errors: ok, state: ok, persist: ok, note: "now supports maxResults/nextToken pagination via pkgs/page (was returning the full list in one page)"}
  DeleteCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateClusterConfig: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "was routed as bare-path PUT /clusters/{name}; real path is POST /clusters/{name}/update-config. gopherstack-muzq (2026-08-21): the returned Update record was stamped InProgress and never advanced -- DescribeUpdate polled InProgress forever; now scheduled to Successful via scheduleUpdateTransition"}
  UpdateClusterVersion: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "was routed at fictional POST /clusters/{name}/update-version; real path is POST /clusters/{name}/updates (shared with ListUpdates GET). gopherstack-muzq (2026-08-21): same InProgress-forever bug and fix as UpdateClusterConfig"}
  RegisterCluster: {wire: fixed, errors: ok, state: ok, persist: ok, note: "was routed at /clusters/{placeholder}/register; real path is global POST /cluster-registrations (name comes from body, always did)"}
  DeregisterCluster: {wire: fixed, errors: ok, state: ok, persist: ok, note: "was routed as POST /clusters/{name}/deregister; real path is DELETE /cluster-registrations/{name}"}
  DescribeClusterVersions: {wire: ok, errors: ok, state: ok, persist: n/a}
  AssociateEncryptionConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateNodegroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeNodegroup: {wire: ok, errors: ok, state: ok, persist: ok}
  ListNodegroups: {wire: fixed, errors: ok, state: ok, persist: ok, note: "now supports maxResults/nextToken pagination"}
  DeleteNodegroup: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateNodegroupConfig: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "was reachable on a bare POST to the nodegroup path with no suffix check, so real SDK traffic to .../update-config fell through with a corrupted nodegroupName (the literal suffix baked in); now requires the real /update-config suffix. gopherstack-muzq (2026-08-21): the Update record built in the handler was stamped InProgress and never advanced; now scheduled to Successful"}
  UpdateNodegroupVersion: {wire: ok, errors: ok, state: fixed, persist: ok, note: "gopherstack-muzq (2026-08-21): the returned Update record was stamped InProgress and never advanced -- DescribeUpdate polled InProgress forever; now scheduled to Successful via scheduleUpdateTransition"}
  CreateAddon: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAddon: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAddons: {wire: fixed, errors: ok, state: ok, persist: ok, note: "now supports maxResults/nextToken pagination"}
  DeleteAddon: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAddon: {wire: fixed, errors: ok, state: ok, persist: ok, note: "was PUT to bare addon path; real op is POST .../addons/{addonName}/update"}
  DescribeAddonVersions: {wire: fixed, errors: ok, state: n/a, persist: n/a, note: "path was /addon-versions; real path is /addons/supported-versions — was completely unreachable by the real SDK client"}
  DescribeAddonConfiguration: {wire: fixed, errors: ok, state: n/a, persist: n/a, note: "path was /addon-configuration; real path is /addons/configuration-schemas — was completely unreachable"}
  CreateAccessEntry: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAccessEntry: {wire: fixed, errors: ok, state: ok, persist: ok, note: "added ModifiedAt (real aws-sdk-go-v2/service/eks/types.AccessEntry.ModifiedAt was entirely unmodeled); set on create and every update"}
  ListAccessEntries: {wire: fixed, errors: ok, state: ok, persist: ok, note: "now supports maxResults/nextToken pagination"}
  DeleteAccessEntry: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAccessEntry: {wire: fixed, errors: ok, state: ok, persist: ok, note: "was routed as PUT; real method is POST to the same leaf path. Also now sets ModifiedAt"}
  AssociateAccessPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateAccessPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAssociatedAccessPolicies: {wire: fixed, errors: ok, state: ok, persist: n/a, note: "now supports maxResults/nextToken pagination"}
  ListAccessPolicies: {wire: fixed, errors: ok, state: n/a, persist: n/a, note: "wire key for each entry was 'policyArn'; real aws-sdk-go-v2/service/eks/types.AccessPolicy field (deserializers.go's awsRestjson1_deserializeDocumentAccessPolicy) is 'arn' -- 'policyArn' is the correct key for AssociatedAccessPolicy elsewhere in this API but was wrong here. Also now supports maxResults/nextToken pagination"}
  CreateFargateProfile: {wire: fixed, errors: ok, state: ok, persist: ok, note: "added Health (real aws-sdk-go-v2/service/eks/types.FargateProfile.Health was entirely absent from the wire response, not just unmodeled in the struct)"}
  DescribeFargateProfile: {wire: fixed, errors: ok, state: ok, persist: ok, note: "same Health fix as CreateFargateProfile"}
  ListFargateProfiles: {wire: fixed, errors: ok, state: ok, persist: ok, note: "now supports maxResults/nextToken pagination; no UpdateFargateProfile exists in the real API either"}
  DeleteFargateProfile: {wire: ok, errors: ok, state: ok, persist: ok}
  CreatePodIdentityAssociation: {wire: fixed, errors: ok, state: ok, persist: ok, note: "added ModifiedAt/ExternalId/Policy/DisableSessionTags -- all real aws-sdk-go-v2/service/eks/types.PodIdentityAssociation fields that were entirely unmodeled"}
  DescribePodIdentityAssociation: {wire: fixed, errors: ok, state: ok, persist: ok, note: "same field additions as CreatePodIdentityAssociation"}
  ListPodIdentityAssociations: {wire: fixed, errors: ok, state: ok, persist: ok, note: "was emitting the FULL PodIdentityAssociation shape (roleArn/createdAt/tags included); real ListPodIdentityAssociations returns the PodIdentityAssociationSummary shape which deliberately omits those fields -- verified against types.PodIdentityAssociationSummary. Also now supports maxResults/nextToken pagination"}
  DeletePodIdentityAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdatePodIdentityAssociation: {wire: fixed, errors: ok, state: ok, persist: ok, note: "was routed as PUT; real method is POST to the same leaf path. Now also accepts Policy/DisableSessionTags and sets ModifiedAt"}
  AssociateIdentityProviderConfig: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "now captures groupsPrefix/usernamePrefix/requiredClaims (previously dropped) and generates a real ARN (previously unset). gopherstack-muzq (2026-08-21): Status was stamped CREATING and nothing ever advanced it -- no ticker, no later call, while sibling cluster/addon/nodegroup resources transition correctly; now scheduled to ACTIVE mirroring scheduleClusterActivation"}
  DescribeIdentityProviderConfig: {wire: fixed, errors: ok, state: ok, persist: n/a, note: "response was a flat {clusterName,name,type,status,oidc,createdAt} object; real shape (aws-sdk-go-v2/service/eks/types.IdentityProviderConfigResponse) nests the full OidcIdentityProviderConfig under an 'oidc' key with identityProviderConfigName/identityProviderConfigArn/clientId/issuerUrl/usernameClaim/usernamePrefix/groupsClaim/groupsPrefix/requiredClaims/tags/status fields, none of which matched gopherstack's flat shape. Route-match looseness (any 3rd path segment) is unchanged, still intentional"}
  ListIdentityProviderConfigs: {wire: fixed, errors: ok, state: ok, persist: ok, note: "now supports maxResults/nextToken pagination (envelope shape {name,type} pairs was already correct)"}
  DisassociateIdentityProviderConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateCapability: {wire: fixed, errors: fixed, state: fixed, persist: fixed, note: "was a GLOBAL (non-cluster-scoped) resource keyed only by 'name' at POST /capabilities, which does not exist in the real API at all (fabricated path). Real Capability is cluster-scoped (unique CapabilityName per cluster) at /clusters/{clusterName}/capabilities and requires ClusterName+CapabilityName+Type+RoleArn+DeletePropagationPolicy. Rebuilt: composite-keyed store (capabilityKey), cluster-scoped route, required-field validation, capabilityName/clusterName/arn/type/roleArn/deletePropagationPolicy/createdAt/tags on the wire (was emitting only name/version/status under the wrong field name 'name' instead of 'capabilityName'). This pass additionally added ModifiedAt/Health/Configuration and accepts (but does not persist for idempotency) ClientRequestToken"}
  DescribeCapability: {wire: fixed, errors: fixed, state: fixed, persist: fixed}
  ListCapabilities: {wire: fixed, errors: fixed, state: fixed, persist: fixed, note: "was returning bare capability-name strings; real ListCapabilities returns CapabilitySummary objects (capabilityName/arn/status/type/version/createdAt/modifiedAt) -- verified against types.CapabilitySummary. Also now supports maxResults/nextToken pagination"}
  DeleteCapability: {wire: fixed, errors: fixed, state: fixed, persist: fixed}
  UpdateCapability: {wire: fixed, errors: fixed, state: fixed, persist: fixed, note: "was PUT; real method is POST to the same leaf path. ModifiedAt now set on every update; Health/Configuration were added to the model (see CreateCapability note) -- Configuration remains a passthrough map (no per-capability-type ArgoCd/Ack/Kro schema validation)"}
  CreateEksAnywhereSubscription: {wire: fixed, errors: fixed, state: fixed, persist: fixed, note: "path was /subscriptions; real path is /eks-anywhere-subscriptions — was completely unreachable. Also now validates the required 'term' field (unit must be MONTHS, duration must be 12 or 36 -- verified against types.EksAnywhereSubscriptionTerm) and models autoRenew/effectiveDate/expirationDate, none of which were previously modeled at all"}
  DescribeEksAnywhereSubscription: {wire: fixed, errors: ok, state: ok, persist: ok}
  ListEksAnywhereSubscriptions: {wire: fixed, errors: ok, state: ok, persist: ok, note: "now supports maxResults/nextToken pagination"}
  DeleteEksAnywhereSubscription: {wire: fixed, errors: ok, state: ok, persist: ok}
  UpdateEksAnywhereSubscription: {wire: fixed, errors: ok, state: ok, persist: ok, note: "was PUT; real method is POST to the same leaf path"}
  DescribeInsight: {wire: ok, errors: ok, state: n/a, persist: n/a, note: "content is synthetic/fabricated (pre-existing; AWS's real insight analysis cannot be emulated) but is now reachable at the correct path"}
  ListInsights: {wire: fixed, errors: ok, state: n/a, persist: n/a, note: "was GET; real method is POST (carries an optional filter body) — was unreachable by the real SDK client. Now also reads maxResults/nextToken from the POST body (not query params, since ListInsights carries no query string) and paginates. Was also emitting the FULL Insight shape (recommendation, plus the invented clusterName that neither Insight nor InsightSummary carries on the wire — the cluster is already identified by the URL path); real ListInsights returns types.InsightSummary, which omits recommendation/additionalInfo/categorySpecificSummary/resources entirely -- verified against types.InsightSummary. DescribeInsight's response still includes the invented clusterName (separate pre-existing bug, out of scope for this pass). kubernetesVersion/name (InsightSummary members) have no honest source in this backend's Insight model and are left absent rather than fabricated"}
  StartInsightsRefresh: {wire: fixed, errors: ok, state: n/a, persist: n/a, note: "was routed/shaped as a per-insight, per-refresh-id nested resource (/insights/{id}/refresh); real API is a cluster-level singleton at /clusters/{name}/insights-refresh with no id at all. Response was also wrongly nested under an 'insightsRefresh' envelope key; real fields (message/status/startedAt/endedAt) are at the response root"}
  DescribeInsightsRefresh: {wire: fixed, errors: ok, state: n/a, persist: n/a, note: "same fixes as StartInsightsRefresh"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "extended to find Capability ARNs too"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "genuinely has no maxResults/nextToken in the real API (ListTagsForResourceInput has neither field) -- not a gap"}
  DescribeUpdate: {wire: ok, errors: ok, state: ok, persist: ok}
  ListUpdates: {wire: fixed, errors: ok, state: ok, persist: ok, note: "now supports maxResults/nextToken pagination"}
  CancelUpdate: {wire: fixed, errors: fixed, state: fixed, persist: fixed, note: "implemented for real: POST /clusters/{name}/updates/{updateId}/cancel-update. Real EKS only performs cancellation for VersionRollback update types that are still InProgress (Kubernetes version rollback on EKS Auto Mode clusters, per the op's doc comment); any other type/status now returns InvalidRequestException (new ErrInvalidRequest sentinel) rather than silently no-opping or 404ing. On success sets Status=Cancelled and a Cancellation{Status,Reason} record, matching types.Update.Cancellation/types.Cancellation. No public op creates a VersionRollback update in this SDK version (it is an AWS-internal transition), so the success path is only reachable by seeding an update via the existing exported StoreUpdate — tests exercise this directly"}
gaps:
  - "Capability Configuration remains an untyped passthrough map — no per-CapabilityType (ArgoCd/Ack/Kro) schema validation of Configuration/UpdateCapabilityConfiguration, unlike the real API's discriminated CapabilityConfigurationResponse/UpdateCapabilityConfiguration union types"
  - "Insight/DescribeInsight content is fabricated/synthetic, not derived from real cluster analysis (pre-existing, inherent emulator limitation -- there is no real cluster to analyze)"
  - "types.InsightSummary/types.Insight's kubernetesVersion and name members have no honest source in this backend's Insight model and are left absent from both DescribeInsight and ListInsights rather than fabricated"
  - "DescribeInsight still emits an invented clusterName field that neither types.Insight nor types.InsightSummary carries on the wire (the cluster is already identified by the URL path); ListInsights was fixed to drop it this pass (gopherstack-uult) but DescribeInsight was out of scope for that fix"
  - "ClientRequestToken (CreateCapability, CancelUpdate, CreatePodIdentityAssociation, etc.) is accepted on the wire for shape parity but never used for idempotency dedup, matching this backend's in-memory non-durable nature; a real duplicate-request-with-same-token replay will create two resources instead of returning the first one"
deferred:
  - "AWS error-code granularity beyond ResourceNotFoundException/ResourceInUseException/InvalidParameterValueException/InvalidRequestException (added this pass for CancelUpdate's not-cancellable case) — ClientException/ResourceLimitExceededException/ServerException are not modeled/reachable anywhere in this backend; a full sweep of which ops can plausibly return them was not done this pass"
leaks: {status: clean, note: "worker.Group timers (cluster/nodegroup/fargate/addon CREATING->ACTIVE transitions) stopped via Handler.Shutdown->Backend.Close->work.Stop(); tags.Tags Prometheus-label objects closed on Delete/Reset for every resource type including Capability (closeIDPAndSubscriptionTagsLocked and DeleteCluster's cascade). No new goroutines/tickers introduced this pass -- CancelUpdate and pagination are synchronous request/response paths"}
---

## Notes

**2026-08-13 (gopherstack-jqh2 pass 3):** re-extracted all 65 ops' real
method+path directly from `eks@v1.90.4` serializers.go and drove them
through `ExtractOperation` via the new `handler_sdk_route_table_test.go`
(`TestExtractOperation_SDKRouteTable`, one subtest per op, `t.Parallel()`).
All 65 resolved correctly, including the several same-path/different-method
collisions this service's routing depends on (`/clusters/{name}/updates`
GET/POST, `/clusters/{clusterName}/insights-refresh` GET/POST,
`/clusters/{clusterName}/{access-entries,capabilities,
pod-identity-associations,eks-anywhere-subscriptions}/{id}`
GET/DELETE/POST). No pre-existing table existed to check. This confirms the
extensive 2026-07-12/07-23 route-matcher fixes documented below held under
the strong per-op SDK diff method — no new routing bugs found. This test is
now the permanent regression guard for route-table drift.

Protocol: REST-JSON (restjson1). All wire-shape and route facts in this file were
verified directly against `aws-sdk-go-v2/service/eks@v1.89.0`'s `serializers.go`
(`httpbinding.SplitURI(...)` + `request.Method = "..."` per
`awsRestjson1_serializeOp<Op>.HandleSerialize`) and `deserializers.go`
(`awsRestjson1_deserializeOpDocument<Op>Output` field switch statements), not
against gopherstack's own output — per the parity-principles memory.

### This pass (2026-07-23): gaps/deferred closeout

Starting point was the prior route-matcher/wire-shape audit's 5 gaps + 2
deferred items. All 5 gaps and both deferred items were addressed:

1. **CancelUpdate** — implemented for real (route, backend state transition,
   `Cancellation` record, `InvalidRequestException` for non-cancellable
   updates). See the `CancelUpdate` ops entry above for the full writeup.
2. **Pagination** — every List op that supports `maxResults`/`nextToken` in
   the real API (all of them except the genuinely-unpaginated
   `ListTagsForResource`) now does, via a shared `eksPaginationParams`/
   `eksPageResponse` helper pair in `helpers.go` built on `pkgs/page`.
   `ListInsights` is POST-only or so its pagination params come from the JSON
   body, not query params — handled as a special case.
3. **CreateEksAnywhereSubscription 'term'** — now required and validated
   (`unit` must be `MONTHS`, `duration` must be 12 or 36); the subscription
   record also now models `autoRenew`/`effectiveDate`/`expirationDate`, none
   of which existed before.
4. **Capability Configuration/Health/ModifiedAt/ClientRequestToken** —
   `ModifiedAt` and `Health` (always an empty-issues object, since this
   backend never generates real capability health problems) are now modeled
   and set on create/update. `Configuration` is modeled as an untyped
   passthrough map (see gaps: no per-type schema). `ClientRequestToken` is
   accepted on the wire but not used for idempotency (see gaps).
5. **Insight fabricated content** — left as-is; this is an inherent emulator
   limitation (there is no real EKS control plane to analyze), not something
   fixable by more wire-shape work. Documented as a permanent gap rather than
   silently dropped.

Deferred item 1 (full field-by-field audit of AccessEntry/FargateProfile/
PodIdentityAssociation/IdentityProviderConfig) turned up real wire bugs, not
just missing-but-harmless fields:

- **AccessEntry**: `ModifiedAt` was completely absent (real
  `types.AccessEntry.ModifiedAt`).
- **FargateProfile**: `Health` was completely absent from the wire response
  (real `types.FargateProfile.Health`), not merely unmodeled internally.
- **PodIdentityAssociation**: `ModifiedAt`/`ExternalId`/`Policy`/
  `DisableSessionTags` were absent. More importantly, **ListPodIdentityAssociations
  was emitting the wrong shape entirely** — the full `PodIdentityAssociation`
  object (including `roleArn`/`createdAt`/`tags`) instead of the real API's
  `PodIdentityAssociationSummary`, which deliberately omits those fields.
- **IdentityProviderConfig**: **DescribeIdentityProviderConfig's response
  shape was wrong** — gopherstack returned a flat
  `{clusterName,name,type,status,oidc,createdAt}` object; the real API nests
  everything under `{identityProviderConfig: {oidc: {...}}}` with
  `identityProviderConfigName`/`identityProviderConfigArn`/`clientId`/
  `issuerUrl`/`usernameClaim`/`usernamePrefix`/`groupsClaim`/`groupsPrefix`/
  `requiredClaims`/`tags`/`status` fields inside the `oidc` object. None of
  gopherstack's flat top-level keys matched what a real SDK client expects to
  unmarshal.
- **ListAccessPolicies** (found incidentally while touching this area): each
  entry's ARN field was keyed `"policyArn"`; the real
  `types.AccessPolicy` wire key is `"arn"` (`"policyArn"` is correct for the
  *different* `AssociatedAccessPolicy` type used by
  `ListAssociatedAccessPolicies`, which was already correct).
- **ListCapabilities** (found incidentally): was returning bare capability-name
  strings; the real API returns `CapabilitySummary` objects.

Deferred item 2 (error-code granularity) got a narrow, real fix: `CancelUpdate`
on a non-cancellable update now returns `InvalidRequestException` (new
`ErrInvalidRequest` sentinel, `awserr.ErrConflict`-backed) rather than a
generic `InvalidParameterValueException` or silently succeeding — this
matches the real API's documented "cancellation is only performed if the
update can be cancelled" behavior. Broader error-code coverage
(`ClientException`, `ResourceLimitExceededException`, `ServerException`)
remains deferred; see `deferred:` above.

### Route-matcher bug class (prior pass, 2026-07-12)

This service had a large number of the "whole family unroutable by a real SDK
client" bug class the audit brief called out (the same class that hit
services/backup). Two flavors:

1. **Wrong path.** `pathSubscriptions` was `/subscriptions` (real:
   `/eks-anywhere-subscriptions`), `pathAddonVersions` was `/addon-versions`
   (real: `/addons/supported-versions`), `pathAddonConfiguration` was
   `/addon-configuration` (real: `/addons/configuration-schemas`),
   `RegisterCluster`/`DeregisterCluster` were nested under
   `/clusters/{name}/register` / `/deregister` (real: global
   `/cluster-registrations` POST, `/cluster-registrations/{name}` DELETE — no
   cluster-scoping in the real API since the cluster doesn't exist in EKS yet
   when you register it), `UpdateClusterConfig` was a bare-path PUT on
   `/clusters/{name}` (real: POST `/clusters/{name}/update-config`),
   `UpdateClusterVersion` was posted to a fictional `/update-version` segment
   (real: POST `/clusters/{name}/updates`, the same path `ListUpdates` GETs),
   `UpdateNodegroupConfig`/`UpdateAddon` were missing their real
   `/update-config` / `/update` path suffixes entirely, and `Capability` was a
   fabricated **global** (non-cluster-scoped) resource at `/capabilities` when
   the real API is cluster-scoped at `/clusters/{clusterName}/capabilities`.
   Every one of these was **completely unreachable** by a real
   `aws-sdk-go-v2` client — the SDK would send requests to paths gopherstack's
   `RouteMatcher`/`parseEKSPath` never recognized.

2. **Wrong method.** Every `Update*` op whose real path is shared with its
   sibling `Describe`/`Delete` op (`UpdateAccessEntry`,
   `UpdateEksAnywhereSubscription`, `UpdatePodIdentityAssociation`,
   `UpdateCapability`) is **POST** in the real API, not PUT — gopherstack had
   routed all four as PUT. `ListInsights` is **POST** (it carries an optional
   filter body), not GET — gopherstack required GET.

`StartInsightsRefresh`/`DescribeInsightsRefresh` additionally had a wrong
*shape*: real EKS insights-refresh is a cluster-level singleton (no
per-refresh id at all, `/clusters/{name}/insights-refresh`) with response
fields (`message`, `status`, `startedAt`, `endedAt`) directly at the response
root; gopherstack invented a nested `insights/{id}/refresh[/{refreshId}]`
resource and wrapped the response in a nonexistent `insightsRefresh` envelope
key.

### Traps for the next auditor

- `DescribeIdentityProviderConfig`'s route match is intentionally loose (any
  POST to the 3rd path segment after `identity-provider-configs`, not just
  literal `describe`) — this is over-permissive but harmless since no other
  real op uses that path shape; don't "fix" it without checking test coverage
  first.
- `RegisterCluster`'s cluster name comes from the **request body**, never the
  URL — the handler already read `in.Name` from the body even before this
  pass's fix; only the route *path* was wrong (pointed at a
  `/clusters/{name}/register` shape that made the URL-derived name irrelevant
  anyway).
- `ListAccessPolicies`, `DescribeAddonVersions`, `DescribeAddonConfiguration`,
  `DescribeClusterVersions` are genuinely **global, static** endpoints (no
  `clusterName` in the path) — don't try to cluster-scope them.
- Timestamps are epoch-seconds JSON numbers everywhere (`createdAt`,
  `startedAt`, `endedAt`, etc.) via `.Unix()`, matching the SDK's
  `smithytime.ParseEpochSeconds` deserializers — already correct throughout,
  not something this pass needed to touch.
- `CancelUpdate`'s success path (VersionRollback + InProgress) cannot be
  reached through any other public op in this SDK version — AWS generates
  VersionRollback updates internally when a node rollback is triggered on an
  Auto Mode cluster, not via a documented Create/StartRollback op. Tests seed
  it directly via `Handler.Backend.StoreUpdate`.
- `page.New`'s `nextToken` is the empty string (omitted from the JSON
  response body via `eksPageResponse`) once a List's results are exhausted —
  don't expect a literal `null` in the map like the real SDK's Go struct
  (`*string` marshals to `null`); gopherstack's map-based JSON just omits the
  key, which decodes identically on the client side (`NextToken` stays nil).

## gopherstack-muzq (2026-08-21): resources stuck in a transitional status forever

Continues gopherstack-oc9v/gopherstack-muzq's cross-service sweep for resources
stamped with a transitional status at construction that nothing in the backend
ever advances -- a client polling for readiness never exits its loop, even
though the emulator answers 200 with a well-formed body every time.

**Two confirmed instances in this service, both fixed:**

- `AssociateIdentityProviderConfig` stamped `IdentityProviderConfig.Status` as
  `CREATING` and nothing else in the backend ever wrote to that field --
  `clusters.go`/`addons.go`/`node_groups.go`'s sibling `CREATING`→`ACTIVE`
  transitions (via `b.work.After` + a `*TransitionDelay` constant) are the
  correct pattern in this exact service; `identity_providers.go` alone never
  had one. Fixed by adding `identityProviderTransitionDelay` and scheduling the
  transition the same way, right after `Put`. The existing
  `TestIDPConfigCreatesAsCreating` asserted `CREATING` immediately after
  associate -- true, but it never checked the status ever changed -- and was
  strengthened with a `require.Eventually` block confirming `ACTIVE`.
- `UpdateClusterConfig`/`UpdateClusterVersion`/`UpdateNodegroupVersion`, plus
  the node-group-config handler's inline `Update` construction, all stamped
  the returned `Update.Status` as `InProgress` and nothing ever advanced it --
  the only other writer of an `Update`'s `Status` is `CancelUpdate`, which only
  handles `VersionRollback`-type updates and only transitions to `Cancelled`.
  A real client's `DescribeUpdate` waiter (the standard EKS "did my update
  finish" poll) never saw a terminal status. Fixed via a new
  `scheduleUpdateTransition` helper (mirroring `scheduleClusterActivation`)
  called from all four sites, advancing `InProgress`→`Successful`.
  `TestDescribeUpdate_Status_Successful` is a striking case of this bug class:
  the test is *named* after the terminal state but its body asserted
  `InProgress` and stopped there. Also strengthened:
  `TestUpdateClusterConfig_Status_InProgress`,
  `TestUpdateNodegroupVersion_Status_InProgress`; added
  `TestNodegroup_UpdateConfig_UpdateRecordReachesSuccessful` for the handler
  path, which had no status test at all.

Both fixes reuse `b.work` (`*worker.Group`, already present on
`InMemoryBackend` and used by every sibling `*TransitionDelay` mechanism in
this service) -- no new async infrastructure was introduced.

Cleared (real advancing path found, not a bug): `CreateCluster`/`CreateAddon`/
`CreateFargateProfile`/`CreateNodegroup` all correctly schedule their own
`CREATING`→`ACTIVE` transitions already. `UpdateClusterVpcEndpoint` sets
`Successful` synchronously at creation (no async work exists for a pure
config-flag flip) -- correct as-is, not a bug.

Verified by hand-revert: each fix's file was reverted to its pre-fix `git
show HEAD:<path>` content, the corresponding test failed with the predicted
symptom (`Condition never satisfied` -- the status stayed transitional), then
was restored and confirmed `md5sum`-identical to the fixed version.
