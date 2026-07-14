---
# PARITY MANIFEST SCHEMA — see services/_PARITY_TEMPLATE.md for the schema doc.
service: eks
sdk_module: aws-sdk-go-v2/service/eks@v1.89.0
last_audit_commit: 7c297a53
last_audit_date: 2026-07-12
overall: A            # major route-matcher + wire-shape fixes found (fresh audit, no prior PARITY.md)
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  ListClusters: {wire: ok, errors: ok, state: ok, persist: ok, note: "no maxResults/nextToken pagination — returns full list (pre-existing, not fixed this pass)"}
  DeleteCluster: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateClusterConfig: {wire: fixed, errors: ok, state: ok, persist: ok, note: "was routed as bare-path PUT /clusters/{name}; real path is POST /clusters/{name}/update-config"}
  UpdateClusterVersion: {wire: fixed, errors: ok, state: ok, persist: ok, note: "was routed at fictional POST /clusters/{name}/update-version; real path is POST /clusters/{name}/updates (shared with ListUpdates GET)"}
  RegisterCluster: {wire: fixed, errors: ok, state: ok, persist: ok, note: "was routed at /clusters/{placeholder}/register; real path is global POST /cluster-registrations (name comes from body, always did)"}
  DeregisterCluster: {wire: fixed, errors: ok, state: ok, persist: ok, note: "was routed as POST /clusters/{name}/deregister; real path is DELETE /cluster-registrations/{name}"}
  DescribeClusterVersions: {wire: ok, errors: ok, state: ok, persist: n/a}
  AssociateEncryptionConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateNodegroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeNodegroup: {wire: ok, errors: ok, state: ok, persist: ok}
  ListNodegroups: {wire: ok, errors: ok, state: ok, persist: ok, note: "no pagination"}
  DeleteNodegroup: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateNodegroupConfig: {wire: fixed, errors: ok, state: ok, persist: ok, note: "was reachable on a bare POST to the nodegroup path with no suffix check, so real SDK traffic to .../update-config fell through with a corrupted nodegroupName (the literal suffix baked in); now requires the real /update-config suffix"}
  UpdateNodegroupVersion: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateAddon: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAddon: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAddons: {wire: ok, errors: ok, state: ok, persist: ok, note: "no pagination"}
  DeleteAddon: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAddon: {wire: fixed, errors: ok, state: ok, persist: ok, note: "was PUT to bare addon path; real op is POST .../addons/{addonName}/update"}
  DescribeAddonVersions: {wire: fixed, errors: ok, state: n/a, persist: n/a, note: "path was /addon-versions; real path is /addons/supported-versions — was completely unreachable by the real SDK client"}
  DescribeAddonConfiguration: {wire: fixed, errors: ok, state: n/a, persist: n/a, note: "path was /addon-configuration; real path is /addons/configuration-schemas — was completely unreachable"}
  CreateAccessEntry: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeAccessEntry: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAccessEntries: {wire: ok, errors: ok, state: ok, persist: ok, note: "no pagination"}
  DeleteAccessEntry: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAccessEntry: {wire: fixed, errors: ok, state: ok, persist: ok, note: "was routed as PUT; real method is POST to the same leaf path"}
  AssociateAccessPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateAccessPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAssociatedAccessPolicies: {wire: ok, errors: ok, state: ok, persist: n/a}
  ListAccessPolicies: {wire: ok, errors: ok, state: n/a, persist: n/a}
  CreateFargateProfile: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeFargateProfile: {wire: ok, errors: ok, state: ok, persist: ok}
  ListFargateProfiles: {wire: ok, errors: ok, state: ok, persist: ok, note: "no pagination; no UpdateFargateProfile exists in the real API either"}
  DeleteFargateProfile: {wire: ok, errors: ok, state: ok, persist: ok}
  CreatePodIdentityAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribePodIdentityAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPodIdentityAssociations: {wire: ok, errors: ok, state: ok, persist: ok, note: "no pagination"}
  DeletePodIdentityAssociation: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdatePodIdentityAssociation: {wire: fixed, errors: ok, state: ok, persist: ok, note: "was routed as PUT; real method is POST to the same leaf path"}
  AssociateIdentityProviderConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeIdentityProviderConfig: {wire: ok, errors: ok, state: ok, persist: n/a, note: "matcher accepts any 3rd path segment as POST->Describe rather than requiring literal 'describe'; over-permissive but not wrong for real traffic, left as-is"}
  ListIdentityProviderConfigs: {wire: ok, errors: ok, state: ok, persist: ok, note: "no pagination"}
  DisassociateIdentityProviderConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateCapability: {wire: fixed, errors: fixed, state: fixed, persist: fixed, note: "was a GLOBAL (non-cluster-scoped) resource keyed only by 'name' at POST /capabilities, which does not exist in the real API at all (fabricated path). Real Capability is cluster-scoped (unique CapabilityName per cluster) at /clusters/{clusterName}/capabilities and requires ClusterName+CapabilityName+Type+RoleArn+DeletePropagationPolicy. Rebuilt: composite-keyed store (capabilityKey), cluster-scoped route, required-field validation, capabilityName/clusterName/arn/type/roleArn/deletePropagationPolicy/createdAt/tags on the wire (was emitting only name/version/status under the wrong field name 'name' instead of 'capabilityName')."}
  DescribeCapability: {wire: fixed, errors: fixed, state: fixed, persist: fixed}
  ListCapabilities: {wire: fixed, errors: fixed, state: fixed, persist: fixed}
  DeleteCapability: {wire: fixed, errors: fixed, state: fixed, persist: fixed}
  UpdateCapability: {wire: fixed, errors: fixed, state: fixed, persist: fixed, note: "was PUT; real method is POST to the same leaf path. Configuration/ModifiedAt/Health/ClientRequestToken not modeled (gap)"}
  CreateEksAnywhereSubscription: {wire: fixed, errors: ok, state: ok, persist: ok, note: "path was /subscriptions; real path is /eks-anywhere-subscriptions — was completely unreachable. Does not validate the required 'term' field (gap, pre-existing)"}
  DescribeEksAnywhereSubscription: {wire: fixed, errors: ok, state: ok, persist: ok}
  ListEksAnywhereSubscriptions: {wire: fixed, errors: ok, state: ok, persist: ok, note: "no pagination"}
  DeleteEksAnywhereSubscription: {wire: fixed, errors: ok, state: ok, persist: ok}
  UpdateEksAnywhereSubscription: {wire: fixed, errors: ok, state: ok, persist: ok, note: "was PUT; real method is POST to the same leaf path"}
  DescribeInsight: {wire: ok, errors: ok, state: n/a, persist: n/a, note: "content is synthetic/fabricated (pre-existing; AWS's real insight analysis cannot be emulated) but is now reachable at the correct path"}
  ListInsights: {wire: fixed, errors: ok, state: n/a, persist: n/a, note: "was GET; real method is POST (carries an optional filter body) — was unreachable by the real SDK client"}
  StartInsightsRefresh: {wire: fixed, errors: ok, state: n/a, persist: n/a, note: "was routed/shaped as a per-insight, per-refresh-id nested resource (/insights/{id}/refresh); real API is a cluster-level singleton at /clusters/{name}/insights-refresh with no id at all. Response was also wrongly nested under an 'insightsRefresh' envelope key; real fields (message/status/startedAt/endedAt) are at the response root"}
  DescribeInsightsRefresh: {wire: fixed, errors: ok, state: n/a, persist: n/a, note: "same fixes as StartInsightsRefresh"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "extended to find Capability ARNs too"}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeUpdate: {wire: ok, errors: ok, state: ok, persist: ok}
  ListUpdates: {wire: ok, errors: ok, state: ok, persist: ok, note: "no pagination"}
  CancelUpdate: {wire: gap, errors: gap, state: gap, persist: gap, note: "not implemented at all — already tracked in sdk_completeness_test.go's notImplemented list (bd: gopherstack-nab); left untouched, outside this pass's route-matcher/wire-shape scope"}
gaps:
  - "CancelUpdate op entirely unimplemented (bd: gopherstack-nab, already tracked pre-existing)"
  - "No List* op implements maxResults/nextToken pagination (ListClusters, ListNodegroups, ListAddons, ListFargateProfiles, ListPodIdentityAssociations, ListIdentityProviderConfigs, ListEksAnywhereSubscriptions, ListUpdates, ListAccessEntries all return the full set in one page) — needs a follow-up bd issue"
  - "CreateEksAnywhereSubscription does not validate the required 'term' input field"
  - "Capability Configuration/Health/ModifiedAt/ClientRequestToken (idempotency) not modeled — only CapabilityName/ClusterName/ARN/Type/RoleArn/DeletePropagationPolicy/Status/CreatedAt/Tags are real"
  - "Insight/DescribeInsight content is fabricated/synthetic, not derived from real cluster analysis (pre-existing, inherent emulator limitation)"
deferred:
  - "Full field-by-field wire audit of AccessEntry/FargateProfile/PodIdentityAssociation/IdentityProviderConfig response bodies beyond envelope key + top-level shape (route correctness was verified for all of these; deep field audit not done this pass)"
  - "AWS error-code granularity beyond the three sentinel mappings (ErrNotFound->ResourceNotFoundException, ErrAlreadyExists->ResourceInUseException, ErrValidation->InvalidParameterValueException) — some real AWS EKS error paths return more specific codes (e.g. InvalidRequestException, ClientException) not modeled here"
leaks: {status: clean, note: "worker.Group timers (cluster/nodegroup/fargate/addon CREATING->ACTIVE transitions) stopped via Handler.Shutdown->Backend.Close->work.Stop(); tags.Tags Prometheus-label objects closed on Delete/Reset for every resource type including the newly cluster-scoped Capability (added Capability to closeIDPAndSubscriptionTagsLocked and to DeleteCluster's cascade)"}
---

## Notes

Protocol: REST-JSON (restjson1). All wire-shape and route facts in this file were
verified directly against `aws-sdk-go-v2/service/eks@v1.89.0`'s `serializers.go`
(`httpbinding.SplitURI(...)` + `request.Method = "..."` per
`awsRestjson1_serializeOp<Op>.HandleSerialize`) and `deserializers.go`
(`awsRestjson1_deserializeOpDocument<Op>Output` field switch statements), not
against gopherstack's own output — per the parity-principles memory.

### Route-matcher bug class (the dominant finding this pass)

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
