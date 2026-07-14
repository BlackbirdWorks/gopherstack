---
service: cloudfront
sdk_module: aws-sdk-go-v2/service/cloudfront@v1.60.2
last_audit_commit: a8c6614b
last_audit_date: 2026-07-12
overall: A            # re-audit: zero drift in services/cloudfront/ since ce30166a
                       # (previous sweep's baseline) and identical pinned SDK version
                       # (v1.60.2) -- no new/changed surface to audit. All ok rows
                       # trusted per re-audit protocol. Spot-checked InUse-guard gap
                       # (OAI/OAC/KeyGroup delete, gopherstack-na4) still accurately
                       # documented, not silently regressed or silently fixed.
                       # go build/vet/test -race/fix -diff/golangci-lint all pass clean.
ops:
  CreateDistribution: {wire: ok, errors: ok, state: ok, persist: ok, note: "now runs validateQuantities on the raw config"}
  CreateDistributionWithTags: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDistribution: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDistributionConfig: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateDistribution: {wire: ok, errors: ok, state: ok, persist: ok, note: "If-Match/ETag enforced; validateQuantities added"}
  DeleteDistribution: {wire: ok, errors: ok, state: ok, persist: ok, note: "If-Match enforced; DistributionNotDisabled enforced"}
  ListDistributions: {wire: ok, errors: ok, state: ok, persist: ok}
  CopyDistribution: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateInvalidation: {wire: ok, errors: ok, state: ok, persist: ok, note: "validateQuantities added for Paths; background reconciler transitions InProgress->Completed"}
  GetInvalidation: {wire: ok, errors: ok, state: ok, persist: ok}
  ListInvalidations: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateCachePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "now returns CachePolicyAlreadyExists (was DistributionAlreadyExists); validateQuantities added"}
  UpdateCachePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "If-Match enforced; CachePolicyAlreadyExists; validateQuantities added"}
  DeleteCachePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW: CachePolicyInUse guard via distribution config token index"}
  GetCachePolicy / GetCachePolicyConfig / ListCachePolicies: {wire: ok, errors: ok, state: ok, persist: ok, note: "no managed-vs-custom Type support -- gap filed"}
  CreateOriginRequestPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "now returns OriginRequestPolicyAlreadyExists; validateQuantities added"}
  UpdateOriginRequestPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "same as above"}
  DeleteOriginRequestPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW: OriginRequestPolicyInUse guard"}
  GetOriginRequestPolicy / GetOriginRequestPolicyConfig / ListOriginRequestPolicies: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateResponseHeadersPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "now returns ResponseHeadersPolicyAlreadyExists; validateQuantities added"}
  UpdateResponseHeadersPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "same as above"}
  DeleteResponseHeadersPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW: ResponseHeadersPolicyInUse guard"}
  GetResponseHeadersPolicy / GetResponseHeadersPolicyConfig / ListResponseHeadersPolicies: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateOriginAccessControl: {wire: ok, errors: ok, state: ok, persist: ok, note: "now returns OriginAccessControlAlreadyExists; validateQuantities added"}
  UpdateOriginAccessControl: {wire: ok, errors: ok, state: ok, persist: ok, note: "same as above"}
  DeleteOriginAccessControl: {wire: ok, errors: ok, state: ok, persist: ok, note: "no InUse guard yet -- gap filed (gopherstack-na4)"}
  GetOriginAccessControl / GetOriginAccessControlConfig / ListOriginAccessControls: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateCloudFrontOriginAccessIdentity: {wire: ok, errors: ok, state: ok, persist: ok, note: "validateQuantities added (harmless no-op for this shape)"}
  UpdateCloudFrontOriginAccessIdentity: {wire: ok, errors: ok, state: ok, persist: ok, note: "If-Match enforced"}
  DeleteCloudFrontOriginAccessIdentity: {wire: ok, errors: ok, state: ok, persist: ok, note: "If-Match enforced; no InUse guard yet -- gap filed"}
  GetCloudFrontOriginAccessIdentity / Config / List: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateFunction: {wire: fixed, errors: ok, state: ok, persist: ok, note: "FIXED this pass: response was missing required FunctionMetadata.FunctionARN/CreatedTime/LastModifiedTime; now returns FunctionAlreadyExists (was DistributionAlreadyExists); validateQuantities added"}
  UpdateFunction: {wire: fixed, errors: ok, state: ok, persist: ok, note: "same wire fix; If-Match enforced; validateQuantities added"}
  PublishFunction: {wire: fixed, errors: ok, state: ok, persist: ok, note: "same wire fix; If-Match enforced; LastModifiedTime now bumped"}
  DeleteFunction: {wire: ok, errors: ok, state: ok, persist: ok, note: "NEW: FunctionInUse guard (keyed by FunctionARN, not name)"}
  GetFunction / DescribeFunction / ListFunctions / TestFunction: {wire: fixed, errors: ok, state: ok, persist: ok, note: "GetFunction/DescribeFunction/ListFunctions share the same FunctionMetadata fix"}
  TagResource / UntagResource / ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
  AssociateAlias / AssociateDistributionWebACL / AssociateDistributionTenantWebACL: {wire: ok, errors: ok, state: ok, persist: ok, families: cross-service}
families:
  distribution_tenants_connection_groups: {status: ok, note: "CreateDistributionTenant/UpdateDistributionTenant now run validateQuantities; If-Match enforced on update/delete; audited, no new findings beyond the Quantity gap"}
  field_level_encryption: {status: ok, note: "Create/Update for config + profile now run validateQuantities and return the correct *AlreadyExists code (FieldLevelEncryptionConfigAlreadyExists / FieldLevelEncryptionProfileAlreadyExists) instead of DistributionAlreadyExists; FLEProfileInUse guard on profile delete pre-existed and is correct"}
  public_keys_key_groups: {status: ok, note: "CreatePublicKey/CreateKeyGroup/UpdateKeyGroup now return PublicKeyAlreadyExists/KeyGroupAlreadyExists instead of DistributionAlreadyExists; PublicKeyInUse guard on public-key delete pre-existed and is correct; KeyGroup delete still has no InUse guard -- gap filed (gopherstack-na4)"}
  realtime_log_configs: {status: ok, note: "CreateRealtimeLogConfig now returns RealtimeLogConfigAlreadyExists instead of DistributionAlreadyExists"}
  key_value_stores: {status: ok, note: "control-plane Create/Update run validateQuantities (no-op, shape has no Quantity/Items pairs); data-plane GetKey/PutKeys/ListKeys correctly use the separate JSON protocol, out of scope for this XML-focused sweep"}
  vpc_origins: {status: ok, note: "Create/Update run validateQuantities (no-op for this shape)"}
  continuous_deployment_policy: {status: ok, note: "Create/Update run validateQuantities; If-Match already enforced"}
  invalidations_realtime_status: {status: ok, note: "background reconciler goroutine (runInvalidationReconciler) has a clean stopCh lifecycle via Close(); no leak"}
  monitoring_subscriptions_public_resource_policy_connection_groups: {status: ok, note: "audited via handler_new_ops.go/handler_batch2.go dispatch; no Quantity/AlreadyExists-code issues found in these shapes"}
gaps:
  - "Managed (AWS-provided) cache/origin-request/response-headers policies are not seeded, and List* does not support the Type=managed|custom filter (bd: gopherstack-a9t)"
  - "DeleteKeyGroup / DeleteCloudFrontOriginAccessIdentity / DeleteOriginAccessControl have no InUse-on-delete guard, unlike the CachePolicy/OriginRequestPolicy/ResponseHeadersPolicy/Function/PublicKey/FLEProfile guards (bd: gopherstack-na4)"
  - "CreateDistribution (and likely CreateOAI/CreateStreamingDistribution) treat CallerReference reuse as unconditionally idempotent; real AWS returns *AlreadyExists when the reused CallerReference's config content differs (bd: gopherstack-mzx)"
deferred:
  - "Distribution status InProgress->Deployed transition timer (currently InProgress persists indefinitely; no test depends on the transition, scope excluded per task's op-by-op priority list)"
  - "KeyValueStore data-plane (GetKey/PutKeys/ListKeys, separate JSON protocol) -- explicitly out of scope for this REST-XML-focused sweep"
  - "Full per-op audit of DistributionConfig nested shape correctness (Origins/OriginGroups/CacheBehaviors/ViewerCertificate/Restrictions field-by-field) beyond the Quantity/Items validation and the pre-existing minimal-parse model; RawConfig storage design predates this pass and was not restructured"
leaks: {status: clean, note: "runInvalidationReconciler goroutine has a proper stopCh + Close() lifecycle; no unbounded maps found; no new goroutines introduced this pass"}
---

## Notes

**ETag/IfMatch** (proven, not touched this pass): Update/Delete for Distribution, CachePolicy,
OriginRequestPolicy, ResponseHeadersPolicy, OriginAccessControl, OAI, CloudFront Function,
ContinuousDeploymentPolicy, and DistributionTenant all require an `If-Match` header equal to
the resource's current ETag, else `412 PreconditionFailed`. This was already correct across
the board before this sweep; verified op-by-op, no gaps found.

**InconsistentQuantities (the headline fix this pass)**: CloudFront's wire format pairs a
caller-supplied `<Quantity>N</Quantity>` with an `<Items>...</Items>` list virtually
everywhere in the schema (57 distinct SDK types carry a `Quantity *int32` field). Real
AWS rejects a request where `N` disagrees with the actual number of items with
`InconsistentQuantities` (400). Before this pass, the emulator had **zero** occurrences of
this validation anywhere in the codebase -- `grep -rn InconsistentQuantities` was empty.
Root cause: `DistributionConfig` (and most other configs) is parsed into either a minimal
typed struct or stored as opaque `RawConfig` bytes; nothing ever re-derived the caller's
stated `Quantity` and compared it to the real list length, because Go slices don't need an
explicit count. Fix: `services/cloudfront/quantity_validation.go` adds a generic recursive
XML-tree walker (`validateQuantities`) that finds every `<X><Quantity>..</Quantity>
<Items>..</Items></X>` pairing in an arbitrary config body and flags a mismatch --
no per-resource schema modeling required, and provably safe against false positives
because it only fires when both `Quantity` and `Items` siblings are actually present
(verified against `KeyGroupConfig`/`PublicKeyConfig`/`RealtimeLogConfig`/`VpcOriginConfig`,
none of which use this pattern in the real SDK, via the smithy serializers). Wired into
all ~58 Create/Update body-parsing call sites across `handler.go`, `handler_batch2.go`,
and `handler_new_ops.go`.

**AlreadyExists error codes were all wrong (second major finding)**: `handleError`'s
`ErrAlreadyExists` sentinel had `code = "DistributionAlreadyExists"` and was reused
verbatim for CachePolicy, OriginRequestPolicy, ResponseHeadersPolicy, OriginAccessControl,
CloudFront Function, FieldLevelEncryptionConfig, FieldLevelEncryptionProfile, PublicKey,
KeyGroup, and RealtimeLogConfig name/CallerReference collisions -- i.e. creating a second
cache policy with a taken name returned the literal string `DistributionAlreadyExists`,
which is CloudFront's *distribution*-specific error code and was never even triggered by
an actual distribution collision (`CreateDistribution` doesn't use this sentinel at all;
it's fully idempotent on CallerReference, see gap above). Two existing tests
(`TestRefinement1_CachePolicyUniqueness`, `TestRefinement1_ErrorMapping`) asserted this
wrong code as if it were correct -- both fixed with justification comments pointing at the
real `aws-sdk-go-v2/service/cloudfront/types` error type names. Fix: 11 new distinct
sentinel errors (one per resource, matching the real SDK's dedicated error type where one
exists, falling back to the real generic `EntityAlreadyExists` where the SDK has no
resource-specific type -- e.g. Anycast IP lists, key value stores, trust stores). The
`handleError` switch (which had grown to cyclomatic complexity 23) was refactored into a
data-driven `errCodeMapping` table (pattern already established by EC2's `errCodeLookup`),
fixing a `cyclop` lint violation as a side effect.

**Function responses were missing FunctionARN/CreatedTime/LastModifiedTime (third
finding)**: `FunctionMetadata` requires `FunctionARN` and `LastModifiedTime` per the real
SDK (`CreatedTime`/`Stage` too). The emulator's `Function` backend struct *did* compute
and store an ARN (`b.functionARN(name)`) on create, but `functionResponseXML` (shared by
Create/Get/Describe/Publish/Update) and the inline `FunctionSummary` builder in
`handleListFunctions` never emitted it -- a real SDK caller had no way to get a function's
ARN back from any read operation, which makes attaching the function to a distribution's
`FunctionAssociations` (which require the ARN, not the name) impossible. Fixed by adding
`CreatedTime`/`LastModifiedTime` fields to `Function`, populating them on
Create/Update/Publish, and emitting all four `FunctionMetadata` fields from both XML
builders.

**InUse-on-delete guards (fourth finding)**: `DeleteCachePolicy`, `DeleteOriginRequestPolicy`,
`DeleteResponseHeadersPolicy`, and `DeleteFunction` had **no** check for whether the
resource was still referenced by a distribution -- real AWS returns `CachePolicyInUse` /
`OriginRequestPolicyInUse` / `ResponseHeadersPolicyInUse` / `FunctionInUse` (409) in that
case. (`PublicKeyInUse` and `FieldLevelEncryptionProfileInUse` already existed and are
correct -- not touched.) Fixed by adding `tokenReferencedByAnyDistribution` to
`backend_search_index.go`, reusing the pre-existing inverted token index that already
backs `ListDistributionsByCachePolicyID` etc. (built for the `ListDistributionsBy*`
control-plane ops) -- an O(1) check with no new scanning logic. `KeyGroup`, `OAI`, and
`OriginAccessControl` still lack this guard; deferred as gopherstack-na4 because OAI's
reference token has a different shape (`origin-access-identity/cloudfront/{id}` path
string, not a bare ID) and OAC has no existing `ListDistributionsBy*` helper to build on,
so both need slightly more care than a drop-in reuse.

**InconsistentQuantities trap for the next auditor**: don't add per-resource Quantity
validation by hand if you find a new Create/Update body-parsing handler missing the
`validateQuantities(body)` call -- just add the one-line call. The generic walker already
covers any shape with a `<Quantity>`/`<Items>` sibling pair; it is a no-op (returns nil)
for shapes that don't use the pattern, so it is always safe to add defensively.

**"Looks wrong but is correct" traps**:
- `ErrKeyGroupNotFound`'s wire code is `NoSuchResource`, not `NoSuchKeyGroup` -- this
  matches the real SDK (`types.NoSuchResource` is what CloudFront actually returns for a
  missing key group; there is no dedicated `NoSuchKeyGroup` type). Don't "fix" this.
- `ErrKeyValueStoreNotFound`/the new fallback `ErrAlreadyExists` both use `EntityNotFound`/
  `EntityAlreadyExists` -- also correct; the real SDK has no KVS-specific *NotFound/
  *AlreadyExists type either.
- `CreateAnycastIPList`/`CreateKeyValueStore`/`CreateTrustStore` intentionally still use the
  generic `ErrAlreadyExists` (now `EntityAlreadyExists`) sentinel rather than a dedicated
  one -- there is no `AnycastIpListAlreadyExists`/`KeyValueStoreAlreadyExists` type in
  `aws-sdk-go-v2/service/cloudfront/types@v1.60.2` to match; this is the AWS-accurate
  fallback, not an oversight.

**Protocol**: REST-XML throughout (control plane). KeyValueStore's data-plane
(`handler_audit.go`, `GetKey`/`ListKeys`/`UpdateKeys`) correctly uses a separate JSON
protocol matching the real `cloudfront-keyvaluestore` service -- do not "fix" it to XML.
