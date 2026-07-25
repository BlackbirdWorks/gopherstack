---
service: ssoadmin
sdk_module: aws-sdk-go-v2/service/ssoadmin@v1.38.0
last_audit_commit: HEAD
last_audit_date: 2026-07-24
overall: A            # multiple severe client-breaking wire-shape bugs found and fixed this sweep
ops:
  # --- fixed this sweep ---
  CreateAccountAssignment: {wire: ok, errors: ok, state: ok, persist: ok, note: "AccountAssignmentCreationStatus used invented 'AccountId' field; real AccountAssignmentOperationStatus uses 'TargetId'/'TargetType'. A real client previously got nil TargetId. Fixed."}
  DeleteAccountAssignment: {wire: ok, errors: ok, state: ok, persist: ok, note: "same AccountAssignmentDeletionStatus TargetId/TargetType fix as CreateAccountAssignment"}
  DescribeAccountAssignmentCreationStatus: {wire: ok, errors: ok, state: ok, persist: ok, note: "same TargetId/TargetType fix"}
  DescribeAccountAssignmentDeletionStatus: {wire: ok, errors: ok, state: ok, persist: ok, note: "same TargetId/TargetType fix"}
  ListAccountAssignmentCreationStatus: {wire: ok, errors: ok, state: ok, persist: ok, note: "was unpaginated (MaxResults ignored, NextToken always nil) and returned the full singular-status shape instead of the real slim AccountAssignmentOperationStatusMetadata (CreatedDate/RequestId/Status only). Both fixed; pagination preserves the backend's CreatedDate-descending order via new paginateOrdered helper (does not re-sort like paginateBy)."}
  ListAccountAssignmentDeletionStatus: {wire: ok, errors: ok, state: ok, persist: ok, note: "same slim-metadata-shape + pagination fix as ListAccountAssignmentCreationStatus"}
  ListAccountAssignmentsForPrincipal: {wire: ok, errors: ok, state: ok, persist: ok, note: "Filter.AccountId and MaxResults/NextToken pagination were both ignored (real op supports both); now implemented"}
  ProvisionPermissionSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "PermissionSetProvisioningStatus shape confirmed correct (uses AccountId, unlike AccountAssignmentOperationStatus -- these two 'status' shapes diverge on the real API and had been conflated into one Go view type)"}
  DescribePermissionSetProvisioningStatus: {wire: ok, errors: ok, state: ok, persist: ok, note: "same shape confirmed correct"}
  ListPermissionSetProvisioningStatus: {wire: ok, errors: ok, state: ok, persist: ok, note: "was unpaginated and returned the full status shape instead of the real slim PermissionSetProvisioningStatusMetadata (CreatedDate/RequestId/Status only); fixed with paginateOrdered (preserves CreatedDate-descending order)"}
  ListPermissionSetsProvisionedToAccount: {wire: ok, errors: ok, state: ok, persist: ok, note: "MaxResults/NextToken were ignored; now paginated"}
  ListAccountsForProvisionedPermissionSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "MaxResults/NextToken were ignored; now paginated"}
  CreateApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "SEVERE: response wrapped the full application under an invented 'Application' object; real CreateApplicationOutput is exactly {ApplicationArn, IdentityStoreArn, InstanceArn} flat, and IdentityStoreArn was never returned. Fixed; backend now derives ApplicationAccount/CreatedFrom/IdentityStoreArn."}
  DescribeApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "SEVERE: entire response was nested one level too deep under an invented 'Application' wrapper (plus a fabricated 'Tags' member) -- a real aws-sdk-go-v2 client parsing this would get every DescribeApplicationOutput field nil. Real shape is flat: ApplicationAccount/ApplicationArn/ApplicationProviderArn/CreatedDate/CreatedFrom/Description/IdentityStoreArn/InstanceArn/Name/PortalOptions/Status, no Tags. Fixed; tags now only reachable via ListTagsForResource like every other taggable resource."}
  UpdateApplication: {wire: ok, errors: ok, state: ok, persist: ok, note: "response echoed a full invented 'Application' object; real UpdateApplicationOutput is void. Fixed to {}."}
  ListApplications: {wire: ok, errors: ok, state: ok, persist: ok, note: "was missing ApplicationAccount/CreatedFrom/IdentityStoreArn (present on the real per-item Application type) and MaxResults/NextToken pagination; both fixed"}
  DescribeApplicationAssignment: {wire: ok, errors: ok, state: ok, persist: ok, note: "SEVERE: response nested under an invented 'ApplicationAssignment' wrapper; real DescribeApplicationAssignmentOutput is flat {ApplicationArn, PrincipalId, PrincipalType}. Fixed."}
  ListApplicationAssignments: {wire: ok, errors: ok, state: ok, persist: ok, note: "MaxResults/NextToken were ignored; now paginated"}
  ListApplicationAssignmentsForPrincipal: {wire: ok, errors: ok, state: ok, persist: ok, note: "Filter.ApplicationArn and MaxResults/NextToken pagination were both ignored; now implemented"}
  PutApplicationAssignmentConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "request accepted an invented 'AssignmentRequiredForAllIdentities' field with no counterpart on the real PutApplicationAssignmentConfigurationInput (exactly {ApplicationArn, AssignmentRequired}); removed"}
  PutApplicationSessionConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "SEVERE: entire operation modeled a fabricated 'SessionDuration' concept that does not exist on the real API at all. Real Put/GetApplicationSessionConfiguration operate on 'UserBackgroundSessionApplicationStatus' (ENABLED/DISABLED), confused here with the unrelated PermissionSet.SessionDuration field. Fixed end-to-end (backend, interface, handlers, validation)."}
  GetApplicationSessionConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "same UserBackgroundSessionApplicationStatus fix; also removed an invented nested 'ApplicationSessionConfiguration' wrapper -- real GetApplicationSessionConfigurationOutput is flat"}
  ListApplicationProviders: {wire: ok, errors: ok, state: ok, persist: ok, note: "FederationProtocol was populated on every seeded catalog entry but silently dropped on the wire; also MaxResults/NextToken were ignored. Both fixed."}
  DescribeApplicationProvider: {wire: ok, errors: ok, state: ok, persist: ok, note: "same FederationProtocol drop bug as ListApplicationProviders; fixed"}
  ListApplicationAccessScopes: {wire: ok, errors: ok, state: ok, persist: ok, note: "MaxResults/NextToken were ignored; now paginated"}
  GetApplicationAuthenticationMethod: {wire: ok, errors: ok, state: ok, persist: ok, note: "SEVERE: response double-wrapped the stored union body under an extra invented 'AuthenticationMethodType'/'AuthenticationMethod' pair one level too deep. Real GetApplicationAuthenticationMethodOutput is exactly {AuthenticationMethod: <union>} with NO sibling AuthenticationMethodType (unlike AuthenticationMethodItem, the ListApplicationAuthenticationMethods item shape, which legitimately has both as siblings). A real client's union deserializer would never find the 'Iam' tag through the old double-wrap. Fixed."}
  GetApplicationGrant: {wire: ok, errors: ok, state: ok, persist: ok, note: "SEVERE: same double-wrap bug as GetApplicationAuthenticationMethod -- real GetApplicationGrantOutput is exactly {Grant: <union>}, no sibling GrantType (unlike GrantItem, the ListApplicationGrants item shape). Fixed."}
  CreateTrustedTokenIssuer: {wire: ok, errors: ok, state: ok, persist: ok, note: "confirmed already correct: flat {TrustedTokenIssuerArn}"}
  DescribeTrustedTokenIssuer: {wire: ok, errors: ok, state: ok, persist: ok, note: "SEVERE: response nested under an invented 'TrustedTokenIssuer' wrapper with fabricated InstanceArn and Tags members; real DescribeTrustedTokenIssuerOutput is flat {Name, TrustedTokenIssuerArn, TrustedTokenIssuerConfiguration, TrustedTokenIssuerType}, no InstanceArn, no Tags. Fixed; tags now only reachable via ListTagsForResource."}
  UpdateTrustedTokenIssuer: {wire: ok, errors: ok, state: ok, persist: ok, note: "response echoed a full invented 'TrustedTokenIssuer' object (with a fabricated InstanceArn); real UpdateTrustedTokenIssuerOutput is void. Fixed to {}."}
  ListTrustedTokenIssuers: {wire: ok, errors: ok, state: ok, persist: ok, note: "per-item shape (types.TrustedTokenIssuerMetadata) had an invented InstanceArn member that doesn't exist on the real type (Name/TrustedTokenIssuerArn/TrustedTokenIssuerType only); also MaxResults/NextToken were ignored. Both fixed."}
  DescribeInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "response included an invented 'Tags' member; real DescribeInstanceOutput has none. Fixed; tags now only reachable via ListTagsForResource."}
  ListRegions: {wire: ok, errors: ok, state: ok, persist: ok, note: "MaxResults/NextToken were ignored; now paginated"}
  # --- error-status class fix (every op) ---
  "*": {wire: ok, errors: ok, state: ok, persist: ok, note: "ALL ops: ResourceNotFoundException/ConflictException were mapped to HTTP 404/409. ssoadmin is the plain 'json' (awsjson1.1) protocol with no per-exception @httpError override in its Smithy model (verified against botocore's sso-admin service-2.json): every exception without fault=true (i.e. everything except InternalServerException) is a client fault and real AWS returns HTTP 400 for ALL of them, matching the convention already used in services/secretsmanager (another pure-JSON-protocol service, single http.StatusBadRequest for its whole handler) and DynamoDB (returns 400 for ResourceNotFoundException in production). Fixed handleBackendError + 6 other writeError call sites + every test asserting on the old codes."}
  # --- previously fixed (last sweep, still verified ok) ---
  DescribeRegion: {wire: ok, errors: ok, state: ok, persist: ok}
  AddRegion: {wire: ok, errors: ok, state: ok, persist: ok}
  RemoveRegion: {wire: ok, errors: ok, state: ok, persist: ok}
  PutApplicationAccessScope: {wire: ok, errors: ok, state: ok, persist: ok}
  GetApplicationAccessScope: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  CreatePermissionSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "confirmed nested under 'PermissionSet' -- this IS correct per the real CreatePermissionSetOutput shape, unlike the Application/TrustedTokenIssuer wrapper bugs above"}
  DescribePermissionSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "same 'PermissionSet' wrapper confirmed correct"}
  ListPermissionSets: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdatePermissionSet: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePermissionSet: {wire: ok, errors: ok, state: ok, persist: ok}
  AttachManagedPolicyToPermissionSet: {wire: ok, errors: ok, state: ok, persist: ok}
  DetachManagedPolicyFromPermissionSet: {wire: ok, errors: ok, state: ok, persist: ok}
  ListManagedPoliciesInPermissionSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "MaxResults/NextToken were ignored; now paginated (dedup'd with ListCustomerManagedPolicyReferencesInPermissionSet via generic listPermissionSetSubItems helper)"}
  ListCustomerManagedPolicyReferencesInPermissionSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "same pagination fix"}
  PutInlinePolicyToPermissionSet: {wire: ok, errors: ok, state: ok, persist: ok}
  GetInlinePolicyForPermissionSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "confirmed flat, correct"}
  PutPermissionsBoundaryToPermissionSet: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPermissionsBoundaryForPermissionSet: {wire: ok, errors: ok, state: ok, persist: ok, note: "confirmed nested under 'PermissionsBoundary' -- correct"}
  GetApplicationAssignmentConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "confirmed flat {AssignmentRequired}, correct"}
  CreateInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "confirmed flat {InstanceArn}, correct"}
  UpdateInstance: {wire: ok, errors: ok, state: ok, persist: ok, note: "confirmed void {}, correct"}
families:
  Instance: {status: ok, note: "CreateInstance/DescribeInstance(fixed)/DeleteInstance/ListInstances/UpdateInstance"}
  PermissionSet+Policies: {status: ok, note: "managed/inline/customer-managed/permissions-boundary attach-detach + pagination fixed on the two List ops that needed it"}
  AccountAssignment: {status: ok, note: "SEVERE TargetId/TargetType wire-shape bug fixed across Create/Delete/Describe*/List*Status; ListAccountAssignmentsForPrincipal Filter+pagination added"}
  ProvisionPermissionSet+Status: {status: ok, note: "PermissionSetProvisioningStatus shape confirmed correctly distinct from AccountAssignmentOperationStatus (previously incorrectly shared one Go view type using AccountAssignment's field name); List variants slimmed to the real Metadata shape + paginated"}
  Application+Assignment+AccessScope+AuthMethod+Grant+SessionConfig: {status: ok, note: "SEVERE: Create/Describe/Update Application and DescribeApplicationAssignment all had invented wrapper objects/fields that would break a real client's deserializer (found and fixed 4 of these 'invented wrapper' bugs across Application+AccountAssignment+AuthMethod+Grant+TrustedTokenIssuer -- same bug class recurring across the file, all now fixed). PutApplicationSessionConfiguration/GetApplicationSessionConfiguration fully re-modeled around the real UserBackgroundSessionApplicationStatus field (previously a fabricated SessionDuration concept). GetApplicationAuthenticationMethod/GetApplicationGrant double-wrap bugs fixed. Pagination added to ListApplicationAssignments(ForPrincipal)/ListApplicationAccessScopes/ListApplicationProviders."}
  TrustedTokenIssuer: {status: ok, note: "SEVERE: DescribeTrustedTokenIssuer/UpdateTrustedTokenIssuer had the same invented-wrapper bug class (plus fabricated InstanceArn/Tags members); fixed. ListTrustedTokenIssuers item shape also had an invented InstanceArn; fixed + paginated."}
  InstanceAccessControlAttributeConfiguration: {status: ok}
  Region: {status: ok, note: "ListRegions pagination added this sweep"}
  Tags: {status: ok, note: "TagResource/UntagResource/ListTagsForResource confirmed as the sole tag-retrieval path for Application/Instance/TrustedTokenIssuer (all three previously had fabricated inline Tags members on their Describe/singular-Get responses, now removed)"}
gaps:
  - "RegionMetadata.IsPrimaryRegion is always false -- known simplification, unchanged from prior sweep (bd: none filed)."
  - "ListApplicationAuthenticationMethods/ListApplicationGrants/ListTagsForResource support NextToken on the real API but have no MaxResults member at all (unlike every other List op in this service); gopherstack still returns everything in one page with a nil NextToken for these three. Low-value: there is no MaxResults contract to violate (a real caller can never request a capped page), and this mirrors the same intentional simplification already accepted for other AWS emulators in this codebase. Not fixed this sweep due to time; flagging for a future pass if strict pagination parity is ever required here."
  - "ListPermissionSetsProvisionedToAccount / ListAccountsForProvisionedPermissionSet accept but ignore the real API's ProvisioningStatus filter (LATEST_PERMISSION_SET_PROVISIONED / LATEST_PERMISSION_SET_NOT_PROVISIONED) -- this backend has no concept of a permission set's provisioned-vs-edited-since-provisioned drift per account, so the filter can't be meaningfully implemented without a much larger feature (tracking a 'provisioned version' per account/permission-set pair). Real API modeled; gopherstack accepts the field and ignores it (returns the superset). Flagging for triage; MaxResults/NextToken on both ops WAS fixed this sweep."
  - "DescribeInstanceOutput's EncryptionConfigurationDetails/StatusReason members are not modeled (this backend has no per-instance encryption-config or status-reason concept); both would always be empty/absent for this backend's instances regardless, so omitting them entirely (rather than emitting a fake empty value) matches real behavior for every instance this backend can produce."
deferred: []
leaks: {status: clean, note: "no new goroutines/janitors introduced this sweep; all fixes are pure request-parsing/response-shape/backend-field changes inside the existing coarse-lock methods. identityStoreArn() helper reads b.instances while b.mu is already held by the caller (CreateApplication/AddApplicationInternal) -- safe because store.Table.Get has no internal locking (backend-level coarse lock only, confirmed in pkgs/store/table.go), consistent with every other Table access pattern in this backend."}
---

## Notes (2026-07-24 sweep)

This sweep re-audited every op's wire shape byte-for-byte against
`aws-sdk-go-v2/service/ssoadmin@v1.38.0`'s real `deserializers.go` (not just the exported
`types` package structs, which don't reveal wrapper nesting) and found a recurring, severe bug
class that the prior sweep's `overall: A` rating missed entirely:

**Invented response wrappers ("double-nesting").** Several singular Describe/Update/Get/Create
handlers wrapped their entire response payload one level too deep under a fabricated top-level
key (e.g. `{"Application": {...}}`, `{"ApplicationAssignment": {...}}`,
`{"TrustedTokenIssuer": {...}}`) that does not exist anywhere in the real SDK. The real ops in
question (`DescribeApplication`, `DescribeApplicationAssignment`, `DescribeTrustedTokenIssuer`,
`UpdateApplication`, `UpdateTrustedTokenIssuer`, `CreateApplication`) are all **flat** or **void**
outputs per their real `awsAwsjson11_deserializeOpDocument*Output` functions. A real
`aws-sdk-go-v2` client calling any of these against the old code would get every single output
field `nil`/zero-valued -- not a subtle bug, a total break of the operation. Root-caused by
reading only the exported Go struct shape and assuming AWS's common "wrap the resource under its
type name" convention (which IS correct for `CreatePermissionSet`/`DescribePermissionSet`/
`GetPermissionsBoundaryForPermissionSet`, confirmed still correct this sweep) applies universally
-- it doesn't; each op's wrapping is independently modeled in Smithy and has to be checked against
the deserializer, not inferred from a pattern seen elsewhere in the same service.

A second, narrower instance of the same class: `GetApplicationAuthenticationMethod` and
`GetApplicationGrant` union-typed responses were wrapped in an *extra* discriminator pair
(`{"AuthenticationMethodType": ..., "AuthenticationMethod": ...}`) copied from the sibling `List*`
op's per-item shape (`AuthenticationMethodItem`/`GrantItem`, which legitimately has both fields as
siblings) -- but the singular `Get*Output` shapes have **only** the union member, no sibling type
field. This is the same underlying mistake (pattern-matching one op's shape onto a
similar-but-different op) at one level of nesting instead of two.

**Fabricated fields on otherwise-correct shapes.** Independent of the wrapper bug:
`DescribeInstance`/`DescribeApplication`/`DescribeTrustedTokenIssuer` all echoed a `Tags` array
inline -- none of the three real `Describe*Output` shapes have a `Tags` member at all (tags are
always a separate `ListTagsForResource` call in real ssoadmin, confirmed for every taggable
resource type). `DescribeTrustedTokenIssuer`/`UpdateTrustedTokenIssuer`/`ListTrustedTokenIssuers`
also fabricated an `InstanceArn` member that doesn't exist on `TrustedTokenIssuerMetadata` or
`DescribeTrustedTokenIssuerOutput`. `PutApplicationAssignmentConfiguration` accepted an invented
`AssignmentRequiredForAllIdentities` request field with no real counterpart.
`AccountAssignmentOperationStatus` (the Create/Delete/Describe*Status shape for account
assignments) used a fabricated `AccountId` field where the real member is `TargetId`/`TargetType`
-- confirmed via the real `awsAwsjson11_deserializeDocumentAccountAssignmentOperationStatus`,
which has no `AccountId` case at all. All three `List*Status` metadata shapes
(`AccountAssignmentOperationStatusMetadata`, `PermissionSetProvisioningStatusMetadata`) are
slimmer than their singular counterparts (`CreatedDate`/`RequestId`/`Status` only) and were
previously returning the full fat shape.

**A whole fabricated concept.** `PutApplicationSessionConfiguration`/
`GetApplicationSessionConfiguration` modeled a `SessionDuration` string (confused with the
unrelated, legitimate `PermissionSet.SessionDuration` field) that does not exist anywhere on
these two real operations. The real member is `UserBackgroundSessionApplicationStatus`
(`ENABLED`/`DISABLED` enum), confirmed via both operations' real Go SDK source. Fully re-modeled
end-to-end (interface signature doc-comment, backend validation, both handlers, both wire
responses) since there were no external callers of the backend method outside this package
(verified via repo-wide grep before changing the signature's semantics).

**Missing pagination.** ~13 List operations that the real SDK models with `MaxResults`/`NextToken`
were silently ignoring both and always returning every result with a nil `NextToken` --
technically tolerated by the Go SDK client (which just gets everything on "page 1"), but a real
`MaxResults` cap is a hard client-side contract that was being violated. Fixed via the existing
`paginateBy` helper for alphabetically-orderable list items, and a new `paginateOrdered` helper
(does NOT re-sort, only locates the resume point by a unique key) for the three
`ProvisioningStatus`-derived lists that must preserve the backend's `CreatedDate`-descending
order -- using `paginateBy` there would have silently scrambled `TestListPermissionSetProvisioningStatusSorted`'s
established chronological-order contract into alphabetical-by-RequestId order, since RequestId is
a random UUID with no correlation to creation time.

**Error-status class bug (every op).** `ResourceNotFoundException`/`ConflictException` were
mapped to HTTP 404/409 throughout the handler. ssoadmin's Smithy model
(`botocore/data/sso-admin/2020-07-20/service-2.json`, cross-checked) has no per-exception
`error.httpStatusCode` override on any of its 7 exception shapes; for the plain `"json"`
(awsjson1.1) protocol this means every exception without `"fault": true` (i.e. everything except
`InternalServerException`) is a client fault that real AWS returns as **HTTP 400**, full stop --
this is the same behavior confirmed for DynamoDB (another pure-JSON-protocol service) and already
implemented correctly in `services/secretsmanager` (single `http.StatusBadRequest` for its entire
handler, no per-exception-name status logic at all). The `aws-sdk-go-v2` client itself doesn't
care about the exact status code (it dispatches on the body's `__type`/error-code string as long
as the status is non-2xx), so this was invisible to Go-SDK-based tests, but any HTTP-status-aware
caller (a different SDK, curl, a test harness asserting on raw status) would see the wrong code.
Fixed `handleBackendError` (the shared error-mapping function) plus 6 other inline
`writeError(..., http.StatusConflict/StatusNotFound, ...)` call sites that bypassed it, and every
test in the package asserting on the old codes (~50 call sites via blanket `sed`, verified each
was actually a ssoadmin-exception-status assertion, not an unrelated HTTP semantic, before the
bulk edit).

**Dedup.** The three `ProvisioningStatus` metadata list handlers (`ListAccountAssignmentCreationStatus`/
`ListAccountAssignmentDeletionStatus`/`ListPermissionSetProvisioningStatus`) and the two
permission-set-subresource list handlers (`ListManagedPoliciesInPermissionSet`/
`ListCustomerManagedPolicyReferencesInPermissionSet`) were each near-identical once the pagination
fix was applied (flagged by `golangci-lint`'s `dupl` linter). Extracted into two generic
package-level helpers (`listProvisioningStatusMetadata[T]`, `listPermissionSetSubItems[T]`) in
`handler.go` rather than suppressed with `//nolint:dupl`; each handler is now a 5-8 line call into
the shared helper.

- Persistence: no snapshot-shape changes this sweep (`ssoadminSnapshotVersion` unchanged);
  `Application` gained three new backend-only fields (`ApplicationAccount`/`CreatedFrom`/
  `IdentityStoreArn`) which round-trip automatically since the whole struct is already
  JSON-snapshotted by value.
- Carried over unmodified from the prior sweep (still verified correct): the awsjson1.1
  `X-Amz-Target` routing, epoch-seconds timestamp handling, and the Region family's
  ADDING/ACTIVE/REMOVING lazy-transition + lazy-prune pattern.
