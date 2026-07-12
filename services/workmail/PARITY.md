---
service: workmail
sdk_module: aws-sdk-go-v2/service/workmail@v1.37.2
last_audit_commit: 43c84585
last_audit_date: 2026-07-12
overall: A            # genuine wire/logic fixes found this pass
ops:
  CreateOrganization: {wire: ok, errors: ok, state: ok, persist: ok, note: "Domains was []string; real wire is [{DomainName,HostedZoneId}] objects -- json.Unmarshal failed for any client-specified domain (500 InternalServiceError). Fixed."}
  DescribeOrganization: {wire: ok, errors: ok, state: ok, persist: ok, note: "added ErrorMessage field; MigrationAdmin still not modeled (gap)."}
  DeleteOrganization: {wire: ok, errors: ok, state: ok, persist: ok}
  ListOrganizations: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateUser: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeUser: {wire: ok, errors: ok, state: ok, persist: ok, note: "many optional profile fields (City/Company/Country/Department/Initials/JobTitle/Office/Street/Telephone/ZipCode/HiddenFromGlobalAddressList/IdentityProvider*/MailboxProvisionedDate/MailboxDeprovisionedDate) not modeled -- gap, not a wire-shape bug (absent keys are valid zero values)."}
  UpdateUser: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteUser: {wire: ok, errors: ok, state: ok, persist: ok}
  ListUsers: {wire: partial, errors: ok, state: ok, persist: ok, note: "Filters (DisplayNamePrefix/PrimaryEmailPrefix/State/UsernamePrefix/IdentityProviderUserIdPrefix) accepted on the wire by real API but silently ignored -- gap, see below."}
  RegisterToWorkMail: {wire: ok, errors: ok, state: ok, persist: ok, note: "verified real ENABLED transition + EnabledDate + email index writes, not a disguised no-op."}
  DeregisterFromWorkMail: {wire: ok, errors: ok, state: ok, persist: ok, note: "verified real DISABLED transition + EnabledDate cleared."}
  ResetPassword: {wire: ok, errors: ok, state: ok, persist: ok, note: "password intentionally not stored (matches other gopherstack auth-adjacent ops); existence is still validated."}
  GetMailboxDetails: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateMailboxQuota: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdatePrimaryEmailAddress: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  ListGroups: {wire: partial, errors: ok, state: ok, persist: ok, note: "Filters (NamePrefix/PrimaryEmailPrefix/State) accepted but ignored -- gap."}
  AssociateMemberToGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateMemberFromGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  ListGroupMembers: {wire: ok, errors: ok, state: ok, persist: ok}
  ListGroupsForEntity: {wire: ok, errors: ok, state: ok, persist: ok, note: "response reused the ListGroups item shape (Id/Name/Email/State); real shape is types.GroupIdentifier (GroupId/GroupName only) -- every field the SDK actually reads was zero-valued. Fixed with a dedicated groupIdentifierResp type."}
  CreateResource: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "BookingOptions / HiddenFromGlobalAddressList not modeled -- gap."}
  UpdateResource: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListResources: {wire: partial, errors: ok, state: ok, persist: ok, note: "Filters accepted but ignored -- gap."}
  AssociateDelegateToResource: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateDelegateFromResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListResourceDelegates: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateAlias: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAlias: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAliases: {wire: ok, errors: ok, state: ok, persist: ok, note: "primary email correctly included as first alias entry."}
  PutMailboxPermissions: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteMailboxPermissions: {wire: ok, errors: ok, state: ok, persist: ok}
  ListMailboxPermissions: {wire: ok, errors: ok, state: ok, persist: ok}
  RegisterMailDomain: {wire: ok, errors: ok, state: ok, persist: ok}
  DeregisterMailDomain: {wire: ok, errors: ok, state: ok, persist: ok, note: "default-domain protection verified (MailDomainStateException)."}
  GetMailDomain: {wire: ok, errors: ok, state: ok, persist: ok, note: "IsDefault/IsTestDomain/OwnershipVerificationStatus correct; DkimVerificationStatus and Records (DNS record list) not modeled -- gap."}
  ListMailDomains: {wire: ok, errors: ok, state: ok, persist: ok, note: "item shape is types.MailDomainSummary, wire key is DefaultDomain (not IsDefault) and there is no IsTestDomain field -- was silently emitting IsDefault=false/absent forever from the real client's point of view. Fixed."}
  UpdateDefaultMailDomain: {wire: ok, errors: ok, state: ok, persist: ok}
  PutAccessControlRule: {wire: ok, errors: ok, state: ok, persist: ok, note: "ImpersonationRoleIds/NotImpersonationRoleIds accepted by real API but not modeled -- gap."}
  DeleteAccessControlRule: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAccessControlEffect: {wire: ok, errors: ok, state: ok, persist: ok, note: "creation-order rule evaluation, CIDR matching verified."}
  ListAccessControlRules: {wire: ok, errors: ok, state: ok, persist: ok, note: "response used IPRanges/NotIPRanges (wrong casing); real wire is IpRanges/NotIpRanges -- an SDK client would see empty slices always. Fixed."}
  CreateImpersonationRole: {wire: ok, errors: ok, state: ok, persist: ok}
  GetImpersonationRole: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateImpersonationRole: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteImpersonationRole: {wire: ok, errors: ok, state: ok, persist: ok}
  ListImpersonationRoles: {wire: ok, errors: ok, state: ok, persist: ok, note: "response field was Items; real field is Roles. Fixed."}
  TagResource: {wire: ok, errors: ok, state: ok, persist: deferred, note: "tags map (b.tags) is one of the raw maps NOT persisted by Snapshot/Restore -- see leaks/gaps below."}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: deferred}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: deferred}
  DescribeEntity: {wire: ok, errors: ok, state: ok, persist: ok, note: "real API's only documented lookup key is Email; backend previously only matched by internal ID or Name, so a real client's DescribeEntity(Email=...) call always 404'd. Fixed to check the byEmail reverse-index maps first, falling back to ID/Name for compatibility."}
  CreateAvailabilityConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAvailabilityConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateAvailabilityConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAvailabilityConfigurations: {wire: ok, errors: ok, state: ok, persist: ok}
  TestAvailabilityConfiguration: {wire: ok, errors: ok, state: ok, persist: ok, note: "real endpoint/ARN validation, not a fabricated always-true stub."}
  CreateMobileDeviceAccessRule: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteMobileDeviceAccessRule: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateMobileDeviceAccessRule: {wire: ok, errors: ok, state: ok, persist: ok}
  ListMobileDeviceAccessRules: {wire: ok, errors: ok, state: ok, persist: ok}
  GetMobileDeviceAccessEffect: {wire: ok, errors: ok, state: ok, persist: ok}
  PutMobileDeviceAccessOverride: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteMobileDeviceAccessOverride: {wire: ok, errors: ok, state: ok, persist: ok}
  GetMobileDeviceAccessOverride: {wire: ok, errors: ok, state: ok, persist: ok}
  ListMobileDeviceAccessOverrides: {wire: ok, errors: ok, state: ok, persist: ok}
  PutEmailMonitoringConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteEmailMonitoringConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeEmailMonitoringConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  PutInboundDmarcSettings: {wire: ok, errors: ok, state: ok, persist: deferred, note: "inboundDmarc raw map is persisted (restoreCollectionMaps); ok."}
  DescribeInboundDmarcSettings: {wire: ok, errors: ok, state: ok, persist: ok}
  PutRetentionPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteRetentionPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  GetDefaultRetentionPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  StartMailboxExportJob: {wire: ok, errors: ok, state: ok, persist: ok}
  CancelMailboxExportJob: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeMailboxExportJob: {wire: ok, errors: ok, state: ok, persist: ok}
  ListMailboxExportJobs: {wire: ok, errors: ok, state: ok, persist: ok, note: "real Jobs[] item type is the FULL MailboxExportJob shape (same as Describe), not a summary; RoleArn/KmsKeyArn/S3Path/S3Prefix/EstimatedProgress/ErrorInfo were missing from the list response even though the backend already tracks them. Fixed."}
  CreateIdentityCenterApplication: {wire: ok, errors: ok, state: ok, persist: deferred, note: "identityCenterApps raw map IS persisted (restoreReverseLookupMaps); ok."}
  DeleteIdentityCenterApplication: {wire: ok, errors: ok, state: ok, persist: ok}
  PutIdentityProviderConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteIdentityProviderConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeIdentityProviderConfiguration: {wire: ok, errors: ok, state: ok, persist: ok}
  DeletePersonalAccessToken: {wire: ok, errors: ok, state: ok, persist: ok}
  GetPersonalAccessTokenMetadata: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPersonalAccessTokens: {wire: ok, errors: ok, state: ok, persist: ok}
  GetImpersonationRoleEffect: {wire: ok, errors: ok, state: ok, persist: ok}
  AssumeImpersonationRole: {wire: ok, errors: ok, state: ok, persist: ok, note: "issued-token bookkeeping (issuedTokens table) is real, not fabricated; token itself is opaque (matches real API, which doesn't document a verifiable format)."}
families:
  route-matcher: {status: ok, note: "single X-Amz-Target-prefix POST endpoint (WorkMailService.<Op>); MatchPriority/RouteMatcher/ExtractOperation all verified against service.HandleTarget's shared dispatcher, not just a Handler() unit test. Every op in buildOps() is dispatch-reachable; GetSupportedOperations() is derived directly from the same map so there is no listed-but-unrouted op."}
  persistence: {status: ok, note: "Handler.Snapshot/Restore already delegate to backend (fixed in an earlier phase per persistence.go's doc comments) so cli.go's setupPersistence picks WorkMail up. Verified all 15 org-nested/composite-keyed tables + 3 registry tables + raw maps round-trip through backendSnapshot; version-mismatch discard-and-reset path present."}
  error-mapping: {status: ok, note: "every backend error path wraps one of ErrNotFound/ErrConflict/ErrValidation/ErrLimitExceeded/ErrMailDomainState/ErrEntityState; handleError's switch covers all six plus isUnknownOp -- no bare fmt.Errorf that would fall through to InternalServiceError found."}
gaps:
  - "ListUsers/ListGroups/ListResources/ListGroupsForEntity accept a Filters object (name/email prefix, state) on the wire but the backend silently ignores it and returns the full unfiltered page -- a real SDK client doing a prefix search gets more results than AWS would return. Not fixed this pass (interface-signature change across 4 ops); needs a bd issue."
  - "PutAccessControlRule/AccessControlRule don't model ImpersonationRoleIds/NotImpersonationRoleIds (added to the real API after impersonation roles shipped)."
  - "DescribeUser doesn't model the optional profile fields (City, Company, Country, Department, HiddenFromGlobalAddressList, IdentityProviderIdentityStoreId, IdentityProviderUserId, Initials, JobTitle, MailboxDeprovisionedDate, MailboxProvisionedDate, Office, Street, Telephone, ZipCode)."
  - "GetMailDomain doesn't return DkimVerificationStatus or the Records (recommended DNS record) list."
  - "DescribeOrganization doesn't model MigrationAdmin (interoperability/migration feature not simulated)."
  - "Organization.State is hardcoded to ACTIVE (org creation is synchronous); real AWS transitions through Creating/Active/etc, but nothing in this backend ever leaves an org in a non-terminal state, so this is a non-issue in practice, not a hidden bug."
deferred:
  - "Tags (TagResource/UntagResource/ListTagsForResource) use a raw map (b.tags) NOT included in backendSnapshot -- tags are dropped across restart/restore while every other resource type persists. Confirmed by reading persistence.go's backendSnapshot struct and Snapshot/Restore; not fixed this pass (would need a new persisted field + wire-compatible key, and tags aren't in the audit's stated highest-traffic set). Needs a bd issue."
leaks: {status: clean, note: "no goroutines, tickers, or background janitors in this service; AssumeImpersonationRole issues tokens into issuedTokens with no TTL sweep, matching MobileDeviceAccessOverride/PersonalAccessToken's pattern of storing ExpiresAt/ExpiresTime as data without an active expiry janitor -- consistent with the rest of the service, not a new leak."}
---

## Notes

Protocol: awsjson1.1, single POST endpoint, `X-Amz-Target: WorkMailService.<Op>`.
Route matcher is a simple header-prefix check (`service.PriorityHeaderExact`); every
op in `buildOps()` is reachable through `service.HandleTarget` → `h.dispatch`, not just
through direct `Handler()` unit-test calls.

### Bugs fixed this pass (all in `services/workmail/`)

1. **`CreateOrganization` request-breaking wire bug** (`handler.go`): `Domains` was typed
   `[]string`, but the real SDK always serializes it as a list of `{DomainName,
   HostedZoneId}` objects (`aws-sdk-go-v2/service/workmail/types.Domain`). Any real
   client call that specified a domain would fail `json.Unmarshal` and surface as a 500
   `InternalServiceError` — this broke the single most common non-trivial
   `CreateOrganization` call shape. Fixed with a `domainReq` struct; `HostedZoneId` is
   accepted and discarded (Route53-only, no meaning for the in-memory backend).
2. **`ListAccessControlRules` response casing** (`handler.go`): emitted `IPRanges`/
   `NotIPRanges`; real wire keys are `IpRanges`/`NotIpRanges` (case-sensitive JSON
   deserialization means a real client always saw empty slices for these two fields).
3. **`ListMailDomains` response key** (`handler.go`): emitted `IsDefault`; the real
   `ListMailDomains` item type (`types.MailDomainSummary`) uses `DefaultDomain` and has
   no `IsTestDomain` field at all (that field only exists on the unrelated
   `GetMailDomainOutput` shape, which was already correct). A real client always saw
   `DefaultDomain: false` regardless of the actual default domain.
4. **`ListGroupsForEntity` response shape** (`handler.go`): reused the `ListGroups` item
   shape (`Id`/`Name`/`Email`/`State`); the real op returns `types.GroupIdentifier`
   (`GroupId`/`GroupName` only) — every field a real client actually reads
   (`GroupId`, `GroupName`) was always absent/zero-valued.
5. **`ListImpersonationRoles` response key** (`handler.go`): emitted `Items`; real field
   is `Roles`.
6. **`ListMailboxExportJobs` incomplete shape** (`handler.go`): real `Jobs[]` reuses the
   full `MailboxExportJob` type (same as `DescribeMailboxExportJob`), not a narrower
   summary. `RoleArn`, `KmsKeyArn`, `S3Path`, `S3Prefix`, `EstimatedProgress`, and
   `ErrorInfo` were missing even though the backend already tracks all of them.
7. **`DescribeEntity` never resolved by email** (`backend.go`): the real API's
   `DescribeEntityInput.Email` field is documented as "the email under which the entity
   exists" — it is an email lookup. The backend's `findUser`/`findGroup`/`findResource`
   only match by internal ID or by `Name`, never by `Email`, so a real client's
   `DescribeEntity` call (which can only pass an email) always 404'd unless the email
   happened to coincide with the entity's `Name`. Fixed by checking the existing
   `usersByEmail`/`groupsByEmail`/`resourcesByEmail` reverse-index maps first, falling
   back to the ID/Name lookup for backward compatibility with internal callers.
8. Added `ErrorMessage` to `DescribeOrganization`'s response (real field, backend
   already tracks `Organization.ErrorMessage`; cheap correctness fix, currently always
   empty in practice since org creation is synchronous).

Two existing tests (`TestAudit1_WorkMail_MailDomains`'s `list_mail_domains` case,
`TestAudit1_WorkMail_...`'s `list_groups_for_entity` and `list_impersonation_roles`
cases) asserted the **old, wrong** wire keys and were updated to assert the corrected
ones. New regression tests added in `handler_bugfix_test.go` cover all of the above.

### Traps for the next auditor

- `GetMailDomain` (single-domain describe) and `ListMailDomains` (list) use **two
  different SDK types** with overlapping-but-different field sets — `IsDefault` is
  correct for `GetMailDomain`, wrong for `ListMailDomains`. Don't conflate them again.
- `ListGroups` and `ListGroupsForEntity` also use two different SDK types
  (`types.Group` vs `types.GroupIdentifier`) despite both being "list of groups" ops.
- The backend's per-entity `find*(orgID, entityID)` helpers (`findUser`, `findGroup`,
  `findResource`) intentionally accept either an internal ID or a `Name` — they do NOT
  search by email. Any op whose real AWS input field is documented as an email
  (`DescribeEntity`) needs the `usersByEmail`/`groupsByEmail`/`resourcesByEmail` maps
  instead, not `find*`.
