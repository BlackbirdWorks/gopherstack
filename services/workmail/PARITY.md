---
service: workmail
sdk_module: aws-sdk-go-v2/service/workmail@v1.39.4
last_audit_commit: dc877102
last_audit_date: 2026-07-23
overall: A            # 6 gaps + 1 (already-fixed, stale-labeled) deferred item closed; 1 real leak class fixed; banned nolint removed
ops:
  CreateOrganization: {wire: ok, errors: ok, state: ok, persist: ok, note: "Domains was []string; real wire is [{DomainName,HostedZoneId}] objects -- json.Unmarshal failed for any client-specified domain (500 InternalServiceError). Fixed (prior pass). Default + client-specified domains now also populate DkimVerificationStatus/Records (see GetMailDomain)."}
  DescribeOrganization: {wire: ok, errors: ok, state: ok, persist: ok, note: "added MigrationAdmin field (types.DescribeOrganizationOutput.MigrationAdmin). Field-diffed the whole SDK surface: no operation in aws-sdk-go-v2/service/workmail@v1.37.2 ever sets MigrationAdmin (it's populated out-of-band by an Exchange interoperability/migration flow this backend doesn't simulate), so it is correctly always empty/omitted -- matches every real org that never configured migration. Not a stub: the field is modeled and wired, it's just never non-empty because nothing in the real API's surface can make it non-empty either."}
  DeleteOrganization: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascade-delete now also purges tags (org's own + every contained user/group/resource's, via ARN-prefix match) and globalAliases rows (primary emails + CreateAlias aliases) for the whole org -- previously both were left as permanent ghost rows post-delete (DeleteOrganization's own doc comment said tags were 'deliberately left untouched'). See leaks below."}
  ListOrganizations: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateUser: {wire: ok, errors: ok, state: ok, persist: ok, note: "now wires FirstName/LastName/IdentityProviderUserId/HiddenFromGlobalAddressList from CreateUserInput -- previously accepted on the wire but silently discarded (never reached the User struct, so DescribeUser could never surface them even before this pass' DescribeUser fix)."}
  DescribeUser: {wire: ok, errors: ok, state: ok, persist: ok, note: "GAP CLOSED: City/Company/Country/Department/Initials/JobTitle/Office/Street/Telephone/ZipCode/HiddenFromGlobalAddressList/IdentityProviderIdentityStoreId/IdentityProviderUserId/MailboxProvisionedDate/MailboxDeprovisionedDate all now modeled on User and wired through DescribeUser's response. MailboxProvisionedDate/MailboxDeprovisionedDate are set alongside EnabledDate/DisabledDate in RegisterToWorkMail/DeregisterFromWorkMail (real WorkMail provisions/deprovisions the mailbox at the same time it enables/disables WorkMail use)."}
  UpdateUser: {wire: ok, errors: ok, state: ok, persist: ok, note: "now accepts City/Company/Country/Department/Initials/JobTitle/Office/Street/Telephone/ZipCode/IdentityProviderUserId/Role/HiddenFromGlobalAddressList (UpdateUserInput's full field set) -- previously only DisplayName/FirstName/LastName were wired."}
  DeleteUser: {wire: ok, errors: ok, state: ok, persist: ok, note: "now cascade-cleans CreateAlias-created aliases + their globalAliases rows, mailbox permissions (as target entity AND as grantee), group memberships, resource delegate listings, mailboxQuotas, and tags -- see leaks below. Also now clears the user's primary email from globalAliases on delete (was previously the only one of the three entity types that skipped this; verified unreachable in practice since delete requires DISABLED state, which only follows DeregisterFromWorkMail, which already clears Email -- defensive fix for consistency with DeleteGroup/DeleteResource, not a live bug)."}
  ListUsers: {wire: ok, errors: ok, state: ok, persist: ok, note: "GAP CLOSED: Filters (DisplayNamePrefix/PrimaryEmailPrefix/State/UsernamePrefix/IdentityProviderUserIdPrefix) now filter the result set (userMatchesFilter in users.go); previously accepted on the wire but silently ignored, returning the full unfiltered page."}
  RegisterToWorkMail: {wire: ok, errors: ok, state: ok, persist: ok, note: "verified real ENABLED transition + EnabledDate + email index writes, not a disguised no-op. Now also sets MailboxProvisionedDate for users."}
  DeregisterFromWorkMail: {wire: ok, errors: ok, state: ok, persist: ok, note: "verified real DISABLED transition + EnabledDate cleared. Now also sets MailboxDeprovisionedDate for users."}
  ResetPassword: {wire: ok, errors: ok, state: ok, persist: ok, note: "password intentionally not stored (matches other gopherstack auth-adjacent ops); existence is still validated."}
  GetMailboxDetails: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateMailboxQuota: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdatePrimaryEmailAddress: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteGroup: {wire: ok, errors: ok, state: ok, persist: ok, note: "now cascade-cleans aliases/globalAliases/permissions(target+grantee)/group-memberships-of-others/resource-delegate-listings/tags via the same cascadeCleanEntity helper DeleteUser/DeleteResource use -- see leaks below."}
  ListGroups: {wire: ok, errors: ok, state: ok, persist: ok, note: "GAP CLOSED: Filters (NamePrefix/PrimaryEmailPrefix/State) now filter the result set (groupMatchesFilter in groups.go); previously accepted but ignored."}
  AssociateMemberToGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateMemberFromGroup: {wire: ok, errors: ok, state: ok, persist: ok}
  ListGroupMembers: {wire: ok, errors: ok, state: ok, persist: ok}
  ListGroupsForEntity: {wire: ok, errors: ok, state: ok, persist: ok, note: "response reused the ListGroups item shape (Id/Name/Email/State); real shape is types.GroupIdentifier (GroupId/GroupName only) -- every field the SDK actually reads was zero-valued. Fixed with a dedicated groupIdentifierResp type (prior pass). GAP CLOSED this pass: Filters.GroupNamePrefix (the op's single filter dimension) now filters the result set; previously accepted but ignored."}
  CreateResource: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "BookingOptions / HiddenFromGlobalAddressList not modeled -- gap, not in this pass' declared 6; see gaps below."}
  UpdateResource: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "now cascade-cleans aliases/globalAliases/permissions(target+grantee)/group-memberships/other-resources'-delegate-listings/tags via cascadeCleanEntity -- see leaks below."}
  ListResources: {wire: ok, errors: ok, state: ok, persist: ok, note: "GAP CLOSED: Filters (NamePrefix/PrimaryEmailPrefix/State) now filter the result set (resourceMatchesFilter in resources.go); previously accepted but ignored."}
  AssociateDelegateToResource: {wire: ok, errors: ok, state: ok, persist: ok}
  DisassociateDelegateFromResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListResourceDelegates: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateAlias: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAlias: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAliases: {wire: ok, errors: ok, state: ok, persist: ok, note: "primary email correctly included as first alias entry."}
  PutMailboxPermissions: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteMailboxPermissions: {wire: ok, errors: ok, state: ok, persist: ok}
  ListMailboxPermissions: {wire: ok, errors: ok, state: ok, persist: ok}
  RegisterMailDomain: {wire: ok, errors: ok, state: ok, persist: ok, note: "now sets DkimVerificationStatus=PENDING and populates Records (see GetMailDomain gap-close note)."}
  DeregisterMailDomain: {wire: ok, errors: ok, state: ok, persist: ok, note: "default-domain protection verified (MailDomainStateException)."}
  GetMailDomain: {wire: ok, errors: ok, state: ok, persist: ok, note: "GAP CLOSED: DkimVerificationStatus (PENDING on RegisterMailDomain, VERIFIED on the org's own domains from CreateOrganization) and Records (types.DnsRecord list: MX + SPF TXT + autodiscover CNAME + 3 DKIM CNAMEs, via dnsRecordsForDomain in mail_domains.go) now modeled and wired through the response. Record token/value contents are simulation-only placeholders (real WorkMail issues real per-domain DKIM tokens); the wire shape ({Hostname,Type,Value} per entry) is what a real SDK client actually reads and is correct. IsDefault/IsTestDomain/OwnershipVerificationStatus still correct (prior pass)."}
  ListMailDomains: {wire: ok, errors: ok, state: ok, persist: ok, note: "item shape is types.MailDomainSummary, wire key is DefaultDomain (not IsDefault) and there is no IsTestDomain field -- was silently emitting IsDefault=false/absent forever from the real client's point of view. Fixed (prior pass)."}
  UpdateDefaultMailDomain: {wire: ok, errors: ok, state: ok, persist: ok}
  PutAccessControlRule: {wire: ok, errors: ok, state: ok, persist: ok, note: "GAP CLOSED: ImpersonationRoleIds/NotImpersonationRoleIds now accepted, stored on AccessControlRule, and echoed back by ListAccessControlRules; previously accepted by the real API (added after impersonation roles shipped) but not modeled anywhere on this backend."}
  DeleteAccessControlRule: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAccessControlEffect: {wire: ok, errors: ok, state: ok, persist: ok, note: "creation-order rule evaluation, CIDR matching verified (prior pass). GAP CLOSED this pass: now accepts ImpersonationRoleId (GetAccessControlEffectInput's fifth condition input) and evaluates it against each rule's ImpersonationRoleIds/NotImpersonationRoleIds, matching the same ALL-non-empty-conditions-must-match semantics as Actions/IpRanges/UserIds."}
  ListAccessControlRules: {wire: ok, errors: ok, state: ok, persist: ok, note: "response used IPRanges/NotIPRanges (wrong casing); real wire is IpRanges/NotIpRanges -- an SDK client would see empty slices always. Fixed (prior pass). Now also echoes ImpersonationRoleIds/NotImpersonationRoleIds."}
  CreateImpersonationRole: {wire: ok, errors: ok, state: ok, persist: ok}
  GetImpersonationRole: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateImpersonationRole: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteImpersonationRole: {wire: ok, errors: ok, state: ok, persist: ok}
  ListImpersonationRoles: {wire: ok, errors: ok, state: ok, persist: ok, note: "response field was Items; real field is Roles. Fixed."}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "RECLASSIFIED this pass: PARITY.md previously marked this 'persist: deferred' claiming b.tags was NOT in backendSnapshot. Independently re-verified by reading persistence.go directly: b.tags IS a field on backendSnapshot (json:\"tags\"), IS populated in Snapshot (Tags: b.tags), and IS restored in restoreCollectionMaps (b.tags = snap.Tags). This is confirmed by persistence_test.go's existing 'tags raw map' subtest, which round-trips a tag through Snapshot/Restore and passes. The prior audit's claim was stale/wrong, not a real gap -- tags were already correctly persisted before this pass touched anything. Also cascade-cleaned on DeleteUser/DeleteGroup/DeleteResource/DeleteOrganization this pass -- see leaks below."}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
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
  route-matcher: {status: ok, note: "single X-Amz-Target-prefix POST endpoint (WorkMailService.<Op>); MatchPriority/RouteMatcher/ExtractOperation all verified against service.HandleTarget's shared dispatcher, not just a Handler() unit test. buildOps() was decomposed into 4 category builder funcs (buildOrgAndEntityOps/buildMailboxAndDomainOps/buildAccessAndImpersonationOps/buildConfigAndTokenOps) merged via maps.Copy to remove a //nolint:funlen -- purely a structural split, every op is still in the one flat dispatch map and still dispatch-reachable through service.HandleTarget -> h.dispatch. Re-verified op count unchanged (92) via HandlerOpsLen and TestSDKCompleteness (still green)."}
  persistence: {status: ok, note: "Handler.Snapshot/Restore already delegate to backend (fixed in an earlier phase per persistence.go's doc comments) so cli.go's setupPersistence picks WorkMail up. Verified all 15 org-nested/composite-keyed tables + 3 registry tables + raw maps (including tags -- see TagResource note above) round-trip through backendSnapshot; version-mismatch discard-and-reset path present. Additive struct fields (User/MailDomain/AccessControlRule/Organization) don't require a snapshot-version bump: old snapshots decode the new fields as zero values, which is the correct behavior (a pre-upgrade org genuinely never had DKIM records, migration admin, etc.)."}
  error-mapping: {status: ok, note: "every backend error path wraps one of ErrNotFound/ErrConflict/ErrValidation/ErrLimitExceeded/ErrMailDomainState/ErrEntityState; handleError's switch covers all six plus isUnknownOp -- no bare fmt.Errorf that would fall through to InternalServiceError found."}
gaps:
  - "DescribeResource doesn't model BookingOptions / HiddenFromGlobalAddressList (types.BookedResource fields) -- not in this pass' declared 6 gaps; genuinely still open. Needs a bd issue."
  - "Organization.State is hardcoded to ACTIVE (org creation is synchronous); real AWS transitions through Creating/Active/etc, but nothing in this backend ever leaves an org in a non-terminal state, so this is a non-issue in practice, not a hidden bug. Left as-is (re-verified this pass, not fixed -- there is nothing to fix: no code path produces an incorrect State)."
  - "CreateOrganizationInput.EnableInteroperability is accepted on the wire (domainReq/createOrgReq) but discarded -- DescribeOrganization's InteroperabilityEnabled field is consequently always false. Found during this pass' field-diff of CreateOrganization/DescribeOrganization but out of the declared 6-gap scope; not fixed. Needs a bd issue."
deferred: []
# The single previously-deferred item (Tags persistence) was independently
# re-verified this pass and found to be NOT actually deferred -- see the
# TagResource ops entry above. No items are deferred as of this audit.
leaks: {status: fixed, note: "Found and fixed a real cascade-cleanup leak class this pass: DeleteUser/DeleteGroup/DeleteResource removed the entity from its own table but left ghost rows behind in aliases/globalAliases (CreateAlias-created aliases, not primary emails, which were already cleaned), mailbox permissions (both as the target entity AND as a grantee on another entity's mailbox), other groups' membership sets, and other resources' delegate lists, plus tags keyed by the entity's ARN -- an alias or ARN belonging to a deleted entity could never be reused by anything else in the org. Fixed via a shared cascadeCleanEntity helper (store.go) called from all three Delete* ops. DeleteOrganization had the same class of bug at org scope: its own doc comment said tags were 'deliberately left untouched', and globalAliases rows for the org's users/groups/resources were never swept either (both the org and everything that could reference them were already gone, so nothing would ever clean them) -- fixed via deleteTagsForOrg (ARN-prefix match) and deleteGlobalAliasesForOrg (OrgID-field scan). Regression tests in cascade_cleanup_test.go. Everything else: no goroutines, tickers, or background janitors in this service; AssumeImpersonationRole issues tokens into issuedTokens with no TTL sweep, matching MobileDeviceAccessOverride/PersonalAccessToken's pattern of storing ExpiresAt/ExpiresTime as data without an active expiry janitor -- consistent with the rest of the service, not a new leak."}
---

## Notes

Protocol: awsjson1.1, single POST endpoint, `X-Amz-Target: WorkMailService.<Op>`.
Route matcher is a simple header-prefix check (`service.PriorityHeaderExact`); every
op in `buildOps()` is reachable through `service.HandleTarget` → `h.dispatch`, not just
through direct `Handler()` unit-test calls.

### Bugs fixed in the prior pass (2026-07-12, all in `services/workmail/`)

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

### Gaps closed this pass (2026-07-23, all in `services/workmail/`)

All 6 gaps + the 1 deferred item listed in the 2026-07-12 PARITY.md were addressed:

1. **List filters** (`users.go`/`groups.go`/`resources.go`/`interfaces.go`/
   `handler_users.go`/`handler_groups.go`/`handler_resources.go`): `ListUsers`,
   `ListGroups`, `ListResources`, and `ListGroupsForEntity` all accept a `Filters`
   object on the wire (`ListUsersFilters`/`ListGroupsFilters`/`ListResourcesFilters`/
   `ListGroupsForEntityFilters`) that was previously parsed but never applied — a real
   client doing a prefix/state search got back the full unfiltered page. Added
   `UserFilter`/`GroupFilter`/`ResourceFilter` structs plus
   `userMatchesFilter`/`groupMatchesFilter`/`resourceMatchesFilter` (name/email prefix
   + state, all AND'd) and threaded `groupNamePrefix` through
   `ListGroupsForEntity` (its one filter dimension).
2. **`PutAccessControlRule`/`GetAccessControlEffect` impersonation conditions**
   (`access_control.go`/`interfaces.go`/`handler_access_control.go`): `AccessControlRule`
   now carries `ImpersonationRoleIDs`/`NotImpersonationRoleIDs` (wire keys
   `ImpersonationRoleIds`/`NotImpersonationRoleIds`), stored by `PutAccessControlRule`,
   echoed by `ListAccessControlRules`, and evaluated by `GetAccessControlEffect` against
   its new `ImpersonationRoleId` input (added to `matchesUserAndImpersonation`).
3. **`DescribeUser`/`CreateUser`/`UpdateUser` profile fields** (`interfaces.go`/
   `users.go`/`handler_users.go`): `User` gained `City`, `Company`, `Country`,
   `Department`, `Initials`, `JobTitle`, `Office`, `Street`, `Telephone`, `ZipCode`,
   `IdentityProviderIdentityStoreID`, `IdentityProviderUserID`,
   `HiddenFromGlobalAddressList`, `MailboxProvisionedDate`, `MailboxDeprovisionedDate`.
   `CreateUser`/`UpdateUser` now accept `CreateUserParams`/`UpdateUserParams` (matching
   `CreateUserInput`/`UpdateUserInput`'s full field sets, replacing the old 5- and
   5-positional-arg signatures) and `DescribeUser`'s response wires all of it.
   `MailboxProvisionedDate`/`MailboxDeprovisionedDate` are set in
   `RegisterToWorkMail`/`DeregisterFromWorkMail` alongside `EnabledDate`/`DisabledDate`.
4. **`GetMailDomain` DKIM/Records** (`mail_domains.go`/`organizations.go`/
   `handler_mail_domains.go`): added `DkimVerificationStatus` to `MailDomain` and a
   `dnsRecordsForDomain` helper producing the recommended `Records` list (MX + SPF TXT +
   autodiscover CNAME + 3 DKIM CNAMEs, matching `types.DnsRecord`'s
   `{Hostname,Type,Value}` shape) wired into both `RegisterMailDomain` (status
   `PENDING`) and `CreateOrganization`'s default + client-specified domains (status
   `VERIFIED`, matching their pre-existing `OwnershipVerificationStatus`).
5. **`DescribeOrganization` MigrationAdmin** (`interfaces.go`/`handler_organizations.go`):
   added the field. Field-diffed the whole `v1.37.2` SDK surface and confirmed no
   operation ever sets it in real AWS's public API either (interoperability/migration is
   admin-console-only) — the field is now modeled and wired, correctly always empty.
6. **`Organization.State` hardcoded to `ACTIVE`**: re-verified, confirmed non-issue (no
   code path ever produces an incorrect `State`); left as-is per the 2026-07-12 note.
7. **Tags "deferred" reclassified to `ok`**: the 2026-07-12 PARITY.md's `deferred` entry
   claimed `b.tags` was not in `backendSnapshot`. Read `persistence.go` directly this
   pass: `b.tags` IS a `backendSnapshot` field, IS populated in `Snapshot`, and IS
   restored in `restoreCollectionMaps` — already correct, not touched. The claim was
   stale/wrong, not a real gap. See the `TagResource` ops entry for the full
   verification trail.

### Leak fixed this pass (2026-07-23)

`DeleteUser`/`DeleteGroup`/`DeleteResource` (`store.go`'s new `cascadeCleanEntity`,
called from `users.go`/`groups.go`/`resources.go`) and `DeleteOrganization`
(`store.go`'s new `deleteTagsForOrg`/`deleteGlobalAliasesForOrg`, called from
`organizations.go`) previously left ghost rows behind in `aliases`/`globalAliases`,
mailbox `permissions` (as target entity AND as grantee), `groupMembers`, `delegates`,
and `tags` after an entity or whole organization was deleted — see the `leaks` block
above for the full description. Regression tests in `cascade_cleanup_test.go`.

### `//nolint:funlen` removed this pass (2026-07-23)

`handler.go`'s `buildOps` carried the service's one banned nolint
(`//nolint:funlen // existing issue.`, 141 lines / 92 ops). Decomposed into
`buildOrgAndEntityOps`/`buildMailboxAndDomainOps`/`buildAccessAndImpersonationOps`/
`buildConfigAndTokenOps`, merged via `maps.Copy` — purely structural, every op is still
in the one flat dispatch map. (An initial 5-way split triggered `dupl` on two
similarly-shaped map-literal builders despite entirely different keys/handlers; merged
back down to 4 to remove the coincidental structural match rather than paper over it
with another nolint.)

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
- Before marking a PARITY.md `deferred`/`gap` entry as still-open, actually re-read the
  source it claims is wrong — the "Tags not persisted" deferred entry in the
  2026-07-12 PARITY.md was stale (persistence.go already persisted tags correctly) and
  would have been carried forward as a phantom gap if not independently re-verified.
- `cascadeCleanEntity` (store.go) is the one place that knows how to fully unlink an
  entity from every collection that references it by ID (aliases, permissions as either
  side, group memberships, resource delegate lists, tags). Any *new* op that creates
  another entity->entity reference (a new collection keyed by another entity's ID) needs
  a corresponding cleanup line added there, or it becomes the next leak.
