---
# PARITY MANIFEST SCHEMA — copy to services/<svc>/PARITY.md, fill, keep updated.
# Purpose: record audit state so the NEXT audit diffs the delta instead of rescanning.
# Re-audit protocol: `git diff <last_audit_commit>..HEAD -- services/<svc>/` for local drift,
# AND check the SDK module for ops added since sdk_version. Only audit changed/new surface;
# trust rows marked ok whose files are unchanged since last_audit_commit.
service: organizations
sdk_module: aws-sdk-go-v2/service/organizations@v1.53.5
last_audit_commit: 012f98aa
last_audit_date: 2026-07-23
overall: A            # this pass: closed the 2 previously-deferred validation gaps (policy content
                      # size/syntax validation, tag validation) + epochSeconds reuse-hygiene fix
# Per-op or per-op-family status. Values: ok | partial | gap | deferred.
# wire=response/request shape vs SDK; errors=code+HTTP status; state=real mutate/read; persist=in backendSnapshot.
ops:
  CreateOrganization: {wire: ok, errors: ok, state: fixed, persist: ok, note: "management account ID now == backend.AccountID() (caller identity), not a synthetic counter-derived ID -- was previously fabricating 000000000001 regardless of the configured/caller account, breaking cross-service account-identity consistency (e.g. vs STS)"}
  DescribeOrganization: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteOrganization: {wire: ok, errors: ok, state: ok, persist: ok, note: "rejects when non-management member accounts remain, matches AWS"}
  EnableAllFeatures: {wire: ok, errors: ok, state: ok, persist: ok, note: "returns a synthetic ENABLE_ALL_FEATURES handshake in ACCEPTED state (no real multi-account approval flow needed for single-account emulation)"}
  ListRoots: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateAccount: {wire: ok, errors: ok, state: ok, persist: ok, note: "CreateAccountStatus transitions straight to SUCCEEDED (no stuck IN_PROGRESS poll trap)"}
  CreateGovCloudAccount: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeCreateAccountStatus: {wire: ok, errors: ok, state: ok, persist: ok}
  ListCreateAccountStatus: {wire: fixed, errors: ok, state: ok, persist: ok, note: "MaxResults/NextToken were parsed into the request but never applied -- handler always returned the full unfiltered set. Now wired through pkgs/page.New."}
  DescribeAccount: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAccounts: {wire: ok, errors: ok, state: ok, persist: ok, note: "already paginated via pkgs/page"}
  RemoveAccountFromOrganization: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascades policyTargets/tags/delegated-admin cleanup"}
  MoveAccount: {wire: ok, errors: ok, state: ok, persist: ok, note: "validates current parent == SourceParentId and dest existence before mutating both index directions"}
  CloseAccount: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateOrganizationalUnit: {wire: ok, errors: ok, state: ok, persist: ok, note: "depth-limit (root=0, OUs 1-5) and O(1) sibling-name uniqueness enforced"}
  DescribeOrganizationalUnit: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteOrganizationalUnit: {wire: ok, errors: ok, state: ok, persist: ok, note: "rejects non-empty OUs (child accounts or child OUs)"}
  UpdateOrganizationalUnit: {wire: ok, errors: ok, state: ok, persist: ok}
  ListOrganizationalUnitsForParent: {wire: ok, errors: ok, state: ok, persist: ok, note: "already paginated"}
  ListAccountsForParent: {wire: fixed, errors: ok, state: ok, persist: ok, note: "request/response DTOs already declared MaxResults/NextToken but the handler ignored both and returned everything -- wired page.New to match sibling ops"}
  ListParents: {wire: ok, errors: ok, state: ok, persist: ok}
  ListChildren: {wire: ok, errors: ok, state: ok, persist: ok, note: "already paginated"}
  CreatePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "content now validated: json.Valid() syntax check -> MalformedPolicyDocumentException, and per-policy-type size quota (SCP/RCP 5120, TAG/BACKUP/DECLARATIVE_POLICY_EC2 10000, AISERVICES_OPT_OUT_POLICY 2500 chars; CHATBOT_POLICY/SECURITYHUB_POLICY default to 10000 as a best-effort value, not independently verified against an AWS quotas doc) -> ConstraintViolationException(POLICY_CONTENT_LIMIT_EXCEEDED). Tags param now validated via validateNewTags before any mutation (see TagResource note)."}
  DescribePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdatePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "Content, when supplied, now goes through the same validatePolicyContent() as CreatePolicy (syntax+size) before ANY field (name/description/content) is mutated, matching AWS's atomic per-request failure semantics -- previously content was accepted unvalidated."}
  DeletePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "rejects deletion while still attached to any target"}
  ListPolicies: {wire: ok, errors: ok, state: ok, persist: ok, note: "requires non-empty Filter, matches AWS; already paginated"}
  AttachPolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "enforces AWS's 5-policies-per-type-per-target limit and duplicate-attachment rejection"}
  DetachPolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  ListPoliciesForTarget: {wire: fixed, errors: ok, state: ok, persist: ok, note: "MaxResults field was missing from the request DTO entirely and results were never truncated; added field + wired page.New"}
  ListTargetsForPolicy: {wire: fixed, errors: ok, state: ok, persist: ok, note: "same gap as ListPoliciesForTarget -- fixed the same way"}
  EnablePolicyType: {wire: ok, errors: ok, state: ok, persist: ok}
  DisablePolicyType: {wire: ok, errors: ok, state: ok, persist: ok, note: "rejects disabling a type while policies of that type remain attached anywhere"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok, note: "Tags now validated (validateNewTags in tags.go) before merge: (1) reserved 'aws:' key prefix (case-insensitive) -> InvalidInputException(INVALID_SYSTEM_TAGS_PARAMETER); (2) duplicate key within one request's Tags list -> InvalidInputException(DUPLICATE_TAG_KEY); (3) resulting tag count > 50 (AWS's documented per-resource limit) -> ConstraintViolationException(MAX_TAG_LIMIT_EXCEEDED). Same validation now also gates CreateAccount/CreateGovCloudAccount/CreateOrganizationalUnit/CreatePolicy's Tags parameter, called before any resource is created so a bad tag list leaves nothing behind (matches AWS's 'the entire request fails' doc note on those Tags params)."}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: fixed, errors: ok, state: ok, persist: ok, note: "response DTO already declared NextToken but it was never populated and the request had no NextToken field at all (real AWS ListTagsForResource paginates via NextToken, no MaxResults param); added the field and wired page.New with the service default page size"}
  EnableAWSServiceAccess: {wire: ok, errors: ok, state: ok, persist: ok}
  DisableAWSServiceAccess: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAWSServiceAccessForOrganization: {wire: fixed, errors: ok, state: ok, persist: ok, note: "handler previously discarded the request body entirely (`_ []byte`), so MaxResults/NextToken were unreachable; added listAWSServiceAccessRequest + page.New wiring, guarded for empty body (matches ListHandshakesForAccount's pattern) since real SDK clients still send at least '{}'"}
  RegisterDelegatedAdministrator: {wire: ok, errors: ok, state: ok, persist: ok, note: "requires EnableAWSServiceAccess first, matches AWS's ErrServiceNotEnabled behavior"}
  DeregisterDelegatedAdministrator: {wire: ok, errors: ok, state: ok, persist: ok}
  ListDelegatedAdministrators: {wire: fixed, errors: ok, state: ok, persist: ok, note: "MaxResults field missing from request DTO, results never truncated; added field + wired page.New"}
  ListDelegatedServicesForAccount: {wire: fixed, errors: ok, state: ok, persist: ok, note: "same gap, fixed the same way"}
  AcceptHandshake: {wire: ok, errors: ok, state: ok, persist: ok}
  CancelHandshake: {wire: ok, errors: ok, state: ok, persist: ok}
  DeclineHandshake: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeHandshake: {wire: ok, errors: ok, state: ok, persist: ok}
  InviteAccountToOrganization: {wire: ok, errors: ok, state: ok, persist: ok}
  LeaveOrganization: {wire: ok, errors: ok, state: ok, persist: ok}
  ListHandshakesForAccount: {wire: ok, errors: ok, state: ok, persist: ok, note: "already paginated; empty-body-tolerant parsing pattern (len(body)>0 guard) reused for the new ListAWSServiceAccessForOrganization fix"}
  ListHandshakesForOrganization: {wire: ok, errors: ok, state: ok, persist: ok, note: "already paginated"}
  DeleteResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  PutResourcePolicy: {wire: ok, errors: ok, state: ok, persist: ok}
  DescribeEffectivePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "walks the OU/root policy chain and merges per policy-type semantics (SCP intersection-style vs tag-style override)"}
  ListAccountsWithInvalidEffectivePolicy: {wire: ok, errors: ok, state: ok, persist: ok, note: "correctly always empty -- this backend performs no policy-schema validation so no account can ever have an invalid effective policy; NOT a stub, a correct void result (parity-principles rule 4)"}
  ListEffectivePolicyValidationErrors: {wire: ok, errors: ok, state: ok, persist: ok, note: "same as above -- correct void result, not a stub"}
  DescribeResponsibilityTransfer: {wire: ok, errors: ok, state: ok, persist: ok}
  InviteOrganizationToTransferResponsibility: {wire: ok, errors: ok, state: ok, persist: ok}
  ListInboundResponsibilityTransfers: {wire: ok, errors: ok, state: ok, persist: ok}
  ListOutboundResponsibilityTransfers: {wire: ok, errors: ok, state: ok, persist: ok}
  TerminateResponsibilityTransfer: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateResponsibilityTransfer: {wire: ok, errors: ok, state: ok, persist: ok}
# Families audited as a group (when per-op is impractical):
families:
  error_table: {status: ok, note: "getErrorTable() in handler.go covers all 28 sentinel errors defined in backend.go one-to-one; no gap that would surface as a 500 InternalFailure for a known error condition"}
  persistence: {status: ok, note: "Handler exposes Snapshot(ctx)/Restore(ctx,[]byte) exactly, delegating to InMemoryBackend's own Snapshot/Restore -- correctly registered by cli.go's setupPersistence. Versioned snapshot format (organizationsSnapshotVersion) discards incompatible old snapshots cleanly instead of partially decoding them."}
  arn_shapes: {status: ok, note: "all ARNs built via pkgs/arn.Build, organization/account/root/ou/policy/resource-policy/handshake resource paths verified against real SDK doc comments (global service, no region segment)"}
  id_formats: {status: ok, note: "12-digit account IDs, ou- root- p- h- o- prefixes match AWS patterns"}
  timestamps: {status: ok, note: "epochSeconds(t) in models.go now delegates to pkgs/awstime.Epoch (was a local float64(t.Unix()) reimplementation that truncated sub-second precision). Wire shape (JSON number, epoch seconds) unchanged and still correct; this closes the reuse-hygiene gap flagged in the prior audit."}
gaps:                     # known divergences NOT fixed — link bd issue ids
  - "ListAccountsWithInvalidEffectivePolicy / ListEffectivePolicyValidationErrors don't paginate (MaxResults/NextToken silently accepted-but-ignored in the same way the 6 fixed ops used to be), but both are provably always-empty results given no real policy-schema validation exists, so pagination there is moot until schema validation is implemented (no bd issue filed yet)"
  - "AWS auto-creates and attaches a default 'FullAWSAccess' SCP to the root when the SERVICE_CONTROL_POLICY policy type is enabled (or org created with ALL features); this backend does not fabricate that default policy, so ListPolicies/ListPoliciesForTarget won't show it. Deep AWS behavior detail, not flagged as broken since no client mutation is silently dropped -- documented here for the next auditor (no bd issue filed yet)"
  - "Policy content size limits are modeled at AWS's DEFAULT per-type quota only (e.g. SCP/RCP 5120 chars); this backend does not model the service-quota-increase path (SCP up to 10240/20480 via a quota request) since there is no quota-management API call being emulated here. A client that successfully requested a real quota increase would see this backend reject documents AWS would accept. CHATBOT_POLICY/SECURITYHUB_POLICY default to the same 10000-char ceiling as BACKUP/TAG/DECLARATIVE_POLICY_EC2 as a best-effort value -- not found in the current orgs_reference_limits.md doc snapshot, so not independently verified (no bd issue filed yet)."
  - "Tag key/value length limits (AWS also caps individual tag key/value string lengths, not just the 50-tags-per-resource count) are not validated -- only count, duplicate-key, and reserved-prefix are enforced this pass (no bd issue filed yet)"
  - "NEW since v1.50.4 (found by gopherstack-u8my's pin-correction pass, not fixed): Account gained a Paths []string field (the account's location paths in the org hierarchy) and OrganizationalUnit gained a Path *string field (its own location path). gopherstack does not compute or populate either on DescribeAccount/ListAccounts/DescribeOrganizationalUnit/UpdateOrganizationalUnit/ListOrganizationalUnitsForParent -- silently omitted from responses. (needs bd issue)"
deferred: []               # both previously-deferred items (policy content validation, tag validation)
                            # were implemented and field-diffed this pass -- see CreatePolicy/UpdatePolicy/
                            # TagResource notes above and the residual-limitation gaps listed above.
leaks: {status: clean, note: "no goroutines, timers, or background janitors in this service; expireStaleHandshakesLocked runs synchronously inside the write lock on relevant ops, not on a ticker"}
---

## Notes

Freeform: AWS-behavior specifics worth remembering, and any "looks-wrong-but-correct" traps
so the next auditor doesn't re-flag them.

- **Protocol**: `awsjson1.1` (single POST endpoint, `X-Amz-Target: AWSOrganizationsV20161128.<Op>`
  dispatch in `handler.go`'s `RouteMatcher`/`ExtractOperation`). `GetSupportedOperations()` was
  cross-checked 1:1 against every `api_op_*.go` file in
  `aws-sdk-go-v2/service/organizations@v1.50.4` -- all 61 real ops are routed and reachable
  (`dispatch` → `dispatchOrg`/`dispatchAccount`/`dispatchOU`/`dispatchPolicy`/`dispatchMisc`/
  `dispatchNewOps` → `dispatchHandshakeOps`/`dispatchTransferOps`). No missing ops, no orphaned
  stub registrations to clean up.

- **Real bug #1 (fixed) -- management account identity**: `CreateOrganization` previously
  minted the org's management account ID from a local counter (`newAccountID(1)` ==
  `"000000000001"`), completely independent of `b.accountID` (the account this backend was
  constructed with via `service.AccountRegionOrDefault`, i.e. whatever account other services
  in this same gopherstack instance report as the caller identity, e.g. STS
  `GetCallerIdentity`). Real AWS: the account that calls `CreateOrganization` *becomes* the
  management account -- same ID. A client cross-checking account identity across services
  (e.g. comparing `sts:GetCallerIdentity` against `organizations:DescribeOrganization`'s
  `MasterAccountId`, which Terraform's provider and many IaC tools do) would see a mismatch.
  Fixed: `mgmtAcctID := b.accountID`. `accountCounter` still starts at
  `managementAccountCounter` (1) afterward so member accounts get sequential 12-digit IDs
  unrelated to the (now real) management account ID -- no behavior change there.

- **Real bug #2 (fixed) -- systemic pagination-wiring gap**: 6 of the 8 non-trivial list ops
  (`ListAccountsForParent`, `ListPoliciesForTarget`, `ListTargetsForPolicy`,
  `ListDelegatedAdministrators`, `ListCreateAccountStatus`, `ListDelegatedServicesForAccount`)
  had response DTOs that already declared a `NextToken` field (clearly intended for
  pagination, matching the pattern used correctly by `ListAccounts`,
  `ListOrganizationalUnitsForParent`, `ListChildren`, `ListPolicies`,
  `ListHandshakesForAccount`, `ListHandshakesForOrganization`) but the handlers never
  populated it and never truncated at `MaxResults`, silently returning the entire unpaginated
  result set every time. `ListTagsForResource` and `ListAWSServiceAccessForOrganization` had
  the same gap plus were missing `NextToken`/`MaxResults` fields from their request DTOs
  entirely (`ListAWSServiceAccessForOrganization`'s handler didn't even parse the request
  body). Fixed all 8 by wiring `pkgs/page.New` the same way the already-correct sibling ops
  do. Added `Test_ListOps_HonorMaxResultsAndNextToken` in
  `handler_pagination_gaps_test.go` covering MaxResults truncation + NextToken continuation
  for each.

- **Trap for next auditor**: `ListAccountsWithInvalidEffectivePolicy` and
  `ListEffectivePolicyValidationErrors` always return empty slices. This is **correct**, not a
  stub -- this backend does no real JSON-schema policy validation, so no account or policy can
  ever be "invalid". Per parity-principles rule 4, confirmed by reading the backend method
  before flagging.

- **CreateAccount lifecycle**: `CreateAccountStatus.State` goes straight to `SUCCEEDED` in the
  same call (no stuck `IN_PROGRESS` that would make a real client poll
  `DescribeCreateAccountStatus` forever) -- correct emulator behavior for this bug class called
  out in parity-principles.

- **Error table**: `getErrorTable()` (handler.go) has a 1:1 entry for all 28 sentinel errors
  declared in backend.go; verified there is no gap that would surface as a raw 500
  `InternalFailure` for a condition that should have a specific AWS exception code.

- **Persistence**: `Handler.Snapshot`/`Restore` delegate directly to
  `InMemoryBackend.Snapshot`/`Restore` with the exact method signatures
  `Snapshot(ctx context.Context) []byte` / `Restore(ctx context.Context, []byte) error` that
  `cli.go`'s `setupPersistence` requires -- correctly registered, not the silent-unregistration
  bug class fixed elsewhere in the ~12-service sweep. Snapshot format is versioned
  (`organizationsSnapshotVersion`) and discards incompatible snapshots cleanly rather than
  partially decoding them.

- **This pass (2026-07-23) -- closed both previously-deferred items**:
  1. **Policy content validation** (`policies.go`): `CreatePolicy`/`UpdatePolicy` now run
     `validatePolicyContent(content, policyType)` before mutating any state.
     `json.Valid([]byte(content))` catches non-JSON content ->
     `MalformedPolicyDocumentException` (real AWS exception type, field-diffed against
     `types.MalformedPolicyDocumentException` in the SDK). A per-policy-type character-count
     ceiling (SCP/RCP 5120, TAG/BACKUP/DECLARATIVE_POLICY_EC2 10000,
     AISERVICES_OPT_OUT_POLICY 2500, per the Organizations quotas reference; CHATBOT_POLICY/
     SECURITYHUB_POLICY default to 10000 as an unverified best-effort value -- see gaps) ->
     `ConstraintViolationException` with `Reason: POLICY_CONTENT_LIMIT_EXCEEDED`, matching
     `types.ConstraintViolationExceptionReasonPolicyContentLimitExceeded` in the SDK enum.
     `UpdatePolicy` validates content *before* applying name/description so a rejected update
     doesn't partially mutate the policy (`TestUpdatePolicy_MalformedContent` asserts this).
  2. **Tag validation** (`tags.go`'s new `validateNewTags` helper, shared by `TagResource`,
     `CreateAccount`, `CreateGovCloudAccount`, `CreateOrganizationalUnit`, and `CreatePolicy`):
     rejects tag keys with the case-insensitive `aws:` reserved prefix
     (`InvalidInputException`/`INVALID_SYSTEM_TAGS_PARAMETER`, matching
     `types.InvalidInputExceptionReasonInvalidSystemTagsParameter`), rejects a duplicate key
     within one call's tag list (`InvalidInputException`/`DUPLICATE_TAG_KEY`, matching
     `types.InvalidInputExceptionReasonDuplicateTagKey`), and enforces AWS's documented
     50-tags-per-resource cap against the *merged* (existing + new) key set
     (`ConstraintViolationException`/`MAX_TAG_LIMIT_EXCEEDED`, matching
     `types.ConstraintViolationExceptionReasonMaxTagLimitExceeded`). Validation runs before any
     resource is created/mutated, so a rejected Tags parameter leaves nothing behind --
     verified by `TestCreateAccount_ReservedTagPrefixRejected`,
     `TestCreateOrganizationalUnit_DuplicateTagKeyRejected`,
     `TestCreatePolicy_ReservedTagPrefixRejected`, and
     `TestTagResource_MaxTagLimitExceeded_AcrossCalls`.
  3. **Reuse-hygiene**: `epochSeconds()` in `models.go` now delegates to `pkgs/awstime.Epoch`
     instead of reimplementing `float64(t.Unix())` (closes the prior audit's flagged gap;
     sub-second precision is now preserved, though every call site in this service only ever
     passes whole-second `time.Now()` values so there's no observable wire-format change).
  4. Not modeled (see `gaps`): the service-quota-increase path for policy size (a client that
     requested and received a real SCP size-limit increase would see this backend reject
     documents AWS would now accept), and per-tag key/value length limits (only count,
     duplicate-key, and reserved-prefix are enforced).
