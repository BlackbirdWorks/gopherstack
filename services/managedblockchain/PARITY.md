---
service: managedblockchain
sdk_module: aws-sdk-go-v2/service/managedblockchain@v1.34.4
last_audit_commit: a073b2b1
last_audit_date: 2026-08-20
overall: A
ops:
  CreateNetwork: {wire: fixed, errors: fixed, state: fixed, persist: ok, note: "FrameworkConfiguration.Fabric.Edition, VpcEndpointServiceName, Framework restricted to HYPERLEDGER_FABRIC; see Notes"}
  GetNetwork: {wire: fixed, errors: ok, state: ok, persist: ok, note: "now returns FrameworkAttributes.Fabric + VpcEndpointServiceName"}
  ListNetworks: {wire: fixed, errors: ok, state: ok, persist: ok, note: "server-side pagination now implemented via pkgs/page; see Notes"}
  CreateMember: {wire: fixed, errors: fixed, state: fixed, persist: ok, note: "InvitationId now required and validated against a real PENDING invitation for this network, consumed (ACCEPTED) on success; MemberConfiguration.FrameworkConfiguration.Fabric.AdminUsername/AdminPassword required and validated, KmsKeyArn accepted; see Notes"}
  GetMember: {wire: fixed, errors: ok, state: ok, persist: ok, note: "now returns FrameworkAttributes.Fabric.AdminUsername/CaEndpoint + KmsKeyArn; LogPublishingConfiguration.Fabric.CaLogs wire key fixed CloudWatch->Cloudwatch, see 2026-08-20 Notes"}
  ListMembers: {wire: fixed, errors: ok, state: ok, persist: ok, note: "server-side pagination now implemented"}
  DeleteMember: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascades to member's nodes, matching real AWS"}
  UpdateMember: {wire: fixed, errors: ok, state: ok, persist: ok, note: "LogPublishingConfiguration.Fabric.CaLogs.Cloudwatch request/response wire key fixed, see 2026-08-20 Notes"}
  CreateNode: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "NodeConfiguration.StateDB accepted (defaults CouchDB), KmsKeyArn inherited from owning member; see Notes"}
  GetNode: {wire: fixed, errors: ok, state: ok, persist: ok, note: "now returns FrameworkAttributes.Fabric.PeerEndpoint/PeerEventEndpoint + StateDB + KmsKeyArn; LogPublishingConfiguration.Fabric.{ChaincodeLogs,PeerLogs} wire key fixed CloudWatch->Cloudwatch, see 2026-08-20 Notes"}
  ListNodes: {wire: fixed, errors: ok, state: ok, persist: ok, note: "server-side pagination now implemented"}
  DeleteNode: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateNode: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "MemberId moved from a required query parameter to the required JSON body field a real client actually sends; LogPublishingConfiguration.Fabric.{ChaincodeLogs,PeerLogs}.Cloudwatch wire key fixed; see 2026-08-20 Notes"}
  CreateProposal: {wire: ok, errors: ok, state: ok, persist: ok}
  GetProposal: {wire: ok, errors: ok, state: ok, persist: ok}
  ListProposals: {wire: fixed, errors: ok, state: ok, persist: ok, note: "server-side pagination now implemented; fabricated ProposalSummary.NetworkId member removed, see 2026-08-20 Notes"}
  VoteOnProposal: {wire: ok, errors: ok, state: ok, persist: ok, note: "tallies votes and resolves APPROVED/REJECTED against VotingPolicy; not a disguised no-op"}
  ListProposalVotes: {wire: fixed, errors: ok, state: ok, persist: ok, note: "server-side pagination now implemented"}
  ListInvitations: {wire: fixed, errors: ok, state: ok, persist: ok, note: "server-side pagination now implemented; fabricated Invitation.NetworkId/NetworkName top-level members removed, see 2026-08-20 Notes"}
  RejectInvitation: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateAccessor: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAccessor: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAccessor: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAccessors: {wire: fixed, errors: ok, state: ok, persist: ok, note: "server-side pagination now implemented"}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  network: {status: fixed, note: "CreateNetwork/GetNetwork/ListNetworks field-diffed against types.go/api_op_*.go/validators.go; FrameworkAttributes+VpcEndpointServiceName+Framework restriction added, see Notes"}
  member: {status: fixed, note: "MemberConfiguration.FrameworkConfiguration was entirely unmodeled (a real, required field per validateMemberFabricConfiguration) -- now implemented with real server-side validation + FrameworkAttributes/KmsKeyArn on responses, see Notes; distinct wire structs confirmed for Member vs MemberSummary, each matching its own live deserializer case list, see 2026-08-20 Notes"}
  node: {status: fixed, note: "StateDB/KmsKeyArn/FrameworkAttributes were entirely unmodeled -- now implemented; the prior audit's node-routing-URI fix remains correct and unchanged; UpdateNode's MemberId location bug and the CloudWatch/Cloudwatch key bug fixed this pass, see 2026-08-20 Notes"}
  proposal: {status: fixed, note: "CreateProposal/GetProposal/ListProposals/ListProposalVotes/VoteOnProposal verified; vote tallying and threshold-based APPROVED/REJECTED transition confirmed real (not a stub); ListProposals/ListProposalVotes now paginate; fabricated ProposalSummary.NetworkId removed this pass, see 2026-08-20 Notes"}
  invitation: {status: fixed, note: "ListInvitations/RejectInvitation only -- correctly no CreateInvitation op (real AWS has none either; invitations are created only as a side effect of an approved proposal's Invitations actions, which executeProposalActionsLocked implements); ListInvitations now paginates; fabricated top-level NetworkId/NetworkName removed this pass, see 2026-08-20 Notes"}
  accessor: {status: ok, note: "CreateAccessor/GetAccessor/DeleteAccessor/ListAccessors verified; ListAccessors now paginates; Accessor vs AccessorSummary wire structs confirmed distinct and each matches its own live deserializer, see 2026-08-20 Notes"}
  tags: {status: ok, note: "TagResource/UntagResource/ListTagsForResource verified against /tags/{ResourceArn} shape and ARN-keyed lookup"}
gaps:
  - "Member.IsOwned is always true, even for a member created via CreateMember (i.e. joining via invitation, which in real AWS is not owned by the joining account's original network-owner relationship). gopherstack has no multi-account model to distinguish an owned member from an invited one, so this is a reasonable simplification, not flagged as a bug to fix (gopherstack-u84u re-reviewed this alongside InvitationId; InvitationId itself is now real, see Notes #8)."
  - "No artificial service quotas (max members per network, max nodes per member, max networks per account) are enforced, so ResourceLimitExceededException is never returned. Consistent with this emulator's general no-limits style elsewhere; not treated as a bug."
  - "Network.FrameworkAttributes.Ethereum and Node.FrameworkAttributes.Ethereum are not modeled. gopherstack-u84u answered the design question this was deferred under: real AWS's CreateNode documents exactly one well-known public Ethereum NetworkId, \"n-ethereum-mainnet\" (aws-sdk-go-v2 managedblockchain api_op_CreateNode.go:44-47 and api_op_DeleteNode.go:36, v1.34.4 -- confirmed NOT invented; older SDKs additionally listed now-sunset n-ethereum-goerli/n-ethereum-rinkeby testnets, absent from this pin), with FrameworkAttributes.Ethereum.ChainId documented as \"1\" for mainnet (types/types.go:538-547's NetworkEthereumAttributes). ListNetworks/GetNetwork both self-document \"Applies to Hyperledger Fabric and Ethereum\", so real AWS does surface this network through both once an account has a node on it. Seeding the network itself would therefore be honest (a real, stable constant, not invented). Still deferred: CreateNode's real MemberId is documented \"Applies only to Hyperledger Fabric\" (api_op_CreateNode.go:56-58) -- Ethereum nodes have no owning member -- but gopherstack's Node storage is keyed by (networkID, memberID, nodeID) (nodeKey in store_setup.go) and CreateNode already requires MemberId unconditionally (ErrMissingNodeMemberID) for its one supported framework. Making CreateNode against Ethereum reachable needs a memberless Node storage path, not just a seeded network row -- a real structural change, not an adjacent fix."
deferred: []
leaks: {status: clean, note: "no goroutines/janitors in this service; InMemoryBackend.mu is the single coarse lockmetrics.RWMutex guarding every map/store.Table, consistent with pkgs-catalog.md's locking rule. The new paginate() helper (pagination.go) and buildNetworkFrameworkAttributes/buildMemberFrameworkAttributes/CreateNode's FrameworkAttributes synthesis are all pure functions operating on already-locked state or post-lock snapshots -- no new lock paths introduced."}
---

## 2026-08-30 (request-field axis sweep, gopherstack-4shm's class)

`cmd/reqfieldscan` flagged `ClientRequestToken` on all 5 create ops
(`CreateNetwork`/`CreateMember`/`CreateNode`/`CreateProposal`/`CreateAccessor`)
as declared-but-never-read. This service does not use `service.JSONOpFunc`/
`service.WrapOp` at all (it's REST-routed through `dispatch`/
`dispatchNetworkOps`/etc., all literal `json.Unmarshal` decodes), so the
scan's coverage guard is silent here by construction (see the tool's own
`packageMentionsJSONOpFunc` gate) -- not a blind spot, confirmed by reading
that condition rather than inferring from the guard's silence.

**Real bug, fixed:** all 5 ops' Go SDK struct doc comments mark
`ClientRequestToken` "This member is required", and `validators.go` (v1.34.4)
enforces it client-side for every one (`validateOpCreateNetworkInput`,
`...CreateMemberInput`, `...CreateNodeInput`, `...CreateProposalInput`,
`...CreateAccessorInput`, all calling `smithy.NewErrParamRequired`). A real
`aws-sdk-go-v2` client never omits it -- the SDK's idempotency-token
middleware (`idempotencyToken_initializeOp<Op>`) auto-fills it when unset --
but gopherstack accepted a raw HTTP request missing it outright, certifying a
call the real service rejects. Fixed: each of the 5 handlers now returns
`InvalidRequestException` (`ErrMissingClientRequestToken`, `errors.go`) when
the field is empty, checked immediately after JSON decode. `~50` pre-existing
tests across `accessors_test.go`, `framework_attributes_test.go`,
`members_test.go`, `networks_test.go`, `nodes_test.go`, `pagination_test.go`,
`proposals_test.go`, `proposals_voting_test.go`, `store_test.go`,
`tags_test.go` built request bodies with no `ClientRequestToken` at all (the
field being silently ignored meant nothing ever caught it) and were updated
to include one; none had an assertion weakened -- one,
`TestHandler_CreateAccessor`'s "empty body still creates accessor" case, was
corrected from asserting 200/`AccessorId` (matching the bug) to asserting 400
(matching real AWS), since an empty body genuinely has no
`ClientRequestToken`. New test: `client_request_token_test.go`
(`TestHandler_CreateOps_MissingClientRequestToken`), confirmed failing
(200/200/200/200/404 instead of 400) against unmodified code before the fix
landed.

**Not implemented (layer-boundary, reported not fixed):** real AWS's
documented purpose for this token is retry-safety -- "allows failed
Create<X> requests to be retried without the risk of running the operation
twice" -- implying idempotency-token *deduplication* (a retried call with the
same token should return the original result, not create a second resource).
gopherstack does not implement that: only presence is now validated, not the
value. This repo has an established pattern for exactly this
(`services/acm`'s `idempotencyMap`/`certIdempotencyEntry`,
`services/acmpca`'s `lookupIdempotentCert`/`idempotentResourceARN`), but
replicating it here means a new per-resource-type dedup store (network/
member/node/proposal/accessor, 5 call sites) plus persistence wiring -- a
real, boundable feature, but its own pass, not a one-line field-read fix.
Left undone; not fabricated, not silently dropped -- recorded here per
gopherstack-4shm's restraint principle.

## Notes

**Framework/protocol**: restjson1. Base path family is `/networks`, plus `/tags/{ResourceArn}`,
`/accessors[/{AccessorId}]`, `/invitations[/{InvitationId}]`.

**This pass's real fixes** (field-diffed against `aws-sdk-go-v2/service/managedblockchain@v1.31.19`'s
`types/types.go`, `api_op_*.go`, and `validators.go`):

1. **`MemberConfiguration.FrameworkConfiguration` was entirely unmodeled.** The real API's
   `validateMemberConfiguration` client-side validator requires it on *both* `CreateNetwork`'s
   nested `MemberConfiguration` and `CreateMember`'s top-level one, and `validateMemberFabricConfiguration`
   requires `Fabric.AdminUsername`/`Fabric.AdminPassword` whenever `FrameworkConfiguration.Fabric` is
   supplied. gopherstack previously accepted `CreateMember`/`CreateNetwork` requests missing this
   field entirely -- a raw HTTP client bypassing SDK-side validation sailed straight through with a
   member that had no Fabric identity at all. `validateMemberConfigurationRequest` in
   `handler_networks.go` now mirrors these validators server-side (stricter than the real API in one
   respect: gopherstack requires `Fabric` specifically, not just a non-nil `FrameworkConfiguration`,
   since gopherstack only emulates Hyperledger Fabric -- same rationale as `ErrMissingNodeMemberID`).
   `AdminPassword`'s real documented 8-32 character length constraint is also enforced
   (`ErrInvalidMemberAdminPassword`). New errors: `ErrMissingMemberFrameworkConfig`,
   `ErrMissingMemberFabricConfig`, `ErrMissingMemberAdminUsername`, `ErrMissingMemberAdminPassword`,
   `ErrInvalidMemberAdminPassword`.

2. **`Member.FrameworkAttributes` / `Node.FrameworkAttributes` / `Network.FrameworkAttributes` were
   entirely unmodeled** -- the prior audit pass explicitly deferred this as "a bigger design question."
   They are now implemented for real: `Member.FrameworkAttributes.Fabric.AdminUsername` (echoed from
   the request) and `.CaEndpoint` (synthesized, since gopherstack has no real Fabric CA --
   `memberCaEndpoint` in `members.go`); `Node.FrameworkAttributes.Fabric.PeerEndpoint`/
   `.PeerEventEndpoint` (synthesized -- `nodePeerEndpoint`/`nodePeerEventEndpoint` in `nodes.go`);
   `Network.FrameworkAttributes.Fabric.Edition` (echoed from `FrameworkConfiguration.Fabric.Edition`
   when the caller supplies it -- gopherstack does *not* invent an edition the caller never asked
   for) and `.OrderingServiceEndpoint` (synthesized -- `fabricOrderingServiceEndpoint` in
   `networks.go`). Only Fabric is modeled on all three, matching gopherstack's Fabric-only scope --
   see the Ethereum gap above.

3. **`Member.KmsKeyArn` / `Node.KmsKeyArn` were entirely unmodeled.** Real AWS documents the sentinel
   string `"AWS Owned KMS Key"` as the default when the caller supplies no customer managed key, and
   documents that a node "inherits this parameter from the member that it belongs to." Both are now
   implemented: `resolveMemberKmsKeyArn` in `members.go` applies the default/passthrough at
   `CreateMember`/`CreateNetwork` time, and `CreateNode` in `nodes.go` copies its owning member's
   current `KmsKeyArn` (looked up under the same lock that already validates the member exists).

4. **`NodeConfiguration.StateDB` / `Node.StateDB` were entirely unmodeled.** Real AWS defaults to
   `CouchDB` for Hyperledger Fabric 1.4+ (gopherstack's only emulated version --
   `defaultFrameworkVersion`); `resolveStateDB` in `nodes.go` now applies that default or the
   caller's explicit `LevelDB`/`CouchDB` choice.

5. **`Network.VpcEndpointServiceName` was entirely unmodeled.** Real AWS assigns every `AVAILABLE`
   network a VPC PrivateLink endpoint service name regardless of framework configuration; now
   synthesized unconditionally at network-creation time (`networkVPCEndpointServiceName`).

6. **`CreateNetwork` accepted any `Framework` value, including `ETHEREUM`.** The real API's
   `CreateNetwork` doc comment states "Applies only to Hyperledger Fabric" -- new networks can no
   longer be created on any other framework. gopherstack now rejects a non-empty, non-
   `HYPERLEDGER_FABRIC` `Framework` at `CreateNetwork` with `InvalidRequestException`
   (`ErrUnsupportedNetworkFramework`), while leaving `Framework=ETHEREUM` valid everywhere else it's
   used (e.g. `Accessor.NetworkType`'s `ETHEREUM_MAINNET`/`ETHEREUM_GOERLI`, unrelated to this enum).

7. **No server-side pagination.** Every `List*` op (`ListNetworks`/`ListMembers`/`ListNodes`/
   `ListProposals`/`ListProposalVotes`/`ListAccessors`/`ListInvitations`) previously accepted
   `maxResults`/`nextToken` but always returned every matching item in one page. Now implemented via
   the shared `paginate()` helper in `pagination.go`, which wraps `pkgs/page.New` (the same
   convention `services/acmpca` already established) -- confirmed the real query parameter names
   (`maxResults`/`nextToken`, both lowercase) directly against `serializers.go`'s
   `SetQuery("maxResults")`/`SetQuery("nextToken")` bindings, identical across all seven ops.
   `defaultListPageSize` (100) matches `services/acmpca`'s `defaultMaxItems` convention since real
   AWS does not document a specific default for this service.

8. **`CreateMember` parsed `InvitationId` off the request body and never read it again**
   (gopherstack-u84u). Real AWS's client-side validator marks it required
   (`validateOpCreateMemberInput`, `validators.go:805-806`, v1.34.4) and never sends a
   request without it; `Invitation.Status`'s doc comment (`types/enums.go:106-122`)
   documents `PENDING`→`ACCEPTED` as the one-time transition a successful `CreateMember`
   drives. gopherstack now requires it (`InvalidRequestException` if empty,
   `ErrMissingInvitationID`), looks it up (`ResourceNotFoundException` if unknown,
   reusing `ErrInvitationNotFound`), rejects one issued for a different network or not
   `PENDING` (`InvalidRequestException`, new `ErrInvitationNetworkMismatch`/
   `ErrInvitationNotPending`), and marks it `ACCEPTED` on success so it cannot be
   replayed. `Member.IsOwned` staying unconditionally `true` is unchanged and still a
   reasonable simplification (see gaps) -- gopherstack has no multi-account model to
   distinguish an owned member from an invited one, but the invitation itself is now a
   real, consumed resource rather than an ignored field.

**The prior pass's node-routing-URI fix** (nodes live at `/networks/{id}/nodes[/{id}]` with
`MemberId` carried via JSON body / `memberId` query parameter, never nested under `/members/`)
remains correct and was re-verified against `serializers.go`'s `opPath` constants during this pass;
no changes were needed there.

**Timestamps**: `*time.Time` fields marshal via Go's default `encoding/json` (RFC3339Nano), which
`smithytime.ParseDateTime` (used by every `CreationDate`/`ExpirationDate` field in the real
deserializer) parses correctly. Confirmed NOT an epoch-vs-ISO8601 bug class hit here -- this
service's JSON protocol (restjson1) uses ISO8601 date-time timestamps by default, unlike services
whose JSON members are individually marked epoch-seconds. Re-confirmed this pass for the new
surfaces added: `FrameworkAttributes`/`KmsKeyArn`/`StateDB`/`VpcEndpointServiceName` are all plain
strings, so no new timestamp fields were introduced.

**Error codes**: gopherstack's `errorResponse{Message, Code}` round-trips correctly through the
real SDK's `restjson.GetErrorInfo`, which matches `Code`/`code` and `Message`/`message` names
case-insensitively via plain `encoding/json` struct tags (confirmed by reading
`aws/protocol/restjson/decoder_util.go`). All error codes gopherstack emits
(`ResourceNotFoundException`, `ResourceAlreadyExistsException`, `InvalidRequestException`,
`InternalServiceErrorException`) match real exception types in `types/errors.go`.

**Vote tallying is real, not a disguised no-op**: confirmed by reading
`applyVoteThresholdLocked` in `backend.go` -- it computes yes-percentage against
`ApprovalThresholdPolicy.ThresholdPercentage`/`ThresholdComparator`, transitions
`IN_PROGRESS → APPROVED` when the threshold is met, and separately computes whether rejection is
mathematically guaranteed (remaining possible yes votes can't reach the requirement) to transition
`IN_PROGRESS → REJECTED`. `executeProposalActionsLocked` genuinely creates invitations and removes
members on approval. This is the "grep-based stub hunting has false positives" trap from
parity-principles.md #4 -- it would be easy to mistake this for a stub without reading the
threshold math.

## 2026-08-20 wrapper-key / nested-shape sweep

Protocol re-confirmed restjson1 (56 `awsRestjson1_*` functions in `serializers.go`; every
`HandleDeserialize` calls its `awsRestjson1_deserializeOpDocument<Op>Output` directly on the
decoded body -- none of managedblockchain's 27 ops hit the singular-output dead-code trap from a
prior appmesh audit). All 27 ops in the pinned SDK (`ls api_op_*.go`, v1.34.4) match
`GetSupportedOperations()` exactly.

The five summary/full pairs (Network/NetworkSummary, Member/MemberSummary, Node/NodeSummary,
Proposal/ProposalSummary, Accessor/AccessorSummary) all use distinct gopherstack wire structs
(`networkObject`/`networkSummaryObject`, etc.), and each was diffed field-by-field against its own
live `awsRestjson1_deserializeDocument<Type>` case list -- confirming gopherstack does NOT reuse
one wire struct across both sides of any pair (the dominant bug class this campaign, per kinesis
Consumer/ConsumerDescription, codeconnections Host/Connection). Four real bugs were found and
fixed, none of them a full/summary struct-reuse case:

1. **`LogConfigurations.Cloudwatch` wire key was `"CloudWatch"` (capital W), not real AWS's
   `"Cloudwatch"`** (`awsRestjson1_deserializeDocumentLogConfigurations`, deserializers.go:4999 --
   confirmed case-sensitive, no other case). Silently dropped by a real client on GetMember's
   `LogPublishingConfiguration.Fabric.CaLogs` and GetNode's `.Fabric.{ChaincodeLogs,PeerLogs}`.
   Fixed in `models.go` (`logConfigRespObj`/`logConfigReq`), `handler_members.go`,
   `handler_nodes.go`. Hand-revert reproduced a nil `ChaincodeLogs.Cloudwatch` through a real
   `managedblockchainsdk.Client` in the new `Test_SDKRoundTrip_NodeCloudwatchLogConfig`
   (`wire_shape_test.go`). Two existing tests (`TestHandler_UpdateMemberLogPublishingConfig`,
   `TestHandler_UpdateNodeLogPublishingConfig`) asserted the wrong response key and were corrected.

2. **`UpdateNode` required `memberId` as a query parameter; real AWS sends `MemberId` only in the
   JSON body.** Confirmed via `awsRestjson1_serializeOpHttpBindingsUpdateNodeInput`
   (serializers.go:2259, binds only NetworkId/NodeId to the URI, no `SetQuery("memberId")`) versus
   `awsRestjson1_serializeOpDocumentUpdateNodeInput` (serializers.go:2285, serializes `MemberId`
   into the body) -- unlike GetNode/ListNodes/DeleteNode, which really do bind `memberId` as a
   query parameter and were left unchanged. This meant every real SDK client's `UpdateNode` call
   was rejected outright with `InvalidRequestException`, not just a dropped field -- a request-side
   HTTP-binding bug found incidentally while diffing the node family, outside this campaign's
   strict response-wrapper-key scope but severe enough to fix. Fixed in `handler_nodes.go`
   (`handleUpdateNode` now reads `MemberId` from the decoded body) and `models.go`
   (`updateNodeRequest.MemberID` added). Hand-revert reproduced the exact real-client
   `InvalidRequestException: MemberId is required...` in
   `Test_SDKRoundTrip_NodeCloudwatchLogConfig`. Three existing tests
   (`TestHandler_NodeLifecycle_RealWireShape`, `TestHandler_UpdateNode`,
   `TestHandler_UpdateNodeLogPublishingConfig`) sent `memberId` as a query parameter for UpdateNode
   and were corrected to send it in the body.

3. **`ProposalSummary` (ListProposals) fabricated a `NetworkId` member real AWS never sends.**
   `awsRestjson1_deserializeDocumentProposalSummary` (deserializers.go:6573) has no `NetworkId`
   case at all -- only the full `Proposal` type (GetProposal) has one. Removed from
   `proposalSummaryObject` (`models.go`) and `toProposalSummaryObject` (`handler_proposals.go`).
   `TestHandler_ProposalSummaryHasNetworkID`, a test literally named for asserting the fabricated
   field was present, was rewritten as `TestHandler_ProposalSummaryOmitsNetworkID` asserting its
   absence. Hand-revert reproduced the field reappearing in the raw response body.

4. **`Invitation` (ListInvitations) fabricated top-level `NetworkId`/`NetworkName` members.**
   `awsRestjson1_deserializeDocumentInvitation` (deserializers.go:4762) has cases only for
   `Arn`/`CreationDate`/`ExpirationDate`/`InvitationId`/`NetworkSummary`/`Status` -- real AWS
   carries network identity only inside the nested `NetworkSummary`, confirmed against
   `types.Invitation`'s field list (types/types.go:116-155). Removed from `invitationObject`
   (`models.go`) and `toInvitationObject` (`handler_invitations.go`); the internal `Invitation`
   struct's `NetworkID`/`NetworkName` fields were left alone since `members.go:109`'s
   invitation/network-mismatch check on `CreateMember` genuinely needs them -- only the wire object
   was fabricating them onto the response. No existing test asserted these fields, so a new one
   (`TestHandler_InvitationOmitsTopLevelNetworkFields`) was added; hand-revert reproduced both
   fields reappearing.

All four fixes were proven by hand-revert (flip the fix back, run the relevant test, confirm the
exact predicted symptom, restore, `diff` byte-identical against a pre-revert copy) before being
finalized. Bug 1 and 2 were additionally proven end-to-end through a real
`aws-sdk-go-v2/service/managedblockchain` client round-tripped through `pkgs/service`'s router
(`wire_shape_test.go`, matching `services/mediaconvert/wire_shape_test.go`'s pattern) since
`types.Node.LogPublishingConfiguration` gives a typed client field to observe. Bugs 3 and 4 used a
raw-body absence assertion instead, since the corresponding real types
(`types.ProposalSummary`, `types.Invitation`) have no field for a typed client to observe a
fabricated key on.

No other gap was found in the five summary/full pairs, `NetworkFrameworkAttributes`/
`NetworkFabricAttributes`, `MemberFrameworkAttributes`/`MemberFabricAttributes`,
`NodeFrameworkAttributes`/`NodeFabricAttributes`, `VotingPolicy`/`ApprovalThresholdPolicy`,
`ProposalActions`/`InviteAction`/`RemoveAction`, or `MemberConfiguration`/
`MemberFabricConfiguration`/`NodeConfiguration`. One gap was noted but NOT fixed (Layer 3, out of
this campaign's scope): real AWS's `NodeConfiguration` (CreateNode's input) accepts an optional
create-time `LogPublishingConfiguration` member (`api_op_CreateNode.go`'s
`awsRestjson1_serializeDocumentNodeConfiguration`, serializers.go:2624) that gopherstack's
`nodeConfiguration` request struct does not model at all -- a real client can only set node log
config after creation via `UpdateNode` in gopherstack today.

**`last_audit_commit` provenance**: this pass's audit date is 2026-08-20; `last_audit_commit` was
updated to `a073b2b1` (this repo's HEAD at the time this file was written, per the schema). The
prior entry (`d08692ef`, dated `2026-08-10` in this file) checks out: `git show -s --format=%ad
d08692ef` returns `2026-08-10`, matching the manifest's `last_audit_date` -- unlike appmesh and
codeconnections, both caught this campaign citing a 2026-07-13 sha against a 2026-08-10 audit
date. Verdict: clean provenance, no fabricated audit trail.
