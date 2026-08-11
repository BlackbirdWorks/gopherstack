---
service: managedblockchain
sdk_module: aws-sdk-go-v2/service/managedblockchain@v1.34.4
last_audit_commit: d08692ef
last_audit_date: 2026-08-10
overall: A
ops:
  CreateNetwork: {wire: fixed, errors: fixed, state: fixed, persist: ok, note: "FrameworkConfiguration.Fabric.Edition, VpcEndpointServiceName, Framework restricted to HYPERLEDGER_FABRIC; see Notes"}
  GetNetwork: {wire: fixed, errors: ok, state: ok, persist: ok, note: "now returns FrameworkAttributes.Fabric + VpcEndpointServiceName"}
  ListNetworks: {wire: fixed, errors: ok, state: ok, persist: ok, note: "server-side pagination now implemented via pkgs/page; see Notes"}
  CreateMember: {wire: fixed, errors: fixed, state: fixed, persist: ok, note: "InvitationId now required and validated against a real PENDING invitation for this network, consumed (ACCEPTED) on success; MemberConfiguration.FrameworkConfiguration.Fabric.AdminUsername/AdminPassword required and validated, KmsKeyArn accepted; see Notes"}
  GetMember: {wire: fixed, errors: ok, state: ok, persist: ok, note: "now returns FrameworkAttributes.Fabric.AdminUsername/CaEndpoint + KmsKeyArn"}
  ListMembers: {wire: fixed, errors: ok, state: ok, persist: ok, note: "server-side pagination now implemented"}
  DeleteMember: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascades to member's nodes, matching real AWS"}
  UpdateMember: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateNode: {wire: fixed, errors: ok, state: fixed, persist: ok, note: "NodeConfiguration.StateDB accepted (defaults CouchDB), KmsKeyArn inherited from owning member; see Notes"}
  GetNode: {wire: fixed, errors: ok, state: ok, persist: ok, note: "now returns FrameworkAttributes.Fabric.PeerEndpoint/PeerEventEndpoint + StateDB + KmsKeyArn"}
  ListNodes: {wire: fixed, errors: ok, state: ok, persist: ok, note: "server-side pagination now implemented"}
  DeleteNode: {wire: ok, errors: ok, state: ok, persist: ok}
  UpdateNode: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateProposal: {wire: ok, errors: ok, state: ok, persist: ok}
  GetProposal: {wire: ok, errors: ok, state: ok, persist: ok}
  ListProposals: {wire: fixed, errors: ok, state: ok, persist: ok, note: "server-side pagination now implemented"}
  VoteOnProposal: {wire: ok, errors: ok, state: ok, persist: ok, note: "tallies votes and resolves APPROVED/REJECTED against VotingPolicy; not a disguised no-op"}
  ListProposalVotes: {wire: fixed, errors: ok, state: ok, persist: ok, note: "server-side pagination now implemented"}
  ListInvitations: {wire: fixed, errors: ok, state: ok, persist: ok, note: "server-side pagination now implemented"}
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
  member: {status: fixed, note: "MemberConfiguration.FrameworkConfiguration was entirely unmodeled (a real, required field per validateMemberFabricConfiguration) -- now implemented with real server-side validation + FrameworkAttributes/KmsKeyArn on responses, see Notes"}
  node: {status: fixed, note: "StateDB/KmsKeyArn/FrameworkAttributes were entirely unmodeled -- now implemented; the prior audit's node-routing-URI fix remains correct and unchanged"}
  proposal: {status: ok, note: "CreateProposal/GetProposal/ListProposals/ListProposalVotes/VoteOnProposal verified; vote tallying and threshold-based APPROVED/REJECTED transition confirmed real (not a stub); ListProposals/ListProposalVotes now paginate"}
  invitation: {status: ok, note: "ListInvitations/RejectInvitation only -- correctly no CreateInvitation op (real AWS has none either; invitations are created only as a side effect of an approved proposal's Invitations actions, which executeProposalActionsLocked implements); ListInvitations now paginates"}
  accessor: {status: ok, note: "CreateAccessor/GetAccessor/DeleteAccessor/ListAccessors verified; ListAccessors now paginates"}
  tags: {status: ok, note: "TagResource/UntagResource/ListTagsForResource verified against /tags/{ResourceArn} shape and ARN-keyed lookup"}
gaps:
  - "Member.IsOwned is always true, even for a member created via CreateMember (i.e. joining via invitation, which in real AWS is not owned by the joining account's original network-owner relationship). gopherstack has no multi-account model to distinguish an owned member from an invited one, so this is a reasonable simplification, not flagged as a bug to fix (gopherstack-u84u re-reviewed this alongside InvitationId; InvitationId itself is now real, see Notes #8)."
  - "No artificial service quotas (max members per network, max nodes per member, max networks per account) are enforced, so ResourceLimitExceededException is never returned. Consistent with this emulator's general no-limits style elsewhere; not treated as a bug."
  - "Network.FrameworkAttributes.Ethereum and Node.FrameworkAttributes.Ethereum are not modeled. gopherstack-u84u answered the design question this was deferred under: real AWS's CreateNode documents exactly one well-known public Ethereum NetworkId, \"n-ethereum-mainnet\" (aws-sdk-go-v2 managedblockchain api_op_CreateNode.go:44-47 and api_op_DeleteNode.go:36, v1.34.4 -- confirmed NOT invented; older SDKs additionally listed now-sunset n-ethereum-goerli/n-ethereum-rinkeby testnets, absent from this pin), with FrameworkAttributes.Ethereum.ChainId documented as \"1\" for mainnet (types/types.go:538-547's NetworkEthereumAttributes). ListNetworks/GetNetwork both self-document \"Applies to Hyperledger Fabric and Ethereum\", so real AWS does surface this network through both once an account has a node on it. Seeding the network itself would therefore be honest (a real, stable constant, not invented). Still deferred: CreateNode's real MemberId is documented \"Applies only to Hyperledger Fabric\" (api_op_CreateNode.go:56-58) -- Ethereum nodes have no owning member -- but gopherstack's Node storage is keyed by (networkID, memberID, nodeID) (nodeKey in store_setup.go) and CreateNode already requires MemberId unconditionally (ErrMissingNodeMemberID) for its one supported framework. Making CreateNode against Ethereum reachable needs a memberless Node storage path, not just a seeded network row -- a real structural change, not an adjacent fix."
deferred: []
leaks: {status: clean, note: "no goroutines/janitors in this service; InMemoryBackend.mu is the single coarse lockmetrics.RWMutex guarding every map/store.Table, consistent with pkgs-catalog.md's locking rule. The new paginate() helper (pagination.go) and buildNetworkFrameworkAttributes/buildMemberFrameworkAttributes/CreateNode's FrameworkAttributes synthesis are all pure functions operating on already-locked state or post-lock snapshots -- no new lock paths introduced."}
---

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
