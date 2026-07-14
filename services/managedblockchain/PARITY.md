---
service: managedblockchain
sdk_module: aws-sdk-go-v2/service/managedblockchain@v1.31.19
last_audit_commit: efd78e54
last_audit_date: 2026-07-13
overall: A
ops:
  CreateNetwork: {wire: ok, errors: ok, state: ok, persist: ok}
  GetNetwork: {wire: ok, errors: ok, state: ok, persist: ok}
  ListNetworks: {wire: ok, errors: ok, state: ok, persist: ok, note: "no server-side pagination (maxResults/nextToken accepted-but-ignored); see gaps"}
  CreateMember: {wire: ok, errors: ok, state: ok, persist: ok}
  GetMember: {wire: ok, errors: ok, state: ok, persist: ok}
  ListMembers: {wire: ok, errors: ok, state: ok, persist: ok, note: "no server-side pagination; see gaps"}
  DeleteMember: {wire: ok, errors: ok, state: ok, persist: ok, note: "cascades to member's nodes, matching real AWS"}
  UpdateMember: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateNode: {wire: fixed, errors: ok, state: ok, persist: ok, note: "path + MemberId location were wrong; see Notes"}
  GetNode: {wire: fixed, errors: ok, state: ok, persist: ok, note: "path + MemberId location were wrong; see Notes"}
  ListNodes: {wire: fixed, errors: ok, state: ok, persist: ok, note: "path + MemberId location were wrong; see Notes"}
  DeleteNode: {wire: fixed, errors: ok, state: ok, persist: ok, note: "path + MemberId location were wrong; see Notes"}
  UpdateNode: {wire: fixed, errors: ok, state: ok, persist: ok, note: "path + MemberId location were wrong; see Notes"}
  CreateProposal: {wire: ok, errors: ok, state: ok, persist: ok}
  GetProposal: {wire: ok, errors: ok, state: ok, persist: ok}
  ListProposals: {wire: ok, errors: ok, state: ok, persist: ok, note: "no server-side pagination; see gaps"}
  VoteOnProposal: {wire: ok, errors: ok, state: ok, persist: ok, note: "tallies votes and resolves APPROVED/REJECTED against VotingPolicy; not a disguised no-op"}
  ListProposalVotes: {wire: ok, errors: ok, state: ok, persist: ok}
  ListInvitations: {wire: ok, errors: ok, state: ok, persist: ok}
  RejectInvitation: {wire: ok, errors: ok, state: ok, persist: ok}
  CreateAccessor: {wire: ok, errors: ok, state: ok, persist: ok}
  GetAccessor: {wire: ok, errors: ok, state: ok, persist: ok}
  DeleteAccessor: {wire: ok, errors: ok, state: ok, persist: ok}
  ListAccessors: {wire: ok, errors: ok, state: ok, persist: ok}
  TagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  UntagResource: {wire: ok, errors: ok, state: ok, persist: ok}
  ListTagsForResource: {wire: ok, errors: ok, state: ok, persist: ok}
families:
  network: {status: ok, note: "CreateNetwork/GetNetwork/ListNetworks verified against serializers.go opPath + field tags"}
  member: {status: ok, note: "CreateMember/GetMember/ListMembers/DeleteMember/UpdateMember paths and bodies match the real SDK"}
  node: {status: fixed, note: "entire family had the wrong URI shape; see Notes -- this was a real, high-impact bug, not a wire nit"}
  proposal: {status: ok, note: "CreateProposal/GetProposal/ListProposals/ListProposalVotes/VoteOnProposal verified; vote tallying and threshold-based APPROVED/REJECTED transition confirmed real (not a stub)"}
  invitation: {status: ok, note: "ListInvitations/RejectInvitation only -- correctly no CreateInvitation op (real AWS has none either; invitations are created only as a side effect of an approved proposal's Invitations actions, which executeProposalActionsLocked implements)"}
  accessor: {status: ok, note: "CreateAccessor/GetAccessor/DeleteAccessor/ListAccessors verified"}
  tags: {status: ok, note: "TagResource/UntagResource/ListTagsForResource verified against /tags/{ResourceArn} shape and ARN-keyed lookup"}
gaps:
  - "List* ops (ListNetworks/ListMembers/ListNodes/ListProposals/ListAccessors/ListInvitations/ListProposalVotes) accept maxResults/nextToken but always return every matching item in one page (NextToken always omitted). Real AWS paginates. Low risk for an emulator (SDK clients that loop on NextToken still terminate correctly since it's never set), but a client asserting a specific page size would see the whole result set. Not filed as a bd issue this pass -- flagging for the next audit to decide whether pkgs/page is worth wiring in."
  - "Node.FrameworkAttributes (e.g. Fabric's PeerEndpoint/PeerEventEndpoint, Ethereum's Http/WebSocket endpoints) is not modeled -- GetNode/ListNodes responses omit it entirely. Same for Member.FrameworkAttributes' KmsKeyArn-style fields. A client that reads a node's peer endpoint to actually connect to Fabric will get nothing back. Deferred: modeling this accurately requires deciding what a 'connectable' emulated peer endpoint even means for gopherstack, which is a bigger design question than a wire-shape bug fix."
  - "CreateMember ignores req.InvitationId -- every CreateMember call succeeds as if the caller already owns the network (IsOwned: true always), whereas real AWS requires a live invitation for cross-account members. gopherstack has no multi-account model, so this is a reasonable simplification, not flagged as a bug to fix."
  - "No artificial service quotas (max members per network, max nodes per member, max networks per account) are enforced, so ResourceLimitExceededException is never returned. Consistent with this emulator's general no-limits style elsewhere; not treated as a bug."
deferred: []
leaks: {status: clean, note: "no goroutines/janitors in this service; InMemoryBackend.mu is the single coarse lockmetrics.RWMutex guarding every map/store.Table, consistent with pkgs-catalog.md's locking rule"}
---

## Notes

**Framework/protocol**: restjson1. Base path family is `/networks`, plus `/tags/{ResourceArn}`,
`/accessors[/{AccessorId}]`, `/invitations[/{InvitationId}]`.

**The Node routing bug (this pass's real fix)**: before this audit, gopherstack routed every node
operation under `/networks/{networkId}/members/{memberId}/nodes[/{nodeId}]` -- i.e. nodes nested
under their owning member in the URI, mirroring how members nest under networks. This shape does
**not exist** in the real API. Checked directly against
`aws-sdk-go-v2/service/managedblockchain@v1.31.19`'s `serializers.go`: every node op's `opPath`
resolves to `/networks/{NetworkId}/nodes` (CreateNode, ListNodes) or
`/networks/{NetworkId}/nodes/{NodeId}` (GetNode, DeleteNode, UpdateNode) -- the member is never
part of the URI. `MemberId` instead travels as a JSON body field on CreateNode
(`encoder` binds it nowhere -- it's a plain body member) and as the `memberId` query parameter on
every other node op (`encoder.SetQuery("memberId")` in the real serializer, confirmed for
GetNode/ListNodes/DeleteNode/UpdateNode).

Impact: this was not a cosmetic wire-shape nit. A real `aws-sdk-go-v2` client calling `CreateNode`
sends `POST /networks/{id}/nodes`; gopherstack's old `parsePath` only recognized
`/networks/{id}/members/{id}/nodes`, so that request fell through to `parsePath`'s `"", ""` case
and every node operation 404'd against a real SDK client (`ResourceNotFoundException: unknown
operation`). Unit tests didn't catch it because they hand-built requests using the same
(wrong) path convention the handler itself expected -- a self-consistent but non-real fixture.

Fixed in `handler.go`: `parseNetworksPath` now recognizes a `nodes` segment as a sibling of
`members`/`proposals` (not nested under `members`); `parseNetworkNodesPath` replaces the deleted
`parseNodesPath` and resolves `/networks/{id}/nodes[/{nodeId}]` without ever consuming a member
segment. `parseMembersPath` was also tightened while here: it now requires the path to end exactly
at `/networks/{id}/members/{id}` (or a trailing slash) rather than accepting any 4+-segment path
with a nonempty `parts[3]` as a member resource -- previously a stray path like the old (now
deleted) member-nested node shape would have silently resolved as `UpdateMember`/`GetMember`
instead of falling through to "unknown operation", which is what a real API gateway would do.

Handler-side: `handleCreateNode` now reads `MemberId` from the decoded JSON body (added to
`createNodeRequest` in `models.go`) instead of splitting it out of the URI; `handleGetNode`,
`handleListNodes`, `handleDeleteNode`, `handleUpdateNode` now read `memberId` from
`c.Request().URL.Query()` instead of a 3-way URI split (the now-dead `splitThreePart` helper was
deleted). All five ops return `InvalidRequestException` (`ErrMissingNodeMemberID`,
new in `backend.go`) if `MemberId`/`memberId` is missing -- real AWS documents it as "required for
Hyperledger Fabric" on every node op, and gopherstack only emulates Hyperledger Fabric networks
(`defaultFramework = "HYPERLEDGER_FABRIC"`), so it is unconditionally required here.

A new test, `TestHandler_NodeLifecycle_RealWireShape` in `handler_test.go`, drives the full node
lifecycle (Create/Get/List/Update/Delete) through both `h.RouteMatcher()` (with a real
`managedblockchain` `Authorization` header) and `h.Handler()` together via a new `doRoutedRequest`
helper, using the exact real-wire path/body/query shape -- this is the "goes through the matcher"
test class called out in `.claude/memories/parity-principles.md`'s route-matcher bug list.
`TestHandler_ExtractOperationAndResource` also gained cases proving the old member-nested node
shape now resolves to no operation at all (rather than silently matching something else).

**Timestamps**: `*time.Time` fields marshal via Go's default `encoding/json` (RFC3339Nano), which
`smithytime.ParseDateTime` (used by every `CreationDate`/`ExpirationDate` field in the real
deserializer) parses correctly. Confirmed NOT an epoch-vs-ISO8601 bug class hit here -- this
service's JSON protocol (restjson1) uses ISO8601 date-time timestamps by default, unlike services
whose JSON members are individually marked epoch-seconds.

**Error codes**: gopherstack's `errorResponse{Message, Code}` round-trips correctly through the
real SDK's `restjson.GetErrorInfo`, which matches `Code`/`code` and `Message`/`message` names
case-insensitively via plain `encoding/json` struct tags (confirmed by reading
`aws/protocol/restjson/decoder_util.go`). All four codes gopherstack emits
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
