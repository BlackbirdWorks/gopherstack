---
# PARITY MANIFEST — PRE-IMPLEMENTATION AUDIT, NOT YET BUILT.
# services/networkmanager/ does not exist yet (confirmed: this file was written into a freshly
# created, otherwise-empty directory; no cli.go registration, no go.mod entry, zero Go symbols
# anywhere in the tree -- grepped case-insensitively for "networkmanager"/"corenetwork"/
# "globalnetwork"/"core-network"/"global-network" across services/ and cli.go: the ONLY hits are
# four lines in services/directconnect/PARITY.md itself, noting that DirectConnect's
# AssociatedCoreNetwork field has no backing Cloud WAN service in this tree -- not references to an
# actual NetworkManager implementation). This document is a wire-shape + behavior SPEC for the
# implementer, not a record of existing code. No .go files were written to produce it; every claim
# below was read directly from the SDK module cache, grepped/read from this repo's existing
# services, or fetched from AWS's own IAM Service Authorization Reference page (which, unlike the
# outposts/mgn/directconnect audits' attempts at docs.aws.amazon.com, rendered its resource-type
# table successfully this pass -- cited per-claim, not guessed).
service: networkmanager
sdk_module: aws-sdk-go-v2/service/networkmanager@v1.44.3   # resolved via `go get
# .../networkmanager@latest` in a throwaway scratch module (`go mod init probe && go get`), run in
# this session's scratchpad, NEVER touching this repo's go.mod (another agent was concurrently
# editing go.mod/go.sum/cli.go during this pass; this audit did not read or write any of those
# three files, and ran every `go` command one at a time per the resource-constraint instruction).
last_audit_commit: 7922e4c4d   # HEAD when this manifest was written; there is no prior Network
# Manager code in the tree at all, so this is a from-scratch pre-implementation audit, matching the
# mgn/directconnect audits done in the same pass (same commit, same session).
last_audit_date: 2026-08-01
overall: gap   # pre-implementation inventory; every op below is status "gap" -- routed nowhere, no
# backend, no wire code. Not a regression signal; it is the expected starting state.
# All 95 ops confirmed present in aws-sdk-go-v2/service/networkmanager@v1.44.3 (`ls api_op_*.go |
# grep -v _test.go | wc -l` => 95, matching this task's ~95 estimate exactly). None are implemented.
# Method/path verified via a Python regex pass over every awsRestjson1_serializeOp<Op>.
# HandleSerialize's httpbinding.SplitURI(...) literal and request.Method assignment in
# serializers.go (all 95 matched, not sampled). Error sets verified by parsing every op's own
# awsRestjson1_deserializeOpError<Op> switch body in deserializers.go for
# strings.EqualFold("X", errorCode) case literals (all 95, not sampled from the shared
# types/errors.go list of 8 shapes, which enumerates all 8 without saying which ops use which --
# same trap the mgn/directconnect/outposts/resiliencehub audits all flagged).
# Grouped by family per this task's own guidance (95 ops is too many for 95 prose blocks); every op
# appears in exactly one family table in the body below.
families:
  global_networks_core: {status: gap, note: "4 ops: CreateGlobalNetwork, UpdateGlobalNetwork, DeleteGlobalNetwork, DescribeGlobalNetworks. The root container every other Global-Networks-side resource (Site/Device/Link/Connection/association) is scoped under via GlobalNetworkId."}
  sites: {status: gap, note: "4 ops: CreateSite, UpdateSite, DeleteSite, GetSites."}
  devices: {status: gap, note: "4 ops: CreateDevice, UpdateDevice, DeleteDevice, GetDevices."}
  links: {status: gap, note: "4 ops: CreateLink, UpdateLink, DeleteLink, GetLinks."}
  link_associations: {status: gap, note: "3 ops: AssociateLink, DisassociateLink, GetLinkAssociations -- binds a Device to a Link within one Site."}
  connections: {status: gap, note: "4 ops: CreateConnection, UpdateConnection, DeleteConnection, GetConnections -- an on-prem device-to-device physical/logical connection over a Link, distinct from a Cloud WAN Connect attachment despite the name collision."}
  customer_gateway_associations: {status: gap, note: "3 ops: AssociateCustomerGateway, DisassociateCustomerGateway, GetCustomerGatewayAssociations -- binds an EC2 CustomerGatewayArn to a Device/Link in the Global Network."}
  transit_gateway_registrations: {status: gap, note: "3 ops: RegisterTransitGateway, DeregisterTransitGateway, GetTransitGatewayRegistrations -- registers an EC2 TransitGateway into the Global Network (a prerequisite for Cloud WAN peering AND for classic Global-Networks route/telemetry visibility into that TGW)."}
  transit_gateway_connect_peer_associations: {status: gap, note: "3 ops: AssociateTransitGatewayConnectPeer, DisassociateTransitGatewayConnectPeer, GetTransitGatewayConnectPeerAssociations -- binds an EC2 TransitGatewayConnectPeer (GRE overlay peer) to a Device/Link."}
  connect_peer_global_network_association: {status: gap, note: "3 ops: AssociateConnectPeer, DisassociateConnectPeer, GetConnectPeerAssociations -- a genuine bridge between the two product halves: associates an already-created Cloud WAN ConnectPeer to a Global-Networks Device/Link, letting a Cloud WAN Connect attachment's peer be modeled as physically terminating at a specific on-prem device. See Missing simulated functionality -- this op family is the concrete rebuttal to treating the two halves as fully separate silos."}
  connect_peers_cloudwan: {status: gap, note: "4 ops: CreateConnectPeer, DeleteConnectPeer, GetConnectPeer, ListConnectPeers -- the Cloud WAN-side Connect Peer lifecycle (a BGP/GRE peer terminating a Connect attachment), distinct from the association family above."}
  core_networks: {status: gap, note: "5 ops: CreateCoreNetwork, UpdateCoreNetwork, DeleteCoreNetwork, GetCoreNetwork, ListCoreNetworks."}
  core_network_policy_lifecycle: {status: gap, note: "8 ops: PutCoreNetworkPolicy, GetCoreNetworkPolicy, ListCoreNetworkPolicyVersions, DeleteCoreNetworkPolicyVersion, RestoreCoreNetworkPolicyVersion, GetCoreNetworkChangeSet, GetCoreNetworkChangeEvents, ExecuteCoreNetworkChangeSet -- the versioned-policy + change-set + LIVE/LATEST alias state machine at the heart of Cloud WAN. See Missing simulated functionality for the full lifecycle."}
  core_network_prefix_list_associations: {status: gap, note: "3 ops: CreateCoreNetworkPrefixListAssociation, DeleteCoreNetworkPrefixListAssociation, ListCoreNetworkPrefixListAssociations -- binds a customer-managed EC2-style prefix list to a core network by alias/ARN."}
  core_network_routing_information: {status: gap, note: "1 op: ListCoreNetworkRoutingInformation -- lists BGP-attribute-tagged routes (AS path, communities, local preference, MED, next hop) per segment/edge, filterable by those same BGP attributes."}
  attachment_routing_policy: {status: gap, note: "3 ops: PutAttachmentRoutingPolicyLabel, RemoveAttachmentRoutingPolicyLabel, ListAttachmentRoutingPolicyAssociations -- a newer sub-feature layering named routing-policy labels onto attachments, read by ListCoreNetworkRoutingInformation-adjacent filtering; not mentioned in this task's framing but a real, distinct 3-op surface."}
  attachment_generic_lifecycle: {status: gap, note: "4 ops: AcceptAttachment, RejectAttachment, DeleteAttachment, ListAttachments -- generic across all 5 attachment subtypes (VPC/Connect/SiteToSiteVpn/DirectConnectGateway/TransitGatewayRouteTable), operating on the shared base Attachment shape and AttachmentId."}
  vpc_attachments: {status: gap, note: "3 ops: CreateVpcAttachment, GetVpcAttachment, UpdateVpcAttachment."}
  connect_attachments: {status: gap, note: "2 ops: CreateConnectAttachment, GetConnectAttachment -- the transport-layer attachment a Cloud WAN ConnectPeer terminates on."}
  site_to_site_vpn_attachments: {status: gap, note: "2 ops: CreateSiteToSiteVpnAttachment, GetSiteToSiteVpnAttachment -- wraps an EC2 VpnConnectionArn."}
  direct_connect_gateway_attachments: {status: gap, note: "3 ops: CreateDirectConnectGatewayAttachment, GetDirectConnectGatewayAttachment, UpdateDirectConnectGatewayAttachment -- wraps a DirectConnectGatewayArn; services/directconnect does not exist as working code yet either (PARITY.md only), so this attachment kind currently has no real cross-service target to validate against."}
  transit_gateway_route_table_attachments: {status: gap, note: "2 ops: CreateTransitGatewayRouteTableAttachment, GetTransitGatewayRouteTableAttachment -- requires an existing Peering (PeeringId), not a bare TransitGatewayArn, to create."}
  peerings: {status: gap, note: "4 ops: CreateTransitGatewayPeering, GetTransitGatewayPeering (subtype-specific), DeletePeering, ListPeerings (generic across peering types -- though PeeringType currently has exactly one value, TRANSIT_GATEWAY, so the generic/specific split is presently a distinction without a second case)."}
  route_analysis: {status: gap, note: "2 ops: StartRouteAnalysis, GetRouteAnalysis -- computes a forward/return path through the modeled topology. See Missing simulated functionality for an honest feasibility assessment -- this is the single riskiest fabrication surface in the whole service."}
  network_introspection: {status: gap, note: "5 ops: GetNetworkResources, GetNetworkResourceCounts, GetNetworkResourceRelationships, GetNetworkRoutes, GetNetworkTelemetry -- read-only rollups over registered/attached resources. See Missing simulated functionality for which are honestly derivable from modeled state (GetNetworkResources/GetNetworkResourceCounts/GetNetworkResourceRelationships/GetNetworkRoutes) versus which require real device telemetry this emulator cannot honestly produce (GetNetworkTelemetry, mostly)."}
  update_network_resource_metadata: {status: gap, note: "1 op: UpdateNetworkResourceMetadata -- attaches a caller-supplied key-value metadata map to a registered resource ARN, independent of AWS tags."}
  organizations_integration: {status: gap, note: "2 ops: StartOrganizationServiceAccessUpdate, ListOrganizationServiceAccessStatus -- enables/disables the NetworkManager AWS-Organizations service-linked-role trust relationship, org-wide. ListOrganizationServiceAccessStatus is the ONLY op in this entire 95-op surface with NO typed exception cases in its deserializer switch (confirmed by direct read, see wire-shape traps) -- every error condition for this one op falls through to the generic smithy.GenericAPIError."}
  resource_policy: {status: gap, note: "3 ops: PutResourcePolicy, GetResourcePolicy, DeleteResourcePolicy -- a resource-based IAM policy (JSON document) attached to a NetworkManager resource ARN (used for cross-account core-network sharing), structurally unrelated to the CoreNetworkPolicy (network configuration) document despite both being called \"policy\" -- see wire-shape traps for the naming collision risk."}
  tagging: {status: gap, note: "3 ops: TagResource, UntagResource, ListTagsForResource -- shares the single generic /tags/{ResourceArn} path with 9 confirmed taggable resource kinds (see Cross-service wiring)."}
gaps:
  - "Zero operations implemented -- from-scratch audit only, per this task's explicit instructions not to write any .go files. All 95 ops need building. (bd: none filed yet by this pass -- filing is the implementer's responsibility per the standard workflow.)"
  - "Route analysis (StartRouteAnalysis/GetRouteAnalysis) computes a real forward/return path through a Transit-Gateway-centric topology (route tables, attachments, blackhole/inactive routes, cyclic-path detection, a 64-hop limit) per RouteAnalysisCompletionReasonCode's 11 real values (TRANSIT_GATEWAY_ATTACHMENT_NOT_FOUND, CYCLIC_PATH_DETECTED, MAX_HOPS_EXCEEDED, BLACKHOLE_ROUTE_FOR_DESTINATION_FOUND, etc., confirmed in types/enums.go). This repo's services/ec2 has real TransitGatewayRouteTable/TransitGatewayRoute/TransitGatewayVpcAttachment state (ec2core.go:70-77, accept_ops.go:84-93) that a genuine graph-walk COULD traverse -- this is not automatically fabrication, but it is real, non-trivial graph-algorithm work (route-table lookup, next-hop resolution across attachments, cycle detection, hop-count limiting) that must actually walk that state, not synthesize a plausible-looking PathComponent list. If an implementer ships this without doing the real walk, the result must be flagged as gap, not partial -- see Missing simulated functionality for the full honesty assessment."
  - "GetNetworkTelemetry's NetworkTelemetry.Health (*ConnectionHealth{Status: UP|DOWN, Timestamp, Type: BGP|IPSEC}) reflects REAL AWS device/link telemetry (SNMP-style polling of actual on-prem hardware and actual BGP/IPSec session state) that has no honest analog in an emulator with no real network hardware. A defensible default is Status always UP once a Connection/ConnectPeer/attachment reaches its AVAILABLE state (deterministic, non-fabricated, boringly consistent with 'nothing is actually wrong because nothing is actually real'), never invented flapping/degraded telemetry designed to look realistic -- seeded random health values would be exactly the kind of fabrication parity-principles.md forbids."
  - "CoreNetworkChangeEvent/CoreNetworkChange's full diff semantics (14 ChangeType values: CORE_NETWORK_SEGMENT, ATTACHMENT_MAPPING, ROUTING_POLICY_ATTACHMENT_ASSOCIATION, SEGMENT_ACTIONS_CONFIGURATION, ...) describe a real structural diff between the LIVE and submitted CoreNetworkPolicy JSON documents. An honest first-pass implementation likely needs to actually parse the policy JSON (segments/network-function-groups/attachment-policies sections) and diff it, not fabricate a plausible-looking change list -- this is real, buildable JSON-diff work (the policy document is caller-supplied JSON, not an opaque AWS-internal format), but it is meaningfully more work than the CRUD shell around it and is a concrete implementation-scoping decision, not something to gloss over with an empty CoreNetworkChanges list dressed up as real analysis."
  - "GetCoreNetworkPolicy's Alias parameter (LIVE|LATEST, CoreNetworkPolicyAlias, confirmed 2 values in types/enums.go) requires the backend to track two logically distinct pointers into the same PolicyVersionId history -- LATEST is whatever PutCoreNetworkPolicy/RestoreCoreNetworkPolicyVersion most recently created, LIVE is whatever ExecuteCoreNetworkChangeSet most recently deployed. These are NOT the same version until execution happens, and DeleteCoreNetworkPolicyVersion's own doc comment ('You can't delete the current LIVE policy') is a real, checkable invariant to enforce, not decorative."
  - "AttachmentType's DIRECT_CONNECT_GATEWAY and TRANSIT_GATEWAY_ROUTE_TABLE attachment kinds each depend on a resource this repo either doesn't have working code for yet (services/directconnect is PARITY.md-only, no .go files, confirmed via `ls services/directconnect/`) or does have (services/ec2's TransitGatewayRouteTable, ec2core.go:70-77) -- an implementer must decide per-kind whether to validate the referenced ARN against real cross-service state (buildable today only for the TransitGatewayRouteTable/VPC/VpnConnection/CustomerGateway/TransitGatewayConnectPeer kinds) or accept any string unchecked for DirectConnectGatewayArn until services/directconnect has real Go code, and must not silently pretend the validation is happening when it isn't."
  - "No AWS::NetworkManager::* CloudFormation resource type exists in this repo (`grep -rli networkmanager services/cloudformation/*.go` across all 147 resources_*.go-pattern files returned zero hits) -- confirmed absent, not silently skipped. This audit did not independently verify whether AWS's own real CloudFormation supports any NetworkManager resource type at all; that claim is about this repo's tree, not a verified claim about AWS's product (real AWS CloudFormation DOES have some networkmanager resource types in its public registry -- not independently re-verified against a live account this pass, flagged as an honest unknown rather than asserted either way for AWS's actual behavior)."
  - "ALL 9 confirmed taggable NetworkManager resource-ARN kinds (attachment/connect-peer/connection/core-network/device/global-network/link/peering/site) are GLOBAL ARNs with NO region segment at all (arn:${Partition}:networkmanager::${Account}:<kind>/<id> -- note the double colon, confirmed directly from AWS's own IAM Service Authorization Reference page for this service, which rendered successfully this pass unlike the docs.aws.amazon.com failures the mgn/directconnect/outposts audits hit). pkgs/arn.Build (pkgs/arn/arn.go:36-39) currently special-cases exactly one global service, `service == \"iam\"`; NetworkManager needs the identical no-region treatment added as a second case -- a strictly SIMPLER structural fix than DirectConnect's problem (where only ONE of five resource kinds, dx-gateway, was global and the rest were regional), since here the WHOLE service is uniformly global."
  - "Four association-only resource kinds (CustomerGatewayAssociation, LinkAssociation, TransitGatewayRegistration, TransitGatewayConnectPeerAssociation) carry NO Tags field in types/types.go and have NO corresponding entry in AWS's own IAM SAR resource-type table for this service -- confirmed not independently taggable/ARN-bearing resources, consistent with (not contradicting) the 9-kind list above being complete."
  - "NetworkFunctionGroup (a named service-insertion grouping referenced by SegmentActionServiceInsertion's send-via/send-to modes and RoutingPolicyDirection) has only a bare Name field in types.go -- it is a name that lives INSIDE the CoreNetworkPolicy JSON document (confirmed: CoreNetworkNetworkFunctionGroup, the variant actually returned on CoreNetwork.NetworkFunctionGroups, likewise carries no independent ARN/Tags), not an independently created/deleted API resource -- there is no CreateNetworkFunctionGroup op anywhere in this 95-op surface, confirming it exists only as policy-document content, never call it out as a missing CRUD op."
deferred:
  - "Nothing implemented yet, so nothing has been implementation-level-audited beyond the wire-shape/error-set inventory above."
leaks: {status: clean, note: "N/A -- nothing implemented yet, so there is nothing to leak. Next pass (implementation) must revisit this per parity-principles.md: AttachmentState's CREATING->AVAILABLE and PENDING_ATTACHMENT_ACCEPTANCE->(AVAILABLE|REJECTED) transitions, CoreNetworkState's CREATING->AVAILABLE, ChangeSetState's PENDING_GENERATION->READY_TO_EXECUTE->EXECUTING->EXECUTION_SUCCEEDED, RouteAnalysisStatus's RUNNING->COMPLETED, and every other *State enum's transient state (PENDING/CREATING/UPDATING/DELETING) all need timer-driven auto-advance following services/eks's scheduleClusterActivation / services/grafana's analogous pattern (both using pkgs/worker) -- Close()/Reset() wiring is mandatory, same as every other timer-driven service in this tree."}
---

## Purpose of this document

`services/networkmanager/` does not exist. This file is a pre-implementation audit: a complete SDK
operation inventory plus a behavioral spec, written so a follow-up implementation pass does not
have to re-derive wire shapes from the SDK source itself. No `.go` files were touched to produce
it. All 95 operation names, the wire protocol, every operation's exact per-op exception set, and
every shared type/enum below were read directly from
`aws-sdk-go-v2/service/networkmanager@v1.44.3`'s `serializers.go` / `deserializers.go` /
`types/types.go` / `types/enums.go` / `types/errors.go` / individual `api_op_*.go` files in the
module cache (resolved via a throwaway `go mod init probe && go get .../networkmanager@latest` in
the scratch dir — **not** added to this repo's `go.mod`, which another agent was concurrently
editing during this pass).

## 1. Complete SDK operation inventory

**95 operations**, SDK version **`v1.44.3`** (resolved 2026-08-01, whatever `@latest` currently
resolves to — not a version pinned by this audit; `go get` additionally resolved the core
`github.com/aws/aws-sdk-go-v2` module at `v1.43.3` in this session's scratch module). This matches
the task's ~95 estimate exactly:

`ls api_op_*.go | grep -v _test.go | wc -l` against
`/home/agbishop/go/pkg/mod/github.com/aws/aws-sdk-go-v2/service/networkmanager@v1.44.3/` returns
**95**.

Alphabetically: AcceptAttachment, AssociateConnectPeer, AssociateCustomerGateway, AssociateLink,
AssociateTransitGatewayConnectPeer, CreateConnectAttachment, CreateConnection, CreateConnectPeer,
CreateCoreNetwork, CreateCoreNetworkPrefixListAssociation, CreateDevice,
CreateDirectConnectGatewayAttachment, CreateGlobalNetwork, CreateLink, CreateSite,
CreateSiteToSiteVpnAttachment, CreateTransitGatewayPeering,
CreateTransitGatewayRouteTableAttachment, CreateVpcAttachment, DeleteAttachment, DeleteConnection,
DeleteConnectPeer, DeleteCoreNetwork, DeleteCoreNetworkPolicyVersion,
DeleteCoreNetworkPrefixListAssociation, DeleteDevice, DeleteGlobalNetwork, DeleteLink,
DeletePeering, DeleteResourcePolicy, DeleteSite, DeregisterTransitGateway,
DescribeGlobalNetworks, DisassociateConnectPeer, DisassociateCustomerGateway, DisassociateLink,
DisassociateTransitGatewayConnectPeer, ExecuteCoreNetworkChangeSet, GetConnectAttachment,
GetConnections, GetConnectPeer, GetConnectPeerAssociations, GetCoreNetwork,
GetCoreNetworkChangeEvents, GetCoreNetworkChangeSet, GetCoreNetworkPolicy,
GetCustomerGatewayAssociations, GetDevices, GetDirectConnectGatewayAttachment,
GetLinkAssociations, GetLinks, GetNetworkResourceCounts, GetNetworkResourceRelationships,
GetNetworkResources, GetNetworkRoutes, GetNetworkTelemetry, GetResourcePolicy, GetRouteAnalysis,
GetSites, GetSiteToSiteVpnAttachment, GetTransitGatewayConnectPeerAssociations,
GetTransitGatewayPeering, GetTransitGatewayRegistrations, GetTransitGatewayRouteTableAttachment,
GetVpcAttachment, ListAttachmentRoutingPolicyAssociations, ListAttachments, ListConnectPeers,
ListCoreNetworkPolicyVersions, ListCoreNetworkPrefixListAssociations,
ListCoreNetworkRoutingInformation, ListCoreNetworks, ListOrganizationServiceAccessStatus,
ListPeerings, ListTagsForResource, PutAttachmentRoutingPolicyLabel, PutCoreNetworkPolicy,
PutResourcePolicy, RegisterTransitGateway, RejectAttachment, RemoveAttachmentRoutingPolicyLabel,
RestoreCoreNetworkPolicyVersion, StartOrganizationServiceAccessUpdate, StartRouteAnalysis,
TagResource, UntagResource, UpdateConnection, UpdateCoreNetwork, UpdateDevice,
UpdateDirectConnectGatewayAttachment, UpdateGlobalNetwork, UpdateLink,
UpdateNetworkResourceMetadata, UpdateSite, UpdateVpcAttachment.

### Protocol and routing shape

Protocol is **REST-JSON** (`awsRestjson1_serializeOp<Op>` struct names throughout
`serializers.go`, one `HandleSerialize` per op, all 95 confirmed by direct Python-regex extraction
of every `httpbinding.SplitURI(...)` literal and `request.Method` assignment — not sampled). Unlike
mgn's action-slug-in-path convention or directconnect's header-dispatch convention, NetworkManager
uses **genuine REST resource paths with path parameters and a full HTTP-verb spread**:

- **36 `GET`, 31 `POST`, 19 `DELETE`, 9 `PATCH`** (counted directly from the Python-regex
  extraction's `request.Method` literal per op, all 95, not sampled) — all four verbs are used
  purposefully: `GET` for reads, `POST` for creates/actions, `DELETE` for deletes, `PATCH` for
  partial updates. **All 9 `PATCH` ops are exactly the 9 `Update*` ops** (`UpdateConnection`,
  `UpdateCoreNetwork`, `UpdateDevice`, `UpdateDirectConnectGatewayAttachment`,
  `UpdateGlobalNetwork`, `UpdateLink`, `UpdateNetworkResourceMetadata`, `UpdateSite`,
  `UpdateVpcAttachment`, confirmed 1:1 by direct extraction) — a real, consistent convention,
  unlike mgn where every op including updates was `POST`.
- **Two ops use `POST` for what looks like a read**: `GetNetworkRoutes` (`POST
  /global-networks/{GlobalNetworkId}/network-routes`) and `ListCoreNetworkRoutingInformation`
  (`POST /core-networks/{CoreNetworkId}/core-network-routing-information`) — both take a rich
  filter body (BGP attribute matches for the latter) too complex for query-string encoding, the
  likely reason they're POST despite being logically reads. Every other `Get*`/`List*`/`Describe*`
  op in this service is a genuine `GET`.
- **Real path-parameter nesting mirrors the resource hierarchy**: e.g. `DELETE
  /global-networks/{GlobalNetworkId}/devices/{DeviceId}`, `DELETE
  /core-networks/{CoreNetworkId}/core-network-policy-versions/{PolicyVersionId}` — a router can
  dispatch on path shape directly, unlike mgn/directconnect where the operation name itself (path
  slug or `X-Amz-Target` header) was the only signal.
- **The tagging trio and `PutResourcePolicy`/`GetResourcePolicy`/`DeleteResourcePolicy` both use a
  single generic `{ResourceArn}` path param** (`/tags/{ResourceArn}` and
  `/resource-policy/{ResourceArn}` respectively) — two structurally identical but functionally
  unrelated single-ARN-keyed resource families (AWS tags vs. a resource-based IAM policy document)
  living at sibling top-level paths.
- **Action-style, non-CRUD paths**: `POST /attachments/{AttachmentId}/accept` (AcceptAttachment),
  `POST /attachments/{AttachmentId}/reject` (RejectAttachment), `POST
  /core-networks/{CoreNetworkId}/core-network-change-sets/{PolicyVersionId}/execute`
  (ExecuteCoreNetworkChangeSet), `POST
  /core-networks/{CoreNetworkId}/core-network-policy-versions/{PolicyVersionId}/restore`
  (RestoreCoreNetworkPolicyVersion) — verb-suffixed paths for state-machine transitions rather than
  resource CRUD.

### Errors — 8 shared exception shapes, all read directly from `types/errors.go`

- **`AccessDeniedException`** {`Message`, `ErrorCodeOverride`} — client fault, no extra fields.
- **`ConflictException`** {`Message`, `ErrorCodeOverride`, `ResourceId`, `ResourceType`} — client
  fault.
- **`CoreNetworkPolicyException`** {`Message`, `ErrorCodeOverride`, `Errors
  []CoreNetworkPolicyError`{`ErrorCode`\*, `Message`\*, `Path`}} — client fault, the
  service-specific shape carrying a list of JSON-path-located policy-document errors. Appears on
  **exactly 2 ops**: `CreateCoreNetwork` and `PutCoreNetworkPolicy` (confirmed by direct per-op
  extraction) — the only two ops that accept a raw `PolicyDocument *string` as direct caller input
  (every other policy-related op works with an already-stored, already-validated document by ID/
  version, so only these two can reject malformed policy JSON at the API boundary).
- **`InternalServerException`** {`Message`, `ErrorCodeOverride`, `RetryAfterSeconds *int32`} —
  server fault; uniquely among the 8 shapes carries a retry hint.
- **`ResourceNotFoundException`** {`Message`, `ErrorCodeOverride`, `ResourceId`, `ResourceType`,
  `Context map[string]string`} — client fault; the richest not-found shape seen in this campaign so
  far (a free-form `Context` map alongside the usual `ResourceId`/`ResourceType`).
- **`ServiceQuotaExceededException`** {`Message`, `ErrorCodeOverride`, `ResourceId`, `ResourceType`,
  `LimitCode`, `ServiceCode`} — client fault.
- **`ThrottlingException`** {`Message`, `ErrorCodeOverride`, `RetryAfterSeconds *int32`} — client
  fault.
- **`ValidationException`** {`Message`, `ErrorCodeOverride`, `Reason ValidationExceptionReason`,
  `Fields []ValidationExceptionField`} — client fault. `ValidationExceptionReason`'s 4 wire values
  (`UnknownOperation`, `CannotParse`, `FieldValidationFailed`, `Other`) are **UpperCamelCase**,
  notably NOT the lower-camelCase mgn uses for its own byte-identical-in-spirit
  `ValidationExceptionReason` (`unknownOperation`/`cannotParse`/...) and NOT the
  `SCREAMING_SNAKE_CASE` every other enum in THIS service uses — a third distinct casing
  convention for what is conceptually the same generated shape across three services in this
  campaign, worth confirming per-service rather than assuming any one convention travels.

**Per-op sets, extracted from every op's own `awsRestjson1_deserializeOpError<Op>` switch in
`deserializers.go` (all 95 read individually, not sampled, counted programmatically not
eyeballed)** — **94 of the 95 ops carry the full `{AccessDeniedException,
InternalServerException, ThrottlingException, ValidationException}` set as a baseline**, plus zero
or more of `{ConflictException (59 ops), ResourceNotFoundException (81 ops),
ServiceQuotaExceededException (18 ops), CoreNetworkPolicyException (2 ops)}` depending on whether
the op mutates (Conflict), addresses an existing resource by ID (ResourceNotFound), can hit a
quota (ServiceQuotaExceeded), or accepts raw policy JSON (CoreNetworkPolicyException — only
`CreateCoreNetwork`/`PutCoreNetworkPolicy`, per above). Exactly **6 ops carry NOTHING beyond the
baseline four** (`ListConnectPeers`, `ListAttachments`, `ListCoreNetworks`, `ListPeerings`,
`GetNetworkResourceCounts`, `GetResourcePolicy` — all pure filtered-list or single-object reads
with no FK to miss and no mutation to conflict on). The lone exception to the 94-op baseline is
called out next, along with one further structural outlier:

- **`ListOrganizationServiceAccessStatus` has NO typed exception cases at all** — confirmed by
  reading its `awsRestjson1_deserializeOpErrorListOrganizationServiceAccessStatus` function body
  directly: the `switch` has only a `default` branch producing a generic `smithy.GenericAPIError`,
  with none of the 8 shapes ever matched. Every other op in this 95-op surface has at least
  `AccessDeniedException`. Do not assume this op needs the same typed-exception plumbing every
  sibling op does.
- **`GetNetworkResourceCounts` lacks `ResourceNotFoundException` despite requiring
  `GlobalNetworkId` as a path parameter**, unlike its three read-only siblings
  `GetNetworkResourceRelationships`/`GetNetworkResources`/`GetNetworkTelemetry` (all four take the
  same `GlobalNetworkId` path param; only this one has no not-found case) — confirmed by direct
  per-op read, not an extraction gap in this table.

## Family tables — every one of the 95 operations

All method/path values below come from the Python-regex extraction over `serializers.go` described
above (all 95 matched, not sampled). All error sets come from the equivalent per-op
`strings.EqualFold` extraction over `deserializers.go` (all 95, not sampled). Field lists come from
directly reading each op's `api_op_<Op>.go` Input/Output struct or the shared `types/types.go`
struct it returns. `AccessDeniedException`/`InternalServerException`/`ThrottlingException`/
`ValidationException` are abbreviated `[base4]` below since 94 of 95 ops carry this set as their
baseline (the lone exception, `ListOrganizationServiceAccessStatus`, has none at all — see above);
only additions relative to `[base4]` are called out per-op, and the 6 ops with no additions at all
(`ListConnectPeers`, `ListAttachments`, `ListCoreNetworks`, `ListPeerings`,
`GetNetworkResourceCounts`, `GetResourcePolicy`) are marked `[base4] only` in their own rows.

### A. Global Networks — root container (4 ops)

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| CreateGlobalNetwork | POST /global-networks | Description, Tags | `GlobalNetwork`{GlobalNetworkId, GlobalNetworkArn, CreatedAt, Description, State (GlobalNetworkState: PENDING/AVAILABLE/DELETING/UPDATING), Tags} | [base4]+Conflict+ServiceQuotaExceeded |
| UpdateGlobalNetwork | PATCH /global-networks/{GlobalNetworkId} | GlobalNetworkId*, Description | `GlobalNetwork` | [base4]+Conflict+ResourceNotFound |
| DeleteGlobalNetwork | DELETE /global-networks/{GlobalNetworkId} | GlobalNetworkId* | `GlobalNetwork` (state DELETING) | [base4]+Conflict+ResourceNotFound |
| DescribeGlobalNetworks | GET /global-networks | GlobalNetworkIds[] (optional filter), MaxResults, NextToken | `GlobalNetworks []GlobalNetwork`, NextToken | [base4]+ResourceNotFound (no Conflict — pure read) |

### B. Sites (4 ops)

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| CreateSite | POST /global-networks/{GlobalNetworkId}/sites | GlobalNetworkId*, Description, `Location *Location`{Address, Latitude, Longitude — all free-form strings, no geocoding}, Tags | `Site`{SiteId, SiteArn, GlobalNetworkId, CreatedAt, Description, Location, State (SiteState: PENDING/AVAILABLE/DELETING/UPDATING), Tags} | [base4]+Conflict+ResourceNotFound+ServiceQuotaExceeded |
| UpdateSite | PATCH /global-networks/{GlobalNetworkId}/sites/{SiteId} | GlobalNetworkId*, SiteId*, Description, Location | `Site` | [base4]+Conflict+ResourceNotFound |
| DeleteSite | DELETE /global-networks/{GlobalNetworkId}/sites/{SiteId} | GlobalNetworkId*, SiteId* | `Site` (state DELETING) | [base4]+Conflict+ResourceNotFound |
| GetSites | GET /global-networks/{GlobalNetworkId}/sites | GlobalNetworkId*, SiteIds[] (optional filter), MaxResults, NextToken | `Sites []Site`, NextToken | [base4]+ResourceNotFound |

### C. Devices (4 ops)

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| CreateDevice | POST /global-networks/{GlobalNetworkId}/devices | GlobalNetworkId*, `AWSLocation *AWSLocation`{SubnetArn, Zone} (for a device that IS itself an AWS resource, e.g. an appliance instance in a subnet), Description, Location (physical address, mutually relevant with AWSLocation for a real on-prem device), Model, SerialNumber, SiteId, Tags, Type, Vendor | `Device`{DeviceId, DeviceArn, GlobalNetworkId, AWSLocation, CreatedAt, Description, Location, Model, SerialNumber, SiteId, State (DeviceState: PENDING/AVAILABLE/DELETING/UPDATING), Tags, Type, Vendor} | [base4]+Conflict+ResourceNotFound+ServiceQuotaExceeded |
| UpdateDevice | PATCH /global-networks/{GlobalNetworkId}/devices/{DeviceId} | GlobalNetworkId*, DeviceId*, AWSLocation, Description, Location, Model, SerialNumber, SiteId, Type, Vendor | `Device` | [base4]+Conflict+ResourceNotFound |
| DeleteDevice | DELETE /global-networks/{GlobalNetworkId}/devices/{DeviceId} | GlobalNetworkId*, DeviceId* | `Device` (state DELETING) | [base4]+Conflict+ResourceNotFound |
| GetDevices | GET /global-networks/{GlobalNetworkId}/devices | GlobalNetworkId*, DeviceIds[], MaxResults, NextToken, SiteId (filter) | `Devices []Device`, NextToken | [base4]+ResourceNotFound |

### D. Links (4 ops)

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| CreateLink | POST /global-networks/{GlobalNetworkId}/links | `Bandwidth *Bandwidth`*{DownloadSpeed, UploadSpeed — Mbps, both plain int32, purely descriptive/self-reported, no enforcement possible}, GlobalNetworkId*, SiteId*, Description, Provider, Tags, Type | `Link`{LinkId, LinkArn, Bandwidth, GlobalNetworkId, SiteId, CreatedAt, Description, Provider, State (LinkState: PENDING/AVAILABLE/DELETING/UPDATING), Tags, Type} | [base4]+Conflict+ResourceNotFound+ServiceQuotaExceeded |
| UpdateLink | PATCH /global-networks/{GlobalNetworkId}/links/{LinkId} | GlobalNetworkId*, LinkId*, Bandwidth, Description, Provider, Type | `Link` | [base4]+Conflict+ResourceNotFound+ServiceQuotaExceeded (the only Update* op with ServiceQuotaExceeded — presumably bandwidth-tier-change quota checks) |
| DeleteLink | DELETE /global-networks/{GlobalNetworkId}/links/{LinkId} | GlobalNetworkId*, LinkId* | `Link` (state DELETING) | [base4]+Conflict+ResourceNotFound |
| GetLinks | GET /global-networks/{GlobalNetworkId}/links | GlobalNetworkId*, LinkIds[], MaxResults, NextToken, Provider (filter), SiteId (filter), Type (filter) | `Links []Link`, NextToken | [base4]+ResourceNotFound |

### E. Link associations (3 ops)

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| AssociateLink | POST /global-networks/{GlobalNetworkId}/link-associations | DeviceId*, GlobalNetworkId*, LinkId* | `LinkAssociation`{DeviceId, GlobalNetworkId, LinkAssociationState (LinkAssociationState: PENDING/AVAILABLE/DELETING/DELETED), LinkId} | [base4]+Conflict+ResourceNotFound+ServiceQuotaExceeded |
| DisassociateLink | DELETE /global-networks/{GlobalNetworkId}/link-associations | DeviceId*, GlobalNetworkId*, LinkId* (all three as body/query fields even on a DELETE — no `{DeviceId}/{LinkId}` path nesting, unusual for this service's otherwise-consistent path-param convention) | `LinkAssociation` (state DELETING) | [base4]+Conflict+ResourceNotFound |
| GetLinkAssociations | GET /global-networks/{GlobalNetworkId}/link-associations | GlobalNetworkId*, DeviceId (filter), LinkId (filter), MaxResults, NextToken | `LinkAssociations []LinkAssociation`, NextToken | [base4]+ResourceNotFound |

### F. Connections — on-prem device-to-device (4 ops)

Note the name collision: this "Connection" is a Global-Networks-side logical link between two
Devices over a Link (BGP or IPsec, per `ConnectionType`), entirely distinct from a Cloud WAN
"Connect attachment"/"Connect peer" despite sharing the English word.

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| CreateConnection | POST /global-networks/{GlobalNetworkId}/connections | ConnectedDeviceId*, DeviceId*, GlobalNetworkId*, ConnectedLinkId, Description, LinkId, Tags | `Connection`{ConnectionId, ConnectionArn, ConnectedDeviceId, DeviceId, GlobalNetworkId, ConnectedLinkId, LinkId, CreatedAt, Description, State (ConnectionState: PENDING/AVAILABLE/DELETING/UPDATING), Tags} — no `ConnectionType`/`ConnectionStatus` fields exposed at create time (both live only via `GetNetworkTelemetry`'s `ConnectionHealth`, not on `Connection` itself, confirmed by direct struct read) | [base4]+Conflict+ResourceNotFound(**absent — see note**)+ServiceQuotaExceeded |
| UpdateConnection | PATCH /global-networks/{GlobalNetworkId}/connections/{ConnectionId} | ConnectionId*, GlobalNetworkId*, ConnectedLinkId, Description, LinkId | `Connection` | [base4]+Conflict+ResourceNotFound |
| DeleteConnection | DELETE /global-networks/{GlobalNetworkId}/connections/{ConnectionId} | ConnectionId*, GlobalNetworkId* | `Connection` (state DELETING) | [base4]+Conflict+ResourceNotFound |
| GetConnections | GET /global-networks/{GlobalNetworkId}/connections | GlobalNetworkId*, ConnectionIds[], DeviceId (filter), MaxResults, NextToken | `Connections []Connection`, NextToken | [base4]+ResourceNotFound |

Note on `CreateConnection`'s error set: confirmed by direct read, it genuinely lacks
`ResourceNotFoundException` despite referencing `ConnectedDeviceId`/`DeviceId`/`ConnectedLinkId`/
`LinkId`, all of which must already exist — every sibling `Create*` op with similar FK-style
required references (e.g. `CreateSite`→no FK, `CreateDevice`→`SiteId` optional FK which DOES carry
`ResourceNotFound`) suggests this may be an SDK-model oversight rather than a deliberate design,
but this audit reports it as read, not as corrected.

### G. Customer Gateway Associations (3 ops)

Binds an **EC2 `CustomerGatewayArn`** (a real, already-existing gopherstack resource —
`services/ec2/advanced_networking.go:109-115`) to a Global-Networks `DeviceId`/`LinkId`, letting
NetworkManager model which physical device an on-prem customer gateway's BGP session terminates on.

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| AssociateCustomerGateway | POST /global-networks/{GlobalNetworkId}/customer-gateway-associations | CustomerGatewayArn*, DeviceId*, GlobalNetworkId*, LinkId | `CustomerGatewayAssociation`{CustomerGatewayArn, DeviceId, GlobalNetworkId, LinkId, State (CustomerGatewayAssociationState: PENDING/AVAILABLE/DELETING/DELETED)} | [base4]+Conflict+ResourceNotFound+ServiceQuotaExceeded |
| DisassociateCustomerGateway | DELETE /global-networks/{GlobalNetworkId}/customer-gateway-associations/{CustomerGatewayArn} | CustomerGatewayArn* (path param — the only Associate/Disassociate pair in this family group where Disassociate uses a real path param instead of query/body fields), GlobalNetworkId* | `CustomerGatewayAssociation` (state DELETING) | [base4]+Conflict+ResourceNotFound |
| GetCustomerGatewayAssociations | GET /global-networks/{GlobalNetworkId}/customer-gateway-associations | GlobalNetworkId*, CustomerGatewayArns[] (filter), MaxResults, NextToken | `CustomerGatewayAssociations []CustomerGatewayAssociation`, NextToken | [base4]+Conflict+ResourceNotFound (the only `Get*` list op in this service carrying `ConflictException` — unusual for a pure read) |

### H. Transit Gateway Registrations (3 ops)

Binds an **EC2 `TransitGatewayArn`** (real backend: `services/ec2/vpcs.go:217-225`,
`InMemoryBackend.CreateTransitGateway`/`DescribeTransitGateways`/`DeleteTransitGateway`/
`ModifyTransitGateway`, `services/ec2/transit_gateways.go:70-225`) into a Global Network — a
prerequisite both for classic route/telemetry visibility into that TGW AND for Cloud WAN peering
(`CreateTransitGatewayPeering` requires the TGW be registered first, per real AWS semantics, not
independently SDK-enforced here).

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| RegisterTransitGateway | POST /global-networks/{GlobalNetworkId}/transit-gateway-registrations | GlobalNetworkId*, TransitGatewayArn* | `TransitGatewayRegistration`{GlobalNetworkId, TransitGatewayArn, `State *TransitGatewayRegistrationStateReason`{Code (TransitGatewayRegistrationState: PENDING/AVAILABLE/DELETING/DELETED/FAILED — 5 values, the only registration/association state enum in this service with a FAILED terminal state), Message}} | [base4]+Conflict+ResourceNotFound |
| DeregisterTransitGateway | DELETE /global-networks/{GlobalNetworkId}/transit-gateway-registrations/{TransitGatewayArn} | GlobalNetworkId*, TransitGatewayArn* (path param) | `TransitGatewayRegistration` (state DELETING) | [base4]+Conflict+ResourceNotFound |
| GetTransitGatewayRegistrations | GET /global-networks/{GlobalNetworkId}/transit-gateway-registrations | GlobalNetworkId*, TransitGatewayArns[] (filter), MaxResults, NextToken | `TransitGatewayRegistrations []TransitGatewayRegistration`, NextToken | [base4]+ResourceNotFound |

### I. Transit Gateway Connect Peer Associations (3 ops)

Binds an **EC2 TransitGatewayConnectPeer** (real backend:
`services/ec2/handler_transit_gateway_peering.go`, `InMemoryBackend.CreateTransitGatewayConnect`/
`CreateTransitGatewayConnectPeer`/`DeleteTransitGatewayConnectPeer`) to a Global-Networks
`DeviceId`/`LinkId`.

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| AssociateTransitGatewayConnectPeer | POST /global-networks/{GlobalNetworkId}/transit-gateway-connect-peer-associations | DeviceId*, GlobalNetworkId*, TransitGatewayConnectPeerArn*, LinkId | `TransitGatewayConnectPeerAssociation`{DeviceId, GlobalNetworkId, LinkId, State (TransitGatewayConnectPeerAssociationState: PENDING/AVAILABLE/DELETING/DELETED), TransitGatewayConnectPeerArn} | [base4]+Conflict+ResourceNotFound+ServiceQuotaExceeded |
| DisassociateTransitGatewayConnectPeer | DELETE /global-networks/{GlobalNetworkId}/transit-gateway-connect-peer-associations/{TransitGatewayConnectPeerArn} | GlobalNetworkId*, TransitGatewayConnectPeerArn* (path param) | `TransitGatewayConnectPeerAssociation` (state DELETING) | [base4]+Conflict+ResourceNotFound |
| GetTransitGatewayConnectPeerAssociations | GET /global-networks/{GlobalNetworkId}/transit-gateway-connect-peer-associations | GlobalNetworkId*, TransitGatewayConnectPeerArns[] (filter), MaxResults, NextToken | `TransitGatewayConnectPeerAssociations []TransitGatewayConnectPeerAssociation`, NextToken | [base4]+Conflict+ResourceNotFound |

### J. ConnectPeer ↔ Global Network association (3 ops) — the cross-product bridge

A genuine, structural link between the Global-Networks half and the Cloud WAN half: an already
Cloud-WAN-created `ConnectPeer` (family K below) gets bound to a specific on-prem `DeviceId`/
`LinkId` here. **This is the concrete reason not to implement the two halves as fully independent
subsystems** — see Missing simulated functionality.

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| AssociateConnectPeer | POST /global-networks/{GlobalNetworkId}/connect-peer-associations | ConnectPeerId*, DeviceId*, GlobalNetworkId*, LinkId | `ConnectPeerAssociation`{ConnectPeerId, DeviceId, GlobalNetworkId, LinkId, State (ConnectPeerAssociationState: PENDING/AVAILABLE/DELETING/DELETED)} | [base4]+Conflict+ResourceNotFound+ServiceQuotaExceeded |
| DisassociateConnectPeer | DELETE /global-networks/{GlobalNetworkId}/connect-peer-associations/{ConnectPeerId} | ConnectPeerId* (path param), GlobalNetworkId* | `ConnectPeerAssociation` (state DELETING) | [base4]+Conflict+ResourceNotFound |
| GetConnectPeerAssociations | GET /global-networks/{GlobalNetworkId}/connect-peer-associations | GlobalNetworkId*, ConnectPeerIds[] (filter), MaxResults, NextToken | `ConnectPeerAssociations []ConnectPeerAssociation`, NextToken | [base4]+Conflict+ResourceNotFound |

### K. Connect Peers — Cloud WAN side (4 ops)

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| CreateConnectPeer | POST /connect-peers | ConnectAttachmentId*, PeerAddress*, `BgpOptions *BgpOptions`{PeerAsn *int64}, ClientToken, CoreNetworkAddress, InsideCidrBlocks[], SubnetArn, Tags | `ConnectPeer`{ConnectPeerId, ConnectAttachmentId, CoreNetworkId, CreatedAt, `Configuration *ConnectPeerConfiguration`{BgpConfigurations []ConnectPeerBgpConfiguration{CoreNetworkAddress,CoreNetworkAsn,PeerAddress,PeerAsn}, CoreNetworkAddress, InsideCidrBlocks, PeerAddress, Protocol (TunnelProtocol: GRE/NO_ENCAP)}, EdgeLocation, State (ConnectPeerState: CREATING/FAILED/AVAILABLE/DELETING — no PENDING, only 4 values), SubnetArn, Tags} | [base4]+Conflict+ResourceNotFound |
| DeleteConnectPeer | DELETE /connect-peers/{ConnectPeerId} | ConnectPeerId* | `ConnectPeer` (state DELETING) | [base4]+Conflict+ResourceNotFound |
| GetConnectPeer | GET /connect-peers/{ConnectPeerId} | ConnectPeerId* | `ConnectPeer` | [base4]+ResourceNotFound |
| ListConnectPeers | GET /connect-peers | ConnectAttachmentId (filter), CoreNetworkId (filter), MaxResults, NextToken | `ConnectPeers []ConnectPeerSummary`{ConnectAttachmentId, ConnectPeerId, ConnectPeerState, CoreNetworkId, CreatedAt, ...}, NextToken | [base4] only (no ResourceNotFound — pure filtered list) |

### L. Core Networks (5 ops)

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| CreateCoreNetwork | POST /core-networks | GlobalNetworkId*, ClientToken, Description, PolicyDocument, Tags | `CoreNetwork`{CoreNetworkArn, CoreNetworkId, CreatedAt, Description, Edges []CoreNetworkEdge{Asn,EdgeLocation,InsideCidrBlocks}, GlobalNetworkId, NetworkFunctionGroups []CoreNetworkNetworkFunctionGroup, Segments []CoreNetworkSegment{EdgeLocations,Name,SharedSegments}, State (CoreNetworkState: CREATING/UPDATING/AVAILABLE/DELETING), Tags} | [base4]+Conflict+CoreNetworkPolicy+ServiceQuotaExceeded |
| UpdateCoreNetwork | PATCH /core-networks/{CoreNetworkId} | CoreNetworkId*, Description (ONLY field — no way to change GlobalNetworkId or any network-shape field via this op; all real network-shape changes go through PutCoreNetworkPolicy) | `CoreNetwork` | [base4]+Conflict+ResourceNotFound |
| DeleteCoreNetwork | DELETE /core-networks/{CoreNetworkId} | CoreNetworkId* | `CoreNetwork` (state DELETING) | [base4]+Conflict+ResourceNotFound |
| GetCoreNetwork | GET /core-networks/{CoreNetworkId} | CoreNetworkId* | `CoreNetwork` | [base4]+ResourceNotFound |
| ListCoreNetworks | GET /core-networks | MaxResults, NextToken (no filters at all — always all core networks in the account) | `CoreNetworks []CoreNetworkSummary`, NextToken | [base4] only |

### M. Core Network Policy lifecycle (8 ops) — the versioned-policy + change-set state machine

See Missing simulated functionality below for the full narrative; this table is the wire shapes.

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| PutCoreNetworkPolicy | POST /core-networks/{CoreNetworkId}/core-network-policy | CoreNetworkId*, PolicyDocument*, ClientToken, Description, LatestVersionId (optimistic-concurrency guard against a concurrent Put) | `CoreNetworkPolicy`{Alias (CoreNetworkPolicyAlias: LIVE/LATEST), ChangeSetState (ChangeSetState: PENDING_GENERATION/FAILED_GENERATION/READY_TO_EXECUTE/EXECUTING/EXECUTION_SUCCEEDED/OUT_OF_DATE — 6 values), CoreNetworkId, CreatedAt, Description, PolicyDocument, PolicyErrors []CoreNetworkPolicyError, PolicyVersionId} — doc comment: "Creates a new, immutable version... A subsequent change set is created showing the differences between the LIVE policy and the submitted policy." | [base4]+Conflict+CoreNetworkPolicy+ResourceNotFound |
| GetCoreNetworkPolicy | GET /core-networks/{CoreNetworkId}/core-network-policy | CoreNetworkId*, Alias (LIVE\|LATEST, defaults presumably to LATEST if omitted — not confirmed in SDK), PolicyVersionId (explicit version, alternative to Alias) | `CoreNetworkPolicy` | [base4]+ResourceNotFound |
| ListCoreNetworkPolicyVersions | GET /core-networks/{CoreNetworkId}/core-network-policy-versions | CoreNetworkId*, MaxResults, NextToken | `CoreNetworkPolicyVersions []CoreNetworkPolicyVersion`, NextToken | [base4]+ResourceNotFound |
| DeleteCoreNetworkPolicyVersion | DELETE /core-networks/{CoreNetworkId}/core-network-policy-versions/{PolicyVersionId} | CoreNetworkId*, PolicyVersionId* | `CoreNetworkPolicy` — doc comment: "You can't delete the current LIVE policy." (real, enforceable invariant) | [base4]+Conflict+ResourceNotFound |
| RestoreCoreNetworkPolicyVersion | POST /core-networks/{CoreNetworkId}/core-network-policy-versions/{PolicyVersionId}/restore | CoreNetworkId*, PolicyVersionId* | `CoreNetworkPolicy` — doc comment: "Restores a previous policy version as a new, immutable version... A subsequent change set is created" (i.e. this creates yet ANOTHER new PolicyVersionId, copying the old content — it does not rewind history, it forks forward from it) | [base4]+Conflict+ResourceNotFound |
| GetCoreNetworkChangeSet | GET /core-networks/{CoreNetworkId}/core-network-change-sets/{PolicyVersionId} | CoreNetworkId*, PolicyVersionId*, MaxResults, NextToken | `CoreNetworkChanges []CoreNetworkChange`{Action (ChangeAction: ADD/MODIFY/REMOVE), Identifier, IdentifierPath (e.g. "CORE_NETWORK_SEGMENT/us-east-1/devsegment"), NewValues/PreviousValues *CoreNetworkChangeValues{Asn,AttachmentId,Cidr,DestinationIdentifier,DnsSupport,...}, Type (ChangeType, 14 values)}, NextToken — doc comment: "Returns a change set between the LIVE core network policy and a submitted policy." | [base4]+ResourceNotFound |
| GetCoreNetworkChangeEvents | GET /core-networks/{CoreNetworkId}/core-network-change-events/{PolicyVersionId} | CoreNetworkId*, PolicyVersionId*, MaxResults, NextToken | `CoreNetworkChangeEvents []CoreNetworkChangeEvent`{Action, EventTime, IdentifierPath, Status (ChangeStatus: NOT_STARTED/IN_PROGRESS/COMPLETE/FAILED), Values *CoreNetworkChangeEventValues{AttachmentId,Cidr,EdgeLocation,NetworkFunctionGroupName,PeerEdgeLocation,...}}, NextToken — per-change EXECUTION progress (as ExecuteCoreNetworkChangeSet deploys), distinct from GetCoreNetworkChangeSet's pre-execution DIFF PREVIEW | [base4]+ResourceNotFound |
| ExecuteCoreNetworkChangeSet | POST /core-networks/{CoreNetworkId}/core-network-change-sets/{PolicyVersionId}/execute | CoreNetworkId*, PolicyVersionId* | empty (void — real, per direct read of the Output struct: only `ResultMetadata`) | [base4]+Conflict+ResourceNotFound — doc comment: "Executes a change set on your core network. Deploys changes globally based on the policy submitted." This is what flips the executed `PolicyVersionId` to become the new LIVE alias target. |

### N. Core Network Prefix List Associations (3 ops)

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| CreateCoreNetworkPrefixListAssociation | POST /prefix-list | CoreNetworkId*, PrefixListAlias*, PrefixListArn*, ClientToken | CoreNetworkId, PrefixListAlias, PrefixListArn (echoed flat, no nested struct) | [base4]+Conflict+ResourceNotFound+ServiceQuotaExceeded |
| DeleteCoreNetworkPrefixListAssociation | DELETE /prefix-list/{PrefixListArn}/core-network/{CoreNetworkId} | CoreNetworkId*, PrefixListArn* (both path params) | CoreNetworkId, PrefixListArn (echoed flat) | [base4]+Conflict+ResourceNotFound+ServiceQuotaExceeded |
| ListCoreNetworkPrefixListAssociations | GET /prefix-list/core-network/{CoreNetworkId} | CoreNetworkId*, MaxResults, NextToken, PrefixListArn (filter) | `PrefixListAssociations []PrefixListAssociation`{CoreNetworkId, PrefixListAlias, PrefixListArn}, NextToken | [base4]+ResourceNotFound |

`PrefixListArn` almost certainly refers to an EC2-managed customer prefix list
(`ManagedPrefixList`) — this repo's `services/ec2` was not searched this pass for a matching
prefix-list backend; flagged as an open cross-service-binding question for the implementer, not
independently confirmed either way here.

### O. Core Network Routing Information (1 op)

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| ListCoreNetworkRoutingInformation | POST /core-networks/{CoreNetworkId}/core-network-routing-information | CoreNetworkId*, EdgeLocation*, SegmentName*, CommunityMatches[], ExactAsPathMatches[], LocalPreferenceMatches[], MedMatches[], NextHopFilters map[string][]string, MaxResults, NextToken | `CoreNetworkRoutingInformation []CoreNetworkRoutingInformation`{AsPath[], Communities[], LocalPreference, Med, NextHop *RoutingInformationNextHop, ...}, NextToken | [base4]+ResourceNotFound |

Genuinely a BGP route table read filtered by BGP path attributes (AS path, community strings,
local preference, MED) — real, honestly derivable ONLY if the emulator actually tracks these BGP
attributes per learned/propagated route; a first pass with no real route-propagation engine should
return an empty list rather than inventing plausible AS-path/community values, per the same
principle as directconnect's BGP-peering honesty note.

### P. Attachment Routing Policy (3 ops)

A distinct, newer sub-feature layering named "routing policy labels" onto attachments to control
inbound/outbound route propagation direction (`RoutingPolicyDirection`: `inbound`/`outbound`,
lowercase) and service-insertion mode (`SegmentActionServiceInsertion`: `send-via`/`send-to`,
`SendViaMode`: `dual-hop`/`single-hop`, all lowercase — a third enum-casing convention alongside
the `SCREAMING_SNAKE_CASE` majority and `ValidationExceptionReason`'s `UpperCamelCase`, confirmed
directly in `types/enums.go`).

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| PutAttachmentRoutingPolicyLabel | POST /routing-policy-label | AttachmentId*, CoreNetworkId*, RoutingPolicyLabel*, ClientToken | AttachmentId, CoreNetworkId, RoutingPolicyLabel (echoed flat) | [base4]+Conflict+ResourceNotFound+ServiceQuotaExceeded |
| RemoveAttachmentRoutingPolicyLabel | DELETE /routing-policy-label/core-network/{CoreNetworkId}/attachment/{AttachmentId} | AttachmentId*, CoreNetworkId* (both path params) | AttachmentId, CoreNetworkId, RoutingPolicyLabel (echoed) | [base4]+Conflict+ResourceNotFound+ServiceQuotaExceeded |
| ListAttachmentRoutingPolicyAssociations | GET /routing-policy-label/core-network/{CoreNetworkId} | CoreNetworkId*, AttachmentId (filter), MaxResults, NextToken | `AttachmentRoutingPolicyAssociationSummary`{AssociatedRoutingPolicies[], AttachmentId, PendingRoutingPolicies[], RoutingPolicyLabel}, NextToken | [base4]+ResourceNotFound |

### Q. Attachment — generic lifecycle across all 5 subtypes (4 ops)

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| AcceptAttachment | POST /attachments/{AttachmentId}/accept | AttachmentId* | `Attachment` (state PENDING_ATTACHMENT_ACCEPTANCE → CREATING/AVAILABLE) | [base4]+Conflict+ResourceNotFound |
| RejectAttachment | POST /attachments/{AttachmentId}/reject | AttachmentId* | `Attachment` (state → REJECTED) | [base4]+Conflict+ResourceNotFound |
| DeleteAttachment | DELETE /attachments/{AttachmentId} | AttachmentId* | `Attachment` (state → DELETING) | [base4]+Conflict+ResourceNotFound |
| ListAttachments | GET /attachments | AttachmentType (filter, one of 5 AttachmentType values), CoreNetworkId (filter), EdgeLocation (filter), State (filter, one of 9 AttachmentState values), MaxResults, NextToken | `Attachments []Attachment`, NextToken | [base4] only (no ResourceNotFound — pure filtered list, matching the K/ListConnectPeers pattern) |

Base `Attachment` shape (shared by all 5 subtypes, each of which wraps it as `Attachment
*Attachment` plus subtype-specific fields — see Q1-Q5 below): AttachmentId, AttachmentPolicyRuleNumber,
AttachmentType (`AttachmentType`: CONNECT/SITE_TO_SITE_VPN/VPC/DIRECT_CONNECT_GATEWAY/
TRANSIT_GATEWAY_ROUTE_TABLE — 5 values), CoreNetworkArn, CoreNetworkId, CreatedAt, EdgeLocation
(single string, all types except Direct Connect Gateway) / EdgeLocations (`[]string`, Direct
Connect Gateway only — confirmed by doc comment cross-reference on both fields), LastModificationErrors
[]AttachmentError{Code (AttachmentErrorCode, 13 values e.g. VPC_NOT_FOUND/SUBNET_NOT_FOUND/
DIRECT_CONNECT_GATEWAY_EXISTING_ATTACHMENTS), Message, RequestId, ResourceArn},
NetworkFunctionGroupName, OwnerAccountId, ProposedNetworkFunctionGroupChange, ProposedSegmentChange,
ResourceArn, SegmentName, State (`AttachmentState`: REJECTED/PENDING_ATTACHMENT_ACCEPTANCE/
CREATING/FAILED/AVAILABLE/UPDATING/PENDING_NETWORK_UPDATE/PENDING_TAG_ACCEPTANCE/DELETING — 9
values, the richest state enum in this service), Tags, UpdatedAt.

### Q1. VPC Attachments (3 ops)

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| CreateVpcAttachment | POST /vpc-attachments | CoreNetworkId*, SubnetArns*[], VpcArn*, ClientToken, `Options *VpcOptions`{ApplianceModeSupport, DnsSupport, Ipv6Support, SecurityGroupReferencingSupport — note doc comment: SecurityGroupReferencingSupport defaults true at the attachment level but false at the core-network-policy level, a real cross-layer default asymmetry}, RoutingPolicyLabel, Tags | `VpcAttachment`{Attachment, Options, SubnetArns} | [base4]+Conflict+ResourceNotFound |
| GetVpcAttachment | GET /vpc-attachments/{AttachmentId} | AttachmentId* | `VpcAttachment` | [base4]+ResourceNotFound |
| UpdateVpcAttachment | PATCH /vpc-attachments/{AttachmentId} | AttachmentId*, AddSubnetArns[], Options, RemoveSubnetArns[] | `VpcAttachment` | [base4]+Conflict+ResourceNotFound |

`VpcArn`/`SubnetArns` bind against this repo's real `services/ec2` `VPC`/`Subnet` types
(`services/ec2/store.go:263`/`:273`) — a genuine, buildable cross-service validation opportunity
(see Cross-service wiring).

### Q2. Connect Attachments (2 ops)

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| CreateConnectAttachment | POST /connect-attachments | CoreNetworkId*, EdgeLocation*, `Options *ConnectAttachmentOptions`*{Protocol (TunnelProtocol: GRE/NO_ENCAP)}, TransportAttachmentId* (the underlying VPC or Direct Connect Gateway attachment this Connect attachment rides on top of — a real, checkable FK), ClientToken, RoutingPolicyLabel, Tags | `ConnectAttachment`{Attachment, Options, TransportAttachmentId} | [base4]+Conflict+ResourceNotFound |
| GetConnectAttachment | GET /connect-attachments/{AttachmentId} | AttachmentId* | `ConnectAttachment` | [base4]+ResourceNotFound |

No `UpdateConnectAttachment`/`DeleteConnectAttachment` op exists — updates/deletes go through the
generic `DeleteAttachment` (family Q) since `ConnectAttachment` has no subtype-specific mutable
fields beyond what the base `Attachment` already covers.

### Q3. Site-to-Site VPN Attachments (2 ops)

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| CreateSiteToSiteVpnAttachment | POST /site-to-site-vpn-attachments | CoreNetworkId*, VpnConnectionArn*, ClientToken, RoutingPolicyLabel, Tags | `SiteToSiteVpnAttachment`{Attachment, VpnConnectionArn} | [base4]+Conflict+ResourceNotFound |
| GetSiteToSiteVpnAttachment | GET /site-to-site-vpn-attachments/{AttachmentId} | AttachmentId* | `SiteToSiteVpnAttachment` | [base4]+ResourceNotFound |

`VpnConnectionArn` binds against this repo's real `services/ec2` `VpnConnection`
(`services/ec2/advanced_networking.go:118-129`, backend `CreateVpnConnection`/
`DescribeVpnConnections`/`DeleteVpnConnection` at `services/ec2/vpn_connections.go:99/142/169`) —
note `VpnConnection` already carries its own `TransitGatewayID`/`VpnGatewayID` fields, so this
attachment is validating a resource that itself may already reference a registered TGW.

### Q4. Direct Connect Gateway Attachments (3 ops)

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| CreateDirectConnectGatewayAttachment | POST /direct-connect-gateway-attachments | CoreNetworkId*, DirectConnectGatewayArn*, EdgeLocations*[]string (plural — the only attachment-create op taking multiple edge locations at once, confirmed against the base Attachment's own EdgeLocations-vs-EdgeLocation split above), ClientToken, RoutingPolicyLabel, Tags | `DirectConnectGatewayAttachment`{Attachment, DirectConnectGatewayArn} | [base4]+Conflict+ResourceNotFound |
| GetDirectConnectGatewayAttachment | GET /direct-connect-gateway-attachments/{AttachmentId} | AttachmentId* | `DirectConnectGatewayAttachment` | [base4]+ResourceNotFound |
| UpdateDirectConnectGatewayAttachment | PATCH /direct-connect-gateway-attachments/{AttachmentId} | AttachmentId*, EdgeLocations[] (replace list) | `DirectConnectGatewayAttachment` | [base4]+Conflict+ResourceNotFound |

`DirectConnectGatewayArn` has no real backend to validate against yet: `services/directconnect/`
contains only `PARITY.md`, confirmed via `ls services/directconnect/` — zero `.go` files exist.
Until that service has working code, this attachment kind's ARN must be accepted as an opaque
string, clearly flagged as unvalidated, not silently treated as if cross-checked.

### Q5. Transit Gateway Route Table Attachments (2 ops)

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| CreateTransitGatewayRouteTableAttachment | POST /transit-gateway-route-table-attachments | PeeringId* (NOT a bare TransitGatewayArn — requires an existing Peering, i.e. `CreateTransitGatewayPeering` must run first), TransitGatewayRouteTableArn*, ClientToken, RoutingPolicyLabel, Tags | `TransitGatewayRouteTableAttachment`{Attachment, PeeringId, TransitGatewayRouteTableArn} | [base4]+Conflict+ResourceNotFound |
| GetTransitGatewayRouteTableAttachment | GET /transit-gateway-route-table-attachments/{AttachmentId} | AttachmentId* | `TransitGatewayRouteTableAttachment` | [base4]+ResourceNotFound |

`TransitGatewayRouteTableArn` binds against this repo's real `services/ec2`
`TransitGatewayRouteTable` (`services/ec2/ec2core.go:70-77`, backend
`CreateTransitGatewayRouteTable` at `services/ec2/ec2core.go:388`) — a genuine, buildable
cross-service validation.

### R. Peerings (4 ops)

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| CreateTransitGatewayPeering | POST /transit-gateway-peerings | CoreNetworkId*, TransitGatewayArn*, ClientToken, Tags | `TransitGatewayPeering`{`Peering *Peering`{CoreNetworkArn, CoreNetworkId, CreatedAt, EdgeLocation, LastModificationErrors []PeeringError, OwnerAccountId, PeeringId, PeeringType (PeeringType: TRANSIT_GATEWAY — currently the ONLY value, confirmed 1-value enum), ResourceArn, State (PeeringState: CREATING/FAILED/AVAILABLE/DELETING), Tags}, TransitGatewayArn, TransitGatewayPeeringAttachmentId} | [base4]+Conflict+ResourceNotFound |
| GetTransitGatewayPeering | GET /transit-gateway-peerings/{PeeringId} | PeeringId* | `TransitGatewayPeering` | [base4]+ResourceNotFound |
| DeletePeering | DELETE /peerings/{PeeringId} | PeeringId* | `Peering` (state DELETING) — generic across peering types, returns the base `Peering`, not the `TransitGatewayPeering` wrapper | [base4]+Conflict+ResourceNotFound |
| ListPeerings | GET /peerings | CoreNetworkId (filter), EdgeLocation (filter), MaxResults, NextToken, PeeringType (filter), State (filter) | `Peerings []Peering`, NextToken | [base4] only |

`PeeringErrorCode` (6 values: TRANSIT_GATEWAY_NOT_FOUND, TRANSIT_GATEWAY_PEERS_LIMIT_EXCEEDED,
MISSING_PERMISSIONS, INTERNAL_ERROR, EDGE_LOCATION_PEER_DUPLICATE, INVALID_TRANSIT_GATEWAY_STATE) —
since `PeeringType` has exactly one real value today, the generic/subtype-specific split
(`DeletePeering`/`ListPeerings` vs. `CreateTransitGatewayPeering`/`GetTransitGatewayPeering`) is
future-proofing for a peering kind that does not exist yet in this SDK version, not a distinction
with two live cases.

### S. Route Analysis (2 ops)

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| StartRouteAnalysis | POST /global-networks/{GlobalNetworkId}/route-analyses | `Destination *RouteAnalysisEndpointOptionsSpecification`*{IpAddress, TransitGatewayAttachmentArn}, GlobalNetworkId*, `Source *RouteAnalysisEndpointOptionsSpecification`*, IncludeReturnPath (bool), UseMiddleboxes (bool) | `RouteAnalysis`{Destination/Source *RouteAnalysisEndpointOptions{IpAddress,TransitGatewayArn,TransitGatewayAttachmentArn}, `ForwardPath`/`ReturnPath *RouteAnalysisPath`{CompletionStatus *RouteAnalysisCompletion{ReasonCode (RouteAnalysisCompletionReasonCode, 11 values), ResultCode (RouteAnalysisCompletionResultCode: CONNECTED/NOT_CONNECTED)}, Path []PathComponent{DestinationCidrBlock, Resource *NetworkResourceSummary{Definition,IsMiddlebox,NameTag,RegisteredGatewayArn,ResourceArn,ResourceType}, Sequence}}, GlobalNetworkId, IncludeReturnPath, OwnerAccountId, RouteAnalysisId, Source, `Status` (RouteAnalysisStatus: RUNNING/COMPLETED/FAILED)} | [base4]+Conflict+ResourceNotFound |
| GetRouteAnalysis | GET /global-networks/{GlobalNetworkId}/route-analyses/{RouteAnalysisId} | GlobalNetworkId*, RouteAnalysisId* | `RouteAnalysis` | [base4]+ResourceNotFound |

See Missing simulated functionality for the honest feasibility assessment — this is the single
riskiest fabrication surface in the whole service.

### T. Network introspection — resources, telemetry, routes, relationships (5 ops)

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| GetNetworkResources | GET /global-networks/{GlobalNetworkId}/network-resources | GlobalNetworkId*, AccountId, AwsRegion, CoreNetworkId, RegisteredGatewayArn, ResourceArn, ResourceType (all optional filters), MaxResults, NextToken | `NetworkResources []NetworkResource`{AccountId, AwsRegion, CoreNetworkId, Definition (JSON string — doc comment: "Network Manager gets this information by describing the resource using its Describe API call", i.e. a real describe-and-serialize of the underlying resource, genuinely derivable if the emulator actually calls into the referenced service's own Describe handler), DefinitionTimestamp, Metadata, ...}, NextToken | [base4]+ResourceNotFound |
| GetNetworkResourceCounts | GET /global-networks/{GlobalNetworkId}/network-resource-count | GlobalNetworkId*, ResourceType (filter), MaxResults, NextToken | `NetworkResourceCounts []NetworkResourceCount`{Count, ResourceType}, NextToken | [base4] only (no ResourceNotFound — see wire-shape traps) |
| GetNetworkResourceRelationships | GET /global-networks/{GlobalNetworkId}/network-resource-relationships | GlobalNetworkId*, AccountId, AwsRegion, CoreNetworkId, RegisteredGatewayArn, ResourceArn, ResourceType, MaxResults, NextToken | `Relationships []Relationship`{From, To — both bare ARN strings, a directed edge}, NextToken | [base4]+ResourceNotFound |
| GetNetworkRoutes | POST /global-networks/{GlobalNetworkId}/network-routes | GlobalNetworkId*, `RouteTableIdentifier *RouteTableIdentifier`*{CoreNetworkNetworkFunctionGroup, CoreNetworkSegmentEdge, TransitGatewayRouteTableArn — exactly one expected}, DestinationFilters map[string][]string, ExactCidrMatches[], LongestPrefixMatch, PrefixListIds[], States[] (RouteState filter), SubnetOfMatches[], SupernetOfMatches[], Types[] (RouteType filter) | `CoreNetworkSegmentEdge`, `NetworkRoutes []NetworkRoute`{DestinationCidrBlock, Destinations []NetworkRouteDestination{CoreNetworkAttachmentId,EdgeLocation,NetworkFunctionGroupName,ResourceId,ResourceType,SegmentName}, PrefixListId, State (RouteState: ACTIVE/BLACKHOLE), Type (RouteType: PROPAGATED/STATIC)}, RouteTableArn, RouteTableTimestamp, RouteTableType (RouteTableType: TRANSIT_GATEWAY_ROUTE_TABLE/CORE_NETWORK_SEGMENT/NETWORK_FUNCTION_GROUP) | [base4]+ResourceNotFound |
| GetNetworkTelemetry | GET /global-networks/{GlobalNetworkId}/network-telemetry | GlobalNetworkId*, AccountId, AwsRegion, CoreNetworkId, RegisteredGatewayArn, ResourceArn, ResourceType, MaxResults, NextToken | `NetworkTelemetry []NetworkTelemetry`{AccountId, Address, AwsRegion, CoreNetworkId, `Health *ConnectionHealth`{Status (ConnectionStatus: UP/DOWN), Timestamp, Type (ConnectionType: BGP/IPSEC)}, RegisteredGatewayArn, ResourceArn, ResourceId, ResourceType}, NextToken | [base4]+ResourceNotFound |

See Missing simulated functionality for which of these five are honestly derivable from modeled
state (all but `GetNetworkTelemetry` are largely mechanical rollups over already-real resource
records) versus which require data this emulator cannot honestly produce
(`GetNetworkTelemetry.Health` — see gaps).

### U. Update network resource metadata (1 op)

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| UpdateNetworkResourceMetadata | PATCH /global-networks/{GlobalNetworkId}/network-resources/{ResourceArn}/metadata | GlobalNetworkId*, Metadata* map[string]string, ResourceArn* (path param) | Metadata, ResourceArn (echoed) | [base4]+Conflict+ResourceNotFound |

A caller-supplied free-form key-value annotation map on a registered resource ARN, independent of
AWS `Tags` — a real, simple, honestly-simulatable side-store keyed by `ResourceArn`.

### V. AWS Organizations integration (2 ops)

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| StartOrganizationServiceAccessUpdate | POST /organizations/service-access | Action* (free-form `*string`, not a typed enum — presumably `ENABLE`/`DISABLE` per general AWS convention, NOT independently confirmed as a closed enum in this SDK) | `OrganizationStatus`{AccountStatusList []AccountStatus, OrganizationAwsServiceAccessStatus *string ("ENABLED"/"DISABLED" per doc comment, not typed), OrganizationId, SLRDeploymentStatus *string ("SUCCEEDED"/"IN_PROGRESS" per doc comment, not typed)} | [base4]+Conflict+ServiceQuotaExceeded |
| ListOrganizationServiceAccessStatus | GET /organizations/service-access | MaxResults, NextToken | NextToken, `OrganizationStatus` | **NONE — the only op in this 95-op surface with zero typed exception cases** (confirmed by direct read of its deserializer switch; see wire-shape traps) |

Real AWS-Organizations delegated-administration bookkeeping — this repo was not searched this pass
for an existing Organizations backend to bind against; flagged as an open question, not confirmed
either way.

### W. Resource-based policy (3 ops)

Structurally distinct from the CoreNetworkPolicy (network-configuration) document despite the
shared English word "policy" — this is a resource-based IAM-style JSON policy attached to a
NetworkManager resource ARN (used for cross-account sharing of e.g. a core network).

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| PutResourcePolicy | POST /resource-policy/{ResourceArn} | PolicyDocument*, ResourceArn* (path param) | empty (void) | [base4]+Conflict+ServiceQuotaExceeded (no ResourceNotFound — presumably creates-if-absent) |
| GetResourcePolicy | GET /resource-policy/{ResourceArn} | ResourceArn* | PolicyDocument | [base4] only |
| DeleteResourcePolicy | DELETE /resource-policy/{ResourceArn} | ResourceArn* | empty (void) | [base4]+Conflict |

### X. Tagging (3 ops)

| Op | Method/Path | Key in | Key out | Errors |
|---|---|---|---|---|
| TagResource | POST /tags/{ResourceArn} | ResourceArn* (path param), Tags* []Tag | empty | [base4]+Conflict+ResourceNotFound+ServiceQuotaExceeded |
| UntagResource | DELETE /tags/{ResourceArn} | ResourceArn*, TagKeys* []string | empty | [base4]+Conflict+ResourceNotFound |
| ListTagsForResource | GET /tags/{ResourceArn} | ResourceArn* | `TagList []Tag` | [base4]+ResourceNotFound |

Every taggable resource type carries its own inline `Tags []Tag` field already (`GlobalNetwork`,
`Site`, `Device`, `Link`, `Connection`, `ConnectPeer`, `CoreNetwork`, `Peering`, `Attachment` — 9
struct kinds confirmed by direct grep, see Cross-service wiring) — this generic ARN-keyed API is
the cross-cutting way to read/mutate all of them, not a separate tag store.

## 2. Missing simulated functionality (the real emulation work)

Network Manager genuinely spans two related but historically distinct AWS products, exactly as
framed in this task, **with one real structural bridge between them** (family J,
AssociateConnectPeer/DisassociateConnectPeer/GetConnectPeerAssociations) that this audit confirmed
by direct field read — implementers should not build the two halves as fully isolated subsystems.

### Global Networks / on-premises modeling (families A-J)

`GlobalNetwork` ⊃ `Site` ⊃ `Device` ⊃ `Link` (via `AssociateLink`) is the base container hierarchy
— all four confirmed real, simple CRUD with a real `PENDING`→`AVAILABLE`, `*`→`DELETING`→(gone)
state machine (`GlobalNetworkState`/`SiteState`/`DeviceState`/`LinkState` are all 4-value,
byte-identical shape: `PENDING`/`AVAILABLE`/`DELETING`/`UPDATING`). On top of that, five distinct
**association** families bind an on-prem `DeviceId`/`LinkId` to something else:
`CustomerGatewayAssociation` (EC2 CustomerGateway), `TransitGatewayRegistration` (EC2
TransitGateway — registered at the `GlobalNetwork` level, not Device/Link), `LinkAssociation`
(pure Device↔Link), `TransitGatewayConnectPeerAssociation` (EC2 TransitGatewayConnectPeer), and
`ConnectPeerAssociation` (a Cloud WAN `ConnectPeer` — the cross-product bridge). All five have
real, buildable backends to validate against in this repo already (see Cross-service wiring) — this
half of the service is honestly, fully simulatable as real cross-referenced state, not just
opaque-string bookkeeping, PROVIDED the implementer actually validates the referenced ARNs against
`services/ec2`'s real stores rather than accepting any string.

`Connection` (family F) is a real on-prem device-to-device logical/physical link
(`ConnectionType`: BGP/IPSEC) distinct from a Cloud WAN Connect attachment — its own
`ConnectionState` (PENDING/AVAILABLE/DELETING/UPDATING) is honestly simulatable bookkeeping; its
operational up/down health (`ConnectionStatus`/`ConnectionHealth`, surfaced only via
`GetNetworkTelemetry`, not on `Connection` itself) is the one place in this family where honest
simulation runs out — see the telemetry discussion below.

### Cloud WAN (families K-S)

**Core Network Policy lifecycle** (family M), the real state machine, confirmed from doc comments
and enum values directly (not the task's sketch, though it turned out to match closely):

1. `PutCoreNetworkPolicy` creates a new, immutable `PolicyVersionId` and (per its own doc comment)
   triggers generation of a change set comparing it against the current LIVE policy —
   `ChangeSetState` starts at `PENDING_GENERATION`, moves to `READY_TO_EXECUTE` once the diff is
   computed (or `FAILED_GENERATION` if the new document is structurally invalid — surfaced via
   `CoreNetworkPolicyException`/`PolicyErrors`), and can go `OUT_OF_DATE` if a newer version is put
   before this one executes.
2. `GetCoreNetworkChangeSet` returns the **pre-execution diff preview**
   (`CoreNetworkChange`{Action: ADD/MODIFY/REMOVE, per `ChangeType`, 14 real values covering
   segments/edges/attachment-mappings/routing-policy associations/etc.} — this is a real structural
   diff of the policy JSON, not decorative).
3. `ExecuteCoreNetworkChangeSet` (void output) deploys the change set — `ChangeSetState` moves
   `READY_TO_EXECUTE`→`EXECUTING`→`EXECUTION_SUCCEEDED`, and the executed `PolicyVersionId` becomes
   the new `LIVE` alias target (confirmed: `CoreNetworkPolicyAlias` has exactly 2 values, `LIVE`
   and `LATEST` — `GetCoreNetworkPolicy(Alias=LIVE)` and `GetCoreNetworkPolicy(Alias=LATEST)` are
   NOT the same version until execution happens).
4. `GetCoreNetworkChangeEvents` tracks **per-change EXECUTION progress** as step 3 runs
   (`ChangeStatus`: NOT_STARTED/IN_PROGRESS/COMPLETE/FAILED per individual
   `IdentifierPath`-addressed change) — a genuinely distinct, finer-grained signal from the
   pre-execution diff in step 2, easy to conflate but structurally separate (confirmed: different
   ops, different output element types, `CoreNetworkChange` vs. `CoreNetworkChangeEvent`).
5. `RestoreCoreNetworkPolicyVersion` forks a NEW version from an OLD version's content (its own
   doc comment: "Restores a previous policy version as a new, immutable version... a subsequent
   change set is created" — it does not rewind, it re-submits old content as new).
6. `DeleteCoreNetworkPolicyVersion`'s doc comment states plainly: "You can't delete the current
   LIVE policy" — a real, enforceable invariant, not decorative.

This entire lifecycle is honestly, fully simulatable as pure state-machine bookkeeping over
caller-supplied JSON — the ONE piece requiring real work rather than a trivial status-flip is
computing `CoreNetworkChange`'s ADD/MODIFY/REMOVE diff (step 2) by actually parsing and comparing
the policy JSON's segments/network-function-groups/attachment-policies sections, since the policy
document is caller-supplied JSON this repo CAN parse, not an AWS-internal opaque format. A
first-pass implementation that flips `ChangeSetState` correctly but returns an empty
`CoreNetworkChanges` list should flag that explicitly as a scoped-down first pass, not present it
as a full diff.

**Attachment state machine and cross-account acceptance** (families Q, Q1-Q5): every attachment
subtype (`VpcAttachment`/`ConnectAttachment`/`SiteToSiteVpnAttachment`/
`DirectConnectGatewayAttachment`/`TransitGatewayRouteTableAttachment`) wraps the same base
`Attachment` shape and shares the same 9-value `AttachmentState`
(`REJECTED`/`PENDING_ATTACHMENT_ACCEPTANCE`/`CREATING`/`FAILED`/`AVAILABLE`/`UPDATING`/
`PENDING_NETWORK_UPDATE`/`PENDING_TAG_ACCEPTANCE`/`DELETING`). The cross-account flow, inferred
from field/enum semantics (real AWS behavior, e.g. whether `RequireAcceptance` is a per-core-network
policy setting, was not independently confirmed beyond what the SDK types expose): a `Create*`
op for any subtype lands the new attachment in `CREATING` (same-account/no-approval-required
policy) or `PENDING_ATTACHMENT_ACCEPTANCE` (cross-account, or the core network's policy requires
manual acceptance); `AcceptAttachment`/`RejectAttachment` (generic, family Q) resolve that pending
state to `CREATING`→`AVAILABLE` or `REJECTED` respectively. `ProposedSegmentChange`/
`ProposedNetworkFunctionGroupChange` on the base `Attachment` describe an in-flight reassignment
(e.g. moving an attachment to a different segment) that itself may require re-acceptance —
`PENDING_NETWORK_UPDATE` is the state for that case specifically, distinct from initial-creation
`PENDING_ATTACHMENT_ACCEPTANCE`. This is real, honestly-simulatable state-machine work with no
fabrication risk — the risk is entirely in getting the specific state-transition rules right, which
this audit flags as inferred-not-confirmed (see gaps) rather than pretending certainty.

**Route analysis (`StartRouteAnalysis`/`GetRouteAnalysis`) — honest feasibility assessment**: this
computes a real forward/return path through a Transit-Gateway-attachment-and-route-table-centric
topology. `RouteAnalysisCompletionReasonCode`'s 11 real values
(`TRANSIT_GATEWAY_ATTACHMENT_NOT_FOUND`, `TRANSIT_GATEWAY_ATTACHMENT_NOT_IN_TRANSIT_GATEWAY`,
`CYCLIC_PATH_DETECTED`, `TRANSIT_GATEWAY_ATTACHMENT_STABLE_ROUTE_TABLE_NOT_FOUND`,
`ROUTE_NOT_FOUND`, `BLACKHOLE_ROUTE_FOR_DESTINATION_FOUND`,
`INACTIVE_ROUTE_FOR_DESTINATION_FOUND`, `TRANSIT_GATEWAY_ATTACHMENT_ATTACH_ARN_NO_MATCH`,
`MAX_HOPS_EXCEEDED`, `POSSIBLE_MIDDLEBOX`, `NO_DESTINATION_ARN_PROVIDED`) describe a REAL
route-table-walking algorithm with real failure modes (cycle detection, a 64-hop limit per the
type's own doc comment, blackhole/inactive route detection). **This is feasible, not automatically
fabrication**: `services/ec2` has real `TransitGatewayRouteTable`/`TransitGatewayRoute`
(`services/ec2/ec2core.go:70-80`) and `TransitGatewayVpcAttachment`
(`services/ec2/accept_ops.go:84-93`) state a genuine graph-walk implementation could traverse hop
by hop, producing a real `PathComponent` sequence with real `ResourceArn`/`ResourceType` values
pulled from actually-existing EC2 resources. The fabrication risk is specifically in SKIPPING the
real walk and inventing a plausible-looking `PathComponent` list or a hardcoded
`RouteAnalysisCompletionResultCode: CONNECTED` — if implemented, this MUST actually resolve
next-hops through the modeled route tables and attachments, actually detect cycles/hop limits,
and return `NOT_CONNECTED` with a real reason code when the modeled topology genuinely doesn't
connect. A first pass that cannot do this real walk should return `Status: FAILED` or an honestly
empty analysis, explicitly flagged as unimplemented, rather than a fabricated `CONNECTED` result.

**Network introspection ops (family T) — which are honestly derivable vs. which require
invention**:

- **`GetNetworkResources`**: honestly derivable IF the implementer actually calls into each
  referenced service's own Describe/Get handler and serializes the real result into `Definition`
  — the doc comment says exactly this is what real AWS does ("Network Manager gets this
  information by describing the resource using its Describe API call"). Returning a
  `Definition` that wasn't actually produced by describing the real underlying resource would be
  fabrication.
- **`GetNetworkResourceCounts`**: a trivial, honest aggregate COUNT over whatever
  `GetNetworkResources` already tracks — no invention needed, this is arithmetic over real state.
- **`GetNetworkResourceRelationships`**: a directed-edge (`From`/`To` ARN pairs) graph over
  already-modeled attachment/registration relationships — honestly derivable from the same
  association records families A-J already track (e.g. a VPC attachment's `From` is the
  `VpcArn`, `To` is the `CoreNetworkArn`).
- **`GetNetworkRoutes`**: honestly derivable ONLY to the extent the emulator tracks real route
  propagation (STATIC routes a caller/policy explicitly configured, PROPAGATED routes actually
  computed from attachment/segment mappings) — same honesty bar as
  `ListCoreNetworkRoutingInformation` above; an empty route list is honest, an invented plausible
  one is not.
- **`GetNetworkTelemetry`**: **the one genuinely underivable piece**. `ConnectionHealth.Status`
  (UP/DOWN) reflects real AWS device/link telemetry (SNMP-style polling of actual on-prem hardware,
  actual BGP/IPsec session liveness) with no honest analog here. A defensible default is `Status:
  UP` for every `Connection`/`ConnectPeer`/attachment that has reached its `AVAILABLE` state
  (deterministic, not fabricated variance), explicitly documented as "nothing is actually polled,
  this reflects modeled-state availability only" — never invented flapping, latency, or
  degraded-health values designed to look like real monitoring data, which is exactly the
  fabrication class `parity-principles.md` forbids.

## 3. Cross-service wiring needed

### Tagging (`resourcegroupstaggingapi`)

Yes — `TagResource`/`UntagResource`/`ListTagsForResource` are real, native NetworkManager ops
(family X above), so this belongs in `wireResourceGroupsTagging` in `cli.go`
(`/home/agbishop/gopherstack/cli.go:5357`, currently wires exactly 30 services enumerated in its
own doc comment at `cli.go:5339-5343`, most recently `wireTaggingGrafana(bk, byName["Grafana"])` at
`cli.go:5408`). NetworkManager would need its own `wireTaggingNetworkManager`-style function
following the `wireTaggingCtxARNResources` generic helper (`cli.go:5520-5537`, used by e.g.
`wireTaggingGrafana`/`wireTaggingEFS`) — **9 distinct taggable resource kinds** share the one
`networkmanager` ARN namespace (`GlobalNetwork`/`Site`/`Device`/`Link`/`Connection`/`ConnectPeer`/
`CoreNetwork`/`Peering`/`Attachment` all carry `Tags []Tag`, confirmed by direct per-struct grep —
below mgn's 12 (the highest in this campaign) but above directconnect's 5 and
outposts'/resiliencehub's 2), so the tag store backing this wiring needs `resourceTypeFromARN`-style
dispatch (`cli.go:5568-5580`) across at least
`global-network`/`site`/`device`/`link`/`connection`/`connect-peer`/`core-network`/`peering`/
`attachment` resource-type prefixes, not a single flat map.

**ARN namespace and format — confirmed directly from AWS's own IAM Service Authorization Reference
page** (`https://docs.aws.amazon.com/service-authorization/latest/reference/list_networkmanager.html`,
fetched and rendered successfully this pass — unlike the JS-shell failures the mgn/directconnect/
outposts audits hit on the same domain). Service prefix confirmed **`networkmanager`** (matching
the package name exactly, not a divergent case). The resource-types table lists exactly 9 kinds,
**every single one a GLOBAL ARN with no region segment** (note the double colon after
`Partition`):

- `attachment`: `arn:${Partition}:networkmanager::${Account}:attachment/${ResourceId}`
- `connect-peer`: `arn:${Partition}:networkmanager::${Account}:connect-peer/${ResourceId}`
- `connection`: `arn:${Partition}:networkmanager::${Account}:connection/${GlobalNetworkId}/${ResourceId}`
- `core-network`: `arn:${Partition}:networkmanager::${Account}:core-network/${ResourceId}`
- `device`: `arn:${Partition}:networkmanager::${Account}:device/${GlobalNetworkId}/${ResourceId}`
- `global-network`: `arn:${Partition}:networkmanager::${Account}:global-network/${ResourceId}`
- `link`: `arn:${Partition}:networkmanager::${Account}:link/${GlobalNetworkId}/${ResourceId}`
- `peering`: `arn:${Partition}:networkmanager::${Account}:peering/${ResourceId}`
- `site`: `arn:${Partition}:networkmanager::${Account}:site/${GlobalNetworkId}/${ResourceId}`

Three of the nine (`connection`/`device`/`link`) nest under `${GlobalNetworkId}` in their resource
path; the rest are flat `${ResourceId}`. This 9-kind list matches exactly (confirmed by
cross-checking each of the 8 struct types read directly in `types/types.go`) the set of types
carrying a `Tags []Tag` field — the four association-only kinds
(`CustomerGatewayAssociation`/`LinkAssociation`/`TransitGatewayRegistration`/
`TransitGatewayConnectPeerAssociation`) have neither a `Tags` field nor an entry in this ARN table,
consistent, not contradictory.

**pkgs/arn.Build's global-service gap — simpler here than DirectConnect's.**
`pkgs/arn/arn.go:36-39` special-cases exactly one global service today: `service == "iam"` (region
omitted). NetworkManager needs the identical treatment (`service == "networkmanager"` added as a
second case) — and because EVERY NetworkManager resource kind is global (unlike DirectConnect,
where only `dx-gateway` was global and the other four kinds — `dxcon`/`dxlag`/`dxvif` regional —
needed a resource-kind-level exception `pkgs/arn` doesn't support today), this is a clean
SERVICE-level addition matching the existing `iam` special-case shape exactly, not a new kind of
exception `pkgs/arn` has never handled before.

### EC2 cross-service binding — real, existing backends to validate against

Every claim below is a real file:line this audit read directly, not inferred:

- **`TransitGateway`** (`services/ec2/vpcs.go:217-225`): `ID`, `Arn`, `Description`, `State`,
  `OwnerID`, `Options`. Backend: `InMemoryBackend.CreateTransitGateway`/
  `DescribeTransitGateways(ids)`/`DeleteTransitGateway`/`ModifyTransitGateway`
  (`services/ec2/transit_gateways.go:70-225`). Binds `RegisterTransitGateway`/
  `DeregisterTransitGateway`'s `TransitGatewayArn` and `CreateTransitGatewayPeering`'s
  `TransitGatewayArn`.
- **`VpnGateway`** (`services/ec2/advanced_networking.go:100-106`): `VpnGatewayID`, `State`,
  `Type`, `AttachedVPCID`, `AttachmentState`. Backend:
  `InMemoryBackend.CreateVpnGateway`/`DescribeVpnGateways(ids)`/`DeleteVpnGateway`/
  `AttachVpnGateway`/`DetachVpnGateway` (`services/ec2/vpn_gateways.go:13-131`). No direct
  NetworkManager op references a bare `VpnGatewayArn`, but `VpnConnection` (below) carries
  `VpnGatewayID`, so an indirect binding chain exists.
- **`CustomerGateway`** (`services/ec2/advanced_networking.go:109-115`): `CustomerGatewayID`,
  `State`, `Type`, `BgpAsn`, `IPAddress`. Backend: `InMemoryBackend.CreateCustomerGateway`/
  `DescribeCustomerGateways(ids)`/`DeleteCustomerGateway` (`services/ec2/vpn_gateways.go:131/164/
  192`, handlers at `services/ec2/handler_vpn_gateways.go:85/108/127`). Binds
  `AssociateCustomerGateway`'s `CustomerGatewayArn` (note: `CustomerGateway` has no `Arn` field
  today, just a bare `CustomerGatewayID` string — an implementer would need to either add one or
  construct the ARN string on the fly using `pkgs/arn` to match what NetworkManager expects).
- **`VpnConnection`** (`services/ec2/advanced_networking.go:118-129`): `VpnConnectionID`, `State`,
  `CustomerGatewayID`, `VpnGatewayID`, `TransitGatewayID`, `Type`, `Category`,
  `CustomerGatewayConfiguration`, `VgwTelemetry`, `Options`. Backend:
  `InMemoryBackend.CreateVpnConnection`/`DescribeVpnConnections(ids)`/`DeleteVpnConnection`
  (`services/ec2/vpn_connections.go:99/142/169`). Binds `CreateSiteToSiteVpnAttachment`'s
  `VpnConnectionArn`.
- **`TransitGatewayVpcAttachment`** (`services/ec2/accept_ops.go:84-93`):
  `TransitGatewayAttachmentID`, `TransitGatewayID`, `VpcID`, `State`, `SubnetIDs`. Backend:
  `InMemoryBackend.CreateTransitGatewayVpcAttachment`/`DescribeTransitGatewayVpcAttachments`
  (`services/ec2/networking1.go:67/100`, handlers at `services/ec2/handler_networking1.go:209/229`).
  Not directly referenced by any NetworkManager op signature, but conceptually the EC2-side
  counterpart to a NetworkManager `VpcAttachment`.
- **`TransitGatewayRouteTable`** (`services/ec2/ec2core.go:70-77`): `TransitGatewayID`,
  `RouteTableID`, `State`, `DefaultAssociation`, `DefaultPropagation`. Backend:
  `InMemoryBackend.CreateTransitGatewayRouteTable` (`services/ec2/ec2core.go:388`). Binds
  `CreateTransitGatewayRouteTableAttachment`'s `TransitGatewayRouteTableArn` and is the real
  route-table state `StartRouteAnalysis`/`GetNetworkRoutes` would need to walk.
- **TransitGatewayConnectPeer** (no exported top-level Go type found by name; state lives inside
  `services/ec2/handler_transit_gateway_peering.go`'s response-serialization structs, e.g.
  `tgwConnectPeerItem`, `TransitGatewayConnectPeerID`): Backend:
  `InMemoryBackend.CreateTransitGatewayConnect`/`CreateTransitGatewayConnectPeer`/
  `DeleteTransitGatewayConnectPeer` (`services/ec2/handler_transit_gateway_peering.go:195/235/256`
  and corresponding un-searched backend-package functions of the same names). Binds
  `AssociateTransitGatewayConnectPeer`'s `TransitGatewayConnectPeerArn`. This audit did not locate
  an exported `TransitGatewayConnectPeer` struct definition by grep in the time available for this
  pass — flagged as needing a closer look at `services/ec2`'s TGW-peering-adjacent files before
  implementation, not asserted as absent.
- **`VPC`/`Subnet`** (`services/ec2/store.go:263`/`273`): binds `CreateVpcAttachment`'s
  `VpcArn`/`SubnetArns`.

**Direct Connect Gateway attachments (family Q4) have no real backend to bind against today** —
`services/directconnect/` contains only `PARITY.md`, zero `.go` files (confirmed via `ls
services/directconnect/`). Until that service exists as working code, `DirectConnectGatewayArn`
must be accepted unvalidated, clearly flagged as such.

### CloudFormation

No `AWS::NetworkManager::*` resource type exists in `services/cloudformation/` — grepped
case-insensitively for "networkmanager" across all 147 `resources_*.go`-pattern files in that
directory, zero hits. Confirmed absent, not silently skipped. This audit did not independently
re-verify whether AWS's own real CloudFormation registry supports NetworkManager resource types;
that claim is scoped to this repo's tree only.

### Grep results for prior references

`grep -rni "networkmanager\|corenetwork\|globalnetwork\|core-network\|global-network" services/
cli.go` returns exactly 4 hits, **all four inside `services/directconnect/PARITY.md`**, all noting
that DirectConnect's `AssociatedCoreNetwork` field (Cloud WAN core-network attachment reference on
`DirectConnectGatewayAssociation`) has "no backing service... in this tree at all" — i.e. these are
a PRIOR AUDIT correctly flagging the absence of exactly this service, not a partial implementation
of it. There is no other NetworkManager-adjacent state anywhere in this tree to cross-reference —
this is a genuinely from-scratch service, matching directconnect's own "no prior references"
finding rather than outposts' scattered-`OutpostArn`-fields situation.

## 4. Honest gap list

See the machine-readable `gaps:` list in the frontmatter for the authoritative version. Summary:

1. Zero operations implemented — this document is the spec, not an audit of running code.
2. Route analysis (`StartRouteAnalysis`/`GetRouteAnalysis`) requires a REAL route-table graph walk
   over modeled EC2 Transit Gateway state (feasible, not automatically fabrication) — but only if
   actually implemented as a real walk; a result that isn't derived from an actual traversal must
   be flagged as gap, never presented as a real analysis.
3. `GetNetworkTelemetry`'s connection-health data has no honest real-telemetry analog; a
   deterministic "UP once AVAILABLE" default is defensible, invented flapping/degraded values are
   not.
4. `CoreNetworkChange`'s ADD/MODIFY/REMOVE diff requires actually parsing/diffing the caller's
   policy JSON — real, buildable, but more work than the CRUD shell around it; an empty
   change-list shortcut must be flagged as scoped-down, not presented as a full diff.
5. `DirectConnectGatewayArn` (family Q4) has no real cross-service backend to validate against —
   `services/directconnect/` is PARITY.md-only, zero `.go` files.
6. `CustomerGateway` (`services/ec2/advanced_networking.go:109-115`) has no `Arn` field today —
   binding `AssociateCustomerGateway`'s `CustomerGatewayArn` requires either adding one to that EC2
   struct or constructing the ARN on the fly.
7. This audit could not locate an exported top-level `TransitGatewayConnectPeer` Go struct in
   `services/ec2` in the time available — the backend methods
   (`CreateTransitGatewayConnectPeer`/`DeleteTransitGatewayConnectPeer`) exist, confirmed by
   function-name grep, but the exact struct shape needs a closer read before implementation.
8. No `AWS::NetworkManager::*` CloudFormation resource type exists in this repo; AWS's own real
   CloudFormation support was not independently re-verified.
9. `ListOrganizationServiceAccessStatus` has zero typed exception cases — every error condition
   falls through to a generic API error; do not build typed-exception plumbing for it that mirrors
   its 94 siblings.
10. Attachment cross-account acceptance transition rules (`PENDING_ATTACHMENT_ACCEPTANCE` vs.
    `PENDING_NETWORK_UPDATE` vs. `PENDING_TAG_ACCEPTANCE`, and exactly which create-time conditions
    land in `CREATING` directly vs. requiring `AcceptAttachment`) are this audit's inference from
    enum/field semantics, not independently confirmed against real AWS behavior — flag as a
    deliberate implementation choice if built this way.
11. `PrefixListArn` (`CreateCoreNetworkPrefixListAssociation` family) likely refers to an
    EC2-managed prefix list; this repo's `services/ec2` was not searched this pass for a matching
    backend to validate against.

## Top 5 hardest/riskiest things about implementing this service

1. **Route analysis is the single largest fabrication trap in this service.** Unlike mgn's
   Network Migration analysis/codegen (which this campaign already correctly flagged as
   unbuildable without a real engine), NetworkManager's route analysis IS feasible against real
   modeled EC2 Transit Gateway route-table state — which makes the temptation to skip the real
   graph walk and return a plausible-looking `CONNECTED` result with an invented `PathComponent`
   list especially dangerous: it would look like a genuine feature rather than an obvious stub.
   Either build the real hop-by-hop walk (with real cycle detection and the documented 64-hop
   limit) or explicitly ship it as `Status: FAILED`/gap — no defensible middle ground exists.
2. **The Core Network Policy lifecycle's LIVE/LATEST alias split plus change-set generation and
   execution is a genuine multi-version state machine** (`ChangeSetState`'s 6 values, the
   `PolicyVersionId` history, "can't delete the current LIVE policy") that must track which
   version is LIVE independently from which is LATEST until `ExecuteCoreNetworkChangeSet` runs —
   getting the alias resolution wrong (e.g. treating LATEST as always LIVE) would silently break
   every downstream `GetCoreNetworkPolicy(Alias=LIVE)` caller in a way unit tests keyed only on the
   happy path might not catch.
3. **Nine ARN resource kinds are ALL global (no region segment)** — a structural property this
   audit could confirm cleanly this pass (via AWS's own IAM SAR page, which rendered where three
   prior audits' attempts on the same domain failed), but `pkgs/arn.Build` still needs a new
   `service == "networkmanager"` case added before ANY of this service's tagging or ARN-building
   code can be wire-correct — a small, mechanical fix, but one that will silently produce
   wrong (regional) ARNs for every single resource in this service if skipped.
4. **Five attachment subtypes share one base `Attachment` shape, one 9-value `AttachmentState`,
   and one generic Accept/Reject/Delete/List op family, but each subtype has its own
   create-time required fields and its own cross-service FK to validate** (VPC's `VpcArn`/
   `SubnetArns`, Connect's `TransportAttachmentId`, Site-to-Site VPN's `VpnConnectionArn`, Direct
   Connect Gateway's `DirectConnectGatewayArn` — currently unbindable, no real backend — and
   Transit Gateway Route Table's `PeeringId`+`TransitGatewayRouteTableArn`). A generic "attachment
   CRUD" implementation that treats all five uniformly will get the FK-validation step wrong for at
   least the Direct Connect Gateway case (nothing real to validate against yet) and needs an
   explicit per-subtype validation branch for the rest.
5. **The Global-Networks/Cloud-WAN "two products" framing has one real, easy-to-miss bridge**:
   `AssociateConnectPeer`/`DisassociateConnectPeer`/`GetConnectPeerAssociations` binds a Cloud-WAN
   `ConnectPeer` to a Global-Networks `DeviceId`/`LinkId`. An implementation that builds these as
   two fully separate subsystems (e.g. separate backend structs with no cross-references) will
   have nowhere natural to hang this association, and will likely either drop the feature silently
   or bolt it on awkwardly later — worth designing the two halves' backend state with this one
   linkage in mind from the start, not as an afterthought.
