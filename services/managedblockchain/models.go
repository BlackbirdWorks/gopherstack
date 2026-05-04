package managedblockchain

import "time"

// Network represents an Amazon Managed Blockchain network.
type Network struct {
	CreationDate     *time.Time        `json:"creationDate"`
	Tags             map[string]string `json:"tags"`
	Arn              string            `json:"arn"`
	Description      string            `json:"description"`
	Framework        string            `json:"framework"`
	FrameworkVersion string            `json:"frameworkVersion"`
	ID               string            `json:"id"`
	Name             string            `json:"name"`
	Status           string            `json:"status"`
}

// NetworkSummary is the short form returned by ListNetworks.
type NetworkSummary struct {
	CreationDate     *time.Time `json:"creationDate"`
	Arn              string     `json:"arn"`
	Description      string     `json:"description"`
	Framework        string     `json:"framework"`
	FrameworkVersion string     `json:"frameworkVersion"`
	ID               string     `json:"id"`
	Name             string     `json:"name"`
	Status           string     `json:"status"`
}

// Member represents a member within a Managed Blockchain network.
type Member struct {
	CreationDate *time.Time        `json:"creationDate"`
	Tags         map[string]string `json:"tags"`
	Arn          string            `json:"arn"`
	Description  string            `json:"description"`
	ID           string            `json:"id"`
	Name         string            `json:"name"`
	NetworkID    string            `json:"networkID"`
	Status       string            `json:"status"`
}

// MemberSummary is the short form returned by ListMembers.
type MemberSummary struct {
	CreationDate *time.Time `json:"creationDate"`
	Arn          string     `json:"arn"`
	Description  string     `json:"description"`
	ID           string     `json:"id"`
	Name         string     `json:"name"`
	Status       string     `json:"status"`
}

// Node represents a peer node within a Managed Blockchain member.
type Node struct {
	CreationDate     *time.Time        `json:"creationDate"`
	Tags             map[string]string `json:"tags"`
	Arn              string            `json:"arn"`
	AvailabilityZone string            `json:"availabilityZone"`
	ID               string            `json:"id"`
	InstanceType     string            `json:"instanceType"`
	MemberID         string            `json:"memberID"`
	NetworkID        string            `json:"networkID"`
	Status           string            `json:"status"`
}

// NodeSummary is the short form returned by ListNodes.
type NodeSummary struct {
	CreationDate *time.Time `json:"creationDate"`
	Arn          string     `json:"arn"`
	ID           string     `json:"id"`
	InstanceType string     `json:"instanceType"`
	Status       string     `json:"status"`
}

// -- Request / Response bodies ------------------------------------------------

// createNetworkRequest is the request body for POST /networks.
type createNetworkRequest struct {
	Tags                map[string]string   `json:"Tags"`
	ClientRequestToken  string              `json:"ClientRequestToken"`
	Description         string              `json:"Description"`
	Framework           string              `json:"Framework"`
	FrameworkVersion    string              `json:"FrameworkVersion"`
	MemberConfiguration memberConfiguration `json:"MemberConfiguration"`
	Name                string              `json:"Name"`
}

// memberConfiguration holds the configuration for the first (or new) member.
type memberConfiguration struct {
	Description string `json:"Description"`
	Name        string `json:"Name"`
}

// createNetworkResponse is the response body for POST /networks.
type createNetworkResponse struct {
	MemberID  string `json:"MemberId"`
	NetworkID string `json:"NetworkId"`
}

// networkObject is the JSON representation of a network for GetNetwork.
type networkObject struct {
	CreationDate     *time.Time        `json:"CreationDate,omitempty"`
	Tags             map[string]string `json:"Tags,omitempty"`
	Arn              string            `json:"Arn"`
	Description      string            `json:"Description,omitempty"`
	Framework        string            `json:"Framework"`
	FrameworkVersion string            `json:"FrameworkVersion"`
	ID               string            `json:"Id"`
	Name             string            `json:"Name"`
	Status           string            `json:"Status"`
}

// getNetworkResponse is the response body for GET /networks/{networkId}.
type getNetworkResponse struct {
	Network networkObject `json:"Network"`
}

// networkSummaryObject is the JSON representation of a network summary.
type networkSummaryObject struct {
	CreationDate     *time.Time `json:"CreationDate,omitempty"`
	Arn              string     `json:"Arn"`
	Description      string     `json:"Description,omitempty"`
	Framework        string     `json:"Framework"`
	FrameworkVersion string     `json:"FrameworkVersion"`
	ID               string     `json:"Id"`
	Name             string     `json:"Name"`
	Status           string     `json:"Status"`
}

// listNetworksResponse is the response body for GET /networks.
type listNetworksResponse struct {
	NextToken *string                `json:"NextToken,omitempty"`
	Networks  []networkSummaryObject `json:"Networks"`
}

// createMemberRequest is the request body for POST /networks/{networkId}/members.
type createMemberRequest struct {
	Tags                map[string]string   `json:"Tags"`
	ClientRequestToken  string              `json:"ClientRequestToken"`
	InvitationID        string              `json:"InvitationId"`
	MemberConfiguration memberConfiguration `json:"MemberConfiguration"`
}

// createNodeRequest is the request body for POST /networks/{networkId}/members/{memberId}/nodes.
type createNodeRequest struct {
	Tags               map[string]string `json:"Tags"`
	ClientRequestToken string            `json:"ClientRequestToken"`
	NodeConfiguration  nodeConfiguration `json:"NodeConfiguration"`
}

// createMemberResponse is the response body for POST /networks/{networkId}/members.
type createMemberResponse struct {
	MemberID string `json:"MemberId"`
}

// memberObject is the JSON representation of a member for GetMember.
type memberObject struct {
	CreationDate *time.Time        `json:"CreationDate,omitempty"`
	Tags         map[string]string `json:"Tags,omitempty"`
	Arn          string            `json:"Arn"`
	Description  string            `json:"Description,omitempty"`
	ID           string            `json:"Id"`
	Name         string            `json:"Name"`
	NetworkID    string            `json:"NetworkId"`
	Status       string            `json:"Status"`
}

// getMemberResponse is the response body for GET /networks/{networkId}/members/{memberId}.
type getMemberResponse struct {
	Member memberObject `json:"Member"`
}

// memberSummaryObject is the JSON representation of a member summary.
type memberSummaryObject struct {
	CreationDate *time.Time `json:"CreationDate,omitempty"`
	Arn          string     `json:"Arn"`
	Description  string     `json:"Description,omitempty"`
	ID           string     `json:"Id"`
	Name         string     `json:"Name"`
	Status       string     `json:"Status"`
}

// listMembersResponse is the response body for GET /networks/{networkId}/members.
type listMembersResponse struct {
	NextToken *string               `json:"NextToken,omitempty"`
	Members   []memberSummaryObject `json:"Members"`
}

// listTagsResponse is the response body for GET /tags/{resourceArn}.
type listTagsResponse struct {
	Tags map[string]string `json:"Tags"`
}

// tagResourceRequest is the request body for POST /tags/{resourceArn}.
type tagResourceRequest struct {
	Tags map[string]string `json:"Tags"`
}

// errorResponse is the standard error response body.
type errorResponse struct {
	Message string `json:"message"`
}

// nodeConfiguration holds the configuration for a node.
type nodeConfiguration struct {
	AvailabilityZone string `json:"AvailabilityZone"`
	InstanceType     string `json:"InstanceType"`
}

// createNodeResponse is the response body for POST /networks/{networkId}/members/{memberId}/nodes.
type createNodeResponse struct {
	NodeID string `json:"NodeId"`
}

// nodeObject is the JSON representation of a node for GetNode.
type nodeObject struct {
	CreationDate     *time.Time        `json:"CreationDate,omitempty"`
	Tags             map[string]string `json:"Tags,omitempty"`
	Arn              string            `json:"Arn"`
	AvailabilityZone string            `json:"AvailabilityZone,omitempty"`
	ID               string            `json:"Id"`
	InstanceType     string            `json:"InstanceType"`
	MemberID         string            `json:"MemberId"`
	NetworkID        string            `json:"NetworkId"`
	Status           string            `json:"Status"`
}

// getNodeResponse is the response body for GET /networks/{networkId}/members/{memberId}/nodes/{nodeId}.
type getNodeResponse struct {
	Node nodeObject `json:"Node"`
}

// nodeSummaryObject is the JSON representation of a node summary.
type nodeSummaryObject struct {
	CreationDate *time.Time `json:"CreationDate,omitempty"`
	Arn          string     `json:"Arn"`
	ID           string     `json:"Id"`
	InstanceType string     `json:"InstanceType"`
	Status       string     `json:"Status"`
}

// listNodesResponse is the response body for GET /networks/{networkId}/members/{memberId}/nodes.
type listNodesResponse struct {
	NextToken *string             `json:"NextToken,omitempty"`
	Nodes     []nodeSummaryObject `json:"Nodes"`
}

// Accessor represents an Amazon Managed Blockchain accessor for token-based access.
type Accessor struct {
	CreationDate *time.Time        `json:"creationDate"`
	Tags         map[string]string `json:"tags"`
	Arn          string            `json:"arn"`
	BillingToken string            `json:"billingToken"`
	ID           string            `json:"id"`
	NetworkType  string            `json:"networkType"`
	Status       string            `json:"status"`
	Type         string            `json:"type"`
}

// Proposal represents a governance proposal on a Managed Blockchain network.
type Proposal struct {
	CreationDate         *time.Time        `json:"creationDate"`
	ExpirationDate       *time.Time        `json:"expirationDate,omitempty"`
	Tags                 map[string]string `json:"tags"`
	Arn                  string            `json:"arn"`
	Description          string            `json:"description"`
	NetworkID            string            `json:"networkId"`
	ProposalID           string            `json:"proposalId"`
	ProposedByMemberID   string            `json:"proposedByMemberId"`
	ProposedByMemberName string            `json:"proposedByMemberName"`
	Status               string            `json:"status"`
	NoVoteCount          int32             `json:"noVoteCount"`
	OutstandingVoteCount int32             `json:"outstandingVoteCount"`
	YesVoteCount         int32             `json:"yesVoteCount"`
}

// Invitation represents an invitation to join a Managed Blockchain network.
type Invitation struct {
	CreationDate   *time.Time `json:"creationDate"`
	ExpirationDate *time.Time `json:"expirationDate,omitempty"`
	Arn            string     `json:"arn"`
	InvitationID   string     `json:"invitationId"`
	NetworkID      string     `json:"networkId"`
	NetworkName    string     `json:"networkName"`
	Status         string     `json:"status"`
}

// ProposalVote represents a single vote cast on a proposal.
type ProposalVote struct {
	MemberID   string `json:"memberId"`
	MemberName string `json:"memberName"`
	Vote       string `json:"vote"`
}

// -- Accessor request / response types -----------------------------------------

// createAccessorRequest is the request body for POST /accessors.
type createAccessorRequest struct {
	Tags               map[string]string `json:"Tags"`
	AccessorType       string            `json:"AccessorType"`
	ClientRequestToken string            `json:"ClientRequestToken"`
	NetworkType        string            `json:"NetworkType"`
}

// createAccessorResponse is the response body for POST /accessors.
type createAccessorResponse struct {
	AccessorID   string `json:"AccessorId"`
	BillingToken string `json:"BillingToken"`
	NetworkType  string `json:"NetworkType"`
}

// accessorObject is the JSON representation of an accessor for GetAccessor.
type accessorObject struct {
	CreationDate *time.Time        `json:"CreationDate,omitempty"`
	Tags         map[string]string `json:"Tags,omitempty"`
	Arn          string            `json:"Arn"`
	BillingToken string            `json:"BillingToken"`
	ID           string            `json:"Id"`
	NetworkType  string            `json:"NetworkType,omitempty"`
	Status       string            `json:"Status"`
	Type         string            `json:"Type"`
}

// getAccessorResponse is the response body for GET /accessors/{accessorId}.
type getAccessorResponse struct {
	Accessor accessorObject `json:"Accessor"`
}

// accessorSummaryObject is the JSON representation of an accessor summary.
type accessorSummaryObject struct {
	CreationDate *time.Time `json:"CreationDate,omitempty"`
	Arn          string     `json:"Arn"`
	ID           string     `json:"Id"`
	NetworkType  string     `json:"NetworkType,omitempty"`
	Status       string     `json:"Status"`
	Type         string     `json:"Type"`
}

// listAccessorsResponse is the response body for GET /accessors.
type listAccessorsResponse struct {
	NextToken *string                 `json:"NextToken,omitempty"`
	Accessors []accessorSummaryObject `json:"Accessors"`
}

// -- Proposal request / response types ----------------------------------------

// createProposalRequest is the request body for POST /networks/{networkId}/proposals.
type createProposalRequest struct {
	Tags               map[string]string `json:"Tags"`
	ClientRequestToken string            `json:"ClientRequestToken"`
	Description        string            `json:"Description"`
	MemberID           string            `json:"MemberId"`
}

// createProposalResponse is the response body for POST /networks/{networkId}/proposals.
type createProposalResponse struct {
	ProposalID string `json:"ProposalId"`
}

// proposalObject is the JSON representation of a proposal for GetProposal.
type proposalObject struct {
	CreationDate         *time.Time        `json:"CreationDate,omitempty"`
	ExpirationDate       *time.Time        `json:"ExpirationDate,omitempty"`
	Tags                 map[string]string `json:"Tags,omitempty"`
	Arn                  string            `json:"Arn"`
	Description          string            `json:"Description,omitempty"`
	NetworkID            string            `json:"NetworkId"`
	ProposalID           string            `json:"ProposalId"`
	ProposedByMemberID   string            `json:"ProposedByMemberId"`
	ProposedByMemberName string            `json:"ProposedByMemberName,omitempty"`
	Status               string            `json:"Status"`
	NoVoteCount          int32             `json:"NoVoteCount"`
	OutstandingVoteCount int32             `json:"OutstandingVoteCount"`
	YesVoteCount         int32             `json:"YesVoteCount"`
}

// getProposalResponse is the response body for GET /networks/{networkId}/proposals/{proposalId}.
type getProposalResponse struct {
	Proposal proposalObject `json:"Proposal"`
}

// proposalSummaryObject is the JSON representation of a proposal summary.
type proposalSummaryObject struct {
	CreationDate         *time.Time `json:"CreationDate,omitempty"`
	ExpirationDate       *time.Time `json:"ExpirationDate,omitempty"`
	Arn                  string     `json:"Arn"`
	Description          string     `json:"Description,omitempty"`
	ProposalID           string     `json:"ProposalId"`
	ProposedByMemberID   string     `json:"ProposedByMemberId"`
	ProposedByMemberName string     `json:"ProposedByMemberName,omitempty"`
	Status               string     `json:"Status"`
}

// listProposalsResponse is the response body for GET /networks/{networkId}/proposals.
type listProposalsResponse struct {
	NextToken *string                 `json:"NextToken,omitempty"`
	Proposals []proposalSummaryObject `json:"Proposals"`
}

// voteSummaryObject is the JSON representation of a vote summary.
type voteSummaryObject struct {
	MemberID   string `json:"MemberId"`
	MemberName string `json:"MemberName,omitempty"`
	Vote       string `json:"Vote"`
}

// listProposalVotesResponse is the response body for GET .../votes.
type listProposalVotesResponse struct {
	NextToken     *string             `json:"NextToken,omitempty"`
	ProposalVotes []voteSummaryObject `json:"ProposalVotes"`
}

// -- Invitation request / response types --------------------------------------

// invitationObject is the JSON representation of an invitation.
type invitationObject struct {
	CreationDate   *time.Time `json:"CreationDate,omitempty"`
	ExpirationDate *time.Time `json:"ExpirationDate,omitempty"`
	Arn            string     `json:"Arn"`
	InvitationID   string     `json:"InvitationId"`
	NetworkID      string     `json:"NetworkId,omitempty"`
	NetworkName    string     `json:"NetworkName,omitempty"`
	Status         string     `json:"Status"`
}

// listInvitationsResponse is the response body for GET /invitations.
type listInvitationsResponse struct {
	NextToken   *string            `json:"NextToken,omitempty"`
	Invitations []invitationObject `json:"Invitations"`
}

// updateMemberRequest is the request body for PATCH /networks/{networkId}/members/{memberId}.
type updateMemberRequest struct {
	LogPublishingConfiguration *memberLogPublishingConfig `json:"LogPublishingConfiguration,omitempty"`
}

// memberLogPublishingConfig holds optional log publishing settings for a member.
type memberLogPublishingConfig struct{}

// voteOnProposalRequest is the request body for POST /networks/{networkId}/proposals/{proposalId}/votes.
type voteOnProposalRequest struct {
	VoterMemberID string `json:"VoterMemberId"`
	Vote          string `json:"Vote"`
}
