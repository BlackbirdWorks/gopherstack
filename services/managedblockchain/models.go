package managedblockchain

import "time"

// Network represents an Amazon Managed Blockchain network.
type Network struct {
	CreationDate     *time.Time        `json:"creationDate"`
	Tags             map[string]string `json:"tags"`
	VotingPolicy     *VotingPolicy     `json:"votingPolicy,omitempty"`
	Arn              string            `json:"arn"`
	Description      string            `json:"description"`
	Framework        string            `json:"framework"`
	FrameworkVersion string            `json:"frameworkVersion"`
	ID               string            `json:"id"`
	Name             string            `json:"name"`
	Status           string            `json:"status"`
}

// VotingPolicy defines how a network votes on proposals.
type VotingPolicy struct {
	ApprovalThresholdPolicy *ApprovalThresholdPolicy `json:"approvalThresholdPolicy,omitempty"`
}

// ApprovalThresholdPolicy defines the threshold for proposal approval.
type ApprovalThresholdPolicy struct {
	ThresholdComparator     string `json:"thresholdComparator,omitempty"`
	ProposalDurationInHours int32  `json:"proposalDurationInHours,omitempty"`
	ThresholdPercentage     int32  `json:"thresholdPercentage,omitempty"`
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
	CreationDate               *time.Time                      `json:"creationDate"`
	Tags                       map[string]string               `json:"tags"`
	LogPublishingConfiguration *MemberLogPublishingConfigState `json:"logPublishingConfiguration,omitempty"`
	Arn                        string                          `json:"arn"`
	Description                string                          `json:"description"`
	ID                         string                          `json:"id"`
	Name                       string                          `json:"name"`
	NetworkID                  string                          `json:"networkID"`
	Status                     string                          `json:"status"`
	IsOwned                    bool                            `json:"isOwned"`
}

// MemberLogPublishingConfigState stores log publishing configuration for a member.
type MemberLogPublishingConfigState struct {
	Fabric *MemberFabricLogState `json:"fabric,omitempty"`
}

// MemberFabricLogState stores Fabric-specific log config for a member.
type MemberFabricLogState struct {
	CALogs *LogConfigState `json:"caLogs,omitempty"`
}

// LogConfigState stores whether logging is enabled.
type LogConfigState struct {
	CloudWatch *CloudWatchLogState `json:"cloudWatch,omitempty"`
}

// CloudWatchLogState stores CloudWatch logging state.
type CloudWatchLogState struct {
	Enabled bool `json:"enabled"`
}

// MemberSummary is the short form returned by ListMembers.
type MemberSummary struct {
	CreationDate *time.Time `json:"creationDate"`
	Arn          string     `json:"arn"`
	Description  string     `json:"description"`
	ID           string     `json:"id"`
	Name         string     `json:"name"`
	Status       string     `json:"status"`
	IsOwned      bool       `json:"isOwned"`
}

// Node represents a peer node within a Managed Blockchain member.
type Node struct {
	CreationDate               *time.Time                    `json:"creationDate"`
	Tags                       map[string]string             `json:"tags"`
	LogPublishingConfiguration *NodeLogPublishingConfigState `json:"logPublishingConfiguration,omitempty"`
	Arn                        string                        `json:"arn"`
	AvailabilityZone           string                        `json:"availabilityZone"`
	ID                         string                        `json:"id"`
	InstanceType               string                        `json:"instanceType"`
	MemberID                   string                        `json:"memberID"`
	NetworkID                  string                        `json:"networkID"`
	Status                     string                        `json:"status"`
}

// NodeLogPublishingConfigState stores log publishing configuration for a node.
type NodeLogPublishingConfigState struct {
	Fabric *NodeFabricLogState `json:"fabric,omitempty"`
}

// NodeFabricLogState stores Fabric-specific log config for a node.
type NodeFabricLogState struct {
	ChaincodeLogs *LogConfigState `json:"chaincodeLogs,omitempty"`
	PeerLogs      *LogConfigState `json:"peerLogs,omitempty"`
}

// NodeSummary is the short form returned by ListNodes.
type NodeSummary struct {
	CreationDate     *time.Time `json:"creationDate"`
	Arn              string     `json:"arn"`
	AvailabilityZone string     `json:"availabilityZone,omitempty"`
	ID               string     `json:"id"`
	InstanceType     string     `json:"instanceType"`
	Status           string     `json:"status"`
}

// -- Request / Response bodies ------------------------------------------------

// createNetworkRequest is the request body for POST /networks.
type createNetworkRequest struct {
	Tags                map[string]string    `json:"Tags"`
	VotingPolicy        *votingPolicyRequest `json:"VotingPolicy"`
	ClientRequestToken  string               `json:"ClientRequestToken"`
	Description         string               `json:"Description"`
	Framework           string               `json:"Framework"`
	FrameworkVersion    string               `json:"FrameworkVersion"`
	MemberConfiguration memberConfiguration  `json:"MemberConfiguration"`
	Name                string               `json:"Name"`
}

// votingPolicyRequest is the request body for VotingPolicy.
type votingPolicyRequest struct {
	ApprovalThresholdPolicy *approvalThresholdPolicyRequest `json:"ApprovalThresholdPolicy"`
}

// approvalThresholdPolicyRequest is the request body for ApprovalThresholdPolicy.
type approvalThresholdPolicyRequest struct {
	ThresholdComparator     string `json:"ThresholdComparator"`
	ProposalDurationInHours int32  `json:"ProposalDurationInHours"`
	ThresholdPercentage     int32  `json:"ThresholdPercentage"`
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
	CreationDate     *time.Time          `json:"CreationDate,omitempty"`
	Tags             map[string]string   `json:"Tags,omitempty"`
	VotingPolicy     *votingPolicyObject `json:"VotingPolicy,omitempty"`
	Arn              string              `json:"Arn"`
	Description      string              `json:"Description,omitempty"`
	Framework        string              `json:"Framework"`
	FrameworkVersion string              `json:"FrameworkVersion"`
	ID               string              `json:"Id"`
	Name             string              `json:"Name"`
	Status           string              `json:"Status"`
}

// votingPolicyObject is the JSON representation of a VotingPolicy in responses.
type votingPolicyObject struct {
	ApprovalThresholdPolicy *approvalThresholdPolicyObject `json:"ApprovalThresholdPolicy,omitempty"`
}

// approvalThresholdPolicyObject is the JSON representation of ApprovalThresholdPolicy.
type approvalThresholdPolicyObject struct {
	ThresholdComparator     string `json:"ThresholdComparator,omitempty"`
	ProposalDurationInHours int32  `json:"ProposalDurationInHours,omitempty"`
	ThresholdPercentage     int32  `json:"ThresholdPercentage,omitempty"`
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
	CreationDate               *time.Time                        `json:"CreationDate,omitempty"`
	Tags                       map[string]string                 `json:"Tags,omitempty"`
	LogPublishingConfiguration *memberLogPublishingConfigRespObj `json:"LogPublishingConfiguration,omitempty"`
	Arn                        string                            `json:"Arn"`
	Description                string                            `json:"Description,omitempty"`
	ID                         string                            `json:"Id"`
	Name                       string                            `json:"Name"`
	NetworkID                  string                            `json:"NetworkId"`
	Status                     string                            `json:"Status"`
	IsOwned                    bool                              `json:"IsOwned"`
}

// memberLogPublishingConfigRespObj is the response JSON for member log publishing config.
type memberLogPublishingConfigRespObj struct {
	Fabric *memberFabricLogRespObj `json:"Fabric,omitempty"`
}

// memberFabricLogRespObj is the response JSON for Fabric-specific member log config.
type memberFabricLogRespObj struct {
	CaLogs *logConfigRespObj `json:"CaLogs,omitempty"`
}

// logConfigRespObj is the response JSON for a log configuration.
type logConfigRespObj struct {
	CloudWatch *cloudWatchLogRespObj `json:"CloudWatch,omitempty"`
}

// cloudWatchLogRespObj is the response JSON for CloudWatch logging config.
type cloudWatchLogRespObj struct {
	Enabled bool `json:"Enabled"`
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
	IsOwned      bool       `json:"IsOwned"`
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
	CreationDate               *time.Time                      `json:"CreationDate,omitempty"`
	Tags                       map[string]string               `json:"Tags,omitempty"`
	LogPublishingConfiguration *nodeLogPublishingConfigRespObj `json:"LogPublishingConfiguration,omitempty"`
	Arn                        string                          `json:"Arn"`
	AvailabilityZone           string                          `json:"AvailabilityZone,omitempty"`
	ID                         string                          `json:"Id"`
	InstanceType               string                          `json:"InstanceType"`
	MemberID                   string                          `json:"MemberId"`
	NetworkID                  string                          `json:"NetworkId"`
	Status                     string                          `json:"Status"`
}

// nodeLogPublishingConfigRespObj is the response JSON for node log publishing config.
type nodeLogPublishingConfigRespObj struct {
	Fabric *nodeFabricLogRespObj `json:"Fabric,omitempty"`
}

// nodeFabricLogRespObj is the response JSON for Fabric-specific node log config.
type nodeFabricLogRespObj struct {
	ChaincodeLogs *logConfigRespObj `json:"ChaincodeLogs,omitempty"`
	PeerLogs      *logConfigRespObj `json:"PeerLogs,omitempty"`
}

// getNodeResponse is the response body for GET /networks/{networkId}/members/{memberId}/nodes/{nodeId}.
type getNodeResponse struct {
	Node nodeObject `json:"Node"`
}

// nodeSummaryObject is the JSON representation of a node summary.
type nodeSummaryObject struct {
	CreationDate     *time.Time `json:"CreationDate,omitempty"`
	Arn              string     `json:"Arn"`
	AvailabilityZone string     `json:"AvailabilityZone,omitempty"`
	ID               string     `json:"Id"`
	InstanceType     string     `json:"InstanceType"`
	Status           string     `json:"Status"`
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
	Actions              *ProposalActions  `json:"actions,omitempty"`
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

// ProposalActions defines the actions taken when a proposal is approved.
type ProposalActions struct {
	Invitations []InviteAction `json:"invitations,omitempty"`
	Removals    []RemoveAction `json:"removals,omitempty"`
}

// InviteAction represents an invitation to an AWS account to join the network.
type InviteAction struct {
	Principal string `json:"principal"`
}

// RemoveAction represents a removal of a member from the network.
type RemoveAction struct {
	MemberID string `json:"memberId"`
}

// Invitation represents an invitation to join a Managed Blockchain network.
type Invitation struct {
	NetworkSummary *InvitationNetworkSummary `json:"networkSummary,omitempty"`
	CreationDate   *time.Time                `json:"creationDate"`
	ExpirationDate *time.Time                `json:"expirationDate,omitempty"`
	Arn            string                    `json:"arn"`
	InvitationID   string                    `json:"invitationId"`
	NetworkID      string                    `json:"networkId"`
	NetworkName    string                    `json:"networkName"`
	Status         string                    `json:"status"`
}

// InvitationNetworkSummary is a summary of the network included in an Invitation.
type InvitationNetworkSummary struct {
	CreationDate     *time.Time `json:"creationDate,omitempty"`
	Arn              string     `json:"arn,omitempty"`
	Description      string     `json:"description,omitempty"`
	Framework        string     `json:"framework,omitempty"`
	FrameworkVersion string     `json:"frameworkVersion,omitempty"`
	ID               string     `json:"id,omitempty"`
	Name             string     `json:"name,omitempty"`
	Status           string     `json:"status,omitempty"`
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
	Actions            *proposalActionsRequest `json:"Actions"`
	Tags               map[string]string       `json:"Tags"`
	ClientRequestToken string                  `json:"ClientRequestToken"`
	Description        string                  `json:"Description"`
	MemberID           string                  `json:"MemberId"`
}

// proposalActionsRequest is the request body for proposal actions.
type proposalActionsRequest struct {
	Invitations []inviteActionRequest `json:"Invitations"`
	Removals    []removeActionRequest `json:"Removals"`
}

// inviteActionRequest is an invitation action in a proposal.
type inviteActionRequest struct {
	Principal string `json:"Principal"`
}

// removeActionRequest is a removal action in a proposal.
type removeActionRequest struct {
	MemberID string `json:"MemberId"`
}

// createProposalResponse is the response body for POST /networks/{networkId}/proposals.
type createProposalResponse struct {
	ProposalID string `json:"ProposalId"`
}

// proposalObject is the JSON representation of a proposal for GetProposal.
type proposalObject struct {
	Actions              *proposalActionsObject `json:"Actions,omitempty"`
	CreationDate         *time.Time             `json:"CreationDate,omitempty"`
	ExpirationDate       *time.Time             `json:"ExpirationDate,omitempty"`
	Tags                 map[string]string      `json:"Tags,omitempty"`
	Arn                  string                 `json:"Arn"`
	Description          string                 `json:"Description,omitempty"`
	NetworkID            string                 `json:"NetworkId"`
	ProposalID           string                 `json:"ProposalId"`
	ProposedByMemberID   string                 `json:"ProposedByMemberId"`
	ProposedByMemberName string                 `json:"ProposedByMemberName,omitempty"`
	Status               string                 `json:"Status"`
	NoVoteCount          int32                  `json:"NoVoteCount"`
	OutstandingVoteCount int32                  `json:"OutstandingVoteCount"`
	YesVoteCount         int32                  `json:"YesVoteCount"`
}

// proposalActionsObject is the response JSON for proposal actions.
type proposalActionsObject struct {
	Invitations []inviteActionObject `json:"Invitations,omitempty"`
	Removals    []removeActionObject `json:"Removals,omitempty"`
}

// inviteActionObject is the response JSON for an invitation action.
type inviteActionObject struct {
	Principal string `json:"Principal"`
}

// removeActionObject is the response JSON for a removal action.
type removeActionObject struct {
	MemberID string `json:"MemberId"`
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
	NetworkID            string     `json:"NetworkId,omitempty"`
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
	NetworkSummary *invitationNetworkSummaryObject `json:"NetworkSummary,omitempty"`
	CreationDate   *time.Time                      `json:"CreationDate,omitempty"`
	ExpirationDate *time.Time                      `json:"ExpirationDate,omitempty"`
	Arn            string                          `json:"Arn"`
	InvitationID   string                          `json:"InvitationId"`
	NetworkID      string                          `json:"NetworkId,omitempty"`
	NetworkName    string                          `json:"NetworkName,omitempty"`
	Status         string                          `json:"Status"`
}

// invitationNetworkSummaryObject is the nested network summary in an invitation response.
type invitationNetworkSummaryObject struct {
	CreationDate     *time.Time `json:"CreationDate,omitempty"`
	Arn              string     `json:"Arn,omitempty"`
	Description      string     `json:"Description,omitempty"`
	Framework        string     `json:"Framework,omitempty"`
	FrameworkVersion string     `json:"FrameworkVersion,omitempty"`
	ID               string     `json:"Id,omitempty"`
	Name             string     `json:"Name,omitempty"`
	Status           string     `json:"Status,omitempty"`
}

// listInvitationsResponse is the response body for GET /invitations.
type listInvitationsResponse struct {
	NextToken   *string            `json:"NextToken,omitempty"`
	Invitations []invitationObject `json:"Invitations"`
}

// updateMemberRequest is the request body for PATCH /networks/{networkId}/members/{memberId}.
type updateMemberRequest struct {
	LogPublishingConfiguration *memberLogPublishingConfigReq `json:"LogPublishingConfiguration,omitempty"`
}

// memberLogPublishingConfigReq holds optional log publishing settings for a member.
type memberLogPublishingConfigReq struct {
	Fabric *memberFabricLogReq `json:"Fabric,omitempty"`
}

// memberFabricLogReq holds Fabric-specific log publishing settings for a member.
type memberFabricLogReq struct {
	CaLogs *logConfigReq `json:"CaLogs,omitempty"`
}

// logConfigReq holds log config for a single log stream.
type logConfigReq struct {
	CloudWatch *cloudWatchLogReq `json:"CloudWatch,omitempty"`
}

// cloudWatchLogReq holds CloudWatch log config.
type cloudWatchLogReq struct {
	Enabled bool `json:"Enabled"`
}

// updateNodeRequest is the request body for PATCH /networks/{networkId}/members/{memberId}/nodes/{nodeId}.
type updateNodeRequest struct {
	LogPublishingConfiguration *nodeLogPublishingConfigReq `json:"LogPublishingConfiguration,omitempty"`
}

// nodeLogPublishingConfigReq holds optional log publishing settings for a node.
type nodeLogPublishingConfigReq struct {
	Fabric *nodeFabricLogReq `json:"Fabric,omitempty"`
}

// nodeFabricLogReq holds Fabric-specific log publishing settings for a node.
type nodeFabricLogReq struct {
	ChaincodeLogs *logConfigReq `json:"ChaincodeLogs,omitempty"`
	PeerLogs      *logConfigReq `json:"PeerLogs,omitempty"`
}

// voteOnProposalRequest is the request body for POST /networks/{networkId}/proposals/{proposalId}/votes.
type voteOnProposalRequest struct {
	VoterMemberID string `json:"VoterMemberId"`
	Vote          string `json:"Vote"`
}
