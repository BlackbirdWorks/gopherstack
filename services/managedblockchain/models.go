package managedblockchain

import "time"

// Network represents an Amazon Managed Blockchain network.
type Network struct {
	CreationDate     *time.Time
	Tags             map[string]string
	Arn              string
	Description      string
	Framework        string
	FrameworkVersion string
	ID               string
	Name             string
	Status           string
}

// NetworkSummary is the short form returned by ListNetworks.
type NetworkSummary struct {
	CreationDate     *time.Time
	Arn              string
	Description      string
	Framework        string
	FrameworkVersion string
	ID               string
	Name             string
	Status           string
}

// Member represents a member within a Managed Blockchain network.
type Member struct {
	CreationDate *time.Time
	Tags         map[string]string
	Arn          string
	Description  string
	ID           string
	Name         string
	NetworkID    string
	Status       string
}

// MemberSummary is the short form returned by ListMembers.
type MemberSummary struct {
	CreationDate *time.Time
	Arn          string
	Description  string
	ID           string
	Name         string
	Status       string
}

// -- Request / Response bodies ------------------------------------------------

// createNetworkRequest is the request body for POST /networks.
type createNetworkRequest struct {
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
	MemberId  string `json:"MemberId"`
	NetworkId string `json:"NetworkId"`
}

// networkObject is the JSON representation of a network for GetNetwork.
type networkObject struct {
	CreationDate     *time.Time        `json:"CreationDate,omitempty"`
	Tags             map[string]string `json:"Tags,omitempty"`
	Arn              string            `json:"Arn"`
	Description      string            `json:"Description,omitempty"`
	Framework        string            `json:"Framework"`
	FrameworkVersion string            `json:"FrameworkVersion"`
	Id               string            `json:"Id"`
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
	Id               string     `json:"Id"`
	Name             string     `json:"Name"`
	Status           string     `json:"Status"`
}

// listNetworksResponse is the response body for GET /networks.
type listNetworksResponse struct {
	Networks  []networkSummaryObject `json:"Networks"`
	NextToken *string                `json:"NextToken,omitempty"`
}

// createMemberRequest is the request body for POST /networks/{networkId}/members.
type createMemberRequest struct {
	ClientRequestToken  string              `json:"ClientRequestToken"`
	InvitationId        string              `json:"InvitationId"`
	MemberConfiguration memberConfiguration `json:"MemberConfiguration"`
}

// createMemberResponse is the response body for POST /networks/{networkId}/members.
type createMemberResponse struct {
	MemberId string `json:"MemberId"`
}

// memberObject is the JSON representation of a member for GetMember.
type memberObject struct {
	CreationDate *time.Time        `json:"CreationDate,omitempty"`
	Tags         map[string]string `json:"Tags,omitempty"`
	Arn          string            `json:"Arn"`
	Description  string            `json:"Description,omitempty"`
	Id           string            `json:"Id"`
	Name         string            `json:"Name"`
	NetworkId    string            `json:"NetworkId"`
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
	Id           string     `json:"Id"`
	Name         string     `json:"Name"`
	Status       string     `json:"Status"`
}

// listMembersResponse is the response body for GET /networks/{networkId}/members.
type listMembersResponse struct {
	Members   []memberSummaryObject `json:"Members"`
	NextToken *string               `json:"NextToken,omitempty"`
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
