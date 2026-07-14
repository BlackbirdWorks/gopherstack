package ec2

import (
	"encoding/xml"
	"net/url"
	"strconv"
)

func (h *Handler) handleCreateNetworkACL(vals url.Values, reqID string) (any, error) {
	acl, err := h.Backend.CreateNetworkACL(vals.Get("VpcId"))
	if err != nil {
		return nil, err
	}

	return &createNetworkACLResponse{
		Xmlns:      ec2XMLNS,
		RequestID:  reqID,
		NetworkACL: networkACLDetailItem{ID: acl.ID, VPCID: acl.VPCID, IsDefault: acl.IsDefault},
	}, nil
}

func (h *Handler) handleDeleteNetworkACL(vals url.Values, _ string) (any, error) {
	return nil, h.Backend.DeleteNetworkACL(vals.Get("NetworkAclId"))
}

func (h *Handler) handleCreateNetworkACLEntry(vals url.Values, reqID string) (any, error) {
	ruleNumber, _ := strconv.Atoi(vals.Get("RuleNumber"))
	egress := vals.Get("Egress") == ec2BooleanTrue
	fromPort, _ := strconv.Atoi(vals.Get("PortRange.From"))
	toPort, _ := strconv.Atoi(vals.Get("PortRange.To"))

	if err := h.Backend.CreateNetworkACLEntry(
		vals.Get("NetworkAclId"),
		ruleNumber,
		vals.Get("Protocol"),
		vals.Get("RuleAction"),
		vals.Get("CidrBlock"),
		egress,
		fromPort,
		toPort,
	); err != nil {
		return nil, err
	}

	return &genericReturnResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    ec2BooleanTrue,
	}, nil
}

func (h *Handler) handleDeleteNetworkACLEntry(vals url.Values, _ string) (any, error) {
	ruleNumber, _ := strconv.Atoi(vals.Get("RuleNumber"))
	egress := vals.Get("Egress") == ec2BooleanTrue

	return nil, h.Backend.DeleteNetworkACLEntry(vals.Get("NetworkAclId"), ruleNumber, egress)
}

// ---- Security group rule handlers ----

type createNetworkACLResponse struct {
	XMLName    xml.Name             `xml:"CreateNetworkAclResponse"`
	Xmlns      string               `xml:"xmlns,attr"`
	RequestID  string               `xml:"requestId"`
	NetworkACL networkACLDetailItem `xml:"networkAcl"`
}

type networkACLDetailItem struct {
	ID        string `xml:"networkAclId"`
	VPCID     string `xml:"vpcId"`
	IsDefault bool   `xml:"default"`
}

type sgRuleDetailItem struct {
	SecurityGroupRuleID string `xml:"securityGroupRuleId"`
	GroupID             string `xml:"groupId"`
	Protocol            string `xml:"ipProtocol"`
	CIDRIPv4            string `xml:"cidrIpv4,omitempty"`
	Description         string `xml:"description,omitempty"`
	FromPort            int    `xml:"fromPort"`
	ToPort              int    `xml:"toPort"`
	IsEgress            bool   `xml:"isEgress"`
}

type sgRuleDetailSet struct {
	Items []sgRuleDetailItem `xml:"item"`
}

type replaceNetworkACLEntryResponse struct {
	XMLName   xml.Name `xml:"ReplaceNetworkAclEntryResponse"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type replaceNetworkACLAssociationResponse struct {
	XMLName          xml.Name `xml:"ReplaceNetworkAclAssociationResponse"`
	RequestID        string   `xml:"requestId"`
	NewAssociationID string   `xml:"newAssociationId"`
}

func (h *Handler) handleReplaceNetworkACLEntry(vals url.Values, reqID string) (any, error) {
	aclID := vals.Get("NetworkAclId")
	ruleNumber, _ := strconv.Atoi(vals.Get("RuleNumber"))
	protocol := vals.Get("Protocol")
	action := vals.Get("RuleAction")
	cidr := vals.Get("CidrBlock")
	if cidr == "" {
		cidr = vals.Get("Ipv6CidrBlock")
	}
	egress := vals.Get("Egress") == ec2BooleanTrue
	fromPort, _ := strconv.Atoi(vals.Get("PortRange.From"))
	toPort, _ := strconv.Atoi(vals.Get("PortRange.To"))

	if err := h.Backend.ReplaceNetworkACLEntry(
		aclID, ruleNumber, protocol, action, cidr, egress, fromPort, toPort,
	); err != nil {
		return nil, err
	}

	return &replaceNetworkACLEntryResponse{RequestID: reqID, Return: true}, nil
}

func (h *Handler) handleReplaceNetworkACLAssociation(vals url.Values, reqID string) (any, error) {
	aclID := vals.Get("NetworkAclId")
	subnetID := vals.Get(
		"AssociationId",
	) // AWS sends the old assocID; we use SubnetId as a simplification
	if subnetID == "" {
		subnetID = vals.Get("SubnetId")
	}

	newAssocID, err := h.Backend.ReplaceNetworkACLAssociation(aclID, subnetID)
	if err != nil {
		return nil, err
	}

	return &replaceNetworkACLAssociationResponse{
		RequestID:        reqID,
		NewAssociationID: newAssocID,
	}, nil
}

// registerNetworkAclsOps registers the NetworkAcls operation handlers.
func registerNetworkAclsOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["CreateNetworkAcl"] = h.handleCreateNetworkACL
	ops["DeleteNetworkAcl"] = h.handleDeleteNetworkACL
	ops["CreateNetworkAclEntry"] = h.handleCreateNetworkACLEntry
	ops["DeleteNetworkAclEntry"] = h.handleDeleteNetworkACLEntry
	ops["ReplaceNetworkAclEntry"] = h.handleReplaceNetworkACLEntry
	ops["ReplaceNetworkAclAssociation"] = h.handleReplaceNetworkACLAssociation
}

// networkAclsSupportedOperations lists the operation names registered by
// registerNetworkAclsOps, for GetSupportedOperations().
func networkAclsSupportedOperations() []string {
	return []string{
		"CreateNetworkAcl",
		"DeleteNetworkAcl",
		"CreateNetworkAclEntry",
		"DeleteNetworkAclEntry",
		"ReplaceNetworkAclEntry",
		"ReplaceNetworkAclAssociation",
	}
}
