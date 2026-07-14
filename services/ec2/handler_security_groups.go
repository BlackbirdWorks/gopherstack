package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
)

type associateSGVpcResponse struct {
	XMLName   xml.Name            `xml:"AssociateSecurityGroupVpcResponse"`
	RequestID string              `xml:"requestId"`
	State     sgVpcAssocStateItem `xml:"state"`
}

type sgReferenceItem struct {
	GroupID                string `xml:"groupId"`
	ReferencingVpcID       string `xml:"referencingVpcId"`
	VpcPeeringConnectionID string `xml:"vpcPeeringConnectionId,omitempty"`
}

type describeSecurityGroupReferencesResponse struct {
	XMLName                   xml.Name `xml:"DescribeSecurityGroupReferencesResponse"`
	RequestID                 string   `xml:"requestId"`
	SecurityGroupReferenceSet struct {
		Items []sgReferenceItem `xml:"item"`
	} `xml:"securityGroupReferenceSet"`
}

type staleSGItem struct {
	GroupID     string `xml:"groupId"`
	GroupName   string `xml:"groupName"`
	Description string `xml:"description"`
	VpcID       string `xml:"vpcId"`
}

type describeStaleSecurityGroupsResponse struct {
	XMLName               xml.Name `xml:"DescribeStaleSecurityGroupsResponse"`
	RequestID             string   `xml:"requestId"`
	StaleSecurityGroupSet struct {
		Items []staleSGItem `xml:"item"`
	} `xml:"staleSecurityGroupSet"`
}

type sgVpcAssocItem struct {
	GroupID string `xml:"groupId"`
	VpcID   string `xml:"vpcId"`
	State   string `xml:"state"`
}

type describeSecurityGroupVpcAssociationsResponse struct {
	XMLName                        xml.Name `xml:"DescribeSecurityGroupVpcAssociationsResponse"`
	RequestID                      string   `xml:"requestId"`
	SecurityGroupVpcAssociationSet struct {
		Items []sgVpcAssocItem `xml:"item"`
	} `xml:"securityGroupVpcAssociationSet"`
}

func (h *Handler) handleAssociateSecurityGroupVpc(vals url.Values, reqID string) (any, error) {
	sgID := vals.Get("GroupId")
	vpcID := vals.Get("VpcId")
	result, err := h.Backend.AssociateSecurityGroupVpc(sgID, vpcID)
	if err != nil {
		return nil, err
	}
	resp := &associateSGVpcResponse{RequestID: reqID}
	resp.State.State = result.State

	return resp, nil
}

func (h *Handler) handleDisassociateSecurityGroupVpc(vals url.Values, reqID string) (any, error) {
	sgID := vals.Get("GroupId")
	vpcID := vals.Get("VpcId")
	if err := h.Backend.DisassociateSecurityGroupVpc(sgID, vpcID); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "DisassociateSecurityGroupVpcResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeSecurityGroupReferences(
	vals url.Values,
	reqID string,
) (any, error) {
	sgIDs := parseMemberList(vals, "GroupId")
	refs := h.Backend.DescribeSecurityGroupReferences(sgIDs)

	resp := &describeSecurityGroupReferencesResponse{RequestID: reqID}
	for _, ref := range refs {
		resp.SecurityGroupReferenceSet.Items = append(
			resp.SecurityGroupReferenceSet.Items,
			sgReferenceItem{
				GroupID:                ref.GroupID,
				ReferencingVpcID:       ref.ReferencingVPCID,
				VpcPeeringConnectionID: ref.VpcPeeringConnectionID,
			},
		)
	}

	return resp, nil
}

func (h *Handler) handleDescribeStaleSecurityGroups(vals url.Values, reqID string) (any, error) {
	vpcID := vals.Get("VpcId")
	if vpcID == "" {
		return nil, fmt.Errorf("%w: VpcId is required", ErrInvalidParameter)
	}
	stale := h.Backend.DescribeStaleSecurityGroups(vpcID)

	resp := &describeStaleSecurityGroupsResponse{RequestID: reqID}
	for _, sg := range stale {
		resp.StaleSecurityGroupSet.Items = append(resp.StaleSecurityGroupSet.Items, staleSGItem{
			GroupID:     sg.GroupID,
			GroupName:   sg.GroupName,
			Description: sg.Description,
			VpcID:       sg.VPCID,
		})
	}

	return resp, nil
}

func (h *Handler) handleDescribeSecurityGroupVpcAssociations(
	vals url.Values,
	reqID string,
) (any, error) {
	sgIDs := parseMemberList(vals, "GroupId")
	assocs := h.Backend.DescribeSecurityGroupVpcAssociations(sgIDs)

	resp := &describeSecurityGroupVpcAssociationsResponse{RequestID: reqID}
	for _, a := range assocs {
		resp.SecurityGroupVpcAssociationSet.Items = append(
			resp.SecurityGroupVpcAssociationSet.Items,
			sgVpcAssocItem{
				GroupID: a.SGID,
				VpcID:   a.VPCID,
				State:   a.State,
			},
		)
	}

	return resp, nil
}

type getSecurityGroupsForVpcResponse struct {
	XMLName                xml.Name `xml:"GetSecurityGroupsForVpcResponse"`
	RequestID              string   `xml:"requestId"`
	SecurityGroupForVpcSet struct {
		Items []sgForVpcItem `xml:"item"`
	} `xml:"securityGroupForVpcSet"`
}

type recycleBinVolumeItem struct {
	VolumeID string `xml:"volumeId"`
}

func (h *Handler) handleGetSecurityGroupsForVpc(vals url.Values, reqID string) (any, error) {
	vpcID := vals.Get("VpcId")
	sgs, err := h.Backend.GetSecurityGroupsForVpc(vpcID)
	if err != nil {
		return nil, err
	}

	resp := &getSecurityGroupsForVpcResponse{RequestID: reqID}
	for _, sg := range sgs {
		resp.SecurityGroupForVpcSet.Items = append(resp.SecurityGroupForVpcSet.Items, sgForVpcItem{
			GroupID:     sg.GroupID,
			GroupName:   sg.GroupName,
			Description: sg.Description,
		})
	}

	return resp, nil
}

func (h *Handler) handleUpdateSGRuleDescriptionsIngress(
	vals url.Values,
	reqID string,
) (any, error) {
	groupID := vals.Get("GroupId")
	rules := parseIPPermissions(vals)

	if err := h.Backend.UpdateSecurityGroupRuleDescriptionsIngress(groupID, rules); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "UpdateSecurityGroupRuleDescriptionsIngressResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleUpdateSGRuleDescriptionsEgress(vals url.Values, reqID string) (any, error) {
	groupID := vals.Get("GroupId")
	rules := parseIPPermissions(vals)

	if err := h.Backend.UpdateSecurityGroupRuleDescriptionsEgress(groupID, rules); err != nil {
		return nil, err
	}

	return &stubResponse{
		XMLName:   xml.Name{Local: "UpdateSecurityGroupRuleDescriptionsEgressResponse"},
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleDescribeSecurityGroupRules(vals url.Values, reqID string) (any, error) {
	groupID := vals.Get("Filter.1.Value")
	if groupID == "" {
		groupID = vals.Get("GroupId")
	}

	rules, err := h.Backend.DescribeSecurityGroupRules(groupID)
	if err != nil {
		return nil, err
	}

	items := make([]sgRuleDetailItem, 0, len(rules))
	for _, r := range rules {
		items = append(items, sgRuleDetailItem{
			SecurityGroupRuleID: r.SecurityGroupRuleID,
			GroupID:             r.GroupID,
			Protocol:            r.Protocol,
			CIDRIPv4:            r.CIDRIPv4,
			Description:         r.Description,
			FromPort:            r.FromPort,
			ToPort:              r.ToPort,
			IsEgress:            r.IsEgress,
		})
	}

	return &describeSecurityGroupRulesResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Rules:     sgRuleDetailSet{Items: items},
	}, nil
}

func (h *Handler) handleModifySecurityGroupRules(vals url.Values, reqID string) (any, error) {
	groupID := vals.Get("GroupId")
	egress := vals.Get("Egress") == ec2BooleanTrue

	rules := parseIPPermissions(vals)

	if err := h.Backend.ModifySecurityGroupRules(groupID, rules, egress); err != nil {
		return nil, err
	}

	return &genericReturnResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    ec2BooleanTrue,
	}, nil
}

// ---- Launch template handlers ----

type describeSecurityGroupRulesResponse struct {
	XMLName   xml.Name        `xml:"DescribeSecurityGroupRulesResponse"`
	Xmlns     string          `xml:"xmlns,attr"`
	RequestID string          `xml:"requestId"`
	Rules     sgRuleDetailSet `xml:"securityGroupRuleSet"`
}

type launchTemplateVersionSet struct {
	Items []launchTemplateItem `xml:"item"`
}

// registerSecurityGroupsOps registers the SecurityGroups operation handlers.
func registerSecurityGroupsOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["AssociateSecurityGroupVpc"] = h.handleAssociateSecurityGroupVpc
	ops["DisassociateSecurityGroupVpc"] = h.handleDisassociateSecurityGroupVpc
	ops["DescribeSecurityGroupReferences"] = h.handleDescribeSecurityGroupReferences
	ops["DescribeStaleSecurityGroups"] = h.handleDescribeStaleSecurityGroups
	ops["DescribeSecurityGroupVpcAssociations"] = h.handleDescribeSecurityGroupVpcAssociations
	ops["GetSecurityGroupsForVpc"] = h.handleGetSecurityGroupsForVpc
	ops["UpdateSecurityGroupRuleDescriptionsIngress"] = h.handleUpdateSGRuleDescriptionsIngress
	ops["UpdateSecurityGroupRuleDescriptionsEgress"] = h.handleUpdateSGRuleDescriptionsEgress
	ops["DescribeSecurityGroupRules"] = h.handleDescribeSecurityGroupRules
	ops["ModifySecurityGroupRules"] = h.handleModifySecurityGroupRules
}

// securityGroupsSupportedOperations lists the operation names registered by
// registerSecurityGroupsOps, for GetSupportedOperations().
func securityGroupsSupportedOperations() []string {
	return []string{
		"AssociateSecurityGroupVpc",
		"DisassociateSecurityGroupVpc",
		"DescribeSecurityGroupReferences",
		"DescribeStaleSecurityGroups",
		"DescribeSecurityGroupVpcAssociations",
		"GetSecurityGroupsForVpc",
		"UpdateSecurityGroupRuleDescriptionsIngress",
		"UpdateSecurityGroupRuleDescriptionsEgress",
		"DescribeSecurityGroupRules",
		"ModifySecurityGroupRules",
	}
}
