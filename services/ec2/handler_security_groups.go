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
	Items []launchTemplateVersionItem `xml:"item"`
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

type authorizeSecurityGroupIngressResponse struct {
	XMLName   xml.Name `xml:"AuthorizeSecurityGroupIngressResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type authorizeSecurityGroupEgressResponse struct {
	XMLName   xml.Name `xml:"AuthorizeSecurityGroupEgressResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type revokeSecurityGroupIngressResponse struct {
	XMLName   xml.Name `xml:"RevokeSecurityGroupIngressResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

func parseIPPermissions(vals url.Values) []SecurityGroupRule {
	var rules []SecurityGroupRule

	for i := 1; ; i++ {
		proto := vals.Get(fmt.Sprintf("IpPermissions.%d.IpProtocol", i))
		if proto == "" {
			break
		}

		fromPort := 0
		toPort := 0

		fromKey := fmt.Sprintf("IpPermissions.%d.FromPort", i)
		toKey := fmt.Sprintf("IpPermissions.%d.ToPort", i)
		// Ports default to 0 if not provided or unparseable, which is correct for protocols
		// like -1 (all traffic) where port ranges are not meaningful.
		_, _ = fmt.Sscan(vals.Get(fromKey), &fromPort)
		_, _ = fmt.Sscan(vals.Get(toKey), &toPort)

		for j := 1; ; j++ {
			cidr := vals.Get(fmt.Sprintf("IpPermissions.%d.IpRanges.%d.CidrIp", i, j))
			if cidr == "" {
				break
			}

			description := vals.Get(fmt.Sprintf("IpPermissions.%d.IpRanges.%d.Description", i, j))

			rules = append(rules, SecurityGroupRule{
				Protocol:    proto,
				FromPort:    fromPort,
				ToPort:      toPort,
				IPRange:     cidr,
				Description: description,
			})
		}

		// Security-group source references (gap 6).
		for j := 1; ; j++ {
			srcGroupID := vals.Get(fmt.Sprintf("IpPermissions.%d.Groups.%d.GroupId", i, j))
			if srcGroupID == "" {
				break
			}

			ownerID := vals.Get(fmt.Sprintf("IpPermissions.%d.Groups.%d.UserId", i, j))
			description := vals.Get(fmt.Sprintf("IpPermissions.%d.Groups.%d.Description", i, j))

			rules = append(rules, SecurityGroupRule{
				Protocol:           proto,
				FromPort:           fromPort,
				ToPort:             toPort,
				SourceGroupID:      srcGroupID,
				SourceGroupOwnerID: ownerID,
				Description:        description,
			})
		}
	}

	return rules
}

func (h *Handler) handleAuthorizeSecurityGroupIngress(vals url.Values, reqID string) (any, error) {
	groupID := vals.Get("GroupId")
	if groupID == "" {
		return nil, fmt.Errorf("%w: GroupId is required", ErrInvalidParameter)
	}

	rules := parseIPPermissions(vals)

	if err := h.Backend.AuthorizeSecurityGroupIngress(groupID, rules); err != nil {
		return nil, err
	}

	return &authorizeSecurityGroupIngressResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleAuthorizeSecurityGroupEgress(vals url.Values, reqID string) (any, error) {
	groupID := vals.Get("GroupId")
	if groupID == "" {
		return nil, fmt.Errorf("%w: GroupId is required", ErrInvalidParameter)
	}

	rules := parseIPPermissions(vals)

	if err := h.Backend.AuthorizeSecurityGroupEgress(groupID, rules); err != nil {
		return nil, err
	}

	return &authorizeSecurityGroupEgressResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}

func (h *Handler) handleRevokeSecurityGroupIngress(vals url.Values, reqID string) (any, error) {
	groupID := vals.Get("GroupId")
	if groupID == "" {
		return nil, fmt.Errorf("%w: GroupId is required", ErrInvalidParameter)
	}

	rules := parseIPPermissions(vals)

	if err := h.Backend.RevokeSecurityGroupIngress(groupID, rules); err != nil {
		return nil, err
	}

	return &revokeSecurityGroupIngressResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}

// handleImportKeyPair is a stub for ImportKeyPair (accepts public key material, stores fingerprint).

func (h *Handler) handleDescribeSecurityGroups(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "GroupId")
	groups := h.Backend.DescribeSecurityGroups(ids)

	// Apply named filters: vpc-id, group-name, group-id.
	filters := parseEC2Filters(vals)
	groups = applySecurityGroupFilters(groups, filters)

	items := make([]sgItem, 0, len(groups))
	for _, sg := range groups {
		items = append(items, toSGItem(sg, h.Backend.TagsForResource(sg.ID)))
	}

	return &describeSecurityGroupsResponse{
		Xmlns:             ec2XMLNS,
		RequestID:         reqID,
		SecurityGroupInfo: sgItemSet{Items: items},
	}, nil
}

func (h *Handler) handleCreateSecurityGroup(vals url.Values, reqID string) (any, error) {
	name := vals.Get("GroupName")
	desc := vals.Get("GroupDescription")
	vpcID := vals.Get("VpcId")

	sg, err := h.Backend.CreateSecurityGroup(name, desc, vpcID)
	if err != nil {
		return nil, err
	}

	tags := parseTagSpecification(vals, "security-group")
	if len(tags) > 0 {
		if err = h.Backend.CreateTags([]string{sg.ID}, tags); err != nil {
			return nil, err
		}
	}

	return &createSecurityGroupResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		GroupID:   sg.ID,
		Return:    true,
		TagSet:    tagItemsFromMap(tags),
	}, nil
}

func (h *Handler) handleDeleteSecurityGroup(vals url.Values, reqID string) (any, error) {
	id := vals.Get("GroupId")
	if id == "" {
		return nil, fmt.Errorf("%w: GroupId is required", ErrInvalidParameter)
	}

	if err := h.Backend.DeleteSecurityGroup(id); err != nil {
		return nil, err
	}

	return &deleteSecurityGroupResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    true,
	}, nil
}

// handleRevokeSecurityGroupEgress removes matching egress rules from a security group.
// Terraform calls this to revoke the default egress rule when creating a security group.
func (h *Handler) handleRevokeSecurityGroupEgress(vals url.Values, reqID string) (any, error) {
	groupID := vals.Get("GroupId")
	if groupID == "" {
		return nil, fmt.Errorf("%w: GroupId is required", ErrInvalidParameter)
	}

	rules := parseIPPermissions(vals)

	if err := h.Backend.RevokeSecurityGroupEgress(groupID, rules); err != nil {
		return nil, err
	}

	return &revokeSecurityGroupEgressResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    ec2BooleanTrue,
	}, nil
}

func toSGItem(sg *SecurityGroup, tags map[string]string) sgItem {
	return sgItem{
		GroupID:             sg.ID,
		GroupName:           sg.Name,
		GroupDescription:    sg.Description,
		VPCID:               sg.VPCID,
		TagSet:              tagItemsFromMap(tags),
		IPPermissions:       toIPPermissionItems(sg.IngressRules),
		IPPermissionsEgress: toIPPermissionItems(sg.EgressRules),
	}
}

// toIPPermissionItems converts the flat per-range/per-source
// SecurityGroupRule entries this backend stores into one ipPermissionItem
// each, carrying a single IpRanges or Groups member. AWS's real wire shape
// allows either representation (fewer IpPermission entries each holding
// several ranges, or one per range); a typed client iterating the flattened
// result observes the same protocol/port/CIDR/group data either way.
func toIPPermissionItems(rules []SecurityGroupRule) []ipPermissionItem {
	items := make([]ipPermissionItem, 0, len(rules))

	for _, r := range rules {
		item := ipPermissionItem{
			IPProtocol: r.Protocol,
			FromPort:   r.FromPort,
			ToPort:     r.ToPort,
		}

		switch {
		case r.SourceGroupID != "":
			item.Groups = []userIDGroupPairItem{{
				GroupID:     r.SourceGroupID,
				UserID:      r.SourceGroupOwnerID,
				Description: r.Description,
			}}
		case r.IPRange != "":
			item.IPRanges = []ipRangeItem{{
				CidrIP:      r.IPRange,
				Description: r.Description,
			}}
		}

		items = append(items, item)
	}

	return items
}

type ipRangeItem struct {
	CidrIP      string `xml:"cidrIp"`
	Description string `xml:"description,omitempty"`
}

type userIDGroupPairItem struct {
	GroupID     string `xml:"groupId"`
	UserID      string `xml:"userId,omitempty"`
	Description string `xml:"description,omitempty"`
}

type ipPermissionItem struct {
	IPProtocol string                `xml:"ipProtocol"`
	IPRanges   []ipRangeItem         `xml:"ipRanges>item,omitempty"`
	Groups     []userIDGroupPairItem `xml:"groups>item,omitempty"`
	FromPort   int                   `xml:"fromPort"`
	ToPort     int                   `xml:"toPort"`
}

type sgItem struct {
	GroupID             string             `xml:"groupId"`
	GroupName           string             `xml:"groupName"`
	GroupDescription    string             `xml:"groupDescription"`
	VPCID               string             `xml:"vpcId,omitempty"`
	TagSet              []simpleTagItem    `xml:"tagSet>item"`
	IPPermissions       []ipPermissionItem `xml:"ipPermissions>item"`
	IPPermissionsEgress []ipPermissionItem `xml:"ipPermissionsEgress>item"`
}

type sgItemSet struct {
	Items []sgItem `xml:"item"`
}

type describeSecurityGroupsResponse struct {
	XMLName           xml.Name  `xml:"DescribeSecurityGroupsResponse"`
	Xmlns             string    `xml:"xmlns,attr"`
	RequestID         string    `xml:"requestId"`
	SecurityGroupInfo sgItemSet `xml:"securityGroupInfo"`
}

type createSecurityGroupResponse struct {
	XMLName   xml.Name        `xml:"CreateSecurityGroupResponse"`
	Xmlns     string          `xml:"xmlns,attr"`
	RequestID string          `xml:"requestId"`
	GroupID   string          `xml:"groupId"`
	TagSet    []simpleTagItem `xml:"tagSet>item"`
	Return    bool            `xml:"return"`
}

type deleteSecurityGroupResponse struct {
	XMLName   xml.Name `xml:"DeleteSecurityGroupResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type revokeSecurityGroupEgressResponse struct {
	XMLName   xml.Name `xml:"RevokeSecurityGroupEgressResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    string   `xml:"return"`
}
