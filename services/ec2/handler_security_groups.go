package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
)

// associateSGVpcResponse matches AssociateSecurityGroupVpcOutput
// (ec2@v1.319.1 api_op_AssociateSecurityGroupVpc.go / deserializers.go,
// awsEc2query_deserializeOpDocumentAssociateSecurityGroupVpcOutput): a flat
// <state> scalar, not a nested element. The real deserializer reads it via
// decoder.Value(), which fails outright ("expected value for state element,
// got xml.StartElement") if <state> contains a child element instead of text.
type associateSGVpcResponse struct {
	XMLName   xml.Name `xml:"AssociateSecurityGroupVpcResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	State     string   `xml:"state"`
}

// sgVpcAssocStateDisassociated is the terminal state this backend reports
// immediately after DisassociateSecurityGroupVpc, since it drops the
// association synchronously rather than modeling the real API's transient
// "disassociating" state.
const sgVpcAssocStateDisassociated = "disassociated"

// disassociateSGVpcResponse matches DisassociateSecurityGroupVpcOutput
// (ec2@v1.319.1 api_op_DisassociateSecurityGroupVpc.go): a flat <state>
// scalar, unlike AssociateSecurityGroupVpcOutput's element name being the
// same but this op has no Return field at all.
type disassociateSGVpcResponse struct {
	XMLName   xml.Name `xml:"DisassociateSecurityGroupVpcResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	State     string   `xml:"state"`
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
	NextToken             string   `xml:"nextToken,omitempty"`
	StaleSecurityGroupSet struct {
		Items []staleSGItem `xml:"item"`
	} `xml:"staleSecurityGroupSet"`
}

type sgVpcAssocItem struct {
	GroupID      string `xml:"groupId"`
	GroupOwnerID string `xml:"groupOwnerId,omitempty"`
	VpcID        string `xml:"vpcId"`
	VpcOwnerID   string `xml:"vpcOwnerId,omitempty"`
	State        string `xml:"state"`
}

type describeSecurityGroupVpcAssociationsResponse struct {
	XMLName                        xml.Name `xml:"DescribeSecurityGroupVpcAssociationsResponse"`
	RequestID                      string   `xml:"requestId"`
	NextToken                      string   `xml:"nextToken,omitempty"`
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

	return &associateSGVpcResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		State:     result.State,
	}, nil
}

func (h *Handler) handleDisassociateSecurityGroupVpc(vals url.Values, reqID string) (any, error) {
	sgID := vals.Get("GroupId")
	vpcID := vals.Get("VpcId")
	if err := h.Backend.DisassociateSecurityGroupVpc(sgID, vpcID); err != nil {
		return nil, err
	}

	return &disassociateSGVpcResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		State:     sgVpcAssocStateDisassociated,
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

	maxResults, offset, err := parseEC2Pagination(vals, ec2PageMinDefault, ec2PageMaxDefault, ec2PageMaxDefault)
	if err != nil {
		return nil, err
	}

	var nextToken string
	stale, nextToken = pageSlice(stale, offset, maxResults)

	resp := &describeStaleSecurityGroupsResponse{RequestID: reqID, NextToken: nextToken}
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

	maxResults, offset, err := parseEC2Pagination(vals, ec2PageMinDefault, ec2PageMaxDefault, ec2PageMaxDefault)
	if err != nil {
		return nil, err
	}

	var nextToken string
	assocs, nextToken = pageSlice(assocs, offset, maxResults)

	resp := &describeSecurityGroupVpcAssociationsResponse{RequestID: reqID, NextToken: nextToken}
	for _, a := range assocs {
		resp.SecurityGroupVpcAssociationSet.Items = append(
			resp.SecurityGroupVpcAssociationSet.Items,
			sgVpcAssocItem{
				GroupID:      a.SGID,
				GroupOwnerID: a.GroupOwnerID,
				VpcID:        a.VPCID,
				VpcOwnerID:   a.VPCOwnerID,
				State:        a.State,
			},
		)
	}

	return resp, nil
}

type getSecurityGroupsForVpcResponse struct {
	XMLName                xml.Name `xml:"GetSecurityGroupsForVpcResponse"`
	RequestID              string   `xml:"requestId"`
	NextToken              string   `xml:"nextToken,omitempty"`
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

	maxResults, offset, err := parseEC2Pagination(vals, ec2PageMinDefault, ec2PageMaxDefault, ec2PageMaxDefault)
	if err != nil {
		return nil, err
	}

	var nextToken string
	sgs, nextToken = pageSlice(sgs, offset, maxResults)

	resp := &getSecurityGroupsForVpcResponse{RequestID: reqID, NextToken: nextToken}
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
	// DescribeSecurityGroupRulesInput carries no top-level GroupId — the real
	// client sends it as Filter.N.Name=group-id / Filter.N.Value.M, not
	// Filter.1.Value (which is never a valid key: AWS query-list values are
	// always indexed).
	filters := parseEC2Filters(vals)

	groupIDs := filters["group-id"]
	if len(groupIDs) == 0 {
		groupIDs = []string{""}
	}

	var rules []*SecurityGroupRuleDetail
	for _, groupID := range groupIDs {
		groupRules, err := h.Backend.DescribeSecurityGroupRules(groupID)
		if err != nil {
			return nil, err
		}

		rules = append(rules, groupRules...)
	}

	maxResults, offset, err := parseEC2Pagination(
		vals, ec2PageMinSecurityGroupRules, ec2PageMaxDefault, ec2PageMaxDefault,
	)
	if err != nil {
		return nil, err
	}

	var nextToken string
	rules, nextToken = pageSlice(rules, offset, maxResults)

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
		NextToken: nextToken,
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
	NextToken string          `xml:"nextToken,omitempty"`
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
	XMLName              xml.Name        `xml:"AuthorizeSecurityGroupIngressResponse"`
	Xmlns                string          `xml:"xmlns,attr"`
	RequestID            string          `xml:"requestId"`
	SecurityGroupRuleSet sgRuleDetailSet `xml:"securityGroupRuleSet"`
	Return               bool            `xml:"return"`
}

type authorizeSecurityGroupEgressResponse struct {
	XMLName              xml.Name        `xml:"AuthorizeSecurityGroupEgressResponse"`
	Xmlns                string          `xml:"xmlns,attr"`
	RequestID            string          `xml:"requestId"`
	SecurityGroupRuleSet sgRuleDetailSet `xml:"securityGroupRuleSet"`
	Return               bool            `xml:"return"`
}

type revokeSecurityGroupIngressResponse struct {
	XMLName                     xml.Name           `xml:"RevokeSecurityGroupIngressResponse"`
	Xmlns                       string             `xml:"xmlns,attr"`
	RequestID                   string             `xml:"requestId"`
	RevokedSecurityGroupRuleSet sgRuleDetailSet    `xml:"revokedSecurityGroupRuleSet"`
	UnknownIPPermissionSet      []ipPermissionItem `xml:"unknownIpPermissionSet>item,omitempty"`
	Return                      bool               `xml:"return"`
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

// newlyAddedRuleDetails re-reads a security group's rules after an Authorize
// call and returns the last n of the requested direction. Authorize appends
// rather than inserts and rejects duplicates (validateSecurityGroupRules), so
// the tail of the direction-filtered, index-ordered list is exactly the set
// just added.
func newlyAddedRuleDetails(b Backend, groupID string, n int, egress bool) ([]*SecurityGroupRuleDetail, error) {
	all, err := b.DescribeSecurityGroupRules(groupID)
	if err != nil {
		return nil, err
	}

	filtered := make([]*SecurityGroupRuleDetail, 0, len(all))
	for _, d := range all {
		if d.IsEgress == egress {
			filtered = append(filtered, d)
		}
	}

	if n > len(filtered) {
		n = len(filtered)
	}

	return filtered[len(filtered)-n:], nil
}

func sgRuleDetailItemsFrom(details []*SecurityGroupRuleDetail) []sgRuleDetailItem {
	items := make([]sgRuleDetailItem, 0, len(details))
	for _, d := range details {
		items = append(items, sgRuleDetailItem{
			SecurityGroupRuleID: d.SecurityGroupRuleID,
			GroupID:             d.GroupID,
			Protocol:            d.Protocol,
			CIDRIPv4:            d.CIDRIPv4,
			Description:         d.Description,
			FromPort:            d.FromPort,
			ToPort:              d.ToPort,
			IsEgress:            d.IsEgress,
		})
	}

	return items
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

	added, err := newlyAddedRuleDetails(h.Backend, groupID, len(rules), false)
	if err != nil {
		return nil, err
	}

	return &authorizeSecurityGroupIngressResponse{
		Xmlns:                ec2XMLNS,
		RequestID:            reqID,
		Return:               true,
		SecurityGroupRuleSet: sgRuleDetailSet{Items: sgRuleDetailItemsFrom(added)},
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

	added, err := newlyAddedRuleDetails(h.Backend, groupID, len(rules), true)
	if err != nil {
		return nil, err
	}

	return &authorizeSecurityGroupEgressResponse{
		Xmlns:                ec2XMLNS,
		RequestID:            reqID,
		Return:               true,
		SecurityGroupRuleSet: sgRuleDetailSet{Items: sgRuleDetailItemsFrom(added)},
	}, nil
}

func (h *Handler) handleRevokeSecurityGroupIngress(vals url.Values, reqID string) (any, error) {
	groupID := vals.Get("GroupId")
	if groupID == "" {
		return nil, fmt.Errorf("%w: GroupId is required", ErrInvalidParameter)
	}

	rules := parseIPPermissions(vals)

	revoked, unknown, err := h.Backend.RevokeSecurityGroupIngress(groupID, rules)
	if err != nil {
		return nil, err
	}

	return &revokeSecurityGroupIngressResponse{
		Xmlns:                       ec2XMLNS,
		RequestID:                   reqID,
		Return:                      true,
		RevokedSecurityGroupRuleSet: sgRuleDetailSet{Items: sgRuleDetailItemsFrom(revoked)},
		UnknownIPPermissionSet:      toIPPermissionItems(unknown),
	}, nil
}

// handleImportKeyPair is a stub for ImportKeyPair (accepts public key material, stores fingerprint).

func (h *Handler) handleDescribeSecurityGroups(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "GroupId")

	var groups []*SecurityGroup
	if names := parseMemberList(vals, "GroupName"); len(ids) == 0 && len(names) > 0 {
		for _, sg := range h.Backend.DescribeSecurityGroups(nil) {
			if anyEqual(sg.Name, names) {
				groups = append(groups, sg)
			}
		}
	} else {
		groups = h.Backend.DescribeSecurityGroups(ids)
	}

	// Apply named filters: vpc-id, group-name, group-id.
	filters := parseEC2Filters(vals)
	groups = applySecurityGroupFilters(groups, filters, h.Backend)

	maxResults, offset, err := parseEC2Pagination(
		vals, ec2PageMinSecurityGroups, ec2PageMaxDefault, ec2PageMaxDefault,
	)
	if err != nil {
		return nil, err
	}

	var nextToken string
	groups, nextToken = pageSlice(groups, offset, maxResults)

	items := make([]sgItem, 0, len(groups))
	for _, sg := range groups {
		items = append(items, toSGItem(sg, h.Backend.TagsForResource(sg.ID)))
	}

	return &describeSecurityGroupsResponse{
		Xmlns:             ec2XMLNS,
		RequestID:         reqID,
		NextToken:         nextToken,
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
		Xmlns:            ec2XMLNS,
		RequestID:        reqID,
		GroupID:          sg.ID,
		SecurityGroupArn: sg.ARN,
		Return:           true,
		TagSet:           tagItemsFromMap(tags),
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
		GroupID:   id,
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

	revoked, err := h.Backend.RevokeSecurityGroupEgress(groupID, rules)
	if err != nil {
		return nil, err
	}

	return &revokeSecurityGroupEgressResponse{
		Xmlns:                       ec2XMLNS,
		RequestID:                   reqID,
		Return:                      true,
		RevokedSecurityGroupRuleSet: sgRuleDetailSet{Items: sgRuleDetailItemsFrom(revoked)},
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
	NextToken         string    `xml:"nextToken,omitempty"`
	SecurityGroupInfo sgItemSet `xml:"securityGroupInfo"`
}

type createSecurityGroupResponse struct {
	XMLName          xml.Name        `xml:"CreateSecurityGroupResponse"`
	Xmlns            string          `xml:"xmlns,attr"`
	RequestID        string          `xml:"requestId"`
	GroupID          string          `xml:"groupId"`
	SecurityGroupArn string          `xml:"securityGroupArn"`
	TagSet           []simpleTagItem `xml:"tagSet>item"`
	Return           bool            `xml:"return"`
}

type deleteSecurityGroupResponse struct {
	XMLName   xml.Name `xml:"DeleteSecurityGroupResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	GroupID   string   `xml:"groupId"`
	Return    bool     `xml:"return"`
}

// revokeSecurityGroupEgressResponse matches RevokeSecurityGroupEgressOutput
// (ec2@v1.319.1 deserializers.go,
// awsEc2query_deserializeOpDocumentRevokeSecurityGroupEgressOutput): same
// shape as RevokeSecurityGroupIngressOutput. UnknownIpPermissionSet is always
// empty here since Backend.RevokeSecurityGroupEgress fails the whole call
// atomically on any unmatched rule rather than reporting it back as unknown.
type revokeSecurityGroupEgressResponse struct {
	XMLName                     xml.Name           `xml:"RevokeSecurityGroupEgressResponse"`
	Xmlns                       string             `xml:"xmlns,attr"`
	RequestID                   string             `xml:"requestId"`
	RevokedSecurityGroupRuleSet sgRuleDetailSet    `xml:"revokedSecurityGroupRuleSet"`
	UnknownIPPermissionSet      []ipPermissionItem `xml:"unknownIpPermissionSet>item,omitempty"`
	Return                      bool               `xml:"return"`
}
