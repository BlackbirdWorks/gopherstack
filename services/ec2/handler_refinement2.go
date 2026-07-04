package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"strconv"
)

// ---- Handler registration ----

func registerRefinement2Ops(h *Handler, ops map[string]ec2ActionFn) {
	ops["CreateSnapshot"] = h.handleCreateSnapshot
	ops["DescribeSnapshots"] = h.handleDescribeSnapshots
	ops["DeleteSnapshot"] = h.handleDeleteSnapshot
	ops["CopyImage"] = h.handleCopyImage
	ops["DeregisterImage"] = h.handleDeregisterImage
	ops["ModifyVpcAttribute"] = h.handleModifyVpcAttribute
	ops["ModifySubnetAttribute"] = h.handleModifySubnetAttribute
	ops["CreateNetworkAcl"] = h.handleCreateNetworkACL
	ops["DeleteNetworkAcl"] = h.handleDeleteNetworkACL
	ops["CreateNetworkAclEntry"] = h.handleCreateNetworkACLEntry
	ops["DeleteNetworkAclEntry"] = h.handleDeleteNetworkACLEntry
	ops["DescribeSecurityGroupRules"] = h.handleDescribeSecurityGroupRules
	ops["ModifySecurityGroupRules"] = h.handleModifySecurityGroupRules
	ops["DeleteLaunchTemplate"] = h.handleDeleteLaunchTemplate
	ops["DescribeLaunchTemplateVersions"] = h.handleDescribeLaunchTemplateVersions
	ops["DeleteVpcEndpoints"] = h.handleDeleteVpcEndpoints
}

func refinement2SupportedOperations() []string {
	return []string{
		"CreateSnapshot",
		"DescribeSnapshots",
		"DeleteSnapshot",
		"CopyImage",
		"DeregisterImage",
		"ModifyVpcAttribute",
		"ModifySubnetAttribute",
		"CreateNetworkAcl",
		"DeleteNetworkAcl",
		"CreateNetworkAclEntry",
		"DeleteNetworkAclEntry",
		"DescribeSecurityGroupRules",
		"ModifySecurityGroupRules",
		"DeleteLaunchTemplate",
		"DescribeLaunchTemplateVersions",
		"DeleteVpcEndpoints",
	}
}

// ---- Snapshot handlers ----

func (h *Handler) handleCreateSnapshot(vals url.Values, reqID string) (any, error) {
	snap, err := h.Backend.CreateSnapshot(vals.Get("VolumeId"), vals.Get("Description"))
	if err != nil {
		return nil, err
	}

	return &createSnapshotResponse{
		Xmlns:       ec2XMLNS,
		RequestID:   reqID,
		SnapshotID:  snap.SnapshotID,
		VolumeID:    snap.VolumeID,
		State:       snap.State,
		Progress:    snap.Progress,
		Description: snap.Description,
		VolumeSize:  snap.VolumeSize,
		StartTime:   snap.StartTime.Format("2006-01-02T15:04:05.000Z"),
	}, nil
}

func (h *Handler) handleDescribeSnapshots(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "SnapshotId")
	snaps := h.Backend.DescribeSnapshots(ids)

	filters := parseEC2Filters(vals)
	snaps = applySnapshotFilters(snaps, filters, h.Backend)

	maxResults := 0
	if v := vals.Get("MaxResults"); v != "" {
		if _, scanErr := fmt.Sscan(v, &maxResults); scanErr != nil || maxResults < 5 || maxResults > 1000 {
			return nil, fmt.Errorf("%w: MaxResults must be between 5 and 1000", ErrInvalidParameter)
		}
	}

	offset := 0
	if tok := vals.Get("NextToken"); tok != "" {
		_, _ = fmt.Sscan(tok, &offset)
	}

	var nextToken string
	if maxResults > 0 {
		if offset > len(snaps) {
			offset = len(snaps)
		}
		snaps = snaps[offset:]
		if len(snaps) > maxResults {
			nextToken = strconv.Itoa(offset + maxResults)
			snaps = snaps[:maxResults]
		}
	}

	items := make([]snapshotItem, 0, len(snaps))
	for _, s := range snaps {
		items = append(items, snapshotItem{
			SnapshotID:  s.SnapshotID,
			VolumeID:    s.VolumeID,
			State:       s.State,
			Progress:    s.Progress,
			Description: s.Description,
			VolumeSize:  s.VolumeSize,
			StartTime:   s.StartTime.Format("2006-01-02T15:04:05.000Z"),
		})
	}

	return &describeSnapshotsResponse{
		Xmlns:       ec2XMLNS,
		RequestID:   reqID,
		SnapshotSet: snapshotSet{Items: items},
		NextToken:   nextToken,
	}, nil
}

func (h *Handler) handleDeleteSnapshot(vals url.Values, _ string) (any, error) {
	return nil, h.Backend.DeleteSnapshot(vals.Get("SnapshotId"))
}

// ---- AMI lifecycle handlers ----

func (h *Handler) handleCopyImage(vals url.Values, reqID string) (any, error) {
	image, err := h.Backend.CopyImage(
		vals.Get("SourceImageId"),
		vals.Get("Name"),
		vals.Get("Description"),
	)
	if err != nil {
		return nil, err
	}

	return &copyImageResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		ImageID:   image.ImageID,
	}, nil
}

func (h *Handler) handleDeregisterImage(vals url.Values, _ string) (any, error) {
	return nil, h.Backend.DeregisterImage(vals.Get("ImageId"))
}

// ---- VPC / Subnet attribute handlers ----

func (h *Handler) handleModifyVpcAttribute(vals url.Values, reqID string) (any, error) {
	var attr string
	var valueStr string

	switch {
	case vals.Get("EnableDnsSupport.Value") != "":
		attr = attrEnableDNSSupport
		valueStr = vals.Get("EnableDnsSupport.Value")
	case vals.Get("EnableDnsHostnames.Value") != "":
		attr = attrEnableDNSHostnames
		valueStr = vals.Get("EnableDnsHostnames.Value")
	default:
		attr = attrEnableDNSSupport
		valueStr = ec2BooleanTrue
	}

	value, _ := strconv.ParseBool(valueStr)

	if err := h.Backend.ModifyVpcAttribute(vals.Get("VpcId"), attr, value); err != nil {
		return nil, err
	}

	return &genericReturnResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    ec2BooleanTrue,
	}, nil
}

func (h *Handler) handleModifySubnetAttribute(vals url.Values, reqID string) (any, error) {
	var attr string
	var valueStr string

	switch {
	case vals.Get("MapPublicIpOnLaunch.Value") != "":
		attr = attrMapPublicIPOnLaunch
		valueStr = vals.Get("MapPublicIpOnLaunch.Value")
	default:
		attr = attrMapPublicIPOnLaunch
		valueStr = ec2BooleanTrue
	}

	value, _ := strconv.ParseBool(valueStr)

	if err := h.Backend.ModifySubnetAttribute(vals.Get("SubnetId"), attr, value); err != nil {
		return nil, err
	}

	return &genericReturnResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Return:    ec2BooleanTrue,
	}, nil
}

// ---- NACL handlers ----

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

func (h *Handler) handleDeleteLaunchTemplate(vals url.Values, _ string) (any, error) {
	return nil, h.Backend.DeleteLaunchTemplate(vals.Get("LaunchTemplateId"))
}

func (h *Handler) handleDescribeLaunchTemplateVersions(vals url.Values, reqID string) (any, error) {
	versions, err := h.Backend.DescribeLaunchTemplateVersions(vals.Get("LaunchTemplateId"))
	if err != nil {
		return nil, err
	}

	items := make([]launchTemplateItem, 0, len(versions))
	for _, lt := range versions {
		items = append(items, launchTemplateItem{
			ID:                   lt.ID,
			Name:                 lt.Name,
			CreateTime:           lt.CreateTime.Format("2006-01-02T15:04:05.000Z"),
			CreatedBy:            lt.CreatedBy,
			DefaultVersionNumber: lt.DefaultVersionNumber,
			LatestVersionNumber:  lt.LatestVersionNumber,
		})
	}

	return &describeLaunchTemplateVersionsResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Versions:  launchTemplateVersionSet{Items: items},
	}, nil
}

// ---- VPC endpoint delete handler ----

func (h *Handler) handleDeleteVpcEndpoints(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "VpcEndpointId")

	unsuccessful, err := h.Backend.DeleteVpcEndpoints(ids)
	if err != nil {
		return nil, err
	}

	items := make([]unsuccessfulEndpointItem, 0, len(unsuccessful))
	for _, id := range unsuccessful {
		items = append(items, unsuccessfulEndpointItem{ID: id})
	}

	return &deleteVpcEndpointsResponse{
		Xmlns:        ec2XMLNS,
		RequestID:    reqID,
		Unsuccessful: unsuccessfulEndpointSet{Items: items},
	}, nil
}

// ---- XML response types ----

type createSnapshotResponse struct {
	XMLName     xml.Name `xml:"CreateSnapshotResponse"`
	Xmlns       string   `xml:"xmlns,attr"`
	RequestID   string   `xml:"requestId"`
	SnapshotID  string   `xml:"snapshotId"`
	VolumeID    string   `xml:"volumeId"`
	State       string   `xml:"status"`
	Progress    string   `xml:"progress"`
	Description string   `xml:"description,omitempty"`
	StartTime   string   `xml:"startTime"`
	VolumeSize  int      `xml:"volumeSize"`
}

type snapshotItem struct {
	SnapshotID  string `xml:"snapshotId"`
	VolumeID    string `xml:"volumeId"`
	State       string `xml:"status"`
	Progress    string `xml:"progress"`
	Description string `xml:"description,omitempty"`
	StartTime   string `xml:"startTime"`
	VolumeSize  int    `xml:"volumeSize"`
}

type snapshotSet struct {
	Items []snapshotItem `xml:"item"`
}

type describeSnapshotsResponse struct {
	XMLName     xml.Name    `xml:"DescribeSnapshotsResponse"`
	Xmlns       string      `xml:"xmlns,attr"`
	RequestID   string      `xml:"requestId"`
	NextToken   string      `xml:"nextToken,omitempty"`
	SnapshotSet snapshotSet `xml:"snapshotSet"`
}

type copyImageResponse struct {
	XMLName   xml.Name `xml:"CopyImageResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	ImageID   string   `xml:"imageId"`
}

type genericReturnResponse struct {
	XMLName   xml.Name `xml:"Response"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    string   `xml:"return"`
}

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
	FromPort            int    `xml:"fromPort"`
	ToPort              int    `xml:"toPort"`
	IsEgress            bool   `xml:"isEgress"`
}

type sgRuleDetailSet struct {
	Items []sgRuleDetailItem `xml:"item"`
}

type describeSecurityGroupRulesResponse struct {
	XMLName   xml.Name        `xml:"DescribeSecurityGroupRulesResponse"`
	Xmlns     string          `xml:"xmlns,attr"`
	RequestID string          `xml:"requestId"`
	Rules     sgRuleDetailSet `xml:"securityGroupRuleSet"`
}

type launchTemplateVersionSet struct {
	Items []launchTemplateItem `xml:"item"`
}

type describeLaunchTemplateVersionsResponse struct {
	XMLName   xml.Name                 `xml:"DescribeLaunchTemplateVersionsResponse"`
	Xmlns     string                   `xml:"xmlns,attr"`
	RequestID string                   `xml:"requestId"`
	Versions  launchTemplateVersionSet `xml:"launchTemplateVersionSet"`
}

type unsuccessfulEndpointItem struct {
	ID string `xml:"vpcEndpointId"`
}

type unsuccessfulEndpointSet struct {
	Items []unsuccessfulEndpointItem `xml:"item"`
}

type deleteVpcEndpointsResponse struct {
	XMLName      xml.Name                `xml:"DeleteVpcEndpointsResponse"`
	Xmlns        string                  `xml:"xmlns,attr"`
	RequestID    string                  `xml:"requestId"`
	Unsuccessful unsuccessfulEndpointSet `xml:"unsuccessful"`
}
