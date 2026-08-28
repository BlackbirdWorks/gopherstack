package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

func toVpcEndpointItem(ep *VpcEndpoint, tags map[string]string) vpcEndpointItem {
	item := vpcEndpointItem{
		ID:              ep.ID,
		VPCID:           ep.VPCID,
		ServiceName:     ep.ServiceName,
		State:           ep.State,
		VpcEndpointType: ep.VpcEndpointType,
		OwnerID:         ep.OwnerID,
		CreateTime:      ep.CreateTime.Format(time.RFC3339),
		TagSet:          tagItemsFromMap(tags),
	}

	item.SubnetIDs.Items = append(item.SubnetIDs.Items, ep.SubnetIDs...)
	item.RouteTableIDs.Items = append(item.RouteTableIDs.Items, ep.RouteTableIDs...)

	for _, pr := range ep.PayerResponsibilities {
		item.PayerResponsibilitySet = append(item.PayerResponsibilitySet, payerResponsibilityEntryItem(pr))
	}

	return item
}

func deepDiveSupportedOperations() []string {
	return []string{
		"CreateImage",
		"DescribeImageUsageReports",
		"DescribeNetworkAcls",
		"CreateVpcEndpoint",
		"DescribeVpcEndpoints",
		"CreateLaunchTemplate",
	}
}

func registerDeepDiveOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["CreateImage"] = h.handleCreateImage
	ops["DescribeImageUsageReports"] = h.handleDescribeImageUsageReports
	ops["DescribeNetworkAcls"] = h.handleDescribeNetworkAcls
	ops["CreateVpcEndpoint"] = h.handleCreateVpcEndpoint
	ops["DescribeVpcEndpoints"] = h.handleDescribeVpcEndpoints
	ops["CreateLaunchTemplate"] = h.handleCreateLaunchTemplate
}

func (h *Handler) handleCreateImage(vals url.Values, reqID string) (any, error) {
	image, err := h.Backend.CreateImage(
		vals.Get("InstanceId"),
		vals.Get("Name"),
		vals.Get("Description"),
	)
	if err != nil {
		return nil, err
	}

	return &createImageResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		ImageID:   image.ImageID,
	}, nil
}

func (h *Handler) handleDescribeImageUsageReports(_ url.Values, reqID string) (any, error) {
	reports := h.Backend.DescribeImageUsageReports()
	items := make([]imageUsageReportItem, 0, len(reports))
	for _, report := range reports {
		items = append(items, imageUsageReportItem{
			ImageID:        report.ImageID,
			State:          report.State,
			GenerationDate: report.GenerationDate,
		})
	}

	return &describeImageUsageReportsResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Reports:   imageUsageReportSet{Items: items},
	}, nil
}

func (h *Handler) handleCreateLaunchTemplate(vals url.Values, reqID string) (any, error) {
	dataImageID := vals.Get("LaunchTemplateData.ImageId")
	dataInstanceType := vals.Get("LaunchTemplateData.InstanceType")
	tags := parseTagSpecification(vals, "launch-template")
	template, err := h.Backend.CreateLaunchTemplate(
		vals.Get("LaunchTemplateName"),
		dataImageID,
		dataInstanceType,
		tags,
	)
	if err != nil {
		return nil, err
	}

	return &createLaunchTemplateResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		LaunchTemplate: launchTemplateItem{
			ID:                   template.ID,
			Name:                 template.Name,
			CreateTime:           template.CreateTime.Format(time.RFC3339),
			CreatedBy:            template.CreatedBy,
			DefaultVersionNumber: template.DefaultVersionNumber,
			LatestVersionNumber:  template.LatestVersionNumber,
			TagSet:               tagItemsFromMap(h.Backend.TagsForResource(template.ID)),
		},
	}, nil
}

func (h *Handler) handleCreateVpcEndpoint(vals url.Values, reqID string) (any, error) {
	subnetIDs := parseMemberList(vals, "SubnetId")
	routeTableIDs := parseMemberList(vals, "RouteTableId")
	endpoint, err := h.Backend.CreateVpcEndpointWithRouteTableIDs(
		vals.Get("VpcId"),
		vals.Get("ServiceName"),
		vals.Get("VpcEndpointType"),
		subnetIDs,
		routeTableIDs,
	)
	if err != nil {
		return nil, err
	}

	if tags := parseTagSpecification(vals, "vpc-endpoint"); len(tags) > 0 {
		// CreateVpcEndpoint(WithRouteTableIDs) has no tags parameter of its
		// own (unlike e.g. CreateSubnet/CreateSecurityGroup); tag it via the
		// same shared store CreateTags itself writes to, matching the
		// handler-level pattern used by those other Create* handlers.
		if err = h.Backend.CreateTags([]string{endpoint.ID}, tags); err != nil {
			return nil, err
		}
	}

	return &createVpcEndpointResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Endpoint:  toVpcEndpointItem(endpoint, h.Backend.TagsForResource(endpoint.ID)),
	}, nil
}

func (h *Handler) handleDescribeVpcEndpoints(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "VpcEndpointId")
	endpoints := h.Backend.DescribeVpcEndpoints(ids)
	items := make([]vpcEndpointItem, 0, len(endpoints))
	for _, endpoint := range endpoints {
		items = append(items, toVpcEndpointItem(endpoint, h.Backend.TagsForResource(endpoint.ID)))
	}

	return &describeVpcEndpointsResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Endpoints: vpcEndpointSet{Items: items},
	}, nil
}

func (h *Handler) handleDescribeNetworkAcls(vals url.Values, reqID string) (any, error) {
	// support both Filter.N.Name=vpc-id filter and NetworkAclId.N direct IDs
	filters := parseEC2Filters(vals)
	aclIDs := parseMemberList(vals, "NetworkAclId")

	var vpcIDs []string
	if v, ok := filters[filterKeyVPCID]; ok {
		vpcIDs = v
	}

	acls := filterNetworkACLsByIDs(h.Backend.DescribeNetworkAclsFiltered(vpcIDs), aclIDs)

	maxResults := 0
	if v := vals.Get("MaxResults"); v != "" {
		if _, scanErr := fmt.Sscan(v, &maxResults); scanErr != nil || maxResults < 5 || maxResults > 1000 {
			return nil, fmt.Errorf("%w: MaxResults must be between 5 and 1000", ErrInvalidParameter)
		}
	}

	offset := 0
	if tok := vals.Get("NextToken"); tok != "" {
		n := page.DecodeHMACToken(tok, ec2PaginationSalt)
		if n == 0 {
			return nil, fmt.Errorf("%w: the pagination token is not valid", ErrInvalidPaginationToken)
		}
		offset = n
	}

	var nextToken string
	if maxResults > 0 {
		if offset > len(acls) {
			offset = len(acls)
		}
		acls = acls[offset:]
		if len(acls) > maxResults {
			nextToken = page.EncodeHMACToken(offset+maxResults, ec2PaginationSalt)
			acls = acls[:maxResults]
		}
	}

	items := make([]networkACLItem, 0, len(acls))
	for _, acl := range acls {
		items = append(items, toNetworkACLItem(acl, h.Backend.TagsForResource(acl.ID)))
	}

	return &describeNetworkAclsResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Acls:      networkACLSet{Items: items},
		NextToken: nextToken,
	}, nil
}

func filterNetworkACLsByIDs(acls []*NetworkACL, ids []string) []*NetworkACL {
	if len(ids) == 0 {
		return acls
	}
	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}
	filtered := acls[:0:0]
	for _, acl := range acls {
		if idSet[acl.ID] {
			filtered = append(filtered, acl)
		}
	}

	return filtered
}

// networkACLEntryHasPortRange reports whether protocol carries a meaningful
// PortRange in real AWS's NACL entry shape (tcp/udp only; icmp uses
// IcmpTypeCode instead, and -1/other protocols carry neither).
func networkACLEntryHasPortRange(protocol string) bool {
	return protocol == "6" || protocol == "17"
}

func toNetworkACLItem(acl *NetworkACL, tags map[string]string) networkACLItem {
	assocs := make([]networkACLAssocItem, 0, len(acl.AssociationIDs))
	for _, aid := range acl.AssociationIDs {
		assocs = append(assocs, networkACLAssocItem{
			NetworkACLAssociationID: aid,
			NetworkACLID:            acl.ID,
		})
	}

	entries := make([]networkACLEntryItem, 0, len(acl.Entries))
	for _, e := range acl.Entries {
		item := networkACLEntryItem{
			CIDRBlock:  e.CIDRBlock,
			RuleAction: e.RuleAction,
			Protocol:   e.Protocol,
			RuleNumber: e.RuleNumber,
			Egress:     e.Egress,
		}
		if networkACLEntryHasPortRange(e.Protocol) {
			item.PortRange = &networkACLPortRangeItem{From: e.FromPort, To: e.ToPort}
		}

		entries = append(entries, item)
	}

	return networkACLItem{
		ID:           acl.ID,
		VPCID:        acl.VPCID,
		IsDefault:    acl.IsDefault,
		Associations: networkACLAssocSet{Items: assocs},
		Entries:      networkACLEntrySet{Items: entries},
		TagSet:       tagItemsFromMap(tags),
	}
}

type createImageResponse struct {
	XMLName   xml.Name `xml:"CreateImageResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	ImageID   string   `xml:"imageId"`
}

type imageUsageReportItem struct {
	ImageID        string `xml:"imageId"`
	State          string `xml:"state"`
	GenerationDate string `xml:"generationDate"`
}

type imageUsageReportSet struct {
	Items []imageUsageReportItem `xml:"item"`
}

type describeImageUsageReportsResponse struct {
	XMLName   xml.Name            `xml:"DescribeImageUsageReportsResponse"`
	Xmlns     string              `xml:"xmlns,attr"`
	RequestID string              `xml:"requestId"`
	Reports   imageUsageReportSet `xml:"imageUsageReportSet"`
}

type createLaunchTemplateResponse struct {
	XMLName        xml.Name           `xml:"CreateLaunchTemplateResponse"`
	Xmlns          string             `xml:"xmlns,attr"`
	RequestID      string             `xml:"requestId"`
	LaunchTemplate launchTemplateItem `xml:"launchTemplate"`
}

type vpcEndpointSubnetIDSet struct {
	Items []string `xml:"item"`
}

type vpcEndpointRouteTableIDSet struct {
	Items []string `xml:"item"`
}

type vpcEndpointItem struct {
	ID                     string                         `xml:"vpcEndpointId"`
	VPCID                  string                         `xml:"vpcId"`
	ServiceName            string                         `xml:"serviceName"`
	State                  string                         `xml:"state"`
	VpcEndpointType        string                         `xml:"vpcEndpointType"`
	OwnerID                string                         `xml:"ownerId,omitempty"`
	CreateTime             string                         `xml:"creationTimestamp"`
	SubnetIDs              vpcEndpointSubnetIDSet         `xml:"subnetIdSet"`
	RouteTableIDs          vpcEndpointRouteTableIDSet     `xml:"routeTableIdSet"`
	PayerResponsibilitySet []payerResponsibilityEntryItem `xml:"payerResponsibilitySet>item,omitempty"`
	TagSet                 []simpleTagItem                `xml:"tagSet>item"`
}

type vpcEndpointSet struct {
	Items []vpcEndpointItem `xml:"item"`
}

type createVpcEndpointResponse struct {
	XMLName   xml.Name        `xml:"CreateVpcEndpointResponse"`
	Xmlns     string          `xml:"xmlns,attr"`
	RequestID string          `xml:"requestId"`
	Endpoint  vpcEndpointItem `xml:"vpcEndpoint"`
}

type describeVpcEndpointsResponse struct {
	XMLName   xml.Name       `xml:"DescribeVpcEndpointsResponse"`
	Xmlns     string         `xml:"xmlns,attr"`
	RequestID string         `xml:"requestId"`
	Endpoints vpcEndpointSet `xml:"vpcEndpointSet"`
}

type networkACLAssocItem struct {
	NetworkACLAssociationID string `xml:"networkAclAssociationId"`
	NetworkACLID            string `xml:"networkAclId"`
	SubnetID                string `xml:"subnetId,omitempty"`
}

type networkACLAssocSet struct {
	Items []networkACLAssocItem `xml:"item"`
}

type networkACLPortRangeItem struct {
	From int `xml:"from"`
	To   int `xml:"to"`
}

type networkACLEntryItem struct {
	PortRange  *networkACLPortRangeItem `xml:"portRange,omitempty"`
	CIDRBlock  string                   `xml:"cidrBlock,omitempty"`
	RuleAction string                   `xml:"ruleAction"`
	Protocol   string                   `xml:"protocol"`
	RuleNumber int                      `xml:"ruleNumber"`
	Egress     bool                     `xml:"egress"`
}

type networkACLEntrySet struct {
	Items []networkACLEntryItem `xml:"item"`
}

type networkACLItem struct {
	ID           string             `xml:"networkAclId"`
	VPCID        string             `xml:"vpcId"`
	Associations networkACLAssocSet `xml:"associationSet"`
	Entries      networkACLEntrySet `xml:"entrySet"`
	TagSet       []simpleTagItem    `xml:"tagSet>item"`
	IsDefault    bool               `xml:"default"`
}

type networkACLSet struct {
	Items []networkACLItem `xml:"item"`
}

type describeNetworkAclsResponse struct {
	XMLName   xml.Name      `xml:"DescribeNetworkAclsResponse"`
	Xmlns     string        `xml:"xmlns,attr"`
	RequestID string        `xml:"requestId"`
	NextToken string        `xml:"nextToken,omitempty"`
	Acls      networkACLSet `xml:"networkAclSet"`
}
