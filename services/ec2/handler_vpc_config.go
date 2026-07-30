package ec2

import (
	"encoding/xml"
	"net/url"
	"sort"
	"time"
)

// note: handler_vpc_config.go implements the VPC ClassicLink and VPC Block
// Public Access (BPA) families.

// ---- Handler registration ----

func registerVpcConfigOps(h *Handler, ops map[string]ec2ActionFn) {
	// VPC ClassicLink
	ops["AttachClassicLinkVpc"] = h.handleAttachClassicLinkVpc
	ops["DetachClassicLinkVpc"] = h.handleDetachClassicLinkVpc
	ops["DescribeClassicLinkInstances"] = h.handleDescribeClassicLinkInstances
	ops["EnableVpcClassicLink"] = h.handleEnableVpcClassicLink
	ops["DisableVpcClassicLink"] = h.handleDisableVpcClassicLink
	ops["DescribeVpcClassicLink"] = h.handleDescribeVpcClassicLink
	ops["EnableVpcClassicLinkDnsSupport"] = h.handleEnableVpcClassicLinkDNSSupport
	ops["DisableVpcClassicLinkDnsSupport"] = h.handleDisableVpcClassicLinkDNSSupport
	ops["DescribeVpcClassicLinkDnsSupport"] = h.handleDescribeVpcClassicLinkDNSSupport

	// VPC Block Public Access
	ops["ModifyVpcBlockPublicAccessOptions"] = h.handleModifyVpcBlockPublicAccessOptions
	ops["DescribeVpcBlockPublicAccessOptions"] = h.handleDescribeVpcBlockPublicAccessOptions
	ops["CreateVpcBlockPublicAccessExclusion"] = h.handleCreateVpcBlockPublicAccessExclusion
	ops["ModifyVpcBlockPublicAccessExclusion"] = h.handleModifyVpcBlockPublicAccessExclusion
	ops["DeleteVpcBlockPublicAccessExclusion"] = h.handleDeleteVpcBlockPublicAccessExclusion
	ops["DescribeVpcBlockPublicAccessExclusions"] = h.handleDescribeVpcBlockPublicAccessExclusions
}

func vpcConfigSupportedOperations() []string {
	return []string{
		"AttachClassicLinkVpc",
		"DetachClassicLinkVpc",
		"DescribeClassicLinkInstances",
		"EnableVpcClassicLink",
		"DisableVpcClassicLink",
		"DescribeVpcClassicLink",
		"EnableVpcClassicLinkDnsSupport",
		"DisableVpcClassicLinkDnsSupport",
		"DescribeVpcClassicLinkDnsSupport",
		"ModifyVpcBlockPublicAccessOptions",
		"DescribeVpcBlockPublicAccessOptions",
		"CreateVpcBlockPublicAccessExclusion",
		"ModifyVpcBlockPublicAccessExclusion",
		"DeleteVpcBlockPublicAccessExclusion",
		"DescribeVpcBlockPublicAccessExclusions",
	}
}

// ---- shared XML fragments ----

type simpleTagItem struct {
	Key   string `xml:"key"`
	Value string `xml:"value"`
}

// tagItemsFromMap converts a tag map into a deterministically ordered XML tag item slice.
func tagItemsFromMap(tags map[string]string) []simpleTagItem {
	if len(tags) == 0 {
		return nil
	}

	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	items := make([]simpleTagItem, 0, len(tags))
	for _, k := range keys {
		items = append(items, simpleTagItem{Key: k, Value: tags[k]})
	}

	return items
}

type groupIdentifierItem struct {
	GroupID   string `xml:"groupId"`
	GroupName string `xml:"groupName,omitempty"`
}

// ---- VPC ClassicLink ----

type attachClassicLinkVpcResponse struct {
	XMLName   xml.Name `xml:"AttachClassicLinkVpcResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

func (h *Handler) handleAttachClassicLinkVpc(vals url.Values, reqID string) (any, error) {
	instanceID := vals.Get("InstanceId")
	vpcID := vals.Get("VpcId")
	groups := parseMemberList(vals, "SecurityGroupId")

	if err := h.Backend.AttachClassicLinkVpc(instanceID, vpcID, groups); err != nil {
		return nil, err
	}

	return &attachClassicLinkVpcResponse{Xmlns: ec2XMLNS, RequestID: reqID, Return: true}, nil
}

type detachClassicLinkVpcResponse struct {
	XMLName   xml.Name `xml:"DetachClassicLinkVpcResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

func (h *Handler) handleDetachClassicLinkVpc(vals url.Values, reqID string) (any, error) {
	instanceID := vals.Get("InstanceId")
	vpcID := vals.Get("VpcId")

	if err := h.Backend.DetachClassicLinkVpc(instanceID, vpcID); err != nil {
		return nil, err
	}

	return &detachClassicLinkVpcResponse{Xmlns: ec2XMLNS, RequestID: reqID, Return: true}, nil
}

type classicLinkInstanceItem struct {
	InstanceID string                `xml:"instanceId"`
	VpcID      string                `xml:"vpcId"`
	GroupSet   []groupIdentifierItem `xml:"groupSet>item"`
	TagSet     []simpleTagItem       `xml:"tagSet>item"`
}

type describeClassicLinkInstancesResponse struct {
	XMLName      xml.Name                  `xml:"DescribeClassicLinkInstancesResponse"`
	Xmlns        string                    `xml:"xmlns,attr"`
	RequestID    string                    `xml:"requestId"`
	NextToken    string                    `xml:"nextToken,omitempty"`
	InstancesSet []classicLinkInstanceItem `xml:"instancesSet>item"`
}

func (h *Handler) handleDescribeClassicLinkInstances(vals url.Values, reqID string) (any, error) {
	instanceIDs := parseMemberList(vals, "InstanceId")
	links := h.Backend.DescribeClassicLinkInstances(instanceIDs)

	resp := &describeClassicLinkInstancesResponse{Xmlns: ec2XMLNS, RequestID: reqID}

	for _, link := range links {
		var groupItems []groupIdentifierItem

		if len(link.Groups) > 0 {
			for _, sg := range h.Backend.DescribeSecurityGroups(link.Groups) {
				groupItems = append(groupItems, groupIdentifierItem{GroupID: sg.ID, GroupName: sg.Name})
			}
		}

		resp.InstancesSet = append(resp.InstancesSet, classicLinkInstanceItem{
			InstanceID: link.InstanceID,
			VpcID:      link.VpcID,
			GroupSet:   groupItems,
			TagSet:     tagItemsFromMap(h.Backend.TagsForResource(link.InstanceID)),
		})
	}

	return resp, nil
}

type enableVpcClassicLinkResponse struct {
	XMLName   xml.Name `xml:"EnableVpcClassicLinkResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

func (h *Handler) handleEnableVpcClassicLink(vals url.Values, reqID string) (any, error) {
	if err := h.Backend.EnableVpcClassicLink(vals.Get("VpcId")); err != nil {
		return nil, err
	}

	return &enableVpcClassicLinkResponse{Xmlns: ec2XMLNS, RequestID: reqID, Return: true}, nil
}

type disableVpcClassicLinkResponse struct {
	XMLName   xml.Name `xml:"DisableVpcClassicLinkResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

func (h *Handler) handleDisableVpcClassicLink(vals url.Values, reqID string) (any, error) {
	if err := h.Backend.DisableVpcClassicLink(vals.Get("VpcId")); err != nil {
		return nil, err
	}

	return &disableVpcClassicLinkResponse{Xmlns: ec2XMLNS, RequestID: reqID, Return: true}, nil
}

type vpcClassicLinkItem struct {
	VpcID              string          `xml:"vpcId"`
	TagSet             []simpleTagItem `xml:"tagSet>item"`
	ClassicLinkEnabled bool            `xml:"classicLinkEnabled"`
}

type describeVpcClassicLinkResponse struct {
	XMLName   xml.Name             `xml:"DescribeVpcClassicLinkResponse"`
	Xmlns     string               `xml:"xmlns,attr"`
	RequestID string               `xml:"requestId"`
	VpcSet    []vpcClassicLinkItem `xml:"vpcSet>item"`
}

func (h *Handler) handleDescribeVpcClassicLink(vals url.Values, reqID string) (any, error) {
	vpcIDs := parseMemberList(vals, "VpcId")

	vpcs, err := h.Backend.DescribeVpcClassicLink(vpcIDs)
	if err != nil {
		return nil, err
	}

	resp := &describeVpcClassicLinkResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, vpc := range vpcs {
		resp.VpcSet = append(resp.VpcSet, vpcClassicLinkItem{
			VpcID:              vpc.ID,
			ClassicLinkEnabled: vpc.ClassicLinkEnabled,
			TagSet:             tagItemsFromMap(h.Backend.TagsForResource(vpc.ID)),
		})
	}

	return resp, nil
}

type enableVpcClassicLinkDNSSupportResponse struct {
	XMLName   xml.Name `xml:"EnableVpcClassicLinkDnsSupportResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

func (h *Handler) handleEnableVpcClassicLinkDNSSupport(vals url.Values, reqID string) (any, error) {
	if err := h.Backend.EnableVpcClassicLinkDNSSupport(vals.Get("VpcId")); err != nil {
		return nil, err
	}

	return &enableVpcClassicLinkDNSSupportResponse{Xmlns: ec2XMLNS, RequestID: reqID, Return: true}, nil
}

type disableVpcClassicLinkDNSSupportResponse struct {
	XMLName   xml.Name `xml:"DisableVpcClassicLinkDnsSupportResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

func (h *Handler) handleDisableVpcClassicLinkDNSSupport(vals url.Values, reqID string) (any, error) {
	if err := h.Backend.DisableVpcClassicLinkDNSSupport(vals.Get("VpcId")); err != nil {
		return nil, err
	}

	return &disableVpcClassicLinkDNSSupportResponse{Xmlns: ec2XMLNS, RequestID: reqID, Return: true}, nil
}

type classicLinkDNSSupportItem struct {
	VpcID                   string `xml:"vpcId"`
	ClassicLinkDNSSupported bool   `xml:"classicLinkDnsSupported"`
}

type describeVpcClassicLinkDNSSupportResponse struct {
	XMLName   xml.Name                    `xml:"DescribeVpcClassicLinkDnsSupportResponse"`
	Xmlns     string                      `xml:"xmlns,attr"`
	RequestID string                      `xml:"requestId"`
	NextToken string                      `xml:"nextToken,omitempty"`
	Vpcs      []classicLinkDNSSupportItem `xml:"vpcs>item"`
}

func (h *Handler) handleDescribeVpcClassicLinkDNSSupport(vals url.Values, reqID string) (any, error) {
	vpcIDs := parseMemberList(vals, "VpcIds")

	vpcs, err := h.Backend.DescribeVpcClassicLinkDNSSupport(vpcIDs)
	if err != nil {
		return nil, err
	}

	resp := &describeVpcClassicLinkDNSSupportResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, vpc := range vpcs {
		resp.Vpcs = append(resp.Vpcs, classicLinkDNSSupportItem{
			VpcID:                   vpc.ID,
			ClassicLinkDNSSupported: vpc.ClassicLinkDNSSupported,
		})
	}

	return resp, nil
}

// ---- VPC Block Public Access ----

type vpcBlockPublicAccessOptionsItem struct {
	AwsAccountID             string `xml:"awsAccountId,omitempty"`
	AwsRegion                string `xml:"awsRegion,omitempty"`
	InternetGatewayBlockMode string `xml:"internetGatewayBlockMode"`
	State                    string `xml:"state,omitempty"`
	Reason                   string `xml:"reason,omitempty"`
	ManagedBy                string `xml:"managedBy,omitempty"`
	ExclusionsAllowed        string `xml:"exclusionsAllowed,omitempty"`
	LastUpdateTimestamp      string `xml:"lastUpdateTimestamp,omitempty"`
}

func vpcBPAOptionsToItem(opts *VpcBlockPublicAccessOptions) vpcBlockPublicAccessOptionsItem {
	item := vpcBlockPublicAccessOptionsItem{
		AwsAccountID:             opts.AwsAccountID,
		AwsRegion:                opts.AwsRegion,
		InternetGatewayBlockMode: opts.InternetGatewayBlockMode,
		State:                    opts.State,
		Reason:                   opts.Reason,
		ManagedBy:                opts.ManagedBy,
		ExclusionsAllowed:        opts.ExclusionsAllowed,
	}
	if !opts.LastUpdateTimestamp.IsZero() {
		item.LastUpdateTimestamp = opts.LastUpdateTimestamp.Format(time.RFC3339)
	}

	return item
}

type modifyVpcBlockPublicAccessOptionsResponse struct {
	XMLName                     xml.Name                        `xml:"ModifyVpcBlockPublicAccessOptionsResponse"`
	Xmlns                       string                          `xml:"xmlns,attr"`
	RequestID                   string                          `xml:"requestId"`
	VpcBlockPublicAccessOptions vpcBlockPublicAccessOptionsItem `xml:"vpcBlockPublicAccessOptions"`
}

func (h *Handler) handleModifyVpcBlockPublicAccessOptions(vals url.Values, reqID string) (any, error) {
	opts, err := h.Backend.ModifyVpcBlockPublicAccessOptions(vals.Get("InternetGatewayBlockMode"))
	if err != nil {
		return nil, err
	}

	return &modifyVpcBlockPublicAccessOptionsResponse{
		Xmlns:                       ec2XMLNS,
		RequestID:                   reqID,
		VpcBlockPublicAccessOptions: vpcBPAOptionsToItem(opts),
	}, nil
}

type describeVpcBlockPublicAccessOptionsResponse struct {
	XMLName                     xml.Name                        `xml:"DescribeVpcBlockPublicAccessOptionsResponse"`
	Xmlns                       string                          `xml:"xmlns,attr"`
	RequestID                   string                          `xml:"requestId"`
	VpcBlockPublicAccessOptions vpcBlockPublicAccessOptionsItem `xml:"vpcBlockPublicAccessOptions"`
}

func (h *Handler) handleDescribeVpcBlockPublicAccessOptions(_ url.Values, reqID string) (any, error) {
	opts := h.Backend.DescribeVpcBlockPublicAccessOptions()

	return &describeVpcBlockPublicAccessOptionsResponse{
		Xmlns:                       ec2XMLNS,
		RequestID:                   reqID,
		VpcBlockPublicAccessOptions: vpcBPAOptionsToItem(opts),
	}, nil
}

type vpcBlockPublicAccessExclusionItem struct {
	ExclusionID                  string          `xml:"exclusionId"`
	ResourceArn                  string          `xml:"resourceArn,omitempty"`
	InternetGatewayExclusionMode string          `xml:"internetGatewayExclusionMode"`
	State                        string          `xml:"state,omitempty"`
	Reason                       string          `xml:"reason,omitempty"`
	CreationTimestamp            string          `xml:"creationTimestamp,omitempty"`
	LastUpdateTimestamp          string          `xml:"lastUpdateTimestamp,omitempty"`
	TagSet                       []simpleTagItem `xml:"tagSet>item"`
}

func vpcBPAExclusionToItem(
	excl *VpcBlockPublicAccessExclusion, tags map[string]string,
) vpcBlockPublicAccessExclusionItem {
	item := vpcBlockPublicAccessExclusionItem{
		ExclusionID:                  excl.ExclusionID,
		ResourceArn:                  excl.ResourceArn,
		InternetGatewayExclusionMode: excl.InternetGatewayExclusionMode,
		State:                        excl.State,
		Reason:                       excl.Reason,
		TagSet:                       tagItemsFromMap(tags),
	}
	if !excl.CreationTimestamp.IsZero() {
		item.CreationTimestamp = excl.CreationTimestamp.Format(time.RFC3339)
	}

	if !excl.LastUpdateTimestamp.IsZero() {
		item.LastUpdateTimestamp = excl.LastUpdateTimestamp.Format(time.RFC3339)
	}

	return item
}

type createVpcBlockPublicAccessExclusionResponse struct {
	XMLName                       xml.Name                          `xml:"CreateVpcBlockPublicAccessExclusionResponse"`
	Xmlns                         string                            `xml:"xmlns,attr"`
	RequestID                     string                            `xml:"requestId"`
	VpcBlockPublicAccessExclusion vpcBlockPublicAccessExclusionItem `xml:"vpcBlockPublicAccessExclusion"`
}

func (h *Handler) handleCreateVpcBlockPublicAccessExclusion(vals url.Values, reqID string) (any, error) {
	tags := parseTagSpecification(vals, "vpc-block-public-access-exclusion")

	excl, err := h.Backend.CreateVpcBlockPublicAccessExclusion(
		vals.Get("VpcId"),
		vals.Get("SubnetId"),
		vals.Get("InternetGatewayExclusionMode"),
		tags,
	)
	if err != nil {
		return nil, err
	}

	return &createVpcBlockPublicAccessExclusionResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		VpcBlockPublicAccessExclusion: vpcBPAExclusionToItem(
			excl, h.Backend.TagsForResource(excl.ExclusionID),
		),
	}, nil
}

type modifyVpcBlockPublicAccessExclusionResponse struct {
	XMLName                       xml.Name                          `xml:"ModifyVpcBlockPublicAccessExclusionResponse"`
	Xmlns                         string                            `xml:"xmlns,attr"`
	RequestID                     string                            `xml:"requestId"`
	VpcBlockPublicAccessExclusion vpcBlockPublicAccessExclusionItem `xml:"vpcBlockPublicAccessExclusion"`
}

func (h *Handler) handleModifyVpcBlockPublicAccessExclusion(vals url.Values, reqID string) (any, error) {
	excl, err := h.Backend.ModifyVpcBlockPublicAccessExclusion(
		vals.Get("ExclusionId"),
		vals.Get("InternetGatewayExclusionMode"),
	)
	if err != nil {
		return nil, err
	}

	return &modifyVpcBlockPublicAccessExclusionResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		VpcBlockPublicAccessExclusion: vpcBPAExclusionToItem(
			excl, h.Backend.TagsForResource(excl.ExclusionID),
		),
	}, nil
}

type deleteVpcBlockPublicAccessExclusionResponse struct {
	XMLName                       xml.Name                          `xml:"DeleteVpcBlockPublicAccessExclusionResponse"`
	Xmlns                         string                            `xml:"xmlns,attr"`
	RequestID                     string                            `xml:"requestId"`
	VpcBlockPublicAccessExclusion vpcBlockPublicAccessExclusionItem `xml:"vpcBlockPublicAccessExclusion"`
}

func (h *Handler) handleDeleteVpcBlockPublicAccessExclusion(vals url.Values, reqID string) (any, error) {
	id := vals.Get("ExclusionId")
	tags := h.Backend.TagsForResource(id)

	excl, err := h.Backend.DeleteVpcBlockPublicAccessExclusion(id)
	if err != nil {
		return nil, err
	}

	return &deleteVpcBlockPublicAccessExclusionResponse{
		Xmlns:                         ec2XMLNS,
		RequestID:                     reqID,
		VpcBlockPublicAccessExclusion: vpcBPAExclusionToItem(excl, tags),
	}, nil
}

type describeVpcBlockPublicAccessExclusionsResponse struct {
	XMLName    xml.Name                            `xml:"DescribeVpcBlockPublicAccessExclusionsResponse"`
	Xmlns      string                              `xml:"xmlns,attr"`
	RequestID  string                              `xml:"requestId"`
	NextToken  string                              `xml:"nextToken,omitempty"`
	Exclusions []vpcBlockPublicAccessExclusionItem `xml:"vpcBlockPublicAccessExclusionSet>item"`
}

func (h *Handler) handleDescribeVpcBlockPublicAccessExclusions(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "ExclusionId")
	excls := h.Backend.DescribeVpcBlockPublicAccessExclusions(ids)

	resp := &describeVpcBlockPublicAccessExclusionsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, excl := range excls {
		resp.Exclusions = append(
			resp.Exclusions, vpcBPAExclusionToItem(excl, h.Backend.TagsForResource(excl.ExclusionID)),
		)
	}

	return resp, nil
}
