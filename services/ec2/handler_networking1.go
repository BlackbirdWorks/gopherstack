package ec2

import (
	"encoding/xml"
	"net/url"
	"strconv"
	"time"
)

// ---- Handler registration ----

func registerNetworking1Ops(h *Handler, ops map[string]ec2ActionFn) {
	// Transit Gateway VPC Attachments
	ops["CreateTransitGatewayVpcAttachment"] = h.handleCreateTransitGatewayVpcAttachment
	ops["DescribeTransitGatewayVpcAttachments"] = h.handleDescribeTransitGatewayVpcAttachments
	ops["DeleteTransitGatewayVpcAttachment"] = h.handleDeleteTransitGatewayVpcAttachment

	// Flow Logs
	ops["CreateFlowLogs"] = h.handleCreateFlowLogs
	ops["DescribeFlowLogs"] = h.handleDescribeFlowLogs
	ops["DeleteFlowLogs"] = h.handleDeleteFlowLogs

	// DHCP Options
	ops["CreateDhcpOptions"] = h.handleCreateDhcpOptions
	ops["DescribeDhcpOptions"] = h.handleDescribeDhcpOptions
	ops["AssociateDhcpOptions"] = h.handleAssociateDhcpOptions
	ops["DeleteDhcpOptions"] = h.handleDeleteDhcpOptions

	// Launch Template extras
	ops["ModifyLaunchTemplate"] = h.handleModifyLaunchTemplate
	ops["CreateLaunchTemplateVersion"] = h.handleCreateLaunchTemplateVersion
	ops["DeleteLaunchTemplateVersions"] = h.handleDeleteLaunchTemplateVersions
	ops["GetLaunchTemplateData"] = h.handleGetLaunchTemplateData
}

func networking1SupportedOperations() []string {
	return []string{
		"CreateTransitGatewayVpcAttachment",
		"DescribeTransitGatewayVpcAttachments",
		"DeleteTransitGatewayVpcAttachment",
		"CreateFlowLogs",
		"DescribeFlowLogs",
		"DeleteFlowLogs",
		"CreateDhcpOptions",
		"DescribeDhcpOptions",
		"AssociateDhcpOptions",
		"DeleteDhcpOptions",
		"ModifyLaunchTemplate",
		"CreateLaunchTemplateVersion",
		"DeleteLaunchTemplateVersions",
		"GetLaunchTemplateData",
	}
}

// ---- XML response types ----

type createTransitGatewayVpcAttachmentResponse struct {
	XMLName    xml.Name             `xml:"CreateTransitGatewayVpcAttachmentResponse"`
	RequestID  string               `xml:"requestId"`
	Attachment tgwVpcAttachmentItem `xml:"transitGatewayVpcAttachment"`
}

type describeTransitGatewayVpcAttachmentsResponse struct {
	XMLName       xml.Name `xml:"DescribeTransitGatewayVpcAttachmentsResponse"`
	RequestID     string   `xml:"requestId"`
	AttachmentSet struct {
		Items []tgwVpcAttachmentItem `xml:"item"`
	} `xml:"transitGatewayVpcAttachments"`
}

type deleteTransitGatewayVpcAttachmentResponse struct {
	XMLName    xml.Name             `xml:"DeleteTransitGatewayVpcAttachmentResponse"`
	RequestID  string               `xml:"requestId"`
	Attachment tgwVpcAttachmentItem `xml:"transitGatewayVpcAttachment"`
}

type flowLogItem struct {
	FlowLogID          string          `xml:"flowLogId"`
	ResourceID         string          `xml:"resourceId"`
	TrafficType        string          `xml:"trafficType"`
	LogDestinationType string          `xml:"logDestinationType"`
	LogDestination     string          `xml:"logDestination"`
	FlowLogStatus      string          `xml:"flowLogStatus"`
	CreationTime       string          `xml:"creationTime"`
	TagSet             []simpleTagItem `xml:"tagSet>item"`
}

type createFlowLogsResponse struct {
	XMLName    xml.Name `xml:"CreateFlowLogsResponse"`
	RequestID  string   `xml:"requestId"`
	FlowLogIDs struct {
		Items []string `xml:"item"`
	} `xml:"flowLogIdSet"`
	Unsuccessful []unsuccessfulItemXML `xml:"unsuccessful>item"`
}

type describeFlowLogsResponse struct {
	XMLName    xml.Name `xml:"DescribeFlowLogsResponse"`
	RequestID  string   `xml:"requestId"`
	FlowLogSet struct {
		Items []flowLogItem `xml:"item"`
	} `xml:"flowLogSet"`
}

type deleteFlowLogsResponse struct {
	XMLName      xml.Name              `xml:"DeleteFlowLogsResponse"`
	RequestID    string                `xml:"requestId"`
	Unsuccessful []unsuccessfulItemXML `xml:"unsuccessful>item"`
}

type dhcpConfigurationItem struct {
	Key    string `xml:"key"`
	Values struct {
		Items []dhcpConfigValueItem `xml:"item"`
	} `xml:"valueSet"`
}

type dhcpConfigValueItem struct {
	Value string `xml:"value"`
}

type dhcpOptionsItem struct {
	DhcpOptionsID  string                  `xml:"dhcpOptionsId"`
	Configurations []dhcpConfigurationItem `xml:"dhcpConfigurationSet>item"`
}

type createDhcpOptionsResponse struct {
	XMLName     xml.Name        `xml:"CreateDhcpOptionsResponse"`
	RequestID   string          `xml:"requestId"`
	DhcpOptions dhcpOptionsItem `xml:"dhcpOptions"`
}

type describeDhcpOptionsResponse struct {
	XMLName       xml.Name `xml:"DescribeDhcpOptionsResponse"`
	RequestID     string   `xml:"requestId"`
	DhcpOptionSet struct {
		Items []dhcpOptionsItem `xml:"item"`
	} `xml:"dhcpOptionsSet"`
}

type associateDhcpOptionsResponse struct {
	XMLName   xml.Name `xml:"AssociateDhcpOptionsResponse"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type deleteDhcpOptionsResponse struct {
	XMLName   xml.Name `xml:"DeleteDhcpOptionsResponse"`
	RequestID string   `xml:"requestId"`
	Return    bool     `xml:"return"`
}

type modifyLaunchTemplateResponse struct {
	XMLName        xml.Name           `xml:"ModifyLaunchTemplateResponse"`
	RequestID      string             `xml:"requestId"`
	LaunchTemplate launchTemplateItem `xml:"launchTemplate"`
}

type launchTemplateVersionItem struct {
	LaunchTemplateData struct {
		ImageID      string `xml:"imageId"`
		InstanceType string `xml:"instanceType"`
	} `xml:"launchTemplateData"`
	LaunchTemplateID   string `xml:"launchTemplateId"`
	LaunchTemplateName string `xml:"launchTemplateName"`
	CreatedBy          string `xml:"createdBy"`
	CreateTime         string `xml:"createTime"`
	VersionNumber      int64  `xml:"versionNumber"`
	DefaultVersion     bool   `xml:"defaultVersion"`
}

type createLaunchTemplateVersionResponse struct {
	XMLName               xml.Name                  `xml:"CreateLaunchTemplateVersionResponse"`
	RequestID             string                    `xml:"requestId"`
	LaunchTemplateVersion launchTemplateVersionItem `xml:"launchTemplateVersion"`
}

type deletedLaunchTemplateVersionItem struct {
	LaunchTemplateID   string `xml:"launchTemplateId"`
	LaunchTemplateName string `xml:"launchTemplateName"`
	VersionNumber      int64  `xml:"versionNumber"`
}

type deleteLaunchTemplateVersionsResponse struct {
	XMLName                                   xml.Name `xml:"DeleteLaunchTemplateVersionsResponse"`
	RequestID                                 string   `xml:"requestId"`
	SuccessfullyDeletedLaunchTemplateVersions struct {
		Items []deletedLaunchTemplateVersionItem `xml:"item"`
	} `xml:"successfullyDeletedLaunchTemplateVersionSet"`
	UnsuccessfullyDeletedLaunchTemplateVersions struct {
		Items []struct{} `xml:"item"`
	} `xml:"unsuccessfullyDeletedLaunchTemplateVersionSet"`
}

type getLaunchTemplateDataResponse struct {
	XMLName            xml.Name `xml:"GetLaunchTemplateDataResponse"`
	RequestID          string   `xml:"requestId"`
	LaunchTemplateData struct {
		ImageID      string `xml:"imageId"`
		InstanceType string `xml:"instanceType"`
	} `xml:"launchTemplateData"`
}

// ---- Handler implementations ----

func tgwVpcAttachmentToItem(att *TransitGatewayVpcAttachment, tags map[string]string) tgwVpcAttachmentItem {
	item := tgwVpcAttachmentItem{
		TransitGatewayAttachmentID: att.TransitGatewayAttachmentID,
		TransitGatewayID:           att.TransitGatewayID,
		VpcID:                      att.VpcID,
		State:                      att.State,
		SubnetIDs:                  att.SubnetIDs,
		TagSet:                     tagItemsFromMap(tags),
	}
	if !att.CreationTime.IsZero() {
		item.CreationTime = att.CreationTime.Format(time.RFC3339)
	}

	return item
}

func (h *Handler) handleCreateTransitGatewayVpcAttachment(
	vals url.Values,
	reqID string,
) (any, error) {
	subnetIDs := parseMemberList(vals, "SubnetIds")
	att, err := h.Backend.CreateTransitGatewayVpcAttachment(
		vals.Get("TransitGatewayId"),
		vals.Get("VpcId"),
		subnetIDs,
	)
	if err != nil {
		return nil, err
	}

	return &createTransitGatewayVpcAttachmentResponse{
		RequestID:  reqID,
		Attachment: tgwVpcAttachmentToItem(att, nil),
	}, nil
}

func (h *Handler) handleDescribeTransitGatewayVpcAttachments(
	vals url.Values,
	reqID string,
) (any, error) {
	ids := parseMemberList(vals, "TransitGatewayAttachmentIds")
	atts := h.Backend.DescribeTransitGatewayVpcAttachments(ids)

	resp := &describeTransitGatewayVpcAttachmentsResponse{RequestID: reqID}

	for _, att := range atts {
		resp.AttachmentSet.Items = append(
			resp.AttachmentSet.Items,
			tgwVpcAttachmentToItem(att, h.Backend.TagsForResource(att.TransitGatewayAttachmentID)),
		)
	}

	return resp, nil
}

func (h *Handler) handleDeleteTransitGatewayVpcAttachment(
	vals url.Values,
	reqID string,
) (any, error) {
	id := vals.Get("TransitGatewayAttachmentId")
	if err := h.Backend.DeleteTransitGatewayVpcAttachment(id); err != nil {
		return nil, err
	}

	return &deleteTransitGatewayVpcAttachmentResponse{
		RequestID: reqID,
		Attachment: tgwVpcAttachmentItem{
			TransitGatewayAttachmentID: id,
			State:                      tgwRouteStateDeleted,
		},
	}, nil
}

func flowLogToItem(fl *FlowLog, tags map[string]string) flowLogItem {
	return flowLogItem{
		FlowLogID:          fl.FlowLogID,
		ResourceID:         fl.ResourceID,
		TrafficType:        fl.TrafficType,
		LogDestinationType: fl.LogDestinationType,
		LogDestination:     fl.LogDestination,
		FlowLogStatus:      fl.FlowLogStatus,
		CreationTime:       fl.CreationTime.Format(time.RFC3339),
		TagSet:             tagItemsFromMap(tags),
	}
}

func (h *Handler) handleCreateFlowLogs(vals url.Values, reqID string) (any, error) {
	resourceIDs := parseMemberList(vals, "ResourceId")
	tags := parseTagSpecification(vals, "vpc-flow-log")
	logs, err := h.Backend.CreateFlowLogs(
		resourceIDs,
		vals.Get("TrafficType"),
		vals.Get("LogDestinationType"),
		vals.Get("LogDestination"),
		tags,
	)
	if err != nil {
		return nil, err
	}

	resp := &createFlowLogsResponse{RequestID: reqID}

	for _, fl := range logs {
		resp.FlowLogIDs.Items = append(resp.FlowLogIDs.Items, fl.FlowLogID)
	}

	return resp, nil
}

func (h *Handler) handleDescribeFlowLogs(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "FlowLogId")
	logs := h.Backend.DescribeFlowLogs(ids)

	resp := &describeFlowLogsResponse{RequestID: reqID}

	for _, fl := range logs {
		resp.FlowLogSet.Items = append(
			resp.FlowLogSet.Items,
			flowLogToItem(fl, h.Backend.TagsForResource(fl.FlowLogID)),
		)
	}

	return resp, nil
}

func (h *Handler) handleDeleteFlowLogs(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "FlowLogId")
	if err := h.Backend.DeleteFlowLogs(ids); err != nil {
		return nil, err
	}

	return &deleteFlowLogsResponse{RequestID: reqID}, nil
}

func dhcpOptsToItem(opts *DhcpOptions) dhcpOptionsItem {
	item := dhcpOptionsItem{DhcpOptionsID: opts.DhcpOptionsID}

	for _, cfg := range opts.Configurations {
		cfgItem := dhcpConfigurationItem{Key: cfg.Key}
		for _, v := range cfg.Values {
			cfgItem.Values.Items = append(cfgItem.Values.Items, dhcpConfigValueItem{Value: v})
		}

		item.Configurations = append(item.Configurations, cfgItem)
	}

	return item
}

func parseDhcpConfigurations(vals url.Values) []DhcpConfiguration {
	configs := []DhcpConfiguration{}

	for i := 1; ; i++ {
		key := vals.Get("DhcpConfiguration." + strconv.Itoa(i) + ".Key")
		if key == "" {
			break
		}

		values := parseMemberList(vals, "DhcpConfiguration."+strconv.Itoa(i)+".Value")
		configs = append(configs, DhcpConfiguration{Key: key, Values: values})
	}

	return configs
}

func (h *Handler) handleCreateDhcpOptions(vals url.Values, reqID string) (any, error) {
	configs := parseDhcpConfigurations(vals)
	opts, err := h.Backend.CreateDhcpOptions(configs)
	if err != nil {
		return nil, err
	}

	return &createDhcpOptionsResponse{
		RequestID:   reqID,
		DhcpOptions: dhcpOptsToItem(opts),
	}, nil
}

func (h *Handler) handleDescribeDhcpOptions(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "DhcpOptionsId")
	opts := h.Backend.DescribeDhcpOptions(ids)

	resp := &describeDhcpOptionsResponse{RequestID: reqID}

	for _, o := range opts {
		resp.DhcpOptionSet.Items = append(resp.DhcpOptionSet.Items, dhcpOptsToItem(o))
	}

	return resp, nil
}

func (h *Handler) handleAssociateDhcpOptions(vals url.Values, reqID string) (any, error) {
	if err := h.Backend.AssociateDhcpOptions(
		vals.Get("DhcpOptionsId"),
		vals.Get("VpcId"),
	); err != nil {
		return nil, err
	}

	return &associateDhcpOptionsResponse{RequestID: reqID, Return: true}, nil
}

func (h *Handler) handleDeleteDhcpOptions(vals url.Values, reqID string) (any, error) {
	if err := h.Backend.DeleteDhcpOptions(vals.Get("DhcpOptionsId")); err != nil {
		return nil, err
	}

	return &deleteDhcpOptionsResponse{RequestID: reqID, Return: true}, nil
}

func (h *Handler) handleModifyLaunchTemplate(vals url.Values, reqID string) (any, error) {
	defaultVersion, _ := strconv.ParseInt(vals.Get("SetDefaultVersion.VersionNumber"), 10, 64)
	lt, err := h.Backend.ModifyLaunchTemplate(vals.Get("LaunchTemplateId"), defaultVersion)
	if err != nil {
		return nil, err
	}

	return &modifyLaunchTemplateResponse{
		RequestID: reqID,
		LaunchTemplate: launchTemplateItem{
			ID:                   lt.ID,
			Name:                 lt.Name,
			CreateTime:           lt.CreateTime.Format(time.RFC3339),
			CreatedBy:            lt.CreatedBy,
			DefaultVersionNumber: lt.DefaultVersionNumber,
			LatestVersionNumber:  lt.LatestVersionNumber,
			TagSet:               tagItemsFromMap(h.Backend.TagsForResource(lt.ID)),
		},
	}, nil
}

func (h *Handler) handleCreateLaunchTemplateVersion(vals url.Values, reqID string) (any, error) {
	ltID := vals.Get("LaunchTemplateId")
	if ltID == "" {
		ltID = vals.Get("LaunchTemplateName")
	}

	ver, err := h.Backend.CreateLaunchTemplateVersion(
		ltID,
		vals.Get("LaunchTemplateData.ImageId"),
		vals.Get("LaunchTemplateData.InstanceType"),
	)
	if err != nil {
		return nil, err
	}

	item := launchTemplateVersionItem{
		LaunchTemplateID:   ver.LaunchTemplateID,
		LaunchTemplateName: ver.LaunchTemplateName,
		CreatedBy:          ver.CreatedBy,
		VersionNumber:      ver.VersionNumber,
		DefaultVersion:     ver.DefaultVersion,
		CreateTime:         ver.CreateTime.Format(time.RFC3339),
	}
	item.LaunchTemplateData.ImageID = ver.ImageID
	item.LaunchTemplateData.InstanceType = ver.InstanceType

	return &createLaunchTemplateVersionResponse{
		RequestID:             reqID,
		LaunchTemplateVersion: item,
	}, nil
}

func (h *Handler) handleDeleteLaunchTemplateVersions(vals url.Values, reqID string) (any, error) {
	ltID := vals.Get("LaunchTemplateId")
	versionStrs := parseMemberList(vals, "LaunchTemplateVersion")

	versions := make([]int64, 0, len(versionStrs))

	for _, s := range versionStrs {
		v, err := strconv.ParseInt(s, 10, 64)
		if err == nil {
			versions = append(versions, v)
		}
	}

	deleted, err := h.Backend.DeleteLaunchTemplateVersions(ltID, versions)
	if err != nil {
		return nil, err
	}

	ltName := ""
	if lts, ltErr := h.Backend.DescribeLaunchTemplateVersions(ltID); ltErr == nil && len(lts) > 0 {
		ltName = lts[0].Name
	}

	resp := &deleteLaunchTemplateVersionsResponse{RequestID: reqID}

	for _, v := range deleted {
		resp.SuccessfullyDeletedLaunchTemplateVersions.Items = append(
			resp.SuccessfullyDeletedLaunchTemplateVersions.Items,
			deletedLaunchTemplateVersionItem{LaunchTemplateID: ltID, LaunchTemplateName: ltName, VersionNumber: v},
		)
	}

	return resp, nil
}

func (h *Handler) handleGetLaunchTemplateData(vals url.Values, reqID string) (any, error) {
	lt, err := h.Backend.GetLaunchTemplateData(vals.Get("InstanceId"))
	if err != nil {
		return nil, err
	}

	resp := &getLaunchTemplateDataResponse{RequestID: reqID}
	resp.LaunchTemplateData.ImageID = lt.ImageID
	resp.LaunchTemplateData.InstanceType = lt.InstanceType

	return resp, nil
}
