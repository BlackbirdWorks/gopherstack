package ec2

import (
	"encoding/xml"
	"net/url"
	"time"
)

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
		Name:      image.Name,
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
	template, err := h.Backend.CreateLaunchTemplate(
		vals.Get("LaunchTemplateName"),
		dataImageID,
		dataInstanceType,
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
		},
	}, nil
}

func (h *Handler) handleCreateVpcEndpoint(vals url.Values, reqID string) (any, error) {
	subnetIDs := parseMemberList(vals, "SubnetId")
	endpoint, err := h.Backend.CreateVpcEndpoint(
		vals.Get("VpcId"),
		vals.Get("ServiceName"),
		vals.Get("VpcEndpointType"),
		subnetIDs,
	)
	if err != nil {
		return nil, err
	}

	return &createVpcEndpointResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Endpoint: vpcEndpointItem{
			ID:              endpoint.ID,
			VPCID:           endpoint.VPCID,
			ServiceName:     endpoint.ServiceName,
			State:           endpoint.State,
			VpcEndpointType: endpoint.VpcEndpointType,
			CreateTime:      endpoint.CreateTime.Format(time.RFC3339),
		},
	}, nil
}

func (h *Handler) handleDescribeVpcEndpoints(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "VpcEndpointId")
	endpoints := h.Backend.DescribeVpcEndpoints(ids)
	items := make([]vpcEndpointItem, 0, len(endpoints))
	for _, endpoint := range endpoints {
		items = append(items, vpcEndpointItem{
			ID:              endpoint.ID,
			VPCID:           endpoint.VPCID,
			ServiceName:     endpoint.ServiceName,
			State:           endpoint.State,
			VpcEndpointType: endpoint.VpcEndpointType,
			CreateTime:      endpoint.CreateTime.Format(time.RFC3339),
		})
	}

	return &describeVpcEndpointsResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Endpoints: vpcEndpointSet{Items: items},
	}, nil
}

func (h *Handler) handleDescribeNetworkAcls(vals url.Values, reqID string) (any, error) {
	vpcIDs := parseMemberList(vals, "Filter.1.Value")
	acls := h.Backend.DescribeNetworkAcls(vpcIDs)
	items := make([]networkACLItem, 0, len(acls))
	for _, acl := range acls {
		items = append(items, networkACLItem{
			ID:        acl.ID,
			VPCID:     acl.VPCID,
			IsDefault: acl.IsDefault,
		})
	}

	return &describeNetworkAclsResponse{
		Xmlns:     ec2XMLNS,
		RequestID: reqID,
		Acls:      networkACLSet{Items: items},
	}, nil
}

type createImageResponse struct {
	XMLName   xml.Name `xml:"CreateImageResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	ImageID   string   `xml:"imageId"`
	Name      string   `xml:"name"`
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

type vpcEndpointItem struct {
	ID              string `xml:"vpcEndpointId"`
	VPCID           string `xml:"vpcId"`
	ServiceName     string `xml:"serviceName"`
	State           string `xml:"vpcEndpointState"`
	VpcEndpointType string `xml:"vpcEndpointType"`
	CreateTime      string `xml:"creationTimestamp"`
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

type networkACLItem struct {
	ID        string `xml:"networkAclId"`
	VPCID     string `xml:"vpcId"`
	IsDefault bool   `xml:"default"`
}

type networkACLSet struct {
	Items []networkACLItem `xml:"item"`
}

type describeNetworkAclsResponse struct {
	XMLName   xml.Name      `xml:"DescribeNetworkAclsResponse"`
	Xmlns     string        `xml:"xmlns,attr"`
	RequestID string        `xml:"requestId"`
	Acls      networkACLSet `xml:"networkAclSet"`
}
