package ec2

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"time"
)

// ---- Capacity Manager ----

type capacityManagerStatusResponse struct {
	XMLName               xml.Name `xml:""`
	Xmlns                 string   `xml:"xmlns,attr"`
	RequestID             string   `xml:"requestId"`
	CapacityManagerStatus string   `xml:"capacityManagerStatus"`
	OrganizationsAccess   bool     `xml:"organizationsAccess"`
}

func (h *Handler) handleEnableCapacityManager(vals url.Values, reqID string) (any, error) {
	orgAccess := vals.Get("OrganizationsAccess") == ec2BooleanTrue
	status, access := h.Backend.EnableCapacityManager(orgAccess)

	return &capacityManagerStatusResponse{
		XMLName:               xml.Name{Local: "EnableCapacityManagerResponse"},
		Xmlns:                 ec2XMLNS,
		RequestID:             reqID,
		CapacityManagerStatus: status,
		OrganizationsAccess:   access,
	}, nil
}

func (h *Handler) handleDisableCapacityManager(_ url.Values, reqID string) (any, error) {
	status, access := h.Backend.DisableCapacityManager()

	return &capacityManagerStatusResponse{
		XMLName:               xml.Name{Local: "DisableCapacityManagerResponse"},
		Xmlns:                 ec2XMLNS,
		RequestID:             reqID,
		CapacityManagerStatus: status,
		OrganizationsAccess:   access,
	}, nil
}

func (h *Handler) handleUpdateCapacityManagerOrganizationsAccess(vals url.Values, reqID string) (any, error) {
	orgAccess := vals.Get("OrganizationsAccess") == ec2BooleanTrue
	status, access := h.Backend.UpdateCapacityManagerOrganizationsAccess(orgAccess)

	return &capacityManagerStatusResponse{
		XMLName:               xml.Name{Local: "UpdateCapacityManagerOrganizationsAccessResponse"},
		Xmlns:                 ec2XMLNS,
		RequestID:             reqID,
		CapacityManagerStatus: status,
		OrganizationsAccess:   access,
	}, nil
}

type getCapacityManagerAttributesResponse struct {
	XMLName                xml.Name `xml:"GetCapacityManagerAttributesResponse"`
	Xmlns                  string   `xml:"xmlns,attr"`
	RequestID              string   `xml:"requestId"`
	CapacityManagerStatus  string   `xml:"capacityManagerStatus"`
	IngestionStatus        string   `xml:"ingestionStatus,omitempty"`
	IngestionStatusMessage string   `xml:"ingestionStatusMessage,omitempty"`
	DataExportCount        int32    `xml:"dataExportCount"`
	OrganizationsAccess    bool     `xml:"organizationsAccess"`
}

func (h *Handler) handleGetCapacityManagerAttributes(_ url.Values, reqID string) (any, error) {
	attrs := h.Backend.GetCapacityManagerAttributes()

	return &getCapacityManagerAttributesResponse{
		Xmlns:                  ec2XMLNS,
		RequestID:              reqID,
		CapacityManagerStatus:  attrs.Status,
		OrganizationsAccess:    attrs.OrganizationsAccess,
		DataExportCount:        attrs.DataExportCount,
		IngestionStatus:        attrs.IngestionStatus,
		IngestionStatusMessage: attrs.IngestionStatusMessage,
	}, nil
}

type getCapacityManagerMetricDataResponse struct {
	XMLName           xml.Name `xml:"GetCapacityManagerMetricDataResponse"`
	Xmlns             string   `xml:"xmlns,attr"`
	RequestID         string   `xml:"requestId"`
	NextToken         string   `xml:"nextToken,omitempty"`
	MetricDataResults struct {
		Items []struct{} `xml:"item"`
	} `xml:"metricDataResultSet"`
}

func (h *Handler) handleGetCapacityManagerMetricData(vals url.Values, reqID string) (any, error) {
	if vals.Get("StartTime") == "" || vals.Get("EndTime") == "" || vals.Get("Period") == "" ||
		vals.Get("MetricName.1") == "" {
		return nil, fmt.Errorf(
			"%w: StartTime, EndTime, Period, and MetricNames are required",
			ErrInvalidParameter,
		)
	}

	_ = h.Backend.GetCapacityManagerMetricData()

	return &getCapacityManagerMetricDataResponse{Xmlns: ec2XMLNS, RequestID: reqID}, nil
}

type getCapacityManagerMetricDimensionsResponse struct {
	XMLName                xml.Name `xml:"GetCapacityManagerMetricDimensionsResponse"`
	Xmlns                  string   `xml:"xmlns,attr"`
	RequestID              string   `xml:"requestId"`
	NextToken              string   `xml:"nextToken,omitempty"`
	MetricDimensionResults struct {
		Items []struct{} `xml:"item"`
	} `xml:"metricDimensionResultSet"`
}

func (h *Handler) handleGetCapacityManagerMetricDimensions(vals url.Values, reqID string) (any, error) {
	if vals.Get("StartTime") == "" || vals.Get("EndTime") == "" ||
		vals.Get("MetricName.1") == "" || vals.Get("GroupBy.1") == "" {
		return nil, fmt.Errorf(
			"%w: StartTime, EndTime, MetricNames, and GroupBy are required",
			ErrInvalidParameter,
		)
	}

	_ = h.Backend.GetCapacityManagerMetricDimensions()

	return &getCapacityManagerMetricDimensionsResponse{Xmlns: ec2XMLNS, RequestID: reqID}, nil
}

type createCapacityManagerDataExportResponse struct {
	XMLName                     xml.Name `xml:"CreateCapacityManagerDataExportResponse"`
	Xmlns                       string   `xml:"xmlns,attr"`
	RequestID                   string   `xml:"requestId"`
	CapacityManagerDataExportID string   `xml:"capacityManagerDataExportId"`
}

func (h *Handler) handleCreateCapacityManagerDataExport(vals url.Values, reqID string) (any, error) {
	tags := parseTagSpecification(vals, "capacity-manager-data-export")

	export, err := h.Backend.CreateCapacityManagerDataExport(
		vals.Get("OutputFormat"),
		vals.Get("S3BucketName"),
		vals.Get("S3BucketPrefix"),
		vals.Get("Schedule"),
		tags,
	)
	if err != nil {
		return nil, err
	}

	return &createCapacityManagerDataExportResponse{
		Xmlns:                       ec2XMLNS,
		RequestID:                   reqID,
		CapacityManagerDataExportID: export.CapacityManagerDataExportID,
	}, nil
}

type capacityManagerDataExportItem struct {
	CapacityManagerDataExportID string          `xml:"capacityManagerDataExportId"`
	OutputFormat                string          `xml:"outputFormat,omitempty"`
	S3BucketName                string          `xml:"s3BucketName,omitempty"`
	S3BucketPrefix              string          `xml:"s3BucketPrefix,omitempty"`
	Schedule                    string          `xml:"schedule,omitempty"`
	LatestDeliveryStatus        string          `xml:"latestDeliveryStatus,omitempty"`
	CreateTime                  string          `xml:"createTime,omitempty"`
	TagSet                      []simpleTagItem `xml:"tagSet>item"`
}

func toCapacityManagerDataExportItem(e *CapacityManagerDataExport) capacityManagerDataExportItem {
	return capacityManagerDataExportItem{
		CapacityManagerDataExportID: e.CapacityManagerDataExportID,
		OutputFormat:                e.OutputFormat,
		S3BucketName:                e.S3BucketName,
		S3BucketPrefix:              e.S3BucketPrefix,
		Schedule:                    e.Schedule,
		LatestDeliveryStatus:        e.LatestDeliveryStatus,
		CreateTime:                  e.CreateTime.Format(time.RFC3339),
		TagSet:                      tagItemsFromMap(e.Tags),
	}
}

type describeCapacityManagerDataExportsResponse struct {
	XMLName   xml.Name `xml:"DescribeCapacityManagerDataExportsResponse"`
	Xmlns     string   `xml:"xmlns,attr"`
	RequestID string   `xml:"requestId"`
	NextToken string   `xml:"nextToken,omitempty"`
	Exports   struct {
		Items []capacityManagerDataExportItem `xml:"item"`
	} `xml:"capacityManagerDataExportSet"`
}

func (h *Handler) handleDescribeCapacityManagerDataExports(vals url.Values, reqID string) (any, error) {
	ids := parseMemberList(vals, "CapacityManagerDataExportId")

	exports := h.Backend.DescribeCapacityManagerDataExports(ids)

	resp := &describeCapacityManagerDataExportsResponse{Xmlns: ec2XMLNS, RequestID: reqID}
	for _, e := range exports {
		resp.Exports.Items = append(resp.Exports.Items, toCapacityManagerDataExportItem(e))
	}

	return resp, nil
}

type deleteCapacityManagerDataExportResponse struct {
	XMLName                     xml.Name `xml:"DeleteCapacityManagerDataExportResponse"`
	Xmlns                       string   `xml:"xmlns,attr"`
	RequestID                   string   `xml:"requestId"`
	CapacityManagerDataExportID string   `xml:"capacityManagerDataExportId"`
}

func (h *Handler) handleDeleteCapacityManagerDataExport(vals url.Values, reqID string) (any, error) {
	id, err := h.Backend.DeleteCapacityManagerDataExport(vals.Get("CapacityManagerDataExportId"))
	if err != nil {
		return nil, err
	}

	return &deleteCapacityManagerDataExportResponse{
		Xmlns:                       ec2XMLNS,
		RequestID:                   reqID,
		CapacityManagerDataExportID: id,
	}, nil
}
