package cloudwatch

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

func (h *Handler) handlePutDashboard(form url.Values, c *echo.Context) error {
	name := form.Get("DashboardName")
	if name == "" {
		return h.xmlError(
			c,
			http.StatusBadRequest,
			"InvalidParameterInput",
			"DashboardName is required",
		)
	}
	body := form.Get("DashboardBody")

	if err := h.Backend.PutDashboard(name, body); err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type response struct {
		DashboardValidationMessages struct{} `xml:"PutDashboardResult>DashboardValidationMessages"`
		XMLName                     xml.Name `xml:"PutDashboardResponse"`
		Xmlns                       string   `xml:"xmlns,attr"`
		RequestID                   string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleGetDashboard(form url.Values, c *echo.Context) error {
	name := form.Get("DashboardName")
	if name == "" {
		return h.xmlError(
			c,
			http.StatusBadRequest,
			"InvalidParameterInput",
			"DashboardName is required",
		)
	}

	entry, body, err := h.Backend.GetDashboard(name)
	if err != nil {
		return h.xmlError(c, http.StatusBadRequest, "ResourceNotFoundException", err.Error())
	}

	type result struct {
		DashboardArn  string `xml:"DashboardArn"`
		DashboardBody string `xml:"DashboardBody"`
		DashboardName string `xml:"DashboardName"`
	}
	type response struct {
		XMLName   xml.Name `xml:"GetDashboardResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
		Result    result   `xml:"GetDashboardResult"`
	}

	return writeXML(c, response{
		Xmlns:     cloudwatchNS,
		RequestID: uuid.New().String(),
		Result: result{
			DashboardArn:  entry.DashboardArn,
			DashboardBody: body,
			DashboardName: entry.DashboardName,
		},
	})
}

func (h *Handler) handleListDashboards(form url.Values, c *echo.Context) error {
	prefix := form.Get("DashboardNamePrefix")
	nextToken := form.Get("NextToken")

	p, err := h.Backend.ListDashboards(prefix, nextToken)
	if err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type entryXML struct {
		DashboardArn  string `xml:"DashboardArn"`
		DashboardName string `xml:"DashboardName"`
		LastModified  string `xml:"LastModified"`
		Size          int64  `xml:"Size"`
	}
	members := make([]entryXML, 0, len(p.Data))
	for _, e := range p.Data {
		members = append(members, entryXML{
			DashboardArn:  e.DashboardArn,
			DashboardName: e.DashboardName,
			LastModified:  e.LastModified.UTC().Format(time.RFC3339),
			Size:          e.Size,
		})
	}

	type listResult struct {
		NextToken        string     `xml:"NextToken,omitempty"`
		DashboardEntries []entryXML `xml:"DashboardEntries>member"`
	}
	type response struct {
		XMLName   xml.Name   `xml:"ListDashboardsResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
		Result    listResult `xml:"ListDashboardsResult"`
	}

	return writeXML(c, response{
		Xmlns:     cloudwatchNS,
		RequestID: uuid.New().String(),
		Result:    listResult{DashboardEntries: members, NextToken: p.Next},
	})
}

func (h *Handler) handleDeleteDashboards(form url.Values, c *echo.Context) error {
	names := parseMemberList(form, "DashboardNames.")

	// Clean up tag entries for dashboards being deleted.
	if b, ok := h.Backend.(*InMemoryBackend); ok {
		for _, arn := range b.GetDashboardARNs(names) {
			h.deleteResourceTags(arn)
		}
	}

	if err := h.Backend.DeleteDashboards(names); err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type response struct {
		XMLName   xml.Name `xml:"DeleteDashboardsResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}
