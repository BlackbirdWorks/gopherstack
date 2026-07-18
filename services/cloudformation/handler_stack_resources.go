package cloudformation

import (
	"encoding/xml"
	"net/url"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

func (h *Handler) dispatchResourceOps(action string, form url.Values, c *echo.Context) (bool, error) {
	switch action {
	case "DescribeStackResource":

		return true, h.handleDescribeStackResource(form, c)
	case "ListStackResources":

		return true, h.handleListStackResources(form, c)
	case "DescribeStackResources":

		return true, h.handleDescribeStackResources(form, c)
	case "ListExports":

		return true, h.handleListExports(form, c)
	case "ListImports":

		return true, h.handleListImports(form, c)
	case "SignalResource":

		return true, h.handleSignalResource(form, c)
	default:

		return false, nil
	}
}

func (h *Handler) handleDescribeStackResource(form url.Values, c *echo.Context) error {
	stackName := form.Get("StackName")
	logicalID := form.Get("LogicalResourceId")

	if stackName == "" || logicalID == "" {
		return h.xmlError(c, "ValidationError", "StackName and LogicalResourceId are required")
	}

	res, err := h.Backend.DescribeStackResource(stackName, logicalID)
	if err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}

	type resourceDetailXML struct {
		StackID            string `xml:"StackId,omitempty"`
		StackName          string `xml:"StackName,omitempty"`
		LogicalResourceID  string `xml:"LogicalResourceId"`
		PhysicalResourceID string `xml:"PhysicalResourceId,omitempty"`
		ResourceType       string `xml:"ResourceType"`
		ResourceStatus     string `xml:"ResourceStatus"`
		LastUpdated        string `xml:"LastUpdatedTimestamp"`
	}
	type descResult struct {
		StackResourceDetail resourceDetailXML `xml:"StackResourceDetail"`
	}
	type response struct {
		XMLName   xml.Name   `xml:"DescribeStackResourceResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
		Result    descResult `xml:"DescribeStackResourceResult"`
	}

	return writeXML(c, response{
		Xmlns: cfnNS,
		Result: descResult{
			StackResourceDetail: resourceDetailXML{
				StackID:            res.StackID,
				StackName:          res.StackName,
				LogicalResourceID:  res.LogicalID,
				PhysicalResourceID: res.PhysicalID,
				ResourceType:       res.Type,
				ResourceStatus:     res.Status,
				LastUpdated:        res.Timestamp.Format("2006-01-02T15:04:05Z"),
			},
		},
		RequestID: uuid.New().String(),
	})
}

func (h *Handler) handleListStackResources(form url.Values, c *echo.Context) error {
	stackName := form.Get("StackName")
	nextToken := form.Get("NextToken")

	if stackName == "" {
		return h.xmlError(c, "ValidationError", "StackName is required")
	}

	p, err := h.Backend.ListStackResources(stackName, nextToken)
	if err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}

	type summaryXML struct {
		LogicalResourceID  string `xml:"LogicalResourceId"`
		PhysicalResourceID string `xml:"PhysicalResourceId,omitempty"`
		ResourceType       string `xml:"ResourceType"`
		ResourceStatus     string `xml:"ResourceStatus"`
		LastUpdated        string `xml:"LastUpdatedTimestamp"`
	}
	members := make([]summaryXML, 0, len(p.Data))
	for _, s := range p.Data {
		members = append(members, summaryXML{
			LogicalResourceID:  s.LogicalResourceID,
			PhysicalResourceID: s.PhysicalResourceID,
			ResourceType:       s.ResourceType,
			ResourceStatus:     s.ResourceStatus,
			LastUpdated:        s.Timestamp.Format("2006-01-02T15:04:05Z"),
		})
	}

	type listResult struct {
		NextToken              string       `xml:"NextToken,omitempty"`
		StackResourceSummaries []summaryXML `xml:"StackResourceSummaries>member"`
	}
	type response struct {
		XMLName   xml.Name   `xml:"ListStackResourcesResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
		Result    listResult `xml:"ListStackResourcesResult"`
	}

	return writeXML(c, response{
		Xmlns:     cfnNS,
		Result:    listResult{StackResourceSummaries: members, NextToken: p.Next},
		RequestID: uuid.New().String(),
	})
}

func (h *Handler) handleDescribeStackResources(form url.Values, c *echo.Context) error {
	stackName := form.Get("StackName")
	if stackName == "" {
		return h.xmlError(c, "ValidationError", "StackName is required")
	}

	resources, err := h.Backend.DescribeStackResources(stackName)
	if err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}

	type resourceXML struct {
		StackID            string `xml:"StackId,omitempty"`
		StackName          string `xml:"StackName,omitempty"`
		LogicalResourceID  string `xml:"LogicalResourceId"`
		PhysicalResourceID string `xml:"PhysicalResourceId,omitempty"`
		ResourceType       string `xml:"ResourceType"`
		ResourceStatus     string `xml:"ResourceStatus"`
		Timestamp          string `xml:"Timestamp"`
	}
	members := make([]resourceXML, 0, len(resources))
	for _, r := range resources {
		members = append(members, resourceXML{
			StackID:            r.StackID,
			StackName:          r.StackName,
			LogicalResourceID:  r.LogicalID,
			PhysicalResourceID: r.PhysicalID,
			ResourceType:       r.Type,
			ResourceStatus:     r.Status,
			Timestamp:          r.Timestamp.Format("2006-01-02T15:04:05Z"),
		})
	}

	type descResult struct {
		StackResources []resourceXML `xml:"StackResources>member"`
	}
	type response struct {
		XMLName   xml.Name   `xml:"DescribeStackResourcesResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
		Result    descResult `xml:"DescribeStackResourcesResult"`
	}

	return writeXML(c, response{
		Xmlns:     cfnNS,
		Result:    descResult{StackResources: members},
		RequestID: uuid.New().String(),
	})
}

func (h *Handler) handleSignalResource(form url.Values, c *echo.Context) error {
	_ = h.Backend.SignalResource(
		form.Get("StackName"),
		form.Get("LogicalResourceId"),
		form.Get("UniqueId"),
		form.Get("Status"),
	)
	type response struct {
		XMLName   xml.Name `xml:"SignalResourceResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cfnNS, RequestID: uuid.New().String()})
}
