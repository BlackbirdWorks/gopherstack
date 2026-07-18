package cloudformation

import (
	"encoding/xml"
	"net/url"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

// dispatchGeneratedTemplateOps handles generated template CRUD operations.
func (h *Handler) dispatchGeneratedTemplateOps(
	action string,
	form url.Values,
	c *echo.Context,
) (bool, error) {
	switch action {
	case "CreateGeneratedTemplate":
		return true, h.handleCreateGeneratedTemplate(form, c)
	case "UpdateGeneratedTemplate":
		return true, h.handleUpdateGeneratedTemplate(form, c)
	case "DeleteGeneratedTemplate":
		return true, h.handleDeleteGeneratedTemplate(form, c)
	case "DescribeGeneratedTemplate":
		return true, h.handleDescribeGeneratedTemplate(form, c)
	case "GetGeneratedTemplate":
		return true, h.handleGetGeneratedTemplate(form, c)
	case "ListGeneratedTemplates":
		return true, h.handleListGeneratedTemplates(form, c)
	}

	return h.dispatchResourceScanOps(action, form, c)
}

// dispatchResourceScanOps handles resource scan operations.
func (h *Handler) dispatchResourceScanOps(
	action string,
	form url.Values,
	c *echo.Context,
) (bool, error) {
	switch action {
	case "StartResourceScan":
		return true, h.handleStartResourceScan(form, c)
	case "DescribeResourceScan":
		return true, h.handleDescribeResourceScan(form, c)
	case "ListResourceScans":
		return true, h.handleListResourceScans(form, c)
	case "ListResourceScanResources":
		return true, h.handleListResourceScanResources(form, c)
	case "ListResourceScanRelatedResources":
		return true, h.handleListResourceScanRelatedResources(form, c)
	}

	return false, nil
}

func (h *Handler) handleCreateGeneratedTemplate(form url.Values, c *echo.Context) error {
	name := form.Get("GeneratedTemplateName")
	if name == "" {
		return h.xmlError(c, "ValidationError", "GeneratedTemplateName is required")
	}
	gt, err := h.Backend.CreateGeneratedTemplate(name, nil)
	if err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}
	type result struct {
		GeneratedTemplateID string `xml:"GeneratedTemplateId"`
	}
	type response struct {
		XMLName   xml.Name `xml:"CreateGeneratedTemplateResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"CreateGeneratedTemplateResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(
		c,
		response{
			Xmlns:     cfnNS,
			Result:    result{GeneratedTemplateID: gt.GeneratedTemplateID},
			RequestID: uuid.New().String(),
		},
	)
}

func (h *Handler) handleUpdateGeneratedTemplate(form url.Values, c *echo.Context) error {
	id := form.Get("GeneratedTemplateId")
	if err := h.Backend.UpdateGeneratedTemplate(id, form.Get("NewGeneratedTemplateName")); err != nil {
		return h.xmlError(c, "GeneratedTemplateNotFound", err.Error())
	}
	type response struct {
		XMLName   xml.Name `xml:"UpdateGeneratedTemplateResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cfnNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleDeleteGeneratedTemplate(form url.Values, c *echo.Context) error {
	if err := h.Backend.DeleteGeneratedTemplate(form.Get("GeneratedTemplateId")); err != nil {
		return h.xmlError(c, "GeneratedTemplateNotFound", err.Error())
	}
	type response struct {
		XMLName   xml.Name `xml:"DeleteGeneratedTemplateResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cfnNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleDescribeGeneratedTemplate(form url.Values, c *echo.Context) error {
	gt, err := h.Backend.DescribeGeneratedTemplate(form.Get("GeneratedTemplateId"))
	if err != nil {
		return h.xmlError(c, "GeneratedTemplateNotFound", err.Error())
	}
	type result struct {
		GeneratedTemplateID   string `xml:"GeneratedTemplateId"`
		GeneratedTemplateName string `xml:"GeneratedTemplateName"`
		Status                string `xml:"Status"`
	}
	type response struct {
		XMLName   xml.Name `xml:"DescribeGeneratedTemplateResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"DescribeGeneratedTemplateResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cfnNS, Result: result{
		GeneratedTemplateID:   gt.GeneratedTemplateID,
		GeneratedTemplateName: gt.GeneratedTemplateName,
		Status:                gt.Status,
	}, RequestID: uuid.New().String()})
}

func (h *Handler) handleGetGeneratedTemplate(form url.Values, c *echo.Context) error {
	body, err := h.Backend.GetGeneratedTemplate(form.Get("GeneratedTemplateId"))
	if err != nil {
		return h.xmlError(c, "GeneratedTemplateNotFound", err.Error())
	}
	type result struct {
		TemplateBody string `xml:"TemplateBody"`
		Status       string `xml:"Status"`
	}
	type response struct {
		XMLName   xml.Name `xml:"GetGeneratedTemplateResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"GetGeneratedTemplateResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(
		c,
		response{
			Xmlns:     cfnNS,
			Result:    result{TemplateBody: body, Status: "COMPLETE"},
			RequestID: uuid.New().String(),
		},
	)
}

func (h *Handler) handleListGeneratedTemplates(form url.Values, c *echo.Context) error {
	p, _ := h.Backend.ListGeneratedTemplates(form.Get("NextToken"))
	type gtXML struct {
		GeneratedTemplateID   string `xml:"GeneratedTemplateId"`
		GeneratedTemplateName string `xml:"GeneratedTemplateName"`
		Status                string `xml:"Status"`
	}
	members := make([]gtXML, 0, len(p.Data))
	for _, t := range p.Data {
		members = append(
			members,
			gtXML{
				GeneratedTemplateID:   t.GeneratedTemplateID,
				GeneratedTemplateName: t.GeneratedTemplateName,
				Status:                t.Status,
			},
		)
	}
	type result struct {
		NextToken string  `xml:"NextToken,omitempty"`
		Summaries []gtXML `xml:"Summaries>member"`
	}
	type response struct {
		XMLName   xml.Name `xml:"ListGeneratedTemplatesResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
		Result    result   `xml:"ListGeneratedTemplatesResult"`
	}

	return writeXML(
		c,
		response{
			Xmlns:     cfnNS,
			Result:    result{NextToken: p.Next, Summaries: members},
			RequestID: uuid.New().String(),
		},
	)
}

func (h *Handler) handleStartResourceScan(_ url.Values, c *echo.Context) error {
	scanID, err := h.Backend.StartResourceScan()
	if err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}
	type result struct {
		ResourceScanID string `xml:"ResourceScanId"`
	}
	type response struct {
		XMLName   xml.Name `xml:"StartResourceScanResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"StartResourceScanResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(
		c,
		response{
			Xmlns:     cfnNS,
			Result:    result{ResourceScanID: scanID},
			RequestID: uuid.New().String(),
		},
	)
}

func (h *Handler) handleDescribeResourceScan(form url.Values, c *echo.Context) error {
	rs, err := h.Backend.DescribeResourceScan(form.Get("ResourceScanId"))
	if err != nil {
		return h.xmlError(c, "ResourceScanNotFound", err.Error())
	}
	type result struct {
		ResourceScanID      string  `xml:"ResourceScanId"`
		Status              string  `xml:"Status"`
		PercentageCompleted float64 `xml:"PercentageCompleted"`
	}
	type response struct {
		XMLName   xml.Name `xml:"DescribeResourceScanResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
		Result    result   `xml:"DescribeResourceScanResult"`
	}

	return writeXML(c, response{Xmlns: cfnNS, Result: result{
		ResourceScanID:      rs.ResourceScanID,
		Status:              rs.Status,
		PercentageCompleted: rs.PercentageCompleted,
	}, RequestID: uuid.New().String()})
}

func (h *Handler) handleListResourceScans(form url.Values, c *echo.Context) error {
	p, _ := h.Backend.ListResourceScans(form.Get("NextToken"))
	type scanXML struct {
		ResourceScanID string `xml:"ResourceScanId"`
		Status         string `xml:"Status"`
	}
	members := make([]scanXML, 0, len(p.Data))
	for _, s := range p.Data {
		members = append(members, scanXML{ResourceScanID: s.ResourceScanID, Status: s.Status})
	}
	type result struct {
		NextToken             string    `xml:"NextToken,omitempty"`
		ResourceScanSummaries []scanXML `xml:"ResourceScanSummaries>member"`
	}
	type response struct {
		XMLName   xml.Name `xml:"ListResourceScansResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
		Result    result   `xml:"ListResourceScansResult"`
	}

	return writeXML(
		c,
		response{
			Xmlns:     cfnNS,
			Result:    result{NextToken: p.Next, ResourceScanSummaries: members},
			RequestID: uuid.New().String(),
		},
	)
}

func (h *Handler) handleListResourceScanResources(form url.Values, c *echo.Context) error {
	scanned, err := h.Backend.ListResourceScanResources(form.Get("ResourceScanId"), "")
	if err != nil {
		return h.xmlError(c, "ResourceScanNotFound", err.Error())
	}
	type resourceXML = ScannedResource
	members := append([]resourceXML(nil), scanned...)
	type result struct {
		Resources []resourceXML `xml:"Resources>member"`
	}
	type response struct {
		XMLName   xml.Name `xml:"ListResourceScanResourcesResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
		Result    result   `xml:"ListResourceScanResourcesResult"`
	}

	return writeXML(
		c,
		response{
			Xmlns:     cfnNS,
			Result:    result{Resources: members},
			RequestID: uuid.New().String(),
		},
	)
}

func (h *Handler) handleListResourceScanRelatedResources(form url.Values, c *echo.Context) error {
	related, err := h.Backend.ListResourceScanRelatedResources(form.Get("ResourceScanId"), nil)
	if err != nil {
		return h.xmlError(c, "ResourceScanNotFound", err.Error())
	}
	// related is []string (legacy plain identifiers).
	type result struct {
		RelatedResources []string `xml:"RelatedResources>member"`
	}
	type response struct {
		XMLName   xml.Name `xml:"ListResourceScanRelatedResourcesResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
		Result    result   `xml:"ListResourceScanRelatedResourcesResult"`
	}

	return writeXML(
		c,
		response{
			Xmlns:     cfnNS,
			Result:    result{RelatedResources: related},
			RequestID: uuid.New().String(),
		},
	)
}
