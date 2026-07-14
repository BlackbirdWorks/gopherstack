package cloudfront

import (
	"encoding/xml"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleGetResourcePolicy(c *echo.Context) error {
	resourceARN := c.Request().URL.Query().Get("arn")
	policy, err := h.Backend.GetResourcePolicy(resourceARN)
	if err != nil {
		return h.handleError(c, err)
	}

	return xmlResp(c, http.StatusOK, fmt.Sprintf(
		`<?xml version="1.0" encoding="UTF-8"?>`+
			`<ResourcePolicy xmlns="%s"><Policy>%s</Policy></ResourcePolicy>`,
		cfNS, policy,
	))
}

func (h *Handler) handlePutResourcePolicy(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}
	var req struct {
		XMLName     xml.Name `xml:"ResourcePolicy"`
		Policy      string   `xml:"Policy"`
		ResourceARN string   `xml:"ResourceArn"`
	}
	if len(body) > 0 {
		_ = xml.Unmarshal(body, &req)
	}
	resourceARN := req.ResourceARN
	if resourceARN == "" {
		resourceARN = c.Request().URL.Query().Get("arn")
	}
	if putErr := h.Backend.PutResourcePolicy(resourceARN, req.Policy); putErr != nil {
		return h.handleError(c, putErr)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteResourcePolicy(c *echo.Context) error {
	resourceARN := c.Request().URL.Query().Get("arn")
	if err := h.Backend.DeleteResourcePolicy(resourceARN); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// ---------------------------------------------------------------------------
// ConnectionGroup extra handlers (Get/List/Update/Delete)
// ---------------------------------------------------------------------------
