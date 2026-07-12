package s3control

import (
	"encoding/xml"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

// ---- Per-AccessPoint PublicAccessBlock ----

type apPABConfigXML struct {
	XMLName               xml.Name `xml:"PublicAccessBlockConfiguration"`
	BlockPublicAcls       bool     `xml:"BlockPublicAcls"`
	IgnorePublicAcls      bool     `xml:"IgnorePublicAcls"`
	BlockPublicPolicy     bool     `xml:"BlockPublicPolicy"`
	RestrictPublicBuckets bool     `xml:"RestrictPublicBuckets"`
}

type putAPPABRequestXML struct {
	XMLName                        xml.Name       `xml:"PutAccessPointPublicAccessBlockRequest"`
	PublicAccessBlockConfiguration apPABConfigXML `xml:"PublicAccessBlockConfiguration"`
}

type getAPPABResponseXML struct {
	XMLName                        xml.Name       `xml:"GetAccessPointPublicAccessBlockResult"`
	PublicAccessBlockConfiguration apPABConfigXML `xml:"PublicAccessBlockConfiguration"`
}

func apNameFromPath(path string) string {
	// /v20180820/accesspoint/{name}/publicAccessBlock → name
	trimmed := strings.TrimPrefix(path, pathAccessPointPrefix)

	return strings.TrimSuffix(trimmed, "/publicAccessBlock")
}

func (h *Handler) handleGetAccessPointPublicAccessBlock(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := apNameFromPath(c.Request().URL.Path)

	pab, err := h.Backend.GetAccessPointPublicAccessBlock(accountID, name)
	if err != nil {
		return handleBackendError(c, err)
	}

	return writeXML(c, getAPPABResponseXML{
		PublicAccessBlockConfiguration: apPABConfigXML{
			BlockPublicAcls:       pab.BlockPublicAcls,
			IgnorePublicAcls:      pab.IgnorePublicAcls,
			BlockPublicPolicy:     pab.BlockPublicPolicy,
			RestrictPublicBuckets: pab.RestrictPublicBuckets,
		},
	})
}

func (h *Handler) handlePutAccessPointPublicAccessBlock(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := apNameFromPath(c.Request().URL.Path)

	var body putAPPABRequestXML
	if err := decodeXML(c, &body); err != nil {
		return writeXMLErrorCode(c, http.StatusBadRequest, "MalformedXML", "invalid request body")
	}

	cfg := PublicAccessBlock{
		AccountID:             accountID,
		BlockPublicAcls:       body.PublicAccessBlockConfiguration.BlockPublicAcls,
		IgnorePublicAcls:      body.PublicAccessBlockConfiguration.IgnorePublicAcls,
		BlockPublicPolicy:     body.PublicAccessBlockConfiguration.BlockPublicPolicy,
		RestrictPublicBuckets: body.PublicAccessBlockConfiguration.RestrictPublicBuckets,
	}

	if err := h.Backend.PutAccessPointPublicAccessBlock(accountID, name, cfg); err != nil {
		return handleBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleDeleteAccessPointPublicAccessBlock(c *echo.Context) error {
	accountID := accountIDFromRequest(c)
	name := apNameFromPath(c.Request().URL.Path)

	if err := h.Backend.DeleteAccessPointPublicAccessBlock(accountID, name); err != nil {
		return handleBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}
