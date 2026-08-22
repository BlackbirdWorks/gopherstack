package cloudwatch

import (
	"encoding/xml"
	"net/http"
	"net/url"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

func (h *Handler) handleGetOTelEnrichment(_ url.Values, c *echo.Context) error {
	status, err := h.Backend.GetOTelEnrichment()
	if err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type getOTelEnrichmentResult struct {
		Status string `xml:"Status"`
	}
	type response struct {
		XMLName   xml.Name                `xml:"GetOTelEnrichmentResponse"`
		Xmlns     string                  `xml:"xmlns,attr"`
		RequestID string                  `xml:"ResponseMetadata>RequestId"`
		Result    getOTelEnrichmentResult `xml:"GetOTelEnrichmentResult"`
	}

	return writeXML(c, response{
		Xmlns:     cloudwatchNS,
		RequestID: uuid.New().String(),
		Result:    getOTelEnrichmentResult{Status: status},
	})
}

func (h *Handler) handleStartOTelEnrichment(_ url.Values, c *echo.Context) error {
	if err := h.Backend.StartOTelEnrichment(); err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type response struct {
		XMLName   xml.Name       `xml:"StartOTelEnrichmentResponse"`
		Result    xmlEmptyResult `xml:"StartOTelEnrichmentResult"`
		Xmlns     string         `xml:"xmlns,attr"`
		RequestID string         `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleStopOTelEnrichment(_ url.Values, c *echo.Context) error {
	if err := h.Backend.StopOTelEnrichment(); err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type response struct {
		XMLName   xml.Name       `xml:"StopOTelEnrichmentResponse"`
		Result    xmlEmptyResult `xml:"StopOTelEnrichmentResult"`
		Xmlns     string         `xml:"xmlns,attr"`
		RequestID string         `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}
