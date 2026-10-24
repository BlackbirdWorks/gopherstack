package cloudwatch

import (
	"encoding/xml"
	"net/http"
	"net/url"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

func (h *Handler) handleGetOTelEnrichment(_ url.Values, c *echo.Context) error {
	st := h.Backend.OTelEnrichmentDetails()

	type getOTelEnrichmentResult struct {
		Status string `xml:"Status"`
		xmlOTelEnrichmentFields
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
		Result:    getOTelEnrichmentResult{Status: st.Status, xmlOTelEnrichmentFields: toXMLOTelFields(st)},
	})
}

func (h *Handler) handleStartOTelEnrichment(_ url.Values, c *echo.Context) error {
	if err := h.Backend.StartOTelEnrichment(); err != nil {
		return h.xmlError(c, http.StatusInternalServerError, "InternalFailure", err.Error())
	}

	type response struct {
		XMLName   xml.Name                `xml:"StartOTelEnrichmentResponse"`
		Xmlns     string                  `xml:"xmlns,attr"`
		RequestID string                  `xml:"ResponseMetadata>RequestId"`
		Result    xmlOTelEnrichmentFields `xml:"StartOTelEnrichmentResult"`
	}

	return writeXML(c, response{
		Xmlns: cloudwatchNS, RequestID: uuid.New().String(),
		Result: toXMLOTelFields(h.Backend.OTelEnrichmentDetails()),
	})
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
