package cloudfront

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

type realtimeLogConfigRequestXML struct {
	XMLName      xml.Name `xml:"RealtimeLogConfig"`
	Name         string   `xml:"Name"`
	Fields       []string `xml:"Fields>Field"`
	SamplingRate int64    `xml:"SamplingRate"`
}

func realtimeLogConfigResponseXML(cfg *RealtimeLogConfig) string {
	var sb strings.Builder
	for _, f := range cfg.Fields {
		sb.WriteString("<Field>")
		sb.WriteString(f)
		sb.WriteString("</Field>")
	}
	fieldsXML := sb.String()

	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<RealtimeLogConfig xmlns="%s">`+
		`<ARN>%s</ARN>`+
		`<Name>%s</Name>`+
		`<SamplingRate>%d</SamplingRate>`+
		`<Fields>%s</Fields>`+
		`</RealtimeLogConfig>`,
		cfNS, cfg.ARN, cfg.Name, cfg.SamplingRate, fieldsXML)
}

func (h *Handler) handleCreateRealtimeLogConfig(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var req realtimeLogConfigRequestXML
	if len(body) > 0 {
		_ = xml.Unmarshal(body, &req)
	}

	if req.Name == "" {
		req.Name = generateID()
	}

	cfg, createErr := h.Backend.CreateRealtimeLogConfig(req.Name, req.SamplingRate, req.Fields)
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	return xmlResp(c, http.StatusCreated, realtimeLogConfigResponseXML(cfg))
}

func (h *Handler) handleGetRealtimeLogConfig(c *echo.Context, arn string) error {
	cfg, err := h.Backend.GetRealtimeLogConfig(arn)
	if err != nil {
		cfg, err = h.Backend.GetRealtimeLogConfigByName(arn)
		if err != nil {
			return h.handleError(c, err)
		}
	}

	return xmlResp(c, http.StatusOK, realtimeLogConfigResponseXML(cfg))
}

//nolint:dupl // list handlers for different CloudFront resource types share XML list structure
func (h *Handler) handleListRealtimeLogConfigs(c *echo.Context) error {
	items := h.Backend.ListRealtimeLogConfigs()

	type rlcItemXML struct {
		XMLName      xml.Name `xml:"member"`
		ARN          string   `xml:"ARN"`
		Name         string   `xml:"Name"`
		SamplingRate int64    `xml:"SamplingRate"`
	}

	type rlcListXML struct {
		XMLName     xml.Name     `xml:"RealtimeLogConfigs"`
		XMLNS       string       `xml:"xmlns,attr"`
		Items       []rlcItemXML `xml:"Items>member"`
		MaxItems    int          `xml:"MaxItems"`
		Quantity    int          `xml:"Quantity"`
		IsTruncated bool         `xml:"IsTruncated"`
	}

	summaries := make([]rlcItemXML, 0, len(items))
	for _, cfg := range items {
		summaries = append(summaries, rlcItemXML{ARN: cfg.ARN, Name: cfg.Name, SamplingRate: cfg.SamplingRate})
	}

	list := rlcListXML{XMLNS: cfNS, MaxItems: maxItems, Quantity: len(summaries), Items: summaries}

	out, xmlErr := xml.Marshal(list)
	if xmlErr != nil {
		return h.handleError(c, xmlErr)
	}

	return xmlResp(c, http.StatusOK, `<?xml version="1.0" encoding="UTF-8"?>`+string(out))
}

func (h *Handler) handleUpdateRealtimeLogConfig(c *echo.Context, arn string) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var req realtimeLogConfigRequestXML
	if len(body) > 0 {
		_ = xml.Unmarshal(body, &req)
	}

	cfg, getErr := h.Backend.GetRealtimeLogConfig(arn)
	if getErr != nil {
		cfg, getErr = h.Backend.GetRealtimeLogConfigByName(arn)
		if getErr != nil {
			return h.handleError(c, getErr)
		}
	}

	updated, updateErr := h.Backend.UpdateRealtimeLogConfig(cfg.ARN, req.SamplingRate, req.Fields)
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}

	return xmlResp(c, http.StatusOK, realtimeLogConfigResponseXML(updated))
}

func (h *Handler) handleDeleteRealtimeLogConfig(c *echo.Context, arn string) error {
	if err := h.Backend.DeleteRealtimeLogConfig(arn); err != nil {
		cfg, getErr := h.Backend.GetRealtimeLogConfigByName(arn)
		if getErr != nil {
			return h.handleError(c, err)
		}

		if delErr := h.Backend.DeleteRealtimeLogConfig(cfg.ARN); delErr != nil {
			return h.handleError(c, delErr)
		}
	}

	return c.NoContent(http.StatusNoContent)
}

// --- Key Value Store handlers ---
