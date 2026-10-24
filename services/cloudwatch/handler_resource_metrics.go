package cloudwatch

import (
	"encoding/xml"
	"fmt"
	"net/url"
	"time"

	"github.com/aws/smithy-go/encoding/cbor"
	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

const keyResourceMetricsConfig = "ResourceMetricsConfiguration"

type xmlMetricSelection struct {
	IncludeMetrics []string `xml:"IncludeMetrics>member"`
}

type xmlResourceMetricsConfig struct {
	ResourceArn      string               `xml:"ResourceArn"`
	CreatedAt        string               `xml:"CreatedAt"`
	UpdatedAt        string               `xml:"UpdatedAt"`
	MetricSelections []xmlMetricSelection `xml:"MetricSelections>member"`
}

type xmlOTelSelector struct {
	Namespace   string   `xml:"Namespace"`
	MetricNames []string `xml:"MetricNames>member"`
}

type xmlOTelEnrichmentFields struct {
	CreatedAt      string            `xml:"CreatedAt,omitempty"`
	UpdatedAt      string            `xml:"UpdatedAt,omitempty"`
	IncludeFilters []xmlOTelSelector `xml:"IncludeFilters>member"`
	ExcludeFilters []xmlOTelSelector `xml:"ExcludeFilters>member"`
}

func xmlTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}

	return t.UTC().Format(time.RFC3339)
}

func toXMLResourceMetrics(c ResourceMetricsConfiguration) xmlResourceMetricsConfig {
	out := xmlResourceMetricsConfig{
		ResourceArn: c.ResourceArn, CreatedAt: xmlTime(c.CreatedAt), UpdatedAt: xmlTime(c.UpdatedAt),
	}
	for _, sel := range c.MetricSelections {
		out.MetricSelections = append(out.MetricSelections, xmlMetricSelection(sel))
	}

	return out
}

func toXMLOTelFields(st OTelEnrichmentState) xmlOTelEnrichmentFields {
	conv := func(in []OTelEnrichmentSelector) []xmlOTelSelector {
		out := make([]xmlOTelSelector, 0, len(in))
		for _, s := range in {
			out = append(out, xmlOTelSelector(s))
		}

		return out
	}

	return xmlOTelEnrichmentFields{
		CreatedAt: xmlTime(st.CreatedAt), UpdatedAt: xmlTime(st.UpdatedAt),
		IncludeFilters: conv(st.IncludeFilters), ExcludeFilters: conv(st.ExcludeFilters),
	}
}

func parseFormMetricSelections(form url.Values) []ResourceMetricSelection {
	var out []ResourceMetricSelection

	for i := 1; form.Has(fmt.Sprintf("MetricSelections.member.%d.IncludeMetrics.member.1", i)); i++ {
		out = append(out, ResourceMetricSelection{
			IncludeMetrics: parseMemberList(form, fmt.Sprintf("MetricSelections.member.%d.IncludeMetrics.", i)),
		})
	}

	return out
}

func parseFormOTelSelectors(form url.Values, listName string) []OTelEnrichmentSelector {
	var out []OTelEnrichmentSelector

	for i := 1; form.Has(fmt.Sprintf("%s.member.%d.Namespace", listName, i)); i++ {
		prefix := fmt.Sprintf("%s.member.%d.", listName, i)
		out = append(out, OTelEnrichmentSelector{
			Namespace:   form.Get(prefix + "Namespace"),
			MetricNames: parseMemberList(form, prefix+"MetricNames."),
		})
	}

	return out
}

func (h *Handler) writeResourceMetricsXML(c *echo.Context, op string, cfg ResourceMetricsConfiguration) error {
	type result struct {
		XMLName xml.Name
		Config  xmlResourceMetricsConfig `xml:"ResourceMetricsConfiguration"`
	}

	type envelope struct {
		XMLName   xml.Name
		Xmlns     string `xml:"xmlns,attr"`
		RequestID string `xml:"ResponseMetadata>RequestId"`
		Result    result
	}

	return writeXML(c, envelope{
		XMLName: xml.Name{Local: op + "Response"}, Xmlns: cloudwatchNS, RequestID: uuid.New().String(),
		Result: result{XMLName: xml.Name{Local: op + "Result"}, Config: toXMLResourceMetrics(cfg)},
	})
}

func (h *Handler) handleCreateResourceMetricsConfiguration(form url.Values, c *echo.Context) error {
	cfg, err := h.Backend.CreateResourceMetricsConfiguration(form.Get("ResourceArn"), parseFormMetricSelections(form))
	if err != nil {
		return h.xmlBackendError(c, err)
	}

	return h.writeResourceMetricsXML(c, opCreateResourceMetricsConfiguration, cfg)
}

func (h *Handler) handleGetResourceMetricsConfiguration(form url.Values, c *echo.Context) error {
	cfg, err := h.Backend.GetResourceMetricsConfiguration(form.Get("ResourceArn"))
	if err != nil {
		return h.xmlBackendError(c, err)
	}

	return h.writeResourceMetricsXML(c, opGetResourceMetricsConfiguration, cfg)
}

func (h *Handler) handleUpdateResourceMetricsConfiguration(form url.Values, c *echo.Context) error {
	cfg, err := h.Backend.UpdateResourceMetricsConfiguration(form.Get("ResourceArn"), parseFormMetricSelections(form))
	if err != nil {
		return h.xmlBackendError(c, err)
	}

	return h.writeResourceMetricsXML(c, opUpdateResourceMetricsConfiguration, cfg)
}

func (h *Handler) handleDeleteResourceMetricsConfiguration(form url.Values, c *echo.Context) error {
	if err := h.Backend.DeleteResourceMetricsConfiguration(form.Get("ResourceArn")); err != nil {
		return h.xmlBackendError(c, err)
	}

	type response struct {
		Result    xmlEmptyResult `xml:"DeleteResourceMetricsConfigurationResult"`
		XMLName   xml.Name       `xml:"DeleteResourceMetricsConfigurationResponse"`
		Xmlns     string         `xml:"xmlns,attr"`
		RequestID string         `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleUpdateOTelEnrichment(form url.Values, c *echo.Context) error {
	st, err := h.Backend.UpdateOTelEnrichment(
		parseFormOTelSelectors(form, "IncludeFilters"), parseFormOTelSelectors(form, "ExcludeFilters"),
	)
	if err != nil {
		return h.xmlBackendError(c, err)
	}

	type response struct {
		XMLName   xml.Name                `xml:"UpdateOTelEnrichmentResponse"`
		Xmlns     string                  `xml:"xmlns,attr"`
		RequestID string                  `xml:"ResponseMetadata>RequestId"`
		Result    xmlOTelEnrichmentFields `xml:"UpdateOTelEnrichmentResult"`
	}

	return writeXML(c, response{Xmlns: cloudwatchNS, RequestID: uuid.New().String(), Result: toXMLOTelFields(st)})
}

func cborMetricSelections(m cbor.Map) []ResourceMetricSelection {
	list, ok := m["MetricSelections"].(cbor.List)
	if !ok {
		return nil
	}

	out := make([]ResourceMetricSelection, 0, len(list))

	for _, item := range list {
		sel, isMap := item.(cbor.Map)
		if !isMap {
			continue
		}

		out = append(out, ResourceMetricSelection{IncludeMetrics: cborStrList(sel, "IncludeMetrics")})
	}

	return out
}

func cborResourceMetrics(cfg ResourceMetricsConfiguration) cbor.Map {
	out := cbor.Map{
		"ResourceArn": cbor.String(cfg.ResourceArn),
		"CreatedAt":   cborFromTime(cfg.CreatedAt),
		"UpdatedAt":   cborFromTime(cfg.UpdatedAt),
	}

	if len(cfg.MetricSelections) > 0 {
		sels := make(cbor.List, 0, len(cfg.MetricSelections))
		for _, sel := range cfg.MetricSelections {
			sels = append(sels, cbor.Map{"IncludeMetrics": cborStringList(sel.IncludeMetrics)})
		}

		out["MetricSelections"] = sels
	}

	return out
}

func cborOTelSelectors(m cbor.Map, key string) []OTelEnrichmentSelector {
	list, ok := m[key].(cbor.List)
	if !ok {
		return nil
	}

	out := make([]OTelEnrichmentSelector, 0, len(list))

	for _, item := range list {
		sel, isMap := item.(cbor.Map)
		if !isMap {
			continue
		}

		out = append(out, OTelEnrichmentSelector{
			Namespace: cborStr(sel, "Namespace"), MetricNames: cborStrList(sel, "MetricNames"),
		})
	}

	return out
}

func cborOTelFields(st OTelEnrichmentState, out cbor.Map) cbor.Map {
	if !st.CreatedAt.IsZero() {
		out["CreatedAt"] = cborFromTime(st.CreatedAt)
	}

	if !st.UpdatedAt.IsZero() {
		out["UpdatedAt"] = cborFromTime(st.UpdatedAt)
	}

	for key, sels := range map[string][]OTelEnrichmentSelector{
		"IncludeFilters": st.IncludeFilters, "ExcludeFilters": st.ExcludeFilters,
	} {
		if len(sels) == 0 {
			continue
		}

		list := make(cbor.List, 0, len(sels))
		for _, s := range sels {
			entry := cbor.Map{"Namespace": cbor.String(s.Namespace)}
			if len(s.MetricNames) > 0 {
				entry["MetricNames"] = cborStringList(s.MetricNames)
			}

			list = append(list, entry)
		}

		out[key] = list
	}

	return out
}

func (h *Handler) cborCreateResourceMetricsConfiguration(input cbor.Map, c *echo.Context) error {
	cfg, err := h.Backend.CreateResourceMetricsConfiguration(cborStr(input, "ResourceArn"), cborMetricSelections(input))
	if err != nil {
		return h.cborBackendError(c, err)
	}

	return writeCBOR(c, cbor.Map{keyResourceMetricsConfig: cborResourceMetrics(cfg)})
}

func (h *Handler) cborGetResourceMetricsConfiguration(input cbor.Map, c *echo.Context) error {
	cfg, err := h.Backend.GetResourceMetricsConfiguration(cborStr(input, "ResourceArn"))
	if err != nil {
		return h.cborBackendError(c, err)
	}

	return writeCBOR(c, cbor.Map{keyResourceMetricsConfig: cborResourceMetrics(cfg)})
}

func (h *Handler) cborUpdateResourceMetricsConfiguration(input cbor.Map, c *echo.Context) error {
	cfg, err := h.Backend.UpdateResourceMetricsConfiguration(cborStr(input, "ResourceArn"), cborMetricSelections(input))
	if err != nil {
		return h.cborBackendError(c, err)
	}

	return writeCBOR(c, cbor.Map{keyResourceMetricsConfig: cborResourceMetrics(cfg)})
}

func (h *Handler) cborDeleteResourceMetricsConfiguration(input cbor.Map, c *echo.Context) error {
	if err := h.Backend.DeleteResourceMetricsConfiguration(cborStr(input, "ResourceArn")); err != nil {
		return h.cborBackendError(c, err)
	}

	return writeCBOR(c, cbor.Map{})
}

func (h *Handler) cborUpdateOTelEnrichment(input cbor.Map, c *echo.Context) error {
	st, err := h.Backend.UpdateOTelEnrichment(
		cborOTelSelectors(input, "IncludeFilters"), cborOTelSelectors(input, "ExcludeFilters"),
	)
	if err != nil {
		return h.cborBackendError(c, err)
	}

	return writeCBOR(c, cborOTelFields(st, cbor.Map{}))
}
