package cloudfront

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

// kinesisStreamConfigXML matches types.KinesisStreamConfig (types/types.go:3969-3989).
type kinesisStreamConfigXML struct {
	RoleARN   string `xml:"RoleARN"`
	StreamARN string `xml:"StreamARN"`
}

// endPointXML matches types.EndPoint (types/types.go:2988-3001). The real wire
// list wraps each entry in <member> (serializers.go:
// awsRestxml_serializeDocumentEndPointList uses array.Member() with no custom name).
type endPointXML struct {
	StreamType          string                 `xml:"StreamType"`
	KinesisStreamConfig kinesisStreamConfigXML `xml:"KinesisStreamConfig"`
}

func toRealtimeLogEndPoints(eps []endPointXML) []RealtimeLogEndPoint {
	out := make([]RealtimeLogEndPoint, 0, len(eps))
	for _, ep := range eps {
		out = append(out, RealtimeLogEndPoint{
			StreamType: ep.StreamType,
			RoleARN:    ep.KinesisStreamConfig.RoleARN,
			StreamARN:  ep.KinesisStreamConfig.StreamARN,
		})
	}

	return out
}

// createRealtimeLogConfigRequestXML matches CreateRealtimeLogConfigInput
// (api_op_CreateRealtimeLogConfig.go:37-67); root element CreateRealtimeLogConfigRequest.
type createRealtimeLogConfigRequestXML struct {
	XMLName      xml.Name      `xml:"CreateRealtimeLogConfigRequest"`
	Name         string        `xml:"Name"`
	Fields       []string      `xml:"Fields>Field"`
	EndPoints    []endPointXML `xml:"EndPoints>member"`
	SamplingRate int64         `xml:"SamplingRate"`
}

// updateRealtimeLogConfigRequestXML matches UpdateRealtimeLogConfigInput
// (api_op_UpdateRealtimeLogConfig.go:43-67); root element UpdateRealtimeLogConfigRequest.
// ARN/Name identify which config to update but, per the op's doc comment, cannot
// themselves be changed by the update.
type updateRealtimeLogConfigRequestXML struct {
	XMLName      xml.Name      `xml:"UpdateRealtimeLogConfigRequest"`
	ARN          string        `xml:"ARN"`
	Name         string        `xml:"Name"`
	Fields       []string      `xml:"Fields>Field"`
	EndPoints    []endPointXML `xml:"EndPoints>member"`
	SamplingRate int64         `xml:"SamplingRate"`
}

// getRealtimeLogConfigRequestXML matches GetRealtimeLogConfigInput
// (api_op_GetRealtimeLogConfig.go:33-42): a POST to /2020-05-31/get-realtime-log-config
// carrying ARN or Name in the body, not a path segment or query string.
type getRealtimeLogConfigRequestXML struct {
	XMLName xml.Name `xml:"GetRealtimeLogConfigRequest"`
	ARN     string   `xml:"ARN"`
	Name    string   `xml:"Name"`
}

// deleteRealtimeLogConfigRequestXML matches DeleteRealtimeLogConfigInput
// (api_op_DeleteRealtimeLogConfig.go:37-45): same POST-to-body-ARN shape as Get.
type deleteRealtimeLogConfigRequestXML struct {
	XMLName xml.Name `xml:"DeleteRealtimeLogConfigRequest"`
	ARN     string   `xml:"ARN"`
	Name    string   `xml:"Name"`
}

// realtimeLogConfigResponseXML matches Create/Get/UpdateRealtimeLogConfigOutput
// (api_op_{Create,Get,Update}RealtimeLogConfig.go): unlike VpcOrigin/Distribution,
// RealtimeLogConfig is not httpPayload-bound, so the deserializer looks for a
// <RealtimeLogConfig> child of the response root rather than reading fields off
// the root itself (deserializers.go: awsRestxml_deserializeOpDocumentCreate
// RealtimeLogConfigOutput matches on a "RealtimeLogConfig" child element).
func realtimeLogConfigResponseXML(cfg *RealtimeLogConfig) string {
	var fields strings.Builder
	for _, f := range cfg.Fields {
		fields.WriteString("<Field>")
		fields.WriteString(xmlEscape(f))
		fields.WriteString("</Field>")
	}

	var endpoints strings.Builder
	for _, ep := range cfg.EndPoints {
		endpoints.WriteString("<member><StreamType>")
		endpoints.WriteString(xmlEscape(ep.StreamType))
		endpoints.WriteString("</StreamType><KinesisStreamConfig><RoleARN>")
		endpoints.WriteString(xmlEscape(ep.RoleARN))
		endpoints.WriteString("</RoleARN><StreamARN>")
		endpoints.WriteString(xmlEscape(ep.StreamARN))
		endpoints.WriteString("</StreamARN></KinesisStreamConfig></member>")
	}

	return fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>`+
		`<RealtimeLogConfigResult xmlns="%s">`+
		`<RealtimeLogConfig>`+
		`<ARN>%s</ARN>`+
		`<Name>%s</Name>`+
		`<SamplingRate>%d</SamplingRate>`+
		`<Fields>%s</Fields>`+
		`<EndPoints>%s</EndPoints>`+
		`</RealtimeLogConfig>`+
		`</RealtimeLogConfigResult>`,
		cfNS, xmlEscape(cfg.ARN), xmlEscape(cfg.Name), cfg.SamplingRate, fields.String(), endpoints.String())
}

func (h *Handler) handleCreateRealtimeLogConfig(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var req createRealtimeLogConfigRequestXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid CreateRealtimeLogConfigRequest XML"),
			)
		}
	}

	if req.Name == "" {
		req.Name = generateID()
	}

	cfg, createErr := h.Backend.CreateRealtimeLogConfig(
		req.Name, req.SamplingRate, req.Fields, toRealtimeLogEndPoints(req.EndPoints),
	)
	if createErr != nil {
		return h.handleError(c, createErr)
	}

	return xmlResp(c, http.StatusCreated, realtimeLogConfigResponseXML(cfg))
}

// resolveRealtimeLogConfig looks a config up by ARN or Name. Per the Get/Delete
// doc comments, a real client provides at least one; if both are given, CloudFront
// uses Name to identify the config.
func (h *Handler) resolveRealtimeLogConfig(arnVal, name string) (*RealtimeLogConfig, error) {
	if name != "" {
		return h.Backend.GetRealtimeLogConfigByName(name)
	}

	return h.Backend.GetRealtimeLogConfig(arnVal)
}

func (h *Handler) handleGetRealtimeLogConfig(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req getRealtimeLogConfigRequestXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid GetRealtimeLogConfigRequest XML"),
			)
		}
	}

	cfg, getErr := h.resolveRealtimeLogConfig(req.ARN, req.Name)
	if getErr != nil {
		return h.handleError(c, getErr)
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

func (h *Handler) handleUpdateRealtimeLogConfig(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	if qErr := validateQuantities(body); qErr != nil {
		return h.handleError(c, qErr)
	}

	var req updateRealtimeLogConfigRequestXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid UpdateRealtimeLogConfigRequest XML"),
			)
		}
	}

	cfg, getErr := h.resolveRealtimeLogConfig(req.ARN, req.Name)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	updated, updateErr := h.Backend.UpdateRealtimeLogConfig(
		cfg.ARN, req.SamplingRate, req.Fields, toRealtimeLogEndPoints(req.EndPoints),
	)
	if updateErr != nil {
		return h.handleError(c, updateErr)
	}

	return xmlResp(c, http.StatusOK, realtimeLogConfigResponseXML(updated))
}

func (h *Handler) handleDeleteRealtimeLogConfig(c *echo.Context) error {
	body, err := readBody(c)
	if err != nil {
		return xmlResp(c, http.StatusBadRequest, cfErrorXML("MalformedXML", "failed to read body"))
	}

	var req deleteRealtimeLogConfigRequestXML
	if len(body) > 0 {
		if xmlErr := xml.Unmarshal(body, &req); xmlErr != nil {
			return xmlResp(
				c,
				http.StatusBadRequest,
				cfErrorXML("MalformedXML", "invalid DeleteRealtimeLogConfigRequest XML"),
			)
		}
	}

	cfg, getErr := h.resolveRealtimeLogConfig(req.ARN, req.Name)
	if getErr != nil {
		return h.handleError(c, getErr)
	}

	if delErr := h.Backend.DeleteRealtimeLogConfig(cfg.ARN); delErr != nil {
		return h.handleError(c, delErr)
	}

	return c.NoContent(http.StatusNoContent)
}

// --- Key Value Store handlers ---
