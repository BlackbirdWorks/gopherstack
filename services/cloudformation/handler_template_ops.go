package cloudformation

import (
	"encoding/xml"
	"net/url"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

func (h *Handler) handleGetTemplate(form url.Values, c *echo.Context) error {
	stackName := form.Get("StackName")
	if stackName == "" {
		return h.xmlError(c, "ValidationError", "StackName is required")
	}

	body, err := h.Backend.GetTemplate(stackName)
	if err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}

	type result struct {
		TemplateBody string `xml:"TemplateBody"`
	}
	type response struct {
		XMLName   xml.Name `xml:"GetTemplateResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"GetTemplateResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{
		Xmlns:     cfnNS,
		Result:    result{TemplateBody: body},
		RequestID: uuid.New().String(),
	})
}

// dispatchTemplateOps handles template inspection and validation operations.
func (h *Handler) dispatchTemplateOps(action string, form url.Values, c *echo.Context) (bool, error) {
	switch action {
	case "GetTemplateSummary":
		return true, h.handleGetTemplateSummary(form, c)
	case "EstimateTemplateCost":
		return true, h.handleEstimateTemplateCost(form, c)
	case "ValidateTemplate":
		return true, h.handleValidateTemplate(form, c)
	default:
		return false, nil
	}
}

func (h *Handler) handleGetTemplateSummary(form url.Values, c *echo.Context) error {
	templateBody := form.Get("TemplateBody")
	stackName := form.Get("StackName")

	summary, err := h.Backend.GetTemplateSummary(templateBody, stackName)
	if err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}

	type paramXML struct {
		ParameterKey          string   `xml:"ParameterKey"`
		ParameterType         string   `xml:"ParameterType,omitempty"`
		DefaultValue          string   `xml:"DefaultValue,omitempty"`
		Description           string   `xml:"Description,omitempty"`
		ConstraintDescription string   `xml:"ConstraintDescription,omitempty"`
		AllowedPattern        string   `xml:"AllowedPattern,omitempty"`
		AllowedValues         []string `xml:"AllowedValues>member,omitempty"`
		NoEcho                bool     `xml:"NoEcho,omitempty"`
	}

	params := make([]paramXML, 0, len(summary.Parameters))
	for _, p := range summary.Parameters {
		params = append(params, paramXML(p))
	}

	type summaryResult struct {
		Description   string     `xml:"Description,omitempty"`
		Parameters    []paramXML `xml:"Parameters>member,omitempty"`
		ResourceTypes []string   `xml:"ResourceTypes>member,omitempty"`
	}
	type response struct {
		XMLName   xml.Name      `xml:"GetTemplateSummaryResponse"`
		Xmlns     string        `xml:"xmlns,attr"`
		RequestID string        `xml:"ResponseMetadata>RequestId"`
		Result    summaryResult `xml:"GetTemplateSummaryResult"`
	}

	return writeXML(c, response{
		Xmlns: cfnNS,
		Result: summaryResult{
			Description:   summary.Description,
			Parameters:    params,
			ResourceTypes: summary.ResourceTypes,
		},
		RequestID: uuid.New().String(),
	})
}

func (h *Handler) handleEstimateTemplateCost(form url.Values, c *echo.Context) error {
	templateBody := form.Get("TemplateBody")
	params := parseParams(form)

	costURL, err := h.Backend.EstimateTemplateCost(templateBody, params)
	if err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}

	type costResult struct {
		URL string `xml:"Url"`
	}
	type response struct {
		XMLName   xml.Name   `xml:"EstimateTemplateCostResponse"`
		Xmlns     string     `xml:"xmlns,attr"`
		Result    costResult `xml:"EstimateTemplateCostResult"`
		RequestID string     `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{
		Xmlns:     cfnNS,
		Result:    costResult{URL: costURL},
		RequestID: uuid.New().String(),
	})
}

func (h *Handler) handleValidateTemplate(form url.Values, c *echo.Context) error {
	templateBody := form.Get("TemplateBody")
	summary, err := h.Backend.ValidateTemplate(templateBody)
	if err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}
	type paramXML struct {
		ParameterKey string `xml:"ParameterKey"`
	}
	params := make([]paramXML, 0, len(summary.Parameters))
	for _, p := range summary.Parameters {
		params = append(params, paramXML{ParameterKey: p.ParameterKey})
	}
	type result struct {
		Description        string     `xml:"Description,omitempty"`
		CapabilitiesReason string     `xml:"CapabilitiesReason,omitempty"`
		Parameters         []paramXML `xml:"Parameters>member,omitempty"`
		Capabilities       []string   `xml:"Capabilities>member,omitempty"`
		DeclaredTransforms []string   `xml:"DeclaredTransforms>member,omitempty"`
	}
	type response struct {
		XMLName   xml.Name `xml:"ValidateTemplateResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
		Result    result   `xml:"ValidateTemplateResult"`
	}

	return writeXML(c, response{
		Xmlns: cfnNS,
		Result: result{
			Description:        summary.Description,
			Parameters:         params,
			Capabilities:       summary.Capabilities,
			CapabilitiesReason: summary.CapabilitiesReason,
			DeclaredTransforms: summary.DeclaredTransforms,
		},
		RequestID: uuid.New().String(),
	})
}
