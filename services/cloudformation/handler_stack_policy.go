package cloudformation

import (
	"encoding/xml"
	"net/url"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

// dispatchStackPolicyOps handles stack policy CloudFormation operations.
func (h *Handler) dispatchStackPolicyOps(action string, form url.Values, c *echo.Context) (bool, error) {
	switch action {
	case "SetStackPolicy":
		return true, h.handleSetStackPolicy(form, c)
	case "GetStackPolicy":
		return true, h.handleGetStackPolicy(form, c)
	default:
		return false, nil
	}
}

func (h *Handler) handleSetStackPolicy(form url.Values, c *echo.Context) error {
	stackName := form.Get("StackName")
	if stackName == "" {
		return h.xmlError(c, "ValidationError", "StackName is required")
	}

	policy := form.Get("StackPolicyBody")
	if policy == "" {
		return h.xmlError(c, "ValidationError", "StackPolicyBody is required")
	}

	if err := h.Backend.SetStackPolicy(stackName, policy); err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}

	type response struct {
		XMLName   xml.Name `xml:"SetStackPolicyResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cfnNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleGetStackPolicy(form url.Values, c *echo.Context) error {
	stackName := form.Get("StackName")
	if stackName == "" {
		return h.xmlError(c, "ValidationError", "StackName is required")
	}

	policy, err := h.Backend.GetStackPolicy(stackName)
	if err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}

	type policyResult struct {
		StackPolicyBody string `xml:"StackPolicyBody,omitempty"`
	}
	type response struct {
		XMLName   xml.Name     `xml:"GetStackPolicyResponse"`
		Xmlns     string       `xml:"xmlns,attr"`
		Result    policyResult `xml:"GetStackPolicyResult"`
		RequestID string       `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{
		Xmlns:     cfnNS,
		Result:    policyResult{StackPolicyBody: policy},
		RequestID: uuid.New().String(),
	})
}
