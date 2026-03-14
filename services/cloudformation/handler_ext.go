package cloudformation

import (
	"encoding/xml"
	"net/url"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

// dispatchExtOps handles the new extended CloudFormation operations.
func (h *Handler) dispatchExtOps(action string, form url.Values, c *echo.Context) (bool, error) {
	switch action {
	case "DetectStackDrift":
		return true, h.handleDetectStackDrift(form, c)
	case "DetectStackResourceDrift":
		return true, h.handleDetectStackResourceDrift(form, c)
	case "DescribeStackDriftDetectionStatus":
		return true, h.handleDescribeStackDriftDetectionStatus(form, c)
	case "DescribeStackResourceDrifts":
		return true, h.handleDescribeStackResourceDrifts(form, c)
	case "SetStackPolicy":
		return true, h.handleSetStackPolicy(form, c)
	case "GetStackPolicy":
		return true, h.handleGetStackPolicy(form, c)
	case "GetTemplateSummary":
		return true, h.handleGetTemplateSummary(form, c)
	case "EstimateTemplateCost":
		return true, h.handleEstimateTemplateCost(form, c)
	case "ContinueUpdateRollback":
		return true, h.handleContinueUpdateRollback(form, c)
	case "CancelUpdateStack":
		return true, h.handleCancelUpdateStack(form, c)
	case "DescribeAccountLimits":
		return true, h.handleDescribeAccountLimits(form, c)
	default:
		return false, nil
	}
}

func (h *Handler) handleDetectStackDrift(form url.Values, c *echo.Context) error {
	stackName := form.Get("StackName")
	if stackName == "" {
		return h.xmlError(c, "ValidationError", "StackName is required")
	}

	detectionID, err := h.Backend.DetectStackDrift(stackName)
	if err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}

	type result struct {
		StackDriftDetectionID string `xml:"StackDriftDetectionId"`
	}
	type response struct {
		XMLName   xml.Name `xml:"DetectStackDriftResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"DetectStackDriftResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{
		Xmlns:     cfnNS,
		Result:    result{StackDriftDetectionID: detectionID},
		RequestID: uuid.New().String(),
	})
}

func (h *Handler) handleDetectStackResourceDrift(form url.Values, c *echo.Context) error {
	stackName := form.Get("StackName")
	if stackName == "" {
		return h.xmlError(c, "ValidationError", "StackName is required")
	}

	logicalID := form.Get("LogicalResourceId")
	if logicalID == "" {
		return h.xmlError(c, "ValidationError", "LogicalResourceId is required")
	}

	detectionID, err := h.Backend.DetectStackResourceDrift(stackName, logicalID)
	if err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}

	type result struct {
		StackDriftDetectionID string `xml:"StackDriftDetectionId"`
	}
	type response struct {
		XMLName   xml.Name `xml:"DetectStackResourceDriftResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		Result    result   `xml:"DetectStackResourceDriftResult"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{
		Xmlns:     cfnNS,
		Result:    result{StackDriftDetectionID: detectionID},
		RequestID: uuid.New().String(),
	})
}

func (h *Handler) handleDescribeStackDriftDetectionStatus(form url.Values, c *echo.Context) error {
	detectionID := form.Get("StackDriftDetectionId")
	if detectionID == "" {
		return h.xmlError(c, "ValidationError", "StackDriftDetectionId is required")
	}

	status, err := h.Backend.DescribeStackDriftDetectionStatus(detectionID)
	if err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}

	type driftResult struct {
		StackID                   string `xml:"StackId"`
		StackDriftDetectionID     string `xml:"StackDriftDetectionId"`
		StackDriftStatus          string `xml:"StackDriftStatus"`
		DetectionStatus           string `xml:"DetectionStatus"`
		DetectionStatusReason     string `xml:"DetectionStatusReason,omitempty"`
		Timestamp                 string `xml:"Timestamp"`
		DriftedStackResourceCount int    `xml:"DriftedStackResourceCount"`
	}
	type response struct {
		XMLName   xml.Name    `xml:"DescribeStackDriftDetectionStatusResponse"`
		Xmlns     string      `xml:"xmlns,attr"`
		RequestID string      `xml:"ResponseMetadata>RequestId"`
		Result    driftResult `xml:"DescribeStackDriftDetectionStatusResult"`
	}

	return writeXML(c, response{
		Xmlns: cfnNS,
		Result: driftResult{
			StackID:                   status.StackID,
			StackDriftDetectionID:     status.StackDriftDetectionID,
			StackDriftStatus:          status.StackDriftStatus,
			DetectionStatus:           status.DetectionStatus,
			DetectionStatusReason:     status.DetectionStatusReason,
			DriftedStackResourceCount: status.DriftedStackResourceCount,
			Timestamp:                 status.Timestamp.Format("2006-01-02T15:04:05Z"),
		},
		RequestID: uuid.New().String(),
	})
}

func (h *Handler) handleDescribeStackResourceDrifts(form url.Values, c *echo.Context) error {
	stackName := form.Get("StackName")
	if stackName == "" {
		return h.xmlError(c, "ValidationError", "StackName is required")
	}

	drifts, err := h.Backend.DescribeStackResourceDrifts(stackName)
	if err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}

	type driftXML struct {
		StackID                  string `xml:"StackId"`
		LogicalResourceID        string `xml:"LogicalResourceId"`
		PhysicalResourceID       string `xml:"PhysicalResourceId,omitempty"`
		ResourceType             string `xml:"ResourceType"`
		StackResourceDriftStatus string `xml:"StackResourceDriftStatus"`
		Timestamp                string `xml:"Timestamp"`
	}

	members := make([]driftXML, 0, len(drifts))
	for _, d := range drifts {
		members = append(members, driftXML{
			StackID:                  d.StackID,
			LogicalResourceID:        d.LogicalResourceID,
			PhysicalResourceID:       d.PhysicalResourceID,
			ResourceType:             d.ResourceType,
			StackResourceDriftStatus: d.StackResourceDriftStatus,
			Timestamp:                d.Timestamp.Format("2006-01-02T15:04:05Z"),
		})
	}

	type driftsResult struct {
		StackResourceDrifts []driftXML `xml:"StackResourceDrifts>member"`
	}
	type response struct {
		XMLName   xml.Name     `xml:"DescribeStackResourceDriftsResponse"`
		Xmlns     string       `xml:"xmlns,attr"`
		RequestID string       `xml:"ResponseMetadata>RequestId"`
		Result    driftsResult `xml:"DescribeStackResourceDriftsResult"`
	}

	return writeXML(c, response{
		Xmlns:     cfnNS,
		Result:    driftsResult{StackResourceDrifts: members},
		RequestID: uuid.New().String(),
	})
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

func (h *Handler) handleGetTemplateSummary(form url.Values, c *echo.Context) error {
	templateBody := form.Get("TemplateBody")
	stackName := form.Get("StackName")

	summary, err := h.Backend.GetTemplateSummary(templateBody, stackName)
	if err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}

	type paramXML struct {
		ParameterKey  string `xml:"ParameterKey"`
		ParameterType string `xml:"ParameterType,omitempty"`
		DefaultValue  string `xml:"DefaultValue,omitempty"`
		Description   string `xml:"Description,omitempty"`
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

func (h *Handler) handleContinueUpdateRollback(form url.Values, c *echo.Context) error {
	stackName := form.Get("StackName")
	if stackName == "" {
		return h.xmlError(c, "ValidationError", "StackName is required")
	}

	if err := h.Backend.ContinueUpdateRollback(c.Request().Context(), stackName); err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}

	type response struct {
		XMLName   xml.Name `xml:"ContinueUpdateRollbackResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cfnNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleCancelUpdateStack(form url.Values, c *echo.Context) error {
	stackName := form.Get("StackName")
	if stackName == "" {
		return h.xmlError(c, "ValidationError", "StackName is required")
	}

	if err := h.Backend.CancelUpdateStack(c.Request().Context(), stackName); err != nil {
		return h.xmlError(c, "ValidationError", err.Error())
	}

	type response struct {
		XMLName   xml.Name `xml:"CancelUpdateStackResponse"`
		Xmlns     string   `xml:"xmlns,attr"`
		RequestID string   `xml:"ResponseMetadata>RequestId"`
	}

	return writeXML(c, response{Xmlns: cfnNS, RequestID: uuid.New().String()})
}

func (h *Handler) handleDescribeAccountLimits(_ url.Values, c *echo.Context) error {
	limits := h.Backend.DescribeAccountLimits()

	type limitXML struct {
		Name  string `xml:"Name"`
		Value int    `xml:"Value"`
	}

	members := make([]limitXML, 0, len(limits))
	for _, l := range limits {
		members = append(members, limitXML(l))
	}

	type limitsResult struct {
		AccountLimits []limitXML `xml:"AccountLimits>member"`
	}
	type response struct {
		XMLName   xml.Name     `xml:"DescribeAccountLimitsResponse"`
		Xmlns     string       `xml:"xmlns,attr"`
		RequestID string       `xml:"ResponseMetadata>RequestId"`
		Result    limitsResult `xml:"DescribeAccountLimitsResult"`
	}

	return writeXML(c, response{
		Xmlns:     cfnNS,
		Result:    limitsResult{AccountLimits: members},
		RequestID: uuid.New().String(),
	})
}
