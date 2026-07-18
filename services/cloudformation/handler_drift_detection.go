package cloudformation

import (
	"encoding/xml"
	"net/url"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

// dispatchDriftOps handles drift-detection CloudFormation operations.
func (h *Handler) dispatchDriftOps(action string, form url.Values, c *echo.Context) (bool, error) {
	switch action {
	case "DetectStackDrift":
		return true, h.handleDetectStackDrift(form, c)
	case "DetectStackResourceDrift":
		return true, h.handleDetectStackResourceDrift(form, c)
	case "DescribeStackDriftDetectionStatus":
		return true, h.handleDescribeStackDriftDetectionStatus(form, c)
	case "DescribeStackResourceDrifts":
		return true, h.handleDescribeStackResourceDrifts(form, c)
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

	type propertyDiffXML struct {
		PropertyPath   string `xml:"PropertyPath"`
		ExpectedValue  string `xml:"ExpectedValue"`
		ActualValue    string `xml:"ActualValue"`
		DifferenceType string `xml:"DifferenceType"`
	}
	type driftXML struct {
		StackID                  string            `xml:"StackId"`
		LogicalResourceID        string            `xml:"LogicalResourceId"`
		PhysicalResourceID       string            `xml:"PhysicalResourceId,omitempty"`
		ResourceType             string            `xml:"ResourceType"`
		StackResourceDriftStatus string            `xml:"StackResourceDriftStatus"`
		ExpectedProperties       string            `xml:"ExpectedProperties,omitempty"`
		ActualProperties         string            `xml:"ActualProperties,omitempty"`
		Timestamp                string            `xml:"Timestamp"`
		PropertyDifferences      []propertyDiffXML `xml:"PropertyDifferences>member,omitempty"`
	}

	members := make([]driftXML, 0, len(drifts))
	for _, d := range drifts {
		propDiffs := make([]propertyDiffXML, 0, len(d.PropertyDifferences))
		for _, pd := range d.PropertyDifferences {
			propDiffs = append(propDiffs, propertyDiffXML(pd))
		}
		members = append(members, driftXML{
			StackID:                  d.StackID,
			LogicalResourceID:        d.LogicalResourceID,
			PhysicalResourceID:       d.PhysicalResourceID,
			ResourceType:             d.ResourceType,
			StackResourceDriftStatus: d.StackResourceDriftStatus,
			ExpectedProperties:       d.ExpectedProperties,
			ActualProperties:         d.ActualProperties,
			PropertyDifferences:      propDiffs,
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
