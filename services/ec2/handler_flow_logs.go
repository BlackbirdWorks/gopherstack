package ec2

import (
	"encoding/xml"
	"net/url"
)

type getFlowLogsIntegrationTemplateResponse struct {
	XMLName   xml.Name `xml:"GetFlowLogsIntegrationTemplateResponse"`
	RequestID string   `xml:"requestId"`
	Result    string   `xml:"result,omitempty"`
}

func (h *Handler) handleGetFlowLogsIntegrationTemplate(vals url.Values, reqID string) (any, error) {
	flowLogID := vals.Get("FlowLogId")
	s3DestinationArn := vals.Get("ConfigDeliveryS3DestinationArn")
	// Top-level key is "IntegrateService" (singular) -- the request member is
	// IntegrateServices but ec2query serializes struct-typed members by their
	// Go field name only when it lacks a FlatKey override; this one is
	// serialized via object.Key("IntegrateService") (ec2@v1.329.0
	// serializers.go:86508), not "IntegrateServices". Using the plural here
	// meant these two always-required fields silently read as empty.
	athenaResultArn := vals.Get("IntegrateService.AthenaIntegration.1.IntegrationResultS3DestinationArn")
	partitionLoadFrequency := vals.Get("IntegrateService.AthenaIntegration.1.PartitionLoadFrequency")

	tmpl, err := h.Backend.GetFlowLogsIntegrationTemplate(
		flowLogID, s3DestinationArn, athenaResultArn, partitionLoadFrequency,
	)
	if err != nil {
		return nil, err
	}

	return &getFlowLogsIntegrationTemplateResponse{
		RequestID: reqID,
		Result:    tmpl,
	}, nil
}

// ---- Misc singletons ----

type spotPlacementScoreItem struct {
	Region             string `xml:"region,omitempty"`
	AvailabilityZoneID string `xml:"availabilityZoneId,omitempty"`
	Score              int32  `xml:"score"`
}

// registerFlowLogsOps registers the FlowLogs operation handlers.
func registerFlowLogsOps(h *Handler, ops map[string]ec2ActionFn) {
	ops["GetFlowLogsIntegrationTemplate"] = h.handleGetFlowLogsIntegrationTemplate
}

// flowLogsSupportedOperations lists the operation names registered by
// registerFlowLogsOps, for GetSupportedOperations().
func flowLogsSupportedOperations() []string {
	return []string{
		"GetFlowLogsIntegrationTemplate",
	}
}
