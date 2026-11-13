package xray

import (
	"context"
	"encoding/json/v2"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func (h *Handler) handleGetTraceSegmentDestination(_ context.Context, _ []byte) ([]byte, error) {
	dest := h.Backend.GetTraceSegmentDestination()

	return json.Marshal(map[string]any{
		"Destination": dest,
		"Status":      statusActive,
	}, jsonx.V1())
}

type updateTraceSegmentDestinationInput struct {
	Destination string `json:"Destination"`
}

func (h *Handler) handleUpdateTraceSegmentDestination(_ context.Context, body []byte) ([]byte, error) {
	var in updateTraceSegmentDestinationInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in, jsonx.V1()); err != nil {
			return nil, err
		}
	}

	if in.Destination == "" {
		return nil, fmt.Errorf("%w: Destination is required", errInvalidRequest)
	}

	// aws-sdk-go-v2/service/xray/types/enums.go: TraceSegmentDestination has exactly
	// two values, XRay and CloudWatchLogs.
	if in.Destination != "XRay" && in.Destination != "CloudWatchLogs" {
		return nil, fmt.Errorf(
			"%w: Destination must be XRay or CloudWatchLogs, got %q", errInvalidRequest, in.Destination,
		)
	}

	dest := h.Backend.UpdateTraceSegmentDestination(in.Destination)

	return json.Marshal(map[string]any{
		"Destination": dest,
		"Status":      statusActive,
	}, jsonx.V1())
}
