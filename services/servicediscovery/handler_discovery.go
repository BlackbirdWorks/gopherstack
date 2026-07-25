package servicediscovery

import (
	"context"
	"encoding/json"
	"fmt"
)

type discoverInstancesRequest struct {
	QueryParameters    map[string]string `json:"QueryParameters"`
	OptionalParameters map[string]string `json:"OptionalParameters"`
	MaxResults         *int              `json:"MaxResults"`
	NamespaceName      string            `json:"NamespaceName"`
	ServiceName        string            `json:"ServiceName"`
	HealthStatus       string            `json:"HealthStatus"`
}

func (h *Handler) handleDiscoverInstances(_ context.Context, body []byte) ([]byte, error) {
	var req discoverInstancesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.NamespaceName == "" {
		return nil, fmt.Errorf("%w: NamespaceName is required", errInvalidRequest)
	}

	if req.ServiceName == "" {
		return nil, fmt.Errorf("%w: ServiceName is required", errInvalidRequest)
	}

	discovered, revision, err := h.Backend.DiscoverInstances(
		req.NamespaceName,
		req.ServiceName,
		req.HealthStatus,
		req.QueryParameters,
		req.OptionalParameters,
	)
	if err != nil {
		return nil, err
	}

	maxResults := 0
	if req.MaxResults != nil {
		maxResults = *req.MaxResults
	}

	if maxResults > 0 && maxResults < len(discovered) {
		discovered = discovered[:maxResults]
	}

	items := make([]map[string]any, 0, len(discovered))
	for _, inst := range discovered {
		attrs := inst.Attributes
		if attrs == nil {
			attrs = map[string]string{}
		}

		items = append(items, map[string]any{
			"InstanceId":    inst.InstanceID,
			"NamespaceName": inst.NamespaceName,
			"ServiceName":   inst.ServiceName,
			"HealthStatus":  inst.HealthStatus,
			keyAttributes:   attrs,
		})
	}

	return json.Marshal(map[string]any{
		"Instances":         items,
		"InstancesRevision": revision,
	})
}

type discoverInstancesRevisionRequest struct {
	NamespaceName string `json:"NamespaceName"`
	ServiceName   string `json:"ServiceName"`
}

func (h *Handler) handleDiscoverInstancesRevision(_ context.Context, body []byte) ([]byte, error) {
	var req discoverInstancesRevisionRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.NamespaceName == "" {
		return nil, fmt.Errorf("%w: NamespaceName is required", errInvalidRequest)
	}

	if req.ServiceName == "" {
		return nil, fmt.Errorf("%w: ServiceName is required", errInvalidRequest)
	}

	revision, err := h.Backend.DiscoverInstancesRevision(req.NamespaceName, req.ServiceName)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"InstancesRevision": revision,
	})
}
