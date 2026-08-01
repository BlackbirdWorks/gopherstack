package grafana

import (
	"context"
	"encoding/json"
	"net/http"
)

func (h *Handler) handleDescribeWorkspaceConfiguration(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)

	configuration, grafanaVersion, err := h.Backend.DescribeWorkspaceConfiguration(segs[1])
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"configuration":  configuration,
		"grafanaVersion": grafanaVersion,
	})
}

func (h *Handler) handleUpdateWorkspaceConfiguration(_ context.Context, r *http.Request, body []byte) ([]byte, error) {
	segs := rawPathSegments(r)

	var req updateWorkspaceConfigurationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, validationError("invalid request body: " + err.Error())
	}

	if err := h.Backend.UpdateWorkspaceConfiguration(segs[1], &req); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}
