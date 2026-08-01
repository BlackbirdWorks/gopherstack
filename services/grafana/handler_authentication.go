package grafana

import (
	"context"
	"encoding/json"
	"net/http"
)

func (h *Handler) handleDescribeWorkspaceAuthentication(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)

	w, err := h.Backend.DescribeWorkspaceAuthentication(segs[1])
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"authentication": toAuthenticationDescriptionWire(w)})
}

func (h *Handler) handleUpdateWorkspaceAuthentication(_ context.Context, r *http.Request, body []byte) ([]byte, error) {
	segs := rawPathSegments(r)

	var req updateWorkspaceAuthenticationRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, validationError("invalid request body: " + err.Error())
	}

	w, err := h.Backend.UpdateWorkspaceAuthentication(segs[1], &req)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"authentication": toAuthenticationDescriptionWire(w)})
}
