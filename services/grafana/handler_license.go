package grafana

import (
	"context"
	"encoding/json"
	"net/http"
)

// grafanaTokenHeader is the HTTP header AssociateLicenseInput.GrafanaToken is
// bound to on the wire (confirmed via serializers.go:
// encoder.SetHeader("Grafana-Token") in
// awsRestjson1_serializeOpHttpBindingsAssociateLicenseInput) -- NOT a body
// or query member, despite looking like an ordinary optional string field on
// the Go struct.
const grafanaTokenHeader = "Grafana-Token"

func (h *Handler) handleAssociateLicense(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	workspaceID, licenseType := segs[1], segs[3]
	grafanaToken := r.Header.Get(grafanaTokenHeader)

	w, err := h.Backend.AssociateLicense(workspaceID, licenseType, grafanaToken)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyWorkspace: toWorkspaceDescriptionWire(w)})
}

func (h *Handler) handleDisassociateLicense(_ context.Context, r *http.Request, _ []byte) ([]byte, error) {
	segs := rawPathSegments(r)
	workspaceID, licenseType := segs[1], segs[3]

	w, err := h.Backend.DisassociateLicense(workspaceID, licenseType)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{keyWorkspace: toWorkspaceDescriptionWire(w)})
}
