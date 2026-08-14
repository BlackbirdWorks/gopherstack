package opensearch

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// capabilityConfigJSON is the wire shape of CapabilityBaseRequestConfig /
// CapabilityBaseResponseConfig / CapabilityExtendedResponseConfig -- all
// three are tagged unions whose only defined member (types.AIConfig) is an
// empty struct, so on the wire they are always exactly {"aiConfig": {}}
// (confirmed via the SDK's own serializeDocumentAIConfig/
// deserializeDocumentAIConfig, both of which read/write zero fields).
type capabilityConfigJSON struct {
	AiConfig struct{} `json:"aiConfig"`
}

// registerCapabilityRequest is the JSON request body for RegisterCapability.
type registerCapabilityRequest struct {
	CapabilityConfig capabilityConfigJSON `json:"capabilityConfig"`
	CapabilityName   string               `json:"capabilityName"`
}

// registerCapabilityOutput matches RegisterCapabilityOutput.
type registerCapabilityOutput struct {
	ApplicationID    string               `json:"applicationId"`
	CapabilityConfig capabilityConfigJSON `json:"capabilityConfig"`
	CapabilityName   string               `json:"capabilityName"`
	Status           string               `json:"status"`
}

// getCapabilityOutput matches GetCapabilityOutput, which additionally
// carries Failures beyond RegisterCapabilityOutput's fields.
type getCapabilityOutput struct {
	ApplicationID    string               `json:"applicationId"`
	CapabilityConfig capabilityConfigJSON `json:"capabilityConfig"`
	CapabilityName   string               `json:"capabilityName"`
	Status           string               `json:"status"`
	Failures         []any                `json:"failures"`
}

// handleCapabilityRoutes handles /application/{appID}/capability/{capPath}:
// "register" (POST), "deregister/{capabilityName}" (DELETE), and
// "{capabilityName}" (GET) -- the three URI shapes serializers.go binds for
// RegisterCapability/DeregisterCapability/GetCapability respectively.
func (h *Handler) handleCapabilityRoutes(w http.ResponseWriter, r *http.Request, appID, capPath string) {
	switch {
	case capPath == "register" && r.Method == http.MethodPost:
		h.handleRegisterCapability(w, r, appID)
	case strings.HasPrefix(capPath, "deregister/") && r.Method == http.MethodDelete:
		capabilityName := strings.TrimPrefix(capPath, "deregister/")
		h.handleDeregisterCapability(w, r, appID, capabilityName)
	case capPath != "" && !strings.Contains(capPath, "/") && r.Method == http.MethodGet:
		h.handleGetCapability(w, r, appID, capPath)
	default:
		h.writeError(r, w, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}
}

func (h *Handler) handleRegisterCapability(w http.ResponseWriter, r *http.Request, appID string) {
	body, err := httputils.ReadBody(r)
	if err != nil {
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", "failed to read body")

		return
	}

	var req registerCapabilityRequest
	if len(body) > 0 {
		if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
			h.writeError(r, w, http.StatusBadRequest, "ValidationException", "invalid JSON body")

			return
		}
	}

	capability, regErr := h.Backend.RegisterCapability(appID, req.CapabilityName)
	if regErr != nil {
		h.writeCapabilityError(r, w, regErr)

		return
	}

	h.writeJSON(r, w, registerCapabilityOutput{
		ApplicationID:  capability.ApplicationID,
		CapabilityName: capability.CapabilityName,
		Status:         capability.Status,
	})
}

func (h *Handler) handleDeregisterCapability(
	w http.ResponseWriter,
	r *http.Request,
	appID, capabilityName string,
) {
	if err := h.Backend.DeregisterCapability(appID, capabilityName); err != nil {
		h.writeCapabilityError(r, w, err)

		return
	}

	h.writeJSON(r, w, map[string]any{jsonKeyStatusLower: capabilityStatusDeleting})
}

func (h *Handler) handleGetCapability(w http.ResponseWriter, r *http.Request, appID, capabilityName string) {
	capability, err := h.Backend.GetCapability(appID, capabilityName)
	if err != nil {
		h.writeCapabilityError(r, w, err)

		return
	}

	h.writeJSON(r, w, getCapabilityOutput{
		ApplicationID:  capability.ApplicationID,
		CapabilityName: capability.CapabilityName,
		Status:         capability.Status,
		Failures:       []any{},
	})
}

// writeCapabilityError maps capability errors to their documented HTTP
// status codes -- ResourceNotFoundException at 409, matching the same
// "application" API family convention as writeAttachmentError.
func (h *Handler) writeCapabilityError(r *http.Request, w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrApplicationNotFound), errors.Is(err, ErrCapabilityNotFound):
		h.writeError(r, w, http.StatusConflict, "ResourceNotFoundException", err.Error())
	default:
		h.writeError(r, w, http.StatusBadRequest, "ValidationException", err.Error())
	}
}
