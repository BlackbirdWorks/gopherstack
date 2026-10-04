package rolesanywhere

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
)

// ---- Attribute mapping handlers ----

func (h *Handler) handlePutAttributeMapping(ctx context.Context, path string, body []byte) (any, int, error) {
	profileID := extractProfileIDFromMappingPath(path)

	var req struct {
		CertificateField string        `json:"certificateField"`
		MappingRules     []MappingRule `json:"mappingRules"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, 0, ErrValidation
	}

	p, err := h.Backend.PutAttributeMapping(ctx, profileID, req.CertificateField, req.MappingRules)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyProfile: h.profileJSON(ctx, p)}, http.StatusOK, nil
}

func (h *Handler) handleDeleteAttributeMapping(ctx context.Context, path, query string) (any, int, error) {
	profileID := extractProfileIDFromMappingPath(path)

	vals, _ := url.ParseQuery(query)
	certificateField := vals.Get("certificateField")
	specifiers := vals["specifiers"]

	p, err := h.Backend.DeleteAttributeMapping(ctx, profileID, certificateField, specifiers)
	if err != nil {
		return nil, 0, err
	}

	return map[string]any{keyProfile: h.profileJSON(ctx, p)}, http.StatusOK, nil
}

// extractProfileIDFromMappingPath extracts the profile ID from /profiles/{id}/mappings.
func extractProfileIDFromMappingPath(path string) string {
	segments := strings.Split(strings.TrimPrefix(path, "/"), "/")

	if len(segments) >= 2 { //nolint:mnd // existing issue.
		return segments[1]
	}

	return ""
}
