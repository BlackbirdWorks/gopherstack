package macie2

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

func parseTemplatesPath(method string, parts []string) (string, string) {
	// /templates/sensitivity-inspections[/{id}]
	if len(parts) < 2 || parts[1] != "sensitivity-inspections" {
		return opUnknown, ""
	}

	switch len(parts) {
	case depthResource: // /templates/sensitivity-inspections
		if method == http.MethodGet {
			return opListSensitivityInspectionTemplates, ""
		}
	case 3: //nolint:mnd // existing issue.
		switch method {
		case http.MethodGet:
			return opGetSensitivityInspectionTemplate, parts[2]
		case http.MethodPut:
			return opUpdateSensitivityInspectionTemplate, parts[2]
		}
	}

	return opUnknown, ""
}

func (h *Handler) dispatchSensitivityTemplateOps(op, path string, body []byte) (any, int, bool, error) {
	switch op {
	case opGetSensitivityInspectionTemplate:
		id := extractTemplateID(path)
		result, code, err := h.handleGetSensitivityInspectionTemplate(id)

		return result, code, true, err

	case opListSensitivityInspectionTemplates:
		result, code, err := h.handleListSensitivityInspectionTemplates()

		return result, code, true, err

	case opUpdateSensitivityInspectionTemplate:
		id := extractTemplateID(path)
		code, err := h.handleUpdateSensitivityInspectionTemplate(id, body)

		return nil, code, true, err
	}

	return nil, 0, false, nil
}

func (h *Handler) handleGetSensitivityInspectionTemplate(templateID string) (any, int, error) {
	tmpl, err := h.Backend.GetSensitivityInspectionTemplate(templateID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return nil, http.StatusNotFound, err
		}

		return nil, http.StatusInternalServerError, err
	}

	return tmpl, http.StatusOK, nil
}

func (h *Handler) handleListSensitivityInspectionTemplates() (any, int, error) {
	templates, err := h.Backend.ListSensitivityInspectionTemplates()
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]any{"sensitivityInspectionTemplates": templates}, http.StatusOK, nil
}

func (h *Handler) handleUpdateSensitivityInspectionTemplate(templateID string, body []byte) (int, error) {
	var req struct {
		Excludes    map[string]any `json:"excludes"`
		Includes    map[string]any `json:"includes"`
		Description string         `json:"description"`
		Name        string         `json:"name"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, ErrValidation
	}

	if err := h.Backend.UpdateSensitivityInspectionTemplate(
		templateID, req.Name, req.Description, req.Excludes, req.Includes,
	); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return http.StatusNotFound, err
		}

		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func extractTemplateID(path string) string {
	// /templates/sensitivity-inspections/{id}
	trimmed := strings.TrimPrefix(path, "/"+pathTemplates+"/sensitivity-inspections/")

	return trimmed
}
