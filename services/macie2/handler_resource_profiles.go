package macie2

import (
	"encoding/json"
	"net/http"
)

func parseResourceProfilesPath(method string, parts []string) (string, string) {
	switch len(parts) {
	case depthRoot: // /resource-profiles
		switch method {
		case http.MethodGet:
			return opGetResourceProfile, ""
		case http.MethodPatch:
			return opUpdateResourceProfile, ""
		}
	case depthResource: // /resource-profiles/{artifacts|detections}
		switch parts[1] {
		case "artifacts":
			if method == http.MethodGet {
				return opListResourceProfileArtifacts, ""
			}
		case "detections":
			switch method {
			case http.MethodGet:
				return opListResourceProfileDetections, ""
			case http.MethodPatch:
				return opUpdateResourceProfileDetections, ""
			}
		}
	}

	return opUnknown, ""
}

func (h *Handler) dispatchResourceProfileOps(op, query string, body []byte) (any, int, bool, error) {
	resourceARN := extractQueryParam(query, "resourceArn")

	switch op {
	case opGetResourceProfile:
		result, code, err := h.handleGetResourceProfile(resourceARN)

		return result, code, true, err

	case opUpdateResourceProfile:
		code, err := h.handleUpdateResourceProfile(resourceARN, body)

		return nil, code, true, err

	case opListResourceProfileArtifacts:
		result, code, err := h.handleListResourceProfileArtifacts(resourceARN)

		return result, code, true, err

	case opListResourceProfileDetections:
		result, code, err := h.handleListResourceProfileDetections(resourceARN)

		return result, code, true, err

	case opUpdateResourceProfileDetections:
		code, err := h.handleUpdateResourceProfileDetections(resourceARN, body)

		return nil, code, true, err
	}

	return nil, 0, false, nil
}

func (h *Handler) handleGetResourceProfile(resourceARN string) (any, int, error) {
	profile, err := h.Backend.GetResourceProfile(resourceARN)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return profile, http.StatusOK, nil
}

func (h *Handler) handleUpdateResourceProfile(resourceARN string, body []byte) (int, error) {
	var req struct {
		SensitivityScoreOverride int32 `json:"sensitivityScoreOverride"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return http.StatusBadRequest, ErrValidation
		}
	}

	if err := h.Backend.UpdateResourceProfile(resourceARN, req.SensitivityScoreOverride); err != nil {
		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}

func (h *Handler) handleListResourceProfileArtifacts(resourceARN string) (any, int, error) {
	artifacts, err := h.Backend.ListResourceProfileArtifacts(resourceARN)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]any{"artifacts": artifacts}, http.StatusOK, nil
}

func (h *Handler) handleListResourceProfileDetections(resourceARN string) (any, int, error) {
	detections, err := h.Backend.ListResourceProfileDetections(resourceARN)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return map[string]any{"detections": detections}, http.StatusOK, nil
}

func (h *Handler) handleUpdateResourceProfileDetections(resourceARN string, body []byte) (int, error) {
	var req struct {
		SuppressDataIdentifiers []map[string]any `json:"suppressDataIdentifiers"`
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &req); err != nil {
			return http.StatusBadRequest, ErrValidation
		}
	}

	if err := h.Backend.UpdateResourceProfileDetections(resourceARN, req.SuppressDataIdentifiers); err != nil {
		return http.StatusInternalServerError, err
	}

	return http.StatusOK, nil
}
