package apigateway

import (
	"encoding/json"
	"net/http"
)

type getDocumentationPartInput struct {
	RestAPIID string `json:"restApiId"`
	DocPartID string `json:"docPartId"`
}

type getDocumentationPartsInput struct {
	RestAPIID string `json:"restApiId"`
}

type deleteDocumentationPartInput struct {
	RestAPIID string `json:"restApiId"`
	DocPartID string `json:"docPartId"`
}

type getDocumentationVersionInput struct {
	RestAPIID  string `json:"restApiId"`
	DocVersion string `json:"documentationVersion"`
}

type getDocumentationVersionsInput struct {
	RestAPIID string `json:"restApiId"`
}

type deleteDocumentationVersionInput struct {
	RestAPIID  string `json:"restApiId"`
	DocVersion string `json:"documentationVersion"`
}

// parseAPIGWRestAPIsDocDepth4 handles /restapis/{id}/documentation/{parts|versions} paths.
func parseAPIGWRestAPIsDocDepth4(method string, segs []string, apiID string) (string, map[string]string, bool) {
	apiParam := map[string]string{keyRestAPIID: apiID}

	switch segs[3] {
	case apiGWSegDocParts:
		switch method {
		case http.MethodPost:
			return opCreateDocumentationPart, apiParam, true
		case http.MethodGet:
			return opGetDocumentationParts, apiParam, true
		case http.MethodPut:
			return opImportDocumentationParts, apiParam, true
		}
	case apiGWSegDocVersions:
		switch method {
		case http.MethodPost:
			return opCreateDocumentationVersion, apiParam, true
		case http.MethodGet:
			return opGetDocumentationVersions, apiParam, true
		}
	}

	return apiGWUnknownOp, nil, false
}

// parseAPIGWRestAPIsDocDeep handles depth-5 documentation parts/versions paths.
func parseAPIGWRestAPIsDocDeep(method string, segs []string, n int, apiID string) (string, map[string]string, bool) {
	if n != pathDepth5 {
		return apiGWUnknownOp, nil, false
	}

	switch segs[3] {
	case apiGWSegDocParts:
		params := map[string]string{keyRestAPIID: apiID, keyDocPartID: segs[4]}
		switch method {
		case http.MethodGet:
			return opGetDocumentationPart, params, true
		case http.MethodDelete:
			return opDeleteDocumentationPart, params, true
		case http.MethodPatch:
			return opUpdateDocumentationPart, params, true
		}
	case apiGWSegDocVersions:
		params := map[string]string{keyRestAPIID: apiID, keyDocumentationVersion: segs[4]}
		switch method {
		case http.MethodGet:
			return opGetDocumentationVersion, params, true
		case http.MethodDelete:
			return opDeleteDocumentationVersion, params, true
		case http.MethodPatch:
			return opUpdateDocumentationVersion, params, true
		}
	}

	return apiGWUnknownOp, nil, false
}

// documentationActions returns the action map for documentation part and
// version CRUD operations.
func (h *Handler) documentationActions() map[string]actionFn {
	return map[string]actionFn{
		opCreateDocumentationPart:    h.createDocumentationPartAction,
		opCreateDocumentationVersion: h.createDocumentationVersionAction,
		opGetDocumentationPart:       h.getDocumentationPartAction,
		opGetDocumentationParts:      h.getDocumentationPartsAction,
		opUpdateDocumentationPart:    h.updateDocumentationPartAction,
		opUpdateDocumentationVersion: h.updateDocumentationVersionAction,
		opDeleteDocumentationPart:    h.deleteDocumentationPartAction,
		opGetDocumentationVersion:    h.getDocumentationVersionAction,
		opGetDocumentationVersions:   h.getDocumentationVersionsAction,
		opDeleteDocumentationVersion: h.deleteDocumentationVersionAction,
	}
}

func (h *Handler) createDocumentationPartAction(b []byte) (int, any, error) {
	var input CreateDocumentationPartInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}

	part, err := h.Backend.CreateDocumentationPart(input)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusCreated, part, nil
}

func (h *Handler) createDocumentationVersionAction(b []byte) (int, any, error) {
	var input CreateDocumentationVersionInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}

	ver, err := h.Backend.CreateDocumentationVersion(input)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusCreated, ver, nil
}

func (h *Handler) getDocumentationPartAction(b []byte) (int, any, error) {
	var input getDocumentationPartInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	p, err := h.Backend.GetDocumentationPart(input.RestAPIID, input.DocPartID)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, p, nil
}

func (h *Handler) getDocumentationPartsAction(b []byte) (int, any, error) {
	var input getDocumentationPartsInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	ps, err := h.Backend.GetDocumentationParts(input.RestAPIID)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, map[string]any{keyItem: ps}, nil
}

func (h *Handler) updateDocumentationPartAction(b []byte) (int, any, error) {
	var input UpdateDocumentationPartInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	out, err := h.Backend.UpdateDocumentationPart(input)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, out, nil
}

func (h *Handler) updateDocumentationVersionAction(b []byte) (int, any, error) {
	var input UpdateDocumentationVersionInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	out, err := h.Backend.UpdateDocumentationVersion(input)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, out, nil
}

func (h *Handler) deleteDocumentationPartAction(b []byte) (int, any, error) {
	var input deleteDocumentationPartInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	if err := h.Backend.DeleteDocumentationPart(input.RestAPIID, input.DocPartID); err != nil {
		return 0, nil, err
	}

	return http.StatusNoContent, map[string]any{}, nil
}

func (h *Handler) getDocumentationVersionAction(b []byte) (int, any, error) {
	var input getDocumentationVersionInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	v, err := h.Backend.GetDocumentationVersion(input.RestAPIID, input.DocVersion)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, v, nil
}

func (h *Handler) getDocumentationVersionsAction(b []byte) (int, any, error) {
	var input getDocumentationVersionsInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	vs, err := h.Backend.GetDocumentationVersions(input.RestAPIID)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, map[string]any{keyItem: vs}, nil
}

func (h *Handler) deleteDocumentationVersionAction(b []byte) (int, any, error) {
	var input deleteDocumentationVersionInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	if err := h.Backend.DeleteDocumentationVersion(input.RestAPIID, input.DocVersion); err != nil {
		return 0, nil, err
	}

	return http.StatusNoContent, map[string]any{}, nil
}
