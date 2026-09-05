package apigateway

import (
	"encoding/json"
	"net/http"
	"strings"
)

type getDocumentationPartInput struct {
	RestAPIID string `json:"restApiId"`
	DocPartID string `json:"docPartId"`
}

type getDocumentationPartsInput struct {
	RestAPIID string `json:"restApiId"`
	Position  string `json:"position"`
	NameQuery string `json:"name"`
	Path      string `json:"path"`
	Type      string `json:"type"`
	Limit     int    `json:"limit"`
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
	Position  string `json:"position"`
	Limit     int    `json:"limit"`
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
	ps = filterDocumentationParts(ps, input.NameQuery, input.Path, input.Type)
	if input.Limit == 0 && input.Position == "" {
		return http.StatusOK, map[string]any{keyItem: ps}, nil
	}
	page, position := paginatePageByKey(
		ps,
		input.Limit,
		input.Position,
		func(p DocumentationPart) string { return p.ID },
	)
	if position != "" {
		return http.StatusOK, map[string]any{keyItem: page, keyPosition: position}, nil
	}

	return http.StatusOK, map[string]any{keyItem: page}, nil
}

// filterDocumentationParts applies GetDocumentationParts' name (substring),
// path (exact) and type (exact) filters. Real keys: name, path, type in
// apigateway@v1.42.4/serializers.go:4904,4908,4925. locationStatus has no
// backing field here — this backend doesn't track a separate "documented"
// version snapshot, so it's not filtered on.
func filterDocumentationParts(parts []DocumentationPart, nameQuery, path, locType string) []DocumentationPart {
	if nameQuery == "" && path == "" && locType == "" {
		return parts
	}
	out := make([]DocumentationPart, 0, len(parts))
	for _, p := range parts {
		if nameQuery != "" && !strings.Contains(p.Location.Name, nameQuery) {
			continue
		}
		if path != "" && p.Location.Path != path {
			continue
		}
		if locType != "" && p.Location.Type != locType {
			continue
		}
		out = append(out, p)
	}

	return out
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
	if input.Limit == 0 && input.Position == "" {
		return http.StatusOK, map[string]any{keyItem: vs}, nil
	}
	page, position := paginatePageByKey(vs, input.Limit, input.Position,
		func(v DocumentationVersion) string { return v.Version })
	if position != "" {
		return http.StatusOK, map[string]any{keyItem: page, keyPosition: position}, nil
	}

	return http.StatusOK, map[string]any{keyItem: page}, nil
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
