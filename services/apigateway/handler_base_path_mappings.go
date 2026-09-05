package apigateway

import (
	"encoding/json"
	"net/http"
)

type getBasePathMappingInput struct {
	DomainName string `json:"domainName"`
	BasePath   string `json:"basePath"`
}

type getBasePathMappingsInput struct {
	DomainName string `json:"domainName"`
	Position   string `json:"position"`
	Limit      int    `json:"limit"`
}

type deleteBasePathMappingInput struct {
	DomainName string `json:"domainName"`
	BasePath   string `json:"basePath"`
}

// parseAPIGWDomainNamesBasePathMapping handles /domainnames/{name}/basepathmappings/{basePath} paths.
func parseAPIGWDomainNamesBasePathMapping(method string, segs []string) (string, map[string]string, bool) {
	params := map[string]string{keyDomainName: segs[1], keyBasePath: segs[3]}

	switch method {
	case http.MethodGet:
		return opGetBasePathMapping, params, true
	case http.MethodPatch:
		return opUpdateBasePathMapping, params, true
	case http.MethodDelete:
		return opDeleteBasePathMapping, params, true
	}

	return apiGWUnknownOp, nil, false
}

// basePathMappingActions returns the action map for base path mapping CRUD operations.
func (h *Handler) basePathMappingActions() map[string]actionFn {
	return map[string]actionFn{
		opCreateBasePathMapping: h.createBasePathMappingAction,
		opGetBasePathMapping:    h.getBasePathMappingAction,
		opGetBasePathMappings:   h.getBasePathMappingsAction,
		opDeleteBasePathMapping: h.deleteBasePathMappingAction,
		opUpdateBasePathMapping: h.updateBasePathMappingAction,
	}
}

func (h *Handler) createBasePathMappingAction(b []byte) (int, any, error) {
	var input CreateBasePathMappingInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}

	bpm, err := h.Backend.CreateBasePathMapping(input)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusCreated, bpm, nil
}

func (h *Handler) getBasePathMappingAction(b []byte) (int, any, error) {
	var input getBasePathMappingInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	bpm, err := h.Backend.GetBasePathMapping(input.DomainName, input.BasePath)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, bpm, nil
}

func (h *Handler) getBasePathMappingsAction(b []byte) (int, any, error) {
	var input getBasePathMappingsInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	bpms, err := h.Backend.GetBasePathMappings(input.DomainName)
	if err != nil {
		return 0, nil, err
	}
	if input.Limit == 0 && input.Position == "" {
		return http.StatusOK, map[string]any{keyItem: bpms}, nil
	}
	page, position := paginatePageByKey(bpms, input.Limit, input.Position,
		func(m BasePathMapping) string { return m.BasePath })
	if position != "" {
		return http.StatusOK, map[string]any{keyItem: page, keyPosition: position}, nil
	}

	return http.StatusOK, map[string]any{keyItem: page}, nil
}

func (h *Handler) deleteBasePathMappingAction(b []byte) (int, any, error) {
	var input deleteBasePathMappingInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	if err := h.Backend.DeleteBasePathMapping(input.DomainName, input.BasePath); err != nil {
		return 0, nil, err
	}

	return http.StatusAccepted, map[string]any{}, nil
}

func (h *Handler) updateBasePathMappingAction(b []byte) (int, any, error) {
	var input UpdateBasePathMappingInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	out, err := h.Backend.UpdateBasePathMapping(input)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, out, nil
}
