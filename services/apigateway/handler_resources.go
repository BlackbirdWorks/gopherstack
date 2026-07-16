package apigateway

import (
	"encoding/json"
	"net/http"
)

type getResourcesInput struct {
	RestAPIID string `json:"restApiId"`
	Position  string `json:"position"`
	Limit     int    `json:"limit"`
}

type getResourceInput struct {
	RestAPIID  string `json:"restApiId"`
	ResourceID string `json:"resourceId"`
}

type createResourceInput struct {
	RestAPIID string `json:"restApiId"`
	ParentID  string `json:"parentId"`
	PathPart  string `json:"pathPart"`
}

type deleteResourceInput struct {
	RestAPIID  string `json:"restApiId"`
	ResourceID string `json:"resourceId"`
}

type updateResourceHandlerInput struct {
	UpdateResourceInput
	RestAPIID  string `json:"restApiId"`
	ResourceID string `json:"resourceId"`
}

func (h *Handler) resourceActions() map[string]actionFn {
	return map[string]actionFn{
		opGetResources:   h.getResourcesAction,
		opGetResource:    h.getResourceAction,
		opCreateResource: h.createResourceAction,
		opDeleteResource: h.deleteResourceAction,
		opUpdateResource: h.updateResourceAction,
	}
}

func (h *Handler) getResourcesAction(b []byte) (int, any, error) {
	var input getResourcesInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	resources, position, err := h.Backend.GetResources(input.RestAPIID, input.Position, input.Limit)
	if err != nil {
		return 0, nil, err
	}
	if position != "" {
		return http.StatusOK, map[string]any{keyItem: resources, keyPosition: position}, nil
	}

	return http.StatusOK, map[string]any{keyItem: resources}, nil
}

func (h *Handler) getResourceAction(b []byte) (int, any, error) {
	var input getResourceInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	r, err := h.Backend.GetResource(input.RestAPIID, input.ResourceID)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, r, nil
}

func (h *Handler) createResourceAction(b []byte) (int, any, error) {
	var input createResourceInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	r, err := h.Backend.CreateResource(input.RestAPIID, input.ParentID, input.PathPart)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusCreated, r, nil
}

func (h *Handler) deleteResourceAction(b []byte) (int, any, error) {
	var input deleteResourceInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	if err := h.Backend.DeleteResource(input.RestAPIID, input.ResourceID); err != nil {
		return 0, nil, err
	}

	return http.StatusNoContent, map[string]any{}, nil
}

func (h *Handler) updateResourceAction(b []byte) (int, any, error) {
	var input updateResourceHandlerInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	res, err := h.Backend.UpdateResource(input.RestAPIID, input.ResourceID, input.UpdateResourceInput)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, res, nil
}
