package apigateway

import (
	"encoding/json"
	"net/http"
)

type getModelInput struct {
	RestAPIID string `json:"restApiId"`
	ModelName string `json:"modelName"`
}

type getModelsInput struct {
	RestAPIID string `json:"restApiId"`
}

type deleteModelInput struct {
	RestAPIID string `json:"restApiId"`
	ModelName string `json:"modelName"`
}

type updateModelInput struct {
	UpdateModelInput
	RestAPIID string `json:"restApiId"`
	ModelName string `json:"modelName"`
}

// schemaModelActions returns the action map for Model CRUD operations, plus
// GetModelTemplate.
func (h *Handler) schemaModelActions() map[string]actionFn {
	return map[string]actionFn{
		opCreateModel:      h.createModelAction,
		opGetModel:         h.getModelAction,
		opGetModels:        h.getModelsAction,
		opDeleteModel:      h.deleteModelAction,
		opUpdateModel:      h.updateModelAction,
		opGetModelTemplate: h.getModelTemplateAction,
	}
}

func (h *Handler) createModelAction(b []byte) (int, any, error) {
	var input CreateModelInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}

	model, err := h.Backend.CreateModel(input)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusCreated, model, nil
}

func (h *Handler) getModelAction(b []byte) (int, any, error) {
	var input getModelInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	m, err := h.Backend.GetModel(input.RestAPIID, input.ModelName)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, m, nil
}

func (h *Handler) getModelsAction(b []byte) (int, any, error) {
	var input getModelsInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	ms, err := h.Backend.GetModels(input.RestAPIID)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, map[string]any{keyItem: ms}, nil
}

func (h *Handler) deleteModelAction(b []byte) (int, any, error) {
	var input deleteModelInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	if err := h.Backend.DeleteModel(input.RestAPIID, input.ModelName); err != nil {
		return 0, nil, err
	}

	return http.StatusAccepted, map[string]any{}, nil
}

func (h *Handler) updateModelAction(b []byte) (int, any, error) {
	var input updateModelInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	m, err := h.Backend.UpdateModel(input.RestAPIID, input.ModelName, input.UpdateModelInput)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, m, nil
}

func (h *Handler) getModelTemplateAction(b []byte) (int, any, error) {
	var params struct {
		RestAPIID string `json:"restApiId"`
		ModelName string `json:"modelName"`
	}
	if err := json.Unmarshal(b, &params); err != nil {
		return 0, nil, err
	}
	out, err := h.Backend.GetModelTemplate(params.RestAPIID, params.ModelName)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, map[string]any{"value": out}, nil
}
