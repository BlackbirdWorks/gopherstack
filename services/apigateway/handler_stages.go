package apigateway

import (
	"encoding/json"
	"net/http"
)

type getStagesInput struct {
	RestAPIID string `json:"restApiId"`
}

type getStageInput struct {
	RestAPIID string `json:"restApiId"`
	StageName string `json:"stageName"`
}

type deleteStageInput struct {
	RestAPIID string `json:"restApiId"`
	StageName string `json:"stageName"`
}

type updateStageHandlerInput struct {
	UpdateStageInput
	RestAPIID string `json:"restApiId"`
	StageName string `json:"stageName"`
}

type flushStageCacheInput struct {
	RestAPIID string `json:"restApiId"`
	StageName string `json:"stageName"`
}

// stageActions returns actions for stage operations.
func (h *Handler) stageActions() map[string]actionFn {
	return map[string]actionFn{
		opCreateStage:                h.createStageAction,
		opUpdateStage:                h.updateStageAction,
		opFlushStageCache:            h.flushStageCacheAction,
		opFlushStageAuthorizersCache: h.flushStageAuthorizersCacheAction,
		opGetStages:                  h.getStagesAction,
		opGetStage:                   h.getStageAction,
		opDeleteStage:                h.deleteStageAction,
	}
}

func (h *Handler) createStageAction(b []byte) (int, any, error) {
	var input CreateStageInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}

	stage, err := h.Backend.CreateStage(input)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusCreated, stage, nil
}

func (h *Handler) updateStageAction(b []byte) (int, any, error) {
	var input updateStageHandlerInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	stage, err := h.Backend.UpdateStage(input.RestAPIID, input.StageName, input.UpdateStageInput)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, stage, nil
}

func (h *Handler) flushStageCacheAction(b []byte) (int, any, error) {
	var input flushStageCacheInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	if _, err := h.Backend.GetStage(input.RestAPIID, input.StageName); err != nil {
		return 0, nil, err
	}

	return http.StatusAccepted, map[string]any{}, nil
}

func (h *Handler) flushStageAuthorizersCacheAction(b []byte) (int, any, error) {
	var input flushStageCacheInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	h.authCache.flush()

	return http.StatusAccepted, map[string]any{}, nil
}

func (h *Handler) getStagesAction(b []byte) (int, any, error) {
	var input getStagesInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	stages, err := h.Backend.GetStages(input.RestAPIID)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, map[string]any{keyItem: stages}, nil
}

func (h *Handler) getStageAction(b []byte) (int, any, error) {
	var input getStageInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	stage, err := h.Backend.GetStage(input.RestAPIID, input.StageName)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, stage, nil
}

func (h *Handler) deleteStageAction(b []byte) (int, any, error) {
	var input deleteStageInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	if err := h.Backend.DeleteStage(input.RestAPIID, input.StageName); err != nil {
		return 0, nil, err
	}

	return http.StatusNoContent, map[string]any{}, nil
}
