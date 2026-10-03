package apigateway

import (
	"encoding/json"
	"net/http"
)

type createRestAPIInput = CreateRestAPIInput

type deleteRestAPIInput struct {
	RestAPIID string `json:"restApiId"`
}

type getRestAPIInput struct {
	RestAPIID string `json:"restApiId"`
}

type getRestApisInput struct {
	Position string `json:"position"`
	Limit    int    `json:"limit"`
}

type updateRestAPIHandlerInput struct {
	RestAPIID string `json:"restApiId"`
	UpdateRestAPIInput
}

func (h *Handler) restAPIActions() map[string]actionFn {
	return map[string]actionFn{
		opCreateRestAPI: h.createRestAPIAction,
		opDeleteRestAPI: h.deleteRestAPIAction,
		opGetRestAPI:    h.getRestAPIAction,
		opGetRestApis:   h.getRestAPIsAction,
		opUpdateRestAPI: h.updateRestAPIAction,
	}
}

func (h *Handler) createRestAPIAction(b []byte) (int, any, error) {
	var input createRestAPIInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	api, err := h.Backend.CreateRestAPI(input)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusCreated, toWireRestAPI(api), nil
}

func (h *Handler) deleteRestAPIAction(b []byte) (int, any, error) {
	var input deleteRestAPIInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	// Deployment IDs are freshly random and never reused, so the trie cache
	// (now keyed by deploymentID -- see routingTrie's doc) never overwrites a
	// stale entry on its own; evict every deployment this API owned before
	// they're gone from the backend and can no longer be listed.
	depls, _ := h.Backend.GetDeployments(input.RestAPIID)

	if err := h.Backend.DeleteRestAPI(input.RestAPIID); err != nil {
		return 0, nil, err
	}

	for _, d := range depls {
		h.trieCache.Delete(d.ID)
	}

	return http.StatusAccepted, map[string]any{}, nil
}

func (h *Handler) getRestAPIAction(b []byte) (int, any, error) {
	var input getRestAPIInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	api, err := h.Backend.GetRestAPI(input.RestAPIID)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, toWireRestAPI(api), nil
}

func (h *Handler) getRestAPIsAction(b []byte) (int, any, error) {
	var input getRestApisInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	apis, position, err := h.Backend.GetRestAPIs(input.Limit, input.Position)
	if err != nil {
		return 0, nil, err
	}

	wireAPIs := toWireRestAPIs(apis)
	if position != "" {
		return http.StatusOK, map[string]any{keyItem: wireAPIs, keyPosition: position}, nil
	}

	return http.StatusOK, map[string]any{keyItem: wireAPIs}, nil
}

func (h *Handler) updateRestAPIAction(b []byte) (int, any, error) {
	var input updateRestAPIHandlerInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	api, err := h.Backend.UpdateRestAPI(input.RestAPIID, input.UpdateRestAPIInput)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, toWireRestAPI(api), nil
}
