package apigateway

import (
	"encoding/json"
	"net/http"
	"sort"
)

const opUpdateGatewayResponse = "UpdateGatewayResponse"

type getGatewayResponseInput struct {
	RestAPIID    string `json:"restApiId"`
	ResponseType string `json:"responseType"`
}

type getGatewayResponsesInput struct {
	RestAPIID string `json:"restApiId"`
	Position  string `json:"position"`
	Limit     int    `json:"limit"`
}

// gatewayResponseActions returns the action map for gateway response CRUD operations.
func (h *Handler) gatewayResponseActions() map[string]actionFn {
	return map[string]actionFn{
		opGetGatewayResponse:    h.getGatewayResponseAction,
		opGetGatewayResponses:   h.getGatewayResponsesAction,
		opPutGatewayResponse:    h.putGatewayResponseAction,
		opUpdateGatewayResponse: h.updateGatewayResponseAction,
		opDeleteGatewayResponse: h.deleteGatewayResponseAction,
	}
}

func (h *Handler) getGatewayResponseAction(b []byte) (int, any, error) {
	var params getGatewayResponseInput
	if err := json.Unmarshal(b, &params); err != nil {
		return 0, nil, err
	}
	out, err := h.Backend.GetGatewayResponse(params.RestAPIID, params.ResponseType)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, out, nil
}

func (h *Handler) getGatewayResponsesAction(b []byte) (int, any, error) {
	var params getGatewayResponsesInput
	if err := json.Unmarshal(b, &params); err != nil {
		return 0, nil, err
	}
	out, err := h.Backend.GetGatewayResponses(params.RestAPIID)
	if err != nil {
		return 0, nil, err
	}
	if params.Limit == 0 && params.Position == "" {
		return http.StatusOK, map[string]any{keyItem: out}, nil
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ResponseType < out[j].ResponseType })
	page, position := paginatePageByKey(out, params.Limit, params.Position,
		func(g GatewayResponse) string { return g.ResponseType })
	if position != "" {
		return http.StatusOK, map[string]any{keyItem: page, keyPosition: position}, nil
	}

	return http.StatusOK, map[string]any{keyItem: page}, nil
}

func (h *Handler) putGatewayResponseAction(b []byte) (int, any, error) {
	var input PutGatewayResponseInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	out, err := h.Backend.PutGatewayResponse(input)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusCreated, out, nil
}

func (h *Handler) updateGatewayResponseAction(b []byte) (int, any, error) {
	var input PutGatewayResponseInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}

	gr, err := h.Backend.UpdateGatewayResponse(input)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, gr, nil
}

func (h *Handler) deleteGatewayResponseAction(b []byte) (int, any, error) {
	var params getGatewayResponseInput
	if err := json.Unmarshal(b, &params); err != nil {
		return 0, nil, err
	}
	if err := h.Backend.DeleteGatewayResponse(params.RestAPIID, params.ResponseType); err != nil {
		return 0, nil, err
	}

	return http.StatusNoContent, nil, nil
}
