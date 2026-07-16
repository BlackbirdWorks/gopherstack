package apigateway

import (
	"encoding/json"
	"net/http"
)

const opUpdateGatewayResponse = "UpdateGatewayResponse"

// gatewayResponseActions returns the action map for gateway response CRUD operations.
func (h *Handler) gatewayResponseActions() map[string]actionFn {
	return map[string]actionFn{
		opGetGatewayResponse: func(b []byte) (int, any, error) {
			var params struct {
				RestAPIID    string `json:"restApiId"`
				ResponseType string `json:"responseType"`
			}
			if err := json.Unmarshal(b, &params); err != nil {
				return 0, nil, err
			}
			out, err := h.Backend.GetGatewayResponse(params.RestAPIID, params.ResponseType)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, out, nil
		},
		opGetGatewayResponses: func(b []byte) (int, any, error) {
			var params struct {
				RestAPIID string `json:"restApiId"`
			}
			if err := json.Unmarshal(b, &params); err != nil {
				return 0, nil, err
			}
			out, err := h.Backend.GetGatewayResponses(params.RestAPIID)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, map[string]any{keyItem: out}, nil
		},
		opPutGatewayResponse: func(b []byte) (int, any, error) {
			var input PutGatewayResponseInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			out, err := h.Backend.PutGatewayResponse(input)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusCreated, out, nil
		},
		opUpdateGatewayResponse: func(b []byte) (int, any, error) {
			var input PutGatewayResponseInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}

			gr, err := h.Backend.UpdateGatewayResponse(input)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, gr, nil
		},
		opDeleteGatewayResponse: func(b []byte) (int, any, error) {
			var params struct {
				RestAPIID    string `json:"restApiId"`
				ResponseType string `json:"responseType"`
			}
			if err := json.Unmarshal(b, &params); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.DeleteGatewayResponse(params.RestAPIID, params.ResponseType); err != nil {
				return 0, nil, err
			}

			return http.StatusNoContent, nil, nil
		},
	}
}
