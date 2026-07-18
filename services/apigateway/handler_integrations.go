package apigateway

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type putIntegrationInput struct {
	RestAPIID  string `json:"restApiId"`
	ResourceID string `json:"resourceId"`
	HTTPMethod string `json:"httpMethod"`
	PutIntegrationInput
}

type getIntegrationInput struct {
	RestAPIID  string `json:"restApiId"`
	ResourceID string `json:"resourceId"`
	HTTPMethod string `json:"httpMethod"`
}

type deleteIntegrationInput struct {
	RestAPIID  string `json:"restApiId"`
	ResourceID string `json:"resourceId"`
	HTTPMethod string `json:"httpMethod"`
}

type putIntegrationResponseInput struct {
	PutIntegrationResponseInput

	RestAPIID  string `json:"restApiId"`
	ResourceID string `json:"resourceId"`
	HTTPMethod string `json:"httpMethod"`
	StatusCode string `json:"statusCode"`
}

type getIntegrationResponseInput struct {
	RestAPIID  string `json:"restApiId"`
	ResourceID string `json:"resourceId"`
	HTTPMethod string `json:"httpMethod"`
	StatusCode string `json:"statusCode"`
}

type deleteIntegrationResponseInput struct {
	RestAPIID  string `json:"restApiId"`
	ResourceID string `json:"resourceId"`
	HTTPMethod string `json:"httpMethod"`
	StatusCode string `json:"statusCode"`
}

// parseAPIGWIntegrationPath routes integration paths.
func parseAPIGWIntegrationPath(
	method string,
	segs []string,
	idxInteg, idxResp int,
	apiID, resID, httpMethod string,
	baseParams map[string]string,
) (string, map[string]string, bool) {
	if len(segs) > idxResp && segs[idxResp] == apiGWSegResponses {
		if len(segs) <= idxResp+1 {
			return apiGWUnknownOp, nil, false
		}

		params := map[string]string{
			keyRestAPIID:  apiID,
			keyResourceID: resID,
			keyHTTPMethod: httpMethod,
			keyStatusCode: segs[idxResp+1],
		}

		switch method {
		case http.MethodPut:
			return opPutIntegrationResponse, params, true
		case http.MethodGet:
			return opGetIntegrationResponse, params, true
		case http.MethodDelete:
			return opDeleteIntegrationResponse, params, true
		case http.MethodPatch:
			return opUpdateIntegrationResponse, params, true
		}

		return apiGWUnknownOp, nil, false
	}

	switch method {
	case http.MethodPut:
		return opPutIntegration, baseParams, true
	case http.MethodGet:
		return opGetIntegration, baseParams, true
	case http.MethodDelete:
		return opDeleteIntegration, baseParams, true
	case http.MethodPatch:
		return opUpdateIntegration, baseParams, true
	}

	_ = idxInteg

	return apiGWUnknownOp, nil, false
}

//nolint:dupl // integrationResponseActions and methodResponseActions have similar structure by design
func (h *Handler) integrationResponseActions() map[string]actionFn {
	return map[string]actionFn{
		opPutIntegrationResponse: func(b []byte) (int, any, error) {
			var input putIntegrationResponseInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			ir, err := h.Backend.PutIntegrationResponse(
				input.RestAPIID,
				input.ResourceID,
				input.HTTPMethod,
				input.StatusCode,
				input.PutIntegrationResponseInput,
			)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusCreated, ir, nil
		},
		opGetIntegrationResponse: func(b []byte) (int, any, error) {
			var input getIntegrationResponseInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			ir, err := h.Backend.GetIntegrationResponse(
				input.RestAPIID,
				input.ResourceID,
				input.HTTPMethod,
				input.StatusCode,
			)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, ir, nil
		},
		opDeleteIntegrationResponse: func(b []byte) (int, any, error) {
			var input deleteIntegrationResponseInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.DeleteIntegrationResponse(
				input.RestAPIID,
				input.ResourceID,
				input.HTTPMethod,
				input.StatusCode,
			); err != nil {
				return 0, nil, err
			}

			return http.StatusNoContent, map[string]any{}, nil
		},
		opUpdateIntegrationResponse: func(b []byte) (int, any, error) {
			var input UpdateIntegrationResponseInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			out, err := h.Backend.UpdateIntegrationResponse(input)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, out, nil
		},
	}
}

func (h *Handler) integrationActions() map[string]actionFn {
	return map[string]actionFn{
		opPutIntegration: func(b []byte) (int, any, error) {
			var input putIntegrationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			switch input.Type {
			case IntegrationTypeAWS, IntegrationTypeAWSProxy,
				IntegrationTypeHTTP, IntegrationTypeHTTPProxy, IntegrationTypeMock:
			default:
				return 0, nil, fmt.Errorf(
					"%w: invalid integration type %q; must be AWS, AWS_PROXY, HTTP, HTTP_PROXY, or MOCK",
					ErrInvalidParameter, input.Type,
				)
			}
			integ, err := h.Backend.PutIntegration(
				input.RestAPIID,
				input.ResourceID,
				input.HTTPMethod,
				input.PutIntegrationInput,
			)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusCreated, integ, nil
		},
		opGetIntegration: func(b []byte) (int, any, error) {
			var input getIntegrationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			integ, err := h.Backend.GetIntegration(input.RestAPIID, input.ResourceID, input.HTTPMethod)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, integ, nil
		},
		opDeleteIntegration: func(b []byte) (int, any, error) {
			var input deleteIntegrationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			if err := h.Backend.DeleteIntegration(input.RestAPIID, input.ResourceID, input.HTTPMethod); err != nil {
				return 0, nil, err
			}

			return http.StatusNoContent, map[string]any{}, nil
		},
		opUpdateIntegration: func(b []byte) (int, any, error) {
			var input UpdateIntegrationInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			out, err := h.Backend.UpdateIntegration(input)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, out, nil
		},
	}
}
