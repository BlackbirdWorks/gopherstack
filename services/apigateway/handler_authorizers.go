package apigateway

import (
	"encoding/json"
	"net/http"
)

type createAuthorizerInput struct {
	RestAPIID                    string   `json:"restApiId"`
	Name                         string   `json:"name"`
	Type                         string   `json:"type"`
	AuthorizerURI                string   `json:"authorizerUri,omitempty"`
	AuthorizerCredentials        string   `json:"authorizerCredentials,omitempty"`
	IdentitySource               string   `json:"identitySource,omitempty"`
	IdentityValidationExpression string   `json:"identityValidationExpression,omitempty"`
	ProviderARNs                 []string `json:"providerARNs,omitempty"`
	AuthorizerResultTTLInSeconds int      `json:"authorizerResultTtlInSeconds,omitempty"`
}

type getAuthorizerInput struct {
	RestAPIID    string `json:"restApiId"`
	AuthorizerID string `json:"authorizerId"`
}

type getAuthorizersInput struct {
	RestAPIID string `json:"restApiId"`
}

type updateAuthorizerInput struct {
	RestAPIID                    string   `json:"restApiId"`
	AuthorizerID                 string   `json:"authorizerId"`
	Name                         string   `json:"name,omitempty"`
	Type                         string   `json:"type,omitempty"`
	AuthorizerURI                string   `json:"authorizerUri,omitempty"`
	AuthorizerCredentials        string   `json:"authorizerCredentials,omitempty"`
	IdentitySource               string   `json:"identitySource,omitempty"`
	IdentityValidationExpression string   `json:"identityValidationExpression,omitempty"`
	ProviderARNs                 []string `json:"providerARNs,omitempty"`
	AuthorizerResultTTLInSeconds int      `json:"authorizerResultTtlInSeconds,omitempty"`
}

type deleteAuthorizerInput struct {
	RestAPIID    string `json:"restApiId"`
	AuthorizerID string `json:"authorizerId"`
}

func (h *Handler) authorizerActions() map[string]actionFn {
	return map[string]actionFn{
		opCreateAuthorizer:     h.createAuthorizerAction,
		opGetAuthorizer:        h.getAuthorizerAction,
		opGetAuthorizers:       h.getAuthorizersAction,
		opUpdateAuthorizer:     h.updateAuthorizerAction,
		opDeleteAuthorizer:     h.deleteAuthorizerAction,
		opTestInvokeAuthorizer: h.testInvokeAuthorizerAction,
	}
}

func (h *Handler) createAuthorizerAction(b []byte) (int, any, error) {
	var input createAuthorizerInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	auth, err := h.Backend.CreateAuthorizer(input.RestAPIID, CreateAuthorizerInput{
		Name:                         input.Name,
		Type:                         input.Type,
		AuthorizerURI:                input.AuthorizerURI,
		AuthorizerCredentials:        input.AuthorizerCredentials,
		IdentitySource:               input.IdentitySource,
		IdentityValidationExpression: input.IdentityValidationExpression,
		AuthorizerResultTTLInSeconds: input.AuthorizerResultTTLInSeconds,
		ProviderARNs:                 input.ProviderARNs,
	})
	if err != nil {
		return 0, nil, err
	}

	return http.StatusCreated, auth, nil
}

func (h *Handler) getAuthorizerAction(b []byte) (int, any, error) {
	var input getAuthorizerInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	auth, err := h.Backend.GetAuthorizer(input.RestAPIID, input.AuthorizerID)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, auth, nil
}

func (h *Handler) getAuthorizersAction(b []byte) (int, any, error) {
	var input getAuthorizersInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	auths, err := h.Backend.GetAuthorizers(input.RestAPIID)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, map[string]any{keyItem: auths}, nil
}

func (h *Handler) updateAuthorizerAction(b []byte) (int, any, error) {
	var input updateAuthorizerInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	auth, err := h.Backend.UpdateAuthorizer(input.RestAPIID, input.AuthorizerID, UpdateAuthorizerInput{
		Name:                         input.Name,
		Type:                         input.Type,
		AuthorizerURI:                input.AuthorizerURI,
		AuthorizerCredentials:        input.AuthorizerCredentials,
		IdentitySource:               input.IdentitySource,
		IdentityValidationExpression: input.IdentityValidationExpression,
		AuthorizerResultTTLInSeconds: input.AuthorizerResultTTLInSeconds,
		ProviderARNs:                 input.ProviderARNs,
	})
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, auth, nil
}

func (h *Handler) deleteAuthorizerAction(b []byte) (int, any, error) {
	var input deleteAuthorizerInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	if err := h.Backend.DeleteAuthorizer(input.RestAPIID, input.AuthorizerID); err != nil {
		return 0, nil, err
	}

	return http.StatusNoContent, map[string]any{}, nil
}

func (h *Handler) testInvokeAuthorizerAction(b []byte) (int, any, error) {
	var input TestInvokeAuthorizerInput
	if err := json.Unmarshal(b, &input); err != nil {
		return 0, nil, err
	}
	out, err := h.Backend.TestInvokeAuthorizer(input)
	if err != nil {
		return 0, nil, err
	}

	return http.StatusOK, out, nil
}
