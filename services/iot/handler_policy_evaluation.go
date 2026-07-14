package iot

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleTestAuthorization(c *echo.Context) error {
	var req struct {
		Principal             string     `json:"principal"`
		CognitoIdentityPoolID string     `json:"cognitoIdentityPoolId"`
		ClientID              string     `json:"clientId"`
		PolicyNamesToAdd      []string   `json:"policyNamesToAdd"`
		PolicyNamesToSkip     []string   `json:"policyNamesToSkip"`
		AuthInfos             []AuthInfo `json:"authInfos"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}

	results, err := h.Backend.TestAuthorization(&TestAuthorizationInput{
		Principal:             req.Principal,
		CognitoIdentityPoolID: req.CognitoIdentityPoolID,
		ClientID:              req.ClientID,
		PolicyNamesToAdd:      req.PolicyNamesToAdd,
		PolicyNamesToSkip:     req.PolicyNamesToSkip,
		AuthInfos:             req.AuthInfos,
	})
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"authResults": results})
}

func (h *Handler) handleTestInvokeAuthorizer(c *echo.Context) error {
	name := strings.TrimSuffix(strings.TrimPrefix(c.Request().URL.Path, "/authorizer/"), "/test")

	var req struct {
		Token          string `json:"token"`
		TokenSignature string `json:"tokenSignature"`
		MQTTContext    struct {
			ClientID string `json:"clientId"`
		} `json:"mqttContext"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}

	out, err := h.Backend.TestInvokeAuthorizer(name, &TestInvokeAuthorizerInput{
		Token:          req.Token,
		TokenSignature: req.TokenSignature,
		MQTTClientID:   req.MQTTContext.ClientID,
	})
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"isAuthenticated":          out.IsAuthenticated,
		"principalId":              out.PrincipalID,
		"policyDocuments":          out.PolicyDocuments,
		"disconnectAfterInSeconds": out.DisconnectAfterInSeconds,
		"refreshAfterInSeconds":    out.RefreshAfterInSeconds,
	})
}
