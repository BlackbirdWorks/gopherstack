package iot

import (
	"errors"
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
		HTTPContext *struct {
			Headers     map[string]string `json:"headers"`
			QueryString string            `json:"queryString"`
		} `json:"httpContext"`
		MQTTContext *struct {
			ClientID string `json:"clientId"`
			Username string `json:"username"`
			Password []byte `json:"password"`
		} `json:"mqttContext"`
		TLSContext *struct {
			ServerName string `json:"serverName"`
		} `json:"tlsContext"`
		Token          string `json:"token"`
		TokenSignature string `json:"tokenSignature"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}

	in := &TestInvokeAuthorizerInput{Token: req.Token, TokenSignature: req.TokenSignature}

	if x := req.HTTPContext; x != nil {
		in.HTTP = &AuthorizerHTTPContext{Headers: x.Headers, QueryString: x.QueryString}
	}

	if x := req.MQTTContext; x != nil {
		in.MQTT = &AuthorizerMQTTContext{ClientID: x.ClientID, Username: x.Username, Password: x.Password}
	}

	if x := req.TLSContext; x != nil {
		in.TLS = &AuthorizerTLSContext{ServerName: x.ServerName}
	}

	out, err := h.Backend.TestInvokeAuthorizer(c.Request().Context(), name, in)
	if err != nil {
		return respondAuthorizerInvokeErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		"isAuthenticated":          out.IsAuthenticated,
		"principalId":              out.PrincipalID,
		"policyDocuments":          out.PolicyDocuments,
		"disconnectAfterInSeconds": out.DisconnectAfterInSeconds,
		"refreshAfterInSeconds":    out.RefreshAfterInSeconds,
	})
}

func respondAuthorizerInvokeErr(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrInvalidAuthorizerResponse):
		return c.JSON(http.StatusBadRequest, awsErrBody{"InvalidResponseException", publicMessage(err)})
	case errors.Is(err, ErrActionTargetUnavailable):
		return c.JSON(http.StatusServiceUnavailable, awsErrBody{"ServiceUnavailableException", publicMessage(err)})
	case errors.Is(err, errRuleActionDenied):
		return c.JSON(http.StatusUnauthorized, awsErrBody{"UnauthorizedException", publicMessage(err)})
	}

	return respondErr(c, err)
}
