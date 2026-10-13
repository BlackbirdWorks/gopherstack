package apigatewayv2

import (
	"encoding/json"
	"slices"
	"strconv"
	"strings"

	"github.com/golang-jwt/jwt/v5"
	"github.com/labstack/echo/v5"
)

const authorizerCtxKey = "apigwv2.authorizer"

func setAuthorizerContext(c *echo.Context, authz map[string]any) {
	if len(authz) > 0 {
		c.Set(authorizerCtxKey, authz)
	}
}

func authorizerContext(c *echo.Context) map[string]any {
	authz, _ := c.Get(authorizerCtxKey).(map[string]any)

	return authz
}

// jwtAuthorizerContext builds requestContext.authorizer for a JWT authorizer:
// claims stringified, scopes from the scope or scp claim.
func jwtAuthorizerContext(claims jwt.MapClaims) map[string]any {
	strClaims := make(map[string]string, len(claims))
	for k, v := range claims {
		strClaims[k] = stringifyClaim(v)
	}

	scopes := tokenScopes(claims)

	return map[string]any{"jwt": map[string]any{"claims": strClaims, "scopes": scopes}}
}

func stringifyClaim(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case float64:
		return strconv.FormatInt(int64(t), 10)
	case []any:
		parts := make([]string, 0, len(t))
		for _, e := range t {
			parts = append(parts, stringifyClaim(e))
		}

		return "[" + strings.Join(parts, " ") + "]"
	default:
		b, _ := json.Marshal(t)

		return string(b)
	}
}

func tokenScopes(claims jwt.MapClaims) []string {
	if v, ok := claims["scope"].(string); ok {
		return strings.Fields(v)
	}

	switch v := claims["scp"].(type) {
	case string:
		return strings.Fields(v)
	case []any:
		out := make([]string, 0, len(v))
		for _, e := range v {
			if s, ok := e.(string); ok {
				out = append(out, s)
			}
		}

		return out
	}

	return []string{}
}

// routeScopesSatisfied reports whether the token carries at least one of the
// route's AuthorizationScopes (always true when the route lists none).
func routeScopesSatisfied(claims jwt.MapClaims, required []string) bool {
	if len(required) == 0 {
		return true
	}

	have := tokenScopes(claims)

	return slices.ContainsFunc(required, func(s string) bool { return slices.Contains(have, s) })
}

// lambdaAuthorizerContext extracts the "context" object a Lambda authorizer
// response returns and shapes it as requestContext.authorizer.lambda.
func lambdaAuthorizerContext(respBytes []byte) map[string]any {
	var resp struct {
		Context map[string]any `json:"context"`
	}
	if err := json.Unmarshal(respBytes, &resp); err != nil || len(resp.Context) == 0 {
		return nil
	}

	return map[string]any{"lambda": resp.Context}
}
