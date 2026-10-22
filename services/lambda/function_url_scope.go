package lambda

import (
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/services/iam"
)

const functionURLInvokeAction = "lambda:InvokeFunctionUrl"

// IAMRequestScope scopes a function URL request to its function's resource policy.
// AWS_IAM URLs honour grants from either policy type; NONE URLs need a public
// lambda:InvokeFunctionUrl statement conditioned on FunctionUrlAuthType NONE.
func (h *Handler) IAMRequestScope(r *http.Request) (iam.RequestScope, bool) {
	if !IsFunctionURLHost(r.Host) {
		return iam.RequestScope{}, false
	}

	bk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return iam.RequestScope{}, false
	}

	cfg := bk.FunctionURLConfigForHost(r.Host)
	if cfg == nil {
		return iam.RequestScope{}, false
	}

	return iam.RequestScope{
		Action:      functionURLInvokeAction,
		ResourceARN: cfg.FunctionArn,
		Extra: map[string]string{
			"lambda:FunctionUrlAuthType":   cfg.AuthType,
			"lambda:InvokedViaFunctionUrl": strconv.FormatBool(true),
		},
		Anonymous:      cfg.AuthType == functionURLAuthNone,
		MatchPrincipal: true,
		Deny:           denyFunctionURL,
	}, true
}

func denyFunctionURL(c *echo.Context) error {
	w := c.Response()
	w.Header().Set("X-Amzn-Errortype", "AccessDeniedException")
	writeFunctionURLError(w, http.StatusForbidden, functionURLForbiddenMsg)

	return nil
}
