package apigateway

import (
	"encoding/json"
	"net/http"
)

// parseAPIGWAccountPath handles /account paths.
func parseAPIGWAccountPath(method string, _ []string, n int) (string, map[string]string, bool) {
	// GET /account → GetAccount
	if n == 1 && method == http.MethodGet {
		return opGetAccount, nil, true
	}
	// PATCH /account → UpdateAccount
	if n == 1 && method == http.MethodPatch {
		return opUpdateAccount, nil, true
	}

	return apiGWUnknownOp, nil, false
}

// accountActions returns the action map for the Account get/update operations.
func (h *Handler) accountActions() map[string]actionFn {
	return map[string]actionFn{
		opGetAccount: func(_ []byte) (int, any, error) {
			acct, err := h.Backend.GetAccount()
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, acct, nil
		},
		opUpdateAccount: func(b []byte) (int, any, error) {
			var input UpdateAccountInput
			if err := json.Unmarshal(b, &input); err != nil {
				return 0, nil, err
			}
			out, err := h.Backend.UpdateAccount(input)
			if err != nil {
				return 0, nil, err
			}

			return http.StatusOK, out, nil
		},
	}
}
