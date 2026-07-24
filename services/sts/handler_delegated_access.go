package sts

import (
	"net/http"
)

// dispatchGetDelegatedAccessToken handles the GetDelegatedAccessToken action.
// GetDelegatedAccessTokenInput has only one request member (TradeInToken) in the
// real AWS API — there is no DurationSeconds parameter for this operation.
func (h *Handler) dispatchGetDelegatedAccessToken(
	r *http.Request,
) (*GetDelegatedAccessTokenResponse, error) {
	input := &GetDelegatedAccessTokenInput{
		TradeInToken: r.FormValue("TradeInToken"),
	}

	return h.Backend.GetDelegatedAccessToken(input)
}
