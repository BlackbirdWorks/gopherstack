package sts

import (
	"net/http"
	"strconv"
)

// dispatchGetDelegatedAccessToken handles the GetDelegatedAccessToken action.
func (h *Handler) dispatchGetDelegatedAccessToken(
	r *http.Request,
) (*GetDelegatedAccessTokenResponse, error) {
	input := &GetDelegatedAccessTokenInput{
		TradeInToken: r.FormValue("TradeInToken"),
	}

	durationStr := r.FormValue("DurationSeconds")
	if durationStr != "" {
		d, err := strconv.ParseInt(durationStr, 10, 32)
		if err != nil {
			return nil, ErrInvalidDuration
		}

		input.DurationSeconds = int32(d)
	}

	return h.Backend.GetDelegatedAccessToken(input)
}
