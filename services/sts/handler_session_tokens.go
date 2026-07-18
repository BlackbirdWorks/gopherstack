package sts

import (
	"net/http"
	"strconv"
)

// dispatchGetSessionToken handles the GetSessionToken action.
func (h *Handler) dispatchGetSessionToken(r *http.Request) (*GetSessionTokenResponse, error) {
	input := &GetSessionTokenInput{
		SerialNumber: r.FormValue("SerialNumber"),
		TokenCode:    r.FormValue("TokenCode"),
	}

	durationStr := r.FormValue("DurationSeconds")
	if durationStr != "" {
		d, err := strconv.ParseInt(durationStr, 10, 32)
		if err != nil {
			return nil, ErrInvalidDuration
		}

		input.DurationSeconds = int32(d)
	}

	return h.Backend.GetSessionToken(input)
}
