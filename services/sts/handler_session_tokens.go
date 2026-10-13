package sts

import (
	"fmt"
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

	if err := h.rejectSessionCaller(r, "GetSessionToken"); err != nil {
		return nil, err
	}

	return h.Backend.GetSessionToken(input)
}

// rejectSessionCaller denies callers using temporary credentials, as AWS does for
// GetSessionToken and GetFederationToken.
func (h *Handler) rejectSessionCaller(r *http.Request, op string) error {
	key := extractAccessKeyFromAuth(r)
	if key == "" {
		return nil
	}

	if h.Backend.LookupSession(key, r.Header.Get("X-Amz-Security-Token")) != nil {
		return fmt.Errorf("%w: Cannot call %s with session credentials", ErrAccessDenied, op)
	}

	return nil
}
