package sts

import (
	"fmt"
	"net/http"
	"strconv"
)

// dispatchGetFederationToken handles the GetFederationToken action.
func (h *Handler) dispatchGetFederationToken(r *http.Request) (*GetFederationTokenResponse, error) {
	input := &GetFederationTokenInput{
		Name:   r.FormValue("Name"),
		Policy: r.FormValue("Policy"),
		Tags:   parseSessionTags(r),
	}

	// Parse policy ARNs: PolicyArns.member.N.arn
	for i := 1; i <= MaxPolicyArnsCount+1; i++ {
		arnVal := r.FormValue(fmt.Sprintf("PolicyArns.member.%d.arn", i))
		if arnVal == "" {
			break
		}

		input.PolicyArns = append(input.PolicyArns, arnVal)
	}

	durationStr := r.FormValue("DurationSeconds")
	if durationStr != "" {
		d, err := strconv.ParseInt(durationStr, 10, 32)
		if err != nil {
			return nil, ErrInvalidDuration
		}

		input.DurationSeconds = int32(d)
	}

	return h.Backend.GetFederationToken(input)
}
