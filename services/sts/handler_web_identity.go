package sts

import (
	"fmt"
	"net/http"
	"strconv"
)

// dispatchAssumeRoleWithWebIdentity handles the AssumeRoleWithWebIdentity action.
func (h *Handler) dispatchAssumeRoleWithWebIdentity(
	r *http.Request,
) (*AssumeRoleWithWebIdentityResponse, error) {
	input := &AssumeRoleWithWebIdentityInput{
		RoleArn:          r.FormValue("RoleArn"),
		RoleSessionName:  r.FormValue("RoleSessionName"),
		WebIdentityToken: r.FormValue("WebIdentityToken"),
		ProviderID:       r.FormValue("ProviderId"),
		Policy:           r.FormValue("Policy"),
		SourceIdentity:   r.FormValue("SourceIdentity"),
		Tags:             parseSessionTags(r),
	}

	durationStr := r.FormValue("DurationSeconds")
	if durationStr != "" {
		d, err := strconv.ParseInt(durationStr, 10, 32)
		if err != nil {
			return nil, ErrInvalidDuration
		}

		input.DurationSeconds = int32(d)
	}

	// Parse policy ARNs: PolicyArns.member.N.arn
	for i := 1; i <= MaxPolicyArnsCount+1; i++ {
		arn := r.FormValue(fmt.Sprintf("PolicyArns.member.%d.arn", i))
		if arn == "" {
			break
		}

		input.PolicyArns = append(input.PolicyArns, arn)
	}

	return h.Backend.AssumeRoleWithWebIdentity(input)
}

// dispatchGetWebIdentityToken handles the GetWebIdentityToken action.
func (h *Handler) dispatchGetWebIdentityToken(
	r *http.Request,
) (*GetWebIdentityTokenResponse, error) {
	input := &GetWebIdentityTokenInput{
		SigningAlgorithm: r.FormValue("SigningAlgorithm"),
		Tags:             parseSessionTags(r),
	}

	// Parse Audience list: Audience.member.N
	for i := 1; i <= MaxAudienceCount; i++ {
		aud := r.FormValue(fmt.Sprintf("Audience.member.%d", i))
		if aud == "" {
			break
		}

		input.Audience = append(input.Audience, aud)
	}

	durationStr := r.FormValue("DurationSeconds")
	if durationStr != "" {
		d, err := strconv.ParseInt(durationStr, 10, 32)
		if err != nil {
			return nil, ErrInvalidDuration
		}

		input.DurationSeconds = int32(d)
	}

	return h.Backend.GetWebIdentityToken(input)
}
