package sts

import (
	"fmt"
	"net/http"
	"strconv"
)

// dispatchAssumeRoleWithSAML handles the AssumeRoleWithSAML action.
// RoleSessionName, SourceIdentity, and session tags are NOT real top-level
// request parameters for this operation (see AssumeRoleWithSAMLInput) — AWS
// derives them from the SAMLAssertion itself, so they are intentionally not
// parsed from the request form here.
func (h *Handler) dispatchAssumeRoleWithSAML(r *http.Request) (*AssumeRoleWithSAMLResponse, error) {
	input := &AssumeRoleWithSAMLInput{
		RoleArn:       r.FormValue("RoleArn"),
		PrincipalArn:  r.FormValue("PrincipalArn"),
		SAMLAssertion: r.FormValue("SAMLAssertion"),
		Policy:        r.FormValue("Policy"),
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

	return h.Backend.AssumeRoleWithSAML(input)
}
