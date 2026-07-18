package sts

import (
	"fmt"
	"net/http"
	"strconv"
)

// dispatchAssumeRoleWithSAML handles the AssumeRoleWithSAML action.
func (h *Handler) dispatchAssumeRoleWithSAML(r *http.Request) (*AssumeRoleWithSAMLResponse, error) {
	input := &AssumeRoleWithSAMLInput{
		RoleArn:         r.FormValue("RoleArn"),
		PrincipalArn:    r.FormValue("PrincipalArn"),
		SAMLAssertion:   r.FormValue("SAMLAssertion"),
		Policy:          r.FormValue("Policy"),
		RoleSessionName: r.FormValue("RoleSessionName"),
		SourceIdentity:  r.FormValue("SourceIdentity"),
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

	// Parse session tags: Tags.member.N.Key / Tags.member.N.Value
	input.Tags = parseSessionTags(r)

	return h.Backend.AssumeRoleWithSAML(input)
}
