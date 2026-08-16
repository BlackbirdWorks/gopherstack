package sts

import (
	"fmt"
	"net/http"
	"strconv"
)

// dispatchAssumeRole handles the AssumeRole action.
func (h *Handler) dispatchAssumeRole(r *http.Request) (*AssumeRoleResponse, error) {
	input := &AssumeRoleInput{
		RoleArn:         r.FormValue("RoleArn"),
		RoleSessionName: r.FormValue("RoleSessionName"),
		ExternalID:      r.FormValue("ExternalId"),
		Policy:          r.FormValue("Policy"),
		SourceIdentity:  r.FormValue("SourceIdentity"),
		SerialNumber:    r.FormValue("SerialNumber"),
		TokenCode:       r.FormValue("TokenCode"),
	}

	durationStr := r.FormValue("DurationSeconds")
	if durationStr != "" {
		d, err := strconv.ParseInt(durationStr, 10, 32)
		if err != nil {
			return nil, ErrInvalidDuration
		}

		input.DurationSeconds = int32(d)
	}

	// Parse session tags: Tags.member.N.Key / Tags.member.N.Value
	input.Tags = parseSessionTags(r)

	// Parse transitive tag keys: TransitiveTagKeys.member.N
	input.TransitiveTagKeys = parseTransitiveTagKeys(r)

	// Parse policy ARNs: PolicyArns.member.N.arn
	for i := 1; i <= MaxPolicyArnsCount+1; i++ {
		arn := r.FormValue(fmt.Sprintf("PolicyArns.member.%d.arn", i))
		if arn == "" {
			break
		}

		input.PolicyArns = append(input.PolicyArns, arn)
	}

	// Parse provided contexts: ProvidedContexts.member.N.ContextAssertion / ProviderArn
	for i := 1; i <= MaxProvidedContextsCount; i++ {
		provArn := r.FormValue(fmt.Sprintf("ProvidedContexts.member.%d.ProviderArn", i))
		ctxAssertion := r.FormValue(fmt.Sprintf("ProvidedContexts.member.%d.ContextAssertion", i))

		if provArn == "" && ctxAssertion == "" {
			break
		}

		input.ProvidedContexts = append(input.ProvidedContexts, ProvidedContext{
			ProviderArn:      provArn,
			ContextAssertion: ctxAssertion,
		})
	}

	// Extract caller identity to support role-chaining duration cap and transitive tag propagation.
	callerKey := extractAccessKeyFromAuth(r)
	input.CallerAccessKeyID = callerKey
	if callerKey != "" {
		secToken := r.Header.Get("X-Amz-Security-Token")
		input.CallerSession = h.Backend.LookupSession(callerKey, secToken)
		if input.CallerSession != nil {
			// The caller is itself an assumed-role session (role chaining); its
			// ARN is the principal evaluated against the target role's trust policy.
			input.CallerArn = input.CallerSession.AssumedRoleArn
		} else if userArn, err := h.Backend.LookupUserArn(callerKey); err == nil && userArn != "" {
			input.CallerArn = userArn
		}
	}

	return h.Backend.AssumeRole(input)
}
