package sts

import (
	"net/http"
	"strconv"
)

// dispatchAssumeRoot handles the AssumeRoot action.
func (h *Handler) dispatchAssumeRoot(r *http.Request) (*AssumeRootResponse, error) {
	taskPolicyArn := r.FormValue("TaskPolicyArn.arn")
	if taskPolicyArn == "" {
		// Fall back to the flat key for backward compatibility with direct-form callers.
		taskPolicyArn = r.FormValue("TaskPolicyArn")
	}

	input := &AssumeRootInput{
		TargetPrincipal: r.FormValue("TargetPrincipal"),
		TaskPolicyArn:   taskPolicyArn,
	}

	durationStr := r.FormValue("DurationSeconds")
	if durationStr != "" {
		d, err := strconv.ParseInt(durationStr, 10, 32)
		if err != nil {
			return nil, ErrInvalidDuration
		}

		input.DurationSeconds = int32(d)
	}

	// Resolve the caller's own STS session (if any) so the backend can
	// propagate its SourceIdentity, per AWS's documented "persists across
	// chained role sessions" behavior (there is no separate SourceIdentity
	// request parameter for AssumeRoot).
	if callerKey := extractAccessKeyFromAuth(r); callerKey != "" {
		secToken := r.Header.Get("X-Amz-Security-Token")
		input.CallerSession = h.Backend.LookupSession(callerKey, secToken)
	}

	return h.Backend.AssumeRoot(input)
}
