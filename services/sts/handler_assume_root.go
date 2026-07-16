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

	return h.Backend.AssumeRoot(input)
}
