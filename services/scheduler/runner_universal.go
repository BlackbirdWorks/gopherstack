package scheduler

import (
	"context"
	"fmt"
	"strings"
)

const universalTargetPrefix = "arn:aws:scheduler:::aws-sdk:"

// UniversalTargetInvoker calls an AWS API on behalf of an "aws-sdk" universal target, with input as the
// JSON request parameters, assuming roleARN.
type UniversalTargetInvoker interface {
	InvokeUniversalTarget(ctx context.Context, region, roleARN, service, action string, input []byte) error
}

// SetUniversalTargetInvoker installs the invoker for aws-sdk universal targets.
func (r *Runner) SetUniversalTargetInvoker(u UniversalTargetInvoker) { r.universal = u }

// parseUniversalTarget splits arn:aws:scheduler:::aws-sdk:{service}:{action}.
func parseUniversalTarget(arn string) (string, string, bool) {
	rest, ok := strings.CutPrefix(arn, universalTargetPrefix)
	if !ok {
		return "", "", false
	}

	service, action, ok := strings.Cut(rest, ":")
	if !ok || service == "" || action == "" {
		return "", "", false
	}

	return service, action, true
}

func (r *Runner) invokeUniversalTarget(ctx context.Context, s *Schedule) error {
	if r.universal == nil {
		return fmt.Errorf("%w: universal target %q", errTargetUnwired, s.Target.ARN)
	}

	service, action, ok := parseUniversalTarget(s.Target.ARN)
	if !ok {
		return fmt.Errorf("%w: %q", errMalformedUniversalTarget, s.Target.ARN)
	}

	input := []byte(s.Target.Input)
	if len(input) == 0 {
		input = []byte("{}")
	}

	return r.universal.InvokeUniversalTarget(ctx, s.Region, s.Target.RoleARN, service, action, input)
}
