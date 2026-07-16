package eventbridge

import "fmt"

const (
	// defaultMaxRetryAttempts matches AWS EventBridge default retry attempts.
	defaultMaxRetryAttempts = 2
	// defaultMaxEventAgeSeconds matches AWS EventBridge default maximum event age (3600s = 1h).
	defaultMaxEventAgeSeconds = 3600
)

// AWS's documented bounds for a target's RetryPolicy (PutTargets API
// reference): MaximumRetryAttempts 0-185, MaximumEventAgeInSeconds 60-86400.
// These are server-side-only constraints (not modeled as smithy range
// traits in the client SDK, so the Go SDK does not reject them locally
// either), so PutTargets must enforce them itself.
const (
	minRetryAttempts   = 0
	maxRetryAttempts   = 185
	minMaxEventAgeSecs = 60
	maxMaxEventAgeSecs = 86400
)

// validateTargetTypeParameters checks the required members of the
// target-type-specific parameter structs (EcsParameters, KinesisParameters,
// RedshiftDataParameters, RunCommandParameters), mirroring the client-side
// constraint validation the real AWS SDK performs before a PutTargets call
// ever reaches the service (see validators.go in aws-sdk-go-v2/eventbridge:
// validateEcsParameters, validateKinesisParameters,
// validateRedshiftDataParameters, validateRunCommandParameters/Target), and
// enforces the RetryPolicy bounds AWS documents for PutTargets.
func validateTargetTypeParameters(t *Target) error {
	if t.EcsParameters != nil && t.EcsParameters.TaskDefinitionArn == "" {
		return fmt.Errorf("%w: EcsParameters.TaskDefinitionArn is required", ErrInvalidParameter)
	}

	if t.KinesisParameters != nil && t.KinesisParameters.PartitionKeyPath == "" {
		return fmt.Errorf("%w: KinesisParameters.PartitionKeyPath is required", ErrInvalidParameter)
	}

	if t.RedshiftDataParameters != nil && t.RedshiftDataParameters.Database == "" {
		return fmt.Errorf("%w: RedshiftDataParameters.Database is required", ErrInvalidParameter)
	}

	if t.RunCommandParameters != nil {
		if err := validateRunCommandParameters(t.RunCommandParameters); err != nil {
			return err
		}
	}

	return validateRetryPolicy(t.RetryPolicy)
}

// validateRetryPolicy enforces AWS's documented RetryPolicy bounds. A zero
// MaximumEventAgeInSeconds is treated as "unset" (the JSON wire form uses
// omitempty, so 0 and absent are indistinguishable) and defaulted downstream
// by deliverToTargetBounded, matching existing delivery behavior; any other
// out-of-range value is rejected.
func validateRetryPolicy(rp *RetryPolicy) error {
	if rp == nil {
		return nil
	}

	if rp.MaximumRetryAttempts < minRetryAttempts || rp.MaximumRetryAttempts > maxRetryAttempts {
		return fmt.Errorf(
			"%w: RetryPolicy.MaximumRetryAttempts must be between %d and %d",
			ErrInvalidParameter, minRetryAttempts, maxRetryAttempts,
		)
	}

	if rp.MaximumEventAgeInSeconds != 0 &&
		(rp.MaximumEventAgeInSeconds < minMaxEventAgeSecs || rp.MaximumEventAgeInSeconds > maxMaxEventAgeSecs) {
		return fmt.Errorf(
			"%w: RetryPolicy.MaximumEventAgeInSeconds must be between %d and %d",
			ErrInvalidParameter, minMaxEventAgeSecs, maxMaxEventAgeSecs,
		)
	}

	return nil
}

// validateRunCommandParameters checks that RunCommandParameters carries at
// least one target and that each target has both a Key and at least one
// Value, matching AWS's required-member constraints for RunCommandTarget.
func validateRunCommandParameters(p *RunCommandParameters) error {
	if len(p.RunCommandTargets) == 0 {
		return fmt.Errorf("%w: RunCommandParameters.RunCommandTargets is required", ErrInvalidParameter)
	}

	for i, rt := range p.RunCommandTargets {
		if rt.Key == "" {
			return fmt.Errorf(
				"%w: RunCommandParameters.RunCommandTargets[%d].Key is required", ErrInvalidParameter, i,
			)
		}

		if len(rt.Values) == 0 {
			return fmt.Errorf(
				"%w: RunCommandParameters.RunCommandTargets[%d].Values is required", ErrInvalidParameter, i,
			)
		}
	}

	return nil
}
