package kinesisanalyticsv2

import (
	"fmt"
	"slices"
)

const maxApplicationNameLen = 128

// runtimeEnvironments mirrors types.RuntimeEnvironment's Values() in the pinned SDK.
//
//nolint:gochecknoglobals // static lookup table, never mutated
var runtimeEnvironments = []string{
	"SQL-1_0", "FLINK-1_6", "FLINK-1_8", "ZEPPELIN-FLINK-1_0", "FLINK-1_11", "FLINK-1_13",
	"ZEPPELIN-FLINK-2_0", "FLINK-1_15", "ZEPPELIN-FLINK-3_0", "FLINK-1_18", "FLINK-1_19",
	"FLINK-1_20", "FLINK-2_2", "FLINK-2_3",
}

// validateCreateApplication checks ApplicationName ([a-zA-Z0-9_.-]+, 1-128), the
// RuntimeEnvironment enum (an empty runtime stays accepted for in-repo callers).
func validateCreateApplication(name, runtimeEnv string) error {
	if name == "" || len(name) > maxApplicationNameLen {
		return fmt.Errorf("%w: ApplicationName must be 1-%d characters", ErrValidation, maxApplicationNameLen)
	}

	for _, r := range name {
		ok := r == '_' || r == '.' || r == '-' || r >= '0' && r <= '9' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z'
		if !ok {
			return fmt.Errorf("%w: ApplicationName must match [a-zA-Z0-9_.-]+", ErrValidation)
		}
	}

	if runtimeEnv != "" && !slices.Contains(runtimeEnvironments, runtimeEnv) {
		return fmt.Errorf("%w: unsupported RuntimeEnvironment %q", ErrValidation, runtimeEnv)
	}

	return nil
}
