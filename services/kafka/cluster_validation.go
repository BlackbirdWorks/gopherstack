package kafka

import (
	"fmt"
	"regexp"
	"strings"
)

const maxClusterNameLen = 64

var kafkaVersionPattern = regexp.MustCompile(`^[0-9]+\.[0-9]+\.([0-9]+|x)(\.kraft|\.tiered)?$`)

var clusterNamePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]*$`)

func validateClusterName(name string) error {
	if len(name) > maxClusterNameLen || !clusterNamePattern.MatchString(name) {
		return fmt.Errorf(
			"clusterName %q is invalid: must be 1-%d characters of letters, digits, hyphens and underscores: %w",
			name, maxClusterNameLen, ErrValidation,
		)
	}

	return nil
}

func validateCreateClusterInput(name, version, instanceType string) error {
	if err := validateClusterName(name); err != nil {
		return err
	}
	if err := validateKafkaVersion(version); err != nil {
		return err
	}

	return validateBrokerInstanceType(instanceType)
}

func validateKafkaVersion(version string) error {
	if !kafkaVersionPattern.MatchString(version) {
		return fmt.Errorf("kafkaVersion %q is not a valid Apache Kafka version: %w", version, ErrValidation)
	}

	return nil
}

func validateBrokerInstanceType(instanceType string) error {
	if instanceType != "" && !strings.HasPrefix(instanceType, "kafka.") &&
		!strings.HasPrefix(instanceType, "express.") {
		return fmt.Errorf(
			"instanceType %q is invalid: must start with kafka. or express.: %w",
			instanceType,
			ErrValidation,
		)
	}

	return nil
}
