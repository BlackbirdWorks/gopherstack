package kafkaconnect

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	maxResourceNameLen = 128
	maxListResults     = 100
	maxCPUPercent      = 100
)

func invalidf(format string, args ...any) error {
	return awserr.Newf(format, awserr.ErrInvalidParameter, args...)
}

func validateResourceName(kind, name string) error {
	if len(name) < 1 || len(name) > maxResourceNameLen {
		return invalidf("%s name must be between 1 and %d characters", kind, maxResourceNameLen)
	}

	return nil
}

func validMcuCount(n int32) bool { return n == 1 || n == 2 || n == 4 || n == 8 }

func validateAutoScaling(a *AutoScaling) error {
	switch {
	case !validMcuCount(a.McuCount):
		return invalidf("autoScaling.mcuCount must be one of 1, 2, 4, 8")
	case a.MinWorkerCount < 1 || a.MaxWorkerCount < 1:
		return invalidf("autoScaling.minWorkerCount and maxWorkerCount must be at least 1")
	case a.MinWorkerCount > a.MaxWorkerCount:
		return invalidf("autoScaling.minWorkerCount must not exceed maxWorkerCount")
	case a.MaxAutoscalingTaskCount != 0 && a.MaxAutoscalingTaskCount < a.MaxWorkerCount:
		return invalidf("autoScaling.maxAutoscalingTaskCount must be at least equal to maxWorkerCount")
	case a.ScaleInCPUPercent < 0 || a.ScaleInCPUPercent > maxCPUPercent ||
		a.ScaleOutCPUPercent < 0 || a.ScaleOutCPUPercent > maxCPUPercent:
		return invalidf("cpuUtilizationPercentage must be between 1 and %d", maxCPUPercent)
	}

	return nil
}

func validateCapacity(c Capacity) error {
	switch {
	case c.AutoScaling != nil && c.Provisioned != nil:
		return invalidf("capacity must set exactly one of autoScaling and provisionedCapacity")
	case c.AutoScaling != nil:
		return validateAutoScaling(c.AutoScaling)
	case c.Provisioned != nil:
		if !validMcuCount(c.Provisioned.McuCount) {
			return invalidf("provisionedCapacity.mcuCount must be one of 1, 2, 4, 8")
		}

		if c.Provisioned.WorkerCount < 1 {
			return invalidf("provisionedCapacity.workerCount must be at least 1")
		}

		return nil
	}

	return invalidf("capacity must set one of autoScaling and provisionedCapacity")
}

func validateRoleARN(role string) error {
	const arnParts = 6

	if len(strings.SplitN(role, ":", arnParts)) < arnParts || !strings.HasPrefix(role, "arn:") {
		return invalidf("serviceExecutionRoleArn %q is not a valid ARN", role)
	}

	return nil
}

func validatePropertiesFile(content string) error {
	if _, err := base64.StdEncoding.DecodeString(content); err != nil {
		return invalidf("propertiesFileContent must be base64 encoded")
	}

	return nil
}

// ValidateListParams rejects malformed pagination input.
func ValidateListParams(nextToken, maxResults string) error {
	if err := page.ValidateToken(nextToken); err != nil {
		return invalidf("invalid nextToken")
	}

	if maxResults == "" {
		return nil
	}

	n, err := strconv.Atoi(maxResults)
	if err != nil || n < 1 || n > maxListResults {
		return fmt.Errorf("%w: maxResults must be between 1 and %d", awserr.ErrInvalidParameter, maxListResults)
	}

	return nil
}
