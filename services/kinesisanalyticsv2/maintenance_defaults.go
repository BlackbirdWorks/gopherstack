package kinesisanalyticsv2

import (
	"slices"
	"strings"
)

// defaultMaintenanceStart returns the documented per-region maintenance window start (UTC) for
// Flink runtimes, or "" (SQL runtimes, unlisted regions). Source:
// docs.aws.amazon.com/managed-flink/latest/java/maintenance.html; eu-west-2 is omitted because
// that table lists it twice with different times.
func defaultMaintenanceStart(region, runtimeEnv string) string {
	if strings.HasPrefix(runtimeEnv, "SQL") {
		return ""
	}

	starts := map[string][]string{
		"03:00": {"us-gov-east-1", "us-east-1", "us-east-2", "ca-central-1"},
		"06:00": {"us-gov-west-1", "us-west-1", "us-west-2", "eu-central-1"},
		"12:00": {"ap-southeast-2"},
		"13:00": {"ap-east-1", "ap-northeast-1", "ap-northeast-2", "cn-north-1", "cn-northwest-1", "me-south-1"},
		"14:00": {"ap-southeast-1"},
		"15:00": {"ap-southeast-3"},
		"16:30": {"ap-south-1", "ap-south-2"},
		"18:00": {"me-central-1"},
		"19:00": {"sa-east-1"},
		"20:00": {"eu-central-2", "af-south-1", "il-central-1"},
		"21:00": {"eu-south-1", "eu-south-2"},
		"22:00": {"eu-west-1"},
		"23:00": {"eu-west-3", "eu-north-1"},
	}

	for start, regions := range starts {
		if slices.Contains(regions, region) {
			return start
		}
	}

	return ""
}
