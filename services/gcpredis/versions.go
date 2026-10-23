package gcpredis

import (
	"strconv"
	"strings"
)

const defaultVersion = "REDIS_7_0"

// supportedVersions lists the supported Redis versions, oldest first.
func supportedVersions() []string {
	return []string{"REDIS_3_2", "REDIS_4_0", "REDIS_5_0", "REDIS_6_X", "REDIS_7_0", "REDIS_7_2"}
}

// versionRank returns the position of v in the supported versions, or -1.
func versionRank(v string) int {
	for i, s := range supportedVersions() {
		if s == v {
			return i
		}
	}

	return -1
}

// majorVersion returns N for REDIS_N_x, for the "Redis N and newer" config gates.
func majorVersion(v string) int {
	first, _, _ := strings.Cut(strings.TrimPrefix(v, "REDIS_"), "_")

	n, err := strconv.Atoi(first)
	if err != nil {
		return 0
	}

	return n
}
