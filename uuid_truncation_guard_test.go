package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

var uuidTruncation = regexp.MustCompile(`uuid\.(?:NewString\(\)|New\(\)\.String\(\))\[:([A-Za-z0-9_]+)\]`)

// maxPureHexTruncation is the longest UUID prefix with no hyphen (8-4-4-4-12 layout).
const maxPureHexTruncation = 8

type truncationAllowance struct {
	reason string
	count  int
}

// hyphenatedTruncations lists files that may slice a UUID string past its first
// hyphen (the result then contains "-"), keyed by path with the permitted count.
func hyphenatedTruncations() map[string]truncationAllowance {
	return map[string]truncationAllowance{
		"services/rds/engine.go":               {"Docker container name, hyphen is legal", 1},
		"services/kinesis/channel_delivery.go": {"S3 object key suffix, not an AWS-pattern ID", 1},
		"services/iot/audit.go":                {"taskId pattern is [a-zA-Z0-9\\-]+, hyphen is legal", 1},
	}
}

// pureHexTruncationDirs lists services whose short-hex (<= 8 chars, no hyphen)
// truncations were reviewed. A short hex tail satisfies every pattern checked;
// the 8-vs-17 digit length is a fidelity gap, not an SDK-pattern violation.
func pureHexTruncationDirs() map[string]string {
	return map[string]string{
		"services/accessanalyzer":  "analyzer name suffix, free-form name",
		"services/awsconfig":       "connector name suffix, free-form name",
		"services/backup":          "job IDs, free-form strings",
		"services/cloudformation":  "physical resource name suffixes, free-form",
		"services/codeartifact":    "Revision and bucket-name suffix; revision is \\S+ (1-50)",
		"services/ec2":             "EC2 IDs accept 8 or 17 hex digits",
		"services/efs":             "fs-/fsap-/fsmt- patterns are [0-9a-f]{8,40}",
		"services/eks":             "container name suffix",
		"services/glue":            "Glue IDs have no charset pattern, length 1-255",
		"services/kafka":           "container name suffix",
		"services/lambda":          "container name and log-stream suffix",
		"services/medialive":       "internal sub-resource IDs",
		"services/mq":              "broker container name suffix",
		"services/opsworks":        "hostname suffix",
		"services/route53resolver": "resolver IDs are 1-64 chars, lowercase hex is legal",
		"services/support":         "case ID is a free-form string",
	}
}

// TestNoUndeclaredUUIDTruncation guards the bug class of hand-built IDs made by
// slicing a UUID string: past 8 chars the slice carries a hyphen and breaks
// hex/alphanumeric ID patterns (Transfer vpce-, SSM AccessKeyId, ECS task sets).
// Mint a shaped ID with a helper instead, or add a reasoned allowlist entry.
func TestNoUndeclaredUUIDTruncation(t *testing.T) {
	t.Parallel()

	hyphenated := hyphenatedTruncations()
	hexDirs := pureHexTruncationDirs()
	gotHyphenated := map[string]int{}

	var offenders []string

	err := filepath.WalkDir("services", func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			n := d.Name()
			if strings.HasPrefix(n, "azure") || n == "cosmosdb" || n == ".claude" || n == "node_modules" {
				return filepath.SkipDir
			}

			return nil
		}

		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		src, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}

		slashed := filepath.ToSlash(path)
		dir := filepath.ToSlash(filepath.Dir(path))

		for _, m := range uuidTruncation.FindAllStringSubmatch(string(src), -1) {
			n, convErr := strconv.Atoi(m[1])
			if convErr == nil && n <= maxPureHexTruncation {
				if _, ok := hexDirs[dir]; !ok {
					offenders = append(
						offenders,
						slashed+": uuid[:"+m[1]+"] in a dir missing from pureHexTruncationDirs",
					)
				}

				continue
			}

			gotHyphenated[slashed]++
		}

		return nil
	})
	require.NoError(t, err)

	for path, n := range gotHyphenated {
		want, ok := hyphenated[path]
		if !ok || want.count != n {
			offenders = append(offenders, path+": hyphen-bearing uuid truncation not matching hyphenatedTruncations()")
		}
	}

	for path := range hyphenated {
		if gotHyphenated[path] == 0 {
			offenders = append(offenders, path+": stale hyphenatedTruncations() entry")
		}
	}

	require.Empty(t, offenders,
		"UUID-slice IDs: use a shaped-ID helper (hex without hyphens at the AWS length) "+
			"or add a reasoned allowlist entry")
}
