package guards_test

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
		"services/neptune/engine.go":           {"Docker container name, hyphen is legal", 1},
		"services/docdb/engine.go":             {"Docker container name, hyphen is legal", 1},
		"services/redshift/engine.go":          {"Docker container name, hyphen is legal", 1},
		"services/kinesis/channel_delivery.go": {"S3 object key suffix, not an AWS-pattern ID", 1},
		"services/iot/audit.go":                {"taskId pattern is [a-zA-Z0-9\\-]+, hyphen is legal", 1},
	}
}

// pureHexTruncationDirs lists services whose short-hex (<= 8 chars, no hyphen)
// truncations were reviewed. A short hex tail satisfies every pattern checked;
// the 8-vs-17 digit length is a fidelity gap, not an SDK-pattern violation.
func pureHexTruncationDirs() map[string]string {
	return map[string]string{
		"services/accessanalyzer": "analyzer name suffix, free-form name",
		"services/awsconfig":      "connector name suffix, free-form name",
		"services/backup":         "job IDs, free-form strings",
		"services/cloudformation": "physical resource name suffixes, free-form",
		"services/codeartifact":   "Revision and bucket-name suffix; revision is \\S+ (1-50)",
		"services/eks":            "container name suffix",
		"services/glue":           "Glue IDs have no charset pattern, length 1-255",
		"services/kafka":          "container name suffix",
		"services/lambda":         "container name and log-stream suffix",
		"services/medialive":      "internal sub-resource IDs",
		"services/mq":             "broker container name suffix",
		"services/opsworks":       "hostname suffix",
		"services/support":        "case ID is a free-form string",
	}
}

func skipGuardDir(name string) bool {
	return strings.HasPrefix(name, "azure") || name == "cosmosdb" || name == ".claude" || name == "node_modules"
}

func isGuardedSource(path string) bool {
	return strings.HasSuffix(path, ".go") && !strings.HasSuffix(path, "_test.go")
}

// classifyTruncations records hyphen-bearing slices in got and returns offenders
// for short-hex slices in directories missing from hexDirs.
func classifyTruncations(
	slashed, dir string, src []byte, hexDirs map[string]string, got map[string]int,
) []string {
	var offenders []string

	for _, m := range uuidTruncation.FindAllStringSubmatch(string(src), -1) {
		n, convErr := strconv.Atoi(m[1])
		if convErr != nil || n > maxPureHexTruncation {
			got[slashed]++

			continue
		}

		if _, ok := hexDirs[dir]; !ok {
			offenders = append(offenders, slashed+": uuid["+":"+m[1]+"] in a dir missing from pureHexTruncationDirs")
		}
	}

	return offenders
}

func scanUUIDTruncations(t *testing.T, hexDirs map[string]string) ([]string, map[string]int) {
	t.Helper()

	got := map[string]int{}

	var offenders []string

	err := filepath.WalkDir("services", func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			if skipGuardDir(d.Name()) {
				return filepath.SkipDir
			}

			return nil
		}

		if !isGuardedSource(path) {
			return nil
		}

		src, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}

		offenders = append(offenders,
			classifyTruncations(filepath.ToSlash(path), filepath.ToSlash(filepath.Dir(path)), src, hexDirs, got)...)

		return nil
	})
	require.NoError(t, err)

	return offenders, got
}

func compareHyphenatedAllowlist(got map[string]int, want map[string]truncationAllowance) []string {
	var offenders []string

	for path, n := range got {
		if w, ok := want[path]; !ok || w.count != n {
			offenders = append(offenders, path+": hyphen-bearing uuid truncation not matching hyphenatedTruncations()")
		}
	}

	for path := range want {
		if got[path] == 0 {
			offenders = append(offenders, path+": stale hyphenatedTruncations() entry")
		}
	}

	return offenders
}

// TestNoUndeclaredUUIDTruncation guards the bug class of hand-built IDs made by
// slicing a UUID string: past 8 chars the slice carries a hyphen and breaks
// hex/alphanumeric ID patterns (Transfer vpce-, SSM AccessKeyId, ECS task sets).
// Mint a shaped ID with a helper instead, or add a reasoned allowlist entry.
func TestNoUndeclaredUUIDTruncation(t *testing.T) {
	t.Parallel()

	offenders, got := scanUUIDTruncations(t, pureHexTruncationDirs())
	offenders = append(offenders, compareHyphenatedAllowlist(got, hyphenatedTruncations())...)

	require.Empty(t, offenders,
		"UUID-slice IDs: use a shaped-ID helper (hex without hyphens at the AWS length) "+
			"or add a reasoned allowlist entry")
}
