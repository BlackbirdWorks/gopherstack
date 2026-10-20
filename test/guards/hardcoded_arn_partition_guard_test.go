package guards_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

const hardcodedARNAllowlistPath = "test/guards/testdata/hardcoded_arn_allowlist.json"

// TestNoNewHardcodedAWSPartitionARNs fails when service code gains a literal "arn:aws:" beyond the ratcheted
// allowlist. Request-path ARNs must come from pkgs/arn (arn.Build, arn.S3) so cn-*/us-gov-* regions get the
// right partition; prefix checks use arn.HasService.
func TestNoNewHardcodedAWSPartitionARNs(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile(hardcodedARNAllowlistPath)
	require.NoError(t, err)

	var allow map[string]int
	require.NoError(t, json.Unmarshal(raw, &allow))

	got := countHardcodedARNs(t)

	var offenders []string

	for path, n := range got {
		if want := allow[path]; n > want {
			offenders = append(offenders, path+": hard-coded \"arn:aws:\" literals exceed allowlist; use pkgs/arn")
		}
	}

	for path, want := range allow {
		if got[path] < want {
			offenders = append(offenders, path+": allowlist is stale, lower the count in "+hardcodedARNAllowlistPath)
		}
	}

	sort.Strings(offenders)
	require.Empty(t, offenders, strings.Join(offenders, "\n"))
}

func countHardcodedARNs(t *testing.T) map[string]int {
	t.Helper()

	got := map[string]int{}

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

		if n := hardcodedARNLines(string(src)); n > 0 {
			got[filepath.ToSlash(path)] = n
		}

		return nil
	})
	require.NoError(t, err)

	return got
}

func hardcodedARNLines(src string) int {
	n := 0

	for line := range strings.SplitSeq(src, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "//") {
			continue
		}

		n += strings.Count(line, `"arn:aws:`) + strings.Count(line, "`arn:aws:")
	}

	return n
}
