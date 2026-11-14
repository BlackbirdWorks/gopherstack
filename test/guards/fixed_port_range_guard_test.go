package guards_test

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var fixedPortRangeCall = regexp.MustCompile(`\bportalloc\.New\(`)

// TestNoFixedPortRangesInTests forbids portalloc.New in any _test.go outside pkgs/portalloc: a
// hardcoded range collides with any process already holding those ports. Use portalloctest.New.
func TestNoFixedPortRangesInTests(t *testing.T) {
	t.Parallel()

	var offenders []string

	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		slash := filepath.ToSlash(path)

		if d.IsDir() {
			if d.Name() == ".git" || d.Name() == "node_modules" || slash == "pkgs/portalloc" {
				return filepath.SkipDir
			}

			return nil
		}

		if !strings.HasSuffix(path, "_test.go") || strings.HasPrefix(slash, "test/guards/") {
			return nil
		}

		src, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}

		if fixedPortRangeCall.Match(src) {
			offenders = append(offenders, slash)
		}

		return nil
	})
	require.NoError(t, err)

	assert.Empty(t, offenders, "use portalloctest.New instead of a fixed portalloc.New range")
}
