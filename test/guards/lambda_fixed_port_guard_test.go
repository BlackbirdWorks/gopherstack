package guards_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNoFixedPortRangesInLambdaTests forbids portalloc.New in services/lambda tests: a hardcoded range
// collides with any process already holding those ports. Use portalloctest.New.
func TestNoFixedPortRangesInLambdaTests(t *testing.T) {
	t.Parallel()

	var offenders []string

	err := filepath.WalkDir(filepath.Join("services", "lambda"), func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() || !strings.HasSuffix(path, "_test.go") {
			return nil
		}

		src, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}

		if strings.Contains(string(src), "portalloc.New(") {
			offenders = append(offenders, filepath.ToSlash(path))
		}

		return nil
	})
	require.NoError(t, err)

	assert.Empty(t, offenders, "use portalloctest.New instead of a fixed portalloc.New range")
}
