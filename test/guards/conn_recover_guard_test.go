package guards_test

import (
	"bytes"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var listenerAccept = regexp.MustCompile(`(\w+)\.Accept\(\)`)

// acceptsConnections skips ssh channel accepts (receiver nc); every other Accept() is a listener.
func acceptsConnections(src []byte) bool {
	for _, m := range listenerAccept.FindAllSubmatch(src, -1) {
		if string(m[1]) != "nc" {
			return true
		}
	}

	return false
}

// connRecoverExempt lists files with an accept loop that spawns no client-serving goroutine of their own.
var connRecoverExempt = map[string]string{
	"pkgs/gcprest/split.go":              "sniffs the first bytes and hands the conn to net/http or grpc, which own the serve goroutine",
	"pkgs/httputils/queryspace.go":       "wraps the conn for net/http, which recovers handler panics",
	"pkgs/smtprelay/smtptest/server.go":  "test double",
	"services/ociapigateway/tlsserve.go": "sniffs the first byte and hands the conn to net/http",
	"services/ocifilestorage/nfs.go":     "go-nfs owns the per-connection goroutine; handlers are billy/osfs calls",
}

// TestListenerLoopsRecoverConnectionPanics keeps raw accept loops from taking the process down: net/http recovers
// handler panics but a hand-rolled per-connection goroutine does not, so every file that accepts connections must
// recover via logger.Recover.
func TestListenerLoopsRecoverConnectionPanics(t *testing.T) {
	t.Parallel()

	var offenders []string

	seen := map[string]bool{}

	for _, root := range []string{"services", "pkgs"} {
		err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}

			if d.IsDir() {
				if d.Name() == "node_modules" {
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

			if !acceptsConnections(src) {
				return nil
			}

			slashed := filepath.ToSlash(path)
			seen[slashed] = true

			if _, ok := connRecoverExempt[slashed]; !ok && !bytes.Contains(src, []byte("logger.Recover(")) {
				offenders = append(offenders, slashed)
			}

			return nil
		})
		require.NoError(t, err)
	}

	assert.Empty(t, offenders, "accept loop without logger.Recover: wrap the per-connection goroutine, or exempt it with a reason")

	for path := range connRecoverExempt {
		assert.True(t, seen[path], "stale connRecoverExempt entry: %s", path)
	}
}
