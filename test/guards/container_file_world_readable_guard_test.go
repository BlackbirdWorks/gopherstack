package guards_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var containerFileModeRe = regexp.MustCompile(`ContainerFilePath:[^}]*FileMode:\s*(0[oO]?[0-7]+)`)

// containerFilesUnreadableByNonRoot returns the modes of copied-in container files
// that lack the world-read bit; the images run as UID 65532, not root.
func containerFilesUnreadableByNonRoot(src string) []string {
	var found []string

	for _, m := range containerFileModeRe.FindAllStringSubmatch(src, -1) {
		mode, err := strconv.ParseUint(strings.TrimPrefix(strings.ToLower(m[1]), "0o"), 8, 32)
		if err == nil && mode&0o004 == 0 {
			found = append(found, m[1])
		}
	}

	return found
}

func TestContainerFilesReadableByNonRoot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
		want int
	}{
		{name: "owner_only_key", src: `{HostFilePath: k, ContainerFilePath: "/k", FileMode: 0o400},`, want: 1},
		{name: "legacy_octal", src: `{HostFilePath: k, ContainerFilePath: "/k", FileMode: 0600},`, want: 1},
		{name: "world_readable", src: `{HostFilePath: k, ContainerFilePath: "/k", FileMode: 0o444},`, want: 0},
		{name: "no_mode", src: `{HostFilePath: k, ContainerFilePath: "/k"}`, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Len(t, containerFilesUnreadableByNonRoot(tt.src), tt.want)
		})
	}
}

// TestNoOwnerOnlyContainerFiles keeps test harnesses from copying root-owned 0400/0600
// files into the non-root gopherstack image, where the server cannot open them.
func TestNoOwnerOnlyContainerFiles(t *testing.T) {
	t.Parallel()

	var offenders []string

	err := filepath.WalkDir("test", func(path string, d os.DirEntry, err error) error {
		slashed := filepath.ToSlash(path)
		if err != nil || d.IsDir() || !strings.HasSuffix(slashed, ".go") || strings.HasPrefix(slashed, "test/guards/") {
			return err
		}

		src, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}

		for _, mode := range containerFilesUnreadableByNonRoot(string(src)) {
			offenders = append(offenders, filepath.ToSlash(path)+": FileMode "+mode)
		}

		return nil
	})
	require.NoError(t, err)
	assert.Empty(t, offenders, "ContainerFile modes must include world-read (0o444)")
}
