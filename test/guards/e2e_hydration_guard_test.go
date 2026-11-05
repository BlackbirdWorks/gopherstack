package guards_test

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// e2eGotoWithoutHydration returns the 1-based lines of page.Goto calls not
// followed (after the err check) by waitHydrated.
func e2eGotoWithoutHydration(src string) []int {
	lines := strings.Split(src, "\n")

	var bad []int

	for i, l := range lines {
		if !strings.Contains(l, "page.Goto(") {
			continue
		}

		if i+2 >= len(lines) || strings.TrimSpace(lines[i+2]) != "waitHydrated(t, page)" {
			bad = append(bad, i+1)
		}
	}

	return bad
}

func TestE2EGotoWithoutHydration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		src  string
		want []int
	}{
		{name: "hydrated", src: "_, err = page.Goto(u)\nrequire.NoError(t, err)\nwaitHydrated(t, page)\n", want: nil},
		{name: "missing", src: "_, err = page.Goto(u)\nrequire.NoError(t, err)\npage.Click()\n", want: []int{1}},
		{name: "at_eof", src: "_, err = page.Goto(u)", want: []int{1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, e2eGotoWithoutHydration(tt.src))
		})
	}
}

// TestE2EGotoWaitsForHydration: a click right after Goto can land before the
// SvelteKit router attaches and degrade to a slow full-page navigation.
func TestE2EGotoWaitsForHydration(t *testing.T) {
	t.Parallel()

	files, err := filepath.Glob("test/e2e/*_test.go")
	require.NoError(t, err)
	require.NotEmpty(t, files)

	var offenders []string

	for _, f := range files {
		src, readErr := os.ReadFile(f)
		require.NoError(t, readErr)

		for _, line := range e2eGotoWithoutHydration(string(src)) {
			offenders = append(offenders, filepath.ToSlash(f)+":"+strconv.Itoa(line))
		}
	}

	assert.Empty(t, offenders, "add waitHydrated(t, page) after page.Goto")
}
