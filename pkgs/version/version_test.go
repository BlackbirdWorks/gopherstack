package version_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/version"
	"github.com/stretchr/testify/require"
)

func TestGet(t *testing.T) { //nolint:paralleltest // mutates global variable Build
	tests := []struct {
		name     string
		buildVal string
		want     string
	}{
		{
			name:     "custom build version",
			buildVal: "v1.2.3",
			want:     "v1.2.3",
		},
		{
			name:     "default dev version",
			buildVal: "dev",
			want:     "dev",
		},
	}

	for _, tt := range tests { //nolint:paralleltest // mutates global variable Build
		t.Run(tt.name, func(t *testing.T) {
			// Restore original after test.
			original := version.Build
			t.Cleanup(func() { version.Build = original }) //nolint:reassign // mock ldflags

			version.Build = tt.buildVal //nolint:reassign // mock ldflags

			got := version.Get()
			require.Equal(t, tt.want, got)
		})
	}
}
