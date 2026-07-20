package version_test

import (
	"runtime/debug"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/version"
	"github.com/stretchr/testify/require"
)

func TestGet(t *testing.T) { //nolint:paralleltest // mutates global variable Build
	tests := []struct {
		name      string
		buildVal  string
		mockBuild func() (*debug.BuildInfo, bool)
		want      string
	}{
		{
			name:     "custom build version",
			buildVal: "v1.2.3",
			want:     "v1.2.3",
		},
		{
			name:     "default dev version no build info",
			buildVal: "dev",
			mockBuild: func() (*debug.BuildInfo, bool) {
				return nil, false
			},
			want: "dev",
		},
		{
			name:     "default dev version with empty build info version",
			buildVal: "dev",
			mockBuild: func() (*debug.BuildInfo, bool) {
				return &debug.BuildInfo{Main: debug.Module{Version: ""}}, true
			},
			want: "dev",
		},
		{
			name:     "default dev version with devel build info version",
			buildVal: "dev",
			mockBuild: func() (*debug.BuildInfo, bool) {
				return &debug.BuildInfo{Main: debug.Module{Version: "(devel)"}}, true
			},
			want: "dev",
		},
		{
			name:     "valid build info version",
			buildVal: "dev",
			mockBuild: func() (*debug.BuildInfo, bool) {
				return &debug.BuildInfo{Main: debug.Module{Version: "v1.0.0"}}, true
			},
			want: "v1.0.0",
		},
	}

	for _, tt := range tests { //nolint:paralleltest // mutates global variable Build
		t.Run(tt.name, func(t *testing.T) {
			// Restore original after test.
			originalBuild := version.Build
			originalReadBuildInfo := *version.MockReadBuildInfo
			t.Cleanup(func() {
				version.Build = originalBuild //nolint:reassign // mock ldflags
				*version.MockReadBuildInfo = originalReadBuildInfo
			})

			version.Build = tt.buildVal //nolint:reassign // mock ldflags
			if tt.mockBuild != nil {
				*version.MockReadBuildInfo = tt.mockBuild
			} else {
				*version.MockReadBuildInfo = func() (*debug.BuildInfo, bool) { return nil, false }
			}

			got := version.Get()
			require.Equal(t, tt.want, got)
		})
	}
}
