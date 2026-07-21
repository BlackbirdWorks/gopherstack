package version_test

import (
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/version"
)

func TestGet(t *testing.T) { //nolint:paralleltest // mutates global variable Build
	tests := []struct {
		name          string
		buildVal      string
		mockReadBuild func() (*debug.BuildInfo, bool)
		want          string
	}{
		{
			name:     "custom build version",
			buildVal: "v1.2.3",
			want:     "v1.2.3",
		},
		{
			name:     "default dev version no build info",
			buildVal: "dev",
			mockReadBuild: func() (*debug.BuildInfo, bool) {
				return nil, false
			},
			want: "dev",
		},
		{
			name:     "default dev version with empty build info version",
			buildVal: "dev",
			mockReadBuild: func() (*debug.BuildInfo, bool) {
				return &debug.BuildInfo{Main: debug.Module{Version: ""}}, true
			},
			want: "dev",
		},
		{
			name:     "default dev version with devel build info version",
			buildVal: "dev",
			mockReadBuild: func() (*debug.BuildInfo, bool) {
				return &debug.BuildInfo{Main: debug.Module{Version: "(devel)"}}, true
			},
			want: "dev",
		},
		{
			name:     "valid build info version",
			buildVal: "dev",
			mockReadBuild: func() (*debug.BuildInfo, bool) {
				return &debug.BuildInfo{Main: debug.Module{Version: "v1.0.0"}}, true
			},
			want: "v1.0.0",
		},
	}

	for _, tt := range tests { //nolint:paralleltest // mutates global variable Build
		t.Run(tt.name, func(t *testing.T) {
			// Restore original after test.
			originalBuild := version.Build
			originalReadBuildInfo := version.ReadBuildInfo
			t.Cleanup(func() {
				version.Build = originalBuild                 //nolint:reassign // mock ldflags
				version.ReadBuildInfo = originalReadBuildInfo //nolint:reassign // mock
			})

			version.Build = tt.buildVal //nolint:reassign // mock ldflags
			if tt.mockReadBuild != nil {
				version.ReadBuildInfo = tt.mockReadBuild //nolint:reassign // mock
			} else {
				version.ReadBuildInfo = func() (*debug.BuildInfo, bool) { return nil, false } //nolint:reassign // mock
			}

			got := version.Get()
			require.Equal(t, tt.want, got)
		})
	}
}
