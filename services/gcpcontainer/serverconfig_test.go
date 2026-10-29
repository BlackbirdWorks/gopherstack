//nolint:staticcheck // the API still serves fields the proto marks deprecated
package gcpcontainer_test

import (
	"testing"

	"cloud.google.com/go/container/apiv1/containerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

func TestGetServerConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		req  *containerpb.GetServerConfigRequest
		name string
		want codes.Code
	}{
		{&containerpb.GetServerConfigRequest{Name: zoneLoc}, "zone", codes.OK},
		{&containerpb.GetServerConfigRequest{Name: regionLc}, "region", codes.OK},
		{&containerpb.GetServerConfigRequest{ProjectId: project, Zone: zone}, "legacy fields", codes.OK},
		{&containerpb.GetServerConfigRequest{Name: "projects/p/locations/moon-1"}, "unknown location", codes.NotFound},
		{&containerpb.GetServerConfigRequest{Name: "garbage"}, "bad name", codes.InvalidArgument},
		{&containerpb.GetServerConfigRequest{}, "empty", codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)

			cfg, err := e.gc.GetServerConfig(t.Context(), tt.req)
			if tt.want != codes.OK {
				assertCode(t, tt.want, err, "")

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "COS_CONTAINERD", cfg.GetDefaultImageType())
			assert.Contains(t, cfg.GetValidImageTypes(), "UBUNTU_CONTAINERD")
			assert.Contains(t, cfg.GetValidMasterVersions(), cfg.GetDefaultClusterVersion())
			assert.Equal(t, "1.35.1-gke.1000000", cfg.GetValidMasterVersions()[0], "newest first")

			for _, v := range cfg.GetValidMasterVersions() {
				assert.Contains(t, cfg.GetValidNodeVersions(), v)
			}

			require.Len(t, cfg.GetChannels(), 4)

			seen := make(map[containerpb.ReleaseChannel_Channel]bool)

			for _, ch := range cfg.GetChannels() {
				seen[ch.GetChannel()] = true

				assert.Contains(t, ch.GetValidVersions(), ch.GetDefaultVersion())
				assert.Contains(t, ch.GetValidVersions(), ch.GetUpgradeTargetVersion())

				for _, v := range ch.GetValidVersions() {
					assert.Contains(t, cfg.GetValidMasterVersions(), v)
				}
			}

			assert.Len(t, seen, 4)
		})
	}
}
