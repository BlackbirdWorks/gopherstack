package efs_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/efs"
)

// TestResourceLimits proves gopherstack-ne9h: CreateFileSystem,
// CreateReplicationConfiguration, and CreateAccessPoint -- the ops whose own
// declared error set includes FileSystemLimitExceeded/AccessPointLimitExceeded
// (see limits.go for the per-op citations) -- now enforce the real quota and
// return the typed error once reached. Every case uses WithResourceLimits to
// shrink the relevant cap instead of creating the real 1,000/10,000 count of
// resources.
func TestResourceLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs error
		setup     func(t *testing.T, b *efs.InMemoryBackend)
		trip      func(b *efs.InMemoryBackend) error
		name      string
	}{
		{
			name: "file_systems_per_account",
			setup: func(t *testing.T, b *efs.InMemoryBackend) {
				t.Helper()
				b.WithResourceLimits(efs.ResourceLimits{FileSystemsPerAccount: 1})
				_, err := b.CreateFileSystem(context.Background(), fsReq("fs-limit-1"))
				require.NoError(t, err)
			},
			trip: func(b *efs.InMemoryBackend) error {
				_, err := b.CreateFileSystem(context.Background(), fsReq("fs-limit-2"))

				return err
			},
			wantErrIs: efs.ErrFileSystemLimitExceeded,
		},
		{
			name: "file_systems_per_account_via_replication",
			setup: func(t *testing.T, b *efs.InMemoryBackend) {
				t.Helper()
				b.WithResourceLimits(efs.ResourceLimits{FileSystemsPerAccount: 1})
				_, err := b.CreateFileSystem(context.Background(), fsReq("fs-limit-rc"))
				require.NoError(t, err)
			},
			trip: func(b *efs.InMemoryBackend) error {
				fs, _, err := b.DescribeFileSystems(context.Background(), "", "fs-limit-rc", "", 0)
				require.NoError(t, err)
				require.Len(t, fs, 1)

				_, err = b.CreateReplicationConfiguration(
					context.Background(), fs[0].FileSystemID,
					[]efs.ReplicationDestination{{Region: "us-west-2"}},
				)

				return err
			},
			wantErrIs: efs.ErrFileSystemLimitExceeded,
		},
		{
			name: "access_points_per_file_system",
			setup: func(t *testing.T, b *efs.InMemoryBackend) {
				t.Helper()
				b.WithResourceLimits(efs.ResourceLimits{AccessPointsPerFileSys: 1})
				fs, err := b.CreateFileSystem(context.Background(), fsReq("fs-ap-limit"))
				require.NoError(t, err)
				_, err = b.CreateAccessPoint(context.Background(), apReq(fs.FileSystemID))
				require.NoError(t, err)
			},
			trip: func(b *efs.InMemoryBackend) error {
				fs, _, err := b.DescribeFileSystems(context.Background(), "", "fs-ap-limit", "", 0)
				require.NoError(t, err)
				require.Len(t, fs, 1)

				_, err = b.CreateAccessPoint(context.Background(), apReq(fs[0].FileSystemID))

				return err
			},
			wantErrIs: efs.ErrAccessPointLimitExceeded,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestEFSBackend()
			tt.setup(t, b)

			err := tt.trip(b)
			require.Error(t, err)
			assert.ErrorIs(t, err, tt.wantErrIs)
		})
	}
}

// TestResourceLimits_HTTPWireShape proves the HTTP-level error shape: real
// AWS EFS's FileSystemLimitExceeded/AccessPointLimitExceeded carry
// httpStatusCode 403 (aws-sdk-go@v1.55.8/models/apis/elasticfilesystem/
// 2015-02-01/api-2.json shapes entries), not the 400 SecurityGroupLimitExceeded
// uses.
func TestResourceLimits_HTTPWireShape(t *testing.T) {
	t.Parallel()

	t.Run("file_system_limit_exceeded", func(t *testing.T) {
		t.Parallel()

		h := newTestEFSHandler()
		h.Backend.WithResourceLimits(efs.ResourceLimits{FileSystemsPerAccount: 1})
		createFS(t, h, "wire-fsle-1")

		rec := doREST(t, h, "POST", "/2015-02-01/file-systems", map[string]any{"CreationToken": "wire-fsle-2"})
		assert.Equal(t, 403, rec.Code)
		assert.Equal(t, "FileSystemLimitExceeded", rec.Header().Get("X-Amzn-Errortype"))

		resp := parseResp(t, rec)
		assert.Equal(t, "FileSystemLimitExceeded", resp["ErrorCode"])
	})

	t.Run("access_point_limit_exceeded", func(t *testing.T) {
		t.Parallel()

		h := newTestEFSHandler()
		h.Backend.WithResourceLimits(efs.ResourceLimits{AccessPointsPerFileSys: 1})
		fsID := createFS(t, h, "wire-aple-1")

		seedRec := doREST(t, h, "POST", "/2015-02-01/access-points", map[string]any{"FileSystemId": fsID})
		require.Equal(t, 200, seedRec.Code, "seed access point failed: %s", seedRec.Body.String())

		rec := doREST(t, h, "POST", "/2015-02-01/access-points", map[string]any{"FileSystemId": fsID})
		assert.Equal(t, 403, rec.Code)
		assert.Equal(t, "AccessPointLimitExceeded", rec.Header().Get("X-Amzn-Errortype"))

		resp := parseResp(t, rec)
		assert.Equal(t, "AccessPointLimitExceeded", resp["ErrorCode"])
	})
}

// TestWithResourceLimits_SurvivesReset proves an override set via
// WithResourceLimits persists across Reset(), matching services/ses's
// TestWithResourceLimits_SurvivesReset precedent.
func TestWithResourceLimits_SurvivesReset(t *testing.T) {
	t.Parallel()

	b := newTestEFSBackend().WithResourceLimits(efs.ResourceLimits{FileSystemsPerAccount: 1})
	_, err := b.CreateFileSystem(context.Background(), fsReq("reset-fsle-1"))
	require.NoError(t, err)

	b.Reset()

	_, err = b.CreateFileSystem(context.Background(), fsReq("reset-fsle-1"))
	require.NoError(t, err)

	_, err = b.CreateFileSystem(context.Background(), fsReq("reset-fsle-2"))
	require.Error(t, err)
	assert.ErrorIs(t, err, efs.ErrFileSystemLimitExceeded)
}
