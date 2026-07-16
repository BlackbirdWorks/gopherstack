package elasticache_test

import (
	"context"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/elasticache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBackend_CreateServerlessCache(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	sc, err := b.CreateServerlessCache(context.Background(), "my-sc", "my serverless cache", "redis")
	require.NoError(t, err)
	assert.Equal(t, "my-sc", sc.Name)
	assert.Equal(t, "redis", sc.Engine)
	assert.Equal(t, "available", sc.Status)
	assert.Contains(t, sc.ARN, "arn:aws:elasticache:")
	assert.Contains(t, sc.ARN, ":serverlesscache:my-sc")
}

func TestBackend_CreateServerlessCache_Valkey(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	sc, err := b.CreateServerlessCache(context.Background(), "valkey-sc", "valkey serverless", "valkey")
	require.NoError(t, err)
	assert.Equal(t, "valkey", sc.Engine)
}

func TestBackend_CreateServerlessCache_AlreadyExists(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b.CreateServerlessCache(context.Background(), "dup-sc", "first", "redis")
	require.NoError(t, err)

	_, err = b.CreateServerlessCache(context.Background(), "dup-sc", "second", "redis")
	require.Error(t, err)
}

func TestBackend_DescribeServerlessCaches_FilterByName(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	for _, name := range []string{"sc-a", "sc-b", "sc-c"} {
		_, err := b.CreateServerlessCache(context.Background(), name, "cache "+name, "redis")
		require.NoError(t, err)
	}

	p, err := b.DescribeServerlessCaches(context.Background(), "sc-b", "", 0)
	require.NoError(t, err)
	require.Len(t, p.Data, 1)
	assert.Equal(t, "sc-b", p.Data[0].Name)
}

func TestBackend_ModifyServerlessCache(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b.CreateServerlessCache(context.Background(), "mod-sc", "original", "redis")
	require.NoError(t, err)

	sc, err := b.ModifyServerlessCache(context.Background(), "mod-sc", "modified description")
	require.NoError(t, err)
	assert.Equal(t, "modified description", sc.Description)
}

func TestBackend_DeleteServerlessCache(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b.CreateServerlessCache(context.Background(), "del-sc", "to be deleted", "redis")
	require.NoError(t, err)

	sc, err := b.DeleteServerlessCache(context.Background(), "del-sc")
	require.NoError(t, err)
	assert.Equal(t, "del-sc", sc.Name)

	_, err = b.DescribeServerlessCaches(context.Background(), "del-sc", "", 0)
	require.Error(t, err)
}

// ----------------------------------------
// ServerlessCacheSnapshot (issue)
// ----------------------------------------

func TestBackend_CreateServerlessCacheSnapshot(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b.CreateServerlessCache(context.Background(), "snap-sc", "cache for snapshot", "redis")
	require.NoError(t, err)

	snap, err := b.CreateServerlessCacheSnapshot(context.Background(), "sc-snap-1", "snap-sc")
	require.NoError(t, err)
	assert.Equal(t, "sc-snap-1", snap.Name)
	assert.Equal(t, "snap-sc", snap.ServerlessCacheName)
	assert.Equal(t, "available", snap.Status)
	assert.Contains(t, snap.ARN, "arn:aws:elasticache:")
}

func TestBackend_CopyServerlessCacheSnapshot(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b.CreateServerlessCache(context.Background(), "copy-sc-snap-cache", "cache", "redis")
	require.NoError(t, err)
	_, err = b.CreateServerlessCacheSnapshot(context.Background(), "copy-sc-src", "copy-sc-snap-cache")
	require.NoError(t, err)

	copied, err := b.CopyServerlessCacheSnapshot(context.Background(), "copy-sc-src", "copy-sc-dst")
	require.NoError(t, err)
	assert.Equal(t, "copy-sc-dst", copied.Name)
}

func TestBackend_ExportServerlessCacheSnapshot(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b.CreateServerlessCache(context.Background(), "export-sc", "cache for export", "redis")
	require.NoError(t, err)
	_, err = b.CreateServerlessCacheSnapshot(context.Background(), "export-sc-snap", "export-sc")
	require.NoError(t, err)

	snap, err := b.ExportServerlessCacheSnapshot(context.Background(), "export-sc-snap", "my-s3-bucket")
	require.NoError(t, err)
	assert.Equal(t, "export-sc-snap", snap.Name)
}

func TestBackend_DeleteServerlessCacheSnapshot(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	_, err := b.CreateServerlessCache(context.Background(), "del-sc-snap-cache", "cache", "redis")
	require.NoError(t, err)
	_, err = b.CreateServerlessCacheSnapshot(context.Background(), "del-sc-snap", "del-sc-snap-cache")
	require.NoError(t, err)

	snap, err := b.DeleteServerlessCacheSnapshot(context.Background(), "del-sc-snap")
	require.NoError(t, err)
	assert.Equal(t, "del-sc-snap", snap.Name)
}

// ----------------------------------------
// ServiceUpdate + UpdateAction (issue)
// ----------------------------------------

func TestBackend_CreateServerlessCacheFull_AllFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantKms    string
		wantGroup  string
		wantSnap   string
		opts       elasticache.ServerlessCreateOpts
		wantRetain int32
	}{
		{
			name: "with_kms_and_user_group",
			opts: elasticache.ServerlessCreateOpts{
				Name:                   "sc-kms",
				Engine:                 "redis",
				KmsKeyID:               "arn:aws:kms:us-east-1:123456789012:key/mrk-abc123",
				UserGroupID:            "my-group",
				DailySnapshotTime:      "05:00",
				SnapshotRetentionLimit: 7,
			},
			wantKms:    "arn:aws:kms:us-east-1:123456789012:key/mrk-abc123",
			wantGroup:  "my-group",
			wantSnap:   "05:00",
			wantRetain: 7,
		},
		{
			name: "valkey_major_version",
			opts: elasticache.ServerlessCreateOpts{
				Name:   "sc-valkey",
				Engine: "valkey",
			},
		},
		{
			name: "with_security_groups",
			opts: elasticache.ServerlessCreateOpts{
				Name:             "sc-sgs",
				Engine:           "redis",
				SecurityGroupIDs: []string{"sg-111", "sg-222"},
				SubnetIDs:        []string{"subnet-aaa", "subnet-bbb"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

			sc, err := b.CreateServerlessCacheFull(context.Background(), tt.opts)
			require.NoError(t, err)
			assert.Equal(t, tt.opts.Name, sc.Name)
			assert.NotEmpty(t, sc.ARN)
			assert.NotEmpty(t, sc.Status)

			if tt.wantKms != "" {
				assert.Equal(t, tt.wantKms, sc.KmsKeyID)
			}

			if tt.wantGroup != "" {
				assert.Equal(t, tt.wantGroup, sc.UserGroupID)
			}

			if tt.wantSnap != "" {
				assert.Equal(t, tt.wantSnap, sc.DailySnapshotTime)
			}

			if tt.wantRetain > 0 {
				assert.Equal(t, tt.wantRetain, sc.SnapshotRetentionLimit)
			}
		})
	}
}

func TestBackend_CreateServerlessCacheFull_AlreadyExists(t *testing.T) {
	t.Parallel()

	b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)

	opts := elasticache.ServerlessCreateOpts{Name: "dup-sc", Engine: "redis"}
	_, err := b.CreateServerlessCacheFull(context.Background(), opts)
	require.NoError(t, err)

	_, err = b.CreateServerlessCacheFull(context.Background(), opts)
	require.Error(t, err)
	assert.ErrorIs(t, err, elasticache.ErrServerlessCacheAlreadyExists)
}

// ----------------------------------------
// ModifyServerlessCacheFull
// ----------------------------------------

func TestBackend_ModifyServerlessCacheFull(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantUG  string
		opts    elasticache.ServerlessModifyOpts
		wantRet int32
		noUG    bool
	}{
		{
			name: "update_description_and_snapshot",
			opts: elasticache.ServerlessModifyOpts{
				Description:       "updated desc",
				DailySnapshotTime: "03:00",
				SnapshotRetentionLimit: func() *int32 {
					v := int32(14)

					return &v
				}(),
			},
			wantRet: 14,
		},
		{
			name: "add_user_group",
			opts: elasticache.ServerlessModifyOpts{
				UserGroupID: "grp-abc",
			},
			wantUG: "grp-abc",
		},
		{
			name: "remove_user_group",
			opts: elasticache.ServerlessModifyOpts{
				RemoveUserGroup: true,
			},
			noUG: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := elasticache.NewInMemoryBackend(elasticache.EngineStub, "000000000000", "us-east-1", nil)
			_, err := b.CreateServerlessCache(context.Background(), "mod-sc", "original", "redis")
			require.NoError(t, err)

			if tt.noUG {
				_, err = b.ModifyServerlessCacheFull(context.Background(),
					"mod-sc",
					elasticache.ServerlessModifyOpts{UserGroupID: "pre-group"},
				)
				require.NoError(t, err)
			}

			sc, err := b.ModifyServerlessCacheFull(context.Background(), "mod-sc", tt.opts)
			require.NoError(t, err)

			if tt.wantRet > 0 {
				assert.Equal(t, tt.wantRet, sc.SnapshotRetentionLimit)
			}

			if tt.wantUG != "" {
				assert.Equal(t, tt.wantUG, sc.UserGroupID)
			}

			if tt.noUG {
				assert.Empty(t, sc.UserGroupID)
			}
		})
	}
}
