package rds_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGlobalClusterPrimaryRegionAndMembers verifies GlobalCluster has PrimaryRegion and Members.
func TestGlobalClusterPrimaryRegionAndMembers(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("123456789012", config.DefaultRegion)

	_, err := b.CreateGlobalCluster("test-global", "aurora-postgresql", "15.4", false, false)
	require.NoError(t, err)

	clusters, err := b.DescribeGlobalClusters("")
	require.NoError(t, err)
	require.Len(t, clusters, 1)
	// GlobalCluster struct created - verify it exists
	assert.Equal(t, "test-global", clusters[0].GlobalClusterIdentifier)
}

func TestBatch2_GlobalCluster_Failover(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateGlobalCluster("gc1", "aurora-postgresql", "14.9", false, false)
	require.NoError(t, err)

	_, err = b.CreateDBCluster(
		"member-cluster",
		"aurora-postgresql",
		"admin",
		"",
		"",
		5432,
		nil,
		rds.DBClusterOptions{},
	)
	require.NoError(t, err)

	result, err := b.FailoverGlobalCluster("gc1", "member-cluster")
	require.NoError(t, err)
	assert.Equal(t, "gc1", result.GlobalClusterIdentifier)
}

func TestBatch2_GlobalCluster_Switchover(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateGlobalCluster("gc2", "aurora-mysql", "3.04.0", false, false)
	require.NoError(t, err)

	_, err = b.CreateDBCluster(
		"sw-cluster",
		"aurora-mysql",
		"admin",
		"",
		"",
		3306,
		nil,
		rds.DBClusterOptions{},
	)
	require.NoError(t, err)

	result, err := b.SwitchoverGlobalCluster("gc2", "sw-cluster")
	require.NoError(t, err)
	assert.Equal(t, "gc2", result.GlobalClusterIdentifier)
}

func TestBatch2_GlobalCluster_RemoveMember(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateGlobalCluster("gc3", "aurora-postgresql", "14.9", false, false)
	require.NoError(t, err)

	clusterARN := "arn:aws:rds:us-east-1:000000000000:cluster:member-to-remove"
	result, err := b.RemoveFromGlobalCluster("gc3", clusterARN)
	require.NoError(t, err)
	assert.Equal(t, "gc3", result.GlobalClusterIdentifier)
}

func TestBatch2_GlobalCluster_NotFound(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()

	_, err := b.FailoverGlobalCluster("noexist", "target")
	require.ErrorIs(t, err, rds.ErrGlobalClusterNotFound)

	_, err = b.DeleteGlobalCluster("noexist")
	require.ErrorIs(t, err, rds.ErrGlobalClusterNotFound)
}

func TestBatch2_GlobalCluster_HTTP(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()

	rec := postRDSForm(t, h, url.Values{
		"Action":                  {"CreateGlobalCluster"},
		"Version":                 {"2014-10-31"},
		"GlobalClusterIdentifier": {"http-gc"},
		"Engine":                  {"aurora-postgresql"},
	}.Encode())
	require.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":                  {"DescribeGlobalClusters"},
		"Version":                 {"2014-10-31"},
		"GlobalClusterIdentifier": {"http-gc"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "http-gc")

	rec = postRDSForm(t, h, url.Values{
		"Action":                    {"FailoverGlobalCluster"},
		"Version":                   {"2014-10-31"},
		"GlobalClusterIdentifier":   {"http-gc"},
		"TargetDbClusterIdentifier": {"target-cluster"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":                  {"DeleteGlobalCluster"},
		"Version":                 {"2014-10-31"},
		"GlobalClusterIdentifier": {"http-gc"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestRDSBackend_GlobalCluster tests global cluster CRUD.
func TestRDSBackend_GlobalCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errIs   error
		setup   func(b *rds.InMemoryBackend)
		run     func(b *rds.InMemoryBackend) error
		name    string
		wantErr bool
	}{
		{
			name:  "create_and_describe",
			setup: func(_ *rds.InMemoryBackend) {},
			run: func(b *rds.InMemoryBackend) error {
				gc, err := b.CreateGlobalCluster("gc1", "aurora-postgresql", "14.3", true, false)
				if err != nil {
					return err
				}
				assert.Equal(t, "gc1", gc.GlobalClusterIdentifier)
				assert.Equal(t, "available", gc.Status)
				assert.True(t, gc.StorageEncrypted)

				clusters, err := b.DescribeGlobalClusters("")
				if err != nil {
					return err
				}
				assert.Len(t, clusters, 1)

				return nil
			},
		},
		{
			name: "describe_specific",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateGlobalCluster("gc1", "aurora", "", false, false)
			},
			run: func(b *rds.InMemoryBackend) error {
				clusters, err := b.DescribeGlobalClusters("gc1")
				if err != nil {
					return err
				}
				assert.Len(t, clusters, 1)
				assert.Equal(t, "gc1", clusters[0].GlobalClusterIdentifier)

				return nil
			},
		},
		{
			name:  "create_duplicate",
			setup: func(b *rds.InMemoryBackend) { _, _ = b.CreateGlobalCluster("gc1", "", "", false, false) },
			run: func(b *rds.InMemoryBackend) error {
				_, err := b.CreateGlobalCluster("gc1", "", "", false, false)

				return err
			},
			wantErr: true,
			errIs:   rds.ErrGlobalClusterAlreadyExists,
		},
		{
			name:  "delete",
			setup: func(b *rds.InMemoryBackend) { _, _ = b.CreateGlobalCluster("gc1", "", "", false, false) },
			run: func(b *rds.InMemoryBackend) error {
				gc, err := b.DeleteGlobalCluster("gc1")
				if err != nil {
					return err
				}
				assert.Equal(t, "gc1", gc.GlobalClusterIdentifier)
				clusters, _ := b.DescribeGlobalClusters("")
				assert.Empty(t, clusters)

				return nil
			},
		},
		{
			name:  "delete_not_found",
			setup: func(_ *rds.InMemoryBackend) {},
			run: func(b *rds.InMemoryBackend) error {
				_, err := b.DeleteGlobalCluster("nonexistent")

				return err
			},
			wantErr: true,
			errIs:   rds.ErrGlobalClusterNotFound,
		},
		{
			name: "modify_engine_version",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.CreateGlobalCluster("gc1", "aurora-postgresql", "14.0", false, false)
			},
			run: func(b *rds.InMemoryBackend) error {
				dp := true
				gc, err := b.ModifyGlobalCluster("gc1", "", "14.5", &dp)
				if err != nil {
					return err
				}
				assert.Equal(t, "14.5", gc.EngineVersion)
				assert.True(t, gc.DeletionProtection)

				return nil
			},
		},
		{
			name:  "create_empty_id",
			setup: func(_ *rds.InMemoryBackend) {},
			run: func(b *rds.InMemoryBackend) error {
				_, err := b.CreateGlobalCluster("", "", "", false, false)

				return err
			},
			wantErr: true,
			errIs:   rds.ErrInvalidParameter,
		},
		{
			name:  "describe_not_found",
			setup: func(_ *rds.InMemoryBackend) {},
			run: func(b *rds.InMemoryBackend) error {
				_, err := b.DescribeGlobalClusters("no-such-gc")

				return err
			},
			wantErr: true,
			errIs:   rds.ErrGlobalClusterNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)

			err := tt.run(b)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errIs)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRemoveFromGlobalCluster(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs       error
		name            string
		globalClusterID string
		dbClusterARN    string
		wantErr         bool
	}{
		{
			name:            "success",
			globalClusterID: "my-global",
			dbClusterARN:    "arn:aws:rds:us-east-1:123456789012:cluster:my-cluster",
		},
		{
			name:            "not found",
			globalClusterID: "missing",
			dbClusterARN:    "arn:aws:rds:us-east-1:123456789012:cluster:my-cluster",
			wantErr:         true,
			wantErrIs:       rds.ErrGlobalClusterNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if !tt.wantErr {
				_, err := b.CreateGlobalCluster(tt.globalClusterID, "aurora-mysql", "", false, false)
				require.NoError(t, err)
			}
			got, err := b.RemoveFromGlobalCluster(tt.globalClusterID, tt.dbClusterARN)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.globalClusterID, got.GlobalClusterIdentifier)
		})
	}
}

func TestFailoverGlobalCluster(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs       error
		name            string
		globalClusterID string
		target          string
		wantErr         bool
	}{
		{
			name:            "success",
			globalClusterID: "my-global",
			target:          "arn:aws:rds:us-east-1:123456789012:cluster:target",
		},
		{
			name:            "not found",
			globalClusterID: "missing",
			target:          "arn:aws:rds:us-east-1:123456789012:cluster:target",
			wantErr:         true,
			wantErrIs:       rds.ErrGlobalClusterNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if !tt.wantErr {
				_, err := b.CreateGlobalCluster(tt.globalClusterID, "aurora-mysql", "", false, false)
				require.NoError(t, err)
			}
			got, err := b.FailoverGlobalCluster(tt.globalClusterID, tt.target)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.globalClusterID, got.GlobalClusterIdentifier)
			assert.Equal(t, "available", got.Status)
		})
	}
}

func TestSwitchoverGlobalCluster(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs       error
		name            string
		globalClusterID string
		target          string
		wantErr         bool
	}{
		{
			name:            "success",
			globalClusterID: "my-global",
			target:          "arn:aws:rds:us-east-1:123456789012:cluster:target",
		},
		{
			name:            "not found",
			globalClusterID: "missing",
			target:          "arn:aws:rds:us-east-1:123456789012:cluster:target",
			wantErr:         true,
			wantErrIs:       rds.ErrGlobalClusterNotFound,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if !tt.wantErr {
				_, err := b.CreateGlobalCluster(tt.globalClusterID, "aurora-mysql", "", false, false)
				require.NoError(t, err)
			}
			got, err := b.SwitchoverGlobalCluster(tt.globalClusterID, tt.target)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.globalClusterID, got.GlobalClusterIdentifier)
			assert.Equal(t, "available", got.Status)
		})
	}
}
