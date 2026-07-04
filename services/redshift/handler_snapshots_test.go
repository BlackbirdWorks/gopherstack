package redshift_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

// ---- CreateClusterSnapshot ----

func TestRedshiftHandler_CreateClusterSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *redshift.Handler)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(h *redshift.Handler) {
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=snap-cluster")
			},
			body: "Action=CreateClusterSnapshot&Version=2012-12-01" +
				"&SnapshotIdentifier=my-snap&ClusterIdentifier=snap-cluster",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateClusterSnapshotResponse", "my-snap", "snap-cluster"},
		},
		{
			name:         "missing_snapshot_id",
			body:         "Action=CreateClusterSnapshot&Version=2012-12-01&ClusterIdentifier=cluster",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "missing_cluster_id",
			body:         "Action=CreateClusterSnapshot&Version=2012-12-01&SnapshotIdentifier=snap",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "cluster_not_found",
			body: "Action=CreateClusterSnapshot&Version=2012-12-01" +
				"&SnapshotIdentifier=snap&ClusterIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ClusterNotFound"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			h := redshift.NewHandler(b)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- DeleteClusterSnapshot ----

func TestRedshiftHandler_DeleteClusterSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *redshift.Handler)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(h *redshift.Handler) {
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=del-snap-cluster")
				postRedshiftForm(t, h, "Action=CreateClusterSnapshot&Version=2012-12-01"+
					"&SnapshotIdentifier=del-snap&ClusterIdentifier=del-snap-cluster")
			},
			body:         "Action=DeleteClusterSnapshot&Version=2012-12-01&SnapshotIdentifier=del-snap",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteClusterSnapshotResponse", "del-snap"},
		},
		{
			name:         "not_found",
			body:         "Action=DeleteClusterSnapshot&Version=2012-12-01&SnapshotIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ClusterSnapshotNotFound"},
		},
		{
			name:         "missing_id",
			body:         "Action=DeleteClusterSnapshot&Version=2012-12-01",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			h := redshift.NewHandler(b)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- DescribeClusterSnapshots ----

func TestRedshiftHandler_DescribeClusterSnapshots(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *redshift.Handler)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "list_empty",
			body:         "Action=DescribeClusterSnapshots&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeClusterSnapshotsResponse"},
		},
		{
			name: "list_with_snapshot",
			setup: func(h *redshift.Handler) {
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=list-snap-cluster")
				postRedshiftForm(t, h, "Action=CreateClusterSnapshot&Version=2012-12-01"+
					"&SnapshotIdentifier=list-snap&ClusterIdentifier=list-snap-cluster")
			},
			body:         "Action=DescribeClusterSnapshots&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeClusterSnapshotsResponse", "list-snap"},
		},
		{
			name: "filter_by_cluster",
			setup: func(h *redshift.Handler) {
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=filter-cluster")
				postRedshiftForm(t, h, "Action=CreateClusterSnapshot&Version=2012-12-01"+
					"&SnapshotIdentifier=filter-snap&ClusterIdentifier=filter-cluster")
			},
			body:         "Action=DescribeClusterSnapshots&Version=2012-12-01&ClusterIdentifier=filter-cluster",
			wantCode:     http.StatusOK,
			wantContains: []string{"filter-snap"},
		},
		{
			name:         "snapshot_not_found",
			body:         "Action=DescribeClusterSnapshots&Version=2012-12-01&SnapshotIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ClusterSnapshotNotFound"},
		},
		{
			name: "response_includes_snapshot_type_and_create_time",
			setup: func(h *redshift.Handler) {
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=meta-cluster")
				postRedshiftForm(t, h, "Action=CreateClusterSnapshot&Version=2012-12-01"+
					"&SnapshotIdentifier=meta-snap&ClusterIdentifier=meta-cluster")
			},
			body:     "Action=DescribeClusterSnapshots&Version=2012-12-01&SnapshotIdentifier=meta-snap",
			wantCode: http.StatusOK,
			wantContains: []string{
				"<SnapshotType>manual</SnapshotType>",
				"<SnapshotCreateTime>",
			},
		},
		{
			name: "filter_by_snapshot_type_manual",
			setup: func(h *redshift.Handler) {
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=type-cluster")
				postRedshiftForm(t, h, "Action=CreateClusterSnapshot&Version=2012-12-01"+
					"&SnapshotIdentifier=type-snap&ClusterIdentifier=type-cluster")
			},
			body:         "Action=DescribeClusterSnapshots&Version=2012-12-01&SnapshotType=manual",
			wantCode:     http.StatusOK,
			wantContains: []string{"type-snap"},
		},
		{
			name: "filter_by_snapshot_type_automated_returns_empty",
			setup: func(h *redshift.Handler) {
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=auto-cluster")
				postRedshiftForm(t, h, "Action=CreateClusterSnapshot&Version=2012-12-01"+
					"&SnapshotIdentifier=auto-snap&ClusterIdentifier=auto-cluster")
			},
			body:         "Action=DescribeClusterSnapshots&Version=2012-12-01&SnapshotType=automated",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeClusterSnapshotsResponse"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			h := redshift.NewHandler(b)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- CopyClusterSnapshot ----

func TestRedshiftHandler_CopyClusterSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *redshift.Handler)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(h *redshift.Handler) {
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=copy-cluster")
				postRedshiftForm(t, h, "Action=CreateClusterSnapshot&Version=2012-12-01"+
					"&SnapshotIdentifier=src-snap&ClusterIdentifier=copy-cluster")
			},
			body: "Action=CopyClusterSnapshot&Version=2012-12-01" +
				"&SourceSnapshotIdentifier=src-snap&TargetSnapshotIdentifier=dst-snap",
			wantCode:     http.StatusOK,
			wantContains: []string{"CopyClusterSnapshotResponse", "dst-snap"},
		},
		{
			name: "source_not_found",
			body: "Action=CopyClusterSnapshot&Version=2012-12-01" +
				"&SourceSnapshotIdentifier=nonexistent&TargetSnapshotIdentifier=dst",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ClusterSnapshotNotFound"},
		},
		{
			name:         "missing_source",
			body:         "Action=CopyClusterSnapshot&Version=2012-12-01&TargetSnapshotIdentifier=dst",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "missing_target",
			body:         "Action=CopyClusterSnapshot&Version=2012-12-01&SourceSnapshotIdentifier=src",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			h := redshift.NewHandler(b)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- RestoreFromClusterSnapshot ----

func TestRedshiftHandler_RestoreFromClusterSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *redshift.Handler)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(h *redshift.Handler) {
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=restore-src")
				postRedshiftForm(t, h, "Action=CreateClusterSnapshot&Version=2012-12-01"+
					"&SnapshotIdentifier=restore-snap&ClusterIdentifier=restore-src")
			},
			body: "Action=RestoreFromClusterSnapshot&Version=2012-12-01" +
				"&ClusterIdentifier=restore-dst&SnapshotIdentifier=restore-snap",
			wantCode:     http.StatusOK,
			wantContains: []string{"RestoreFromClusterSnapshotResponse", "restore-dst"},
		},
		{
			name: "snapshot_not_found",
			body: "Action=RestoreFromClusterSnapshot&Version=2012-12-01" +
				"&ClusterIdentifier=new-cluster&SnapshotIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ClusterSnapshotNotFound"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			h := redshift.NewHandler(b)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- Backend: SnapshotCount ----

func TestRedshiftBackend_SnapshotCount(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	require.Equal(t, 0, redshift.SnapshotCount(b))

	h := redshift.NewHandler(b)
	postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=count-cluster")
	postRedshiftForm(t, h, "Action=CreateClusterSnapshot&Version=2012-12-01"+
		"&SnapshotIdentifier=count-snap&ClusterIdentifier=count-cluster")
	require.Equal(t, 1, redshift.SnapshotCount(b))

	postRedshiftForm(t, h, "Action=DeleteClusterSnapshot&Version=2012-12-01&SnapshotIdentifier=count-snap")
	require.Equal(t, 0, redshift.SnapshotCount(b))
}

// TestRedshiftHandler_DescribeClusterSnapshots_SnapshotTypeFilter verifies that
// the SnapshotType filter correctly includes and excludes snapshots by type.
func TestRedshiftHandler_DescribeClusterSnapshots_SnapshotTypeFilter(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	h := redshift.NewHandler(b)

	postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=st-cluster")
	postRedshiftForm(t, h, "Action=CreateClusterSnapshot&Version=2012-12-01"+
		"&SnapshotIdentifier=manual-snap&ClusterIdentifier=st-cluster")

	// manual filter: snapshot appears
	rec := postRedshiftForm(t, h, "Action=DescribeClusterSnapshots&Version=2012-12-01&SnapshotType=manual")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "manual-snap")

	// automated filter: snapshot absent
	rec = postRedshiftForm(t, h, "Action=DescribeClusterSnapshots&Version=2012-12-01&SnapshotType=automated")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "manual-snap")
}
