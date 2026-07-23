package redshift_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

// ---- DescribeTableRestoreStatus ----

func TestHandler_DescribeTableRestoreStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "empty",
			body:         "Action=DescribeTableRestoreStatus&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeTableRestoreStatusResponse"},
		},
		{
			name:         "with_cluster_filter",
			body:         "Action=DescribeTableRestoreStatus&Version=2012-12-01&ClusterIdentifier=c1",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeTableRestoreStatusResponse"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- RestoreTableFromClusterSnapshot ----

func TestHandler_RestoreTableFromClusterSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			body: "Action=RestoreTableFromClusterSnapshot&Version=2012-12-01" +
				"&ClusterIdentifier=my-cluster" +
				"&SnapshotIdentifier=my-snapshot" +
				"&SourceDatabaseName=mydb" +
				"&SourceTableName=orders" +
				"&TargetDatabaseName=mydb" +
				"&NewTableName=orders_restored",
			wantCode:     http.StatusOK,
			wantContains: []string{"RestoreTableFromClusterSnapshotResponse", "IN_PROGRESS"},
		},
		{
			name:         "missing_cluster_id",
			body:         "Action=RestoreTableFromClusterSnapshot&Version=2012-12-01&SnapshotIdentifier=my-snap",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- Backend tests for TableRestoreStatus ----

func TestBackend_TableRestoreStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, b *redshift.InMemoryBackend)
		name string
	}{
		{
			name: "create_returns_in_progress",
			run: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				tr, err := b.CreateTableRestoreStatus(
					"my-cluster", "my-snap", "db1", "table1", "db1", "table1_restored",
				)
				require.NoError(t, err)
				assert.Equal(t, "IN_PROGRESS", tr.Status)
				assert.Equal(t, "my-cluster", tr.ClusterIdentifier)
				assert.NotEmpty(t, tr.TableRestoreRequestID)
			},
		},
		{
			name: "describe_returns_created",
			run: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateTableRestoreStatus("my-cluster", "snap-1", "db1", "t1", "db1", "t1_new")
				require.NoError(t, err)
				statuses, err := b.DescribeTableRestoreStatus("my-cluster")
				require.NoError(t, err)
				assert.Len(t, statuses, 1)
			},
		},
		{
			name: "missing_cluster_id_returns_error",
			run: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateTableRestoreStatus("", "snap-1", "db1", "t1", "db1", "t1_new")
				require.Error(t, err)
				assert.ErrorIs(t, err, redshift.ErrInvalidParameter)
			},
		},
		{
			name: "multiple_restores_unique_ids",
			run: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				tr1, err := b.CreateTableRestoreStatus("c1", "snap-1", "db1", "t1", "db1", "t1_new")
				require.NoError(t, err)
				tr2, err := b.CreateTableRestoreStatus("c1", "snap-1", "db1", "t2", "db1", "t2_new")
				require.NoError(t, err)
				assert.NotEqual(t, tr1.TableRestoreRequestID, tr2.TableRestoreRequestID)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("123456789012", "us-east-1")
			tt.run(t, b)
		})
	}
}

// TestHandler_RestoreTableFromClusterSnapshot_WireIncludesRequestTimeAndSnapshot
// locks in that RequestTime and SnapshotIdentifier are actually serialized on the
// response. Before this fix, SnapshotIdentifier was parsed from the request and
// then silently discarded (never stored on the backend record at all), and
// RequestTime was computed but never written into any XML response -- both were
// wire-level data loss bugs a real client relying on TableRestoreStatus would hit.
func TestHandler_RestoreTableFromClusterSnapshot_WireIncludesRequestTimeAndSnapshot(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()
	postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=tr-cluster")

	rec := postRedshiftForm(t, h, "Action=RestoreTableFromClusterSnapshot&Version=2012-12-01"+
		"&ClusterIdentifier=tr-cluster&SnapshotIdentifier=tr-snap"+
		"&SourceDatabaseName=db1&SourceTableName=t1&TargetDatabaseName=db1&NewTableName=t1_restored")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	body := rec.Body.String()
	assert.Contains(t, body, "<SnapshotIdentifier>tr-snap</SnapshotIdentifier>")
	assert.Contains(t, body, "<RequestTime>")
	assert.Contains(t, body, "<NewTableName>t1_restored</NewTableName>")

	// DescribeTableRestoreStatus must reflect the same data.
	rec = postRedshiftForm(t, h, "Action=DescribeTableRestoreStatus&Version=2012-12-01&ClusterIdentifier=tr-cluster")
	require.Equal(t, http.StatusOK, rec.Code)
	body = rec.Body.String()
	assert.Contains(t, body, "<SnapshotIdentifier>tr-snap</SnapshotIdentifier>")
	assert.Contains(t, body, "<RequestTime>")
}
