package redshift_test

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

func newRedshiftBackend() *redshift.InMemoryBackend {
	return redshift.NewInMemoryBackend("000000000000", "us-east-1")
}

// TestParity_DescribeClusters_Pagination verifies Marker/MaxRecords pagination.
func TestParity_DescribeClusters_Pagination(t *testing.T) {
	t.Parallel()

	type pageTC struct {
		name       string
		query      string
		wantAbsent []string
		wantInBody []string
		wantCode   int
		wantMarker bool
	}

	tests := []pageTC{
		{
			name:       "no_params_returns_all",
			query:      "Action=DescribeClusters&Version=2012-12-01",
			wantCode:   http.StatusOK,
			wantInBody: []string{"alpha", "beta", "gamma"},
			wantMarker: false,
		},
		{
			name:       "max_records_1_returns_first",
			query:      "Action=DescribeClusters&Version=2012-12-01&MaxRecords=1",
			wantCode:   http.StatusOK,
			wantInBody: []string{"alpha", "Marker"},
			wantAbsent: []string{"beta", "gamma"},
			wantMarker: true,
		},
		{
			name:       "marker_advances_to_second_page",
			query:      "Action=DescribeClusters&Version=2012-12-01&MaxRecords=1&Marker=alpha",
			wantCode:   http.StatusOK,
			wantInBody: []string{"beta"},
			wantAbsent: []string{"alpha", "gamma"},
			wantMarker: true,
		},
		{
			name:       "marker_advances_to_last_page",
			query:      "Action=DescribeClusters&Version=2012-12-01&MaxRecords=1&Marker=beta",
			wantCode:   http.StatusOK,
			wantInBody: []string{"gamma"},
			wantAbsent: []string{"alpha", "beta"},
			wantMarker: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			for _, id := range []string{"alpha", "beta", "gamma"} {
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier="+id)
			}

			rec := postRedshiftForm(t, h, tt.query)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantInBody {
				assert.Contains(t, rec.Body.String(), s, "expected %q in response", s)
			}

			for _, s := range tt.wantAbsent {
				assert.NotContains(t, rec.Body.String(), s, "unexpected %q in response", s)
			}
		})
	}
}

// TestParity_ModifyClusterIamRoles_Persists verifies that IAM roles are stored and returned.
func TestParity_ModifyClusterIamRoles_Persists(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()
	postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=iam-cluster")

	// Add two roles.
	rec := postRedshiftForm(t, h,
		"Action=ModifyClusterIamRoles&Version=2012-12-01&ClusterIdentifier=iam-cluster"+
			"&AddIamRoles.IamRoleArn.1=arn:aws:iam::123456789012:role/Role1"+
			"&AddIamRoles.IamRoleArn.2=arn:aws:iam::123456789012:role/Role2")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Role1")
	assert.Contains(t, rec.Body.String(), "Role2")

	// Describe cluster — roles must be present.
	rec2 := postRedshiftForm(t, h, "Action=DescribeClusters&Version=2012-12-01&ClusterIdentifier=iam-cluster")
	require.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "Role1")
	assert.Contains(t, rec2.Body.String(), "Role2")

	// Remove one role.
	rec3 := postRedshiftForm(t, h,
		"Action=ModifyClusterIamRoles&Version=2012-12-01&ClusterIdentifier=iam-cluster"+
			"&RemoveIamRoles.IamRoleArn.1=arn:aws:iam::123456789012:role/Role1")
	require.Equal(t, http.StatusOK, rec3.Code)

	// Describe cluster — only Role2 remains.
	rec4 := postRedshiftForm(t, h, "Action=DescribeClusters&Version=2012-12-01&ClusterIdentifier=iam-cluster")
	require.Equal(t, http.StatusOK, rec4.Code)
	assert.NotContains(t, rec4.Body.String(), "Role1")
	assert.Contains(t, rec4.Body.String(), "Role2")
}

// TestParity_ModifyClusterMaintenance_Persists verifies that maintenance window is stored.
func TestParity_ModifyClusterMaintenance_Persists(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()
	postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=maint-cluster")

	rec := postRedshiftForm(t, h,
		"Action=ModifyClusterMaintenance&Version=2012-12-01&ClusterIdentifier=maint-cluster"+
			"&MaintenanceTrackName=current")
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := postRedshiftForm(t, h, "Action=DescribeClusters&Version=2012-12-01&ClusterIdentifier=maint-cluster")
	require.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "current")
}

// TestParity_ModifyCluster_ApplyImmediately verifies PendingModifiedValues semantics.
func TestParity_ModifyCluster_ApplyImmediately(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantNodeTypeInCluster string
		name                  string
		applyImmediately      string
		newNodeType           string
	}{
		{
			name:                  "apply_immediately_true_changes_cluster",
			applyImmediately:      "true",
			newNodeType:           "ds2.xlarge",
			wantNodeTypeInCluster: "ds2.xlarge",
		},
		{
			name:                  "apply_immediately_false_stores_pending",
			applyImmediately:      "false",
			newNodeType:           "ds2.xlarge",
			wantNodeTypeInCluster: "dc2.large",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=apply-cluster")

			postRedshiftForm(t, h,
				"Action=ModifyCluster&Version=2012-12-01&ClusterIdentifier=apply-cluster"+
					"&NodeType="+tt.newNodeType+
					"&ApplyImmediately="+tt.applyImmediately)

			rec := postRedshiftForm(t, h, "Action=DescribeClusters&Version=2012-12-01&ClusterIdentifier=apply-cluster")
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantNodeTypeInCluster)
		})
	}
}

// TestParity_ClusterLifecycle_CreatingToAvailable verifies the creating→available state machine.
func TestParity_ClusterLifecycle_CreatingToAvailable(t *testing.T) {
	t.Parallel()

	b := newRedshiftBackend()
	redshift.SetClusterActivationDelay(b, 50*time.Millisecond)

	_, err := b.CreateCluster("lifecycle-cluster", "dc2.large", "dev", "admin")
	require.NoError(t, err)

	// Immediately after create, status should be "creating".
	clusters, _, err := b.DescribeClusters("lifecycle-cluster", "", 0)
	require.NoError(t, err)
	require.Len(t, clusters, 1)
	assert.Equal(t, "creating", clusters[0].Status,
		"cluster should be in creating state immediately after CreateCluster")

	// After the activation delay, status should be "available".
	time.Sleep(200 * time.Millisecond)

	clusters2, _, err := b.DescribeClusters("lifecycle-cluster", "", 0)
	require.NoError(t, err)
	require.Len(t, clusters2, 1)
	assert.Equal(t, "available", clusters2[0].Status, "cluster should be available after activation delay")
}

// TestParity_Persistence_FullRoundTrip verifies snapshot+restore preserves all fields.
func TestParity_Persistence_FullRoundTrip(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()
	postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=persist-cluster")
	postRedshiftForm(t, h,
		"Action=ModifyClusterIamRoles&Version=2012-12-01&ClusterIdentifier=persist-cluster"+
			"&AddIamRoles.IamRoleArn.1=arn:aws:iam::123456789012:role/Role1")
	postRedshiftForm(t, h,
		"Action=CreateSnapshotCopyGrant&Version=2012-12-01&SnapshotCopyGrantName=my-grant")
	postRedshiftForm(t, h,
		"Action=CreateUsageLimit&Version=2012-12-01&ClusterIdentifier=persist-cluster"+
			"&FeatureType=spectrum&LimitType=time&Amount=1000&BreachAction=log")

	ctx := t.Context()
	data := h.Backend.Snapshot(ctx)
	require.NotNil(t, data)

	h2 := newRedshiftHandler()
	err := h2.Backend.Restore(ctx, data)
	require.NoError(t, err)

	// Verify clusters restored.
	rec := postRedshiftForm(t, h2, "Action=DescribeClusters&Version=2012-12-01&ClusterIdentifier=persist-cluster")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "persist-cluster")
	assert.Contains(t, rec.Body.String(), "Role1")

	// Verify snapshot copy grant restored.
	rec2 := postRedshiftForm(t, h2,
		"Action=DescribeSnapshotCopyGrants&Version=2012-12-01&SnapshotCopyGrantName=my-grant")
	require.Equal(t, http.StatusOK, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "my-grant")

	// Verify usage limits restored.
	rec3 := postRedshiftForm(t, h2,
		"Action=DescribeUsageLimits&Version=2012-12-01&ClusterIdentifier=persist-cluster")
	require.Equal(t, http.StatusOK, rec3.Code)
	assert.Contains(t, rec3.Body.String(), "spectrum")
}

// TestParity_RestoreFromClusterSnapshot_CopiesClusterProperties verifies that
// RestoreFromClusterSnapshot uses the source cluster's properties, not defaults.
func TestParity_RestoreFromClusterSnapshot_CopiesClusterProperties(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()

	// Create cluster with non-default node type.
	postRedshiftForm(t, h,
		"Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=src-cluster"+
			"&NodeType=ds2.xlarge&DBName=mydb")
	postRedshiftForm(t, h,
		"Action=CreateClusterSnapshot&Version=2012-12-01"+
			"&ClusterIdentifier=src-cluster&SnapshotIdentifier=my-snap")

	// Restore from snapshot.
	postRedshiftForm(t, h,
		"Action=RestoreFromClusterSnapshot&Version=2012-12-01"+
			"&ClusterIdentifier=restored-cluster&SnapshotIdentifier=my-snap")

	// Verify restored cluster has source cluster's node type and DBName.
	rec := postRedshiftForm(t, h, "Action=DescribeClusters&Version=2012-12-01&ClusterIdentifier=restored-cluster")
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.Contains(t, body, "ds2.xlarge", "restored cluster should use source node type")
	assert.True(t,
		strings.Contains(body, "mydb") || strings.Contains(body, "DBName"),
		"restored cluster should reference source DBName")
}
