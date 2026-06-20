package rds_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_CreateDBCluster_BackupRetentionPeriodBounds verifies that
// CreateDBCluster validates BackupRetentionPeriod within the AWS-allowed
// range [1, 35]. Real AWS returns InvalidParameterValue for out-of-range values
// and defaults to 1 when the parameter is omitted.
func TestParity_CreateDBCluster_BackupRetentionPeriodBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		retention string
		wantCode  int
	}{
		{
			name:      "zero_rejected",
			retention: "0",
			wantCode:  http.StatusBadRequest,
		},
		{
			name:      "above_maximum_rejected",
			retention: "36",
			wantCode:  http.StatusBadRequest,
		},
		{
			name:      "minimum_boundary_accepted",
			retention: "1",
			wantCode:  http.StatusOK,
		},
		{
			name:      "maximum_boundary_accepted",
			retention: "35",
			wantCode:  http.StatusOK,
		},
		{
			name:      "mid_range_accepted",
			retention: "7",
			wantCode:  http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRDSHandler()
			body := "Action=CreateDBCluster" +
				"&DBClusterIdentifier=test-cluster-" + tt.name +
				"&Engine=aurora-mysql" +
				"&BackupRetentionPeriod=" + tt.retention

			rec := postRDSForm(t, h, body)
			assert.Equal(t, tt.wantCode, rec.Code,
				"CreateDBCluster BackupRetentionPeriod=%s", tt.retention)

			if tt.wantCode == http.StatusBadRequest {
				assert.Contains(t, rec.Body.String(), "Error",
					"expected error response for BackupRetentionPeriod=%s", tt.retention)
			}
		})
	}
}

// TestParity_CreateDBCluster_BackupRetentionPeriodDefault verifies that
// CreateDBCluster defaults BackupRetentionPeriod to 1 when omitted.
// Real AWS documents this default and includes it in DescribeDBClusters output.
func TestParity_CreateDBCluster_BackupRetentionPeriodDefault(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()
	body := "Action=CreateDBCluster" +
		"&DBClusterIdentifier=default-retention-cluster" +
		"&Engine=aurora-postgresql"

	rec := postRDSForm(t, h, body)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "<BackupRetentionPeriod>1</BackupRetentionPeriod>",
		"default BackupRetentionPeriod should be 1")
}

// TestParity_CreateDBCluster_BackupRetentionPeriodPersisted verifies that an
// explicitly set BackupRetentionPeriod is round-tripped through DescribeDBClusters.
func TestParity_CreateDBCluster_BackupRetentionPeriodPersisted(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()

	createBody := "Action=CreateDBCluster" +
		"&DBClusterIdentifier=ret-cluster" +
		"&Engine=aurora-mysql" +
		"&BackupRetentionPeriod=14"

	createRec := postRDSForm(t, h, createBody)
	require.Equal(t, http.StatusOK, createRec.Code)

	describeBody := "Action=DescribeDBClusters&DBClusterIdentifier=ret-cluster"
	describeRec := postRDSForm(t, h, describeBody)
	require.Equal(t, http.StatusOK, describeRec.Code)
	assert.Contains(t, describeRec.Body.String(), "<BackupRetentionPeriod>14</BackupRetentionPeriod>",
		"BackupRetentionPeriod=14 should be returned by DescribeDBClusters")
}
