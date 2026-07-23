package rds_test

import (
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRDSHandler_DescribePagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupBodies   []string
		name          string
		action        string
		wantFirstPage []string
		wantSecond    []string
	}{
		{
			name:   "instances pagination",
			action: "DescribeDBInstances",
			setupBodies: []string{
				"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=db-03",
				"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=db-01",
				"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=db-02",
			},
			wantFirstPage: []string{"db-01", "db-02"},
			wantSecond:    []string{"db-03"},
		},
		{
			name:   "snapshots pagination",
			action: "DescribeDBSnapshots",
			setupBodies: []string{
				"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=snap-db",
				"Action=CreateDBSnapshot&Version=2014-10-31&DBSnapshotIdentifier=snap-03&DBInstanceIdentifier=snap-db",
				"Action=CreateDBSnapshot&Version=2014-10-31&DBSnapshotIdentifier=snap-01&DBInstanceIdentifier=snap-db",
				"Action=CreateDBSnapshot&Version=2014-10-31&DBSnapshotIdentifier=snap-02&DBInstanceIdentifier=snap-db",
			},
			wantFirstPage: []string{"snap-01", "snap-02"},
			wantSecond:    []string{"snap-03"},
		},
		{
			name:   "clusters pagination",
			action: "DescribeDBClusters",
			setupBodies: []string{
				"Action=CreateDBCluster&Version=2014-10-31&DBClusterIdentifier=cluster-03&Engine=aurora-postgresql",
				"Action=CreateDBCluster&Version=2014-10-31&DBClusterIdentifier=cluster-01&Engine=aurora-postgresql",
				"Action=CreateDBCluster&Version=2014-10-31&DBClusterIdentifier=cluster-02&Engine=aurora-postgresql",
			},
			wantFirstPage: []string{"cluster-01", "cluster-02"},
			wantSecond:    []string{"cluster-03"},
		},
		{
			name:   "subnet groups pagination",
			action: "DescribeDBSubnetGroups",
			setupBodies: []string{
				"Action=CreateDBSubnetGroup&Version=2014-10-31&DBSubnetGroupName=subnet-03",
				"Action=CreateDBSubnetGroup&Version=2014-10-31&DBSubnetGroupName=subnet-01",
				"Action=CreateDBSubnetGroup&Version=2014-10-31&DBSubnetGroupName=subnet-02",
			},
			wantFirstPage: []string{"subnet-01", "subnet-02"},
			wantSecond:    []string{"subnet-03"},
		},
		{
			// Regression test for a missing-pagination gap: real AWS's
			// DescribeDBClusterSnapshotsOutput/DescribeEventsOutput both
			// carry a Marker field, but this emulator previously returned
			// every cluster snapshot / event unpaginated regardless of
			// MaxRecords.
			name:   "cluster snapshots pagination",
			action: "DescribeDBClusterSnapshots",
			setupBodies: []string{
				"Action=CreateDBCluster&Version=2014-10-31&DBClusterIdentifier=page-clu" +
					"&Engine=aurora-postgresql&MasterUsername=admin&MasterUserPassword=password123",
				"Action=CreateDBClusterSnapshot&Version=2014-10-31" +
					"&DBClusterSnapshotIdentifier=csnap-03&DBClusterIdentifier=page-clu",
				"Action=CreateDBClusterSnapshot&Version=2014-10-31" +
					"&DBClusterSnapshotIdentifier=csnap-01&DBClusterIdentifier=page-clu",
				"Action=CreateDBClusterSnapshot&Version=2014-10-31" +
					"&DBClusterSnapshotIdentifier=csnap-02&DBClusterIdentifier=page-clu",
			},
			wantFirstPage: []string{"csnap-01", "csnap-02"},
			wantSecond:    []string{"csnap-03"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRDSHandler()
			for _, setupBody := range tt.setupBodies {
				rec := postRDSForm(t, h, setupBody)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			firstReq := fmt.Sprintf("Action=%s&Version=2014-10-31&MaxRecords=2", tt.action)
			firstPage := postRDSForm(t, h, firstReq)
			require.Equal(t, http.StatusOK, firstPage.Code)

			firstBody := firstPage.Body.String()
			for _, id := range tt.wantFirstPage {
				assert.Contains(t, firstBody, id)
			}
			for _, id := range tt.wantSecond {
				assert.NotContains(t, firstBody, id)
			}

			marker := extractXMLMarker(firstBody)
			require.NotEmpty(t, marker)

			secondReq := fmt.Sprintf(
				"Action=%s&Version=2014-10-31&MaxRecords=2&Marker=%s",
				tt.action,
				url.QueryEscape(marker),
			)
			secondPage := postRDSForm(t, h, secondReq)
			require.Equal(t, http.StatusOK, secondPage.Code)

			secondBody := secondPage.Body.String()
			for _, id := range tt.wantSecond {
				assert.Contains(t, secondBody, id)
			}
			assert.NotContains(t, secondBody, "<Marker>")
		})
	}
}

func TestRDSHandler_DescribePagination_InvalidMaxRecords(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		maxRecord string
	}{
		{name: "non numeric", maxRecord: "abc"},
		{name: "zero", maxRecord: "0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRDSHandler()
			rec := postRDSForm(
				t,
				h,
				fmt.Sprintf("Action=DescribeDBInstances&Version=2014-10-31&MaxRecords=%s", tt.maxRecord),
			)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
		})
	}
}

// TestRDSHandler_DescribeEventsPagination is a regression test for a
// missing-pagination gap: real AWS's DescribeEventsOutput carries a Marker
// field, but this emulator previously returned every event unpaginated
// regardless of MaxRecords. Events are seeded as a side effect of
// CreateDBInstance ("DB instance created").
func TestRDSHandler_DescribeEventsPagination(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()
	for _, id := range []string{"evt-db-1", "evt-db-2", "evt-db-3"} {
		rec := postRDSForm(t, h,
			"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier="+id+"&Engine=postgres")
		require.Equal(t, http.StatusOK, rec.Code)
	}

	firstPage := postRDSForm(t, h, "Action=DescribeEvents&Version=2014-10-31&MaxRecords=2")
	require.Equal(t, http.StatusOK, firstPage.Code)

	firstBody := firstPage.Body.String()
	marker := extractXMLMarker(firstBody)
	require.NotEmpty(t, marker, "first page of 3 events with MaxRecords=2 must return a Marker")

	secondPage := postRDSForm(t, h, fmt.Sprintf(
		"Action=DescribeEvents&Version=2014-10-31&MaxRecords=2&Marker=%s",
		url.QueryEscape(marker),
	))
	require.Equal(t, http.StatusOK, secondPage.Code)
	assert.NotContains(t, secondPage.Body.String(), "<Marker>")
}

func extractXMLMarker(body string) string {
	match := regexp.MustCompile(`<Marker>([^<]+)</Marker>`).FindStringSubmatch(body)
	if len(match) < 2 {
		return ""
	}

	return match[1]
}
