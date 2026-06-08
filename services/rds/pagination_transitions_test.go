package rds_test

import (
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
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

func TestRDSBackend_InstanceModifyTransitionAndDeletePublishesEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "create modify delete transitions"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			const instanceID = "transition-db"

			created, err := b.CreateDBInstance(instanceID, "postgres", "", "", "", "", 20, rds.DBInstanceOptions{})
			require.NoError(t, err)
			assert.Equal(t, "creating", created.DBInstanceStatus)

			modified, err := b.ModifyDBInstance(instanceID, "db.r5.large", 100, rds.DBInstanceOptions{})
			require.NoError(t, err)
			assert.Equal(t, "modifying", modified.DBInstanceStatus)

			require.Eventually(t, func() bool {
				instances, describeErr := b.DescribeDBInstances(instanceID)
				if describeErr != nil || len(instances) != 1 {
					return false
				}

				return instances[0].DBInstanceStatus == "available" && instances[0].DBInstanceClass == "db.r5.large"
			}, 3*time.Second, 20*time.Millisecond)

			deleted, err := b.DeleteDBInstance(instanceID)
			require.NoError(t, err)
			assert.Equal(t, "deleting", deleted.DBInstanceStatus)
			_, err = b.DescribeDBInstances(instanceID)
			require.ErrorIs(t, err, rds.ErrInstanceNotFound)

			messages := rds.EventMessagesForSource(b, instanceID)
			assert.Contains(t, messages, "DB instance created")
			assert.Contains(t, messages, "DB instance is now available")
			assert.Contains(t, messages, "DB instance modification started")
			assert.Contains(t, messages, "DB instance deletion started")
			assert.Contains(t, messages, "DB instance deleted")
		})
	}
}

func extractXMLMarker(body string) string {
	match := regexp.MustCompile(`<Marker>([^<]+)</Marker>`).FindStringSubmatch(body)
	if len(match) < 2 {
		return ""
	}

	return match[1]
}
