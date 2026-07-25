package rds_test

import (
	"encoding/xml"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDescribeDBClusters_Filters verifies AWS's DescribeDBClusters
// Filters.Filter.N.Name/Values.member.M contract: db-cluster-id,
// db-cluster-resource-id, and engine narrow the result set (OR within a
// filter's Values, AND across filters); clone-group-id and domain are
// accepted but not modeled (vacuous match, matching the existing
// DescribeDBInstances "domain" precedent); an unrecognized filter name
// returns InvalidParameterValue.
func TestDescribeDBClusters_Filters(t *testing.T) {
	t.Parallel()

	type describeResp struct {
		XMLName xml.Name `xml:"DescribeDBClustersResponse"`
		Result  struct {
			DBClusters struct {
				Members []struct {
					DBClusterIdentifier string `xml:"DBClusterIdentifier"`
					DBClusterResourceID string `xml:"DbClusterResourceId"`
				} `xml:"DBCluster"`
			} `xml:"DBClusters"`
		} `xml:"DescribeDBClustersResult"`
	}

	cases := []struct {
		name        string
		query       string
		wantErrText string
		wantIDs     []string
		wantCode    int
	}{
		{
			name:     "engine filter matches only aurora-mysql clusters",
			query:    "Filters.Filter.1.Name=engine&Filters.Filter.1.Values.member.1=aurora-mysql",
			wantCode: http.StatusOK,
			wantIDs:  []string{"filt-mysql-clu"},
		},
		{
			name: "db-cluster-id filter with multiple values ORs together",
			query: "Filters.Filter.1.Name=db-cluster-id" +
				"&Filters.Filter.1.Values.member.1=filt-mysql-clu" +
				"&Filters.Filter.1.Values.member.2=filt-pg-clu",
			wantCode: http.StatusOK,
			wantIDs:  []string{"filt-mysql-clu", "filt-pg-clu"},
		},
		{
			name: "two filters AND together",
			query: "Filters.Filter.1.Name=engine&Filters.Filter.1.Values.member.1=aurora-postgresql" +
				"&Filters.Filter.2.Name=db-cluster-id&Filters.Filter.2.Values.member.1=filt-mysql-clu",
			wantCode: http.StatusOK,
			wantIDs:  nil,
		},
		{
			name:     "domain filter is accepted but vacuous",
			query:    "Filters.Filter.1.Name=domain&Filters.Filter.1.Values.member.1=d-1",
			wantCode: http.StatusOK,
			wantIDs:  []string{"filt-mysql-clu", "filt-pg-clu"},
		},
		{
			name:        "unrecognized filter name is rejected",
			query:       "Filters.Filter.1.Name=bogus-filter&Filters.Filter.1.Values.member.1=x",
			wantCode:    http.StatusBadRequest,
			wantErrText: "InvalidParameterValue",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRDSHandler()
			postRDSForm(t, h,
				"Action=CreateDBCluster&Version=2014-10-31"+
					"&DBClusterIdentifier=filt-mysql-clu&Engine=aurora-mysql"+
					"&MasterUsername=admin&MasterUserPassword=password123")
			postRDSForm(t, h,
				"Action=CreateDBCluster&Version=2014-10-31"+
					"&DBClusterIdentifier=filt-pg-clu&Engine=aurora-postgresql"+
					"&MasterUsername=admin&MasterUserPassword=password123")

			body := "Action=DescribeDBClusters&Version=2014-10-31"
			if tt.query != "" {
				body += "&" + tt.query
			}
			rec := postRDSForm(t, h, body)

			require.Equal(t, tt.wantCode, rec.Code)
			if tt.wantErrText != "" {
				assert.Contains(t, rec.Body.String(), tt.wantErrText)

				return
			}

			var resp describeResp
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

			gotIDs := make([]string, 0, len(resp.Result.DBClusters.Members))
			for _, m := range resp.Result.DBClusters.Members {
				gotIDs = append(gotIDs, m.DBClusterIdentifier)
				// Regression check for the DbClusterResourceId wire-shape
				// gap: every returned cluster must carry a resource ID.
				assert.NotEmpty(t, m.DBClusterResourceID)
			}
			assert.ElementsMatch(t, tt.wantIDs, gotIDs)
		})
	}
}

// TestDescribeDBSnapshots_Filters verifies AWS's DescribeDBSnapshots
// Filters.Filter.N.Name/Values.member.M contract: db-instance-id,
// db-snapshot-id, snapshot-type, and engine narrow the result set; an
// unrecognized filter name returns InvalidParameterValue.
func TestDescribeDBSnapshots_Filters(t *testing.T) {
	t.Parallel()

	type describeResp struct {
		XMLName xml.Name `xml:"DescribeDBSnapshotsResponse"`
		Result  struct {
			DBSnapshots struct {
				Members []struct {
					DBSnapshotIdentifier string `xml:"DBSnapshotIdentifier"`
					DbiResourceID        string `xml:"DbiResourceId"`
				} `xml:"DBSnapshot"`
			} `xml:"DBSnapshots"`
		} `xml:"DescribeDBSnapshotsResult"`
	}

	cases := []struct {
		name        string
		query       string
		wantErrText string
		wantIDs     []string
		wantCode    int
	}{
		{
			name:     "snapshot-type filter matches only manual snapshots",
			query:    "Filters.Filter.1.Name=snapshot-type&Filters.Filter.1.Values.member.1=manual",
			wantCode: http.StatusOK,
			wantIDs:  []string{"filt-snap-1", "filt-snap-2"},
		},
		{
			name: "db-snapshot-id filter with multiple values ORs together",
			query: "Filters.Filter.1.Name=db-snapshot-id" +
				"&Filters.Filter.1.Values.member.1=filt-snap-1" +
				"&Filters.Filter.1.Values.member.2=filt-snap-2",
			wantCode: http.StatusOK,
			wantIDs:  []string{"filt-snap-1", "filt-snap-2"},
		},
		{
			name:     "db-instance-id filter narrows to one snapshot",
			query:    "Filters.Filter.1.Name=db-instance-id&Filters.Filter.1.Values.member.1=filt-snap-db-1",
			wantCode: http.StatusOK,
			wantIDs:  []string{"filt-snap-1"},
		},
		{
			name:        "unrecognized filter name is rejected",
			query:       "Filters.Filter.1.Name=bogus-filter&Filters.Filter.1.Values.member.1=x",
			wantCode:    http.StatusBadRequest,
			wantErrText: "InvalidParameterValue",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRDSHandler()
			postRDSForm(t, h,
				"Action=CreateDBInstance&Version=2014-10-31"+
					"&DBInstanceIdentifier=filt-snap-db-1&Engine=postgres")
			postRDSForm(t, h,
				"Action=CreateDBInstance&Version=2014-10-31"+
					"&DBInstanceIdentifier=filt-snap-db-2&Engine=postgres")
			postRDSForm(t, h,
				"Action=CreateDBSnapshot&Version=2014-10-31"+
					"&DBSnapshotIdentifier=filt-snap-1&DBInstanceIdentifier=filt-snap-db-1")
			postRDSForm(t, h,
				"Action=CreateDBSnapshot&Version=2014-10-31"+
					"&DBSnapshotIdentifier=filt-snap-2&DBInstanceIdentifier=filt-snap-db-2")

			body := "Action=DescribeDBSnapshots&Version=2014-10-31"
			if tt.query != "" {
				body += "&" + tt.query
			}
			rec := postRDSForm(t, h, body)

			require.Equal(t, tt.wantCode, rec.Code)
			if tt.wantErrText != "" {
				assert.Contains(t, rec.Body.String(), tt.wantErrText)

				return
			}

			var resp describeResp
			require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))

			gotIDs := make([]string, 0, len(resp.Result.DBSnapshots.Members))
			for _, m := range resp.Result.DBSnapshots.Members {
				gotIDs = append(gotIDs, m.DBSnapshotIdentifier)
				// Regression check for the DbiResourceId wire-shape gap.
				assert.NotEmpty(t, m.DbiResourceID)
			}
			assert.ElementsMatch(t, tt.wantIDs, gotIDs)
		})
	}
}

// TestDescribeDBClusterSnapshots_Filters verifies AWS's
// DescribeDBClusterSnapshots Filters.Filter.N.Name/Values.member.M contract
// and that the op now paginates via Marker/MaxRecords like every other
// Describe op (it previously returned every cluster snapshot unpaginated).
func TestDescribeDBClusterSnapshots_Filters(t *testing.T) {
	t.Parallel()

	type describeResp struct {
		XMLName xml.Name `xml:"DescribeDBClusterSnapshotsResponse"`
		Result  struct {
			Marker             string `xml:"Marker"`
			DBClusterSnapshots struct {
				Members []struct {
					DBClusterSnapshotIdentifier string `xml:"DBClusterSnapshotIdentifier"`
					DBClusterResourceID         string `xml:"DbClusterResourceId"`
					SnapshotType                string `xml:"SnapshotType"`
				} `xml:"DBClusterSnapshot"`
			} `xml:"DBClusterSnapshots"`
		} `xml:"DescribeDBClusterSnapshotsResult"`
	}

	h := newRDSHandler()
	postRDSForm(t, h,
		"Action=CreateDBCluster&Version=2014-10-31"+
			"&DBClusterIdentifier=filt-csnap-clu&Engine=aurora-mysql"+
			"&MasterUsername=admin&MasterUserPassword=password123")
	postRDSForm(t, h,
		"Action=CreateDBClusterSnapshot&Version=2014-10-31"+
			"&DBClusterSnapshotIdentifier=filt-csnap-1&DBClusterIdentifier=filt-csnap-clu")
	postRDSForm(t, h,
		"Action=CreateDBClusterSnapshot&Version=2014-10-31"+
			"&DBClusterSnapshotIdentifier=filt-csnap-2&DBClusterIdentifier=filt-csnap-clu")

	t.Run("snapshot-type filter matches manual snapshots", func(t *testing.T) {
		t.Parallel()

		rec := postRDSForm(t, h,
			"Action=DescribeDBClusterSnapshots&Version=2014-10-31"+
				"&Filters.Filter.1.Name=snapshot-type&Filters.Filter.1.Values.member.1=manual")
		require.Equal(t, http.StatusOK, rec.Code)

		var resp describeResp
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
		gotIDs := make([]string, 0, len(resp.Result.DBClusterSnapshots.Members))
		for _, m := range resp.Result.DBClusterSnapshots.Members {
			gotIDs = append(gotIDs, m.DBClusterSnapshotIdentifier)
			assert.Equal(t, "manual", m.SnapshotType)
			assert.NotEmpty(t, m.DBClusterResourceID)
		}
		assert.ElementsMatch(t, []string{"filt-csnap-1", "filt-csnap-2"}, gotIDs)
	})

	t.Run("unrecognized filter name is rejected", func(t *testing.T) {
		t.Parallel()

		rec := postRDSForm(t, h,
			"Action=DescribeDBClusterSnapshots&Version=2014-10-31"+
				"&Filters.Filter.1.Name=bogus-filter&Filters.Filter.1.Values.member.1=x")
		require.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
	})

	t.Run("MaxRecords paginates the result", func(t *testing.T) {
		t.Parallel()

		rec := postRDSForm(t, h, "Action=DescribeDBClusterSnapshots&Version=2014-10-31&MaxRecords=1")
		require.Equal(t, http.StatusOK, rec.Code)

		var resp describeResp
		require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Len(t, resp.Result.DBClusterSnapshots.Members, 1)
		assert.NotEmpty(t, resp.Result.Marker)
	})
}
