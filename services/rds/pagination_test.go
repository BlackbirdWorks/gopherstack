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

const (
	describeMinPage = 20
	describeSeeded  = describeMinPage + 1
)

func TestRDSHandler_DescribePagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create  func(id string) string
		name    string
		action  string
		prefix  string
		prelude []string
	}{
		{
			name:   "instances pagination",
			action: "DescribeDBInstances",
			prefix: "db-",
			create: func(id string) string {
				return "Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=" + id
			},
		},
		{
			name:   "snapshots pagination",
			action: "DescribeDBSnapshots",
			prefix: "snap-",
			prelude: []string{
				"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=snap-db",
			},
			create: func(id string) string {
				return "Action=CreateDBSnapshot&Version=2014-10-31&DBSnapshotIdentifier=" + id +
					"&DBInstanceIdentifier=snap-db"
			},
		},
		{
			name:   "clusters pagination",
			action: "DescribeDBClusters",
			prefix: "cluster-",
			create: func(id string) string {
				return "Action=CreateDBCluster&Version=2014-10-31&DBClusterIdentifier=" + id +
					"&Engine=aurora-postgresql"
			},
		},
		{
			name:   "subnet groups pagination",
			action: "DescribeDBSubnetGroups",
			prefix: "subnet-",
			create: func(id string) string {
				return "Action=CreateDBSubnetGroup&Version=2014-10-31&DBSubnetGroupName=" + id
			},
		},
		{
			name:   "cluster snapshots pagination",
			action: "DescribeDBClusterSnapshots",
			prefix: "csnap-",
			prelude: []string{
				"Action=CreateDBCluster&Version=2014-10-31&DBClusterIdentifier=page-clu" +
					"&Engine=aurora-postgresql&MasterUsername=admin&MasterUserPassword=password123",
			},
			create: func(id string) string {
				return "Action=CreateDBClusterSnapshot&Version=2014-10-31" +
					"&DBClusterSnapshotIdentifier=" + id + "&DBClusterIdentifier=page-clu"
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRDSHandler(t)
			for _, body := range tt.prelude {
				require.Equal(t, http.StatusOK, postRDSForm(t, h, body).Code)
			}
			for i := describeSeeded; i >= 1; i-- {
				rec := postRDSForm(t, h, tt.create(fmt.Sprintf("%s%02d", tt.prefix, i)))
				require.Equal(t, http.StatusOK, rec.Code)
			}

			firstReq := fmt.Sprintf("Action=%s&Version=2014-10-31&MaxRecords=%d", tt.action, describeMinPage)
			firstPage := postRDSForm(t, h, firstReq)
			require.Equal(t, http.StatusOK, firstPage.Code)

			firstBody := firstPage.Body.String()
			assert.Contains(t, firstBody, fmt.Sprintf("%s%02d<", tt.prefix, 1))
			assert.Contains(t, firstBody, fmt.Sprintf("%s%02d<", tt.prefix, describeMinPage))
			assert.NotContains(t, firstBody, fmt.Sprintf("%s%02d<", tt.prefix, describeSeeded))

			marker := extractXMLMarker(firstBody)
			require.NotEmpty(t, marker)

			secondReq := fmt.Sprintf(
				"Action=%s&Version=2014-10-31&MaxRecords=%d&Marker=%s",
				tt.action,
				describeMinPage,
				url.QueryEscape(marker),
			)
			secondPage := postRDSForm(t, h, secondReq)
			require.Equal(t, http.StatusOK, secondPage.Code)

			secondBody := secondPage.Body.String()
			assert.Contains(t, secondBody, fmt.Sprintf("%s%02d<", tt.prefix, describeSeeded))
			assert.NotContains(t, secondBody, fmt.Sprintf("%s%02d<", tt.prefix, 1))
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
		{name: "below minimum", maxRecord: "19"},
		{name: "above maximum", maxRecord: "101"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRDSHandler(t)
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

	h := newRDSHandler(t)
	for i := range describeSeeded {
		rec := postRDSForm(
			t,
			h,
			fmt.Sprintf(
				"Action=CreateDBInstance&Version=2014-10-31&DBInstanceIdentifier=evt-db-%02d&Engine=postgres",
				i,
			),
		)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	firstPage := postRDSForm(t, h, "Action=DescribeEvents&Version=2014-10-31&MaxRecords=20")
	require.Equal(t, http.StatusOK, firstPage.Code)

	firstBody := firstPage.Body.String()
	marker := extractXMLMarker(firstBody)
	require.NotEmpty(t, marker, "first page of 21 events with MaxRecords=20 must return a Marker")

	secondPage := postRDSForm(t, h, fmt.Sprintf(
		"Action=DescribeEvents&Version=2014-10-31&MaxRecords=20&Marker=%s",
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

func TestRDSHandler_DescribeMaxRecordsBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		action     string
		extra      string
		maxRecords int
		wantOK     bool
	}{
		{name: "instances at minimum", action: "DescribeDBInstances", maxRecords: 20, wantOK: true},
		{name: "instances at maximum", action: "DescribeDBInstances", maxRecords: 100, wantOK: true},
		{name: "instances below minimum", action: "DescribeDBInstances", maxRecords: 19},
		{name: "instances above maximum", action: "DescribeDBInstances", maxRecords: 101},
		{
			name: "orderable above 100", action: "DescribeOrderableDBInstanceOptions",
			extra: "&Engine=postgres", maxRecords: 1000, wantOK: true,
		},
		{
			name: "orderable above 1000", action: "DescribeOrderableDBInstanceOptions",
			extra: "&Engine=postgres", maxRecords: 1001,
		},
		{name: "serverless v2 minimum 1", action: "DescribeServerlessV2PlatformVersions", maxRecords: 1, wantOK: true},
		{name: "serverless v2 above 200", action: "DescribeServerlessV2PlatformVersions", maxRecords: 201},
		{name: "tenant databases undocumented", action: "DescribeTenantDatabases", maxRecords: 1, wantOK: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRDSHandler(t)
			rec := postRDSForm(t, h, fmt.Sprintf(
				"Action=%s&Version=2014-10-31&MaxRecords=%d%s", tt.action, tt.maxRecords, tt.extra))
			if tt.wantOK {
				assert.Equal(t, http.StatusOK, rec.Code)

				return
			}

			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), "InvalidParameterValue")
		})
	}
}
