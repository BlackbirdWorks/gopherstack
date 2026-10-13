package neptune_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDBClusterSnapshot_SnapshotTypeFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		snapshotType string
		want         []string
		notWant      []string
	}{
		{
			name: "public", snapshotType: "public",
			want: []string{"snap-public"}, notWant: []string{"snap-shared", "snap-private"},
		},
		{
			name: "shared", snapshotType: "shared",
			want: []string{"snap-shared"}, notWant: []string{"snap-public", "snap-private"},
		},
		{
			name: "manual", snapshotType: "manual",
			want: []string{"snap-public", "snap-shared", "snap-private"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createCluster(t, h, "stf-cluster")
			for _, snap := range []string{"snap-public", "snap-shared", "snap-private"} {
				doRequest(t, h, url.Values{
					"Action":                      {"CreateDBClusterSnapshot"},
					"Version":                     {"2014-10-31"},
					"DBClusterSnapshotIdentifier": {snap},
					"DBClusterIdentifier":         {"stf-cluster"},
				})
			}
			for snap, val := range map[string]string{"snap-public": "all", "snap-shared": "123456789012"} {
				rr := doRequest(t, h, url.Values{
					"Action":                       {"ModifyDBClusterSnapshotAttribute"},
					"Version":                      {"2014-10-31"},
					"DBClusterSnapshotIdentifier":  {snap},
					"AttributeName":                {"restore"},
					"ValuesToAdd.AttributeValue.1": {val},
				})
				require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
			}

			rr := doRequest(t, h, url.Values{
				"Action":       {"DescribeDBClusterSnapshots"},
				"Version":      {"2014-10-31"},
				"SnapshotType": {tt.snapshotType},
			})
			require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
			for _, w := range tt.want {
				assert.Contains(t, rr.Body.String(), "<DBClusterSnapshotIdentifier>"+w+"<")
			}
			for _, w := range tt.notWant {
				assert.NotContains(t, rr.Body.String(), "<DBClusterSnapshotIdentifier>"+w+"<")
			}
		})
	}
}
