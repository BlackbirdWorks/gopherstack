package docdb_test

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/docdb"
)

func TestHandler_SubnetGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "create_subnet_group",
			vals: url.Values{
				"Action":                       {"CreateDBSubnetGroup"},
				"Version":                      {"2014-10-31"},
				"DBSubnetGroupName":            {"my-sg"},
				"DBSubnetGroupDescription":     {"test sg"},
				"VpcId":                        {"vpc-12345"},
				"SubnetIds.SubnetIdentifier.1": {"subnet-aaa"},
				"SubnetIds.SubnetIdentifier.2": {"subnet-bbb"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "my-sg",
		},
		{
			name: "describe_subnet_groups_all",
			vals: url.Values{
				"Action":  {"DescribeDBSubnetGroups"},
				"Version": {"2014-10-31"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DescribeDBSubnetGroupsResponse",
		},
		{
			name: "describe_subnet_group_by_name",
			vals: url.Values{
				"Action":            {"DescribeDBSubnetGroups"},
				"Version":           {"2014-10-31"},
				"DBSubnetGroupName": {"my-sg"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "my-sg",
		},
		{
			name: "delete_subnet_group",
			vals: url.Values{
				"Action":            {"DeleteDBSubnetGroup"},
				"Version":           {"2014-10-31"},
				"DBSubnetGroupName": {"my-sg"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DeleteDBSubnetGroupResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.name != "create_subnet_group" {
				doRequest(t, h, url.Values{
					"Action":                   {"CreateDBSubnetGroup"},
					"Version":                  {"2014-10-31"},
					"DBSubnetGroupName":        {"my-sg"},
					"DBSubnetGroupDescription": {"test sg"},
				})
			}

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestSortedDescribeSubnetGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		names []string
		want  []string
	}{
		{
			name:  "sorted_order",
			names: []string{"sg-z", "sg-a", "sg-m"},
			want:  []string{"sg-a", "sg-m", "sg-z"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := docdb.NewInMemoryBackend("000000000000", "us-east-1")
			for _, name := range tt.names {
				b.AddDBSubnetGroupInternal(&docdb.DBSubnetGroup{DBSubnetGroupName: name})
			}

			got, err := b.DescribeDBSubnetGroups(context.Background(), "")
			require.NoError(t, err)

			gotNames := make([]string, len(got))
			for i, sg := range got {
				gotNames[i] = sg.DBSubnetGroupName
			}

			assert.Equal(t, tt.want, gotNames)
		})
	}
}

func TestHandler_ModifyDBSubnetGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *docdb.Handler)
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "modify_subnet_group",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":                       {"CreateDBSubnetGroup"},
					"Version":                      {"2014-10-31"},
					"DBSubnetGroupName":            {"my-sg"},
					"DBSubnetGroupDescription":     {"original"},
					"SubnetIds.SubnetIdentifier.1": {"subnet-aaa"},
				})
			},
			vals: url.Values{
				"Action":                   {"ModifyDBSubnetGroup"},
				"Version":                  {"2014-10-31"},
				"DBSubnetGroupName":        {"my-sg"},
				"DBSubnetGroupDescription": {"updated"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "updated",
		},
		{
			name: "modify_subnet_group_not_found",
			vals: url.Values{
				"Action":            {"ModifyDBSubnetGroup"},
				"Version":           {"2014-10-31"},
				"DBSubnetGroupName": {"nonexistent"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBSubnetGroupNotFoundFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestDeleteSubnetGroupInUse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "subnet_group_in_use_rejected",
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidDBSubnetGroupStateFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":                       {"CreateDBSubnetGroup"},
				"Version":                      {"2014-10-31"},
				"DBSubnetGroupName":            {"my-sg"},
				"DBSubnetGroupDescription":     {"test sg"},
				"SubnetIds.SubnetIdentifier.1": {"subnet-aaa"},
			})
			doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"sg-cluster"},
				"DBSubnetGroupName":   {"my-sg"},
			})
			rr := doRequest(t, h, url.Values{
				"Action":            {"DeleteDBSubnetGroup"},
				"Version":           {"2014-10-31"},
				"DBSubnetGroupName": {"my-sg"},
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestSubnetGroup_ModifyDescription(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		newDescription string
		wantContains   string
		wantStatus     int
	}{
		{
			name:           "update_description",
			newDescription: "updated description",
			wantContains:   "ModifyDBSubnetGroupResponse",
			wantStatus:     200,
		},
		{
			name:           "no_description_change",
			newDescription: "",
			wantContains:   "ModifyDBSubnetGroupResponse",
			wantStatus:     200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":                       {"CreateDBSubnetGroup"},
				"Version":                      {"2014-10-31"},
				"DBSubnetGroupName":            {"mod-sg"},
				"DBSubnetGroupDescription":     {"original"},
				"SubnetIds.SubnetIdentifier.1": {"subnet-111"},
			})
			vals := url.Values{
				"Action":            {"ModifyDBSubnetGroup"},
				"Version":           {"2014-10-31"},
				"DBSubnetGroupName": {"mod-sg"},
			}
			if tt.newDescription != "" {
				vals.Set("DBSubnetGroupDescription", tt.newDescription)
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestSubnetGroup_DeleteInUseFails(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		wantStatus   int
		useInCluster bool
	}{
		{
			name:         "in_use_by_cluster_fails",
			useInCluster: true,
			wantStatus:   400,
			wantContains: "InvalidDBSubnetGroupStateFault",
		},
		{
			name:         "not_in_use_succeeds",
			useInCluster: false,
			wantStatus:   200,
			wantContains: "DeleteDBSubnetGroupResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":                       {"CreateDBSubnetGroup"},
				"Version":                      {"2014-10-31"},
				"DBSubnetGroupName":            {"inuse-sg"},
				"DBSubnetGroupDescription":     {"test"},
				"SubnetIds.SubnetIdentifier.1": {"subnet-aaa"},
			})
			if tt.useInCluster {
				h.Backend.AddDBClusterInternal(&docdb.DBCluster{
					DBClusterIdentifier: "using-cluster",
					DBSubnetGroupName:   "inuse-sg",
				})
			}
			rr := doRequest(t, h, url.Values{
				"Action":            {"DeleteDBSubnetGroup"},
				"Version":           {"2014-10-31"},
				"DBSubnetGroupName": {"inuse-sg"},
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestDescribeDBSubnetGroups_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		maxRecords  string
		wantCount   int
		wantHasMore bool
	}{
		{
			name:        "no_limit_returns_all",
			wantCount:   3,
			wantHasMore: false,
		},
		{
			name:        "limit_to_2",
			maxRecords:  "2",
			wantCount:   2,
			wantHasMore: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			for i := range 3 {
				b2CreateSubnetGroup(t, h, fmt.Sprintf("sg-%d", i))
			}

			vals := url.Values{
				"Action":  {"DescribeDBSubnetGroups"},
				"Version": {"2014-10-31"},
			}
			if tt.maxRecords != "" {
				vals.Set("MaxRecords", tt.maxRecords)
			}
			rr := doRequest(t, h, vals)
			require.Equal(t, http.StatusOK, rr.Code)
			body := rr.Body.String()
			count := strings.Count(body, "<DBSubnetGroupName>")
			assert.Equal(t, tt.wantCount, count)
			if tt.wantHasMore {
				assert.Contains(t, body, "<Marker>")
			} else {
				assert.NotContains(t, body, "<Marker>")
			}
		})
	}
}

// TestParity_SubnetGroupStatus verifies subnet group status is lowercase "complete".
func TestSubnetGroupStatus(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":                       {"CreateDBSubnetGroup"},
		"Version":                      {"2014-10-31"},
		"DBSubnetGroupName":            {"parity-sg"},
		"DBSubnetGroupDescription":     {"parity test"},
		"SubnetIds.SubnetIdentifier.1": {"subnet-aabbccdd"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "<SubnetGroupStatus>complete</SubnetGroupStatus>",
		"status must be lowercase 'complete', not 'Complete'")
}

// TestParity_SubnetAvailabilityZone verifies SubnetStatus and SubnetAvailabilityZone in response.
func TestSubnetAvailabilityZone(t *testing.T) {
	t.Parallel()
	h := newTestHandler(t)

	rr := doRequest(t, h, url.Values{
		"Action":                       {"CreateDBSubnetGroup"},
		"Version":                      {"2014-10-31"},
		"DBSubnetGroupName":            {"az-sg"},
		"DBSubnetGroupDescription":     {"parity test"},
		"SubnetIds.SubnetIdentifier.1": {"subnet-aabb1122"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "<SubnetStatus>Active</SubnetStatus>")
}
