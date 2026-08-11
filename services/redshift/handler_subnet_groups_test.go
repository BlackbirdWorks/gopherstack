package redshift_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

// ---- CreateClusterSubnetGroup ----

func TestRedshiftHandler_CreateClusterSubnetGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			body: "Action=CreateClusterSubnetGroup&Version=2012-12-01" +
				"&ClusterSubnetGroupName=my-sng&Description=my+desc" +
				"&SubnetIds.SubnetIdentifier.1=subnet-001&SubnetIds.SubnetIdentifier.2=subnet-002",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateClusterSubnetGroupResponse", "my-sng", "subnet-001"},
		},
		{
			name:         "missing_name",
			body:         "Action=CreateClusterSubnetGroup&Version=2012-12-01&Description=desc",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "duplicate",
			body: "Action=CreateClusterSubnetGroup&Version=2012-12-01" +
				"&ClusterSubnetGroupName=dup-sng",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ClusterSubnetGroupAlreadyExists"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			h := redshift.NewHandler(b)

			if tt.name == "duplicate" {
				postRedshiftForm(t, h, "Action=CreateClusterSubnetGroup&Version=2012-12-01"+
					"&ClusterSubnetGroupName=dup-sng")
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// TestRedshiftHandler_CreateClusterSubnetGroup_VpcIdNotFabricated locks in that
// VpcId is no longer accepted as a CreateClusterSubnetGroup request param (real
// CreateClusterSubnetGroupInput has no such field -- see
// awsAwsquery_serializeOpDocumentCreateClusterSubnetGroupInput in
// aws-sdk-go-v2/service/redshift@v1.65.4/serializers.go). A caller sending VpcId
// on the wire (as a real SDK client never would) must not see it echoed back.
func TestRedshiftHandler_CreateClusterSubnetGroup_VpcIdNotFabricated(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	h := redshift.NewHandler(b)

	rec := postRedshiftForm(t, h, "Action=CreateClusterSubnetGroup&Version=2012-12-01"+
		"&ClusterSubnetGroupName=honest-sng&VpcId=vpc-should-not-appear"+
		"&SubnetIds.SubnetIdentifier.1=subnet-001")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.NotContains(t, rec.Body.String(), "vpc-should-not-appear")
	assert.NotContains(t, rec.Body.String(), "<VpcId>")
}

// ---- DeleteClusterSubnetGroup ----

func TestRedshiftHandler_DeleteClusterSubnetGroup(t *testing.T) {
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
				postRedshiftForm(t, h, "Action=CreateClusterSubnetGroup&Version=2012-12-01"+
					"&ClusterSubnetGroupName=del-sng")
			},
			body:         "Action=DeleteClusterSubnetGroup&Version=2012-12-01&ClusterSubnetGroupName=del-sng",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteClusterSubnetGroupResponse"},
		},
		{
			name:         "not_found",
			body:         "Action=DeleteClusterSubnetGroup&Version=2012-12-01&ClusterSubnetGroupName=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ClusterSubnetGroupNotFound"},
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

// ---- DescribeClusterSubnetGroups ----

func TestRedshiftHandler_DescribeClusterSubnetGroups(t *testing.T) {
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
			body:         "Action=DescribeClusterSubnetGroups&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeClusterSubnetGroupsResponse"},
		},
		{
			name: "list_with_group",
			setup: func(h *redshift.Handler) {
				postRedshiftForm(t, h, "Action=CreateClusterSubnetGroup&Version=2012-12-01"+
					"&ClusterSubnetGroupName=list-sng")
			},
			body:         "Action=DescribeClusterSubnetGroups&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeClusterSubnetGroupsResponse", "list-sng"},
		},
		{
			name:         "not_found",
			body:         "Action=DescribeClusterSubnetGroups&Version=2012-12-01&ClusterSubnetGroupName=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ClusterSubnetGroupNotFound"},
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

// ---- ModifyClusterSubnetGroup ----

func TestRedshiftHandler_ModifyClusterSubnetGroup(t *testing.T) {
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
				postRedshiftForm(t, h, "Action=CreateClusterSubnetGroup&Version=2012-12-01"+
					"&ClusterSubnetGroupName=mod-sng&VpcId=vpc-old")
			},
			body: "Action=ModifyClusterSubnetGroup&Version=2012-12-01" +
				"&ClusterSubnetGroupName=mod-sng&Description=new-desc" +
				"&SubnetIds.SubnetIdentifier.1=subnet-new",
			wantCode:     http.StatusOK,
			wantContains: []string{"ModifyClusterSubnetGroupResponse", "mod-sng", "subnet-new"},
		},
		{
			name:         "not_found",
			body:         "Action=ModifyClusterSubnetGroup&Version=2012-12-01&ClusterSubnetGroupName=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ClusterSubnetGroupNotFound"},
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

// ---- Backend: SubnetGroupCount ----

func TestRedshiftBackend_SubnetGroupCount(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	require.Equal(t, 0, redshift.SubnetGroupCount(b))

	b.AddSubnetGroupInternal(&redshift.ClusterSubnetGroup{
		ClusterSubnetGroupName: "test-sng",
	})

	require.Equal(t, 1, redshift.SubnetGroupCount(b))
}
