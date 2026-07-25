package redshift_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

// ---- CreateEndpointAccess ----

func TestHandler_CreateEndpointAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *redshift.Handler)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			// Real CreateEndpointAccessInput has no VpcId field: the VPC is derived
			// from SubnetGroupName (there is no such thing as VpcId on this API,
			// confirmed against aws-sdk-go-v2/service/redshift@v1.62.3). This test
			// seeds a ClusterSubnetGroup and verifies its VpcId is inherited.
			name: "success_derives_vpc_from_subnet_group",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h,
					"Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=cluster1&NodeType=dc2.large")
				postRedshiftForm(t, h,
					"Action=CreateClusterSubnetGroup&Version=2012-12-01"+
						"&ClusterSubnetGroupName=my-subnet-group&VpcId=vpc-from-subnet-group"+
						"&SubnetIds.SubnetIdentifier.1=subnet-1")
			},
			body: "Action=CreateEndpointAccess&" +
				"Version=2012-12-01&ClusterIdentifier=cluster1&EndpointName=myendpoint" +
				"&SubnetGroupName=my-subnet-group" +
				"&VpcSecurityGroupIds.VpcSecurityGroupId.1=sg-123",
			wantCode: http.StatusOK,
			wantContains: []string{
				"CreateEndpointAccessResponse", "myendpoint", "active",
				"<SubnetGroupName>my-subnet-group</SubnetGroupName>",
				"<VpcSecurityGroupId>sg-123</VpcSecurityGroupId>",
			},
		},
		{
			// A subnet group name that doesn't exist should not error -- it just
			// leaves VpcId unpopulated, matching how a real client-facing lookup
			// failure for a cross-referenced but not directly validated field would
			// behave in a best-effort emulator.
			name: "success_no_subnet_group",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h,
					"Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=cluster1&NodeType=dc2.large")
			},
			body: "Action=CreateEndpointAccess&" +
				"Version=2012-12-01&ClusterIdentifier=cluster1&EndpointName=myendpoint2",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateEndpointAccessResponse", "myendpoint2", "active"},
		},
		{
			name: "duplicate",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h,
					"Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=cluster1&NodeType=dc2.large")
				postRedshiftForm(t, h,
					"Action=CreateEndpointAccess&Version=2012-12-01&ClusterIdentifier=cluster1&EndpointName=dupep")
			},
			body:         "Action=CreateEndpointAccess&Version=2012-12-01&ClusterIdentifier=cluster1&EndpointName=dupep",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"EndpointAlreadyExists"},
		},
		{
			name:         "cluster_not_found",
			body:         "Action=CreateEndpointAccess&Version=2012-12-01&ClusterIdentifier=nonexistent&EndpointName=myendpoint",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ClusterNotFound"},
		},
		{
			name:         "missing_endpoint_name",
			body:         "Action=CreateEndpointAccess&Version=2012-12-01&ClusterIdentifier=cluster1&EndpointName=",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "missing_cluster_id",
			body:         "Action=CreateEndpointAccess&Version=2012-12-01&EndpointName=myendpoint",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- DeleteEndpointAccess ----

func TestHandler_DeleteEndpointAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *redshift.Handler)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h,
					"Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=cluster1&NodeType=dc2.large")
				postRedshiftForm(t, h,
					"Action=CreateEndpointAccess&Version=2012-12-01&ClusterIdentifier=cluster1&EndpointName=ep-del")
			},
			body:         "Action=DeleteEndpointAccess&Version=2012-12-01&EndpointName=ep-del",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteEndpointAccessResponse", "deleting"},
		},
		{
			name:         "not_found",
			body:         "Action=DeleteEndpointAccess&Version=2012-12-01&EndpointName=missing",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"EndpointNotFound"},
		},
		{
			name:         "missing_name",
			body:         "Action=DeleteEndpointAccess&Version=2012-12-01&EndpointName=",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- DescribeEndpointAccess ----

func TestHandler_DescribeEndpointAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *redshift.Handler)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "list_all",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h,
					"Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=cluster1&NodeType=dc2.large")
				postRedshiftForm(t, h,
					"Action=CreateEndpointAccess&Version=2012-12-01&ClusterIdentifier=cluster1&EndpointName=ep-a")
				postRedshiftForm(t, h,
					"Action=CreateEndpointAccess&Version=2012-12-01&ClusterIdentifier=cluster1&EndpointName=ep-b")
			},
			body:         "Action=DescribeEndpointAccess&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeEndpointAccessResponse", "ep-a", "ep-b"},
		},
		{
			name: "filter_by_endpoint_name",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h,
					"Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=cluster1&NodeType=dc2.large")
				postRedshiftForm(
					t,
					h,
					"Action=CreateEndpointAccess&Version=2012-12-01&ClusterIdentifier=cluster1&EndpointName=specific-ep",
				)
			},
			body:         "Action=DescribeEndpointAccess&Version=2012-12-01&EndpointName=specific-ep",
			wantCode:     http.StatusOK,
			wantContains: []string{"specific-ep"},
		},
		{
			name:         "empty",
			body:         "Action=DescribeEndpointAccess&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeEndpointAccessResponse"},
		},
		{
			name:         "not_found_by_name",
			body:         "Action=DescribeEndpointAccess&Version=2012-12-01&EndpointName=missing",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"EndpointNotFound"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- ModifyEndpointAccess ----

func TestHandler_ModifyEndpointAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *redshift.Handler)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			// Real ModifyEndpointAccessInput only supports changing
			// VpcSecurityGroupIds -- there is no VpcId parameter.
			name: "success",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h,
					"Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=cluster1&NodeType=dc2.large")
				postRedshiftForm(t, h,
					"Action=CreateEndpointAccess&Version=2012-12-01&ClusterIdentifier=cluster1&EndpointName=ep-mod")
			},
			body: "Action=ModifyEndpointAccess&Version=2012-12-01&EndpointName=ep-mod" +
				"&VpcSecurityGroupIds.VpcSecurityGroupId.1=sg-new",
			wantCode: http.StatusOK,
			wantContains: []string{
				"ModifyEndpointAccessResponse", "ep-mod", "<VpcSecurityGroupId>sg-new</VpcSecurityGroupId>",
			},
		},
		{
			name:         "not_found",
			body:         "Action=ModifyEndpointAccess&Version=2012-12-01&EndpointName=missing",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"EndpointNotFound"},
		},
		{
			name:         "missing_name",
			body:         "Action=ModifyEndpointAccess&Version=2012-12-01&EndpointName=",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- Backend: EndpointAccess count tracking ----

func TestBackend_EndpointAccess_Count(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("123456789012", "us-east-1")
	h := redshift.NewHandler(b)

	assert.Equal(t, 0, redshift.EndpointAccessCount(b))

	postRedshiftForm(t, h,
		"Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=c1&NodeType=dc2.large")
	postRedshiftForm(t, h,
		"Action=CreateEndpointAccess&Version=2012-12-01&ClusterIdentifier=c1&EndpointName=ep1")
	postRedshiftForm(t, h,
		"Action=CreateEndpointAccess&Version=2012-12-01&ClusterIdentifier=c1&EndpointName=ep2")

	assert.Equal(t, 2, redshift.EndpointAccessCount(b))

	postRedshiftForm(t, h,
		"Action=DeleteEndpointAccess&Version=2012-12-01&EndpointName=ep1")

	assert.Equal(t, 1, redshift.EndpointAccessCount(b))
}

// ---- CRUD Lifecycle ----

func TestHandler_EndpointAccess_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()

	postRedshiftForm(t, h,
		"Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=ep-cluster&NodeType=dc2.large")

	// Create
	rec := postRedshiftForm(
		t,
		h,
		"Action=CreateEndpointAccess&Version=2012-12-01&ClusterIdentifier=ep-cluster&EndpointName=ep-lifecycle",
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ep-lifecycle")

	// Describe — should appear
	rec = postRedshiftForm(t, h,
		"Action=DescribeEndpointAccess&Version=2012-12-01&EndpointName=ep-lifecycle")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ep-lifecycle")

	// Modify
	rec = postRedshiftForm(t, h,
		"Action=ModifyEndpointAccess&Version=2012-12-01&EndpointName=ep-lifecycle"+
			"&VpcSecurityGroupIds.VpcSecurityGroupId.1=sg-new")
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete
	rec = postRedshiftForm(t, h,
		"Action=DeleteEndpointAccess&Version=2012-12-01&EndpointName=ep-lifecycle")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "deleting")

	// Describe after delete — should be not found
	rec = postRedshiftForm(t, h,
		"Action=DescribeEndpointAccess&Version=2012-12-01&EndpointName=ep-lifecycle")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "EndpointNotFound")
}
