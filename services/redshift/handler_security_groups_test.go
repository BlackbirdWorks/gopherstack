package redshift_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

// ---- CreateClusterSecurityGroup ----

func TestRedshiftHandler_CreateClusterSecurityGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			body: "Action=CreateClusterSecurityGroup&Version=2012-12-01" +
				"&ClusterSecurityGroupName=my-sg&Description=my+sg+description",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateClusterSecurityGroupResponse", "my-sg"},
		},
		{
			name:         "missing_name",
			body:         "Action=CreateClusterSecurityGroup&Version=2012-12-01&Description=desc",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "duplicate",
			body: "Action=CreateClusterSecurityGroup&Version=2012-12-01" +
				"&ClusterSecurityGroupName=dup-sg",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ClusterSecurityGroupAlreadyExists"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			h := redshift.NewHandler(b)

			if tt.name == "duplicate" {
				postRedshiftForm(t, h, "Action=CreateClusterSecurityGroup&Version=2012-12-01"+
					"&ClusterSecurityGroupName=dup-sg")
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- DeleteClusterSecurityGroup ----

func TestRedshiftHandler_DeleteClusterSecurityGroup(t *testing.T) {
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
				postRedshiftForm(t, h, "Action=CreateClusterSecurityGroup&Version=2012-12-01"+
					"&ClusterSecurityGroupName=del-sg")
			},
			body:         "Action=DeleteClusterSecurityGroup&Version=2012-12-01&ClusterSecurityGroupName=del-sg",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteClusterSecurityGroupResponse"},
		},
		{
			name:         "not_found",
			body:         "Action=DeleteClusterSecurityGroup&Version=2012-12-01&ClusterSecurityGroupName=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ClusterSecurityGroupNotFound"},
		},
		{
			name:         "missing_name",
			body:         "Action=DeleteClusterSecurityGroup&Version=2012-12-01",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
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

// ---- DescribeClusterSecurityGroups ----

func TestRedshiftHandler_DescribeClusterSecurityGroups(t *testing.T) {
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
			body:         "Action=DescribeClusterSecurityGroups&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeClusterSecurityGroupsResponse"},
		},
		{
			name: "list_with_sg",
			setup: func(h *redshift.Handler) {
				postRedshiftForm(t, h, "Action=CreateClusterSecurityGroup&Version=2012-12-01"+
					"&ClusterSecurityGroupName=my-sg2")
			},
			body:         "Action=DescribeClusterSecurityGroups&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeClusterSecurityGroupsResponse", "my-sg2"},
		},
		{
			name: "describe_specific",
			setup: func(h *redshift.Handler) {
				postRedshiftForm(t, h, "Action=CreateClusterSecurityGroup&Version=2012-12-01"+
					"&ClusterSecurityGroupName=specific-sg")
			},
			body:         "Action=DescribeClusterSecurityGroups&Version=2012-12-01&ClusterSecurityGroupName=specific-sg",
			wantCode:     http.StatusOK,
			wantContains: []string{"specific-sg"},
		},
		{
			name:         "not_found",
			body:         "Action=DescribeClusterSecurityGroups&Version=2012-12-01&ClusterSecurityGroupName=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ClusterSecurityGroupNotFound"},
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

// ---- RevokeClusterSecurityGroupIngress ----

func TestRedshiftHandler_RevokeClusterSecurityGroupIngress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *redshift.Handler)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "revoke_cidr",
			setup: func(h *redshift.Handler) {
				postRedshiftForm(t, h, "Action=CreateClusterSecurityGroup&Version=2012-12-01"+
					"&ClusterSecurityGroupName=revoke-sg")
				postRedshiftForm(t, h, "Action=AuthorizeClusterSecurityGroupIngress&Version=2012-12-01"+
					"&ClusterSecurityGroupName=revoke-sg&CIDRIP=10.0.0.0/8")
			},
			body: "Action=RevokeClusterSecurityGroupIngress&Version=2012-12-01" +
				"&ClusterSecurityGroupName=revoke-sg&CIDRIP=10.0.0.0/8",
			wantCode:     http.StatusOK,
			wantContains: []string{"RevokeClusterSecurityGroupIngressResponse", "revoke-sg"},
		},
		{
			name: "not_found",
			body: "Action=RevokeClusterSecurityGroupIngress&Version=2012-12-01" +
				"&ClusterSecurityGroupName=nonexistent&CIDRIP=10.0.0.0/8",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ClusterSecurityGroupNotFound"},
		},
		{
			name:         "missing_cidr_and_ec2_group",
			body:         "Action=RevokeClusterSecurityGroupIngress&Version=2012-12-01&ClusterSecurityGroupName=some-sg",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
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

// ---- Backend: SecurityGroupCount ----

func TestRedshiftBackend_SecurityGroupCount(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	require.Equal(t, 0, redshift.SecurityGroupCount(b))

	h := redshift.NewHandler(b)
	postRedshiftForm(t, h, "Action=CreateClusterSecurityGroup&Version=2012-12-01"+
		"&ClusterSecurityGroupName=count-sg")
	require.Equal(t, 1, redshift.SecurityGroupCount(b))

	postRedshiftForm(t, h, "Action=DeleteClusterSecurityGroup&Version=2012-12-01"+
		"&ClusterSecurityGroupName=count-sg")
	require.Equal(t, 0, redshift.SecurityGroupCount(b))
}
