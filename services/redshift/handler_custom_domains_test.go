package redshift_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

// ---- CreateCustomDomainAssociation ----

func TestHandler_CreateCustomDomainAssociation(t *testing.T) {
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
			},
			body: "Action=CreateCustomDomainAssociation&" +
				"Version=2012-12-01&ClusterIdentifier=cluster1" +
				"&CustomDomainName=custom.example.com" +
				"&CustomDomainCertificateArn=arn:aws:acm:us-east-1:123:certificate/abc",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateCustomDomainAssociationResponse", "cluster1", "custom.example.com"},
		},
		{
			name: "duplicate",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h,
					"Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=cluster1&NodeType=dc2.large")
				postRedshiftForm(
					t,
					h,
					"Action=CreateCustomDomainAssociation&"+
						"Version=2012-12-01&ClusterIdentifier=cluster1&CustomDomainName=dup.example.com",
				)
			},
			body: "Action=CreateCustomDomainAssociation&" +
				"Version=2012-12-01&ClusterIdentifier=cluster1&CustomDomainName=dup.example.com",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"CustomCnameAssociationFault"},
		},
		{
			name: "cluster_not_found",
			body: "Action=CreateCustomDomainAssociation&" +
				"Version=2012-12-01&ClusterIdentifier=nonexistent&CustomDomainName=custom.example.com",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ClusterNotFound"},
		},
		{
			name:         "missing_cluster_id",
			body:         "Action=CreateCustomDomainAssociation&Version=2012-12-01&CustomDomainName=custom.example.com",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "missing_domain_name",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h,
					"Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=cluster1&NodeType=dc2.large")
			},
			body:         "Action=CreateCustomDomainAssociation&Version=2012-12-01&ClusterIdentifier=cluster1&CustomDomainName=",
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

// ---- DeleteCustomDomainAssociation ----

func TestHandler_DeleteCustomDomainAssociation(t *testing.T) {
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
				postRedshiftForm(
					t,
					h,
					"Action=CreateCustomDomainAssociation&"+
						"Version=2012-12-01&ClusterIdentifier=cluster1&CustomDomainName=del.example.com",
				)
			},
			body: "Action=DeleteCustomDomainAssociation&" +
				"Version=2012-12-01&ClusterIdentifier=cluster1&CustomDomainName=del.example.com",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteCustomDomainAssociationResponse"},
		},
		{
			name: "not_found",
			body: "Action=DeleteCustomDomainAssociation&" +
				"Version=2012-12-01&ClusterIdentifier=cluster1&CustomDomainName=missing.example.com",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"CustomDomainAssociationNotFoundFault"},
		},
		{
			name:         "missing_cluster_id",
			body:         "Action=DeleteCustomDomainAssociation&Version=2012-12-01&CustomDomainName=custom.example.com",
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

// ---- DescribeCustomDomainAssociations ----

func TestHandler_DescribeCustomDomainAssociations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *redshift.Handler)
		name         string
		body         string
		wantContains []string
		wantCode     int
		wantCount    int
	}{
		{
			name: "list_all",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h,
					"Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=cluster1&NodeType=dc2.large")
				postRedshiftForm(
					t,
					h,
					"Action=CreateCustomDomainAssociation&"+
						"Version=2012-12-01&ClusterIdentifier=cluster1&CustomDomainName=a.example.com",
				)
				postRedshiftForm(
					t,
					h,
					"Action=CreateCustomDomainAssociation&"+
						"Version=2012-12-01&ClusterIdentifier=cluster1&CustomDomainName=b.example.com",
				)
			},
			body:         "Action=DescribeCustomDomainAssociations&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeCustomDomainAssociationsResponse", "a.example.com", "b.example.com"},
		},
		{
			name: "filter_by_cluster",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h,
					"Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=cluster1&NodeType=dc2.large")
				postRedshiftForm(
					t,
					h,
					"Action=CreateCustomDomainAssociation&"+
						"Version=2012-12-01&ClusterIdentifier=cluster1&CustomDomainName=filter.example.com",
				)
			},
			body:         "Action=DescribeCustomDomainAssociations&Version=2012-12-01&ClusterIdentifier=cluster1",
			wantCode:     http.StatusOK,
			wantContains: []string{"filter.example.com"},
		},
		{
			name:         "empty",
			body:         "Action=DescribeCustomDomainAssociations&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeCustomDomainAssociationsResponse"},
		},
		{
			name: "not_found_specific",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h,
					"Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=cluster1&NodeType=dc2.large")
			},
			body: "Action=DescribeCustomDomainAssociations&" +
				"Version=2012-12-01&ClusterIdentifier=cluster1&CustomDomainName=missing.example.com",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"CustomDomainAssociationNotFoundFault"},
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

// ---- ModifyCustomDomainAssociation ----

func TestHandler_ModifyCustomDomainAssociation(t *testing.T) {
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
				postRedshiftForm(
					t,
					h,
					"Action=CreateCustomDomainAssociation&"+
						"Version=2012-12-01&ClusterIdentifier=cluster1&CustomDomainName=mod.example.com"+
						"&CustomDomainCertificateArn=arn:old",
				)
			},
			body: "Action=ModifyCustomDomainAssociation&" +
				"Version=2012-12-01&ClusterIdentifier=cluster1&CustomDomainName=mod.example.com&CustomDomainCertificateArn=arn:new",
			wantCode:     http.StatusOK,
			wantContains: []string{"ModifyCustomDomainAssociationResponse", "arn:new"},
		},
		{
			name: "not_found",
			body: "Action=ModifyCustomDomainAssociation&" +
				"Version=2012-12-01&ClusterIdentifier=cluster1&CustomDomainName=missing.example.com",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"CustomDomainAssociationNotFoundFault"},
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

// ---- Backend: CustomDomainAssociation count tracking ----

func TestBackend_CustomDomainAssociation_Count(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("123456789012", "us-east-1")
	h := redshift.NewHandler(b)

	assert.Equal(t, 0, redshift.CustomDomainCount(b))

	postRedshiftForm(t, h,
		"Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=c1&NodeType=dc2.large")
	postRedshiftForm(t, h,
		"Action=CreateCustomDomainAssociation&Version=2012-12-01&ClusterIdentifier=c1&CustomDomainName=x.example.com")

	assert.Equal(t, 1, redshift.CustomDomainCount(b))

	postRedshiftForm(t, h,
		"Action=DeleteCustomDomainAssociation&Version=2012-12-01&ClusterIdentifier=c1&CustomDomainName=x.example.com")

	assert.Equal(t, 0, redshift.CustomDomainCount(b))
}

// ---- CRUD Lifecycle ----

func TestHandler_CustomDomainAssociation_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()

	postRedshiftForm(t, h,
		"Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=lifecycle-cluster&NodeType=dc2.large")

	// Create
	rec := postRedshiftForm(
		t,
		h,
		"Action=CreateCustomDomainAssociation&"+
			"Version=2012-12-01&ClusterIdentifier=lifecycle-cluster"+
			"&CustomDomainName=lifecycle.example.com"+
			"&CustomDomainCertificateArn=arn:aws:acm:us-east-1:123:certificate/v1",
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "lifecycle.example.com")

	// Describe — should appear
	rec = postRedshiftForm(
		t,
		h,
		"Action=DescribeCustomDomainAssociations&"+
			"Version=2012-12-01&ClusterIdentifier=lifecycle-cluster&CustomDomainName=lifecycle.example.com",
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "lifecycle.example.com")

	// Modify
	rec = postRedshiftForm(
		t,
		h,
		"Action=ModifyCustomDomainAssociation&"+
			"Version=2012-12-01&ClusterIdentifier=lifecycle-cluster"+
			"&CustomDomainName=lifecycle.example.com"+
			"&CustomDomainCertificateArn=arn:aws:acm:us-east-1:123:certificate/v2",
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "v2")

	// Delete
	rec = postRedshiftForm(
		t,
		h,
		"Action=DeleteCustomDomainAssociation&"+
			"Version=2012-12-01&ClusterIdentifier=lifecycle-cluster&CustomDomainName=lifecycle.example.com",
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Describe after delete — should be not found
	rec = postRedshiftForm(
		t,
		h,
		"Action=DescribeCustomDomainAssociations&"+
			"Version=2012-12-01&ClusterIdentifier=lifecycle-cluster&CustomDomainName=lifecycle.example.com",
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "CustomDomainAssociationNotFoundFault")
}
