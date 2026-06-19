package redshift_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
			wantContains: []string{"CustomDomainAssociationAlreadyExistsFault"},
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
			name: "success",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h,
					"Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=cluster1&NodeType=dc2.large")
			},
			body: "Action=CreateEndpointAccess&" +
				"Version=2012-12-01&ClusterIdentifier=cluster1&EndpointName=myendpoint&VpcId=vpc-123",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateEndpointAccessResponse", "myendpoint", "active"},
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
			name: "success",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h,
					"Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=cluster1&NodeType=dc2.large")
				postRedshiftForm(t, h,
					"Action=CreateEndpointAccess&Version=2012-12-01&ClusterIdentifier=cluster1&EndpointName=ep-mod")
			},
			body:         "Action=ModifyEndpointAccess&Version=2012-12-01&EndpointName=ep-mod&VpcId=vpc-new",
			wantCode:     http.StatusOK,
			wantContains: []string{"ModifyEndpointAccessResponse", "ep-mod"},
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

// ---- CreateIntegration ----

func TestHandler_CreateIntegration(t *testing.T) {
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
			body: "Action=CreateIntegration&" +
				"Version=2012-12-01&IntegrationName=my-integration" +
				"&SourceArn=arn:aws:redshift:us-east-1:123:cluster/src" +
				"&TargetArn=arn:aws:redshift:us-east-1:123:namespace/tgt",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateIntegrationResponse", "my-integration", "active"},
		},
		{
			name: "success_with_kms",
			body: "Action=CreateIntegration&" +
				"Version=2012-12-01&IntegrationName=kms-integration" +
				"&SourceArn=arn:aws:redshift:us-east-1:123:cluster/src" +
				"&TargetArn=arn:aws:redshift:us-east-1:123:namespace/tgt&KmsKeyId=key123",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateIntegrationResponse", "kms-integration", "key123"},
		},
		{
			name: "duplicate",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(
					t,
					h,
					"Action=CreateIntegration&Version=2012-12-01&IntegrationName=dup-integration&SourceArn=arn:src&TargetArn=arn:tgt",
				)
			},
			body: "Action=CreateIntegration&" +
				"Version=2012-12-01&IntegrationName=dup-integration&SourceArn=arn:src&TargetArn=arn:tgt",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"IntegrationAlreadyExists"},
		},
		{
			name:         "missing_name",
			body:         "Action=CreateIntegration&Version=2012-12-01&IntegrationName=&SourceArn=arn:src&TargetArn=arn:tgt",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "missing_source_arn",
			body:         "Action=CreateIntegration&Version=2012-12-01&IntegrationName=test&TargetArn=arn:tgt",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "missing_target_arn",
			body:         "Action=CreateIntegration&Version=2012-12-01&IntegrationName=test&SourceArn=arn:src",
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

// ---- DeleteIntegration ----

func TestHandler_DeleteIntegration(t *testing.T) {
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
				rec := postRedshiftForm(
					t,
					h,
					"Action=CreateIntegration&Version=2012-12-01&IntegrationName=del-integration&SourceArn=arn:src&TargetArn=arn:tgt",
				)
				require.Equal(t, http.StatusOK, rec.Code)
			},
			body: "Action=DeleteIntegration&" +
				"Version=2012-12-01&IntegrationArn=arn:aws:redshift:us-east-1:000000000000:integration/del-integration",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteIntegrationResponse"},
		},
		{
			name: "not_found",
			body: "Action=DeleteIntegration&" +
				"Version=2012-12-01&IntegrationArn=arn:aws:redshift:us-east-1:000000000000:integration/missing",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"IntegrationNotFound"},
		},
		{
			name:         "missing_arn",
			body:         "Action=DeleteIntegration&Version=2012-12-01&IntegrationArn=",
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

// ---- DescribeIntegrations ----

func TestHandler_DescribeIntegrations(t *testing.T) {
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
				postRedshiftForm(
					t,
					h,
					"Action=CreateIntegration&Version=2012-12-01&IntegrationName=ig-a&SourceArn=arn:src&TargetArn=arn:tgt",
				)
				postRedshiftForm(
					t,
					h,
					"Action=CreateIntegration&Version=2012-12-01&IntegrationName=ig-b&SourceArn=arn:src&TargetArn=arn:tgt",
				)
			},
			body:         "Action=DescribeIntegrations&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeIntegrationsResponse", "ig-a", "ig-b"},
		},
		{
			name:         "empty",
			body:         "Action=DescribeIntegrations&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeIntegrationsResponse"},
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

// ---- ModifyIntegration ----

func TestHandler_ModifyIntegration(t *testing.T) {
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
				postRedshiftForm(
					t,
					h,
					"Action=CreateIntegration&"+
						"Version=2012-12-01&IntegrationName=mod-integration&SourceArn=arn:src&TargetArn=arn:tgt&Description=old",
				)
			},
			body: "Action=ModifyIntegration&" +
				"Version=2012-12-01" +
				"&IntegrationArn=arn:aws:redshift:us-east-1:000000000000:integration/mod-integration" +
				"&Description=new-desc",
			wantCode:     http.StatusOK,
			wantContains: []string{"ModifyIntegrationResponse", "new-desc"},
		},
		{
			name:         "not_found",
			body:         "Action=ModifyIntegration&Version=2012-12-01&IntegrationArn=arn:missing",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"IntegrationNotFound"},
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

// ---- Backend: Integration count tracking ----

func TestBackend_Integration_Count(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	h := redshift.NewHandler(b)

	assert.Equal(t, 0, redshift.IntegrationCount(b))

	postRedshiftForm(t, h,
		"Action=CreateIntegration&Version=2012-12-01&IntegrationName=ig1&SourceArn=arn:src&TargetArn=arn:tgt")

	assert.Equal(t, 1, redshift.IntegrationCount(b))
}

// ---- CreateIdcApplication ----

func TestHandler_CreateIdcApplication(t *testing.T) {
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
			body: "Action=CreateIdcApplication&" +
				"Version=2012-12-01&IdcApplicationName=my-app" +
				"&IdcInstanceArn=arn:aws:sso:::instance/abc" +
				"&IamRoleArn=arn:aws:iam::123:role/MyRole",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateIdcApplicationResponse", "my-app"},
		},
		{
			name: "duplicate",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(
					t,
					h,
					"Action=CreateIdcApplication&"+
						"Version=2012-12-01&IdcApplicationName=dup-app&IdcInstanceArn=arn:idc&IamRoleArn=arn:role",
				)
			},
			body: "Action=CreateIdcApplication&" +
				"Version=2012-12-01&IdcApplicationName=dup-app&IdcInstanceArn=arn:idc&IamRoleArn=arn:role",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"IdcApplicationAlreadyExistsFault"},
		},
		{
			name: "missing_name",
			body: "Action=CreateIdcApplication&" +
				"Version=2012-12-01&IdcApplicationName=&IdcInstanceArn=arn:idc",
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

// ---- DeleteIdcApplication ----

func TestHandler_DeleteIdcApplication(t *testing.T) {
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
				postRedshiftForm(
					t,
					h,
					"Action=CreateIdcApplication&"+
						"Version=2012-12-01&IdcApplicationName=del-app&IdcInstanceArn=arn:idc&IamRoleArn=arn:role",
				)
			},
			body: "Action=DeleteIdcApplication&" +
				"Version=2012-12-01" +
				"&IdcApplicationArn=arn:aws:redshift:us-east-1:000000000000:redshiftidcapplication/del-app",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeleteIdcApplicationResponse"},
		},
		{
			name: "not_found",
			body: "Action=DeleteIdcApplication&" +
				"Version=2012-12-01" +
				"&IdcApplicationArn=arn:aws:redshift:us-east-1:000000000000:redshiftidcapplication/missing",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"IdcApplicationNotExistsFault"},
		},
		{
			name:         "missing_arn",
			body:         "Action=DeleteIdcApplication&Version=2012-12-01&IdcApplicationArn=",
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

// ---- DescribeIdcApplications ----

func TestHandler_DescribeIdcApplications(t *testing.T) {
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
				postRedshiftForm(
					t,
					h,
					"Action=CreateIdcApplication&"+
						"Version=2012-12-01&IdcApplicationName=app-a&IdcInstanceArn=arn:idc&IamRoleArn=arn:role",
				)
				postRedshiftForm(
					t,
					h,
					"Action=CreateIdcApplication&"+
						"Version=2012-12-01&IdcApplicationName=app-b&IdcInstanceArn=arn:idc&IamRoleArn=arn:role",
				)
			},
			body:         "Action=DescribeIdcApplications&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeIdcApplicationsResponse", "app-a", "app-b"},
		},
		{
			name:         "empty",
			body:         "Action=DescribeIdcApplications&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeIdcApplicationsResponse"},
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

// ---- ModifyIdcApplication ----

func TestHandler_ModifyIdcApplication(t *testing.T) {
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
				postRedshiftForm(
					t,
					h,
					"Action=CreateIdcApplication&"+
						"Version=2012-12-01&IdcApplicationName=mod-app&IdcInstanceArn=arn:idc"+
						"&IdcDisplayName=OldName&IamRoleArn=arn:old-role",
				)
			},
			body: "Action=ModifyIdcApplication&" +
				"Version=2012-12-01" +
				"&IdcApplicationArn=arn:aws:redshift:us-east-1:000000000000:redshiftidcapplication/mod-app" +
				"&IdcDisplayName=NewName&IamRoleArn=arn:new-role",
			wantCode:     http.StatusOK,
			wantContains: []string{"ModifyIdcApplicationResponse", "mod-app"},
		},
		{
			name:         "not_found",
			body:         "Action=ModifyIdcApplication&Version=2012-12-01&IdcApplicationArn=arn:missing",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"IdcApplicationNotExistsFault"},
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

// ---- Backend: IdcApplication count tracking ----

func TestBackend_IdcApplication_Count(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	h := redshift.NewHandler(b)

	assert.Equal(t, 0, redshift.IdcApplicationCount(b))

	postRedshiftForm(
		t,
		h,
		"Action=CreateIdcApplication&"+
			"Version=2012-12-01&IdcApplicationName=app1&IdcInstanceArn=arn:idc&IamRoleArn=arn:role",
	)

	assert.Equal(t, 1, redshift.IdcApplicationCount(b))

	postRedshiftForm(
		t,
		h,
		"Action=DeleteIdcApplication&"+
			"Version=2012-12-01&IdcApplicationArn=arn:aws:redshift:us-east-1:000000000000:redshiftidcapplication/app1",
	)

	assert.Equal(t, 0, redshift.IdcApplicationCount(b))
}

// ---- CRUD Lifecycle Tests ----

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

func TestHandler_EndpointAccess_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()

	postRedshiftForm(t, h,
		"Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=ep-cluster&NodeType=dc2.large")

	// Create
	rec := postRedshiftForm(
		t,
		h,
		"Action=CreateEndpointAccess&Version=2012-12-01&ClusterIdentifier=ep-cluster&EndpointName=ep-lifecycle&VpcId=vpc-abc",
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
		"Action=ModifyEndpointAccess&Version=2012-12-01&EndpointName=ep-lifecycle&VpcId=vpc-new")
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

func TestHandler_Integration_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()

	// Create
	rec := postRedshiftForm(
		t,
		h,
		"Action=CreateIntegration&"+
			"Version=2012-12-01&IntegrationName=lc-integration"+
			"&SourceArn=arn:aws:redshift:us-east-1:123:cluster/src"+
			"&TargetArn=arn:aws:redshift:us-east-1:123:namespace/tgt&Description=initial",
	)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "lc-integration")
	assert.Contains(t, rec.Body.String(), "active")

	// Describe — should appear
	rec = postRedshiftForm(t, h,
		"Action=DescribeIntegrations&Version=2012-12-01")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "lc-integration")

	// Modify description
	rec = postRedshiftForm(
		t,
		h,
		"Action=ModifyIntegration&"+
			"Version=2012-12-01"+
			"&IntegrationArn=arn:aws:redshift:us-east-1:000000000000:integration/lc-integration"+
			"&Description=updated",
	)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "updated")

	// Delete
	rec = postRedshiftForm(
		t,
		h,
		"Action=DeleteIntegration&"+
			"Version=2012-12-01&IntegrationArn=arn:aws:redshift:us-east-1:000000000000:integration/lc-integration",
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe after delete — not found
	rec = postRedshiftForm(
		t,
		h,
		"Action=DescribeIntegrations&"+
			"Version=2012-12-01&IntegrationArn=arn:aws:redshift:us-east-1:000000000000:integration/lc-integration",
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "IntegrationNotFound")
}

func TestHandler_IdcApplication_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()

	// Create
	rec := postRedshiftForm(
		t,
		h,
		"Action=CreateIdcApplication&"+
			"Version=2012-12-01&IdcApplicationName=lc-app"+
			"&IdcInstanceArn=arn:aws:sso:::instance/abc&IdcDisplayName=LifecycleApp"+
			"&IamRoleArn=arn:aws:iam::123:role/MyRole",
	)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "lc-app")

	// Describe — should appear
	rec = postRedshiftForm(t, h,
		"Action=DescribeIdcApplications&Version=2012-12-01")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "lc-app")

	// Modify
	rec = postRedshiftForm(
		t,
		h,
		"Action=ModifyIdcApplication&"+
			"Version=2012-12-01"+
			"&IdcApplicationArn=arn:aws:redshift:us-east-1:000000000000:redshiftidcapplication/lc-app"+
			"&IdcDisplayName=UpdatedApp",
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete
	rec = postRedshiftForm(
		t,
		h,
		"Action=DeleteIdcApplication&"+
			"Version=2012-12-01&IdcApplicationArn=arn:aws:redshift:us-east-1:000000000000:redshiftidcapplication/lc-app",
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe after delete — not found
	rec = postRedshiftForm(
		t,
		h,
		"Action=DescribeIdcApplications&"+
			"Version=2012-12-01&IdcApplicationArn=arn:aws:redshift:us-east-1:000000000000:redshiftidcapplication/lc-app",
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "IdcApplicationNotExistsFault")
}

// ---- GetIdentityCenterAuthToken ----

func TestRedshiftHandler_GetIdentityCenterAuthToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:     "missing_arn_returns_400",
			body:     "Action=GetIdentityCenterAuthToken&Version=2012-12-01",
			wantCode: http.StatusBadRequest,
		},
		{
			name:         "with_arn_returns_token",
			body:         "Action=GetIdentityCenterAuthToken&Version=2012-12-01&IdentityCenterApplicationArn=arn:aws:sso::123:application/app-abc",
			wantCode:     http.StatusOK,
			wantContains: []string{"<AuthToken>ict-", "<AuthTokenExpiration>"},
		},
		{
			name:         "different_arn_returns_different_token",
			body:         "Action=GetIdentityCenterAuthToken&Version=2012-12-01&IdentityCenterApplicationArn=arn:aws:sso::456:application/app-xyz",
			wantCode:     http.StatusOK,
			wantContains: []string{"<AuthToken>ict-"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			rec := postRedshiftForm(t, h, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code, rec.Body.String())

			for _, want := range tc.wantContains {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
}
