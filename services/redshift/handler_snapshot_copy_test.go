package redshift_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

// ---- CreateSnapshotCopyGrant ----

func TestHandler_CreateSnapshotCopyGrant(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "success",
			body:         "Action=CreateSnapshotCopyGrant&Version=2012-12-01&SnapshotCopyGrantName=my-grant&KmsKeyId=key123",
			wantCode:     http.StatusOK,
			wantContains: []string{"CreateSnapshotCopyGrantResponse", "my-grant"},
		},
		{
			name:     "duplicate",
			body:     "Action=CreateSnapshotCopyGrant&Version=2012-12-01&SnapshotCopyGrantName=my-grant",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing_name",
			body:     "Action=CreateSnapshotCopyGrant&Version=2012-12-01&SnapshotCopyGrantName=",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			if tt.name == "duplicate" {
				postRedshiftForm(
					t,
					h,
					"Action=CreateSnapshotCopyGrant&Version=2012-12-01&SnapshotCopyGrantName=my-grant",
				)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- DeleteSnapshotCopyGrant ----

func TestHandler_DeleteSnapshotCopyGrant(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *redshift.Handler)
		name     string
		body     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(
					t,
					h,
					"Action=CreateSnapshotCopyGrant&Version=2012-12-01&SnapshotCopyGrantName=grant-to-delete",
				)
			},
			body:     "Action=DeleteSnapshotCopyGrant&Version=2012-12-01&SnapshotCopyGrantName=grant-to-delete",
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			body:     "Action=DeleteSnapshotCopyGrant&Version=2012-12-01&SnapshotCopyGrantName=missing",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing_name",
			body:     "Action=DeleteSnapshotCopyGrant&Version=2012-12-01&SnapshotCopyGrantName=",
			wantCode: http.StatusBadRequest,
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
		})
	}
}

// ---- DescribeSnapshotCopyGrants ----

func TestHandler_DescribeSnapshotCopyGrants(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *redshift.Handler)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "empty",
			body:         "Action=DescribeSnapshotCopyGrants&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeSnapshotCopyGrantsResponse"},
		},
		{
			name: "with_grant",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h, "Action=CreateSnapshotCopyGrant&Version=2012-12-01&SnapshotCopyGrantName=g1")
			},
			body:         "Action=DescribeSnapshotCopyGrants&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"g1"},
		},
		{
			name:     "not_found_filter",
			body:     "Action=DescribeSnapshotCopyGrants&Version=2012-12-01&SnapshotCopyGrantName=missing",
			wantCode: http.StatusBadRequest,
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

// ---- EnableSnapshotCopy ----

func TestHandler_EnableSnapshotCopy(t *testing.T) {
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
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=sc-cluster")
			},
			body: "Action=EnableSnapshotCopy&Version=2012-12-01" +
				"&ClusterIdentifier=sc-cluster&DestinationRegion=us-west-2",
			wantCode:     http.StatusOK,
			wantContains: []string{"EnableSnapshotCopyResponse", "sc-cluster"},
		},
		{
			name: "already_enabled",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=sc-cluster2")
				postRedshiftForm(
					t,
					h,
					"Action=EnableSnapshotCopy&Version=2012-12-01&ClusterIdentifier=sc-cluster2&DestinationRegion=us-west-2",
				)
			},
			body:     "Action=EnableSnapshotCopy&Version=2012-12-01&ClusterIdentifier=sc-cluster2&DestinationRegion=us-west-2",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "cluster_not_found",
			body:     "Action=EnableSnapshotCopy&Version=2012-12-01&ClusterIdentifier=missing&DestinationRegion=us-west-2",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing_region",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=sc-cluster3")
			},
			body:     "Action=EnableSnapshotCopy&Version=2012-12-01&ClusterIdentifier=sc-cluster3&DestinationRegion=",
			wantCode: http.StatusBadRequest,
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

// ---- DisableSnapshotCopy ----

func TestHandler_DisableSnapshotCopy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *redshift.Handler)
		name     string
		body     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=ds-cluster")
				postRedshiftForm(
					t,
					h,
					"Action=EnableSnapshotCopy&Version=2012-12-01&ClusterIdentifier=ds-cluster&DestinationRegion=us-west-2",
				)
			},
			body:     "Action=DisableSnapshotCopy&Version=2012-12-01&ClusterIdentifier=ds-cluster",
			wantCode: http.StatusOK,
		},
		{
			name: "not_enabled",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=ds-cluster2")
			},
			body:     "Action=DisableSnapshotCopy&Version=2012-12-01&ClusterIdentifier=ds-cluster2",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "cluster_not_found",
			body:     "Action=DisableSnapshotCopy&Version=2012-12-01&ClusterIdentifier=missing",
			wantCode: http.StatusBadRequest,
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
		})
	}
}

// ---- ModifySnapshotCopyRetentionPeriod ----

func TestHandler_ModifySnapshotCopyRetentionPeriod(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *redshift.Handler)
		name     string
		body     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=mr-cluster")
				postRedshiftForm(
					t,
					h,
					"Action=EnableSnapshotCopy&Version=2012-12-01&ClusterIdentifier=mr-cluster&DestinationRegion=us-west-2",
				)
			},
			body: "Action=ModifySnapshotCopyRetentionPeriod&Version=2012-12-01" +
				"&ClusterIdentifier=mr-cluster&RetentionPeriod=14",
			wantCode: http.StatusOK,
		},
		{
			name: "not_enabled",
			body: "Action=ModifySnapshotCopyRetentionPeriod&Version=2012-12-01" +
				"&ClusterIdentifier=missing&RetentionPeriod=14",
			wantCode: http.StatusBadRequest,
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
		})
	}
}
