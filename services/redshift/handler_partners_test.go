package redshift_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

// ---- AddPartner ----

func TestHandler_AddPartner(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *redshift.Handler, b *redshift.InMemoryBackend)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *redshift.Handler, _ *redshift.InMemoryBackend) {
				t.Helper()
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=partner-cluster")
			},
			body: "Action=AddPartner&Version=2012-12-01" +
				"&ClusterIdentifier=partner-cluster&DatabaseName=mydb&PartnerIntegrationId=my-partner",
			wantCode:     http.StatusOK,
			wantContains: []string{"AddPartnerResponse", "mydb", "my-partner"},
		},
		{
			name:         "missing_cluster_identifier",
			body:         "Action=AddPartner&Version=2012-12-01&DatabaseName=mydb&PartnerIntegrationId=my-partner",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "missing_database_name",
			body:         "Action=AddPartner&Version=2012-12-01&ClusterIdentifier=c1&PartnerIntegrationId=my-partner",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "missing_partner_name",
			body:         "Action=AddPartner&Version=2012-12-01&ClusterIdentifier=c1&DatabaseName=mydb",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "cluster_not_found",
			body: "Action=AddPartner&Version=2012-12-01" +
				"&ClusterIdentifier=nonexistent&DatabaseName=mydb&PartnerIntegrationId=p1",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ClusterNotFound"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			h := redshift.NewHandler(b)
			if tt.setup != nil {
				tt.setup(t, h, b)
			}

			rec := postRedshiftForm(t, h, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)
			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

func TestBackend_AddPartner(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		setup     func(b *redshift.InMemoryBackend)
		name      string
		accountID string
		clusterID string
		database  string
		partner   string
	}{
		{
			name: "success",
			setup: func(b *redshift.InMemoryBackend) {
				_, _ = b.CreateCluster("p-cluster", "", "", "")
			},
			accountID: "000000000000",
			clusterID: "p-cluster",
			database:  "mydb",
			partner:   "my-partner",
		},
		{
			name:      "empty_cluster_id",
			clusterID: "",
			database:  "mydb",
			partner:   "my-partner",
			wantErr:   redshift.ErrInvalidParameter,
		},
		{
			name:      "empty_database",
			clusterID: "p-cluster",
			database:  "",
			partner:   "my-partner",
			wantErr:   redshift.ErrInvalidParameter,
		},
		{
			name:      "empty_partner",
			clusterID: "p-cluster",
			database:  "mydb",
			partner:   "",
			wantErr:   redshift.ErrInvalidParameter,
		},
		{
			name:      "cluster_not_found",
			clusterID: "nonexistent",
			database:  "mydb",
			partner:   "p1",
			wantErr:   redshift.ErrClusterNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.setup != nil {
				tt.setup(b)
			}

			result, err := b.AddPartner(tt.accountID, tt.clusterID, tt.database, tt.partner)

			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.clusterID, result.ClusterIdentifier)
			assert.Equal(t, tt.database, result.DatabaseName)
			assert.Equal(t, tt.partner, result.PartnerName)
			assert.Equal(t, "Active", result.Status)
		})
	}
}

// ---- AddPartner response includes ClusterIdentifier ----

func TestAddPartner_ResponseIncludesClusterIdentifier(t *testing.T) {
	t.Parallel()

	h := newRedshiftHandler()
	postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=ap-cluster")

	rec := postRedshiftForm(t, h,
		"Action=AddPartner&Version=2012-12-01"+
			"&ClusterIdentifier=ap-cluster"+
			"&DatabaseName=mydb"+
			"&PartnerIntegrationId=mypartner")

	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "AddPartnerResponse")
	assert.Contains(t, body, "ap-cluster")
	assert.Contains(t, body, "mydb")
	assert.Contains(t, body, "mypartner")
}

// ---- DeletePartner ----

func TestHandler_DeletePartner(t *testing.T) {
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
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=dp-cluster")
				postRedshiftForm(
					t,
					h,
					"Action=AddPartner&Version=2012-12-01"+
						"&ClusterIdentifier=dp-cluster&DatabaseName=mydb&PartnerIntegrationId=mypartner",
				)
			},
			body: "Action=DeletePartner&Version=2012-12-01" +
				"&ClusterIdentifier=dp-cluster&DatabaseName=mydb&PartnerIntegrationId=mypartner",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeletePartnerResponse", "dp-cluster"},
		},
		{
			name: "not_found",
			body: "Action=DeletePartner&Version=2012-12-01" +
				"&ClusterIdentifier=dp-cluster&DatabaseName=mydb&PartnerIntegrationId=missing",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing_cluster_id",
			body: "Action=DeletePartner&Version=2012-12-01" +
				"&ClusterIdentifier=&DatabaseName=mydb&PartnerIntegrationId=mypartner",
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

// ---- DescribePartners ----

func TestHandler_DescribePartners(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *redshift.Handler)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "empty",
			body: "Action=DescribePartners&Version=2012-12-01&ClusterIdentifier=c1",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=c1")
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribePartnersResponse"},
		},
		{
			name: "with_partner",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=c2")
				postRedshiftForm(
					t,
					h,
					"Action=AddPartner&Version=2012-12-01&ClusterIdentifier=c2&DatabaseName=db1&PartnerIntegrationId=partner1",
				)
			},
			body:         "Action=DescribePartners&Version=2012-12-01&ClusterIdentifier=c2",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribePartnersResponse", "partner1", "c2"},
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

// ---- UpdatePartnerStatus ----

func TestHandler_UpdatePartnerStatus(t *testing.T) {
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
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=ups-cluster")
				postRedshiftForm(
					t,
					h,
					"Action=AddPartner&Version=2012-12-01"+
						"&ClusterIdentifier=ups-cluster&DatabaseName=db1&PartnerIntegrationId=partner1",
				)
			},
			body: "Action=UpdatePartnerStatus&Version=2012-12-01" +
				"&ClusterIdentifier=ups-cluster&DatabaseName=db1&PartnerIntegrationId=partner1&Status=Active&StatusMessage=ok",
			wantCode:     http.StatusOK,
			wantContains: []string{"UpdatePartnerStatusResponse", "ups-cluster"},
		},
		{
			name: "not_found",
			body: "Action=UpdatePartnerStatus&Version=2012-12-01" +
				"&ClusterIdentifier=ups-cluster&DatabaseName=db1&PartnerIntegrationId=missing",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing_cluster_id",
			body: "Action=UpdatePartnerStatus&Version=2012-12-01" +
				"&ClusterIdentifier=&DatabaseName=db1&PartnerIntegrationId=partner1",
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

// ---- Backend.DeletePartner ----

func TestBackend_DeletePartner(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(b *redshift.InMemoryBackend)
		name    string
		cluster string
		db      string
		partner string
		wantErr bool
	}{
		{
			name: "success",
			setup: func(b *redshift.InMemoryBackend) {
				_, _ = b.CreateCluster("c1", "dc2.large", "dev", "admin")
				_, _ = b.AddPartner("acc", "c1", "mydb", "partner1")
			},
			cluster: "c1",
			db:      "mydb",
			partner: "partner1",
			wantErr: false,
		},
		{
			name:    "missing_cluster_id",
			wantErr: true,
		},
		{
			name:    "not_found",
			cluster: "c1",
			db:      "mydb",
			partner: "nonexistent",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.setup != nil {
				tt.setup(b)
			}

			err := b.DeletePartner("acc", tt.cluster, tt.db, tt.partner)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, 0, redshift.PartnerCount(b))
		})
	}
}
