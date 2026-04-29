package redshift_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

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

// ---- DescribeDataShares ----

func TestHandler_DescribeDataShares(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, b *redshift.InMemoryBackend)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "empty",
			body:         "Action=DescribeDataShares&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeDataSharesResponse"},
		},
		{
			name: "with_data_share",
			setup: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				b.AddDataShareInternal(&redshift.DataShare{
					DataShareArn: "arn:aws:redshift::123:datashare:ds1",
					ProducerArn:  "arn:aws:redshift::123:namespace:ns1",
				})
			},
			body:         "Action=DescribeDataShares&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeDataSharesResponse", "ds1"},
		},
		{
			name: "specific_arn",
			setup: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				b.AddDataShareInternal(&redshift.DataShare{
					DataShareArn: "arn:aws:redshift::123:datashare:ds2",
				})
			},
			body: "Action=DescribeDataShares&Version=2012-12-01" +
				"&DataShareArn=arn%3Aaws%3Aredshift%3A%3A123%3Adatashare%3Ads2",
			wantCode:     http.StatusOK,
			wantContains: []string{"ds2"},
		},
		{
			name: "specific_arn_not_found",
			body: "Action=DescribeDataShares&Version=2012-12-01" +
				"&DataShareArn=arn%3Aaws%3Aredshift%3A%3A123%3Adatashare%3Amissing",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			h := redshift.NewHandler(b)

			if tt.setup != nil {
				tt.setup(t, b)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- DescribeDataSharesForConsumer ----

func TestHandler_DescribeDataSharesForConsumer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, b *redshift.InMemoryBackend)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "empty",
			body:         "Action=DescribeDataSharesForConsumer&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeDataSharesForConsumerResponse"},
		},
		{
			name: "with_matching_consumer",
			setup: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				b.AddDataShareInternal(&redshift.DataShare{
					DataShareArn: "arn:aws:redshift::123:datashare:ds3",
					DataShareAssociations: []redshift.DataShareAssociation{
						{ConsumerIdentifier: "consumer1", Status: "ACTIVE"},
					},
				})
			},
			body:         "Action=DescribeDataSharesForConsumer&Version=2012-12-01&ConsumerArn=consumer1",
			wantCode:     http.StatusOK,
			wantContains: []string{"ds3", "consumer1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			h := redshift.NewHandler(b)

			if tt.setup != nil {
				tt.setup(t, b)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- DescribeDataSharesForProducer ----

func TestHandler_DescribeDataSharesForProducer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, b *redshift.InMemoryBackend)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "empty",
			body:         "Action=DescribeDataSharesForProducer&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeDataSharesForProducerResponse"},
		},
		{
			name: "matching_producer",
			setup: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				b.AddDataShareInternal(&redshift.DataShare{
					DataShareArn: "arn:aws:redshift::123:datashare:ds4",
					ProducerArn:  "producer1",
				})
			},
			body:         "Action=DescribeDataSharesForProducer&Version=2012-12-01&ProducerArn=producer1",
			wantCode:     http.StatusOK,
			wantContains: []string{"ds4", "producer1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			h := redshift.NewHandler(b)

			if tt.setup != nil {
				tt.setup(t, b)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- DeauthorizeDataShare ----

func TestHandler_DeauthorizeDataShare(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, b *redshift.InMemoryBackend)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				b.AddDataShareInternal(&redshift.DataShare{
					DataShareArn: "arn:aws:redshift::123:datashare:ds5",
					DataShareAssociations: []redshift.DataShareAssociation{
						{ConsumerIdentifier: "consumer2", Status: "AUTHORIZED"},
					},
				})
			},
			body: "Action=DeauthorizeDataShare&Version=2012-12-01" +
				"&DataShareArn=arn%3Aaws%3Aredshift%3A%3A123%3Adatashare%3Ads5&ConsumerIdentifier=consumer2",
			wantCode:     http.StatusOK,
			wantContains: []string{"DeauthorizeDataShareResponse", "DEAUTHORIZED"},
		},
		{
			name:     "missing_arn",
			body:     "Action=DeauthorizeDataShare&Version=2012-12-01&DataShareArn=&ConsumerIdentifier=consumer2",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "not_found",
			body: "Action=DeauthorizeDataShare&Version=2012-12-01" +
				"&DataShareArn=arn%3Aaws%3Aredshift%3A%3A123%3Adatashare%3Amissing&ConsumerIdentifier=consumer2",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			h := redshift.NewHandler(b)

			if tt.setup != nil {
				tt.setup(t, b)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- DisassociateDataShareConsumer ----

func TestHandler_DisassociateDataShareConsumer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, b *redshift.InMemoryBackend)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				b.AddDataShareInternal(&redshift.DataShare{
					DataShareArn: "arn:aws:redshift::123:datashare:ds6",
					DataShareAssociations: []redshift.DataShareAssociation{
						{ConsumerIdentifier: "consumer3", Status: "ACTIVE"},
					},
				})
			},
			body: "Action=DisassociateDataShareConsumer&Version=2012-12-01" +
				"&DataShareArn=arn%3Aaws%3Aredshift%3A%3A123%3Adatashare%3Ads6&ConsumerArn=consumer3",
			wantCode:     http.StatusOK,
			wantContains: []string{"DisassociateDataShareConsumerResponse"},
		},
		{
			name:     "missing_arn",
			body:     "Action=DisassociateDataShareConsumer&Version=2012-12-01&DataShareArn=&ConsumerArn=consumer3",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			h := redshift.NewHandler(b)

			if tt.setup != nil {
				tt.setup(t, b)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- RejectDataShare ----

func TestHandler_RejectDataShare(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, b *redshift.InMemoryBackend)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				b.AddDataShareInternal(&redshift.DataShare{
					DataShareArn: "arn:aws:redshift::123:datashare:ds7",
					DataShareAssociations: []redshift.DataShareAssociation{
						{ConsumerIdentifier: "consumer4", Status: "PENDING_AUTHORIZATION"},
					},
				})
			},
			body: "Action=RejectDataShare&Version=2012-12-01" +
				"&DataShareArn=arn%3Aaws%3Aredshift%3A%3A123%3Adatashare%3Ads7",
			wantCode:     http.StatusOK,
			wantContains: []string{"RejectDataShareResponse", "REJECTED"},
		},
		{
			name:     "missing_arn",
			body:     "Action=RejectDataShare&Version=2012-12-01&DataShareArn=",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "not_found",
			body: "Action=RejectDataShare&Version=2012-12-01" +
				"&DataShareArn=arn%3Aaws%3Aredshift%3A%3A123%3Adatashare%3Amissing",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			h := redshift.NewHandler(b)

			if tt.setup != nil {
				tt.setup(t, b)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- DescribeEndpointAuthorization ----

func TestHandler_DescribeEndpointAuthorization(t *testing.T) {
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
			body:         "Action=DescribeEndpointAuthorization&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeEndpointAuthorizationResponse"},
		},
		{
			name: "with_auth",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=ea-cluster")
				postRedshiftForm(
					t,
					h,
					"Action=AuthorizeEndpointAccess&Version=2012-12-01&ClusterIdentifier=ea-cluster&Account=acc1",
				)
			},
			body:         "Action=DescribeEndpointAuthorization&Version=2012-12-01&ClusterIdentifier=ea-cluster",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeEndpointAuthorizationResponse", "ea-cluster", "acc1"},
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

// ---- RevokeEndpointAccess ----

func TestHandler_RevokeEndpointAccess(t *testing.T) {
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
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=rea-cluster")
				postRedshiftForm(
					t,
					h,
					"Action=AuthorizeEndpointAccess&Version=2012-12-01&ClusterIdentifier=rea-cluster&Account=acc2",
				)
			},
			body:         "Action=RevokeEndpointAccess&Version=2012-12-01&ClusterIdentifier=rea-cluster&Account=acc2",
			wantCode:     http.StatusOK,
			wantContains: []string{"RevokeEndpointAccessResponse", "Revoking"},
		},
		{
			name:     "missing_cluster_id",
			body:     "Action=RevokeEndpointAccess&Version=2012-12-01&ClusterIdentifier=&Account=acc2",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "not_found",
			body:     "Action=RevokeEndpointAccess&Version=2012-12-01&ClusterIdentifier=missing&Account=acc2",
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

// ---- DescribeResize ----

func TestHandler_DescribeResize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, b *redshift.InMemoryBackend)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(t *testing.T, b *redshift.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateCluster("dr-cluster", "dc2.large", "dev", "admin")
				require.NoError(t, err)
				b.AddActiveResizeInternal("dr-cluster", &redshift.ResizeProgress{
					Status:         "IN_PROGRESS",
					TargetNodeType: "dc2.large",
				})
			},
			body:         "Action=DescribeResize&Version=2012-12-01&ClusterIdentifier=dr-cluster",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeResizeResponse", "IN_PROGRESS"},
		},
		{
			name:     "missing_cluster_id",
			body:     "Action=DescribeResize&Version=2012-12-01&ClusterIdentifier=",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "not_found",
			body:     "Action=DescribeResize&Version=2012-12-01&ClusterIdentifier=missing",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			h := redshift.NewHandler(b)

			if tt.setup != nil {
				tt.setup(t, b)
			}

			rec := postRedshiftForm(t, h, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- RevokeSnapshotAccess ----

func TestHandler_RevokeSnapshotAccess(t *testing.T) {
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
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=rsa-cluster")
				postRedshiftForm(
					t,
					h,
					"Action=CreateClusterSnapshot&Version=2012-12-01&SnapshotIdentifier=snap-rsa&ClusterIdentifier=rsa-cluster",
				)
				postRedshiftForm(
					t,
					h,
					"Action=AuthorizeSnapshotAccess&Version=2012-12-01&SnapshotIdentifier=snap-rsa&AccountWithRestoreAccess=acc-rsa",
				)
			},
			body: "Action=RevokeSnapshotAccess&Version=2012-12-01" +
				"&SnapshotIdentifier=snap-rsa&AccountWithRestoreAccess=acc-rsa",
			wantCode:     http.StatusOK,
			wantContains: []string{"RevokeSnapshotAccessResponse", "snap-rsa"},
		},
		{
			name:     "missing_snapshot_id",
			body:     "Action=RevokeSnapshotAccess&Version=2012-12-01&SnapshotIdentifier=&AccountWithRestoreAccess=acc-rsa",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "snapshot_not_found",
			body: "Action=RevokeSnapshotAccess&Version=2012-12-01" +
				"&SnapshotIdentifier=missing&AccountWithRestoreAccess=acc-rsa",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "account_not_found",
			setup: func(t *testing.T, h *redshift.Handler) {
				t.Helper()
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=rsa-cluster2")
				postRedshiftForm(
					t,
					h,
					"Action=CreateClusterSnapshot&Version=2012-12-01&SnapshotIdentifier=snap-rsa2&ClusterIdentifier=rsa-cluster2",
				)
			},
			body: "Action=RevokeSnapshotAccess&Version=2012-12-01" +
				"&SnapshotIdentifier=snap-rsa2&AccountWithRestoreAccess=nonexistent",
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

// ---- ModifyClusterSnapshot ----

func TestHandler_ModifyClusterSnapshot(t *testing.T) {
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
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=mcs-cluster")
				postRedshiftForm(
					t,
					h,
					"Action=CreateClusterSnapshot&Version=2012-12-01&SnapshotIdentifier=snap-mcs&ClusterIdentifier=mcs-cluster",
				)
			},
			body: "Action=ModifyClusterSnapshot&Version=2012-12-01" +
				"&SnapshotIdentifier=snap-mcs&ManualSnapshotRetentionPeriod=14",
			wantCode:     http.StatusOK,
			wantContains: []string{"ModifyClusterSnapshotResponse", "snap-mcs", "14"},
		},
		{
			name:     "missing_snapshot_id",
			body:     "Action=ModifyClusterSnapshot&Version=2012-12-01&SnapshotIdentifier=",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "snapshot_not_found",
			body:     "Action=ModifyClusterSnapshot&Version=2012-12-01&SnapshotIdentifier=missing",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "invalid_retention",
			body: "Action=ModifyClusterSnapshot&Version=2012-12-01" +
				"&SnapshotIdentifier=snap-mcs&ManualSnapshotRetentionPeriod=notanumber",
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

// ---- GetClusterCredentials ----

func TestHandler_GetClusterCredentials(t *testing.T) {
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
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=gcc-cluster")
			},
			body:         "Action=GetClusterCredentials&Version=2012-12-01&ClusterIdentifier=gcc-cluster&DbUser=admin",
			wantCode:     http.StatusOK,
			wantContains: []string{"GetClusterCredentialsResponse", "admin", "Tmp1_"},
		},
		{
			name:     "missing_cluster_id",
			body:     "Action=GetClusterCredentials&Version=2012-12-01&ClusterIdentifier=&DbUser=admin",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing_db_user",
			body:     "Action=GetClusterCredentials&Version=2012-12-01&ClusterIdentifier=gcc-cluster&DbUser=",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "cluster_not_found",
			body:     "Action=GetClusterCredentials&Version=2012-12-01&ClusterIdentifier=missing&DbUser=admin",
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

// ---- DescribeAccountAttributes ----

func TestHandler_DescribeAccountAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "success",
			body:         "Action=DescribeAccountAttributes&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeAccountAttributesResponse"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			rec := postRedshiftForm(t, h, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- DescribeClusterTracks ----

func TestHandler_DescribeClusterTracks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "success",
			body:         "Action=DescribeClusterTracks&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeClusterTracksResponse", "current", "trailing"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			rec := postRedshiftForm(t, h, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- DescribeClusterVersions ----

func TestHandler_DescribeClusterVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "success",
			body:         "Action=DescribeClusterVersions&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeClusterVersionsResponse", "1.0"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			rec := postRedshiftForm(t, h, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- DescribeOrderableClusterOptions ----

func TestHandler_DescribeOrderableClusterOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "success",
			body:         "Action=DescribeOrderableClusterOptions&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeOrderableClusterOptionsResponse", "dc2.large"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			rec := postRedshiftForm(t, h, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- DescribeStorage ----

func TestHandler_DescribeStorage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "success",
			body:         "Action=DescribeStorage&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"DescribeStorageResponse"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newRedshiftHandler()
			rec := postRedshiftForm(t, h, tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)

			for _, s := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), s)
			}
		})
	}
}

// ---- Backend unit tests ----

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

func TestBackend_DescribeResize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(b *redshift.InMemoryBackend)
		name      string
		clusterID string
		wantErr   bool
	}{
		{
			name: "success",
			setup: func(b *redshift.InMemoryBackend) {
				_, _ = b.CreateCluster("c1", "dc2.large", "dev", "admin")
				b.AddActiveResizeInternal("c1", &redshift.ResizeProgress{Status: "IN_PROGRESS"})
			},
			clusterID: "c1",
			wantErr:   false,
		},
		{
			name:      "missing_cluster_id",
			clusterID: "",
			wantErr:   true,
		},
		{
			name:      "not_found",
			clusterID: "missing",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.setup != nil {
				tt.setup(b)
			}

			rp, err := b.DescribeResize(tt.clusterID)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "IN_PROGRESS", rp.Status)
		})
	}
}

func TestBackend_GetClusterCredentials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(b *redshift.InMemoryBackend)
		name      string
		clusterID string
		dbUser    string
		wantErr   bool
	}{
		{
			name: "success",
			setup: func(b *redshift.InMemoryBackend) {
				_, _ = b.CreateCluster("c1", "dc2.large", "dev", "admin")
			},
			clusterID: "c1",
			dbUser:    "alice",
			wantErr:   false,
		},
		{
			name:      "missing_cluster_id",
			clusterID: "",
			dbUser:    "alice",
			wantErr:   true,
		},
		{
			name:      "missing_db_user",
			clusterID: "c1",
			dbUser:    "",
			wantErr:   true,
		},
		{
			name:      "cluster_not_found",
			clusterID: "missing",
			dbUser:    "alice",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.setup != nil {
				tt.setup(b)
			}

			creds, err := b.GetClusterCredentials(tt.clusterID, tt.dbUser, false)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.dbUser, creds.DBUser)
			assert.NotEmpty(t, creds.DBPassword)
			assert.False(t, creds.Expiration.IsZero())
		})
	}
}

func TestBackend_RevokeSnapshotAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(b *redshift.InMemoryBackend)
		name       string
		snapshotID string
		accountID  string
		wantErr    bool
	}{
		{
			name: "success",
			setup: func(b *redshift.InMemoryBackend) {
				_, _ = b.CreateCluster("c1", "dc2.large", "dev", "admin")
				_, _ = b.CreateClusterSnapshot("snap1", "c1")
				_, _ = b.AuthorizeSnapshotAccess("snap1", "acc1")
			},
			snapshotID: "snap1",
			accountID:  "acc1",
			wantErr:    false,
		},
		{
			name:       "missing_snapshot_id",
			snapshotID: "",
			accountID:  "acc1",
			wantErr:    true,
		},
		{
			name:       "snapshot_not_found",
			snapshotID: "missing",
			accountID:  "acc1",
			wantErr:    true,
		},
		{
			name: "account_not_found",
			setup: func(b *redshift.InMemoryBackend) {
				_, _ = b.CreateCluster("c1", "dc2.large", "dev", "admin")
				_, _ = b.CreateClusterSnapshot("snap2", "c1")
			},
			snapshotID: "snap2",
			accountID:  "nonexistent",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.setup != nil {
				tt.setup(b)
			}

			snap, err := b.RevokeSnapshotAccess(tt.snapshotID, tt.accountID)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Empty(t, snap.AccountsWithRestoreAccess)
		})
	}
}

func TestBackend_ModifyClusterSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup           func(b *redshift.InMemoryBackend)
		name            string
		snapshotID      string
		retentionPeriod int
		wantErr         bool
		wantRetention   int
	}{
		{
			name: "success",
			setup: func(b *redshift.InMemoryBackend) {
				_, _ = b.CreateCluster("c1", "dc2.large", "dev", "admin")
				_, _ = b.CreateClusterSnapshot("snap1", "c1")
			},
			snapshotID:      "snap1",
			retentionPeriod: 30,
			wantErr:         false,
			wantRetention:   30,
		},
		{
			name:       "missing_snapshot_id",
			snapshotID: "",
			wantErr:    true,
		},
		{
			name:       "snapshot_not_found",
			snapshotID: "missing",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.setup != nil {
				tt.setup(b)
			}

			snap, err := b.ModifyClusterSnapshot(tt.snapshotID, tt.retentionPeriod, false)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantRetention, snap.ManualSnapshotRetentionPeriod)
		})
	}
}
