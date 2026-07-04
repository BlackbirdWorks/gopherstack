package redshift_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshift"
)

// ---- AcceptReservedNodeExchange ----

func TestRedshiftHandler_AcceptReservedNodeExchange(t *testing.T) {
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
			setup: func(_ *testing.T, _ *redshift.Handler, b *redshift.InMemoryBackend) {
				b.AddReservedNodeInternal(&redshift.ReservedNode{
					ReservedNodeID:         "rn-001",
					ReservedNodeOfferingID: "offering-old",
					NodeType:               "dc2.large",
					NodeCount:              1,
					State:                  "active",
					OfferingType:           "All Upfront",
					CurrencyCode:           "USD",
				})
			},
			body: "Action=AcceptReservedNodeExchange&Version=2012-12-01" +
				"&ReservedNodeId=rn-001&TargetReservedNodeOfferingId=offering-new",
			wantCode:     http.StatusOK,
			wantContains: []string{"AcceptReservedNodeExchangeResponse", "offering-new", "rn-001"},
		},
		{
			name:         "missing_reserved_node_id",
			body:         "Action=AcceptReservedNodeExchange&Version=2012-12-01&TargetReservedNodeOfferingId=offering-new",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "missing_target_offering_id",
			body:         "Action=AcceptReservedNodeExchange&Version=2012-12-01&ReservedNodeId=rn-001",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "reserved_node_not_found",
			body: "Action=AcceptReservedNodeExchange&Version=2012-12-01" +
				"&ReservedNodeId=nonexistent&TargetReservedNodeOfferingId=offering-new",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ReservedNodeNotFound"},
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

// ---- AddPartner ----

func TestRedshiftHandler_AddPartner(t *testing.T) {
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

// ---- AssociateDataShareConsumer ----

func TestRedshiftHandler_AssociateDataShareConsumer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(_ *testing.T, _ *redshift.Handler, b *redshift.InMemoryBackend)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(_ *testing.T, _ *redshift.Handler, b *redshift.InMemoryBackend) {
				b.AddDataShareInternal(&redshift.DataShare{
					DataShareArn: "arn:aws:redshift:us-east-1:000000000000:datashare:ds-001",
					ProducerArn:  "arn:aws:redshift:us-east-1:000000000000:namespace:ns-001",
				})
			},
			body: "Action=AssociateDataShareConsumer&Version=2012-12-01" +
				"&DataShareArn=arn%3Aaws%3Aredshift%3Aus-east-1%3A000000000000%3Adatashare%3Ads-001" +
				"&ConsumerArn=arn%3Aaws%3Aredshift%3Aus-east-1%3A111111111111%3Anamespace%3Ans-002",
			wantCode:     http.StatusOK,
			wantContains: []string{"AssociateDataShareConsumerResponse"},
		},
		{
			name: "missing_data_share_arn",
			body: "Action=AssociateDataShareConsumer&Version=2012-12-01" +
				"&ConsumerArn=arn:aws:redshift:us-east-1:111111111111:namespace:ns-002",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "data_share_not_found",
			body:         "Action=AssociateDataShareConsumer&Version=2012-12-01&DataShareArn=nonexistent&ConsumerArn=consumer",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DataShareNotFound"},
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

// ---- AuthorizeClusterSecurityGroupIngress ----

func TestRedshiftHandler_AuthorizeClusterSecurityGroupIngress(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(_ *testing.T, _ *redshift.Handler, b *redshift.InMemoryBackend)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success_cidr",
			setup: func(_ *testing.T, _ *redshift.Handler, b *redshift.InMemoryBackend) {
				b.AddSecurityGroupInternal(&redshift.ClusterSecurityGroup{
					ClusterSecurityGroupName: "my-sg",
					Description:              "test sg",
				})
			},
			body: "Action=AuthorizeClusterSecurityGroupIngress&Version=2012-12-01" +
				"&ClusterSecurityGroupName=my-sg&CIDRIP=10.0.0.0%2F8",
			wantCode:     http.StatusOK,
			wantContains: []string{"AuthorizeClusterSecurityGroupIngressResponse", "10.0.0.0/8"},
		},
		{
			name: "success_ec2_sg",
			setup: func(_ *testing.T, _ *redshift.Handler, b *redshift.InMemoryBackend) {
				b.AddSecurityGroupInternal(&redshift.ClusterSecurityGroup{
					ClusterSecurityGroupName: "my-sg2",
				})
			},
			body: "Action=AuthorizeClusterSecurityGroupIngress&Version=2012-12-01" +
				"&ClusterSecurityGroupName=my-sg2&EC2SecurityGroupName=sg-12345&EC2SecurityGroupOwnerId=111111111111",
			wantCode:     http.StatusOK,
			wantContains: []string{"AuthorizeClusterSecurityGroupIngressResponse", "sg-12345"},
		},
		{
			name:         "missing_group_name",
			body:         "Action=AuthorizeClusterSecurityGroupIngress&Version=2012-12-01&CIDRIP=10.0.0.0/8",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "missing_cidr_and_ec2_sg",
			body:         "Action=AuthorizeClusterSecurityGroupIngress&Version=2012-12-01&ClusterSecurityGroupName=my-sg",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "security_group_not_found",
			body: "Action=AuthorizeClusterSecurityGroupIngress&Version=2012-12-01" +
				"&ClusterSecurityGroupName=nonexistent&CIDRIP=10.0.0.1/32",
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

// ---- AuthorizeDataShare ----

func TestRedshiftHandler_AuthorizeDataShare(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(_ *testing.T, _ *redshift.Handler, b *redshift.InMemoryBackend)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(_ *testing.T, _ *redshift.Handler, b *redshift.InMemoryBackend) {
				b.AddDataShareInternal(&redshift.DataShare{
					DataShareArn: "arn:aws:redshift:us-east-1:000000000000:datashare:ds-auth-001",
				})
			},
			body: "Action=AuthorizeDataShare&Version=2012-12-01" +
				"&DataShareArn=arn%3Aaws%3Aredshift%3Aus-east-1%3A000000000000%3Adatashare%3Ads-auth-001" +
				"&ConsumerIdentifier=111111111111",
			wantCode:     http.StatusOK,
			wantContains: []string{"AuthorizeDataShareResponse"},
		},
		{
			name:         "missing_data_share_arn",
			body:         "Action=AuthorizeDataShare&Version=2012-12-01&ConsumerIdentifier=111111111111",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "missing_consumer_identifier",
			body: "Action=AuthorizeDataShare&Version=2012-12-01" +
				"&DataShareArn=arn:aws:redshift:us-east-1:000000000000:datashare:ds-001",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "data_share_not_found",
			body:         "Action=AuthorizeDataShare&Version=2012-12-01&DataShareArn=nonexistent&ConsumerIdentifier=consumer",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"DataShareNotFound"},
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

// ---- AuthorizeEndpointAccess ----

func TestRedshiftHandler_AuthorizeEndpointAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *redshift.Handler, b *redshift.InMemoryBackend)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success_all_vpcs",
			setup: func(t *testing.T, h *redshift.Handler, _ *redshift.InMemoryBackend) {
				t.Helper()
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=ep-cluster")
			},
			body: "Action=AuthorizeEndpointAccess&Version=2012-12-01" +
				"&ClusterIdentifier=ep-cluster&Account=111111111111",
			wantCode:     http.StatusOK,
			wantContains: []string{"AuthorizeEndpointAccessResponse", "111111111111"},
		},
		{
			name: "success_specific_vpcs",
			setup: func(t *testing.T, h *redshift.Handler, _ *redshift.InMemoryBackend) {
				t.Helper()
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=ep-cluster-vpc")
			},
			body: "Action=AuthorizeEndpointAccess&Version=2012-12-01" +
				"&ClusterIdentifier=ep-cluster-vpc&Account=222222222222" +
				"&VpcIds.VpcIdentifier.1=vpc-12345&VpcIds.VpcIdentifier.2=vpc-67890",
			wantCode:     http.StatusOK,
			wantContains: []string{"AuthorizeEndpointAccessResponse", "222222222222"},
		},
		{
			name:         "missing_cluster_identifier",
			body:         "Action=AuthorizeEndpointAccess&Version=2012-12-01&Account=111111111111",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "missing_account",
			body:         "Action=AuthorizeEndpointAccess&Version=2012-12-01&ClusterIdentifier=ep-cluster",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "cluster_not_found",
			body:         "Action=AuthorizeEndpointAccess&Version=2012-12-01&ClusterIdentifier=nonexistent&Account=111111111111",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ClusterNotFound"},
		},
		{
			name: "duplicate_authorization",
			setup: func(t *testing.T, h *redshift.Handler, _ *redshift.InMemoryBackend) {
				t.Helper()
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=dup-ep-cluster")
				postRedshiftForm(t, h, "Action=AuthorizeEndpointAccess&Version=2012-12-01"+
					"&ClusterIdentifier=dup-ep-cluster&Account=333333333333")
			},
			body: "Action=AuthorizeEndpointAccess&Version=2012-12-01" +
				"&ClusterIdentifier=dup-ep-cluster&Account=333333333333",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"EndpointAuthorizationAlreadyExists"},
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

// ---- AuthorizeSnapshotAccess ----

func TestRedshiftHandler_AuthorizeSnapshotAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(_ *testing.T, _ *redshift.Handler, b *redshift.InMemoryBackend)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(_ *testing.T, _ *redshift.Handler, b *redshift.InMemoryBackend) {
				b.AddSnapshotInternal(&redshift.Snapshot{
					SnapshotIdentifier: "snap-001",
					ClusterIdentifier:  "my-cluster",
					Status:             "available",
				})
			},
			body: "Action=AuthorizeSnapshotAccess&Version=2012-12-01" +
				"&SnapshotIdentifier=snap-001&AccountWithRestoreAccess=111111111111",
			wantCode:     http.StatusOK,
			wantContains: []string{"AuthorizeSnapshotAccessResponse", "snap-001", "111111111111"},
		},
		{
			name:         "missing_snapshot_identifier",
			body:         "Action=AuthorizeSnapshotAccess&Version=2012-12-01&AccountWithRestoreAccess=111111111111",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "missing_account",
			body:         "Action=AuthorizeSnapshotAccess&Version=2012-12-01&SnapshotIdentifier=snap-001",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "snapshot_not_found",
			body: "Action=AuthorizeSnapshotAccess&Version=2012-12-01" +
				"&SnapshotIdentifier=nonexistent&AccountWithRestoreAccess=111111111111",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ClusterSnapshotNotFound"},
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

// ---- BatchDeleteClusterSnapshots ----

func TestRedshiftHandler_BatchDeleteClusterSnapshots(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(_ *testing.T, _ *redshift.Handler, b *redshift.InMemoryBackend)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success_all_deleted",
			setup: func(_ *testing.T, _ *redshift.Handler, b *redshift.InMemoryBackend) {
				b.AddSnapshotInternal(
					&redshift.Snapshot{SnapshotIdentifier: "snap-del-1", ClusterIdentifier: "c1", Status: "available"},
				)
				b.AddSnapshotInternal(
					&redshift.Snapshot{SnapshotIdentifier: "snap-del-2", ClusterIdentifier: "c1", Status: "available"},
				)
			},
			body: "Action=BatchDeleteClusterSnapshots&Version=2012-12-01" +
				"&Identifiers.SnapshotIdentifier.1=snap-del-1&Identifiers.SnapshotIdentifier.2=snap-del-2",
			wantCode:     http.StatusOK,
			wantContains: []string{"BatchDeleteClusterSnapshotsResponse"},
		},
		{
			name: "partial_success_with_errors",
			setup: func(_ *testing.T, _ *redshift.Handler, b *redshift.InMemoryBackend) {
				b.AddSnapshotInternal(
					&redshift.Snapshot{SnapshotIdentifier: "snap-del-3", ClusterIdentifier: "c1", Status: "available"},
				)
			},
			body: "Action=BatchDeleteClusterSnapshots&Version=2012-12-01" +
				"&Identifiers.SnapshotIdentifier.1=snap-del-3&Identifiers.SnapshotIdentifier.2=nonexistent",
			wantCode:     http.StatusOK,
			wantContains: []string{"BatchDeleteClusterSnapshotsResponse", "ClusterSnapshotNotFound"},
		},
		{
			name:         "empty_list",
			body:         "Action=BatchDeleteClusterSnapshots&Version=2012-12-01",
			wantCode:     http.StatusOK,
			wantContains: []string{"BatchDeleteClusterSnapshotsResponse"},
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

// ---- BatchModifyClusterSnapshots ----

func TestRedshiftHandler_BatchModifyClusterSnapshots(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(_ *testing.T, _ *redshift.Handler, b *redshift.InMemoryBackend)
		name         string
		body         string
		wantContains []string
		wantCode     int
	}{
		{
			name: "success",
			setup: func(_ *testing.T, _ *redshift.Handler, b *redshift.InMemoryBackend) {
				b.AddSnapshotInternal(
					&redshift.Snapshot{SnapshotIdentifier: "snap-mod-1", ClusterIdentifier: "c1", Status: "available"},
				)
				b.AddSnapshotInternal(
					&redshift.Snapshot{SnapshotIdentifier: "snap-mod-2", ClusterIdentifier: "c1", Status: "available"},
				)
			},
			body: "Action=BatchModifyClusterSnapshots&Version=2012-12-01" +
				"&SnapshotIdentifierList.String.1=snap-mod-1&SnapshotIdentifierList.String.2=snap-mod-2" +
				"&ManualSnapshotRetentionPeriod=7",
			wantCode:     http.StatusOK,
			wantContains: []string{"BatchModifyClusterSnapshotsResponse"},
		},
		{
			name: "partial_with_errors",
			setup: func(_ *testing.T, _ *redshift.Handler, b *redshift.InMemoryBackend) {
				b.AddSnapshotInternal(
					&redshift.Snapshot{SnapshotIdentifier: "snap-mod-3", ClusterIdentifier: "c1", Status: "available"},
				)
			},
			body: "Action=BatchModifyClusterSnapshots&Version=2012-12-01" +
				"&SnapshotIdentifierList.String.1=snap-mod-3&SnapshotIdentifierList.String.2=nonexistent" +
				"&ManualSnapshotRetentionPeriod=14",
			wantCode:     http.StatusOK,
			wantContains: []string{"BatchModifyClusterSnapshotsResponse", "ClusterSnapshotNotFound"},
		},
		{
			name: "invalid_retention_period",
			setup: func(_ *testing.T, _ *redshift.Handler, b *redshift.InMemoryBackend) {
				b.AddSnapshotInternal(
					&redshift.Snapshot{SnapshotIdentifier: "snap-mod-4", ClusterIdentifier: "c1", Status: "available"},
				)
			},
			body: "Action=BatchModifyClusterSnapshots&Version=2012-12-01" +
				"&SnapshotIdentifierList.String.1=snap-mod-4&ManualSnapshotRetentionPeriod=notanumber",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name: "with_force_flag",
			setup: func(_ *testing.T, _ *redshift.Handler, b *redshift.InMemoryBackend) {
				b.AddSnapshotInternal(
					&redshift.Snapshot{SnapshotIdentifier: "snap-mod-5", ClusterIdentifier: "c1", Status: "available"},
				)
			},
			body: "Action=BatchModifyClusterSnapshots&Version=2012-12-01" +
				"&SnapshotIdentifierList.String.1=snap-mod-5&ManualSnapshotRetentionPeriod=30&Force=true",
			wantCode:     http.StatusOK,
			wantContains: []string{"BatchModifyClusterSnapshotsResponse"},
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

// ---- CancelResize ----

func TestRedshiftHandler_CancelResize(t *testing.T) {
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
			setup: func(t *testing.T, h *redshift.Handler, b *redshift.InMemoryBackend) {
				t.Helper()
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=resize-cluster")
				b.AddActiveResizeInternal("resize-cluster", &redshift.ResizeProgress{
					TargetNodeType:      "ra3.xlplus",
					TargetNumberOfNodes: 4,
					TargetClusterType:   "multi-node",
					Status:              "IN_PROGRESS",
					ResizeType:          "ClassicResize",
					AllowCancelResize:   true,
				})
			},
			body:         "Action=CancelResize&Version=2012-12-01&ClusterIdentifier=resize-cluster",
			wantCode:     http.StatusOK,
			wantContains: []string{"CancelResizeResponse", "CANCELLED"},
		},
		{
			name:         "missing_cluster_identifier",
			body:         "Action=CancelResize&Version=2012-12-01",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"InvalidParameterValue"},
		},
		{
			name:         "cluster_not_found",
			body:         "Action=CancelResize&Version=2012-12-01&ClusterIdentifier=nonexistent",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ClusterNotFound"},
		},
		{
			name: "no_active_resize",
			setup: func(t *testing.T, h *redshift.Handler, _ *redshift.InMemoryBackend) {
				t.Helper()
				postRedshiftForm(t, h, "Action=CreateCluster&Version=2012-12-01&ClusterIdentifier=no-resize-cluster")
			},
			body:         "Action=CancelResize&Version=2012-12-01&ClusterIdentifier=no-resize-cluster",
			wantCode:     http.StatusBadRequest,
			wantContains: []string{"ResizeNotFound"},
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

// ---- Backend unit tests for new operations ----

func TestRedshiftBackend_AcceptReservedNodeExchange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr        error
		setup          func(b *redshift.InMemoryBackend)
		name           string
		reservedNodeID string
		targetOffering string
		wantOfferingID string
	}{
		{
			name: "success",
			setup: func(b *redshift.InMemoryBackend) {
				b.AddReservedNodeInternal(&redshift.ReservedNode{
					ReservedNodeID:         "rn-test",
					ReservedNodeOfferingID: "old-offering",
					NodeType:               "dc2.large",
					NodeCount:              1,
					State:                  "active",
				})
			},
			reservedNodeID: "rn-test",
			targetOffering: "new-offering",
			wantOfferingID: "new-offering",
		},
		{
			name:           "empty_reserved_node_id",
			reservedNodeID: "",
			targetOffering: "new-offering",
			wantErr:        redshift.ErrInvalidParameter,
		},
		{
			name:           "empty_target_offering",
			reservedNodeID: "rn-test",
			targetOffering: "",
			wantErr:        redshift.ErrInvalidParameter,
		},
		{
			name:           "not_found",
			reservedNodeID: "nonexistent",
			targetOffering: "new-offering",
			wantErr:        redshift.ErrReservedNodeNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.setup != nil {
				tt.setup(b)
			}

			node, err := b.AcceptReservedNodeExchange(tt.reservedNodeID, tt.targetOffering)

			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantOfferingID, node.ReservedNodeOfferingID)
			assert.Equal(t, "active", node.State)
		})
	}
}

func TestRedshiftBackend_AddPartner(t *testing.T) {
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

func TestRedshiftBackend_AuthorizeEndpointAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		setup     func(b *redshift.InMemoryBackend)
		name      string
		clusterID string
		grantee   string
		vpcIDs    []string
	}{
		{
			name: "success_no_vpcs",
			setup: func(b *redshift.InMemoryBackend) {
				_, _ = b.CreateCluster("ea-cluster", "", "", "")
			},
			clusterID: "ea-cluster",
			grantee:   "111111111111",
			vpcIDs:    nil,
		},
		{
			name: "success_with_vpcs",
			setup: func(b *redshift.InMemoryBackend) {
				_, _ = b.CreateCluster("ea-cluster-vpcs", "", "", "")
			},
			clusterID: "ea-cluster-vpcs",
			grantee:   "222222222222",
			vpcIDs:    []string{"vpc-1", "vpc-2"},
		},
		{
			name:      "empty_cluster_id",
			clusterID: "",
			grantee:   "111111111111",
			wantErr:   redshift.ErrInvalidParameter,
		},
		{
			name:      "empty_grantee",
			clusterID: "ea-cluster",
			grantee:   "",
			wantErr:   redshift.ErrInvalidParameter,
		},
		{
			name:      "cluster_not_found",
			clusterID: "nonexistent",
			grantee:   "111111111111",
			wantErr:   redshift.ErrClusterNotFound,
		},
		{
			name: "duplicate",
			setup: func(b *redshift.InMemoryBackend) {
				_, _ = b.CreateCluster("dup-ea-cluster", "", "", "")
				_, _ = b.AuthorizeEndpointAccess("dup-ea-cluster", "333333333333", nil)
			},
			clusterID: "dup-ea-cluster",
			grantee:   "333333333333",
			wantErr:   redshift.ErrEndpointAuthAlreadyExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.setup != nil {
				tt.setup(b)
			}

			auth, err := b.AuthorizeEndpointAccess(tt.clusterID, tt.grantee, tt.vpcIDs)

			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.clusterID, auth.ClusterIdentifier)
			assert.Equal(t, tt.grantee, auth.Grantee)
			if len(tt.vpcIDs) == 0 {
				assert.True(t, auth.AllowedAllVPCs)
			} else {
				assert.False(t, auth.AllowedAllVPCs)
				assert.Equal(t, tt.vpcIDs, auth.AllowedVPCs)
			}
		})
	}
}

func TestRedshiftBackend_BatchDeleteAndModifySnapshots(t *testing.T) {
	t.Parallel()

	t.Run("batch_delete_all_found", func(t *testing.T) {
		t.Parallel()

		b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
		b.AddSnapshotInternal(
			&redshift.Snapshot{SnapshotIdentifier: "s1", ClusterIdentifier: "c1", Status: "available"},
		)
		b.AddSnapshotInternal(
			&redshift.Snapshot{SnapshotIdentifier: "s2", ClusterIdentifier: "c1", Status: "available"},
		)

		errs, deleted := b.BatchDeleteClusterSnapshots([]string{"s1", "s2"})
		assert.Empty(t, errs)
		assert.ElementsMatch(t, []string{"s1", "s2"}, deleted)
	})

	t.Run("batch_delete_partial_not_found", func(t *testing.T) {
		t.Parallel()

		b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
		b.AddSnapshotInternal(
			&redshift.Snapshot{SnapshotIdentifier: "s3", ClusterIdentifier: "c1", Status: "available"},
		)

		errs, deleted := b.BatchDeleteClusterSnapshots([]string{"s3", "missing"})
		assert.Len(t, errs, 1)
		assert.Equal(t, "missing", errs[0].SnapshotIdentifier)
		assert.Equal(t, []string{"s3"}, deleted)
	})

	t.Run("batch_modify_all_found", func(t *testing.T) {
		t.Parallel()

		b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
		b.AddSnapshotInternal(
			&redshift.Snapshot{SnapshotIdentifier: "m1", ClusterIdentifier: "c1", Status: "available"},
		)
		b.AddSnapshotInternal(
			&redshift.Snapshot{SnapshotIdentifier: "m2", ClusterIdentifier: "c1", Status: "available"},
		)

		errs, modified := b.BatchModifyClusterSnapshots([]string{"m1", "m2"}, 7, false)
		assert.Empty(t, errs)
		assert.ElementsMatch(t, []string{"m1", "m2"}, modified)
	})

	t.Run("batch_modify_partial_not_found", func(t *testing.T) {
		t.Parallel()

		b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
		b.AddSnapshotInternal(
			&redshift.Snapshot{SnapshotIdentifier: "m3", ClusterIdentifier: "c1", Status: "available"},
		)

		errs, modified := b.BatchModifyClusterSnapshots([]string{"m3", "missing"}, 14, true)
		assert.Len(t, errs, 1)
		assert.Equal(t, "missing", errs[0].SnapshotIdentifier)
		assert.Equal(t, []string{"m3"}, modified)
	})
}

func TestRedshiftBackend_CancelResize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		setup     func(b *redshift.InMemoryBackend)
		name      string
		clusterID string
	}{
		{
			name: "success",
			setup: func(b *redshift.InMemoryBackend) {
				_, _ = b.CreateCluster("cr-cluster", "", "", "")
				b.AddActiveResizeInternal("cr-cluster", &redshift.ResizeProgress{
					Status:            "IN_PROGRESS",
					AllowCancelResize: true,
				})
			},
			clusterID: "cr-cluster",
		},
		{
			name:      "empty_cluster_id",
			clusterID: "",
			wantErr:   redshift.ErrInvalidParameter,
		},
		{
			name:      "cluster_not_found",
			clusterID: "nonexistent",
			wantErr:   redshift.ErrClusterNotFound,
		},
		{
			name: "no_active_resize",
			setup: func(b *redshift.InMemoryBackend) {
				_, _ = b.CreateCluster("no-resize", "", "", "")
			},
			clusterID: "no-resize",
			wantErr:   redshift.ErrResizeNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			if tt.setup != nil {
				tt.setup(b)
			}

			result, err := b.CancelResize(tt.clusterID)

			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "CANCELLED", result.Status)
		})
	}
}

// ---- Persistence round-trip for new maps ----

func TestRedshiftBackend_SnapshotRestore_NewMaps(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")

	b.AddReservedNodeInternal(&redshift.ReservedNode{
		ReservedNodeID:         "rn-persist",
		ReservedNodeOfferingID: "offering-persist",
		NodeType:               "ra3.xlplus",
		NodeCount:              2,
		State:                  "active",
	})
	b.AddDataShareInternal(&redshift.DataShare{
		DataShareArn: "arn:aws:redshift:us-east-1:000000000000:datashare:ds-persist",
		ProducerArn:  "producer-arn",
	})
	b.AddSecurityGroupInternal(&redshift.ClusterSecurityGroup{
		ClusterSecurityGroupName: "sg-persist",
		Description:              "persist test",
	})
	b.AddSnapshotInternal(&redshift.Snapshot{
		SnapshotIdentifier: "snap-persist",
		ClusterIdentifier:  "c-persist",
		Status:             "available",
	})

	_, err := b.CreateCluster("resize-persist-cluster", "", "", "")
	require.NoError(t, err)
	b.AddActiveResizeInternal("resize-persist-cluster", &redshift.ResizeProgress{
		Status:            "IN_PROGRESS",
		AllowCancelResize: true,
	})

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	fresh := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(t.Context(), snap))

	// Verify reserved node round-trip
	_, err = fresh.AcceptReservedNodeExchange("rn-persist", "new-offering")
	require.NoError(t, err)

	// Verify data share round-trip
	_, err = fresh.AuthorizeDataShare("arn:aws:redshift:us-east-1:000000000000:datashare:ds-persist", "consumer-id")
	require.NoError(t, err)

	// Verify security group round-trip
	_, err = fresh.AuthorizeClusterSecurityGroupIngress("sg-persist", "192.168.1.0/24", "", "")
	require.NoError(t, err)

	// Verify snapshot round-trip
	_, err = fresh.AuthorizeSnapshotAccess("snap-persist", "111111111111")
	require.NoError(t, err)

	// Verify resize round-trip
	_, err = fresh.CancelResize("resize-persist-cluster")
	require.NoError(t, err)
}
