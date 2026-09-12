package opensearch_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	opensearchsdk "github.com/aws/aws-sdk-go-v2/service/opensearch"
	"github.com/aws/aws-sdk-go-v2/service/opensearch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

// newSlice16OpenSearchClient stands up a fresh backend/handler/client triple
// for gopherstack-n3zi typed slice 16.
func newSlice16OpenSearchClient(t *testing.T) (*opensearch.InMemoryBackend, *opensearchsdk.Client) {
	t.Helper()

	b := opensearch.NewInMemoryBackend("000000000000", "us-east-1")
	h := opensearch.NewHandler(b)

	return b, newTestOpenSearchClient(t, h)
}

// slice16CreateDomain creates a domain via the real client and returns its
// CreateDomainOutput.
func slice16CreateDomain(
	t *testing.T,
	client *opensearchsdk.Client,
	name string,
) *opensearchsdk.CreateDomainOutput {
	t.Helper()

	out, err := client.CreateDomain(t.Context(), &opensearchsdk.CreateDomainInput{
		DomainName: aws.String(name),
	})
	require.NoError(t, err)
	require.NotNil(t, out.DomainStatus)

	return out
}

// TestTypedSlice16OpenSearchRealClient drives every op the census still
// listed as uncovered before this pass (gopherstack-n3zi typed-client
// coverage slice 16).
func TestTypedSlice16OpenSearchRealClient(t *testing.T) {
	t.Parallel()

	t.Run("domains_family", func(t *testing.T) {
		t.Parallel()

		_, client := newSlice16OpenSearchClient(t)

		slice16CreateDomain(t, client, "domains-fam-1")

		listedNames, err := client.ListDomainNames(t.Context(), &opensearchsdk.ListDomainNamesInput{})
		require.NoError(t, err)
		require.NotEmpty(t, listedNames.DomainNames)

		described, err := client.DescribeDomains(t.Context(), &opensearchsdk.DescribeDomainsInput{
			DomainNames: []string{"domains-fam-1"},
		})
		require.NoError(t, err)
		require.Len(t, described.DomainStatusList, 1)
		assert.Equal(t, "domains-fam-1", aws.ToString(described.DomainStatusList[0].DomainName))

		_, err = client.StartServiceSoftwareUpdate(t.Context(), &opensearchsdk.StartServiceSoftwareUpdateInput{
			DomainName: aws.String("domains-fam-1"),
		})
		require.NoError(t, err)

		_, err = client.CancelServiceSoftwareUpdate(t.Context(), &opensearchsdk.CancelServiceSoftwareUpdateInput{
			DomainName: aws.String("domains-fam-1"),
		})
		require.NoError(t, err)

		_, err = client.RollbackServiceSoftwareUpdate(
			t.Context(),
			&opensearchsdk.RollbackServiceSoftwareUpdateInput{DomainName: aws.String("domains-fam-1")},
		)
		require.NoError(t, err)

		_, err = client.DeleteDomain(t.Context(), &opensearchsdk.DeleteDomainInput{
			DomainName: aws.String("domains-fam-1"),
		})
		require.NoError(t, err)
	})

	t.Run("domain_config_maintenance_family", func(t *testing.T) {
		t.Parallel()

		_, client := newSlice16OpenSearchClient(t)

		slice16CreateDomain(t, client, "maint-fam-1")

		_, err := client.CancelDomainConfigChange(t.Context(), &opensearchsdk.CancelDomainConfigChangeInput{
			DomainName: aws.String("maint-fam-1"),
			DryRun:     aws.Bool(true),
		})
		require.NoError(t, err)

		started, err := client.StartDomainMaintenance(t.Context(), &opensearchsdk.StartDomainMaintenanceInput{
			DomainName: aws.String("maint-fam-1"),
			Action:     types.MaintenanceTypeRebootNode,
		})
		require.NoError(t, err)
		maintenanceID := aws.ToString(started.MaintenanceId)
		require.NotEmpty(t, maintenanceID)

		gotStatus, err := client.GetDomainMaintenanceStatus(
			t.Context(),
			&opensearchsdk.GetDomainMaintenanceStatusInput{
				DomainName:    aws.String("maint-fam-1"),
				MaintenanceId: aws.String(maintenanceID),
			},
		)
		require.NoError(t, err)
		assert.Equal(t, types.MaintenanceTypeRebootNode, gotStatus.Action)
	})

	t.Run("vpc_endpoint_family", func(t *testing.T) {
		t.Parallel()

		_, client := newSlice16OpenSearchClient(t)

		domOut := slice16CreateDomain(t, client, "vpc-fam-1")

		authorized, err := client.AuthorizeVpcEndpointAccess(
			t.Context(),
			&opensearchsdk.AuthorizeVpcEndpointAccessInput{
				DomainName: aws.String("vpc-fam-1"),
				Account:    aws.String("111111111111"),
			},
		)
		require.NoError(t, err)
		require.NotNil(t, authorized.AuthorizedPrincipal)
		assert.Equal(t, "111111111111", aws.ToString(authorized.AuthorizedPrincipal.Principal))

		_, err = client.RevokeVpcEndpointAccess(t.Context(), &opensearchsdk.RevokeVpcEndpointAccessInput{
			DomainName: aws.String("vpc-fam-1"),
			Account:    aws.String("111111111111"),
		})
		require.NoError(t, err)

		created, err := client.CreateVpcEndpoint(t.Context(), &opensearchsdk.CreateVpcEndpointInput{
			DomainArn: domOut.DomainStatus.ARN,
			VpcOptions: &types.VPCOptions{
				SubnetIds: []string{"subnet-0123456789abcdef0"},
			},
		})
		require.NoError(t, err)
		vpcEndpointID := aws.ToString(created.VpcEndpoint.VpcEndpointId)
		require.NotEmpty(t, vpcEndpointID)

		described, err := client.DescribeVpcEndpoints(t.Context(), &opensearchsdk.DescribeVpcEndpointsInput{
			VpcEndpointIds: []string{vpcEndpointID},
		})
		require.NoError(t, err)
		require.Len(t, described.VpcEndpoints, 1)
		assert.Equal(t, vpcEndpointID, aws.ToString(described.VpcEndpoints[0].VpcEndpointId))

		updated, err := client.UpdateVpcEndpoint(t.Context(), &opensearchsdk.UpdateVpcEndpointInput{
			VpcEndpointId: aws.String(vpcEndpointID),
			VpcOptions: &types.VPCOptions{
				SubnetIds: []string{"subnet-0123456789abcdef1"},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, updated.VpcEndpoint)

		_, err = client.DeleteVpcEndpoint(t.Context(), &opensearchsdk.DeleteVpcEndpointInput{
			VpcEndpointId: aws.String(vpcEndpointID),
		})
		require.NoError(t, err)
	})

	t.Run("inbound_outbound_connection_family", func(t *testing.T) {
		t.Parallel()

		_, client := newSlice16OpenSearchClient(t)

		outbound, err := client.CreateOutboundConnection(
			t.Context(),
			&opensearchsdk.CreateOutboundConnectionInput{
				ConnectionAlias: aws.String("conn-fam-alias"),
				LocalDomainInfo: &types.DomainInformationContainer{
					AWSDomainInformation: &types.AWSDomainInformation{DomainName: aws.String("local-dom")},
				},
				RemoteDomainInfo: &types.DomainInformationContainer{
					AWSDomainInformation: &types.AWSDomainInformation{DomainName: aws.String("remote-dom")},
				},
			},
		)
		require.NoError(t, err)
		connectionID := aws.ToString(outbound.ConnectionId)
		require.NotEmpty(t, connectionID)

		accepted, err := client.AcceptInboundConnection(t.Context(), &opensearchsdk.AcceptInboundConnectionInput{
			ConnectionId: aws.String(connectionID),
		})
		require.NoError(t, err)
		require.NotNil(t, accepted.Connection)
		assert.Equal(t, connectionID, aws.ToString(accepted.Connection.ConnectionId))

		_, err = client.DeleteInboundConnection(t.Context(), &opensearchsdk.DeleteInboundConnectionInput{
			ConnectionId: aws.String(connectionID),
		})
		require.NoError(t, err)

		outbound2, err := client.CreateOutboundConnection(
			t.Context(),
			&opensearchsdk.CreateOutboundConnectionInput{
				ConnectionAlias: aws.String("conn-fam-alias-2"),
				LocalDomainInfo: &types.DomainInformationContainer{
					AWSDomainInformation: &types.AWSDomainInformation{DomainName: aws.String("local-dom-2")},
				},
				RemoteDomainInfo: &types.DomainInformationContainer{
					AWSDomainInformation: &types.AWSDomainInformation{DomainName: aws.String("remote-dom-2")},
				},
			},
		)
		require.NoError(t, err)
		connectionID2 := aws.ToString(outbound2.ConnectionId)

		rejected, err := client.RejectInboundConnection(t.Context(), &opensearchsdk.RejectInboundConnectionInput{
			ConnectionId: aws.String(connectionID2),
		})
		require.NoError(t, err)
		require.NotNil(t, rejected.Connection)

		_, err = client.DeleteOutboundConnection(t.Context(), &opensearchsdk.DeleteOutboundConnectionInput{
			ConnectionId: aws.String(connectionID2),
		})
		require.NoError(t, err)
	})

	t.Run("package_family", func(t *testing.T) {
		t.Parallel()

		_, client := newSlice16OpenSearchClient(t)

		slice16CreateDomain(t, client, "pkg-fam-domain")

		created, err := client.CreatePackage(t.Context(), &opensearchsdk.CreatePackageInput{
			PackageName: aws.String("pkg-fam-1"),
			PackageType: types.PackageTypeTxtDictionary,
			PackageSource: &types.PackageSource{
				S3BucketName: aws.String("pkg-bucket"),
				S3Key:        aws.String("pkg-key-1"),
			},
		})
		require.NoError(t, err)
		packageID := aws.ToString(created.PackageDetails.PackageID)
		require.NotEmpty(t, packageID)

		associated, err := client.AssociatePackage(t.Context(), &opensearchsdk.AssociatePackageInput{
			DomainName: aws.String("pkg-fam-domain"),
			PackageID:  aws.String(packageID),
		})
		require.NoError(t, err)
		require.NotNil(t, associated.DomainPackageDetails)

		listedForDomain, err := client.ListPackagesForDomain(t.Context(), &opensearchsdk.ListPackagesForDomainInput{
			DomainName: aws.String("pkg-fam-domain"),
		})
		require.NoError(t, err)
		require.Len(t, listedForDomain.DomainPackageDetailsList, 1)

		listedForPackage, err := client.ListDomainsForPackage(t.Context(), &opensearchsdk.ListDomainsForPackageInput{
			PackageID: aws.String(packageID),
		})
		require.NoError(t, err)
		require.Len(t, listedForPackage.DomainPackageDetailsList, 1)

		history, err := client.GetPackageVersionHistory(t.Context(), &opensearchsdk.GetPackageVersionHistoryInput{
			PackageID: aws.String(packageID),
		})
		require.NoError(t, err)
		assert.Equal(t, packageID, aws.ToString(history.PackageID))

		updated, err := client.UpdatePackage(t.Context(), &opensearchsdk.UpdatePackageInput{
			PackageID: aws.String(packageID),
			PackageSource: &types.PackageSource{
				S3BucketName: aws.String("pkg-bucket"),
				S3Key:        aws.String("pkg-key-2"),
			},
		})
		require.NoError(t, err)
		require.NotNil(t, updated.PackageDetails)

		dissociated, err := client.DissociatePackage(t.Context(), &opensearchsdk.DissociatePackageInput{
			DomainName: aws.String("pkg-fam-domain"),
			PackageID:  aws.String(packageID),
		})
		require.NoError(t, err)
		require.NotNil(t, dissociated.DomainPackageDetails)

		_, err = client.DeletePackage(t.Context(), &opensearchsdk.DeletePackageInput{
			PackageID: aws.String(packageID),
		})
		require.NoError(t, err)

		created2, err := client.CreatePackage(t.Context(), &opensearchsdk.CreatePackageInput{
			PackageName: aws.String("pkg-fam-2"),
			PackageType: types.PackageTypeTxtDictionary,
			PackageSource: &types.PackageSource{
				S3BucketName: aws.String("pkg-bucket"),
				S3Key:        aws.String("pkg-key-3"),
			},
		})
		require.NoError(t, err)
		packageID2 := aws.ToString(created2.PackageDetails.PackageID)

		associatedMulti, err := client.AssociatePackages(t.Context(), &opensearchsdk.AssociatePackagesInput{
			DomainName: aws.String("pkg-fam-domain"),
			PackageList: []types.PackageDetailsForAssociation{
				{PackageID: aws.String(packageID2)},
			},
		})
		require.NoError(t, err)
		require.Len(t, associatedMulti.DomainPackageDetailsList, 1)

		dissociatedMulti, err := client.DissociatePackages(t.Context(), &opensearchsdk.DissociatePackagesInput{
			DomainName:  aws.String("pkg-fam-domain"),
			PackageList: []string{packageID2},
		})
		require.NoError(t, err)
		require.Len(t, dissociatedMulti.DomainPackageDetailsList, 1)
	})

	t.Run("data_source_family", func(t *testing.T) {
		t.Parallel()

		_, client := newSlice16OpenSearchClient(t)

		slice16CreateDomain(t, client, "ds-fam-domain")

		dsType := &types.DataSourceTypeMemberS3GlueDataCatalog{
			Value: types.S3GlueDataCatalog{RoleArn: aws.String("arn:aws:iam::000000000000:role/glue")},
		}

		_, err := client.AddDataSource(t.Context(), &opensearchsdk.AddDataSourceInput{
			DomainName:     aws.String("ds-fam-domain"),
			Name:           aws.String("ds-1"),
			DataSourceType: dsType,
		})
		require.NoError(t, err)

		listed, err := client.ListDataSources(t.Context(), &opensearchsdk.ListDataSourcesInput{
			DomainName: aws.String("ds-fam-domain"),
		})
		require.NoError(t, err)
		require.NotEmpty(t, listed.DataSources)

		got, err := client.GetDataSource(t.Context(), &opensearchsdk.GetDataSourceInput{
			DomainName: aws.String("ds-fam-domain"),
			Name:       aws.String("ds-1"),
		})
		require.NoError(t, err)
		assert.Equal(t, "ds-1", aws.ToString(got.Name))

		_, err = client.UpdateDataSource(t.Context(), &opensearchsdk.UpdateDataSourceInput{
			DomainName:     aws.String("ds-fam-domain"),
			Name:           aws.String("ds-1"),
			DataSourceType: dsType,
		})
		require.NoError(t, err)

		_, err = client.DeleteDataSource(t.Context(), &opensearchsdk.DeleteDataSourceInput{
			DomainName: aws.String("ds-fam-domain"),
			Name:       aws.String("ds-1"),
		})
		require.NoError(t, err)

		_, err = client.GetDataSource(t.Context(), &opensearchsdk.GetDataSourceInput{
			DomainName: aws.String("ds-fam-domain"),
			Name:       aws.String("ds-1"),
		})
		require.Error(t, err)
	})

	t.Run("direct_query_data_source_family", func(t *testing.T) {
		t.Parallel()

		_, client := newSlice16OpenSearchClient(t)

		dqType := &types.DirectQueryDataSourceTypeMemberCloudWatchLog{
			Value: types.CloudWatchDirectQueryDataSource{
				RoleArn: aws.String("arn:aws:iam::000000000000:role/cwl"),
			},
		}

		_, err := client.AddDirectQueryDataSource(t.Context(), &opensearchsdk.AddDirectQueryDataSourceInput{
			DataSourceName: aws.String("dq-1"),
			DataSourceType: dqType,
		})
		require.NoError(t, err)

		listed, err := client.ListDirectQueryDataSources(
			t.Context(),
			&opensearchsdk.ListDirectQueryDataSourcesInput{},
		)
		require.NoError(t, err)
		require.NotEmpty(t, listed.DirectQueryDataSources)

		got, err := client.GetDirectQueryDataSource(t.Context(), &opensearchsdk.GetDirectQueryDataSourceInput{
			DataSourceName: aws.String("dq-1"),
		})
		require.NoError(t, err)
		assert.Equal(t, "dq-1", aws.ToString(got.DataSourceName))

		_, err = client.UpdateDirectQueryDataSource(t.Context(), &opensearchsdk.UpdateDirectQueryDataSourceInput{
			DataSourceName: aws.String("dq-1"),
			DataSourceType: dqType,
		})
		require.NoError(t, err)

		_, err = client.DeleteDirectQueryDataSource(t.Context(), &opensearchsdk.DeleteDirectQueryDataSourceInput{
			DataSourceName: aws.String("dq-1"),
		})
		require.NoError(t, err)

		_, err = client.GetDirectQueryDataSource(t.Context(), &opensearchsdk.GetDirectQueryDataSourceInput{
			DataSourceName: aws.String("dq-1"),
		})
		require.Error(t, err)
	})

	t.Run("data_source_attachment_family", func(t *testing.T) {
		t.Parallel()

		_, client := newSlice16OpenSearchClient(t)

		domOut := slice16CreateDomain(t, client, "dsattach-fam-domain")
		domainARN := aws.ToString(domOut.DomainStatus.ARN)

		app, err := client.CreateApplication(t.Context(), &opensearchsdk.CreateApplicationInput{
			Name: aws.String("dsattach-fam-app"),
		})
		require.NoError(t, err)
		appID := aws.ToString(app.Id)
		require.NotEmpty(t, appID)

		_, err = client.AttachDataSource(t.Context(), &opensearchsdk.AttachDataSourceInput{
			Id:            aws.String(appID),
			DataSourceArn: aws.String(domainARN),
		})
		require.NoError(t, err)

		described, err := client.DescribeDataSourceAttachment(
			t.Context(),
			&opensearchsdk.DescribeDataSourceAttachmentInput{
				Id:            aws.String(appID),
				DataSourceArn: aws.String(domainARN),
			},
		)
		require.NoError(t, err)
		assert.Equal(t, domainARN, aws.ToString(described.DataSourceArn))

		_, err = client.DetachDataSource(t.Context(), &opensearchsdk.DetachDataSourceInput{
			Id:            aws.String(appID),
			DataSourceArn: aws.String(domainARN),
		})
		require.NoError(t, err)

		_, err = client.DescribeDataSourceAttachment(
			t.Context(),
			&opensearchsdk.DescribeDataSourceAttachmentInput{
				Id:            aws.String(appID),
				DataSourceArn: aws.String(domainARN),
			},
		)
		require.Error(t, err)
	})

	t.Run("application_capability_family", func(t *testing.T) {
		t.Parallel()

		_, client := newSlice16OpenSearchClient(t)

		app, err := client.CreateApplication(t.Context(), &opensearchsdk.CreateApplicationInput{
			Name: aws.String("cap-fam-app"),
		})
		require.NoError(t, err)
		appID := aws.ToString(app.Id)
		require.NotEmpty(t, appID)

		_, err = client.RegisterCapability(t.Context(), &opensearchsdk.RegisterCapabilityInput{
			ApplicationId:  aws.String(appID),
			CapabilityName: aws.String("ai-config"),
			CapabilityConfig: &types.CapabilityBaseRequestConfigMemberAiConfig{
				Value: types.AIConfig{},
			},
		})
		require.NoError(t, err)

		got, err := client.GetCapability(t.Context(), &opensearchsdk.GetCapabilityInput{
			ApplicationId:  aws.String(appID),
			CapabilityName: aws.String("ai-config"),
		})
		require.NoError(t, err)
		assert.Equal(t, "ai-config", aws.ToString(got.CapabilityName))

		_, err = client.DeregisterCapability(t.Context(), &opensearchsdk.DeregisterCapabilityInput{
			ApplicationId:  aws.String(appID),
			CapabilityName: aws.String("ai-config"),
		})
		require.NoError(t, err)

		_, err = client.DeleteApplication(t.Context(), &opensearchsdk.DeleteApplicationInput{
			Id: aws.String(appID),
		})
		require.NoError(t, err)

		_, err = client.GetApplication(t.Context(), &opensearchsdk.GetApplicationInput{
			Id: aws.String(appID),
		})
		require.Error(t, err)
	})

	t.Run("default_application_setting_family", func(t *testing.T) {
		t.Parallel()

		_, client := newSlice16OpenSearchClient(t)

		app, err := client.CreateApplication(t.Context(), &opensearchsdk.CreateApplicationInput{
			Name: aws.String("default-app-setting-app"),
		})
		require.NoError(t, err)
		appARN := aws.ToString(app.Arn)
		require.NotEmpty(t, appARN)

		_, err = client.PutDefaultApplicationSetting(t.Context(), &opensearchsdk.PutDefaultApplicationSettingInput{
			ApplicationArn: aws.String(appARN),
			SetAsDefault:   aws.Bool(true),
		})
		require.NoError(t, err)

		got, err := client.GetDefaultApplicationSetting(
			t.Context(),
			&opensearchsdk.GetDefaultApplicationSettingInput{},
		)
		require.NoError(t, err)
		assert.Equal(t, appARN, aws.ToString(got.ApplicationArn))
	})

	t.Run("insight_family", func(t *testing.T) {
		t.Parallel()

		_, client := newSlice16OpenSearchClient(t)

		slice16CreateDomain(t, client, "insight-fam-domain")

		listed, err := client.ListInsights(t.Context(), &opensearchsdk.ListInsightsInput{
			Entity: &types.InsightEntity{
				Type:  types.InsightEntityTypeDomain,
				Value: aws.String("insight-fam-domain"),
			},
		})
		require.NoError(t, err)
		assert.Empty(t, listed.Insights)

		_, err = client.DescribeInsightDetails(t.Context(), &opensearchsdk.DescribeInsightDetailsInput{
			Entity: &types.InsightEntity{
				Type:  types.InsightEntityTypeDomain,
				Value: aws.String("insight-fam-domain"),
			},
			InsightId: aws.String("insight-does-not-exist"),
		})
		require.Error(t, err)

		_, err = client.InsightFeedback(t.Context(), &opensearchsdk.InsightFeedbackInput{
			Entity: &types.InsightFeedbackEntity{
				Type:  types.InsightFeedbackEntityTypeDomain,
				Value: aws.String("insight-fam-domain"),
			},
			InsightId: aws.String("insight-does-not-exist"),
		})
		require.Error(t, err)
	})

	t.Run("migration_family", func(t *testing.T) {
		t.Parallel()

		_, client := newSlice16OpenSearchClient(t)

		domOut := slice16CreateDomain(t, client, "migration-fam-domain")
		domainARN := aws.ToString(domOut.DomainStatus.ARN)

		app, err := client.CreateApplication(t.Context(), &opensearchsdk.CreateApplicationInput{
			Name: aws.String("migration-fam-app"),
		})
		require.NoError(t, err)
		appID := aws.ToString(app.Id)
		require.NotEmpty(t, appID)

		started, err := client.StartMigration(t.Context(), &opensearchsdk.StartMigrationInput{
			ApplicationId: aws.String(appID),
			MigrationOptions: &types.MigrationOptions{
				Source: &types.MigrationSource{DatasourceArn: aws.String(domainARN)},
				Workspace: &types.MigrationWorkspace{
					CreateWorkspace: aws.Bool(true),
					Name:            aws.String("migration-fam-workspace"),
				},
			},
		})
		require.NoError(t, err)
		migrationID := aws.ToString(started.MigrationId)
		require.NotEmpty(t, migrationID)

		got, err := client.GetMigration(t.Context(), &opensearchsdk.GetMigrationInput{
			MigrationId: aws.String(migrationID),
		})
		require.NoError(t, err)
		assert.Equal(t, migrationID, aws.ToString(got.MigrationId))
	})

	t.Run("reserved_instance_family", func(t *testing.T) {
		t.Parallel()

		_, client := newSlice16OpenSearchClient(t)

		offerings, err := client.DescribeReservedInstanceOfferings(
			t.Context(),
			&opensearchsdk.DescribeReservedInstanceOfferingsInput{},
		)
		require.NoError(t, err)
		require.NotEmpty(t, offerings.ReservedInstanceOfferings)

		purchased, err := client.PurchaseReservedInstanceOffering(
			t.Context(),
			&opensearchsdk.PurchaseReservedInstanceOfferingInput{
				ReservedInstanceOfferingId: aws.String("ri-offering-1"),
				ReservationName:            aws.String("ri-fam-reservation"),
				InstanceCount:              aws.Int32(1),
			},
		)
		require.NoError(t, err)
		reservationID := aws.ToString(purchased.ReservedInstanceId)
		require.NotEmpty(t, reservationID)

		described, err := client.DescribeReservedInstances(t.Context(), &opensearchsdk.DescribeReservedInstancesInput{
			ReservedInstanceId: aws.String(reservationID),
		})
		require.NoError(t, err)
		require.Len(t, described.ReservedInstances, 1)
		assert.Equal(t, "ri-fam-reservation", aws.ToString(described.ReservedInstances[0].ReservationName))
	})

	t.Run("scheduled_action_family", func(t *testing.T) {
		t.Parallel()

		backend, client := newSlice16OpenSearchClient(t)

		slice16CreateDomain(t, client, "sched-fam-domain")

		opensearch.AddScheduledActionInternal(backend, "sched-fam-domain", &opensearch.ScheduledAction{
			ID:   "action-1",
			Type: "SERVICE_SOFTWARE_UPDATE",
		})

		listed, err := client.ListScheduledActions(t.Context(), &opensearchsdk.ListScheduledActionsInput{
			DomainName: aws.String("sched-fam-domain"),
		})
		require.NoError(t, err)
		require.Len(t, listed.ScheduledActions, 1)

		updated, err := client.UpdateScheduledAction(t.Context(), &opensearchsdk.UpdateScheduledActionInput{
			DomainName: aws.String("sched-fam-domain"),
			ActionID:   aws.String("action-1"),
			ActionType: types.ActionTypeServiceSoftwareUpdate,
			ScheduleAt: types.ScheduleAtNow,
		})
		require.NoError(t, err)
		require.NotNil(t, updated.ScheduledAction)
		assert.Equal(t, "action-1", aws.ToString(updated.ScheduledAction.Id))
	})
}
