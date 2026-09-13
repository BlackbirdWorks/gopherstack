package dms_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dmssdk "github.com/aws/aws-sdk-go-v2/service/databasemigrationservice"
	"github.com/aws/aws-sdk-go-v2/service/databasemigrationservice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRealClient_ReplicationAndMigrationManagement drives 57 of dms's 86 typed-coverage-blind ops
// (gopherstack-n3zi) through the real aws-sdk-go-v2 databasemigrationservice
// client, one subtest per named priority family, asserting decoded values.
func TestRealClient_ReplicationAndMigrationManagement(t *testing.T) {
	t.Parallel()
	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "tags", run: func(t *testing.T) {
			t.Helper()

			h := newTestDMSHandler()
			client := newTestDMSClient(t, h)

			ri, err := client.CreateReplicationInstance(t.Context(), &dmssdk.CreateReplicationInstanceInput{
				ReplicationInstanceIdentifier: aws.String("s7-tags-ri"),
				ReplicationInstanceClass:      aws.String("dms.t3.micro"),
			})
			require.NoError(t, err)
			riArn := aws.ToString(ri.ReplicationInstance.ReplicationInstanceArn)

			_, err = client.AddTagsToResource(t.Context(), &dmssdk.AddTagsToResourceInput{
				ResourceArn: aws.String(riArn),
				Tags:        []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
			})
			require.NoError(t, err)

			listOut, err := client.ListTagsForResource(t.Context(), &dmssdk.ListTagsForResourceInput{
				ResourceArn: aws.String(riArn),
			})
			require.NoError(t, err)
			require.Len(t, listOut.TagList, 1)
			assert.Equal(t, "prod", aws.ToString(listOut.TagList[0].Value))

			_, err = client.RemoveTagsFromResource(t.Context(), &dmssdk.RemoveTagsFromResourceInput{
				ResourceArn: aws.String(riArn),
				TagKeys:     []string{"env"},
			})
			require.NoError(t, err)

			listOut, err = client.ListTagsForResource(t.Context(), &dmssdk.ListTagsForResourceInput{
				ResourceArn: aws.String(riArn),
			})
			require.NoError(t, err)
			assert.Empty(t, listOut.TagList)
		}},
		{name: "replication_instance_and_subnet_group", run: func(t *testing.T) {
			t.Helper()

			h := newTestDMSHandler()
			client := newTestDMSClient(t, h)

			sgOut, err := client.CreateReplicationSubnetGroup(t.Context(), &dmssdk.CreateReplicationSubnetGroupInput{
				ReplicationSubnetGroupIdentifier:  aws.String("s7-sg"),
				ReplicationSubnetGroupDescription: aws.String("initial"),
				SubnetIds:                         []string{"subnet-1", "subnet-2"},
			})
			require.NoError(t, err)

			modOut, err := client.ModifyReplicationSubnetGroup(t.Context(), &dmssdk.ModifyReplicationSubnetGroupInput{
				ReplicationSubnetGroupIdentifier:  sgOut.ReplicationSubnetGroup.ReplicationSubnetGroupIdentifier,
				ReplicationSubnetGroupDescription: aws.String("updated"),
				SubnetIds:                         []string{"subnet-1", "subnet-2", "subnet-3"},
			})
			require.NoError(t, err)
			assert.Equal(t, "updated", aws.ToString(modOut.ReplicationSubnetGroup.ReplicationSubnetGroupDescription))
			assert.Len(t, modOut.ReplicationSubnetGroup.Subnets, 3)

			riOut, err := client.CreateReplicationInstance(t.Context(), &dmssdk.CreateReplicationInstanceInput{
				ReplicationInstanceIdentifier: aws.String("s7-reboot-ri"),
				ReplicationInstanceClass:      aws.String("dms.t3.micro"),
			})
			require.NoError(t, err)
			riArn := aws.ToString(riOut.ReplicationInstance.ReplicationInstanceArn)

			rebootOut, err := client.RebootReplicationInstance(t.Context(), &dmssdk.RebootReplicationInstanceInput{
				ReplicationInstanceArn: aws.String(riArn),
			})
			require.NoError(t, err)
			assert.Equal(t, riArn, aws.ToString(rebootOut.ReplicationInstance.ReplicationInstanceArn))

			logsOut, err := client.DescribeReplicationInstanceTaskLogs(
				t.Context(),
				&dmssdk.DescribeReplicationInstanceTaskLogsInput{ReplicationInstanceArn: aws.String(riArn)},
			)
			require.NoError(t, err)
			assert.Equal(t, riArn, aws.ToString(logsOut.ReplicationInstanceArn))
			assert.NotNil(t, logsOut.ReplicationInstanceTaskLogs)

			_, err = client.DeleteReplicationSubnetGroup(t.Context(), &dmssdk.DeleteReplicationSubnetGroupInput{
				ReplicationSubnetGroupIdentifier: aws.String("s7-sg"),
			})
			require.NoError(t, err)
		}},
		{name: "endpoints_connections", run: func(t *testing.T) {
			t.Helper()

			h := newTestDMSHandler()
			client := newTestDMSClient(t, h)

			riOut, err := client.CreateReplicationInstance(t.Context(), &dmssdk.CreateReplicationInstanceInput{
				ReplicationInstanceIdentifier: aws.String("s7-conn-ri"),
				ReplicationInstanceClass:      aws.String("dms.t3.micro"),
			})
			require.NoError(t, err)
			riArn := aws.ToString(riOut.ReplicationInstance.ReplicationInstanceArn)

			epOut, err := client.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
				EndpointIdentifier: aws.String("s7-conn-ep"),
				EndpointType:       types.ReplicationEndpointTypeValueSource,
				EngineName:         aws.String("mysql"),
			})
			require.NoError(t, err)
			epArn := aws.ToString(epOut.Endpoint.EndpointArn)

			testOut, err := client.TestConnection(t.Context(), &dmssdk.TestConnectionInput{
				EndpointArn:            aws.String(epArn),
				ReplicationInstanceArn: aws.String(riArn),
			})
			require.NoError(t, err)
			require.NotNil(t, testOut.Connection)
			assert.Equal(t, epArn, aws.ToString(testOut.Connection.EndpointArn))
			assert.Equal(t, riArn, aws.ToString(testOut.Connection.ReplicationInstanceArn))

			descOut, err := client.DescribeConnections(t.Context(), &dmssdk.DescribeConnectionsInput{})
			require.NoError(t, err)
			require.Len(t, descOut.Connections, 1)
			assert.Equal(t, epArn, aws.ToString(descOut.Connections[0].EndpointArn))

			settingsOut, err := client.DescribeEndpointSettings(t.Context(), &dmssdk.DescribeEndpointSettingsInput{
				EngineName: aws.String("mysql"),
			})
			require.NoError(t, err)
			assert.NotNil(t, settingsOut.EndpointSettings)

			delOut, err := client.DeleteConnection(t.Context(), &dmssdk.DeleteConnectionInput{
				EndpointArn:            aws.String(epArn),
				ReplicationInstanceArn: aws.String(riArn),
			})
			require.NoError(t, err)
			require.NotNil(t, delOut.Connection)
			assert.Equal(t, epArn, aws.ToString(delOut.Connection.EndpointArn))

			descOut, err = client.DescribeConnections(t.Context(), &dmssdk.DescribeConnectionsInput{})
			require.NoError(t, err)
			assert.Empty(t, descOut.Connections)
		}},
		{name: "certificates", run: func(t *testing.T) {
			t.Helper()

			h := newTestDMSHandler()
			client := newTestDMSClient(t, h)

			importOut, err := client.ImportCertificate(t.Context(), &dmssdk.ImportCertificateInput{
				CertificateIdentifier: aws.String("s7-cert"),
				CertificatePem: aws.String(
					"-----BEGIN CERTIFICATE-----\nMIIBogIBADANBgkq\n-----END CERTIFICATE-----",
				),
			})
			require.NoError(t, err)
			require.NotNil(t, importOut.Certificate)
			certArn := aws.ToString(importOut.Certificate.CertificateArn)
			assert.Equal(t, "s7-cert", aws.ToString(importOut.Certificate.CertificateIdentifier))

			descOut, err := client.DescribeCertificates(t.Context(), &dmssdk.DescribeCertificatesInput{})
			require.NoError(t, err)
			require.Len(t, descOut.Certificates, 1)
			assert.Equal(t, certArn, aws.ToString(descOut.Certificates[0].CertificateArn))

			delOut, err := client.DeleteCertificate(t.Context(), &dmssdk.DeleteCertificateInput{
				CertificateArn: aws.String(certArn),
			})
			require.NoError(t, err)
			require.NotNil(t, delOut.Certificate)
			assert.Equal(t, certArn, aws.ToString(delOut.Certificate.CertificateArn))

			descOut, err = client.DescribeCertificates(t.Context(), &dmssdk.DescribeCertificatesInput{})
			require.NoError(t, err)
			assert.Empty(t, descOut.Certificates)
		}},
		{name: "event_subscriptions", run: func(t *testing.T) {
			t.Helper()

			h := newTestDMSHandler()
			client := newTestDMSClient(t, h)

			createOut, err := client.CreateEventSubscription(t.Context(), &dmssdk.CreateEventSubscriptionInput{
				SubscriptionName: aws.String("s7-sub"),
				SnsTopicArn:      aws.String("arn:aws:sns:us-east-1:123456789012:s7-topic"),
				SourceType:       aws.String("replication-instance"),
				Enabled:          aws.Bool(true),
			})
			require.NoError(t, err)
			require.NotNil(t, createOut.EventSubscription)
			assert.True(t, createOut.EventSubscription.Enabled)

			modOut, err := client.ModifyEventSubscription(t.Context(), &dmssdk.ModifyEventSubscriptionInput{
				SubscriptionName: aws.String("s7-sub"),
				Enabled:          aws.Bool(false),
			})
			require.NoError(t, err)
			require.NotNil(t, modOut.EventSubscription)
			assert.False(t, modOut.EventSubscription.Enabled)

			descOut, err := client.DescribeEventSubscriptions(t.Context(), &dmssdk.DescribeEventSubscriptionsInput{})
			require.NoError(t, err)
			require.Len(t, descOut.EventSubscriptionsList, 1)
			assert.Equal(t, "s7-sub", aws.ToString(descOut.EventSubscriptionsList[0].CustSubscriptionId))

			catOut, err := client.DescribeEventCategories(t.Context(), &dmssdk.DescribeEventCategoriesInput{
				SourceType: aws.String("replication-instance"),
			})
			require.NoError(t, err)
			assert.NotEmpty(t, catOut.EventCategoryGroupList)

			delOut, err := client.DeleteEventSubscription(t.Context(), &dmssdk.DeleteEventSubscriptionInput{
				SubscriptionName: aws.String("s7-sub"),
			})
			require.NoError(t, err)
			require.NotNil(t, delOut.EventSubscription)
			assert.Equal(t, "s7-sub", aws.ToString(delOut.EventSubscription.CustSubscriptionId))
		}},
		{name: "replication_configs_serverless", run: func(t *testing.T) {
			t.Helper()

			h := newTestDMSHandler()
			client := newTestDMSClient(t, h)

			srcOut, err := client.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
				EndpointIdentifier: aws.String("s7-rc-src"),
				EndpointType:       types.ReplicationEndpointTypeValueSource,
				EngineName:         aws.String("mysql"),
			})
			require.NoError(t, err)
			tgtOut, err := client.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
				EndpointIdentifier: aws.String("s7-rc-tgt"),
				EndpointType:       types.ReplicationEndpointTypeValueTarget,
				EngineName:         aws.String("s3"),
			})
			require.NoError(t, err)

			createOut, err := client.CreateReplicationConfig(t.Context(), &dmssdk.CreateReplicationConfigInput{
				ReplicationConfigIdentifier: aws.String("s7-rc"),
				ReplicationType:             types.MigrationTypeValueFullLoad,
				SourceEndpointArn:           srcOut.Endpoint.EndpointArn,
				TargetEndpointArn:           tgtOut.Endpoint.EndpointArn,
				TableMappings:               aws.String(`{"rules":[]}`),
				ComputeConfig: &types.ComputeConfig{
					MaxCapacityUnits: aws.Int32(4),
				},
			})
			require.NoError(t, err)
			configArn := aws.ToString(createOut.ReplicationConfig.ReplicationConfigArn)

			descOut, err := client.DescribeReplicationConfigs(t.Context(), &dmssdk.DescribeReplicationConfigsInput{})
			require.NoError(t, err)
			require.Len(t, descOut.ReplicationConfigs, 1)
			assert.Equal(t, configArn, aws.ToString(descOut.ReplicationConfigs[0].ReplicationConfigArn))

			modOut, err := client.ModifyReplicationConfig(t.Context(), &dmssdk.ModifyReplicationConfigInput{
				ReplicationConfigArn: aws.String(configArn),
				TableMappings:        aws.String(`{"rules":[]}`),
				ComputeConfig:        &types.ComputeConfig{MaxCapacityUnits: aws.Int32(8)},
			})
			require.NoError(t, err)
			require.NotNil(t, modOut.ReplicationConfig.ComputeConfig)
			assert.EqualValues(t, 8, aws.ToInt32(modOut.ReplicationConfig.ComputeConfig.MaxCapacityUnits))

			startOut, err := client.StartReplication(t.Context(), &dmssdk.StartReplicationInput{
				ReplicationConfigArn: aws.String(configArn),
				StartReplicationType: aws.String("start-replication"),
			})
			require.NoError(t, err)
			require.NotNil(t, startOut.Replication)
			assert.Equal(t, "running", aws.ToString(startOut.Replication.Status))

			replsOut, err := client.DescribeReplications(t.Context(), &dmssdk.DescribeReplicationsInput{})
			require.NoError(t, err)
			require.Len(t, replsOut.Replications, 1)
			assert.Equal(t, configArn, aws.ToString(replsOut.Replications[0].ReplicationConfigArn))

			reloadOut, err := client.ReloadReplicationTables(t.Context(), &dmssdk.ReloadReplicationTablesInput{
				ReplicationConfigArn: aws.String(configArn),
				TablesToReload: []types.TableToReload{
					{SchemaName: aws.String("public"), TableName: aws.String("orders")},
				},
			})
			require.NoError(t, err)
			assert.Equal(t, configArn, aws.ToString(reloadOut.ReplicationConfigArn))

			statsOut, err := client.DescribeReplicationTableStatistics(
				t.Context(),
				&dmssdk.DescribeReplicationTableStatisticsInput{ReplicationConfigArn: aws.String(configArn)},
			)
			require.NoError(t, err)
			assert.Equal(t, configArn, aws.ToString(statsOut.ReplicationConfigArn))
			assert.NotNil(t, statsOut.ReplicationTableStatistics)

			stopOut, err := client.StopReplication(t.Context(), &dmssdk.StopReplicationInput{
				ReplicationConfigArn: aws.String(configArn),
			})
			require.NoError(t, err)
			assert.Equal(t, "stopped", aws.ToString(stopOut.Replication.Status))

			delOut, err := client.DeleteReplicationConfig(t.Context(), &dmssdk.DeleteReplicationConfigInput{
				ReplicationConfigArn: aws.String(configArn),
			})
			require.NoError(t, err)
			assert.Equal(t, configArn, aws.ToString(delOut.ReplicationConfig.ReplicationConfigArn))
		}},
		{name: "migration_project_family", run: func(t *testing.T) {
			t.Helper()

			h := newTestDMSHandler()
			client := newTestDMSClient(t, h)

			ipOut, err := client.CreateInstanceProfile(t.Context(), &dmssdk.CreateInstanceProfileInput{
				InstanceProfileName: aws.String("s7-instprof"),
			})
			require.NoError(t, err)
			ipID := aws.ToString(ipOut.InstanceProfile.InstanceProfileName)

			ipModOut, err := client.ModifyInstanceProfile(t.Context(), &dmssdk.ModifyInstanceProfileInput{
				InstanceProfileIdentifier: aws.String(ipID),
				Description:               aws.String("updated profile"),
			})
			require.NoError(t, err)
			assert.Equal(t, "updated profile", aws.ToString(ipModOut.InstanceProfile.Description))

			srcDP, err := client.CreateDataProvider(t.Context(), &dmssdk.CreateDataProviderInput{
				DataProviderName: aws.String("s7-src-dp"),
				Engine:           aws.String("mysql"),
				Settings: &types.DataProviderSettingsMemberMySqlSettings{
					Value: types.MySqlDataProviderSettings{Port: aws.Int32(3306)},
				},
			})
			require.NoError(t, err)
			srcDPID := aws.ToString(srcDP.DataProvider.DataProviderName)

			dpModOut, err := client.ModifyDataProvider(t.Context(), &dmssdk.ModifyDataProviderInput{
				DataProviderIdentifier: aws.String(srcDPID),
				Description:            aws.String("src provider"),
			})
			require.NoError(t, err)
			assert.Equal(t, "src provider", aws.ToString(dpModOut.DataProvider.Description))

			tgtDP, err := client.CreateDataProvider(t.Context(), &dmssdk.CreateDataProviderInput{
				DataProviderName: aws.String("s7-tgt-dp"),
				Engine:           aws.String("postgres"),
				Settings: &types.DataProviderSettingsMemberPostgreSqlSettings{
					Value: types.PostgreSqlDataProviderSettings{Port: aws.Int32(5432)},
				},
			})
			require.NoError(t, err)
			tgtDPID := aws.ToString(tgtDP.DataProvider.DataProviderName)

			listDP, err := client.DescribeDataProviders(t.Context(), &dmssdk.DescribeDataProvidersInput{})
			require.NoError(t, err)
			assert.Len(t, listDP.DataProviders, 2)

			mpOut, err := client.CreateMigrationProject(t.Context(), &dmssdk.CreateMigrationProjectInput{
				MigrationProjectName:      aws.String("s7-mp"),
				InstanceProfileIdentifier: aws.String(ipID),
				SourceDataProviderDescriptors: []types.DataProviderDescriptorDefinition{
					{DataProviderIdentifier: aws.String(srcDPID)},
				},
				TargetDataProviderDescriptors: []types.DataProviderDescriptorDefinition{
					{DataProviderIdentifier: aws.String(tgtDPID)},
				},
			})
			require.NoError(t, err)
			mpID := aws.ToString(mpOut.MigrationProject.MigrationProjectName)

			mpModOut, err := client.ModifyMigrationProject(t.Context(), &dmssdk.ModifyMigrationProjectInput{
				MigrationProjectIdentifier: aws.String(mpID),
				Description:                aws.String("updated project"),
			})
			require.NoError(t, err)
			assert.Equal(t, "updated project", aws.ToString(mpModOut.MigrationProject.Description))

			delMP, err := client.DeleteMigrationProject(t.Context(), &dmssdk.DeleteMigrationProjectInput{
				MigrationProjectIdentifier: aws.String(mpID),
			})
			require.NoError(t, err)
			assert.Equal(t, mpID, aws.ToString(delMP.MigrationProject.MigrationProjectName))

			delDP, err := client.DeleteDataProvider(t.Context(), &dmssdk.DeleteDataProviderInput{
				DataProviderIdentifier: aws.String(srcDPID),
			})
			require.NoError(t, err)
			assert.Equal(t, srcDPID, aws.ToString(delDP.DataProvider.DataProviderName))

			_, err = client.DeleteDataProvider(t.Context(), &dmssdk.DeleteDataProviderInput{
				DataProviderIdentifier: aws.String(tgtDPID),
			})
			require.NoError(t, err)

			delIP, err := client.DeleteInstanceProfile(t.Context(), &dmssdk.DeleteInstanceProfileInput{
				InstanceProfileIdentifier: aws.String(ipID),
			})
			require.NoError(t, err)
			assert.Equal(t, ipID, aws.ToString(delIP.InstanceProfile.InstanceProfileName))
		}},
		{name: "data_migrations", run: func(t *testing.T) {
			t.Helper()

			h := newTestDMSHandler()
			client := newTestDMSClient(t, h)

			ipOut, err := client.CreateInstanceProfile(t.Context(), &dmssdk.CreateInstanceProfileInput{
				InstanceProfileName: aws.String("s7-dm-instprof"),
			})
			require.NoError(t, err)

			srcDP, err := client.CreateDataProvider(t.Context(), &dmssdk.CreateDataProviderInput{
				DataProviderName: aws.String("s7-dm-src-dp"),
				Engine:           aws.String("mysql"),
				Settings: &types.DataProviderSettingsMemberMySqlSettings{
					Value: types.MySqlDataProviderSettings{Port: aws.Int32(3306)},
				},
			})
			require.NoError(t, err)
			tgtDP, err := client.CreateDataProvider(t.Context(), &dmssdk.CreateDataProviderInput{
				DataProviderName: aws.String("s7-dm-tgt-dp"),
				Engine:           aws.String("postgres"),
				Settings: &types.DataProviderSettingsMemberPostgreSqlSettings{
					Value: types.PostgreSqlDataProviderSettings{Port: aws.Int32(5432)},
				},
			})
			require.NoError(t, err)

			mpOut, err := client.CreateMigrationProject(t.Context(), &dmssdk.CreateMigrationProjectInput{
				MigrationProjectName:      aws.String("s7-dm-mp"),
				InstanceProfileIdentifier: ipOut.InstanceProfile.InstanceProfileName,
				SourceDataProviderDescriptors: []types.DataProviderDescriptorDefinition{
					{DataProviderIdentifier: srcDP.DataProvider.DataProviderName},
				},
				TargetDataProviderDescriptors: []types.DataProviderDescriptorDefinition{
					{DataProviderIdentifier: tgtDP.DataProvider.DataProviderName},
				},
			})
			require.NoError(t, err)

			dmOut, err := client.CreateDataMigration(t.Context(), &dmssdk.CreateDataMigrationInput{
				DataMigrationName:          aws.String("s7-dm"),
				MigrationProjectIdentifier: mpOut.MigrationProject.MigrationProjectName,
				ServiceAccessRoleArn:       aws.String("arn:aws:iam::123456789012:role/dms-role"),
				DataMigrationType:          types.MigrationTypeValueFullLoad,
			})
			require.NoError(t, err)
			dmArn := aws.ToString(dmOut.DataMigration.DataMigrationArn)

			modOut, err := client.ModifyDataMigration(t.Context(), &dmssdk.ModifyDataMigrationInput{
				DataMigrationIdentifier: aws.String(dmArn),
				DataMigrationName:       aws.String("s7-dm-renamed"),
			})
			require.NoError(t, err)
			assert.Equal(t, "s7-dm-renamed", aws.ToString(modOut.DataMigration.DataMigrationName))

			startOut, err := client.StartDataMigration(t.Context(), &dmssdk.StartDataMigrationInput{
				DataMigrationIdentifier: aws.String(dmArn),
				StartType:               types.StartReplicationMigrationTypeValueStartReplication,
			})
			require.NoError(t, err)
			require.NotNil(t, startOut.DataMigration)

			stopOut, err := client.StopDataMigration(t.Context(), &dmssdk.StopDataMigrationInput{
				DataMigrationIdentifier: aws.String(dmArn),
			})
			require.NoError(t, err)
			require.NotNil(t, stopOut.DataMigration)

			delOut, err := client.DeleteDataMigration(t.Context(), &dmssdk.DeleteDataMigrationInput{
				DataMigrationIdentifier: aws.String(dmArn),
			})
			require.NoError(t, err)
			assert.Equal(t, "s7-dm-renamed", aws.ToString(delOut.DataMigration.DataMigrationName))
		}},
		{name: "assessments", run: func(t *testing.T) {
			t.Helper()

			h := newTestDMSHandler()
			client := newTestDMSClient(t, h)

			riOut, err := client.CreateReplicationInstance(t.Context(), &dmssdk.CreateReplicationInstanceInput{
				ReplicationInstanceIdentifier: aws.String("s7-assess-ri"),
				ReplicationInstanceClass:      aws.String("dms.t3.micro"),
			})
			require.NoError(t, err)
			srcOut, err := client.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
				EndpointIdentifier: aws.String("s7-assess-src"),
				EndpointType:       types.ReplicationEndpointTypeValueSource,
				EngineName:         aws.String("mysql"),
			})
			require.NoError(t, err)
			tgtOut, err := client.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
				EndpointIdentifier: aws.String("s7-assess-tgt"),
				EndpointType:       types.ReplicationEndpointTypeValueTarget,
				EngineName:         aws.String("s3"),
			})
			require.NoError(t, err)
			taskOut, err := client.CreateReplicationTask(t.Context(), &dmssdk.CreateReplicationTaskInput{
				ReplicationTaskIdentifier: aws.String("s7-assess-task"),
				SourceEndpointArn:         srcOut.Endpoint.EndpointArn,
				TargetEndpointArn:         tgtOut.Endpoint.EndpointArn,
				ReplicationInstanceArn:    riOut.ReplicationInstance.ReplicationInstanceArn,
				MigrationType:             types.MigrationTypeValueFullLoad,
				TableMappings:             aws.String(`{"rules":[]}`),
			})
			require.NoError(t, err)
			taskArn := aws.ToString(taskOut.ReplicationTask.ReplicationTaskArn)

			_, err = client.TestConnection(t.Context(), &dmssdk.TestConnectionInput{
				EndpointArn:            srcOut.Endpoint.EndpointArn,
				ReplicationInstanceArn: riOut.ReplicationInstance.ReplicationInstanceArn,
			})
			require.NoError(t, err)
			_, err = client.TestConnection(t.Context(), &dmssdk.TestConnectionInput{
				EndpointArn:            tgtOut.Endpoint.EndpointArn,
				ReplicationInstanceArn: riOut.ReplicationInstance.ReplicationInstanceArn,
			})
			require.NoError(t, err)

			_, err = client.StartReplicationTask(t.Context(), &dmssdk.StartReplicationTaskInput{
				ReplicationTaskArn:       aws.String(taskArn),
				StartReplicationTaskType: types.StartReplicationTaskTypeValueStartReplication,
			})
			require.NoError(t, err)
			_, err = client.StopReplicationTask(t.Context(), &dmssdk.StopReplicationTaskInput{
				ReplicationTaskArn: aws.String(taskArn),
			})
			require.NoError(t, err)

			applicableOut, err := client.DescribeApplicableIndividualAssessments(
				t.Context(),
				&dmssdk.DescribeApplicableIndividualAssessmentsInput{ReplicationTaskArn: aws.String(taskArn)},
			)
			require.NoError(t, err)
			assert.NotNil(t, applicableOut.IndividualAssessmentNames)

			startAssessOut, err := client.StartReplicationTaskAssessment(
				t.Context(),
				&dmssdk.StartReplicationTaskAssessmentInput{ReplicationTaskArn: aws.String(taskArn)},
			)
			require.NoError(t, err)
			assert.Equal(t, taskArn, aws.ToString(startAssessOut.ReplicationTask.ReplicationTaskArn))

			runOut, err := client.StartReplicationTaskAssessmentRun(
				t.Context(),
				&dmssdk.StartReplicationTaskAssessmentRunInput{
					AssessmentRunName:    aws.String("s7-run"),
					ReplicationTaskArn:   aws.String(taskArn),
					ResultLocationBucket: aws.String("s7-bucket"),
					ServiceAccessRoleArn: aws.String("arn:aws:iam::123456789012:role/dms-assess"),
				},
			)
			require.NoError(t, err)
			runArn := aws.ToString(runOut.ReplicationTaskAssessmentRun.ReplicationTaskAssessmentRunArn)
			assert.Equal(t, "s7-run", aws.ToString(runOut.ReplicationTaskAssessmentRun.AssessmentRunName))

			runsOut, err := client.DescribeReplicationTaskAssessmentRuns(
				t.Context(), &dmssdk.DescribeReplicationTaskAssessmentRunsInput{},
			)
			require.NoError(t, err)
			require.Len(t, runsOut.ReplicationTaskAssessmentRuns, 1)
			assert.Equal(
				t,
				runArn,
				aws.ToString(runsOut.ReplicationTaskAssessmentRuns[0].ReplicationTaskAssessmentRunArn),
			)

			resultsOut, err := client.DescribeReplicationTaskAssessmentResults(
				t.Context(),
				&dmssdk.DescribeReplicationTaskAssessmentResultsInput{ReplicationTaskArn: aws.String(taskArn)},
			)
			require.NoError(t, err)
			assert.NotNil(t, resultsOut.ReplicationTaskAssessmentResults)

			iaOut, err := client.DescribeReplicationTaskIndividualAssessments(
				t.Context(), &dmssdk.DescribeReplicationTaskIndividualAssessmentsInput{},
			)
			require.NoError(t, err)
			assert.NotNil(t, iaOut.ReplicationTaskIndividualAssessments)

			cancelOut, err := client.CancelReplicationTaskAssessmentRun(
				t.Context(),
				&dmssdk.CancelReplicationTaskAssessmentRunInput{ReplicationTaskAssessmentRunArn: aws.String(runArn)},
			)
			require.NoError(t, err)
			assert.Equal(
				t,
				runArn,
				aws.ToString(cancelOut.ReplicationTaskAssessmentRun.ReplicationTaskAssessmentRunArn),
			)

			delOut, err := client.DeleteReplicationTaskAssessmentRun(
				t.Context(),
				&dmssdk.DeleteReplicationTaskAssessmentRunInput{ReplicationTaskAssessmentRunArn: aws.String(runArn)},
			)
			require.NoError(t, err)
			assert.Equal(t, runArn, aws.ToString(delOut.ReplicationTaskAssessmentRun.ReplicationTaskAssessmentRunArn))
		}},
		{name: "fleet_advisor", run: func(t *testing.T) {
			t.Helper()

			h := newTestDMSHandler()
			client := newTestDMSClient(t, h)

			_, err := client.CreateFleetAdvisorCollector(t.Context(), &dmssdk.CreateFleetAdvisorCollectorInput{
				CollectorName:        aws.String("s7-collector"),
				ServiceAccessRoleArn: aws.String("arn:aws:iam::123456789012:role/fleet-advisor"),
				S3BucketName:         aws.String("s7-fleet-bucket"),
			})
			require.NoError(t, err)

			lsaRunOut, err := client.RunFleetAdvisorLsaAnalysis(t.Context(), &dmssdk.RunFleetAdvisorLsaAnalysisInput{})
			require.NoError(t, err)
			assert.NotEmpty(t, aws.ToString(lsaRunOut.LsaAnalysisId))
			assert.NotEmpty(t, aws.ToString(lsaRunOut.Status))

			lsaOut, err := client.DescribeFleetAdvisorLsaAnalysis(
				t.Context(), &dmssdk.DescribeFleetAdvisorLsaAnalysisInput{},
			)
			require.NoError(t, err)
			assert.NotNil(t, lsaOut.Analysis)

			schemasOut, err := client.DescribeFleetAdvisorSchemas(
				t.Context(),
				&dmssdk.DescribeFleetAdvisorSchemasInput{},
			)
			require.NoError(t, err)
			assert.NotNil(t, schemasOut.FleetAdvisorSchemas)

			summaryOut, err := client.DescribeFleetAdvisorSchemaObjectSummary(
				t.Context(), &dmssdk.DescribeFleetAdvisorSchemaObjectSummaryInput{},
			)
			require.NoError(t, err)
			assert.NotNil(t, summaryOut.FleetAdvisorSchemaObjects)

			dbsOut, err := client.DescribeFleetAdvisorDatabases(
				t.Context(),
				&dmssdk.DescribeFleetAdvisorDatabasesInput{},
			)
			require.NoError(t, err)
			require.Len(t, dbsOut.Databases, 2, "CreateFleetAdvisorCollector seeds two discovered databases")

			ids := make([]string, 0, len(dbsOut.Databases))
			for _, db := range dbsOut.Databases {
				ids = append(ids, aws.ToString(db.DatabaseId))
			}

			delOut, err := client.DeleteFleetAdvisorDatabases(
				t.Context(), &dmssdk.DeleteFleetAdvisorDatabasesInput{DatabaseIds: ids},
			)
			require.NoError(t, err)
			assert.ElementsMatch(t, ids, delOut.DatabaseIds)
		}},
		{name: "replication_task_lifecycle", run: func(t *testing.T) {
			t.Helper()

			h := newTestDMSHandler()
			client := newTestDMSClient(t, h)

			ri1, err := client.CreateReplicationInstance(t.Context(), &dmssdk.CreateReplicationInstanceInput{
				ReplicationInstanceIdentifier: aws.String("s7-task-ri1"),
				ReplicationInstanceClass:      aws.String("dms.t3.micro"),
			})
			require.NoError(t, err)
			ri2, err := client.CreateReplicationInstance(t.Context(), &dmssdk.CreateReplicationInstanceInput{
				ReplicationInstanceIdentifier: aws.String("s7-task-ri2"),
				ReplicationInstanceClass:      aws.String("dms.t3.micro"),
			})
			require.NoError(t, err)

			srcOut, err := client.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
				EndpointIdentifier: aws.String("s7-task-src"),
				EndpointType:       types.ReplicationEndpointTypeValueSource,
				EngineName:         aws.String("mysql"),
			})
			require.NoError(t, err)
			tgtOut, err := client.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
				EndpointIdentifier: aws.String("s7-task-tgt"),
				EndpointType:       types.ReplicationEndpointTypeValueTarget,
				EngineName:         aws.String("s3"),
			})
			require.NoError(t, err)

			taskOut, err := client.CreateReplicationTask(t.Context(), &dmssdk.CreateReplicationTaskInput{
				ReplicationTaskIdentifier: aws.String("s7-task"),
				SourceEndpointArn:         srcOut.Endpoint.EndpointArn,
				TargetEndpointArn:         tgtOut.Endpoint.EndpointArn,
				ReplicationInstanceArn:    ri1.ReplicationInstance.ReplicationInstanceArn,
				MigrationType:             types.MigrationTypeValueFullLoad,
				TableMappings:             aws.String(`{"rules":[]}`),
			})
			require.NoError(t, err)
			taskArn := aws.ToString(taskOut.ReplicationTask.ReplicationTaskArn)

			startOut, err := client.StartReplicationTask(t.Context(), &dmssdk.StartReplicationTaskInput{
				ReplicationTaskArn:       aws.String(taskArn),
				StartReplicationTaskType: types.StartReplicationTaskTypeValueStartReplication,
			})
			require.NoError(t, err)
			assert.Equal(t, "running", aws.ToString(startOut.ReplicationTask.Status))

			reloadOut, err := client.ReloadTables(t.Context(), &dmssdk.ReloadTablesInput{
				ReplicationTaskArn: aws.String(taskArn),
				TablesToReload: []types.TableToReload{
					{SchemaName: aws.String("public"), TableName: aws.String("orders")},
				},
			})
			require.NoError(t, err)
			assert.Equal(t, taskArn, aws.ToString(reloadOut.ReplicationTaskArn))

			stopOut, err := client.StopReplicationTask(t.Context(), &dmssdk.StopReplicationTaskInput{
				ReplicationTaskArn: aws.String(taskArn),
			})
			require.NoError(t, err)
			assert.Equal(t, "stopped", aws.ToString(stopOut.ReplicationTask.Status))

			moveOut, err := client.MoveReplicationTask(t.Context(), &dmssdk.MoveReplicationTaskInput{
				ReplicationTaskArn:           aws.String(taskArn),
				TargetReplicationInstanceArn: ri2.ReplicationInstance.ReplicationInstanceArn,
			})
			require.NoError(t, err)
			assert.Equal(t,
				aws.ToString(ri2.ReplicationInstance.ReplicationInstanceArn),
				aws.ToString(moveOut.ReplicationTask.ReplicationInstanceArn),
			)

			schemasOut, err := client.DescribeSchemas(t.Context(), &dmssdk.DescribeSchemasInput{
				EndpointArn: srcOut.Endpoint.EndpointArn,
			})
			require.NoError(t, err)
			assert.NotNil(t, schemasOut.Schemas)

			refreshOut, err := client.RefreshSchemas(t.Context(), &dmssdk.RefreshSchemasInput{
				EndpointArn:            srcOut.Endpoint.EndpointArn,
				ReplicationInstanceArn: ri1.ReplicationInstance.ReplicationInstanceArn,
			})
			require.NoError(t, err)
			require.NotNil(t, refreshOut.RefreshSchemasStatus)
			assert.Equal(
				t, aws.ToString(srcOut.Endpoint.EndpointArn), aws.ToString(refreshOut.RefreshSchemasStatus.EndpointArn),
			)

			statusOut, err := client.DescribeRefreshSchemasStatus(
				t.Context(),
				&dmssdk.DescribeRefreshSchemasStatusInput{
					EndpointArn: srcOut.Endpoint.EndpointArn,
				},
			)
			require.NoError(t, err)
			require.NotNil(t, statusOut.RefreshSchemasStatus)
			assert.Equal(
				t, aws.ToString(srcOut.Endpoint.EndpointArn), aws.ToString(statusOut.RefreshSchemasStatus.EndpointArn),
			)
		}},
		{name: "misc_account_and_maintenance", run: func(t *testing.T) {
			t.Helper()

			h := newTestDMSHandler()
			client := newTestDMSClient(t, h)

			acctOut, err := client.DescribeAccountAttributes(t.Context(), &dmssdk.DescribeAccountAttributesInput{})
			require.NoError(t, err)
			assert.NotEmpty(t, acctOut.AccountQuotas)

			engOut, err := client.DescribeEngineVersions(t.Context(), &dmssdk.DescribeEngineVersionsInput{})
			require.NoError(t, err)
			assert.NotEmpty(t, engOut.EngineVersions)

			limOut, err := client.DescribeRecommendationLimitations(
				t.Context(), &dmssdk.DescribeRecommendationLimitationsInput{},
			)
			require.NoError(t, err)
			assert.NotNil(t, limOut.Limitations)

			batchOut, err := client.BatchStartRecommendations(t.Context(), &dmssdk.BatchStartRecommendationsInput{})
			require.NoError(t, err)
			assert.NotNil(t, batchOut.ErrorEntries)

			ebOut, err := client.UpdateSubscriptionsToEventBridge(
				t.Context(), &dmssdk.UpdateSubscriptionsToEventBridgeInput{},
			)
			require.NoError(t, err)
			assert.NotNil(t, ebOut.Result)

			riOut, err := client.CreateReplicationInstance(t.Context(), &dmssdk.CreateReplicationInstanceInput{
				ReplicationInstanceIdentifier: aws.String("s7-maint-ri"),
				ReplicationInstanceClass:      aws.String("dms.t3.micro"),
			})
			require.NoError(t, err)
			riArn := aws.ToString(riOut.ReplicationInstance.ReplicationInstanceArn)

			applyOut, err := client.ApplyPendingMaintenanceAction(
				t.Context(),
				&dmssdk.ApplyPendingMaintenanceActionInput{
					ReplicationInstanceArn: aws.String(riArn),
					ApplyAction:            aws.String("os-upgrade"),
					OptInType:              aws.String("immediate"),
				},
			)
			require.NoError(t, err)
			require.NotNil(t, applyOut.ResourcePendingMaintenanceActions)
			assert.Equal(t, riArn, aws.ToString(applyOut.ResourcePendingMaintenanceActions.ResourceIdentifier))

			pendingOut, err := client.DescribePendingMaintenanceActions(
				t.Context(), &dmssdk.DescribePendingMaintenanceActionsInput{},
			)
			require.NoError(t, err)
			assert.NotNil(t, pendingOut.PendingMaintenanceActions)
		}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
