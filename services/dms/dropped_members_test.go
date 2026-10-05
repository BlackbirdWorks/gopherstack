package dms_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	dmssdk "github.com/aws/aws-sdk-go-v2/service/databasemigrationservice"
	"github.com/aws/aws-sdk-go-v2/service/databasemigrationservice/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mysqlSettings(port int32, server string) *types.DataProviderSettingsMemberMySqlSettings {
	return &types.DataProviderSettingsMemberMySqlSettings{
		Value: types.MySqlDataProviderSettings{Port: aws.Int32(port), ServerName: aws.String(server)},
	}
}

func TestRealClient_ReplicationConfigSettings(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T, c *dmssdk.Client)
		name string
	}{
		{name: "create_and_modify_settings_and_rename", run: func(t *testing.T, c *dmssdk.Client) {
			t.Helper()

			created, err := c.CreateReplicationConfig(t.Context(), &dmssdk.CreateReplicationConfigInput{
				ReplicationConfigIdentifier: aws.String("rc-a"),
				ReplicationType:             types.MigrationTypeValueFullLoad,
				SourceEndpointArn:           aws.String("arn:aws:dms:us-east-1:000000000000:endpoint:s"),
				TargetEndpointArn:           aws.String("arn:aws:dms:us-east-1:000000000000:endpoint:t"),
				ComputeConfig:               &types.ComputeConfig{},
				TableMappings:               aws.String(`{"rules":[]}`),
				ReplicationSettings:         aws.String(`{"Logging":{"EnableLogging":true}}`),
				SupplementalSettings:        aws.String(`{"a":1}`),
				ResourceIdentifier:          aws.String("rc-suffix"),
			})
			require.NoError(t, err)
			assert.JSONEq(t, `{"Logging":{"EnableLogging":true}}`,
				aws.ToString(created.ReplicationConfig.ReplicationSettings))
			assert.Equal(t, `{"a":1}`, aws.ToString(created.ReplicationConfig.SupplementalSettings))
			assert.Contains(t, aws.ToString(created.ReplicationConfig.ReplicationConfigArn), "rc-suffix")

			mod, err := c.ModifyReplicationConfig(t.Context(), &dmssdk.ModifyReplicationConfigInput{
				ReplicationConfigArn:        created.ReplicationConfig.ReplicationConfigArn,
				SupplementalSettings:        aws.String(`{"a":2}`),
				ReplicationConfigIdentifier: aws.String("rc-b"),
			})
			require.NoError(t, err)
			assert.Equal(t, `{"a":2}`, aws.ToString(mod.ReplicationConfig.SupplementalSettings))
			assert.JSONEq(t, `{"Logging":{"EnableLogging":true}}`,
				aws.ToString(mod.ReplicationConfig.ReplicationSettings), "omitted member keeps its value")
			assert.Equal(t, "rc-b", aws.ToString(mod.ReplicationConfig.ReplicationConfigIdentifier))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t, newTestDMSClient(t, newTestDMSHandler()))
		})
	}
}

func TestRealClient_MigrationProjectMembers(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T, c *dmssdk.Client)
		name string
	}{
		{name: "create_echoes_attributes_and_modify_is_partial", run: func(t *testing.T, c *dmssdk.Client) {
			t.Helper()

			seedMigrationProjectViaClient(t, c, "mp-base")
			_, err := c.CreateInstanceProfile(t.Context(), &dmssdk.CreateInstanceProfileInput{
				InstanceProfileName: aws.String("mp-ip2"),
			})
			require.NoError(t, err)

			descs := []types.DataProviderDescriptorDefinition{{DataProviderIdentifier: aws.String("mp-base-dp")}}
			created, err := c.CreateMigrationProject(t.Context(), &dmssdk.CreateMigrationProjectInput{
				MigrationProjectName:                  aws.String("mp-x"),
				Description:                           aws.String("keep me"),
				InstanceProfileIdentifier:             aws.String("mp-base-ip"),
				SourceDataProviderDescriptors:         descs,
				TargetDataProviderDescriptors:         descs,
				TransformationRules:                   aws.String(`{"rules":[]}`),
				SchemaConversionApplicationAttributes: &types.SCApplicationAttributes{S3BucketPath: aws.String("b/p")},
			})
			require.NoError(t, err)
			assert.JSONEq(t, `{"rules":[]}`, aws.ToString(created.MigrationProject.TransformationRules))
			assert.Equal(t, "b/p",
				aws.ToString(created.MigrationProject.SchemaConversionApplicationAttributes.S3BucketPath))

			mod, err := c.ModifyMigrationProject(t.Context(), &dmssdk.ModifyMigrationProjectInput{
				MigrationProjectIdentifier: aws.String("mp-x"),
				InstanceProfileIdentifier:  aws.String("mp-ip2"),
				MigrationProjectName:       aws.String("mp-y"),
				TransformationRules:        aws.String(`{"rules":[1]}`),
			})
			require.NoError(t, err)
			assert.Equal(t, "keep me", aws.ToString(mod.MigrationProject.Description))
			assert.Equal(t, "mp-ip2", aws.ToString(mod.MigrationProject.InstanceProfileName))
			assert.Equal(t, "mp-y", aws.ToString(mod.MigrationProject.MigrationProjectName))
			assert.JSONEq(t, `{"rules":[1]}`, aws.ToString(mod.MigrationProject.TransformationRules))
			assert.Equal(t, "b/p",
				aws.ToString(mod.MigrationProject.SchemaConversionApplicationAttributes.S3BucketPath))
			require.Len(t, mod.MigrationProject.SourceDataProviderDescriptors, 1)

			list, err := c.DescribeMigrationProjects(t.Context(), &dmssdk.DescribeMigrationProjectsInput{})
			require.NoError(t, err)
			assert.Len(t, list.MigrationProjects, 2)
		}},
		{name: "modify_rename_collision", run: func(t *testing.T, c *dmssdk.Client) {
			t.Helper()

			seedMigrationProjectViaClient(t, c, "mp-one")
			descs := []types.DataProviderDescriptorDefinition{{DataProviderIdentifier: aws.String("mp-one-dp")}}
			_, err := c.CreateMigrationProject(t.Context(), &dmssdk.CreateMigrationProjectInput{
				MigrationProjectName:          aws.String("mp-two"),
				InstanceProfileIdentifier:     aws.String("mp-one-ip"),
				SourceDataProviderDescriptors: descs,
				TargetDataProviderDescriptors: descs,
			})
			require.NoError(t, err)

			_, err = c.ModifyMigrationProject(t.Context(), &dmssdk.ModifyMigrationProjectInput{
				MigrationProjectIdentifier: aws.String("mp-two"),
				MigrationProjectName:       aws.String("mp-one"),
			})
			require.Error(t, err)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t, newTestDMSClient(t, newTestDMSHandler()))
		})
	}
}

func TestRealClient_InstanceProfileVpcSecurityGroupsAndRename(t *testing.T) {
	t.Parallel()

	c := newTestDMSClient(t, newTestDMSHandler())

	created, err := c.CreateInstanceProfile(t.Context(), &dmssdk.CreateInstanceProfileInput{
		InstanceProfileName: aws.String("ip-a"),
		VpcSecurityGroups:   []string{"sg-1"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"sg-1"}, created.InstanceProfile.VpcSecurityGroups)

	mod, err := c.ModifyInstanceProfile(t.Context(), &dmssdk.ModifyInstanceProfileInput{
		InstanceProfileIdentifier: aws.String("ip-a"),
		InstanceProfileName:       aws.String("ip-b"),
	})
	require.NoError(t, err)
	assert.Equal(t, "ip-b", aws.ToString(mod.InstanceProfile.InstanceProfileName))
	assert.Equal(t, []string{"sg-1"}, mod.InstanceProfile.VpcSecurityGroups)

	mod, err = c.ModifyInstanceProfile(t.Context(), &dmssdk.ModifyInstanceProfileInput{
		InstanceProfileIdentifier: aws.String("ip-b"),
		VpcSecurityGroups:         []string{"sg-2", "sg-3"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"sg-2", "sg-3"}, mod.InstanceProfile.VpcSecurityGroups)
}

func TestRealClient_DataProviderSettings(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T, c *dmssdk.Client)
		name string
	}{
		{name: "settings_virtual_merge_exact_and_rename", run: func(t *testing.T, c *dmssdk.Client) {
			t.Helper()

			created, err := c.CreateDataProvider(t.Context(), &dmssdk.CreateDataProviderInput{
				DataProviderName: aws.String("dp-a"),
				Engine:           aws.String("mysql"),
				Settings:         mysqlSettings(3306, "db.example"),
				Virtual:          aws.Bool(true),
			})
			require.NoError(t, err)
			got, ok := created.DataProvider.Settings.(*types.DataProviderSettingsMemberMySqlSettings)
			require.True(t, ok)
			assert.Equal(t, "db.example", aws.ToString(got.Value.ServerName))
			assert.True(t, aws.ToBool(created.DataProvider.Virtual))

			mod, err := c.ModifyDataProvider(t.Context(), &dmssdk.ModifyDataProviderInput{
				DataProviderIdentifier: aws.String("dp-a"),
				Settings: &types.DataProviderSettingsMemberMySqlSettings{
					Value: types.MySqlDataProviderSettings{Port: aws.Int32(3307)},
				},
				DataProviderName: aws.String("dp-b"),
			})
			require.NoError(t, err)
			got = mod.DataProvider.Settings.(*types.DataProviderSettingsMemberMySqlSettings)
			assert.Equal(t, int32(3307), aws.ToInt32(got.Value.Port))
			assert.Equal(t, "db.example", aws.ToString(got.Value.ServerName), "merge keeps other settings")
			assert.Equal(t, "dp-b", aws.ToString(mod.DataProvider.DataProviderName))

			mod, err = c.ModifyDataProvider(t.Context(), &dmssdk.ModifyDataProviderInput{
				DataProviderIdentifier: aws.String("dp-b"),
				Settings: &types.DataProviderSettingsMemberMySqlSettings{
					Value: types.MySqlDataProviderSettings{Port: aws.Int32(1)},
				},
				ExactSettings: aws.Bool(true),
			})
			require.NoError(t, err)
			got = mod.DataProvider.Settings.(*types.DataProviderSettingsMemberMySqlSettings)
			assert.Nil(t, got.Value.ServerName, "exact replaces all settings")
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t, newTestDMSClient(t, newTestDMSHandler()))
		})
	}
}

func TestRealClient_DataMigrationMembers(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T, c *dmssdk.Client)
		name string
	}{
		{name: "settings_create_modify_and_project_resolution", run: func(t *testing.T, c *dmssdk.Client) {
			t.Helper()

			seedMigrationProjectViaClient(t, c, "dm-proj")
			start := time.Unix(1700000000, 0)

			created, err := c.CreateDataMigration(t.Context(), &dmssdk.CreateDataMigrationInput{
				DataMigrationName:          aws.String("dm-a"),
				DataMigrationType:          types.MigrationTypeValueFullLoad,
				MigrationProjectIdentifier: aws.String("dm-proj"),
				ServiceAccessRoleArn:       aws.String("arn:aws:iam::000000000000:role/r"),
				SelectionRules:             aws.String(`{"rules":[]}`),
				SourceDataSettings: []types.SourceDataSetting{
					{CDCStartTime: aws.Time(start), SlotName: aws.String("slot")},
				},
				TargetDataSettings: []types.TargetDataSetting{
					{TablePreparationMode: types.TablePreparationModeTruncate},
				},
			})
			require.NoError(t, err)
			dm := created.DataMigration
			assert.Contains(t, aws.ToString(dm.MigrationProjectArn), ":migration-project:")
			assert.JSONEq(t, `{"rules":[]}`, aws.ToString(dm.DataMigrationSettings.SelectionRules))
			require.Len(t, dm.SourceDataSettings, 1)
			assert.Equal(t, "slot", aws.ToString(dm.SourceDataSettings[0].SlotName))
			assert.True(t, start.Equal(aws.ToTime(dm.SourceDataSettings[0].CDCStartTime)))
			require.Len(t, dm.TargetDataSettings, 1)
			assert.Equal(t, types.TablePreparationModeTruncate, dm.TargetDataSettings[0].TablePreparationMode)

			mod, err := c.ModifyDataMigration(t.Context(), &dmssdk.ModifyDataMigrationInput{
				DataMigrationIdentifier: aws.String("dm-a"),
				EnableCloudwatchLogs:    aws.Bool(true),
				TargetDataSettings: []types.TargetDataSetting{
					{TablePreparationMode: types.TablePreparationModeDoNothing},
				},
			})
			require.NoError(t, err)
			assert.True(t, aws.ToBool(mod.DataMigration.DataMigrationSettings.CloudwatchLogsEnabled))
			assert.JSONEq(t, `{"rules":[]}`, aws.ToString(mod.DataMigration.DataMigrationSettings.SelectionRules))
			assert.Equal(t, types.TablePreparationModeDoNothing,
				mod.DataMigration.TargetDataSettings[0].TablePreparationMode)
			require.Len(t, mod.DataMigration.SourceDataSettings, 1)
		}},
		{name: "unknown_project_is_not_found", run: func(t *testing.T, c *dmssdk.Client) {
			t.Helper()

			_, err := c.CreateDataMigration(t.Context(), &dmssdk.CreateDataMigrationInput{
				DataMigrationName:          aws.String("dm-x"),
				DataMigrationType:          types.MigrationTypeValueFullLoad,
				MigrationProjectIdentifier: aws.String("nope"),
				ServiceAccessRoleArn:       aws.String("arn:aws:iam::000000000000:role/r"),
			})
			require.Error(t, err)
		}},
		{name: "invalid_table_preparation_mode", run: func(t *testing.T, c *dmssdk.Client) {
			t.Helper()

			_, err := c.CreateDataMigration(t.Context(), &dmssdk.CreateDataMigrationInput{
				DataMigrationName:    aws.String("dm-y"),
				DataMigrationType:    types.MigrationTypeValueFullLoad,
				ServiceAccessRoleArn: aws.String("arn:aws:iam::000000000000:role/r"),
				TargetDataSettings:   []types.TargetDataSetting{{TablePreparationMode: "bogus"}},
			})
			require.Error(t, err)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t, newTestDMSClient(t, newTestDMSHandler()))
		})
	}
}

func TestRealClient_ReplicationInstanceKerberosAndRename(t *testing.T) {
	t.Parallel()

	c := newTestDMSClient(t, newTestDMSHandler())

	created, err := c.CreateReplicationInstance(t.Context(), &dmssdk.CreateReplicationInstanceInput{
		ReplicationInstanceIdentifier: aws.String("ri-a"),
		ReplicationInstanceClass:      aws.String("dms.t3.micro"),
		KerberosAuthenticationSettings: &types.KerberosAuthenticationSettings{
			KeyCacheSecretId: aws.String("secret-1"),
			Krb5FileContents: aws.String("[libdefaults]"),
		},
	})
	require.NoError(t, err)
	kerb := created.ReplicationInstance.KerberosAuthenticationSettings
	assert.Equal(t, "secret-1", aws.ToString(kerb.KeyCacheSecretId))

	mod, err := c.ModifyReplicationInstance(t.Context(), &dmssdk.ModifyReplicationInstanceInput{
		ReplicationInstanceArn:        created.ReplicationInstance.ReplicationInstanceArn,
		ReplicationInstanceIdentifier: aws.String("ri-b"),
		KerberosAuthenticationSettings: &types.KerberosAuthenticationSettings{
			KeyCacheSecretId: aws.String("secret-2"),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "ri-b", aws.ToString(mod.ReplicationInstance.ReplicationInstanceIdentifier))
	assert.Equal(t, "secret-2", aws.ToString(mod.ReplicationInstance.KerberosAuthenticationSettings.KeyCacheSecretId))
}

func TestRealClient_CertificateWalletAndTagsArnList(t *testing.T) {
	t.Parallel()

	c := newTestDMSClient(t, newTestDMSHandler())

	wallet := []byte("wallet-bytes")
	created, err := c.ImportCertificate(t.Context(), &dmssdk.ImportCertificateInput{
		CertificateIdentifier: aws.String("cert-w"),
		CertificateWallet:     wallet,
		Tags:                  []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}},
	})
	require.NoError(t, err)
	assert.Equal(t, wallet, created.Certificate.CertificateWallet)

	other, err := c.ImportCertificate(t.Context(), &dmssdk.ImportCertificateInput{
		CertificateIdentifier: aws.String("cert-o"),
		CertificatePem:        aws.String("pem"),
		Tags:                  []types.Tag{{Key: aws.String("a"), Value: aws.String("b")}},
	})
	require.NoError(t, err)

	tags, err := c.ListTagsForResource(t.Context(), &dmssdk.ListTagsForResourceInput{
		ResourceArnList: []string{
			aws.ToString(created.Certificate.CertificateArn),
			aws.ToString(other.Certificate.CertificateArn),
		},
	})
	require.NoError(t, err)
	require.Len(t, tags.TagList, 2)
	assert.Equal(t, aws.ToString(created.Certificate.CertificateArn), aws.ToString(tags.TagList[0].ResourceArn))
	assert.Equal(t, "k", aws.ToString(tags.TagList[0].Key))
	assert.Equal(t, aws.ToString(other.Certificate.CertificateArn), aws.ToString(tags.TagList[1].ResourceArn))
}

func TestRealClient_EventSubscriptionModifyMembers(t *testing.T) {
	t.Parallel()

	c := newTestDMSClient(t, newTestDMSHandler())

	_, err := c.CreateEventSubscription(t.Context(), &dmssdk.CreateEventSubscriptionInput{
		SubscriptionName: aws.String("es-a"),
		SnsTopicArn:      aws.String("arn:aws:sns:us-east-1:000000000000:one"),
		SourceType:       aws.String("replication-instance"),
		EventCategories:  []string{"creation"},
	})
	require.NoError(t, err)

	mod, err := c.ModifyEventSubscription(t.Context(), &dmssdk.ModifyEventSubscriptionInput{
		SubscriptionName: aws.String("es-a"),
		SnsTopicArn:      aws.String("arn:aws:sns:us-east-1:000000000000:two"),
		SourceType:       aws.String("replication-task"),
		EventCategories:  []string{"failure", "deletion"},
	})
	require.NoError(t, err)
	sub := mod.EventSubscription
	assert.Equal(t, "arn:aws:sns:us-east-1:000000000000:two", aws.ToString(sub.SnsTopicArn))
	assert.Equal(t, "replication-task", aws.ToString(sub.SourceType))
	assert.Equal(t, []string{"failure", "deletion"}, sub.EventCategoriesList)
	assert.True(t, sub.Enabled, "omitted Enabled keeps its value")
}

func TestRealClient_EndpointRenameAndExactSettings(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T, c *dmssdk.Client)
		name string
	}{
		{name: "rename_and_collision", run: func(t *testing.T, c *dmssdk.Client) {
			t.Helper()

			for _, id := range []string{"ep-a", "ep-c"} {
				_, err := c.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
					EndpointIdentifier: aws.String(id),
					EndpointType:       types.ReplicationEndpointTypeValueSource,
					EngineName:         aws.String("mysql"),
				})
				require.NoError(t, err)
			}

			list, err := c.DescribeEndpoints(t.Context(), &dmssdk.DescribeEndpointsInput{})
			require.NoError(t, err)

			var arnA *string
			for _, ep := range list.Endpoints {
				if aws.ToString(ep.EndpointIdentifier) == "ep-a" {
					arnA = ep.EndpointArn
				}
			}

			mod, err := c.ModifyEndpoint(t.Context(), &dmssdk.ModifyEndpointInput{
				EndpointArn: arnA, EndpointIdentifier: aws.String("ep-b"),
			})
			require.NoError(t, err)
			assert.Equal(t, "ep-b", aws.ToString(mod.Endpoint.EndpointIdentifier))

			_, err = c.ModifyEndpoint(t.Context(), &dmssdk.ModifyEndpointInput{
				EndpointArn: arnA, EndpointIdentifier: aws.String("ep-c"),
			})
			require.Error(t, err)
		}},
		{name: "s3_settings_merge_then_exact", run: func(t *testing.T, c *dmssdk.Client) {
			t.Helper()

			created, err := c.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
				EndpointIdentifier: aws.String("ep-s3"),
				EndpointType:       types.ReplicationEndpointTypeValueTarget,
				EngineName:         aws.String("s3"),
				S3Settings:         &types.S3Settings{BucketName: aws.String("b1"), BucketFolder: aws.String("f1")},
			})
			require.NoError(t, err)

			mod, err := c.ModifyEndpoint(t.Context(), &dmssdk.ModifyEndpointInput{
				EndpointArn: created.Endpoint.EndpointArn,
				S3Settings:  &types.S3Settings{BucketName: aws.String("b2")},
			})
			require.NoError(t, err)
			assert.Equal(t, "b2", aws.ToString(mod.Endpoint.S3Settings.BucketName))
			assert.Equal(t, "f1", aws.ToString(mod.Endpoint.S3Settings.BucketFolder))

			mod, err = c.ModifyEndpoint(t.Context(), &dmssdk.ModifyEndpointInput{
				EndpointArn:   created.Endpoint.EndpointArn,
				S3Settings:    &types.S3Settings{BucketName: aws.String("b3")},
				ExactSettings: aws.Bool(true),
			})
			require.NoError(t, err)
			assert.Equal(t, "b3", aws.ToString(mod.Endpoint.S3Settings.BucketName))
			assert.Nil(t, mod.Endpoint.S3Settings.BucketFolder)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t, newTestDMSClient(t, newTestDMSHandler()))
		})
	}
}

func seedReplicationTask(t *testing.T, c *dmssdk.Client, prefix string) string {
	t.Helper()

	src, err := c.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
		EndpointIdentifier: aws.String(prefix + "-src"),
		EndpointType:       types.ReplicationEndpointTypeValueSource,
		EngineName:         aws.String("mysql"),
	})
	require.NoError(t, err)

	tgt, err := c.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
		EndpointIdentifier: aws.String(prefix + "-tgt"),
		EndpointType:       types.ReplicationEndpointTypeValueTarget,
		EngineName:         aws.String("mysql"),
	})
	require.NoError(t, err)

	inst, err := c.CreateReplicationInstance(t.Context(), &dmssdk.CreateReplicationInstanceInput{
		ReplicationInstanceIdentifier: aws.String(prefix + "-inst"),
		ReplicationInstanceClass:      aws.String("dms.t3.micro"),
	})
	require.NoError(t, err)

	task, err := c.CreateReplicationTask(t.Context(), &dmssdk.CreateReplicationTaskInput{
		ReplicationTaskIdentifier: aws.String(prefix + "-task"),
		SourceEndpointArn:         src.Endpoint.EndpointArn,
		TargetEndpointArn:         tgt.Endpoint.EndpointArn,
		ReplicationInstanceArn:    inst.ReplicationInstance.ReplicationInstanceArn,
		MigrationType:             types.MigrationTypeValueFullLoad,
		TableMappings:             aws.String(`{"rules":[]}`),
	})
	require.NoError(t, err)

	return aws.ToString(task.ReplicationTask.ReplicationTaskArn)
}

func TestRealClient_ReplicationTaskRenameAndCdcStartExclusive(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T, c *dmssdk.Client, taskArn string)
		name string
	}{
		{name: "rename", run: func(t *testing.T, c *dmssdk.Client, taskArn string) {
			t.Helper()

			mod, err := c.ModifyReplicationTask(t.Context(), &dmssdk.ModifyReplicationTaskInput{
				ReplicationTaskArn:        aws.String(taskArn),
				ReplicationTaskIdentifier: aws.String("renamed-task"),
			})
			require.NoError(t, err)
			assert.Equal(t, "renamed-task", aws.ToString(mod.ReplicationTask.ReplicationTaskIdentifier))
		}},
		{name: "modify_rejects_both_start_forms", run: func(t *testing.T, c *dmssdk.Client, taskArn string) {
			t.Helper()

			_, err := c.ModifyReplicationTask(t.Context(), &dmssdk.ModifyReplicationTaskInput{
				ReplicationTaskArn: aws.String(taskArn),
				CdcStartPosition:   aws.String("mysql-bin.000001:4"),
				CdcStartTime:       aws.Time(time.Unix(1700000000, 0)),
			})
			require.Error(t, err)
		}},
		{name: "start_rejects_both_start_forms", run: func(t *testing.T, c *dmssdk.Client, taskArn string) {
			t.Helper()

			_, err := c.StartReplicationTask(t.Context(), &dmssdk.StartReplicationTaskInput{
				ReplicationTaskArn:       aws.String(taskArn),
				StartReplicationTaskType: types.StartReplicationTaskTypeValueStartReplication,
				CdcStartPosition:         aws.String("mysql-bin.000001:4"),
				CdcStartTime:             aws.Time(time.Unix(1700000000, 0)),
			})
			require.Error(t, err)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newTestDMSClient(t, newTestDMSHandler())
			tc.run(t, c, seedReplicationTask(t, c, "cdc"))
		})
	}
}

func TestRealClient_AssessmentRunResultMembersAndTags(t *testing.T) {
	t.Parallel()

	c := newTestDMSClient(t, newTestDMSHandler())
	taskArn := seedReplicationTask(t, c, "ar")

	out, err := c.StartReplicationTaskAssessmentRun(t.Context(), &dmssdk.StartReplicationTaskAssessmentRunInput{
		ReplicationTaskArn:   aws.String(taskArn),
		ServiceAccessRoleArn: aws.String("arn:aws:iam::000000000000:role/dms-assess"),
		ResultLocationBucket: aws.String("bucket"),
		ResultLocationFolder: aws.String("folder/x"),
		AssessmentRunName:    aws.String("run-1"),
		ResultEncryptionMode: aws.String("SSE_KMS"),
		ResultKmsKeyArn:      aws.String("arn:aws:kms:us-east-1:000000000000:key/k"),
		Tags:                 []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}},
	})
	require.NoError(t, err)
	run := out.ReplicationTaskAssessmentRun
	assert.Equal(t, "folder/x", aws.ToString(run.ResultLocationFolder))
	assert.Equal(t, "arn:aws:kms:us-east-1:000000000000:key/k", aws.ToString(run.ResultKmsKeyArn))

	tags, err := c.ListTagsForResource(t.Context(), &dmssdk.ListTagsForResourceInput{
		ResourceArn: run.ReplicationTaskAssessmentRunArn,
	})
	require.NoError(t, err)
	require.Len(t, tags.TagList, 1)
	assert.Equal(t, "dev", aws.ToString(tags.TagList[0].Value))

	plain, err := c.StartReplicationTaskAssessmentRun(t.Context(), &dmssdk.StartReplicationTaskAssessmentRunInput{
		ReplicationTaskArn:   aws.String(taskArn),
		ServiceAccessRoleArn: aws.String("arn:aws:iam::000000000000:role/dms-assess"),
		ResultLocationBucket: aws.String("bucket"),
		AssessmentRunName:    aws.String("run-2"),
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(plain.ReplicationTaskAssessmentRun.ResultLocationFolder))
}
