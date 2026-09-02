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

// TestDescribeFleetAdvisorCollectorsFilter proves Filters (collector-name,
// collector-referenced-id) genuinely narrow the result -- databasemigrationservice
// @v1.66.4 api_op_DescribeFleetAdvisorCollectors.go documents both names.
// Pre-fix the handler ignored *describeFleetAdvisorCollectorsInput entirely.
func TestDescribeFleetAdvisorCollectorsFilter(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	client := newTestDMSClient(t, h)

	c1, err := client.CreateFleetAdvisorCollector(t.Context(), &dmssdk.CreateFleetAdvisorCollectorInput{
		CollectorName:        aws.String("col-alpha"),
		ServiceAccessRoleArn: aws.String("arn:aws:iam::000000000000:role/fleet-role"),
		S3BucketName:         aws.String("fleet-bucket"),
	})
	require.NoError(t, err)

	_, err = client.CreateFleetAdvisorCollector(t.Context(), &dmssdk.CreateFleetAdvisorCollectorInput{
		CollectorName:        aws.String("col-beta"),
		ServiceAccessRoleArn: aws.String("arn:aws:iam::000000000000:role/fleet-role"),
		S3BucketName:         aws.String("fleet-bucket"),
	})
	require.NoError(t, err)

	all, err := client.DescribeFleetAdvisorCollectors(t.Context(), &dmssdk.DescribeFleetAdvisorCollectorsInput{})
	require.NoError(t, err)
	require.Len(t, all.Collectors, 2)

	byName, err := client.DescribeFleetAdvisorCollectors(t.Context(), &dmssdk.DescribeFleetAdvisorCollectorsInput{
		Filters: []types.Filter{{Name: aws.String("collector-name"), Values: []string{"col-alpha"}}},
	})
	require.NoError(t, err)
	require.Len(t, byName.Collectors, 1)
	assert.Equal(t, "col-alpha", aws.ToString(byName.Collectors[0].CollectorName))

	byID, err := client.DescribeFleetAdvisorCollectors(t.Context(), &dmssdk.DescribeFleetAdvisorCollectorsInput{
		Filters: []types.Filter{
			{Name: aws.String("collector-referenced-id"), Values: []string{aws.ToString(c1.CollectorReferencedId)}},
		},
	})
	require.NoError(t, err)
	require.Len(t, byID.Collectors, 1)
	assert.Equal(t, "col-alpha", aws.ToString(byID.Collectors[0].CollectorName))
}

// TestDescribeFleetAdvisorDatabasesFilter proves Filters (database-name,
// database-engine) narrow the result -- api_op_DescribeFleetAdvisorDatabases.go
// documents database-id/database-name/database-engine/server-ip-address/
// database-ip-address/collector-name. Pre-fix the handler ignored the input
// struct entirely.
func TestDescribeFleetAdvisorDatabasesFilter(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	client := newTestDMSClient(t, h)

	_, err := client.CreateFleetAdvisorCollector(t.Context(), &dmssdk.CreateFleetAdvisorCollectorInput{
		CollectorName:        aws.String("db-col"),
		ServiceAccessRoleArn: aws.String("arn:aws:iam::000000000000:role/fleet-role"),
		S3BucketName:         aws.String("fleet-bucket"),
	})
	require.NoError(t, err)

	all, err := client.DescribeFleetAdvisorDatabases(t.Context(), &dmssdk.DescribeFleetAdvisorDatabasesInput{})
	require.NoError(t, err)
	require.Len(t, all.Databases, 2, "CreateFleetAdvisorCollector seeds two databases")

	byEngine, err := client.DescribeFleetAdvisorDatabases(t.Context(), &dmssdk.DescribeFleetAdvisorDatabasesInput{
		Filters: []types.Filter{{Name: aws.String("database-engine"), Values: []string{"postgresql"}}},
	})
	require.NoError(t, err)
	require.Len(t, byEngine.Databases, 1)

	byName, err := client.DescribeFleetAdvisorDatabases(t.Context(), &dmssdk.DescribeFleetAdvisorDatabasesInput{
		Filters: []types.Filter{{Name: aws.String("database-name"), Values: []string{"db-col-mysql-db"}}},
	})
	require.NoError(t, err)
	require.Len(t, byName.Databases, 1)
}

// TestDescribeInstanceProfilesFilter proves the instance-profile-identifier
// filter (api_op_DescribeInstanceProfiles.go, the only documented filter name)
// narrows the result. Pre-fix in.Filters was never read.
func TestDescribeInstanceProfilesFilter(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	client := newTestDMSClient(t, h)

	_, err := client.CreateInstanceProfile(t.Context(), &dmssdk.CreateInstanceProfileInput{
		InstanceProfileName: aws.String("ip-alpha"),
	})
	require.NoError(t, err)
	_, err = client.CreateInstanceProfile(t.Context(), &dmssdk.CreateInstanceProfileInput{
		InstanceProfileName: aws.String("ip-beta"),
	})
	require.NoError(t, err)

	out, err := client.DescribeInstanceProfiles(t.Context(), &dmssdk.DescribeInstanceProfilesInput{
		Filters: []types.Filter{{Name: aws.String("instance-profile-identifier"), Values: []string{"ip-alpha"}}},
	})
	require.NoError(t, err)
	require.Len(t, out.InstanceProfiles, 1)
	assert.Equal(t, "ip-alpha", aws.ToString(out.InstanceProfiles[0].InstanceProfileName))
}

// TestDescribeMigrationProjectsFilter proves migration-project-identifier and
// instance-profile-identifier (both documented on
// api_op_DescribeMigrationProjects.go) narrow the result. Pre-fix in.Filters
// was never read at all.
func TestDescribeMigrationProjectsFilter(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	client := newTestDMSClient(t, h)

	ip, err := client.CreateInstanceProfile(t.Context(), &dmssdk.CreateInstanceProfileInput{
		InstanceProfileName: aws.String("mp-ip"),
	})
	require.NoError(t, err)

	mkProvider := func(name string) *dmssdk.CreateDataProviderOutput {
		out, providerErr := client.CreateDataProvider(t.Context(), &dmssdk.CreateDataProviderInput{
			DataProviderName: aws.String(name),
			Engine:           aws.String("mysql"),
			Settings: &types.DataProviderSettingsMemberMySqlSettings{
				Value: types.MySqlDataProviderSettings{},
			},
		})
		require.NoError(t, providerErr)

		return out
	}

	src := mkProvider("mp-filter-src")
	tgt := mkProvider("mp-filter-tgt")

	_, err = client.CreateMigrationProject(t.Context(), &dmssdk.CreateMigrationProjectInput{
		MigrationProjectName:      aws.String("mp-alpha"),
		InstanceProfileIdentifier: ip.InstanceProfile.InstanceProfileName,
		SourceDataProviderDescriptors: []types.DataProviderDescriptorDefinition{
			{DataProviderIdentifier: src.DataProvider.DataProviderName},
		},
		TargetDataProviderDescriptors: []types.DataProviderDescriptorDefinition{
			{DataProviderIdentifier: tgt.DataProvider.DataProviderName},
		},
	})
	require.NoError(t, err)

	src2 := mkProvider("mp-filter-src2")
	tgt2 := mkProvider("mp-filter-tgt2")
	_, err = client.CreateMigrationProject(t.Context(), &dmssdk.CreateMigrationProjectInput{
		MigrationProjectName:      aws.String("mp-beta"),
		InstanceProfileIdentifier: ip.InstanceProfile.InstanceProfileName,
		SourceDataProviderDescriptors: []types.DataProviderDescriptorDefinition{
			{DataProviderIdentifier: src2.DataProvider.DataProviderName},
		},
		TargetDataProviderDescriptors: []types.DataProviderDescriptorDefinition{
			{DataProviderIdentifier: tgt2.DataProvider.DataProviderName},
		},
	})
	require.NoError(t, err)

	byName, err := client.DescribeMigrationProjects(t.Context(), &dmssdk.DescribeMigrationProjectsInput{
		Filters: []types.Filter{
			{Name: aws.String("migration-project-identifier"), Values: []string{"mp-alpha"}},
		},
	})
	require.NoError(t, err)
	require.Len(t, byName.MigrationProjects, 1)
	assert.Equal(t, "mp-alpha", aws.ToString(byName.MigrationProjects[0].MigrationProjectName))

	bySrc, err := client.DescribeMigrationProjects(t.Context(), &dmssdk.DescribeMigrationProjectsInput{
		Filters: []types.Filter{
			{Name: aws.String("source-data-provider-identifier"), Values: []string{"mp-filter-src2"}},
		},
	})
	require.NoError(t, err)
	require.Len(t, bySrc.MigrationProjects, 1)
	assert.Equal(t, "mp-beta", aws.ToString(bySrc.MigrationProjects[0].MigrationProjectName))
}

// TestDescribeRecommendationsFilter proves database-id/engine-name
// (api_op_DescribeRecommendations.go: "Valid filter names: database-id |
// engine-name") narrow the result. Pre-fix in.Filters was never read.
func TestDescribeRecommendationsFilter(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	client := newTestDMSClient(t, h)

	_, err := client.CreateFleetAdvisorCollector(t.Context(), &dmssdk.CreateFleetAdvisorCollectorInput{
		CollectorName:        aws.String("rec-col"),
		ServiceAccessRoleArn: aws.String("arn:aws:iam::000000000000:role/fleet-role"),
		S3BucketName:         aws.String("fleet-bucket"),
	})
	require.NoError(t, err)

	dbs, err := client.DescribeFleetAdvisorDatabases(t.Context(), &dmssdk.DescribeFleetAdvisorDatabasesInput{})
	require.NoError(t, err)
	require.Len(t, dbs.Databases, 2)

	for _, db := range dbs.Databases {
		_, err = client.StartRecommendations(t.Context(), &dmssdk.StartRecommendationsInput{
			DatabaseId: db.DatabaseId,
			Settings: &types.RecommendationSettings{
				InstanceSizingType: aws.String("total-capacity"),
				WorkloadType:       aws.String("production"),
			},
		})
		require.NoError(t, err)
	}

	all, err := client.DescribeRecommendations(t.Context(), &dmssdk.DescribeRecommendationsInput{})
	require.NoError(t, err)
	require.Len(t, all.Recommendations, 2)

	byEngine, err := client.DescribeRecommendations(t.Context(), &dmssdk.DescribeRecommendationsInput{
		Filters: []types.Filter{{Name: aws.String("engine-name"), Values: []string{"postgresql"}}},
	})
	require.NoError(t, err)
	require.Len(t, byEngine.Recommendations, 1)

	byDB, err := client.DescribeRecommendations(t.Context(), &dmssdk.DescribeRecommendationsInput{
		Filters: []types.Filter{
			{Name: aws.String("database-id"), Values: []string{aws.ToString(dbs.Databases[0].DatabaseId)}},
		},
	})
	require.NoError(t, err)
	require.Len(t, byDB.Recommendations, 1)
}

// TestDescribeReplicationTasksFilter_MigrationTypeAndArns proves
// migration-type, endpoint-arn and replication-instance-arn (all documented
// on api_op_DescribeReplicationTasks.go alongside the already-honoured
// replication-task-id/replication-task-arn) narrow the result.
func TestDescribeReplicationTasksFilter_MigrationTypeAndArns(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	client := newTestDMSClient(t, h)

	src, err := client.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
		EndpointIdentifier: aws.String("rt-src"),
		EndpointType:       types.ReplicationEndpointTypeValueSource,
		EngineName:         aws.String("mysql"),
	})
	require.NoError(t, err)

	tgt, err := client.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
		EndpointIdentifier: aws.String("rt-tgt"),
		EndpointType:       types.ReplicationEndpointTypeValueTarget,
		EngineName:         aws.String("mysql"),
	})
	require.NoError(t, err)

	ri, err := client.CreateReplicationInstance(t.Context(), &dmssdk.CreateReplicationInstanceInput{
		ReplicationInstanceIdentifier: aws.String("rt-ri"),
		ReplicationInstanceClass:      aws.String("dms.t3.micro"),
	})
	require.NoError(t, err)

	_, err = client.CreateReplicationTask(t.Context(), &dmssdk.CreateReplicationTaskInput{
		ReplicationTaskIdentifier: aws.String("rt-full"),
		SourceEndpointArn:         src.Endpoint.EndpointArn,
		TargetEndpointArn:         tgt.Endpoint.EndpointArn,
		ReplicationInstanceArn:    ri.ReplicationInstance.ReplicationInstanceArn,
		MigrationType:             types.MigrationTypeValueFullLoad,
		TableMappings:             aws.String(`{"rules":[]}`),
	})
	require.NoError(t, err)

	_, err = client.CreateReplicationTask(t.Context(), &dmssdk.CreateReplicationTaskInput{
		ReplicationTaskIdentifier: aws.String("rt-cdc"),
		SourceEndpointArn:         src.Endpoint.EndpointArn,
		TargetEndpointArn:         tgt.Endpoint.EndpointArn,
		ReplicationInstanceArn:    ri.ReplicationInstance.ReplicationInstanceArn,
		MigrationType:             types.MigrationTypeValueCdc,
		TableMappings:             aws.String(`{"rules":[]}`),
	})
	require.NoError(t, err)

	byType, err := client.DescribeReplicationTasks(t.Context(), &dmssdk.DescribeReplicationTasksInput{
		Filters: []types.Filter{{Name: aws.String("migration-type"), Values: []string{"cdc"}}},
	})
	require.NoError(t, err)
	require.Len(t, byType.ReplicationTasks, 1)
	assert.Equal(t, "rt-cdc", aws.ToString(byType.ReplicationTasks[0].ReplicationTaskIdentifier))

	byRI, err := client.DescribeReplicationTasks(t.Context(), &dmssdk.DescribeReplicationTasksInput{
		Filters: []types.Filter{
			{
				Name:   aws.String("replication-instance-arn"),
				Values: []string{aws.ToString(ri.ReplicationInstance.ReplicationInstanceArn)},
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, byRI.ReplicationTasks, 2)

	byEndpoint, err := client.DescribeReplicationTasks(t.Context(), &dmssdk.DescribeReplicationTasksInput{
		Filters: []types.Filter{
			{
				Name:   aws.String("endpoint-arn"),
				Values: []string{"arn:aws:dms:us-east-1:000000000000:endpoint:nonexistent"},
			},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, byEndpoint.ReplicationTasks)
}

// TestDescribeTableStatisticsFilter proves schema-name/table-name/table-state
// (api_op_DescribeTableStatistics.go: "Valid filter names: schema-name |
// table-name | table-state") narrow the result. Pre-fix in.Filters was never
// read.
func TestDescribeTableStatisticsFilter(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	client := newTestDMSClient(t, h)

	src, err := client.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
		EndpointIdentifier: aws.String("ts-src"),
		EndpointType:       types.ReplicationEndpointTypeValueSource,
		EngineName:         aws.String("mysql"),
	})
	require.NoError(t, err)

	tgt, err := client.CreateEndpoint(t.Context(), &dmssdk.CreateEndpointInput{
		EndpointIdentifier: aws.String("ts-tgt"),
		EndpointType:       types.ReplicationEndpointTypeValueTarget,
		EngineName:         aws.String("mysql"),
	})
	require.NoError(t, err)

	ri, err := client.CreateReplicationInstance(t.Context(), &dmssdk.CreateReplicationInstanceInput{
		ReplicationInstanceIdentifier: aws.String("ts-ri"),
		ReplicationInstanceClass:      aws.String("dms.t3.micro"),
	})
	require.NoError(t, err)

	mappings := `{"rules":[` +
		`{"rule-type":"selection","schema-name":"public","table-name":"users"},` +
		`{"rule-type":"selection","schema-name":"public","table-name":"orders"}` +
		`]}`

	task, err := client.CreateReplicationTask(t.Context(), &dmssdk.CreateReplicationTaskInput{
		ReplicationTaskIdentifier: aws.String("ts-task"),
		SourceEndpointArn:         src.Endpoint.EndpointArn,
		TargetEndpointArn:         tgt.Endpoint.EndpointArn,
		ReplicationInstanceArn:    ri.ReplicationInstance.ReplicationInstanceArn,
		MigrationType:             types.MigrationTypeValueFullLoad,
		TableMappings:             aws.String(mappings),
	})
	require.NoError(t, err)

	all, err := client.DescribeTableStatistics(t.Context(), &dmssdk.DescribeTableStatisticsInput{
		ReplicationTaskArn: task.ReplicationTask.ReplicationTaskArn,
	})
	require.NoError(t, err)
	require.Len(t, all.TableStatistics, 2)

	byTable, err := client.DescribeTableStatistics(t.Context(), &dmssdk.DescribeTableStatisticsInput{
		ReplicationTaskArn: task.ReplicationTask.ReplicationTaskArn,
		Filters:            []types.Filter{{Name: aws.String("table-name"), Values: []string{"orders"}}},
	})
	require.NoError(t, err)
	require.Len(t, byTable.TableStatistics, 1)
	assert.Equal(t, "orders", aws.ToString(byTable.TableStatistics[0].TableName))
}

// TestDescribeEventsFilter proves the top-level SourceIdentifier, SourceType
// and StartTime/EndTime members (api_op_DescribeEvents.go -- NOT part of
// Filters, which only carries replication-instance-id) narrow the result.
// Pre-fix the handler's describeEventsInput struct declared none of these
// fields at all, so a real client's values could never be read.
func TestDescribeEventsFilter(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddEventInternal("ri-1", "replication-instance", "instance ri-1 created", []string{"creation"})
	h.Backend.AddEventInternal("task-1", "replication-task", "task task-1 started", []string{"state change"})

	client := newTestDMSClient(t, h)

	all, err := client.DescribeEvents(t.Context(), &dmssdk.DescribeEventsInput{})
	require.NoError(t, err)
	require.Len(t, all.Events, 2)

	bySource, err := client.DescribeEvents(t.Context(), &dmssdk.DescribeEventsInput{
		SourceIdentifier: aws.String("ri-1"),
		SourceType:       types.SourceTypeReplicationInstance,
	})
	require.NoError(t, err)
	require.Len(t, bySource.Events, 1)
	assert.Equal(t, "ri-1", aws.ToString(bySource.Events[0].SourceIdentifier))

	byType, err := client.DescribeEvents(t.Context(), &dmssdk.DescribeEventsInput{
		SourceType: types.SourceType("replication-task"),
	})
	require.NoError(t, err)
	require.Len(t, byType.Events, 1)
	assert.Equal(t, "task-1", aws.ToString(byType.Events[0].SourceIdentifier))

	future := time.Now().Add(time.Hour)
	byWindow, err := client.DescribeEvents(t.Context(), &dmssdk.DescribeEventsInput{
		StartTime: aws.Time(future),
	})
	require.NoError(t, err)
	assert.Empty(t, byWindow.Events, "StartTime in the future must exclude all past events")
}

// TestDescribeDataMigrationsWithoutSettings proves WithoutSettings
// (api_op_DescribeDataMigrations.go: "avoid returning information about
// settings") suppresses DataMigrationSettings. Pre-fix the field wasn't even
// in the handler's input struct.
func TestDescribeDataMigrationsWithoutSettings(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	client := newTestDMSClient(t, h)

	ip, err := client.CreateInstanceProfile(t.Context(), &dmssdk.CreateInstanceProfileInput{
		InstanceProfileName: aws.String("dm-ip"),
	})
	require.NoError(t, err)

	mkProvider := func(name string) *dmssdk.CreateDataProviderOutput {
		out, providerErr := client.CreateDataProvider(t.Context(), &dmssdk.CreateDataProviderInput{
			DataProviderName: aws.String(name),
			Engine:           aws.String("mysql"),
			Settings: &types.DataProviderSettingsMemberMySqlSettings{
				Value: types.MySqlDataProviderSettings{},
			},
		})
		require.NoError(t, providerErr)

		return out
	}
	src := mkProvider("dm-src")
	tgt := mkProvider("dm-tgt")

	mp, err := client.CreateMigrationProject(t.Context(), &dmssdk.CreateMigrationProjectInput{
		MigrationProjectName:      aws.String("dm-project"),
		InstanceProfileIdentifier: ip.InstanceProfile.InstanceProfileName,
		SourceDataProviderDescriptors: []types.DataProviderDescriptorDefinition{
			{DataProviderIdentifier: src.DataProvider.DataProviderName},
		},
		TargetDataProviderDescriptors: []types.DataProviderDescriptorDefinition{
			{DataProviderIdentifier: tgt.DataProvider.DataProviderName},
		},
	})
	require.NoError(t, err)

	_, err = client.CreateDataMigration(t.Context(), &dmssdk.CreateDataMigrationInput{
		DataMigrationName:          aws.String("dm-1"),
		MigrationProjectIdentifier: mp.MigrationProject.MigrationProjectName,
		DataMigrationType:          types.MigrationTypeValueFullLoad,
		ServiceAccessRoleArn:       aws.String("arn:aws:iam::000000000000:role/dms"),
	})
	require.NoError(t, err)

	withSettings, err := client.DescribeDataMigrations(t.Context(), &dmssdk.DescribeDataMigrationsInput{})
	require.NoError(t, err)
	require.Len(t, withSettings.DataMigrations, 1)
	require.NotNil(t, withSettings.DataMigrations[0].DataMigrationSettings)

	without, err := client.DescribeDataMigrations(t.Context(), &dmssdk.DescribeDataMigrationsInput{
		WithoutSettings: aws.Bool(true),
	})
	require.NoError(t, err)
	require.Len(t, without.DataMigrations, 1)
	assert.Nil(t, without.DataMigrations[0].DataMigrationSettings,
		"WithoutSettings=true must suppress DataMigrationSettings")
}

// TestDescribeReplicationSubnetGroupsFilter proves replication-subnet-group-id
// (api_op_DescribeReplicationSubnetGroups.go: "Valid filter names:
// replication-subnet-group-id") narrows the result. Pre-fix in.Filters was
// never read.
func TestDescribeReplicationSubnetGroupsFilter(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	client := newTestDMSClient(t, h)

	_, err := client.CreateReplicationSubnetGroup(t.Context(), &dmssdk.CreateReplicationSubnetGroupInput{
		ReplicationSubnetGroupIdentifier:  aws.String("sg-alpha"),
		ReplicationSubnetGroupDescription: aws.String("alpha"),
		SubnetIds:                         []string{"subnet-1"},
	})
	require.NoError(t, err)

	_, err = client.CreateReplicationSubnetGroup(t.Context(), &dmssdk.CreateReplicationSubnetGroupInput{
		ReplicationSubnetGroupIdentifier:  aws.String("sg-beta"),
		ReplicationSubnetGroupDescription: aws.String("beta"),
		SubnetIds:                         []string{"subnet-2"},
	})
	require.NoError(t, err)

	out, err := client.DescribeReplicationSubnetGroups(t.Context(), &dmssdk.DescribeReplicationSubnetGroupsInput{
		Filters: []types.Filter{
			{Name: aws.String("replication-subnet-group-id"), Values: []string{"sg-alpha"}},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.ReplicationSubnetGroups, 1)
	assert.Equal(t, "sg-alpha", aws.ToString(out.ReplicationSubnetGroups[0].ReplicationSubnetGroupIdentifier))
}

// TestDescribeEndpointTypesFilter proves engine-name/endpoint-type
// (api_op_DescribeEndpointTypes.go: "Valid filter names: engine-name |
// endpoint-type") narrow the static support-matrix result. Pre-fix
// in.Filters was never read.
func TestDescribeEndpointTypesFilter(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	client := newTestDMSClient(t, h)

	all, err := client.DescribeEndpointTypes(t.Context(), &dmssdk.DescribeEndpointTypesInput{})
	require.NoError(t, err)
	require.NotEmpty(t, all.SupportedEndpointTypes)

	byEngine, err := client.DescribeEndpointTypes(t.Context(), &dmssdk.DescribeEndpointTypesInput{
		Filters: []types.Filter{{Name: aws.String("engine-name"), Values: []string{"mysql"}}},
	})
	require.NoError(t, err)
	for _, et := range byEngine.SupportedEndpointTypes {
		assert.Equal(t, "mysql", aws.ToString(et.EngineName))
	}
	assert.Less(t, len(byEngine.SupportedEndpointTypes), len(all.SupportedEndpointTypes))

	byDirection, err := client.DescribeEndpointTypes(t.Context(), &dmssdk.DescribeEndpointTypesInput{
		Filters: []types.Filter{{Name: aws.String("endpoint-type"), Values: []string{"source"}}},
	})
	require.NoError(t, err)
	for _, et := range byDirection.SupportedEndpointTypes {
		assert.Equal(t, types.ReplicationEndpointTypeValueSource, et.EndpointType)
	}
}
