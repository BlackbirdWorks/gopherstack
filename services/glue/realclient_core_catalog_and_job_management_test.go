package glue_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// TestRealClient_CoreCatalogAndJobManagement covers glue's highest-priority typed-client-
// uncovered op families (gopherstack-n3zi): tables/partitions,
// crawlers/classifiers, jobs/bookmarks, triggers, workflows, connections,
// dev endpoints, sessions/statements, schema registry, data quality, ML
// transforms, blueprints, resource policy, catalog encryption/security
// configurations, multi-catalog, column statistics and zero-ETL
// integrations. Each subtest creates real state through the typed
// aws-sdk-go-v2 client and asserts decoded response values.
func TestRealClient_CoreCatalogAndJobManagement(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{testPartitionsRealClient, "partitions"},
		{testTableVersionsRealClient, "table_versions"},
		{testCrawlersRealClient, "crawlers"},
		{testClassifiersRealClient, "classifiers"},
		{testJobsBookmarksRealClient, "jobs_bookmarks"},
		{testTriggersRealClient, "triggers"},
		{testWorkflowsRealClient, "workflows"},
		{testConnectionsRealClient, "connections"},
		{testDevEndpointsRealClient, "dev_endpoints"},
		{testSessionsRealClient, "sessions_statements"},
		{testSchemaRegistryRealClient, "schema_registry"},
		{testDataQualityRealClient, "data_quality"},
		{testMLTransformsRealClient, "ml_transforms"},
		{testBlueprintsRealClient, "blueprints"},
		{testResourcePolicyRealClient, "resource_policy"},
		{testCatalogSecurityRealClient, "catalog_security"},
		{testCatalogsRealClient, "catalogs"},
		{testColumnStatisticsRealClient, "column_statistics"},
		{testIntegrationsRealClient, "integrations"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t)
		})
	}
}

func newRealClient(t *testing.T) *gluesdk.Client {
	t.Helper()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)

	return newTestGlueClient(t, glue.NewHandler(backend))
}

func testPartitionsRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)
	ctx := t.Context()

	_, err := client.CreateDatabase(ctx, &gluesdk.CreateDatabaseInput{
		DatabaseInput: &types.DatabaseInput{Name: aws.String("pdb")},
	})
	require.NoError(t, err)
	_, err = client.CreateTable(ctx, &gluesdk.CreateTableInput{
		DatabaseName: aws.String("pdb"),
		TableInput: &types.TableInput{
			Name:          aws.String("ptbl"),
			PartitionKeys: []types.Column{{Name: aws.String("dt")}},
		},
	})
	require.NoError(t, err)

	mkPartition := func(v string) types.PartitionInput {
		return types.PartitionInput{
			Values:            []string{v},
			StorageDescriptor: &types.StorageDescriptor{Location: aws.String("s3://bucket/" + v)},
		}
	}

	batchCreated, err := client.BatchCreatePartition(ctx, &gluesdk.BatchCreatePartitionInput{
		DatabaseName:       aws.String("pdb"),
		TableName:          aws.String("ptbl"),
		PartitionInputList: []types.PartitionInput{mkPartition("2024-01-01"), mkPartition("2024-01-02")},
	})
	require.NoError(t, err)
	assert.Empty(t, batchCreated.Errors)

	_, err = client.CreatePartition(ctx, &gluesdk.CreatePartitionInput{
		DatabaseName:   aws.String("pdb"),
		TableName:      aws.String("ptbl"),
		PartitionInput: new(mkPartition("2024-01-03")),
	})
	require.NoError(t, err)

	got, err := client.GetPartition(ctx, &gluesdk.GetPartitionInput{
		DatabaseName:    aws.String("pdb"),
		TableName:       aws.String("ptbl"),
		PartitionValues: []string{"2024-01-01"},
	})
	require.NoError(t, err)
	require.NotNil(t, got.Partition)
	assert.Equal(t, []string{"2024-01-01"}, got.Partition.Values)
	assert.Equal(t, "s3://bucket/2024-01-01", aws.ToString(got.Partition.StorageDescriptor.Location))

	all, err := client.GetPartitions(ctx, &gluesdk.GetPartitionsInput{
		DatabaseName: aws.String("pdb"), TableName: aws.String("ptbl"),
	})
	require.NoError(t, err)
	assert.Len(t, all.Partitions, 3)

	idx, err := client.CreatePartitionIndex(ctx, &gluesdk.CreatePartitionIndexInput{
		DatabaseName: aws.String("pdb"),
		TableName:    aws.String("ptbl"),
		PartitionIndex: &types.PartitionIndex{
			IndexName: aws.String("dtidx"),
			Keys:      []string{"dt"},
		},
	})
	require.NoError(t, err)
	_ = idx

	idxList, err := client.GetPartitionIndexes(ctx, &gluesdk.GetPartitionIndexesInput{
		DatabaseName: aws.String("pdb"), TableName: aws.String("ptbl"),
	})
	require.NoError(t, err)
	require.Len(t, idxList.PartitionIndexDescriptorList, 1)
	assert.Equal(t, "dtidx", aws.ToString(idxList.PartitionIndexDescriptorList[0].IndexName))

	_, err = client.DeletePartitionIndex(ctx, &gluesdk.DeletePartitionIndexInput{
		DatabaseName: aws.String("pdb"), TableName: aws.String("ptbl"), IndexName: aws.String("dtidx"),
	})
	require.NoError(t, err)

	batchGot, err := client.BatchGetPartition(ctx, &gluesdk.BatchGetPartitionInput{
		DatabaseName: aws.String("pdb"),
		TableName:    aws.String("ptbl"),
		PartitionsToGet: []types.PartitionValueList{
			{Values: []string{"2024-01-01"}},
			{Values: []string{"missing"}},
		},
	})
	require.NoError(t, err)
	assert.Len(t, batchGot.Partitions, 1)
	assert.Len(t, batchGot.UnprocessedKeys, 1)

	updated := mkPartition("2024-01-01")
	updated.StorageDescriptor.Location = aws.String("s3://bucket/updated")
	_, err = client.UpdatePartition(ctx, &gluesdk.UpdatePartitionInput{
		DatabaseName:       aws.String("pdb"),
		TableName:          aws.String("ptbl"),
		PartitionValueList: []string{"2024-01-01"},
		PartitionInput:     &updated,
	})
	require.NoError(t, err)

	reGot, err := client.GetPartition(ctx, &gluesdk.GetPartitionInput{
		DatabaseName: aws.String("pdb"), TableName: aws.String("ptbl"), PartitionValues: []string{"2024-01-01"},
	})
	require.NoError(t, err)
	assert.Equal(t, "s3://bucket/updated", aws.ToString(reGot.Partition.StorageDescriptor.Location))

	_, err = client.BatchUpdatePartition(ctx, &gluesdk.BatchUpdatePartitionInput{
		DatabaseName: aws.String("pdb"),
		TableName:    aws.String("ptbl"),
		Entries: []types.BatchUpdatePartitionRequestEntry{
			{PartitionValueList: []string{"2024-01-02"}, PartitionInput: new(mkPartition("2024-01-02"))},
		},
	})
	require.NoError(t, err)

	unfilteredOne, err := client.GetUnfilteredPartitionMetadata(ctx, &gluesdk.GetUnfilteredPartitionMetadataInput{
		DatabaseName: aws.String("pdb"), TableName: aws.String("ptbl"), PartitionValues: []string{"2024-01-01"},
		CatalogId:                aws.String(testAccountID),
		SupportedPermissionTypes: []types.PermissionType{types.PermissionTypeColumnPermission},
	})
	require.NoError(t, err)
	require.NotNil(t, unfilteredOne.Partition)
	assert.Equal(t, []string{"2024-01-01"}, unfilteredOne.Partition.Values)

	unfilteredAll, err := client.GetUnfilteredPartitionsMetadata(ctx, &gluesdk.GetUnfilteredPartitionsMetadataInput{
		DatabaseName: aws.String("pdb"), TableName: aws.String("ptbl"), CatalogId: aws.String(testAccountID),
		SupportedPermissionTypes: []types.PermissionType{types.PermissionTypeColumnPermission},
	})
	require.NoError(t, err)
	assert.Len(t, unfilteredAll.UnfilteredPartitions, 3)

	_, err = client.DeletePartition(ctx, &gluesdk.DeletePartitionInput{
		DatabaseName: aws.String("pdb"), TableName: aws.String("ptbl"), PartitionValues: []string{"2024-01-03"},
	})
	require.NoError(t, err)

	_, err = client.BatchDeletePartition(ctx, &gluesdk.BatchDeletePartitionInput{
		DatabaseName:       aws.String("pdb"),
		TableName:          aws.String("ptbl"),
		PartitionsToDelete: []types.PartitionValueList{{Values: []string{"2024-01-01"}}},
	})
	require.NoError(t, err)

	remaining, err := client.GetPartitions(ctx, &gluesdk.GetPartitionsInput{
		DatabaseName: aws.String("pdb"), TableName: aws.String("ptbl"),
	})
	require.NoError(t, err)
	assert.Len(t, remaining.Partitions, 1)
}

func testTableVersionsRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)
	ctx := t.Context()

	_, err := client.CreateDatabase(ctx, &gluesdk.CreateDatabaseInput{
		DatabaseInput: &types.DatabaseInput{Name: aws.String("tvdb")},
	})
	require.NoError(t, err)
	_, err = client.CreateTable(ctx, &gluesdk.CreateTableInput{
		DatabaseName: aws.String("tvdb"),
		TableInput:   &types.TableInput{Name: aws.String("tvtbl")},
	})
	require.NoError(t, err)

	_, err = client.UpdateTable(ctx, &gluesdk.UpdateTableInput{
		DatabaseName: aws.String("tvdb"),
		TableInput:   &types.TableInput{Name: aws.String("tvtbl"), Description: aws.String("v2")},
	})
	require.NoError(t, err)

	versions, err := client.GetTableVersions(ctx, &gluesdk.GetTableVersionsInput{
		DatabaseName: aws.String("tvdb"), TableName: aws.String("tvtbl"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, versions.TableVersions)

	firstVersionID := aws.ToString(versions.TableVersions[0].VersionId)
	one, err := client.GetTableVersion(ctx, &gluesdk.GetTableVersionInput{
		DatabaseName: aws.String("tvdb"), TableName: aws.String("tvtbl"), VersionId: aws.String(firstVersionID),
	})
	require.NoError(t, err)
	require.NotNil(t, one.TableVersion)
	assert.Equal(t, "tvtbl", aws.ToString(one.TableVersion.Table.Name))

	tables, err := client.GetTables(ctx, &gluesdk.GetTablesInput{DatabaseName: aws.String("tvdb")})
	require.NoError(t, err)
	require.Len(t, tables.TableList, 1)
	assert.Equal(t, "v2", aws.ToString(tables.TableList[0].Description))

	unfiltered, err := client.GetUnfilteredTableMetadata(ctx, &gluesdk.GetUnfilteredTableMetadataInput{
		DatabaseName: aws.String("tvdb"), Name: aws.String("tvtbl"), CatalogId: aws.String(testAccountID),
		SupportedPermissionTypes: []types.PermissionType{types.PermissionTypeColumnPermission},
	})
	require.NoError(t, err)
	require.NotNil(t, unfiltered.Table)
	assert.Equal(t, "tvtbl", aws.ToString(unfiltered.Table.Name))

	_, err = client.UpdateDatabase(ctx, &gluesdk.UpdateDatabaseInput{
		Name:          aws.String("tvdb"),
		DatabaseInput: &types.DatabaseInput{Name: aws.String("tvdb"), Description: aws.String("updated db")},
	})
	require.NoError(t, err)

	_, err = client.CreateTable(ctx, &gluesdk.CreateTableInput{
		DatabaseName: aws.String("tvdb"),
		TableInput:   &types.TableInput{Name: aws.String("tvtbl2")},
	})
	require.NoError(t, err)

	batchDel, err := client.BatchDeleteTable(ctx, &gluesdk.BatchDeleteTableInput{
		DatabaseName:   aws.String("tvdb"),
		TablesToDelete: []string{"tvtbl2"},
	})
	require.NoError(t, err)
	assert.Empty(t, batchDel.Errors)

	batchDelVer, err := client.BatchDeleteTableVersion(ctx, &gluesdk.BatchDeleteTableVersionInput{
		DatabaseName: aws.String("tvdb"), TableName: aws.String("tvtbl"), VersionIds: []string{firstVersionID},
	})
	require.NoError(t, err)
	assert.Empty(t, batchDelVer.Errors)
}

func testCrawlersRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)
	ctx := t.Context()

	_, err := client.CreateDatabase(ctx, &gluesdk.CreateDatabaseInput{
		DatabaseInput: &types.DatabaseInput{Name: aws.String("cdb")},
	})
	require.NoError(t, err)

	_, err = client.CreateCrawler(ctx, &gluesdk.CreateCrawlerInput{
		Name:         aws.String("c1"),
		Role:         aws.String("arn:aws:iam::" + testAccountID + ":role/glue-role"),
		DatabaseName: aws.String("cdb"),
		Targets:      &types.CrawlerTargets{S3Targets: []types.S3Target{{Path: aws.String("s3://bucket/data")}}},
	})
	require.NoError(t, err)

	// UpdateCrawler is rejected while RUNNING/STARTING/STOPPING (real AWS
	// behavior, matched here) -- run it before StartCrawler so it exercises
	// the READY-state path.
	_, err = client.UpdateCrawler(ctx, &gluesdk.UpdateCrawlerInput{
		Name:         aws.String("c1"),
		Role:         aws.String("arn:aws:iam::" + testAccountID + ":role/glue-role2"),
		DatabaseName: aws.String("cdb"),
		Targets:      &types.CrawlerTargets{S3Targets: []types.S3Target{{Path: aws.String("s3://bucket/data2")}}},
	})
	require.NoError(t, err)

	got, err := client.GetCrawler(ctx, &gluesdk.GetCrawlerInput{Name: aws.String("c1")})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iam::"+testAccountID+":role/glue-role2", aws.ToString(got.Crawler.Role))

	_, err = client.StartCrawler(ctx, &gluesdk.StartCrawlerInput{Name: aws.String("c1")})
	require.NoError(t, err)

	_, err = client.StopCrawler(ctx, &gluesdk.StopCrawlerInput{Name: aws.String("c1")})
	require.NoError(t, err)

	_, err = client.UpdateCrawlerSchedule(ctx, &gluesdk.UpdateCrawlerScheduleInput{
		CrawlerName: aws.String("c1"), Schedule: aws.String("cron(0 12 * * ? *)"),
	})
	require.NoError(t, err)

	_, err = client.StartCrawlerSchedule(ctx, &gluesdk.StartCrawlerScheduleInput{CrawlerName: aws.String("c1")})
	require.NoError(t, err)

	_, err = client.StopCrawlerSchedule(ctx, &gluesdk.StopCrawlerScheduleInput{CrawlerName: aws.String("c1")})
	require.NoError(t, err)

	batch, err := client.BatchGetCrawlers(ctx, &gluesdk.BatchGetCrawlersInput{CrawlerNames: []string{"c1", "missing"}})
	require.NoError(t, err)
	require.Len(t, batch.Crawlers, 1)
	assert.Equal(t, []string{"missing"}, batch.CrawlersNotFound)

	metrics, err := client.GetCrawlerMetrics(ctx, &gluesdk.GetCrawlerMetricsInput{CrawlerNameList: []string{"c1"}})
	require.NoError(t, err)
	require.Len(t, metrics.CrawlerMetricsList, 1)
	assert.Equal(t, "c1", aws.ToString(metrics.CrawlerMetricsList[0].CrawlerName))

	crawls, err := client.ListCrawls(ctx, &gluesdk.ListCrawlsInput{CrawlerName: aws.String("c1")})
	require.NoError(t, err)
	assert.NotNil(t, crawls.Crawls)
}

func testClassifiersRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)
	ctx := t.Context()

	_, err := client.CreateClassifier(ctx, &gluesdk.CreateClassifierInput{
		GrokClassifier: &types.CreateGrokClassifierRequest{
			Classification: aws.String("json"),
			Name:           aws.String("cl1"),
			GrokPattern:    aws.String("%{GREEDYDATA}"),
		},
	})
	require.NoError(t, err)

	_, err = client.UpdateClassifier(ctx, &gluesdk.UpdateClassifierInput{
		GrokClassifier: &types.UpdateGrokClassifierRequest{
			Name:        aws.String("cl1"),
			GrokPattern: aws.String("%{TIMESTAMP_ISO8601}"),
		},
	})
	require.NoError(t, err)

	got, err := client.GetClassifier(ctx, &gluesdk.GetClassifierInput{Name: aws.String("cl1")})
	require.NoError(t, err)
	require.NotNil(t, got.Classifier.GrokClassifier)
	assert.Equal(t, "%{TIMESTAMP_ISO8601}", aws.ToString(got.Classifier.GrokClassifier.GrokPattern))

	_, err = client.DeleteClassifier(ctx, &gluesdk.DeleteClassifierInput{Name: aws.String("cl1")})
	require.NoError(t, err)

	_, err = client.GetClassifier(ctx, &gluesdk.GetClassifierInput{Name: aws.String("cl1")})
	require.Error(t, err)
}

func testJobsBookmarksRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)
	ctx := t.Context()

	_, err := client.CreateJob(ctx, &gluesdk.CreateJobInput{
		Name:    aws.String("j1"),
		Role:    aws.String("r"),
		Command: &types.JobCommand{Name: aws.String("glueetl")},
	})
	require.NoError(t, err)

	_, err = client.UpdateJob(ctx, &gluesdk.UpdateJobInput{
		JobName: aws.String("j1"),
		JobUpdate: &types.JobUpdate{
			Role:    aws.String("r2"),
			Command: &types.JobCommand{Name: aws.String("glueetl")},
		},
	})
	require.NoError(t, err)

	jobGot, err := client.GetJob(ctx, &gluesdk.GetJobInput{JobName: aws.String("j1")})
	require.NoError(t, err)
	assert.Equal(t, "r2", aws.ToString(jobGot.Job.Role))

	_, err = client.StartJobRun(ctx, &gluesdk.StartJobRunInput{JobName: aws.String("j1")})
	require.NoError(t, err)

	bm, err := client.GetJobBookmark(ctx, &gluesdk.GetJobBookmarkInput{JobName: aws.String("j1")})
	require.NoError(t, err)
	require.NotNil(t, bm.JobBookmarkEntry)
	assert.Equal(t, "j1", aws.ToString(bm.JobBookmarkEntry.JobName))
	assert.NotEmpty(t, aws.ToString(bm.JobBookmarkEntry.RunId))

	reset, err := client.ResetJobBookmark(ctx, &gluesdk.ResetJobBookmarkInput{JobName: aws.String("j1")})
	require.NoError(t, err)
	require.NotNil(t, reset.JobBookmarkEntry)

	_, err = client.CreateJob(ctx, &gluesdk.CreateJobInput{
		Name:    aws.String("j2"),
		Role:    aws.String("r"),
		Command: &types.JobCommand{Name: aws.String("glueetl")},
	})
	require.NoError(t, err)

	batch, err := client.BatchGetJobs(ctx, &gluesdk.BatchGetJobsInput{JobNames: []string{"j1", "j2", "missing"}})
	require.NoError(t, err)
	assert.Len(t, batch.Jobs, 2)
	assert.Equal(t, []string{"missing"}, batch.JobsNotFound)

	fromSrc, err := client.UpdateJobFromSourceControl(ctx, &gluesdk.UpdateJobFromSourceControlInput{
		JobName: aws.String("j1"), Provider: types.SourceControlProviderGithub,
	})
	require.NoError(t, err)
	assert.Equal(t, "j1", aws.ToString(fromSrc.JobName))

	toSrc, err := client.UpdateSourceControlFromJob(ctx, &gluesdk.UpdateSourceControlFromJobInput{
		JobName: aws.String("j1"), Provider: types.SourceControlProviderGithub,
	})
	require.NoError(t, err)
	assert.Equal(t, "j1", aws.ToString(toSrc.JobName))
}

func testTriggersRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)
	ctx := t.Context()

	_, err := client.CreateJob(ctx, &gluesdk.CreateJobInput{
		Name:    aws.String("trigjob"),
		Role:    aws.String("r"),
		Command: &types.JobCommand{Name: aws.String("glueetl")},
	})
	require.NoError(t, err)

	_, err = client.CreateTrigger(ctx, &gluesdk.CreateTriggerInput{
		Name:    aws.String("t1"),
		Type:    types.TriggerTypeOnDemand,
		Actions: []types.Action{{JobName: aws.String("trigjob")}},
	})
	require.NoError(t, err)

	_, err = client.StartTrigger(ctx, &gluesdk.StartTriggerInput{Name: aws.String("t1")})
	require.NoError(t, err)

	_, err = client.StopTrigger(ctx, &gluesdk.StopTriggerInput{Name: aws.String("t1")})
	require.NoError(t, err)

	batch, err := client.BatchGetTriggers(ctx, &gluesdk.BatchGetTriggersInput{TriggerNames: []string{"t1", "missing"}})
	require.NoError(t, err)
	require.Len(t, batch.Triggers, 1)
	assert.Equal(t, "t1", aws.ToString(batch.Triggers[0].Name))
	assert.Equal(t, []string{"missing"}, batch.TriggersNotFound)
}

func testWorkflowsRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)
	ctx := t.Context()

	_, err := client.CreateWorkflow(ctx, &gluesdk.CreateWorkflowInput{Name: aws.String("wf1")})
	require.NoError(t, err)

	_, err = client.UpdateWorkflow(ctx, &gluesdk.UpdateWorkflowInput{
		Name: aws.String("wf1"), Description: aws.String("updated wf"),
	})
	require.NoError(t, err)

	wfGot, err := client.GetWorkflow(ctx, &gluesdk.GetWorkflowInput{Name: aws.String("wf1")})
	require.NoError(t, err)
	assert.Equal(t, "updated wf", aws.ToString(wfGot.Workflow.Description))

	_, err = client.CreateWorkflow(ctx, &gluesdk.CreateWorkflowInput{Name: aws.String("wf2")})
	require.NoError(t, err)

	batch, err := client.BatchGetWorkflows(ctx, &gluesdk.BatchGetWorkflowsInput{
		Names: []string{"wf1", "wf2", "missing"},
	})
	require.NoError(t, err)
	assert.Len(t, batch.Workflows, 2)
	assert.Equal(t, []string{"missing"}, batch.MissingWorkflows)

	run, err := client.StartWorkflowRun(ctx, &gluesdk.StartWorkflowRunInput{Name: aws.String("wf1")})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(run.RunId))

	_, err = client.PutWorkflowRunProperties(ctx, &gluesdk.PutWorkflowRunPropertiesInput{
		Name: aws.String("wf1"), RunId: run.RunId, RunProperties: map[string]string{"k": "v"},
	})
	require.NoError(t, err)

	props, err := client.GetWorkflowRunProperties(ctx, &gluesdk.GetWorkflowRunPropertiesInput{
		Name: aws.String("wf1"), RunId: run.RunId,
	})
	require.NoError(t, err)
	assert.Equal(t, "v", props.RunProperties["k"])
}

func testConnectionsRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)
	ctx := t.Context()

	_, err := client.CreateConnection(ctx, &gluesdk.CreateConnectionInput{
		ConnectionInput: &types.ConnectionInput{
			Name:                 aws.String("conn1"),
			ConnectionType:       types.ConnectionTypeJdbc,
			ConnectionProperties: map[string]string{"JDBC_CONNECTION_URL": "jdbc:mysql://host/db"},
		},
	})
	require.NoError(t, err)

	_, err = client.UpdateConnection(ctx, &gluesdk.UpdateConnectionInput{
		Name: aws.String("conn1"),
		ConnectionInput: &types.ConnectionInput{
			Name:                 aws.String("conn1"),
			ConnectionType:       types.ConnectionTypeJdbc,
			ConnectionProperties: map[string]string{"JDBC_CONNECTION_URL": "jdbc:mysql://host2/db"},
			Description:          aws.String("updated"),
		},
	})
	require.NoError(t, err)

	got, err := client.GetConnection(ctx, &gluesdk.GetConnectionInput{Name: aws.String("conn1")})
	require.NoError(t, err)
	assert.Equal(t, "updated", aws.ToString(got.Connection.Description))

	_, err = client.TestConnection(ctx, &gluesdk.TestConnectionInput{ConnectionName: aws.String("conn1")})
	require.NoError(t, err)

	_, err = client.RegisterConnectionType(ctx, &gluesdk.RegisterConnectionTypeInput{
		ConnectionType:  aws.String("REST-SLICE5CUSTOMTYPE"),
		IntegrationType: types.IntegrationTypeRest,
		ConnectionProperties: &types.ConnectionPropertiesConfiguration{
			Url: &types.ConnectorProperty{
				Name: aws.String("endpoint"), PropertyType: types.PropertyTypeUserInput, Required: aws.Bool(true),
			},
		},
		ConnectorAuthenticationConfiguration: &types.ConnectorAuthenticationConfiguration{
			AuthenticationTypes: []types.AuthenticationType{types.AuthenticationTypeBasic},
		},
		RestConfiguration: &types.RestConfiguration{},
	})
	require.NoError(t, err)

	_, err = client.DeleteConnectionType(ctx, &gluesdk.DeleteConnectionTypeInput{
		ConnectionType: aws.String("REST-SLICE5CUSTOMTYPE"),
	})
	require.NoError(t, err)

	_, err = client.DescribeConnectionType(ctx, &gluesdk.DescribeConnectionTypeInput{
		ConnectionType: aws.String("REST-SLICE5CUSTOMTYPE"),
	})
	require.Error(t, err)
}

func testDevEndpointsRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)
	ctx := t.Context()

	created, err := client.CreateDevEndpoint(ctx, &gluesdk.CreateDevEndpointInput{
		EndpointName: aws.String("dep1"),
		RoleArn:      aws.String("arn:aws:iam::" + testAccountID + ":role/glue-role"),
	})
	require.NoError(t, err)
	assert.Equal(t, "dep1", aws.ToString(created.EndpointName))

	got, err := client.GetDevEndpoint(ctx, &gluesdk.GetDevEndpointInput{EndpointName: aws.String("dep1")})
	require.NoError(t, err)
	require.NotNil(t, got.DevEndpoint)
	assert.Equal(t, "dep1", aws.ToString(got.DevEndpoint.EndpointName))

	_, err = client.UpdateDevEndpoint(ctx, &gluesdk.UpdateDevEndpointInput{
		EndpointName: aws.String("dep1"),
		AddArguments: map[string]string{"--foo": "bar"},
	})
	require.NoError(t, err)

	reGot, err := client.GetDevEndpoint(ctx, &gluesdk.GetDevEndpointInput{EndpointName: aws.String("dep1")})
	require.NoError(t, err)
	assert.Equal(t, "bar", reGot.DevEndpoint.Arguments["--foo"])

	_, err = client.CreateDevEndpoint(ctx, &gluesdk.CreateDevEndpointInput{
		EndpointName: aws.String("dep2"),
		RoleArn:      aws.String("arn:aws:iam::" + testAccountID + ":role/glue-role"),
	})
	require.NoError(t, err)

	batch, err := client.BatchGetDevEndpoints(ctx, &gluesdk.BatchGetDevEndpointsInput{
		DevEndpointNames: []string{"dep1", "dep2", "missing"},
	})
	require.NoError(t, err)
	assert.Len(t, batch.DevEndpoints, 2)
	assert.Equal(t, []string{"missing"}, batch.DevEndpointsNotFound)

	_, err = client.DeleteDevEndpoint(ctx, &gluesdk.DeleteDevEndpointInput{EndpointName: aws.String("dep2")})
	require.NoError(t, err)
}

func testSessionsRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)
	ctx := t.Context()

	_, err := client.CreateSession(ctx, &gluesdk.CreateSessionInput{
		Id:      aws.String("sess1"),
		Role:    aws.String("r"),
		Command: &types.SessionCommand{Name: aws.String("glueetl")},
	})
	require.NoError(t, err)

	sessGot, err := client.GetSession(ctx, &gluesdk.GetSessionInput{Id: aws.String("sess1")})
	require.NoError(t, err)
	require.NotNil(t, sessGot.Session)
	assert.Equal(t, "sess1", aws.ToString(sessGot.Session.Id))

	ep, err := client.GetSessionEndpoint(ctx, &gluesdk.GetSessionEndpointInput{SessionId: aws.String("sess1")})
	require.NoError(t, err)
	require.NotNil(t, ep.SparkConnect)
	assert.NotEmpty(t, aws.ToString(ep.SparkConnect.Url))

	run, err := client.RunStatement(ctx, &gluesdk.RunStatementInput{
		SessionId: aws.String("sess1"), Code: aws.String("print(1)"),
	})
	require.NoError(t, err)
	assert.NotZero(t, run.Id)

	stmtGot, err := client.GetStatement(ctx, &gluesdk.GetStatementInput{
		SessionId: aws.String("sess1"), Id: run.Id,
	})
	require.NoError(t, err)
	require.NotNil(t, stmtGot.Statement)
	assert.Equal(t, run.Id, stmtGot.Statement.Id)
	assert.NotEmpty(t, string(stmtGot.Statement.State))

	list, err := client.ListStatements(ctx, &gluesdk.ListStatementsInput{SessionId: aws.String("sess1")})
	require.NoError(t, err)
	require.Len(t, list.Statements, 1)

	_, err = client.CancelStatement(ctx, &gluesdk.CancelStatementInput{
		SessionId: aws.String("sess1"), Id: run.Id,
	})
	require.NoError(t, err)
}

func testSchemaRegistryRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)
	ctx := t.Context()

	valid, err := client.CheckSchemaVersionValidity(ctx, &gluesdk.CheckSchemaVersionValidityInput{
		DataFormat: types.DataFormatJson, SchemaDefinition: aws.String(`{"type":"object"}`),
	})
	require.NoError(t, err)
	assert.True(t, valid.Valid)

	_, err = client.CreateRegistry(ctx, &gluesdk.CreateRegistryInput{RegistryName: aws.String("reg1")})
	require.NoError(t, err)

	schema, err := client.CreateSchema(ctx, &gluesdk.CreateSchemaInput{
		SchemaName:       aws.String("sch1"),
		RegistryId:       &types.RegistryId{RegistryName: aws.String("reg1")},
		DataFormat:       types.DataFormatJson,
		Compatibility:    types.CompatibilityBackward,
		SchemaDefinition: aws.String(`{"type":"object"}`),
	})
	require.NoError(t, err)
	require.NotNil(t, schema.SchemaVersionId)

	byDef, err := client.GetSchemaByDefinition(ctx, &gluesdk.GetSchemaByDefinitionInput{
		SchemaId:         &types.SchemaId{RegistryName: aws.String("reg1"), SchemaName: aws.String("sch1")},
		SchemaDefinition: aws.String(`{"type":"object"}`),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(schema.SchemaVersionId), aws.ToString(byDef.SchemaVersionId))

	reg2, err := client.RegisterSchemaVersion(ctx, &gluesdk.RegisterSchemaVersionInput{
		SchemaId:         &types.SchemaId{RegistryName: aws.String("reg1"), SchemaName: aws.String("sch1")},
		SchemaDefinition: aws.String(`{"type":"object","extra":true}`),
	})
	require.NoError(t, err)
	require.NotNil(t, reg2.VersionNumber)
	assert.EqualValues(t, 2, aws.ToInt64(reg2.VersionNumber))

	diff, err := client.GetSchemaVersionsDiff(ctx, &gluesdk.GetSchemaVersionsDiffInput{
		SchemaId:                  &types.SchemaId{RegistryName: aws.String("reg1"), SchemaName: aws.String("sch1")},
		FirstSchemaVersionNumber:  &types.SchemaVersionNumber{VersionNumber: aws.Int64(1)},
		SecondSchemaVersionNumber: &types.SchemaVersionNumber{VersionNumber: aws.Int64(2)},
		SchemaDiffType:            types.SchemaDiffTypeSyntaxDiff,
	})
	require.NoError(t, err)
	_ = diff

	removed, err := client.RemoveSchemaVersionMetadata(ctx, &gluesdk.RemoveSchemaVersionMetadataInput{
		SchemaVersionId: reg2.SchemaVersionId,
		MetadataKeyValue: &types.MetadataKeyValuePair{
			MetadataKey: aws.String("k"), MetadataValue: aws.String("v"),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "k", aws.ToString(removed.MetadataKey))

	delVers, err := client.DeleteSchemaVersions(ctx, &gluesdk.DeleteSchemaVersionsInput{
		SchemaId: &types.SchemaId{RegistryName: aws.String("reg1"), SchemaName: aws.String("sch1")},
		Versions: aws.String("1"),
	})
	require.NoError(t, err)
	assert.Empty(t, delVers.SchemaVersionErrors)
}

func testDataQualityRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)
	ctx := t.Context()

	_, err := client.CreateDataQualityRuleset(ctx, &gluesdk.CreateDataQualityRulesetInput{
		Name:    aws.String("dqr1"),
		Ruleset: aws.String("Rules = [ ColumnCount > 0 ]"),
	})
	require.NoError(t, err)

	_, err = client.UpdateDataQualityRuleset(ctx, &gluesdk.UpdateDataQualityRulesetInput{
		Name: aws.String("dqr1"), Description: aws.String("updated ruleset"),
	})
	require.NoError(t, err)

	got, err := client.GetDataQualityRuleset(ctx, &gluesdk.GetDataQualityRulesetInput{Name: aws.String("dqr1")})
	require.NoError(t, err)
	assert.Equal(t, "updated ruleset", aws.ToString(got.Description))

	run, err := client.StartDataQualityRulesetEvaluationRun(ctx, &gluesdk.StartDataQualityRulesetEvaluationRunInput{
		RulesetNames: []string{"dqr1"},
		DataSource: &types.DataSource{
			GlueTable: &types.GlueTable{DatabaseName: aws.String("db"), TableName: aws.String("tbl")},
		},
		Role: aws.String("r"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(run.RunId))

	batch, err := client.BatchGetDataQualityRulesetEvaluationRun(
		ctx,
		&gluesdk.BatchGetDataQualityRulesetEvaluationRunInput{RunIds: []string{aws.ToString(run.RunId), "missing"}},
	)
	require.NoError(t, err)
	require.Len(t, batch.Runs, 1)
	assert.Equal(t, []string{"missing"}, batch.RunsNotFound)

	_, err = client.CancelDataQualityRulesetEvaluationRun(ctx, &gluesdk.CancelDataQualityRulesetEvaluationRunInput{
		RunId: run.RunId,
	})
	require.NoError(t, err)

	_, err = client.DeleteDataQualityRuleset(ctx, &gluesdk.DeleteDataQualityRulesetInput{Name: aws.String("dqr1")})
	require.NoError(t, err)

	model, err := client.GetDataQualityModel(ctx, &gluesdk.GetDataQualityModelInput{ProfileId: aws.String("p1")})
	require.NoError(t, err)
	assert.Equal(t, "SUCCEEDED", string(model.Status))

	modelResult, err := client.GetDataQualityModelResult(ctx, &gluesdk.GetDataQualityModelResultInput{
		ProfileId: aws.String("p1"), StatisticId: aws.String("s1"),
	})
	require.NoError(t, err)
	assert.Empty(t, modelResult.Model)

	stats, err := client.ListDataQualityStatistics(ctx, &gluesdk.ListDataQualityStatisticsInput{})
	require.NoError(t, err)
	assert.Empty(t, stats.Statistics)

	_, err = client.PutDataQualityProfileAnnotation(ctx, &gluesdk.PutDataQualityProfileAnnotationInput{
		ProfileId: aws.String("p1"), InclusionAnnotation: types.InclusionAnnotationValueInclude,
	})
	require.NoError(t, err)

	dqResult, err := client.BatchGetDataQualityResult(ctx, &gluesdk.BatchGetDataQualityResultInput{
		ResultIds: []string{"missing-result"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"missing-result"}, dqResult.ResultsNotFound)

	_, err = client.GetDataQualityResult(
		ctx, &gluesdk.GetDataQualityResultInput{ResultId: aws.String("missing-result")},
	)
	require.Error(t, err)
}

func testMLTransformsRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)
	ctx := t.Context()

	_, err := client.CreateDatabase(ctx, &gluesdk.CreateDatabaseInput{
		DatabaseInput: &types.DatabaseInput{Name: aws.String("mldb")},
	})
	require.NoError(t, err)
	_, err = client.CreateTable(ctx, &gluesdk.CreateTableInput{
		DatabaseName: aws.String("mldb"),
		TableInput:   &types.TableInput{Name: aws.String("mltbl")},
	})
	require.NoError(t, err)

	created, err := client.CreateMLTransform(ctx, &gluesdk.CreateMLTransformInput{
		Name: aws.String("mlt1"),
		Role: aws.String("r"),
		InputRecordTables: []types.GlueTable{
			{DatabaseName: aws.String("mldb"), TableName: aws.String("mltbl")},
		},
		Parameters: &types.TransformParameters{TransformType: types.TransformTypeFindMatches},
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(created.TransformId))

	_, err = client.UpdateMLTransform(ctx, &gluesdk.UpdateMLTransformInput{
		TransformId: created.TransformId, Description: aws.String("updated transform"),
	})
	require.NoError(t, err)

	got, err := client.GetMLTransform(ctx, &gluesdk.GetMLTransformInput{TransformId: created.TransformId})
	require.NoError(t, err)
	assert.Equal(t, "updated transform", aws.ToString(got.Description))

	exportRun, err := client.StartExportLabelsTaskRun(ctx, &gluesdk.StartExportLabelsTaskRunInput{
		TransformId: created.TransformId, OutputS3Path: aws.String("s3://bucket/out"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(exportRun.TaskRunId))

	importRun, err := client.StartImportLabelsTaskRun(ctx, &gluesdk.StartImportLabelsTaskRunInput{
		TransformId: created.TransformId, InputS3Path: aws.String("s3://bucket/in"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(importRun.TaskRunId))

	evalRun, err := client.StartMLEvaluationTaskRun(ctx, &gluesdk.StartMLEvaluationTaskRunInput{
		TransformId: created.TransformId,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(evalRun.TaskRunId))

	labelRun, err := client.StartMLLabelingSetGenerationTaskRun(ctx, &gluesdk.StartMLLabelingSetGenerationTaskRunInput{
		TransformId:  created.TransformId,
		OutputS3Path: aws.String("s3://bucket/labels"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(labelRun.TaskRunId))

	_, err = client.CancelMLTaskRun(ctx, &gluesdk.CancelMLTaskRunInput{
		TransformId: created.TransformId, TaskRunId: evalRun.TaskRunId,
	})
	require.NoError(t, err)

	_, err = client.DeleteMLTransform(ctx, &gluesdk.DeleteMLTransformInput{TransformId: created.TransformId})
	require.NoError(t, err)
}

func testBlueprintsRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)
	ctx := t.Context()

	_, err := client.CreateBlueprint(ctx, &gluesdk.CreateBlueprintInput{
		Name: aws.String("bp1"), BlueprintLocation: aws.String("s3://bucket/blueprint"),
	})
	require.NoError(t, err)

	_, err = client.UpdateBlueprint(ctx, &gluesdk.UpdateBlueprintInput{
		Name: aws.String("bp1"), BlueprintLocation: aws.String("s3://bucket/blueprint2"),
	})
	require.NoError(t, err)

	got, err := client.GetBlueprint(ctx, &gluesdk.GetBlueprintInput{Name: aws.String("bp1")})
	require.NoError(t, err)
	assert.Equal(t, "s3://bucket/blueprint2", aws.ToString(got.Blueprint.BlueprintLocation))

	_, err = client.CreateBlueprint(ctx, &gluesdk.CreateBlueprintInput{
		Name: aws.String("bp2"), BlueprintLocation: aws.String("s3://bucket/blueprint"),
	})
	require.NoError(t, err)

	batch, err := client.BatchGetBlueprints(
		ctx, &gluesdk.BatchGetBlueprintsInput{Names: []string{"bp1", "bp2", "missing"}},
	)
	require.NoError(t, err)
	assert.Len(t, batch.Blueprints, 2)
	assert.Equal(t, []string{"missing"}, batch.MissingBlueprints)

	_, err = client.DeleteBlueprint(ctx, &gluesdk.DeleteBlueprintInput{Name: aws.String("bp2")})
	require.NoError(t, err)

	_, err = client.GetBlueprint(ctx, &gluesdk.GetBlueprintInput{Name: aws.String("bp2")})
	require.Error(t, err)
}

func testResourcePolicyRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)
	ctx := t.Context()

	_, err := client.PutResourcePolicy(ctx, &gluesdk.PutResourcePolicyInput{
		PolicyInJson: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
	})
	require.NoError(t, err)

	_, err = client.DeleteResourcePolicy(ctx, &gluesdk.DeleteResourcePolicyInput{})
	require.NoError(t, err)

	_, err = client.GetResourcePolicy(ctx, &gluesdk.GetResourcePolicyInput{})
	require.Error(t, err)
}

func testCatalogSecurityRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)
	ctx := t.Context()

	_, err := client.PutDataCatalogEncryptionSettings(ctx, &gluesdk.PutDataCatalogEncryptionSettingsInput{
		DataCatalogEncryptionSettings: &types.DataCatalogEncryptionSettings{
			EncryptionAtRest: &types.EncryptionAtRest{CatalogEncryptionMode: types.CatalogEncryptionModeSsekms},
		},
	})
	require.NoError(t, err)

	settings, err := client.GetDataCatalogEncryptionSettings(ctx, &gluesdk.GetDataCatalogEncryptionSettingsInput{})
	require.NoError(t, err)
	require.NotNil(t, settings.DataCatalogEncryptionSettings)
	assert.Equal(
		t,
		types.CatalogEncryptionModeSsekms,
		settings.DataCatalogEncryptionSettings.EncryptionAtRest.CatalogEncryptionMode,
	)

	created, err := client.CreateSecurityConfiguration(ctx, &gluesdk.CreateSecurityConfigurationInput{
		Name: aws.String("sc1"),
		EncryptionConfiguration: &types.EncryptionConfiguration{
			JobBookmarksEncryption: &types.JobBookmarksEncryption{
				JobBookmarksEncryptionMode: types.JobBookmarksEncryptionModeCsekms,
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "sc1", aws.ToString(created.Name))

	got, err := client.GetSecurityConfiguration(ctx, &gluesdk.GetSecurityConfigurationInput{Name: aws.String("sc1")})
	require.NoError(t, err)
	require.NotNil(t, got.SecurityConfiguration.EncryptionConfiguration.JobBookmarksEncryption)
	assert.Equal(
		t,
		types.JobBookmarksEncryptionModeCsekms,
		got.SecurityConfiguration.EncryptionConfiguration.JobBookmarksEncryption.JobBookmarksEncryptionMode,
	)

	_, err = client.DeleteSecurityConfiguration(ctx, &gluesdk.DeleteSecurityConfigurationInput{Name: aws.String("sc1")})
	require.NoError(t, err)
}

func testCatalogsRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)
	ctx := t.Context()

	_, err := client.CreateCatalog(ctx, &gluesdk.CreateCatalogInput{
		Name:         aws.String("cat1"),
		CatalogInput: &types.CatalogInput{Description: aws.String("catalog one")},
	})
	require.NoError(t, err)

	_, err = client.UpdateCatalog(ctx, &gluesdk.UpdateCatalogInput{
		CatalogId:    aws.String("cat1"),
		CatalogInput: &types.CatalogInput{Description: aws.String("catalog one updated")},
	})
	require.NoError(t, err)

	got, err := client.GetCatalog(ctx, &gluesdk.GetCatalogInput{CatalogId: aws.String("cat1")})
	require.NoError(t, err)
	assert.Equal(t, "catalog one updated", aws.ToString(got.Catalog.Description))

	status, err := client.GetCatalogImportStatus(
		ctx, &gluesdk.GetCatalogImportStatusInput{CatalogId: aws.String("cat1")},
	)
	require.NoError(t, err)
	assert.NotNil(t, status.ImportStatus)

	_, err = client.ImportCatalogToGlue(ctx, &gluesdk.ImportCatalogToGlueInput{CatalogId: aws.String("cat1")})
	require.NoError(t, err)

	exportCfg, err := client.PutDataCatalogExportConfiguration(ctx, &gluesdk.PutDataCatalogExportConfigurationInput{
		ExportSetting: types.ExportSettingEnabled,
	})
	require.NoError(t, err)
	assert.Equal(t, types.ExportSettingEnabled, exportCfg.ExportSetting)

	gotExport, err := client.GetDataCatalogExportConfiguration(ctx, &gluesdk.GetDataCatalogExportConfigurationInput{})
	require.NoError(t, err)
	assert.Equal(t, types.ExportSettingEnabled, gotExport.ExportSetting)

	_, err = client.DeleteCatalog(ctx, &gluesdk.DeleteCatalogInput{CatalogId: aws.String("cat1")})
	require.NoError(t, err)

	_, err = client.GetCatalog(ctx, &gluesdk.GetCatalogInput{CatalogId: aws.String("cat1")})
	require.Error(t, err)
}

func testColumnStatisticsRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)
	ctx := t.Context()

	_, err := client.CreateDatabase(ctx, &gluesdk.CreateDatabaseInput{
		DatabaseInput: &types.DatabaseInput{Name: aws.String("csdb")},
	})
	require.NoError(t, err)
	_, err = client.CreateTable(ctx, &gluesdk.CreateTableInput{
		DatabaseName: aws.String("csdb"),
		TableInput: &types.TableInput{
			Name:          aws.String("cstbl"),
			PartitionKeys: []types.Column{{Name: aws.String("dt")}},
		},
	})
	require.NoError(t, err)
	_, err = client.CreatePartition(ctx, &gluesdk.CreatePartitionInput{
		DatabaseName:   aws.String("csdb"),
		TableName:      aws.String("cstbl"),
		PartitionInput: &types.PartitionInput{Values: []string{"2024-01-01"}},
	})
	require.NoError(t, err)

	boolStats := []types.ColumnStatistics{{
		ColumnName:   aws.String("active"),
		ColumnType:   aws.String("boolean"),
		AnalyzedTime: aws.Time(time.Now().UTC()),
		StatisticsData: &types.ColumnStatisticsData{
			Type: types.ColumnStatisticsTypeBoolean,
			BooleanColumnStatisticsData: &types.BooleanColumnStatisticsData{
				NumberOfTrues: 3, NumberOfFalses: 1, NumberOfNulls: 0,
			},
		},
	}}

	_, err = client.UpdateColumnStatisticsForTable(ctx, &gluesdk.UpdateColumnStatisticsForTableInput{
		DatabaseName: aws.String("csdb"), TableName: aws.String("cstbl"), ColumnStatisticsList: boolStats,
	})
	require.NoError(t, err)

	forTable, err := client.GetColumnStatisticsForTable(ctx, &gluesdk.GetColumnStatisticsForTableInput{
		DatabaseName: aws.String("csdb"), TableName: aws.String("cstbl"), ColumnNames: []string{"active"},
	})
	require.NoError(t, err)
	require.Len(t, forTable.ColumnStatisticsList, 1)
	assert.Equal(t, "active", aws.ToString(forTable.ColumnStatisticsList[0].ColumnName))

	_, err = client.UpdateColumnStatisticsForPartition(ctx, &gluesdk.UpdateColumnStatisticsForPartitionInput{
		DatabaseName: aws.String("csdb"), TableName: aws.String("cstbl"),
		PartitionValues: []string{"2024-01-01"}, ColumnStatisticsList: boolStats,
	})
	require.NoError(t, err)

	forPart, err := client.GetColumnStatisticsForPartition(ctx, &gluesdk.GetColumnStatisticsForPartitionInput{
		DatabaseName: aws.String("csdb"), TableName: aws.String("cstbl"),
		PartitionValues: []string{"2024-01-01"}, ColumnNames: []string{"active"},
	})
	require.NoError(t, err)
	require.Len(t, forPart.ColumnStatisticsList, 1)

	_, err = client.DeleteColumnStatisticsForPartition(ctx, &gluesdk.DeleteColumnStatisticsForPartitionInput{
		DatabaseName: aws.String("csdb"), TableName: aws.String("cstbl"),
		PartitionValues: []string{"2024-01-01"}, ColumnName: aws.String("active"),
	})
	require.NoError(t, err)

	_, err = client.DeleteColumnStatisticsForTable(ctx, &gluesdk.DeleteColumnStatisticsForTableInput{
		DatabaseName: aws.String("csdb"), TableName: aws.String("cstbl"), ColumnName: aws.String("active"),
	})
	require.NoError(t, err)

	_, err = client.CreateColumnStatisticsTaskSettings(ctx, &gluesdk.CreateColumnStatisticsTaskSettingsInput{
		DatabaseName: aws.String("csdb"), TableName: aws.String("cstbl"), Role: aws.String("r"),
	})
	require.NoError(t, err)

	settings, err := client.GetColumnStatisticsTaskSettings(ctx, &gluesdk.GetColumnStatisticsTaskSettingsInput{
		DatabaseName: aws.String("csdb"), TableName: aws.String("cstbl"),
	})
	require.NoError(t, err)
	require.NotNil(t, settings.ColumnStatisticsTaskSettings)

	_, err = client.UpdateColumnStatisticsTaskSettings(ctx, &gluesdk.UpdateColumnStatisticsTaskSettingsInput{
		DatabaseName: aws.String("csdb"), TableName: aws.String("cstbl"), Role: aws.String("r2"),
	})
	require.NoError(t, err)

	_, err = client.StartColumnStatisticsTaskRunSchedule(ctx, &gluesdk.StartColumnStatisticsTaskRunScheduleInput{
		DatabaseName: aws.String("csdb"), TableName: aws.String("cstbl"),
	})
	require.NoError(t, err)

	_, err = client.StopColumnStatisticsTaskRunSchedule(ctx, &gluesdk.StopColumnStatisticsTaskRunScheduleInput{
		DatabaseName: aws.String("csdb"), TableName: aws.String("cstbl"),
	})
	require.NoError(t, err)

	_, err = client.DeleteColumnStatisticsTaskSettings(ctx, &gluesdk.DeleteColumnStatisticsTaskSettingsInput{
		DatabaseName: aws.String("csdb"), TableName: aws.String("cstbl"),
	})
	require.NoError(t, err)
}

func testIntegrationsRealClient(t *testing.T) {
	t.Helper()

	client := newRealClient(t)
	ctx := t.Context()

	resourceArn := "arn:aws:glue:" + testRegion + ":" + testAccountID + ":database/integdb"

	created, err := client.CreateIntegrationResourceProperty(ctx, &gluesdk.CreateIntegrationResourcePropertyInput{
		ResourceArn: aws.String(resourceArn),
		SourceProcessingProperties: &types.SourceProcessingProperties{
			RoleArn: aws.String("arn:aws:iam::" + testAccountID + ":role/glue-role"),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, resourceArn, aws.ToString(created.ResourceArn))
	require.NotNil(t, created.SourceProcessingProperties)
	assert.Equal(
		t,
		"arn:aws:iam::"+testAccountID+":role/glue-role",
		aws.ToString(created.SourceProcessingProperties.RoleArn),
	)

	got, err := client.GetIntegrationResourceProperty(ctx, &gluesdk.GetIntegrationResourcePropertyInput{
		ResourceArn: aws.String(resourceArn),
	})
	require.NoError(t, err)
	require.NotNil(t, got.SourceProcessingProperties)
	assert.Equal(
		t, "arn:aws:iam::"+testAccountID+":role/glue-role", aws.ToString(got.SourceProcessingProperties.RoleArn),
	)

	_, err = client.UpdateIntegrationResourceProperty(ctx, &gluesdk.UpdateIntegrationResourcePropertyInput{
		ResourceArn: aws.String(resourceArn),
		SourceProcessingProperties: &types.SourceProcessingProperties{
			RoleArn: aws.String("arn:aws:iam::" + testAccountID + ":role/glue-role2"),
		},
	})
	require.NoError(t, err)

	reGot, err := client.GetIntegrationResourceProperty(ctx, &gluesdk.GetIntegrationResourcePropertyInput{
		ResourceArn: aws.String(resourceArn),
	})
	require.NoError(t, err)
	require.NotNil(t, reGot.SourceProcessingProperties)
	assert.Equal(
		t,
		"arn:aws:iam::"+testAccountID+":role/glue-role2",
		aws.ToString(reGot.SourceProcessingProperties.RoleArn),
	)

	_, err = client.CreateIntegrationTableProperties(ctx, &gluesdk.CreateIntegrationTablePropertiesInput{
		ResourceArn: aws.String(resourceArn),
		TableName:   aws.String("integtbl"),
		SourceTableConfig: &types.SourceTableConfig{
			Fields: []string{"id", "name"},
		},
	})
	require.NoError(t, err)

	tableProps, err := client.GetIntegrationTableProperties(ctx, &gluesdk.GetIntegrationTablePropertiesInput{
		ResourceArn: aws.String(resourceArn), TableName: aws.String("integtbl"),
	})
	require.NoError(t, err)
	require.NotNil(t, tableProps.SourceTableConfig)
	assert.Equal(t, []string{"id", "name"}, tableProps.SourceTableConfig.Fields)

	_, err = client.UpdateIntegrationTableProperties(ctx, &gluesdk.UpdateIntegrationTablePropertiesInput{
		ResourceArn: aws.String(resourceArn),
		TableName:   aws.String("integtbl"),
		SourceTableConfig: &types.SourceTableConfig{
			Fields: []string{"id"},
		},
	})
	require.NoError(t, err)

	_, err = client.DeleteIntegrationTableProperties(ctx, &gluesdk.DeleteIntegrationTablePropertiesInput{
		ResourceArn: aws.String(resourceArn), TableName: aws.String("integtbl"),
	})
	require.NoError(t, err)

	_, err = client.DeleteIntegrationResourceProperty(ctx, &gluesdk.DeleteIntegrationResourcePropertyInput{
		ResourceArn: aws.String(resourceArn),
	})
	require.NoError(t, err)

	_, err = client.GetIntegrationResourceProperty(ctx, &gluesdk.GetIntegrationResourcePropertyInput{
		ResourceArn: aws.String(resourceArn),
	})
	require.Error(t, err)
}
