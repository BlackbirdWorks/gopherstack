package glue_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	smithy "github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// TestReqFieldSlice1_Glue_RealClient proves the gopherstack-xhu2t slice 1
// fixes for glue's tier-1 reqfielddiff findings: CatalogId scoping across
// database/table/connection/UDF/partition/column-statistics/materialized-
// view ops, CreateJob.AllocatedCapacity/JobMode, StartJobRun.
// AllocatedCapacity, CreateSession.IdleTimeout, GetPlan.
// AdditionalPlanOptionsMap, UpdateTable.SkipArchive, and StartDataQuality*.
// NumberOfWorkers/Timeout. Each subtest drives the real aws-sdk-go-v2 client
// and asserts an observable effect.
func TestReqFieldSlice1_Glue_RealClient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T)
		name string
	}{
		{testCatalogIDDatabaseScoping, "catalog_id_database"},
		{testCatalogIDTableScoping, "catalog_id_table"},
		{testCatalogIDConnectionScoping, "catalog_id_connection"},
		{testCatalogIDUDFScoping, "catalog_id_udf"},
		{testCatalogIDPartitionScoping, "catalog_id_partition"},
		{testCatalogIDColumnStatisticsScoping, "catalog_id_column_statistics"},
		{testCatalogIDMaterializedViewScoping, "catalog_id_materialized_view"},
		{testCatalogIDStartColumnStatisticsTaskRun, "catalog_id_start_column_statistics_task_run"},
		{testCreateJobAllocatedCapacityAndJobMode, "create_job_allocated_capacity_and_job_mode"},
		{testStartJobRunAllocatedCapacity, "start_job_run_allocated_capacity"},
		{testCreateSessionIdleTimeout, "create_session_idle_timeout"},
		{testGetPlanAdditionalPlanOptionsMap, "get_plan_additional_plan_options_map"},
		{testUpdateTableSkipArchive, "update_table_skip_archive"},
		{testStartDataQualityRunsWorkersAndTimeout, "start_data_quality_runs_workers_and_timeout"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t)
		})
	}
}

func newReqFieldSlice1GlueClient(t *testing.T) *gluesdk.Client {
	t.Helper()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)

	return newTestGlueClient(t, glue.NewHandler(backend))
}

const otherCatalogID = "999999999999"

func testCatalogIDDatabaseScoping(t *testing.T) {
	t.Helper()

	client := newReqFieldSlice1GlueClient(t)
	ctx := t.Context()

	_, err := client.CreateDatabase(ctx, &gluesdk.CreateDatabaseInput{
		CatalogId:     aws.String(otherCatalogID),
		DatabaseInput: &types.DatabaseInput{Name: aws.String("db1")},
	})
	require.NoError(t, err)

	got, err := client.GetDatabase(ctx, &gluesdk.GetDatabaseInput{Name: aws.String("db1")})
	require.NoError(t, err)
	assert.Equal(t, otherCatalogID, aws.ToString(got.Database.CatalogId))

	_, err = client.GetDatabase(ctx, &gluesdk.GetDatabaseInput{
		Name:      aws.String("db1"),
		CatalogId: aws.String(testAccountID),
	})
	assertEntityNotFound(t, err)

	list, err := client.GetDatabases(ctx, &gluesdk.GetDatabasesInput{CatalogId: aws.String(otherCatalogID)})
	require.NoError(t, err)
	assert.Len(t, list.DatabaseList, 1)

	list, err = client.GetDatabases(ctx, &gluesdk.GetDatabasesInput{CatalogId: aws.String(testAccountID)})
	require.NoError(t, err)
	assert.Empty(t, list.DatabaseList)

	_, err = client.DeleteDatabase(ctx, &gluesdk.DeleteDatabaseInput{
		Name:      aws.String("db1"),
		CatalogId: aws.String(testAccountID),
	})
	assertEntityNotFound(t, err)

	_, err = client.UpdateDatabase(ctx, &gluesdk.UpdateDatabaseInput{
		Name:          aws.String("db1"),
		CatalogId:     aws.String(testAccountID),
		DatabaseInput: &types.DatabaseInput{Name: aws.String("db1")},
	})
	assertEntityNotFound(t, err)

	_, err = client.DeleteDatabase(ctx, &gluesdk.DeleteDatabaseInput{
		Name:      aws.String("db1"),
		CatalogId: aws.String(otherCatalogID),
	})
	require.NoError(t, err)
}

func testCatalogIDTableScoping(t *testing.T) {
	t.Helper()

	client := newReqFieldSlice1GlueClient(t)
	ctx := t.Context()

	_, err := client.CreateDatabase(ctx, &gluesdk.CreateDatabaseInput{
		DatabaseInput: &types.DatabaseInput{Name: aws.String("tdb")},
	})
	require.NoError(t, err)

	_, err = client.CreateTable(ctx, &gluesdk.CreateTableInput{
		DatabaseName: aws.String("tdb"),
		CatalogId:    aws.String(otherCatalogID),
		TableInput:   &types.TableInput{Name: aws.String("t1")},
	})
	require.NoError(t, err)

	got, err := client.GetTable(ctx, &gluesdk.GetTableInput{DatabaseName: aws.String("tdb"), Name: aws.String("t1")})
	require.NoError(t, err)
	assert.Equal(t, otherCatalogID, aws.ToString(got.Table.CatalogId))

	_, err = client.GetTable(ctx, &gluesdk.GetTableInput{
		DatabaseName: aws.String("tdb"), Name: aws.String("t1"), CatalogId: aws.String(testAccountID),
	})
	assertEntityNotFound(t, err)

	tables, err := client.GetTables(ctx, &gluesdk.GetTablesInput{
		DatabaseName: aws.String("tdb"), CatalogId: aws.String(otherCatalogID),
	})
	require.NoError(t, err)
	assert.Len(t, tables.TableList, 1)

	_, err = client.DeleteTable(ctx, &gluesdk.DeleteTableInput{
		DatabaseName: aws.String("tdb"), Name: aws.String("t1"), CatalogId: aws.String(testAccountID),
	})
	assertEntityNotFound(t, err)

	batchErrs, err := client.BatchDeleteTable(ctx, &gluesdk.BatchDeleteTableInput{
		DatabaseName:   aws.String("tdb"),
		TablesToDelete: []string{"t1"},
		CatalogId:      aws.String(testAccountID),
	})
	require.NoError(t, err)
	require.Len(t, batchErrs.Errors, 1)
	assert.Equal(t, "t1", aws.ToString(batchErrs.Errors[0].TableName))
}

func testCatalogIDConnectionScoping(t *testing.T) {
	t.Helper()

	client := newReqFieldSlice1GlueClient(t)
	ctx := t.Context()

	_, err := client.CreateConnection(ctx, &gluesdk.CreateConnectionInput{
		CatalogId: aws.String(otherCatalogID),
		ConnectionInput: &types.ConnectionInput{
			Name:                 aws.String("c1"),
			ConnectionType:       types.ConnectionTypeJdbc,
			ConnectionProperties: map[string]string{"JDBC_CONNECTION_URL": "jdbc:mysql://host/db"},
		},
	})
	require.NoError(t, err)

	// types.Connection has no CatalogId member on the real wire (glue@v1.157.0
	// types/types.go) -- CatalogId here is a request-side scoping parameter
	// only, proven below via the mismatch behavior it produces, not an echo.
	_, err = client.GetConnection(ctx, &gluesdk.GetConnectionInput{Name: aws.String("c1")})
	require.NoError(t, err)

	_, err = client.GetConnection(ctx, &gluesdk.GetConnectionInput{
		Name: aws.String("c1"), CatalogId: aws.String(testAccountID),
	})
	assertEntityNotFound(t, err)

	_, err = client.DeleteConnection(ctx, &gluesdk.DeleteConnectionInput{
		ConnectionName: aws.String("c1"), CatalogId: aws.String(testAccountID),
	})
	assertEntityNotFound(t, err)

	batchOut, err := client.BatchDeleteConnection(ctx, &gluesdk.BatchDeleteConnectionInput{
		ConnectionNameList: []string{"c1"},
		CatalogId:          aws.String(testAccountID),
	})
	require.NoError(t, err)
	assert.Contains(t, batchOut.Errors, "c1")
}

func testCatalogIDUDFScoping(t *testing.T) {
	t.Helper()

	client := newReqFieldSlice1GlueClient(t)
	ctx := t.Context()

	_, err := client.CreateDatabase(ctx, &gluesdk.CreateDatabaseInput{
		DatabaseInput: &types.DatabaseInput{Name: aws.String("udb")},
	})
	require.NoError(t, err)

	_, err = client.CreateUserDefinedFunction(ctx, &gluesdk.CreateUserDefinedFunctionInput{
		DatabaseName: aws.String("udb"),
		CatalogId:    aws.String(otherCatalogID),
		FunctionInput: &types.UserDefinedFunctionInput{
			FunctionName: aws.String("f1"),
		},
	})
	require.NoError(t, err)

	got, err := client.GetUserDefinedFunction(ctx, &gluesdk.GetUserDefinedFunctionInput{
		DatabaseName: aws.String("udb"), FunctionName: aws.String("f1"),
	})
	require.NoError(t, err)
	assert.Equal(t, otherCatalogID, aws.ToString(got.UserDefinedFunction.CatalogId))

	_, err = client.GetUserDefinedFunction(ctx, &gluesdk.GetUserDefinedFunctionInput{
		DatabaseName: aws.String("udb"), FunctionName: aws.String("f1"), CatalogId: aws.String(testAccountID),
	})
	assertEntityNotFound(t, err)

	list, err := client.GetUserDefinedFunctions(ctx, &gluesdk.GetUserDefinedFunctionsInput{
		DatabaseName: aws.String("udb"), Pattern: aws.String("f.*"), CatalogId: aws.String(otherCatalogID),
	})
	require.NoError(t, err)
	assert.Len(t, list.UserDefinedFunctions, 1)

	_, err = client.DeleteUserDefinedFunction(ctx, &gluesdk.DeleteUserDefinedFunctionInput{
		DatabaseName: aws.String("udb"), FunctionName: aws.String("f1"), CatalogId: aws.String(testAccountID),
	})
	assertEntityNotFound(t, err)
}

func testCatalogIDPartitionScoping(t *testing.T) {
	t.Helper()

	client := newReqFieldSlice1GlueClient(t)
	ctx := t.Context()

	_, err := client.CreateDatabase(ctx, &gluesdk.CreateDatabaseInput{
		DatabaseInput: &types.DatabaseInput{Name: aws.String("pdb")},
	})
	require.NoError(t, err)
	_, err = client.CreateTable(ctx, &gluesdk.CreateTableInput{
		DatabaseName: aws.String("pdb"),
		TableInput:   &types.TableInput{Name: aws.String("pt")},
	})
	require.NoError(t, err)
	_, err = client.CreatePartition(ctx, &gluesdk.CreatePartitionInput{
		DatabaseName:   aws.String("pdb"),
		TableName:      aws.String("pt"),
		PartitionInput: &types.PartitionInput{Values: []string{"2024"}},
	})
	require.NoError(t, err)

	// Partitions are always created under the backend's own account ID
	// (CreatePartition doesn't take CatalogId -- tier-3, out of this
	// slice's scope), so a mismatched CatalogId on any read/write op is
	// observable as EntityNotFoundException.
	_, err = client.GetPartition(ctx, &gluesdk.GetPartitionInput{
		DatabaseName: aws.String("pdb"), TableName: aws.String("pt"),
		PartitionValues: []string{"2024"}, CatalogId: aws.String(otherCatalogID),
	})
	assertEntityNotFound(t, err)

	got, err := client.GetPartition(ctx, &gluesdk.GetPartitionInput{
		DatabaseName: aws.String("pdb"), TableName: aws.String("pt"),
		PartitionValues: []string{"2024"}, CatalogId: aws.String(testAccountID),
	})
	require.NoError(t, err)
	assert.Equal(t, testAccountID, aws.ToString(got.Partition.CatalogId))

	list, err := client.GetPartitions(ctx, &gluesdk.GetPartitionsInput{
		DatabaseName: aws.String("pdb"), TableName: aws.String("pt"), CatalogId: aws.String(otherCatalogID),
	})
	require.NoError(t, err)
	assert.Empty(t, list.Partitions)

	batchGot, err := client.BatchGetPartition(ctx, &gluesdk.BatchGetPartitionInput{
		DatabaseName: aws.String("pdb"), TableName: aws.String("pt"),
		PartitionsToGet: []types.PartitionValueList{{Values: []string{"2024"}}},
		CatalogId:       aws.String(otherCatalogID),
	})
	require.NoError(t, err)
	assert.Empty(t, batchGot.Partitions)
	assert.Len(t, batchGot.UnprocessedKeys, 1)

	_, err = client.UpdatePartition(ctx, &gluesdk.UpdatePartitionInput{
		DatabaseName: aws.String("pdb"), TableName: aws.String("pt"),
		PartitionValueList: []string{"2024"},
		PartitionInput:     &types.PartitionInput{Values: []string{"2024"}},
		CatalogId:          aws.String(otherCatalogID),
	})
	assertEntityNotFound(t, err)

	_, err = client.DeletePartition(ctx, &gluesdk.DeletePartitionInput{
		DatabaseName: aws.String("pdb"), TableName: aws.String("pt"),
		PartitionValues: []string{"2024"}, CatalogId: aws.String(otherCatalogID),
	})
	assertEntityNotFound(t, err)

	batchDelErrs, err := client.BatchDeletePartition(ctx, &gluesdk.BatchDeletePartitionInput{
		DatabaseName: aws.String("pdb"), TableName: aws.String("pt"),
		PartitionsToDelete: []types.PartitionValueList{{Values: []string{"2024"}}},
		CatalogId:          aws.String(otherCatalogID),
	})
	require.NoError(t, err)
	assert.Len(t, batchDelErrs.Errors, 1)

	_, err = client.GetUnfilteredPartitionsMetadata(ctx, &gluesdk.GetUnfilteredPartitionsMetadataInput{
		DatabaseName: aws.String("pdb"), TableName: aws.String("pt"), CatalogId: aws.String(otherCatalogID),
		SupportedPermissionTypes: []types.PermissionType{types.PermissionTypeColumnPermission},
	})
	assertEntityNotFound(t, err)

	unfiltered, err := client.GetUnfilteredPartitionsMetadata(ctx, &gluesdk.GetUnfilteredPartitionsMetadataInput{
		DatabaseName: aws.String("pdb"), TableName: aws.String("pt"), CatalogId: aws.String(testAccountID),
		SupportedPermissionTypes: []types.PermissionType{types.PermissionTypeColumnPermission},
	})
	require.NoError(t, err)
	assert.Len(t, unfiltered.UnfilteredPartitions, 1)
}

func testCatalogIDColumnStatisticsScoping(t *testing.T) {
	t.Helper()

	client := newReqFieldSlice1GlueClient(t)
	ctx := t.Context()

	_, err := client.CreateDatabase(ctx, &gluesdk.CreateDatabaseInput{
		DatabaseInput: &types.DatabaseInput{Name: aws.String("csdb")},
	})
	require.NoError(t, err)
	_, err = client.CreateTable(ctx, &gluesdk.CreateTableInput{
		DatabaseName: aws.String("csdb"),
		TableInput:   &types.TableInput{Name: aws.String("cst")},
	})
	require.NoError(t, err)

	_, err = client.GetColumnStatisticsForTable(ctx, &gluesdk.GetColumnStatisticsForTableInput{
		DatabaseName: aws.String("csdb"), TableName: aws.String("cst"),
		ColumnNames: []string{"col"}, CatalogId: aws.String(otherCatalogID),
	})
	assertEntityNotFound(t, err)

	_, err = client.UpdateColumnStatisticsForTable(ctx, &gluesdk.UpdateColumnStatisticsForTableInput{
		DatabaseName: aws.String("csdb"), TableName: aws.String("cst"), CatalogId: aws.String(otherCatalogID),
		ColumnStatisticsList: []types.ColumnStatistics{{
			ColumnName:     aws.String("col"),
			ColumnType:     aws.String("string"),
			AnalyzedTime:   aws.Time(time.Now()),
			StatisticsData: &types.ColumnStatisticsData{Type: types.ColumnStatisticsTypeString},
		}},
	})
	assertEntityNotFound(t, err)

	_, err = client.DeleteColumnStatisticsForTable(ctx, &gluesdk.DeleteColumnStatisticsForTableInput{
		DatabaseName: aws.String("csdb"), TableName: aws.String("cst"),
		ColumnName: aws.String("col"), CatalogId: aws.String(otherCatalogID),
	})
	assertEntityNotFound(t, err)
}

func testCatalogIDMaterializedViewScoping(t *testing.T) {
	t.Helper()

	client := newReqFieldSlice1GlueClient(t)
	ctx := t.Context()

	_, err := client.CreateDatabase(ctx, &gluesdk.CreateDatabaseInput{
		DatabaseInput: &types.DatabaseInput{Name: aws.String("mvdb")},
	})
	require.NoError(t, err)
	_, err = client.CreateTable(ctx, &gluesdk.CreateTableInput{
		DatabaseName: aws.String("mvdb"),
		TableInput:   &types.TableInput{Name: aws.String("mvt")},
	})
	require.NoError(t, err)

	_, err = client.StartMaterializedViewRefreshTaskRun(ctx, &gluesdk.StartMaterializedViewRefreshTaskRunInput{
		DatabaseName: aws.String("mvdb"), TableName: aws.String("mvt"), CatalogId: aws.String(otherCatalogID),
	})
	assertEntityNotFound(t, err)

	started, err := client.StartMaterializedViewRefreshTaskRun(ctx, &gluesdk.StartMaterializedViewRefreshTaskRunInput{
		DatabaseName: aws.String("mvdb"), TableName: aws.String("mvt"), CatalogId: aws.String(testAccountID),
	})
	require.NoError(t, err)

	_, err = client.GetMaterializedViewRefreshTaskRun(ctx, &gluesdk.GetMaterializedViewRefreshTaskRunInput{
		MaterializedViewRefreshTaskRunId: started.MaterializedViewRefreshTaskRunId,
		CatalogId:                        aws.String(otherCatalogID),
	})
	assertEntityNotFound(t, err)

	_, err = client.StopMaterializedViewRefreshTaskRun(ctx, &gluesdk.StopMaterializedViewRefreshTaskRunInput{
		DatabaseName: aws.String("mvdb"), TableName: aws.String("mvt"), CatalogId: aws.String(otherCatalogID),
	})
	assertEntityNotFound(t, err)
}

func testCatalogIDStartColumnStatisticsTaskRun(t *testing.T) {
	t.Helper()

	client := newReqFieldSlice1GlueClient(t)
	ctx := t.Context()

	_, err := client.CreateDatabase(ctx, &gluesdk.CreateDatabaseInput{
		DatabaseInput: &types.DatabaseInput{Name: aws.String("cstrdb")},
	})
	require.NoError(t, err)
	_, err = client.CreateTable(ctx, &gluesdk.CreateTableInput{
		DatabaseName: aws.String("cstrdb"),
		TableInput:   &types.TableInput{Name: aws.String("cstrt")},
	})
	require.NoError(t, err)

	_, err = client.StartColumnStatisticsTaskRun(ctx, &gluesdk.StartColumnStatisticsTaskRunInput{
		DatabaseName: aws.String("cstrdb"), TableName: aws.String("cstrt"),
		Role: aws.String("role1"), CatalogID: aws.String(otherCatalogID),
	})
	assertEntityNotFound(t, err)

	_, err = client.StartColumnStatisticsTaskRun(ctx, &gluesdk.StartColumnStatisticsTaskRunInput{
		DatabaseName: aws.String("cstrdb"), TableName: aws.String("cstrt"),
		Role: aws.String("role1"), CatalogID: aws.String(testAccountID),
	})
	require.NoError(t, err)
}

func testCreateJobAllocatedCapacityAndJobMode(t *testing.T) {
	t.Helper()

	client := newReqFieldSlice1GlueClient(t)
	ctx := t.Context()

	_, err := client.CreateJob(ctx, &gluesdk.CreateJobInput{
		Name:              aws.String("j1"),
		Role:              aws.String("role1"),
		Command:           &types.JobCommand{Name: aws.String("glueetl")},
		AllocatedCapacity: 7, //nolint:staticcheck // deprecated field is exactly what's under test
	})
	require.NoError(t, err)

	got, err := client.GetJob(ctx, &gluesdk.GetJobInput{JobName: aws.String("j1")})
	require.NoError(t, err)
	assert.InDelta(t, 7.0, aws.ToFloat64(got.Job.MaxCapacity), 0.001)
	assert.Equal(t, types.JobModeScript, got.Job.JobMode)

	_, err = client.CreateJob(ctx, &gluesdk.CreateJobInput{
		Name:    aws.String("j2"),
		Role:    aws.String("role1"),
		Command: &types.JobCommand{Name: aws.String("glueetl")},
		JobMode: types.JobModeVisual,
	})
	require.NoError(t, err)

	got2, err := client.GetJob(ctx, &gluesdk.GetJobInput{JobName: aws.String("j2")})
	require.NoError(t, err)
	assert.Equal(t, types.JobModeVisual, got2.Job.JobMode)
}

func testStartJobRunAllocatedCapacity(t *testing.T) {
	t.Helper()

	client := newReqFieldSlice1GlueClient(t)
	ctx := t.Context()

	_, err := client.CreateJob(ctx, &gluesdk.CreateJobInput{
		Name:    aws.String("jr1"),
		Role:    aws.String("role1"),
		Command: &types.JobCommand{Name: aws.String("glueetl")},
	})
	require.NoError(t, err)

	run, err := client.StartJobRun(ctx, &gluesdk.StartJobRunInput{
		JobName:           aws.String("jr1"),
		AllocatedCapacity: 4, //nolint:staticcheck // deprecated field is exactly what's under test
	})
	require.NoError(t, err)

	got, err := client.GetJobRun(ctx, &gluesdk.GetJobRunInput{
		JobName: aws.String("jr1"), RunId: run.JobRunId,
	})
	require.NoError(t, err)
	assert.InDelta(t, 4.0, aws.ToFloat64(got.JobRun.MaxCapacity), 0.001)
}

func testCreateSessionIdleTimeout(t *testing.T) {
	t.Helper()

	client := newReqFieldSlice1GlueClient(t)
	ctx := t.Context()

	_, err := client.CreateSession(ctx, &gluesdk.CreateSessionInput{
		Id:          aws.String("s1"),
		Role:        aws.String("role1"),
		Command:     &types.SessionCommand{Name: aws.String("glueetl")},
		IdleTimeout: aws.Int32(42),
	})
	require.NoError(t, err)

	got, err := client.GetSession(ctx, &gluesdk.GetSessionInput{Id: aws.String("s1")})
	require.NoError(t, err)
	assert.Equal(t, int32(42), aws.ToInt32(got.Session.IdleTimeout))
}

func testGetPlanAdditionalPlanOptionsMap(t *testing.T) {
	t.Helper()

	client := newReqFieldSlice1GlueClient(t)
	ctx := t.Context()

	got, err := client.GetPlan(ctx, &gluesdk.GetPlanInput{
		Language:                 types.LanguagePython,
		Source:                   &types.CatalogEntry{DatabaseName: aws.String("d"), TableName: aws.String("t")},
		Mapping:                  []types.MappingEntry{},
		AdditionalPlanOptionsMap: map[string]string{"inferSchema": "true"},
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(got.PythonScript), "inferSchema: true")
}

func testUpdateTableSkipArchive(t *testing.T) {
	t.Helper()

	client := newReqFieldSlice1GlueClient(t)
	ctx := t.Context()

	_, err := client.CreateDatabase(ctx, &gluesdk.CreateDatabaseInput{
		DatabaseInput: &types.DatabaseInput{Name: aws.String("skdb")},
	})
	require.NoError(t, err)
	_, err = client.CreateTable(ctx, &gluesdk.CreateTableInput{
		DatabaseName: aws.String("skdb"),
		TableInput:   &types.TableInput{Name: aws.String("skt")},
	})
	require.NoError(t, err)

	_, err = client.UpdateTable(ctx, &gluesdk.UpdateTableInput{
		DatabaseName: aws.String("skdb"),
		TableInput:   &types.TableInput{Name: aws.String("skt"), Description: aws.String("v1")},
		SkipArchive:  aws.Bool(true),
	})
	require.NoError(t, err)

	versions, err := client.GetTableVersions(ctx, &gluesdk.GetTableVersionsInput{
		DatabaseName: aws.String("skdb"), TableName: aws.String("skt"),
	})
	require.NoError(t, err)
	assert.Len(t, versions.TableVersions, 1, "SkipArchive=true must not create a new version")

	_, err = client.UpdateTable(ctx, &gluesdk.UpdateTableInput{
		DatabaseName: aws.String("skdb"),
		TableInput:   &types.TableInput{Name: aws.String("skt"), Description: aws.String("v2")},
	})
	require.NoError(t, err)

	versions, err = client.GetTableVersions(ctx, &gluesdk.GetTableVersionsInput{
		DatabaseName: aws.String("skdb"), TableName: aws.String("skt"),
	})
	require.NoError(t, err)
	assert.Len(t, versions.TableVersions, 2, "default (SkipArchive=false) must archive a version")
}

func testStartDataQualityRunsWorkersAndTimeout(t *testing.T) {
	t.Helper()

	client := newReqFieldSlice1GlueClient(t)
	ctx := t.Context()

	rec, err := client.StartDataQualityRuleRecommendationRun(ctx, &gluesdk.StartDataQualityRuleRecommendationRunInput{
		DataSource: &types.DataSource{
			GlueTable: &types.GlueTable{DatabaseName: aws.String("d"), TableName: aws.String("t")},
		},
		Role:            aws.String("role1"),
		NumberOfWorkers: aws.Int32(11),
		Timeout:         aws.Int32(22),
	})
	require.NoError(t, err)

	gotRec, err := client.GetDataQualityRuleRecommendationRun(ctx, &gluesdk.GetDataQualityRuleRecommendationRunInput{
		RunId: rec.RunId,
	})
	require.NoError(t, err)
	assert.Equal(t, int32(11), aws.ToInt32(gotRec.NumberOfWorkers))
	assert.Equal(t, int32(22), aws.ToInt32(gotRec.Timeout))

	_, err = client.CreateDataQualityRuleset(ctx, &gluesdk.CreateDataQualityRulesetInput{
		Name:    aws.String("rs1"),
		Ruleset: aws.String("Rules = []"),
	})
	require.NoError(t, err)

	eval, err := client.StartDataQualityRulesetEvaluationRun(ctx, &gluesdk.StartDataQualityRulesetEvaluationRunInput{
		RulesetNames: []string{"rs1"},
		Role:         aws.String("role1"),
		DataSource: &types.DataSource{
			GlueTable: &types.GlueTable{DatabaseName: aws.String("d"), TableName: aws.String("t")},
		},
		NumberOfWorkers: aws.Int32(33),
		Timeout:         aws.Int32(44),
	})
	require.NoError(t, err)

	gotEval, err := client.GetDataQualityRulesetEvaluationRun(ctx, &gluesdk.GetDataQualityRulesetEvaluationRunInput{
		RunId: eval.RunId,
	})
	require.NoError(t, err)
	assert.Equal(t, int32(33), aws.ToInt32(gotEval.NumberOfWorkers))
	assert.Equal(t, int32(44), aws.ToInt32(gotEval.Timeout))
}

// assertEntityNotFound asserts err is a real EntityNotFoundException as
// decoded by the real aws-sdk-go-v2 client.
func assertEntityNotFound(t *testing.T, err error) {
	t.Helper()

	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "EntityNotFoundException", apiErr.ErrorCode())
}
