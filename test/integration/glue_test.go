package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_Glue_CatalogLifecycle exercises the core Glue Data Catalog
// workflow end-to-end via the AWS SDK v2: database, table, crawler, connection,
// job, and job run lifecycle. This is the primary integration coverage for
// AWS Glue and protects against JSON-RPC regressions in the AWSGlue target
// dispatch and the per-op input/output shapes.
func TestIntegration_Glue_CatalogLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createGlueClient(t)
	ctx := t.Context()

	const (
		dbName        = "it_glue_db"
		tableName     = "it_glue_table"
		crawlerName   = "it-glue-crawler"
		connName      = "it-glue-conn"
		jobName       = "it-glue-job"
		roleArn       = "arn:aws:iam::000000000000:role/GlueServiceRole"
		s3TargetPath  = "s3://it-glue-bucket/raw/"
		jdbcConnURL   = "jdbc:mysql://example.internal:3306/it"
		scriptS3Path  = "s3://it-glue-scripts/etl.py"
		scriptCommand = "glueetl"
	)

	// CreateDatabase.
	_, err := client.CreateDatabase(ctx, &gluesdk.CreateDatabaseInput{
		DatabaseInput: &gluetypes.DatabaseInput{
			Name:        aws.String(dbName),
			Description: aws.String("integration test database"),
		},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteDatabase(cleanupCtx, &gluesdk.DeleteDatabaseInput{Name: aws.String(dbName)})
	})

	// GetDatabase.
	getDBOut, err := client.GetDatabase(ctx, &gluesdk.GetDatabaseInput{Name: aws.String(dbName)})
	require.NoError(t, err)
	require.NotNil(t, getDBOut.Database)
	assert.Equal(t, dbName, aws.ToString(getDBOut.Database.Name))

	// GetDatabases - the new DB should show up.
	listDBOut, err := client.GetDatabases(ctx, &gluesdk.GetDatabasesInput{})
	require.NoError(t, err)

	foundDB := false

	for _, db := range listDBOut.DatabaseList {
		if aws.ToString(db.Name) == dbName {
			foundDB = true

			break
		}
	}

	assert.True(t, foundDB, "newly created database should be listed")

	// CreateTable.
	_, err = client.CreateTable(ctx, &gluesdk.CreateTableInput{
		DatabaseName: aws.String(dbName),
		TableInput: &gluetypes.TableInput{
			Name:        aws.String(tableName),
			Description: aws.String("integration test table"),
			TableType:   aws.String("EXTERNAL_TABLE"),
			StorageDescriptor: &gluetypes.StorageDescriptor{
				Location: aws.String(s3TargetPath),
				Columns: []gluetypes.Column{
					{Name: aws.String("id"), Type: aws.String("string")},
					{Name: aws.String("ts"), Type: aws.String("timestamp")},
				},
			},
		},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteTable(cleanupCtx, &gluesdk.DeleteTableInput{
			DatabaseName: aws.String(dbName),
			Name:         aws.String(tableName),
		})
	})

	// GetTable.
	tblOut, err := client.GetTable(ctx, &gluesdk.GetTableInput{
		DatabaseName: aws.String(dbName),
		Name:         aws.String(tableName),
	})
	require.NoError(t, err)
	require.NotNil(t, tblOut.Table)
	assert.Equal(t, tableName, aws.ToString(tblOut.Table.Name))

	// CreateCrawler.
	_, err = client.CreateCrawler(ctx, &gluesdk.CreateCrawlerInput{
		Name:         aws.String(crawlerName),
		Role:         aws.String(roleArn),
		DatabaseName: aws.String(dbName),
		Targets: &gluetypes.CrawlerTargets{
			S3Targets: []gluetypes.S3Target{{Path: aws.String(s3TargetPath)}},
		},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteCrawler(cleanupCtx, &gluesdk.DeleteCrawlerInput{Name: aws.String(crawlerName)})
	})

	crawlerOut, err := client.GetCrawler(ctx, &gluesdk.GetCrawlerInput{Name: aws.String(crawlerName)})
	require.NoError(t, err)
	require.NotNil(t, crawlerOut.Crawler)
	assert.Equal(t, crawlerName, aws.ToString(crawlerOut.Crawler.Name))
	assert.Equal(t, dbName, aws.ToString(crawlerOut.Crawler.DatabaseName))

	// CreateConnection.
	_, err = client.CreateConnection(ctx, &gluesdk.CreateConnectionInput{
		ConnectionInput: &gluetypes.ConnectionInput{
			Name:           aws.String(connName),
			ConnectionType: gluetypes.ConnectionTypeJdbc,
			ConnectionProperties: map[string]string{
				"JDBC_CONNECTION_URL": jdbcConnURL,
				"USERNAME":            "user",
				"PASSWORD":            "pass",
			},
		},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteConnection(cleanupCtx, &gluesdk.DeleteConnectionInput{ConnectionName: aws.String(connName)})
	})

	connOut, err := client.GetConnection(ctx, &gluesdk.GetConnectionInput{Name: aws.String(connName)})
	require.NoError(t, err)
	require.NotNil(t, connOut.Connection)
	assert.Equal(t, connName, aws.ToString(connOut.Connection.Name))

	// CreateJob.
	jobCreateOut, err := client.CreateJob(ctx, &gluesdk.CreateJobInput{
		Name: aws.String(jobName),
		Role: aws.String(roleArn),
		Command: &gluetypes.JobCommand{
			Name:           aws.String(scriptCommand),
			ScriptLocation: aws.String(scriptS3Path),
			PythonVersion:  aws.String("3"),
		},
		GlueVersion: aws.String("4.0"),
		WorkerType:  gluetypes.WorkerTypeG1x,
		DefaultArguments: map[string]string{
			"--job-language":            "python",
			"--enable-glue-datacatalog": "true",
		},
	})
	require.NoError(t, err)
	assert.Equal(t, jobName, aws.ToString(jobCreateOut.Name))

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteJob(cleanupCtx, &gluesdk.DeleteJobInput{JobName: aws.String(jobName)})
	})

	jobOut, err := client.GetJob(ctx, &gluesdk.GetJobInput{JobName: aws.String(jobName)})
	require.NoError(t, err)
	require.NotNil(t, jobOut.Job)
	assert.Equal(t, jobName, aws.ToString(jobOut.Job.Name))
	assert.Equal(t, roleArn, aws.ToString(jobOut.Job.Role))

	// StartJobRun then GetJobRun.
	runOut, err := client.StartJobRun(ctx, &gluesdk.StartJobRunInput{
		JobName: aws.String(jobName),
		Arguments: map[string]string{
			"--source-db": dbName,
		},
	})
	require.NoError(t, err)
	runID := aws.ToString(runOut.JobRunId)
	require.NotEmpty(t, runID)

	getRunOut, err := client.GetJobRun(ctx, &gluesdk.GetJobRunInput{
		JobName: aws.String(jobName),
		RunId:   aws.String(runID),
	})
	require.NoError(t, err)
	require.NotNil(t, getRunOut.JobRun)
	assert.Equal(t, runID, aws.ToString(getRunOut.JobRun.Id))

	runsOut, err := client.GetJobRuns(ctx, &gluesdk.GetJobRunsInput{JobName: aws.String(jobName)})
	require.NoError(t, err)
	require.NotEmpty(t, runsOut.JobRuns)
}

// TestIntegration_Glue_IntegrationLifecycle drives
// CreateIntegration -> ModifyIntegration -> DeleteIntegration via the real
// AWS SDK v2 client. All three outputs require CreateTime, IntegrationArn,
// IntegrationName, SourceArn, Status, and TargetArn
// (deserializers.go's respective Output switches); a wrong or missing key
// leaves the SDK's corresponding field nil/zero regardless of what the raw
// body holds, so decoded non-zero values are the only real proof
// (gopherstack-lx5h). ModifyIntegration/DeleteIntegration are addressed by
// IntegrationArn, matching their own SDK doc comments ("The Amazon Resource
// Name (ARN) for the integration") rather than the bare name.
func TestIntegration_Glue_IntegrationLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createGlueClient(t)
	ctx := t.Context()

	const (
		integrationName = "it-glue-integration"
		sourceArn       = "arn:aws:s3:::it-glue-source-bucket"
		targetArn       = "arn:aws:redshift:us-east-1:123456789012:cluster/it-glue-target"
	)

	createOut, err := client.CreateIntegration(ctx, &gluesdk.CreateIntegrationInput{
		IntegrationName: aws.String(integrationName),
		SourceArn:       aws.String(sourceArn),
		TargetArn:       aws.String(targetArn),
	})
	require.NoError(t, err, "CreateIntegration should succeed")
	assert.Equal(t, integrationName, aws.ToString(createOut.IntegrationName))
	assert.Equal(t, sourceArn, aws.ToString(createOut.SourceArn))
	assert.Equal(t, targetArn, aws.ToString(createOut.TargetArn))
	assert.NotEmpty(t, aws.ToString(createOut.IntegrationArn), "IntegrationArn is a required response field")
	assert.NotZero(t, aws.ToTime(createOut.CreateTime), "CreateTime is a required response field")
	assert.NotEmpty(t, string(createOut.Status), "Status is a required response field")
	integrationARN := aws.ToString(createOut.IntegrationArn)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteIntegration(cleanupCtx, &gluesdk.DeleteIntegrationInput{
			IntegrationIdentifier: aws.String(integrationARN),
		})
	})

	modifyOut, err := client.ModifyIntegration(ctx, &gluesdk.ModifyIntegrationInput{
		IntegrationIdentifier: aws.String(integrationARN),
	})
	require.NoError(t, err, "ModifyIntegration should succeed")
	assert.Equal(t, integrationName, aws.ToString(modifyOut.IntegrationName))
	assert.Equal(t, integrationARN, aws.ToString(modifyOut.IntegrationArn))
	assert.Equal(t, sourceArn, aws.ToString(modifyOut.SourceArn))
	assert.Equal(t, targetArn, aws.ToString(modifyOut.TargetArn))
	assert.NotZero(t, aws.ToTime(modifyOut.CreateTime), "CreateTime is a required response field")

	deleteOut, err := client.DeleteIntegration(ctx, &gluesdk.DeleteIntegrationInput{
		IntegrationIdentifier: aws.String(integrationARN),
	})
	require.NoError(t, err, "DeleteIntegration should succeed")
	assert.Equal(t, integrationName, aws.ToString(deleteOut.IntegrationName))
	assert.Equal(t, integrationARN, aws.ToString(deleteOut.IntegrationArn))
	assert.Equal(t, sourceArn, aws.ToString(deleteOut.SourceArn))
	assert.Equal(t, targetArn, aws.ToString(deleteOut.TargetArn))
	assert.NotZero(t, aws.ToTime(deleteOut.CreateTime), "CreateTime is a required response field")
}
