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

func newDroppedMembersClient(t *testing.T) *gluesdk.Client {
	t.Helper()

	return newTestGlueClient(t, glue.NewHandler(glue.NewInMemoryBackend(testAccountID, testRegion)))
}

func TestJobMembersRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		update bool
	}{
		{name: "create"},
		{name: "update", update: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newDroppedMembersClient(t)
			ctx := t.Context()
			role := aws.String("arn:aws:iam::" + testAccountID + ":role/glue-role")
			cmd := &types.JobCommand{Name: aws.String("glueetl")}

			_, err := client.CreateJob(ctx, &gluesdk.CreateJobInput{Name: aws.String("j"), Role: role, Command: cmd})
			require.NoError(t, err)

			if tt.update {
				_, err = client.UpdateJob(ctx, &gluesdk.UpdateJobInput{
					JobName: aws.String("j"),
					JobUpdate: &types.JobUpdate{
						Role:                    role,
						Command:                 cmd,
						LogUri:                  aws.String("s3://logs/"),
						ExecutionClass:          types.ExecutionClassFlex,
						MaintenanceWindow:       aws.String("Sun:03"),
						JobRunQueuingEnabled:    aws.Bool(true),
						NonOverridableArguments: map[string]string{"--k": "v"},
					},
				})
				require.NoError(t, err)
			} else {
				_, err = client.DeleteJob(ctx, &gluesdk.DeleteJobInput{JobName: aws.String("j")})
				require.NoError(t, err)
				_, err = client.CreateJob(ctx, &gluesdk.CreateJobInput{
					Name:                    aws.String("j"),
					Role:                    role,
					Command:                 cmd,
					LogUri:                  aws.String("s3://logs/"),
					ExecutionClass:          types.ExecutionClassFlex,
					MaintenanceWindow:       aws.String("Sun:03"),
					JobRunQueuingEnabled:    aws.Bool(true),
					NonOverridableArguments: map[string]string{"--k": "v"},
					SecurityConfiguration:   aws.String("sc1"),
				})
				require.NoError(t, err)
			}

			got, err := client.GetJob(ctx, &gluesdk.GetJobInput{JobName: aws.String("j")})
			require.NoError(t, err)

			job := got.Job
			assert.Equal(t, "s3://logs/", aws.ToString(job.LogUri))
			assert.Equal(t, types.ExecutionClassFlex, job.ExecutionClass)
			assert.Equal(t, "Sun:03", aws.ToString(job.MaintenanceWindow))
			assert.True(t, aws.ToBool(job.JobRunQueuingEnabled))
			assert.Equal(t, map[string]string{"--k": "v"}, job.NonOverridableArguments)

			if !tt.update {
				assert.Equal(t, "sc1", aws.ToString(job.SecurityConfiguration))
			}

			run, err := client.StartJobRun(ctx, &gluesdk.StartJobRunInput{
				JobName:                    aws.String("j"),
				ExecutionRoleSessionPolicy: aws.String(`{"Version":"2012-10-17"}`),
			})
			require.NoError(t, err)

			jr, err := client.GetJobRun(ctx, &gluesdk.GetJobRunInput{JobName: aws.String("j"), RunId: run.JobRunId})
			require.NoError(t, err)
			assert.Equal(t, types.ExecutionClassFlex, jr.JobRun.ExecutionClass)
			assert.Equal(t, "Sun:03", aws.ToString(jr.JobRun.MaintenanceWindow))
			assert.JSONEq(t, `{"Version":"2012-10-17"}`, aws.ToString(jr.JobRun.ExecutionRoleSessionPolicy))
		})
	}
}

func TestCreateJobStoresCodeGenNodesAndSourceControl(t *testing.T) {
	t.Parallel()

	client := newDroppedMembersClient(t)
	ctx := t.Context()

	_, err := client.CreateJob(ctx, &gluesdk.CreateJobInput{
		Name:    aws.String("visual"),
		Role:    aws.String("arn:aws:iam::" + testAccountID + ":role/glue-role"),
		Command: &types.JobCommand{Name: aws.String("glueetl")},
		CodeGenConfigurationNodes: map[string]types.CodeGenConfigurationNode{
			"n1": {S3CsvSource: &types.S3CsvSource{
				Name: aws.String("src"), Paths: []string{"s3://b/p"},
				QuoteChar: types.QuoteCharQuote, Separator: types.SeparatorComma,
			}},
		},
		SourceControlDetails: &types.SourceControlDetails{
			Provider: types.SourceControlProviderGithub,
			Branch:   aws.String("main"),
		},
	})
	require.NoError(t, err)

	got, err := client.GetJob(ctx, &gluesdk.GetJobInput{JobName: aws.String("visual")})
	require.NoError(t, err)

	node := got.Job.CodeGenConfigurationNodes["n1"]
	require.NotNil(t, node.S3CsvSource)
	assert.Equal(t, "src", aws.ToString(node.S3CsvSource.Name))
	assert.Equal(t, []string{"s3://b/p"}, node.S3CsvSource.Paths)
	require.NotNil(t, got.Job.SourceControlDetails)
	assert.Equal(t, "main", aws.ToString(got.Job.SourceControlDetails.Branch))
}

func TestCreateSessionStoresSizingMembers(t *testing.T) {
	t.Parallel()

	client := newDroppedMembersClient(t)
	ctx := t.Context()

	_, err := client.CreateSession(ctx, &gluesdk.CreateSessionInput{
		Id:                    aws.String("s1"),
		Role:                  aws.String("arn:aws:iam::" + testAccountID + ":role/glue-role"),
		Command:               &types.SessionCommand{Name: aws.String("glueetl"), PythonVersion: aws.String("3")},
		GlueVersion:           aws.String("4.0"),
		WorkerType:            types.WorkerTypeG1x,
		NumberOfWorkers:       aws.Int32(4),
		SecurityConfiguration: aws.String("sc1"),
		Connections:           &types.ConnectionsList{Connections: []string{"c1"}},
	})
	require.NoError(t, err)

	got, err := client.GetSession(ctx, &gluesdk.GetSessionInput{Id: aws.String("s1")})
	require.NoError(t, err)

	s := got.Session
	assert.Equal(t, "4.0", aws.ToString(s.GlueVersion))
	assert.Equal(t, types.WorkerTypeG1x, s.WorkerType)
	assert.Equal(t, int32(4), aws.ToInt32(s.NumberOfWorkers))
	assert.Equal(t, "sc1", aws.ToString(s.SecurityConfiguration))
	require.NotNil(t, s.Connections)
	assert.Equal(t, []string{"c1"}, s.Connections.Connections)
}

func TestIntegrationOptionalMembers(t *testing.T) {
	t.Parallel()

	client := newDroppedMembersClient(t)
	ctx := t.Context()

	created, err := client.CreateIntegration(ctx, &gluesdk.CreateIntegrationInput{
		IntegrationName:             aws.String("i1"),
		SourceArn:                   aws.String("arn:aws:rds:us-east-1:" + testAccountID + ":cluster:src"),
		TargetArn:                   aws.String("arn:aws:glue:us-east-1:" + testAccountID + ":database/tgt"),
		Description:                 aws.String("d1"),
		KmsKeyId:                    aws.String("key1"),
		AdditionalEncryptionContext: map[string]string{"k": "v"},
		IntegrationConfig: &types.IntegrationConfig{
			ContinuousSync:  aws.Bool(true),
			RefreshInterval: aws.String("15"),
		},
		DataFilter: aws.String("include:db.*"),
		Tags:       []types.Tag{{Key: aws.String("t"), Value: aws.String("1")}},
	})
	require.NoError(t, err)
	assert.Equal(t, "d1", aws.ToString(created.Description))
	assert.Equal(t, "key1", aws.ToString(created.KmsKeyId))

	mod, err := client.ModifyIntegration(ctx, &gluesdk.ModifyIntegrationInput{
		IntegrationIdentifier: created.IntegrationArn,
		Description:           aws.String("d2"),
		IntegrationConfig:     &types.IntegrationConfig{RefreshInterval: aws.String("30")},
	})
	require.NoError(t, err)
	assert.Equal(t, "d2", aws.ToString(mod.Description))
	assert.Equal(t, "include:db.*", aws.ToString(mod.DataFilter), "unset DataFilter stays")

	_, err = client.TagResource(ctx, &gluesdk.TagResourceInput{
		ResourceArn: created.IntegrationArn, TagsToAdd: map[string]string{"extra": "2"},
	})
	require.NoError(t, err)

	tagged, err := client.GetTags(ctx, &gluesdk.GetTagsInput{ResourceArn: created.IntegrationArn})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"t": "1", "extra": "2"}, tagged.Tags)

	_, err = client.UntagResource(ctx, &gluesdk.UntagResourceInput{
		ResourceArn: created.IntegrationArn, TagsToRemove: []string{"extra"},
	})
	require.NoError(t, err)

	desc, err := client.DescribeIntegrations(ctx, &gluesdk.DescribeIntegrationsInput{})
	require.NoError(t, err)
	require.Len(t, desc.Integrations, 1)

	ig := desc.Integrations[0]
	assert.Equal(t, "d2", aws.ToString(ig.Description))
	assert.Equal(t, "key1", aws.ToString(ig.KmsKeyId))
	assert.Equal(t, map[string]string{"k": "v"}, ig.AdditionalEncryptionContext)
	assert.Equal(t, "include:db.*", aws.ToString(ig.DataFilter))
	require.NotNil(t, ig.IntegrationConfig)
	assert.Equal(t, "30", aws.ToString(ig.IntegrationConfig.RefreshInterval))
	require.Len(t, ig.Tags, 1)
	assert.Equal(t, "t", aws.ToString(ig.Tags[0].Key))
}

func TestColumnStatisticsTaskSettingsRoundTrip(t *testing.T) {
	t.Parallel()

	client := newDroppedMembersClient(t)
	ctx := t.Context()

	_, err := client.CreateDatabase(
		ctx,
		&gluesdk.CreateDatabaseInput{DatabaseInput: &types.DatabaseInput{Name: aws.String("db")}},
	)
	require.NoError(t, err)
	_, err = client.CreateTable(ctx, &gluesdk.CreateTableInput{
		DatabaseName: aws.String("db"), TableInput: &types.TableInput{Name: aws.String("tbl")},
	})
	require.NoError(t, err)

	_, err = client.GetColumnStatisticsTaskSettings(ctx, &gluesdk.GetColumnStatisticsTaskSettingsInput{
		DatabaseName: aws.String("db"), TableName: aws.String("tbl"),
	})
	require.Error(t, err, "settings were never created")

	_, err = client.UpdateColumnStatisticsTaskSettings(ctx, &gluesdk.UpdateColumnStatisticsTaskSettingsInput{
		DatabaseName: aws.String("db"), TableName: aws.String("tbl"), SampleSize: 10,
	})
	require.Error(t, err, "nothing to update yet")

	_, err = client.CreateColumnStatisticsTaskSettings(ctx, &gluesdk.CreateColumnStatisticsTaskSettingsInput{
		DatabaseName:          aws.String("db"),
		TableName:             aws.String("tbl"),
		Role:                  aws.String("arn:aws:iam::" + testAccountID + ":role/r"),
		Schedule:              aws.String("cron(0 1 * * ? *)"),
		SampleSize:            50,
		SecurityConfiguration: aws.String("sc1"),
		ColumnNameList:        []string{"a"},
	})
	require.NoError(t, err)

	_, err = client.UpdateColumnStatisticsTaskSettings(ctx, &gluesdk.UpdateColumnStatisticsTaskSettingsInput{
		DatabaseName: aws.String(
			"db",
		), TableName: aws.String("tbl"), SampleSize: 25, ColumnNameList: []string{"a", "b"},
	})
	require.NoError(t, err)

	got, err := client.GetColumnStatisticsTaskSettings(ctx, &gluesdk.GetColumnStatisticsTaskSettingsInput{
		DatabaseName: aws.String("db"), TableName: aws.String("tbl"),
	})
	require.NoError(t, err)

	s := got.ColumnStatisticsTaskSettings
	assert.Equal(t, "arn:aws:iam::"+testAccountID+":role/r", aws.ToString(s.Role))
	assert.InDelta(t, 25, s.SampleSize, 0)
	assert.Equal(t, "sc1", aws.ToString(s.SecurityConfiguration))
	assert.Equal(t, []string{"a", "b"}, s.ColumnNameList)
	assert.Equal(t, types.ScheduleTypeCron, s.ScheduleType)
	require.NotNil(t, s.Schedule)
	assert.Equal(t, "cron(0 1 * * ? *)", aws.ToString(s.Schedule.ScheduleExpression))
}

func TestCreateTableStoresViewAndStorageMembers(t *testing.T) {
	t.Parallel()

	client := newDroppedMembersClient(t)
	ctx := t.Context()

	_, err := client.CreateDatabase(
		ctx,
		&gluesdk.CreateDatabaseInput{DatabaseInput: &types.DatabaseInput{Name: aws.String("db")}},
	)
	require.NoError(t, err)

	accessed := time.Unix(1_700_000_000, 0)
	_, err = client.CreateTable(ctx, &gluesdk.CreateTableInput{
		DatabaseName: aws.String("db"),
		TableInput: &types.TableInput{
			Name:             aws.String("tbl"),
			ViewOriginalText: aws.String("select 1"),
			ViewExpandedText: aws.String("select 1 from x"),
			LastAccessTime:   aws.Time(accessed),
			LastAnalyzedTime: aws.Time(accessed),
			TargetTable:      &types.TableIdentifier{DatabaseName: aws.String("other"), Name: aws.String("src")},
			PartitionKeys:    []types.Column{{Name: aws.String("dt"), Type: aws.String("string")}},
			StorageDescriptor: &types.StorageDescriptor{
				AdditionalLocations: []string{"s3://extra"},
				SkewedInfo: &types.SkewedInfo{
					SkewedColumnNames:  []string{"c"},
					SkewedColumnValues: []string{"v"},
				},
				SchemaReference: &types.SchemaReference{
					SchemaId:            &types.SchemaId{RegistryName: aws.String("r"), SchemaName: aws.String("s")},
					SchemaVersionNumber: aws.Int64(3),
				},
			},
		},
		PartitionIndexes: []types.PartitionIndex{{IndexName: aws.String("ix"), Keys: []string{"dt"}}},
	})
	require.NoError(t, err)

	got, err := client.GetTable(ctx, &gluesdk.GetTableInput{DatabaseName: aws.String("db"), Name: aws.String("tbl")})
	require.NoError(t, err)

	tbl := got.Table
	assert.Equal(t, "select 1", aws.ToString(tbl.ViewOriginalText))
	assert.Equal(t, "select 1 from x", aws.ToString(tbl.ViewExpandedText))
	assert.True(t, accessed.Equal(aws.ToTime(tbl.LastAccessTime)))
	assert.True(t, accessed.Equal(aws.ToTime(tbl.LastAnalyzedTime)))
	require.NotNil(t, tbl.TargetTable)
	assert.Equal(t, "src", aws.ToString(tbl.TargetTable.Name))

	sd := tbl.StorageDescriptor
	require.NotNil(t, sd)
	assert.Equal(t, []string{"s3://extra"}, sd.AdditionalLocations)
	require.NotNil(t, sd.SkewedInfo)
	assert.Equal(t, []string{"c"}, sd.SkewedInfo.SkewedColumnNames)
	require.NotNil(t, sd.SchemaReference)
	assert.Equal(t, int64(3), aws.ToInt64(sd.SchemaReference.SchemaVersionNumber))
	assert.Equal(t, "s", aws.ToString(sd.SchemaReference.SchemaId.SchemaName))

	idx, err := client.GetPartitionIndexes(ctx, &gluesdk.GetPartitionIndexesInput{
		DatabaseName: aws.String("db"), TableName: aws.String("tbl"),
	})
	require.NoError(t, err)
	require.Len(t, idx.PartitionIndexDescriptorList, 1)
	assert.Equal(t, "ix", aws.ToString(idx.PartitionIndexDescriptorList[0].IndexName))

	_, err = client.CreateTable(ctx, &gluesdk.CreateTableInput{
		DatabaseName:     aws.String("db"),
		TableInput:       &types.TableInput{Name: aws.String("bad")},
		PartitionIndexes: []types.PartitionIndex{{IndexName: aws.String("ix"), Keys: []string{"nokey"}}},
	})
	require.Error(t, err, "index keys must be partition keys")

	_, err = client.GetTable(ctx, &gluesdk.GetTableInput{DatabaseName: aws.String("db"), Name: aws.String("bad")})
	require.Error(t, err, "failed create leaves no table")
}

func TestGetMLTaskRunReportsLastModifiedOn(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))
	ctx := t.Context()

	tr, err := client.CreateMLTransform(ctx, &gluesdk.CreateMLTransformInput{
		Name:              aws.String("t"),
		Role:              aws.String("arn:aws:iam::" + testAccountID + ":role/r"),
		InputRecordTables: []types.GlueTable{{DatabaseName: aws.String("db"), TableName: aws.String("tbl")}},
		Parameters: &types.TransformParameters{
			TransformType:         types.TransformTypeFindMatches,
			FindMatchesParameters: &types.FindMatchesParameters{PrimaryKeyColumnName: aws.String("id")},
		},
	})
	require.NoError(t, err)

	started, err := client.StartMLEvaluationTaskRun(
		ctx,
		&gluesdk.StartMLEvaluationTaskRunInput{TransformId: tr.TransformId},
	)
	require.NoError(t, err)

	got, err := client.GetMLTaskRun(ctx, &gluesdk.GetMLTaskRunInput{
		TransformId: tr.TransformId, TaskRunId: started.TaskRunId,
	})
	require.NoError(t, err)
	require.NotNil(t, got.LastModifiedOn)
	assert.False(t, got.LastModifiedOn.IsZero())

	_, err = client.GetMLTaskRun(
		ctx,
		&gluesdk.GetMLTaskRunInput{TransformId: tr.TransformId, TaskRunId: aws.String("unknown")},
	)
	require.Error(t, err)
}

func TestGetConnectionHidePassword(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		hidePassword bool
		wantPassword bool
	}{
		{name: "hidden", hidePassword: true},
		{name: "shown", wantPassword: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newDroppedMembersClient(t)
			ctx := t.Context()

			_, err := client.CreateConnection(ctx, &gluesdk.CreateConnectionInput{
				ConnectionInput: &types.ConnectionInput{
					Name:           aws.String("c"),
					ConnectionType: types.ConnectionTypeJdbc,
					ConnectionProperties: map[string]string{
						"JDBC_CONNECTION_URL": "jdbc:mysql://h/db", "USERNAME": "u", "PASSWORD": "secret",
					},
				},
			})
			require.NoError(t, err)

			got, err := client.GetConnection(ctx, &gluesdk.GetConnectionInput{
				Name: aws.String("c"), HidePassword: tt.hidePassword,
			})
			require.NoError(t, err)

			_, hasPassword := got.Connection.ConnectionProperties["PASSWORD"]
			assert.Equal(t, tt.wantPassword, hasPassword)
			assert.Equal(t, "u", got.Connection.ConnectionProperties["USERNAME"])
		})
	}
}

func TestSchemaVersionMetadataBySchemaIDAndNumber(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		version types.SchemaVersionNumber
		wantID  int
	}{
		{name: "explicit version", version: types.SchemaVersionNumber{VersionNumber: aws.Int64(1)}, wantID: 1},
		{name: "latest version", version: types.SchemaVersionNumber{LatestVersion: true}, wantID: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newDroppedMembersClient(t)
			ctx := t.Context()
			schemaID := &types.SchemaId{RegistryName: aws.String("reg"), SchemaName: aws.String("sch")}

			_, err := client.CreateRegistry(ctx, &gluesdk.CreateRegistryInput{RegistryName: aws.String("reg")})
			require.NoError(t, err)

			v1, err := client.CreateSchema(ctx, &gluesdk.CreateSchemaInput{
				SchemaName: aws.String("sch"), RegistryId: &types.RegistryId{RegistryName: aws.String("reg")},
				DataFormat: types.DataFormatJson, Compatibility: types.CompatibilityNone,
				SchemaDefinition: aws.String(`{"type":"object"}`),
			})
			require.NoError(t, err)

			v2, err := client.RegisterSchemaVersion(ctx, &gluesdk.RegisterSchemaVersionInput{
				SchemaId: schemaID, SchemaDefinition: aws.String(`{"type":"object","extra":true}`),
			})
			require.NoError(t, err)

			wantVersionID := v1.SchemaVersionId
			if tt.wantID == 2 {
				wantVersionID = v2.SchemaVersionId
			}

			put, err := client.PutSchemaVersionMetadata(ctx, &gluesdk.PutSchemaVersionMetadataInput{
				SchemaId:            schemaID,
				SchemaVersionNumber: &tt.version,
				MetadataKeyValue: &types.MetadataKeyValuePair{
					MetadataKey:   aws.String("owner"),
					MetadataValue: aws.String("a"),
				},
			})
			require.NoError(t, err)
			assert.Equal(t, aws.ToString(wantVersionID), aws.ToString(put.SchemaVersionId))

			q, err := client.QuerySchemaVersionMetadata(ctx, &gluesdk.QuerySchemaVersionMetadataInput{
				SchemaId: schemaID, SchemaVersionNumber: &tt.version,
			})
			require.NoError(t, err)
			require.Contains(t, q.MetadataInfoMap, "owner")

			_, err = client.RemoveSchemaVersionMetadata(ctx, &gluesdk.RemoveSchemaVersionMetadataInput{
				SchemaId:            schemaID,
				SchemaVersionNumber: &tt.version,
				MetadataKeyValue: &types.MetadataKeyValuePair{
					MetadataKey:   aws.String("owner"),
					MetadataValue: aws.String("a"),
				},
			})
			require.NoError(t, err)

			byID, err := client.QuerySchemaVersionMetadata(ctx, &gluesdk.QuerySchemaVersionMetadataInput{
				SchemaVersionId: wantVersionID,
			})
			require.NoError(t, err)
			assert.Empty(t, byID.MetadataInfoMap)

			got, err := client.GetSchemaVersion(
				ctx,
				&gluesdk.GetSchemaVersionInput{SchemaId: schemaID, SchemaVersionNumber: &tt.version},
			)
			require.NoError(t, err)
			assert.Equal(t, int64(tt.wantID), aws.ToInt64(got.VersionNumber))

			_, err = client.PutSchemaVersionMetadata(ctx, &gluesdk.PutSchemaVersionMetadataInput{
				MetadataKeyValue: &types.MetadataKeyValuePair{
					MetadataKey:   aws.String("k"),
					MetadataValue: aws.String("v"),
				},
			})
			require.Error(t, err, "neither a version id nor schema id plus number")
		})
	}
}
