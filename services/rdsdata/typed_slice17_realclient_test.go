package rdsdata_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	rdsdatasdk "github.com/aws/aws-sdk-go-v2/service/rdsdata"
	"github.com/aws/aws-sdk-go-v2/service/rdsdata/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rdsdata"
)

const (
	execSQLResourceARN = "arn:aws:rds:us-east-1:000000000000:cluster:execsql-cluster"
	execSQLSecretARN   = "arn:aws:secretsmanager:us-east-1:000000000000:secret:execsql-secret"
)

// TestExecuteSql_RealClient covers rdsdata's last typed-client-uncovered op
// (gopherstack-n3zi slice 17): the deprecated ExecuteSql batch-statement
// entry point. Creates a table, inserts a row, and selects it back through
// the real aws-sdk-go-v2 client, asserting the decoded legacy
// SqlStatementResult/ResultFrame/ColumnMetadata/Value shapes.
func TestExecuteSql_RealClient(t *testing.T) {
	t.Parallel()

	backend := rdsdata.NewInMemoryBackend("000000000000", "us-east-1")
	client := newRoundTripClient(t, rdsdata.NewHandler(backend))
	ctx := t.Context()

	_, err := client.ExecuteSql(ctx, &rdsdatasdk.ExecuteSqlInput{
		DbClusterOrInstanceArn: aws.String(execSQLResourceARN),
		AwsSecretStoreArn:      aws.String(execSQLSecretARN),
		SqlStatements:          aws.String("CREATE TABLE es (v INTEGER)"),
	})
	require.NoError(t, err)

	insertOut, err := client.ExecuteSql(ctx, &rdsdatasdk.ExecuteSqlInput{
		DbClusterOrInstanceArn: aws.String(execSQLResourceARN),
		AwsSecretStoreArn:      aws.String(execSQLSecretARN),
		SqlStatements:          aws.String("INSERT INTO es (v) VALUES (42)"),
	})
	require.NoError(t, err)
	require.Len(t, insertOut.SqlStatementResults, 1)
	assert.Equal(t, int64(1), insertOut.SqlStatementResults[0].NumberOfRecordsUpdated)
	assert.Nil(t, insertOut.SqlStatementResults[0].ResultFrame)

	selectOut, err := client.ExecuteSql(ctx, &rdsdatasdk.ExecuteSqlInput{
		DbClusterOrInstanceArn: aws.String(execSQLResourceARN),
		AwsSecretStoreArn:      aws.String(execSQLSecretARN),
		SqlStatements:          aws.String("SELECT v FROM es"),
	})
	require.NoError(t, err)
	require.Len(t, selectOut.SqlStatementResults, 1)

	frame := selectOut.SqlStatementResults[0].ResultFrame
	require.NotNil(t, frame)
	require.NotNil(t, frame.ResultSetMetadata)
	assert.Equal(t, int64(1), frame.ResultSetMetadata.ColumnCount)
	require.Len(t, frame.Records, 1)
	require.Len(t, frame.Records[0].Values, 1)
	v, ok := frame.Records[0].Values[0].(*types.ValueMemberBigIntValue)
	require.True(t, ok, "expected bigIntValue union member, got %T", frame.Records[0].Values[0])
	assert.Equal(t, int64(42), v.Value)
}
