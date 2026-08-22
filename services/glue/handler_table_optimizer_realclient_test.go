package glue_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSDKRoundTrip_GetTableOptimizer_ConfigurationFields drives
// CreateTableOptimizer and GetTableOptimizer through the real aws-sdk-go-v2
// client and proves TableOptimizer.Type, .Configuration and
// .Configuration.Enabled/.RoleArn all decode non-nil. Real Glue's nested
// TableOptimizer document (deserializeDocumentTableOptimizer,
// glue@v1.152.0) is entirely lowerCamelCase ("type", "configuration",
// "lastRun") and its own nested TableOptimizerConfiguration is too
// ("enabled", "roleArn") -- gopherstack tagged all of them PascalCase, so a
// real client's response decode silently dropped every field of the
// optimizer it just created. Refs: gopherstack-v4a4.
func TestSDKRoundTrip_GetTableOptimizer_ConfigurationFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestGlueClient(t, h)

	dbRec := doGlueRequest(t, h, "CreateDatabase", map[string]any{
		"DatabaseInput": map[string]any{"Name": "db1"},
	})
	require.Equal(t, 200, dbRec.Code)

	tblRec := doGlueRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "db1",
		"TableInput":   map[string]any{"Name": "tbl1"},
	})
	require.Equal(t, 200, tblRec.Code)

	_, err := client.CreateTableOptimizer(t.Context(), &gluesdk.CreateTableOptimizerInput{
		CatalogId:    aws.String(testAccountID),
		DatabaseName: aws.String("db1"),
		TableName:    aws.String("tbl1"),
		Type:         types.TableOptimizerTypeCompaction,
		TableOptimizerConfiguration: &types.TableOptimizerConfiguration{
			Enabled: aws.Bool(true),
			RoleArn: aws.String("arn:aws:iam::000000000000:role/GlueRole"),
		},
	})
	require.NoError(t, err)

	out, err := client.GetTableOptimizer(t.Context(), &gluesdk.GetTableOptimizerInput{
		CatalogId:    aws.String(testAccountID),
		DatabaseName: aws.String("db1"),
		TableName:    aws.String("tbl1"),
		Type:         types.TableOptimizerTypeCompaction,
	})
	require.NoError(t, err)
	require.NotNil(t, out.TableOptimizer, "TableOptimizer must decode non-nil against the real SDK deserializer")

	to := out.TableOptimizer
	assert.Equal(t, types.TableOptimizerTypeCompaction, to.Type)
	require.NotNil(t, to.Configuration, "Configuration must decode non-nil against the real SDK deserializer")
	require.NotNil(t, to.Configuration.Enabled, "Configuration.Enabled must decode non-nil")
	assert.True(t, aws.ToBool(to.Configuration.Enabled))
	require.NotNil(t, to.Configuration.RoleArn, "Configuration.RoleArn must decode non-nil")
	assert.Equal(t, "arn:aws:iam::000000000000:role/GlueRole", aws.ToString(to.Configuration.RoleArn))
}

// TestSDKRoundTrip_BatchGetTableOptimizer_NestedShape drives
// BatchGetTableOptimizer through the real aws-sdk-go-v2 client and proves
// both its success and failure arms decode correctly. Real Glue's
// BatchTableOptimizer document nests TableOptimizer one level deeper than
// GetTableOptimizerOutput does, under a "tableOptimizer" key
// (deserializeDocumentBatchTableOptimizer, glue@v1.152.0), and its own
// catalogId/databaseName/tableName are lowerCamelCase rather than
// GetTableOptimizerOutput's PascalCase. Its failure entries
// (deserializeDocumentBatchGetTableOptimizerError) are lowerCamelCase too,
// except the nested ErrorDetail document, which stays PascalCase. Refs:
// gopherstack-5mvf.
func TestSDKRoundTrip_BatchGetTableOptimizer_NestedShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestGlueClient(t, h)

	dbRec := doGlueRequest(t, h, "CreateDatabase", map[string]any{
		"DatabaseInput": map[string]any{"Name": "db2"},
	})
	require.Equal(t, 200, dbRec.Code)

	tblRec := doGlueRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "db2",
		"TableInput":   map[string]any{"Name": "tbl2"},
	})
	require.Equal(t, 200, tblRec.Code)

	_, err := client.CreateTableOptimizer(t.Context(), &gluesdk.CreateTableOptimizerInput{
		CatalogId:    aws.String(testAccountID),
		DatabaseName: aws.String("db2"),
		TableName:    aws.String("tbl2"),
		Type:         types.TableOptimizerTypeCompaction,
		TableOptimizerConfiguration: &types.TableOptimizerConfiguration{
			Enabled: aws.Bool(true),
			RoleArn: aws.String("arn:aws:iam::000000000000:role/GlueRole"),
		},
	})
	require.NoError(t, err)

	out, err := client.BatchGetTableOptimizer(t.Context(), &gluesdk.BatchGetTableOptimizerInput{
		Entries: []types.BatchGetTableOptimizerEntry{
			{
				CatalogId:    aws.String(testAccountID),
				DatabaseName: aws.String("db2"),
				TableName:    aws.String("tbl2"),
				Type:         types.TableOptimizerTypeCompaction,
			},
			{
				CatalogId:    aws.String(testAccountID),
				DatabaseName: aws.String("db2"),
				TableName:    aws.String("missing"),
				Type:         types.TableOptimizerTypeCompaction,
			},
		},
	})
	require.NoError(t, err)

	t.Run("success entry nests tableoptimizer under batchtableoptimizer", func(t *testing.T) {
		t.Parallel()

		require.Len(t, out.TableOptimizers, 1)
		entry := out.TableOptimizers[0]
		assert.Equal(t, "db2", aws.ToString(entry.DatabaseName))
		assert.Equal(t, "tbl2", aws.ToString(entry.TableName))
		require.NotNil(t, entry.TableOptimizer, "TableOptimizer must decode non-nil against the real SDK deserializer")
		assert.Equal(t, types.TableOptimizerTypeCompaction, entry.TableOptimizer.Type)
		require.NotNil(t, entry.TableOptimizer.Configuration, "Configuration must decode non-nil")
		require.NotNil(t, entry.TableOptimizer.Configuration.Enabled, "Configuration.Enabled must decode non-nil")
		assert.True(t, aws.ToBool(entry.TableOptimizer.Configuration.Enabled))
	})

	t.Run("failure entry decodes identifiers and error detail", func(t *testing.T) {
		t.Parallel()

		require.Len(t, out.Failures, 1)
		failure := out.Failures[0]
		assert.Equal(t, "db2", aws.ToString(failure.DatabaseName))
		assert.Equal(t, "missing", aws.ToString(failure.TableName))
		assert.Equal(t, types.TableOptimizerTypeCompaction, failure.Type)
		require.NotNil(t, failure.Error, "Error must decode non-nil against the real SDK deserializer")
		assert.NotEmpty(t, aws.ToString(failure.Error.ErrorCode))
	})
}
