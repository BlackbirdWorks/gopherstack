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
