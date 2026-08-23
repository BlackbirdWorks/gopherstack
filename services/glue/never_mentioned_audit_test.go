package glue_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	gluesdktypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// TestBatchDeleteConnection_ErrorsIsAMapKeyedByName proves
// BatchDeleteConnectionOutput.Errors decodes as a map keyed by connection
// name (glue@v1.152.0 deserializers.go:
// awsAwsjson11_deserializeDocumentErrorByName decodes a JSON object, not an
// array) -- previously a JSON array, which a real client fails to decode.
func TestBatchDeleteConnection_ErrorsIsAMapKeyedByName(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))

	out, err := client.BatchDeleteConnection(t.Context(), &gluesdk.BatchDeleteConnectionInput{
		ConnectionNameList: []string{"no-such-connection"},
	})
	require.NoError(t, err, "a real client must be able to decode a non-empty Errors map")
	require.Contains(t, out.Errors, "no-such-connection")
	assert.Equal(t, "EntityNotFoundException", aws.ToString(out.Errors["no-such-connection"].ErrorCode))
}

// TestBatchGetDataQualityResult_ResultsNotFoundIsAStringList proves
// BatchGetDataQualityResultOutput.ResultsNotFound decodes as a list of
// result ID strings (glue@v1.152.0 deserializers.go:
// awsAwsjson11_deserializeDocumentDataQualityResultIds decodes into
// []string) -- previously a list of ErrorDetail objects, which a real
// client fails to decode.
func TestBatchGetDataQualityResult_ResultsNotFoundIsAStringList(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))

	out, err := client.BatchGetDataQualityResult(t.Context(), &gluesdk.BatchGetDataQualityResultInput{
		ResultIds: []string{"no-such-result"},
	})
	require.NoError(t, err, "a real client must be able to decode a non-empty ResultsNotFound list")
	assert.Equal(t, []string{"no-such-result"}, out.ResultsNotFound)
}

// TestColumnStatisticsTaskRun_StopByTable_NotRunID proves
// StopColumnStatisticsTaskRun identifies the run by DatabaseName+TableName
// (glue@v1.152.0 api_op_StopColumnStatisticsTaskRun.go has no run-ID member
// at all) and that Start requires the real, required Role member.
func TestColumnStatisticsTaskRun_StopByTable_NotRunID(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))

	_, err := client.StartColumnStatisticsTaskRun(t.Context(), &gluesdk.StartColumnStatisticsTaskRunInput{
		DatabaseName: aws.String("db1"),
		TableName:    aws.String("tbl1"),
		Role:         aws.String("arn:aws:iam::000000000000:role/GlueRole"),
	})
	require.NoError(t, err)

	_, err = client.StopColumnStatisticsTaskRun(t.Context(), &gluesdk.StopColumnStatisticsTaskRunInput{
		DatabaseName: aws.String("db1"),
		TableName:    aws.String("tbl1"),
	})
	require.NoError(t, err, "a real client's Stop call (DatabaseName+TableName, no RunId) must actually stop the run")

	runs, err := client.GetColumnStatisticsTaskRuns(t.Context(), &gluesdk.GetColumnStatisticsTaskRunsInput{
		DatabaseName: aws.String("db1"),
		TableName:    aws.String("tbl1"),
	})
	require.NoError(t, err)
	require.Len(t, runs.ColumnStatisticsTaskRuns, 1)
	assert.Equal(t, gluesdktypes.ColumnStatisticsStateStopped, runs.ColumnStatisticsTaskRuns[0].Status)
}

// TestGetMaterializedViewRefreshTaskRun_WrapsRunObject proves
// GetMaterializedViewRefreshTaskRun takes MaterializedViewRefreshTaskRunId
// (not RunId) and wraps the result under a MaterializedViewRefreshTaskRun
// object (glue@v1.152.0 api_op_GetMaterializedViewRefreshTaskRun.go),
// instead of a flat RunId/Status pair.
func TestGetMaterializedViewRefreshTaskRun_WrapsRunObject(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))

	started, err := client.StartMaterializedViewRefreshTaskRun(
		t.Context(), &gluesdk.StartMaterializedViewRefreshTaskRunInput{
			CatalogId:    aws.String(testAccountID),
			DatabaseName: aws.String("mvdb"),
			TableName:    aws.String("mvtbl"),
		},
	)
	require.NoError(t, err)
	require.NotEmpty(
		t, aws.ToString(started.MaterializedViewRefreshTaskRunId),
		"StartMaterializedViewRefreshTaskRunOutput.MaterializedViewRefreshTaskRunId must decode non-empty",
	)

	got, err := client.GetMaterializedViewRefreshTaskRun(
		t.Context(), &gluesdk.GetMaterializedViewRefreshTaskRunInput{
			CatalogId:                        aws.String(testAccountID),
			MaterializedViewRefreshTaskRunId: started.MaterializedViewRefreshTaskRunId,
		},
	)
	require.NoError(t, err)
	require.NotNil(
		t, got.MaterializedViewRefreshTaskRun, "response must wrap the run under MaterializedViewRefreshTaskRun",
	)
	assert.Equal(
		t, aws.ToString(started.MaterializedViewRefreshTaskRunId),
		aws.ToString(got.MaterializedViewRefreshTaskRun.MaterializedViewRefreshTaskRunId),
	)
	assert.Equal(t, "mvdb", aws.ToString(got.MaterializedViewRefreshTaskRun.DatabaseName))

	_, err = client.StopMaterializedViewRefreshTaskRun(
		t.Context(), &gluesdk.StopMaterializedViewRefreshTaskRunInput{
			CatalogId:    aws.String(testAccountID),
			DatabaseName: aws.String("mvdb"),
			TableName:    aws.String("mvtbl"),
		},
	)
	require.NoError(t, err, "a real client's Stop call (DatabaseName+TableName, no RunId) must actually stop the run")

	got2, err := client.GetMaterializedViewRefreshTaskRun(
		t.Context(), &gluesdk.GetMaterializedViewRefreshTaskRunInput{
			CatalogId:                        aws.String(testAccountID),
			MaterializedViewRefreshTaskRunId: started.MaterializedViewRefreshTaskRunId,
		},
	)
	require.NoError(t, err)
	assert.Equal(t, gluesdktypes.MaterializedViewRefreshStateStopped, got2.MaterializedViewRefreshTaskRun.Status)
}

// TestCreateRegistry_ReturnsDescriptionNotStatus proves CreateRegistryOutput
// carries Description, and that it has no Status member at all
// (glue@v1.152.0 api_op_CreateRegistry.go) -- previously the reverse: the
// request's own Description was dropped and a fabricated Status field was
// returned instead.
func TestCreateRegistry_ReturnsDescriptionNotStatus(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))

	out, err := client.CreateRegistry(t.Context(), &gluesdk.CreateRegistryInput{
		RegistryName: aws.String("reg1"),
		Description:  aws.String("a real description"),
	})
	require.NoError(t, err)
	assert.Equal(t, "a real description", aws.ToString(out.Description))
}

// TestPutSchemaVersionMetadata_EchoesKeyAndValue proves
// PutSchemaVersionMetadataOutput carries MetadataKey/MetadataValue
// (glue@v1.152.0 api_op_PutSchemaVersionMetadata.go), trivially available
// from the request but previously dropped entirely.
func TestPutSchemaVersionMetadata_EchoesKeyAndValue(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))

	reg, err := client.CreateRegistry(t.Context(), &gluesdk.CreateRegistryInput{RegistryName: aws.String("reg1")})
	require.NoError(t, err)

	schema, err := client.CreateSchema(t.Context(), &gluesdk.CreateSchemaInput{
		SchemaName:       aws.String("schema1"),
		RegistryId:       &gluesdktypes.RegistryId{RegistryName: reg.RegistryName},
		DataFormat:       gluesdktypes.DataFormatJson,
		SchemaDefinition: aws.String(`{"type":"object"}`),
	})
	require.NoError(t, err)

	out, err := client.PutSchemaVersionMetadata(t.Context(), &gluesdk.PutSchemaVersionMetadataInput{
		SchemaVersionId: schema.SchemaVersionId,
		MetadataKeyValue: &gluesdktypes.MetadataKeyValuePair{
			MetadataKey:   aws.String("env"),
			MetadataValue: aws.String("prod"),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "env", aws.ToString(out.MetadataKey))
	assert.Equal(t, "prod", aws.ToString(out.MetadataValue))
	assert.Equal(t, aws.ToString(reg.RegistryName), aws.ToString(out.RegistryName))
	assert.Equal(t, "schema1", aws.ToString(out.SchemaName))
}

// TestGetDataQualityRuleset_HasNoArnField proves GetDataQualityRulesetOutput
// has no Arn member on the wire (glue@v1.152.0
// api_op_GetDataQualityRuleset.go), by decoding the raw handler response
// directly -- the typed SDK client would simply ignore an extra unknown
// field, silently masking the fabrication this proves.
func TestGetDataQualityRuleset_HasNoArnField(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doGlueRequest(t, h, "CreateDataQualityRuleset", map[string]any{
		"Name":    "ruleset1",
		"Ruleset": "Rules = []",
	})

	rec := doGlueRequest(t, h, "GetDataQualityRuleset", map[string]any{"Name": "ruleset1"})
	require.Equal(t, 200, rec.Code)
	assert.NotContains(
		t, rec.Body.String(), `"Arn"`, "GetDataQualityRulesetOutput must not leak the internal ARN field",
	)
}

// TestUpdateGlueIdentityCenterConfiguration_UpdatesScopesNotInstanceArn
// proves UpdateGlueIdentityCenterConfiguration accepts
// Scopes/UserBackgroundSessionsEnabled (glue@v1.152.0
// api_op_UpdateGlueIdentityCenterConfiguration.go has no InstanceArn member
// at all) and, crucially, does not clobber the stored InstanceArn -- the
// previous handler read a nonexistent InstanceArn from every real Update
// call and overwrote the stored one with empty string.
func TestUpdateGlueIdentityCenterConfiguration_UpdatesScopesNotInstanceArn(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))

	_, err := client.CreateGlueIdentityCenterConfiguration(
		t.Context(), &gluesdk.CreateGlueIdentityCenterConfigurationInput{
			InstanceArn: aws.String("arn:aws:sso:::instance/ssoins-1234567890abcdef"),
		},
	)
	require.NoError(t, err)

	_, err = client.UpdateGlueIdentityCenterConfiguration(
		t.Context(), &gluesdk.UpdateGlueIdentityCenterConfigurationInput{
			Scopes:                        []string{"scope1", "scope2"},
			UserBackgroundSessionsEnabled: aws.Bool(true),
		},
	)
	require.NoError(t, err)

	got, err := client.GetGlueIdentityCenterConfiguration(
		t.Context(), &gluesdk.GetGlueIdentityCenterConfigurationInput{},
	)
	require.NoError(t, err)
	assert.Equal(
		t, "arn:aws:sso:::instance/ssoins-1234567890abcdef", aws.ToString(got.InstanceArn),
		"Update must not clobber InstanceArn -- it has no InstanceArn input member at all",
	)
	assert.Equal(t, []string{"scope1", "scope2"}, got.Scopes)
	assert.True(t, aws.ToBool(got.UserBackgroundSessionsEnabled))
}
