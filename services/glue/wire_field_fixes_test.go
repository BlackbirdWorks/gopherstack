package glue_test

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// TestStartColumnStatisticsTaskRun_StatusIsLegalEnumMember drives
// StartColumnStatisticsTaskRun/GetColumnStatisticsTaskRun through the real
// aws-sdk-go-v2 client. ColumnStatisticsTaskRun.Status is
// types.ColumnStatisticsState (STARTING/RUNNING/SUCCEEDED/FAILED/STOPPED --
// glue@v1.152.0 types/enums.go:225); the backend previously set "STARTED",
// which is not a member, so a real client's waiter for this run would never
// match any case and poll until timeout.
func TestStartColumnStatisticsTaskRun_StatusIsLegalEnumMember(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))
	ctx := t.Context()

	_, err := client.CreateDatabase(ctx, &gluesdk.CreateDatabaseInput{
		DatabaseInput: &types.DatabaseInput{Name: aws.String("db1")},
	})
	require.NoError(t, err)
	_, err = client.CreateTable(ctx, &gluesdk.CreateTableInput{
		DatabaseName: aws.String("db1"),
		TableInput:   &types.TableInput{Name: aws.String("tbl1")},
	})
	require.NoError(t, err)

	_, err = client.StartColumnStatisticsTaskRun(ctx, &gluesdk.StartColumnStatisticsTaskRunInput{
		DatabaseName: aws.String("db1"),
		TableName:    aws.String("tbl1"),
		Role:         aws.String("arn:aws:iam::" + testAccountID + ":role/glue-role"),
	})
	require.NoError(t, err)

	all, err := client.GetColumnStatisticsTaskRuns(ctx, &gluesdk.GetColumnStatisticsTaskRunsInput{
		DatabaseName: aws.String("db1"),
		TableName:    aws.String("tbl1"),
	})
	require.NoError(t, err)
	require.Len(t, all.ColumnStatisticsTaskRuns, 1)

	out, err := client.GetColumnStatisticsTaskRun(ctx, &gluesdk.GetColumnStatisticsTaskRunInput{
		ColumnStatisticsTaskRunId: all.ColumnStatisticsTaskRuns[0].ColumnStatisticsTaskRunId,
	})
	require.NoError(t, err)
	require.NotNil(t, out.ColumnStatisticsTaskRun)
	assert.Equal(t, types.ColumnStatisticsStateStarting, out.ColumnStatisticsTaskRun.Status)
}

// TestCancelDataQualityRuleRecommendationRun_StatusIsLegalEnumMember drives
// Start/Cancel/GetDataQualityRuleRecommendationRun through the real client.
// GetDataQualityRuleRecommendationRunOutput.Status is types.TaskStatusType
// (STARTING/RUNNING/STOPPING/STOPPED/SUCCEEDED/FAILED/TIMEOUT --
// glue@v1.152.0 types/enums.go:3323); the backend previously set "CANCELLED"
// on cancel, which is not a member of TaskStatusType.
func TestCancelDataQualityRuleRecommendationRun_StatusIsLegalEnumMember(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	client := newTestGlueClient(t, glue.NewHandler(backend))
	ctx := t.Context()

	started, err := client.StartDataQualityRuleRecommendationRun(
		ctx,
		&gluesdk.StartDataQualityRuleRecommendationRunInput{
			DataSource: &types.DataSource{
				GlueTable: &types.GlueTable{DatabaseName: aws.String("db1"), TableName: aws.String("tbl1")},
			},
			Role: aws.String("arn:aws:iam::" + testAccountID + ":role/glue-role"),
		},
	)
	require.NoError(t, err)

	_, err = client.CancelDataQualityRuleRecommendationRun(ctx, &gluesdk.CancelDataQualityRuleRecommendationRunInput{
		RunId: started.RunId,
	})
	require.NoError(t, err)

	out, err := client.GetDataQualityRuleRecommendationRun(ctx, &gluesdk.GetDataQualityRuleRecommendationRunInput{
		RunId: started.RunId,
	})
	require.NoError(t, err)
	assert.Equal(t, types.TaskStatusTypeStopped, out.Status)
}

// TestCancelDataQualityRulesetEvaluationRun_StatusIsLegalEnumMember covers
// the same TaskStatusType bug on DataQualityEvaluationRun.Status. Unlike
// GetDataQualityRuleRecommendationRunOutput, GetDataQualityRulesetEvaluationRunOutput
// flattens Status (and CompletedOn/DataSource/...) at the response root in
// the real API (glue@v1.152.0 api_op_GetDataQualityRulesetEvaluationRun.go),
// but this backend's handler wraps them under a "DataQualityEvaluationRun"
// key instead -- a pre-existing, unrelated wire-shape bug (not fixed here;
// flagged separately) that stops the real SDK client from decoding Status at
// all. This test therefore reads the raw response body rather than the
// SDK's decoded Status field, and still compares against the typed enum
// constant's wire value, not a bare literal.
func TestCancelDataQualityRulesetEvaluationRun_StatusIsLegalEnumMember(t *testing.T) {
	t.Parallel()

	backend := glue.NewInMemoryBackend(testAccountID, testRegion)
	h := glue.NewHandler(backend)

	doGlueRequest(t, h, "CreateDataQualityRuleset", map[string]any{
		"Name":    "my-ruleset",
		"Ruleset": "Rules = [ RowCount > 100 ]",
	})

	startRec := doGlueRequest(t, h, "StartDataQualityRulesetEvaluationRun", map[string]any{
		"RulesetNames": []string{"my-ruleset"},
	})
	require.Equal(t, 200, startRec.Code)
	var startOut map[string]string
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))
	runID := startOut["RunId"]

	cancelRec := doGlueRequest(t, h, "CancelDataQualityRulesetEvaluationRun", map[string]any{"RunId": runID})
	require.Equal(t, 200, cancelRec.Code)

	getRec := doGlueRequest(t, h, "GetDataQualityRulesetEvaluationRun", map[string]any{"RunId": runID})
	require.Equal(t, 200, getRec.Code)

	var getOut struct {
		DataQualityEvaluationRun struct {
			Status string `json:"Status"`
		} `json:"DataQualityEvaluationRun"`
	}
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	assert.Equal(t, string(types.TaskStatusTypeStopped), getOut.DataQualityEvaluationRun.Status)
}
