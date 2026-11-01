package stepfunctions_test

import (
	"encoding/json/v2"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func TestHandler_CreateStateMachine_PropagatesTracingAndLogging(t *testing.T) {
	t.Parallel()

	body := map[string]any{
		"name":       "tracing-sm",
		"definition": validPassDef,
		"roleArn":    "arn:aws:iam::000000000000:role/sfn",
		"tracingConfiguration": map[string]any{
			"enabled": true,
		},
		"loggingConfiguration": map[string]any{
			"level":                "ALL",
			"includeExecutionData": true,
			"destinations": []map[string]any{{
				"cloudWatchLogsLogGroup": map[string]any{
					"logGroupArn": "arn:aws:logs:us-east-1:000000000000:log-group:sfn-logs:*",
				},
			}},
		},
	}

	raw, err := json.Marshal(body, jsonx.V1())
	require.NoError(t, err)

	ctx := t.Context()
	h, e := newSFNHandler(t)

	rec := sfnPost(ctx, t, h, e, "CreateStateMachine", string(raw))
	require.Equal(t, http.StatusOK, rec.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut, jsonx.V1()))

	arnStr, _ := createOut["stateMachineArn"].(string)
	require.NotEmpty(t, arnStr)

	descBody, err := json.Marshal(map[string]any{"stateMachineArn": arnStr}, jsonx.V1())
	require.NoError(t, err)

	descRec := sfnPost(ctx, t, h, e, "DescribeStateMachine", string(descBody))
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut, jsonx.V1()))

	tracing, _ := descOut["tracingConfiguration"].(map[string]any)
	require.NotNil(t, tracing)
	assert.Equal(t, true, tracing["enabled"])

	logging, _ := descOut["loggingConfiguration"].(map[string]any)
	require.NotNil(t, logging)
	assert.Equal(t, "ALL", logging["level"])
	assert.Equal(t, true, logging["includeExecutionData"])
}

func TestHandler_UpdateStateMachine_PropagatesTracing(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	h, e := newSFNHandler(t)
	arn := createSM(ctx, t, h, e, "update-tracing-sm")

	updBody, err := json.Marshal(map[string]any{
		"stateMachineArn":      arn,
		"tracingConfiguration": map[string]any{"enabled": true},
	}, jsonx.V1())
	require.NoError(t, err)

	rec := sfnPost(ctx, t, h, e, "UpdateStateMachine", string(updBody))
	require.Equal(t, http.StatusOK, rec.Code)

	descBody, _ := json.Marshal(map[string]any{"stateMachineArn": arn}, jsonx.V1())

	descRec := sfnPost(ctx, t, h, e, "DescribeStateMachine", string(descBody))
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut, jsonx.V1()))

	tracing, _ := descOut["tracingConfiguration"].(map[string]any)
	require.NotNil(t, tracing)
	assert.Equal(t, true, tracing["enabled"])
}
