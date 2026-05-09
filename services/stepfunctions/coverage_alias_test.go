package stepfunctions_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

func createSFNStateMachineCov(
	ctx context.Context,
	t *testing.T,
	h *stepfunctions.Handler,
	e *echo.Echo,
	name string,
) string {
	t.Helper()

	rec := sfnPost(ctx, t, h, e, "CreateStateMachine", makeSMBody(name, validPassDef, "STANDARD"))
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	arn, _ := resp["stateMachineArn"].(string)
	require.NotEmpty(t, arn)

	return arn
}

func TestSFN_StateMachineAliases(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	h, e := newSFNHandler(t)
	smARN := createSFNStateMachineCov(ctx, t, h, e, "alias-sm")

	// CreateStateMachineAlias
	rec := sfnPost(ctx, t, h, e, "CreateStateMachineAlias", fmt.Sprintf(`{
		"name": "my-alias",
		"routingConfiguration": [{"stateMachineVersionArn": "%s", "weight": 100}]
	}`, smARN))
	assert.Positive(t, rec.Code)

	// ListStateMachineAliases
	rec = sfnPost(ctx, t, h, e, "ListStateMachineAliases", fmt.Sprintf(`{"stateMachineArn": "%s"}`, smARN))
	assert.Positive(t, rec.Code)
}

func TestSFN_DescribeStateMachineForExecution(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	h, e := newSFNHandler(t)
	smARN := createSFNStateMachineCov(ctx, t, h, e, "exec-sm")

	// Start an execution
	rec := sfnPost(ctx, t, h, e, "StartExecution", fmt.Sprintf(`{
		"stateMachineArn": "%s",
		"name": "exec-1",
		"input": "{}"
	}`, smARN))
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	execARN, _ := resp["executionArn"].(string)
	require.NotEmpty(t, execARN)

	// DescribeStateMachineForExecution
	rec = sfnPost(ctx, t, h, e, "DescribeStateMachineForExecution", fmt.Sprintf(`{
		"executionArn": "%s"
	}`, execARN))
	assert.Positive(t, rec.Code)
}
