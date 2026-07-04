package timestreamquery_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_DescribeAccountSettings_DefaultQueryCompute verifies that
// DescribeAccountSettings always includes QueryCompute even without a prior
// UpdateAccountSettings call. Real AWS includes ComputeMode: "ON_DEMAND" by
// default; the emulator previously omitted the field entirely.
func TestParity_DescribeAccountSettings_DefaultQueryCompute(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doRequest(t, h, "DescribeAccountSettings", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResponse(t, rec)
	qc, ok := resp["QueryCompute"].(map[string]any)
	require.True(t, ok, "QueryCompute must be present in default DescribeAccountSettings response")
	assert.Equal(t, "ON_DEMAND", qc["ComputeMode"])
}

// TestParity_CancelQuery_IncludesCancellationMessage verifies that CancelQuery
// returns a CancellationMessage field. Real AWS CancelQueryOutput includes this
// field; the emulator previously returned an empty object.
func TestParity_CancelQuery_IncludesCancellationMessage(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	// Issue a query so there is a valid QueryId to cancel.
	qRec := doRequest(t, h, "Query", map[string]any{"QueryString": "SELECT 1"})
	require.Equal(t, http.StatusOK, qRec.Code)
	qResp := parseResponse(t, qRec)
	queryID, _ := qResp["QueryId"].(string)
	require.NotEmpty(t, queryID)

	rec := doRequest(t, h, "CancelQuery", map[string]any{"QueryId": queryID})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResponse(t, rec)
	msg, ok := resp["CancellationMessage"].(string)
	assert.True(t, ok, "CancellationMessage must be a string")
	assert.NotEmpty(t, msg, "CancellationMessage must not be empty")
}

// TestParity_ListScheduledQueries_TimestampsAreNumbers verifies that the
// ListScheduledQueries response encodes timestamps as JSON numbers (Unix epoch
// seconds), not strings. Real AWS JSON protocol 1.0 always sends timestamps as
// floating-point numbers; the emulator previously serialised them as strings.
func TestParity_ListScheduledQueries_TimestampsAreNumbers(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createRec := doRequest(t, h, "CreateScheduledQuery", map[string]any{
		"Name":                           "parity-ts-sq",
		"QueryString":                    "SELECT 1",
		"ScheduledQueryExecutionRoleArn": "arn:aws:iam::123:role/r",
		"ScheduleConfiguration":          map[string]any{"ScheduleExpression": "rate(1 hour)"},
		"NotificationConfiguration": map[string]any{
			"SnsConfiguration": map[string]any{"TopicArn": "arn:aws:sns:us-east-1:123:topic"},
		},
		"ErrorReportConfiguration": map[string]any{
			"S3Configuration": map[string]any{"BucketName": "my-errors-bucket"},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	listRec := doRequest(t, h, "ListScheduledQueries", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)

	resp := parseResponse(t, listRec)
	items, ok := resp["ScheduledQueries"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, items)

	item := items[0].(map[string]any)

	// JSON numbers unmarshal to float64 in Go's map[string]any.
	// Strings would unmarshal to string; either would be non-nil, so we
	// assert the concrete type is float64 (not string).
	ct, hasCT := item["CreationTime"]
	assert.True(t, hasCT, "CreationTime must be present")
	_, ctIsFloat := ct.(float64)
	assert.True(t, ctIsFloat, "CreationTime must be a JSON number (float64), got %T", ct)

	nit, hasNIT := item["NextInvocationTime"]
	assert.True(t, hasNIT, "NextInvocationTime must be present")
	_, nitIsFloat := nit.(float64)
	assert.True(t, nitIsFloat, "NextInvocationTime must be a JSON number (float64), got %T", nit)
}

// TestParity_DescribeScheduledQuery_LastRunSummaryTimestampsAreNumbers verifies
// that InvocationTime and TriggerTime in LastRunSummary are JSON numbers, not
// strings, matching the AWS JSON protocol 1.0 wire format.
func TestParity_DescribeScheduledQuery_LastRunSummaryTimestampsAreNumbers(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	createRec := doRequest(t, h, "CreateScheduledQuery", map[string]any{
		"Name":                           "parity-lrs-sq",
		"QueryString":                    "SELECT 1",
		"ScheduledQueryExecutionRoleArn": "arn:aws:iam::123:role/r",
		"ScheduleConfiguration":          map[string]any{"ScheduleExpression": "rate(1 hour)"},
		"NotificationConfiguration": map[string]any{
			"SnsConfiguration": map[string]any{"TopicArn": "arn:aws:sns:us-east-1:123:topic"},
		},
		"ErrorReportConfiguration": map[string]any{
			"S3Configuration": map[string]any{"BucketName": "my-errors-bucket"},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	arnResp := parseResponse(t, createRec)
	arn, _ := arnResp["Arn"].(string)
	require.NotEmpty(t, arn)

	// Execute so that LastRunSummary is populated.
	execRec := doRequest(t, h, "ExecuteScheduledQuery", map[string]any{
		"ScheduledQueryArn": arn,
		"InvocationTime":    1715000000.0,
	})
	require.Equal(t, http.StatusOK, execRec.Code)

	descRec := doRequest(t, h, "DescribeScheduledQuery", map[string]any{
		"ScheduledQueryArn": arn,
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	resp := parseResponse(t, descRec)
	sq, ok := resp["ScheduledQuery"].(map[string]any)
	require.True(t, ok)

	lrs, ok := sq["LastRunSummary"].(map[string]any)
	require.True(t, ok, "LastRunSummary must be present after execution")

	invTime, hasInv := lrs["InvocationTime"]
	assert.True(t, hasInv, "InvocationTime must be present")
	_, invIsFloat := invTime.(float64)
	assert.True(t, invIsFloat, "InvocationTime must be a JSON number (float64), got %T", invTime)

	trigTime, hasTrig := lrs["TriggerTime"]
	assert.True(t, hasTrig, "TriggerTime must be present")
	_, trigIsFloat := trigTime.(float64)
	assert.True(t, trigIsFloat, "TriggerTime must be a JSON number (float64), got %T", trigTime)
}
