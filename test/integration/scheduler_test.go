package integration_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func schedulerPost(t *testing.T, action string, body any) *http.Response {
	t.Helper()

	bodyBytes, err := json.Marshal(body)
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint, bytes.NewReader(bodyBytes))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AWSScheduler."+action)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	return resp
}

func schedulerReadBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return string(data)
}

func TestIntegration_Scheduler_CreateSchedule(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := schedulerPost(t, "CreateSchedule", map[string]any{
		"Name":               "integ-schedule",
		"ScheduleExpression": "rate(5 minutes)",
		"Target": map[string]string{
			"Arn":     "arn:aws:lambda:us-east-1:000000000000:function:my-fn",
			"RoleArn": "arn:aws:iam::000000000000:role/scheduler-role",
		},
		"FlexibleTimeWindow": map[string]string{"Mode": "OFF"},
		"State":              "ENABLED",
	})
	body := schedulerReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "ScheduleArn")
}

func TestIntegration_Scheduler_ListSchedules(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	schedulerPost(t, "CreateSchedule", map[string]any{
		"Name":               "list-schedule-test",
		"ScheduleExpression": "rate(1 hour)",
		"Target": map[string]string{
			"Arn":     "arn:aws:lambda:us-east-1:000000000000:function:fn",
			"RoleArn": "arn:aws:iam::000000000000:role/r",
		},
		"FlexibleTimeWindow": map[string]string{"Mode": "OFF"},
		"State":              "ENABLED",
	})

	resp := schedulerPost(t, "ListSchedules", map[string]any{})
	body := schedulerReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "Schedules")
}

func TestIntegration_Scheduler_GetSchedule(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	schedulerPost(t, "CreateSchedule", map[string]any{
		"Name":               "get-schedule-test",
		"ScheduleExpression": "cron(0 12 * * ? *)",
		"Target": map[string]string{
			"Arn":     "arn:aws:lambda:us-east-1:000000000000:function:fn",
			"RoleArn": "arn:aws:iam::000000000000:role/r",
		},
		"FlexibleTimeWindow": map[string]string{"Mode": "OFF"},
		"State":              "ENABLED",
	})

	resp := schedulerPost(t, "GetSchedule", map[string]any{
		"Name": "get-schedule-test",
	})
	body := schedulerReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "get-schedule-test")
}

func TestIntegration_Scheduler_UpdateSchedule(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	schedulerPost(t, "CreateSchedule", map[string]any{
		"Name":               "update-schedule-test",
		"ScheduleExpression": "rate(5 minutes)",
		"Target": map[string]string{
			"Arn":     "arn:aws:lambda:us-east-1:000000000000:function:fn",
			"RoleArn": "arn:aws:iam::000000000000:role/r",
		},
		"FlexibleTimeWindow": map[string]string{"Mode": "OFF"},
		"State":              "ENABLED",
	})

	resp := schedulerPost(t, "UpdateSchedule", map[string]any{
		"Name":               "update-schedule-test",
		"ScheduleExpression": "rate(10 minutes)",
		"Target": map[string]string{
			"Arn":     "arn:aws:lambda:us-east-1:000000000000:function:fn",
			"RoleArn": "arn:aws:iam::000000000000:role/r",
		},
		"FlexibleTimeWindow": map[string]string{"Mode": "OFF"},
		"State":              "DISABLED",
	})
	body := schedulerReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "ScheduleArn")
}

func TestIntegration_Scheduler_DeleteSchedule(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	schedulerPost(t, "CreateSchedule", map[string]any{
		"Name":               "delete-schedule-test",
		"ScheduleExpression": "rate(1 day)",
		"Target": map[string]string{
			"Arn":     "arn:aws:lambda:us-east-1:000000000000:function:fn",
			"RoleArn": "arn:aws:iam::000000000000:role/r",
		},
		"FlexibleTimeWindow": map[string]string{"Mode": "OFF"},
		"State":              "ENABLED",
	})

	resp := schedulerPost(t, "DeleteSchedule", map[string]any{
		"Name": "delete-schedule-test",
	})
	body := schedulerReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
}
