package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

// ---------------------------------------------------------------------------
// List*JobDefinitions
// ---------------------------------------------------------------------------

func createDataQualityJobDef(t *testing.T, h *sagemaker.Handler, name, endpointName string) {
	t.Helper()

	body := map[string]any{"JobDefinitionName": name}
	if endpointName != "" {
		body["DataQualityJobInput"] = map[string]any{
			"EndpointInput": map[string]any{"EndpointName": endpointName, "LocalPath": "/opt/ml/input"},
		}
	}

	rec := doSageMakerRequest(t, h, "CreateDataQualityJobDefinition", body)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
}

func TestHandler_ListDataQualityJobDefinitions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createDataQualityJobDef(t, h, "dq-alpha", "endpoint-a")
	createDataQualityJobDef(t, h, "dq-beta", "endpoint-b")
	createDataQualityJobDef(t, h, "dq-gamma", "endpoint-a")

	rec := doSageMakerRequest(t, h, "ListDataQualityJobDefinitions", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		JobDefinitionSummaries []map[string]any `json:"JobDefinitionSummaries"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp.JobDefinitionSummaries, 3)
}

func TestHandler_ListDataQualityJobDefinitions_FilterByEndpointName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createDataQualityJobDef(t, h, "dq-alpha", "endpoint-a")
	createDataQualityJobDef(t, h, "dq-beta", "endpoint-b")
	createDataQualityJobDef(t, h, "dq-gamma", "endpoint-a")

	rec := doSageMakerRequest(t, h, "ListDataQualityJobDefinitions", map[string]any{
		"EndpointName": "endpoint-a",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		JobDefinitionSummaries []map[string]any `json:"JobDefinitionSummaries"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.JobDefinitionSummaries, 2)

	for _, s := range resp.JobDefinitionSummaries {
		assert.Equal(t, "endpoint-a", s["EndpointName"])
	}
}

func TestHandler_ListDataQualityJobDefinitions_NameContainsAndSort(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createDataQualityJobDef(t, h, "alpha-job", "")
	createDataQualityJobDef(t, h, "beta-job", "")
	createDataQualityJobDef(t, h, "alpha-other", "")

	rec := doSageMakerRequest(t, h, "ListDataQualityJobDefinitions", map[string]any{
		"NameContains": "alpha",
		"SortBy":       "Name",
		"SortOrder":    "Ascending",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		JobDefinitionSummaries []map[string]any `json:"JobDefinitionSummaries"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.JobDefinitionSummaries, 2)
	assert.Equal(t, "alpha-job", resp.JobDefinitionSummaries[0]["MonitoringJobDefinitionName"])
	assert.Equal(t, "alpha-other", resp.JobDefinitionSummaries[1]["MonitoringJobDefinitionName"])
}

func TestHandler_ListDataQualityJobDefinitions_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createDataQualityJobDef(t, h, "page-a", "")
	createDataQualityJobDef(t, h, "page-b", "")
	createDataQualityJobDef(t, h, "page-c", "")

	rec := doSageMakerRequest(t, h, "ListDataQualityJobDefinitions", map[string]any{
		"SortBy":     "Name",
		"SortOrder":  "Ascending",
		"MaxResults": 2,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var page1 struct {
		NextToken              string           `json:"NextToken"`
		JobDefinitionSummaries []map[string]any `json:"JobDefinitionSummaries"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page1))
	require.Len(t, page1.JobDefinitionSummaries, 2)
	require.NotEmpty(t, page1.NextToken)

	rec = doSageMakerRequest(t, h, "ListDataQualityJobDefinitions", map[string]any{
		"SortBy":    "Name",
		"SortOrder": "Ascending",
		"NextToken": page1.NextToken,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var page2 struct {
		JobDefinitionSummaries []map[string]any `json:"JobDefinitionSummaries"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &page2))
	assert.Len(t, page2.JobDefinitionSummaries, 1)
}

func TestHandler_ListModelBiasJobDefinitions_Empty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListModelBiasJobDefinitions", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		JobDefinitionSummaries []map[string]any `json:"JobDefinitionSummaries"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp.JobDefinitionSummaries)
}

func TestHandler_ListModelQualityJobDefinitions_ReturnsCreated(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateModelQualityJobDefinition", map[string]any{
		"JobDefinitionName": "mq-list-1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "ListModelQualityJobDefinitions", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		JobDefinitionSummaries []map[string]any `json:"JobDefinitionSummaries"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.JobDefinitionSummaries, 1)
	assert.Equal(t, "mq-list-1", resp.JobDefinitionSummaries[0]["MonitoringJobDefinitionName"])
}

func TestHandler_ListModelExplainabilityJobDefinitions_ReturnsCreated(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateModelExplainabilityJobDefinition", map[string]any{
		"JobDefinitionName": "me-list-1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "ListModelExplainabilityJobDefinitions", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		JobDefinitionSummaries []map[string]any `json:"JobDefinitionSummaries"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.JobDefinitionSummaries, 1)
	assert.Equal(t, "me-list-1", resp.JobDefinitionSummaries[0]["MonitoringJobDefinitionName"])
}

// ---------------------------------------------------------------------------
// MonitoringAlert
// ---------------------------------------------------------------------------

func TestHandler_UpdateMonitoringAlert_UnknownScheduleReturnsError(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "UpdateMonitoringAlert", map[string]any{
		"MonitoringScheduleName": "no-such-schedule",
		"MonitoringAlertName":    "alert-1",
		"DatapointsToAlert":      1,
		"EvaluationPeriod":       1,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UpdateMonitoringAlert_CreatesThenUpdates(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateMonitoringSchedule", map[string]any{"MonitoringScheduleName": "mm-sched"})

	rec := doSageMakerRequest(t, h, "UpdateMonitoringAlert", map[string]any{
		"MonitoringScheduleName": "mm-sched",
		"MonitoringAlertName":    "alert-1",
		"DatapointsToAlert":      2,
		"EvaluationPeriod":       3,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["MonitoringScheduleArn"], "mm-sched")
	assert.Equal(t, "alert-1", resp["MonitoringAlertName"])

	// Second update should modify the same record, not create a duplicate.
	rec = doSageMakerRequest(t, h, "UpdateMonitoringAlert", map[string]any{
		"MonitoringScheduleName": "mm-sched",
		"MonitoringAlertName":    "alert-1",
		"DatapointsToAlert":      5,
		"EvaluationPeriod":       6,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "ListMonitoringAlerts", map[string]any{
		"MonitoringScheduleName": "mm-sched",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp struct {
		MonitoringAlertSummaries []map[string]any `json:"MonitoringAlertSummaries"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	require.Len(t, listResp.MonitoringAlertSummaries, 1)
	assert.InEpsilon(t, float64(5), listResp.MonitoringAlertSummaries[0]["DatapointsToAlert"], 0)
	assert.InEpsilon(t, float64(6), listResp.MonitoringAlertSummaries[0]["EvaluationPeriod"], 0)
	assert.Equal(t, "OK", listResp.MonitoringAlertSummaries[0]["AlertStatus"])
}

func TestHandler_ListMonitoringAlerts_UnknownScheduleReturnsError(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListMonitoringAlerts", map[string]any{
		"MonitoringScheduleName": "no-such-schedule",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListMonitoringAlerts_EmptyForFreshSchedule(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateMonitoringSchedule", map[string]any{"MonitoringScheduleName": "fresh-sched"})

	rec := doSageMakerRequest(t, h, "ListMonitoringAlerts", map[string]any{
		"MonitoringScheduleName": "fresh-sched",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		MonitoringAlertSummaries []map[string]any `json:"MonitoringAlertSummaries"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp.MonitoringAlertSummaries)
}

func TestHandler_ListMonitoringAlertHistory_FiltersByScheduleAndStatus(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	sagemaker.SeedMonitoringAlertHistory(h.Backend, "us-east-1", &sagemaker.MonitoringAlertHistoryEntry{
		MonitoringScheduleName: "sched-1",
		MonitoringAlertName:    "alert-1",
		AlertStatus:            "InAlert",
	})
	sagemaker.SeedMonitoringAlertHistory(h.Backend, "us-east-1", &sagemaker.MonitoringAlertHistoryEntry{
		MonitoringScheduleName: "sched-1",
		MonitoringAlertName:    "alert-1",
		AlertStatus:            "OK",
	})
	sagemaker.SeedMonitoringAlertHistory(h.Backend, "us-east-1", &sagemaker.MonitoringAlertHistoryEntry{
		MonitoringScheduleName: "sched-2",
		MonitoringAlertName:    "alert-2",
		AlertStatus:            "InAlert",
	})

	rec := doSageMakerRequest(t, h, "ListMonitoringAlertHistory", map[string]any{
		"MonitoringScheduleName": "sched-1",
		"StatusEquals":           "InAlert",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		MonitoringAlertHistory []map[string]any `json:"MonitoringAlertHistory"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.MonitoringAlertHistory, 1)
	assert.Equal(t, "sched-1", resp.MonitoringAlertHistory[0]["MonitoringScheduleName"])
	assert.Equal(t, "InAlert", resp.MonitoringAlertHistory[0]["AlertStatus"])
}

// ---------------------------------------------------------------------------
// MonitoringExecution
// ---------------------------------------------------------------------------

func TestHandler_ListMonitoringExecutions_FiltersByScheduleAndStatus(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	sagemaker.SeedMonitoringExecution(h.Backend, "us-east-1", &sagemaker.MonitoringExecution{
		MonitoringScheduleName:    "sched-a",
		MonitoringExecutionStatus: "Completed",
		ProcessingJobArn:          "arn:aws:sagemaker:us-east-1:000000000000:processing-job/exec-1",
	})
	sagemaker.SeedMonitoringExecution(h.Backend, "us-east-1", &sagemaker.MonitoringExecution{
		MonitoringScheduleName:    "sched-a",
		MonitoringExecutionStatus: "Failed",
		ProcessingJobArn:          "arn:aws:sagemaker:us-east-1:000000000000:processing-job/exec-2",
	})
	sagemaker.SeedMonitoringExecution(h.Backend, "us-east-1", &sagemaker.MonitoringExecution{
		MonitoringScheduleName:    "sched-b",
		MonitoringExecutionStatus: "Completed",
		ProcessingJobArn:          "arn:aws:sagemaker:us-east-1:000000000000:processing-job/exec-3",
	})

	rec := doSageMakerRequest(t, h, "ListMonitoringExecutions", map[string]any{
		"MonitoringScheduleName": "sched-a",
		"StatusEquals":           "Completed",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		MonitoringExecutionSummaries []map[string]any `json:"MonitoringExecutionSummaries"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.Len(t, resp.MonitoringExecutionSummaries, 1)
	assert.Equal(t, "sched-a", resp.MonitoringExecutionSummaries[0]["MonitoringScheduleName"])
	assert.Equal(t, "Completed", resp.MonitoringExecutionSummaries[0]["MonitoringExecutionStatus"])
}

func TestHandler_ListMonitoringExecutions_EmptyWhenNoneSeeded(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListMonitoringExecutions", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		MonitoringExecutionSummaries []map[string]any `json:"MonitoringExecutionSummaries"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp.MonitoringExecutionSummaries)
}
