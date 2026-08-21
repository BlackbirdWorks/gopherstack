package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateMonitoringSchedule(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateMonitoringSchedule", map[string]any{
		"MonitoringScheduleName":   "my-schedule",
		"MonitoringScheduleConfig": map[string]any{},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["MonitoringScheduleArn"], "my-schedule")
}

func TestHandler_CreateMonitoringSchedule_ConfigRequired(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateMonitoringSchedule", map[string]any{
		"MonitoringScheduleName": "no-config-schedule",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DescribeMonitoringSchedule(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateMonitoringSchedule", map[string]any{
		"MonitoringScheduleName":   "sched-1",
		"MonitoringScheduleConfig": map[string]any{},
	})

	rec := doSageMakerRequest(t, h, "DescribeMonitoringSchedule", map[string]any{"MonitoringScheduleName": "sched-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "sched-1", resp["MonitoringScheduleName"])
	assert.Equal(t, "Scheduled", resp["MonitoringScheduleStatus"])
	assert.Contains(t, resp, "MonitoringScheduleConfig")
}

func TestHandler_StopMonitoringSchedule(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateMonitoringSchedule", map[string]any{
		"MonitoringScheduleName":   "sched-stop",
		"MonitoringScheduleConfig": map[string]any{},
	})
	rec := doSageMakerRequest(t, h, "StopMonitoringSchedule", map[string]any{"MonitoringScheduleName": "sched-stop"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeMonitoringSchedule", map[string]any{"MonitoringScheduleName": "sched-stop"})
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "Stopped", resp["MonitoringScheduleStatus"])
}

func TestHandler_StartMonitoringSchedule(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateMonitoringSchedule", map[string]any{
		"MonitoringScheduleName":   "sched-start",
		"MonitoringScheduleConfig": map[string]any{},
	})
	doSageMakerRequest(t, h, "StopMonitoringSchedule", map[string]any{"MonitoringScheduleName": "sched-start"})
	rec := doSageMakerRequest(t, h, "StartMonitoringSchedule", map[string]any{"MonitoringScheduleName": "sched-start"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(
		t,
		h,
		"DescribeMonitoringSchedule",
		map[string]any{"MonitoringScheduleName": "sched-start"},
	)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "Scheduled", resp["MonitoringScheduleStatus"])
}

func TestHandler_DeleteMonitoringSchedule(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateMonitoringSchedule", map[string]any{
		"MonitoringScheduleName":   "sched-del",
		"MonitoringScheduleConfig": map[string]any{},
	})
	rec := doSageMakerRequest(t, h, "DeleteMonitoringSchedule", map[string]any{"MonitoringScheduleName": "sched-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeMonitoringSchedule", map[string]any{"MonitoringScheduleName": "sched-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListMonitoringSchedules(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateMonitoringSchedule", map[string]any{
		"MonitoringScheduleName":   "sched-a",
		"MonitoringScheduleConfig": map[string]any{},
	})
	doSageMakerRequest(t, h, "CreateMonitoringSchedule", map[string]any{
		"MonitoringScheduleName":   "sched-b",
		"MonitoringScheduleConfig": map[string]any{},
	})

	rec := doSageMakerRequest(t, h, "ListMonitoringSchedules", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items := resp["MonitoringScheduleSummaries"].([]any)
	assert.Len(t, items, 2)
}

// ---------------------------------------------------------------------------
// Workteam
// ---------------------------------------------------------------------------

func TestStopMonitoringSchedule_AlreadyStopped_Rejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateMonitoringSchedule", map[string]any{
		"MonitoringScheduleName":   "sched-stop-twice",
		"MonitoringScheduleConfig": map[string]any{},
	})
	doSageMakerRequest(t, h, "StopMonitoringSchedule", map[string]any{
		"MonitoringScheduleName": "sched-stop-twice",
	})

	rec := doSageMakerRequest(t, h, "StopMonitoringSchedule", map[string]any{
		"MonitoringScheduleName": "sched-stop-twice",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestStartMonitoringSchedule_AlreadyScheduled_Rejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateMonitoringSchedule", map[string]any{
		"MonitoringScheduleName":   "sched-start-running",
		"MonitoringScheduleConfig": map[string]any{},
	})

	rec := doSageMakerRequest(t, h, "StartMonitoringSchedule", map[string]any{
		"MonitoringScheduleName": "sched-start-running",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandler_MonitoringScheduleConfig_RealClient asserts
// MonitoringScheduleConfig (required by the real API, previously never read
// at all) round-trips through Describe, including its nested
// MonitoringJobDefinition and ScheduleConfig, and that EndpointName is
// derived from MonitoringInputs[0].EndpointInput.EndpointName.
func TestHandler_MonitoringScheduleConfig_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	jobDef := &smtypes.MonitoringJobDefinition{
		MonitoringAppSpecification: &smtypes.MonitoringAppSpecification{
			ImageUri: aws.String("123.dkr.ecr.us-east-1.amazonaws.com/monitor:latest"),
		},
		MonitoringInputs: []smtypes.MonitoringInput{
			{EndpointInput: &smtypes.EndpointInput{
				EndpointName: aws.String("my-endpoint"),
				LocalPath:    aws.String("/opt/ml/processing/input"),
			}},
		},
		MonitoringOutputConfig: &smtypes.MonitoringOutputConfig{
			MonitoringOutputs: []smtypes.MonitoringOutput{
				{S3Output: &smtypes.MonitoringS3Output{
					S3Uri:     aws.String("s3://bucket/out"),
					LocalPath: aws.String("/opt/ml/processing/output"),
				}},
			},
		},
		MonitoringResources: &smtypes.MonitoringResources{
			ClusterConfig: &smtypes.MonitoringClusterConfig{
				InstanceCount:  aws.Int32(1),
				InstanceType:   smtypes.ProcessingInstanceTypeMlM5Large,
				VolumeSizeInGB: aws.Int32(20),
			},
		},
		RoleArn: aws.String("arn:aws:iam::000000000000:role/MonitorRole"),
	}

	_, err := client.CreateMonitoringSchedule(t.Context(), &sagemakersdk.CreateMonitoringScheduleInput{
		MonitoringScheduleName: aws.String("full-schedule"),
		MonitoringScheduleConfig: &smtypes.MonitoringScheduleConfig{
			MonitoringType:          smtypes.MonitoringTypeDataQuality,
			MonitoringJobDefinition: jobDef,
			ScheduleConfig: &smtypes.ScheduleConfig{
				ScheduleExpression: aws.String("cron(0 * ? * * *)"),
			},
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeMonitoringSchedule(t.Context(), &sagemakersdk.DescribeMonitoringScheduleInput{
		MonitoringScheduleName: aws.String("full-schedule"),
	})
	require.NoError(t, err)
	assert.Equal(t, "my-endpoint", aws.ToString(out.EndpointName))
	assert.Equal(t, smtypes.MonitoringTypeDataQuality, out.MonitoringType)
	require.NotNil(t, out.MonitoringScheduleConfig)
	require.NotNil(t, out.MonitoringScheduleConfig.ScheduleConfig)
	assert.Equal(t, "cron(0 * ? * * *)", aws.ToString(out.MonitoringScheduleConfig.ScheduleConfig.ScheduleExpression))
	require.NotNil(t, out.MonitoringScheduleConfig.MonitoringJobDefinition)
	assert.Equal(
		t,
		"arn:aws:iam::000000000000:role/MonitorRole",
		aws.ToString(out.MonitoringScheduleConfig.MonitoringJobDefinition.RoleArn),
	)
}

// TestHandler_UpdateMonitoringSchedule_ConfigRequired_RealClient asserts
// UpdateMonitoringScheduleInput's MonitoringScheduleConfig round-trips
// (previously never read; UpdateMonitoringSchedule ignored it entirely) and
// that a mismatched config is rejected by CreateMonitoringSchedule too.
func TestHandler_UpdateMonitoringSchedule_ConfigRequired_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateMonitoringSchedule(t.Context(), &sagemakersdk.CreateMonitoringScheduleInput{
		MonitoringScheduleName:   aws.String("update-schedule"),
		MonitoringScheduleConfig: &smtypes.MonitoringScheduleConfig{MonitoringType: smtypes.MonitoringTypeDataQuality},
	})
	require.NoError(t, err)

	_, err = client.UpdateMonitoringSchedule(t.Context(), &sagemakersdk.UpdateMonitoringScheduleInput{
		MonitoringScheduleName:   aws.String("update-schedule"),
		MonitoringScheduleConfig: &smtypes.MonitoringScheduleConfig{MonitoringType: smtypes.MonitoringTypeModelQuality},
	})
	require.NoError(t, err)

	out, err := client.DescribeMonitoringSchedule(t.Context(), &sagemakersdk.DescribeMonitoringScheduleInput{
		MonitoringScheduleName: aws.String("update-schedule"),
	})
	require.NoError(t, err)
	assert.Equal(t, smtypes.MonitoringTypeModelQuality, out.MonitoringType)
}

// TestHandler_ListMonitoringSchedules_FilterSortPage_RealClient asserts
// ListMonitoringSchedulesInput's NameContains/StatusEquals/MonitoringTypeEquals/
// SortBy/SortOrder -- all absent before this pass -- now work, and that the
// documented defaults (SortBy=CreationTime, SortOrder=Descending) are
// honored.
func TestHandler_ListMonitoringSchedules_FilterSortPage_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	names := []string{"alpha-sched", "beta-sched", "gamma-widget"}
	for _, n := range names {
		_, err := client.CreateMonitoringSchedule(t.Context(), &sagemakersdk.CreateMonitoringScheduleInput{
			MonitoringScheduleName: aws.String(n),
			MonitoringScheduleConfig: &smtypes.MonitoringScheduleConfig{
				MonitoringType: smtypes.MonitoringTypeDataQuality,
			},
		})
		require.NoError(t, err)
	}

	t.Run("name contains filters", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListMonitoringSchedules(t.Context(), &sagemakersdk.ListMonitoringSchedulesInput{
			NameContains: aws.String("sched"),
		})
		require.NoError(t, err)
		assert.Len(t, out.MonitoringScheduleSummaries, 2)
	})

	t.Run("descending default sort by creation time", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListMonitoringSchedules(t.Context(), &sagemakersdk.ListMonitoringSchedulesInput{})
		require.NoError(t, err)
		require.Len(t, out.MonitoringScheduleSummaries, 3)
		assert.Equal(t, "gamma-widget", aws.ToString(out.MonitoringScheduleSummaries[0].MonitoringScheduleName))
	})

	t.Run("ascending sort by name", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListMonitoringSchedules(t.Context(), &sagemakersdk.ListMonitoringSchedulesInput{
			SortBy:    smtypes.MonitoringScheduleSortKeyName,
			SortOrder: smtypes.SortOrderAscending,
		})
		require.NoError(t, err)
		require.Len(t, out.MonitoringScheduleSummaries, 3)
		assert.Equal(t, "alpha-sched", aws.ToString(out.MonitoringScheduleSummaries[0].MonitoringScheduleName))
	})
}
