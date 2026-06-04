package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// ModelPackageGroup: dependency guard
// ---------------------------------------------------------------------------

func TestAccuracy_Batch2_DeleteModelPackageGroup_WithPackages_Conflict(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelPackageGroup", map[string]any{
		"ModelPackageGroupName": "grp-with-pkgs",
	})
	doSageMakerRequest(t, h, "CreateModelPackage", map[string]any{
		"ModelPackageName":      "pkg-in-group",
		"ModelPackageGroupName": "grp-with-pkgs",
	})

	rec := doSageMakerRequest(t, h, "DeleteModelPackageGroup", map[string]any{
		"ModelPackageGroupName": "grp-with-pkgs",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAccuracy_Batch2_DeleteModelPackageGroup_AfterPackagesRemoved_OK(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelPackageGroup", map[string]any{
		"ModelPackageGroupName": "grp-empty",
	})
	doSageMakerRequest(t, h, "CreateModelPackage", map[string]any{
		"ModelPackageName":      "pkg-to-remove",
		"ModelPackageGroupName": "grp-empty",
	})
	doSageMakerRequest(t, h, "DeleteModelPackage", map[string]any{
		"ModelPackageName": "pkg-to-remove",
	})

	rec := doSageMakerRequest(t, h, "DeleteModelPackageGroup", map[string]any{
		"ModelPackageGroupName": "grp-empty",
	})

	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---------------------------------------------------------------------------
// AutoMLJob: initial status and terminal-state stop guard
// ---------------------------------------------------------------------------

func TestAccuracy_Batch2_AutoMLJob_InitialStatus_InProgress(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateAutoMLJob", map[string]any{
		"AutoMLJobName": "automl-status",
		"RoleArn":       "arn:test",
	})

	rec := doSageMakerRequest(t, h, "DescribeAutoMLJob", map[string]any{
		"AutoMLJobName": "automl-status",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "InProgress", resp["AutoMLJobStatus"])
}

func TestAccuracy_Batch2_StopAutoMLJob_Terminal_Rejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateAutoMLJob", map[string]any{
		"AutoMLJobName": "automl-terminal",
		"RoleArn":       "arn:test",
	})
	doSageMakerRequest(t, h, "StopAutoMLJob", map[string]any{
		"AutoMLJobName": "automl-terminal",
	})

	rec := doSageMakerRequest(t, h, "StopAutoMLJob", map[string]any{
		"AutoMLJobName": "automl-terminal",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// CompilationJob: initial status and terminal-state stop guard
// ---------------------------------------------------------------------------

func TestAccuracy_Batch2_CompilationJob_InitialStatus_InProgress(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateCompilationJob", map[string]any{
		"CompilationJobName": "compile-status",
		"RoleArn":            "arn:test",
	})

	rec := doSageMakerRequest(t, h, "DescribeCompilationJob", map[string]any{
		"CompilationJobName": "compile-status",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "INPROGRESS", resp["CompilationJobStatus"])
}

func TestAccuracy_Batch2_StopCompilationJob_Terminal_Rejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateCompilationJob", map[string]any{
		"CompilationJobName": "compile-terminal",
		"RoleArn":            "arn:test",
	})
	doSageMakerRequest(t, h, "StopCompilationJob", map[string]any{
		"CompilationJobName": "compile-terminal",
	})

	rec := doSageMakerRequest(t, h, "StopCompilationJob", map[string]any{
		"CompilationJobName": "compile-terminal",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// Image: version guard on delete
// ---------------------------------------------------------------------------

func TestAccuracy_Batch2_DeleteImage_WithVersions_Conflict(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateImage", map[string]any{
		"ImageName": "img-has-ver",
		"RoleArn":   "arn:test",
	})
	doSageMakerRequest(t, h, "CreateImageVersion", map[string]any{
		"ImageName": "img-has-ver",
	})

	rec := doSageMakerRequest(t, h, "DeleteImage", map[string]any{
		"ImageName": "img-has-ver",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAccuracy_Batch2_DeleteImage_AfterVersionsRemoved_OK(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateImage", map[string]any{
		"ImageName": "img-ver-cleanup",
		"RoleArn":   "arn:test",
	})
	doSageMakerRequest(t, h, "CreateImageVersion", map[string]any{
		"ImageName": "img-ver-cleanup",
	})
	doSageMakerRequest(t, h, "DeleteImageVersion", map[string]any{
		"ImageName": "img-ver-cleanup",
		"Version":   1,
	})

	rec := doSageMakerRequest(t, h, "DeleteImage", map[string]any{
		"ImageName": "img-ver-cleanup",
	})

	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---------------------------------------------------------------------------
// MonitoringSchedule: stop/start state guards
// ---------------------------------------------------------------------------

func TestAccuracy_Batch2_StopMonitoringSchedule_AlreadyStopped_Rejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateMonitoringSchedule", map[string]any{
		"MonitoringScheduleName": "sched-stop-twice",
	})
	doSageMakerRequest(t, h, "StopMonitoringSchedule", map[string]any{
		"MonitoringScheduleName": "sched-stop-twice",
	})

	rec := doSageMakerRequest(t, h, "StopMonitoringSchedule", map[string]any{
		"MonitoringScheduleName": "sched-stop-twice",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAccuracy_Batch2_StartMonitoringSchedule_AlreadyScheduled_Rejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateMonitoringSchedule", map[string]any{
		"MonitoringScheduleName": "sched-start-running",
	})

	rec := doSageMakerRequest(t, h, "StartMonitoringSchedule", map[string]any{
		"MonitoringScheduleName": "sched-start-running",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
