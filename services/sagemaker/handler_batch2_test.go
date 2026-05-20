package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// ModelPackage
// ---------------------------------------------------------------------------

func TestHandler_CreateModelPackage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateModelPackage", map[string]any{
		"ModelPackageName": "my-pkg",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["ModelPackageArn"], "my-pkg")
}

func TestHandler_DescribeModelPackage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelPackage", map[string]any{"ModelPackageName": "pkg-1"})

	rec := doSageMakerRequest(t, h, "DescribeModelPackage", map[string]any{"ModelPackageName": "pkg-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "pkg-1", resp["ModelPackageName"])
}

func TestHandler_DeleteModelPackage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelPackage", map[string]any{"ModelPackageName": "pkg-del"})
	rec := doSageMakerRequest(t, h, "DeleteModelPackage", map[string]any{"ModelPackageName": "pkg-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeModelPackage", map[string]any{"ModelPackageName": "pkg-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListModelPackages(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelPackage", map[string]any{"ModelPackageName": "pkg-a"})
	doSageMakerRequest(t, h, "CreateModelPackage", map[string]any{"ModelPackageName": "pkg-b"})

	rec := doSageMakerRequest(t, h, "ListModelPackages", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items := resp["ModelPackageSummaryList"].([]any)
	assert.Len(t, items, 2)
}

// ---------------------------------------------------------------------------
// ModelPackageGroup
// ---------------------------------------------------------------------------

func TestHandler_CreateModelPackageGroup(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateModelPackageGroup", map[string]any{
		"ModelPackageGroupName": "my-group",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["ModelPackageGroupArn"], "my-group")
}

func TestHandler_DescribeModelPackageGroup(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelPackageGroup", map[string]any{"ModelPackageGroupName": "grp-1"})

	rec := doSageMakerRequest(t, h, "DescribeModelPackageGroup", map[string]any{"ModelPackageGroupName": "grp-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "grp-1", resp["ModelPackageGroupName"])
	assert.Equal(t, "Completed", resp["ModelPackageGroupStatus"])
}

func TestHandler_DeleteModelPackageGroup(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelPackageGroup", map[string]any{"ModelPackageGroupName": "grp-del"})
	rec := doSageMakerRequest(t, h, "DeleteModelPackageGroup", map[string]any{"ModelPackageGroupName": "grp-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeModelPackageGroup", map[string]any{"ModelPackageGroupName": "grp-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListModelPackageGroups(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelPackageGroup", map[string]any{"ModelPackageGroupName": "grp-a"})
	doSageMakerRequest(t, h, "CreateModelPackageGroup", map[string]any{"ModelPackageGroupName": "grp-b"})

	rec := doSageMakerRequest(t, h, "ListModelPackageGroups", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items := resp["ModelPackageGroupSummaryList"].([]any)
	assert.Len(t, items, 2)
}

// ---------------------------------------------------------------------------
// AutoMLJob
// ---------------------------------------------------------------------------

func TestHandler_CreateAutoMLJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateAutoMLJob", map[string]any{
		"AutoMLJobName": "my-job",
		"RoleArn":       "arn:aws:iam::000000000000:role/test",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["AutoMLJobArn"], "my-job")
}

func TestHandler_DescribeAutoMLJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateAutoMLJob", map[string]any{"AutoMLJobName": "job-1", "RoleArn": "arn:test"})

	rec := doSageMakerRequest(t, h, "DescribeAutoMLJob", map[string]any{"AutoMLJobName": "job-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "job-1", resp["AutoMLJobName"])
	assert.Equal(t, "Completed", resp["AutoMLJobStatus"])
}

func TestHandler_StopAutoMLJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateAutoMLJob", map[string]any{"AutoMLJobName": "job-stop", "RoleArn": "arn:test"})
	rec := doSageMakerRequest(t, h, "StopAutoMLJob", map[string]any{"AutoMLJobName": "job-stop"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeAutoMLJob", map[string]any{"AutoMLJobName": "job-stop"})
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "Stopped", resp["AutoMLJobStatus"])
}

func TestHandler_ListAutoMLJobs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateAutoMLJob", map[string]any{"AutoMLJobName": "job-a", "RoleArn": "arn:test"})
	doSageMakerRequest(t, h, "CreateAutoMLJob", map[string]any{"AutoMLJobName": "job-b", "RoleArn": "arn:test"})

	rec := doSageMakerRequest(t, h, "ListAutoMLJobs", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items := resp["AutoMLJobSummaries"].([]any)
	assert.Len(t, items, 2)
}

// ---------------------------------------------------------------------------
// CodeRepository
// ---------------------------------------------------------------------------

func TestHandler_CreateCodeRepository(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateCodeRepository", map[string]any{
		"CodeRepositoryName": "my-repo",
		"GitConfig":          map[string]string{"RepositoryUrl": "https://github.com/test/repo"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["CodeRepositoryArn"], "my-repo")
}

func TestHandler_DescribeCodeRepository(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateCodeRepository", map[string]any{"CodeRepositoryName": "repo-1"})

	rec := doSageMakerRequest(t, h, "DescribeCodeRepository", map[string]any{"CodeRepositoryName": "repo-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "repo-1", resp["CodeRepositoryName"])
}

func TestHandler_UpdateCodeRepository(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateCodeRepository", map[string]any{"CodeRepositoryName": "repo-upd"})
	rec := doSageMakerRequest(t, h, "UpdateCodeRepository", map[string]any{
		"CodeRepositoryName": "repo-upd",
		"GitConfig":          map[string]string{"Branch": "main"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["CodeRepositoryArn"], "repo-upd")
}

func TestHandler_DeleteCodeRepository(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateCodeRepository", map[string]any{"CodeRepositoryName": "repo-del"})
	rec := doSageMakerRequest(t, h, "DeleteCodeRepository", map[string]any{"CodeRepositoryName": "repo-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeCodeRepository", map[string]any{"CodeRepositoryName": "repo-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListCodeRepositories(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateCodeRepository", map[string]any{"CodeRepositoryName": "repo-x"})
	doSageMakerRequest(t, h, "CreateCodeRepository", map[string]any{"CodeRepositoryName": "repo-y"})

	rec := doSageMakerRequest(t, h, "ListCodeRepositories", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items := resp["CodeRepositorySummaryList"].([]any)
	assert.Len(t, items, 2)
}

// ---------------------------------------------------------------------------
// Project
// ---------------------------------------------------------------------------

func TestHandler_CreateProject(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateProject", map[string]any{
		"ProjectName": "my-project",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["ProjectArn"], "my-project")
	assert.NotEmpty(t, resp["ProjectId"])
}

func TestHandler_DescribeProject(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateProject", map[string]any{"ProjectName": "proj-1"})

	rec := doSageMakerRequest(t, h, "DescribeProject", map[string]any{"ProjectName": "proj-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "proj-1", resp["ProjectName"])
	assert.Equal(t, "CreateCompleted", resp["ProjectStatus"])
}

func TestHandler_DeleteProject(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateProject", map[string]any{"ProjectName": "proj-del"})
	rec := doSageMakerRequest(t, h, "DeleteProject", map[string]any{"ProjectName": "proj-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeProject", map[string]any{"ProjectName": "proj-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListProjects(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateProject", map[string]any{"ProjectName": "proj-a"})
	doSageMakerRequest(t, h, "CreateProject", map[string]any{"ProjectName": "proj-b"})

	rec := doSageMakerRequest(t, h, "ListProjects", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items := resp["ProjectSummaryList"].([]any)
	assert.Len(t, items, 2)
}

// ---------------------------------------------------------------------------
// Space
// ---------------------------------------------------------------------------

func TestHandler_CreateSpace(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateSpace", map[string]any{
		"DomainId":  "d-abc123",
		"SpaceName": "my-space",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["SpaceArn"], "my-space")
}

func TestHandler_DescribeSpace(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateSpace", map[string]any{"DomainId": "d-1", "SpaceName": "space-1"})

	rec := doSageMakerRequest(t, h, "DescribeSpace", map[string]any{"DomainId": "d-1", "SpaceName": "space-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "space-1", resp["SpaceName"])
	assert.Equal(t, "InService", resp["SpaceStatus"])
}

func TestHandler_DeleteSpace(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateSpace", map[string]any{"DomainId": "d-1", "SpaceName": "space-del"})
	rec := doSageMakerRequest(t, h, "DeleteSpace", map[string]any{"DomainId": "d-1", "SpaceName": "space-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeSpace", map[string]any{"DomainId": "d-1", "SpaceName": "space-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListSpaces(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateSpace", map[string]any{"DomainId": "d-1", "SpaceName": "sp-a"})
	doSageMakerRequest(t, h, "CreateSpace", map[string]any{"DomainId": "d-1", "SpaceName": "sp-b"})
	doSageMakerRequest(t, h, "CreateSpace", map[string]any{"DomainId": "d-2", "SpaceName": "sp-c"})

	rec := doSageMakerRequest(t, h, "ListSpaces", map[string]any{"DomainIdEquals": "d-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items := resp["Spaces"].([]any)
	assert.Len(t, items, 2)
}

// ---------------------------------------------------------------------------
// Image
// ---------------------------------------------------------------------------

func TestHandler_CreateImage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateImage", map[string]any{
		"ImageName": "my-image",
		"RoleArn":   "arn:aws:iam::000000000000:role/test",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["ImageArn"], "my-image")
}

func TestHandler_DescribeImage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateImage", map[string]any{"ImageName": "img-1", "RoleArn": "arn:test"})

	rec := doSageMakerRequest(t, h, "DescribeImage", map[string]any{"ImageName": "img-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "img-1", resp["ImageName"])
	assert.Equal(t, "CREATED", resp["ImageStatus"])
}

func TestHandler_DeleteImage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateImage", map[string]any{"ImageName": "img-del", "RoleArn": "arn:test"})
	rec := doSageMakerRequest(t, h, "DeleteImage", map[string]any{"ImageName": "img-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeImage", map[string]any{"ImageName": "img-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListImages(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateImage", map[string]any{"ImageName": "img-a", "RoleArn": "arn:test"})
	doSageMakerRequest(t, h, "CreateImage", map[string]any{"ImageName": "img-b", "RoleArn": "arn:test"})

	rec := doSageMakerRequest(t, h, "ListImages", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items := resp["Images"].([]any)
	assert.Len(t, items, 2)
}

// ---------------------------------------------------------------------------
// ImageVersion
// ---------------------------------------------------------------------------

func TestHandler_CreateImageVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateImage", map[string]any{"ImageName": "img-ver", "RoleArn": "arn:test"})

	rec := doSageMakerRequest(t, h, "CreateImageVersion", map[string]any{"ImageName": "img-ver"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["ImageVersionArn"], "img-ver")
}

func TestHandler_DescribeImageVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateImage", map[string]any{"ImageName": "img-v", "RoleArn": "arn:test"})
	doSageMakerRequest(t, h, "CreateImageVersion", map[string]any{"ImageName": "img-v"})

	rec := doSageMakerRequest(t, h, "DescribeImageVersion", map[string]any{"ImageName": "img-v", "Version": 1})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.InEpsilon(t, float64(1), resp["Version"], 0.001)
	assert.Equal(t, "CREATED", resp["ImageVersionStatus"])
}

func TestHandler_ListImageVersions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateImage", map[string]any{"ImageName": "img-lv", "RoleArn": "arn:test"})
	doSageMakerRequest(t, h, "CreateImageVersion", map[string]any{"ImageName": "img-lv"})
	doSageMakerRequest(t, h, "CreateImageVersion", map[string]any{"ImageName": "img-lv"})

	rec := doSageMakerRequest(t, h, "ListImageVersions", map[string]any{"ImageName": "img-lv"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items := resp["ImageVersions"].([]any)
	assert.Len(t, items, 2)
}

// ---------------------------------------------------------------------------
// CompilationJob
// ---------------------------------------------------------------------------

func TestHandler_CreateCompilationJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateCompilationJob", map[string]any{
		"CompilationJobName": "my-compile",
		"RoleArn":            "arn:test",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["CompilationJobArn"], "my-compile")
}

func TestHandler_DescribeCompilationJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(
		t,
		h,
		"CreateCompilationJob",
		map[string]any{"CompilationJobName": "cj-1", "RoleArn": "arn:test"},
	)

	rec := doSageMakerRequest(t, h, "DescribeCompilationJob", map[string]any{"CompilationJobName": "cj-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "cj-1", resp["CompilationJobName"])
	assert.Equal(t, "COMPLETED", resp["CompilationJobStatus"])
}

func TestHandler_StopCompilationJob(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(
		t,
		h,
		"CreateCompilationJob",
		map[string]any{"CompilationJobName": "cj-stop", "RoleArn": "arn:test"},
	)
	rec := doSageMakerRequest(t, h, "StopCompilationJob", map[string]any{"CompilationJobName": "cj-stop"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeCompilationJob", map[string]any{"CompilationJobName": "cj-stop"})
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "STOPPED", resp["CompilationJobStatus"])
}

func TestHandler_ListCompilationJobs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(
		t,
		h,
		"CreateCompilationJob",
		map[string]any{"CompilationJobName": "cj-a", "RoleArn": "arn:test"},
	)
	doSageMakerRequest(
		t,
		h,
		"CreateCompilationJob",
		map[string]any{"CompilationJobName": "cj-b", "RoleArn": "arn:test"},
	)

	rec := doSageMakerRequest(t, h, "ListCompilationJobs", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items := resp["CompilationJobSummaries"].([]any)
	assert.Len(t, items, 2)
}

// ---------------------------------------------------------------------------
// MonitoringSchedule
// ---------------------------------------------------------------------------

func TestHandler_CreateMonitoringSchedule(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateMonitoringSchedule", map[string]any{
		"MonitoringScheduleName": "my-schedule",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["MonitoringScheduleArn"], "my-schedule")
}

func TestHandler_DescribeMonitoringSchedule(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateMonitoringSchedule", map[string]any{"MonitoringScheduleName": "sched-1"})

	rec := doSageMakerRequest(t, h, "DescribeMonitoringSchedule", map[string]any{"MonitoringScheduleName": "sched-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "sched-1", resp["MonitoringScheduleName"])
	assert.Equal(t, "Scheduled", resp["MonitoringScheduleStatus"])
}

func TestHandler_StopMonitoringSchedule(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateMonitoringSchedule", map[string]any{"MonitoringScheduleName": "sched-stop"})
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

	doSageMakerRequest(t, h, "CreateMonitoringSchedule", map[string]any{"MonitoringScheduleName": "sched-start"})
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

	doSageMakerRequest(t, h, "CreateMonitoringSchedule", map[string]any{"MonitoringScheduleName": "sched-del"})
	rec := doSageMakerRequest(t, h, "DeleteMonitoringSchedule", map[string]any{"MonitoringScheduleName": "sched-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeMonitoringSchedule", map[string]any{"MonitoringScheduleName": "sched-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListMonitoringSchedules(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateMonitoringSchedule", map[string]any{"MonitoringScheduleName": "sched-a"})
	doSageMakerRequest(t, h, "CreateMonitoringSchedule", map[string]any{"MonitoringScheduleName": "sched-b"})

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

func TestHandler_CreateWorkteam(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateWorkteam", map[string]any{
		"WorkteamName": "my-team",
		"Description":  "Test team",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["WorkteamArn"], "my-team")
}

func TestHandler_DescribeWorkteam(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateWorkteam", map[string]any{"WorkteamName": "team-1", "Description": "desc"})

	rec := doSageMakerRequest(t, h, "DescribeWorkteam", map[string]any{"WorkteamName": "team-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	wt := resp["Workteam"].(map[string]any)
	assert.Equal(t, "team-1", wt["WorkteamName"])
}

func TestHandler_DeleteWorkteam(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateWorkteam", map[string]any{"WorkteamName": "team-del"})
	rec := doSageMakerRequest(t, h, "DeleteWorkteam", map[string]any{"WorkteamName": "team-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeWorkteam", map[string]any{"WorkteamName": "team-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListWorkteams(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateWorkteam", map[string]any{"WorkteamName": "team-a"})
	doSageMakerRequest(t, h, "CreateWorkteam", map[string]any{"WorkteamName": "team-b"})

	rec := doSageMakerRequest(t, h, "ListWorkteams", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items := resp["Workteams"].([]any)
	assert.Len(t, items, 2)
}
