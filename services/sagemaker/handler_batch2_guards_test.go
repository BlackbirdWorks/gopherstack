package sagemaker_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

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

// TestCompilationJob_InputOutputConfigRoundtrip verifies that InputConfig, OutputConfig,
// and StoppingCondition provided at CreateCompilationJob are persisted and returned by
// DescribeCompilationJob. Real AWS stores and returns these fields.
func TestCompilationJob_InputOutputConfigRoundtrip(t *testing.T) {
	t.Parallel()

	b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")
	ctx := context.Background()

	_, err := b.CreateCompilationJob(ctx, "roundtrip-job", "arn:aws:iam::123456789012:role/Neo", nil)
	require.NoError(t, err)

	inputCfg := &sagemaker.CompilationInputConfig{
		S3Uri:     "s3://my-bucket/model.tar.gz",
		Framework: "TENSORFLOW",
	}
	outputCfg := &sagemaker.CompilationOutputConfig{
		S3OutputLocation: "s3://my-bucket/output/",
		TargetDevice:     "ml_c5",
	}
	sc := &sagemaker.StoppingCondition{MaxRuntimeInSeconds: 300}

	err = b.SetCompilationJobExtras(ctx, "roundtrip-job", inputCfg, outputCfg, sc)
	require.NoError(t, err)

	got, err := b.DescribeCompilationJob(ctx, "roundtrip-job")
	require.NoError(t, err)

	require.NotNil(t, got.InputConfig, "InputConfig must be persisted")
	assert.Equal(t, "s3://my-bucket/model.tar.gz", got.InputConfig.S3Uri)
	assert.Equal(t, "TENSORFLOW", got.InputConfig.Framework)

	require.NotNil(t, got.OutputConfig, "OutputConfig must be persisted")
	assert.Equal(t, "s3://my-bucket/output/", got.OutputConfig.S3OutputLocation)
	assert.Equal(t, "ml_c5", got.OutputConfig.TargetDevice)

	require.NotNil(t, got.StoppingCondition, "StoppingCondition must be persisted")
	assert.Equal(t, int32(300), got.StoppingCondition.MaxRuntimeInSeconds)
}

// TestCompilationJob_HandlerCapturesInputOutputConfig verifies that the HTTP handler
// passes InputConfig and OutputConfig through to the backend on creation.
func TestCompilationJob_HandlerCapturesInputOutputConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doSageMakerRequest(t, h, "CreateCompilationJob", map[string]any{
		"CompilationJobName": "handler-roundtrip-job",
		"RoleArn":            "arn:aws:iam::123456789012:role/Neo",
		"InputConfig": map[string]any{
			"S3Uri":     "s3://bucket/model.tar.gz",
			"Framework": "PYTORCH",
		},
		"OutputConfig": map[string]any{
			"S3OutputLocation": "s3://bucket/out/",
			"TargetDevice":     "jetson_nano",
		},
		"StoppingCondition": map[string]any{
			"MaxRuntimeInSeconds": 600,
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code,
		"CreateCompilationJob failed: %s", createRec.Body.String())

	descRec := doSageMakerRequest(t, h, "DescribeCompilationJob", map[string]any{
		"CompilationJobName": "handler-roundtrip-job",
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var out struct {
		InputConfig *struct {
			S3Uri     string `json:"S3Uri"`
			Framework string `json:"Framework"`
		} `json:"InputConfig"`
		OutputConfig *struct {
			S3OutputLocation string `json:"S3OutputLocation"`
			TargetDevice     string `json:"TargetDevice"`
		} `json:"OutputConfig"`
		StoppingCondition *struct {
			MaxRuntimeInSeconds int32 `json:"MaxRuntimeInSeconds"`
		} `json:"StoppingCondition"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &out))

	require.NotNil(t, out.InputConfig, "InputConfig must be returned by DescribeCompilationJob")
	assert.Equal(t, "s3://bucket/model.tar.gz", out.InputConfig.S3Uri)
	assert.Equal(t, "PYTORCH", out.InputConfig.Framework)

	require.NotNil(t, out.OutputConfig, "OutputConfig must be returned by DescribeCompilationJob")
	assert.Equal(t, "s3://bucket/out/", out.OutputConfig.S3OutputLocation)
	assert.Equal(t, "jetson_nano", out.OutputConfig.TargetDevice)

	require.NotNil(t, out.StoppingCondition, "StoppingCondition must be returned by DescribeCompilationJob")
	assert.Equal(t, int32(600), out.StoppingCondition.MaxRuntimeInSeconds)
}

// TestAutoMLJob_OutputDataConfigRoundtrip verifies that OutputDataConfig and
// AutoMLJobObjective provided at CreateAutoMLJob are persisted and returned by
// DescribeAutoMLJob. Real AWS stores and returns these fields.
func TestAutoMLJob_OutputDataConfigRoundtrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doSageMakerRequest(t, h, "CreateAutoMLJob", map[string]any{
		"AutoMLJobName": "automl-output-roundtrip",
		"RoleArn":       "arn:aws:iam::123456789012:role/AutoML",
		"OutputDataConfig": map[string]any{
			"S3OutputPath": "s3://my-bucket/automl-output/",
		},
		"AutoMLJobObjective": map[string]any{
			"MetricName": "Accuracy",
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code,
		"CreateAutoMLJob failed: %s", createRec.Body.String())

	descRec := doSageMakerRequest(t, h, "DescribeAutoMLJob", map[string]any{
		"AutoMLJobName": "automl-output-roundtrip",
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var out struct {
		OutputDataConfig *struct {
			S3OutputPath string `json:"S3OutputPath"`
		} `json:"OutputDataConfig"`
		AutoMLJobObjective *struct {
			MetricName string `json:"MetricName"`
		} `json:"AutoMLJobObjective"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &out))

	require.NotNil(t, out.OutputDataConfig, "OutputDataConfig must be returned by DescribeAutoMLJob")
	assert.Equal(t, "s3://my-bucket/automl-output/", out.OutputDataConfig.S3OutputPath)

	require.NotNil(t, out.AutoMLJobObjective, "AutoMLJobObjective must be returned by DescribeAutoMLJob")
	assert.Equal(t, "Accuracy", out.AutoMLJobObjective.MetricName)
}

// ---------------------------------------------------------------------------
// ModelPackageGroup: dependency guard
// ---------------------------------------------------------------------------

func TestDeleteModelPackageGroup_WithPackages_Conflict(t *testing.T) {
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

func TestDeleteModelPackageGroup_AfterPackagesRemoved_OK(t *testing.T) {
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

func TestAutoMLJob_InitialStatus_InProgress(t *testing.T) {
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

func TestStopAutoMLJob_Terminal_Rejected(t *testing.T) {
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

func TestCompilationJob_InitialStatus_InProgress(t *testing.T) {
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

func TestStopCompilationJob_Terminal_Rejected(t *testing.T) {
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

func TestDeleteImage_WithVersions_Conflict(t *testing.T) {
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

func TestDeleteImage_AfterVersionsRemoved_OK(t *testing.T) {
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

func TestStopMonitoringSchedule_AlreadyStopped_Rejected(t *testing.T) {
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

func TestStartMonitoringSchedule_AlreadyScheduled_Rejected(t *testing.T) {
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

func TestAddModelPackageInternal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		pkgName   string
		wantCount int
	}{
		{
			name:      "creates model package",
			pkgName:   "my-pkg",
			wantCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")
			arnStr := "arn:aws:sagemaker:us-east-1:000000000000:model-package/" + tt.pkgName
			b.AddModelPackageInternal(context.Background(), &sagemaker.ModelPackage{
				ModelPackageName:   tt.pkgName,
				ModelPackageArn:    arnStr,
				ModelPackageStatus: "Approved",
			})

			assert.Equal(t, tt.wantCount, sagemaker.ModelPackageCount(b))
		})
	}
}

func TestAddTags_ModelPackage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantTags map[string]string
		name     string
		wantCode int
	}{
		{
			name:     "tags on model package via ARN",
			wantCode: http.StatusOK,
			wantTags: map[string]string{"env": "prod"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			arnStr := "arn:aws:sagemaker:us-east-1:000000000000:model-package/my-pkg"
			h.Backend.AddModelPackageInternal(context.Background(), &sagemaker.ModelPackage{
				ModelPackageName:   "my-pkg",
				ModelPackageArn:    arnStr,
				ModelPackageStatus: "Approved",
				Tags:               make(map[string]string),
			})

			rec := doSageMakerRequest(t, h, "AddTags", map[string]any{
				"ResourceArn": arnStr,
				"Tags":        []map[string]string{{"Key": "env", "Value": "prod"}},
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			rec2 := doSageMakerRequest(t, h, "ListTags", map[string]any{
				"ResourceArn": arnStr,
			})
			require.Equal(t, http.StatusOK, rec2.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))

			tags := resp["Tags"].([]any)
			require.Len(t, tags, 1)

			tag := tags[0].(map[string]any)
			assert.Equal(t, "env", tag["Key"])
			assert.Equal(t, "prod", tag["Value"])
		})
	}
}

func TestBatchDescribeModelPackage_MixedResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		seedArns  []string
		queryArns []string
		wantFound int
		wantErr   int
	}{
		{
			name:     "some found some not",
			seedArns: []string{"arn:aws:sagemaker:us-east-1:000000000000:model-package/pkg-1"},
			queryArns: []string{
				"arn:aws:sagemaker:us-east-1:000000000000:model-package/pkg-1",
				"arn:aws:sagemaker:us-east-1:000000000000:model-package/pkg-missing",
			},
			wantFound: 1,
			wantErr:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")

			for _, arnStr := range tt.seedArns {
				b.AddModelPackageInternal(context.Background(), &sagemaker.ModelPackage{
					ModelPackageName:   "pkg-1",
					ModelPackageArn:    arnStr,
					ModelPackageStatus: "Approved",
				})
			}

			results := b.BatchDescribeModelPackage(context.Background(), tt.queryArns)
			found, errs := 0, 0

			for _, r := range results {
				if r.ModelPackage != nil {
					found++
				} else {
					errs++
				}
			}

			assert.Equal(t, tt.wantFound, found)
			assert.Equal(t, tt.wantErr, errs)
		})
	}
}

func TestHandler_ModelPackageGroupPolicy_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelPackageGroup", map[string]any{
		"ModelPackageGroupName": "policy-group",
	})

	// No policy attached yet.
	recGet := doSageMakerRequest(t, h, "GetModelPackageGroupPolicy", map[string]any{
		"ModelPackageGroupName": "policy-group",
	})
	assert.Equal(t, http.StatusBadRequest, recGet.Code)

	policy := `{"Version":"2012-10-17","Statement":[]}`
	recPut := doSageMakerRequest(t, h, "PutModelPackageGroupPolicy", map[string]any{
		"ModelPackageGroupName": "policy-group",
		"ResourcePolicy":        policy,
	})
	require.Equal(t, http.StatusOK, recPut.Code)

	var putOut map[string]any
	require.NoError(t, json.Unmarshal(recPut.Body.Bytes(), &putOut))
	assert.NotEmpty(t, putOut["ModelPackageGroupArn"])

	recGet2 := doSageMakerRequest(t, h, "GetModelPackageGroupPolicy", map[string]any{
		"ModelPackageGroupName": "policy-group",
	})
	require.Equal(t, http.StatusOK, recGet2.Code)

	var getOut map[string]any
	require.NoError(t, json.Unmarshal(recGet2.Body.Bytes(), &getOut))
	assert.Equal(t, policy, getOut["ResourcePolicy"])

	recDelete := doSageMakerRequest(t, h, "DeleteModelPackageGroupPolicy", map[string]any{
		"ModelPackageGroupName": "policy-group",
	})
	require.Equal(t, http.StatusOK, recDelete.Code)

	recGet3 := doSageMakerRequest(t, h, "GetModelPackageGroupPolicy", map[string]any{
		"ModelPackageGroupName": "policy-group",
	})
	assert.Equal(t, http.StatusBadRequest, recGet3.Code)
}

func TestHandler_ModelPackageGroupPolicy_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		body map[string]any
		op   string
	}{
		{op: "GetModelPackageGroupPolicy", body: map[string]any{"ModelPackageGroupName": "no-such-group"}},
		{
			op: "PutModelPackageGroupPolicy",
			body: map[string]any{
				"ModelPackageGroupName": "no-such-group", "ResourcePolicy": "{}",
			},
		},
		{op: "DeleteModelPackageGroupPolicy", body: map[string]any{"ModelPackageGroupName": "no-such-group"}},
	}

	for _, tc := range tests {
		t.Run(tc.op, func(t *testing.T) {
			t.Parallel()

			rec := doSageMakerRequest(t, h, tc.op, tc.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// ---------------------------------------------------------------------------
// Service Catalog portfolio toggle
// ---------------------------------------------------------------------------

func TestHandler_ListAliases(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateImage", map[string]any{"ImageName": "alias-img"})
	doSageMakerRequest(t, h, "CreateImageVersion", map[string]any{"ImageName": "alias-img"})
	doSageMakerRequest(t, h, "UpdateImageVersion", map[string]any{
		"ImageName": "alias-img", "AliasesToAdd": []string{"latest", "stable"},
	})

	rec := doSageMakerRequest(t, h, "ListAliases", map[string]any{"ImageName": "alias-img"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	aliases, _ := out["SageMakerImageVersionAliases"].([]any)
	assert.ElementsMatch(t, []any{"latest", "stable"}, aliases)
}

func TestHandler_ListAliases_ImageNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListAliases", map[string]any{"ImageName": "no-such-image"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// UpdateProject
// ---------------------------------------------------------------------------

func TestHandler_UpdateProject(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateProject", map[string]any{"ProjectName": "proj-a"})

	rec := doSageMakerRequest(t, h, "UpdateProject", map[string]any{
		"ProjectName": "proj-a", "ProjectDescription": "updated description",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	recDescribe := doSageMakerRequest(t, h, "DescribeProject", map[string]any{"ProjectName": "proj-a"})
	require.Equal(t, http.StatusOK, recDescribe.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(recDescribe.Body.Bytes(), &out))
	assert.Equal(t, "updated description", out["ProjectDescription"])
}

func TestHandler_UpdateProject_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "UpdateProject", map[string]any{"ProjectName": "no-such-project"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// Pipeline versions and execution-definition extras
// ---------------------------------------------------------------------------

func TestHandler_UpdateImage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateImage", map[string]any{
		"ImageName":   "my-image",
		"RoleArn":     "arn:test",
		"Description": "original",
	})

	rec := doSageMakerRequest(t, h, "UpdateImage", map[string]any{
		"ImageName":   "my-image",
		"DisplayName": "My Image",
		"Description": "updated",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	describeRec := doSageMakerRequest(t, h, "DescribeImage", map[string]any{
		"ImageName": "my-image",
	})

	var resp map[string]any
	require.NoError(t, json.Unmarshal(describeRec.Body.Bytes(), &resp))
	assert.Equal(t, "updated", resp["Description"])
	assert.Equal(t, "My Image", resp["DisplayName"])

	// DeleteProperties clears Description.
	doSageMakerRequest(t, h, "UpdateImage", map[string]any{
		"ImageName":        "my-image",
		"DeleteProperties": []string{"Description"},
	})

	describeRec2 := doSageMakerRequest(t, h, "DescribeImage", map[string]any{
		"ImageName": "my-image",
	})

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(describeRec2.Body.Bytes(), &resp2))
	assert.Empty(t, resp2["Description"])
}

func TestHandler_UpdateImage_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "UpdateImage", map[string]any{
		"ImageName": "does-not-exist",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UpdateImageVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateImage", map[string]any{
		"ImageName": "my-image",
		"RoleArn":   "arn:test",
	})
	doSageMakerRequest(t, h, "CreateImageVersion", map[string]any{
		"ImageName": "my-image",
	})

	rec := doSageMakerRequest(t, h, "UpdateImageVersion", map[string]any{
		"ImageName":    "my-image",
		"Version":      1,
		"MLFramework":  "PyTorch 2.0",
		"AliasesToAdd": []string{"latest", "stable"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	describeRec := doSageMakerRequest(t, h, "DescribeImageVersion", map[string]any{
		"ImageName": "my-image",
		"Version":   1,
	})

	var resp map[string]any
	require.NoError(t, json.Unmarshal(describeRec.Body.Bytes(), &resp))
	assert.Equal(t, "PyTorch 2.0", resp["MLFramework"])
	aliases, ok := resp["Aliases"].([]any)
	require.True(t, ok)
	assert.Len(t, aliases, 2)

	// Remove one alias.
	doSageMakerRequest(t, h, "UpdateImageVersion", map[string]any{
		"ImageName":       "my-image",
		"Version":         1,
		"AliasesToDelete": []string{"latest"},
	})

	describeRec2 := doSageMakerRequest(t, h, "DescribeImageVersion", map[string]any{
		"ImageName": "my-image",
		"Version":   1,
	})

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(describeRec2.Body.Bytes(), &resp2))
	aliases2, ok := resp2["Aliases"].([]any)
	require.True(t, ok)
	assert.Len(t, aliases2, 1)
	assert.Equal(t, "stable", aliases2[0])
}

func TestHandler_UpdateImageVersion_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "UpdateImageVersion", map[string]any{
		"ImageName": "does-not-exist",
		"Version":   1,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
