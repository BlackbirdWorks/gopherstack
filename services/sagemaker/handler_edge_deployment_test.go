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

	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

// ---------------------------------------------------------------------------
// EdgeDeploymentPlan tests
// ---------------------------------------------------------------------------

func TestHandler_EdgeDeploymentPlanLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateDeviceFleet", map[string]any{
		"DeviceFleetName": "plan-fleet",
		"OutputConfig":    map[string]any{"S3OutputLocation": "s3://my-bucket/output"},
	})

	rec := doSageMakerRequest(t, h, "CreateEdgeDeploymentPlan", map[string]any{
		"EdgeDeploymentPlanName": "my-plan",
		"DeviceFleetName":        "plan-fleet",
		"ModelConfigs": []any{
			map[string]any{"ModelHandle": "handle-1", "EdgePackagingJobName": "job-1"},
		},
		"Stages": []any{
			map[string]any{
				"StageName": "stage-1",
				"DeviceSelectionConfig": map[string]any{
					"DeviceSubsetType": "PERCENTAGE",
					"Percentage":       100,
				},
				"DeploymentConfig": map[string]any{"FailureHandlingPolicy": "ROLLBACK_ON_FAILURE"},
			},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	assert.Contains(t, createResp["EdgeDeploymentPlanArn"], "my-plan")

	// Describe
	rec = doSageMakerRequest(t, h, "DescribeEdgeDeploymentPlan", map[string]any{
		"EdgeDeploymentPlanName": "my-plan",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "my-plan", descResp["EdgeDeploymentPlanName"])
	assert.Equal(t, "plan-fleet", descResp["DeviceFleetName"])

	modelConfigs, ok := descResp["ModelConfigs"].([]any)
	require.True(t, ok)
	require.Len(t, modelConfigs, 1)
	mc, ok := modelConfigs[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "handle-1", mc["ModelHandle"])
	assert.Equal(t, "job-1", mc["EdgePackagingJobName"])

	stages, ok := descResp["Stages"].([]any)
	require.True(t, ok)
	require.Len(t, stages, 1)
	stage, ok := stages[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "stage-1", stage["StageName"])

	deploymentStatus, ok := stage["DeploymentStatus"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "READYTODEPLOY", deploymentStatus["StageStatus"])

	deviceSelectionConfig, ok := stage["DeviceSelectionConfig"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "PERCENTAGE", deviceSelectionConfig["DeviceSubsetType"])

	// List
	rec = doSageMakerRequest(t, h, "ListEdgeDeploymentPlans", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	summaries, ok := listResp["EdgeDeploymentPlanSummaries"].([]any)
	require.True(t, ok)
	assert.Len(t, summaries, 1)

	// Delete
	rec = doSageMakerRequest(t, h, "DeleteEdgeDeploymentPlan", map[string]any{
		"EdgeDeploymentPlanName": "my-plan",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify deleted
	rec = doSageMakerRequest(t, h, "DescribeEdgeDeploymentPlan", map[string]any{
		"EdgeDeploymentPlanName": "my-plan",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_EdgeDeploymentPlan_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "DescribeEdgeDeploymentPlan", map[string]any{
		"EdgeDeploymentPlanName": "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_EdgeDeploymentPlan_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateDeviceFleet", map[string]any{
		"DeviceFleetName": "dup-fleet",
		"OutputConfig":    map[string]any{"S3OutputLocation": "s3://my-bucket/output"},
	})

	body := map[string]any{"EdgeDeploymentPlanName": "dup-plan", "DeviceFleetName": "dup-fleet"}
	doSageMakerRequest(t, h, "CreateEdgeDeploymentPlan", body)

	rec := doSageMakerRequest(t, h, "CreateEdgeDeploymentPlan", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_EdgeDeploymentPlan_UnknownDeviceFleet(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateEdgeDeploymentPlan", map[string]any{
		"EdgeDeploymentPlanName": "orphan-plan",
		"DeviceFleetName":        "nonexistent-fleet",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// EdgeDeploymentStage tests
// ---------------------------------------------------------------------------

func TestHandler_EdgeDeploymentStageLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateDeviceFleet", map[string]any{
		"DeviceFleetName": "stage-fleet",
		"OutputConfig":    map[string]any{"S3OutputLocation": "s3://my-bucket/output"},
	})
	doSageMakerRequest(t, h, "CreateEdgeDeploymentPlan", map[string]any{
		"EdgeDeploymentPlanName": "stage-plan",
		"DeviceFleetName":        "stage-fleet",
	})

	// Create a stage
	rec := doSageMakerRequest(t, h, "CreateEdgeDeploymentStage", map[string]any{
		"EdgeDeploymentPlanName": "stage-plan",
		"Stages": []any{
			map[string]any{"StageName": "canary"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	descRec := func() map[string]any {
		descResp := doSageMakerRequest(t, h, "DescribeEdgeDeploymentPlan", map[string]any{
			"EdgeDeploymentPlanName": "stage-plan",
		})
		var resp map[string]any
		require.NoError(t, json.Unmarshal(descResp.Body.Bytes(), &resp))

		return resp
	}

	stageStatus := func(resp map[string]any) string {
		stages, ok := resp["Stages"].([]any)
		require.True(t, ok)
		require.Len(t, stages, 1)
		stage, ok := stages[0].(map[string]any)
		require.True(t, ok)
		status, ok := stage["DeploymentStatus"].(map[string]any)
		require.True(t, ok)
		s, _ := status["StageStatus"].(string)

		return s
	}

	assert.Equal(t, "READYTODEPLOY", stageStatus(descRec()))

	// Start
	rec = doSageMakerRequest(t, h, "StartEdgeDeploymentStage", map[string]any{
		"EdgeDeploymentPlanName": "stage-plan",
		"StageName":              "canary",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "INPROGRESS", stageStatus(descRec()))

	// Stop
	rec = doSageMakerRequest(t, h, "StopEdgeDeploymentStage", map[string]any{
		"EdgeDeploymentPlanName": "stage-plan",
		"StageName":              "canary",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "STOPPED", stageStatus(descRec()))

	// Delete the stage
	rec = doSageMakerRequest(t, h, "DeleteEdgeDeploymentStage", map[string]any{
		"EdgeDeploymentPlanName": "stage-plan",
		"StageName":              "canary",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	resp := descRec()
	stages, ok := resp["Stages"].([]any)
	require.True(t, ok)
	assert.Empty(t, stages)
}

func TestHandler_EdgeDeploymentStage_UnknownStage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateDeviceFleet", map[string]any{
		"DeviceFleetName": "fleet-x",
		"OutputConfig":    map[string]any{"S3OutputLocation": "s3://my-bucket/output"},
	})
	doSageMakerRequest(t, h, "CreateEdgeDeploymentPlan", map[string]any{
		"EdgeDeploymentPlanName": "plan-x",
		"DeviceFleetName":        "fleet-x",
	})

	rec := doSageMakerRequest(t, h, "StartEdgeDeploymentStage", map[string]any{
		"EdgeDeploymentPlanName": "plan-x",
		"StageName":              "does-not-exist",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_EdgeDeploymentStage_UnknownPlan(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateEdgeDeploymentStage", map[string]any{
		"EdgeDeploymentPlanName": "does-not-exist",
		"Stages":                 []any{map[string]any{"StageName": "s1"}},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// GetDeviceFleetReport tests
// ---------------------------------------------------------------------------

func TestHandler_GetDeviceFleetReport(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateDeviceFleet", map[string]any{
		"DeviceFleetName": "report-fleet",
		"Description":     "fleet for reports",
		"OutputConfig": map[string]any{
			"S3OutputLocation": "s3://bucket/prefix",
		},
	})
	doSageMakerRequest(t, h, "RegisterDevices", map[string]any{
		"DeviceFleetName": "report-fleet",
		"Devices": []any{
			map[string]any{"DeviceName": "d1"},
			map[string]any{"DeviceName": "d2"},
		},
	})

	rec := doSageMakerRequest(t, h, "GetDeviceFleetReport", map[string]any{
		"DeviceFleetName": "report-fleet",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "report-fleet", resp["DeviceFleetName"])
	assert.Contains(t, resp["DeviceFleetArn"], "report-fleet")
	assert.Equal(t, "fleet for reports", resp["Description"])

	outputConfig, ok := resp["OutputConfig"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "s3://bucket/prefix", outputConfig["S3OutputLocation"])

	deviceStats, ok := resp["DeviceStats"].(map[string]any)
	require.True(t, ok)
	assert.InDelta(t, float64(2), deviceStats["RegisteredDeviceCount"], 0)
}

func TestHandler_GetDeviceFleetReport_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "GetDeviceFleetReport", map[string]any{
		"DeviceFleetName": "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// ListStageDevices tests
// ---------------------------------------------------------------------------

func TestHandler_ListStageDevices(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateDeviceFleet", map[string]any{
		"DeviceFleetName": "lsd-fleet",
		"OutputConfig":    map[string]any{"S3OutputLocation": "s3://my-bucket/output"},
	})
	doSageMakerRequest(t, h, "RegisterDevices", map[string]any{
		"DeviceFleetName": "lsd-fleet",
		"Devices": []any{
			map[string]any{"DeviceName": "dev-a"},
			map[string]any{"DeviceName": "dev-b"},
		},
	})
	doSageMakerRequest(t, h, "CreateEdgeDeploymentPlan", map[string]any{
		"EdgeDeploymentPlanName": "lsd-plan",
		"DeviceFleetName":        "lsd-fleet",
		"Stages": []any{
			map[string]any{"StageName": "stage-1"},
		},
	})

	rec := doSageMakerRequest(t, h, "ListStageDevices", map[string]any{
		"EdgeDeploymentPlanName": "lsd-plan",
		"StageName":              "stage-1",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	summaries, ok := resp["DeviceDeploymentSummaries"].([]any)
	require.True(t, ok)
	require.Len(t, summaries, 2)

	first, ok := summaries[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "lsd-plan", first["EdgeDeploymentPlanName"])
	assert.Equal(t, "stage-1", first["StageName"])
	assert.Equal(t, "READYTODEPLOY", first["DeviceDeploymentStatus"])
	assert.Equal(t, "lsd-fleet", first["DeviceFleetName"])
}

func TestHandler_ListStageDevices_UnknownStage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateDeviceFleet", map[string]any{
		"DeviceFleetName": "lsd-fleet-2",
		"OutputConfig":    map[string]any{"S3OutputLocation": "s3://my-bucket/output"},
	})
	doSageMakerRequest(t, h, "CreateEdgeDeploymentPlan", map[string]any{
		"EdgeDeploymentPlanName": "lsd-plan-2",
		"DeviceFleetName":        "lsd-fleet-2",
	})

	rec := doSageMakerRequest(t, h, "ListStageDevices", map[string]any{
		"EdgeDeploymentPlanName": "lsd-plan-2",
		"StageName":              "missing-stage",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// UpdateDevices tests
// ---------------------------------------------------------------------------

func TestHandler_UpdateDevices(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateDeviceFleet", map[string]any{
		"DeviceFleetName": "upd-fleet",
		"OutputConfig":    map[string]any{"S3OutputLocation": "s3://my-bucket/output"},
	})
	doSageMakerRequest(t, h, "RegisterDevices", map[string]any{
		"DeviceFleetName": "upd-fleet",
		"Devices": []any{
			map[string]any{"DeviceName": "dev-1", "Description": "original"},
		},
	})

	rec := doSageMakerRequest(t, h, "UpdateDevices", map[string]any{
		"DeviceFleetName": "upd-fleet",
		"Devices": []any{
			map[string]any{"DeviceName": "dev-1", "Description": "updated", "IotThingName": "thing-1"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeDevice", map[string]any{
		"DeviceFleetName": "upd-fleet",
		"DeviceName":      "dev-1",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "updated", resp["Description"])
	assert.Equal(t, "thing-1", resp["IotThingName"])
}

// ---------------------------------------------------------------------------
// Persistence tests
// ---------------------------------------------------------------------------

func TestHandler_EdgeDeploymentPlan_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		makePlan  bool
		wantCount int
	}{
		{name: "snapshot and restore preserves an edge deployment plan", makePlan: true, wantCount: 1},
		{name: "empty backend snapshot and restore", makePlan: false, wantCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h1 := newTestHandler(t)

			if tt.makePlan {
				doSageMakerRequest(t, h1, "CreateDeviceFleet", map[string]any{
					"DeviceFleetName": "persist-fleet",
					"OutputConfig":    map[string]any{"S3OutputLocation": "s3://my-bucket/output"},
				})
				doSageMakerRequest(t, h1, "CreateEdgeDeploymentPlan", map[string]any{
					"EdgeDeploymentPlanName": "persist-plan",
					"DeviceFleetName":        "persist-fleet",
					"Stages": []any{
						map[string]any{"StageName": "stage-1"},
					},
				})
				doSageMakerRequest(t, h1, "StartEdgeDeploymentStage", map[string]any{
					"EdgeDeploymentPlanName": "persist-plan",
					"StageName":              "stage-1",
				})
			}

			snap := h1.Backend.Snapshot(t.Context())
			require.NotNil(t, snap)

			b2 := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, b2.Restore(t.Context(), snap))
			h2 := sagemaker.NewHandler(b2)

			rec := doSageMakerRequest(t, h2, "ListEdgeDeploymentPlans", map[string]any{})
			assert.Equal(t, http.StatusOK, rec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
			summaries, ok := listResp["EdgeDeploymentPlanSummaries"].([]any)
			require.True(t, ok)
			assert.Len(t, summaries, tt.wantCount)

			if !tt.makePlan {
				return
			}

			rec = doSageMakerRequest(t, h2, "DescribeEdgeDeploymentPlan", map[string]any{
				"EdgeDeploymentPlanName": "persist-plan",
			})
			assert.Equal(t, http.StatusOK, rec.Code)

			var descResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
			assert.Equal(t, "persist-fleet", descResp["DeviceFleetName"])

			stages, ok := descResp["Stages"].([]any)
			require.True(t, ok)
			require.Len(t, stages, 1)
			stage, ok := stages[0].(map[string]any)
			require.True(t, ok)
			status, ok := stage["DeploymentStatus"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "INPROGRESS", status["StageStatus"])
		})
	}
}

func TestHandler_UpdateDevices_UnknownFleet(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "UpdateDevices", map[string]any{
		"DeviceFleetName": "nonexistent",
		"Devices":         []any{map[string]any{"DeviceName": "dev-1"}},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandler_DescribeEdgeDeploymentPlan_StagesPaginated_RealClient asserts
// DescribeEdgeDeploymentPlanInput's MaxResults/NextToken -- absent before
// this pass -- actually paginate the Stages list rather than always
// returning every stage in one response, matching
// api_op_DescribeEdgeDeploymentPlan.go:39-41's "enough stages to require
// tokening" contract.
func TestHandler_DescribeEdgeDeploymentPlan_StagesPaginated_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateDeviceFleet(t.Context(), &sagemakersdk.CreateDeviceFleetInput{
		DeviceFleetName: aws.String("paginated-fleet"),
		OutputConfig:    &smtypes.EdgeOutputConfig{S3OutputLocation: aws.String("s3://bucket/out")},
	})
	require.NoError(t, err)

	stages := make([]smtypes.DeploymentStage, 0, 3)
	for i := range 3 {
		stages = append(stages, smtypes.DeploymentStage{
			StageName:             aws.String(stageName(i)),
			DeviceSelectionConfig: &smtypes.DeviceSelectionConfig{DeviceSubsetType: smtypes.DeviceSubsetTypePercentage},
		})
	}

	_, err = client.CreateEdgeDeploymentPlan(t.Context(), &sagemakersdk.CreateEdgeDeploymentPlanInput{
		EdgeDeploymentPlanName: aws.String("paginated-plan"),
		DeviceFleetName:        aws.String("paginated-fleet"),
		ModelConfigs:           testEdgeModelConfigs(),
		Stages:                 stages,
	})
	require.NoError(t, err)

	out, err := client.DescribeEdgeDeploymentPlan(t.Context(), &sagemakersdk.DescribeEdgeDeploymentPlanInput{
		EdgeDeploymentPlanName: aws.String("paginated-plan"),
		MaxResults:             aws.Int32(2),
	})
	require.NoError(t, err)
	require.Len(t, out.Stages, 2)
	assert.NotEmpty(t, aws.ToString(out.NextToken))

	out2, err := client.DescribeEdgeDeploymentPlan(t.Context(), &sagemakersdk.DescribeEdgeDeploymentPlanInput{
		EdgeDeploymentPlanName: aws.String("paginated-plan"),
		MaxResults:             aws.Int32(2),
		NextToken:              out.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, out2.Stages, 1)
	assert.Empty(t, aws.ToString(out2.NextToken))
}

func stageName(i int) string {
	return "stage-" + string(rune('a'+i))
}

// testEdgeModelConfigs returns a minimal ModelConfigs value satisfying
// CreateEdgeDeploymentPlanInput's required field -- the real SDK client
// validates this locally before ever reaching the server.
func testEdgeModelConfigs() []smtypes.EdgeDeploymentModelConfig {
	return []smtypes.EdgeDeploymentModelConfig{
		{ModelHandle: aws.String("handle-1"), EdgePackagingJobName: aws.String("job-1")},
	}
}

// TestHandler_ListEdgeDeploymentPlans_FilterSortPage_RealClient asserts
// ListEdgeDeploymentPlansInput's filter/sort/pagination fields -- absent
// before this pass except NextToken -- narrow, reorder, and paginate the
// result set.
func TestHandler_ListEdgeDeploymentPlans_FilterSortPage_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateDeviceFleet(t.Context(), &sagemakersdk.CreateDeviceFleetInput{
		DeviceFleetName: aws.String("lep-fleet"),
		OutputConfig:    &smtypes.EdgeOutputConfig{S3OutputLocation: aws.String("s3://bucket/out")},
	})
	require.NoError(t, err)

	names := []string{"alpha-plan", "beta-plan", "gamma-widget"}
	for _, n := range names {
		_, planErr := client.CreateEdgeDeploymentPlan(t.Context(), &sagemakersdk.CreateEdgeDeploymentPlanInput{
			EdgeDeploymentPlanName: aws.String(n),
			DeviceFleetName:        aws.String("lep-fleet"),
			ModelConfigs:           testEdgeModelConfigs(),
		})
		require.NoError(t, planErr)
	}

	t.Run("name contains filters", func(t *testing.T) {
		t.Parallel()

		out, listErr := client.ListEdgeDeploymentPlans(t.Context(), &sagemakersdk.ListEdgeDeploymentPlansInput{
			NameContains: aws.String("plan"),
		})
		require.NoError(t, listErr)
		assert.Len(t, out.EdgeDeploymentPlanSummaries, 2)
	})

	t.Run("sort by name ascending", func(t *testing.T) {
		t.Parallel()

		out, listErr := client.ListEdgeDeploymentPlans(t.Context(), &sagemakersdk.ListEdgeDeploymentPlansInput{
			SortBy:    smtypes.ListEdgeDeploymentPlansSortByName,
			SortOrder: smtypes.SortOrderAscending,
		})
		require.NoError(t, listErr)
		require.Len(t, out.EdgeDeploymentPlanSummaries, 3)
		assert.Equal(t, "alpha-plan", aws.ToString(out.EdgeDeploymentPlanSummaries[0].EdgeDeploymentPlanName))
		assert.Equal(t, "gamma-widget", aws.ToString(out.EdgeDeploymentPlanSummaries[2].EdgeDeploymentPlanName))
	})

	t.Run("max results caps the page and returns a token", func(t *testing.T) {
		t.Parallel()

		out, listErr := client.ListEdgeDeploymentPlans(t.Context(), &sagemakersdk.ListEdgeDeploymentPlansInput{
			MaxResults: aws.Int32(1),
			SortBy:     smtypes.ListEdgeDeploymentPlansSortByName,
			SortOrder:  smtypes.SortOrderAscending,
		})
		require.NoError(t, listErr)
		require.Len(t, out.EdgeDeploymentPlanSummaries, 1)
		assert.Equal(t, "alpha-plan", aws.ToString(out.EdgeDeploymentPlanSummaries[0].EdgeDeploymentPlanName))
		assert.NotEmpty(t, aws.ToString(out.NextToken))
	})
}

// TestHandler_ListStageDevices_MaxResults_RealClient asserts
// ListStageDevicesInput's MaxResults -- absent before this pass -- caps the
// page of devices returned.
func TestHandler_ListStageDevices_MaxResults_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateDeviceFleet(t.Context(), &sagemakersdk.CreateDeviceFleetInput{
		DeviceFleetName: aws.String("lsd-rc-fleet"),
		OutputConfig:    &smtypes.EdgeOutputConfig{S3OutputLocation: aws.String("s3://bucket/out")},
	})
	require.NoError(t, err)

	_, err = client.RegisterDevices(t.Context(), &sagemakersdk.RegisterDevicesInput{
		DeviceFleetName: aws.String("lsd-rc-fleet"),
		Devices: []smtypes.Device{
			{DeviceName: aws.String("dev-a")},
			{DeviceName: aws.String("dev-b")},
		},
	})
	require.NoError(t, err)

	_, err = client.CreateEdgeDeploymentPlan(t.Context(), &sagemakersdk.CreateEdgeDeploymentPlanInput{
		EdgeDeploymentPlanName: aws.String("lsd-rc-plan"),
		DeviceFleetName:        aws.String("lsd-rc-fleet"),
		ModelConfigs:           testEdgeModelConfigs(),
		Stages: []smtypes.DeploymentStage{{
			StageName:             aws.String("stage-1"),
			DeviceSelectionConfig: &smtypes.DeviceSelectionConfig{DeviceSubsetType: smtypes.DeviceSubsetTypePercentage},
		}},
	})
	require.NoError(t, err)

	out, err := client.ListStageDevices(t.Context(), &sagemakersdk.ListStageDevicesInput{
		EdgeDeploymentPlanName: aws.String("lsd-rc-plan"),
		StageName:              aws.String("stage-1"),
		MaxResults:             aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, out.DeviceDeploymentSummaries, 1)
	assert.NotEmpty(t, aws.ToString(out.NextToken))
}
