package emr_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/emr"
)

// keep context in scope for TestParity_ValidateReleaseLabel.
var _ = context.Background

// TestParity_CancelSteps_ReturnsPerStepInfo verifies CancelSteps returns per-step status.
func TestParity_CancelSteps_ReturnsPerStepInfo(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{
		"Name": "cancel-test",
		"Steps": []map[string]any{
			{
				"Name":            "step1",
				"ActionOnFailure": "CONTINUE",
				"HadoopJarStep":   map[string]any{"Jar": "command-runner.jar"},
			},
			{
				"Name":            "step2",
				"ActionOnFailure": "CONTINUE",
				"HadoopJarStep":   map[string]any{"Jar": "command-runner.jar"},
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))

	// List steps to get IDs
	listRec := doEMRRequest(t, h, "ListSteps", map[string]any{"ClusterId": create.JobFlowID})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut struct {
		Steps []struct {
			ID string `json:"Id"`
		} `json:"Steps"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	require.NotEmpty(t, listOut.Steps)

	stepID := listOut.Steps[0].ID
	rec := doEMRRequest(t, h, "CancelSteps", map[string]any{
		"ClusterId": create.JobFlowID,
		"StepIds":   []string{stepID},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		CancelStepsInfoList []struct {
			StepID string `json:"StepId"`
			Status string `json:"Status"`
		} `json:"CancelStepsInfoList"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Len(t, out.CancelStepsInfoList, 1)
	assert.Equal(t, stepID, out.CancelStepsInfoList[0].StepID)
	assert.NotEmpty(t, out.CancelStepsInfoList[0].Status)
}

// TestParity_ErrorMapping_NotFound_ClusterNotFoundException verifies ErrNotFound maps to ClusterNotFoundException.
func TestParity_ErrorMapping_NotFound_ClusterNotFoundException(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doEMRRequest(t, h, "DescribeCluster", map[string]any{"ClusterId": "j-NONEXISTENT"})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var errOut map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errOut))
	assert.Equal(t, "ClusterNotFoundException", errOut["__type"])
}

// TestParity_ValidateReleaseLabel verifies valid and invalid label formats.
func TestParity_ValidateReleaseLabel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		label  string
		wantOK bool
	}{
		{"known registry label", "emr-6.0.0", true},
		{"known registry label 7x", "emr-7.3.0", true},
		{"arbitrary valid label", "emr-5.20.1", true},
		{"arbitrary high version", "emr-8.0.0", true},
		{"arbitrary minor only", "emr-6.99", true},
		{"empty label defaults to latest", "", true},
		{"no prefix", "6.0.0", false},
		{"bad format", "emr-abc", false},
		{"trailing dot", "emr-6.", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := emr.NewInMemoryBackend(testAccountID, testRegion)
			_, err := b.RunJobFlow(context.Background(), emr.RunJobFlowParams{
				Name:         "label-test",
				ReleaseLabel: tt.label,
			})

			if tt.wantOK {
				assert.NoError(t, err, "label %q should be accepted", tt.label)
			} else {
				assert.Error(t, err, "label %q should be rejected", tt.label)
			}
		})
	}
}

// TestParity_BuildInstanceGroups_UniqueIDs verifies consecutive RunJobFlow calls produce unique instance group IDs.
func TestParity_BuildInstanceGroups_UniqueIDs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	groups := []map[string]any{
		{"InstanceRole": "MASTER", "InstanceType": "m5.xlarge", "InstanceCount": 1},
		{"InstanceRole": "CORE", "InstanceType": "m5.xlarge", "InstanceCount": 2},
	}

	rec1 := doEMRRequest(t, h, "RunJobFlow", map[string]any{
		"Name":      "c1",
		"Instances": map[string]any{"InstanceGroups": groups},
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := doEMRRequest(t, h, "RunJobFlow", map[string]any{
		"Name":      "c2",
		"Instances": map[string]any{"InstanceGroups": groups},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var c1, c2 struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &c1))
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &c2))

	// Gather instance group IDs via ListInstanceGroups for each cluster
	allIDs := make(map[string]bool)
	for _, clusterID := range []string{c1.JobFlowID, c2.JobFlowID} {
		igRec := doEMRRequest(t, h, "ListInstanceGroups", map[string]any{"ClusterId": clusterID})
		require.Equal(t, http.StatusOK, igRec.Code)

		var igOut struct {
			InstanceGroups []struct {
				ID string `json:"Id"`
			} `json:"InstanceGroups"`
		}
		require.NoError(t, json.Unmarshal(igRec.Body.Bytes(), &igOut))

		for _, ig := range igOut.InstanceGroups {
			assert.False(t, allIDs[ig.ID], "duplicate instance group ID %s", ig.ID)
			allIDs[ig.ID] = true
		}
	}
}

// TestParity_GetOnClusterPresignedURL_NonExistentCluster verifies cluster existence is checked.
func TestParity_GetOnClusterPresignedURL_NonExistentCluster(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doEMRRequest(t, h, "GetOnClusterAppUIPresignedURL", map[string]any{
		"ClusterId": "j-NOSUCHCLUSTER",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errOut map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errOut))
	assert.Equal(t, "ClusterNotFoundException", errOut["__type"])
}
