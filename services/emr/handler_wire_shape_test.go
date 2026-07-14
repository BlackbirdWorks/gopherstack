package emr_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestWireShape_ListClusters_IncludesTimeline verifies ListClusters' per-cluster
// Status.Timeline.CreationDateTime is present on the wire (it was previously
// dropped entirely by gatherClusterSummaries, which also broke the
// most-recently-created-first sort since it read the same, always-empty
// field) and is an epoch-seconds JSON number, matching the real EMR
// awsjson1.1 wire format.
func TestWireShape_ListClusters_IncludesTimeline(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "timeline-wire-cluster"})
	require.Equal(t, http.StatusOK, rec.Code)

	listRec := doEMRRequest(t, h, "ListClusters", map[string]any{})
	require.Equal(t, http.StatusOK, listRec.Code)

	var out struct {
		Clusters []struct {
			Status struct {
				Timeline struct {
					CreationDateTime float64 `json:"CreationDateTime"`
					ReadyDateTime    float64 `json:"ReadyDateTime"`
				} `json:"Timeline"`
			} `json:"Status"`
		} `json:"Clusters"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
	require.Len(t, out.Clusters, 1)
	assert.NotZero(t, out.Clusters[0].Status.Timeline.CreationDateTime)
	assert.NotZero(t, out.Clusters[0].Status.Timeline.ReadyDateTime)
}

// TestWireShape_ModifyInstanceFleet_AppliesCapacities verifies
// ModifyInstanceFleet actually mutates the fleet's target/provisioned
// capacities -- it previously looked up the fleet, matched its ID, and
// returned success without changing anything (a disguised no-op).
func TestWireShape_ModifyInstanceFleet_AppliesCapacities(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "modify-fleet-cluster"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))

	addRec := doEMRRequest(t, h, "AddInstanceFleet", map[string]any{
		"ClusterId": create.JobFlowID,
		"InstanceFleet": map[string]any{
			"Name":                   "task-fleet",
			"InstanceFleetType":      "TASK",
			"TargetOnDemandCapacity": 1,
			"TargetSpotCapacity":     1,
		},
	})
	require.Equal(t, http.StatusOK, addRec.Code)

	var added struct {
		InstanceFleetID string `json:"InstanceFleetId"`
	}
	require.NoError(t, json.Unmarshal(addRec.Body.Bytes(), &added))

	modRec := doEMRRequest(t, h, "ModifyInstanceFleet", map[string]any{
		"ClusterId": create.JobFlowID,
		"InstanceFleet": map[string]any{
			"InstanceFleetId":        added.InstanceFleetID,
			"TargetOnDemandCapacity": 8,
			"TargetSpotCapacity":     3,
		},
	})
	require.Equal(t, http.StatusOK, modRec.Code)

	listRec := doEMRRequest(t, h, "ListInstanceFleets", map[string]any{"ClusterId": create.JobFlowID})
	require.Equal(t, http.StatusOK, listRec.Code)

	var out struct {
		InstanceFleets []struct {
			ID                          string `json:"Id"`
			TargetOnDemandCapacity      int    `json:"TargetOnDemandCapacity"`
			TargetSpotCapacity          int    `json:"TargetSpotCapacity"`
			ProvisionedOnDemandCapacity int    `json:"ProvisionedOnDemandCapacity"`
			ProvisionedSpotCapacity     int    `json:"ProvisionedSpotCapacity"`
		} `json:"InstanceFleets"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
	require.Len(t, out.InstanceFleets, 1)
	f := out.InstanceFleets[0]
	assert.Equal(t, 8, f.TargetOnDemandCapacity)
	assert.Equal(t, 3, f.TargetSpotCapacity)
	assert.Equal(t, 8, f.ProvisionedOnDemandCapacity)
	assert.Equal(t, 3, f.ProvisionedSpotCapacity)
}

// TestWireShape_CancelSteps_RealEnumValues verifies CancelSteps reports the
// real CancelStepsRequestStatus enum values (SUBMITTED for a step that was
// still pending, FAILED for one that no longer is), not the fabricated
// "SUCCESS"/"QUEUED" strings this backend used to return -- a real client
// type-asserting on the enum would never recognize the old values.
func TestWireShape_CancelSteps_RealEnumValues(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{
		"Name": "cancel-enum-cluster",
		"Steps": []map[string]any{
			{
				"Name":            "step1",
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

	// First cancellation: the step is still PENDING, so it is accepted.
	firstRec := doEMRRequest(t, h, "CancelSteps", map[string]any{
		"ClusterId": create.JobFlowID,
		"StepIds":   []string{stepID},
	})
	require.Equal(t, http.StatusOK, firstRec.Code)

	var firstOut struct {
		CancelStepsInfoList []struct {
			StepID string `json:"StepId"`
			Status string `json:"Status"`
		} `json:"CancelStepsInfoList"`
	}
	require.NoError(t, json.Unmarshal(firstRec.Body.Bytes(), &firstOut))
	require.Len(t, firstOut.CancelStepsInfoList, 1)
	assert.Equal(t, "SUBMITTED", firstOut.CancelStepsInfoList[0].Status)

	// Second cancellation of the now-CANCELLED step must fail.
	secondRec := doEMRRequest(t, h, "CancelSteps", map[string]any{
		"ClusterId": create.JobFlowID,
		"StepIds":   []string{stepID},
	})
	require.Equal(t, http.StatusOK, secondRec.Code)

	var secondOut struct {
		CancelStepsInfoList []struct {
			StepID string `json:"StepId"`
			Status string `json:"Status"`
			Reason string `json:"Reason"`
		} `json:"CancelStepsInfoList"`
	}
	require.NoError(t, json.Unmarshal(secondRec.Body.Bytes(), &secondOut))
	require.Len(t, secondOut.CancelStepsInfoList, 1)
	assert.Equal(t, "FAILED", secondOut.CancelStepsInfoList[0].Status)
	assert.NotEmpty(t, secondOut.CancelStepsInfoList[0].Reason)
}

// TestWireShape_NotebookExecution_EpochTimestamps verifies
// StartNotebookExecution/DescribeNotebookExecution emit StartTime as an
// epoch-seconds JSON number, matching the real EMR awsjson1.1 wire format --
// it previously marshalled the embedded time.Time with Go's default RFC3339
// string encoding, which a real SDK client's smithytime.ParseEpochSeconds
// deserializer rejects outright.
func TestWireShape_NotebookExecution_EpochTimestamps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	startRec := doEMRRequest(t, h, "StartNotebookExecution", map[string]any{
		"EditorId":              "e-123",
		"NotebookExecutionName": "nb-exec",
		"ExecutionEngineConfig": map[string]any{"Id": "j-CLUSTER"},
	})
	require.Equal(t, http.StatusOK, startRec.Code)

	var started struct {
		NotebookExecutionID string `json:"NotebookExecutionId"`
	}
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &started))

	descRec := doEMRRequest(t, h, "DescribeNotebookExecution", map[string]any{
		"NotebookExecutionId": started.NotebookExecutionID,
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var out struct {
		NotebookExecution struct {
			StartTime float64 `json:"StartTime"`
		} `json:"NotebookExecution"`
	}
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &out))
	assert.NotZero(t, out.NotebookExecution.StartTime)
}
