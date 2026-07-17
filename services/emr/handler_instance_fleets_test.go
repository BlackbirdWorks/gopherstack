package emr_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/emr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEMR_ListInstanceFleets(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "fleet-cluster"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))

	rec := doEMRRequest(t, h, "ListInstanceFleets", map[string]any{
		"ClusterId": createOut.JobFlowID,
	})

	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		InstanceFleets []emr.InstanceFleet `json:"InstanceFleets"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Empty(t, out.InstanceFleets)
}

func TestEMR_AddInstanceFleet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		clusterID string
		wantCode  int
	}{
		{
			name:      "adds fleet to existing cluster",
			clusterID: "",
			wantCode:  http.StatusOK,
		},
		{
			name:      "returns error for non-existent cluster",
			clusterID: "j-NOTEXIST",
			wantCode:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "fleet-cluster"})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createOut struct {
				JobFlowID string `json:"JobFlowId"`
			}
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))

			clusterID := tt.clusterID
			if clusterID == "" {
				clusterID = createOut.JobFlowID
			}

			rec := doEMRRequest(t, h, "AddInstanceFleet", map[string]any{
				"ClusterId": clusterID,
				"InstanceFleet": map[string]any{
					"InstanceFleetType": "TASK",
					"Name":              "task-fleet",
				},
			})

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var out struct {
					ClusterArn      string `json:"ClusterArn"`
					ClusterID       string `json:"ClusterId"`
					InstanceFleetID string `json:"InstanceFleetId"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotEmpty(t, out.InstanceFleetID)
				assert.Equal(t, clusterID, out.ClusterID)
				assert.Contains(t, out.ClusterArn, "elasticmapreduce")
			}
		})
	}
}

func TestEMR_AddInstanceFleet_ListInstanceFleets(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "fleet-cluster"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))

	// Add a fleet.
	addRec := doEMRRequest(t, h, "AddInstanceFleet", map[string]any{
		"ClusterId": createOut.JobFlowID,
		"InstanceFleet": map[string]any{
			"InstanceFleetType": "TASK",
			"Name":              "task-fleet",
		},
	})
	require.Equal(t, http.StatusOK, addRec.Code)

	// List fleets - should now have one.
	listRec := doEMRRequest(t, h, "ListInstanceFleets", map[string]any{
		"ClusterId": createOut.JobFlowID,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut struct {
		InstanceFleets []struct {
			ID   string `json:"Id"`
			Name string `json:"Name"`
		} `json:"InstanceFleets"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	require.Len(t, listOut.InstanceFleets, 1)
	assert.Equal(t, "task-fleet", listOut.InstanceFleets[0].Name)
	assert.NotEmpty(t, listOut.InstanceFleets[0].ID)
}

func TestEMR_ListInstanceFleets_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doEMRRequest(t, h, "ListInstanceFleets", map[string]any{
		"ClusterId": "j-NOTEXIST",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestInstanceFleet_CapacityTargets(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "fleet-cap-cluster"})
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
			"TargetOnDemandCapacity": 5,
			"TargetSpotCapacity":     10,
		},
	})
	require.Equal(t, http.StatusOK, addRec.Code)

	listRec := doEMRRequest(t, h, "ListInstanceFleets", map[string]any{"ClusterId": create.JobFlowID})
	require.Equal(t, http.StatusOK, listRec.Code)

	var out struct {
		InstanceFleets []struct {
			Status struct {
				State string `json:"State"`
			} `json:"Status"`
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
	assert.Equal(t, 5, f.TargetOnDemandCapacity)
	assert.Equal(t, 10, f.TargetSpotCapacity)
	assert.Equal(t, 5, f.ProvisionedOnDemandCapacity)
	assert.Equal(t, 10, f.ProvisionedSpotCapacity)
	assert.Equal(t, "RUNNING", f.Status.State)
	assert.NotEmpty(t, f.ID)
}

// TestNonNilInstanceFleets verifies ListInstanceFleets returns non-nil for no fleets.
func TestNonNilInstanceFleets(t *testing.T) {
	t.Parallel()

	b := emr.NewInMemoryBackend(testAccountID, testRegion)
	cluster, err := b.RunJobFlow(
		context.Background(),
		emr.RunJobFlowParams{Name: "nofleet-cluster", ReleaseLabel: "emr-6.0.0"},
	)
	require.NoError(t, err)

	fleets, err := b.ListInstanceFleets(context.Background(), cluster.ID)
	require.NoError(t, err)
	assert.NotNil(t, fleets)
	assert.Empty(t, fleets)
}

// TestListInstanceFleetsTyped verifies ListInstanceFleets HTTP response is typed.
func TestListInstanceFleetsTyped(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{
		"Name": "fleet-typed",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createOut struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))

	addRec := doEMRRequest(t, h, "AddInstanceFleet", map[string]any{
		"ClusterId":     createOut.JobFlowID,
		"InstanceFleet": map[string]any{"InstanceFleetType": "TASK", "Name": "typed-fleet"},
	})
	require.Equal(t, http.StatusOK, addRec.Code)

	listRec := doEMRRequest(t, h, "ListInstanceFleets", map[string]any{
		"ClusterId": createOut.JobFlowID,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var out struct {
		InstanceFleets []emr.InstanceFleet `json:"InstanceFleets"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
	require.Len(t, out.InstanceFleets, 1)
	assert.Equal(t, "typed-fleet", out.InstanceFleets[0].Name)
	assert.NotEmpty(t, out.InstanceFleets[0].ID)
}
