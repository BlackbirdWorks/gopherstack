package emr_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListInstances(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{
		"Name": "instances-cluster",
		"Instances": map[string]any{
			"InstanceGroups": []map[string]any{
				{"Name": "master", "InstanceRole": "MASTER", "InstanceType": "m5.xlarge", "InstanceCount": 1},
				{"Name": "core", "InstanceRole": "CORE", "InstanceType": "m5.2xlarge", "InstanceCount": 2},
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))

	listRec := doEMRRequest(t, h, "ListInstances", map[string]any{"ClusterId": create.JobFlowID})
	require.Equal(t, http.StatusOK, listRec.Code)

	var out struct {
		Instances []struct {
			ID            string `json:"Id"`
			Ec2InstanceID string `json:"Ec2InstanceId"`
		} `json:"Instances"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
	assert.Len(t, out.Instances, 3)
}

func TestListInstances_InstanceFleets(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "fleet-instances-cluster"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))

	addFleet := func(fleetType string, onDemand, spot int) string {
		rec := doEMRRequest(t, h, "AddInstanceFleet", map[string]any{
			"ClusterId": create.JobFlowID,
			"InstanceFleet": map[string]any{
				"InstanceFleetType":      fleetType,
				"Name":                   fleetType + "-fleet",
				"TargetOnDemandCapacity": onDemand,
				"TargetSpotCapacity":     spot,
			},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			InstanceFleetID string `json:"InstanceFleetId"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

		return out.InstanceFleetID
	}

	addFleet("MASTER", 1, 0)
	taskFleetID := addFleet("TASK", 1, 2)

	type instancesResp struct {
		Instances []struct {
			InstanceGroupID string `json:"InstanceGroupId"`
			InstanceFleetID string `json:"InstanceFleetId"`
			Market          string `json:"Market"`
		} `json:"Instances"`
	}

	tests := []struct {
		params  map[string]any
		name    string
		wantLen int
	}{
		{
			name:    "no filter returns every group and fleet instance",
			params:  map[string]any{"ClusterId": create.JobFlowID},
			wantLen: 4,
		},
		{
			name:    "instance fleet id filters to that fleet only",
			params:  map[string]any{"ClusterId": create.JobFlowID, "InstanceFleetId": taskFleetID},
			wantLen: 3,
		},
		{
			name:    "instance fleet type filters to matching fleets",
			params:  map[string]any{"ClusterId": create.JobFlowID, "InstanceFleetType": "MASTER"},
			wantLen: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doEMRRequest(t, h, "ListInstances", tt.params)
			require.Equal(t, http.StatusOK, rec.Code)

			var out instancesResp
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.Instances, tt.wantLen)

			for _, inst := range out.Instances {
				assert.Empty(t, inst.InstanceGroupID, "fleet instances must not carry an InstanceGroupId")
				assert.NotEmpty(t, inst.InstanceFleetID)
			}
		})
	}
}
