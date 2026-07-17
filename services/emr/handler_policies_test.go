package emr_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManagedScalingPolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "msp-cluster"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))
	clusterID := create.JobFlowID

	putRec := doEMRRequest(t, h, "PutManagedScalingPolicy", map[string]any{
		"ClusterId": clusterID,
		"ManagedScalingPolicy": map[string]any{
			"ComputeLimits": map[string]any{
				"UnitType":             "InstanceFleetUnits",
				"MinimumCapacityUnits": 1,
				"MaximumCapacityUnits": 10,
			},
		},
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doEMRRequest(t, h, "GetManagedScalingPolicy", map[string]any{"ClusterId": clusterID})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut struct {
		ManagedScalingPolicy struct {
			ComputeLimits struct {
				UnitType             string `json:"UnitType"`
				MaximumCapacityUnits int    `json:"MaximumCapacityUnits"`
			} `json:"ComputeLimits"`
		} `json:"ManagedScalingPolicy"`
	}
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	assert.Equal(t, "InstanceFleetUnits", getOut.ManagedScalingPolicy.ComputeLimits.UnitType)
	assert.Equal(t, 10, getOut.ManagedScalingPolicy.ComputeLimits.MaximumCapacityUnits)

	removeRec := doEMRRequest(t, h, "RemoveManagedScalingPolicy", map[string]any{"ClusterId": clusterID})
	assert.Equal(t, http.StatusOK, removeRec.Code)
}

func TestAutoTerminationPolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "atp-cluster"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))
	clusterID := create.JobFlowID

	putRec := doEMRRequest(t, h, "PutAutoTerminationPolicy", map[string]any{
		"ClusterId":             clusterID,
		"AutoTerminationPolicy": map[string]any{"IdleTimeout": 3600},
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec := doEMRRequest(t, h, "GetAutoTerminationPolicy", map[string]any{"ClusterId": clusterID})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut struct {
		AutoTerminationPolicy struct {
			IdleTimeout int64 `json:"IdleTimeout"`
		} `json:"AutoTerminationPolicy"`
	}
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	assert.Equal(t, int64(3600), getOut.AutoTerminationPolicy.IdleTimeout)
}

func TestAutoTerminationPolicy_InvalidTimeout(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "atp-cluster"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))

	putRec := doEMRRequest(t, h, "PutAutoTerminationPolicy", map[string]any{
		"ClusterId":             create.JobFlowID,
		"AutoTerminationPolicy": map[string]any{"IdleTimeout": 10},
	})
	assert.Equal(t, http.StatusBadRequest, putRec.Code)
}

func TestBlockPublicAccessConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	getRec := doEMRRequest(t, h, "GetBlockPublicAccessConfiguration", map[string]any{})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut struct {
		BlockPublicAccessConfiguration struct {
			BlockPublicSecurityGroupRules bool `json:"BlockPublicSecurityGroupRules"`
		} `json:"BlockPublicAccessConfiguration"`
	}
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	assert.True(t, getOut.BlockPublicAccessConfiguration.BlockPublicSecurityGroupRules)

	putRec := doEMRRequest(t, h, "PutBlockPublicAccessConfiguration", map[string]any{
		"BlockPublicAccessConfiguration": map[string]any{
			"BlockPublicSecurityGroupRules":          false,
			"PermittedPublicSecurityGroupRuleRanges": []map[string]any{},
		},
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	getRec2 := doEMRRequest(t, h, "GetBlockPublicAccessConfiguration", map[string]any{})
	require.NoError(t, json.Unmarshal(getRec2.Body.Bytes(), &getOut))
	assert.False(t, getOut.BlockPublicAccessConfiguration.BlockPublicSecurityGroupRules)
}

func TestAutoScalingPolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{
		"Name": "asg-cluster",
		"Instances": map[string]any{
			"InstanceGroups": []map[string]any{
				{"Name": "core", "InstanceRole": "CORE", "InstanceType": "m5.xlarge", "InstanceCount": 2},
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))

	listRec := doEMRRequest(t, h, "ListInstanceGroups", map[string]any{"ClusterId": create.JobFlowID})
	var listOut struct {
		InstanceGroups []struct {
			ID string `json:"Id"`
		} `json:"InstanceGroups"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	require.NotEmpty(t, listOut.InstanceGroups)
	groupID := listOut.InstanceGroups[0].ID

	putRec := doEMRRequest(t, h, "PutAutoScalingPolicy", map[string]any{
		"ClusterId":       create.JobFlowID,
		"InstanceGroupId": groupID,
		"AutoScalingPolicy": map[string]any{
			"Constraints": map[string]any{"MinCapacity": 1, "MaxCapacity": 10},
		},
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	var putOut struct {
		AutoScalingPolicy *struct {
			Constraints struct {
				MaxCapacity int `json:"MaxCapacity"`
			} `json:"Constraints"`
		} `json:"AutoScalingPolicy"`
	}
	require.NoError(t, json.Unmarshal(putRec.Body.Bytes(), &putOut))
	require.NotNil(t, putOut.AutoScalingPolicy)
	assert.Equal(t, 10, putOut.AutoScalingPolicy.Constraints.MaxCapacity)

	removeRec := doEMRRequest(t, h, "RemoveAutoScalingPolicy", map[string]any{
		"ClusterId":       create.JobFlowID,
		"InstanceGroupId": groupID,
	})
	assert.Equal(t, http.StatusOK, removeRec.Code)
}
