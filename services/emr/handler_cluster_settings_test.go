package emr_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTerminationProtection(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "protected-cluster"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))
	clusterID := create.JobFlowID

	protectRec := doEMRRequest(t, h, "SetTerminationProtection", map[string]any{
		"JobFlowIds":           []string{clusterID},
		"TerminationProtected": true,
	})
	require.Equal(t, http.StatusOK, protectRec.Code)

	termRec := doEMRRequest(t, h, "TerminateJobFlows", map[string]any{
		"JobFlowIds": []string{clusterID},
	})
	assert.Equal(t, http.StatusBadRequest, termRec.Code)

	unprotectRec := doEMRRequest(t, h, "SetTerminationProtection", map[string]any{
		"JobFlowIds":           []string{clusterID},
		"TerminationProtected": false,
	})
	require.Equal(t, http.StatusOK, unprotectRec.Code)

	termRec2 := doEMRRequest(t, h, "TerminateJobFlows", map[string]any{
		"JobFlowIds": []string{clusterID},
	})
	assert.Equal(t, http.StatusOK, termRec2.Code)
}

func TestModifyCluster(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "modify-cluster"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))

	modRec := doEMRRequest(t, h, "ModifyCluster", map[string]any{
		"ClusterId":            create.JobFlowID,
		"StepConcurrencyLevel": 5,
	})
	require.Equal(t, http.StatusOK, modRec.Code)

	var modOut struct {
		StepConcurrencyLevel int `json:"StepConcurrencyLevel"`
	}
	require.NoError(t, json.Unmarshal(modRec.Body.Bytes(), &modOut))
	assert.Equal(t, 5, modOut.StepConcurrencyLevel)
}

func TestModifyCluster_InvalidRange(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "modify-cluster"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create))

	modRec := doEMRRequest(t, h, "ModifyCluster", map[string]any{
		"ClusterId":            create.JobFlowID,
		"StepConcurrencyLevel": 999,
	})
	assert.Equal(t, http.StatusBadRequest, modRec.Code)
}
