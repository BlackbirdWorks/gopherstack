package emr_test

import (
	"encoding/json/v2"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func TestTerminationProtection(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "protected-cluster"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create, jsonx.V1()))
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

// TestSetKeepJobFlowAliveWhenNoSteps_SyncsAutoTerminate covers a real,
// separately-filed bug (gopherstack-g3ex): SetKeepJobFlowAliveWhenNoSteps
// updated cluster.KeepJobFlowAliveWhenNoSteps but not cluster.AutoTerminate,
// its real inverse field (types.Cluster.AutoTerminate, emr@v1.64.4
// types/types.go:314-315), so a client that toggled the setting after
// creation would see AutoTerminate drift stale on the next DescribeCluster.
// Both write directions are exercised on one cluster so a one-way-only fix
// (e.g. only handling keep=true) cannot pass.
func TestSetKeepJobFlowAliveWhenNoSteps_SyncsAutoTerminate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "keep-alive-sync-cluster"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create, jsonx.V1()))
	clusterID := create.JobFlowID

	describe := func() bool {
		rec := doEMRRequest(t, h, "DescribeCluster", map[string]any{"ClusterId": clusterID})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			Cluster struct {
				AutoTerminate bool `json:"AutoTerminate"`
			} `json:"Cluster"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out, jsonx.V1()))

		return out.Cluster.AutoTerminate
	}

	assert.True(t, describe(), "default KeepJobFlowAliveWhenNoSteps=false must echo AutoTerminate=true")

	keepRec := doEMRRequest(t, h, "SetKeepJobFlowAliveWhenNoSteps", map[string]any{
		"JobFlowIds":                  []string{clusterID},
		"KeepJobFlowAliveWhenNoSteps": true,
	})
	require.Equal(t, http.StatusOK, keepRec.Code)
	assert.False(t, describe(), "keep=true must flip AutoTerminate to false")

	unkeepRec := doEMRRequest(t, h, "SetKeepJobFlowAliveWhenNoSteps", map[string]any{
		"JobFlowIds":                  []string{clusterID},
		"KeepJobFlowAliveWhenNoSteps": false,
	})
	require.Equal(t, http.StatusOK, unkeepRec.Code)
	assert.True(t, describe(), "keep=false must flip AutoTerminate back to true")
}

func TestModifyCluster(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doEMRRequest(t, h, "RunJobFlow", map[string]any{"Name": "modify-cluster"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var create struct {
		JobFlowID string `json:"JobFlowId"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create, jsonx.V1()))

	modRec := doEMRRequest(t, h, "ModifyCluster", map[string]any{
		"ClusterId":            create.JobFlowID,
		"StepConcurrencyLevel": 5,
	})
	require.Equal(t, http.StatusOK, modRec.Code)

	var modOut struct {
		StepConcurrencyLevel int `json:"StepConcurrencyLevel"`
	}
	require.NoError(t, json.Unmarshal(modRec.Body.Bytes(), &modOut, jsonx.V1()))
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
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &create, jsonx.V1()))

	modRec := doEMRRequest(t, h, "ModifyCluster", map[string]any{
		"ClusterId":            create.JobFlowID,
		"StepConcurrencyLevel": 999,
	})
	assert.Equal(t, http.StatusBadRequest, modRec.Code)
}
