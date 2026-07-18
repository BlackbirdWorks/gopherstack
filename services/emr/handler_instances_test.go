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
