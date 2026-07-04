package ssm_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDescribeAvailablePatches_SeededCatalog verifies that the available
// patches catalog is backed by a real built-in seed instead of permanently
// returning an empty list.
func TestDescribeAvailablePatches_SeededCatalog(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	rec := doRequest(t, h, "DescribeAvailablePatches", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Patches []struct {
			Name string `json:"Name"`
		} `json:"Patches"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotEmpty(t, out.Patches, "the catalog must be seeded with real built-in patches")
}

// TestSendCommand_RunPatchBaseline_PopulatesInstanceState verifies that
// running the AWS-RunPatchBaseline document against a managed instance
// records real InstancePatchState/PatchComplianceData, rather than leaving
// those stores permanently empty.
func TestSendCommand_RunPatchBaseline_PopulatesInstanceState(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	createResp := doRequest(t, h, "CreatePatchBaseline", `{
		"Name": "patch-wiring-baseline",
		"OperatingSystem": "AMAZON_LINUX_2",
		"ApprovedPatches": ["ALAS2-2024-2451"],
		"ApprovedPatchesComplianceLevel": "CRITICAL"
	}`)
	require.Equal(t, http.StatusOK, createResp.Code)

	var created struct {
		BaselineID string `json:"BaselineId"`
	}
	require.NoError(t, json.Unmarshal(createResp.Body.Bytes(), &created))
	require.NotEmpty(t, created.BaselineID)

	regResp := doRequest(t, h, "RegisterPatchBaselineForPatchGroup", `{
		"BaselineId":"`+created.BaselineID+`","PatchGroup":"wiring-group"
	}`)
	require.Equal(t, http.StatusOK, regResp.Code)

	sendResp := doRequest(t, h, "SendCommand", `{
		"DocumentName": "AWS-RunPatchBaseline",
		"InstanceIds": ["i-patchtest"],
		"Parameters": {"PatchGroup": ["wiring-group"], "Operation": ["Install"]}
	}`)
	require.Equal(t, http.StatusOK, sendResp.Code)

	stateResp := doRequest(t, h, "DescribeInstancePatchStates", `{"InstanceIds":["i-patchtest"]}`)
	require.Equal(t, http.StatusOK, stateResp.Code)

	var states struct {
		InstancePatchStates []struct {
			InstanceID string `json:"InstanceId"`
			BaselineID string `json:"BaselineId"`
			PatchGroup string `json:"PatchGroup"`
		} `json:"InstancePatchStates"`
	}
	require.NoError(t, json.Unmarshal(stateResp.Body.Bytes(), &states))
	require.Len(t, states.InstancePatchStates, 1)
	assert.Equal(t, "i-patchtest", states.InstancePatchStates[0].InstanceID)
	assert.Equal(t, created.BaselineID, states.InstancePatchStates[0].BaselineID)
	assert.Equal(t, "wiring-group", states.InstancePatchStates[0].PatchGroup)

	patchesResp := doRequest(t, h, "DescribeInstancePatches", `{"InstanceId":"i-patchtest"}`)
	require.Equal(t, http.StatusOK, patchesResp.Code)

	var patches struct {
		Patches []struct {
			Title string `json:"Title"`
			State string `json:"State"`
		} `json:"Patches"`
	}
	require.NoError(t, json.Unmarshal(patchesResp.Body.Bytes(), &patches))
	require.NotEmpty(t, patches.Patches)

	var sawInstalled bool
	for _, p := range patches.Patches {
		if p.Title == "ALAS2-2024-2451" {
			assert.Equal(t, "Installed", p.State)
			sawInstalled = true
		}
	}
	assert.True(t, sawInstalled, "the explicitly-approved patch must be reported Installed")
}

// TestDescribeInstanceProperties_DerivedFromActivations verifies that
// registered managed instances (activations) are reflected in
// DescribeInstanceProperties rather than requiring separate seeding.
func TestDescribeInstanceProperties_DerivedFromActivations(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler(t)

	actResp := doRequest(t, h, "CreateActivation", `{"IamRole":"my-role"}`)
	require.Equal(t, http.StatusOK, actResp.Code)

	var activation struct {
		ActivationID string `json:"ActivationId"`
	}
	require.NoError(t, json.Unmarshal(actResp.Body.Bytes(), &activation))
	require.NotEmpty(t, activation.ActivationID)

	propsResp := doRequest(t, h, "DescribeInstanceProperties", `{}`)
	require.Equal(t, http.StatusOK, propsResp.Code)
	assert.Contains(t, propsResp.Body.String(), activation.ActivationID)
}
