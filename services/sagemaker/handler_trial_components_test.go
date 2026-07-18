package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_ListAndDisassociateTrialComponents(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateExperiment", map[string]any{"ExperimentName": "exp-a"})
	doSageMakerRequest(t, h, "CreateTrial", map[string]any{"TrialName": "trial-a", "ExperimentName": "exp-a"})
	doSageMakerRequest(t, h, "CreateTrialComponent", map[string]any{"TrialComponentName": "tc-a"})
	doSageMakerRequest(t, h, "CreateTrialComponent", map[string]any{"TrialComponentName": "tc-b"})
	doSageMakerRequest(t, h, "AssociateTrialComponent", map[string]any{
		"TrialName": "trial-a", "TrialComponentName": "tc-a",
	})

	// Filtering by TrialName returns only the associated component.
	recByTrial := doSageMakerRequest(t, h, "ListTrialComponents", map[string]any{"TrialName": "trial-a"})
	require.Equal(t, http.StatusOK, recByTrial.Code)

	var byTrial map[string]any
	require.NoError(t, json.Unmarshal(recByTrial.Body.Bytes(), &byTrial))
	summaries, _ := byTrial["TrialComponentSummaries"].([]any)
	require.Len(t, summaries, 1)
	assert.Equal(t, "tc-a", summaries[0].(map[string]any)["TrialComponentName"])

	// Filtering by ExperimentName joins through the trial.
	recByExp := doSageMakerRequest(t, h, "ListTrialComponents", map[string]any{"ExperimentName": "exp-a"})
	require.Equal(t, http.StatusOK, recByExp.Code)

	var byExp map[string]any
	require.NoError(t, json.Unmarshal(recByExp.Body.Bytes(), &byExp))
	expSummaries, _ := byExp["TrialComponentSummaries"].([]any)
	require.Len(t, expSummaries, 1)

	// No filter returns every trial component regardless of association.
	recAll := doSageMakerRequest(t, h, "ListTrialComponents", map[string]any{})
	require.Equal(t, http.StatusOK, recAll.Code)

	var all map[string]any
	require.NoError(t, json.Unmarshal(recAll.Body.Bytes(), &all))
	allSummaries, _ := all["TrialComponentSummaries"].([]any)
	assert.Len(t, allSummaries, 2)

	// Disassociate, then the trial-scoped list is empty.
	recDisassoc := doSageMakerRequest(t, h, "DisassociateTrialComponent", map[string]any{
		"TrialName": "trial-a", "TrialComponentName": "tc-a",
	})
	require.Equal(t, http.StatusOK, recDisassoc.Code)

	var disassocOut map[string]any
	require.NoError(t, json.Unmarshal(recDisassoc.Body.Bytes(), &disassocOut))
	assert.NotEmpty(t, disassocOut["TrialArn"])
	assert.NotEmpty(t, disassocOut["TrialComponentArn"])

	recByTrial2 := doSageMakerRequest(t, h, "ListTrialComponents", map[string]any{"TrialName": "trial-a"})
	require.Equal(t, http.StatusOK, recByTrial2.Code)

	var byTrial2 map[string]any
	require.NoError(t, json.Unmarshal(recByTrial2.Body.Bytes(), &byTrial2))
	assert.Empty(t, byTrial2["TrialComponentSummaries"])
}

func TestHandler_DisassociateTrialComponent_Idempotent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Disassociating components that were never associated still succeeds
	// and returns the resources' ARNs (mirrors AssociateTrialComponent's
	// non-strict existence checks).
	rec := doSageMakerRequest(t, h, "DisassociateTrialComponent", map[string]any{
		"TrialName": "never-existed", "TrialComponentName": "never-existed-either",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Contains(t, out["TrialArn"], "never-existed")
}

func TestHandler_TrialComponent_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create trial component.
	recCreate := doSageMakerRequest(t, h, "CreateTrialComponent", map[string]any{
		"TrialComponentName": "my-component",
	})
	assert.Equal(t, http.StatusOK, recCreate.Code)

	// Describe trial component.
	recDesc := doSageMakerRequest(t, h, "DescribeTrialComponent", map[string]any{
		"TrialComponentName": "my-component",
	})
	assert.Equal(t, http.StatusOK, recDesc.Code)

	// Delete trial component.
	recDelete := doSageMakerRequest(t, h, "DeleteTrialComponent", map[string]any{
		"TrialComponentName": "my-component",
	})
	assert.Equal(t, http.StatusOK, recDelete.Code)
}

func TestHandler_UpdateTrialComponent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateTrialComponent", map[string]any{
		"TrialComponentName": "my-tc",
	})

	rec := doSageMakerRequest(t, h, "UpdateTrialComponent", map[string]any{
		"TrialComponentName": "my-tc",
		"DisplayName":        "TC Display",
		"Status":             "InProgress",
		"Parameters": map[string]any{
			"lr": map[string]any{"NumberValue": 0.001},
		},
		"InputArtifacts": map[string]any{
			"train": map[string]any{"Value": "s3://bucket/train", "MediaType": "text/csv"},
		},
		"OutputArtifacts": map[string]any{
			"model": map[string]any{"Value": "s3://bucket/model.tar.gz"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var updateResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
	assert.NotEmpty(t, updateResp["TrialComponentArn"])

	// Describe returns updated fields
	rec = doSageMakerRequest(t, h, "DescribeTrialComponent", map[string]any{
		"TrialComponentName": "my-tc",
	})
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "TC Display", descResp["DisplayName"])
	assert.Equal(t, "InProgress", descResp["Status"])
	assert.NotNil(t, descResp["Parameters"])
	assert.NotNil(t, descResp["InputArtifacts"])
	assert.NotNil(t, descResp["OutputArtifacts"])
}
