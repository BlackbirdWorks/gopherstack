package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

// ---------------------------------------------------------------------------
// Artifact
// ---------------------------------------------------------------------------

func TestHandler_ArtifactLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	recCreate := doSageMakerRequest(t, h, "CreateArtifact", map[string]any{
		"ArtifactName": "my-artifact",
		"ArtifactType": "DataSet",
		"Source":       map[string]any{"SourceUri": "s3://bucket/data.csv"},
		"Properties":   map[string]any{"env": "prod"},
	})
	require.Equal(t, http.StatusOK, recCreate.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(recCreate.Body.Bytes(), &createOut))
	artifactArn, _ := createOut["ArtifactArn"].(string)
	require.NotEmpty(t, artifactArn)
	assert.Contains(t, artifactArn, "artifact/")

	// Describe.
	recDesc := doSageMakerRequest(t, h, "DescribeArtifact", map[string]any{"ArtifactArn": artifactArn})
	require.Equal(t, http.StatusOK, recDesc.Code)

	var descOut map[string]any
	require.NoError(t, json.Unmarshal(recDesc.Body.Bytes(), &descOut))
	assert.Equal(t, "my-artifact", descOut["ArtifactName"])
	assert.Equal(t, "DataSet", descOut["ArtifactType"])
	assert.NotZero(t, descOut["CreationTime"])

	// Update.
	recUpdate := doSageMakerRequest(t, h, "UpdateArtifact", map[string]any{
		"ArtifactArn":  artifactArn,
		"ArtifactName": "renamed-artifact",
		"Properties":   map[string]any{"stage": "final"},
	})
	require.Equal(t, http.StatusOK, recUpdate.Code)

	recDesc2 := doSageMakerRequest(t, h, "DescribeArtifact", map[string]any{"ArtifactArn": artifactArn})
	var descOut2 map[string]any
	require.NoError(t, json.Unmarshal(recDesc2.Body.Bytes(), &descOut2))
	assert.Equal(t, "renamed-artifact", descOut2["ArtifactName"])
	props, _ := descOut2["Properties"].(map[string]any)
	assert.Equal(t, "final", props["stage"])
	assert.Equal(t, "prod", props["env"])

	// List.
	recList := doSageMakerRequest(t, h, "ListArtifacts", map[string]any{})
	require.Equal(t, http.StatusOK, recList.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listOut))
	assert.Len(t, listOut["ArtifactSummaries"].([]any), 1)

	// Delete.
	recDelete := doSageMakerRequest(t, h, "DeleteArtifact", map[string]any{"ArtifactArn": artifactArn})
	require.Equal(t, http.StatusOK, recDelete.Code)

	recList2 := doSageMakerRequest(t, h, "ListArtifacts", map[string]any{})
	var listOut2 map[string]any
	require.NoError(t, json.Unmarshal(recList2.Body.Bytes(), &listOut2))
	assert.Empty(t, listOut2["ArtifactSummaries"].([]any))
}

func TestHandler_Artifact_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, op := range []string{"DescribeArtifact", "UpdateArtifact", "DeleteArtifact"} {
		t.Run(op, func(t *testing.T) {
			t.Parallel()

			body := map[string]any{"ArtifactArn": "arn:aws:sagemaker:us-east-1:0:artifact/nope"}
			rec := doSageMakerRequest(t, h, op, body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestHandler_Artifact_MissingRequiredFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		body map[string]any
		name string
	}{
		{name: "missing ArtifactType", body: map[string]any{"Source": map[string]any{"SourceUri": "s3://x"}}},
		{name: "missing Source", body: map[string]any{"ArtifactType": "DataSet"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doSageMakerRequest(t, h, "CreateArtifact", tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestHandler_ListArtifacts_FilterByType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateArtifact", map[string]any{
		"ArtifactName": "a1", "ArtifactType": "DataSet",
		"Source": map[string]any{"SourceUri": "s3://bucket/a1"},
	})
	doSageMakerRequest(t, h, "CreateArtifact", map[string]any{
		"ArtifactName": "a2", "ArtifactType": "Model",
		"Source": map[string]any{"SourceUri": "s3://bucket/a2"},
	})

	rec := doSageMakerRequest(t, h, "ListArtifacts", map[string]any{"ArtifactType": "Model"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	summaries := out["ArtifactSummaries"].([]any)
	require.Len(t, summaries, 1)
	assert.Equal(t, "a2", summaries[0].(map[string]any)["ArtifactName"])
}

// ---------------------------------------------------------------------------
// Context
// ---------------------------------------------------------------------------

func TestHandler_ContextLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	recCreate := doSageMakerRequest(t, h, "CreateContext", map[string]any{
		"ContextName": "my-context",
		"ContextType": "Endpoint",
		"Source":      map[string]any{"SourceUri": "arn:aws:sagemaker:us-east-1:0:endpoint/my-endpoint"},
	})
	require.Equal(t, http.StatusOK, recCreate.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(recCreate.Body.Bytes(), &createOut))
	contextArn, _ := createOut["ContextArn"].(string)
	require.NotEmpty(t, contextArn)
	assert.Contains(t, contextArn, "context/my-context")

	recDesc := doSageMakerRequest(t, h, "DescribeContext", map[string]any{"ContextName": "my-context"})
	require.Equal(t, http.StatusOK, recDesc.Code)

	var descOut map[string]any
	require.NoError(t, json.Unmarshal(recDesc.Body.Bytes(), &descOut))
	assert.Equal(t, "Endpoint", descOut["ContextType"])

	recUpdate := doSageMakerRequest(t, h, "UpdateContext", map[string]any{
		"ContextName": "my-context",
		"Description": "updated",
	})
	require.Equal(t, http.StatusOK, recUpdate.Code)

	recDesc2 := doSageMakerRequest(t, h, "DescribeContext", map[string]any{"ContextName": "my-context"})
	var descOut2 map[string]any
	require.NoError(t, json.Unmarshal(recDesc2.Body.Bytes(), &descOut2))
	assert.Equal(t, "updated", descOut2["Description"])

	recList := doSageMakerRequest(t, h, "ListContexts", map[string]any{})
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listOut))
	assert.Len(t, listOut["ContextSummaries"].([]any), 1)

	recDelete := doSageMakerRequest(t, h, "DeleteContext", map[string]any{"ContextName": "my-context"})
	require.Equal(t, http.StatusOK, recDelete.Code)

	recDesc3 := doSageMakerRequest(t, h, "DescribeContext", map[string]any{"ContextName": "my-context"})
	assert.Equal(t, http.StatusBadRequest, recDesc3.Code)
}

func TestHandler_Context_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, op := range []string{"DescribeContext", "UpdateContext", "DeleteContext"} {
		t.Run(op, func(t *testing.T) {
			t.Parallel()

			rec := doSageMakerRequest(t, h, op, map[string]any{"ContextName": "nonexistent"})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestHandler_CreateContext_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"ContextName": "dup-context",
		"ContextType": "Endpoint",
		"Source":      map[string]any{"SourceUri": "arn:aws:sagemaker:us-east-1:0:endpoint/e"},
	}

	rec1 := doSageMakerRequest(t, h, "CreateContext", body)
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := doSageMakerRequest(t, h, "CreateContext", body)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// ---------------------------------------------------------------------------
// Action (Describe/Update/Delete/List; Create is tested elsewhere)
// ---------------------------------------------------------------------------

func TestHandler_ActionLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	recCreate := doSageMakerRequest(t, h, "CreateAction", map[string]any{
		"ActionName": "my-action",
		"ActionType": "ModelDeployment",
		"Source":     map[string]any{"SourceUri": "arn:aws:sagemaker:us-east-1:0:endpoint/e"},
		"Status":     "InProgress",
	})
	require.Equal(t, http.StatusOK, recCreate.Code)

	recDesc := doSageMakerRequest(t, h, "DescribeAction", map[string]any{"ActionName": "my-action"})
	require.Equal(t, http.StatusOK, recDesc.Code)

	var descOut map[string]any
	require.NoError(t, json.Unmarshal(recDesc.Body.Bytes(), &descOut))
	assert.Equal(t, "ModelDeployment", descOut["ActionType"])
	assert.Equal(t, "InProgress", descOut["Status"])

	recUpdate := doSageMakerRequest(t, h, "UpdateAction", map[string]any{
		"ActionName": "my-action",
		"Status":     "Completed",
	})
	require.Equal(t, http.StatusOK, recUpdate.Code)

	recDesc2 := doSageMakerRequest(t, h, "DescribeAction", map[string]any{"ActionName": "my-action"})
	var descOut2 map[string]any
	require.NoError(t, json.Unmarshal(recDesc2.Body.Bytes(), &descOut2))
	assert.Equal(t, "Completed", descOut2["Status"])

	recList := doSageMakerRequest(t, h, "ListActions", map[string]any{})
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listOut))
	assert.Len(t, listOut["ActionSummaries"].([]any), 1)

	recDelete := doSageMakerRequest(t, h, "DeleteAction", map[string]any{"ActionName": "my-action"})
	require.Equal(t, http.StatusOK, recDelete.Code)

	recDesc3 := doSageMakerRequest(t, h, "DescribeAction", map[string]any{"ActionName": "my-action"})
	assert.Equal(t, http.StatusBadRequest, recDesc3.Code)
}

func TestHandler_Action_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, op := range []string{"DescribeAction", "UpdateAction", "DeleteAction"} {
		t.Run(op, func(t *testing.T) {
			t.Parallel()

			rec := doSageMakerRequest(t, h, op, map[string]any{"ActionName": "nonexistent"})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// ---------------------------------------------------------------------------
// Association (Delete/List; AddAssociation is tested elsewhere)
// ---------------------------------------------------------------------------

func TestHandler_AssociationLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	srcArn := "arn:aws:sagemaker:us-east-1:000000000000:action/src"
	dstArn := "arn:aws:sagemaker:us-east-1:000000000000:artifact/dst"

	recAdd := doSageMakerRequest(t, h, "AddAssociation", map[string]any{
		"SourceArn":       srcArn,
		"DestinationArn":  dstArn,
		"AssociationType": "ContributedTo",
	})
	require.Equal(t, http.StatusOK, recAdd.Code)

	recList := doSageMakerRequest(t, h, "ListAssociations", map[string]any{})
	require.Equal(t, http.StatusOK, recList.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listOut))
	summaries := listOut["AssociationSummaries"].([]any)
	require.Len(t, summaries, 1)
	assoc := summaries[0].(map[string]any)
	assert.Equal(t, srcArn, assoc["SourceArn"])
	assert.Equal(t, dstArn, assoc["DestinationArn"])
	assert.Equal(t, "ContributedTo", assoc["AssociationType"])

	recDelete := doSageMakerRequest(t, h, "DeleteAssociation", map[string]any{
		"SourceArn":      srcArn,
		"DestinationArn": dstArn,
	})
	require.Equal(t, http.StatusOK, recDelete.Code)

	recList2 := doSageMakerRequest(t, h, "ListAssociations", map[string]any{})
	var listOut2 map[string]any
	require.NoError(t, json.Unmarshal(recList2.Body.Bytes(), &listOut2))
	assert.Empty(t, listOut2["AssociationSummaries"].([]any))
}

func TestHandler_DeleteAssociation_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "DeleteAssociation", map[string]any{
		"SourceArn":      "arn:aws:sagemaker:us-east-1:0:action/a",
		"DestinationArn": "arn:aws:sagemaker:us-east-1:0:artifact/b",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListAssociations_ResolvesEntityNames(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	recAction := doSageMakerRequest(t, h, "CreateAction", map[string]any{
		"ActionName": "src-action",
		"ActionType": "ModelDeployment",
		"Source":     map[string]any{"SourceUri": "arn:aws:sagemaker:us-east-1:0:endpoint/e"},
	})
	var actionOut map[string]any
	require.NoError(t, json.Unmarshal(recAction.Body.Bytes(), &actionOut))
	actionArn := actionOut["ActionArn"].(string)

	recArtifact := doSageMakerRequest(t, h, "CreateArtifact", map[string]any{
		"ArtifactName": "dst-artifact",
		"ArtifactType": "DataSet",
		"Source":       map[string]any{"SourceUri": "s3://bucket/data"},
	})
	var artifactOut map[string]any
	require.NoError(t, json.Unmarshal(recArtifact.Body.Bytes(), &artifactOut))
	artifactArn := artifactOut["ArtifactArn"].(string)

	doSageMakerRequest(t, h, "AddAssociation", map[string]any{
		"SourceArn":       actionArn,
		"DestinationArn":  artifactArn,
		"AssociationType": "Produced",
	})

	rec := doSageMakerRequest(t, h, "ListAssociations", map[string]any{})
	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assoc := out["AssociationSummaries"].([]any)[0].(map[string]any)
	assert.Equal(t, "src-action", assoc["SourceName"])
	assert.Equal(t, "ModelDeployment", assoc["SourceType"])
	assert.Equal(t, "dst-artifact", assoc["DestinationName"])
	assert.Equal(t, "DataSet", assoc["DestinationType"])
}

// ---------------------------------------------------------------------------
// QueryLineage — graph traversal
// ---------------------------------------------------------------------------

func TestHandler_QueryLineage_Traversal(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Build a small lineage graph:
	//   context/train-job -> action/deploy -> artifact/model -> artifact/endpoint
	// (associations point Source -> Destination)
	contextArn := createTestContext(t, h, "train-job")
	actionArn := createTestAction(t, h, "deploy")
	modelArtifactArn := createTestArtifact(t, h, "model")
	endpointArtifactArn := createTestArtifact(t, h, "endpoint")

	doSageMakerRequest(t, h, "AddAssociation", map[string]any{
		"SourceArn": contextArn, "DestinationArn": actionArn, "AssociationType": "ContributedTo",
	})
	doSageMakerRequest(t, h, "AddAssociation", map[string]any{
		"SourceArn": actionArn, "DestinationArn": modelArtifactArn, "AssociationType": "Produced",
	})
	doSageMakerRequest(t, h, "AddAssociation", map[string]any{
		"SourceArn": modelArtifactArn, "DestinationArn": endpointArtifactArn, "AssociationType": "AssociatedWith",
	})

	// Descendants from the action should reach the two downstream artifacts only.
	rec := doSageMakerRequest(t, h, "QueryLineage", map[string]any{
		"StartArns": []string{actionArn},
		"Direction": "Descendants",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	vertices := out["Vertices"].([]any)
	arns := make([]string, 0, len(vertices))

	for _, v := range vertices {
		arns = append(arns, v.(map[string]any)["Arn"].(string))
	}

	assert.ElementsMatch(t, []string{actionArn, modelArtifactArn, endpointArtifactArn}, arns)
	assert.NotContains(t, arns, contextArn)

	edges := out["Edges"].([]any)
	assert.Len(t, edges, 2)
}

func TestHandler_QueryLineage_MaxDepth(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	a1 := createTestArtifact(t, h, "a1")
	a2 := createTestArtifact(t, h, "a2")
	a3 := createTestArtifact(t, h, "a3")

	doSageMakerRequest(t, h, "AddAssociation", map[string]any{
		"SourceArn": a1, "DestinationArn": a2, "AssociationType": "DerivedFrom",
	})
	doSageMakerRequest(t, h, "AddAssociation", map[string]any{
		"SourceArn": a2, "DestinationArn": a3, "AssociationType": "DerivedFrom",
	})

	rec := doSageMakerRequest(t, h, "QueryLineage", map[string]any{
		"StartArns": []string{a1},
		"Direction": "Descendants",
		"MaxDepth":  1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	vertices := out["Vertices"].([]any)
	arns := make([]string, 0, len(vertices))

	for _, v := range vertices {
		arns = append(arns, v.(map[string]any)["Arn"].(string))
	}

	assert.ElementsMatch(t, []string{a1, a2}, arns)
	assert.NotContains(t, arns, a3)
}

func TestHandler_QueryLineage_MissingStartArns(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "QueryLineage", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_QueryLineage_UnknownArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "QueryLineage", map[string]any{
		"StartArns": []string{"arn:aws:sagemaker:us-east-1:0:artifact/unknown"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	vertices := out["Vertices"].([]any)
	require.Len(t, vertices, 1)
	assert.Empty(t, vertices[0].(map[string]any)["Type"])
}

// ---------------------------------------------------------------------------
// LineageGroup
// ---------------------------------------------------------------------------

func TestHandler_DescribeLineageGroup(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "DescribeLineageGroup", map[string]any{
		"LineageGroupName": "sagemaker-default-lineage-group",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Contains(t, out["LineageGroupArn"], "lineage-group/sagemaker-default-lineage-group")
}

func TestHandler_DescribeLineageGroup_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "DescribeLineageGroup", map[string]any{"LineageGroupName": "other-group"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListLineageGroups(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListLineageGroups", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	summaries := out["LineageGroupSummaries"].([]any)
	require.Len(t, summaries, 1)
	assert.Equal(t, "sagemaker-default-lineage-group", summaries[0].(map[string]any)["LineageGroupName"])
}

func TestHandler_GetLineageGroupPolicy_NoPolicyAttached(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "GetLineageGroupPolicy", map[string]any{
		"LineageGroupName": "sagemaker-default-lineage-group",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetLineageGroupPolicy_UnknownGroup(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "GetLineageGroupPolicy", map[string]any{"LineageGroupName": "other-group"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// GetSupportedOperations coverage
// ---------------------------------------------------------------------------

func TestHandler_SupportedOps_Lineage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	required := []string{
		"CreateArtifact", "DescribeArtifact", "UpdateArtifact", "DeleteArtifact", "ListArtifacts",
		"CreateContext", "DescribeContext", "UpdateContext", "DeleteContext", "ListContexts",
		"CreateAction", "DescribeAction", "UpdateAction", "DeleteAction", "ListActions",
		"AddAssociation", "DeleteAssociation", "ListAssociations",
		"QueryLineage", "DescribeLineageGroup", "ListLineageGroups", "GetLineageGroupPolicy",
	}

	for _, op := range required {
		assert.Contains(t, ops, op, "missing op: %s", op)
	}
}

// ---------------------------------------------------------------------------
// test helpers
// ---------------------------------------------------------------------------

func createTestContext(t *testing.T, h *sagemaker.Handler, name string) string {
	t.Helper()

	rec := doSageMakerRequest(t, h, "CreateContext", map[string]any{
		"ContextName": name,
		"ContextType": "Experiment",
		"Source":      map[string]any{"SourceUri": "arn:aws:sagemaker:us-east-1:0:context-source/" + name},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out["ContextArn"].(string)
}

func createTestAction(t *testing.T, h *sagemaker.Handler, name string) string {
	t.Helper()

	rec := doSageMakerRequest(t, h, "CreateAction", map[string]any{
		"ActionName": name,
		"ActionType": "ModelDeployment",
		"Source":     map[string]any{"SourceUri": "arn:aws:sagemaker:us-east-1:0:action-source/" + name},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out["ActionArn"].(string)
}

func createTestArtifact(t *testing.T, h *sagemaker.Handler, name string) string {
	t.Helper()

	rec := doSageMakerRequest(t, h, "CreateArtifact", map[string]any{
		"ArtifactName": name,
		"ArtifactType": "DataSet",
		"Source":       map[string]any{"SourceUri": "s3://bucket/" + name},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out["ArtifactArn"].(string)
}
