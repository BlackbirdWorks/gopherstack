package sagemaker_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
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

// TestHandler_Artifact_MetadataProperties_RoundTrip proves, through the real
// aws-sdk-go-v2 client, that CreateArtifactInput.MetadataProperties — absent
// from this wire struct entirely before this pass (types/types.go:13617) —
// is not just parsed but reaches DescribeArtifact's response.
func TestHandler_Artifact_MetadataProperties_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	created, err := client.CreateArtifact(t.Context(), &sagemakersdk.CreateArtifactInput{
		ArtifactType: aws.String("Model"),
		Source:       &smtypes.ArtifactSource{SourceUri: aws.String("s3://bucket/model")},
		MetadataProperties: &smtypes.MetadataProperties{
			CommitId:    aws.String("abc123"),
			GeneratedBy: aws.String("pipeline-x"),
			ProjectId:   aws.String("proj-1"),
			Repository:  aws.String("git@example.com:org/repo"),
		},
	})
	require.NoError(t, err)

	desc, err := client.DescribeArtifact(t.Context(), &sagemakersdk.DescribeArtifactInput{
		ArtifactArn: created.ArtifactArn,
	})
	require.NoError(t, err)
	require.NotNil(t, desc.MetadataProperties)
	assert.Equal(t, "abc123", aws.ToString(desc.MetadataProperties.CommitId))
	assert.Equal(t, "pipeline-x", aws.ToString(desc.MetadataProperties.GeneratedBy))
	assert.Equal(t, "proj-1", aws.ToString(desc.MetadataProperties.ProjectId))
	assert.Equal(t, "git@example.com:org/repo", aws.ToString(desc.MetadataProperties.Repository))
}

// TestHandler_DeleteArtifact_BySource proves DeleteArtifactInput.Source —
// the real alternative identity to ArtifactArn per
// docs.aws.amazon.com/sagemaker/latest/APIReference/API_DeleteArtifact.html
// ("Either ArtifactArn or Source must be specified"), previously absent from
// this wire struct entirely — actually deletes through the real SDK client.
func TestHandler_DeleteArtifact_BySource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	created, err := client.CreateArtifact(t.Context(), &sagemakersdk.CreateArtifactInput{
		ArtifactType: aws.String("Model"),
		Source:       &smtypes.ArtifactSource{SourceUri: aws.String("s3://bucket/by-source")},
	})
	require.NoError(t, err)

	deleted, err := client.DeleteArtifact(t.Context(), &sagemakersdk.DeleteArtifactInput{
		Source: &smtypes.ArtifactSource{SourceUri: aws.String("s3://bucket/by-source")},
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(created.ArtifactArn), aws.ToString(deleted.ArtifactArn))

	_, err = client.DescribeArtifact(t.Context(), &sagemakersdk.DescribeArtifactInput{
		ArtifactArn: created.ArtifactArn,
	})
	require.Error(t, err)
}

// TestHandler_ListArtifacts_CreatedWindowAndSort proves ListArtifactsInput's
// CreatedAfter/CreatedBefore/SortBy/SortOrder — previously absent from this
// wire struct entirely — narrow and order the real result set, through the
// real SDK client.
func TestHandler_ListArtifacts_CreatedWindowAndSort(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	older, err := client.CreateArtifact(t.Context(), &sagemakersdk.CreateArtifactInput{
		ArtifactName: aws.String("older"), ArtifactType: aws.String("Model"),
		Source: &smtypes.ArtifactSource{SourceUri: aws.String("s3://bucket/older")},
	})
	require.NoError(t, err)

	newer, err := client.CreateArtifact(t.Context(), &sagemakersdk.CreateArtifactInput{
		ArtifactName: aws.String("newer"), ArtifactType: aws.String("Model"),
		Source: &smtypes.ArtifactSource{SourceUri: aws.String("s3://bucket/newer")},
	})
	require.NoError(t, err)

	future := time.Now().Add(365 * 24 * time.Hour)
	past := time.Now().Add(-365 * 24 * time.Hour)

	out, err := client.ListArtifacts(t.Context(), &sagemakersdk.ListArtifactsInput{
		CreatedAfter:  &past,
		CreatedBefore: &future,
		SortOrder:     smtypes.SortOrderAscending,
	})
	require.NoError(t, err)
	require.Len(t, out.ArtifactSummaries, 2)
	assert.Equal(t, aws.ToString(older.ArtifactArn), aws.ToString(out.ArtifactSummaries[0].ArtifactArn))
	assert.Equal(t, aws.ToString(newer.ArtifactArn), aws.ToString(out.ArtifactSummaries[1].ArtifactArn))

	excluded, err := client.ListArtifacts(t.Context(), &sagemakersdk.ListArtifactsInput{
		CreatedAfter: &future,
	})
	require.NoError(t, err)
	assert.Empty(t, excluded.ArtifactSummaries)
}

// TestHandler_ListArtifacts_MaxResults proves MaxResults/NextToken —
// previously absent — actually cap and page the real result set.
func TestHandler_ListArtifacts_MaxResults(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	for _, name := range []string{"a1", "a2"} {
		_, err := client.CreateArtifact(t.Context(), &sagemakersdk.CreateArtifactInput{
			ArtifactName: aws.String(name), ArtifactType: aws.String("Model"),
			Source: &smtypes.ArtifactSource{SourceUri: aws.String("s3://bucket/" + name)},
		})
		require.NoError(t, err)
	}

	page1, err := client.ListArtifacts(t.Context(), &sagemakersdk.ListArtifactsInput{MaxResults: aws.Int32(1)})
	require.NoError(t, err)
	require.Len(t, page1.ArtifactSummaries, 1)
	require.NotNil(t, page1.NextToken)

	page2, err := client.ListArtifacts(t.Context(), &sagemakersdk.ListArtifactsInput{
		MaxResults: aws.Int32(1),
		NextToken:  page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.ArtifactSummaries, 1)
	assert.NotEqual(t,
		aws.ToString(page1.ArtifactSummaries[0].ArtifactArn),
		aws.ToString(page2.ArtifactSummaries[0].ArtifactArn),
	)
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

// TestHandler_ListContexts_SortByNameAndMaxResults proves ListContextsInput's
// SortBy=Name and MaxResults/NextToken — previously absent from this wire
// struct entirely — actually order and page the real result set, through the
// real SDK client.
func TestHandler_ListContexts_SortByNameAndMaxResults(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	for _, name := range []string{"zeta-context", "alpha-context"} {
		_, err := client.CreateContext(t.Context(), &sagemakersdk.CreateContextInput{
			ContextName: aws.String(name),
			ContextType: aws.String("Endpoint"),
			Source: &smtypes.ContextSource{
				SourceUri: aws.String("arn:aws:sagemaker:us-east-1:0:endpoint/" + name),
			},
		})
		require.NoError(t, err)
	}

	out, err := client.ListContexts(t.Context(), &sagemakersdk.ListContextsInput{
		SortBy:    smtypes.SortContextsByName,
		SortOrder: smtypes.SortOrderAscending,
	})
	require.NoError(t, err)
	require.Len(t, out.ContextSummaries, 2)
	assert.Equal(t, "alpha-context", aws.ToString(out.ContextSummaries[0].ContextName))
	assert.Equal(t, "zeta-context", aws.ToString(out.ContextSummaries[1].ContextName))

	page1, err := client.ListContexts(t.Context(), &sagemakersdk.ListContextsInput{
		SortBy: smtypes.SortContextsByName, SortOrder: smtypes.SortOrderAscending, MaxResults: aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, page1.ContextSummaries, 1)
	require.NotNil(t, page1.NextToken)
	assert.Equal(t, "alpha-context", aws.ToString(page1.ContextSummaries[0].ContextName))
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

// TestHandler_Action_MetadataProperties_RoundTrip proves, through the real
// SDK client, that CreateActionInput.MetadataProperties — the same gap
// disclosed for CreateArtifact in parity-5, fixed alongside it here — reaches
// DescribeAction's response.
func TestHandler_Action_MetadataProperties_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateAction(t.Context(), &sagemakersdk.CreateActionInput{
		ActionName: aws.String("metadata-action"),
		ActionType: aws.String("ModelDeployment"),
		Source:     &smtypes.ActionSource{SourceUri: aws.String("arn:aws:sagemaker:us-east-1:0:endpoint/e")},
		MetadataProperties: &smtypes.MetadataProperties{
			CommitId:   aws.String("def456"),
			ProjectId:  aws.String("proj-2"),
			Repository: aws.String("git@example.com:org/other"),
		},
	})
	require.NoError(t, err)

	desc, err := client.DescribeAction(t.Context(), &sagemakersdk.DescribeActionInput{
		ActionName: aws.String("metadata-action"),
	})
	require.NoError(t, err)
	require.NotNil(t, desc.MetadataProperties)
	assert.Equal(t, "def456", aws.ToString(desc.MetadataProperties.CommitId))
	assert.Equal(t, "proj-2", aws.ToString(desc.MetadataProperties.ProjectId))
	assert.Equal(t, "git@example.com:org/other", aws.ToString(desc.MetadataProperties.Repository))
}

// TestHandler_ListActions_CreatedWindow proves ListActionsInput's
// CreatedAfter/CreatedBefore — previously absent from this wire struct
// entirely — narrow the real result set, through the real SDK client.
func TestHandler_ListActions_CreatedWindow(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateAction(t.Context(), &sagemakersdk.CreateActionInput{
		ActionName: aws.String("windowed-action"),
		ActionType: aws.String("ModelDeployment"),
		Source:     &smtypes.ActionSource{SourceUri: aws.String("arn:aws:sagemaker:us-east-1:0:endpoint/e")},
	})
	require.NoError(t, err)

	future := time.Now().Add(365 * 24 * time.Hour)

	included, err := client.ListActions(t.Context(), &sagemakersdk.ListActionsInput{CreatedBefore: &future})
	require.NoError(t, err)
	assert.Len(t, included.ActionSummaries, 1)

	excluded, err := client.ListActions(t.Context(), &sagemakersdk.ListActionsInput{CreatedAfter: &future})
	require.NoError(t, err)
	assert.Empty(t, excluded.ActionSummaries)
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
// ListAssociations — filter/sort/pagination request members
// ---------------------------------------------------------------------------

// setupListAssociationsFixture creates two associations sharing one
// destination but differing in source entity type and AssociationType, so
// SourceType/DestinationType/AssociationType filters each have a true and a
// false case within the same result set.
func setupListAssociationsFixture(t *testing.T, h *sagemaker.Handler) (string, string) {
	t.Helper()

	ctxArn := createTestContext(t, h, "src-context")
	actionArn := createTestAction(t, h, "src-action")
	artifactArn := createTestArtifact(t, h, "dst-artifact")

	doSageMakerRequest(t, h, "AddAssociation", map[string]any{
		"SourceArn":       ctxArn,
		"DestinationArn":  artifactArn,
		"AssociationType": "ContributedTo",
	})
	doSageMakerRequest(t, h, "AddAssociation", map[string]any{
		"SourceArn":       actionArn,
		"DestinationArn":  artifactArn,
		"AssociationType": "Produced",
	})

	return ctxArn, actionArn
}

func listAssociationSourceArns(t *testing.T, body []byte) []string {
	t.Helper()

	var out map[string]any
	require.NoError(t, json.Unmarshal(body, &out))

	summaries, _ := out["AssociationSummaries"].([]any)
	arns := make([]string, 0, len(summaries))

	for _, s := range summaries {
		arns = append(arns, s.(map[string]any)["SourceArn"].(string))
	}

	return arns
}

func TestHandler_ListAssociations_Filters(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ctxArn, actionArn := setupListAssociationsFixture(t, h)

	tests := []struct {
		name    string
		request map[string]any
		want    []string
	}{
		{
			name:    "association type narrows to one",
			request: map[string]any{"AssociationType": "Produced"},
			want:    []string{actionArn},
		},
		{
			name:    "source type narrows to context source",
			request: map[string]any{"SourceType": "Experiment"},
			want:    []string{ctxArn},
		},
		{
			name:    "source type narrows to action source",
			request: map[string]any{"SourceType": "ModelDeployment"},
			want:    []string{actionArn},
		},
		{
			name:    "destination type matches both",
			request: map[string]any{"DestinationType": "DataSet"},
			want:    []string{ctxArn, actionArn},
		},
		{
			name:    "destination type excludes all on mismatch",
			request: map[string]any{"DestinationType": "Endpoint"},
			want:    []string{},
		},
		{
			name:    "created after far future excludes all",
			request: map[string]any{"CreatedAfter": 32503680000.0}, // 3000-01-01
			want:    []string{},
		},
		{
			name:    "created before far past excludes all",
			request: map[string]any{"CreatedBefore": 0.0}, // 1970-01-01
			want:    []string{},
		},
		{
			name:    "created window spanning now matches both",
			request: map[string]any{"CreatedAfter": 0.0, "CreatedBefore": 32503680000.0},
			want:    []string{ctxArn, actionArn},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doSageMakerRequest(t, h, "ListAssociations", tt.request)
			require.Equal(t, http.StatusOK, rec.Code)

			got := listAssociationSourceArns(t, rec.Body.Bytes())
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}

func TestHandler_ListAssociations_Sort(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ctxArn, actionArn := setupListAssociationsFixture(t, h)

	wantAscending := []string{ctxArn, actionArn}
	if actionArn < ctxArn {
		wantAscending = []string{actionArn, ctxArn}
	}

	wantDescending := []string{wantAscending[1], wantAscending[0]}

	tests := []struct {
		name    string
		request map[string]any
		want    []string
	}{
		{
			name:    "sort by source arn ascending",
			request: map[string]any{"SortBy": "SourceArn", "SortOrder": "Ascending"},
			want:    wantAscending,
		},
		{
			name:    "sort by source arn descending",
			request: map[string]any{"SortBy": "SourceArn", "SortOrder": "Descending"},
			want:    wantDescending,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doSageMakerRequest(t, h, "ListAssociations", tt.request)
			require.Equal(t, http.StatusOK, rec.Code)

			got := listAssociationSourceArns(t, rec.Body.Bytes())
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestHandler_ListAssociations_MaxResults(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupListAssociationsFixture(t, h)

	rec := doSageMakerRequest(t, h, "ListAssociations", map[string]any{"MaxResults": 1})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	summaries, _ := out["AssociationSummaries"].([]any)
	require.Len(t, summaries, 1, "MaxResults=1 should return exactly one association out of two")

	nextToken, _ := out["NextToken"].(string)
	require.NotEmpty(t, nextToken, "a truncated page must carry a NextToken")

	rec2 := doSageMakerRequest(t, h, "ListAssociations", map[string]any{
		"MaxResults": 1,
		"NextToken":  nextToken,
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))
	summaries2, _ := out2["AssociationSummaries"].([]any)
	require.Len(t, summaries2, 1, "second page should return the remaining association")

	assert.NotEqual(t, summaries[0], summaries2[0], "the two pages should not repeat the same association")
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

// TestHandler_QueryLineage_FiltersLineageTypesAndProperties proves
// QueryLineageInput.Filters — previously absent from this wire struct
// entirely — actually narrows the real traversal's result vertices, through
// the real SDK client.
func TestHandler_QueryLineage_FiltersLineageTypesAndProperties(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	ctxOut, err := client.CreateContext(t.Context(), &sagemakersdk.CreateContextInput{
		ContextName: aws.String("filter-train-job"),
		ContextType: aws.String("Experiment"),
		Source:      &smtypes.ContextSource{SourceUri: aws.String("arn:aws:sagemaker:us-east-1:0:experiment/e")},
	})
	require.NoError(t, err)

	actionOut, err := client.CreateAction(t.Context(), &sagemakersdk.CreateActionInput{
		ActionName: aws.String("filter-deploy"),
		ActionType: aws.String("ModelDeployment"),
		Source:     &smtypes.ActionSource{SourceUri: aws.String("arn:aws:sagemaker:us-east-1:0:endpoint/e")},
		Properties: map[string]string{"team": "ml-platform"},
	})
	require.NoError(t, err)

	artifactOut, err := client.CreateArtifact(t.Context(), &sagemakersdk.CreateArtifactInput{
		ArtifactType: aws.String("Model"),
		Source:       &smtypes.ArtifactSource{SourceUri: aws.String("s3://bucket/filter-model")},
	})
	require.NoError(t, err)

	_, err = client.AddAssociation(t.Context(), &sagemakersdk.AddAssociationInput{
		SourceArn: ctxOut.ContextArn, DestinationArn: actionOut.ActionArn,
		AssociationType: smtypes.AssociationEdgeTypeContributedTo,
	})
	require.NoError(t, err)

	_, err = client.AddAssociation(t.Context(), &sagemakersdk.AddAssociationInput{
		SourceArn: actionOut.ActionArn, DestinationArn: artifactOut.ArtifactArn,
		AssociationType: smtypes.AssociationEdgeTypeProduced,
	})
	require.NoError(t, err)

	byType, err := client.QueryLineage(t.Context(), &sagemakersdk.QueryLineageInput{
		StartArns: []string{aws.ToString(ctxOut.ContextArn)},
		Filters:   &smtypes.QueryFilters{LineageTypes: []smtypes.LineageType{smtypes.LineageTypeArtifact}},
	})
	require.NoError(t, err)
	require.Len(t, byType.Vertices, 1)
	assert.Equal(t, aws.ToString(artifactOut.ArtifactArn), aws.ToString(byType.Vertices[0].Arn))

	byProps, err := client.QueryLineage(t.Context(), &sagemakersdk.QueryLineageInput{
		StartArns: []string{aws.ToString(ctxOut.ContextArn)},
		Filters:   &smtypes.QueryFilters{Properties: map[string]string{"team": "ml-platform"}},
	})
	require.NoError(t, err)
	require.Len(t, byProps.Vertices, 1)
	assert.Equal(t, aws.ToString(actionOut.ActionArn), aws.ToString(byProps.Vertices[0].Arn))
}

// TestHandler_QueryLineage_MaxResults proves QueryLineageInput.MaxResults/
// NextToken — previously absent — actually page the real vertex result set.
func TestHandler_QueryLineage_MaxResults(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	a1, err := client.CreateArtifact(t.Context(), &sagemakersdk.CreateArtifactInput{
		ArtifactType: aws.String("DataSet"),
		Source:       &smtypes.ArtifactSource{SourceUri: aws.String("s3://bucket/maxres-a1")},
	})
	require.NoError(t, err)

	a2, err := client.CreateArtifact(t.Context(), &sagemakersdk.CreateArtifactInput{
		ArtifactType: aws.String("DataSet"),
		Source:       &smtypes.ArtifactSource{SourceUri: aws.String("s3://bucket/maxres-a2")},
	})
	require.NoError(t, err)

	_, err = client.AddAssociation(t.Context(), &sagemakersdk.AddAssociationInput{
		SourceArn: a1.ArtifactArn, DestinationArn: a2.ArtifactArn,
		AssociationType: smtypes.AssociationEdgeTypeContributedTo,
	})
	require.NoError(t, err)

	page1, err := client.QueryLineage(t.Context(), &sagemakersdk.QueryLineageInput{
		StartArns:  []string{aws.ToString(a1.ArtifactArn)},
		MaxResults: aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, page1.Vertices, 1)
	require.NotNil(t, page1.NextToken)

	page2, err := client.QueryLineage(t.Context(), &sagemakersdk.QueryLineageInput{
		StartArns:  []string{aws.ToString(a1.ArtifactArn)},
		MaxResults: aws.Int32(1),
		NextToken:  page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.Vertices, 1)
	assert.NotEqual(t, aws.ToString(page1.Vertices[0].Arn), aws.ToString(page2.Vertices[0].Arn))
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

// TestHandler_ListLineageGroups_CreatedWindow proves ListLineageGroupsInput's
// CreatedAfter/CreatedBefore — previously absent from this wire struct
// entirely — are real filters even though there is only ever one lineage
// group: a window that excludes it must actually return an empty list, not
// silently ignore the filter, through the real SDK client.
func TestHandler_ListLineageGroups_CreatedWindow(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	future := time.Now().Add(365 * 24 * time.Hour)

	excluded, err := client.ListLineageGroups(t.Context(), &sagemakersdk.ListLineageGroupsInput{
		CreatedAfter: &future,
	})
	require.NoError(t, err)
	assert.Empty(t, excluded.LineageGroupSummaries)

	included, err := client.ListLineageGroups(t.Context(), &sagemakersdk.ListLineageGroupsInput{
		CreatedBefore: &future,
	})
	require.NoError(t, err)
	assert.Len(t, included.LineageGroupSummaries, 1)
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

func TestAddActionInternal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		actionName string
		actionType string
	}{
		{
			name:       "creates action with correct fields",
			actionName: "my-action",
			actionType: "ModelDeployment",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sagemaker.NewInMemoryBackend("000000000000", "us-east-1")
			a := b.AddActionInternal(context.Background(), tt.actionName, tt.actionType)

			require.NotNil(t, a)
			assert.Equal(t, tt.actionName, a.ActionName)
			assert.Equal(t, tt.actionType, a.ActionType)
			assert.Contains(t, a.ActionArn, "arn:aws:sagemaker")
			assert.Equal(t, 1, sagemaker.ActionCount(b))
		})
	}
}

func TestCreateAction_TagsPresent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
		wantTags bool
	}{
		{
			name: "create action with tags, tags survive",
			body: map[string]any{
				"ActionName": "my-action",
				"ActionType": "ModelDeployment",
				"Source": map[string]any{
					"SourceUri": "s3://bucket/key",
				},
				"Tags": []map[string]string{{"Key": "team", "Value": "ml"}},
			},
			wantCode: http.StatusOK,
			wantTags: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doSageMakerRequest(t, h, "CreateAction", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantTags {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				arnStr := resp["ActionArn"]
				require.NotEmpty(t, arnStr)

				rec2 := doSageMakerRequest(t, h, "ListTags", map[string]any{"ResourceArn": arnStr})
				require.Equal(t, http.StatusOK, rec2.Code)

				var tagsResp map[string]any
				require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &tagsResp))

				tags := tagsResp["Tags"].([]any)
				require.Len(t, tags, 1)
			}
		})
	}
}

func TestHandler_AddAssociation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
		wantARN  bool
	}{
		{
			name: "success with association type",
			body: map[string]any{
				"SourceArn":       "arn:aws:sagemaker:us-east-1:000000000000:experiment-trial/trial-1",
				"DestinationArn":  "arn:aws:sagemaker:us-east-1:000000000000:artifact/artifact-1",
				"AssociationType": "ContributedTo",
			},
			wantCode: http.StatusOK,
			wantARN:  true,
		},
		{
			name: "success without association type",
			body: map[string]any{
				"SourceArn":      "arn:aws:sagemaker:us-east-1:000000000000:experiment/exp-1",
				"DestinationArn": "arn:aws:sagemaker:us-east-1:000000000000:artifact/artifact-2",
			},
			wantCode: http.StatusOK,
			wantARN:  true,
		},
		{
			name: "missing SourceArn",
			body: map[string]any{
				"DestinationArn": "arn:aws:sagemaker:us-east-1:000000000000:artifact/x",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing DestinationArn",
			body: map[string]any{
				"SourceArn": "arn:aws:sagemaker:us-east-1:000000000000:trial/x",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "invalid json",
			body:     nil,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var body map[string]any
			if tt.body != nil {
				body = tt.body
			}

			rec := doSageMakerRequest(t, h, "AddAssociation", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantARN {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp["SourceArn"], "arn:aws:sagemaker")
				assert.Contains(t, resp["DestinationArn"], "arn:aws:sagemaker")
			}
		})
	}
}

func TestHandler_AddAssociation_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"SourceArn":      "arn:aws:sagemaker:us-east-1:000000000000:trial/t1",
		"DestinationArn": "arn:aws:sagemaker:us-east-1:000000000000:artifact/a1",
	}

	rec := doSageMakerRequest(t, h, "AddAssociation", body)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doSageMakerRequest(t, h, "AddAssociation", body)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

// TestHandler_AddAssociation_TagsNotOnWire proves Tags cannot be set via
// AddAssociation: AddAssociationInput has no Tags member at all
// (api_op_AddAssociation.go), so no real client can ever send it. A prior
// version of this handler accepted and applied it anyway (a fabricated
// field, not a real wire member).
func TestHandler_AddAssociation_TagsNotOnWire(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	sourceArn := "arn:aws:sagemaker:us-east-1:000000000000:trial/t1"
	destinationArn := "arn:aws:sagemaker:us-east-1:000000000000:artifact/a1"

	rec := doSageMakerRequest(t, h, "AddAssociation", map[string]any{
		"SourceArn":      sourceArn,
		"DestinationArn": destinationArn,
		"Tags":           []map[string]string{{"Key": "env", "Value": "prod"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	count := sagemaker.AssociationTagCount(h.Backend, "us-east-1", sourceArn, destinationArn)
	assert.Zero(t, count, "Tags is not a real AddAssociationInput field and must not be applied")
}

func TestHandler_AssociateTrialComponent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
		wantArns bool
	}{
		{
			name: "success",
			body: map[string]any{
				"TrialName":          "my-trial",
				"TrialComponentName": "my-component",
			},
			wantCode: http.StatusOK,
			wantArns: true,
		},
		{
			name:     "missing TrialName",
			body:     map[string]any{"TrialComponentName": "my-component"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing TrialComponentName",
			body:     map[string]any{"TrialName": "my-trial"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "invalid json",
			body:     nil,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var body map[string]any
			if tt.body != nil {
				body = tt.body
			}

			rec := doSageMakerRequest(t, h, "AssociateTrialComponent", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantArns {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp["TrialArn"], "arn:aws:sagemaker")
				assert.Contains(t, resp["TrialComponentArn"], "arn:aws:sagemaker")
			}
		})
	}
}

func TestHandler_AssociateTrialComponent_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"TrialName":          "trial-x",
		"TrialComponentName": "comp-x",
	}

	rec := doSageMakerRequest(t, h, "AssociateTrialComponent", body)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doSageMakerRequest(t, h, "AssociateTrialComponent", body)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}

func TestHandler_CreateAction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
		wantARN  bool
	}{
		{
			name: "success with all fields",
			body: map[string]any{
				"ActionName": "my-action",
				"ActionType": "ModelDeployment",
				"Source": map[string]any{
					"SourceUri":  "s3://my-bucket/my-model",
					"SourceType": "S3URIPrefix",
				},
				"Description": "test action",
				"Status":      "Completed",
				"Properties":  map[string]string{"key": "value"},
				"Tags":        []map[string]string{{"Key": "env", "Value": "test"}},
			},
			wantCode: http.StatusOK,
			wantARN:  true,
		},
		{
			name: "success minimal",
			body: map[string]any{
				"ActionName": "minimal-action",
				"ActionType": "ModelDeployment",
			},
			wantCode: http.StatusOK,
			wantARN:  true,
		},
		{
			name:     "missing ActionName",
			body:     map[string]any{"ActionType": "ModelDeployment"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "invalid json",
			body:     nil,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var body map[string]any
			if tt.body != nil {
				body = tt.body
			}

			rec := doSageMakerRequest(t, h, "CreateAction", body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantARN {
				var resp map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Contains(t, resp["ActionArn"], "arn:aws:sagemaker")
				assert.Contains(t, resp["ActionArn"], "action/")
			}
		})
	}
}

func TestHandler_CreateAction_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"ActionName": "dup-action",
		"ActionType": "ModelDeployment",
	}

	rec := doSageMakerRequest(t, h, "CreateAction", body)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doSageMakerRequest(t, h, "CreateAction", body)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
}
