package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateModelCard(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateModelCard", map[string]any{
		"ModelCardName":   "my-card",
		"Content":         "{}",
		"ModelCardStatus": "Draft",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["ModelCardArn"], "my-card")
}

func TestHandler_CreateModelCard_StatusRequired(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateModelCard", map[string]any{
		"ModelCardName": "no-status-card",
		"Content":       "{}",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DescribeModelCard(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelCard", map[string]any{"ModelCardName": "card-1", "ModelCardStatus": "Draft"})
	rec := doSageMakerRequest(t, h, "DescribeModelCard", map[string]any{"ModelCardName": "card-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "card-1", resp["ModelCardName"])
}

func TestHandler_UpdateModelCard(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelCard", map[string]any{"ModelCardName": "card-upd", "ModelCardStatus": "Draft"})
	rec := doSageMakerRequest(t, h, "UpdateModelCard", map[string]any{
		"ModelCardName": "card-upd",
		"Content":       "{\"updated\": true}",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify version incremented
	rec = doSageMakerRequest(t, h, "DescribeModelCard", map[string]any{"ModelCardName": "card-upd"})
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.InDelta(t, float64(2), resp["ModelCardVersion"], 0)
}

func TestHandler_DeleteModelCard(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelCard", map[string]any{"ModelCardName": "card-del", "ModelCardStatus": "Draft"})
	rec := doSageMakerRequest(t, h, "DeleteModelCard", map[string]any{"ModelCardName": "card-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeModelCard", map[string]any{"ModelCardName": "card-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// OptimizationJob
// ---------------------------------------------------------------------------

func TestHandler_ListModelCards(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Empty initially
	rec := doSageMakerRequest(t, h, "ListModelCards", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["ModelCardSummaries"])

	// Create one
	doSageMakerRequest(t, h, "CreateModelCard", map[string]any{
		"ModelCardName":   "my-card",
		"ModelCardStatus": "Draft",
	})

	rec = doSageMakerRequest(t, h, "ListModelCards", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	summaries := resp["ModelCardSummaries"].([]any)
	assert.Len(t, summaries, 1)

	// ModelCardSummary (types/types.go) has no ModelCardVersion member — a
	// previous version of this test asserted one anyway, ratifying a
	// fabricated response field with no real wire counterpart.
	s := summaries[0].(map[string]any)
	assert.Equal(t, "my-card", s["ModelCardName"])
	assert.Equal(t, "Draft", s["ModelCardStatus"])
	assert.NotContains(t, s, "ModelCardVersion")
}

func TestHandler_ListModelCardVersions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelCard", map[string]any{
		"ModelCardName":   "my-card",
		"ModelCardStatus": "Draft",
	})

	rec := doSageMakerRequest(t, h, "ListModelCardVersions", map[string]any{
		"ModelCardName": "my-card",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	versions := resp["ModelCardVersionSummaryList"].([]any)
	assert.Len(t, versions, 1)

	v := versions[0].(map[string]any)
	assert.Equal(t, "my-card", v["ModelCardName"])
	assert.EqualValues(t, 1, v["ModelCardVersion"])
}

func TestHandler_ListModelCardVersions_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListModelCardVersions", map[string]any{
		"ModelCardName": "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListModelCardExportJobs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelCard", map[string]any{
		"ModelCardName":   "my-card",
		"ModelCardStatus": "Draft",
	})

	rec := doSageMakerRequest(t, h, "ListModelCardExportJobs", map[string]any{
		"ModelCardName": "my-card",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	jobs := resp["ModelCardExportJobSummaries"].([]any)
	assert.Empty(t, jobs)
}

// TestHandler_CreateModelCard_Status_SecurityConfig_RealClient asserts
// ModelCardStatus (required, previously never read at all — every real
// client's card was silently forced to "Draft") and SecurityConfig.KmsKeyId
// (previously absent) round-trip through Describe.
func TestHandler_CreateModelCard_Status_SecurityConfig_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateModelCard(t.Context(), &sagemakersdk.CreateModelCardInput{
		ModelCardName:   aws.String("approved-card"),
		Content:         aws.String("{}"),
		ModelCardStatus: smtypes.ModelCardStatusApproved,
		SecurityConfig:  &smtypes.ModelCardSecurityConfig{KmsKeyId: aws.String("alias/my-key")},
	})
	require.NoError(t, err)

	out, err := client.DescribeModelCard(t.Context(), &sagemakersdk.DescribeModelCardInput{
		ModelCardName: aws.String("approved-card"),
	})
	require.NoError(t, err)
	assert.Equal(t, smtypes.ModelCardStatusApproved, out.ModelCardStatus)
	require.NotNil(t, out.SecurityConfig)
	assert.Equal(t, "alias/my-key", aws.ToString(out.SecurityConfig.KmsKeyId))
}

// TestHandler_UpdateModelCard_StatusOnly_RealClient asserts a status-only
// update changes ModelCardStatus without bumping ModelCardVersion (real
// model card versioning tracks content revisions, not approval-workflow
// transitions), and that supplying both Content and ModelCardStatus in one
// call is rejected per UpdateModelCard's own doc comment.
func TestHandler_UpdateModelCard_StatusOnly_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateModelCard(t.Context(), &sagemakersdk.CreateModelCardInput{
		ModelCardName:   aws.String("status-card"),
		Content:         aws.String("{}"),
		ModelCardStatus: smtypes.ModelCardStatusDraft,
	})
	require.NoError(t, err)

	_, err = client.UpdateModelCard(t.Context(), &sagemakersdk.UpdateModelCardInput{
		ModelCardName:   aws.String("status-card"),
		ModelCardStatus: smtypes.ModelCardStatusPendingreview,
	})
	require.NoError(t, err)

	out, err := client.DescribeModelCard(t.Context(), &sagemakersdk.DescribeModelCardInput{
		ModelCardName: aws.String("status-card"),
	})
	require.NoError(t, err)
	assert.Equal(t, smtypes.ModelCardStatusPendingreview, out.ModelCardStatus)
	assert.EqualValues(t, 1, aws.ToInt32(out.ModelCardVersion))

	_, err = client.UpdateModelCard(t.Context(), &sagemakersdk.UpdateModelCardInput{
		ModelCardName:   aws.String("status-card"),
		Content:         aws.String(`{"updated":true}`),
		ModelCardStatus: smtypes.ModelCardStatusArchived,
	})
	require.Error(t, err)
}

// TestHandler_DescribeModelCard_MetadataOnly_RealClient asserts
// IncludedData=MetadataOnly sanitizes Content down to the fixed JSON paths
// the real op documents, dropping everything else.
func TestHandler_DescribeModelCard_MetadataOnly_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	content := `{
		"model_overview": {"model_id": "m-1", "model_name": "My Model", "problem_type": "Regression"},
		"intended_uses": {"risk_rating": "Low", "purpose_of_model": "demo"},
		"model_package_details": {"model_package_group_name": "grp", "model_package_arn": "arn:pkg"},
		"training_details": {"objective_function": "secret"}
	}`

	_, err := client.CreateModelCard(t.Context(), &sagemakersdk.CreateModelCardInput{
		ModelCardName:   aws.String("metadata-card"),
		Content:         aws.String(content),
		ModelCardStatus: smtypes.ModelCardStatusDraft,
	})
	require.NoError(t, err)

	out, err := client.DescribeModelCard(t.Context(), &sagemakersdk.DescribeModelCardInput{
		ModelCardName: aws.String("metadata-card"),
		IncludedData:  smtypes.IncludedDataMetadataOnly,
	})
	require.NoError(t, err)

	assert.JSONEq(t, `{
		"model_overview": {"model_id": "m-1", "model_name": "My Model"},
		"intended_uses": {"risk_rating": "Low"},
		"model_package_details": {"model_package_group_name": "grp", "model_package_arn": "arn:pkg"}
	}`, aws.ToString(out.Content))

	full, err := client.DescribeModelCard(t.Context(), &sagemakersdk.DescribeModelCardInput{
		ModelCardName: aws.String("metadata-card"),
	})
	require.NoError(t, err)
	assert.JSONEq(t, content, aws.ToString(full.Content))
}

// TestHandler_DescribeModelCard_Version_RealClient asserts ModelCardVersion
// honors the current version and rejects any other, since this backend
// keeps no historical per-version snapshot.
func TestHandler_DescribeModelCard_Version_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateModelCard(t.Context(), &sagemakersdk.CreateModelCardInput{
		ModelCardName:   aws.String("versioned-card"),
		Content:         aws.String("{}"),
		ModelCardStatus: smtypes.ModelCardStatusDraft,
	})
	require.NoError(t, err)

	_, err = client.DescribeModelCard(t.Context(), &sagemakersdk.DescribeModelCardInput{
		ModelCardName:    aws.String("versioned-card"),
		ModelCardVersion: aws.Int32(1),
	})
	require.NoError(t, err)

	_, err = client.DescribeModelCard(t.Context(), &sagemakersdk.DescribeModelCardInput{
		ModelCardName:    aws.String("versioned-card"),
		ModelCardVersion: aws.Int32(2),
	})
	require.Error(t, err)
}

// TestHandler_ListModelCards_FilterSortPage_RealClient asserts
// ListModelCardsInput's NameContains/ModelCardStatus/SortBy/SortOrder --
// all absent before this pass -- now work.
func TestHandler_ListModelCards_FilterSortPage_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	cards := []struct {
		name   string
		status smtypes.ModelCardStatus
	}{
		{"alpha-card", smtypes.ModelCardStatusDraft},
		{"beta-card", smtypes.ModelCardStatusApproved},
		{"gamma-widget", smtypes.ModelCardStatusDraft},
	}
	for _, c := range cards {
		_, err := client.CreateModelCard(t.Context(), &sagemakersdk.CreateModelCardInput{
			ModelCardName:   aws.String(c.name),
			Content:         aws.String("{}"),
			ModelCardStatus: c.status,
		})
		require.NoError(t, err)
	}

	t.Run("name contains filters", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListModelCards(t.Context(), &sagemakersdk.ListModelCardsInput{
			NameContains: aws.String("card"),
		})
		require.NoError(t, err)
		assert.Len(t, out.ModelCardSummaries, 2)
	})

	t.Run("status filters", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListModelCards(t.Context(), &sagemakersdk.ListModelCardsInput{
			ModelCardStatus: smtypes.ModelCardStatusApproved,
		})
		require.NoError(t, err)
		require.Len(t, out.ModelCardSummaries, 1)
		assert.Equal(t, "beta-card", aws.ToString(out.ModelCardSummaries[0].ModelCardName))
	})

	t.Run("ascending sort by name", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListModelCards(t.Context(), &sagemakersdk.ListModelCardsInput{
			SortBy:    smtypes.ModelCardSortByName,
			SortOrder: smtypes.ModelCardSortOrderAscending,
		})
		require.NoError(t, err)
		require.Len(t, out.ModelCardSummaries, 3)
		assert.Equal(t, "alpha-card", aws.ToString(out.ModelCardSummaries[0].ModelCardName))
	})
}

// TestHandler_ListModelCardExportJobs_Version_RealClient asserts
// ListModelCardExportJobsInput's ModelCardVersion filter -- absent before
// this pass -- now works.
func TestHandler_ListModelCardExportJobs_Version_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateModelCard(t.Context(), &sagemakersdk.CreateModelCardInput{
		ModelCardName:   aws.String("export-card"),
		Content:         aws.String("{}"),
		ModelCardStatus: smtypes.ModelCardStatusDraft,
	})
	require.NoError(t, err)

	_, err = client.UpdateModelCard(t.Context(), &sagemakersdk.UpdateModelCardInput{
		ModelCardName: aws.String("export-card"),
		Content:       aws.String(`{"v":2}`),
	})
	require.NoError(t, err)

	jobNames := map[int32]string{1: "export-v1", 2: "export-v2"}
	for _, v := range []int32{1, 2} {
		_, jobErr := client.CreateModelCardExportJob(t.Context(), &sagemakersdk.CreateModelCardExportJobInput{
			ModelCardExportJobName: aws.String(jobNames[v]),
			ModelCardName:          aws.String("export-card"),
			ModelCardVersion:       aws.Int32(v),
			OutputConfig:           &smtypes.ModelCardExportOutputConfig{S3OutputPath: aws.String("s3://bucket/out")},
		})
		require.NoError(t, jobErr)
	}

	out, err := client.ListModelCardExportJobs(t.Context(), &sagemakersdk.ListModelCardExportJobsInput{
		ModelCardName:    aws.String("export-card"),
		ModelCardVersion: aws.Int32(2),
	})
	require.NoError(t, err)
	require.Len(t, out.ModelCardExportJobSummaries, 1)
	assert.EqualValues(t, 2, aws.ToInt32(out.ModelCardExportJobSummaries[0].ModelCardVersion))
}

// ---------------------------------------------------------------------------
// UpdateModelPackage tests
// ---------------------------------------------------------------------------
