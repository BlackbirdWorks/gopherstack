package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sagemakersdk "github.com/aws/aws-sdk-go-v2/service/sagemaker"
	smtypes "github.com/aws/aws-sdk-go-v2/service/sagemaker/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_TrialLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create experiment first.
	doSageMakerRequest(
		t,
		h,
		"CreateExperiment",
		map[string]any{"ExperimentName": "trial-experiment"},
	)

	// Create trial.
	recCreate := doSageMakerRequest(t, h, "CreateTrial", map[string]any{
		"TrialName":      "my-trial",
		"ExperimentName": "trial-experiment",
	})
	assert.Equal(t, http.StatusOK, recCreate.Code)

	// Describe trial.
	recDesc := doSageMakerRequest(t, h, "DescribeTrial", map[string]any{"TrialName": "my-trial"})
	assert.Equal(t, http.StatusOK, recDesc.Code)

	// List trials.
	recList := doSageMakerRequest(t, h, "ListTrials", map[string]any{
		"ExperimentName": "trial-experiment",
	})
	assert.Equal(t, http.StatusOK, recList.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listOut))
	assert.Len(t, listOut["TrialSummaries"].([]any), 1)

	// Delete trial.
	recDelete := doSageMakerRequest(t, h, "DeleteTrial", map[string]any{"TrialName": "my-trial"})
	assert.Equal(t, http.StatusOK, recDelete.Code)
}

func TestHandler_UpdateTrial(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateExperiment", map[string]any{"ExperimentName": "exp"})
	doSageMakerRequest(t, h, "CreateTrial", map[string]any{
		"TrialName":      "my-trial",
		"ExperimentName": "exp",
	})

	rec := doSageMakerRequest(t, h, "UpdateTrial", map[string]any{
		"TrialName":   "my-trial",
		"DisplayName": "Trial Display",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var updateResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
	assert.NotEmpty(t, updateResp["TrialArn"])

	// Describe returns updated DisplayName
	rec = doSageMakerRequest(t, h, "DescribeTrial", map[string]any{"TrialName": "my-trial"})
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "Trial Display", descResp["DisplayName"])
}

func TestHandler_CreateTrial_DisplayName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateExperiment", map[string]any{"ExperimentName": "exp-dn"})

	rec := doSageMakerRequest(t, h, "CreateTrial", map[string]any{
		"TrialName":      "trial-with-display",
		"ExperimentName": "exp-dn",
		"DisplayName":    "My Trial",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeTrial", map[string]any{"TrialName": "trial-with-display"})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "My Trial", descResp["DisplayName"])

	rec = doSageMakerRequest(t, h, "ListTrials", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	summaries := listResp["TrialSummaries"].([]any)
	require.Len(t, summaries, 1)
	assert.Equal(t, "My Trial", summaries[0].(map[string]any)["DisplayName"])
}

// TestHandler_CreateTrial_MetadataProperties_RealClient asserts
// CreateTrialInput.MetadataProperties -- previously entirely absent -- now
// round-trips through DescribeTrial.
func TestHandler_CreateTrial_MetadataProperties_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, err := client.CreateExperiment(t.Context(), &sagemakersdk.CreateExperimentInput{
		ExperimentName: aws.String("exp-metadata"),
	})
	require.NoError(t, err)

	_, err = client.CreateTrial(t.Context(), &sagemakersdk.CreateTrialInput{
		TrialName:      aws.String("trial-metadata"),
		ExperimentName: aws.String("exp-metadata"),
		MetadataProperties: &smtypes.MetadataProperties{
			CommitId:   aws.String("abc123"),
			Repository: aws.String("gopherstack"),
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeTrial(t.Context(), &sagemakersdk.DescribeTrialInput{
		TrialName: aws.String("trial-metadata"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.MetadataProperties)
	assert.Equal(t, "abc123", aws.ToString(out.MetadataProperties.CommitId))
	assert.Equal(t, "gopherstack", aws.ToString(out.MetadataProperties.Repository))
}

// TestHandler_ListTrials_FilterSortPage_RealClient asserts ListTrialsInput's
// ExperimentName/CreatedAfter/CreatedBefore/SortBy/SortOrder/MaxResults --
// previously only NextToken was decoded, so ExperimentName was silently
// ignored despite looking like a real filter in existing tests that never
// exercised a non-matching value.
func TestHandler_ListTrials_FilterSortPage_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSageMakerClient(t, h)

	_, setupErr := client.CreateExperiment(t.Context(), &sagemakersdk.CreateExperimentInput{
		ExperimentName: aws.String("exp-list-a"),
	})
	require.NoError(t, setupErr)
	_, setupErr = client.CreateExperiment(t.Context(), &sagemakersdk.CreateExperimentInput{
		ExperimentName: aws.String("exp-list-b"),
	})
	require.NoError(t, setupErr)

	names := []string{"alpha-trial", "beta-trial"}
	for _, n := range names {
		_, createErr := client.CreateTrial(t.Context(), &sagemakersdk.CreateTrialInput{
			TrialName:      aws.String(n),
			ExperimentName: aws.String("exp-list-a"),
		})
		require.NoError(t, createErr)
	}

	_, setupErr = client.CreateTrial(t.Context(), &sagemakersdk.CreateTrialInput{
		TrialName:      aws.String("other-experiment-trial"),
		ExperimentName: aws.String("exp-list-b"),
	})
	require.NoError(t, setupErr)

	t.Run("experiment name scopes the list", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListTrials(t.Context(), &sagemakersdk.ListTrialsInput{
			ExperimentName: aws.String("exp-list-a"),
		})
		require.NoError(t, err)
		require.Len(t, out.TrialSummaries, 2)
	})

	t.Run("ascending sort by name", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListTrials(t.Context(), &sagemakersdk.ListTrialsInput{
			ExperimentName: aws.String("exp-list-a"),
			SortBy:         smtypes.SortTrialsByName,
			SortOrder:      smtypes.SortOrderAscending,
		})
		require.NoError(t, err)
		require.Len(t, out.TrialSummaries, 2)
		assert.Equal(t, "alpha-trial", aws.ToString(out.TrialSummaries[0].TrialName))
		assert.Equal(t, "beta-trial", aws.ToString(out.TrialSummaries[1].TrialName))
	})

	t.Run("max results caps the page and returns a token", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListTrials(t.Context(), &sagemakersdk.ListTrialsInput{
			ExperimentName: aws.String("exp-list-a"),
			MaxResults:     aws.Int32(1),
			SortBy:         smtypes.SortTrialsByName,
			SortOrder:      smtypes.SortOrderAscending,
		})
		require.NoError(t, err)
		require.Len(t, out.TrialSummaries, 1)
		assert.Equal(t, "alpha-trial", aws.ToString(out.TrialSummaries[0].TrialName))
		assert.NotEmpty(t, aws.ToString(out.NextToken))
	})

	t.Run("created after future excludes", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListTrials(t.Context(), &sagemakersdk.ListTrialsInput{
			CreatedAfter: aws.Time(time.Now().Add(time.Hour)),
		})
		require.NoError(t, err)
		assert.Empty(t, out.TrialSummaries)
	})

	t.Run("created after past includes", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListTrials(t.Context(), &sagemakersdk.ListTrialsInput{
			CreatedAfter: aws.Time(time.Now().Add(-time.Hour)),
		})
		require.NoError(t, err)
		assert.Len(t, out.TrialSummaries, 3)
	})
}

// TestHandler_ListTrials_TrialComponentName asserts ListTrialsInput's
// TrialComponentName filter -- previously entirely absent -- narrows the
// result to trials associated with the named component via
// AssociateTrialComponent.
func TestHandler_ListTrials_TrialComponentName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateExperiment", map[string]any{"ExperimentName": "exp-tc-filter"})
	doSageMakerRequest(t, h, "CreateTrial", map[string]any{
		"TrialName": "trial-with-tc", "ExperimentName": "exp-tc-filter",
	})
	doSageMakerRequest(t, h, "CreateTrial", map[string]any{
		"TrialName": "trial-without-tc", "ExperimentName": "exp-tc-filter",
	})
	doSageMakerRequest(t, h, "CreateTrialComponent", map[string]any{"TrialComponentName": "tc-filter"})
	doSageMakerRequest(t, h, "AssociateTrialComponent", map[string]any{
		"TrialName": "trial-with-tc", "TrialComponentName": "tc-filter",
	})

	rec := doSageMakerRequest(t, h, "ListTrials", map[string]any{"TrialComponentName": "tc-filter"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	summaries := resp["TrialSummaries"].([]any)
	require.Len(t, summaries, 1)
	assert.Equal(t, "trial-with-tc", summaries[0].(map[string]any)["TrialName"])
}
