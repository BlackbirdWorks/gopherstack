package personalize_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/personalize"
)

// TestPersonalize_Create_ValidatesParentARN locks the true-parity fix that
// every Create* op which references a parent resource ARN now validates that
// the parent actually exists -- real AWS returns ResourceNotFoundException
// for a dangling reference (e.g. a solutionArn that was never created); this
// backend previously accepted it silently. One subtest per Create op that
// carries a foreign-key-shaped ARN field, each building its own fresh
// handler and only the *other*, unrelated prerequisite resources for real,
// leaving the one field under test dangling.
func TestPersonalize_Create_ValidatesParentARN(t *testing.T) {
	t.Parallel()

	danglingDatasetGroupArn := "arn:aws:personalize:us-east-1:000000000000:dataset-group/does-not-exist"
	danglingSchemaArn := "arn:aws:personalize:us-east-1:000000000000:schema/does-not-exist"
	danglingSolutionArn := "arn:aws:personalize:us-east-1:000000000000:solution/does-not-exist"
	danglingSolutionVersionArn := "arn:aws:personalize:us-east-1:000000000000:solution/does-not-exist/v1"
	danglingDatasetArn := "arn:aws:personalize:us-east-1:000000000000:dataset/does-not-exist"
	danglingRecipeArn := "arn:aws:personalize:::recipe/does-not-exist"

	tests := []struct {
		setup  func(t *testing.T, h *personalize.Handler) map[string]any
		name   string
		action string
	}{
		{
			name:   "CreateDataset_datasetGroupArn",
			action: "CreateDataset",
			setup: func(t *testing.T, h *personalize.Handler) map[string]any {
				t.Helper()
				schemaArn := personalizeCreateSchema(t, h, "sc")

				return map[string]any{
					"name": "ds", "datasetGroupArn": danglingDatasetGroupArn,
					"datasetType": "INTERACTIONS", "schemaArn": schemaArn,
				}
			},
		},
		{
			name:   "CreateDataset_schemaArn",
			action: "CreateDataset",
			setup: func(t *testing.T, h *personalize.Handler) map[string]any {
				t.Helper()
				dgArn := personalizeCreateDatasetGroup(t, h, "dg")

				return map[string]any{
					"name": "ds", "datasetGroupArn": dgArn,
					"datasetType": "INTERACTIONS", "schemaArn": danglingSchemaArn,
				}
			},
		},
		{
			name:   "CreateSolution_datasetGroupArn",
			action: "CreateSolution",
			setup: func(t *testing.T, _ *personalize.Handler) map[string]any {
				t.Helper()

				return map[string]any{
					"name": "sol", "datasetGroupArn": danglingDatasetGroupArn,
					"recipeArn": "arn:aws:personalize:::recipe/aws-user-personalization",
				}
			},
		},
		{
			name:   "CreateSolution_recipeArn",
			action: "CreateSolution",
			setup: func(t *testing.T, h *personalize.Handler) map[string]any {
				t.Helper()
				dgArn := personalizeCreateDatasetGroup(t, h, "dg")

				return map[string]any{
					"name": "sol", "datasetGroupArn": dgArn, "recipeArn": danglingRecipeArn,
				}
			},
		},
		{
			name:   "CreateSolutionVersion_solutionArn",
			action: "CreateSolutionVersion",
			setup: func(t *testing.T, _ *personalize.Handler) map[string]any {
				t.Helper()

				return map[string]any{"solutionArn": danglingSolutionArn}
			},
		},
		{
			name:   "CreateCampaign_solutionVersionArn",
			action: "CreateCampaign",
			setup: func(t *testing.T, _ *personalize.Handler) map[string]any {
				t.Helper()

				return map[string]any{"name": "camp", "solutionVersionArn": danglingSolutionVersionArn}
			},
		},
		{
			name:   "CreateEventTracker_datasetGroupArn",
			action: "CreateEventTracker",
			setup: func(t *testing.T, _ *personalize.Handler) map[string]any {
				t.Helper()

				return map[string]any{"name": "et", "datasetGroupArn": danglingDatasetGroupArn}
			},
		},
		{
			name:   "CreateFilter_datasetGroupArn",
			action: "CreateFilter",
			setup: func(t *testing.T, _ *personalize.Handler) map[string]any {
				t.Helper()

				return map[string]any{
					"name": "f", "datasetGroupArn": danglingDatasetGroupArn,
					"filterExpression": "INCLUDE ItemID WHERE Items.CATEGORY IN ($CATEGORIES)",
				}
			},
		},
		{
			name:   "CreateRecommender_datasetGroupArn",
			action: "CreateRecommender",
			setup: func(t *testing.T, _ *personalize.Handler) map[string]any {
				t.Helper()

				return map[string]any{
					"name": "rec", "datasetGroupArn": danglingDatasetGroupArn,
					"recipeArn": "arn:aws:personalize:::recipe/aws-user-personalization",
				}
			},
		},
		{
			name:   "CreateRecommender_recipeArn",
			action: "CreateRecommender",
			setup: func(t *testing.T, h *personalize.Handler) map[string]any {
				t.Helper()
				dgArn := personalizeCreateDatasetGroup(t, h, "dg")

				return map[string]any{"name": "rec", "datasetGroupArn": dgArn, "recipeArn": danglingRecipeArn}
			},
		},
		{
			name:   "CreateDatasetImportJob_datasetArn",
			action: "CreateDatasetImportJob",
			setup: func(t *testing.T, _ *personalize.Handler) map[string]any {
				t.Helper()

				return map[string]any{
					"jobName": "job", "datasetArn": danglingDatasetArn,
					"roleArn": "arn:aws:iam::000000000000:role/r",
					"dataSource": map[string]any{
						"dataLocation": "s3://bucket/data.csv",
					},
				}
			},
		},
		{
			name:   "CreateDatasetExportJob_datasetArn",
			action: "CreateDatasetExportJob",
			setup: func(t *testing.T, _ *personalize.Handler) map[string]any {
				t.Helper()

				return map[string]any{
					"jobName": "job", "datasetArn": danglingDatasetArn,
					"roleArn": "arn:aws:iam::000000000000:role/r",
				}
			},
		},
		{
			name:   "CreateBatchInferenceJob_solutionVersionArn",
			action: "CreateBatchInferenceJob",
			setup: func(t *testing.T, _ *personalize.Handler) map[string]any {
				t.Helper()

				return map[string]any{
					"jobName": "job", "solutionVersionArn": danglingSolutionVersionArn,
					"roleArn": "arn:aws:iam::000000000000:role/r",
				}
			},
		},
		{
			name:   "CreateBatchSegmentJob_solutionVersionArn",
			action: "CreateBatchSegmentJob",
			setup: func(t *testing.T, _ *personalize.Handler) map[string]any {
				t.Helper()

				return map[string]any{
					"jobName": "job", "solutionVersionArn": danglingSolutionVersionArn,
					"roleArn": "arn:aws:iam::000000000000:role/r",
				}
			},
		},
		{
			name:   "CreateDataDeletionJob_datasetGroupArn",
			action: "CreateDataDeletionJob",
			setup: func(t *testing.T, _ *personalize.Handler) map[string]any {
				t.Helper()

				return map[string]any{
					"jobName": "job", "datasetGroupArn": danglingDatasetGroupArn,
					"roleArn": "arn:aws:iam::000000000000:role/r",
				}
			},
		},
		{
			name:   "CreateMetricAttribution_datasetGroupArn",
			action: "CreateMetricAttribution",
			setup: func(t *testing.T, _ *personalize.Handler) map[string]any {
				t.Helper()

				return map[string]any{
					"name": "ma", "datasetGroupArn": danglingDatasetGroupArn,
					"metrics": []map[string]any{
						{"eventType": "click", "expression": "SUM(Items.PRICE)", "metricName": "m1"},
					},
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := personalizeHandler(t)
			body := tt.setup(t, h)

			rec := personalizeDo(t, h, tt.action, body)

			require.Equal(t, http.StatusBadRequest, rec.Code)
			m := personalizeUnmarshal(t, rec)
			assert.Equal(t, "ResourceNotFoundException", m["__type"])
		})
	}
}

// TestPersonalize_UpdateCampaign_ValidatesSolutionVersionArn locks that
// UpdateCampaign, like CreateCampaign, rejects a solutionVersionArn that
// does not resolve to a real solution version.
func TestPersonalize_UpdateCampaign_ValidatesSolutionVersionArn(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)
	personalizeCreateCampaign(t, h, "camp")

	rec := personalizeDo(t, h, "ListCampaigns", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	campaigns := personalizeUnmarshal(t, rec)["campaigns"].([]any)
	require.Len(t, campaigns, 1)
	campArn, _ := campaigns[0].(map[string]any)["campaignArn"].(string)
	require.NotEmpty(t, campArn)

	rec = personalizeDo(t, h, "UpdateCampaign", map[string]any{
		"campaignArn":        campArn,
		"solutionVersionArn": "arn:aws:personalize:us-east-1:000000000000:solution/does-not-exist/v1",
	})

	require.Equal(t, http.StatusBadRequest, rec.Code)
	m := personalizeUnmarshal(t, rec)
	assert.Equal(t, "ResourceNotFoundException", m["__type"])
}
