package personalize_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/personalize"
)

// TestPersonalize_ListOps_SummaryShape locks that every List op emits its
// real types.*Summary shape instead of reusing the corresponding Get op's
// full converter unscoped (gopherstack-sm02). These assertions read the raw
// JSON body via personalizeUnmarshal, not through an AWS SDK client -- the
// SDK deserializer silently drops keys it does not recognise, so a
// client-driven test cannot see these fields leak.
func TestPersonalize_ListOps_SummaryShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(t *testing.T, h *personalize.Handler) map[string]any
		name    string
		leaked  []string
		present []string
	}{
		{
			name: "solutions",
			present: []string{
				"solutionArn", "name", "recipeArn", "status", "creationDateTime", "lastUpdatedDateTime",
			},
			leaked: []string{
				"datasetGroupArn", "eventType", "performAutoML", "performHPO", "performAutoTraining",
				"performIncrementalUpdate", "solutionConfig", "autoMLResult", "latestSolutionUpdate",
			},
			setup: func(t *testing.T, h *personalize.Handler) map[string]any {
				t.Helper()

				personalizeCreateSolution(t, h, "list-leak-sol")

				return listSingle(t, h, "ListSolutions", "solutions")
			},
		},
		{
			name: "solutionVersions",
			present: []string{
				"solutionVersionArn", "status", "trainingMode", "trainingType",
				"creationDateTime", "lastUpdatedDateTime",
			},
			leaked: []string{
				"solutionArn", "datasetGroupArn", "recipeArn", "eventType", "performAutoML", "performHPO",
				"performIncrementalUpdate", "trainingHours", "solutionConfig",
			},
			setup: func(t *testing.T, h *personalize.Handler) map[string]any {
				t.Helper()

				personalizeCreateSolutionVersion(t, h, "list-leak-sv")

				return listSingle(t, h, "ListSolutionVersions", "solutionVersions")
			},
		},
		{
			name:    "campaigns",
			present: []string{"campaignArn", "name", "status", "creationDateTime", "lastUpdatedDateTime"},
			leaked:  []string{"solutionVersionArn", "minProvisionedTPS", "campaignConfig", "latestCampaignUpdate"},
			setup: func(t *testing.T, h *personalize.Handler) map[string]any {
				t.Helper()

				personalizeCreateCampaign(t, h, "list-leak-camp")

				return listSingle(t, h, "ListCampaigns", "campaigns")
			},
		},
		{
			name:    "dataDeletionJobs",
			present: []string{"dataDeletionJobArn", "datasetGroupArn", "jobName", "status", "creationDateTime"},
			leaked:  []string{"roleArn", "dataSource", "numDeleted"},
			setup: func(t *testing.T, h *personalize.Handler) map[string]any {
				t.Helper()

				dgArn := personalizeCreateDatasetGroup(t, h, "list-leak-ddj-dg")
				rec := personalizeDo(t, h, "CreateDataDeletionJob", map[string]any{
					"jobName":         "list-leak-ddj",
					"datasetGroupArn": dgArn,
					"roleArn":         "arn:aws:iam::000000000000:role/PersonalizeRole",
					"dataSource":      map[string]any{"dataLocation": "s3://bucket/delete.csv"},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				return listSingle(t, h, "ListDataDeletionJobs", "dataDeletionJobs")
			},
		},
		{
			name:    "datasetGroups",
			present: []string{"datasetGroupArn", "name", "domain", "status", "creationDateTime"},
			leaked:  []string{"kmsKeyArn", "roleArn"},
			setup: func(t *testing.T, h *personalize.Handler) map[string]any {
				t.Helper()

				personalizeCreateDatasetGroup(t, h, "list-leak-dg")

				return listSingle(t, h, "ListDatasetGroups", "datasetGroups")
			},
		},
		{
			name:    "filters",
			present: []string{"filterArn", "name", "datasetGroupArn", "status", "creationDateTime"},
			leaked:  []string{"filterExpression"},
			setup: func(t *testing.T, h *personalize.Handler) map[string]any {
				t.Helper()

				dgArn := personalizeCreateDatasetGroup(t, h, "list-leak-filter-dg")
				rec := personalizeDo(t, h, "CreateFilter", map[string]any{
					"name":             "list-leak-filter",
					"datasetGroupArn":  dgArn,
					"filterExpression": "INCLUDE ItemID WHERE Items.CATEGORY IN ($CATEGORIES)",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				return listSingle(t, h, "ListFilters", "filters")
			},
		},
		{
			name: "recommenders",
			present: []string{
				"recommenderArn", "name", "datasetGroupArn", "recipeArn", "status", "creationDateTime",
			},
			leaked: []string{"latestRecommenderUpdate"},
			setup: func(t *testing.T, h *personalize.Handler) map[string]any {
				t.Helper()

				dgArn := personalizeCreateDatasetGroup(t, h, "list-leak-rec-dg")
				rec := personalizeDo(t, h, "CreateRecommender", map[string]any{
					"name":            "list-leak-rec",
					"datasetGroupArn": dgArn,
					"recipeArn":       "arn:aws:personalize:::recipe/aws-user-personalization",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				recArn := personalizeUnmarshal(t, rec)["recommenderArn"].(string)

				// UpdateRecommender populates latestRecommenderUpdate on
				// Describe -- the List item must still omit it.
				rec = personalizeDo(t, h, "UpdateRecommender", map[string]any{
					"recommenderArn": recArn,
					"recommenderConfig": map[string]any{
						"minRecommendationRequestsPerSecond": float64(2),
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				return listSingle(t, h, "ListRecommenders", "recommenders")
			},
		},
		{
			name:    "schemas",
			present: []string{"schemaArn", "name", "domain", "creationDateTime"},
			leaked:  []string{"schema"},
			setup: func(t *testing.T, h *personalize.Handler) map[string]any {
				t.Helper()

				rec := personalizeDo(t, h, "CreateSchema", map[string]any{
					"name":   "list-leak-schema",
					"schema": `{"type":"record"}`,
					"domain": "ECOMMERCE",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				return listSingle(t, h, "ListSchemas", "schemas")
			},
		},
		{
			name:    "eventTrackers",
			present: []string{"eventTrackerArn", "name", "status", "creationDateTime"},
			leaked:  []string{"datasetGroupArn", "trackingId"},
			setup: func(t *testing.T, h *personalize.Handler) map[string]any {
				t.Helper()

				dgArn := personalizeCreateDatasetGroup(t, h, "list-leak-et-dg")
				rec := personalizeDo(t, h, "CreateEventTracker", map[string]any{
					"name":            "list-leak-et",
					"datasetGroupArn": dgArn,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				return listSingle(t, h, "ListEventTrackers", "eventTrackers")
			},
		},
		{
			name:    "batchInferenceJobs",
			present: []string{"batchInferenceJobArn", "jobName", "solutionVersionArn", "status", "creationDateTime"},
			leaked:  []string{"roleArn", "jobInput", "jobOutput"},
			setup: func(t *testing.T, h *personalize.Handler) map[string]any {
				t.Helper()

				svArn := personalizeCreateSolutionVersion(t, h, "list-leak-bij-sol")
				rec := personalizeDo(t, h, "CreateBatchInferenceJob", map[string]any{
					"jobName":            "list-leak-bij",
					"solutionVersionArn": svArn,
					"roleArn":            "arn:aws:iam::000000000000:role/PersonalizeRole",
					"jobInput":           map[string]any{"s3DataSource": map[string]any{"path": "s3://bucket/in"}},
					"jobOutput": map[string]any{
						"s3DataDestination": map[string]any{"path": "s3://bucket/out"},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				return listSingle(t, h, "ListBatchInferenceJobs", "batchInferenceJobs")
			},
		},
		{
			name:    "batchSegmentJobs",
			present: []string{"batchSegmentJobArn", "jobName", "solutionVersionArn", "status", "creationDateTime"},
			leaked:  []string{"roleArn", "jobInput", "jobOutput"},
			setup: func(t *testing.T, h *personalize.Handler) map[string]any {
				t.Helper()

				svArn := personalizeCreateSolutionVersion(t, h, "list-leak-bsj-sol")
				rec := personalizeDo(t, h, "CreateBatchSegmentJob", map[string]any{
					"jobName":            "list-leak-bsj",
					"solutionVersionArn": svArn,
					"roleArn":            "arn:aws:iam::000000000000:role/PersonalizeRole",
					"jobInput":           map[string]any{"s3DataSource": map[string]any{"path": "s3://bucket/in"}},
					"jobOutput": map[string]any{
						"s3DataDestination": map[string]any{"path": "s3://bucket/out"},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				return listSingle(t, h, "ListBatchSegmentJobs", "batchSegmentJobs")
			},
		},
		{
			name:    "metricAttributions",
			present: []string{"metricAttributionArn", "name", "status", "creationDateTime"},
			leaked:  []string{"datasetGroupArn", "metricsOutputConfig"},
			setup: func(t *testing.T, h *personalize.Handler) map[string]any {
				t.Helper()

				dgArn := personalizeCreateDatasetGroup(t, h, "list-leak-ma-dg")
				rec := personalizeDo(t, h, "CreateMetricAttribution", map[string]any{
					"name":            "list-leak-ma",
					"datasetGroupArn": dgArn,
					"metrics": []map[string]any{
						{"eventType": "click", "expression": "SUM(Items.PRICE)", "metricName": "click-sum"},
					},
					"metricsOutputConfig": map[string]any{
						"s3DataDestination": map[string]any{"path": "s3://bucket/metrics"},
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				return listSingle(t, h, "ListMetricAttributions", "metricAttributions")
			},
		},
		{
			name:    "datasets",
			present: []string{"datasetArn", "name", "datasetType", "status", "creationDateTime"},
			leaked:  []string{"datasetGroupArn", "schemaArn"},
			setup: func(t *testing.T, h *personalize.Handler) map[string]any {
				t.Helper()

				personalizeCreateDataset(t, h, "list-leak-ds")

				return listSingle(t, h, "ListDatasets", "datasets")
			},
		},
		{
			name:    "datasetImportJobs",
			present: []string{"datasetImportJobArn", "jobName", "status", "creationDateTime"},
			leaked:  []string{"datasetArn", "roleArn", "dataSource"},
			setup: func(t *testing.T, h *personalize.Handler) map[string]any {
				t.Helper()

				dsArn := personalizeCreateDataset(t, h, "list-leak-dij")
				rec := personalizeDo(t, h, "CreateDatasetImportJob", map[string]any{
					"jobName":    "list-leak-dij",
					"datasetArn": dsArn,
					"roleArn":    "arn:aws:iam::000000000000:role/PersonalizeRole",
					"dataSource": map[string]any{"dataLocation": "s3://bucket/import.csv"},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				return listSingle(t, h, "ListDatasetImportJobs", "datasetImportJobs")
			},
		},
		{
			name:    "datasetExportJobs",
			present: []string{"datasetExportJobArn", "jobName", "status", "creationDateTime"},
			leaked:  []string{"datasetArn", "roleArn", "jobOutput"},
			setup: func(t *testing.T, h *personalize.Handler) map[string]any {
				t.Helper()

				dsArn := personalizeCreateDataset(t, h, "list-leak-dej")
				rec := personalizeDo(t, h, "CreateDatasetExportJob", map[string]any{
					"jobName":    "list-leak-dej",
					"datasetArn": dsArn,
					"roleArn":    "arn:aws:iam::000000000000:role/PersonalizeRole",
					"jobOutput":  map[string]any{"s3DataDestination": map[string]any{"path": "s3://bucket/export"}},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				return listSingle(t, h, "ListDatasetExportJobs", "datasetExportJobs")
			},
		},
		{
			name:    "recipes",
			present: []string{"name", "recipeArn", "status"},
			leaked:  []string{"recipeType"},
			setup: func(t *testing.T, h *personalize.Handler) map[string]any {
				t.Helper()

				rec := personalizeDo(t, h, "ListRecipes", map[string]any{"maxResults": float64(1)})
				require.Equal(t, http.StatusOK, rec.Code)
				items := personalizeUnmarshal(t, rec)["recipes"].([]any)
				require.Len(t, items, 1)

				item, ok := items[0].(map[string]any)
				require.True(t, ok)

				return item
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := personalizeHandler(t)
			item := tt.setup(t, h)

			for _, k := range tt.present {
				assert.Contains(t, item, k, "expected real Summary member %q", k)
			}
			for _, k := range tt.leaked {
				assert.NotContains(t, item, k, "leaked Get-only member %q", k)
			}
		})
	}
}

// listSingle calls a List action with an empty filter body, expecting
// exactly one item under listKey, and returns it.
func listSingle(
	t *testing.T,
	h *personalize.Handler,
	action string,
	listKey string,
) map[string]any {
	t.Helper()

	rec := personalizeDo(t, h, action, map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	items, ok := personalizeUnmarshal(t, rec)[listKey].([]any)
	require.True(t, ok, "%s response missing %q list", action, listKey)
	require.Len(t, items, 1)

	item, ok := items[0].(map[string]any)
	require.True(t, ok)

	return item
}
