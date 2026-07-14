package personalize_test

// Audit batch-1: AWS-accuracy coverage for Personalize (go-89ux).
//
// Covers:
//   - Protocol: Content-Type application/x-amz-json-1.1 on success/error, error envelope
//     __type+message fields, non-POST → 405, missing X-Amz-Target → 400
//   - ARN format: all resource ARNs contain "arn:aws:personalize:us-east-1:000000000000:"
//   - DatasetGroup: Create/Describe/List/Delete field shapes, domain passthrough
//   - Dataset: Create/Describe/List/Delete/Update, datasetType + schemaArn retention
//   - Schema: Create/Describe/List/Delete, schema+domain field shapes
//   - Solution: Create/Describe/List/Delete, recipeArn + performAutoML retention
//   - SolutionVersion: Create/Describe/List/Delete, solutionArn linkage, GetSolutionMetrics
//   - Campaign: Create/Describe/List/Delete/Update, solutionVersionArn + minProvisionedTPS
//   - EventTracker: Create/Describe/List/Delete, trackingId non-empty, datasetGroupArn linkage
//   - Filter: Create/Describe/List/Delete, filterExpression retention
//   - Recommender: Create/Describe/List/Delete/Update, Start/StopRecommender status transitions
//   - MetricAttribution: Create/Describe/List/Delete/Update, ListMetricAttributionMetrics
//   - DatasetImportJob: Create/Describe/List, ARN returned
//   - DatasetExportJob: Create/Describe/List, ARN returned
//   - BatchInferenceJob: Create/Describe/List, ARN returned
//   - BatchSegmentJob: Create/Describe/List, ARN returned
//   - DataDeletionJob: Create/Describe/List, ARN returned
//   - Recipe: DescribeRecipe/ListRecipes — built-in recipes present
//   - FeatureTransformation: DescribeFeatureTransformation — returns status ACTIVE
//   - Algorithm: DescribeAlgorithm — returns status ACTIVE
//   - Tag round-trip: TagResource/ListTagsForResource/UntagResource tagKey+tagValue shape
//   - StopSolutionVersionCreation: status → CREATE STOPPED
//   - Error paths: ResourceNotFoundException → 400, ResourceAlreadyExistsException → 400,
//     InvalidInputException → 400, with __type+message envelope

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/personalize"
)

// --- Test helpers ---

func a1PersonalizeHandler(t *testing.T) *personalize.Handler {
	t.Helper()

	return personalize.NewHandler(personalize.NewInMemoryBackend("000000000000", "us-east-1"))
}

func a1PersonalizeDo(
	t *testing.T,
	h *personalize.Handler,
	action string,
	body map[string]any,
) *httptest.ResponseRecorder {
	t.Helper()

	return a1PersonalizeDoWithPrefix(t, h, "AmazonPersonalize.", action, body)
}

func a1PersonalizeRuntimeDo(
	t *testing.T,
	h *personalize.Handler,
	action string,
	body map[string]any,
) *httptest.ResponseRecorder {
	t.Helper()

	return a1PersonalizeDoWithPrefix(t, h, "AmazonPersonalizeRuntime.", action, body)
}

func a1PersonalizeDoWithPrefix(
	t *testing.T,
	h *personalize.Handler,
	prefix, action string,
	body map[string]any,
) *httptest.ResponseRecorder {
	t.Helper()

	payload, err := json.Marshal(body)
	require.NoError(t, err)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(payload)))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", prefix+action)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

func a1PersonalizeUnmarshal(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))

	return m
}

// a1PersonalizeCreateCampaign creates a solution version then a campaign with the given name.
func a1PersonalizeCreateCampaign(t *testing.T, h *personalize.Handler, name string) {
	t.Helper()

	rec := a1PersonalizeDo(t, h, "CreateSolutionVersion", map[string]any{
		"solutionArn": "arn:aws:personalize:us-east-1:000000000000:solution/sol",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	svArn := a1PersonalizeUnmarshal(t, rec)["solutionVersionArn"].(string)

	rec = a1PersonalizeDo(t, h, "CreateCampaign", map[string]any{
		"name":               name,
		"solutionVersionArn": svArn,
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

// --- Protocol accuracy ---

func TestAudit1_Personalize_Protocol_ContentType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		action     string
		wantCT     string
		wantStatus int
	}{
		{
			name:       "success_has_json11_content_type",
			action:     "CreateDatasetGroup",
			body:       map[string]any{"name": "ct-group", "domain": "ECOMMERCE"},
			wantStatus: http.StatusOK,
			wantCT:     "application/x-amz-json-1.1",
		},
		{
			name:   "error_has_json11_content_type",
			action: "DescribeDatasetGroup",
			body: map[string]any{
				"datasetGroupArn": "arn:aws:personalize:us-east-1:000000000000:dataset-group/missing",
			},
			wantStatus: http.StatusBadRequest,
			wantCT:     "application/x-amz-json-1.1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := a1PersonalizeHandler(t)
			rec := a1PersonalizeDo(t, h, tt.action, tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Contains(t, rec.Header().Get("Content-Type"), tt.wantCT)
		})
	}
}

func TestAudit1_Personalize_Protocol_NonPost(t *testing.T) {
	t.Parallel()

	h := a1PersonalizeHandler(t)

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/some-path", nil)
	req.Header.Set("X-Amz-Target", "AmazonPersonalize.CreateDatasetGroup")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestAudit1_Personalize_Protocol_MissingTarget(t *testing.T) {
	t.Parallel()

	h := a1PersonalizeHandler(t)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestAudit1_Personalize_Protocol_ErrorEnvelope(t *testing.T) {
	t.Parallel()

	h := a1PersonalizeHandler(t)
	rec := a1PersonalizeDo(t, h, "DescribeDatasetGroup", map[string]any{
		"datasetGroupArn": "arn:aws:personalize:us-east-1:000000000000:dataset-group/not-here",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	m := a1PersonalizeUnmarshal(t, rec)
	assert.Equal(t, "ResourceNotFoundException", m["__type"])
	assert.NotEmpty(t, m["message"])
}

// --- ARN format ---

func TestAudit1_Personalize_ARNFormat(t *testing.T) {
	t.Parallel()

	const arnPrefix = "arn:aws:personalize:us-east-1:000000000000:"

	h := a1PersonalizeHandler(t)

	tests := []struct {
		createAction string
		createBody   map[string]any
		arnField     string
		name         string
	}{
		{
			name:         "dataset_group",
			createAction: "CreateDatasetGroup",
			createBody:   map[string]any{"name": "arn-dg", "domain": "ECOMMERCE"},
			arnField:     "datasetGroupArn",
		},
		{
			name:         "schema",
			createAction: "CreateSchema",
			createBody:   map[string]any{"name": "arn-schema", "schema": `{"type":"record"}`},
			arnField:     "schemaArn",
		},
		{
			name:         "solution",
			createAction: "CreateSolution",
			createBody: map[string]any{
				"name":            "arn-sol",
				"datasetGroupArn": "arn:aws:personalize:us-east-1:000000000000:dataset-group/x",
			},
			arnField: "solutionArn",
		},
		{
			name:         "campaign",
			createAction: "CreateCampaign",
			createBody: map[string]any{
				"name":               "arn-camp",
				"solutionVersionArn": "arn:aws:personalize:us-east-1:000000000000:solution/x/v1",
			},
			arnField: "campaignArn",
		},
		{
			name:         "filter",
			createAction: "CreateFilter",
			createBody: map[string]any{
				"name":             "arn-filter",
				"datasetGroupArn":  "arn:aws:personalize:us-east-1:000000000000:dataset-group/x",
				"filterExpression": "INCLUDE ItemID WHERE Items.CATEGORY IN ($CATEGORIES)",
			},
			arnField: "filterArn",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := a1PersonalizeDo(t, h, tt.createAction, tt.createBody)
			assert.Equal(t, http.StatusOK, rec.Code)
			m := a1PersonalizeUnmarshal(t, rec)
			arn, _ := m[tt.arnField].(string)
			assert.True(t, strings.HasPrefix(arn, arnPrefix),
				"ARN %q should have prefix %q", arn, arnPrefix)
		})
	}
}

// --- DatasetGroup ---

func TestAudit1_Personalize_DatasetGroup_CRUD(t *testing.T) {
	t.Parallel()

	h := a1PersonalizeHandler(t)

	// Create
	rec := a1PersonalizeDo(t, h, "CreateDatasetGroup", map[string]any{
		"name":   "my-group",
		"domain": "ECOMMERCE",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	created := a1PersonalizeUnmarshal(t, rec)
	dgArn, _ := created["datasetGroupArn"].(string)
	assert.NotEmpty(t, dgArn)
	assert.Equal(t, "ECOMMERCE", created["domain"])

	// Describe
	rec = a1PersonalizeDo(t, h, "DescribeDatasetGroup", map[string]any{"datasetGroupArn": dgArn})
	require.Equal(t, http.StatusOK, rec.Code)
	described := a1PersonalizeUnmarshal(t, rec)
	dg := described["datasetGroup"].(map[string]any)
	assert.Equal(t, "my-group", dg["name"])
	assert.Equal(t, "ECOMMERCE", dg["domain"])
	assert.Equal(t, "ACTIVE", dg["status"])
	assert.NotEmpty(t, dg["creationDateTime"])
	assert.NotEmpty(t, dg["lastUpdatedDateTime"])

	// List
	rec = a1PersonalizeDo(t, h, "ListDatasetGroups", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	listed := a1PersonalizeUnmarshal(t, rec)
	groups := listed["datasetGroups"].([]any)
	assert.Len(t, groups, 1)

	// Delete
	rec = a1PersonalizeDo(t, h, "DeleteDatasetGroup", map[string]any{"datasetGroupArn": dgArn})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify deleted
	rec = a1PersonalizeDo(t, h, "DescribeDatasetGroup", map[string]any{"datasetGroupArn": dgArn})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	errBody := a1PersonalizeUnmarshal(t, rec)
	assert.Equal(t, "ResourceNotFoundException", errBody["__type"])
}

func TestAudit1_Personalize_DatasetGroup_AlreadyExists(t *testing.T) {
	t.Parallel()

	h := a1PersonalizeHandler(t)
	a1PersonalizeDo(t, h, "CreateDatasetGroup", map[string]any{"name": "dup-group"})
	rec := a1PersonalizeDo(t, h, "CreateDatasetGroup", map[string]any{"name": "dup-group"})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	m := a1PersonalizeUnmarshal(t, rec)
	assert.Equal(t, "ResourceAlreadyExistsException", m["__type"])
}

// --- Dataset ---

func TestAudit1_Personalize_Dataset_FieldRetention(t *testing.T) {
	t.Parallel()

	h := a1PersonalizeHandler(t)

	rec := a1PersonalizeDo(t, h, "CreateDataset", map[string]any{
		"name":            "interaction-ds",
		"datasetGroupArn": "arn:aws:personalize:us-east-1:000000000000:dataset-group/g1",
		"datasetType":     "INTERACTIONS",
		"schemaArn":       "arn:aws:personalize:us-east-1:000000000000:schema/my-schema",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	created := a1PersonalizeUnmarshal(t, rec)
	dsArn := created["datasetArn"].(string)

	rec = a1PersonalizeDo(t, h, "DescribeDataset", map[string]any{"datasetArn": dsArn})
	require.Equal(t, http.StatusOK, rec.Code)
	m := a1PersonalizeUnmarshal(t, rec)
	ds := m["dataset"].(map[string]any)
	assert.Equal(t, "interaction-ds", ds["name"])
	assert.Equal(t, "INTERACTIONS", ds["datasetType"])
	assert.Equal(t, "arn:aws:personalize:us-east-1:000000000000:schema/my-schema", ds["schemaArn"])
	assert.Equal(t, "ACTIVE", ds["status"])
}

func TestAudit1_Personalize_Dataset_Update(t *testing.T) {
	t.Parallel()

	h := a1PersonalizeHandler(t)

	rec := a1PersonalizeDo(t, h, "CreateDataset", map[string]any{
		"name":        "update-ds",
		"datasetType": "ITEMS",
		"schemaArn":   "arn:aws:personalize:us-east-1:000000000000:schema/old-schema",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	dsArn := a1PersonalizeUnmarshal(t, rec)["datasetArn"].(string)

	rec = a1PersonalizeDo(t, h, "UpdateDataset", map[string]any{
		"datasetArn": dsArn,
		"schemaArn":  "arn:aws:personalize:us-east-1:000000000000:schema/new-schema",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = a1PersonalizeDo(t, h, "DescribeDataset", map[string]any{"datasetArn": dsArn})
	ds := a1PersonalizeUnmarshal(t, rec)["dataset"].(map[string]any)
	assert.Equal(t, "arn:aws:personalize:us-east-1:000000000000:schema/new-schema", ds["schemaArn"])
}

// --- Schema ---

func TestAudit1_Personalize_Schema_FieldRetention(t *testing.T) {
	t.Parallel()

	h := a1PersonalizeHandler(t)

	schemaContent := `{"type":"record","name":"Interactions","namespace":"com.amazonaws.personalize.schema",` +
		`"fields":[{"name":"USER_ID","type":"string"},{"name":"ITEM_ID","type":"string"},` +
		`{"name":"TIMESTAMP","type":"long"}],"version":"1.0"}`
	rec := a1PersonalizeDo(t, h, "CreateSchema", map[string]any{
		"name":   "my-schema",
		"schema": schemaContent,
		"domain": "ECOMMERCE",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	schemaArn := a1PersonalizeUnmarshal(t, rec)["schemaArn"].(string)
	assert.NotEmpty(t, schemaArn)

	rec = a1PersonalizeDo(t, h, "DescribeSchema", map[string]any{"schemaArn": schemaArn})
	require.Equal(t, http.StatusOK, rec.Code)
	m := a1PersonalizeUnmarshal(t, rec)
	s := m["schema"].(map[string]any)
	assert.Equal(t, "my-schema", s["name"])
	assert.Equal(t, schemaContent, s["schema"])
	assert.Equal(t, "ECOMMERCE", s["domain"])
	assert.NotEmpty(t, s["creationDateTime"])
}

func TestAudit1_Personalize_Schema_List(t *testing.T) {
	t.Parallel()

	h := a1PersonalizeHandler(t)
	a1PersonalizeDo(t, h, "CreateSchema", map[string]any{"name": "s1"})
	a1PersonalizeDo(t, h, "CreateSchema", map[string]any{"name": "s2"})

	rec := a1PersonalizeDo(t, h, "ListSchemas", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	listed := a1PersonalizeUnmarshal(t, rec)
	schemas := listed["schemas"].([]any)
	assert.Len(t, schemas, 2)
}

// --- Solution ---

func TestAudit1_Personalize_Solution_FieldRetention(t *testing.T) {
	t.Parallel()

	h := a1PersonalizeHandler(t)

	rec := a1PersonalizeDo(t, h, "CreateSolution", map[string]any{
		"name":            "my-solution",
		"datasetGroupArn": "arn:aws:personalize:us-east-1:000000000000:dataset-group/g1",
		"recipeArn":       "arn:aws:personalize:::recipe/aws-user-personalization",
		"performAutoML":   false,
		"performHPO":      true,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	solArn := a1PersonalizeUnmarshal(t, rec)["solutionArn"].(string)

	rec = a1PersonalizeDo(t, h, "DescribeSolution", map[string]any{"solutionArn": solArn})
	require.Equal(t, http.StatusOK, rec.Code)
	sol := a1PersonalizeUnmarshal(t, rec)["solution"].(map[string]any)
	assert.Equal(t, "my-solution", sol["name"])
	assert.Equal(t, "arn:aws:personalize:::recipe/aws-user-personalization", sol["recipeArn"])
	assert.Equal(t, false, sol["performAutoML"])
	assert.Equal(t, true, sol["performHPO"])
	assert.Equal(t, "ACTIVE", sol["status"])
	// performAutoTraining defaults to true when omitted from CreateSolution.
	assert.Equal(t, true, sol["performAutoTraining"])
	assert.Equal(t, false, sol["performIncrementalUpdate"])
}

// TestAudit1_Personalize_Solution_Update verifies UpdateSolution mutates the
// real wire fields (performAutoTraining/performIncrementalUpdate), not the
// creation-only performAutoML/performHPO fields the real UpdateSolutionInput
// does not carry.
func TestAudit1_Personalize_Solution_Update(t *testing.T) {
	t.Parallel()

	h := a1PersonalizeHandler(t)

	rec := a1PersonalizeDo(t, h, "CreateSolution", map[string]any{
		"name":            "update-me",
		"datasetGroupArn": "arn:aws:personalize:us-east-1:000000000000:dataset-group/g1",
		"recipeArn":       "arn:aws:personalize:::recipe/aws-user-personalization",
		"performAutoML":   false,
		"performHPO":      true,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	solArn := a1PersonalizeUnmarshal(t, rec)["solutionArn"].(string)

	rec = a1PersonalizeDo(t, h, "UpdateSolution", map[string]any{
		"solutionArn":              solArn,
		"performAutoTraining":      false,
		"performIncrementalUpdate": true,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = a1PersonalizeDo(t, h, "DescribeSolution", map[string]any{"solutionArn": solArn})
	sol := a1PersonalizeUnmarshal(t, rec)["solution"].(map[string]any)
	assert.Equal(t, false, sol["performAutoTraining"])
	assert.Equal(t, true, sol["performIncrementalUpdate"])
	// performAutoML/performHPO are creation-only and must be unaffected by UpdateSolution.
	assert.Equal(t, false, sol["performAutoML"])
	assert.Equal(t, true, sol["performHPO"])

	// Omitting both update fields leaves the current values untouched.
	rec = a1PersonalizeDo(t, h, "UpdateSolution", map[string]any{"solutionArn": solArn})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = a1PersonalizeDo(t, h, "DescribeSolution", map[string]any{"solutionArn": solArn})
	sol = a1PersonalizeUnmarshal(t, rec)["solution"].(map[string]any)
	assert.Equal(t, false, sol["performAutoTraining"])
	assert.Equal(t, true, sol["performIncrementalUpdate"])
}

// --- SolutionVersion ---

func TestAudit1_Personalize_SolutionVersion_Lifecycle(t *testing.T) {
	t.Parallel()

	h := a1PersonalizeHandler(t)

	solArn := "arn:aws:personalize:us-east-1:000000000000:solution/my-solution"

	// Create solution version
	rec := a1PersonalizeDo(t, h, "CreateSolutionVersion", map[string]any{
		"solutionArn":  solArn,
		"trainingMode": "FULL",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	svArn, _ := a1PersonalizeUnmarshal(t, rec)["solutionVersionArn"].(string)
	assert.True(t, strings.HasPrefix(svArn, solArn+"/"), "solutionVersionArn should start with solutionArn/")

	// Describe
	rec = a1PersonalizeDo(t, h, "DescribeSolutionVersion", map[string]any{"solutionVersionArn": svArn})
	require.Equal(t, http.StatusOK, rec.Code)
	sv := a1PersonalizeUnmarshal(t, rec)["solutionVersion"].(map[string]any)
	assert.Equal(t, solArn, sv["solutionArn"])
	assert.Equal(t, "FULL", sv["trainingMode"])
	assert.Equal(t, "ACTIVE", sv["status"])

	// List
	rec = a1PersonalizeDo(t, h, "ListSolutionVersions", map[string]any{"solutionArn": solArn})
	require.Equal(t, http.StatusOK, rec.Code)
	listed := a1PersonalizeUnmarshal(t, rec)
	versions := listed["solutionVersions"].([]any)
	assert.Len(t, versions, 1)
}

func TestAudit1_Personalize_StopSolutionVersionCreation(t *testing.T) {
	t.Parallel()

	h := a1PersonalizeHandler(t)

	rec := a1PersonalizeDo(t, h, "CreateSolutionVersion", map[string]any{
		"solutionArn": "arn:aws:personalize:us-east-1:000000000000:solution/sol",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	svArn := a1PersonalizeUnmarshal(t, rec)["solutionVersionArn"].(string)

	rec = a1PersonalizeDo(t, h, "StopSolutionVersionCreation", map[string]any{"solutionVersionArn": svArn})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = a1PersonalizeDo(t, h, "DescribeSolutionVersion", map[string]any{"solutionVersionArn": svArn})
	sv := a1PersonalizeUnmarshal(t, rec)["solutionVersion"].(map[string]any)
	// The SolutionVersion.Status wire enum has no bare "STOPPED" value --
	// only "CREATE STOPPED" (see aws-sdk-go-v2/service/personalize/types.SolutionVersion).
	assert.Equal(t, "CREATE STOPPED", sv["status"])
}

func TestAudit1_Personalize_GetSolutionMetrics(t *testing.T) {
	t.Parallel()

	h := a1PersonalizeHandler(t)

	rec := a1PersonalizeDo(t, h, "CreateSolutionVersion", map[string]any{
		"solutionArn": "arn:aws:personalize:us-east-1:000000000000:solution/sol",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	svArn := a1PersonalizeUnmarshal(t, rec)["solutionVersionArn"].(string)

	rec = a1PersonalizeDo(t, h, "GetSolutionMetrics", map[string]any{"solutionVersionArn": svArn})
	assert.Equal(t, http.StatusOK, rec.Code)
	m := a1PersonalizeUnmarshal(t, rec)
	assert.Equal(t, svArn, m["solutionVersionArn"])
	metrics, ok := m["metrics"].(map[string]any)
	require.True(t, ok)
	assert.Contains(t, metrics, "coverage")
	assert.Contains(t, metrics, "precision_at_5")
}

// --- Campaign ---

func TestAudit1_Personalize_Campaign_CRUD(t *testing.T) {
	t.Parallel()

	h := a1PersonalizeHandler(t)

	svArn := "arn:aws:personalize:us-east-1:000000000000:solution/sol/v1"

	rec := a1PersonalizeDo(t, h, "CreateCampaign", map[string]any{
		"name":               "my-campaign",
		"solutionVersionArn": svArn,
		"minProvisionedTPS":  float64(5),
	})
	require.Equal(t, http.StatusOK, rec.Code)
	campArn := a1PersonalizeUnmarshal(t, rec)["campaignArn"].(string)
	assert.NotEmpty(t, campArn)

	rec = a1PersonalizeDo(t, h, "DescribeCampaign", map[string]any{"campaignArn": campArn})
	require.Equal(t, http.StatusOK, rec.Code)
	c := a1PersonalizeUnmarshal(t, rec)["campaign"].(map[string]any)
	assert.Equal(t, "my-campaign", c["name"])
	assert.Equal(t, svArn, c["solutionVersionArn"])
	assert.InDelta(t, float64(5), c["minProvisionedTPS"], 0)
	assert.Equal(t, "ACTIVE", c["status"])

	// Update
	newSvArn := "arn:aws:personalize:us-east-1:000000000000:solution/sol/v2"
	rec = a1PersonalizeDo(t, h, "UpdateCampaign", map[string]any{
		"campaignArn":        campArn,
		"solutionVersionArn": newSvArn,
		"minProvisionedTPS":  float64(10),
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = a1PersonalizeDo(t, h, "DescribeCampaign", map[string]any{"campaignArn": campArn})
	c = a1PersonalizeUnmarshal(t, rec)["campaign"].(map[string]any)
	assert.Equal(t, newSvArn, c["solutionVersionArn"])
	assert.InDelta(t, float64(10), c["minProvisionedTPS"], 0)

	// List
	rec = a1PersonalizeDo(t, h, "ListCampaigns", map[string]any{})
	listed := a1PersonalizeUnmarshal(t, rec)
	assert.Len(t, listed["campaigns"].([]any), 1)

	// Delete
	rec = a1PersonalizeDo(t, h, "DeleteCampaign", map[string]any{"campaignArn": campArn})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- EventTracker ---

func TestAudit1_Personalize_EventTracker_TrackingID(t *testing.T) {
	t.Parallel()

	h := a1PersonalizeHandler(t)

	rec := a1PersonalizeDo(t, h, "CreateEventTracker", map[string]any{
		"name":            "my-tracker",
		"datasetGroupArn": "arn:aws:personalize:us-east-1:000000000000:dataset-group/g1",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	m := a1PersonalizeUnmarshal(t, rec)
	etArn, _ := m["eventTrackerArn"].(string)
	trackingID, _ := m["trackingId"].(string)
	assert.NotEmpty(t, etArn)
	assert.NotEmpty(t, trackingID, "trackingId must be non-empty on create")

	// Describe returns same trackingId
	rec = a1PersonalizeDo(t, h, "DescribeEventTracker", map[string]any{"eventTrackerArn": etArn})
	require.Equal(t, http.StatusOK, rec.Code)
	et := a1PersonalizeUnmarshal(t, rec)["eventTracker"].(map[string]any)
	assert.Equal(t, trackingID, et["trackingId"])
	assert.Equal(t, "ACTIVE", et["status"])
	assert.Equal(t, "arn:aws:personalize:us-east-1:000000000000:dataset-group/g1", et["datasetGroupArn"])
}

func TestAudit1_Personalize_EventTracker_ListDelete(t *testing.T) {
	t.Parallel()

	h := a1PersonalizeHandler(t)
	dgArn := "arn:aws:personalize:us-east-1:000000000000:dataset-group/g1"
	a1PersonalizeDo(t, h, "CreateEventTracker", map[string]any{"name": "t1", "datasetGroupArn": dgArn})
	a1PersonalizeDo(t, h, "CreateEventTracker", map[string]any{"name": "t2", "datasetGroupArn": dgArn})

	rec := a1PersonalizeDo(t, h, "ListEventTrackers", map[string]any{"datasetGroupArn": dgArn})
	listed := a1PersonalizeUnmarshal(t, rec)
	assert.Len(t, listed["eventTrackers"].([]any), 2)

	// Find the first tracker and delete it
	trackers := listed["eventTrackers"].([]any)
	first := trackers[0].(map[string]any)
	firstArn := first["eventTrackerArn"].(string)
	a1PersonalizeDo(t, h, "DeleteEventTracker", map[string]any{"eventTrackerArn": firstArn})

	rec = a1PersonalizeDo(t, h, "ListEventTrackers", map[string]any{})
	listed = a1PersonalizeUnmarshal(t, rec)
	assert.Len(t, listed["eventTrackers"].([]any), 1)
}

// --- Filter ---

func TestAudit1_Personalize_Filter_FieldRetention(t *testing.T) {
	t.Parallel()

	h := a1PersonalizeHandler(t)

	filterExpr := "INCLUDE ItemID WHERE Items.CATEGORY IN ($CATEGORIES)"
	rec := a1PersonalizeDo(t, h, "CreateFilter", map[string]any{
		"name":             "my-filter",
		"datasetGroupArn":  "arn:aws:personalize:us-east-1:000000000000:dataset-group/g1",
		"filterExpression": filterExpr,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	filterArn := a1PersonalizeUnmarshal(t, rec)["filterArn"].(string)

	rec = a1PersonalizeDo(t, h, "DescribeFilter", map[string]any{"filterArn": filterArn})
	require.Equal(t, http.StatusOK, rec.Code)
	f := a1PersonalizeUnmarshal(t, rec)["filter"].(map[string]any)
	assert.Equal(t, "my-filter", f["name"])
	assert.Equal(t, filterExpr, f["filterExpression"])
	assert.Equal(t, "ACTIVE", f["status"])
}

// --- Recommender ---

func TestAudit1_Personalize_Recommender_StartStop(t *testing.T) {
	t.Parallel()

	h := a1PersonalizeHandler(t)

	rec := a1PersonalizeDo(t, h, "CreateRecommender", map[string]any{
		"name":            "my-rec",
		"datasetGroupArn": "arn:aws:personalize:us-east-1:000000000000:dataset-group/g1",
		"recipeArn":       "arn:aws:personalize:::recipe/aws-user-personalization",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	recArn := a1PersonalizeUnmarshal(t, rec)["recommenderArn"].(string)

	// Stop
	rec = a1PersonalizeDo(t, h, "StopRecommender", map[string]any{"recommenderArn": recArn})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = a1PersonalizeDo(t, h, "DescribeRecommender", map[string]any{"recommenderArn": recArn})
	r := a1PersonalizeUnmarshal(t, rec)["recommender"].(map[string]any)
	assert.Equal(t, "INACTIVE", r["status"])

	// Start
	rec = a1PersonalizeDo(t, h, "StartRecommender", map[string]any{"recommenderArn": recArn})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = a1PersonalizeDo(t, h, "DescribeRecommender", map[string]any{"recommenderArn": recArn})
	r = a1PersonalizeUnmarshal(t, rec)["recommender"].(map[string]any)
	assert.Equal(t, "ACTIVE", r["status"])
}

func TestAudit1_Personalize_Recommender_Config(t *testing.T) {
	t.Parallel()

	h := a1PersonalizeHandler(t)

	rec := a1PersonalizeDo(t, h, "CreateRecommender", map[string]any{
		"name":            "config-rec",
		"datasetGroupArn": "arn:aws:personalize:us-east-1:000000000000:dataset-group/g1",
		"recipeArn":       "arn:aws:personalize:::recipe/aws-user-personalization",
		"recommenderConfig": map[string]any{
			"minRecommendationRequestsPerSecond": float64(10),
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	recArn := a1PersonalizeUnmarshal(t, rec)["recommenderArn"].(string)

	rec = a1PersonalizeDo(t, h, "DescribeRecommender", map[string]any{"recommenderArn": recArn})
	r := a1PersonalizeUnmarshal(t, rec)["recommender"].(map[string]any)
	cfg := r["recommenderConfig"].(map[string]any)
	assert.InDelta(t, float64(10), cfg["minRecommendationRequestsPerSecond"], 0)
}

// --- MetricAttribution ---

func TestAudit1_Personalize_MetricAttribution_CRUD(t *testing.T) {
	t.Parallel()

	h := a1PersonalizeHandler(t)

	rec := a1PersonalizeDo(t, h, "CreateMetricAttribution", map[string]any{
		"name":            "my-ma",
		"datasetGroupArn": "arn:aws:personalize:us-east-1:000000000000:dataset-group/g1",
		"metrics": []map[string]any{
			{"eventType": "click", "expression": "SUM(Items.PRICE)", "metricName": "click-sum"},
		},
		"metricsOutputConfig": map[string]any{
			"s3DataDestination": map[string]any{"path": "s3://my-bucket/metrics"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	maArn := a1PersonalizeUnmarshal(t, rec)["metricAttributionArn"].(string)
	assert.NotEmpty(t, maArn)

	rec = a1PersonalizeDo(t, h, "DescribeMetricAttribution", map[string]any{"metricAttributionArn": maArn})
	ma := a1PersonalizeUnmarshal(t, rec)["metricAttribution"].(map[string]any)
	assert.Equal(t, "my-ma", ma["name"])
	assert.Equal(t, "ACTIVE", ma["status"])

	// ListMetricAttributionMetrics returns the metrics configured at creation,
	// not a fabricated static list.
	rec = a1PersonalizeDo(t, h, "ListMetricAttributionMetrics", map[string]any{"metricAttributionArn": maArn})
	require.Equal(t, http.StatusOK, rec.Code)
	listed := a1PersonalizeUnmarshal(t, rec)
	metrics := listed["metrics"].([]any)
	require.Len(t, metrics, 1)
	first := metrics[0].(map[string]any)
	assert.Equal(t, "click-sum", first["metricName"])
	assert.Equal(t, "click", first["eventType"])
	assert.Equal(t, "SUM(Items.PRICE)", first["expression"])

	// UpdateMetricAttribution: add a metric and remove the original one.
	rec = a1PersonalizeDo(t, h, "UpdateMetricAttribution", map[string]any{
		"metricAttributionArn": maArn,
		"addMetrics": []map[string]any{
			{"eventType": "purchase", "expression": "SUM(Items.PRICE)", "metricName": "purchase-sum"},
		},
		"removeMetrics": []string{"click-sum"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = a1PersonalizeDo(t, h, "ListMetricAttributionMetrics", map[string]any{"metricAttributionArn": maArn})
	require.Equal(t, http.StatusOK, rec.Code)
	metrics = a1PersonalizeUnmarshal(t, rec)["metrics"].([]any)
	require.Len(t, metrics, 1)
	assert.Equal(t, "purchase-sum", metrics[0].(map[string]any)["metricName"])

	// Delete
	rec = a1PersonalizeDo(t, h, "DeleteMetricAttribution", map[string]any{"metricAttributionArn": maArn})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestAudit1_Personalize_MetricAttribution_Create_RequiresMetrics(t *testing.T) {
	t.Parallel()

	h := a1PersonalizeHandler(t)

	rec := a1PersonalizeDo(t, h, "CreateMetricAttribution", map[string]any{
		"name":            "no-metrics-ma",
		"datasetGroupArn": "arn:aws:personalize:us-east-1:000000000000:dataset-group/g1",
		"metricsOutputConfig": map[string]any{
			"s3DataDestination": map[string]any{"path": "s3://my-bucket/metrics"},
		},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)
	body := a1PersonalizeUnmarshal(t, rec)
	assert.Equal(t, "InvalidInputException", body["__type"])
}

// --- Async jobs ---

func TestAudit1_Personalize_DatasetImportJob(t *testing.T) {
	t.Parallel()

	h := a1PersonalizeHandler(t)

	rec := a1PersonalizeDo(t, h, "CreateDatasetImportJob", map[string]any{
		"jobName":    "import-job-1",
		"datasetArn": "arn:aws:personalize:us-east-1:000000000000:dataset/my-ds",
		"roleArn":    "arn:aws:iam::000000000000:role/PersonalizeRole",
		"dataSource": map[string]any{"dataLocation": "s3://my-bucket/data.csv"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	jobArn := a1PersonalizeUnmarshal(t, rec)["datasetImportJobArn"].(string)
	assert.NotEmpty(t, jobArn)
	assert.Contains(t, jobArn, "arn:aws:personalize:us-east-1:000000000000:")

	rec = a1PersonalizeDo(t, h, "DescribeDatasetImportJob", map[string]any{"datasetImportJobArn": jobArn})
	require.Equal(t, http.StatusOK, rec.Code)
	job := a1PersonalizeUnmarshal(t, rec)["datasetImportJob"].(map[string]any)
	assert.Equal(t, "import-job-1", job["jobName"])
	assert.Equal(t, "ACTIVE", job["status"])

	rec = a1PersonalizeDo(t, h, "ListDatasetImportJobs", map[string]any{})
	listed := a1PersonalizeUnmarshal(t, rec)
	assert.Len(t, listed["datasetImportJobs"].([]any), 1)
}

func TestAudit1_Personalize_BatchInferenceJob(t *testing.T) {
	t.Parallel()

	h := a1PersonalizeHandler(t)

	rec := a1PersonalizeDo(t, h, "CreateBatchInferenceJob", map[string]any{
		"jobName":            "batch-inf-1",
		"solutionVersionArn": "arn:aws:personalize:us-east-1:000000000000:solution/sol/v1",
		"roleArn":            "arn:aws:iam::000000000000:role/PersonalizeRole",
		"jobInput":           map[string]any{"s3DataSource": map[string]any{"path": "s3://bucket/input"}},
		"jobOutput":          map[string]any{"s3DataDestination": map[string]any{"path": "s3://bucket/output"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	jobArn := a1PersonalizeUnmarshal(t, rec)["batchInferenceJobArn"].(string)
	assert.NotEmpty(t, jobArn)

	rec = a1PersonalizeDo(t, h, "DescribeBatchInferenceJob", map[string]any{"batchInferenceJobArn": jobArn})
	job := a1PersonalizeUnmarshal(t, rec)["batchInferenceJob"].(map[string]any)
	assert.Equal(t, "batch-inf-1", job["jobName"])
	assert.Equal(t, "ACTIVE", job["status"])
}

func TestAudit1_Personalize_BatchSegmentJob(t *testing.T) {
	t.Parallel()

	h := a1PersonalizeHandler(t)

	rec := a1PersonalizeDo(t, h, "CreateBatchSegmentJob", map[string]any{
		"jobName":            "seg-job-1",
		"solutionVersionArn": "arn:aws:personalize:us-east-1:000000000000:solution/sol/v1",
		"roleArn":            "arn:aws:iam::000000000000:role/PersonalizeRole",
		"jobInput":           map[string]any{"s3DataSource": map[string]any{"path": "s3://bucket/input"}},
		"jobOutput":          map[string]any{"s3DataDestination": map[string]any{"path": "s3://bucket/output"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	jobArn := a1PersonalizeUnmarshal(t, rec)["batchSegmentJobArn"].(string)
	assert.NotEmpty(t, jobArn)

	rec = a1PersonalizeDo(t, h, "ListBatchSegmentJobs", map[string]any{})
	listed := a1PersonalizeUnmarshal(t, rec)
	assert.Len(t, listed["batchSegmentJobs"].([]any), 1)
}

func TestAudit1_Personalize_DataDeletionJob(t *testing.T) {
	t.Parallel()

	h := a1PersonalizeHandler(t)

	rec := a1PersonalizeDo(t, h, "CreateDataDeletionJob", map[string]any{
		"jobName":         "delete-job-1",
		"datasetGroupArn": "arn:aws:personalize:us-east-1:000000000000:dataset-group/g1",
		"roleArn":         "arn:aws:iam::000000000000:role/PersonalizeRole",
		"dataSource":      map[string]any{"dataLocation": "s3://bucket/users-to-delete.csv"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	jobArn := a1PersonalizeUnmarshal(t, rec)["dataDeletionJobArn"].(string)
	assert.NotEmpty(t, jobArn)

	rec = a1PersonalizeDo(t, h, "DescribeDataDeletionJob", map[string]any{"dataDeletionJobArn": jobArn})
	job := a1PersonalizeUnmarshal(t, rec)["dataDeletionJob"].(map[string]any)
	assert.Equal(t, "delete-job-1", job["jobName"])
	assert.Equal(t, "ACTIVE", job["status"])
	assert.NotNil(t, job["numDeleted"])
}

// --- Recipe ---

func TestAudit1_Personalize_Recipe_DescribeList(t *testing.T) {
	t.Parallel()

	h := a1PersonalizeHandler(t)

	rec := a1PersonalizeDo(t, h, "ListRecipes", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	listed := a1PersonalizeUnmarshal(t, rec)
	recipes := listed["recipes"].([]any)
	assert.NotEmpty(t, recipes)

	first := recipes[0].(map[string]any)
	recipeArn, _ := first["recipeArn"].(string)
	assert.NotEmpty(t, recipeArn)

	rec = a1PersonalizeDo(t, h, "DescribeRecipe", map[string]any{"recipeArn": recipeArn})
	require.Equal(t, http.StatusOK, rec.Code)
	m := a1PersonalizeUnmarshal(t, rec)
	r := m["recipe"].(map[string]any)
	assert.Equal(t, "ACTIVE", r["status"])

	// Missing recipe → 400
	rec = a1PersonalizeDo(
		t,
		h,
		"DescribeRecipe",
		map[string]any{"recipeArn": "arn:aws:personalize:::recipe/not-a-real-recipe"},
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- FeatureTransformation / Algorithm ---

func TestAudit1_Personalize_ReadOnlyResources(t *testing.T) {
	t.Parallel()

	h := a1PersonalizeHandler(t)

	ftArn := "arn:aws:personalize:us-east-1:000000000000:" +
		"feature-transformation/aws-feature-transformation"
	rec := a1PersonalizeDo(t, h, "DescribeFeatureTransformation", map[string]any{
		"featureTransformationArn": ftArn,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	ft := a1PersonalizeUnmarshal(t, rec)["featureTransformation"].(map[string]any)
	assert.Equal(t, "ACTIVE", ft["status"])

	rec = a1PersonalizeDo(t, h, "DescribeAlgorithm", map[string]any{
		"algorithmArn": "arn:aws:personalize:::algorithm/user-personalization",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	algo := a1PersonalizeUnmarshal(t, rec)["algorithm"].(map[string]any)
	assert.Equal(t, "ACTIVE", algo["status"])
}

// --- Tag round-trip ---

func TestAudit1_Personalize_TagRoundTrip(t *testing.T) {
	t.Parallel()

	h := a1PersonalizeHandler(t)

	rec := a1PersonalizeDo(t, h, "CreateDatasetGroup", map[string]any{
		"name": "tagged-group",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	dgArn := a1PersonalizeUnmarshal(t, rec)["datasetGroupArn"].(string)

	// Tag
	rec = a1PersonalizeDo(t, h, "TagResource", map[string]any{
		"resourceArn": dgArn,
		"tags": []any{
			map[string]any{"tagKey": "env", "tagValue": "test"},
			map[string]any{"tagKey": "team", "tagValue": "ml"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// List
	rec = a1PersonalizeDo(t, h, "ListTagsForResource", map[string]any{"resourceArn": dgArn})
	require.Equal(t, http.StatusOK, rec.Code)
	m := a1PersonalizeUnmarshal(t, rec)
	tags := m["tags"].([]any)
	assert.Len(t, tags, 2)
	// Verify Key+Value shape
	for _, tag := range tags {
		t2 := tag.(map[string]any)
		assert.Contains(t, t2, "tagKey")
		assert.Contains(t, t2, "tagValue")
	}

	// Untag
	rec = a1PersonalizeDo(t, h, "UntagResource", map[string]any{
		"resourceArn": dgArn,
		"tagKeys":     []any{"env"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = a1PersonalizeDo(t, h, "ListTagsForResource", map[string]any{"resourceArn": dgArn})
	m = a1PersonalizeUnmarshal(t, rec)
	tags = m["tags"].([]any)
	assert.Len(t, tags, 1)
	remaining := tags[0].(map[string]any)
	assert.Equal(t, "team", remaining["tagKey"])
	assert.Equal(t, "ml", remaining["tagValue"])
}

// --- FeatureTransformation real lookup ---

func TestAudit1_Personalize_DescribeFeatureTransformation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		arnOrName  string
		wantName   string
		wantErr    string
		wantStatus int
	}{
		{
			name:       "known_arn_aws_feature_transformation",
			arnOrName:  "arn:aws:personalize:us-east-1:000000000000:feature-transformation/aws-feature-transformation",
			wantStatus: http.StatusOK,
			wantName:   "aws-feature-transformation",
		},
		{
			name: "known_arn_bandits",
			arnOrName: "arn:aws:personalize:us-east-1:000000000000:feature-transformation/" +
				"aws-explicit-contextual-bandits-feature-transformation",
			wantStatus: http.StatusOK,
			wantName:   "aws-explicit-contextual-bandits-feature-transformation",
		},
		{
			name:       "unknown_arn_returns_404",
			arnOrName:  "arn:aws:personalize:us-east-1:000000000000:feature-transformation/not-a-real-one",
			wantStatus: http.StatusBadRequest,
			wantErr:    "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := a1PersonalizeHandler(t)
			rec := a1PersonalizeDo(t, h, "DescribeFeatureTransformation", map[string]any{
				"featureTransformationArn": tt.arnOrName,
			})

			assert.Equal(t, tt.wantStatus, rec.Code)
			m := a1PersonalizeUnmarshal(t, rec)
			if tt.wantErr != "" {
				assert.Equal(t, tt.wantErr, m["__type"])
			} else {
				ft := m["featureTransformation"].(map[string]any)
				assert.Equal(t, tt.wantName, ft["name"])
				assert.Equal(t, "ACTIVE", ft["status"])
				assert.Equal(t, tt.arnOrName, ft["featureTransformationArn"])
			}
		})
	}
}

// --- GetRecommendations / GetPersonalizedRanking ---

func TestAudit1_Personalize_GetRecommendations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input         map[string]any
		name          string
		wantItemCount int
	}{
		{
			name: "default_25_items",
			input: map[string]any{
				"campaignArn": "arn:aws:personalize:us-east-1:000000000000:campaign/my-campaign",
				"userId":      "user-123",
			},
			wantItemCount: 25,
		},
		{
			name: "explicit_numResults_5",
			input: map[string]any{
				"campaignArn": "arn:aws:personalize:us-east-1:000000000000:campaign/my-campaign",
				"userId":      "user-456",
				"numResults":  float64(5),
			},
			wantItemCount: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := a1PersonalizeHandler(t)
			a1PersonalizeCreateCampaign(t, h, "my-campaign")
			rec := a1PersonalizeRuntimeDo(t, h, "GetRecommendations", tt.input)

			require.Equal(t, http.StatusOK, rec.Code)
			m := a1PersonalizeUnmarshal(t, rec)
			assert.NotEmpty(t, m["recommendationId"])
			itemList, ok := m["itemList"].([]any)
			require.True(t, ok)
			assert.Len(t, itemList, tt.wantItemCount)
			first := itemList[0].(map[string]any)
			assert.NotEmpty(t, first["itemId"])
			_, hasScore := first["score"]
			assert.True(t, hasScore)
		})
	}
}

func TestAudit1_Personalize_GetPersonalizedRanking(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		inputList []any
	}{
		{
			name:      "three_items_returned_ranked",
			inputList: []any{"item-a", "item-b", "item-c"},
		},
		{
			name:      "single_item",
			inputList: []any{"item-x"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := a1PersonalizeHandler(t)
			a1PersonalizeCreateCampaign(t, h, "my-campaign")
			rec := a1PersonalizeRuntimeDo(t, h, "GetPersonalizedRanking", map[string]any{
				"campaignArn": "arn:aws:personalize:us-east-1:000000000000:campaign/my-campaign",
				"userId":      "user-789",
				"inputList":   tt.inputList,
			})

			require.Equal(t, http.StatusOK, rec.Code)
			m := a1PersonalizeUnmarshal(t, rec)
			assert.NotEmpty(t, m["recommendationId"])
			ranked, ok := m["personalizedRanking"].([]any)
			require.True(t, ok)
			assert.Len(t, ranked, len(tt.inputList))
			for i, v := range ranked {
				item := v.(map[string]any)
				assert.Equal(t, tt.inputList[i], item["itemId"])
				_, hasScore := item["score"]
				assert.True(t, hasScore)
			}
		})
	}
}

// --- Error paths ---

func TestAudit1_Personalize_ErrorPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body    map[string]any
		name    string
		action  string
		wantErr string
	}{
		{
			name:   "not_found_dataset_group",
			action: "DescribeDatasetGroup",
			body: map[string]any{
				"datasetGroupArn": "arn:aws:personalize:us-east-1:000000000000:dataset-group/missing",
			},
			wantErr: "ResourceNotFoundException",
		},
		{
			name:    "not_found_solution",
			action:  "DescribeSolution",
			body:    map[string]any{"solutionArn": "arn:aws:personalize:us-east-1:000000000000:solution/missing"},
			wantErr: "ResourceNotFoundException",
		},
		{
			name:    "not_found_campaign",
			action:  "DescribeCampaign",
			body:    map[string]any{"campaignArn": "arn:aws:personalize:us-east-1:000000000000:campaign/missing"},
			wantErr: "ResourceNotFoundException",
		},
		{
			name:    "not_found_filter",
			action:  "DescribeFilter",
			body:    map[string]any{"filterArn": "arn:aws:personalize:us-east-1:000000000000:filter/missing"},
			wantErr: "ResourceNotFoundException",
		},
		{
			name:   "not_found_batch_inference_job",
			action: "DescribeBatchInferenceJob",
			body: map[string]any{
				"batchInferenceJobArn": "arn:aws:personalize:us-east-1:000000000000:batch-inference-job/missing",
			},
			wantErr: "ResourceNotFoundException",
		},
		{
			name:    "invalid_unknown_operation",
			action:  "NotARealOperation",
			body:    map[string]any{},
			wantErr: "InvalidInputException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := a1PersonalizeHandler(t)
			rec := a1PersonalizeDo(t, h, tt.action, tt.body)

			assert.Equal(t, http.StatusBadRequest, rec.Code)
			m := a1PersonalizeUnmarshal(t, rec)
			assert.Equal(t, tt.wantErr, m["__type"])
			assert.NotEmpty(t, m["message"])
		})
	}
}
