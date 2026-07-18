package personalize_test

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

func personalizeHandler(t *testing.T) *personalize.Handler {
	t.Helper()

	return personalize.NewHandler(personalize.NewInMemoryBackend("000000000000", "us-east-1"))
}

func personalizeDo(
	t *testing.T,
	h *personalize.Handler,
	action string,
	body map[string]any,
) *httptest.ResponseRecorder {
	t.Helper()

	return personalizeDoWithPrefix(t, h, "AmazonPersonalize.", action, body)
}

func personalizeRuntimeDo(
	t *testing.T,
	h *personalize.Handler,
	action string,
	body map[string]any,
) *httptest.ResponseRecorder {
	t.Helper()

	return personalizeDoWithPrefix(t, h, "AmazonPersonalizeRuntime.", action, body)
}

func personalizeDoWithPrefix(
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

func personalizeUnmarshal(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()

	var m map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &m))

	return m
}

// personalizeCreateCampaign creates a solution version then a campaign with the given name.
func personalizeCreateCampaign(t *testing.T, h *personalize.Handler, name string) {
	t.Helper()

	rec := personalizeDo(t, h, "CreateSolutionVersion", map[string]any{
		"solutionArn": "arn:aws:personalize:us-east-1:000000000000:solution/sol",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	svArn := personalizeUnmarshal(t, rec)["solutionVersionArn"].(string)

	rec = personalizeDo(t, h, "CreateCampaign", map[string]any{
		"name":               name,
		"solutionVersionArn": svArn,
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

// --- Protocol accuracy ---

func TestPersonalize_Protocol_ContentType(t *testing.T) {
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

			h := personalizeHandler(t)
			rec := personalizeDo(t, h, tt.action, tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Contains(t, rec.Header().Get("Content-Type"), tt.wantCT)
		})
	}
}

func TestPersonalize_Protocol_NonPost(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/some-path", nil)
	req.Header.Set("X-Amz-Target", "AmazonPersonalize.CreateDatasetGroup")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestPersonalize_Protocol_MissingTarget(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestPersonalize_Protocol_ErrorEnvelope(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)
	rec := personalizeDo(t, h, "DescribeDatasetGroup", map[string]any{
		"datasetGroupArn": "arn:aws:personalize:us-east-1:000000000000:dataset-group/not-here",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	m := personalizeUnmarshal(t, rec)
	assert.Equal(t, "ResourceNotFoundException", m["__type"])
	assert.NotEmpty(t, m["message"])
}

// --- ARN format ---

func TestPersonalize_ARNFormat(t *testing.T) {
	t.Parallel()

	const arnPrefix = "arn:aws:personalize:us-east-1:000000000000:"

	h := personalizeHandler(t)

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

			rec := personalizeDo(t, h, tt.createAction, tt.createBody)
			assert.Equal(t, http.StatusOK, rec.Code)
			m := personalizeUnmarshal(t, rec)
			arn, _ := m[tt.arnField].(string)
			assert.True(t, strings.HasPrefix(arn, arnPrefix),
				"ARN %q should have prefix %q", arn, arnPrefix)
		})
	}
}

// --- DatasetGroup ---

func TestPersonalize_ErrorPaths(t *testing.T) {
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

			h := personalizeHandler(t)
			rec := personalizeDo(t, h, tt.action, tt.body)

			assert.Equal(t, http.StatusBadRequest, rec.Code)
			m := personalizeUnmarshal(t, rec)
			assert.Equal(t, tt.wantErr, m["__type"])
			assert.NotEmpty(t, m["message"])
		})
	}
}
