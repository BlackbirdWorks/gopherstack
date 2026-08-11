package lambda_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/synctest"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

// newRealHandler creates a lambda.Handler backed by a real InMemoryBackend for ESM tests.
func newRealHandler(t *testing.T) (*lambda.Handler, *lambda.InMemoryBackend) {
	t.Helper()

	backend := lambda.NewInMemoryBackend(
		nil, nil, lambda.DefaultSettings(),
		"000000000000", "us-east-1",
	)
	closeBackend(t, backend)
	handler := lambda.NewHandler(backend)

	return handler, backend
}

// doESMRequest sends an HTTP request to the ESM endpoint.
func doESMRequest(t *testing.T, h *lambda.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// TestLambda_ESM_FilterCriteria_RoundTrip verifies that FilterCriteria submitted
// at create / update time is preserved across Get and List operations for AWS parity.
func TestLambda_ESM_FilterCriteria_RoundTrip(t *testing.T) {
	t.Parallel()

	h, bk := newRealHandler(t)

	streamARN := "arn:aws:kinesis:us-east-1:000000000000:stream/my-stream-fc"
	require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{FunctionName: "fc-fn"}))

	createBody := map[string]any{
		"EventSourceArn":   streamARN,
		"FunctionName":     "fc-fn",
		"StartingPosition": "TRIM_HORIZON",
		"BatchSize":        10,
		"Enabled":          true,
		"FilterCriteria": map[string]any{
			"Filters": []map[string]string{
				{"Pattern": `{"data":{"orderType":["premium"]}}`},
			},
		},
	}

	rec := doESMRequest(t, h, http.MethodPost, "/2015-03-31/event-source-mappings/", createBody)
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp struct {
		UUID           string `json:"UUID"`
		FilterCriteria struct {
			Filters []struct {
				Pattern string `json:"Pattern"`
			} `json:"Filters"`
		} `json:"FilterCriteria"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	require.NotEmpty(t, createResp.UUID)
	require.Len(t, createResp.FilterCriteria.Filters, 1)
	assert.JSONEq(t, `{"data":{"orderType":["premium"]}}`, createResp.FilterCriteria.Filters[0].Pattern)

	// Update with new filter pattern.
	updBody := map[string]any{
		"FilterCriteria": map[string]any{
			"Filters": []map[string]string{
				{"Pattern": `{"data":{"orderType":["standard"]}}`},
			},
		},
	}
	rec = doESMRequest(t, h, http.MethodPut, "/2015-03-31/event-source-mappings/"+createResp.UUID, updBody)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doESMRequest(t, h, http.MethodGet, "/2015-03-31/event-source-mappings/"+createResp.UUID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp struct {
		FilterCriteria struct {
			Filters []struct {
				Pattern string `json:"Pattern"`
			} `json:"Filters"`
		} `json:"FilterCriteria"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	require.Len(t, getResp.FilterCriteria.Filters, 1)
	assert.JSONEq(t, `{"data":{"orderType":["standard"]}}`, getResp.FilterCriteria.Filters[0].Pattern)
}

// TestLambda_ESM_CRUD tests the full Create / Get / List / Delete lifecycle.
func TestLambda_ESM_CRUD(t *testing.T) {
	t.Parallel()

	h, bk := newRealHandler(t)

	streamARN := "arn:aws:kinesis:us-east-1:000000000000:stream/my-stream"

	require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{FunctionName: "my-function"}))

	// CreateEventSourceMapping
	rec := doESMRequest(t, h, http.MethodPost, "/2015-03-31/event-source-mappings/", map[string]any{
		"EventSourceArn":   streamARN,
		"FunctionName":     "my-function",
		"StartingPosition": "TRIM_HORIZON",
		"BatchSize":        50,
		"Enabled":          true,
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp struct {
		UUID           string `json:"UUID"`
		EventSourceARN string `json:"EventSourceArn"`
		State          string `json:"State"`
		BatchSize      int    `json:"BatchSize"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	assert.NotEmpty(t, createResp.UUID)
	assert.Equal(t, streamARN, createResp.EventSourceARN)
	assert.Equal(t, "Enabled", createResp.State)
	assert.Equal(t, 50, createResp.BatchSize)

	esmUUID := createResp.UUID

	// GetEventSourceMapping
	rec = doESMRequest(t, h, http.MethodGet, "/2015-03-31/event-source-mappings/"+esmUUID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp struct {
		UUID  string `json:"UUID"`
		State string `json:"State"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.Equal(t, esmUUID, getResp.UUID)

	// ListEventSourceMappings
	rec = doESMRequest(t, h, http.MethodGet, "/2015-03-31/event-source-mappings/", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp struct {
		EventSourceMappings []struct {
			UUID string `json:"UUID"`
		} `json:"EventSourceMappings"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	require.Len(t, listResp.EventSourceMappings, 1)
	assert.Equal(t, esmUUID, listResp.EventSourceMappings[0].UUID)

	// DeleteEventSourceMapping
	rec = doESMRequest(t, h, http.MethodDelete, "/2015-03-31/event-source-mappings/"+esmUUID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify gone
	rec = doESMRequest(t, h, http.MethodGet, "/2015-03-31/event-source-mappings/"+esmUUID, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestLambda_ESM_CreateDisabled tests creating a disabled event source mapping.
func TestLambda_ESM_CreateDisabled(t *testing.T) {
	t.Parallel()

	h, bk := newRealHandler(t)

	require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{FunctionName: "my-function"}))

	enabled := false
	rec := doESMRequest(t, h, http.MethodPost, "/2015-03-31/event-source-mappings/", map[string]any{
		"EventSourceArn": "arn:aws:kinesis:us-east-1:000000000000:stream/my-stream",
		"FunctionName":   "my-function",
		"Enabled":        enabled,
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp struct {
		State string `json:"State"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "Disabled", resp.State)
}

// TestLambda_ESM_CreateNilEnabled tests that omitting Enabled defaults to true.
func TestLambda_ESM_CreateNilEnabled(t *testing.T) {
	t.Parallel()

	h, bk := newRealHandler(t)

	require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{FunctionName: "my-function"}))

	// No Enabled field - should default to enabled
	rec := doESMRequest(t, h, http.MethodPost, "/2015-03-31/event-source-mappings/", map[string]any{
		"EventSourceArn": "arn:aws:kinesis:us-east-1:000000000000:stream/my-stream",
		"FunctionName":   "my-function",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp struct {
		State string `json:"State"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "Enabled", resp.State)
}

// TestLambda_ESM_ListByFunctionName tests filtering ListEventSourceMappings by function name.
func TestLambda_ESM_ListByFunctionName(t *testing.T) {
	t.Parallel()

	h, bk := newRealHandler(t)

	require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{FunctionName: "function-a"}))
	require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{FunctionName: "function-b"}))

	// Create two mappings for different functions
	doESMRequest(t, h, http.MethodPost, "/2015-03-31/event-source-mappings/", map[string]any{
		"EventSourceArn": "arn:aws:kinesis:us-east-1:000000000000:stream/stream-1",
		"FunctionName":   "function-a",
	})
	doESMRequest(t, h, http.MethodPost, "/2015-03-31/event-source-mappings/", map[string]any{
		"EventSourceArn": "arn:aws:kinesis:us-east-1:000000000000:stream/stream-2",
		"FunctionName":   "function-b",
	})

	// List all
	rec := doESMRequest(t, h, http.MethodGet, "/2015-03-31/event-source-mappings/", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var allResp struct {
		EventSourceMappings []any `json:"EventSourceMappings"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &allResp))
	assert.Len(t, allResp.EventSourceMappings, 2)

	// List for function-a only
	rec = doESMRequest(t, h, http.MethodGet, "/2015-03-31/event-source-mappings/?FunctionName=function-a", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var filtResp struct {
		EventSourceMappings []struct {
			FunctionARN string `json:"FunctionArn"`
		} `json:"EventSourceMappings"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &filtResp))
	assert.Len(t, filtResp.EventSourceMappings, 1)
	assert.Contains(t, filtResp.EventSourceMappings[0].FunctionARN, "function-a")
}

// TestLambda_ESM_GetNotFound tests getting a non-existent ESM.
func TestLambda_ESM_GetNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newRealHandler(t)

	rec := doESMRequest(t, h, http.MethodGet, "/2015-03-31/event-source-mappings/nonexistent-uuid", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestLambda_ESM_DeleteNotFound tests deleting a non-existent ESM.
func TestLambda_ESM_DeleteNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newRealHandler(t)

	rec := doESMRequest(t, h, http.MethodDelete, "/2015-03-31/event-source-mappings/nonexistent-uuid", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestLambda_ESM_InvalidJSON tests that CreateESM rejects invalid JSON.
func TestLambda_ESM_InvalidJSON(t *testing.T) {
	t.Parallel()

	h, _ := newRealHandler(t)

	e := echo.New()
	req := httptest.NewRequest(
		http.MethodPost,
		"/2015-03-31/event-source-mappings/",
		bytes.NewReader([]byte("{invalid")),
	)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestLambda_ESM_RouteMatcher tests ESM routes are matched.
func TestLambda_ESM_RouteMatcher(t *testing.T) {
	t.Parallel()

	h, _ := newRealHandler(t)
	e := echo.New()

	// ESM path should be matched
	req := httptest.NewRequest(http.MethodPost, "/2015-03-31/event-source-mappings/", nil)
	c := e.NewContext(req, httptest.NewRecorder())
	assert.True(t, h.RouteMatcher()(c))

	// Lambda function path should still be matched
	req2 := httptest.NewRequest(http.MethodGet, "/2015-03-31/functions", nil)
	c2 := e.NewContext(req2, httptest.NewRecorder())
	assert.True(t, h.RouteMatcher()(c2))
}

// TestLambda_ESM_UnknownESMMethod tests that an unknown method returns 404.
func TestLambda_ESM_UnknownESMMethod(t *testing.T) {
	t.Parallel()

	h, _ := newRealHandler(t)

	// PUT is not supported for ESM
	e := echo.New()
	req := httptest.NewRequest(
		http.MethodPut,
		"/2015-03-31/event-source-mappings/some-uuid",
		bytes.NewReader([]byte("{}")),
	)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestLambda_StreamNameFromARN tests the ARN parser.
func TestLambda_StreamNameFromARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		arn      string
		expected string
	}{
		{
			name:     "valid Kinesis ARN",
			arn:      "arn:aws:kinesis:us-east-1:000000000000:stream/my-stream",
			expected: "my-stream",
		},
		{
			name:     "valid ARN with hyphens",
			arn:      "arn:aws:kinesis:eu-west-1:123456789012:stream/my-test-stream",
			expected: "my-test-stream",
		},
		{
			name:     "empty string",
			arn:      "",
			expected: "",
		},
		{
			name:     "invalid ARN",
			arn:      "not-an-arn",
			expected: "",
		},
		{
			name:     "too short ARN",
			arn:      "arn:aws",
			expected: "",
		},
		{
			// Passes the len(arn)>len(prefix) check but has too few colon-separated parts.
			name:     "too few parts after prefix",
			arn:      "arn:aws:kinesis:us-east-1",
			expected: "",
		},
		{
			// Has 6 parts but the last segment is just "stream/" with no stream name.
			name:     "stream segment too short",
			arn:      "arn:aws:kinesis:us-east-1:000000000000:stream/",
			expected: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := lambda.StreamNameFromARN(tc.arn)
			assert.Equal(t, tc.expected, result)
		})
	}
}

// TestLambda_FunctionNameFromARN tests the Lambda ARN function name parser.
func TestLambda_FunctionNameFromARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		arn      string
		expected string
	}{
		{
			name:     "valid Lambda ARN",
			arn:      "arn:aws:lambda:us-east-1:000000000000:function:my-function",
			expected: "my-function",
		},
		{
			name:     "empty string",
			arn:      "",
			expected: "",
		},
		{
			// Bug fix (parity-sweep-3): the old implementation SplitN'd on ":"
			// with a fixed part count and returned "" for any input shorter
			// than a full ARN, silently dropping bare function names. A bare
			// name is a legitimate input (callers fall back to it when the
			// ARN doesn't parse) and must round-trip unchanged.
			name:     "just a name",
			arn:      "my-function",
			expected: "my-function",
		},
		{
			// Qualified ARN: the qualifier segment must be stripped, not
			// glued onto the name (previously "arn:...:function:my-func:PROD"
			// incorrectly returned "PROD" as the "name").
			name:     "qualified ARN with alias",
			arn:      "arn:aws:lambda:us-east-1:000000000000:function:my-function:PROD",
			expected: "my-function",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := lambda.FunctionNameFromARN(tc.arn)
			assert.Equal(t, tc.expected, result)
		})
	}
}

// TestLambda_ESMIndex_ListByFunctionName_UsesIndex verifies that the ESM function-ARN
// index correctly filters mappings per function and that listing with no filter returns all.
func TestLambda_ESMIndex_ListByFunctionName_UsesIndex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		filterFn     string
		wantContains string
		wantCount    int
	}{
		{
			name:         "filter_function_a",
			filterFn:     "function-a",
			wantCount:    1,
			wantContains: "function-a",
		},
		{
			name:         "filter_function_b",
			filterFn:     "function-b",
			wantCount:    1,
			wantContains: "function-b",
		},
		{
			name:      "no_filter_returns_all",
			filterFn:  "",
			wantCount: 2,
		},
	}

	_, backend := newRealHandler(t)

	require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{FunctionName: "function-a"}))
	require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{FunctionName: "function-b"}))

	// Create ESMs for two distinct functions.
	_, err := backend.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
		EventSourceARN: "arn:aws:kinesis:us-east-1:000000000000:stream/stream-a",
		FunctionName:   "function-a",
		Enabled:        true,
	})
	require.NoError(t, err)

	_, err = backend.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
		EventSourceARN: "arn:aws:kinesis:us-east-1:000000000000:stream/stream-b",
		FunctionName:   "function-b",
		Enabled:        true,
	})
	require.NoError(t, err)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := backend.ListEventSourceMappings(tt.filterFn, "", "", 0)
			assert.Len(t, result.Data, tt.wantCount)

			if tt.wantContains != "" {
				require.Len(t, result.Data, 1)
				assert.Contains(t, result.Data[0].FunctionARN, tt.wantContains)
			}
		})
	}
}

// TestLambda_DeleteFunction_CascadesESMDelete verifies that deleting a function also
// removes all of its event source mappings.
func TestLambda_DeleteFunction_CascadesESMDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "cascade_deletes_esm"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, backend := newRealHandler(t)

			fnName := "cascade-fn-" + tt.name
			require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{FunctionName: fnName}))

			_, err := backend.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
				EventSourceARN: "arn:aws:kinesis:us-east-1:000000000000:stream/cascade-stream",
				FunctionName:   fnName,
				Enabled:        true,
			})
			require.NoError(t, err)

			// Verify ESM exists before deletion.
			before := backend.ListEventSourceMappings(fnName, "", "", 0)
			require.Len(t, before.Data, 1)

			// Delete the function.
			require.NoError(t, backend.DeleteFunction(fnName))

			// ESMs for the deleted function must be gone.
			after := backend.ListEventSourceMappings(fnName, "", "", 0)
			assert.Empty(t, after.Data)

			// The global list must also be empty.
			all := backend.ListEventSourceMappings("", "", "", 0)
			assert.Empty(t, all.Data)
		})
	}
}

// TestLambda_CreateESM_AllowsNonExistentFunction verifies that CreateEventSourceMapping
// succeeds even when the referenced Lambda function does not exist. AWS allows creating ESMs
// for functions that have not yet been created; the ESM enters an error state at invoke time.
func TestLambda_CreateESM_AllowsNonExistentFunction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		functionName string
	}{
		{
			name:         "bare_function_name",
			functionName: "does-not-exist",
		},
		{
			name:         "function_arn",
			functionName: "arn:aws:lambda:us-east-1:000000000000:function:also-does-not-exist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, backend := newRealHandler(t)

			m, err := backend.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
				EventSourceARN: "arn:aws:kinesis:us-east-1:000000000000:stream/test-stream",
				FunctionName:   tt.functionName,
				Enabled:        true,
			})

			require.NoError(t, err, "CreateEventSourceMapping must succeed even when function does not exist")
			assert.NotEmpty(t, m.UUID)
		})
	}
}

// TestLambda_UpdateESM_UpdatesLastModified verifies that UpdateEventSourceMapping sets
// LastModified to a time after the mapping was created.
func TestLambda_UpdateESM_UpdatesLastModified(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "updates_last_modified"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				_, backend := newRealHandler(t)

				require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{FunctionName: "update-esm-fn"}))

				m, err := backend.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
					EventSourceARN: "arn:aws:kinesis:us-east-1:000000000000:stream/update-stream",
					FunctionName:   "update-esm-fn",
					Enabled:        true,
				})
				require.NoError(t, err)

				createdAt := m.LastModified

				// Ensure at least 1ms passes.
				time.Sleep(time.Millisecond)

				updated, updateErr := backend.UpdateEventSourceMapping(m.UUID, &lambda.UpdateEventSourceMappingInput{
					Enabled:   new(false),
					BatchSize: 0,
				})
				require.NoError(t, updateErr)

				assert.True(t, updated.LastModified.After(createdAt),
					"LastModified should be after creation time: got %v, created %v", updated.LastModified, createdAt)
			})
		})
	}
}
