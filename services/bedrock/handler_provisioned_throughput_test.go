package bedrock_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

// --- Provisioned model throughput tests --- //nolint:godot // existing issue.
func TestHandler_CreateProvisionedModelThroughput(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		setup      func(*testing.T, *bedrock.Handler)
		input      map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "valid provisioned throughput",
			input: map[string]any{
				"provisionedModelName": "my-throughput",
				"modelId":              "amazon.titan-text-express-v1",
				"modelUnits":           1,
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "duplicate name",
			setup: func(t *testing.T, h *bedrock.Handler) {
				t.Helper()

				_, err := h.Backend.CreateProvisionedModelThroughput(
					"dup-throughput", "amazon.titan-text-express-v1", 1, "", nil,
				)
				require.NoError(t, err)
			},
			input: map[string]any{
				"provisionedModelName": "dup-throughput",
				"modelId":              "amazon.titan-text-express-v1",
				"modelUnits":           1,
			},
			// CreateProvisionedModelThroughput's deserializer declares no
			// ConflictException (bedrock@v1.66.4 deserializers.go); the
			// backend now reports this as ValidationException/400.
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests { //nolint:paralleltest // existing issue.
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doRequest(t, h, http.MethodPost, "/provisioned-model-throughput", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var out map[string]any
				mustUnmarshal(t, rec, &out)
				assert.NotEmpty(t, out["provisionedModelArn"])
			}
		})
	}
}

func TestHandler_GetProvisionedModelThroughput(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		name       string
		setup      func(*bedrock.Handler) string
		id         string
		wantStatus int
	}{
		{
			name: "existing pmt by arn",
			setup: func(h *bedrock.Handler) string {
				pmt, err := h.Backend.CreateProvisionedModelThroughput(
					"my-pmt",
					"amazon.titan-text-express-v1",
					1,
					"",
					nil,
				)
				if err != nil {
					panic(err)
				}

				return pmt.ProvisionedModelArn
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "existing pmt by name",
			setup: func(h *bedrock.Handler) string {
				_, err := h.Backend.CreateProvisionedModelThroughput(
					"named-pmt",
					"amazon.titan-text-express-v1",
					1,
					"",
					nil,
				)
				if err != nil {
					panic(err)
				}

				return "named-pmt"
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "non-existent pmt",
			id:         "nonexistent",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests { //nolint:paralleltest // existing issue.
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHandler(t)

			id := tt.id
			if tt.setup != nil {
				id = tt.setup(h)
			}

			encodedID := url.PathEscape(id)
			rec := doRequest(t, h, http.MethodGet, "/provisioned-model-throughput/"+encodedID, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListProvisionedModelThroughputs(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)
	_, err := h.Backend.CreateProvisionedModelThroughput("pmt1", "amazon.titan-text-express-v1", 1, "", nil)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodGet, "/provisioned-model-throughputs", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)
	summaries := out["provisionedModelSummaries"].([]any)
	assert.Len(t, summaries, 1)
}

func TestHandler_DeleteProvisionedModelThroughput(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		name       string
		setup      func(*bedrock.Handler) string
		id         string
		wantStatus int
	}{
		{
			name: "delete existing",
			setup: func(h *bedrock.Handler) string {
				pmt, err := h.Backend.CreateProvisionedModelThroughput(
					"my-pmt",
					"amazon.titan-text-express-v1",
					1,
					"",
					nil,
				)
				if err != nil {
					panic(err)
				}

				return pmt.ProvisionedModelArn
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete non-existent",
			id:         "nonexistent",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests { //nolint:paralleltest // existing issue.
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHandler(t)

			id := tt.id
			if tt.setup != nil {
				id = tt.setup(h)
			}

			encodedID := url.PathEscape(id)
			rec := doRequest(t, h, http.MethodDelete, "/provisioned-model-throughput/"+encodedID, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_UpdateProvisionedModelThroughput(t *testing.T) { //nolint:paralleltest // existing issue.
	tests := []struct {
		setup      func(*bedrock.Handler) string
		input      map[string]any
		name       string
		id         string
		wantStatus int
	}{
		{
			name: "update existing",
			setup: func(h *bedrock.Handler) string {
				pmt, err := h.Backend.CreateProvisionedModelThroughput(
					"upd-pmt",
					"amazon.titan-text-express-v1",
					1,
					"",
					nil,
				)
				if err != nil {
					panic(err)
				}

				return pmt.ProvisionedModelArn
			},
			input:      map[string]any{"desiredModelId": "anthropic.claude-v2"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "update non-existent",
			id:         "nonexistent",
			input:      map[string]any{"desiredModelId": "anthropic.claude-v2"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "invalid json",
			id:         "nonexistent",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests { //nolint:paralleltest // existing issue.
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHandler(t)

			id := tt.id
			if tt.setup != nil {
				id = tt.setup(h)
			}

			var rec *httptest.ResponseRecorder

			if tt.input == nil {
				e := echo.New()
				req := httptest.NewRequest(
					http.MethodPatch,
					"/provisioned-model-throughput/"+url.PathEscape(id),
					bytes.NewReader([]byte("bad json")),
				)
				req.Header.Set("Content-Type", "application/json")
				recRaw := httptest.NewRecorder()
				c := e.NewContext(req, recRaw)
				err := h.Handler()(c)
				require.NoError(t, err)
				assert.Equal(t, tt.wantStatus, recRaw.Code)

				return
			}

			rec = doRequest(t, h, http.MethodPatch, "/provisioned-model-throughput/"+url.PathEscape(id), tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListProvisionedModelThroughputsPagination( //nolint:paralleltest // existing issue.
	t *testing.T,
) {
	h := newTestHandler(t)

	for i := range 105 {
		_, err := h.Backend.CreateProvisionedModelThroughput(
			fmt.Sprintf("pmt-%04d", i),
			"amazon.titan-text-express-v1",
			1, "", nil,
		)
		require.NoError(t, err)
	}

	// First page.
	rec := doRequest(t, h, http.MethodGet, "/provisioned-model-throughputs", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	mustUnmarshal(t, rec, &out)

	summaries := out["provisionedModelSummaries"].([]any)
	assert.Len(t, summaries, 100)
	nextToken, ok := out["nextToken"].(string)
	require.True(t, ok, "nextToken should be present")
	assert.NotEmpty(t, nextToken)

	// Second page.
	rec2 := doRequest(t, h, http.MethodGet, "/provisioned-model-throughputs?nextToken="+url.QueryEscape(nextToken), nil)
	assert.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	mustUnmarshal(t, rec2, &out2)

	summaries2 := out2["provisionedModelSummaries"].([]any)
	assert.Len(t, summaries2, 5)
	assert.Empty(t, out2["nextToken"])
}

func TestHandler_CreateProvisionedModelThroughputValidation( //nolint:paralleltest // existing issue.
	t *testing.T,
) {
	tests := []struct {
		input      map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "missing name",
			input: map[string]any{
				"provisionedModelName": "",
				"modelId":              "amazon.titan-text-express-v1",
				"modelUnits":           1,
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "zero model units",
			input: map[string]any{
				"provisionedModelName": "my-pmt",
				"modelId":              "amazon.titan-text-express-v1",
				"modelUnits":           0,
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "negative model units",
			input: map[string]any{
				"provisionedModelName": "my-pmt",
				"modelId":              "amazon.titan-text-express-v1",
				"modelUnits":           -1,
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests { //nolint:paralleltest // existing issue.
		t.Run(tt.name, func(t *testing.T) {
			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/provisioned-model-throughput", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestAccuracy_ProvisionedModelThroughput_InitialStatusCreating(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	pmt, err := b.CreateProvisionedModelThroughput("my-pmt", "amazon.titan-text-express-v1", 1, "", nil)
	require.NoError(t, err)
	assert.Equal(t, "Creating", pmt.Status)
}

// TestAccuracy_CreateProvisionedModelThroughput_ModelUnitsBounds verifies that
// modelUnits is validated against both the lower (>0) and upper bound AWS enforces.
func TestAccuracy_CreateProvisionedModelThroughput_ModelUnitsBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		modelUnits int32
		wantErr    bool
	}{
		{name: "valid", modelUnits: 1, wantErr: false},
		{name: "zero_rejected", modelUnits: 0, wantErr: true},
		{name: "negative_rejected", modelUnits: -1, wantErr: true},
		{name: "above_upper_bound_rejected", modelUnits: 100000, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := b.CreateProvisionedModelThroughput(
				"pmt-"+tt.name, "amazon.titan-text-express-v1", tt.modelUnits, "", nil)

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "ValidationException")

				return
			}
			require.NoError(t, err)
		})
	}
}

func TestAccuracy_AdvanceProvisionedModelThroughput_CreatingToInService(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	pmt, err := b.CreateProvisionedModelThroughput("pmt-advance", "amazon.titan-text-express-v1", 1, "", nil)
	require.NoError(t, err)
	assert.Equal(t, "Creating", pmt.Status)

	n := b.AdvanceProvisionedModelThroughputStatuses()
	assert.Equal(t, 1, n)

	advanced, err := b.GetProvisionedModelThroughput(pmt.ProvisionedModelArn)
	require.NoError(t, err)
	assert.Equal(t, "InService", advanced.Status)
}

func TestAccuracy_AdvanceProvisionedModelThroughput_AlreadyInService(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	pmt, err := b.CreateProvisionedModelThroughput("pmt-inservice", "amazon.titan-text-express-v1", 1, "", nil)
	require.NoError(t, err)

	// Advance once to InService.
	b.AdvanceProvisionedModelThroughputStatuses()

	// Advance again — should not count any new advancements.
	n := b.AdvanceProvisionedModelThroughputStatuses()
	assert.Equal(t, 0, n)

	got, err := b.GetProvisionedModelThroughput(pmt.ProvisionedModelArn)
	require.NoError(t, err)
	assert.Equal(t, "InService", got.Status)
}

// TestAccuracy_PMT_UpdateDesiredModelAndName verifies UpdateProvisionedModelThroughput's
// real (and only) two mutable fields: desiredModelId and desiredProvisionedModelName.
// AWS has no way to change modelUnits after creation — that's fixed at CreateProvisionedModelThroughput.
func TestAccuracy_PMT_UpdateDesiredModelAndName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		desiredModelID string
		desiredNewName string
	}{
		{name: "swap desired model", desiredModelID: "anthropic.claude-v2"},
		{name: "rename", desiredNewName: "renamed-pmt"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)
			pmt, err := b.CreateProvisionedModelThroughput(
				"update-pmt-"+tt.name, "amazon.titan-text-express-v1", 1, "", nil,
			)
			require.NoError(t, err)

			body := map[string]any{}
			if tt.desiredModelID != "" {
				body["desiredModelId"] = tt.desiredModelID
			}

			if tt.desiredNewName != "" {
				body["desiredProvisionedModelName"] = tt.desiredNewName
			}

			rec := doRequest(
				t, h, http.MethodPatch,
				"/provisioned-model-throughput/"+url.PathEscape(pmt.ProvisionedModelArn),
				body,
			)
			assert.Equal(t, http.StatusOK, rec.Code)

			updated, err := b.GetProvisionedModelThroughput(pmt.ProvisionedModelArn)
			require.NoError(t, err)

			if tt.desiredModelID != "" {
				assert.Contains(t, updated.DesiredModelArn, tt.desiredModelID)
			}

			if tt.desiredNewName != "" {
				assert.Equal(t, tt.desiredNewName, updated.ProvisionedModelName)
			}
		})
	}
}

func TestAccuracy_PMT_TagsOnCreate(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	tags := []bedrock.Tag{{Key: "cost-center", Value: "ml-team"}}
	pmt, err := b.CreateProvisionedModelThroughput("tagged-pmt", "amazon.titan-text-express-v1", int32(1), "", tags)
	require.NoError(t, err)
	assert.Len(t, pmt.Tags, 1)
	assert.Equal(t, "cost-center", pmt.Tags[0].Key)
	assert.Equal(t, "ml-team", pmt.Tags[0].Value)
}

// TestParity_ListProvisionedModelThroughputs_NameContainsFilter locks in the
// nameContains query filter (bedrock@v1.66.4
// api_op_ListProvisionedModelThroughputs.go's NameContains) --
// ListProvisionedModelThroughputs previously only read nextToken, so
// statusEquals, modelArnEquals, nameContains, creationTimeAfter/Before, and
// sortOrder were silently ignored regardless of what a real client sent.
func TestParity_ListProvisionedModelThroughputs_NameContainsFilter(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("123456789012", "us-east-1")
	h := bedrock.NewHandler(b)

	_, err := b.CreateProvisionedModelThroughput("other-throughput", "amazon.titan-text-express-v1", 1, "", nil)
	require.NoError(t, err)

	wantPMT, err := b.CreateProvisionedModelThroughput(
		"target-throughput", "amazon.titan-text-express-v1", 1, "", nil,
	)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodGet, "/provisioned-model-throughputs?nameContains=target", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	summaries, ok := out["provisionedModelSummaries"].([]any)
	require.True(t, ok)
	require.Len(t, summaries, 1)

	summary, ok := summaries[0].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, wantPMT.ProvisionedModelArn, summary["provisionedModelArn"])
}
