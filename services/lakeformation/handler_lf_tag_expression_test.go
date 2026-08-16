package lakeformation_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lakeformation"
)

func TestHandler_CreateLFTagExpression(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		body       string
		wantStatus int
	}{
		{
			name:       "success",
			body:       `{"Name":"my-expr","Expression":[{"TagKey":"env","TagValues":["prod"]}]}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing_name",
			body:       `{"Expression":[{"TagKey":"env","TagValues":["prod"]}]}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_expression",
			body:       `{"Name":"my-expr"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_json",
			body:       `not-json`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			rec := doLFRequest(t, h, "/CreateLFTagExpression", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CreateLFTagExpression_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	body := `{"Name":"dup-expr","Expression":[{"TagKey":"env","TagValues":["prod"]}]}`

	rec := doLFRequest(t, h, "/CreateLFTagExpression", body)
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doLFRequest(t, h, "/CreateLFTagExpression", body)
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestHandler_DeleteLFTagExpression(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setupFn    func(b *lakeformation.InMemoryBackend)
		body       string
		wantStatus int
	}{
		{
			name: "success",
			setupFn: func(b *lakeformation.InMemoryBackend) {
				err := b.CreateLFTagExpression("my-expr", "desc", "123456789012", []lakeformation.LFTag{
					{TagKey: "env", TagValues: []string{"prod"}},
				})
				require.NoError(t, err)
			},
			body:       `{"Name":"my-expr","CatalogId":"123456789012"}`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			body:       `{"Name":"nonexistent"}`,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "missing_name",
			body:       `{}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid_json",
			body:       `not-json`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := lakeformation.NewInMemoryBackend()
			if tt.setupFn != nil {
				tt.setupFn(b)
			}

			h := lakeformation.NewHandler(b)
			h.AccountID = testAccountID
			h.DefaultRegion = testRegion

			rec := doLFRequest(t, h, "/DeleteLFTagExpression", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestCreateLFTagExpression_RoundTrip(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/CreateLFTagExpression", map[string]any{
		"Name":        "my-expr",
		"Description": "a test expression",
		"CatalogId":   "cat",
		"Expression":  []map[string]any{{"TagKey": "env", "TagValues": []string{"dev"}}},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, b.LFTagExpressionCount())
}

func TestDeleteLFTagExpression_NotFound(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/DeleteLFTagExpression", map[string]any{
		"Name":      "nonexistent",
		"CatalogId": "cat",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestListLFTagExpressions_Empty(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/ListLFTagExpressions", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	assert.NotNil(t, out["LFTagExpressions"])
}

func TestListLFTagExpressions_AfterCreate(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	b.AddLFTagExpressionInternal(&lakeformation.LFTagExpression{
		Name:      "myexpr",
		CatalogID: "123",
		Expression: []lakeformation.LFTag{
			{TagKey: "env", TagValues: []string{"prod"}},
		},
	})

	rec := postJSON(t, h, "/ListLFTagExpressions", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	exprs := out["LFTagExpressions"].([]any)
	assert.Len(t, exprs, 1)
}

// --- DeleteLakeFormationOptIn tests ---

func TestCreateLFTagExpression_RequiresTagKey(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/CreateLFTagExpression", map[string]any{
		"Name": "myexpr",
		"Expression": []any{
			map[string]any{"TagKey": "", "TagValues": []string{"v"}},
		},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestGetLFTagExpression_RoundTrip(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	b.AddLFTagExpressionInternal(&lakeformation.LFTagExpression{
		Name:        "myexpr",
		CatalogID:   "cat",
		Description: "test expression",
		Expression:  []lakeformation.LFTag{{TagKey: "env", TagValues: []string{"prod"}}},
	})

	rec := postJSON(t, h, "/GetLFTagExpression", map[string]any{
		"Name":      "myexpr",
		"CatalogId": "cat",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	assert.Equal(t, "myexpr", out["Name"])
	assert.Equal(t, "test expression", out["Description"])
}

func TestUpdateLFTagExpression_Success(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)
	b.AddLFTagExpressionInternal(&lakeformation.LFTagExpression{
		Name:      "expr1",
		CatalogID: "cat",
		Expression: []lakeformation.LFTag{
			{TagKey: "env", TagValues: []string{"dev"}},
		},
	})

	rec := postJSON(t, h, "/UpdateLFTagExpression", map[string]any{
		"Name":        "expr1",
		"CatalogId":   "cat",
		"Description": "updated",
		"Expression":  []map[string]any{{"TagKey": "env", "TagValues": []string{"prod"}}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify via GetLFTagExpression
	rec2 := postJSON(t, h, "/GetLFTagExpression", map[string]any{"Name": "expr1", "CatalogId": "cat"})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec2.Body, &out))
	assert.Equal(t, "updated", out["Description"])
}

// --- Identity Center configuration lifecycle ---

func TestUpdateLFTagExpression_PartialUpdate(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	// Create expression
	postJSON(t, h, "/CreateLFTagExpression", map[string]any{
		"Name":        "myexpr",
		"Description": "original description",
		"Expression":  []any{map[string]any{"TagKey": "env", "TagValues": []any{"dev"}}},
	})

	// Update description only (no Expression field)
	rec := postJSON(t, h, "/UpdateLFTagExpression", map[string]any{
		"Name":        "myexpr",
		"Description": "updated description",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify expression preserved
	rec2 := postJSON(t, h, "/GetLFTagExpression", map[string]any{"Name": "myexpr"})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec2.Body, &out))
	assert.Equal(t, "updated description", out["Description"])
	expr := out["Expression"].([]any)
	assert.Len(t, expr, 1, "Expression should be preserved when not updated")
}

func TestUpdateLFTagExpression_EmptyExpressionRejected(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	postJSON(t, h, "/CreateLFTagExpression", map[string]any{
		"Name":       "myexpr2",
		"Expression": []any{map[string]any{"TagKey": "env", "TagValues": []any{"dev"}}},
	})

	// Send empty Expression array — should fail
	rec := postJSON(t, h, "/UpdateLFTagExpression", map[string]any{
		"Name":       "myexpr2",
		"Expression": []any{},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
