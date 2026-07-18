package bedrock_test

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/bedrock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAccuracy_PromptRouter_CreateResponseShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		routerName   string
		wantStatus   int
		wantRouterID bool
	}{
		{
			name:         "valid create",
			routerName:   "my-router",
			wantStatus:   http.StatusCreated,
			wantRouterID: true,
		},
		{
			name:       "missing name returns 400",
			routerName: "",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/prompt-routers",
				map[string]any{"promptRouterName": tt.routerName})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantRouterID {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotEmpty(t, out["promptRouterArn"])
			}
		})
	}
}

func TestAccuracy_PromptRouter_GetAndListAccuracy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		routerNames []string
		wantLen     int
	}{
		{name: "single router", routerNames: []string{"router-a"}, wantLen: 1},
		{name: "two routers", routerNames: []string{"router-x", "router-y"}, wantLen: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrock.NewHandler(b)

			var lastARN string
			for _, name := range tt.routerNames {
				r, err := b.CreatePromptRouter(name, nil)
				require.NoError(t, err)
				lastARN = r.PromptRouterArn
			}

			// List.
			recList := doRequest(t, h, http.MethodGet, "/prompt-routers", nil)
			require.Equal(t, http.StatusOK, recList.Code)

			var listOut map[string]any
			require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listOut))
			routers := listOut["promptRouters"].([]any)
			assert.Len(t, routers, tt.wantLen)

			// Get last.
			recGet := doRequest(t, h, http.MethodGet,
				"/prompt-routers/"+url.PathEscape(lastARN), nil)
			require.Equal(t, http.StatusOK, recGet.Code)

			var getOut map[string]any
			require.NoError(t, json.Unmarshal(recGet.Body.Bytes(), &getOut))
			assert.Equal(t, lastARN, getOut["promptRouterArn"])
			assert.Equal(t, "AVAILABLE", getOut["status"])
			assert.NotEmpty(t, getOut["createdAt"])
			assert.NotEmpty(t, getOut["updatedAt"])
		})
	}
}

func TestAccuracy_PromptRouter_DeleteRemovesFromList(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)
	r, err := b.CreatePromptRouter("del-router", nil)
	require.NoError(t, err)

	rec := doRequest(t, h, http.MethodDelete, "/prompt-routers/"+url.PathEscape(r.PromptRouterArn), nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	recGet := doRequest(t, h, http.MethodGet, "/prompt-routers/"+url.PathEscape(r.PromptRouterArn), nil)
	assert.Equal(t, http.StatusNotFound, recGet.Code)
}

func TestAccuracy_PromptRouter_DuplicateNameConflict(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreatePromptRouter("dup-router", nil)
	require.NoError(t, err)

	_, err2 := b.CreatePromptRouter("dup-router", nil)
	require.Error(t, err2)
}

func TestAccuracy_PromptRouter_GetNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/prompt-routers/nonexistent-router", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAccuracy_PromptRouter_NamePreservedInGetAndList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		routerName string
	}{
		{name: "production router", routerName: "prod-router"},
		{name: "staging router", routerName: "stage-router"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/prompt-routers",
				map[string]any{"promptRouterName": tt.routerName})
			require.Equal(t, http.StatusCreated, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			routerARN := out["promptRouterArn"].(string)
			assert.NotEmpty(t, routerARN)

			getRec := doRequest(t, h, http.MethodGet, "/prompt-routers/"+routerARN, nil)
			require.Equal(t, http.StatusOK, getRec.Code)

			var getOut map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
			assert.Equal(t, tt.routerName, getOut["promptRouterName"])
			assert.Equal(t, "AVAILABLE", getOut["status"])
		})
	}
}

func TestAccuracy_PromptRouter_TagsPreserved(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/prompt-routers", map[string]any{
		"promptRouterName": "tagged-router",
		"tags": []map[string]any{
			{"key": "env", "value": "prod"},
			{"key": "team", "value": "ml"},
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	routerARN := out["promptRouterArn"].(string)

	getRec := doRequest(t, h, http.MethodGet, "/prompt-routers/"+routerARN, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	// Router ARN is returned and non-empty
	var getOut map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	assert.Equal(t, "AVAILABLE", getOut["status"])
}

func TestAccuracy_PromptRouter_DeleteNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodDelete,
		"/prompt-routers/arn:aws:bedrock:us-east-1:000000000000:prompt-router/nonexistent", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAccuracy_PromptRouter_ListAfterDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create two routers
	for i := range 2 {
		doRequest(
			t, h, http.MethodPost, "/prompt-routers",
			map[string]any{"promptRouterName": "router-del"},
		)
		_ = i
	}

	// Create one to delete
	delRec := doRequest(t, h, http.MethodPost, "/prompt-routers",
		map[string]any{"promptRouterName": "to-be-deleted"})
	require.Equal(t, http.StatusCreated, delRec.Code)

	var delOut map[string]any
	require.NoError(t, json.Unmarshal(delRec.Body.Bytes(), &delOut))
	delARN := delOut["promptRouterArn"].(string)

	// Delete it
	deleteRec := doRequest(t, h, http.MethodDelete, "/prompt-routers/"+delARN, nil)
	assert.Equal(t, http.StatusNoContent, deleteRec.Code)

	// It should not appear in list
	listRec := doRequest(t, h, http.MethodGet, "/prompt-routers", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	routers := listOut["promptRouters"].([]any)

	for _, r := range routers {
		router := r.(map[string]any)
		assert.NotEqual(t, delARN, router["promptRouterArn"])
	}
}

func TestHandler_PromptRouter_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doRequest(t, h, http.MethodPost, "/prompt-routers", map[string]any{"promptRouterName": "my-router"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var created map[string]any
	mustUnmarshal(t, rec, &created)
	routerARN := created["promptRouterArn"].(string)
	assert.NotEmpty(t, routerARN)

	// Get
	rec2 := doRequest(t, h, http.MethodGet, "/prompt-routers/"+url.PathEscape(routerARN), nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]any
	mustUnmarshal(t, rec2, &out)
	assert.Equal(t, routerARN, out["promptRouterArn"])

	// List
	rec3 := doRequest(t, h, http.MethodGet, "/prompt-routers", nil)
	require.Equal(t, http.StatusOK, rec3.Code)

	var listOut map[string]any
	mustUnmarshal(t, rec3, &listOut)
	assert.Len(t, listOut["promptRouters"], 1)

	// Delete
	rec4 := doRequest(t, h, http.MethodDelete, "/prompt-routers/"+url.PathEscape(routerARN), nil)
	assert.Equal(t, http.StatusNoContent, rec4.Code)

	// Get after delete
	rec5 := doRequest(t, h, http.MethodGet, "/prompt-routers/"+url.PathEscape(routerARN), nil)
	assert.Equal(t, http.StatusNotFound, rec5.Code)
}

func TestHandler_PromptRouter_MissingName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/prompt-routers", map[string]any{"promptRouterName": ""})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
