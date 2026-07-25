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

// validPromptRouterBody returns a CreatePromptRouter request body with every
// real-AWS-required field populated (fallbackModel, models, routingCriteria)
// alongside promptRouterName.
func validPromptRouterBody(name string) map[string]any {
	claudeV2 := "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-v2"
	claudeInstant := "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-instant-v1"

	return map[string]any{
		"promptRouterName": name,
		"fallbackModel":    map[string]any{"modelArn": claudeV2},
		"models": []map[string]any{
			{"modelArn": claudeV2},
			{"modelArn": claudeInstant},
		},
		"routingCriteria": map[string]any{"responseQualityDifference": 0.5},
	}
}

func createTestPromptRouter(
	t *testing.T, b *bedrock.InMemoryBackend, name string,
) *bedrock.PromptRouter {
	t.Helper()

	r, err := b.CreatePromptRouter(
		name, "", "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-v2",
		[]string{"arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-v2"},
		0.5, nil,
	)
	require.NoError(t, err)

	return r
}

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
			body := validPromptRouterBody(tt.routerName)
			rec := doRequest(t, h, http.MethodPost, "/prompt-routers", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantRouterID {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotEmpty(t, out["promptRouterArn"])
			}
		})
	}
}

// TestAccuracy_PromptRouter_CreateRequiresFallbackModelAndModels locks in that
// fallbackModel and models are required (real AWS: CreatePromptRouterInput
// marks both "This member is required") -- gopherstack previously accepted a
// bare {"promptRouterName": ...} body and silently dropped both.
func TestAccuracy_PromptRouter_CreateRequiresFallbackModelAndModels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "missing fallbackModel",
			body: map[string]any{
				"promptRouterName": "r1",
				"models":           []map[string]any{{"modelArn": "m"}},
				"routingCriteria":  map[string]any{"responseQualityDifference": 0.5},
			},
		},
		{
			name: "missing models",
			body: map[string]any{
				"promptRouterName": "r2",
				"fallbackModel":    map[string]any{"modelArn": "m"},
				"routingCriteria":  map[string]any{"responseQualityDifference": 0.5},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/prompt-routers", tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
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
				r := createTestPromptRouter(t, b, name)
				lastARN = r.PromptRouterArn
			}

			// List.
			recList := doRequest(t, h, http.MethodGet, "/prompt-routers", nil)
			require.Equal(t, http.StatusOK, recList.Code)

			var listOut map[string]any
			require.NoError(t, json.Unmarshal(recList.Body.Bytes(), &listOut))
			routers := listOut["promptRouterSummaries"].([]any)
			assert.Len(t, routers, tt.wantLen)

			// Get last.
			recGet := doRequest(t, h, http.MethodGet,
				"/prompt-routers/"+url.PathEscape(lastARN), nil)
			require.Equal(t, http.StatusOK, recGet.Code)

			var getOut map[string]any
			require.NoError(t, json.Unmarshal(recGet.Body.Bytes(), &getOut))
			assert.Equal(t, lastARN, getOut["promptRouterArn"])
			assert.Equal(t, "AVAILABLE", getOut["status"])
			assert.Equal(t, "custom", getOut["type"])
			assert.NotEmpty(t, getOut["fallbackModel"])
			assert.NotEmpty(t, getOut["models"])
			assert.NotEmpty(t, getOut["routingCriteria"])
			assert.NotEmpty(t, getOut["createdAt"])
			assert.NotEmpty(t, getOut["updatedAt"])
		})
	}
}

func TestAccuracy_PromptRouter_DeleteRemovesFromList(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrock.NewHandler(b)
	r := createTestPromptRouter(t, b, "del-router")

	rec := doRequest(t, h, http.MethodDelete, "/prompt-routers/"+url.PathEscape(r.PromptRouterArn), nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	recGet := doRequest(t, h, http.MethodGet, "/prompt-routers/"+url.PathEscape(r.PromptRouterArn), nil)
	assert.Equal(t, http.StatusNotFound, recGet.Code)
}

func TestAccuracy_PromptRouter_DuplicateNameConflict(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend("000000000000", "us-east-1")
	createTestPromptRouter(t, b, "dup-router")

	_, err2 := b.CreatePromptRouter(
		"dup-router", "", "arn:model", []string{"arn:model"}, 0.5, nil,
	)
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
			rec := doRequest(t, h, http.MethodPost, "/prompt-routers", validPromptRouterBody(tt.routerName))
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
	body := validPromptRouterBody("tagged-router")
	body["tags"] = []map[string]any{
		{"key": "env", "value": "prod"},
		{"key": "team", "value": "ml"},
	}
	rec := doRequest(t, h, http.MethodPost, "/prompt-routers", body)
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
	for i, name := range []string{"router-del-1", "router-del-2"} {
		rec := doRequest(t, h, http.MethodPost, "/prompt-routers", validPromptRouterBody(name))
		require.Equal(t, http.StatusCreated, rec.Code, "router %d", i)
	}

	// Create one to delete
	delRec := doRequest(t, h, http.MethodPost, "/prompt-routers", validPromptRouterBody("to-be-deleted"))
	require.Equal(t, http.StatusCreated, delRec.Code)

	var delOut map[string]any
	require.NoError(t, json.Unmarshal(delRec.Body.Bytes(), &delOut))
	delARN := delOut["promptRouterArn"].(string)

	// Delete it
	deleteRec := doRequest(t, h, http.MethodDelete, "/prompt-routers/"+delARN, nil)
	assert.Equal(t, http.StatusOK, deleteRec.Code)

	// It should not appear in list
	listRec := doRequest(t, h, http.MethodGet, "/prompt-routers", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	routers := listOut["promptRouterSummaries"].([]any)

	for _, r := range routers {
		router := r.(map[string]any)
		assert.NotEqual(t, delARN, router["promptRouterArn"])
	}
}

// TestAccuracy_PromptRouter_ListFilterByType locks in the typeEquals query
// filter and nextToken pagination that ListPromptRouters previously ignored
// (it took zero params and always returned every router unfiltered).
func TestAccuracy_PromptRouter_ListFilterByType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/prompt-routers", validPromptRouterBody("custom-router"))

	recCustom := doRequest(t, h, http.MethodGet, "/prompt-routers?type=custom", nil)
	require.Equal(t, http.StatusOK, recCustom.Code)

	var outCustom map[string]any
	mustUnmarshal(t, recCustom, &outCustom)
	assert.Len(t, outCustom["promptRouterSummaries"], 1)

	recDefault := doRequest(t, h, http.MethodGet, "/prompt-routers?type=default", nil)
	require.Equal(t, http.StatusOK, recDefault.Code)

	var outDefault map[string]any
	mustUnmarshal(t, recDefault, &outDefault)
	assert.Empty(t, outDefault["promptRouterSummaries"])
}

func TestHandler_PromptRouter_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doRequest(t, h, http.MethodPost, "/prompt-routers", validPromptRouterBody("my-router"))
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
	assert.Len(t, listOut["promptRouterSummaries"], 1)

	// Delete
	rec4 := doRequest(t, h, http.MethodDelete, "/prompt-routers/"+url.PathEscape(routerARN), nil)
	assert.Equal(t, http.StatusOK, rec4.Code)

	// Get after delete
	rec5 := doRequest(t, h, http.MethodGet, "/prompt-routers/"+url.PathEscape(routerARN), nil)
	assert.Equal(t, http.StatusNotFound, rec5.Code)
}

func TestHandler_PromptRouter_MissingName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/prompt-routers", validPromptRouterBody(""))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
