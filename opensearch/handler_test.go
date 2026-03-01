package opensearch_test

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/opensearch"
)

func newTestHandler() *opensearch.Handler {
	bk := opensearch.NewInMemoryBackend("123456789012", "us-east-1")

	return opensearch.NewHandler(bk, slog.Default())
}

func doRequest(t *testing.T, h *opensearch.Handler, method, path string, body any) *http.Response {
	t.Helper()

	var reqBody io.Reader

	if body != nil {
		b, err := json.Marshal(body)
		require.NoError(t, err)

		reqBody = bytes.NewReader(b)
	}

	req := httptest.NewRequest(method, path, reqBody)

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	rw := httptest.NewRecorder()
	h.ServeHTTP(rw, req)

	return rw.Result()
}

func TestOpenSearchHandler_CreateDomain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		setup         func(t *testing.T, h *opensearch.Handler)
		domainName    string
		engineVersion string
		wantCode      int
		wantContains  []string
	}{
		{
			name:          "success",
			domainName:    "test-domain",
			engineVersion: "OpenSearch_2.11",
			wantCode:      http.StatusOK,
			wantContains:  []string{"test-domain", "OpenSearch_2.11", "ARN", "Endpoint"},
		},
		{
			name:       "already_exists",
			domainName: "my-domain",
			setup: func(t *testing.T, h *opensearch.Handler) {
				resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain", map[string]any{"DomainName": "my-domain"})
				resp.Body.Close()
			},
			wantCode: http.StatusConflict,
		},
		{
			name:     "no_name",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}
			body := map[string]any{}
			if tt.domainName != "" {
				body["DomainName"] = tt.domainName
			}
			if tt.engineVersion != "" {
				body["EngineVersion"] = tt.engineVersion
			}
			resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain", body)
			defer resp.Body.Close()
			assert.Equal(t, tt.wantCode, resp.StatusCode)
			if len(tt.wantContains) > 0 {
				bodyBytes, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				for _, s := range tt.wantContains {
					assert.Contains(t, string(bodyBytes), s)
				}
			}
		})
	}
}

func TestOpenSearchHandler_DescribeDomain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		setup        func(t *testing.T, h *opensearch.Handler)
		domainName   string
		wantCode     int
		wantContains []string
	}{
		{
			name:       "success",
			domainName: "my-domain",
			setup: func(t *testing.T, h *opensearch.Handler) {
				resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain", map[string]any{"DomainName": "my-domain"})
				resp.Body.Close()
			},
			wantCode:     http.StatusOK,
			wantContains: []string{"my-domain"},
		},
		{
			name:       "not_found",
			domainName: "nonexistent",
			wantCode:   http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}
			resp := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/domain/"+tt.domainName, nil)
			defer resp.Body.Close()
			assert.Equal(t, tt.wantCode, resp.StatusCode)
			if len(tt.wantContains) > 0 {
				bodyBytes, err := io.ReadAll(resp.Body)
				require.NoError(t, err)
				for _, s := range tt.wantContains {
					assert.Contains(t, string(bodyBytes), s)
				}
			}
		})
	}
}

func TestOpenSearchHandler_ListDomainNames(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	for _, name := range []string{"alpha", "beta"} {
		r := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain", map[string]any{
			"DomainName": name,
		})
		r.Body.Close()
	}

	resp := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/domain", nil)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	names, ok := out["DomainNames"].([]any)
	require.True(t, ok)
	assert.Len(t, names, 2)
}

func TestOpenSearchHandler_DeleteDomain(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *opensearch.Handler)
		domainName string
		wantCode   int
	}{
		{
			name:       "success",
			domainName: "to-delete",
			setup: func(t *testing.T, h *opensearch.Handler) {
				r := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain", map[string]any{"DomainName": "to-delete"})
				r.Body.Close()
			},
			wantCode: http.StatusOK,
		},
		{
			name:       "not_found",
			domainName: "nonexistent",
			wantCode:   http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			if tt.setup != nil {
				tt.setup(t, h)
			}
			resp := doRequest(t, h, http.MethodDelete, "/2021-01-01/opensearch/domain/"+tt.domainName, nil)
			defer resp.Body.Close()
			assert.Equal(t, tt.wantCode, resp.StatusCode)

			if tt.wantCode == http.StatusOK {
				resp2 := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/domain/"+tt.domainName, nil)
				defer resp2.Body.Close()
				assert.Equal(t, http.StatusNotFound, resp2.StatusCode)
			}
		})
	}
}

func createDomainAndGetARN(t *testing.T, h *opensearch.Handler, domainName string) string {
	t.Helper()

	createResp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain", map[string]any{
		"DomainName": domainName,
	})

	var createOut map[string]any
	require.NoError(t, json.NewDecoder(createResp.Body).Decode(&createOut))
	createResp.Body.Close()

	status := createOut["DomainStatus"].(map[string]any)

	return status["ARN"].(string)
}

func TestOpenSearchHandler_AddTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	domainARN := createDomainAndGetARN(t, h, "tag-domain")

	resp := doRequest(t, h, http.MethodPost, "/2021-01-01/tags", map[string]any{
		"ARN": domainARN,
		"TagList": []map[string]string{
			{"Key": "env", "Value": "prod"},
			{"Key": "team", "Value": "platform"},
		},
	})
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	listResp := doRequest(t, h, http.MethodGet, "/2021-01-01/tags?arn="+domainARN, nil)
	defer listResp.Body.Close()

	var listOut map[string]any
	require.NoError(t, json.NewDecoder(listResp.Body).Decode(&listOut))

	tagList, ok := listOut["TagList"].([]any)
	require.True(t, ok)
	assert.Len(t, tagList, 2)
}

func TestOpenSearchHandler_RemoveTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	domainARN := createDomainAndGetARN(t, h, "remove-tag-domain")

	addResp := doRequest(t, h, http.MethodPost, "/2021-01-01/tags", map[string]any{
		"ARN": domainARN,
		"TagList": []map[string]string{
			{"Key": "env", "Value": "prod"},
			{"Key": "team", "Value": "platform"},
		},
	})
	addResp.Body.Close()

	removeResp := doRequest(t, h, http.MethodPost, "/2021-01-01/tags-removal", map[string]any{
		"ARN":     domainARN,
		"TagKeys": []string{"env"},
	})
	defer removeResp.Body.Close()

	assert.Equal(t, http.StatusOK, removeResp.StatusCode)

	listResp := doRequest(t, h, http.MethodGet, "/2021-01-01/tags?arn="+domainARN, nil)
	defer listResp.Body.Close()

	var listOut map[string]any
	require.NoError(t, json.NewDecoder(listResp.Body).Decode(&listOut))

	tagList, ok := listOut["TagList"].([]any)
	require.True(t, ok)
	assert.Len(t, tagList, 1)

	tag := tagList[0].(map[string]any)
	assert.Equal(t, "team", tag["Key"])
	assert.Equal(t, "platform", tag["Value"])
}

func TestOpenSearchHandler_ListTags_EmptyDomain(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	domainARN := createDomainAndGetARN(t, h, "empty-tags-domain")

	resp := doRequest(t, h, http.MethodGet, "/2021-01-01/tags?arn="+domainARN, nil)
	defer resp.Body.Close()

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	tagList, ok := out["TagList"].([]any)
	require.True(t, ok)
	assert.Empty(t, tagList)
}
