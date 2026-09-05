package opensearch_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

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

func TestOpenSearchHandler_ListTags_UnknownARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	unknownARN := "arn:aws:es:us-east-1:123456789012:domain/nonexistent"
	resp := doRequest(t, h, http.MethodGet, "/2021-01-01/tags?arn="+unknownARN, nil)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	tagList, ok := out["TagList"].([]any)
	require.True(t, ok)
	assert.Empty(t, tagList)
}

func TestTagRoutes_InvalidBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
	}{
		{name: "add_tags", path: "/2021-01-01/tags"},
		{name: "remove_tags", path: "/2021-01-01/tags-removal"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			req := httptest.NewRequest(http.MethodPost, tt.path, strings.NewReader("not-json"))
			req.Header.Set("Content-Type", "application/json")

			rw := httptest.NewRecorder()
			h.ServeHTTP(rw, req)

			assert.Equal(t, http.StatusBadRequest, rw.Code)
		})
	}
}

// TestTagRoutes_UnknownARN covers AddTags/RemoveTags with an ARN that names
// no existing resource. Neither op's own deserializer (opensearch@v1.75.4
// deserializers.go) models ResourceNotFoundException -- this is
// ValidationException (400), unlike most other domain-scoped ops in this
// service.
func TestTagRoutes_UnknownARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
		path string
	}{
		{
			name: "add_tags",
			path: "/2021-01-01/tags",
			body: map[string]any{
				"ARN":     "arn:aws:es:us-east-1:123456789012:domain/nonexistent",
				"TagList": []map[string]string{{"Key": "k", "Value": "v"}},
			},
		},
		{
			name: "remove_tags",
			path: "/2021-01-01/tags-removal",
			body: map[string]any{
				"ARN":     "arn:aws:es:us-east-1:123456789012:domain/nonexistent",
				"TagKeys": []string{"env"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			resp := doRequest(t, h, http.MethodPost, tt.path, tt.body)
			defer resp.Body.Close()

			assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
		})
	}
}
