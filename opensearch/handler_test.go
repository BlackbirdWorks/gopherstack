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

func TestCreateDomain(t *testing.T) {
	t.Parallel()
	h := newTestHandler()

	resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain", map[string]any{
		"DomainName":    "test-domain",
		"EngineVersion": "OpenSearch_2.11",
	})
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	status, ok := out["DomainStatus"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "test-domain", status["DomainName"])
	assert.Equal(t, "OpenSearch_2.11", status["EngineVersion"])
	assert.NotEmpty(t, status["ARN"])
	assert.NotEmpty(t, status["Endpoint"])
}

func TestCreateDomain_AlreadyExists(t *testing.T) {
	t.Parallel()
	h := newTestHandler()

	body := map[string]any{"DomainName": "my-domain"}
	resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain", body)
	resp.Body.Close()

	resp2 := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain", body)
	defer resp2.Body.Close()

	assert.Equal(t, http.StatusConflict, resp2.StatusCode)
}

func TestCreateDomain_NoName(t *testing.T) {
	t.Parallel()
	h := newTestHandler()

	resp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain", map[string]any{})
	defer resp.Body.Close()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestDescribeDomain(t *testing.T) {
	t.Parallel()
	h := newTestHandler()

	// Create first
	createResp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain", map[string]any{
		"DomainName": "my-domain",
	})
	createResp.Body.Close()

	resp := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/domain/my-domain", nil)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	status, ok := out["DomainStatus"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "my-domain", status["DomainName"])
}

func TestDescribeDomain_NotFound(t *testing.T) {
	t.Parallel()
	h := newTestHandler()

	resp := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/domain/nonexistent", nil)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestListDomainNames(t *testing.T) {
	t.Parallel()
	h := newTestHandler()

	// Create two domains
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

func TestDeleteDomain(t *testing.T) {
	t.Parallel()
	h := newTestHandler()

	// Create
	r := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain", map[string]any{
		"DomainName": "to-delete",
	})
	r.Body.Close()

	// Delete
	resp := doRequest(t, h, http.MethodDelete, "/2021-01-01/opensearch/domain/to-delete", nil)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Confirm gone
	resp2 := doRequest(t, h, http.MethodGet, "/2021-01-01/opensearch/domain/to-delete", nil)
	defer resp2.Body.Close()

	assert.Equal(t, http.StatusNotFound, resp2.StatusCode)
}

func TestDeleteDomain_NotFound(t *testing.T) {
	t.Parallel()
	h := newTestHandler()

	resp := doRequest(t, h, http.MethodDelete, "/2021-01-01/opensearch/domain/nonexistent", nil)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestAddTags(t *testing.T) {
	t.Parallel()
	h := newTestHandler()

	// Create a domain to get its ARN
	createResp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain", map[string]any{
		"DomainName": "tag-domain",
	})

	var createOut map[string]any
	require.NoError(t, json.NewDecoder(createResp.Body).Decode(&createOut))
	createResp.Body.Close()

	status := createOut["DomainStatus"].(map[string]any)
	domainARN := status["ARN"].(string)

	// Add tags
	resp := doRequest(t, h, http.MethodPost, "/2021-01-01/tags", map[string]any{
		"ARN": domainARN,
		"TagList": []map[string]string{
			{"Key": "env", "Value": "prod"},
			{"Key": "team", "Value": "platform"},
		},
	})
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// List tags
	listResp := doRequest(t, h, http.MethodGet, "/2021-01-01/tags?arn="+domainARN, nil)
	defer listResp.Body.Close()

	var listOut map[string]any
	require.NoError(t, json.NewDecoder(listResp.Body).Decode(&listOut))

	tagList, ok := listOut["TagList"].([]any)
	require.True(t, ok)
	assert.Len(t, tagList, 2)
}

func TestRemoveTags(t *testing.T) {
	t.Parallel()
	h := newTestHandler()

	// Create domain
	createResp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain", map[string]any{
		"DomainName": "remove-tag-domain",
	})

	var createOut map[string]any
	require.NoError(t, json.NewDecoder(createResp.Body).Decode(&createOut))
	createResp.Body.Close()

	status := createOut["DomainStatus"].(map[string]any)
	domainARN := status["ARN"].(string)

	// Add tags
	addResp := doRequest(t, h, http.MethodPost, "/2021-01-01/tags", map[string]any{
		"ARN": domainARN,
		"TagList": []map[string]string{
			{"Key": "env", "Value": "prod"},
			{"Key": "team", "Value": "platform"},
		},
	})
	addResp.Body.Close()

	// Remove one tag
	removeResp := doRequest(t, h, http.MethodPost, "/2021-01-01/tags-removal", map[string]any{
		"ARN":     domainARN,
		"TagKeys": []string{"env"},
	})
	defer removeResp.Body.Close()

	assert.Equal(t, http.StatusOK, removeResp.StatusCode)

	// List tags — only "team" should remain
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

func TestListTags_EmptyDomain(t *testing.T) {
	t.Parallel()
	h := newTestHandler()

	// Create domain with no tags
	createResp := doRequest(t, h, http.MethodPost, "/2021-01-01/opensearch/domain", map[string]any{
		"DomainName": "empty-tags-domain",
	})

	var createOut map[string]any
	require.NoError(t, json.NewDecoder(createResp.Body).Decode(&createOut))
	createResp.Body.Close()

	status := createOut["DomainStatus"].(map[string]any)
	domainARN := status["ARN"].(string)

	// List tags on new domain — should be empty
	resp := doRequest(t, h, http.MethodGet, "/2021-01-01/tags?arn="+domainARN, nil)
	defer resp.Body.Close()

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))

	tagList, ok := out["TagList"].([]any)
	require.True(t, ok)
	assert.Empty(t, tagList)
}
