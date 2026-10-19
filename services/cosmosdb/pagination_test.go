package cosmosdb_test

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pageIDs walks every page of a feed, returning each page's ids and asserting the continuation protocol.
func pageIDs(
	t *testing.T,
	doRequestFn func(headers map[string]string) (int, map[string]any, http.Header),
	listKey string,
	size int,
) [][]string {
	t.Helper()

	var pages [][]string

	token := ""

	for range 20 {
		headers := map[string]string{"X-Ms-Max-Item-Count": strconv.Itoa(size)}
		if token != "" {
			headers["X-Ms-Continuation"] = token
		}

		code, body, hdr := doRequestFn(headers)
		require.Equal(t, http.StatusOK, code)

		items, ok := body[listKey].([]any)
		require.True(t, ok)

		ids := make([]string, 0, len(items))

		for _, it := range items {
			if m, isMap := it.(map[string]any); isMap {
				id, _ := m["id"].(string)
				ids = append(ids, id)
			} else if s, isStr := it.(string); isStr {
				ids = append(ids, s)
			}
		}

		pages = append(pages, ids)
		assert.Equal(t, strconv.Itoa(len(ids)), hdr.Get("X-Ms-Item-Count"))

		token = hdr.Get("X-Ms-Continuation")
		if token == "" {
			return pages
		}
	}

	require.Fail(t, "continuation never terminated")

	return pages
}

func TestPagination_Feeds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		method  string
		path    string
		body    string
		listKey string
		headers map[string]string
		want    [][]string
		size    int
	}{
		{
			name:    "databases",
			path:    "/dbs",
			listKey: "Databases",
			size:    2,
			want:    [][]string{{"d0", "d1"}, {"d2", "d3"}, {"d4"}},
		},
		{
			name: "containers", path: "/dbs/d0/colls", listKey: "DocumentCollections", size: 2,
			want: [][]string{{"c0", "c1"}, {"c2"}},
		},
		{
			name: "documents", path: "/dbs/d0/colls/c0/docs", listKey: "Documents", size: 3,
			want: [][]string{{"k0", "k1", "k2"}, {"k3", "k4"}},
		},
		{
			name:    "query ordered",
			method:  http.MethodPost,
			path:    "/dbs/d0/colls/c0/docs",
			listKey: "Documents",
			size:    2,
			body:    `{"query":"SELECT c.id FROM c ORDER BY c.id DESC"}`,
			headers: map[string]string{"X-Ms-Documentdb-Isquery": "true", "Content-Type": "application/query+json"},
			want:    [][]string{{"k4", "k3"}, {"k2", "k1"}, {"k0"}},
		},
		{
			name: "query value", method: http.MethodPost, path: "/dbs/d0/colls/c0/docs", listKey: "Documents", size: 4,
			body:    `{"query":"SELECT VALUE c.id FROM c"}`,
			headers: map[string]string{"X-Ms-Documentdb-Isquery": "true", "Content-Type": "application/query+json"},
			want:    [][]string{{"k0", "k1", "k2", "k3"}, {"k4"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for i := range 5 {
				rec := doRequest(t, h, http.MethodPost, "/dbs", nil, fmt.Appendf(nil, `{"id":"d%d"}`, i))
				require.Equal(t, http.StatusCreated, rec.Code)
			}

			for i := range 3 {
				rec := doRequest(t, h, http.MethodPost, "/dbs/d0/colls", nil,
					fmt.Appendf(nil, `{"id":"c%d","partitionKey":{"paths":["/pk"]}}`, i))
				require.Equal(t, http.StatusCreated, rec.Code)
			}

			for i := range 5 {
				rec := doRequest(t, h, http.MethodPost, "/dbs/d0/colls/c0/docs", nil,
					fmt.Appendf(nil, `{"id":"k%d","pk":"p"}`, i))
				require.Equal(t, http.StatusCreated, rec.Code)
			}

			method := tt.method
			if method == "" {
				method = http.MethodGet
			}

			fetch := func(extra map[string]string) (int, map[string]any, http.Header) {
				headers := map[string]string{}
				maps.Copy(headers, tt.headers)
				maps.Copy(headers, extra)

				var body []byte
				if tt.body != "" {
					body = []byte(tt.body)
				}

				rec := doRequest(t, h, method, tt.path, headers, body)

				var decoded map[string]any
				_ = json.Unmarshal(rec.Body.Bytes(), &decoded)

				return rec.Code, decoded, rec.Header()
			}

			assert.Equal(t, tt.want, pageIDs(t, fetch, tt.listKey, tt.size))
		})
	}
}

func TestPagination_DefaultIsUnbounded(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 3 {
		doRequest(t, h, http.MethodPost, "/dbs", nil, fmt.Appendf(nil, `{"id":"d%d"}`, i))
	}

	rec := doRequest(t, h, http.MethodGet, "/dbs", nil, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Empty(t, rec.Header().Get("X-Ms-Continuation"))
	assert.InDelta(t, 3, decodeBody(t, rec)["_count"], 0)
}

func TestPagination_InvalidContinuation(t *testing.T) {
	t.Parallel()

	paths := []string{"/dbs", "/dbs/d0/colls", "/dbs/d0/colls/c0/docs"}

	for _, p := range paths {
		t.Run(p, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodGet, p, map[string]string{"X-Ms-Continuation": "!!not-a-token!!"}, nil)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}
