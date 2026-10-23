package azureappconfig_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const av = "api-version=2023-11-01"

func readJSON(t *testing.T, resp *http.Response) map[string]any {
	t.Helper()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var out map[string]any

	require.NoError(t, json.Unmarshal(raw, &out), string(raw))

	return out
}

func TestHTTP_StatusAndHeaders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		hdr    map[string]string
		name   string
		method string
		path   string
		body   string
		status int
	}{
		{name: "missing api-version", method: http.MethodGet, path: "/kv", status: http.StatusBadRequest},
		{name: "unsupported api-version", method: http.MethodGet, path: "/kv?api-version=1999", status: 400},
		{name: "list empty", method: http.MethodGet, path: "/kv?" + av, status: http.StatusOK},
		{name: "get missing", method: http.MethodGet, path: "/kv/nope?" + av, status: http.StatusNotFound},
		{name: "delete missing", method: http.MethodDelete, path: "/kv/nope?" + av, status: http.StatusNoContent},
		{name: "unknown route", method: http.MethodGet, path: "/nothing?" + av, status: http.StatusNotFound},
		{name: "post kv", method: http.MethodPost, path: "/kv/a?" + av, status: http.StatusBadRequest},
		{name: "bad select", method: http.MethodGet, path: "/kv?$select=bogus&" + av, status: http.StatusBadRequest},
		{name: "bad json", method: http.MethodPut, path: "/kv/a?" + av, body: "{", status: http.StatusBadRequest},
		{name: "dot key", method: http.MethodPut, path: "/kv/..?" + av, body: "{}", status: http.StatusBadRequest},
		{name: "too many filters", method: http.MethodGet, path: "/kv?key=a,b,c,d,e,f&" + av, status: 400},
		{name: "bad accept-datetime", method: http.MethodGet, path: "/kv/a?" + av, status: http.StatusBadRequest,
			hdr: map[string]string{"Accept-Datetime": "yesterday"}},
		{
			name:   "operations unknown snapshot",
			method: http.MethodGet,
			path:   "/operations?snapshot=x&" + av,
			status: 404,
		},
		{
			name:   "snapshot without filters",
			method: http.MethodPut,
			path:   "/snapshots/s1?" + av,
			body:   "{}",
			status: 400,
		},
	}

	e := newEnv(t, envOpts{})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp := e.raw(t, tt.method, tt.path, tt.body, tt.hdr)
			assert.Equal(t, tt.status, resp.StatusCode)
			assert.Regexp(t, `^[^=]+=[^;]+;sn=\d+$`, resp.Header.Get("Sync-Token"))
		})
	}
}

func TestHTTP_KeyValueWire(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})

	put := e.raw(t, http.MethodPut, "/kv/app%2Fname?label=prod&"+av,
		`{"value":"v","content_type":"text/plain","tags":{"a":"b"}}`, nil)
	require.Equal(t, http.StatusOK, put.StatusCode)

	etag := strings.Trim(put.Header.Get("ETag"), `"`)
	require.NotEmpty(t, etag)

	body := readJSON(t, put)
	assert.Equal(t, "app/name", body["key"])
	assert.Equal(t, "prod", body["label"])
	assert.Equal(t, etag, body["etag"])
	assert.Equal(t, false, body["locked"])
	assert.Regexp(t, `^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$`, body["last_modified"])

	tests := []struct {
		hdr    map[string]string
		name   string
		method string
		path   string
		status int
	}{
		{name: "get encoded slash", method: http.MethodGet, path: "/kv/app%2Fname?label=prod&" + av, status: 200},
		{name: "get raw slash", method: http.MethodGet, path: "/kv/app/name?label=prod&" + av, status: 200},
		{name: "head", method: http.MethodHead, path: "/kv/app%2Fname?label=prod&" + av, status: 200},
		{name: "wrong label", method: http.MethodGet, path: "/kv/app%2Fname?label=dev&" + av, status: 404},
		{name: "null label is distinct", method: http.MethodGet, path: "/kv/app%2Fname?label=%00&" + av, status: 404},
		{name: "if-none-match current", method: http.MethodGet, path: "/kv/app%2Fname?label=prod&" + av,
			hdr: map[string]string{"If-None-Match": `"` + etag + `"`}, status: http.StatusNotModified},
		{name: "if-match stale", method: http.MethodGet, path: "/kv/app%2Fname?label=prod&" + av,
			hdr: map[string]string{"If-Match": `"stale"`}, status: http.StatusPreconditionFailed},
		{name: "if-match star", method: http.MethodGet, path: "/kv/app%2Fname?label=prod&" + av,
			hdr: map[string]string{"If-Match": "*"}, status: 200},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp := e.raw(t, tt.method, tt.path, "", tt.hdr)
			assert.Equal(t, tt.status, resp.StatusCode)

			if tt.status == http.StatusOK && tt.method == http.MethodGet {
				assert.Equal(t, `"`+etag+`"`, resp.Header.Get("ETag"))
			}
		})
	}
}

func TestHTTP_SelectKeysLabelsAndPaging(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})

	for i := range 130 {
		path := "/kv/k" + url.PathEscape(string(rune('a'+i/26))+string(rune('a'+i%26))) + "?label=l" +
			string(rune('0'+i%3)) + "&" + av
		require.Equal(t, http.StatusOK, e.raw(t, http.MethodPut, path, `{"value":"v"}`, nil).StatusCode)
	}

	t.Run("select", func(t *testing.T) {
		t.Parallel()

		resp := e.raw(t, http.MethodGet, "/kv?$select=key,value&key=kaa&"+av, "", nil)
		require.Equal(t, http.StatusOK, resp.StatusCode)

		items, _ := readJSON(t, resp)["items"].([]any)
		require.Len(t, items, 1)
		assert.Equal(t, map[string]any{"key": "kaa", "value": "v"}, items[0])
	})

	t.Run("link paging", func(t *testing.T) {
		t.Parallel()

		resp := e.raw(t, http.MethodGet, "/kv?"+av, "", nil)
		first := readJSON(t, resp)
		assert.Len(t, first["items"], 100)

		link := resp.Header.Get("Link")
		require.Contains(t, link, `rel="next"`)
		assert.Equal(t, "<"+first["@nextLink"].(string)+`>; rel="next"`, link)

		next := e.raw(t, http.MethodGet, first["@nextLink"].(string), "", nil)
		second := readJSON(t, next)
		assert.Len(t, second["items"], 30)
		assert.Empty(t, next.Header.Get("Link"))
	})

	t.Run("keys", func(t *testing.T) {
		t.Parallel()

		resp := e.raw(t, http.MethodGet, "/keys?name=kb*&"+av, "", nil)
		items, _ := readJSON(t, resp)["items"].([]any)
		assert.Len(t, items, 26)
		assert.Equal(t, map[string]any{"name": "kba"}, items[0])
	})

	t.Run("labels", func(t *testing.T) {
		t.Parallel()

		resp := e.raw(t, http.MethodGet, "/labels?"+av, "", nil)
		items, _ := readJSON(t, resp)["items"].([]any)
		assert.Equal(t, []any{
			map[string]any{"name": "l0"}, map[string]any{"name": "l1"}, map[string]any{"name": "l2"},
		}, items)
	})
}

func TestHTTP_StoreResolution(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})

	require.Equal(t, http.StatusOK,
		e.raw(t, http.MethodPut, "/stores/other-store/kv/x?"+av, `{"value":"1"}`, nil).StatusCode)

	tests := []struct {
		name   string
		path   string
		status int
	}{
		{name: "prefix store sees its key", path: "/stores/other-store/kv/x?" + av, status: 200},
		{name: "default store does not", path: "/kv/x?" + av, status: 404},
		{name: "teststore does not", path: "/stores/teststore/kv/x?" + av, status: 404},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.status, e.raw(t, http.MethodGet, tt.path, "", nil).StatusCode)
		})
	}
}
