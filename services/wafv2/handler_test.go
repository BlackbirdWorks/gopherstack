package wafv2_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/wafv2"
)

func newTestHandler(t *testing.T) *wafv2.Handler {
	t.Helper()

	return wafv2.NewHandler(wafv2.NewInMemoryBackend("000000000000", "us-east-1"))
}

func doWafv2Request(
	t *testing.T,
	h *wafv2.Handler,
	target string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AWSWAF_20190729."+target)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "Wafv2", h.Name())
}

func TestBackend_Region(t *testing.T) {
	t.Parallel()

	b := wafv2.NewInMemoryBackend("000000000000", "eu-west-1")
	assert.Equal(t, "eu-west-1", b.Region())
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name   string
		target string
		want   bool
	}{
		{name: "matching_target", target: "AWSWAF_20190729.ListWebACLs", want: true},
		{name: "non_matching_target", target: "SageMaker.ListModels", want: false},
		{name: "empty_target", target: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, h.RouteMatcher()(c))
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", "AWSWAF_20190729.CreateWebACL")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	assert.Equal(t, "CreateWebACL", h.ExtractResource(c))
}

func TestProvider_InitAndName(t *testing.T) {
	t.Parallel()

	p := &wafv2.Provider{}
	assert.Equal(t, "Wafv2", p.Name())

	h, err := p.Init(&service.AppContext{})
	require.NoError(t, err)
	assert.NotNil(t, h)
}

func TestHandler_CreateWebACL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
		wantID     bool
	}{
		{
			name: "valid",
			body: map[string]any{
				"Name":          "my-web-acl",
				"Scope":         "REGIONAL",
				"DefaultAction": "ALLOW",
			},
			wantStatus: http.StatusOK,
			wantID:     true,
		},
		{
			name: "missing_name",
			body: map[string]any{
				"Scope": "REGIONAL",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_scope",
			body: map[string]any{
				"Name": "my-web-acl",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doWafv2Request(t, h, "CreateWebACL", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantID {
				var result map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
				summary, ok := result["Summary"].(map[string]any)
				require.True(t, ok)
				assert.NotEmpty(t, summary["Id"])
				assert.NotEmpty(t, summary["LockToken"])
			}
		})
	}
}

func TestHandler_GetWebACL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*wafv2.Handler) string
		body       func(id string) map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "existing",
			setup: func(h *wafv2.Handler) string {
				w, _ := h.Backend.CreateWebACL("my-acl", "REGIONAL", "", "ALLOW", nil)

				return w.ID
			},
			body: func(id string) map[string]any {
				return map[string]any{"Id": id, "Name": "my-acl", "Scope": "REGIONAL"}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not_found",
			setup: func(_ *wafv2.Handler) string {
				return "nonexistent"
			},
			body: func(id string) map[string]any {
				return map[string]any{"Id": id, "Name": "x", "Scope": "REGIONAL"}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			id := tt.setup(h)
			rec := doWafv2Request(t, h, "GetWebACL", tt.body(id))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_UpdateWebACL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*wafv2.Handler) string
		body       func(id string) map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "existing",
			setup: func(h *wafv2.Handler) string {
				w, _ := h.Backend.CreateWebACL("my-acl", "REGIONAL", "", "ALLOW", nil)

				return w.ID
			},
			body: func(id string) map[string]any {
				return map[string]any{
					"Id":          id,
					"Name":        "my-acl",
					"Scope":       "REGIONAL",
					"LockToken":   "tok",
					"Description": "updated",
				}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not_found",
			setup: func(_ *wafv2.Handler) string {
				return "nonexistent"
			},
			body: func(id string) map[string]any {
				return map[string]any{"Id": id, "Name": "x", "Scope": "REGIONAL", "LockToken": "tok"}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			id := tt.setup(h)
			rec := doWafv2Request(t, h, "UpdateWebACL", tt.body(id))
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var result map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
				assert.NotEmpty(t, result["NextLockToken"])
			}
		})
	}
}

func TestHandler_DeleteWebACL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*wafv2.Handler) string
		body       func(id string) map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "existing",
			setup: func(h *wafv2.Handler) string {
				w, _ := h.Backend.CreateWebACL("my-acl", "REGIONAL", "", "ALLOW", nil)

				return w.ID
			},
			body: func(id string) map[string]any {
				return map[string]any{"Id": id, "Name": "my-acl", "Scope": "REGIONAL", "LockToken": "tok"}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not_found",
			setup: func(_ *wafv2.Handler) string {
				return "nonexistent"
			},
			body: func(id string) map[string]any {
				return map[string]any{"Id": id, "Name": "x", "Scope": "REGIONAL", "LockToken": "tok"}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			id := tt.setup(h)
			rec := doWafv2Request(t, h, "DeleteWebACL", tt.body(id))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListWebACLs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(*wafv2.Handler)
		name      string
		wantCount int
	}{
		{
			name:      "empty",
			setup:     func(_ *wafv2.Handler) {},
			wantCount: 0,
		},
		{
			name: "with_items",
			setup: func(h *wafv2.Handler) {
				_, _ = h.Backend.CreateWebACL("acl1", "REGIONAL", "", "ALLOW", nil)
				_, _ = h.Backend.CreateWebACL("acl2", "REGIONAL", "", "BLOCK", nil)
			},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setup(h)

			rec := doWafv2Request(t, h, "ListWebACLs", map[string]any{"Scope": "REGIONAL"})
			assert.Equal(t, http.StatusOK, rec.Code)

			var result map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))

			list, ok := result["WebACLs"].([]any)
			require.True(t, ok)
			assert.Len(t, list, tt.wantCount)
		})
	}
}

func TestHandler_CreateIPSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
		wantID     bool
	}{
		{
			name: "valid",
			body: map[string]any{
				"Name":             "my-ipset",
				"Scope":            "REGIONAL",
				"IPAddressVersion": "IPV4",
				"Addresses":        []string{"1.2.3.4/32"},
			},
			wantStatus: http.StatusOK,
			wantID:     true,
		},
		{
			name: "missing_name",
			body: map[string]any{
				"Scope":            "REGIONAL",
				"IPAddressVersion": "IPV4",
				"Addresses":        []string{},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doWafv2Request(t, h, "CreateIPSet", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantID {
				var result map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
				summary, ok := result["Summary"].(map[string]any)
				require.True(t, ok)
				assert.NotEmpty(t, summary["Id"])
			}
		})
	}
}

func TestHandler_GetIPSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*wafv2.Handler) string
		body       func(id string) map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "existing",
			setup: func(h *wafv2.Handler) string {
				s, _ := h.Backend.CreateIPSet("my-ipset", "REGIONAL", "", "IPV4", nil, nil)

				return s.ID
			},
			body: func(id string) map[string]any {
				return map[string]any{"Id": id, "Name": "my-ipset", "Scope": "REGIONAL"}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not_found",
			setup: func(_ *wafv2.Handler) string {
				return "nonexistent"
			},
			body: func(id string) map[string]any {
				return map[string]any{"Id": id, "Name": "x", "Scope": "REGIONAL"}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			id := tt.setup(h)
			rec := doWafv2Request(t, h, "GetIPSet", tt.body(id))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_UpdateIPSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*wafv2.Handler) string
		body       func(id string) map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "existing",
			setup: func(h *wafv2.Handler) string {
				s, _ := h.Backend.CreateIPSet("my-ipset", "REGIONAL", "", "IPV4", nil, nil)

				return s.ID
			},
			body: func(id string) map[string]any {
				return map[string]any{
					"Id":        id,
					"Name":      "my-ipset",
					"Scope":     "REGIONAL",
					"LockToken": "tok",
					"Addresses": []string{"10.0.0.0/8"},
				}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not_found",
			setup: func(_ *wafv2.Handler) string {
				return "nonexistent"
			},
			body: func(id string) map[string]any {
				return map[string]any{
					"Id":        id,
					"Name":      "x",
					"Scope":     "REGIONAL",
					"LockToken": "tok",
					"Addresses": []string{},
				}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			id := tt.setup(h)
			rec := doWafv2Request(t, h, "UpdateIPSet", tt.body(id))
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var result map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
				assert.NotEmpty(t, result["NextLockToken"])
			}
		})
	}
}

func TestHandler_DeleteIPSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*wafv2.Handler) string
		body       func(id string) map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "existing",
			setup: func(h *wafv2.Handler) string {
				s, _ := h.Backend.CreateIPSet("my-ipset", "REGIONAL", "", "IPV4", nil, nil)

				return s.ID
			},
			body: func(id string) map[string]any {
				return map[string]any{"Id": id, "Name": "my-ipset", "Scope": "REGIONAL", "LockToken": "tok"}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not_found",
			setup: func(_ *wafv2.Handler) string {
				return "nonexistent"
			},
			body: func(id string) map[string]any {
				return map[string]any{"Id": id, "Name": "x", "Scope": "REGIONAL", "LockToken": "tok"}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			id := tt.setup(h)
			rec := doWafv2Request(t, h, "DeleteIPSet", tt.body(id))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListIPSets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(*wafv2.Handler)
		name      string
		wantCount int
	}{
		{
			name:      "empty",
			setup:     func(_ *wafv2.Handler) {},
			wantCount: 0,
		},
		{
			name: "with_items",
			setup: func(h *wafv2.Handler) {
				_, _ = h.Backend.CreateIPSet("set1", "REGIONAL", "", "IPV4", nil, nil)
				_, _ = h.Backend.CreateIPSet("set2", "REGIONAL", "", "IPV6", nil, nil)
			},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setup(h)

			rec := doWafv2Request(t, h, "ListIPSets", map[string]any{"Scope": "REGIONAL"})
			assert.Equal(t, http.StatusOK, rec.Code)

			var result map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))

			list, ok := result["IPSets"].([]any)
			require.True(t, ok)
			assert.Len(t, list, tt.wantCount)
		})
	}
}

func TestHandler_TagResource_and_ListTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*wafv2.Handler) string
		name       string
		wantTag    string
		tags       []map[string]string
		wantStatus int
	}{
		{
			name: "tags_flow",
			setup: func(h *wafv2.Handler) string {
				w, _ := h.Backend.CreateWebACL("tagged-acl", "REGIONAL", "", "ALLOW", nil)

				return h.Backend.WebACLARN(w.Name, w.ID, w.Scope)
			},
			tags:       []map[string]string{{"Key": "env", "Value": "prod"}},
			wantStatus: http.StatusOK,
			wantTag:    "env",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			arnStr := tt.setup(h)

			tagRec := doWafv2Request(t, h, "TagResource", map[string]any{
				"ResourceARN": arnStr,
				"Tags":        tt.tags,
			})
			assert.Equal(t, tt.wantStatus, tagRec.Code)

			listRec := doWafv2Request(t, h, "ListTagsForResource", map[string]any{
				"ResourceARN": arnStr,
			})
			assert.Equal(t, http.StatusOK, listRec.Code)

			var result map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &result))

			info, ok := result["TagInfoForResource"].(map[string]any)
			require.True(t, ok)

			tagList, ok := info["TagList"].([]any)
			require.True(t, ok)
			require.NotEmpty(t, tagList)

			firstTag, ok := tagList[0].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.wantTag, firstTag["Key"])
		})
	}
}

func TestHandler_UnknownOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doWafv2Request(t, h, "UnknownOperation", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var result map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
	assert.Equal(t, "WAFInvalidOperationException", result["__type"])
}
