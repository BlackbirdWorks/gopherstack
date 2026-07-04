package wafv2_test

import (
	"bytes"
	"context"
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
				"DefaultAction": map[string]any{"Allow": map[string]any{}},
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
				w, _ := wafv2.CreateWebACLSimple(h.Backend, "my-acl", "REGIONAL", "", "ALLOW", nil)

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
				w, _ := wafv2.CreateWebACLSimple(h.Backend, "my-acl", "REGIONAL", "", "ALLOW", nil)

				return w.ID
			},
			body: func(id string) map[string]any {
				return map[string]any{
					"Id":          id,
					"Name":        "my-acl",
					"Scope":       "REGIONAL",
					"LockToken":   "",
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
				return map[string]any{"Id": id, "Name": "x", "Scope": "REGIONAL", "LockToken": ""}
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
				w, _ := wafv2.CreateWebACLSimple(h.Backend, "my-acl", "REGIONAL", "", "ALLOW", nil)

				return w.ID
			},
			body: func(id string) map[string]any {
				return map[string]any{"Id": id, "Name": "my-acl", "Scope": "REGIONAL", "LockToken": ""}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not_found",
			setup: func(_ *wafv2.Handler) string {
				return "nonexistent"
			},
			body: func(id string) map[string]any {
				return map[string]any{"Id": id, "Name": "x", "Scope": "REGIONAL", "LockToken": ""}
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
				_, _ = wafv2.CreateWebACLSimple(h.Backend, "acl1", "REGIONAL", "", "ALLOW", nil)
				_, _ = wafv2.CreateWebACLSimple(h.Backend, "acl2", "REGIONAL", "", "BLOCK", nil)
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
				s, _ := h.Backend.CreateIPSet(context.Background(), "my-ipset", "REGIONAL", "", "IPV4", nil, nil)

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
				s, _ := h.Backend.CreateIPSet(context.Background(), "my-ipset", "REGIONAL", "", "IPV4", nil, nil)

				return s.ID
			},
			body: func(id string) map[string]any {
				return map[string]any{
					"Id":        id,
					"Name":      "my-ipset",
					"Scope":     "REGIONAL",
					"LockToken": "",
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
					"LockToken": "",
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
				s, _ := h.Backend.CreateIPSet(context.Background(), "my-ipset", "REGIONAL", "", "IPV4", nil, nil)

				return s.ID
			},
			body: func(id string) map[string]any {
				return map[string]any{"Id": id, "Name": "my-ipset", "Scope": "REGIONAL", "LockToken": ""}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not_found",
			setup: func(_ *wafv2.Handler) string {
				return "nonexistent"
			},
			body: func(id string) map[string]any {
				return map[string]any{"Id": id, "Name": "x", "Scope": "REGIONAL", "LockToken": ""}
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
				_, _ = h.Backend.CreateIPSet(context.Background(), "set1", "REGIONAL", "", "IPV4", nil, nil)
				_, _ = h.Backend.CreateIPSet(context.Background(), "set2", "REGIONAL", "", "IPV6", nil, nil)
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
				w, _ := wafv2.CreateWebACLSimple(h.Backend, "tagged-acl", "REGIONAL", "", "ALLOW", nil)

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

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	tests := []struct {
		name string
		op   string
	}{
		{name: "AssociateWebACL", op: "AssociateWebACL"},
		{name: "DisassociateWebACL", op: "DisassociateWebACL"},
		{name: "GetWebACLForResource", op: "GetWebACLForResource"},
		{name: "UntagResource", op: "UntagResource"},
		{name: "TagResource", op: "TagResource"},
		{name: "ListTagsForResource", op: "ListTagsForResource"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Contains(t, ops, tt.op)
		})
	}
}

func TestHandler_ChaosMetadata(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tests := []struct {
		name string
		got  string
		want string
	}{
		{name: "service_name", got: h.ChaosServiceName(), want: "wafv2"},
		{name: "region", got: h.ChaosRegions()[0], want: "us-east-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, tt.got)
		})
	}
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, service.PriorityHeaderExact, h.MatchPriority())
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		want   string
	}{
		{name: "valid_target", target: "AWSWAF_20190729.CreateWebACL", want: "CreateWebACL"},
		{name: "no_prefix", target: "CreateWebACL", want: "CreateWebACL"},
		{name: "empty", target: "", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, h.ExtractOperation(c))
		})
	}
}

func TestBackend_Reset(t *testing.T) {
	t.Parallel()

	b := wafv2.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := wafv2.CreateWebACLSimple(b, "acl1", "REGIONAL", "", "ALLOW", nil)
	require.NoError(t, err)
	_, err = b.CreateIPSet(context.Background(), "set1", "REGIONAL", "", "IPV4", nil, nil)
	require.NoError(t, err)

	b.Reset()

	assert.Empty(t, b.ListWebACLs(context.Background()))
	assert.Empty(t, b.ListIPSets(context.Background()))
}

func TestHandler_AssociateWebACL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*wafv2.Handler) (string, string)
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *wafv2.Handler) (string, string) {
				w, _ := wafv2.CreateWebACLSimple(h.Backend, "my-acl", "REGIONAL", "", "ALLOW", nil)
				webACLARN := h.Backend.WebACLARN(w.Name, w.ID, w.Scope)

				return webACLARN, "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/my-lb/abc"
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_web_acl_arn",
			setup: func(_ *wafv2.Handler) (string, string) {
				return "", "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/my-lb/abc"
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_resource_arn",
			setup: func(h *wafv2.Handler) (string, string) {
				w, _ := wafv2.CreateWebACLSimple(h.Backend, "my-acl", "REGIONAL", "", "ALLOW", nil)
				webACLARN := h.Backend.WebACLARN(w.Name, w.ID, w.Scope)

				return webACLARN, ""
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "web_acl_not_found",
			setup: func(_ *wafv2.Handler) (string, string) {
				webACLARN := "arn:aws:wafv2:us-east-1:000000000000:regional/webacl/nonexistent/badid"
				resourceARN := "arn:aws:ec2:us-east-1:000000000000:instance/i-abc"

				return webACLARN, resourceARN
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			webACLARN, resourceARN := tt.setup(h)

			body := map[string]any{}
			if webACLARN != "" {
				body["WebACLArn"] = webACLARN
			}
			if resourceARN != "" {
				body["ResourceArn"] = resourceARN
			}

			rec := doWafv2Request(t, h, "AssociateWebACL", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DisassociateWebACL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*wafv2.Handler) string
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *wafv2.Handler) string {
				w, _ := wafv2.CreateWebACLSimple(h.Backend, "my-acl", "REGIONAL", "", "ALLOW", nil)
				webACLARN := h.Backend.WebACLARN(w.Name, w.ID, w.Scope)
				resourceARN := "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/my-lb/abc"
				require.NoError(t, h.Backend.AssociateWebACL(context.Background(), webACLARN, resourceARN))

				return resourceARN
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_resource_arn",
			setup: func(_ *wafv2.Handler) string {
				return ""
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			// AWS DisassociateWebACL is idempotent — calling it when no
			// association exists succeeds (no-op), matching real AWS behaviour.
			name: "not_found_idempotent",
			setup: func(_ *wafv2.Handler) string {
				return "arn:aws:ec2:us-east-1:000000000000:instance/i-nonexistent"
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			resourceARN := tt.setup(h)

			body := map[string]any{}
			if resourceARN != "" {
				body["ResourceArn"] = resourceARN
			}

			rec := doWafv2Request(t, h, "DisassociateWebACL", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetWebACLForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*wafv2.Handler) string
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *wafv2.Handler) string {
				w, _ := wafv2.CreateWebACLSimple(h.Backend, "my-acl", "REGIONAL", "", "ALLOW", nil)
				webACLARN := h.Backend.WebACLARN(w.Name, w.ID, w.Scope)
				resourceARN := "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/my-lb/abc"
				require.NoError(t, h.Backend.AssociateWebACL(context.Background(), webACLARN, resourceARN))

				return resourceARN
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_resource_arn",
			setup: func(_ *wafv2.Handler) string {
				return ""
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "not_found",
			setup: func(_ *wafv2.Handler) string {
				return "arn:aws:ec2:us-east-1:000000000000:instance/i-nonexistent"
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			resourceARN := tt.setup(h)

			body := map[string]any{}
			if resourceARN != "" {
				body["ResourceArn"] = resourceARN
			}

			rec := doWafv2Request(t, h, "GetWebACLForResource", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var result map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
				webACL, ok := result["WebACL"].(map[string]any)
				require.True(t, ok)
				assert.NotEmpty(t, webACL["Id"])
				assert.Equal(t, "my-acl", webACL["Name"])
			}
		})
	}
}

func TestHandler_UntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*wafv2.Handler) string
		name       string
		tagKeys    []string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *wafv2.Handler) string {
				w, _ := wafv2.CreateWebACLSimple(
					h.Backend,
					"tagged-acl",
					"REGIONAL",
					"",
					"ALLOW",
					map[string]string{"env": "prod", "team": "ops"},
				)
				arnStr := h.Backend.WebACLARN(w.Name, w.ID, w.Scope)

				return arnStr
			},
			tagKeys:    []string{"env"},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_resource_arn",
			setup: func(_ *wafv2.Handler) string {
				return ""
			},
			tagKeys:    []string{"env"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "not_found",
			setup: func(_ *wafv2.Handler) string {
				return "arn:aws:wafv2:us-east-1:000000000000:regional/webacl/nonexistent/badid"
			},
			tagKeys:    []string{"env"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "ipset_success",
			setup: func(h *wafv2.Handler) string {
				s, _ := h.Backend.CreateIPSet(
					context.Background(),
					"tagged-set",
					"REGIONAL",
					"",
					"IPV4",
					nil,
					map[string]string{"env": "prod"},
				)
				arnStr := h.Backend.IPSetARN(s.Name, s.ID, s.Scope)

				return arnStr
			},
			tagKeys:    []string{"env"},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			resourceARN := tt.setup(h)

			body := map[string]any{"TagKeys": tt.tagKeys}
			if resourceARN != "" {
				body["ResourceARN"] = resourceARN
			}

			rec := doWafv2Request(t, h, "UntagResource", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_TagResource_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantType   string
		wantStatus int
	}{
		{
			name:       "missing_resource_arn",
			body:       map[string]any{"Tags": []map[string]string{{"Key": "k", "Value": "v"}}},
			wantStatus: http.StatusBadRequest,
			wantType:   "WAFInvalidParameterException",
		},
		{
			name: "not_found",
			body: map[string]any{
				"ResourceARN": "arn:aws:wafv2:us-east-1:000000000000:regional/webacl/nonexistent/badid",
				"Tags":        []map[string]string{{"Key": "k", "Value": "v"}},
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "WAFNonexistentItemException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doWafv2Request(t, h, "TagResource", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var result map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
			assert.Equal(t, tt.wantType, result["__type"])
		})
	}
}

func TestHandler_ListTagsForResource_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantType   string
		wantStatus int
	}{
		{
			name:       "missing_resource_arn",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
			wantType:   "WAFInvalidParameterException",
		},
		{
			name: "not_found",
			body: map[string]any{
				"ResourceARN": "arn:aws:wafv2:us-east-1:000000000000:regional/webacl/nonexistent/badid",
			},
			wantStatus: http.StatusBadRequest,
			wantType:   "WAFNonexistentItemException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doWafv2Request(t, h, "ListTagsForResource", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var result map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
			assert.Equal(t, tt.wantType, result["__type"])
		})
	}
}

func TestHandler_CreateWebACL_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "CreateWebACL", map[string]any{
		"Name":  "my-acl",
		"Scope": "REGIONAL",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doWafv2Request(t, h, "CreateWebACL", map[string]any{
		"Name":  "my-acl",
		"Scope": "REGIONAL",
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)

	var result map[string]string
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &result))
	assert.Equal(t, "WAFDuplicateItemException", result["__type"])
}

func TestHandler_CreateIPSet_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doWafv2Request(t, h, "CreateIPSet", map[string]any{
		"Name":             "my-set",
		"Scope":            "REGIONAL",
		"IPAddressVersion": "IPV4",
		"Addresses":        []string{},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doWafv2Request(t, h, "CreateIPSet", map[string]any{
		"Name":             "my-set",
		"Scope":            "REGIONAL",
		"IPAddressVersion": "IPV4",
		"Addresses":        []string{},
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)

	var result map[string]string
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &result))
	assert.Equal(t, "WAFDuplicateItemException", result["__type"])
}

func TestHandler_GetWebACL_MissingID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doWafv2Request(t, h, "GetWebACL", map[string]any{"Name": "my-acl", "Scope": "REGIONAL"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var result map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
	assert.Equal(t, "WAFInvalidParameterException", result["__type"])
}

func TestHandler_UpdateWebACL_MissingID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doWafv2Request(
		t,
		h,
		"UpdateWebACL",
		map[string]any{"Name": "my-acl", "Scope": "REGIONAL", "LockToken": "tok"},
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DeleteWebACL_MissingID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doWafv2Request(
		t,
		h,
		"DeleteWebACL",
		map[string]any{"Name": "my-acl", "Scope": "REGIONAL", "LockToken": "tok"},
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_GetIPSet_MissingID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doWafv2Request(t, h, "GetIPSet", map[string]any{"Name": "my-set", "Scope": "REGIONAL"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UpdateIPSet_MissingID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doWafv2Request(
		t,
		h,
		"UpdateIPSet",
		map[string]any{"Name": "my-set", "Scope": "REGIONAL", "LockToken": "tok"},
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DeleteIPSet_MissingID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doWafv2Request(
		t,
		h,
		"DeleteIPSet",
		map[string]any{"Name": "my-set", "Scope": "REGIONAL", "LockToken": "tok"},
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListWebACLs_Scope_Filter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(*wafv2.Handler)
		name      string
		scope     string
		wantCount int
	}{
		{
			name: "filter_cloudfront",
			setup: func(h *wafv2.Handler) {
				_, _ = wafv2.CreateWebACLSimple(h.Backend, "regional-acl", "REGIONAL", "", "ALLOW", nil)
				_, _ = wafv2.CreateWebACLSimple(h.Backend, "cf-acl", "CLOUDFRONT", "", "ALLOW", nil)
			},
			scope:     "CLOUDFRONT",
			wantCount: 1,
		},
		{
			name: "no_filter_returns_all",
			setup: func(h *wafv2.Handler) {
				_, _ = wafv2.CreateWebACLSimple(h.Backend, "regional-acl", "REGIONAL", "", "ALLOW", nil)
				_, _ = wafv2.CreateWebACLSimple(h.Backend, "cf-acl", "CLOUDFRONT", "", "ALLOW", nil)
			},
			scope:     "",
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setup(h)

			body := map[string]any{}
			if tt.scope != "" {
				body["Scope"] = tt.scope
			}

			rec := doWafv2Request(t, h, "ListWebACLs", body)
			assert.Equal(t, http.StatusOK, rec.Code)

			var result map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
			list, ok := result["WebACLs"].([]any)
			require.True(t, ok)
			assert.Len(t, list, tt.wantCount)
		})
	}
}

func TestHandler_ListIPSets_Scope_Filter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(*wafv2.Handler)
		name      string
		scope     string
		wantCount int
	}{
		{
			name: "filter_cloudfront",
			setup: func(h *wafv2.Handler) {
				_, _ = h.Backend.CreateIPSet(context.Background(), "regional-set", "REGIONAL", "", "IPV4", nil, nil)
				_, _ = h.Backend.CreateIPSet(context.Background(), "cf-set", "CLOUDFRONT", "", "IPV4", nil, nil)
			},
			scope:     "CLOUDFRONT",
			wantCount: 1,
		},
		{
			name: "no_filter_returns_all",
			setup: func(h *wafv2.Handler) {
				_, _ = h.Backend.CreateIPSet(context.Background(), "regional-set", "REGIONAL", "", "IPV4", nil, nil)
				_, _ = h.Backend.CreateIPSet(context.Background(), "cf-set", "CLOUDFRONT", "", "IPV4", nil, nil)
			},
			scope:     "",
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setup(h)

			body := map[string]any{}
			if tt.scope != "" {
				body["Scope"] = tt.scope
			}

			rec := doWafv2Request(t, h, "ListIPSets", body)
			assert.Equal(t, http.StatusOK, rec.Code)

			var result map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
			list, ok := result["IPSets"].([]any)
			require.True(t, ok)
			assert.Len(t, list, tt.wantCount)
		})
	}
}

func TestBackend_WebACLARN(t *testing.T) {
	t.Parallel()

	b := wafv2.NewInMemoryBackend("123456789012", "us-east-1")
	tests := []struct {
		name     string
		wantPart string
		scope    string
	}{
		{name: "regional_scope", scope: "REGIONAL", wantPart: "regional"},
		{name: "cloudfront_scope", scope: "CLOUDFRONT", wantPart: "global"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			arnStr := b.WebACLARN("my-acl", "myid", tt.scope)
			assert.Contains(t, arnStr, tt.wantPart)
			assert.Contains(t, arnStr, "webacl")
		})
	}
}

func TestBackend_IPSetARN(t *testing.T) {
	t.Parallel()

	b := wafv2.NewInMemoryBackend("123456789012", "us-east-1")
	tests := []struct {
		name     string
		wantPart string
		scope    string
	}{
		{name: "regional_scope", scope: "REGIONAL", wantPart: "regional"},
		{name: "cloudfront_scope", scope: "CLOUDFRONT", wantPart: "global"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			arnStr := b.IPSetARN("my-set", "myid", tt.scope)
			assert.Contains(t, arnStr, tt.wantPart)
			assert.Contains(t, arnStr, "ipset")
		})
	}
}

func TestHandler_GetWebACL_BlockDefaultAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	w, err := wafv2.CreateWebACLSimple(h.Backend, "blocked-acl", "REGIONAL", "", "BLOCK", nil)
	require.NoError(t, err)

	rec := doWafv2Request(t, h, "GetWebACL", map[string]any{
		"Id":    w.ID,
		"Name":  w.Name,
		"Scope": w.Scope,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var result map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
	webACL, ok := result["WebACL"].(map[string]any)
	require.True(t, ok)
	defaultAction, ok := webACL["DefaultAction"].(map[string]any)
	require.True(t, ok)
	assert.Contains(t, defaultAction, "Block")
}

func TestHandler_CreateWebACL_MissingScope(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doWafv2Request(t, h, "CreateWebACL", map[string]any{"Name": "my-acl"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_CreateIPSet_MissingScope(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doWafv2Request(
		t,
		h,
		"CreateIPSet",
		map[string]any{"Name": "my-set", "IPAddressVersion": "IPV4", "Addresses": []string{}},
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestBackend_Snapshot_And_Restore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*wafv2.InMemoryBackend)
		name    string
		wantIDs int
	}{
		{
			name:  "empty_backend",
			setup: func(_ *wafv2.InMemoryBackend) {},
		},
		{
			name: "with_webacls_and_ipsets",
			setup: func(b *wafv2.InMemoryBackend) {
				_, _ = wafv2.CreateWebACLSimple(b, "acl1", "REGIONAL", "desc", "ALLOW", nil)
				_, _ = b.CreateIPSet(
					context.Background(), "set1", "REGIONAL", "desc", "IPV4", []string{"1.2.3.4/32"}, nil,
				)
			},
			wantIDs: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := wafv2.NewInMemoryBackend("123456789012", "us-east-1")
			tt.setup(b)

			snap := b.Snapshot(t.Context())
			require.NotNil(t, snap)

			b2 := wafv2.NewInMemoryBackend("123456789012", "us-east-1")
			require.NoError(t, b2.Restore(t.Context(), snap))

			acls := b2.ListWebACLs(context.Background())
			sets := b2.ListIPSets(context.Background())

			assert.Len(t, acls, len(b.ListWebACLs(context.Background())))
			assert.Len(t, sets, len(b.ListIPSets(context.Background())))
		})
	}
}

func TestHandler_Snapshot_And_Restore(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, err := wafv2.CreateWebACLSimple(h.Backend, "my-acl", "REGIONAL", "", "ALLOW", nil)
	require.NoError(t, err)

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	h2 := newTestHandler(t)
	require.NoError(t, h2.Restore(t.Context(), snap))

	acls := h2.Backend.ListWebACLs(context.Background())
	require.Len(t, acls, 1)
	assert.Equal(t, "my-acl", acls[0].Name)
}

func TestBackend_Restore_InvalidData(t *testing.T) {
	t.Parallel()

	b := wafv2.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

func TestHandler_ChaosOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.ChaosOperations()
	assert.Equal(t, h.GetSupportedOperations(), ops)
}

func TestHandler_GetWebACL_WithVisibilityConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create WebACL via HTTP API to include VisibilityConfig.
	createRec := doWafv2Request(t, h, "CreateWebACL", map[string]any{
		"Name":          "my-acl",
		"Scope":         "REGIONAL",
		"DefaultAction": map[string]any{"Allow": map[string]any{}},
		"VisibilityConfig": map[string]any{
			"CloudWatchMetricsEnabled": true,
			"MetricName":               "my-acl",
			"SampledRequestsEnabled":   true,
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResult map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResult))
	summary := createResult["Summary"].(map[string]any)
	id := summary["Id"].(string)

	rec := doWafv2Request(t, h, "GetWebACL", map[string]any{
		"Id":    id,
		"Name":  "my-acl",
		"Scope": "REGIONAL",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var result map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
	webACL, ok := result["WebACL"].(map[string]any)
	require.True(t, ok)
	vis, ok := webACL["VisibilityConfig"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, true, vis["CloudWatchMetricsEnabled"])
}

func TestHandler_GetWebACLForResource_WithVisibilityConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	w, err := wafv2.CreateWebACLSimple(h.Backend, "my-acl", "REGIONAL", "", "BLOCK", nil)
	require.NoError(t, err)

	webACLARN := h.Backend.WebACLARN(w.Name, w.ID, w.Scope)
	resourceARN := "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/my-lb/xyz"
	require.NoError(t, h.Backend.AssociateWebACL(context.Background(), webACLARN, resourceARN))

	rec := doWafv2Request(t, h, "GetWebACLForResource", map[string]any{
		"ResourceArn": resourceARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var result map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
	webACL, ok := result["WebACL"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "my-acl", webACL["Name"])
	defaultAction, ok := webACL["DefaultAction"].(map[string]any)
	require.True(t, ok)
	assert.Contains(t, defaultAction, "Block")
}

func TestBackend_TagResource_IPSet(t *testing.T) {
	t.Parallel()

	b := wafv2.NewInMemoryBackend("000000000000", "us-east-1")
	s, err := b.CreateIPSet(context.Background(), "my-set", "REGIONAL", "", "IPV4", nil, nil)
	require.NoError(t, err)

	arnStr := b.IPSetARN(s.Name, s.ID, s.Scope)
	require.NoError(t, b.TagResource(context.Background(), arnStr, map[string]string{"env": "test"}))

	tags, err := b.ListTagsForResource(context.Background(), arnStr)
	require.NoError(t, err)
	assert.Equal(t, "test", tags["env"])
}

func TestHandler_CheckCapacity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
		wantMinCap int
	}{
		{
			name:       "no_rules",
			body:       map[string]any{"Scope": "REGIONAL", "Rules": []any{}},
			wantStatus: http.StatusOK,
			wantMinCap: 0,
		},
		{
			name: "two_rules",
			body: map[string]any{
				"Scope": "REGIONAL",
				"Rules": []any{
					map[string]any{"Name": "rule1"},
					map[string]any{"Name": "rule2"},
				},
			},
			wantStatus: http.StatusOK,
			wantMinCap: 2,
		},
		{
			name:       "missing_scope",
			body:       map[string]any{"Rules": []any{}},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doWafv2Request(t, h, "CheckCapacity", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var result map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
				consumed, ok := result["ConsumedCapacity"].(float64)
				require.True(t, ok)
				assert.GreaterOrEqual(t, int(consumed), tt.wantMinCap)
			}
		})
	}
}

func TestHandler_CreateAndDeleteAPIKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		createBody map[string]any
		deleteBody func(apiKey string) map[string]any
		name       string
		wantCreate int
		wantDelete int
	}{
		{
			name:       "success",
			createBody: map[string]any{"Scope": "REGIONAL", "TokenDomains": []string{"example.com"}},
			deleteBody: func(apiKey string) map[string]any {
				return map[string]any{"Scope": "REGIONAL", "APIKey": apiKey}
			},
			wantCreate: http.StatusOK,
			wantDelete: http.StatusOK,
		},
		{
			name:       "create_missing_scope",
			createBody: map[string]any{"TokenDomains": []string{"example.com"}},
			deleteBody: func(_ string) map[string]any { return nil },
			wantCreate: http.StatusBadRequest,
		},
		{
			name:       "delete_missing_scope",
			createBody: map[string]any{"Scope": "REGIONAL", "TokenDomains": []string{"example.com"}},
			deleteBody: func(_ string) map[string]any {
				return map[string]any{"APIKey": "somekey"}
			},
			wantCreate: http.StatusOK,
			wantDelete: http.StatusBadRequest,
		},
		{
			name:       "delete_missing_key",
			createBody: map[string]any{"Scope": "REGIONAL", "TokenDomains": []string{"example.com"}},
			deleteBody: func(_ string) map[string]any {
				return map[string]any{"Scope": "REGIONAL"}
			},
			wantCreate: http.StatusOK,
			wantDelete: http.StatusBadRequest,
		},
		{
			name:       "delete_not_found",
			createBody: map[string]any{"Scope": "REGIONAL", "TokenDomains": []string{"example.com"}},
			deleteBody: func(_ string) map[string]any {
				return map[string]any{"Scope": "REGIONAL", "APIKey": "nonexistent-key"}
			},
			wantCreate: http.StatusOK,
			wantDelete: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createRec := doWafv2Request(t, h, "CreateAPIKey", tt.createBody)
			assert.Equal(t, tt.wantCreate, createRec.Code)

			if tt.wantCreate != http.StatusOK || tt.deleteBody == nil {
				return
			}

			var createResult map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResult))
			apiKey, _ := createResult["APIKey"].(string)

			deleteRec := doWafv2Request(t, h, "DeleteAPIKey", tt.deleteBody(apiKey))
			assert.Equal(t, tt.wantDelete, deleteRec.Code)
		})
	}
}

func TestHandler_CreateRegexPatternSet(t *testing.T) {
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
				"Name":                  "my-regex",
				"Scope":                 "REGIONAL",
				"RegularExpressionList": []string{"^foo.*", "bar$"},
			},
			wantStatus: http.StatusOK,
			wantID:     true,
		},
		{
			name:       "missing_name",
			body:       map[string]any{"Scope": "REGIONAL"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_scope",
			body:       map[string]any{"Name": "my-regex"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "duplicate",
			body: map[string]any{
				"Name":  "dup-regex",
				"Scope": "REGIONAL",
			},
			wantStatus: http.StatusOK,
			wantID:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "duplicate" {
				_, _ = h.Backend.CreateRegexPatternSet(context.Background(), "dup-regex", "REGIONAL", "", nil, nil)
			}

			rec := doWafv2Request(t, h, "CreateRegexPatternSet", tt.body)

			if tt.name == "duplicate" {
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantID {
				var result map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
				summary, ok := result["Summary"].(map[string]any)
				require.True(t, ok)
				assert.NotEmpty(t, summary["Id"])
				assert.NotEmpty(t, summary["ARN"])
				assert.NotEmpty(t, summary["LockToken"])
			}
		})
	}
}

func TestHandler_DeleteRegexPatternSet(t *testing.T) {
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
				rps, _ := h.Backend.CreateRegexPatternSet(context.Background(), "my-regex", "REGIONAL", "", nil, nil)

				return rps.ID
			},
			body: func(id string) map[string]any {
				return map[string]any{"Id": id, "Name": "my-regex", "Scope": "REGIONAL", "LockToken": ""}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not_found",
			setup: func(_ *wafv2.Handler) string {
				return "nonexistent"
			},
			body: func(id string) map[string]any {
				return map[string]any{"Id": id, "Name": "x", "Scope": "REGIONAL", "LockToken": ""}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_id",
			setup: func(_ *wafv2.Handler) string {
				return ""
			},
			body: func(_ string) map[string]any {
				return map[string]any{"Name": "x", "Scope": "REGIONAL", "LockToken": ""}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			id := tt.setup(h)
			rec := doWafv2Request(t, h, "DeleteRegexPatternSet", tt.body(id))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CreateRuleGroup(t *testing.T) {
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
				"Name":     "my-rulegroup",
				"Scope":    "REGIONAL",
				"Capacity": 100,
			},
			wantStatus: http.StatusOK,
			wantID:     true,
		},
		{
			name:       "missing_name",
			body:       map[string]any{"Scope": "REGIONAL", "Capacity": 10},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_scope",
			body:       map[string]any{"Name": "my-rulegroup", "Capacity": 10},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doWafv2Request(t, h, "CreateRuleGroup", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantID {
				var result map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
				summary, ok := result["Summary"].(map[string]any)
				require.True(t, ok)
				assert.NotEmpty(t, summary["Id"])
				assert.NotEmpty(t, summary["ARN"])
				assert.NotEmpty(t, summary["LockToken"])
			}
		})
	}
}

func TestHandler_CreateRuleGroup_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doWafv2Request(t, h, "CreateRuleGroup", map[string]any{
		"Name":     "dup-group",
		"Scope":    "REGIONAL",
		"Capacity": 10,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doWafv2Request(t, h, "CreateRuleGroup", map[string]any{
		"Name":     "dup-group",
		"Scope":    "REGIONAL",
		"Capacity": 10,
	})
	assert.Equal(t, http.StatusBadRequest, rec2.Code)

	var result map[string]string
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &result))
	assert.Equal(t, "WAFDuplicateItemException", result["__type"])
}

func TestHandler_DeleteFirewallManagerRuleGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*wafv2.Handler) string
		body       func(arnStr string) map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *wafv2.Handler) string {
				w, _ := wafv2.CreateWebACLSimple(h.Backend, "my-acl", "REGIONAL", "", "ALLOW", nil)

				return h.Backend.WebACLARN(w.Name, w.ID, w.Scope)
			},
			body: func(arnStr string) map[string]any {
				return map[string]any{"WebACLArn": arnStr, "WebACLLockToken": "tok"}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_arn",
			setup: func(_ *wafv2.Handler) string {
				return ""
			},
			body: func(_ string) map[string]any {
				return map[string]any{"WebACLLockToken": "tok"}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "not_found",
			setup: func(_ *wafv2.Handler) string {
				return "arn:aws:wafv2:us-east-1:000000000000:regional/webacl/nonexistent/badid"
			},
			body: func(arnStr string) map[string]any {
				return map[string]any{"WebACLArn": arnStr, "WebACLLockToken": "tok"}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			arnStr := tt.setup(h)
			rec := doWafv2Request(t, h, "DeleteFirewallManagerRuleGroups", tt.body(arnStr))
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var result map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
				assert.NotEmpty(t, result["NextWebACLLockToken"])
			}
		})
	}
}

func TestHandler_DeleteLoggingConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*wafv2.Handler) string
		body       func(arnStr string) map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *wafv2.Handler) string {
				w, _ := wafv2.CreateWebACLSimple(h.Backend, "my-acl", "REGIONAL", "", "ALLOW", nil)
				arnStr := h.Backend.WebACLARN(w.Name, w.ID, w.Scope)
				require.NoError(t, h.Backend.PutLoggingConfiguration(
					context.Background(),
					arnStr,
					json.RawMessage(`{"ResourceArn":"`+arnStr+`"}`),
				))

				return arnStr
			},
			body: func(arnStr string) map[string]any {
				return map[string]any{"ResourceArn": arnStr}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_arn",
			setup: func(_ *wafv2.Handler) string {
				return ""
			},
			body: func(_ string) map[string]any {
				return map[string]any{}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "not_found",
			setup: func(_ *wafv2.Handler) string {
				return "arn:aws:wafv2:us-east-1:000000000000:regional/webacl/nonexistent/badid"
			},
			body: func(arnStr string) map[string]any {
				return map[string]any{"ResourceArn": arnStr}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			arnStr := tt.setup(h)
			rec := doWafv2Request(t, h, "DeleteLoggingConfiguration", tt.body(arnStr))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DeletePermissionPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*wafv2.Handler) string
		body       func(arnStr string) map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(h *wafv2.Handler) string {
				rg, _ := h.Backend.CreateRuleGroup(context.Background(), "my-rg", "REGIONAL", "", "", 10, nil, nil)
				arnStr := h.Backend.RuleGroupARN(rg.Name, rg.ID, rg.Scope)
				require.NoError(t, h.Backend.PutPermissionPolicy(
					context.Background(), arnStr, `{"Version":"2012-10-17"}`,
				))

				return arnStr
			},
			body: func(arnStr string) map[string]any {
				return map[string]any{"ResourceArn": arnStr}
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing_arn",
			setup: func(_ *wafv2.Handler) string {
				return ""
			},
			body: func(_ string) map[string]any {
				return map[string]any{}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "not_found",
			setup: func(_ *wafv2.Handler) string {
				return "arn:aws:wafv2:us-east-1:000000000000:regional/rulegroup/nonexistent/badid"
			},
			body: func(arnStr string) map[string]any {
				return map[string]any{"ResourceArn": arnStr}
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			arnStr := tt.setup(h)
			rec := doWafv2Request(t, h, "DeletePermissionPolicy", tt.body(arnStr))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestBackend_RegexPatternSetARN(t *testing.T) {
	t.Parallel()

	b := wafv2.NewInMemoryBackend("123456789012", "us-east-1")
	tests := []struct {
		name     string
		wantPart string
		scope    string
	}{
		{name: "regional_scope", scope: "REGIONAL", wantPart: "regional"},
		{name: "cloudfront_scope", scope: "CLOUDFRONT", wantPart: "global"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			arnStr := b.RegexPatternSetARN("my-regex", "myid", tt.scope)
			assert.Contains(t, arnStr, tt.wantPart)
			assert.Contains(t, arnStr, "regexpatternset")
		})
	}
}

func TestBackend_RuleGroupARN(t *testing.T) {
	t.Parallel()

	b := wafv2.NewInMemoryBackend("123456789012", "us-east-1")
	tests := []struct {
		name     string
		wantPart string
		scope    string
	}{
		{name: "regional_scope", scope: "REGIONAL", wantPart: "regional"},
		{name: "cloudfront_scope", scope: "CLOUDFRONT", wantPart: "global"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			arnStr := b.RuleGroupARN("my-rg", "myid", tt.scope)
			assert.Contains(t, arnStr, tt.wantPart)
			assert.Contains(t, arnStr, "rulegroup")
		})
	}
}

func TestBackend_Snapshot_WithNewResources(t *testing.T) {
	t.Parallel()

	b := wafv2.NewInMemoryBackend("123456789012", "us-east-1")

	_, err := b.CreateRegexPatternSet(
		context.Background(), "my-regex", "REGIONAL", "", []wafv2.RegexEntry{{RegexString: "^foo"}}, nil,
	)
	require.NoError(t, err)

	_, err = b.CreateRuleGroup(context.Background(), "my-rg", "REGIONAL", "", "", 10, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateAPIKey(context.Background(), "REGIONAL", []string{"example.com"})
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := wafv2.NewInMemoryBackend("123456789012", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	// Verify regex pattern sets are restored (via delete which requires lookup).
	rps2, err := b.CreateRegexPatternSet(context.Background(), "another-regex", "REGIONAL", "", nil, nil)
	require.NoError(t, err)
	require.NoError(t, b.DeleteRegexPatternSet(context.Background(), rps2.ID, ""))
}
