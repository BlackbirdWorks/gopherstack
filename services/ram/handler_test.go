package ram_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ram"
)

func newTestHandler(t *testing.T) *ram.Handler {
	t.Helper()

	return ram.NewHandler(ram.NewInMemoryBackend("000000000000", "us-east-1"))
}

func doRAMRequest(t *testing.T, h *ram.Handler, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	return doRAMRawRequest(t, h, http.MethodPost, path, bodyBytes)
}

func doRAMRawRequest(t *testing.T, h *ram.Handler, method, path string, body []byte) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20230101/us-east-1/ram/aws4_request")

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
	assert.Equal(t, "RAM", h.Name())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateResourceShare")
	assert.Contains(t, ops, "GetResourceShares")
	assert.Contains(t, ops, "UpdateResourceShare")
	assert.Contains(t, ops, "DeleteResourceShare")
	assert.Contains(t, ops, "AssociateResourceShare")
	assert.Contains(t, ops, "DisassociateResourceShare")
	assert.Contains(t, ops, "GetResourceShareAssociations")
	assert.Contains(t, ops, "TagResource")
	assert.Contains(t, ops, "UntagResource")
	assert.Contains(t, ops, "ListTagsForResource")
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, 87, h.MatchPriority())
}

func TestHandler_ChaosServiceName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "ram", h.ChaosServiceName())
}

func TestHandler_ChaosOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, h.GetSupportedOperations(), h.ChaosOperations())
}

func TestHandler_ChaosRegions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, []string{"us-east-1"}, h.ChaosRegions())
}

func TestHandler_CreateResourceShare(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		setup      func(*testing.T, *ram.Handler)
		name       string
		wantBody   string
		wantStatus int
		wantErr    bool
	}{
		{
			name: "success",
			body: map[string]any{
				"name":                    "my-share",
				"allowExternalPrincipals": true,
			},
			wantStatus: http.StatusOK,
			wantBody:   "my-share",
		},
		{
			name:       "missing name",
			body:       map[string]any{},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "duplicate name",
			setup: func(t *testing.T, h *ram.Handler) {
				t.Helper()
				_, err := h.Backend.CreateResourceShare("dup-share", true, nil, nil, nil)
				require.NoError(t, err)
			},
			body: map[string]any{
				"name": "dup-share",
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doRAMRequest(t, h, "/createresourceshare", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_GetResourceShares(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		setup      func(*testing.T, *ram.Handler)
		name       string
		wantBody   string
		wantStatus int
	}{
		{
			name: "list all",
			setup: func(t *testing.T, h *ram.Handler) {
				t.Helper()
				_, err := h.Backend.CreateResourceShare("list-share", true, nil, nil, nil)
				require.NoError(t, err)
			},
			body:       map[string]any{"resourceOwner": "SELF"},
			wantStatus: http.StatusOK,
			wantBody:   "list-share",
		},
		{
			name: "by ARN",
			setup: func(t *testing.T, h *ram.Handler) {
				t.Helper()
				rs, err := h.Backend.CreateResourceShare("arn-share", true, nil, nil, nil)
				require.NoError(t, err)
				t.Cleanup(func() {
					_ = h.Backend.DeleteResourceShare(rs.ARN)
				})
			},
			body:       map[string]any{"resourceOwner": "SELF"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "empty list",
			body:       map[string]any{"resourceOwner": "SELF"},
			wantStatus: http.StatusOK,
			wantBody:   "resourceShares",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doRAMRequest(t, h, "/getresourceshares", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_UpdateResourceShare(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *ram.Handler) string
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *ram.Handler) string {
				t.Helper()
				rs, err := h.Backend.CreateResourceShare("upd-share", true, nil, nil, nil)
				require.NoError(t, err)

				return rs.ARN
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not found",
			setup: func(_ *testing.T, _ *ram.Handler) string {
				return "arn:aws:ram:us-east-1:000000000000:resource-share/nonexistent"
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			shareARN := tt.setup(t, h)

			rec := doRAMRequest(t, h, "/updateresourceshare", map[string]any{
				"resourceShareArn":        shareARN,
				"name":                    "updated-share",
				"allowExternalPrincipals": false,
			})

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DeleteResourceShare(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *ram.Handler) string
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *ram.Handler) string {
				t.Helper()
				rs, err := h.Backend.CreateResourceShare("del-share", true, nil, nil, nil)
				require.NoError(t, err)

				return rs.ARN
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not found",
			setup: func(_ *testing.T, _ *ram.Handler) string {
				return "arn:aws:ram:us-east-1:000000000000:resource-share/nonexistent"
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing query param",
			setup: func(_ *testing.T, _ *ram.Handler) string {
				return ""
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			shareARN := tt.setup(t, h)

			path := "/deleteresourceshare"
			if shareARN != "" {
				path += "?resourceShareArn=" + shareARN
			}

			rec := doRAMRawRequest(t, h, http.MethodDelete, path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_AssociateResourceShare(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *ram.Handler) string
		name       string
		wantStatus int
	}{
		{
			name: "associate principal",
			setup: func(t *testing.T, h *ram.Handler) string {
				t.Helper()
				rs, err := h.Backend.CreateResourceShare("assoc-share", true, nil, nil, nil)
				require.NoError(t, err)

				return rs.ARN
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not found",
			setup: func(_ *testing.T, _ *ram.Handler) string {
				return "arn:aws:ram:us-east-1:000000000000:resource-share/nonexistent"
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			shareARN := tt.setup(t, h)

			rec := doRAMRequest(t, h, "/associateresourceshare", map[string]any{
				"resourceShareArn": shareARN,
				"principals":       []string{"123456789012"},
			})

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DisassociateResourceShare(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *ram.Handler) string
		name       string
		wantStatus int
	}{
		{
			name: "disassociate principal",
			setup: func(t *testing.T, h *ram.Handler) string {
				t.Helper()
				rs, err := h.Backend.CreateResourceShare("disassoc-share", true, nil, []string{"123456789012"}, nil)
				require.NoError(t, err)

				return rs.ARN
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not found",
			setup: func(_ *testing.T, _ *ram.Handler) string {
				return "arn:aws:ram:us-east-1:000000000000:resource-share/nonexistent"
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			shareARN := tt.setup(t, h)

			rec := doRAMRequest(t, h, "/disassociateresourceshare", map[string]any{
				"resourceShareArn": shareARN,
				"principals":       []string{"123456789012"},
			})

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetResourceShareAssociations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *ram.Handler) string
		name       string
		wantBody   string
		wantStatus int
	}{
		{
			name: "list associations",
			setup: func(t *testing.T, h *ram.Handler) string {
				t.Helper()
				rs, err := h.Backend.CreateResourceShare("assoc-list-share", true, nil, []string{"123456789012"}, nil)
				require.NoError(t, err)

				return rs.ARN
			},
			wantStatus: http.StatusOK,
			wantBody:   "123456789012",
		},
		{
			name: "empty",
			setup: func(_ *testing.T, _ *ram.Handler) string {
				return ""
			},
			wantStatus: http.StatusOK,
			wantBody:   "resourceShareAssociations",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			shareARN := tt.setup(t, h)

			body := map[string]any{"associationType": "PRINCIPAL"}
			if shareARN != "" {
				body["resourceShareArns"] = []string{shareARN}
			}

			rec := doRAMRequest(t, h, "/getresourceshareassociations", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_TagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *ram.Handler) string
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *ram.Handler) string {
				t.Helper()
				rs, err := h.Backend.CreateResourceShare("tag-share", true, nil, nil, nil)
				require.NoError(t, err)

				return rs.ARN
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not found",
			setup: func(_ *testing.T, _ *ram.Handler) string {
				return "arn:aws:ram:us-east-1:000000000000:resource-share/nonexistent"
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			shareARN := tt.setup(t, h)

			rec := doRAMRequest(t, h, "/tagresource", map[string]any{
				"resourceShareArn": shareARN,
				"tags":             []map[string]string{{"key": "Env", "value": "test"}},
			})

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_UntagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *ram.Handler) string
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *ram.Handler) string {
				t.Helper()
				rs, err := h.Backend.CreateResourceShare(
					"untag-share",
					true,
					map[string]string{"Env": "test"},
					nil,
					nil,
				)
				require.NoError(t, err)

				return rs.ARN
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not found",
			setup: func(_ *testing.T, _ *ram.Handler) string {
				return "arn:aws:ram:us-east-1:000000000000:resource-share/nonexistent"
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			shareARN := tt.setup(t, h)

			rec := doRAMRequest(t, h, "/untagresource", map[string]any{
				"resourceShareArn": shareARN,
				"tagKeys":          []string{"Env"},
			})

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListTagsForResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *ram.Handler) string
		name       string
		wantBody   string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *ram.Handler) string {
				t.Helper()
				rs, err := h.Backend.CreateResourceShare(
					"listtag-share",
					true,
					map[string]string{"Env": "prod"},
					nil,
					nil,
				)
				require.NoError(t, err)

				return rs.ARN
			},
			wantStatus: http.StatusOK,
			wantBody:   "Env",
		},
		{
			name: "not found",
			setup: func(_ *testing.T, _ *ram.Handler) string {
				return "arn:aws:ram:us-east-1:000000000000:resource-share/nonexistent"
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			shareARN := tt.setup(t, h)

			rec := doRAMRequest(t, h, "/listtagsforresource", map[string]any{
				"resourceShareArn": shareARN,
			})

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_EnableSharingWithAwsOrganization(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRawRequest(t, h, http.MethodPost, "/enablesharingwithawsorganization", nil)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "returnValue")
}

func TestHandler_ListResourceSharePermissions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rs, err := h.Backend.CreateResourceShare("perm-share", true, nil, nil, nil)
	require.NoError(t, err)

	rec := doRAMRequest(t, h, "/listresourcesharepermissions", map[string]any{
		"resourceShareArn": rs.ARN,
	})

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "permissions")
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		want string
	}{
		{name: "create", path: "/createresourceshare", want: "CreateResourceShare"},
		{name: "get", path: "/getresourceshares", want: "GetResourceShares"},
		{name: "update", path: "/updateresourceshare", want: "UpdateResourceShare"},
		{name: "delete", path: "/deleteresourceshare", want: "DeleteResourceShare"},
		{name: "associate", path: "/associateresourceshare", want: "AssociateResourceShare"},
		{name: "disassociate", path: "/disassociateresourceshare", want: "DisassociateResourceShare"},
		{name: "get associations", path: "/getresourceshareassociations", want: "GetResourceShareAssociations"},
		{name: "tag", path: "/tagresource", want: "TagResource"},
		{name: "untag", path: "/untagresource", want: "UntagResource"},
		{name: "list tags", path: "/listtagsforresource", want: "ListTagsForResource"},
		{name: "list permissions", path: "/listresourcesharepermissions", want: "ListResourceSharePermissions"},
		{
			name: "enable org sharing",
			path: "/enablesharingwithawsorganization",
			want: "EnableSharingWithAwsOrganization",
		},
		{name: "unknown", path: "/unknownpath", want: "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.want, h.ExtractOperation(c))
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	req := httptest.NewRequest(http.MethodDelete,
		"/deleteresourceshare?resourceShareArn=arn:aws:ram:us-east-1:000000000000:resource-share/my-share",
		nil)
	c := e.NewContext(req, httptest.NewRecorder())

	got := h.ExtractResource(c)
	assert.Equal(t, "arn:aws:ram:us-east-1:000000000000:resource-share/my-share", got)
}

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	p := &ram.Provider{}
	assert.Equal(t, "RAM", p.Name())
}

func TestBackend_Region(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-west-2")
	assert.Equal(t, "us-west-2", b.Region())
}

func TestBackend_GetResourceShare(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*testing.T, *ram.InMemoryBackend) string
		name    string
		wantErr bool
	}{
		{
			name: "found",
			setup: func(t *testing.T, b *ram.InMemoryBackend) string {
				t.Helper()
				rs, err := b.CreateResourceShare("found-share", true, nil, nil, nil)
				require.NoError(t, err)

				return rs.ARN
			},
		},
		{
			name: "not found",
			setup: func(_ *testing.T, _ *ram.InMemoryBackend) string {
				return "arn:aws:ram:us-east-1:000000000000:resource-share/missing"
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ram.NewInMemoryBackend("000000000000", "us-east-1")
			shareARN := tt.setup(t, b)

			rs, err := b.GetResourceShare(shareARN)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, shareARN, rs.ARN)
		})
	}
}

func TestHandler_GetResourceShares_ByARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rs, err := h.Backend.CreateResourceShare("by-arn-share", true, nil, nil, nil)
	require.NoError(t, err)

	rec := doRAMRequest(t, h, "/getresourceshares", map[string]any{
		"resourceOwner":     "SELF",
		"resourceShareArns": []string{rs.ARN},
	})

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "by-arn-share")
}

func TestHandler_TagResource_MissingARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRequest(t, h, "/tagresource", map[string]any{
		"tags": []map[string]string{{"key": "Env", "value": "test"}},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UntagResource_MissingARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRequest(t, h, "/untagresource", map[string]any{
		"tagKeys": []string{"Env"},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListTagsForResource_MissingARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRequest(t, h, "/listtagsforresource", map[string]any{})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UpdateResourceShare_MissingARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRequest(t, h, "/updateresourceshare", map[string]any{
		"name": "updated",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_AssociateResourceShare_MissingARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRequest(t, h, "/associateresourceshare", map[string]any{
		"principals": []string{"123456789012"},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DisassociateResourceShare_MissingARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRequest(t, h, "/disassociateresourceshare", map[string]any{
		"principals": []string{"123456789012"},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_CreateResourceShare_WithTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRequest(t, h, "/createresourceshare", map[string]any{
		"name":                    "tagged-share",
		"allowExternalPrincipals": true,
		"tags":                    []map[string]string{{"key": "Env", "value": "prod"}},
	})

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "tagged-share")
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	matcher := h.RouteMatcher()
	e := echo.New()

	tests := []struct {
		name    string
		path    string
		service string
		want    bool
	}{
		{
			name:    "createresourceshare with ram service",
			path:    "/createresourceshare",
			service: "ram",
			want:    true,
		},
		{
			name:    "getresourceshares with ram service",
			path:    "/getresourceshares",
			service: "ram",
			want:    true,
		},
		{
			name:    "wrong service",
			path:    "/createresourceshare",
			service: "s3",
			want:    false,
		},
		{
			name:    "unknown path",
			path:    "/unknownpath",
			service: "ram",
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, tt.path, nil)
			req.Header.Set(
				"Authorization",
				"AWS4-HMAC-SHA256 Credential=test/20230101/us-east-1/"+tt.service+"/aws4_request",
			)
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.want, matcher(c))
		})
	}
}

func TestRAM_Backend_DeleteResourceShare_RemovesFromMemory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		principals []string
	}{
		{
			name:       "share with associations is fully removed",
			principals: []string{"123456789012"},
		},
		{
			name:       "share without associations is fully removed",
			principals: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ram.NewInMemoryBackend("000000000000", "us-east-1")
			rs, err := b.CreateResourceShare("del-test", true, nil, tt.principals, nil)
			require.NoError(t, err)

			err = b.DeleteResourceShare(rs.ARN)
			require.NoError(t, err)

			// Share should not be retrievable.
			_, err = b.GetResourceShare(rs.ARN)
			require.Error(t, err)

			// Associations for the deleted share should be gone.
			assocs := b.GetResourceShareAssociations("", []string{rs.ARN})
			assert.Empty(t, assocs, "associations for deleted share should be removed")
		})
	}
}

func TestRAM_Backend_DisassociateResourceShare_RemovesFromSlice(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		initials      []string
		toDisassoc    []string
		wantRemaining int
	}{
		{
			name:          "disassociate one of two principals",
			initials:      []string{"111111111111", "222222222222"},
			toDisassoc:    []string{"111111111111"},
			wantRemaining: 1,
		},
		{
			name:          "disassociate all principals",
			initials:      []string{"111111111111", "222222222222"},
			toDisassoc:    []string{"111111111111", "222222222222"},
			wantRemaining: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ram.NewInMemoryBackend("000000000000", "us-east-1")
			rs, err := b.CreateResourceShare("disassoc-test", true, nil, tt.initials, nil)
			require.NoError(t, err)

			_, err = b.DisassociateResourceShare(rs.ARN, tt.toDisassoc, nil)
			require.NoError(t, err)

			assocs := b.GetResourceShareAssociations("PRINCIPAL", []string{rs.ARN})
			assert.Len(t, assocs, tt.wantRemaining)
		})
	}
}

func TestRAM_Backend_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		createShares   int
		wantAfterReset int
	}{
		{
			name:           "reset clears all shares",
			createShares:   3,
			wantAfterReset: 0,
		},
		{
			name:           "reset on empty backend is a no-op",
			createShares:   0,
			wantAfterReset: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ram.NewInMemoryBackend("000000000000", "us-east-1")

			for i := range tt.createShares {
				_, err := b.CreateResourceShare(
					fmt.Sprintf("share-%d", i),
					true, nil, nil, nil,
				)
				require.NoError(t, err)
			}

			b.Reset()

			shares := b.ListResourceShares("SELF")
			assert.Len(t, shares, tt.wantAfterReset)
		})
	}
}

func TestRAM_Handler_Reset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRAMRequest(t, h, "/createresourceshare", map[string]any{
		"name":                    "reset-share",
		"allowExternalPrincipals": true,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	h.Reset()

	listRec := doRAMRequest(t, h, "/getresourceshares", map[string]any{
		"resourceOwner": "SELF",
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var out struct {
		ResourceShares []any `json:"resourceShares"`
	}

	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
	assert.Empty(t, out.ResourceShares)
}

func TestRAM_Backend_AssociateResourceShare_Idempotent(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")
	rs, err := b.CreateResourceShare("idem-share", true, nil, nil, nil)
	require.NoError(t, err)

	// First association creates one entry.
	added1, err := b.AssociateResourceShare(rs.ARN, []string{"111111111111"}, nil)
	require.NoError(t, err)
	assert.Len(t, added1, 1)

	// Repeated association with the same entity must be a no-op (no new entry).
	added2, err := b.AssociateResourceShare(rs.ARN, []string{"111111111111"}, nil)
	require.NoError(t, err)
	assert.Empty(t, added2, "re-associating the same entity must return no new associations")

	// Exactly one association should exist in total.
	assocs := b.GetResourceShareAssociations("PRINCIPAL", []string{rs.ARN})
	assert.Len(t, assocs, 1)
}
