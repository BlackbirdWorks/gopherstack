package s3control_test

import (
	"encoding/xml"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/s3control"
)

func newTestS3ControlHandler(t *testing.T) *s3control.Handler {
	t.Helper()

	return s3control.NewHandler(s3control.NewInMemoryBackend(), slog.Default())
}

const publicAccessBlockPath = "/v20180820/configuration/publicAccessBlock"

func doS3ControlRequest(
	t *testing.T,
	h *s3control.Handler,
	method, accountID, body string,
) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(method, publicAccessBlockPath, strings.NewReader(body))
	if accountID != "" {
		req.Header.Set("X-Amz-Account-Id", accountID)
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestS3Control_Handler_PublicAccessBlockFlows(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *s3control.Handler)
		name string
	}{
		{
			name: "put_and_get_returns_stored_config",
			run: func(t *testing.T, h *s3control.Handler) {
				t.Helper()

				putBody := `<PublicAccessBlockConfiguration>
					<BlockPublicAcls>true</BlockPublicAcls>
					<IgnorePublicAcls>true</IgnorePublicAcls>
					<BlockPublicPolicy>false</BlockPublicPolicy>
					<RestrictPublicBuckets>false</RestrictPublicBuckets>
				</PublicAccessBlockConfiguration>`

				putRec := doS3ControlRequest(t, h, http.MethodPut, "000000000000", putBody)
				assert.Equal(t, http.StatusCreated, putRec.Code)

				getRec := doS3ControlRequest(t, h, http.MethodGet, "000000000000", "")
				require.Equal(t, http.StatusOK, getRec.Code)
				assert.Contains(t, getRec.Body.String(), "BlockPublicAcls")
			},
		},
		{
			name: "put_then_delete_then_get_returns_not_found",
			run: func(t *testing.T, h *s3control.Handler) {
				t.Helper()

				putBody := `<PublicAccessBlockConfiguration>` +
					`<BlockPublicAcls>true</BlockPublicAcls>` +
					`</PublicAccessBlockConfiguration>`
				doS3ControlRequest(t, h, http.MethodPut, "000000000000", putBody)

				delRec := doS3ControlRequest(t, h, http.MethodDelete, "000000000000", "")
				assert.Equal(t, http.StatusNoContent, delRec.Code)

				getRec := doS3ControlRequest(t, h, http.MethodGet, "000000000000", "")
				assert.Equal(t, http.StatusNotFound, getRec.Code)
			},
		},
		{
			name: "default_account_put_get_delete",
			run: func(t *testing.T, h *s3control.Handler) {
				t.Helper()

				putBody := `<PublicAccessBlockConfiguration>` +
					`<BlockPublicAcls>true</BlockPublicAcls>` +
					`</PublicAccessBlockConfiguration>`

				putRec := doS3ControlRequest(t, h, http.MethodPut, "", putBody)
				assert.Equal(t, http.StatusCreated, putRec.Code)

				getRec := doS3ControlRequest(t, h, http.MethodGet, "", "")
				assert.Equal(t, http.StatusOK, getRec.Code)

				delRec := doS3ControlRequest(t, h, http.MethodDelete, "", "")
				assert.Equal(t, http.StatusNoContent, delRec.Code)
			},
		},
		{
			name: "xml_response_fields_are_correct",
			run: func(t *testing.T, h *s3control.Handler) {
				t.Helper()

				putBody := `<PublicAccessBlockConfiguration>
					<BlockPublicAcls>true</BlockPublicAcls>
					<IgnorePublicAcls>false</IgnorePublicAcls>
					<BlockPublicPolicy>true</BlockPublicPolicy>
					<RestrictPublicBuckets>true</RestrictPublicBuckets>
				</PublicAccessBlockConfiguration>`
				doS3ControlRequest(t, h, http.MethodPut, "test-account", putBody)

				rec := doS3ControlRequest(t, h, http.MethodGet, "test-account", "")
				require.Equal(t, http.StatusOK, rec.Code)

				var out struct {
					XMLName           xml.Name `xml:"PublicAccessBlockConfiguration"`
					BlockPublicAcls   bool     `xml:"BlockPublicAcls"`
					BlockPublicPolicy bool     `xml:"BlockPublicPolicy"`
				}
				require.NoError(t, xml.Unmarshal(rec.Body.Bytes()[len(xml.Header):], &out))
				assert.True(t, out.BlockPublicAcls)
				assert.True(t, out.BlockPublicPolicy)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestS3ControlHandler(t)
			tt.run(t, h)
		})
	}
}

func TestS3Control_Handler_SingleRequestErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		accountID  string
		body       string
		wantStatus int
	}{
		{
			name:       "get_not_found_for_unknown_account",
			method:     http.MethodGet,
			accountID:  "999999999999",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete_not_found_for_nonexistent_account",
			method:     http.MethodDelete,
			accountID:  "nonexistent-account",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "invalid_method_returns_not_found",
			method:     http.MethodPost,
			accountID:  "000000000000",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "put_invalid_xml_returns_bad_request",
			method:     http.MethodPut,
			accountID:  "000000000000",
			body:       "not-xml",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestS3ControlHandler(t)
			rec := doS3ControlRequest(t, h, tt.method, tt.accountID, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestS3Control_Handler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		want bool
	}{
		{
			name: "matches_public_access_block_path",
			path: publicAccessBlockPath,
			want: true,
		},
		{
			name: "no_match_for_other_path",
			path: "/s3/bucket",
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestS3ControlHandler(t)
			matcher := h.RouteMatcher()

			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.want, matcher(c))
		})
	}
}

func TestS3Control_Handler_Meta(t *testing.T) {
	t.Parallel()

	h := newTestS3ControlHandler(t)

	tests := []struct {
		check func(t *testing.T)
		name  string
	}{
		{
			name: "handler_name",
			check: func(t *testing.T) {
				t.Helper()
				assert.Equal(t, "S3Control", h.Name())
			},
		},
		{
			name: "match_priority",
			check: func(t *testing.T) {
				t.Helper()
				assert.Equal(t, 85, h.MatchPriority())
			},
		},
		{
			name: "supported_operations",
			check: func(t *testing.T) {
				t.Helper()
				ops := h.GetSupportedOperations()
				assert.Contains(t, ops, "GetPublicAccessBlock")
				assert.Contains(t, ops, "PutPublicAccessBlock")
				assert.Contains(t, ops, "DeletePublicAccessBlock")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.check(t)
		})
	}
}

func TestS3Control_Handler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		want   string
	}{
		{
			name:   "get_returns_GetPublicAccessBlock",
			method: http.MethodGet,
			path:   publicAccessBlockPath,
			want:   "GetPublicAccessBlock",
		},
		{
			name:   "put_returns_PutPublicAccessBlock",
			method: http.MethodPut,
			path:   publicAccessBlockPath,
			want:   "PutPublicAccessBlock",
		},
		{
			name:   "delete_returns_DeletePublicAccessBlock",
			method: http.MethodDelete,
			path:   publicAccessBlockPath,
			want:   "DeletePublicAccessBlock",
		},
		{
			name:   "unknown_path_returns_Unknown",
			method: http.MethodGet,
			path:   "/other/path",
			want:   "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestS3ControlHandler(t)
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.want, h.ExtractOperation(c))
		})
	}
}

func TestS3Control_Handler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		accountID string
		want      string
	}{
		{
			name:      "extracts_account_id_from_header",
			accountID: "123456789012",
			want:      "123456789012",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestS3ControlHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, publicAccessBlockPath, nil)
			req.Header.Set("X-Amz-Account-Id", tt.accountID)
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.want, h.ExtractResource(c))
		})
	}
}

func TestS3Control_Backend_ListAll(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		entries   []s3control.PublicAccessBlock
		wantCount int
	}{
		{
			name: "empty_initially_then_grows_with_entries",
			entries: []s3control.PublicAccessBlock{
				{AccountID: "acc1", BlockPublicAcls: true},
				{AccountID: "acc2", BlockPublicPolicy: true},
			},
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			assert.Empty(t, b.ListAll())

			for _, entry := range tt.entries {
				b.PutPublicAccessBlock(entry)
			}

			assert.Len(t, b.ListAll(), tt.wantCount)
		})
	}
}

func TestS3Control_Provider(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		testInit bool
	}{
		{
			name: "provider_name",
		},
		{
			name:     "provider_init_returns_valid_service",
			testInit: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &s3control.Provider{}
			assert.Equal(t, "S3Control", p.Name())

			if tt.testInit {
				ctx := &service.AppContext{
					Logger:     slog.Default(),
					JanitorCtx: t.Context(),
				}
				svc, err := p.Init(ctx)
				require.NoError(t, err)
				assert.NotNil(t, svc)
				assert.Equal(t, "S3Control", svc.Name())
			}
		})
	}
}
