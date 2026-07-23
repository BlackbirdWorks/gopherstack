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
	"github.com/blackbirdworks/gopherstack/services/s3control"
)

func newTestS3ControlHandler(t *testing.T) *s3control.Handler {
	t.Helper()

	return s3control.NewHandler(s3control.NewInMemoryBackend())
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

// doS3ControlNewOpRequest sends a request to the S3Control handler at the
// given path and account ID. Shared by tests across every op family.
func doS3ControlNewOpRequest(
	t *testing.T,
	h *s3control.Handler,
	method, path, accountID, body string,
) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if accountID != "" {
		req.Header.Set("X-Amz-Account-Id", accountID)
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// doS3Request sends a request to the S3Control handler at the given path.
// The account ID is fixed to "acct1" for all test requests.
func doS3Request(
	t *testing.T,
	h *s3control.Handler,
	method, path, body string,
) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("X-Amz-Account-Id", "acct1")

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

// TestHandler_ChaosOps tests the chaos-related handler methods.
func TestHandler_ChaosOps(t *testing.T) {
	t.Parallel()

	h := newTestS3ControlHandler(t)

	tests := []struct {
		want    any
		checkFn func() any
		name    string
	}{
		{
			name:    "chaos_service_name",
			want:    "s3",
			checkFn: func() any { return h.ChaosServiceName() },
		},
		{
			name:    "chaos_regions_not_empty",
			want:    true,
			checkFn: func() any { return len(h.ChaosRegions()) > 0 },
		},
		{
			name:    "chaos_operations_not_empty",
			want:    true,
			checkFn: func() any { return len(h.ChaosOperations()) > 0 },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := tt.checkFn()
			assert.Equal(t, tt.want, result)
		})
	}
}

// TestHandler_NotFoundFallthrough tests the 404 fallthrough for the dispatch chain.
func TestHandler_NotFoundFallthrough(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "unknown_path",
			method:     http.MethodGet,
			path:       "/v20180820/unknownresource/foo",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestS3ControlHandler(t)
			rec := doS3Request(t, h, tt.method, tt.path, "")
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// ---- General backend/handler surface tests ----

func TestStorageBackend_Interface(t *testing.T) {
	t.Parallel()

	// Compile-time assertion: var _ StorageBackend = (*InMemoryBackend)(nil) in interfaces.go
	// This test confirms the assertion file exists and compiles.
	var _ s3control.StorageBackend = s3control.NewInMemoryBackend()
}

func TestHandlerOpsLen(t *testing.T) {
	t.Parallel()

	h := s3control.NewHandler(s3control.NewInMemoryBackend())
	// Was 100 before the fabricated GetAccessPointPublicAccessBlock /
	// PutAccessPointPublicAccessBlock / DeleteAccessPointPublicAccessBlock
	// ops were deleted (see handler_access_points_config_test.go's
	// TestNoFabricatedAccessPointPublicAccessBlockRoute) -- those three
	// operations do not exist in aws-sdk-go-v2/service/s3control.
	assert.Equal(t, 97, s3control.HandlerOpsLen(h))
}

func TestProvider_NilAppContext(t *testing.T) {
	t.Parallel()

	p := &s3control.Provider{}
	_, err := p.Init(nil)
	require.ErrorIs(t, err, s3control.ErrNilAppContext)
}

func TestBackend_Reset(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	b.PutPublicAccessBlock(s3control.PublicAccessBlock{AccountID: "acc1", BlockPublicAcls: true})
	b.AddBatchJobInternal("acc1", "arn:aws:iam::acc1:role/R", 5)

	require.Equal(t, 1, s3control.AccessBlockCount(b))
	require.Equal(t, 1, s3control.BatchJobCount(b))

	b.Reset()

	assert.Equal(t, 0, s3control.AccessBlockCount(b))
	assert.Equal(t, 0, s3control.BatchJobCount(b))
}

func TestHandler_Reset(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()
	h := s3control.NewHandler(b)
	b.PutPublicAccessBlock(s3control.PublicAccessBlock{AccountID: "x", BlockPublicAcls: true})

	require.Equal(t, 1, s3control.AccessBlockCount(b))

	h.Reset()

	assert.Equal(t, 0, s3control.AccessBlockCount(b))
}

func TestAccountID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		accountID string
		wantID    string
	}{
		{name: "custom", accountID: "111122223333", wantID: "111122223333"},
		{name: "default", accountID: "000000000000", wantID: "000000000000"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackendWithConfig(tt.accountID, "us-east-1")
			assert.Equal(t, tt.wantID, b.AccountID())
		})
	}
}

func TestRegion(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackendWithConfig("123456789012", "eu-west-1")
	assert.Equal(t, "eu-west-1", b.Region())
}

func TestSeedHelpers(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()

	block := &s3control.PublicAccessBlock{AccountID: "a1", BlockPublicAcls: true}
	b.AddPublicAccessBlockInternal("a1", block)
	assert.Equal(t, 1, s3control.AccessBlockCount(b))

	inst := b.AddAccessGrantsInstanceInternal("a1", "arn:aws:sso:::instance/ins-1")
	require.NotNil(t, inst)
	assert.Equal(t, 1, s3control.AccessGrantsInstanceCount(b))

	grant := b.AddAccessGrantInternal("a1", "loc1", "DIRECTORY_USER", "u@x.com", "READ")
	require.NotNil(t, grant)
	assert.Equal(t, 1, s3control.AccessGrantCount(b))

	ap := b.AddAccessPointInternal("a1", "my-ap", "my-bucket")
	require.NotNil(t, ap)
	assert.Equal(t, 1, s3control.AccessPointCount(b))

	job := b.AddBatchJobInternal("a1", "arn:aws:iam::a1:role/R", 10)
	require.NotNil(t, job)
	assert.Equal(t, 1, s3control.BatchJobCount(b))
}

func TestARNsUseConfiguredRegion(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackendWithConfig("123456789012", "ap-southeast-1")
	inst := b.CreateAccessGrantsInstance("123456789012", "")
	assert.Contains(t, inst.AccessGrantsInstanceArn, "ap-southeast-1")

	ap := b.CreateAccessPoint("123456789012", "my-ap", "my-bucket")
	assert.Contains(t, ap.AccessPointArn, "ap-southeast-1")
}

func TestExportCountHelpers(t *testing.T) {
	t.Parallel()

	b := s3control.NewInMemoryBackend()

	assert.Equal(t, 0, s3control.AccessBlockCount(b))
	assert.Equal(t, 0, s3control.AccessGrantsInstanceCount(b))
	assert.Equal(t, 0, s3control.AccessGrantCount(b))
	assert.Equal(t, 0, s3control.AccessGrantsLocationCount(b))
	assert.Equal(t, 0, s3control.AccessPointCount(b))
	assert.Equal(t, 0, s3control.ObjectLambdaAccessPointCount(b))
	assert.Equal(t, 0, s3control.OutpostsBucketCount(b))
	assert.Equal(t, 0, s3control.BatchJobCount(b))
	assert.Equal(t, 0, s3control.MRAPRequestCount(b))
	assert.Equal(t, 0, s3control.StorageLensGroupCount(b))

	b.CreateAccessGrantsInstance("a1", "")
	assert.Equal(t, 1, s3control.AccessGrantsInstanceCount(b))

	loc := b.CreateAccessGrantsLocation("a1", "s3://bucket", "arn:aws:iam::a1:role/R")
	require.NotNil(t, loc)
	assert.Equal(t, 1, s3control.AccessGrantsLocationCount(b))

	b.CreateAccessPoint("a1", "ap1", "bucket1")
	assert.Equal(t, 1, s3control.AccessPointCount(b))

	b.CreateAccessPointForObjectLambda("a1", "olap1")
	assert.Equal(t, 1, s3control.ObjectLambdaAccessPointCount(b))

	b.CreateBucket("a1", "outpost-bucket")
	assert.Equal(t, 1, s3control.OutpostsBucketCount(b))

	b.CreateMultiRegionAccessPoint("a1", "mrap1", "token1")
	assert.Equal(t, 1, s3control.MRAPRequestCount(b))

	b.CreateStorageLensGroup("a1", "group1")
	assert.Equal(t, 1, s3control.StorageLensGroupCount(b))
}
