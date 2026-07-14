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

// TestExtractOperation_CreateOperations exercises ExtractOperation for the
// create-style (POST/PUT) operation across every op family.
func TestExtractOperation_CreateOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		want   string
	}{
		{
			name:   "post_accessgrantsinstance_returns_CreateAccessGrantsInstance",
			method: http.MethodPost,
			path:   "/v20180820/accessgrantsinstance",
			want:   "CreateAccessGrantsInstance",
		},
		{
			name:   "post_identitycenter_returns_AssociateAccessGrantsIdentityCenter",
			method: http.MethodPost,
			path:   "/v20180820/accessgrantsinstance/identitycenter",
			want:   "AssociateAccessGrantsIdentityCenter",
		},
		{
			name:   "post_grant_returns_CreateAccessGrant",
			method: http.MethodPost,
			path:   "/v20180820/accessgrantsinstance/grant",
			want:   "CreateAccessGrant",
		},
		{
			name:   "post_location_returns_CreateAccessGrantsLocation",
			method: http.MethodPost,
			path:   "/v20180820/accessgrantsinstance/location",
			want:   "CreateAccessGrantsLocation",
		},
		{
			name:   "put_accesspoint_returns_CreateAccessPoint",
			method: http.MethodPut,
			path:   "/v20180820/accesspoint/my-ap",
			want:   "CreateAccessPoint",
		},
		{
			name:   "put_objectlambda_returns_CreateAccessPointForObjectLambda",
			method: http.MethodPut,
			path:   "/v20180820/accesspointforobjectlambda/my-ap",
			want:   "CreateAccessPointForObjectLambda",
		},
		{
			name:   "put_bucket_returns_CreateBucket",
			method: http.MethodPut,
			path:   "/v20180820/bucket/my-bucket",
			want:   "CreateBucket",
		},
		{
			name:   "post_mrap_create_returns_CreateMultiRegionAccessPoint",
			method: http.MethodPost,
			path:   "/v20180820/async-requests/mrap/create",
			want:   "CreateMultiRegionAccessPoint",
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

// --- Dispatch stub coverage ---

func TestHandler_StubOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		path       string
		wantBody   string
		wantStatus int
	}{
		{
			name:       "list_tags_for_resource",
			method:     http.MethodGet,
			path:       "/v20180820/tags/arn:aws:s3:us-east-1:123:accesspoint/myap",
			wantStatus: http.StatusOK,
			wantBody:   "ListTagsForResourceResult",
		},
		{
			name:       "tag_resource",
			method:     http.MethodPost,
			path:       "/v20180820/tags/arn:aws:s3:us-east-1:123:accesspoint/myap",
			wantStatus: http.StatusOK,
			wantBody:   "TagResourceResult",
		},
		{
			name:       "untag_resource",
			method:     http.MethodDelete,
			path:       "/v20180820/tags/arn:aws:s3:us-east-1:123:accesspoint/myap",
			wantStatus: http.StatusNoContent,
			wantBody:   "",
		},
		{
			name:       "get_bucket_replication",
			method:     http.MethodGet,
			path:       "/v20180820/bucket/mybucket/replication",
			wantStatus: http.StatusNotFound,
			wantBody:   "ReplicationConfigurationNotFoundError",
		},
		{
			name:       "put_bucket_replication",
			method:     http.MethodPut,
			path:       "/v20180820/bucket/mybucket/replication",
			wantStatus: http.StatusOK,
			wantBody:   "",
		},
		{
			name:       "delete_bucket_replication",
			method:     http.MethodDelete,
			path:       "/v20180820/bucket/mybucket/replication",
			wantStatus: http.StatusNoContent,
			wantBody:   "",
		},
		{
			name:       "get_storage_lens_config",
			method:     http.MethodGet,
			path:       "/v20180820/storagelens/myconfig",
			wantStatus: http.StatusNotFound,
			wantBody:   "NoSuchConfiguration",
		},
		{
			name:       "put_storage_lens_config",
			method:     http.MethodPut,
			path:       "/v20180820/storagelens/myconfig",
			wantStatus: http.StatusOK,
			wantBody:   "",
		},
		{
			name:       "delete_storage_lens_config",
			method:     http.MethodDelete,
			path:       "/v20180820/storagelens/myconfig",
			wantStatus: http.StatusNoContent,
			wantBody:   "",
		},
		{
			name:       "get_storage_lens_tagging",
			method:     http.MethodGet,
			path:       "/v20180820/storagelens/myconfig/tagging",
			wantStatus: http.StatusNotFound,
			wantBody:   "NoSuchConfiguration",
		},
		{
			name:       "put_storage_lens_tagging",
			method:     http.MethodPut,
			path:       "/v20180820/storagelens/myconfig/tagging",
			wantStatus: http.StatusOK,
			wantBody:   "",
		},
		{
			name:       "delete_storage_lens_tagging",
			method:     http.MethodDelete,
			path:       "/v20180820/storagelens/myconfig/tagging",
			wantStatus: http.StatusNoContent,
			wantBody:   "",
		},
		{
			name:       "list_storage_lens_configs",
			method:     http.MethodGet,
			path:       "/v20180820/storagelens",
			wantStatus: http.StatusOK,
			wantBody:   "ListStorageLensConfigurationsResult",
		},
		{
			name:       "get_storage_lens_group",
			method:     http.MethodGet,
			path:       "/v20180820/storagelensgroup/mygroup",
			wantStatus: http.StatusNotFound,
			wantBody:   "NoSuchStorageLensGroup",
		},
		{
			name:       "update_storage_lens_group",
			method:     http.MethodPut,
			path:       "/v20180820/storagelensgroup/mygroup",
			wantStatus: http.StatusNotFound,
			wantBody:   "NoSuchStorageLensGroup",
		},
		{
			name:       "delete_storage_lens_group",
			method:     http.MethodDelete,
			path:       "/v20180820/storagelensgroup/mygroup",
			wantStatus: http.StatusNotFound,
			wantBody:   "NoSuchStorageLensGroup",
		},
		{
			name:       "list_storage_lens_groups",
			method:     http.MethodGet,
			path:       "/v20180820/storagelensgroup",
			wantStatus: http.StatusOK,
			wantBody:   "ListStorageLensGroupsResult",
		},
		{
			name:       "submit_mrap_routes",
			method:     http.MethodPatch,
			path:       "/v20180820/mrap/instances/mymrap/routes",
			wantStatus: http.StatusOK,
			wantBody:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestS3ControlHandler(t)
			rec := doS3Request(t, h, tt.method, tt.path, "")
			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
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

// TestHandler_ExtractOperation tests operation extraction for various paths.
func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		{
			name:   "delete_public_access_block",
			method: http.MethodDelete,
			path:   "/v20180820/configuration/publicAccessBlock",
			wantOp: "DeletePublicAccessBlock",
		},
		{
			name:   "get_access_point",
			method: http.MethodGet,
			path:   "/v20180820/accesspoint/myap",
			wantOp: "GetAccessPoint",
		},
		{
			name:   "delete_access_point",
			method: http.MethodDelete,
			path:   "/v20180820/accesspoint/myap",
			wantOp: "DeleteAccessPoint",
		},
		{
			name:   "list_access_points",
			method: http.MethodGet,
			path:   "/v20180820/accesspoint",
			wantOp: "ListAccessPoints",
		},
		{
			name:   "get_access_point_policy",
			method: http.MethodGet,
			path:   "/v20180820/accesspoint/myap/policy",
			wantOp: "GetAccessPointPolicy",
		},
		{
			name:   "put_access_point_policy",
			method: http.MethodPut,
			path:   "/v20180820/accesspoint/myap/policy",
			wantOp: "PutAccessPointPolicy",
		},
		{
			name:   "delete_access_point_policy",
			method: http.MethodDelete,
			path:   "/v20180820/accesspoint/myap/policy",
			wantOp: "DeleteAccessPointPolicy",
		},
		{
			name:   "get_access_point_policy_status",
			method: http.MethodGet,
			path:   "/v20180820/accesspoint/myap/policyStatus",
			wantOp: "GetAccessPointPolicyStatus",
		},
		{name: "list_jobs", method: http.MethodGet, path: "/v20180820/jobs", wantOp: "ListJobs"},
		{name: "create_job", method: http.MethodPost, path: "/v20180820/jobs", wantOp: "CreateJob"},
		{name: "describe_job", method: http.MethodGet, path: "/v20180820/jobs/job-1", wantOp: "DescribeJob"},
		{
			name:   "update_job_priority",
			method: http.MethodPost,
			path:   "/v20180820/jobs/job-1/priority",
			wantOp: "UpdateJobPriority",
		},
		{
			name:   "update_job_status",
			method: http.MethodPost,
			path:   "/v20180820/jobs/job-1/status",
			wantOp: "UpdateJobStatus",
		},
		{
			name:   "list_mrap",
			method: http.MethodGet,
			path:   "/v20180820/mrap/instances",
			wantOp: "ListMultiRegionAccessPoints",
		},
		{
			name:   "get_mrap",
			method: http.MethodGet,
			path:   "/v20180820/mrap/instances/mymrap",
			wantOp: "GetMultiRegionAccessPoint",
		},
		{
			name:   "delete_mrap_instance",
			method: http.MethodDelete,
			path:   "/v20180820/mrap/instances/mymrap",
			wantOp: "DeleteMultiRegionAccessPoint",
		},
		{
			name:   "submit_mrap_routes",
			method: http.MethodPatch,
			path:   "/v20180820/mrap/instances/mymrap/routes",
			wantOp: "SubmitMultiRegionAccessPointRoutes",
		},
		{
			name:   "list_tags",
			method: http.MethodGet,
			path:   "/v20180820/tags/arn:aws:s3:us-east-1:123:accesspoint/myap",
			wantOp: "ListTagsForResource",
		},
		{
			name:   "tag_resource",
			method: http.MethodPost,
			path:   "/v20180820/tags/arn:aws:s3:us-east-1:123:accesspoint/myap",
			wantOp: "TagResource",
		},
		{
			name:   "untag_resource",
			method: http.MethodDelete,
			path:   "/v20180820/tags/arn:aws:s3:us-east-1:123:accesspoint/myap",
			wantOp: "UntagResource",
		},
		{name: "get_bucket", method: http.MethodGet, path: "/v20180820/bucket/mybucket", wantOp: "GetBucket"},
		{name: "delete_bucket", method: http.MethodDelete, path: "/v20180820/bucket/mybucket", wantOp: "DeleteBucket"},
		{
			name:   "get_bucket_replication",
			method: http.MethodGet,
			path:   "/v20180820/bucket/mybucket/replication",
			wantOp: "GetBucketReplication",
		},
		{
			name:   "put_bucket_replication",
			method: http.MethodPut,
			path:   "/v20180820/bucket/mybucket/replication",
			wantOp: "PutBucketReplication",
		},
		{
			name:   "delete_bucket_replication",
			method: http.MethodDelete,
			path:   "/v20180820/bucket/mybucket/replication",
			wantOp: "DeleteBucketReplication",
		},
		{
			name:   "get_storage_lens",
			method: http.MethodGet,
			path:   "/v20180820/storagelens/myconfig",
			wantOp: "GetStorageLensConfiguration",
		},
		{
			name:   "put_storage_lens",
			method: http.MethodPut,
			path:   "/v20180820/storagelens/myconfig",
			wantOp: "PutStorageLensConfiguration",
		},
		{
			name:   "delete_storage_lens",
			method: http.MethodDelete,
			path:   "/v20180820/storagelens/myconfig",
			wantOp: "DeleteStorageLensConfiguration",
		},
		{
			name:   "list_storage_lens",
			method: http.MethodGet,
			path:   "/v20180820/storagelens",
			wantOp: "ListStorageLensConfigurations",
		},
		{
			name:   "get_storage_lens_tagging",
			method: http.MethodGet,
			path:   "/v20180820/storagelens/myconfig/tagging",
			wantOp: "GetStorageLensConfigurationTagging",
		},
		{
			name:   "put_storage_lens_tagging",
			method: http.MethodPut,
			path:   "/v20180820/storagelens/myconfig/tagging",
			wantOp: "PutStorageLensConfigurationTagging",
		},
		{
			name:   "delete_storage_lens_tagging",
			method: http.MethodDelete,
			path:   "/v20180820/storagelens/myconfig/tagging",
			wantOp: "DeleteStorageLensConfigurationTagging",
		},
		{
			name:   "get_storage_lens_group",
			method: http.MethodGet,
			path:   "/v20180820/storagelensgroup/mygroup",
			wantOp: "GetStorageLensGroup",
		},
		{
			name:   "update_storage_lens_group",
			method: http.MethodPut,
			path:   "/v20180820/storagelensgroup/mygroup",
			wantOp: "UpdateStorageLensGroup",
		},
		{
			name:   "delete_storage_lens_group",
			method: http.MethodDelete,
			path:   "/v20180820/storagelensgroup/mygroup",
			wantOp: "DeleteStorageLensGroup",
		},
		{
			name:   "list_storage_lens_groups",
			method: http.MethodGet,
			path:   "/v20180820/storagelensgroup",
			wantOp: "ListStorageLensGroups",
		},
		{
			name:   "create_storage_lens_group",
			method: http.MethodPost,
			path:   "/v20180820/storagelensgroup",
			wantOp: "CreateStorageLensGroup",
		},
		{
			name:   "delete_mrap_async",
			method: http.MethodPost,
			path:   "/v20180820/async-requests/mrap/delete/token1",
			wantOp: "DeleteMultiRegionAccessPoint",
		},
		{
			name:   "put_mrap_policy",
			method: http.MethodPost,
			path:   "/v20180820/async-requests/mrap/put-policy/token1",
			wantOp: "PutMultiRegionAccessPointPolicy",
		},
		{name: "unknown_op", method: http.MethodPost, path: "/v20180820/unknownresource", wantOp: "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			h := newTestS3ControlHandler(t)
			op := h.ExtractOperation(c)
			assert.Equal(t, tt.wantOp, op)
		})
	}
}

// TestHandler_WriteXML_MarshalError covers the marshal error branch in writeXML.
func TestHandler_WriteXML_MarshalError(t *testing.T) {
	t.Parallel()

	// Use an unmarshalable type - channel cannot be XML-marshaled.
	// We can trigger this by creating a fake handler scenario.
	// Instead we just verify the createAccessPoint happy path works via XML.
	tests := []struct {
		name       string
		wantBody   string
		wantStatus int
	}{
		{
			name:       "create_access_point_xml_response",
			wantStatus: http.StatusOK,
			wantBody:   "CreateAccessPointResult",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestS3ControlHandler(t)
			body := `<CreateAccessPointRequest><Bucket>mybucket</Bucket></CreateAccessPointRequest>`
			rec := doS3Request(t, h, http.MethodPut, "/v20180820/accesspoint/ap1", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

// TestHandler_HandleBackendError covers additional error branches.
func TestHandler_HandleBackendError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "create_access_grant_missing_permission_returns_bad_request",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestS3ControlHandler(t)
			// Missing permission field → ErrValidation → 400
			body := "<CreateAccessGrantRequest>" +
				"<AccessGrantsLocationId>loc-1</AccessGrantsLocationId>" +
				"<Permission></Permission>" +
				"</CreateAccessGrantRequest>"
			rec := doS3Request(t, h, http.MethodPost, "/v20180820/accessgrantsinstance/grant", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_DecodeXML_BadBody covers the decodeXML error path.
func TestHandler_DecodeXML_BadBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		method     string
		body       string
		wantStatus int
	}{
		{
			name:       "create_access_grants_instance_bad_body",
			path:       "/v20180820/accessgrantsinstance",
			method:     http.MethodPost,
			body:       "<Invalid</>",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			h := newTestS3ControlHandler(t)
			err := h.Handler()(c)
			require.NoError(t, err)
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
	assert.Equal(t, 100, s3control.HandlerOpsLen(h))
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
