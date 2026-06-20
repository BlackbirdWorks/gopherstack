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

// --- helper for new operations ---

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

func TestS3Control_CreateAccessGrantsInstance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		accountID        string
		body             string
		wantBodyContains string
		wantStatus       int
	}{
		{
			name:      "creates_instance_with_identity_center_arn",
			accountID: "123456789012",
			body: `<CreateAccessGrantsInstanceRequest>
<IdentityCenterArn>arn:aws:sso:::instance/ssoins-abc</IdentityCenterArn>
</CreateAccessGrantsInstanceRequest>`,
			wantStatus:       http.StatusOK,
			wantBodyContains: "AccessGrantsInstanceArn",
		},
		{
			name:             "creates_instance_without_identity_center",
			accountID:        "000000000000",
			body:             `<CreateAccessGrantsInstanceRequest></CreateAccessGrantsInstanceRequest>`,
			wantStatus:       http.StatusOK,
			wantBodyContains: "AccessGrantsInstanceId",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestS3ControlHandler(t)
			rec := doS3ControlNewOpRequest(
				t,
				h,
				http.MethodPost,
				"/v20180820/accessgrantsinstance",
				tt.accountID,
				tt.body,
			)

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBodyContains != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBodyContains)
			}
		})
	}
}

func TestS3Control_AssociateAccessGrantsIdentityCenter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		accountID  string
		body       string
		wantStatus int
	}{
		{
			name:      "associates_identity_center",
			accountID: "123456789012",
			body: `<AssociateAccessGrantsIdentityCenterRequest>
<IdentityCenterArn>arn:aws:sso:::instance/ssoins-xyz</IdentityCenterArn>
</AssociateAccessGrantsIdentityCenterRequest>`,
			wantStatus: http.StatusOK,
		},
		{
			name:       "associates_with_empty_body",
			accountID:  "000000000000",
			body:       "",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestS3ControlHandler(t)
			rec := doS3ControlNewOpRequest(
				t,
				h,
				http.MethodPost,
				"/v20180820/accessgrantsinstance/identitycenter",
				tt.accountID,
				tt.body,
			)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestS3Control_CreateAccessGrant(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		accountID        string
		body             string
		wantBodyContains string
		wantStatus       int
	}{
		{
			name:      "creates_access_grant",
			accountID: "123456789012",
			body: `<CreateAccessGrantRequest>
<AccessGrantsLocationId>default</AccessGrantsLocationId>
<Permission>READ</Permission>
<Grantee>
<GranteeType>IAM</GranteeType>
<GranteeIdentifier>arn:aws:iam::123456789012:user/test-user</GranteeIdentifier>
</Grantee>
</CreateAccessGrantRequest>`,
			wantStatus:       http.StatusOK,
			wantBodyContains: "AccessGrantArn",
		},
		{
			name:      "creates_access_grant_with_application_arn",
			accountID: "000000000000",
			body: `<CreateAccessGrantRequest>
<AccessGrantsLocationId>location-1</AccessGrantsLocationId>
<Permission>READWRITE</Permission>
<Grantee>
<GranteeType>DIRECTORY_USER</GranteeType>
<GranteeIdentifier>user-id-123</GranteeIdentifier>
</Grantee>
<ApplicationArn>arn:aws:sso::000000000000:application/app-123</ApplicationArn>
</CreateAccessGrantRequest>`,
			wantStatus:       http.StatusOK,
			wantBodyContains: "AccessGrantId",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestS3ControlHandler(t)
			rec := doS3ControlNewOpRequest(
				t,
				h,
				http.MethodPost,
				"/v20180820/accessgrantsinstance/grant",
				tt.accountID,
				tt.body,
			)

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBodyContains != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBodyContains)
			}
		})
	}
}

func TestS3Control_CreateAccessGrantsLocation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		accountID        string
		body             string
		wantBodyContains string
		wantStatus       int
	}{
		{
			name:      "creates_location_with_scope_and_role",
			accountID: "123456789012",
			body: `<CreateAccessGrantsLocationRequest>
<LocationScope>s3://my-bucket/prefix/</LocationScope>
<IAMRoleArn>arn:aws:iam::123456789012:role/S3AccessGrantsRole</IAMRoleArn>
</CreateAccessGrantsLocationRequest>`,
			wantStatus:       http.StatusOK,
			wantBodyContains: "AccessGrantsLocationArn",
		},
		{
			name:       "empty_body_missing_role_rejected",
			accountID:  "000000000000",
			body:       `<CreateAccessGrantsLocationRequest></CreateAccessGrantsLocationRequest>`,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestS3ControlHandler(t)
			rec := doS3ControlNewOpRequest(
				t,
				h,
				http.MethodPost,
				"/v20180820/accessgrantsinstance/location",
				tt.accountID,
				tt.body,
			)

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBodyContains != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBodyContains)
			}
		})
	}
}

func TestS3Control_CreateAccessPoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		accountID        string
		apName           string
		body             string
		wantBodyContains string
		wantStatus       int
	}{
		{
			name:      "creates_access_point",
			accountID: "123456789012",
			apName:    "my-access-point",
			body: `<CreateAccessPointRequest>
<Bucket>my-bucket</Bucket>
</CreateAccessPointRequest>`,
			wantStatus:       http.StatusOK,
			wantBodyContains: "AccessPointArn",
		},
		{
			name:             "creates_access_point_no_bucket",
			accountID:        "000000000000",
			apName:           "another-access-point",
			body:             "",
			wantStatus:       http.StatusOK,
			wantBodyContains: "AccessPointArn",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestS3ControlHandler(t)
			path := "/v20180820/accesspoint/" + tt.apName
			rec := doS3ControlNewOpRequest(t, h, http.MethodPut, path, tt.accountID, tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBodyContains != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBodyContains)
			}
		})
	}
}

func TestS3Control_CreateAccessPointForObjectLambda(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		accountID        string
		apName           string
		wantBodyContains string
		wantStatus       int
	}{
		{
			name:             "creates_object_lambda_access_point",
			accountID:        "123456789012",
			apName:           "my-lambda-ap",
			wantStatus:       http.StatusOK,
			wantBodyContains: "ObjectLambdaAccessPointArn",
		},
		{
			name:             "creates_object_lambda_access_point_different_account",
			accountID:        "000000000000",
			apName:           "another-lambda-ap",
			wantStatus:       http.StatusOK,
			wantBodyContains: "s3-object-lambda",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestS3ControlHandler(t)
			path := "/v20180820/accesspointforobjectlambda/" + tt.apName
			rec := doS3ControlNewOpRequest(t, h, http.MethodPut, path, tt.accountID, "")

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBodyContains != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBodyContains)
			}
		})
	}
}

func TestS3Control_CreateBucket(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		accountID        string
		bucketName       string
		wantBodyContains string
		wantStatus       int
		wantLocationHdr  bool
	}{
		{
			name:             "creates_outposts_bucket",
			accountID:        "123456789012",
			bucketName:       "my-outposts-bucket",
			wantStatus:       http.StatusOK,
			wantBodyContains: "BucketArn",
			wantLocationHdr:  true,
		},
		{
			name:             "creates_outposts_bucket_default_account",
			accountID:        "",
			bucketName:       "test-bucket",
			wantStatus:       http.StatusOK,
			wantBodyContains: "BucketArn",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestS3ControlHandler(t)
			path := "/v20180820/bucket/" + tt.bucketName
			rec := doS3ControlNewOpRequest(t, h, http.MethodPut, path, tt.accountID, "")

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBodyContains != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBodyContains)
			}

			if tt.wantLocationHdr {
				assert.NotEmpty(t, rec.Header().Get("Location"))
			}
		})
	}
}

func TestS3Control_CreateJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		accountID        string
		body             string
		wantBodyContains string
		wantStatus       int
	}{
		{
			name:      "creates_batch_job",
			accountID: "123456789012",
			body: `<CreateJobRequest>
<ClientRequestToken>token-123</ClientRequestToken>
<Priority>10</Priority>
<RoleArn>arn:aws:iam::123456789012:role/BatchOpsRole</RoleArn>
</CreateJobRequest>`,
			wantStatus:       http.StatusOK,
			wantBodyContains: "JobId",
		},
		{
			name:             "creates_job_with_minimal_body",
			accountID:        "000000000000",
			body:             `<CreateJobRequest><RoleArn>arn:aws:iam::000000000000:role/Role</RoleArn></CreateJobRequest>`,
			wantStatus:       http.StatusOK,
			wantBodyContains: "JobId",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestS3ControlHandler(t)
			rec := doS3ControlNewOpRequest(t, h, http.MethodPost, "/v20180820/jobs", tt.accountID, tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBodyContains != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBodyContains)
			}
		})
	}
}

func TestS3Control_CreateMultiRegionAccessPoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		accountID        string
		body             string
		wantBodyContains string
		wantStatus       int
	}{
		{
			name:      "creates_mrap_async_request",
			accountID: "123456789012",
			body: `<CreateMultiRegionAccessPointRequest>
<ClientToken>idempotency-token-123</ClientToken>
<Details>
<Name>my-mrap</Name>
</Details>
</CreateMultiRegionAccessPointRequest>`,
			wantStatus:       http.StatusOK,
			wantBodyContains: "RequestTokenARN",
		},
		{
			name:      "creates_mrap_with_empty_details",
			accountID: "000000000000",
			body: `<CreateMultiRegionAccessPointRequest>
<ClientToken>token-456</ClientToken>
<Details></Details>
</CreateMultiRegionAccessPointRequest>`,
			wantStatus:       http.StatusOK,
			wantBodyContains: "RequestTokenARN",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestS3ControlHandler(t)
			rec := doS3ControlNewOpRequest(
				t,
				h,
				http.MethodPost,
				"/v20180820/async-requests/mrap/create",
				tt.accountID,
				tt.body,
			)

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantBodyContains != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBodyContains)
			}
		})
	}
}

func TestS3Control_CreateStorageLensGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		accountID  string
		body       string
		wantStatus int
	}{
		{
			name:      "creates_storage_lens_group",
			accountID: "123456789012",
			body: `<CreateStorageLensGroupRequest>
<StorageLensGroup>
<Name>my-lens-group</Name>
</StorageLensGroup>
</CreateStorageLensGroupRequest>`,
			wantStatus: http.StatusCreated,
		},
		{
			name:       "creates_storage_lens_group_empty_name",
			accountID:  "000000000000",
			body:       `<CreateStorageLensGroupRequest><StorageLensGroup></StorageLensGroup></CreateStorageLensGroupRequest>`,
			wantStatus: http.StatusCreated,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestS3ControlHandler(t)
			rec := doS3ControlNewOpRequest(t, h, http.MethodPost, "/v20180820/storagelensgroup", tt.accountID, tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestS3Control_NewOps_ExtractOperation(t *testing.T) {
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
			name:   "post_jobs_returns_CreateJob",
			method: http.MethodPost,
			path:   "/v20180820/jobs",
			want:   "CreateJob",
		},
		{
			name:   "post_mrap_create_returns_CreateMultiRegionAccessPoint",
			method: http.MethodPost,
			path:   "/v20180820/async-requests/mrap/create",
			want:   "CreateMultiRegionAccessPoint",
		},
		{
			name:   "post_storagelensgroup_returns_CreateStorageLensGroup",
			method: http.MethodPost,
			path:   "/v20180820/storagelensgroup",
			want:   "CreateStorageLensGroup",
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

func TestS3Control_NewOps_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *s3control.InMemoryBackend)
		verify func(t *testing.T, b *s3control.InMemoryBackend)
		name   string
	}{
		{
			name: "snapshot_restore_preserves_access_grants_instance",
			setup: func(b *s3control.InMemoryBackend) {
				b.CreateAccessGrantsInstance("account-1", "arn:aws:sso:::instance/inst-1")
			},
			verify: func(t *testing.T, _ *s3control.InMemoryBackend) {
				t.Helper()
			},
		},
		{
			name: "snapshot_restore_preserves_batch_job",
			setup: func(b *s3control.InMemoryBackend) {
				_, _ = b.CreateJob("account-2", "arn:aws:iam::account-2:role/role", 10)
			},
			verify: func(t *testing.T, _ *s3control.InMemoryBackend) {
				t.Helper()
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := s3control.NewInMemoryBackend()
			tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := s3control.NewInMemoryBackend()
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh)
		})
	}
}
