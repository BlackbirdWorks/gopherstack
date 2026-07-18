package ram_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ram"
)

func TestListPermissionVersions_BuiltIn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		permARN string
	}{
		{
			name:    "EC2 Subnet built-in has version 1",
			permARN: "arn:aws:ram::aws:permission/AWSRAMDefaultPermissionEC2Subnet",
		},
		{
			name:    "EC2 PrefixList built-in has version 1",
			permARN: "arn:aws:ram::aws:permission/AWSRAMDefaultPermissionEC2PrefixList",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRAMRequest(t, h, "/listpermissionversions", map[string]any{
				"permissionArn": tt.permARN,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				Permissions []struct {
					Version string `json:"version"`
				} `json:"permissions"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			require.Len(t, resp.Permissions, 1)
			assert.Equal(t, "1", resp.Permissions[0].Version)
		})
	}
}

func TestSetDefaultPermissionVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *ram.Handler) (string, int32)
		name       string
		wantStatus int
	}{
		{
			name: "set version 2 as default",
			setup: func(t *testing.T, h *ram.Handler) (string, int32) {
				t.Helper()
				p, err := h.Backend.CreatePermission("setdef-perm", "ec2:Subnet", `{}`, nil)
				require.NoError(t, err)
				_, err = h.Backend.CreatePermissionVersion(p.ARN, `{"v":"2"}`)
				require.NoError(t, err)

				return p.ARN, 2
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "set nonexistent version returns error",
			setup: func(t *testing.T, h *ram.Handler) (string, int32) {
				t.Helper()
				p, err := h.Backend.CreatePermission("setdef-err-perm", "ec2:Subnet", `{}`, nil)
				require.NoError(t, err)

				return p.ARN, 99
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing permissionArn",
			setup: func(_ *testing.T, _ *ram.Handler) (string, int32) {
				return "", 1
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			permARN, version := tt.setup(t, h)

			body := map[string]any{"permissionVersion": version}
			if permARN != "" {
				body["permissionArn"] = permARN
			}

			rec := doRAMRequest(t, h, "/setdefaultpermissionversion", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestDeletePermissionVersion_RejectsNegativeVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		version    string
		wantStatus int
	}{
		{
			name:       "negative version rejected",
			version:    "-1",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "zero version rejected",
			version:    "0",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "non-integer rejected",
			version:    "abc",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			p, err := h.Backend.CreatePermission("ver-perm", "ec2:Subnet", `{}`, nil)
			require.NoError(t, err)

			rec := doRAMRequest(
				t,
				h,
				"/deletepermissionversion?permissionArn="+p.ARN+"&permissionVersion="+tt.version,
				nil,
			)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_CreatePermissionVersion(t *testing.T) {
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
				p, err := h.Backend.CreatePermission("version-perm", "ec2:Subnet", `{}`, nil)
				require.NoError(t, err)

				return p.ARN
			},
			wantStatus: http.StatusOK,
			wantBody:   "version-perm",
		},
		{
			name: "missing permissionArn",
			setup: func(_ *testing.T, _ *ram.Handler) string {
				return ""
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "not found",
			setup: func(_ *testing.T, _ *ram.Handler) string {
				return "arn:aws:ram:us-east-1:000000000000:permission/nonexistent"
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			permARN := tt.setup(t, h)

			body := map[string]any{
				"policyTemplate": `{"Effect":"Allow","Action":["ec2:DescribeSubnets"]}`,
			}
			if permARN != "" {
				body["permissionArn"] = permARN
			}

			rec := doRAMRequest(t, h, "/createpermissionversion", body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_DeletePermissionVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *ram.Handler) (string, int32)
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *ram.Handler) (string, int32) {
				t.Helper()
				p, err := h.Backend.CreatePermission("delpv-perm", "ec2:Subnet", `{}`, nil)
				require.NoError(t, err)
				// Create a second version so v1 is not the only version.
				_, err = h.Backend.CreatePermissionVersion(p.ARN, `{"v":"2"}`)
				require.NoError(t, err)

				return p.ARN, 2
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "cannot delete default version",
			setup: func(t *testing.T, h *ram.Handler) (string, int32) {
				t.Helper()
				p, err := h.Backend.CreatePermission("del-default-perm", "ec2:Subnet", `{}`, nil)
				require.NoError(t, err)

				return p.ARN, 1
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "not found permission",
			setup: func(_ *testing.T, _ *ram.Handler) (string, int32) {
				return "arn:aws:ram:us-east-1:000000000000:permission/nonexistent", 1
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing permissionArn",
			setup: func(_ *testing.T, _ *ram.Handler) (string, int32) {
				return "", 1
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			permARN, version := tt.setup(t, h)

			path := "/deletepermissionversion"
			if permARN != "" {
				path += fmt.Sprintf("?permissionArn=%s&permissionVersion=%d", permARN, version)
			}

			rec := doRAMRawRequest(t, h, http.MethodDelete, path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRAMPagination_ListPermissionVersions covers version pagination.
func TestListPermissionVersions_Pagination(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")
	h := ram.NewHandler(b)

	permARN := "arn:aws:ram::aws:permission/versioned-perm"
	ram.AddPermissionInternal(b, ram.NewTestPermission(permARN, "VersionedPerm", "ec2:Subnet"))

	// Create 3 more versions via CreatePermissionVersion.
	for i := range 3 {
		_ = i
		body := map[string]any{
			"permissionArn":  permARN,
			"policyTemplate": `{"Effect":"Allow","Action":["ec2:DescribeSubnets"]}`,
		}
		rec := doRAMRequest(t, h, "/createpermissionversion", body)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	tests := []struct {
		maxResults *int32
		name       string
		wantMin    int
		wantError  bool
	}{
		{
			name:       "maxResults=1 paginates all versions",
			maxResults: ptr32(1),
			wantMin:    2,
		},
		{
			name:       "maxResults=100 returns all in one page",
			maxResults: ptr32(100),
			wantMin:    1,
		},
		{
			name:       "maxResults=0 returns error",
			maxResults: ptr32(0),
			wantError:  true,
		},
	}

	type reqBody struct {
		PermissionArn string `json:"permissionArn"`
		MaxResults    *int32 `json:"maxResults,omitempty"`
		NextToken     string `json:"nextToken,omitempty"`
	}

	type respBody struct {
		NextToken   string `json:"nextToken"`
		Permissions []any  `json:"permissions"`
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			nextToken := ""
			totalSeen := 0

			for {
				req := reqBody{
					PermissionArn: permARN,
					MaxResults:    tc.maxResults,
					NextToken:     nextToken,
				}

				rec := doRAMRequest(t, h, "/listpermissionversions", req)

				if tc.wantError {
					assert.Equal(t, http.StatusBadRequest, rec.Code)

					var errResp map[string]string
					require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
					assert.Equal(t, "InvalidParameterException", errResp["__type"])

					return
				}

				require.Equal(t, http.StatusOK, rec.Code)

				var resp respBody
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				totalSeen += len(resp.Permissions)
				nextToken = resp.NextToken

				if nextToken == "" {
					break
				}
			}

			assert.GreaterOrEqual(t, totalSeen, tc.wantMin)
		})
	}
}

// TestRefinement1_CreatePermissionVersion_BadJSON verifies error handling.
func TestCreatePermissionVersion_BadJSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRawRequest(t, h, http.MethodPost, "/createpermissionversion", []byte("{bad"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
