package ram_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ram"
)

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

// TestHandler_GetResourceShares_ByARN_AppliesNameAndStatusFilters verifies that
// the resourceShareArns lookup path still honors the name and resourceShareStatus
// filters combined in the same request, matching AWS's GetResourceShares behavior
// of combining filters rather than treating resourceShareArns as an exclusive mode.
func TestHandler_GetResourceShares_ByARN_AppliesNameAndStatusFilters(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rs, err := h.Backend.CreateResourceShare("filtered-arn-share", true, nil, nil, nil)
	require.NoError(t, err)

	// Name filter that doesn't match must exclude the share even when its ARN
	// is explicitly requested.
	rec := doRAMRequest(t, h, "/getresourceshares", map[string]any{
		"resourceOwner":     "SELF",
		"resourceShareArns": []string{rs.ARN},
		"name":              "does-not-match",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "filtered-arn-share")

	// Status filter that doesn't match (share is ACTIVE) must also exclude it.
	rec = doRAMRequest(t, h, "/getresourceshares", map[string]any{
		"resourceOwner":       "SELF",
		"resourceShareArns":   []string{rs.ARN},
		"resourceShareStatus": "DELETED",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "filtered-arn-share")

	// Matching status filter must include it.
	rec = doRAMRequest(t, h, "/getresourceshares", map[string]any{
		"resourceOwner":       "SELF",
		"resourceShareArns":   []string{rs.ARN},
		"resourceShareStatus": "ACTIVE",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "filtered-arn-share")
}

func TestHandler_UpdateResourceShare_MissingARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRequest(t, h, "/updateresourceshare", map[string]any{
		"name": "updated",
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

func TestHandler_EnableSharingWithAwsOrganization(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRawRequest(t, h, http.MethodPost, "/enablesharingwithawsorganization", nil)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "returnValue")
}

func TestCreateResourceShare_PermissionArns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		permissionArns []string
		wantStatus     int
		wantPermCount  int
	}{
		{
			name:           "single built-in permission associated",
			permissionArns: []string{"arn:aws:ram::aws:permission/AWSRAMDefaultPermissionEC2Subnet"},
			wantStatus:     http.StatusOK,
			wantPermCount:  1,
		},
		{
			name:           "no permissionArns - share created without explicit permissions",
			permissionArns: nil,
			wantStatus:     http.StatusOK,
			wantPermCount:  0,
		},
		{
			name: "multiple permissions associated",
			permissionArns: []string{
				"arn:aws:ram::aws:permission/AWSRAMDefaultPermissionEC2Subnet",
				"arn:aws:ram::aws:permission/AWSRAMDefaultPermissionEC2VPC",
			},
			wantStatus:    http.StatusOK,
			wantPermCount: 2,
		},
		{
			name:           "invalid permission ARN returns error",
			permissionArns: []string{"arn:aws:ram::aws:permission/NonExistentPermission"},
			wantStatus:     http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{
				"name": "perm-share-" + strings.ReplaceAll(tt.name, " ", "-"),
			}
			if len(tt.permissionArns) > 0 {
				body["permissionArns"] = tt.permissionArns
			}

			rec := doRAMRequest(t, h, "/createresourceshare", body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus != http.StatusOK {
				return
			}

			var createResp struct {
				ResourceShare struct {
					ResourceShareArn string `json:"resourceShareArn"`
				} `json:"resourceShare"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			shareARN := createResp.ResourceShare.ResourceShareArn

			// Verify permissions were associated.
			listRec := doRAMRequest(t, h, "/listresourcesharepermissions", map[string]any{
				"resourceShareArn": shareARN,
			})
			require.Equal(t, http.StatusOK, listRec.Code)

			var listResp struct {
				Permissions []json.RawMessage `json:"permissions"`
			}
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
			assert.Len(t, listResp.Permissions, tt.wantPermCount)
		})
	}
}

func TestGetResourceShares_OtherAccounts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		setup         func(*testing.T, *ram.Handler)
		resourceOwner string
		wantCount     int
	}{
		{
			name: "SELF returns owned shares",
			setup: func(t *testing.T, h *ram.Handler) {
				t.Helper()
				_, err := h.Backend.CreateResourceShare("self-share", false, nil, nil, nil)
				require.NoError(t, err)
			},
			resourceOwner: "SELF",
			wantCount:     1,
		},
		{
			name: "OTHER-ACCOUNTS returns empty for single-account mock",
			setup: func(t *testing.T, h *ram.Handler) {
				t.Helper()
				_, err := h.Backend.CreateResourceShare("self-share-2", false, nil, nil, nil)
				require.NoError(t, err)
			},
			resourceOwner: "OTHER-ACCOUNTS",
			wantCount:     0,
		},
		{
			name:          "OTHER-ACCOUNTS empty when no shares exist",
			setup:         func(_ *testing.T, _ *ram.Handler) {},
			resourceOwner: "OTHER-ACCOUNTS",
			wantCount:     0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setup(t, h)

			rec := doRAMRequest(t, h, "/getresourceshares", map[string]any{
				"resourceOwner": tt.resourceOwner,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				ResourceShares []json.RawMessage `json:"resourceShares"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Len(t, resp.ResourceShares, tt.wantCount)
		})
	}
}

func TestAllowExternalPrincipals_Default(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		allowExternal bool
		wantExternal  bool
	}{
		{
			name:          "allow external principals true",
			allowExternal: true,
			wantExternal:  true,
		},
		{
			name:          "allow external principals false",
			allowExternal: false,
			wantExternal:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRAMRequest(t, h, "/createresourceshare", map[string]any{
				"name":                    "ext-share",
				"allowExternalPrincipals": tt.allowExternal,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				ResourceShare struct {
					AllowExternalPrincipals bool `json:"allowExternalPrincipals"`
				} `json:"resourceShare"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.wantExternal, resp.ResourceShare.AllowExternalPrincipals)
		})
	}
}

func TestCreateResourceShare_WithPrincipalsAndResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		principals     []string
		resourceArns   []string
		allowExternal  bool
		wantPrincipals int
		wantResources  int
	}{
		{
			name:           "only principals",
			principals:     []string{"123456789012", "arn:aws:iam::999999999999:root"},
			resourceArns:   nil,
			allowExternal:  true,
			wantPrincipals: 2,
			wantResources:  0,
		},
		{
			name:           "only resources",
			principals:     nil,
			resourceArns:   []string{"arn:aws:ec2:us-east-1:123456789012:subnet/subnet-abc"},
			allowExternal:  false,
			wantPrincipals: 0,
			wantResources:  1,
		},
		{
			name:           "both principals and resources",
			principals:     []string{"111111111111"},
			resourceArns:   []string{"arn:aws:ec2:us-east-1:123456789012:subnet/subnet-abc"},
			allowExternal:  true,
			wantPrincipals: 1,
			wantResources:  1,
		},
		{
			name:           "no associations",
			principals:     nil,
			resourceArns:   nil,
			allowExternal:  false,
			wantPrincipals: 0,
			wantResources:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			shareName := strings.ReplaceAll("assoc-share-"+tt.name, " ", "-")
			body := map[string]any{
				"name":                    shareName,
				"allowExternalPrincipals": tt.allowExternal,
			}
			if len(tt.principals) > 0 {
				body["principals"] = tt.principals
			}

			if len(tt.resourceArns) > 0 {
				body["resourceArns"] = tt.resourceArns
			}

			rec := doRAMRequest(t, h, "/createresourceshare", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var shareResp struct {
				ResourceShare struct {
					ResourceShareArn string `json:"resourceShareArn"`
				} `json:"resourceShare"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &shareResp))
			shareARN := shareResp.ResourceShare.ResourceShareArn

			// Check principals.
			principalRec := doRAMRequest(t, h, "/listprincipals", map[string]any{
				"resourceOwner":    "SELF",
				"resourceShareArn": shareARN,
			})
			require.Equal(t, http.StatusOK, principalRec.Code)

			var principalResp struct {
				Principals []json.RawMessage `json:"principals"`
			}
			require.NoError(t, json.Unmarshal(principalRec.Body.Bytes(), &principalResp))
			assert.Len(t, principalResp.Principals, tt.wantPrincipals)

			// Check resources.
			resourceRec := doRAMRequest(t, h, "/listresources", map[string]any{
				"resourceOwner":    "SELF",
				"resourceShareArn": shareARN,
			})
			require.Equal(t, http.StatusOK, resourceRec.Code)

			var resourceResp struct {
				Resources []json.RawMessage `json:"resources"`
			}
			require.NoError(t, json.Unmarshal(resourceRec.Body.Bytes(), &resourceResp))
			assert.Len(t, resourceResp.Resources, tt.wantResources)
		})
	}
}

func TestGetResourceShares_NameFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"alpha-share", "beta-share", "gamma-share"} {
		_, err := h.Backend.CreateResourceShare(name, false, nil, nil, nil)
		require.NoError(t, err)
	}

	tests := []struct {
		name      string
		filter    string
		wantCount int
	}{
		{
			name:      "filter by exact name",
			filter:    "alpha-share",
			wantCount: 1,
		},
		{
			name:      "filter returns zero for non-matching name",
			filter:    "nonexistent-share",
			wantCount: 0,
		},
		{
			name:      "no filter returns all",
			filter:    "",
			wantCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := map[string]any{"resourceOwner": "SELF"}
			if tt.filter != "" {
				body["name"] = tt.filter
			}

			rec := doRAMRequest(t, h, "/getresourceshares", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				ResourceShares []json.RawMessage `json:"resourceShares"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Len(t, resp.ResourceShares, tt.wantCount)
		})
	}
}

func TestResourceShare_TagsInResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		tags     []map[string]string
		wantTags int
	}{
		{
			name: "with tags",
			tags: []map[string]string{
				{"key": "Environment", "value": "prod"},
				{"key": "Owner", "value": "team-alpha"},
			},
			wantTags: 2,
		},
		{
			name:     "without tags",
			tags:     nil,
			wantTags: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{"name": "tag-accuracy-share"}
			if len(tt.tags) > 0 {
				body["tags"] = tt.tags
			}

			rec := doRAMRequest(t, h, "/createresourceshare", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				ResourceShare struct {
					Tags []struct {
						Key   string `json:"key"`
						Value string `json:"value"`
					} `json:"tags"`
				} `json:"resourceShare"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Len(t, resp.ResourceShare.Tags, tt.wantTags)
		})
	}
}

func TestResourceShare_FeatureSetStandard(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRequest(t, h, "/createresourceshare", map[string]any{
		"name": "feature-set-share",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		ResourceShare struct {
			FeatureSet string `json:"featureSet"`
		} `json:"resourceShare"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "STANDARD", resp.ResourceShare.FeatureSet)
}

func TestCreateResourceShare_NameValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		shareName  string
		wantStatus int
	}{
		{
			name:       "valid name alphanumeric",
			shareName:  "my-share-01",
			wantStatus: http.StatusOK,
		},
		{
			name:       "valid name with dots",
			shareName:  "my.share.name",
			wantStatus: http.StatusOK,
		},
		{
			name:       "valid name with underscores",
			shareName:  "my_share_name",
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid name with spaces",
			shareName:  "my share name",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid name with at-sign",
			shareName:  "my@share",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid empty name",
			shareName:  "",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{"name": tt.shareName}
			rec := doRAMRequest(t, h, "/createresourceshare", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// makeNShares seeds n resource shares and returns their ARNs sorted.
func makeNShares(t *testing.T, b *ram.InMemoryBackend, n int) []string {
	t.Helper()

	arns := make([]string, n)

	for i := range n {
		arn := fmt.Sprintf("arn:aws:ram:us-east-1:000000000000:resource-share/%04d", i)
		arns[i] = arn
		ram.AddResourceShareInternal(b, ram.NewTestResourceShare(arn, fmt.Sprintf("share-%04d", i)))
	}

	return arns
}

// TestRAMPagination_GetResourceShares covers multi-page round-trips and bounds validation.
func TestGetResourceShares_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		maxResults  *int32
		name        string
		totalShares int
		wantPages   int
		wantError   bool
	}{
		{
			name:        "no maxResults returns all up to cap",
			totalShares: 5,
			maxResults:  nil,
			wantPages:   1,
		},
		{
			name:        "maxResults=2 paginates 5 shares into 3 pages",
			totalShares: 5,
			maxResults:  ptr32(2),
			wantPages:   3,
		},
		{
			name:        "maxResults=1 paginates 3 shares into 3 pages",
			totalShares: 3,
			maxResults:  ptr32(1),
			wantPages:   3,
		},
		{
			name:        "maxResults=100 (cap) returns all in one page",
			totalShares: 50,
			maxResults:  ptr32(100),
			wantPages:   1,
		},
		{
			name:        "maxResults=101 returns InvalidParameterException",
			totalShares: 1,
			maxResults:  ptr32(101),
			wantError:   true,
		},
		{
			name:        "maxResults=0 returns InvalidParameterException",
			totalShares: 1,
			maxResults:  ptr32(0),
			wantError:   true,
		},
		{
			name:        "empty result set",
			totalShares: 0,
			maxResults:  ptr32(10),
			wantPages:   1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ram.NewInMemoryBackend("000000000000", "us-east-1")
			h := ram.NewHandler(b)

			makeNShares(t, b, tc.totalShares)

			type reqBody struct {
				ResourceOwner string `json:"resourceOwner"`
				MaxResults    *int32 `json:"maxResults,omitempty"`
				NextToken     string `json:"nextToken,omitempty"`
			}

			type respBody struct {
				NextToken      string `json:"nextToken"`
				ResourceShares []any  `json:"resourceShares"`
			}

			nextToken := ""
			totalSeen := 0
			pages := 0

			for {
				req := reqBody{
					ResourceOwner: "SELF",
					MaxResults:    tc.maxResults,
					NextToken:     nextToken,
				}

				rec := doRAMRequest(t, h, "/getresourceshares", req)

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

				pages++
				totalSeen += len(resp.ResourceShares)
				nextToken = resp.NextToken

				if nextToken == "" {
					break
				}
			}

			assert.Equal(t, tc.totalShares, totalSeen, "total items across all pages")
			assert.Equal(t, tc.wantPages, pages, "number of pages")
		})
	}
}

// TestRefinement1_GetResourceShares_StatusFilter verifies status filter via HTTP.
func TestGetResourceShares_StatusFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create then delete a share.
	createRec := doRAMRequest(t, h, "/createresourceshare", map[string]any{"name": "to-delete"})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp struct {
		ResourceShare struct {
			ResourceShareArn string `json:"resourceShareArn"`
		} `json:"resourceShare"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

	delRec := doRAMRawRequest(t, h, http.MethodDelete,
		"/deleteresourceshare?resourceShareArn="+createResp.ResourceShare.ResourceShareArn, nil)
	require.Equal(t, http.StatusOK, delRec.Code)

	// Create a live share.
	_, err := h.Backend.CreateResourceShare("live-share", false, nil, nil, nil)
	require.NoError(t, err)

	// Filter by ACTIVE status.
	rec := doRAMRequest(t, h, "/getresourceshares", map[string]any{
		"resourceOwner":       "SELF",
		"resourceShareStatus": "ACTIVE",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		ResourceShares []struct {
			Name string `json:"name"`
		} `json:"resourceShares"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp.ResourceShares, 1)
	assert.Equal(t, "live-share", resp.ResourceShares[0].Name)
}

// TestRefinement1_CreateResourceShare_NameRequired verifies that name validation works.
func TestCreateResourceShare_NameRequired(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRequest(t, h, "/createresourceshare", map[string]any{
		"allowExternalPrincipals": true,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestDeleteResourceShare_RemovesFromMap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createName string
	}{
		{name: "single share deleted from map", createName: "my-share"},
		{name: "share with dashes deleted cleanly", createName: "share-with-dashes"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create a share.
			createResp := doRAMRequest(t, h, "/createresourceshare", map[string]any{
				"name": tc.createName,
			})
			require.Equal(t, http.StatusOK, createResp.Code)

			var createBody map[string]any
			require.NoError(t, json.Unmarshal(createResp.Body.Bytes(), &createBody))
			shareARN := createBody["resourceShare"].(map[string]any)["resourceShareArn"].(string)
			require.NotEmpty(t, shareARN)

			// Delete via query param.
			rec := doRAMRawRequest(
				t, h, http.MethodDelete,
				"/deleteresourceshare?resourceShareArn="+shareARN,
				nil,
			)
			require.Equal(t, http.StatusOK, rec.Code)

			// Re-listing SELF shares must not return the deleted share.
			listResp := doRAMRequest(t, h, "/getresourceshares", map[string]any{
				"resourceOwner": "SELF",
			})
			require.Equal(t, http.StatusOK, listResp.Code)

			var listBody map[string]any
			require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listBody))
			shares := listBody["resourceShares"].([]any)

			for _, s := range shares {
				arn := s.(map[string]any)["resourceShareArn"].(string)
				assert.NotEqual(t, shareARN, arn, "deleted share must not appear in list")
			}
		})
	}
}

func TestPaginationTokenIsOpaque(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")
	h := ram.NewHandler(b)

	// Seed 5 shares.
	for i := range 5 {
		arn := fmt.Sprintf("arn:aws:ram:us-east-1:000000000000:resource-share/share-%02d", i)
		rs := ram.NewTestResourceShare(arn, fmt.Sprintf("share-%02d", i))
		ram.AddResourceShareInternal(b, rs)
	}

	maxResults := int32(2)
	rec := doRAMRequest(t, h, "/getresourceshares", map[string]any{
		"resourceOwner": "SELF",
		"maxResults":    maxResults,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	nextToken, _ := resp["nextToken"].(string)
	require.NotEmpty(t, nextToken, "expected a nextToken for multi-page result")

	// The token must not be a plain integer.
	var plainInt int
	err := json.Unmarshal([]byte(nextToken), &plainInt)
	require.Error(t, err, "nextToken should not be a plain integer")

	// Following the token must yield the next page without overlap.
	rec2 := doRAMRequest(t, h, "/getresourceshares", map[string]any{
		"resourceOwner": "SELF",
		"maxResults":    maxResults,
		"nextToken":     nextToken,
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))

	page1Shares := resp["resourceShares"].([]any)
	page2Shares := resp2["resourceShares"].([]any)

	// Collect page1 ARNs.
	p1ARNs := make(map[string]struct{}, len(page1Shares))
	for _, s := range page1Shares {
		arn := s.(map[string]any)["resourceShareArn"].(string)
		p1ARNs[arn] = struct{}{}
	}

	// No page2 ARN should appear in page1.
	for _, s := range page2Shares {
		arn := s.(map[string]any)["resourceShareArn"].(string)
		assert.NotContains(t, p1ARNs, arn, "page 2 must not overlap page 1")
	}
}
