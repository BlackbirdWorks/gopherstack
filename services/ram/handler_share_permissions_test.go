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

func TestAssociateBuiltInPermission(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		permARN string
	}{
		{
			name:    "associate EC2 Subnet built-in",
			permARN: "arn:aws:ram::aws:permission/AWSRAMDefaultPermissionEC2Subnet",
		},
		{
			name:    "associate S3 Bucket built-in",
			permARN: "arn:aws:ram::aws:permission/AWSRAMDefaultPermissionS3Bucket",
		},
		{
			name:    "associate License Manager built-in",
			permARN: "arn:aws:ram::aws:permission/AWSRAMDefaultPermissionLicenseManagerLicenseConfiguration",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rs, err := h.Backend.CreateResourceShare("assoc-builtin-share", false, nil, nil, nil)
			require.NoError(t, err)

			rec := doRAMRequest(t, h, "/associateresourcesharepermission", map[string]any{
				"resourceShareArn": rs.ARN,
				"permissionArn":    tt.permARN,
			})
			assert.Equal(t, http.StatusOK, rec.Code)

			// Verify the permission appears in the share's permission list.
			listRec := doRAMRequest(t, h, "/listresourcesharepermissions", map[string]any{
				"resourceShareArn": rs.ARN,
			})
			require.Equal(t, http.StatusOK, listRec.Code)

			var resp struct {
				Permissions []struct {
					Arn            string `json:"arn"`
					PermissionType string `json:"permissionType"`
				} `json:"permissions"`
			}
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
			require.Len(t, resp.Permissions, 1)
			assert.Equal(t, tt.permARN, resp.Permissions[0].Arn)
			assert.Equal(t, "AWS_MANAGED", resp.Permissions[0].PermissionType)
		})
	}
}

func TestReplacePermissionAssociations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		numShares  int
		wantStatus int
	}{
		{
			name:       "replace across multiple shares",
			numShares:  3,
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing fromPermissionArn",
			numShares:  0,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.wantStatus == http.StatusBadRequest {
				rec := doRAMRequest(t, h, "/replacepermissionassociations", map[string]any{
					"toPermissionArn": "arn:aws:ram::aws:permission/AWSRAMDefaultPermissionEC2VPC",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			// Create source and target permissions.
			fromP, err := h.Backend.CreatePermission("from-perm", "ec2:Subnet", `{"v":"1"}`, nil)
			require.NoError(t, err)
			toP, err := h.Backend.CreatePermission("to-perm", "ec2:Subnet", `{"v":"2"}`, nil)
			require.NoError(t, err)

			// Create shares and associate fromP.
			for i := range tt.numShares {
				rs, shareErr := h.Backend.CreateResourceShare(fmt.Sprintf("replace-share-%d", i), false, nil, nil, nil)
				require.NoError(t, shareErr)
				err = h.Backend.AssociateResourceSharePermission(rs.ARN, fromP.ARN, false, nil)
				require.NoError(t, err)
			}

			// Replace associations.
			rec := doRAMRequest(t, h, "/replacepermissionassociations", map[string]any{
				"fromPermissionArn": fromP.ARN,
				"toPermissionArn":   toP.ARN,
			})
			require.Equal(t, tt.wantStatus, rec.Code)

			var resp struct {
				ReplacePermissionAssociationsWork struct {
					Status string `json:"status"`
					ID     string `json:"id"`
				} `json:"replacePermissionAssociationsWork"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp.ReplacePermissionAssociationsWork.ID)
			// This mock performs the association swap synchronously (no async worker),
			// so the work item is already COMPLETED by the time the call returns.
			assert.Equal(t, "COMPLETED", resp.ReplacePermissionAssociationsWork.Status)
		})
	}
}

// TestListReplacePermissionAssociationsWork verifies that work items created by
// ReplacePermissionAssociations are retrievable, and that the id/status filters work.
func TestListReplacePermissionAssociationsWork(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	fromP, err := h.Backend.CreatePermission("lrpaw-from", "ec2:Subnet", `{"v":"1"}`, nil)
	require.NoError(t, err)
	toP, err := h.Backend.CreatePermission("lrpaw-to", "ec2:Subnet", `{"v":"2"}`, nil)
	require.NoError(t, err)

	rs, err := h.Backend.CreateResourceShare("lrpaw-share", false, nil, nil, nil)
	require.NoError(t, err)
	require.NoError(t, h.Backend.AssociateResourceSharePermission(rs.ARN, fromP.ARN, false, nil))

	rec := doRAMRequest(t, h, "/replacepermissionassociations", map[string]any{
		"fromPermissionArn": fromP.ARN,
		"toPermissionArn":   toP.ARN,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp struct {
		ReplacePermissionAssociationsWork struct {
			ID string `json:"id"`
		} `json:"replacePermissionAssociationsWork"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	workID := createResp.ReplacePermissionAssociationsWork.ID
	require.NotEmpty(t, workID)

	type listResp struct {
		ReplacePermissionAssociationsWorks []struct {
			ID                    string `json:"id"`
			FromPermissionArn     string `json:"fromPermissionArn"`
			FromPermissionVersion string `json:"fromPermissionVersion"`
			ToPermissionArn       string `json:"toPermissionArn"`
			ToPermissionVersion   string `json:"toPermissionVersion"`
			Status                string `json:"status"`
		} `json:"replacePermissionAssociationsWorks"`
	}

	t.Run("unfiltered lists the work item", func(t *testing.T) {
		t.Parallel()

		listRec := doRAMRequest(t, h, "/listreplacepermissionassociationswork", map[string]any{})
		require.Equal(t, http.StatusOK, listRec.Code)

		var resp listResp
		require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
		require.Len(t, resp.ReplacePermissionAssociationsWorks, 1)

		w := resp.ReplacePermissionAssociationsWorks[0]
		assert.Equal(t, workID, w.ID)
		assert.Equal(t, fromP.ARN, w.FromPermissionArn)
		assert.Equal(t, toP.ARN, w.ToPermissionArn)
		assert.Equal(t, "1", w.FromPermissionVersion)
		assert.Equal(t, "1", w.ToPermissionVersion)
		assert.Equal(t, "COMPLETED", w.Status)
	})

	t.Run("filtered by workIds", func(t *testing.T) {
		t.Parallel()

		listRec := doRAMRequest(t, h, "/listreplacepermissionassociationswork", map[string]any{
			"workIds": []string{"nonexistent-work-id"},
		})
		require.Equal(t, http.StatusOK, listRec.Code)

		var resp listResp
		require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
		assert.Empty(t, resp.ReplacePermissionAssociationsWorks)
	})

	t.Run("filtered by status", func(t *testing.T) {
		t.Parallel()

		listRec := doRAMRequest(t, h, "/listreplacepermissionassociationswork", map[string]any{
			"status": "FAILED",
		})
		require.Equal(t, http.StatusOK, listRec.Code)

		var resp listResp
		require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))
		assert.Empty(t, resp.ReplacePermissionAssociationsWorks)
	})
}

func TestHandler_AssociateResourceSharePermission(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *ram.Handler) (string, string)
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *ram.Handler) (string, string) {
				t.Helper()
				rs, err := h.Backend.CreateResourceShare("perm-share", true, nil, nil, nil)
				require.NoError(t, err)
				p, err := h.Backend.CreatePermission("assoc-perm", "ec2:Subnet", `{}`, nil)
				require.NoError(t, err)

				return rs.ARN, p.ARN
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "resource share not found",
			setup: func(t *testing.T, h *ram.Handler) (string, string) {
				t.Helper()
				p, err := h.Backend.CreatePermission("assoc-perm2", "ec2:Subnet", `{}`, nil)
				require.NoError(t, err)

				return "arn:aws:ram:us-east-1:000000000000:resource-share/nonexistent", p.ARN
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "permission not found",
			setup: func(t *testing.T, h *ram.Handler) (string, string) {
				t.Helper()
				rs, err := h.Backend.CreateResourceShare("perm-share2", true, nil, nil, nil)
				require.NoError(t, err)

				return rs.ARN, "arn:aws:ram:us-east-1:000000000000:permission/nonexistent"
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing resourceShareArn",
			setup: func(_ *testing.T, _ *ram.Handler) (string, string) {
				return "", "arn:aws:ram:us-east-1:000000000000:permission/some-perm"
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			shareARN, permARN := tt.setup(t, h)

			body := map[string]any{
				"permissionArn": permARN,
			}
			if shareARN != "" {
				body["resourceShareArn"] = shareARN
			}

			rec := doRAMRequest(t, h, "/associateresourcesharepermission", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DisassociateResourceSharePermission(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *ram.Handler) (string, string)
		name       string
		wantStatus int
	}{
		{
			name: "success",
			setup: func(t *testing.T, h *ram.Handler) (string, string) {
				t.Helper()
				rs, err := h.Backend.CreateResourceShare("disassoc-perm-share", true, nil, nil, nil)
				require.NoError(t, err)
				p, err := h.Backend.CreatePermission("disassoc-perm", "ec2:Subnet", `{}`, nil)
				require.NoError(t, err)
				err = h.Backend.AssociateResourceSharePermission(rs.ARN, p.ARN, false, nil)
				require.NoError(t, err)

				return rs.ARN, p.ARN
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "resource share not found",
			setup: func(t *testing.T, h *ram.Handler) (string, string) {
				t.Helper()
				p, err := h.Backend.CreatePermission("disassoc-perm2", "ec2:Subnet", `{}`, nil)
				require.NoError(t, err)

				return "arn:aws:ram:us-east-1:000000000000:resource-share/nonexistent", p.ARN
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing resourceShareArn",
			setup: func(_ *testing.T, _ *ram.Handler) (string, string) {
				return "", "arn:aws:ram:us-east-1:000000000000:permission/some-perm"
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			shareARN, permARN := tt.setup(t, h)

			body := map[string]any{
				"permissionArn": permARN,
			}
			if shareARN != "" {
				body["resourceShareArn"] = shareARN
			}

			rec := doRAMRequest(t, h, "/disassociateresourcesharepermission", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestDisassociateResourceSharePermission_BlockedWhileResourceOfTypeAttached verifies
// AWS's documented rule: you can remove a managed permission from a resource share only
// if there are currently no resources of the relevant resource type currently attached
// to the resource share.
func TestDisassociateResourceSharePermission_BlockedWhileResourceOfTypeAttached(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rs, err := h.Backend.CreateResourceShare("blocked-disassoc-share", false, nil, nil, nil)
	require.NoError(t, err)

	p, err := h.Backend.CreatePermission("blocked-disassoc-perm", "ec2:Subnet", `{}`, nil)
	require.NoError(t, err)
	require.NoError(t, h.Backend.AssociateResourceSharePermission(rs.ARN, p.ARN, false, nil))

	// No resources of that type attached yet: disassociation must succeed.
	rec := doRAMRequest(t, h, "/disassociateresourcesharepermission", map[string]any{
		"resourceShareArn": rs.ARN,
		"permissionArn":    p.ARN,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Re-associate, then attach a resource of the covered type.
	require.NoError(t, h.Backend.AssociateResourceSharePermission(rs.ARN, p.ARN, false, nil))
	_, err = h.Backend.AssociateResourceShare(
		rs.ARN, nil, []string{"arn:aws:ec2:us-east-1:000000000000:subnet/sub-1"},
	)
	require.NoError(t, err)

	// Now disassociation must be refused.
	rec = doRAMRequest(t, h, "/disassociateresourcesharepermission", map[string]any{
		"resourceShareArn": rs.ARN,
		"permissionArn":    p.ARN,
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "OperationNotPermittedException", errResp["__type"])

	// The permission must still be associated -- the disassociation was refused, not
	// silently dropped.
	listRec := doRAMRequest(t, h, "/listresourcesharepermissions", map[string]any{
		"resourceShareArn": rs.ARN,
	})
	require.Equal(t, http.StatusOK, listRec.Code)
	assert.Contains(t, listRec.Body.String(), p.ARN)

	// After removing the resource, disassociation succeeds again.
	_, err = h.Backend.DisassociateResourceShare(
		rs.ARN, nil, []string{"arn:aws:ec2:us-east-1:000000000000:subnet/sub-1"},
	)
	require.NoError(t, err)

	rec = doRAMRequest(t, h, "/disassociateresourcesharepermission", map[string]any{
		"resourceShareArn": rs.ARN,
		"permissionArn":    p.ARN,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestRAMPagination_ListResourceSharePermissions covers pagination of permissions on a share.
func TestListResourceSharePermissions_Pagination(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")
	h := ram.NewHandler(b)

	shareARN := "arn:aws:ram:us-east-1:000000000000:resource-share/perm-share"
	ram.AddResourceShareInternal(b, ram.NewTestResourceShare(shareARN, "perm-share"))

	// The default permission is added at share creation; add extra custom permissions.
	for i := range 3 {
		permARN := fmt.Sprintf("arn:aws:ram::aws:permission/custom-%d", i)
		ram.AddPermissionInternal(b, ram.NewTestPermission(permARN, fmt.Sprintf("Custom%d", i), "ec2:Subnet"))

		body := map[string]any{
			"resourceShareArn": shareARN,
			"permissionArn":    permARN,
		}
		rec := doRAMRequest(t, h, "/associateresourcesharepermission", body)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	tests := []struct {
		maxResults *int32
		name       string
		minPages   int
		wantError  bool
	}{
		{
			name:       "maxResults=1 paginates",
			maxResults: ptr32(1),
			minPages:   2,
		},
		{
			name:       "no maxResults returns all",
			maxResults: nil,
			minPages:   1,
		},
		{
			name:       "maxResults=101 returns error",
			maxResults: ptr32(101),
			wantError:  true,
		},
	}

	type reqBody struct {
		ResourceShareArn string `json:"resourceShareArn"`
		MaxResults       *int32 `json:"maxResults,omitempty"`
		NextToken        string `json:"nextToken,omitempty"`
	}

	type respBody struct {
		NextToken   string `json:"nextToken"`
		Permissions []any  `json:"permissions"`
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			nextToken := ""
			pages := 0

			for {
				req := reqBody{
					ResourceShareArn: shareARN,
					MaxResults:       tc.maxResults,
					NextToken:        nextToken,
				}

				rec := doRAMRequest(t, h, "/listresourcesharepermissions", req)

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
				nextToken = resp.NextToken

				if nextToken == "" {
					break
				}
			}

			assert.GreaterOrEqual(t, pages, tc.minPages)
		})
	}
}

// TestRAMPagination_ListPermissionAssociations covers association pagination.
func TestListPermissionAssociations_Pagination(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")
	h := ram.NewHandler(b)

	permARN := "arn:aws:ram::aws:permission/assoc-perm"
	ram.AddPermissionInternal(b, ram.NewTestPermission(permARN, "AssocPerm", "ec2:Subnet"))

	// Create 4 shares that associate to this permission.
	for i := range 4 {
		shareARN := fmt.Sprintf("arn:aws:ram:us-east-1:000000000000:resource-share/assoc-share-%d", i)
		ram.AddResourceShareInternal(b, ram.NewTestResourceShare(shareARN, fmt.Sprintf("assoc-share-%d", i)))

		body := map[string]any{
			"resourceShareArn": shareARN,
			"permissionArn":    permARN,
		}
		rec := doRAMRequest(t, h, "/associateresourcesharepermission", body)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	tests := []struct {
		maxResults *int32
		name       string
		wantTotal  int
		wantPages  int
		wantError  bool
	}{
		{
			name:       "maxResults=2 paginates 4 associations",
			maxResults: ptr32(2),
			wantTotal:  4,
			wantPages:  2,
		},
		{
			name:       "maxResults=4 returns all in one page",
			maxResults: ptr32(4),
			wantTotal:  4,
			wantPages:  1,
		},
		{
			name:       "maxResults=101 returns error",
			maxResults: ptr32(101),
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
			pages := 0

			for {
				req := reqBody{
					PermissionArn: permARN,
					MaxResults:    tc.maxResults,
					NextToken:     nextToken,
				}

				rec := doRAMRequest(t, h, "/listpermissionassociations", req)

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
				totalSeen += len(resp.Permissions)
				nextToken = resp.NextToken

				if nextToken == "" {
					break
				}
			}

			assert.Equal(t, tc.wantTotal, totalSeen)
			assert.Equal(t, tc.wantPages, pages)
		})
	}
}

// TestRefinement1_ListResourceSharePermissions_RealData verifies that actual
// associated permissions are returned instead of an empty list.
func TestListResourceSharePermissions_RealData(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a share and a permission.
	shareRec := doRAMRequest(t, h, "/createresourceshare", map[string]any{"name": "perm-share"})
	require.Equal(t, http.StatusOK, shareRec.Code)

	var shareResp struct {
		ResourceShare struct {
			ResourceShareArn string `json:"resourceShareArn"`
		} `json:"resourceShare"`
	}
	require.NoError(t, json.Unmarshal(shareRec.Body.Bytes(), &shareResp))
	shareARN := shareResp.ResourceShare.ResourceShareArn

	permRec := doRAMRequest(t, h, "/createpermission", map[string]any{
		"name":           "perm-for-share",
		"resourceType":   "ec2:Subnet",
		"policyTemplate": `{}`,
	})
	require.Equal(t, http.StatusOK, permRec.Code)

	var permResp struct {
		Permission struct {
			Arn string `json:"arn"`
		} `json:"permission"`
	}
	require.NoError(t, json.Unmarshal(permRec.Body.Bytes(), &permResp))
	permARN := permResp.Permission.Arn

	// Associate permission with share.
	assocRec := doRAMRequest(t, h, "/associateresourcesharepermission", map[string]any{
		"resourceShareArn": shareARN,
		"permissionArn":    permARN,
	})
	require.Equal(t, http.StatusOK, assocRec.Code)

	// List permissions for the share.
	listRec := doRAMRequest(t, h, "/listresourcesharepermissions", map[string]any{
		"resourceShareArn": shareARN,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp struct {
		Permissions []struct {
			Arn string `json:"arn"`
		} `json:"permissions"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	require.Len(t, listResp.Permissions, 1)
	assert.Equal(t, permARN, listResp.Permissions[0].Arn)
}

// TestRefinement1_ListResourceSharePermissions_RequiresShareARN verifies validation.
func TestListResourceSharePermissions_RequiresShareARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRequest(t, h, "/listresourcesharepermissions", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestRefinement1_ListResourceSharePermissions_ReportsAssociatedVersion verifies that
// the version and defaultVersion fields reflect the version actually associated with
// the resource share, not the permission's current default version. AWS lets a share
// pin a non-default permission version (via AssociateResourceSharePermission's
// permissionVersion parameter); ListResourceSharePermissions must surface that pinned
// version rather than always reporting the permission's latest default.
func TestListResourceSharePermissions_ReportsAssociatedVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	shareRec := doRAMRequest(t, h, "/createresourceshare", map[string]any{"name": "pinned-version-share"})
	require.Equal(t, http.StatusOK, shareRec.Code)

	var shareResp struct {
		ResourceShare struct {
			ResourceShareArn string `json:"resourceShareArn"`
		} `json:"resourceShare"`
	}
	require.NoError(t, json.Unmarshal(shareRec.Body.Bytes(), &shareResp))
	shareARN := shareResp.ResourceShare.ResourceShareArn

	permRec := doRAMRequest(t, h, "/createpermission", map[string]any{
		"name":           "pinned-perm",
		"resourceType":   "ec2:Subnet",
		"policyTemplate": `{}`,
	})
	require.Equal(t, http.StatusOK, permRec.Code)

	var permResp struct {
		Permission struct {
			Arn string `json:"arn"`
		} `json:"permission"`
	}
	require.NoError(t, json.Unmarshal(permRec.Body.Bytes(), &permResp))
	permARN := permResp.Permission.Arn

	// Create a second version and make it the default, then pin the share to
	// version 1 so the share's associated version diverges from the default.
	verRec := doRAMRequest(t, h, "/createpermissionversion", map[string]any{
		"permissionArn":  permARN,
		"policyTemplate": `{"v":2}`,
	})
	require.Equal(t, http.StatusOK, verRec.Code)

	setDefaultRec := doRAMRequest(t, h, "/setdefaultpermissionversion", map[string]any{
		"permissionArn":     permARN,
		"permissionVersion": int32(2),
	})
	require.Equal(t, http.StatusOK, setDefaultRec.Code)

	pinnedVersion := int32(1)
	assocRec := doRAMRequest(t, h, "/associateresourcesharepermission", map[string]any{
		"resourceShareArn":  shareARN,
		"permissionArn":     permARN,
		"permissionVersion": pinnedVersion,
	})
	require.Equal(t, http.StatusOK, assocRec.Code)

	listRec := doRAMRequest(t, h, "/listresourcesharepermissions", map[string]any{
		"resourceShareArn": shareARN,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp struct {
		Permissions []struct {
			Arn            string `json:"arn"`
			Version        string `json:"version"`
			DefaultVersion bool   `json:"defaultVersion"`
		} `json:"permissions"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	require.Len(t, listResp.Permissions, 1)
	assert.Equal(
		t, "1", listResp.Permissions[0].Version,
		"must report the version pinned to the share, not the permission's default",
	)
	assert.False(
		t,
		listResp.Permissions[0].DefaultVersion,
		"pinned version 1 is no longer the permission's default (version 2 is)",
	)
}

// TestRefinement1_AssociateResourceSharePermission_BadJSON verifies error handling.
func TestAssociateResourceSharePermission_BadJSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRawRequest(t, h, http.MethodPost, "/associateresourcesharepermission", []byte("{bad json"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
