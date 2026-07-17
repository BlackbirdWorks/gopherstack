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

func TestBuiltInPermissions_Present(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		resourceType string
	}{
		{name: "EC2 Subnet", resourceType: "ec2:Subnet"},
		{name: "EC2 VPC", resourceType: "ec2:VPC"},
		{name: "EC2 TransitGateway", resourceType: "ec2:TransitGateway"},
		{name: "EC2 PrefixList", resourceType: "ec2:PrefixList"},
		{name: "S3 Bucket", resourceType: "s3:Bucket"},
		{name: "Route53Resolver Rule", resourceType: "route53resolver:ResolverRule"},
		{name: "License Manager Config", resourceType: "license-manager:LicenseConfiguration"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRAMRequest(t, h, "/listpermissions", map[string]any{
				"resourceType": tt.resourceType,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				Permissions []struct {
					ResourceType   string `json:"resourceType"`
					PermissionType string `json:"permissionType"`
				} `json:"permissions"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			require.NotEmpty(t, resp.Permissions, "built-in permission for %s should exist", tt.resourceType)
			assert.Equal(t, tt.resourceType, resp.Permissions[0].ResourceType)
			assert.Equal(t, "AWS_MANAGED", resp.Permissions[0].PermissionType)
		})
	}
}

func TestBuiltInPermissions_Count(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRequest(t, h, "/listpermissions", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Permissions []json.RawMessage `json:"permissions"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp.Permissions, ram.BuiltInPermissionCount)
}

func TestListPermissions_TypeFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		permissionType string
		wantCount      int
	}{
		{
			name:           "AWS_MANAGED returns only built-ins",
			permissionType: "AWS_MANAGED",
			wantCount:      ram.BuiltInPermissionCount,
		},
		{
			name:           "CUSTOMER_MANAGED returns only custom",
			permissionType: "CUSTOMER_MANAGED",
			wantCount:      0,
		},
		{
			name:           "empty returns all",
			permissionType: "",
			wantCount:      ram.BuiltInPermissionCount,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{}
			if tt.permissionType != "" {
				body["permissionType"] = tt.permissionType
			}

			rec := doRAMRequest(t, h, "/listpermissions", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				Permissions []json.RawMessage `json:"permissions"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Len(t, resp.Permissions, tt.wantCount)
		})
	}
}

func TestListPermissions_TypeFilter_WithCustom(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create two customer-managed permissions.
	for _, name := range []string{"custom-perm-1", "custom-perm-2"} {
		rec := doRAMRequest(t, h, "/createpermission", map[string]any{
			"name":           name,
			"resourceType":   "ec2:Subnet",
			"policyTemplate": `{"Effect":"Allow"}`,
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	tests := []struct {
		name           string
		permissionType string
		wantCount      int
	}{
		{
			name:           "AWS_MANAGED filter excludes custom",
			permissionType: "AWS_MANAGED",
			wantCount:      ram.BuiltInPermissionCount,
		},
		{
			name:           "CUSTOMER_MANAGED filter returns only custom",
			permissionType: "CUSTOMER_MANAGED",
			wantCount:      2,
		},
		{
			name:           "no filter returns all",
			permissionType: "",
			wantCount:      ram.BuiltInPermissionCount + 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := map[string]any{}
			if tt.permissionType != "" {
				body["permissionType"] = tt.permissionType
			}

			rec := doRAMRequest(t, h, "/listpermissions", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				Permissions []json.RawMessage `json:"permissions"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Len(t, resp.Permissions, tt.wantCount)
		})
	}
}

func TestBuiltInPermission_PermissionType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		permName           string
		wantPermissionType string
		wantScope          string
		wantIsDefault      bool
	}{
		{
			name:               "EC2 Subnet built-in",
			permName:           "AWSRAMDefaultPermissionEC2Subnet",
			wantPermissionType: "AWS_MANAGED",
			wantIsDefault:      true,
			wantScope:          "REGIONAL",
		},
		{
			name:               "S3 Bucket built-in",
			permName:           "AWSRAMDefaultPermissionS3Bucket",
			wantPermissionType: "AWS_MANAGED",
			wantIsDefault:      true,
			wantScope:          "REGIONAL",
		},
		{
			name:               "License Manager built-in",
			permName:           "AWSRAMDefaultPermissionLicenseManagerLicenseConfiguration",
			wantPermissionType: "AWS_MANAGED",
			wantIsDefault:      true,
			wantScope:          "REGIONAL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			permARN := "arn:aws:ram::aws:permission/" + tt.permName

			rec := doRAMRequest(t, h, "/getpermission", map[string]any{
				"permissionArn": permARN,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				Permission struct {
					PermissionType        string `json:"permissionType"`
					ResourceRegionScope   string `json:"resourceRegionScope"`
					IsResourceTypeDefault bool   `json:"isResourceTypeDefault"`
				} `json:"permission"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.wantPermissionType, resp.Permission.PermissionType)
			assert.Equal(t, tt.wantIsDefault, resp.Permission.IsResourceTypeDefault)
			assert.Equal(t, tt.wantScope, resp.Permission.ResourceRegionScope)
		})
	}
}

func TestDeleteBuiltInPermission_Rejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		permARN string
	}{
		{
			name:    "EC2 Subnet built-in",
			permARN: "arn:aws:ram::aws:permission/AWSRAMDefaultPermissionEC2Subnet",
		},
		{
			name:    "S3 Bucket built-in",
			permARN: "arn:aws:ram::aws:permission/AWSRAMDefaultPermissionS3Bucket",
		},
		{
			name:    "Route53Resolver built-in",
			permARN: "arn:aws:ram::aws:permission/AWSRAMDefaultPermissionRoute53ResolverResolverRule",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			path := fmt.Sprintf("/deletepermission?permissionArn=%s", tt.permARN)
			rec := doRAMRawRequest(t, h, http.MethodDelete, path, nil)

			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), "OperationNotPermittedException")
		})
	}
}

func TestDeleteCustomPermission_Allowed(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a customer-managed permission.
	createRec := doRAMRequest(t, h, "/createpermission", map[string]any{
		"name":           "custom-delete-perm",
		"resourceType":   "ec2:Subnet",
		"policyTemplate": `{"Effect":"Allow"}`,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp struct {
		Permission struct {
			Arn string `json:"arn"`
		} `json:"permission"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

	path := fmt.Sprintf("/deletepermission?permissionArn=%s", createResp.Permission.Arn)
	rec := doRAMRawRequest(t, h, http.MethodDelete, path, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestResourceRegionScope_OnPermissions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		permARN        string
		wantScope      string
		wantAWSManaged bool
	}{
		{
			name:           "built-in EC2 subnet permission has REGIONAL scope",
			permARN:        "arn:aws:ram::aws:permission/AWSRAMDefaultPermissionEC2Subnet",
			wantScope:      "REGIONAL",
			wantAWSManaged: true,
		},
		{
			name:           "built-in S3 bucket permission has REGIONAL scope",
			permARN:        "arn:aws:ram::aws:permission/AWSRAMDefaultPermissionS3Bucket",
			wantScope:      "REGIONAL",
			wantAWSManaged: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRAMRequest(t, h, "/getpermission", map[string]any{
				"permissionArn": tt.permARN,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				Permission struct {
					ResourceRegionScope string `json:"resourceRegionScope"`
					PermissionType      string `json:"permissionType"`
				} `json:"permission"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.wantScope, resp.Permission.ResourceRegionScope)
			if tt.wantAWSManaged {
				assert.Equal(t, "AWS_MANAGED", resp.Permission.PermissionType)
			}
		})
	}
}

func TestCustomerPermission_HasRegionScope(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a customer permission.
	createRec := doRAMRequest(t, h, "/createpermission", map[string]any{
		"name":           "custom-scope-perm",
		"resourceType":   "ec2:Subnet",
		"policyTemplate": `{"Effect":"Allow","Action":["ec2:DescribeSubnets"]}`,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp struct {
		Permission struct {
			Arn                 string `json:"arn"`
			PermissionType      string `json:"permissionType"`
			ResourceRegionScope string `json:"resourceRegionScope"`
		} `json:"permission"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	assert.Equal(t, "CUSTOMER_MANAGED", createResp.Permission.PermissionType)
	assert.Equal(t, "REGIONAL", createResp.Permission.ResourceRegionScope)
}

func TestIsResourceTypeDefault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		permARN     string
		wantDefault bool
	}{
		{
			name:        "built-in is resource-type default",
			permARN:     "arn:aws:ram::aws:permission/AWSRAMDefaultPermissionEC2Subnet",
			wantDefault: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRAMRequest(t, h, "/getpermission", map[string]any{
				"permissionArn": tt.permARN,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				Permission struct {
					IsResourceTypeDefault bool `json:"isResourceTypeDefault"`
				} `json:"permission"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.wantDefault, resp.Permission.IsResourceTypeDefault)
		})
	}
}

func TestCustomerPermission_NotResourceTypeDefault(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRAMRequest(t, h, "/createpermission", map[string]any{
		"name":           "custom-not-default",
		"resourceType":   "ec2:Subnet",
		"policyTemplate": `{"Effect":"Allow"}`,
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp struct {
		Permission struct {
			Arn                   string `json:"arn"`
			IsResourceTypeDefault bool   `json:"isResourceTypeDefault"`
		} `json:"permission"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	assert.False(t, createResp.Permission.IsResourceTypeDefault)
}

func TestListPermissions_ResourceTypeFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		resourceType string
		wantMinCount int
	}{
		{
			name:         "filter by ec2:Subnet",
			resourceType: "ec2:Subnet",
			wantMinCount: 1,
		},
		{
			name:         "filter by s3:Bucket",
			resourceType: "s3:Bucket",
			wantMinCount: 1,
		},
		{
			name:         "filter by nonexistent type returns empty",
			resourceType: "nonexistent:Type",
			wantMinCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRAMRequest(t, h, "/listpermissions", map[string]any{
				"resourceType": tt.resourceType,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				Permissions []struct {
					ResourceType string `json:"resourceType"`
				} `json:"permissions"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			if tt.wantMinCount == 0 {
				assert.Empty(t, resp.Permissions)
			} else {
				assert.GreaterOrEqual(t, len(resp.Permissions), tt.wantMinCount)
				for _, p := range resp.Permissions {
					assert.Equal(t, tt.resourceType, p.ResourceType)
				}
			}
		})
	}
}

func TestGetPermission_BuiltIn_HasPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		permARN string
	}{
		{
			name:    "EC2 Subnet",
			permARN: "arn:aws:ram::aws:permission/AWSRAMDefaultPermissionEC2Subnet",
		},
		{
			name:    "S3 Bucket",
			permARN: "arn:aws:ram::aws:permission/AWSRAMDefaultPermissionS3Bucket",
		},
		{
			name:    "Route53Resolver Rule",
			permARN: "arn:aws:ram::aws:permission/AWSRAMDefaultPermissionRoute53ResolverResolverRule",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRAMRequest(t, h, "/getpermission", map[string]any{
				"permissionArn": tt.permARN,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				Permission struct {
					Permission string `json:"permission"`
				} `json:"permission"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp.Permission.Permission, "built-in permission should have non-empty policy")

			// Policy should be valid JSON.
			var policy map[string]any
			assert.NoError(t, json.Unmarshal([]byte(resp.Permission.Permission), &policy))
		})
	}
}

func TestListPermissions_SummaryHasType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a customer permission.
	doRAMRequest(t, h, "/createpermission", map[string]any{
		"name":           "type-summary-perm",
		"resourceType":   "ec2:Subnet",
		"policyTemplate": `{}`,
	})

	rec := doRAMRequest(t, h, "/listpermissions", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Permissions []struct {
			Name           string `json:"name"`
			PermissionType string `json:"permissionType"`
		} `json:"permissions"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	for _, p := range resp.Permissions {
		assert.NotEmpty(t, p.PermissionType, "permission %q should have PermissionType set", p.Name)
		assert.True(t, p.PermissionType == "AWS_MANAGED" || p.PermissionType == "CUSTOMER_MANAGED",
			"PermissionType must be AWS_MANAGED or CUSTOMER_MANAGED, got %q", p.PermissionType)
	}
}

func TestPromotePermissionCreatedFromPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *ram.Handler) string
		name       string
		wantStatus int
	}{
		{
			name: "promote CREATED_FROM_POLICY permission succeeds",
			setup: func(t *testing.T, h *ram.Handler) string {
				t.Helper()

				b := h.Backend.(*ram.InMemoryBackend)
				p := ram.NewTestPermission(
					"arn:aws:ram:us-east-1:000000000000:permission/from-policy",
					"from-policy-perm",
					"ec2:Subnet",
				)
				p.PermissionType = "CREATED_FROM_POLICY"
				ram.AddPermissionInternal(b, p)

				return p.ARN
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "promote CUSTOMER_MANAGED permission returns error",
			setup: func(t *testing.T, h *ram.Handler) string {
				t.Helper()
				p, err := h.Backend.CreatePermission("customer-perm", "ec2:Subnet", `{}`, nil)
				require.NoError(t, err)

				return p.ARN
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "promote nonexistent permission returns error",
			setup: func(_ *testing.T, _ *ram.Handler) string {
				return "arn:aws:ram:us-east-1:000000000000:permission/nonexistent"
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing permissionArn",
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
			permARN := tt.setup(t, h)

			body := map[string]any{"name": "promoted-perm"}
			if permARN != "" {
				body["permissionArn"] = permARN
			}

			rec := doRAMRequest(t, h, "/promotepermissioncreatedfrompolicy", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestDeletePermission_InUseRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rs, err := h.Backend.CreateResourceShare("inuse-share", false, nil, nil, nil)
	require.NoError(t, err)

	p, err := h.Backend.CreatePermission("inuse-perm", "ec2:Subnet", `{}`, nil)
	require.NoError(t, err)

	err = h.Backend.AssociateResourceSharePermission(rs.ARN, p.ARN, false, nil)
	require.NoError(t, err)

	// HTTP delete should return 400 PermissionInUseException.
	rec := doRAMRequest(t, h, "/deletepermission?permissionArn="+p.ARN, nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "PermissionInUseException")
}

func TestPermissionNotFound_UsesUnknownResourceException(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRequest(t, h, "/getpermission", map[string]any{
		"permissionArn": "arn:aws:ram:us-east-1:000000000000:permission/nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "UnknownResourceException", resp["__type"])
}

func TestHandler_CreatePermission(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		setup      func(*testing.T, *ram.Handler)
		name       string
		wantBody   string
		wantStatus int
	}{
		{
			name: "success",
			body: map[string]any{
				"name":           "my-perm",
				"resourceType":   "ec2:Subnet",
				"policyTemplate": `{"Effect":"Allow","Action":["ec2:*"]}`,
			},
			wantStatus: http.StatusOK,
			wantBody:   "my-perm",
		},
		{
			name: "missing name",
			body: map[string]any{
				"resourceType":   "ec2:Subnet",
				"policyTemplate": `{}`,
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing resourceType",
			body: map[string]any{
				"name":           "perm-no-type",
				"policyTemplate": `{}`,
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing policyTemplate",
			body: map[string]any{
				"name":         "perm-no-policy",
				"resourceType": "ec2:Subnet",
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "duplicate",
			setup: func(t *testing.T, h *ram.Handler) {
				t.Helper()
				_, err := h.Backend.CreatePermission("dup-perm", "ec2:Subnet", `{}`, nil)
				require.NoError(t, err)
			},
			body: map[string]any{
				"name":           "dup-perm",
				"resourceType":   "ec2:Subnet",
				"policyTemplate": `{}`,
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

			rec := doRAMRequest(t, h, "/createpermission", tt.body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestHandler_DeletePermission(t *testing.T) {
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
				p, err := h.Backend.CreatePermission("del-perm", "ec2:Subnet", `{}`, nil)
				require.NoError(t, err)

				return p.ARN
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not found",
			setup: func(_ *testing.T, _ *ram.Handler) string {
				return "arn:aws:ram:us-east-1:000000000000:permission/nonexistent"
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing permissionArn",
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
			permARN := tt.setup(t, h)

			path := "/deletepermission"
			if permARN != "" {
				path += "?permissionArn=" + permARN
			}

			rec := doRAMRawRequest(t, h, http.MethodDelete, path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_GetPermission(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*testing.T, *ram.Handler) (string, *int32)
		name       string
		wantBody   string
		wantStatus int
	}{
		{
			name: "success_default_version",
			setup: func(t *testing.T, h *ram.Handler) (string, *int32) {
				t.Helper()
				p, err := h.Backend.CreatePermission("get-perm", "ec2:Subnet", `{"allow":"all"}`, nil)
				require.NoError(t, err)

				return p.ARN, nil
			},
			wantStatus: http.StatusOK,
			wantBody:   "get-perm",
		},
		{
			name: "success_specific_version",
			setup: func(t *testing.T, h *ram.Handler) (string, *int32) {
				t.Helper()
				p, err := h.Backend.CreatePermission("get-perm-v", "ec2:Subnet", `{}`, nil)
				require.NoError(t, err)
				_, err = h.Backend.CreatePermissionVersion(p.ARN, `{"v":"2"}`)
				require.NoError(t, err)
				v := int32(2)

				return p.ARN, &v
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing permissionArn",
			setup: func(_ *testing.T, _ *ram.Handler) (string, *int32) {
				return "", nil
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "not found",
			setup: func(_ *testing.T, _ *ram.Handler) (string, *int32) {
				return "arn:aws:ram:us-east-1:000000000000:permission/nonexistent", nil
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			permARN, version := tt.setup(t, h)

			body := map[string]any{}
			if permARN != "" {
				body["permissionArn"] = permARN
			}

			if version != nil {
				body["permissionVersion"] = *version
			}

			rec := doRAMRequest(t, h, "/getpermission", body)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}
}

// TestRAMPagination_ListPermissions covers multi-page round-trips for ListPermissions.
func TestListPermissions_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		maxResults *int32
		name       string
		extra      int
		wantPages  int
		wantError  bool
	}{
		{
			name:       "default cap returns all permissions in one page",
			extra:      0,
			maxResults: nil,
			wantPages:  1,
		},
		{
			name:       "maxResults=1 paginates into BuiltInPermissionCount+extra pages",
			extra:      2,
			maxResults: ptr32(1),
			wantPages:  ram.BuiltInPermissionCount + 2, // each page has 1 item
		},
		{
			name:       "maxResults=101 returns error",
			extra:      0,
			wantError:  true,
			maxResults: ptr32(101),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ram.NewInMemoryBackend("000000000000", "us-east-1")
			h := ram.NewHandler(b)

			for i := range tc.extra {
				arn := fmt.Sprintf("arn:aws:ram::aws:permission/extra-%d", i)
				ram.AddPermissionInternal(b, ram.NewTestPermission(arn, fmt.Sprintf("Extra%d", i), "ec2:Subnet"))
			}

			type reqBody struct {
				MaxResults *int32 `json:"maxResults,omitempty"`
				NextToken  string `json:"nextToken,omitempty"`
			}

			type respBody struct {
				NextToken   string `json:"nextToken"`
				Permissions []any  `json:"permissions"`
			}

			nextToken := ""
			pages := 0
			totalSeen := 0

			for {
				req := reqBody{MaxResults: tc.maxResults, NextToken: nextToken}
				rec := doRAMRequest(t, h, "/listpermissions", req)

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

			assert.Equal(t, tc.wantPages, pages)
			assert.Equal(t, ram.BuiltInPermissionCount+tc.extra, totalSeen)
		})
	}
}

// TestRefinement1_CreatePermission_Duplicate verifies AlreadyExists is returned via HTTP.
func TestCreatePermission_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"name":           "dup-http-perm",
		"resourceType":   "ec2:Subnet",
		"policyTemplate": `{}`,
	}
	rec1 := doRAMRequest(t, h, "/createpermission", body)
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := doRAMRequest(t, h, "/createpermission", body)
	assert.Equal(t, http.StatusBadRequest, rec2.Code)
	assert.Contains(t, rec2.Body.String(), "AlreadyExists")
}

// TestRefinement1_GetPermission_NotFound verifies 400 for missing permission.
func TestGetPermission_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRequest(t, h, "/getpermission", map[string]any{
		"permissionArn": "arn:aws:ram:us-east-1:000000000000:permission/no-such-permission",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestPromotePermissionCreatedFromPolicy_Table(t *testing.T) {
	t.Parallel()

	const (
		accountID    = "000000000000"
		permARN      = "arn:aws:ram::aws:permission/from-policy-perm"
		resourceType = "ec2:Subnet"
	)

	tests := []struct {
		name     string
		permType string
		newName  string
		wantType string
		wantName string
		wantErr  bool
	}{
		{
			name:     "CREATED_FROM_POLICY promotes to CUSTOMER_MANAGED",
			permType: "CREATED_FROM_POLICY",
			newName:  "my-promoted-perm",
			wantType: "CUSTOMER_MANAGED",
			wantName: "my-promoted-perm",
		},
		{
			name:     "CUSTOMER_MANAGED permission rejected",
			permType: "CUSTOMER_MANAGED",
			newName:  "irrelevant",
			wantErr:  true,
		},
		{
			name:     "AWS_MANAGED permission rejected",
			permType: "AWS_MANAGED",
			newName:  "irrelevant",
			wantErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := ram.NewInMemoryBackend(accountID, "us-east-1")
			h := ram.NewHandler(b)

			// Inject a permission with the requested type.
			p := ram.NewTestPermission(permARN, "base-perm", resourceType)
			p.PermissionType = tc.permType
			ram.AddPermissionInternal(b, p)

			rec := doRAMRequest(t, h, "/promotepermissioncreatedfrompolicy", map[string]any{
				"permissionArn": permARN,
				"name":          tc.newName,
			})

			if tc.wantErr {
				assert.NotEqual(t, http.StatusOK, rec.Code)

				return
			}

			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			perm := resp["permission"].(map[string]any)
			assert.Equal(t, tc.wantType, perm["permissionType"])
			assert.Equal(t, tc.wantName, perm["name"])
		})
	}
}

func TestListPermissions_Smoke(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// ListPermissions
	rec := doRAMRequest(t, h, "/listpermissions", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListPermissionVersions
	rec = doRAMRequest(t, h, "/permissions/aws:aws:ram::aws:permission/AWSRAMDefaultPermissionVPC/versions", nil)
	assert.True(t, rec.Code >= 200 && rec.Code < 300 || rec.Code == 400)
}
