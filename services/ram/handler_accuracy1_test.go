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

// --- Built-in AWS-managed permissions ---

func TestRAM_Accuracy_BuiltInPermissions_Present(t *testing.T) {
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

func TestRAM_Accuracy_BuiltInPermissions_Count(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRequest(t, h, "/listpermissions", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Permissions []json.RawMessage `json:"permissions"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, ram.BuiltInPermissionCount, len(resp.Permissions))
}

func TestRAM_Accuracy_ListPermissions_TypeFilter(t *testing.T) {
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
			assert.Equal(t, tt.wantCount, len(resp.Permissions))
		})
	}
}

func TestRAM_Accuracy_ListPermissions_TypeFilter_WithCustom(t *testing.T) {
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
			assert.Equal(t, tt.wantCount, len(resp.Permissions))
		})
	}
}

// --- Permission type on built-ins ---

func TestRAM_Accuracy_BuiltInPermission_PermissionType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		permName           string
		wantPermissionType string
		wantIsDefault      bool
		wantScope          string
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
					IsResourceTypeDefault bool   `json:"isResourceTypeDefault"`
					ResourceRegionScope   string `json:"resourceRegionScope"`
				} `json:"permission"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tt.wantPermissionType, resp.Permission.PermissionType)
			assert.Equal(t, tt.wantIsDefault, resp.Permission.IsResourceTypeDefault)
			assert.Equal(t, tt.wantScope, resp.Permission.ResourceRegionScope)
		})
	}
}

// --- Delete AWS-managed permissions rejected ---

func TestRAM_Accuracy_DeleteBuiltInPermission_Rejected(t *testing.T) {
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

// --- Customer-managed permissions still deletable ---

func TestRAM_Accuracy_DeleteCustomPermission_Allowed(t *testing.T) {
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

// --- CreateResourceShare with permissionArns ---

func TestRAM_Accuracy_CreateResourceShare_PermissionArns(t *testing.T) {
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

// --- ResourceRegionScope on resource objects ---

func TestRAM_Accuracy_ResourceRegionScope_InListResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		resourceARN string
		wantType    string
		wantScope   string
	}{
		{
			name:        "EC2 subnet resource",
			resourceARN: "arn:aws:ec2:us-east-1:123456789012:subnet/subnet-abc123",
			wantType:    "ec2:Subnet",
			wantScope:   "REGIONAL",
		},
		{
			name:        "EC2 VPC resource",
			resourceARN: "arn:aws:ec2:us-east-1:123456789012:vpc/vpc-12345678",
			wantType:    "ec2:VPC",
			wantScope:   "REGIONAL",
		},
		{
			name:        "EC2 transit gateway",
			resourceARN: "arn:aws:ec2:us-east-1:123456789012:transit-gateway/tgw-1234abcd",
			wantType:    "ec2:TransitGateway",
			wantScope:   "REGIONAL",
		},
		{
			name:        "EC2 prefix list",
			resourceARN: "arn:aws:ec2:us-east-1:123456789012:prefix-list/pl-abc123",
			wantType:    "ec2:PrefixList",
			wantScope:   "REGIONAL",
		},
		{
			name:        "Route53Resolver rule",
			resourceARN: "arn:aws:route53resolver:us-east-1:123456789012:resolver-rule/rslvr-rr-abc",
			wantType:    "route53resolver:ResolverRule",
			wantScope:   "REGIONAL",
		},
		{
			name:        "License Manager config",
			resourceARN: "arn:aws:license-manager:us-east-1:123456789012:license-configuration:lic-abc123",
			wantType:    "license-manager:LicenseConfiguration",
			wantScope:   "REGIONAL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create share with a resource.
			rs, err := h.Backend.CreateResourceShare("scope-share", false, nil, nil, []string{tt.resourceARN})
			require.NoError(t, err)

			rec := doRAMRequest(t, h, "/listresources", map[string]any{
				"resourceOwner":    "SELF",
				"resourceShareArn": rs.ARN,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				Resources []struct {
					Arn                 string `json:"arn"`
					Type                string `json:"type"`
					ResourceRegionScope string `json:"resourceRegionScope"`
				} `json:"resources"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			require.Len(t, resp.Resources, 1)
			assert.Equal(t, tt.resourceARN, resp.Resources[0].Arn)
			assert.Equal(t, tt.wantType, resp.Resources[0].Type)
			assert.Equal(t, tt.wantScope, resp.Resources[0].ResourceRegionScope)
		})
	}
}

// --- ResourceRegionScope on permissions ---

func TestRAM_Accuracy_ResourceRegionScope_OnPermissions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		permARN       string
		wantScope     string
		wantAWSManaged bool
	}{
		{
			name:          "built-in EC2 subnet permission has REGIONAL scope",
			permARN:       "arn:aws:ram::aws:permission/AWSRAMDefaultPermissionEC2Subnet",
			wantScope:     "REGIONAL",
			wantAWSManaged: true,
		},
		{
			name:          "built-in S3 bucket permission has REGIONAL scope",
			permARN:       "arn:aws:ram::aws:permission/AWSRAMDefaultPermissionS3Bucket",
			wantScope:     "REGIONAL",
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

func TestRAM_Accuracy_CustomerPermission_HasRegionScope(t *testing.T) {
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

// --- GetResourceShares with OTHER-ACCOUNTS ---

func TestRAM_Accuracy_GetResourceShares_OtherAccounts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		setup        func(*testing.T, *ram.Handler)
		resourceOwner string
		wantCount    int
	}{
		{
			name: "SELF returns owned shares",
			setup: func(t *testing.T, h *ram.Handler) {
				t.Helper()
				_, err := h.Backend.CreateResourceShare("self-share", false, nil, nil, nil)
				require.NoError(t, err)
			},
			resourceOwner: "SELF",
			wantCount:    1,
		},
		{
			name: "OTHER-ACCOUNTS returns empty for single-account mock",
			setup: func(t *testing.T, h *ram.Handler) {
				t.Helper()
				_, err := h.Backend.CreateResourceShare("self-share-2", false, nil, nil, nil)
				require.NoError(t, err)
			},
			resourceOwner: "OTHER-ACCOUNTS",
			wantCount:    0,
		},
		{
			name:          "OTHER-ACCOUNTS empty when no shares exist",
			setup:         func(_ *testing.T, _ *ram.Handler) {},
			resourceOwner: "OTHER-ACCOUNTS",
			wantCount:    0,
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

// --- IsResourceTypeDefault on built-ins vs custom ---

func TestRAM_Accuracy_IsResourceTypeDefault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		permARN   string
		wantDefault bool
	}{
		{
			name:      "built-in is resource-type default",
			permARN:   "arn:aws:ram::aws:permission/AWSRAMDefaultPermissionEC2Subnet",
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

func TestRAM_Accuracy_CustomerPermission_NotResourceTypeDefault(t *testing.T) {
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

// --- Reset re-seeds built-in permissions ---

func TestRAM_Accuracy_Reset_ReSeedsBuiltIns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "reset via backend"},
		{name: "reset via handler"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Add a customer permission.
			doRAMRequest(t, h, "/createpermission", map[string]any{
				"name":           "to-be-cleared",
				"resourceType":   "ec2:Subnet",
				"policyTemplate": `{}`,
			})

			// Reset clears user state.
			h.Reset()

			// Built-ins should be back; customer permission gone.
			rec := doRAMRequest(t, h, "/listpermissions", map[string]any{
				"permissionType": "CUSTOMER_MANAGED",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var custResp struct {
				Permissions []json.RawMessage `json:"permissions"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &custResp))
			assert.Empty(t, custResp.Permissions, "customer permissions cleared after reset")

			rec2 := doRAMRequest(t, h, "/listpermissions", map[string]any{
				"permissionType": "AWS_MANAGED",
			})
			require.Equal(t, http.StatusOK, rec2.Code)

			var awsResp struct {
				Permissions []json.RawMessage `json:"permissions"`
			}
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &awsResp))
			assert.Equal(t, ram.BuiltInPermissionCount, len(awsResp.Permissions),
				"built-in permissions re-seeded after reset")
		})
	}
}

// --- ListPermissions filtered by resourceType ---

func TestRAM_Accuracy_ListPermissions_ResourceTypeFilter(t *testing.T) {
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

// --- Built-in permissions can be associated with shares ---

func TestRAM_Accuracy_AssociateBuiltInPermission(t *testing.T) {
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

// --- Resource type derivation accuracy ---

func TestRAM_Accuracy_ResourceTypeDerivation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		resourceARN string
		wantType    string
	}{
		{
			name:        "EC2 subnet via slash separator",
			resourceARN: "arn:aws:ec2:us-east-1:123456789012:subnet/subnet-abc123",
			wantType:    "ec2:Subnet",
		},
		{
			name:        "EC2 VPC",
			resourceARN: "arn:aws:ec2:us-east-1:123456789012:vpc/vpc-12345678",
			wantType:    "ec2:VPC",
		},
		{
			name:        "transit gateway",
			resourceARN: "arn:aws:ec2:us-east-1:123456789012:transit-gateway/tgw-1234abcd",
			wantType:    "ec2:TransitGateway",
		},
		{
			name:        "prefix list",
			resourceARN: "arn:aws:ec2:us-east-1:123456789012:prefix-list/pl-abc123",
			wantType:    "ec2:PrefixList",
		},
		{
			name:        "Route53Resolver rule",
			resourceARN: "arn:aws:route53resolver:us-east-1:123456789012:resolver-rule/rslvr-rr-abc",
			wantType:    "route53resolver:ResolverRule",
		},
		{
			name:        "License Manager colon separator",
			resourceARN: "arn:aws:license-manager:us-east-1:123456789012:license-configuration:lic-abc123",
			wantType:    "license-manager:LicenseConfiguration",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rs, err := h.Backend.CreateResourceShare("type-derive-share", false, nil, nil, []string{tt.resourceARN})
			require.NoError(t, err)

			rec := doRAMRequest(t, h, "/listresources", map[string]any{
				"resourceOwner":    "SELF",
				"resourceShareArn": rs.ARN,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				Resources []struct {
					Type string `json:"type"`
				} `json:"resources"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			require.Len(t, resp.Resources, 1)
			assert.Equal(t, tt.wantType, resp.Resources[0].Type)
		})
	}
}

// --- ListPermissionVersions on built-ins ---

func TestRAM_Accuracy_ListPermissionVersions_BuiltIn(t *testing.T) {
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

// --- GetPermission returns policyTemplate for built-ins ---

func TestRAM_Accuracy_GetPermission_BuiltIn_HasPolicy(t *testing.T) {
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

// --- Snapshot/Restore preserves built-in permission fields ---

func TestRAM_Accuracy_Snapshot_PreservesBuiltInFields(t *testing.T) {
	t.Parallel()

	b := ram.NewInMemoryBackend("000000000000", "us-east-1")

	snap := b.Snapshot()
	require.NotNil(t, snap)

	b2 := ram.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(snap))

	// Check a built-in permission is still accessible and has correct fields.
	permARN := "arn:aws:ram::aws:permission/AWSRAMDefaultPermissionEC2Subnet"
	p, pv, err := b2.GetPermission(permARN, nil)
	require.NoError(t, err)

	assert.Equal(t, "AWS_MANAGED", p.PermissionType)
	assert.Equal(t, "REGIONAL", p.ResourceRegionScope)
	assert.True(t, p.IsResourceTypeDefault)
	assert.NotEmpty(t, pv.PolicyTemplate)
}

// --- PermissionType in ListPermissions summary ---

func TestRAM_Accuracy_ListPermissions_SummaryHasType(t *testing.T) {
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

// --- AllowExternalPrincipals toggle ---

func TestRAM_Accuracy_AllowExternalPrincipals_Default(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		allowExternal bool
		wantExternal bool
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

// --- CreateResourceShare with Principals and ResourceArns ---

func TestRAM_Accuracy_CreateResourceShare_WithPrincipalsAndResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		principals   []string
		resourceArns []string
		wantPrincipals int
		wantResources  int
	}{
		{
			name:           "only principals",
			principals:     []string{"123456789012", "arn:aws:iam::999999999999:root"},
			resourceArns:   nil,
			wantPrincipals: 2,
			wantResources:  0,
		},
		{
			name:           "only resources",
			principals:     nil,
			resourceArns:   []string{"arn:aws:ec2:us-east-1:123456789012:subnet/subnet-abc"},
			wantPrincipals: 0,
			wantResources:  1,
		},
		{
			name:           "both principals and resources",
			principals:     []string{"111111111111"},
			resourceArns:   []string{"arn:aws:ec2:us-east-1:123456789012:subnet/subnet-abc"},
			wantPrincipals: 1,
			wantResources:  1,
		},
		{
			name:           "no associations",
			principals:     nil,
			resourceArns:   nil,
			wantPrincipals: 0,
			wantResources:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{"name": "assoc-share-" + tt.name}
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

// --- GetResourceShares filtered by name ---

func TestRAM_Accuracy_GetResourceShares_NameFilter(t *testing.T) {
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

// --- Tags field on resource share ---

func TestRAM_Accuracy_ResourceShare_TagsInResponse(t *testing.T) {
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

// --- FeatureSet field on resource shares ---

func TestRAM_Accuracy_ResourceShare_FeatureSetStandard(t *testing.T) {
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

// --- GetResourceShareAssociations type filtering ---

func TestRAM_Accuracy_GetResourceShareAssociations_Filters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		associationType string
		setupPrincipals []string
		setupResources  []string
		wantCount      int
	}{
		{
			name:            "PRINCIPAL type returns only principals",
			associationType: "PRINCIPAL",
			setupPrincipals: []string{"111111111111", "222222222222"},
			setupResources:  []string{"arn:aws:ec2:us-east-1:123456789012:subnet/sub-1"},
			wantCount:       2,
		},
		{
			name:            "RESOURCE type returns only resources",
			associationType: "RESOURCE",
			setupPrincipals: []string{"111111111111"},
			setupResources:  []string{"arn:aws:ec2:us-east-1:123456789012:subnet/sub-1", "arn:aws:ec2:us-east-1:123456789012:subnet/sub-2"},
			wantCount:       2,
		},
		{
			name:            "no type filter returns all",
			associationType: "",
			setupPrincipals: []string{"111111111111"},
			setupResources:  []string{"arn:aws:ec2:us-east-1:123456789012:subnet/sub-1"},
			wantCount:       2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rs, err := h.Backend.CreateResourceShare(
				"assoc-filter-share",
				false,
				nil,
				tt.setupPrincipals,
				tt.setupResources,
			)
			require.NoError(t, err)

			body := map[string]any{
				"resourceShareArns": []string{rs.ARN},
			}
			if tt.associationType != "" {
				body["associationType"] = tt.associationType
			}

			rec := doRAMRequest(t, h, "/getresourceshareassociations", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				ResourceShareAssociations []json.RawMessage `json:"resourceShareAssociations"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Len(t, resp.ResourceShareAssociations, tt.wantCount)
		})
	}
}

// --- ReplacePermissionAssociations swaps permissions across shares ---

func TestRAM_Accuracy_ReplacePermissionAssociations(t *testing.T) {
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
			assert.Equal(t, "IN_PROGRESS", resp.ReplacePermissionAssociationsWork.Status)
		})
	}
}

// --- PromotePermissionCreatedFromPolicy returns correct type ---

func TestRAM_Accuracy_PromotePermissionCreatedFromPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*testing.T, *ram.Handler) string
		wantStatus int
	}{
		{
			name: "promote existing customer permission",
			setup: func(t *testing.T, h *ram.Handler) string {
				t.Helper()
				p, err := h.Backend.CreatePermission("promote-perm", "ec2:Subnet", `{}`, nil)
				require.NoError(t, err)

				return p.ARN
			},
			wantStatus: http.StatusOK,
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

// --- GetResourceShareInvitations accuracy ---

func TestRAM_Accuracy_GetResourceShareInvitations_Status(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*testing.T, *ram.Handler) string
		wantStatus string
	}{
		{
			name: "pending invitation",
			setup: func(t *testing.T, h *ram.Handler) string {
				t.Helper()
				rs, err := h.Backend.CreateResourceShare("inv-acc-share", true, nil, nil, nil)
				require.NoError(t, err)
				inv := ram.CreateInvitation(h.Backend.(*ram.InMemoryBackend), rs.ARN, "inv-acc-share", "111111111111", "222222222222")

				return inv.InvitationARN
			},
			wantStatus: "PENDING",
		},
		{
			name: "accepted invitation",
			setup: func(t *testing.T, h *ram.Handler) string {
				t.Helper()
				rs, err := h.Backend.CreateResourceShare("inv-acc2-share", true, nil, nil, nil)
				require.NoError(t, err)
				inv := ram.CreateInvitation(h.Backend.(*ram.InMemoryBackend), rs.ARN, "inv-acc2-share", "111111111111", "222222222222")
				_, err = h.Backend.AcceptResourceShareInvitation(inv.InvitationARN)
				require.NoError(t, err)

				return inv.InvitationARN
			},
			wantStatus: "ACCEPTED",
		},
		{
			name: "rejected invitation",
			setup: func(t *testing.T, h *ram.Handler) string {
				t.Helper()
				rs, err := h.Backend.CreateResourceShare("inv-rej-share", true, nil, nil, nil)
				require.NoError(t, err)
				inv := ram.CreateInvitation(h.Backend.(*ram.InMemoryBackend), rs.ARN, "inv-rej-share", "111111111111", "222222222222")
				_, err = h.Backend.RejectResourceShareInvitation(inv.InvitationARN)
				require.NoError(t, err)

				return inv.InvitationARN
			},
			wantStatus: "REJECTED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			invARN := tt.setup(t, h)

			rec := doRAMRequest(t, h, "/getresourceshareinvitations", map[string]any{
				"resourceShareInvitationArns": []string{invARN},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				ResourceShareInvitations []struct {
					Status string `json:"status"`
				} `json:"resourceShareInvitations"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			require.Len(t, resp.ResourceShareInvitations, 1)
			assert.Equal(t, tt.wantStatus, resp.ResourceShareInvitations[0].Status)
		})
	}
}

// --- SetDefaultPermissionVersion accuracy ---

func TestRAM_Accuracy_SetDefaultPermissionVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(*testing.T, *ram.Handler) (string, int32)
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

// --- ListPendingInvitationResources accuracy ---

func TestRAM_Accuracy_ListPendingInvitationResources(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		resourceArns  []string
		wantResources int
		wantStatus    int
	}{
		{
			name:          "invitation with resources",
			resourceArns:  []string{"arn:aws:ec2:us-east-1:123456789012:subnet/sub-1"},
			wantResources: 1,
			wantStatus:    http.StatusOK,
		},
		{
			name:          "invitation with multiple resources",
			resourceArns:  []string{"arn:aws:ec2:us-east-1:123456789012:subnet/sub-1", "arn:aws:ec2:us-east-1:123456789012:subnet/sub-2"},
			wantResources: 2,
			wantStatus:    http.StatusOK,
		},
		{
			name:          "invitation with no resources",
			resourceArns:  nil,
			wantResources: 0,
			wantStatus:    http.StatusOK,
		},
		{
			name:       "nonexistent invitation returns error",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.wantStatus == http.StatusBadRequest && tt.resourceArns == nil && tt.name == "nonexistent invitation returns error" {
				rec := doRAMRequest(t, h, "/listpendinginvitationresources", map[string]any{
					"resourceShareInvitationArn": "arn:aws:ram:us-east-1:000000000000:resource-share-invitation/nonexistent",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)

				return
			}

			rs, err := h.Backend.CreateResourceShare("pending-inv-share", true, nil, nil, tt.resourceArns)
			require.NoError(t, err)

			inv := ram.CreateInvitation(
				h.Backend.(*ram.InMemoryBackend),
				rs.ARN,
				"pending-inv-share",
				"111111111111",
				"222222222222",
			)

			rec := doRAMRequest(t, h, "/listpendinginvitationresources", map[string]any{
				"resourceShareInvitationArn": inv.InvitationARN,
			})
			require.Equal(t, tt.wantStatus, rec.Code)

			var resp struct {
				Resources []json.RawMessage `json:"resources"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Len(t, resp.Resources, tt.wantResources)
		})
	}
}

// --- ListResourceTypes accuracy ---

func TestRAM_Accuracy_ListResourceTypes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRAMRequest(t, h, "/listresourcetypes", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		ResourceTypes []struct {
			ResourceType string `json:"resourceType"`
			ServiceName  string `json:"serviceName"`
		} `json:"resourceTypes"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.NotEmpty(t, resp.ResourceTypes)

	found := map[string]bool{}
	for _, rt := range resp.ResourceTypes {
		found[rt.ResourceType] = true
		assert.NotEmpty(t, rt.ServiceName, "resource type %q should have serviceName", rt.ResourceType)
	}

	assert.True(t, found["ec2:Subnet"], "ec2:Subnet should be in list")
	assert.True(t, found["ec2:VPC"], "ec2:VPC should be in list")
}
