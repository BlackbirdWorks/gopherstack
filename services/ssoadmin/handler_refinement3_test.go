package ssoadmin_test

import (
	"net/http"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssoadmin"
)

// TestRefinement3_HandlerOpsLen verifies GetSupportedOperations count after refinement3.
func TestRefinement3_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	assert.Equal(t, 78, ssoadmin.HandlerOpsLen(h))
}

// TestRefinement3_DeletePermissionSetWithAssignmentsConflict verifies that DeletePermissionSet
// returns ConflictException when there are active account assignments.
func TestRefinement3_DeletePermissionSetWithAssignmentsConflict(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantCode   string
		wantStatus int
	}{
		{
			name:       "delete_ps_with_assignments_returns_conflict",
			wantStatus: http.StatusConflict,
			wantCode:   "ConflictException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "r3-del-ps-inst")
			psArn := createPermissionSet(t, h, instanceArn, "r3-del-ps")

			// Create an account assignment so the PS has active assignments.
			rec := doRequest(t, h, "CreateAccountAssignment", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
				"TargetType":       "AWS_ACCOUNT",
				"TargetId":         "111122223333",
				"PrincipalType":    "USER",
				"PrincipalId":      "user-123",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// Now try to delete the PS — should fail with ConflictException.
			delRec := doRequest(t, h, "DeletePermissionSet", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
			})
			assert.Equal(t, tt.wantStatus, delRec.Code)
			resp := parseResponse(t, delRec)
			assert.Equal(t, tt.wantCode, resp["__type"])
		})
	}
}

// TestRefinement3_DetachManagedPolicyNotFound verifies that DetachManagedPolicyFromPermissionSet
// returns ResourceNotFoundException when the policy is not attached.
func TestRefinement3_DetachManagedPolicyNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		policyArn  string
		wantStatus int
	}{
		{
			name:       "detach_policy_not_attached",
			policyArn:  "arn:aws:iam::aws:policy/NotAttachedPolicy",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "r3-detach-inst")
			psArn := createPermissionSet(t, h, instanceArn, "r3-detach-ps")

			rec := doRequest(t, h, "DetachManagedPolicyFromPermissionSet", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
				"ManagedPolicyArn": tt.policyArn,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement3_ListPermissionSetProvisioningStatusSorted verifies that results are
// sorted by date descending.
func TestRefinement3_ListPermissionSetProvisioningStatusSorted(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "r3-prov-inst")

	// Provision multiple permission sets.
	for i := range 3 {
		psArn := createPermissionSet(t, h, instanceArn, "r3-prov-ps-"+string(rune('A'+i)))
		rec := doRequest(t, h, "ProvisionPermissionSet", map[string]any{
			"InstanceArn":      instanceArn,
			"PermissionSetArn": psArn,
			"TargetType":       "ALL_PROVISIONED_ACCOUNTS",
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doRequest(t, h, "ListPermissionSetProvisioningStatus", map[string]any{
		"InstanceArn": instanceArn,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResponse(t, rec)
	statuses, ok := resp["PermissionSetsProvisioningStatus"].([]any)
	require.True(t, ok)
	assert.Len(t, statuses, 3)

	// Verify sorted order (descending by date means first >= second >= third).
	for i := 1; i < len(statuses); i++ {
		prev := statuses[i-1].(map[string]any)["CreatedDate"].(float64)
		curr := statuses[i].(map[string]any)["CreatedDate"].(float64)
		assert.GreaterOrEqual(t, prev, curr, "statuses should be sorted by date desc")
	}
}

// TestRefinement3_ListInstancesIncludesCreatedDate verifies CreatedDate in ListInstances response.
func TestRefinement3_ListInstancesIncludesCreatedDate(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := doRequest(t, h, "ListInstances", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResponse(t, rec)
	instances, ok := resp["Instances"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, instances)

	inst := instances[0].(map[string]any)
	createdDate, hasDate := inst["CreatedDate"]
	assert.True(t, hasDate, "ListInstances should include CreatedDate")
	assert.NotNil(t, createdDate)
	assert.Greater(t, createdDate.(float64), float64(0), "CreatedDate should be a positive unix timestamp")
}

// TestRefinement3_DescribeInstanceIncludesTags verifies Tags field in DescribeInstance response.
func TestRefinement3_DescribeInstanceIncludesTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "r3-tags-inst")

	// Tag the instance.
	tagRec := doRequest(t, h, "TagResource", map[string]any{
		"InstanceArn": instanceArn,
		"ResourceArn": instanceArn,
		"Tags":        []map[string]any{{"Key": "env", "Value": "test"}},
	})
	require.Equal(t, http.StatusOK, tagRec.Code)

	rec := doRequest(t, h, "DescribeInstance", map[string]any{"InstanceArn": instanceArn})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResponse(t, rec)
	tags, hasTags := resp["Tags"]
	assert.True(t, hasTags, "DescribeInstance should include Tags")
	tagSlice, ok := tags.([]any)
	assert.True(t, ok)
	assert.Len(t, tagSlice, 1)
	tagEntry := tagSlice[0].(map[string]any)
	assert.Equal(t, "env", tagEntry["Key"])
	assert.Equal(t, "test", tagEntry["Value"])
}

// TestRefinement3_DescribeTrustedTokenIssuerIncludesTags verifies Tags field in
// DescribeTrustedTokenIssuer response.
func TestRefinement3_DescribeTrustedTokenIssuerIncludesTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "r3-tti-tags-inst")

	rec := doRequest(t, h, "CreateTrustedTokenIssuer", map[string]any{
		"InstanceArn":            instanceArn,
		"Name":                   "r3-tti-tagged",
		"TrustedTokenIssuerType": "OIDC_JWT",
		"Tags":                   []map[string]any{{"Key": "tier", "Value": "prod"}},
		"TrustedTokenIssuerConfiguration": map[string]any{
			"OidcJwtConfiguration": map[string]any{
				"IssuerUrl":                  "https://issuer.example.com",
				"ClaimAttributePath":         "sub",
				"IdentityStoreAttributePath": "UserName",
				"JwksRetrievalOption":        "OPEN_ID_DISCOVERY",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	ttiArn := parseResponse(t, rec)["TrustedTokenIssuerArn"].(string)

	descRec := doRequest(t, h, "DescribeTrustedTokenIssuer", map[string]any{
		"TrustedTokenIssuerArn": ttiArn,
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	resp := parseResponse(t, descRec)
	ttiMap := resp["TrustedTokenIssuer"].(map[string]any)

	tags, hasTags := ttiMap["Tags"]
	assert.True(t, hasTags, "DescribeTrustedTokenIssuer should include Tags")
	tagSlice, ok := tags.([]any)
	assert.True(t, ok)
	assert.Len(t, tagSlice, 1)
	tagEntry := tagSlice[0].(map[string]any)
	assert.Equal(t, "tier", tagEntry["Key"])
	assert.Equal(t, "prod", tagEntry["Value"])
}

// TestRefinement3_UpdateTrustedTokenIssuerWithConfig verifies that configuration can be updated.
func TestRefinement3_UpdateTrustedTokenIssuerWithConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "r3-tti-cfg-inst")

	createRec := doRequest(t, h, "CreateTrustedTokenIssuer", map[string]any{
		"InstanceArn":            instanceArn,
		"Name":                   "r3-tti-update",
		"TrustedTokenIssuerType": "OIDC_JWT",
		"TrustedTokenIssuerConfiguration": map[string]any{
			"OidcJwtConfiguration": map[string]any{
				"IssuerUrl":                  "https://old-issuer.example.com",
				"ClaimAttributePath":         "sub",
				"IdentityStoreAttributePath": "UserName",
				"JwksRetrievalOption":        "OPEN_ID_DISCOVERY",
			},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	ttiArn := parseResponse(t, createRec)["TrustedTokenIssuerArn"].(string)

	// Update with new OIDC config.
	updateRec := doRequest(t, h, "UpdateTrustedTokenIssuer", map[string]any{
		"TrustedTokenIssuerArn": ttiArn,
		"TrustedTokenIssuerConfiguration": map[string]any{
			"OidcJwtConfiguration": map[string]any{
				"IssuerUrl":                  "https://new-issuer.example.com",
				"ClaimAttributePath":         "email",
				"IdentityStoreAttributePath": "Emails",
				"JwksRetrievalOption":        "OPEN_ID_DISCOVERY",
			},
		},
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	// Verify the config was updated.
	descRec := doRequest(t, h, "DescribeTrustedTokenIssuer", map[string]any{
		"TrustedTokenIssuerArn": ttiArn,
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	resp := parseResponse(t, descRec)
	ttiMap := resp["TrustedTokenIssuer"].(map[string]any)
	cfg := ttiMap["TrustedTokenIssuerConfiguration"].(map[string]any)
	oidc := cfg["OidcJwtConfiguration"].(map[string]any)
	assert.Equal(t, "https://new-issuer.example.com", oidc["IssuerUrl"])
	assert.Equal(t, "email", oidc["ClaimAttributePath"])
}

// TestRefinement3_ApplicationProviderDisplayDataStruct verifies DisplayData is a struct.
func TestRefinement3_ApplicationProviderDisplayDataStruct(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "r3-provider-inst")

	_ = doRequest(t, h, "CreateApplication", map[string]any{
		"InstanceArn":            instanceArn,
		"ApplicationProviderArn": "arn:aws:sso::123456789012:applicationProvider/custom",
		"Name":                   "r3-provider-app",
	})

	rec := doRequest(t, h, "DescribeApplicationProvider", map[string]any{
		"ApplicationProviderArn": "arn:aws:sso::123456789012:applicationProvider/custom",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResponse(t, rec)
	displayData := resp["DisplayData"]
	// Verify DisplayData is a map (struct), not a string.
	displayDataMap, ok := displayData.(map[string]any)
	assert.True(t, ok, "DisplayData should be a struct/object, not a string")
	assert.NotEmpty(t, displayDataMap["DisplayName"])
}

// TestRefinement3_PrincipalTypeValidation verifies CreateAccountAssignment validates PrincipalType.
func TestRefinement3_PrincipalTypeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		principalType string
		wantStatus    int
	}{
		{
			name:          "valid_USER",
			principalType: "USER",
			wantStatus:    http.StatusOK,
		},
		{
			name:          "valid_GROUP",
			principalType: "GROUP",
			wantStatus:    http.StatusOK,
		},
		{
			name:          "invalid_principal_type",
			principalType: "ROLE",
			wantStatus:    http.StatusBadRequest,
		},
		{
			name:          "empty_principal_type",
			principalType: "",
			wantStatus:    http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "r3-pt-inst")
			psArn := createPermissionSet(t, h, instanceArn, "r3-pt-ps")

			rec := doRequest(t, h, "CreateAccountAssignment", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
				"TargetType":       "AWS_ACCOUNT",
				"TargetId":         "111122223333",
				"PrincipalType":    tt.principalType,
				"PrincipalId":      "principal-123",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement3_TargetTypeValidation verifies CreateAccountAssignment validates TargetType.
func TestRefinement3_TargetTypeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		targetType string
		wantStatus int
	}{
		{
			name:       "valid_AWS_ACCOUNT",
			targetType: "AWS_ACCOUNT",
			wantStatus: http.StatusOK,
		},
		{
			name:       "empty_target_type_allowed",
			targetType: "",
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid_target_type",
			targetType: "ORGANIZATION",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "r3-tt-inst")
			psArn := createPermissionSet(t, h, instanceArn, "r3-tt-ps")

			rec := doRequest(t, h, "CreateAccountAssignment", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
				"TargetType":       tt.targetType,
				"TargetId":         "111122223333",
				"PrincipalType":    "USER",
				"PrincipalId":      "user-123",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement3_CreatePermissionSetNameTooLong verifies name length validation.
func TestRefinement3_CreatePermissionSetNameTooLong(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		psName     string
		wantStatus int
	}{
		{
			name:       "name_exactly_32_chars",
			psName:     "12345678901234567890123456789012",
			wantStatus: http.StatusOK,
		},
		{
			name:       "name_33_chars_too_long",
			psName:     "123456789012345678901234567890123",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "normal_name",
			psName:     "AdminAccess",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "r3-name-inst")

			rec := doRequest(t, h, "CreatePermissionSet", map[string]any{
				"InstanceArn": instanceArn,
				"Name":        tt.psName,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement3_GetApplicationAccessScope verifies GetApplicationAccessScope operation.
func TestRefinement3_GetApplicationAccessScope(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		scope      string
		putScope   bool
		wantStatus int
	}{
		{
			name:       "get_existing_scope",
			scope:      "openid",
			putScope:   true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "get_nonexistent_scope",
			scope:      "nonexistent",
			putScope:   false,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "missing_scope_param",
			scope:      "",
			putScope:   false,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "r3-scope-inst")
			appRec := doRequest(t, h, "CreateApplication", map[string]any{
				"InstanceArn":            instanceArn,
				"ApplicationProviderArn": "arn:aws:sso::123456789012:applicationProvider/custom",
				"Name":                   "r3-scope-app",
			})
			require.Equal(t, http.StatusOK, appRec.Code)
			appArn := parseResponse(t, appRec)["ApplicationArn"].(string)

			if tt.putScope {
				putRec := doRequest(t, h, "PutApplicationAccessScope", map[string]any{
					"ApplicationArn": appArn,
					"Scope":          tt.scope,
				})
				require.Equal(t, http.StatusOK, putRec.Code)
			}

			rec := doRequest(t, h, "GetApplicationAccessScope", map[string]any{
				"ApplicationArn": appArn,
				"Scope":          tt.scope,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResponse(t, rec)
				assert.Equal(t, tt.scope, resp["Scope"])
				assert.NotNil(t, resp["AuthorizedTargets"])
			}
		})
	}
}

// TestRefinement3_GetApplicationAuthenticationMethod verifies GetApplicationAuthenticationMethod.
func TestRefinement3_GetApplicationAuthenticationMethod(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		authType   string
		putAuth    bool
		wantStatus int
	}{
		{
			name:       "get_existing_auth_method",
			authType:   "IAM",
			putAuth:    true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "get_nonexistent_auth_method",
			authType:   "SAML",
			putAuth:    false,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "r3-auth-inst")
			appRec := doRequest(t, h, "CreateApplication", map[string]any{
				"InstanceArn":            instanceArn,
				"ApplicationProviderArn": "arn:aws:sso::123456789012:applicationProvider/custom",
				"Name":                   "r3-auth-app",
			})
			require.Equal(t, http.StatusOK, appRec.Code)
			appArn := parseResponse(t, appRec)["ApplicationArn"].(string)

			if tt.putAuth {
				putRec := doRequest(t, h, "PutApplicationAuthenticationMethod", map[string]any{
					"ApplicationArn":           appArn,
					"AuthenticationMethodType": tt.authType,
				})
				require.Equal(t, http.StatusOK, putRec.Code)
			}

			rec := doRequest(t, h, "GetApplicationAuthenticationMethod", map[string]any{
				"ApplicationArn":           appArn,
				"AuthenticationMethodType": tt.authType,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResponse(t, rec)
				authMethod := resp["AuthenticationMethod"].(map[string]any)
				assert.Equal(t, tt.authType, authMethod["AuthenticationMethodType"])
			}
		})
	}
}

// TestRefinement3_GetApplicationGrant verifies GetApplicationGrant operation.
func TestRefinement3_GetApplicationGrant(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		grantType  string
		putGrant   bool
		wantStatus int
	}{
		{
			name:       "get_existing_grant",
			grantType:  "authorization_code",
			putGrant:   true,
			wantStatus: http.StatusOK,
		},
		{
			name:       "get_nonexistent_grant",
			grantType:  "implicit",
			putGrant:   false,
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "missing_grant_type_param",
			grantType:  "",
			putGrant:   false,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "r3-grant-inst")
			appRec := doRequest(t, h, "CreateApplication", map[string]any{
				"InstanceArn":            instanceArn,
				"ApplicationProviderArn": "arn:aws:sso::123456789012:applicationProvider/custom",
				"Name":                   "r3-grant-app",
			})
			require.Equal(t, http.StatusOK, appRec.Code)
			appArn := parseResponse(t, appRec)["ApplicationArn"].(string)

			if tt.putGrant {
				putRec := doRequest(t, h, "PutApplicationGrant", map[string]any{
					"ApplicationArn": appArn,
					"GrantType":      tt.grantType,
				})
				require.Equal(t, http.StatusOK, putRec.Code)
			}

			rec := doRequest(t, h, "GetApplicationGrant", map[string]any{
				"ApplicationArn": appArn,
				"GrantType":      tt.grantType,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResponse(t, rec)
				grant := resp["Grant"].(map[string]any)
				assert.Equal(t, tt.grantType, grant["GrantType"])
			}
		})
	}
}

// TestRefinement3_ListAccountsForProvisionedPermissionSet verifies the new operation.
func TestRefinement3_ListAccountsForProvisionedPermissionSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		accounts   []string
		wantStatus int
		wantCount  int
	}{
		{
			name:       "ps_with_multiple_accounts",
			accounts:   []string{"111122223333", "444455556666", "777788889999"},
			wantStatus: http.StatusOK,
			wantCount:  3,
		},
		{
			name:       "ps_with_no_accounts",
			accounts:   []string{},
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "r3-lacps-inst")
			psArn := createPermissionSet(t, h, instanceArn, "r3-lacps-ps")

			for _, accountID := range tt.accounts {
				rec := doRequest(t, h, "CreateAccountAssignment", map[string]any{
					"InstanceArn":      instanceArn,
					"PermissionSetArn": psArn,
					"TargetType":       "AWS_ACCOUNT",
					"TargetId":         accountID,
					"PrincipalType":    "USER",
					"PrincipalId":      "user-xyz",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "ListAccountsForProvisionedPermissionSet", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResponse(t, rec)
				accounts, ok := resp["AccountIds"].([]any)
				require.True(t, ok)
				assert.Len(t, accounts, tt.wantCount)
			}
		})
	}
}

// TestRefinement3_ListAccountsForProvisionedPermissionSetErrors tests error paths.
func TestRefinement3_ListAccountsForProvisionedPermissionSetErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		req        map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "missing_instance_arn",
			req:        map[string]any{"PermissionSetArn": "arn:aws:sso:::permissionSet/inst/ps"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing_permission_set_arn",
			req:        map[string]any{"InstanceArn": "arn:aws:sso:::instance/ssoins-test"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doRequest(t, h, "ListAccountsForProvisionedPermissionSet", tt.req)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestRefinement3_ListApplicationAssignmentsForPrincipal verifies the new operation.
func TestRefinement3_ListApplicationAssignmentsForPrincipal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		principalID   string
		principalType string
		wantStatus    int
		wantCount     int
	}{
		{
			name:          "principal_with_app_assignments",
			principalID:   "user-abc",
			principalType: "USER",
			wantStatus:    http.StatusOK,
			wantCount:     2,
		},
		{
			name:          "principal_with_no_assignments",
			principalID:   "user-nobody",
			principalType: "USER",
			wantStatus:    http.StatusOK,
			wantCount:     0,
		},
		{
			name:          "missing_principal_id",
			principalID:   "",
			principalType: "USER",
			wantStatus:    http.StatusBadRequest,
		},
		{
			name:          "missing_principal_type",
			principalID:   "user-abc",
			principalType: "",
			wantStatus:    http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "r3-laafp-inst")

			// Create 2 apps and assign user-abc to both.
			for i := range 2 {
				appRec := doRequest(t, h, "CreateApplication", map[string]any{
					"InstanceArn":            instanceArn,
					"ApplicationProviderArn": "arn:aws:sso::123456789012:applicationProvider/custom",
					"Name":                   "r3-laafp-app-" + string(rune('A'+i)),
				})
				require.Equal(t, http.StatusOK, appRec.Code)
				appArn := parseResponse(t, appRec)["ApplicationArn"].(string)

				assignRec := doRequest(t, h, "CreateApplicationAssignment", map[string]any{
					"ApplicationArn": appArn,
					"PrincipalId":    "user-abc",
					"PrincipalType":  "USER",
				})
				require.Equal(t, http.StatusOK, assignRec.Code)
			}

			rec := doRequest(t, h, "ListApplicationAssignmentsForPrincipal", map[string]any{
				"InstanceArn":   instanceArn,
				"PrincipalId":   tt.principalID,
				"PrincipalType": tt.principalType,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				resp := parseResponse(t, rec)
				assignments, ok := resp["ApplicationAssignments"].([]any)
				require.True(t, ok)
				assert.Len(t, assignments, tt.wantCount)
			}
		})
	}
}

// TestRefinement3_GetApplicationAccessScopeNotFoundAfterDelete verifies scope removed after delete.
func TestRefinement3_GetApplicationAccessScopeNotFoundAfterDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "r3-scope-del-inst")
	appRec := doRequest(t, h, "CreateApplication", map[string]any{
		"InstanceArn":            instanceArn,
		"ApplicationProviderArn": "arn:aws:sso::123456789012:applicationProvider/custom",
		"Name":                   "r3-scope-del-app",
	})
	require.Equal(t, http.StatusOK, appRec.Code)
	appArn := parseResponse(t, appRec)["ApplicationArn"].(string)

	// Add scope.
	putRec := doRequest(t, h, "PutApplicationAccessScope", map[string]any{
		"ApplicationArn": appArn,
		"Scope":          "profile",
	})
	require.Equal(t, http.StatusOK, putRec.Code)

	// Verify scope exists.
	getRec := doRequest(t, h, "GetApplicationAccessScope", map[string]any{
		"ApplicationArn": appArn,
		"Scope":          "profile",
	})
	assert.Equal(t, http.StatusOK, getRec.Code)

	// Remove scope.
	delRec := doRequest(t, h, "DeleteApplicationAccessScope", map[string]any{
		"ApplicationArn": appArn,
		"Scope":          "profile",
	})
	require.Equal(t, http.StatusOK, delRec.Code)

	// Now it should return 404.
	getRec2 := doRequest(t, h, "GetApplicationAccessScope", map[string]any{
		"ApplicationArn": appArn,
		"Scope":          "profile",
	})
	assert.Equal(t, http.StatusNotFound, getRec2.Code)
}

// TestRefinement3_DeletePermissionSetAfterRemovingAssignmentsSucceeds verifies the
// happy path works once assignments are removed.
func TestRefinement3_DeletePermissionSetAfterRemovingAssignmentsSucceeds(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "r3-del-clear-inst")
	psArn := createPermissionSet(t, h, instanceArn, "r3-del-clear-ps")

	// Create assignment.
	rec := doRequest(t, h, "CreateAccountAssignment", map[string]any{
		"InstanceArn":      instanceArn,
		"PermissionSetArn": psArn,
		"TargetType":       "AWS_ACCOUNT",
		"TargetId":         "999988887777",
		"PrincipalType":    "GROUP",
		"PrincipalId":      "grp-delete-test",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Confirm deletion fails.
	delFail := doRequest(t, h, "DeletePermissionSet", map[string]any{
		"InstanceArn":      instanceArn,
		"PermissionSetArn": psArn,
	})
	require.Equal(t, http.StatusConflict, delFail.Code)

	// Remove the assignment.
	delAssign := doRequest(t, h, "DeleteAccountAssignment", map[string]any{
		"InstanceArn":      instanceArn,
		"PermissionSetArn": psArn,
		"TargetType":       "AWS_ACCOUNT",
		"TargetId":         "999988887777",
		"PrincipalType":    "GROUP",
		"PrincipalId":      "grp-delete-test",
	})
	require.Equal(t, http.StatusOK, delAssign.Code)

	// Now deletion should succeed.
	delOK := doRequest(t, h, "DeletePermissionSet", map[string]any{
		"InstanceArn":      instanceArn,
		"PermissionSetArn": psArn,
	})
	assert.Equal(t, http.StatusOK, delOK.Code)
}

// TestRefinement3_SDKCompletenessNoNotImplemented verifies SDK completeness.
func TestRefinement3_SDKCompletenessNoNotImplemented(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	ops := h.GetSupportedOperations()
	// Verify the 5 newly added operations are present.
	expectedOps := []string{
		"GetApplicationAccessScope",
		"GetApplicationAuthenticationMethod",
		"GetApplicationGrant",
		"ListAccountsForProvisionedPermissionSet",
		"ListApplicationAssignmentsForPrincipal",
	}
	for _, op := range expectedOps {
		found := slices.Contains(ops, op)
		assert.True(t, found, "operation %s should be in GetSupportedOperations", op)
	}
}
