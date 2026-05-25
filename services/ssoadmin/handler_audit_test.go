package ssoadmin_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssoadmin"
)

// TestAudit_InstanceLazyActivation verifies instances transition CREATE_IN_PROGRESS → ACTIVE lazily.
func TestAudit_InstanceLazyActivation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus string
		describe   bool
	}{
		{
			name:       "describe triggers activation",
			describe:   true,
			wantStatus: "ACTIVE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doRequest(t, h, "CreateInstance", map[string]any{"Name": "lazy-inst"})
			require.Equal(t, http.StatusOK, rec.Code)
			resp := parseResponse(t, rec)
			arn := resp["InstanceArn"].(string)

			rec2 := doRequest(t, h, "DescribeInstance", map[string]any{"InstanceArn": arn})
			require.Equal(t, http.StatusOK, rec2.Code)
			resp2 := parseResponse(t, rec2)
			assert.Equal(t, tt.wantStatus, resp2["Status"])
		})
	}
}

// TestAudit_DeleteInstanceCascade verifies that DeleteInstance immediately removes dependent resources.
func TestAudit_DeleteInstanceCascade(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createPS   bool
		createApp  bool
		wantPSCnt  int
		wantAppCnt int
	}{
		{
			name:       "cascades permission sets and apps",
			createPS:   true,
			createApp:  true,
			wantPSCnt:  0,
			wantAppCnt: 0,
		},
		{
			name:       "empty instance cascade no-op",
			createPS:   false,
			createApp:  false,
			wantPSCnt:  0,
			wantAppCnt: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssoadmin.NewInMemoryBackend("000000000000", "us-east-1")
			h := ssoadmin.NewHandler(b)
			inst := b.AddInstanceInternal("cascade-inst")
			instanceArn := inst.InstanceArn

			if tt.createPS {
				b.AddPermissionSetInternal(instanceArn, "ps1")
			}
			if tt.createApp {
				b.AddApplicationInternal(instanceArn, "app1")
			}

			rec := doRequest(t, h, "DeleteInstance", map[string]any{"InstanceArn": instanceArn})
			require.Equal(t, http.StatusOK, rec.Code)

			assert.Equal(t, tt.wantPSCnt, ssoadmin.PermissionSetCount(b))
			assert.Equal(t, tt.wantAppCnt, ssoadmin.ApplicationCount(b))
		})
	}
}

// TestAudit_PermissionSetNameValidation verifies that invalid permission set names are rejected.
func TestAudit_PermissionSetNameValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		psName     string
		wantStatus int
	}{
		{
			name:       "valid name accepted",
			psName:     "ValidName123",
			wantStatus: http.StatusOK,
		},
		{
			name:       "name with special chars rejected",
			psName:     "bad name!",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty name rejected",
			psName:     "",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "ps-name-inst")
			rec := doRequest(t, h, "CreatePermissionSet", map[string]any{
				"InstanceArn": instanceArn,
				"Name":        tt.psName,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestAudit_SessionDurationValidation verifies ISO8601 duration validation for permission sets.
func TestAudit_SessionDurationValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		sessionDuration string
		wantStatus      int
	}{
		{
			name:            "valid PT1H accepted",
			sessionDuration: "PT1H",
			wantStatus:      http.StatusOK,
		},
		{
			name:            "valid PT12H accepted",
			sessionDuration: "PT12H",
			wantStatus:      http.StatusOK,
		},
		{
			name:            "invalid format rejected",
			sessionDuration: "1h",
			wantStatus:      http.StatusBadRequest,
		},
		{
			name:            "zero duration rejected",
			sessionDuration: "PT0H",
			wantStatus:      http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "session-duration-inst")
			rec := doRequest(t, h, "CreatePermissionSet", map[string]any{
				"InstanceArn":     instanceArn,
				"Name":            "TestPS",
				"SessionDuration": tt.sessionDuration,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestAudit_AccountAssignmentStatusTransition verifies IN_PROGRESS → SUCCEEDED on describe.
func TestAudit_AccountAssignmentStatusTransition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus string
	}{
		{
			name:       "creation status transitions to SUCCEEDED",
			wantStatus: "SUCCEEDED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "assign-status-inst")
			psArn := createPermissionSet(t, h, instanceArn, "StatusPS")

			rec := doRequest(t, h, "CreateAccountAssignment", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
				"TargetId":         "111111111111",
				"TargetType":       "AWS_ACCOUNT",
				"PrincipalType":    "USER",
				"PrincipalId":      "user-abc",
			})
			require.Equal(t, http.StatusOK, rec.Code)
			resp := parseResponse(t, rec)
			reqID := resp["AccountAssignmentCreationStatus"].(map[string]any)["RequestId"].(string)

			rec2 := doRequest(t, h, "DescribeAccountAssignmentCreationStatus", map[string]any{
				"InstanceArn":                        instanceArn,
				"AccountAssignmentCreationRequestId": reqID,
			})
			require.Equal(t, http.StatusOK, rec2.Code)
			resp2 := parseResponse(t, rec2)
			status := resp2["AccountAssignmentCreationStatus"].(map[string]any)["Status"].(string)
			assert.Equal(t, tt.wantStatus, status)
		})
	}
}

// TestAudit_ListAccountAssignmentCreationStatusFilter verifies filter by status works.
func TestAudit_ListAccountAssignmentCreationStatusFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		filter       string
		wantMinCount int
	}{
		{
			name:         "no filter returns all",
			filter:       "",
			wantMinCount: 1,
		},
		{
			name:         "SUCCEEDED filter returns items after transition",
			filter:       "SUCCEEDED",
			wantMinCount: 1,
		},
		{
			name:         "FAILED filter returns empty",
			filter:       "FAILED",
			wantMinCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "list-creation-status-inst")
			psArn := createPermissionSet(t, h, instanceArn, "FilterPS")

			rec := doRequest(t, h, "CreateAccountAssignment", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
				"TargetId":         "222222222222",
				"TargetType":       "AWS_ACCOUNT",
				"PrincipalType":    "USER",
				"PrincipalId":      "user-xyz",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			if tt.filter == "SUCCEEDED" {
				resp := parseResponse(t, rec)
				reqID := resp["AccountAssignmentCreationStatus"].(map[string]any)["RequestId"].(string)
				_ = doRequest(t, h, "DescribeAccountAssignmentCreationStatus", map[string]any{
					"InstanceArn":                        instanceArn,
					"AccountAssignmentCreationRequestId": reqID,
				})
			}

			listReq := map[string]any{"InstanceArn": instanceArn}
			if tt.filter != "" {
				listReq["Filter"] = map[string]any{"Status": tt.filter}
			}
			rec2 := doRequest(t, h, "ListAccountAssignmentCreationStatus", listReq)
			require.Equal(t, http.StatusOK, rec2.Code)
			resp2 := parseResponse(t, rec2)
			items := resp2["AccountAssignmentsCreationStatus"].([]any)
			if tt.wantMinCount == 0 {
				assert.Empty(t, items)
			} else {
				assert.GreaterOrEqual(t, len(items), tt.wantMinCount)
			}
		})
	}
}

// TestAudit_ProvisionPermissionSetStatusTransition verifies provisioning status transitions.
func TestAudit_ProvisionPermissionSetStatusTransition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		targetType string
		wantStatus string
	}{
		{
			name:       "ALL_PROVISIONED_ACCOUNTS transitions to SUCCEEDED",
			targetType: "ALL_PROVISIONED_ACCOUNTS",
			wantStatus: "SUCCEEDED",
		},
		{
			name:       "AWS_ACCOUNT transitions to SUCCEEDED",
			targetType: "AWS_ACCOUNT",
			wantStatus: "SUCCEEDED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "provision-status-inst")
			psArn := createPermissionSet(t, h, instanceArn, "ProvPS")

			reqBody := map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
				"TargetType":       tt.targetType,
			}
			if tt.targetType == "AWS_ACCOUNT" {
				reqBody["TargetId"] = "333333333333"
			}
			rec := doRequest(t, h, "ProvisionPermissionSet", reqBody)
			require.Equal(t, http.StatusOK, rec.Code)
			resp := parseResponse(t, rec)
			reqID := resp["PermissionSetProvisioningStatus"].(map[string]any)["RequestId"].(string)

			rec2 := doRequest(t, h, "DescribePermissionSetProvisioningStatus", map[string]any{
				"InstanceArn":                     instanceArn,
				"ProvisionPermissionSetRequestId": reqID,
			})
			require.Equal(t, http.StatusOK, rec2.Code)
			resp2 := parseResponse(t, rec2)
			status := resp2["PermissionSetProvisioningStatus"].(map[string]any)["Status"].(string)
			assert.Equal(t, tt.wantStatus, status)
		})
	}
}

// TestAudit_PermissionsBoundaryUnionType verifies put/get boundary with both ManagedPolicy and CMPR.
func TestAudit_PermissionsBoundaryUnionType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		managedPolicyArn string
		cmprName         string
		cmprPath         string
		wantErrPut       bool
	}{
		{
			name:             "managed policy boundary accepted",
			managedPolicyArn: "arn:aws:iam::aws:policy/ReadOnlyAccess",
			wantErrPut:       false,
		},
		{
			name:       "customer managed policy reference accepted",
			cmprName:   "my-policy",
			cmprPath:   "/",
			wantErrPut: false,
		},
		{
			name:             "both set rejected",
			managedPolicyArn: "arn:aws:iam::aws:policy/ReadOnlyAccess",
			cmprName:         "my-policy",
			wantErrPut:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "boundary-inst")
			psArn := createPermissionSet(t, h, instanceArn, "BoundaryPS")

			boundary := map[string]any{}
			if tt.managedPolicyArn != "" {
				boundary["ManagedPolicyArn"] = tt.managedPolicyArn
			}
			if tt.cmprName != "" {
				cmpr := map[string]any{"Name": tt.cmprName}
				if tt.cmprPath != "" {
					cmpr["Path"] = tt.cmprPath
				}
				boundary["CustomerManagedPolicyReference"] = cmpr
			}

			rec := doRequest(t, h, "PutPermissionsBoundaryToPermissionSet", map[string]any{
				"InstanceArn":         instanceArn,
				"PermissionSetArn":    psArn,
				"PermissionsBoundary": boundary,
			})
			if tt.wantErrPut {
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			} else {
				require.Equal(t, http.StatusOK, rec.Code)
				rec2 := doRequest(t, h, "GetPermissionsBoundaryForPermissionSet", map[string]any{
					"InstanceArn":      instanceArn,
					"PermissionSetArn": psArn,
				})
				require.Equal(t, http.StatusOK, rec2.Code)
				resp2 := parseResponse(t, rec2)
				pb := resp2["PermissionsBoundary"].(map[string]any)
				if tt.managedPolicyArn != "" {
					assert.Equal(t, tt.managedPolicyArn, pb["ManagedPolicyArn"])
				} else {
					cmpr := pb["CustomerManagedPolicyReference"].(map[string]any)
					assert.Equal(t, tt.cmprName, cmpr["Name"])
				}
			}
		})
	}
}

// TestAudit_TagKeyValidation verifies that tags with aws: prefix are rejected.
func TestAudit_TagKeyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		tagKey     string
		wantStatus int
	}{
		{
			name:       "valid tag key accepted",
			tagKey:     "Environment",
			wantStatus: http.StatusOK,
		},
		{
			name:       "aws: prefix tag key rejected",
			tagKey:     "aws:Reserved",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "AWS: prefix tag key rejected",
			tagKey:     "AWS:Reserved",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "tag-validation-inst")
			rec := doRequest(t, h, "CreatePermissionSet", map[string]any{
				"InstanceArn": instanceArn,
				"Name":        "TagPS",
				"Tags":        []map[string]any{{"Key": tt.tagKey, "Value": "val"}},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestAudit_ABACLifecycleStatus verifies ABAC config status transitions CREATION_IN_PROGRESS → ENABLED.
func TestAudit_ABACLifecycleStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus string
	}{
		{
			name:       "describe triggers ENABLED transition",
			wantStatus: "ENABLED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "abac-status-inst")

			rec := doRequest(t, h, "CreateInstanceAccessControlAttributeConfiguration", map[string]any{
				"InstanceArn": instanceArn,
				"InstanceAccessControlAttributeConfiguration": map[string]any{
					"AccessControlAttributes": []map[string]any{
						{
							"Key": "email",
							"Value": map[string]any{
								"Source": []string{"${path:enterprise.email}"},
							},
						},
					},
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			rec2 := doRequest(t, h, "DescribeInstanceAccessControlAttributeConfiguration", map[string]any{
				"InstanceArn": instanceArn,
			})
			require.Equal(t, http.StatusOK, rec2.Code)
			resp2 := parseResponse(t, rec2)
			assert.Equal(t, tt.wantStatus, resp2["Status"])
		})
	}
}

// TestAudit_ApplicationAuthMethodStructured verifies PutApplicationAuthenticationMethod stores structured body.
func TestAudit_ApplicationAuthMethodStructured(t *testing.T) {
	t.Parallel()

	tests := []struct {
		authBody       map[string]any
		name           string
		authMethodType string
		wantStatus     int
	}{
		{
			name:           "IAM auth method accepted",
			authMethodType: "IAM",
			authBody:       map[string]any{"Iam": map[string]any{}},
			wantStatus:     http.StatusOK,
		},
		{
			name:           "non-IAM auth method rejected",
			authMethodType: "SAML",
			authBody:       map[string]any{},
			wantStatus:     http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "auth-method-inst")
			appArn := createApplication(t, h, instanceArn, "AuthMethodApp")

			bodyBytes, err := json.Marshal(tt.authBody)
			require.NoError(t, err)

			rec := doRequest(t, h, "PutApplicationAuthenticationMethod", map[string]any{
				"ApplicationArn":           appArn,
				"AuthenticationMethodType": tt.authMethodType,
				"AuthenticationMethod":     json.RawMessage(bodyBytes),
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestAudit_ApplicationGrantStructured verifies PutApplicationGrant stores structured body.
func TestAudit_ApplicationGrantStructured(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		grantType  string
		wantStatus int
	}{
		{
			name:       "authorization_code grant accepted",
			grantType:  "authorization_code",
			wantStatus: http.StatusOK,
		},
		{
			name:       "refresh_token grant accepted",
			grantType:  "refresh_token",
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid grant type rejected",
			grantType:  "password",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "grant-inst")
			appArn := createApplication(t, h, instanceArn, "GrantApp")

			rec := doRequest(t, h, "PutApplicationGrant", map[string]any{
				"ApplicationArn": appArn,
				"GrantType":      tt.grantType,
				"Grant":          map[string]any{},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestAudit_StaticApplicationProviderCatalog verifies ListApplicationProviders returns static catalog.
func TestAudit_StaticApplicationProviderCatalog(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantMinCount int
	}{
		{
			name:         "returns at least 5 providers",
			wantMinCount: 5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doRequest(t, h, "ListApplicationProviders", map[string]any{})
			require.Equal(t, http.StatusOK, rec.Code)
			resp := parseResponse(t, rec)
			providers := resp["ApplicationProviders"].([]any)
			assert.GreaterOrEqual(t, len(providers), tt.wantMinCount)
		})
	}
}

// TestAudit_DescribeApplicationProviderAccountScopedArn verifies account-scoped custom provider ARN resolves.
func TestAudit_DescribeApplicationProviderAccountScopedArn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		arn        string
		wantStatus int
	}{
		{
			name:       "aws-managed ARN resolves",
			arn:        "arn:aws:sso::aws:applicationProvider/custom",
			wantStatus: http.StatusOK,
		},
		{
			name:       "account-scoped custom ARN resolves",
			arn:        "arn:aws:sso::123456789012:applicationProvider/custom",
			wantStatus: http.StatusOK,
		},
		{
			name:       "unknown provider returns 404",
			arn:        "arn:aws:sso::aws:applicationProvider/nonexistent",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doRequest(t, h, "DescribeApplicationProvider", map[string]any{
				"ApplicationProviderArn": tt.arn,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestAudit_TrustedTokenIssuerValidation verifies TTI type validation.
func TestAudit_TrustedTokenIssuerValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		issuerType string
		wantStatus int
	}{
		{
			name:       "OIDC_JWT accepted",
			issuerType: "OIDC_JWT",
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid type rejected",
			issuerType: "SAML",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "tti-validation-inst")
			rec := doRequest(t, h, "CreateTrustedTokenIssuer", map[string]any{
				"InstanceArn":            instanceArn,
				"Name":                   "MyIssuer",
				"TrustedTokenIssuerType": tt.issuerType,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestAudit_CustomerManagedPolicyReferenceValidation verifies CMPR name/path validation.
func TestAudit_CustomerManagedPolicyReferenceValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		cmprName   string
		cmprPath   string
		wantStatus int
	}{
		{
			name:       "valid name and path accepted",
			cmprName:   "my-policy",
			cmprPath:   "/",
			wantStatus: http.StatusOK,
		},
		{
			name:       "empty name rejected",
			cmprName:   "",
			cmprPath:   "/",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "invalid path rejected",
			cmprName:   "my-policy",
			cmprPath:   "no-leading-slash",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "cmpr-val-inst")
			psArn := createPermissionSet(t, h, instanceArn, "CMPRPS")

			rec := doRequest(t, h, "AttachCustomerManagedPolicyReferenceToPermissionSet", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
				"CustomerManagedPolicyReference": map[string]any{
					"Name": tt.cmprName,
					"Path": tt.cmprPath,
				},
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestAudit_ListPermissionSetProvisioningStatusFilter verifies filter parameter works.
func TestAudit_ListPermissionSetProvisioningStatusFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		filter       string
		triggerFlip  bool
		wantMinCount int
	}{
		{
			name:         "no filter returns all",
			filter:       "",
			wantMinCount: 1,
		},
		{
			name:         "FAILED filter returns empty",
			filter:       "FAILED",
			wantMinCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "provision-filter-inst")
			psArn := createPermissionSet(t, h, instanceArn, "ProvFilterPS")

			rec := doRequest(t, h, "ProvisionPermissionSet", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
				"TargetType":       "ALL_PROVISIONED_ACCOUNTS",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			listReq := map[string]any{"InstanceArn": instanceArn}
			if tt.filter != "" {
				listReq["Filter"] = map[string]any{"Status": tt.filter}
			}
			rec2 := doRequest(t, h, "ListPermissionSetProvisioningStatus", listReq)
			require.Equal(t, http.StatusOK, rec2.Code)
			resp2 := parseResponse(t, rec2)
			items := resp2["PermissionSetsProvisioningStatus"].([]any)
			if tt.wantMinCount == 0 {
				assert.Empty(t, items)
			} else {
				assert.GreaterOrEqual(t, len(items), tt.wantMinCount)
			}
		})
	}
}

// TestAudit_ApplicationPortalOptions verifies CreateApplication and UpdateApplication handle PortalOptions.
func TestAudit_ApplicationPortalOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		visibility   string
		signInOrigin string
		wantStatus   int
	}{
		{
			name:         "application created with portal options",
			visibility:   "ENABLED",
			signInOrigin: "IDENTITY_CENTER",
			wantStatus:   http.StatusOK,
		},
		{
			name:       "application created without portal options",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "portal-opts-inst")

			reqBody := map[string]any{
				"InstanceArn":            instanceArn,
				"ApplicationProviderArn": "arn:aws:sso::aws:applicationProvider/custom",
				"Name":                   "PortalApp",
			}
			if tt.visibility != "" {
				reqBody["PortalOptions"] = map[string]any{
					"Visibility": tt.visibility,
					"SignInOptions": map[string]any{
						"Origin": tt.signInOrigin,
					},
				}
			}

			rec := doRequest(t, h, "CreateApplication", reqBody)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestAudit_ApplicationStatusValidation verifies UpdateApplication validates Status enum.
func TestAudit_ApplicationStatusValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		status     string
		wantStatus int
	}{
		{
			name:       "ENABLED status accepted",
			status:     "ENABLED",
			wantStatus: http.StatusOK,
		},
		{
			name:       "DISABLED status accepted",
			status:     "DISABLED",
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid status rejected",
			status:     "ACTIVE",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "app-status-inst")
			appArn := createApplication(t, h, instanceArn, "StatusApp")

			rec := doRequest(t, h, "UpdateApplication", map[string]any{
				"ApplicationArn": appArn,
				"Status":         tt.status,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestAudit_GetApplicationGrant verifies structured grant retrieval.
func TestAudit_GetApplicationGrant(t *testing.T) {
	t.Parallel()

	tests := []struct {
		grantBody  map[string]any
		name       string
		grantType  string
		wantStatus int
	}{
		{
			name:       "stored grant retrievable",
			grantType:  "authorization_code",
			grantBody:  map[string]any{"AuthorizationCode": map[string]any{}},
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "get-grant-inst")
			appArn := createApplication(t, h, instanceArn, "GetGrantApp")

			rec := doRequest(t, h, "PutApplicationGrant", map[string]any{
				"ApplicationArn": appArn,
				"GrantType":      tt.grantType,
				"Grant":          tt.grantBody,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			rec2 := doRequest(t, h, "GetApplicationGrant", map[string]any{
				"ApplicationArn": appArn,
				"GrantType":      tt.grantType,
			})
			assert.Equal(t, tt.wantStatus, rec2.Code)
			if tt.wantStatus == http.StatusOK {
				resp2 := parseResponse(t, rec2)
				grant := resp2["Grant"].(map[string]any)
				assert.Equal(t, tt.grantType, grant["GrantType"])
			}
		})
	}
}

// TestAudit_GetApplicationAuthenticationMethod verifies structured auth method retrieval.
func TestAudit_GetApplicationAuthenticationMethod(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		authMethodType string
		wantStatus     int
	}{
		{
			name:           "stored IAM auth method retrievable",
			authMethodType: "IAM",
			wantStatus:     http.StatusOK,
		},
		{
			name:           "missing auth method returns 404",
			authMethodType: "IAM",
			wantStatus:     http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "get-auth-method-inst")
			appArn := createApplication(t, h, instanceArn, "GetAuthMethodApp")

			if tt.wantStatus == http.StatusOK {
				rec := doRequest(t, h, "PutApplicationAuthenticationMethod", map[string]any{
					"ApplicationArn":           appArn,
					"AuthenticationMethodType": tt.authMethodType,
					"AuthenticationMethod":     map[string]any{"Iam": map[string]any{}},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec2 := doRequest(t, h, "GetApplicationAuthenticationMethod", map[string]any{
				"ApplicationArn":           appArn,
				"AuthenticationMethodType": tt.authMethodType,
			})
			assert.Equal(t, tt.wantStatus, rec2.Code)
		})
	}
}

// createApplication is a test helper to create an application and return its ARN.
func createApplication(t *testing.T, h *ssoadmin.Handler, instanceArn, name string) string {
	t.Helper()
	rec := doRequest(t, h, "CreateApplication", map[string]any{
		"InstanceArn":            instanceArn,
		"ApplicationProviderArn": "arn:aws:sso::aws:applicationProvider/custom",
		"Name":                   name,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	arn, ok := resp["ApplicationArn"].(string)
	require.True(t, ok, "expected ApplicationArn in response")
	require.NotEmpty(t, arn)

	return arn
}
