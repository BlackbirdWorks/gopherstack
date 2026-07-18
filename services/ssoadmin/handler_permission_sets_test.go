package ssoadmin_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssoadmin"
)

func TestPermissionSetExtended(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceARN := createInstance(t, h, "test-instance")

	// Create permission set
	rec := doRequest(t, h, "CreatePermissionSet", map[string]any{
		"InstanceArn": instanceARN,
		"Name":        "my-perm-set",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	psARN := resp["PermissionSet"].(map[string]any)["PermissionSetArn"].(string)

	// ListCustomerManagedPolicyReferencesInPermissionSet
	rec = doRequest(t, h, "ListCustomerManagedPolicyReferencesInPermissionSet", map[string]any{
		"InstanceArn":      instanceARN,
		"PermissionSetArn": psARN,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// DeletePermissionsBoundaryFromPermissionSet (no boundary set - exercises the code path)
	doRequest(t, h, "DeletePermissionsBoundaryFromPermissionSet", map[string]any{
		"InstanceArn":      instanceARN,
		"PermissionSetArn": psARN,
	})
}

// TestPermissionSetNameValidation verifies that invalid permission set names are rejected.
func TestPermissionSetNameValidation(t *testing.T) {
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

// TestSessionDurationValidation verifies ISO8601 duration validation for permission sets.
func TestSessionDurationValidation(t *testing.T) {
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

// TestProvisionPermissionSetStatusTransition verifies provisioning status transitions.
func TestProvisionPermissionSetStatusTransition(t *testing.T) {
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

// TestListPermissionSetProvisioningStatusFilter verifies filter parameter works.
func TestListPermissionSetProvisioningStatusFilter(t *testing.T) {
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

// TestPermissionSetCount verifies PermissionSetCount export helper.
func TestPermissionSetCount(t *testing.T) {
	t.Parallel()

	b := ssoadmin.NewInMemoryBackend("000000000000", "us-east-1")
	assert.Equal(t, 0, ssoadmin.PermissionSetCount(b))

	inst := b.AddInstanceInternal("ps-inst")
	b.AddPermissionSetInternal(inst.InstanceArn, "ps1")
	b.AddPermissionSetInternal(inst.InstanceArn, "ps2")
	assert.Equal(t, 2, ssoadmin.PermissionSetCount(b))
}

// TestCreatePermissionSetValidation verifies required-field validation.
func TestCreatePermissionSetValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		instanceArn string
		psName      string
		wantStatus  int
	}{
		{
			name:        "missing instance arn",
			instanceArn: "",
			psName:      "MyPS",
			wantStatus:  http.StatusBadRequest,
		},
		{
			name:        "missing name",
			instanceArn: "arn:aws:sso:::instance/ssoins-test1234",
			psName:      "",
			wantStatus:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			rec := doRequest(t, h, "CreatePermissionSet", map[string]any{
				"InstanceArn": tt.instanceArn,
				"Name":        tt.psName,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestDefaultSessionDuration verifies that a new permission set gets a default
// session duration of PT1H.
func TestDefaultSessionDuration(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "session-inst")
	psArn := createPermissionSet(t, h, instanceArn, "session-ps")

	rec := doRequest(t, h, "DescribePermissionSet", map[string]any{
		"InstanceArn":      instanceArn,
		"PermissionSetArn": psArn,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	ps := resp["PermissionSet"].(map[string]any)
	assert.Equal(t, "PT1H", ps["SessionDuration"])
}

// TestAddPermissionSetInternal verifies the seed helper.
func TestAddPermissionSetInternal(t *testing.T) {
	t.Parallel()

	b := ssoadmin.NewInMemoryBackend("000000000000", "us-east-1")
	inst := b.AddInstanceInternal("seed-inst")
	ps := b.AddPermissionSetInternal(inst.InstanceArn, "seed-ps")
	require.NotNil(t, ps)
	assert.NotEmpty(t, ps.PermissionSetArn)
	assert.Equal(t, "seed-ps", ps.Name)
	assert.Equal(t, 1, ssoadmin.PermissionSetCount(b))
}

// TestListPermissionSetsProvisionedToAccount verifies the new operation.
func TestListPermissionSetsProvisionedToAccount(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "pspta-inst")
	psArn := createPermissionSet(t, h, instanceArn, "pspta-ps")

	accountID := "111111111111"
	doRequest(t, h, "CreateAccountAssignment", map[string]any{
		"InstanceArn":      instanceArn,
		"PermissionSetArn": psArn,
		"TargetId":         accountID,
		"TargetType":       "AWS_ACCOUNT",
		"PrincipalType":    "USER",
		"PrincipalId":      "user-xyz",
	})

	rec := doRequest(t, h, "ListPermissionSetsProvisionedToAccount", map[string]any{
		"InstanceArn": instanceArn,
		"AccountId":   accountID,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	psets, ok := resp["PermissionSets"].([]any)
	require.True(t, ok)
	assert.Len(t, psets, 1)
	assert.Equal(t, psArn, psets[0].(string))
}

// TestListPermissionSetsProvisionedToAccountMissing verifies required fields.
func TestListPermissionSetsProvisionedToAccountMissing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		payload    map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "missing InstanceArn",
			payload:    map[string]any{"AccountId": "123456789012"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing AccountId",
			payload:    map[string]any{"InstanceArn": "arn:aws:sso:::instance/ssoins-xxx"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			rec := doRequest(t, h, "ListPermissionSetsProvisionedToAccount", tt.payload)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestDeletePermissionSetWithAssignmentsConflict verifies that DeletePermissionSet
// returns ConflictException when there are active account assignments.
func TestDeletePermissionSetWithAssignmentsConflict(t *testing.T) {
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

// TestListPermissionSetProvisioningStatusSorted verifies that results are
// sorted by date descending.
func TestListPermissionSetProvisioningStatusSorted(t *testing.T) {
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

// TestCreatePermissionSetNameTooLong verifies name length validation.
func TestCreatePermissionSetNameTooLong(t *testing.T) {
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

// TestListAccountsForProvisionedPermissionSet verifies the new operation.
func TestListAccountsForProvisionedPermissionSet(t *testing.T) {
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

// TestListAccountsForProvisionedPermissionSetErrors tests error paths.
func TestListAccountsForProvisionedPermissionSetErrors(t *testing.T) {
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

// TestDeletePermissionSetAfterRemovingAssignmentsSucceeds verifies the
// happy path works once assignments are removed.
func TestDeletePermissionSetAfterRemovingAssignmentsSucceeds(t *testing.T) {
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

func TestPermissionSetCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		psName          string
		description     string
		sessionDuration string
		wantStatus      int
	}{
		{
			name:            "create and describe permission set",
			psName:          "ReadOnly",
			description:     "Read only access",
			sessionDuration: "PT2H",
			wantStatus:      http.StatusOK,
		},
		{
			name:       "create permission set default session duration",
			psName:     "AdminAccess",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			instanceArn := createInstance(t, h, "test-inst")

			rec := doRequest(t, h, "CreatePermissionSet", map[string]any{
				"InstanceArn":     instanceArn,
				"Name":            tt.psName,
				"Description":     tt.description,
				"SessionDuration": tt.sessionDuration,
			})
			require.Equal(t, tt.wantStatus, rec.Code)

			resp := parseResponse(t, rec)
			ps, ok := resp["PermissionSet"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.psName, ps["Name"])
			assert.NotEmpty(t, ps["PermissionSetArn"])

			psArn := ps["PermissionSetArn"].(string)

			descRec := doRequest(t, h, "DescribePermissionSet", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
			})
			require.Equal(t, http.StatusOK, descRec.Code)
			descResp := parseResponse(t, descRec)
			descPS, ok := descResp["PermissionSet"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.psName, descPS["Name"])
		})
	}
}

func TestPermissionSetConflict(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "duplicate permission set name",
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			instanceArn := createInstance(t, h, "inst")
			createPermissionSet(t, h, instanceArn, "DuplicateName")

			rec := doRequest(t, h, "CreatePermissionSet", map[string]any{
				"InstanceArn": instanceArn,
				"Name":        "DuplicateName",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestListPermissionSets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		psNames      []string
		wantMinCount int
	}{
		{
			name:         "empty list",
			psNames:      nil,
			wantMinCount: 0,
		},
		{
			name:         "lists created permission sets",
			psNames:      []string{"PS1", "PS2", "PS3"},
			wantMinCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			instanceArn := createInstance(t, h, "inst")

			for _, n := range tt.psNames {
				createPermissionSet(t, h, instanceArn, n)
			}

			rec := doRequest(t, h, "ListPermissionSets", map[string]any{"InstanceArn": instanceArn})
			require.Equal(t, http.StatusOK, rec.Code)
			resp := parseResponse(t, rec)
			psList, ok := resp["PermissionSets"].([]any)
			require.True(t, ok)
			assert.GreaterOrEqual(t, len(psList), tt.wantMinCount)
		})
	}
}

func TestDeletePermissionSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		useInvalid bool
		wantStatus int
	}{
		{
			name:       "delete existing permission set",
			useInvalid: false,
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete non-existing permission set",
			useInvalid: true,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			instanceArn := createInstance(t, h, "inst")

			psArn := "arn:aws:sso:::permissionSet/ssoins-bad/badid"
			if !tt.useInvalid {
				psArn = createPermissionSet(t, h, instanceArn, "ToDelete")
			}

			rec := doRequest(t, h, "DeletePermissionSet", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestUpdatePermissionSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		newDescription  string
		newSession      string
		wantDescription string
		wantSession     string
	}{
		{
			name:            "update description and session duration",
			newDescription:  "Updated description",
			newSession:      "PT4H",
			wantDescription: "Updated description",
			wantSession:     "PT4H",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			instanceArn := createInstance(t, h, "inst")
			psArn := createPermissionSet(t, h, instanceArn, "UpdateMe")

			rec := doRequest(t, h, "UpdatePermissionSet", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
				"Description":      tt.newDescription,
				"SessionDuration":  tt.newSession,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			descRec := doRequest(t, h, "DescribePermissionSet", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
			})
			require.Equal(t, http.StatusOK, descRec.Code)
			descResp := parseResponse(t, descRec)
			ps, ok := descResp["PermissionSet"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.wantDescription, ps["Description"])
			assert.Equal(t, tt.wantSession, ps["SessionDuration"])
		})
	}
}

func TestProvisionPermissionSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		targetType string
		wantStatus string
	}{
		{
			name:       "provision permission set succeeds",
			targetType: "ALL_PROVISIONED_ACCOUNTS",
			wantStatus: "SUCCEEDED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			instanceArn := createInstance(t, h, "inst")
			psArn := createPermissionSet(t, h, instanceArn, "PS")

			rec := doRequest(t, h, "ProvisionPermissionSet", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
				"TargetType":       tt.targetType,
			})
			require.Equal(t, http.StatusOK, rec.Code)
			resp := parseResponse(t, rec)
			status, ok := resp["PermissionSetProvisioningStatus"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.wantStatus, status["Status"])
			requestID := status["RequestId"].(string)
			assert.NotEmpty(t, requestID)

			descRec := doRequest(t, h, "DescribePermissionSetProvisioningStatus", map[string]any{
				"InstanceArn":                     instanceArn,
				"ProvisionPermissionSetRequestId": requestID,
			})
			require.Equal(t, http.StatusOK, descRec.Code)
			descResp := parseResponse(t, descRec)
			descStatus, ok := descResp["PermissionSetProvisioningStatus"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.wantStatus, descStatus["Status"])
		})
	}
}

func TestDescribePermissionSetProvisioningStatus_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		requestID  string
		wantStatus int
	}{
		{
			name:       "non_existent_request_id",
			requestID:  "nonexistent-request-id",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "inst")

			rec := doRequest(t, h, "DescribePermissionSetProvisioningStatus", map[string]any{
				"InstanceArn":                     instanceArn,
				"ProvisionPermissionSetRequestId": tt.requestID,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			resp := parseResponse(t, rec)
			assert.Equal(t, "ResourceNotFoundException", resp["__type"])
		})
	}
}

func TestPermissionSet_ErrorPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		op         string
		wantType   string
		wantStatus int
	}{
		{
			name: "create_permission_set_instance_not_found",
			op:   "CreatePermissionSet",
			body: map[string]any{
				"InstanceArn": "arn:aws:sso:::instance/ssoins-notfound",
				"Name":        "TestPS",
			},
			wantStatus: http.StatusNotFound,
			wantType:   "ResourceNotFoundException",
		},
		{
			name: "describe_permission_set_wrong_instance",
			op:   "DescribePermissionSet",
			body: map[string]any{
				"InstanceArn":      "arn:aws:sso:::instance/ssoins-notfound",
				"PermissionSetArn": "arn:aws:sso:::permissionSet/ssoins-x/badid",
			},
			wantStatus: http.StatusNotFound,
			wantType:   "ResourceNotFoundException",
		},
		{
			name: "update_permission_set_not_found",
			op:   "UpdatePermissionSet",
			body: map[string]any{
				"InstanceArn":      "arn:aws:sso:::instance/ssoins-notfound",
				"PermissionSetArn": "arn:aws:sso:::permissionSet/ssoins-x/badid",
				"Description":      "Updated",
			},
			wantStatus: http.StatusNotFound,
			wantType:   "ResourceNotFoundException",
		},
		{
			name: "attach_managed_policy_not_found",
			op:   "AttachManagedPolicyToPermissionSet",
			body: map[string]any{
				"InstanceArn":      "arn:aws:sso:::instance/ssoins-notfound",
				"PermissionSetArn": "arn:aws:sso:::permissionSet/ssoins-x/badid",
				"ManagedPolicyArn": "arn:aws:iam::aws:policy/ReadOnlyAccess",
			},
			wantStatus: http.StatusNotFound,
			wantType:   "ResourceNotFoundException",
		},
		{
			name: "detach_managed_policy_not_found",
			op:   "DetachManagedPolicyFromPermissionSet",
			body: map[string]any{
				"InstanceArn":      "arn:aws:sso:::instance/ssoins-notfound",
				"PermissionSetArn": "arn:aws:sso:::permissionSet/ssoins-x/badid",
				"ManagedPolicyArn": "arn:aws:iam::aws:policy/ReadOnlyAccess",
			},
			wantStatus: http.StatusNotFound,
			wantType:   "ResourceNotFoundException",
		},
		{
			name: "list_managed_policies_not_found",
			op:   "ListManagedPoliciesInPermissionSet",
			body: map[string]any{
				"InstanceArn":      "arn:aws:sso:::instance/ssoins-notfound",
				"PermissionSetArn": "arn:aws:sso:::permissionSet/ssoins-x/badid",
			},
			wantStatus: http.StatusNotFound,
			wantType:   "ResourceNotFoundException",
		},
		{
			name: "put_inline_policy_not_found",
			op:   "PutInlinePolicyToPermissionSet",
			body: map[string]any{
				"InstanceArn":      "arn:aws:sso:::instance/ssoins-notfound",
				"PermissionSetArn": "arn:aws:sso:::permissionSet/ssoins-x/badid",
				"InlinePolicy":     "{}",
			},
			wantStatus: http.StatusNotFound,
			wantType:   "ResourceNotFoundException",
		},
		{
			name: "get_inline_policy_not_found",
			op:   "GetInlinePolicyForPermissionSet",
			body: map[string]any{
				"InstanceArn":      "arn:aws:sso:::instance/ssoins-notfound",
				"PermissionSetArn": "arn:aws:sso:::permissionSet/ssoins-x/badid",
			},
			wantStatus: http.StatusNotFound,
			wantType:   "ResourceNotFoundException",
		},
		{
			name: "delete_inline_policy_not_found",
			op:   "DeleteInlinePolicyFromPermissionSet",
			body: map[string]any{
				"InstanceArn":      "arn:aws:sso:::instance/ssoins-notfound",
				"PermissionSetArn": "arn:aws:sso:::permissionSet/ssoins-x/badid",
			},
			wantStatus: http.StatusNotFound,
			wantType:   "ResourceNotFoundException",
		},
		{
			name: "provision_permission_set_instance_not_found",
			op:   "ProvisionPermissionSet",
			body: map[string]any{
				"InstanceArn":      "arn:aws:sso:::instance/ssoins-notfound",
				"PermissionSetArn": "arn:aws:sso:::permissionSet/ssoins-x/badid",
				"TargetType":       "ALL_PROVISIONED_ACCOUNTS",
			},
			wantStatus: http.StatusNotFound,
			wantType:   "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			resp := parseResponse(t, rec)
			assert.Equal(t, tt.wantType, resp["__type"])
		})
	}
}
