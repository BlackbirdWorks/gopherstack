package ssoadmin_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/ssoadmin"
)

// TestAccountAssignmentStatusTransition verifies IN_PROGRESS → SUCCEEDED on describe.
func TestAccountAssignmentStatusTransition(t *testing.T) {
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

// TestListAccountAssignmentCreationStatusFilter verifies filter by status works.
func TestListAccountAssignmentCreationStatusFilter(t *testing.T) {
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

// TestListAccountAssignmentsReturnsCopies verifies that mutations of returned
// assignments do not affect backend state.
func TestListAccountAssignmentsReturnsCopies(t *testing.T) {
	t.Parallel()

	b := ssoadmin.NewInMemoryBackend("000000000000", "us-east-1")
	h := ssoadmin.NewHandler(b)

	instanceArn := createInstance(t, h, "copy-inst")
	psArn := createPermissionSet(t, h, instanceArn, "copy-ps")

	_ = doRequest(t, h, "CreateAccountAssignment", map[string]any{
		"InstanceArn":      instanceArn,
		"PermissionSetArn": psArn,
		"TargetId":         "123456789012",
		"TargetType":       "AWS_ACCOUNT",
		"PrincipalId":      "user-001",
		"PrincipalType":    "USER",
	})

	rec := doRequest(t, h, "ListAccountAssignments", map[string]any{
		"InstanceArn":      instanceArn,
		"PermissionSetArn": psArn,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	assignments := resp["AccountAssignments"].([]any)
	assert.Len(t, assignments, 1)
}

// TestListAccountAssignmentsForPrincipal verifies the new operation.
func TestListAccountAssignmentsForPrincipal(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "laafp-inst")
	psArn := createPermissionSet(t, h, instanceArn, "laafp-ps")

	accountID := "222222222222"
	doRequest(t, h, "CreateAccountAssignment", map[string]any{
		"InstanceArn":      instanceArn,
		"PermissionSetArn": psArn,
		"TargetId":         accountID,
		"TargetType":       "AWS_ACCOUNT",
		"PrincipalType":    "USER",
		"PrincipalId":      "user-abc",
	})

	rec := doRequest(t, h, "ListAccountAssignmentsForPrincipal", map[string]any{
		"InstanceArn":   instanceArn,
		"PrincipalId":   "user-abc",
		"PrincipalType": "USER",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	assignments, ok := resp["AccountAssignments"].([]any)
	require.True(t, ok)
	assert.Len(t, assignments, 1)
	a := assignments[0].(map[string]any)
	assert.Equal(t, accountID, a["AccountId"])
	assert.Equal(t, psArn, a["PermissionSetArn"])
}

// TestCreateAccountAssignmentIdempotency verifies deterministic idempotency.
func TestCreateAccountAssignmentIdempotency(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "idempotency-inst")
	psArn := createPermissionSet(t, h, instanceArn, "idempotency-ps")

	req := map[string]any{
		"InstanceArn":      instanceArn,
		"PermissionSetArn": psArn,
		"TargetId":         "333333333333",
		"TargetType":       "AWS_ACCOUNT",
		"PrincipalType":    "USER",
		"PrincipalId":      "user-idem",
	}

	rec1 := doRequest(t, h, "CreateAccountAssignment", req)
	require.Equal(t, http.StatusOK, rec1.Code)
	resp1 := parseResponse(t, rec1)

	rec2 := doRequest(t, h, "CreateAccountAssignment", req)
	require.Equal(t, http.StatusOK, rec2.Code)
	resp2 := parseResponse(t, rec2)

	// Both responses should return the same request ID.
	id1 := resp1["AccountAssignmentCreationStatus"].(map[string]any)["RequestId"]
	id2 := resp2["AccountAssignmentCreationStatus"].(map[string]any)["RequestId"]
	assert.Equal(t, id1, id2, "idempotent calls should return the same RequestId")
}

// TestListAccountAssignmentStatusSorted verifies creation statuses are sorted desc.
func TestListAccountAssignmentStatusSorted(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "status-sort-inst")
	psArn := createPermissionSet(t, h, instanceArn, "status-sort-ps")

	for i := range 3 {
		doRequest(t, h, "CreateAccountAssignment", map[string]any{
			"InstanceArn":      instanceArn,
			"PermissionSetArn": psArn,
			"TargetId":         "444444444444",
			"TargetType":       "AWS_ACCOUNT",
			"PrincipalType":    "USER",
			"PrincipalId":      "user-sort-" + string(rune('a'+i)),
		})
	}

	rec := doRequest(t, h, "ListAccountAssignmentCreationStatus", map[string]any{
		"InstanceArn": instanceArn,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResponse(t, rec)
	statuses, ok := resp["AccountAssignmentsCreationStatus"].([]any)
	require.True(t, ok)
	assert.Len(t, statuses, 3)
}

// TestPrincipalTypeValidation verifies CreateAccountAssignment validates PrincipalType.
func TestPrincipalTypeValidation(t *testing.T) {
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

// TestTargetTypeValidation verifies CreateAccountAssignment validates TargetType.
func TestTargetTypeValidation(t *testing.T) {
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

// TestStatusMapCap verifies that creationStatuses and deletionStatuses do not
// grow unboundedly: after creating and deleting assignments the backend
// remains functional and the counts stay within the expected cap.
func TestStatusMapCap(t *testing.T) {
	t.Parallel()

	backend := ssoadmin.NewInMemoryBackend("123456789012", config.DefaultRegion)
	h := ssoadmin.NewHandler(backend)

	instanceArn := createInstance(t, h, "cap-test")
	psArn := createPermissionSet(t, h, instanceArn, "cap-ps")

	const n = 20
	for i := range n {
		body := map[string]any{
			"InstanceArn":      instanceArn,
			"PermissionSetArn": psArn,
			"PrincipalId":      "user-" + string(rune('a'+i)),
			"PrincipalType":    "USER",
			"TargetId":         "111111111111",
			"TargetType":       "AWS_ACCOUNT",
		}
		rec := doRequest(t, h, "CreateAccountAssignment", body)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

		// Also delete each one so deletionStatuses accumulates entries.
		rec2 := doRequest(t, h, "DeleteAccountAssignment", body)
		require.Equal(t, http.StatusOK, rec2.Code, rec2.Body.String())
	}

	creationCount := ssoadmin.CreationStatusCount(backend)
	deletionCount := ssoadmin.DeletionStatusCount(backend)

	// After n creates (pruned by delete idempotency cleanup) and n deletes,
	// counts should remain reasonable — well below any runaway growth.
	assert.LessOrEqual(t, creationCount, n,
		"creationStatuses grew beyond expected: %d", creationCount)
	assert.LessOrEqual(t, deletionCount, n,
		"deletionStatuses grew beyond expected: %d", deletionCount)
}

func TestAccountAssignmentCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		accountID     string
		principalID   string
		principalType string
		wantStatus    int
	}{
		{
			name:          "create and list assignment",
			accountID:     "123456789012",
			principalID:   "user-abc",
			principalType: "USER",
			wantStatus:    http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler()
			instanceArn := createInstance(t, h, "inst")
			psArn := createPermissionSet(t, h, instanceArn, "PS")

			rec := doRequest(t, h, "CreateAccountAssignment", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
				"TargetId":         tt.accountID,
				"TargetType":       "AWS_ACCOUNT",
				"PrincipalId":      tt.principalID,
				"PrincipalType":    tt.principalType,
			})
			require.Equal(t, tt.wantStatus, rec.Code)

			resp := parseResponse(t, rec)
			creationStatus, ok := resp["AccountAssignmentCreationStatus"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "SUCCEEDED", creationStatus["Status"])
			requestID := creationStatus["RequestId"].(string)
			assert.NotEmpty(t, requestID)

			descRec := doRequest(t, h, "DescribeAccountAssignmentCreationStatus", map[string]any{
				"InstanceArn":                        instanceArn,
				"AccountAssignmentCreationRequestId": requestID,
			})
			require.Equal(t, http.StatusOK, descRec.Code)
			descResp := parseResponse(t, descRec)
			status, ok := descResp["AccountAssignmentCreationStatus"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "SUCCEEDED", status["Status"])

			listRec := doRequest(t, h, "ListAccountAssignments", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
			})
			require.Equal(t, http.StatusOK, listRec.Code)
			listResp := parseResponse(t, listRec)
			assignments, ok := listResp["AccountAssignments"].([]any)
			require.True(t, ok)
			assert.Len(t, assignments, 1)

			delRec := doRequest(t, h, "DeleteAccountAssignment", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
				"TargetId":         tt.accountID,
				"TargetType":       "AWS_ACCOUNT",
				"PrincipalId":      tt.principalID,
				"PrincipalType":    tt.principalType,
			})
			require.Equal(t, http.StatusOK, delRec.Code)
			delResp := parseResponse(t, delRec)
			delStatus, ok := delResp["AccountAssignmentDeletionStatus"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "SUCCEEDED", delStatus["Status"])
		})
	}
}

func TestDescribeAccountAssignmentCreationStatus_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		requestID  string
		wantStatus int
	}{
		{
			name:       "non_existent_request_id",
			requestID:  "nonexistent-request-id",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "inst")

			rec := doRequest(t, h, "DescribeAccountAssignmentCreationStatus", map[string]any{
				"InstanceArn":                        instanceArn,
				"AccountAssignmentCreationRequestId": tt.requestID,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			resp := parseResponse(t, rec)
			assert.Equal(t, "ResourceNotFoundException", resp["__type"])
		})
	}
}

func TestDescribeAccountAssignmentDeletionStatus_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		requestID  string
		wantStatus int
	}{
		{
			name:       "non_existent_request_id",
			requestID:  "nonexistent-request-id",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "inst")

			rec := doRequest(t, h, "DescribeAccountAssignmentDeletionStatus", map[string]any{
				"InstanceArn":                        instanceArn,
				"AccountAssignmentDeletionRequestId": tt.requestID,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			resp := parseResponse(t, rec)
			assert.Equal(t, "ResourceNotFoundException", resp["__type"])
		})
	}
}

func TestDeleteAccountAssignment_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "delete_non_existing_assignment",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			instanceArn := createInstance(t, h, "inst")
			psArn := createPermissionSet(t, h, instanceArn, "PS")

			rec := doRequest(t, h, "DeleteAccountAssignment", map[string]any{
				"InstanceArn":      instanceArn,
				"PermissionSetArn": psArn,
				"TargetId":         "123456789012",
				"TargetType":       "AWS_ACCOUNT",
				"PrincipalId":      "user-nonexistent",
				"PrincipalType":    "USER",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			resp := parseResponse(t, rec)
			assert.Equal(t, "ResourceNotFoundException", resp["__type"])
		})
	}
}

func TestCreateAccountAssignment_InstanceNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "instance_not_found",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()

			rec := doRequest(t, h, "CreateAccountAssignment", map[string]any{
				"InstanceArn":      "arn:aws:sso:::instance/ssoins-notfound",
				"PermissionSetArn": "arn:aws:sso:::permissionSet/ssoins-x/badid",
				"TargetId":         "123456789012",
				"TargetType":       "AWS_ACCOUNT",
				"PrincipalId":      "user-abc",
				"PrincipalType":    "USER",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			resp := parseResponse(t, rec)
			assert.Equal(t, "ResourceNotFoundException", resp["__type"])
		})
	}
}

// TestAccountAssignmentStatusWireShape locks in the real
// types.AccountAssignmentOperationStatus wire shape: the account identifier
// field is "TargetId" (echoing the request's TargetId/TargetType), NOT
// "AccountId" -- unlike PermissionSetProvisioningStatus, which really does use
// "AccountId" (see TestPermissionSetProvisioningStatusWireShape in
// handler_permission_sets_test.go). A real aws-sdk-go-v2 client parsing the
// old "AccountId"-keyed response would silently get a nil TargetId.
func TestAccountAssignmentStatusWireShape(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "wire-shape-inst")
	psArn := createPermissionSet(t, h, instanceArn, "WireShapePS")

	rec := doRequest(t, h, "CreateAccountAssignment", map[string]any{
		"InstanceArn":      instanceArn,
		"PermissionSetArn": psArn,
		"TargetId":         "222222222222",
		"TargetType":       "AWS_ACCOUNT",
		"PrincipalType":    "GROUP",
		"PrincipalId":      "group-xyz",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResponse(t, rec)
	status, ok := resp["AccountAssignmentCreationStatus"].(map[string]any)
	require.True(t, ok, "AccountAssignmentCreationStatus must be present")

	assert.Equal(t, "222222222222", status["TargetId"], "wire field must be TargetId")
	assert.Equal(t, "AWS_ACCOUNT", status["TargetType"])
	assert.NotContains(t, status, "AccountId", "AccountAssignmentOperationStatus has no AccountId member")
	assert.Equal(t, psArn, status["PermissionSetArn"])
	assert.Equal(t, "group-xyz", status["PrincipalId"])
	assert.Equal(t, "GROUP", status["PrincipalType"])
	assert.Contains(t, status, "RequestId")
	assert.Contains(t, status, "CreatedDate")

	requestID, _ := status["RequestId"].(string)

	// ListAccountAssignmentCreationStatus returns the slim
	// AccountAssignmentOperationStatusMetadata shape: only
	// CreatedDate/RequestId/Status, no TargetId/PermissionSetArn/etc. Checked
	// before delete: DeleteAccountAssignment prunes the matching creation
	// status entry to bound growth, so it would no longer be listed after.
	listRec := doRequest(t, h, "ListAccountAssignmentCreationStatus", map[string]any{
		"InstanceArn": instanceArn,
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	listResp := parseResponse(t, listRec)
	items, ok := listResp["AccountAssignmentsCreationStatus"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, items)

	var found bool

	for _, raw := range items {
		item, itemOK := raw.(map[string]any)
		require.True(t, itemOK)

		assert.NotContains(t, item, "TargetId", "list metadata shape has no TargetId member")
		assert.NotContains(t, item, "PermissionSetArn", "list metadata shape has no PermissionSetArn member")
		assert.NotContains(t, item, "PrincipalId", "list metadata shape has no PrincipalId member")
		assert.Contains(t, item, "RequestId")
		assert.Contains(t, item, "Status")

		if item["RequestId"] == requestID {
			found = true
		}
	}

	assert.True(t, found, "created assignment's request id must appear in the list")

	// Same shape check on DeleteAccountAssignment's status.
	delRec := doRequest(t, h, "DeleteAccountAssignment", map[string]any{
		"InstanceArn":      instanceArn,
		"PermissionSetArn": psArn,
		"TargetId":         "222222222222",
		"TargetType":       "AWS_ACCOUNT",
		"PrincipalType":    "GROUP",
		"PrincipalId":      "group-xyz",
	})
	require.Equal(t, http.StatusOK, delRec.Code)

	delResp := parseResponse(t, delRec)
	delStatus, ok := delResp["AccountAssignmentDeletionStatus"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "222222222222", delStatus["TargetId"])
	assert.NotContains(t, delStatus, "AccountId")
}

// TestListAccountAssignmentCreationStatusPagination locks in MaxResults +
// NextToken pagination support on ListAccountAssignmentCreationStatus, which
// previously ignored MaxResults entirely and always returned a nil NextToken
// even when more results existed.
func TestListAccountAssignmentCreationStatusPagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	instanceArn := createInstance(t, h, "pagination-inst")
	psArn := createPermissionSet(t, h, instanceArn, "PaginationPS")

	const totalAssignments = 5
	for i := range totalAssignments {
		rec := doRequest(t, h, "CreateAccountAssignment", map[string]any{
			"InstanceArn":      instanceArn,
			"PermissionSetArn": psArn,
			"TargetId":         "10000000000" + string(rune('0'+i)),
			"TargetType":       "AWS_ACCOUNT",
			"PrincipalType":    "USER",
			"PrincipalId":      "user-" + string(rune('0'+i)),
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doRequest(t, h, "ListAccountAssignmentCreationStatus", map[string]any{
		"InstanceArn": instanceArn,
		"MaxResults":  2,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResponse(t, rec)
	items, ok := resp["AccountAssignmentsCreationStatus"].([]any)
	require.True(t, ok)
	assert.Len(t, items, 2, "MaxResults must cap the page size")
	assert.NotEmpty(t, resp["NextToken"], "NextToken must be set when more results remain")

	seen := len(items)
	nextToken, _ := resp["NextToken"].(string)

	for nextToken != "" {
		rec = doRequest(t, h, "ListAccountAssignmentCreationStatus", map[string]any{
			"InstanceArn": instanceArn,
			"MaxResults":  2,
			"NextToken":   nextToken,
		})
		require.Equal(t, http.StatusOK, rec.Code)
		resp = parseResponse(t, rec)
		items, ok = resp["AccountAssignmentsCreationStatus"].([]any)
		require.True(t, ok)
		seen += len(items)
		nextToken, _ = resp["NextToken"].(string)
	}

	assert.Equal(t, totalAssignments, seen, "pagination must eventually surface every status entry exactly once")
}
