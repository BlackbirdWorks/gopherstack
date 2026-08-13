package quicksight_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- IAM Policy Assignment CRUD round-trip, not-found, and duplicate errors ----

func TestQuickSight_IAMPolicyAssignmentCRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, http.MethodPost, nsPath("/iam-policy-assignments"), map[string]any{
		"AssignmentName":   "assign-a",
		"AssignmentStatus": "ENABLED",
		"PolicyArn":        "arn:aws:iam::000000000000:policy/p1",
		"Identities": map[string]any{
			"alice": []any{"alice"},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)
	createBody := parseBody(t, createRec)
	assert.Equal(t, "assign-a", createBody["AssignmentName"])
	assert.Equal(t, "ENABLED", createBody["AssignmentStatus"])
	assert.NotEmpty(t, createBody["AssignmentId"])

	// Duplicate create -> ResourceExistsException.
	dupRec := doRequest(t, h, http.MethodPost, nsPath("/iam-policy-assignments"), map[string]any{
		"AssignmentName":   "assign-a",
		"AssignmentStatus": "ENABLED",
	})
	assert.Equal(t, http.StatusConflict, dupRec.Code)
	assert.Equal(t, "ResourceExistsException", parseBody(t, dupRec)["Code"])

	// Missing AssignmentName -> validation error.
	invalidRec := doRequest(
		t,
		h,
		http.MethodPost,
		nsPath("/iam-policy-assignments"),
		map[string]any{},
	)
	assert.Equal(t, http.StatusBadRequest, invalidRec.Code)
	assert.Equal(t, "InvalidParameterValueException", parseBody(t, invalidRec)["Code"])

	// Invalid AssignmentStatus enum -> validation error.
	badStatusRec := doRequest(
		t,
		h,
		http.MethodPost,
		nsPath("/iam-policy-assignments"),
		map[string]any{
			"AssignmentName":   "assign-bad",
			"AssignmentStatus": "NOT_A_STATUS",
		},
	)
	assert.Equal(t, http.StatusBadRequest, badStatusRec.Code)

	// Create in a namespace that does not exist -> ResourceNotFoundException.
	missingNsRec := doRequest(
		t, h, http.MethodPost,
		fmt.Sprintf("/accounts/%s/namespaces/nope/iam-policy-assignments", testAccountID),
		map[string]any{"AssignmentName": "x", "AssignmentStatus": "ENABLED"},
	)
	assert.Equal(t, http.StatusNotFound, missingNsRec.Code)

	// Describe.
	describeRec := doRequest(t, h, http.MethodGet, nsPath("/iam-policy-assignments/assign-a"), nil)
	require.Equal(t, http.StatusOK, describeRec.Code)
	assignment, ok := parseBody(t, describeRec)["IAMPolicyAssignment"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "assign-a", assignment["AssignmentName"])
	assert.Equal(t, "arn:aws:iam::000000000000:policy/p1", assignment["PolicyArn"])
	// types.IAMPolicyAssignment (unlike Create/UpdateIAMPolicyAssignmentOutput) carries AwsAccountId.
	assert.Equal(t, testAccountID, assignment["AwsAccountId"])

	// Describe missing -> 404.
	missingRec := doRequest(t, h, http.MethodGet, nsPath("/iam-policy-assignments/notexist"), nil)
	assert.Equal(t, http.StatusNotFound, missingRec.Code)
	assert.Equal(t, "ResourceNotFoundException", parseBody(t, missingRec)["Code"])

	// Update.
	updateRec := doRequest(
		t,
		h,
		http.MethodPut,
		nsPath("/iam-policy-assignments/assign-a"),
		map[string]any{
			"AssignmentStatus": "DISABLED",
		},
	)
	require.Equal(t, http.StatusOK, updateRec.Code)
	updateBody := parseBody(t, updateRec)
	assert.Equal(t, "assign-a", updateBody["AssignmentName"])
	assert.Equal(t, "DISABLED", updateBody["AssignmentStatus"])
	// PolicyArn untouched by the partial update.
	assert.Equal(t, "arn:aws:iam::000000000000:policy/p1", updateBody["PolicyArn"])

	// Update missing -> 404.
	updateMissingRec := doRequest(
		t,
		h,
		http.MethodPut,
		nsPath("/iam-policy-assignments/notexist"),
		map[string]any{
			"AssignmentStatus": "ENABLED",
		},
	)
	assert.Equal(t, http.StatusNotFound, updateMissingRec.Code)

	// Delete (via the singular "namespace" route QuickSight uses for delete).
	deleteRec := doRequest(
		t,
		h,
		http.MethodDelete,
		fmt.Sprintf(
			"/accounts/%s/namespace/%s/iam-policy-assignments/assign-a",
			testAccountID,
			testNamespace,
		),
		nil,
	)
	require.Equal(t, http.StatusOK, deleteRec.Code)
	assert.Equal(t, "assign-a", parseBody(t, deleteRec)["AssignmentName"])

	// Delete missing -> 404.
	deleteMissingRec := doRequest(
		t,
		h,
		http.MethodDelete,
		fmt.Sprintf(
			"/accounts/%s/namespace/%s/iam-policy-assignments/assign-a",
			testAccountID,
			testNamespace,
		),
		nil,
	)
	assert.Equal(t, http.StatusNotFound, deleteMissingRec.Code)
}

// ---- ListIAMPolicyAssignments pagination ----

func TestQuickSight_ListIAMPolicyAssignments_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	for _, name := range []string{"a1", "a2", "a3", "a4", "a5"} {
		doRequest(t, h, http.MethodPost, nsPath("/iam-policy-assignments"), map[string]any{
			"AssignmentName":   name,
			"AssignmentStatus": "ENABLED",
		})
	}

	rec := doRequest(t, h, http.MethodGet, nsPath("/iam-policy-assignments?max-results=2"), nil)
	require.Equal(t, http.StatusOK, rec.Code)
	body := parseBody(t, rec)
	items, ok := body["IAMPolicyAssignments"].([]any)
	require.True(t, ok)
	assert.Len(t, items, 2)
	next, ok := body["NextToken"].(string)
	require.True(t, ok)
	require.NotEmpty(t, next)

	seen := map[string]bool{}
	for _, it := range items {
		m := it.(map[string]any)
		seen[m["AssignmentName"].(string)] = true
	}

	page2Path := nsPath(fmt.Sprintf("/iam-policy-assignments?max-results=2&next-token=%s", next))
	page2 := doRequest(t, h, http.MethodGet, page2Path, nil)
	require.Equal(t, http.StatusOK, page2.Code)
	items2 := parseBody(t, page2)["IAMPolicyAssignments"].([]any)
	assert.Len(t, items2, 2)
	for _, it := range items2 {
		m := it.(map[string]any)
		assert.False(t, seen[m["AssignmentName"].(string)], "page 2 must not repeat page 1 items")
	}
}

// TestQuickSight_ListIAMPolicyAssignments_SummaryScoping proves the list
// response no longer leaks AssignmentId/PolicyArn/Identities, none of which
// types.IAMPolicyAssignmentSummary (quicksight@v1.123.1 types.go:12309-12318)
// declares -- it declares only AssignmentName and AssignmentStatus. An
// SDK-driven client cannot prove this: its deserializer silently drops
// unrecognized members, so the over-wide response decodes "successfully"
// either way. Only a raw-body assertion distinguishes fixed from unfixed.
func TestQuickSight_ListIAMPolicyAssignments_SummaryScoping(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, nsPath("/iam-policy-assignments"), map[string]any{
		"AssignmentName":   "assign-scoped",
		"AssignmentStatus": "ENABLED",
		"PolicyArn":        "arn:aws:iam::000000000000:policy/must-not-leak",
		"Identities": map[string]any{
			"alice": []any{"alice"},
		},
	})

	rec := doRequest(t, h, http.MethodGet, nsPath("/iam-policy-assignments"), nil)
	require.Equal(t, http.StatusOK, rec.Code)
	items, ok := parseBody(t, rec)["IAMPolicyAssignments"].([]any)
	require.True(t, ok)
	require.Len(t, items, 1)

	m, ok := items[0].(map[string]any)
	require.True(t, ok)

	for _, forbidden := range []string{"AssignmentId", "PolicyArn", "Identities"} {
		assert.NotContainsf(t, m, forbidden, "%s is not a member of types.IAMPolicyAssignmentSummary", forbidden)
	}
	assert.Equal(t, "assign-scoped", m["AssignmentName"])
	assert.Equal(t, "ENABLED", m["AssignmentStatus"])
}

// ---- ListIAMPolicyAssignmentsForUser: only ENABLED assignments referencing the user ----

func TestQuickSight_ListIAMPolicyAssignmentsForUser(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, http.MethodPost, nsPath("/iam-policy-assignments"), map[string]any{
		"AssignmentName":   "for-alice",
		"AssignmentStatus": "ENABLED",
		"Identities": map[string]any{
			"users": []any{"alice"},
		},
	})
	doRequest(t, h, http.MethodPost, nsPath("/iam-policy-assignments"), map[string]any{
		"AssignmentName":   "for-bob-disabled",
		"AssignmentStatus": "DISABLED",
		"Identities": map[string]any{
			"users": []any{"alice"},
		},
	})
	doRequest(t, h, http.MethodPost, nsPath("/iam-policy-assignments"), map[string]any{
		"AssignmentName":   "for-bob",
		"AssignmentStatus": "ENABLED",
		"Identities": map[string]any{
			"users": []any{"bob"},
		},
	})

	rec := doRequest(t, h, http.MethodGet, nsPath("/users/alice/iam-policy-assignments"), nil)
	require.Equal(t, http.StatusOK, rec.Code)
	body := parseBody(t, rec)
	// Real ListIAMPolicyAssignmentsForUserOutput carries ActiveAssignments
	// ([]types.ActiveIAMPolicyAssignment: AssignmentName/PolicyArn only), not
	// IAMPolicyAssignments -- see aws-sdk-go-v2/service/quicksight's
	// deserializers.go ActiveAssignments case vs. ListIAMPolicyAssignments'
	// separate IAMPolicyAssignments case.
	items, ok := body["ActiveAssignments"].([]any)
	require.True(t, ok)
	require.Len(t, items, 1)
	m := items[0].(map[string]any)
	assert.Equal(t, "for-alice", m["AssignmentName"])
	assert.NotContains(t, m, "AssignmentStatus")
	assert.NotContains(t, m, "Identities")
	assert.NotContains(t, body, "IAMPolicyAssignments")
}

// ---- IAM Policy Assignment tests ---- //nolint:godot // existing issue.
func TestQuickSight_IAMPolicyAssignments(t *testing.T) { //nolint:paralleltest // existing issue.
	h := newTestHandler(t)

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		wantKey    string
		wantStatus int
	}{
		{
			name:       "create iam policy assignment",
			method:     http.MethodPost,
			path:       nsPath("/iam-policy-assignments/"),
			body:       map[string]any{"AssignmentName": "assign1", "AssignmentStatus": "ENABLED"},
			wantStatus: http.StatusOK,
			wantKey:    "AssignmentName",
		},
		{
			name:       "describe iam policy assignment",
			method:     http.MethodGet,
			path:       nsPath("/iam-policy-assignments/assign1"),
			wantStatus: http.StatusOK,
			wantKey:    "IAMPolicyAssignment",
		},
		{
			name:       "update iam policy assignment",
			method:     http.MethodPut,
			path:       nsPath("/iam-policy-assignments/assign1"),
			body:       map[string]any{"AssignmentStatus": "DISABLED"},
			wantStatus: http.StatusOK,
			wantKey:    "AssignmentName",
		},
		{
			name:       "list iam policy assignments",
			method:     http.MethodGet,
			path:       nsPath("/iam-policy-assignments"),
			wantStatus: http.StatusOK,
			wantKey:    "IAMPolicyAssignments",
		},
		{
			name:       "list iam policy assignments for user",
			method:     http.MethodGet,
			path:       nsPath("/v2/iam-policy-assignments"),
			wantStatus: http.StatusOK,
			wantKey:    "IAMPolicyAssignments",
		},
	}

	for _, tc := range tests { //nolint:paralleltest // existing issue.
		t.Run(tc.name, func(t *testing.T) {
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantStatus, rec.Code, "status")
			if tc.wantKey != "" {
				body := parseBody(t, rec)
				assert.Contains(t, body, tc.wantKey)
			}
		})
	}
}
