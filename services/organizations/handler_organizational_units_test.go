package organizations_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandler_CreateOrganizationalUnit tests the HTTP handler for CreateOrganizationalUnit.
func TestHandler_CreateOrganizationalUnit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		wantOU     bool
	}{
		{
			name:       "creates_ou",
			wantStatus: http.StatusOK,
			wantOU:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})

			rootsRec := doRequest(t, h, "ListRoots", map[string]any{})
			require.Equal(t, http.StatusOK, rootsRec.Code)

			var rootsResp map[string]any
			require.NoError(t, json.NewDecoder(rootsRec.Body).Decode(&rootsResp))
			roots := rootsResp["Roots"].([]any)
			rootID := roots[0].(map[string]any)["Id"].(string)

			rec := doRequest(t, h, "CreateOrganizationalUnit", map[string]any{
				"ParentId": rootID,
				"Name":     "dev-ou",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantOU {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				ou, ok := resp["OrganizationalUnit"].(map[string]any)
				require.True(t, ok, "response should contain OrganizationalUnit")
				assert.NotEmpty(t, ou["Id"])
				assert.Equal(t, "dev-ou", ou["Name"])
			}
		})
	}
}

// TestHandler_ListParents tests the HTTP handler for ListParents.
func TestHandler_ListParents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantStatus  int
		wantParents bool
	}{
		{
			name:        "lists_parents_of_account",
			wantStatus:  http.StatusOK,
			wantParents: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})

			acctRec := doRequest(t, h, "CreateAccount", map[string]any{
				"AccountName": "test-account",
				"Email":       "test@example.com",
			})
			require.Equal(t, http.StatusOK, acctRec.Code)

			var acctResp map[string]any
			require.NoError(t, json.NewDecoder(acctRec.Body).Decode(&acctResp))
			status := acctResp["CreateAccountStatus"].(map[string]any)
			accountID := status["AccountId"].(string)

			rec := doRequest(t, h, "ListParents", map[string]any{"ChildId": accountID})
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantParents {
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				parents, ok := resp["Parents"].([]any)
				require.True(t, ok, "response should contain Parents")
				assert.NotEmpty(t, parents)
			}
		})
	}
}

// TestHandler_OUErrors tests OU handler error paths.
func TestHandler_OUErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		op         string
		wantStatus int
	}{
		{
			name:       "create_ou_missing_name",
			op:         "CreateOrganizationalUnit",
			body:       map[string]any{"ParentId": "r-root"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "describe_ou_not_found",
			op:         "DescribeOrganizationalUnit",
			body:       map[string]any{"OrganizationalUnitId": "ou-notexist"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "delete_ou_not_found",
			op:         "DeleteOrganizationalUnit",
			body:       map[string]any{"OrganizationalUnitId": "ou-notexist"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newHandlerWithOrg(t)

			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestOUDepth_ViaHandler tests OU depth limit enforcement through the handler.
func TestOUDepth_ViaHandler(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get root ID.
	rec = doRequest(t, h, "ListRoots", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var rootsResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&rootsResp))
	roots := rootsResp["Roots"].([]any)
	rootID := roots[0].(map[string]any)["Id"].(string)

	// Create 5 levels of OUs (should succeed).
	parentID := rootID

	for d := 1; d <= 5; d++ {
		rec = doRequest(t, h, "CreateOrganizationalUnit", map[string]any{
			"ParentId": parentID,
			"Name":     fmt.Sprintf("OU-Level-%d", d),
		})
		require.Equal(t, http.StatusOK, rec.Code, "depth %d should succeed", d)

		var ouResp map[string]any
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&ouResp))
		parentID = ouResp["OrganizationalUnit"].(map[string]any)["Id"].(string)
	}

	// Attempt to create level 6 (should fail).
	rec = doRequest(t, h, "CreateOrganizationalUnit", map[string]any{
		"ParentId": parentID,
		"Name":     "OU-Level-6",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "depth 6 should be rejected")
}

// TestOUNameUniqueness_ViaHandler tests OU name uniqueness via handler.
func TestOUNameUniqueness_ViaHandler(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateOrganization", map[string]any{"FeatureSet": "ALL"})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get root ID.
	rec = doRequest(t, h, "ListRoots", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var rootsResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&rootsResp))
	roots := rootsResp["Roots"].([]any)
	rootID := roots[0].(map[string]any)["Id"].(string)

	// Create OU.
	rec = doRequest(t, h, "CreateOrganizationalUnit", map[string]any{
		"ParentId": rootID,
		"Name":     "Engineering",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Attempt to create another OU with the same name.
	rec = doRequest(t, h, "CreateOrganizationalUnit", map[string]any{
		"ParentId": rootID,
		"Name":     "Engineering",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	// Create with a different name - should succeed.
	rec = doRequest(t, h, "CreateOrganizationalUnit", map[string]any{
		"ParentId": rootID,
		"Name":     "Finance",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestHandler_DescribeOrganizationalUnit tests the HTTP handler.
func TestHandler_DescribeOrganizationalUnit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "describes_ou",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, rootID := newHandlerWithOrg(t)

			ouRec := doRequest(t, h, "CreateOrganizationalUnit", map[string]any{
				"ParentId": rootID,
				"Name":     "test-ou",
			})
			require.Equal(t, http.StatusOK, ouRec.Code)

			var ouResp map[string]any
			require.NoError(t, json.NewDecoder(ouRec.Body).Decode(&ouResp))
			ou := ouResp["OrganizationalUnit"].(map[string]any)
			ouID := ou["Id"].(string)

			rec := doRequest(t, h, "DescribeOrganizationalUnit", map[string]any{"OrganizationalUnitId": ouID})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_DeleteOrganizationalUnit tests the HTTP handler.
func TestHandler_DeleteOrganizationalUnit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "deletes_ou",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, rootID := newHandlerWithOrg(t)

			ouRec := doRequest(t, h, "CreateOrganizationalUnit", map[string]any{
				"ParentId": rootID,
				"Name":     "test-ou",
			})
			require.Equal(t, http.StatusOK, ouRec.Code)

			var ouResp map[string]any
			require.NoError(t, json.NewDecoder(ouRec.Body).Decode(&ouResp))
			ou := ouResp["OrganizationalUnit"].(map[string]any)
			ouID := ou["Id"].(string)

			rec := doRequest(t, h, "DeleteOrganizationalUnit", map[string]any{"OrganizationalUnitId": ouID})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_UpdateOrganizationalUnit tests the HTTP handler.
func TestHandler_UpdateOrganizationalUnit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "updates_ou",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, rootID := newHandlerWithOrg(t)

			ouRec := doRequest(t, h, "CreateOrganizationalUnit", map[string]any{
				"ParentId": rootID,
				"Name":     "test-ou",
			})
			require.Equal(t, http.StatusOK, ouRec.Code)

			var ouResp map[string]any
			require.NoError(t, json.NewDecoder(ouRec.Body).Decode(&ouResp))
			ou := ouResp["OrganizationalUnit"].(map[string]any)
			ouID := ou["Id"].(string)

			rec := doRequest(t, h, "UpdateOrganizationalUnit", map[string]any{
				"OrganizationalUnitId": ouID,
				"Name":                 "renamed-ou",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_ListOrganizationalUnitsForParent tests the HTTP handler.
func TestHandler_ListOrganizationalUnitsForParent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "lists_ous",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, rootID := newHandlerWithOrg(t)

			doRequest(t, h, "CreateOrganizationalUnit", map[string]any{"ParentId": rootID, "Name": "test-ou"})

			rec := doRequest(t, h, "ListOrganizationalUnitsForParent", map[string]any{"ParentId": rootID})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_ListAccountsForParent tests the HTTP handler.
func TestHandler_ListAccountsForParent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "lists_accounts",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, rootID := newHandlerWithOrg(t)

			createAccountViaHandler(t, h, "test-account", "test@example.com")

			rec := doRequest(t, h, "ListAccountsForParent", map[string]any{"ParentId": rootID})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// TestHandler_ListChildren tests the HTTP handler.
func TestHandler_ListChildren(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		childType  string
		wantStatus int
	}{
		{
			name:       "lists_account_children",
			childType:  "ACCOUNT",
			wantStatus: http.StatusOK,
		},
		{
			name:       "lists_ou_children",
			childType:  "ORGANIZATIONAL_UNIT",
			wantStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, rootID := newHandlerWithOrg(t)

			rec := doRequest(t, h, "ListChildren", map[string]any{
				"ParentId":  rootID,
				"ChildType": tt.childType,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
