package cognitoidp_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
)

// TestAdminSetUserPassword_PolicyEnforced verifies that the (non-"Full")
// AdminSetUserPassword backend entry point — the one used by the JSON handler —
// rejects a password that violates the pool's password policy, matching
// ConfirmForgotPassword and AWS's InvalidPasswordException behavior.
func TestAdminSetUserPassword_PolicyEnforced(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPoolWithOpts("admin-set-pwd-policy", cognitoidp.UserPoolOptions{
		PasswordPolicy: &cognitoidp.PasswordPolicy{
			MinimumLength:    10,
			RequireUppercase: true,
			RequireNumbers:   true,
			RequireSymbols:   true,
		},
	})
	require.NoError(t, err)

	_, err = b.AdminCreateUser(pool.ID, "policy-user", "Temp1234!@#", nil)
	require.NoError(t, err)

	tests := []struct {
		name     string
		password string
		wantErr  bool
	}{
		{name: "too short", password: "short", wantErr: true},
		{name: "missing uppercase/number/symbol", password: "alllowercase", wantErr: true},
		{name: "valid", password: "LongPass1234!", wantErr: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			setErr := b.AdminSetUserPassword(pool.ID, "policy-user", tc.password, true)
			if tc.wantErr {
				require.Error(t, setErr)
			} else {
				require.NoError(t, setErr)
			}
		})
	}
}

// TestListUserPools_Pagination verifies that ListUserPools honors MaxResults,
// emits a NextToken, and walks pages without dropping or duplicating pools.
func TestListUserPools_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	for i := range 5 {
		doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": fmt.Sprintf("pool-%02d", i)})
	}

	type listResp struct {
		NextToken string           `json:"NextToken"`
		UserPools []map[string]any `json:"UserPools"`
	}

	seen := map[string]bool{}
	nextToken := ""
	pages := 0

	for {
		body := map[string]any{"MaxResults": 2}
		if nextToken != "" {
			body["NextToken"] = nextToken
		}

		rec := doCognitoRequest(t, h, "ListUserPools", body)
		require.Equal(t, http.StatusOK, rec.Code)

		var resp listResp
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.LessOrEqual(t, len(resp.UserPools), 2, "page must not exceed MaxResults")

		for _, p := range resp.UserPools {
			name := p["Name"].(string)
			assert.False(t, seen[name], "pool %s returned twice", name)
			seen[name] = true
		}

		pages++
		require.Less(t, pages, 10, "pagination did not terminate")

		nextToken = resp.NextToken
		if nextToken == "" {
			break
		}
	}

	assert.Len(t, seen, 5, "every pool must be returned exactly once across pages")
}

// TestListUserPools_MaxResultsBound verifies that an out-of-range MaxResults is
// rejected with InvalidParameterException.
func TestListUserPools_MaxResultsBound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		name       string
		maxResults int
		wantStatus int
	}{
		{name: "negative", maxResults: -1, wantStatus: http.StatusBadRequest},
		{name: "over cap", maxResults: 61, wantStatus: http.StatusBadRequest},
		{name: "at cap", maxResults: 60, wantStatus: http.StatusOK},
		{name: "min", maxResults: 1, wantStatus: http.StatusOK},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := doCognitoRequest(t, h, "ListUserPools", map[string]any{"MaxResults": tc.maxResults})
			assert.Equal(t, tc.wantStatus, rec.Code)
		})
	}
}

// TestListUsers_Pagination verifies ListUsers honors Limit and PaginationToken.
func TestListUsers_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "users-page-pool"})
	require.Equal(t, http.StatusOK, poolRec.Code)

	var poolResp struct {
		UserPool struct {
			ID string `json:"Id"`
		} `json:"UserPool"`
	}
	require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
	poolID := poolResp.UserPool.ID

	for i := range 5 {
		rec := doCognitoRequest(t, h, "AdminCreateUser", map[string]any{
			"UserPoolId": poolID,
			"Username":   fmt.Sprintf("user-%02d", i),
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	type listResp struct {
		PaginationToken string           `json:"PaginationToken"`
		Users           []map[string]any `json:"Users"`
	}

	seen := map[string]bool{}
	token := ""
	pages := 0

	for {
		body := map[string]any{"UserPoolId": poolID, "Limit": 2}
		if token != "" {
			body["PaginationToken"] = token
		}

		rec := doCognitoRequest(t, h, "ListUsers", body)
		require.Equal(t, http.StatusOK, rec.Code)

		var resp listResp
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.LessOrEqual(t, len(resp.Users), 2)

		for _, u := range resp.Users {
			name := u["Username"].(string)
			assert.False(t, seen[name], "user %s returned twice", name)
			seen[name] = true
		}

		pages++
		require.Less(t, pages, 10)

		token = resp.PaginationToken
		if token == "" {
			break
		}
	}

	assert.Len(t, seen, 5)
}
