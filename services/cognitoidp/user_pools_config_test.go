package cognitoidp_test

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_UpdateUserPool_WithOpts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		setup    func(t *testing.T, h *cognitoidp.Handler) string
		name     string
		wantCode int
	}{
		{
			name:     "update_mfa_config",
			wantCode: http.StatusOK,
			setup: func(t *testing.T, h *cognitoidp.Handler) string {
				t.Helper()
				id, _ := setupHandlerPoolAndClient(t, h, "uu-pool")

				return id
			},
			body: map[string]any{
				"MfaConfiguration": "OPTIONAL",
			},
		},
		{
			name:     "update_password_policy",
			wantCode: http.StatusOK,
			setup: func(t *testing.T, h *cognitoidp.Handler) string {
				t.Helper()
				id, _ := setupHandlerPoolAndClient(t, h, "uu-pool2")

				return id
			},
			body: map[string]any{
				"Policies": map[string]any{
					"PasswordPolicy": map[string]any{
						"MinimumLength":    10,
						"RequireUppercase": true,
						"RequireLowercase": true,
						"RequireNumbers":   true,
						"RequireSymbols":   false,
					},
				},
			},
		},
		{
			name:     "pool_not_found",
			wantCode: http.StatusBadRequest,
			setup:    func(_ *testing.T, _ *cognitoidp.Handler) string { return "invalid-pool" },
			body:     map[string]any{"MfaConfiguration": "OFF"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID := tt.setup(t, h)

			body := make(map[string]any, len(tt.body)+1)
			maps.Copy(body, tt.body)

			body["UserPoolId"] = poolID

			rec := doCognitoRequest(t, h, "UpdateUserPool", body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_CreateUserPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         map[string]any
		name         string
		wantContains []string
		wantCode     int
	}{
		{
			name:         "success",
			body:         map[string]any{"PoolName": "my-test-pool"},
			wantCode:     http.StatusOK,
			wantContains: []string{"my-test-pool", "Arn", "Id"},
		},
		{
			name:     "duplicate_pool",
			body:     map[string]any{"PoolName": "duplicate-pool"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "duplicate_pool" {
				rec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "duplicate-pool"})
				assert.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doCognitoRequest(t, h, "CreateUserPool", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			for _, want := range tt.wantContains {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
}

func TestHandler_ListUserPools(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		numPools  int
		wantCode  int
		wantPools int
	}{
		{
			name:      "empty",
			numPools:  0,
			wantCode:  http.StatusOK,
			wantPools: 0,
		},
		{
			name:      "with_pools",
			numPools:  2,
			wantCode:  http.StatusOK,
			wantPools: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for i := range tt.numPools {
				rec := doCognitoRequest(
					t,
					h,
					"CreateUserPool",
					map[string]any{"PoolName": fmt.Sprintf("pool-%d", i)},
				)
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doCognitoRequest(t, h, "ListUserPools", map[string]any{})
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			pools, ok := resp["UserPools"].([]any)
			require.True(t, ok)
			assert.Len(t, pools, tt.wantPools)
		})
	}
}

func TestHandler_DescribeUserPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *cognitoidp.Handler) string
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(h *cognitoidp.Handler) string {
				rec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "p"})
				var resp map[string]map[string]any
				_ = json.Unmarshal(rec.Body.Bytes(), &resp)

				return resp["UserPool"]["Id"].(string)
			},
			wantCode: http.StatusOK,
		},
		{
			name: "not_found",
			setup: func(_ *cognitoidp.Handler) string {
				return "us-east-1_nonexistent"
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID := tt.setup(h)

			rec := doCognitoRequest(t, h, "DescribeUserPool", map[string]any{
				"UserPoolId": poolID,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestCognitoIDP_DeleteUserPool_CleansRefreshTokens(t *testing.T) {
	t.Parallel()

	b := cognitoidp.NewInMemoryBackend("000000000000", "us-east-1", "http://localhost:8000")

	pool, err := b.CreateUserPool("my-pool")
	require.NoError(t, err)

	client, err := b.CreateUserPoolClient(pool.ID, "my-client")
	require.NoError(t, err)

	u, err := b.SignUp(client.ClientID, "alice", "Password123!", nil)
	require.NoError(t, err)

	require.NoError(t, b.ConfirmSignUp(client.ClientID, "alice", u.ConfirmCode))

	tokens, err := b.InitiateAuth(client.ClientID, "USER_PASSWORD_AUTH", "alice", "Password123!")
	require.NoError(t, err)
	require.NotNil(t, tokens.Tokens)
	require.NotEmpty(t, tokens.Tokens.RefreshToken)

	// Deleting the pool should clean up the refresh token.
	require.NoError(t, b.DeleteUserPool(pool.ID))

	// Attempting to use the refresh token should fail now (token cleaned up).
	_, err = b.InitiateAuthRefreshToken(client.ClientID, tokens.Tokens.RefreshToken)
	require.Error(t, err, "refresh token should have been cleaned up on pool deletion")
}

func TestHandler_DeleteUserPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *cognitoidp.Handler) string
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(h *cognitoidp.Handler) string {
				rec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "del-pool"})
				var resp map[string]map[string]any
				_ = json.Unmarshal(rec.Body.Bytes(), &resp)

				return resp["UserPool"]["Id"].(string)
			},
			wantCode: http.StatusOK,
		},
		{
			name: "not_found",
			setup: func(_ *cognitoidp.Handler) string {
				return "us-east-1_nonexistent"
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID := tt.setup(h)

			rec := doCognitoRequest(t, h, "DeleteUserPool", map[string]any{"UserPoolId": poolID})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_GetUserPoolMfaConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(h *cognitoidp.Handler) string
		name     string
		wantCode int
	}{
		{
			name: "success",
			setup: func(h *cognitoidp.Handler) string {
				rec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "mfa-pool"})
				var resp map[string]map[string]any
				_ = json.Unmarshal(rec.Body.Bytes(), &resp)

				return resp["UserPool"]["Id"].(string)
			},
			wantCode: http.StatusOK,
		},
		{
			name: "pool_not_found",
			setup: func(_ *cognitoidp.Handler) string {
				return "us-east-1_nonexistent"
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			poolID := tt.setup(h)

			rec := doCognitoRequest(t, h, "GetUserPoolMfaConfig", map[string]any{"UserPoolId": poolID})
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantCode == http.StatusOK {
				assert.Contains(t, rec.Body.String(), "OFF")
			}
		})
	}
}

func TestSortedListUserPools(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"zebra-pool", "alpha-pool", "mango-pool"} {
		doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": name})
	}

	rec := doCognitoRequest(t, h, "ListUserPools", map[string]any{"MaxResults": 10})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	pools := resp["UserPools"].([]any)
	require.Len(t, pools, 3)
	assert.Equal(t, "alpha-pool", pools[0].(map[string]any)["Name"])
	assert.Equal(t, "mango-pool", pools[1].(map[string]any)["Name"])
	assert.Equal(t, "zebra-pool", pools[2].(map[string]any)["Name"])
}

func TestDescribeUserPool_IncludesSchemaAttributes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "schema-pool"})
	var poolResp map[string]any
	require.NoError(t, json.Unmarshal(poolRec.Body.Bytes(), &poolResp))
	poolID := poolResp["UserPool"].(map[string]any)["Id"].(string)

	doCognitoRequest(t, h, "AddCustomAttributes", map[string]any{
		"UserPoolId": poolID,
		"CustomAttributes": []map[string]any{
			{"Name": "custom:department", "AttributeDataType": "String"},
		},
	})

	rec := doCognitoRequest(t, h, "DescribeUserPool", map[string]any{"UserPoolId": poolID})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	schema := resp["UserPool"].(map[string]any)["SchemaAttributes"]
	assert.NotNil(t, schema)
	schemaList := schema.([]any)
	require.Len(t, schemaList, 1)
	assert.Equal(t, "custom:department", schemaList[0].(map[string]any)["Name"])
}

func TestListUserPools_NonNilWhenEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doCognitoRequest(t, h, "ListUserPools", map[string]any{"MaxResults": 10})
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	pools, ok := listResp["UserPools"].([]any)
	assert.True(t, ok, "UserPools should be an array, not nil")
	assert.Empty(t, pools)
}

// TestParityB_PasswordPolicyDefaults verifies default policy returned when none configured.
func TestPasswordPolicyDefaults(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name       string
		withPolicy bool
		wantMinLen int
	}

	tests := []testCase{
		{name: "no_policy_gets_defaults", withPolicy: false, wantMinLen: 8},
		{name: "explicit_policy_used", withPolicy: true, wantMinLen: 12},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{"PoolName": "pp-pool-" + tt.name}

			if tt.withPolicy {
				body["Policies"] = map[string]any{
					"PasswordPolicy": map[string]any{
						"MinimumLength": 12,
					},
				}
			}

			rec := doCognitoRequest(t, h, "CreateUserPool", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			pool := resp["UserPool"].(map[string]any)
			policies := pool["Policies"].(map[string]any)
			pp := policies["PasswordPolicy"].(map[string]any)

			assert.InDelta(t, float64(tt.wantMinLen), pp["MinimumLength"], 0, "MinimumLength mismatch")
		})
	}
}

// TestParityB_UserPoolLastModifiedDate verifies LastModifiedDate updates on UpdateUserPool.
func TestUserPoolLastModifiedDate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "lmd-pool"})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	pool := createResp["UserPool"].(map[string]any)
	poolID := pool["Id"].(string)
	createDate := pool["CreationDate"].(float64)
	lastModBefore := pool["LastModifiedDate"].(float64)

	// On creation, LastModifiedDate should equal CreationDate (no updates yet).
	assert.InDelta(t, createDate, lastModBefore, 0)

	// Update the pool.
	upRec := doCognitoRequest(t, h, "UpdateUserPool", map[string]any{
		"UserPoolId":       poolID,
		"MfaConfiguration": "OFF",
	})
	require.Equal(t, http.StatusOK, upRec.Code)

	// DescribeUserPool must return updated LastModifiedDate >= CreationDate.
	descRec := doCognitoRequest(t, h, "DescribeUserPool", map[string]any{"UserPoolId": poolID})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	lastModAfter := descResp["UserPool"].(map[string]any)["LastModifiedDate"].(float64)
	assert.GreaterOrEqual(t, lastModAfter, createDate, "LastModifiedDate must be >= CreationDate after update")
}

// TestParityB_UpdateUserPoolLambdaConfig verifies LambdaConfig round-trips through UpdateUserPool.
func TestUpdateUserPoolLambdaConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": "lambda-pool"})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	poolID := createResp["UserPool"].(map[string]any)["Id"].(string)

	lambdaConfig := map[string]any{
		"PreSignUp": "arn:aws:lambda:us-east-1:000000000000:function:pre-signup",
	}

	upRec := doCognitoRequest(t, h, "UpdateUserPool", map[string]any{
		"UserPoolId":   poolID,
		"LambdaConfig": lambdaConfig,
	})
	require.Equal(t, http.StatusOK, upRec.Code)

	descRec := doCognitoRequest(t, h, "DescribeUserPool", map[string]any{"UserPoolId": poolID})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	returnedPool := descResp["UserPool"].(map[string]any)
	returnedLambda := returnedPool["LambdaConfig"].(map[string]any)
	assert.Equal(t, "arn:aws:lambda:us-east-1:000000000000:function:pre-signup", returnedLambda["PreSignUp"])
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
