package cognitoidp_test

import (
	"encoding/json"
	"net/http"
	"strconv"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/cognitoidp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateUserPool_PasswordPolicy_Persisted(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	pool, err := b.CreateUserPoolWithOpts("pp-pool", cognitoidp.UserPoolOptions{
		PasswordPolicy: &cognitoidp.PasswordPolicy{
			MinimumLength:    12,
			RequireUppercase: true,
			RequireSymbols:   true,
		},
	})
	require.NoError(t, err)

	got, err := b.DescribeUserPool(pool.ID)
	require.NoError(t, err)
	require.NotNil(t, got.PasswordPolicy)
	assert.Equal(t, 12, got.PasswordPolicy.MinimumLength)
	assert.True(t, got.PasswordPolicy.RequireUppercase)
	assert.True(t, got.PasswordPolicy.RequireSymbols)
}

func TestHandler_CreateUserPool_WithPasswordPolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{
		"PoolName": "policy-pool",
		"Policies": map[string]any{
			"PasswordPolicy": map[string]any{
				"MinimumLength":    10,
				"RequireUppercase": true,
				"RequireNumbers":   true,
			},
		},
		"AutoVerifiedAttributes": []string{"email"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		UserPool struct {
			Policies *struct {
				PasswordPolicy *struct {
					MinimumLength  int  `json:"MinimumLength,omitempty"`
					RequireNumbers bool `json:"RequireNumbers,omitempty"`
				} `json:"PasswordPolicy"`
			} `json:"Policies"`
			AutoVerifiedAttributes []string `json:"AutoVerifiedAttributes,omitempty"`
		} `json:"UserPool"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.NotNil(t, resp.UserPool.Policies)
	require.NotNil(t, resp.UserPool.Policies.PasswordPolicy)
	assert.Equal(t, 10, resp.UserPool.Policies.PasswordPolicy.MinimumLength)
	assert.True(t, resp.UserPool.Policies.PasswordPolicy.RequireNumbers)
	assert.Equal(t, []string{"email"}, resp.UserPool.AutoVerifiedAttributes)
}

func TestHandler_DescribeUserPool_IncludesPolicy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{
		"PoolName": "describe-policy-pool",
		"Policies": map[string]any{
			"PasswordPolicy": map[string]any{"MinimumLength": 12},
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp struct {
		UserPool struct {
			ID string `json:"Id,omitempty"`
		} `json:"UserPool"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

	rec := doCognitoRequest(t, h, "DescribeUserPool", map[string]any{"UserPoolId": createResp.UserPool.ID})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		UserPool struct {
			Policies *struct {
				PasswordPolicy *struct {
					MinimumLength int `json:"MinimumLength,omitempty"`
				} `json:"PasswordPolicy"`
			} `json:"Policies"`
		} `json:"UserPool"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	require.NotNil(t, resp.UserPool.Policies)
	require.NotNil(t, resp.UserPool.Policies.PasswordPolicy)
	assert.Equal(t, 12, resp.UserPool.Policies.PasswordPolicy.MinimumLength)
}

func TestUserPool_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		poolName string
	}{
		{name: "basic_create_describe_delete", poolName: "test-pool-crud"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create.
			rec := doCognitoRequest(t, h, "CreateUserPool", map[string]any{"PoolName": tt.poolName})
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			var createResp struct {
				UserPool struct {
					ID   string `json:"Id"`
					Name string `json:"Name"`
				} `json:"UserPool"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
			poolID := createResp.UserPool.ID
			assert.Equal(t, tt.poolName, createResp.UserPool.Name)
			assert.NotEmpty(t, poolID)

			// Describe.
			descRec := doCognitoRequest(t, h, "DescribeUserPool", map[string]any{"UserPoolId": poolID})
			require.Equal(t, http.StatusOK, descRec.Code)

			var descResp struct {
				UserPool struct {
					ID string `json:"Id"`
				} `json:"UserPool"`
			}
			require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
			assert.Equal(t, poolID, descResp.UserPool.ID)

			// Delete.
			delRec := doCognitoRequest(t, h, "DeleteUserPool", map[string]any{"UserPoolId": poolID})
			require.Equal(t, http.StatusOK, delRec.Code)

			// Describe after delete returns error.
			afterRec := doCognitoRequest(t, h, "DescribeUserPool", map[string]any{"UserPoolId": poolID})
			assert.Equal(t, http.StatusBadRequest, afterRec.Code)
		})
	}
}

func TestInMemoryBackend_CreateUserPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget error
		name      string
		poolName  string
		wantErr   bool
	}{
		{
			name:     "success",
			poolName: "my-pool",
		},
		{
			name:      "duplicate_name",
			poolName:  "my-pool",
			wantErr:   true,
			errTarget: cognitoidp.ErrUserPoolAlreadyExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			if tt.name == "duplicate_name" {
				// Pre-create pool to trigger duplicate.
				_, setupErr := b.CreateUserPool("my-pool")
				require.NoError(t, setupErr)
			}

			pool, createErr := b.CreateUserPool(tt.poolName)

			if tt.wantErr {
				require.Error(t, createErr)
				assert.ErrorIs(t, createErr, tt.errTarget)

				return
			}

			require.NoError(t, createErr)
			assert.NotEmpty(t, pool.ID)
			assert.Equal(t, tt.poolName, pool.Name)
			assert.NotEmpty(t, pool.ARN)
		})
	}
}

func TestInMemoryBackend_DescribeUserPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget  error
		name       string
		userPoolID string
		wantErr    bool
	}{
		{
			name:    "success",
			wantErr: false,
		},
		{
			name:       "not_found",
			userPoolID: "us-east-1_nonexistent",
			wantErr:    true,
			errTarget:  cognitoidp.ErrUserPoolNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			pool, setupErr := b.CreateUserPool("test-pool")
			require.NoError(t, setupErr)

			poolID := pool.ID
			if tt.userPoolID != "" {
				poolID = tt.userPoolID
			}

			got, err := b.DescribeUserPool(poolID)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, poolID, got.ID)
		})
	}
}

func TestInMemoryBackend_ListUserPools(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		numPools int
	}{
		{
			name:     "empty",
			numPools: 0,
		},
		{
			name:     "multiple_pools",
			numPools: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			for i := range tt.numPools {
				_, err := b.CreateUserPool("pool-" + strconv.Itoa(i))
				require.NoError(t, err)
			}

			pools := b.ListUserPools()
			assert.Len(t, pools, tt.numPools)
		})
	}
}

func TestInMemoryBackend_GetUserPoolJWKS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget  error
		userPoolID func(b *cognitoidp.InMemoryBackend) string
		name       string
		wantErr    bool
	}{
		{
			name: "success",
			userPoolID: func(b *cognitoidp.InMemoryBackend) string {
				pool, _ := b.CreateUserPool("p")

				return pool.ID
			},
		},
		{
			name: "pool_not_found",
			userPoolID: func(_ *cognitoidp.InMemoryBackend) string {
				return "us-east-1_nonexistent"
			},
			wantErr:   true,
			errTarget: cognitoidp.ErrUserPoolNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			poolID := tt.userPoolID(b)

			jwks, err := b.GetUserPoolJWKS(poolID)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			require.NoError(t, err)
			require.Len(t, jwks.Keys, 1)
			assert.Equal(t, "RSA", jwks.Keys[0].Kty)
			assert.Equal(t, "RS256", jwks.Keys[0].Alg)
			assert.Equal(t, "sig", jwks.Keys[0].Use)
			assert.NotEmpty(t, jwks.Keys[0].N)
			assert.NotEmpty(t, jwks.Keys[0].E)
		})
	}
}

func TestInMemoryBackend_UpdateUserPool(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget        error
		setup            func(b *cognitoidp.InMemoryBackend) string
		name             string
		mfaConfiguration string
		wantMfa          string
		wantErr          bool
	}{
		{
			name: "update_mfa_to_optional",
			setup: func(b *cognitoidp.InMemoryBackend) string {
				p, _ := b.CreateUserPool("pool")

				return p.ID
			},
			mfaConfiguration: "OPTIONAL",
			wantMfa:          "OPTIONAL",
		},
		{
			name: "pool_not_found",
			setup: func(_ *cognitoidp.InMemoryBackend) string {
				return "us-east-1_missing"
			},
			mfaConfiguration: "ON",
			wantErr:          true,
			errTarget:        cognitoidp.ErrUserPoolNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			poolID := tt.setup(b)

			err := b.UpdateUserPool(poolID, tt.mfaConfiguration)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.errTarget)

				return
			}

			require.NoError(t, err)

			pool, descErr := b.DescribeUserPool(poolID)
			require.NoError(t, descErr)
			assert.Equal(t, tt.wantMfa, pool.MfaConfiguration)
		})
	}
}

func TestInMemoryBackend_GetPoolMetrics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		errTarget error
		name      string
		wantErr   bool
	}{
		{
			name: "success",
		},
		{
			name:      "pool_not_found",
			wantErr:   true,
			errTarget: cognitoidp.ErrUserPoolNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()

			if !tt.wantErr {
				p, _ := b.CreateUserPool("pool")
				_, _ = b.AdminCreateUser(p.ID, "user1", "Pass1!", nil)
				_, _ = b.CreateUserPoolClient(p.ID, "client1")
				_, _ = b.CreateGroup(p.ID, "grp", "", 0)

				m, err := b.GetPoolMetrics(p.ID)
				require.NoError(t, err)
				assert.Equal(t, 1, m.UserCount)
				assert.Equal(t, 1, m.ClientCount)
				assert.Equal(t, 1, m.GroupCount)

				return
			}

			_, err := b.GetPoolMetrics("us-east-1_missing")
			require.Error(t, err)
			assert.ErrorIs(t, err, tt.errTarget)
		})
	}
}

func TestGetUserPoolMfaConfig_DefaultsToOFF(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	poolID, _ := setupHandlerPoolAndClient(t, h, "mfa-default-pool")

	rec := doCognitoRequest(t, h, "GetUserPoolMfaConfig", map[string]any{
		"UserPoolId": poolID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		MfaConfiguration string `json:"MfaConfiguration,omitempty"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "OFF", out.MfaConfiguration)
}
