package appsync_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/appsync"
)

func TestCreateAPIKey_ExpiryDefaulted_WhenZero(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	// expires=0 → backend assigns default expiry (365 days from now).
	key, err := b.CreateAPIKey(api.APIID, "test key", 0)
	require.NoError(t, err)
	assert.Positive(t, key.Expires, "expiry should be defaulted to a future timestamp")
}

func TestCreateAPIKey_ExpiryClampedToMax(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	// expires far in the future → clamped to max (365 days).
	key, err := b.CreateAPIKey(api.APIID, "test key", 9999999999)
	require.NoError(t, err)
	assert.Positive(t, key.Expires, "expiry should be clamped to max")
}

func TestUpdateAPIKey_ExpiryRoundTrip(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	key, err := b.CreateAPIKey(api.APIID, "initial desc", 1000)
	require.NoError(t, err)

	updated, err := b.UpdateAPIKey(api.APIID, key.ID, "updated desc", 2000)
	require.NoError(t, err)
	assert.Equal(t, "updated desc", updated.Description)
	assert.Equal(t, int64(2000), updated.Expires)
}

func TestInMemoryBackend_CreateAPIKey_DefaultExpiry(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	key, err := b.CreateAPIKey(api.APIID, "test", 0)
	require.NoError(t, err)

	// Expires should be in the future.
	assert.Positive(t, key.Expires)
}

func TestInMemoryBackend_CreateAPIKey_Da2Prefix(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	key, err := b.CreateAPIKey(api.APIID, "test", 0)
	require.NoError(t, err)

	assert.Greater(t, len(key.ID), 4, "key ID should be longer than the prefix")
	assert.Equal(t, "da2-", key.ID[:4])
}

func TestInMemoryBackend_ListAndDeleteAPIKeys(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	key1, err := b.CreateAPIKey(api.APIID, "k1", 0)
	require.NoError(t, err)
	_, err = b.CreateAPIKey(api.APIID, "k2", 0)
	require.NoError(t, err)

	// List returns 2 keys.
	keys, err := b.ListAPIKeys(api.APIID)
	require.NoError(t, err)
	assert.Len(t, keys, 2)

	// Delete one.
	err = b.DeleteAPIKey(api.APIID, key1.ID)
	require.NoError(t, err)

	// List returns 1 key.
	keys, err = b.ListAPIKeys(api.APIID)
	require.NoError(t, err)
	assert.Len(t, keys, 1)

	// Delete non-existent returns error.
	err = b.DeleteAPIKey(api.APIID, "nonexistent")
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_UpdateAPIKey(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	key, err := b.CreateAPIKey(api.APIID, "original", 0)
	require.NoError(t, err)

	updated, err := b.UpdateAPIKey(api.APIID, key.ID, "updated", 0)
	require.NoError(t, err)
	assert.Equal(t, "updated", updated.Description)

	// Not found key returns error.
	_, err = b.UpdateAPIKey(api.APIID, "nonexistent", "x", 0)
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_ListAPIKeys_APINotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	_, err := b.ListAPIKeys("nonexistent")
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_DeleteAPIKey_APINotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	err := b.DeleteAPIKey("nonexistent", "key-id")
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_CreateAPIKey_MaxKeysLimit(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	// Create up to the limit (50).
	for i := range 50 {
		_, err = b.CreateAPIKey(api.APIID, fmt.Sprintf("key%d", i+1), 0)
		require.NoError(t, err, "key %d should succeed", i+1)
	}

	// 51st key should fail.
	_, err = b.CreateAPIKey(api.APIID, "key51", 0)
	require.Error(t, err)
}

func TestInMemoryBackend_CreateAPIKey_ExpiresCapToMaxDays(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	// Set expires to 2 years from now (> 365 days).
	farFuture := time.Now().AddDate(2, 0, 0).Unix()
	key, err := b.CreateAPIKey(api.APIID, "key1", farFuture)
	require.NoError(t, err)

	// Expires should be capped at 365 days.
	maxExpires := time.Now().AddDate(0, 0, 365).Unix()
	assert.LessOrEqual(t, key.Expires, maxExpires+60) // +60s tolerance
}

func TestInMemoryBackend_ListAPIKeys_FilterExpired(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	// Create a key that expired in the past.
	pastExpiry := time.Now().Add(-24 * time.Hour).Unix()
	_, err = b.CreateAPIKey(api.APIID, "expired", pastExpiry)
	require.NoError(t, err)

	// Create a valid key.
	_, err = b.CreateAPIKey(api.APIID, "valid", 0)
	require.NoError(t, err)

	keys, err := b.ListAPIKeys(api.APIID)
	require.NoError(t, err)
	// Only the non-expired key should be returned.
	assert.Len(t, keys, 1)
	assert.Equal(t, "valid", keys[0].Description)
}

func TestInMemoryBackend_UpdateAPIKey_ExpiryCapEnforced(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("MyAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	key, err := b.CreateAPIKey(api.APIID, "desc", 0)
	require.NoError(t, err)

	// Attempt to update with an expiry far in the future (10 years).
	farFuture := time.Now().AddDate(10, 0, 0).Unix()
	updated, err := b.UpdateAPIKey(api.APIID, key.ID, "", farFuture)
	require.NoError(t, err)

	// Should be capped at 365 days from now.
	maxAllowed := time.Now().AddDate(0, 0, 365).Unix()
	assert.LessOrEqual(t, updated.Expires, maxAllowed+60, "expiry should be capped at 365 days")
	assert.Greater(t, updated.Expires, maxAllowed-60, "expiry should be close to 365 days")
}

func TestInMemoryBackend_UpdateAPIKey_ValidExpiryUnchanged(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("MyAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	key, err := b.CreateAPIKey(api.APIID, "desc", 0)
	require.NoError(t, err)

	// Set a valid expiry within the cap.
	validExpiry := time.Now().AddDate(0, 0, 30).Unix()
	updated, err := b.UpdateAPIKey(api.APIID, key.ID, "", validExpiry)
	require.NoError(t, err)
	assert.Equal(t, validExpiry, updated.Expires, "valid expiry should be stored as-is")
}

func TestBackend_SweepExpiredAPIKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup         func(b *appsync.InMemoryBackend) string
		name          string
		wantEvicted   int
		wantKeyExists bool
	}{
		{
			name: "no_keys",
			setup: func(b *appsync.InMemoryBackend) string {
				api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
				require.NoError(t, err)

				return api.APIID
			},
			wantEvicted:   0,
			wantKeyExists: false,
		},
		{
			name: "expired_key_is_swept",
			setup: func(b *appsync.InMemoryBackend) string {
				api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
				require.NoError(t, err)
				// Create a key that expires in the past.
				_, err = b.CreateAPIKey(api.APIID, "expired", time.Now().Add(-1*time.Hour).Unix())
				require.NoError(t, err)

				return api.APIID
			},
			wantEvicted:   1,
			wantKeyExists: false,
		},
		{
			name: "valid_key_not_swept",
			setup: func(b *appsync.InMemoryBackend) string {
				api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
				require.NoError(t, err)
				// Create a key that expires far in the future.
				_, err = b.CreateAPIKey(api.APIID, "valid", time.Now().Add(24*time.Hour).Unix())
				require.NoError(t, err)

				return api.APIID
			},
			wantEvicted:   0,
			wantKeyExists: true,
		},
		{
			name: "mixed_keys",
			setup: func(b *appsync.InMemoryBackend) string {
				api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
				require.NoError(t, err)
				_, err = b.CreateAPIKey(api.APIID, "expired", time.Now().Add(-1*time.Hour).Unix())
				require.NoError(t, err)
				_, err = b.CreateAPIKey(api.APIID, "valid", time.Now().Add(24*time.Hour).Unix())
				require.NoError(t, err)

				return api.APIID
			},
			wantEvicted:   1,
			wantKeyExists: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			apiID := tt.setup(b)

			evicted := b.SweepExpiredAPIKeys()
			assert.Equal(t, tt.wantEvicted, evicted)

			keys, err := b.ListAPIKeys(apiID)
			require.NoError(t, err)

			if tt.wantKeyExists {
				assert.NotEmpty(t, keys)
			} else {
				// Either no keys or only non-expired ones remain.
				for _, k := range keys {
					assert.True(t, k.Expires == 0 || k.Expires > time.Now().Unix())
				}
			}
		})
	}
}

// TestListAPIKeys_Pagination verifies maxResults/nextToken on Listapikeys.
func TestListAPIKeys_Pagination(t *testing.T) {
	t.Parallel()

	h, _ := newTestHandler()

	rec := doRequest(t, h, http.MethodPost, "/v1/apis", map[string]any{
		"name":               "key-api",
		"authenticationType": "API_KEY",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var apiOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &apiOut))
	apiID := apiOut["graphqlApi"].(map[string]any)["apiId"].(string)

	for range 4 {
		rec = doRequest(t, h, http.MethodPost, fmt.Sprintf("/v1/apis/%s/apikeys", apiID), map[string]any{
			"description": "test key",
		})
		require.Equal(t, http.StatusCreated, rec.Code)
	}

	tests := []struct {
		name          string
		path          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			path:          fmt.Sprintf("/v1/apis/%s/apikeys", apiID),
			wantLen:       4,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			path:          fmt.Sprintf("/v1/apis/%s/apikeys?maxResults=2", apiID),
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			listRec := doRequest(t, h, http.MethodGet, tt.path, nil)
			require.Equal(t, http.StatusOK, listRec.Code)

			var out struct {
				NextToken string           `json:"nextToken"`
				APIKeys   []map[string]any `json:"apiKeys"`
			}
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &out))
			assert.Len(t, out.APIKeys, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out.NextToken)
			} else {
				assert.Empty(t, out.NextToken)
			}
		})
	}
}
