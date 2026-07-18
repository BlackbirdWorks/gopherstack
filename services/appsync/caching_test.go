package appsync_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/appsync"
)

func TestCreateAPICache_StatusDefaultsToAvailable(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	cache := &appsync.APICache{
		Type:               "T2_SMALL",
		APICachingBehavior: "FULL_REQUEST_CACHING",
		TTL:                300,
	}

	created, err := b.CreateAPICache(api.APIID, cache)
	require.NoError(t, err)
	assert.NotEmpty(t, created.Status)

	got, err := b.GetAPICache(api.APIID)
	require.NoError(t, err)
	assert.Equal(t, created.Status, got.Status)
}

func TestInMemoryBackend_CreateAPICache_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cache   appsync.APICache
		wantErr bool
	}{
		{
			name: "valid",
			cache: appsync.APICache{
				TTL:                60,
				Type:               "SMALL",
				APICachingBehavior: "FULL_REQUEST_CACHING",
			},
		},
		{
			name:    "ttl_zero_rejected",
			cache:   appsync.APICache{TTL: 0, Type: "SMALL", APICachingBehavior: "FULL_REQUEST_CACHING"},
			wantErr: true,
		},
		{
			name:    "ttl_negative_rejected",
			cache:   appsync.APICache{TTL: -1, Type: "SMALL", APICachingBehavior: "FULL_REQUEST_CACHING"},
			wantErr: true,
		},
		{
			name:    "missing_type_rejected",
			cache:   appsync.APICache{TTL: 60, APICachingBehavior: "FULL_REQUEST_CACHING"},
			wantErr: true,
		},
		{
			name:    "invalid_type_rejected",
			cache:   appsync.APICache{TTL: 60, Type: "BOGUS", APICachingBehavior: "FULL_REQUEST_CACHING"},
			wantErr: true,
		},
		{
			name:    "missing_caching_behavior_rejected",
			cache:   appsync.APICache{TTL: 60, Type: "SMALL"},
			wantErr: true,
		},
		{
			name:    "invalid_caching_behavior_rejected",
			cache:   appsync.APICache{TTL: 60, Type: "SMALL", APICachingBehavior: "BOGUS"},
			wantErr: true,
		},
		{
			name: "large_type_valid",
			cache: appsync.APICache{
				TTL:                3600,
				Type:               "LARGE_8X",
				APICachingBehavior: "PER_RESOLVER_CACHING",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
			require.NoError(t, err)

			_, cacheErr := b.CreateAPICache(api.APIID, &tt.cache)

			if tt.wantErr {
				require.Error(t, cacheErr)

				return
			}

			require.NoError(t, cacheErr)
		})
	}
}

func TestInMemoryBackend_GetAndDeleteAPICache(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	cache := &appsync.APICache{TTL: 60, Type: "SMALL", APICachingBehavior: "FULL_REQUEST_CACHING"}
	_, err = b.CreateAPICache(api.APIID, cache)
	require.NoError(t, err)

	// Get returns the cache.
	got, err := b.GetAPICache(api.APIID)
	require.NoError(t, err)
	assert.Equal(t, "SMALL", got.Type)

	// Delete the cache.
	err = b.DeleteAPICache(api.APIID)
	require.NoError(t, err)

	// Get after delete returns error.
	_, err = b.GetAPICache(api.APIID)
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_UpdateAPICache(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateAPICache(
		api.APIID,
		&appsync.APICache{TTL: 60, Type: "SMALL", APICachingBehavior: "FULL_REQUEST_CACHING"},
	)
	require.NoError(t, err)

	updated, err := b.UpdateAPICache(api.APIID, &appsync.APICache{TTL: 120, Type: "LARGE"})
	require.NoError(t, err)
	assert.Equal(t, "LARGE", updated.Type)
	assert.Equal(t, int64(120), updated.TTL)

	// Invalid type returns error.
	_, err = b.UpdateAPICache(api.APIID, &appsync.APICache{Type: "INVALID"})
	require.Error(t, err)

	// Not found API cache returns error.
	api2, err := b.CreateGraphqlAPI("TestAPI2", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)
	_, err = b.UpdateAPICache(api2.APIID, &appsync.APICache{TTL: 60})
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_FlushAPICache(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateAPICache(
		api.APIID,
		&appsync.APICache{TTL: 60, Type: "SMALL", APICachingBehavior: "FULL_REQUEST_CACHING"},
	)
	require.NoError(t, err)

	// Flush succeeds.
	err = b.FlushAPICache(api.APIID)
	require.NoError(t, err)

	// Flush without cache returns error.
	api2, err := b.CreateGraphqlAPI("TestAPI2", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)
	err = b.FlushAPICache(api2.APIID)
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_UpdateAPICache_CachingBehaviorValidation(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	api, err := b.CreateGraphqlAPI("TestAPI", appsync.AuthTypeAPIKey, false, "", "", nil, nil, nil)
	require.NoError(t, err)

	_, err = b.CreateAPICache(api.APIID, &appsync.APICache{
		TTL: 60, Type: "SMALL", APICachingBehavior: "FULL_REQUEST_CACHING",
	})
	require.NoError(t, err)

	// Invalid apiCachingBehavior returns validation error.
	_, err = b.UpdateAPICache(api.APIID, &appsync.APICache{APICachingBehavior: "INVALID"})
	require.Error(t, err)
}

func TestInMemoryBackend_GetAPICache_APINotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	_, err := b.GetAPICache("nonexistent")
	require.ErrorIs(t, err, awserr.ErrNotFound)
}

func TestInMemoryBackend_DeleteAPICache_APINotFound(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	err := b.DeleteAPICache("nonexistent")
	require.ErrorIs(t, err, awserr.ErrNotFound)
}
