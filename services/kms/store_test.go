package kms_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

func TestExportCountHelpers(t *testing.T) {
	t.Parallel()

	b := newTestBackend()

	assert.Equal(t, 0, kms.KeyCount(b))
	assert.Equal(t, 0, kms.AliasCount(b))
	assert.Equal(t, 0, kms.GrantCount(b))
	assert.Equal(t, 0, kms.CustomKeyStoreCount(b))

	key, err := b.CreateKey(context.Background(), &kms.CreateKeyInput{})
	require.NoError(t, err)
	assert.Equal(t, 1, kms.KeyCount(b))

	err = b.CreateAlias(context.Background(), &kms.CreateAliasInput{
		AliasName:   "alias/test-count",
		TargetKeyID: key.KeyMetadata.KeyID,
	})
	require.NoError(t, err)
	assert.Equal(t, 1, kms.AliasCount(b))

	_, err = b.CreateGrant(context.Background(), &kms.CreateGrantInput{
		KeyID:            key.KeyMetadata.KeyID,
		GranteePrincipal: "arn:aws:iam::123456789012:user/alice",
		Operations:       []string{"Encrypt"},
	})
	require.NoError(t, err)
	assert.Equal(t, 1, kms.GrantCount(b))

	_, err = b.CreateCustomKeyStore(context.Background(), &kms.CreateCustomKeyStoreInput{
		CustomKeyStoreName: "test-store",
	})
	require.NoError(t, err)
	assert.Equal(t, 1, kms.CustomKeyStoreCount(b))
}

func TestSeedHelpers(t *testing.T) {
	t.Parallel()

	b := newTestBackend()
	assert.Equal(t, 0, kms.KeyCount(b))

	// AddKeyInternal
	testKey := &kms.Key{
		KeyID:    "test-key-id-1234-1234-1234-123456789012",
		Arn:      "arn:aws:kms:us-east-1:000000000000:key/test-key-id-1234-1234-1234-123456789012",
		KeyState: kms.KeyStateEnabled,
		KeyUsage: kms.KeyUsageEncryptDecrypt,
		KeySpec:  "SYMMETRIC_DEFAULT",
		Enabled:  true,
	}
	b.AddKeyInternal(testKey, nil)
	assert.Equal(t, 1, kms.KeyCount(b))

	// AddCustomKeyStoreInternal
	testStore := &kms.CustomKeyStore{
		CustomKeyStoreID:   "test-ks-id",
		CustomKeyStoreName: "seeded-store",
		ConnectionState:    kms.ConnectionStateDisconnected,
		CustomKeyStoreType: "AWS_CLOUDHSM",
	}
	b.AddCustomKeyStoreInternal(testStore)
	assert.Equal(t, 1, kms.CustomKeyStoreCount(b))

	// Verify describe finds it.
	out, err := b.DescribeCustomKeyStores(context.Background(), &kms.DescribeCustomKeyStoresInput{
		CustomKeyStoreID: "test-ks-id",
	})
	require.NoError(t, err)
	require.Len(t, out.CustomKeyStores, 1)
	assert.Equal(t, "seeded-store", out.CustomKeyStores[0].CustomKeyStoreName)
}

// TestKMSResolveKeyIDAlias verifies resolveKeyID works with alias input.
func TestKMSResolveKeyIDAlias(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()

	key, _ := backend.CreateKey(context.Background(), &kms.CreateKeyInput{})
	_ = backend.CreateAlias(context.Background(), &kms.CreateAliasInput{
		AliasName:   "alias/resolve-test",
		TargetKeyID: key.KeyMetadata.KeyID,
	})

	// Encrypt with alias - exercises resolveKeyID alias path
	out, err := backend.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:     "alias/resolve-test",
		Plaintext: []byte("hello"),
	})
	require.NoError(t, err)
	assert.Equal(t, key.KeyMetadata.Arn, out.KeyID)
}

// TestKMSParseMarkerBadToken verifies parseMarker handles invalid tokens gracefully.
func TestKMSParseMarkerBadToken(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	for range 3 {
		_, _ = backend.CreateKey(context.Background(), &kms.CreateKeyInput{})
	}

	// A bad marker should be treated as 0 (start from beginning)
	out, err := backend.ListKeys(context.Background(), &kms.ListKeysInput{Marker: "not-a-number"})
	require.NoError(t, err)
	assert.Len(t, out.Keys, 3)
}

// TestKMSResolveKeyIDARN verifies resolveKeyID handles ARN-format key IDs.
func TestKMSResolveKeyIDARN(t *testing.T) {
	t.Parallel()

	backend := kms.NewInMemoryBackend()
	key, _ := backend.CreateKey(context.Background(), &kms.CreateKeyInput{})

	// Use ARN format to encrypt
	keyArn := key.KeyMetadata.Arn
	out, err := backend.Encrypt(context.Background(), &kms.EncryptInput{
		KeyID:     keyArn,
		Plaintext: []byte("arn-test"),
	})
	require.NoError(t, err)
	assert.Equal(t, keyArn, out.KeyID)
}

// TestResolutionCacheInvalidation_DisableKey verifies that disabling a key
// evicts alias→keyID entries from the resolution cache so that subsequent
// lookups re-validate against the live store instead of serving a stale hit.
func TestResolutionCacheInvalidation_DisableKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		createAlias   bool
		wantCacheGone bool
	}{
		{
			name:          "alias_evicted_after_disable",
			createAlias:   true,
			wantCacheGone: true,
		},
		{
			name:          "no_alias_no_cache_entry",
			createAlias:   false,
			wantCacheGone: true, // nothing to evict, cache stays empty
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			ctx := context.Background()

			out, err := b.CreateKey(ctx, &kms.CreateKeyInput{Description: "test"})
			require.NoError(t, err)
			keyID := out.KeyMetadata.KeyID

			aliasName := "alias/disable-test"
			if tt.createAlias {
				require.NoError(t, b.CreateAlias(ctx, &kms.CreateAliasInput{
					AliasName:   aliasName,
					TargetKeyID: keyID,
				}))
				// Warm the cache by resolving the alias.
				_, err = b.DescribeKey(ctx, &kms.DescribeKeyInput{KeyID: aliasName})
				require.NoError(t, err)
				assert.True(
					t,
					kms.ResolutionCacheHas(b, aliasName),
					"cache should be warm before disable",
				)
			}

			require.NoError(t, b.DisableKey(ctx, &kms.DisableKeyInput{KeyID: keyID}))

			if tt.createAlias {
				assert.False(
					t,
					kms.ResolutionCacheHas(b, aliasName),
					"cache entry must be evicted after DisableKey",
				)
			}

			// Key still accessible by ID (state check returns correct error).
			_, err = b.Encrypt(ctx, &kms.EncryptInput{KeyID: keyID, Plaintext: []byte("hi")})
			assert.Error(t, err, "encrypt must fail on disabled key")
		})
	}
}

// TestResolutionCacheInvalidation_ScheduleKeyDeletion verifies that scheduling
// a key for deletion evicts alias→keyID entries from the resolution cache.
func TestResolutionCacheInvalidation_ScheduleKeyDeletion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		createAlias bool
	}{
		{
			name:        "alias_evicted_after_schedule_deletion",
			createAlias: true,
		},
		{
			name:        "no_alias_cache_unaffected",
			createAlias: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			ctx := context.Background()

			out, err := b.CreateKey(ctx, &kms.CreateKeyInput{Description: "test"})
			require.NoError(t, err)
			keyID := out.KeyMetadata.KeyID

			aliasName := "alias/sched-del-test"
			if tt.createAlias {
				require.NoError(t, b.CreateAlias(ctx, &kms.CreateAliasInput{
					AliasName:   aliasName,
					TargetKeyID: keyID,
				}))
				_, err = b.DescribeKey(ctx, &kms.DescribeKeyInput{KeyID: aliasName})
				require.NoError(t, err)
				assert.True(
					t,
					kms.ResolutionCacheHas(b, aliasName),
					"cache warm before schedule deletion",
				)
			}

			_, err = b.ScheduleKeyDeletion(ctx, &kms.ScheduleKeyDeletionInput{
				KeyID:               keyID,
				PendingWindowInDays: 7,
			})
			require.NoError(t, err)

			if tt.createAlias {
				assert.False(t, kms.ResolutionCacheHas(b, aliasName),
					"cache entry must be evicted after ScheduleKeyDeletion")
			}
		})
	}
}

// TestResolutionCacheTargetedEviction_AliasOps verifies that alias mutations
// use targeted single-entry eviction rather than full cache sweeps.
func TestResolutionCacheTargetedEviction_AliasOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		operation string // "update" | "delete"
	}{
		{name: "update_alias_evicts_only_that_entry", operation: "update"},
		{name: "delete_alias_evicts_only_that_entry", operation: "delete"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			ctx := context.Background()

			out1, err := b.CreateKey(ctx, &kms.CreateKeyInput{Description: "k1"})
			require.NoError(t, err)
			out2, err := b.CreateKey(ctx, &kms.CreateKeyInput{Description: "k2"})
			require.NoError(t, err)
			keyID1 := out1.KeyMetadata.KeyID
			keyID2 := out2.KeyMetadata.KeyID

			alias1 := "alias/target-evict"
			alias2 := "alias/bystander"

			require.NoError(
				t,
				b.CreateAlias(ctx, &kms.CreateAliasInput{AliasName: alias1, TargetKeyID: keyID1}),
			)
			require.NoError(
				t,
				b.CreateAlias(ctx, &kms.CreateAliasInput{AliasName: alias2, TargetKeyID: keyID2}),
			)

			// Warm the cache for both aliases.
			_, err = b.DescribeKey(ctx, &kms.DescribeKeyInput{KeyID: alias1})
			require.NoError(t, err)
			_, err = b.DescribeKey(ctx, &kms.DescribeKeyInput{KeyID: alias2})
			require.NoError(t, err)

			assert.True(t, kms.ResolutionCacheHas(b, alias1), "alias1 must be cached")
			assert.True(t, kms.ResolutionCacheHas(b, alias2), "alias2 must be cached before op")

			switch tt.operation {
			case "update":
				require.NoError(t, b.UpdateAlias(ctx, &kms.UpdateAliasInput{
					AliasName:   alias1,
					TargetKeyID: keyID2,
				}))
			case "delete":
				require.NoError(t, b.DeleteAlias(ctx, &kms.DeleteAliasInput{AliasName: alias1}))
			}

			assert.False(
				t,
				kms.ResolutionCacheHas(b, alias1),
				"mutated alias must be evicted from cache",
			)
			assert.True(
				t,
				kms.ResolutionCacheHas(b, alias2),
				"bystander alias must remain in cache",
			)
		})
	}
}

// TestClearResolutionCache_O1 verifies that clearResolutionCache (called by Reset)
// discards all cache entries without blocking on iteration.
func TestClearResolutionCache_O1(t *testing.T) {
	t.Parallel()

	b := kms.NewInMemoryBackend()
	ctx := context.Background()

	// Create several keys and aliases, warm the cache.
	aliases := []string{"alias/cache-a", "alias/cache-b", "alias/cache-c"}
	for _, a := range aliases {
		out, err := b.CreateKey(ctx, &kms.CreateKeyInput{Description: a})
		require.NoError(t, err)
		require.NoError(t, b.CreateAlias(ctx, &kms.CreateAliasInput{
			AliasName:   a,
			TargetKeyID: out.KeyMetadata.KeyID,
		}))
		_, err = b.DescribeKey(ctx, &kms.DescribeKeyInput{KeyID: a})
		require.NoError(t, err)
	}

	assert.Equal(t, len(aliases), kms.ResolutionCacheLen(b), "cache must be warm")

	b.Reset()

	assert.Equal(t, 0, kms.ResolutionCacheLen(b), "cache must be empty after Reset")
}
