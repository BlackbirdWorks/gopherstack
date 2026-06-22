package kms_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

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

// TestLastUsageLeak_PurgeKey verifies that the janitor's purgeKey removes the
// lastUsage entry for the deleted key, preventing unbounded map growth.
func TestLastUsageLeak_PurgeKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		performCryptoOp bool
		expectLastUsage bool // before purge
	}{
		{
			name:            "key_with_usage_entry_is_cleaned_up",
			performCryptoOp: true,
			expectLastUsage: true,
		},
		{
			name:            "key_without_usage_entry_no_error_on_purge",
			performCryptoOp: false,
			expectLastUsage: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			ctx := context.Background()

			out, err := b.CreateKey(ctx, &kms.CreateKeyInput{Description: "leak-test"})
			require.NoError(t, err)
			keyID := out.KeyMetadata.KeyID

			if tt.performCryptoOp {
				_, err = b.Encrypt(ctx, &kms.EncryptInput{
					KeyID:     keyID,
					Plaintext: []byte("hello"),
				})
				require.NoError(t, err)
				assert.True(t, kms.LastUsageExists(b, kms.MockRegion, keyID),
					"lastUsage must exist after crypto op")
			}

			// Schedule with past deletion date and sweep.
			_, err = b.ScheduleKeyDeletion(ctx, &kms.ScheduleKeyDeletionInput{
				KeyID:               keyID,
				PendingWindowInDays: 7,
			})
			require.NoError(t, err)
			b.SetDeletionDateForTest(keyID, time.Now().Add(-time.Second))

			j := kms.NewJanitor(b, time.Hour)
			j.SweepOnce(ctx)

			assert.Equal(t, 0, kms.KeyCount(b), "key must be purged")
			assert.False(t, kms.LastUsageExists(b, kms.MockRegion, keyID),
				"lastUsage entry must be deleted after purge")
		})
	}
}

// TestLastUsageLeak_PurgeKeyWithAlias verifies that purgeKey also evicts alias
// cache entries and removes lastUsage for each purged key.
func TestLastUsageLeak_PurgeKeyWithAlias(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		numKeys    int
		numAliases int
	}{
		{name: "single_key_single_alias", numKeys: 1, numAliases: 1},
		{name: "single_key_multiple_aliases", numKeys: 1, numAliases: 3},
		{name: "multiple_keys", numKeys: 3, numAliases: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			ctx := context.Background()

			keyIDs := make([]string, tt.numKeys)
			for i := range tt.numKeys {
				out, err := b.CreateKey(ctx, &kms.CreateKeyInput{Description: "k"})
				require.NoError(t, err)
				keyIDs[i] = out.KeyMetadata.KeyID

				// Do a crypto op to populate lastUsage.
				_, err = b.Encrypt(ctx, &kms.EncryptInput{
					KeyID:     keyIDs[i],
					Plaintext: []byte("x"),
				})
				require.NoError(t, err)

				// Create aliases up to numAliases for first key only.
				if i == 0 {
					for j := range tt.numAliases {
						aliasName := "alias/leak-test-" + string(rune('a'+j))
						require.NoError(t, b.CreateAlias(ctx, &kms.CreateAliasInput{
							AliasName:   aliasName,
							TargetKeyID: keyIDs[i],
						}))
						// Warm the cache.
						_, err = b.DescribeKey(ctx, &kms.DescribeKeyInput{KeyID: aliasName})
						require.NoError(t, err)
					}
				}
			}

			// Schedule all keys for deletion and set past deletion date.
			for _, kid := range keyIDs {
				_, err := b.ScheduleKeyDeletion(ctx, &kms.ScheduleKeyDeletionInput{
					KeyID:               kid,
					PendingWindowInDays: 7,
				})
				require.NoError(t, err)
				b.SetDeletionDateForTest(kid, time.Now().Add(-time.Second))
			}

			j := kms.NewJanitor(b, time.Hour)
			j.SweepOnce(ctx)

			assert.Equal(t, 0, kms.KeyCount(b), "all keys must be purged")
			assert.Equal(t, 0, kms.AliasCount(b), "all aliases must be purged")
			assert.Equal(t, 0, kms.ResolutionCacheLen(b), "cache must be empty after purge")

			for _, kid := range keyIDs {
				assert.False(t, kms.LastUsageExists(b, kms.MockRegion, kid),
					"lastUsage must not exist for purged key %s", kid)
			}
		})
	}
}
