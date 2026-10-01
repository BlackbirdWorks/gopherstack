package apigateway

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func seedUsageState(b *InMemoryBackend, planID, keyID string) {
	b.mu.Lock("seed")
	defer b.mu.Unlock()

	mapKey := usageKey(planID, keyID)
	b.usage.quota[mapKey] = &quotaCounter{used: 1}
	b.usage.buckets[mapKey] = &tokenBucket{}
}

func TestUsageTracker_ClearedOnDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		del  func(b *InMemoryBackend, planID, keyID string) error
		name string
	}{
		{
			name: "delete_usage_plan",
			del:  func(b *InMemoryBackend, planID, _ string) error { return b.DeleteUsagePlan(planID) },
		},
		{
			name: "delete_usage_plan_key",
			del: func(b *InMemoryBackend, planID, keyID string) error {
				return b.DeleteUsagePlanKey(planID, keyID)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := NewInMemoryBackend()
			plan, err := b.CreateUsagePlan(CreateUsagePlanInput{Name: "p"})
			require.NoError(t, err)
			key, err := b.CreateAPIKey(CreateAPIKeyInput{Name: "k", Enabled: true})
			require.NoError(t, err)
			_, err = b.CreateUsagePlanKey(CreateUsagePlanKeyInput{
				UsagePlanID: plan.ID, KeyID: key.ID, KeyType: "API_KEY",
			})
			require.NoError(t, err)

			seedUsageState(b, plan.ID, key.ID)
			require.NoError(t, tt.del(b, plan.ID, key.ID))

			b.mu.RLock("check")
			defer b.mu.RUnlock()

			assert.Empty(t, b.usage.quota)
			assert.Empty(t, b.usage.buckets)
		})
	}
}

func TestDeleteRestAPI_ClearsStageThrottleBuckets(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend()
	api, err := b.CreateRestAPI(CreateRestAPIInput{Name: "a"})
	require.NoError(t, err)
	_, err = b.CreateDeployment(api.ID, "prod", "")
	require.NoError(t, err)

	b.mu.Lock("seed")
	b.usage.stageBuckets[stageThrottleKey(api.ID, "prod", "*/*")] = &tokenBucket{}
	b.mu.Unlock()

	require.NoError(t, b.DeleteRestAPI(api.ID))

	b.mu.RLock("check")
	defer b.mu.RUnlock()

	assert.Empty(t, b.usage.stageBuckets)
}

func TestHandler_ResetAndRestoreClearTrieCache(t *testing.T) {
	t.Parallel()

	tests := []struct {
		act  func(h *Handler) error
		name string
	}{
		{name: "reset", act: func(h *Handler) error {
			h.Reset()

			return nil
		}},
		{name: "restore", act: func(h *Handler) error { return h.Restore(t.Context(), nil) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := NewHandler(NewInMemoryBackend())
			h.trieCache.Store("d-1", newResourcePathTrie())

			_ = tt.act(h)

			n := 0
			h.trieCache.Range(func(_, _ any) bool {
				n++

				return true
			})
			assert.Zero(t, n)
		})
	}
}
