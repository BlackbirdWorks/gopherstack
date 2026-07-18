package bedrockruntime_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/bedrockruntime"
)

// --- Backend record/list/reset tests ---

func TestBackend_RecordAndList(t *testing.T) {
	t.Parallel()

	b := bedrockruntime.NewInMemoryBackend("123456789012", "us-east-1")
	assert.Equal(t, "us-east-1", b.Region())

	invocations := b.ListInvocations()
	assert.Empty(t, invocations)

	inv := b.RecordInvocation("InvokeModel", "anthropic.claude-v2", `{"prompt":"hi"}`, `{"completion":"hello"}`)
	assert.Equal(t, "InvokeModel", inv.Operation)
	assert.Equal(t, "anthropic.claude-v2", inv.ModelID)
	assert.NotZero(t, inv.CreatedAt)

	invocations = b.ListInvocations()
	require.Len(t, invocations, 1)
	assert.Equal(t, "InvokeModel", invocations[0].Operation)
}

func TestBackend_InvocationHistoryCap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		recordCount int
		wantLen     int
	}{
		{
			name:        "below_cap",
			recordCount: 10,
			wantLen:     10,
		},
		{
			name:        "at_cap",
			recordCount: bedrockruntime.MaxInvocationHistory,
			wantLen:     bedrockruntime.MaxInvocationHistory,
		},
		{
			name:        "above_cap_retains_most_recent",
			recordCount: bedrockruntime.MaxInvocationHistory + 50,
			wantLen:     bedrockruntime.MaxInvocationHistory,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrockruntime.NewInMemoryBackend("123456789012", "us-east-1")

			for i := range tt.recordCount {
				b.RecordInvocation("InvokeModel", "model", fmt.Sprintf(`{"seq":%d}`, i), `{}`)
			}

			invocations := b.ListInvocations()
			assert.Len(t, invocations, tt.wantLen)

			// Verify the most recent entries are retained (not the oldest).
			if tt.recordCount > bedrockruntime.MaxInvocationHistory {
				last := invocations[len(invocations)-1]
				assert.Contains(t, last.Input, fmt.Sprintf(`"seq":%d`, tt.recordCount-1))
			}
		})
	}
}

func TestBackend_Reset(t *testing.T) {
	t.Parallel()

	b := bedrockruntime.NewInMemoryBackend("123456789012", "us-east-1")

	// Add some state.
	b.RecordInvocation("InvokeModel", "model", `{"prompt":"hi"}`, `{}`)
	_, err := b.StartAsyncInvoke("model", "s3://bucket/", "token-abc", nil)
	require.NoError(t, err)

	require.Len(t, b.ListInvocations(), 1)
	require.Len(t, b.ListAsyncInvokes(bedrockruntime.ListAsyncInvokesFilter{}), 1)

	// Reset clears all state.
	b.Reset()

	assert.Empty(t, b.ListInvocations())
	assert.Empty(t, b.ListAsyncInvokes(bedrockruntime.ListAsyncInvokesFilter{}))

	// After reset the idempotency index is also cleared:
	// same token can be used again.
	inv, err := b.StartAsyncInvoke("model", "s3://bucket/", "token-abc", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, inv.InvocationArn)
}

// --- Invocation history accuracy across operations ---

func TestInvocationHistory_RecordedAfterInvoke(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		operation string
		path      string
	}{
		{name: "invoke model", operation: "InvokeModel", path: "/model/anthropic.claude-v2/invoke"},
		{name: "converse", operation: "Converse", path: "/model/anthropic.claude-v2/converse"},
		{name: "count tokens", operation: "CountTokens", path: "/model/anthropic.claude-v2/count-tokens"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := bedrockruntime.NewInMemoryBackend("000000000000", "us-east-1")
			h := bedrockruntime.NewHandler(b)

			rec := doRequest(t, h, http.MethodPost, tt.path, map[string]any{"prompt": "test"})
			assert.Equal(t, http.StatusOK, rec.Code)

			invocations := b.ListInvocations()
			require.Len(t, invocations, 1)
			assert.Equal(t, tt.operation, invocations[0].Operation)
			assert.Equal(t, "anthropic.claude-v2", invocations[0].ModelID)
		})
	}
}

func TestInvocationHistory_MultipleInvocations(t *testing.T) {
	t.Parallel()

	b := bedrockruntime.NewInMemoryBackend("000000000000", "us-east-1")
	h := bedrockruntime.NewHandler(b)

	for i := range 5 {
		rec := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/invoke",
			map[string]any{"prompt": fmt.Sprintf("prompt %d", i)})
		assert.Equal(t, http.StatusOK, rec.Code)
	}

	invocations := b.ListInvocations()
	assert.Len(t, invocations, 5)
}
