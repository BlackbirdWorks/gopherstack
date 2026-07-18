package sagemakerruntime_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sagemakerruntime"
)

// --- Backend tests ---

func TestBackend_RecordAndList(t *testing.T) {
	t.Parallel()

	b := sagemakerruntime.NewInMemoryBackend("123456789012", "us-east-1")
	assert.Equal(t, "us-east-1", b.Region())

	invocations := b.ListInvocations()
	assert.Empty(t, invocations)

	inv := b.RecordInvocation("InvokeEndpoint", "my-endpoint", `{"data":"hi"}`, `{"Body":"ok"}`)
	assert.Equal(t, "InvokeEndpoint", inv.Operation)
	assert.Equal(t, "my-endpoint", inv.EndpointName)
	assert.NotZero(t, inv.CreatedAt)

	invocations = b.ListInvocations()
	require.Len(t, invocations, 1)
	assert.Equal(t, "InvokeEndpoint", invocations[0].Operation)
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
			recordCount: sagemakerruntime.MaxInvocationHistory,
			wantLen:     sagemakerruntime.MaxInvocationHistory,
		},
		{
			name:        "above_cap_retains_most_recent",
			recordCount: sagemakerruntime.MaxInvocationHistory + 50,
			wantLen:     sagemakerruntime.MaxInvocationHistory,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sagemakerruntime.NewInMemoryBackend("123456789012", "us-east-1")

			for i := range tt.recordCount {
				b.RecordInvocation("InvokeEndpoint", "ep", fmt.Sprintf(`{"seq":%d}`, i), `{}`)
			}

			invocations := b.ListInvocations()
			assert.Len(t, invocations, tt.wantLen)

			if tt.recordCount > sagemakerruntime.MaxInvocationHistory {
				last := invocations[len(invocations)-1]
				assert.Contains(t, last.Input, fmt.Sprintf(`"seq":%d`, tt.recordCount-1))
			}
		})
	}
}
