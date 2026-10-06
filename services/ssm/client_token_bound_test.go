package ssm_test

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

func TestClientTokenMemoIsBounded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantReplay map[string]bool
		name       string
		tokens     int
	}{
		{map[string]bool{"tok-1099": true, "tok-0": false}, "newest_token_replays", 1100},
		{map[string]bool{"tok-9": true, "tok-0": true}, "under_cap_keeps_all", 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			ids := make(map[string]string, tt.tokens)

			for i := range tt.tokens {
				tok := "tok-" + strconv.Itoa(i)
				out, err := b.StartAutomationExecution(t.Context(), &ssm.StartAutomationExecutionInput{
					DocumentName: "doc", ClientToken: tok,
				})
				require.NoError(t, err)

				ids[tok] = out.AutomationExecutionID
			}

			for tok, wantSame := range tt.wantReplay {
				out, err := b.StartAutomationExecution(t.Context(), &ssm.StartAutomationExecutionInput{
					DocumentName: "doc", ClientToken: tok,
				})
				require.NoError(t, err)
				assert.Equal(t, wantSame, out.AutomationExecutionID == ids[tok], tok)
			}

			var snap struct {
				Idempotency map[string]map[string]json.RawMessage `json:"idempotency"`
			}

			require.NoError(t, json.Unmarshal(b.Snapshot(t.Context()), &snap))

			for _, recs := range snap.Idempotency {
				assert.LessOrEqual(t, len(recs), 1024)
			}
		})
	}
}
