package bedrockagentcore_test

import (
	"bytes"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/bedrockagentcore"
)

func TestCreateEventLogsWouldExtract(t *testing.T) {
	t.Parallel()

	const body = `{"actorId":"a","sessionId":"s","eventTimestamp":1767323045,` +
		`"payload":[{"conversational":{"role":"USER","content":{"text":"hi"}}}]}`

	tests := []struct {
		name       string
		strategies []bedrockagentcore.StrategyInfo
		want       bool
	}{
		{
			name:       "with strategy",
			strategies: []bedrockagentcore.StrategyInfo{{ID: "s1", Name: "facts", Type: "SEMANTIC"}},
			want:       true,
		},
		{name: "no strategy", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctl := defaultControl()
			ctl.memories = map[string]bedrockagentcore.MemoryInfo{
				testMemory: {ID: testMemory, ExpiryDays: 30, Strategies: tt.strategies},
			}
			f := newFixture(t, ctl)

			var buf bytes.Buffer

			ctx := logger.Save(t.Context(), slog.New(slog.NewTextHandler(&buf, nil)))
			require.NoError(t, f.backend.CreateEventDirect(ctx, testMemory, []byte(body)))

			if tt.want {
				assert.Contains(t, buf.String(), "would extract memory records")

				return
			}

			assert.NotContains(t, buf.String(), "would extract")
		})
	}
}
