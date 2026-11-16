package neptune

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestResetTokenExpiry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		advance time.Duration
		want    bool
	}{
		{"fresh", time.Minute, true},
		{"edge", resetTokenTTL, true},
		{"expired", resetTokenTTL + time.Second, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			now := time.Unix(1_000_000, 0)
			rt := newResetTokens()
			rt.now = func() time.Time { return now }
			tok := rt.issue()
			now = now.Add(tt.advance)

			assert.Equal(t, tt.want, rt.consume(tok))
			assert.False(t, rt.consume(tok), "single use")
		})
	}
}
