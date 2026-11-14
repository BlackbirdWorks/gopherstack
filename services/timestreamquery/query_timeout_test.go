package timestreamquery_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/timestreamquery"
)

func TestQuery_Timeout(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		timeout time.Duration
		wantErr bool
	}{
		{"expired deadline", time.Nanosecond, true},
		{"default limit", 0, false},
		{"generous limit", time.Hour, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := timestreamquery.NewInMemoryBackend("000000000000", "us-east-1")
			b.SetQueryTimeout(tt.timeout)

			_, err := b.QueryWithOptions(t.Context(), timestreamquery.QueryOptions{QueryString: "SELECT 1"})
			if tt.wantErr {
				require.ErrorIs(t, err, timestreamquery.ErrQueryExecution)
				assert.NotErrorIs(t, err, timestreamquery.ErrValidation)

				return
			}

			require.NoError(t, err)
		})
	}

	assert.Equal(t, time.Hour, timestreamquery.DefaultQueryTimeout)
}
