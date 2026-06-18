package s3control_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/s3control"
)

// TestParity_CreateJob_PriorityBound verifies CreateJob rejects a negative
// priority (AWS bounds Priority to a non-negative integer) while accepting valid
// non-negative values.
func TestParity_CreateJob_PriorityBound(t *testing.T) {
	t.Parallel()

	const role = "arn:aws:iam::000000000000:role/R"

	tests := []struct {
		name     string
		priority int32
		wantErr  bool
	}{
		{name: "zero_ok", priority: 0, wantErr: false},
		{name: "positive_ok", priority: 100, wantErr: false},
		{name: "negative_rejected", priority: -1, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := s3control.NewInMemoryBackend()
			_, err := b.CreateJob("000000000000", role, tt.priority)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, s3control.ErrValidation)

				return
			}

			require.NoError(t, err)
		})
	}
}
