package cloudformation_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

func TestQuotaLimiterStackCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		limit     float64
		creates   int
		wantCount int
		known     bool
		wantErr   bool
	}{
		{name: "within limit", limit: 2, known: true, creates: 2, wantCount: 2},
		{name: "over limit", limit: 2, known: true, creates: 3, wantCount: 2, wantErr: true},
		{name: "unknown quota", limit: 1, creates: 3, wantCount: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := cloudformation.NewInMemoryBackendWithConfig("123456789012", "us-east-1", nil)
			h := cloudformation.NewHandler(b)
			h.SetQuotaLimiter(func(_, _, svc, code string) (float64, bool) {
				return tt.limit, tt.known && svc == "cloudformation" && code == "L-0485CB21"
			})

			var err error

			for i := range tt.creates {
				_, err = b.CreateStack(
					t.Context(), "s"+strconv.Itoa(i), simpleTemplate, nil, cloudformation.StackOptions{},
				)
			}

			if tt.wantErr {
				require.ErrorIs(t, err, cloudformation.ErrStackLimitExceeded)
			} else {
				require.NoError(t, err)
			}

			assert.Equal(t, tt.wantCount, h.StackCount("us-east-1"))
			assert.Equal(t, 0, h.StackCount("us-west-2"))
		})
	}
}

func TestQuotaLimiterExistingStackKeepsAlreadyExists(t *testing.T) {
	t.Parallel()

	b := cloudformation.NewInMemoryBackendWithConfig("123456789012", "us-east-1", nil)
	h := cloudformation.NewHandler(b)

	_, err := b.CreateStack(t.Context(), "dup", simpleTemplate, nil, cloudformation.StackOptions{})
	require.NoError(t, err)

	h.SetQuotaLimiter(func(_, _, _, _ string) (float64, bool) { return 1, true })

	_, err = b.CreateStack(t.Context(), "dup", simpleTemplate, nil, cloudformation.StackOptions{})
	require.ErrorIs(t, err, cloudformation.ErrStackAlreadyExists)
}
