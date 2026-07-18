package kms_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

// TestGenerateRandom_Validation covers default/custom/max sizes and validation
// failures (zero, negative, over-max NumberOfBytes); it subsumes what were
// previously separate DefaultSize/CustomSize/MaxSize/OverMaxFails tests.
func TestGenerateRandom_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		n       *int32
		name    string
		wantErr bool
	}{
		{name: "valid_32", n: func() *int32 {
			v := int32(32)

			return &v
		}()},
		{name: "valid_1024", n: func() *int32 {
			v := int32(1024)

			return &v
		}()},
		{name: "zero", n: func() *int32 {
			v := int32(0)

			return &v
		}(), wantErr: true},
		{name: "negative", n: func() *int32 {
			v := int32(-1)

			return &v
		}(), wantErr: true},
		{name: "over_max", n: func() *int32 {
			v := int32(1025)

			return &v
		}(), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			out, err := b.GenerateRandom(context.Background(), &kms.GenerateRandomInput{NumberOfBytes: tt.n})

			if tt.wantErr {
				require.ErrorIs(t, err, kms.ErrValidation)

				return
			}

			require.NoError(t, err)
			assert.Len(t, out.Plaintext, int(*tt.n))
		})
	}
}

// TestGenerateRandom verifies random byte generation.
func TestGenerateRandom(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		wantLen       int
		numberOfBytes int32
		wantErr       bool
	}{
		{name: "default_32_bytes", wantLen: 32},
		{name: "explicit_64_bytes", numberOfBytes: 64, wantLen: 64},
		{name: "max_1024_bytes", numberOfBytes: 1024, wantLen: 1024},
		{name: "zero_returns_error", numberOfBytes: -1, wantErr: true},
		{name: "over_max_returns_error", numberOfBytes: 1025, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()

			input := &kms.GenerateRandomInput{}
			if tt.numberOfBytes > 0 {
				n := tt.numberOfBytes
				input.NumberOfBytes = &n
			} else if tt.numberOfBytes != 0 {
				// Negative means explicitly pass zero to trigger validation error.
				zero := int32(0)
				input.NumberOfBytes = &zero
			}

			out, err := b.GenerateRandom(context.Background(), input)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Len(t, out.Plaintext, tt.wantLen)

			// Two calls should produce different random bytes.
			out2, err := b.GenerateRandom(context.Background(), input)
			require.NoError(t, err)
			assert.NotEqual(t, out.Plaintext, out2.Plaintext)
		})
	}
}
