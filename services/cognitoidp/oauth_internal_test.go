package cognitoidp

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuthCodeStore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, b *InMemoryBackend)
		name string
	}{
		{name: "single use", run: func(t *testing.T, b *InMemoryBackend) {
			t.Helper()

			code, err := b.storeAuthCode(&authCodeEntry{ClientID: "c"})
			require.NoError(t, err)

			e, err := b.consumeAuthCode(code)
			require.NoError(t, err)
			assert.Equal(t, "c", e.ClientID)

			_, err = b.consumeAuthCode(code)
			require.ErrorIs(t, err, errAuthCodeInvalid)
		}},
		{name: "expired code rejected and removed", run: func(t *testing.T, b *InMemoryBackend) {
			t.Helper()

			code, err := b.storeAuthCode(&authCodeEntry{ClientID: "c"})
			require.NoError(t, err)

			b.authCodes[code].ExpiresAt = time.Now().Add(-time.Second)

			_, err = b.consumeAuthCode(code)
			require.ErrorIs(t, err, errAuthCodeInvalid)
			assert.Empty(t, b.authCodes)
		}},
		{name: "ttl is five minutes", run: func(t *testing.T, b *InMemoryBackend) {
			t.Helper()

			code, err := b.storeAuthCode(&authCodeEntry{})
			require.NoError(t, err)
			assert.WithinDuration(t, time.Now().Add(5*time.Minute), b.authCodes[code].ExpiresAt, time.Minute)
		}},
		{name: "store is bounded", run: func(t *testing.T, b *InMemoryBackend) {
			t.Helper()

			for range maxAuthCodes + 50 {
				_, err := b.storeAuthCode(&authCodeEntry{})
				require.NoError(t, err)
			}

			assert.LessOrEqual(t, len(b.authCodes), maxAuthCodes)
		}},
		{name: "reset clears codes", run: func(t *testing.T, b *InMemoryBackend) {
			t.Helper()

			code, err := b.storeAuthCode(&authCodeEntry{})
			require.NoError(t, err)

			b.Reset()

			_, err = b.consumeAuthCode(code)
			require.ErrorIs(t, err, errAuthCodeInvalid)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.run(t, NewInMemoryBackend("000000000000", "us-east-1", "http://localhost:8000"))
		})
	}
}

func TestSecretMatches(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		provided   string
		candidates []string
		want       bool
	}{
		{name: "match", candidates: []string{"a", "b"}, provided: "b", want: true},
		{name: "second secret", candidates: []string{"a", "b"}, provided: "a", want: true},
		{name: "mismatch", candidates: []string{"a"}, provided: "x"},
		{name: "empty provided", candidates: []string{"a"}, provided: ""},
		{name: "no candidates", provided: "a"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, secretMatches(tt.candidates, tt.provided))
		})
	}
}
