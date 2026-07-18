package cloudwatch_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

// ---------------------------------------------------------------------------
// Token signing (gap #15)
// ---------------------------------------------------------------------------

func TestSignPageToken_ZeroReturnsEmpty(t *testing.T) {
	t.Parallel()
	assert.Empty(t, cloudwatch.SignPageTokenForTest(0))
}

func TestSignPageToken_NonZeroRoundTrip(t *testing.T) {
	t.Parallel()

	offsets := []int{1, 10, 99, 500, 1000, 9999, 99999, 1000000}

	for _, offset := range offsets {
		t.Run("", func(t *testing.T) {
			t.Parallel()

			tok := cloudwatch.SignPageTokenForTest(offset)
			require.NotEmpty(t, tok, "offset %d should produce a token", offset)

			got, err := cloudwatch.ParseSignedPageTokenForTest(tok)
			require.NoError(t, err, "offset %d", offset)
			assert.Equal(t, offset, got, "offset %d round-trip mismatch", offset)
		})
	}
}

func TestSignPageToken_TokensDistinct(t *testing.T) {
	t.Parallel()

	tok1 := cloudwatch.SignPageTokenForTest(1)
	tok2 := cloudwatch.SignPageTokenForTest(2)
	assert.NotEqual(t, tok1, tok2, "different offsets must produce different tokens")
}

func TestParseSignedPageToken_InvalidInputs(t *testing.T) {
	t.Parallel()

	tamperedToken := cloudwatch.SignPageTokenForTest(42)
	if len(tamperedToken) > 0 {
		tamperedToken = tamperedToken[:len(tamperedToken)-1] + "X"
	}

	tests := []struct {
		name    string
		token   string
		wantErr bool
		wantVal int
	}{
		{name: "empty token is zero", token: "", wantErr: false, wantVal: 0},
		{name: "tampered payload rejected", token: tamperedToken, wantErr: true},
		{name: "garbage token rejected", token: "definitely-not-a-token", wantErr: true},
		{name: "missing separator rejected", token: "aGVsbG8=", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := cloudwatch.ParseSignedPageTokenForTest(tc.token)
			if tc.wantErr {
				assert.Error(t, err)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantVal, got)
		})
	}
}
