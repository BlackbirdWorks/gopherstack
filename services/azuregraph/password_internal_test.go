package azuregraph

import (
	"encoding/hex"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func storedHash(t *testing.T, scheme string, iters int, saltHex, pw string) string {
	t.Helper()

	salt, err := hex.DecodeString(saltHex)
	require.NoError(t, err)

	derived, ok := derivePassword(pw, salt, iters)
	require.True(t, ok)

	return strings.Join([]string{scheme, strconv.Itoa(iters), saltHex, derived}, "$")
}

func TestPasswordMatchesMalformedStoredHash(t *testing.T) {
	t.Parallel()

	good, err := hashPassword("Secret#123")
	require.NoError(t, err)

	parts := strings.Split(good, "$")
	salt := parts[2]

	tests := []struct {
		name   string
		stored string
		pw     string
		want   bool
	}{
		{name: "match", stored: good, pw: "Secret#123", want: true},
		{
			name:   "valid other params",
			stored: storedHash(t, hashScheme, 1000, salt, "Secret#123"),
			pw:     "Secret#123",
			want:   true,
		},
		{name: "wrong", stored: good, pw: "Secret#124"},
		{name: "empty", stored: ""},
		{name: "no separators", stored: "abcdef"},
		{name: "too few parts", stored: strings.Join(parts[:3], "$")},
		{name: "too many parts", stored: good + "$x"},
		{name: "wrong scheme", stored: storedHash(t, "md5", passwordIterations, salt, "Secret#123"), pw: "Secret#123"},
		{
			name:   "iterations over bound",
			stored: storedHash(t, hashScheme, maxPasswordIters+1, salt, "Secret#123"),
			pw:     "Secret#123",
		},
		{name: "empty salt", stored: storedHash(t, hashScheme, 1000, "", "Secret#123"), pw: "Secret#123"},
		{name: "non numeric iterations", stored: strings.Join([]string{parts[0], "x", parts[2], parts[3]}, "$")},
		{name: "bad salt hex", stored: strings.Join([]string{parts[0], parts[1], "zz", parts[3]}, "$")},
		{
			name:   "oversize candidate",
			stored: storedHash(t, hashScheme, 1000, salt, strings.Repeat("a", maxPasswordBytes+1)),
			pw:     strings.Repeat("a", maxPasswordBytes+1),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, passwordMatches(tt.stored, tt.pw))
		})
	}
}
