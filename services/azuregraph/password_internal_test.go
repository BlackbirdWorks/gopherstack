package azuregraph

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPasswordMatchesMalformedStoredHash(t *testing.T) {
	t.Parallel()

	good, err := hashPassword("Secret#123")
	require.NoError(t, err)

	parts := strings.Split(good, "$")

	tests := []struct {
		name   string
		stored string
		pw     string
		want   bool
	}{
		{name: "match", stored: good, pw: "Secret#123", want: true},
		{name: "wrong", stored: good, pw: "Secret#124"},
		{name: "empty", stored: ""},
		{name: "no separators", stored: "abcdef"},
		{name: "too few parts", stored: strings.Join(parts[:3], "$")},
		{name: "too many parts", stored: good + "$x"},
		{name: "wrong scheme", stored: "md5$" + strings.Join(parts[1:], "$")},
		{name: "non numeric iterations", stored: strings.Join([]string{parts[0], "x", parts[2], parts[3]}, "$")},
		{name: "zero iterations", stored: strings.Join([]string{parts[0], "0", parts[2], parts[3]}, "$")},
		{name: "huge iterations", stored: strings.Join([]string{parts[0], "999999999", parts[2], parts[3]}, "$")},
		{name: "bad salt hex", stored: strings.Join([]string{parts[0], parts[1], "zz", parts[3]}, "$")},
		{name: "empty salt", stored: strings.Join([]string{parts[0], parts[1], "", parts[3]}, "$")},
		{name: "oversize candidate", stored: good, pw: strings.Repeat("a", maxPasswordBytes+1)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, passwordMatches(tt.stored, tt.pw))
		})
	}
}
