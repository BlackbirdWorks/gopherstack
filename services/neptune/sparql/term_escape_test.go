package sparql_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/neptune/sparql"
)

func TestTermStringEscapesLiterals(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
		term sparql.Term
	}{
		{"plain", `"abc"`, sparql.Literal("abc")},
		{"double quote", `"a\"b"`, sparql.Literal(`a"b`)},
		{"backslash", `"a\\b"`, sparql.Literal(`a\b`)},
		{"quote then backslash", `"\\\""`, sparql.Literal(`\"`)},
		{"control characters", `"a\nb\rc\td"`, sparql.Literal("a\nb\rc\td")},
		{"lang tag", `"say \"hi\""@en`, sparql.LangLiteral(`say "hi"`, "en")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, tt.term.String())
		})
	}
}
