package azurearm

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type apimStubEnv struct{}

func (apimStubEnv) member(_ any, name string) (any, error) { return "v-" + name, nil }
func (apimStubEnv) now() time.Time                         { return time.Unix(0, 0).UTC() }

func apimEvalForTest(src string) (any, error) {
	n, err := apimParseExpr(src)
	if err != nil {
		return nil, err
	}

	return apimEvalExpr(n, apimStubEnv{}, &apimExprCounter{})
}

func TestAPIMExprInternal_Pathological(t *testing.T) {
	t.Parallel()

	deepParen := strings.Repeat("(", 5000) + "1" + strings.Repeat(")", 5000)
	deepNot := strings.Repeat("!", 5000) + "true"
	deepNeg := strings.Repeat("-", 5000) + "1"
	deepTernary := strings.Repeat("true ? ", 3000) + "1" + strings.Repeat(" : 2", 3000)
	chain := "context" + strings.Repeat(".Request", 3000)
	args := "string.Concat(" + strings.Repeat("1,", 5000) + "1)"
	big := `"` + strings.Repeat("a", 3000) + `"`
	doubling := big + strings.Repeat(" + "+big, 0)

	tests := []struct {
		name string
		src  string
	}{
		{"deep paren", deepParen},
		{"deep not", deepNot},
		{"deep neg", deepNeg},
		{"deep ternary", deepTernary},
		{"chain", chain},
		{"args", args},
		{"long literal", doubling},
		{"huge int", "99999999999999999999999999"},
		{"min int div", "(-9223372036854775807 - 1) / -1"},
		{"min int mod", "(-9223372036854775807 - 1) % -1"},
		{"unterminated", `"abc`},
		{"lone op", "&&"},
		{"nul", "\x00"},
		{"invalid utf8", "\xff\xfe"},
		{"empty", ""},
		{"index", `context.Request.Headers[`},
		{"generic", `context.Variables.GetValueOrDefault<`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.NotPanics(t, func() { _, _ = apimEvalForTest(tt.src) })
		})
	}
}

func TestAPIMExprInternal_Doubling(t *testing.T) {
	t.Parallel()

	// 12 concatenations of a 1000-char literal tree cannot exceed the string bound.
	lit := `"` + strings.Repeat("a", 900) + `"`
	expr := lit
	for len(expr) < apimExprMaxLen/2 {
		expr = "(" + expr + " + " + expr + ")"
	}

	v, err := apimEvalForTest(expr)
	if err == nil {
		s, _ := v.(string)
		assert.LessOrEqual(t, len(s), apimExprMaxString)
	}

	require.NotPanics(t, func() { _, _ = apimEvalForTest(expr) })
}

func FuzzAPIMExpr(f *testing.F) {
	for _, s := range []string{
		`"a" + "b"`, `1 + 2 * 3`, `true ? 1 : 2`, `context.Request.Method == "GET"`,
		`(int)"5" + 1`, `null ?? "x"`, `!true || false && true`, `-5 % 3`,
		`context.Variables.GetValueOrDefault<string>("k", null)`, `"a".ToUpper().Length`,
	} {
		f.Add(s)
	}

	f.Fuzz(func(_ *testing.T, src string) {
		_, _ = apimEvalForTest(src)
	})
}
