package expr_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/dynamodb/expr"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParser_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		input   string
		isUpd   bool
	}{
		{
			name:    "grouped expr missing rparen",
			input:   "(pk = :v",
			isUpd:   false,
			wantErr: expr.ErrExpectedRParen2,
		},
		{
			name:    "dot segment missing identifier",
			input:   "pk.",
			isUpd:   false,
			wantErr: expr.ErrExpectedIdentifierDot,
		},
		{
			name:    "bracket segment missing index",
			input:   "pk[@]",
			isUpd:   false,
			wantErr: expr.ErrExpectedIndex,
		},
		{
			name:    "bracket segment missing rbracket",
			input:   "pk[0",
			isUpd:   false,
			wantErr: expr.ErrExpectedRBracket,
		},
		{
			name:    "function missing rparen",
			input:   "size(tags",
			isUpd:   false,
			wantErr: expr.ErrExpectedRParen,
		},
		{
			name:    "between missing and",
			input:   "age BETWEEN :v1 :v2",
			isUpd:   false,
			wantErr: expr.ErrExpectedANDInBetween,
		},
		{
			name:    "in missing lparen",
			input:   "pk IN :v1",
			isUpd:   false,
			wantErr: expr.ErrExpectedLParenAfterIN,
		},
		{
			name:    "in missing rparen",
			input:   "pk IN (:v1",
			isUpd:   false,
			wantErr: expr.ErrExpectedRParenAfterIN,
		},
		{
			name:    "update unexpected token",
			input:   "INVALID #a = :v",
			isUpd:   true,
			wantErr: expr.ErrUnexpectedToken,
		},
		{
			name:    "set missing equal",
			input:   "SET #a :v",
			isUpd:   true,
			wantErr: expr.ErrExpectedEqualInSET,
		},
		{
			name:    "unexpected operand",
			input:   "=",
			isUpd:   false,
			wantErr: expr.ErrUnexpectedToken,
		},
		{
			// AWS: "each action keyword can appear only once" in an UpdateExpression.
			// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.UpdateExpressions.html
			name:    "duplicate SET section rejected",
			input:   "SET #a = :v1 SET #b = :v2",
			isUpd:   true,
			wantErr: expr.ErrDuplicateUpdateSection,
		},
		{
			name:    "duplicate REMOVE section rejected",
			input:   "REMOVE #a REMOVE #b",
			isUpd:   true,
			wantErr: expr.ErrDuplicateUpdateSection,
		},
		{
			// AWS: "The list can contain up to 100 values, separated by commas."
			// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html
			name:    "IN with 101 values rejected",
			input:   "pk IN (" + repeatValuePlaceholders(101) + ")",
			isUpd:   false,
			wantErr: expr.ErrTooManyINValues,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			l := expr.NewLexer(tt.input)
			p := expr.NewParser(l)
			var err error

			if tt.isUpd {
				_, err = p.ParseUpdate()
			} else {
				_, err = p.ParseCondition()
			}

			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}

// repeatValuePlaceholders returns n comma-separated distinct value
// placeholders (":v0, :v1, ...") for building large IN clauses.
func repeatValuePlaceholders(n int) string {
	parts := make([]string, n)
	for i := range n {
		parts[i] = ":v" + strconv.Itoa(i)
	}

	return strings.Join(parts, ", ")
}

func TestParser_IN_MaxValues(t *testing.T) {
	t.Parallel()

	// Exactly 100 values is the documented limit and must still parse.
	l := expr.NewLexer("pk IN (" + repeatValuePlaceholders(100) + ")")
	p := expr.NewParser(l)
	_, err := p.ParseCondition()
	require.NoError(t, err)
}

func TestParser_Projection(t *testing.T) {
	t.Parallel()

	l := expr.NewLexer("a, b.c, d[0]")
	p := expr.NewParser(l)
	proj, err := p.ParseProjection()
	require.NoError(t, err)
	assert.Len(t, proj.Paths, 3)
}

func TestParser_UpdateComplex(t *testing.T) {
	t.Parallel()

	l := expr.NewLexer("SET #a = :v1 REMOVE #b ADD #c :v2")
	p := expr.NewParser(l)
	upd, err := p.ParseUpdate()
	require.NoError(t, err)
	assert.Len(t, upd.Actions, 3)
}
