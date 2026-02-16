package expr_test

import (
	"Gopherstack/dynamodb/expr"
	"testing"

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
			name:    "function missing lparen",
			input:   "size tags",
			isUpd:   false,
			wantErr: expr.ErrExpectedLParen,
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
