package expr

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParser_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		input   string
		isUpd   bool
	}{
		{"grouped expr missing rparen", "(pk = :v", false, ErrExpectedRParen2},
		{"dot segment missing identifier", "pk.", false, ErrExpectedIdentifierDot},
		{"bracket segment missing index", "pk[@]", false, ErrExpectedIndex},
		{"bracket segment missing rbracket", "pk[0", false, ErrExpectedRBracket},
		{"function missing lparen", "size tags", false, ErrExpectedLParen},
		{"function missing rparen", "size(tags", false, ErrExpectedRParen},
		{"between missing and", "age BETWEEN :v1 :v2", false, ErrExpectedANDInBetween},
		{"in missing lparen", "pk IN :v1", false, ErrExpectedLParenAfterIN},
		{"in missing rparen", "pk IN (:v1", false, ErrExpectedRParenAfterIN},
		{"update unexpected token", "INVALID #a = :v", true, ErrUnexpectedToken},
		{"set missing equal", "SET #a :v", true, ErrExpectedEqualInSET},
		{"unexpected operand", "=", false, ErrUnexpectedToken}, // Starts with =
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			l := NewLexer(tt.input)
			p := NewParser(l)
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
	l := NewLexer("a, b.c, d[0]")
	p := NewParser(l)
	proj, err := p.ParseProjection()
	assert.NoError(t, err)
	assert.Len(t, proj.Paths, 3)
}

func TestParser_UpdateComplex(t *testing.T) {
	t.Parallel()
	l := NewLexer("SET #a = :v1 REMOVE #b ADD #c :v2")
	p := NewParser(l)
	upd, err := p.ParseUpdate()
	assert.NoError(t, err)
	assert.Len(t, upd.Actions, 3)
}
