package expr

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLexer_NextToken(t *testing.T) {
	t.Parallel()

	input := `SET #a = :v1, #b = :v2 REMOVE #c age >= :minAge begins_with(pk, :prefix) contains(tags, :tag) size(tags) < 10 <> <= > ( ) [ ] . AND OR NOT BETWEEN IN ADD DELETE attribute_exists attribute_not_exists attribute_type`

	tests := []struct {
		expectedType    TokenType
		expectedLiteral string
	}{
		{TokenSET, "SET"},
		{TokenIdentifier, "#a"},
		{TokenEqual, "="},
		{TokenValue, ":v1"},
		{TokenComma, ","},
		{TokenIdentifier, "#b"},
		{TokenEqual, "="},
		{TokenValue, ":v2"},
		{TokenREMOVE, "REMOVE"},
		{TokenIdentifier, "#c"},
		{TokenIdentifier, "age"},
		{TokenGreaterEqual, ">="},
		{TokenValue, ":minAge"},
		{TokenBeginsWith, "begins_with"},
		{TokenLParen, "("},
		{TokenIdentifier, "pk"},
		{TokenComma, ","},
		{TokenValue, ":prefix"},
		{TokenRParen, ")"},
		{TokenContains, "contains"},
		{TokenLParen, "("},
		{TokenIdentifier, "tags"},
		{TokenComma, ","},
		{TokenValue, ":tag"},
		{TokenRParen, ")"},
		{TokenSize, "size"},
		{TokenLParen, "("},
		{TokenIdentifier, "tags"},
		{TokenRParen, ")"},
		{TokenLess, "<"},
		{TokenIdentifier, "10"},
		{TokenNotEqual, "<>"},
		{TokenLessEqual, "<="},
		{TokenGreater, ">"},
		{TokenLParen, "("},
		{TokenRParen, ")"},
		{TokenLBracket, "["},
		{TokenRBracket, "]"},
		{TokenDot, "."},
		{TokenAND, "AND"},
		{TokenOR, "OR"},
		{TokenNOT, "NOT"},
		{TokenBETWEEN, "BETWEEN"},
		{TokenIN, "IN"},
		{TokenADD, "ADD"},
		{TokenDELETE, "DELETE"},
		{TokenAttributeExists, "attribute_exists"},
		{TokenAttributeNotExists, "attribute_not_exists"},
		{TokenAttributeType, "attribute_type"},
		{TokenEOF, ""},
	}

	l := NewLexer(input)

	for i, tt := range tests {
		tok := l.NextToken()
		assert.Equal(t, tt.expectedType, tok.Type, "test[%d] - tokentype wrong", i)
		assert.Equal(t, tt.expectedLiteral, tok.Literal, "test[%d] - literal wrong", i)
	}
}

func TestLexer_CaseInsensitivity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input        string
		expectedType TokenType
	}{
		{"AND", TokenAND},
		{"and", TokenAND},
		{"OR", TokenOR},
		{"or", TokenOR},
		{"NOT", TokenNOT},
		{"not", TokenNOT},
		{"SET", TokenSET},
		{"set", TokenSET},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			l := NewLexer(tt.input)
			tok := l.NextToken()
			assert.Equal(t, tt.expectedType, tok.Type)
		})
	}
}

func TestLexer_Errors(t *testing.T) {
	t.Parallel()

	input := "@"
	l := NewLexer(input)
	tok := l.NextToken()
	assert.Equal(t, TokenError, tok.Type)
	assert.Equal(t, "@", tok.Literal)
}

func TestToken_String(t *testing.T) {
	t.Parallel()
	tok := Token{Type: TokenEqual, Literal: "="}
	expected := fmt.Sprintf("{%d \"=\"}", TokenEqual)
	assert.Equal(t, expected, tok.String())
}
