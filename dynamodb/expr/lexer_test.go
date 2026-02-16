package expr_test

import (
	"Gopherstack/dynamodb/expr"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLexer_NextToken(t *testing.T) {
	t.Parallel()

	input := "SET #a = :v1, #b = :v2 REMOVE #c age >= :minAge " +
		"begins_with(pk, :prefix) contains(tags, :tag) size(tags) < 10 <> <= > " +
		"( ) [ ] . AND OR NOT BETWEEN IN ADD DELETE " +
		"attribute_exists attribute_not_exists attribute_type"

	tests := []struct {
		expectedLiteral string
		expectedType    expr.TokenType
	}{
		{expectedType: expr.TokenSET, expectedLiteral: "SET"},
		{expectedType: expr.TokenIdentifier, expectedLiteral: "#a"},
		{expectedType: expr.TokenEqual, expectedLiteral: "="},
		{expectedType: expr.TokenValue, expectedLiteral: ":v1"},
		{expectedType: expr.TokenComma, expectedLiteral: ","},
		{expectedType: expr.TokenIdentifier, expectedLiteral: "#b"},
		{expectedType: expr.TokenEqual, expectedLiteral: "="},
		{expectedType: expr.TokenValue, expectedLiteral: ":v2"},
		{expectedType: expr.TokenREMOVE, expectedLiteral: "REMOVE"},
		{expectedType: expr.TokenIdentifier, expectedLiteral: "#c"},
		{expectedType: expr.TokenIdentifier, expectedLiteral: "age"},
		{expectedType: expr.TokenGreaterEqual, expectedLiteral: ">="},
		{expectedType: expr.TokenValue, expectedLiteral: ":minAge"},
		{expectedType: expr.TokenBeginsWith, expectedLiteral: "begins_with"},
		{expectedType: expr.TokenLParen, expectedLiteral: "("},
		{expectedType: expr.TokenIdentifier, expectedLiteral: "pk"},
		{expectedType: expr.TokenComma, expectedLiteral: ","},
		{expectedType: expr.TokenValue, expectedLiteral: ":prefix"},
		{expectedType: expr.TokenRParen, expectedLiteral: ")"},
		{expectedType: expr.TokenContains, expectedLiteral: "contains"},
		{expectedType: expr.TokenLParen, expectedLiteral: "("},
		{expectedType: expr.TokenIdentifier, expectedLiteral: "tags"},
		{expectedType: expr.TokenComma, expectedLiteral: ","},
		{expectedType: expr.TokenValue, expectedLiteral: ":tag"},
		{expectedType: expr.TokenRParen, expectedLiteral: ")"},
		{expectedType: expr.TokenSize, expectedLiteral: "size"},
		{expectedType: expr.TokenLParen, expectedLiteral: "("},
		{expectedType: expr.TokenIdentifier, expectedLiteral: "tags"},
		{expectedType: expr.TokenRParen, expectedLiteral: ")"},
		{expectedType: expr.TokenLess, expectedLiteral: "<"},
		{expectedType: expr.TokenIdentifier, expectedLiteral: "10"},
		{expectedType: expr.TokenNotEqual, expectedLiteral: "<>"},
		{expectedType: expr.TokenLessEqual, expectedLiteral: "<="},
		{expectedType: expr.TokenGreater, expectedLiteral: ">"},
		{expectedType: expr.TokenLParen, expectedLiteral: "("},
		{expectedType: expr.TokenRParen, expectedLiteral: ")"},
		{expectedType: expr.TokenLBracket, expectedLiteral: "["},
		{expectedType: expr.TokenRBracket, expectedLiteral: "]"},
		{expectedType: expr.TokenDot, expectedLiteral: "."},
		{expectedType: expr.TokenAND, expectedLiteral: "AND"},
		{expectedType: expr.TokenOR, expectedLiteral: "OR"},
		{expectedType: expr.TokenNOT, expectedLiteral: "NOT"},
		{expectedType: expr.TokenBETWEEN, expectedLiteral: "BETWEEN"},
		{expectedType: expr.TokenIN, expectedLiteral: "IN"},
		{expectedType: expr.TokenADD, expectedLiteral: "ADD"},
		{expectedType: expr.TokenDELETE, expectedLiteral: "DELETE"},
		{expectedType: expr.TokenAttributeExists, expectedLiteral: "attribute_exists"},
		{expectedType: expr.TokenAttributeNotExists, expectedLiteral: "attribute_not_exists"},
		{expectedType: expr.TokenAttributeType, expectedLiteral: "attribute_type"},
		{expectedType: expr.TokenEOF, expectedLiteral: ""},
	}

	l := expr.NewLexer(input)

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
		expectedType expr.TokenType
	}{
		{input: "AND", expectedType: expr.TokenAND},
		{input: "and", expectedType: expr.TokenAND},
		{input: "OR", expectedType: expr.TokenOR},
		{input: "or", expectedType: expr.TokenOR},
		{input: "NOT", expectedType: expr.TokenNOT},
		{input: "not", expectedType: expr.TokenNOT},
		{input: "SET", expectedType: expr.TokenSET},
		{input: "set", expectedType: expr.TokenSET},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			t.Parallel()
			l := expr.NewLexer(tt.input)
			tok := l.NextToken()
			assert.Equal(t, tt.expectedType, tok.Type)
		})
	}
}

func TestLexer_Errors(t *testing.T) {
	t.Parallel()

	input := "@"
	l := expr.NewLexer(input)
	tok := l.NextToken()
	assert.Equal(t, expr.TokenError, tok.Type)
	assert.Equal(t, "@", tok.Literal)
}

func TestToken_String(t *testing.T) {
	t.Parallel()

	tok := expr.Token{Type: expr.TokenEqual, Literal: "="}
	expected := fmt.Sprintf("{%d \"=\"}", expr.TokenEqual)
	assert.Equal(t, expected, tok.String())
}
