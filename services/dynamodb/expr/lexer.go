package expr

import (
	"fmt"
	"strings"
)

type TokenType int

const (
	TokenEOF TokenType = iota
	TokenError

	// TokenIdentifier represents identifiers and placeholders.
	TokenIdentifier // #name or pk or info.tags
	TokenValue      // :val

	// TokenEqual represents operators.
	TokenEqual        // =
	TokenNotEqual     // <>
	TokenLess         // <
	TokenGreater      // >
	TokenLessEqual    // <=
	TokenGreaterEqual // >=
	TokenPlus         // +
	TokenMinus        // -
	TokenLParen       // (
	TokenRParen       // )
	TokenComma        // ,
	TokenDot          // .
	TokenLBracket     // [
	TokenRBracket     // ]

	// TokenAND represents keywords.
	TokenAND
	TokenOR
	TokenNOT
	TokenBETWEEN
	TokenIN
	TokenSET
	TokenREMOVE
	TokenADD
	TokenDELETE

	// TokenSize represents function keywords.
	TokenSize
	TokenAttributeExists
	TokenAttributeNotExists
	TokenBeginsWith
	TokenContains
	TokenAttributeType
	TokenIfNotExists
	TokenListAppend
)

type Token struct {
	Literal string
	Type    TokenType
}

func (t Token) String() string {
	return fmt.Sprintf("{%d %q}", t.Type, t.Literal)
}

type Lexer struct {
	input        string
	position     int
	readPosition int
	ch           byte
}

func NewLexer(input string) *Lexer {
	l := &Lexer{input: input}
	l.readChar()

	return l
}

func (l *Lexer) readChar() {
	if l.readPosition >= len(l.input) {
		l.ch = 0
	} else {
		l.ch = l.input[l.readPosition]
	}
	l.position = l.readPosition
	l.readPosition++
}

func (l *Lexer) NextToken() Token {
	l.skipWhitespace()

	var tok Token

	switch l.ch {
	case '=':
		tok = Token{Type: TokenEqual, Literal: "="}
	case '<':
		tok = l.handleLessThan()
	case '>':
		tok = l.handleGreaterThan()
	case '+':
		tok = Token{Type: TokenPlus, Literal: "+"}
	case '-':
		tok = Token{Type: TokenMinus, Literal: "-"}
	case '(':
		tok = Token{Type: TokenLParen, Literal: "("}
	case ')':
		tok = Token{Type: TokenRParen, Literal: ")"}
	case ',':
		tok = Token{Type: TokenComma, Literal: ","}
	case '.':
		tok = Token{Type: TokenDot, Literal: "."}
	case '[':
		tok = Token{Type: TokenLBracket, Literal: "["}
	case ']':
		tok = Token{Type: TokenRBracket, Literal: "]"}
	case 0:
		tok = Token{Type: TokenEOF, Literal: ""}
	default:
		return l.handleDefault()
	}

	l.readChar()

	return tok
}

func (l *Lexer) handleLessThan() Token {
	if l.peekChar() == '>' {
		l.readChar()

		return Token{Type: TokenNotEqual, Literal: "<>"}
	}
	if l.peekChar() == '=' {
		l.readChar()

		return Token{Type: TokenLessEqual, Literal: "<="}
	}

	return Token{Type: TokenLess, Literal: "<"}
}

func (l *Lexer) handleGreaterThan() Token {
	if l.peekChar() == '=' {
		l.readChar()

		return Token{Type: TokenGreaterEqual, Literal: ">="}
	}

	return Token{Type: TokenGreater, Literal: ">"}
}

func (l *Lexer) handleDefault() Token {
	if isLetter(l.ch) || l.ch == '#' || l.ch == ':' {
		literal := l.readIdentifier()

		return Token{Type: lookupIdentifier(literal), Literal: literal}
	}
	if isDigit(l.ch) {
		return Token{Type: TokenIdentifier, Literal: l.readNumber()}
	}

	return Token{Type: TokenError, Literal: string(l.ch)}
}

func (l *Lexer) readIdentifier() string {
	position := l.position
	// DynamoDB expression identifiers allow letters, digits, underscores, '#' (expression
	// attribute name placeholder prefix), and ':' (expression attribute value placeholder prefix).
	// '.' is treated as a path separator and is NOT part of the identifier token.
	for isLetter(l.ch) || isDigit(l.ch) || l.ch == '_' || l.ch == '#' || l.ch == ':' {
		l.readChar()
	}

	return l.input[position:l.position]
}

func (l *Lexer) readNumber() string {
	position := l.position
	for isDigit(l.ch) {
		l.readChar()
	}

	return l.input[position:l.position]
}

func (l *Lexer) peekChar() byte {
	if l.readPosition >= len(l.input) {
		return 0
	}

	return l.input[l.readPosition]
}

func (l *Lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

func isLetter(ch byte) bool {
	return 'a' <= ch && ch <= 'z' || 'A' <= ch && ch <= 'Z' || ch == '_'
}

func isDigit(ch byte) bool {
	return '0' <= ch && ch <= '9'
}

func lookupKeyword(ident string) (TokenType, bool) {
	if tok, ok := lookupLogicalKeyword(ident); ok {
		return tok, true
	}

	return lookupFunctionKeyword(ident)
}

func lookupLogicalKeyword(ident string) (TokenType, bool) {
	switch ident {
	case "AND":
		return TokenAND, true
	case "OR":
		return TokenOR, true
	case "NOT":
		return TokenNOT, true
	case "BETWEEN":
		return TokenBETWEEN, true
	case "IN":
		return TokenIN, true
	case "SET":
		return TokenSET, true
	case "REMOVE":
		return TokenREMOVE, true
	case "ADD":
		return TokenADD, true
	case "DELETE":
		return TokenDELETE, true
	default:
		return TokenIdentifier, false
	}
}

func lookupFunctionKeyword(ident string) (TokenType, bool) {
	switch ident {
	case "size":
		return TokenSize, true
	case "attribute_exists":
		return TokenAttributeExists, true
	case "attribute_not_exists":
		return TokenAttributeNotExists, true
	case "begins_with":
		return TokenBeginsWith, true
	case "contains":
		return TokenContains, true
	case "attribute_type":
		return TokenAttributeType, true
	case "if_not_exists":
		return TokenIfNotExists, true
	case "list_append":
		return TokenListAppend, true
	default:
		return TokenIdentifier, false
	}
}

func lookupIdentifier(ident string) TokenType {
	if strings.HasPrefix(ident, ":") {
		return TokenValue
	}
	if tok, ok := lookupKeyword(ident); ok {
		return tok
	}
	upper := strings.ToUpper(ident)
	if tok, ok := lookupKeyword(upper); ok {
		return tok
	}

	return TokenIdentifier
}
