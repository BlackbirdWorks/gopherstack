package expr

import (
	"fmt"
	"strings"
)

type TokenType int

const (
	TokenEOF TokenType = iota
	TokenError

	// TokenIdentifier represents identifiers and placeholders
	TokenIdentifier // #name or pk or info.tags
	TokenValue      // :val

	// TokenEqual represents operators
	TokenEqual        // =
	TokenNotEqual     // <>
	TokenLess         // <
	TokenGreater      // >
	TokenLessEqual    // <=
	TokenGreaterEqual // >=
	TokenLParen       // (
	TokenRParen       // )
	TokenComma        // ,
	TokenDot          // .
	TokenLBracket     // [
	TokenRBracket     // ]

	// TokenAND represents keywords
	TokenAND
	TokenOR
	TokenNOT
	TokenBETWEEN
	TokenIN
	TokenSET
	TokenREMOVE
	TokenADD
	TokenDELETE

	// TokenSize represents function keywords
	TokenSize
	TokenAttributeExists
	TokenAttributeNotExists
	TokenBeginsWith
	TokenContains
	TokenAttributeType
)

type Token struct {
	Type    TokenType
	Literal string
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
		if l.peekChar() == '>' {
			l.readChar()
			tok = Token{Type: TokenNotEqual, Literal: "<>"}
		} else if l.peekChar() == '=' {
			l.readChar()
			tok = Token{Type: TokenLessEqual, Literal: "<="}
		} else {
			tok = Token{Type: TokenLess, Literal: "<"}
		}
	case '>':
		if l.peekChar() == '=' {
			l.readChar()
			tok = Token{Type: TokenGreaterEqual, Literal: ">="}
		} else {
			tok = Token{Type: TokenGreater, Literal: ">"}
		}
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
		if isLetter(l.ch) || l.ch == '#' || l.ch == ':' {
			literal := l.readIdentifier()
			tok.Literal = literal
			tok.Type = lookupIdentifier(literal)
			return tok
		} else if isDigit(l.ch) {
			tok.Literal = l.readNumber()
			tok.Type = TokenIdentifier // Numbers in DynamoDB expressions are usually part of paths or literals?
			// Actually, literals are always placeholders like :val.
			// But indices are numbers: list[0]
			return tok
		}

		tok = Token{Type: TokenError, Literal: string(l.ch)}
	}

	l.readChar()
	return tok
}

func (l *Lexer) readIdentifier() string {
	position := l.position
	for isLetter(l.ch) || isDigit(l.ch) || l.ch == '_' || l.ch == '#' || l.ch == ':' || l.ch == '.' {
		// Note: '.' is allowed in paths like 'info.tags', but technically it's a separator.
		// However, for simplicity, we might want to lex 'info.tags' as multiple tokens or one?
		// Standard SQL lexers treat '.' as a separator.
		// Let's treat it as a separator.
		if l.ch == '.' {
			break
		}
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

var keywords = map[string]TokenType{
	"AND":                  TokenAND,
	"OR":                   TokenOR,
	"NOT":                  TokenNOT,
	"BETWEEN":              TokenBETWEEN,
	"IN":                   TokenIN,
	"SET":                  TokenSET,
	"REMOVE":               TokenREMOVE,
	"ADD":                  TokenADD,
	"DELETE":               TokenDELETE,
	"size":                 TokenSize,
	"attribute_exists":     TokenAttributeExists,
	"attribute_not_exists": TokenAttributeNotExists,
	"begins_with":          TokenBeginsWith,
	"contains":             TokenContains,
	"attribute_type":       TokenAttributeType,
}

func lookupIdentifier(ident string) TokenType {
	if strings.HasPrefix(ident, ":") {
		return TokenValue
	}
	if tok, ok := keywords[ident]; ok {
		return tok
	}
	if strings.HasPrefix(ident, "AND") || strings.HasPrefix(ident, "OR") || strings.HasPrefix(ident, "NOT") {
		// Handle potential case issues or spaces? No, keywords should match exactly.
		// Wait, DynamoDB keywords are case-insensitive? Most SQL is.
		// Actually, DynamoDB docs show them in upper case but let's be safe.
		upper := strings.ToUpper(ident)
		if tok, ok := keywords[upper]; ok {
			return tok
		}
	}

	return TokenIdentifier
}
