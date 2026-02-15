package expr

import (
	"errors"
	"fmt"
	"strconv"
)

// Sentinel errors for parser.
var (
	ErrExpectedRParen        = errors.New("expected ) after function args")
	ErrExpectedLParen        = errors.New("expected ( after function name")
	ErrExpectedIdentifierDot = errors.New("expected identifier after dot")
	ErrExpectedRBracket      = errors.New("expected ]")
	ErrExpectedANDInBetween  = errors.New("expected AND in BETWEEN")
	ErrExpectedLParenAfterIN = errors.New("expected ( after IN")
	ErrExpectedRParenAfterIN = errors.New("expected ) after IN candidates")
	ErrExpectedEqualInSET    = errors.New("expected = in SET")
	ErrUnexpectedToken       = errors.New("unexpected token")
	ErrUnexpectedOperand     = errors.New("unexpected operand token")
	ErrExpectedIndex         = errors.New("expected index")
	ErrExpectedRParen2       = errors.New("expected )")
)

type Parser struct {
	l         *Lexer
	curToken  Token
	peekToken Token
}

func NewParser(l *Lexer) *Parser {
	p := &Parser{l: l}
	p.nextToken()
	p.nextToken()

	return p
}

func (p *Parser) nextToken() {
	p.curToken = p.peekToken
	p.peekToken = p.l.NextToken()
}

func (p *Parser) curTokenIs(t TokenType) bool {
	return p.curToken.Type == t
}

func (p *Parser) peekTokenIs(t TokenType) bool {
	return p.peekToken.Type == t
}

func (p *Parser) expectPeek(t TokenType) bool {
	if p.peekTokenIs(t) {
		p.nextToken()

		return true
	}

	return false
}

// ParseCondition parses a ConditionExpression.
func (p *Parser) ParseCondition() (Node, error) {
	return p.parseExpression(0) // Start with lowest precedence
}

const (
	_ int = iota
	PrecOR
	PrecAND
	PrecNOT
	PrecComparison
)

func precedenceOf(t TokenType) int {
	switch t {
	case TokenOR:
		return PrecOR
	case TokenAND:
		return PrecAND
	case TokenEqual, TokenNotEqual, TokenLess, TokenLessEqual, TokenGreater, TokenGreaterEqual:
		return PrecComparison
	case TokenBETWEEN:
		return PrecComparison
	case TokenIN:
		return PrecComparison
	default:
		return 0
	}
}

func (p *Parser) curPrecedence() int {
	return precedenceOf(p.curToken.Type)
}

func (p *Parser) peekPrecedence() int {
	return precedenceOf(p.peekToken.Type)
}

func (p *Parser) parseExpression(precedence int) (Node, error) {
	var left Node
	var err error

	switch p.curToken.Type {
	case TokenNOT:
		left, err = p.parseNotExpr()
	case TokenLParen:
		left, err = p.parseGroupedExpr()
	case TokenIdentifier,
		TokenValue,
		TokenSize,
		TokenAttributeExists,
		TokenAttributeNotExists,
		TokenBeginsWith,
		TokenContains,
		TokenAttributeType:
		left, err = p.parseOperand()
	default:
		return nil, fmt.Errorf("%w %v at start of expression", ErrUnexpectedToken, p.curToken)
	}

	if err != nil {
		return nil, err
	}

	for !p.peekTokenIs(TokenEOF) && precedence < p.peekPrecedence() {
		p.nextToken()
		left, err = p.parseInfixExpr(left)
		if err != nil {
			return nil, err
		}
	}

	return left, nil
}

func (p *Parser) parseNotExpr() (Node, error) {
	p.nextToken() // consume NOT
	expr, err := p.parseExpression(PrecNOT)
	if err != nil {
		return nil, err
	}

	return &NotExpr{Expression: expr}, nil
}

func (p *Parser) parseGroupedExpr() (Node, error) {
	p.nextToken() // consume (
	expr, err := p.parseExpression(0)
	if err != nil {
		return nil, err
	}
	if !p.expectPeek(TokenRParen) {
		return nil, fmt.Errorf("%w, got %v", ErrExpectedRParen2, p.peekToken)
	}

	return expr, nil
}

func (p *Parser) parseOperand() (Node, error) {
	switch p.curToken.Type {
	case TokenSize, TokenAttributeExists, TokenAttributeNotExists, TokenBeginsWith, TokenContains, TokenAttributeType:
		return p.parseFunctionExpr()
	case TokenValue:
		return &ValuePlaceholder{Name: p.curToken.Literal}, nil
	case TokenIdentifier:
		return p.parsePathExpr()
	default:
		return nil, fmt.Errorf("%w %v", ErrUnexpectedOperand, p.curToken)
	}
}

func (p *Parser) parsePathExpr() (Node, error) {
	expr := &PathExpr{}

	// First segment
	expr.Elements = append(expr.Elements, PathElement{Type: ElementKey, Name: p.curToken.Literal})

	for p.peekTokenIs(TokenDot) || p.peekTokenIs(TokenLBracket) {
		p.nextToken()

		var parseErr error
		if p.curTokenIs(TokenDot) {
			parseErr = p.parseDotSegment(expr)
		} else {
			parseErr = p.parseBracketSegment(expr)
		}

		if parseErr != nil {
			return nil, parseErr
		}
	}

	return expr, nil
}

func (p *Parser) parseDotSegment(expr *PathExpr) error {
	if !p.expectPeek(TokenIdentifier) {
		return ErrExpectedIdentifierDot
	}
	expr.Elements = append(expr.Elements, PathElement{Type: ElementKey, Name: p.curToken.Literal})

	return nil
}

func (p *Parser) parseBracketSegment(expr *PathExpr) error {
	p.nextToken()
	if !p.curTokenIs(TokenIdentifier) { // Lexer treats numbers as Identifier for now if not carefully handled
		return fmt.Errorf("%w, got %v", ErrExpectedIndex, p.curToken)
	}
	idx, err := strconv.Atoi(p.curToken.Literal)
	if err != nil {
		return fmt.Errorf("invalid index: %w", err)
	}
	expr.Elements = append(expr.Elements, PathElement{Type: ElementIndex, Index: idx})
	if !p.expectPeek(TokenRBracket) {
		return ErrExpectedRBracket
	}

	return nil
}

func (p *Parser) parseFunctionExpr() (Node, error) {
	name := p.curToken.Literal
	if !p.expectPeek(TokenLParen) {
		// Functions must be followed by (
		// In some cases, keywords like size might be used as identifiers if not followed by (, but DynamoDB prefers quoting.
		// For now, let's assume if it's a function keyword, it must be a function.
		return nil, fmt.Errorf("expected ( after function %s: %w", name, ErrExpectedLParen)
	}

	p.nextToken() // consume (

	var args []Node
	if !p.curTokenIs(TokenRParen) {
		firstArg, err := p.parseExpression(0)
		if err != nil {
			return nil, err
		}
		args = append(args, firstArg)

		for p.peekTokenIs(TokenComma) {
			p.nextToken() // consume ,
			p.nextToken() // consume next start
			var nextArg Node
			nextArg, err = p.parseExpression(0)
			if err != nil {
				return nil, err
			}
			args = append(args, nextArg)
		}

		if !p.expectPeek(TokenRParen) {
			return nil, fmt.Errorf("%w, got %v", ErrExpectedRParen, p.peekToken)
		}
	}

	return &FunctionExpr{Name: name, Args: args}, nil
}

func (p *Parser) parseInfixExpr(left Node) (Node, error) {
	token := p.curToken
	precedence := p.curPrecedence()

	switch token.Type {
	case TokenAND, TokenOR:
		return p.parseLogicalInfix(left, token, precedence)
	case TokenBETWEEN:
		return p.parseBetweenInfix(left, precedence)
	case TokenIN:
		return p.parseInInfix(left)
	default:
		// Comparison operators
		p.nextToken()
		right, err := p.parseExpression(precedence)
		if err != nil {
			return nil, err
		}

		return &ComparisonExpr{Left: left, Operator: token.Type, Right: right}, nil
	}
}

func (p *Parser) parseLogicalInfix(left Node, token Token, precedence int) (Node, error) {
	p.nextToken()
	right, err := p.parseExpression(precedence)
	if err != nil {
		return nil, err
	}

	return &LogicalExpr{Left: left, Operator: token.Type, Right: right}, nil
}

func (p *Parser) parseBetweenInfix(left Node, precedence int) (Node, error) {
	p.nextToken()
	lower, err := p.parseExpression(precedence)
	if err != nil {
		return nil, err
	}
	if !p.expectPeek(TokenAND) {
		return nil, ErrExpectedANDInBetween
	}
	p.nextToken()
	upper, err := p.parseExpression(precedence)
	if err != nil {
		return nil, err
	}

	return &BetweenExpr{Value: left, Lower: lower, Upper: upper}, nil
}

func (p *Parser) parseInInfix(left Node) (Node, error) {
	// IN (v1, v2)
	if !p.expectPeek(TokenLParen) {
		return nil, ErrExpectedLParenAfterIN
	}
	p.nextToken()
	var candidates []Node
	for !p.curTokenIs(TokenRParen) {
		cand, err := p.parseExpression(0)
		if err != nil {
			return nil, err
		}
		candidates = append(candidates, cand)
		if p.peekTokenIs(TokenComma) {
			p.nextToken()
			p.nextToken()
		} else {
			break
		}
	}
	if !p.expectPeek(TokenRParen) {
		return nil, ErrExpectedRParenAfterIN
	}

	return &InExpr{Value: left, Candidates: candidates}, nil
}

// ParseUpdate parses an UpdateExpression.
func (p *Parser) ParseUpdate() (*UpdateExpr, error) {
	expr := &UpdateExpr{}

	for !p.curTokenIs(TokenEOF) {
		action := UpdateAction{Type: p.curToken.Type}
		switch p.curToken.Type {
		case TokenSET, TokenREMOVE, TokenADD, TokenDELETE:
			p.nextToken()
			for {
				item, err := p.parseUpdateItem(action.Type)
				if err != nil {
					return nil, err
				}
				action.Items = append(action.Items, item)
				if p.peekTokenIs(TokenComma) {
					p.nextToken()
					p.nextToken()
				} else {
					break
				}
			}
			expr.Actions = append(expr.Actions, action)
		default:
			return nil, fmt.Errorf("%w %v in update expression", ErrUnexpectedToken, p.curToken)
		}

		p.nextToken()
	}

	return expr, nil
}

func (p *Parser) parseUpdateItem(actionType TokenType) (UpdateItem, error) {
	path, err := p.parsePathExpr()
	if err != nil {
		return UpdateItem{}, err
	}

	item := UpdateItem{Path: path}

	switch actionType {
	case TokenSET:
		if !p.expectPeek(TokenEqual) {
			return UpdateItem{}, ErrExpectedEqualInSET
		}
		p.nextToken()
		val, setErr := p.parseExpression(0) // Right side can be path or value or size() or atomic addition
		if setErr != nil {
			return UpdateItem{}, setErr
		}
		item.Value = val
	case TokenADD, TokenDELETE:
		// ADD path value
		p.nextToken()
		val, addErr := p.parseOperand()
		if addErr != nil {
			return UpdateItem{}, addErr
		}
		item.Value = val
	default:
		// TokenREMOVE: no value needed
	}

	return item, nil
}

// ParseProjection parses a ProjectionExpression.
func (p *Parser) ParseProjection() (*ProjectionExpr, error) {
	expr := &ProjectionExpr{}
	for {
		path, err := p.parsePathExpr()
		if err != nil {
			return nil, err
		}
		expr.Paths = append(expr.Paths, path)
		if p.peekTokenIs(TokenComma) {
			p.nextToken()
			p.nextToken()
		} else {
			break
		}
	}

	return expr, nil
}
