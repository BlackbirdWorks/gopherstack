package expr

import (
	"fmt"
	"strconv"
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

// ParseCondition parses a ConditionExpression
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

var precedences = map[TokenType]int{
	TokenOR:           PrecOR,
	TokenAND:          PrecAND,
	TokenEqual:        PrecComparison,
	TokenNotEqual:     PrecComparison,
	TokenLess:         PrecComparison,
	TokenLessEqual:    PrecComparison,
	TokenGreater:      PrecComparison,
	TokenGreaterEqual: PrecComparison,
	TokenBETWEEN:      PrecComparison,
	TokenIN:           PrecComparison,
}

func (p *Parser) curPrecedence() int {
	if p, ok := precedences[p.curToken.Type]; ok {
		return p
	}
	return 0
}

func (p *Parser) peekPrecedence() int {
	if p, ok := precedences[p.peekToken.Type]; ok {
		return p
	}
	return 0
}

func (p *Parser) parseExpression(precedence int) (Node, error) {
	var left Node
	var err error

	switch p.curToken.Type {
	case TokenNOT:
		left, err = p.parseNotExpr()
	case TokenLParen:
		left, err = p.parseGroupedExpr()
	case TokenIdentifier, TokenValue, TokenSize, TokenAttributeExists, TokenAttributeNotExists, TokenBeginsWith, TokenContains, TokenAttributeType:
		left, err = p.parseOperand()
	default:
		return nil, fmt.Errorf("unexpected token %v at start of expression", p.curToken)
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
		return nil, fmt.Errorf("expected ), got %v", p.peekToken)
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
		return nil, fmt.Errorf("unexpected operand token %v", p.curToken)
	}
}

func (p *Parser) parsePathExpr() (Node, error) {
	expr := &PathExpr{}

	// First segment
	expr.Elements = append(expr.Elements, PathElement{Type: ElementKey, Name: p.curToken.Literal})

	for p.peekTokenIs(TokenDot) || p.peekTokenIs(TokenLBracket) {
		p.nextToken()
		if p.curTokenIs(TokenDot) {
			if !p.expectPeek(TokenIdentifier) {
				return nil, fmt.Errorf("expected identifier after .")
			}
			expr.Elements = append(expr.Elements, PathElement{Type: ElementKey, Name: p.curToken.Literal})
		} else if p.curTokenIs(TokenLBracket) {
			p.nextToken()
			if !p.curTokenIs(TokenIdentifier) { // Lexer treats numbers as Identifier for now if not carefully handled
				return nil, fmt.Errorf("expected index, got %v", p.curToken)
			}
			idx, err := strconv.Atoi(p.curToken.Literal)
			if err != nil {
				return nil, fmt.Errorf("invalid index: %v", err)
			}
			expr.Elements = append(expr.Elements, PathElement{Type: ElementIndex, Index: idx})
			if !p.expectPeek(TokenRBracket) {
				return nil, fmt.Errorf("expected ]")
			}
		}
	}

	return expr, nil
}

func (p *Parser) parseFunctionExpr() (Node, error) {
	name := p.curToken.Literal
	if !p.expectPeek(TokenLParen) {
		// Functions must be followed by (
		// In some cases, keywords like size might be used as identifiers if not followed by (, but DynamoDB prefers quoting.
		// For now, let's assume if it's a function keyword, it must be a function.
		return nil, fmt.Errorf("expected ( after function %s", name)
	}

	p.nextToken() // consume (

	var args []Node
	if !p.curTokenIs(TokenRParen) {
		arg, err := p.parseExpression(0)
		if err != nil {
			return nil, err
		}
		args = append(args, arg)

		for p.peekTokenIs(TokenComma) {
			p.nextToken() // consume ,
			p.nextToken() // consume next start
			arg, err := p.parseExpression(0)
			if err != nil {
				return nil, err
			}
			args = append(args, arg)
		}

		if !p.expectPeek(TokenRParen) {
			return nil, fmt.Errorf("expected ) after function args, got %v", p.peekToken)
		}
	}

	return &FunctionExpr{Name: name, Args: args}, nil
}

func (p *Parser) parseInfixExpr(left Node) (Node, error) {
	token := p.curToken
	precedence := p.curPrecedence()

	switch token.Type {
	case TokenAND, TokenOR:
		p.nextToken()
		right, err := p.parseExpression(precedence)
		if err != nil {
			return nil, err
		}
		return &LogicalExpr{Left: left, Operator: token.Type, Right: right}, nil
	case TokenBETWEEN:
		p.nextToken()
		lower, err := p.parseExpression(precedence)
		if err != nil {
			return nil, err
		}
		if !p.expectPeek(TokenAND) {
			return nil, fmt.Errorf("expected AND in BETWEEN")
		}
		p.nextToken()
		upper, err := p.parseExpression(precedence)
		if err != nil {
			return nil, err
		}
		return &BetweenExpr{Value: left, Lower: lower, Upper: upper}, nil
	case TokenIN:
		// IN (v1, v2)
		if !p.expectPeek(TokenLParen) {
			return nil, fmt.Errorf("expected ( after IN")
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
			return nil, fmt.Errorf("expected ) after IN candidates")
		}
		return &InExpr{Value: left, Candidates: candidates}, nil
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

// ParseUpdate parses an UpdateExpression
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
			return nil, fmt.Errorf("unexpected token %v in update expression", p.curToken)
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

	if actionType == TokenSET {
		if !p.expectPeek(TokenEqual) {
			return UpdateItem{}, fmt.Errorf("expected = in SET")
		}
		p.nextToken()
		val, err := p.parseExpression(0) // Right side can be path or value or size() or atomic addition
		if err != nil {
			return UpdateItem{}, err
		}
		item.Value = val
	} else if actionType == TokenADD || actionType == TokenDELETE {
		// ADD path value
		p.nextToken()
		val, err := p.parseOperand()
		if err != nil {
			return UpdateItem{}, err
		}
		item.Value = val
	}

	return item, nil
}

// ParseProjection parses a ProjectionExpression
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
