package timestreamquery

import (
	"fmt"
	"strings"
	"unicode"
)

func isStatementKeyword(w string) bool {
	switch strings.ToUpper(w) {
	case "SELECT", "WITH", "SHOW", "DESCRIBE", "EXPLAIN":
		return true
	default:
		return false
	}
}

// validateQuerySyntax rejects queries that fail trivially: unknown leading keyword, unbalanced
// parentheses or an unterminated string literal. It is not a SQL parser.
func validateQuerySyntax(q string) error {
	s := strings.TrimLeftFunc(q, func(r rune) bool { return unicode.IsSpace(r) || r == '(' })
	if s == "" {
		return fmt.Errorf("%w: line 1:1: mismatched input '<EOF>'", ErrValidation)
	}

	end := strings.IndexFunc(s, func(r rune) bool { return !unicode.IsLetter(r) && r != '_' })
	if end < 0 {
		end = len(s)
	}

	word := s[:end]
	if !isStatementKeyword(word) {
		tok := word
		if tok == "" {
			tok = s[:1]
		}

		return fmt.Errorf(
			"%w: line 1:%d: mismatched input '%s'. Expecting: 'DESCRIBE', 'EXPLAIN', 'SELECT', 'SHOW', 'WITH'",
			ErrValidation, len(q)-len(s)+1, tok,
		)
	}

	return checkBalanced(q)
}

func checkBalanced(q string) error {
	depth := 0
	quote := rune(0)

	for _, r := range q {
		switch {
		case quote != 0:
			if r == quote {
				quote = 0
			}
		case r == '\'' || r == '"':
			quote = r
		case r == '(':
			depth++
		case r == ')':
			depth--
			if depth < 0 {
				return fmt.Errorf("%w: line 1:1: mismatched input ')'", ErrValidation)
			}
		}
	}

	if quote != 0 {
		return fmt.Errorf("%w: line 1:1: unterminated quoted literal", ErrValidation)
	}

	if depth != 0 {
		return fmt.Errorf("%w: line 1:1: mismatched input '<EOF>'. Expecting: ')'", ErrValidation)
	}

	return nil
}
