package expr

import (
	"errors"
	"fmt"
	"strings"
)

// ErrReservedWord identifies a *ReservedWordError via errors.Is/errors.As. Its
// own text is lowercase Go style; the AWS-facing wording lives on
// ReservedWordError.Error() instead -- see that doc comment.
var ErrReservedWord = errors.New("reserved keyword")

// ReservedWordError carries the offending bare attribute name, preserving its
// original case as written in the expression. Its Error() text matches
// DynamoDB's observed runtime wording, e.g. "Invalid KeyConditionExpression:
// Attribute name is a reserved keyword; reserved keyword: hash" -- confirmed
// via https://github.com/aws/aws-sdk-php/issues/1233. The developer guide
// (ReservedWords.html, Expressions.ExpressionAttributeNames.html) documents
// the underlying rule but not this verbatim ValidationException string.
type ReservedWordError struct {
	Word string
}

func (e *ReservedWordError) Error() string {
	return fmt.Sprintf("Attribute name is a reserved keyword; reserved keyword: %s", e.Word)
}

func (e *ReservedWordError) Unwrap() error {
	return ErrReservedWord
}

// IsReservedWord reports whether name is a DynamoDB reserved word. Matching is
// case-insensitive, per ReservedWords.html ("This list isn't case-sensitive.").
func IsReservedWord(name string) bool {
	_, ok := reservedWords[strings.ToUpper(name)]

	return ok
}

// CheckReservedWords walks a parsed expression AST and returns a
// *ReservedWordError for the first bare (non-#placeholder) document-path
// segment that collides with a reserved word. Every path segment is checked
// independently, so a nested path like a.SIZE is rejected exactly like a
// top-level SIZE would be -- DynamoDB requires an expression attribute name
// alias at whichever path level the reserved word appears (see
// Expressions.ExpressionAttributeNames.html's "Nested attributes" guidance to
// alias each element in the document path).
func CheckReservedWords(node Node) error {
	switch v := node.(type) {
	case nil:
		return nil
	case *LogicalExpr:
		return firstErr(CheckReservedWords(v.Left), CheckReservedWords(v.Right))
	case *NotExpr:
		return CheckReservedWords(v.Expression)
	case *ComparisonExpr:
		return firstErr(CheckReservedWords(v.Left), CheckReservedWords(v.Right))
	case *BetweenExpr:
		return firstErr(
			CheckReservedWords(v.Value),
			CheckReservedWords(v.Lower),
			CheckReservedWords(v.Upper),
		)
	case *InExpr:
		return checkReservedWordsInExpr(v)
	case *FunctionExpr:
		return checkReservedWordsList(v.Args)
	case *PathExpr:
		return checkReservedWordsPath(v)
	case *ValuePlaceholder:
		return nil
	case *UpdateExpr:
		return checkReservedWordsUpdate(v)
	case *ProjectionExpr:
		return checkReservedWordsList(v.Paths)
	default:
		return nil
	}
}

func checkReservedWordsInExpr(v *InExpr) error {
	if err := CheckReservedWords(v.Value); err != nil {
		return err
	}

	return checkReservedWordsList(v.Candidates)
}

func checkReservedWordsList(nodes []Node) error {
	for _, n := range nodes {
		if err := CheckReservedWords(n); err != nil {
			return err
		}
	}

	return nil
}

func checkReservedWordsUpdate(v *UpdateExpr) error {
	for _, action := range v.Actions {
		for _, item := range action.Items {
			if err := firstErr(CheckReservedWords(item.Path), CheckReservedWords(item.Value)); err != nil {
				return err
			}
		}
	}

	return nil
}

func checkReservedWordsPath(p *PathExpr) error {
	for _, el := range p.Elements {
		if el.Type != ElementKey {
			continue
		}
		if strings.HasPrefix(el.Name, "#") {
			continue
		}
		if IsReservedWord(el.Name) {
			return &ReservedWordError{Word: el.Name}
		}
	}

	return nil
}

// firstErr returns the first non-nil error among errs, or nil.
func firstErr(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}

	return nil
}
