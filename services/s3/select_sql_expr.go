package s3

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// sqlLiteral represents a constant value.
type sqlLiteral struct {
	val     string
	numeric bool
	null    bool
}

func (l *sqlLiteral) eval(_ sqlRow) (any, error) {
	if l.null {
		return sqlNullValue, nil
	}

	return l.val, nil
}

// sqlColumnRef is a reference to a column by name or positional _N.
type sqlColumnRef struct {
	name string
}

func (c *sqlColumnRef) eval(row sqlRow) (any, error) {
	// Support positional _1, _2, etc. for string rows.
	if val, found := evalPositionalRef(c.name, row); found {
		return val, nil
	}

	val, _ := row.field(c.name)

	return val, nil
}

// evalPositionalRef handles positional column references like _1, _2, etc.
// Returns (value, true) if the name is a valid positional reference for a stringRow, otherwise (_, false).
func evalPositionalRef(name string, row sqlRow) (string, bool) {
	if len(name) < 2 || name[0] != '_' {
		return "", false
	}

	sr, ok := row.(*stringRow)
	if !ok {
		return "", false
	}

	idx, err := strconv.Atoi(name[1:])
	if err != nil || idx < 1 {
		return "", false
	}

	keys := sortedKeys(sr.data)
	if idx-1 >= len(keys) {
		return "", true // out-of-range positional: return empty (found=true to skip field lookup)
	}

	return sr.data[keys[idx-1]], true
}

// sqlStarExpr represents SELECT *.
type sqlStarExpr struct{}

func (s *sqlStarExpr) eval(_ sqlRow) (any, error) {
	return "*", nil
}

// sqlBinaryExpr handles comparison and logical operators.
type sqlBinaryExpr struct {
	left, right sqlExpr
	op          string
}

func (b *sqlBinaryExpr) eval(row sqlRow) (any, error) {
	left, leftErr := b.left.eval(row)
	if leftErr != nil {
		return nil, leftErr
	}

	switch strings.ToUpper(b.op) {
	case "AND":
		if !isTruthy(left) {
			return false, nil
		}

		right, rightErr := b.right.eval(row)
		if rightErr != nil {
			return nil, rightErr
		}

		return isTruthy(right), nil

	case "OR":
		if isTruthy(left) {
			return true, nil
		}

		right, rightErr := b.right.eval(row)
		if rightErr != nil {
			return nil, rightErr
		}

		return isTruthy(right), nil
	}

	right, rightErr := b.right.eval(row)
	if rightErr != nil {
		return nil, rightErr
	}

	return compareSQLValues(b.op, left, right)
}

// sqlNotExpr negates a boolean expression.
type sqlNotExpr struct {
	inner sqlExpr
}

func (n *sqlNotExpr) eval(row sqlRow) (any, error) {
	val, err := n.inner.eval(row)
	if err != nil {
		return nil, err
	}

	return !isTruthy(val), nil
}

// sqlIsNullExpr checks for NULL.
type sqlIsNullExpr struct {
	inner   sqlExpr
	notNull bool
}

func (n *sqlIsNullExpr) eval(row sqlRow) (any, error) {
	val, err := n.inner.eval(row)
	if err != nil {
		return nil, err
	}

	_, isNullType := val.(sqlNullType)
	isNull := val == nil || val == "" || isNullType

	if n.notNull {
		return !isNull, nil
	}

	return isNull, nil
}

// sqlLikeExpr implements SQL LIKE pattern matching.
type sqlLikeExpr struct {
	left    sqlExpr
	pattern string
}

func (l *sqlLikeExpr) eval(row sqlRow) (any, error) {
	val, err := l.left.eval(row)
	if err != nil {
		return nil, err
	}

	return sqlLikeMatch(l.pattern, fmt.Sprintf("%v", val)), nil
}

// sqlCastExpr casts a value to a target type.
type sqlCastExpr struct {
	inner    sqlExpr
	castType string
}

func (c *sqlCastExpr) eval(row sqlRow) (any, error) {
	val, err := c.inner.eval(row)
	if err != nil {
		return nil, err
	}

	return castSQLValue(fmt.Sprintf("%v", val), c.castType)
}

func castSQLValue(s, castType string) (string, error) {
	switch castType {
	case "INT", "INTEGER", "BIGINT", "SMALLINT":
		n, convErr := strconv.ParseFloat(s, 64)
		if convErr != nil {
			return "", convErr
		}

		return strconv.FormatInt(int64(n), 10), nil

	case "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "REAL":
		return s, nil

	case "STRING", "CHAR", "VARCHAR", "TEXT":
		return s, nil

	case "BOOL", "BOOLEAN":
		const sTrue = "true"
		switch strings.ToLower(s) {
		case sTrue, "1", "yes":
			return sTrue, nil
		default:
			return "false", nil
		}

	case "TIMESTAMP":
		return s, nil

	default:
		return s, nil
	}
}

// sqlBetweenExpr handles BETWEEN ... AND ...
type sqlBetweenExpr struct {
	val, low, high sqlExpr
}

func (b *sqlBetweenExpr) eval(row sqlRow) (any, error) {
	val, err := b.val.eval(row)
	if err != nil {
		return nil, err
	}

	low, err := b.low.eval(row)
	if err != nil {
		return nil, err
	}

	high, err := b.high.eval(row)
	if err != nil {
		return nil, err
	}

	ge, err := compareSQLValues(">=", val, low)
	if err != nil {
		return nil, err
	}

	le, err := compareSQLValues("<=", val, high)
	if err != nil {
		return nil, err
	}

	return isTruthy(ge) && isTruthy(le), nil
}

// sqlInExpr handles IN (...)
type sqlInExpr struct {
	val   sqlExpr
	items []sqlExpr
}

func (n *sqlInExpr) eval(row sqlRow) (any, error) {
	val, err := n.val.eval(row)
	if err != nil {
		return nil, err
	}

	for _, item := range n.items {
		itemVal, itemErr := item.eval(row)
		if itemErr != nil {
			return nil, itemErr
		}

		eq, eqErr := compareSQLValues("=", val, itemVal)
		if eqErr != nil {
			return nil, eqErr
		}

		if isTruthy(eq) {
			return true, nil
		}
	}

	return false, nil
}

// isTruthy converts a value to boolean.
func isTruthy(v any) bool {
	if v == nil {
		return false
	}

	switch val := v.(type) {
	case bool:
		return val
	case string:
		switch strings.ToLower(val) {
		case "", "false", "0":
			return false
		default:
			return true
		}
	case float64:
		return val != 0
	case int64:
		return val != 0
	case sqlNullType:
		return false
	default:
		return false
	}
}

// compareSQLValues compares two values with a SQL operator.
// Both numeric and string comparisons are supported.
func compareSQLValues(op string, left, right any) (bool, error) {
	if left == nil || right == nil {
		return false, nil
	}

	// SQL NULL comparisons always return false.
	if _, ok := left.(sqlNullType); ok {
		return false, nil
	}

	if _, ok := right.(sqlNullType); ok {
		return false, nil
	}

	ls := fmt.Sprintf("%v", left)
	rs := fmt.Sprintf("%v", right)

	// Try numeric comparison first.
	ln, leftNumErr := strconv.ParseFloat(ls, 64)
	rn, rightNumErr := strconv.ParseFloat(rs, 64)

	if leftNumErr == nil && rightNumErr == nil {
		return compareFloat(op, ln, rn)
	}

	return compareString(op, ls, rs)
}

func compareFloat(op string, l, r float64) (bool, error) {
	const eps = 1e-12

	switch op {
	case "=":
		return math.Abs(l-r) < eps, nil
	case "!=", "<>":
		return math.Abs(l-r) >= eps, nil
	case "<":
		return l < r, nil
	case "<=":
		return l <= r, nil
	case ">":
		return l > r, nil
	case ">=":
		return l >= r, nil
	default:
		return false, fmt.Errorf("unknown operator %q: %w", op, errUnknownOperator)
	}
}

func compareString(op string, l, r string) (bool, error) {
	cmp := strings.Compare(l, r)

	switch op {
	case "=":
		return cmp == 0, nil
	case "!=", "<>":
		return cmp != 0, nil
	case "<":
		return cmp < 0, nil
	case "<=":
		return cmp <= 0, nil
	case ">":
		return cmp > 0, nil
	case ">=":
		return cmp >= 0, nil
	default:
		return false, fmt.Errorf("unknown operator %q: %w", op, errUnknownOperator)
	}
}

// sqlLikeMatch matches a string against an SQL LIKE pattern.
// % matches any sequence of characters, _ matches a single character.
func sqlLikeMatch(pattern, s string) bool {
	return likeMatch(pattern, s)
}

// likeMatch performs SQL LIKE pattern matching in linear time.
// % matches any sequence of characters, _ matches any single character.
func likeMatch(pattern, s string) bool {
	pLen := len(pattern)
	sLen := len(s)

	p := 0
	i := 0
	starIdx := -1
	match := 0

	for i < sLen {
		if p < pLen && (pattern[p] == s[i] || pattern[p] == '_') {
			i++
			p++

			continue
		}

		if p < pLen && pattern[p] == '%' {
			starIdx = p
			match = i
			p++

			continue
		}

		if starIdx != -1 {
			p = starIdx + 1
			match++
			i = match

			continue
		}

		return false
	}

	for p < pLen && pattern[p] == '%' {
		p++
	}

	return p == pLen
}

// sqlAggFuncName identifies a SQL aggregate function.
type sqlAggFuncName string

const (
	aggFuncCount sqlAggFuncName = "COUNT"
	aggFuncSum   sqlAggFuncName = "SUM"
	aggFuncAvg   sqlAggFuncName = "AVG"
	aggFuncMin   sqlAggFuncName = "MIN"
	aggFuncMax   sqlAggFuncName = "MAX"
)

var errAggPerRow = errors.New("aggregate must be evaluated across all rows, not per-row")

// sqlAggExpr represents a SQL aggregate function call (COUNT, SUM, AVG, MIN, MAX).
// arg is nil for COUNT(*).
type sqlAggExpr struct {
	arg sqlExpr
	fn  sqlAggFuncName
}

func (a *sqlAggExpr) eval(_ sqlRow) (any, error) {
	return nil, fmt.Errorf("aggregate %s: %w", a.fn, errAggPerRow)
}
