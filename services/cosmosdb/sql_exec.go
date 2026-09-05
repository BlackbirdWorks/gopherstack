package cosmosdb

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// sql_exec.go executes a parsed sqlSelectStmt (see sql_parser.go) against a
// container's documents. WHERE evaluation never panics, matching
// services/azuretable's odata_filter_eval.go's evaluate-never-panic
// posture, but -- unlike that file's simpler two-valued "missing field
// means false" model -- implements real Cosmos SQL's three-valued logic
// (sqlTriState: true/false/undefined). A missing document field (or an
// unbound @param) is Undefined, NOT false: Undefined is not the same value
// as false, and critically does not flip to true under NOT (`NOT
// (c.missing = 1)` must still not match -- if a missing comparison were
// modeled as a plain false, NOT would incorrectly turn it into a match).
// AND/OR follow standard SQL 3VL (undefined AND false = false; undefined OR
// true = true; anything else touching undefined stays undefined). Only a
// definite sqlTrue at the top level counts as a row match -- see
// ExecuteQuery. IS NULL/IS NOT NULL are the one place this emulator
// deliberately does NOT propagate Undefined: both evaluate to a definite
// boolean false against an undefined operand (real Cosmos's own behavior),
// since undefined is neither "null" nor meaningfully "not null" for what
// these operators test.

// QueryParameter is one "@name"/value binding from a query request body's
// "parameters" array.
type QueryParameter struct {
	Value any    `json:"value"`
	Name  string `json:"name"`
}

// ExecuteQuery parses and runs a Cosmos SQL query against docs, returning
// the projected result rows in Cosmos's own {"Documents": [...]} response
// shape (see document_ops.go's queryDocuments). Never panics: a malformed
// query returns (nil, ErrQueryParse-wrapped error); a runtime type mismatch
// during WHERE evaluation simply excludes that document, exactly like
// services/azuretable's EvaluateFilter.
func ExecuteQuery(query string, params []QueryParameter, docs []DocumentInfo) ([]map[string]any, error) {
	stmt, err := ParseQuery(query)
	if err != nil {
		return nil, err
	}

	paramValues := make(map[string]any, len(params))
	for _, p := range params {
		paramValues[p.Name] = p.Value
	}

	rows := make([]map[string]any, 0, len(docs))

	for _, doc := range docs {
		row := documentAsMap(doc)

		// Only a definite sqlTrue counts as a match: both sqlFalse and
		// sqlUndefined exclude the row (see this file's top doc comment on
		// three-valued logic).
		if stmt.where != nil && evalSQLNode(stmt.where, row, paramValues) != sqlTrue {
			continue
		}

		rows = append(rows, row)
	}

	sortRows(rows, stmt.orderBy)

	rows = applyOffsetAndLimit(rows, stmt)

	return projectRows(rows, stmt), nil
}

// applyOffsetAndLimit applies TOP or OFFSET/LIMIT (mutually exclusive in
// real Cosmos SQL; if a query somehow specified both, this emulator applies
// OFFSET/LIMIT and ignores TOP -- an ambiguous case with no real-world
// occurrence worth erroring over) to the already-filtered, already-sorted
// row set.
func applyOffsetAndLimit(rows []map[string]any, stmt *sqlSelectStmt) []map[string]any {
	if stmt.hasOffset || stmt.hasLimit {
		offset := min(stmt.offset, len(rows))

		rows = rows[offset:]

		if stmt.hasLimit && stmt.limit < len(rows) {
			rows = rows[:stmt.limit]
		}

		return rows
	}

	if stmt.hasTop && stmt.top < len(rows) {
		rows = rows[:stmt.top]
	}

	return rows
}

// projectRows applies stmt's SELECT list to rows: SELECT * returns rows
// unchanged; an explicit projection list rebuilds each row with only the
// requested paths, under their (possibly AS-aliased) output keys.
func projectRows(rows []map[string]any, stmt *sqlSelectStmt) []map[string]any {
	if stmt.isStar {
		return rows
	}

	out := make([]map[string]any, len(rows))

	for i, row := range rows {
		projected := make(map[string]any, len(stmt.projections))

		for _, proj := range stmt.projections {
			if v, ok := resolvePath(row, proj.path); ok {
				projected[proj.alias] = v
			}
		}

		out[i] = projected
	}

	return out
}

// sortRows sorts rows in place per orderKeys, evaluated left-to-right (a
// tie on the first key falls through to the next). A row missing a sort
// key's field sorts before one that has it, so ordering is total and
// deterministic even against heterogeneous documents.
func sortRows(rows []map[string]any, orderKeys []sqlOrderKey) {
	if len(orderKeys) == 0 {
		return
	}

	sort.SliceStable(rows, func(i, j int) bool {
		return rowLess(rows[i], rows[j], orderKeys)
	})
}

func rowLess(a, b map[string]any, orderKeys []sqlOrderKey) bool {
	for _, key := range orderKeys {
		av, aok := resolvePath(a, key.path)
		bv, bok := resolvePath(b, key.path)

		cmp, ok := compareOrdered(av, aok, bv, bok)
		if !ok || cmp == 0 {
			continue
		}

		if key.desc {
			return cmp > 0
		}

		return cmp < 0
	}

	return false
}

// compareOrdered compares two ORDER BY key values, returning ok=false when
// they are incomparable (mismatched types) so rowLess falls through to the
// next key instead of asserting an arbitrary order.
func compareOrdered(a any, aok bool, b any, bok bool) (int, bool) {
	switch {
	case !aok && !bok:
		return 0, false
	case !aok:
		return -1, true
	case !bok:
		return 1, true
	default:
		return compareValuesOrdered(a, b)
	}
}

func compareValuesOrdered(a, b any) (int, bool) {
	switch av := a.(type) {
	case json.Number:
		bv, ok := b.(json.Number)
		if !ok {
			return 0, false
		}

		return compareJSONNumbers(av, bv), true
	case string:
		bv, ok := b.(string)
		if !ok {
			return 0, false
		}

		return strings.Compare(av, bv), true
	case bool:
		bv, ok := b.(bool)
		if !ok {
			return 0, false
		}

		return cmpBool(av, bv), true
	default:
		return 0, false
	}
}

// resolvePath navigates row along path's segments (e.g. ["a","b"] ->
// row["a"].(map[string]any)["b"]), mirroring store.go's
// extractPartitionKeyValue. An empty path (a bare alias reference, e.g.
// "SELECT c FROM c") resolves to the whole row.
func resolvePath(row map[string]any, path []string) (any, bool) {
	var cur any = row

	for _, seg := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil, false
		}

		v, ok := m[seg]
		if !ok {
			return nil, false
		}

		cur = v
	}

	return cur, true
}

// sqlTriState is Cosmos SQL's three-valued logic result: true, false, or
// undefined (the result of evaluating an expression against a field that
// doesn't exist, or an unbound @param). See this file's top doc comment.
type sqlTriState int

const (
	sqlUndefined sqlTriState = iota
	sqlFalse
	sqlTrue
)

// boolToTri lifts a definite Go bool into sqlTriState. Never returns
// sqlUndefined -- callers that need to produce Undefined do so explicitly.
func boolToTri(b bool) sqlTriState {
	if b {
		return sqlTrue
	}

	return sqlFalse
}

// triAnd implements SQL 3VL's AND truth table: false in either operand
// forces false (even against undefined -- false is "stronger" than
// undefined); true in both operands is required for true; anything else
// (undefined paired with true, or undefined paired with undefined) stays
// undefined.
func triAnd(a, b sqlTriState) sqlTriState {
	if a == sqlFalse || b == sqlFalse {
		return sqlFalse
	}

	if a == sqlTrue && b == sqlTrue {
		return sqlTrue
	}

	return sqlUndefined
}

// triOr implements SQL 3VL's OR truth table: true in either operand forces
// true; false in both operands is required for false; anything else stays
// undefined.
func triOr(a, b sqlTriState) sqlTriState {
	if a == sqlTrue || b == sqlTrue {
		return sqlTrue
	}

	if a == sqlFalse && b == sqlFalse {
		return sqlFalse
	}

	return sqlUndefined
}

// triNot implements SQL 3VL's NOT: true and false invert as usual, but
// undefined stays undefined -- it never flips to true. This is the crux of
// the bug this tri-state model fixes: "NOT (c.missing = 1)" must still
// exclude the row, not match it just because negating the inner
// comparison's (incorrect, two-valued) false would otherwise yield true.
func triNot(a sqlTriState) sqlTriState {
	switch a {
	case sqlTrue:
		return sqlFalse
	case sqlFalse:
		return sqlTrue
	default:
		return sqlUndefined
	}
}

// evalSQLNode evaluates node against row under Cosmos SQL's three-valued
// logic, resolving @param references against paramValues. Never panics --
// see this file's top doc comment.
func evalSQLNode(node sqlNode, row map[string]any, paramValues map[string]any) sqlTriState {
	switch n := node.(type) {
	case *sqlAndNode:
		return triAnd(evalSQLNode(n.left, row, paramValues), evalSQLNode(n.right, row, paramValues))
	case *sqlOrNode:
		return triOr(evalSQLNode(n.left, row, paramValues), evalSQLNode(n.right, row, paramValues))
	case *sqlNotNode:
		return triNot(evalSQLNode(n.expr, row, paramValues))
	case *sqlCmpNode:
		return evalSQLComparison(n, row, paramValues)
	case *sqlIsNullNode:
		return evalSQLIsNull(n, row, paramValues)
	default:
		return sqlFalse
	}
}

// evalSQLIsNull evaluates IS NULL / IS NOT NULL. Deliberately never returns
// sqlUndefined: against an undefined operand (a missing field or unbound
// param), BOTH "IS NULL" and "IS NOT NULL" evaluate to a definite false --
// matching real Cosmos SQL, where undefined is neither "null" nor
// meaningfully "not null" for what these operators test. Only a resolved,
// non-nil value makes "IS NOT NULL" true.
func evalSQLIsNull(n *sqlIsNullNode, row map[string]any, paramValues map[string]any) sqlTriState {
	v, ok := resolveSQLOperand(n.operand, row, paramValues)
	if !ok {
		return sqlFalse
	}

	result := v == nil
	if n.negate {
		result = !result
	}

	return boolToTri(result)
}

// evalSQLComparison evaluates a comparison operator. Returns sqlUndefined
// (never sqlFalse) when either side fails to resolve (a missing field or an
// unbound @param) -- see this file's top doc comment for why that
// distinction matters under NOT/AND/OR. A resolved comparison between
// mismatched types (e.g. a string against a number) still evaluates to a
// definite false, not undefined -- this emulator does not attempt to
// replicate every nuance of real Cosmos's type-coercion rules, only the
// undefined-propagation behavior the review that added this function was
// actually about.
func evalSQLComparison(n *sqlCmpNode, row map[string]any, paramValues map[string]any) sqlTriState {
	left, leftOK := resolveSQLOperand(n.left, row, paramValues)
	right, rightOK := resolveSQLOperand(n.right, row, paramValues)

	if !leftOK || !rightOK {
		return sqlUndefined
	}

	return boolToTri(compareValues(left, right, n.op))
}

// resolveSQLOperand resolves op against row/paramValues: a path resolves
// via resolvePath; a param looks up paramValues (ok=false if unbound); a
// literal resolves to its typed Go value directly. Numeric literals are
// carried as json.Number (parsed from their raw source text) rather than
// float64, so a literal like 9007199254740993 compares exactly against a
// stored value of the same magnitude -- see compareValues/compareJSONNumbers.
func resolveSQLOperand(op sqlOperand, row map[string]any, paramValues map[string]any) (any, bool) {
	switch op.kind {
	case sqlOperandPath:
		return resolvePath(row, op.path)
	case sqlOperandParam:
		v, ok := paramValues[op.paramName]

		return v, ok
	case sqlOperandString:
		return op.strVal, true
	case sqlOperandNumber:
		return json.Number(op.numLit), true
	case sqlOperandBool:
		return op.boolVal, true
	case sqlOperandNull:
		return nil, true
	default:
		return nil, false
	}
}

// compareValues applies op to left/right (each a JSON-decoded value: nil,
// bool, string, json.Number, or a param-supplied Go value of one of those
// kinds), never erroring or panicking: an unsupported type (map/slice) or a
// type mismatch between operands evaluates to false, mirroring
// services/azuretable's compareOperands.
func compareValues(left, right any, op sqlTokenType) bool {
	if left == nil || right == nil {
		return compareNullOperand(left, right, op)
	}

	switch lv := left.(type) {
	case json.Number:
		rv, ok := normalizeToJSONNumber(right)
		if !ok {
			return false
		}

		return applySQLCompare(compareJSONNumbers(lv, rv), op)
	case string:
		rv, ok := right.(string)
		if !ok {
			return false
		}

		return applySQLCompare(strings.Compare(lv, rv), op)
	case bool:
		rv, ok := right.(bool)
		if !ok || (op != sqlTokEq && op != sqlTokNeq) {
			return false
		}

		return applySQLCompare(cmpBool(lv, rv), op)
	default:
		return false
	}
}

// normalizeToJSONNumber accepts either a json.Number (the type every
// document-derived or SQL-literal number takes) or a plain float64
// (possible only for a caller-supplied @param bound to a bare JSON number
// decoded elsewhere without UseNumber), so a query parameter numeric value
// still compares correctly regardless of which numeric Go type it arrived
// as.
func normalizeToJSONNumber(v any) (json.Number, bool) {
	switch n := v.(type) {
	case json.Number:
		return n, true
	case float64:
		return json.Number(fmt.Sprintf("%v", n)), true
	default:
		return "", false
	}
}

func compareNullOperand(left, right any, op sqlTokenType) bool {
	if op != sqlTokEq && op != sqlTokNeq {
		return false
	}

	eq := left == nil && right == nil
	if op == sqlTokEq {
		return eq
	}

	return !eq
}

// compareJSONNumbers compares two json.Number values. When both parse as
// int64 exactly, they are compared as int64 directly rather than through
// float64 -- float64 has only a 53-bit mantissa, so a blanket
// float64-conversion comparison would silently corrupt any magnitude beyond
// 2^53 (mirrors services/azuretable's compareNumeric and its documented
// Int64-precision rationale). Falls back to float64 comparison when either
// side isn't an exact integer (e.g. a fractional number).
func compareJSONNumbers(a, b json.Number) int {
	ai, aErr := a.Int64()
	bi, bErr := b.Int64()

	if aErr == nil && bErr == nil {
		switch {
		case ai < bi:
			return -1
		case ai > bi:
			return 1
		default:
			return 0
		}
	}

	af, _ := a.Float64()
	bf, _ := b.Float64()

	switch {
	case af < bf:
		return -1
	case af > bf:
		return 1
	default:
		return 0
	}
}

func applySQLCompare(cmp int, op sqlTokenType) bool {
	switch op {
	case sqlTokEq:
		return cmp == 0
	case sqlTokNeq:
		return cmp != 0
	case sqlTokLt:
		return cmp < 0
	case sqlTokLte:
		return cmp <= 0
	case sqlTokGt:
		return cmp > 0
	case sqlTokGte:
		return cmp >= 0
	default:
		return false
	}
}

func cmpBool(a, b bool) int {
	switch {
	case a == b:
		return 0
	case !a && b:
		return -1
	default:
		return 1
	}
}
