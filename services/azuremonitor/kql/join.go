package kql

import (
	"strconv"
	"strings"
)

const (
	kindFullOuter    = "fullouter"
	kindLeftAnti     = "leftanti"
	kindLeftAntiSemi = "leftantisemi"
	kindInnerUnique  = "innerunique"
	kindLeftSemi     = "leftsemi"
	kindRightAntiSm  = "rightantisemi"
	kindRightAnti    = "rightanti"
	kindRightSemi    = "rightsemi"
)

// maxJoinRows and maxJoinCols bound a join result: a skewed key cannot blow up into a cross product and nested
// joins cannot widen the schema without limit.
const (
	maxJoinRows = 500_000
	maxJoinCols = 4096
)

type joinKey struct{ left, right string }

type joinOp struct {
	right node
	kind  string
	keys  []joinKey
}

var joinKinds = map[string]bool{ //nolint:gochecknoglobals // static lookup
	kindInnerUnique: true, "inner": true, "leftouter": true, "rightouter": true, kindFullOuter: true,
	kindLeftSemi: true, kindRightSemi: true, kindLeftAnti: true, kindRightAnti: true,
	kindLeftAntiSemi: true, kindRightAntiSm: true,
}

func (p *parser) parseJoin() (node, error) {
	op := &joinOp{kind: kindInnerUnique}

	for p.cur().kind == tkIdent && p.opAt(1, "=") {
		name := strings.ToLower(p.cur().text)
		p.i += 2

		val := strings.ToLower(p.cur().text)
		p.i++

		if name != "kind" {
			continue
		}

		if !joinKinds[val] {
			return nil, unsupported("join kind '" + val + "'")
		}

		op.kind = val
	}

	right, err := p.parseUnionMember()
	if err != nil {
		return nil, err
	}

	op.right = right

	if err = p.expectKw("on"); err != nil {
		return nil, err
	}

	for {
		k, kerr := p.parseJoinKey()
		if kerr != nil {
			return nil, kerr
		}

		op.keys = append(op.keys, k)

		if !p.isOp(",") && !p.isKw("and") {
			return op, nil
		}

		p.i++
	}
}

func (p *parser) parseJoinKey() (joinKey, error) {
	if p.cur().kind != tkIdent || !strings.EqualFold(p.cur().text, "$left") {
		n, err := p.parseName()

		return joinKey{left: n, right: n}, err
	}

	p.i++

	l, err := p.parseJoinSide()
	if err != nil {
		return joinKey{}, err
	}

	if err = p.expectOp("=="); err != nil {
		return joinKey{}, err
	}

	if p.cur().kind != tkIdent || !strings.EqualFold(p.cur().text, "$right") {
		return joinKey{}, p.unexpected()
	}

	p.i++

	r, err := p.parseJoinSide()

	return joinKey{left: l, right: r}, err
}

// parseJoinSide reads ".column" after $left or $right.
func (p *parser) parseJoinSide() (string, error) {
	if err := p.expectOp("."); err != nil {
		return "", err
	}

	return p.parseName()
}

func colIndex(r *Rel, name string) int {
	for i, c := range r.Cols {
		if strings.EqualFold(c.Name, name) {
			return i
		}
	}

	return -1
}

func rowKey(row []any, idx []int) string {
	var sb strings.Builder

	for _, i := range idx {
		sb.WriteString(groupKey(row[i]))
		sb.WriteByte(0)
	}

	return sb.String()
}

func joinOutputCols(left, right *Rel, kind string) ([]Col, error) {
	switch kind {
	case kindLeftSemi, kindLeftAnti, kindLeftAntiSemi:
		return append([]Col(nil), left.Cols...), nil
	case kindRightSemi, kindRightAnti, kindRightAntiSm:
		return append([]Col(nil), right.Cols...), nil
	}

	if len(left.Cols)+len(right.Cols) > maxJoinCols {
		return nil, semErr("the join result exceeds the emulator limit of %d columns", maxJoinCols)
	}

	cols := append([]Col(nil), left.Cols...)
	used := map[string]bool{}

	for _, c := range cols {
		used[strings.ToLower(c.Name)] = true
	}

	next := map[string]int{}

	for _, c := range right.Cols {
		name, base := c.Name, strings.ToLower(c.Name)

		for n := max(next[base], 1); used[strings.ToLower(name)]; n++ {
			name = c.Name + strconv.Itoa(n)
			next[base] = n + 1
		}

		used[strings.ToLower(name)] = true
		cols = append(cols, Col{Name: name, Type: c.Type})
	}

	return cols, nil
}

func (e *env) applyJoin(in *Rel, x *joinOp) (*Rel, error) {
	right, err := e.exec(x.right)
	if err != nil {
		return nil, err
	}

	li, ri := make([]int, len(x.keys)), make([]int, len(x.keys))

	for i, k := range x.keys {
		if li[i] = colIndex(in, k.left); li[i] < 0 {
			return nil, semErr("Failed to resolve scalar expression named '%s'", k.left)
		}

		if ri[i] = colIndex(right, k.right); ri[i] < 0 {
			return nil, semErr("Failed to resolve scalar expression named '%s'", k.right)
		}
	}

	byKey := map[string][]int{}
	for i, row := range right.Rows {
		k := rowKey(row, ri)
		byKey[k] = append(byKey[k], i)
	}

	cols, err := joinOutputCols(in, right, x.kind)
	if err != nil {
		return nil, err
	}

	out := &Rel{Cols: cols}
	matched := make([]bool, len(right.Rows))
	seen := map[string]bool{}

	for _, lrow := range in.Rows {
		k := rowKey(lrow, li)
		if x.kind == kindInnerUnique {
			if seen[k] {
				continue
			}

			seen[k] = true
		}

		hits := byKey[k]
		for _, h := range hits {
			matched[h] = true
		}

		out.Rows = appendLeftRow(out.Rows, x.kind, lrow, right, hits)
		if len(out.Rows) > maxJoinRows {
			return nil, semErr("the join result exceeds the emulator limit of %d rows", maxJoinRows)
		}
	}

	return appendRightRows(out, x.kind, in, right, matched), nil
}

func appendLeftRow(rows [][]any, kind string, lrow []any, right *Rel, hits []int) [][]any {
	switch kind {
	case kindLeftSemi:
		if len(hits) > 0 {
			rows = append(rows, lrow)
		}
	case kindLeftAnti, kindLeftAntiSemi:
		if len(hits) == 0 {
			rows = append(rows, lrow)
		}
	case kindRightSemi, kindRightAnti, kindRightAntiSm:
	default:
		for _, h := range hits {
			rows = append(rows, append(append([]any(nil), lrow...), right.Rows[h]...))
		}

		if len(hits) == 0 && (kind == "leftouter" || kind == kindFullOuter) {
			rows = append(rows, append(append([]any(nil), lrow...), emptyRow(right.Cols)...))
		}
	}

	return rows
}

func appendRightRows(out *Rel, kind string, left, right *Rel, matched []bool) *Rel {
	for i, row := range right.Rows {
		switch kind {
		case kindRightSemi:
			if matched[i] {
				out.Rows = append(out.Rows, row)
			}
		case kindRightAnti, kindRightAntiSm:
			if !matched[i] {
				out.Rows = append(out.Rows, row)
			}
		case "rightouter", kindFullOuter:
			if !matched[i] {
				out.Rows = append(out.Rows, append(emptyRow(left.Cols), row...))
			}
		}
	}

	return out
}

// emptyRow is the all-null row of cols; string columns hold "" because Kusto strings are never null.
func emptyRow(cols []Col) []any {
	row := make([]any, len(cols))

	for i, c := range cols {
		if c.Type == TString {
			row[i] = ""
		}
	}

	return row
}
