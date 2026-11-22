package timestreamquery

import (
	"slices"
	"strings"
	"time"
)

// annotation is a query with its type-dependent rewrites applied and the static type of each result column.
type annotation struct {
	query       string
	plain       string
	cols        []outCol
	unsupported []string
	scans       []pruneScan
	params      []outCol
	ok          bool
}

func compareEdits(a, b typedEdit) int {
	switch {
	case a.start != b.start:
		return a.start - b.start
	case a.closing != b.closing:
		if a.closing {
			return -1
		}

		return 1
	case a.closing:
		return a.seq - b.seq
	}

	return b.seq - a.seq
}

func applyEdits(q string, edits []typedEdit) string {
	sorted := slices.Clone(edits)
	slices.SortStableFunc(sorted, compareEdits)

	var sb strings.Builder

	cur := 0

	for _, e := range sorted {
		if e.start < cur || e.end > len(q) {
			continue
		}

		sb.WriteString(q[cur:e.start])
		sb.WriteString(e.text)

		cur = e.end
	}

	sb.WriteString(q[cur:])

	return sb.String()
}

// annotateQuery parses q and infers the type of every result column; ok is false when q is not parsed.
func annotateQuery(q string, tables loadedSet, now time.Time, vars map[string]string) annotation {
	res := annotation{query: q, plain: q}

	func() {
		defer func() { _ = recover() }()

		res = buildAnnotation(q, tables, now, vars)
	}()

	return res
}

func buildAnnotation(q string, tables loadedSet, now time.Time, vars map[string]string) annotation {
	qn, err := parseSQL(q)
	if err != nil {
		return annotation{query: q, plain: q}
	}

	inf := newInferrer(tables, now, vars)
	cols := inf.inferQuery(qn, nil)

	return annotation{
		query: applyEdits(q, inf.edits), plain: applyEdits(q, withoutChecked(inf.edits)), cols: cols,
		unsupported: inf.unsupported, scans: inf.scans, params: inf.params, ok: true,
	}
}

func withoutChecked(edits []typedEdit) []typedEdit {
	return slices.DeleteFunc(slices.Clone(edits), func(e typedEdit) bool { return e.checked })
}

// columnTypes returns the per-column static types, or nil when they cannot be aligned with n result columns.
func (a annotation) columnTypes(n int) []tsType {
	if !a.ok || len(a.cols) != n {
		return nil
	}

	out := make([]tsType, n)
	for i, c := range a.cols {
		out[i] = c.typ
	}

	return out
}
