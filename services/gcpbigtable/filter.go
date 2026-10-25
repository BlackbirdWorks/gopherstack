package gcpbigtable

import (
	"bytes"
	"math/rand/v2"
	"regexp"
	"sort"
	"strings"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
)

const (
	maxFilterDepth = 20
	maxLabelChars  = 15
)

var labelValueRe = regexp.MustCompile(`^[a-z0-9-]+$`)

type fcell struct {
	fam, qual, label string
	val              []byte
	ts               int64
}

type evalCtx struct {
	key  string
	sink []fcell
}

type filterFn func(e *evalCtx, in []fcell) []fcell

func filterErr(msg string) error {
	return errInvalid("Error in field 'filter' : %s", msg)
}

// compileFilter validates a RowFilter and turns it into an evaluator; nil means pass everything.
func compileFilter(f *btpb.RowFilter) (filterFn, error) {
	if f == nil {
		return func(_ *evalCtx, in []fcell) []fcell { return in }, nil
	}

	return compile(f, 0, false)
}

func compile(f *btpb.RowFilter, depth int, inCond bool) (filterFn, error) {
	if depth > maxFilterDepth {
		return nil, filterErr("filters are nested too deeply")
	}

	switch ft := f.GetFilter().(type) {
	case *btpb.RowFilter_Chain_:
		return compileChain(ft.Chain.GetFilters(), depth, inCond)
	case *btpb.RowFilter_Interleave_:
		return compileInterleave(ft.Interleave.GetFilters(), depth, inCond)
	case *btpb.RowFilter_Condition_:
		return compileCondition(ft.Condition, depth)
	case *btpb.RowFilter_Sink:
		if inCond {
			return nil, filterErr("sink cannot be used inside a condition")
		}

		return func(e *evalCtx, in []fcell) []fcell {
			e.sink = append(e.sink, in...)

			return nil
		}, nil
	case *btpb.RowFilter_PassAllFilter:
		if !ft.PassAllFilter {
			return nil, filterErr("pass_all_filter must be true if set")
		}

		return func(_ *evalCtx, in []fcell) []fcell { return in }, nil
	case *btpb.RowFilter_BlockAllFilter:
		if !ft.BlockAllFilter {
			return nil, filterErr("block_all_filter must be true if set")
		}

		return func(*evalCtx, []fcell) []fcell { return nil }, nil
	default:
		return compileLeaf(f)
	}
}

func hasLabel(f *btpb.RowFilter) bool {
	switch ft := f.GetFilter().(type) {
	case *btpb.RowFilter_ApplyLabelTransformer:
		return true
	case *btpb.RowFilter_Chain_:
		for _, s := range ft.Chain.GetFilters() {
			if hasLabel(s) {
				return true
			}
		}
	case *btpb.RowFilter_Interleave_:
		for _, s := range ft.Interleave.GetFilters() {
			if hasLabel(s) {
				return true
			}
		}
	case *btpb.RowFilter_Condition_:
		return hasLabel(ft.Condition.GetTrueFilter()) || hasLabel(ft.Condition.GetFalseFilter())
	}

	return false
}

func compileChain(subs []*btpb.RowFilter, depth int, inCond bool) (filterFn, error) {
	if len(subs) < 2 { //nolint:mnd // a chain needs at least two filters
		return nil, filterErr("Chain must contain at least two RowFilters")
	}

	labels := 0
	fns := make([]filterFn, 0, len(subs))

	for _, s := range subs {
		if hasLabel(s) {
			labels++
		}

		fn, err := compile(s, depth+1, inCond)
		if err != nil {
			return nil, err
		}

		fns = append(fns, fn)
	}

	if labels > 1 {
		return nil, filterErr("Chain may contain at most one sub-filter with an apply_label_transformer")
	}

	return func(e *evalCtx, in []fcell) []fcell {
		for _, fn := range fns {
			in = fn(e, in)
			if len(in) == 0 {
				return nil
			}
		}

		return in
	}, nil
}

func compileInterleave(subs []*btpb.RowFilter, depth int, inCond bool) (filterFn, error) {
	if len(subs) < 2 { //nolint:mnd // an interleave needs at least two filters
		return nil, filterErr("Interleave must contain at least two RowFilters")
	}

	fns := make([]filterFn, 0, len(subs))

	for _, s := range subs {
		fn, err := compile(s, depth+1, inCond)
		if err != nil {
			return nil, err
		}

		fns = append(fns, fn)
	}

	return func(e *evalCtx, in []fcell) []fcell {
		var out []fcell
		for _, fn := range fns {
			out = append(out, fn(e, append([]fcell(nil), in...))...)
		}

		return mergeCells(out)
	}, nil
}

func compileCondition(c *btpb.RowFilter_Condition, depth int) (filterFn, error) {
	if c.GetPredicateFilter() == nil {
		return nil, filterErr("Condition must have a predicate_filter")
	}

	if c.GetTrueFilter() == nil && c.GetFalseFilter() == nil {
		return nil, filterErr("Condition must have a true_filter or a false_filter")
	}

	pred, err := compile(c.GetPredicateFilter(), depth+1, true)
	if err != nil {
		return nil, err
	}

	var onTrue, onFalse filterFn

	if c.GetTrueFilter() != nil {
		if onTrue, err = compile(c.GetTrueFilter(), depth+1, true); err != nil {
			return nil, err
		}
	}

	if c.GetFalseFilter() != nil {
		if onFalse, err = compile(c.GetFalseFilter(), depth+1, true); err != nil {
			return nil, err
		}
	}

	return func(e *evalCtx, in []fcell) []fcell {
		matched := len(pred(e, append([]fcell(nil), in...))) > 0

		switch {
		case matched && onTrue != nil:
			return onTrue(e, in)
		case !matched && onFalse != nil:
			return onFalse(e, in)
		}

		return nil
	}, nil
}

func mergeCells(cells []fcell) []fcell {
	sort.SliceStable(cells, func(i, j int) bool {
		a, b := cells[i], cells[j]
		if a.fam != b.fam {
			return a.fam < b.fam
		}

		if a.qual != b.qual {
			return a.qual < b.qual
		}

		return a.ts > b.ts
	})

	return cells
}

func keep(pred func(c fcell) bool) filterFn {
	return func(_ *evalCtx, in []fcell) []fcell {
		out := in[:0:0]

		for _, c := range in {
			if pred(c) {
				out = append(out, c)
			}
		}

		return out
	}
}

// bytesRegex compiles a Bigtable RE2 pattern that must match the whole input; \C matches any single byte.
func bytesRegex(field string, pat []byte) (*regexp.Regexp, error) {
	src := strings.ReplaceAll(string(pat), `\C`, `(?s:.)`)

	re, err := regexp.Compile(`^(?:` + src + `)$`)
	if err != nil {
		return nil, filterErr("invalid " + field + ": " + err.Error())
	}

	return re, nil
}

func compileLeaf(f *btpb.RowFilter) (filterFn, error) {
	switch ft := f.GetFilter().(type) {
	case *btpb.RowFilter_RowKeyRegexFilter:
		re, err := bytesRegex("row_key_regex_filter", ft.RowKeyRegexFilter)
		if err != nil {
			return nil, err
		}

		return func(e *evalCtx, in []fcell) []fcell {
			if re.MatchString(e.key) {
				return in
			}

			return nil
		}, nil
	case *btpb.RowFilter_RowSampleFilter:
		if ft.RowSampleFilter <= 0 || ft.RowSampleFilter >= 1 {
			return nil, filterErr("row_sample_filter must be in the range (0, 1)")
		}

		p := ft.RowSampleFilter

		return func(_ *evalCtx, in []fcell) []fcell {
			if rand.Float64() < p { //nolint:gosec // sampling, not security
				return in
			}

			return nil
		}, nil
	case *btpb.RowFilter_FamilyNameRegexFilter:
		re, err := bytesRegex("family_name_regex_filter", []byte(ft.FamilyNameRegexFilter))
		if err != nil {
			return nil, err
		}

		return keep(func(c fcell) bool { return re.MatchString(c.fam) }), nil
	case *btpb.RowFilter_ColumnQualifierRegexFilter:
		re, err := bytesRegex("column_qualifier_regex_filter", ft.ColumnQualifierRegexFilter)
		if err != nil {
			return nil, err
		}

		return keep(func(c fcell) bool { return re.MatchString(c.qual) }), nil
	case *btpb.RowFilter_ValueRegexFilter:
		re, err := bytesRegex("value_regex_filter", ft.ValueRegexFilter)
		if err != nil {
			return nil, err
		}

		return keep(func(c fcell) bool { return re.Match(c.val) }), nil
	}

	return compileRangeOrLimit(f)
}

func compileRangeOrLimit(f *btpb.RowFilter) (filterFn, error) {
	switch ft := f.GetFilter().(type) {
	case *btpb.RowFilter_ColumnRangeFilter:
		return compileColumnRange(ft.ColumnRangeFilter)
	case *btpb.RowFilter_TimestampRangeFilter:
		start, end := ft.TimestampRangeFilter.GetStartTimestampMicros(), ft.TimestampRangeFilter.GetEndTimestampMicros()
		if start < 0 || end < 0 || (end != 0 && start > end) {
			return nil, filterErr("invalid timestamp_range_filter")
		}

		return keep(func(c fcell) bool { return c.ts >= start && (end == 0 || c.ts < end) }), nil
	case *btpb.RowFilter_ValueRangeFilter:
		return compileValueRange(ft.ValueRangeFilter)
	case *btpb.RowFilter_CellsPerRowOffsetFilter:
		n := int(ft.CellsPerRowOffsetFilter)
		if n < 0 {
			return nil, filterErr("cells_per_row_offset_filter must not be negative")
		}

		return func(_ *evalCtx, in []fcell) []fcell { return in[min(n, len(in)):] }, nil
	case *btpb.RowFilter_CellsPerRowLimitFilter:
		n := int(ft.CellsPerRowLimitFilter)
		if n < 0 {
			return nil, filterErr("cells_per_row_limit_filter must not be negative")
		}

		return func(_ *evalCtx, in []fcell) []fcell { return in[:min(n, len(in))] }, nil
	case *btpb.RowFilter_CellsPerColumnLimitFilter:
		n := int(ft.CellsPerColumnLimitFilter)
		if n < 0 {
			return nil, filterErr("cells_per_column_limit_filter must not be negative")
		}

		return perColumnLimit(n), nil
	case *btpb.RowFilter_StripValueTransformer:
		return func(_ *evalCtx, in []fcell) []fcell {
			out := make([]fcell, len(in))
			for i, c := range in {
				c.val = nil
				out[i] = c
			}

			return out
		}, nil
	case *btpb.RowFilter_ApplyLabelTransformer:
		return compileLabel(ft.ApplyLabelTransformer)
	case *btpb.RowFilter_ValueBitmaskFilter:
		mask := ft.ValueBitmaskFilter.GetMask()

		return keep(func(c fcell) bool {
			if len(c.val) != len(mask) {
				return false
			}

			for i := range mask {
				if c.val[i]&mask[i] != mask[i] {
					return false
				}
			}

			return true
		}), nil
	}

	return nil, filterErr("RowFilter must have exactly one of its fields set")
}

func perColumnLimit(n int) filterFn {
	return func(_ *evalCtx, in []fcell) []fcell {
		out := in[:0:0]
		run := 0

		for i, c := range in {
			if i == 0 || c.fam != in[i-1].fam || c.qual != in[i-1].qual {
				run = 0
			}

			run++

			if run <= n {
				out = append(out, c)
			}
		}

		return out
	}
}

func compileLabel(label string) (filterFn, error) {
	if len(label) > maxLabelChars || !labelValueRe.MatchString(label) {
		return nil, filterErr("apply_label_transformer must be 1-15 characters matching [a-z0-9\\-]+")
	}

	return func(_ *evalCtx, in []fcell) []fcell {
		out := make([]fcell, len(in))
		for i, c := range in {
			c.label = label
			out[i] = c
		}

		return out
	}, nil
}

type bound struct {
	val    []byte
	set    bool
	closed bool
}

func (lo bound) below(v []byte) bool {
	if !lo.set {
		return true
	}

	c := bytes.Compare(v, lo.val)

	return c > 0 || (c == 0 && lo.closed)
}

func (hi bound) above(v []byte) bool {
	if !hi.set {
		return true
	}

	c := bytes.Compare(v, hi.val)

	return c < 0 || (c == 0 && hi.closed)
}

func compileColumnRange(r *btpb.ColumnRange) (filterFn, error) {
	if r.GetFamilyName() == "" {
		return nil, filterErr("column_range_filter must specify a family_name")
	}

	var lo, hi bound

	switch s := r.GetStartQualifier().(type) {
	case *btpb.ColumnRange_StartQualifierClosed:
		lo = bound{val: s.StartQualifierClosed, set: true, closed: true}
	case *btpb.ColumnRange_StartQualifierOpen:
		lo = bound{val: s.StartQualifierOpen, set: true}
	}

	switch e := r.GetEndQualifier().(type) {
	case *btpb.ColumnRange_EndQualifierClosed:
		hi = bound{val: e.EndQualifierClosed, set: true, closed: true}
	case *btpb.ColumnRange_EndQualifierOpen:
		hi = bound{val: e.EndQualifierOpen, set: true}
	}

	if lo.set && hi.set && bytes.Compare(lo.val, hi.val) > 0 {
		return nil, filterErr("column_range_filter start qualifier is after its end")
	}

	fam := r.GetFamilyName()

	return keep(func(c fcell) bool {
		q := []byte(c.qual)

		return c.fam == fam && lo.below(q) && hi.above(q)
	}), nil
}

func compileValueRange(r *btpb.ValueRange) (filterFn, error) {
	var lo, hi bound

	switch s := r.GetStartValue().(type) {
	case *btpb.ValueRange_StartValueClosed:
		lo = bound{val: s.StartValueClosed, set: true, closed: true}
	case *btpb.ValueRange_StartValueOpen:
		lo = bound{val: s.StartValueOpen, set: true}
	}

	switch e := r.GetEndValue().(type) {
	case *btpb.ValueRange_EndValueClosed:
		hi = bound{val: e.EndValueClosed, set: true, closed: true}
	case *btpb.ValueRange_EndValueOpen:
		hi = bound{val: e.EndValueOpen, set: true}
	}

	if lo.set && hi.set && bytes.Compare(lo.val, hi.val) > 0 {
		return nil, filterErr("value_range_filter start value is after its end")
	}

	return keep(func(c fcell) bool { return lo.below(c.val) && hi.above(c.val) }), nil
}

// runFilter applies a compiled filter to a row's cells and returns the final cells including any sink output.
func runFilter(fn filterFn, key string, cells []fcell) []fcell {
	e := &evalCtx{key: key}
	out := fn(e, cells)

	if len(e.sink) > 0 {
		out = mergeCells(append(append([]fcell(nil), out...), e.sink...))
	}

	return out
}
