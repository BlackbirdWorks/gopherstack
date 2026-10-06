package ec2

import (
	"fmt"
	"maps"
	"math"
	"net/url"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

// pageSpec is one op's documented MaxResults range and ID-combination rule.
type pageSpec struct {
	idParam   string
	min, max  int
	defaultTo int
	clamp     bool
	noMaxArg  bool
	parallel  bool
}

// describeOpts selects what finishDescribe applies to an op's response.
type describeOpts struct {
	bounds  func(url.Values) []timeBound
	spec    pageSpec
	filters bool
	noPage  bool
}

func stdPageSpec() pageSpec {
	return pageSpec{min: ec2PageMinDefault, max: ec2PageMaxDefault}
}

// finishPaged pages resp with the shared 1..1000 MaxResults range.
func finishPaged(vals url.Values, resp any) (any, error) {
	return finishDescribe(vals, resp, describeOpts{spec: stdPageSpec()})
}

// finishPagedFiltered pages resp and applies Filters over its wire fields.
func finishPagedFiltered(vals url.Values, resp any) (any, error) {
	return finishDescribe(vals, resp, describeOpts{spec: stdPageSpec(), filters: true})
}

// postProcessor filters then pages a handler's response set in one place.
type postProcessor struct {
	filters       []wireFilter
	bounds        []timeBound
	limit, offset int
	paged         bool
	parallel      bool
}

func newPostProcessor(vals url.Values, o describeOpts) (postProcessor, error) {
	pp := postProcessor{paged: !o.noPage, parallel: o.spec.parallel}

	if pp.paged {
		if err := pp.parsePage(o.spec, vals); err != nil {
			return pp, err
		}
	}

	if o.filters {
		pp.filters = parseWireFilters(vals)
	}

	if o.bounds != nil {
		pp.bounds = o.bounds(vals)
	}

	return pp, nil
}

func (pp *postProcessor) parsePage(spec pageSpec, vals url.Values) error {
	if spec.noMaxArg {
		_, offset, err := parseEC2Pagination(vals, 1, 1, 1)
		pp.limit, pp.offset = math.MaxInt, offset

		return err
	}

	if spec.clamp {
		if n, err := strconv.Atoi(vals.Get("MaxResults")); err == nil && n > spec.max {
			vals = cloneWithMaxResults(vals, spec.max)
		}
	}

	def := spec.max
	if spec.defaultTo > 0 {
		def = spec.defaultTo
	}

	limit, offset, err := parseEC2Pagination(vals, spec.min, spec.max, def)
	pp.limit, pp.offset = limit, offset

	return err
}

func cloneWithMaxResults(vals url.Values, n int) url.Values {
	out := maps.Clone(vals)
	out.Set("MaxResults", strconv.Itoa(n))

	return out
}

// apply filters and pages resp's item sets in place and sets NextToken.
func (pp *postProcessor) apply(resp any) error {
	rv := reflect.ValueOf(resp)
	if rv.Kind() != reflect.Pointer || rv.Elem().Kind() != reflect.Struct {
		return nil
	}

	rv = rv.Elem()
	sets := findItemSets(rv)

	if len(pp.filters) > 0 || len(pp.bounds) > 0 {
		for _, set := range sets {
			filtered, err := filterWireItems(set, pp.filters)
			if err != nil {
				return err
			}

			set.Set(filterTimeBounds(filtered, pp.bounds))
		}
	}

	if !pp.paged || len(sets) == 0 {
		return nil
	}

	token := pp.pageSets(sets)

	if nt := rv.FieldByName("NextToken"); nt.IsValid() && nt.Kind() == reflect.String && nt.CanSet() {
		nt.SetString(token)
	}

	return nil
}

// pageSets windows the sets as one concatenated list, or in lockstep when parallel.
func (pp *postProcessor) pageSets(sets []reflect.Value) string {
	remaining, skip := pp.limit, pp.offset
	more := false

	for _, set := range sets {
		n := set.Len()
		if n > pp.limit || pp.offset > 0 {
			sortWireItems(set)
		}

		off := min(skip, n)

		if !pp.parallel {
			skip -= off
		}

		end := min(n, off+remaining)
		if end < n {
			more = true
		}

		set.Set(set.Slice(off, end))

		if !pp.parallel {
			remaining -= end - off
		}
	}

	if !more {
		return ""
	}

	return page.EncodeHMACToken(pp.offset+pp.limit, ec2PaginationSalt)
}

// findItemSets locates the response's repeated-item slices, directly or inside set wrappers.
func findItemSets(rv reflect.Value) []reflect.Value {
	var sets []reflect.Value

	for i := range rv.NumField() {
		sf := rv.Type().Field(i)
		if sf.Name == "XMLName" || sf.Name == "NextToken" || sf.Name == "RequestID" || sf.Name == "Xmlns" {
			continue
		}

		fv := rv.Field(i)
		if fv.Kind() == reflect.Struct && fv.NumField() == 1 {
			fv = fv.Field(0)
		}

		if fv.Kind() == reflect.Slice && fv.Type().Elem().Kind() != reflect.Uint8 && fv.CanSet() {
			sets = append(sets, fv)
		}
	}

	return sets
}

// sortWireItems orders items by their wire content so offsets stay stable across calls.
func sortWireItems(set reflect.Value) {
	n := set.Len()
	keys := make([]string, n)

	for i := range n {
		var sb strings.Builder

		wireSortKey(set.Index(i), &sb)
		keys[i] = sb.String()
	}

	order := make([]int, n)
	for i := range order {
		order[i] = i
	}

	sort.SliceStable(order, func(a, b int) bool { return keys[order[a]] < keys[order[b]] })

	sorted := reflect.MakeSlice(set.Type(), n, n)
	for i, from := range order {
		sorted.Index(i).Set(set.Index(from))
	}

	set.Set(sorted)
}

func wireSortKey(v reflect.Value, sb *strings.Builder) {
	for v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface {
		if v.IsNil() {
			return
		}

		v = v.Elem()
	}

	switch v.Kind() {
	case reflect.String:
		sb.WriteString(v.String())
		sb.WriteByte(0)
	case reflect.Struct:
		for _, field := range v.Fields() {
			wireSortKey(field, sb)
		}
	case reflect.Slice:
		for i := range v.Len() {
			wireSortKey(v.Index(i), sb)
		}
	default:
	}
}

// finishDescribe applies the documented Filters and MaxResults/NextToken to resp.
func finishDescribe(vals url.Values, resp any, o describeOpts) (any, error) {
	pp, err := newPostProcessor(vals, o)
	if err != nil {
		return nil, err
	}

	if err = pp.apply(resp); err != nil {
		return nil, err
	}

	return resp, nil
}

// checkPageIDCombo rejects MaxResults alongside explicit IDs where the op documents it.
func checkPageIDCombo(vals url.Values, spec pageSpec) error {
	if vals.Get("MaxResults") != "" && vals.Get(spec.idParam+".1") != "" {
		return fmt.Errorf(
			"%w: cannot specify both MaxResults and %ss", ErrInvalidParameterCombination, spec.idParam,
		)
	}

	return nil
}

// timeBound limits items to those whose wire timestamp field is >= (lower) or <= (upper) at.
type timeBound struct {
	at    time.Time
	field string
	lower bool
}

// timeBoundSpec is one raw RFC3339 request value bounding a wire timestamp field.
type timeBoundSpec struct {
	raw, field string
	lower      bool
}

func timeBoundsOf(specs ...timeBoundSpec) []timeBound {
	var out []timeBound

	for _, sp := range specs {
		if t, err := time.Parse(time.RFC3339, sp.raw); err == nil {
			out = append(out, timeBound{at: t, field: sp.field, lower: sp.lower})
		}
	}

	return out
}

func filterTimeBounds(items reflect.Value, bounds []timeBound) reflect.Value {
	if len(bounds) == 0 {
		return items
	}

	out := reflect.MakeSlice(items.Type(), 0, items.Len())

	for i := range items.Len() {
		if itemWithinBounds(items.Index(i), bounds) {
			out = reflect.Append(out, items.Index(i))
		}
	}

	return out
}

func itemWithinBounds(item reflect.Value, bounds []timeBound) bool {
	for _, b := range bounds {
		var got []string

		collectWireValues(item, []string{b.field}, &got)

		if len(got) == 0 {
			return false
		}

		ts, err := time.Parse(time.RFC3339Nano, got[0])
		if err != nil || (b.lower && ts.Before(b.at)) || (!b.lower && ts.After(b.at)) {
			return false
		}
	}

	return true
}
