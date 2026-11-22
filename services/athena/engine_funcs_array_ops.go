package athena

import (
	"database/sql/driver"
	"encoding/json/v2"
	"math/rand/v2"
	"slices"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	maxSequenceEntries = 10000
	maxRepeatCount     = 10000
	minZipArrays       = 2
	maxZipArrays       = 5
)

func elemKey(e any) string {
	b, err := json.Marshal(e, jsonx.V1())
	if err != nil {
		return kwNull
	}

	return string(b)
}

func keySet(a []any) map[string]bool {
	out := make(map[string]bool, len(a))
	for _, e := range a {
		out[elemKey(e)] = true
	}

	return out
}

func distinctElems(a []any) []any {
	seen := make(map[string]bool, len(a))
	out := make([]any, 0, len(a))

	for _, e := range a {
		if k := elemKey(e); !seen[k] {
			seen[k] = true
			out = append(out, e)
		}
	}

	return out
}

func twoArrays(args []driver.Value) ([]any, []any, bool) {
	x, xok := asArray(args[0])
	y, yok := asArray(args[1])

	return x, y, xok && yok
}

func setOperation(keep func(inY bool) bool, includeY bool) scalarFn {
	return func(_ *fnCtx, args []driver.Value) (driver.Value, error) {
		x, y, ok := twoArrays(args)
		if !ok {
			return sqlNull(), nil
		}

		inY := keySet(y)
		out := make([]any, 0, len(x)+len(y))

		for _, e := range x {
			if keep(inY[elemKey(e)]) {
				out = append(out, e)
			}
		}

		if includeY {
			out = append(out, y...)
		}

		return jsonText(distinctElems(out)), nil
	}
}

func udfArraysOverlap(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	x, y, ok := twoArrays(args)
	if !ok {
		return sqlNull(), nil
	}

	inY := keySet(y)
	sawNull := inY[kwNull]

	for _, e := range x {
		k := elemKey(e)
		if k == kwNull {
			sawNull = true

			continue
		}

		if inY[k] {
			return int64(1), nil
		}
	}

	if sawNull {
		return sqlNull(), nil
	}

	return int64(0), nil
}

func udfShuffle(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	a, ok := asArray(args[0])
	if !ok {
		return sqlNull(), nil
	}

	rand.Shuffle(len(a), func(i, j int) { a[i], a[j] = a[j], a[i] }) //nolint:gosec // not security-sensitive

	return jsonText(a), nil
}

func udfRepeat(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	if args[1] == nil {
		return sqlNull(), nil
	}

	n, ok := toInt(args[1])
	if !ok {
		return nil, errArg("Unexpected parameters for function repeat")
	}

	switch {
	case n < 0:
		return nil, errArg("count argument of repeat function must be greater than or equal to 0")
	case n > maxRepeatCount:
		return nil, errArg("count argument of repeat function must be less than or equal to %d", maxRepeatCount)
	}

	elem := jsonElem(args[0])
	out := make([]any, n)

	for i := range out {
		out[i] = elem
	}

	return jsonText(out), nil
}

func udfZip(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	if len(args) < minZipArrays || len(args) > maxZipArrays {
		return nil, errArg("Unexpected parameters for function zip: expected between 2 and 5 arrays")
	}

	arrays := make([][]any, len(args))
	width := 0

	for i, a := range args {
		arr, ok := asArray(a)
		if !ok {
			return sqlNull(), nil
		}

		arrays[i] = arr
		width = max(width, len(arr))
	}

	names := make([]string, len(arrays))
	rows := make([]any, width)

	for r := range rows {
		vals := make([]any, len(arrays))

		for c, arr := range arrays {
			if r < len(arr) {
				vals[c] = arr[r]
			}
		}

		rows[r] = newRow(names, vals)
	}

	return jsonText(rows), nil
}

func udfArrayFirst(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	a, ok := asArray(args[0])
	if !ok || len(a) == 0 {
		return sqlNull(), nil
	}

	return sqlValue(a[0]), nil
}

func udfArrayLast(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	a, ok := asArray(args[0])
	if !ok || len(a) == 0 {
		return sqlNull(), nil
	}

	return sqlValue(a[len(a)-1]), nil
}

func udfFlatten(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	a, ok := asArray(args[0])
	if !ok {
		return sqlNull(), nil
	}

	out := make([]any, 0, len(a))

	for _, e := range a {
		if e == nil {
			continue
		}

		inner, isArr := e.([]any)
		if !isArr {
			return nil, errArg("Unexpected parameters for function flatten: expected array(array(T))")
		}

		out = append(out, inner...)
	}

	return jsonText(out), nil
}

func udfTrimArray(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	a, ok := asArray(args[0])
	n, nok := toInt(args[1])

	if !ok || !nok {
		return sqlNull(), nil
	}

	switch {
	case n < 0:
		return nil, errArg("size must not be negative")
	case n > int64(len(a)):
		return nil, errArg("size must not exceed array length")
	}

	return jsonText(a[:int64(len(a))-n]), nil
}

func udfContainsSequence(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	x, y, ok := twoArrays(args)
	if !ok {
		return sqlNull(), nil
	}

	for i := 0; i+len(y) <= len(x); i++ {
		if slices.EqualFunc(x[i:i+len(y)], y, func(a, b any) bool { return elemKey(a) == elemKey(b) }) {
			return int64(1), nil
		}
	}

	return int64(0), nil
}

type sequenceSpec struct {
	at   func(i int64) time.Time
	past func(t time.Time) bool
	date bool
}

func udfSequence(_ *fnCtx, args []driver.Value) (driver.Value, error) {
	if len(args) < argsTwo || len(args) > argsThree {
		return nil, errArg("Unexpected parameters for function sequence")
	}

	if slices.Contains(args, nil) {
		return sqlNull(), nil
	}

	if _, isInt := args[0].(int64); isInt {
		return integerSequence(args)
	}

	return temporalSequence(args)
}

func integerSequence(args []driver.Value) (driver.Value, error) {
	start, sok := args[0].(int64)
	stop, eok := args[1].(int64)

	if !sok || !eok {
		return nil, errArg("Unexpected parameters for function sequence")
	}

	step := int64(1)
	if start > stop {
		step = -1
	}

	if len(args) == argsThree {
		var ok bool
		if step, ok = args[argsTwo].(int64); !ok {
			return nil, errArg("Unexpected parameters for function sequence")
		}
	}

	if err := checkSequenceBounds(step, start > stop, start == stop); err != nil {
		return nil, err
	}

	var span uint64
	if start <= stop {
		span = uint64(stop) - uint64(start) //nolint:gosec // two's-complement distance
	} else {
		span = uint64(start) - uint64(stop) //nolint:gosec // two's-complement distance
	}

	stride := uint64(step)
	if step < 0 {
		stride = -uint64(step)
	}

	if span/stride >= maxSequenceEntries {
		return nil, errSequenceLength()
	}

	out := make([]any, 0, span/stride+1)
	for v, i := start, uint64(0); i <= span/stride; i++ {
		out = append(out, v)
		v += step
	}

	return jsonText(out), nil
}

func errSequenceLength() error {
	return errArg("result of sequence function must not have more than %d entries", maxSequenceEntries)
}

func checkSequenceBounds(step int64, descending, equal bool) error {
	switch {
	case step == 0:
		return errArg("step must not be zero")
	case !equal && ((step > 0) == descending):
		return errArg("sequence stop value should be greater than or equal to start value if step is greater " +
			"than zero otherwise stop should be less than or equal to start")
	}

	return nil
}

func temporalSequence(args []driver.Value) (driver.Value, error) {
	start, sok := parseTS(args[0])
	stop, eok := parseTS(args[1])

	if !sok || !eok {
		return nil, errArg("Unexpected parameters for function sequence")
	}

	date := isDateOnly(args[0])
	kind, step := ivKindDT, int64(millisPerDay)

	if len(args) == argsThree {
		var ok bool
		if kind, step, ok = ivArg(args[argsTwo]); !ok {
			return nil, errArg("Unexpected parameters for function sequence")
		}
	} else if !date {
		return nil, errArg("Unexpected parameters for function sequence")
	}

	spec, err := newSequenceSpec(start, stop, kind, step, date)
	if err != nil {
		return nil, err
	}

	out := make([]any, 0, argsThree)

	for i := range int64(maxSequenceEntries + 1) {
		t := spec.at(i)
		if spec.past(t) {
			return jsonText(out), nil
		}

		out = append(out, formatLike(t, spec.date))
	}

	return nil, errSequenceLength()
}

func newSequenceSpec(start, stop time.Time, kind byte, step int64, date bool) (*sequenceSpec, error) {
	if err := checkSequenceBounds(step, start.After(stop), start.Equal(stop)); err != nil {
		return nil, err
	}

	spec := &sequenceSpec{date: date}

	switch {
	case kind == ivKindYM:
		spec.at = func(i int64) time.Time { return addMonthsClamped(start, int(i*step)) }
	case date:
		if step%millisPerDay != 0 {
			return nil, errArg("sequence step must be a day interval if start and end values are dates")
		}

		spec.at = func(i int64) time.Time { return start.AddDate(0, 0, int(i*(step/millisPerDay))) }
	default:
		spec.at = func(i int64) time.Time { return start.Add(time.Duration(i*step) * time.Millisecond) }
	}

	if step > 0 {
		spec.past = func(t time.Time) bool { return t.After(stop) }
	} else {
		spec.past = func(t time.Time) bool { return t.Before(stop) }
	}

	return spec, nil
}

func registerArrayOpFuncs() {
	registerNondeterministic("sequence", -1, udfSequence)
	registerNondeterministic("repeat", argsTwo, udfRepeat)
	registerNondeterministic("zip", -1, udfZip)
	registerNondeterministic("shuffle", 1, udfShuffle)
	registerNondeterministic("trim_array", argsTwo, udfTrimArray)
	registerNondeterministic("flatten", 1, udfFlatten)
	registerScalar("array_union", argsTwo, setOperation(func(bool) bool { return true }, true))
	registerScalar("array_intersect", argsTwo, setOperation(func(inY bool) bool { return inY }, false))
	registerScalar("array_except", argsTwo, setOperation(func(inY bool) bool { return !inY }, false))
	registerScalar("arrays_overlap", argsTwo, udfArraysOverlap)
	registerScalar("array_first", 1, udfArrayFirst)
	registerScalar("array_last", 1, udfArrayLast)
	registerScalar("contains_sequence", argsTwo, udfContainsSequence)
}
