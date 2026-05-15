package asl

import (
	cryptorand "crypto/rand"
	"errors"
	"fmt"
	"maps"
	"reflect"
	"strings"
)

// Sentinel errors for the extended intrinsic function set.
var (
	ErrStatesUUIDNoArgs             = errors.New("States.UUID takes no arguments")
	ErrStatesMathAddRequiresTwoArgs = errors.New("States.MathAdd requires exactly two arguments")
	ErrStatesMathAddArgNotNumber    = errors.New("States.MathAdd: arguments must be numbers")
	ErrStatesArrayRangeRequiresArgs = errors.New(
		"States.ArrayRange requires exactly three arguments (start, end, step)",
	)
	ErrStatesArrayRangeArgNotNumber   = errors.New("States.ArrayRange: arguments must be numbers")
	ErrStatesArrayRangeStepZero       = errors.New("States.ArrayRange: step must be non-zero")
	ErrStatesArrayGetItemRequiresArgs = errors.New("States.ArrayGetItem requires exactly two arguments (array, index)")
	ErrStatesArrayGetItemNotArray     = errors.New("States.ArrayGetItem: first argument must be an array")
	ErrStatesArrayGetItemIndexRange   = errors.New("States.ArrayGetItem: index out of range")
	ErrStatesArrayUniqueRequiresArg   = errors.New("States.ArrayUnique requires exactly one argument")
	ErrStatesArrayUniqueArgNotArray   = errors.New("States.ArrayUnique: argument must be an array")
	ErrStatesJSONMergeRequiresArgs    = errors.New("States.JsonMerge requires exactly three arguments")
	ErrStatesJSONMergeArgNotObject    = errors.New("States.JsonMerge: first two arguments must be JSON objects")
	ErrStatesJSONMergeDeepUnsupported = errors.New(
		"States.JsonMerge: deep merge is not supported (third arg must be false)",
	)
	ErrStatesStringSplitRequiresArgs = errors.New("States.StringSplit requires exactly two arguments")
	ErrStatesStringSplitArgNotString = errors.New("States.StringSplit: arguments must be strings")
)

// intrinsicArgsThree is the expected length for three-argument intrinsics.
const intrinsicArgsThree = 3

// evalExtraIntrinsic dispatches the additional intrinsic functions added for
// closer parity with AWS Step Functions.
func evalExtraIntrinsic(fnName string, args []any) (any, error) {
	switch fnName {
	case "States.UUID":
		return intrinsicUUID(args)
	case "States.MathAdd":
		return intrinsicMathAdd(args)
	case "States.ArrayRange":
		return intrinsicArrayRange(args)
	case "States.ArrayGetItem":
		return intrinsicArrayGetItem(args)
	case "States.ArrayUnique":
		return intrinsicArrayUnique(args)
	case "States.JsonMerge":
		return intrinsicJSONMerge(args)
	case "States.StringSplit":
		return intrinsicStringSplit(args)
	}

	return nil, errIntrinsicNotHandled
}

// intrinsicUUID returns a v4 UUID per AWS States.UUID spec.
func intrinsicUUID(args []any) (any, error) {
	if len(args) != 0 {
		return nil, ErrStatesUUIDNoArgs
	}

	var b [16]byte
	if _, err := cryptorand.Read(b[:]); err != nil {
		return nil, fmt.Errorf("States.UUID: random read: %w", err)
	}

	// RFC 4122 version 4, variant 10xx.
	const (
		uuidVersionMask  = 0x0f
		uuidVersion4Bits = 0x40
		uuidVariantMask  = 0x3f
		uuidVariant10Bit = 0x80
	)

	b[6] = (b[6] & uuidVersionMask) | uuidVersion4Bits
	b[8] = (b[8] & uuidVariantMask) | uuidVariant10Bit

	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

// intrinsicMathAdd returns the arithmetic sum of two numeric arguments.
func intrinsicMathAdd(args []any) (any, error) {
	if len(args) != intrinsicTwoArgs {
		return nil, ErrStatesMathAddRequiresTwoArgs
	}

	a, ok := toFloat(args[0])
	if !ok {
		return nil, ErrStatesMathAddArgNotNumber
	}

	b, ok := toFloat(args[1])
	if !ok {
		return nil, ErrStatesMathAddArgNotNumber
	}

	return a + b, nil
}

// intrinsicArrayRange returns an array of numbers from start to end with step.
func intrinsicArrayRange(args []any) (any, error) {
	if len(args) != intrinsicArgsThree {
		return nil, ErrStatesArrayRangeRequiresArgs
	}

	start, ok := toFloat(args[0])
	if !ok {
		return nil, ErrStatesArrayRangeArgNotNumber
	}

	end, ok := toFloat(args[1])
	if !ok {
		return nil, ErrStatesArrayRangeArgNotNumber
	}

	step, ok := toFloat(args[2])
	if !ok {
		return nil, ErrStatesArrayRangeArgNotNumber
	}

	if step == 0 {
		return nil, ErrStatesArrayRangeStepZero
	}

	out := []any{}

	if step > 0 {
		for v := start; v <= end; v += step {
			out = append(out, v)
		}

		return out, nil
	}

	for v := start; v >= end; v += step {
		out = append(out, v)
	}

	return out, nil
}

// intrinsicArrayGetItem returns the element at the given index.
func intrinsicArrayGetItem(args []any) (any, error) {
	if len(args) != intrinsicTwoArgs {
		return nil, ErrStatesArrayGetItemRequiresArgs
	}

	arr, ok := args[0].([]any)
	if !ok {
		return nil, ErrStatesArrayGetItemNotArray
	}

	idxF, ok := toFloat(args[1])
	if !ok {
		return nil, ErrStatesArrayGetItemIndexRange
	}

	idx := int(idxF)
	if idx < 0 || idx >= len(arr) {
		return nil, fmt.Errorf("%w: %d (size %d)", ErrStatesArrayGetItemIndexRange, idx, len(arr))
	}

	return arr[idx], nil
}

// intrinsicArrayUnique returns a new array with duplicates removed, preserving order.
func intrinsicArrayUnique(args []any) (any, error) {
	if len(args) != 1 {
		return nil, ErrStatesArrayUniqueRequiresArg
	}

	arr, ok := args[0].([]any)
	if !ok {
		return nil, ErrStatesArrayUniqueArgNotArray
	}

	out := make([]any, 0, len(arr))

	for _, item := range arr {
		duplicate := false

		for _, existing := range out {
			if reflect.DeepEqual(existing, item) {
				duplicate = true

				break
			}
		}

		if !duplicate {
			out = append(out, item)
		}
	}

	return out, nil
}

// intrinsicJSONMerge implements the shallow merge variant of States.JsonMerge.
// AWS only supports deep=false at this time; we mirror that constraint.
func intrinsicJSONMerge(args []any) (any, error) {
	if len(args) != intrinsicArgsThree {
		return nil, ErrStatesJSONMergeRequiresArgs
	}

	left, ok := args[0].(map[string]any)
	if !ok {
		return nil, ErrStatesJSONMergeArgNotObject
	}

	right, ok := args[1].(map[string]any)
	if !ok {
		return nil, ErrStatesJSONMergeArgNotObject
	}

	deep, ok := args[2].(bool)
	if !ok || deep {
		return nil, ErrStatesJSONMergeDeepUnsupported
	}

	// Preallocate using the larger of the two inputs to avoid an integer-addition
	// overflow flagged by CodeQL go/allocation-size-overflow. The size hint is
	// only an optimization for `make(map)`, so a slightly low estimate is safe;
	// avoiding `len(left)+len(right)` removes the theoretical overflow path.
	out := make(map[string]any, max(len(left), len(right)))
	maps.Copy(out, left)
	maps.Copy(out, right)

	return out, nil
}

// intrinsicStringSplit splits a string on any of the delimiter characters.
func intrinsicStringSplit(args []any) (any, error) {
	if len(args) != intrinsicTwoArgs {
		return nil, ErrStatesStringSplitRequiresArgs
	}

	s, ok := args[0].(string)
	if !ok {
		return nil, ErrStatesStringSplitArgNotString
	}

	delims, ok := args[1].(string)
	if !ok {
		return nil, ErrStatesStringSplitArgNotString
	}

	if delims == "" {
		return []any{s}, nil
	}

	parts := strings.FieldsFunc(s, func(r rune) bool {
		return strings.ContainsRune(delims, r)
	})

	out := make([]any, len(parts))
	for i, p := range parts {
		out[i] = p
	}

	return out, nil
}
