package asl

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

// Sentinel errors for the parity-extension intrinsic functions.
var (
	ErrStatesStringConcatNoArgs       = errors.New("States.StringConcat requires at least one argument")
	ErrStatesStringConcatArgNotString = errors.New("States.StringConcat: arguments must be strings")
	ErrStatesArraySliceRequiresArgs   = errors.New("States.ArraySlice requires array, start, and end arguments")
	ErrStatesArraySliceNotArray       = errors.New("States.ArraySlice: first argument must be an array")
	ErrStatesArraySliceIndexNotNumber = errors.New("States.ArraySlice: start/end must be numbers")
	ErrStatesArrayFlattenRequiresArg  = errors.New("States.ArrayFlatten requires exactly one argument")
	ErrStatesArrayFlattenNotArray     = errors.New("States.ArrayFlatten: argument must be an array")
	ErrStatesArrayReverseRequiresArg  = errors.New("States.ArrayReverse requires exactly one argument")
	ErrStatesArrayReverseNotArray     = errors.New("States.ArrayReverse: argument must be an array")
	ErrStatesArraySortRequiresArg     = errors.New("States.ArraySort requires exactly one argument")
	ErrStatesArraySortNotArray        = errors.New("States.ArraySort: argument must be an array")
	ErrStatesArraySortMixedTypes      = errors.New("States.ArraySort: array must contain only numbers or only strings")
	ErrStatesMathSubRequiresTwoArgs   = errors.New("States.MathSubtract requires exactly two arguments")
	ErrStatesMathMulRequiresTwoArgs   = errors.New("States.MathMultiply requires exactly two arguments")
	ErrStatesMathDivRequiresTwoArgs   = errors.New("States.MathDivide requires exactly two arguments")
	ErrStatesMathModRequiresTwoArgs   = errors.New("States.MathMod requires exactly two arguments")
	ErrStatesMathMinRequiresTwoArgs   = errors.New("States.MathMin requires exactly two arguments")
	ErrStatesMathMaxRequiresTwoArgs   = errors.New("States.MathMax requires exactly two arguments")
	ErrStatesMathArgNotNumber         = errors.New("math intrinsic: arguments must be numbers")
	ErrStatesMathDivideByZero         = errors.New("States.MathDivide: division by zero")
	ErrStatesMathModByZero            = errors.New("States.MathMod: modulo by zero")
	ErrStatesStringLengthRequiresArg  = errors.New("States.StringLength requires exactly one argument")
	ErrStatesStringLengthArgNotString = errors.New("States.StringLength: argument must be a string")
	ErrStatesStringCaseRequiresArg    = errors.New("States.StringToLower/Upper require exactly one argument")
	ErrStatesStringCaseArgNotString   = errors.New("States.StringToLower/Upper: argument must be a string")
	ErrStatesStringIndexRequiresArgs  = errors.New("States.StringIndex requires exactly two arguments (string, substr)")
	ErrStatesStringIndexArgNotString  = errors.New("States.StringIndex: arguments must be strings")
)

// evalParityIntrinsic dispatches the second batch of parity intrinsics so that
// the original dispatcher map stays under the cyclomatic complexity budget.
func evalParityIntrinsic(fnName string, args []any) (any, error) {
	switch fnName {
	case "States.StringConcat":
		return intrinsicStringConcat(args)
	case "States.StringLength":
		return intrinsicStringLength(args)
	case "States.StringToLower":
		return intrinsicStringChangeCase(args, strings.ToLower)
	case "States.StringToUpper":
		return intrinsicStringChangeCase(args, strings.ToUpper)
	case "States.StringIndex":
		return intrinsicStringIndex(args)
	case "States.ArraySlice":
		return intrinsicArraySlice(args)
	case "States.ArrayFlatten":
		return intrinsicArrayFlatten(args)
	case "States.ArrayReverse":
		return intrinsicArrayReverse(args)
	case "States.ArraySort":
		return intrinsicArraySort(args)
	}

	return evalMathParityIntrinsic(fnName, args)
}

// evalMathParityIntrinsic dispatches the math parity intrinsic family.
func evalMathParityIntrinsic(fnName string, args []any) (any, error) {
	switch fnName {
	case "States.MathSubtract":
		return intrinsicBinaryMath(
			args,
			ErrStatesMathSubRequiresTwoArgs,
			func(a, b float64) (float64, error) { return a - b, nil },
		)
	case "States.MathMultiply":
		return intrinsicBinaryMath(
			args,
			ErrStatesMathMulRequiresTwoArgs,
			func(a, b float64) (float64, error) { return a * b, nil },
		)
	case "States.MathDivide":
		return intrinsicBinaryMath(args, ErrStatesMathDivRequiresTwoArgs, func(a, b float64) (float64, error) {
			if b == 0 {
				return 0, ErrStatesMathDivideByZero
			}

			return a / b, nil
		})
	case "States.MathMod":
		return intrinsicBinaryMath(args, ErrStatesMathModRequiresTwoArgs, func(a, b float64) (float64, error) {
			if b == 0 {
				return 0, ErrStatesMathModByZero
			}

			return float64(int64(a) % int64(b)), nil
		})
	case "States.MathMin":
		return intrinsicBinaryMath(args, ErrStatesMathMinRequiresTwoArgs, func(a, b float64) (float64, error) {
			if a < b {
				return a, nil
			}

			return b, nil
		})
	case "States.MathMax":
		return intrinsicBinaryMath(args, ErrStatesMathMaxRequiresTwoArgs, func(a, b float64) (float64, error) {
			if a > b {
				return a, nil
			}

			return b, nil
		})
	}

	return nil, errIntrinsicNotHandled
}

func intrinsicBinaryMath(
	args []any,
	arityErr error,
	op func(a, b float64) (float64, error),
) (any, error) {
	if len(args) != intrinsicTwoArgs {
		return nil, arityErr
	}

	a, ok := toFloat(args[0])
	if !ok {
		return nil, ErrStatesMathArgNotNumber
	}

	b, ok := toFloat(args[1])
	if !ok {
		return nil, ErrStatesMathArgNotNumber
	}

	r, err := op(a, b)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func intrinsicStringConcat(args []any) (any, error) {
	if len(args) == 0 {
		return nil, ErrStatesStringConcatNoArgs
	}

	var b strings.Builder

	for _, a := range args {
		s, ok := a.(string)
		if !ok {
			return nil, fmt.Errorf("%w, got %T", ErrStatesStringConcatArgNotString, a)
		}

		b.WriteString(s)
	}

	return b.String(), nil
}

func intrinsicStringLength(args []any) (any, error) {
	if len(args) != 1 {
		return nil, ErrStatesStringLengthRequiresArg
	}

	s, ok := args[0].(string)
	if !ok {
		return nil, ErrStatesStringLengthArgNotString
	}

	return float64(len([]rune(s))), nil
}

func intrinsicStringChangeCase(args []any, fn func(string) string) (any, error) {
	if len(args) != 1 {
		return nil, ErrStatesStringCaseRequiresArg
	}

	s, ok := args[0].(string)
	if !ok {
		return nil, ErrStatesStringCaseArgNotString
	}

	return fn(s), nil
}

func intrinsicStringIndex(args []any) (any, error) {
	if len(args) != intrinsicTwoArgs {
		return nil, ErrStatesStringIndexRequiresArgs
	}

	s, ok := args[0].(string)
	if !ok {
		return nil, ErrStatesStringIndexArgNotString
	}

	sub, ok := args[1].(string)
	if !ok {
		return nil, ErrStatesStringIndexArgNotString
	}

	return float64(strings.Index(s, sub)), nil
}

func intrinsicArraySlice(args []any) (any, error) {
	if len(args) != intrinsicArgsThree {
		return nil, ErrStatesArraySliceRequiresArgs
	}

	arr, ok := args[0].([]any)
	if !ok {
		return nil, ErrStatesArraySliceNotArray
	}

	startF, ok := toFloat(args[1])
	if !ok {
		return nil, ErrStatesArraySliceIndexNotNumber
	}

	endF, ok := toFloat(args[2])
	if !ok {
		return nil, ErrStatesArraySliceIndexNotNumber
	}

	start, end := clampSlice(int(startF), int(endF), len(arr))
	out := make([]any, end-start)
	copy(out, arr[start:end])

	return out, nil
}

func clampSlice(start, end, length int) (int, int) {
	if start < 0 {
		start = 0
	}

	if end > length {
		end = length
	}

	if start > end {
		start = end
	}

	return start, end
}

func intrinsicArrayFlatten(args []any) (any, error) {
	if len(args) != 1 {
		return nil, ErrStatesArrayFlattenRequiresArg
	}

	arr, ok := args[0].([]any)
	if !ok {
		return nil, ErrStatesArrayFlattenNotArray
	}

	out := make([]any, 0, len(arr))

	for _, item := range arr {
		if sub, isArr := item.([]any); isArr {
			out = append(out, sub...)
		} else {
			out = append(out, item)
		}
	}

	return out, nil
}

func intrinsicArrayReverse(args []any) (any, error) {
	if len(args) != 1 {
		return nil, ErrStatesArrayReverseRequiresArg
	}

	arr, ok := args[0].([]any)
	if !ok {
		return nil, ErrStatesArrayReverseNotArray
	}

	out := make([]any, len(arr))
	for i, v := range arr {
		out[len(arr)-1-i] = v
	}

	return out, nil
}

func intrinsicArraySort(args []any) (any, error) {
	if len(args) != 1 {
		return nil, ErrStatesArraySortRequiresArg
	}

	arr, ok := args[0].([]any)
	if !ok {
		return nil, ErrStatesArraySortNotArray
	}

	if len(arr) == 0 {
		return []any{}, nil
	}

	out := make([]any, len(arr))
	copy(out, arr)

	switch out[0].(type) {
	case string:
		return sortStringsArray(out)
	default:
		return sortNumbersArray(out)
	}
}

func sortStringsArray(arr []any) ([]any, error) {
	strs := make([]string, len(arr))

	for i, v := range arr {
		s, ok := v.(string)
		if !ok {
			return nil, ErrStatesArraySortMixedTypes
		}

		strs[i] = s
	}

	sort.Strings(strs)

	out := make([]any, len(strs))
	for i, s := range strs {
		out[i] = s
	}

	return out, nil
}

func sortNumbersArray(arr []any) ([]any, error) {
	nums := make([]float64, len(arr))
	for i, v := range arr {
		f, ok := toFloat(v)
		if !ok {
			return nil, ErrStatesArraySortMixedTypes
		}

		nums[i] = f
	}

	sort.Float64s(nums)

	out := make([]any, len(nums))
	for i, n := range nums {
		out[i] = n
	}

	return out, nil
}

// reflectEqual placeholder removed; structural equality is provided by
// reflect.DeepEqual where needed in other intrinsic implementations.
