package asl

import (
	"crypto/md5"  //nolint:gosec // MD5 is AWS-spec-mandated for States.Hash
	"crypto/sha1" //nolint:gosec // SHA-1 is AWS-spec-mandated for States.Hash
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"math/rand/v2"
	"reflect"
	"strconv"
	"strings"
)

// Sentinel errors for intrinsic function evaluation.
var (
	ErrInvalidIntrinsicSyntax               = errors.New("invalid intrinsic function syntax")
	ErrUnknownIntrinsicFunction             = errors.New("unknown intrinsic function")
	ErrInvalidIntrinsicArg                  = errors.New("invalid intrinsic function argument")
	ErrStatesFormatRequiresArg              = errors.New("States.Format requires at least one argument")
	ErrStatesFormatFirstArgNotString        = errors.New("States.Format: first argument must be a string")
	ErrStatesFormatNotEnoughArgs            = errors.New("States.Format: not enough arguments for placeholders")
	ErrStatesStringToJSONRequiresArg        = errors.New("States.StringToJson requires exactly one argument")
	ErrStatesStringToJSONArgNotString       = errors.New("States.StringToJson: argument must be a string")
	ErrStatesJSONToStringRequiresArg        = errors.New("States.JsonToString requires exactly one argument")
	ErrStatesArrayLengthRequiresArg         = errors.New("States.ArrayLength requires exactly one argument")
	ErrStatesArrayLengthArgNotArray         = errors.New("States.ArrayLength: argument must be an array")
	ErrStatesArrayContainsRequiresTwoArgs   = errors.New("States.ArrayContains requires exactly two arguments")
	ErrStatesArrayContainsFirstArgNotArray  = errors.New("States.ArrayContains: first argument must be an array")
	ErrStatesArrayPartitionRequiresTwoArgs  = errors.New("States.ArrayPartition requires exactly two arguments")
	ErrStatesArrayPartitionFirstArgNotArray = errors.New("States.ArrayPartition: first argument must be an array")
	ErrStatesArrayPartitionSizeNotPositive  = errors.New(
		"States.ArrayPartition: second argument must be a positive number",
	)
	ErrStatesMathRandomRequiresTwoArgs = errors.New("States.MathRandom requires at least two arguments (start, end)")
	ErrStatesMathRandomStartNotNumber  = errors.New("States.MathRandom: start must be a number")
	ErrStatesMathRandomEndNotNumber    = errors.New("States.MathRandom: end must be a number")
	ErrStatesMathRandomRange           = errors.New("States.MathRandom: end must be greater than start")
	ErrStatesBase64EncodeRequiresArg   = errors.New("States.Base64Encode requires exactly one argument")
	ErrStatesBase64EncodeArgNotString  = errors.New("States.Base64Encode: argument must be a string")
	ErrStatesBase64DecodeRequiresArg   = errors.New("States.Base64Decode requires exactly one argument")
	ErrStatesBase64DecodeArgNotString  = errors.New("States.Base64Decode: argument must be a string")
	ErrStatesHashRequiresTwoArgs       = errors.New("States.Hash requires exactly two arguments (data, algorithm)")
	ErrStatesHashFirstArgNotString     = errors.New("States.Hash: first argument must be a string")
	ErrStatesHashSecondArgNotString    = errors.New("States.Hash: second argument must be a string")
	ErrStatesHashUnsupportedAlgorithm  = errors.New("States.Hash: unsupported algorithm")
)

const intrinsicTwoArgs = 2

// evaluateIntrinsicFunction evaluates an ASL intrinsic function call such as
// States.Format, States.StringToJson, etc.
//
//nolint:cyclop // dispatches to 11 ASL intrinsic functions
func evaluateIntrinsicFunction(expr string, input any) (any, error) {
	parenIdx := strings.IndexByte(expr, '(')
	if parenIdx < 0 || !strings.HasSuffix(strings.TrimSpace(expr), ")") {
		return nil, fmt.Errorf("%w: %q", ErrInvalidIntrinsicSyntax, expr)
	}

	fnName := strings.TrimSpace(expr[:parenIdx])
	// Trim the outer parentheses to get the raw argument string.
	argsStr := expr[parenIdx+1 : strings.LastIndexByte(expr, ')')]

	args, err := parseIntrinsicArgs(argsStr, input)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", fnName, err)
	}

	switch fnName {
	case "States.Format":
		return intrinsicFormat(args)
	case "States.StringToJson":
		return intrinsicStringToJSON(args)
	case "States.JsonToString":
		return intrinsicJSONToString(args)
	case "States.Array":
		return args, nil
	case "States.ArrayLength":
		return intrinsicArrayLength(args)
	case "States.ArrayContains":
		return intrinsicArrayContains(args)
	case "States.ArrayPartition":
		return intrinsicArrayPartition(args)
	case "States.MathRandom":
		return intrinsicMathRandom(args)
	case "States.Base64Encode":
		return intrinsicBase64Encode(args)
	case "States.Base64Decode":
		return intrinsicBase64Decode(args)
	case "States.Hash":
		return intrinsicHash(args)
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnknownIntrinsicFunction, fnName)
	}
}

// parseIntrinsicArgs tokenizes a comma-separated argument list, respecting
// nested parentheses and single-quoted string literals.
func parseIntrinsicArgs(argsStr string, input any) ([]any, error) {
	tokens := splitIntrinsicArgs(argsStr)
	args := make([]any, 0, len(tokens))

	for _, tok := range tokens {
		tok = strings.TrimSpace(tok)
		if tok == "" {
			continue
		}

		val, err := evalIntrinsicArg(tok, input)
		if err != nil {
			return nil, err
		}

		args = append(args, val)
	}

	return args, nil
}

// splitIntrinsicArgs splits a comma-separated list respecting parentheses and
// single-quoted strings.
func splitIntrinsicArgs(s string) []string {
	var result []string
	var current strings.Builder
	depth := 0
	inString := false

	for _, c := range s {
		switch {
		case c == '\'' && !inString:
			inString = true
			current.WriteRune(c)
		case c == '\'' && inString:
			inString = false
			current.WriteRune(c)
		case inString:
			current.WriteRune(c)
		case c == '(':
			depth++
			current.WriteRune(c)
		case c == ')':
			depth--
			current.WriteRune(c)
		case c == ',' && depth == 0:
			result = append(result, current.String())
			current.Reset()
		default:
			current.WriteRune(c)
		}
	}

	// Append the last token (even if empty, for trailing comma detection).
	result = append(result, current.String())

	return result
}

// evalIntrinsicArg evaluates a single argument token.
func evalIntrinsicArg(arg string, input any) (any, error) {
	// Single-quoted string literal.
	if len(arg) >= 2 && arg[0] == '\'' && arg[len(arg)-1] == '\'' {
		return arg[1 : len(arg)-1], nil
	}

	// JSONPath expression.
	if strings.HasPrefix(arg, "$.") || arg == "$" || arg == "$$" {
		return applyPath(arg, input)
	}

	// Nested intrinsic function.
	if strings.HasPrefix(arg, "States.") {
		return evaluateIntrinsicFunction(arg, input)
	}

	// Null literal.
	if arg == "null" {
		return nil, nil //nolint:nilnil // null is a valid ASL literal value
	}

	// Boolean literals.
	if arg == "true" {
		return true, nil
	}

	if arg == "false" {
		return false, nil
	}

	// Numeric literal.
	if n, err := strconv.ParseFloat(arg, 64); err == nil {
		return n, nil
	}

	return nil, fmt.Errorf("%w: %q", ErrInvalidIntrinsicArg, arg)
}

// intrinsicFormat implements States.Format('template {}', $.arg1, ...).
func intrinsicFormat(args []any) (any, error) {
	if len(args) < 1 {
		return nil, ErrStatesFormatRequiresArg
	}

	template, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("%w, got %T", ErrStatesFormatFirstArgNotString, args[0])
	}

	var result strings.Builder
	argIdx := 1

	for i := 0; i < len(template); i++ {
		if template[i] == '{' && i+1 < len(template) && template[i+1] == '}' {
			if argIdx >= len(args) {
				return nil, fmt.Errorf("%w in %q", ErrStatesFormatNotEnoughArgs, template)
			}

			val := args[argIdx]
			argIdx++

			switch v := val.(type) {
			case string:
				result.WriteString(v)
			case nil:
				result.WriteString("null")
			default:
				b, err := json.Marshal(v)
				if err != nil {
					return nil, fmt.Errorf("States.Format: cannot serialize argument: %w", err)
				}

				result.Write(b)
			}

			i++ // skip the '}'
		} else {
			result.WriteByte(template[i])
		}
	}

	return result.String(), nil
}

// intrinsicStringToJSON implements States.StringToJson($.jsonString).
func intrinsicStringToJSON(args []any) (any, error) {
	if len(args) != 1 {
		return nil, ErrStatesStringToJSONRequiresArg
	}

	s, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("%w, got %T", ErrStatesStringToJSONArgNotString, args[0])
	}

	var result any
	if err := json.Unmarshal([]byte(s), &result); err != nil {
		return nil, fmt.Errorf("States.StringToJson: invalid JSON: %w", err)
	}

	return result, nil
}

// intrinsicJSONToString implements States.JsonToString($.value).
func intrinsicJSONToString(args []any) (any, error) {
	if len(args) != 1 {
		return nil, ErrStatesJSONToStringRequiresArg
	}

	b, err := json.Marshal(args[0])
	if err != nil {
		return nil, fmt.Errorf("States.JsonToString: cannot serialize: %w", err)
	}

	return string(b), nil
}

// intrinsicArrayLength implements States.ArrayLength($.arr).
func intrinsicArrayLength(args []any) (any, error) {
	if len(args) != 1 {
		return nil, ErrStatesArrayLengthRequiresArg
	}

	arr, ok := args[0].([]any)
	if !ok {
		return nil, fmt.Errorf("%w, got %T", ErrStatesArrayLengthArgNotArray, args[0])
	}

	return float64(len(arr)), nil
}

// intrinsicArrayContains implements States.ArrayContains($.arr, value).
func intrinsicArrayContains(args []any) (any, error) {
	if len(args) != intrinsicTwoArgs {
		return nil, ErrStatesArrayContainsRequiresTwoArgs
	}

	arr, ok := args[0].([]any)
	if !ok {
		return nil, fmt.Errorf("%w, got %T", ErrStatesArrayContainsFirstArgNotArray, args[0])
	}

	target := args[1]

	for _, item := range arr {
		if reflect.DeepEqual(item, target) {
			return true, nil
		}
	}

	return false, nil
}

// intrinsicArrayPartition implements States.ArrayPartition($.arr, chunkSize).
func intrinsicArrayPartition(args []any) (any, error) {
	if len(args) != intrinsicTwoArgs {
		return nil, ErrStatesArrayPartitionRequiresTwoArgs
	}

	arr, ok := args[0].([]any)
	if !ok {
		return nil, fmt.Errorf("%w, got %T", ErrStatesArrayPartitionFirstArgNotArray, args[0])
	}

	sizeF, ok := toFloat(args[1])
	if !ok || sizeF <= 0 {
		return nil, ErrStatesArrayPartitionSizeNotPositive
	}

	size := int(sizeF)
	chunks := make([]any, 0, (len(arr)+size-1)/size)

	for i := 0; i < len(arr); i += size {
		end := min(i+size, len(arr))

		chunk := make([]any, end-i)
		copy(chunk, arr[i:end])
		chunks = append(chunks, chunk)
	}

	return chunks, nil
}

// intrinsicMathRandom implements States.MathRandom(start, end).
func intrinsicMathRandom(args []any) (any, error) {
	if len(args) < intrinsicTwoArgs {
		return nil, ErrStatesMathRandomRequiresTwoArgs
	}

	startF, ok := toFloat(args[0])
	if !ok {
		return nil, ErrStatesMathRandomStartNotNumber
	}

	endF, ok := toFloat(args[1])
	if !ok {
		return nil, ErrStatesMathRandomEndNotNumber
	}

	start := int64(startF)
	end := int64(endF)

	if end <= start {
		return nil, fmt.Errorf("%w: end=%d, start=%d", ErrStatesMathRandomRange, end, start)
	}

	r := start + rand.Int64N(end-start+1) //nolint:gosec // non-cryptographic per ASL spec

	return float64(r), nil
}

// intrinsicBase64Encode implements States.Base64Encode($.str).
func intrinsicBase64Encode(args []any) (any, error) {
	if len(args) != 1 {
		return nil, ErrStatesBase64EncodeRequiresArg
	}

	s, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("%w, got %T", ErrStatesBase64EncodeArgNotString, args[0])
	}

	return base64.StdEncoding.EncodeToString([]byte(s)), nil
}

// intrinsicBase64Decode implements States.Base64Decode($.encoded).
func intrinsicBase64Decode(args []any) (any, error) {
	if len(args) != 1 {
		return nil, ErrStatesBase64DecodeRequiresArg
	}

	s, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("%w, got %T", ErrStatesBase64DecodeArgNotString, args[0])
	}

	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("States.Base64Decode: invalid base64: %w", err)
	}

	return string(b), nil
}

// intrinsicHash implements States.Hash($.data, 'algorithm').
// Supported algorithms: MD5, SHA-1, SHA-256.
func intrinsicHash(args []any) (any, error) {
	if len(args) != intrinsicTwoArgs {
		return nil, ErrStatesHashRequiresTwoArgs
	}

	data, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("%w, got %T", ErrStatesHashFirstArgNotString, args[0])
	}

	algo, ok := args[1].(string)
	if !ok {
		return nil, fmt.Errorf("%w, got %T", ErrStatesHashSecondArgNotString, args[1])
	}

	var h hash.Hash

	switch strings.ToUpper(algo) {
	case "MD5":
		h = md5.New() //nolint:gosec // MD5 required by AWS States.Hash spec
	case "SHA-1":
		h = sha1.New() //nolint:gosec // SHA-1 required by AWS States.Hash spec
	case "SHA-256":
		h = sha256.New()
	default:
		return nil, fmt.Errorf("%w %q (supported: MD5, SHA-1, SHA-256)", ErrStatesHashUnsupportedAlgorithm, algo)
	}

	h.Write([]byte(data))

	return hex.EncodeToString(h.Sum(nil)), nil
}
