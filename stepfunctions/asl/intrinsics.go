package asl

import (
	"crypto/md5"  //nolint:gosec // MD5 is AWS-spec-mandated for States.Hash
	"crypto/sha1" //nolint:gosec // SHA-1 is AWS-spec-mandated for States.Hash
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"math/rand/v2"
	"reflect"
	"strconv"
	"strings"
)

// evaluateIntrinsicFunction evaluates an ASL intrinsic function call such as
// States.Format, States.StringToJson, etc.
func evaluateIntrinsicFunction(expr string, input any) (any, error) {
	parenIdx := strings.IndexByte(expr, '(')
	if parenIdx < 0 || !strings.HasSuffix(strings.TrimSpace(expr), ")") {
		return nil, fmt.Errorf("invalid intrinsic function syntax: %q", expr)
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
		return intrinsicStringToJson(args)
	case "States.JsonToString":
		return intrinsicJsonToString(args)
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
		return nil, fmt.Errorf("unknown intrinsic function: %q", fnName)
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

	for i := 0; i < len(s); i++ {
		c := s[i]

		switch {
		case c == '\'' && !inString:
			inString = true
			current.WriteByte(c)
		case c == '\'' && inString:
			inString = false
			current.WriteByte(c)
		case inString:
			current.WriteByte(c)
		case c == '(':
			depth++
			current.WriteByte(c)
		case c == ')':
			depth--
			current.WriteByte(c)
		case c == ',' && depth == 0:
			result = append(result, current.String())
			current.Reset()
		default:
			current.WriteByte(c)
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
		return nil, nil
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

	return nil, fmt.Errorf("invalid intrinsic function argument: %q", arg)
}

// intrinsicFormat implements States.Format('template {}', $.arg1, ...).
func intrinsicFormat(args []any) (any, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("States.Format requires at least one argument")
	}

	template, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("States.Format: first argument must be a string, got %T", args[0])
	}

	var result strings.Builder
	argIdx := 1

	for i := 0; i < len(template); i++ {
		if template[i] == '{' && i+1 < len(template) && template[i+1] == '}' {
			if argIdx >= len(args) {
				return nil, fmt.Errorf("States.Format: not enough arguments for placeholders in %q", template)
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

// intrinsicStringToJson implements States.StringToJson($.jsonString).
func intrinsicStringToJson(args []any) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("States.StringToJson requires exactly one argument")
	}

	s, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("States.StringToJson: argument must be a string, got %T", args[0])
	}

	var result any
	if err := json.Unmarshal([]byte(s), &result); err != nil {
		return nil, fmt.Errorf("States.StringToJson: invalid JSON: %w", err)
	}

	return result, nil
}

// intrinsicJsonToString implements States.JsonToString($.value).
func intrinsicJsonToString(args []any) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("States.JsonToString requires exactly one argument")
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
		return nil, fmt.Errorf("States.ArrayLength requires exactly one argument")
	}

	arr, ok := args[0].([]any)
	if !ok {
		return nil, fmt.Errorf("States.ArrayLength: argument must be an array, got %T", args[0])
	}

	return float64(len(arr)), nil
}

// intrinsicArrayContains implements States.ArrayContains($.arr, value).
func intrinsicArrayContains(args []any) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("States.ArrayContains requires exactly two arguments")
	}

	arr, ok := args[0].([]any)
	if !ok {
		return nil, fmt.Errorf("States.ArrayContains: first argument must be an array, got %T", args[0])
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
	if len(args) != 2 {
		return nil, fmt.Errorf("States.ArrayPartition requires exactly two arguments")
	}

	arr, ok := args[0].([]any)
	if !ok {
		return nil, fmt.Errorf("States.ArrayPartition: first argument must be an array, got %T", args[0])
	}

	sizeF, ok := toFloat(args[1])
	if !ok || sizeF <= 0 {
		return nil, fmt.Errorf("States.ArrayPartition: second argument must be a positive number")
	}

	size := int(sizeF)
	chunks := make([]any, 0, (len(arr)+size-1)/size)

	for i := 0; i < len(arr); i += size {
		end := i + size
		if end > len(arr) {
			end = len(arr)
		}

		chunk := make([]any, end-i)
		copy(chunk, arr[i:end])
		chunks = append(chunks, chunk)
	}

	return chunks, nil
}

// intrinsicMathRandom implements States.MathRandom(start, end).
func intrinsicMathRandom(args []any) (any, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("States.MathRandom requires at least two arguments (start, end)")
	}

	startF, ok := toFloat(args[0])
	if !ok {
		return nil, fmt.Errorf("States.MathRandom: start must be a number")
	}

	endF, ok := toFloat(args[1])
	if !ok {
		return nil, fmt.Errorf("States.MathRandom: end must be a number")
	}

	start := int64(startF)
	end := int64(endF)

	if end <= start {
		return nil, fmt.Errorf("States.MathRandom: end (%d) must be greater than start (%d)", end, start)
	}

	r := start + rand.Int64N(end-start+1)

	return float64(r), nil
}

// intrinsicBase64Encode implements States.Base64Encode($.str).
func intrinsicBase64Encode(args []any) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("States.Base64Encode requires exactly one argument")
	}

	s, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("States.Base64Encode: argument must be a string, got %T", args[0])
	}

	return base64.StdEncoding.EncodeToString([]byte(s)), nil
}

// intrinsicBase64Decode implements States.Base64Decode($.encoded).
func intrinsicBase64Decode(args []any) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("States.Base64Decode requires exactly one argument")
	}

	s, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("States.Base64Decode: argument must be a string, got %T", args[0])
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
	if len(args) != 2 {
		return nil, fmt.Errorf("States.Hash requires exactly two arguments (data, algorithm)")
	}

	data, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("States.Hash: first argument must be a string, got %T", args[0])
	}

	algo, ok := args[1].(string)
	if !ok {
		return nil, fmt.Errorf("States.Hash: second argument must be a string, got %T", args[1])
	}

	var h hash.Hash

	switch strings.ToUpper(algo) {
	case "MD5":
		h = md5.New() //nolint:gosec
	case "SHA-1":
		h = sha1.New() //nolint:gosec
	case "SHA-256":
		h = sha256.New()
	default:
		return nil, fmt.Errorf("States.Hash: unsupported algorithm %q (supported: MD5, SHA-1, SHA-256)", algo)
	}

	h.Write([]byte(data))

	return hex.EncodeToString(h.Sum(nil)), nil
}
