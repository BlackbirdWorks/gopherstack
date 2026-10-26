package wfinterp

import (
	"fmt"
	"strings"
)

// Error tags used by the runtime, as listed in the Workflows error reference.
const (
	TagTypeError          = "TypeError"
	TagValueError         = "ValueError"
	TagKeyError           = "KeyError"
	TagIndexError         = "IndexError"
	TagZeroDivisionError  = "ZeroDivisionError"
	TagArithmeticError    = "ArithmeticError"
	TagHTTPError          = "HttpError"
	TagConnectionError    = "ConnectionError"
	TagConnectionFailed   = "ConnectionFailedError"
	TagTimeoutError       = "TimeoutError"
	TagRecursionError     = "RecursionError"
	TagResourceLimitError = "ResourceLimitError"
	TagResponseTypeError  = "ResponseTypeError"
	TagOperationError     = "OperationError"
	TagUnhandledBranch    = "UnhandledBranchError"
	TagRuntimeError       = "RuntimeError"
	TagAuthError          = "AuthError"
	TagSystemError        = "SystemError"
)

// Frame is one position in a workflow: a step of a routine.
type Frame struct {
	Routine string
	Step    string
	Line    int
}

// Error is a workflow error: the value a raise produced or a runtime error map.
type Error struct {
	Value  any
	Stack  []Frame
	cancel bool
	fatal  bool
}

func (e *Error) Error() string {
	raw, err := ToJSON(e.Value)
	if err != nil {
		return fmt.Sprint(e.Value)
	}

	return string(raw)
}

// Payload is the error value as JSON.
func (e *Error) Payload() string {
	raw, err := ToJSON(e.Value)
	if err != nil {
		return typeNull
	}

	return string(raw)
}

// Cancelled reports whether the error is the execution being cancelled rather than a catchable error.
func (e *Error) Cancelled() bool { return e.cancel }

// Context renders the position the way Execution.Error.Context shows it.
func (e *Error) Context() string {
	kind := "RuntimeError"
	if m, ok := e.Value.(map[string]any); ok {
		if msg, isStr := m["message"].(string); isStr && msg != "" {
			kind = "RuntimeError: " + msg
		}
	} else {
		kind = "RuntimeError: " + formatValue(e.Value)
	}

	if len(e.Stack) == 0 {
		return kind
	}

	f := e.Stack[0]

	return fmt.Sprintf("%s\nin step %q, routine %q, line: %d", kind, f.Step, f.Routine, f.Line)
}

func errMap(msg string, tags ...string) map[string]any {
	ts := make([]any, len(tags))
	for i, t := range tags {
		ts[i] = t
	}

	return map[string]any{"message": msg, "tags": ts}
}

func rtErr(tag, format string, a ...any) *Error {
	tags := []string{tag}
	if tag == TagZeroDivisionError {
		tags = append(tags, TagArithmeticError)
	}

	return &Error{Value: errMap(fmt.Sprintf(format, a...), tags...)}
}

func typeErr(format string, a ...any) *Error { return rtErr(TagTypeError, format, a...) }

func valueErr(format string, a ...any) *Error { return rtErr(TagValueError, format, a...) }

// hasTag reports whether an error value carries tag.
func hasTag(v any, tag string) bool {
	m, ok := v.(map[string]any)
	if !ok {
		return false
	}

	tags, _ := m["tags"].([]any)
	for _, t := range tags {
		if s, isStr := t.(string); isStr && s == tag {
			return true
		}
	}

	return false
}

// SyntaxError is a workflow that fails validation at deploy time.
type SyntaxError struct {
	Routine string
	Step    string
	Msg     string
	Line    int
}

func (e *SyntaxError) Error() string {
	var b strings.Builder

	b.WriteString("error in ")

	if e.Routine != "" {
		b.WriteString("subworkflow '" + e.Routine + "'")
	} else {
		b.WriteString("workflow")
	}

	if e.Step != "" {
		b.WriteString(", step '" + e.Step + "'")
	}

	b.WriteString(": " + e.Msg)

	if e.Line > 0 {
		fmt.Fprintf(&b, " (line %d)", e.Line)
	}

	return b.String()
}

// NewError builds a catchable error map with the given message and tags, for hosts such as connectors.
func NewError(msg string, tags ...string) *Error { return &Error{Value: errMap(msg, tags...)} }

// NewHTTPError builds the error a failed HTTP call raises.
func NewHTTPError(code int, headers map[string][]string, body any, msg string) *Error {
	h := make(map[string]any, len(headers))
	for k, v := range headers {
		h[k] = strings.Join(v, ",")
	}

	if msg == "" {
		msg = fmt.Sprintf("HTTP server responded with error code %d", code)
	}

	m := errMap(msg, TagHTTPError)
	m["code"], m["headers"], m["body"] = int64(code), h, body

	return &Error{Value: m}
}
