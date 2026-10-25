package kql

import "fmt"

// Error is a query failure in the documented BadArgumentError shape.
type Error struct {
	// Kind is the inner error code: SyntaxError or SemanticError.
	Kind string
	// Code is the innermost code, e.g. SYN0002 or SEM0100.
	Code    string
	Message string
}

func (e *Error) Error() string { return e.Kind + ": " + e.Message }

func syntaxErr(format string, a ...any) *Error {
	return &Error{Kind: "SyntaxError", Code: "SYN0002", Message: fmt.Sprintf(format, a...)}
}

func semErr(format string, a ...any) *Error {
	return &Error{Kind: "SemanticError", Code: "SEM0100", Message: fmt.Sprintf(format, a...)}
}

func unsupported(what string) *Error {
	return &Error{
		Kind: "SemanticError", Code: "SEM0001",
		Message: fmt.Sprintf("%s is not supported by this emulator", what),
	}
}
