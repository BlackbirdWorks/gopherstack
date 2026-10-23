package jsruntime

import (
	"errors"

	"github.com/dop251/goja"
)

// ErrNoFunction means the requested global function is not defined.
var ErrNoFunction = errors.New("function not defined")

// Kind classifies a script failure.
type Kind int

const (
	// KindOther is any failure that is neither a timeout nor a JS exception.
	KindOther Kind = iota
	// KindTimeout is a deadline interrupt.
	KindTimeout
	// KindException is an uncaught JS exception.
	KindException
)

// Classify returns the failure kind and its message (the thrown value's string
// form for exceptions).
func Classify(err error) (Kind, string) {
	if _, ok := errors.AsType[*goja.InterruptedError](err); ok {
		return KindTimeout, "execution timed out"
	}

	if _, ok := errors.AsType[*goja.StackOverflowError](err); ok {
		return KindException, "RangeError: Maximum call stack size exceeded"
	}

	if exc, ok := errors.AsType[*goja.Exception](err); ok {
		return KindException, exc.Value().String()
	}

	return KindOther, err.Error()
}
