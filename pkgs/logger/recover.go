package logger

import (
	"context"
	"runtime/debug"
)

// Recover is deferred directly (`defer logger.Recover(ctx, "x", fn)`) at the top of a goroutine that serves a
// client outside net/http's handler recovery. It logs the panic with its stack and then runs onPanic (may be
// nil) so the caller can send a protocol-level error or close that one connection.
func Recover(ctx context.Context, component string, onPanic func(r any)) {
	r := recover()
	if r == nil {
		return
	}

	Load(ctx).ErrorContext(ctx, "recovered from panic serving connection",
		"component", component,
		"panic", r,
		"stack", string(debug.Stack()),
	)

	if onPanic != nil {
		onPanic(r)
	}
}
