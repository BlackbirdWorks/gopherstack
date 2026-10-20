// Package jsruntime is the shared sandboxed JavaScript runtime (goja) behind
// APPSYNC_JS, CloudFront Functions and Cosmos stored procedures. A Sandbox
// exposes only the ECMAScript builtins plus what the caller binds: no require,
// process, fs or network.
package jsruntime

import (
	"fmt"
	"time"

	"github.com/dop251/goja"
)

const (
	// DefaultTimeout bounds one script execution when Options.Timeout is zero.
	DefaultTimeout = 750 * time.Millisecond
	// DefaultMaxCallStack bounds JS recursion when Options.MaxCallStack is zero.
	DefaultMaxCallStack = 200
	// DefaultMaxLogLines caps captured console lines when Options.MaxLogLines is zero.
	DefaultMaxLogLines = 100
)

// Options tunes a Sandbox.
type Options struct {
	// InterruptMsg is the message carried by the timeout interrupt.
	InterruptMsg string
	// Timeout is the wall-clock limit for the sandbox lifetime.
	Timeout time.Duration
	// MaxCallStack is the maximum JS call depth.
	MaxCallStack int
	// MaxLogLines caps captured console lines.
	MaxLogLines int
	// Console installs a capturing console object when true.
	Console bool
}

// Sandbox is one single-use goja VM with a deadline.
type Sandbox struct {
	VM       *goja.Runtime
	timer    *time.Timer
	started  time.Time
	interMsg string
	logs     []string
	maxLogs  int
}

// New creates a Sandbox; call Close when done.
func New(opts Options) *Sandbox {
	if opts.Timeout <= 0 {
		opts.Timeout = DefaultTimeout
	}

	if opts.MaxCallStack <= 0 {
		opts.MaxCallStack = DefaultMaxCallStack
	}

	if opts.MaxLogLines <= 0 {
		opts.MaxLogLines = DefaultMaxLogLines
	}

	if opts.InterruptMsg == "" {
		opts.InterruptMsg = "execution timed out"
	}

	vm := goja.New()
	vm.SetMaxCallStackSize(opts.MaxCallStack)

	s := &Sandbox{VM: vm, started: time.Now(), interMsg: opts.InterruptMsg, maxLogs: opts.MaxLogLines}
	s.timer = time.AfterFunc(opts.Timeout, func() { vm.Interrupt(opts.InterruptMsg) })

	if opts.Console {
		s.installConsole()
	}

	return s
}

// Rearm restarts the deadline and Elapsed clock with a fresh timeout, discarding any pending interrupt.
func (s *Sandbox) Rearm(timeout time.Duration) {
	s.timer.Stop()
	s.VM.ClearInterrupt()
	s.started = time.Now()
	s.timer = time.AfterFunc(timeout, func() { s.VM.Interrupt(s.interMsg) })
}

// Close stops the deadline timer.
func (s *Sandbox) Close() { s.timer.Stop() }

// Elapsed is the time since the sandbox was created.
func (s *Sandbox) Elapsed() time.Duration { return time.Since(s.started) }

// Logs returns the console lines captured so far.
func (s *Sandbox) Logs() []string { return append([]string(nil), s.logs...) }

// Run evaluates src and returns its completion value.
func (s *Sandbox) Run(src string) (goja.Value, error) {
	v, err := s.VM.RunString(src)
	if err != nil {
		return nil, fmt.Errorf("run: %w", err)
	}

	return v, nil
}

// Call invokes the global function name with args.
func (s *Sandbox) Call(name string, args ...goja.Value) (goja.Value, error) {
	fn, ok := goja.AssertFunction(s.VM.Get(name))
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrNoFunction, name)
	}

	v, err := fn(goja.Undefined(), args...)
	if err != nil {
		return nil, fmt.Errorf("call %s: %w", name, err)
	}

	return v, nil
}

// Set binds a Go value as a global.
func (s *Sandbox) Set(name string, v any) { _ = s.VM.Set(name, v) }
