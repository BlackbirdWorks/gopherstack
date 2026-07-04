// Package testleak centralises goroutine-leak detection for tests. It wraps
// [goleak.VerifyTestMain] with a shared ignore list for the long-lived
// goroutines gopherstack legitimately runs (metrics collectors, persistence
// timers, runtime/test-framework internals) so individual packages can opt in
// with a one-line TestMain:
//
//	func TestMain(m *testing.M) { testleak.VerifyTestMain(m) }
//
// Packages that spawn their own known-persistent goroutines can pass extra
// [goleak.Option] values (e.g. goleak.IgnoreTopFunction("...")).
package testleak

import (
	"testing"

	"go.uber.org/goleak"
)

// defaultIgnores lists goroutine top-functions that are expected to outlive
// individual tests across the codebase. These are framework/runtime helpers and
// shared infrastructure rather than service leaks.
func defaultIgnores() []goleak.Option {
	return []goleak.Option{
		// Go test framework / runtime background goroutines.
		goleak.IgnoreTopFunction("testing.(*T).Parallel"),
		goleak.IgnoreTopFunction("runtime.goexit"),
		// OpenCensus/Prometheus and similar metric exporters occasionally keep
		// a worker parked between scrapes.
		goleak.IgnoreAnyFunction("go.opencensus.io/stats/view.(*worker).start"),
	}
}

// VerifyTestMain runs the package's tests via m and then asserts that no
// unexpected goroutines were leaked, exiting non-zero if any are found. Pass
// extra options to ignore package-specific long-lived goroutines.
func VerifyTestMain(m *testing.M, extra ...goleak.Option) {
	opts := append(defaultIgnores(), extra...)
	goleak.VerifyTestMain(m, opts...)
}
