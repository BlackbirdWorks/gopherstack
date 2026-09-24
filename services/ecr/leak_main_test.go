package ecr_test

import (
	"testing"

	"go.uber.org/goleak"

	"github.com/blackbirdworks/gopherstack/pkgs/testleak"
)

// TestMain asserts ecr tests leave no goroutines running, guarding the
// embedded Docker registry (docker_registry.go) against leaks.
func TestMain(m *testing.M) {
	testleak.VerifyTestMain(m,
		// distribution v3.1.1's handlers.App.configureEvents unconditionally
		// starts a github.com/docker/go-events Broadcaster (app.events.sink,
		// an unexported field) with go b.run(); App exposes no way to reach
		// or close it, and App.Shutdown() only closes a proxy registry's
		// remote connection, not this. startUploadPurger's goroutine is
		// disabled via config instead (see docker_registry.go) since it can
		// be turned off cleanly.
		goleak.IgnoreTopFunction("github.com/docker/go-events.(*Broadcaster).run"),
	)
}
