package ociregistry_test

import (
	"testing"

	"go.uber.org/goleak"

	"github.com/blackbirdworks/gopherstack/pkgs/testleak"
)

// TestMain asserts the tests leave no goroutines running, apart from the registry library's event broadcaster.
func TestMain(m *testing.M) {
	testleak.VerifyTestMain(m,
		// distribution's handlers.App starts a go-events Broadcaster it never stops.
		goleak.IgnoreTopFunction("github.com/docker/go-events.(*Broadcaster).run"),
	)
}
