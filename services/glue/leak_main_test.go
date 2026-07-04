package glue_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/testleak"
)

// TestMain asserts the glue test suite leaves no goroutines running. This guards the
// managed lifecycle reconciler: NewInMemoryBackend must not spawn a background
// goroutine, and any test that starts the reconciler must stop it. It is the
// package-wide backstop for the reconciler-leak fix (previously the constructor
// started `go b.runReconciler()` that nothing ever stopped).
func TestMain(m *testing.M) {
	testleak.VerifyTestMain(m)
}
