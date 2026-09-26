package amplify_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/testleak"
)

// TestMain asserts amplify tests leave no goroutines running.
func TestMain(m *testing.M) {
	testleak.VerifyTestMain(m)
}
