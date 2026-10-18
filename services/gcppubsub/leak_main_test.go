package gcppubsub_test

import (
	"testing"

	"go.uber.org/goleak"

	"github.com/blackbirdworks/gopherstack/pkgs/testleak"
)

func TestMain(m *testing.M) {
	// The pubsub client parks this goroutine on a ticker until the next tick after Receive returns.
	testleak.VerifyTestMain(m,
		goleak.IgnoreAnyFunction("cloud.google.com/go/pubsub/v2.(*messageIterator).streamKeepAliveHandler"))
}
