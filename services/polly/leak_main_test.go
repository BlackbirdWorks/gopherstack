package polly_test

import (
	"testing"

	"go.uber.org/goleak"

	"github.com/blackbirdworks/gopherstack/pkgs/testleak"
)

func TestMain(m *testing.M) {
	testleak.VerifyTestMain(m,
		// SDK event-stream reader ranges a result channel it never closes.
		goleak.IgnoreAnyFunction("github.com/aws/aws-sdk-go-v2/service/polly.newAsyncEventStreamReader.func1"),
	)
}
