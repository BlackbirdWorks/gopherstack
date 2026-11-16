package bedrockagentcore_test

import (
	"testing"

	"go.uber.org/goleak"

	"github.com/blackbirdworks/gopherstack/pkgs/testleak"
)

func TestMain(m *testing.M) {
	// SDK event-stream reader ranges a result channel it never closes.
	const sdkReader = "github.com/aws/aws-sdk-go-v2/service/bedrockagentcore.newAsyncEventStreamReader.func1"

	testleak.VerifyTestMain(m, goleak.IgnoreAnyFunction(sdkReader))
}
