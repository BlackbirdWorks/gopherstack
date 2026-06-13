package bedrockagent_test

import (
	"testing"

	bedrockagentsdk "github.com/aws/aws-sdk-go-v2/service/bedrockagent"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/bedrockagent"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	b := bedrockagent.NewTestBackend("us-east-1", "123456789012")
	h := bedrockagent.NewTestHandler(b)

	sdkcheck.CheckCompleteness(t, &bedrockagentsdk.Client{}, h.GetSupportedOperations(), []string{})
}
