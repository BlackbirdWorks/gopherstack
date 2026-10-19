package bedrockagentcorecontrol_test

import (
	"testing"

	sdk "github.com/aws/aws-sdk-go-v2/service/bedrockagentcorecontrol"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/bedrockagentcorecontrol"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	h := bedrockagentcorecontrol.NewHandler(bedrockagentcorecontrol.NewInMemoryBackend("123456789012", "us-east-1"))
	sdkcheck.CheckCompleteness(t, &sdk.Client{}, h.GetSupportedOperations(), []string{})
}
