package pricing_test

import (
	"testing"

	psdk "github.com/aws/aws-sdk-go-v2/service/pricing"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/pricing"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	sdkcheck.CheckCompleteness(t, &psdk.Client{}, pricing.NewHandler().GetSupportedOperations(), []string{})
}
