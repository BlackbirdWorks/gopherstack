package budgets_test

import (
	"testing"

	bsdk "github.com/aws/aws-sdk-go-v2/service/budgets"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/budgets"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	h := budgets.NewHandler(budgets.NewInMemoryBackend("123456789012", "us-east-1"))
	sdkcheck.CheckCompleteness(t, &bsdk.Client{}, h.GetSupportedOperations(), []string{})
}
