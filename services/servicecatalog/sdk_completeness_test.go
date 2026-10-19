package servicecatalog_test

import (
	"testing"

	sdk "github.com/aws/aws-sdk-go-v2/service/servicecatalog"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/servicecatalog"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	h := servicecatalog.NewHandler(servicecatalog.NewInMemoryBackend("123456789012", "us-east-1"))
	sdkcheck.CheckCompleteness(
		t,
		&sdk.Client{},
		h.GetSupportedOperations(),
		[]string{"ListStackInstancesForProvisionedProduct"},
	)
}
