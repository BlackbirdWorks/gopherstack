package timestreaminfluxdb_test

import (
	"testing"

	sdk "github.com/aws/aws-sdk-go-v2/service/timestreaminfluxdb"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/timestreaminfluxdb"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	h := timestreaminfluxdb.NewHandler(timestreaminfluxdb.NewInMemoryBackend(testAccount, "us-east-1"))
	sdkcheck.CheckCompleteness(t, &sdk.Client{}, h.GetSupportedOperations(), []string{})
}
