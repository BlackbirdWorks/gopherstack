package forecast_test

import (
	"testing"

	forecastsdk "github.com/aws/aws-sdk-go-v2/service/forecast"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/forecast"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	h := forecast.NewHandler(nil)
	sdkcheck.CheckCompleteness(t, &forecastsdk.Client{}, h.GetSupportedOperations(), []string{})
}
