package textract_test

import (
	"testing"

	textractsdk "github.com/aws/aws-sdk-go-v2/service/textract"
	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/textract"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()
	h := textract.NewHandler(nil)
	sdkcheck.CheckCompleteness(t, &textractsdk.Client{}, h.GetSupportedOperations(), []string{})
}
