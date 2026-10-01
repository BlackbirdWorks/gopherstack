package dsql_test

import (
	"testing"

	dsqlsdk "github.com/aws/aws-sdk-go-v2/service/dsql"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/dsql"
)

func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	h := dsql.NewHandler(dsql.NewInMemoryBackend())
	sdkcheck.CheckCompleteness(t, &dsqlsdk.Client{}, h.GetSupportedOperations(), nil)
}
