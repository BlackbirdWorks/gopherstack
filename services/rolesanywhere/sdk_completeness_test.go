package rolesanywhere_test

import (
	"testing"

	rasdk "github.com/aws/aws-sdk-go-v2/service/rolesanywhere"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/rolesanywhere"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// rolesanywhere client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := rolesanywhere.NewInMemoryBackend("000000000000", "us-east-1")
	h := rolesanywhere.NewHandler(backend)

	// Operations not yet implemented — CRL management, subject queries, attribute mappings.
	notImplemented := []string{
		"DeleteCrl",
		"DisableCrl",
		"EnableCrl",
		"GetCrl",
		"ImportCrl",
		"ListCrls",
		"UpdateCrl",
		"GetSubject",
		"ListSubjects",
		"PutAttributeMapping",
		"DeleteAttributeMapping",
		"PutNotificationSettings",
		"ResetNotificationSettings",
	}

	sdkcheck.CheckCompleteness(t, &rasdk.Client{}, h.GetSupportedOperations(), notImplemented)
}
