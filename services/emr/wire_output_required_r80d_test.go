package emr_test

import (
	"testing"

	emrsdk "github.com/aws/aws-sdk-go-v2/service/emr"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/emr"
)

// Test_SDKRoundTrip_GetBlockPublicAccessConfiguration_CreatedByArn proves
// gopherstack-r80d batch 26's finding: GetBlockPublicAccessConfigurationOutput's
// BlockPublicAccessConfigurationMetadata (aws-sdk-go-v2/service/emr@v1.64.4/
// types/types.go:167-180) marks both CreatedByArn and CreationDateTime as
// "This member is required." and declares CreatedByArn as *string, so an
// omitted key and a present-empty one are distinguishable to a real client.
// Before the fix, gopherstack's wire struct tagged CreatedByArn
// `json:"CreatedByArn,omitempty"`; for a freshly created backend that has
// never called PutBlockPublicAccessConfiguration for a region -- the default
// state, matching real EMR's own account-level default -- the metadata
// carried an empty CreatedByArn, so the tag silently dropped the key
// (deserializers.go:7538-7545's awsAwsjson11_deserializeDocumentBlockPublic
// AccessConfigurationMetadata only sets CreatedByArn when the "CreatedByArn"
// key is present at all), leaving CreatedByArn nil on the decoded response
// instead of a non-nil pointer to "".
func Test_SDKRoundTrip_GetBlockPublicAccessConfiguration_CreatedByArn(t *testing.T) {
	t.Parallel()

	backend := emr.NewInMemoryBackend(testAccountID, testRegion)
	h := emr.NewHandler(backend)
	client := newTestEMRClient(t, h)
	ctx := t.Context()

	out, err := client.GetBlockPublicAccessConfiguration(ctx, &emrsdk.GetBlockPublicAccessConfigurationInput{})
	require.NoError(t, err)
	require.NotNil(t, out.BlockPublicAccessConfigurationMetadata)
	require.NotNil(
		t, out.BlockPublicAccessConfigurationMetadata.CreatedByArn,
		"CreatedByArn is required on BlockPublicAccessConfigurationMetadata and must be "+
			"present (even as an empty string) rather than omitted, even before any "+
			"PutBlockPublicAccessConfiguration call for this region",
	)
	require.NotNil(t, out.BlockPublicAccessConfigurationMetadata.CreationDateTime)
}
