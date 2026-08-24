package codeconnections_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codeconnectionssdk "github.com/aws/aws-sdk-go-v2/service/codeconnections"
	codeconnectionstypes "github.com/aws/aws-sdk-go-v2/service/codeconnections/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codeconnections"
)

// TestDeleteSyncConfiguration_UnknownResourceIsIdempotentSDKRoundTrip proves
// that DeleteSyncConfiguration no longer surfaces a not-found error through
// the real SDK client (gopherstack-wlo1). Before the fix, a missing
// ResourceName+SyncType returned ErrNotFound mapped to
// ResourceNotFoundException, a code absent from this op's own
// deserializeOpErrorDeleteSyncConfiguration switch (codeconnections@v1.13.4
// deserializers.go) -- DeleteSyncConfigurationOutput also carries no fields
// at all, consistent with an idempotent delete.
func TestDeleteSyncConfiguration_UnknownResourceIsIdempotentSDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := codeconnections.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestCodeConnectionsClient(t, codeconnections.NewHandler(backend))

	_, err := client.DeleteSyncConfiguration(
		t.Context(),
		&codeconnectionssdk.DeleteSyncConfigurationInput{
			ResourceName: aws.String("no-such-resource"),
			SyncType:     codeconnectionstypes.SyncConfigurationTypeCfnStackSync,
		},
	)
	require.NoError(
		t,
		err,
		"DeleteSyncConfiguration on an unknown resource must succeed (its own switch has no ResourceNotFoundException case)",
	)
}
