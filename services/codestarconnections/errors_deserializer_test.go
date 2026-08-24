package codestarconnections_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codestarconnectionssdk "github.com/aws/aws-sdk-go-v2/service/codestarconnections"
	codestarconnectionstypes "github.com/aws/aws-sdk-go-v2/service/codestarconnections/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codestarconnections"
)

// These tests prove that CreateConnection and CreateHost no longer reject a
// duplicate name through the real SDK client (gopherstack-wlo1). Before the
// fix, both used ErrAlreadyExists mapped to InvalidInputException -- a code
// absent from EITHER op's own deserializeOpError<Op> switch
// (codestarconnections@v1.38.4 deserializers.go: CreateConnection types only
// LimitExceededException/ResourceNotFoundException/
// ResourceUnavailableException; CreateHost types only
// LimitExceededException) -- so a real client's second create for the same
// name got a smithy.GenericAPIError instead of a distinct resource, even
// though sibling service codeconnections@v1.13.4's byte-identical switches
// already document that a name collision is not an error at all.

func TestCreateConnection_DuplicateNameSucceedsSDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := codestarconnections.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestCodeStarConnectionsClient(t, codestarconnections.NewHandler(backend))

	out1, err := client.CreateConnection(t.Context(), &codestarconnectionssdk.CreateConnectionInput{
		ConnectionName: aws.String("dup"),
		ProviderType:   "GitHub",
	})
	require.NoError(t, err)

	out2, err := client.CreateConnection(t.Context(), &codestarconnectionssdk.CreateConnectionInput{
		ConnectionName: aws.String("dup"),
		ProviderType:   "GitHub",
	})
	require.NoError(
		t,
		err,
		"CreateConnection on a duplicate name must succeed (its own switch has no code for a name collision)",
	)
	require.NotEqual(t, aws.ToString(out1.ConnectionArn), aws.ToString(out2.ConnectionArn))
}

func TestCreateHost_DuplicateNameSucceedsSDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := codestarconnections.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestCodeStarConnectionsClient(t, codestarconnections.NewHandler(backend))

	out1, err := client.CreateHost(t.Context(), &codestarconnectionssdk.CreateHostInput{
		Name:             aws.String("dup-host"),
		ProviderType:     "GitHub",
		ProviderEndpoint: aws.String("https://example.com"),
	})
	require.NoError(t, err)

	out2, err := client.CreateHost(t.Context(), &codestarconnectionssdk.CreateHostInput{
		Name:             aws.String("dup-host"),
		ProviderType:     "GitHub",
		ProviderEndpoint: aws.String("https://example.com"),
	})
	require.NoError(
		t,
		err,
		"CreateHost on a duplicate name must succeed (its own switch has no code for a name collision)",
	)
	require.NotEqual(t, aws.ToString(out1.HostArn), aws.ToString(out2.HostArn))
}

// TestDeleteSyncConfiguration_UnknownResourceIsIdempotentSDKRoundTrip proves
// that DeleteSyncConfiguration no longer surfaces a not-found error through
// the real SDK client. Before the fix, a missing ResourceName+SyncType
// returned ErrNotFound mapped to ResourceNotFoundException, a code absent
// from this op's own deserializeOpErrorDeleteSyncConfiguration switch
// (codestarconnections@v1.38.4 deserializers.go) -- same bug independently
// confirmed in sibling codeconnections@v1.13.4's identical switch.
func TestDeleteSyncConfiguration_UnknownResourceIsIdempotentSDKRoundTrip(t *testing.T) {
	t.Parallel()

	backend := codestarconnections.NewInMemoryBackend("000000000000", "us-east-1")
	client := newTestCodeStarConnectionsClient(t, codestarconnections.NewHandler(backend))

	_, err := client.DeleteSyncConfiguration(
		t.Context(),
		&codestarconnectionssdk.DeleteSyncConfigurationInput{
			ResourceName: aws.String("no-such-resource"),
			SyncType:     codestarconnectionstypes.SyncConfigurationTypeCfnStackSync,
		},
	)
	require.NoError(
		t,
		err,
		"DeleteSyncConfiguration on an unknown resource must succeed (its own switch has no ResourceNotFoundException case)",
	)
}
