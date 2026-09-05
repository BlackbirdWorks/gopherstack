package networkmanager_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	networkmanagersdk "github.com/aws/aws-sdk-go-v2/service/networkmanager"
	"github.com/aws/aws-sdk-go-v2/service/networkmanager/types"
	"github.com/stretchr/testify/require"
)

// Test_CreateCoreNetwork_UnknownGlobalNetworkIsValidation proves that
// CreateCoreNetwork's unknown-GlobalNetworkId path is wire-shape-wrong.
// CreateCoreNetwork's own deserializeOpError switch (deserializers.go,
// networkmanager@v1.44.4) recognizes AccessDeniedException,
// ConflictException, CoreNetworkPolicyException, InternalServerException,
// ServiceQuotaExceededException, ThrottlingException and
// ValidationException -- it has no ResourceNotFoundException case at all,
// unlike almost every sibling op that takes a GlobalNetworkId. gopherstack's
// backend (corenetworks.go CreateCoreNetwork) returns notFoundError for an
// unknown GlobalNetworkId, which handler.go's classifyError renders as
// ResourceNotFoundException -- a code this operation's real deserializer
// switch never matches, so it falls to the switch's default case and
// produces a *smithy.GenericAPIError instead of any typed exception. A real
// client's errors.As(&types.ValidationException{}) branch -- the correct
// typed exception this op actually declares for an invalid resource
// reference -- can never fire either way today.
func Test_CreateCoreNetwork_UnknownGlobalNetworkIsValidation(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)

	_, err := client.CreateCoreNetwork(t.Context(), &networkmanagersdk.CreateCoreNetworkInput{
		GlobalNetworkId: aws.String("no-such-global-network"),
	})
	require.Error(t, err)

	var ve *types.ValidationException
	require.ErrorAs(t, err, &ve,
		"expected a typed ValidationException, got: %v", err)
}

// Test_CreateConnection_UnknownDeviceIsValidation proves the identical bug
// for CreateConnection. PARITY.md and a code comment in globalnetworks.go
// (CreateConnection) already document that this op's real error set lacks
// ResourceNotFoundException and defend using notFoundError anyway as "the
// closest honest match available" -- but that reasoning only weighs message
// honesty, not wire-shape correctness: notFoundError's ResourceNotFoundException
// code is not in this op's declared set either, so a real client gets a
// generic error regardless. ValidationException (declared for this op, with
// reason FieldValidationFailed) is the only choice that actually decodes
// into a typed exception.
func Test_CreateConnection_UnknownDeviceIsValidation(t *testing.T) {
	t.Parallel()

	_, client := newTestHandlerAndClient(t)

	gn, err := client.CreateGlobalNetwork(t.Context(), &networkmanagersdk.CreateGlobalNetworkInput{})
	require.NoError(t, err)

	_, err = client.CreateConnection(t.Context(), &networkmanagersdk.CreateConnectionInput{
		GlobalNetworkId:   gn.GlobalNetwork.GlobalNetworkId,
		ConnectedDeviceId: aws.String("no-such-device-1"),
		DeviceId:          aws.String("no-such-device-2"),
	})
	require.Error(t, err)

	var ve *types.ValidationException
	require.ErrorAs(t, err, &ve,
		"expected a typed ValidationException, got: %v", err)
}
