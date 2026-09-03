package eventbridge_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	eventbridgesdk "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	eventbridgetypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

// TestCancelReplay_TerminalState_RealClient drives CancelReplay against an
// already-COMPLETED replay through the real aws-sdk-go-v2 eventbridge client
// and asserts errors.As unwraps to *types.IllegalStatusException, not
// *types.InvalidStateException. CancelReplay's own deserializeOpError switch
// (eventbridge@v1.48.4 deserializers.go) declares
// [ConcurrentModificationException, IllegalStatusException,
// InternalException, ResourceNotFoundException] -- no InvalidStateException
// at all. InvalidStateException is real and correctly spelled (it IS
// declared by ActivateEventSource/CreateEventBus/DeactivateEventSource, the
// only other ops in this package that emit the shared ErrInvalidState
// sentinel), but CancelReplay uses a differently-named sibling code for the
// same "wrong state" condition (gopherstack-uox6 sweep).
func TestCancelReplay_TerminalState_RealClient(t *testing.T) {
	t.Parallel()

	backend := eventbridge.NewInMemoryBackend()
	backend.AddReplayInternal(&eventbridge.Replay{ReplayName: "r1", State: "COMPLETED"})

	client := newTestEventBridgeClient(t, eventbridge.NewHandler(backend))

	_, err := client.CancelReplay(t.Context(), &eventbridgesdk.CancelReplayInput{
		ReplayName: aws.String("r1"),
	}, func(o *eventbridgesdk.Options) { o.RetryMaxAttempts = 1 })
	require.Error(t, err)

	var invalidState *eventbridgetypes.InvalidStateException
	require.NotErrorAs(t, err, &invalidState,
		"CancelReplay never declares InvalidStateException; got: %v", err)

	var illegalStatus *eventbridgetypes.IllegalStatusException
	require.ErrorAs(t, err, &illegalStatus,
		"expected a real IllegalStatusException from the SDK deserializer, got: %v", err)
}
