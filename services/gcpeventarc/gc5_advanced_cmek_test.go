package gcpeventarc_test

import (
	"testing"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"cloud.google.com/go/kms/apiv1/kmspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func disableKeyVersion(t *testing.T, kb interface {
	UpdateCryptoKeyVersion(*kmspb.UpdateCryptoKeyVersionRequest) (*kmspb.CryptoKeyVersion, error)
}) {
	t.Helper()

	_, err := kb.UpdateCryptoKeyVersion(&kmspb.UpdateCryptoKeyVersionRequest{
		CryptoKeyVersion: &kmspb.CryptoKeyVersion{
			Name: symKey + "/cryptoKeyVersions/1", State: kmspb.CryptoKeyVersion_DISABLED,
		},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"state"}},
	})
	require.NoError(t, err)
}

func TestBusPublishStopsWhenKeyDisabled(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	kb := newKMS(t)
	e.handler.Backend.SetKeyChecker(kb)

	op, err := e.client.CreateMessageBus(t.Context(), &eventarcpb.CreateMessageBusRequest{
		Parent: parent, MessageBusId: "keyed-bus", MessageBus: &eventarcpb.MessageBus{CryptoKeyName: symKey},
	})
	require.NoError(t, err)

	bus, err := op.Wait(t.Context())
	require.NoError(t, err)

	p := e.createPipeline(t, "to-http", httpPipeline(e.dest.srv.URL+"/api", "", ""))
	e.createEnrollment(t, "all-events", bus.GetName(), `message.id != ""`, p)

	require.NoError(t, e.publishJSON(t, bus.GetName(), orderEvent("evt-0", "order.created")))
	e.dest.next(t)

	disableKeyVersion(t, kb)

	err = e.publishJSON(t, bus.GetName(), orderEvent("evt-1", "order.created"))
	assert.Equal(t, codes.FailedPrecondition, code(err))
	e.dest.none(t)
}

func TestPipelineDeliveryStopsWhenKeyDisabled(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	kb := newKMS(t)
	e.handler.Backend.SetKeyChecker(kb)

	bus := e.createBus(t, "plain-bus")
	pl := httpPipeline(e.dest.srv.URL+"/api", "", "")
	pl.CryptoKeyName = symKey

	p := e.createPipeline(t, "keyed-pipeline", pl)
	e.createEnrollment(t, "all-events", bus, `message.id != ""`, p)

	require.NoError(t, e.publishJSON(t, bus, orderEvent("evt-0", "order.created")))
	e.dest.next(t)

	disableKeyVersion(t, kb)

	require.NoError(t, e.publishJSON(t, bus, orderEvent("evt-1", "order.created")))
	e.dest.none(t)
}
