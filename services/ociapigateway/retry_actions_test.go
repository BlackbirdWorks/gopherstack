package ociapigateway_test

import (
	"testing"

	"github.com/oracle/oci-go-sdk/v65/apigateway"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRetryTokenOnActions(t *testing.T) {
	t.Parallel()

	t.Run("create subscriber", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t, envOpts{})
		plan := e.mkPlan(t, "p", apigateway.Entitlement{Name: new("e")})
		create := func() string {
			r, err := e.sub.CreateSubscriber(t.Context(), apigateway.CreateSubscriberRequest{
				OpcRetryToken: new("sub-tok"),
				CompartmentId: new(testCompartment), DisplayName: new("s"), UsagePlans: []string{plan},
				Clients: []apigateway.Client{{Name: new("c"), Token: new("tok-1")}},
			})
			require.NoError(t, err)

			return *r.Id
		}

		first := create()
		assert.Equal(t, first, create())
	})

	t.Run("change compartment", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t, envOpts{})
		gw := e.mkGateway(t, "mv-retry")
		move := func() string {
			r, err := e.gw.ChangeGatewayCompartment(t.Context(), apigateway.ChangeGatewayCompartmentRequest{
				GatewayId: gw.Id, CompartmentId: new(otherCompart), OpcRetryToken: new("mv-tok"),
			})
			require.NoError(t, err)

			return *r.OpcWorkRequestId
		}

		first := move()
		assert.Equal(t, first, move())
	})

	t.Run("cancel work request", func(t *testing.T) {
		t.Parallel()

		e := newEnv(t, envOpts{})
		created := e.createGateway(t, "cancel-retry")

		for range 2 {
			_, err := e.wr.CancelWorkRequest(t.Context(), apigateway.CancelWorkRequestRequest{
				WorkRequestId: created.OpcWorkRequestId, OpcRetryToken: new("cancel-tok"),
			})
			require.NoError(t, err)
		}
	})
}
