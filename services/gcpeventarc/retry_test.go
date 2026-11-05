package gcpeventarc_test

import (
	"testing"
	"time"

	"cloud.google.com/go/eventarc/apiv1/eventarcpb"
	"cloud.google.com/go/pubsub/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

func TestRetryPolicyDelivery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		policy    *eventarcpb.Trigger_RetryPolicy
		name      string
		wantRetry bool
	}{
		{name: "no policy redelivers through Pub/Sub", wantRetry: true},
		{name: "max attempts 1 drops after the first failure", policy: &eventarcpb.Trigger_RetryPolicy{MaxAttempts: 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			trig := pubsubTrigger("", runDestination("receiver", "/r"))
			trig.RetryPolicy = tt.policy
			created := e.createTrigger(t, "retry-trigger", trig)

			e.dest.failNext(1)
			e.publish(t, created.GetTransport().GetPubsub().GetTopic(), &pubsub.Message{Data: []byte("m")})

			require.Eventually(t, func() bool { return e.dest.callCount() == 1 }, waitFor, tick)

			if tt.wantRetry {
				e.dest.next(t)
				assert.Equal(t, 2, e.dest.callCount())

				return
			}

			require.Never(t, func() bool { return e.dest.callCount() > 1 }, 3*time.Second, 100*time.Millisecond)
			e.dest.none(t)
		})
	}
}

func TestRetryPolicyValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		dest   func(e *env) *eventarcpb.Destination
		policy *eventarcpb.Trigger_RetryPolicy
		name   string
		want   codes.Code
	}{
		{
			name: "one attempt on cloud run", want: codes.OK,
			dest:   func(*env) *eventarcpb.Destination { return runDestination("receiver", "") },
			policy: &eventarcpb.Trigger_RetryPolicy{MaxAttempts: 1},
		},
		{
			name: "only 1 is valid", want: codes.InvalidArgument,
			dest:   func(*env) *eventarcpb.Destination { return runDestination("receiver", "") },
			policy: &eventarcpb.Trigger_RetryPolicy{MaxAttempts: 3},
		},
		{
			name: "cloud run only", want: codes.InvalidArgument,
			dest:   func(e *env) *eventarcpb.Destination { return httpDestination(e.dest.srv.URL) },
			policy: &eventarcpb.Trigger_RetryPolicy{MaxAttempts: 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			trig := pubsubTrigger("", tt.dest(e))
			trig.RetryPolicy = tt.policy

			_, err := e.client.CreateTrigger(t.Context(), &eventarcpb.CreateTriggerRequest{
				Parent: parent, TriggerId: "retry-validate", Trigger: trig,
			})
			assert.Equal(t, tt.want, code(err))
		})
	}
}
