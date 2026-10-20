package gcpfunctions_test

import (
	"net/http"
	"strings"
	"testing"

	"cloud.google.com/go/functions/apiv2/functionspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/blackbirdworks/gopherstack/services/gcpfunctions"
)

const (
	topic    = "projects/test-project/topics/events"
	wantSub  = "projects/test-project/subscriptions/gcf-ev-us-central1-events"
	envelope = `{"subscription":"s","message":{"data":"aGVsbG8=","messageId":"42",` +
		`"publishTime":"2026-01-02T03:04:05Z","attributes":{"k":"v"}}}`
)

func eventSpec(src *functionspb.StorageSource, retry functionspb.EventTrigger_RetryPolicy) *functionspb.Function {
	fn := fnSpec(src)
	fn.EventTrigger = &functionspb.EventTrigger{
		EventType: gcpfunctions.EventTypePubSub, PubsubTopic: topic, RetryPolicy: retry,
	}

	return fn
}

func TestEvent_TriggerSubscriptionLifecycle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	fn := e.createFunction(t, "ev", eventSpec(e.upload(t, zipOf(t, nodeFilesFn())), 0))

	assert.Equal(t, functionspb.EventTrigger_RETRY_POLICY_DO_NOT_RETRY, fn.GetEventTrigger().GetRetryPolicy())
	assert.Equal(t, "us-central1", fn.GetEventTrigger().GetTriggerRegion())
	assert.Contains(t, fn.GetEventTrigger().GetTrigger(), "/triggers/ev-")

	sub := e.bus.sub(wantSub)
	require.NotNil(t, sub)
	assert.Equal(t, topic, sub.GetTopic())
	assert.True(t, strings.HasSuffix(sub.GetPushConfig().GetPushEndpoint(), "/_gcf/events/"+fn.GetName()))

	del, err := e.client.DeleteFunction(t.Context(), &functionspb.DeleteFunctionRequest{Name: fn.GetName()})
	require.NoError(t, err)
	require.NoError(t, del.Wait(t.Context()))
	assert.Nil(t, e.bus.sub(wantSub))
}

func TestEvent_Delivery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		retry     functionspb.EventTrigger_RetryPolicy
		reply     int32
		wantCode  int
		wantEvent bool
	}{
		{"success", functionspb.EventTrigger_RETRY_POLICY_DO_NOT_RETRY, http.StatusOK, http.StatusOK, true},
		{
			"failure dropped without retry",
			functionspb.EventTrigger_RETRY_POLICY_DO_NOT_RETRY,
			http.StatusInternalServerError,
			http.StatusNoContent,
			true,
		},
		{
			"failure retried",
			functionspb.EventTrigger_RETRY_POLICY_RETRY,
			http.StatusInternalServerError,
			http.StatusInternalServerError,
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			e.rt.reply.Store(tt.reply)

			fn := e.createFunction(t, "ev", eventSpec(e.upload(t, zipOf(t, nodeFilesFn())), tt.retry))

			req, err := http.NewRequestWithContext(t.Context(), http.MethodPost,
				"http://"+e.handler.IngressAddr()+"/_gcf/events/"+fn.GetName(), strings.NewReader(envelope))
			require.NoError(t, err)

			resp, err := e.http.Do(req)
			require.NoError(t, err)
			_ = resp.Body.Close()
			assert.Equal(t, tt.wantCode, resp.StatusCode)

			specs := e.rt.specs()
			require.Len(t, specs, 1)
			assert.Contains(t, specs[0].Env, "FUNCTION_SIGNATURE_TYPE=cloudevent")
		})
	}
}

func TestEvent_CloudEventShape(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	fn := e.createFunction(t, "ev", eventSpec(e.upload(t, zipOf(t, nodeFilesFn())), 0))

	code, _ := e.call(t, http.MethodPost, "localhost", "/_gcf/events/"+fn.GetName(), envelope)
	require.Equal(t, http.StatusOK, code)

	reqs := e.rt.requests()
	require.Len(t, reqs, 1)
	assert.Equal(t, "1.0", reqs[0].Headers["ce-specversion"])
	assert.Equal(t, "42", reqs[0].Headers["ce-id"])
	assert.Equal(t, gcpfunctions.EventTypePubSub, reqs[0].Headers["ce-type"])
	assert.Equal(t, "//pubsub.googleapis.com/"+topic, reqs[0].Headers["ce-source"])
	assert.Equal(t, "2026-01-02T03:04:05Z", reqs[0].Headers["ce-time"])
	assert.Equal(t, "application/json", reqs[0].Headers["content-type"])
	assert.JSONEq(t, envelope, reqs[0].Body)
}

func TestEvent_Errors(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	fn := e.createFunction(t, "plain", fnSpec(e.upload(t, zipOf(t, nodeFilesFn()))))

	tests := []struct {
		name string
		path string
		body string
		want int
	}{
		{"unknown function", "/_gcf/events/projects/p/locations/l/functions/none", envelope, http.StatusNotFound},
		{"http function", "/_gcf/events/" + fn.GetName(), envelope, http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			code, _ := e.call(t, http.MethodPost, "localhost", tt.path, tt.body)
			assert.Equal(t, tt.want, code)
		})
	}

	ev := e.createFunction(t, "ev2", eventSpec(e.upload(t, zipOf(t, nodeFilesFn())), 0))
	code, _ := e.call(t, http.MethodPost, "localhost", "/_gcf/events/"+ev.GetName(), "not json")
	assert.Equal(t, http.StatusBadRequest, code)
}

func TestEvent_UpdateCannotChangeTriggerKind(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	fn := e.createFunction(t, "kind", fnSpec(e.upload(t, zipOf(t, nodeFilesFn()))))

	next := eventSpec(nil, 0)
	next.Name = fn.GetName()

	_, err := e.client.UpdateFunction(t.Context(), &functionspb.UpdateFunctionRequest{
		Function: next, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"event_trigger"}},
	})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}
