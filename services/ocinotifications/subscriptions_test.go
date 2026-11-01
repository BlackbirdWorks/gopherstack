package ocinotifications_test

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/ons"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateSubscriptionValidation(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	topic := e.mkTopic(t, "val")

	tests := []struct {
		name     string
		protocol string
		endpoint string
		code     int
	}{
		{"email ok", "EMAIL", "a@example.com", 0},
		{"email bad", "EMAIL", "not-an-email", http.StatusBadRequest},
		{"https ok", "HTTPS", "https://example.com/hook", 0},
		{"https bad scheme", "HTTPS", "ftp://example.com", http.StatusBadRequest},
		{"custom https ok", "CUSTOM_HTTPS", "http://example.com/hook", 0},
		{"sms ok", "SMS", "+14155550100", 0},
		{"sms bad", "SMS", "4155550100", http.StatusBadRequest},
		{"function ok", "ORACLE_FUNCTIONS", "ocid1.fnfunc.oc1..abc", 0},
		{"function bad", "ORACLE_FUNCTIONS", "fn", http.StatusBadRequest},
		{"slack bad", "SLACK", "hook", http.StatusBadRequest},
		{"pagerduty ok", "PAGERDUTY", "key", 0},
		{"unknown protocol", "CARRIER_PIGEON", "x", http.StatusBadRequest},
		{"long endpoint", "HTTPS", "https://example.com/" + strings.Repeat("a", 512), http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := e.dp.CreateSubscription(t.Context(), ons.CreateSubscriptionRequest{
				TopicId: topic.TopicId, CompartmentId: new(testCompartment),
				Protocol: new(tt.protocol), Endpoint: new(tt.endpoint),
			})
			if tt.code == 0 {
				require.NoError(t, err)

				return
			}

			assert.Equal(t, tt.code, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}

func TestSubscriptionInitialState(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	topic := e.mkTopic(t, "states")

	tests := []struct {
		protocol string
		endpoint string
		want     ons.SubscriptionLifecycleStateEnum
	}{
		{"EMAIL", "a@example.com", ons.SubscriptionLifecycleStatePending},
		{"HTTPS", "https://example.com/a", ons.SubscriptionLifecycleStatePending},
		{"CUSTOM_HTTPS", "https://example.com/b", ons.SubscriptionLifecycleStatePending},
		{"SMS", "+14155550100", ons.SubscriptionLifecycleStatePending},
		{"SLACK", "https://hooks.example.com/x", ons.SubscriptionLifecycleStatePending},
		{"ORACLE_FUNCTIONS", "ocid1.fnfunc.oc1..abc", ons.SubscriptionLifecycleStateActive},
		{"PAGERDUTY", "key", ons.SubscriptionLifecycleStateActive},
	}

	for _, tt := range tests {
		t.Run(tt.protocol, func(t *testing.T) {
			t.Parallel()

			sub := e.subscribe(t, topic, tt.protocol, tt.endpoint)
			assert.Equal(t, tt.want, sub.LifecycleState)
			assert.Contains(t, *sub.DeliverPolicy, "7200000")
			assert.NotNil(t, sub.CreatedTime)
			require.NotNil(t, sub.Etag)
		})
	}
}

func TestDuplicateSubscription(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	topic := e.mkTopic(t, "dup")
	e.subscribe(t, topic, "EMAIL", "a@example.com")

	_, err := e.dp.CreateSubscription(t.Context(), ons.CreateSubscriptionRequest{
		TopicId: topic.TopicId, CompartmentId: new(testCompartment),
		Protocol: new("EMAIL"), Endpoint: new("a@example.com"),
	})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())
}

func TestHTTPSConfirmationAndDelivery(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{enforce: true, register: true})
	topic := e.mkTopic(t, "https")
	sink := newSink(t)
	sub := e.subscribe(t, topic, "HTTPS", sink.srv.URL+"/hook")

	conf := sink.next(t)
	assert.Equal(t, *sub.Id, conf.header.Get("X-Oci-Ns-Subscriptionid"))
	assert.Equal(t, "SubscriptionConfirmation", conf.header.Get("X-Oci-Ns-Messagetype"))
	assert.Contains(t, conf.body, "ConfirmationURL")

	skipped := e.publish(t, topic, "before confirmation")
	assert.NotEmpty(t, *skipped.MessageId)
	assert.Empty(t, sink.requests, "a PENDING subscription receives nothing")

	// The confirmation link is unsigned: it must work against an enforcing listener.
	u, err := url.Parse(conf.header.Get("X-Oci-Ns-Confirmationurl"))
	require.NoError(t, err)

	status, body := rawDo(t, http.MethodGet, conf.header.Get("X-Oci-Ns-Confirmationurl"), "")
	require.Equal(t, http.StatusOK, status, body)
	assert.Contains(t, body, *sub.Id)
	assert.NotEmpty(t, u.Query().Get("token"))

	got, err := e.dp.GetSubscription(t.Context(), ons.GetSubscriptionRequest{SubscriptionId: sub.Id})
	require.NoError(t, err)
	assert.Equal(t, ons.SubscriptionLifecycleStateActive, got.LifecycleState)

	res := e.publish(t, topic, `{"hello":"world"}`)

	msg := sink.next(t)
	assert.JSONEq(t, `{"hello":"world"}`, msg.body)
	assert.Equal(t, *res.MessageId, msg.header.Get("X-Oci-Ns-Messageid"))
	assert.Equal(t, *topic.TopicId, msg.header.Get("X-Oci-Ns-Topicocid"))
	assert.Equal(t, "https", msg.header.Get("X-Oci-Ns-Topicname"))
	assert.Equal(t, "Notification", msg.header.Get("X-Oci-Ns-Messagetype"))
	assert.Contains(t, msg.header.Get("X-Oci-Ns-Unsubscribeurl"), "/unsubscription?token=")
	assert.Contains(t, msg.header.Get("Content-Type"), "text/plain")

	require.Eventually(t, func() bool {
		for _, d := range e.deliveries(t, *sub.Id) {
			if d.Kind == "MESSAGE" && d.MessageID == *res.MessageId {
				return d.Status == "DELIVERED" && d.Attempts == 1
			}
		}

		return false
	}, 5*time.Second, 5*time.Millisecond)
}

func TestJSONMessageTypeAndTitle(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	topic := e.mkTopic(t, "json")
	sink := newSink(t)
	sub := e.subscribe(t, topic, "CUSTOM_HTTPS", sink.srv.URL)
	e.confirm(t, sub, sink.next(t).header.Get("X-Oci-Ns-Confirmationurl"))

	_, err := e.dp.PublishMessage(t.Context(), ons.PublishMessageRequest{
		TopicId: topic.TopicId, MessageType: ons.PublishMessageMessageTypeJson,
		Body: new(`{"a":1}`), Title: new("t"),
	})
	require.NoError(t, err)
	assert.Equal(t, "application/json", sink.next(t).header.Get("Content-Type"))

	_, err = e.dp.PublishMessage(t.Context(), ons.PublishMessageRequest{
		TopicId: topic.TopicId, MessageType: ons.PublishMessageMessageTypeJson,
		Body: new("not json"),
	})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())
}

func TestPublishValidation(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	topic := e.mkTopic(t, "pub")

	tests := []struct {
		name string
		body string
		ok   bool
	}{
		{"limit", strings.Repeat("x", 64*1024), true},
		{"over limit", strings.Repeat("x", 64*1024+1), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := e.dp.PublishMessage(t.Context(), ons.PublishMessageRequest{
				TopicId: topic.TopicId, Body: new(tt.body),
			})
			if tt.ok {
				require.NoError(t, err)

				return
			}

			assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())
		})
	}

	_, err := e.dp.PublishMessage(t.Context(), ons.PublishMessageRequest{
		TopicId: new("ocid1.onstopic.oc1..nope"), Body: new("x"),
	})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestDeliveryRetriesWithBackoff(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	topic := e.mkTopic(t, "retry")
	sink := newSink(t, 200, 500, 503)
	sub := e.subscribe(t, topic, "HTTPS", sink.srv.URL)
	e.confirm(t, sub, sink.next(t).header.Get("X-Oci-Ns-Confirmationurl"))

	res := e.publish(t, topic, "retry me")

	attempts := func() (int, string) {
		for _, d := range e.deliveries(t, *sub.Id) {
			if d.MessageID == *res.MessageId {
				return d.Attempts, d.Status
			}
		}

		return 0, ""
	}

	sink.next(t) // 500
	sink.next(t) // 503: the immediate retry

	require.Eventually(t, func() bool {
		n, _ := attempts()

		return n == 2
	}, 5*time.Second, 5*time.Millisecond)
	assert.Empty(t, sink.requests, "no further attempt before the first backoff elapses")

	e.clock.Advance(30 * time.Second)
	e.backend.Kick()
	e.clock.Advance(31 * time.Second)
	e.backend.Kick()

	assert.Equal(t, "retry me", sink.next(t).body)

	require.Eventually(t, func() bool {
		n, st := attempts()

		return n == 3 && st == "DELIVERED"
	}, 5*time.Second, 5*time.Millisecond)
}

func TestDeliveryDiscardedAfterRetryDuration(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	topic := e.mkTopic(t, "discard")
	sink := newSink(t, 200, 500, 500, 500)
	sub := e.subscribe(t, topic, "HTTPS", sink.srv.URL)
	e.confirm(t, sub, sink.next(t).header.Get("X-Oci-Ns-Confirmationurl"))

	_, err := e.dp.UpdateSubscription(t.Context(), ons.UpdateSubscriptionRequest{
		SubscriptionId: sub.Id,
		DeliveryPolicy: &ons.DeliveryPolicy{
			BackoffRetryPolicy: &ons.BackoffRetryPolicy{
				MaxRetryDuration: new(10000), PolicyType: ons.BackoffRetryPolicyPolicyTypeExponential,
			},
		},
	})
	require.NoError(t, err)

	res := e.publish(t, topic, "doomed")
	sink.next(t)
	sink.next(t)

	status := func() (int, string) {
		for _, d := range e.deliveries(t, *sub.Id) {
			if d.MessageID == *res.MessageId {
				return d.Attempts, d.Status
			}
		}

		return 0, ""
	}

	require.Eventually(t, func() bool {
		n, _ := status()

		return n == 2
	}, 5*time.Second, 5*time.Millisecond)

	e.clock.Advance(61 * time.Second)
	e.backend.Kick()
	sink.next(t)

	require.Eventually(t, func() bool {
		_, st := status()

		return st == "DISCARDED"
	}, 5*time.Second, 5*time.Millisecond)
}

func TestUpdateSubscriptionPolicyValidation(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	topic := e.mkTopic(t, "policy")
	sub := e.subscribe(t, topic, "PAGERDUTY", "key")

	tests := []struct {
		name string
		ms   int
		ok   bool
	}{
		{"too short", 9999, false},
		{"too long", 86400001, false},
		{"valid", 60000, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp, err := e.dp.UpdateSubscription(t.Context(), ons.UpdateSubscriptionRequest{
				SubscriptionId: sub.Id,
				FreeformTags:   map[string]string{"a": "b"},
				DeliveryPolicy: &ons.DeliveryPolicy{BackoffRetryPolicy: &ons.BackoffRetryPolicy{
					MaxRetryDuration: new(tt.ms), PolicyType: ons.BackoffRetryPolicyPolicyTypeExponential,
				}},
			})
			if !tt.ok {
				assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.ms, *resp.DeliveryPolicy.BackoffRetryPolicy.MaxRetryDuration)
			require.NotNil(t, resp.Etag)

			list, lerr := e.dp.ListSubscriptions(t.Context(), ons.ListSubscriptionsRequest{
				CompartmentId: new(testCompartment), TopicId: topic.TopicId,
			})
			require.NoError(t, lerr)
			require.Len(t, list.Items, 1)
			assert.Equal(t, tt.ms, *list.Items[0].DeliveryPolicy.BackoffRetryPolicy.MaxRetryDuration)
		})
	}
}

func TestRecordedProtocols(t *testing.T) {
	t.Parallel()

	tests := []struct {
		protocol string
		endpoint string
	}{
		{"EMAIL", "ops@example.com"},
		{"SMS", "+14155550100"},
		{"SLACK", "https://hooks.example.com/services/x"},
	}

	for _, tt := range tests {
		t.Run(tt.protocol, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			topic := e.mkTopic(t, "rec")
			sub := e.subscribe(t, topic, tt.protocol, tt.endpoint)

			ds := e.deliveries(t, *sub.Id)
			require.Len(t, ds, 1)
			assert.Equal(t, "CONFIRMATION", ds[0].Kind)
			assert.Equal(t, "RECORDED", ds[0].Status)
			assert.Equal(t, tt.endpoint, ds[0].Endpoint)

			e.publish(t, topic, "before")
			assert.Len(t, e.deliveries(t, *sub.Id), 1, "PENDING subscriptions receive nothing")

			res := e.confirm(t, sub, ds[0].ConfirmationURL)
			assert.Equal(t, tt.endpoint, *res.Endpoint)
			assert.Equal(t, *topic.TopicId, *res.TopicId)
			assert.Equal(t, "rec", *res.TopicName)

			published := e.publish(t, topic, "after")

			ds = e.deliveries(t, *sub.Id)
			require.Len(t, ds, 2)
			assert.Equal(t, "MESSAGE", ds[1].Kind)
			assert.Equal(t, "RECORDED", ds[1].Status)
			assert.Equal(t, "after", ds[1].Body)
			assert.Equal(t, *published.MessageId, ds[1].MessageID)
		})
	}
}

func TestConfirmationTokenChecks(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	topic := e.mkTopic(t, "tok")
	sub := e.subscribe(t, topic, "EMAIL", "a@example.com")

	tests := []struct {
		id       *string
		name     string
		token    string
		protocol string
		code     int
	}{
		{sub.Id, "wrong token", "nope", "EMAIL", http.StatusBadRequest},
		{sub.Id, "wrong protocol", "x", "SMS", http.StatusBadRequest},
		{new("missing"), "unknown subscription", "x", "EMAIL", http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := e.dp.GetConfirmSubscription(t.Context(), ons.GetConfirmSubscriptionRequest{
				Id: tt.id, Token: new(tt.token), Protocol: new(tt.protocol),
			})
			assert.Equal(t, tt.code, svcErr(t, err).GetHTTPStatusCode())
		})
	}
}

func TestResendConfirmation(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	topic := e.mkTopic(t, "resend")
	sub := e.subscribe(t, topic, "EMAIL", "a@example.com")

	_, err := e.dp.ResendSubscriptionConfirmation(t.Context(), ons.ResendSubscriptionConfirmationRequest{Id: sub.Id})
	require.NoError(t, err)

	ds := e.deliveries(t, *sub.Id)
	require.Len(t, ds, 2)
	assert.Equal(t, "RECORDED", ds[1].Status)

	e.confirm(t, sub, ds[1].ConfirmationURL)

	_, err = e.dp.ResendSubscriptionConfirmation(t.Context(), ons.ResendSubscriptionConfirmationRequest{Id: sub.Id})
	assert.Equal(t, http.StatusConflict, svcErr(t, err).GetHTTPStatusCode())
}

func TestUnsubscribeLink(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{enforce: true, register: true})
	topic := e.mkTopic(t, "unsub")
	sink := newSink(t)
	sub := e.subscribe(t, topic, "HTTPS", sink.srv.URL)
	e.confirm(t, sub, sink.next(t).header.Get("X-Oci-Ns-Confirmationurl"))

	e.publish(t, topic, "hello")
	link := sink.next(t).header.Get("X-Oci-Ns-Unsubscribeurl")

	status, _ := rawDo(t, http.MethodGet, link, "")
	require.Equal(t, http.StatusOK, status)

	_, err := e.dp.GetSubscription(t.Context(), ons.GetSubscriptionRequest{SubscriptionId: sub.Id})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())

	status, _ = rawDo(t, http.MethodGet, link, "")
	assert.Equal(t, http.StatusNotFound, status)
}

func TestUnsubscriptionThroughSDK(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	topic := e.mkTopic(t, "sdkunsub")
	sub := e.subscribe(t, topic, "EMAIL", "a@example.com")

	link := e.deliveries(t, *sub.Id)[0].UnsubscribeURL
	u, err := url.Parse(link)
	require.NoError(t, err)

	resp, err := e.dp.GetUnsubscription(t.Context(), ons.GetUnsubscriptionRequest{
		Id: sub.Id, Token: new(u.Query().Get("token")), Protocol: new("EMAIL"),
	})
	require.NoError(t, err)
	require.NotNil(t, resp.Value)
	assert.Contains(t, *resp.Value, *sub.Id)
}

func TestSubscriptionManagement(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	topic := e.mkTopic(t, "mgmt")
	sub := e.subscribe(t, topic, "PAGERDUTY", "key")

	_, err := e.dp.ChangeSubscriptionCompartment(t.Context(), ons.ChangeSubscriptionCompartmentRequest{
		SubscriptionId: sub.Id,
		ChangeSubscriptionCompartmentDetails: ons.ChangeCompartmentDetails{
			CompartmentId: new("ocid1.compartment.oc1..other"),
		},
	})
	require.NoError(t, err)

	got, err := e.dp.GetSubscription(t.Context(), ons.GetSubscriptionRequest{SubscriptionId: sub.Id})
	require.NoError(t, err)
	assert.Equal(t, "ocid1.compartment.oc1..other", *got.CompartmentId)

	_, err = e.dp.DeleteSubscription(
		t.Context(),
		ons.DeleteSubscriptionRequest{SubscriptionId: sub.Id, IfMatch: new("stale")},
	)
	assert.Equal(t, http.StatusPreconditionFailed, svcErr(t, err).GetHTTPStatusCode())

	_, err = e.dp.DeleteSubscription(
		t.Context(),
		ons.DeleteSubscriptionRequest{SubscriptionId: sub.Id, IfMatch: got.Etag},
	)
	require.NoError(t, err)

	_, err = e.dp.DeleteSubscription(t.Context(), ons.DeleteSubscriptionRequest{SubscriptionId: sub.Id})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestFunctionsDelivery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		invoker func(context.Context, string, http.Header, []byte) error
		want    string
	}{
		{"invoked", func(context.Context, string, http.Header, []byte) error { return nil }, "DELIVERED"},
		{"no backend", nil, "RECORDED"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})

			var (
				mu   sync.Mutex
				seen []string
			)

			if tt.invoker != nil {
				e.backend.SetFunctionInvoker(func(ctx context.Context, fn string, h http.Header, body []byte) error {
					mu.Lock()
					seen = append(seen, fn+"|"+string(body)+"|"+headerValue(h, "x-oci-ns-topicname"))
					mu.Unlock()

					return tt.invoker(ctx, fn, h, body)
				})
			}

			topic := e.mkTopic(t, "fn")
			sub := e.subscribe(t, topic, "ORACLE_FUNCTIONS", "ocid1.fnfunc.oc1..abc")
			require.Equal(t, ons.SubscriptionLifecycleStateActive, sub.LifecycleState)

			e.publish(t, topic, "payload")

			require.Eventually(t, func() bool {
				ds := e.deliveries(t, *sub.Id)

				return len(ds) == 1 && ds[0].Status == tt.want
			}, 5*time.Second, 5*time.Millisecond)

			mu.Lock()
			defer mu.Unlock()

			if tt.invoker != nil {
				assert.Equal(t, []string{"ocid1.fnfunc.oc1..abc|payload|fn"}, seen)
			}
		})
	}
}

var errFunctionFailed = errors.New("function failed")

func TestFunctionsRetryOnError(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})

	calls := make(chan struct{}, 8)
	e.backend.SetFunctionInvoker(func(context.Context, string, http.Header, []byte) error {
		calls <- struct{}{}

		return errFunctionFailed
	})

	topic := e.mkTopic(t, "fnretry")
	sub := e.subscribe(t, topic, "ORACLE_FUNCTIONS", "ocid1.fnfunc.oc1..abc")
	e.publish(t, topic, "payload")

	<-calls
	<-calls

	require.Eventually(t, func() bool {
		ds := e.deliveries(t, *sub.Id)

		return len(ds) == 1 && ds[0].Attempts == 2 && ds[0].LastError == "function failed"
	}, 5*time.Second, 5*time.Millisecond)
}

// headerValue reads a header whose key keeps Oracle's documented casing, so Header.Get cannot find it.
func headerValue(h http.Header, name string) string {
	for k, v := range h {
		if strings.EqualFold(k, name) && len(v) > 0 {
			return v[0]
		}
	}

	return ""
}
