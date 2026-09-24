package sns_test

// Golden fixture and correctness check for the gopherstack perf sweep's SNS
// Publish investigation (2026-09-24), which extends the 2026-09-19 sweep
// (golden_perf_test.go, perf_sweep_bench_test.go): that earlier sweep only
// benchmarked sqs subscriptions and concluded handlePublish's cost was
// already-optimal single-sign-per-publish RSA work in buildPublishedEvent.
// It missed that buildHTTPDeliveryPayload (services/sns/delivery.go), used
// for http/https subscribers, re-signed the notification with a fresh
// time.Now() timestamp once per HTTP/HTTPS subscriber instead of once per
// publish. Per the AWS SNS message-signing spec, Timestamp reflects when the
// message was published, not when a given subscriber happened to receive it,
// so real SNS's Timestamp/Signature are publish-scoped, not delivery-scoped.
// Publish now computes one shared publish timestamp and signs each distinct
// resolved body once, reusing that signature across every HTTP/HTTPS
// subscriber (and the SQS/Lambda/Firehose/SMS/Application event) whose body
// matches.
//
// To regenerate: SNS_GOLDEN_UPDATE=1 go test -run TestGolden ./services/sns/...

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sns"
)

// TestGolden_PublishMixedSubscribersDeliveredEnvelope pins the delivered JSON
// notification envelope for an HTTP subscriber of a topic that also has an
// sqs subscriber, byte-identical (modulo the redacted fields) before and
// after the perf sweep.
func TestGolden_PublishMixedSubscribersDeliveredEnvelope(t *testing.T) {
	t.Parallel()

	received := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	backend := sns.NewInMemoryBackend()
	h := sns.NewHandler(backend)

	topic, err := backend.CreateTopic("golden-mixed-topic", nil)
	require.NoError(t, err)
	_, err = backend.Subscribe(
		topic.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:golden-mixed-queue", "")
	require.NoError(t, err)
	_, err = backend.Subscribe(topic.TopicArn, "http", ts.URL, "")
	require.NoError(t, err)

	rec := snsPost(t, h, url.Values{
		"Action":   {"Publish"},
		"TopicArn": {topic.TopicArn},
		"Message":  {"golden mixed delivered body"},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var deliveredBody string
	select {
	case deliveredBody = <-received:
	case <-time.After(2 * time.Second):
		t.Fatal("HTTP delivery did not arrive in time")
	}

	checkGoldenSNS(t, "golden_mixed_delivered_message.json", redactDeliveredEnvelope(t, []byte(deliveredBody)))
}

// TestSignature_SharedAcrossHTTPSubscribers verifies that when two HTTP/HTTPS
// subscribers of a single Publish call resolve to the same body, they receive
// the exact same Timestamp and Signature -- matching real SNS, which signs
// once per publish, not once per delivery.
func TestSignature_SharedAcrossHTTPSubscribers(t *testing.T) {
	t.Parallel()

	const numSubs = 3

	received := make(chan string, numSubs)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
		received <- string(body)
	}))
	defer ts.Close()

	backend := sns.NewInMemoryBackend()
	topic, err := backend.CreateTopic("shared-signature-topic", nil)
	require.NoError(t, err)

	for range numSubs {
		_, subErr := backend.Subscribe(topic.TopicArn, "http", ts.URL, "")
		require.NoError(t, subErr)
	}

	_, err = backend.Publish(topic.TopicArn, "shared signature body", "", "", nil)
	require.NoError(t, err)

	envelopes := make([]map[string]any, 0, numSubs)
	for range numSubs {
		select {
		case body := <-received:
			var env map[string]any
			require.NoError(t, json.Unmarshal([]byte(body), &env))
			envelopes = append(envelopes, env)
		case <-time.After(2 * time.Second):
			t.Fatal("HTTP delivery did not arrive in time")
		}
	}

	require.Len(t, envelopes, numSubs)
	wantTimestamp := envelopes[0]["Timestamp"]
	wantSignature := envelopes[0]["Signature"]
	require.NotEmpty(t, wantTimestamp)
	require.NotEmpty(t, wantSignature)

	for i, env := range envelopes {
		require.Equal(t, wantTimestamp, env["Timestamp"], "subscriber %d Timestamp diverged", i)
		require.Equal(t, wantSignature, env["Signature"], "subscriber %d Signature diverged", i)
	}
}
