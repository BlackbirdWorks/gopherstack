package sns_test

// Golden fixtures for the gopherstack perf sweep's SNS Publish investigation
// (2026-09-19). The profile showed handlePublish's cost is essentially all
// RSA signing in buildPublishedEvent, which Publish already does once per
// message (not once per subscription) -- so this sweep made no SNS code
// change. These goldens pin the current Publish response and one delivered
// HTTP notification body byte-for-byte (modulo MessageId/RequestId/
// Timestamp/Signature/UnsubscribeURL, which are randomly or wall-clock
// generated per call) as a regression baseline.
//
// To regenerate: SNS_GOLDEN_UPDATE=1 go test -run TestGolden ./services/sns/...

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sns"
)

func goldenSNSPath(name string) string {
	return filepath.Join("testdata", name)
}

func checkGoldenSNS(t *testing.T, name string, got []byte) {
	t.Helper()

	path := goldenSNSPath(name)
	if os.Getenv("SNS_GOLDEN_UPDATE") != "" {
		require.NoError(t, os.MkdirAll("testdata", 0o755))
		require.NoError(t, os.WriteFile(path, got, 0o600))

		return
	}

	want, err := os.ReadFile(path)
	require.NoError(t, err, "golden file %s missing -- run with SNS_GOLDEN_UPDATE=1 first", path)
	require.Equal(t, string(want), string(got), "response for %s changed", name)
}

var (
	xmlMessageIDRe = regexp.MustCompile(`<MessageId>[^<]*</MessageId>`)
	xmlRequestIDRe = regexp.MustCompile(`<RequestId>[^<]*</RequestId>`)
)

// redactPublishXML blanks the randomly generated MessageId/RequestId in a
// PublishResponse XML body.
func redactPublishXML(body []byte) []byte {
	body = xmlMessageIDRe.ReplaceAll(body, []byte("<MessageId>REDACTED</MessageId>"))
	body = xmlRequestIDRe.ReplaceAll(body, []byte("<RequestId>REDACTED</RequestId>"))

	return body
}

// TestGolden_PublishResponse asserts Publish's XML response is byte-identical
// (modulo MessageId/RequestId) before and after the perf sweep.
func TestGolden_PublishResponse(t *testing.T) {
	t.Parallel()

	backend := sns.NewInMemoryBackend()
	h := sns.NewHandler(backend)

	topic, err := backend.CreateTopic("golden-publish-topic", nil)
	require.NoError(t, err)
	_, err = backend.Subscribe(
		topic.TopicArn, "sqs", "arn:aws:sqs:us-east-1:000000000000:golden-queue", "")
	require.NoError(t, err)

	rec := snsPost(t, h, url.Values{
		"Action":   {"Publish"},
		"TopicArn": {topic.TopicArn},
		"Message":  {"golden publish body"},
		"Subject":  {"golden-subject"},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	checkGoldenSNS(t, "golden_publish_response.xml", redactPublishXML(rec.Body.Bytes()))
}

// redactDeliveredEnvelope normalizes the wall-clock/random fields of a
// delivered SNS HTTP notification envelope (MessageId, Timestamp, Signature,
// UnsubscribeURL -- the last embeds a randomly generated SubscriptionArn)
// before golden comparison.
func redactDeliveredEnvelope(t *testing.T, body []byte) []byte {
	t.Helper()

	var env map[string]any
	require.NoError(t, json.Unmarshal(body, &env))

	for _, k := range []string{"MessageId", "Timestamp", "Signature", "UnsubscribeURL"} {
		if _, ok := env[k]; ok {
			env[k] = "REDACTED"
		}
	}

	out, err := json.MarshalIndent(env, "", "  ")
	require.NoError(t, err)

	return out
}

// TestGolden_PublishDeliveredMessageBody asserts the JSON notification
// envelope delivered to an HTTP subscriber is byte-identical (modulo the
// fields redactDeliveredEnvelope normalizes) before and after the perf
// sweep.
func TestGolden_PublishDeliveredMessageBody(t *testing.T) {
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

	topic, err := backend.CreateTopic("golden-delivery-topic", nil)
	require.NoError(t, err)
	_, err = backend.Subscribe(topic.TopicArn, "http", ts.URL, "")
	require.NoError(t, err)

	rec := snsPost(t, h, url.Values{
		"Action":   {"Publish"},
		"TopicArn": {topic.TopicArn},
		"Message":  {"golden delivered body"},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var deliveredBody string
	select {
	case deliveredBody = <-received:
	case <-time.After(2 * time.Second):
		t.Fatal("HTTP delivery did not arrive in time")
	}

	checkGoldenSNS(t, "golden_delivered_message.json", redactDeliveredEnvelope(t, []byte(deliveredBody)))
}
