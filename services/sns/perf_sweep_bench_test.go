package sns_test

// Benchmark for the gopherstack perf sweep's SNS Publish investigation
// (2026-09-19), run through the full HTTP handler (matching production
// request path: form decode -> dispatch -> backend -> XML encode).
//
// Subscriptions use protocol "sqs" with no queue actually wired to the
// topic's published-event stream, so Publish's per-subscription snapshot
// work is exercised with zero real network I/O or cross-service delivery.

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sns"
)

func snsPostB(tb testing.TB, h *sns.Handler, form url.Values) *httptest.ResponseRecorder {
	tb.Helper()

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(tb, h.Handler()(c))

	return rec
}

// BenchmarkPublish_10Subscriptions runs Publish against a topic with 10 sqs
// subscriptions, through the full HTTP handler.
func BenchmarkPublish_10Subscriptions(b *testing.B) {
	backend := sns.NewInMemoryBackend()
	h := sns.NewHandler(backend)

	topic, err := backend.CreateTopic("bench-topic", nil)
	require.NoError(b, err)

	for i := range 10 {
		_, subErr := backend.Subscribe(
			topic.TopicArn, "sqs",
			"arn:aws:sqs:us-east-1:000000000000:bench-queue-"+strconv.Itoa(i), "")
		require.NoError(b, subErr)
	}

	form := url.Values{
		"Action":   {"Publish"},
		"TopicArn": {topic.TopicArn},
		"Message":  {"pgoload notification worker=0 iter=0"},
		"Subject":  {"pgoload"},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		rec := snsPostB(b, h, form)
		if rec.Code != http.StatusOK {
			b.Fatalf("Publish failed: %d %s", rec.Code, rec.Body.String())
		}
	}
}
