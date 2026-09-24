package sns_test

// Benchmarks for the gopherstack perf sweep's SNS Publish investigation
// (2026-09-24), extending the 2026-09-19 sweep (perf_sweep_bench_test.go)
// which only exercised sqs subscriptions and so never drove
// deliverHTTPWithMeta / buildHTTPDeliveryPayload. These mix in live HTTP
// subscribers and wait for every delivery to land before ending each
// iteration, so the RSA signing work those async goroutines do is actually
// inside the timed/profiled window instead of racing past it.

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sns"
)

const (
	mixedBenchSQSSubs  = 10
	mixedBenchHTTPSubs = 5
)

// newMixedSubscriberBackend creates a topic with mixedBenchSQSSubs sqs
// subscriptions (no queue wired, matching perf_sweep_bench_test.go) and
// mixedBenchHTTPSubs live http subscriptions backed by an httptest.Server
// that reports each delivered body on delivered.
func newMixedSubscriberBackend(tb testing.TB, delivered chan<- struct{}) (*sns.Handler, *httptest.Server, string) {
	tb.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		_ = r.Body.Close()
		w.WriteHeader(http.StatusOK)
		delivered <- struct{}{}
	}))

	backend := sns.NewInMemoryBackend()
	h := sns.NewHandler(backend)

	topic, err := backend.CreateTopic("bench-mixed-topic", nil)
	require.NoError(tb, err)

	for i := range mixedBenchSQSSubs {
		_, subErr := backend.Subscribe(
			topic.TopicArn, "sqs",
			"arn:aws:sqs:us-east-1:000000000000:bench-mixed-queue-"+strconv.Itoa(i), "")
		require.NoError(tb, subErr)
	}

	for range mixedBenchHTTPSubs {
		_, subErr := backend.Subscribe(topic.TopicArn, "http", srv.URL, "")
		require.NoError(tb, subErr)
	}

	return h, srv, topic.TopicArn
}

// BenchmarkPublish_10SQS5HTTP publishes to a topic with 10 sqs and 5 http
// subscribers, through the full HTTP handler, waiting each iteration for all
// 5 http deliveries to complete so their signing cost is inside the timed
// window.
func BenchmarkPublish_10SQS5HTTP(b *testing.B) {
	delivered := make(chan struct{}, mixedBenchHTTPSubs)
	h, srv, topicArn := newMixedSubscriberBackend(b, delivered)
	defer srv.Close()

	form := url.Values{
		"Action":   {"Publish"},
		"TopicArn": {topicArn},
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

		for range mixedBenchHTTPSubs {
			<-delivered
		}
	}
}

// BenchmarkPublishBatch_10SQS5HTTP publishes a 10-entry PublishBatch to a
// topic with 10 sqs and 5 http subscribers, through the full HTTP handler,
// waiting each iteration for all 50 resulting http deliveries (5 http subs x
// 10 batch entries) to complete.
func BenchmarkPublishBatch_10SQS5HTTP(b *testing.B) {
	const batchEntries = 10

	delivered := make(chan struct{}, mixedBenchHTTPSubs*batchEntries)
	h, srv, topicArn := newMixedSubscriberBackend(b, delivered)
	defer srv.Close()

	form := url.Values{
		"Action":   {"PublishBatch"},
		"TopicArn": {topicArn},
	}
	for i := 1; i <= batchEntries; i++ {
		n := strconv.Itoa(i)
		form.Add("PublishBatchRequestEntries.member."+n+".Id", "entry-"+n)
		form.Add("PublishBatchRequestEntries.member."+n+".Message", "pgoload batch body "+n)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		rec := snsPostB(b, h, form)
		if rec.Code != http.StatusOK {
			b.Fatalf("PublishBatch failed: %d %s", rec.Code, rec.Body.String())
		}

		for range mixedBenchHTTPSubs * batchEntries {
			<-delivered
		}
	}
}
