package sqs_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/services/sqs"
)

// tickClock advances virtual time by step on every read, keeping FIFO
// throughput windows (300 calls/sec, 3000 msgs/sec) from throttling
// benchmark iterations without any real sleep.
type tickClock struct {
	now  time.Time
	step time.Duration
	mu   sync.Mutex
}

func newTickClock(step time.Duration) *tickClock {
	return &tickClock{now: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), step: step}
}

func (c *tickClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.now = c.now.Add(c.step)

	return c.now
}

func benchJSONRequest(b *testing.B, h *sqs.Handler, action string, body []byte) {
	b.Helper()

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "AmazonSQS."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	if err := h.Handler()(c); err != nil {
		b.Fatalf("%s: %v", action, err)
	}
}

// benchQueryRequest sends a pre-encoded Query-protocol body. Callers encode
// vals.Encode() once outside the timed loop so the benchmark measures the
// server's parse/handle cost, not client-side re-encoding on every iteration.
func benchQueryRequest(b *testing.B, h *sqs.Handler, action string, body []byte) {
	b.Helper()

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	if err := h.Handler()(c); err != nil {
		b.Fatalf("query %s: %v", action, err)
	}
}

func jsonSendBody(b *testing.B, qURL string) []byte {
	b.Helper()

	body, err := json.Marshal(map[string]any{
		"QueueUrl":    qURL,
		"MessageBody": "benchmark payload of representative length for SQS SendMessage",
		"MessageAttributes": map[string]any{
			"attr-one": map[string]any{"DataType": "String", "StringValue": "value-one"},
			"attr-two": map[string]any{"DataType": "Number", "StringValue": "42"},
		},
	})
	if err != nil {
		b.Fatalf("marshal send body: %v", err)
	}

	return body
}

func BenchmarkJSONSendMessage(b *testing.B) {
	backend := newBenchBackend(b)
	h := sqs.NewHandler(backend)
	qURL := benchCreateQueue(b, backend, "bench-json-send-q")
	body := jsonSendBody(b, qURL)

	b.ReportAllocs()

	for b.Loop() {
		benchJSONRequest(b, h, "SendMessage", body)
	}
}

func BenchmarkQuerySendMessage(b *testing.B) {
	backend := newBenchBackend(b)
	h := sqs.NewHandler(backend)
	qURL := benchCreateQueue(b, backend, "bench-query-send-q")

	vals := url.Values{
		"Action":                               {"SendMessage"},
		"QueueUrl":                             {qURL},
		"MessageBody":                          {"benchmark payload of representative length for SQS SendMessage"},
		"MessageAttribute.1.Name":              {"attr-one"},
		"MessageAttribute.1.Value.DataType":    {"String"},
		"MessageAttribute.1.Value.StringValue": {"value-one"},
		"MessageAttribute.2.Name":              {"attr-two"},
		"MessageAttribute.2.Value.DataType":    {"Number"},
		"MessageAttribute.2.Value.StringValue": {"42"},
	}
	body := []byte(vals.Encode())

	b.ReportAllocs()

	for b.Loop() {
		benchQueryRequest(b, h, "SendMessage", body)
	}
}

func BenchmarkJSONSendMessageBatch10(b *testing.B) {
	backend := newBenchBackend(b)
	h := sqs.NewHandler(backend)
	qURL := benchCreateQueue(b, backend, "bench-json-batch-q")

	entries := make([]map[string]any, 10)
	for i := range entries {
		entries[i] = map[string]any{
			"Id":          strconv.Itoa(i),
			"MessageBody": "batch payload " + strconv.Itoa(i),
			"MessageAttributes": map[string]any{
				"attr-one": map[string]any{"DataType": "String", "StringValue": "value-one"},
			},
		}
	}

	body, err := json.Marshal(map[string]any{"QueueUrl": qURL, "Entries": entries})
	if err != nil {
		b.Fatalf("marshal batch body: %v", err)
	}

	b.ReportAllocs()

	for b.Loop() {
		benchJSONRequest(b, h, "SendMessageBatch", body)
	}
}

func BenchmarkQuerySendMessageBatch10(b *testing.B) {
	backend := newBenchBackend(b)
	h := sqs.NewHandler(backend)
	qURL := benchCreateQueue(b, backend, "bench-query-batch-q")

	vals := url.Values{"Action": {"SendMessageBatch"}, "QueueUrl": {qURL}}
	for i := 1; i <= 10; i++ {
		n := strconv.Itoa(i)
		prefix := "SendMessageBatchRequestEntry." + n + "."
		vals.Set(prefix+"Id", n)
		vals.Set(prefix+"MessageBody", "batch payload "+n)
		vals.Set(prefix+"MessageAttribute.1.Name", "attr-one")
		vals.Set(prefix+"MessageAttribute.1.Value.DataType", "String")
		vals.Set(prefix+"MessageAttribute.1.Value.StringValue", "value-one")
	}
	body := []byte(vals.Encode())

	b.ReportAllocs()

	for b.Loop() {
		benchQueryRequest(b, h, "SendMessageBatch", body)
	}
}

const benchReceiveDepth = 10000

func setupReceiveDepthQueue(b *testing.B, backend *sqs.InMemoryBackend, name string) string {
	b.Helper()

	qURL := benchCreateQueue(b, backend, name)
	benchSendN(b, backend, qURL, benchReceiveDepth)

	return qURL
}

func BenchmarkJSONReceiveMessage10_Depth10000(b *testing.B) {
	backend := newBenchBackend(b)
	h := sqs.NewHandler(backend)
	qURL := setupReceiveDepthQueue(b, backend, "bench-json-recv-q")

	body, err := json.Marshal(map[string]any{"QueueUrl": qURL, "MaxNumberOfMessages": 10})
	if err != nil {
		b.Fatalf("marshal receive body: %v", err)
	}

	b.ReportAllocs()

	for b.Loop() {
		benchJSONRequest(b, h, "ReceiveMessage", body)
	}
}

func BenchmarkQueryReceiveMessage10_Depth10000(b *testing.B) {
	backend := newBenchBackend(b)
	h := sqs.NewHandler(backend)
	qURL := setupReceiveDepthQueue(b, backend, "bench-query-recv-q")

	vals := url.Values{
		"Action":              {"ReceiveMessage"},
		"QueueUrl":            {qURL},
		"MaxNumberOfMessages": {"10"},
		"AttributeName.1":     {"All"},
	}
	body := []byte(vals.Encode())

	b.ReportAllocs()

	for b.Loop() {
		benchQueryRequest(b, h, "ReceiveMessage", body)
	}
}

// setupFIFOManyGroups creates a FIFO queue with numGroups groups of
// perGroup messages each, sent under a virtual clock so the 300 calls/sec
// FIFO throughput budget never throttles the benchmark itself.
func setupFIFOManyGroups(b *testing.B, backend *sqs.InMemoryBackend, name string, numGroups, perGroup int) string {
	b.Helper()

	out, err := backend.CreateQueue(&sqs.CreateQueueInput{
		QueueName: name + ".fifo",
		Endpoint:  testEndpoint,
		Attributes: map[string]string{
			"FifoQueue":                 "true",
			"ContentBasedDeduplication": "true",
		},
	})
	if err != nil {
		b.Fatalf("CreateQueue: %v", err)
	}

	clock := newTickClock(4 * time.Millisecond)
	sqs.SetNowFunc(backend, clock.Now)

	for g := range numGroups {
		group := "group-" + strconv.Itoa(g)
		for i := range perGroup {
			_, sendErr := backend.SendMessage(&sqs.SendMessageInput{
				QueueURL:       out.QueueURL,
				MessageBody:    "fifo body " + strconv.Itoa(g) + "-" + strconv.Itoa(i),
				MessageGroupID: group,
			})
			if sendErr != nil {
				b.Fatalf("SendMessage: %v", sendErr)
			}
		}
	}

	return out.QueueURL
}

func BenchmarkJSONReceiveMessage10_FIFOManyGroups(b *testing.B) {
	backend := newBenchBackend(b)
	h := sqs.NewHandler(backend)
	qURL := setupFIFOManyGroups(b, backend, "bench-json-fifo-recv-q", 1000, 10)

	body, err := json.Marshal(map[string]any{"QueueUrl": qURL, "MaxNumberOfMessages": 10})
	if err != nil {
		b.Fatalf("marshal receive body: %v", err)
	}

	b.ReportAllocs()

	for b.Loop() {
		benchJSONRequest(b, h, "ReceiveMessage", body)
	}
}

func BenchmarkQueryReceiveMessage10_FIFOManyGroups(b *testing.B) {
	backend := newBenchBackend(b)
	h := sqs.NewHandler(backend)
	qURL := setupFIFOManyGroups(b, backend, "bench-query-fifo-recv-q", 1000, 10)

	vals := url.Values{
		"Action":              {"ReceiveMessage"},
		"QueueUrl":            {qURL},
		"MaxNumberOfMessages": {"10"},
		"AttributeName.1":     {"All"},
	}
	body := []byte(vals.Encode())

	b.ReportAllocs()

	for b.Loop() {
		benchQueryRequest(b, h, "ReceiveMessage", body)
	}
}

// setupInFlightHandles receives depth/10 batches of 10 messages under a long
// visibility timeout (direct backend calls, no HTTP) and returns all handles.
func setupInFlightHandles(b *testing.B, backend *sqs.InMemoryBackend, qURL string, depth int) []string {
	b.Helper()

	handles := make([]string, 0, depth)
	for len(handles) < depth {
		recv, err := backend.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueURL:            qURL,
			MaxNumberOfMessages: 10,
			VisibilityTimeout:   3600,
		})
		if err != nil || len(recv.Messages) == 0 {
			b.Fatalf("ReceiveMessage: %v (got %d)", err, len(recv.Messages))
		}
		handles = append(handles, receiptHandles(recv.Messages)...)
	}

	return handles
}

func BenchmarkJSONDeleteMessage(b *testing.B) {
	backend := newBenchBackend(b)
	h := sqs.NewHandler(backend)
	qURL := setupReceiveDepthQueue(b, backend, "bench-json-delete-q")
	handles := setupInFlightHandles(b, backend, qURL, benchReceiveDepth)

	bodies := make([][]byte, len(handles))
	for i, rh := range handles {
		body, err := json.Marshal(map[string]any{"QueueUrl": qURL, "ReceiptHandle": rh})
		if err != nil {
			b.Fatalf("marshal delete body: %v", err)
		}
		bodies[i] = body
	}

	b.ReportAllocs()

	for i := 0; b.Loop(); i++ {
		benchJSONRequest(b, h, "DeleteMessage", bodies[i%len(bodies)])
	}
}

func BenchmarkQueryDeleteMessage(b *testing.B) {
	backend := newBenchBackend(b)
	h := sqs.NewHandler(backend)
	qURL := setupReceiveDepthQueue(b, backend, "bench-query-delete-q")
	handles := setupInFlightHandles(b, backend, qURL, benchReceiveDepth)

	bodies := make([][]byte, len(handles))
	for i, rh := range handles {
		vals := url.Values{"Action": {"DeleteMessage"}, "QueueUrl": {qURL}, "ReceiptHandle": {rh}}
		bodies[i] = []byte(vals.Encode())
	}

	b.ReportAllocs()

	for i := 0; b.Loop(); i++ {
		benchQueryRequest(b, h, "DeleteMessage", bodies[i%len(bodies)])
	}
}

func BenchmarkJSONDeleteMessageBatch10(b *testing.B) {
	backend := newBenchBackend(b)
	h := sqs.NewHandler(backend)
	qURL := benchCreateQueue(b, backend, "bench-json-delbatch-q")

	b.ReportAllocs()

	for b.Loop() {
		b.StopTimer()
		benchSendN(b, backend, qURL, 10)
		recv, err := backend.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueURL: qURL, MaxNumberOfMessages: 10, VisibilityTimeout: 300,
		})
		if err != nil || len(recv.Messages) != 10 {
			b.Fatalf("ReceiveMessage: %v (got %d)", err, len(recv.Messages))
		}
		entries := make([]map[string]any, 10)
		for i, m := range recv.Messages {
			entries[i] = map[string]any{"Id": strconv.Itoa(i), "ReceiptHandle": m.ReceiptHandle}
		}
		body, err := json.Marshal(map[string]any{"QueueUrl": qURL, "Entries": entries})
		if err != nil {
			b.Fatalf("marshal delete batch body: %v", err)
		}
		b.StartTimer()

		benchJSONRequest(b, h, "DeleteMessageBatch", body)
	}
}

func BenchmarkQueryDeleteMessageBatch10(b *testing.B) {
	backend := newBenchBackend(b)
	h := sqs.NewHandler(backend)
	qURL := benchCreateQueue(b, backend, "bench-query-delbatch-q")

	b.ReportAllocs()

	for b.Loop() {
		b.StopTimer()
		benchSendN(b, backend, qURL, 10)
		recv, err := backend.ReceiveMessage(&sqs.ReceiveMessageInput{
			QueueURL: qURL, MaxNumberOfMessages: 10, VisibilityTimeout: 300,
		})
		if err != nil || len(recv.Messages) != 10 {
			b.Fatalf("ReceiveMessage: %v (got %d)", err, len(recv.Messages))
		}
		vals := url.Values{"Action": {"DeleteMessageBatch"}, "QueueUrl": {qURL}}
		for i, m := range recv.Messages {
			n := strconv.Itoa(i + 1)
			vals.Set("DeleteMessageBatchRequestEntry."+n+".Id", strconv.Itoa(i))
			vals.Set("DeleteMessageBatchRequestEntry."+n+".ReceiptHandle", m.ReceiptHandle)
		}
		body := []byte(vals.Encode())
		b.StartTimer()

		benchQueryRequest(b, h, "DeleteMessageBatch", body)
	}
}
