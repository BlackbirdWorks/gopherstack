package azurequeue_test

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/services/azurequeue"
)

func benchQueue(b *testing.B, n int) *azurequeue.InMemoryBackend {
	b.Helper()

	be := azurequeue.NewInMemoryBackend()
	_, _ = be.CreateQueue("q")

	for i := range n {
		if _, err := be.PutMessage("q", fmt.Sprintf("msg-%d", i), 0, 0); err != nil {
			b.Fatal(err)
		}
	}

	return be
}

func BenchmarkQueuePut(b *testing.B) {
	be := benchQueue(b, 0)
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		_, _ = be.PutMessage("q", "hello", 0, 0)
	}
}

func BenchmarkQueueGetDelete10k(b *testing.B) {
	be := benchQueue(b, 10000)
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		msgs, _ := be.GetMessages("q", 1, time.Minute)
		if len(msgs) == 0 {
			b.StopTimer()
			be = benchQueue(b, 10000)
			b.StartTimer()

			continue
		}

		_ = be.DeleteMessage("q", msgs[0].ID, msgs[0].PopReceipt)
	}
}

func BenchmarkQueueDrainInflight10k(b *testing.B) {
	be := benchQueue(b, 10000)
	msgs, _ := be.GetMessages("q", 32, time.Hour)
	for range 300 {
		_, _ = be.GetMessages("q", 32, time.Hour)
	}
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		_, _ = be.GetMessages("q", 1, time.Minute)
		_, _ = be.PeekMessages("q", 32)
	}

	_ = msgs
}

func BenchmarkQueueDeleteTail10k(b *testing.B) {
	be := benchQueue(b, 10000)
	msgs, _ := be.GetMessages("q", 32, time.Hour)
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		m := msgs[len(msgs)-1]
		// wrong receipt: exercises lookup without mutating
		_ = be.DeleteMessage("q", m.ID, "bad")
	}
}

func BenchmarkQueuePutBounded(b *testing.B) {
	be := benchQueue(b, 0)
	b.ReportAllocs()
	b.ResetTimer()

	i := 0
	for b.Loop() {
		_, _ = be.PutMessage("q", "hello", 0, 0)
		if i++; i%10000 == 0 {
			_ = be.ClearMessages("q")
		}
	}
}

func BenchmarkQueueDeleteInflightDeep(b *testing.B) {
	be := benchQueue(b, 10000)
	msgs := make([]azurequeue.MessageInfo, 0, 10000)
	for range 313 {
		m, _ := be.GetMessages("q", 32, time.Hour)
		msgs = append(msgs, m...)
	}
	b.ReportAllocs()
	b.ResetTimer()

	i := 0
	for b.Loop() {
		m := msgs[len(msgs)-1-(i%3000)]
		_ = be.DeleteMessage("q", m.ID, "bad")
		i++
	}
}

func benchQueueDo(b *testing.B, e *echo.Echo, h *azurequeue.Handler, method, path string, body []byte) {
	b.Helper()

	var req *http.Request
	if body != nil {
		req = httptest.NewRequest(method, path, bytes.NewReader(body))
	} else {
		req = httptest.NewRequest(method, path, http.NoBody)
	}

	rec := httptest.NewRecorder()
	if err := h.Handler()(e.NewContext(req, rec)); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkQueueHTTP(b *testing.B) {
	be := benchQueue(b, 0)
	h := azurequeue.NewHandler(be)
	e := echo.New()
	body := []byte(`<QueueMessage><MessageText>aGVsbG8gd29ybGQ=</MessageText></QueueMessage>`)

	b.Run("put", func(b *testing.B) {
		b.ReportAllocs()

		i := 0
		for b.Loop() {
			benchQueueDo(b, e, h, http.MethodPost, "/acct/q/messages", body)
			if i++; i%10000 == 0 {
				_ = be.ClearMessages("q")
			}
		}
	})
	b.Run("get_peek", func(b *testing.B) {
		for range 5000 {
			_, _ = be.PutMessage("q", "x", 0, 0)
		}

		b.ReportAllocs()
		b.ResetTimer()

		for b.Loop() {
			benchQueueDo(b, e, h, http.MethodGet, "/acct/q/messages?peekonly=true&numofmessages=16", nil)
		}
	})
}
