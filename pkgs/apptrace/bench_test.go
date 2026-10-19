package apptrace_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/apptrace"
)

func benchServe(b *testing.B, tr *apptrace.Tracer) {
	b.Helper()

	e := echo.New()
	if tr != nil {
		e.Use(apptrace.Middleware(tr))
	}

	e.POST("/*", func(c *echo.Context) error {
		apptrace.FromContext(c.Request().Context()).SetOperation("sqs", "SendMessage", "q")

		return c.String(http.StatusOK, `{"MessageId":"abc"}`)
	})

	body := `{"QueueUrl":"http://x/q","MessageBody":"hello"}`

	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=AK/x")
		e.ServeHTTP(httptest.NewRecorder(), req)
	}
}

func BenchmarkRequestNoTracer(b *testing.B) { benchServe(b, nil) }

func BenchmarkRequestMetadataOnly(b *testing.B) {
	benchServe(b, apptrace.New(apptrace.Config{Enabled: true}))
}

func BenchmarkRequestDetailCapture(b *testing.B) {
	benchServe(b, apptrace.New(apptrace.Config{Enabled: true, Detail: true}))
}

func BenchmarkRequestDisabled(b *testing.B) {
	benchServe(b, apptrace.New(apptrace.Config{}))
}
