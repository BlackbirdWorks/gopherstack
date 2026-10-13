package azurestoragevhost_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/services/azurestoragevhost"
)

type noopStorage struct{}

func (noopStorage) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error { return c.NoContent(http.StatusOK) }
}

func BenchmarkVHostRoute(b *testing.B) {
	h := azurestoragevhost.NewHandler()
	h.Blob, h.Queue, h.Table = noopStorage{}, noopStorage{}, noopStorage{}
	fn := h.Handler()
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/container/blob?x=1", http.NoBody)
	req.Host = "myaccount.BLOB.localhost:10010"
	rec := httptest.NewRecorder()
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		if err := fn(e.NewContext(req, rec)); err != nil {
			b.Fatal(err)
		}
	}
}
