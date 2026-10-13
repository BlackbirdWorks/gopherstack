package azuretable_test

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/odatatable"
	"github.com/blackbirdworks/gopherstack/services/azuretable"
)

func benchTableHandler(b *testing.B, n int) (*azuretable.Handler, *echo.Echo) {
	b.Helper()

	be := azuretable.NewInMemoryBackend()
	if err := be.CreateTable("t"); err != nil {
		b.Fatal(err)
	}

	for i := range n {
		props := map[string]odatatable.EntityProperty{
			"Name":  {Type: odatatable.EdmString, Value: fmt.Sprintf("name-%d", i)},
			"Score": {Type: odatatable.EdmInt32, Value: int32(i % 100)},
			"Flag":  {Type: odatatable.EdmBoolean, Value: i%2 == 0},
		}
		if _, err := be.InsertEntity("t", fmt.Sprintf("pk%d", i%50), fmt.Sprintf("rk%06d", i), props); err != nil {
			b.Fatal(err)
		}
	}

	return azuretable.NewHandler(be), echo.New()
}

func benchTableDo(b *testing.B, e *echo.Echo, h *azuretable.Handler, method, path string, body []byte) {
	b.Helper()

	var req *http.Request
	if body != nil {
		req = httptest.NewRequest(method, path, bytes.NewReader(body))
	} else {
		req = httptest.NewRequest(method, path, http.NoBody)
	}

	req.Header.Set("Accept", "application/json;odata=minimalmetadata")

	rec := httptest.NewRecorder()
	if err := h.Handler()(e.NewContext(req, rec)); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkTableInsert(b *testing.B) {
	h, e := benchTableHandler(b, 0)
	b.ReportAllocs()
	b.ResetTimer()

	i := 0
	for b.Loop() {
		body := fmt.Appendf(nil, `{"PartitionKey":"pk","RowKey":"r%d","Name":"x","Score":5}`, i)
		benchTableDo(b, e, h, http.MethodPost, "/acct/t", body)
		i++
	}
}

func BenchmarkTableQuery10k(b *testing.B) {
	h, e := benchTableHandler(b, 10000)

	for _, tc := range []struct{ name, q string }{
		{"filter_score", "$filter=" + url.QueryEscape("Score eq 7 and Flag eq true")},
		{"filter_pk_top100", "$top=100&$filter=" + url.QueryEscape("PartitionKey eq 'pk7'")},
		{"top1000_nofilter", "$top=1000"},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				benchTableDo(b, e, h, http.MethodGet, "/acct/t()?"+tc.q, nil)
			}
		})
	}
}

func BenchmarkTableGet10k(b *testing.B) {
	h, e := benchTableHandler(b, 10000)
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		benchTableDo(b, e, h, http.MethodGet, "/acct/t(PartitionKey='pk7',RowKey='rk000007')", nil)
	}
}

func BenchmarkParseFilter(b *testing.B) {
	b.ReportAllocs()

	for b.Loop() {
		_, _ = azuretable.ParseFilter("(PartitionKey eq 'pk7' and Score ge 10) or (Name eq 'x' and not (Flag eq true))")
	}
}
