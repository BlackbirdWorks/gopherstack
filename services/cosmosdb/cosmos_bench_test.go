package cosmosdb_test

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/services/cosmosdb"
)

func benchCosmos(b *testing.B, n int) (*cosmosdb.Handler, *echo.Echo) {
	b.Helper()

	be := cosmosdb.NewInMemoryBackend()
	if _, err := be.CreateDatabase("db"); err != nil {
		b.Fatal(err)
	}

	if _, err := be.CreateContainer("db", cosmosdb.ContainerSpec{ID: "c", PartitionKeyPath: "/pk"}); err != nil {
		b.Fatal(err)
	}

	for i := range n {
		doc := map[string]any{
			"id":    fmt.Sprintf("doc%06d", i),
			"pk":    fmt.Sprintf("p%d", i%20),
			"value": i,
			"name":  fmt.Sprintf("name-%d", i%500),
			"tags":  []any{"a", "b"},
		}
		if _, err := be.CreateDocument("db", "c", doc, false); err != nil {
			b.Fatal(err)
		}
	}

	return cosmosdb.NewHandler(be), echo.New()
}

func benchCosmosDo(
	b *testing.B,
	e *echo.Echo,
	h *cosmosdb.Handler,
	method, path string,
	hdr map[string]string,
	body []byte,
) {
	b.Helper()

	var req *http.Request
	if body != nil {
		req = httptest.NewRequest(method, path, bytes.NewReader(body))
	} else {
		req = httptest.NewRequest(method, path, http.NoBody)
	}

	for k, v := range hdr {
		req.Header.Set(k, v)
	}

	rec := httptest.NewRecorder()
	if err := h.Handler()(e.NewContext(req, rec)); err != nil {
		b.Fatal(err)
	}

	if rec.Code >= 400 {
		b.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
}

func BenchmarkCosmosCreateDocument(b *testing.B) {
	h, e := benchCosmos(b, 0)
	b.ReportAllocs()
	b.ResetTimer()

	i := 0
	for b.Loop() {
		body := fmt.Appendf(nil, `{"id":"d%d","pk":"p%d","value":%d,"name":"x"}`, i, i%20, i)
		benchCosmosDo(b, e, h, http.MethodPost, "/dbs/db/colls/c/docs", nil, body)
		i++
	}
}

func BenchmarkCosmosGetDocument10k(b *testing.B) {
	h, e := benchCosmos(b, 10000)
	hdr := map[string]string{"X-Ms-Documentdb-Partitionkey": `["p7"]`}
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		benchCosmosDo(b, e, h, http.MethodGet, "/dbs/db/colls/c/docs/doc000007", hdr, nil)
	}
}

func BenchmarkCosmosQuery10k(b *testing.B) {
	h, e := benchCosmos(b, 10000)
	hdr := map[string]string{"X-Ms-Documentdb-Isquery": "true", "Content-Type": "application/query+json"}

	for _, tc := range []struct{ name, q string }{
		{"where_eq", `{"query":"SELECT * FROM c WHERE c.name = @n AND c.value > 100",` +
			`"parameters":[{"name":"@n","value":"name-7"}]}`},
		{"where_pk_top10", `{"query":"SELECT TOP 10 c.id, c.value FROM c WHERE c.pk = 'p3'"}`},
		{"order_by_limit", `{"query":"SELECT * FROM c WHERE c.value >= 0 ORDER BY c.value DESC OFFSET 0 LIMIT 20"}`},
		{"select_all_top100", `{"query":"SELECT TOP 100 * FROM c"}`},
	} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()

			for b.Loop() {
				benchCosmosDo(b, e, h, http.MethodPost, "/dbs/db/colls/c/docs", hdr, []byte(tc.q))
			}
		})
	}
}

func BenchmarkCosmosListDocuments10k(b *testing.B) {
	h, e := benchCosmos(b, 10000)
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		benchCosmosDo(b, e, h, http.MethodGet, "/dbs/db/colls/c/docs", nil, nil)
	}
}
