package azurearm_test

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/aadauth"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

func benchARM(b *testing.B) http.Handler {
	b.Helper()

	backend, registry := azurearm.NewTestRegistryWithStorage()

	issuer, err := aadauth.NewIssuer()
	if err != nil {
		b.Fatal(err)
	}

	h := azurearm.NewHandler(backend, registry, issuer, azurearm.DefaultSettings())
	e := echo.New()
	e.Any("/*", h.Handler())

	return e
}

func benchARMDo(b *testing.B, e http.Handler, method, path string, body []byte) {
	b.Helper()

	var req *http.Request
	if body != nil {
		req = httptest.NewRequest(method, path, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, path, http.NoBody)
	}

	req.Host = "host:10006"

	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	if rec.Code >= 400 {
		b.Fatalf("%s %s: %d %s", method, path, rec.Code, rec.Body.String())
	}
}

const benchVNetPath = "/subscriptions/sub1/resourceGroups/rg%d/providers/Microsoft.Network/" +
	"applicationGateways/vnet%d?api-version=2023-01-01"

func BenchmarkARMPutGeneric(b *testing.B) {
	e := benchARM(b)
	body := []byte(
		`{"location":"eastus","tags":{"a":"b"},"properties":{"addressSpace":{"addressPrefixes":["10.0.0.0/16"]}}}`,
	)
	b.ReportAllocs()
	b.ResetTimer()

	i := 0
	for b.Loop() {
		benchARMDo(b, e, http.MethodPut, fmt.Sprintf(benchVNetPath, i%20, i), body)
		i++
	}
}

func BenchmarkARMGetList10k(b *testing.B) {
	e := benchARM(b)
	body := []byte(`{"location":"eastus","tags":{"a":"b"},"properties":{"x":1}}`)

	for i := range 10000 {
		benchARMDo(b, e, http.MethodPut, fmt.Sprintf(benchVNetPath, i%20, i), body)
	}

	// other types to make the list filter scan non-matching resources
	for i := range 10000 {
		benchARMDo(
			b,
			e,
			http.MethodPut,
			fmt.Sprintf(
				"/subscriptions/sub1/resourceGroups/rg%d/providers/Microsoft.Network/"+
					"networkSecurityGroups/nsg%d?api-version=2023-01-01",
				i%20,
				i,
			),
			body,
		)
	}

	b.Run("get", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			benchARMDo(b, e, http.MethodGet, fmt.Sprintf(benchVNetPath, 7, 7), nil)
		}
	})
	b.Run("list_rg", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			benchARMDo(
				b,
				e,
				http.MethodGet,
				"/subscriptions/sub1/resourceGroups/rg3/providers/Microsoft.Network/applicationGateways?api-version=2023-01-01",
				nil,
			)
		}
	})
	b.Run("list_sub", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			benchARMDo(b, e, http.MethodGet,
				"/subscriptions/sub1/providers/Microsoft.Network/applicationGateways?api-version=2023-01-01", nil)
		}
	})
}
