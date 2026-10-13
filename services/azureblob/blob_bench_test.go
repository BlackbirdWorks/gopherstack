package azureblob_test

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/services/azureblob"
)

func benchDo(b *testing.B, e *echo.Echo, h *azureblob.Handler, method, path string, body []byte) {
	b.Helper()

	var req *http.Request
	if body != nil {
		req = httptest.NewRequest(method, path, bytes.NewReader(body))
		req.Header.Set("X-Ms-Blob-Type", "BlockBlob")
	} else {
		req = httptest.NewRequest(method, path, http.NoBody)
	}

	rec := httptest.NewRecorder()
	if err := h.Handler()(e.NewContext(req, rec)); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkBlobPutGet(b *testing.B) {
	for _, size := range []int{1 << 10, 4 << 20} {
		b.Run(fmt.Sprintf("put_%d", size), func(b *testing.B) {
			be := azureblob.NewInMemoryBackend()
			_ = be.CreateContainer("c")
			h := azureblob.NewHandler(be)
			e := echo.New()
			data := bytes.Repeat([]byte("x"), size)
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				benchDo(b, e, h, http.MethodPut, "/acct/c/blob", data)
			}
		})
		b.Run(fmt.Sprintf("get_%d", size), func(b *testing.B) {
			be := azureblob.NewInMemoryBackend()
			_ = be.CreateContainer("c")
			_, _ = be.PutBlob("c", "blob", bytes.Repeat([]byte("x"), size), "text/plain")
			h := azureblob.NewHandler(be)
			e := echo.New()
			b.SetBytes(int64(size))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				benchDo(b, e, h, http.MethodGet, "/acct/c/blob", nil)
			}
		})
	}
}

func BenchmarkBlobList10k(b *testing.B) {
	be := azureblob.NewInMemoryBackend()
	_ = be.CreateContainer("c")
	for i := range 10000 {
		_, _ = be.PutBlob("c", fmt.Sprintf("dir%d/blob%05d", i%10, i), []byte("x"), "text/plain")
	}
	h := azureblob.NewHandler(be)
	e := echo.New()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		benchDo(b, e, h, http.MethodGet, "/acct/c?restype=container&comp=list", nil)
	}
}

func BenchmarkBlobHead(b *testing.B) {
	be := azureblob.NewInMemoryBackend()
	_ = be.CreateContainer("c")
	_, _ = be.PutBlob("c", "blob", []byte("x"), "text/plain")
	h := azureblob.NewHandler(be)
	e := echo.New()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		benchDo(b, e, h, http.MethodHead, "/acct/c/blob", nil)
	}
}
