package app

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/docdb"
)

func TestWireDocDBCA(t *testing.T) {
	t.Parallel()

	tests := []struct {
		path string
	}{{docdbCAPath}, {docdbBundlePath}}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			h := docdb.NewHandler(docdb.NewInMemoryBackend("000000000000", "us-east-1"))
			wireDocDBCA(e, []service.Registerable{h})

			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tt.path, nil))
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}
