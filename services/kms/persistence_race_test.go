package kms_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kms"
)

// Read ops lazily register per-region tables under tableMu while Snapshot holds only b.mu (gopherstack-fwd0g).
func TestSnapshot_RacesWithLazyRegionTables(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
	}{
		{name: "list_keys_new_regions", target: "TrentService.ListKeys"},
		{name: "list_aliases_new_regions", target: "TrentService.ListAliases"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kms.NewInMemoryBackend()
			h := kms.NewHandler(b)
			e := echo.New()

			var wg sync.WaitGroup

			stop := make(chan struct{})

			wg.Go(func() {
				for {
					select {
					case <-stop:
						return
					default:
					}

					_ = b.Snapshot(t.Context())
				}
			})

			for i := range 200 {
				req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
				req.Header.Set("X-Amz-Target", tt.target)
				req.Header.Set("X-Amz-Region", fmt.Sprintf("xx-region-%d", i))

				require.NoError(t, h.Handler()(e.NewContext(req, httptest.NewRecorder())))
			}

			close(stop)
			wg.Wait()
		})
	}
}
