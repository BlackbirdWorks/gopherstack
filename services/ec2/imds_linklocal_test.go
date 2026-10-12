package ec2_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/internal/linklocal"
)

type ipOwners map[string]string

func (ipOwners) Active(context.Context) bool                  { return true }
func (ipOwners) Attach(context.Context, string, string) error { return nil }
func (ipOwners) Release(string)                               {}

func (o ipOwners) OwnerOf(ctx context.Context, ip string) (string, bool) {
	if ctx.Err() != nil {
		return "", false
	}

	id, ok := o[ip]

	return id, ok
}

func TestIMDSLinkLocalRouting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		peer     string
		wantOf   int
		wantCode int
		wire     bool
		canceled bool
	}{
		{name: "first_instance", wire: true, peer: "169.254.169.3", wantCode: http.StatusOK, wantOf: 0},
		{name: "second_instance", wire: true, peer: "169.254.169.4", wantCode: http.StatusOK, wantOf: 1},
		{name: "client_half_closed", wire: true, canceled: true, peer: "169.254.169.3", wantCode: http.StatusOK},
		{name: "unknown_peer", wire: true, peer: "169.254.169.99", wantCode: http.StatusNotFound},
		{name: "missing_peer", wire: true, wantCode: http.StatusNotFound},
		{name: "garbage_peer", wire: true, peer: "not-an-ip", wantCode: http.StatusNotFound},
		{name: "not_wired", peer: "169.254.169.3", wantCode: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fx := newIMDSFixture(t)
			second, err := fx.b.RunInstances("ami-0abc", "t3.micro", "", 1)
			require.NoError(t, err)

			ids := []string{fx.instanceID, second[0].ID}

			if tt.wire {
				fx.h.SetLinkLocal(ipOwners{"169.254.169.3": ids[0], "169.254.169.4": ids[1]})
			}

			e := echo.New()
			e.GET("/_gopherstack/ec2/imds/:instance/*", fx.h.ServeIMDS)

			req := httptest.NewRequest(
				http.MethodGet,
				"/_gopherstack/ec2/imds/linklocal/latest/meta-data/instance-id",
				nil,
			)
			if tt.peer != "" {
				req.Header.Set(linklocal.PeerHeader, tt.peer)
			}

			if tt.canceled {
				ctx, cancel := context.WithCancel(req.Context())
				cancel()

				req = req.WithContext(ctx)
			}

			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, req)

			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				assert.Equal(t, ids[tt.wantOf], rec.Body.String())
			}
		})
	}
}
