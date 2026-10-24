package service_test

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/smithy-go/encoding/cbor"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func TestRPCv2CBORBridge(t *testing.T) {
	t.Parallel()

	bridge := service.RPCv2CBORBridge{
		Name:       "Svc_1",
		BinaryKeys: map[string]bool{"Blob": true},
		TimeKeys:   map[string]bool{"At": true},
	}

	tests := []struct {
		name       string
		path       string
		wantTarget string
		wantStatus int
	}{
		{"ok", "/service/Svc_1/operation/Echo", "Svc_1.Echo", http.StatusOK},
		{"error", "/service/Svc_1/operation/Fail", "Svc_1.Fail", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var gotTarget string

			var gotBody []byte

			h := bridge.Wrap(func(c *echo.Context) error {
				gotTarget = c.Request().Header.Get("X-Amz-Target")
				gotBody = make([]byte, c.Request().ContentLength)
				_, _ = c.Request().Body.Read(gotBody)
				c.Response().Header().Set("Content-Type", "application/x-amz-json-1.0")

				if tt.wantStatus != http.StatusOK {
					return c.JSONBlob(tt.wantStatus, []byte(`{"__type":"Boom","message":"bad"}`))
				}

				return c.JSONBlob(http.StatusOK, []byte(`{"Blob":"aGk=","At":1.5,"N":3}`))
			})

			in := cbor.Encode(cbor.Map{"Blob": cbor.Slice("hi"), "N": cbor.Uint(7)})
			req := httptest.NewRequest(http.MethodPost, tt.path, bytes.NewReader(in))
			rec := httptest.NewRecorder()

			require.True(t, bridge.Matches(req))
			require.NoError(t, h(echo.New().NewContext(req, rec)))

			assert.Equal(t, tt.wantTarget, gotTarget)
			assert.JSONEq(t, `{"Blob":"aGk=","N":7}`, string(gotBody))
			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Equal(t, "rpc-v2-cbor", rec.Header().Get("Smithy-Protocol"))

			out, err := cbor.Decode(rec.Body.Bytes())
			require.NoError(t, err)

			m, ok := out.(cbor.Map)
			require.True(t, ok)

			if tt.wantStatus != http.StatusOK {
				assert.Equal(t, cbor.String("Boom"), m["__type"])

				return
			}

			assert.Equal(t, cbor.Slice("hi"), m["Blob"])
			assert.Equal(t, cbor.Uint(3), m["N"])
			assert.IsType(t, &cbor.Tag{}, m["At"])
		})
	}
}
