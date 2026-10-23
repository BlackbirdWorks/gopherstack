package cosmosdb_test

import (
	"encoding/base64"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuth_ExtraKeysAcceptedUntilRevoked(t *testing.T) {
	t.Parallel()

	extra := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))

	signer := authHandler(t, false)
	signer.MasterKey = extra

	tests := []struct {
		name   string
		accept bool
		revoke bool
		want   int
	}{
		{name: "unknown_key_rejected", want: http.StatusUnauthorized},
		{name: "accepted_key", accept: true, want: http.StatusOK},
		{name: "revoked_key", accept: true, revoke: true, want: http.StatusUnauthorized},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := authHandler(t, true)
			if tc.accept {
				h.AcceptKey(extra)
			}

			if tc.revoke {
				h.RevokeKey(extra)
			}

			req := newReq(t, http.MethodGet, "/dbs", "")
			require.NoError(t, signer.SignRequest(req))

			assert.Equal(t, tc.want, serve(t, h, req).Code)
		})
	}
}
