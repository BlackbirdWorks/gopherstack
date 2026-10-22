package client_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/internal/dockercompat/client"
)

const pushDigestValue = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestImagePush(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		stream     string
		wantDigest string
		wantErr    string
	}{
		{
			name: "digest from aux",
			stream: `{"status":"Pushed"}` + "\n" +
				`{"aux":{"Tag":"v1","Digest":"` + pushDigestValue + `","Size":527}}` + "\n",
			wantDigest: pushDigestValue,
		},
		{
			name: "digest from the closing status line",
			stream: `{"status":"Pushed"}` + "\n" +
				`{"status":"v1: digest: ` + pushDigestValue + ` size: 527"}` + "\n",
			wantDigest: pushDigestValue,
		},
		{name: "no digest reported", stream: `{"status":"Pushed"}` + "\n"},
		{
			name:    "daemon reports an error",
			stream:  `{"errorDetail":{"message":"denied"},"error":"denied"}` + "\n",
			wantErr: "denied",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !strings.HasSuffix(r.URL.Path, "/push") {
					w.WriteHeader(http.StatusNotFound)

					return
				}

				assert.Equal(t, "e30=", r.Header.Get("X-Registry-Auth"), "anonymous registry auth")
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(tt.stream))
			}))
			t.Cleanup(srv.Close)

			c, err := client.NewClientWithOpts(client.WithHost("tcp://" + strings.TrimPrefix(srv.URL, "http://")))
			require.NoError(t, err)
			t.Cleanup(func() { _ = c.Close() })

			digest, err := c.ImagePush(t.Context(), "localhost:5000/app:v1")
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantDigest, digest)
		})
	}
}
