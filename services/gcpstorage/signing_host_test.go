package gcpstorage_test

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	"github.com/blackbirdworks/gopherstack/services/gcpstorage"
)

func TestSignedURLHostBinding(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		host     string
		wantCode string
		want     int
	}{
		{name: "path style on endpoint host", host: "storage.googleapis.com", want: http.StatusOK},
		{
			name: "replayed on virtual host", host: "other.storage.googleapis.com",
			want: http.StatusForbidden, wantCode: "SignatureDoesNotMatch",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newSignEnv(t)
			_, err := s.backend.CreateBucket(sdkProject, gcpstorage.Patch{"name": []byte(`"other"`)})
			require.NoError(t, err)

			_, err = s.backend.PutObject(
				"other", "bkt/dir/a b.txt", []byte("leak"), gcpstorage.Patch{}, gcpstorage.Preconds{})
			require.NoError(t, err)

			key, err := s.backend.CreateHMACKey(sdkProject, saA)
			require.NoError(t, err)

			signed := s.signV4HMACHost(http.MethodGet, key.AccessID, key.Secret,
				"storage.googleapis.com", time.Now().UTC(), 600)
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
				strings.Replace(signed, "storage.googleapis.com", s.host, 1), nil)
			require.NoError(t, err)

			req.Host = tt.host

			resp, err := testclient.New(t).Do(req)
			require.NoError(t, err)

			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			assert.Equal(t, tt.want, resp.StatusCode, string(body))
			assert.NotContains(t, string(body), "leak")

			if tt.wantCode != "" {
				assert.Contains(t, string(body), "<Code>"+tt.wantCode+"</Code>")
			}
		})
	}
}
