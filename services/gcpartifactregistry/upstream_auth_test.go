package gcpartifactregistry_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"cloud.google.com/go/artifactregistry/apiv1/artifactregistrypb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/gcpartifactregistry"
)

type secretMap map[string]string

var errNoSecret = errors.New("no such secret version")

func (m secretMap) AccessSecretVersion(name string) ([]byte, error) {
	if v, ok := m[name]; ok {
		return []byte(v), nil
	}

	return nil, errNoSecret
}

func TestRemoteUpstreamCredentialsFromSecretManager(t *testing.T) {
	t.Parallel()

	const secretVersion = "projects/p/secrets/up-pass/versions/1"

	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != "robot" || pass != "s3cret-pass" {
			w.WriteHeader(http.StatusUnauthorized)

			return
		}

		_, _ = w.Write([]byte(`{"name":"left-pad","dist-tags":{},"versions":{}}`))
	}))
	t.Cleanup(up.Close)

	tests := []struct {
		name    string
		secrets gcpartifactregistry.SecretResolver
		version string
		want    int
	}{
		{
			name:    "resolved",
			secrets: secretMap{secretVersion: "s3cret-pass"},
			version: secretVersion,
			want:    http.StatusOK,
		},
		{
			name:    "wrong password",
			secrets: secretMap{secretVersion: "nope"},
			version: secretVersion,
			want:    http.StatusBadGateway,
		},
		{name: "missing secret", secrets: secretMap{}, version: secretVersion, want: http.StatusBadGateway},
		{name: "no resolver", version: secretVersion, want: http.StatusBadGateway},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnvWith(t, "", func(b *gcpartifactregistry.InMemoryBackend) {
				b.SetRemoteUpstreams(true)

				if tt.secrets != nil {
					b.SetSecretResolver(tt.secrets)
				}
			})

			cfg := npmUpstream(up.URL)
			cfg.UpstreamCredentials = &artifactregistrypb.RemoteRepositoryConfig_UpstreamCredentials{
				Credentials: &artifactregistrypb.RemoteRepositoryConfig_UpstreamCredentials_UsernamePasswordCredentials_{
					UsernamePasswordCredentials: &artifactregistrypb.RemoteRepositoryConfig_UpstreamCredentials_UsernamePasswordCredentials{
						Username:              "robot",
						PasswordSecretVersion: tt.version,
					},
				},
			}
			e.createRepo(t, "npm-auth", remoteRepo(artifactregistrypb.Repository_NPM, cfg))

			code, body := getBody(t, "http://127.0.0.1:"+e.port+"/npm/"+project+"/npm-auth/left-pad")
			assert.Equal(t, tt.want, code, string(body))
			assert.NotContains(t, string(body), "s3cret-pass")

			repo, err := e.client.GetRepository(
				t.Context(),
				&artifactregistrypb.GetRepositoryRequest{Name: repoName("npm-auth")},
			)
			require.NoError(t, err)
			assert.NotContains(t, repo.String(), "s3cret-pass")
		})
	}
}
