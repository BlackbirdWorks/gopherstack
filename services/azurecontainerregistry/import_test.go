package azurecontainerregistry_test

import (
	"encoding/json/v2"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	acr "github.com/blackbirdworks/gopherstack/services/azurecontainerregistry"
)

func newImportEnv(t *testing.T) (*env, acr.Credentials, acr.Credentials) {
	t.Helper()

	e := newEnv(t, "")
	src := e.registry(t, "sourcereg", true)
	dst := e.registry(t, "targetreg", true)

	e.client(t, "sourcereg", basic(src.Username, src.Password1)).PushImage("team/app", "v1")

	return e, src, dst
}

func pullStatus(t *testing.T, e *env, creds acr.Credentials, repo, ref string) int {
	t.Helper()

	return e.client(t, creds.Username, basic(creds.Username, creds.Password1)).
		Do(http.MethodGet, "/v2/"+repo+"/manifests/"+ref, nil, nil).Status
}

func TestImportImageFromEmulatedRegistry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		present [][2]string
		absent  [][2]string
		req     acr.ImportRequest
	}{
		{name: "same repo and tag",
			req:     acr.ImportRequest{Source: acr.ImportSource{Registry: "sourcereg", Image: "team/app:v1"}},
			present: [][2]string{{"team/app", "v1"}}},
		{name: "target tags",
			req: acr.ImportRequest{
				Source:     acr.ImportSource{Registry: "sourcereg", Image: "team/app:v1"},
				TargetTags: []string{"copy/one:stable", "copy/two"},
			},
			present: [][2]string{{"copy/one", "stable"}, {"copy/two", "v1"}},
			absent:  [][2]string{{"team/app", "v1"}}},
		{name: "default latest tag",
			req: acr.ImportRequest{
				Source: acr.ImportSource{Registry: "sourcereg", Image: "team/app:v1"}, TargetTags: []string{"copy/x"},
			},
			present: [][2]string{{"copy/x", "v1"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, _, dst := newImportEnv(t)
			require.NoError(t, e.h.Backend.ImportImage(t.Context(), "targetreg", tt.req))

			for _, p := range tt.present {
				assert.Equal(t, http.StatusOK, pullStatus(t, e, dst, p[0], p[1]), p)
			}

			for _, p := range tt.absent {
				assert.Equal(t, http.StatusNotFound, pullStatus(t, e, dst, p[0], p[1]), p)
			}
		})
	}
}

func TestImportImageBlobsAndMetadata(t *testing.T) {
	t.Parallel()

	e, _, dst := newImportEnv(t)
	require.NoError(t, e.h.Backend.ImportImage(t.Context(), "targetreg", acr.ImportRequest{
		Source: acr.ImportSource{Registry: "sourcereg", Image: "team/app:v1"},
	}))

	c := e.client(t, "targetreg", basic(dst.Username, dst.Password1))
	tags := c.Do(http.MethodGet, "/acr/v1/team/app/_tags", nil, nil)
	require.Equal(t, http.StatusOK, tags.Status)
	assert.Contains(t, string(tags.Body), `"name":"v1"`)

	m := c.Do(http.MethodGet, "/v2/team/app/manifests/v1", nil, nil)
	require.Equal(t, http.StatusOK, m.Status)

	var doc struct {
		Layers []struct {
			Digest string `json:"digest"`
		} `json:"layers"`
	}

	require.NoError(t, json.Unmarshal(m.Body, &doc, jsonx.V1()))
	require.NotEmpty(t, doc.Layers)

	blob := c.Do(http.MethodGet, "/v2/team/app/blobs/"+doc.Layers[0].Digest, nil, nil)
	assert.Equal(t, http.StatusOK, blob.Status)
}

func TestImportImageTagConflict(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		force   bool
	}{
		{name: "noforce refuses", wantErr: acr.ErrImportTagExists},
		{name: "force overwrites", force: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, _, dst := newImportEnv(t)
			e.client(t, "targetreg", basic(dst.Username, dst.Password1)).PushImage("team/app", "v1")

			err := e.h.Backend.ImportImage(t.Context(), "targetreg", acr.ImportRequest{
				Source: acr.ImportSource{Registry: "sourcereg", Image: "team/app:v1"}, Force: tt.force,
			})
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestImportImageUntaggedAndDigest(t *testing.T) {
	t.Parallel()

	e, src, dst := newImportEnv(t)
	digest := e.client(t, "sourcereg", basic(src.Username, src.Password1)).
		Do(http.MethodHead, "/v2/team/app/manifests/v1", nil, nil).Header.Get("Docker-Content-Digest")
	require.NotEmpty(t, digest)

	require.NoError(t, e.h.Backend.ImportImage(t.Context(), "targetreg", acr.ImportRequest{
		Source:          acr.ImportSource{Registry: "sourcereg", Image: "team/app@" + digest},
		UntaggedTargets: []string{"untagged/app"},
	}))

	assert.Equal(t, http.StatusOK, pullStatus(t, e, dst, "untagged/app", digest))

	tags := e.client(t, "targetreg", basic(dst.Username, dst.Password1)).
		Do(http.MethodGet, "/acr/v1/untagged/app/_tags", nil, nil)
	assert.NotContains(t, string(tags.Body), `"name"`)
}

func TestImportImageErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		src     acr.ImportSource
		name    string
	}{
		{name: "missing image", src: acr.ImportSource{Registry: "sourcereg", Image: "team/none:v1"},
			wantErr: acr.ErrImportSourceNotFound},
		{name: "missing registry", src: acr.ImportSource{Registry: "ghostreg", Image: "team/app:v1"},
			wantErr: acr.ErrImportSourceNotFound},
		{name: "empty image", src: acr.ImportSource{Registry: "sourcereg"}, wantErr: acr.ErrImportInvalid},
		{name: "no source", src: acr.ImportSource{Image: "team/app:v1"}, wantErr: acr.ErrImportInvalid},
		{name: "external disabled", src: acr.ImportSource{RegistryURI: "mcr.microsoft.com", Image: "hello-world"},
			wantErr: acr.ErrImportRemoteDisabled},
		{name: "uri without credentials", src: acr.ImportSource{
			RegistryURI: "sourcereg.azurecr.localhost:10011", Image: "team/app:v1",
		}, wantErr: acr.ErrImportRemoteDisabled},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, _, _ := newImportEnv(t)

			err := e.h.Backend.ImportImage(t.Context(), "targetreg", acr.ImportRequest{Source: tt.src})
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestImportImageByRegistryURI(t *testing.T) {
	t.Parallel()

	e, src, dst := newImportEnv(t)

	require.NoError(t, e.h.Backend.ImportImage(t.Context(), "targetreg", acr.ImportRequest{
		Source: acr.ImportSource{
			RegistryURI: "sourcereg.azurecr.localhost:10011", Image: "team/app:v1",
			Username: src.Username, Password: src.Password1,
		},
	}))
	assert.Equal(t, http.StatusOK, pullStatus(t, e, dst, "team/app", "v1"))
}

func TestImportImageFromExternalRegistry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		wrong   bool
	}{
		{name: "credentials accepted"},
		{name: "wrong credentials", wrong: true, wantErr: acr.ErrImportAuth},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			upstream := newEnv(t, "")
			creds := upstream.registry(t, "upstreamreg", true)
			upstream.client(t, "upstreamreg", basic(creds.Username, creds.Password1)).PushImage("ext/app", "1.0")

			e := newEnv(t, "")
			dst := e.registry(t, "targetreg", true)
			e.h.Backend.SetRemoteImports(true)

			password := creds.Password1
			if tt.wrong {
				password = "wrong"
			}

			err := e.h.Backend.ImportImage(t.Context(), "targetreg", acr.ImportRequest{Source: acr.ImportSource{
				RegistryURI: upstream.srv.URL, Image: "ext/app:1.0", Username: creds.Username, Password: password,
			}})
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, http.StatusOK, pullStatus(t, e, dst, "ext/app", "1.0"))
		})
	}
}
