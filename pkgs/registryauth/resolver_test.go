package registryauth_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/registryauth"
)

var errPull = errors.New("pull denied")

type pullCall struct{ ref, auth string }

type fakeDaemon struct {
	failRefs map[string]bool
	pulls    []pullCall
	tags     [][2]string
}

func (f *fakeDaemon) pull(_ context.Context, ref, auth string) error {
	f.pulls = append(f.pulls, pullCall{ref, auth})
	if f.failRefs[ref] {
		return errPull
	}

	return nil
}

func (f *fakeDaemon) tag(_ context.Context, src, dst string) error {
	f.tags = append(f.tags, [2]string{src, dst})

	return nil
}

func decode(t *testing.T, enc string) map[string]string {
	t.Helper()

	raw, err := base64.URLEncoding.DecodeString(enc)
	require.NoError(t, err)

	m := map[string]string{}
	require.NoError(t, json.Unmarshal(raw, &m))

	return m
}

func writeConfig(t *testing.T, cfg string) {
	t.Helper()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "config.json"), []byte(cfg), 0o600))
	t.Setenv("DOCKER_CONFIG", dir)
}

func writeHelper(t *testing.T, name, script string) {
	t.Helper()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "docker-credential-"+name), []byte("#!/bin/sh\n"+script), 0o700))
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

//nolint:paralleltest // t.Setenv
func TestAuthForSources(t *testing.T) {
	writeConfig(t, `{"auths":{
		"https://index.docker.io/v1/":{"auth":"`+b64("hubuser:hubpass")+`"},
		"ghcr.io":{"identitytoken":"tok123"},
		"cfg.example.com":{"username":"u","password":"p"}},
		"credHelpers":{"helper.example.com":"fake"}}`)
	writeHelper(t, "fake", `read s; [ "$s" = "helper.example.com" ] || exit 1
echo '{"ServerURL":"helper.example.com","Username":"hu","Secret":"hs"}'`)

	r, err := registryauth.New(registryauth.Settings{Auth: []string{"explicit.example.com=eu:ep"}})
	require.NoError(t, err)

	r.RegisterEmulated("localhost:4566", "AWS", "dummy-password")

	tests := []struct {
		want map[string]string
		name string
		host string
	}{
		{map[string]string{"username": "eu", "password": "ep"}, "explicit", "explicit.example.com"},
		{map[string]string{"username": "hubuser", "password": "hubpass"}, "hub alias", "registry-1.docker.io"},
		{map[string]string{"identitytoken": "tok123"}, "identity token", "ghcr.io"},
		{map[string]string{"username": "u", "password": "p"}, "plain user", "cfg.example.com"},
		{map[string]string{"username": "hu", "password": "hs"}, "helper", "helper.example.com"},
		{map[string]string{"username": "AWS", "password": "dummy-password"}, "emulated ecr", "localhost:4566"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := decode(t, r.AuthFor(t.Context(), tt.host))
			delete(got, "serveraddress")
			assert.Equal(t, tt.want, got)
		})
	}

	assert.Empty(t, r.AuthFor(t.Context(), "unknown.example.com"))
}

//nolint:paralleltest // t.Setenv
func TestCredsStoreAndTokenHelper(t *testing.T) {
	writeConfig(t, `{"credsStore":"store"}`)
	writeHelper(t, "store", `cat >/dev/null
echo '{"Username":"<token>","Secret":"idtok"}'`)

	r, err := registryauth.New(registryauth.Settings{})
	require.NoError(t, err)

	got := decode(t, r.AuthFor(t.Context(), "any.example.com"))
	assert.Equal(t, "idtok", got["identitytoken"])
	assert.Empty(t, got["username"])
}

//nolint:paralleltest // t.Setenv
func TestPullMirror(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", t.TempDir())

	tests := []struct {
		name      string
		ref       string
		failRefs  []string
		wantPulls []string
		wantTags  [][2]string
	}{
		{
			name:      "hub implicit library",
			ref:       "alpine:3.19",
			wantPulls: []string{"mirror.example.com/library/alpine:3.19"},
			wantTags:  [][2]string{{"mirror.example.com/library/alpine:3.19", "docker.io/library/alpine:3.19"}},
		},
		{
			name:      "ghcr default tag",
			ref:       "ghcr.io/org/app",
			wantPulls: []string{"ghcr.example.com/org/app:latest"},
			wantTags:  [][2]string{{"ghcr.example.com/org/app:latest", "ghcr.io/org/app:latest"}},
		},
		{
			name:      "fallback on mirror failure",
			ref:       "alpine:3.19",
			failRefs:  []string{"mirror.example.com/library/alpine:3.19"},
			wantPulls: []string{"mirror.example.com/library/alpine:3.19", "alpine:3.19"},
		},
		{
			name:      "digest bypasses mirror",
			ref:       "alpine@sha256:abc",
			wantPulls: []string{"alpine@sha256:abc"},
		},
		{
			name:      "unmirrored registry",
			ref:       "quay.io/x/y:1",
			wantPulls: []string{"quay.io/x/y:1"},
		},
	}

	for _, tt := range tests {
		//nolint:paralleltest // t.Setenv in parent
		t.Run(tt.name, func(t *testing.T) {
			r, err := registryauth.New(registryauth.Settings{
				Mirror: []string{"docker.io=mirror.example.com,ghcr.io=https://ghcr.example.com/"},
			})
			require.NoError(t, err)

			d := &fakeDaemon{failRefs: map[string]bool{}}
			for _, f := range tt.failRefs {
				d.failRefs[f] = true
			}

			require.NoError(t, r.Pull(t.Context(), tt.ref, d.pull, d.tag))

			pulled := make([]string, 0, len(d.pulls))
			for _, p := range d.pulls {
				pulled = append(pulled, p.ref)
			}

			assert.Equal(t, tt.wantPulls, pulled)
			assert.Equal(t, tt.wantTags, d.tags)
		})
	}
}

func TestPullUsesPerHostAuthAndSkipsMirrorWithoutTagger(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", t.TempDir())

	r, err := registryauth.New(registryauth.Settings{
		Auth:   []string{"docker.io=hu:hp", "mirror.example.com=mu:mp"},
		Mirror: []string{"docker.io=mirror.example.com"},
	})
	require.NoError(t, err)

	d := &fakeDaemon{}
	require.NoError(t, r.Pull(t.Context(), "alpine", d.pull, d.tag))
	require.Len(t, d.pulls, 1)
	assert.Equal(t, "mu", decode(t, d.pulls[0].auth)["username"])

	d = &fakeDaemon{}
	require.NoError(t, r.Pull(t.Context(), "alpine", d.pull, nil))
	require.Len(t, d.pulls, 1)
	assert.Equal(t, "alpine", d.pulls[0].ref)
	assert.Equal(t, "hu", decode(t, d.pulls[0].auth)["username"])
}

func TestSettingsErrorsAndLogsNeverLeakSecrets(t *testing.T) {
	const secret = "s3cr3t-pw"

	_, err := registryauth.New(registryauth.Settings{Auth: []string{"host-" + secret}})
	require.ErrorIs(t, err, registryauth.ErrInvalidSetting)
	assert.NotContains(t, err.Error(), secret)

	t.Setenv("DOCKER_CONFIG", t.TempDir())

	r, err := registryauth.New(registryauth.Settings{
		Auth:   []string{"mirror.example.com=u:" + secret},
		Mirror: []string{"docker.io=mirror.example.com"},
	})
	require.NoError(t, err)

	var buf bytes.Buffer

	ctx := logger.Save(t.Context(), slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	d := &fakeDaemon{failRefs: map[string]bool{"mirror.example.com/library/alpine:latest": true}}

	require.NoError(t, r.Pull(ctx, "alpine", d.pull, d.tag))
	assert.Contains(t, buf.String(), "falling back")
	assert.NotContains(t, buf.String(), secret)
	assert.NotContains(t, buf.String(), b64("u:"+secret))
}

//nolint:paralleltest // mutates the process-wide resolver
func TestConfigureAndDefault(t *testing.T) {
	require.NoError(t, registryauth.Configure(registryauth.Settings{Auth: []string{"d.example.com=a:b"}}))
	assert.NotEmpty(t, registryauth.Default().AuthFor(t.Context(), "d.example.com"))

	require.Error(t, registryauth.Configure(registryauth.Settings{Mirror: []string{"bad"}}))
	assert.Nil(t, registryauth.TaggerFrom(struct{}{}))
}
