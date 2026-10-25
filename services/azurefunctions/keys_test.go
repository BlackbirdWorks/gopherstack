package azurefunctions_test

import (
	"encoding/json"
	"net/http"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurefunctions"
)

type secretsFile struct {
	MasterKey struct {
		Value string `json:"value"`
	} `json:"masterKey"`
	FunctionKeys []secretEntry `json:"functionKeys"`
	SystemKeys   []secretEntry `json:"systemKeys"`
	Keys         []secretEntry `json:"keys"`
}

type secretEntry struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

func loadSecrets(t *testing.T, dir, file string) secretsFile {
	t.Helper()

	raw, err := readFile(filepath.Join(dir, file))
	require.NoError(t, err)

	var s secretsFile
	require.NoError(t, json.Unmarshal([]byte(raw), &s))

	return s
}

func startedSecretsDir(t *testing.T, e *env) string {
	t.Helper()

	resp, _ := e.do(t, http.MethodGet, "app."+testDomain, "/api/greet", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	specs := e.rt.specs()
	require.NotEmpty(t, specs)

	return mountSource(specs[len(specs)-1], "/azure-functions-host/Secrets")
}

func newKeyEnv(t *testing.T) *env {
	t.Helper()

	e := newEnv(t, true)
	e.backend.PutApp("app", azurefunctions.AppConfig{LinuxFx: "NODE|20"})
	_, err := e.backend.Deploy("app", helloZip(t), "t", "m")
	require.NoError(t, err)

	return e
}

func TestKeys_DefaultsAndSecretFiles(t *testing.T) {
	t.Parallel()

	e := newKeyEnv(t)

	hk, err := e.backend.HostKeys("app")
	require.NoError(t, err)
	assert.NotEmpty(t, hk.MasterKey)
	assert.Contains(t, hk.FunctionKeys, "default")
	assert.Empty(t, hk.SystemKeys)

	dir := startedSecretsDir(t, e)
	s := loadSecrets(t, dir, "host.json")
	assert.Equal(t, hk.MasterKey, s.MasterKey.Value)
	require.Len(t, s.FunctionKeys, 1)
	assert.Equal(t, hk.FunctionKeys["default"], s.FunctionKeys[0].Value)
}

func TestKeys_HostKeyMutationsReachRunningHost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		keyType string
		keyName string
		value   string
	}{
		{"function key explicit", azurefunctions.KeyTypeFunction, "ci", "explicit-value=="},
		{"function key generated", azurefunctions.KeyTypeFunction, "gen", ""},
		{"system key", azurefunctions.KeyTypeSystem, "blobs_extension", "sys=="},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newKeyEnv(t)
			dir := startedSecretsDir(t, e)

			got, err := e.backend.SetHostKey("app", tt.keyType, tt.keyName, tt.value)
			require.NoError(t, err)

			if tt.value != "" {
				assert.Equal(t, tt.value, got)
			} else {
				assert.NotEmpty(t, got)
			}

			s := loadSecrets(t, dir, "host.json")

			files := s.FunctionKeys
			if tt.keyType == azurefunctions.KeyTypeSystem {
				files = s.SystemKeys
			}

			found := false

			for _, k := range files {
				found = found || k.Name == tt.keyName && k.Value == got
			}

			assert.True(t, found, "key written to the host's secret file")
			assert.Equal(t, 1, e.rt.running(), "no restart needed")

			require.NoError(t, e.backend.DeleteHostKey("app", tt.keyType, tt.keyName))
			require.ErrorIs(t, e.backend.DeleteHostKey("app", tt.keyType, tt.keyName), azurefunctions.ErrKeyNotFound)
		})
	}
}

func TestKeys_InvalidType(t *testing.T) {
	t.Parallel()

	e := newKeyEnv(t)
	_, err := e.backend.SetHostKey("app", "masterKey", "x", "")
	require.ErrorIs(t, err, azurefunctions.ErrInvalidKeyType)

	_, err = e.backend.SetHostKey("ghost", azurefunctions.KeyTypeFunction, "x", "")
	require.ErrorIs(t, err, azurefunctions.ErrAppNotFound)
}

func TestKeys_FunctionKeys(t *testing.T) {
	t.Parallel()

	e := newKeyEnv(t)
	dir := startedSecretsDir(t, e)

	keys, err := e.backend.FunctionKeys("app", "hello")
	require.NoError(t, err)
	require.Contains(t, keys, "default")

	again, err := e.backend.FunctionKeys("app", "hello")
	require.NoError(t, err)
	assert.Equal(t, keys, again, "default key is created once")

	s := loadSecrets(t, dir, "hello.json")
	require.Len(t, s.Keys, 1)
	assert.Equal(t, keys["default"], s.Keys[0].Value)

	_, err = e.backend.SetFunctionKey("app", "hello", "extra", "v==")
	require.NoError(t, err)
	assert.Len(t, loadSecrets(t, dir, "hello.json").Keys, 2)

	require.NoError(t, e.backend.DeleteFunctionKey("app", "hello", "extra"))
	require.ErrorIs(t, e.backend.DeleteFunctionKey("app", "hello", "extra"), azurefunctions.ErrKeyNotFound)
	assert.Len(t, loadSecrets(t, dir, "hello.json").Keys, 1)
}
