package ocivault_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/keymanagement"
	"github.com/oracle/oci-go-sdk/v65/secrets"
	"github.com/oracle/oci-go-sdk/v65/vault"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/service"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	oci "github.com/blackbirdworks/gopherstack/services/ocivault"
)

func rawDo(t *testing.T, method, url, body string) (int, string) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, url, strings.NewReader(body))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/json")

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	out, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, string(out)
}

func TestPerVaultHostRouting(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	ctx := t.Context()
	va, vb := e.mkVault(t, "a"), e.mkVault(t, "b")
	ma, mb := e.mgmt(t, va), e.mgmt(t, vb)

	ka := e.mkKey(t, ma, "in-a", aes256(), "")
	kb := e.mkKey(t, mb, "in-b", aes256(), "")
	assert.Equal(t, *va.Id, *ka.VaultId)
	assert.Equal(t, *vb.Id, *kb.VaultId)

	list, err := ma.ListKeys(ctx, keymanagement.ListKeysRequest{CompartmentId: new(testCompartment)})
	require.NoError(t, err)
	require.Len(t, list.Items, 1)
	assert.Equal(t, "in-a", *list.Items[0].DisplayName)

	_, err = ma.GetKey(ctx, keymanagement.GetKeyRequest{KeyId: kb.Id})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode(), "key of another vault is invisible")

	_, err = e.crypto(t, vb).Encrypt(ctx, keymanagement.EncryptRequest{
		KeyId: ka.Id, Plaintext: new(b64([]byte("x")))})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode(), "crypto endpoint of another vault")

	unknown, err := keymanagement.NewKmsManagementClientWithConfigurationProvider(
		e.prov,
		strings.Replace(*va.ManagementEndpoint, "http://", "http://deadbeef", 1),
	)
	require.NoError(t, err)

	_, err = unknown.ListKeys(ctx, keymanagement.ListKeysRequest{CompartmentId: new(testCompartment)})
	assert.Equal(t, http.StatusNotFound, svcErr(t, err).GetHTTPStatusCode())
}

func TestEndpointlessHostFallsBackToSoleVault(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	v := e.mkVault(t, "only")

	m, err := keymanagement.NewKmsManagementClientWithConfigurationProvider(e.prov, e.base)
	require.NoError(t, err)

	k := e.mkKey(t, m, "k", aes256(), "")
	assert.Equal(t, *v.Id, *k.VaultId)

	e.mkVault(t, "second")

	_, err = m.ListKeys(t.Context(), keymanagement.ListKeysRequest{CompartmentId: new(testCompartment)})
	se := svcErr(t, err)
	assert.Equal(t, http.StatusBadRequest, se.GetHTTPStatusCode())
	assert.Equal(t, "InvalidParameter", se.GetCode())
}

func TestPathVaultRouting(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	v := e.mkVault(t, "path")
	e.mkVault(t, "other")

	body := `{"compartmentId":"` + testCompartment +
		`","displayName":"via-path","keyShape":{"algorithm":"AES","length":16}}`
	code, out := rawDo(t, http.MethodPost, e.base+"/_vault/"+*v.Id+"/20180608/keys", body)
	require.Equal(t, http.StatusOK, code, out)

	var k struct {
		VaultID string `json:"vaultId"`
	}

	require.NoError(t, json.Unmarshal([]byte(out), &k))
	assert.Equal(t, *v.Id, k.VaultID)

	code, out = rawDo(t, http.MethodGet, e.base+"/_vault/"+*v.Id+"/20180608/keys?compartmentId="+testCompartment, "")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, out, "via-path")
}

func TestErrorBodyShapeAndUnknownRoutes(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})

	tests := []struct {
		name     string
		method   string
		path     string
		wantCode string
		status   int
	}{
		{"unknown path", http.MethodGet, "/20180608/nothing", "NotFound", http.StatusNotFound},
		{"wrong method", http.MethodDelete, "/20180608/vaults", "NotFound", http.StatusNotFound},
		{
			"missing vault",
			http.MethodGet,
			"/20180608/vaults/ocid1.vault.oc1..x",
			"NotAuthorizedOrNotFound",
			http.StatusNotFound,
		},
		{"bad json", http.MethodPost, "/20180608/vaults", "CannotParseRequest", http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := ""
			if tt.method == http.MethodPost {
				body = "{not json"
			}

			code, out := rawDo(t, tt.method, e.base+tt.path, body)
			assert.Equal(t, tt.status, code)

			var eb struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			}

			require.NoError(t, json.Unmarshal([]byte(out), &eb))
			assert.Equal(t, tt.wantCode, eb.Code)
			assert.NotEmpty(t, eb.Message)
			assert.NotEqual(t, eb.Code, eb.Message)
		})
	}
}

func TestSignatureVerification(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
		opts     envOpts
		forge    bool
	}{
		{name: "permissive accepts unregistered key", opts: envOpts{}, wantCode: 200},
		{name: "registered key valid signature", opts: envOpts{register: true}, wantCode: 200},
		{name: "registered key forged signature", opts: envOpts{register: true}, forge: true, wantCode: 401},
		{name: "enforce rejects unknown key", opts: envOpts{enforce: true}, wantCode: 401},
		{name: "enforce accepts registered key", opts: envOpts{enforce: true, register: true}, wantCode: 200},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, tt.opts)

			if tt.forge {
				other := newKeyPair(t)
				prov := common.NewRawConfigurationProvider(
					testTenancy,
					testUser,
					oci.Region,
					e.kp.fprint,
					other.privPEM,
					nil,
				)
				c, err := keymanagement.NewKmsVaultClientWithConfigurationProvider(prov)
				require.NoError(t, err)

				c.Host = e.base
				e.vaults = c
			}

			_, err := e.vaults.ListVaults(
				t.Context(),
				keymanagement.ListVaultsRequest{CompartmentId: new(testCompartment)},
			)
			if tt.wantCode == http.StatusOK {
				require.NoError(t, err)

				return
			}

			se := svcErr(t, err)
			assert.Equal(t, tt.wantCode, se.GetHTTPStatusCode())
			assert.Equal(t, "NotAuthenticated", se.GetCode())
		})
	}
}

func TestAdminKeyRegistration(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{enforce: true})
	listReq := keymanagement.ListVaultsRequest{CompartmentId: new(testCompartment)}

	_, err := e.vaults.ListVaults(t.Context(), listReq)
	assert.Equal(t, http.StatusUnauthorized, svcErr(t, err).GetHTTPStatusCode())

	payload, _ := json.Marshal(map[string]string{"tenancy": testTenancy, "user": testUser, "publicKey": e.kp.pubPEM})
	code, body := rawDo(t, http.MethodPost, e.base+"/_gopherstack/oci/keys", string(payload))
	require.Equal(t, http.StatusCreated, code, body)

	_, err = e.vaults.ListVaults(t.Context(), listReq)
	require.NoError(t, err)

	code, body = rawDo(t, http.MethodGet, e.base+"/_gopherstack/oci/keys", "")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, testTenancy)

	code, _ = rawDo(t, http.MethodPost, e.base+"/_gopherstack/oci/keys", `{"tenancy":"t"}`)
	assert.Equal(t, http.StatusBadRequest, code)
}

func TestSnapshotRestoreFullState(t *testing.T) {
	t.Parallel()

	src := newSecretEnv(t)
	ctx := t.Context()

	cenv := &cryptoEnv{m: src.m, c: src.e.crypto(t, src.v), e: src.e, v: src.v}
	enc, err := cenv.encrypt(t, src.key, "persisted", nil, "")
	require.NoError(t, err)

	rsaKey := src.e.mkKey(t, src.m, "rsa", shape(keymanagement.KeyShapeAlgorithmRsa, 256, ""), "")
	signed, err := cenv.sign(t, rsaKey, "SHA_256_RSA_PKCS_PSS", "RAW", []byte("m"))
	require.NoError(t, err)

	s := src.create(t, "kept", "secret-value", nil)
	src.update(t, s.Id, vault.UpdateSecretDetails{SecretContent: content("secret-v2")})

	snap := src.e.backend.Snapshot(ctx)

	dst := newEnv(t, envOpts{})
	require.NoError(t, dst.handler.Restore(ctx, snap))

	r := &secretEnv{e: dst}
	b, err := r.bundle(
		t,
		s.Id,
		func(req *secrets.GetSecretBundleRequest) { req.Stage = secrets.GetSecretBundleStagePrevious },
	)
	require.NoError(t, err)
	assert.Equal(t, "secret-value", bundleText(t, b))

	got, err := dst.vaults.GetVault(ctx, keymanagement.GetVaultRequest{VaultId: src.v.Id})
	require.NoError(t, err)
	assert.Equal(t, *src.v.ManagementEndpoint != "", *got.ManagementEndpoint != "")

	rc := &cryptoEnv{m: dst.mgmt(t, got.Vault), c: dst.crypto(t, got.Vault), e: dst, v: got.Vault}
	dec, err := rc.decrypt(t, src.key, *enc.Ciphertext, nil, "")
	require.NoError(t, err)
	assert.Equal(t, "persisted", string(unb64(t, *dec.Plaintext)))
	assert.True(
		t,
		rc.verify(
			t,
			rsaKey,
			*signed.KeyVersionId,
			"SHA_256_RSA_PKCS_PSS",
			"RAW",
			[]byte("m"),
			unb64(t, *signed.Signature),
		),
	)

	fresh := dst.mkVault(t, "after-restore")
	assert.NotEqual(t, *src.v.Id, *fresh.Id)
}

func TestRestoreRejections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		snap    map[string]any
		name    string
		wantErr bool
	}{
		{
			name: "future version starts empty",
			snap: map[string]any{"version": 99, "vaults": map[string]any{"x": map[string]any{}}},
		},
		{name: "null vault", snap: map[string]any{"version": 1, "vaults": map[string]any{"x": nil}}, wantErr: true},
		{name: "null key", snap: map[string]any{"version": 1, "keys": map[string]any{"x": nil}}, wantErr: true},
		{name: "null secret", snap: map[string]any{"version": 1, "secrets": map[string]any{"x": nil}}, wantErr: true},
		{name: "empty maps tolerated", snap: map[string]any{"version": 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, envOpts{})
			e.mkVault(t, "pre")

			err := e.backend.Restore(t.Context(), persistence.MarshalSnapshot(t.Context(), "ocivault", tt.snap))
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			list, lerr := e.vaults.ListVaults(
				t.Context(),
				keymanagement.ListVaultsRequest{CompartmentId: new(testCompartment)},
			)
			require.NoError(t, lerr)
			assert.Empty(t, list.Items)
		})
	}
}

func TestHandlerMetadata(t *testing.T) {
	t.Parallel()

	h := oci.NewHandler(oci.NewInMemoryBackend())
	assert.Equal(t, "OCIVault", h.Name())
	assert.Len(t, h.GetSupportedOperations(), 45)
	assert.Zero(t, h.MatchPriority())
	assert.False(t, h.RouteMatcher()(nil))

	var _ service.Registerable = h

	h.Reset()
	h.Shutdown(context.Background())
}

func TestProviderInit(t *testing.T) {
	t.Parallel()

	p := &oci.Provider{}
	assert.Equal(t, "OCIVault", p.Name())

	_, err := p.Init(nil)
	require.ErrorIs(t, err, oci.ErrNilAppContext)

	reg, err := p.Init(&service.AppContext{Config: settingsConfig{oci.Settings{Port: 4321, EnforceAuth: true}}})
	require.NoError(t, err)

	h, ok := reg.(*oci.Handler)
	require.True(t, ok)
	assert.Equal(t, 4321, h.Port)
	assert.True(t, h.EnforceAuth)

	reg, err = p.Init(&service.AppContext{})
	require.NoError(t, err)

	h, ok = reg.(*oci.Handler)
	require.True(t, ok)
	assert.Equal(t, oci.DefaultPort, h.Port)
}

type settingsConfig struct{ s oci.Settings }

func (c settingsConfig) GetOCIVaultSettings() oci.Settings { return c.s }
