package ocivault_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/keymanagement"
	"github.com/oracle/oci-go-sdk/v65/secrets"
	"github.com/oracle/oci-go-sdk/v65/vault"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"

	oci "github.com/blackbirdworks/gopherstack/services/ocivault"
)

const (
	testTenancy     = "ocid1.tenancy.oc1..tenancy"
	testUser        = "ocid1.user.oc1..user"
	testCompartment = "ocid1.compartment.oc1..comp"
)

type keyPair struct {
	priv    *rsa.PrivateKey
	privPEM string
	pubPEM  string
	fprint  string
}

func newKeyPair(t *testing.T) keyPair {
	t.Helper()

	k, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	pub, err := x509.MarshalPKIXPublicKey(&k.PublicKey)
	require.NoError(t, err)

	fp, err := ociauth.Fingerprint(&k.PublicKey)
	require.NoError(t, err)

	return keyPair{
		priv:    k,
		privPEM: string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(k)})),
		pubPEM:  string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pub})),
		fprint:  fp,
	}
}

type fakeClock struct{ ns atomic.Int64 }

func newFakeClock() *fakeClock {
	c := &fakeClock{}
	c.ns.Store(time.Now().UnixNano())

	return c
}

func (c *fakeClock) Now() time.Time          { return time.Unix(0, c.ns.Load()).UTC() }
func (c *fakeClock) Advance(d time.Duration) { c.ns.Add(int64(d)) }

type env struct {
	backend *oci.InMemoryBackend
	handler *oci.Handler
	clock   *fakeClock
	prov    common.ConfigurationProvider
	kp      keyPair
	vaults  keymanagement.KmsVaultClient
	vsecret vault.VaultsClient
	bundles secrets.SecretsClient
	base    string
}

type envOpts struct {
	enforce  bool
	register bool
}

func freePort(t *testing.T) int {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	defer l.Close()

	tcp, ok := l.Addr().(*net.TCPAddr)
	require.True(t, ok)

	return tcp.Port
}

func newEnv(t *testing.T, opts envOpts) *env {
	t.Helper()

	be := oci.NewInMemoryBackend()
	clock := newFakeClock()
	be.SetClock(clock.Now)

	h := oci.NewHandler(be)
	h.Port = freePort(t)
	h.EnforceAuth = opts.enforce

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	kp := newKeyPair(t)
	if opts.register {
		_, err := be.Auth.Register(testTenancy, testUser, kp.pubPEM)
		require.NoError(t, err)
	}

	e := &env{
		backend: be, handler: h, clock: clock, kp: kp,
		prov: common.NewRawConfigurationProvider(testTenancy, testUser, oci.Region, kp.fprint, kp.privPEM, nil),
		base: fmt.Sprintf("http://localhost:%d", h.Port),
	}

	var err error

	e.vaults, err = keymanagement.NewKmsVaultClientWithConfigurationProvider(e.prov)
	require.NoError(t, err)

	e.vaults.Host = e.base

	e.vsecret, err = vault.NewVaultsClientWithConfigurationProvider(e.prov)
	require.NoError(t, err)

	e.vsecret.Host = e.base

	e.bundles, err = secrets.NewSecretsClientWithConfigurationProvider(e.prov)
	require.NoError(t, err)

	e.bundles.Host = e.base

	return e
}

func (e *env) mgmt(t *testing.T, v keymanagement.Vault) keymanagement.KmsManagementClient {
	t.Helper()

	c, err := keymanagement.NewKmsManagementClientWithConfigurationProvider(e.prov, *v.ManagementEndpoint)
	require.NoError(t, err)

	return c
}

func (e *env) crypto(t *testing.T, v keymanagement.Vault) keymanagement.KmsCryptoClient {
	t.Helper()

	c, err := keymanagement.NewKmsCryptoClientWithConfigurationProvider(e.prov, *v.CryptoEndpoint)
	require.NoError(t, err)

	return c
}

func (e *env) mkVault(t *testing.T, name string) keymanagement.Vault {
	t.Helper()

	resp, err := e.vaults.CreateVault(t.Context(), keymanagement.CreateVaultRequest{
		CompartmentId: new(testCompartment), DisplayName: new(name),
		VaultType: keymanagement.CreateVaultDetailsVaultTypeDefault,
	})
	require.NoError(t, err)

	return resp.Vault
}

func shape(
	alg keymanagement.KeyShapeAlgorithmEnum,
	length int,
	curve keymanagement.KeyShapeCurveIdEnum,
) *keymanagement.KeyShape {
	return &keymanagement.KeyShape{Algorithm: alg, Length: new(length), CurveId: curve}
}

func aes256() *keymanagement.KeyShape { return shape(keymanagement.KeyShapeAlgorithmAes, 32, "") }

func (e *env) mkKey(t *testing.T, m keymanagement.KmsManagementClient, name string, ks *keymanagement.KeyShape,
	mode keymanagement.CreateKeyDetailsProtectionModeEnum,
) keymanagement.Key {
	t.Helper()

	resp, err := m.CreateKey(t.Context(), keymanagement.CreateKeyRequest{
		CompartmentId: new(testCompartment), DisplayName: new(name), KeyShape: ks, ProtectionMode: mode,
	})
	require.NoError(t, err)

	return resp.Key
}

func svcErr(t *testing.T, err error) common.ServiceError {
	t.Helper()
	require.Error(t, err)

	se, ok := common.IsServiceError(err)
	require.True(t, ok, "not a service error: %v", err)

	return se
}
