package integration_test

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azcertificates"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azkeys"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type kvTestCredential struct{}

func (kvTestCredential) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{Token: "gopherstack-test-token", ExpiresOn: time.Now().Add(time.Hour)}, nil
}

// kvVault returns a fresh, uniquely named vault URL (path-prefix addressing)
// and client options trusting the listener's self-signed certificate.
func kvVault(t *testing.T) (string, azcore.ClientOptions) {
	t.Helper()

	if azureKeyVaultEndpoint == "" {
		t.Skip("Azure Key Vault endpoint not available (mapped port could not be determined)")
	}

	httpClient := &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}}

	return azureKeyVaultEndpoint + "/vaults/it-" + uuid.NewString()[:8], azcore.ClientOptions{Transport: httpClient}
}

func kvSecretsClient(t *testing.T) *azsecrets.Client {
	t.Helper()

	url, opts := kvVault(t)
	c, err := azsecrets.NewClient(url, kvTestCredential{}, &azsecrets.ClientOptions{
		ClientOptions: opts, DisableChallengeResourceVerification: true,
	})
	require.NoError(t, err)

	return c
}

func kvKeysClient(t *testing.T) *azkeys.Client {
	t.Helper()

	url, opts := kvVault(t)
	c, err := azkeys.NewClient(url, kvTestCredential{}, &azkeys.ClientOptions{
		ClientOptions: opts, DisableChallengeResourceVerification: true,
	})
	require.NoError(t, err)

	return c
}

func kvCertsClient(t *testing.T) *azcertificates.Client {
	t.Helper()

	url, opts := kvVault(t)
	c, err := azcertificates.NewClient(url, kvTestCredential{}, &azcertificates.ClientOptions{
		ClientOptions: opts, DisableChallengeResourceVerification: true,
	})
	require.NoError(t, err)

	return c
}

func requireKVNotFound(t *testing.T, err error, code string) {
	t.Helper()

	var respErr *azcore.ResponseError

	require.ErrorAs(t, err, &respErr)
	assert.Equal(t, http.StatusNotFound, respErr.StatusCode)
	assert.Equal(t, code, respErr.ErrorCode)
}

func TestIntegration_AzureKeyVault_SecretLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	c := kvSecretsClient(t)
	ctx := t.Context()

	first, err := c.SetSecret(ctx, "db", azsecrets.SetSecretParameters{Value: new("one")}, nil)
	require.NoError(t, err)

	second, err := c.SetSecret(ctx, "db", azsecrets.SetSecretParameters{Value: new("two")}, nil)
	require.NoError(t, err)
	require.NotEqual(t, first.ID.Version(), second.ID.Version())

	latest, err := c.GetSecret(ctx, "db", "", nil)
	require.NoError(t, err)
	assert.Equal(t, "two", *latest.Value)

	old, err := c.GetSecret(ctx, "db", first.ID.Version(), nil)
	require.NoError(t, err)
	assert.Equal(t, "one", *old.Value)

	versions := 0
	vp := c.NewListSecretPropertiesVersionsPager("db", nil)

	for vp.More() {
		pg, pErr := vp.NextPage(ctx)
		require.NoError(t, pErr)

		versions += len(pg.Value)
	}

	assert.Equal(t, 2, versions)

	names := []string{}
	lp := c.NewListSecretPropertiesPager(nil)

	for lp.More() {
		pg, pErr := lp.NextPage(ctx)
		require.NoError(t, pErr)

		for _, s := range pg.Value {
			names = append(names, s.ID.Name())
		}
	}

	assert.Equal(t, []string{"db"}, names)

	_, err = c.DeleteSecret(ctx, "db", nil)
	require.NoError(t, err)

	_, err = c.GetSecret(ctx, "db", "", nil)
	requireKVNotFound(t, err, "SecretNotFound")

	_, err = c.RecoverDeletedSecret(ctx, "db", nil)
	require.NoError(t, err)

	_, err = c.GetSecret(ctx, "db", "", nil)
	require.NoError(t, err)

	_, err = c.DeleteSecret(ctx, "db", nil)
	require.NoError(t, err)
	_, err = c.PurgeDeletedSecret(ctx, "db", nil)
	require.NoError(t, err)

	_, err = c.RecoverDeletedSecret(ctx, "db", nil)
	requireKVNotFound(t, err, "SecretNotFound")
}

func TestIntegration_AzureKeyVault_SecretNotFound(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	c := kvSecretsClient(t)

	_, err := c.GetSecret(t.Context(), "missing", "", nil)
	requireKVNotFound(t, err, "SecretNotFound")

	_, err = c.DeleteSecret(t.Context(), "missing", nil)
	requireKVNotFound(t, err, "SecretNotFound")
}

func TestIntegration_AzureKeyVault_KeyNotFound(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	c := kvKeysClient(t)

	_, err := c.GetKey(t.Context(), "missing", "", nil)
	requireKVNotFound(t, err, "KeyNotFound")
}

func TestIntegration_AzureKeyVault_CertificateNotFound(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	c := kvCertsClient(t)

	_, err := c.GetCertificate(t.Context(), "missing", "", nil)
	requireKVNotFound(t, err, "CertificateNotFound")
}

func TestIntegration_AzureKeyVault_RSAKeyOperations(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	c := kvKeysClient(t)
	ctx := t.Context()

	created, err := c.CreateKey(ctx, "rsa", azkeys.CreateKeyParameters{
		Kty: new(azkeys.KeyTypeRSA), KeySize: new(int32(2048)),
	}, nil)
	require.NoError(t, err)

	version := created.Key.KID.Version()
	plain := []byte("attack at dawn")

	encAlg := azkeys.EncryptionAlgorithmRSAOAEP256
	enc, err := c.Encrypt(ctx, "rsa", version, azkeys.KeyOperationParameters{Algorithm: &encAlg, Value: plain}, nil)
	require.NoError(t, err)

	dec, err := c.Decrypt(
		ctx,
		"rsa",
		version,
		azkeys.KeyOperationParameters{Algorithm: &encAlg, Value: enc.Result},
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, plain, dec.Result)

	cek := []byte("0123456789abcdef0123456789abcdef")
	w, err := c.WrapKey(ctx, "rsa", version, azkeys.KeyOperationParameters{Algorithm: &encAlg, Value: cek}, nil)
	require.NoError(t, err)

	u, err := c.UnwrapKey(ctx, "rsa", version, azkeys.KeyOperationParameters{Algorithm: &encAlg, Value: w.Result}, nil)
	require.NoError(t, err)
	assert.Equal(t, cek, u.Result)

	digest := sha256.Sum256(plain)
	sigAlg := azkeys.SignatureAlgorithmRS256
	sig, err := c.Sign(ctx, "rsa", version, azkeys.SignParameters{Algorithm: &sigAlg, Value: digest[:]}, nil)
	require.NoError(t, err)

	ok, err := c.Verify(ctx, "rsa", version,
		azkeys.VerifyParameters{Algorithm: &sigAlg, Digest: digest[:], Signature: sig.Result}, nil)
	require.NoError(t, err)
	assert.True(t, *ok.Value)

	rotated, err := c.RotateKey(ctx, "rsa", nil)
	require.NoError(t, err)
	assert.NotEqual(t, version, rotated.Key.KID.Version())

	count := 0
	vp := c.NewListKeyPropertiesVersionsPager("rsa", nil)

	for vp.More() {
		pg, pErr := vp.NextPage(ctx)
		require.NoError(t, pErr)

		count += len(pg.Value)
	}

	assert.Equal(t, 2, count)
}

func TestIntegration_AzureKeyVault_ECKeyOperations(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	c := kvKeysClient(t)
	ctx := t.Context()

	_, err := c.CreateKey(ctx, "ec", azkeys.CreateKeyParameters{
		Kty: new(azkeys.KeyTypeEC), Curve: new(azkeys.CurveNameP256),
	}, nil)
	require.NoError(t, err)

	digest := sha256.Sum256([]byte("payload"))
	alg := azkeys.SignatureAlgorithmES256
	sig, err := c.Sign(ctx, "ec", "", azkeys.SignParameters{Algorithm: &alg, Value: digest[:]}, nil)
	require.NoError(t, err)

	ok, err := c.Verify(ctx, "ec", "",
		azkeys.VerifyParameters{Algorithm: &alg, Digest: digest[:], Signature: sig.Result}, nil)
	require.NoError(t, err)
	assert.True(t, *ok.Value)

	names := 0
	lp := c.NewListKeyPropertiesPager(nil)

	for lp.More() {
		pg, pErr := lp.NextPage(ctx)
		require.NoError(t, pErr)

		names += len(pg.Value)
	}

	assert.Equal(t, 1, names)
}

func kvSelfSignedPolicy() azcertificates.CreateCertificateParameters {
	return azcertificates.CreateCertificateParameters{CertificatePolicy: &azcertificates.CertificatePolicy{
		IssuerParameters: &azcertificates.IssuerParameters{Name: new("Self")},
		X509CertificateProperties: &azcertificates.X509CertificateProperties{
			Subject:          new("CN=example.com"),
			ValidityInMonths: new(int32(6)),
		},
		KeyProperties: &azcertificates.KeyProperties{
			Exportable: new(true), KeyType: new(azcertificates.KeyTypeEC), Curve: new(azcertificates.CurveNameP256),
		},
	}}
}

func TestIntegration_AzureKeyVault_CertificateLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	c := kvCertsClient(t)
	ctx := t.Context()

	op, err := c.CreateCertificate(ctx, "web", kvSelfSignedPolicy(), nil)
	require.NoError(t, err)
	assert.Equal(t, "completed", *op.Status)

	got, err := c.GetCertificate(ctx, "web", "", nil)
	require.NoError(t, err)

	cert, err := x509.ParseCertificate(got.CER)
	require.NoError(t, err)
	assert.Equal(t, "example.com", cert.Subject.CommonName)

	count := 0
	lp := c.NewListCertificatePropertiesPager(nil)

	for lp.More() {
		pg, pErr := lp.NextPage(ctx)
		require.NoError(t, pErr)

		count += len(pg.Value)
	}

	assert.Equal(t, 1, count)

	_, err = c.DeleteCertificate(ctx, "web", nil)
	require.NoError(t, err)

	_, err = c.GetCertificate(ctx, "web", "", nil)
	requireKVNotFound(t, err, "CertificateNotFound")

	_, err = c.RecoverDeletedCertificate(ctx, "web", nil)
	require.NoError(t, err)

	_, err = c.GetCertificate(ctx, "web", "", nil)
	require.NoError(t, err)
}
